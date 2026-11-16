//! Concurrent Kafka partition fetcher with adaptive byte estimation, ordered result
//! reassembly, gap detection, and error/backoff handling.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/fetcher.go

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::error::{ParseResponseErrorCode, ResponseError};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, MetadataRequest, MetadataResponse,
    RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use mimir_common::backoff::{Backoff, BackoffConfig};
use parking_lot::Mutex as SyncMutex;
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error};

use super::compression::decompress_record_batch;
use super::kafka_client::{FetchedRecord, KAFKA_OFFSET_END, KAFKA_OFFSET_START};
use super::partition_offset_client::PartitionOffsetClient;
use super::transport::KafkaTransport;
use super::partition_offset_reader::GenericOffsetReader;

pub const UNKNOWN_BROKER: &str = "unknown broker";
pub const CHOSEN_BROKER_DIED: &str =
    "the internal broker struct chosen to issue this request has died--either the broker id is migrating or no longer exists";

pub const INITIAL_BYTES_PER_RECORD: usize = 10_000;
pub const FORCED_MIN_VALUE_FOR_MAX_BYTES: i32 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeErrorPolicy {
    ResumeFromStart,
    Abort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetRange {
    pub start: i64,
    pub end: i64,
}

impl OffsetRange {
    pub fn num_offsets(&self) -> i64 {
        self.end - self.start
    }
}

/// Finds offset gaps in a sequence of fetched records relative to `last_seen_offset`.
/// Ported from `findGapsInRecords` in `pkg/storage/ingest/fetcher.go`.
pub fn find_gaps_in_records(records: &[FetchedRecord], mut last_seen_offset: i64) -> Vec<OffsetRange> {
    let mut gaps = Vec::new();
    if last_seen_offset < 0 && !records.is_empty() {
        last_seen_offset = records[0].offset;
    }
    for r in records {
        if r.offset > last_seen_offset + 1 {
            gaps.push(OffsetRange {
                start: last_seen_offset + 1,
                end: r.offset,
            });
        }
        last_seen_offset = r.offset;
    }
    gaps
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchWant {
    pub start_offset: i64,
    pub end_offset: i64,
    pub estimated_bytes_per_record: usize,
    pub max_bytes_limit: i32,
}

impl FetchWant {
    pub fn from_offset(
        offset: i64,
        max_bytes_limit: i32,
        estimated_bytes_per_record: usize,
    ) -> Self {
        let est = estimated_bytes_per_record.max(1);
        let limit = max_bytes_limit.max(1);
        let estimated_num_records = (limit as usize / est).max(1) as i64;
        Self {
            start_offset: offset,
            end_offset: offset.saturating_add(estimated_num_records),
            estimated_bytes_per_record: est,
            max_bytes_limit: limit,
        }
    }

    pub fn next(&self) -> Self {
        let mut n = Self::from_offset(
            self.end_offset,
            self.max_bytes_limit,
            self.estimated_bytes_per_record,
        );
        n.estimated_bytes_per_record = self.estimated_bytes_per_record;
        n
    }

    /// Computes MaxBytes for a Fetch request, capping overflow to `i32::MAX`,
    /// enforcing a 1MB minimum (`FORCED_MIN_VALUE_FOR_MAX_BYTES`), and respecting `max_bytes_limit`.
    pub fn max_bytes(&self) -> i32 {
        const OVER_FETCH_BYTES_FACTOR: f64 = 1.05;
        let span = self.end_offset.saturating_sub(self.start_offset).max(0) as f64;
        let expected_f = OVER_FETCH_BYTES_FACTOR * (self.estimated_bytes_per_record as f64) * span;
        let fetch_bytes: i32 = if !expected_f.is_finite()
            || expected_f >= i32::MAX as f64
            || expected_f < 0.0
        {
            i32::MAX
        } else {
            expected_f as i32
        };
        let max_bytes = fetch_bytes.max(FORCED_MIN_VALUE_FOR_MAX_BYTES);
        max_bytes.min(self.max_bytes_limit)
    }

    pub fn update_bytes_per_record(
        &self,
        last_fetch_bytes: usize,
        last_fetch_records: usize,
    ) -> Self {
        if last_fetch_records == 0 {
            return self.clone();
        }
        const CURRENT_ESTIMATE_WEIGHT: f64 = 0.8;
        let actual_bytes_per_record = (last_fetch_bytes as f64) / (last_fetch_records as f64);
        let next_f = CURRENT_ESTIMATE_WEIGHT * (self.estimated_bytes_per_record as f64)
            + (1.0 - CURRENT_ESTIMATE_WEIGHT) * actual_bytes_per_record;
        let est = if !next_f.is_finite() || next_f >= usize::MAX as f64 {
            usize::MAX
        } else {
            (next_f as usize).max(1)
        };
        let mut updated = self.clone();
        updated.estimated_bytes_per_record = est;
        updated
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum KafkaFetchError {
    #[error("kafka broker error: {0}")]
    Broker(ResponseError),
    #[error("unknown partition leader")]
    UnknownPartitionLeader,
    #[error("first read EOF")]
    FirstReadEOF,
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone)]
pub struct FetchResult {
    pub partition: i32,
    pub records: Vec<FetchedRecord>,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub fetched_bytes: usize,
    pub err: Option<KafkaFetchError>,
}

impl FetchResult {
    pub fn empty(partition_id: i32) -> Self {
        Self {
            partition: partition_id,
            records: Vec::new(),
            high_watermark: -1,
            log_start_offset: -1,
            fetched_bytes: 0,
            err: None,
        }
    }

    /// Creates an error `FetchResult`. Panics if `err` is `None` (matching Go `newErrorFetchResult`).
    pub fn error(partition_id: i32, err: Option<KafkaFetchError>) -> Self {
        let Some(err) = err else {
            panic!("an error must be provided to newErrorFetchResult()");
        };
        Self {
            partition: partition_id,
            records: Vec::new(),
            high_watermark: -1,
            log_start_offset: -1,
            fetched_bytes: 0,
            err: Some(err),
        }
    }

    /// Merges `older` into `self` (prepending `older.records`).
    /// Panics if either `self.err` or `older.err` is `Some`.
    pub fn merge(mut self, older: FetchResult) -> Self {
        if self.err.is_some() {
            panic!("fetchResult.Merge() has been called on a fetchResult with Err set");
        }
        if older.err.is_some() {
            panic!("fetchResult.Merge() has been called with older fetchResult with Err set");
        }
        let mut combined = older.records;
        combined.extend(self.records);
        self.records = combined;
        self.fetched_bytes = self.fetched_bytes.saturating_add(older.fetched_bytes);
        self
    }
}

pub trait FetchWaiter: Send + Sync {
    fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

pub trait MetadataRefresher: Send + Sync {
    fn force_metadata_refresh(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

struct BackoffWaiter(SyncMutex<Backoff>);

impl FetchWaiter for BackoffWaiter {
    fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let dur = {
            let mut b = self.0.lock();
            b.next_delay()
        };
        Box::pin(async move {
            tokio::time::sleep(dur).await;
        })
    }
}

/// Handles a Kafka fetch error according to `RangeErrorPolicy`, triggering backoff and/or
/// metadata refresh and adjusting `FetchWant` on `OffsetOutOfRange`.
/// Ported from `handleKafkaFetchErr` in `pkg/storage/ingest/fetcher.go`.
pub async fn handle_kafka_fetch_err(
    err: Option<&KafkaFetchError>,
    mut fw: FetchWant,
    long_backoff: &dyn FetchWaiter,
    range_error_policy: RangeErrorPolicy,
    partition_start_offset: &GenericOffsetReader<i64>,
    refresher: &dyn MetadataRefresher,
) -> Result<FetchWant, KafkaFetchError> {
    let Some(err) = err else {
        return Ok(fw);
    };

    match err {
        KafkaFetchError::Broker(ResponseError::OffsetOutOfRange) => {
            match range_error_policy {
                RangeErrorPolicy::Abort => return Err(err.clone()),
                RangeErrorPolicy::ResumeFromStart => {
                    match partition_start_offset.cached_offset() {
                        Some(Ok(partition_start)) => {
                            if fw.start_offset < partition_start {
                                if partition_start >= fw.end_offset {
                                    fw.start_offset = fw.end_offset;
                                    debug!(
                                        log_start_offset = partition_start,
                                        start_offset = fw.start_offset,
                                        end_offset = fw.end_offset,
                                        "we're too far behind aborting fetch"
                                    );
                                } else {
                                    fw.start_offset = partition_start;
                                    debug!(
                                        log_start_offset = partition_start,
                                        start_offset = fw.start_offset,
                                        end_offset = fw.end_offset,
                                        "part of fetch want is outside of available offsets, adjusted start offset"
                                    );
                                }
                            } else {
                                debug!(
                                    log_start_offset = partition_start,
                                    start_offset = fw.start_offset,
                                    "offset out of range; waiting for new records to be produced"
                                );
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "failed to find start offset to readjust on OffsetOutOfRange; retrying same records range");
                        }
                        None => {
                            error!("start offset not yet cached on OffsetOutOfRange; retrying same records range");
                        }
                    }
                }
            }
        }
        KafkaFetchError::Broker(
            ResponseError::TopicAuthorizationFailed
            | ResponseError::UnknownTopicOrPartition
            | ResponseError::UnsupportedCompressionType
            | ResponseError::UnsupportedVersion
            | ResponseError::KafkaStorageError
            | ResponseError::UnknownTopicId,
        )
        | KafkaFetchError::FirstReadEOF => {
            long_backoff.wait().await;
        }
        KafkaFetchError::Other(msg) if msg.contains("OFFSET_MOVED_TO_TIERED_STORAGE") => {
            long_backoff.wait().await;
        }
        KafkaFetchError::Broker(
            ResponseError::NotLeaderOrFollower
            | ResponseError::ReplicaNotAvailable
            | ResponseError::UnknownLeaderEpoch
            | ResponseError::FencedLeaderEpoch
            | ResponseError::LeaderNotAvailable
            | ResponseError::BrokerNotAvailable,
        )
        | KafkaFetchError::UnknownPartitionLeader => {
            refresher.force_metadata_refresh().await;
            long_backoff.wait().await;
        }
        KafkaFetchError::Other(msg) => {
            if msg.contains(UNKNOWN_BROKER)
                || msg.contains(CHOSEN_BROKER_DIED)
                || msg.contains("use of closed network connection")
            {
                // Immediate retry without backoff or metadata refresh.
            } else if msg.contains("i/o timeout") {
                refresher.force_metadata_refresh().await;
                long_backoff.wait().await;
            } else {
                error!(
                    error = %msg,
                    "received an error we're not prepared to handle; this shouldn't happen; please report this as a bug"
                );
                long_backoff.wait().await;
            }
        }
        KafkaFetchError::Broker(other) => {
            error!(
                error = %other,
                "received an error we're not prepared to handle; this shouldn't happen; please report this as a bug"
            );
            long_backoff.wait().await;
        }
    }

    Ok(fw)
}

#[derive(Clone)]
pub struct FetcherMetrics {
    pub fetch_max_bytes: Histogram,
    pub fetch_wait_duration: Histogram,
    pub fetched_discarded_record_bytes: Counter,
}

impl FetcherMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let fetch_max_bytes = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_reader_records_batch_fetch_max_bytes",
            "MaxBytes requested in Fetch requests.",
        ))
        .expect("valid metric");
        let fetch_wait_duration = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingest_storage_reader_fetch_wait_duration_seconds",
            "Time spent waiting for ordered fetch results.",
        ))
        .expect("valid metric");
        let fetched_discarded_record_bytes = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_fetched_discarded_record_bytes_total",
            "Total bytes of fetched records discarded because they were already returned.",
        ))
        .expect("valid metric");

        if let Some(reg) = registry {
            let _ = reg.register(Box::new(fetch_max_bytes.clone()));
            let _ = reg.register(Box::new(fetch_wait_duration.clone()));
            let _ = reg.register(Box::new(fetched_discarded_record_bytes.clone()));
        }

        Self {
            fetch_max_bytes,
            fetch_wait_duration,
            fetched_discarded_record_bytes,
        }
    }
}

/// Raw TCP broker connection for `ConcurrentFetchers`, supporting Fetch v13 (topicID) and Metadata.
struct FetchBrokerClient {
    seed_addrs: Vec<String>,
    client_id: String,
    transport: Arc<KafkaTransport>,
    request_timeout: Duration,
    topic_name: String,
    topic_template: SyncMutex<Option<FetchTopic>>,
    state: Mutex<Option<FetchBrokerState>>,
}

struct FetchBrokerState {
    conn: Arc<MuxConn>,
}

struct MuxConn {
    client_id: String,
    next_corr_id: AtomicI32,
    write_tx: mpsc::Sender<Bytes>,
    pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>>,
    alive: Arc<std::sync::atomic::AtomicBool>,
    /// SASL session expiry: past it the connection is replaced instead of reused.
    sasl_expiry: Option<std::time::Instant>,
}

impl MuxConn {
    async fn connect(
        addr: &str,
        client_id: String,
        transport: &KafkaTransport,
        request_timeout: Duration,
    ) -> Result<Arc<Self>, String> {
        let established = transport
            .connect(addr, &client_id, request_timeout)
            .await
            .map_err(|e| format!("connect error to {addr}: {e}"))?;

        let (mut reader, mut writer) = established.stream.into_split();
        let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(256);
        let pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>> =
            Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let alive_w = Arc::clone(&alive);
        tokio::spawn(async move {
            while let Some(frame) = write_rx.recv().await {
                if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
                    break;
                }
            }
            alive_w.store(false, Ordering::SeqCst);
        });

        let pending_r = Arc::clone(&pending);
        let alive_r = Arc::clone(&alive);
        tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];
                if reader.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let frame_len = i32::from_be_bytes(len_buf) as usize;
                if !(4..=256 * 1024 * 1024).contains(&frame_len) {
                    break;
                }
                let mut body = vec![0u8; frame_len];
                if reader.read_exact(&mut body).await.is_err() {
                    break;
                }
                let corr_id = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let tx = pending_r.lock().remove(&corr_id);
                if let Some(tx) = tx {
                    let _ = tx.send(Bytes::from(body));
                }
            }
            alive_r.store(false, Ordering::SeqCst);
            let drained: Vec<_> = pending_r.lock().drain().map(|(_, v)| v).collect();
            for tx in drained {
                drop(tx);
            }
        });

        Ok(Arc::new(Self {
            client_id,
            next_corr_id: AtomicI32::new(established.next_correlation_id),
            write_tx,
            pending,
            alive,
            sasl_expiry: established.sasl_expiry,
        }))
    }

    /// Alive and its SASL session (if any) has not expired: may be handed out for new requests.
    fn is_usable(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
            && self
                .sasl_expiry
                .is_none_or(|e| std::time::Instant::now() < e)
    }

    async fn send<Req: Request + Encodable, Resp: Decodable + HeaderVersion>(
        &self,
        api_key: ApiKey,
        ver: i16,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, KafkaFetchError> {
        if !self.alive.load(Ordering::SeqCst) {
            return Err(KafkaFetchError::Other(
                "use of closed network connection".to_string(),
            ));
        }
        let corr_id = self.next_corr_id.fetch_add(1, Ordering::SeqCst);
        let req_header_ver = Req::header_version(ver);
        let mut hdr = RequestHeader::default();
        hdr.request_api_key = api_key as i16;
        hdr.request_api_version = ver;
        hdr.correlation_id = corr_id;
        hdr.client_id = Some(StrBytes::from_string(self.client_id.clone()));

        let mut payload = bytes::BytesMut::new();
        hdr.encode(&mut payload, req_header_ver)
            .map_err(|e| KafkaFetchError::Other(e.to_string()))?;
        req.encode(&mut payload, ver)
            .map_err(|e| KafkaFetchError::Other(e.to_string()))?;

        let mut frame = bytes::BytesMut::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        frame.extend_from_slice(&payload);

        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(corr_id, tx);
        if self.write_tx.send(frame.freeze()).await.is_err() {
            self.pending.lock().remove(&corr_id);
            return Err(KafkaFetchError::Other(
                "use of closed network connection".to_string(),
            ));
        }

        let mut resp_buf = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| {
                self.pending.lock().remove(&corr_id);
                KafkaFetchError::Other("i/o timeout".to_string())
            })?
            .map_err(|_| KafkaFetchError::Other("use of closed network connection".to_string()))?;

        let resp_header_ver = Resp::header_version(ver);
        let _hdr = ResponseHeader::decode(&mut resp_buf, resp_header_ver)
            .map_err(|e| KafkaFetchError::Other(e.to_string()))?;
        Resp::decode(&mut resp_buf, ver).map_err(|e| KafkaFetchError::Other(e.to_string()))
    }
}

impl FetchBrokerClient {
    async fn conn(&self) -> Result<Arc<MuxConn>, KafkaFetchError> {
        let mut guard = self.state.lock().await;
        if let Some(ref st) = *guard {
            if st.conn.is_usable() {
                return Ok(Arc::clone(&st.conn));
            }
        }
        for addr in &self.seed_addrs {
            if let Ok(conn) = MuxConn::connect(
                addr,
                self.client_id.clone(),
                &self.transport,
                self.request_timeout,
            )
            .await
            {
                *guard = Some(FetchBrokerState {
                    conn: Arc::clone(&conn),
                });
                return Ok(conn);
            }
        }
        Err(KafkaFetchError::Other(UNKNOWN_BROKER.to_string()))
    }

    async fn fetch_metadata(&self) -> Result<[u8; 16], String> {
        let conn = self.conn().await.map_err(|e| format!("failed to find topic ID: {e}"))?;
        let mut req = MetadataRequest::default();
        req.allow_auto_topic_creation = false;
        let mut mt = MetadataRequestTopic::default();
        mt.name = Some(TopicName(StrBytes::from_string(self.topic_name.clone())));
        req.topics = Some(vec![mt]);

        let resp: MetadataResponse = conn
            .send(ApiKey::Metadata, 10, &req, self.request_timeout)
            .await
            .map_err(|e| format!("failed to find topic ID: {e}"))?;

        for t in resp.topics {
            let matches_name = t
                .name
                .as_ref()
                .map(|n| n.0.as_str() == self.topic_name)
                .unwrap_or(false);
            if !matches_name {
                continue;
            }
            if let Some(err) = t.error_code.err() {
                return Err(format!("failed to find topic ID: {err}"));
            }
            let bytes = *t.topic_id.as_bytes();
            let ft = FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(self.topic_name.clone())))
                .with_topic_id(t.topic_id);
            *self.topic_template.lock() = Some(ft);
            return Ok(bytes);
        }
        Err("failed to find topic ID: topic not found".to_string())
    }
}

impl MetadataRefresher for FetchBrokerClient {
    fn force_metadata_refresh(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let _ = self.fetch_metadata().await;
        })
    }
}

/// Concurrent Kafka partition fetcher maintaining ordered record delivery across parallel workers.
pub struct ConcurrentFetchers {
    broker: Arc<FetchBrokerClient>,
    topic_name: String,
    topic_id: [u8; 16],
    partition_id: i32,
    start_offset: i64,
    start_concurrency: usize,
    max_buffered_bytes_limit: i32,
    track_compressed_bytes: bool,
    min_bytes_wait_time: Duration,
    range_error_policy: RangeErrorPolicy,
    start_offsets_reader: Arc<GenericOffsetReader<i64>>,
    fetch_backoff_config: BackoffConfig,
    metrics: FetcherMetrics,

    buffered_fetched_records: Arc<AtomicI64>,
    buffered_fetched_bytes: Arc<AtomicI64>,
    estimated_bytes_per_record: Arc<AtomicI64>,
    last_returned_offset: Arc<AtomicI64>,

    ordered_rx: Mutex<mpsc::Receiver<FetchResult>>,
    ordered_tx: mpsc::Sender<FetchResult>,
    stop_tx: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl ConcurrentFetchers {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        seed_addrs: Vec<String>,
        client_id: impl Into<String>,
        topic: impl Into<String>,
        partition_id: i32,
        start_offset: i64,
        concurrency: usize,
        max_buffered_bytes_limit: i32,
        track_compressed_bytes: bool,
        min_bytes_wait_time: Duration,
        offset_reader: &PartitionOffsetClient,
        range_error_policy: RangeErrorPolicy,
        start_offsets_reader: Arc<GenericOffsetReader<i64>>,
        fetch_backoff_config: BackoffConfig,
        metrics: FetcherMetrics,
    ) -> Result<Self, String> {
        if fetch_backoff_config.max_backoff.is_zero() {
            return Err("fetch_backoff_config.max_backoff has not been set".to_string());
        }
        if fetch_backoff_config.max_retries != 0 {
            return Err("fetch_backoff_config.max_retries must be 0".to_string());
        }

        let topic_name = topic.into();
        let resolved_start = match start_offset {
            KAFKA_OFFSET_START => offset_reader
                .fetch_partition_start_offset(&topic_name, partition_id)
                .await
                .map_err(|e| format!("resolving offset to start consuming from: {e}"))?,
            KAFKA_OFFSET_END => {
                let last = offset_reader
                    .fetch_partition_last_produced_offset(&topic_name, partition_id)
                    .await
                    .map_err(|e| format!("resolving offset to start consuming from: {e}"))?;
                last + 1
            }
            other => other,
        };

        let max_limit = if max_buffered_bytes_limit <= 0 {
            i32::MAX
        } else {
            max_buffered_bytes_limit
        };

        let broker = Arc::new(FetchBrokerClient {
            seed_addrs,
            client_id: client_id.into(),
            // Dial, TLS and SASL settings of the offset client's Kafka client (same KafkaConfig).
            transport: Arc::clone(offset_reader.kafka_client().transport()),
            request_timeout: Duration::from_secs(10).max(min_bytes_wait_time + Duration::from_secs(5)),
            topic_name: topic_name.clone(),
            topic_template: SyncMutex::new(None),
            state: Mutex::new(None),
        });

        let topic_id = broker.fetch_metadata().await?;

        let (ordered_tx, ordered_rx) = mpsc::channel(1);
        let (stop_tx, _) = watch::channel(false);

        Ok(Self {
            broker,
            topic_name,
            topic_id,
            partition_id,
            start_offset: resolved_start,
            start_concurrency: concurrency.max(1),
            max_buffered_bytes_limit: max_limit,
            track_compressed_bytes,
            min_bytes_wait_time,
            range_error_policy,
            start_offsets_reader,
            fetch_backoff_config,
            metrics,
            buffered_fetched_records: Arc::new(AtomicI64::new(0)),
            buffered_fetched_bytes: Arc::new(AtomicI64::new(0)),
            estimated_bytes_per_record: Arc::new(AtomicI64::new(INITIAL_BYTES_PER_RECORD as i64)),
            last_returned_offset: Arc::new(AtomicI64::new(resolved_start - 1)),
            ordered_rx: Mutex::new(ordered_rx),
            ordered_tx,
            stop_tx,
            task: Mutex::new(None),
        })
    }

    pub fn topic_id(&self) -> [u8; 16] {
        self.topic_id
    }

    pub fn partition_id(&self) -> i32 {
        self.partition_id
    }

    pub fn buffered_records(&self) -> i64 {
        self.buffered_fetched_records.load(Ordering::SeqCst)
    }

    pub fn buffered_bytes(&self) -> i64 {
        self.buffered_fetched_bytes.load(Ordering::SeqCst)
    }

    pub fn estimated_bytes_per_record(&self) -> i64 {
        self.estimated_bytes_per_record.load(Ordering::SeqCst)
    }

    /// Starts the coordinator and worker tasks.
    pub async fn start(&self) {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return;
        }

        let concurrency = self.start_concurrency;
        let max_bytes_per_fetcher = (self.max_buffered_bytes_limit / concurrency as i32).max(1);
        let high_watermark = Arc::new(AtomicI64::new(0));
        let (wants_tx, wants_rx) = mpsc::channel::<(FetchWant, mpsc::Sender<FetchResult>)>(1);
        let wants_rx = Arc::new(Mutex::new(wants_rx));

        let mut worker_handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let rx = Arc::clone(&wants_rx);
            let broker = Arc::clone(&self.broker);
            let topic_name = self.topic_name.clone();
            let topic_id = self.topic_id;
            let partition_id = self.partition_id;
            let track_compressed = self.track_compressed_bytes;
            let min_wait = self.min_bytes_wait_time;
            let range_policy = self.range_error_policy;
            let start_offsets_reader = Arc::clone(&self.start_offsets_reader);
            let backoff_cfg = self.fetch_backoff_config;
            let metrics = self.metrics.clone();
            let buffered_recs = Arc::clone(&self.buffered_fetched_records);
            let hwm = Arc::clone(&high_watermark);
            let mut stop_rx = self.stop_tx.subscribe();

            let h = tokio::spawn(async move {
                loop {
                    if *stop_rx.borrow() {
                        break;
                    }
                    let item = {
                        let mut locked = rx.lock().await;
                        tokio::select! {
                            biased;
                            _ = stop_rx.changed() => None,
                            res = locked.recv() => res,
                        }
                    };
                    let Some((mut fw, res_tx)) = item else {
                        break;
                    };

                    let backoff_waiter = BackoffWaiter(SyncMutex::new(Backoff::new(backoff_cfg)));
                    let mut buffered = FetchResult::empty(partition_id);

                    while fw.end_offset > fw.start_offset {
                        if *stop_rx.borrow() {
                            break;
                        }
                        let single = tokio::select! {
                            biased;
                            _ = stop_rx.changed() => break,
                            s = fetch_single_inner(
                                &broker,
                                &topic_name,
                                topic_id,
                                partition_id,
                                &fw,
                                min_wait,
                                track_compressed,
                                &metrics,
                            ) => s,
                        };

                        if let Some(ref err) = single.err {
                            match handle_kafka_fetch_err(
                                Some(err),
                                fw.clone(),
                                &backoff_waiter,
                                range_policy,
                                &start_offsets_reader,
                                broker.as_ref(),
                            )
                            .await
                            {
                                Ok(adjusted) => {
                                    fw = adjusted;
                                }
                                Err(fatal) => {
                                    let _ = tokio::select! {
                                        biased;
                                        _ = stop_rx.changed() => Ok(()),
                                        sent = res_tx.send(FetchResult::error(partition_id, Some(fatal))) => sent.map_err(|_| ()),
                                    };
                                    break;
                                }
                            }
                        } else {
                            buffered_recs.fetch_add(single.records.len() as i64, Ordering::SeqCst);
                            if single.high_watermark >= 0 {
                                cas_hwm(&hwm, single.high_watermark);
                            }
                            buffered = single.merge(buffered);
                        }

                        if buffered.records.is_empty() {
                            continue;
                        }

                        let last_off = buffered.records.last().unwrap().offset;
                        fw.start_offset = last_off + 1;
                        fw = fw.update_bytes_per_record(buffered.fetched_bytes, buffered.records.len());

                        // Reset backoff after progress.
                        backoff_waiter.0.lock().reset();

                        match res_tx.try_send(buffered) {
                            Ok(()) => {
                                buffered = FetchResult::empty(partition_id);
                            }
                            Err(mpsc::error::TrySendError::Full(b)) => {
                                if fw.start_offset >= fw.end_offset {
                                    tokio::select! {
                                        biased;
                                        _ = stop_rx.changed() => break,
                                        _ = res_tx.send(b) => {}
                                    }
                                    buffered = FetchResult::empty(partition_id);
                                } else {
                                    buffered = b;
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                break;
                            }
                        }
                    }
                    drop(res_tx);
                }
            });
            worker_handles.push(h);
        }

        let start_offset = self.start_offset;
        let max_buffered_bytes_limit = self.max_buffered_bytes_limit;
        let buffered_bytes = Arc::clone(&self.buffered_fetched_bytes);
        let est_bpr = Arc::clone(&self.estimated_bytes_per_record);
        let ordered_tx = self.ordered_tx.clone();
        let mut stop_rx = self.stop_tx.subscribe();

        let coord = tokio::spawn(async move {
            let mut next_fetch = FetchWant::from_offset(
                start_offset,
                max_bytes_per_fetcher,
                INITIAL_BYTES_PER_RECORD,
            );
            let mut inflight: VecDeque<(FetchWant, mpsc::Receiver<FetchResult>)> = VecDeque::new();
            let mut ready_result: Option<FetchResult> = None;

            loop {
                est_bpr.store(next_fetch.estimated_bytes_per_record as i64, Ordering::SeqCst);

                if *stop_rx.borrow() {
                    break;
                }

                let would_exceed = buffered_bytes.load(Ordering::SeqCst)
                    + (next_fetch.max_bytes() as i64)
                    > (max_buffered_bytes_limit as i64);
                let can_dispatch = inflight.is_empty()
                    || (!would_exceed && next_fetch.start_offset <= high_watermark.load(Ordering::SeqCst));

                if let Some(res_to_send) = ready_result.take() {
                    tokio::select! {
                        biased;
                        _ = stop_rx.changed() => break,
                        sent = ordered_tx.send(res_to_send) => {
                            if sent.is_err() {
                                break;
                            }
                        }
                    }
                    continue;
                }

                if can_dispatch && inflight.len() < concurrency {
                    let (res_tx, res_rx) = mpsc::channel(1);
                    let fw_to_send = next_fetch.clone();
                    tokio::select! {
                        biased;
                        _ = stop_rx.changed() => break,
                        sent = wants_tx.send((fw_to_send.clone(), res_tx)) => {
                            if sent.is_err() {
                                break;
                            }
                            buffered_bytes.fetch_add(fw_to_send.max_bytes() as i64, Ordering::SeqCst);
                            inflight.push_back((fw_to_send, res_rx));
                            next_fetch = next_fetch.next();
                            continue;
                        }
                        maybe_res = async {
                            match inflight.front_mut() {
                                Some((_, rx)) => rx.recv().await,
                                None => std::future::pending().await,
                            }
                        } => {
                            match maybe_res {
                                Some(res) => {
                                    if res.err.is_none() && !res.records.is_empty() {
                                        next_fetch = next_fetch.update_bytes_per_record(res.fetched_bytes, res.records.len());
                                    }
                                    ready_result = Some(res);
                                }
                                None => {
                                    if let Some((fw, _)) = inflight.pop_front() {
                                        buffered_bytes.fetch_sub(fw.max_bytes() as i64, Ordering::SeqCst);
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }

                if let Some((_, rx)) = inflight.front_mut() {
                    let maybe_res = tokio::select! {
                        biased;
                        _ = stop_rx.changed() => break,
                        r = rx.recv() => r,
                    };
                    match maybe_res {
                        Some(res) => {
                            if res.err.is_none() && !res.records.is_empty() {
                                next_fetch = next_fetch.update_bytes_per_record(res.fetched_bytes, res.records.len());
                            }
                            ready_result = Some(res);
                        }
                        None => {
                            if let Some((fw, _)) = inflight.pop_front() {
                                buffered_bytes.fetch_sub(fw.max_bytes() as i64, Ordering::SeqCst);
                            }
                        }
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }

            drop(wants_tx);
            for h in worker_handles {
                let _ = h.await;
            }
        });

        *guard = Some(coord);
    }

    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        if let Some(h) = self.task.lock().await.take() {
            let _ = h.await;
        }
        self.buffered_fetched_records.store(0, Ordering::SeqCst);
        self.buffered_fetched_bytes.store(0, Ordering::SeqCst);
    }

    /// Polls the next ordered fetch batch, skipping any records already returned (`<= last_returned_offset`).
    /// Returns `None` if `timeout` elapses or the fetcher is stopped.
    pub async fn poll_fetches(&self, timeout: Duration) -> Option<FetchResult> {
        let mut rx = self.ordered_rx.lock().await;
        let fut = rx.recv();
        let res = if timeout.is_zero() {
            fut.await
        } else {
            tokio::time::timeout(timeout, fut).await.ok()?
        };
        let mut f = res?;

        let raw_len = f.records.len() as i64;
        self.buffered_fetched_records
            .fetch_sub(raw_len, Ordering::SeqCst);

        let last_ret = self.last_returned_offset.load(Ordering::SeqCst);
        let first_new = f
            .records
            .iter()
            .position(|r| r.offset > last_ret)
            .unwrap_or(f.records.len());
        if first_new > 0 {
            let discarded_bytes: usize = f.records[..first_new].iter().map(|r| r.value.len()).sum();
            self.metrics
                .fetched_discarded_record_bytes
                .inc_by(discarded_bytes as f64);
            f.records.drain(..first_new);
        }
        if let Some(last) = f.records.last() {
            self.last_returned_offset.store(last.offset, Ordering::SeqCst);
        }
        Some(f)
    }

    /// Issues a single Fetch request for `fw` without starting the background worker loop.
    pub async fn fetch_single(&self, fw: &FetchWant) -> FetchResult {
        fetch_single_inner(
            &self.broker,
            &self.topic_name,
            self.topic_id,
            self.partition_id,
            fw,
            self.min_bytes_wait_time,
            self.track_compressed_bytes,
            &self.metrics,
        )
        .await
    }

    /// Issues a single Fetch request that returns early if `cancel_rx` fires.
    pub async fn fetch_single_cancellable(
        &self,
        fw: &FetchWant,
        mut cancel_rx: watch::Receiver<bool>,
    ) -> FetchResult {
        if *cancel_rx.borrow() {
            return FetchResult::empty(self.partition_id);
        }
        tokio::select! {
            biased;
            _ = cancel_rx.changed() => FetchResult::empty(self.partition_id),
            res = self.fetch_single(fw) => res,
        }
    }

    /// Parses and validates a raw `FetchResponse`.
    pub fn parse_fetch_response(&self, start_offset: i64, resp: &FetchResponse) -> FetchResult {
        parse_fetch_response_inner(
            &self.topic_name,
            self.topic_id,
            self.partition_id,
            start_offset,
            self.track_compressed_bytes,
            resp,
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_single_inner(
    broker: &FetchBrokerClient,
    topic_name: &str,
    topic_id: [u8; 16],
    partition_id: i32,
    fw: &FetchWant,
    min_bytes_wait_time: Duration,
    track_compressed_bytes: bool,
    metrics: &FetcherMetrics,
) -> FetchResult {
    let conn = match broker.conn().await {
        Ok(c) => c,
        Err(e) => return FetchResult::error(partition_id, Some(e)),
    };

    let mut fp = FetchPartition::default();
    fp.partition = partition_id;
    fp.current_leader_epoch = -1;
    fp.fetch_offset = fw.start_offset;
    fp.partition_max_bytes = fw.max_bytes();

    let mut ft = broker
        .topic_template
        .lock()
        .clone()
        .unwrap_or_else(|| FetchTopic::default().with_topic(TopicName(StrBytes::from_string(topic_name.to_string()))));
    ft.partitions = vec![fp];

    let mut req = FetchRequest::default();
    req.replica_id = BrokerId(-1);
    req.min_bytes = 1;
    req.max_wait_ms = min_bytes_wait_time.as_millis().min(i32::MAX as u128) as i32;
    req.max_bytes = fw.max_bytes();
    req.isolation_level = 0;
    req.session_id = 0;
    req.session_epoch = -1;
    req.topics = vec![ft];

    metrics.fetch_max_bytes.observe(req.max_bytes as f64);

    match conn.send::<FetchRequest, FetchResponse>(ApiKey::Fetch, 13, &req, broker.request_timeout).await {
        Ok(resp) => parse_fetch_response_inner(
            topic_name,
            topic_id,
            partition_id,
            fw.start_offset,
            track_compressed_bytes,
            &resp,
        ),
        Err(e) => FetchResult::error(partition_id, Some(e)),
    }
}

fn parse_fetch_response_inner(
    _topic_name: &str,
    expected_topic_id: [u8; 16],
    expected_partition_id: i32,
    start_offset: i64,
    track_compressed_bytes: bool,
    resp: &FetchResponse,
) -> FetchResult {
    if resp.error_code != 0 {
        return FetchResult::error(
            expected_partition_id,
            Some(KafkaFetchError::Other(format!(
                "received error code {}",
                resp.error_code
            ))),
        );
    }

    if resp.responses.len() != 1 {
        return FetchResult::error(
            expected_partition_id,
            Some(KafkaFetchError::Other(format!(
                "unexpected number of topics in the Fetch response (expected: 1 got: {})",
                resp.responses.len()
            ))),
        );
    }

    let topic_resp = &resp.responses[0];
    if *topic_resp.topic_id.as_bytes() != expected_topic_id {
        return FetchResult::error(
            expected_partition_id,
            Some(KafkaFetchError::Other(format!(
                "unexpected topic ID in the Fetch response (expected: {:?} got: {:?})",
                expected_topic_id,
                topic_resp.topic_id.as_bytes()
            ))),
        );
    }

    if topic_resp.partitions.len() != 1 {
        return FetchResult::error(
            expected_partition_id,
            Some(KafkaFetchError::Other(format!(
                "unexpected number of partitions in the Fetch response (expected: 1 got: {})",
                topic_resp.partitions.len()
            ))),
        );
    }

    let part_resp = &topic_resp.partitions[0];
    if part_resp.partition_index != expected_partition_id {
        return FetchResult::error(
            expected_partition_id,
            Some(KafkaFetchError::Other(format!(
                "unexpected partition ID in the Fetch response (expected: {} got: {})",
                expected_partition_id, part_resp.partition_index
            ))),
        );
    }

    if let Some(err) = part_resp.error_code.err() {
        return FetchResult::error(expected_partition_id, Some(KafkaFetchError::Broker(err)));
    }

    let mut records = Vec::new();
    let mut raw_len = 0usize;
    if let Some(ref raw) = part_resp.records {
        raw_len = raw.len();
        if !raw.is_empty() {
            let mut slice = raw.clone();
            match RecordBatchDecoder::decode_with_custom_compression(
                &mut slice,
                Some(decompress_record_batch),
            ) {
                Ok(batch) => {
                    for rec in batch {
                        if rec.control || rec.offset < start_offset {
                            continue;
                        }
                        let headers = rec
                            .headers
                            .into_iter()
                            .map(|(k, v)| (k.as_str().to_owned(), v.unwrap_or_default()))
                            .collect();
                        records.push(FetchedRecord {
                            offset: rec.offset,
                            timestamp_ms: rec.timestamp,
                            key: rec.key.unwrap_or_default(),
                            value: rec.value.unwrap_or_default(),
                            headers,
                        });
                    }
                }
                Err(e) => {
                    return FetchResult::error(
                        expected_partition_id,
                        Some(KafkaFetchError::Other(format!("decode batch: {e}"))),
                    );
                }
            }
        }
    }

    let fetched_bytes = if track_compressed_bytes {
        raw_len
    } else {
        records.iter().map(|r| r.value.len()).sum()
    };

    FetchResult {
        partition: expected_partition_id,
        records,
        high_watermark: part_resp.high_watermark,
        log_start_offset: part_resp.log_start_offset,
        fetched_bytes,
        err: None,
    }
}

fn cas_hwm(hwm: &AtomicI64, new_hwm: i64) {
    let mut cur = hwm.load(Ordering::SeqCst);
    while cur < new_hwm {
        match hwm.compare_exchange_weak(cur, new_hwm, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}
