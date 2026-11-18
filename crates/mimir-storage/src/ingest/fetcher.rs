//! Concurrent Kafka partition fetcher with adaptive byte estimation, ordered result
//! reassembly, gap detection, and error/backoff handling.
//!
//! Ported from github.com/grafana/mimir/pkg/storage/ingest/fetcher.go

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::error::{ParseResponseErrorCode, ResponseError};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{BrokerId, FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use mimir_common::backoff::{Backoff, BackoffConfig};
use parking_lot::Mutex as SyncMutex;
use prometheus::{Counter, Histogram, HistogramOpts, Opts, Registry};
use thiserror::Error;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error};

use super::kafka_client::{FetchedRecord, KafkaClient, KafkaClientError, KAFKA_OFFSET_END, KAFKA_OFFSET_START};
use super::partition_offset_client::PartitionOffsetClient;
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
    TransientTransport(String),
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
    fw: FetchWant,
    long_backoff: &dyn FetchWaiter,
    range_error_policy: RangeErrorPolicy,
    partition_start_offset: &GenericOffsetReader<i64>,
    refresher: &dyn MetadataRefresher,
) -> Result<FetchWant, KafkaFetchError> {
    handle_kafka_fetch_err_with_log_start(
        err,
        fw,
        long_backoff,
        range_error_policy,
        partition_start_offset,
        refresher,
        None,
    )
    .await
}

async fn handle_kafka_fetch_err_with_log_start(
    err: Option<&KafkaFetchError>,
    mut fw: FetchWant,
    long_backoff: &dyn FetchWaiter,
    range_error_policy: RangeErrorPolicy,
    partition_start_offset: &GenericOffsetReader<i64>,
    refresher: &dyn MetadataRefresher,
    response_log_start_offset: Option<i64>,
) -> Result<FetchWant, KafkaFetchError> {
    let Some(err) = err else {
        return Ok(fw);
    };

    match err {
        KafkaFetchError::Broker(ResponseError::OffsetOutOfRange) => {
            match range_error_policy {
                RangeErrorPolicy::Abort => return Err(err.clone()),
                RangeErrorPolicy::ResumeFromStart => {
                    let cached = partition_start_offset.cached_offset();
                    let resolved_start = match (response_log_start_offset.filter(|&o| o >= 0), cached) {
                        (Some(resp_lso), Some(Ok(cached_lso))) => Ok(Some(resp_lso.max(cached_lso))),
                        (Some(resp_lso), _) => Ok(Some(resp_lso)),
                        (None, Some(Ok(cached_lso))) => Ok(Some(cached_lso)),
                        (None, Some(Err(e))) => Err(Some(e)),
                        (None, None) => Err(None),
                    };
                    match resolved_start {
                        Ok(Some(partition_start)) => {
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
                                long_backoff.wait().await;
                            }
                        }
                        Err(Some(e)) => {
                            error!(err = %e, "failed to find start offset to readjust on OffsetOutOfRange; retrying same records range");
                            long_backoff.wait().await;
                        }
                        Err(None) | Ok(None) => {
                            error!("start offset not yet cached on OffsetOutOfRange; retrying same records range");
                            long_backoff.wait().await;
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
        | KafkaFetchError::UnknownPartitionLeader
        | KafkaFetchError::TransientTransport(_) => {
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
                    err = %msg,
                    "received an error we're not prepared to handle; this shouldn't happen; please report this as a bug"
                );
                long_backoff.wait().await;
            }
        }
        KafkaFetchError::Broker(other) => {
            error!(
                err = %other,
                "received an error we're not prepared to handle; this shouldn't happen; please report this as a bug"
            );
            long_backoff.wait().await;
        }
    }

    Ok(fw)
}

/// The [`ReaderMetrics`](super::reader::ReaderMetrics) series owned by `ConcurrentFetchers`
/// (`fetchMaxBytes`, `fetchWaitDuration`, `fetchedDiscardedRecordBytes` in `reader.go`).
#[derive(Clone)]
pub struct FetcherMetrics {
    pub fetch_max_bytes: Histogram,
    pub fetch_wait_duration: Histogram,
    pub fetched_discarded_record_bytes: Counter,
}

/// Classic buckets of a client_golang histogram that only sets `NativeHistogramBucketFactor`
/// (no `Buckets`): it has no classic bucket, so the text exposition only carries `le="+Inf"`.
/// The prometheus crate drops a trailing `+Inf` bound and keeps the implicit one.
pub(crate) fn native_only_buckets() -> Vec<f64> {
    vec![f64::INFINITY]
}

impl FetcherMetrics {
    pub fn new(registry: Option<&Registry>) -> Self {
        let fetch_max_bytes = Histogram::with_opts(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_records_batch_fetch_max_bytes",
                "The distribution of MaxBytes specified in the Fetch requests sent to Kafka.",
            )
            .buckets(native_only_buckets()),
        )
        .expect("valid metric");
        let fetch_wait_duration = Histogram::with_opts(
            HistogramOpts::new(
                "cortex_ingest_storage_reader_records_batch_wait_duration_seconds",
                "How long a consumer spent waiting for a batch of records from the Kafka client. If fetching is faster than processing, then this will be close to 0.",
            )
            .buckets(native_only_buckets()),
        )
        .expect("valid metric");
        let fetched_discarded_record_bytes = Counter::with_opts(Opts::new(
            "cortex_ingest_storage_reader_fetched_discarded_bytes_total",
            "Total number of uncompressed bytes of records discarded from because they were already consumed. A higher rate means that the concurrent fetching estimations are less accurate.",
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

/// Keeps the reader client's cluster metadata fresh like kgo does for a consumed partition:
/// a periodic refresh every `DefaultMetadataRefreshInterval` (`MetadataMinAge` =
/// `MetadataMaxAge` = 10s in `commonKafkaClientOptions`) plus asynchronous forced refreshes
/// (`ForceMetadataRefresh`), at most one request in flight at a time.
pub struct PartitionMetadataRefresher {
    client: Arc<KafkaClient>,
    topic: String,
    in_flight: Arc<AtomicBool>,
}

/// `DefaultMetadataRefreshInterval` (`writer_client.go`).
pub const DEFAULT_METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

impl PartitionMetadataRefresher {
    pub fn new(client: Arc<KafkaClient>, topic: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            client,
            topic: topic.into(),
            in_flight: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Triggers an asynchronous metadata refresh unless one is already running.
    pub fn trigger(&self) {
        if self.in_flight.swap(true, Ordering::SeqCst) {
            return;
        }
        let client = Arc::clone(&self.client);
        let topic = self.topic.clone();
        let in_flight = Arc::clone(&self.in_flight);
        tokio::spawn(async move {
            if let Err(e) = client.refresh_metadata(&[topic.as_str()]).await {
                debug!(topic = %topic, err = %e, "metadata refresh failed");
            }
            in_flight.store(false, Ordering::SeqCst);
        });
    }

    /// Runs the periodic refresh until `stop_rx` flips to `true`.
    pub fn spawn_periodic(self: &Arc<Self>, mut stop_rx: watch::Receiver<bool>) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(DEFAULT_METADATA_REFRESH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            return;
                        }
                    }
                    _ = ticker.tick() => this.trigger(),
                }
            }
        })
    }
}

impl MetadataRefresher for PartitionMetadataRefresher {
    fn force_metadata_refresh(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.trigger();
        Box::pin(std::future::ready(()))
    }
}

/// Maps a client error of a Fetch request to the franz-go error the upstream handlers match on
/// (`handleKafkaFetchErr` matches some errors by their kgo message).
pub(crate) fn kafka_fetch_error_from_client(err: KafkaClientError) -> KafkaFetchError {
    match err {
        KafkaClientError::Broker(e) => KafkaFetchError::Broker(e),
        KafkaClientError::UnknownBroker => KafkaFetchError::Other(format!(
            "fetching from kafka: {UNKNOWN_BROKER}"
        )),
        // The connection died: kgo fails the request with errChosenBrokerDead and a new
        // connection is dialed on the next attempt.
        KafkaClientError::ConnectionClosed | KafkaClientError::ChosenBrokerDead => {
            KafkaFetchError::Other(format!("fetching from kafka: {CHOSEN_BROKER_DIED}"))
        }
        KafkaClientError::RequestTimeout(_) | KafkaClientError::ConnectTimeout { .. } => {
            KafkaFetchError::Other("fetching from kafka: i/o timeout".to_string())
        }
        KafkaClientError::FirstReadEof(_) => KafkaFetchError::FirstReadEOF,
        ref dial @ KafkaClientError::Dial(_)
            if !super::kgo_retry::is_permanent_dial_err(dial) =>
        {
            KafkaFetchError::TransientTransport(format!("fetching from kafka: {dial}"))
        }
        KafkaClientError::Io(ref io_err)
            if matches!(
                io_err.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::UnexpectedEof
            ) =>
        {
            KafkaFetchError::TransientTransport(format!("fetching from kafka: {err}"))
        }
        other => KafkaFetchError::Other(format!("fetching from kafka: {other}")),
    }
}

/// Concurrent Kafka partition fetcher maintaining ordered record delivery across parallel workers.
pub struct ConcurrentFetchers {
    client: Arc<KafkaClient>,
    refresher: Arc<PartitionMetadataRefresher>,
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

/// Fetch request version: v13 (topic IDs) like upstream, lowered when the broker or the
/// metadata cannot do topic IDs.
const CONCURRENT_FETCH_VERSION: i16 = 13;

impl ConcurrentFetchers {
    /// `NewConcurrentFetchers`. `start_offset` can be `KAFKA_OFFSET_START`, `KAFKA_OFFSET_END`
    /// or a specific offset.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        client: Arc<KafkaClient>,
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
            return Err("fetchBackoffConfig.MaxBackoff has not been set".to_string());
        }
        if fetch_backoff_config.max_retries != 0 {
            return Err("fetchBackoffConfig.MaxRetries must be 0".to_string());
        }

        let topic_name = topic.into();
        let resolved_start = match start_offset {
            KAFKA_OFFSET_START => offset_reader
                .fetch_partition_start_offset(&topic_name, partition_id)
                .await
                .map_err(|e| format!("resolving offset to start consuming from: {e}"))?,
            KAFKA_OFFSET_END => {
                // End (-1) means "ignore all existing records": start from the next one.
                offset_reader
                    .fetch_partition_last_produced_offset(&topic_name, partition_id)
                    .await
                    .map_err(|e| format!("resolving offset to start consuming from: {e}"))?
                    + 1
            }
            other => other,
        };

        let max_limit = if max_buffered_bytes_limit <= 0 {
            i32::MAX
        } else {
            max_buffered_bytes_limit
        };

        // kadm ListTopics(topic): the topic must exist and report no error.
        let topic_id = client
            .topic_id(&topic_name)
            .await
            .map_err(|e| format!("failed to find topic ID: {e}"))?;

        let (ordered_tx, ordered_rx) = mpsc::channel(1);
        let (stop_tx, _) = watch::channel(false);

        Ok(Self {
            refresher: PartitionMetadataRefresher::new(Arc::clone(&client), topic_name.clone()),
            client,
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
            estimated_bytes_per_record: Arc::new(AtomicI64::new(0)),
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

    /// The resolved offset fetching starts from.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
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

    fn worker_ctx(&self) -> WorkerCtx {
        WorkerCtx {
            client: Arc::clone(&self.client),
            refresher: Arc::clone(&self.refresher),
            topic_name: self.topic_name.clone(),
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            min_bytes_wait_time: self.min_bytes_wait_time,
            track_compressed_bytes: self.track_compressed_bytes,
            metrics: self.metrics.clone(),
        }
    }

    /// Starts the coordinator and worker tasks (`ConcurrentFetchers.start`).
    pub async fn start(&self) {
        let mut guard = self.task.lock().await;
        if guard.is_some() {
            return;
        }

        let concurrency = self.start_concurrency;
        let max_bytes_per_fetcher = self.max_buffered_bytes_limit / concurrency as i32;
        tracing::info!(
            partition = self.partition_id,
            start_offset = self.start_offset,
            concurrency,
            max_bytes_per_fetch_request = max_bytes_per_fetcher,
            "starting concurrent fetchers"
        );

        // HWM is updated by the fetchers. A value of 0 is the same as there not being any
        // produced records; it doesn't prevent progress because there is always at least one
        // inflight fetchWant.
        let high_watermark = Arc::new(AtomicI64::new(0));
        let known_log_start = Arc::new(AtomicI64::new(-1));
        let (wants_tx, wants_rx) = mpsc::channel::<(FetchWant, mpsc::Sender<FetchResult>)>(1);
        let wants_rx = Arc::new(Mutex::new(wants_rx));

        let mut worker_handles = Vec::with_capacity(concurrency + 1);
        worker_handles.push(self.refresher.spawn_periodic(self.stop_tx.subscribe()));
        for _ in 0..concurrency {
            let ctx = self.worker_ctx();
            let rx = Arc::clone(&wants_rx);
            let range_policy = self.range_error_policy;
            let start_offsets_reader = Arc::clone(&self.start_offsets_reader);
            let backoff_cfg = self.fetch_backoff_config;
            let buffered_recs = Arc::clone(&self.buffered_fetched_records);
            let hwm = Arc::clone(&high_watermark);
            let lso = Arc::clone(&known_log_start);
            let stop_rx = self.stop_tx.subscribe();
            worker_handles.push(tokio::spawn(run_worker(
                ctx,
                rx,
                range_policy,
                start_offsets_reader,
                backoff_cfg,
                buffered_recs,
                hwm,
                lso,
                stop_rx,
            )));
        }

        let start_offset = self.start_offset;
        let max_buffered_bytes_limit = self.max_buffered_bytes_limit;
        let range_policy = self.range_error_policy;
        let inflight_bytes = Arc::clone(&self.buffered_fetched_bytes);
        let est_bpr = Arc::clone(&self.estimated_bytes_per_record);
        let ordered_tx = self.ordered_tx.clone();
        let mut stop_rx = self.stop_tx.subscribe();

        let coord = tokio::spawn(async move {
            // nextFetch is the next records fetch operation we want to issue to one of the
            // running workers.
            let mut next_fetch =
                FetchWant::from_offset(start_offset, max_bytes_per_fetcher, INITIAL_BYTES_PER_RECORD);
            // inflight: all fetchWants currently in flight, in the order their results must be
            // returned to PollFetches().
            let mut inflight: VecDeque<(FetchWant, mpsc::Receiver<FetchResult>)> = VecDeque::new();
            // The next ordered result ready to be polled (`readyBufferedResults != nil`).
            let mut ready: Option<FetchResult> = None;

            loop {
                if *stop_rx.borrow() {
                    break;
                }
                if range_policy == RangeErrorPolicy::ResumeFromStart {
                    let lso = known_log_start.load(Ordering::SeqCst);
                    if lso > next_fetch.start_offset {
                        next_fetch = FetchWant::from_offset(
                            lso,
                            next_fetch.max_bytes_limit,
                            next_fetch.estimated_bytes_per_record,
                        );
                    }
                }
                let refill = ready.is_none() && !inflight.is_empty();
                let would_exceed = inflight_bytes.load(Ordering::SeqCst)
                    + next_fetch.max_bytes() as i64
                    > max_buffered_bytes_limit as i64;
                // Dispatch only fetches of existing offsets (or when nothing is in flight, so the
                // HWM can be updated) and without exceeding the buffered bytes limit.
                let dispatch = inflight.is_empty()
                    || (!would_exceed && next_fetch.start_offset <= high_watermark.load(Ordering::SeqCst));

                // Periodically update our estimation, so it's exported as a metric.
                est_bpr.store(next_fetch.estimated_bytes_per_record as i64, Ordering::SeqCst);

                let front_rx = inflight.front_mut().map(|(_, rx)| rx);
                tokio::select! {
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    permit = wants_tx.reserve(), if dispatch => {
                        let Ok(permit) = permit else { break };
                        let (res_tx, res_rx) = mpsc::channel(1);
                        permit.send((next_fetch.clone(), res_tx));
                        inflight_bytes.fetch_add(next_fetch.max_bytes() as i64, Ordering::SeqCst);
                        inflight.push_back((next_fetch.clone(), res_rx));
                        next_fetch = next_fetch.next();
                    }
                    res = async { front_rx.expect("refill requires an inflight want").recv().await }, if refill => {
                        match res {
                            None => {
                                // The worker is done with this want: removeNextResult(). The
                                // MaxBytes counted is the one at dispatch time.
                                if let Some((fw, _)) = inflight.pop_front() {
                                    inflight_bytes.fetch_sub(fw.max_bytes() as i64, Ordering::SeqCst);
                                }
                            }
                            Some(result) => {
                                next_fetch = next_fetch
                                    .update_bytes_per_record(result.fetched_bytes, result.records.len());
                                ready = Some(result);
                            }
                        }
                    }
                    permit = ordered_tx.reserve(), if ready.is_some() => {
                        let Ok(permit) = permit else { break };
                        permit.send(ready.take().expect("checked"));
                    }
                }
            }

            drop(wants_tx);
            for h in worker_handles {
                let _ = h.await;
            }
        });

        *guard = Some(coord);
    }

    /// `Stop`: buffered records are intentionally dropped.
    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);
        if let Some(h) = self.task.lock().await.take() {
            let _ = h.await;
        }
        self.buffered_fetched_records.store(0, Ordering::SeqCst);
        self.buffered_fetched_bytes.store(0, Ordering::SeqCst);
        tracing::info!(
            partition = self.partition_id,
            last_returned_offset = self.last_returned_offset.load(Ordering::SeqCst),
            "stopped concurrent fetchers"
        );
    }

    /// `PollFetches`: the next ordered fetch, skipping records already returned
    /// (`<= lastReturnedOffset`). Returns `None` once `cancel` completes first.
    pub async fn poll_fetches_until<F: Future>(&self, cancel: F) -> Option<FetchResult> {
        let wait_start = std::time::Instant::now();
        let mut rx = self.ordered_rx.lock().await;
        let mut f = tokio::select! {
            biased;
            res = rx.recv() => res?,
            _ = cancel => return None,
        };
        drop(rx);

        // Decrease by the number of actually buffered records, not by the number returned
        // (which can be lower if some records are discarded because already returned).
        self.buffered_fetched_records
            .fetch_sub(f.records.len() as i64, Ordering::SeqCst);

        let last_ret = self.last_returned_offset.load(Ordering::SeqCst);
        let first_new = f
            .records
            .iter()
            .position(|r| r.offset > last_ret)
            .unwrap_or(f.records.len());

        // recordOrderedFetchTelemetry
        self.metrics
            .fetch_wait_duration
            .observe(wait_start.elapsed().as_secs_f64());
        let discarded_bytes: usize = f.records[..first_new].iter().map(|r| r.value.len()).sum();
        self.metrics
            .fetched_discarded_record_bytes
            .inc_by(discarded_bytes as f64);
        if first_new > 0 {
            debug!(
                skipped_records_count = first_new,
                first_skipped_offset = f.records[0].offset,
                last_skipped_offset = f.records[first_new - 1].offset,
                "skipped records because it is already returned"
            );
            f.records.drain(..first_new);
        }
        if let Some(last) = f.records.last() {
            self.last_returned_offset.store(last.offset, Ordering::SeqCst);
        }
        Some(f)
    }

    /// `PollFetches` bounded by `timeout` (zero = wait forever).
    pub async fn poll_fetches(&self, timeout: Duration) -> Option<FetchResult> {
        if timeout.is_zero() {
            self.poll_fetches_until(std::future::pending::<()>()).await
        } else {
            self.poll_fetches_until(tokio::time::sleep(timeout)).await
        }
    }

    /// `fetchSingle`: one Fetch request for `fw` to the partition leader.
    pub async fn fetch_single(&self, fw: &FetchWant) -> FetchResult {
        self.worker_ctx().fetch_single(fw).await
    }

    /// `fetchSingle` with a canceled context: returns an empty, non-error result.
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

    /// `parseFetchResponse` of a v13 (topic ID) response.
    pub fn parse_fetch_response(&self, start_offset: i64, resp: &FetchResponse) -> FetchResult {
        self.worker_ctx()
            .parse_fetch_response(start_offset, resp, CONCURRENT_FETCH_VERSION)
    }
}

/// What a worker needs to issue and parse Fetch requests.
struct WorkerCtx {
    client: Arc<KafkaClient>,
    refresher: Arc<PartitionMetadataRefresher>,
    topic_name: String,
    topic_id: [u8; 16],
    partition_id: i32,
    min_bytes_wait_time: Duration,
    track_compressed_bytes: bool,
    metrics: FetcherMetrics,
}

impl WorkerCtx {
    async fn fetch_single(&self, fw: &FetchWant) -> FetchResult {
        let fetch_start = std::time::Instant::now();
        let res = self.fetch_single_inner(fw).await;
        log_completed_fetch(&res, fetch_start, fw);
        res
    }

    async fn fetch_single_inner(&self, fw: &FetchWant) -> FetchResult {
        // Lookup the leader broker for the topic partition (matching kgo PartitionLeader).
        let (leader_id, leader_epoch, part_err) = self
            .client
            .partition_leader(&self.topic_name, self.partition_id);
        if let Some(err) = part_err {
            return FetchResult::error(self.partition_id, Some(KafkaFetchError::Broker(err)));
        }
        if leader_id < 0 {
            return FetchResult::error(self.partition_id, Some(KafkaFetchError::UnknownPartitionLeader));
        }

        let max_bytes = fw.max_bytes();
        let mut fp = FetchPartition::default();
        fp.partition = self.partition_id;
        fp.current_leader_epoch = leader_epoch;
        fp.fetch_offset = fw.start_offset;
        fp.last_fetched_epoch = -1;
        fp.log_start_offset = -1;
        fp.partition_max_bytes = max_bytes;

        let mut ft = FetchTopic::default();
        ft.topic = TopicName(StrBytes::from_string(self.topic_name.clone()));
        ft.topic_id = uuid::Uuid::from_bytes(self.topic_id);
        ft.partitions = vec![fp];

        let mut req = FetchRequest::default();
        req.replica_id = BrokerId(-1);
        // Warpstream ignores MinBytes: MaxWaitMillis is always waited.
        req.min_bytes = 1;
        req.max_wait_ms = self.min_bytes_wait_time.as_millis().min(i32::MAX as u128) as i32;
        req.max_bytes = max_bytes;
        req.isolation_level = 0;
        req.session_id = 0;
        req.session_epoch = -1;
        // No rack: ConcurrentFetchers always reads from the partition leader.
        req.topics = vec![ft];

        self.metrics.fetch_max_bytes.observe(req.max_bytes as f64);

        // Topic IDs are only usable with v13+; without a known topic ID stay on v12.
        let max_version = if self.topic_id == [0u8; 16] {
            12
        } else {
            CONCURRENT_FETCH_VERSION
        };
        // kgo read deadline: MaxWait + RequestTimeoutOverhead (10s).
        let timeout = self.min_bytes_wait_time + super::kgo_retry::DEFAULT_REQUEST_TIMEOUT_OVERHEAD;
        match self
            .client
            .fetch_from_broker(leader_id, &req, 4, max_version, timeout)
            .await
        {
            Ok((resp, version)) => self.parse_fetch_response(fw.start_offset, &resp, version),
            Err(e) => FetchResult::error(self.partition_id, Some(kafka_fetch_error_from_client(e))),
        }
    }

    /// `parseFetchResponse`. Guarantees no record is returned together with an error.
    fn parse_fetch_response(&self, start_offset: i64, resp: &FetchResponse, version: i16) -> FetchResult {
        // PreferredReadReplica is ignored: no rack is sent, so the broker has no recommended
        // replica for us.
        let pid = self.partition_id;
        let err = |msg: String| FetchResult::error(pid, Some(KafkaFetchError::Other(msg)));
        if resp.error_code != 0 {
            return err(format!("received error code {}", resp.error_code));
        }
        if resp.responses.len() != 1 {
            return err(format!(
                "unexpected number of topics in the Fetch response (expected: 1 got: {})",
                resp.responses.len()
            ));
        }
        let topic_resp = &resp.responses[0];
        if version >= 13 {
            if *topic_resp.topic_id.as_bytes() != self.topic_id {
                return err(format!(
                    "unexpected topic ID in the Fetch response (expected: {} got {})",
                    uuid_string(&self.topic_id),
                    uuid_string(topic_resp.topic_id.as_bytes())
                ));
            }
        } else if topic_resp.topic.0.as_str() != self.topic_name {
            return err(format!(
                "unexpected topic in the Fetch response (expected: {} got {})",
                self.topic_name,
                topic_resp.topic.0.as_str()
            ));
        }
        if topic_resp.partitions.len() != 1 {
            return err(format!(
                "unexpected number of partitions in the Fetch response (expected: 1 got: {})",
                topic_resp.partitions.len()
            ));
        }
        let part_resp = &topic_resp.partitions[0];
        if part_resp.partition_index != pid {
            return err(format!(
                "unexpected partition ID in the Fetch response (expected: {} got {})",
                pid, part_resp.partition_index
            ));
        }
        if let Some(e) = part_resp.error_code.err() {
            let mut fr = FetchResult::error(pid, Some(KafkaFetchError::Broker(e)));
            fr.log_start_offset = part_resp.log_start_offset;
            fr.high_watermark = part_resp.high_watermark;
            return fr;
        }

        let raw = part_resp.records.clone().unwrap_or_default();
        let raw_len = raw.len();
        let records = if raw.is_empty() {
            Vec::new()
        } else {
            match self.client.decode_fetched_partition(raw, start_offset) {
                Ok((records, _, _)) => records,
                Err(e) => return err(format!("decode batch: {e}")),
            }
        };

        let fetched_bytes = if self.track_compressed_bytes {
            raw_len
        } else {
            records.iter().map(|r| r.value.len()).sum()
        };

        FetchResult {
            partition: pid,
            records,
            high_watermark: part_resp.high_watermark,
            log_start_offset: part_resp.log_start_offset,
            fetched_bytes,
            err: None,
        }
    }
}

fn uuid_string(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// `fetchResult.logCompletedFetch`.
fn log_completed_fetch(fr: &FetchResult, fetch_start: std::time::Instant, fw: &FetchWant) {
    let got = fr.records.len() as i64;
    let asked = fw.end_offset - fw.start_offset;
    let first_ts = fr.records.first().map(|r| r.timestamp_ms);
    let last_ts = fr.records.last().map(|r| r.timestamp_ms);
    match &fr.err {
        None | Some(KafkaFetchError::Broker(ResponseError::OffsetOutOfRange)) => debug!(
            duration = ?fetch_start.elapsed(),
            start_offset = fw.start_offset,
            end_offset = fw.end_offset,
            asked_records = asked,
            got_records = got,
            diff_records = asked - got,
            asked_bytes = fw.max_bytes(),
            got_bytes = fr.fetched_bytes,
            first_timestamp = ?first_ts,
            last_timestamp = ?last_ts,
            hwm = fr.high_watermark,
            lso = fr.log_start_offset,
            err = ?fr.err,
            "{}",
            if fr.err.is_none() { "fetched records" } else { "received an error while fetching records; will retry after processing received records (if any)" }
        ),
        Some(e) => error!(
            duration = ?fetch_start.elapsed(),
            start_offset = fw.start_offset,
            end_offset = fw.end_offset,
            asked_records = asked,
            got_records = got,
            asked_bytes = fw.max_bytes(),
            hwm = fr.high_watermark,
            lso = fr.log_start_offset,
            err = %e,
            "received an error while fetching records; will retry after processing received records (if any)"
        ),
    }
}

/// The workers' shared end of the `wants` channel: a fetchWant and where to send its results.
type WantsRx = mpsc::Receiver<(FetchWant, mpsc::Sender<FetchResult>)>;

/// `ConcurrentFetchers.run`: one worker servicing fetchWants until the wants channel closes.
#[allow(clippy::too_many_arguments)]
async fn run_worker(
    ctx: WorkerCtx,
    wants_rx: Arc<Mutex<WantsRx>>,
    range_policy: RangeErrorPolicy,
    start_offsets_reader: Arc<GenericOffsetReader<i64>>,
    backoff_cfg: BackoffConfig,
    buffered_recs: Arc<AtomicI64>,
    hwm: Arc<AtomicI64>,
    known_lso: Arc<AtomicI64>,
    mut stop_rx: watch::Receiver<bool>,
) {
    // One backoff per worker, shared by all its fetchWants (reset on any received record).
    let err_backoff = BackoffWaiter(SyncMutex::new(Backoff::new(backoff_cfg)));
    let partition_id = ctx.partition_id;
    loop {
        if *stop_rx.borrow() {
            return;
        }
        let item = {
            let mut locked = wants_rx.lock().await;
            tokio::select! {
                biased;
                _ = stop_rx.changed() => None,
                res = locked.recv() => res,
            }
        };
        let Some((mut w, result_tx)) = item else {
            return;
        };

        // The buffered fetchResult not yet sent to the result channel.
        let mut buffered = FetchResult::empty(partition_id);

        while w.end_offset > w.start_offset {
            if *stop_rx.borrow() {
                return;
            }
            let res = tokio::select! {
                biased;
                _ = stop_rx.changed() => return,
                res = ctx.fetch_single(&w) => res,
            };

            if res.log_start_offset >= 0 {
                cas_hwm(&known_lso, res.log_start_offset);
            }

            if let Some(ref err) = res.err {
                if *stop_rx.borrow() {
                    return;
                }
                let response_lso = Some(res.log_start_offset).filter(|&o| o >= 0);
                // We got an error: handle it and discard this fetch result content.
                let handled = tokio::select! {
                    biased;
                    _ = stop_rx.changed() => return,
                    r = handle_kafka_fetch_err_with_log_start(
                        Some(err),
                        w.clone(),
                        &err_backoff,
                        range_policy,
                        &start_offsets_reader,
                        ctx.refresher.as_ref(),
                        response_lso,
                    ) => r,
                };
                match handled {
                    Ok(adjusted) => w = adjusted,
                    Err(fatal) => {
                        tracing::warn!(partition = partition_id, error = %fatal, "failed to handle Kafka fetch error");
                        tokio::select! {
                            biased;
                            _ = stop_rx.changed() => return,
                            _ = result_tx.send(FetchResult::error(partition_id, Some(fatal))) => {}
                        }
                        // Break out of the retry loop, but continue servicing the wants.
                        break;
                    }
                }
            } else {
                // Count buffered records as soon as they are fetched.
                buffered_recs.fetch_add(res.records.len() as i64, Ordering::SeqCst);
                if res.high_watermark >= 0 {
                    cas_hwm(&hwm, res.high_watermark);
                }
                // Keep non-mergeable fields from res: the last response is the most updated.
                buffered = res.merge(buffered);
            }

            if buffered.records.is_empty() {
                if *stop_rx.borrow() {
                    return;
                }
                continue;
            }

            // The next attempt is from the last record onwards.
            w.start_offset = buffered.records.last().expect("non-empty").offset + 1;
            w = w.update_bytes_per_record(buffered.fetched_bytes, buffered.records.len());

            // Any received record means some success: reset the backoff.
            err_backoff.0.lock().reset();

            match result_tx.try_send(buffered) {
                Ok(()) => buffered = FetchResult::empty(partition_id),
                Err(mpsc::error::TrySendError::Full(b)) => {
                    if w.start_offset >= w.end_offset {
                        // The whole want is fetched: we definitely have to wait to send now.
                        tokio::select! {
                            biased;
                            _ = stop_rx.changed() => return,
                            _ = result_tx.send(b) => {}
                        }
                        buffered = FetchResult::empty(partition_id);
                    } else {
                        buffered = b;
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
        // Closing the result channel tells the coordinator this want is done.
        drop(result_tx);
    }
}

pub(crate) fn cas_hwm(hwm: &AtomicI64, new_hwm: i64) {
    let mut cur = hwm.load(Ordering::SeqCst);
    while cur < new_hwm {
        match hwm.compare_exchange_weak(cur, new_hwm, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
    use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };
    use kafka_protocol::messages::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::{
        ApiKey, ApiVersionsRequest, BrokerId, FetchRequest, FetchResponse, ListOffsetsRequest,
        ListOffsetsResponse, MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader,
        TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    };
    use mimir_config::types::StringSliceCSV;
    use mimir_config::{GoDuration, KafkaConfig};
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct CountingWaiter(AtomicUsize);
    impl FetchWaiter for CountingWaiter {
        fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(()))
        }
    }

    struct NoopRefresher;
    impl MetadataRefresher for NoopRefresher {
        fn force_metadata_refresh(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(std::future::ready(()))
        }
    }

    #[tokio::test]
    async fn handle_kafka_fetch_err_backs_off_when_ahead_and_uses_response_lso() {
        let offset_reader = GenericOffsetReader::new(
            Arc::new(|| Box::pin(async { Ok(0) })),
            Duration::from_secs(60),
        );
        offset_reader.start().await;
        for _ in 0..20 {
            if offset_reader.cached_offset().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // 1. fw.start_offset >= partition_start on OffsetOutOfRange MUST invoke long_backoff.
        let waiter = CountingWaiter(AtomicUsize::new(0));
        let fw = FetchWant {
            start_offset: 50,
            end_offset: 100,
            estimated_bytes_per_record: 100,
            max_bytes_limit: 1_000_000,
        };
        let adjusted = handle_kafka_fetch_err_with_log_start(
            Some(&KafkaFetchError::Broker(ResponseError::OffsetOutOfRange)),
            fw.clone(),
            &waiter,
            RangeErrorPolicy::ResumeFromStart,
            &offset_reader,
            &NoopRefresher,
            Some(0),
        )
        .await
        .unwrap();
        assert_eq!(adjusted, fw);
        assert_eq!(
            waiter.0.load(Ordering::SeqCst),
            1,
            "OffsetOutOfRange with start_offset >= partition_start must back off"
        );

        // 2. response_log_start_offset fast-forwards even when cached_offset is stale (0).
        let waiter2 = CountingWaiter(AtomicUsize::new(0));
        let adjusted2 = handle_kafka_fetch_err_with_log_start(
            Some(&KafkaFetchError::Broker(ResponseError::OffsetOutOfRange)),
            FetchWant {
                start_offset: 50,
                end_offset: 100,
                estimated_bytes_per_record: 100,
                max_bytes_limit: 1_000_000,
            },
            &waiter2,
            RangeErrorPolicy::ResumeFromStart,
            &offset_reader,
            &NoopRefresher,
            Some(75),
        )
        .await
        .unwrap();
        assert_eq!(adjusted2.start_offset, 75);
        assert_eq!(waiter2.0.load(Ordering::SeqCst), 0);

        offset_reader.stop().await;
    }

    fn encode_single_record_batch(offset: i64, val: &[u8]) -> Bytes {
        let rec = Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: 0,
            timestamp: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::copy_from_slice(val)),
            headers: IndexMap::new(),
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode_with_custom_compression(
            &mut buf,
            [rec].iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
            Some(crate::ingest::compression::compress_record_batch),
        )
        .unwrap();
        buf.freeze()
    }

    async fn spawn_out_of_range_mock_broker(
        log_start_offset: Arc<AtomicI64>,
        high_watermark: Arc<AtomicI64>,
        deliver_at_offset: Arc<AtomicI64>,
        fetch_count: Arc<AtomicUsize>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let topic_uuid = uuid::Uuid::from_bytes([7u8; 16]);

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let lso = Arc::clone(&log_start_offset);
                let hwm = Arc::clone(&high_watermark);
                let deliver = Arc::clone(&deliver_at_offset);
                let fc = Arc::clone(&fetch_count);
                let port = addr.port() as i32;
                tokio::spawn(async move {
                    let (mut rd, mut wr) = stream.into_split();
                    let mut len_buf = [0u8; 4];
                    while rd.read_exact(&mut len_buf).await.is_ok() {
                        let sz = u32::from_be_bytes(len_buf) as usize;
                        let mut payload = vec![0u8; sz];
                        if rd.read_exact(&mut payload).await.is_err() {
                            break;
                        }
                        let mut buf = Bytes::from(payload);
                        let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let Ok(api_key) = ApiKey::try_from(key_i16) else {
                            break;
                        };
                        let hdr_ver = match api_key {
                            ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                            ApiKey::Metadata => MetadataRequest::header_version(ver),
                            ApiKey::ListOffsets => ListOffsetsRequest::header_version(ver),
                            ApiKey::Fetch => FetchRequest::header_version(ver),
                            _ => break,
                        };
                        let Ok(req_hdr) = RequestHeader::decode(&mut buf, hdr_ver) else {
                            break;
                        };
                        let mut out = BytesMut::new();
                        out.put_u32(0);
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr
                            .encode(&mut out, api_key.response_header_version(ver))
                            .unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 1, 12),
                                    (ApiKey::ListOffsets as i16, 0, 7),
                                    (ApiKey::Fetch as i16, 4, 13),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = mn;
                                    av.max_version = mx;
                                    resp.api_keys.push(av);
                                }
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, ver);
                                let mut resp = MetadataResponse::default();
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = port;
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);
                                let mut mt = MetadataResponseTopic::default();
                                mt.name = Some(TopicName(StrBytes::from_static_str("ingest")));
                                mt.topic_id = topic_uuid;
                                let mut mp = MetadataResponsePartition::default();
                                mp.partition_index = 0;
                                mp.leader_id = BrokerId(0);
                                mt.partitions.push(mp);
                                resp.topics.push(mt);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::ListOffsets => {
                                let _ = ListOffsetsRequest::decode(&mut buf, ver);
                                let mut resp = ListOffsetsResponse::default();
                                let mut lt = ListOffsetsTopicResponse::default();
                                lt.name = TopicName(StrBytes::from_static_str("ingest"));
                                let mut lp = ListOffsetsPartitionResponse::default();
                                lp.partition_index = 0;
                                lp.offset = 0;
                                lp.timestamp = -1;
                                lt.partitions.push(lp);
                                resp.topics.push(lt);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Fetch => {
                                let req = FetchRequest::decode(&mut buf, ver).unwrap();
                                fc.fetch_add(1, Ordering::SeqCst);
                                let fetch_off = req.topics[0].partitions[0].fetch_offset;
                                let cur_lso = lso.load(Ordering::SeqCst);
                                let cur_hwm = hwm.load(Ordering::SeqCst);
                                let target_off = deliver.load(Ordering::SeqCst);

                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                pd.log_start_offset = cur_lso;
                                pd.high_watermark = cur_hwm;
                                if fetch_off < cur_lso || fetch_off > cur_hwm {
                                    pd.error_code = ResponseError::OffsetOutOfRange.code();
                                } else if target_off >= 0 && fetch_off == target_off {
                                    pd.records = Some(encode_single_record_batch(
                                        target_off,
                                        b"fast-forwarded",
                                    ));
                                }

                                let mut ft = FetchableTopicResponse::default();
                                ft.topic_id = topic_uuid;
                                ft.topic = TopicName(StrBytes::from_static_str("ingest"));
                                ft.partitions.push(pd);
                                let mut resp = FetchResponse::default();
                                resp.responses.push(ft);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            _ => {}
                        }
                        let frame_len = (out.len() - 4) as u32;
                        out[0..4].copy_from_slice(&frame_len.to_be_bytes());
                        if wr.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        (addr, handle)
    }

    #[tokio::test]
    async fn concurrent_fetchers_offset_out_of_range_bounds_requests_and_fast_forwards() {
        // 1. HWM regression (start_offset >= lso) must back off instead of 0ms spin storm.
        {
            let lso = Arc::new(AtomicI64::new(0));
            let hwm = Arc::new(AtomicI64::new(10));
            let deliver = Arc::new(AtomicI64::new(-1));
            let fetch_count = Arc::new(AtomicUsize::new(0));
            let (addr, srv) =
                spawn_out_of_range_mock_broker(lso, hwm, deliver, Arc::clone(&fetch_count)).await;

            let cfg = KafkaConfig {
                address: StringSliceCSV(vec![addr.to_string()]),
                topic: "ingest".to_string(),
                dial_timeout: GoDuration::from_secs(2),
                ..Default::default()
            };
            let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
            let oc = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, None);
            let start_reader = Arc::new(GenericOffsetReader::new(
                Arc::new(|| Box::pin(async { Ok(0) })),
                Duration::from_secs(60),
            ));
            start_reader.start().await;

            let f = ConcurrentFetchers::new(
                client,
                "ingest",
                0,
                50,
                4,
                1024 * 1024,
                false,
                Duration::from_millis(50),
                &oc,
                RangeErrorPolicy::ResumeFromStart,
                Arc::clone(&start_reader),
                BackoffConfig {
                    min_backoff: Duration::from_millis(50),
                    max_backoff: Duration::from_millis(100),
                    max_retries: 0,
                },
                FetcherMetrics::new(None),
            )
            .await
            .unwrap();

            f.start().await;
            tokio::time::sleep(Duration::from_millis(260)).await;
            let count = fetch_count.load(Ordering::SeqCst);
            f.stop().await;
            start_reader.stop().await;
            srv.abort();

            assert!(
                count <= 20,
                "expected bounded fetch count with backoff, got {count} (spin storm!)"
            );
        }

        // 2. Retention jump (log_start_offset = 10_000 while start_offsets_reader caches 0)
        // must fast-forward immediately from FetchResponse.log_start_offset!
        {
            let lso = Arc::new(AtomicI64::new(10_000));
            let hwm = Arc::new(AtomicI64::new(10_001));
            let deliver = Arc::new(AtomicI64::new(10_000));
            let fetch_count = Arc::new(AtomicUsize::new(0));
            let (addr, srv) =
                spawn_out_of_range_mock_broker(lso, hwm, deliver, Arc::clone(&fetch_count)).await;

            let cfg = KafkaConfig {
                address: StringSliceCSV(vec![addr.to_string()]),
                topic: "ingest".to_string(),
                dial_timeout: GoDuration::from_secs(2),
                ..Default::default()
            };
            let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
            let oc = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, None);
            let start_reader = Arc::new(GenericOffsetReader::new(
                Arc::new(|| Box::pin(async { Ok(0) })),
                Duration::from_secs(60),
            ));
            start_reader.start().await;

            let f = ConcurrentFetchers::new(
                client,
                "ingest",
                0,
                0,
                4,
                1024 * 1024,
                false,
                Duration::from_millis(50),
                &oc,
                RangeErrorPolicy::ResumeFromStart,
                Arc::clone(&start_reader),
                BackoffConfig {
                    min_backoff: Duration::from_millis(50),
                    max_backoff: Duration::from_millis(100),
                    max_retries: 0,
                },
                FetcherMetrics::new(None),
            )
            .await
            .unwrap();

            f.start().await;
            let res = tokio::time::timeout(
                Duration::from_secs(2),
                f.poll_fetches(Duration::from_secs(2)),
            )
            .await
            .expect("timed out waiting for fast-forwarded record")
            .expect("poll_fetches returned None");

            assert!(res.err.is_none());
            assert_eq!(res.records.len(), 1);
            assert_eq!(res.records[0].offset, 10_000);
            assert_eq!(res.records[0].value, Bytes::from_static(b"fast-forwarded"));
            assert!(
                fetch_count.load(Ordering::SeqCst) < 20,
                "expected O(1) fast-forward, got {} fetches",
                fetch_count.load(Ordering::SeqCst)
            );

            f.stop().await;
            start_reader.stop().await;
            srv.abort();
        }
    }
    struct CountingRefresher(AtomicUsize);
    impl MetadataRefresher for CountingRefresher {
        fn force_metadata_refresh(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(()))
        }
    }

    #[tokio::test]
    async fn transient_transport_dial_and_io_errors_refresh_metadata_and_back_off() {
        use crate::ingest::kgo_retry::{DialError, DialErrorKind};

        let make_dial = |kind: DialErrorKind| {
            KafkaClientError::Dial(DialError {
                addr: "127.0.0.1:9092".to_string(),
                kind,
                source: std::io::Error::other("dial err"),
            })
        };
        for kind in [
            DialErrorKind::Refused,
            DialErrorKind::Unreachable,
            DialErrorKind::TimedOut,
            DialErrorKind::DnsTemporary,
        ] {
            let mapped = kafka_fetch_error_from_client(make_dial(kind));
            assert!(
                matches!(mapped, KafkaFetchError::TransientTransport(_)),
                "expected TransientTransport for {kind:?}, got {mapped:?}"
            );
        }
        for kind in [DialErrorKind::DnsNotFound, DialErrorKind::PermissionDenied] {
            let mapped = kafka_fetch_error_from_client(make_dial(kind));
            assert!(
                matches!(mapped, KafkaFetchError::Other(_)),
                "expected Other for permanent {kind:?}, got {mapped:?}"
            );
        }
        for io_kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::NotConnected,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            let mapped = kafka_fetch_error_from_client(KafkaClientError::Io(
                std::io::Error::from(io_kind),
            ));
            assert!(
                matches!(mapped, KafkaFetchError::TransientTransport(_)),
                "expected TransientTransport for {io_kind:?}, got {mapped:?}"
            );
        }

        let offset_reader = GenericOffsetReader::new(
            Arc::new(|| Box::pin(async { Ok(0) })),
            Duration::from_secs(60),
        );
        let waiter = CountingWaiter(AtomicUsize::new(0));
        let refresher = CountingRefresher(AtomicUsize::new(0));
        let fw = FetchWant::from_offset(0, 1_000_000, 100);
        let err = kafka_fetch_error_from_client(make_dial(DialErrorKind::Refused));
        let adjusted = handle_kafka_fetch_err_with_log_start(
            Some(&err),
            fw.clone(),
            &waiter,
            RangeErrorPolicy::ResumeFromStart,
            &offset_reader,
            &refresher,
            None,
        )
        .await
        .unwrap();
        assert_eq!(adjusted, fw);
        assert_eq!(refresher.0.load(Ordering::SeqCst), 1);
        assert_eq!(waiter.0.load(Ordering::SeqCst), 1);
        offset_reader.stop().await;
    }

    #[tokio::test]
    async fn metadata_partition_load_error_and_leaderless_epoch_do_not_spin_unknown_broker() {
        // Mode 0: LEADER_NOT_AVAILABLE with leader_id=-1, leader_epoch=7
        // Mode 1: error_code=0 with leader_id=-1, leader_epoch=7 (synthesizes LEADER_NOT_AVAILABLE)
        // Mode 2: error_code=0 with leader_id=0, leader_epoch=8 (healthy)
        let mode = Arc::new(AtomicI64::new(0));
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let list_offsets_stale = Arc::new(AtomicBool::new(false));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let topic_uuid = uuid::Uuid::from_bytes([9u8; 16]);
        let (srv_stop_tx, mut srv_stop_rx) = watch::channel(false);
        let mode_srv = Arc::clone(&mode);
        let fc_srv = Arc::clone(&fetch_count);
        let lo_stale_srv = Arc::clone(&list_offsets_stale);
        let srv_stop_tx_outer = srv_stop_tx.clone();
        let srv = tokio::spawn(async move {
            let port = addr.port() as i32;
            loop {
                let stream = tokio::select! {
                    biased;
                    _ = srv_stop_rx.changed() => break,
                    res = listener.accept() => match res {
                        Ok((s, _)) => s,
                        Err(_) => break,
                    },
                };
                let m = Arc::clone(&mode_srv);
                let fc = Arc::clone(&fc_srv);
                let lo_stale = Arc::clone(&lo_stale_srv);
                let mut conn_stop = srv_stop_tx.subscribe();
                tokio::spawn(async move {
                    let (mut rd, mut wr) = stream.into_split();
                    let mut len_buf = [0u8; 4];
                    loop {
                        let read_res = tokio::select! {
                            biased;
                            _ = conn_stop.changed() => break,
                            r = rd.read_exact(&mut len_buf) => r,
                        };
                        if read_res.is_err() {
                            break;
                        }
                        let sz = u32::from_be_bytes(len_buf) as usize;
                        let mut payload = vec![0u8; sz];
                        if rd.read_exact(&mut payload).await.is_err() {
                            break;
                        }
                        let mut buf = Bytes::from(payload);
                        let key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
                        let ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let Ok(api_key) = ApiKey::try_from(key_i16) else {
                            break;
                        };
                        let hdr_ver = match api_key {
                            ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                            ApiKey::Metadata => MetadataRequest::header_version(ver),
                            ApiKey::ListOffsets => ListOffsetsRequest::header_version(ver),
                            ApiKey::Fetch => FetchRequest::header_version(ver),
                            _ => break,
                        };
                        let Ok(req_hdr) = RequestHeader::decode(&mut buf, hdr_ver) else {
                            break;
                        };
                        let mut out = BytesMut::new();
                        out.put_u32(0);
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr
                            .encode(&mut out, api_key.response_header_version(ver))
                            .unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [
                                    (ApiKey::ApiVersions as i16, 0, 3),
                                    (ApiKey::Metadata as i16, 1, 12),
                                    (ApiKey::ListOffsets as i16, 0, 7),
                                    (ApiKey::Fetch as i16, 4, 13),
                                ] {
                                    let mut av = ApiVersion::default();
                                    av.api_key = k;
                                    av.min_version = mn;
                                    av.max_version = mx;
                                    resp.api_keys.push(av);
                                }
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, ver);
                                let mut resp = MetadataResponse::default();
                                let mut b = MetadataResponseBroker::default();
                                b.node_id = BrokerId(0);
                                b.host = StrBytes::from_static_str("127.0.0.1");
                                b.port = port;
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);
                                let mut mt = MetadataResponseTopic::default();
                                mt.name = Some(TopicName(StrBytes::from_static_str("ingest")));
                                mt.topic_id = topic_uuid;
                                let mut mp = MetadataResponsePartition::default();
                                mp.partition_index = 0;
                                match m.load(Ordering::SeqCst) {
                                    0 => {
                                        mp.error_code = ResponseError::LeaderNotAvailable.code();
                                        mp.leader_id = BrokerId(-1);
                                        mp.leader_epoch = 7;
                                    }
                                    1 => {
                                        mp.error_code = 0;
                                        mp.leader_id = BrokerId(-1);
                                        mp.leader_epoch = 7;
                                    }
                                    _ => {
                                        mp.error_code = 0;
                                        mp.leader_id = BrokerId(0);
                                        mp.leader_epoch = 8;
                                    }
                                }
                                mt.partitions.push(mp);
                                resp.topics.push(mt);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::ListOffsets => {
                                let _ = ListOffsetsRequest::decode(&mut buf, ver);
                                let mut resp = ListOffsetsResponse::default();
                                let mut lt = ListOffsetsTopicResponse::default();
                                lt.name = TopicName(StrBytes::from_static_str("ingest"));
                                let mut lp = ListOffsetsPartitionResponse::default();
                                lp.partition_index = 0;
                                if lo_stale.swap(false, Ordering::SeqCst) {
                                    lp.error_code = ResponseError::FencedLeaderEpoch.code();
                                } else {
                                    lp.offset = 0;
                                    lp.timestamp = -1;
                                }
                                lt.partitions.push(lp);
                                resp.topics.push(lt);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            ApiKey::Fetch => {
                                let _ = FetchRequest::decode(&mut buf, ver).unwrap();
                                fc.fetch_add(1, Ordering::SeqCst);
                                let mut pd = PartitionData::default();
                                pd.partition_index = 0;
                                pd.log_start_offset = 0;
                                pd.high_watermark = 1;
                                pd.records = Some(encode_single_record_batch(0, b"ok"));
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic_id = topic_uuid;
                                ft.topic = TopicName(StrBytes::from_static_str("ingest"));
                                ft.partitions.push(pd);
                                let mut resp = FetchResponse::default();
                                resp.responses.push(ft);
                                resp.encode(&mut out, ver).unwrap();
                            }
                            _ => {}
                        }
                        let frame_len = (out.len() - 4) as u32;
                        out[0..4].copy_from_slice(&frame_len.to_be_bytes());
                        if wr.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![addr.to_string()]),
            topic: "ingest".to_string(),
            dial_timeout: GoDuration::from_secs(2),
            last_produced_offset_retry_timeout: GoDuration::from_millis(300),
            ..Default::default()
        };
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let oc = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, None);
        let start_reader = Arc::new(GenericOffsetReader::new(
            Arc::new(|| Box::pin(async { Ok(0) })),
            Duration::from_secs(60),
        ));
        start_reader.start().await;

        let f = ConcurrentFetchers::new(
            Arc::clone(&client),
            "ingest",
            0,
            0,
            1,
            1024 * 1024,
            false,
            Duration::from_millis(20),
            &oc,
            RangeErrorPolicy::ResumeFromStart,
            Arc::clone(&start_reader),
            BackoffConfig {
                min_backoff: Duration::from_millis(50),
                max_backoff: Duration::from_millis(100),
                max_retries: 0,
            },
            FetcherMetrics::new(None),
        )
        .await
        .unwrap();

        // Mode 0: LEADER_NOT_AVAILABLE with leader_epoch=7 must NOT hit fetch_from_broker(-1).
        assert_eq!(
            client.partition_leader("ingest", 0),
            (-1, 7, Some(ResponseError::LeaderNotAvailable))
        );
        let r0 = f.fetch_single(&FetchWant::from_offset(0, 1_000_000, 100)).await;
        assert_eq!(r0.err, Some(KafkaFetchError::Broker(ResponseError::LeaderNotAvailable)));
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);

        // Mode 1: error_code=0, leader_id=-1, leader_epoch=7 synthesizes LEADER_NOT_AVAILABLE.
        mode.store(1, Ordering::SeqCst);
        client.refresh_metadata(&["ingest"]).await.unwrap();
        assert_eq!(
            client.partition_leader("ingest", 0),
            (-1, 7, Some(ResponseError::LeaderNotAvailable))
        );
        let r1 = f.fetch_single(&FetchWant::from_offset(0, 1_000_000, 100)).await;
        assert_eq!(r1.err, Some(KafkaFetchError::Broker(ResponseError::LeaderNotAvailable)));
        assert_eq!(fetch_count.load(Ordering::SeqCst), 0);

        // Mode 2: leader_id=0, leader_epoch=8 clears partition_load_errors and fetches cleanly.
        mode.store(2, Ordering::SeqCst);
        client.refresh_metadata(&["ingest"]).await.unwrap();
        assert_eq!(client.partition_leader("ingest", 0), (0, 8, None));
        let r2 = f.fetch_single(&FetchWant::from_offset(0, 1_000_000, 100)).await;
        assert!(r2.err.is_none());
        assert_eq!(r2.records.len(), 1);
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

        // Chain B2: broker stale-leader on ListOffsets invalidates leader + epoch.
        list_offsets_stale.store(true, Ordering::SeqCst);
        let off = client.list_offset_with_timestamp("ingest", 0, -1).await.unwrap();
        assert_eq!(off.offset, 0);
        assert_eq!(client.partition_leader("ingest", 0), (0, 8, None));

        // Chain B2: transport/dial error in ListOffsets must NOT wipe cached partition leader.
        let _ = srv_stop_tx_outer.send(true);
        let _ = srv.await;
        let mut parts = std::collections::HashMap::new();
        parts.insert("ingest".to_string(), vec![0]);
        let _ = client.list_offset_with_timestamp("ingest", 0, -1).await.unwrap_err();
        assert_eq!(
            client.partition_leader("ingest", 0),
            (0, 8, None),
            "single-partition ListOffsets transport error must preserve cached leader"
        );
        let _ = client.list_offsets_sharded(&parts, -1).await.unwrap_err();
        assert_eq!(
            client.partition_leader("ingest", 0),
            (0, 8, None),
            "sharded ListOffsets transport error must preserve cached leader"
        );

        start_reader.stop().await;
    }
}
