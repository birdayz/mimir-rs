//! Pure-Rust async TCP Kafka client using `kafka-protocol`.
//!
//! Implements Kafka TCP length-prefixed framing (`[4B BE size][header+body]`),
//! `ApiVersions` negotiation, `Metadata` discovery & broker connection pooling,
//! `ListOffsets` (`-1` latest / `-2` earliest / timestamp), `Fetch` (RecordBatch v2
//! decoding + decompression), `Produce` (RecordBatch v2 encoding + compression + CRC32C),
//! `CreateTopics`, `FindCoordinator`, `OffsetFetch`, and `OffsetCommit`.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go
//! - github.com/grafana/mimir/pkg/storage/ingest/reader_client.go
//! - github.com/grafana/mimir/pkg/storage/ingest/writer_client.go
//! - github.com/grafana/mimir/pkg/storage/ingest/DESIGN.md

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use mimir_common::thread_pool::ScopedWorkerPool;
use kafka_protocol::error::{ParseResponseErrorCode, ResponseError};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::{
    ApiKey, BrokerId, CreateTopicsRequest,
    CreateTopicsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, GroupId, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
    TopicName,
};
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use mimir_config::KafkaConfig;
use parking_lot::RwLock;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Mutex};

use super::compression::{compress_record_batch, decompress_record_batch, KafkaCompression};
use super::kgo_retry::{
    self, classify_connect_error, handle_dial_err, load_connection_retrying_once,
    mimir_retry_timeout, retryable_request, sharded_retry_backoff, AnyBrokerRotation,
    BrokerChoice, BrokerKey,
};
use super::kprom::{self, FetchBatchMetrics, KpromMetrics};
use super::transport::{KafkaTransport, SaslAuthConfig};

pub const KAFKA_OFFSET_START: i64 = -2;
pub const KAFKA_OFFSET_END: i64 = -1;

/// Maximum Kafka TCP response frame length (`256 MiB`), preventing 1 GiB upfront allocations on
/// corrupt or hostile 4-byte headers (upstream `franz-go` defaults `maxBrokerReadBytes` to 100 MiB).
pub(crate) const MAX_KAFKA_FRAME_BYTES: usize = 256 * 1024 * 1024;
const KAFKA_FRAME_INITIAL_CAP: usize = 64 * 1024;

/// Reads a single 4-byte length-prefixed Kafka response frame, growing the buffer incrementally as
/// bytes arrive so a 4-byte corrupt header followed by EOF cannot trigger a huge allocation.
pub(crate) async fn read_kafka_frame_bounded<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    if reader.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    if !(4..=max_frame_bytes).contains(&frame_len) {
        return None;
    }
    let mut payload = Vec::with_capacity(frame_len.min(KAFKA_FRAME_INITIAL_CAP));
    let mut filled = 0usize;
    while filled < frame_len {
        if filled == payload.len() {
            let next_len = if payload.is_empty() {
                frame_len.min(KAFKA_FRAME_INITIAL_CAP)
            } else {
                payload.len().saturating_mul(2).min(frame_len)
            };
            payload.resize(next_len, 0);
        }
        match reader.read(&mut payload[filled..]).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => filled += n,
        }
    }
    Some(payload)
}

#[derive(Debug, Error)]
pub enum KafkaClientError {
    #[error("no seed brokers configured")]
    NoSeedBrokers,
    /// The dial deadline (`-ingest-storage.kafka.dial-timeout`) expired (retried, like kgo).
    #[error("unable to dial: dial tcp {addr}: i/o timeout")]
    ConnectTimeout { addr: String },
    /// Any other dial failure (connection refused, DNS, unreachable, ...).
    #[error("unable to dial: {0}")]
    Dial(super::kgo_retry::DialError),
    /// The broker closed a new connection during its setup (`kgo.ErrFirstReadEOF`); never retried.
    #[error("{0}")]
    FirstReadEof(String),
    #[error("the broker connection has died and the request will be retried on a new connection")]
    ChosenBrokerDead,
    #[error("{}", unknown_controller_msg(*.0))]
    UnknownController(i32),
    #[error("unknown broker")]
    UnknownBroker,
    #[error("request timeout after {0:?}")]
    RequestTimeout(Duration),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol decode/encode error: {0}")]
    Protocol(#[from] anyhow::Error),
    #[error("kafka broker error: {0:?}")]
    Broker(ResponseError),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
    /// Invalid client configuration (e.g. TLS), detected when building the client.
    #[error("{0}")]
    Config(String),
    /// SASL authentication failed on a new broker connection.
    #[error("{0}")]
    Sasl(String),
}

fn unknown_controller_msg(id: i32) -> String {
    if id == -1 {
        "broker replied that the controller broker is not available".to_string()
    } else {
        format!(
            "broker replied that the controller broker is {id}, but did not reply with that broker in the broker list"
        )
    }
}

#[derive(Debug, Clone)]
pub struct FetchedRecord {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Bytes,
    pub value: Bytes,
    pub headers: Vec<(String, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct FetchPartitionResult {
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub records: Vec<FetchedRecord>,
    /// Next offset to fetch (`max(fetch_offset, last_batch_end_offset + 1)` across all decoded batches,
    /// including Kafka transaction control batches).
    pub next_fetch_offset: i64,
    /// Highest offset across all decoded batches (including control batches), or `None` if no batches
    /// were present in the response.
    pub last_batch_end_offset: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ProduceRecord {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Bytes)>,
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ListedOffset {
    pub offset: i64,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
struct BrokerInfo {
    addr: String,
}

#[derive(Debug, Clone, Default)]
struct ClusterMetadata {
    brokers: HashMap<i32, BrokerInfo>,
    /// `(topic, partition) -> leader broker id`
    partition_leaders: HashMap<(String, i32), i32>,
    /// `(topic, partition) -> leader epoch` (`-1` when the broker does not report it).
    partition_leader_epochs: HashMap<(String, i32), i32>,
    /// `topic -> topic ID` (all zeros when the broker does not report it).
    topic_ids: HashMap<String, [u8; 16]>,
    /// `topic -> sorted partition ids`
    topic_partitions: HashMap<String, Vec<i32>>,
}

/// kgo `broker.loadConnection`: requests are spread over separate connections per broker so
/// that long-polling Fetch requests (and slow / produce requests) never delay other requests
/// queued behind them on the same connection (brokers answer a connection's requests in order).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ConnKind {
    /// `cxnNormal`: metadata, ListOffsets, offset commit / fetch, ...
    Normal,
    /// `cxnProduce`.
    Produce,
    /// `cxnFetch`: Fetch (long poll).
    Fetch,
    /// `cxnSlow`: requests carrying a timeout (e.g. CreateTopics).
    Slow,
}

struct BrokerConn {
    addr: String,
    kind: ConnKind,
    client_id: String,
    next_corr_id: AtomicI32,
    write_tx: mpsc::Sender<Bytes>,
    pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>>,
    api_versions: RwLock<HashMap<i16, (i16, i16)>>,
    alive: Arc<std::sync::atomic::AtomicBool>,
    disconnected: Arc<std::sync::atomic::AtomicBool>,
    reader_abort: tokio::task::AbortHandle,
    /// SASL session expiry: past it the connection is replaced instead of reused.
    sasl_expiry: Option<std::time::Instant>,
    /// Responses successfully read on this connection after its setup (kgo `cxn.successes`).
    successes: std::sync::atomic::AtomicU64,
    /// kprom `node_id` label value (`kgo.NodeName`) and hooks, when the client has them.
    node: String,
    kprom: Option<Arc<KpromMetrics>>,
}

impl Drop for BrokerConn {
    fn drop(&mut self) {
        self.reader_abort.abort();
    }
}

struct PendingGuard<'a> {
    pending: &'a parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>,
    corr_id: i32,
    completed: bool,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.pending.lock().remove(&self.corr_id);
        }
    }
}

impl BrokerConn {
    #[allow(clippy::too_many_arguments)]
    async fn connect(
        addr: String,
        kind: ConnKind,
        client_id: String,
        transport: &KafkaTransport,
        request_timeout: Duration,
        node: String,
        kprom: Option<Arc<KpromMetrics>>,
    ) -> Result<Arc<Self>, KafkaClientError> {
        let established = match transport.connect(&addr, &client_id, request_timeout).await {
            Ok(e) => e,
            Err(e) => {
                if let Some(ref k) = kprom {
                    k.on_broker_connect(&node, false);
                }
                return Err(e);
            }
        };
        if let Some(ref k) = kprom {
            k.on_broker_connect(&node, true);
        }

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
        let disconnected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let disconnected_r = Arc::clone(&disconnected);
        let disconnect_hook = kprom.clone().map(|k| (k, node.clone()));
        let reader_handle = tokio::spawn(async move {
            while let Some(payload) =
                read_kafka_frame_bounded(&mut reader, MAX_KAFKA_FRAME_BYTES).await
            {
                let corr_id = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let tx = pending_r.lock().remove(&corr_id);
                if let Some(tx) = tx {
                    let _ = tx.send(Bytes::from(payload));
                }
            }
            alive_r.store(false, Ordering::SeqCst);
            pending_r.lock().clear();
            if let Some((k, node)) = disconnect_hook {
                if !disconnected_r.swap(true, Ordering::SeqCst) {
                    k.on_broker_disconnect(&node);
                }
            }
        });
        let reader_abort = reader_handle.abort_handle();

        Ok(Arc::new(Self {
            addr,
            kind,
            client_id,
            next_corr_id: AtomicI32::new(established.next_correlation_id),
            write_tx,
            pending,
            api_versions: RwLock::new(established.api_versions),
            alive,
            disconnected,
            reader_abort,
            sasl_expiry: established.sasl_expiry,
            successes: std::sync::atomic::AtomicU64::new(0),
            node,
            kprom,
        }))
    }

    /// `send_raw` with a read deadline: a timed-out read is a kprom read error (kgo kills the
    /// connection and reports `ReadErr` to `OnBrokerE2E`).
    async fn send_raw_timeout<Req, Resp>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, KafkaClientError>
    where
        Req: Request + Encodable,
        Resp: Decodable + HeaderVersion,
    {
        match tokio::time::timeout(timeout, self.send_raw(api_key, api_version, req)).await {
            Ok(res) => res,
            Err(_) => {
                if let Some(ref k) = self.kprom {
                    k.on_read_error(&self.node, 0);
                }
                Err(KafkaClientError::RequestTimeout(timeout))
            }
        }
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// Alive and its SASL session (if any) has not expired: may be handed out for new requests.
    fn is_usable(&self) -> bool {
        self.is_alive() && self.sasl_expiry.is_none_or(|e| std::time::Instant::now() < e)
    }

    fn mark_dead(&self) {
        self.alive.store(false, Ordering::SeqCst);
        self.reader_abort.abort();
        self.pending.lock().clear();
        if let Some(ref k) = self.kprom {
            if !self.disconnected.swap(true, Ordering::SeqCst) {
                k.on_broker_disconnect(&self.node);
            }
        }
    }

    fn pick_version(&self, key: ApiKey, our_min: i16, our_max: i16) -> i16 {
        let map = self.api_versions.read();
        if let Some(&(b_min, b_max)) = map.get(&(key as i16)) {
            let negotiated = our_max.min(b_max);
            if negotiated >= b_min && negotiated >= our_min {
                return negotiated;
            }
        }
        our_min
    }

    async fn send_raw<Req, Resp>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        req: &Req,
    ) -> Result<Resp, KafkaClientError>
    where
        Req: Request + Encodable,
        Resp: Decodable + HeaderVersion,
    {
        if !self.is_alive() {
            return Err(KafkaClientError::ConnectionClosed);
        }

        let corr_id = self.next_corr_id.fetch_add(1, Ordering::Relaxed);
        let req_header_ver = Req::header_version(api_version);
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = api_version;
        header.correlation_id = corr_id;
        header.client_id = if self.client_id.is_empty() {
            None
        } else {
            Some(StrBytes::from_string(self.client_id.clone()))
        };

        let mut body = BytesMut::with_capacity(256);
        body.put_u32(0); // placeholder for 4B BE length
        header.encode(&mut body, req_header_ver)?;
        req.encode(&mut body, api_version)?;

        let frame_len = (body.len() - 4) as u32;
        body[0..4].copy_from_slice(&frame_len.to_be_bytes());

        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(corr_id, tx);
        let mut guard = PendingGuard {
            pending: &self.pending,
            corr_id,
            completed: false,
        };

        let written = body.len();
        if self.write_tx.send(body.freeze()).await.is_err() {
            if let Some(ref k) = self.kprom {
                k.on_write_error(&self.node);
            }
            return Err(KafkaClientError::ConnectionClosed);
        }

        let mut resp_buf = match rx.await {
            Ok(b) => b,
            Err(_) => {
                if let Some(ref k) = self.kprom {
                    k.on_read_error(&self.node, written);
                }
                return Err(KafkaClientError::ConnectionClosed);
            }
        };
        guard.completed = true;
        if let Some(ref k) = self.kprom {
            k.on_e2e(&self.node, written, resp_buf.len() + 4);
        }
        let resp_header_ver = if api_key == ApiKey::ApiVersions {
            0
        } else {
            Resp::header_version(api_version)
        };
        let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver)?;
        self.successes.fetch_add(1, Ordering::Relaxed);
        let resp = Resp::decode(&mut resp_buf, api_version)?;
        Ok(resp)
    }
}

fn is_transport_error(err: &KafkaClientError) -> bool {
    matches!(
        err,
        KafkaClientError::Io(_)
            | KafkaClientError::ConnectionClosed
            | KafkaClientError::RequestTimeout(_)
            | KafkaClientError::ConnectTimeout { .. }
            | KafkaClientError::FirstReadEof(_)
    )
}

fn is_leader_or_io_error(err: &KafkaClientError) -> bool {
    is_transport_error(err)
        || matches!(
            err,
            KafkaClientError::Broker(
                ResponseError::NotLeaderOrFollower
                    | ResponseError::UnknownTopicOrPartition
                    | ResponseError::LeaderNotAvailable
                    | ResponseError::FencedLeaderEpoch
            )
        )
}

/// Discovered brokers sorted by node ID (kgo keeps `cl.brokers` sorted).
fn sorted_brokers(md: &ClusterMetadata) -> Vec<(i32, String)> {
    let mut v: Vec<(i32, String)> = md.brokers.iter().map(|(id, b)| (*id, b.addr.clone())).collect();
    v.sort_by_key(|(id, _)| *id);
    v
}

/// Pure-Rust async TCP Kafka client supporting broker pooling, metadata routing, SASL,
/// `ListOffsets`, `Fetch`, `Produce`, `CreateTopics`, `OffsetFetch`, and `OffsetCommit`.
pub struct KafkaClient {
    seed_brokers: Vec<String>,
    client_id: String,
    client_rack: String,
    request_timeout: Duration,
    transport: Arc<KafkaTransport>,
    conns_by_addr: Mutex<HashMap<(String, ConnKind), Arc<BrokerConn>>>,
    metadata: RwLock<ClusterMetadata>,
    decode_pool: OnceLock<ScopedWorkerPool>,
    /// `-ingest-storage.kafka.last-produced-offset-retry-timeout`: the `ListOffsets` retry timeout
    /// (Mimir's `kgo.RetryTimeoutFn`).
    last_produced_offset_retry_timeout: Duration,
    /// Cached controller broker ID (`-1` = unknown), as kgo's `controllerID`.
    controller_id: AtomicI32,
    any_broker: parking_lot::Mutex<AnyBrokerRotation>,
    /// kprom client hooks (`kgo.WithHooks(metrics)`), set once by the client's owner.
    kprom: OnceLock<Arc<KpromMetrics>>,
}

impl KafkaClient {
    pub fn new(
        seed_brokers: Vec<String>,
        client_id: impl Into<String>,
        client_rack: impl Into<String>,
        dial_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, KafkaClientError> {
        if seed_brokers.is_empty() {
            return Err(KafkaClientError::NoSeedBrokers);
        }
        Ok(Self {
            seed_brokers,
            client_id: client_id.into(),
            client_rack: client_rack.into(),
            request_timeout: if request_timeout.is_zero() {
                Duration::from_secs(10)
            } else {
                request_timeout
            },
            transport: Arc::new(KafkaTransport::plaintext(dial_timeout)),
            conns_by_addr: Mutex::new(HashMap::new()),
            metadata: RwLock::new(ClusterMetadata::default()),
            decode_pool: OnceLock::new(),
            last_produced_offset_retry_timeout: Duration::from_secs(10),
            controller_id: AtomicI32::new(-1),
            any_broker: parking_lot::Mutex::new(AnyBrokerRotation::default()),
            kprom: OnceLock::new(),
        })
    }

    /// Installs the kprom hooks of this client (`kgo.WithHooks(kpromMetrics)`). Only the first
    /// call has an effect; connections opened before it are not instrumented.
    pub fn set_kprom(&self, metrics: Arc<KpromMetrics>) {
        let _ = self.kprom.set(metrics);
    }

    pub fn kprom(&self) -> Option<&Arc<KpromMetrics>> {
        self.kprom.get()
    }

    /// `kgo.NodeName` of the broker a connection to `addr` belongs to.
    fn node_name_for_addr(&self, addr: &str) -> String {
        if let Some((id, _)) = self
            .metadata
            .read()
            .brokers
            .iter()
            .find(|(_, b)| b.addr == addr)
        {
            return id.to_string();
        }
        let idx = self.seed_brokers.iter().position(|s| s == addr).unwrap_or(0);
        kprom::seed_node_name(idx)
    }

    /// `kgo.Client.PartitionLeader`: the cached `(leader, leader_epoch)` of a partition, or
    /// `(-1, -1)` if it is unknown.
    pub fn cached_partition_leader(&self, topic: &str, partition: i32) -> (i32, i32) {
        let md = self.metadata.read();
        let key = (topic.to_string(), partition);
        match md.partition_leaders.get(&key) {
            Some(&leader) => (
                leader,
                md.partition_leader_epochs.get(&key).copied().unwrap_or(-1),
            ),
            None => (-1, -1),
        }
    }

    /// Whether `node_id` is a broker known from the last metadata response.
    pub fn is_known_broker(&self, node_id: i32) -> bool {
        self.metadata.read().brokers.contains_key(&node_id)
    }

    /// kadm `ListTopics(topic)` + `topics[topic].ID`: loads the topic metadata and returns its ID.
    pub async fn topic_id(&self, topic: &str) -> Result<[u8; 16], KafkaClientError> {
        self.refresh_metadata(&[topic]).await?;
        self.metadata
            .read()
            .topic_ids
            .get(topic)
            .copied()
            .ok_or_else(|| KafkaClientError::UnexpectedResponse("topic not found".to_string()))
    }

    /// Sends a `Fetch` request to the broker `node_id` (`req.RequestWith(ctx, cl.Broker(id))`),
    /// negotiating the version up to `max_version`. Returns the response and the version used.
    /// `UnknownBroker` if the broker is not in the current metadata.
    pub async fn fetch_from_broker(
        &self,
        node_id: i32,
        req: &FetchRequest,
        min_version: i16,
        max_version: i16,
        timeout: Duration,
    ) -> Result<(FetchResponse, i16), KafkaClientError> {
        let addr = self.metadata.read().brokers.get(&node_id).map(|b| b.addr.clone());
        let Some(addr) = addr else {
            return Err(KafkaClientError::UnknownBroker);
        };
        let conn = self.load_conn_kind(&addr, ConnKind::Fetch).await?;
        let ver = conn.pick_version(ApiKey::Fetch, min_version, max_version);
        let res = conn.send_raw_timeout(ApiKey::Fetch, ver, req, timeout).await;
        if let Err(ref e) = res {
            if is_transport_error(e) {
                self.drop_conn(&conn).await;
            }
        }
        res.map(|r| (r, ver))
    }

    /// Decodes the record batches of a fetched partition (kgo `ProcessFetchPartition`), invoking
    /// the kprom `OnFetchBatchRead` hook for every batch.
    pub fn decode_fetched_partition(
        &self,
        raw_records: Bytes,
        fetch_offset: i64,
    ) -> Result<(Vec<FetchedRecord>, i64, Option<i64>), KafkaClientError> {
        decode_fetch_records_with_pool(
            raw_records,
            fetch_offset,
            || self.get_decode_pool(),
            self.kprom.get().map(|k| k.as_ref()),
        )
    }

    /// Sets the read deadline used for requests without a timeout field and for the connection
    /// handshake (kgo `RequestTimeoutOverhead`).
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        if !timeout.is_zero() {
            self.request_timeout = timeout;
        }
        self
    }

    /// Sets the `ListOffsets` retry timeout (`last_produced_offset_retry_timeout`; zero = no limit).
    pub fn with_last_produced_offset_retry_timeout(mut self, timeout: Duration) -> Self {
        self.last_produced_offset_retry_timeout = timeout;
        self
    }

    pub fn with_sasl(mut self, sasl: SaslAuthConfig) -> Self {
        self.transport = Arc::new((*self.transport).clone().with_sasl(sasl));
        self
    }

    /// Replaces the dial / TLS / SASL settings used for every broker connection.
    pub fn with_transport(mut self, transport: Arc<KafkaTransport>) -> Self {
        self.transport = transport;
        self
    }

    /// The dial / TLS / SASL settings shared by every connection of this client.
    pub fn transport(&self) -> &Arc<KafkaTransport> {
        &self.transport
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// `-ingest-storage.kafka.client-rack` (kgo `Rack`).
    pub fn client_rack(&self) -> &str {
        &self.client_rack
    }

    fn get_decode_pool(&self) -> &ScopedWorkerPool {
        self.decode_pool.get_or_init(|| {
            let hw = std::thread::available_parallelism().map_or(4, |n| n.get());
            ScopedWorkerPool::new("kafka-decode", hw.clamp(2, 8).saturating_sub(1))
        })
    }

    pub fn from_config(cfg: &KafkaConfig) -> Result<Self, KafkaClientError> {
        let mut addrs = Vec::new();
        for raw in &cfg.address.0 {
            for part in raw.split(',') {
                let trimmed = part.trim();
                if !trimmed.is_empty() {
                    addrs.push(trimmed.to_string());
                }
            }
        }
        let client = Self::new(
            addrs,
            cfg.client_id.clone(),
            cfg.client_rack.clone(),
            Duration::from_nanos(cfg.dial_timeout.as_nanos().max(0) as u64),
            Duration::from_nanos(cfg.write_timeout.as_nanos().max(0) as u64)
                .max(Duration::from_secs(10)),
        )?
        .with_transport(Arc::new(KafkaTransport::from_kafka_config(cfg)?))
        .with_last_produced_offset_retry_timeout(Duration::from_nanos(
            cfg.last_produced_offset_retry_timeout.as_nanos().max(0) as u64,
        ));
        Ok(client)
    }

    async fn get_conn_for_addr(
        &self,
        addr: &str,
        kind: ConnKind,
    ) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let key = (addr.to_string(), kind);
        {
            let mut map = self.conns_by_addr.lock().await;
            if let Some(c) = map.get(&key) {
                if c.is_usable() {
                    return Ok(Arc::clone(c));
                }
                map.remove(&key);
            }
        }

        // Connect and negotiate outside conns_by_addr lock so slow/dead brokers never block others.
        let conn = BrokerConn::connect(
            addr.to_string(),
            kind,
            self.client_id.clone(),
            &self.transport,
            self.request_timeout,
            self.node_name_for_addr(addr),
            self.kprom.get().cloned(),
        )
        .await
        .map_err(|e| {
            let e = classify_connect_error(addr, e);
            if kgo_retry::is_any_dial_err(&e) {
                let err = e.to_string();
                tracing::warn!(
                    component = "kafka_client",
                    addr,
                    err = err.trim_start_matches("unable to dial: "),
                    "unable to open connection to broker"
                );
            }
            e
        })?;

        let mut map = self.conns_by_addr.lock().await;
        if let Some(existing) = map.get(&key) {
            if existing.is_usable() {
                conn.mark_dead();
                return Ok(Arc::clone(existing));
            }
        }
        map.insert(key, Arc::clone(&conn));
        Ok(conn)
    }

    async fn drop_conn(&self, conn: &Arc<BrokerConn>) {
        conn.mark_dead();
        let mut map = self.conns_by_addr.lock().await;
        let key = (conn.addr.clone(), conn.kind);
        if let Some(existing) = map.get(&key) {
            if Arc::ptr_eq(existing, conn) {
                map.remove(&key);
            }
        }
    }

    fn invalidate_partition_leader(&self, topic: &str, partition: i32) {
        self.metadata
            .write()
            .partition_leaders
            .remove(&(topic.to_string(), partition));
    }

    /// Returns a pooled connection, dialing a new one if needed. A new connection failing with a
    /// retryable error is retried once (kgo `broker.handleReq`).
    async fn load_conn(&self, addr: &str) -> Result<Arc<BrokerConn>, KafkaClientError> {
        self.load_conn_kind(addr, ConnKind::Normal).await
    }

    async fn load_conn_kind(
        &self,
        addr: &str,
        kind: ConnKind,
    ) -> Result<Arc<BrokerConn>, KafkaClientError> {
        load_connection_retrying_once(|| self.get_conn_for_addr(addr, kind)).await
    }

    /// `cl.broker()`: the next "any" broker (discovered brokers, then seeds).
    fn next_any_broker(&self) -> Result<BrokerChoice, KafkaClientError> {
        let discovered = sorted_brokers(&self.metadata.read());
        self.any_broker
            .lock()
            .next(&self.seed_brokers, &discovered)
            .ok_or(KafkaClientError::NoSeedBrokers)
    }

    /// Sends `req` on `conn` with a read deadline of `timeout`, dropping the connection on
    /// transport errors.
    async fn send_on<Req, Resp>(
        &self,
        conn: &Arc<BrokerConn>,
        api_key: ApiKey,
        ver: i16,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, KafkaClientError>
    where
        Req: Request + Encodable,
        Resp: Decodable + HeaderVersion,
    {
        let res = conn
            .send_raw_timeout(api_key, ver, req, timeout)
            .await
            .map_err(|e| {
                // kgo handleResp: EOF before any successful response on a connection without SASL
                // is ErrFirstReadEOF ("is SASL missing?"), which is never retried.
                if matches!(e, KafkaClientError::ConnectionClosed)
                    && conn.successes.load(Ordering::Relaxed) == 0
                    && self.transport.sasl().mechanism_name().is_none()
                {
                    tracing::warn!(
                        component = "kafka_client",
                        req = ?api_key,
                        addr = %conn.addr,
                        err = "EOF",
                        "read from broker errored, killing connection after 0 successful responses (is SASL missing?)"
                    );
                    return KafkaClientError::FirstReadEof(
                        kgo_retry::FirstReadEofKind::Sasl.to_string(),
                    );
                }
                e
            });
        if let Err(ref e) = res {
            if is_transport_error(e) {
                self.drop_conn(conn).await;
            }
        }
        res
    }

    /// `cl.fetchMetadata`: a `Metadata` request to any broker with kgo's retry semantics
    /// (`limit_retries = 3` for internal loads, 30s retry timeout). Updates the known brokers and
    /// the controller ID from the response.
    async fn fetch_metadata(
        &self,
        topics: Option<&[&str]>,
        limit_retries: u32,
    ) -> Result<MetadataResponse, KafkaClientError> {
        let mut req = MetadataRequest::default();
        req.allow_auto_topic_creation = false;
        req.topics = topics.map(|ts| {
            ts.iter()
                .map(|t| {
                    let mut mt = MetadataRequestTopic::default();
                    mt.name = Some(TopicName(StrBytes::from_string((*t).to_owned())));
                    mt
                })
                .collect()
        });
        let resp: MetadataResponse = retryable_request(
            mimir_retry_timeout(ApiKey::Metadata, self.last_produced_offset_retry_timeout),
            limit_retries,
            || std::future::ready(self.next_any_broker()),
            |b: BrokerChoice| {
                let req = &req;
                async move {
                    let conn = self.load_conn(&b.addr).await?;
                    // v10+ carries topic IDs (kadm `ListTopics`, ConcurrentFetchers Fetch v13).
                    let ver = conn.pick_version(ApiKey::Metadata, 1, 12);
                    self.send_on(&conn, ApiKey::Metadata, ver, req, self.request_timeout)
                        .await
                }
            },
            |_, _| None,
        )
        .await?;

        if resp.controller_id.0 >= 0 {
            self.controller_id.store(resp.controller_id.0, Ordering::SeqCst);
        }
        {
            let mut md = self.metadata.write();
            md.brokers = resp
                .brokers
                .iter()
                .map(|b| {
                    (
                        b.node_id.0,
                        BrokerInfo {
                            addr: format!("{}:{}", b.host.as_str(), b.port),
                        },
                    )
                })
                .collect();
        }

        let mut conns = self.conns_by_addr.lock().await;
        let live_addrs: std::collections::HashSet<String> = self
            .metadata
            .read()
            .brokers
            .values()
            .map(|b| b.addr.clone())
            .collect();
        if !live_addrs.is_empty() {
            conns.retain(|(addr, _), conn| {
                let keep = live_addrs.contains(addr)
                    || self.seed_brokers.iter().any(|seed| seed == addr);
                if !keep {
                    conn.mark_dead();
                }
                keep
            });
        }
        Ok(resp)
    }

    /// `cl.controller()`: the controller broker, loading broker metadata if it is unknown.
    async fn controller(&self) -> Result<BrokerChoice, KafkaClientError> {
        let mut id = self.controller_id.load(Ordering::SeqCst);
        if id < 0 {
            self.fetch_metadata(Some(&[]), kgo_retry::METADATA_LIMIT_RETRIES)
                .await?;
            id = self.controller_id.load(Ordering::SeqCst);
            if id < 0 {
                return Err(KafkaClientError::UnknownController(id));
            }
        }
        let addr = self.metadata.read().brokers.get(&id).map(|b| b.addr.clone());
        match addr {
            Some(addr) => Ok(BrokerChoice {
                key: BrokerKey::Node(id),
                addr,
            }),
            None => {
                self.forget_controller_id(id);
                Err(KafkaClientError::UnknownController(id))
            }
        }
    }

    fn forget_controller_id(&self, id: i32) {
        let _ = self
            .controller_id
            .compare_exchange(id, -1, Ordering::SeqCst, Ordering::SeqCst);
    }

    async fn any_conn(&self) -> Result<Arc<BrokerConn>, KafkaClientError> {
        self.any_conn_kind(ConnKind::Normal).await
    }

    async fn any_conn_kind(&self, kind: ConnKind) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let mut last_err = None;
        for seed in &self.seed_brokers {
            match self.load_conn_kind(seed, kind).await {
                Ok(c) => return Ok(c),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(KafkaClientError::NoSeedBrokers))
    }

    async fn conn_for_node(
        &self,
        node_id: i32,
        kind: ConnKind,
    ) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let addr = {
            let md = self.metadata.read();
            md.brokers.get(&node_id).map(|b| b.addr.clone())
        };
        if let Some(addr) = addr {
            if let Ok(conn) = self.load_conn_kind(&addr, kind).await {
                return Ok(conn);
            }
        }
        self.any_conn_kind(kind).await
    }

    async fn leader_conn(
        &self,
        topic: &str,
        partition: i32,
        kind: ConnKind,
    ) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let leader = {
            let md = self.metadata.read();
            md.partition_leaders.get(&(topic.to_string(), partition)).copied()
        };
        if let Some(node_id) = leader {
            if node_id >= 0 {
                return self.conn_for_node(node_id, kind).await;
            }
        }
        self.refresh_metadata(&[topic]).await?;
        let leader = {
            let md = self.metadata.read();
            md.partition_leaders.get(&(topic.to_string(), partition)).copied()
        };
        if let Some(node_id) = leader {
            if node_id >= 0 {
                return self.conn_for_node(node_id, kind).await;
            }
        }
        self.any_conn_kind(kind).await
    }

    /// Refreshes cluster & topic metadata from any reachable broker.
    pub async fn refresh_metadata(&self, topics: &[&str]) -> Result<(), KafkaClientError> {
        let topics_opt = if topics.is_empty() { None } else { Some(topics) };
        let resp = self
            .fetch_metadata(topics_opt, kgo_retry::METADATA_LIMIT_RETRIES)
            .await?;

        let mut md = self.metadata.write();
        for t in resp.topics {
            let Some(name) = t.name else {
                continue;
            };
            let topic_name = name.0.as_str().to_owned();
            if let Some(err) = t.error_code.err() {
                return Err(KafkaClientError::Broker(err));
            }
            md.topic_ids
                .insert(topic_name.clone(), *t.topic_id.as_bytes());
            let mut pids = Vec::with_capacity(t.partitions.len());
            for p in t.partitions {
                pids.push(p.partition_index);
                md.partition_leaders
                    .insert((topic_name.clone(), p.partition_index), p.leader_id.0);
                md.partition_leader_epochs
                    .insert((topic_name.clone(), p.partition_index), p.leader_epoch);
            }
            pids.sort();
            md.topic_partitions.insert(topic_name, pids);
        }
        Ok(())
    }

    /// Returns sorted partition IDs for `topic`.
    pub async fn list_topic_partition_ids(
        &self,
        topic: &str,
    ) -> Result<Vec<i32>, KafkaClientError> {
        self.refresh_metadata(&[topic]).await?;
        let md = self.metadata.read();
        Ok(md.topic_partitions.get(topic).cloned().unwrap_or_default())
    }

    /// Creates topics if they do not exist (ignoring `TopicAlreadyExists`).
    ///
    /// Issued like kadm/kgo admin requests (`handleAdminReq`): to the controller (loaded through
    /// broker metadata), retried with backoff on retryable errors, transient dial errors to the
    /// controller and `NOT_CONTROLLER`, for up to the 30s retry timeout.
    pub async fn create_topics(
        &self,
        topics: &[&str],
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<(), KafkaClientError> {
        let resp = self
            .create_topics_request(topics, num_partitions, replication_factor)
            .await?;
        for t in resp.topics {
            if let Some(err) = t.error_code.err() {
                if err != ResponseError::TopicAlreadyExists {
                    return Err(KafkaClientError::Broker(err));
                }
            }
        }
        Ok(())
    }

    /// Issues a `CreateTopics` admin request with kgo `handleAdminReq` semantics and returns the
    /// raw response.
    pub async fn create_topics_request(
        &self,
        topics: &[&str],
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<CreateTopicsResponse, KafkaClientError> {
        let mut req = CreateTopicsRequest::default();
        req.timeout_ms = kgo_retry::KADM_DEFAULT_TIMEOUT_MILLIS;
        for t in topics {
            let mut ct = CreatableTopic::default();
            ct.name = TopicName(StrBytes::from_string((*t).to_owned()));
            ct.num_partitions = num_partitions;
            ct.replication_factor = replication_factor;
            req.topics.push(ct);
        }
        // CreateTopics is a kmsg.TimeoutRequest: read deadline = overhead + TimeoutMillis.
        let read_timeout =
            self.request_timeout + Duration::from_millis(req.timeout_ms.max(0) as u64);

        retryable_request(
            mimir_retry_timeout(ApiKey::CreateTopics, self.last_produced_offset_retry_timeout),
            0,
            || self.controller(),
            |b: BrokerChoice| {
                let req = &req;
                async move {
                    let conn = self.load_conn_kind(&b.addr, ConnKind::Slow).await?;
                    let ver = conn.pick_version(ApiKey::CreateTopics, 0, 5);
                    self.send_on(&conn, ApiKey::CreateTopics, ver, req, read_timeout)
                        .await
                }
            },
            |b: &BrokerChoice, res: &Result<CreateTopicsResponse, KafkaClientError>| {
                let forget = || {
                    if let BrokerKey::Node(id) = b.key {
                        self.forget_controller_id(id);
                    }
                };
                match res {
                    Err(e) => handle_dial_err(e, forget),
                    Ok(resp) => {
                        let code = resp.topics.first().map_or(0, |t| t.error_code);
                        if code == ResponseError::NotController.code() {
                            forget();
                            return Some(KafkaClientError::Broker(ResponseError::NotController));
                        }
                        None
                    }
                }
            },
        )
        .await
    }

    /// Issues `ListOffsets` for a single `(topic, partition)` and `timestamp`.
    /// - `timestamp = -2`: earliest / partition start offset
    /// - `timestamp = -1`: latest (next write offset; last produced offset is `offset - 1`)
    /// - `timestamp >= 0`: first offset with record timestamp >= `timestamp`
    ///
    /// Retried like kgo's sharded `ListOffsets` (`handleShardedReq` + `listOffsetsSharder`): failing
    /// to load the partition leader metadata is not retried; request errors are retried with
    /// backoff while retryable, for up to `last_produced_offset_retry_timeout`; a
    /// `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION` partition error invalidates the
    /// cached leader and re-shards.
    pub async fn list_offset_with_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<ListedOffset, KafkaClientError> {
        let start = tokio::time::Instant::now();
        let retry_timeout =
            mimir_retry_timeout(ApiKey::ListOffsets, self.last_produced_offset_retry_timeout);
        let mut tries = 0u32;
        loop {
            let broker = self.partition_leader_choice(topic, partition).await?;
            tries += 1;

            let mut p = ListOffsetsPartition::default();
            p.partition_index = partition;
            p.current_leader_epoch = -1;
            p.timestamp = timestamp;
            p.max_num_offsets = 1;

            let mut t = ListOffsetsTopic::default();
            t.name = TopicName(StrBytes::from_string(topic.to_owned()));
            t.partitions = vec![p];

            let mut req = ListOffsetsRequest::default();
            req.replica_id = BrokerId(-1);
            req.isolation_level = 0; // READ_UNCOMMITTED
            req.topics = vec![t];

            let res: Result<ListedOffset, KafkaClientError> = async {
                let conn = self.load_conn(&broker.addr).await?;
                let ver = conn.pick_version(ApiKey::ListOffsets, 2, 7);
                let resp: ListOffsetsResponse = self
                    .send_on(&conn, ApiKey::ListOffsets, ver, &req, self.request_timeout)
                    .await?;

                for rt in resp.topics {
                    if rt.name.0.as_str() != topic {
                        continue;
                    }
                    for rp in rt.partitions {
                        if rp.partition_index != partition {
                            continue;
                        }
                        if let Some(err) = rp.error_code.err() {
                            return Err(KafkaClientError::Broker(err));
                        }
                        let offset = if ver == 0 {
                            rp.old_style_offsets.first().copied().unwrap_or(0)
                        } else {
                            rp.offset
                        };
                        return Ok(ListedOffset {
                            offset,
                            timestamp: rp.timestamp,
                        });
                    }
                }
                Err(KafkaClientError::UnexpectedResponse(format!(
                    "missing partition {partition} in ListOffsetsResponse for {topic}"
                )))
            }
            .await;

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    let err = handle_dial_err(&err, || {
                        self.invalidate_partition_leader(topic, partition);
                    })
                    .unwrap_or(err);
                    let stale_leader = matches!(
                        err,
                        KafkaClientError::Broker(
                            ResponseError::NotLeaderOrFollower
                                | ResponseError::UnknownTopicOrPartition
                        )
                    );
                    if stale_leader || is_leader_or_io_error(&err) {
                        self.invalidate_partition_leader(topic, partition);
                    }
                    // listOffsetsSharder.onResp only asks for a retry on stale-leader partition
                    // errors; any other partition error is returned as part of the response.
                    let retryable = !matches!(err, KafkaClientError::Broker(_)) || stale_leader;
                    if retryable {
                        if let Some(backoff) =
                            sharded_retry_backoff(start, retry_timeout, tries, &err)
                        {
                            tokio::time::sleep(backoff).await;
                            continue;
                        }
                    }
                    return Err(err);
                }
            }
        }
    }

    /// The broker a partition request is sharded to: the cached (or freshly loaded) partition
    /// leader, falling back to any broker when the leader is unknown. Metadata load errors are
    /// returned as-is (sharding failures are not retried by kgo).
    async fn partition_leader_choice(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<BrokerChoice, KafkaClientError> {
        let lookup = |md: &ClusterMetadata| -> Option<BrokerChoice> {
            let node = *md.partition_leaders.get(&(topic.to_string(), partition))?;
            if node < 0 {
                return None;
            }
            md.brokers.get(&node).map(|b| BrokerChoice {
                key: BrokerKey::Node(node),
                addr: b.addr.clone(),
            })
        };
        if let Some(b) = lookup(&self.metadata.read()) {
            return Ok(b);
        }
        self.refresh_metadata(&[topic]).await?;
        if let Some(b) = lookup(&self.metadata.read()) {
            return Ok(b);
        }
        self.next_any_broker()
    }

    /// Sharded `ListOffsets` across all requested `(topic, partition_ids)` grouped by each
    /// partition's leader broker, matching `franz-go` `RequestSharded`.
    ///
    /// Retries on `NOT_LEADER_OR_FOLLOWER` / `UNKNOWN_TOPIC_OR_PARTITION` / `LEADER_NOT_AVAILABLE`
    /// and transient I/O errors with metadata invalidation up to `last_produced_offset_retry_timeout`.
    pub async fn list_offsets_sharded(
        &self,
        partitions_by_topic: &HashMap<String, Vec<i32>>,
        timestamp: i64,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaClientError> {
        let mut pending: HashMap<String, Vec<i32>> = HashMap::with_capacity(partitions_by_topic.len());
        let mut out: HashMap<String, HashMap<i32, i64>> =
            HashMap::with_capacity(partitions_by_topic.len());
        for (topic, pids) in partitions_by_topic {
            if pids.is_empty() {
                continue;
            }
            let mut dedup = pids.clone();
            dedup.sort_unstable();
            dedup.dedup();
            pending.insert(topic.clone(), dedup);
            out.insert(topic.clone(), HashMap::with_capacity(pids.len()));
        }
        if pending.is_empty() {
            return Ok(out);
        }

        let start = tokio::time::Instant::now();
        let retry_timeout =
            mimir_retry_timeout(ApiKey::ListOffsets, self.last_produced_offset_retry_timeout);
        let mut tries = 0u32;

        loop {
            // Ensure metadata is populated for any partition lacking a cached leader.
            let missing_topics: Vec<String> = {
                let md = self.metadata.read();
                let mut set = std::collections::HashSet::new();
                for (topic, pids) in &pending {
                    for &pid in pids {
                        let has_leader = md
                            .partition_leaders
                            .get(&(topic.clone(), pid))
                            .copied()
                            .is_some_and(|node| node >= 0 && md.brokers.contains_key(&node));
                        if !has_leader {
                            set.insert(topic.clone());
                        }
                    }
                }
                set.into_iter().collect()
            };
            if !missing_topics.is_empty() {
                let topic_refs: Vec<&str> = missing_topics.iter().map(String::as_str).collect();
                self.refresh_metadata(&topic_refs).await?;
            }

            // Group pending partitions by leader broker address.
            let mut shards: HashMap<String, HashMap<String, Vec<i32>>> = HashMap::new();
            {
                let fallback = self.next_any_broker().ok();
                let md = self.metadata.read();
                for (topic, pids) in &pending {
                    for &pid in pids {
                        let addr = md
                            .partition_leaders
                            .get(&(topic.clone(), pid))
                            .copied()
                            .filter(|&node| node >= 0)
                            .and_then(|node| md.brokers.get(&node).map(|b| b.addr.clone()))
                            .or_else(|| fallback.as_ref().map(|b| b.addr.clone()))
                            .ok_or(KafkaClientError::NoSeedBrokers)?;
                        shards
                            .entry(addr)
                            .or_default()
                            .entry(topic.clone())
                            .or_default()
                            .push(pid);
                    }
                }
            }

            let shard_futs = shards.into_iter().map(|(broker_addr, shard_topics)| async move {
                let mut req = ListOffsetsRequest::default();
                req.replica_id = BrokerId(-1);
                req.isolation_level = 0;
                for (topic, pids) in &shard_topics {
                    let mut lt = ListOffsetsTopic::default();
                    lt.name = TopicName(StrBytes::from_string(topic.clone()));
                    for &pid in pids {
                        let mut lp = ListOffsetsPartition::default();
                        lp.partition_index = pid;
                        lp.current_leader_epoch = -1;
                        lp.timestamp = timestamp;
                        lp.max_num_offsets = 1;
                        lt.partitions.push(lp);
                    }
                    req.topics.push(lt);
                }

                let res: Result<(i16, ListOffsetsResponse), KafkaClientError> = async {
                    let conn = self.load_conn(&broker_addr).await?;
                    let ver = conn.pick_version(ApiKey::ListOffsets, 2, 7);
                    let resp: ListOffsetsResponse = self
                        .send_on(&conn, ApiKey::ListOffsets, ver, &req, self.request_timeout)
                        .await?;
                    Ok((ver, resp))
                }
                .await;

                (shard_topics, res)
            });

            let results = futures::future::join_all(shard_futs).await;

            let mut retry_pending: HashMap<String, Vec<i32>> = HashMap::new();
            let mut last_err: Option<KafkaClientError> = None;

            for (shard_topics, res) in results {
                match res {
                    Ok((ver, resp)) => {
                        let mut shard_ok: HashMap<(String, i32), i64> = HashMap::new();
                        let mut shard_retry: HashMap<(String, i32), ResponseError> = HashMap::new();

                        for rt in resp.topics {
                            let topic_name = rt.name.0.as_str();
                            if !shard_topics.contains_key(topic_name) {
                                return Err(KafkaClientError::UnexpectedResponse(format!(
                                    "unexpected topic in the response: {topic_name}"
                                )));
                            }
                            for rp in rt.partitions {
                                let pid = rp.partition_index;
                                if let Some(err) = rp.error_code.err() {
                                    let stale_leader = matches!(
                                        err,
                                        ResponseError::NotLeaderOrFollower
                                            | ResponseError::UnknownTopicOrPartition
                                            | ResponseError::LeaderNotAvailable
                                    );
                                    if stale_leader {
                                        self.invalidate_partition_leader(topic_name, pid);
                                        shard_ok.remove(&(topic_name.to_string(), pid));
                                        shard_retry.insert((topic_name.to_string(), pid), err);
                                        last_err = Some(KafkaClientError::Broker(err));
                                    } else {
                                        return Err(KafkaClientError::Broker(err));
                                    }
                                } else if !shard_retry.contains_key(&(topic_name.to_string(), pid))
                                {
                                    let offset = if ver == 0 {
                                        rp.old_style_offsets.first().copied().unwrap_or(0)
                                    } else {
                                        rp.offset
                                    };
                                    shard_ok.insert((topic_name.to_string(), pid), offset);
                                }
                            }
                        }

                        for (topic, pids) in &shard_topics {
                            for &pid in pids {
                                let key = (topic.clone(), pid);
                                if let Some(off) = shard_ok.get(&key) {
                                    out.entry(topic.clone()).or_default().insert(pid, *off);
                                } else if shard_retry.contains_key(&key) || !shard_retry.is_empty() {
                                    retry_pending.entry(topic.clone()).or_default().push(pid);
                                } else {
                                    return Err(KafkaClientError::UnexpectedResponse(format!(
                                        "the offset for topic {topic:?} partition {pid} is missing in the response"
                                    )));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        let err = handle_dial_err(&err, || {
                            for (topic, pids) in &shard_topics {
                                for &pid in pids {
                                    self.invalidate_partition_leader(topic, pid);
                                }
                            }
                        })
                        .unwrap_or(err);
                        let retryable = !matches!(err, KafkaClientError::Broker(_));
                        for (topic, pids) in &shard_topics {
                            for &pid in pids {
                                self.invalidate_partition_leader(topic, pid);
                            }
                        }
                        if !retryable {
                            return Err(err);
                        }
                        for (topic, pids) in shard_topics {
                            retry_pending.entry(topic).or_default().extend(pids);
                        }
                        last_err = Some(err);
                    }
                }
            }

            if retry_pending.is_empty() {
                return Ok(out);
            }

            tries += 1;
            let err = last_err.unwrap_or_else(|| {
                KafkaClientError::UnexpectedResponse("list_offsets_sharded retry".to_string())
            });
            let Some(backoff) = sharded_retry_backoff(start, retry_timeout, tries, &err) else {
                return Err(err);
            };
            tokio::time::sleep(backoff).await;
            pending = retry_pending;
        }
    }

    pub async fn list_offset(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64, KafkaClientError> {
        Ok(self
            .list_offset_with_timestamp(topic, partition, timestamp)
            .await?
            .offset)
    }

    /// Returns the partition start offset (`ListOffsets` with `timestamp = -2`).
    pub async fn fetch_partition_start_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, KafkaClientError> {
        self.list_offset(topic, partition, KAFKA_OFFSET_START).await
    }

    /// Returns the last produced offset (`ListOffsets(timestamp = -1) - 1`), or `-1` if empty.
    /// Matches `pkg/storage/ingest/DESIGN.md` & `partition_offset_client.go` 1:1.
    pub async fn fetch_partition_last_produced_offset(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, KafkaClientError> {
        let next_offset = self.list_offset(topic, partition, KAFKA_OFFSET_END).await?;
        Ok(next_offset - 1)
    }

    /// Fetches records from `(topic, partition)` starting at `fetch_offset`.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        max_wait: Duration,
        min_bytes: i32,
        max_bytes: i32,
    ) -> Result<FetchPartitionResult, KafkaClientError> {
        let wait_timeout = self.request_timeout.max(max_wait + Duration::from_secs(5));
        for attempt in 0..2 {
            let conn = self.leader_conn(topic, partition, ConnKind::Fetch).await?;
            let ver = conn.pick_version(ApiKey::Fetch, 4, 12);

            let mut fp = FetchPartition::default();
            fp.partition = partition;
            fp.current_leader_epoch = -1;
            fp.fetch_offset = fetch_offset;
            fp.last_fetched_epoch = -1;
            fp.log_start_offset = -1;
            fp.partition_max_bytes = max_bytes;

            let mut ft = FetchTopic::default();
            ft.topic = TopicName(StrBytes::from_string(topic.to_owned()));
            ft.partitions = vec![fp];

            let mut req = FetchRequest::default();
            req.replica_id = BrokerId(-1);
            req.max_wait_ms = max_wait.as_millis().min(i32::MAX as u128) as i32;
            req.min_bytes = min_bytes;
            req.max_bytes = max_bytes;
            req.isolation_level = 0; // READ_UNCOMMITTED
            req.session_id = 0;
            req.session_epoch = -1;
            req.topics = vec![ft];
            // No rack: this one-shot fetch always reads from the partition leader. Sending the
            // rack without honouring `preferred_read_replica` makes a rack-aware broker answer
            // with an empty redirect forever (KAFKA-1). Rack-aware consumption lives in
            // `client_consumer.rs` (the kgo consumer path).

            let res: Result<FetchPartitionResult, KafkaClientError> = async {
                let resp: FetchResponse = conn.send_raw_timeout(ApiKey::Fetch, ver, &req, wait_timeout).await?;

                if let Some(err) = resp.error_code.err() {
                    return Err(KafkaClientError::Broker(err));
                }

                for rt in resp.responses {
                    if rt.topic.0.as_str() != topic {
                        continue;
                    }
                    for rp in rt.partitions {
                        if rp.partition_index != partition {
                            continue;
                        }
                        if let Some(err) = rp.error_code.err() {
                            return Err(KafkaClientError::Broker(err));
                        }

                        let (decoded_records, next_fetch_offset, last_batch_end_offset) =
                            match rp.records {
                                Some(raw_records) if !raw_records.is_empty() => {
                                    self.decode_fetched_partition(raw_records, fetch_offset)?
                                }
                                _ => (Vec::new(), fetch_offset, None),
                            };

                        return Ok(FetchPartitionResult {
                            high_watermark: rp.high_watermark,
                            log_start_offset: rp.log_start_offset,
                            records: decoded_records,
                            next_fetch_offset,
                            last_batch_end_offset,
                        });
                    }
                }

                Err(KafkaClientError::UnexpectedResponse(format!(
                    "missing partition {partition} in FetchResponse for {topic}"
                )))
            }
            .await;

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    if is_transport_error(&err) {
                        self.drop_conn(&conn).await;
                    }
                    if attempt == 0 && is_leader_or_io_error(&err) {
                        self.invalidate_partition_leader(topic, partition);
                        let _ = self.refresh_metadata(&[topic]).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        unreachable!()
    }

    /// Produces a batch of records to `(topic, partition)` as a RecordBatch v2 with CRC32C
    /// and the specified `compression`, returning the broker-assigned `base_offset`.
    pub async fn produce(
        &self,
        topic: &str,
        partition: i32,
        compression: KafkaCompression,
        records: Vec<ProduceRecord>,
    ) -> Result<i64, KafkaClientError> {
        if records.is_empty() {
            return Ok(0);
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let proto_records: Vec<Record> = records
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                let mut headers = IndexMap::with_capacity(r.headers.len());
                for (k, v) in r.headers {
                    headers.insert(StrBytes::from_string(k), Some(v));
                }
                Record {
                    transactional: false,
                    control: false,
                    partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    producer_id: NO_PRODUCER_ID,
                    producer_epoch: NO_PRODUCER_EPOCH,
                    timestamp_type: TimestampType::Creation,
                    offset: i as i64,
                    sequence: (i as i32).wrapping_add(NO_SEQUENCE),
                    timestamp: r.timestamp_ms.unwrap_or(now_ms),
                    key: r.key,
                    value: r.value,
                    headers,
                }
            })
            .collect();

        let mut batch_buf = BytesMut::new();
        let encode_opts = RecordEncodeOptions {
            version: 2,
            compression: compression.to_protocol(),
        };
        RecordBatchEncoder::encode_with_custom_compression(
            &mut batch_buf,
            proto_records.iter(),
            &encode_opts,
            Some(compress_record_batch),
        )?;
        let payload = batch_buf.freeze();

        for attempt in 0..2 {
            let conn = self.leader_conn(topic, partition, ConnKind::Produce).await?;
            let ver = conn.pick_version(ApiKey::Produce, 3, 9);

            let mut pp = PartitionProduceData::default();
            pp.index = partition;
            pp.records = Some(payload.clone());

            let mut tp = TopicProduceData::default();
            tp.name = TopicName(StrBytes::from_string(topic.to_owned()));
            tp.partition_data = vec![pp];

            let mut req = ProduceRequest::default();
            req.transactional_id = None;
            req.acks = -1;
            req.timeout_ms = self.request_timeout.as_millis().min(i32::MAX as u128) as i32;
            req.topic_data = vec![tp];

            let res: Result<i64, KafkaClientError> = async {
                let resp: ProduceResponse = conn.send_raw_timeout(ApiKey::Produce, ver, &req, self.request_timeout).await?;

                for rt in resp.responses {
                    if rt.name.0.as_str() != topic {
                        continue;
                    }
                    for rp in rt.partition_responses {
                        if rp.index != partition {
                            continue;
                        }
                        if let Some(err) = rp.error_code.err() {
                            return Err(KafkaClientError::Broker(err));
                        }
                        return Ok(rp.base_offset);
                    }
                }

                Err(KafkaClientError::UnexpectedResponse(format!(
                    "missing partition {partition} in ProduceResponse for {topic}"
                )))
            }
            .await;

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    if is_transport_error(&err) {
                        self.drop_conn(&conn).await;
                    }
                    if attempt == 0 && is_leader_or_io_error(&err) {
                        self.invalidate_partition_leader(topic, partition);
                        let _ = self.refresh_metadata(&[topic]).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        unreachable!()
    }

    async fn find_group_coordinator(
        &self,
        group_id: &str,
    ) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let conn = self.any_conn().await?;
        let ver = conn.pick_version(ApiKey::FindCoordinator, 0, 3);
        let mut req = FindCoordinatorRequest::default();
        req.key = StrBytes::from_string(group_id.to_owned());
        req.key_type = 0; // GROUP

        let resp: FindCoordinatorResponse = conn.send_raw_timeout(ApiKey::FindCoordinator, ver, &req, self.request_timeout).await?;

        if let Some(err) = resp.error_code.err() {
            return Err(KafkaClientError::Broker(err));
        }
        let addr = format!("{}:{}", resp.host.as_str(), resp.port);
        self.load_conn(&addr).await
    }

    /// Commits a consumer group offset for `(topic, partition)`.
    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), KafkaClientError> {
        if group_id.is_empty() {
            return Ok(());
        }
        let start = tokio::time::Instant::now();
        let mut tries = 0u32;
        loop {
            tries += 1;
            let conn = match self.find_group_coordinator(group_id).await {
                Ok(c) => c,
                Err(err) => match sharded_retry_backoff(start, kgo_retry::DEFAULT_RETRY_TIMEOUT, tries, &err) {
                    Some(backoff) => {
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    None => return Err(err),
                },
            };
            let ver = conn.pick_version(ApiKey::OffsetCommit, 2, 8);

            let mut p = OffsetCommitRequestPartition::default();
            p.partition_index = partition;
            p.committed_offset = offset;
            p.committed_leader_epoch = -1;
            p.commit_timestamp = -1;

            let mut t = OffsetCommitRequestTopic::default();
            t.name = TopicName(StrBytes::from_string(topic.to_owned()));
            t.partitions = vec![p];

            let mut req = OffsetCommitRequest::default();
            req.group_id = GroupId(StrBytes::from_string(group_id.to_owned()));
            req.generation_id_or_member_epoch = -1;
            req.topics = vec![t];

            let res: Result<(), KafkaClientError> = async {
                let resp: OffsetCommitResponse = conn.send_raw_timeout(ApiKey::OffsetCommit, ver, &req, self.request_timeout).await?;

                for rt in resp.topics {
                    if rt.name.0.as_str() != topic {
                        continue;
                    }
                    for rp in rt.partitions {
                        if rp.partition_index == partition {
                            if let Some(err) = rp.error_code.err() {
                                return Err(KafkaClientError::Broker(err));
                            }
                            return Ok(());
                        }
                    }
                }
                Ok(())
            }
            .await;

            match res {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if is_transport_error(&err) {
                        self.drop_conn(&conn).await;
                    }
                    if let Some(backoff) = sharded_retry_backoff(
                        start,
                        kgo_retry::DEFAULT_RETRY_TIMEOUT,
                        tries,
                        &err,
                    ) {
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    /// Fetches the committed consumer group offset for `(topic, partition)`, returning `None` if
    /// no offset is committed yet (`offset < 0`).
    pub async fn fetch_committed_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, KafkaClientError> {
        if group_id.is_empty() {
            return Ok(None);
        }
        let start = tokio::time::Instant::now();
        let mut tries = 0u32;
        loop {
            tries += 1;
            let conn = match self.find_group_coordinator(group_id).await {
                Ok(c) => c,
                Err(err) => match sharded_retry_backoff(start, kgo_retry::DEFAULT_RETRY_TIMEOUT, tries, &err) {
                    Some(backoff) => {
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    None => return Err(err),
                },
            };
            let ver = conn.pick_version(ApiKey::OffsetFetch, 1, 7);

            let mut t = OffsetFetchRequestTopic::default();
            t.name = TopicName(StrBytes::from_string(topic.to_owned()));
            t.partition_indexes = vec![partition];

            let mut req = OffsetFetchRequest::default();
            req.group_id = GroupId(StrBytes::from_string(group_id.to_owned()));
            req.topics = Some(vec![t]);

            let res: Result<Option<i64>, KafkaClientError> = async {
                let resp: OffsetFetchResponse = conn.send_raw_timeout(ApiKey::OffsetFetch, ver, &req, self.request_timeout).await?;

                if let Some(err) = resp.error_code.err() {
                    if err == ResponseError::GroupIdNotFound {
                        return Ok(None);
                    }
                    return Err(KafkaClientError::Broker(err));
                }

                for rt in resp.topics {
                    if rt.name.0.as_str() != topic {
                        continue;
                    }
                    for rp in rt.partitions {
                        if rp.partition_index == partition {
                            if let Some(err) = rp.error_code.err() {
                                if err == ResponseError::UnknownTopicOrPartition
                                    || err == ResponseError::GroupIdNotFound
                                {
                                    return Ok(None);
                                }
                                return Err(KafkaClientError::Broker(err));
                            }
                            if rp.committed_offset < 0 {
                                return Ok(None);
                            }
                            return Ok(Some(rp.committed_offset));
                        }
                    }
                }
                Ok(None)
            }
            .await;

            match res {
                Ok(v) => return Ok(v),
                Err(err) => {
                    if is_transport_error(&err) {
                        self.drop_conn(&conn).await;
                    }
                    if let Some(backoff) = sharded_retry_backoff(
                        start,
                        kgo_retry::DEFAULT_RETRY_TIMEOUT,
                        tries,
                        &err,
                    ) {
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }
}

struct V2FrameInfo {
    slice: Bytes,
    base_offset: i64,
    last_offset_delta: i32,
}

/// RecordBatch v2 header layout (offsets within a frame).
const V2_ATTRIBUTES_OFFSET: usize = 21;
const V2_FIRST_TIMESTAMP_OFFSET: usize = 27;
const V2_MAX_TIMESTAMP_OFFSET: usize = 35;
const V2_NUM_RECORDS_OFFSET: usize = 57;
/// Size of the RecordBatch v2 header preceding the records section.
const V2_HEADER_LEN: usize = 61;
/// Timestamp type attribute bit: set for `LogAppendTime` batches.
const V2_TIMESTAMP_TYPE_LOG_APPEND_TIME: i16 = 0x08;

fn be_i64(raw: &[u8], at: usize) -> i64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&raw[at..at + 8]);
    i64::from_be_bytes(b)
}

fn split_record_batch_v2_frames(raw: &Bytes) -> Result<Option<Vec<V2FrameInfo>>, KafkaClientError> {
    let mut frames = Vec::new();
    let mut pos = 0usize;
    let total = raw.len();
    while pos < total {
        if pos + 17 > total {
            if frames.is_empty() {
                return Err(KafkaClientError::UnexpectedResponse(
                    "first record batch truncated in fetch response (header < 17 bytes)".to_string(),
                ));
            }
            break;
        }
        if raw[pos + 16] != 2 {
            if frames.is_empty() {
                return Ok(None);
            }
            break;
        }
        let batch_len =
            i32::from_be_bytes([raw[pos + 8], raw[pos + 9], raw[pos + 10], raw[pos + 11]]);
        if batch_len < 49 {
            if frames.is_empty() {
                return Err(KafkaClientError::UnexpectedResponse(format!(
                    "invalid RecordBatch v2 length {batch_len}"
                )));
            }
            break;
        }
        let Some(frame_len) = 12usize.checked_add(batch_len as usize) else {
            if frames.is_empty() {
                return Err(KafkaClientError::UnexpectedResponse(
                    "overflow computing RecordBatch v2 frame length".to_string(),
                ));
            }
            break;
        };
        if pos + frame_len > total {
            // Apache Kafka may truncate the trailing batch at max_partition_bytes.
            // Decode all complete preceding batches; error only if 0 complete batches fit.
            if frames.is_empty() {
                return Err(KafkaClientError::UnexpectedResponse(format!(
                    "first record batch ({frame_len} bytes) truncated to {} bytes (exceeds max_partition_bytes)",
                    total - pos
                )));
            }
            break;
        }
        let base_offset = be_i64(raw, pos);
        let last_offset_delta = i32::from_be_bytes([
            raw[pos + 23],
            raw[pos + 24],
            raw[pos + 25],
            raw[pos + 26],
        ]);
        frames.push(V2FrameInfo {
            slice: raw.slice(pos..pos + frame_len),
            base_offset,
            last_offset_delta,
        });
        pos += frame_len;
    }
    Ok(Some(frames))
}

#[inline(always)]
fn read_zigzag_i32(buf: &[u8], pos: &mut usize) -> Result<i32, KafkaClientError> {
    let b0 = *buf.get(*pos).ok_or_else(|| {
        KafkaClientError::Protocol(anyhow::anyhow!("unexpected EOF reading Kafka varint"))
    })?;
    if b0 < 0x80 {
        *pos += 1;
        let z = b0 as u32;
        return Ok(((z >> 1) as i32) ^ (-((z & 1) as i32)));
    }
    read_zigzag_i32_slow(buf, pos)
}

#[cold]
fn read_zigzag_i32_slow(buf: &[u8], pos: &mut usize) -> Result<i32, KafkaClientError> {
    let mut z = 0u32;
    let mut shift = 0u32;
    for i in 0..5 {
        let b = *buf.get(*pos).ok_or_else(|| {
            KafkaClientError::Protocol(anyhow::anyhow!("unexpected EOF reading Kafka varint"))
        })?;
        *pos += 1;
        z |= ((b & 0x7F) as u32) << shift;
        if b < 0x80 {
            return Ok(((z >> 1) as i32) ^ (-((z & 1) as i32)));
        }
        if i == 4 {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "Kafka varint exceeds 5 bytes"
            )));
        }
        shift += 7;
    }
    unreachable!()
}

#[inline(always)]
fn read_zigzag_i64(buf: &[u8], pos: &mut usize) -> Result<i64, KafkaClientError> {
    let b0 = *buf.get(*pos).ok_or_else(|| {
        KafkaClientError::Protocol(anyhow::anyhow!("unexpected EOF reading Kafka varlong"))
    })?;
    if b0 < 0x80 {
        *pos += 1;
        let z = b0 as u64;
        return Ok(((z >> 1) as i64) ^ (-((z & 1) as i64)));
    }
    read_zigzag_i64_slow(buf, pos)
}

#[cold]
fn read_zigzag_i64_slow(buf: &[u8], pos: &mut usize) -> Result<i64, KafkaClientError> {
    let mut z = 0u64;
    let mut shift = 0u32;
    for i in 0..10 {
        let b = *buf.get(*pos).ok_or_else(|| {
            KafkaClientError::Protocol(anyhow::anyhow!("unexpected EOF reading Kafka varlong"))
        })?;
        *pos += 1;
        z |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Ok(((z >> 1) as i64) ^ (-((z & 1) as i64)));
        }
        if i == 9 {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "Kafka varlong exceeds 10 bytes"
            )));
        }
        shift += 7;
    }
    unreachable!()
}

/// Decodes one RecordBatch v2 frame directly (zero `IndexMap` / `Vec<Record>` allocations),
/// returning the records at or after `fetch_offset` and the kgo `FetchBatchMetrics` of the batch.
fn decode_single_batch_slice(
    slice: Bytes,
    fetch_offset: i64,
) -> Result<(Vec<FetchedRecord>, FetchBatchMetrics), KafkaClientError> {
    let frame_len = slice.len();
    if frame_len < V2_HEADER_LEN {
        return Err(KafkaClientError::UnexpectedResponse(format!(
            "RecordBatch v2 frame too short: {frame_len} < {V2_HEADER_LEN}"
        )));
    }
    if slice[16] != 2 {
        return Err(KafkaClientError::UnexpectedResponse(format!(
            "unexpected RecordBatch magic {}, expected 2",
            slice[16]
        )));
    }
    let supplied_crc = u32::from_be_bytes([slice[17], slice[18], slice[19], slice[20]]);
    let actual_crc = crc32c::crc32c(&slice[V2_ATTRIBUTES_OFFSET..]);
    if supplied_crc != actual_crc {
        return Err(KafkaClientError::Protocol(anyhow::anyhow!(
            "Cyclic redundancy check failed ({supplied_crc} != {actual_crc})"
        )));
    }

    let attributes = i16::from_be_bytes([
        slice[V2_ATTRIBUTES_OFFSET],
        slice[V2_ATTRIBUTES_OFFSET + 1],
    ]);
    let base_offset = be_i64(&slice, 0);
    let first_timestamp = be_i64(&slice, V2_FIRST_TIMESTAMP_OFFSET);
    let max_timestamp = be_i64(&slice, V2_MAX_TIMESTAMP_OFFSET);
    let num_records = i32::from_be_bytes([
        slice[V2_NUM_RECORDS_OFFSET],
        slice[V2_NUM_RECORDS_OFFSET + 1],
        slice[V2_NUM_RECORDS_OFFSET + 2],
        slice[V2_NUM_RECORDS_OFFSET + 3],
    ]);
    if num_records < 0 {
        return Err(KafkaClientError::Protocol(anyhow::anyhow!(
            "Unexpected negative record count ({num_records})"
        )));
    }
    let compression = match attributes & 0x07 {
        0 => Compression::None,
        1 => Compression::Gzip,
        2 => Compression::Snappy,
        3 => Compression::Lz4,
        4 => Compression::Zstd,
        other => {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "Unknown compression algorithm used: {other}"
            )));
        }
    };
    let is_control = (attributes & 0x20) != 0;
    let log_append_time = attributes & V2_TIMESTAMP_TYPE_LOG_APPEND_TIME != 0;

    // kgo: CompressedBytes = len(batch.Records) (the records section as sent), UncompressedBytes
    // = its decompressed length (equal when the batch is not compressed).
    let compressed_bytes = frame_len - V2_HEADER_LEN;
    let records_bytes = if matches!(compression, Compression::None) {
        slice.slice(V2_HEADER_LEN..)
    } else {
        let mut compressed_slice = slice.slice(V2_HEADER_LEN..);
        decompress_record_batch(&mut compressed_slice, compression)?
    };
    let uncompressed_bytes = records_bytes.len();
    let metrics = FetchBatchMetrics {
        num_records: num_records as usize,
        uncompressed_bytes: if uncompressed_bytes == 0 {
            compressed_bytes
        } else {
            uncompressed_bytes
        },
        compressed_bytes,
    };

    let record_count = num_records as usize;
    let buf = records_bytes.as_ref();
    let mut pos = 0usize;
    // Bound upfront Vec capacity by minimum possible v2 record size (~6B) to avoid OOM on corrupt headers.
    let mut out = if is_control {
        Vec::new()
    } else {
        Vec::with_capacity(record_count.min(buf.len() / 6))
    };

    for _ in 0..record_count {
        let rec_size = read_zigzag_i32(buf, &mut pos)?;
        if rec_size < 0 {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "Unexpected negative record size: {rec_size}"
            )));
        }
        let rec_end = pos
            .checked_add(rec_size as usize)
            .filter(|&end| end <= buf.len())
            .ok_or_else(|| {
                KafkaClientError::Protocol(anyhow::anyhow!(
                    "truncated Kafka v2 record (size {rec_size}, remaining {})",
                    buf.len() - pos
                ))
            })?;
        let rec_base = pos;
        let rec_slice = &buf[rec_base..rec_end];
        pos = rec_end;

        let mut rp = 0usize;
        if rp >= rec_slice.len() {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "unexpected EOF reading record attributes"
            )));
        }
        // 1-byte record attributes (unused in v2).
        rp += 1;

        let timestamp_delta = read_zigzag_i64(rec_slice, &mut rp)?;
        let offset_delta = read_zigzag_i32(rec_slice, &mut rp)?;
        let offset = base_offset.wrapping_add(offset_delta as i64);

        let key_len = read_zigzag_i32(rec_slice, &mut rp)?;
        if key_len < -1 {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "Unexpected negative record key length ({key_len} bytes)"
            )));
        }
        let key_range = if key_len == -1 {
            None
        } else {
            let klen = key_len as usize;
            let kend = rp
                .checked_add(klen)
                .filter(|&e| e <= rec_slice.len())
                .ok_or_else(|| {
                    KafkaClientError::Protocol(anyhow::anyhow!(
                        "truncated Kafka record key ({klen} bytes)"
                    ))
                })?;
            let range = (rec_base + rp)..(rec_base + kend);
            rp = kend;
            Some(range)
        };

        let val_len = read_zigzag_i32(rec_slice, &mut rp)?;
        if val_len < -1 {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "Unexpected negative record value length ({val_len} bytes)"
            )));
        }
        let val_range = if val_len == -1 {
            None
        } else {
            let vlen = val_len as usize;
            let vend = rp
                .checked_add(vlen)
                .filter(|&e| e <= rec_slice.len())
                .ok_or_else(|| {
                    KafkaClientError::Protocol(anyhow::anyhow!(
                        "truncated Kafka record value ({vlen} bytes)"
                    ))
                })?;
            let range = (rec_base + rp)..(rec_base + vend);
            rp = vend;
            Some(range)
        };

        let num_headers = read_zigzag_i32(rec_slice, &mut rp)?;
        if num_headers < 0 {
            return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                "Unexpected negative record header count: {num_headers}"
            )));
        }
        let num_headers = num_headers as usize;
        let skip = is_control || offset < fetch_offset;
        let mut headers = if skip || num_headers == 0 {
            Vec::new()
        } else {
            Vec::with_capacity(num_headers.min(rec_slice.len().saturating_sub(rp) / 2))
        };

        for _ in 0..num_headers {
            let hk_len = read_zigzag_i32(rec_slice, &mut rp)?;
            if hk_len < 0 {
                return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                    "Unexpected negative record header key length ({hk_len} bytes)"
                )));
            }
            let hk_len = hk_len as usize;
            let hk_end = rp
                .checked_add(hk_len)
                .filter(|&e| e <= rec_slice.len())
                .ok_or_else(|| {
                    KafkaClientError::Protocol(anyhow::anyhow!(
                        "truncated Kafka header key ({hk_len} bytes)"
                    ))
                })?;
            let hk_str = std::str::from_utf8(&rec_slice[rp..hk_end])
                .map_err(|e| KafkaClientError::Protocol(e.into()))?;
            rp = hk_end;

            let hv_len = read_zigzag_i32(rec_slice, &mut rp)?;
            if hv_len < -1 {
                return Err(KafkaClientError::Protocol(anyhow::anyhow!(
                    "Unexpected negative record header value length ({hv_len} bytes)"
                )));
            }
            let hv = if hv_len == -1 {
                Bytes::new()
            } else {
                let hlen = hv_len as usize;
                let hv_end = rp
                    .checked_add(hlen)
                    .filter(|&e| e <= rec_slice.len())
                    .ok_or_else(|| {
                        KafkaClientError::Protocol(anyhow::anyhow!(
                            "truncated Kafka header value ({hlen} bytes)"
                        ))
                    })?;
                let bytes = if skip {
                    Bytes::new()
                } else {
                    records_bytes.slice((rec_base + rp)..(rec_base + hv_end))
                };
                rp = hv_end;
                bytes
            };

            if !skip {
                headers.push((hk_str.to_owned(), hv));
            }
        }

        if !skip {
            let key = match key_range {
                Some(r) => records_bytes.slice(r),
                None => Bytes::new(),
            };
            let value = match val_range {
                Some(r) => records_bytes.slice(r),
                None => Bytes::new(),
            };
            out.push(FetchedRecord {
                offset,
                // franz-go source.go: `if batch.Attributes&0x0008 != 0 { r.Timestamp = MaxTimestamp }`
                // (LogAppendTime batches carry the broker append time as the batch max timestamp).
                timestamp_ms: if log_append_time {
                    max_timestamp
                } else {
                    first_timestamp.wrapping_add(timestamp_delta)
                },
                key,
                value,
                headers,
            });
        }
    }

    Ok((out, metrics))
}

/// Decodes the record batches of a fetched partition, returning
/// `(records >= fetch_offset, next_fetch_offset, last_batch_end_offset)`. `kprom` receives the
/// `OnFetchBatchRead` hook for every complete batch.
pub(crate) fn decode_fetch_records_with_pool<'a>(
    mut raw_records: Bytes,
    fetch_offset: i64,
    get_pool: impl FnOnce() -> &'a ScopedWorkerPool,
    kprom: Option<&KpromMetrics>,
) -> Result<(Vec<FetchedRecord>, i64, Option<i64>), KafkaClientError> {
    if raw_records.is_empty() {
        return Ok((Vec::new(), fetch_offset, None));
    }

    if let Some(frames) = split_record_batch_v2_frames(&raw_records)? {
        let mut last_batch_end_offset: Option<i64> = None;
        for f in &frames {
            let batch_end = f.base_offset.saturating_add(f.last_offset_delta as i64);
            if batch_end >= fetch_offset {
                last_batch_end_offset = Some(
                    last_batch_end_offset.map_or(batch_end, |prev| prev.max(batch_end)),
                );
            }
        }
        let next_fetch_offset =
            last_batch_end_offset.map_or(fetch_offset, |end| fetch_offset.max(end + 1));

        // Verify CRC32C and decode EVERY frame (including `is_control` batches) inside
        // `decode_single_batch_slice` so a bit-flip setting `attributes & 0x20` on a data
        // batch cannot bypass CRC32C and silently drop records.
        let all_frames: Vec<Bytes> = frames.into_iter().map(|f| f.slice).collect();

        let observe = |m: FetchBatchMetrics| {
            if let Some(k) = kprom {
                k.on_fetch_batch_read(m);
            }
        };

        if all_frames.len() > 1 {
            let pool = get_pool();
            type BatchDecodeSlot = parking_lot::Mutex<
                Option<Result<(Vec<FetchedRecord>, FetchBatchMetrics), KafkaClientError>>,
            >;
            let slots: Vec<BatchDecodeSlot> =
                (0..all_frames.len()).map(|_| parking_lot::Mutex::new(None)).collect();
            let run = || {
                pool.scoped_execute(all_frames.len(), |idx| {
                    let res = decode_single_batch_slice(all_frames[idx].clone(), fetch_offset);
                    *slots[idx].lock() = Some(res);
                });
            };
            if tokio::runtime::Handle::try_current()
                .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
            {
                tokio::task::block_in_place(run);
            } else {
                run();
            }
            let mut decoded = Vec::new();
            for slot in slots {
                let (recs, m) = slot
                    .into_inner()
                    .expect("parallel batch slot populated")?;
                observe(m);
                if decoded.is_empty() {
                    decoded = recs;
                } else {
                    decoded.extend(recs);
                }
            }
            return Ok((decoded, next_fetch_offset, last_batch_end_offset));
        }
        if all_frames.len() == 1 {
            let (decoded, m) = decode_single_batch_slice(
                all_frames.into_iter().next().unwrap(),
                fetch_offset,
            )?;
            observe(m);
            return Ok((decoded, next_fetch_offset, last_batch_end_offset));
        }
        return Ok((Vec::new(), next_fetch_offset, last_batch_end_offset));
    }

    let batch = RecordBatchDecoder::decode_with_custom_compression(
        &mut raw_records,
        Some(decompress_record_batch),
    )?;
    let mut last_batch_end_offset: Option<i64> = None;
    let mut decoded_records = Vec::with_capacity(batch.len());
    for rec in batch {
        if rec.offset >= fetch_offset {
            last_batch_end_offset = Some(
                last_batch_end_offset.map_or(rec.offset, |prev| prev.max(rec.offset)),
            );
        }
        if rec.control || rec.offset < fetch_offset {
            continue;
        }
        let headers = rec
            .headers
            .into_iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.unwrap_or_default()))
            .collect();
        decoded_records.push(FetchedRecord {
            offset: rec.offset,
            timestamp_ms: rec.timestamp,
            key: rec.key.unwrap_or_default(),
            value: rec.value.unwrap_or_default(),
            headers,
        });
    }
    let next_fetch_offset =
        last_batch_end_offset.map_or(fetch_offset, |end| fetch_offset.max(end + 1));
    Ok((decoded_records, next_fetch_offset, last_batch_end_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::Compression;

    fn encode_test_batch(offsets: &[i64], control: bool, compression: Compression) -> Bytes {
        let records: Vec<Record> = offsets
            .iter()
            .map(|&offset| {
                let mut headers = IndexMap::new();
                if !control {
                    headers.insert(StrBytes::from_static_str("v"), Some(Bytes::from_static(b"2")));
                }
                Record {
                    transactional: control,
                    control,
                    partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    producer_id: if control { 42 } else { NO_PRODUCER_ID },
                    producer_epoch: if control { 1 } else { NO_PRODUCER_EPOCH },
                    timestamp_type: TimestampType::Creation,
                    offset,
                    sequence: if control { NO_SEQUENCE } else { offset as i32 },
                    timestamp: 1_700_000_000_000 + offset,
                    key: Some(if control {
                        Bytes::from_static(&[0, 0, 0, 0])
                    } else {
                        Bytes::from(format!("tenant-{}", offset % 2))
                    }),
                    value: Some(if control {
                        Bytes::from_static(&[0, 0, 0, 0, 0, 0])
                    } else {
                        Bytes::from(format!("payload-{offset}-repeated-repeated-data"))
                    }),
                    headers,
                }
            })
            .collect();
        let mut buf = BytesMut::new();
        let opts = RecordEncodeOptions {
            version: 2,
            compression,
        };
        RecordBatchEncoder::encode_with_custom_compression(
            &mut buf,
            records.iter(),
            &opts,
            Some(compress_record_batch),
        )
        .unwrap();
        buf.freeze()
    }

    /// Sets the LogAppendTime attribute bit and the batch max timestamp, re-computing the CRC.
    fn to_log_append_time(batch: &Bytes, max_timestamp: i64) -> Bytes {
        let mut b = BytesMut::from(&batch[..]);
        let attrs = i16::from_be_bytes([b[21], b[22]]) | V2_TIMESTAMP_TYPE_LOG_APPEND_TIME;
        b[21..23].copy_from_slice(&attrs.to_be_bytes());
        b[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        let crc = crc32c::crc32c(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        b.freeze()
    }

    // franz-go source.go processRecordBatch: LogAppendTime batches use the batch MaxTimestamp.
    #[test]
    fn log_append_time_batches_use_the_batch_max_timestamp() {
        let pool = ScopedWorkerPool::new("test-decode", 1);
        for compression in [Compression::None, Compression::Zstd] {
            let create = encode_test_batch(&[0, 1, 2], false, compression);
            let (recs, _, _) =
                decode_fetch_records_with_pool(create.clone(), 0, || &pool, None).unwrap();
            let ts: Vec<i64> = recs.iter().map(|r| r.timestamp_ms).collect();
            assert_eq!(ts, vec![1_700_000_000_000, 1_700_000_000_001, 1_700_000_000_002]);

            let appended = to_log_append_time(&create, 1_800_000_000_000);
            let (recs, _, _) = decode_fetch_records_with_pool(appended, 1, || &pool, None).unwrap();
            assert_eq!(recs.len(), 2);
            assert!(recs.iter().all(|r| r.timestamp_ms == 1_800_000_000_000));
        }
    }

    // kprom OnFetchBatchRead: one call per batch, CompressedBytes = len(batch.Records),
    // UncompressedBytes = decompressed records length, NumRecords from the batch header.
    #[test]
    fn fetch_batch_metrics_match_kgo() {
        let pool = ScopedWorkerPool::new("test-decode", 1);
        let reg = prometheus::Registry::new();
        let k = KpromMetrics::new("cortex_ingest_storage_reader", "partition-reader", Some(&reg));
        let plain = encode_test_batch(&[0, 1, 2], false, Compression::None);
        let zstd = encode_test_batch(&[3, 4, 5], false, Compression::Zstd);
        let ctrl = encode_test_batch(&[6], true, Compression::None);
        let mut all = BytesMut::new();
        all.extend_from_slice(&plain);
        all.extend_from_slice(&zstd);
        all.extend_from_slice(&ctrl);
        let (recs, next, _) =
            decode_fetch_records_with_pool(all.freeze(), 1, || &pool, Some(&k)).unwrap();
        assert_eq!(recs.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
        assert_eq!(next, 7);
        let get = |name: &str| -> f64 {
            reg.gather()
                .iter()
                .find(|mf| mf.get_name() == name)
                .map_or(0.0, |mf| mf.get_metric()[0].get_counter().get_value())
        };
        assert_eq!(get("cortex_ingest_storage_reader_fetch_batches_total"), 3.0);
        assert_eq!(get("cortex_ingest_storage_reader_fetch_records_total"), 7.0);
        let compressed = (plain.len() - V2_HEADER_LEN) + (zstd.len() - V2_HEADER_LEN) + (ctrl.len() - V2_HEADER_LEN);
        assert_eq!(get("cortex_ingest_storage_reader_fetch_compressed_bytes_total"), compressed as f64);
        // The zstd batch decompresses to the same records section as the plain one would.
        let plain_zstd_equiv = encode_test_batch(&[3, 4, 5], false, Compression::None);
        let uncompressed = (plain.len() - V2_HEADER_LEN)
            + (plain_zstd_equiv.len() - V2_HEADER_LEN)
            + (ctrl.len() - V2_HEADER_LEN);
        assert_eq!(get("cortex_ingest_storage_reader_fetch_bytes_total"), uncompressed as f64);
    }

    #[test]
    fn parallel_multi_batch_zstd_decode_matches_sequential() {
        let mut combined = BytesMut::new();
        for batch_idx in 0..4i64 {
            let offsets: Vec<i64> = (0..5).map(|i| batch_idx * 5 + i).collect();
            combined.extend_from_slice(&encode_test_batch(&offsets, false, Compression::Zstd));
        }

        let raw = combined.freeze();
        let pool = ScopedWorkerPool::new("test-kafka-dec", 3);
        let (decoded, next_off, last_end) =
            decode_fetch_records_with_pool(raw, 3, || &pool, None).unwrap();
        assert_eq!(decoded.len(), 17);
        assert_eq!(decoded[0].offset, 3);
        assert_eq!(decoded[0].value, Bytes::from_static(b"payload-3-repeated-repeated-data"));
        assert_eq!(decoded.last().unwrap().offset, 19);
        assert_eq!(last_end, Some(19));
        assert_eq!(next_off, 20);
    }

    #[test]
    fn bit_flip_into_control_attribute_fails_crc32c() {
        let pool = ScopedWorkerPool::new("test-crc-control", 1);
        let batch = encode_test_batch(&[0, 1, 2], false, Compression::None);
        let mut corrupted = BytesMut::from(&batch[..]);
        // Flip bit 0x20 (control batch flag) in the low byte of attributes (offset 22)
        // without updating the batch CRC32C.
        corrupted[V2_ATTRIBUTES_OFFSET + 1] ^= 0x20;
        let err = decode_fetch_records_with_pool(corrupted.freeze(), 0, || &pool, None)
            .expect_err("corrupted attributes must fail CRC32C");
        assert!(
            err.to_string().contains("Cyclic redundancy check failed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn truncated_trailing_batch_returns_preceding_complete_batches_and_errors_if_zero_fit() {
        let b1 = encode_test_batch(&[0, 1, 2], false, Compression::None);
        let b2 = encode_test_batch(&[3, 4, 5], false, Compression::None);
        let b3 = encode_test_batch(&[6, 7, 8], false, Compression::None);

        let mut combined = BytesMut::new();
        combined.extend_from_slice(&b1);
        combined.extend_from_slice(&b2);
        // Append truncated tail of b3 (only half of it)
        combined.extend_from_slice(&b3[..b3.len() / 2]);

        let pool = ScopedWorkerPool::new("test-kafka-trunc", 2);
        let (decoded, next_off, last_end) =
            decode_fetch_records_with_pool(combined.freeze(), 0, || &pool, None).unwrap();
        assert_eq!(decoded.len(), 6);
        assert_eq!(decoded.last().unwrap().offset, 5);
        assert_eq!(last_end, Some(5));
        assert_eq!(next_off, 6);

        // If the very first batch is truncated, an error must be returned.
        let truncated_first = b1.slice(..b1.len() - 5);
        assert!(decode_fetch_records_with_pool(truncated_first, 0, || &pool, None).is_err());
        let truncated_header = b1.slice(..10);
        assert!(decode_fetch_records_with_pool(truncated_header, 0, || &pool, None).is_err());
    }

    #[test]
    fn control_batches_advance_next_fetch_offset_and_last_batch_end_offset() {
        let data = encode_test_batch(&[10, 11], false, Compression::None);
        let ctrl = encode_test_batch(&[12], true, Compression::None);

        let mut combined = BytesMut::new();
        combined.extend_from_slice(&data);
        combined.extend_from_slice(&ctrl);

        let pool = ScopedWorkerPool::new("test-kafka-ctrl", 2);
        let (decoded, next_off, last_end) =
            decode_fetch_records_with_pool(combined.freeze(), 10, || &pool, None).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(last_end, Some(12));
        assert_eq!(next_off, 13);

        // Pure control batch fetch still advances next_fetch_offset without spinning
        let (decoded_ctrl_only, next_off_ctrl, last_end_ctrl) =
            decode_fetch_records_with_pool(ctrl, 12, || &pool, None).unwrap();
        assert!(decoded_ctrl_only.is_empty());
        assert_eq!(last_end_ctrl, Some(12));
        assert_eq!(next_off_ctrl, 13);
    }

    #[test]
    fn multi_record_produce_encodes_single_record_batch() {
        let records: Vec<Record> = (0..5i64)
            .map(|i| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: i,
                sequence: (i as i32).wrapping_add(NO_SEQUENCE),
                timestamp: 1_700_000_000_000,
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from(format!("val-{i}"))),
                headers: IndexMap::new(),
            })
            .collect();
        let mut buf = BytesMut::new();
        let opts = RecordEncodeOptions {
            version: 2,
            compression: Compression::Zstd,
        };
        RecordBatchEncoder::encode_with_custom_compression(
            &mut buf,
            records.iter(),
            &opts,
            Some(compress_record_batch),
        )
        .unwrap();
        let raw = buf.freeze();
        // Verify batch length header spans the entire encoded buffer (single RecordBatch).
        let batch_len = i32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;
        assert_eq!(12 + batch_len, raw.len());
        let last_offset_delta = i32::from_be_bytes([raw[23], raw[24], raw[25], raw[26]]);
        assert_eq!(last_offset_delta, 4);
    }

    #[tokio::test]
    async fn kafka_frame_reader_rejects_oversized_and_truncated_large_headers() {
        // 1 GiB frame header is rejected before reading payload.
        let oversized = (1_000_000_000u32).to_be_bytes();
        let mut cursor = std::io::Cursor::new(&oversized[..]);
        assert!(read_kafka_frame_bounded(&mut cursor, MAX_KAFKA_FRAME_BYTES).await.is_none());

        // 200 MiB frame header followed by 8 bytes + EOF returns None without a 200 MiB alloc.
        let mut truncated = Vec::from((200 * 1024 * 1024u32).to_be_bytes());
        truncated.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let mut cursor_trunc = std::io::Cursor::new(truncated);
        assert!(read_kafka_frame_bounded(&mut cursor_trunc, MAX_KAFKA_FRAME_BYTES).await.is_none());

        // Valid multi-chunk frame > INITIAL_CAP (150 KiB) succeeds.
        let body: Vec<u8> = (0..150 * 1024).map(|i| (i % 251) as u8).collect();
        let mut wire = Vec::with_capacity(4 + body.len());
        wire.extend_from_slice(&(body.len() as u32).to_be_bytes());
        wire.extend_from_slice(&body);
        let mut cursor_ok = std::io::Cursor::new(wire);
        let got = read_kafka_frame_bounded(&mut cursor_ok, MAX_KAFKA_FRAME_BYTES)
            .await
            .expect("valid large frame");
        assert_eq!(got, body);
    }

    /// Reference decoder backed by `kafka_protocol::records::RecordBatchDecoder` for differential
    /// parity verification against our zero-`IndexMap` v2 parser.
    fn reference_decode_batch(
        mut slice: Bytes,
        fetch_offset: i64,
    ) -> Result<Vec<FetchedRecord>, KafkaClientError> {
        let attributes = i16::from_be_bytes([
            slice[V2_ATTRIBUTES_OFFSET],
            slice[V2_ATTRIBUTES_OFFSET + 1],
        ]);
        let max_timestamp = be_i64(&slice, V2_MAX_TIMESTAMP_OFFSET);
        let log_append_time = attributes & V2_TIMESTAMP_TYPE_LOG_APPEND_TIME != 0;
        let batch = RecordBatchDecoder::decode_with_custom_compression(
            &mut slice,
            Some(decompress_record_batch),
        )?;
        let mut out = Vec::new();
        for rec in batch {
            if rec.control || rec.offset < fetch_offset {
                continue;
            }
            let headers = rec
                .headers
                .into_iter()
                .map(|(k, v)| (k.as_str().to_owned(), v.unwrap_or_default()))
                .collect();
            out.push(FetchedRecord {
                offset: rec.offset,
                timestamp_ms: if log_append_time {
                    max_timestamp
                } else {
                    rec.timestamp
                },
                key: rec.key.unwrap_or_default(),
                value: rec.value.unwrap_or_default(),
                headers,
            });
        }
        Ok(out)
    }

    #[test]
    fn fast_v2_decoder_matches_record_batch_decoder_differential() {
        let codecs = [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ];
        let base_ts = 1_700_000_000_000i64;
        let big_val = Bytes::from(vec![b'x'; 20_000]);

        for compression in codecs {
            let mut records = Vec::new();
            // Record 0: None key, None value, no headers
            records.push(Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: 100,
                sequence: 0,
                timestamp: base_ts,
                key: None,
                value: None,
                headers: IndexMap::new(),
            });
            // Record 1: empty key, empty value, multiple headers
            let mut h1 = IndexMap::new();
            h1.insert(StrBytes::from_static_str("version"), Some(Bytes::from_static(b"2")));
            h1.insert(StrBytes::from_static_str("empty"), Some(Bytes::new()));
            records.push(Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: 101,
                sequence: 1,
                timestamp: base_ts + 250,
                key: Some(Bytes::new()),
                value: Some(Bytes::new()),
                headers: h1,
            });
            // Record 2: large payload (multi-byte varint), large timestamp delta
            let mut h2 = IndexMap::new();
            h2.insert(StrBytes::from_static_str("v"), Some(Bytes::from_static(b"1")));
            h2.insert(
                StrBytes::from_static_str("trace"),
                Some(Bytes::from_static(b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")),
            );
            records.push(Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: 102,
                sequence: 2,
                timestamp: base_ts + 500_000,
                key: Some(Bytes::from_static(b"tenant-alpha")),
                value: Some(big_val.clone()),
                headers: h2,
            });

            let mut buf = BytesMut::new();
            let opts = RecordEncodeOptions {
                version: 2,
                compression,
            };
            RecordBatchEncoder::encode_with_custom_compression(
                &mut buf,
                records.iter(),
                &opts,
                Some(compress_record_batch),
            )
            .unwrap();
            let raw = buf.freeze();

            for fetch_offset in [99, 100, 101, 102, 103] {
                for batch in [raw.clone(), to_log_append_time(&raw, base_ts + 999_999)] {
                    let expected = reference_decode_batch(batch.clone(), fetch_offset).unwrap();
                    let (actual, _) = decode_single_batch_slice(batch, fetch_offset).unwrap();
                    assert_eq!(actual.len(), expected.len(), "codec {compression:?}");
                    for (a, e) in actual.iter().zip(expected.iter()) {
                        assert_eq!(a.offset, e.offset);
                        assert_eq!(a.timestamp_ms, e.timestamp_ms);
                        assert_eq!(a.key, e.key);
                        assert_eq!(a.value, e.value);
                        assert_eq!(a.headers, e.headers);
                    }
                }
            }
        }
    }

    #[test]
    fn fast_v2_decoder_rejects_corrupt_crc_and_headers_without_panic() {
        let valid = encode_test_batch(&[0, 1, 2], false, Compression::None);

        // Corrupt CRC byte.
        let mut bad_crc = BytesMut::from(&valid[..]);
        bad_crc[17] ^= 0xff;
        assert!(decode_single_batch_slice(bad_crc.freeze(), 0).is_err());

        // Corrupt num_records = i32::MAX (with recomputed CRC) must fail cleanly without OOM/panic.
        let mut huge_count = BytesMut::from(&valid[..]);
        huge_count[V2_NUM_RECORDS_OFFSET..V2_NUM_RECORDS_OFFSET + 4]
            .copy_from_slice(&i32::MAX.to_be_bytes());
        let crc = crc32c::crc32c(&huge_count[V2_ATTRIBUTES_OFFSET..]);
        huge_count[17..21].copy_from_slice(&crc.to_be_bytes());
        assert!(decode_single_batch_slice(huge_count.freeze(), 0).is_err());

        // Corrupt negative num_records (-5) must fail cleanly.
        let mut neg_count = BytesMut::from(&valid[..]);
        neg_count[V2_NUM_RECORDS_OFFSET..V2_NUM_RECORDS_OFFSET + 4]
            .copy_from_slice(&(-5i32).to_be_bytes());
        let crc = crc32c::crc32c(&neg_count[V2_ATTRIBUTES_OFFSET..]);
        neg_count[17..21].copy_from_slice(&crc.to_be_bytes());
        assert!(decode_single_batch_slice(neg_count.freeze(), 0).is_err());
    }

    #[tokio::test]
    async fn fetch_metadata_prunes_stale_broker_conns_but_keeps_seed_brokers() {
        use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
        use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
        use kafka_protocol::messages::ApiVersionsRequest;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let advertised_port = Arc::new(AtomicI32::new(addr.port() as i32));
        let adv_c = Arc::clone(&advertised_port);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let adv = Arc::clone(&adv_c);
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
                            _ => break,
                        };
                        let Ok(req_hdr) = RequestHeader::decode(&mut buf, hdr_ver) else {
                            break;
                        };
                        let mut out = BytesMut::new();
                        out.put_u32(0);
                        let resp_hdr_ver = api_key.response_header_version(ver);
                        let mut resp_hdr = ResponseHeader::default();
                        resp_hdr.correlation_id = req_hdr.correlation_id;
                        resp_hdr.encode(&mut out, resp_hdr_ver).unwrap();

                        match api_key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, ver);
                                let mut resp = ApiVersionsResponse::default();
                                for (k, mn, mx) in [(ApiKey::ApiVersions as i16, 0, 3), (ApiKey::Metadata as i16, 1, 12)] {
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
                                b.port = adv.load(Ordering::SeqCst);
                                resp.brokers.push(b);
                                resp.controller_id = BrokerId(0);
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

        let client = KafkaClient::new(
            vec![addr.to_string()],
            "test-client",
            "",
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .unwrap();

        // Insert a stale non-seed broker connection and a seed broker connection.
        let make_dummy_conn = |b_addr: &str| {
            let (tx, _rx) = mpsc::channel(1);
            Arc::new(BrokerConn {
                addr: b_addr.to_string(),
                kind: ConnKind::Normal,
                client_id: "test".to_string(),
                next_corr_id: AtomicI32::new(1),
                write_tx: tx,
                pending: Arc::new(parking_lot::Mutex::new(HashMap::new())),
                api_versions: RwLock::new(HashMap::new()),
                alive: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                disconnected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                reader_abort: tokio::spawn(std::future::pending::<()>()).abort_handle(),
                sasl_expiry: None,
                successes: std::sync::atomic::AtomicU64::new(0),
                node: "99".to_string(),
                kprom: None,
            })
        };

        let stale_conn = make_dummy_conn("10.0.0.99:9092");
        {
            let mut map = client.conns_by_addr.lock().await;
            map.insert(("10.0.0.99:9092".to_string(), ConnKind::Normal), Arc::clone(&stale_conn));
        }

        // First metadata response advertises port 19092 (different from seed), so seed is kept
        // because it's in seed_brokers, and 10.0.0.99:9092 is pruned and marked dead.
        advertised_port.store(19092, Ordering::SeqCst);
        client.fetch_metadata(Some(&[]), 1).await.unwrap();

        assert!(!stale_conn.is_alive(), "pruned stale conn must be marked dead");
        {
            let map = client.conns_by_addr.lock().await;
            assert!(!map.contains_key(&("10.0.0.99:9092".to_string(), ConnKind::Normal)));
            assert!(map.contains_key(&(addr.to_string(), ConnKind::Normal)));
        }

        srv.abort();
    }
}
