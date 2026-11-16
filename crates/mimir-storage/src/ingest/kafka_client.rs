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
    Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
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
use super::transport::{KafkaTransport, SaslAuthConfig};

pub const KAFKA_OFFSET_START: i64 = -2;
pub const KAFKA_OFFSET_END: i64 = -1;

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
    /// `topic -> sorted partition ids`
    topic_partitions: HashMap<String, Vec<i32>>,
}

struct BrokerConn {
    addr: String,
    client_id: String,
    next_corr_id: AtomicI32,
    write_tx: mpsc::Sender<Bytes>,
    pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>>,
    api_versions: RwLock<HashMap<i16, (i16, i16)>>,
    alive: Arc<std::sync::atomic::AtomicBool>,
    /// SASL session expiry: past it the connection is replaced instead of reused.
    sasl_expiry: Option<std::time::Instant>,
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
    async fn connect(
        addr: String,
        client_id: String,
        transport: &KafkaTransport,
        request_timeout: Duration,
    ) -> Result<Arc<Self>, KafkaClientError> {
        let established = transport.connect(&addr, &client_id, request_timeout).await?;

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
            let mut len_buf = [0u8; 4];
            loop {
                if reader.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let frame_len = u32::from_be_bytes(len_buf) as usize;
                if !(4..=1_000_000_000).contains(&frame_len) {
                    break;
                }
                let mut payload = vec![0u8; frame_len];
                if reader.read_exact(&mut payload).await.is_err() {
                    break;
                }
                let corr_id = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let tx = pending_r.lock().remove(&corr_id);
                if let Some(tx) = tx {
                    let _ = tx.send(Bytes::from(payload));
                }
            }
            alive_r.store(false, Ordering::SeqCst);
            let mut map = pending_r.lock();
            map.clear();
        });

        Ok(Arc::new(Self {
            addr,
            client_id,
            next_corr_id: AtomicI32::new(established.next_correlation_id),
            write_tx,
            pending,
            api_versions: RwLock::new(established.api_versions),
            alive,
            sasl_expiry: established.sasl_expiry,
        }))
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

        if self.write_tx.send(body.freeze()).await.is_err() {
            return Err(KafkaClientError::ConnectionClosed);
        }

        let mut resp_buf = rx.await.map_err(|_| KafkaClientError::ConnectionClosed)?;
        guard.completed = true;
        let resp_header_ver = if api_key == ApiKey::ApiVersions {
            0
        } else {
            Resp::header_version(api_version)
        };
        let _resp_header = ResponseHeader::decode(&mut resp_buf, resp_header_ver)?;
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
    conns_by_addr: Mutex<HashMap<String, Arc<BrokerConn>>>,
    metadata: RwLock<ClusterMetadata>,
    decode_pool: OnceLock<ScopedWorkerPool>,
    /// `-ingest-storage.kafka.last-produced-offset-retry-timeout`: the `ListOffsets` retry timeout
    /// (Mimir's `kgo.RetryTimeoutFn`).
    last_produced_offset_retry_timeout: Duration,
    /// Cached controller broker ID (`-1` = unknown), as kgo's `controllerID`.
    controller_id: AtomicI32,
    any_broker: parking_lot::Mutex<AnyBrokerRotation>,
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
        })
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

    async fn get_conn_for_addr(&self, addr: &str) -> Result<Arc<BrokerConn>, KafkaClientError> {
        {
            let mut map = self.conns_by_addr.lock().await;
            if let Some(c) = map.get(addr) {
                if c.is_usable() {
                    return Ok(Arc::clone(c));
                }
                map.remove(addr);
            }
        }

        // Connect and negotiate outside conns_by_addr lock so slow/dead brokers never block others.
        let conn = BrokerConn::connect(
            addr.to_string(),
            self.client_id.clone(),
            &self.transport,
            self.request_timeout,
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
        if let Some(existing) = map.get(addr) {
            if existing.is_usable() {
                conn.mark_dead();
                return Ok(Arc::clone(existing));
            }
        }
        map.insert(addr.to_string(), Arc::clone(&conn));
        Ok(conn)
    }

    async fn drop_conn(&self, conn: &Arc<BrokerConn>) {
        conn.mark_dead();
        let mut map = self.conns_by_addr.lock().await;
        if let Some(existing) = map.get(&conn.addr) {
            if Arc::ptr_eq(existing, conn) {
                map.remove(&conn.addr);
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
        load_connection_retrying_once(|| self.get_conn_for_addr(addr)).await
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
        let res = tokio::time::timeout(timeout, conn.send_raw(api_key, ver, req))
            .await
            .map_err(|_| KafkaClientError::RequestTimeout(timeout))
            .and_then(|r| r);
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
                    let ver = conn.pick_version(ApiKey::Metadata, 1, 9);
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
        drop(md);
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
        let mut last_err = None;
        for seed in &self.seed_brokers {
            match self.load_conn(seed).await {
                Ok(c) => return Ok(c),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(KafkaClientError::NoSeedBrokers))
    }

    async fn conn_for_node(&self, node_id: i32) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let addr = {
            let md = self.metadata.read();
            md.brokers.get(&node_id).map(|b| b.addr.clone())
        };
        if let Some(addr) = addr {
            if let Ok(conn) = self.load_conn(&addr).await {
                return Ok(conn);
            }
        }
        self.any_conn().await
    }

    async fn leader_conn(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let leader = {
            let md = self.metadata.read();
            md.partition_leaders.get(&(topic.to_string(), partition)).copied()
        };
        if let Some(node_id) = leader {
            if node_id >= 0 {
                return self.conn_for_node(node_id).await;
            }
        }
        self.refresh_metadata(&[topic]).await?;
        let leader = {
            let md = self.metadata.read();
            md.partition_leaders.get(&(topic.to_string(), partition)).copied()
        };
        if let Some(node_id) = leader {
            if node_id >= 0 {
                return self.conn_for_node(node_id).await;
            }
        }
        self.any_conn().await
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
            let mut pids = Vec::with_capacity(t.partitions.len());
            for p in t.partitions {
                pids.push(p.partition_index);
                md.partition_leaders
                    .insert((topic_name.clone(), p.partition_index), p.leader_id.0);
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
                    let conn = self.load_conn(&b.addr).await?;
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
            let conn = self.leader_conn(topic, partition).await?;
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
            if !self.client_rack.is_empty() {
                req.rack_id = StrBytes::from_string(self.client_rack.clone());
            }

            let res: Result<FetchPartitionResult, KafkaClientError> = async {
                let resp: FetchResponse = tokio::time::timeout(
                    wait_timeout,
                    conn.send_raw(ApiKey::Fetch, ver, &req),
                )
                .await
                .map_err(|_| KafkaClientError::RequestTimeout(wait_timeout))??;

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
                                    decode_fetch_records_with_pool(
                                        raw_records,
                                        fetch_offset,
                                        || self.get_decode_pool(),
                                    )?
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
                    sequence: NO_SEQUENCE,
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
            let conn = self.leader_conn(topic, partition).await?;
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
                let resp: ProduceResponse = tokio::time::timeout(
                    self.request_timeout,
                    conn.send_raw(ApiKey::Produce, ver, &req),
                )
                .await
                .map_err(|_| KafkaClientError::RequestTimeout(self.request_timeout))??;

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

        let resp: FindCoordinatorResponse = tokio::time::timeout(
            self.request_timeout,
            conn.send_raw(ApiKey::FindCoordinator, ver, &req),
        )
        .await
        .map_err(|_| KafkaClientError::RequestTimeout(self.request_timeout))??;

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
                let resp: OffsetCommitResponse = tokio::time::timeout(
                    self.request_timeout,
                    conn.send_raw(ApiKey::OffsetCommit, ver, &req),
                )
                .await
                .map_err(|_| KafkaClientError::RequestTimeout(self.request_timeout))??;

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
                let resp: OffsetFetchResponse = tokio::time::timeout(
                    self.request_timeout,
                    conn.send_raw(ApiKey::OffsetFetch, ver, &req),
                )
                .await
                .map_err(|_| KafkaClientError::RequestTimeout(self.request_timeout))??;

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
    is_control: bool,
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
        let base_offset = i64::from_be_bytes([
            raw[pos],
            raw[pos + 1],
            raw[pos + 2],
            raw[pos + 3],
            raw[pos + 4],
            raw[pos + 5],
            raw[pos + 6],
            raw[pos + 7],
        ]);
        let attributes = i16::from_be_bytes([raw[pos + 21], raw[pos + 22]]);
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
            is_control: (attributes & 0x20) != 0,
        });
        pos += frame_len;
    }
    Ok(Some(frames))
}

fn decode_single_batch_slice(
    mut slice: Bytes,
    fetch_offset: i64,
) -> Result<Vec<FetchedRecord>, KafkaClientError> {
    let batch =
        RecordBatchDecoder::decode_with_custom_compression(&mut slice, Some(decompress_record_batch))?;
    let mut out = Vec::with_capacity(batch.len());
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
            timestamp_ms: rec.timestamp,
            key: rec.key.unwrap_or_default(),
            value: rec.value.unwrap_or_default(),
            headers,
        });
    }
    Ok(out)
}

pub(crate) fn decode_fetch_records_with_pool<'a>(
    mut raw_records: Bytes,
    fetch_offset: i64,
    get_pool: impl FnOnce() -> &'a ScopedWorkerPool,
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

        let data_frames: Vec<Bytes> = frames
            .into_iter()
            .filter(|f| !f.is_control)
            .map(|f| f.slice)
            .collect();

        if data_frames.len() > 1 {
            let pool = get_pool();
            type BatchDecodeSlot =
                parking_lot::Mutex<Option<Result<Vec<FetchedRecord>, KafkaClientError>>>;
            let slots: Vec<BatchDecodeSlot> =
                (0..data_frames.len()).map(|_| parking_lot::Mutex::new(None)).collect();
            let run = || {
                pool.scoped_execute(data_frames.len(), |idx| {
                    let res = decode_single_batch_slice(data_frames[idx].clone(), fetch_offset);
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
                let recs = slot
                    .into_inner()
                    .expect("parallel batch slot populated")?;
                if decoded.is_empty() {
                    decoded = recs;
                } else {
                    decoded.extend(recs);
                }
            }
            return Ok((decoded, next_fetch_offset, last_batch_end_offset));
        }
        if data_frames.len() == 1 {
            let decoded = decode_single_batch_slice(
                data_frames.into_iter().next().unwrap(),
                fetch_offset,
            )?;
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
            decode_fetch_records_with_pool(raw, 3, || &pool).unwrap();
        assert_eq!(decoded.len(), 17);
        assert_eq!(decoded[0].offset, 3);
        assert_eq!(decoded[0].value, Bytes::from_static(b"payload-3-repeated-repeated-data"));
        assert_eq!(decoded.last().unwrap().offset, 19);
        assert_eq!(last_end, Some(19));
        assert_eq!(next_off, 20);
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
            decode_fetch_records_with_pool(combined.freeze(), 0, || &pool).unwrap();
        assert_eq!(decoded.len(), 6);
        assert_eq!(decoded.last().unwrap().offset, 5);
        assert_eq!(last_end, Some(5));
        assert_eq!(next_off, 6);

        // If the very first batch is truncated, an error must be returned.
        let truncated_first = b1.slice(..b1.len() - 5);
        assert!(decode_fetch_records_with_pool(truncated_first, 0, || &pool).is_err());
        let truncated_header = b1.slice(..10);
        assert!(decode_fetch_records_with_pool(truncated_header, 0, || &pool).is_err());
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
            decode_fetch_records_with_pool(combined.freeze(), 10, || &pool).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(last_end, Some(12));
        assert_eq!(next_off, 13);

        // Pure control batch fetch still advances next_fetch_offset without spinning
        let (decoded_ctrl_only, next_off_ctrl, last_end_ctrl) =
            decode_fetch_records_with_pool(ctrl, 12, || &pool).unwrap();
        assert!(decoded_ctrl_only.is_empty());
        assert_eq!(last_end_ctrl, Some(12));
        assert_eq!(next_off_ctrl, 13);
    }
}
