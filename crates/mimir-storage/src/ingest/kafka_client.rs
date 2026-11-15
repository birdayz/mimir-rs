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
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, CreateTopicsRequest,
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
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};

use super::compression::{compress_record_batch, decompress_record_batch, KafkaCompression};

pub const KAFKA_OFFSET_START: i64 = -2;
pub const KAFKA_OFFSET_END: i64 = -1;

#[derive(Debug, Error)]
pub enum KafkaClientError {
    #[error("no seed brokers configured")]
    NoSeedBrokers,
    #[error("connection timeout connecting to {addr}")]
    ConnectTimeout { addr: String },
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
    client_id: String,
    next_corr_id: AtomicI32,
    write_tx: mpsc::Sender<Bytes>,
    pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>>,
    api_versions: RwLock<HashMap<i16, (i16, i16)>>,
    alive: Arc<std::sync::atomic::AtomicBool>,
}

impl BrokerConn {
    async fn connect(
        addr: String,
        client_id: String,
        dial_timeout: Duration,
    ) -> Result<Arc<Self>, KafkaClientError> {
        let stream = tokio::time::timeout(dial_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| KafkaClientError::ConnectTimeout { addr })??;
        let _ = stream.set_nodelay(true);

        let (mut reader, mut writer) = stream.into_split();
        let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(256);
        let pending: Arc<parking_lot::Mutex<HashMap<i32, oneshot::Sender<Bytes>>>> =
            Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let alive_w = Arc::clone(&alive);
        tokio::spawn(async move {
            while let Some(frame) = write_rx.recv().await {
                if writer.write_all(&frame).await.is_err() {
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

        let conn = Arc::new(Self {
            client_id,
            next_corr_id: AtomicI32::new(1),
            write_tx,
            pending,
            api_versions: RwLock::new(HashMap::new()),
            alive,
        });

        conn.negotiate_api_versions().await?;
        Ok(conn)
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    async fn negotiate_api_versions(&self) -> Result<(), KafkaClientError> {
        let mut req = ApiVersionsRequest::default();
        req.client_software_name = StrBytes::from_static_str("mimir-rs");
        req.client_software_version = StrBytes::from_static_str("0.1.0");

        // Try v3 first; fall back to v0 if broker rejects v3.
        let resp: ApiVersionsResponse = match self.send_raw(ApiKey::ApiVersions, 3, &req).await {
            Ok(r) => r,
            Err(_) => {
                let req0 = ApiVersionsRequest::default();
                self.send_raw(ApiKey::ApiVersions, 0, &req0).await?
            }
        };
        if let Some(err) = resp.error_code.err() {
            if err != ResponseError::UnsupportedVersion {
                return Err(KafkaClientError::Broker(err));
            }
            let req0 = ApiVersionsRequest::default();
            let resp0: ApiVersionsResponse = self.send_raw(ApiKey::ApiVersions, 0, &req0).await?;
            if let Some(e) = resp0.error_code.err() {
                return Err(KafkaClientError::Broker(e));
            }
            let mut map = self.api_versions.write();
            for k in resp0.api_keys {
                map.insert(k.api_key, (k.min_version, k.max_version));
            }
            return Ok(());
        }

        let mut map = self.api_versions.write();
        for k in resp.api_keys {
            map.insert(k.api_key, (k.min_version, k.max_version));
        }
        Ok(())
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

        if self.write_tx.send(body.freeze()).await.is_err() {
            self.pending.lock().remove(&corr_id);
            return Err(KafkaClientError::ConnectionClosed);
        }

        let mut resp_buf = rx.await.map_err(|_| KafkaClientError::ConnectionClosed)?;
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

/// Pure-Rust async TCP Kafka client supporting broker pooling, metadata routing,
/// `ListOffsets`, `Fetch`, `Produce`, `CreateTopics`, `OffsetFetch`, and `OffsetCommit`.
pub struct KafkaClient {
    seed_brokers: Vec<String>,
    client_id: String,
    client_rack: String,
    dial_timeout: Duration,
    request_timeout: Duration,
    conns_by_addr: Mutex<HashMap<String, Arc<BrokerConn>>>,
    metadata: RwLock<ClusterMetadata>,
    decode_pool: OnceLock<ScopedWorkerPool>,
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
            dial_timeout,
            request_timeout,
            conns_by_addr: Mutex::new(HashMap::new()),
            metadata: RwLock::new(ClusterMetadata::default()),
            decode_pool: OnceLock::new(),
        })
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
        Self::new(
            addrs,
            cfg.client_id.clone(),
            cfg.client_rack.clone(),
            Duration::from_nanos(cfg.dial_timeout.as_nanos().max(0) as u64),
            Duration::from_nanos(cfg.write_timeout.as_nanos().max(0) as u64)
                .max(Duration::from_secs(10)),
        )
    }

    async fn get_conn_for_addr(&self, addr: &str) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let mut map = self.conns_by_addr.lock().await;
        if let Some(c) = map.get(addr) {
            if c.is_alive() {
                return Ok(Arc::clone(c));
            }
        }
        let conn = BrokerConn::connect(
            addr.to_string(),
            self.client_id.clone(),
            self.dial_timeout,
        )
        .await?;
        map.insert(addr.to_string(), Arc::clone(&conn));
        Ok(conn)
    }

    async fn any_conn(&self) -> Result<Arc<BrokerConn>, KafkaClientError> {
        let mut last_err = None;
        for seed in &self.seed_brokers {
            match self.get_conn_for_addr(seed).await {
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
            if let Ok(conn) = self.get_conn_for_addr(&addr).await {
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
        let conn = self.any_conn().await?;
        let ver = conn.pick_version(ApiKey::Metadata, 1, 9);

        let mut req = MetadataRequest::default();
        req.allow_auto_topic_creation = false;
        if topics.is_empty() {
            req.topics = None;
        } else {
            req.topics = Some(
                topics
                    .iter()
                    .map(|t| {
                        let mut mt = MetadataRequestTopic::default();
                        mt.name = Some(TopicName(StrBytes::from_string((*t).to_owned())));
                        mt
                    })
                    .collect(),
            );
        }

        let resp: MetadataResponse = tokio::time::timeout(
            self.request_timeout,
            conn.send_raw(ApiKey::Metadata, ver, &req),
        )
        .await
        .map_err(|_| KafkaClientError::RequestTimeout(self.request_timeout))??;

        let mut md = self.metadata.write();
        for b in resp.brokers {
            let addr = format!("{}:{}", b.host.as_str(), b.port);
            md.brokers.insert(
                b.node_id.0,
                BrokerInfo { addr },
            );
        }
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
    pub async fn create_topics(
        &self,
        topics: &[&str],
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<(), KafkaClientError> {
        let conn = self.any_conn().await?;
        let ver = conn.pick_version(ApiKey::CreateTopics, 0, 5);
        let mut req = CreateTopicsRequest::default();
        req.timeout_ms = self.request_timeout.as_millis().min(i32::MAX as u128) as i32;
        for t in topics {
            let mut ct = CreatableTopic::default();
            ct.name = TopicName(StrBytes::from_string((*t).to_owned()));
            ct.num_partitions = num_partitions;
            ct.replication_factor = replication_factor;
            req.topics.push(ct);
        }

        let resp: CreateTopicsResponse = tokio::time::timeout(
            self.request_timeout,
            conn.send_raw(ApiKey::CreateTopics, ver, &req),
        )
        .await
        .map_err(|_| KafkaClientError::RequestTimeout(self.request_timeout))??;

        for t in resp.topics {
            if let Some(err) = t.error_code.err() {
                if err != ResponseError::TopicAlreadyExists {
                    return Err(KafkaClientError::Broker(err));
                }
            }
        }
        Ok(())
    }

    /// Issues `ListOffsets` for a single `(topic, partition)` and `timestamp`.
    /// - `timestamp = -2`: earliest / partition start offset
    /// - `timestamp = -1`: latest (next write offset; last produced offset is `offset - 1`)
    /// - `timestamp >= 0`: first offset with record timestamp >= `timestamp`
    pub async fn list_offset_with_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<ListedOffset, KafkaClientError> {
        let conn = self.leader_conn(topic, partition).await?;
        let ver = conn.pick_version(ApiKey::ListOffsets, 2, 7);

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

        let resp: ListOffsetsResponse = tokio::time::timeout(
            self.request_timeout,
            conn.send_raw(ApiKey::ListOffsets, ver, &req),
        )
        .await
        .map_err(|_| KafkaClientError::RequestTimeout(self.request_timeout))??;

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

        let wait_timeout = self.request_timeout.max(max_wait + Duration::from_secs(5));
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

                let decoded_records = match rp.records {
                    Some(raw_records) if !raw_records.is_empty() => {
                        decode_fetch_records_parallel(
                            raw_records,
                            fetch_offset,
                            || self.get_decode_pool(),
                        )?
                    }
                    _ => Vec::new(),
                };

                return Ok(FetchPartitionResult {
                    high_watermark: rp.high_watermark,
                    log_start_offset: rp.log_start_offset,
                    records: decoded_records,
                });
            }
        }

        Err(KafkaClientError::UnexpectedResponse(format!(
            "missing partition {partition} in FetchResponse for {topic}"
        )))
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

        let conn = self.leader_conn(topic, partition).await?;
        let ver = conn.pick_version(ApiKey::Produce, 3, 9);

        let mut pp = PartitionProduceData::default();
        pp.index = partition;
        pp.records = Some(batch_buf.freeze());

        let mut tp = TopicProduceData::default();
        tp.name = TopicName(StrBytes::from_string(topic.to_owned()));
        tp.partition_data = vec![pp];

        let mut req = ProduceRequest::default();
        req.transactional_id = None;
        req.acks = -1;
        req.timeout_ms = self.request_timeout.as_millis().min(i32::MAX as u128) as i32;
        req.topic_data = vec![tp];

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
        self.get_conn_for_addr(&addr).await
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
        let conn = self.find_group_coordinator(group_id).await?;
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
        let conn = self.find_group_coordinator(group_id).await?;
        let ver = conn.pick_version(ApiKey::OffsetFetch, 1, 7);

        let mut t = OffsetFetchRequestTopic::default();
        t.name = TopicName(StrBytes::from_string(topic.to_owned()));
        t.partition_indexes = vec![partition];

        let mut req = OffsetFetchRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group_id.to_owned()));
        req.topics = Some(vec![t]);

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
}

fn split_record_batch_v2_frames(raw: &Bytes) -> Option<Vec<Bytes>> {
    let mut frames = Vec::new();
    let mut pos = 0usize;
    let total = raw.len();
    while pos < total {
        if pos + 17 > total {
            return None;
        }
        if raw[pos + 16] != 2 {
            return None;
        }
        let batch_len =
            i32::from_be_bytes([raw[pos + 8], raw[pos + 9], raw[pos + 10], raw[pos + 11]]);
        if batch_len < 49 {
            return None;
        }
        let frame_len = 12usize.checked_add(batch_len as usize)?;
        if pos + frame_len > total {
            return None;
        }
        frames.push(raw.slice(pos..pos + frame_len));
        pos += frame_len;
    }
    Some(frames)
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

fn decode_fetch_records_parallel<'a>(
    mut raw_records: Bytes,
    fetch_offset: i64,
    get_pool: impl FnOnce() -> &'a ScopedWorkerPool,
) -> Result<Vec<FetchedRecord>, KafkaClientError> {
    if let Some(frames) = split_record_batch_v2_frames(&raw_records) {
        if frames.len() > 1 {
            let pool = get_pool();
            type BatchDecodeSlot =
                parking_lot::Mutex<Option<Result<Vec<FetchedRecord>, KafkaClientError>>>;
            let slots: Vec<BatchDecodeSlot> =
                (0..frames.len()).map(|_| parking_lot::Mutex::new(None)).collect();
            let run = || {
                pool.scoped_execute(frames.len(), |idx| {
                    let res = decode_single_batch_slice(frames[idx].clone(), fetch_offset);
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
            return Ok(decoded);
        }
        if frames.len() == 1 {
            return decode_single_batch_slice(
                frames.into_iter().next().unwrap(),
                fetch_offset,
            );
        }
        return Ok(Vec::new());
    }

    let batch = RecordBatchDecoder::decode_with_custom_compression(
        &mut raw_records,
        Some(decompress_record_batch),
    )?;
    let mut decoded_records = Vec::with_capacity(batch.len());
    for rec in batch {
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
    Ok(decoded_records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::Compression;

    #[test]
    fn parallel_multi_batch_zstd_decode_matches_sequential() {
        let mut combined = BytesMut::new();
        for batch_idx in 0..4i64 {
            let mut records = Vec::new();
            for i in 0..5i64 {
                let offset = batch_idx * 5 + i;
                let mut headers = IndexMap::new();
                headers.insert(StrBytes::from_static_str("v"), Some(Bytes::from_static(b"2")));
                records.push(Record {
                    transactional: false,
                    control: false,
                    partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                    producer_id: NO_PRODUCER_ID,
                    producer_epoch: NO_PRODUCER_EPOCH,
                    timestamp_type: TimestampType::Creation,
                    offset,
                    sequence: NO_SEQUENCE,
                    timestamp: 1_700_000_000_000 + offset,
                    key: Some(Bytes::from(format!("tenant-{}", offset % 2))),
                    value: Some(Bytes::from(format!("payload-{offset}-repeated-repeated-data"))),
                    headers,
                });
            }
            let opts = RecordEncodeOptions {
                version: 2,
                compression: Compression::Zstd,
            };
            RecordBatchEncoder::encode_with_custom_compression(
                &mut combined,
                records.iter(),
                &opts,
                Some(compress_record_batch),
            )
            .unwrap();
        }

        let raw = combined.freeze();
        let pool = ScopedWorkerPool::new("test-kafka-dec", 3);
        let decoded = decode_fetch_records_parallel(raw, 3, || &pool).unwrap();
        assert_eq!(decoded.len(), 17);
        assert_eq!(decoded[0].offset, 3);
        assert_eq!(decoded[0].value, Bytes::from_static(b"payload-3-repeated-repeated-data"));
        assert_eq!(decoded.last().unwrap().offset, 19);
    }
}
