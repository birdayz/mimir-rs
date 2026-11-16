#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::error::ResponseError;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, CreateTopicsRequest,
    CreateTopicsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use mimir_config::{GoDuration, KafkaConfig, StringSliceCSV};
use mimir_proto::bytes::Bytes as ProtoBytes;
use mimir_proto::cortexpb;
use mimir_storage::ingest::compression::{compress_record_batch, decompress_record_batch};
use mimir_storage::ingest::{base64_encode, record_version_header_raw, ScramAlg};
use parking_lot::Mutex as SyncMutex;
use prometheus::proto::MetricType;
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub enum ClusterSasl {
    None,
    Plain { username: String, password: String },
    ScramSha256 { username: String, password: String },
    ScramSha512 { username: String, password: String },
}

#[derive(Debug, Clone)]
pub struct StoredRecord {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

#[derive(Debug, Default)]
pub struct PartitionState {
    pub log_start_offset: i64,
    pub records: Vec<StoredRecord>,
}

impl PartitionState {
    pub fn high_watermark(&self) -> i64 {
        self.records
            .last()
            .map(|r| r.offset + 1)
            .unwrap_or(self.log_start_offset)
    }
}

pub enum HookAction<Resp> {
    PassThrough,
    Respond(Resp),
    FailConnection,
}

pub type CreateTopicsHook = Arc<
    dyn Fn(CreateTopicsRequest) -> Pin<Box<dyn Future<Output = HookAction<CreateTopicsResponse>> + Send>>
        + Send
        + Sync,
>;
pub type ProduceHook = Arc<
    dyn Fn(ProduceRequest) -> Pin<Box<dyn Future<Output = HookAction<ProduceResponse>> + Send>>
        + Send
        + Sync,
>;
pub type FetchHook = Arc<
    dyn Fn(FetchRequest) -> Pin<Box<dyn Future<Output = HookAction<FetchResponse>> + Send>>
        + Send
        + Sync,
>;
pub type ListOffsetsHook = Arc<
    dyn Fn(ListOffsetsRequest) -> Pin<Box<dyn Future<Output = HookAction<ListOffsetsResponse>> + Send>>
        + Send
        + Sync,
>;
pub type OffsetCommitHook = Arc<
    dyn Fn(OffsetCommitRequest) -> Pin<Box<dyn Future<Output = HookAction<OffsetCommitResponse>> + Send>>
        + Send
        + Sync,
>;
pub type OffsetFetchHook = Arc<
    dyn Fn(OffsetFetchRequest) -> Pin<Box<dyn Future<Output = HookAction<OffsetFetchResponse>> + Send>>
        + Send
        + Sync,
>;

#[derive(Default)]
pub struct Hooks {
    pub create_topics: SyncMutex<Option<CreateTopicsHook>>,
    pub produce: SyncMutex<Option<ProduceHook>>,
    pub fetch: SyncMutex<Option<FetchHook>>,
    pub list_offsets: SyncMutex<Option<ListOffsetsHook>>,
    pub offset_commit: SyncMutex<Option<OffsetCommitHook>>,
    pub offset_fetch: SyncMutex<Option<OffsetFetchHook>>,
}

pub struct FakeKafkaCluster {
    pub addr: String,
    pub port: i32,
    /// Port advertised in Metadata / FindCoordinator (e.g. a TLS proxy in front of the cluster).
    advertised_port: AtomicI32,
    sasl: ClusterSasl,
    pub custom_groups_enabled: bool,
    topics: SyncMutex<HashMap<String, Vec<PartitionState>>>,
    committed_offsets: SyncMutex<HashMap<(String, String, i32), i64>>,
    notify: Notify,
    pub hooks: Hooks,
    shutdown: Arc<tokio::sync::watch::Sender<bool>>,
}

impl FakeKafkaCluster {
    pub async fn start(num_partitions: i32, initial_topic: &str) -> Arc<Self> {
        Self::start_with_opts(num_partitions, initial_topic, ClusterSasl::None, true).await
    }

    pub async fn start_with_opts(
        num_partitions: i32,
        initial_topic: &str,
        sasl: ClusterSasl,
        custom_groups_enabled: bool,
    ) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        let addr = local.to_string();
        let port = local.port() as i32;

        let mut topics = HashMap::new();
        if !initial_topic.is_empty() && num_partitions > 0 {
            let mut parts = Vec::with_capacity(num_partitions as usize);
            for _ in 0..num_partitions {
                parts.push(PartitionState::default());
            }
            topics.insert(initial_topic.to_string(), parts);
        }

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let cluster = Arc::new(Self {
            addr,
            port,
            advertised_port: AtomicI32::new(port),
            sasl,
            custom_groups_enabled,
            topics: SyncMutex::new(topics),
            committed_offsets: SyncMutex::new(HashMap::new()),
            notify: Notify::new(),
            hooks: Hooks::default(),
            shutdown: Arc::new(shutdown_tx),
        });

        let c_accept = Arc::clone(&cluster);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    res = listener.accept() => {
                        let Ok((stream, _)) = res else { break };
                        let c_conn = Arc::clone(&c_accept);
                        tokio::spawn(async move {
                            c_conn.handle_conn(stream).await;
                        });
                    }
                }
            }
        });

        cluster
    }

    pub fn close(&self) {
        let _ = self.shutdown.send(true);
    }

    /// Advertises `port` (on 127.0.0.1) as the broker address in Metadata / FindCoordinator.
    pub fn set_advertised_port(&self, port: i32) {
        self.advertised_port.store(port, AtomicOrdering::SeqCst);
    }

    pub fn topic_exists(&self, topic: &str) -> bool {
        self.topics.lock().contains_key(topic)
    }

    pub fn list_topics(&self) -> Vec<String> {
        self.topics.lock().keys().cloned().collect()
    }

    pub fn advance_partition_start(&self, topic: &str, partition: i32, new_start: i64) {
        let mut guard = self.topics.lock();
        if let Some(parts) = guard.get_mut(topic) {
            if let Some(p) = parts.get_mut(partition as usize) {
                p.log_start_offset = new_start;
                p.records.retain(|r| r.offset >= new_start);
            }
        }
    }

    pub fn get_end_offset(&self, topic: &str, partition: i32) -> i64 {
        let guard = self.topics.lock();
        guard
            .get(topic)
            .and_then(|parts| parts.get(partition as usize))
            .map(|p| p.high_watermark())
            .unwrap_or(0)
    }

    pub fn produce_direct(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<(String, Option<Bytes>)>,
        timestamp_ms: Option<i64>,
    ) -> i64 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut guard = self.topics.lock();
        let parts = guard.get_mut(topic).expect("topic exists");
        let p = &mut parts[partition as usize];
        let offset = p.high_watermark();
        p.records.push(StoredRecord {
            offset,
            timestamp_ms: timestamp_ms.unwrap_or(now_ms),
            key,
            value,
            headers,
        });
        drop(guard);
        self.notify.notify_waiters();
        offset
    }

    pub fn fetch_direct_encoded(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Option<FetchResponse> {
        let guard = self.topics.lock();
        let parts = guard.get(topic)?;
        let p = parts.get(partition as usize)?;
        let rec = p.records.iter().find(|r| r.offset == offset)?.clone();
        let hwm = p.high_watermark();
        let log_start = p.log_start_offset;
        drop(guard);

        let batch = encode_stored_records(&[rec]);
        let mut pd = PartitionData::default();
        pd.partition_index = partition;
        pd.error_code = 0;
        pd.high_watermark = hwm;
        pd.last_stable_offset = hwm;
        pd.log_start_offset = log_start;
        pd.records = Some(batch);

        let mut ft = FetchableTopicResponse::default();
        ft.topic = TopicName(StrBytes::from_string(topic.to_string()));
        ft.partitions = vec![pd];

        let mut resp = FetchResponse::default();
        resp.responses = vec![ft];
        Some(resp)
    }

    async fn handle_conn(self: Arc<Self>, stream: TcpStream) {
        let _ = stream.set_nodelay(true);
        let (mut reader, mut writer) = stream.into_split();
        let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<Bytes>(256);
        tokio::spawn(async move {
            while let Some(frame) = resp_rx.recv().await {
                if writer.write_all(&frame).await.is_err() {
                    break;
                }
            }
        });

        let mut authed = matches!(self.sasl, ClusterSasl::None);
        let mut selected_mech: Option<String> = None;
        let mut scram_state: Option<ScramServerState> = None;

        let mut len_buf = [0u8; 4];
        loop {
            if reader.read_exact(&mut len_buf).await.is_err() {
                break;
            }
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            if !(4..=100_000_000).contains(&frame_len) {
                break;
            }
            let mut payload = vec![0u8; frame_len];
            if reader.read_exact(&mut payload).await.is_err() {
                break;
            }
            let mut buf = Bytes::from(payload);
            let api_key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
            let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
            let Ok(api_key) = ApiKey::try_from(api_key_i16) else {
                break;
            };

            let req_hdr_ver = match api_key {
                ApiKey::ApiVersions => ApiVersionsRequest::header_version(api_ver),
                ApiKey::Metadata => MetadataRequest::header_version(api_ver),
                ApiKey::FindCoordinator => FindCoordinatorRequest::header_version(api_ver),
                ApiKey::CreateTopics => CreateTopicsRequest::header_version(api_ver),
                ApiKey::ListOffsets => ListOffsetsRequest::header_version(api_ver),
                ApiKey::Fetch => FetchRequest::header_version(api_ver),
                ApiKey::Produce => ProduceRequest::header_version(api_ver),
                ApiKey::OffsetCommit => OffsetCommitRequest::header_version(api_ver),
                ApiKey::OffsetFetch => OffsetFetchRequest::header_version(api_ver),
                ApiKey::SaslHandshake => SaslHandshakeRequest::header_version(api_ver),
                ApiKey::SaslAuthenticate => SaslAuthenticateRequest::header_version(api_ver),
                _ => break,
            };

            let Ok(req_hdr) = RequestHeader::decode(&mut buf, req_hdr_ver) else {
                break;
            };
            let corr_id = req_hdr.correlation_id;

            // Handle auth & metadata synchronously; spawn hooked/blocking requests concurrently so pipelining works.
            match api_key {
                ApiKey::ApiVersions => {
                    let _ = ApiVersionsRequest::decode(&mut buf, api_ver);
                    let resp = encode_resp(api_key, api_ver, corr_id, &build_api_versions_response());
                    if resp_tx.send(resp).await.is_err() {
                        break;
                    }
                }
                ApiKey::SaslHandshake => {
                    let Ok(req) = SaslHandshakeRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    selected_mech = Some(req.mechanism.as_str().to_string());
                    let resp = self.handle_sasl_handshake(&req);
                    let bytes = encode_resp(api_key, api_ver, corr_id, &resp);
                    if resp_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                ApiKey::SaslAuthenticate => {
                    let Ok(req) = SaslAuthenticateRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let (resp, ok) = self.handle_sasl_authenticate(
                        &req,
                        selected_mech.as_deref().unwrap_or(""),
                        &mut scram_state,
                    );
                    if ok {
                        authed = true;
                    }
                    let bytes = encode_resp(api_key, api_ver, corr_id, &resp);
                    if resp_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                _ if !authed => {
                    break;
                }
                ApiKey::Metadata => {
                    let Ok(req) = MetadataRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let resp = self.handle_metadata(&req);
                    let bytes = encode_resp(api_key, api_ver, corr_id, &resp);
                    if resp_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                ApiKey::FindCoordinator => {
                    let Ok(_req) = FindCoordinatorRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let mut resp = FindCoordinatorResponse::default();
                    resp.error_code = 0;
                    resp.node_id = BrokerId(0);
                    resp.host = StrBytes::from_static_str("127.0.0.1");
                    resp.port = self.advertised_port.load(AtomicOrdering::SeqCst);
                    let bytes = encode_resp(api_key, api_ver, corr_id, &resp);
                    if resp_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                ApiKey::CreateTopics => {
                    let Ok(req) = CreateTopicsRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let this = Arc::clone(&self);
                    let tx = resp_tx.clone();
                    tokio::spawn(async move {
                        let hook = this.hooks.create_topics.lock().clone();
                        let resp = if let Some(h) = hook {
                            match h(req.clone()).await {
                                HookAction::FailConnection => return,
                                HookAction::Respond(r) => r,
                                HookAction::PassThrough => this.handle_create_topics(&req),
                            }
                        } else {
                            this.handle_create_topics(&req)
                        };
                        let _ = tx.send(encode_resp(api_key, api_ver, corr_id, &resp)).await;
                    });
                }
                ApiKey::ListOffsets => {
                    let Ok(req) = ListOffsetsRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let this = Arc::clone(&self);
                    let tx = resp_tx.clone();
                    tokio::spawn(async move {
                        let hook = this.hooks.list_offsets.lock().clone();
                        let resp = if let Some(h) = hook {
                            match h(req.clone()).await {
                                HookAction::FailConnection => return,
                                HookAction::Respond(r) => r,
                                HookAction::PassThrough => this.handle_list_offsets(&req),
                            }
                        } else {
                            this.handle_list_offsets(&req)
                        };
                        let _ = tx.send(encode_resp(api_key, api_ver, corr_id, &resp)).await;
                    });
                }
                ApiKey::Produce => {
                    let Ok(req) = ProduceRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let this = Arc::clone(&self);
                    let tx = resp_tx.clone();
                    tokio::spawn(async move {
                        let hook = this.hooks.produce.lock().clone();
                        let resp = if let Some(h) = hook {
                            match h(req.clone()).await {
                                HookAction::FailConnection => return,
                                HookAction::Respond(r) => r,
                                HookAction::PassThrough => this.handle_produce(&req),
                            }
                        } else {
                            this.handle_produce(&req)
                        };
                        let _ = tx.send(encode_resp(api_key, api_ver, corr_id, &resp)).await;
                    });
                }
                ApiKey::Fetch => {
                    let Ok(req) = FetchRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let this = Arc::clone(&self);
                    let tx = resp_tx.clone();
                    tokio::spawn(async move {
                        let hook = this.hooks.fetch.lock().clone();
                        let resp = if let Some(h) = hook {
                            match h(req.clone()).await {
                                HookAction::FailConnection => return,
                                HookAction::Respond(r) => r,
                                HookAction::PassThrough => this.handle_fetch(&req).await,
                            }
                        } else {
                            this.handle_fetch(&req).await
                        };
                        let _ = tx.send(encode_resp(api_key, api_ver, corr_id, &resp)).await;
                    });
                }
                ApiKey::OffsetCommit => {
                    let Ok(req) = OffsetCommitRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let this = Arc::clone(&self);
                    let tx = resp_tx.clone();
                    tokio::spawn(async move {
                        let hook = this.hooks.offset_commit.lock().clone();
                        let resp = if let Some(h) = hook {
                            match h(req.clone()).await {
                                HookAction::FailConnection => return,
                                HookAction::Respond(r) => r,
                                HookAction::PassThrough => this.handle_offset_commit(&req),
                            }
                        } else {
                            this.handle_offset_commit(&req)
                        };
                        let _ = tx.send(encode_resp(api_key, api_ver, corr_id, &resp)).await;
                    });
                }
                ApiKey::OffsetFetch => {
                    let Ok(req) = OffsetFetchRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let this = Arc::clone(&self);
                    let tx = resp_tx.clone();
                    tokio::spawn(async move {
                        let hook = this.hooks.offset_fetch.lock().clone();
                        let resp = if let Some(h) = hook {
                            match h(req.clone()).await {
                                HookAction::FailConnection => return,
                                HookAction::Respond(r) => r,
                                HookAction::PassThrough => this.handle_offset_fetch(&req),
                            }
                        } else {
                            this.handle_offset_fetch(&req)
                        };
                        let _ = tx.send(encode_resp(api_key, api_ver, corr_id, &resp)).await;
                    });
                }
                _ => break,
            }
        }
    }

    fn handle_sasl_handshake(&self, req: &SaslHandshakeRequest) -> SaslHandshakeResponse {
        let mut resp = SaslHandshakeResponse::default();
        let expected = match &self.sasl {
            ClusterSasl::None => "",
            ClusterSasl::Plain { .. } => "PLAIN",
            ClusterSasl::ScramSha256 { .. } => "SCRAM-SHA-256",
            ClusterSasl::ScramSha512 { .. } => "SCRAM-SHA-512",
        };
        if !expected.is_empty() {
            resp.mechanisms.push(StrBytes::from_static_str(expected));
        }
        if req.mechanism.as_str() == expected {
            resp.error_code = 0;
        } else {
            resp.error_code = ResponseError::UnsupportedSaslMechanism.code();
        }
        resp
    }

    fn handle_sasl_authenticate(
        &self,
        req: &SaslAuthenticateRequest,
        mech: &str,
        scram_state: &mut Option<ScramServerState>,
    ) -> (SaslAuthenticateResponse, bool) {
        let mut resp = SaslAuthenticateResponse::default();
        match (&self.sasl, mech) {
            (
                ClusterSasl::Plain {
                    username: eu,
                    password: ep,
                },
                "PLAIN",
            ) => {
                let parts: Vec<&[u8]> = req.auth_bytes.split(|&b| b == 0).collect();
                if parts.len() == 3
                    && parts[1] == eu.as_bytes()
                    && parts[2] == ep.as_bytes()
                {
                    resp.error_code = 0;
                    return (resp, true);
                }
                resp.error_code = ResponseError::SaslAuthenticationFailed.code();
                (resp, false)
            }
            (
                ClusterSasl::ScramSha256 {
                    username: eu,
                    password: ep,
                },
                "SCRAM-SHA-256",
            ) => handle_scram_step(req, eu, ep, ScramAlg::Sha256, scram_state),
            (
                ClusterSasl::ScramSha512 {
                    username: eu,
                    password: ep,
                },
                "SCRAM-SHA-512",
            ) => handle_scram_step(req, eu, ep, ScramAlg::Sha512, scram_state),
            _ => {
                resp.error_code = ResponseError::SaslAuthenticationFailed.code();
                (resp, false)
            }
        }
    }

    fn handle_metadata(&self, req: &MetadataRequest) -> MetadataResponse {
        let mut resp = MetadataResponse::default();
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(0);
        broker.host = StrBytes::from_static_str("127.0.0.1");
        broker.port = self.advertised_port.load(AtomicOrdering::SeqCst);
        resp.brokers = vec![broker];
        resp.controller_id = BrokerId(0);

        let guard = self.topics.lock();
        let req_topics: Vec<String> = match &req.topics {
            None => guard.keys().cloned().collect(),
            Some(list) => list
                .iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.0.as_str().to_string()))
                .collect(),
        };

        for tname in req_topics {
            let mut mt = MetadataResponseTopic::default();
            mt.name = Some(TopicName(StrBytes::from_string(tname.clone())));
            if let Some(parts) = guard.get(&tname) {
                mt.error_code = 0;
                for pid in 0..parts.len() {
                    let mut mp = MetadataResponsePartition::default();
                    mp.partition_index = pid as i32;
                    mp.leader_id = BrokerId(0);
                    mp.replica_nodes = vec![BrokerId(0)];
                    mp.isr_nodes = vec![BrokerId(0)];
                    mt.partitions.push(mp);
                }
            } else {
                mt.error_code = ResponseError::UnknownTopicOrPartition.code();
            }
            resp.topics.push(mt);
        }
        resp
    }

    fn handle_create_topics(&self, req: &CreateTopicsRequest) -> CreateTopicsResponse {
        let mut resp = CreateTopicsResponse::default();
        let mut guard = self.topics.lock();
        for t in &req.topics {
            let name = t.name.0.as_str().to_string();
            let mut tr = CreatableTopicResult::default();
            tr.name = TopicName(StrBytes::from_string(name.clone()));
            if let std::collections::hash_map::Entry::Vacant(e) = guard.entry(name) {
                let n = if t.num_partitions <= 0 {
                    1
                } else {
                    t.num_partitions
                };
                let mut parts = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    parts.push(PartitionState::default());
                }
                e.insert(parts);
                tr.error_code = 0;
                tr.num_partitions = n;
                tr.replication_factor = 1;
            } else {
                tr.error_code = ResponseError::TopicAlreadyExists.code();
            }
            resp.topics.push(tr);
        }
        resp
    }

    fn handle_list_offsets(&self, req: &ListOffsetsRequest) -> ListOffsetsResponse {
        let mut resp = ListOffsetsResponse::default();
        let guard = self.topics.lock();
        for rt in &req.topics {
            let tname = rt.name.0.as_str();
            let mut lr_t = ListOffsetsTopicResponse::default();
            lr_t.name = TopicName(StrBytes::from_string(tname.to_string()));
            for rp in &rt.partitions {
                let mut lr_p = ListOffsetsPartitionResponse::default();
                lr_p.partition_index = rp.partition_index;
                if let Some(p) = guard
                    .get(tname)
                    .and_then(|parts| parts.get(rp.partition_index as usize))
                {
                    lr_p.error_code = 0;
                    if rp.timestamp == -2 {
                        lr_p.offset = p.log_start_offset;
                        lr_p.timestamp = -1;
                    } else if rp.timestamp == -1 {
                        lr_p.offset = p.high_watermark();
                        lr_p.timestamp = -1;
                    } else {
                        if let Some(found) = p.records.iter().find(|r| r.timestamp_ms >= rp.timestamp)
                        {
                            lr_p.offset = found.offset;
                            lr_p.timestamp = found.timestamp_ms;
                        } else {
                            lr_p.offset = -1;
                            lr_p.timestamp = -1;
                        }
                    }
                } else {
                    lr_p.error_code = ResponseError::UnknownTopicOrPartition.code();
                }
                lr_t.partitions.push(lr_p);
            }
            resp.topics.push(lr_t);
        }
        resp
    }

    fn handle_produce(&self, req: &ProduceRequest) -> ProduceResponse {
        let mut resp = ProduceResponse::default();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut produced_any = false;
        let mut guard = self.topics.lock();

        for rt in &req.topic_data {
            let tname = rt.name.0.as_str();
            let mut tr = TopicProduceResponse::default();
            tr.name = TopicName(StrBytes::from_string(tname.to_string()));

            for rp in &rt.partition_data {
                let mut pr = PartitionProduceResponse::default();
                pr.index = rp.index;

                if let Some(p) = guard
                    .get_mut(tname)
                    .and_then(|parts| parts.get_mut(rp.index as usize))
                {
                    if let Some(ref raw_rec) = rp.records {
                        if raw_rec.len() > 16_000_000 {
                            pr.error_code = ResponseError::MessageTooLarge.code();
                        } else {
                            let mut bytes = raw_rec.clone();
                            match RecordBatchDecoder::decode_with_custom_compression(
                                &mut bytes,
                                Some(decompress_record_batch),
                            ) {
                                Ok(batch) => {
                                    let base = p.high_watermark();
                                    for (idx, rec) in batch.into_iter().enumerate() {
                                        let headers = rec
                                            .headers
                                            .into_iter()
                                            .map(|(k, v)| (k.as_str().to_string(), v))
                                            .collect();
                                        p.records.push(StoredRecord {
                                            offset: base + idx as i64,
                                            timestamp_ms: if rec.timestamp > 0 {
                                                rec.timestamp
                                            } else {
                                                now_ms
                                            },
                                            key: rec.key,
                                            value: rec.value,
                                            headers,
                                        });
                                    }
                                    pr.error_code = 0;
                                    pr.base_offset = base;
                                    produced_any = true;
                                }
                                Err(_) => {
                                    pr.error_code = ResponseError::CorruptMessage.code();
                                }
                            }
                        }
                    }
                } else {
                    pr.error_code = ResponseError::UnknownTopicOrPartition.code();
                }

                tr.partition_responses.push(pr);
            }
            resp.responses.push(tr);
        }

        drop(guard);
        if produced_any {
            self.notify.notify_waiters();
        }
        resp
    }

    async fn handle_fetch(&self, req: &FetchRequest) -> FetchResponse {
        let max_wait = Duration::from_millis(req.max_wait_ms.max(0) as u64);
        let deadline = tokio::time::Instant::now() + max_wait;

        loop {
            let mut any_records = false;
            let mut any_error = false;
            let mut resp = FetchResponse::default();

            {
                let guard = self.topics.lock();
                for ft in &req.topics {
                    let tname = ft.topic.0.as_str();
                    let mut fr_t = FetchableTopicResponse::default();
                    fr_t.topic = TopicName(StrBytes::from_string(tname.to_string()));
                    for fp in &ft.partitions {
                        let mut pd = PartitionData::default();
                        pd.partition_index = fp.partition;
                        if let Some(p) = guard
                            .get(tname)
                            .and_then(|parts| parts.get(fp.partition as usize))
                        {
                            let hwm = p.high_watermark();
                            pd.high_watermark = hwm;
                            pd.last_stable_offset = hwm;
                            pd.log_start_offset = p.log_start_offset;
                            let mut recs = Vec::new();
                            let mut total_bytes = 0usize;
                            let budget = fp.partition_max_bytes.max(1) as usize;
                            for r in &p.records {
                                if r.offset >= fp.fetch_offset {
                                    let rec_size = r.value.as_ref().map_or(0, Bytes::len)
                                        + r.key.as_ref().map_or(0, Bytes::len)
                                        + 32;
                                    if !recs.is_empty() && total_bytes + rec_size > budget {
                                        break;
                                    }
                                    total_bytes += rec_size;
                                    recs.push(r.clone());
                                }
                            }
                            if !recs.is_empty() {
                                any_records = true;
                                pd.records = Some(encode_stored_records(&recs));
                            }
                        } else {
                            pd.error_code = ResponseError::UnknownTopicOrPartition.code();
                            any_error = true;
                        }
                        fr_t.partitions.push(pd);
                    }
                    resp.responses.push(fr_t);
                }
            }

            if any_records || any_error || tokio::time::Instant::now() >= deadline {
                return resp;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let _ = tokio::time::timeout(remaining, self.notify.notified()).await;
        }
    }

    fn handle_offset_commit(&self, req: &OffsetCommitRequest) -> OffsetCommitResponse {
        let mut resp = OffsetCommitResponse::default();
        let mut guard = self.committed_offsets.lock();
        let group = req.group_id.0.as_str();
        for rt in &req.topics {
            let tname = rt.name.0.as_str();
            let mut tr = OffsetCommitResponseTopic::default();
            tr.name = TopicName(StrBytes::from_string(tname.to_string()));
            for rp in &rt.partitions {
                let mut pr = OffsetCommitResponsePartition::default();
                pr.partition_index = rp.partition_index;
                if self.custom_groups_enabled {
                    guard.insert(
                        (group.to_string(), tname.to_string(), rp.partition_index),
                        rp.committed_offset,
                    );
                    pr.error_code = 0;
                } else {
                    pr.error_code = ResponseError::GroupIdNotFound.code();
                }
                tr.partitions.push(pr);
            }
            resp.topics.push(tr);
        }
        resp
    }

    fn handle_offset_fetch(&self, req: &OffsetFetchRequest) -> OffsetFetchResponse {
        let mut resp = OffsetFetchResponse::default();
        if !self.custom_groups_enabled {
            resp.error_code = ResponseError::GroupIdNotFound.code();
            return resp;
        }
        let guard = self.committed_offsets.lock();
        let group = req.group_id.0.as_str();
        if let Some(ref topics) = req.topics {
            for rt in topics {
                let tname = rt.name.0.as_str();
                let mut tr = OffsetFetchResponseTopic::default();
                tr.name = TopicName(StrBytes::from_string(tname.to_string()));
                for &pid in &rt.partition_indexes {
                    let mut pr = OffsetFetchResponsePartition::default();
                    pr.partition_index = pid;
                    if let Some(&off) = guard.get(&(group.to_string(), tname.to_string(), pid)) {
                        pr.committed_offset = off;
                    } else {
                        pr.committed_offset = -1;
                    }
                    tr.partitions.push(pr);
                }
                resp.topics.push(tr);
            }
        }
        resp
    }
}

struct ScramServerState {
    client_first_bare: String,
    server_first: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

fn handle_scram_step(
    req: &SaslAuthenticateRequest,
    expected_user: &str,
    expected_pass: &str,
    alg: ScramAlg,
    state: &mut Option<ScramServerState>,
) -> (SaslAuthenticateResponse, bool) {
    let mut resp = SaslAuthenticateResponse::default();
    let msg = String::from_utf8_lossy(&req.auth_bytes);
    if state.is_none() {
        let Some(bare) = msg.strip_prefix("n,,") else {
            resp.error_code = ResponseError::SaslAuthenticationFailed.code();
            return (resp, false);
        };
        let mut user = "";
        let mut cnonce = "";
        for part in bare.split(',') {
            if let Some(u) = part.strip_prefix("n=") {
                user = u;
            } else if let Some(r) = part.strip_prefix("r=") {
                cnonce = r;
            }
        }
        if user != expected_user || cnonce.is_empty() {
            resp.error_code = ResponseError::SaslAuthenticationFailed.code();
            return (resp, false);
        }
        let salt = b"mimir-test-salt-16";
        let salt_b64 = base64_encode(salt);
        let snonce = format!("{cnonce}servernonce123");
        let iters = 4096u32;
        let server_first = format!("r={snonce},s={salt_b64},i={iters}");

        let salted = alg.pbkdf2(expected_pass.as_bytes(), salt, iters);
        let client_key = alg.hmac(&salted, b"Client Key");
        let stored_key = alg.hash(&client_key);
        let server_key = alg.hmac(&salted, b"Server Key");

        *state = Some(ScramServerState {
            client_first_bare: bare.to_string(),
            server_first: server_first.clone(),
            stored_key,
            server_key,
        });
        resp.error_code = 0;
        resp.auth_bytes = Bytes::from(server_first.into_bytes());
        (resp, false)
    } else {
        let st = state.take().unwrap();
        let Some((without_proof, proof_part)) = msg.rsplit_once(",p=") else {
            resp.error_code = ResponseError::SaslAuthenticationFailed.code();
            return (resp, false);
        };
        let auth_msg = format!(
            "{},{},{}",
            st.client_first_bare, st.server_first, without_proof
        );
        let client_sig = alg.hmac(&st.stored_key, auth_msg.as_bytes());
        let expected_client_key_xor: Vec<u8> = match mimir_storage::ingest::base64_decode(proof_part)
        {
            Ok(proof) if proof.len() == client_sig.len() => proof
                .iter()
                .zip(client_sig.iter())
                .map(|(a, b)| a ^ b)
                .collect(),
            _ => {
                resp.error_code = ResponseError::SaslAuthenticationFailed.code();
                return (resp, false);
            }
        };
        if alg.hash(&expected_client_key_xor) != st.stored_key {
            resp.error_code = ResponseError::SaslAuthenticationFailed.code();
            return (resp, false);
        }
        let server_sig = base64_encode(&alg.hmac(&st.server_key, auth_msg.as_bytes()));
        resp.error_code = 0;
        resp.auth_bytes = Bytes::from(format!("v={server_sig}").into_bytes());
        (resp, true)
    }
}

pub fn encode_stored_records(records: &[StoredRecord]) -> Bytes {
    let proto_recs: Vec<Record> = records
        .iter()
        .map(|r| {
            let mut headers = IndexMap::with_capacity(r.headers.len());
            for (k, v) in &r.headers {
                headers.insert(StrBytes::from_string(k.clone()), v.clone());
            }
            Record {
                transactional: false,
                control: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: r.offset,
                sequence: NO_SEQUENCE,
                timestamp: r.timestamp_ms,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect();
    let mut buf = BytesMut::new();
    let opts = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        proto_recs.iter(),
        &opts,
        Some(compress_record_batch),
    )
    .unwrap();
    buf.freeze()
}

fn encode_resp<Resp: Encodable + HeaderVersion>(
    api_key: ApiKey,
    api_ver: i16,
    corr_id: i32,
    resp: &Resp,
) -> Bytes {
    let hdr_ver = if api_key == ApiKey::ApiVersions {
        0
    } else {
        Resp::header_version(api_ver)
    };
    let mut hdr = ResponseHeader::default();
    hdr.correlation_id = corr_id;

    let mut out = BytesMut::with_capacity(128);
    out.put_u32(0);
    hdr.encode(&mut out, hdr_ver).unwrap();
    resp.encode(&mut out, api_ver).unwrap();
    let len = (out.len() - 4) as u32;
    out[0..4].copy_from_slice(&len.to_be_bytes());
    out.freeze()
}

fn build_api_versions_response() -> ApiVersionsResponse {
    let mut resp = ApiVersionsResponse::default();
    let apis = [
        (ApiKey::Produce, 0, 9),
        (ApiKey::Fetch, 0, 12),
        (ApiKey::ListOffsets, 0, 7),
        (ApiKey::Metadata, 0, 9),
        (ApiKey::OffsetCommit, 0, 8),
        (ApiKey::OffsetFetch, 0, 7),
        (ApiKey::FindCoordinator, 0, 3),
        (ApiKey::SaslHandshake, 0, 1),
        (ApiKey::ApiVersions, 0, 3),
        (ApiKey::CreateTopics, 0, 5),
        (ApiKey::SaslAuthenticate, 0, 2),
    ];
    for (k, min_v, max_v) in apis {
        let mut av = ApiVersion::default();
        av.api_key = k as i16;
        av.min_version = min_v;
        av.max_version = max_v;
        resp.api_keys.push(av);
    }
    resp
}

// ---------------------------------------------------------------------------
// Test Data & Metric Assertion Helpers
// ---------------------------------------------------------------------------

pub fn create_test_kafka_config(cluster_addr: &str, topic: &str) -> KafkaConfig {
    let mut cfg = KafkaConfig::default();
    if !cluster_addr.is_empty() {
        cfg.address = StringSliceCSV(vec![cluster_addr.to_string()]);
    }
    cfg.topic = topic.to_string();
    cfg.write_timeout = GoDuration::from_secs(5);
    cfg.fetch_concurrency_max = 2;
    cfg.auto_create_topic_enabled = false;
    cfg.consumer_group_offset_commit_interval = GoDuration::from_millis(50);
    cfg.last_produced_offset_poll_interval = GoDuration::from_millis(50);
    cfg
}

pub fn mock_timeseries(metric_name: &str) -> cortexpb::TimeSeries {
    mock_timeseries_with_sample(metric_name, 1, 2.0)
}

pub fn mock_timeseries_with_sample(
    metric_name: &str,
    ts: i64,
    val: f64,
) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: vec![cortexpb::LabelPair::from_strs("__name__", metric_name)],
        samples: vec![cortexpb::Sample {
            value: val,
            timestamp_ms: ts,
        }],
        ..Default::default()
    }
}

pub fn mock_timeseries_with_exemplar(metric_name: &str) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: vec![cortexpb::LabelPair::from_strs("__name__", metric_name)],
        samples: vec![cortexpb::Sample {
            value: 2.0,
            timestamp_ms: 1,
        }],
        exemplars: vec![cortexpb::Exemplar {
            labels: vec![cortexpb::LabelPair::from_strs(
                "trace_id",
                format!("{metric_name}_trace"),
            )],
            value: 14.0,
            timestamp_ms: 2,
        }],
        ..Default::default()
    }
}

pub fn mock_timeseries_with_all(metric_name: &str) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: vec![cortexpb::LabelPair::from_strs("__name__", metric_name)],
        samples: vec![cortexpb::Sample {
            value: 2.0,
            timestamp_ms: 1,
        }],
        exemplars: vec![cortexpb::Exemplar {
            labels: vec![cortexpb::LabelPair::from_strs(
                "trace_id",
                format!("{metric_name}_trace"),
            )],
            value: 14.0,
            timestamp_ms: 2,
        }],
        histograms: vec![cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountFloat(2.0)),
            sum: 10.0,
            schema: 1,
            zero_threshold: 0.001,
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.0)),
            negative_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0],
            positive_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![1.0],
            reset_hint: cortexpb::histogram::ResetHint::Unknown as i32,
            timestamp: 0,
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn mock_metric_metadata(name: &str) -> cortexpb::MetricMetadata {
    cortexpb::MetricMetadata {
        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
        metric_family_name: name.to_string(),
        help: format!("Help for {name}"),
        unit: "seconds".to_string(),
    }
}

pub fn get_counter_value(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            if labels_match(m.get_label(), labels) {
                return m.get_counter().get_value();
            }
        }
    }
    0.0
}

pub fn get_gauge_value(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            if labels_match(m.get_label(), labels) {
                return Some(m.get_gauge().get_value());
            }
        }
    }
    None
}

pub fn get_histogram_count_and_sum(
    reg: &Registry,
    name: &str,
    labels: &[(&str, &str)],
) -> Option<(u64, f64)> {
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            if labels_match(m.get_label(), labels) {
                let h = m.get_histogram();
                return Some((h.get_sample_count(), h.get_sample_sum()));
            }
        }
    }
    None
}

pub fn get_summary_quantile(
    reg: &Registry,
    name: &str,
    quantile: f64,
) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() != name || mf.get_field_type() != MetricType::SUMMARY {
            continue;
        }
        for m in mf.get_metric() {
            for q in m.get_summary().get_quantile() {
                if (q.get_quantile() - quantile).abs() < 0.0001 {
                    return Some(q.get_value());
                }
            }
        }
    }
    None
}

pub fn has_metric(reg: &Registry, name: &str) -> bool {
    reg.gather().iter().any(|mf| mf.get_name() == name)
}

fn labels_match(actual: &[prometheus::proto::LabelPair], expected: &[(&str, &str)]) -> bool {
    for &(k, v) in expected {
        if !actual
            .iter()
            .any(|lp| lp.get_name() == k && lp.get_value() == v)
        {
            return false;
        }
    }
    true
}

pub fn create_kfake_record(
    topic: &str,
    partition: i32,
    content: ProtoBytes,
    version: u32,
) -> (String, i32, ProtoBytes, Vec<(String, ProtoBytes)>) {
    let headers = if version == 0 {
        Vec::new()
    } else {
        vec![(
            "Version".to_string(),
            ProtoBytes::copy_from_slice(&record_version_header_raw(version)),
        )]
    };
    (topic.to_string(), partition, content, headers)
}
