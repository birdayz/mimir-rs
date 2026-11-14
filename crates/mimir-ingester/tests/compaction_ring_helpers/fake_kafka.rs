#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
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
    TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    TimestampType, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use mimir_storage::ingest::compression::{compress_record_batch, decompress_record_batch};
use parking_lot::Mutex as SyncMutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

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

#[derive(Default)]
pub struct Hooks {
    pub fetch: SyncMutex<Option<FetchHook>>,
    pub list_offsets: SyncMutex<Option<ListOffsetsHook>>,
}

pub struct FakeKafkaCluster {
    pub addr: String,
    pub port: i32,
    topics: SyncMutex<HashMap<String, Vec<PartitionState>>>,
    committed_offsets: SyncMutex<HashMap<(String, String, i32), i64>>,
    notify: Notify,
    pub hooks: Hooks,
    shutdown: Arc<tokio::sync::watch::Sender<bool>>,
}

impl FakeKafkaCluster {
    pub async fn start(num_partitions: i32, initial_topic: &str) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        Self::start_with_listener(listener, num_partitions, initial_topic)
    }

    pub fn start_with_listener(
        listener: TcpListener,
        num_partitions: i32,
        initial_topic: &str,
    ) -> Arc<Self> {
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
                _ => break,
            };

            let Ok(req_hdr) = RequestHeader::decode(&mut buf, req_hdr_ver) else {
                break;
            };
            let corr_id = req_hdr.correlation_id;

            match api_key {
                ApiKey::ApiVersions => {
                    let _ = ApiVersionsRequest::decode(&mut buf, api_ver);
                    let resp = encode_resp(api_key, api_ver, corr_id, &build_api_versions_response());
                    if resp_tx.send(resp).await.is_err() {
                        break;
                    }
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
                    resp.port = self.port;
                    let bytes = encode_resp(api_key, api_ver, corr_id, &resp);
                    if resp_tx.send(bytes).await.is_err() {
                        break;
                    }
                }
                ApiKey::CreateTopics => {
                    let Ok(req) = CreateTopicsRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let resp = self.handle_create_topics(&req);
                    let _ = resp_tx
                        .send(encode_resp(api_key, api_ver, corr_id, &resp))
                        .await;
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
                    let resp = self.handle_produce(&req);
                    let _ = resp_tx
                        .send(encode_resp(api_key, api_ver, corr_id, &resp))
                        .await;
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
                    let resp = self.handle_offset_commit(&req);
                    let _ = resp_tx
                        .send(encode_resp(api_key, api_ver, corr_id, &resp))
                        .await;
                }
                ApiKey::OffsetFetch => {
                    let Ok(req) = OffsetFetchRequest::decode(&mut buf, api_ver) else {
                        break;
                    };
                    let resp = self.handle_offset_fetch(&req);
                    let _ = resp_tx
                        .send(encode_resp(api_key, api_ver, corr_id, &resp))
                        .await;
                }
                _ => break,
            }
        }
    }

    fn handle_metadata(&self, req: &MetadataRequest) -> MetadataResponse {
        let mut resp = MetadataResponse::default();
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(0);
        broker.host = StrBytes::from_static_str("127.0.0.1");
        broker.port = self.port;
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
                    } else if let Some(found) =
                        p.records.iter().find(|r| r.timestamp_ms >= rp.timestamp)
                    {
                        lr_p.offset = found.offset;
                        lr_p.timestamp = found.timestamp_ms;
                    } else {
                        lr_p.offset = -1;
                        lr_p.timestamp = -1;
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
                guard.insert(
                    (group.to_string(), tname.to_string(), rp.partition_index),
                    rp.committed_offset,
                );
                pr.error_code = 0;
                tr.partitions.push(pr);
            }
            resp.topics.push(tr);
        }
        resp
    }

    fn handle_offset_fetch(&self, req: &OffsetFetchRequest) -> OffsetFetchResponse {
        let mut resp = OffsetFetchResponse::default();
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
        (ApiKey::ApiVersions, 0, 3),
        (ApiKey::CreateTopics, 0, 5),
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
