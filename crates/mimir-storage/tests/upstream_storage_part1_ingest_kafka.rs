//! 1:1 upstream test ports for `pkg/storage/ingest/` (Kafka-backed tests: 6 files, 30 tests).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
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
use mimir_common::backoff::BackoffConfig;
use mimir_config::{GoDuration, KafkaConfig, OrderedConsumptionConfig, StringSliceCSV};
use mimir_proto::cortexpb::{write_request::SourceEnum, LabelPair, Sample, TimeSeries, WriteRequest};
use mimir_storage::ingest::compression::compress_record_batch;
use mimir_storage::ingest::compression::decompress_record_batch;
use mimir_storage::ingest::{
    decode_fetched_record, find_gaps_in_records, handle_kafka_fetch_err, ConcurrentFetchers,
    FetchResult, FetchWaiter, FetchWant, FetchedRecord, FetcherMetrics, GenericOffsetReader,
    GetPartitionIDsFn, IngestWriter, KafkaClient, KafkaClientError, KafkaFetchError,
    MetadataRefresher, MultiClusterOffsetsReader, MultiClusterPartitionReader, OffsetRange,
    PartitionOffsetClient, PartitionOffsets, RangeErrorPolicy, RecordBatchConsumer,
    SingleClusterPartitionOffsetReader, SingleClusterTopicOffsetsReader, CHOSEN_BROKER_DIED,
    INITIAL_BYTES_PER_RECORD, UNKNOWN_BROKER,
};
use mimir_proto::zerocopy::WriteRequestScratch;
use parking_lot::Mutex as SyncMutex;
use prometheus::{Registry, TextEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{watch, Notify};

// ===========================================================================
// Lightweight in-process async TCP Mock Kafka Cluster (via `kafka-protocol`)
// ===========================================================================

type ListOffsetsHook = Arc<
    dyn Fn(
            &ListOffsetsRequest,
        ) -> Pin<Box<dyn Future<Output = Option<ListOffsetsResponse>> + Send>>
        + Send
        + Sync,
>;

type MetadataHook = Arc<
    dyn Fn(&MetadataRequest) -> Option<MetadataResponse> + Send + Sync,
>;

type FetchHook = Arc<
    dyn Fn(&FetchRequest) -> Option<FetchResponse> + Send + Sync,
>;

#[derive(Clone)]
struct StoredRecord {
    offset: i64,
    timestamp_ms: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<(String, Option<Bytes>)>,
}

struct PartitionLog {
    log_start_offset: i64,
    records: Vec<StoredRecord>,
}

impl PartitionLog {
    fn new() -> Self {
        Self {
            log_start_offset: 0,
            records: Vec::new(),
        }
    }

    fn next_offset(&self) -> i64 {
        self.records.last().map(|r| r.offset + 1).unwrap_or(0)
    }
}

struct TopicState {
    topic_id: [u8; 16],
    partitions: HashMap<i32, PartitionLog>,
}

struct BrokerState {
    port: u16,
    topics: HashMap<String, TopicState>,
    committed_offsets: HashMap<(String, String, i32), i64>,
    next_topic_id: u64,
    list_offsets_hook: Option<ListOffsetsHook>,
    metadata_hook: Option<MetadataHook>,
    fetch_hook: Option<FetchHook>,
}

struct MockKafkaCluster {
    addr: String,
    state: Arc<SyncMutex<BrokerState>>,
    notify: Arc<Notify>,
    stop_tx: watch::Sender<bool>,
}

impl MockKafkaCluster {
    async fn start(num_partitions: i32, topic: &str) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("127.0.0.1:{port}");

        let mut state = BrokerState {
            port,
            topics: HashMap::new(),
            committed_offsets: HashMap::new(),
            next_topic_id: 1,
            list_offsets_hook: None,
            metadata_hook: None,
            fetch_hook: None,
        };
        Self::create_topic_in_state(&mut state, topic, num_partitions);

        let state = Arc::new(SyncMutex::new(state));
        let notify = Arc::new(Notify::new());
        let (stop_tx, mut stop_rx) = watch::channel(false);

        let st = Arc::clone(&state);
        let nt = Arc::clone(&notify);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break; };
                        let st_c = Arc::clone(&st);
                        let nt_c = Arc::clone(&nt);
                        let mut stop_c = stop_rx.clone();
                        tokio::spawn(async move {
                            handle_conn(stream, st_c, nt_c, &mut stop_c).await;
                        });
                    }
                }
            }
        });

        Arc::new(Self {
            addr,
            state,
            notify,
            stop_tx,
        })
    }

    fn create_topic_in_state(state: &mut BrokerState, topic: &str, num_partitions: i32) {
        if state.topics.contains_key(topic) {
            return;
        }
        let mut tid = [0u8; 16];
        tid[8..16].copy_from_slice(&state.next_topic_id.to_be_bytes());
        state.next_topic_id += 1;
        let mut parts = HashMap::new();
        for p in 0..num_partitions.max(1) {
            parts.insert(p, PartitionLog::new());
        }
        state.topics.insert(
            topic.to_string(),
            TopicState {
                topic_id: tid,
                partitions: parts,
            },
        );
    }

    fn create_topic(&self, topic: &str, num_partitions: i32) {
        let mut guard = self.state.lock();
        Self::create_topic_in_state(&mut guard, topic, num_partitions);
    }

    fn produce_raw(&self, topic: &str, partition: i32, value: &[u8]) -> i64 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut guard = self.state.lock();
        let t = guard.topics.get_mut(topic).expect("topic exists");
        let p = t.partitions.get_mut(&partition).expect("partition exists");
        let off = p.next_offset();
        p.records.push(StoredRecord {
            offset: off,
            timestamp_ms: now_ms,
            key: None,
            value: Some(Bytes::copy_from_slice(value)),
            headers: Vec::new(),
        });
        drop(guard);
        self.notify.notify_waiters();
        off
    }

    fn delete_records_before(&self, topic: &str, partition: i32, new_start_offset: i64) {
        let mut guard = self.state.lock();
        if let Some(t) = guard.topics.get_mut(topic) {
            if let Some(p) = t.partitions.get_mut(&partition) {
                p.log_start_offset = p.log_start_offset.max(new_start_offset);
                p.records.retain(|r| r.offset >= p.log_start_offset);
            }
        }
    }

    fn set_list_offsets_hook(&self, hook: Option<ListOffsetsHook>) {
        self.state.lock().list_offsets_hook = hook;
    }

    fn set_metadata_hook(&self, hook: Option<MetadataHook>) {
        self.state.lock().metadata_hook = hook;
    }

    fn set_fetch_hook(&self, hook: Option<FetchHook>) {
        self.state.lock().fetch_hook = hook;
    }

    fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }
}

impl Drop for MockKafkaCluster {
    fn drop(&mut self) {
        self.stop();
    }
}

enum MockReq {
    ApiVersions,
    Metadata(MetadataRequest),
    CreateTopics(CreateTopicsRequest),
    ListOffsets(ListOffsetsRequest),
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    FindCoordinator,
    OffsetCommit(OffsetCommitRequest),
    OffsetFetch(OffsetFetchRequest),
}

enum MockResp {
    ApiVersions(ApiVersionsResponse),
    Metadata(MetadataResponse),
    CreateTopics(CreateTopicsResponse),
    ListOffsets(ListOffsetsResponse),
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    FindCoordinator(FindCoordinatorResponse),
    OffsetCommit(OffsetCommitResponse),
    OffsetFetch(OffsetFetchResponse),
}

impl MockResp {
    fn header_version(&self, ver: i16) -> i16 {
        match self {
            Self::ApiVersions(_) => 0,
            Self::Metadata(_) => MetadataResponse::header_version(ver),
            Self::CreateTopics(_) => CreateTopicsResponse::header_version(ver),
            Self::ListOffsets(_) => ListOffsetsResponse::header_version(ver),
            Self::Produce(_) => ProduceResponse::header_version(ver),
            Self::Fetch(_) => FetchResponse::header_version(ver),
            Self::FindCoordinator(_) => FindCoordinatorResponse::header_version(ver),
            Self::OffsetCommit(_) => OffsetCommitResponse::header_version(ver),
            Self::OffsetFetch(_) => OffsetFetchResponse::header_version(ver),
        }
    }

    fn encode(&self, buf: &mut BytesMut, ver: i16) {
        match self {
            Self::ApiVersions(r) => r.encode(buf, ver).unwrap(),
            Self::Metadata(r) => r.encode(buf, ver).unwrap(),
            Self::CreateTopics(r) => r.encode(buf, ver).unwrap(),
            Self::ListOffsets(r) => r.encode(buf, ver).unwrap(),
            Self::Produce(r) => r.encode(buf, ver).unwrap(),
            Self::Fetch(r) => r.encode(buf, ver).unwrap(),
            Self::FindCoordinator(r) => r.encode(buf, ver).unwrap(),
            Self::OffsetCommit(r) => r.encode(buf, ver).unwrap(),
            Self::OffsetFetch(r) => r.encode(buf, ver).unwrap(),
        }
    }
}

fn decode_mock_req(api_key: ApiKey, buf: &mut Bytes, ver: i16) -> Option<MockReq> {
    match api_key {
        ApiKey::ApiVersions => {
            let _ = ApiVersionsRequest::decode(buf, ver).ok()?;
            Some(MockReq::ApiVersions)
        }
        ApiKey::Metadata => Some(MockReq::Metadata(MetadataRequest::decode(buf, ver).ok()?)),
        ApiKey::CreateTopics => Some(MockReq::CreateTopics(CreateTopicsRequest::decode(buf, ver).ok()?)),
        ApiKey::ListOffsets => Some(MockReq::ListOffsets(ListOffsetsRequest::decode(buf, ver).ok()?)),
        ApiKey::Produce => Some(MockReq::Produce(ProduceRequest::decode(buf, ver).ok()?)),
        ApiKey::Fetch => Some(MockReq::Fetch(FetchRequest::decode(buf, ver).ok()?)),
        ApiKey::FindCoordinator => {
            let _ = FindCoordinatorRequest::decode(buf, ver).ok()?;
            Some(MockReq::FindCoordinator)
        }
        ApiKey::OffsetCommit => Some(MockReq::OffsetCommit(OffsetCommitRequest::decode(buf, ver).ok()?)),
        ApiKey::OffsetFetch => Some(MockReq::OffsetFetch(OffsetFetchRequest::decode(buf, ver).ok()?)),
        _ => None,
    }
}

async fn handle_conn(
    stream: tokio::net::TcpStream,
    state: Arc<SyncMutex<BrokerState>>,
    notify: Arc<Notify>,
    stop_rx: &mut watch::Receiver<bool>,
) {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<Bytes>(128);

    tokio::spawn(async move {
        while let Some(frame) = resp_rx.recv().await {
            if writer.write_all(&frame).await.is_err() {
                break;
            }
        }
    });

    loop {
        let mut len_buf = [0u8; 4];
        tokio::select! {
            biased;
            _ = stop_rx.changed() => break,
            r = reader.read_exact(&mut len_buf) => {
                if r.is_err() { break; }
            }
        }
        let len = i32::from_be_bytes(len_buf) as usize;
        if !(4..=128 * 1024 * 1024).contains(&len) {
            break;
        }
        let mut body = vec![0u8; len];
        if reader.read_exact(&mut body).await.is_err() {
            break;
        }

        let mut buf = Bytes::from(body);
        if buf.len() < 4 {
            break;
        }
        let api_key_i16 = i16::from_be_bytes([buf[0], buf[1]]);
        let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
        let Ok(api_key) = ApiKey::try_from(api_key_i16) else {
            break;
        };
        let req_hdr_ver = api_key.request_header_version(api_ver);
        let Ok(hdr) = RequestHeader::decode(&mut buf, req_hdr_ver) else {
            break;
        };
        let Some(req_kind) = decode_mock_req(api_key, &mut buf, api_ver) else {
            break;
        };

        let st = Arc::clone(&state);
        let nt = Arc::clone(&notify);
        let tx = resp_tx.clone();
        let corr_id = hdr.correlation_id;
        tokio::spawn(async move {
            let resp_kind = process_request(req_kind, st, nt).await;
            let resp_hdr_ver = resp_kind.header_version(api_ver);
            let mut resp_hdr = ResponseHeader::default();
            resp_hdr.correlation_id = corr_id;

            let mut out = BytesMut::new();
            resp_hdr.encode(&mut out, resp_hdr_ver).unwrap();
            resp_kind.encode(&mut out, api_ver);

            let mut frame = BytesMut::with_capacity(4 + out.len());
            frame.put_i32(out.len() as i32);
            frame.extend_from_slice(&out);
            let _ = tx.send(frame.freeze()).await;
        });
    }
}

async fn process_request(
    req: MockReq,
    state: Arc<SyncMutex<BrokerState>>,
    notify: Arc<Notify>,
) -> MockResp {
    match req {
        MockReq::ApiVersions => {
            let mut resp = ApiVersionsResponse::default();
            for (k, max_v) in [
                (ApiKey::Produce, 9),
                (ApiKey::Fetch, 13),
                (ApiKey::ListOffsets, 7),
                (ApiKey::Metadata, 10),
                (ApiKey::OffsetCommit, 8),
                (ApiKey::OffsetFetch, 7),
                (ApiKey::FindCoordinator, 3),
                (ApiKey::ApiVersions, 3),
                (ApiKey::CreateTopics, 5),
            ] {
                let mut av = ApiVersion::default();
                av.api_key = k as i16;
                av.min_version = 0;
                av.max_version = max_v;
                resp.api_keys.push(av);
            }
            MockResp::ApiVersions(resp)
        }
        MockReq::Metadata(mreq) => {
            let hook = state.lock().metadata_hook.clone();
            if let Some(h) = hook {
                if let Some(custom) = h(&mreq) {
                    return MockResp::Metadata(custom);
                }
            }
            let guard = state.lock();
            let mut resp = MetadataResponse::default();
            let mut b = MetadataResponseBroker::default();
            b.node_id = BrokerId(0);
            b.host = StrBytes::from_static_str("127.0.0.1");
            b.port = guard.port as i32;
            resp.brokers.push(b);
            resp.controller_id = BrokerId(0);

            let requested: Option<Vec<String>> = mreq.topics.as_ref().map(|ts| {
                ts.iter()
                    .filter_map(|t| t.name.as_ref().map(|n| n.0.as_str().to_string()))
                    .collect()
            });

            let topic_names: Vec<String> = match requested {
                Some(names) => names,
                None => guard.topics.keys().cloned().collect(),
            };

            for name in topic_names {
                let mut mt = MetadataResponseTopic::default();
                mt.name = Some(TopicName(StrBytes::from_string(name.clone())));
                if let Some(ts) = guard.topics.get(&name) {
                    let hex = format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        ts.topic_id[0], ts.topic_id[1], ts.topic_id[2], ts.topic_id[3],
                        ts.topic_id[4], ts.topic_id[5], ts.topic_id[6], ts.topic_id[7],
                        ts.topic_id[8], ts.topic_id[9], ts.topic_id[10], ts.topic_id[11],
                        ts.topic_id[12], ts.topic_id[13], ts.topic_id[14], ts.topic_id[15],
                    );
                    mt.topic_id = hex.parse().unwrap_or_default();
                    let mut pids: Vec<i32> = ts.partitions.keys().copied().collect();
                    pids.sort();
                    for pid in pids {
                        let mut mp = MetadataResponsePartition::default();
                        mp.partition_index = pid;
                        mp.leader_id = BrokerId(0);
                        mp.leader_epoch = 0;
                        mp.replica_nodes = vec![BrokerId(0)];
                        mp.isr_nodes = vec![BrokerId(0)];
                        mt.partitions.push(mp);
                    }
                } else {
                    mt.error_code = ResponseError::UnknownTopicOrPartition.code();
                }
                resp.topics.push(mt);
            }
            MockResp::Metadata(resp)
        }
        MockReq::CreateTopics(creq) => {
            let mut guard = state.lock();
            let mut resp = CreateTopicsResponse::default();
            for ct in creq.topics {
                let name = ct.name.0.as_str().to_string();
                let mut res = CreatableTopicResult::default();
                res.name = ct.name;
                if guard.topics.contains_key(&name) {
                    res.error_code = ResponseError::TopicAlreadyExists.code();
                } else {
                    let parts = if ct.num_partitions <= 0 { 1 } else { ct.num_partitions };
                    MockKafkaCluster::create_topic_in_state(&mut guard, &name, parts);
                }
                resp.topics.push(res);
            }
            MockResp::CreateTopics(resp)
        }
        MockReq::ListOffsets(lreq) => {
            let hook = state.lock().list_offsets_hook.clone();
            if let Some(h) = hook {
                if let Some(custom) = h(&lreq).await {
                    return MockResp::ListOffsets(custom);
                }
            }
            let guard = state.lock();
            let mut resp = ListOffsetsResponse::default();
            for lt in lreq.topics {
                let tname = lt.name.0.as_str().to_string();
                let mut rt = ListOffsetsTopicResponse::default();
                rt.name = lt.name;
                for lp in lt.partitions {
                    let mut rp = ListOffsetsPartitionResponse::default();
                    rp.partition_index = lp.partition_index;
                    if let Some(ts) = guard.topics.get(&tname).and_then(|t| t.partitions.get(&lp.partition_index)) {
                        match lp.timestamp {
                            -2 => {
                                rp.offset = ts.log_start_offset;
                            }
                            -1 => {
                                rp.offset = ts.next_offset();
                            }
                            target_ts => {
                                rp.offset = ts
                                    .records
                                    .iter()
                                    .find(|r| r.timestamp_ms >= target_ts)
                                    .map(|r| r.offset)
                                    .unwrap_or_else(|| ts.next_offset());
                            }
                        }
                    } else {
                        rp.error_code = ResponseError::UnknownTopicOrPartition.code();
                    }
                    rt.partitions.push(rp);
                }
                resp.topics.push(rt);
            }
            MockResp::ListOffsets(resp)
        }
        MockReq::Produce(preq) => {
            let mut resp = ProduceResponse::default();
            let mut produced_any = false;
            {
                let mut guard = state.lock();
                for tp in preq.topic_data {
                    let tname = tp.name.0.as_str().to_string();
                    let mut rtp = TopicProduceResponse::default();
                    rtp.name = tp.name;
                    for pp in tp.partition_data {
                        let mut rpp = PartitionProduceResponse::default();
                        rpp.index = pp.index;
                        if let Some(part) = guard
                            .topics
                            .get_mut(&tname)
                            .and_then(|t| t.partitions.get_mut(&pp.index))
                        {
                            let base_offset = part.next_offset();
                            if let Some(mut raw) = pp.records {
                                if let Ok(batch) = RecordBatchDecoder::decode_with_custom_compression(
                                    &mut raw,
                                    Some(decompress_record_batch),
                                ) {
                                    for (i, rec) in batch.into_iter().enumerate() {
                                        let headers = rec
                                            .headers
                                            .into_iter()
                                            .map(|(k, v)| (k.as_str().to_string(), v))
                                            .collect();
                                        part.records.push(StoredRecord {
                                            offset: base_offset + i as i64,
                                            timestamp_ms: rec.timestamp,
                                            key: rec.key,
                                            value: rec.value,
                                            headers,
                                        });
                                    }
                                    produced_any = true;
                                }
                            }
                            rpp.base_offset = base_offset;
                            rpp.log_start_offset = part.log_start_offset;
                        } else {
                            rpp.error_code = ResponseError::UnknownTopicOrPartition.code();
                        }
                        rtp.partition_responses.push(rpp);
                    }
                    resp.responses.push(rtp);
                }
            }
            if produced_any {
                notify.notify_waiters();
            }
            MockResp::Produce(resp)
        }
        MockReq::Fetch(freq) => {
            let hook = state.lock().fetch_hook.clone();
            if let Some(h) = hook {
                if let Some(custom) = h(&freq) {
                    return MockResp::Fetch(custom);
                }
            }

            let wait_dur = Duration::from_millis((freq.max_wait_ms.max(0) as u64).min(200));
            let deadline = tokio::time::Instant::now() + wait_dur;

            loop {
                let mut should_wait = false;
                let mut resp = FetchResponse::default();
                {
                    let guard = state.lock();
                    for ft in &freq.topics {
                        let matched_topic = if !ft.topic.0.as_str().is_empty() {
                            guard
                                .topics
                                .get(ft.topic.0.as_str())
                                .map(|ts| (ft.topic.0.as_str().to_string(), ts))
                        } else {
                            let tid_bytes = *ft.topic_id.as_bytes();
                            guard
                                .topics
                                .iter()
                                .find(|(_, ts)| ts.topic_id == tid_bytes)
                                .map(|(n, ts)| (n.clone(), ts))
                        };

                        let mut rt = FetchableTopicResponse::default();
                        rt.topic = ft.topic.clone();
                        rt.topic_id = ft.topic_id;

                        for fp in &ft.partitions {
                            let mut pd = PartitionData::default();
                            pd.partition_index = fp.partition;
                            if let Some((ref tname, ts)) = matched_topic {
                                rt.topic = TopicName(StrBytes::from_string(tname.clone()));
                                if let Some(part) = ts.partitions.get(&fp.partition) {
                                    let next_off = part.next_offset();
                                    pd.high_watermark = next_off;
                                    pd.log_start_offset = part.log_start_offset;
                                    if fp.fetch_offset < part.log_start_offset
                                        || fp.fetch_offset > next_off
                                    {
                                        pd.error_code = ResponseError::OffsetOutOfRange.code();
                                    } else {
                                        let mut batch_records = Vec::new();
                                        let mut total_bytes = 0usize;
                                        let max_b = fp.partition_max_bytes.max(1024) as usize;
                                        for sr in &part.records {
                                            if sr.offset < fp.fetch_offset {
                                                continue;
                                            }
                                            let sz = sr.value.as_ref().map(|b| b.len()).unwrap_or(0) + 64;
                                            if !batch_records.is_empty() && total_bytes + sz > max_b {
                                                break;
                                            }
                                            total_bytes += sz;
                                            let mut hdrs = IndexMap::new();
                                            for (k, v) in &sr.headers {
                                                hdrs.insert(StrBytes::from_string(k.clone()), v.clone());
                                            }
                                            batch_records.push(Record {
                                                transactional: false,
                                                control: false,
                                                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                                                producer_id: NO_PRODUCER_ID,
                                                producer_epoch: NO_PRODUCER_EPOCH,
                                                timestamp_type: TimestampType::Creation,
                                                offset: sr.offset,
                                                sequence: NO_SEQUENCE,
                                                timestamp: sr.timestamp_ms,
                                                key: sr.key.clone(),
                                                value: sr.value.clone(),
                                                headers: hdrs,
                                            });
                                        }
                                        if !batch_records.is_empty() {
                                            let mut enc = BytesMut::new();
                                            let opts = RecordEncodeOptions {
                                                version: 2,
                                                compression: Compression::None,
                                            };
                                            RecordBatchEncoder::encode_with_custom_compression(
                                                &mut enc,
                                                batch_records.iter(),
                                                &opts,
                                                Some(compress_record_batch),
                                            )
                                            .unwrap();
                                            pd.records = Some(enc.freeze());
                                        } else if fp.fetch_offset == next_off {
                                            should_wait = true;
                                        }
                                    }
                                } else {
                                    pd.error_code = ResponseError::UnknownTopicOrPartition.code();
                                }
                            } else {
                                pd.error_code = ResponseError::UnknownTopicOrPartition.code();
                            }
                            rt.partitions.push(pd);
                        }
                        resp.responses.push(rt);
                    }
                }

                if !should_wait || tokio::time::Instant::now() >= deadline {
                    return MockResp::Fetch(resp);
                }

                let notified = notify.notified();
                let rem = deadline.saturating_duration_since(tokio::time::Instant::now());
                let _ = tokio::time::timeout(rem, notified).await;
            }
        }
        MockReq::FindCoordinator => {
            let guard = state.lock();
            let mut resp = FindCoordinatorResponse::default();
            resp.node_id = BrokerId(0);
            resp.host = StrBytes::from_static_str("127.0.0.1");
            resp.port = guard.port as i32;
            MockResp::FindCoordinator(resp)
        }
        MockReq::OffsetCommit(creq) => {
            let mut guard = state.lock();
            let group = creq.group_id.0.as_str().to_string();
            let mut resp = OffsetCommitResponse::default();
            for t in creq.topics {
                let tname = t.name.0.as_str().to_string();
                let mut rt = OffsetCommitResponseTopic::default();
                rt.name = t.name;
                for p in t.partitions {
                    guard.committed_offsets.insert(
                        (group.clone(), tname.clone(), p.partition_index),
                        p.committed_offset,
                    );
                    let mut rp = OffsetCommitResponsePartition::default();
                    rp.partition_index = p.partition_index;
                    rt.partitions.push(rp);
                }
                resp.topics.push(rt);
            }
            MockResp::OffsetCommit(resp)
        }
        MockReq::OffsetFetch(freq) => {
            let guard = state.lock();
            let group = freq.group_id.0.as_str().to_string();
            let mut resp = OffsetFetchResponse::default();
            for t in freq.topics.unwrap_or_default() {
                let tname = t.name.0.as_str().to_string();
                let mut rt = OffsetFetchResponseTopic::default();
                rt.name = t.name;
                for pid in t.partition_indexes {
                    let mut rp = OffsetFetchResponsePartition::default();
                    rp.partition_index = pid;
                    rp.committed_offset = guard
                        .committed_offsets
                        .get(&(group.clone(), tname.clone(), pid))
                        .copied()
                        .unwrap_or(-1);
                    rt.partitions.push(rp);
                }
                resp.topics.push(rt);
            }
            MockResp::OffsetFetch(resp)
        }
    }
}

fn make_test_kafka_cfg(addr: &str, topic: &str) -> KafkaConfig {
    KafkaConfig {
        address: StringSliceCSV(vec![addr.to_string()]),
        topic: topic.to_string(),
        client_id: "test-client".to_string(),
        auto_create_topic_enabled: false,
        consume_from_position_at_startup: "start".to_string(),
        last_produced_offset_poll_interval: GoDuration::from_millis(25),
        consumer_group_offset_commit_interval: GoDuration::from_millis(25),
        fetch_max_wait: GoDuration::from_millis(100),
        ..Default::default()
    }
}

fn mock_write_req(metric_name: &str) -> WriteRequest {
    WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![LabelPair::from_strs("__name__", metric_name)],
            samples: vec![Sample {
                timestamp_ms: 1_700_000_000_000,
                value: 1.0,
            }],
            ..Default::default()
        }],
        source: SourceEnum::Api as i32,
        ..Default::default()
    }
}

// ===========================================================================
// pkg/storage/ingest/fetcher_test.go (11 tests)
// ===========================================================================

struct FlagWaiter(Arc<AtomicBool>);
impl FetchWaiter for FlagWaiter {
    fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.0.store(true, Ordering::SeqCst);
        Box::pin(async {})
    }
}

struct FlagRefresher(Arc<AtomicBool>);
impl MetadataRefresher for FlagRefresher {
    fn force_metadata_refresh(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.0.store(true, Ordering::SeqCst);
        Box::pin(async {})
    }
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestHandleKafkaFetchErr
#[tokio::test]
async fn test_handle_kafka_fetch_err() {
    struct Case {
        name: &'static str,
        err: Option<KafkaFetchError>,
        lso: i64,
        fw: FetchWant,
        policy: RangeErrorPolicy,
        exp_fw: FetchWant,
        exp_backoff: bool,
        exp_refresh: bool,
        exp_err: Option<KafkaFetchError>,
    }

    let fw_default = FetchWant {
        start_offset: 11,
        end_offset: 15,
        estimated_bytes_per_record: 100,
        max_bytes_limit: 1_000_000,
    };

    let cases = vec![
        Case {
            name: "no error",
            err: None,
            lso: 1,
            fw: FetchWant { start_offset: 1, end_offset: 5, ..fw_default },
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: FetchWant { start_offset: 1, end_offset: 5, ..fw_default },
            exp_backoff: false,
            exp_refresh: false,
            exp_err: None,
        },
        Case {
            name: "offset out of range - fetching slightly before start",
            err: Some(KafkaFetchError::Broker(ResponseError::OffsetOutOfRange)),
            lso: 5,
            fw: FetchWant { start_offset: 4, end_offset: 10, ..fw_default },
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: FetchWant { start_offset: 5, end_offset: 10, ..fw_default },
            exp_backoff: false,
            exp_refresh: false,
            exp_err: None,
        },
        Case {
            name: "offset out of range - completely outside available offsets",
            err: Some(KafkaFetchError::Broker(ResponseError::OffsetOutOfRange)),
            lso: 5,
            fw: FetchWant { start_offset: 1, end_offset: 3, ..fw_default },
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: FetchWant { start_offset: 3, end_offset: 3, ..fw_default },
            exp_backoff: false,
            exp_refresh: false,
            exp_err: None,
        },
        Case {
            name: "offset out of range - abort",
            err: Some(KafkaFetchError::Broker(ResponseError::OffsetOutOfRange)),
            lso: 5,
            fw: FetchWant { start_offset: 1, end_offset: 3, ..fw_default },
            policy: RangeErrorPolicy::Abort,
            exp_fw: FetchWant { start_offset: 1, end_offset: 3, ..fw_default },
            exp_backoff: false,
            exp_refresh: false,
            exp_err: Some(KafkaFetchError::Broker(ResponseError::OffsetOutOfRange)),
        },
        Case {
            name: "recoverable error",
            err: Some(KafkaFetchError::Broker(ResponseError::KafkaStorageError)),
            lso: -1,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: false,
            exp_err: None,
        },
        Case {
            name: "NotLeaderForPartition",
            err: Some(KafkaFetchError::Broker(ResponseError::NotLeaderOrFollower)),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
        Case {
            name: "ReplicaNotAvailable",
            err: Some(KafkaFetchError::Broker(ResponseError::ReplicaNotAvailable)),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
        Case {
            name: "UnknownLeaderEpoch",
            err: Some(KafkaFetchError::Broker(ResponseError::UnknownLeaderEpoch)),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
        Case {
            name: "FencedLeaderEpoch",
            err: Some(KafkaFetchError::Broker(ResponseError::FencedLeaderEpoch)),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
        Case {
            name: "LeaderNotAvailable",
            err: Some(KafkaFetchError::Broker(ResponseError::LeaderNotAvailable)),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
        Case {
            name: "BrokerNotAvailable",
            err: Some(KafkaFetchError::Broker(ResponseError::BrokerNotAvailable)),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
        Case {
            name: "errUnknownPartitionLeader",
            err: Some(KafkaFetchError::UnknownPartitionLeader),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
        Case {
            name: "unknown broker",
            err: Some(KafkaFetchError::Other(UNKNOWN_BROKER.to_string())),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: false,
            exp_refresh: false,
            exp_err: None,
        },
        Case {
            name: "closed broker",
            err: Some(KafkaFetchError::Other(CHOSEN_BROKER_DIED.to_string())),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: false,
            exp_refresh: false,
            exp_err: None,
        },
        Case {
            name: "closed network connection",
            err: Some(KafkaFetchError::Other(
                "read tcp 10.0.227.72:37486->10.0.29.4:9092: use of closed network connection".to_string(),
            )),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: false,
            exp_refresh: false,
            exp_err: None,
        },
        Case {
            name: "network timeout",
            err: Some(KafkaFetchError::Other(
                "read tcp 127.0.0.1:62984->127.0.0.1:9092: i/o timeout".to_string(),
            )),
            lso: 5,
            fw: fw_default.clone(),
            policy: RangeErrorPolicy::ResumeFromStart,
            exp_fw: fw_default.clone(),
            exp_backoff: true,
            exp_refresh: true,
            exp_err: None,
        },
    ];

    for tc in cases {
        let waited = Arc::new(AtomicBool::new(false));
        let refreshed = Arc::new(AtomicBool::new(false));
        let waiter = FlagWaiter(Arc::clone(&waited));
        let refresher = FlagRefresher(Arc::clone(&refreshed));

        let lso = tc.lso;
        let offset_reader = GenericOffsetReader::new(
            Arc::new(move || Box::pin(async move { Ok(lso) })),
            Duration::from_millis(10),
        );
        offset_reader.start().await;

        let res = handle_kafka_fetch_err(
            tc.err.as_ref(),
            tc.fw.clone(),
            &waiter,
            tc.policy,
            &offset_reader,
            &refresher,
        )
        .await;
        offset_reader.stop().await;

        match tc.exp_err {
            Some(e) => assert_eq!(res.unwrap_err(), e, "{}", tc.name),
            None => assert_eq!(res.unwrap(), tc.exp_fw, "{}", tc.name),
        }
        assert_eq!(waited.load(Ordering::SeqCst), tc.exp_backoff, "{}", tc.name);
        assert_eq!(refreshed.load(Ordering::SeqCst), tc.exp_refresh, "{}", tc.name);
    }
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestFranzGoErrorStrings
#[test]
fn test_franz_go_error_strings() {
    assert_eq!(UNKNOWN_BROKER, "unknown broker");
    assert!(CHOSEN_BROKER_DIED.contains("chosen to issue this request has died"));
}

fn fast_backoff_cfg() -> BackoffConfig {
    BackoffConfig {
        min_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
        max_retries: 0,
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_concurrent_fetchers(
    cluster: &MockKafkaCluster,
    topic: &str,
    partition_id: i32,
    start_offset: i64,
    concurrency: usize,
    max_inflight_bytes: i32,
    start: bool,
    policy: RangeErrorPolicy,
) -> (ConcurrentFetchers, Registry) {
    let reg = Registry::new();
    let cfg = make_test_kafka_cfg(&cluster.addr, topic);
    let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
    let offset_client = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, Some(&reg));

    let oc = offset_client.clone();
    let t = topic.to_string();
    let start_offsets_reader = Arc::new(GenericOffsetReader::new(
        Arc::new(move || {
            let oc = oc.clone();
            let t = t.clone();
            Box::pin(async move {
                oc.fetch_partition_start_offset(&t, partition_id)
                    .await
                    .map_err(|e| e.to_string())
            })
        }),
        Duration::from_secs(1),
    ));
    start_offsets_reader.start().await;

    let f = ConcurrentFetchers::new(
        client,
        topic,
        partition_id,
        start_offset,
        concurrency,
        max_inflight_bytes,
        true,
        Duration::from_millis(50),
        &offset_client,
        policy,
        start_offsets_reader,
        fast_backoff_cfg(),
        FetcherMetrics::new(Some(&reg)),
    )
    .await
    .unwrap();

    if start {
        f.start().await;
    }
    (f, reg)
}

async fn long_poll_fetches(
    fetchers: &ConcurrentFetchers,
    min_records: usize,
    timeout: Duration,
) -> Vec<FetchedRecord> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut out = Vec::new();
    while tokio::time::Instant::now() < deadline && out.len() < min_records {
        let rem = deadline.saturating_duration_since(tokio::time::Instant::now());
        if let Some(res) = fetchers.poll_fetches(rem.min(Duration::from_millis(200))).await {
            if res.err.is_none() {
                out.extend(res.records);
            }
        }
    }
    out
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestConcurrentFetchers
#[tokio::test]
async fn test_concurrent_fetchers() {
    const TOPIC: &str = "test-topic";
    const PID: i32 = 1;
    const CONCURRENCY: usize = 2;

    // 1. Respect context cancellation / poll timeout
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        let res = f.poll_fetches(Duration::from_millis(100)).await;
        assert!(res.is_none());
        assert_eq!(f.buffered_records(), 0);
        f.stop().await;
    }

    // 2. Cold replay
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        for i in 0..5 {
            cluster.produce_raw(TOPIC, PID, format!("record-{i}").as_bytes());
        }
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        let recs = long_poll_fetches(&f, 5, Duration::from_secs(3)).await;
        assert_eq!(recs.len(), 5);
        f.stop().await;
    }

    // 3. Fetch records produced after startup
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        for i in 0..3 {
            cluster.produce_raw(TOPIC, PID, format!("record-{i}").as_bytes());
        }
        let recs = long_poll_fetches(&f, 3, Duration::from_secs(3)).await;
        assert_eq!(recs.len(), 3);
        f.stop().await;
    }

    // 4. Slow processing & fast processing & different concurrency levels
    for conc in [1, 2, 4] {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            conc,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        for i in 0..20 {
            cluster.produce_raw(TOPIC, PID, format!("record-{i}").as_bytes());
        }
        let recs = long_poll_fetches(&f, 20, Duration::from_secs(5)).await;
        assert_eq!(recs.len(), 20);
        f.stop().await;
    }

    // 5. Start from mid-stream offset
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        for i in 0..5 {
            cluster.produce_raw(TOPIC, PID, format!("record-{i}").as_bytes());
        }
        let last_off = cluster.produce_raw(TOPIC, PID, b"last-initial-record");
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            last_off - 1,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        for i in 0..3 {
            cluster.produce_raw(TOPIC, PID, format!("new-record-{i}").as_bytes());
        }
        let recs = long_poll_fetches(&f, 5, Duration::from_secs(3)).await;
        let vals: Vec<String> = recs
            .iter()
            .map(|r| String::from_utf8(r.value.to_vec()).unwrap())
            .collect();
        assert_eq!(
            vals,
            vec![
                "record-4",
                "last-initial-record",
                "new-record-0",
                "new-record-1",
                "new-record-2"
            ]
        );
        f.stop().await;
    }

    // 6. Synchronous produce & fetch rounds + staggered production
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        for round in 0..3 {
            let mut expected = Vec::new();
            for i in 0..4 {
                let s = format!("round-{round}-record-{i}");
                cluster.produce_raw(TOPIC, PID, s.as_bytes());
                expected.push(s);
            }
            let recs = long_poll_fetches(&f, 4, Duration::from_secs(3)).await;
            let got: Vec<String> = recs
                .iter()
                .map(|r| String::from_utf8(r.value.to_vec()).unwrap())
                .collect();
            assert_eq!(got, expected);
        }
        f.stop().await;
    }

    // 7. Fetchers do not request offset beyond high watermark
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        for i in 0..8 {
            cluster.produce_raw(TOPIC, PID, format!("record-{}", i + 1).as_bytes());
        }
        let recs = long_poll_fetches(&f, 8, Duration::from_secs(3)).await;
        assert_eq!(recs.len(), 8);

        let fetch_count = Arc::new(AtomicI64::new(0));
        let max_req_off = Arc::new(AtomicI64::new(-1));
        let fc = Arc::clone(&fetch_count);
        let mro = Arc::clone(&max_req_off);
        cluster.set_fetch_hook(Some(Arc::new(move |req| {
            fc.fetch_add(1, Ordering::SeqCst);
            let off = req.topics[0].partitions[0].fetch_offset;
            mro.fetch_max(off, Ordering::SeqCst);
            None
        })));
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(max_req_off.load(Ordering::SeqCst) <= 8);
        f.stop().await;
    }

    // 8. Broken broker fails creating fetchers
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        cluster.set_metadata_hook(Some(Arc::new(|_| {
            let mut resp = MetadataResponse::default();
            let mut mt = MetadataResponseTopic::default();
            mt.name = Some(TopicName(StrBytes::from_static_str(TOPIC)));
            mt.error_code = ResponseError::BrokerNotAvailable.code();
            resp.topics.push(mt);
            Some(resp)
        })));

        let reg = Registry::new();
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        let oc = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, Some(&reg));
        let start_offsets_reader = Arc::new(GenericOffsetReader::new(
            Arc::new(|| Box::pin(async { Ok(0) })),
            Duration::from_secs(1),
        ));
        let err = ConcurrentFetchers::new(
            client,
            TOPIC,
            PID,
            0,
            CONCURRENCY,
            0,
            false,
            Duration::from_millis(50),
            &oc,
            RangeErrorPolicy::ResumeFromStart,
            start_offsets_reader,
            fast_backoff_cfg(),
            FetcherMetrics::new(Some(&reg)),
        )
        .await
        .err()
        .unwrap();
        assert!(err.contains("failed to find topic ID"), "err: {err}");
    }

    // 9. Reset buffered records count on stop
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        for i in 0..10 {
            cluster.produce_raw(TOPIC, PID, format!("record-{i}").as_bytes());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        f.stop().await;
        assert_eq!(f.buffered_records(), 0);
        assert_eq!(f.buffered_bytes(), 0);
    }

    // 10. Respect maximum buffered bytes limit
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let val = vec![b'a'; 5000];
        for _ in 0..200 {
            cluster.produce_raw(TOPIC, PID, &val);
        }
        let max_inflight = 4_000_000;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            3,
            max_inflight,
            true,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(f.buffered_bytes() <= max_inflight as i64);
        let recs = long_poll_fetches(&f, 200, Duration::from_secs(5)).await;
        assert_eq!(recs.len(), 200);
        f.stop().await;
    }

    // 11. Out of range error aborts under abort policy
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        for i in 0..5 {
            cluster.produce_raw(TOPIC, PID, format!("record-{i}").as_bytes());
        }
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            10_000_000,
            CONCURRENCY,
            0,
            true,
            RangeErrorPolicy::Abort,
        )
        .await;
        let res = f.poll_fetches(Duration::from_secs(3)).await.unwrap();
        assert_eq!(
            res.err,
            Some(KafkaFetchError::Broker(ResponseError::OffsetOutOfRange))
        );
        f.stop().await;
    }
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestConcurrentFetchers_fetchSingle
#[tokio::test]
async fn test_concurrent_fetchers_fetch_single() {
    const TOPIC: &str = "test-topic";
    const PID: i32 = 1;

    // 1. Honor start offset
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        cluster.produce_raw(TOPIC, PID, b"record-1");
        cluster.produce_raw(TOPIC, PID, b"record-2");
        cluster.produce_raw(TOPIC, PID, b"record-3");

        let (f, reg) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            1,
            0,
            false,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        let fw = FetchWant {
            start_offset: 1,
            end_offset: 5,
            estimated_bytes_per_record: 100,
            max_bytes_limit: 1_000_000,
        };
        let res = f.fetch_single(&fw).await;
        assert!(res.err.is_none());
        assert_eq!(res.records.len(), 2);
        assert_eq!(res.records[0].value.as_ref(), b"record-2");
        assert_eq!(res.records[1].value.as_ref(), b"record-3");

        let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
        assert!(text.contains("cortex_ingest_storage_reader_records_batch_fetch_max_bytes_count 1"));
    }

    // 2. Empty non-error response if canceled
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            1,
            0,
            false,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        let fw = FetchWant {
            start_offset: 1,
            end_offset: 5,
            estimated_bytes_per_record: 100,
            max_bytes_limit: 1_000_000,
        };
        let (tx, rx) = watch::channel(true);
        let res = f.fetch_single_cancellable(&fw, rx).await;
        drop(tx);
        assert!(res.err.is_none());
        assert!(res.records.is_empty());
    }

    // 3. Error response if Fetch contains partition error
    {
        let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
        let (f, _) = create_concurrent_fetchers(
            &cluster,
            TOPIC,
            PID,
            0,
            1,
            0,
            false,
            RangeErrorPolicy::ResumeFromStart,
        )
        .await;
        cluster.set_fetch_hook(Some(Arc::new(|req| {
            let mut resp = FetchResponse::default();
            let mut rt = FetchableTopicResponse::default();
            rt.topic = req.topics[0].topic.clone();
            rt.topic_id = req.topics[0].topic_id;
            let mut pd = PartitionData::default();
            pd.partition_index = req.topics[0].partitions[0].partition;
            pd.error_code = ResponseError::UnknownServerError.code();
            rt.partitions.push(pd);
            resp.responses.push(rt);
            Some(resp)
        })));
        let fw = FetchWant {
            start_offset: 1,
            end_offset: 5,
            estimated_bytes_per_record: 100,
            max_bytes_limit: 1_000_000,
        };
        let res = f.fetch_single(&fw).await;
        assert_eq!(
            res.err,
            Some(KafkaFetchError::Broker(ResponseError::UnknownServerError))
        );
        assert!(res.records.is_empty());
    }
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestConcurrentFetchers_parseFetchResponse
#[tokio::test]
async fn test_concurrent_fetchers_parse_fetch_response() {
    const TOPIC: &str = "test-topic";
    const PID: i32 = 1;
    let cluster = MockKafkaCluster::start(PID + 1, TOPIC).await;
    let (f, _) = create_concurrent_fetchers(
        &cluster,
        TOPIC,
        PID,
        0,
        1,
        0,
        false,
        RangeErrorPolicy::ResumeFromStart,
    )
    .await;

    let valid_tid = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        f.topic_id()[0], f.topic_id()[1], f.topic_id()[2], f.topic_id()[3],
        f.topic_id()[4], f.topic_id()[5], f.topic_id()[6], f.topic_id()[7],
        f.topic_id()[8], f.topic_id()[9], f.topic_id()[10], f.topic_id()[11],
        f.topic_id()[12], f.topic_id()[13], f.topic_id()[14], f.topic_id()[15],
    )
    .parse()
    .unwrap();
    let wrong_tid = "01010101-0101-0101-0101-010101010101".parse().unwrap();

    // 1. No topic
    let res = f.parse_fetch_response(0, &FetchResponse::default());
    assert!(res.err.is_some());

    // 2. Response-level error
    let mut r2 = FetchResponse::default();
    r2.error_code = ResponseError::UnknownServerError.code();
    let res = f.parse_fetch_response(0, &r2);
    assert!(res.err.unwrap().to_string().contains("received error"));

    // 3. More than 1 topic
    let mut r3 = FetchResponse::default();
    let mut t_a = FetchableTopicResponse::default();
    t_a.topic_id = valid_tid;
    let mut t_b = FetchableTopicResponse::default();
    t_b.topic_id = wrong_tid;
    r3.responses = vec![t_a, t_b];
    let res = f.parse_fetch_response(0, &r3);
    assert!(res.err.unwrap().to_string().contains("unexpected number of topics"));

    // 4. Unexpected topic ID
    let mut r4 = FetchResponse::default();
    let mut t4 = FetchableTopicResponse::default();
    t4.topic_id = wrong_tid;
    r4.responses = vec![t4];
    let res = f.parse_fetch_response(0, &r4);
    assert!(res.err.unwrap().to_string().contains("unexpected topic ID"));

    // 5. More than 1 partition
    let mut r5 = FetchResponse::default();
    let mut t5 = FetchableTopicResponse::default();
    t5.topic_id = valid_tid;
    let mut p0 = PartitionData::default();
    p0.partition_index = 0;
    let mut p1 = PartitionData::default();
    p1.partition_index = 1;
    t5.partitions = vec![p0, p1];
    r5.responses = vec![t5];
    let res = f.parse_fetch_response(0, &r5);
    assert!(res.err.unwrap().to_string().contains("unexpected number of partitions"));

    // 6. Wrong partition ID
    let mut r6 = FetchResponse::default();
    let mut t6 = FetchableTopicResponse::default();
    t6.topic_id = valid_tid;
    let mut p6 = PartitionData::default();
    p6.partition_index = 12345;
    t6.partitions = vec![p6];
    r6.responses = vec![t6];
    let res = f.parse_fetch_response(0, &r6);
    assert!(res.err.unwrap().to_string().contains("unexpected partition ID"));

    // 7. Partition error code
    let mut r7 = FetchResponse::default();
    let mut t7 = FetchableTopicResponse::default();
    t7.topic_id = valid_tid;
    let mut p7 = PartitionData::default();
    p7.partition_index = PID;
    p7.error_code = ResponseError::UnknownServerError.code();
    t7.partitions = vec![p7];
    r7.responses = vec![t7];
    let res = f.parse_fetch_response(0, &r7);
    assert_eq!(
        res.err,
        Some(KafkaFetchError::Broker(ResponseError::UnknownServerError))
    );
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestNewEmptyFetchResult
#[test]
fn test_new_empty_fetch_result() {
    let res = FetchResult::empty(1);
    assert_eq!(res.partition, 1);
    assert!(res.err.is_none());
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestNewErrorFetchResult
#[test]
fn test_new_error_fetch_result() {
    let err = KafkaFetchError::Other("test error".to_string());
    let res = FetchResult::error(1, Some(err.clone()));
    assert_eq!(res.partition, 1);
    assert_eq!(res.err, Some(err));

    let panicked = std::panic::catch_unwind(|| {
        let _ = FetchResult::error(1, None);
    })
    .is_err();
    assert!(panicked);
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestFetchResult_Merge
#[test]
fn test_fetch_result_merge() {
    let p1 = std::panic::catch_unwind(|| {
        let a = FetchResult::error(0, Some(KafkaFetchError::Other("test".to_string())));
        let b = FetchResult::empty(0);
        let _ = a.merge(b);
    })
    .is_err();
    assert!(p1);

    let p2 = std::panic::catch_unwind(|| {
        let a = FetchResult::empty(0);
        let b = FetchResult::error(0, Some(KafkaFetchError::Other("test".to_string())));
        let _ = a.merge(b);
    })
    .is_err();
    assert!(p2);
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestFetchWant_MaxBytes
#[test]
fn test_fetch_want_max_bytes() {
    let cases = [
        (
            "small fetch",
            FetchWant {
                start_offset: 0,
                end_offset: 10,
                estimated_bytes_per_record: 100,
                max_bytes_limit: i32::MAX,
            },
            1_000_000,
        ),
        (
            "medium fetch",
            FetchWant {
                start_offset: 0,
                end_offset: 1000,
                estimated_bytes_per_record: 1000,
                max_bytes_limit: i32::MAX,
            },
            1_050_000,
        ),
        (
            "huge fetch overflow risk",
            FetchWant {
                start_offset: 0,
                end_offset: 2 << 31,
                estimated_bytes_per_record: 2 << 30,
                max_bytes_limit: i32::MAX,
            },
            i32::MAX,
        ),
        (
            "max i64 overflow",
            FetchWant {
                start_offset: 0,
                end_offset: i64::MAX,
                estimated_bytes_per_record: i32::MAX as usize,
                max_bytes_limit: i32::MAX,
            },
            i32::MAX,
        ),
        (
            "capped by max_bytes_limit",
            FetchWant {
                start_offset: 0,
                end_offset: 1000,
                estimated_bytes_per_record: 10000,
                max_bytes_limit: 5_000_000,
            },
            5_000_000,
        ),
    ];

    for (name, fw, expected) in cases {
        let res = fw.max_bytes();
        assert_eq!(res, expected, "{name}");
        assert!(res >= 0);
    }
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestFetchWant_UpdateBytesPerRecord
#[test]
fn test_fetch_want_update_bytes_per_record() {
    let base = FetchWant {
        start_offset: 100,
        end_offset: 200,
        estimated_bytes_per_record: 1000,
        max_bytes_limit: i32::MAX,
    };

    let cases = [
        ("similar to estimate", 10_000, 10, 1000),
        ("much larger than estimate", 100_000, 10, 2800),
        ("much smaller than estimate", 1_000, 10, 820),
        (
            "risk of overflow",
            i64::MAX as usize,
            1,
            (i64::MAX as usize / 5) + 800,
        ),
    ];

    for (name, bytes, count, expected) in cases {
        let updated = base.update_bytes_per_record(bytes, count);
        assert_eq!(updated.start_offset, base.start_offset, "{name}");
        assert_eq!(updated.end_offset, base.end_offset, "{name}");
        let diff = (updated.estimated_bytes_per_record as f64 - expected as f64).abs();
        assert!(diff / (expected as f64) < 0.001, "{name}: got {}", updated.estimated_bytes_per_record);
        let mb = updated.max_bytes();
        assert!(mb >= 0);
    }

    assert_eq!(INITIAL_BYTES_PER_RECORD, 10_000);
}

// Upstream: pkg/storage/ingest/fetcher_test.go::TestFindGapsInRecords
#[test]
fn test_find_gaps_in_records() {
    let mk = |off: i64| FetchedRecord {
        offset: off,
        timestamp_ms: 0,
        key: Bytes::new(),
        value: Bytes::new(),
        headers: Vec::new(),
    };

    let cases: Vec<(&str, Vec<FetchedRecord>, i64, Vec<OffsetRange>)> = vec![
        ("no gaps", vec![mk(1), mk(2), mk(3)], 0, vec![]),
        ("single gap", vec![mk(5)], 2, vec![OffsetRange { start: 3, end: 5 }]),
        (
            "multiple gaps",
            vec![mk(3), mk(7), mk(10)],
            1,
            vec![
                OffsetRange { start: 2, end: 3 },
                OffsetRange { start: 4, end: 7 },
                OffsetRange { start: 8, end: 10 },
            ],
        ),
        ("empty records", vec![], 5, vec![]),
        (
            "gap at start",
            vec![mk(10), mk(11)],
            5,
            vec![OffsetRange { start: 6, end: 10 }],
        ),
        (
            "gap at start and middle",
            vec![mk(10), mk(11), mk(15), mk(16)],
            5,
            vec![
                OffsetRange { start: 6, end: 10 },
                OffsetRange { start: 12, end: 15 },
            ],
        ),
        ("negative gap at start ignored", vec![mk(5), mk(6)], 10, vec![]),
        ("-1 start offset ignored", vec![mk(5), mk(6)], -1, vec![]),
        (
            "-1 start offset ignored but rest kept",
            vec![mk(5), mk(6), mk(10)],
            -1,
            vec![OffsetRange { start: 7, end: 10 }],
        ),
    ];

    for (name, recs, last_off, expected) in cases {
        assert_eq!(find_gaps_in_records(&recs, last_off), expected, "{name}");
    }
}

// ===========================================================================
// pkg/storage/ingest/multi_cluster_reader_test.go (6 tests)
// ===========================================================================

struct MetricNameCaptureConsumer(Arc<SyncMutex<Vec<String>>>);

impl RecordBatchConsumer for MetricNameCaptureConsumer {
    fn consume(
        &self,
        records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            let mut scratch = WriteRequestScratch::new();
            let mut names = Vec::new();
            for rec in &records {
                let dec = decode_fetched_record(rec, &mut scratch).map_err(|e| e.to_string())?;
                for ts in dec.write_request.timeseries() {
                    for l in ts.labels() {
                        if l.name == b"__name__" {
                            names.push(String::from_utf8_lossy(l.value).into_owned());
                        }
                    }
                }
            }
            self.0.lock().extend(names);
            Ok(())
        })
    }
}

struct NoopConsumer;
impl RecordBatchConsumer for NoopConsumer {
    fn consume(
        &self,
        _records: Vec<FetchedRecord>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// Upstream: pkg/storage/ingest/multi_cluster_reader_test.go::TestMultiClusterPartitionReader_ConsumesFromAllWriteCompartments
#[tokio::test]
async fn test_multi_cluster_partition_reader_consumes_from_all_write_compartments() {
    const TOPIC: &str = "ingest-rc-0";
    const PID: i32 = 0;
    const TENANT: &str = "user-1";
    const NUM_WC: usize = 3;

    for ordered_cfg in [
        OrderedConsumptionConfig::default(),
        OrderedConsumptionConfig {
            enabled: true,
            max_batch_records: 1024,
            max_batch_wait: GoDuration::from_millis(10),
            input_buffer_size: 0,
        },
    ] {
        let mut clusters = Vec::new();
        let mut cfgs = Vec::new();
        let mut expected = Vec::new();
        for wc in 0..NUM_WC {
            let c = MockKafkaCluster::start(PID + 1, TOPIC).await;
            let cfg = make_test_kafka_cfg(&c.addr, TOPIC);
            let w = IngestWriter::from_config(&cfg, None).unwrap();
            let name = format!("series_wc_{wc}");
            w.write_sync(TOPIC, PID, TENANT, &mock_write_req(&name))
                .await
                .unwrap();
            expected.push(name);
            cfgs.push(cfg);
            clusters.push(c);
        }

        let captured = Arc::new(SyncMutex::new(Vec::new()));
        let consumer = Arc::new(MetricNameCaptureConsumer(Arc::clone(&captured)));
        let dir = tempfile::tempdir().unwrap();
        let offset_paths: Vec<_> = (0..NUM_WC)
            .map(|wc| Some(dir.path().join(format!("wc-{wc}.offset"))))
            .collect();

        let reader = MultiClusterPartitionReader::new(
            &cfgs,
            &ordered_cfg,
            PID,
            "ingester-0",
            offset_paths,
            consumer,
            None,
        )
        .unwrap();
        reader.start().await.unwrap();
        reader
            .wait_read_consistency_until_last_produced_offset(Duration::from_secs(5))
            .await
            .unwrap();

        let mut got = captured.lock().clone();
        got.sort();
        expected.sort();
        assert_eq!(got, expected);

        reader.stop().await;
    }
}

// Upstream: pkg/storage/ingest/multi_cluster_reader_test.go::TestMultiClusterPartitionReader_SingleClusterSkipsMerging
#[tokio::test]
async fn test_multi_cluster_partition_reader_single_cluster_skips_merging() {
    let c = MockKafkaCluster::start(1, "ingest-rc-0").await;
    let cfgs = vec![make_test_kafka_cfg(&c.addr, "ingest-rc-0")];
    let ordered_cfg = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 1024,
        max_batch_wait: GoDuration::from_millis(10),
        input_buffer_size: 0,
    };
    let reg = Registry::new();
    let dir = tempfile::tempdir().unwrap();
    let reader = MultiClusterPartitionReader::new(
        &cfgs,
        &ordered_cfg,
        0,
        "ingester-0",
        vec![Some(dir.path().join("wc-0.offset"))],
        Arc::new(NoopConsumer),
        Some(&reg),
    )
    .unwrap();
    // Since HeapMerger was skipped, ordered consumption metrics are not registered.
    let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
    assert!(!text.contains("cortex_ingest_storage_ordered_consumption_records_total"));
    reader.stop().await;
}

// Upstream: pkg/storage/ingest/multi_cluster_reader_test.go::TestMultiClusterPartitionReader_FailsToStartIfAnyClusterReaderFailsToStart
#[tokio::test]
async fn test_multi_cluster_partition_reader_fails_to_start_if_any_cluster_reader_fails_to_start() {
    let healthy = MockKafkaCluster::start(1, "ingest-rc-0").await;
    let healthy_cfg = make_test_kafka_cfg(&healthy.addr, "ingest-rc-0");

    // Write compartment 1 is misconfigured so that its reader fails to start: with file-based offset
    // enforcement enabled but no replay period and consumption from the last committed offset,
    // getStartOffset returns an error synchronously at startup.
    let mut broken_cfg = make_test_kafka_cfg(&healthy.addr, "ingest-rc-0");
    broken_cfg.consumer_group_offset_commit_file_enforced = true;
    broken_cfg.consume_from_position_at_startup = "last-offset".to_string();
    let dir = tempfile::tempdir().unwrap();

    let reader = MultiClusterPartitionReader::new(
        &[healthy_cfg, broken_cfg],
        &OrderedConsumptionConfig::default(),
        0,
        "ingester-0",
        vec![
            Some(dir.path().join("wc-0.offset")),
            Some(dir.path().join("wc-1.offset")),
        ],
        Arc::new(NoopConsumer),
        None,
    )
    .unwrap();

    assert!(reader.start().await.is_err());
    reader.stop().await;
}

// Upstream: pkg/storage/ingest/multi_cluster_reader_test.go::TestNewMultiClusterPartitionReader
#[test]
fn test_new_multi_cluster_partition_reader() {
    // Rejects empty cluster configs
    assert!(MultiClusterPartitionReader::new(
        &[],
        &OrderedConsumptionConfig::default(),
        0,
        "ingester-0",
        vec![],
        Arc::new(NoopConsumer),
        None,
    )
    .is_err());

    // Rejects mismatched offset_file_paths length
    let cfg = make_test_kafka_cfg("127.0.0.1:9092", "ingest-rc-0");
    assert!(MultiClusterPartitionReader::new(
        &[cfg],
        &OrderedConsumptionConfig::default(),
        0,
        "ingester-0",
        vec![],
        Arc::new(NoopConsumer),
        None,
    )
    .is_err());
}

// Upstream: pkg/storage/ingest/multi_cluster_reader_test.go::TestMultiClusterPartitionReader_WaitReadConsistencyUntilOffsets_RejectsKafkaClusterCountMismatch
#[tokio::test]
async fn test_multi_cluster_partition_reader_wait_read_consistency_until_offsets_rejects_mismatch() {
    let cfg = make_test_kafka_cfg("127.0.0.1:9092", "ingest-rc-0");
    let reader = MultiClusterPartitionReader::new(
        &[cfg.clone(), cfg],
        &OrderedConsumptionConfig::default(),
        0,
        "ingester-0",
        vec![None, None],
        Arc::new(NoopConsumer),
        None,
    )
    .unwrap();

    let err1 = reader
        .wait_read_consistency_until_offsets(
            &PartitionOffsets::new_single_cluster(10),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(err1.to_string().contains("expected 2 clusters, got 1"));

    let err2 = reader
        .wait_read_consistency_until_offsets(
            &PartitionOffsets::new_multi_cluster(vec![1, 2, 3]),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    assert!(err2.to_string().contains("expected 2 clusters, got 3"));
}

// Upstream: pkg/storage/ingest/multi_cluster_reader_test.go::TestMultiClusterPartitionReader_WaitReadConsistencyUntilOffsets_RoutesOffsetsPerCluster
#[tokio::test]
async fn test_multi_cluster_partition_reader_wait_read_consistency_until_offsets_routes_per_cluster() {
    const TOPIC: &str = "ingest-rc-0";
    const PID: i32 = 0;
    const TENANT: &str = "user-1";

    let c0 = MockKafkaCluster::start(PID + 1, TOPIC).await;
    let c1 = MockKafkaCluster::start(PID + 1, TOPIC).await;
    let cfg0 = make_test_kafka_cfg(&c0.addr, TOPIC);
    let cfg1 = make_test_kafka_cfg(&c1.addr, TOPIC);

    let w0 = IngestWriter::from_config(&cfg0, None).unwrap();
    let w1 = IngestWriter::from_config(&cfg1, None).unwrap();
    for i in 0..3 {
        w0.write_sync(TOPIC, PID, TENANT, &mock_write_req(&format!("c0_{i}")))
            .await
            .unwrap();
    }
    w1.write_sync(TOPIC, PID, TENANT, &mock_write_req("c1_0"))
        .await
        .unwrap();

    let reader = MultiClusterPartitionReader::new(
        &[cfg0, cfg1],
        &OrderedConsumptionConfig::default(),
        PID,
        "ingester-0",
        vec![None, None],
        Arc::new(NoopConsumer),
        None,
    )
    .unwrap();
    reader.start().await.unwrap();

    reader
        .wait_read_consistency_until_last_produced_offset(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        reader.last_seen_offsets(),
        PartitionOffsets::new_multi_cluster(vec![2, 0])
    );

    reader
        .wait_read_consistency_until_offsets(
            &PartitionOffsets::new_multi_cluster(vec![2, 0]),
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    // Swapped offsets must time out on cluster 1 (asked for 2 when only 0 exists).
    assert!(reader
        .wait_read_consistency_until_offsets(
            &PartitionOffsets::new_multi_cluster(vec![0, 2]),
            Duration::from_millis(150),
        )
        .await
        .is_err());

    reader.stop().await;
}

// ===========================================================================
// pkg/storage/ingest/partition_offset_client_test.go (4 tests)
// ===========================================================================

// Upstream: pkg/storage/ingest/partition_offset_client_test.go::TestPartitionOffsetClient_FetchPartitionLastProducedOffset
#[tokio::test]
async fn test_partition_offset_client_fetch_partition_last_produced_offset() {
    const TOPIC: &str = "test";
    const PID: i32 = 0;

    // 1. Returns last produced offset or -1 if empty + tracks metrics
    {
        let cluster = MockKafkaCluster::start(1, TOPIC).await;
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let reg = Registry::new();
        let oc = PartitionOffsetClient::from_config(&cfg, Some(&reg)).unwrap();

        assert_eq!(oc.fetch_partition_last_produced_offset(TOPIC, PID).await.unwrap(), -1);
        cluster.produce_raw(TOPIC, PID, b"message 1");
        assert_eq!(oc.fetch_partition_last_produced_offset(TOPIC, PID).await.unwrap(), 0);
        cluster.produce_raw(TOPIC, PID, b"message 2");
        assert_eq!(oc.fetch_partition_last_produced_offset(TOPIC, PID).await.unwrap(), 1);

        let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
        assert!(text.contains(r#"cortex_ingest_storage_reader_last_produced_offset_requests_total{partition="0",topic="test"} 3"#));
        assert!(text.contains(r#"cortex_ingest_storage_reader_last_produced_offset_failures_total{partition="0",topic="test"} 0"#));
    }

    // 2. Context deadline doesn't fail other in-flight requests
    {
        let cluster = MockKafkaCluster::start(1, TOPIC).await;
        cluster.produce_raw(TOPIC, PID, b"message 1");
        cluster.produce_raw(TOPIC, PID, b"message 2");

        let first_req = Arc::new(AtomicBool::new(true));
        let first_received = Arc::new(Notify::new());
        let fr = Arc::clone(&first_req);
        let recv = Arc::clone(&first_received);
        cluster.set_list_offsets_hook(Some(Arc::new(move |_| {
            let fr = Arc::clone(&fr);
            let recv = Arc::clone(&recv);
            Box::pin(async move {
                if fr.swap(false, Ordering::SeqCst) {
                    recv.notify_one();
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                None
            })
        })));

        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = Arc::new(PartitionOffsetClient::from_config(&cfg, None).unwrap());

        let oc1 = Arc::clone(&oc);
        let h1 = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                oc1.fetch_partition_last_produced_offset(TOPIC, PID),
            )
            .await
        });

        first_received.notified().await;
        let off = oc
            .fetch_partition_last_produced_offset(TOPIC, PID)
            .await
            .unwrap();
        assert_eq!(off, 1);
        assert!(h1.await.unwrap().is_err());
    }
}

// Upstream: pkg/storage/ingest/partition_offset_client_test.go::TestPartitionOffsetClient_FetchPartitionStartOffset
#[tokio::test]
async fn test_partition_offset_client_fetch_partition_start_offset() {
    const TOPIC: &str = "test";
    const PID: i32 = 0;

    // 1. Returns start offset including after DeleteRecords
    {
        let cluster = MockKafkaCluster::start(1, TOPIC).await;
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let reg = Registry::new();
        let oc = PartitionOffsetClient::from_config(&cfg, Some(&reg)).unwrap();

        assert_eq!(oc.fetch_partition_start_offset(TOPIC, PID).await.unwrap(), 0);
        cluster.produce_raw(TOPIC, PID, b"record 1");
        assert_eq!(oc.fetch_partition_start_offset(TOPIC, PID).await.unwrap(), 0);
        cluster.produce_raw(TOPIC, PID, b"record 2");
        assert_eq!(oc.fetch_partition_start_offset(TOPIC, PID).await.unwrap(), 0);

        cluster.delete_records_before(TOPIC, PID, 1);
        assert_eq!(oc.fetch_partition_start_offset(TOPIC, PID).await.unwrap(), 1);

        let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
        assert!(text.contains(r#"cortex_ingest_storage_reader_partition_start_offset_requests_total{partition="0",topic="test"} 4"#));
        assert!(text.contains(r#"cortex_ingest_storage_reader_partition_start_offset_failures_total{partition="0",topic="test"} 0"#));
    }

    // 2. Context deadline doesn't fail other in-flight requests
    {
        let cluster = MockKafkaCluster::start(1, TOPIC).await;
        cluster.produce_raw(TOPIC, PID, b"r1");
        cluster.produce_raw(TOPIC, PID, b"r2");
        cluster.delete_records_before(TOPIC, PID, 1);

        let first_req = Arc::new(AtomicBool::new(true));
        let first_received = Arc::new(Notify::new());
        let fr = Arc::clone(&first_req);
        let recv = Arc::clone(&first_received);
        cluster.set_list_offsets_hook(Some(Arc::new(move |_| {
            let fr = Arc::clone(&fr);
            let recv = Arc::clone(&recv);
            Box::pin(async move {
                if fr.swap(false, Ordering::SeqCst) {
                    recv.notify_one();
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                None
            })
        })));

        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = Arc::new(PartitionOffsetClient::from_config(&cfg, None).unwrap());

        let oc1 = Arc::clone(&oc);
        let h1 = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                oc1.fetch_partition_start_offset(TOPIC, PID),
            )
            .await
        });

        first_received.notified().await;
        assert_eq!(oc.fetch_partition_start_offset(TOPIC, PID).await.unwrap(), 1);
        assert!(h1.await.unwrap().is_err());
    }
}

// Upstream: pkg/storage/ingest/partition_offset_client_test.go::TestPartitionOffsetClient_FetchPartitionsLastProducedOffsets
#[tokio::test]
async fn test_partition_offset_client_fetch_partitions_last_produced_offsets() {
    const TOPIC: &str = "test";
    let all_pids = vec![0, 1, 2];

    // 1. Returns last produced offsets or -1 if empty
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let reg = Registry::new();
        let oc = PartitionOffsetClient::from_config(&cfg, Some(&reg)).unwrap();

        let res = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                all_pids.clone(),
            )]))
            .await
            .unwrap();
        assert_eq!(res[TOPIC], HashMap::from([(0, -1), (1, -1), (2, -1)]));

        cluster.produce_raw(TOPIC, 0, b"m1");
        cluster.produce_raw(TOPIC, 0, b"m2");
        cluster.produce_raw(TOPIC, 1, b"m3");
        let res = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                all_pids.clone(),
            )]))
            .await
            .unwrap();
        assert_eq!(res[TOPIC], HashMap::from([(0, 1), (1, 0), (2, -1)]));

        cluster.produce_raw(TOPIC, 0, b"m4");
        cluster.produce_raw(TOPIC, 1, b"m5");
        cluster.produce_raw(TOPIC, 2, b"m6");
        let res = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                all_pids.clone(),
            )]))
            .await
            .unwrap();
        assert_eq!(res[TOPIC], HashMap::from([(0, 2), (1, 1), (2, 0)]));

        let res = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                vec![0, 2],
            )]))
            .await
            .unwrap();
        assert_eq!(res[TOPIC], HashMap::from([(0, 2), (2, 0)]));

        let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
        assert!(text.contains(r#"cortex_ingest_storage_reader_last_produced_offset_requests_total{partition="mixed",topic="test"} 4"#));
    }

    // 2. Does not track metric if no partition requested
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let reg = Registry::new();
        let oc = PartitionOffsetClient::from_config(&cfg, Some(&reg)).unwrap();
        let res = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                vec![],
            )]))
            .await
            .unwrap();
        assert!(res.is_empty());
        let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
        assert!(!text.contains("last_produced_offset_requests_total"));
    }

    // 3. Context deadline does not fail other in-flight requests
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        cluster.produce_raw(TOPIC, 0, b"m1");
        cluster.produce_raw(TOPIC, 1, b"m2");

        let first_req = Arc::new(AtomicBool::new(true));
        let first_received = Arc::new(Notify::new());
        let fr = Arc::clone(&first_req);
        let recv = Arc::clone(&first_received);
        cluster.set_list_offsets_hook(Some(Arc::new(move |_| {
            let fr = Arc::clone(&fr);
            let recv = Arc::clone(&recv);
            Box::pin(async move {
                if fr.swap(false, Ordering::SeqCst) {
                    recv.notify_one();
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                None
            })
        })));

        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = Arc::new(PartitionOffsetClient::from_config(&cfg, None).unwrap());
        let oc1 = Arc::clone(&oc);
        let req_map = HashMap::from([(TOPIC.to_string(), all_pids.clone())]);
        let req_map1 = req_map.clone();
        let h1 = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                oc1.fetch_partitions_last_produced_offsets(&req_map1),
            )
            .await
        });
        first_received.notified().await;
        let res = oc
            .fetch_partitions_last_produced_offsets(&req_map)
            .await
            .unwrap();
        assert_eq!(res[TOPIC], HashMap::from([(0, 0), (1, 0), (2, -1)]));
        assert!(h1.await.unwrap().is_err());
    }

    // 4. Unexpected topic in response (alongside expected)
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        cluster.set_list_offsets_hook(Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = ListOffsetsResponse::default();
                let mut t1 = ListOffsetsTopicResponse::default();
                t1.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut t2 = ListOffsetsTopicResponse::default();
                t2.name = TopicName(StrBytes::from_static_str("another-unknown-topic"));
                resp.topics = vec![t1, t2];
                Some(resp)
            })
        })));
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = PartitionOffsetClient::from_config(&cfg, None).unwrap();
        let err = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                all_pids.clone(),
            )]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unexpected topic in the response"));
    }

    // 5. Response contains 1 topic but not expected
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        cluster.set_list_offsets_hook(Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = ListOffsetsResponse::default();
                let mut t1 = ListOffsetsTopicResponse::default();
                t1.name = TopicName(StrBytes::from_static_str("another-unknown-topic"));
                resp.topics = vec![t1];
                Some(resp)
            })
        })));
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = PartitionOffsetClient::from_config(&cfg, None).unwrap();
        let err = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                all_pids.clone(),
            )]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unexpected topic in the response"));
    }

    // 6. Partition error in response
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        cluster.set_list_offsets_hook(Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = ListOffsetsResponse::default();
                let mut t1 = ListOffsetsTopicResponse::default();
                t1.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut p0 = ListOffsetsPartitionResponse::default();
                p0.partition_index = 0;
                p0.offset = 1;
                let mut p1 = ListOffsetsPartitionResponse::default();
                p1.partition_index = 0;
                p1.error_code = ResponseError::NotLeaderOrFollower.code();
                t1.partitions = vec![p0, p1];
                resp.topics = vec![t1];
                Some(resp)
            })
        })));
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = PartitionOffsetClient::from_config(&cfg, None).unwrap();
        let err = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                all_pids.clone(),
            )]))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            KafkaClientError::Broker(ResponseError::NotLeaderOrFollower)
        ));
    }

    // 7. Fetches offsets of multiple topics in a SINGLE request
    {
        const T0: &str = "topic-0";
        const T1: &str = "topic-1";
        let cluster = MockKafkaCluster::start(2, T0).await;
        cluster.create_topic(T1, 2);
        cluster.produce_raw(T0, 0, b"a");
        cluster.produce_raw(T0, 0, b"b");
        cluster.produce_raw(T0, 1, b"c");
        cluster.produce_raw(T1, 0, b"d");

        let count = Arc::new(AtomicI64::new(0));
        let cc = Arc::clone(&count);
        cluster.set_list_offsets_hook(Some(Arc::new(move |_| {
            cc.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { None })
        })));

        let reg = Registry::new();
        let cfg = make_test_kafka_cfg(&cluster.addr, T0);
        let oc = PartitionOffsetClient::from_config(&cfg, Some(&reg)).unwrap();
        let res = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([
                (T0.to_string(), vec![0, 1]),
                (T1.to_string(), vec![0, 1]),
            ]))
            .await
            .unwrap();
        assert_eq!(res[T0], HashMap::from([(0, 1), (1, 0)]));
        assert_eq!(res[T1], HashMap::from([(0, 0), (1, -1)]));
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let text = TextEncoder::new().encode_to_string(&reg.gather()).unwrap();
        assert!(text.contains(r#"cortex_ingest_storage_reader_last_produced_offset_requests_total{partition="mixed",topic="mixed"} 1"#));
    }

    // 8. Missing requested topic-partition in response
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        cluster.set_list_offsets_hook(Some(Arc::new(|_| {
            Box::pin(async {
                let mut resp = ListOffsetsResponse::default();
                let mut t1 = ListOffsetsTopicResponse::default();
                t1.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut p0 = ListOffsetsPartitionResponse::default();
                p0.partition_index = 0;
                p0.offset = 1;
                t1.partitions = vec![p0];
                resp.topics = vec![t1];
                Some(resp)
            })
        })));
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = PartitionOffsetClient::from_config(&cfg, None).unwrap();
        let err = oc
            .fetch_partitions_last_produced_offsets(&HashMap::from([(
                TOPIC.to_string(),
                all_pids,
            )]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing in the response"));
    }
}

// Upstream: pkg/storage/ingest/partition_offset_client_test.go::TestPartitionOffsetClient_ListTopicPartitionIDs
#[tokio::test]
async fn test_partition_offset_client_list_topic_partition_ids() {
    const TOPIC: &str = "test";

    // 1. Returns sorted partition IDs
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = PartitionOffsetClient::from_config(&cfg, None).unwrap();
        assert_eq!(oc.list_topic_partition_ids(TOPIC).await.unwrap(), vec![0, 1, 2]);
    }

    // 2. Returns error if response contains topic error
    {
        let cluster = MockKafkaCluster::start(3, TOPIC).await;
        cluster.set_metadata_hook(Some(Arc::new(|_| {
            let mut resp = MetadataResponse::default();
            let mut mt = MetadataResponseTopic::default();
            mt.name = Some(TopicName(StrBytes::from_static_str(TOPIC)));
            mt.error_code = ResponseError::UnknownServerError.code();
            resp.topics.push(mt);
            Some(resp)
        })));
        let cfg = make_test_kafka_cfg(&cluster.addr, TOPIC);
        let oc = PartitionOffsetClient::from_config(&cfg, None).unwrap();
        let err = oc.list_topic_partition_ids(TOPIC).await.unwrap_err();
        assert!(matches!(
            err,
            KafkaClientError::Broker(ResponseError::UnknownServerError)
        ));
    }
}

// ===========================================================================
// pkg/storage/ingest/partition_offset_reader_multi_cluster_test.go (4 tests)
// ===========================================================================

fn partition_zero_fn() -> GetPartitionIDsFn {
    Arc::new(|| Box::pin(async { Ok(vec![0]) }))
}

// Upstream: pkg/storage/ingest/partition_offset_reader_multi_cluster_test.go::TestMultiClusterOffsetsReader_WaitNextFetchLastProducedOffset
#[tokio::test]
async fn test_multi_cluster_offsets_reader_wait_next_fetch_last_produced_offset() {
    const T0: &str = "rc-0";
    const T1: &str = "rc-1";

    let c0 = MockKafkaCluster::start(1, T0).await;
    c0.create_topic(T1, 1);
    let c1 = MockKafkaCluster::start(1, T0).await;
    c1.create_topic(T1, 1);

    c0.produce_raw(T0, 0, b"a");
    c0.produce_raw(T0, 0, b"b");
    c0.produce_raw(T0, 0, b"c");
    c0.produce_raw(T1, 0, b"d");
    c1.produce_raw(T1, 0, b"e");
    c1.produce_raw(T1, 0, b"f");

    let cfg0 = make_test_kafka_cfg(&c0.addr, T0);
    let cfg1 = make_test_kafka_cfg(&c1.addr, T0);
    let oc0 = PartitionOffsetClient::from_config(&cfg0, None).unwrap();
    let oc1 = PartitionOffsetClient::from_config(&cfg1, None).unwrap();

    let reader = MultiClusterOffsetsReader::with_partition_ids(
        vec![oc0, oc1],
        vec![T0.to_string(), T1.to_string()],
        vec![partition_zero_fn(), partition_zero_fn()],
        Duration::from_millis(50),
        true,
    )
    .unwrap();
    reader.start().await;

    let offsets = reader.wait_next_fetch_last_produced_offset().await.unwrap();
    assert_eq!(
        offsets,
        HashMap::from([
            (
                T0.to_string(),
                HashMap::from([(0, PartitionOffsets::new_multi_cluster(vec![2, -1]))]),
            ),
            (
                T1.to_string(),
                HashMap::from([(0, PartitionOffsets::new_multi_cluster(vec![0, 1]))]),
            ),
        ])
    );
    assert_eq!(reader.clients().len(), 2);
    reader.stop().await;
}

// Upstream: pkg/storage/ingest/partition_offset_reader_multi_cluster_test.go::TestMultiClusterOffsetsReader_SingleFlight
#[tokio::test]
async fn test_multi_cluster_offsets_reader_single_flight() {
    const TOPIC: &str = "rc-0";
    const NUM_WAITERS: usize = 20;

    let c0 = MockKafkaCluster::start(1, TOPIC).await;
    let c1 = MockKafkaCluster::start(1, TOPIC).await;
    c0.produce_raw(TOPIC, 0, b"a");
    c1.produce_raw(TOPIC, 0, b"b");
    c1.produce_raw(TOPIC, 0, b"c");

    let list0 = Arc::new(AtomicI64::new(0));
    let list1 = Arc::new(AtomicI64::new(0));
    let first0 = Arc::new(Notify::new());
    let first1 = Arc::new(Notify::new());

    {
        let cnt = Arc::clone(&list0);
        let n = Arc::clone(&first0);
        c0.set_list_offsets_hook(Some(Arc::new(move |_| {
            let cnt = Arc::clone(&cnt);
            let n = Arc::clone(&n);
            Box::pin(async move {
                if cnt.fetch_add(1, Ordering::SeqCst) == 0 {
                    n.notify_one();
                }
                None
            })
        })));
    }
    {
        let cnt = Arc::clone(&list1);
        let n = Arc::clone(&first1);
        c1.set_list_offsets_hook(Some(Arc::new(move |_| {
            let cnt = Arc::clone(&cnt);
            let n = Arc::clone(&n);
            Box::pin(async move {
                if cnt.fetch_add(1, Ordering::SeqCst) == 0 {
                    n.notify_one();
                }
                None
            })
        })));
    }

    let cfg0 = make_test_kafka_cfg(&c0.addr, TOPIC);
    let cfg1 = make_test_kafka_cfg(&c1.addr, TOPIC);
    let oc0 = PartitionOffsetClient::from_config(&cfg0, None).unwrap();
    let oc1 = PartitionOffsetClient::from_config(&cfg1, None).unwrap();

    let reader = Arc::new(
        MultiClusterOffsetsReader::with_partition_ids(
            vec![oc0, oc1],
            vec![TOPIC.to_string()],
            vec![partition_zero_fn()],
            Duration::from_millis(200),
            true,
        )
        .unwrap(),
    );
    reader.start().await;

    let mut handles = Vec::with_capacity(NUM_WAITERS);
    for _ in 0..NUM_WAITERS {
        let r = Arc::clone(&reader);
        handles.push(tokio::spawn(async move {
            r.wait_next_fetch_last_produced_offset().await
        }));
    }

    let expected = HashMap::from([(
        TOPIC.to_string(),
        HashMap::from([(0, PartitionOffsets::new_multi_cluster(vec![0, 1]))]),
    )]);
    for h in handles {
        assert_eq!(h.await.unwrap().unwrap(), expected);
    }

    assert!(list0.load(Ordering::SeqCst) <= 3);
    assert!(list1.load(Ordering::SeqCst) <= 3);
    reader.stop().await;
}

// Upstream: pkg/storage/ingest/partition_offset_reader_multi_cluster_test.go::TestMultiClusterOffsetsReader_ClosesClientsOnStop
#[tokio::test]
async fn test_multi_cluster_offsets_reader_closes_clients_on_stop() {
    let c0 = MockKafkaCluster::start(1, "rc-0").await;
    let c1 = MockKafkaCluster::start(1, "rc-0").await;
    let oc0 = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&c0.addr, "rc-0"), None).unwrap();
    let oc1 = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&c1.addr, "rc-0"), None).unwrap();

    let reader = MultiClusterOffsetsReader::with_partition_ids(
        vec![oc0, oc1],
        vec!["rc-0".to_string()],
        vec![partition_zero_fn()],
        Duration::from_millis(100),
        true,
    )
    .unwrap();
    reader.start().await;
    assert_eq!(reader.clients().len(), 2);
    reader.stop().await;

    for client in reader.clients() {
        assert!(client.is_closed());
        assert!(client.list_topic_partition_ids("rc-0").await.is_err());
    }
}

// Upstream: pkg/storage/ingest/partition_offset_reader_multi_cluster_test.go::TestNewMultiClusterOffsetsReader_Validation
#[test]
fn test_new_multi_cluster_offsets_reader_validation() {
    assert!(MultiClusterOffsetsReader::with_partition_ids(
        vec![],
        vec!["rc-0".to_string()],
        vec![partition_zero_fn()],
        Duration::from_secs(1),
        false,
    )
    .is_err());

    let oc = PartitionOffsetClient::from_config(&make_test_kafka_cfg("127.0.0.1:0", "rc-0"), None).unwrap();
    assert!(MultiClusterOffsetsReader::with_partition_ids(
        vec![oc],
        vec!["rc-0".to_string(), "rc-1".to_string()],
        vec![partition_zero_fn()],
        Duration::from_secs(1),
        false,
    )
    .is_err());
}

// ===========================================================================
// pkg/storage/ingest/partition_offset_reader_single_cluster_test.go (2 tests)
// ===========================================================================

// Upstream: pkg/storage/ingest/partition_offset_reader_single_cluster_test.go::TestSingleClusterTopicOffsetsReader
#[tokio::test]
async fn test_single_cluster_topic_offsets_reader() {
    let cluster = MockKafkaCluster::start(1, "test").await;
    let oc = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&cluster.addr, "test"), None).unwrap();
    let reader = Arc::new(SingleClusterTopicOffsetsReader::with_partition_ids(
        oc,
        "test",
        partition_zero_fn(),
        Duration::from_secs(3600),
    ));
    reader.start().await;

    let r1 = Arc::clone(&reader);
    let h1 = tokio::spawn(async move { r1.wait_next_fetch_last_produced_offset().await });
    let r2 = Arc::clone(&reader);
    let h2 = tokio::spawn(async move { r2.wait_next_fetch_last_produced_offset().await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    reader.stop().await;

    assert_eq!(h1.await.unwrap().unwrap_err(), "partition offset reader is stopped");
    assert_eq!(h2.await.unwrap().unwrap_err(), "partition offset reader is stopped");
    assert_eq!(
        reader.wait_next_fetch_last_produced_offset().await.unwrap_err(),
        "partition offset reader is stopped"
    );
}

// Upstream: pkg/storage/ingest/partition_offset_reader_single_cluster_test.go::TestSingleClusterTopicOffsetsReader_WaitNextFetchLastProducedOffset
#[tokio::test]
async fn test_single_cluster_topic_offsets_reader_wait_next_fetch_last_produced_offset() {
    const TOPIC: &str = "test";
    let cluster = MockKafkaCluster::start(2, TOPIC).await;

    let last_off = Arc::new(AtomicI64::new(1));
    let first_recv = Arc::new(Notify::new());
    let second_recv = Arc::new(Notify::new());
    let first_gate = Arc::new(Notify::new());
    let second_gate = Arc::new(Notify::new());

    {
        let lo = Arc::clone(&last_off);
        let r1 = Arc::clone(&first_recv);
        let r2 = Arc::clone(&second_recv);
        let g1 = Arc::clone(&first_gate);
        let g2 = Arc::clone(&second_gate);
        cluster.set_list_offsets_hook(Some(Arc::new(move |_| {
            let lo = Arc::clone(&lo);
            let r1 = Arc::clone(&r1);
            let r2 = Arc::clone(&r2);
            let g1 = Arc::clone(&g1);
            let g2 = Arc::clone(&g2);
            Box::pin(async move {
                let cur = lo.load(Ordering::SeqCst);
                if cur == 1 {
                    r1.notify_one();
                    g1.notified().await;
                } else if cur == 3 {
                    r2.notify_one();
                    g2.notified().await;
                }
                let v0 = lo.fetch_add(1, Ordering::SeqCst) + 1;
                let v1 = lo.fetch_add(1, Ordering::SeqCst) + 1;
                let mut resp = ListOffsetsResponse::default();
                let mut rt = ListOffsetsTopicResponse::default();
                rt.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut p0 = ListOffsetsPartitionResponse::default();
                p0.partition_index = 0;
                p0.offset = v0;
                let mut p1 = ListOffsetsPartitionResponse::default();
                p1.partition_index = 1;
                p1.offset = v1;
                rt.partitions = vec![p0, p1];
                resp.topics = vec![rt];
                Some(resp)
            })
        })));
    }

    let oc = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&cluster.addr, TOPIC), None).unwrap();
    let reader = Arc::new(SingleClusterTopicOffsetsReader::with_partition_ids(
        oc,
        TOPIC,
        Arc::new(|| Box::pin(async { Ok(vec![0, 1]) })),
        Duration::from_millis(20),
    ));

    let r_start = Arc::clone(&reader);
    let start_h = tokio::spawn(async move { r_start.start().await });

    first_recv.notified().await;
    let r1 = Arc::clone(&reader);
    let w1 = tokio::spawn(async move { r1.wait_next_fetch_last_produced_offset().await });
    tokio::task::yield_now().await;
    first_gate.notify_one();
    start_h.await.unwrap();

    second_recv.notified().await;
    let r2 = Arc::clone(&reader);
    let w2 = tokio::spawn(async move { r2.wait_next_fetch_last_produced_offset().await });
    tokio::task::yield_now().await;
    second_gate.notify_one();

    assert_eq!(w1.await.unwrap().unwrap(), HashMap::from([(0, 3), (1, 4)]));
    assert_eq!(w2.await.unwrap().unwrap(), HashMap::from([(0, 5), (1, 6)]));
    reader.stop().await;

    // Canceled / timed-out wait returns immediately
    let c2 = MockKafkaCluster::start(1, TOPIC).await;
    let oc2 = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&c2.addr, TOPIC), None).unwrap();
    let r_slow = SingleClusterTopicOffsetsReader::with_partition_ids(
        oc2,
        TOPIC,
        partition_zero_fn(),
        Duration::from_secs(3600),
    );
    r_slow.start().await;
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        r_slow.wait_next_fetch_last_produced_offset()
    )
    .await
    .is_err());
    r_slow.stop().await;
}

// ===========================================================================
// pkg/storage/ingest/partition_offset_reader_test.go (3 tests)
// ===========================================================================

// Upstream: pkg/storage/ingest/partition_offset_reader_test.go::TestPartitionOffsetReader
#[tokio::test]
async fn test_partition_offset_reader() {
    let cluster = MockKafkaCluster::start(1, "test").await;
    let oc = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&cluster.addr, "test"), None).unwrap();
    let reader = Arc::new(SingleClusterPartitionOffsetReader::new(
        oc,
        "test",
        0,
        Duration::from_secs(3600),
    ));
    reader.start().await;

    let r1 = Arc::clone(&reader);
    let h1 = tokio::spawn(async move { r1.wait_next_fetch_last_produced_offset().await });
    let r2 = Arc::clone(&reader);
    let h2 = tokio::spawn(async move { r2.wait_next_fetch_last_produced_offset().await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    reader.stop().await;

    assert_eq!(h1.await.unwrap().unwrap_err(), "partition offset reader is stopped");
    assert_eq!(h2.await.unwrap().unwrap_err(), "partition offset reader is stopped");
    assert_eq!(
        reader.wait_next_fetch_last_produced_offset().await.unwrap_err(),
        "partition offset reader is stopped"
    );
}

// Upstream: pkg/storage/ingest/partition_offset_reader_test.go::TestPartitionOffsetReader_WaitNextFetchLastProducedOffset
#[tokio::test]
async fn test_partition_offset_reader_wait_next_fetch_last_produced_offset() {
    const TOPIC: &str = "test";
    let cluster = MockKafkaCluster::start(1, TOPIC).await;

    let last_off = Arc::new(AtomicI64::new(1));
    let first_recv = Arc::new(Notify::new());
    let second_recv = Arc::new(Notify::new());
    let first_gate = Arc::new(Notify::new());
    let second_gate = Arc::new(Notify::new());

    {
        let lo = Arc::clone(&last_off);
        let r1 = Arc::clone(&first_recv);
        let r2 = Arc::clone(&second_recv);
        let g1 = Arc::clone(&first_gate);
        let g2 = Arc::clone(&second_gate);
        cluster.set_list_offsets_hook(Some(Arc::new(move |_| {
            let lo = Arc::clone(&lo);
            let r1 = Arc::clone(&r1);
            let r2 = Arc::clone(&r2);
            let g1 = Arc::clone(&g1);
            let g2 = Arc::clone(&g2);
            Box::pin(async move {
                let cur = lo.load(Ordering::SeqCst);
                if cur == 1 {
                    r1.notify_one();
                    g1.notified().await;
                } else if cur == 2 {
                    r2.notify_one();
                    g2.notified().await;
                }
                let val = lo.fetch_add(1, Ordering::SeqCst) + 1;
                let mut resp = ListOffsetsResponse::default();
                let mut rt = ListOffsetsTopicResponse::default();
                rt.name = TopicName(StrBytes::from_static_str(TOPIC));
                let mut p0 = ListOffsetsPartitionResponse::default();
                p0.partition_index = 0;
                p0.offset = val;
                rt.partitions = vec![p0];
                resp.topics = vec![rt];
                Some(resp)
            })
        })));
    }

    let oc = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&cluster.addr, TOPIC), None).unwrap();
    let reader = Arc::new(SingleClusterPartitionOffsetReader::new(
        oc,
        TOPIC,
        0,
        Duration::from_millis(20),
    ));

    let r_start = Arc::clone(&reader);
    let start_h = tokio::spawn(async move { r_start.start().await });

    first_recv.notified().await;
    let r1 = Arc::clone(&reader);
    let w1 = tokio::spawn(async move { r1.wait_next_fetch_last_produced_offset().await });
    tokio::task::yield_now().await;
    first_gate.notify_one();
    start_h.await.unwrap();

    second_recv.notified().await;
    let r2 = Arc::clone(&reader);
    let w2 = tokio::spawn(async move { r2.wait_next_fetch_last_produced_offset().await });
    tokio::task::yield_now().await;
    second_gate.notify_one();

    // ListOffsets returns next write offset, so last produced offset is (2 - 1 = 1) -> wait:
    // Go test mocks ListOffsetsResponse returning Offset: lastOffset.Inc() where lastOffset starts at 1,
    // and FetchPartitionLastProducedOffset returns offset - 1, wait: in Go test:
    // `lastOffset = atomic.NewInt64(1)` -> `lastOffset.Inc()` returns 2, FetchPartitionLastProducedOffset subtracts 1 => wait in Go test:
    // Go test expects `2` and `3` because `atomic.NewInt64(1).Inc()` returns 2, wait no: FetchPartitionLastProducedOffset returns `offset - 1`, wait why did Go test assert `2` and `3`?
    // Look at `partition_offset_reader_test.go` lines 97-118: `lastOffset.Inc()` returns 2 for req 1? No, `2 - 1 = 1`, req 2 returns 3 (`3 - 1 = 2`), req 3 returns 4 (`4 - 1 = 3`)!
    // Wait: why did our hook check `cur == 1` and `cur == 2` and return `lo.fetch_add(1) + 1`?
    // For req 1: `lo` goes 1 -> 2, returns 2 -> `FetchPartitionLastProducedOffset` returns 2 - 1 = 1.
    // For req 2: `lo` goes 2 -> 3, returns 3 -> `FetchPartitionLastProducedOffset` returns 3 - 1 = 2.
    // For req 3: `lo` goes 3 -> 4, returns 4 -> `FetchPartitionLastProducedOffset` returns 4 - 1 = 3.
    assert_eq!(w1.await.unwrap().unwrap(), 2);
    assert_eq!(w2.await.unwrap().unwrap(), 3);
    reader.stop().await;

    // Never-started reader times out when waited on
    let oc2 = PartitionOffsetClient::from_config(&make_test_kafka_cfg(&cluster.addr, TOPIC), None).unwrap();
    let unstarted = SingleClusterPartitionOffsetReader::new(oc2, TOPIC, 0, Duration::from_secs(1));
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        unstarted.wait_next_fetch_last_produced_offset()
    )
    .await
    .is_err());
}

// Upstream: pkg/storage/ingest/partition_offset_reader_test.go::TestGenericPartitionReader_Caching
#[tokio::test]
async fn test_generic_partition_reader_caching() {
    // 1. Initializes with fetched offset
    {
        let r = GenericOffsetReader::new(
            Arc::new(|| Box::pin(async { Ok(42i64) })),
            Duration::from_secs(1),
        );
        r.start().await;
        assert_eq!(r.cached_offset(), Some(Ok(42)));
        r.stop().await;
    }

    // 2. Caches error from initial fetch
    {
        let r: GenericOffsetReader<i64> = GenericOffsetReader::new(
            Arc::new(|| Box::pin(async { Err("fetch error".to_string()) })),
            Duration::from_secs(1),
        );
        r.start().await;
        assert_eq!(r.cached_offset(), Some(Err("fetch error".to_string())));
        r.stop().await;
    }

    // 3. Updates cache on poll interval
    {
        let count = Arc::new(AtomicI64::new(0));
        let ch = Arc::new(Notify::new());
        let cc = Arc::clone(&count);
        let nc = Arc::clone(&ch);
        let r = GenericOffsetReader::new(
            Arc::new(move || {
                let cc = Arc::clone(&cc);
                let nc = Arc::clone(&nc);
                Box::pin(async move {
                    let v = cc.fetch_add(1, Ordering::SeqCst) + 1;
                    nc.notify_one();
                    Ok(v)
                })
            }),
            Duration::from_millis(10),
        );
        r.start().await;
        while count.load(Ordering::SeqCst) < 3 {
            ch.notified().await;
        }
        let cached = r.cached_offset().unwrap().unwrap();
        assert!(cached >= 2, "cached={cached}");
        r.stop().await;
    }

    // 4. Handles concurrent access
    {
        let r = Arc::new(GenericOffsetReader::new(
            Arc::new(|| Box::pin(async { Ok(42i64) })),
            Duration::from_secs(1),
        ));
        r.start().await;
        let mut hs = Vec::new();
        for _ in 0..100 {
            let r = Arc::clone(&r);
            hs.push(tokio::spawn(async move {
                assert_eq!(r.cached_offset(), Some(Ok(42)));
            }));
        }
        for h in hs {
            h.await.unwrap();
        }
        r.stop().await;
    }
}
