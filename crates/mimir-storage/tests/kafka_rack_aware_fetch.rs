//! KAFKA-1: rack-aware brokers (KIP-392 follower fetching) against both reader fetch paths.
//!
//! A minimal 3-broker fake (racks `zone-a` / `zone-b` / `zone-c`, partition leader on broker 2)
//! answers a Fetch carrying a rack that is not the leader's with a `preferred_read_replica` and
//! no records, like Redpanda with `enable_rack_awareness` / Kafka with `RackAwareReplicaSelector`.
//!
//! Upstream behavior under test:
//! - github.com/grafana/mimir/pkg/storage/ingest/reader.go (`start`: `ConcurrentFetchers` when
//!   `FetchConcurrencyMax > 0`, the Kafka client consuming the partition otherwise)
//! - github.com/grafana/mimir/pkg/storage/ingest/fetcher.go (`fetchSingle` / `buildFetchRequest`:
//!   Fetch v13 to the partition leader, no rack, `CurrentLeaderEpoch` from metadata)
//! - github.com/twmb/franz-go/pkg/kgo/source.go (rack in the Fetch request, preferred read
//!   replica move)

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
    ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse, RequestHeader,
    ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use mimir_common::backoff::BackoffConfig;
use mimir_config::{GoDuration, KafkaConfig, StringSliceCSV};
use mimir_storage::ingest::client_consumer::ClientPartitionConsumer;
use mimir_storage::ingest::{
    ConcurrentFetchers, FetcherMetrics, GenericOffsetReader, KafkaClient, PartitionOffsetClient,
    RangeErrorPolicy,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOPIC: &str = "rack";
const LEADER: i32 = 2;
const LEADER_EPOCH: i32 = 5;
const NUM_RECORDS: i64 = 5;
const TOPIC_ID: [u8; 16] = [7; 16];

fn rack_of(node: i32) -> &'static str {
    ["zone-a", "zone-b", "zone-c"][node as usize]
}

#[derive(Debug, Clone)]
struct SeenFetch {
    broker: i32,
    version: i16,
    rack: String,
    current_leader_epoch: i32,
    topic_id: [u8; 16],
}

struct Cluster {
    ports: Vec<u16>,
    fetches: Mutex<Vec<SeenFetch>>,
    hwm: AtomicI64,
}

impl Cluster {
    async fn start() -> Arc<Self> {
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let c = Arc::new(Self {
            ports: listeners.iter().map(|l| l.local_addr().unwrap().port()).collect(),
            fetches: Mutex::new(Vec::new()),
            hwm: AtomicI64::new(NUM_RECORDS),
        });
        for (node, l) in listeners.into_iter().enumerate() {
            let c = Arc::clone(&c);
            tokio::spawn(async move {
                while let Ok((s, _)) = l.accept().await {
                    tokio::spawn(Arc::clone(&c).serve(node as i32, s));
                }
            });
        }
        c
    }

    fn addr(&self, node: usize) -> String {
        format!("127.0.0.1:{}", self.ports[node])
    }

    async fn serve(self: Arc<Self>, node: i32, mut s: TcpStream) {
        loop {
            let mut len = [0u8; 4];
            if s.read_exact(&mut len).await.is_err() {
                return;
            }
            let mut buf = vec![0u8; u32::from_be_bytes(len) as usize];
            if s.read_exact(&mut buf).await.is_err() {
                return;
            }
            let mut buf = Bytes::from(buf);
            let key = ApiKey::try_from(i16::from_be_bytes([buf[0], buf[1]])).unwrap();
            let ver = i16::from_be_bytes([buf[2], buf[3]]);
            let hv = match key {
                ApiKey::ApiVersions => ApiVersionsRequest::header_version(ver),
                ApiKey::Metadata => MetadataRequest::header_version(ver),
                ApiKey::ListOffsets => ListOffsetsRequest::header_version(ver),
                ApiKey::Fetch => FetchRequest::header_version(ver),
                _ => return,
            };
            let hdr = RequestHeader::decode(&mut buf, hv).unwrap();
            let corr = hdr.correlation_id;
            let out = match key {
                ApiKey::ApiVersions => {
                    let _ = ApiVersionsRequest::decode(&mut buf, ver);
                    let mut r = ApiVersionsResponse::default();
                    for (k, max) in [
                        (ApiKey::ApiVersions, 3),
                        (ApiKey::Metadata, 12),
                        (ApiKey::ListOffsets, 7),
                        (ApiKey::Fetch, 13),
                    ] {
                        let mut av = ApiVersion::default();
                        av.api_key = k as i16;
                        av.min_version = 0;
                        av.max_version = max;
                        r.api_keys.push(av);
                    }
                    encode(0, ver, corr, &r)
                }
                ApiKey::Metadata => {
                    let _ = MetadataRequest::decode(&mut buf, ver);
                    encode(MetadataResponse::header_version(ver), ver, corr, &self.metadata())
                }
                ApiKey::ListOffsets => {
                    let req = ListOffsetsRequest::decode(&mut buf, ver).unwrap();
                    let ts = req.topics[0].partitions[0].timestamp;
                    let mut p = ListOffsetsPartitionResponse::default();
                    p.partition_index = 0;
                    p.offset = if ts == -2 { 0 } else { self.hwm.load(Ordering::SeqCst) };
                    p.timestamp = -1;
                    let mut t = ListOffsetsTopicResponse::default();
                    t.name = TopicName(StrBytes::from_static_str(TOPIC));
                    t.partitions = vec![p];
                    let mut r = ListOffsetsResponse::default();
                    r.topics = vec![t];
                    encode(ListOffsetsResponse::header_version(ver), ver, corr, &r)
                }
                ApiKey::Fetch => {
                    let req = FetchRequest::decode(&mut buf, ver).unwrap();
                    let resp = self.fetch(node, ver, &req).await;
                    encode(FetchResponse::header_version(ver), ver, corr, &resp)
                }
                _ => unreachable!(),
            };
            if s.write_all(&out).await.is_err() {
                return;
            }
        }
    }

    fn metadata(&self) -> MetadataResponse {
        let mut r = MetadataResponse::default();
        for node in 0..3 {
            let mut b = MetadataResponseBroker::default();
            b.node_id = BrokerId(node);
            b.host = StrBytes::from_static_str("127.0.0.1");
            b.port = self.ports[node as usize] as i32;
            b.rack = Some(StrBytes::from_static_str(rack_of(node)));
            r.brokers.push(b);
        }
        r.controller_id = BrokerId(LEADER);
        let mut p = MetadataResponsePartition::default();
        p.partition_index = 0;
        p.leader_id = BrokerId(LEADER);
        p.leader_epoch = LEADER_EPOCH;
        p.replica_nodes = vec![BrokerId(0), BrokerId(1), BrokerId(2)];
        p.isr_nodes = p.replica_nodes.clone();
        let mut t = MetadataResponseTopic::default();
        t.name = Some(TopicName(StrBytes::from_static_str(TOPIC)));
        t.topic_id = uuid::Uuid::from_bytes(TOPIC_ID);
        t.partitions = vec![p];
        r.topics = vec![t];
        r
    }

    /// KIP-392 broker side: a consumer with a rack other than the leader's is redirected to the
    /// replica in its rack (no records); any replica serves records.
    async fn fetch(&self, node: i32, ver: i16, req: &FetchRequest) -> FetchResponse {
        let t = &req.topics[0];
        let fp = &t.partitions[0];
        let rack = req.rack_id.as_str().to_string();
        self.fetches.lock().push(SeenFetch {
            broker: node,
            version: ver,
            rack: rack.clone(),
            current_leader_epoch: fp.current_leader_epoch,
            topic_id: *t.topic_id.as_bytes(),
        });
        let hwm = self.hwm.load(Ordering::SeqCst);
        let mut pd = PartitionData::default();
        pd.partition_index = 0;
        pd.high_watermark = hwm;
        pd.last_stable_offset = hwm;
        pd.log_start_offset = 0;
        pd.preferred_read_replica = BrokerId(-1);
        if ver >= 11 && node == LEADER && !rack.is_empty() && rack != rack_of(LEADER) {
            let preferred = (0..3).find(|n| rack_of(*n) == rack).unwrap();
            pd.preferred_read_replica = BrokerId(preferred);
        } else if fp.fetch_offset < hwm {
            pd.records = Some(encode_records(fp.fetch_offset, hwm));
        } else {
            tokio::time::sleep(Duration::from_millis(req.max_wait_ms.clamp(0, 2000) as u64)).await;
        }
        let mut ft = FetchableTopicResponse::default();
        ft.topic = t.topic.clone();
        ft.topic_id = t.topic_id;
        ft.partitions = vec![pd];
        let mut r = FetchResponse::default();
        r.responses = vec![ft];
        r
    }
}

fn encode<R: Encodable>(hv: i16, ver: i16, corr: i32, r: &R) -> Bytes {
    let mut hdr = ResponseHeader::default();
    hdr.correlation_id = corr;
    let mut out = BytesMut::new();
    out.put_u32(0);
    hdr.encode(&mut out, hv).unwrap();
    r.encode(&mut out, ver).unwrap();
    let len = (out.len() - 4) as u32;
    out[0..4].copy_from_slice(&len.to_be_bytes());
    out.freeze()
}

fn encode_records(from: i64, to: i64) -> Bytes {
    let recs: Vec<Record> = (from..to)
        .map(|o| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: o,
            sequence: NO_SEQUENCE,
            timestamp: 1_700_000_000_000 + o,
            key: Some(Bytes::from_static(b"tenant")),
            value: Some(Bytes::from(format!("record-{o}"))),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        recs.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
        Some(mimir_storage::ingest::compression::compress_record_batch),
    )
    .unwrap();
    buf.freeze()
}

fn kafka_cfg(cluster: &Cluster, rack: &str) -> KafkaConfig {
    KafkaConfig {
        address: StringSliceCSV(vec![cluster.addr(0)]),
        topic: TOPIC.to_string(),
        client_rack: rack.to_string(),
        write_timeout: GoDuration::from_secs(5),
        ..Default::default()
    }
}

/// `fetch_concurrency_max = 0`: the client path sends the rack, follows the leader's
/// `preferred_read_replica` to the follower in its rack and consumes from it (Go: kgo consumer).
/// Before the fix the reader re-sent the rack to the leader forever and never got a record.
#[tokio::test]
async fn client_path_sends_rack_and_follows_preferred_read_replica() {
    let cluster = Cluster::start().await;
    let client = Arc::new(KafkaClient::from_config(&kafka_cfg(&cluster, "zone-a")).unwrap());
    let consumer =
        ClientPartitionConsumer::new(Arc::clone(&client), TOPIC, 0, 0, Duration::from_millis(100));
    consumer.start().await;

    let mut got = Vec::new();
    while (got.len() as i64) < NUM_RECORDS {
        let f = consumer
            .poll_fetches_until(tokio::time::sleep(Duration::from_secs(10)))
            .await
            .expect("records within 10s");
        assert!(f.err.is_none(), "{:?}", f.err);
        got.extend(f.records.iter().map(|r| r.offset));
    }
    assert_eq!(got, (0..NUM_RECORDS).collect::<Vec<_>>());
    consumer.stop().await;

    let seen = cluster.fetches.lock().clone();
    assert!(seen.iter().all(|f| f.rack == "zone-a"), "{seen:?}");
    assert!(seen.iter().all(|f| f.current_leader_epoch == LEADER_EPOCH), "{seen:?}");
    // The first fetch goes to the leader, which redirects; everything after is served by the
    // zone-a replica (broker 0).
    assert_eq!(seen[0].broker, LEADER, "{seen:?}");
    let leader_fetches = seen.iter().filter(|f| f.broker == LEADER).count();
    assert_eq!(leader_fetches, 1, "busy loop on the leader: {seen:?}");
    assert!(seen[1..].iter().all(|f| f.broker == 0), "{seen:?}");
}

/// Without a rack the client path reads from the leader.
#[tokio::test]
async fn client_path_without_rack_reads_from_leader() {
    let cluster = Cluster::start().await;
    let client = Arc::new(KafkaClient::from_config(&kafka_cfg(&cluster, "")).unwrap());
    let consumer =
        ClientPartitionConsumer::new(Arc::clone(&client), TOPIC, 0, -2, Duration::from_millis(100));
    consumer.start().await;
    let f = consumer
        .poll_fetches_until(tokio::time::sleep(Duration::from_secs(10)))
        .await
        .expect("records within 10s");
    assert_eq!(f.records.len() as i64, NUM_RECORDS);
    consumer.stop().await;
    let seen = cluster.fetches.lock().clone();
    assert!(seen.iter().all(|f| f.broker == LEADER && f.rack.is_empty()), "{seen:?}");
}

/// `fetch_concurrency_max > 0`: ConcurrentFetchers never sends the rack and always fetches
/// from the partition leader with Fetch v13 (topic ID) and the leader epoch from metadata, even
/// if the client rack is configured.
#[tokio::test]
async fn concurrent_fetchers_ignore_rack_and_read_from_leader() {
    let cluster = Cluster::start().await;
    let cfg = kafka_cfg(&cluster, "zone-a");
    let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
    assert_eq!(client.client_rack(), "zone-a");
    let offsets = PartitionOffsetClient::with_config(Arc::clone(&client), &cfg, None);
    let start_reader = Arc::new(GenericOffsetReader::new(
        Arc::new(|| Box::pin(async { Ok(0) })),
        Duration::from_secs(1),
    ));
    let f = ConcurrentFetchers::new(
        Arc::clone(&client),
        TOPIC,
        0,
        -2,
        3,
        0,
        true,
        Duration::from_millis(100),
        &offsets,
        RangeErrorPolicy::ResumeFromStart,
        start_reader,
        BackoffConfig {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            max_retries: 0,
        },
        FetcherMetrics::new(None),
    )
    .await
    .unwrap();
    assert_eq!(f.topic_id(), TOPIC_ID);
    f.start().await;
    let mut got = Vec::new();
    while (got.len() as i64) < NUM_RECORDS {
        let r = f.poll_fetches(Duration::from_secs(10)).await.expect("records within 10s");
        assert!(r.err.is_none(), "{:?}", r.err);
        got.extend(r.records.iter().map(|r| r.offset));
    }
    assert_eq!(got, (0..NUM_RECORDS).collect::<Vec<_>>());
    f.stop().await;

    let seen = cluster.fetches.lock().clone();
    let mut by: BTreeMap<(i32, i16, String, i32), usize> = BTreeMap::new();
    for s in &seen {
        assert_eq!(s.topic_id, TOPIC_ID);
        *by.entry((s.broker, s.version, s.rack.clone(), s.current_leader_epoch)).or_default() += 1;
    }
    assert_eq!(by.len(), 1, "{by:?}");
    assert!(by.contains_key(&(LEADER, 13, String::new(), LEADER_EPOCH)), "{by:?}");
}

/// kgo `broker.loadConnection`: Fetch requests use their own connection, so a long-polling
/// Fetch (brokers answer a connection's requests in order) never delays ListOffsets / metadata.
#[tokio::test]
async fn long_poll_fetch_does_not_block_other_requests() {
    let cluster = Cluster::start().await;
    let client = Arc::new(KafkaClient::from_config(&kafka_cfg(&cluster, "")).unwrap());
    client.refresh_metadata(&[TOPIC]).await.unwrap();
    let c = Arc::clone(&client);
    let fetch = tokio::spawn(async move {
        // Beyond the HWM: the broker holds the request for max_wait.
        c.fetch(TOPIC, 0, NUM_RECORDS, Duration::from_millis(1500), 1, 1 << 20)
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let start = std::time::Instant::now();
    let end = client.list_offset(TOPIC, 0, -1).await.unwrap();
    assert_eq!(end, NUM_RECORDS);
    assert!(
        start.elapsed() < Duration::from_millis(800),
        "ListOffsets waited {:?} behind a long-poll Fetch",
        start.elapsed()
    );
    assert!(fetch.await.unwrap().unwrap().records.is_empty());
}
