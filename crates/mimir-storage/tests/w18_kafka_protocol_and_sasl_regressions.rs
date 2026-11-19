//! Regression tests for Wave 18 Kafka wire protocol, SASL re-auth, consumer/producer,
//! multi-cluster teardown, committer, and W3C tracing fixes (W18-SR-06 & W18-KM-01..08).

#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use helpers::{create_test_kafka_config, FakeKafkaCluster};
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_commit_response::OffsetCommitResponse;
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
    FindCoordinatorRequest, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest, ProduceResponse,
    RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use kafka_protocol::indexmap::IndexMap;
use mimir_config::types::SASLMechanism;
use mimir_config::{GoDuration, KafkaConfig, OrderedConsumptionConfig, StringSliceCSV};
use mimir_storage::ingest::client_consumer::ClientPartitionConsumer;
use mimir_storage::ingest::compression::compress_record_batch;
use mimir_storage::ingest::{
    AdmClient, KafkaClient, KafkaClientError, KafkaClientExtendedMetrics,
    KafkaCompression, KafkaProducerClient, KafkaWriterClient, KafkaWriterClientOptions,
    MultiClusterPartitionReader, OffsetFile, PartitionCommitter, PreCommitNotifier, ProducerError,
    RecordBatchConsumer, SampledOnlyPropagator, SampledOnlyTracer, SpanRecorder,
    TraceSpanContext, TracedKafkaRecord, WriterRecord, FetchedRecord,
};
use parking_lot::Mutex as SyncMutex;
use prometheus::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    if stream.read_exact(&mut buf).await.is_err() {
        return None;
    }
    Some(buf)
}

async fn write_response<Resp: Encodable + HeaderVersion>(
    stream: &mut TcpStream,
    api_key: ApiKey,
    ver: i16,
    corr_id: i32,
    resp: &Resp,
) -> std::io::Result<()> {
    let mut hdr = ResponseHeader::default();
    hdr.correlation_id = corr_id;
    let hdr_ver = if api_key == ApiKey::ApiVersions {
        0
    } else {
        Resp::header_version(ver)
    };
    let mut out = BytesMut::new();
    out.put_u32(0);
    hdr.encode(&mut out, hdr_ver).unwrap();
    resp.encode(&mut out, ver).unwrap();
    let frame_len = (out.len() - 4) as u32;
    out[0..4].copy_from_slice(&frame_len.to_be_bytes());
    stream.write_all(&out).await?;
    stream.flush().await
}

fn default_api_versions() -> Vec<ApiVersion> {
    let entries = [
        (ApiKey::Produce, 3, 9),
        (ApiKey::Fetch, 4, 13),
        (ApiKey::ListOffsets, 1, 7),
        (ApiKey::Metadata, 1, 12),
        (ApiKey::OffsetCommit, 2, 8),
        (ApiKey::OffsetFetch, 1, 7),
        (ApiKey::FindCoordinator, 0, 3),
        (ApiKey::SaslHandshake, 0, 1),
        (ApiKey::ApiVersions, 0, 3),
        (ApiKey::CreateTopics, 0, 5),
        (ApiKey::SaslAuthenticate, 0, 2),
    ];
    entries
        .into_iter()
        .map(|(k, min, max)| {
            let mut v = ApiVersion::default();
            v.api_key = k as i16;
            v.min_version = min;
            v.max_version = max;
            v
        })
        .collect()
}

fn encode_v2_batch(base_offset: i64, recs: &[(i64, &[u8], &[u8])]) -> Bytes {
    let records: Vec<Record> = recs
        .iter()
        .enumerate()
        .map(|(idx, (ts, k, v))| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: base_offset + idx as i64,
            sequence: NO_SEQUENCE,
            timestamp: *ts,
            key: Some(Bytes::copy_from_slice(k)),
            value: Some(Bytes::copy_from_slice(v)),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
        Some(compress_record_batch),
    )
    .unwrap();
    buf.freeze()
}

// ---------------------------------------------------------------------------
// 1. W18-SR-06: PartitionCommitter warns & continues on notify_pre_commit error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_sr_06_partition_committer_warns_and_continues_and_stop_commits_unconditionally() {
    struct AlwaysFailNotifier {
        calls: AtomicUsize,
    }
    impl PreCommitNotifier for AlwaysFailNotifier {
        fn notify_pre_commit(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err("EIO: tenant WAL sync failed".to_string())
            })
        }
    }

    struct RecordingAdm {
        commits: SyncMutex<Vec<i64>>,
    }
    impl AdmClient for RecordingAdm {
        fn commit_offsets(
            &self,
            _group: &str,
            _topic: &str,
            _partition_id: i32,
            offset: i64,
        ) -> Pin<Box<dyn Future<Output = Result<i64, String>> + Send + '_>> {
            Box::pin(async move {
                self.commits.lock().push(offset);
                Ok(offset)
            })
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let off_path = dir.path().join("kafka-offset.json");
    let off_file = Arc::new(OffsetFile::new(off_path.clone(), 0));
    let notifier = Arc::new(AlwaysFailNotifier {
        calls: AtomicUsize::new(0),
    });
    let adm = Arc::new(RecordingAdm {
        commits: SyncMutex::new(Vec::new()),
    });
    let cfg = create_test_kafka_config("127.0.0.1:9092", "ingest");
    let reg = Registry::new();

    let committer = PartitionCommitter::new(
        cfg,
        Arc::clone(&adm) as Arc<dyn AdmClient>,
        0,
        "test-group",
        Arc::clone(&notifier) as Arc<dyn PreCommitNotifier>,
        Some(Arc::clone(&off_file)),
        Some(&reg),
    );

    // Go 1:1 parity: notify_pre_commit failure warns and continues with Kafka & offset-file commit,
    // without incrementing commit_failures_total.
    committer.commit(100).await.unwrap();
    assert_eq!(committer.last_committed_offset(), 100);
    assert_eq!(off_file.read().unwrap(), 100);
    assert_eq!(*adm.commits.lock(), vec![100]);
    assert_eq!(committer.commit_failures_count(), 0.0);

    // stop() must always commit when to_commit >= 0 (even when equal to last_committed_offset).
    committer.enqueue_offset(100);
    committer.stop().await;
    assert_eq!(*adm.commits.lock(), vec![100, 100]);
    assert_eq!(notifier.calls.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// 2. W18-KM-01: ClientPartitionConsumer::reset_offset clamps [start, end]
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_km_01_client_consumer_reset_offset_clamps_after_retention() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let list_offsets_calls = Arc::new(AtomicUsize::new(0));
    let fc = Arc::clone(&fetch_calls);
    let lc = Arc::clone(&list_offsets_calls);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let fc = Arc::clone(&fc);
            let lc = Arc::clone(&lc);
            tokio::spawn(async move {
                while let Some(frame) = read_frame(&mut stream).await {
                    let mut buf = Bytes::from(frame);
                    let api_key = i16::from_be_bytes([buf[0], buf[1]]);
                    let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
                    let key = ApiKey::try_from(api_key).unwrap();
                    let hdr = RequestHeader::decode(&mut buf, key.request_header_version(api_ver))
                        .unwrap();

                    match key {
                        ApiKey::ApiVersions => {
                            let _ = ApiVersionsRequest::decode(&mut buf, api_ver).unwrap();
                            let mut resp = ApiVersionsResponse::default();
                            resp.api_keys = default_api_versions();
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::Metadata => {
                            let _ = MetadataRequest::decode(&mut buf, api_ver).unwrap();
                            let mut broker = MetadataResponseBroker::default();
                            broker.node_id = BrokerId(0);
                            broker.host = StrBytes::from_static_str("127.0.0.1");
                            broker.port = addr.port() as i32;

                            let mut part = MetadataResponsePartition::default();
                            part.partition_index = 0;
                            part.leader_id = BrokerId(0);
                            part.leader_epoch = 1;

                            let mut topic = MetadataResponseTopic::default();
                            topic.name = Some(TopicName(StrBytes::from_static_str("w18-ooor")));
                            topic.partitions = vec![part];

                            let mut resp = MetadataResponse::default();
                            resp.brokers = vec![broker];
                            resp.controller_id = BrokerId(0);
                            resp.topics = vec![topic];
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::ListOffsets => {
                            let req = ListOffsetsRequest::decode(&mut buf, api_ver).unwrap();
                            lc.fetch_add(1, Ordering::SeqCst);
                            let ts = req.topics[0].partitions[0].timestamp;
                            let mut p = ListOffsetsPartitionResponse::default();
                            p.partition_index = 0;
                            match ts {
                                -2 => {
                                    // Log retention advanced start offset to 500.
                                    p.offset = 500;
                                    p.timestamp = -1;
                                }
                                -1 => {
                                    p.offset = 502;
                                    p.timestamp = -1;
                                }
                                _ => {
                                    // By-timestamp lookup returns 500 (>= prior=100!).
                                    p.offset = 500;
                                    p.timestamp = 5000;
                                }
                            }
                            let mut t = ListOffsetsTopicResponse::default();
                            t.name = TopicName(StrBytes::from_static_str("w18-ooor"));
                            t.partitions = vec![p];
                            let mut resp = ListOffsetsResponse::default();
                            resp.topics = vec![t];
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::Fetch => {
                            let req = FetchRequest::decode(&mut buf, api_ver).unwrap();
                            let n = fc.fetch_add(1, Ordering::SeqCst);
                            let req_off = req.topics[0].partitions[0].fetch_offset;
                            let mut p = PartitionData::default();
                            p.partition_index = 0;
                            p.high_watermark = 502;
                            p.log_start_offset = 500;

                            if n == 0 && req_off == 0 {
                                // First fetch: deliver offsets 0..=99, setting prior=100, ts=1099.
                                let recs: Vec<(i64, &[u8], &[u8])> = (0..100)
                                    .map(|i| (1000 + i, b"k".as_slice(), b"v".as_slice()))
                                    .collect();
                                p.records = Some(encode_v2_batch(0, &recs));
                            } else if req_off < 500 {
                                p.error_code = ResponseError::OffsetOutOfRange.code();
                            } else {
                                p.records = Some(encode_v2_batch(
                                    500,
                                    &[(5000, b"k".as_slice(), b"after-retention".as_slice())],
                                ));
                            }

                            let mut ft = FetchableTopicResponse::default();
                            ft.topic = TopicName(StrBytes::from_static_str("w18-ooor"));
                            ft.partitions = vec![p];
                            let mut resp = FetchResponse::default();
                            resp.responses = vec![ft];
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    let client = Arc::new(
        KafkaClient::new(
            vec![addr.to_string()],
            "w18-km-01",
            "",
            Duration::from_secs(2),
            Duration::from_secs(2),
        )
        .unwrap(),
    );
    let consumer =
        ClientPartitionConsumer::new(client, "w18-ooor", 0, 0, Duration::from_millis(50));
    consumer.start().await;

    let mut collected = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    while collected.len() < 101 && Instant::now() < deadline {
        let rem = deadline.saturating_duration_since(Instant::now());
        let Some(f) = consumer.poll_fetches_until(tokio::time::sleep(rem)).await else {
            break;
        };
        assert!(f.err.is_none(), "unexpected fetch err: {:?}", f.err);
        collected.extend(f.records);
    }
    consumer.stop().await;
    srv.abort();

    assert_eq!(collected.len(), 101);
    assert_eq!(collected.last().unwrap().offset, 500);
    assert_eq!(collected.last().unwrap().value.as_ref(), b"after-retention");
    // Must not spin: 1 initial fetch + 1 OOOR fetch + 1 recovered fetch (+ optional idle poll).
    assert!(
        fetch_calls.load(Ordering::SeqCst) <= 5,
        "fetch storm detected: {} fetches",
        fetch_calls.load(Ordering::SeqCst)
    );
    assert_eq!(
        list_offsets_calls.load(Ordering::SeqCst),
        3,
        "expected exactly start, end, and by_time ListOffsets calls"
    );
}

// ---------------------------------------------------------------------------
// 3. W18-KM-02: KIP-368 in-place SASL re-auth + drain + <=1000ms no livelock
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_km_02_kip368_in_place_reauth_drains_inflight_and_handles_short_lifetime() {
    #[derive(Default)]
    struct Stats {
        tcp_accepts: AtomicUsize,
        sasl_auths: AtomicUsize,
        fetch_started: tokio::sync::Notify,
    }

    async fn run_sasl_server(
        session_lifetime_ms: i64,
        slow_first_fetch_ms: u64,
        fail_reauth_after_first: bool,
    ) -> (SocketAddr, Arc<Stats>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stats = Arc::new(Stats::default());
        let st = Arc::clone(&stats);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                st.tcp_accepts.fetch_add(1, Ordering::SeqCst);
                let st = Arc::clone(&st);
                tokio::spawn(async move {
                    let mut conn_auth_count = 0usize;
                    while let Some(frame) = read_frame(&mut stream).await {
                        let mut buf = Bytes::from(frame);
                        let api_key = i16::from_be_bytes([buf[0], buf[1]]);
                        let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
                        let key = ApiKey::try_from(api_key).unwrap();
                        let hdr =
                            RequestHeader::decode(&mut buf, key.request_header_version(api_ver))
                                .unwrap();

                        match key {
                            ApiKey::ApiVersions => {
                                let _ = ApiVersionsRequest::decode(&mut buf, api_ver).unwrap();
                                let mut resp = ApiVersionsResponse::default();
                                resp.api_keys = default_api_versions();
                                write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                    .await
                                    .unwrap();
                            }
                            ApiKey::SaslHandshake => {
                                let _ = SaslHandshakeRequest::decode(&mut buf, api_ver).unwrap();
                                let mut resp = SaslHandshakeResponse::default();
                                resp.mechanisms = vec![StrBytes::from_static_str("PLAIN")];
                                write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                    .await
                                    .unwrap();
                            }
                            ApiKey::SaslAuthenticate => {
                                let _ = SaslAuthenticateRequest::decode(&mut buf, api_ver).unwrap();
                                st.sasl_auths.fetch_add(1, Ordering::SeqCst);
                                conn_auth_count += 1;
                                let mut resp = SaslAuthenticateResponse::default();
                                if fail_reauth_after_first && conn_auth_count > 1 {
                                    resp.error_code = ResponseError::SaslAuthenticationFailed.code();
                                    resp.error_message =
                                        Some(StrBytes::from_static_str("forced reauth fail"));
                                } else {
                                    resp.session_lifetime_ms = session_lifetime_ms;
                                }
                                write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                    .await
                                    .unwrap();
                            }
                            ApiKey::Metadata => {
                                let _ = MetadataRequest::decode(&mut buf, api_ver).unwrap();
                                let mut broker = MetadataResponseBroker::default();
                                broker.node_id = BrokerId(0);
                                broker.host = StrBytes::from_static_str("127.0.0.1");
                                broker.port = addr.port() as i32;

                                let mut part = MetadataResponsePartition::default();
                                part.partition_index = 0;
                                part.leader_id = BrokerId(0);

                                let mut topic = MetadataResponseTopic::default();
                                topic.name = Some(TopicName(StrBytes::from_static_str("t")));
                                topic.partitions = vec![part];

                                let mut resp = MetadataResponse::default();
                                resp.brokers = vec![broker];
                                resp.controller_id = BrokerId(0);
                                resp.topics = vec![topic];
                                write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                    .await
                                    .unwrap();
                            }
                            ApiKey::ListOffsets => {
                                let _ = ListOffsetsRequest::decode(&mut buf, api_ver).unwrap();
                                let mut p = ListOffsetsPartitionResponse::default();
                                p.partition_index = 0;
                                p.offset = 7;
                                let mut t = ListOffsetsTopicResponse::default();
                                t.name = TopicName(StrBytes::from_static_str("t"));
                                t.partitions = vec![p];
                                let mut resp = ListOffsetsResponse::default();
                                resp.topics = vec![t];
                                write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                    .await
                                    .unwrap();
                            }
                            ApiKey::Fetch => {
                                let _ = FetchRequest::decode(&mut buf, api_ver).unwrap();
                                st.fetch_started.notify_waiters();
                                if slow_first_fetch_ms > 0 {
                                    tokio::time::sleep(Duration::from_millis(slow_first_fetch_ms))
                                        .await;
                                }
                                let mut p = PartitionData::default();
                                p.partition_index = 0;
                                p.high_watermark = 1;
                                p.records = Some(encode_v2_batch(
                                    0,
                                    &[(1000, b"k".as_slice(), b"ok".as_slice())],
                                ));
                                let mut ft = FetchableTopicResponse::default();
                                ft.topic = TopicName(StrBytes::from_static_str("t"));
                                ft.partitions = vec![p];
                                let mut resp = FetchResponse::default();
                                resp.responses = vec![ft];
                                if write_response(
                                    &mut stream,
                                    key,
                                    api_ver,
                                    hdr.correlation_id,
                                    &resp,
                                )
                                .await
                                .is_err()
                                {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                });
            }
        });
        (addr, stats, srv)
    }

    // Subtest A: In-flight Fetch must drain cleanly without ConnectionClosed, then reauth in-place.
    {
        let (addr, stats, srv) = run_sasl_server(1150, 260, false).await;
        let mut cfg = create_test_kafka_config(&addr.to_string(), "t");
        cfg.sasl_mechanism = SASLMechanism::Plain;
        cfg.sasl_username = "u".into();
        cfg.sasl_password = "p".into();
        let reg = Registry::new();
        let ext = KafkaClientExtendedMetrics::new("w18_test_", Some(&reg), None);
        let client = Arc::new(
            KafkaClient::from_config(&cfg)
                .unwrap()
                .with_extended_metrics(Some(ext.clone())),
        );

        // Initial metadata establishes Normal conn (expiry ~150ms).
        client.refresh_metadata(&["t"]).await.unwrap();
        // Also warm Fetch conn or use Fetch? Note: Normal conn handles ListOffsets & Metadata;
        // Fetch uses Fetch conn! So let's drive Fetch conn with a slow Fetch, wait for its ~150ms
        // expiry, and issue a 2nd Fetch (or slow Metadata/ListOffsets? Wait: Fetch conn is per
        // (addr, Fetch), Normal conn is per (addr, Normal)!).
        // Let's issue slow Fetch #1 on Fetch conn, wait for expiry (~160ms), then issue Fetch #2
        // on the SAME Fetch conn!
        let c1 = Arc::clone(&client);
        let f1 = tokio::spawn(async move {
            c1.fetch("t", 0, 0, Duration::from_millis(50), 1, 1024 * 1024)
                .await
        });
        stats.fetch_started.notified().await;
        // Wait past sasl_expiry (~150ms) while Fetch #1 is still in-flight (260ms).
        tokio::time::sleep(Duration::from_millis(180)).await;
        // Fetch #2 hits expired sasl_expiry on the SAME Fetch conn, must drain Fetch #1 cleanly
        // and re-authenticate in-place without killing Fetch #1.
        let r2 = client
            .fetch("t", 0, 0, Duration::from_millis(50), 1, 1024 * 1024)
            .await
            .unwrap();
        let r1 = f1.await.unwrap().unwrap();
        assert_eq!(r1.records[0].value.as_ref(), b"ok");
        assert_eq!(r2.records[0].value.as_ref(), b"ok");
        // 1 Normal conn + 1 Fetch conn = 2 TCP accepts total (NO reconnect!).
        assert_eq!(stats.tcp_accepts.load(Ordering::SeqCst), 2);
        // Initial auth on Normal + initial auth on Fetch + in-place reauth on Fetch = 3 SASL auths.
        assert_eq!(stats.sasl_auths.load(Ordering::SeqCst), 3);
        assert_eq!(ext.connects_total.get() as usize, 2);
        client.close().await;
        srv.abort();
    }

    // Subtest B: SessionLifetimeMs <= 1000 never livelocks (always allows at least 1 request).
    {
        let (addr, stats, srv) = run_sasl_server(500, 0, false).await;
        let mut cfg = create_test_kafka_config(&addr.to_string(), "t");
        cfg.sasl_mechanism = SASLMechanism::Plain;
        cfg.sasl_username = "u".into();
        cfg.sasl_password = "p".into();
        let client = KafkaClient::from_config(&cfg).unwrap();

        for _ in 0..4 {
            let off = client.list_offset("t", 0, -1).await.unwrap();
            assert_eq!(off, 7);
        }
        // Only 1 TCP connection opened; each request after the 1st re-authed in-place!
        assert_eq!(stats.tcp_accepts.load(Ordering::SeqCst), 1);
        assert!(stats.sasl_auths.load(Ordering::SeqCst) >= 4);
        client.close().await;
        srv.abort();
    }

    // Subtest C: In-place re-auth failure falls back to a fresh connection without killing old in-flight Fetch.
    {
        let (addr, stats, srv) = run_sasl_server(1100, 260, true).await;
        let mut cfg = create_test_kafka_config(&addr.to_string(), "t");
        cfg.sasl_mechanism = SASLMechanism::Plain;
        cfg.sasl_username = "u".into();
        cfg.sasl_password = "p".into();
        let client = Arc::new(KafkaClient::from_config(&cfg).unwrap());
        client.refresh_metadata(&["t"]).await.unwrap();

        let c1 = Arc::clone(&client);
        let f1 = tokio::spawn(async move {
            c1.fetch("t", 0, 0, Duration::from_millis(50), 1, 1024 * 1024)
                .await
        });
        stats.fetch_started.notified().await;
        tokio::time::sleep(Duration::from_millis(160)).await;
        // Fetch #2 sees expired Fetch conn, drains Fetch #1, attempts in-place reauth -> rejected ->
        // falls back to a fresh Fetch connection and succeeds!
        let r2 = client
            .fetch("t", 0, 0, Duration::from_millis(50), 1, 1024 * 1024)
            .await
            .unwrap();
        let r1 = f1.await.unwrap().unwrap();
        assert_eq!(r1.records[0].value.as_ref(), b"ok");
        assert_eq!(r2.records[0].value.as_ref(), b"ok");
        assert_eq!(stats.tcp_accepts.load(Ordering::SeqCst), 3);
        client.close().await;
        srv.abort();
    }
}

// ---------------------------------------------------------------------------
// 4. W18-KM-03: Fail fast on non-retriable broker errors & split >16MB batches
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_km_03_writer_client_fails_fast_and_splits_batches_at_max_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let produce_calls = Arc::new(AtomicUsize::new(0));
    let batch_sizes = Arc::new(SyncMutex::new(Vec::<usize>::new()));
    let force_err = Arc::new(AtomicI64::new(0));
    let next_offset = Arc::new(AtomicI64::new(0));

    let pc = Arc::clone(&produce_calls);
    let bs = Arc::clone(&batch_sizes);
    let fe = Arc::clone(&force_err);
    let no = Arc::clone(&next_offset);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let pc = Arc::clone(&pc);
            let bs = Arc::clone(&bs);
            let fe = Arc::clone(&fe);
            let no = Arc::clone(&no);
            tokio::spawn(async move {
                while let Some(frame) = read_frame(&mut stream).await {
                    let mut buf = Bytes::from(frame);
                    let api_key = i16::from_be_bytes([buf[0], buf[1]]);
                    let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
                    let key = ApiKey::try_from(api_key).unwrap();
                    let hdr = RequestHeader::decode(&mut buf, key.request_header_version(api_ver))
                        .unwrap();

                    match key {
                        ApiKey::ApiVersions => {
                            let _ = ApiVersionsRequest::decode(&mut buf, api_ver).unwrap();
                            let mut resp = ApiVersionsResponse::default();
                            resp.api_keys = default_api_versions();
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::Metadata => {
                            let _ = MetadataRequest::decode(&mut buf, api_ver).unwrap();
                            let mut broker = MetadataResponseBroker::default();
                            broker.node_id = BrokerId(0);
                            broker.host = StrBytes::from_static_str("127.0.0.1");
                            broker.port = addr.port() as i32;

                            let mut part = MetadataResponsePartition::default();
                            part.partition_index = 0;
                            part.leader_id = BrokerId(0);

                            let mut topic = MetadataResponseTopic::default();
                            topic.name = Some(TopicName(StrBytes::from_static_str("t")));
                            topic.partitions = vec![part];

                            let mut resp = MetadataResponse::default();
                            resp.brokers = vec![broker];
                            resp.controller_id = BrokerId(0);
                            resp.topics = vec![topic];
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::Produce => {
                            let req = ProduceRequest::decode(&mut buf, api_ver).unwrap();
                            pc.fetch_add(1, Ordering::SeqCst);
                            let raw_records = req.topic_data[0].partition_data[0]
                                .records
                                .as_ref()
                                .unwrap();
                            bs.lock().push(raw_records.len());

                            let mut pr = PartitionProduceResponse::default();
                            pr.index = 0;
                            let forced = fe.load(Ordering::SeqCst);
                            if forced != 0 {
                                pr.error_code = forced as i16;
                            } else if raw_records.len() > 16_000_000 {
                                pr.error_code = ResponseError::MessageTooLarge.code();
                            } else {
                                let num_recs = i32::from_be_bytes([
                                    raw_records[57],
                                    raw_records[58],
                                    raw_records[59],
                                    raw_records[60],
                                ]) as i64;
                                pr.base_offset = no.fetch_add(num_recs, Ordering::SeqCst);
                            }

                            let mut tr = TopicProduceResponse::default();
                            tr.name = TopicName(StrBytes::from_static_str("t"));
                            tr.partition_responses = vec![pr];
                            let mut resp = ProduceResponse::default();
                            resp.responses = vec![tr];
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    let mut cfg = create_test_kafka_config(&addr.to_string(), "t");
    cfg.write_timeout = GoDuration::from_secs(5);
    cfg.producer_compression = "none".to_string();
    let writer = KafkaWriterClient::new(&cfg, 4, None, KafkaWriterClientOptions::default()).unwrap();

    // Part A: Non-retriable broker errors fail fast (not retried until write_timeout -> RecordTimeout).
    for (code, expected_reason) in [
        (ResponseError::TopicAuthorizationFailed, "other"),
        (ResponseError::UnknownServerError, "other"),
        (ResponseError::RecordListTooLarge, "other"),
        (ResponseError::MessageTooLarge, "record-too-large"),
    ] {
        produce_calls.store(0, Ordering::SeqCst);
        force_err.store(code.code() as i64, Ordering::SeqCst);
        let t0 = Instant::now();
        let err = writer
            .produce_sync(WriterRecord {
                topic: "t".to_string(),
                partition: 0,
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
                headers: Vec::new(),
                timestamp_ms: None,
                offset: 0,
            })
            .await
            .unwrap_err();
        assert!(
            t0.elapsed() < Duration::from_secs(1),
            "non-retriable {code:?} took too long: {:?}",
            t0.elapsed()
        );
        assert_eq!(err.reason(), expected_reason, "err={err:?}");
        assert!(!matches!(err, ProducerError::RecordTimeout));
        assert_eq!(produce_calls.load(Ordering::SeqCst), 1);
    }

    // Part B: 4 x 5 MB records queued together within linger window split into <= 16 MB batches.
    force_err.store(0, Ordering::SeqCst);
    produce_calls.store(0, Ordering::SeqCst);
    batch_sizes.lock().clear();

    let payload = Bytes::from(vec![b'a'; 5_000_000]);
    let mut futs = Vec::new();
    for _ in 0..4 {
        let payload = payload.clone();
        let w = Arc::clone(&writer);
        futs.push(tokio::spawn(async move {
            w.produce_sync(WriterRecord {
                topic: "t".to_string(),
                partition: 0,
                key: Bytes::new(),
                value: payload,
                headers: Vec::new(),
                timestamp_ms: None,
                offset: 0,
            })
            .await
        }));
    }
    let mut offsets = Vec::new();
    for f in futs {
        offsets.push(f.await.unwrap().unwrap());
    }
    offsets.sort();
    assert_eq!(offsets, vec![0, 1, 2, 3]);
    let sizes = batch_sizes.lock().clone();
    assert!(sizes.len() >= 2, "expected >=2 split batches, got {sizes:?}");
    assert!(
        sizes.iter().all(|&s| s <= 16_000_000),
        "batch exceeded 16MB: {sizes:?}"
    );

    writer.close();
    srv.abort();
}

// ---------------------------------------------------------------------------
// 5. W18-KM-04: MultiClusterPartitionReader partial startup failure drains in-flight consume
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_km_04_multi_cluster_partial_start_failure_drains_inflight_consume() {
    let cluster0 = FakeKafkaCluster::start(1, "t0").await;
    // Produce 2 records to cluster0 so startup lag catch-up consumes them.
    let c0_client = KafkaClient::from_config(&create_test_kafka_config(&cluster0.addr, "t0")).unwrap();
    c0_client
        .produce(
            "t0",
            0,
            KafkaCompression::None,
            vec![
                mimir_storage::ingest::ProduceRecord {
                    key: Some(Bytes::from_static(b"t")),
                    value: Some(Bytes::from_static(b"r0")),
                    headers: vec![],
                    timestamp_ms: Some(1_000_000),
                },
                mimir_storage::ingest::ProduceRecord {
                    key: Some(Bytes::from_static(b"t")),
                    value: Some(Bytes::from_static(b"r1")),
                    headers: vec![],
                    timestamp_ms: Some(1_000_001),
                },
            ],
        )
        .await
        .unwrap();
    c0_client.close().await;

    struct SlowConsumer {
        started: AtomicBool,
        started_notify: tokio::sync::Notify,
        completed: AtomicBool,
        consumed_offsets: SyncMutex<Vec<i64>>,
    }
    impl RecordBatchConsumer for SlowConsumer {
        fn consume(
            &self,
            records: Vec<FetchedRecord>,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async move {
                self.started.store(true, Ordering::SeqCst);
                self.started_notify.notify_waiters();
                // Keep consume_fetches in-flight past cluster 1's startup failure.
                tokio::time::sleep(Duration::from_millis(200)).await;
                for r in records {
                    self.consumed_offsets.lock().push(r.offset);
                }
                self.completed.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    let consumer = Arc::new(SlowConsumer {
        started: AtomicBool::new(false),
        started_notify: tokio::sync::Notify::new(),
        completed: AtomicBool::new(false),
        consumed_offsets: SyncMutex::new(Vec::new()),
    });
    let c_wait = Arc::clone(&consumer);

    // Cluster 1 waits until cluster 0 is mid-consume_fetches, then fails startup cleanly.
    let fail_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fail_addr = fail_listener.local_addr().unwrap();
    let fail_srv = tokio::spawn(async move {
        if let Ok((stream, _)) = fail_listener.accept().await {
            c_wait.started_notify.notified().await;
            drop(stream);
        }
    });

    let mut cfg0 = create_test_kafka_config(&cluster0.addr, "t0");
    cfg0.target_consumer_lag_at_startup = GoDuration::from_millis(50);
    cfg0.max_consumer_lag_at_startup = GoDuration::from_millis(50);

    let mut cfg1 = create_test_kafka_config(&fail_addr.to_string(), "t1");
    cfg1.target_consumer_lag_at_startup = GoDuration::from_millis(50);
    cfg1.max_consumer_lag_at_startup = GoDuration::from_millis(50);
    let dir = tempfile::tempdir().unwrap();
    let off0 = dir.path().join("off0.json");
    let off1 = dir.path().join("off1.json");

    let ord = OrderedConsumptionConfig {
        enabled: true,
        max_batch_records: 2,
        max_batch_wait: GoDuration::from_millis(10),
        ..Default::default()
    };
    let multi = MultiClusterPartitionReader::new(
        &[cfg0, cfg1],
        &ord,
        0,
        "ing-0",
        vec![Some(off0.clone()), Some(off1)],
        Arc::clone(&consumer) as Arc<dyn RecordBatchConsumer>,
        None,
    )
    .unwrap();

    let err = multi.start().await.unwrap_err();
    let _ = err;

    // In-flight consume_fetches on cluster 0 must NOT have been dropped mid-flight,
    // and its committed offset must match the records flushed through HeapMerger!
    assert!(
        consumer.started.load(Ordering::SeqCst),
        "expected cluster 0 to enter in-flight consume_fetches"
    );
    assert!(
        consumer.completed.load(Ordering::SeqCst),
        "in-flight consume_fetches was cancelled mid-await on partial startup failure"
    );
    assert_eq!(*consumer.consumed_offsets.lock(), vec![0, 1]);
    assert_eq!(OffsetFile::new(off0, 0).read().unwrap(), 1);

    fail_srv.abort();
    cluster0.close();
}

// ---------------------------------------------------------------------------
// 6. W18-KM-05: IPv6 [host]:port formatting & any_conn_kind discovered brokers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_km_05_ipv6_host_port_and_any_conn_discovered_brokers() {
    // Part A: IPv6 server returning bare "::1" in MetadataResponse and FindCoordinatorResponse.
    let listener = match TcpListener::bind("[::1]:0").await {
        Ok(l) => l,
        Err(_) => return, // IPv6 loopback disabled in container
    };
    let addr = listener.local_addr().unwrap();
    let committed = Arc::new(SyncMutex::new(Vec::<i64>::new()));
    let cm = Arc::clone(&committed);

    let srv = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let cm = Arc::clone(&cm);
            tokio::spawn(async move {
                while let Some(frame) = read_frame(&mut stream).await {
                    let mut buf = Bytes::from(frame);
                    let api_key = i16::from_be_bytes([buf[0], buf[1]]);
                    let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
                    let key = ApiKey::try_from(api_key).unwrap();
                    let hdr = RequestHeader::decode(&mut buf, key.request_header_version(api_ver))
                        .unwrap();

                    match key {
                        ApiKey::ApiVersions => {
                            let _ = ApiVersionsRequest::decode(&mut buf, api_ver).unwrap();
                            let mut resp = ApiVersionsResponse::default();
                            resp.api_keys = default_api_versions();
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::Metadata => {
                            let _ = MetadataRequest::decode(&mut buf, api_ver).unwrap();
                            let mut broker = MetadataResponseBroker::default();
                            broker.node_id = BrokerId(1);
                            broker.host = StrBytes::from_static_str("::1");
                            broker.port = addr.port() as i32;

                            let mut part = MetadataResponsePartition::default();
                            part.partition_index = 0;
                            part.leader_id = BrokerId(1);

                            let mut topic = MetadataResponseTopic::default();
                            topic.name = Some(TopicName(StrBytes::from_static_str("v6-topic")));
                            topic.partitions = vec![part];

                            let mut resp = MetadataResponse::default();
                            resp.brokers = vec![broker];
                            resp.controller_id = BrokerId(1);
                            resp.topics = vec![topic];
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::FindCoordinator => {
                            let _ = FindCoordinatorRequest::decode(&mut buf, api_ver).unwrap();
                            let mut resp = FindCoordinatorResponse::default();
                            resp.node_id = BrokerId(1);
                            resp.host = StrBytes::from_static_str("::1");
                            resp.port = addr.port() as i32;
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        ApiKey::OffsetCommit => {
                            let req = OffsetCommitRequest::decode(&mut buf, api_ver).unwrap();
                            let off = req.topics[0].partitions[0].committed_offset;
                            cm.lock().push(off);
                            let mut p =
                                kafka_protocol::messages::offset_commit_response::OffsetCommitResponsePartition::default();
                            p.partition_index = 0;
                            let mut t =
                                kafka_protocol::messages::offset_commit_response::OffsetCommitResponseTopic::default();
                            t.name = TopicName(StrBytes::from_static_str("v6-topic"));
                            t.partitions = vec![p];
                            let mut resp = OffsetCommitResponse::default();
                            resp.topics = vec![t];
                            write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                                .await
                                .unwrap();
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    let client = KafkaClient::new(
        vec![format!("[::1]:{}", addr.port())],
        "v6-client",
        "",
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap();
    client.refresh_metadata(&["v6-topic"]).await.unwrap();
    client.commit_offset("grp", "v6-topic", 0, 88).await.unwrap();
    assert_eq!(*committed.lock(), vec![88]);
    client.close().await;
    srv.abort();

    // Part B: any_conn_kind uses metadata-discovered brokers when seed broker goes away.
    let discovered = FakeKafkaCluster::start(1, "rot-topic").await;
    let disc_port: i32 = discovered.addr.rsplit(':').next().unwrap().parse().unwrap();

    let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = seed_listener.local_addr().unwrap();
    let seed_srv = tokio::spawn(async move {
        if let Ok((mut stream, _)) = seed_listener.accept().await {
            while let Some(frame) = read_frame(&mut stream).await {
                let mut buf = Bytes::from(frame);
                let api_key = i16::from_be_bytes([buf[0], buf[1]]);
                let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
                let key = ApiKey::try_from(api_key).unwrap();
                let hdr =
                    RequestHeader::decode(&mut buf, key.request_header_version(api_ver)).unwrap();
                match key {
                    ApiKey::ApiVersions => {
                        let _ = ApiVersionsRequest::decode(&mut buf, api_ver).unwrap();
                        let mut resp = ApiVersionsResponse::default();
                        resp.api_keys = default_api_versions();
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                    }
                    ApiKey::Metadata => {
                        let _ = MetadataRequest::decode(&mut buf, api_ver).unwrap();
                        let mut broker = MetadataResponseBroker::default();
                        broker.node_id = BrokerId(42);
                        broker.host = StrBytes::from_static_str("127.0.0.1");
                        broker.port = disc_port;

                        let mut part = MetadataResponsePartition::default();
                        part.partition_index = 0;
                        part.leader_id = BrokerId(42);

                        let mut topic = MetadataResponseTopic::default();
                        topic.name = Some(TopicName(StrBytes::from_static_str("rot-topic")));
                        topic.partitions = vec![part];

                        let mut resp = MetadataResponse::default();
                        resp.brokers = vec![broker];
                        resp.controller_id = BrokerId(42);
                        resp.topics = vec![topic];
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                        break;
                    }
                    _ => break,
                }
            }
        }
    });

    let rot_client = KafkaClient::new(
        vec![seed_addr.to_string()],
        "rot",
        "",
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap();
    rot_client.refresh_metadata(&["rot-topic"]).await.unwrap();
    // Kill seed listener and close seed socket; only discovered broker 42 remains reachable.
    seed_srv.await.unwrap();
    // Clear pooled connections so any_conn_kind must re-dial (and seed is now dead!).
    rot_client.close().await;
    rot_client
        .commit_offset("grp", "rot-topic", 0, 12)
        .await
        .unwrap();
    assert_eq!(
        rot_client
            .fetch_committed_offset("grp", "rot-topic", 0)
            .await
            .unwrap(),
        Some(12)
    );
    rot_client.close().await;
    discovered.close();
}

// ---------------------------------------------------------------------------
// 7. W18-KM-06: Metadata v1..3 allow_auto_topic_creation & non-null empty client_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_km_06_metadata_v3_and_empty_client_id_non_null() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured_headers = Arc::new(SyncMutex::new(Vec::<(i16, Vec<u8>)>::new()));
    let ch = Arc::clone(&captured_headers);

    let srv = tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            while let Some(frame) = read_frame(&mut stream).await {
                let raw_clone = frame.clone();
                let mut buf = Bytes::from(frame);
                let api_key = i16::from_be_bytes([buf[0], buf[1]]);
                let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
                let key = ApiKey::try_from(api_key).unwrap();
                ch.lock().push((api_key, raw_clone));

                let hdr =
                    RequestHeader::decode(&mut buf, key.request_header_version(api_ver)).unwrap();
                assert_eq!(
                    hdr.client_id.as_ref().map(|s| s.as_str()),
                    Some(""),
                    "empty client_id must be non-null Some('') on wire for {key:?}"
                );

                match key {
                    ApiKey::ApiVersions => {
                        let _ = ApiVersionsRequest::decode(&mut buf, api_ver).unwrap();
                        let mut av_ver = ApiVersion::default();
                        av_ver.api_key = ApiKey::ApiVersions as i16;
                        av_ver.min_version = 0;
                        av_ver.max_version = 3;

                        // Advertise Metadata max_version = 3 only!
                        let mut md_ver = ApiVersion::default();
                        md_ver.api_key = ApiKey::Metadata as i16;
                        md_ver.min_version = 1;
                        md_ver.max_version = 3;

                        let mut resp = ApiVersionsResponse::default();
                        resp.api_keys = vec![av_ver, md_ver];
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                    }
                    ApiKey::Metadata => {
                        assert_eq!(api_ver, 3);
                        let _ = MetadataRequest::decode(&mut buf, api_ver).unwrap();
                        let mut broker = MetadataResponseBroker::default();
                        broker.node_id = BrokerId(0);
                        broker.host = StrBytes::from_static_str("127.0.0.1");
                        broker.port = addr.port() as i32;

                        let mut resp = MetadataResponse::default();
                        resp.brokers = vec![broker];
                        resp.controller_id = BrokerId(0);
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                    }
                    _ => {}
                }
            }
        }
    });

    let cfg = KafkaConfig {
        address: StringSliceCSV(vec![addr.to_string()]),
        ..Default::default()
    };
    assert_eq!(cfg.client_id, "");
    let client = KafkaClient::from_config(&cfg).unwrap();
    client.ping().await.unwrap();

    let captured = captured_headers.lock().clone();
    assert!(captured.len() >= 2);
    for (api_key, raw) in &captured {
        // RequestHeader v1/v2 wire layout: 2B api_key + 2B api_ver + 4B corr_id + 2B client_id len.
        // Non-null empty string "" is [0x00, 0x00], NOT [0xff, 0xff].
        assert_eq!(
            &raw[8..10],
            &[0x00, 0x00],
            "api_key {api_key} encoded client_id as non-empty/null: {:?}",
            &raw[..12]
        );
    }

    client.close().await;
    srv.abort();
}

// ---------------------------------------------------------------------------
// 8. W18-KM-07: OffsetCommit missing partition errors & v2 batch split edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn w18_km_07_offset_commit_missing_partition_and_mixed_or_corrupt_batches() {
    // Part A: OffsetCommitResponse omitting the requested partition must return Err.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let srv = tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            while let Some(frame) = read_frame(&mut stream).await {
                let mut buf = Bytes::from(frame);
                let api_key = i16::from_be_bytes([buf[0], buf[1]]);
                let api_ver = i16::from_be_bytes([buf[2], buf[3]]);
                let key = ApiKey::try_from(api_key).unwrap();
                let hdr =
                    RequestHeader::decode(&mut buf, key.request_header_version(api_ver)).unwrap();
                match key {
                    ApiKey::ApiVersions => {
                        let _ = ApiVersionsRequest::decode(&mut buf, api_ver).unwrap();
                        let mut resp = ApiVersionsResponse::default();
                        resp.api_keys = default_api_versions();
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                    }
                    ApiKey::FindCoordinator => {
                        let _ = FindCoordinatorRequest::decode(&mut buf, api_ver).unwrap();
                        let mut resp = FindCoordinatorResponse::default();
                        resp.node_id = BrokerId(0);
                        resp.host = StrBytes::from_static_str("127.0.0.1");
                        resp.port = addr.port() as i32;
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                    }
                    ApiKey::OffsetCommit => {
                        let _ = OffsetCommitRequest::decode(&mut buf, api_ver).unwrap();
                        // Return empty topics list (omitted partition!).
                        let resp = OffsetCommitResponse::default();
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                    }
                    ApiKey::OffsetFetch => {
                        let _ = OffsetFetchRequest::decode(&mut buf, api_ver).unwrap();
                        let mut p = OffsetFetchResponsePartition::default();
                        p.partition_index = 0;
                        p.committed_offset = -1;
                        let mut t = OffsetFetchResponseTopic::default();
                        t.name = TopicName(StrBytes::from_static_str("t"));
                        t.partitions = vec![p];
                        let mut resp = OffsetFetchResponse::default();
                        resp.topics = vec![t];
                        write_response(&mut stream, key, api_ver, hdr.correlation_id, &resp)
                            .await
                            .unwrap();
                    }
                    _ => {}
                }
            }
        }
    });

    let client = KafkaClient::new(
        vec![addr.to_string()],
        "c",
        "",
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap();
    let err = client.commit_offset("g", "t", 0, 5).await.unwrap_err();
    assert!(
        matches!(err, KafkaClientError::UnexpectedResponse(ref m) if m.contains("missing")),
        "expected missing partition error, got {err:?}"
    );

    // Part B: Mixed [v2 batch][v1 batch] must decode BOTH batches (fallback to RecordBatchDecoder).
    let b_v2 = encode_v2_batch(0, &[(1000, b"k0".as_slice(), b"v0".as_slice())]);
    let rec_v1 = Record {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 1,
        sequence: NO_SEQUENCE,
        timestamp: 1001,
        key: Some(Bytes::from_static(b"k1")),
        value: Some(Bytes::from_static(b"v1")),
        headers: IndexMap::new(),
    };
    let mut b_v1 = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut b_v1,
        std::iter::once(&rec_v1),
        &RecordEncodeOptions {
            version: 1,
            compression: Compression::None,
        },
        Some(compress_record_batch),
    )
    .unwrap();

    b_v1[0..8].copy_from_slice(&1i64.to_be_bytes());
    let mut mixed = BytesMut::new();
    mixed.extend_from_slice(&b_v2);
    mixed.extend_from_slice(&b_v1);
    let (recs, next_off, last_end) = client.decode_fetched_partition(mixed.freeze(), 0).unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].offset, 0);
    assert_eq!(recs[1].offset, 1);
    assert_eq!(recs[1].value.as_ref(), b"v1");
    assert_eq!(last_end, Some(1));
    assert_eq!(next_off, 2);

    // Part C: Trailing frame with unknown/corrupt magic > 2 must error, not silently drop.
    let mut corrupt = BytesMut::new();
    corrupt.extend_from_slice(&b_v2);
    let mut bad_tail = vec![0u8; 64];
    bad_tail[16] = 99; // invalid magic byte
    corrupt.extend_from_slice(&bad_tail);
    let err = client
        .decode_fetched_partition(corrupt.freeze(), 0)
        .unwrap_err();
    assert!(
        err.to_string().contains("magic"),
        "expected unknown magic error, got {err}"
    );

    client.close().await;
    srv.abort();
}

// ---------------------------------------------------------------------------
// 9. W18-KM-08: TraceSpanContext preserves W3C 0x02 (FlagsRandom) bit
// ---------------------------------------------------------------------------

#[test]
fn w18_km_08_trace_span_context_preserves_random_bit_0x02() {
    let tp_sampled_random = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03";
    let sc = TraceSpanContext::from_traceparent(tp_sampled_random).unwrap();
    assert!(sc.sampled);
    assert_eq!(sc.trace_flags, 0x03);
    assert_eq!(sc.to_traceparent(), tp_sampled_random);

    let tp_unsampled_random = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02";
    let sc_unsampled = TraceSpanContext::from_traceparent(tp_unsampled_random).unwrap();
    assert!(!sc_unsampled.sampled);
    assert_eq!(sc_unsampled.trace_flags, 0x02);
    assert_eq!(sc_unsampled.to_traceparent(), tp_unsampled_random);

    // SampledOnlyPropagator extract + inject preserves -03
    let prop = SampledOnlyPropagator;
    let mut headers = Vec::new();
    prop.inject(Some(&sc), &mut headers);
    assert_eq!(
        headers,
        vec![("traceparent".to_string(), Bytes::from(tp_sampled_random))]
    );
    let extracted = prop.extract(&headers).unwrap();
    assert_eq!(extracted.trace_flags, 0x03);
    assert_eq!(extracted.to_traceparent(), tp_sampled_random);

    // SampledOnlyTracer produce + fetch hooks preserve -03
    let recorder = SpanRecorder::new();
    let tracer = SampledOnlyTracer::new(recorder);
    let mut rec = TracedKafkaRecord {
        topic: "trace-topic".to_string(),
        span_ctx: Some(sc),
        ..Default::default()
    };
    tracer.on_produce_record_buffered(&mut rec);
    assert_eq!(
        rec.headers.iter().find(|(k, _)| k == "traceparent").unwrap().1.as_ref(),
        tp_sampled_random.as_bytes()
    );
    tracer.on_produce_record_unbuffered(&mut rec);

    let mut fetch_rec = TracedKafkaRecord {
        topic: "trace-topic".to_string(),
        headers: rec.headers,
        ..Default::default()
    };
    tracer.on_fetch_record_buffered(&mut fetch_rec);
    assert_eq!(
        fetch_rec.span_ctx.unwrap().to_traceparent(),
        tp_sampled_random
    );
    tracer.on_fetch_record_unbuffered(&mut fetch_rec, true);
}
