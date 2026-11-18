//! Wave 13 twin-process Go-vs-Rust differential tests for Ingester shutdown,
//! gRPC Push wire contract, unknown MetricMetadata type, native histogram zero_count
//! oneof parity, and Kafka offset-catalogue.json persistence on shutdown.

#![allow(clippy::result_large_err)]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use bytes::{Buf, BufMut, Bytes};
use futures::FutureExt;
use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    KfakeBrokerRequest, KfakeRecordInput, LabelJson, SampleJson, SpawnedKfakeBroker,
    TimeSeriesJson, WriteRequestJson,
};
use mimir_conformance::twin::{start_mimir, wait_ready, FakeS3, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use mimir_proto::zerocopy::encode_write_request_rw2_bytes;
use tokio::sync::Notify;
use tokio_stream::StreamExt;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Status};

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

#[derive(Clone, Default)]
struct RawBytesCodec;

impl Codec for RawBytesCodec {
    type Encode = Vec<u8>;
    type Decode = Bytes;
    type Encoder = RawBytesEncoder;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawBytesDecoder
    }
}

#[derive(Clone, Default)]
struct RawBytesEncoder;

impl Encoder for RawBytesEncoder {
    type Item = Vec<u8>;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct RawBytesDecoder;

impl Decoder for RawBytesDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}

async fn send_raw_grpc_unary(
    ch: Channel,
    path: &'static str,
    tenant: Option<&str>,
    payload: Vec<u8>,
) -> Result<Bytes, Status> {
    let mut grpc = tonic::client::Grpc::new(ch);
    grpc.ready()
        .await
        .map_err(|e| Status::unavailable(format!("client not ready: {e}")))?;
    let mut req = tonic::Request::new(payload);
    if let Some(t) = tenant {
        req.metadata_mut().insert(
            LOWER_ORG_ID_HEADER_NAME,
            MetadataValue::try_from(t).unwrap(),
        );
    }
    let resp = grpc
        .unary(
            req,
            axum::http::uri::PathAndQuery::from_static(path),
            RawBytesCodec,
        )
        .await?;
    Ok(resp.into_inner())
}

fn with_tenant<T>(tenant: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        LOWER_ORG_ID_HEADER_NAME,
        MetadataValue::try_from(tenant).unwrap(),
    );
    req
}

fn basic_ingester_yaml(http: u16, grpc: u16, dir: &Path, extra_blocks: &str) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
usage_stats:
  enabled: false
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ing-0
    instance_addr: 127.0.0.1
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
{extra_blocks}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
        tsdb = dir.join("tsdb").display(),
    )
}

fn fs_blocks(dir: &Path) -> String {
    format!(
        "  backend: filesystem\n  filesystem:\n    dir: {}",
        dir.join("bucket").display()
    )
}

fn s3_blocks(endpoint: &str) -> String {
    format!(
        "  backend: s3\n  s3:\n    endpoint: {endpoint}\n    bucket_name: blocks\n    region: us-east-1\n    access_key_id: conformance\n    secret_access_key: conformance-secret\n    insecure: true"
    )
}

#[test]
fn push_wire_malformed_rw2_metadata_type_and_histogram_zero_count_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();

    block_on_for_tests(async {
        let mut procs: Vec<(Impl, Proc, u16, u16)> = Vec::new();
        for which in IMPLS {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let http = free_test_port();
            let grpc = free_test_port();
            let yaml = basic_ingester_yaml(http, grpc, &dir, &fs_blocks(&dir));
            let proc = start_mimir(which, "ingester", &dir, &yaml, &[]);
            procs.push((which, proc, http, grpc));
        }
        for (_, proc, http, _) in &mut procs {
            wait_ready(&http_client, proc, *http, Duration::from_secs(60)).await;
        }

        let ts = now_ms() - 5_000;
        let rw1_sample = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "rw2_reject_probe")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut rw2_wire = Vec::new();
        encode_write_request_rw2_bytes(&rw1_sample, 0, None, &mut rw2_wire).unwrap();

        type WireParityTuple = (Impl, Vec<(String, Code)>, Vec<i32>, Vec<(u64, f64)>);
        let mut results: Vec<WireParityTuple> = Vec::new();

        for (which, _, _, grpc) in &procs {
            let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc}"))
                .unwrap()
                .connect()
                .await
                .unwrap();

            // 1. Malformed protobuf with & without tenant -> Internal; RW2 on Push -> Internal; valid without tenant -> Unknown.
            let mut push_codes = Vec::new();
            for (name, tenant, payload) in [
                ("malformed_with_tenant", Some("t-wire"), vec![0x08u8]),
                ("malformed_no_tenant", None, vec![0x08u8]),
                ("rw2_with_tenant", Some("t-wire"), rw2_wire.clone()),
                ("rw2_no_tenant", None, rw2_wire.clone()),
                ("valid_no_tenant", None, rw1_sample.encode_to_vec()),
            ] {
                let st = send_raw_grpc_unary(
                    ch.clone(),
                    "/cortex.Ingester/Push",
                    tenant,
                    payload,
                )
                .await
                .unwrap_err();
                push_codes.push((name.to_string(), st.code()));
            }

            // 2. Unknown MetricMetadata.MetricType enum values (e.g. 99, 8) preserved alongside 0.
            let mut client = IngesterClient::new(ch.clone());
            let meta_req = cortexpb::WriteRequest {
                metadata: vec![
                    cortexpb::MetricMetadata {
                        r#type: 0,
                        metric_family_name: "unk_enum_metric".to_string(),
                        help: "same help".to_string(),
                        unit: "u".to_string(),
                    },
                    cortexpb::MetricMetadata {
                        r#type: 8,
                        metric_family_name: "unk_enum_metric".to_string(),
                        help: "same help".to_string(),
                        unit: "u".to_string(),
                    },
                    cortexpb::MetricMetadata {
                        r#type: 99,
                        metric_family_name: "unk_enum_metric".to_string(),
                        help: "same help".to_string(),
                        unit: "u".to_string(),
                    },
                ],
                ..Default::default()
            };
            client
                .push(with_tenant("t-meta-enum", meta_req))
                .await
                .expect("push unknown metadata enum");
            let meta_resp = client
                .metrics_metadata(with_tenant(
                    "t-meta-enum",
                    ingesterpb::MetricsMetadataRequest {
                        limit: 100,
                        metric: "unk_enum_metric".to_string(),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner();
            let mut types: Vec<i32> = meta_resp.metadata.iter().map(|m| m.r#type).collect();
            types.sort_unstable();

            // 3. Histogram zero_count oneof variant mismatch parity:
            //    int histogram with ZeroCountFloat(100.0) -> zero_count == 0
            //    float histogram with ZeroCountInt(100) -> zero_count == 0.0
            let h_req = cortexpb::WriteRequest {
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_static("__name__", "zc_int_mismatch")],
                        histograms: vec![cortexpb::Histogram {
                            count: Some(cortexpb::histogram::Count::CountInt(5)),
                            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(100.0)),
                            sum: 10.0,
                            schema: 0,
                            positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 1 }],
                            positive_deltas: vec![5],
                            timestamp: ts,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_static("__name__", "zc_float_mismatch")],
                        histograms: vec![cortexpb::Histogram {
                            count: Some(cortexpb::histogram::Count::CountFloat(5.0)),
                            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(100)),
                            sum: 10.0,
                            schema: 0,
                            positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 1 }],
                            positive_counts: vec![5.0],
                            timestamp: ts,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };
            client
                .push(with_tenant("t-zc", h_req))
                .await
                .expect("mismatched zero_count oneof must succeed like Go");

            let mut int_zc = u64::MAX;
            let mut float_zc = f64::NAN;
            for (metric, is_float) in [("zc_int_mismatch", false), ("zc_float_mismatch", true)] {
                let mut stream = client
                    .query_stream(with_tenant(
                        "t-zc",
                        ingesterpb::QueryRequest {
                            start_timestamp_ms: ts - 1000,
                            end_timestamp_ms: ts + 1000,
                            matchers: vec![ingesterpb::LabelMatcher {
                                r#type: ingesterpb::MatchType::Equal as i32,
                                name: "__name__".to_string(),
                                value: metric.to_string(),
                            }],
                            streaming_chunks_batch_size: 0,
                        },
                    ))
                    .await
                    .unwrap()
                    .into_inner();
                while let Some(msg) = stream.next().await {
                    let msg = msg.unwrap();
                    for sc in msg.streaming_series_chunks {
                        for c in sc.chunks {
                            if is_float {
                                let dec = mimir_tsdb::chunkenc::float_histogram::FloatHistogramChunk::from_bytes(c.data.to_vec());
                                let mut it = dec.iter();
                                let (_, fh) = it.next_float_histogram().unwrap().unwrap();
                                float_zc = fh.zero_count;
                            } else {
                                let dec = mimir_tsdb::chunkenc::histogram::HistogramChunk::from_bytes(c.data.to_vec());
                                let mut it = dec.iter();
                                let (_, ih) = it.next_histogram().unwrap().unwrap();
                                int_zc = ih.zero_count;
                            }
                        }
                    }
                }
            }
            results.push((*which, push_codes, types, vec![(int_zc, float_zc)]));
        }

        let go_res = &results[0];
        let rust_res = &results[1];
        assert_eq!(rust_res.1, go_res.1, "push codes mismatch");
        for (label, code) in &rust_res.1 {
            if label == "valid_no_tenant" {
                assert_eq!(*code, Code::Unknown, "{label}");
            } else {
                assert_eq!(*code, Code::Internal, "{label}");
            }
        }
        assert_eq!(rust_res.2, vec![0, 8, 99], "rust unknown metadata types");
        assert_eq!(rust_res.2, go_res.2, "metadata types Go vs Rust");
        assert_eq!(rust_res.3, vec![(0, 0.0)], "rust zero_count parity");
        assert_eq!(rust_res.3, go_res.3, "zero_count Go vs Rust");

        for (_, mut proc, _, _) in procs {
            proc.sigterm();
            let st = proc.wait_exit(Duration::from_secs(15)).expect("exit");
            assert!(st.success(), "{}", proc.log());
        }
    });
}

#[test]
fn sigterm_during_post_ingester_shutdown_waits_for_full_flush() {
    let tmp = tempfile::tempdir().unwrap();
    let http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();

    block_on_for_tests(async {
        for which in IMPLS {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();

            let put_started = Arc::new(Notify::new());
            let (allow_put_tx, allow_put_rx) = tokio::sync::watch::channel(false);
            let put_started_c = Arc::clone(&put_started);
            let once = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let hook = Arc::new(move |_path: String| {
                let put_started_c = Arc::clone(&put_started_c);
                let mut rx = allow_put_rx.clone();
                let once = Arc::clone(&once);
                async move {
                    if !once.swap(true, std::sync::atomic::Ordering::AcqRel) {
                        put_started_c.notify_one();
                        while !*rx.borrow_and_update() {
                            if rx.changed().await.is_err() {
                                break;
                            }
                        }
                    }
                }
                .boxed()
            });

            let s3_port = free_test_port();
            let s3 = FakeS3::start(s3_port, Some(hook)).await;
            let http = free_test_port();
            let grpc = free_test_port();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
usage_stats:
  enabled: false
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ing-0
    instance_addr: 127.0.0.1
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
{s3_blk}
  tsdb:
    dir: {tsdb}
    ship_interval: 1m
    flush_blocks_on_shutdown: false
activity_tracker:
  filepath: ""
"#,
                s3_blk = s3_blocks(&s3.endpoint()),
                tsdb = dir.join("tsdb").display(),
            );

            let mut proc = start_mimir(which, "ingester", &dir, &yaml, &[]);
            wait_ready(&http_client, &mut proc, http, Duration::from_secs(60)).await;

            let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut client = IngesterClient::new(ch);
            let ts = now_ms() - 5_000;
            client
                .push(with_tenant(
                    "t-shut",
                    cortexpb::WriteRequest {
                        timeseries: vec![cortexpb::TimeSeries {
                            labels: vec![cortexpb::LabelPair::from_static("__name__", "shut_metric")],
                            samples: vec![cortexpb::Sample {
                                value: 42.0,
                                timestamp_ms: ts,
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ))
                .await
                .unwrap();

            let shut_client = http_client.clone();
            let shut_fut = tokio::spawn(async move {
                shut_client
                    .post(format!("http://127.0.0.1:{http}/ingester/shutdown"))
                    .send()
                    .await
                    .unwrap()
                    .status()
            });

            tokio::time::timeout(Duration::from_secs(15), put_started.notified())
                .await
                .expect("s3 PUT started during /ingester/shutdown");
            proc.sigterm();

            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                proc.try_wait().is_none(),
                "{}: SIGTERM caused early exit mid-flush/upload!\n{}",
                which.name(),
                proc.log()
            );

            let _ = allow_put_tx.send(true);
            let status = tokio::time::timeout(Duration::from_secs(15), shut_fut)
                .await
                .expect("/ingester/shutdown completed")
                .unwrap();
            assert_eq!(status, StatusCode::NO_CONTENT, "{}", which.name());

            let exit = proc.wait_exit(Duration::from_secs(15)).expect("process exit");
            assert!(exit.success(), "{}: {}", which.name(), proc.log());

            let puts: Vec<String> = s3
                .requests()
                .into_iter()
                .filter(|r| r.method == "PUT")
                .map(|r| r.path)
                .collect();
            assert!(
                puts.iter().any(|p| p.contains("meta.json")),
                "{}: expected meta.json uploaded to FakeS3 before exit, got {puts:?}",
                which.name()
            );
        }
    });
}

#[test]
fn kafka_shutdown_writes_offset_catalogue_json() {
    let tmp = tempfile::tempdir().unwrap();
    let http_client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();

    block_on_for_tests(async {
        for which in IMPLS {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();

            let ts = now_ms() - 5_000;
            let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
                topic: "mimir-ingest".to_string(),
                partitions: 1,
                compression: None,
                records: vec![KfakeRecordInput {
                    partition: 0,
                    tenant: "t-cat".to_string(),
                    version: 2,
                    compression: None,
                    write_request: WriteRequestJson {
                        mode: String::new(),
                        source: "API".to_string(),
                        skip_label_validation: false,
                        skip_label_count_validation: false,
                        timeseries: vec![TimeSeriesJson {
                            labels: vec![LabelJson {
                                name: "__name__".to_string(),
                                value: "kafka_cat_metric".to_string(),
                            }],
                            samples: vec![SampleJson {
                                timestamp_ms: ts,
                                value: 7.5,
                            }],
                            ..Default::default()
                        }],
                        metadata: Vec::new(),
                    },
                }],
            })
            .expect("spawn kfake");

            let http = free_test_port();
            let grpc = free_test_port();
            let yaml = format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
usage_stats:
  enabled: false
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-zone-a-0
    instance_addr: 127.0.0.1
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
ingest_storage:
  enabled: true
  kafka:
    address: {addr}
    topic: mimir-ingest
    auto_create_topic_enabled: false
    consume_from_position_at_startup: start
    wait_strong_read_consistency_timeout: 2s
    last_produced_offset_poll_interval: 100ms
    consumer_group_offset_commit_interval: 100ms
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1m
    flush_blocks_on_shutdown: true
    offset_catalogue:
      enabled: true
activity_tracker:
  filepath: ""
"#,
                addr = broker.ready.broker_addr,
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );

            let mut proc = start_mimir(which, "ingester", &dir, &yaml, &[]);
            wait_ready(&http_client, &mut proc, http, Duration::from_secs(60)).await;

            let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut client = IngesterClient::new(ch);
            let dl = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                let stats = client
                    .user_stats(with_tenant("t-cat", ingesterpb::UserStatsRequest::default()))
                    .await
                    .unwrap()
                    .into_inner();
                if stats.num_series >= 1 {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < dl,
                    "{}: Kafka record not ingested",
                    which.name()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            let resp = http_client
                .post(format!("http://127.0.0.1:{http}/ingester/shutdown"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::NO_CONTENT, "{}", which.name());

            proc.sigterm();
            let exit = proc.wait_exit(Duration::from_secs(15)).expect("exit");
            assert!(exit.success(), "{}: {}", which.name(), proc.log());

            let cat_path = dir.join("tsdb").join("t-cat").join("offset-catalogue.json");
            assert!(
                cat_path.exists(),
                "{}: expected {} to exist after Kafka shutdown flush!\n{}",
                which.name(),
                cat_path.display(),
                proc.log()
            );
            let cat_json: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&cat_path).unwrap()).unwrap();
            let data = cat_json["data"]
                .as_object()
                .expect("offset-catalogue.json must contain data map");
            assert!(
                !data.is_empty(),
                "{}: offset-catalogue.json had empty data: {cat_json}",
                which.name()
            );
        }
    });
}