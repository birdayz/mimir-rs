//! Wave 35: Clean shutdown, crash/graceful restart, Kubernetes rolling updates,
//! and bidirectional Go <-> Rust PVC handoff conformance suite.
//!
//! Exercises the real compiled Go target/go-mimir and Rust target/release/mimir binaries
//! across:
//! 1. SIGTERM during startup (observe_period) and double-SIGTERM immediate termination (CMD-03).
//! 2. -shutdown-delay: /ready -> 503 + Connection: close + gRPC NOT_SERVING while in-flight
//!    gRPC Push/QueryStream succeed AND an un-prefaced raw TCP socket on the gRPC port does
//!    not hang shutdown (CMD-01, CMD-02).
//! 3. SIGKILL mid-write + crash debris (.tmp block/checkpoint/chunk_snapshot dirs) recovery
//!    after snapshot + WAL checkpoint (TSDB-1..10).
//! 4. POST /ingester/prepare-shutdown marker persistence across SIGKILL + restart, and
//!    idempotent POST /ingester/shutdown + SIGTERM.
//! 5. Live 3-node RF=3 mixed Go+Rust memberlist rolling restart with tokens_file_path,
//!    unregister_on_shutdown=false, continuous quorum writes, and zero acknowledged data loss.
//! 6. Bidirectional Go -> Rust -> Go PVC handoff under BOTH graceful SIGTERM and SIGKILL
//!    (floats, native histograms, OOO samples, and exemplars).
//! 7. Kafka ingest_storage clean shutdown/restart (kafka-offset.json persistence) and fast
//!    SIGTERM cancellation when Kafka broker is unreachable at startup (STOR-01).

#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    KfakeBrokerRequest, KfakeRecordInput, LabelJson, SampleJson, SpawnedKfakeBroker,
    TimeSeriesJson, WriteRequestJson,
};
use mimir_conformance::twin::{
    free_tcp_ports, grpc_health, http_get, ingester_ring, services, start_mimir, wait_ready,
    FakeS3, Impl, Proc,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const BOTH: [Impl; 2] = [Impl::Go, Impl::Rust];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .expect("build http client")
}

fn with_tenant<T>(tenant: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert(
        LOWER_ORG_ID_HEADER_NAME,
        MetadataValue::try_from(tenant).expect("valid tenant"),
    );
    req
}

fn assert_exit_ok(proc: &mut Proc, timeout: Duration) {
    match proc.wait_exit(timeout) {
        Some(s) => assert!(s.success(), "{} exited with {s}:\n{}", proc.name, proc.log()),
        None => panic!("{} did not exit within {timeout:?}:\n{}", proc.name, proc.log()),
    }
}

fn sigkill_and_wait(proc: &mut Proc) {
    proc.signal("KILL");
    let status = proc
        .wait_exit(Duration::from_secs(10))
        .unwrap_or_else(|| panic!("{} did not die after SIGKILL", proc.name));
    assert!(
        !status.success(),
        "{} unexpectedly exited 0 after SIGKILL",
        proc.name
    );
}

fn wire_enc_to_tsdb(enc: i32) -> Encoding {
    match enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => panic!("unexpected wire chunk encoding {other}"),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum DecodedVal {
    Float(i64, f64),
    IntHist(i64, i32, u64, f64),
    FloatHist(i64, i32, f64, f64),
}

type LabelPairs = Vec<(String, String)>;

fn canonical_labels(labels: Vec<cortexpb::LabelPair>) -> LabelPairs {
    let mut out: LabelPairs = labels
        .into_iter()
        .map(|l| {
            (
                String::from_utf8_lossy(&l.name).into_owned(),
                String::from_utf8_lossy(&l.value).into_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

async fn connect_ingester(grpc_port: u16) -> IngesterClient<Channel> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("valid uri")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .connect()
        .await
        .expect("connect gRPC");
    IngesterClient::new(ch)
}

async fn push_timeseries(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    timeseries: Vec<cortexpb::TimeSeries>,
) -> Result<(), tonic::Status> {
    client
        .push(with_tenant(
            tenant,
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries,
                ..Default::default()
            },
        ))
        .await
        .map(|_| ())
}

async fn query_all_series(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    start_ms: i64,
    end_ms: i64,
) -> BTreeMap<LabelPairs, Vec<DecodedVal>> {
    let req = with_tenant(
        tenant,
        ingesterpb::QueryRequest {
            start_timestamp_ms: start_ms,
            end_timestamp_ms: end_ms,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::RegexMatch as i32,
                name: "__name__".to_string(),
                value: ".+".to_string(),
            }],
            streaming_chunks_batch_size: 8,
        },
    );
    let mut stream = client
        .query_stream(req)
        .await
        .expect("query_stream")
        .into_inner();
    let mut series_labels: Vec<LabelPairs> = Vec::new();
    let mut saw_end = false;
    let mut chunks_by_idx: BTreeMap<u64, Vec<DecodedVal>> = BTreeMap::new();
    while let Some(item) = stream.next().await {
        let msg = item.expect("stream item");
        if !msg.streaming_series.is_empty() || msg.is_end_of_series_stream {
            for s in msg.streaming_series {
                series_labels.push(canonical_labels(s.labels));
            }
            if msg.is_end_of_series_stream {
                saw_end = true;
            }
        } else {
            for sc in msg.streaming_series_chunks {
                let entry = chunks_by_idx.entry(sc.series_index).or_default();
                for ch in sc.chunks {
                    let enc = wire_enc_to_tsdb(ch.encoding);
                    let chunk = EncodedChunk::from_bytes(enc, ch.data.to_vec()).expect("chunk");
                    for s in decode_chunk_samples(&chunk).expect("decode samples") {
                        match s.value {
                            SampleValue::Float(v) => entry.push(DecodedVal::Float(s.t, v)),
                            SampleValue::Histogram(h) => {
                                entry.push(DecodedVal::IntHist(s.t, h.schema, h.count, h.sum))
                            }
                            SampleValue::FloatHistogram(fh) => {
                                entry.push(DecodedVal::FloatHist(s.t, fh.schema, fh.count, fh.sum))
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(saw_end, "expected is_end_of_series_stream");
    let mut out = BTreeMap::new();
    for (idx, lbls) in series_labels.into_iter().enumerate() {
        let mut vals = chunks_by_idx.remove(&(idx as u64)).unwrap_or_default();
        vals.sort_by_key(|v| match *v {
            DecodedVal::Float(t, _)
            | DecodedVal::IntHist(t, _, _, _)
            | DecodedVal::FloatHist(t, _, _, _) => t,
        });
        out.insert(lbls, vals);
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn simple_ingester_yaml(
    http: u16,
    grpc: u16,
    dir: &Path,
    id: &str,
    ring_extra: &str,
    tsdb_extra: &str,
    limits_extra: &str,
    top_extra: &str,
) -> String {
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
    instance_id: {id}
    instance_addr: 127.0.0.1
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
{ring_extra}blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1m
{tsdb_extra}limits:
  max_global_exemplars_per_user: 1000
{limits_extra}activity_tracker:
  filepath: ""
{top_extra}"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

fn s3_ingester_yaml(
    http: u16,
    grpc: u16,
    dir: &Path,
    id: &str,
    s3_endpoint: &str,
    ring_extra: &str,
    tsdb_extra: &str,
) -> String {
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
    instance_id: {id}
    instance_addr: 127.0.0.1
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
{ring_extra}blocks_storage:
  backend: s3
  s3:
    endpoint: {s3_endpoint}
    bucket_name: blocks
    region: us-east-1
    access_key_id: conformance
    secret_access_key: conformance-secret
    insecure: true
  tsdb:
    dir: {tsdb}
    ship_interval: 1m
{tsdb_extra}activity_tracker:
  filepath: ""
"#,
        tsdb = dir.join("tsdb").display(),
    )
}

fn sample_histogram(ts: i64) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(5)),
        sum: 12.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
        negative_spans: Vec::new(),
        negative_deltas: Vec::new(),
        negative_counts: Vec::new(),
        positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 2 }],
        positive_deltas: vec![2, 0],
        positive_counts: Vec::new(),
        reset_hint: 0,
        timestamp: ts,
        custom_values: Vec::new(),
    }
}

// =============================================================================================
// 1. SIGTERM during startup (observe_period) & double-SIGTERM immediate termination (CMD-03)
// =============================================================================================

#[test]
fn w35_sigterm_during_startup_and_double_sigterm() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        // 1a. SIGTERM while ingester is stuck in 30s observe_period -> exits 0 cleanly!
        for which in BOTH {
            let ports = free_tcp_ports(2);
            let dir = tmp.path().join(format!("{}-startup-term", which.name()));
            let yaml = simple_ingester_yaml(
                ports[0],
                ports[1],
                &dir,
                "ing-startup",
                "    observe_period: 30s\n",
                "",
                "",
                "shutdown_delay: 400ms\n",
            );
            let mut proc = start_mimir(which, "startup-term", &dir, &yaml, &[]);

            // Wait until HTTP /ready is listening and returning 503 (still Starting).
            let dl = Instant::now() + Duration::from_secs(20);
            loop {
                if let Ok((code, _, _)) = http_get(&client, ports[0], "/ready", false).await {
                    assert_eq!(code, 503, "{} expected 503 during observe_period", proc.name);
                    break;
                }
                assert!(Instant::now() < dl, "{} /ready never came up", proc.name);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            let t0 = Instant::now();
            proc.sigterm();
            assert_exit_ok(&mut proc, Duration::from_secs(15));
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "{} took too long ({:?}) to abort startup observe_period",
                proc.name,
                t0.elapsed()
            );
        }

        // 1b. Double SIGTERM during long -shutdown-delay terminates immediately (CMD-03).
        for which in BOTH {
            let ports = free_tcp_ports(2);
            let dir = tmp.path().join(format!("{}-double-term", which.name()));
            let yaml = simple_ingester_yaml(
                ports[0],
                ports[1],
                &dir,
                "ing-double-term",
                "",
                "",
                "",
                "shutdown_delay: 30s\n",
            );
            let mut proc = start_mimir(which, "double-term", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, ports[0], Duration::from_secs(30)).await;

            // 1st SIGTERM enters 30s shutdown_delay and flips /ready -> 503.
            proc.sigterm();
            let dl = Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok((503, _, body)) = http_get(&client, ports[0], "/ready", false).await {
                    if body.contains("Application is stopping") {
                        break;
                    }
                }
                assert!(Instant::now() < dl, "{} did not flip /ready to stopping", proc.name);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(
                proc.try_wait().is_none(),
                "{} exited prematurely during 30s shutdown_delay",
                proc.name
            );

            // 2nd SIGTERM must immediately kill the process instead of waiting 30s!
            let t0 = Instant::now();
            proc.sigterm();
            let status = proc
                .wait_exit(Duration::from_secs(5))
                .unwrap_or_else(|| {
                    panic!(
                        "{} swallowed 2nd SIGTERM during shutdown_delay!\n{}",
                        proc.name,
                        proc.log()
                    )
                });
            assert!(
                t0.elapsed() < Duration::from_secs(5),
                "{} 2nd SIGTERM took {:?}",
                proc.name,
                t0.elapsed()
            );
            assert!(
                !status.success(),
                "{} 2nd SIGTERM should exit non-zero/signaled, got {status}",
                proc.name
            );
        }
    });
}

// =============================================================================================
// 2. -shutdown-delay: /ready 503 while in-flight RPCs succeed + un-prefaced TCP does not hang
// =============================================================================================

#[test]
fn w35_shutdown_delay_ready_503_inflight_rpc_and_unprefaced_grpc_tcp() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        for which in BOTH {
            let ports = free_tcp_ports(2);
            let dir = tmp.path().join(which.name());
            let yaml = simple_ingester_yaml(
                ports[0],
                ports[1],
                &dir,
                "ing-drain",
                "",
                "",
                "",
                "shutdown_delay: 5s\n",
            );
            let mut proc = start_mimir(which, "drain", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, ports[0], Duration::from_secs(30)).await;
            assert_eq!(grpc_health(ports[1]).await.unwrap(), 1, "{} SERVING", proc.name);

            // Keep an idle HTTP/2 gRPC channel open across shutdown on both Go and Rust, plus
            // an un-prefaced raw TCP socket on Rust (verifying CMD-01 does not hang indefinitely;
            // grpc-go waits its 120s ConnectionTimeout for un-prefaced raw sockets).
            let _idle_grpc = connect_ingester(ports[1]).await;
            let _raw_stalled_grpc = (which == Impl::Rust).then(|| {
                TcpStream::connect(format!("127.0.0.1:{}", ports[1]))
                    .expect("raw tcp connect to gRPC port")
            });

            // Send SIGTERM -> enters 5s shutdown_delay; poll until /ready flips to 503.
            proc.sigterm();
            let dl = Instant::now() + Duration::from_secs(5);
            let (code, headers, body) = loop {
                if let Ok(r) = http_get(&client, ports[0], "/ready", false).await {
                    if r.0 == 503 {
                        break r;
                    }
                }
                assert!(Instant::now() < dl, "{} did not flip /ready to 503", proc.name);
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            assert_eq!(code, 503, "{} /ready during shutdown_delay", proc.name);
            assert_eq!(body, "Application is stopping\n", "{} body", proc.name);
            let conn = headers
                .get("connection")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_ascii_lowercase();
            assert_eq!(conn, "close", "{} expected Connection: close", proc.name);
            assert_eq!(
                grpc_health(ports[1]).await.unwrap(),
                2,
                "{} gRPC health NOT_SERVING during shutdown_delay",
                proc.name
            );

            // In-flight gRPC Push + QueryStream MUST STILL SUCCEED during shutdown_delay!
            let ts = now_ms();
            let mut grpc = connect_ingester(ports[1]).await;
            push_timeseries(
                &mut grpc,
                "t-drain",
                vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "drain_metric")],
                    samples: vec![cortexpb::Sample { value: 42.0, timestamp_ms: ts }],
                    ..Default::default()
                }],
            )
            .await
            .unwrap_or_else(|e| panic!("{} Push failed during shutdown_delay: {e}", proc.name));

            let series = query_all_series(&mut grpc, "t-drain", ts - 10_000, ts + 10_000).await;
            assert_eq!(series.len(), 1, "{} QueryStream during shutdown_delay", proc.name);
            drop(grpc);

            // Must exit cleanly within 15s even while _raw_stalled_grpc stays open!
            assert_exit_ok(&mut proc, Duration::from_secs(15));
        }
    });
}

// =============================================================================================
// 3. SIGKILL mid-write + flush + OOO + .tmp debris crash recovery (TSDB-1..10; memory-snapshot
//    & checkpoint edge cases are covered by Head unit tests since memory_snapshot_on_shutdown
//    is explicitly rejected at config validation)
// =============================================================================================

#[test]
fn w35_sigkill_mid_write_and_dirty_tmp_crash_recovery() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        let base_ts = now_ms() - 120_000;

        for which in BOTH {
            let ports = free_tcp_ports(2);
            let dir = tmp.path().join(which.name());
            let yaml = simple_ingester_yaml(
                ports[0],
                ports[1],
                &dir,
                "ing-crash",
                "    unregister_on_shutdown: false\n",
                "",
                "  out_of_order_time_window: 1h\n",
                "",
            );

            // Boot #1: write wave 1 (float + histogram) and do a clean shutdown so WAL +
            // chunks_head are flushed cleanly before Boot #2.
            let mut proc1 = start_mimir(which, "crash-1", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc1, ports[0], Duration::from_secs(30)).await;
            let mut grpc = connect_ingester(ports[1]).await;
            push_timeseries(
                &mut grpc,
                "t-crash",
                vec![
                    cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "crash_float"),
                            cortexpb::LabelPair::from_static("wave", "all"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 10.0,
                            timestamp_ms: base_ts,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_static("__name__", "crash_hist")],
                        histograms: vec![sample_histogram(base_ts)],
                        ..Default::default()
                    },
                ],
            )
            .await
            .unwrap();
            drop(grpc);
            proc1.sigterm();
            assert_exit_ok(&mut proc1, Duration::from_secs(30));

            // Boot #2: write wave 2 -> flush (compacts block) -> write wave 3
            // (including an OOO sample) -> plant simulated SIGKILL crash debris -> SIGKILL!
            let mut proc2 = start_mimir(which, "crash-2", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc2, ports[0], Duration::from_secs(30)).await;
            let mut grpc = connect_ingester(ports[1]).await;
            push_timeseries(
                &mut grpc,
                "t-crash",
                vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "crash_float"),
                        cortexpb::LabelPair::from_static("wave", "all"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 20.0,
                        timestamp_ms: base_ts + 30_000,
                    }],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();

            // Force flush/compact so a block + WAL checkpoint are created on top of Boot #1's snapshot.
            let flush_resp = client
                .post(format!("http://127.0.0.1:{}/ingester/flush?wait=true", ports[0]))
                .send()
                .await
                .unwrap();
            assert_eq!(flush_resp.status(), StatusCode::NO_CONTENT);

            // Wave 3: active WAL write + OOO sample right before SIGKILL!
            push_timeseries(
                &mut grpc,
                "t-crash",
                vec![
                    cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "crash_float"),
                            cortexpb::LabelPair::from_static("wave", "all"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 30.0,
                            timestamp_ms: base_ts + 60_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "crash_ooo"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 100.0,
                            timestamp_ms: base_ts + 50_000,
                        }],
                        ..Default::default()
                    },
                ],
            )
            .await
            .unwrap();
            push_timeseries(
                &mut grpc,
                "t-crash",
                vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "crash_ooo")],
                    samples: vec![cortexpb::Sample {
                        value: 99.0,
                        timestamp_ms: base_ts + 10_000, // OOO sample!
                    }],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
            drop(grpc);

            // Crash abruptly with SIGKILL!
            sigkill_and_wait(&mut proc2);

            // Plant simulated mid-compaction / mid-snapshot / mid-checkpoint crash debris.
            let tenant_dir = dir.join("tsdb").join("t-crash");
            let fake_block_tmp = tenant_dir.join("01J00000000000000000000001.tmp-for-creation");
            let fake_snap_tmp = tenant_dir.join("chunk_snapshot.999999.0000000001.tmp");
            let fake_cp_tmp = tenant_dir.join("wal").join("checkpoint.999999.tmp");
            std::fs::create_dir_all(&fake_block_tmp).unwrap();
            std::fs::create_dir_all(&fake_snap_tmp).unwrap();
            std::fs::create_dir_all(&fake_cp_tmp).unwrap();

            // Boot #3: must recover cleanly from SIGKILL + snapshot/checkpoint + crash debris!
            let mut proc3 = start_mimir(which, "crash-3", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc3, ports[0], Duration::from_secs(30)).await;

            assert!(!fake_block_tmp.exists(), "{} leaked tmp block dir", proc3.name);
            assert!(!fake_snap_tmp.exists(), "{} leaked tmp chunk_snapshot dir", proc3.name);
            assert!(!fake_cp_tmp.exists(), "{} leaked tmp checkpoint dir", proc3.name);

            let mut grpc = connect_ingester(ports[1]).await;
            let got = query_all_series(&mut grpc, "t-crash", base_ts - 10_000, base_ts + 90_000).await;
            let float_key = vec![
                ("__name__".to_string(), "crash_float".to_string()),
                ("wave".to_string(), "all".to_string()),
            ];
            assert_eq!(
                got.get(&float_key),
                Some(&vec![
                    DecodedVal::Float(base_ts, 10.0),
                    DecodedVal::Float(base_ts + 30_000, 20.0),
                    DecodedVal::Float(base_ts + 60_000, 30.0),
                ]),
                "{}: lost float samples across snapshot+checkpoint+SIGKILL: {got:?}",
                proc3.name
            );
            let ooo_key = vec![("__name__".to_string(), "crash_ooo".to_string())];
            assert_eq!(
                got.get(&ooo_key),
                Some(&vec![
                    DecodedVal::Float(base_ts + 10_000, 99.0),
                    DecodedVal::Float(base_ts + 50_000, 100.0),
                ]),
                "{}: lost OOO samples across SIGKILL: {got:?}",
                proc3.name
            );
            let hist_key = vec![("__name__".to_string(), "crash_hist".to_string())];
            assert_eq!(
                got.get(&hist_key),
                Some(&vec![DecodedVal::IntHist(base_ts, 1, 5, 12.5)]),
                "{}: lost native histogram across SIGKILL: {got:?}",
                proc3.name
            );
            drop(grpc);

            proc3.sigterm();
            assert_exit_ok(&mut proc3, Duration::from_secs(30));
        }
    });
}

// =============================================================================================
// 4. /ingester/prepare-shutdown marker survives SIGKILL + /ingester/shutdown idempotency
// =============================================================================================

#[test]
fn w35_prepare_shutdown_marker_survives_sigkill_and_http_shutdown_idempotency() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        // 4a. prepare-shutdown marker survives SIGKILL and causes flush + marker removal on SIGTERM.
        for which in BOTH {
            let ports = free_tcp_ports(3);
            let s3 = FakeS3::start(ports[2], None).await;
            let dir = tmp.path().join(format!("{}-prep", which.name()));
            let yaml = s3_ingester_yaml(
                ports[0],
                ports[1],
                &dir,
                "ing-prep",
                &s3.endpoint(),
                "    unregister_on_shutdown: false\n",
                "    flush_blocks_on_shutdown: false\n",
            );

            let mut proc1 = start_mimir(which, "prep-1", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc1, ports[0], Duration::from_secs(30)).await;

            let (code, _, body) = http_get(&client, ports[0], "/ingester/prepare-shutdown", false)
                .await
                .unwrap();
            assert_eq!(code, 200);
            assert_eq!(body, "unset\n", "{} initial prepare-shutdown", proc1.name);

            let ts = now_ms();
            let mut grpc = connect_ingester(ports[1]).await;
            push_timeseries(
                &mut grpc,
                "t-prep",
                vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "prep_metric")],
                    samples: vec![cortexpb::Sample { value: 7.0, timestamp_ms: ts }],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
            drop(grpc);

            let post = client
                .post(format!("http://127.0.0.1:{}/ingester/prepare-shutdown", ports[0]))
                .send()
                .await
                .unwrap();
            assert_eq!(post.status(), StatusCode::NO_CONTENT);

            let marker = dir.join("tsdb").join("shutdown-requested.txt");
            assert!(marker.exists(), "{} marker must exist on disk", proc1.name);

            // Crash with SIGKILL before shutdown!
            sigkill_and_wait(&mut proc1);
            assert!(marker.exists(), "{} marker must survive SIGKILL", proc1.name);

            // Restart on same PVC -> marker MUST be detected on startup.
            let mut proc2 = start_mimir(which, "prep-2", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc2, ports[0], Duration::from_secs(30)).await;

            let (code, _, body) = http_get(&client, ports[0], "/ingester/prepare-shutdown", false)
                .await
                .unwrap();
            assert_eq!(code, 200);
            assert_eq!(body, "set\n", "{} marker must remain set after restart", proc2.name);

            let (_, _, metrics) = http_get(&client, ports[0], "/metrics", false).await.unwrap();
            assert!(
                metrics.contains("cortex_ingester_prepare_shutdown_requested 1"),
                "{} missing prepare_shutdown metric after restart",
                proc2.name
            );

            // Clean SIGTERM must now flush & ship blocks to S3 despite flush_blocks_on_shutdown=false
            // AND delete shutdown-requested.txt!
            proc2.sigterm();
            assert_exit_ok(&mut proc2, Duration::from_secs(30));

            let puts: Vec<_> = s3
                .requests()
                .into_iter()
                .filter(|r| r.method == "PUT" && r.path.starts_with("/blocks/t-prep/"))
                .collect();
            assert!(
                !puts.is_empty(),
                "{}: expected blocks shipped to S3 on prepared shutdown:\n{}",
                proc2.name,
                proc2.log()
            );
            assert!(
                !marker.exists(),
                "{}: marker file must be removed after clean prepared shutdown",
                proc2.name
            );
        }

        // 4b. POST /ingester/shutdown stops ingester, keeps HTTP/gRPC alive, is idempotent,
        // and clean SIGTERM exits 0 afterwards.
        for which in BOTH {
            let ports = free_tcp_ports(3);
            let s3 = FakeS3::start(ports[2], None).await;
            let dir = tmp.path().join(format!("{}-http-shut", which.name()));
            let yaml = s3_ingester_yaml(
                ports[0],
                ports[1],
                &dir,
                "ing-http-shut",
                &s3.endpoint(),
                "    unregister_on_shutdown: false\n",
                "    flush_blocks_on_shutdown: false\n",
            );

            let mut proc = start_mimir(which, "http-shut", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, ports[0], Duration::from_secs(30)).await;

            let ts = now_ms();
            let mut grpc = connect_ingester(ports[1]).await;
            push_timeseries(
                &mut grpc,
                "t-shut",
                vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "shut_metric")],
                    samples: vec![cortexpb::Sample { value: 3.5, timestamp_ms: ts }],
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
            drop(grpc);

            // 1st POST /ingester/shutdown -> flushes & terminates ingester-service, HTTP stays up.
            let resp1 = client
                .post(format!("http://127.0.0.1:{}/ingester/shutdown", ports[0]))
                .send()
                .await
                .unwrap();
            assert_eq!(resp1.status(), StatusCode::NO_CONTENT);

            let svcs = services(&client, ports[0]).await.unwrap();
            assert_eq!(
                svcs.get("ingester-service").map(String::as_str),
                Some("Terminated"),
                "{}: ingester-service must be Terminated after /ingester/shutdown",
                proc.name
            );
            let (ready_code, _, _) = http_get(&client, ports[0], "/ready", false).await.unwrap();
            assert_eq!(ready_code, 503, "{} /ready must be 503 after /ingester/shutdown", proc.name);

            let puts: Vec<_> = s3
                .requests()
                .into_iter()
                .filter(|r| r.method == "PUT" && r.path.starts_with("/blocks/t-shut/"))
                .collect();
            assert!(!puts.is_empty(), "{} expected S3 block upload on /ingester/shutdown", proc.name);

            // 2nd POST /ingester/shutdown must be idempotent (204, no hang/panic).
            let resp2 = client
                .post(format!("http://127.0.0.1:{}/ingester/shutdown", ports[0]))
                .send()
                .await
                .unwrap();
            assert_eq!(resp2.status(), StatusCode::NO_CONTENT);

            // Process is still alive until SIGTERM, then exits 0 cleanly.
            assert!(proc.try_wait().is_none(), "{} should remain alive until SIGTERM", proc.name);
            proc.sigterm();
            assert_exit_ok(&mut proc, Duration::from_secs(20));
        }
    });
}

// =============================================================================================
// 5. RF=3 Mixed Go+Rust Memberlist Rolling Restart with Continuous Quorum Writes
// =============================================================================================

struct ClusterNode {
    which: Impl,
    id: String,
    http: u16,
    grpc: u16,
    dir: std::path::PathBuf,
    yaml: String,
    proc: Option<Proc>,
}

#[test]
fn w35_rf3_mixed_memberlist_rolling_restart_zero_data_loss() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        let ports = free_tcp_ports(9);

        let specs = [
            (Impl::Rust, "ing-0", ports[0], ports[1], ports[2]),
            (Impl::Rust, "ing-1", ports[3], ports[4], ports[5]),
            (Impl::Go, "ing-2", ports[6], ports[7], ports[8]),
        ];
        let join_addrs: Vec<String> = specs
            .iter()
            .map(|(_, _, _, _, ml)| format!("127.0.0.1:{ml}"))
            .collect();

        let mut nodes: Vec<ClusterNode> = specs
            .iter()
            .map(|&(which, id, http, grpc, ml)| {
                let dir = tmp.path().join(id);
                std::fs::create_dir_all(dir.join("tsdb")).unwrap();
                std::fs::create_dir_all(dir.join("bucket")).unwrap();
                let join_yaml: String = join_addrs
                    .iter()
                    .map(|a| format!("    - {a}\n"))
                    .collect();
                let tokens_path = dir.join("tsdb").join("tokens");
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
memberlist:
  node_name: {id}
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
  gossip_interval: 100ms
  leave_timeout: 2s
  abort_if_cluster_join_fails: false
  join_members:
{join_yaml}ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: {id}
    instance_addr: 127.0.0.1
    replication_factor: 3
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
    heartbeat_period: 1s
    heartbeat_timeout: 15s
    unregister_on_shutdown: false
    tokens_file_path: {tokens_path}
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1m
activity_tracker:
  filepath: ""
"#,
                    tokens_path = tokens_path.display(),
                    bucket = dir.join("bucket").display(),
                    tsdb = dir.join("tsdb").display(),
                );
                let proc = start_mimir(which, id, &dir, &yaml, &[]);
                ClusterNode {
                    which,
                    id: id.to_string(),
                    http,
                    grpc,
                    dir,
                    yaml,
                    proc: Some(proc),
                }
            })
            .collect();

        for n in &mut nodes {
            let p = n.proc.as_mut().unwrap();
            wait_ready(&client, p, n.http, Duration::from_secs(45)).await;
        }

        // Wait until EVERY node sees all 3 instances ACTIVE in /ingester/ring.
        async fn wait_all_active(client: &reqwest::Client, nodes: &[ClusterNode]) {
            let dl = Instant::now() + Duration::from_secs(30);
            loop {
                let mut all_ok = true;
                for n in nodes {
                    match ingester_ring(client, n.http).await {
                        Ok(r) if r.len() == 3
                            && r.values().all(|(st, toks)| st == "ACTIVE" && *toks == 16) => {}
                        _ => {
                            all_ok = false;
                            break;
                        }
                    }
                }
                if all_ok {
                    return;
                }
                assert!(Instant::now() < dl, "cluster did not converge to 3x ACTIVE");
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        }
        wait_all_active(&client, &nodes).await;

        // Verify tokens files exist on disk for every node.
        for n in &nodes {
            assert!(
                n.dir.join("tsdb").join("tokens").exists(),
                "{} missing tokens_file_path on disk",
                n.id
            );
        }

        // Start continuous RF=3 quorum writer pushing numbered samples to all 3 nodes.
        let stop_writer = Arc::new(AtomicBool::new(false));
        type SampleList = Vec<(i64, f64)>;
        let acked: Arc<Mutex<SampleList>> = Arc::new(Mutex::new(Vec::new()));
        let per_node_acked: Arc<Mutex<Vec<SampleList>>> =
            Arc::new(Mutex::new(vec![Vec::new(); nodes.len()]));
        let grpc_ports: Vec<u16> = nodes.iter().map(|n| n.grpc).collect();
        let base_ts = now_ms() - 60_000;
        let writer_handle = {
            let stop = Arc::clone(&stop_writer);
            let acked = Arc::clone(&acked);
            let per_node_acked = Arc::clone(&per_node_acked);
            tokio::spawn(async move {
                let mut seq: i64 = 0;
                while !stop.load(Ordering::Acquire) {
                    // Advance by 50ms per ~80ms iteration so timestamps never outrun wall clock
                    // or cross creation_grace_period on slow 2-core CI runners.
                    let ts = base_ts + seq * 50;
                    let val = seq as f64 + 0.5;
                    let mut ok_nodes = Vec::with_capacity(grpc_ports.len());
                    for (node_idx, &gp) in grpc_ports.iter().enumerate() {
                        let ch_res = Channel::from_shared(format!("http://127.0.0.1:{gp}"))
                            .unwrap()
                            .connect_timeout(Duration::from_millis(500))
                            .timeout(Duration::from_secs(2))
                            .connect()
                            .await;
                        if let Ok(ch) = ch_res {
                            let mut c = IngesterClient::new(ch);
                            let res = push_timeseries(
                                &mut c,
                                "t-roll",
                                vec![cortexpb::TimeSeries {
                                    labels: vec![cortexpb::LabelPair::from_static(
                                        "__name__",
                                        "rolling_counter",
                                    )],
                                    samples: vec![cortexpb::Sample {
                                        value: val,
                                        timestamp_ms: ts,
                                    }],
                                    ..Default::default()
                                }],
                            )
                            .await;
                            if res.is_ok() {
                                ok_nodes.push(node_idx);
                            }
                        }
                    }
                    if !ok_nodes.is_empty() {
                        let mut per_node = per_node_acked.lock().unwrap();
                        for idx in &ok_nodes {
                            per_node[*idx].push((ts, val));
                        }
                    }
                    // RF=3 quorum = at least 2 of 3 replicas acknowledged.
                    if ok_nodes.len() >= 2 {
                        acked.lock().unwrap().push((ts, val));
                    }
                    seq += 1;
                    tokio::time::sleep(Duration::from_millis(80)).await;
                }
            })
        };

        // Ensure some pre-roll writes succeed.
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Rolling restart all 3 nodes one by one (ing-0 Rust -> ing-1 Rust -> ing-2 Go),
        // keeping the exact same ports, PVC, tokens_file_path, and instance_id!
        for idx in 0..nodes.len() {
            {
                let p = nodes[idx].proc.as_mut().unwrap();
                p.sigterm();
                assert_exit_ok(p, Duration::from_secs(30));
            }
            nodes[idx].proc = None;

            // Immediately restart on the same ports & PVC (tests immediate listener drop +
            // same-second LEAVING -> ACTIVE timestamp bump + memberlist rejoin!).
            let restarted = start_mimir(
                nodes[idx].which,
                &nodes[idx].id,
                &nodes[idx].dir,
                &nodes[idx].yaml,
                &[],
            );
            nodes[idx].proc = Some(restarted);
            let http = nodes[idx].http;
            wait_ready(&client, nodes[idx].proc.as_mut().unwrap(), http, Duration::from_secs(45))
                .await;
            wait_all_active(&client, &nodes).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        stop_writer.store(true, Ordering::Release);
        writer_handle.await.expect("writer join");

        let expected: Vec<(i64, f64)> = acked.lock().unwrap().clone();
        assert!(
            expected.len() >= 15,
            "expected continuous quorum writes during rolling restart, got only {}",
            expected.len()
        );

        // Verify BOTH per-replica WAL durability (each individual node — including both Rust
        // nodes — must still serve every sample it individually acknowledged, so Go cannot mask
        // a Rust WAL loss) AND RF=3 quorum union durability across all 3 replicas.
        let per_node_expected: Vec<Vec<(i64, f64)>> = per_node_acked.lock().unwrap().clone();
        let mut union_samples: BTreeSet<(i64, u64)> = BTreeSet::new();
        let series_key = vec![("__name__".to_string(), "rolling_counter".to_string())];
        for (node_idx, n) in nodes.iter().enumerate() {
            let mut grpc = connect_ingester(n.grpc).await;
            let got = query_all_series(
                &mut grpc,
                "t-roll",
                base_ts - 10_000,
                base_ts + 3_600_000,
            )
            .await;
            let mut node_samples: BTreeSet<(i64, u64)> = BTreeSet::new();
            if let Some(vals) = got.get(&series_key) {
                for v in vals {
                    if let DecodedVal::Float(t, f) = *v {
                        node_samples.insert((t, f.to_bits()));
                        union_samples.insert((t, f.to_bits()));
                    }
                }
            }
            assert!(
                !per_node_expected[node_idx].is_empty(),
                "node {} acknowledged 0 samples during rolling restart",
                n.id
            );
            let node_missing: Vec<_> = per_node_expected[node_idx]
                .iter()
                .copied()
                .filter(|(ts, val)| !node_samples.contains(&(*ts, val.to_bits())))
                .collect();
            assert!(
                node_missing.is_empty(),
                "node {} ({}) lost {} / {} individually-acked samples across restart: {:?}",
                n.id,
                n.which.name(),
                node_missing.len(),
                per_node_expected[node_idx].len(),
                &node_missing[..node_missing.len().min(10)]
            );
        }

        let mut missing = Vec::new();
        for (ts, val) in &expected {
            if !union_samples.contains(&(*ts, val.to_bits())) {
                missing.push((*ts, *val));
            }
        }
        assert!(
            missing.is_empty(),
            "lost {} / {} quorum-acknowledged samples during RF=3 rolling restart: {:?}",
            missing.len(),
            expected.len(),
            &missing[..missing.len().min(10)]
        );

        for n in &mut nodes {
            if let Some(mut p) = n.proc.take() {
                p.sigterm();
                assert_exit_ok(&mut p, Duration::from_secs(30));
            }
        }
    });
}

// =============================================================================================
// 6. Bidirectional Go <-> Rust PVC Handoff (Graceful SIGTERM & Crash SIGKILL)
// =============================================================================================

async fn run_bidirectional_pvc_handoff(sigkill: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let client = http_client();
    let ports = free_tcp_ports(2);
    let shared_dir = tmp.path().join("shared-pvc");
    let yaml = simple_ingester_yaml(
        ports[0],
        ports[1],
        &shared_dir,
        "ing-pvc-handoff",
        "    unregister_on_shutdown: false\n",
        "",
        "  out_of_order_time_window: 1h\n",
        "",
    );
    let base_ts = now_ms() - 120_000;
    let mode = if sigkill { "sigkill" } else { "graceful" };

    // Phase 1: Go writes float + native histogram + OOO + exemplar.
    let mut go1 = start_mimir(Impl::Go, &format!("pvc-go1-{mode}"), &shared_dir, &yaml, &[]);
    wait_ready(&client, &mut go1, ports[0], Duration::from_secs(30)).await;
    {
        let mut grpc = connect_ingester(ports[1]).await;
        push_timeseries(
            &mut grpc,
            "t-pvc",
            vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "handoff_counter"),
                        cortexpb::LabelPair::from_static("job", "shared"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 1.25,
                        timestamp_ms: base_ts,
                    }],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "go-phase-1")],
                        value: 1.25,
                        timestamp_ms: base_ts,
                    }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "handoff_hist")],
                    histograms: vec![sample_histogram(base_ts)],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "handoff_ooo")],
                    samples: vec![cortexpb::Sample {
                        value: 50.0,
                        timestamp_ms: base_ts + 40_000,
                    }],
                    ..Default::default()
                },
            ],
        )
        .await
        .unwrap();
        push_timeseries(
            &mut grpc,
            "t-pvc",
            vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "handoff_ooo")],
                samples: vec![cortexpb::Sample {
                    value: 45.0,
                    timestamp_ms: base_ts + 5_000, // OOO written by Go
                }],
                ..Default::default()
            }],
        )
        .await
        .unwrap();
    }
    if sigkill {
        sigkill_and_wait(&mut go1);
    } else {
        go1.sigterm();
        assert_exit_ok(&mut go1, Duration::from_secs(30));
    }

    // Phase 2: Rust opens Go's PVC, verifies Phase 1 data, writes Phase 2 data!
    let mut rs = start_mimir(Impl::Rust, &format!("pvc-rs-{mode}"), &shared_dir, &yaml, &[]);
    wait_ready(&client, &mut rs, ports[0], Duration::from_secs(30)).await;
    {
        let mut grpc = connect_ingester(ports[1]).await;
        let got = query_all_series(&mut grpc, "t-pvc", base_ts - 10_000, base_ts + 120_000).await;
        let counter_key = vec![
            ("__name__".to_string(), "handoff_counter".to_string()),
            ("job".to_string(), "shared".to_string()),
        ];
        assert_eq!(
            got.get(&counter_key),
            Some(&vec![DecodedVal::Float(base_ts, 1.25)]),
            "Rust failed to read Go Phase 1 float ({mode}): {got:?}"
        );
        let hist_key = vec![("__name__".to_string(), "handoff_hist".to_string())];
        assert_eq!(
            got.get(&hist_key),
            Some(&vec![DecodedVal::IntHist(base_ts, 1, 5, 12.5)]),
            "Rust failed to read Go Phase 1 histogram ({mode}): {got:?}"
        );
        let ooo_key = vec![("__name__".to_string(), "handoff_ooo".to_string())];
        assert_eq!(
            got.get(&ooo_key),
            Some(&vec![
                DecodedVal::Float(base_ts + 5_000, 45.0),
                DecodedVal::Float(base_ts + 40_000, 50.0),
            ]),
            "Rust failed to read Go Phase 1 OOO ({mode}): {got:?}"
        );

        // Rust appends Phase 2 samples (extending Go's series + brand-new Rust series + OOO + exemplar).
        push_timeseries(
            &mut grpc,
            "t-pvc",
            vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "handoff_counter"),
                        cortexpb::LabelPair::from_static("job", "shared"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 2.5,
                        timestamp_ms: base_ts + 30_000,
                    }],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "rust-phase-2")],
                        value: 2.5,
                        timestamp_ms: base_ts + 30_000,
                    }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "handoff_rust_only")],
                    samples: vec![cortexpb::Sample {
                        value: 77.75,
                        timestamp_ms: base_ts + 60_000,
                    }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "handoff_ooo")],
                    samples: vec![cortexpb::Sample {
                        value: 47.0,
                        timestamp_ms: base_ts + 15_000, // OOO written by Rust
                    }],
                    ..Default::default()
                },
            ],
        )
        .await
        .unwrap();
    }
    if sigkill {
        sigkill_and_wait(&mut rs);
    } else {
        rs.sigterm();
        assert_exit_ok(&mut rs, Duration::from_secs(30));
    }

    // Phase 3: Go reopens the SAME PVC after Rust and must read ALL Phase 1 + Phase 2 data!
    let mut go2 = start_mimir(Impl::Go, &format!("pvc-go2-{mode}"), &shared_dir, &yaml, &[]);
    wait_ready(&client, &mut go2, ports[0], Duration::from_secs(30)).await;
    {
        let mut grpc = connect_ingester(ports[1]).await;
        let got = query_all_series(&mut grpc, "t-pvc", base_ts - 10_000, base_ts + 120_000).await;
        let counter_key = vec![
            ("__name__".to_string(), "handoff_counter".to_string()),
            ("job".to_string(), "shared".to_string()),
        ];
        assert_eq!(
            got.get(&counter_key),
            Some(&vec![
                DecodedVal::Float(base_ts, 1.25),
                DecodedVal::Float(base_ts + 30_000, 2.5),
            ]),
            "Go Phase 3 failed to read Go+Rust counter series ({mode}): {got:?}"
        );
        let rust_key = vec![("__name__".to_string(), "handoff_rust_only".to_string())];
        assert_eq!(
            got.get(&rust_key),
            Some(&vec![DecodedVal::Float(base_ts + 60_000, 77.75)]),
            "Go Phase 3 failed to read Rust-created series ({mode}): {got:?}"
        );
        let hist_key = vec![("__name__".to_string(), "handoff_hist".to_string())];
        assert_eq!(
            got.get(&hist_key),
            Some(&vec![DecodedVal::IntHist(base_ts, 1, 5, 12.5)]),
            "Go Phase 3 failed to read histogram ({mode}): {got:?}"
        );
        let ooo_key = vec![("__name__".to_string(), "handoff_ooo".to_string())];
        assert_eq!(
            got.get(&ooo_key),
            Some(&vec![
                DecodedVal::Float(base_ts + 5_000, 45.0),
                DecodedVal::Float(base_ts + 15_000, 47.0),
                DecodedVal::Float(base_ts + 40_000, 50.0),
            ]),
            "Go Phase 3 failed to read Go+Rust OOO series ({mode}): {got:?}"
        );

        let ex_resp = grpc
            .query_exemplars(with_tenant(
                "t-pvc",
                ingesterpb::ExemplarQueryRequest {
                    start_timestamp_ms: base_ts - 10_000,
                    end_timestamp_ms: base_ts + 120_000,
                    matchers: vec![ingesterpb::LabelMatchers {
                        matchers: vec![ingesterpb::LabelMatcher {
                            r#type: ingesterpb::MatchType::Equal as i32,
                            name: "__name__".to_string(),
                            value: "handoff_counter".to_string(),
                        }],
                    }],
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let trace_ids: BTreeSet<String> = ex_resp
            .timeseries
            .into_iter()
            .flat_map(|ts| ts.exemplars)
            .flat_map(|e| e.labels)
            .filter(|l| l.name.as_ref() == b"trace_id")
            .map(|l| String::from_utf8_lossy(&l.value).into_owned())
            .collect();
        assert!(
            trace_ids.contains("go-phase-1") && trace_ids.contains("rust-phase-2"),
            "Go Phase 3 missing exemplars ({mode}): {trace_ids:?}"
        );
    }
    go2.sigterm();
    assert_exit_ok(&mut go2, Duration::from_secs(30));
}

#[test]
fn w35_pvc_handoff_bidirectional_graceful() {
    block_on_for_tests(run_bidirectional_pvc_handoff(false));
}

#[test]
fn w35_pvc_handoff_bidirectional_sigkill() {
    block_on_for_tests(run_bidirectional_pvc_handoff(true));
}

// =============================================================================================
// 7. Kafka ingest_storage Clean Shutdown/Restart & Unreachable-Broker SIGTERM Cancel (STOR-01)
// =============================================================================================

#[test]
fn w35_kafka_ingest_storage_restart_and_unreachable_cancel() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        // 7a. Clean shutdown persists kafka-offset.json and restart serves WAL + reaches ready.
        for which in BOTH {
            let dir = tmp.path().join(format!("{}-kafka-restart", which.name()));
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();

            let ts = now_ms() - 5_000;
            let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
                topic: "mimir-ingest".to_string(),
                partitions: 1,
                compression: None,
                records: vec![KfakeRecordInput {
                    partition: 0,
                    tenant: "t-kafka".to_string(),
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
                                value: "kafka_restart_metric".to_string(),
                            }],
                            samples: vec![SampleJson {
                                timestamp_ms: ts,
                                value: 19.25,
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
    unregister_on_shutdown: false
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
activity_tracker:
  filepath: ""
"#,
                addr = broker.ready.broker_addr,
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );

            let mut proc1 = start_mimir(which, "kafka-1", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc1, http, Duration::from_secs(45)).await;

            // Wait until Kafka record is ingested.
            let mut grpc_c = connect_ingester(grpc).await;
            let dl = Instant::now() + Duration::from_secs(15);
            loop {
                let got = query_all_series(&mut grpc_c, "t-kafka", ts - 10_000, ts + 10_000).await;
                if !got.is_empty() {
                    break;
                }
                assert!(Instant::now() < dl, "{} Kafka record not consumed", proc1.name);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            drop(grpc_c);

            // Graceful SIGTERM commits local offset file cleanly.
            proc1.sigterm();
            assert_exit_ok(&mut proc1, Duration::from_secs(20));

            let offset_file = dir.join("tsdb").join("kafka-offset.json");
            assert!(
                offset_file.exists(),
                "{}: expected {} to be persisted on clean Kafka shutdown",
                proc1.name,
                offset_file.display()
            );
            let offset_json: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&offset_file).unwrap())
                    .expect("valid kafka-offset.json");
            assert!(
                offset_json
                    .get("offset")
                    .and_then(|v| v.as_i64())
                    .is_some_and(|o| o >= 0),
                "{}: expected non-negative persisted offset in kafka-offset.json, got {offset_json}",
                proc1.name
            );

            // Restart on the same PVC with `consume_from_position_at_startup: last-offset` so
            // Boot #2 resumes strictly AFTER the committed offset and MUST serve the earlier
            // sample via TSDB WAL replay (not by re-reading offset 0 from Kafka).
            let yaml_boot2 = yaml.replace(
                "consume_from_position_at_startup: start",
                "consume_from_position_at_startup: last-offset",
            );
            let mut proc2 = start_mimir(which, "kafka-2", &dir, &yaml_boot2, &[]);
            wait_ready(&client, &mut proc2, http, Duration::from_secs(45)).await;

            let mut grpc_c = connect_ingester(grpc).await;
            let got = query_all_series(&mut grpc_c, "t-kafka", ts - 10_000, ts + 10_000).await;
            let key = vec![("__name__".to_string(), "kafka_restart_metric".to_string())];
            assert_eq!(
                got.get(&key),
                Some(&vec![DecodedVal::Float(ts, 19.25)]),
                "{}: lost Kafka sample after restart: {got:?}",
                proc2.name
            );
            drop(grpc_c);

            proc2.sigterm();
            assert_exit_ok(&mut proc2, Duration::from_secs(20));
        }

        // 7b. Hung/blackhole Kafka broker at startup + SIGTERM -> cancels get_start_offset
        // immediately instead of hanging 30s-300s+ (STOR-01).
        for which in BOTH {
            let ports = free_tcp_ports(2);
            let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let dead_kafka = blackhole.local_addr().unwrap().port();
            let bh_accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let bh_accepted_srv = Arc::clone(&bh_accepted);
            let bh_task = tokio::spawn(async move {
                let mut held = Vec::new();
                while let Ok((sock, _)) = blackhole.accept().await {
                    bh_accepted_srv.fetch_add(1, Ordering::AcqRel);
                    held.push(sock);
                }
            });
            let dir = tmp.path().join(format!("{}-kafka-unreach", which.name()));
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
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
    address: 127.0.0.1:{dead_kafka}
    topic: mimir-ingest
    auto_create_topic_enabled: false
    consume_from_position_at_startup: last-offset
    last_produced_offset_retry_timeout: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1m
activity_tracker:
  filepath: ""
"#,
                http = ports[0],
                grpc = ports[1],
                dead_kafka = dead_kafka,
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            );
            let mut proc = start_mimir(which, "kafka-unreach", &dir, &yaml, &[]);
            // Wait until HTTP server is up (still Starting, blocked on hung Kafka broker).
            let dl = Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(st) = proc.try_wait() {
                    panic!(
                        "{} exited prematurely ({st}) before /ready:\n{}",
                        proc.name,
                        proc.log()
                    );
                }
                if let Ok((503, _, _)) = http_get(&client, ports[0], "/ready", false).await {
                    break;
                }
                assert!(Instant::now() < dl, "{} /ready never came up", proc.name);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let dial_dl = Instant::now() + Duration::from_secs(10);
            while bh_accepted.load(Ordering::Acquire) == 0 {
                assert!(
                    Instant::now() < dial_dl,
                    "{} never dialed hung Kafka broker",
                    proc.name
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            let t0 = Instant::now();
            proc.sigterm();
            assert_exit_ok(&mut proc, Duration::from_secs(10));
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "{} hung shutting down with hung Kafka broker ({:?})",
                proc.name,
                t0.elapsed()
            );
            bh_task.abort();
        }
    });
}
