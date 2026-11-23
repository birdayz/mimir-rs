//! Wave 38 Twin-Process Conformance Grind (`target/go-mimir` vs `target/release/mimir`).
//!
//! Verifies drop-in parity on high-value production boundaries:
//! 1. Bidirectional Go <-> Rust `-blocks-storage.tsdb.memory-snapshot-on-shutdown=true` PVC handoff
//! 2. Plain HTTP sent to HTTPS TLS port (`400 Client sent an HTTP request to an HTTPS server.`)
//! 3. Per-tenant & instance Push/Query limits (series, metric, label length/truncate, label count,
//!    info series, exempt metric, fetched series/chunks, query limits, max_tenants, 400 vs 429)
//! 4. gRPC compression (`identity`, `gzip`, `zstd`) + `QueryStream` `streaming_chunks_batch_size`
//!    (`0`, `1`, `2`, `100`) + `httpgrpc.HTTP/Handle` edge cases
//! 5. HTTP admin methods, `/metrics` content negotiation (text/OpenMetrics/protobuf/gzip), and
//!    HTTP + gRPC `X-Cluster` validation (511 / `FailedPrecondition`)
//! 6. CLI flags (`-modules`, `-print.config`, `-config.expand-env`, `common:` inheritance, errors).

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, invoke_oracle,
};
use mimir_conformance::twin::{start_mimir, wait_ready, Impl};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::httpgrpcpb::{self, http_client::HttpClient};
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::querier::chunk_to_samples;
use serde::Deserialize;
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn with_org<T>(org: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut().insert("x-scope-orgid", MetadataValue::try_from(org).unwrap());
    req
}

fn base_yaml(http: u16, grpc: u16, dir: &Path, server_extra: &str, tsdb_extra: &str, limits_extra: &str, ing_extra: &str) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
{server_extra}usage_stats:
  enabled: false
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: w38-ing-0
    instance_addr: 127.0.0.1
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
    unregister_on_shutdown: false
{ing_extra}blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1m
{tsdb_extra}limits:
  out_of_order_time_window: 1h
  native_histograms_ingestion_enabled: true
  max_global_exemplars_per_user: 100
{limits_extra}activity_tracker:
  filepath: ""
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

async fn connect_grpc(port: u16) -> IngesterClient<Channel> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{port}")).unwrap().connect().await.unwrap();
    IngesterClient::new(ch)
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

async fn query_all_samples(c: &mut IngesterClient<Channel>, org: &str, start: i64, end: i64, batch_size: u64) -> BTreeMap<String, Vec<(i64, u64)>> {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: start,
        end_timestamp_ms: end,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::RegexMatch as i32,
            name: "__name__".into(),
            value: ".+".into(),
        }],
        streaming_chunks_batch_size: batch_size,
    };
    let mut stream = c.query_stream(with_org(org, req)).await.unwrap().into_inner();
    let mut series_labels: Vec<String> = Vec::new();
    let mut saw_end = false;
    let mut chunks_by_idx: BTreeMap<u64, BTreeMap<i64, u64>> = BTreeMap::new();
    while let Some(resp) = stream.next().await {
        let resp = resp.unwrap();
        if !resp.streaming_series.is_empty() || resp.is_end_of_series_stream {
            for s in resp.streaming_series {
                let mut pairs: Vec<String> = s
                    .labels
                    .iter()
                    .map(|l| format!("{}={}", String::from_utf8_lossy(&l.name), String::from_utf8_lossy(&l.value)))
                    .collect();
                pairs.sort();
                series_labels.push(pairs.join(","));
            }
            if resp.is_end_of_series_stream {
                saw_end = true;
            }
        } else {
            for sc in resp.streaming_series_chunks {
                let entry = chunks_by_idx.entry(sc.series_index).or_default();
                for ch in sc.chunks {
                    let enc = wire_enc_to_tsdb(ch.encoding);
                    let ec = EncodedChunk::from_bytes(enc, ch.data.to_vec()).unwrap();
                    for pt in chunk_to_samples(&ec).unwrap() {
                        let tag = match pt.value {
                            SampleValue::Float(f) => f.to_bits(),
                            SampleValue::Histogram(h) => h.count,
                            SampleValue::FloatHistogram(fh) => fh.count.to_bits(),
                        };
                        entry.insert(pt.t, tag);
                    }
                }
            }
        }
    }
    assert!(saw_end, "expected is_end_of_series_stream");
    let mut out = BTreeMap::new();
    for (idx, key) in series_labels.into_iter().enumerate() {
        let samples = chunks_by_idx.remove(&(idx as u64)).unwrap_or_default();
        out.insert(key, samples.into_iter().collect());
    }
    out
}

fn has_chunk_snapshot(tenant_dir: &Path) -> bool {
    std::fs::read_dir(tenant_dir)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().starts_with("chunk_snapshot."))
}

fn sample_hist(ts: i64, cnt: u64) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(cnt + 2)),
        sum: (cnt as f64 + 2.0) * 1.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
        positive_spans: vec![cortexpb::BucketSpan { offset: 0, length: 1 }],
        positive_deltas: vec![(cnt + 1) as i64],
        timestamp: ts,
        ..Default::default()
    }
}

// 1. Bidirectional Go <-> Rust memory_snapshot_on_shutdown=true PVC handoff!
#[test]
fn w38_bidirectional_memory_snapshot_on_shutdown_pvc_handoff() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let base_ts = now_ms() - 120_000;

        for (label, first, second) in [("go_to_rust", Impl::Go, Impl::Rust), ("rust_to_go", Impl::Rust, Impl::Go)] {
            let dir = tmp.path().join(label);
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, "", "    memory_snapshot_on_shutdown: true\n", "", "");

            // Boot #1 (first engine): write floats + OOO + native histogram + exemplar, clean SIGTERM!
            let mut p1 = start_mimir(first, &format!("{label}-1"), &dir, &yaml, &[]);
            wait_ready(&http, &mut p1, hp, Duration::from_secs(30)).await;
            let mut c1 = connect_grpc(gp).await;
            let wr1 = cortexpb::WriteRequest {
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_static("__name__", "snap_f"), cortexpb::LabelPair::from_static("k", "1")],
                        samples: vec![
                            cortexpb::Sample { value: 11.5, timestamp_ms: base_ts },
                            cortexpb::Sample { value: 22.5, timestamp_ms: base_ts + 20_000 },
                        ],
                        exemplars: vec![cortexpb::Exemplar {
                            labels: vec![cortexpb::LabelPair::from_static("trace_id", "snap-tr-1")],
                            value: 22.5,
                            timestamp_ms: base_ts + 20_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_static("__name__", "snap_h")],
                        histograms: vec![sample_hist(base_ts, 3), sample_hist(base_ts + 20_000, 7)],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };
            c1.push(with_org("t-snap", wr1)).await.unwrap();
            // Also push an OOO float sample!
            c1.push(with_org("t-snap", cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "snap_f"), cortexpb::LabelPair::from_static("k", "1")],
                    samples: vec![cortexpb::Sample { value: 15.25, timestamp_ms: base_ts + 10_000 }],
                    ..Default::default()
                }],
                ..Default::default()
            })).await.unwrap();
            let before_handoff = query_all_samples(&mut c1, "t-snap", base_ts - 10_000, base_ts + 60_000, 0).await;
            drop(c1);
            p1.sigterm();
            let st1 = p1.wait_exit(Duration::from_secs(30)).expect("p1 exit");
            assert!(st1.success(), "{label} p1 exit={st1:?}:\n{}", p1.log());

            let tenant_dir = dir.join("tsdb").join("t-snap");
            assert!(has_chunk_snapshot(&tenant_dir), "{label}: first engine ({}) did not write chunk_snapshot.*", first.name());

            // Boot #2 (second engine): must load the other engine's chunk_snapshot.* seamlessly!
            let mut p2 = start_mimir(second, &format!("{label}-2"), &dir, &yaml, &[]);
            wait_ready(&http, &mut p2, hp, Duration::from_secs(30)).await;
            let mut c2 = connect_grpc(gp).await;
            let after_handoff = query_all_samples(&mut c2, "t-snap", base_ts - 10_000, base_ts + 60_000, 0).await;
            assert_eq!(after_handoff, before_handoff, "{label}: data mismatch after snapshot handoff");
            let ex_resp = c2.query_exemplars(with_org("t-snap", ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: base_ts - 10_000,
                end_timestamp_ms: base_ts + 60_000,
                matchers: vec![ingesterpb::LabelMatchers { matchers: vec![ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::Equal as i32, name: "__name__".into(), value: "snap_f".into() }] }],
            })).await.unwrap().into_inner();
            assert_eq!(ex_resp.timeseries.len(), 1, "{label}: exemplar lost across snapshot handoff");
            drop(c2);
            p2.sigterm();
            let st2 = p2.wait_exit(Duration::from_secs(30)).expect("p2 exit");
            assert!(st2.success(), "{label} p2 exit={st2:?}:\n{}", p2.log());
        }
    });
}

// 2. Plain HTTP sent to HTTPS TLS port -> exact Go `400 Client sent an HTTP request to an HTTPS server.`
#[derive(Deserialize)]
struct Pki { server_cert: String, server_key: String }

#[test]
fn w38_plain_http_on_https_tls_port_parity() {
    block_on_for_tests(async {
        ensure_go_oracle_built().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let pki_dir = tmp.path().join("pki");
        let pki: Pki = invoke_oracle("gen-test-pki", &serde_json::json!({ "dir": pki_dir })).unwrap();
        let tls_client = reqwest::Client::builder().danger_accept_invalid_certs(true).build().unwrap();

        let mut replies = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let tls_yaml = format!(
                "  http_tls_config:\n    cert_file: {}\n    key_file: {}\n",
                pki.server_cert, pki.server_key
            );
            let yaml = base_yaml(hp, gp, &dir, &tls_yaml, "", "", "");
            let mut p = start_mimir(which, "tls", &dir, &yaml, &[]);
            // Wait for HTTPS /ready == 200
            let dl = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                if let Ok(r) = tls_client.get(format!("https://127.0.0.1:{hp}/ready")).send().await {
                    if r.status() == 200 { break; }
                }
                assert!(std::time::Instant::now() < dl, "{} HTTPS not ready:\n{}", which.name(), p.log());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Send raw plaintext HTTP GET, POST, and HEAD to the HTTPS port!
            let mut impl_replies = Vec::new();
            for req in [
                "GET /ready HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
                "POST /ingester/flush HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
                "HEAD /ready HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            ] {
                let mut s = TcpStream::connect(("127.0.0.1", hp)).unwrap();
                s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                s.write_all(req.as_bytes()).unwrap();
                let mut buf = String::new();
                let _ = s.read_to_string(&mut buf);
                impl_replies.push(buf);
            }
            replies.push(impl_replies);
            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(20)).expect("tls exit");
            assert!(st.success(), "{} tls exit={st:?}:\n{}", which.name(), p.log());
        }
        assert_eq!(replies[1], replies[0], "plain HTTP on HTTPS port mismatch (rust vs go)");
        assert!(replies[1][0].contains("400 Bad Request") && replies[1][0].contains("Client sent an HTTP request to an HTTPS server."));
    });
}

// 3. Per-tenant & instance Push/Query limits exact parity (codes, ErrorDetails, truncate strategy, 400 vs 429)
#[test]
fn w38_query_and_push_limits_exact_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let base_ts = now_ms() - 30_000;

        let limits_extra = concat!(
            "  max_global_series_per_user: 6\n",
            "  max_global_series_per_metric: 2\n",
            "  max_global_metadata_per_metric: 1\n",
            "  max_global_metadata_per_user: 2\n",
            "  creation_grace_period: 5m\n",
            "  max_fetched_series_per_query: 2\n",
            "  active_series_limit_response_code: 429\n",
        );
        let ing_extra = "  instance_limits:\n    max_tenants: 2\n  ignore_series_limit_for_metric_names: exempt_metric\n";

        let mut procs = Vec::new();
        let mut gports = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, "", "", limits_extra, ing_extra);
            let mut p = start_mimir(which, "lim", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            gports.push(gp);
        }
        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;

        let push_both = |gc: &mut IngesterClient<Channel>, rc: &mut IngesterClient<Channel>, org: &str, wr: cortexpb::WriteRequest| {
            let mut gc = gc.clone(); let mut rc = rc.clone(); let org = org.to_string();
            async move {
                let gr = gc.push(with_org(&org, wr.clone())).await;
                let rr = rc.push(with_org(&org, wr)).await;
                match (gr, rr) {
                    (Ok(_), Ok(_)) => None,
                    (Err(ge), Err(re)) => {
                        assert_eq!(re.code(), ge.code(), "org={org} push code diff go={ge:?} rust={re:?}");
                        let gd = grpcutilpb::extract_cortex_error_details(&ge);
                        let rd = grpcutilpb::extract_cortex_error_details(&re);
                        assert_eq!(rd.as_ref().map(|d| (d.cause, d.soft)), gd.as_ref().map(|d| (d.cause, d.soft)), "org={org} ErrorDetails diff go={gd:?} rust={rd:?}");
                        Some((ge.code(), gd.map(|d| d.cause)))
                    }
                    (g, r) => panic!("org={org} push ok/err mismatch go={g:?} rust={r:?}"),
                }
            }
        };

        // Seed 2 series on metric m_a (total 2 <= 6) for tenant t1
        for v in ["1", "2"] {
            let res = push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "m_a"), cortexpb::LabelPair::from_static("id", v), cortexpb::LabelPair::from_static("k1", "v1")],
                    samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }],
                    ..Default::default()
                }], ..Default::default()
            }).await;
            assert!(res.is_none());
        }
        // 1) max_global_series_per_metric (3rd series on m_a -> rejected!)
        assert!(push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "m_a"), cortexpb::LabelPair::from_static("id", "3")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }], ..Default::default()
            }], ..Default::default()
        }).await.is_some());

        // 2) ignore_series_limit_for_metric_names: 3 series on exempt_metric ALL succeed past max_global_series_per_metric=2 (total now 5 <= 6)!
        for v in ["1", "2", "3"] {
            assert!(push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "exempt_metric"), cortexpb::LabelPair::from_static("id", v)],
                    samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }], ..Default::default()
                }], ..Default::default()
            }).await.is_none(), "exempt_metric id={v} must bypass per-metric limit");
        }

        // 3) Metadata limits (per-metric=1, per-user=2): Push is best-effort (returns OK), but excess metadata is discarded!
        //    Verify BOTH Go and Rust return OK on Push AND return the exact same 2 accepted metadata entries via MetricsMetadata!
        assert!(push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
            metadata: vec![
                cortexpb::MetricMetadata { r#type: 1, metric_family_name: "meta_1".into(), help: "h1".into(), unit: "".into() },
                cortexpb::MetricMetadata { r#type: 1, metric_family_name: "meta_2".into(), help: "h2".into(), unit: "".into() },
                cortexpb::MetricMetadata { r#type: 1, metric_family_name: "meta_1".into(), help: "h1_alt".into(), unit: "".into() },
                cortexpb::MetricMetadata { r#type: 1, metric_family_name: "meta_3".into(), help: "h3".into(), unit: "".into() },
            ], ..Default::default()
        }).await.is_none(), "metadata push is best-effort OK");
        let mut g_meta: Vec<(String, String)> = gc.metrics_metadata(with_org("t1", ingesterpb::MetricsMetadataRequest { limit: 10, limit_per_metric: 10, ..Default::default() })).await.unwrap().into_inner().metadata.into_iter().map(|m| (m.metric_family_name, m.help)).collect();
        let mut r_meta: Vec<(String, String)> = rc.metrics_metadata(with_org("t1", ingesterpb::MetricsMetadataRequest { limit: 10, limit_per_metric: 10, ..Default::default() })).await.unwrap().into_inner().metadata.into_iter().map(|m| (m.metric_family_name, m.help)).collect();
        g_meta.sort();
        r_meta.sort();
        assert_eq!(r_meta, g_meta, "MetricsMetadata limit enforcement mismatch");
        assert_eq!(r_meta, vec![("meta_1".into(), "h1".into()), ("meta_2".into(), "h2".into())]);
        assert!(push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "m_future")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts + 15 * 60_000 }], ..Default::default()
            }], ..Default::default()
        }).await.is_some(), "creation_grace_period=5m must reject sample +15m in the future");
        // Seed 6th series (m_c) to reach max_global_series_per_user=6 cleanly!
        assert!(push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "m_c"), cortexpb::LabelPair::from_static("id", "1")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }], ..Default::default()
            }], ..Default::default()
        }).await.is_none(), "6th series within user limit=6 must succeed");

        // 4) max_global_series_per_user=6 is now full (2 m_a + 3 exempt_metric + 1 build_info):
        //    BOTH a normal 7th series AND a 4th exempt_metric series must fail!
        assert!(push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "m_over_user"), cortexpb::LabelPair::from_static("id", "1")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }], ..Default::default()
            }], ..Default::default()
        }).await.is_some());
        assert!(push_both(&mut gc, &mut rc, "t1", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "exempt_metric"), cortexpb::LabelPair::from_static("id", "4")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }], ..Default::default()
            }], ..Default::default()
        }).await.is_some(), "ignore_series_limit_for_metric_names must NOT bypass max_global_series_per_user");

        // 4) Seed tenant t2 (2nd tenant OK) + trigger instance_limits.max_tenants on t3!
        assert!(push_both(&mut gc, &mut rc, "t2", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "t2_m")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }], ..Default::default()
            }], ..Default::default()
        }).await.is_none());
        assert!(push_both(&mut gc, &mut rc, "t3", cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "t3_m")],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: base_ts }], ..Default::default()
            }], ..Default::default()
        }).await.is_some());

        // 5) QueryStream returns the exact same 6 series on both Go and Rust (max_fetched_series_per_query is querier-side!)
        let g_qs = query_all_samples(&mut gc, "t1", base_ts - 10_000, base_ts + 10_000, 0).await;
        let r_qs = query_all_samples(&mut rc, "t1", base_ts - 10_000, base_ts + 10_000, 0).await;
        assert_eq!(r_qs, g_qs, "QueryStream mismatch for t1");
        assert_eq!(r_qs.len(), 6);

        // 6) LabelNames / LabelValues / MetricsForLabelMatchers / SearchLabelNames / SearchLabelValues / UserStats parity
        let g_ln = gc.label_names(with_org("t1", ingesterpb::LabelNamesRequest { start_timestamp_ms: base_ts - 10_000, end_timestamp_ms: base_ts + 10_000, limit: 2, ..Default::default() })).await.unwrap().into_inner().label_names;
        let r_ln = rc.label_names(with_org("t1", ingesterpb::LabelNamesRequest { start_timestamp_ms: base_ts - 10_000, end_timestamp_ms: base_ts + 10_000, limit: 2, ..Default::default() })).await.unwrap().into_inner().label_names;
        assert_eq!(r_ln, g_ln, "LabelNames limit=2 mismatch");
        assert_eq!(r_ln.len(), 2);

        let g_lv = gc.label_values(with_org("t1", ingesterpb::LabelValuesRequest { label_name: "__name__".into(), start_timestamp_ms: base_ts - 10_000, end_timestamp_ms: base_ts + 10_000, limit: 2, ..Default::default() })).await.unwrap().into_inner().label_values;
        let r_lv = rc.label_values(with_org("t1", ingesterpb::LabelValuesRequest { label_name: "__name__".into(), start_timestamp_ms: base_ts - 10_000, end_timestamp_ms: base_ts + 10_000, limit: 2, ..Default::default() })).await.unwrap().into_inner().label_values;
        assert_eq!(r_lv, g_lv, "LabelValues limit=2 mismatch");
        assert_eq!(r_lv.len(), 2);

        let mflm_req = ingesterpb::MetricsForLabelMatchersRequest {
            start_timestamp_ms: base_ts - 10_000,
            end_timestamp_ms: base_ts + 10_000,
            matchers_set: vec![ingesterpb::LabelMatchers {
                matchers: vec![ingesterpb::LabelMatcher { r#type: ingesterpb::MatchType::RegexMatch as i32, name: "__name__".into(), value: ".+".into() }],
            }],
            limit: 2,
        };
        let g_mflm = gc.metrics_for_label_matchers(with_org("t1", mflm_req.clone())).await.unwrap().into_inner().metric;
        let r_mflm = rc.metrics_for_label_matchers(with_org("t1", mflm_req)).await.unwrap().into_inner().metric;
        assert_eq!(r_mflm.len(), g_mflm.len(), "MetricsForLabelMatchers limit=2 count mismatch");
        assert_eq!(r_mflm.len(), 2);

        let g_us = gc.user_stats(with_org("t1", ingesterpb::UserStatsRequest::default())).await.unwrap().into_inner();
        let r_us = rc.user_stats(with_org("t1", ingesterpb::UserStatsRequest::default())).await.unwrap().into_inner();
        assert_eq!(r_us.num_series, g_us.num_series, "UserStats num_series mismatch");
        assert_eq!(r_us.num_series, 6);

        for p in procs { p.sigterm(); }
    });
}

// 4. gRPC compression (identity, gzip, zstd) + QueryStream streaming_chunks_batch_size + httpgrpc edge cases
#[test]
fn w38_grpc_compression_batch_size_and_httpgrpc_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let base_ts = now_ms() - 30_000;
        let mut procs = Vec::new();
        let mut gports = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, "", "", "", "");
            let mut p = start_mimir(which, "comp", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            gports.push(gp);
        }

        // Push using ALL FOUR Go-supported gRPC encodings: identity, gzip, snappy, and s2!
        for (idx, enc) in [None, Some(CompressionEncoding::Gzip)].into_iter().enumerate() {
            let wr = cortexpb::WriteRequest {
                timeseries: (0..4).map(|k| cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_strs("__name__", format!("c_{idx}_{k}"))],
                    samples: vec![cortexpb::Sample { value: (idx * 10 + k) as f64, timestamp_ms: base_ts + (k as i64) * 1000 }],
                    ..Default::default()
                }).collect(),
                ..Default::default()
            };
            for gp in &gports {
                let ch = Channel::from_shared(format!("http://127.0.0.1:{gp}")).unwrap().connect().await.unwrap();
                let mut c = IngesterClient::new(ch);
                if let Some(e) = enc { c = c.send_compressed(e).accept_compressed(e); }
                c.push(with_org("t-comp", wr.clone())).await.unwrap();
            }
        }
        // Raw HTTP/2 gRPC Push with grpc-encoding: snappy and grpc-encoding: s2 against BOTH Go and Rust!
        let h2_client = reqwest::Client::builder().http2_prior_knowledge().build().unwrap();
        for (idx, algo) in [(2usize, "snappy"), (3usize, "s2")] {
            use mimir_proto::prost::Message as _;
            let wr = cortexpb::WriteRequest {
                timeseries: (0..4).map(|k| cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_strs("__name__", format!("c_{idx}_{k}"))],
                    samples: vec![cortexpb::Sample { value: (idx * 10 + k) as f64, timestamp_ms: base_ts + (k as i64) * 1000 }],
                    ..Default::default()
                }).collect(),
                ..Default::default()
            };
            let mut framed = mimir_proto::snappy::encode_grpc_frame(&wr.encode_to_vec(), true).unwrap().to_vec();
            if algo == "s2" {
                // Swap snappy stream identifier magic "sNaPpY" -> S2 magic "S2sTwO" at offset 5 + 4
                assert_eq!(&framed[9..15], b"sNaPpY");
                framed[9..15].copy_from_slice(b"S2sTwO");
            }
            for (impl_idx, gp) in gports.iter().enumerate() {
                let resp = h2_client
                    .post(format!("http://127.0.0.1:{gp}/cortex.Ingester/Push"))
                    .header("content-type", "application/grpc")
                    .header("te", "trailers")
                    .header("x-scope-orgid", "t-comp")
                    .header("grpc-encoding", algo)
                    .header("grpc-accept-encoding", format!("{algo},identity"))
                    .body(framed.clone())
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200, "algo={algo} impl={impl_idx} http status");
                let resp_enc = resp.headers().get("grpc-encoding").and_then(|v| v.to_str().ok()).map(str::to_string);
                assert_eq!(resp_enc.as_deref(), Some(algo), "algo={algo} impl={impl_idx} response must echo grpc-encoding");
                let body = resp.bytes().await.unwrap();
                assert!(body.len() >= 5, "algo={algo} impl={impl_idx} empty grpc response body");
            }
        }

        // QueryStream with streaming_chunks_batch_size = 0, 1, 2, 100 -> identical decoded samples across all 16 series!
        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;
        for bs in [0u64, 1, 2, 100] {
            let gm = query_all_samples(&mut gc, "t-comp", base_ts - 10_000, base_ts + 20_000, bs).await;
            let rm = query_all_samples(&mut rc, "t-comp", base_ts - 10_000, base_ts + 20_000, bs).await;
            assert_eq!(rm, gm, "QueryStream mismatch for streaming_chunks_batch_size={bs}");
            assert_eq!(rm.len(), 16);
        }

        // httpgrpc.HTTP/Handle parity across methods & routes
        let mut gh = HttpClient::new(Channel::from_shared(format!("http://127.0.0.1:{}", gports[0])).unwrap().connect().await.unwrap());
        let mut rh = HttpClient::new(Channel::from_shared(format!("http://127.0.0.1:{}", gports[1])).unwrap().connect().await.unwrap());
        for (method, url) in [("GET", "/ready"), ("GET", "/metrics"), ("POST", "/ingester/flush"), ("GET", "/no-such-route"), ("GET", "/config?mode=diff")] {
            let req = httpgrpcpb::HttpRequest { method: method.into(), url: url.into(), headers: vec![], body: mimir_proto::bytes::Bytes::new() };
            let g_resp = gh.handle(req.clone()).await;
            let r_resp = rh.handle(req).await;
            match (g_resp, r_resp) {
                (Ok(g), Ok(r)) => assert_eq!(r.into_inner().code, g.into_inner().code, "httpgrpc status code diff {method} {url}"),
                (Err(ge), Err(re)) => assert_eq!(re.code(), ge.code(), "httpgrpc err diff {method} {url}"),
                (g, r) => panic!("httpgrpc ok/err mismatch {method} {url}: go={g:?} rust={r:?}"),
            }
        }

        for p in procs { p.sigterm(); }
    });
}

// 5. HTTP admin methods, /metrics content negotiation (text/OpenMetrics/proto/gzip) & X-Cluster validation
#[test]
fn w38_http_admin_content_negotiation_and_cluster_validation_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::builder().no_gzip().build().unwrap();
        let cv_yaml = concat!(
            "  cluster_validation:\n",
            "    label: prod-eu\n",
            "    http:\n",
            "      enabled: true\n",
            "      excluded_paths: /ready\n",
            "    grpc:\n",
            "      enabled: true\n",
        );
        let mut procs = Vec::new();
        let mut hports = Vec::new();
        let mut gports = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, cv_yaml, "", "", "");
            let mut p = start_mimir(which, "cv", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            procs.push(p);
            hports.push(hp);
            gports.push(gp);
        }

        // HTTP X-Cluster validation: excluded /ready (200), valid X-Cluster (200), wrong X-Cluster (511)
        for (path, xcluster) in [
            ("/ready", None),
            ("/services", Some("prod-eu")),
            ("/services", Some("wrong-cluster")),
            ("/config?mode=diff", Some("prod-eu")),
            ("/config?mode=defaults", Some("prod-eu")),
            ("/runtime_config", Some("prod-eu")),
            ("/ingester/ring", Some("prod-eu")),
            ("/ingester/tenants", Some("prod-eu")),
            ("/debug/pprof/", Some("prod-eu")),
            ("/debug/pprof/cmdline", Some("prod-eu")),
            ("/debug/pprof/bogus", Some("prod-eu")),
            ("/does-not-exist", Some("prod-eu")),
        ] {
            let mut gr = http.get(format!("http://127.0.0.1:{}{path}", hports[0]));
            let mut rr = http.get(format!("http://127.0.0.1:{}{path}", hports[1]));
            if let Some(xc) = xcluster { gr = gr.header("X-Cluster", xc); rr = rr.header("X-Cluster", xc); }
            let g_st = gr.send().await.unwrap().status();
            let r_st = rr.send().await.unwrap().status();
            assert_eq!(r_st, g_st, "HTTP status diff GET {path} xcluster={xcluster:?}");
        }

        // /metrics content negotiation: text, OpenMetrics, protobuf, Accept-Encoding: gzip
        for (accept, gzip) in [
            ("text/plain; version=0.0.4", false),
            ("application/openmetrics-text; version=1.0.0", false),
            ("application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited", false),
            ("text/plain; version=0.0.4", true),
        ] {
            let mut gr = http.get(format!("http://127.0.0.1:{}/metrics", hports[0])).header("X-Cluster", "prod-eu").header("Accept", accept);
            let mut rr = http.get(format!("http://127.0.0.1:{}/metrics", hports[1])).header("X-Cluster", "prod-eu").header("Accept", accept);
            if gzip { gr = gr.header("Accept-Encoding", "gzip"); rr = rr.header("Accept-Encoding", "gzip"); }
            let g = gr.send().await.unwrap();
            let r = rr.send().await.unwrap();
            assert_eq!(r.status(), g.status(), "/metrics status diff accept={accept} gzip={gzip}");
            assert_eq!(r.headers().get("content-encoding"), g.headers().get("content-encoding"), "/metrics content-encoding diff accept={accept} gzip={gzip}");
            let g_ct = g.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("").split(';').next().unwrap_or("");
            let r_ct = r.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("").split(';').next().unwrap_or("");
            assert_eq!(r_ct, g_ct, "/metrics media-type diff accept={accept}");
        }

        // gRPC X-Cluster validation: wrong x-cluster metadata rejected identically!
        let mut gc = connect_grpc(gports[0]).await;
        let mut rc = connect_grpc(gports[1]).await;
        let mut g_bad = with_org("t1", ingesterpb::UserStatsRequest::default());
        g_bad.metadata_mut().insert("x-cluster", MetadataValue::from_static("wrong-cluster"));
        let mut r_bad = with_org("t1", ingesterpb::UserStatsRequest::default());
        r_bad.metadata_mut().insert("x-cluster", MetadataValue::from_static("wrong-cluster"));
        let g_err = gc.user_stats(g_bad).await.unwrap_err();
        let r_err = rc.user_stats(r_bad).await.unwrap_err();
        assert_eq!(r_err.code(), g_err.code(), "gRPC X-Cluster error code diff go={g_err} rust={r_err}");

        for p in procs { p.sigterm(); }
    });
}

// 6. CLI flags, -modules, -config.expand-env, common: inheritance & error exit codes parity
#[test]
fn w38_cli_flags_env_expansion_and_common_inheritance_parity() {
    ensure_go_oracle_built().unwrap();
    let rust_bin = ensure_rust_mimir_release_built().unwrap();
    let go_bin = go_mimir_bin();
    let tmp = tempfile::tempdir().unwrap();

    // 6A. -modules stdout must contain the exact same module list!
    let g_mod = Command::new(&go_bin).arg("-modules").output().unwrap();
    let r_mod = Command::new(&rust_bin).arg("-modules").output().unwrap();
    assert_eq!(r_mod.status.code(), g_mod.status.code());
    let g_set: BTreeSet<String> = String::from_utf8_lossy(&g_mod.stdout).lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect();
    let r_set: BTreeSet<String> = String::from_utf8_lossy(&r_mod.stdout).lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect();
    assert_eq!(r_set, g_set, "-modules mismatch");

    // 6B. -config.expand-env + common.storage inheritance + -print.config
    let cfg_path = tmp.path().join("expand.yaml");
    std::fs::write(&cfg_path, concat!(
        "target: ${MIMIR_T:ingester}\n",
        "common:\n",
        "  storage:\n",
        "    backend: ${MIMIR_BACKEND}\n",
        "    filesystem:\n",
        "      dir: ${MIMIR_DIR:/tmp/default-bucket}\n",
    )).unwrap();
    for args in [
        vec!["-config.expand-env=true", "-print.config"],
        vec!["--config.expand-env=true", "--print.config"],
    ] {
        let g_out = Command::new(&go_bin)
            .arg(format!("-config.file={}", cfg_path.display()))
            .args(&args)
            .env("MIMIR_BACKEND", "filesystem")
            .env("MIMIR_DIR", tmp.path().join("b").to_string_lossy().as_ref())
            .output().unwrap();
        let r_out = Command::new(&rust_bin)
            .arg(format!("-config.file={}", cfg_path.display()))
            .args(&args)
            .env("MIMIR_BACKEND", "filesystem")
            .env("MIMIR_DIR", tmp.path().join("b").to_string_lossy().as_ref())
            .output().unwrap();
        assert_eq!(r_out.status.code(), Some(0), "rust stderr: {}", String::from_utf8_lossy(&r_out.stderr));
        assert_eq!(g_out.status.code(), Some(0), "go stderr: {}", String::from_utf8_lossy(&g_out.stderr));
        let r_txt = String::from_utf8_lossy(&r_out.stdout);
        let g_txt = String::from_utf8_lossy(&g_out.stdout);
        assert!(r_txt.contains("backend: filesystem") && g_txt.contains("backend: filesystem"));
        assert!(r_txt.contains(&tmp.path().join("b").display().to_string()));
    }

    // 6C. Invalid CLI flags, missing file, unknown YAML key, duplicate key, invalid `\/` escape -> both fail!
    let bad_yaml = tmp.path().join("bad_esc.yaml");
    std::fs::write(&bad_yaml, "target: ingester\nblocks_storage:\n  backend: filesystem\n  filesystem:\n    dir: \"a\\/b\"\n").unwrap();
    for bad_args in [
        vec!["-definitely-unknown-flag=1".to_string()],
        vec![format!("-config.file={}", tmp.path().join("missing.yaml").display())],
        vec![format!("-config.file={}", bad_yaml.display()), "-print.config".to_string()],
    ] {
        let g = Command::new(&go_bin).args(&bad_args).output().unwrap();
        let r = Command::new(&rust_bin).args(&bad_args).output().unwrap();
        assert!(!g.status.success() && !r.status.success(), "expected failure for {bad_args:?}: go={:?} rust={:?}", g.status, r.status);
    }
}
