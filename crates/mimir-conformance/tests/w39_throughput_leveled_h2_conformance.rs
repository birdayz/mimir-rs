//! Wave 39 live Go-vs-Rust twin-process & wire conformance tests:
//! 1. `-server.throughput.latency-cutoff` & `-server.throughput.unit` twin-process parity
//! 2. Multi-range `-blocks-storage.tsdb.block-ranges-period=2h,6h` (local leveled compaction)
//!    twin-process parity + bucket level-1-only shipping parity
//! 3. `blocks_storage.s3.http.force_attempt_http2: true` CLI/YAML `-print.config` parity vs Go
//!    AND live HTTPS/HTTP2 (ALPN `h2`) wire round-trip via `S3Bucket`

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_config::{BucketHTTPConfig, S3BucketLookupType, S3Config};
use mimir_conformance::oracle::{ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, invoke_oracle};
use mimir_conformance::twin::{free_tcp_ports, start_mimir, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb::{self, ingester_client::IngesterClient};
use mimir_storage::bucket::s3::S3Bucket;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples as chunk_to_samples, SampleValue};
use serde::Deserialize;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

fn free_test_port() -> u16 {
    free_tcp_ports(1)[0]
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn with_org<T>(org: &str, msg: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut()
        .insert("x-scope-orgid", MetadataValue::try_from(org).unwrap());
    r
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

fn base_yaml(http_port: u16, grpc_port: u16, dir: &Path, tsdb_extra: &str) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: warn
ingester:
  ring:
    instance_id: w39-test
    instance_addr: 127.0.0.1
    instance_port: {grpc_port}
    kvstore:
      store: inmemory
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 1h
{tsdb_extra}
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

async fn wait_ready(http: &reqwest::Client, p: &mut Proc, port: u16, timeout: Duration) {
    let dl = std::time::Instant::now() + timeout;
    loop {
        if let Some(st) = p.try_wait() {
            panic!("process exited early ({st:?}):\n{}", p.log());
        }
        if let Ok(r) = http.get(format!("http://127.0.0.1:{port}/ready")).send().await {
            if r.status() == 200 {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < dl,
            "not ready within {timeout:?}:\n{}",
            p.log()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn connect_grpc(port: u16) -> IngesterClient<Channel> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    IngesterClient::new(ch)
}

async fn query_all_samples(
    c: &mut IngesterClient<Channel>,
    org: &str,
    start: i64,
    end: i64,
) -> BTreeMap<String, Vec<(i64, u64)>> {
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: start,
        end_timestamp_ms: end,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::RegexMatch as i32,
            name: "__name__".into(),
            value: ".+".into(),
        }],
        streaming_chunks_batch_size: 8,
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
                    .map(|l| {
                        format!(
                            "{}={}",
                            String::from_utf8_lossy(&l.name),
                            String::from_utf8_lossy(&l.value)
                        )
                    })
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

#[derive(Deserialize)]
struct BlockMetaLite {
    compaction: BlockMetaCompactionLite,
}

#[derive(Deserialize)]
struct BlockMetaCompactionLite {
    level: i32,
}

fn block_levels_in_dir(dir: &Path) -> Vec<i32> {
    let mut levels = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return levels;
    };
    for ent in rd.flatten() {
        let meta_path = ent.path().join("meta.json");
        if meta_path.is_file() {
            if let Ok(bytes) = std::fs::read(&meta_path) {
                if let Ok(m) = serde_json::from_slice::<BlockMetaLite>(&bytes) {
                    levels.push(m.compaction.level);
                }
            }
        }
    }
    levels.sort();
    levels
}

// 1. -server.throughput.latency-cutoff & -server.throughput.unit twin-process parity
#[test]
fn w39_server_throughput_latency_cutoff_twin_process_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let extra = vec![
            "-server.throughput.latency-cutoff=15ms".to_string(),
            "-server.throughput.unit=processed_bytes".to_string(),
        ];

        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, "");
            let mut p = start_mimir(which, "tp", &dir, &yaml, &extra);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;

            // Verify /metrics responds 200 cleanly and config endpoint succeeds.
            let m = http
                .get(format!("http://127.0.0.1:{hp}/metrics"))
                .send()
                .await
                .unwrap();
            assert_eq!(m.status(), 200, "{} /metrics failed", which.name());
            let body = m.text().await.unwrap();
            assert!(
                body.contains("cortex_request_duration_seconds"),
                "{} missing request_duration metric",
                which.name()
            );

            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(20)).expect("exit");
            assert!(st.success(), "{} exit={st:?}:\n{}", which.name(), p.log());
        }
    });
}

// 2. Multi-range -blocks-storage.tsdb.block-ranges-period=2h,6h twin-process parity
#[test]
fn w39_tsdb_multi_range_block_ranges_period_twin_process_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let http = reqwest::Client::new();
        let two_h = 2 * 3_600_000i64;
        // Align start to a 6h boundary in the recent past so 3 consecutive 2h blocks fit cleanly.
        let six_h = 3 * two_h;
        let base_t = ((now_ms() - 8 * 3_600_000) / six_h) * six_h;
        let tsdb_extra = "    block_ranges_period:
      - 2h
      - 6h
";

        let mut results = Vec::new();
        let mut shipped_levels_all = Vec::new();

        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            std::fs::create_dir_all(dir.join("tsdb")).unwrap();
            std::fs::create_dir_all(dir.join("bucket")).unwrap();
            let hp = free_test_port();
            let gp = free_test_port();
            let yaml = base_yaml(hp, gp, &dir, tsdb_extra);
            let mut p = start_mimir(which, "mr", &dir, &yaml, &[]);
            wait_ready(&http, &mut p, hp, Duration::from_secs(30)).await;
            let mut c = connect_grpc(gp).await;

            // Push samples spanning 4 consecutive 2h intervals (creating multiple blocks + head).
            for step in 0..4i64 {
                let ts = base_t + step * two_h + 60_000;
                let wr = cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_static("__name__", "multi_range_metric"),
                            cortexpb::LabelPair::from_static("env", "prod"),
                        ],
                        samples: vec![
                            cortexpb::Sample {
                                value: (step * 10 + 1) as f64,
                                timestamp_ms: ts,
                            },
                            cortexpb::Sample {
                                value: (step * 10 + 2) as f64,
                                timestamp_ms: ts + 30_000,
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                c.push(with_org("t-mr", wr)).await.unwrap();
            }

            // Force flush + ship (wait=true) -> compacts & ships level-1 blocks!
            let flush = http
                .post(format!("http://127.0.0.1:{hp}/ingester/flush?wait=true"))
                .send()
                .await
                .unwrap();
            assert_eq!(flush.status(), 204, "{} flush failed", which.name());

            let queried = query_all_samples(&mut c, "t-mr", base_t - 60_000, base_t + 8 * two_h).await;
            assert_eq!(queried.len(), 1, "{} expected 1 series", which.name());
            results.push(queried);

            let bucket_levels = block_levels_in_dir(&dir.join("bucket").join("t-mr"));
            assert!(
                !bucket_levels.is_empty(),
                "{} shipped 0 blocks to bucket",
                which.name()
            );
            assert!(
                bucket_levels.iter().all(|&lvl| lvl == 1),
                "{} must ship only level-1 blocks, got {bucket_levels:?}",
                which.name()
            );
            shipped_levels_all.push(bucket_levels);

            drop(c);
            p.sigterm();
            let st = p.wait_exit(Duration::from_secs(30)).expect("exit");
            assert!(st.success(), "{} exit={st:?}:\n{}", which.name(), p.log());
        }

        assert_eq!(results[1], results[0], "multi-range query parity mismatch (rust vs go)");
        assert_eq!(
            shipped_levels_all[1].len(),
            shipped_levels_all[0].len(),
            "shipped block count mismatch (rust={:?} vs go={:?})",
            shipped_levels_all[1],
            shipped_levels_all[0]
        );
    });
}

#[derive(Deserialize)]
struct TestPki {
    ca_cert: String,
    server_cert: String,
    server_key: String,
}

// 3. S3 force_attempt_http2: true CLI/YAML parity vs Go AND live HTTPS/ALPN h2 wire round-trip
#[test]
fn w39_s3_force_attempt_http2_config_and_live_h2_alpn_parity() {
    ensure_go_oracle_built().unwrap();
    let rust_bin = ensure_rust_mimir_release_built().unwrap();
    let go_bin = go_mimir_bin();
    let tmp = tempfile::tempdir().unwrap();

    // 3A. Both Go and Rust accept blocks_storage.s3.http.force_attempt_http2: true with -print.config
    let cfg_path = tmp.path().join("s3_h2.yaml");
    std::fs::write(
        &cfg_path,
        concat!(
            "target: ingester
",
            "blocks_storage:
",
            "  backend: s3
",
            "  s3:
",
            "    endpoint: s3.example.com
",
            "    bucket_name: test-bucket
",
            "    http:
",
            "      force_attempt_http2: true
",
        ),
    )
    .unwrap();
    let g_out = Command::new(&go_bin)
        .args([
            format!("-config.file={}", cfg_path.display()),
            "-print.config".to_string(),
        ])
        .output()
        .unwrap();
    let r_out = Command::new(&rust_bin)
        .args([
            format!("-config.file={}", cfg_path.display()),
            "-print.config".to_string(),
        ])
        .output()
        .unwrap();
    assert_eq!(g_out.status.code(), Some(0), "go stderr: {}", String::from_utf8_lossy(&g_out.stderr));
    assert_eq!(r_out.status.code(), Some(0), "rust stderr: {}", String::from_utf8_lossy(&r_out.stderr));
    assert!(String::from_utf8_lossy(&r_out.stdout).contains("force_attempt_http2: true"));
    assert!(String::from_utf8_lossy(&g_out.stdout).contains("force_attempt_http2: true"));

    // 3B. Live HTTPS + ALPN h2 wire test: S3Bucket with force_attempt_http2=true MUST negotiate HTTP/2!
    block_on_for_tests(async {
        let pki_dir = tmp.path().join("pki");
        let pki: TestPki = invoke_oracle("gen-test-pki", &serde_json::json!({ "dir": pki_dir })).unwrap();

        let h2_hits = Arc::new(AtomicUsize::new(0));
        let h2_hits_cb = Arc::clone(&h2_hits);
        let store: Arc<parking_lot::Mutex<BTreeMap<String, Vec<u8>>>> =
            Arc::new(parking_lot::Mutex::new(BTreeMap::new()));
        let store_cb = Arc::clone(&store);

        let app = axum::Router::new().fallback(move |req: axum::extract::Request| {
            let h2_hits = Arc::clone(&h2_hits_cb);
            let store = Arc::clone(&store_cb);
            async move {
                if req.version() == axum::http::Version::HTTP_2 {
                    h2_hits.fetch_add(1, Ordering::AcqRel);
                }
                let method = req.method().clone();
                let key = req.uri().path().trim_start_matches('/').to_string();
                match method.as_str() {
                    "PUT" => {
                        let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                            .await
                            .unwrap()
                            .to_vec();
                        store.lock().insert(key, body);
                        (
                            axum::http::StatusCode::OK,
                            [(axum::http::header::ETAG, "\"abc123\"")],
                            Vec::<u8>::new(),
                        )
                    }
                    "GET" => {
                        if let Some(data) = store.lock().get(&key).cloned() {
                            (
                                axum::http::StatusCode::OK,
                                [(axum::http::header::ETAG, "\"abc123\"")],
                                data,
                            )
                        } else {
                            (
                                axum::http::StatusCode::NOT_FOUND,
                                [(axum::http::header::ETAG, "")],
                                b"<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>".to_vec(),
                            )
                        }
                    }
                    "HEAD" => {
                        if store.lock().contains_key(&key) {
                            (
                                axum::http::StatusCode::OK,
                                [(axum::http::header::ETAG, "\"abc123\"")],
                                Vec::new(),
                            )
                        } else {
                            (
                                axum::http::StatusCode::NOT_FOUND,
                                [(axum::http::header::ETAG, "")],
                                Vec::new(),
                            )
                        }
                    }
                    _ => (
                        axum::http::StatusCode::OK,
                        [(axum::http::header::ETAG, "")],
                        Vec::new(),
                    ),
                }
            }
        });

        let hp = free_test_port();
        let gp = free_test_port();
        let mut srv_cfg = mimir_config::ServerConfig {
            http_listen_address: "127.0.0.1".to_string(),
            http_listen_port: hp as i64,
            grpc_listen_address: "127.0.0.1".to_string(),
            grpc_listen_port: gp as i64,
            ..Default::default()
        };
        srv_cfg.http_tls_config.cert_file = pki.server_cert;
        srv_cfg.http_tls_config.key_file = pki.server_key;

        let reg = prometheus::Registry::new();
        let logger: Arc<dyn mimir_common::logging::Logger> =
            Arc::new(mimir_common::logging::StructuredLogger::new(
                mimir_common::logging::LogFormat::Logfmt,
                mimir_common::logging::LogLevel::Error,
                Box::new(std::io::sink()),
            ));
        let server = mimir_cmd::server::Server::new(&srv_cfg, &reg, logger).unwrap();
        let grpc_routes = tonic::service::Routes::new(
            mimir_cmd::grpc::HealthService::new(
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
                mimir_cmd::modules::ModuleStates::new(),
            ),
        );
        let running = server.start(app, grpc_routes).unwrap();

        let s3_cfg = S3Config {
            endpoint: format!("127.0.0.1:{hp}"),
            bucket_name: "h2bkt".to_string(),
            access_key_id: "testkey".to_string(),
            secret_access_key: "testsecret".to_string(),
            region: "us-east-1".to_string(),
            insecure: false,
            bucket_lookup_type: S3BucketLookupType::Path,
            http: BucketHTTPConfig {
                force_attempt_http2: true,
                tls_ca_path: pki.ca_cert,
                tls_server_name: "localhost".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let bucket = S3Bucket::new(&s3_cfg, "w39-h2").unwrap();
        bucket
            .upload("probe/hello.txt", mimir_storage::bucket::s3::UploadSource::Bytes(bytes::Bytes::from_static(b"hello over h2")), None)
            .await
            .unwrap();
        let got = bucket.get("probe/hello.txt").await.unwrap();
        assert_eq!(&got[..], b"hello over h2");
        assert!(
            h2_hits.load(Ordering::Acquire) >= 2,
            "expected S3Bucket with force_attempt_http2=true to negotiate HTTP/2 (h2 hits={})",
            h2_hits.load(Ordering::Acquire)
        );
        let _ = running.shutdown().await;
    });


}
