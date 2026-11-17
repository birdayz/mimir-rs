//! Differential twin-process and in-process conformance tests for Wave 4
//! Ingester / Cmd / Ring fixes:
//! - W4-03: `spawn_shipping_loop` 1% ticker jitter prevents phase-locking with
//!   `spawn_idle_tsdb_loop` (`close_idle_tsdb_timeout`).
//! - W4-05: `-server.path-prefix` / `/static/*` / `/debug/pprof` / root index routes.
//! - W4-06: `LifecyclerConfig` rejects `heartbeat_period: 0s` and `heartbeat_timeout: 0s`.
//! - W4-08: `/services` `activity-tracker` + Go timestamp format, and
//!   `/ingester/tsdb_metrics` `Vary: Accept` + protobuf/text negotiation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_config::storage::BlocksStorageConfig;
use mimir_config::{GoDuration, IngestStorageConfig, IngesterConfig, LimitsConfig, Overrides};
use mimir_conformance::free_test_port;
use mimir_conformance::twin::{http_get, start_mimir, Impl, Proc};
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::KvClient;
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig};
use mimir_ring::ring::model::ring_desc_codec;
use prometheus::Registry;
use tonic::metadata::MetadataValue;

const BOTH: [Impl; 2] = [Impl::Go, Impl::Rust];

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_gzip()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .build()
        .unwrap()
}

fn base_yaml(http: u16, grpc: u16, dir: &Path, extra_server: &str, extra_ring: &str, extra_top: &str) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
{extra_server}usage_stats:
  enabled: false
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
{extra_ring}blocks_storage:
  backend: filesystem
  filesystem:
    dir: {blocks}
  tsdb:
    dir: {tsdb}
    ship_interval: 1h
{extra_top}"#,
        blocks = dir.join("blocks").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

async fn wait_ready_at(client: &reqwest::Client, proc: &mut Proc, port: u16, ready_path: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(s) = proc.try_wait() {
            panic!("{} exited ({s}) before ready:\n{}", proc.name, proc.log());
        }
        if let Ok((200, _, _)) = http_get(client, port, ready_path, false).await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} not ready at {ready_path}:\n{}",
            proc.name,
            proc.log()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn push_sample(grpc_port: u16, tenant: &str, metric: &str, ts_ms: i64, val: f64) {
    let mut client = IngesterClient::connect(format!("http://127.0.0.1:{grpc_port}"))
        .await
        .unwrap();
    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: "__name__".to_string().into(),
                value: metric.to_string().into(),
            }],
            samples: vec![cortexpb::Sample {
                timestamp_ms: ts_ms,
                value: val,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut grpc_req = tonic::Request::new(req);
    grpc_req
        .metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(tenant).unwrap());
    client.push(grpc_req).await.unwrap();
}

// -----------------------------------------------------------------------------
// W4-03: spawn_shipping_loop 1% jitter avoids phase-locking with idle closer
// -----------------------------------------------------------------------------

#[test]
fn w4_03_shipping_loop_jitter_allows_idle_tsdb_close_without_phase_lock() {
    block_on_for_tests(async {
        let dir = tempfile::tempdir().unwrap();
        let mut ing_cfg = IngesterConfig::default();
        ing_cfg.ring.instance_id = "ing-w403".to_string();
        ing_cfg.ring.instance_addr = "127.0.0.1".to_string();
        ing_cfg.ring.kvstore.store = "inmemory".to_string();
        ing_cfg.ring.replication_factor = 1;
        ing_cfg.ring.num_tokens = 8;
        ing_cfg.ring.min_ready_duration = GoDuration::ZERO;
        ing_cfg.ring.final_sleep = GoDuration::ZERO;

        let mut blocks_cfg = BlocksStorageConfig::default();
        blocks_cfg.bucket.backend_cfg.backend = "filesystem".to_string();
        blocks_cfg.bucket.backend_cfg.filesystem.dir =
            dir.path().join("blocks").to_string_lossy().into_owned();
        blocks_cfg.tsdb.dir = dir.path().join("tsdb").to_string_lossy().into_owned();
        // Exact integer divisor of close_idle_tsdb_interval (5 * 200ms = 1000ms),
        // mirroring default 5 * 1m = 5m.
        blocks_cfg.tsdb.ship_interval = GoDuration::from_millis(200);
        blocks_cfg.tsdb.close_idle_tsdb_interval = GoDuration::from_secs(1);
        blocks_cfg.tsdb.close_idle_tsdb_timeout = GoDuration::from_millis(300);

        let reg = Registry::new();
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let lc_cfg = LifecyclerConfig::from_ring_config(&ing_cfg.ring, 0).unwrap();
        let lc = Lifecycler::with_store(lc_cfg, kv, None, "ingester", "ring", false, None);
        let ing = Ingester::new(
            ing_cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            Overrides::new(LimitsConfig::default()),
            None,
            Some(lc),
            None,
            None,
            None,
            Some(&reg),
        )
        .unwrap();

        ing.start_async().await.unwrap();
        ing.await_running().await.unwrap();

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        ing.push_write_request(
            "t-idle",
            &cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair {
                        name: "__name__".to_string().into(),
                        value: "idle_metric".to_string().into(),
                    }],
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: now_ms,
                        value: 1.0,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Compact head and ship blocks so the TSDB becomes eligible for idle closing.
        ing.compact_blocks(true, i64::MAX, None).await;
        ing.ship_blocks(None).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        let mut closed = false;
        while tokio::time::Instant::now() < deadline {
            if ing.get_tsdb("t-idle").is_none() {
                closed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(closed, "idle tenant TSDB was not closed within timeout");

        let fams = reg.gather();
        let mut idle_closed = 0.0;
        let mut not_active = 0.0;
        for f in &fams {
            if f.get_name() == "cortex_ingester_idle_tsdb_checks_total" {
                for m in f.get_metric() {
                    let result = m
                        .get_label()
                        .iter()
                        .find(|l| l.get_name() == "result")
                        .map(|l| l.get_value())
                        .unwrap_or("");
                    match result {
                        "idle_closed" => idle_closed = m.get_counter().get_value(),
                        "not_active" => not_active = m.get_counter().get_value(),
                        _ => {}
                    }
                }
            }
        }
        assert!(idle_closed >= 1.0, "expected idle_closed >= 1, got {idle_closed}");
        // A single transient overlap at t=1s/2s can still occur when the random ±1% draw
        // lands near exact 200.0ms on the same 1ms timer tick, but 1% jitter prevents
        // persistent phase-locking and allows idle closing to succeed promptly.
        assert!(not_active <= 2.0, "expected no persistent phase-locked not_active, got {not_active}");

        let _ = ing.stop().await;
    });
}

// -----------------------------------------------------------------------------
// W4-05 + W4-08: -server.path-prefix, /static/*, /debug/pprof, /services,
// /ingester/tsdb_metrics Vary + content negotiation
// -----------------------------------------------------------------------------

#[test]
fn w4_05_and_w4_08_http_prefix_static_pprof_services_and_tsdb_metrics_parity() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        struct Inst {
            which: Impl,
            http: u16,
            grpc: u16,
            proc: Proc,
        }

        let mut insts = Vec::new();
        for which in BOTH {
            let http = free_test_port();
            let grpc = free_test_port();
            let dir = tmp.path().join(which.name());
            let rt_file = dir.join("runtime.yaml");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                &rt_file,
                "overrides:\n  t-w4:\n    max_global_series_per_user: 100\n",
            )
            .unwrap();
            let yaml = base_yaml(
                http,
                grpc,
                &dir,
                "",
                "",
                &format!(
                    "runtime_config:\n  file: {}\n  period: 1s\n",
                    rt_file.display()
                ),
            );
            let proc = start_mimir(
                which,
                "w4-http",
                &dir,
                &yaml,
                &["-server.path-prefix=/mimir".to_string()],
            );
            insts.push(Inst {
                which,
                http,
                grpc,
                proc,
            });
        }

        for inst in &mut insts {
            wait_ready_at(&client, &mut inst.proc, inst.http, "/mimir/ready").await;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        for inst in &insts {
            push_sample(inst.grpc, "t-w4", "w4_probe_metric", now_ms, 42.0).await;
        }

        // Compare HTTP behavior across W4-05 and W4-08 cases.
        #[derive(Debug, PartialEq, Eq)]
        struct HttpSnap {
            status: u16,
            ce: String,
            vary: String,
            loc: String,
            non_empty: bool,
        }

        async fn fetch(
            client: &reqwest::Client,
            port: u16,
            method: reqwest::Method,
            path: &str,
            headers: &[(&str, &str)],
        ) -> (HttpSnap, Vec<u8>) {
            let mut req = client
                .request(method, format!("http://127.0.0.1:{port}{path}"))
                .timeout(Duration::from_secs(5));
            let mut has_ae = false;
            for &(k, v) in headers {
                if k.eq_ignore_ascii_case("accept-encoding") {
                    has_ae = true;
                }
                req = req.header(k, v);
            }
            if !has_ae {
                req = req.header("Accept-Encoding", "identity");
            }
            let resp = req.send().await.unwrap();
            let status = resp.status().as_u16();
            let hdr = |n: &str| {
                resp.headers()
                    .get(n)
                    .map(|v| v.to_str().unwrap_or_default().to_string())
                    .unwrap_or_default()
            };
            let ce = hdr("content-encoding");
            let vary = hdr("vary");
            let loc = hdr("location");
            let bytes = resp.bytes().await.unwrap().to_vec();
            (
                HttpSnap {
                    status,
                    ce,
                    vary,
                    loc,
                    non_empty: !bytes.is_empty(),
                },
                bytes,
            )
        }

        type HttpCase<'a> = (&'a str, reqwest::Method, &'a str, &'a [(&'a str, &'a str)]);
        let cases: &[HttpCase<'_>] = &[
            ("GET /mimir (no trailing slash)", reqwest::Method::GET, "/mimir", &[]),
            (
                "GET /mimir/ (gzip)",
                reqwest::Method::GET,
                "/mimir/",
                &[("Accept-Encoding", "gzip")],
            ),
            ("POST /mimir/", reqwest::Method::POST, "/mimir/", &[]),
            (
                "GET /mimir/static/mimir-styles.css",
                reqwest::Method::GET,
                "/mimir/static/mimir-styles.css",
                &[],
            ),
            ("GET /mimir/debug/pprof", reqwest::Method::GET, "/mimir/debug/pprof", &[]),
            ("GET /mimir/debug/pprof/", reqwest::Method::GET, "/mimir/debug/pprof/", &[]),
            (
                "GET /mimir/debug/pprof/cmdline",
                reqwest::Method::GET,
                "/mimir/debug/pprof/cmdline",
                &[],
            ),
            (
                "GET /mimir/ingester/tsdb_metrics (text)",
                reqwest::Method::GET,
                "/mimir/ingester/tsdb_metrics",
                &[("X-Scope-OrgID", "t-w4")],
            ),
        ];

        for &(label, ref method, path, hdrs) in cases {
            let (g_snap, g_bytes) =
                fetch(&client, insts[0].http, method.clone(), path, hdrs).await;
            let (r_snap, r_bytes) =
                fetch(&client, insts[1].http, method.clone(), path, hdrs).await;
            assert_eq!(g_snap, r_snap, "{label}: HTTP snapshot mismatch");
            if path == "/mimir/static/mimir-styles.css" {
                assert_eq!(g_bytes, r_bytes, "static asset bytes differ");
            }
            if path == "/mimir/" {
                let plain_client = reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .pool_max_idle_per_host(0)
                    .build()
                    .unwrap();
                let extract_hrefs = |s: &str| -> BTreeSet<String> {
                    let mut out = BTreeSet::new();
                    for part in s.split("href=\"").skip(1) {
                        if let Some(end) = part.find('"') {
                            let h = &part[..end];
                            if !h.contains("bootstrap") && !h.contains("mimir-styles") {
                                out.insert(h.to_string());
                            }
                        }
                    }
                    out
                };
                let g_html = plain_client
                    .get(format!("http://127.0.0.1:{}/mimir/", insts[0].http))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap();
                let r_html = plain_client
                    .get(format!("http://127.0.0.1:{}/mimir/", insts[1].http))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap();
                assert_eq!(
                    extract_hrefs(&g_html),
                    extract_hrefs(&r_html),
                    "GET /mimir/ index hrefs mismatch"
                );
            }
        }

        // Protobuf negotiation + Vary: Accept on /ingester/tsdb_metrics.
        let proto_accept = "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited";
        for inst in &insts {
            let resp = client
                .get(format!(
                    "http://127.0.0.1:{}/mimir/ingester/tsdb_metrics",
                    inst.http
                ))
                .header("X-Scope-OrgID", "t-w4")
                .header("Accept", proto_accept)
                .header("Accept-Encoding", "identity")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 200, "{}", inst.which.name());
            assert_eq!(resp.headers().get("vary").unwrap(), "Accept");
            let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
            assert!(
                ct.starts_with("application/vnd.google.protobuf"),
                "{}: unexpected content-type {ct}",
                inst.which.name()
            );
        }

        // W4-08: /services JSON includes activity-tracker and HTML uses Go time.Time.String() layout.
        let mut svc_maps = Vec::new();
        for inst in &insts {
            let (_, json_bytes) = fetch(
                &client,
                inst.http,
                reqwest::Method::GET,
                "/mimir/services",
                &[("Accept", "application/json")],
            )
            .await;
            let v: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
            let map: BTreeMap<String, String> = v["services"]
                .as_array()
                .unwrap()
                .iter()
                .map(|s| {
                    (
                        s["name"].as_str().unwrap().to_string(),
                        s["status"].as_str().unwrap().to_string(),
                    )
                })
                .collect();
            assert_eq!(
                map.get("activity-tracker").map(String::as_str),
                Some("Running"),
                "{}: missing activity-tracker",
                inst.which.name()
            );
            svc_maps.push(map);

            let (_, html_bytes) =
                fetch(&client, inst.http, reqwest::Method::GET, "/mimir/services", &[]).await;
            let html = String::from_utf8(html_bytes).unwrap();
            assert!(html.contains("<td>activity-tracker</td>"), "{}", inst.which.name());
            assert!(
                html.contains("Current time: ") && !html.contains("Z</p>"),
                "{}: expected Go time.Time.String(), got {html}",
                inst.which.name()
            );
        }
        assert_eq!(svc_maps[0], svc_maps[1], "/mimir/services JSON parity");

        // Compare route labels in cortex_request_duration_seconds_count.
        let route_labels = |metrics: &str| -> BTreeSet<String> {
            metrics
                .lines()
                .filter(|l| {
                    l.starts_with("cortex_request_duration_seconds_count{")
                        && !l.contains("method=\"gRPC\"")
                        && !l.contains("route=\"mimir_metrics\"")
                        && !l.contains("route=\"mimir_ready\"")
                })
                .map(str::to_string)
                .collect()
        };
        let (_, g_metrics) =
            fetch(&client, insts[0].http, reqwest::Method::GET, "/mimir/metrics", &[]).await;
        let (_, r_metrics) =
            fetch(&client, insts[1].http, reqwest::Method::GET, "/mimir/metrics", &[]).await;
        assert_eq!(
            route_labels(&String::from_utf8_lossy(&g_metrics)),
            route_labels(&String::from_utf8_lossy(&r_metrics)),
            "cortex_request_duration_seconds_count route labels mismatch"
        );

        for inst in &mut insts {
            inst.proc.sigterm();
        }
        for inst in &mut insts {
            let st = inst.proc.wait_exit(Duration::from_secs(20)).unwrap();
            assert!(st.success(), "{} exited with {st}", inst.which.name());
        }

        // Slashless http_path_prefix="mimir": gorilla/mux ignores the invalid PathPrefix on the
        // subrouter, so both Go and Rust return 404 on /mimir/ready and 200 on /ready.
        for which in BOTH {
            let http = free_test_port();
            let grpc = free_test_port();
            let dir = tmp.path().join(format!("slashless-{}", which.name()));
            let yaml = base_yaml(http, grpc, &dir, "  http_path_prefix: mimir\n", "", "");
            let mut proc = start_mimir(which, "slashless", &dir, &yaml, &[]);
            wait_ready_at(&client, &mut proc, http, "/ready").await;
            let (code, _, _) = http_get(&client, http, "/mimir/ready", false).await.unwrap();
            assert_eq!(code, 404, "{}: /mimir/ready must be 404", which.name());
            proc.sigterm();
            let _ = proc.wait_exit(Duration::from_secs(10));
        }
    });
}

// -----------------------------------------------------------------------------
// W4-06: LifecyclerConfig rejects heartbeat_period: 0s & heartbeat_timeout: 0s
// -----------------------------------------------------------------------------

#[test]
fn w4_06_zero_heartbeat_period_and_timeout_fail_startup_like_go() {
    let tmp = tempfile::tempdir().unwrap();
    for (case_name, ring_extra, expected_substr) in [
        (
            "zero-period",
            "    heartbeat_period: 0s\n",
            "heartbeat period must be greater than 0",
        ),
        (
            "zero-timeout",
            "    heartbeat_timeout: 0s\n",
            "heartbeat timeout must be greater than 0",
        ),
    ] {
        for which in BOTH {
            let http = free_test_port();
            let grpc = free_test_port();
            let dir = tmp.path().join(format!("{case_name}-{}", which.name()));
            let yaml = base_yaml(http, grpc, &dir, "", ring_extra, "");
            let mut proc = start_mimir(which, case_name, &dir, &yaml, &[]);
            let status = proc
                .wait_exit(Duration::from_secs(15))
                .unwrap_or_else(|| {
                    panic!(
                        "{} did not exit on {case_name}:\n{}",
                        which.name(),
                        proc.log()
                    )
                });
            assert_eq!(
                status.code(),
                Some(1),
                "{} expected exit code 1 on {case_name}, got {status:?}:\n{}",
                which.name(),
                proc.log()
            );
            assert!(
                proc.log().contains(expected_substr),
                "{} log missing {expected_substr:?} on {case_name}:\n{}",
                which.name(),
                proc.log()
            );
        }
    }
}
