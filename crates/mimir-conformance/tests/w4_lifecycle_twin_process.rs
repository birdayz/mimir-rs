//! Startup / readiness / shutdown orchestration: differential twin-process tests.
//!
//! The unmodified Go `mimir` (`target/go-mimir`) and the Rust `mimir` are booted as
//! `-target=ingester` with identical configuration; Go defines the expected behavior.
//!
//! Upstream behavior under test:
//! - github.com/grafana/mimir/pkg/mimir/sanity_check.go (`runSanityCheck`; `Server` depends on
//!   `SanityCheck`, so nothing is served until it passes) — hunt LC-5
//! - github.com/grafana/mimir/pkg/mimir/mimir.go (`readyHandler`, `servicesHandler`, gRPC health
//!   `WithManager`, signal handler: `-shutdown-delay`, keep-alives disabled) — LC-12/13/14
//! - github.com/grafana/dskit/ring/lifecycler.go (`stopping`: LEAVING, then `Flush()` while
//!   heartbeating, then unregister) + pkg/ingester (`Ingester` is the FlushTransferer) — LC-4
//!
//! Needs only the two binaries and loopback networking (no Docker).

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::twin::{
    free_tcp_ports, grpc_health, http_get, ingester_ring, ring_metric_series, services,
    start_mimir, wait_ready, FakeS3, Impl, Proc,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use tonic::metadata::MetadataValue;

const BOTH: [Impl; 2] = [Impl::Go, Impl::Rust];

struct Ports {
    http: u16,
    grpc: u16,
}

fn ports() -> Ports {
    let p = free_tcp_ports(2);
    Ports {
        http: p[0],
        grpc: p[1],
    }
}

/// `-target=ingester` with an in-memory ring KV and the given `blocks_storage` backend block.
fn ingester_yaml(
    p: &Ports,
    dir: &Path,
    id: &str,
    blocks_backend: &str,
    tsdb_extra: &str,
    extra: &str,
) -> String {
    format!(
        r#"target: ingester
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
blocks_storage:
{blocks_backend}
  tsdb:
    dir: {tsdb}
{tsdb_extra}activity_tracker:
  filepath: ""
{extra}"#,
        http = p.http,
        grpc = p.grpc,
        tsdb = dir.join("tsdb").display(),
    )
}

fn filesystem_backend(dir: &Path) -> String {
    format!(
        "  backend: filesystem\n  filesystem:\n    dir: {}\n",
        dir.join("bucket").display()
    )
}

fn s3_backend(endpoint: &str) -> String {
    format!(
        "  backend: s3\n  s3:\n    endpoint: {endpoint}\n    bucket_name: blocks\n    region: us-east-1\n    access_key_id: conformance\n    secret_access_key: conformance-secret\n    insecure: true\n"
    )
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap()
}

fn assert_exit_ok(proc: &mut Proc, timeout: Duration) {
    match proc.wait_exit(timeout) {
        Some(s) => assert!(s.success(), "{} exited with {s}:\n{}", proc.name, proc.log()),
        None => panic!("{} did not exit within {timeout:?}:\n{}", proc.name, proc.log()),
    }
}

// ---------------------------------------------------------------------------------------------
// LC-5: sanity check
// ---------------------------------------------------------------------------------------------

/// An unwritable TSDB directory fails the sanity check: the process never becomes ready and
/// exits non-zero.
#[test]
fn lc5_sanity_check_unwritable_tsdb_dir_fails_startup() {
    use std::os::unix::fs::PermissionsExt;
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let ro = tmp.path().join("readonly");
        std::fs::create_dir_all(&ro).unwrap();
        std::fs::set_permissions(&ro, std::fs::Permissions::from_mode(0o555)).unwrap();
        if std::fs::write(ro.join("probe"), b"").is_ok() {
            eprintln!("SKIPPED lc5_sanity_check_unwritable_tsdb_dir_fails_startup: running with permission bypass (root)");
            return;
        }
        let client = http_client();
        for which in BOTH {
            let p = ports();
            let dir = tmp.path().join(which.name());
            let yaml = ingester_yaml(&p, &dir, "ingester-1", &filesystem_backend(&dir), "", "")
                .replace(
                    &format!("dir: {}", dir.join("tsdb").display()),
                    &format!("dir: {}", ro.join("tsdb").display()),
                );
            let mut proc = start_mimir(which, "dir-check", &dir, &yaml, &[]);
            let status = proc
                .wait_exit(Duration::from_secs(30))
                .unwrap_or_else(|| panic!("{} still running:\n{}", proc.name, proc.log()));
            assert!(!status.success(), "{} exited successfully:\n{}", proc.name, proc.log());
            let log = proc.log();
            assert!(log.contains("Unable to access directory"), "{}:\n{log}", proc.name);
            assert!(log.contains(&format!("failed to access directory {}", ro.join("tsdb").display())), "{}:\n{log}", proc.name);
            assert!(http_get(&client, p.http, "/ready", false).await.is_err());
        }
    });
}

/// With a reachable object storage the sanity check reads `sanity-check-at-startup` (and
/// treats "not found" as success) before anything is served.
#[test]
fn lc5_sanity_check_reads_object_storage_before_serving() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        for which in BOTH {
            let s3 = FakeS3::start(free_tcp_ports(1)[0], None).await;
            let p = ports();
            let dir = tmp.path().join(which.name());
            let yaml = ingester_yaml(&p, &dir, "ingester-1", &s3_backend(&s3.endpoint()), "", "");
            let mut proc = start_mimir(which, "s3-ok", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, p.http, Duration::from_secs(60)).await;
            let reqs = s3.requests();
            assert!(
                reqs.iter().any(|r| r.method == "GET" && r.path == "/blocks/sanity-check-at-startup"),
                "{}: sanity check GET missing: {reqs:?}",
                proc.name
            );
            proc.sigterm();
            assert_exit_ok(&mut proc, Duration::from_secs(30));
        }
    });
}

/// With an unreachable object storage the sanity check keeps retrying and nothing is served
/// (Go: `Server` depends on `SanityCheck`); SIGTERM during the check is a requested shutdown
/// and exits 0.
#[test]
fn lc5_sanity_check_unreachable_object_storage_blocks_serving() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        let mut procs = Vec::new();
        for which in BOTH {
            // A port nothing listens on.
            let dead = free_tcp_ports(1)[0];
            let p = ports();
            let dir = tmp.path().join(which.name());
            let yaml = ingester_yaml(&p, &dir, "ingester-1", &s3_backend(&format!("127.0.0.1:{dead}")), "", "");
            procs.push((start_mimir(which, "s3-dead", &dir, &yaml, &[]), p));
        }
        let deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < deadline {
            for (proc, p) in procs.iter_mut() {
                assert!(proc.try_wait().is_none(), "{} exited early:\n{}", proc.name, proc.log());
                if let Ok((code, _, _)) = http_get(&client, p.http, "/ready", false).await {
                    panic!("{} served /ready ({code}) during the sanity check:\n{}", proc.name, proc.log());
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        for (proc, _) in procs.iter_mut() {
            let log = proc.log();
            assert!(
                log.contains("Unable to successfully connect to configured object storage (will retry)"),
                "{}:\n{log}",
                proc.name
            );
            proc.sigterm();
        }
        for (proc, _) in procs.iter_mut() {
            assert_exit_ok(proc, Duration::from_secs(30));
        }
    });
}

// ---------------------------------------------------------------------------------------------
// LC-12 / LC-13: /services, /ready and gRPC health follow the module states
// ---------------------------------------------------------------------------------------------

#[test]
fn lc13_services_ready_and_grpc_health_follow_module_states() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        let mut running = Vec::new();
        for which in BOTH {
            let p = ports();
            let dir = tmp.path().join(which.name());
            // Usage stats enabled (the default, also with Helm's installation_mode=helm): the
            // `usage-stats` module is part of the service map. Nothing is sent within the test:
            // the first report is due 4h after the cluster seed is created.
            let yaml = ingester_yaml(&p, &dir, "ingester-1", &filesystem_backend(&dir), "", "")
                .replace("usage_stats:\n  enabled: false\n", "usage_stats:\n  installation_mode: helm\n");
            let mut proc = start_mimir(which, "services", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, p.http, Duration::from_secs(60)).await;
            running.push((which, proc, p));
        }

        let snapshot = |label: &'static str| {
            let client = client.clone();
            let ports: Vec<(Impl, u16, u16)> = running.iter().map(|(w, _, p)| (*w, p.http, p.grpc)).collect();
            async move {
                let mut out = Vec::new();
                for (which, http, grpc) in ports {
                    let svcs = services(&client, http).await.unwrap();
                    let ready = http_get(&client, http, "/ready", false).await.unwrap();
                    let health = grpc_health(grpc).await.unwrap();
                    out.push((which, svcs, (ready.0, ready.2), health));
                }
                (label, out)
            }
        };

        {
            let (label, out) = snapshot("running").await;
            let (_, go_svcs, go_ready, go_health) = &out[0];
            let (_, rs_svcs, rs_ready, rs_health) = &out[1];
            assert_eq!(go_svcs, rs_svcs, "{label}: /services");
            assert_eq!(go_ready, rs_ready, "{label}: /ready");
            assert_eq!(go_health, rs_health, "{label}: gRPC health");
            assert_eq!(*go_health, 1, "{label}: SERVING");
        }

        // `/ingester/shutdown` terminates only the ingester service.
        for (_, _, p) in &running {
            let resp = client
                .post(format!("http://127.0.0.1:{}/ingester/shutdown", p.http))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status().as_u16(), 204);
        }
        let (label, out) = snapshot("after /ingester/shutdown").await;
        let (_, go_svcs, go_ready, go_health) = &out[0];
        let (_, rs_svcs, rs_ready, rs_health) = &out[1];
        assert_eq!(go_svcs.get("ingester-service").map(String::as_str), Some("Terminated"));
        assert_eq!(go_svcs.get("usage-stats").map(String::as_str), Some("Running"));
        assert_eq!(go_svcs, rs_svcs, "{label}: /services");
        assert_eq!(go_ready.0, 503);
        // Go lists not-Running modules in map order: compare the set of lines.
        let lines = |s: &str| {
            let mut v: Vec<String> = s.lines().map(str::to_string).collect();
            v.sort();
            v
        };
        assert_eq!(go_ready.0, rs_ready.0, "{label}: /ready status");
        assert_eq!(lines(&go_ready.1), lines(&rs_ready.1), "{label}: /ready body");
        assert_eq!(go_health, rs_health, "{label}: gRPC health");
        assert_eq!(*go_health, 1, "{label}: still SERVING");

        for (_, proc, _) in running.iter_mut() {
            proc.sigterm();
        }
        for (_, proc, _) in running.iter_mut() {
            assert_exit_ok(proc, Duration::from_secs(30));
        }
    });
}

// ---------------------------------------------------------------------------------------------
// LC-14: -shutdown-delay and keep-alives
// ---------------------------------------------------------------------------------------------

#[test]
fn lc14_shutdown_delay_reports_not_ready_and_closes_connections() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        let mut running = Vec::new();
        for which in BOTH {
            let p = ports();
            let dir = tmp.path().join(which.name());
            let yaml = ingester_yaml(&p, &dir, "ingester-1", &filesystem_backend(&dir), "", "shutdown_delay: 5s\n");
            let mut proc = start_mimir(which, "shutdown-delay", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, p.http, Duration::from_secs(60)).await;
            running.push((proc, p));
        }
        for (proc, _) in &running {
            proc.sigterm();
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let mut seen = Vec::new();
        for (proc, p) in running.iter_mut() {
            assert!(proc.try_wait().is_none(), "{} exited during -shutdown-delay", proc.name);
            let (code, headers, body) = http_get(&client, p.http, "/ready", false).await.unwrap();
            let conn = headers
                .get("connection")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_ascii_lowercase();
            seen.push((code, body, conn));
        }
        assert_eq!(seen[0], seen[1], "go vs rust /ready during -shutdown-delay");
        assert_eq!(seen[0].0, 503);
        assert_eq!(seen[0].1, "Application is stopping\n");
        assert_eq!(seen[0].2, "close");
        for (proc, _) in running.iter_mut() {
            assert_exit_ok(proc, Duration::from_secs(30));
        }
    });
}

// ---------------------------------------------------------------------------------------------
// LC-4: flush on shutdown happens while the instance is LEAVING
// ---------------------------------------------------------------------------------------------

#[test]
fn lc4_flush_on_shutdown_ships_blocks_while_leaving() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        for which in BOTH {
            let p = ports();
            // Ring state of this instance as seen by its own /ingester/ring while the first
            // block upload of the shutdown flush is in flight.
            let observed: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
            let hook = {
                let observed = Arc::clone(&observed);
                let client = client.clone();
                let http = p.http;
                Arc::new(move |_path: String| {
                    let observed = Arc::clone(&observed);
                    let client = client.clone();
                    Box::pin(async move {
                        if observed.lock().unwrap().is_some() {
                            return;
                        }
                        let state = ingester_ring(&client, http)
                            .await
                            .map(|r| r.get("ingester-1").map(|(s, _)| s.clone()).unwrap_or_else(|| "ABSENT".into()))
                            .unwrap_or_else(|e| format!("ERROR {e}"));
                        *observed.lock().unwrap() = Some(state);
                    }) as futures::future::BoxFuture<'static, ()>
                }) as Arc<dyn Fn(String) -> futures::future::BoxFuture<'static, ()> + Send + Sync>
            };
            let s3 = FakeS3::start(free_tcp_ports(1)[0], Some(hook)).await;
            let dir = tmp.path().join(which.name());
            let yaml = ingester_yaml(
                &p,
                &dir,
                "ingester-1",
                &s3_backend(&s3.endpoint()),
                "    flush_blocks_on_shutdown: true\n    ship_interval: 1m\n",
                "",
            );
            let mut proc = start_mimir(which, "flush", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, p.http, Duration::from_secs(60)).await;

            let mut ing = IngesterClient::connect(format!("http://127.0.0.1:{}", p.grpc)).await.unwrap();
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
            let mut req = tonic::Request::new(cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_static("__name__", "lc4_flush_test")],
                    samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: ts }],
                    ..Default::default()
                }],
                ..Default::default()
            });
            req.metadata_mut().insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::from_static("lc4"));
            ing.push(req).await.unwrap();
            drop(ing);

            proc.sigterm();
            assert_exit_ok(&mut proc, Duration::from_secs(60));
            let puts: Vec<_> = s3.requests().into_iter().filter(|r| r.method == "PUT").collect();
            assert!(!puts.is_empty(), "{}: no block shipped on shutdown flush:\n{}", proc.name, proc.log());
            assert!(puts.iter().any(|r| r.path.starts_with("/blocks/lc4/")), "{}: {puts:?}", proc.name);
            assert_eq!(
                observed.lock().unwrap().as_deref(),
                Some("LEAVING"),
                "{}: ring state while flushing",
                proc.name
            );
        }
    });
}

// ---------------------------------------------------------------------------------------------
// LC-8: ring / lifecycler / KV metrics names and labels
// ---------------------------------------------------------------------------------------------

#[test]
fn lc8_ring_lifecycler_and_kv_metrics_match() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();
        let mut series = Vec::new();
        for which in BOTH {
            let p = ports();
            let dir = tmp.path().join(which.name());
            let yaml = ingester_yaml(&p, &dir, "ingester-1", &filesystem_backend(&dir), "", "")
                .replace("store: inmemory", "store: memberlist")
                + &format!(
                    "memberlist:\n  bind_addr: [127.0.0.1]\n  bind_port: {}\n  advertise_addr: 127.0.0.1\n",
                    free_tcp_ports(1)[0]
                );
            let mut proc = start_mimir(which, "metrics", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, p.http, Duration::from_secs(60)).await;
            // Let at least one heartbeat / ring update happen.
            tokio::time::sleep(Duration::from_secs(1)).await;
            let (code, _, body) = http_get(&client, p.http, "/metrics", false).await.unwrap();
            assert_eq!(code, 200);
            series.push((proc.name.clone(), ring_metric_series(&body)));
            proc.sigterm();
            assert_exit_ok(&mut proc, Duration::from_secs(30));
        }
        let (go_name, go) = &series[0];
        let (rs_name, rs) = &series[1];
        let only_go: Vec<_> = go.difference(rs).collect();
        let only_rs: Vec<_> = rs.difference(go).collect();
        assert!(
            only_go.is_empty() && only_rs.is_empty(),
            "ring/lifecycler/kv metric series differ\nonly in {go_name}: {only_go:#?}\nonly in {rs_name}: {only_rs:#?}"
        );
        assert!(go.iter().any(|s| s.starts_with("cortex_ring_members{")), "{go:#?}");
    });
}
