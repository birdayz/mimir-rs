//! Black-box binary & CLI/YAML regression tests for Wave 18
//! (W18-KM-09, W18-KM-10, W18-KM-13, W18-SR-04, W18-SR-05).

use std::fs;
use std::io::{BufRead, BufReader};
use std::net::TcpListener as StdTcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mimir_cmd::run_cli_test_mode;
use tempfile::tempdir;

struct RunningMimir {
    child: Child,
    stderr: Arc<Mutex<Vec<String>>>,
    reader: std::thread::JoinHandle<()>,
}

impl RunningMimir {
    fn spawn(args: &[String]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mimir"))
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mimir binary");
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let pipe = child.stderr.take().unwrap();
        let sink = Arc::clone(&stderr);
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(pipe).lines() {
                match line {
                    Ok(l) => sink.lock().unwrap().push(l),
                    Err(_) => break,
                }
            }
        });
        Self {
            child,
            stderr,
            reader,
        }
    }

    fn wait_for_http_addr(&mut self, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            {
                let lines = self.stderr.lock().unwrap();
                for l in lines.iter() {
                    if l.contains("server listening on addresses") {
                        if let Some(rest) = l.split("http=").nth(1) {
                            let addr = rest.split_whitespace().next().unwrap_or("");
                            if !addr.is_empty() {
                                return addr.to_string();
                            }
                        }
                    }
                }
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "mimir exited prematurely with {status}; stderr:
{}",
                    self.stderr.lock().unwrap().join("
")
                );
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for HTTP listen addr; stderr:
{}",
                self.stderr.lock().unwrap().join("
")
            );
            std::thread::sleep(Duration::from_millis(15));
        }
    }

    fn wait_for_line(&mut self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self.stderr.lock().unwrap().iter().any(|l| l.contains(needle)) {
                return;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "mimir exited with {status} before {needle:?}; stderr:
{}",
                    self.stderr.lock().unwrap().join("
")
                );
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {needle:?}; stderr:
{}",
                self.stderr.lock().unwrap().join("
")
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn sigterm_and_wait(self) -> (Option<i32>, Vec<String>) {
        let pid = self.child.id().to_string();
        assert!(Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success());
        self.wait()
    }

    fn wait(mut self) -> (Option<i32>, Vec<String>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                break s;
            }
            if Instant::now() > deadline {
                let _ = self.child.kill();
                panic!(
                    "mimir did not exit; stderr:
{}",
                    self.stderr.lock().unwrap().join("
")
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        self.reader.join().unwrap();
        let lines = self.stderr.lock().unwrap().clone();
        (status.code(), lines)
    }
}

fn free_tcp_port() -> u16 {
    let l = StdTcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn write_base_yaml(dir: &Path, body: &str) -> std::path::PathBuf {
    let p = dir.join("mimir.yaml");
    fs::write(&p, body).unwrap();
    p
}

// ============================================================================
// W18-KM-09 & W18-KM-10: -target=memberlist-kv skips runtime-config and marks
// memberlist-kv Running immediately while fast-joining in background
// ============================================================================

#[tokio::test]
async fn w18_km_09_and_10_memberlist_kv_target_no_runtime_config_and_ready_during_stalled_join() {
    // Stalling blackhole TCP listener: accepts connections and never writes or closes them,
    // holding fast-join in MemberlistKv::starting for the full stream_timeout (4s).
    let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let blackhole_addr = blackhole.local_addr().unwrap().to_string();
    let _bh_task = tokio::spawn(async move {
        let mut conns = Vec::new();
        while let Ok((s, _)) = blackhole.accept().await {
            conns.push(s);
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    let dir = tempdir().unwrap();
    let cfg = write_base_yaml(
        dir.path(),
        &format!(
            r#"
target: memberlist-kv
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
memberlist:
  node_name: seed-1
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: 0
  stream_timeout: 4s
  join_members: [{blackhole_addr}]
  min_join_backoff: 200ms
  max_join_backoff: 200ms
  max_join_retries: 1
runtime_config:
  file: /nonexistent/w18-km-09-must-not-load-runtime.yaml
activity_tracker:
  filepath: ""
"#
        ),
    );

    let mut proc = RunningMimir::spawn(&[format!("-config.file={}", cfg.display())]);
    let http_addr = proc.wait_for_http_addr(Duration::from_secs(10));
    let client = reqwest::Client::new();

    // Must become ready IMMEDIATELY (< 1.5s) even though fast-join is stalled for 4s.
    let start = Instant::now();
    let ready_resp = loop {
        if let Ok(r) = client.get(format!("http://{http_addr}/ready")).send().await {
            if r.status() == reqwest::StatusCode::OK {
                break r;
            }
        }
        assert!(
            start.elapsed() < Duration::from_millis(1500),
            "/ready must return 200 immediately for -target=memberlist-kv while fast-join stalls"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(ready_resp.status(), reqwest::StatusCode::OK);

    // W18-KM-10: /services reports memberlist-kv => Running, and W18-KM-09: no runtime-config.
    let services_body = client
        .get(format!("http://{http_addr}/services"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        services_body.contains("<td>memberlist-kv</td>") && services_body.contains("<td>Running</td>"),
        "services:\n{services_body}"
    );
    assert!(
        !services_body.contains("runtime-config"),
        "runtime-config must not be registered on -target=memberlist-kv:
{services_body}"
    );

    // W18-KM-09: /runtime_config, /api/v1/user_limits return 404; / omits Current runtime config;
    // /metrics omits cortex_runtime_config_*.
    let rc_status = client
        .get(format!("http://{http_addr}/runtime_config"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(rc_status, reqwest::StatusCode::NOT_FOUND);

    let ul_status = client
        .get(format!("http://{http_addr}/api/v1/user_limits"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(ul_status, reqwest::StatusCode::NOT_FOUND);

    let index_body = client
        .get(format!("http://{http_addr}/"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        !index_body.contains("Current runtime config"),
        "index must omit runtime config link:
{index_body}"
    );

    let metrics_body = client
        .get(format!("http://{http_addr}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!metrics_body.contains("cortex_runtime_config_hash"));
    assert!(!metrics_body.contains("cortex_runtime_config_last_reload_successful"));

    let (code, _lines) = proc.sigterm_and_wait();
    assert_eq!(code, Some(0));
}

#[tokio::test]
async fn w18_km_10_symmetric_two_seed_bootstrap_and_abort_on_fast_join_failure() {
    // Part A: Symmetric two-seed bootstrap where seed A and seed B target each other's
    // pre-allocated memberlist ports (both must report /ready 200 immediately and join cleanly).
    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let dir = tempdir().unwrap();

    let cfg_a = write_base_yaml(
        &dir.path().join("a").tap_mkdir(),
        &format!(
            r#"
target: memberlist-kv
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
memberlist:
  node_name: sym-a
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {port_a}
  advertise_addr: 127.0.0.1
  advertise_port: {port_a}
  join_members: [127.0.0.1:{port_b}]
  min_join_backoff: 50ms
  max_join_backoff: 100ms
activity_tracker:
  filepath: ""
"#
        ),
    );
    let cfg_b = write_base_yaml(
        &dir.path().join("b").tap_mkdir(),
        &format!(
            r#"
target: memberlist-kv
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
memberlist:
  node_name: sym-b
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {port_b}
  advertise_addr: 127.0.0.1
  advertise_port: {port_b}
  join_members: [127.0.0.1:{port_a}]
  min_join_backoff: 50ms
  max_join_backoff: 100ms
activity_tracker:
  filepath: ""
"#
        ),
    );

    let mut proc_a = RunningMimir::spawn(&[format!("-config.file={}", cfg_a.display())]);
    let mut proc_b = RunningMimir::spawn(&[format!("-config.file={}", cfg_b.display())]);
    let http_a = proc_a.wait_for_http_addr(Duration::from_secs(10));
    let http_b = proc_b.wait_for_http_addr(Duration::from_secs(10));

    let client = reqwest::Client::new();
    for addr in [&http_a, &http_b] {
        let r = client.get(format!("http://{addr}/ready")).send().await.unwrap();
        assert_eq!(r.status(), reqwest::StatusCode::OK);
    }

    // Both nodes must discover each other via /memberlist.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let body = client
            .get(format!("http://{http_a}/memberlist"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        if body.contains("sym-a") && body.contains("sym-b") {
            break;
        }
        assert!(Instant::now() < deadline, "sym-a did not discover sym-b:
{body}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(proc_a.sigterm_and_wait().0, Some(0));
    assert_eq!(proc_b.sigterm_and_wait().0, Some(0));

    // Part B: abort_if_cluster_fast_join_fails: true on -target=memberlist-kv still fails the process.
    let fail_cfg = write_base_yaml(
        &dir.path().join("fail").tap_mkdir(),
        r#"
target: memberlist-kv
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
memberlist:
  node_name: fail-seed
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: 0
  join_members: [127.0.0.1:1]
  abort_if_cluster_fast_join_fails: true
  abort_if_cluster_fast_join_fails_min_nodes: 1
activity_tracker:
  filepath: ""
"#,
    );
    let proc_fail = RunningMimir::spawn(&[format!("-config.file={}", fail_cfg.display())]);
    let (code, lines) = proc_fail.wait();
    assert_eq!(code, Some(1), "stderr:
{}", lines.join("
"));
    assert!(
        lines
            .iter()
            .any(|l| l.contains("module=memberlist-kv") && l.contains("level=error")),
        "expected memberlist-kv failure log; stderr:
{}",
        lines.join("
")
    );
}

#[tokio::test]
async fn w18_km_10_ingester_still_awaits_memberlist_kv_readiness_before_ready_200() {
    let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let blackhole_addr = blackhole.local_addr().unwrap().to_string();
    let _bh_task = tokio::spawn(async move {
        let mut conns = Vec::new();
        while let Ok((s, _)) = blackhole.accept().await {
            conns.push(s);
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    let dir = tempdir().unwrap();
    let cfg = write_base_yaml(
        dir.path(),
        &format!(
            r#"
target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
memberlist:
  node_name: ing-wait
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: 0
  stream_timeout: 1500ms
  join_members: [{blackhole_addr}]
  max_join_retries: 0
ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: ing-wait
    instance_addr: 127.0.0.1
    num_tokens: 4
    replication_factor: 1
    min_ready_duration: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {dir}/bucket
  tsdb:
    dir: {dir}/tsdb
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
            dir = dir.path().display()
        ),
    );

    let mut proc = RunningMimir::spawn(&[format!("-config.file={}", cfg.display())]);
    let http_addr = proc.wait_for_http_addr(Duration::from_secs(10));
    let client = reqwest::Client::new();

    // While MemberlistKv fast-join is stalled (1.5s), /ready MUST stay 503 (ingester-ring waits
    // on await_kv_running_or_stopping).
    let r = client.get(format!("http://{http_addr}/ready")).send().await.unwrap();
    assert_eq!(
        r.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "ingester /ready must stay 503 while MemberlistKv fast-join is in progress"
    );

    proc.wait_for_line(r#"msg="Application started""#, Duration::from_secs(15));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let r = client.get(format!("http://{http_addr}/ready")).send().await.unwrap();
        if r.status() == reqwest::StatusCode::OK {
            break;
        }
        assert!(Instant::now() < deadline, "ingester did not become ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(proc.sigterm_and_wait().0, Some(0));
}

// ============================================================================
// W18-KM-13, W18-SR-04, W18-SR-05: CLI / YAML validation checks
// ============================================================================

#[test]
fn w18_km_13_and_sr_04_05_cli_and_yaml_validation() {
    let dir = tempdir().unwrap();
    let ml_cfg = write_base_yaml(
        dir.path(),
        r#"
target: memberlist-kv
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
activity_tracker:
  filepath: ""
"#,
    );

    // W18-KM-13: -target=memberlist-kv -memberlist.watch-prefix-buffer-size=0 fails with Go message.
    let proc = RunningMimir::spawn(&[
        format!("-config.file={}", ml_cfg.display()),
        "-memberlist.watch-prefix-buffer-size=0".to_string(),
    ]);
    let (code, lines) = proc.wait();
    assert_eq!(code, Some(1), "stderr:
{}", lines.join("
"));
    let joined = lines.join("
");
    assert!(
        joined.contains("failed to initialise memberlist client instance:")
            && joined.contains("invalid WatchPrefixBufferSize: must be greater than 0"),
        "unexpected stderr:\n{joined}"
    );

    // W18-SR-04 & W18-SR-05 via CLI test mode.
    let ok = run_cli_test_mode(
        "mimir",
        &[
            format!("-config.file={}", ml_cfg.display()),
            "-ingester.client.initial-stream-window-size=1.5MiB".to_string(),
            "-ingest-storage.kafka.sasl-oauthbearer-extensions=null".to_string(),
            r#"-ingest-storage.kafka.sasl-oauthbearer-extensions={"k":null}"#.to_string(),
            "-print.config".to_string(),
        ],
    );
    assert_eq!(ok.exit_code, 0, "stderr:
{}", ok.stderr);

    for (flag, want_err) in [
        (
            "-ingester.client.initial-stream-window-size=64K",
            "units: unknown unit K in 64K",
        ),
        (
            "-ingester.client.initial-stream-window-size=65536",
            "units: unknown unit  in 65536",
        ),
    ] {
        let bad = run_cli_test_mode(
            "mimir",
            &[format!("-config.file={}", ml_cfg.display()), flag.to_string()],
        );
        assert_eq!(bad.exit_code, 2, "stderr:
{}", bad.stderr);
        assert!(bad.stderr.contains(want_err), "stderr:
{}", bad.stderr);
    }

    // YAML integer for flagext.Bytes rejected like Go.
    let bad_yaml = dir.path().join("bad_bytes.yaml");
    fs::write(
        &bad_yaml,
        "ingester_client:
  grpc_client_config:
    initial_stream_window_size: 1048576
",
    )
    .unwrap();
    let bad_res = run_cli_test_mode(
        "mimir",
        &[format!("-config.file={}", bad_yaml.display())],
    );
    assert_eq!(bad_res.exit_code, 1, "stderr:
{}", bad_res.stderr);
    assert!(
        bad_res.stderr.contains("units: unknown unit  in 1048576"),
        "stderr:
{}",
        bad_res.stderr
    );
}

trait PathExt {
    fn tap_mkdir(self) -> Self;
}

impl PathExt for std::path::PathBuf {
    fn tap_mkdir(self) -> Self {
        fs::create_dir_all(&self).unwrap();
        self
    }
}
