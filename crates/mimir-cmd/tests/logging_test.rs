//! Black-box tests of the `mimir` binary's log output: every line (including `tracing` events
//! from the ring, lifecycler and memberlist crates) must be go-kit logfmt/json as written by Go
//! Mimir's `util_log.InitLogger` (`pkg/util/log/log.go`), honouring `-log.level`/`-log.format`,
//! and fatal errors must be logged like `util_log.CheckFatal` (`cmd/mimir/main.go`).

use std::fs;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::tempdir;

fn free_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Minimal memberlist-backed ingester whose memberlist join target (`127.0.0.1:1`) refuses
/// connections, so the binary logs memberlist join failures, then becomes ACTIVE alone.
fn write_memberlist_ingester_config(dir: &Path) -> std::path::PathBuf {
    let config_path = dir.join("mimir.yaml");
    fs::write(
        &config_path,
        format!(
            r#"
target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
memberlist:
  node_name: logtest
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {ml}
  join_members: [127.0.0.1:1]
  min_join_backoff: 50ms
  max_join_backoff: 50ms
  max_join_retries: 2
ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: logtest
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
            http = free_tcp_port(),
            grpc = free_tcp_port(),
            ml = free_tcp_port(),
            dir = dir.display(),
        ),
    )
    .unwrap();
    config_path
}

struct Running {
    child: Child,
    stderr: Arc<Mutex<Vec<String>>>,
    reader: std::thread::JoinHandle<()>,
}

fn spawn(args: &[String]) -> Running {
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
    Running {
        child,
        stderr,
        reader,
    }
}

impl Running {
    fn wait_for_line(&mut self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self.stderr.lock().unwrap().iter().any(|l| l.contains(needle)) {
                return;
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "mimir exited with {status} before logging {needle:?}; stderr:\n{}",
                    self.stderr.lock().unwrap().join("\n")
                );
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {needle:?}; stderr:\n{}",
                self.stderr.lock().unwrap().join("\n")
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
                    "mimir did not exit; stderr:\n{}",
                    self.stderr.lock().unwrap().join("\n")
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        self.reader.join().unwrap();
        let lines = self.stderr.lock().unwrap().clone();
        (status.code(), lines)
    }
}

/// `ts=<RFC3339Nano UTC> caller=<file>:<line> level=<level> ...` (go-kit `log.With(logger, "ts",
/// log.DefaultTimestampUTC, "caller", spanlogger.Caller(n))` + `level.X(...)`).
fn assert_go_logfmt_prefix(line: &str) {
    let mut parts = line.splitn(4, ' ');
    let ts = parts.next().unwrap_or_default();
    let caller = parts.next().unwrap_or_default();
    let level = parts.next().unwrap_or_default();
    assert!(
        ts.starts_with("ts=20") && ts.ends_with('Z') && ts.contains('T'),
        "bad ts in {line:?}"
    );
    let c = caller.strip_prefix("caller=").unwrap_or_else(|| panic!("no caller in {line:?}"));
    let (file, lineno) = c.rsplit_once(':').unwrap_or_else(|| panic!("bad caller in {line:?}"));
    assert!(file.ends_with(".rs") && !file.contains('/'), "bad caller file in {line:?}");
    assert!(lineno.parse::<u32>().is_ok(), "bad caller line in {line:?}");
    assert!(
        matches!(level, "level=debug" | "level=info" | "level=warn" | "level=error"),
        "bad level in {line:?}"
    );
}

/// Asserts that `needles` appear, in this order, as substrings of distinct lines.
fn assert_in_order(lines: &[String], needles: &[&str]) {
    let mut idx = 0;
    for needle in needles {
        match lines[idx..].iter().position(|l| l.contains(needle)) {
            Some(p) => idx += p + 1,
            None => panic!(
                "missing (in order) {needle:?}; stderr:\n{}",
                lines.join("\n")
            ),
        }
    }
}

#[test]
fn test_binary_logs_tracing_events_in_go_logfmt_through_sigterm() {
    let dir = tempdir().unwrap();
    let cfg = write_memberlist_ingester_config(dir.path());
    let mut run = spawn(&[format!("-config.file={}", cfg.display())]);
    run.wait_for_line("msg=\"Application started\"", Duration::from_secs(30));
    // The retrying join runs in the background (abort_if_cluster_join_fails=false): wait for its
    // final outcome before stopping.
    run.wait_for_line("msg=\"joining memberlist cluster failed\"", Duration::from_secs(30));
    let (code, lines) = run.sigterm_and_wait();
    assert_eq!(code, Some(0), "stderr:\n{}", lines.join("\n"));

    for l in &lines {
        assert_go_logfmt_prefix(l);
    }

    // Startup + SIGTERM sequence, with Go's messages and key order (compare with a Go
    // `mimir -target=ingester` log for the same config).
    assert_in_order(
        &lines,
        &[
            "level=info msg=\"Starting application\" version=\"(version=",
            "level=info msg=\"server listening on addresses\"",
            "level=info msg=starting module=usage-stats",
            "level=info msg=starting module=sanity-check",
            "level=info msg=\"Checking directories read/write access\"",
            "level=info msg=\"Object storage config successfully checked\"",
            "level=info msg=starting module=server",
            "level=info msg=starting module=memberlist-kv",
            "level=info component=memberlist msg=\"Using memberlist cluster label and node name\" cluster_label= node=logtest",
            "level=info component=memberlist msg=\"memberlist fast-join starting\" nodes_found=1 to_join=",
            "level=info component=memberlist msg=\"fast-joining node failed\" node=127.0.0.1:1 err=",
            "level=warn component=memberlist msg=\"memberlist fast-join failed to reach minimum required seed nodes\" joined_nodes=0 required_nodes=1 elapsed_time=",
            "level=error component=memberlist msg=\"failed to fast-join the memberlist cluster at startup\" err=",
            "level=info msg=starting module=ingester-ring",
            "level=info msg=\"ring doesn't exist in KV store yet\"",
            "level=info msg=starting module=ingester-service",
            "level=info msg=\"not loading tokens from file, tokens file path is empty\"",
            "level=info msg=\"instance not found in ring, adding with no tokens\" ring=ingester",
            "level=info msg=\"Application started\"",
            "level=info msg=\"=== received SIGINT/SIGTERM ===\\n*** exiting\"",
            "level=info msg=\"module stopped\" module=active-groups-cleanup-service",
            "level=info msg=\"lifecycler loop() exited gracefully\" ring=ingester",
            "level=info msg=\"changing instance state from\" old_state=ACTIVE new_state=LEAVING ring=ingester",
            "level=info msg=\"lifecycler entering final sleep before shutdown\" final_sleep=0s",
            "level=info msg=\"instance removed from the KV store\" ring=ingester",
            "level=info msg=\"module stopped\" module=ingester-service",
            "level=info msg=\"module stopped\" module=ingester-ring",
            "level=info component=memberlist msg=\"leaving memberlist cluster\"",
            "level=info msg=\"module stopped\" module=memberlist-kv",
            "level=info msg=\"server stopped\"",
            "level=info msg=\"module stopped\" module=server",
            "level=info msg=\"module stopped\" module=sanity-check",
            "level=info msg=\"module stopped\" module=usage-stats",
            "level=info msg=\"Application stopped\"",
        ],
    );
    // Background join with retries (dskit joinMembersOnStartup / joinMembersWithRetries).
    assert_in_order(
        &lines,
        &[
            "level=info component=memberlist phase=startup msg=\"joining memberlist cluster\" join_members=127.0.0.1:1",
            "level=warn component=memberlist phase=startup msg=\"joining memberlist cluster\" attempts=1 max_attempts=2 err=",
            "level=warn component=memberlist phase=startup msg=\"joining memberlist cluster\" attempts=2 max_attempts=2 err=",
            "level=error component=memberlist phase=startup msg=\"joining memberlist cluster failed\" err=",
        ],
    );
}

#[test]
fn test_binary_log_format_json_and_level_filter() {
    let dir = tempdir().unwrap();
    let cfg = write_memberlist_ingester_config(dir.path());
    let mut run = spawn(&[
        format!("-config.file={}", cfg.display()),
        "-log.format=json".to_string(),
        "-log.level=warn".to_string(),
    ]);
    run.wait_for_line("joining memberlist cluster failed", Duration::from_secs(30));
    let (code, lines) = run.sigterm_and_wait();
    assert_eq!(code, Some(0), "stderr:\n{}", lines.join("\n"));
    assert!(!lines.is_empty());

    let mut saw_join_warn = false;
    for l in &lines {
        let v: serde_json::Value = serde_json::from_str(l)
            .unwrap_or_else(|e| panic!("not a JSON log line ({e}): {l:?}"));
        let obj = v.as_object().unwrap();
        for key in ["ts", "caller", "level", "msg"] {
            assert!(obj.contains_key(key), "missing {key} in {l:?}");
        }
        // encoding/json sorts map keys.
        let keys: Vec<&String> = obj.keys().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "{l:?}");
        assert!(
            matches!(obj["level"].as_str(), Some("warn" | "error")),
            "-log.level=warn must filter {l:?}"
        );
        if obj["msg"] == "joining memberlist cluster" && obj["level"] == "warn" {
            saw_join_warn = true;
            assert_eq!(obj["component"], "memberlist");
            assert_eq!(obj["phase"], "startup");
            assert!(obj["attempts"].is_u64(), "integers stay JSON numbers: {l:?}");
        }
    }
    assert!(saw_join_warn, "stderr:\n{}", lines.join("\n"));
}

#[test]
fn test_binary_fatal_error_logged_like_check_fatal() {
    let dir = tempdir().unwrap();
    let cfg = dir.path().join("mimir.yaml");
    fs::write(
        &cfg,
        format!(
            r#"
target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
ingester:
  ring:
    kvstore:
      store: consul
      consul:
        host: 127.0.0.1:1
    instance_id: fataltest
    instance_addr: 127.0.0.1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {dir}/bucket
  tsdb:
    dir: {dir}/tsdb
activity_tracker:
  filepath: ""
"#,
            http = free_tcp_port(),
            grpc = free_tcp_port(),
            dir = dir.path().display(),
        ),
    )
    .unwrap();
    let run = spawn(&[format!("-config.file={}", cfg.display())]);
    let (code, lines) = run.wait();
    assert_eq!(code, Some(1), "stderr:\n{}", lines.join("\n"));
    for l in &lines {
        assert_go_logfmt_prefix(l);
    }
    // Go: the service manager logs every failed module (the ring, then the ingester waiting
    // for it), stops the running ones, and `Run` returns "failed services".
    assert_in_order(
        &lines,
        &[
            "level=error msg=\"module failed\" module=ingester-ring err=\"starting module ingester-ring: invalid service state: Failed, expected: Running, failure: unable to initialise ring state: ",
            "level=error msg=\"module failed\" module=ingester-service err=\"failed to start ingester-service, because it depends on module ingester-ring, which has failed: invalid service state: Failed, expected: Running, failure: starting module ingester-ring: ",
            "level=info msg=\"module stopped\" module=memberlist-kv",
            "level=info msg=\"module stopped\" module=server",
            "level=info msg=\"Application stopped\"",
        ],
    );
    for never_running in ["ingester-ring", "ingester-service"] {
        let stopped = format!("msg=\"module stopped\" module={never_running}");
        assert!(
            !lines.iter().any(|l| l.contains(&stopped)),
            "{never_running} never reached Running: no stop() in dskit\nstderr:\n{}",
            lines.join("\n")
        );
    }
    let last = lines.last().expect("some output");
    assert!(
        last.contains(" level=error msg=\"error running application\" err=\"failed services"),
        "last line: {last:?}\nstderr:\n{}",
        lines.join("\n")
    );
    assert!(
        !lines.iter().any(|l| l.starts_with("error running mimir")),
        "plain-text fatal error must be gone"
    );
}
