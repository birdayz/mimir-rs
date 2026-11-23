//! End-to-end binary verification of Rust-runtime operational metrics (`mimir_rs_*`).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn http_get(port: u16, path: &str) -> Option<(u16, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).ok()?;
    let (head, body) = buf.split_once("\r\n\r\n")?;
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse::<u16>()
        .ok()?;
    Some((status, body.to_string()))
}

fn metric_value(text: &str, prefix: &str) -> Option<f64> {
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(prefix) {
            if rest.starts_with(' ') {
                return rest.trim().parse::<f64>().ok();
            }
        }
    }
    None
}

#[test]
fn binary_exposes_live_mimir_rs_runtime_and_allocator_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let http_port = free_port();
    let grpc_port = free_port();
    let cfg_path = tmp.path().join("mimir.yaml");
    std::fs::create_dir_all(tmp.path().join("tsdb")).unwrap();
    std::fs::create_dir_all(tmp.path().join("bucket")).unwrap();
    std::fs::write(
        &cfg_path,
        format!(
            r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
usage_stats:
  enabled: false
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: rust-metrics-test
    instance_addr: 127.0.0.1
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
activity_tracker:
  filepath: ""
"#,
            bucket = tmp.path().join("bucket").display(),
            tsdb = tmp.path().join("tsdb").display(),
        ),
    )
    .unwrap();

    let bin = env!("CARGO_BIN_EXE_mimir");
    let mut child = Command::new(bin)
        .arg(format!("-config.file={}", cfg_path.display()))
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mimir binary");

    let deadline = Instant::now() + Duration::from_secs(20);
    let ready = loop {
        if let Some((200, _)) = http_get(http_port, "/ready") {
            break true;
        }
        if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(ready, "mimir binary did not become ready on :{http_port}");

    let (code, body) = http_get(http_port, "/metrics").expect("GET /metrics");
    assert_eq!(code, 200);

    // Tokio metrics for BOTH main and dedicated rpc runtimes.
    assert!(
        metric_value(&body, "mimir_rs_tokio_workers{runtime=\"main\"}").unwrap_or(0.0) >= 1.0,
        "missing/zero main tokio workers"
    );
    assert!(
        metric_value(&body, "mimir_rs_tokio_workers{runtime=\"rpc\"}").unwrap_or(0.0) >= 2.0,
        "missing/zero rpc tokio workers"
    );
    assert!(metric_value(&body, "mimir_rs_tokio_alive_tasks{runtime=\"main\"}").is_some());
    assert!(metric_value(&body, "mimir_rs_tokio_alive_tasks{runtime=\"rpc\"}").is_some());
    assert!(metric_value(&body, "mimir_rs_tokio_global_queue_depth{runtime=\"main\"}").is_some());
    assert!(metric_value(&body, "mimir_rs_tokio_worker_busy_seconds_total{runtime=\"main\"}").is_some());
    assert!(metric_value(&body, "mimir_rs_tokio_worker_park_total{runtime=\"rpc\"}").is_some());

    // Off-heap NanosSlab & ScopedWorkerPool gauges/counters.
    assert!(metric_value(&body, "mimir_rs_nanos_slab_mapped_bytes").is_some());
    assert!(metric_value(&body, "mimir_rs_nanos_slab_pages").is_some());
    assert!(metric_value(&body, "mimir_rs_nanos_slab_reclaimed_pages_total").is_some());
    assert!(metric_value(&body, "mimir_rs_scoped_worker_pool_threads").is_some());
    assert!(metric_value(&body, "mimir_rs_scoped_worker_pool_executions_total").is_some());

    // Linux jemalloc mallctl stats + /proc/self/status RSS breakdown.
    #[cfg(target_os = "linux")]
    {
        let alloc = metric_value(&body, "mimir_rs_jemalloc_allocated_bytes").unwrap_or(0.0);
        let active = metric_value(&body, "mimir_rs_jemalloc_active_bytes").unwrap_or(0.0);
        let resident = metric_value(&body, "mimir_rs_jemalloc_resident_bytes").unwrap_or(0.0);
        let mapped = metric_value(&body, "mimir_rs_jemalloc_mapped_bytes").unwrap_or(0.0);
        let metadata = metric_value(&body, "mimir_rs_jemalloc_metadata_bytes").unwrap_or(0.0);
        assert!(alloc > 0.0, "expected jemalloc allocated > 0, got {alloc}");
        assert!(active >= alloc, "expected jemalloc active ({active}) >= allocated ({alloc})");
        assert!(resident > 0.0, "expected jemalloc resident > 0, got {resident}");
        assert!(mapped > 0.0, "expected jemalloc mapped > 0, got {mapped}");
        assert!(metadata > 0.0, "expected jemalloc metadata > 0, got {metadata}");
        assert!(metric_value(&body, "mimir_rs_jemalloc_retained_bytes").is_some());
        assert!(metric_value(&body, "mimir_rs_jemalloc_arena_purges_total").is_some());

        assert!(metric_value(&body, "mimir_rs_process_rss_anon_bytes").unwrap_or(0.0) > 0.0);
        assert!(metric_value(&body, "mimir_rs_process_rss_file_bytes").is_some());
        assert!(metric_value(&body, "mimir_rs_process_rss_shmem_bytes").is_some());
        assert!(metric_value(&body, "mimir_rs_process_threads").unwrap_or(0.0) >= 3.0);
    }

    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let status = child.wait().expect("wait child");
    assert!(status.success(), "mimir did not exit cleanly: {status}");
}
