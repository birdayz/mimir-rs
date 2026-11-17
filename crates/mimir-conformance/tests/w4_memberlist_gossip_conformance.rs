//! Tier-3 differential memberlist tests: Go `mimir` and Rust `mimir` (-target=ingester) gossiping
//! in one memberlist cluster.
//!
//! Covers (each scenario asserts the Rust process behaves like the Go processes):
//! 1. Failure detection: a 3-node mixed cluster where one node is `kill -9`ed; every surviving
//!    node (Go and Rust) must drop `memberlist_client_cluster_members_count` to 2.
//! 2. TLS gossip (`-memberlist.tls-enabled`) between Go and Rust nodes.
//! 3. `dns+host:port` join members (Helm's default `join_members`) in both directions.
//!
//! Upstream behaviour: github.com/hashicorp/memberlist (grafana fork) state.go, dskit
//! kv/memberlist/{memberlist_client.go,tcp_transport.go}, dskit dns/provider.go.

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, rust_mimir_release_bin,
    workspace_root,
};

struct Proc {
    name: String,
    child: Option<Child>,
    log: PathBuf,
    http_port: u16,
}

impl Proc {
    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill(); // SIGKILL
            let _ = c.wait();
        }
    }

    fn log_tail(&self) -> String {
        let s = fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = s.lines().collect();
        lines[lines.len().saturating_sub(30)..].join("\n")
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill9();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn http_get(port: u16, path: &str) -> Option<String> {
    let mut s = TcpStream::connect_timeout(&format!("127.0.0.1:{port}").parse().unwrap(), Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    write!(s, "GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).ok()?;
    Some(buf)
}

fn members_count(p: &Proc) -> Option<u64> {
    let body = http_get(p.http_port, "/metrics")?;
    body.lines()
        .find(|l| l.starts_with("memberlist_client_cluster_members_count "))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v as u64)
}

fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    f()
}

struct NodeSpec<'a> {
    go: bool,
    name: &'a str,
    ml_port: u16,
    join: Vec<String>,
    extra_memberlist: &'a str,
}

fn spawn(dir: &Path, spec: NodeSpec<'_>) -> Proc {
    let http_port = free_port();
    let grpc_port = free_port();
    let ndir = dir.join(spec.name);
    fs::create_dir_all(ndir.join("tsdb")).unwrap();
    fs::create_dir_all(ndir.join("bucket")).unwrap();
    let join = spec
        .join
        .iter()
        .map(|j| format!("    - {j}\n"))
        .collect::<String>();
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
multitenancy_enabled: true
memberlist:
  node_name: {name}
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {ml}
  advertise_addr: 127.0.0.1
  advertise_port: {ml}
  gossip_interval: 100ms
  abort_if_cluster_join_fails: false
  join_members:
{join}{extra}
ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: {name}
    instance_addr: 127.0.0.1
    num_tokens: 16
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
        name = spec.name,
        ml = spec.ml_port,
        extra = spec.extra_memberlist,
        bucket = ndir.join("bucket").display(),
        tsdb = ndir.join("tsdb").display(),
    );
    let cfg = ndir.join("mimir.yaml");
    fs::write(&cfg, yaml).unwrap();
    let log = ndir.join("stderr.log");
    let bin = if spec.go { go_mimir_bin() } else { rust_mimir_release_bin() };
    let child = Command::new(&bin)
        .arg(format!("-config.file={}", cfg.display()))
        .current_dir(&ndir)
        .stdout(Stdio::null())
        .stderr(fs::File::create(&log).unwrap())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
    Proc {
        name: spec.name.to_string(),
        child: Some(child),
        log,
        http_port,
    }
}

fn ensure_bins() {
    if !go_mimir_bin().exists() {
        ensure_go_oracle_built().expect("go-mimir must build");
    }
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");
}

fn assert_members(procs: &[&Proc], want: u64, timeout: Duration, what: &str) {
    for p in procs {
        let ok = wait_until(timeout, || members_count(p) == Some(want));
        assert!(
            ok,
            "{what}: {} sees {:?} members, want {want}\n--- {} log tail ---\n{}",
            p.name,
            members_count(p),
            p.name,
            p.log_tail()
        );
    }
}

#[test]
fn mixed_go_rust_memberlist_kill9_failure_detection() {
    ensure_bins();
    let tmp = tempfile::tempdir().unwrap();
    let (p_go1, p_rs, p_go2) = (free_port(), free_port(), free_port());
    let seed = vec![format!("127.0.0.1:{p_go1}")];
    let go1 = spawn(tmp.path(), NodeSpec { go: true, name: "go-1", ml_port: p_go1, join: vec![format!("127.0.0.1:{p_rs}")], extra_memberlist: "" });
    let rs = spawn(tmp.path(), NodeSpec { go: false, name: "rust-1", ml_port: p_rs, join: seed.clone(), extra_memberlist: "" });
    let mut go2 = spawn(tmp.path(), NodeSpec { go: true, name: "go-2", ml_port: p_go2, join: seed.clone(), extra_memberlist: "" });
    assert_members(&[&go1, &rs, &go2], 3, Duration::from_secs(60), "converge to 3 members");

    // A Go node crashes: both the remaining Go node and the Rust node detect it.
    go2.kill9();
    assert_members(&[&go1, &rs], 2, Duration::from_secs(90), "after kill -9 of go-2");

    // A Rust node crashes: Go and Rust detect it as well.
    let p_rs2 = free_port();
    let mut rs2 = spawn(tmp.path(), NodeSpec { go: false, name: "rust-2", ml_port: p_rs2, join: seed, extra_memberlist: "" });
    assert_members(&[&go1, &rs, &rs2], 3, Duration::from_secs(60), "rust-2 joined");
    rs2.kill9();
    assert_members(&[&go1, &rs], 2, Duration::from_secs(90), "after kill -9 of rust-2");
}

#[test]
fn mixed_go_rust_memberlist_tls_gossip() {
    ensure_bins();
    let tmp = tempfile::tempdir().unwrap();
    let fx = workspace_root().join("crates/mimir-storage/tests/fixtures/kafka_tls");
    let tls = format!(
        "  tls_enabled: true\n  tls_cert_path: {}\n  tls_key_path: {}\n  tls_ca_path: {}\n",
        fx.join("server.crt").display(),
        fx.join("server.key").display(),
        fx.join("ca.crt").display()
    );
    let (p_go, p_rs) = (free_port(), free_port());
    let go = spawn(tmp.path(), NodeSpec { go: true, name: "go-tls", ml_port: p_go, join: vec![format!("127.0.0.1:{p_rs}")], extra_memberlist: &tls });
    let rs = spawn(tmp.path(), NodeSpec { go: false, name: "rust-tls", ml_port: p_rs, join: vec![format!("127.0.0.1:{p_go}")], extra_memberlist: &tls });
    assert_members(&[&go, &rs], 2, Duration::from_secs(60), "TLS gossip");
    // Both rings see both ingesters (KV gossip over TLS works, not only membership).
    for p in [&go, &rs] {
        let ok = wait_until(Duration::from_secs(30), || {
            http_get(p.http_port, "/ingester/ring")
                .is_some_and(|b| b.contains("go-tls") && b.contains("rust-tls"))
        });
        assert!(ok, "{} ring page does not list both ingesters\n{}", p.name, p.log_tail());
    }

    // A plaintext Rust node cannot join the TLS cluster.
    let p_plain = free_port();
    let plain = spawn(tmp.path(), NodeSpec { go: false, name: "rust-plain", ml_port: p_plain, join: vec![format!("127.0.0.1:{p_go}")], extra_memberlist: "" });
    std::thread::sleep(Duration::from_secs(5));
    assert_eq!(members_count(&plain), Some(1), "plaintext node must not join a TLS cluster");
    assert_eq!(members_count(&go), Some(2));
}

#[test]
fn mixed_go_rust_memberlist_tls_invalid_cert_fails_startup_like_go() {
    ensure_bins();
    let tmp = tempfile::tempdir().unwrap();
    let tls = "  tls_enabled: true\n  tls_cert_path: /nonexistent/cert.pem\n  tls_key_path: /nonexistent/key.pem\n";
    for go in [true, false] {
        let name = if go { "go-badtls" } else { "rust-badtls" };
        let mut p = spawn(tmp.path(), NodeSpec { go, name, ml_port: free_port(), join: vec![], extra_memberlist: tls });
        let mut child = p.child.take().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(s) = child.try_wait().unwrap() {
                break Some(s);
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                break None;
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let status = status.unwrap_or_else(|| panic!("{name} kept running with an invalid memberlist TLS cert\n{}", p.log_tail()));
        assert!(!status.success(), "{name} must exit with an error");
        assert!(p.log_tail().contains("memberlist-kv"), "{name} log:\n{}", p.log_tail());
    }
}

#[test]
fn mixed_go_rust_memberlist_dns_plus_join() {
    ensure_bins();
    let tmp = tempfile::tempdir().unwrap();
    let (p_go, p_rs) = (free_port(), free_port());
    // Helm default form: dns+<headless service>:<port>.
    let go = spawn(tmp.path(), NodeSpec { go: true, name: "go-dns", ml_port: p_go, join: vec![format!("dns+localhost:{p_rs}")], extra_memberlist: "" });
    let rs = spawn(tmp.path(), NodeSpec { go: false, name: "rust-dns", ml_port: p_rs, join: vec![format!("dns+localhost:{p_go}")], extra_memberlist: "" });
    assert_members(&[&go, &rs], 2, Duration::from_secs(60), "dns+ join");
    let body = http_get(rs.http_port, "/metrics").unwrap_or_default();
    assert!(
        body.contains("cortex_dns_lookups_total{component=\"memberlist\"}"),
        "Rust must expose cortex_dns_lookups_total like Go"
    );
}
