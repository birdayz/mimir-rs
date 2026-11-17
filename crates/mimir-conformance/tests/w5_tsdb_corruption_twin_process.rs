//! Twin-process Go-vs-Rust `-target=ingester` startup with corrupted tenant TSDBs
//! (audit finding TSDB-1 / TSDB-2 repro).
//!
//! Two tenants are prepared on disk before start-up:
//! - `user1`: `chunks_head/000001` holds a valid header followed by 30 non-zero garbage
//!   bytes (torn buffered write after an OOMKill) and an empty `wal/`.
//! - `user2`: a WAL whose last record cannot be decoded.
//!
//! Both the unmodified Go `mimir` and the Rust `mimir` must open both TSDBs (repairing them),
//! become ready, and export identical `cortex_ingester_tsdb_mmap_chunk_corruptions_total`,
//! `cortex_ingester_tsdb_wal_corruptions_total` values and the per-user
//! `cortex_ingester_tsdb_storage_blocks_bytes` series.
//!
//! Go reference: prometheus/tsdb/head.go (`Head.Init` m-map chunk repair), tsdb/db.go
//! (`DB.Open` WAL repair), mimir pkg/storage/tsdb/tsdb_metrics.go (aggregation).

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin,
};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::record::{RecordEncoder, RefSample, RefSeries};
use mimir_tsdb::wlog::{CompressionType, WlogWriter};

struct Proc {
    name: &'static str,
    child: Child,
}

impl Proc {
    fn spawn(name: &'static str, bin: &Path, config: &Path, workdir: &Path) -> Self {
        let child = Command::new(bin)
            .arg("-target=ingester")
            .arg(format!("-config.file={}", config.display()))
            .current_dir(workdir)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {name}: {e}"));
        Self { name, child }
    }

    fn stderr(&mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let mut s = String::new();
        if let Some(mut e) = self.child.stderr.take() {
            let _ = e.read_to_string(&mut s);
        }
        s
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    mimir_conformance::free_test_port()
}

fn write_yaml(path: &Path, http: u16, grpc: u16, tsdb_dir: &Path, bucket_dir: &Path) {
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 10m
    head_compaction_interval: 10m
activity_tracker:
  filepath: ""
"#,
        bucket_dir.display(),
        tsdb_dir.display()
    );
    fs::write(path, yaml).unwrap();
}

fn build_tenants(tsdb_dir: &Path) {
    // user1: torn chunks_head tail.
    let u1 = tsdb_dir.join("user1");
    fs::create_dir_all(u1.join("wal")).unwrap();
    fs::create_dir_all(u1.join("chunks_head")).unwrap();
    let mut data = vec![0x01, 0x30, 0xBC, 0x91, 0x01, 0, 0, 0];
    data.extend((1..=30u8).map(|b| b.wrapping_mul(37) | 1));
    fs::write(u1.join("chunks_head").join("000001"), data).unwrap();

    // user2: WAL with an undecodable last record.
    let u2 = tsdb_dir.join("user2");
    let enc = RecordEncoder::new(false);
    let mut w = WlogWriter::open(u2.join("wal"), CompressionType::None).unwrap();
    let series = enc.series(
        &[RefSeries {
            ref_id: 1,
            labels: Labels::from_pairs([("__name__", "m")]),
        }],
        Vec::new(),
    );
    let samples = enc.samples(
        &[RefSample {
            ref_id: 1,
            st: 0,
            t: 1_000,
            v: 1.0,
        }],
        Vec::new(),
    );
    w.log(&[&series]).unwrap();
    w.log(&[&samples]).unwrap();
    w.log(&[&samples[..3]]).unwrap();
    w.close().unwrap();
}

/// Sums every sample of `family` in Prometheus text exposition, keyed by label set.
fn scrape(text: &str, family: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some(rest) = line.strip_prefix(family) else {
            continue;
        };
        let (labels, value) = if let Some(r) = rest.strip_prefix('{') {
            let Some((l, v)) = r.split_once('}') else {
                continue;
            };
            (l.to_string(), v.trim())
        } else if rest.starts_with(' ') {
            (String::new(), rest.trim())
        } else {
            continue;
        };
        if let Ok(v) = value.parse::<f64>() {
            out.insert(labels, v);
        }
    }
    out
}

async fn wait_ready_and_scrape(proc: &mut Proc, http: u16) -> String {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(Some(status)) = proc.child.try_wait() {
            let err = proc.stderr();
            panic!("{} exited with {status} before /ready:\n{err}", proc.name);
        }
        if let Ok(r) = client
            .get(format!("http://127.0.0.1:{http}/ready"))
            .send()
            .await
        {
            if r.status().as_u16() == 200 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "{} not ready within 60s", proc.name);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    client
        .get(format!("http://127.0.0.1:{http}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[test]
fn corrupted_tenant_tsdbs_are_repaired_at_startup_like_go() {
    if std::env::var("MIMIR_CONFORMANCE_SKIP_TWIN_PROCESS").is_ok() {
        return;
    }
    ensure_go_oracle_built().expect("go binaries");
    let rust_bin: std::path::PathBuf = match std::env::var("MIMIR_CONFORMANCE_RUST_BIN") {
        Ok(p) => p.into(),
        Err(_) => ensure_rust_mimir_release_built().expect("rust mimir release build"),
    };

    let mut results = Vec::new();
    for (name, bin) in [("go", go_mimir_bin()), ("rust", rust_bin)] {
        let root = tempfile::tempdir().unwrap();
        let tsdb_dir = root.path().join("tsdb");
        let bucket_dir = root.path().join("bucket");
        fs::create_dir_all(&bucket_dir).unwrap();
        build_tenants(&tsdb_dir);
        let (http, grpc) = (free_port(), free_port());
        let cfg = root.path().join("mimir.yaml");
        write_yaml(&cfg, http, grpc, &tsdb_dir, &bucket_dir);
        let proc_name = if name == "go" { "go-mimir" } else { "rust-mimir" };
        let mut proc = Proc::spawn(proc_name, &bin, &cfg, root.path());
        let text = mimir_common::block_on_for_tests(wait_ready_and_scrape(&mut proc, http));
        drop(proc);
        let chunk_files: Vec<String> = fs::read_dir(tsdb_dir.join("user1/chunks_head"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        results.push((name, text, chunk_files));
    }

    let (_, go_text, go_files) = &results[0];
    let (_, rust_text, rust_files) = &results[1];
    for family in [
        "cortex_ingester_tsdb_mmap_chunk_corruptions_total",
        "cortex_ingester_tsdb_wal_corruptions_total",
        "cortex_ingester_tsdb_storage_blocks_bytes",
        "cortex_ingester_tsdb_reloads_total",
        "cortex_ingester_tsdb_blocks_loaded",
    ] {
        let go = scrape(go_text, family);
        let rust = scrape(rust_text, family);
        assert!(!go.is_empty(), "Go does not export {family}");
        assert_eq!(go, rust, "{family} differs");
    }
    assert_eq!(
        scrape(go_text, "cortex_ingester_tsdb_mmap_chunk_corruptions_total")[""],
        1.0
    );
    assert_eq!(scrape(go_text, "cortex_ingester_tsdb_wal_corruptions_total")[""], 1.0);
    assert_eq!(go_files, rust_files, "chunks_head files left after repair differ");
}
