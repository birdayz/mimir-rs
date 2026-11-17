//! S3 blocks-storage differential conformance against a real MinIO-compatible server
//! (bucket findings F10/F12, B1-B5, B10-B15).
//!
//! A Silo server (`pgsty/silo`, the MinIO community fork used by upstream Mimir's own
//! `development/*/docker-compose.yml`) is started with testcontainers-rs, with a static KMS key
//! (`MINIO_KMS_SECRET_KEY`) so SSE-S3 and SSE-KMS work. The unmodified Go `mimir`
//! (`target/go-mimir`) and the Rust `mimir` are booted as `-target=ingester` with identical
//! `blocks_storage.s3` configuration (each with its own bucket), receive the same push and ship
//! their blocks on `POST /ingester/flush?wait=true`. Go runs first (Go-vs-Go validation of the
//! scenario) and defines the expected outcome; the Rust outcome must be identical:
//! - the shipped objects (block ULIDs normalised) and, per object, the server-side encryption,
//!   KMS key and storage class recorded by the server (`mc stat`);
//! - for plain-HTTP scenarios, every S3 request as seen by a recording proxy in front of the
//!   server: method, normalised path, query keys, `Content-Type`, `Content-Md5` presence,
//!   `X-Amz-Content-Sha256` kind, SSE / storage-class headers, the SigV4 credential scope
//!   (region) and the exact `SignedHeaders` list;
//! - the `thanos_objstore_bucket_*{component="ingester"}` series and the upload count.
//!
//! Upstream behaviour under test:
//! - github.com/grafana/mimir/pkg/storage/bucket/{client.go,s3/*,sse_bucket_client.go,
//!   user_bucket_client.go}
//! - github.com/thanos-io/objstore/providers/s3/s3.go, exthttp/*, objstore.go (metrics)
//! - github.com/minio/minio-go/v7 (region discovery, signing, PutObject)
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).
//! `MIMIR_CONFORMANCE_RUST_BIN=/path/to/mimir` tests a prebuilt Rust binary instead of
//! rebuilding `target/release/mimir`. Leftover containers carry the label
//! `org.mimir-rs.conformance=w8-bucket-s3`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{go_mimir_bin, rust_mimir_release_bin, workspace_root};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;

const SILO_IMAGE: &str = "pgsty/silo";
const SILO_TAG: &str = "RELEASE.2026-09-03T13-18-01Z";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w8-bucket-s3");
const ACCESS_KEY: &str = "mimir";
const SECRET_KEY: &str = "supersecret";
const KMS_KEY: &str = "mimir-kms";
/// 32 bytes, base64 (test-only static KMS master key).
const KMS_SECRET: &str = "bWltaXItcnMtdzgtYnVja2V0LXRlc3Qta21zLWtleSE=";
const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
const BUCKETS: &[&str] = &["mimir-go", "mimir-rs"];

fn skip_docker(test: &str) -> bool {
    if std::env::var("MIMIR_CONFORMANCE_SKIP_DOCKER").as_deref() == Ok("1") {
        eprintln!("SKIPPED {test}: MIMIR_CONFORMANCE_SKIP_DOCKER=1 (this test needs Docker)");
        return true;
    }
    false
}

fn rust_bin() -> PathBuf {
    match std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN").filter(|v| !v.is_empty()) {
        Some(p) => PathBuf::from(p),
        None => rust_mimir_release_bin(),
    }
}

/// Rebuilds the Rust release binary (incremental) unless `MIMIR_CONFORMANCE_RUST_BIN` is set, and
/// makes sure `target/go-mimir` exists (built by `scripts/build-go-oracle.sh`).
fn ensure_fresh_binaries() {
    static BUILT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    let res = BUILT.get_or_init(|| {
        let root = workspace_root();
        if !go_mimir_bin().exists() {
            let status = Command::new(root.join("scripts").join("build-go-oracle.sh"))
                .current_dir(&root)
                .status()
                .map_err(|e| format!("spawn scripts/build-go-oracle.sh: {e}"))?;
            if !status.success() {
                return Err(format!("scripts/build-go-oracle.sh exited with {status}"));
            }
        }
        if std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN").is_some_and(|v| !v.is_empty()) {
            return Ok(());
        }
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(["build", "--release", "-p", "mimir-cmd"])
            .current_dir(&root)
            .status()
            .map_err(|e| format!("spawn cargo build --release -p mimir-cmd: {e}"))?;
        if !status.success() {
            return Err(format!("cargo build --release -p mimir-cmd exited with {status}"));
        }
        Ok(())
    });
    if let Err(e) = res {
        panic!("building conformance binaries failed: {e}");
    }
}

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .unwrap()
        .port()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn fixture(name: &str) -> PathBuf {
    workspace_root()
        .join("crates/mimir-storage/tests/fixtures/kafka_tls")
        .join(name)
}

// ---------------------------------------------------------------------------------------------
// Child process guard
// ---------------------------------------------------------------------------------------------

struct ProcessGuard {
    child: Option<Child>,
    log: Arc<Mutex<Vec<u8>>>,
    readers: Vec<JoinHandle<()>>,
}

impl ProcessGuard {
    fn spawn(bin: &Path, config: &Path, workdir: &Path) -> Result<Self> {
        let mut child = Command::new(bin)
            .args([
                "-target=ingester".to_string(),
                format!("-config.file={}", config.display()),
            ])
            .current_dir(workdir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn {}", bin.display()))?;
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut readers = Vec::new();
        let out: Box<dyn Read + Send> = Box::new(child.stdout.take().unwrap());
        let err: Box<dyn Read + Send> = Box::new(child.stderr.take().unwrap());
        for mut stream in [out, err] {
            let log = Arc::clone(&log);
            readers.push(std::thread::spawn(move || {
                let mut buf = [0u8; 8192];
                while let Ok(n) = stream.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    log.lock().unwrap().extend_from_slice(&buf[..n]);
                }
            }));
        }
        Ok(Self {
            child: Some(child),
            log,
            readers,
        })
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child.as_mut().and_then(|c| c.try_wait().ok().flatten())
    }

    fn log_tail(&self, lines: usize) -> String {
        let text = String::from_utf8_lossy(&self.log.lock().unwrap()).to_string();
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..]
            .iter()
            .map(|l| format!("        | {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn sigterm(&self) {
        if let Some(c) = self.child.as_ref() {
            let _ = Command::new("kill")
                .args(["-TERM", &c.id().to_string()])
                .status();
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            if c.try_wait().ok().flatten().is_none() {
                let _ = c.kill();
            }
            let _ = c.wait();
        }
        for r in self.readers.drain(..) {
            let _ = r.join();
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Silo (MinIO) server
// ---------------------------------------------------------------------------------------------

struct Silo {
    container: ContainerAsync<GenericImage>,
    port: u16,
    tls: bool,
}

impl Silo {
    /// `tls`: (certificate, key) served from `--certs-dir`.
    async fn start(tls: Option<(&Path, &Path)>) -> Result<Self> {
        let port = free_tcp_port();
        let mkdirs: String = BUCKETS.iter().map(|b| format!("/data/{b} ")).collect();
        let certs = if tls.is_some() { "--certs-dir /certs " } else { "" };
        let mut req = GenericImage::new(SILO_IMAGE, SILO_TAG)
            .with_wait_for(WaitFor::message_on_either_std("API:"))
            .with_entrypoint("sh")
            .with_cmd([
                "-c".to_string(),
                format!("mkdir -p {mkdirs}&& exec silo server {certs}/data"),
            ])
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_env_var("MINIO_KMS_SECRET_KEY", format!("{KMS_KEY}:{KMS_SECRET}"))
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_mapped_port(port, ContainerPort::Tcp(9000))
            .with_startup_timeout(Duration::from_secs(120));
        if let Some((cert, key)) = tls {
            for (name, path) in [("public.crt", cert), ("private.key", key)] {
                req = req.with_copy_to(
                    CopyTargetOptions::new(format!("/certs/{name}")).with_mode(0o644),
                    path.to_path_buf(),
                );
            }
        }
        let container = req
            .start()
            .await
            .context("starting pgsty/silo via testcontainers (is Docker running?)")?;
        let silo = Self {
            container,
            port,
            tls: tls.is_some(),
        };
        let scheme = if silo.tls { "https" } else { "http" };
        let insecure = if silo.tls { " --insecure" } else { "" };
        silo.sh(&format!(
            "mc alias set{insecure} l {scheme}://127.0.0.1:9000 {ACCESS_KEY} {SECRET_KEY}"
        ))
        .await?;
        Ok(silo)
    }

    async fn sh(&self, script: &str) -> Result<String> {
        let mut res = self
            .container
            .exec(ExecCommand::new(["sh", "-c", script]))
            .await?;
        let stdout = res.stdout_to_vec().await?;
        let stderr = res.stderr_to_vec().await?;
        let code = loop {
            if let Some(c) = res.exit_code().await? {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        ensure!(
            code == 0,
            "`{script}` exited {code}: {}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    }

    fn insecure_flag(&self) -> &'static str {
        if self.tls {
            " --insecure"
        } else {
            ""
        }
    }

    /// Recursive object listing of `bucket/prefix`.
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let out = self
            .sh(&format!(
                "mc ls{} --recursive --json l/{bucket}/{prefix} || true",
                self.insecure_flag()
            ))
            .await?;
        let mut keys = Vec::new();
        for line in out.lines().filter(|l| !l.trim().is_empty()) {
            let v: serde_json::Value = serde_json::from_str(line)?;
            if let Some(k) = v["key"].as_str() {
                keys.push(format!("{prefix}{k}"));
            }
        }
        keys.sort();
        Ok(keys)
    }

    /// `mc stat` metadata relevant for the comparison.
    async fn stat(&self, bucket: &str, key: &str) -> Result<BTreeMap<String, String>> {
        let out = self
            .sh(&format!("mc stat{} --json l/{bucket}/{key}", self.insecure_flag()))
            .await?;
        let v: serde_json::Value = serde_json::from_str(out.trim())?;
        let mut m = BTreeMap::new();
        if let Some(meta) = v["metadata"].as_object() {
            for (k, val) in meta {
                let lk = k.to_ascii_lowercase();
                if lk.starts_with("x-amz-server-side-encryption") || lk == "x-amz-storage-class" {
                    m.insert(lk, val.as_str().unwrap_or_default().to_string());
                }
            }
        }
        Ok(m)
    }
}

// ---------------------------------------------------------------------------------------------
// Recording proxy (plain HTTP): forwards bytes and records every request head
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
}

struct Proxy {
    port: u16,
    seen: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl Proxy {
    async fn start(upstream_port: u16) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let seen: Arc<Mutex<Vec<RecordedRequest>>> = Arc::default();
        let seen2 = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let seen = Arc::clone(&seen2);
                tokio::spawn(async move {
                    let Ok(server) =
                        tokio::net::TcpStream::connect(("127.0.0.1", upstream_port)).await
                    else {
                        return;
                    };
                    let (mut cr, mut cw) = client.into_split();
                    let (mut sr, mut sw) = server.into_split();
                    tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut sr, &mut cw).await;
                    });
                    // client -> server, parsing request framing.
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = vec![0u8; 65536];
                    let mut state = ParseState::Head;
                    loop {
                        let n = match cr.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        if sw.write_all(&chunk[..n]).await.is_err() {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        while let Some(rest) = advance(&mut state, &mut buf, &seen) {
                            buf = rest;
                        }
                    }
                });
            }
        });
        Ok(Self { port, seen })
    }

    fn take(&self) -> Vec<RecordedRequest> {
        std::mem::take(&mut self.seen.lock().unwrap())
    }
}

enum ParseState {
    Head,
    Body(usize),
    Chunked,
}

/// Consumes one framing step from `buf`; returns the remaining bytes when progress was made.
fn advance(
    state: &mut ParseState,
    buf: &mut Vec<u8>,
    seen: &Mutex<Vec<RecordedRequest>>,
) -> Option<Vec<u8>> {
    match state {
        ParseState::Head => {
            let end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
            let mut lines = head.lines();
            let first = lines.next().unwrap_or("");
            let mut parts = first.split_whitespace();
            let method = parts.next().unwrap_or("").to_string();
            let target = parts.next().unwrap_or("").to_string();
            let headers: BTreeMap<String, String> = lines
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                .collect();
            let len: usize = headers
                .get("content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let chunked = headers
                .get("transfer-encoding")
                .is_some_and(|v| v.eq_ignore_ascii_case("chunked"));
            seen.lock().unwrap().push(RecordedRequest {
                method,
                target,
                headers,
            });
            *state = if chunked {
                ParseState::Chunked
            } else {
                ParseState::Body(len)
            };
            Some(buf[end + 4..].to_vec())
        }
        ParseState::Body(len) => {
            if buf.len() < *len {
                *len -= buf.len();
                buf.clear();
                return None;
            }
            let rest = buf[*len..].to_vec();
            *state = ParseState::Head;
            Some(rest)
        }
        ParseState::Chunked => {
            let line_end = buf.windows(2).position(|w| w == b"\r\n")?;
            let size = usize::from_str_radix(
                String::from_utf8_lossy(&buf[..line_end])
                    .split(';')
                    .next()
                    .unwrap_or("0")
                    .trim(),
                16,
            )
            .unwrap_or(0);
            let total = line_end + 2 + size + 2;
            if buf.len() < total {
                return None;
            }
            if size == 0 {
                *state = ParseState::Head;
            }
            Some(buf[total..].to_vec())
        }
    }
}

/// Normalised view of one S3 request for the Go/Rust comparison.
fn normalise_request(r: &RecordedRequest, bucket: &str) -> Option<String> {
    let (path, query) = r.target.split_once('?').unwrap_or((&r.target, ""));
    // Out of scope (owned by other work streams / not part of the ingester bucket client):
    // startup sanity check object and usage-stats seed.
    if path.contains("sanity-check-at-startup") || path.contains("__mimir_cluster") {
        return None;
    }
    let path = path.replacen(&format!("/{bucket}"), "/<bucket>", 1);
    let path: String = path
        .split('/')
        .map(|seg| {
            if ulid::Ulid::from_string(seg).is_ok() {
                "<ULID>"
            } else {
                seg
            }
        })
        .collect::<Vec<_>>()
        .join("/");
    let mut qkeys: Vec<&str> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| kv.split('=').next().unwrap_or(""))
        .collect();
    qkeys.sort();
    let h = |k: &str| r.headers.get(k).map(String::as_str).unwrap_or("");
    let sha = match h("x-amz-content-sha256") {
        "" => "-".to_string(),
        v if v.len() == 64 && v.chars().all(|c| c.is_ascii_hexdigit()) => {
            if v == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
                "empty-sha256".into()
            } else {
                "sha256".into()
            }
        }
        v => v.to_string(),
    };
    let auth = h("authorization");
    let scope = auth
        .split("Credential=")
        .nth(1)
        .and_then(|c| c.split([',', ' ']).next())
        .map(|c| {
            let parts: Vec<&str> = c.split('/').collect();
            // AK/date/region/service/aws4_request -> region/service
            if parts.len() == 5 {
                format!("{}/{}", parts[2], parts[3])
            } else {
                c.to_string()
            }
        })
        .unwrap_or_default();
    let signed = auth
        .split("SignedHeaders=")
        .nth(1)
        .and_then(|s| s.split([',', ' ']).next())
        .unwrap_or("");
    let mut sse: Vec<String> = r
        .headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-amz-server-side-encryption") || *k == "x-amz-storage-class")
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    sse.sort();
    Some(format!(
        "{} {} ?{} ct={} md5={} sha={} scope={} signed={} {}",
        r.method,
        path,
        qkeys.join("&"),
        h("content-type"),
        !h("content-md5").is_empty(),
        sha,
        scope,
        signed,
        sse.join(" ")
    ))
}

// ---------------------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Impl {
    Go,
    Rust,
}

impl Impl {
    fn label(self) -> &'static str {
        match self {
            Impl::Go => "go",
            Impl::Rust => "rs",
        }
    }
    fn bin(self) -> PathBuf {
        match self {
            Impl::Go => go_mimir_bin(),
            Impl::Rust => rust_bin(),
        }
    }
    fn bucket(self) -> &'static str {
        match self {
            Impl::Go => BUCKETS[0],
            Impl::Rust => BUCKETS[1],
        }
    }
}

struct Scenario {
    name: &'static str,
    /// `blocks_storage.s3` YAML body (indented by 4), without `bucket_name`.
    s3_yaml: String,
    /// Runtime-config overrides YAML (indented by 2 under `overrides:`), if any.
    overrides: String,
    /// Whether shipping is expected to succeed.
    expect_upload: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    /// `<tenant>/<ULID>/<file>` -> SSE/storage-class metadata.
    objects: BTreeMap<String, BTreeMap<String, String>>,
    /// Normalised S3 requests (sorted multiset), plain-HTTP scenarios only.
    requests: Vec<String>,
    /// `thanos_objstore_bucket_*` series (name + labels, bucket label normalised).
    series: BTreeSet<String>,
    /// `thanos_objstore_bucket_operations_total{operation="upload"}`.
    uploads: u64,
    /// Operations with failures (`operation_failures_total > 0`).
    failed_ops: BTreeSet<String>,
}

fn write_requests(base_ts: i64) -> Vec<(&'static str, cortexpb::WriteRequest)> {
    let series = |job: &'static str| cortexpb::TimeSeries {
        labels: vec![
            cortexpb::LabelPair::from_static("__name__", "w8_s3_metric"),
            cortexpb::LabelPair::from_static("job", job),
        ],
        samples: (0..5)
            .map(|i| cortexpb::Sample {
                value: i as f64,
                timestamp_ms: base_ts + i * 1000,
            })
            .collect(),
        ..Default::default()
    };
    [TENANT_A, TENANT_B]
        .into_iter()
        .map(|t| {
            (
                t,
                cortexpb::WriteRequest {
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    timeseries: vec![series("a"), series("b")],
                    ..Default::default()
                },
            )
        })
        .collect()
}

fn parse_metrics(text: &str, bucket: &str) -> (BTreeSet<String>, u64, BTreeSet<String>) {
    let mut series = BTreeSet::new();
    let mut uploads = 0;
    let mut failed = BTreeSet::new();
    for line in text.lines() {
        if !line.starts_with("thanos_objstore_bucket_") {
            continue;
        }
        let (name_labels, value) = line.rsplit_once(' ').unwrap_or((line, "0"));
        let value: f64 = value.parse().unwrap_or(0.0);
        let mut normalised =
            name_labels.replace(&format!("bucket=\"{bucket}\""), "bucket=\"<bucket>\"");
        // Go's text exposition renders `le` like strconv 'g' (`1.048576e+06`), the Rust
        // `prometheus` crate as `1048576` (a crate-wide formatting difference, identical for
        // PromQL); compare the numeric bucket bound.
        if let Some(i) = normalised.find("le=\"") {
            let rest = &normalised[i + 4..];
            if let Some(j) = rest.find('"') {
                if let Ok(v) = rest[..j].parse::<f64>() {
                    normalised = format!("{}le=\"{v}\"{}", &normalised[..i], &rest[j..]);
                }
            }
        }
        // Histogram buckets/sum/count are compared by family+labels only.
        series.insert(normalised.clone());
        if name_labels.starts_with("thanos_objstore_bucket_operations_total")
            && name_labels.contains("operation=\"upload\"")
        {
            uploads = value as u64;
        }
        if name_labels.starts_with("thanos_objstore_bucket_operation_failures_total") && value > 0.0
        {
            if let Some(op) = name_labels.split("operation=\"").nth(1) {
                failed.insert(op.split('"').next().unwrap_or("").to_string());
            }
        }
    }
    (series, uploads, failed)
}

async fn run_ingester(
    imp: Impl,
    sc: &Scenario,
    silo: &Silo,
    endpoint: &str,
    proxy: Option<&Proxy>,
    base_ts: i64,
) -> Result<Outcome> {
    let dir = tempfile::tempdir()?;
    let tsdb = dir.path().join("tsdb");
    fs::create_dir_all(&tsdb)?;
    let (http_port, grpc_port) = (free_tcp_port(), free_tcp_port());
    let runtime = dir.path().join("runtime.yaml");
    fs::write(&runtime, format!("overrides:\n{}", sc.overrides))?;
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
multitenancy_enabled: true
usage_stats:
  enabled: false
runtime_config:
  file: {runtime}
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
    unregister_on_shutdown: true
blocks_storage:
  backend: s3
  s3:
    endpoint: {endpoint}
    bucket_name: {bucket}
    access_key_id: {ACCESS_KEY}
    secret_access_key: {SECRET_KEY}
{s3}  tsdb:
    dir: {tsdb}
    ship_interval: 10m
activity_tracker:
  filepath: ""
"#,
        runtime = runtime.display(),
        bucket = imp.bucket(),
        s3 = sc.s3_yaml,
        tsdb = tsdb.display(),
    );
    let yaml_path = dir.path().join("mimir.yaml");
    fs::write(&yaml_path, yaml)?;
    if let Some(p) = proxy {
        p.take();
    }
    let mut proc = ProcessGuard::spawn(&imp.bin(), &yaml_path, dir.path())?;
    let ctx = |what: &str, proc: &ProcessGuard| {
        format!(
            "[{} {}] {what}\n      process log tail:\n{}",
            sc.name,
            imp.label(),
            proc.log_tail(40)
        )
    };

    let http = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if let Some(st) = proc.try_wait() {
            bail!(ctx(&format!("exited with {st} before /ready"), &proc));
        }
        if let Ok(r) = http
            .get(format!("http://127.0.0.1:{http_port}/ready"))
            .send()
            .await
        {
            if r.status().is_success() {
                break;
            }
        }
        if Instant::now() > deadline {
            bail!(ctx("not /ready within 90s", &proc));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))?
        .connect()
        .await?;
    let mut client = IngesterClient::new(ch);
    for (tenant, wr) in write_requests(base_ts) {
        let mut req = tonic::Request::new(wr);
        req.metadata_mut()
            .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(tenant)?);
        if let Err(e) = client.push(req).await {
            bail!(ctx(&format!("push failed: {e}"), &proc));
        }
    }
    let r = http
        .post(format!("http://127.0.0.1:{http_port}/ingester/flush?wait=true"))
        .send()
        .await?;
    if !r.status().is_success() {
        bail!(ctx(&format!("flush returned {}", r.status()), &proc));
    }

    // Collect what reached the bucket.
    let deadline = Instant::now() + Duration::from_secs(if sc.expect_upload { 60 } else { 5 });
    let names = loop {
        let mut names = silo.list(imp.bucket(), &format!("{TENANT_A}/")).await?;
        names.extend(silo.list(imp.bucket(), &format!("{TENANT_B}/")).await?);
        let metas = names.iter().filter(|n| n.ends_with("/meta.json")).count();
        if metas == 2 || (!sc.expect_upload && Instant::now() > deadline) {
            break names;
        }
        if Instant::now() > deadline {
            bail!(ctx(&format!("blocks not shipped within 60s (objects: {names:?})"), &proc));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let mut objects = BTreeMap::new();
    for n in &names {
        let norm: String = n
            .split('/')
            .map(|seg| {
                if ulid::Ulid::from_string(seg).is_ok() {
                    "<ULID>"
                } else {
                    seg
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        objects.insert(norm, silo.stat(imp.bucket(), n).await?);
    }

    let metrics = http
        .get(format!("http://127.0.0.1:{http_port}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let (series, uploads, failed_ops) = parse_metrics(&metrics, imp.bucket());

    proc.sigterm();
    let deadline = Instant::now() + Duration::from_secs(30);
    while proc.try_wait().is_none() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The shutdown flush may issue more requests; only the ones until here are compared.
    let mut requests: Vec<String> = proxy
        .map(|p| {
            p.take()
                .iter()
                .filter_map(|r| normalise_request(r, imp.bucket()))
                .collect()
        })
        .unwrap_or_default();
    requests.sort();
    // Go's startup sanity-check module (owned by another work stream) builds its own bucket
    // clients, each discovering the bucket location once: compare location lookups as a set.
    let mut seen_location = false;
    requests.retain(|r| {
        if r.starts_with("GET /<bucket>/ ?location ") {
            let first = !seen_location;
            seen_location = true;
            return first;
        }
        true
    });
    Ok(Outcome {
        objects,
        requests,
        series,
        uploads,
        failed_ops,
    })
}

async fn run_scenario(
    sc: Scenario,
    silo: &Silo,
    endpoint: &str,
    proxy: Option<&Proxy>,
) -> Result<String> {
    let base_ts = now_ms() - 60_000;
    // Go-vs-Go validation first: the scenario must behave as expected for Go.
    let go = run_ingester(Impl::Go, &sc, silo, endpoint, proxy, base_ts)
        .await
        .with_context(|| format!("Go reference failed scenario {}", sc.name))?;
    if sc.expect_upload {
        ensure!(
            go.objects.keys().filter(|k| k.ends_with("/meta.json")).count() == 2
                && go.uploads > 0,
            "Go reference did not ship both blocks in scenario {}: {:?}",
            sc.name,
            go
        );
    } else {
        ensure!(
            go.objects.is_empty() && !go.failed_ops.is_empty(),
            "Go reference unexpectedly shipped / did not fail in scenario {}: {:?}",
            sc.name,
            go
        );
    }
    let rs = run_ingester(Impl::Rust, &sc, silo, endpoint, proxy, base_ts).await?;
    let mut diffs = Vec::new();
    if go.objects != rs.objects {
        diffs.push(format!("objects:\n  go:   {:?}\n  rust: {:?}", go.objects, rs.objects));
    }
    if go.requests != rs.requests {
        let count = |v: &[String]| {
            let mut m: BTreeMap<String, usize> = BTreeMap::new();
            for r in v {
                *m.entry(r.clone()).or_default() += 1;
            }
            m
        };
        let (g, r) = (count(&go.requests), count(&rs.requests));
        let keys: BTreeSet<&String> = g.keys().chain(r.keys()).collect();
        let delta: Vec<String> = keys
            .into_iter()
            .filter(|k| g.get(*k) != r.get(*k))
            .map(|k| format!("go x{} / rust x{}: {k}", g.get(k).unwrap_or(&0), r.get(k).unwrap_or(&0)))
            .collect();
        diffs.push(format!("requests (count differences):\n  {}", delta.join("\n  ")));
    }
    if go.series != rs.series {
        diffs.push(format!(
            "thanos_objstore series:\n  only go:   {:?}\n  only rust: {:?}",
            go.series.difference(&rs.series).collect::<Vec<_>>(),
            rs.series.difference(&go.series).collect::<Vec<_>>()
        ));
    }
    if go.uploads != rs.uploads {
        diffs.push(format!("upload operations: go {} rust {}", go.uploads, rs.uploads));
    }
    if go.failed_ops != rs.failed_ops {
        diffs.push(format!(
            "failed operations: go {:?} rust {:?}",
            go.failed_ops, rs.failed_ops
        ));
    }
    ensure!(diffs.is_empty(), "scenario {}:\n{}", sc.name, diffs.join("\n"));
    Ok(format!(
        "{}: {} objects, {} requests, {} uploads, failed ops {:?}",
        sc.name,
        go.objects.len(),
        go.requests.len(),
        go.uploads,
        go.failed_ops
    ))
}

#[test]
fn w8_s3_minio_sse_storage_class_md5_tls_metrics_go_rust_parity() {
    const NAME: &str = "w8_s3_minio_sse_storage_class_md5_tls_metrics_go_rust_parity";
    if skip_docker(NAME) {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let mut failures = Vec::new();
        let mut record = |name: &str, r: Result<String>| match r {
            Ok(summary) => eprintln!("PASS {summary}"),
            Err(e) => {
                eprintln!("FAIL {name}: {e:#}");
                failures.push(format!("{name}: {e:#}"));
            }
        };

        // Plain HTTP behind the recording proxy.
        {
            let silo = Silo::start(None).await.expect("start silo (http)");
            let proxy = Proxy::start(silo.port).await.expect("start proxy");
            let endpoint = format!("127.0.0.1:{}", proxy.port);
            for sc in [
                Scenario {
                    // Region unset -> GetBucketLocation; bucket SSE-KMS with context, storage
                    // class, Content-MD5; tenant-b overrides SSE to SSE-S3 (runtime config).
                    name: "http_kms_context_storage_class_md5_tenant_override",
                    s3_yaml: format!(
                        "    insecure: true\n    storage_class: REDUCED_REDUNDANCY\n    send_content_md5: true\n    sse:\n      type: SSE-KMS\n      kms_key_id: {KMS_KEY}\n      kms_encryption_context: '{{\"team\":\"obs\"}}'\n"
                    ),
                    overrides: format!("  {TENANT_B}:\n    s3_sse_type: SSE-S3\n"),
                    expect_upload: true,
                },
                Scenario {
                    // Explicit region, SSE-S3, default (no MD5), per-tenant SSE-KMS override.
                    name: "http_region_sse_s3_tenant_kms_override",
                    s3_yaml: "    insecure: true\n    region: us-east-1\n    sse:\n      type: SSE-S3\n".to_string(),
                    overrides: format!(
                        "  {TENANT_B}:\n    s3_sse_type: SSE-KMS\n    s3_sse_kms_key_id: {KMS_KEY}\n    s3_sse_kms_encryption_context: '{{\"tenant\":\"b\"}}'\n"
                    ),
                    expect_upload: true,
                },
                Scenario {
                    // signature_version v2 is accepted; thanos never applies it (V4 is sent).
                    name: "http_signature_version_v2_list_objects_v1",
                    s3_yaml: "    insecure: true\n    signature_version: v2\n    list_objects_version: v1\n".to_string(),
                    overrides: String::new(),
                    expect_upload: true,
                },
            ] {
                let name = sc.name;
                // Fresh buckets per scenario.
                for b in BUCKETS {
                    let _ = silo.sh(&format!("mc rm --recursive --force l/{b}/ >/dev/null 2>&1 || true")).await;
                }
                record(name, run_scenario(sc, &silo, &endpoint, Some(&proxy)).await);
            }
        }

        // TLS: a server certificate valid only for `kafka.mimir-rs.test`, signed by the test CA.
        {
            let silo = Silo::start(Some((&fixture("sni.crt"), &fixture("sni.key"))))
                .await
                .expect("start silo (https)");
            let endpoint = format!("127.0.0.1:{}", silo.port);
            for sc in [
                Scenario {
                    name: "https_private_ca_and_server_name",
                    s3_yaml: format!(
                        "    region: us-east-1\n    http:\n      tls_ca_path: {}\n      tls_server_name: kafka.mimir-rs.test\n",
                        fixture("ca.crt").display()
                    ),
                    overrides: String::new(),
                    expect_upload: true,
                },
                Scenario {
                    name: "https_insecure_skip_verify",
                    s3_yaml: "    region: us-east-1\n    http:\n      insecure_skip_verify: true\n".to_string(),
                    overrides: String::new(),
                    expect_upload: true,
                },
            ] {
                let name = sc.name;
                for b in BUCKETS {
                    let _ = silo.sh(&format!("mc rm --insecure --recursive --force l/{b}/ >/dev/null 2>&1 || true")).await;
                }
                record(name, run_scenario(sc, &silo, &endpoint, None).await);
            }
        }

        assert!(failures.is_empty(), "failures:\n{}", failures.join("\n\n"));
    });
}
