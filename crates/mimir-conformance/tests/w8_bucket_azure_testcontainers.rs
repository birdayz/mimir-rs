//! Azure Blob Storage blocks-storage differential conformance (bucket findings B5/B6/B7/B8).
//!
//! A real Azurite emulator is started with testcontainers-rs and both the unmodified Go `mimir`
//! (`target/go-mimir`) and the Rust `mimir` (`target/release/mimir`) are booted as
//! `-target=ingester` with byte-identical `blocks_storage.azure` configuration (each with its own
//! container name). Each binary must create its container at startup (thanos
//! `StorageCreateContainer`), accept a push, and ship the block on `POST /ingester/flush`. Go is
//! run first and defines the expected outcome; the Rust outcome must be identical.
//!
//! Upstream behavior under test:
//! - github.com/grafana/mimir/pkg/storage/bucket/azure/bucket_client.go (`newBucketClient`)
//! - github.com/thanos-io/objstore/providers/azure/azure.go (`NewBucketWithConfig`: container
//!   `GetProperties` + `Create`), helpers.go (`getContainerClient`: connection string vs
//!   `https://<account>.<endpoint_suffix>/<container>` with shared key)
//! - github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/internal/shared/shared.go
//!   (`ParseConnectionString`: `http` `BlobEndpoint` for Azurite)
//! - github.com/thanos-io/objstore/exthttp/tlsconfig.go (`tls_ca_path`)
//!
//! Scenarios:
//! - `connection_string_http`: the documented Azurite dev connection string (`http://`
//!   `BlobEndpoint`, `AccountName` + `AccountKey`).
//! - `endpoint_suffix_https`: `account_name` + `account_key` + `endpoint_suffix:
//!   blob.localhost:<port>` against an HTTPS Azurite (production-style host
//!   `devstoreaccount1.blob.localhost`), trusting a private CA via `http.tls_ca_path`.
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).
//! `MIMIR_CONFORMANCE_RUST_BIN=/path/to/mimir` tests a prebuilt Rust binary instead of
//! rebuilding `target/release/mimir`. Leftover containers carry the label
//! `org.mimir-rs.conformance=w8-bucket-azure`.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_config::{AzureConfig, BucketConfig, StorageBackendConfig};
use mimir_conformance::oracle::{go_mimir_bin, rust_mimir_release_bin, workspace_root};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_storage::bucket::BucketClient;

const AZURITE_IMAGE: &str = "mcr.microsoft.com/azure-storage/azurite";
const AZURITE_TAG: &str = "latest";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w8-bucket-azure");
const DEV_ACCOUNT: &str = "devstoreaccount1";
const DEV_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const TENANT: &str = "tenant-w8-azure";

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

/// Rebuilds the Rust release binary (incremental) unless `MIMIR_CONFORMANCE_RUST_BIN` is set, so
/// this test never silently exercises a stale `target/release/mimir`.
fn ensure_fresh_rust_binary() {
    static BUILT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    let res = BUILT.get_or_init(|| {
        if std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN").is_some_and(|v| !v.is_empty()) {
            return Ok(());
        }
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(["build", "--release", "-p", "mimir-cmd"])
            .current_dir(workspace_root())
            .status()
            .map_err(|e| format!("spawn cargo build --release -p mimir-cmd: {e}"))?;
        if !status.success() {
            return Err(format!("cargo build --release -p mimir-cmd exited with {status}"));
        }
        Ok(())
    });
    if let Err(e) = res {
        panic!("building target/release/mimir failed: {e}");
    }
    assert!(
        go_mimir_bin().exists(),
        "{} is missing (build it with scripts/build-go-oracle.sh)",
        go_mimir_bin().display()
    );
}

fn free_tcp_port() -> u16 {
    mimir_conformance::free_test_port()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// ---------------------------------------------------------------------------------------------
// Child process guard (stdout+stderr captured, killed and reaped on drop)
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

    fn log_text(&self) -> String {
        String::from_utf8_lossy(&self.log.lock().unwrap()).to_string()
    }

    fn log_tail(&self, lines: usize) -> String {
        let text = self.log_text();
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
// Azurite
// ---------------------------------------------------------------------------------------------

struct Pki {
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
}

/// Private CA + server certificate for `devstoreaccount1.blob.localhost` (openssl CLI).
fn gen_pki(dir: &Path) -> Result<Pki> {
    let run = |args: &[&str]| -> Result<()> {
        let out = Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .output()
            .context("spawn openssl")?;
        ensure!(
            out.status.success(),
            "openssl {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(())
    };
    run(&[
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem",
        "-days", "2", "-subj", "/CN=w8-bucket-azure-ca", "-addext",
        "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    ])?;
    run(&[
        "req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key", "-out", "server.csr",
        "-subj", "/CN=devstoreaccount1.blob.localhost",
    ])?;
    fs::write(
        dir.join("ext.cnf"),
        "subjectAltName=DNS:devstoreaccount1.blob.localhost,DNS:localhost,IP:127.0.0.1,IP:::1\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n",
    )?;
    run(&[
        "x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
        "-CAcreateserial", "-out", "server.pem", "-days", "2", "-extfile", "ext.cnf",
    ])?;
    Ok(Pki {
        ca: dir.join("ca.pem"),
        cert: dir.join("server.pem"),
        key: dir.join("server.key"),
    })
}

struct Azurite {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

impl Azurite {
    async fn start(tls: Option<&Pki>) -> Result<Self> {
        let port = free_tcp_port();
        let mut cmd: Vec<String> = [
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--blobPort",
            "10000",
            "--skipApiVersionCheck",
            "--loose",
            "--inMemoryPersistence",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if tls.is_some() {
            cmd.extend(
                ["--cert", "/certs/server.pem", "--key", "/certs/server.key"]
                    .iter()
                    .map(|s| s.to_string()),
            );
        }
        let mut req = GenericImage::new(AZURITE_IMAGE, AZURITE_TAG)
            .with_wait_for(WaitFor::message_on_stdout(
                "Azurite Blob service successfully listens on",
            ))
            .with_cmd(cmd)
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_mapped_port(port, ContainerPort::Tcp(10000))
            .with_startup_timeout(Duration::from_secs(120));
        if let Some(pki) = tls {
            for (name, path) in [("server.pem", &pki.cert), ("server.key", &pki.key)] {
                req = req.with_copy_to(
                    CopyTargetOptions::new(format!("/certs/{name}")).with_mode(0o644),
                    path.clone(),
                );
            }
        }
        let container = req
            .start()
            .await
            .context("starting Azurite via testcontainers (is Docker running?)")?;
        Ok(Self {
            _container: container,
            port,
        })
    }
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
}

/// The `blocks_storage.azure` settings of a scenario (identical for Go and Rust apart from the
/// container name).
#[derive(Clone, Debug)]
struct AzureScenario {
    name: &'static str,
    cfg: AzureConfig,
}

fn azure_yaml(cfg: &AzureConfig) -> String {
    let mut s = String::from("  azure:\n");
    let mut kv = |k: &str, v: &str| {
        if !v.is_empty() {
            s.push_str(&format!("    {k}: {v:?}\n"));
        }
    };
    kv("account_name", &cfg.account_name);
    kv("account_key", &cfg.account_key);
    kv("connection_string", &cfg.connection_string);
    kv("container_name", &cfg.container_name);
    kv("endpoint_suffix", &cfg.endpoint_suffix);
    if !cfg.http.tls_ca_path.is_empty() {
        s.push_str(&format!(
            "    http:\n      tls_ca_path: {:?}\n",
            cfg.http.tls_ca_path
        ));
    }
    s
}

fn ingester_yaml(http_port: u16, grpc_port: u16, tsdb_dir: &Path, azure: &AzureConfig) -> String {
    format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
multitenancy_enabled: true
usage_stats:
  enabled: false
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
  backend: azure
{azure}  tsdb:
    dir: {tsdb}
    ship_interval: 1m
activity_tracker:
  filepath: ""
"#,
        azure = azure_yaml(azure),
        tsdb = tsdb_dir.display(),
    )
}

/// What one ingester run left in object storage (block ULIDs normalised).
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    /// Object names under `<tenant>/` with the block ULID replaced by `<ULID>`.
    objects: Vec<String>,
    /// `meta.json` fields that must match between Go and Rust.
    meta: BTreeMap<String, serde_json::Value>,
}

fn write_request(base_ts: i64) -> cortexpb::WriteRequest {
    let series = |job: &'static str| cortexpb::TimeSeries {
        labels: vec![
            cortexpb::LabelPair::from_static("__name__", "w8_azure_metric"),
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
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![series("a"), series("b"), series("c")],
        ..Default::default()
    }
}

fn lister(cfg: &AzureConfig) -> Result<BucketClient> {
    BucketClient::from_config(&BucketConfig {
        backend_cfg: StorageBackendConfig {
            backend: "azure".into(),
            azure: cfg.clone(),
            ..Default::default()
        },
        storage_prefix: String::new(),
    })
    .context("building verification bucket client")
}

async fn run_ingester(imp: Impl, sc: &AzureScenario, base_ts: i64) -> Result<Outcome> {
    let dir = tempfile::tempdir()?;
    let tsdb = dir.path().join("tsdb");
    fs::create_dir_all(&tsdb)?;
    let mut cfg = sc.cfg.clone();
    cfg.container_name = format!("w8-{}-{}", sc.name.replace('_', "-"), imp.label());
    let (http_port, grpc_port) = (free_tcp_port(), free_tcp_port());
    let yaml_path = dir.path().join("mimir.yaml");
    fs::write(&yaml_path, ingester_yaml(http_port, grpc_port, &tsdb, &cfg))?;
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
    let mut req = tonic::Request::new(write_request(base_ts));
    req.metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(TENANT)?);
    if let Err(e) = client.push(req).await {
        bail!(ctx(&format!("push failed: {e}"), &proc));
    }

    let r = http
        .post(format!("http://127.0.0.1:{http_port}/ingester/flush?wait=true"))
        .send()
        .await?;
    if !r.status().is_success() {
        bail!(ctx(&format!("flush returned {}", r.status()), &proc));
    }

    // Built only now: if the ingester had not created the container, nothing could have been
    // uploaded and the listing below stays empty.
    let cfg2 = cfg.clone();
    let bucket = tokio::task::spawn_blocking(move || lister(&cfg2)).await??;
    let deadline = Instant::now() + Duration::from_secs(60);
    let names = loop {
        let names = bucket.list_all(TENANT).await.unwrap_or_default();
        if names.iter().any(|n| n.ends_with("/meta.json")) {
            break names;
        }
        if Instant::now() > deadline {
            bail!(ctx(
                &format!("no block shipped to container {} within 60s (objects: {names:?})", cfg.container_name),
                &proc
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let mut objects = Vec::new();
    let mut meta = BTreeMap::new();
    for n in &names {
        let rel = n.strip_prefix(&format!("{TENANT}/")).unwrap_or(n);
        let mut parts: Vec<&str> = rel.split('/').collect();
        if parts.len() >= 2 && ulid::Ulid::from_string(parts[0]).is_ok() {
            if parts[1] == "meta.json" {
                let raw = bucket.download(n).await?;
                let v: serde_json::Value = serde_json::from_slice(&raw)?;
                meta.insert("stats".into(), v["stats"].clone());
                meta.insert("version".into(), v["version"].clone());
                meta.insert("thanos.labels".into(), v["thanos"]["labels"].clone());
                meta.insert("thanos.source".into(), v["thanos"]["source"].clone());
                meta.insert("compaction.level".into(), v["compaction"]["level"].clone());
            }
            parts[0] = "<ULID>";
        }
        objects.push(parts.join("/"));
    }
    objects.sort();

    proc.sigterm();
    let deadline = Instant::now() + Duration::from_secs(30);
    while proc.try_wait().is_none() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(Outcome { objects, meta })
}

async fn run_scenario(sc: AzureScenario) -> Result<String> {
    let base_ts = now_ms() - 60_000;
    // Go-vs-Go validation first: the scenario must be valid for the reference implementation.
    let go = run_ingester(Impl::Go, &sc, base_ts)
        .await
        .with_context(|| format!("Go reference failed scenario {}", sc.name))?;
    ensure!(
        go.objects.iter().any(|o| o == "<ULID>/meta.json")
            && go.objects.iter().any(|o| o == "<ULID>/index")
            && go.objects.iter().any(|o| o.starts_with("<ULID>/chunks/")),
        "Go reference shipped an incomplete block in scenario {}: {:?}",
        sc.name,
        go.objects
    );
    let rs = run_ingester(Impl::Rust, &sc, base_ts).await?;
    ensure!(
        go == rs,
        "scenario {}: Go/Rust outcome mismatch\n  go:   {go:?}\n  rust: {rs:?}",
        sc.name
    );
    Ok(format!("{}: {:?}", sc.name, go.objects))
}

#[test]
fn w8_azure_azurite_container_create_and_block_shipping_go_rust_parity() {
    const NAME: &str = "w8_azure_azurite_container_create_and_block_shipping_go_rust_parity";
    if skip_docker(NAME) {
        return;
    }
    ensure_fresh_rust_binary();
    block_on_for_tests(async {
        let pki_dir = tempfile::tempdir().unwrap();
        let pki = gen_pki(pki_dir.path()).expect("generating test PKI with openssl");
        let plain = Azurite::start(None).await.expect("start Azurite (http)");
        let tls = Azurite::start(Some(&pki)).await.expect("start Azurite (https)");

        let scenarios = vec![
            AzureScenario {
                name: "connection_string_http",
                cfg: AzureConfig {
                    account_name: DEV_ACCOUNT.into(),
                    connection_string: format!(
                        "DefaultEndpointsProtocol=http;AccountName={DEV_ACCOUNT};AccountKey={DEV_KEY};BlobEndpoint=http://127.0.0.1:{}/{DEV_ACCOUNT};",
                        plain.port
                    ),
                    ..Default::default()
                },
            },
            AzureScenario {
                name: "endpoint_suffix_https",
                cfg: {
                    let mut c = AzureConfig {
                        account_name: DEV_ACCOUNT.into(),
                        account_key: DEV_KEY.into(),
                        endpoint_suffix: format!("blob.localhost:{}", tls.port),
                        ..Default::default()
                    };
                    c.http.tls_ca_path = pki.ca.to_string_lossy().into_owned();
                    c
                },
            },
        ];

        let mut failures = Vec::new();
        for sc in scenarios {
            let name = sc.name;
            match run_scenario(sc).await {
                Ok(summary) => eprintln!("PASS {summary}"),
                Err(e) => {
                    eprintln!("FAIL {name}: {e:#}");
                    failures.push(format!("{name}: {e:#}"));
                }
            }
        }
        assert!(failures.is_empty(), "failures:\n{}", failures.join("\n\n"));
    });
}
