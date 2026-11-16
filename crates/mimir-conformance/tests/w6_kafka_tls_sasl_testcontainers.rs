//! OBSERV-1374 — Kafka `ingest_storage` TLS / SASL and Kafka-outage differential conformance.
//!
//! A real Redpanda broker is started with testcontainers-rs and both the unmodified Go
//! `mimir` (`target/go-mimir`) and the Rust `mimir` (`target/release/mimir`) are booted as
//! `-target=ingester` with byte-identical `ingest_storage.kafka` configuration. Every
//! assertion is differential: Go Mimir defines the expected behavior.
//!
//! Upstream behavior under test:
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (`commonKafkaClientOptions`:
//!   `kafkaAuthOptions` + `kgo.DialTLSConfig`)
//! - github.com/grafana/mimir/pkg/storage/ingest/config.go (`KafkaConfig` `sasl_*` / `tls_*`)
//! - github.com/grafana/dskit/crypto/tls/tls.go (`ClientConfig.GetTLSConfig`)
//! - github.com/grafana/mimir/pkg/mimir/mimir.go (`readyHandler`: `Some services are not Running:`)
//!
//! Records are produced by `target/go-oracle kafka-produce`, which builds its client via
//! upstream `ingest.KafkaConfig` + `ingest.NewKafkaWriterClient` from the same YAML block, so
//! each scenario's client config is first proven against Go's own Kafka client.
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).
//! `MIMIR_CONFORMANCE_RUST_BIN=/path/to/mimir` tests a prebuilt Rust binary instead of
//! rebuilding `target/release/mimir`.
//! Leftover containers (e.g. after SIGKILL of the test runner) carry the label
//! `org.mimir-rs.conformance=observ-1374`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use testcontainers::core::{ContainerPort, ExecCommand};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{
    go_mimir_bin, invoke_oracle, rust_mimir_release_bin, workspace_root, KfakeRecordInput,
    LabelJson, SampleJson, TimeSeriesJson, WriteRequestJson,
};
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};

const REDPANDA_IMAGE: &str = "redpandadata/redpanda";
const REDPANDA_TAG: &str =
    "v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "observ-1374");

const TENANT: &str = "tenant-observ-1374";
const SCRAM256_USER: &str = "mimir-scram256";
const SCRAM256_PASS: &str = "scram256-Secret-1374";
const SCRAM512_USER: &str = "mimir-scram512";
const SCRAM512_PASS: &str = "scram512-Secret-1374";
const SERIES_PER_RECORD: usize = 8;
const RECORDS_PER_BATCH: usize = 2;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

// ---------------------------------------------------------------------------------------------
// Preconditions
// ---------------------------------------------------------------------------------------------

fn skip_docker(test: &str) -> bool {
    if std::env::var("MIMIR_CONFORMANCE_SKIP_DOCKER").as_deref() == Ok("1") {
        eprintln!("SKIPPED {test}: MIMIR_CONFORMANCE_SKIP_DOCKER=1 (this test needs Docker)");
        return true;
    }
    false
}

fn rust_bin_override() -> Option<PathBuf> {
    std::env::var_os("MIMIR_CONFORMANCE_RUST_BIN")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Always rebuilds the Go oracle (new subcommands) and the Rust release binary (incremental), so
/// these tests can never silently exercise a stale `target/release/mimir`.
fn ensure_fresh_binaries() {
    static BUILT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    let res = BUILT.get_or_init(|| {
        let root = workspace_root();
        let status = Command::new(root.join("scripts").join("build-go-oracle.sh"))
            .current_dir(&root)
            .status()
            .map_err(|e| format!("spawn scripts/build-go-oracle.sh: {e}"))?;
        if !status.success() {
            return Err(format!("scripts/build-go-oracle.sh exited with {status}"));
        }
        if let Some(bin) = rust_bin_override() {
            eprintln!("using MIMIR_CONFORMANCE_RUST_BIN={}", bin.display());
            return Ok(());
        }
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(["build", "--release", "-p", "mimir-cmd"])
            .current_dir(&root)
            .status()
            .map_err(|e| format!("spawn cargo build --release -p mimir-cmd: {e}"))?;
        if !status.success() {
            return Err(format!(
                "cargo build --release -p mimir-cmd exited with {status}"
            ));
        }
        Ok(())
    });
    if let Err(e) = res {
        panic!("building conformance binaries failed: {e}");
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Hands out `n` distinct free TCP ports for listeners.
///
/// Ports come from 20000..32000, i.e. BELOW Linux's default ephemeral range (32768..60999):
/// dozens of concurrent Go/Rust processes open outgoing Kafka connections, and an OS-picked
/// ephemeral port (`bind :0`, then release) can be grabbed by one of those connections before
/// the ingester binds it. A process-wide counter also keeps concurrent scenarios apart.
fn free_tcp_ports(n: usize) -> Vec<u16> {
    use std::sync::atomic::{AtomicU32, Ordering};
    const LOW: u32 = 20_000;
    const SPAN: u32 = 12_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let offset = (std::process::id() * 397) % SPAN;
    let mut out = Vec::with_capacity(n);
    for _ in 0..SPAN {
        if out.len() == n {
            break;
        }
        let port = (LOW + (offset + NEXT.fetch_add(1, Ordering::Relaxed)) % SPAN) as u16;
        let free = ["127.0.0.1", "0.0.0.0"]
            .iter()
            .all(|host| StdTcpListener::bind((*host, port)).is_ok());
        if free {
            out.push(port);
        }
    }
    assert_eq!(
        out.len(),
        n,
        "no free TCP ports left in {LOW}..{}",
        LOW + SPAN
    );
    out
}

// ---------------------------------------------------------------------------------------------
// Child process guard (stdout+stderr captured, killed and reaped on drop)
// ---------------------------------------------------------------------------------------------

struct ProcessGuard {
    child: Option<Child>,
    log: Arc<Mutex<Vec<u8>>>,
    readers: Vec<JoinHandle<()>>,
    exit: Option<ExitStatus>,
}

impl ProcessGuard {
    fn spawn(name: &str, bin: &Path, config: &Path, workdir: &Path) -> Self {
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
            .unwrap_or_else(|e| panic!("failed to spawn {name} ({}): {e}", bin.display()));
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
                    let mut g = log.lock().unwrap();
                    g.extend_from_slice(&buf[..n]);
                    if g.len() > 4 << 20 {
                        let excess = g.len() - (2 << 20);
                        g.drain(..excess);
                    }
                }
            }));
        }
        Self {
            child: Some(child),
            log,
            readers,
            exit: None,
        }
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        if self.exit.is_none() {
            if let Some(c) = self.child.as_mut() {
                self.exit = c.try_wait().ok().flatten();
            }
        }
        self.exit
    }

    fn sigterm(&self) {
        if let Some(c) = self.child.as_ref() {
            let _ = Command::new("kill")
                .args(["-TERM", &c.id().to_string()])
                .status();
        }
    }

    async fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(st) = self.try_wait() {
                return Some(st);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn log_tail(&self, lines: usize) -> String {
        let g = self.log.lock().unwrap();
        let text = String::from_utf8_lossy(&g);
        let all: Vec<&str> = text.lines().collect();
        let start = all.len().saturating_sub(lines);
        all[start..]
            .iter()
            .map(|l| {
                let mut l = l.to_string();
                if l.len() > 400 {
                    let mut cut = 400;
                    while !l.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    l.truncate(cut);
                    l.push('…');
                }
                format!("        | {l}")
            })
            .collect::<Vec<_>>()
            .join("\n")
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
// Test PKI (generated by the Go oracle with crypto/x509)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct Pki {
    ca_cert: String,
    server_cert: String,
    server_key: String,
    sni_cert: String,
    sni_key: String,
    sni_name: String,
    client_cert: String,
    client_key: String,
}

// ---------------------------------------------------------------------------------------------
// Redpanda fixture
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Listener {
    /// Plaintext, no authentication.
    Plain,
    /// Plaintext + SASL (SCRAM / PLAIN).
    SaslPlain,
    /// TLS (server cert for `localhost` / `127.0.0.1`), no authentication.
    Tls,
    /// TLS + SASL — the Redpanda Cloud / OBSERV-1374 production shape.
    SaslTls,
    /// TLS whose certificate is only valid for `Pki::sni_name` (needs `tls_server_name`).
    TlsSni,
    /// Mutual TLS (client certificate required).
    Mtls,
}

const LISTENERS: [Listener; 6] = [
    Listener::Plain,
    Listener::SaslPlain,
    Listener::Tls,
    Listener::SaslTls,
    Listener::TlsSni,
    Listener::Mtls,
];

impl Listener {
    fn name(self) -> &'static str {
        match self {
            Listener::Plain => "plain",
            Listener::SaslPlain => "sasl_plain",
            Listener::Tls => "tls",
            Listener::SaslTls => "sasl_tls",
            Listener::TlsSni => "tls_sni",
            Listener::Mtls => "mtls",
        }
    }

    fn sasl(self) -> bool {
        matches!(self, Listener::SaslPlain | Listener::SaslTls)
    }

    /// (cert, key, require_client_auth)
    fn tls(self) -> Option<(&'static str, &'static str, bool)> {
        match self {
            Listener::Plain | Listener::SaslPlain => None,
            Listener::Tls | Listener::SaslTls => Some(("server.crt", "server.key", false)),
            Listener::TlsSni => Some(("sni.crt", "sni.key", false)),
            Listener::Mtls => Some(("server.crt", "server.key", true)),
        }
    }
}

fn redpanda_yaml(ports: &BTreeMap<Listener, u16>) -> String {
    let mut y = String::from(
        "redpanda:\n  data_directory: /var/lib/redpanda/data\n  empty_seed_starts_cluster: true\n  seed_servers: []\n  rpc_server: {address: 0.0.0.0, port: 33145}\n  advertised_rpc_api: {address: 127.0.0.1, port: 33145}\n  admin: [{address: 0.0.0.0, port: 9644}]\n  developer_mode: true\n  kafka_api:\n    - {name: internal, address: 0.0.0.0, port: 9092}\n",
    );
    for (l, p) in ports {
        let auth = if l.sasl() {
            ", authentication_method: sasl"
        } else {
            ""
        };
        let _ = writeln!(
            y,
            "    - {{name: {}, address: 0.0.0.0, port: {p}{auth}}}",
            l.name()
        );
    }
    y.push_str("  advertised_kafka_api:\n    - {name: internal, address: 127.0.0.1, port: 9092}\n");
    for (l, p) in ports {
        let _ = writeln!(
            y,
            "    - {{name: {}, address: 127.0.0.1, port: {p}}}",
            l.name()
        );
    }
    y.push_str("  kafka_api_tls:\n");
    for l in ports.keys() {
        if let Some((cert, key, mtls)) = l.tls() {
            let _ = writeln!(
                y,
                "    - {{name: {}, enabled: true, cert_file: /config/{cert}, key_file: /config/{key}, truststore_file: /config/ca.crt, require_client_auth: {mtls}}}",
                l.name()
            );
        }
    }
    y
}

struct Redpanda {
    container: ContainerAsync<GenericImage>,
    ports: BTreeMap<Listener, u16>,
    pki: Pki,
    _dir: tempfile::TempDir,
}

impl Redpanda {
    async fn start() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let pki_dir = dir.path().join("pki");
        let pki: Pki = tokio::task::spawn_blocking(move || {
            invoke_oracle("gen-test-pki", &serde_json::json!({ "dir": pki_dir }))
        })
        .await??;

        let ports: BTreeMap<Listener, u16> = LISTENERS
            .iter()
            .copied()
            .zip(free_tcp_ports(LISTENERS.len()))
            .collect();

        let mut req = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
            .with_entrypoint("/bin/sh")
            .with_cmd([
                "-c",
                // The image's /etc/redpanda is owned by the redpanda user and `rpk` rewrites
                // redpanda.yaml in place, so copy our config there instead of bind-mounting it.
                "cp /config/redpanda.yaml /etc/redpanda/redpanda.yaml && exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --default-log-level=info",
            ])
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_startup_timeout(Duration::from_secs(180))
            .with_copy_to(
                CopyTargetOptions::new("/config/redpanda.yaml").with_mode(0o644),
                redpanda_yaml(&ports).into_bytes(),
            );
        for (name, path) in [
            ("ca.crt", &pki.ca_cert),
            ("server.crt", &pki.server_cert),
            ("server.key", &pki.server_key),
            ("sni.crt", &pki.sni_cert),
            ("sni.key", &pki.sni_key),
        ] {
            req = req.with_copy_to(
                CopyTargetOptions::new(format!("/config/{name}")).with_mode(0o644),
                PathBuf::from(path),
            );
        }
        for p in ports.values() {
            req = req.with_mapped_port(*p, ContainerPort::Tcp(*p));
        }
        let container = req
            .start()
            .await
            .context("starting Redpanda via testcontainers (is Docker running?)")?;

        let rp = Self {
            container,
            ports,
            pki,
            _dir: dir,
        };
        rp.wait_healthy().await?;
        rp.rpk(&[
            "security",
            "user",
            "create",
            SCRAM256_USER,
            "-p",
            SCRAM256_PASS,
            "--mechanism",
            "SCRAM-SHA-256",
        ])
        .await?;
        rp.rpk(&[
            "security",
            "user",
            "create",
            SCRAM512_USER,
            "-p",
            SCRAM512_PASS,
            "--mechanism",
            "SCRAM-SHA-512",
        ])
        .await?;
        rp.rpk(&[
            "cluster",
            "config",
            "set",
            "sasl_mechanisms",
            r#"["SCRAM","PLAIN"]"#,
        ])
        .await?;
        Ok(rp)
    }

    async fn wait_healthy(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let attempt = tokio::time::timeout(
                Duration::from_secs(30),
                self.rpk(&["cluster", "health", "--exit-when-healthy"]),
            )
            .await;
            match attempt {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) if Instant::now() >= deadline => {
                    return Err(e.context("Redpanda never became healthy"))
                }
                Err(_) if Instant::now() >= deadline => bail!("Redpanda health check timed out"),
                _ => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
    }

    async fn rpk(&self, args: &[&str]) -> Result<String> {
        let cmd: Vec<String> = std::iter::once("rpk")
            .chain(args.iter().copied())
            .map(str::to_string)
            .collect();
        let mut res = self.container.exec(ExecCommand::new(cmd)).await?;
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
            "rpk {args:?} exited {code}: {}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
        Ok(String::from_utf8_lossy(&stdout).into_owned())
    }

    async fn create_topic(&self, topic: &str) -> Result<()> {
        self.rpk(&["topic", "create", topic, "-p", "1", "-r", "1"])
            .await
            .map(|_| ())
    }

    fn addr(&self, l: Listener) -> String {
        format!("127.0.0.1:{}", self.ports[&l])
    }
}

// ---------------------------------------------------------------------------------------------
// Kafka client config shared verbatim by Go Mimir, Rust Mimir and the Go oracle producer
// ---------------------------------------------------------------------------------------------

type Kv = Vec<(&'static str, String)>;

#[derive(Debug, Clone)]
struct KafkaClientCfg {
    address: String,
    topic: String,
    /// `sasl_*` / `tls_*` / misc keys of `ingest_storage.kafka` (upstream YAML names).
    extra: Kv,
}

impl KafkaClientCfg {
    fn oracle_yaml(&self) -> String {
        let mut y = format!("address: {}\ntopic: {}\n", self.address, self.topic);
        for (k, v) in &self.extra {
            let _ = writeln!(y, "{k}: {v}");
        }
        y
    }

    fn mimir_block(&self, consumer_group: &str, consume_from: &str) -> String {
        let mut y = String::new();
        for (k, v) in [
            ("address", self.address.clone()),
            ("topic", self.topic.clone()),
            ("consumer_group", consumer_group.to_string()),
            ("consume_from_position_at_startup", consume_from.to_string()),
            ("consumer_group_offset_commit_interval", "50ms".to_string()),
            ("last_produced_offset_poll_interval", "25ms".to_string()),
        ]
        .into_iter()
        .chain(self.extra.iter().cloned())
        {
            let _ = writeln!(y, "    {k}: {v}");
        }
        y
    }
}

fn scram(mechanism: &str, user: &str, pass: &str) -> Kv {
    vec![
        ("sasl_mechanism", mechanism.to_string()),
        ("sasl_username", user.to_string()),
        ("sasl_password", pass.to_string()),
    ]
}

fn tls_ca(pki: &Pki) -> Kv {
    vec![
        ("tls_enabled", "true".to_string()),
        ("tls_ca_path", pki.ca_cert.clone()),
    ]
}

fn sarah_prod_cfg(rp: &Redpanda, topic: &str) -> KafkaClientCfg {
    let mut extra = scram("SCRAM-SHA-256", SCRAM256_USER, SCRAM256_PASS);
    extra.extend(tls_ca(&rp.pki));
    KafkaClientCfg {
        address: rp.addr(Listener::SaslTls),
        topic: topic.to_string(),
        extra,
    }
}

#[derive(Serialize)]
struct KafkaProduceReq {
    kafka_yaml: String,
    records: Vec<KfakeRecordInput>,
    consumer_groups: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KafkaProduceResp {
    end_offsets: BTreeMap<String, i64>,
    committed: BTreeMap<String, i64>,
}

impl KafkaProduceResp {
    fn end_offset(&self) -> i64 {
        self.end_offsets.get("0").copied().unwrap_or(-1)
    }
}

/// Produces through upstream `ingest.NewKafkaWriterClient` and returns end / committed offsets.
async fn oracle_kafka(
    cfg: &KafkaClientCfg,
    records: Vec<KfakeRecordInput>,
    consumer_groups: &[String],
) -> Result<KafkaProduceResp> {
    let req = KafkaProduceReq {
        kafka_yaml: cfg.oracle_yaml(),
        records,
        consumer_groups: consumer_groups.to_vec(),
    };
    tokio::task::spawn_blocking(move || invoke_oracle("kafka-produce", &req)).await?
}

// ---------------------------------------------------------------------------------------------
// Workload + expected data
// ---------------------------------------------------------------------------------------------

type Series = (Vec<(String, String)>, Vec<(i64, u64)>);

fn series_labels(scenario: &str, i: usize) -> Vec<(String, String)> {
    vec![
        ("__name__".into(), "observ1374_requests_total".into()),
        ("pod".into(), format!("pod-{i:02}")),
        ("scenario".into(), scenario.into()),
    ]
}

fn sample_at(base_ts: i64, batch: usize, i: usize) -> (i64, f64) {
    (
        base_ts + batch as i64 * 15_000,
        (batch * 1000 + i) as f64 + 0.25,
    )
}

fn batch_records(scenario: &str, base_ts: i64, batch: usize) -> Vec<KfakeRecordInput> {
    (0..RECORDS_PER_BATCH)
        .map(|r| KfakeRecordInput {
            partition: 0,
            tenant: TENANT.to_string(),
            version: 2,
            compression: None,
            write_request: WriteRequestJson {
                mode: "rw2".to_string(),
                source: "API".to_string(),
                timeseries: (r * SERIES_PER_RECORD..(r + 1) * SERIES_PER_RECORD)
                    .map(|i| {
                        let (t, v) = sample_at(base_ts, batch, i);
                        TimeSeriesJson {
                            labels: series_labels(scenario, i)
                                .into_iter()
                                .map(|(name, value)| LabelJson { name, value })
                                .collect(),
                            samples: vec![SampleJson {
                                timestamp_ms: t,
                                value: v,
                            }],
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            },
        })
        .collect()
}

fn expected_series(scenario: &str, base_ts: i64, batches: usize) -> Vec<Series> {
    let mut out: Vec<Series> = (0..RECORDS_PER_BATCH * SERIES_PER_RECORD)
        .map(|i| {
            let samples = (0..batches)
                .map(|b| {
                    let (t, v) = sample_at(base_ts, b, i);
                    (t, v.to_bits())
                })
                .collect();
            (series_labels(scenario, i), samples)
        })
        .collect();
    out.sort();
    out
}

fn describe_diff(want: &[Series], got: &[Series]) -> String {
    let samples = |s: &[Series]| s.iter().map(|(_, v)| v.len()).sum::<usize>();
    let mut msg = format!(
        "want {} series / {} samples, got {} series / {} samples",
        want.len(),
        samples(want),
        got.len(),
        samples(got)
    );
    if let Some((w, g)) = want.iter().zip(got.iter()).find(|(w, g)| w != g) {
        let _ = write!(msg, "; first difference: want {w:?}, got {g:?}");
    }
    msg
}

// ---------------------------------------------------------------------------------------------
// gRPC QueryStream
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Consistency {
    /// Strong read consistency at an explicit offset (`__consistency_offsets__`).
    Offset(i64),
    /// Strong read consistency without offsets: the ingester must look up the last produced
    /// offset itself (partition offset reader → Kafka `ListOffsets`).
    LastProduced,
}

fn wire_enc_to_tsdb(enc: i32) -> Result<Encoding> {
    Ok(match enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => bail!("unexpected wire chunk encoding {other}"),
    })
}

async fn query_stream(
    grpc_port: u16,
    consistency: Consistency,
    base_ts: i64,
) -> Result<Vec<Series>> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .context("connect gRPC")?;
    let mut client = IngesterClient::new(ch);
    let mut req = tonic::Request::new(ingesterpb::QueryRequest {
        start_timestamp_ms: base_ts - 3_600_000,
        end_timestamp_ms: now_ms() + 3_600_000,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::RegexMatch as i32,
            name: "__name__".to_string(),
            value: ".+".to_string(),
        }],
        streaming_chunks_batch_size: 8,
    });
    req.set_timeout(Duration::from_secs(30));
    let md = req.metadata_mut();
    md.insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::from_static(TENANT));
    md.insert(
        "__consistency_level__",
        MetadataValue::from_static("strong"),
    );
    if let Consistency::Offset(off) = consistency {
        md.insert(
            "__consistency_offsets__",
            MetadataValue::try_from(format!("v1=0:{off}"))?,
        );
    }

    let mut stream = client
        .query_stream(req)
        .await
        .map_err(|s| anyhow::anyhow!("QueryStream failed: {s}"))?
        .into_inner();
    let mut labels: Vec<Vec<(String, String)>> = Vec::new();
    let mut samples: BTreeMap<u64, Vec<(i64, u64)>> = BTreeMap::new();
    while let Some(item) = stream.next().await {
        let msg = item.map_err(|s| anyhow::anyhow!("QueryStream stream error: {s}"))?;
        for s in msg.streaming_series {
            let mut pairs: Vec<(String, String)> = s
                .labels
                .into_iter()
                .map(|l| {
                    (
                        String::from_utf8_lossy(&l.name).into_owned(),
                        String::from_utf8_lossy(&l.value).into_owned(),
                    )
                })
                .collect();
            pairs.sort();
            labels.push(pairs);
        }
        for sc in msg.streaming_series_chunks {
            let entry = samples.entry(sc.series_index).or_default();
            for c in sc.chunks {
                let chunk =
                    EncodedChunk::from_bytes(wire_enc_to_tsdb(c.encoding)?, c.data.to_vec())
                        .map_err(|e| anyhow::anyhow!("decode chunk: {e:?}"))?;
                for s in decode_chunk_samples(&chunk).map_err(|e| anyhow::anyhow!("{e:?}"))? {
                    match s.value {
                        SampleValue::Float(v) => entry.push((s.t, v.to_bits())),
                        other => bail!("unexpected non-float sample {other:?}"),
                    }
                }
            }
        }
    }
    let mut out: Vec<Series> = labels
        .into_iter()
        .enumerate()
        .map(|(idx, l)| {
            let mut v = samples.remove(&(idx as u64)).unwrap_or_default();
            v.sort();
            (l, v)
        })
        .collect();
    out.sort();
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// Twin Go + Rust ingesters
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Go,
    Rust,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Side::Go => "go",
            Side::Rust => "rust",
        }
    }

    fn bin(self) -> PathBuf {
        match self {
            Side::Go => go_mimir_bin(),
            Side::Rust => rust_bin_override().unwrap_or_else(rust_mimir_release_bin),
        }
    }
}

struct Instance {
    side: Side,
    http: u16,
    grpc: u16,
    dir: PathBuf,
    group: String,
    proc: ProcessGuard,
    spawned_at: Instant,
}

impl Instance {
    fn write_config(&self, kafka: &KafkaClientCfg, consume_from: &str) -> PathBuf {
        let cfg = format!(
            r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
multitenancy_enabled: true
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
  push_grpc_method_enabled: false
ingest_storage:
  enabled: true
  kafka:
{kafka}blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
            http = self.http,
            grpc = self.grpc,
            kafka = kafka.mimir_block(&self.group, consume_from),
            bucket = self.dir.join("bucket").display(),
            tsdb = self.dir.join("tsdb").display(),
        );
        let path = self.dir.join("mimir.yaml");
        fs::write(&path, cfg).unwrap();
        path
    }

    fn start(&mut self, kafka: &KafkaClientCfg, consume_from: &str) {
        let cfg = self.write_config(kafka, consume_from);
        self.proc = ProcessGuard::spawn(self.side.label(), &self.side.bin(), &cfg, &self.dir);
        self.spawned_at = Instant::now();
    }
}

struct Twin {
    go: Instance,
    rust: Instance,
    _dir: tempfile::TempDir,
}

impl Twin {
    fn spawn(kafka: &KafkaClientCfg, consume_from: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let ports = free_tcp_ports(4);
        let mk = |side: Side, http: u16, grpc: u16| {
            let d = dir.path().join(side.label());
            fs::create_dir_all(d.join("tsdb")).unwrap();
            fs::create_dir_all(d.join("bucket")).unwrap();
            let mut inst = Instance {
                side,
                http,
                grpc,
                dir: d,
                group: format!("{}-{}", kafka.topic, side.label()),
                proc: ProcessGuard {
                    child: None,
                    log: Arc::default(),
                    readers: Vec::new(),
                    exit: None,
                },
                spawned_at: Instant::now(),
            };
            inst.start(kafka, consume_from);
            inst
        };
        let go = mk(Side::Go, ports[0], ports[1]);
        let rust = mk(Side::Rust, ports[2], ports[3]);
        Self {
            go,
            rust,
            _dir: dir,
        }
    }

    fn groups(&self) -> Vec<String> {
        vec![self.go.group.clone(), self.rust.group.clone()]
    }

    fn log_tails(&self, lines: usize) -> String {
        format!(
            "      go log tail:\n{}\n      rust log tail:\n{}",
            self.go.proc.log_tail(lines),
            self.rust.proc.log_tail(lines)
        )
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP lifecycle observation (/ready + /metrics), the OBSERV-1374 startup contract
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Probe {
    NoAnswer,
    Exited(Option<i32>),
    Answered {
        ready: u16,
        ready_body: String,
        metrics: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Ready(Duration),
    Exited(Duration, Option<i32>),
    NotReadyAlive,
}

#[derive(Debug, Default, Clone)]
struct Lifecycle {
    samples: Vec<(Duration, Probe)>,
}

impl Lifecycle {
    fn first_answer(&self) -> Option<Duration> {
        self.samples
            .iter()
            .find(|(_, p)| matches!(p, Probe::Answered { .. }))
            .map(|(t, _)| *t)
    }

    fn outcome(&self) -> Outcome {
        for (t, p) in &self.samples {
            match p {
                Probe::Exited(code) => return Outcome::Exited(*t, *code),
                Probe::Answered { ready: 200, .. } => return Outcome::Ready(*t),
                _ => {}
            }
        }
        Outcome::NotReadyAlive
    }

    /// Samples after the first HTTP answer where the server did not answer or /metrics != 200.
    fn http_gaps(&self) -> usize {
        let Some(first) = self.first_answer() else {
            return 0;
        };
        self.samples
            .iter()
            .filter(|(t, p)| {
                *t > first
                    && match p {
                        Probe::NoAnswer => true,
                        Probe::Answered { metrics, .. } => *metrics != 200,
                        Probe::Exited(_) => false,
                    }
            })
            .count()
    }

    /// The last not-ready `/ready` response (steady state while Kafka is unavailable). Lines
    /// after the first are sorted: Go's readyHandler iterates a map, so service order is random.
    /// Earlier transient bodies (e.g. rings still `Starting` in the first few hundred ms) depend
    /// on sampling phase and are intentionally not compared.
    fn steady_not_ready_body(&self) -> Option<String> {
        self.samples.iter().rev().find_map(|(_, p)| match p {
            Probe::Answered {
                ready, ready_body, ..
            } if *ready != 200 => {
                let mut lines = ready_body.lines();
                let head = lines.next().unwrap_or_default().to_string();
                let mut rest: Vec<&str> = lines.collect();
                rest.sort_unstable();
                Some(format!(
                    "{ready} {:?}",
                    std::iter::once(head.as_str())
                        .chain(rest)
                        .collect::<Vec<_>>()
                        .join("\n")
                ))
            }
            _ => None,
        })
    }

    fn timeline(&self) -> String {
        let mut out: Vec<String> = Vec::new();
        let mut i = 0;
        while i < self.samples.len() {
            let (t0, p) = &self.samples[i];
            let mut j = i;
            while j + 1 < self.samples.len() && &self.samples[j + 1].1 == p {
                j += 1;
            }
            let desc = match p {
                Probe::NoAnswer => "HTTP no answer".to_string(),
                Probe::Exited(c) => format!("exited (code {c:?})"),
                Probe::Answered {
                    ready,
                    ready_body,
                    metrics,
                } => format!(
                    "/ready {ready} {:?} /metrics {metrics}",
                    ready_body.chars().take(120).collect::<String>()
                ),
            };
            out.push(format!(
                "{:.2}s..{:.2}s {desc}",
                t0.as_secs_f64(),
                self.samples[j].0.as_secs_f64()
            ));
            if matches!(p, Probe::Exited(_)) {
                break;
            }
            i = j + 1;
        }
        out.join(" | ")
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(1500))
        .pool_max_idle_per_host(0)
        .build()
        .unwrap()
}

async fn probe(http: &reqwest::Client, inst: &mut Instance) -> (Duration, Probe) {
    let t = inst.spawned_at.elapsed();
    if let Some(st) = inst.proc.try_wait() {
        return (t, Probe::Exited(st.code()));
    }
    let base = format!("http://127.0.0.1:{}", inst.http);
    let ready = match http.get(format!("{base}/ready")).send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            (status, resp.text().await.unwrap_or_default())
        }
        Err(_) => {
            if let Some(st) = inst.proc.try_wait() {
                return (t, Probe::Exited(st.code()));
            }
            return (t, Probe::NoAnswer);
        }
    };
    let metrics = match http.get(format!("{base}/metrics")).send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let _ = resp.bytes().await;
            status
        }
        Err(_) => 0,
    };
    (
        t,
        Probe::Answered {
            ready: ready.0,
            ready_body: ready.1,
            metrics,
        },
    )
}

fn settled(p: &Probe) -> bool {
    matches!(p, Probe::Exited(_) | Probe::Answered { ready: 200, .. })
}

/// Samples both instances every `SAMPLE_INTERVAL` until both are settled (ready or exited) or
/// `window` has elapsed (measured from now).
async fn observe(
    twin: &mut Twin,
    window: Duration,
    stop_when_settled: bool,
) -> (Lifecycle, Lifecycle) {
    let http = http_client();
    let start = Instant::now();
    let (mut go, mut rust) = (Lifecycle::default(), Lifecycle::default());
    loop {
        let (g, r) = tokio::join!(probe(&http, &mut twin.go), probe(&http, &mut twin.rust));
        let done = settled(&g.1) && settled(&r.1);
        go.samples.push(g);
        rust.samples.push(r);
        if (stop_when_settled && done) || start.elapsed() >= window {
            return (go, rust);
        }
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
}

/// Differential lifecycle contract: Rust must do what Go does.
fn lifecycle_parity(go: &Lifecycle, rust: &Lifecycle) -> Vec<String> {
    let mut errs = Vec::new();
    // OBSERV-1374 #2: HTTP (/metrics, /ready) must answer while Kafka is unavailable. (Not
    // applied when either side exits: Go also answers briefly while its modules shut down,
    // and exit parity is checked on the outcome below.)
    let exited = |lc: &Lifecycle| matches!(lc.outcome(), Outcome::Exited(..));
    if let Some(g) = go.first_answer().filter(|_| !exited(go) && !exited(rust)) {
        match rust.first_answer() {
            None => errs.push(format!(
                "Go served HTTP {:.2}s after start, Rust never answered HTTP",
                g.as_secs_f64()
            )),
            Some(r) if r > g + Duration::from_secs(5) => errs.push(format!(
                "Rust answered HTTP only after {:.2}s (Go after {:.2}s)",
                r.as_secs_f64(),
                g.as_secs_f64()
            )),
            _ => {}
        }
    }
    if !exited(go) && !exited(rust) && go.http_gaps() == 0 && rust.http_gaps() > 0 {
        errs.push(format!(
            "Rust had {} samples without an HTTP answer or with /metrics != 200 after first answering (Go: 0)",
            rust.http_gaps()
        ));
    }
    match (go.outcome(), rust.outcome()) {
        (Outcome::Ready(_), Outcome::Ready(_)) => {}
        (Outcome::Exited(_, gc), Outcome::Exited(_, rc)) => {
            if gc != rc {
                errs.push(format!("exit code mismatch: Go {gc:?}, Rust {rc:?}"));
            }
        }
        (Outcome::NotReadyAlive, Outcome::NotReadyAlive) => {
            let (gb, rb) = (go.steady_not_ready_body(), rust.steady_not_ready_body());
            if gb != rb {
                errs.push(format!(
                    "/ready not-ready responses differ: Go {gb:?}, Rust {rb:?}"
                ));
            }
        }
        (g, r) => errs.push(format!("lifecycle outcome mismatch: Go {g:?}, Rust {r:?}")),
    }
    errs
}

// ---------------------------------------------------------------------------------------------
// Ingest verification
// ---------------------------------------------------------------------------------------------

async fn query_both(
    twin: &Twin,
    c: Consistency,
    base_ts: i64,
) -> (Result<Vec<Series>>, Result<Vec<Series>>) {
    tokio::join!(
        query_stream(twin.go.grpc, c, base_ts),
        query_stream(twin.rust.grpc, c, base_ts)
    )
}

fn check_query(
    errs: &mut Vec<String>,
    what: &str,
    want: &[Series],
    got: (Result<Vec<Series>>, Result<Vec<Series>>),
) {
    match got.0 {
        Ok(g) if g == want => {}
        Ok(g) => errs.push(format!("harness: Go {what}: {}", describe_diff(want, &g))),
        Err(e) => errs.push(format!("harness: Go {what} failed: {e:#}")),
    }
    match got.1 {
        Ok(r) if r == want => {}
        Ok(r) => errs.push(format!("Rust {what}: {}", describe_diff(want, &r))),
        Err(e) => errs.push(format!("Rust {what} failed: {e:#}")),
    }
}

/// Polls committed consumer-group offsets until Go and Rust agree (Go is the reference).
async fn check_committed_offsets(
    errs: &mut Vec<String>,
    cfg: &KafkaClientCfg,
    twin: &Twin,
    end: i64,
) {
    let groups = twin.groups();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = None;
    while Instant::now() < deadline {
        match oracle_kafka(cfg, Vec::new(), &groups).await {
            Ok(resp) => {
                let g = resp.committed.get(&groups[0]).copied().unwrap_or(-2);
                let r = resp.committed.get(&groups[1]).copied().unwrap_or(-2);
                if g >= end - 1 && g == r {
                    return;
                }
                last = Some((g, r));
            }
            Err(e) => {
                errs.push(format!("harness: fetching committed offsets: {e:#}"));
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    errs.push(format!(
        "committed consumer-group offsets never converged (end offset {end}): (Go, Rust) = {last:?}"
    ));
}

/// Backlog (explicit offset) + live tail (`ListOffsets` path) + committed offsets.
async fn verify_ingest(
    cfg: &KafkaClientCfg,
    twin: &Twin,
    scenario: &str,
    base_ts: i64,
) -> Vec<String> {
    let mut errs = Vec::new();
    let end0 = match oracle_kafka(cfg, Vec::new(), &[]).await {
        Ok(r) => r.end_offset(),
        Err(e) => return vec![format!("harness: ListEndOffsets: {e:#}")],
    };
    let got = query_both(twin, Consistency::Offset(end0 - 1), base_ts).await;
    check_query(
        &mut errs,
        "backlog QueryStream",
        &expected_series(scenario, base_ts, 1),
        got,
    );

    let end1 = match oracle_kafka(cfg, batch_records(scenario, base_ts, 1), &[]).await {
        Ok(r) => r.end_offset(),
        Err(e) => {
            return {
                errs.push(format!("harness: producing live batch: {e:#}"));
                errs
            }
        }
    };
    let got = query_both(twin, Consistency::LastProduced, base_ts).await;
    check_query(
        &mut errs,
        "live-tail QueryStream (strong consistency, ListOffsets)",
        &expected_series(scenario, base_ts, 2),
        got,
    );
    check_committed_offsets(&mut errs, cfg, twin, end1).await;
    errs
}

// ---------------------------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------------------------

struct Report {
    name: String,
    errors: Vec<String>,
    go_timeline: String,
    rust_timeline: String,
    logs: String,
}

fn finish(title: &str, reports: Vec<Report>) {
    let failed = reports.iter().filter(|r| !r.errors.is_empty()).count();
    let mut out = format!(
        "\n===== {title}: {failed}/{} scenarios failed =====\n",
        reports.len()
    );
    for r in &reports {
        let status = if r.errors.is_empty() { "ok  " } else { "FAIL" };
        let _ = writeln!(out, "[{status}] {}", r.name);
        for e in &r.errors {
            let _ = writeln!(out, "    - {e}");
        }
        if !r.errors.is_empty() {
            let _ = writeln!(out, "      go:   {}", r.go_timeline);
            let _ = writeln!(out, "      rust: {}", r.rust_timeline);
            let _ = writeln!(out, "{}", r.logs);
        }
    }
    eprintln!("{out}");
    assert_eq!(failed, 0, "{out}");
}

// ---------------------------------------------------------------------------------------------
// Test 1: Sarah's production shape end to end (TLS + SCRAM-SHA-256)
// ---------------------------------------------------------------------------------------------

/// TLS + SCRAM-SHA-256 (the Redpanda Cloud production config from OBSERV-1374): startup with a
/// backlog, live tail, consumer-group commits, a broker outage after ready (pause/unpause),
/// and a SIGTERM restart resuming from the committed offset.
#[test]
fn observ_1374_prod_tls_scram_sha256_end_to_end() {
    if skip_docker("observ_1374_prod_tls_scram_sha256_end_to_end") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let rp = Redpanda::start().await.expect("start Redpanda");
        let scenario = "prod-tls-scram-sha-256";
        let topic = "o1374-prod";
        rp.create_topic(topic).await.unwrap();
        let cfg = sarah_prod_cfg(&rp, topic);
        let base_ts = now_ms() - 10 * 60_000;
        let mut errs: Vec<String> = Vec::new();

        oracle_kafka(&cfg, batch_records(scenario, base_ts, 0), &[])
            .await
            .expect(
                "harness: Go oracle must produce with the production TLS + SCRAM-SHA-256 config",
            );

        // 1. Startup with backlog.
        let mut twin = Twin::spawn(&cfg, "start");
        let (go_lc, rust_lc) = observe(&mut twin, Duration::from_secs(60), true).await;
        let mut go_tl = vec![format!("startup: {}", go_lc.timeline())];
        let mut rust_tl = vec![format!("startup: {}", rust_lc.timeline())];
        assert!(
            matches!(go_lc.outcome(), Outcome::Ready(_)),
            "harness: Go Mimir must become ready with TLS + SCRAM-SHA-256: {}\n{}",
            go_lc.timeline(),
            twin.log_tails(30)
        );
        errs.extend(lifecycle_parity(&go_lc, &rust_lc));

        if matches!(rust_lc.outcome(), Outcome::Ready(_)) {
            // 2. Backlog + live tail + commits.
            errs.extend(verify_ingest(&cfg, &twin, scenario, base_ts).await);

            // 3. Broker outage after ready: both must keep serving /metrics and /ready.
            rp.container.pause().await.expect("pause Redpanda");
            let (go_o, rust_o) = observe(&mut twin, Duration::from_secs(8), false).await;
            rp.container.unpause().await.expect("unpause Redpanda");
            go_tl.push(format!("outage: {}", go_o.timeline()));
            rust_tl.push(format!("outage: {}", rust_o.timeline()));
            for e in lifecycle_parity(&go_o, &rust_o) {
                errs.push(format!("during broker outage: {e}"));
            }
            let ready_states = |lc: &Lifecycle| {
                lc.samples
                    .iter()
                    .filter_map(|(_, p)| match p {
                        Probe::Answered { ready, .. } => Some(*ready),
                        _ => None,
                    })
                    .collect::<BTreeSet<u16>>()
            };
            if ready_states(&go_o) != ready_states(&rust_o) {
                errs.push(format!(
                    "/ready status during broker outage differs: Go {:?}, Rust {:?}",
                    ready_states(&go_o),
                    ready_states(&rust_o)
                ));
            }

            // 4. Recovery: new data after the outage must be consumed by both.
            match oracle_kafka(&cfg, batch_records(scenario, base_ts, 2), &[]).await {
                Ok(r) => {
                    let got =
                        query_both(&twin, Consistency::Offset(r.end_offset() - 1), base_ts).await;
                    check_query(
                        &mut errs,
                        "post-outage QueryStream",
                        &expected_series(scenario, base_ts, 3),
                        got,
                    );
                    check_committed_offsets(&mut errs, &cfg, &twin, r.end_offset()).await;
                }
                Err(e) => errs.push(format!("harness: producing post-outage batch: {e:#}")),
            }

            // 5. SIGTERM restart; batch 3 is produced while both are down and must be picked up
            //    from the committed offset (`consume_from_position_at_startup: last-offset`).
            twin.go.proc.sigterm();
            twin.rust.proc.sigterm();
            let (g, r) = tokio::join!(
                twin.go.proc.wait_exit(Duration::from_secs(60)),
                twin.rust.proc.wait_exit(Duration::from_secs(60))
            );
            let (gc, rc) = (g.map(|s| s.code()), r.map(|s| s.code()));
            if gc != rc {
                errs.push(format!("SIGTERM exit mismatch: Go {gc:?}, Rust {rc:?}"));
            }
            match oracle_kafka(&cfg, batch_records(scenario, base_ts, 3), &[]).await {
                Ok(resp) => {
                    twin.go.start(&cfg, "last-offset");
                    twin.rust.start(&cfg, "last-offset");
                    let (go_r, rust_r) = observe(&mut twin, Duration::from_secs(60), true).await;
                    go_tl.push(format!("restart: {}", go_r.timeline()));
                    rust_tl.push(format!("restart: {}", rust_r.timeline()));
                    for e in lifecycle_parity(&go_r, &rust_r) {
                        errs.push(format!("after restart: {e}"));
                    }
                    if matches!(rust_r.outcome(), Outcome::Ready(_)) {
                        let got =
                            query_both(&twin, Consistency::Offset(resp.end_offset() - 1), base_ts)
                                .await;
                        check_query(
                            &mut errs,
                            "post-restart QueryStream (WAL replay + resume from committed offset)",
                            &expected_series(scenario, base_ts, 4),
                            got,
                        );
                    }
                }
                Err(e) => errs.push(format!("harness: producing batch while stopped: {e:#}")),
            }
        }

        let logs = twin.log_tails(25);
        finish(
            "OBSERV-1374 production TLS + SCRAM-SHA-256 end to end",
            vec![Report {
                name: format!("{scenario} via {}", rp.addr(Listener::SaslTls)),
                errors: errs,
                go_timeline: go_tl.join("\n                "),
                rust_timeline: rust_tl.join("\n                "),
                logs,
            }],
        );
        drop(twin);
        drop(rp);
    });
}

// ---------------------------------------------------------------------------------------------
// Test 2: TLS / SASL client configuration matrix
// ---------------------------------------------------------------------------------------------

struct ClientScenario {
    name: &'static str,
    listener: Listener,
    extra: Kv,
    /// Harness sanity: whether Go Mimir is expected to become ready with this config.
    go_ready: bool,
}

fn client_scenarios(pki: &Pki) -> Vec<ClientScenario> {
    let with = |a: Kv, b: Kv| a.into_iter().chain(b).collect::<Kv>();
    vec![
        ClientScenario {
            name: "plaintext (control)",
            listener: Listener::Plain,
            extra: vec![],
            go_ready: true,
        },
        ClientScenario {
            name: "tls + scram-sha-256 (OBSERV-1374 prod)",
            listener: Listener::SaslTls,
            extra: with(
                scram("SCRAM-SHA-256", SCRAM256_USER, SCRAM256_PASS),
                tls_ca(pki),
            ),
            go_ready: true,
        },
        ClientScenario {
            name: "tls + scram-sha-512",
            listener: Listener::SaslTls,
            extra: with(
                scram("SCRAM-SHA-512", SCRAM512_USER, SCRAM512_PASS),
                tls_ca(pki),
            ),
            go_ready: true,
        },
        ClientScenario {
            name: "tls + sasl plain",
            listener: Listener::SaslTls,
            extra: with(scram("PLAIN", SCRAM256_USER, SCRAM256_PASS), tls_ca(pki)),
            go_ready: true,
        },
        ClientScenario {
            name: "tls + scram-sha-256 + tls_min_version VersionTLS13",
            listener: Listener::SaslTls,
            extra: with(
                with(
                    scram("SCRAM-SHA-256", SCRAM256_USER, SCRAM256_PASS),
                    tls_ca(pki),
                ),
                vec![("tls_min_version", "VersionTLS13".to_string())],
            ),
            go_ready: true,
        },
        ClientScenario {
            name: "scram-sha-256 without tls",
            listener: Listener::SaslPlain,
            extra: scram("SCRAM-SHA-256", SCRAM256_USER, SCRAM256_PASS),
            go_ready: true,
        },
        ClientScenario {
            name: "tls only (no sasl)",
            listener: Listener::Tls,
            extra: tls_ca(pki),
            go_ready: true,
        },
        ClientScenario {
            name: "tls_server_name override",
            listener: Listener::TlsSni,
            extra: with(tls_ca(pki), vec![("tls_server_name", pki.sni_name.clone())]),
            go_ready: true,
        },
        ClientScenario {
            name: "mtls client certificate",
            listener: Listener::Mtls,
            extra: with(
                tls_ca(pki),
                vec![
                    ("tls_cert_path", pki.client_cert.clone()),
                    ("tls_key_path", pki.client_key.clone()),
                ],
            ),
            go_ready: true,
        },
        ClientScenario {
            name: "tls_insecure_skip_verify without CA",
            listener: Listener::Tls,
            extra: vec![
                ("tls_enabled", "true".to_string()),
                ("tls_insecure_skip_verify", "true".to_string()),
            ],
            go_ready: true,
        },
        // Misconfigurations: Rust must fail exactly like Go does.
        ClientScenario {
            name: "wrong scram password",
            listener: Listener::SaslTls,
            extra: with(
                scram("SCRAM-SHA-256", SCRAM256_USER, "wrong-password"),
                tls_ca(pki),
            ),
            go_ready: false,
        },
        ClientScenario {
            name: "scram-sha-512 mechanism for a scram-sha-256 user",
            listener: Listener::SaslTls,
            extra: with(
                scram("SCRAM-SHA-512", SCRAM256_USER, SCRAM256_PASS),
                tls_ca(pki),
            ),
            go_ready: false,
        },
        ClientScenario {
            name: "sasl listener without credentials",
            listener: Listener::SaslTls,
            extra: tls_ca(pki),
            go_ready: false,
        },
        ClientScenario {
            name: "tls with untrusted CA",
            listener: Listener::Tls,
            extra: vec![("tls_enabled", "true".to_string())],
            go_ready: false,
        },
        ClientScenario {
            name: "tls server name mismatch",
            listener: Listener::TlsSni,
            extra: tls_ca(pki),
            go_ready: false,
        },
        ClientScenario {
            name: "mtls without client certificate",
            listener: Listener::Mtls,
            extra: tls_ca(pki),
            go_ready: false,
        },
        ClientScenario {
            name: "plaintext client to tls + sasl listener",
            listener: Listener::SaslTls,
            extra: scram("SCRAM-SHA-256", SCRAM256_USER, SCRAM256_PASS),
            go_ready: false,
        },
        ClientScenario {
            name: "tls client to plaintext listener",
            listener: Listener::Plain,
            extra: tls_ca(pki),
            go_ready: false,
        },
    ]
}

async fn run_client_scenario(
    rp: &Redpanda,
    idx: usize,
    sc: &ClientScenario,
    base_ts: i64,
) -> Report {
    let topic = format!("o1374-matrix-{idx}");
    let cfg = KafkaClientCfg {
        address: rp.addr(sc.listener),
        topic: topic.clone(),
        extra: sc.extra.clone(),
    };
    let mut report = Report {
        name: format!("{} [listener {}]", sc.name, sc.listener.name()),
        errors: Vec::new(),
        go_timeline: String::new(),
        rust_timeline: String::new(),
        logs: String::new(),
    };
    if let Err(e) = rp.create_topic(&topic).await {
        report.errors.push(format!("harness: create topic: {e:#}"));
        return report;
    }
    if sc.go_ready {
        if let Err(e) = oracle_kafka(&cfg, batch_records(sc.name, base_ts, 0), &[]).await {
            report.errors.push(format!(
                "harness: upstream ingest.NewKafkaWriterClient could not produce with this config: {e:#}"
            ));
            return report;
        }
    }

    let mut twin = Twin::spawn(&cfg, "start");
    let window = Duration::from_secs(if sc.go_ready { 60 } else { 20 });
    let (go_lc, rust_lc) = observe(&mut twin, window, true).await;
    report.go_timeline = go_lc.timeline();
    report.rust_timeline = rust_lc.timeline();
    let go_became_ready = matches!(go_lc.outcome(), Outcome::Ready(_));
    if go_became_ready != sc.go_ready {
        report.errors.push(format!(
            "harness: Go Mimir outcome {:?} contradicts scenario expectation (go_ready = {})",
            go_lc.outcome(),
            sc.go_ready
        ));
    }
    report.errors.extend(lifecycle_parity(&go_lc, &rust_lc));
    if go_became_ready && matches!(rust_lc.outcome(), Outcome::Ready(_)) {
        report
            .errors
            .extend(verify_ingest(&cfg, &twin, sc.name, base_ts).await);
    }
    if !report.errors.is_empty() {
        report.logs = twin.log_tails(12);
    }
    report
}

/// Every supported `ingest_storage.kafka` TLS / SASL combination (and the common
/// misconfigurations) against one Redpanda with six listeners, Go vs Rust.
#[test]
fn observ_1374_kafka_tls_sasl_matrix_go_rust_parity() {
    if skip_docker("observ_1374_kafka_tls_sasl_matrix_go_rust_parity") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let rp = Redpanda::start().await.expect("start Redpanda");
        let base_ts = now_ms() - 10 * 60_000;
        let scenarios = client_scenarios(&rp.pki);
        let reports = futures::future::join_all(
            scenarios
                .iter()
                .enumerate()
                .map(|(idx, sc)| run_client_scenario(&rp, idx, sc, base_ts)),
        )
        .await;
        finish("OBSERV-1374 Kafka TLS / SASL matrix", reports);
        drop(rp);
    });
}

// ---------------------------------------------------------------------------------------------
// Test 3: Kafka unavailable at startup
// ---------------------------------------------------------------------------------------------

struct OutageScenario {
    name: &'static str,
    cfg: KafkaClientCfg,
    /// Uses the (paused) Redpanda; after the outage window the broker is unpaused and both
    /// ingesters must recover the same way.
    redpanda: bool,
}

/// While Kafka is unreachable / unresponsive at startup, Rust must keep serving HTTP exactly like
/// Go (`/metrics` 200, `/ready` 503 with the same body) or fail exactly like Go, and must recover
/// like Go once the broker comes back.
#[test]
fn observ_1374_kafka_unavailable_at_startup_lifecycle_parity() {
    if skip_docker("observ_1374_kafka_unavailable_at_startup_lifecycle_parity") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let rp = Redpanda::start().await.expect("start Redpanda");
        rp.create_topic("o1374-outage-plain").await.unwrap();
        rp.create_topic("o1374-outage-prod").await.unwrap();

        // TCP accept + silence (an unresponsive broker / load balancer).
        let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blackhole_addr = blackhole.local_addr().unwrap().to_string();
        let blackhole_task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = blackhole.accept().await {
                held.push(sock);
            }
        });
        let refused_addr = format!("127.0.0.1:{}", free_tcp_ports(1)[0]);

        let plain = |name: &'static str, address: String| OutageScenario {
            name,
            cfg: KafkaClientCfg {
                address,
                topic: "o1374-outage".to_string(),
                extra: vec![],
            },
            redpanda: false,
        };
        let mut scenarios = vec![
            plain("connection refused", refused_addr),
            plain("dns nxdomain", "kafka.invalid:9092".to_string()),
            plain("tcp accept + silence", blackhole_addr),
            // RFC 5737 TEST-NET-1: SYNs are normally dropped (dial timeout path).
            plain(
                "unroutable address (syn dropped)",
                "192.0.2.1:9092".to_string(),
            ),
            OutageScenario {
                name: "redpanda paused, plaintext",
                cfg: KafkaClientCfg {
                    address: rp.addr(Listener::Plain),
                    topic: "o1374-outage-plain".to_string(),
                    extra: vec![],
                },
                redpanda: true,
            },
            OutageScenario {
                name: "redpanda paused, tls + scram-sha-256 (OBSERV-1374 prod)",
                cfg: sarah_prod_cfg(&rp, "o1374-outage-prod"),
                redpanda: true,
            },
        ];
        let topics: Vec<String> = (0..scenarios.len())
            .map(|i| format!("o1374-outage-{i}"))
            .collect();
        for (sc, topic) in scenarios.iter_mut().zip(&topics) {
            if !sc.redpanda {
                sc.cfg.topic = topic.clone();
            }
        }

        rp.container.pause().await.expect("pause Redpanda");
        let mut twins: Vec<Twin> = scenarios
            .iter()
            .map(|sc| Twin::spawn(&sc.cfg, "start"))
            .collect();

        // Phase A: Kafka unavailable for 20s.
        let phase_a: Vec<(Lifecycle, Lifecycle)> = futures::future::join_all(
            twins
                .iter_mut()
                .map(|t| observe(t, Duration::from_secs(20), true)),
        )
        .await;
        rp.container.unpause().await.expect("unpause Redpanda");

        // Phase B: broker back (Redpanda scenarios only).
        let phase_b: Vec<Option<(Lifecycle, Lifecycle)>> =
            futures::future::join_all(twins.iter_mut().zip(&scenarios).map(|(t, sc)| async move {
                if sc.redpanda {
                    Some(observe(t, Duration::from_secs(60), true).await)
                } else {
                    None
                }
            }))
            .await;

        let base_ts = now_ms() - 10 * 60_000;
        let mut reports = Vec::new();
        for (i, sc) in scenarios.iter().enumerate() {
            let (go_a, rust_a) = &phase_a[i];
            let mut errors: Vec<String> = lifecycle_parity(go_a, rust_a)
                .into_iter()
                .map(|e| format!("while unavailable: {e}"))
                .collect();
            let mut go_tl = format!("unavailable: {}", go_a.timeline());
            let mut rust_tl = format!("unavailable: {}", rust_a.timeline());
            if let Some((go_b, rust_b)) = &phase_b[i] {
                let _ = write!(go_tl, "\n                recovered: {}", go_b.timeline());
                let _ = write!(
                    rust_tl,
                    "\n                recovered: {}",
                    rust_b.timeline()
                );
                errors.extend(
                    lifecycle_parity(go_b, rust_b)
                        .into_iter()
                        .map(|e| format!("after broker recovery: {e}")),
                );
                if matches!(go_b.outcome(), Outcome::Ready(_))
                    && matches!(rust_b.outcome(), Outcome::Ready(_))
                {
                    let scenario = format!("outage-{i}");
                    match oracle_kafka(&sc.cfg, batch_records(&scenario, base_ts, 0), &[]).await {
                        Ok(r) => {
                            let got = query_both(
                                &twins[i],
                                Consistency::Offset(r.end_offset() - 1),
                                base_ts,
                            )
                            .await;
                            check_query(
                                &mut errors,
                                "post-recovery QueryStream",
                                &expected_series(&scenario, base_ts, 1),
                                got,
                            );
                        }
                        Err(e) => errors.push(format!("harness: producing after recovery: {e:#}")),
                    }
                }
            }
            let logs = if errors.is_empty() {
                String::new()
            } else {
                twins[i].log_tails(12)
            };
            reports.push(Report {
                name: format!("{} [{}]", sc.name, sc.cfg.address),
                errors,
                go_timeline: go_tl,
                rust_timeline: rust_tl,
                logs,
            });
        }
        blackhole_task.abort();
        drop(twins);
        finish("OBSERV-1374 Kafka unavailable at startup", reports);
        drop(rp);
    });
}
