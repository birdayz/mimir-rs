//! W7 — Kubernetes-faithful differential conformance: `-target=ingester` deployed the way the
//! `mimir-distributed` Helm chart deploys it (zone-aware StatefulSets, memberlist via a headless
//! `dns+` gossip service, ingest storage on Kafka, S3 blocks storage).
//!
//! Every "pod" is a container of the production base image
//! (`gcr.io/distroless/cc-debian13`) with the host-built Go (`target/go-mimir`) or Rust
//! (`target/release/mimir`) binary bind-mounted at `/bin/mimir`. All pods share one compose
//! network and carry the network alias of the chart's headless gossip-ring Service
//! (`publishNotReadyAddresses: true` ⇒ every started pod resolves, ready or not). Redpanda
//! (Kafka) and Silo (the chart's MinIO image) carry the chart's Service names as aliases.
//!
//! The ingester configuration is NOT hand-written: it is loaded at test time from the Helm-rendered
//! ConfigMaps + StatefulSet manifests in
//! `.ref/mimir/operations/helm/tests/test-ingest-storage-values-generated` (release
//! `test-ingest-storage-values` / namespace `citestns` renamed to release `mimir` / namespace
//! `mimir`), i.e. `mimir.yaml` verbatim (including `join_members:
//! dns+…svc.cluster.local.:7946` with the trailing dot), `runtime.yaml` verbatim, and per-zone
//! container `args` / `env` verbatim. Deviations from a real k8s pod (all documented in
//! `pod_service_yaml`): uid/gid are the host user's (bind-mount cleanup) instead of 10001, the
//! `docker stop` grace period is capped at 120s instead of 1200s, and the readiness probe is the
//! test's own `/ready` poller.
//!
//! Upstream behavior under test:
//! - github.com/grafana/dskit/kv/memberlist/memberlist_client.go (`dns+` join, fast-join,
//!   `abort_if_fast_join_fails`, rejoin), dskit/dns (resolver)
//! - github.com/grafana/dskit/ring/lifecycler.go, ring/util.go (`GetInstanceAddr`), tokens file
//! - github.com/grafana/dskit/ring/partition_instance_lifecycler.go
//! - github.com/grafana/dskit/ring/ring_http.go, partition_ring_http.go,
//!   kv/memberlist/http_status_handler.go (JSON views used by every assertion below)
//! - github.com/grafana/mimir/pkg/mimir/modules.go (module graph: sanity-check, usage-stats,
//!   server, memberlist-kv, ingester-ring, ingester-partitions-ring, ingester)
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (`QueryStream`, strong read consistency),
//!   pkg/storage/ingest (partition reader), pkg/ingester/ingester_http.go (`/ingester/flush`)
//!
//! Scenarios (each Go-only vs Rust-only vs mixed Go+Rust; Go defines every expectation):
//! 1. fresh install: all pods started simultaneously (StatefulSet `podManagementPolicy: Parallel`)
//! 2. a single first pod alone (DNS resolves only to itself), then a second pod joins
//! 3. convergence: every pod's memberlist members, `/ingester/ring` and
//!    `/ingester/partition-ring` must show the full membership
//! 4. rolling restart of one pod (SIGTERM, volume/tokens file kept, pod recreated)
//! 5. ingest via Kafka records produced by `target/go-oracle kafka-produce` + `QueryStream`
//!    parity on every pod, `/ingester/flush` block shipping to S3
//! 6. object storage unreachable at startup (`sanity-check` module)
//!
//! Requires Docker + `docker compose`. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly.
//! `MIMIR_CONFORMANCE_RUST_BIN=/path/to/mimir` tests a prebuilt Rust binary (set it to
//! `target/go-mimir` to validate the harness Go-vs-Go). Leftover containers carry the label
//! `org.mimir-rs.conformance=w7-k8s`. Reports are also written to `target/w7-k8s-reports/`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_yaml::Value as Yaml;
use testcontainers::compose::DockerCompose;
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

/// The production base image of the Mimir container (`cmd/mimir/Dockerfile`).
const DISTROLESS_IMAGE: &str = "gcr.io/distroless/cc-debian13@sha256:809d04afa1be033c441f76bb0051878c342fb76d0ea236bc6dc94b655fa0fbd6";
const REDPANDA_IMAGE: &str = "redpandadata/redpanda:v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649";
/// The MinIO-compatible image the chart's `minio` sub-chart renders (`pgsty/silo`).
const SILO_IMAGE: &str = "pgsty/silo:RELEASE.2026-09-03T13-18-01Z@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w7-k8s");

const HELM_TEST: &str = "test-ingest-storage-values";
const HELM_RELEASE: &str = "test-ingest-storage-values";
const HELM_NAMESPACE: &str = "citestns";
const RELEASE: &str = "mimir";
const NAMESPACE: &str = "mimir";

const TENANT: &str = "k8s-tenant";
const SERIES_PER_PARTITION: usize = 6;
const REPLICAS_PER_ZONE: usize = 2;
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
/// "HTTP must answer within ~2s of process start like Go."
const HTTP_UP_BUDGET: Duration = Duration::from_millis(2500);
const DOCKER_STOP_GRACE_SECS: u64 = 120;

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

/// Always rebuilds the Go oracle and the Rust release binary (incremental), so these tests can
/// never silently exercise a stale `target/release/mimir`.
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

/// Distinct free host TCP ports below the ephemeral range (see w6 for the rationale).
fn free_tcp_ports(n: usize) -> Vec<u16> {
    use std::sync::atomic::AtomicU32;
    const LOW: u32 = 20_000;
    const SPAN: u32 = 12_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let offset = (std::process::id() * 397 + 6_007) % SPAN;
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
    assert_eq!(out.len(), n, "no free TCP ports left");
    out
}

/// `2026-11-16T23:19:13.326375061Z` (Docker `State.StartedAt`) → `SystemTime`.
fn parse_rfc3339_utc(s: &str) -> Option<SystemTime> {
    fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let mp = (m + 9) % 12;
        let doy = (153 * mp + 2) / 5 + d - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }
    let (date, rest) = s.trim().split_once('T')?;
    let mut d = date.split('-').map(|p| p.parse::<i64>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let rest = rest.strip_suffix('Z')?;
    let (hms, frac) = rest.split_once('.').unwrap_or((rest, "0"));
    let mut t = hms.split(':').map(|p| p.parse::<i64>().ok());
    let (h, mi, sec) = (t.next()??, t.next()??, t.next()??);
    let mut frac = frac.to_string();
    frac.truncate(9);
    while frac.len() < 9 {
        frac.push('0');
    }
    let nanos: u32 = frac.parse().ok()?;
    let secs = days_from_civil(y, m, day) * 86_400 + h * 3_600 + mi * 60 + sec;
    if secs <= 0 {
        return None;
    }
    Some(UNIX_EPOCH + Duration::new(secs as u64, nanos))
}

// ---------------------------------------------------------------------------------------------
// Helm-rendered deployment (loaded from .ref, never hand-written)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ZoneSts {
    args: Vec<String>,
    env: Vec<(String, String)>,
    termination_grace_secs: u64,
}

#[derive(Debug, Clone)]
struct Helm {
    mimir_yaml: String,
    runtime_yaml: String,
    zones: BTreeMap<String, ZoneSts>,
    http_port: u16,
    grpc_port: u16,
    gossip_alias: String,
    kafka_alias: String,
    kafka_port: u16,
    topic: String,
    s3_alias: String,
    s3_port: u16,
    s3_access_key: String,
    s3_secret_key: String,
    blocks_bucket: String,
    buckets: BTreeSet<String>,
    num_tokens: usize,
}

fn helm_rename(s: &str) -> String {
    s.replace(&format!("{HELM_RELEASE}-mimir"), RELEASE)
        .replace(&format!("{HELM_RELEASE}-"), &format!("{RELEASE}-"))
        .replace(HELM_RELEASE, RELEASE)
        .replace(HELM_NAMESPACE, NAMESPACE)
}

fn yaml_docs(path: &Path) -> Result<Vec<Yaml>> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let text = helm_rename(&text);
    let mut docs = Vec::new();
    for de in serde_yaml::Deserializer::from_str(&text) {
        let v = Yaml::deserialize(de)?;
        if !v.is_null() {
            docs.push(v);
        }
    }
    Ok(docs)
}

fn ystr<'a>(v: &'a Yaml, path: &[&str]) -> Result<&'a str> {
    let mut cur = v;
    for p in path {
        cur = cur.get(*p).ok_or_else(|| anyhow!("missing {path:?}"))?;
    }
    cur.as_str()
        .ok_or_else(|| anyhow!("{path:?} is not a string"))
}

/// Splits `host[.]:port` (dropping a `dns+` prefix and the FQDN trailing dot) for the alias.
fn host_port(addr: &str) -> Result<(String, u16)> {
    let addr = addr.trim_start_matches("dns+");
    let (h, p) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("no port in {addr}"))?;
    Ok((h.trim_end_matches('.').to_string(), p.parse()?))
}

impl Helm {
    fn load() -> Result<Self> {
        let dir = workspace_root()
            .join(".ref/mimir/operations/helm/tests")
            .join(format!("{HELM_TEST}-generated"))
            .join("mimir-distributed/templates");
        let cfg_docs = yaml_docs(&dir.join("mimir-config.yaml"))?;
        let mimir_yaml = ystr(&cfg_docs[0], &["data", "mimir.yaml"])?.to_string();
        let rt_docs = yaml_docs(&dir.join("runtime-configmap.yaml"))?;
        let runtime_yaml = ystr(&rt_docs[0], &["data", "runtime.yaml"])?.to_string();

        let mut zones = BTreeMap::new();
        let (mut http_port, mut grpc_port) = (0u16, 0u16);
        for doc in yaml_docs(&dir.join("ingester/ingester-statefulset.yaml"))? {
            if doc.get("kind").and_then(Yaml::as_str) != Some("StatefulSet") {
                continue;
            }
            let zone = ystr(&doc, &["metadata", "labels", "zone"])?.to_string();
            let pod = &doc["spec"]["template"]["spec"];
            let c = &pod["containers"][0];
            let args = c["args"]
                .as_sequence()
                .ok_or_else(|| anyhow!("no args"))?
                .iter()
                .map(|a| a.as_str().unwrap_or_default().to_string())
                .collect();
            let env = c["env"]
                .as_sequence()
                .map(|s| {
                    s.iter()
                        .filter_map(|e| {
                            Some((
                                e["name"].as_str()?.to_string(),
                                e["value"].as_str()?.to_string(),
                            ))
                        })
                        .collect()
                })
                .unwrap_or_default();
            for p in c["ports"].as_sequence().into_iter().flatten() {
                let port = p["containerPort"].as_u64().unwrap_or_default() as u16;
                match p["name"].as_str() {
                    Some("http-metrics") => http_port = port,
                    Some("grpc") => grpc_port = port,
                    _ => {}
                }
            }
            zones.insert(
                zone,
                ZoneSts {
                    args,
                    env,
                    termination_grace_secs: pod["terminationGracePeriodSeconds"]
                        .as_u64()
                        .unwrap_or(30),
                },
            );
        }
        ensure!(zones.len() == 3, "expected 3 ingester zones, got {zones:?}");

        let cfg: Yaml = serde_yaml::from_str(&mimir_yaml)?;
        let join = cfg["memberlist"]["join_members"][0]
            .as_str()
            .ok_or_else(|| anyhow!("no memberlist.join_members"))?;
        ensure!(
            join.starts_with("dns+") && join.ends_with(".svc.cluster.local.:7946"),
            "unexpected join member {join}"
        );
        let (gossip_alias, _) = host_port(join)?;
        let (kafka_alias, kafka_port) =
            host_port(ystr(&cfg, &["ingest_storage", "kafka", "address"])?)?;
        let topic = ystr(&cfg, &["ingest_storage", "kafka", "topic"])?.to_string();
        let (s3_alias, s3_port) = host_port(ystr(&cfg, &["blocks_storage", "s3", "endpoint"])?)?;
        let mut buckets = BTreeSet::new();
        for section in ["blocks_storage", "alertmanager_storage", "ruler_storage"] {
            if let Ok(b) = ystr(&cfg, &[section, "s3", "bucket_name"]) {
                buckets.insert(b.to_string());
            }
        }
        Ok(Self {
            runtime_yaml,
            zones,
            http_port,
            grpc_port,
            gossip_alias,
            kafka_alias,
            kafka_port,
            topic,
            s3_alias,
            s3_port,
            s3_access_key: ystr(&cfg, &["blocks_storage", "s3", "access_key_id"])?.to_string(),
            s3_secret_key: ystr(&cfg, &["blocks_storage", "s3", "secret_access_key"])?.to_string(),
            blocks_bucket: ystr(&cfg, &["blocks_storage", "s3", "bucket_name"])?.to_string(),
            buckets,
            num_tokens: cfg["ingester"]["ring"]["num_tokens"]
                .as_u64()
                .unwrap_or(128) as usize,
            mimir_yaml,
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Pods, variants
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Side {
    Go,
    Rust,
}

impl Side {
    fn bin(self) -> PathBuf {
        let p = match self {
            Side::Go => go_mimir_bin(),
            Side::Rust => rust_bin_override().unwrap_or_else(rust_mimir_release_bin),
        };
        let p = if p.is_relative() {
            workspace_root().join(p)
        } else {
            p
        };
        fs::canonicalize(&p).unwrap_or(p)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Go,
    Rust,
    /// Alternating Go / Rust so every partition has Go and Rust owners and every zone has both.
    Mixed,
    /// `Mixed` with the sides swapped.
    MixedRev,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Variant::Go => "go",
            Variant::Rust => "rust",
            Variant::Mixed => "mixed",
            Variant::MixedRev => "mixedrev",
        }
    }

    fn side(self, zone_idx: usize, replica: usize) -> Side {
        let even = (zone_idx + replica).is_multiple_of(2);
        match self {
            Variant::Go => Side::Go,
            Variant::Rust => Side::Rust,
            Variant::Mixed if even => Side::Go,
            Variant::Mixed => Side::Rust,
            Variant::MixedRev if even => Side::Rust,
            Variant::MixedRev => Side::Go,
        }
    }
}

#[derive(Debug, Clone)]
struct PodSpec {
    zone: String,
    replica: usize,
    side: Side,
}

impl PodSpec {
    /// StatefulSet pod name = hostname = default `instance_id`.
    fn host(&self) -> String {
        format!("{RELEASE}-ingester-{}-{}", self.zone, self.replica)
    }

    fn service(&self) -> String {
        format!("ingester-{}-{}", self.zone, self.replica)
    }
}

fn topology(helm: &Helm, variant: Variant, replicas: usize) -> Vec<PodSpec> {
    let mut pods = Vec::new();
    for replica in 0..replicas {
        for (zi, zone) in helm.zones.keys().enumerate() {
            pods.push(PodSpec {
                zone: zone.clone(),
                replica,
                side: variant.side(zi, replica),
            });
        }
    }
    pods
}

// ---------------------------------------------------------------------------------------------
// Docker helpers
// ---------------------------------------------------------------------------------------------

async fn docker(args: &[&str]) -> Result<String> {
    let out = tokio::process::Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .context("spawn docker")?;
    ensure!(
        out.status.success(),
        "docker {} failed ({}): {}{}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

#[derive(Debug, Clone)]
struct Inspect {
    id: String,
    running: bool,
    exit_code: i64,
    started_at: Option<SystemTime>,
    ip: String,
}

async fn inspect(container: &str) -> Result<Inspect> {
    let out = docker(&[
        "inspect",
        "-f",
        "{{.Id}}|{{.State.Running}}|{{.State.ExitCode}}|{{.State.StartedAt}}|{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        container,
    ])
    .await?;
    let parts: Vec<&str> = out.trim().split('|').collect();
    ensure!(parts.len() == 5, "unexpected inspect output {out:?}");
    Ok(Inspect {
        id: parts[0].to_string(),
        running: parts[1] == "true",
        exit_code: parts[2].parse().unwrap_or(-1),
        started_at: parse_rfc3339_utc(parts[3]),
        ip: parts[4].to_string(),
    })
}

fn q(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}

// ---------------------------------------------------------------------------------------------
// Cluster = one compose project (Redpanda + Silo + ingester pods on one network)
// ---------------------------------------------------------------------------------------------

struct Pod {
    spec: PodSpec,
    container: String,
    started_at: SystemTime,
    ip: String,
}

struct Cluster {
    project: String,
    compose_file: PathBuf,
    dir: tempfile::TempDir,
    helm: Arc<Helm>,
    kafka_ext_port: u16,
    pods: Vec<Pod>,
    compose: Option<DockerCompose>,
    /// Containers that only hold an IP address on the network (see `start_ip_filler`).
    fillers: Vec<String>,
}

impl Cluster {
    async fn start(
        test: &str,
        variant: Variant,
        helm: Arc<Helm>,
        specs: Vec<PodSpec>,
        restart: &str,
    ) -> Result<Self> {
        let project = format!("w7-{test}-{}-{}", variant.name(), std::process::id());
        let dir = tempfile::Builder::new().prefix("w7-k8s-").tempdir()?;
        let meta = fs::metadata(dir.path())?;
        let user = format!("{}:{}", meta.uid(), meta.gid());
        let kafka_ext_port = free_tcp_ports(1)[0];

        fs::create_dir_all(dir.path().join("config"))?;
        fs::create_dir_all(dir.path().join("runtime"))?;
        fs::write(dir.path().join("config/mimir.yaml"), &helm.mimir_yaml)?;
        fs::write(dir.path().join("runtime/runtime.yaml"), &helm.runtime_yaml)?;
        for b in &helm.buckets {
            fs::create_dir_all(dir.path().join("minio").join(b))?;
        }
        fs::write(
            dir.path().join("redpanda.yaml"),
            format!(
                "redpanda:\n  data_directory: /var/lib/redpanda/data\n  empty_seed_starts_cluster: true\n  seed_servers: []\n  rpc_server: {{address: 0.0.0.0, port: 33145}}\n  advertised_rpc_api: {{address: 127.0.0.1, port: 33145}}\n  admin: [{{address: 0.0.0.0, port: 9644}}]\n  developer_mode: true\n  kafka_api:\n    - {{name: internal, address: 0.0.0.0, port: {kp}}}\n    - {{name: external, address: 0.0.0.0, port: {ext}}}\n  advertised_kafka_api:\n    - {{name: internal, address: {alias}, port: {kp}}}\n    - {{name: external, address: 127.0.0.1, port: {ext}}}\n",
                kp = helm.kafka_port,
                ext = kafka_ext_port,
                alias = helm.kafka_alias,
            ),
        )?;

        let d = dir.path().display();
        let mut y = format!(
            "name: {project}\nnetworks:\n  k8s: {{}}\nservices:\n  kafka:\n    image: {img}\n    container_name: {project}-kafka\n    labels: {{{lk}: {lv}}}\n    entrypoint: [\"/bin/sh\", \"-c\", \"cp /config/redpanda.yaml /etc/redpanda/redpanda.yaml && exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --reserve-memory 0M --default-log-level=warn\"]\n    volumes: [{rp}]\n    ports: [\"127.0.0.1:{ext}:{ext}\"]\n    healthcheck: {{test: [\"CMD\", \"rpk\", \"cluster\", \"health\", \"--exit-when-healthy\"], interval: 1s, timeout: 15s, retries: 120}}\n    networks:\n      k8s:\n        aliases: [{ka}]\n",
            img = REDPANDA_IMAGE,
            lk = CONTAINER_LABEL.0,
            lv = CONTAINER_LABEL.1,
            rp = q(&format!("{d}/redpanda.yaml:/config/redpanda.yaml:ro")),
            ext = kafka_ext_port,
            ka = q(&helm.kafka_alias),
        );
        let _ = write!(
            y,
            "  minio:\n    image: {img}\n    container_name: {project}-minio\n    labels: {{{lk}: {lv}}}\n    user: {user}\n    entrypoint: [\"/bin/sh\", \"-c\", \"exec /usr/bin/docker-entrypoint.sh minio server /export --address :{port}\"]\n    environment: {{MINIO_ROOT_USER: {ak}, MINIO_ROOT_PASSWORD: {sk}, HOME: /tmp}}\n    volumes: [{vol}]\n    healthcheck: {{test: [\"CMD\", \"curl\", \"-sf\", \"http://127.0.0.1:{port}/minio/health/live\"], interval: 1s, timeout: 5s, retries: 120}}\n    networks:\n      k8s:\n        aliases: [{alias}]\n",
            img = SILO_IMAGE,
            lk = CONTAINER_LABEL.0,
            lv = CONTAINER_LABEL.1,
            user = q(&user),
            port = helm.s3_port,
            ak = q(&helm.s3_access_key),
            sk = q(&helm.s3_secret_key),
            vol = q(&format!("{d}/minio:/export")),
            alias = q(&helm.s3_alias),
        );
        let mut pods = Vec::new();
        for spec in specs {
            y.push_str(&pod_service_yaml(
                &project,
                &helm,
                &spec,
                dir.path(),
                &user,
                restart,
            )?);
            pods.push(Pod {
                container: format!("{project}-{}", spec.host()),
                spec,
                started_at: UNIX_EPOCH,
                ip: String::new(),
            });
        }
        let compose_file = dir.path().join("compose.yaml");
        fs::write(&compose_file, y)?;

        // `--wait` of testcontainers' compose client is capped at 60s; Redpanda on a loaded host
        // can need longer, so wait for the healthchecks here instead.
        let mut compose = DockerCompose::with_local_client(&[compose_file.as_path()])
            .with_project_name(project.clone())
            .with_wait(false);
        compose
            .up()
            .await
            .with_context(|| format!("docker compose up (infra) for {project}"))?;
        let cluster = Self {
            project,
            compose_file,
            dir,
            helm,
            kafka_ext_port,
            pods,
            compose: Some(compose),
            fillers: Vec::new(),
        };
        cluster.wait_infra_healthy(Duration::from_secs(240)).await?;
        Ok(cluster)
    }

    /// Starts an idle container on the cluster network; it takes the lowest free IP (the one a
    /// just-stopped pod released), so the recreated pod comes back with a new IP like in k8s.
    async fn start_ip_filler(&mut self) -> Result<()> {
        let name = format!("{}-ipfiller-{}", self.project, self.fillers.len());
        let network = format!("{}_k8s", self.project);
        let label = format!("{}={}", CONTAINER_LABEL.0, CONTAINER_LABEL.1);
        docker(&[
            "run",
            "-d",
            "--name",
            &name,
            "--label",
            &label,
            "--network",
            &network,
            "--entrypoint",
            "/bin/sh",
            SILO_IMAGE,
            "-c",
            "sleep 3600",
        ])
        .await?;
        self.fillers.push(name);
        Ok(())
    }

    async fn wait_infra_healthy(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        for svc in ["kafka", "minio"] {
            let container = format!("{}-{svc}", self.project);
            loop {
                let health = docker(&["inspect", "-f", "{{.State.Health.Status}}", &container])
                    .await
                    .unwrap_or_default();
                if health.trim() == "healthy" {
                    break;
                }
                if Instant::now() >= deadline {
                    let logs = docker(&["logs", "--tail", "20", &container])
                        .await
                        .unwrap_or_default();
                    bail!(
                        "{container} not healthy after {timeout:?} (status {:?}); log tail:\n{logs}",
                        health.trim()
                    );
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(())
    }

    async fn compose_cmd(&self, args: &[&str]) -> Result<String> {
        let file = self.compose_file.display().to_string();
        let mut all = vec![
            "compose",
            "-p",
            self.project.as_str(),
            "-f",
            file.as_str(),
            "--profile",
            "pods",
        ];
        all.extend_from_slice(args);
        docker(&all).await
    }

    fn idx(&self, host: &str) -> usize {
        self.pods
            .iter()
            .position(|p| p.spec.host() == host)
            .unwrap_or_else(|| panic!("no pod {host}"))
    }

    async fn refresh(&mut self, i: usize) -> Result<()> {
        let ins = inspect(&self.pods[i].container).await?;
        self.pods[i].started_at = ins.started_at.unwrap_or_else(SystemTime::now);
        if !ins.ip.is_empty() {
            self.pods[i].ip = ins.ip;
        }
        Ok(())
    }

    /// SIGTERM via `docker stop` (k8s pod deletion); returns (exit code, stop duration).
    async fn stop_pod(&self, i: usize) -> Result<(i64, Duration)> {
        let t = Instant::now();
        let grace = self.helm.zones[&self.pods[i].spec.zone]
            .termination_grace_secs
            .min(DOCKER_STOP_GRACE_SECS)
            .to_string();
        docker(&["stop", "-t", &grace, &self.pods[i].container]).await?;
        let took = t.elapsed();
        Ok((inspect(&self.pods[i].container).await?.exit_code, took))
    }

    fn pod_dir(&self, i: usize) -> PathBuf {
        self.dir.path().join("pods").join(self.pods[i].spec.host())
    }

    fn ip_to_host(&self) -> BTreeMap<String, String> {
        self.pods
            .iter()
            .filter(|p| !p.ip.is_empty())
            .map(|p| (p.ip.clone(), p.spec.host()))
            .collect()
    }

    async fn log_tail(&self, i: usize, lines: usize) -> String {
        let out = tokio::process::Command::new("docker")
            .args([
                "logs",
                "--tail",
                &lines.to_string(),
                &self.pods[i].container,
            ])
            .output()
            .await;
        let text = match out {
            Ok(o) => format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
            Err(e) => format!("docker logs failed: {e}"),
        };
        text.lines()
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

    async fn all_log_tails(&self, idx: &[usize], lines: usize) -> String {
        let mut out = String::new();
        for i in idx {
            let _ = writeln!(
                out,
                "      {} ({:?}) log tail:\n{}",
                self.pods[*i].spec.host(),
                self.pods[*i].spec.side,
                self.log_tail(*i, lines).await
            );
        }
        out
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for f in &self.fillers {
            let _ = Command::new("docker")
                .args(["rm", "-f", f])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        // Remove everything of the project (pods live in a profile `compose down` would skip).
        let _ = Command::new("docker")
            .args([
                "compose",
                "-p",
                &self.project,
                "-f",
                &self.compose_file.display().to_string(),
                "--profile",
                "pods",
                "down",
                "-v",
                "-t",
                "0",
                "--remove-orphans",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = self.compose.take();
    }
}

/// One StatefulSet pod as a compose service.
fn pod_service_yaml(
    project: &str,
    helm: &Helm,
    spec: &PodSpec,
    dir: &Path,
    user: &str,
    restart: &str,
) -> Result<String> {
    let sts = &helm.zones[&spec.zone];
    let host = spec.host();
    let pod_dir = dir.join("pods").join(&host);
    // PVC `storage` -> /data (survives pod recreation), emptyDir `active-queries`.
    fs::create_dir_all(pod_dir.join("data"))?;
    fs::create_dir_all(pod_dir.join("active-queries"))?;
    let args: Vec<String> = sts.args.iter().map(|a| q(a)).collect();
    let env: Vec<String> = sts
        .env
        .iter()
        .map(|(k, v)| format!("{k}: {}", q(v)))
        .collect();
    let vols = [
        format!("{}:/bin/mimir:ro", spec.side.bin().display()),
        format!("{}/config:/etc/mimir:ro", dir.display()),
        format!("{}/runtime:/var/mimir:ro", dir.display()),
        format!("{}/data:/data", pod_dir.display()),
        format!("{}/active-queries:/active-query-tracker", pod_dir.display()),
    ];
    Ok(format!(
        "  {svc}:\n    image: {img}\n    profiles: [pods]\n    container_name: {project}-{host}\n    hostname: {host}\n    labels: {{{lk}: {lv}}}\n    # securityContext: runAsNonRoot (host uid instead of 10001 so the test can clean up),\n    # readOnlyRootFilesystem, allowPrivilegeEscalation: false, capabilities.drop: [ALL].\n    user: {user}\n    read_only: true\n    cap_drop: [ALL]\n    security_opt: [\"no-new-privileges:true\"]\n    entrypoint: [\"/bin/mimir\"]\n    command: [{args}]\n    environment: {{{env}}}\n    stop_grace_period: {grace}s\n    # Pod restartPolicy: Always (crash -> kubelet restarts the container) where requested.\n    restart: {restart}\n    volumes: [{vols}]\n    networks:\n      k8s:\n        aliases: [{alias}]\n",
        svc = spec.service(),
        img = DISTROLESS_IMAGE,
        lk = CONTAINER_LABEL.0,
        lv = CONTAINER_LABEL.1,
        user = q(user),
        args = args.join(", "),
        env = env.join(", "),
        grace = sts.termination_grace_secs,
        restart = q(restart),
        vols = vols.iter().map(|v| q(v)).collect::<Vec<_>>().join(", "),
        alias = q(&helm.gossip_alias),
    ))
}

// ---------------------------------------------------------------------------------------------
// HTTP lifecycle observation
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Probe {
    NoAnswer,
    Exited(i64),
    Answered { ready: u16, body: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Ready(Duration),
    Exited(Duration, i64),
    NotReadyAlive,
}

impl Outcome {
    fn kind(&self) -> String {
        match self {
            Outcome::Ready(_) => "ready".into(),
            Outcome::Exited(_, c) => format!("exited({c})"),
            Outcome::NotReadyAlive => "not-ready".into(),
        }
    }
}

/// Samples timed from the container's `State.StartedAt` (= process start).
#[derive(Debug, Default, Clone)]
struct Lifecycle {
    samples: Vec<(Duration, Probe)>,
}

impl Lifecycle {
    /// Observed NOT answering HTTP at or after `HTTP_UP_BUDGET` (before its first answer). A first
    /// sample that already answers proves nothing about lateness, so only this is asserted.
    fn http_late(&self) -> bool {
        let first = self.first_answer();
        self.samples.iter().any(|(t, p)| {
            *t >= HTTP_UP_BUDGET && matches!(p, Probe::NoAnswer) && first.is_none_or(|f| *t < f)
        })
    }

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
                Probe::Exited(c) => format!("exited (code {c})"),
                Probe::Answered { ready, body } => format!(
                    "/ready {ready} {:?}",
                    body.chars().take(100).collect::<String>()
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

/// Starts the pods (`docker compose up -d --no-deps`, ONE call = parallel start) while already
/// polling them, so the first HTTP answer is timed from each container's real process start
/// rather than from whenever `docker compose up` returned for the last of them.
async fn start_and_observe(
    cluster: &mut Cluster,
    idx: &[usize],
    recreate: bool,
    window: Duration,
) -> Result<BTreeMap<usize, Lifecycle>> {
    let mut old_ids = BTreeMap::new();
    for i in idx {
        if let Ok(ins) = inspect(&cluster.pods[*i].container).await {
            old_ids.insert(*i, ins.id);
        }
    }
    let services: Vec<String> = idx
        .iter()
        .map(|i| cluster.pods[*i].spec.service())
        .collect();
    let mut args = vec!["up", "-d", "--no-deps"];
    if recreate {
        args.push("--force-recreate");
    }
    args.extend(services.iter().map(String::as_str));
    let c: &Cluster = cluster;
    let (up, lcs) = tokio::join!(
        c.compose_cmd(&args),
        observe_inner(c, idx, Some(&old_ids), window, true)
    );
    up?;
    for i in idx {
        cluster.refresh(*i).await?;
    }
    Ok(lcs)
}

async fn probe(http: &reqwest::Client, pod: &Pod, port: u16) -> (Duration, Probe) {
    let t = SystemTime::now()
        .duration_since(pod.started_at)
        .unwrap_or_default();
    match http
        .get(format!("http://{}:{port}/ready", pod.ip))
        .send()
        .await
    {
        Ok(resp) => {
            let ready = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            (t, Probe::Answered { ready, body })
        }
        Err(_) => match inspect(&pod.container).await {
            Ok(ins) if !ins.running => (t, Probe::Exited(ins.exit_code)),
            _ => (t, Probe::NoAnswer),
        },
    }
}

fn settled(p: &Probe) -> bool {
    matches!(p, Probe::Exited(_) | Probe::Answered { ready: 200, .. })
}

/// Samples the given pods every `SAMPLE_INTERVAL` until all are settled (if requested) or
/// `window` elapsed.
/// With `starting`, pods are first discovered (container id differs from the given old id and
/// it has a start time + IP) before being probed; until then nothing is sampled.
async fn observe_inner(
    cluster: &Cluster,
    idx: &[usize],
    starting: Option<&BTreeMap<usize, String>>,
    window: Duration,
    stop_when_settled: bool,
) -> BTreeMap<usize, Lifecycle> {
    let http = http_client();
    let start = Instant::now();
    let mut out: BTreeMap<usize, Lifecycle> =
        idx.iter().map(|i| (*i, Lifecycle::default())).collect();
    let mut known: BTreeMap<usize, Pod> = BTreeMap::new();
    if starting.is_none() {
        for i in idx {
            let p = &cluster.pods[*i];
            known.insert(
                *i,
                Pod {
                    spec: p.spec.clone(),
                    container: p.container.clone(),
                    started_at: p.started_at,
                    ip: p.ip.clone(),
                },
            );
        }
    }
    loop {
        if let Some(old) = starting {
            let pending: Vec<usize> = idx
                .iter()
                .copied()
                .filter(|i| !known.contains_key(i))
                .collect();
            for i in pending.iter() {
                let p = &cluster.pods[*i];
                if let Ok(ins) = inspect(&p.container).await {
                    let fresh = old.get(i) != Some(&ins.id);
                    if let (true, Some(started_at)) = (fresh, ins.started_at) {
                        if !ins.ip.is_empty() || !ins.running {
                            known.insert(
                                *i,
                                Pod {
                                    spec: p.spec.clone(),
                                    container: p.container.clone(),
                                    started_at,
                                    ip: ins.ip,
                                },
                            );
                        }
                    }
                }
            }
        }
        let probes = futures::future::join_all(known.iter().map(|(i, p)| {
            let http = &http;
            async move { (*i, probe(http, p, cluster.helm.http_port).await) }
        }))
        .await;
        let done = known.len() == idx.len() && probes.iter().all(|(_, (_, p))| settled(p));
        for (i, p) in probes {
            out.get_mut(&i).unwrap().samples.push(p);
        }
        if (stop_when_settled && done) || start.elapsed() >= window {
            return out;
        }
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
}

// ---------------------------------------------------------------------------------------------
// Cluster views: /memberlist, /ingester/ring, /ingester/partition-ring (JSON, Accept header)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct RingNorm {
    state: String,
    /// `<pod hostname>:<port>` when the registered address is a pod IP, raw otherwise.
    addr: String,
    zone: String,
    tokens: usize,
    read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PartNorm {
    state: i64,
    owners: BTreeSet<String>,
    tokens: usize,
    corrupted: bool,
    locked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct View {
    members: BTreeSet<String>,
    /// Raw memberlist entries (a stale entry of a restarted pod shows up here, not in `members`).
    member_entries: usize,
    ring: BTreeMap<String, RingNorm>,
    partitions: BTreeMap<i64, PartNorm>,
}

#[derive(Debug, Clone, Default)]
struct RawView {
    view: View,
    ring_tokens: BTreeMap<String, Vec<u32>>,
    memberlist_keys: Vec<String>,
}

async fn get_json(http: &reqwest::Client, url: &str) -> Result<serde_json::Value> {
    let resp = http
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    let body = resp.text().await?;
    ensure!(status.is_success(), "GET {url}: {status} {body:.200}");
    serde_json::from_str(&body).with_context(|| format!("GET {url}: not JSON: {body:.200}"))
}

/// memberlist node name = `<hostname>-<8 hex chars>` (dskit `generateRandomSuffix`).
fn member_host(name: &str) -> String {
    match name.rsplit_once('-') {
        Some((h, suffix)) if suffix.len() == 8 && suffix.chars().all(|c| c.is_ascii_hexdigit()) => {
            h.to_string()
        }
        _ => name.to_string(),
    }
}

async fn fetch_view(
    http: &reqwest::Client,
    ip: &str,
    port: u16,
    ip_to_host: &BTreeMap<String, String>,
) -> Result<RawView> {
    let base = format!("http://{ip}:{port}");
    let (ml_url, ring_url, pring_url) = (
        format!("{base}/memberlist"),
        format!("{base}/ingester/ring"),
        format!("{base}/ingester/partition-ring"),
    );
    let (ml, ring, pring) = tokio::join!(
        get_json(http, &ml_url),
        get_json(http, &ring_url),
        get_json(http, &pring_url),
    );
    let (ml, ring, pring) = (ml?, ring?, pring?);
    let mut out = RawView::default();
    if let Some(obj) = ml.as_object() {
        out.memberlist_keys = obj.keys().cloned().collect();
    }
    // Go schema: StatusPageData.SortedMembers[].Name. Anything else is reported via the key diff;
    // to still check convergence we also accept a `members[].name` list.
    let members = ml
        .get("SortedMembers")
        .and_then(|m| m.as_array())
        .map(|a| (a, "Name"))
        .or_else(|| {
            ml.get("members")
                .and_then(|m| m.as_array())
                .map(|a| (a, "name"))
        });
    if let Some((arr, key)) = members {
        for m in arr {
            if let Some(n) = m.get(key).and_then(|n| n.as_str()) {
                out.view.members.insert(member_host(n));
                out.view.member_entries += 1;
            }
        }
    }
    for s in ring["shards"].as_array().into_iter().flatten() {
        let id = s["id"].as_str().unwrap_or_default().to_string();
        let addr = s["address"].as_str().unwrap_or_default();
        let addr = match addr.rsplit_once(':') {
            Some((h, p)) if ip_to_host.contains_key(h) => format!("{}:{p}", ip_to_host[h]),
            _ => addr.to_string(),
        };
        let tokens: Vec<u32> = s["tokens"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| t.as_u64().map(|t| t as u32))
            .collect();
        out.view.ring.insert(
            id.clone(),
            RingNorm {
                state: s["state"].as_str().unwrap_or_default().to_string(),
                addr,
                zone: s["zone"].as_str().unwrap_or_default().to_string(),
                tokens: tokens.len(),
                read_only: s["read_only"].as_bool().unwrap_or(false),
            },
        );
        out.ring_tokens.insert(id, tokens);
    }
    for p in pring["partitions"].as_array().into_iter().flatten() {
        out.view.partitions.insert(
            p["id"].as_i64().unwrap_or(-1),
            PartNorm {
                state: p["state"].as_i64().unwrap_or(-1),
                owners: p["owner_ids"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|o| o.as_str().map(str::to_string))
                    .collect(),
                tokens: p["tokens"].as_array().map(Vec::len).unwrap_or(0),
                corrupted: p["corrupted"].as_bool().unwrap_or(false),
                locked: p["state_change_locked"].as_bool().unwrap_or(false),
            },
        );
    }
    Ok(out)
}

/// Full membership of the running pods (what Go converges to; partition tokens excluded since
/// they are compared differentially).
fn expected_view(helm: &Helm, running: &[&PodSpec]) -> View {
    let mut v = View {
        member_entries: running.len(),
        ..View::default()
    };
    for p in running {
        v.members.insert(p.host());
        v.ring.insert(
            p.host(),
            RingNorm {
                state: "ACTIVE".into(),
                addr: format!("{}:{}", p.host(), helm.grpc_port),
                zone: p.zone.clone(),
                tokens: helm.num_tokens,
                read_only: false,
            },
        );
        let part = v.partitions.entry(p.replica as i64).or_insert(PartNorm {
            state: 2, // PartitionActive
            owners: BTreeSet::new(),
            tokens: 0,
            corrupted: false,
            locked: false,
        });
        part.owners.insert(p.host());
    }
    v
}

fn matches_expected(actual: &View, expected: &View) -> bool {
    let mut a = actual.clone();
    for p in a.partitions.values_mut() {
        p.tokens = 0;
    }
    &a == expected
}

fn describe_view_diff(actual: &View, expected: &View) -> String {
    let mut d = Vec::new();
    if actual.members != expected.members {
        d.push(format!(
            "members {:?} (want {:?})",
            actual.members, expected.members
        ));
    }
    if actual.member_entries != expected.member_entries {
        d.push(format!(
            "{} memberlist entries (want {}, stale entry of a restarted/crashed pod)",
            actual.member_entries, expected.member_entries
        ));
    }
    for (id, want) in &expected.ring {
        match actual.ring.get(id) {
            None => d.push(format!("ring: {id} missing")),
            Some(got) if got != want => d.push(format!("ring {id}: {got:?} (want {want:?})")),
            _ => {}
        }
    }
    for id in actual
        .ring
        .keys()
        .filter(|k| !expected.ring.contains_key(*k))
    {
        d.push(format!("ring: unexpected {id} {:?}", actual.ring[id]));
    }
    for (id, want) in &expected.partitions {
        match actual.partitions.get(id) {
            None => d.push(format!("partition {id} missing")),
            Some(got)
                if got.state != want.state
                    || got.owners != want.owners
                    || got.corrupted
                    || got.locked =>
            {
                d.push(format!(
                    "partition {id}: {got:?} (want state {} owners {:?})",
                    want.state, want.owners
                ))
            }
            _ => {}
        }
    }
    for id in actual
        .partitions
        .keys()
        .filter(|k| !expected.partitions.contains_key(*k))
    {
        d.push(format!(
            "unexpected partition {id} {:?}",
            actual.partitions[id]
        ));
    }
    d.join("; ")
}

struct Convergence {
    after: Option<Duration>,
    views: BTreeMap<String, Result<RawView, String>>,
}

/// Polls every given pod's view until all of them equal the full membership of `running`.
async fn converge(
    cluster: &Cluster,
    observers: &[usize],
    running: &[usize],
    timeout: Duration,
) -> Convergence {
    let http = http_client();
    let start = Instant::now();
    let running_specs: Vec<&PodSpec> = running.iter().map(|i| &cluster.pods[*i].spec).collect();
    let want = expected_view(&cluster.helm, &running_specs);
    let ip_to_host = cluster.ip_to_host();
    loop {
        let views = futures::future::join_all(observers.iter().map(|i| {
            let pod = &cluster.pods[*i];
            let http = &http;
            let ip_to_host = &ip_to_host;
            async move {
                (
                    pod.spec.host(),
                    fetch_view(http, &pod.ip, cluster.helm.http_port, ip_to_host)
                        .await
                        .map_err(|e| format!("{e:#}")),
                )
            }
        }))
        .await;
        let views: BTreeMap<String, Result<RawView, String>> = views.into_iter().collect();
        let ok = views
            .values()
            .all(|v| matches!(v, Ok(rv) if matches_expected(&rv.view, &want)));
        if ok {
            return Convergence {
                after: Some(start.elapsed()),
                views,
            };
        }
        if start.elapsed() >= timeout {
            return Convergence { after: None, views };
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn convergence_errors(cluster: &Cluster, running: &[usize], c: &Convergence) -> Vec<String> {
    if c.after.is_some() {
        return Vec::new();
    }
    let specs: Vec<&PodSpec> = running.iter().map(|i| &cluster.pods[*i].spec).collect();
    let want = expected_view(&cluster.helm, &specs);
    // Group identical differences: diff component -> observers that see it.
    let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (host, v) in &c.views {
        let who = format!("{host} ({:?})", cluster.pods[cluster.idx(host)].spec.side);
        match v {
            Ok(rv) if matches_expected(&rv.view, &want) => {}
            Ok(rv) => {
                for part in describe_view_diff(&rv.view, &want).split("; ") {
                    grouped
                        .entry(part.to_string())
                        .or_default()
                        .insert(who.clone());
                }
            }
            Err(e) => {
                grouped.entry(e.clone()).or_default().insert(who);
            }
        }
    }
    grouped
        .into_iter()
        .map(|(diff, who)| {
            let who: Vec<String> = who.into_iter().collect();
            let who = if who.len() == c.views.len() {
                "all pods".to_string()
            } else {
                who.join(", ")
            };
            format!("view never converged: {diff} [seen by {who}]")
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Kafka ingest (records produced by upstream ingest.NewKafkaWriterClient via the Go oracle)
// ---------------------------------------------------------------------------------------------

type Series = (Vec<(String, String)>, Vec<(i64, u64)>);

#[derive(Serialize)]
struct KafkaProduceReq {
    kafka_yaml: String,
    records: Vec<KfakeRecordInput>,
    consumer_groups: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KafkaProduceResp {
    end_offsets: BTreeMap<String, i64>,
}

fn series_labels(partition: usize, i: usize) -> Vec<(String, String)> {
    vec![
        ("__name__".into(), "w7_k8s_requests_total".into()),
        ("partition".into(), partition.to_string()),
        ("series".into(), format!("s{i:02}")),
    ]
}

fn sample_at(base_ts: i64, batch: usize, partition: usize, i: usize) -> (i64, f64) {
    (
        base_ts + batch as i64 * 15_000,
        (batch * 1000 + partition * 100 + i) as f64 + 0.5,
    )
}

fn batch_records(partitions: &[usize], base_ts: i64, batch: usize) -> Vec<KfakeRecordInput> {
    partitions
        .iter()
        .map(|&p| KfakeRecordInput {
            partition: p as i32,
            tenant: TENANT.to_string(),
            version: 2,
            compression: None,
            write_request: WriteRequestJson {
                mode: "rw2".to_string(),
                source: "API".to_string(),
                timeseries: (0..SERIES_PER_PARTITION)
                    .map(|i| {
                        let (t, v) = sample_at(base_ts, batch, p, i);
                        TimeSeriesJson {
                            labels: series_labels(p, i)
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

fn expected_series(partition: usize, base_ts: i64, batches: usize) -> Vec<Series> {
    let mut out: Vec<Series> = (0..SERIES_PER_PARTITION)
        .map(|i| {
            let samples = (0..batches)
                .map(|b| {
                    let (t, v) = sample_at(base_ts, b, partition, i);
                    (t, v.to_bits())
                })
                .collect();
            (series_labels(partition, i), samples)
        })
        .collect();
    out.sort();
    out
}

async fn produce(
    cluster: &Cluster,
    records: Vec<KfakeRecordInput>,
) -> Result<BTreeMap<usize, i64>> {
    let req = KafkaProduceReq {
        kafka_yaml: format!(
            "address: 127.0.0.1:{}\ntopic: {}\n",
            cluster.kafka_ext_port, cluster.helm.topic
        ),
        records,
        consumer_groups: Vec::new(),
    };
    let resp: KafkaProduceResp =
        tokio::task::spawn_blocking(move || invoke_oracle("kafka-produce", &req)).await??;
    Ok(resp
        .end_offsets
        .into_iter()
        .filter_map(|(p, o)| Some((p.parse().ok()?, o)))
        .collect())
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

/// `QueryStream` with strong read consistency at `offset` of the pod's own partition.
async fn query_stream(
    ip: &str,
    port: u16,
    partition: usize,
    offset: i64,
    base_ts: i64,
) -> Result<Vec<Series>> {
    let ch = Channel::from_shared(format!("http://{ip}:{port}"))?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .context("connect gRPC")?;
    let mut client = IngesterClient::new(ch);
    let mut req = tonic::Request::new(ingesterpb::QueryRequest {
        start_timestamp_ms: base_ts - 3_600_000,
        end_timestamp_ms: now_ms() + 3_600_000,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: "w7_k8s_requests_total".to_string(),
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
    md.insert(
        "__consistency_offsets__",
        MetadataValue::try_from(format!("v1={partition}:{offset}"))?,
    );
    let mut stream = client
        .query_stream(req)
        .await
        .map_err(|s| anyhow!("QueryStream failed: {s}"))?
        .into_inner();
    let mut labels: Vec<Vec<(String, String)>> = Vec::new();
    let mut samples: BTreeMap<u64, Vec<(i64, u64)>> = BTreeMap::new();
    while let Some(item) = stream.next().await {
        let msg = item.map_err(|s| anyhow!("QueryStream stream error: {s}"))?;
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
                        .map_err(|e| anyhow!("decode chunk: {e:?}"))?;
                for s in decode_chunk_samples(&chunk).map_err(|e| anyhow!("{e:?}"))? {
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
        .map(|(idx, mut l)| {
            l.sort();
            let mut v = samples.remove(&(idx as u64)).unwrap_or_default();
            v.sort();
            v.dedup();
            (l, v)
        })
        .collect();
    out.sort();
    Ok(out)
}

fn describe_series_diff(want: &[Series], got: &[Series]) -> String {
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

/// Queries every given pod for its own partition; returns host -> error (None = correct data).
async fn verify_pods_data(
    cluster: &Cluster,
    idx: &[usize],
    offsets: &BTreeMap<usize, i64>,
    base_ts: i64,
    batches: usize,
) -> BTreeMap<String, Option<String>> {
    let res = futures::future::join_all(idx.iter().map(|i| {
        let pod = &cluster.pods[*i];
        let p = pod.spec.replica;
        let off = offsets.get(&p).copied().unwrap_or(0) - 1;
        async move {
            let want = expected_series(p, base_ts, batches);
            let got = query_stream(&pod.ip, cluster.helm.grpc_port, p, off, base_ts).await;
            let err = match got {
                Ok(g) if g == want => None,
                Ok(g) => Some(describe_series_diff(&want, &g)),
                Err(e) => Some(format!("{e:#}")),
            };
            (pod.spec.host(), err)
        }
    }))
    .await;
    res.into_iter().collect()
}

// ---------------------------------------------------------------------------------------------
// Object storage (Silo export dir on the host: buckets are directories, objects directories)
// ---------------------------------------------------------------------------------------------

/// ULID block directories of the tenant in the blocks bucket that contain `meta.json`, `index`
/// and `chunks/000001`.
fn shipped_blocks(cluster: &Cluster) -> Vec<String> {
    let tenant_dir = cluster
        .dir
        .path()
        .join("minio")
        .join(&cluster.helm.blocks_bucket)
        .join(TENANT);
    let mut out = Vec::new();
    for e in fs::read_dir(&tenant_dir).into_iter().flatten().flatten() {
        let p = e.path();
        let complete = ["meta.json", "index", "chunks/000001"]
            .iter()
            .all(|f| p.join(f).exists());
        if complete {
            out.push(e.file_name().to_string_lossy().into_owned());
        }
    }
    out.sort();
    out
}

fn bucket_objects(cluster: &Cluster, prefix: &str) -> bool {
    cluster
        .dir
        .path()
        .join("minio")
        .join(&cluster.helm.blocks_bucket)
        .join(prefix)
        .exists()
}

// ---------------------------------------------------------------------------------------------
// Admin HTTP surface (pkg/api/api.go routes an ingester serves; read-only GETs) and /metrics
// families referenced by the mimir-mixin dashboards / alerts / recording rules
// ---------------------------------------------------------------------------------------------

/// GET-only (no state change) admin paths used by the Helm chart, jsonnet, rollout-operator
/// (`grafana.com/prepare-downscale-http-path`) and operators.
const ADMIN_GETS: &[&str] = &[
    "/",
    "/ready",
    "/metrics",
    "/config",
    "/config?mode=diff",
    "/config?mode=defaults",
    "/runtime_config",
    "/runtime_config?mode=diff",
    "/services",
    "/memberlist",
    "/ingester/ring",
    "/ingester/partition-ring",
    "/ingester/tenants",
    "/ingester/tsdb/k8s-tenant",
    "/ingester/prepare-shutdown",
    "/ingester/prepare-partition-downscale",
    "/ingester/prepare-instance-ring-downscale",
    "/ingester/unregister-on-shutdown",
    "/api/v1/status/buildinfo",
    "/api/v1/status/config",
    "/api/v1/status/flags",
    "/debug/pprof/",
    "/debug/pprof/heap",
    "/debug/pprof/goroutine",
];

/// path -> "<status> <mime essence>" (+ the body for the small rollout-operator endpoints).
async fn admin_surface(ip: &str, port: u16) -> BTreeMap<String, String> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut out = BTreeMap::new();
    for path in ADMIN_GETS {
        let v = match http.get(format!("http://{ip}:{port}{path}")).send().await {
            Ok(r) => {
                let status = r.status().as_u16();
                let mime = r
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-")
                    .split(';')
                    .next()
                    .unwrap_or("-")
                    .trim()
                    .to_string();
                let body = r.text().await.unwrap_or_default();
                if path.contains("prepare-") || path.contains("unregister-on-shutdown") {
                    format!("{status} {mime} {:?}", body.trim())
                } else {
                    format!("{status} {mime}")
                }
            }
            Err(e) => format!("error {e}"),
        };
        out.insert((*path).to_string(), v);
    }
    out
}

/// Identifiers in the compiled mixin (dashboards, alerts, rules): any token that is also a series
/// name exposed by a Go ingester is a metric the mixin depends on.
fn mixin_tokens() -> &'static BTreeSet<String> {
    static T: OnceLock<BTreeSet<String>> = OnceLock::new();
    T.get_or_init(|| {
        let dir = workspace_root().join(".ref/mimir/operations/mimir-mixin-compiled");
        let mut files = vec![dir.join("alerts.yaml"), dir.join("rules.yaml")];
        files.extend(
            fs::read_dir(dir.join("dashboards"))
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| e.path()),
        );
        let mut out = BTreeSet::new();
        for f in files {
            let text = fs::read_to_string(&f).unwrap_or_default();
            for tok in text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':')) {
                if tok.len() > 3 && tok.contains('_') {
                    out.insert(tok.to_string());
                }
            }
        }
        out
    })
}

/// Series name -> union of label names, from the Prometheus text exposition.
async fn scrape_series(ip: &str, port: u16) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let body = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .get(format!("http://{ip}:{port}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for line in body.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let name_end = line.find(['{', ' ']).unwrap_or(line.len());
        let labels = out.entry(line[..name_end].to_string()).or_default();
        if line[name_end..].starts_with('{') {
            // k="v" pairs; values may contain escaped quotes, commas and braces.
            let mut rest = &line[name_end + 1..];
            while let Some(eq) = rest.find('=') {
                let key = rest[..eq].trim_start_matches(',').trim();
                if key.starts_with('}') {
                    break;
                }
                labels.insert(key.to_string());
                let mut chars = rest[eq + 1..].char_indices();
                chars.next(); // opening quote
                let mut end = None;
                let mut escaped = false;
                for (j, c) in chars {
                    match (escaped, c) {
                        (true, _) => escaped = false,
                        (false, '\\') => escaped = true,
                        (false, '"') => {
                            end = Some(eq + 1 + j + 1);
                            break;
                        }
                        _ => {}
                    }
                }
                match end {
                    Some(e) => rest = &rest[e..],
                    None => break,
                }
                if rest.starts_with('}') {
                    break;
                }
            }
        }
    }
    Ok(out)
}

/// Mixin-referenced series of `reference` that `other` lacks or exposes with other label names.
fn mixin_metric_diffs(
    reference: &BTreeMap<String, BTreeSet<String>>,
    other: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    let mixin = mixin_tokens();
    let mut missing = Vec::new();
    let mut labels = Vec::new();
    // `go_*` are Go-runtime metrics (GC CPU classes, memstats, threads) without a Rust
    // equivalent; they are listed in the informational family diff only.
    for (name, want) in reference
        .iter()
        .filter(|(n, _)| mixin.contains(*n) && !n.starts_with("go_"))
    {
        match other.get(name) {
            None => missing.push(name.clone()),
            Some(got) if got != want => labels.push(format!(
                "{name}: missing labels {:?}, extra labels {:?}",
                want.difference(got).collect::<Vec<_>>(),
                got.difference(want).collect::<Vec<_>>()
            )),
            _ => {}
        }
    }
    let mut out = Vec::new();
    if !missing.is_empty() {
        out.push(format!(
            "{} mixin-referenced series missing: {missing:?}",
            missing.len()
        ));
    }
    out.extend(labels);
    out
}

fn union_series(
    per_pod: &BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    sides: &BTreeMap<String, Side>,
    side: Side,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (_, m) in per_pod.iter().filter(|(h, _)| sides.get(*h) == Some(&side)) {
        for (n, l) in m {
            out.entry(n.clone()).or_default().extend(l.iter().cloned());
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Rolling restart observation
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct RestartObs {
    exit_code: Option<i64>,
    stop_took: Option<Duration>,
    /// observer host -> deduplicated ring states of the restarted instance ("absent" when not in
    /// the ring, "?" on observer errors are skipped).
    ring_states: BTreeMap<String, Vec<String>>,
    /// observer host -> deduplicated "owner"/"not-owner" of the restarted instance's partition.
    partition_ownership: BTreeMap<String, Vec<String>>,
    tokens_before: Vec<u32>,
    tokens_after: Vec<u32>,
    tokens_file_before: Option<Vec<u32>>,
    tokens_file_after: Option<Vec<u32>>,
    lifecycle: Lifecycle,
}

impl RestartObs {
    /// States seen by observers with transient PENDING/JOINING removed (polling may miss them).
    fn essential(&self) -> BTreeSet<Vec<String>> {
        self.ring_states
            .values()
            .map(|seq| {
                let mut v: Vec<String> = seq
                    .iter()
                    .filter(|s| *s != "PENDING" && *s != "JOINING")
                    .cloned()
                    .collect();
                v.dedup();
                v
            })
            .collect()
    }
}

fn read_tokens_file(path: &Path) -> Option<Vec<u32>> {
    let v: serde_json::Value = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
    let mut t: Vec<u32> = v["tokens"]
        .as_array()?
        .iter()
        .filter_map(|t| t.as_u64().map(|t| t as u32))
        .collect();
    t.sort_unstable();
    Some(t)
}

/// observer host -> deduplicated observed values.
type Sequences = BTreeMap<String, Vec<String>>;

/// Background poller of the observers' `/ingester/ring` + `/ingester/partition-ring`.
fn spawn_ring_watch(
    observers: Vec<(String, String)>,
    port: u16,
    target: String,
    partition: i64,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<(Sequences, Sequences)> {
    tokio::spawn(async move {
        let http = http_client();
        let mut states: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut owners: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let push = |m: &mut BTreeMap<String, Vec<String>>, k: &str, v: String| {
            let seq = m.entry(k.to_string()).or_default();
            if seq.last() != Some(&v) {
                seq.push(v);
            }
        };
        while !stop.load(Ordering::Relaxed) {
            for (host, ip) in &observers {
                let base = format!("http://{ip}:{port}");
                if let Ok(ring) = get_json(&http, &format!("{base}/ingester/ring")).await {
                    let st = ring["shards"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|s| s["id"].as_str() == Some(target.as_str()))
                        .map(|s| s["state"].as_str().unwrap_or("?").to_string())
                        .unwrap_or_else(|| "absent".to_string());
                    push(&mut states, host, st);
                }
                if let Ok(pr) = get_json(&http, &format!("{base}/ingester/partition-ring")).await {
                    let owned = pr["partitions"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|p| p["id"].as_i64() == Some(partition))
                        .map(|p| {
                            let owner = p["owner_ids"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .any(|o| o.as_str() == Some(target.as_str()));
                            format!(
                                "{}/state={}",
                                if owner { "owner" } else { "not-owner" },
                                p["state"]
                            )
                        })
                        .unwrap_or_else(|| "no-partition".to_string());
                    push(&mut owners, host, owned);
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        (states, owners)
    })
}

// ---------------------------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------------------------

struct Report {
    name: String,
    errors: Vec<String>,
    details: String,
}

/// Divergences already reported and tracked (hunt-k8s findings, PORT_STATUS.md). A Rust error
/// matching one of these substrings is printed as `KNOWN <id>` and does not fail the test unless
/// `MIMIR_W7_STRICT=1`. Every other difference fails. Harness errors (`harness:`) never match.
/// Remove an entry as soon as the divergence is fixed.
const KNOWN_DIVERGENCES: &[(&str, &str)] = &[
    // K8S-2: ring / partition-ring / lifecycler metrics registered without the `cortex_` prefix
    // (`ring_members`, `partition_ring_partitions`, `lifecycler_read_only`, ...).
    ("K8S-2", "mixin-referenced series missing"),
    ("K8S-2", "missing labels"),
    // K8S-4: no `sanity-check` module (Rust becomes ready with object storage unreachable).
    ("K8S-4", "outcome ready, Go pod exited(1)"),
    // K8S-6: /memberlist JSON is not dskit's StatusPageData.
    (
        "K8S-6",
        "/memberlist JSON (Accept: application/json) top-level keys differ",
    ),
    // K8S-7: Go runtime profiles (/debug/pprof/heap, goroutine) not served.
    ("K8S-7", "GET /debug/pprof/"),
];

fn known_divergence(err: &str) -> Option<&'static str> {
    if err.starts_with("harness:") {
        return None;
    }
    KNOWN_DIVERGENCES
        .iter()
        .find(|(_, pat)| err.contains(pat))
        .map(|(id, _)| *id)
}

fn finish(test: &str, title: &str, reports: Vec<Report>) {
    let strict = std::env::var("MIMIR_W7_STRICT").as_deref() == Ok("1");
    let is_failure = |e: &String| strict || known_divergence(e).is_none();
    let failed = reports
        .iter()
        .filter(|r| r.errors.iter().any(is_failure))
        .count();
    let mut out = format!(
        "\n===== {title}: {failed}/{} variants failed{} =====\n",
        reports.len(),
        if strict { " (MIMIR_W7_STRICT=1)" } else { "" }
    );
    for r in &reports {
        let status = if r.errors.iter().any(is_failure) {
            "FAIL"
        } else if r.errors.is_empty() {
            "ok  "
        } else {
            "ok* "
        };
        let _ = writeln!(out, "[{status}] {}", r.name);
        for e in &r.errors {
            match known_divergence(e) {
                Some(id) => {
                    let _ = writeln!(out, "    - KNOWN {id}: {e}");
                }
                None => {
                    let _ = writeln!(out, "    - {e}");
                }
            }
        }
        let _ = writeln!(out, "{}", r.details);
    }
    eprintln!("{out}");
    let dir = workspace_root().join("target").join("w7-k8s-reports");
    let _ = fs::create_dir_all(&dir);
    let _ = fs::write(dir.join(format!("{test}.txt")), &out);
    assert_eq!(failed, 0, "{out}");
}

fn lifecycle_lines(cluster: &Cluster, lcs: &BTreeMap<usize, Lifecycle>) -> String {
    lcs.iter()
        .map(|(i, lc)| {
            format!(
                "      {} ({:?}): {}",
                cluster.pods[*i].spec.host(),
                cluster.pods[*i].spec.side,
                lc.timeline()
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Facts of one variant run that are compared against the Go-only run.
#[derive(Debug, Clone, Default)]
struct Facts {
    outcomes: BTreeMap<String, String>,
    /// host -> (first HTTP answer, observed still not answering at >= HTTP_UP_BUDGET).
    first_http: BTreeMap<String, (Option<Duration>, bool)>,
    ready_after: BTreeMap<String, Option<Duration>>,
    converged: Option<bool>,
    steady_view: Option<View>,
    memberlist_keys: BTreeMap<String, Vec<String>>,
    alone_view: Option<View>,
    ingest_ok: BTreeMap<String, bool>,
    blocks: Option<usize>,
    cluster_seed: Option<bool>,
    admin: BTreeMap<String, BTreeMap<String, String>>,
    series: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    restart: Option<RestartObs>,
    restart_outcome: Option<String>,
    restart_data_ok: BTreeMap<String, bool>,
    /// (frozen pod dropped from memberlist by every other pod, reconvergence after the thaw).
    freeze: Option<(bool, Option<Duration>)>,
    crash_dropped_by_all: Option<bool>,
    /// Container restarts by the restart policy (crash loops), per pod.
    restarts: BTreeMap<String, u32>,
}

fn sides(cluster: &Cluster) -> BTreeMap<String, Side> {
    cluster
        .pods
        .iter()
        .map(|p| (p.spec.host(), p.spec.side))
        .collect()
}

/// Differential contract: every fact of `other` must match the Go-only run.
fn compare_facts(go: &Facts, other: &Facts, other_sides: &BTreeMap<String, Side>) -> Vec<String> {
    let mut errs = Vec::new();
    let side = |h: &str| {
        format!(
            "{h} ({:?})",
            other_sides.get(h).copied().unwrap_or(Side::Go)
        )
    };
    for (h, o) in &other.outcomes {
        if let Some(g) = go.outcomes.get(h) {
            if g != o {
                errs.push(format!("{}: outcome {o}, Go pod {g}", side(h)));
            }
        }
    }
    let go_http_ok = go.first_http.values().all(|(_, late)| !late);
    for (h, (t, late)) in &other.first_http {
        let late = *late;
        if late && go_http_ok && other_sides.get(h) == Some(&Side::Rust) {
            errs.push(format!(
                "{}: still no HTTP answer {:?} after process start (first answer {:?}); every Go pod answered within it",
                side(h),
                HTTP_UP_BUDGET,
                t
            ));
        }
    }
    let go_max_ready = go.ready_after.values().flatten().max().copied();
    if let Some(gmax) = go_max_ready {
        for (h, t) in &other.ready_after {
            if let Some(t) = t {
                if *t > gmax + Duration::from_secs(15) {
                    errs.push(format!(
                        "{}: ready after {:.1}s, slowest Go pod {:.1}s",
                        side(h),
                        t.as_secs_f64(),
                        gmax.as_secs_f64()
                    ));
                }
            }
        }
    }
    if go.converged.is_some() && go.converged != other.converged {
        errs.push(format!(
            "convergence to full membership: {:?}, Go {:?}",
            other.converged, go.converged
        ));
    }
    if let (Some(g), Some(o)) = (&go.steady_view, &other.steady_view) {
        if g != o {
            errs.push(format!(
                "steady-state cluster view differs from Go: {}",
                describe_view_diff(o, g)
            ));
        }
    }
    if let Some(gk) = go.memberlist_keys.values().next() {
        let bad: BTreeMap<&String, &Vec<String>> = other
            .memberlist_keys
            .iter()
            .filter(|(_, k)| *k != gk)
            .collect();
        if !bad.is_empty() {
            errs.push(format!(
                "/memberlist JSON (Accept: application/json) top-level keys differ from Go {gk:?}: {:?}",
                bad.iter().map(|(h, k)| (side(h), *k)).collect::<Vec<_>>()
            ));
        }
    }
    if let (Some(g), Some(o)) = (&go.alone_view, &other.alone_view) {
        if g != o {
            errs.push(format!(
                "single-pod view differs from Go: {}",
                describe_view_diff(o, g)
            ));
        }
    }
    for (h, ok) in &other.ingest_ok {
        if !ok && go.ingest_ok.get(h) == Some(&true) {
            errs.push(format!(
                "{}: QueryStream data wrong (Go pod correct)",
                side(h)
            ));
        }
    }
    if go.blocks.is_some() && go.blocks != other.blocks {
        errs.push(format!(
            "/ingester/flush shipped {:?} complete blocks to S3, Go {:?}",
            other.blocks, go.blocks
        ));
    }
    if let Some(g) = go.admin.values().next() {
        let mut diffs: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (h, a) in &other.admin {
            for (path, want) in g {
                let got = a.get(path).cloned().unwrap_or_default();
                if &got != want {
                    diffs
                        .entry(format!("GET {path}: {got:?}, Go {want:?}"))
                        .or_default()
                        .insert(side(h));
                }
            }
        }
        for (d, who) in diffs {
            errs.push(format!(
                "{d} [{}]",
                who.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    let go_series = union_series(
        &go.series,
        &go.series.keys().map(|h| (h.clone(), Side::Go)).collect(),
        Side::Go,
    );
    let rust_series = union_series(&other.series, other_sides, Side::Rust);
    if !go_series.is_empty() && !rust_series.is_empty() {
        errs.extend(
            mixin_metric_diffs(&go_series, &rust_series)
                .into_iter()
                .map(|e| format!("/metrics of the Rust pods (union) vs Go-only cluster: {e}")),
        );
    }
    // `cluster_seed` is informational only: Go's usage-stats reporter writes the seed after a
    // random 0..5m jitter (pkg/usagestats/seed.go `clusterSeedFileMinStability`), so its presence
    // within the test window is not deterministic.
    if let (Some(g), Some(o)) = (&go.restart, &other.restart) {
        if g.exit_code != o.exit_code {
            errs.push(format!(
                "SIGTERM exit code {:?}, Go {:?}",
                o.exit_code, g.exit_code
            ));
        }
        if let (Some(gt), Some(ot)) = (g.stop_took, o.stop_took) {
            if ot > gt + Duration::from_secs(10) {
                errs.push(format!(
                    "SIGTERM shutdown took {:.1}s, Go {:.1}s",
                    ot.as_secs_f64(),
                    gt.as_secs_f64()
                ));
            }
        }
        if g.essential() != o.essential() {
            errs.push(format!(
                "ring state sequence of the restarted instance seen by the other pods: {:?}, Go {:?}",
                o.ring_states, g.ring_states
            ));
        }
        let own = |r: &RestartObs| {
            r.partition_ownership
                .values()
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        if own(g) != own(o) {
            errs.push(format!(
                "partition-ring ownership sequence during restart: {:?}, Go {:?}",
                o.partition_ownership, g.partition_ownership
            ));
        }
        let same = |r: &RestartObs| {
            (
                r.tokens_before == r.tokens_after,
                r.tokens_file_before == r.tokens_file_after,
                r.tokens_file_before.as_ref()
                    == Some(&{
                        let mut t = r.tokens_before.clone();
                        t.sort_unstable();
                        t
                    }),
            )
        };
        if same(g) != same(o) {
            errs.push(format!(
                "tokens across restart (ring same, file same, file==ring): {:?}, Go {:?}",
                same(o),
                same(g)
            ));
        }
    }
    if let (Some((g_drop, g_conv)), Some((o_drop, o_conv))) = (go.freeze, other.freeze) {
        // One-directional: whether a 40s freeze outlasts the suspicion timeout varies run to
        // run for Go too; only "Go detected it, Rust did not" is a divergence.
        if g_drop && !o_drop {
            errs.push(format!(
                "frozen pod dropped from memberlist by every other pod: {o_drop}, Go {g_drop}"
            ));
        }
        match (g_conv, o_conv) {
            (Some(_), None) => errs.push("never reconverged after the pod freeze, Go did".into()),
            (Some(g), Some(o)) if o > g + Duration::from_secs(30) => errs.push(format!(
                "reconverged {:.1}s after the thaw, Go {:.1}s",
                o.as_secs_f64(),
                g.as_secs_f64()
            )),
            _ => {}
        }
    }
    if go.crash_dropped_by_all == Some(true) && other.crash_dropped_by_all != Some(true) {
        errs.push(format!(
            "SIGKILLed pod dropped from memberlist by every other pod within 90s: {:?}, Go {:?}",
            other.crash_dropped_by_all, go.crash_dropped_by_all
        ));
    }
    let go_restarts: u32 = go.restarts.values().sum();
    for (h, n) in &other.restarts {
        if *n > 0 && go_restarts == 0 && other_sides.get(h) == Some(&Side::Rust) {
            errs.push(format!(
                "{}: container restarted {n} time(s) (crash), no Go pod restarted",
                side(h)
            ));
        }
    }
    if go.restart_outcome.is_some() && go.restart_outcome != other.restart_outcome {
        errs.push(format!(
            "restarted pod outcome {:?}, Go {:?}",
            other.restart_outcome, go.restart_outcome
        ));
    }
    for (h, ok) in &other.restart_data_ok {
        if !ok && go.restart_data_ok.get(h) == Some(&true) {
            errs.push(format!(
                "{}: QueryStream after rolling restart wrong (Go pod correct)",
                side(h)
            ));
        }
    }
    errs
}

// ---------------------------------------------------------------------------------------------
// Scenario 1 + 3 + 5 + 4: fresh install, convergence, Kafka ingest, flush, rolling restart
// ---------------------------------------------------------------------------------------------

struct VariantRun {
    variant: Variant,
    sides: BTreeMap<String, Side>,
    /// Absolute errors (prefixed `harness:` for the Go-only run).
    errors: Vec<String>,
    facts: Facts,
    details: String,
}

async fn fresh_install(helm: Arc<Helm>, variant: Variant) -> VariantRun {
    let specs = topology(&helm, variant, REPLICAS_PER_ZONE);
    let mut run = VariantRun {
        variant,
        sides: BTreeMap::new(),
        errors: Vec::new(),
        facts: Facts::default(),
        details: String::new(),
    };
    let mut cluster =
        match Cluster::start("fresh", variant, helm.clone(), specs, "on-failure").await {
            Ok(c) => c,
            Err(e) => {
                run.errors.push(format!("harness: cluster start: {e:#}"));
                return run;
            }
        };
    run.sides = sides(&cluster);
    let all: Vec<usize> = (0..cluster.pods.len()).collect();
    let mut details = Vec::new();

    // 1. all pods at once (podManagementPolicy: Parallel).
    progress(&format!("{}: infra up, starting all pods", cluster.project));
    let lcs = match start_and_observe(&mut cluster, &all, false, Duration::from_secs(120)).await {
        Ok(l) => l,
        Err(e) => {
            run.errors.push(format!("harness: start pods: {e:#}"));
            return run;
        }
    };
    progress(&format!("{}: startup settled", cluster.project));
    details.push(format!("    startup:\n{}", lifecycle_lines(&cluster, &lcs)));
    for (i, lc) in &lcs {
        let h = cluster.pods[*i].spec.host();
        run.facts.outcomes.insert(h.clone(), lc.outcome().kind());
        run.facts
            .first_http
            .insert(h.clone(), (lc.first_answer(), lc.http_late()));
        run.facts.ready_after.insert(
            h.clone(),
            match lc.outcome() {
                Outcome::Ready(t) => Some(t),
                _ => None,
            },
        );
        if !matches!(lc.outcome(), Outcome::Ready(_)) {
            run.errors.push(format!(
                "{h} ({:?}) did not become ready: {}",
                cluster.pods[*i].spec.side,
                lc.timeline()
            ));
        }
        if lc.http_late() {
            run.errors.push(format!(
                "{h} ({:?}) still no HTTP answer at >= {HTTP_UP_BUDGET:?} after process start (first answer {:?})",
                cluster.pods[*i].spec.side,
                lc.first_answer()
            ));
        }
    }

    // 3. convergence of every pod's view.
    let conv = converge(&cluster, &all, &all, Duration::from_secs(90)).await;
    run.facts.converged = Some(conv.after.is_some());
    run.errors.extend(convergence_errors(&cluster, &all, &conv));
    details.push(format!("    converged after {:?}", conv.after));
    let views: Vec<&RawView> = conv
        .views
        .values()
        .filter_map(|v| v.as_ref().ok())
        .collect();
    if let Some(first) = views.first() {
        run.facts.steady_view = Some(first.view.clone());
        for (h, v) in &conv.views {
            if let Ok(v) = v {
                run.facts
                    .memberlist_keys
                    .insert(h.clone(), v.memberlist_keys.clone());
                if v.view != first.view {
                    run.errors.push(format!(
                        "{h}: view differs from other pods: {}",
                        describe_view_diff(&v.view, &first.view)
                    ));
                }
            }
        }
    }

    progress(&format!("{}: converged {:?}", cluster.project, conv.after));
    // 5. Kafka ingest + QueryStream on every pod.
    let base_ts = now_ms() - 10 * 60_000;
    let partitions: Vec<usize> = (0..REPLICAS_PER_ZONE).collect();
    let offsets = match produce(&cluster, batch_records(&partitions, base_ts, 0)).await {
        Ok(o) => o,
        Err(e) => {
            run.errors.push(format!("harness: produce batch 0: {e:#}"));
            run.details = details.join("\n");
            return run;
        }
    };
    for (h, err) in verify_pods_data(&cluster, &all, &offsets, base_ts, 1).await {
        run.facts.ingest_ok.insert(h.clone(), err.is_none());
        if let Some(e) = err {
            run.errors.push(format!(
                "{h} ({:?}) QueryStream after Kafka ingest: {e}",
                run.sides[&h]
            ));
        }
    }

    progress(&format!("{}: ingest verified", cluster.project));
    // 5b. /ingester/flush on every pod -> one block per pod shipped to S3 (MinIO).
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    for i in &all {
        let url = format!(
            "http://{}:{}/ingester/flush?wait=true",
            cluster.pods[*i].ip, helm.http_port
        );
        match http.post(&url).send().await {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => run.errors.push(format!(
                "{} ({:?}) POST /ingester/flush?wait=true: {}",
                cluster.pods[*i].spec.host(),
                cluster.pods[*i].spec.side,
                r.status()
            )),
            Err(e) => run.errors.push(format!(
                "{} POST /ingester/flush: {e}",
                cluster.pods[*i].spec.host()
            )),
        }
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut blocks = shipped_blocks(&cluster);
    while blocks.len() < all.len() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        blocks = shipped_blocks(&cluster);
    }
    run.facts.blocks = Some(blocks.len());
    if blocks.len() != all.len() {
        run.errors.push(format!(
            "expected {} complete blocks (one per pod) in s3://{}/{TENANT}/ after /ingester/flush, found {:?}",
            all.len(),
            helm.blocks_bucket,
            blocks
        ));
    }

    progress(&format!(
        "{}: flush done, {} blocks",
        cluster.project,
        blocks.len()
    ));
    for i in &all {
        let p = &cluster.pods[*i];
        run.facts
            .admin
            .insert(p.spec.host(), admin_surface(&p.ip, helm.http_port).await);
        match scrape_series(&p.ip, helm.http_port).await {
            Ok(s) => {
                run.facts.series.insert(p.spec.host(), s);
            }
            Err(e) => run
                .errors
                .push(format!("{} GET /metrics: {e:#}", p.spec.host())),
        }
    }
    // 4. Rolling restart of zone-a-1 (a Rust pod in the mixed variant).
    let target_i = cluster.idx(&format!("{RELEASE}-ingester-zone-a-1"));
    let target = cluster.pods[target_i].spec.host();
    let partition = cluster.pods[target_i].spec.replica as i64;
    let observers: Vec<(String, String)> = all
        .iter()
        .filter(|i| **i != target_i)
        .map(|i| (cluster.pods[*i].spec.host(), cluster.pods[*i].ip.clone()))
        .collect();
    let mut obs = RestartObs {
        tokens_before: views
            .first()
            .and_then(|v| v.ring_tokens.get(&target).cloned())
            .unwrap_or_default(),
        tokens_file_before: read_tokens_file(&cluster.pod_dir(target_i).join("data/tokens")),
        ..Default::default()
    };
    let stop = Arc::new(AtomicBool::new(false));
    let watch = spawn_ring_watch(
        observers.clone(),
        helm.http_port,
        target.clone(),
        partition,
        stop.clone(),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    match cluster.stop_pod(target_i).await {
        Ok((code, took)) => {
            obs.exit_code = Some(code);
            obs.stop_took = Some(took);
        }
        Err(e) => run
            .errors
            .push(format!("harness: docker stop {target}: {e:#}")),
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    // Batch 1 is produced while the pod is down; it must be consumed after the restart.
    let offsets1 = produce(&cluster, batch_records(&partitions, base_ts, 1)).await;
    // emptyDir is per pod: a recreated pod gets an empty one.
    let _ = fs::remove_dir_all(cluster.pod_dir(target_i).join("active-queries"));
    let _ = fs::create_dir_all(cluster.pod_dir(target_i).join("active-queries"));
    // A recreated k8s pod gets a new IP: occupy the freed one so Docker hands out another.
    let old_ip = cluster.pods[target_i].ip.clone();
    if let Err(e) = cluster.start_ip_filler().await {
        run.errors.push(format!("harness: ip filler: {e:#}"));
    }
    progress(&format!("{}: recreating {target}", cluster.project));
    let lc =
        match start_and_observe(&mut cluster, &[target_i], true, Duration::from_secs(120)).await {
            Ok(l) => l.into_values().next().unwrap_or_default(),
            Err(e) => {
                run.errors
                    .push(format!("harness: recreate {target}: {e:#}"));
                Lifecycle::default()
            }
        };
    details.push(format!(
        "    {target} IP before/after recreate: {old_ip} -> {} (k8s: new pod IP)",
        cluster.pods[target_i].ip
    ));
    run.facts.restart_outcome = Some(lc.outcome().kind());
    if !matches!(lc.outcome(), Outcome::Ready(_)) {
        run.errors.push(format!(
            "{target} ({:?}) did not become ready after restart: {}",
            run.sides[&target],
            lc.timeline()
        ));
    }
    let conv2 = converge(&cluster, &all, &all, Duration::from_secs(60)).await;
    run.errors.extend(
        convergence_errors(&cluster, &all, &conv2)
            .into_iter()
            .map(|e| format!("after restart: {e}")),
    );
    stop.store(true, Ordering::Relaxed);
    if let Ok((states, owners)) = watch.await {
        obs.ring_states = states;
        obs.partition_ownership = owners;
    }
    obs.tokens_after = conv2
        .views
        .values()
        .filter_map(|v| v.as_ref().ok())
        .find_map(|v| v.ring_tokens.get(&target).cloned())
        .unwrap_or_default();
    obs.tokens_file_after = read_tokens_file(&cluster.pod_dir(target_i).join("data/tokens"));
    obs.lifecycle = lc;
    if obs.tokens_before.is_empty() || obs.tokens_before != obs.tokens_after {
        run.errors.push(format!(
            "{target}: ring tokens changed across restart ({} before, {} after, equal: {})",
            obs.tokens_before.len(),
            obs.tokens_after.len(),
            obs.tokens_before == obs.tokens_after
        ));
    }
    if obs.tokens_file_before.is_none() || obs.tokens_file_before != obs.tokens_file_after {
        run.errors.push(format!(
            "{target}: tokens file /data/tokens missing or changed across restart"
        ));
    }
    match offsets1 {
        Ok(off1) => {
            for (h, err) in verify_pods_data(&cluster, &all, &off1, base_ts, 2).await {
                run.facts.restart_data_ok.insert(h.clone(), err.is_none());
                if let Some(e) = err {
                    run.errors.push(format!(
                        "{h} ({:?}) QueryStream after rolling restart of {target}: {e}",
                        run.sides[&h]
                    ));
                }
            }
        }
        Err(e) => run.errors.push(format!("harness: produce batch 1: {e:#}")),
    }
    details.push(format!(
        "    restart of {target}: exit {:?} after {:?}; restart: {}\n      ring states seen by observers: {:?}\n      partition ownership seen: {:?}",
        obs.exit_code,
        obs.stop_took,
        obs.lifecycle.timeline(),
        obs.ring_states,
        obs.partition_ownership
    ));
    run.facts.restart = Some(obs);

    // 6. Pod freeze (stalled node / VM pause): SIGSTOP zone-c-1 for 40s, then SIGCONT. Go
    //    peers declare it dead in memberlist; after the thaw it refutes and the cluster view
    //    must converge back to the full membership.
    let frozen_i = cluster.idx(&format!("{RELEASE}-ingester-zone-c-1"));
    let frozen = cluster.pods[frozen_i].spec.host();
    let others: Vec<usize> = all.iter().copied().filter(|i| *i != frozen_i).collect();
    progress(&format!("{}: freezing {frozen}", cluster.project));
    if let Err(e) = docker(&["pause", &cluster.pods[frozen_i].container]).await {
        run.errors.push(format!("harness: docker pause: {e:#}"));
    }
    let http = http_client();
    let ip_to_host = cluster.ip_to_host();
    let freeze_start = Instant::now();
    let mut dropped_by: BTreeSet<String> = BTreeSet::new();
    while freeze_start.elapsed() < Duration::from_secs(40) {
        for i in &others {
            let p = &cluster.pods[*i];
            if let Ok(v) = fetch_view(&http, &p.ip, helm.http_port, &ip_to_host).await {
                if !v.view.members.contains(&frozen) {
                    dropped_by.insert(p.spec.host());
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if let Err(e) = docker(&["unpause", &cluster.pods[frozen_i].container]).await {
        run.errors.push(format!("harness: docker unpause: {e:#}"));
    }
    let conv3 = converge(&cluster, &all, &all, Duration::from_secs(120)).await;
    run.errors.extend(
        convergence_errors(&cluster, &all, &conv3)
            .into_iter()
            .map(|e| format!("after 40s freeze of {frozen}: {e}")),
    );
    run.facts.freeze = Some((dropped_by.len() == others.len(), conv3.after));
    details.push(format!(
        "    freeze of {frozen} for 40s: dropped from memberlist by {dropped_by:?}; reconverged {:?} after thaw",
        conv3.after
    ));

    // 7. Pod crash (OOMKill / node loss): SIGKILL zone-b-1, no graceful memberlist leave. Go
    //    peers mark it failed after the suspicion timeout and drop it from the member list.
    let crashed_i = cluster.idx(&format!("{RELEASE}-ingester-zone-b-1"));
    let crashed = cluster.pods[crashed_i].spec.host();
    let others: Vec<usize> = all.iter().copied().filter(|i| *i != crashed_i).collect();
    progress(&format!("{}: SIGKILL {crashed}", cluster.project));
    if let Err(e) = docker(&["kill", &cluster.pods[crashed_i].container]).await {
        run.errors.push(format!("harness: docker kill: {e:#}"));
    }
    let crash_start = Instant::now();
    let mut dropped_after: BTreeMap<String, Duration> = BTreeMap::new();
    while crash_start.elapsed() < Duration::from_secs(90) && dropped_after.len() < others.len() {
        for i in &others {
            let p = &cluster.pods[*i];
            if dropped_after.contains_key(&p.spec.host()) {
                continue;
            }
            if let Ok(v) = fetch_view(&http, &p.ip, helm.http_port, &ip_to_host).await {
                if !v.view.members.contains(&crashed) {
                    dropped_after.insert(p.spec.host(), crash_start.elapsed());
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    run.facts.crash_dropped_by_all = Some(dropped_after.len() == others.len());
    if dropped_after.len() != others.len() {
        let kept: Vec<String> = others
            .iter()
            .map(|i| &cluster.pods[*i].spec)
            .filter(|s| !dropped_after.contains_key(&s.host()))
            .map(|s| format!("{} ({:?})", s.host(), s.side))
            .collect();
        run.errors.push(format!(
            "SIGKILLed {crashed} still listed as a memberlist member 90s later by {kept:?}"
        ));
    }
    details.push(format!(
        "    SIGKILL of {crashed}: dropped from memberlist after {dropped_after:?}"
    ));
    let lc =
        match start_and_observe(&mut cluster, &[crashed_i], true, Duration::from_secs(120)).await {
            Ok(l) => l.into_values().next().unwrap_or_default(),
            Err(e) => {
                run.errors
                    .push(format!("harness: recreate {crashed}: {e:#}"));
                Lifecycle::default()
            }
        };
    if !matches!(lc.outcome(), Outcome::Ready(_)) {
        run.errors.push(format!(
            "{crashed} did not become ready after the crash: {}",
            lc.timeline()
        ));
    }
    let conv4 = converge(&cluster, &all, &all, Duration::from_secs(90)).await;
    run.errors.extend(
        convergence_errors(&cluster, &all, &conv4)
            .into_iter()
            .map(|e| format!("after SIGKILL + restart of {crashed}: {e}")),
    );
    for i in &all {
        let restarts = docker(&[
            "inspect",
            "-f",
            "{{.RestartCount}}",
            &cluster.pods[*i].container,
        ])
        .await
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(0);
        run.facts
            .restarts
            .insert(cluster.pods[*i].spec.host(), restarts);
    }
    details.push(format!(
        "    container restarts (restartPolicy): {:?}",
        run.facts.restarts
    ));
    run.facts.cluster_seed = Some(bucket_objects(
        &cluster,
        "__mimir_cluster/mimir_cluster_seed.json",
    ));
    details.push(format!(
        "    usage-stats cluster seed in s3://{}/__mimir_cluster/ at the end: {:?}",
        helm.blocks_bucket, run.facts.cluster_seed
    ));
    if !run.errors.is_empty() {
        details.push(cluster.all_log_tails(&all, 15).await);
    }
    run.details = details.join("\n");
    run
}

/// Runs the variants one after another (one Redpanda + Silo + pods set at a time, to bound host
/// memory) unless `MIMIR_W7_PARALLEL=1`. `MIMIR_W7_VARIANTS=go,rust` restricts the variants (Go
/// is always kept: it defines the expectations).
async fn run_variants<F, Fut>(variants: &[Variant], timeout: Duration, f: F) -> Vec<VariantRun>
where
    F: Fn(Variant) -> Fut,
    Fut: std::future::Future<Output = VariantRun>,
{
    let filter = std::env::var("MIMIR_W7_VARIANTS").ok();
    let selected: Vec<Variant> = variants
        .iter()
        .copied()
        .filter(|v| {
            *v == Variant::Go
                || filter
                    .as_deref()
                    .is_none_or(|f| f.split(',').any(|n| n.trim() == v.name()))
        })
        .collect();
    let guarded = |v: Variant| {
        let fut = f(v);
        async move {
            progress(&format!("variant {} started", v.name()));
            let run = match tokio::time::timeout(timeout, fut).await {
                Ok(run) => run,
                Err(_) => VariantRun {
                    variant: v,
                    sides: BTreeMap::new(),
                    errors: vec![format!("variant did not finish within {timeout:?}")],
                    facts: Facts::default(),
                    details: String::new(),
                },
            };
            progress(&format!(
                "variant {} finished with {} error(s)",
                v.name(),
                run.errors.len()
            ));
            run
        }
    };
    if std::env::var("MIMIR_W7_PARALLEL").as_deref() == Ok("1") {
        futures::future::join_all(selected.into_iter().map(guarded)).await
    } else {
        let mut out = Vec::new();
        for v in selected {
            out.push(guarded(v).await);
        }
        out
    }
}

fn progress(msg: &str) {
    static T0: OnceLock<Instant> = OnceLock::new();
    let t0 = T0.get_or_init(Instant::now);
    eprintln!("[w7 +{:>6.1}s] {msg}", t0.elapsed().as_secs_f64());
}

fn report_variants(runs: Vec<VariantRun>) -> Vec<Report> {
    let go_facts = runs
        .iter()
        .find(|r| r.variant == Variant::Go)
        .map(|r| r.facts.clone());
    runs.into_iter()
        .map(|r| {
            let mut errors: Vec<String> = if r.variant == Variant::Go {
                r.errors
                    .iter()
                    .map(|e| {
                        if e.starts_with("harness:") {
                            e.clone()
                        } else {
                            format!("harness: Go-only cluster: {e}")
                        }
                    })
                    .collect()
            } else {
                r.errors.clone()
            };
            let mut details = r.details.clone();
            if r.variant != Variant::Go {
                if let Some(g) = &go_facts {
                    // Informational: every Go series family (not only mixin-referenced ones,
                    // Go-runtime `go_*` excluded) that no Rust pod exposes.
                    let family = |n: &str| {
                        n.trim_end_matches("_bucket")
                            .trim_end_matches("_sum")
                            .trim_end_matches("_count")
                            .to_string()
                    };
                    let go_series = union_series(
                        &g.series,
                        &g.series.keys().map(|h| (h.clone(), Side::Go)).collect(),
                        Side::Go,
                    );
                    let rust_series = union_series(&r.facts.series, &r.sides, Side::Rust);
                    if !rust_series.is_empty() {
                        let rust_fams: BTreeSet<String> =
                            rust_series.keys().map(|n| family(n)).collect();
                        let missing: BTreeSet<String> = go_series
                            .keys()
                            .map(|n| family(n))
                            .filter(|f| !f.starts_with("go_") && !rust_fams.contains(f))
                            .collect();
                        let _ = write!(
                            details,
                            "\n    informational: {} Go series families absent on all Rust pods: {missing:?}",
                            missing.len()
                        );
                    }

                    errors.extend(
                        compare_facts(g, &r.facts, &r.sides)
                            .into_iter()
                            .map(|e| format!("vs Go: {e}")),
                    );
                }
            }
            Report {
                name: format!(
                    "{} cluster {:?}",
                    r.variant.name(),
                    r.sides
                        .iter()
                        .map(|(h, s)| format!("{}={s:?}", h.trim_start_matches("mimir-ingester-")))
                        .collect::<Vec<_>>()
                ),
                errors,
                details,
            }
        })
        .collect()
}

#[test]
fn k8s_helm_fresh_install_converge_ingest_rolling_restart() {
    if skip_docker("k8s_helm_fresh_install_converge_ingest_rolling_restart") {
        return;
    }
    ensure_fresh_binaries();
    let helm = Arc::new(Helm::load().expect("load Helm-rendered manifests from .ref"));
    block_on_for_tests(async {
        let runs = run_variants(
            &[Variant::Go, Variant::Rust, Variant::Mixed],
            Duration::from_secs(900),
            |v| fresh_install(helm.clone(), v),
        )
        .await;
        finish(
            "k8s_helm_fresh_install_converge_ingest_rolling_restart",
            "W7 k8s Helm: fresh install (parallel pods) + convergence + Kafka ingest + flush + rolling restart",
            report_variants(runs),
        );
    });
}

// ---------------------------------------------------------------------------------------------
// Scenario 2: the first pod alone (gossip DNS resolves only to itself), then a second pod joins
// ---------------------------------------------------------------------------------------------

async fn first_pod_alone(helm: Arc<Helm>, variant: Variant) -> VariantRun {
    let specs = topology(&helm, variant, 1);
    let mut run = VariantRun {
        variant,
        sides: BTreeMap::new(),
        errors: Vec::new(),
        facts: Facts::default(),
        details: String::new(),
    };
    let mut cluster =
        match Cluster::start("alone", variant, helm.clone(), specs, "on-failure").await {
            Ok(c) => c,
            Err(e) => {
                run.errors.push(format!("harness: cluster start: {e:#}"));
                return run;
            }
        };
    run.sides = sides(&cluster);
    let a = cluster.idx(&format!("{RELEASE}-ingester-zone-a-0"));
    let b = cluster.idx(&format!("{RELEASE}-ingester-zone-b-0"));
    let mut details = Vec::new();
    progress(&format!("{}: starting first pod", cluster.project));
    let lcs = match start_and_observe(&mut cluster, &[a], false, Duration::from_secs(120)).await {
        Ok(l) => l,
        Err(e) => {
            run.errors.push(format!("harness: start: {e:#}"));
            return run;
        }
    };
    progress(&format!("{}: first pod settled", cluster.project));
    details.push(format!("    alone:\n{}", lifecycle_lines(&cluster, &lcs)));
    let lc = &lcs[&a];
    let host_a = cluster.pods[a].spec.host();
    run.facts
        .outcomes
        .insert(host_a.clone(), lc.outcome().kind());
    run.facts
        .first_http
        .insert(host_a.clone(), (lc.first_answer(), lc.http_late()));
    run.facts.ready_after.insert(
        host_a.clone(),
        match lc.outcome() {
            Outcome::Ready(t) => Some(t),
            _ => None,
        },
    );
    if !matches!(lc.outcome(), Outcome::Ready(_)) {
        run.errors.push(format!(
            "{host_a} alone did not become ready: {}",
            lc.timeline()
        ));
    }
    if lc.http_late() {
        run.errors.push(format!(
            "{host_a} still no HTTP answer at >= {HTTP_UP_BUDGET:?} after process start (first answer {:?})",
            lc.first_answer()
        ));
    }
    let conv = converge(&cluster, &[a], &[a], Duration::from_secs(30)).await;
    run.errors.extend(convergence_errors(&cluster, &[a], &conv));
    if let Some(Ok(v)) = conv.views.values().next() {
        run.facts.alone_view = Some(v.view.clone());
    }

    // Second pod (other implementation in the mixed variants) joins the lone first pod.
    progress(&format!("{}: starting second pod", cluster.project));
    let lcs = match start_and_observe(&mut cluster, &[b], false, Duration::from_secs(120)).await {
        Ok(l) => l,
        Err(e) => {
            run.errors.push(format!("harness: start second pod: {e:#}"));
            return run;
        }
    };
    details.push(format!(
        "    second pod:\n{}",
        lifecycle_lines(&cluster, &lcs)
    ));
    let host_b = cluster.pods[b].spec.host();
    run.facts
        .outcomes
        .insert(host_b.clone(), lcs[&b].outcome().kind());
    let conv = converge(&cluster, &[a, b], &[a, b], Duration::from_secs(60)).await;
    run.facts.converged = Some(conv.after.is_some());
    run.errors
        .extend(convergence_errors(&cluster, &[a, b], &conv));
    details.push(format!("    two pods converged after {:?}", conv.after));
    if let Some(Ok(v)) = conv.views.values().next() {
        run.facts.steady_view = Some(v.view.clone());
    }
    if !run.errors.is_empty() {
        details.push(cluster.all_log_tails(&[a, b], 20).await);
    }
    run.details = details.join("\n");
    run
}

#[test]
fn k8s_helm_first_pod_alone_then_second_joins() {
    if skip_docker("k8s_helm_first_pod_alone_then_second_joins") {
        return;
    }
    ensure_fresh_binaries();
    let helm = Arc::new(Helm::load().expect("load Helm-rendered manifests from .ref"));
    block_on_for_tests(async {
        let runs = run_variants(
            &[
                Variant::Go,
                Variant::Rust,
                Variant::Mixed,
                Variant::MixedRev,
            ],
            Duration::from_secs(600),
            |v| first_pod_alone(helm.clone(), v),
        )
        .await;
        finish(
            "k8s_helm_first_pod_alone_then_second_joins",
            "W7 k8s Helm: first pod alone (dns+ resolves to itself) then a second pod joins",
            report_variants(runs),
        );
    });
}

// ---------------------------------------------------------------------------------------------
// Scenario 6: object storage unreachable at startup (`sanity-check` module)
// ---------------------------------------------------------------------------------------------

async fn object_storage_down(helm: Arc<Helm>, variant: Variant) -> VariantRun {
    let specs = topology(&helm, variant, 1);
    let mut run = VariantRun {
        variant,
        sides: BTreeMap::new(),
        errors: Vec::new(),
        facts: Facts::default(),
        details: String::new(),
    };
    let mut cluster = match Cluster::start("s3down", variant, helm.clone(), specs, "no").await {
        Ok(c) => c,
        Err(e) => {
            run.errors.push(format!("harness: cluster start: {e:#}"));
            return run;
        }
    };
    run.sides = sides(&cluster);
    let a = cluster.idx(&format!("{RELEASE}-ingester-zone-a-0"));
    if let Err(e) = docker(&["stop", "-t", "0", &format!("{}-minio", cluster.project)]).await {
        run.errors.push(format!("harness: stop minio: {e:#}"));
        return run;
    }
    let lcs = match start_and_observe(&mut cluster, &[a], false, Duration::from_secs(180)).await {
        Ok(l) => l,
        Err(e) => {
            run.errors.push(format!("harness: start: {e:#}"));
            return run;
        }
    };
    let lc = &lcs[&a];
    let h = cluster.pods[a].spec.host();
    run.facts.outcomes.insert(h.clone(), lc.outcome().kind());
    run.details = format!(
        "    object storage stopped:\n{}\n{}",
        lifecycle_lines(&cluster, &lcs),
        cluster.all_log_tails(&[a], 8).await
    );
    run
}

#[test]
fn k8s_helm_object_storage_unreachable_at_startup() {
    if skip_docker("k8s_helm_object_storage_unreachable_at_startup") {
        return;
    }
    ensure_fresh_binaries();
    let helm = Arc::new(Helm::load().expect("load Helm-rendered manifests from .ref"));
    block_on_for_tests(async {
        let runs = run_variants(
            &[Variant::Go, Variant::Rust],
            Duration::from_secs(400),
            |v| object_storage_down(helm.clone(), v),
        )
        .await;
        finish(
            "k8s_helm_object_storage_unreachable_at_startup",
            "W7 k8s Helm: S3 blocks storage unreachable at startup",
            report_variants(runs),
        );
    });
}

#[test]
fn helm_manifests_parse() {
    let helm = Helm::load().expect("load Helm-rendered manifests from .ref");
    assert_eq!(
        helm.gossip_alias,
        "mimir-gossip-ring.mimir.svc.cluster.local"
    );
    assert_eq!(helm.kafka_alias, "mimir-kafka.mimir.svc.cluster.local");
    assert_eq!(helm.s3_alias, "mimir-minio.mimir.svc");
    assert!(helm
        .mimir_yaml
        .contains("dns+mimir-gossip-ring.mimir.svc.cluster.local.:7946"));
    for (zone, sts) in &helm.zones {
        assert!(sts
            .args
            .contains(&format!("-ingester.ring.instance-availability-zone={zone}")));
        assert!(sts
            .args
            .contains(&"-memberlist.abort-if-fast-join-fails=true".to_string()));
    }
    assert_eq!((helm.http_port, helm.grpc_port), (8080, 9095));
    assert_eq!(
        parse_rfc3339_utc("1970-01-02T00:00:01.5Z"),
        Some(UNIX_EPOCH + Duration::from_millis(86_401_500))
    );
}
