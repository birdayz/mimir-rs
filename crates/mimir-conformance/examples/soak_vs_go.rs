//! Production-grade twin Go-vs-Rust 24-hour soak test harness.
//!
//! Boots unmodified Go `mimir` (`target/go-mimir`) and Rust `mimir` (`target/release/mimir`)
//! side-by-side inside `gcr.io/distroless/cc-debian13` Docker containers using the upstream
//! `mimir-distributed` Helm `ingest_storage` configuration against real Redpanda (Kafka) and
//! Silo (MinIO S3) containers:
//! - Go ingester:   `mimir-ingester-zone-a-0` (zone-a, partition 0)
//! - Rust ingester: `mimir-ingester-zone-b-0` (zone-b, partition 0)
//! - Both join the same memberlist gossip ring and consume the exact same Redpanda topic.
//! - Both ship compacted TSDB blocks to the shared Silo S3 bucket (`mimir-tsdb`).
//!
//! Continuous workload:
//! - Kafka v2 RW2 batches (floats, native histograms, exemplars, metadata, OOO samples, series churn)
//! - Concurrent read traffic across ALL 13 `cortex.Ingester` read RPCs + `grpc.health.v1.Health/Check`
//!   + `/httpgrpc.HTTP/Handle`, asserting 0 RPC errors and periodic Go-vs-Rust response parity
//! - Telemetry sampling (`telemetry.csv` + `latest-summary.txt`) combining cgroup v2 (`working_set`),
//!   host `/proc/<pid>`, and scraped `/metrics`, plus automatic anomaly detection.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::TcpListener as StdTcpListener;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::Bytes;
use chrono::Utc;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_config::storage::KafkaConfig;
use mimir_config::{parse_go_duration, StringSliceCSV};
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin,
    rust_mimir_release_bin, workspace_root,
};
use mimir_proto::cortexpb;
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::HttpRequest;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use mimir_proto::symbols::{
    v2_common_symbols, RECORD_VERSION_HEADER_KEY, V2_RECORD_SYMBOL_OFFSET,
};
use mimir_proto::zerocopy::from_write_request_to_rw2;
use mimir_storage::ingest::{
    record_version_header, KafkaClient, KafkaCompression, ProduceRecord, RecordVersion,
};
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_yaml::Value as Yaml;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const DISTROLESS_IMAGE: &str = "gcr.io/distroless/cc-debian13@sha256:809d04afa1be033c441f76bb0051878c342fb76d0ea236bc6dc94b655fa0fbd6";
const REDPANDA_IMAGE: &str = "redpandadata/redpanda:v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649";
const SILO_IMAGE: &str = "pgsty/silo:RELEASE.2026-09-03T13-18-01Z@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "soak-vs-go");

const HELM_TEST: &str = "test-ingest-storage-values";
const HELM_RELEASE: &str = "test-ingest-storage-values";
const HELM_NAMESPACE: &str = "citestns";
const RELEASE: &str = "mimir";
const NAMESPACE: &str = "mimir";

const TENANT_PROD: &str = "soak-prod";
const TENANT_CANARY: &str = "soak-canary";
const TENANT_EPHEMERAL_PREFIX: &str = "soak-ephemeral-";
const EPHEMERAL_SERIES_COUNT: usize = 500;
const EPHEMERAL_CLOSE_IDLE_TIMEOUT_SECS: u64 = 15 * 60;
const EPHEMERAL_CLOSE_IDLE_TIMEOUT_STR: &str = "15m";
const EPHEMERAL_HEAD_COMPACTION_IDLE_TIMEOUT_STR: &str = "10m";
const SAWTOOTH_LONG_RUN_SECS: u64 = 4 * 3600;
const FLOAT_FAMILIES: usize = 160;
const HIST_FAMILIES: usize = 20;
const SERIES_PER_POD: usize = 5;
const SAMPLES_PER_RECORD: usize = 400;

const CLUSTERS: &[&str] = &["prod-us-east-1", "prod-us-west-2", "prod-eu-west-1"];
const CONTAINERS: &[&str] = &["app", "envoy", "otel-sidecar", "worker", "proxy"];
const STATUS_CODES: &[&str] = &["200", "201", "204", "400", "404", "500", "503"];

// ---------------------------------------------------------------------------
// CLI Config
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct SoakConfig {
    series: usize,
    rate_samples_per_sec: usize,
    churn_rate_per_min: f64,
    ooo_rate: f64,
    query_rps: f64,
    duration_secs: u64,
    sample_secs: u64,
    warmup_secs: Option<u64>,
    parity_interval_secs: u64,
    retention_period: String,
    head_compaction_interval: Option<String>,
    ephemeral_tenant_interval_secs: u64,
    out_dir: PathBuf,
    skip_build: bool,
    strict_rss_trend: bool,
}

impl Default for SoakConfig {
    fn default() -> Self {
        let ts = Utc::now().format("%Y%m%d-%H%M%S");
        Self {
            series: 200_000,
            rate_samples_per_sec: 20_000,
            churn_rate_per_min: 120.0,
            ooo_rate: 0.005,
            query_rps: 10.0,
            duration_secs: 86_400,
            sample_secs: 60,
            warmup_secs: None,
            parity_interval_secs: 60,
            retention_period: "6h".to_string(),
            head_compaction_interval: None,
            ephemeral_tenant_interval_secs: 300,
            out_dir: workspace_root().join("target").join(format!("soak-{ts}")),
            skip_build: false,
            strict_rss_trend: false,
        }
    }
}

impl SoakConfig {
    fn effective_warmup_secs(&self) -> u64 {
        self.warmup_secs
            .unwrap_or_else(|| (self.duration_secs / 4).clamp(30, 600))
    }

    fn retention_secs(&self) -> f64 {
        parse_go_duration(&self.retention_period)
            .ok()
            .filter(|&ns| ns > 0)
            .map(|ns| ns as f64 / 1_000_000_000.0)
            .unwrap_or(6.0 * 3600.0)
    }

    fn head_compaction_interval_secs(&self) -> u64 {
        let s = self.head_compaction_interval.as_deref().unwrap_or("15m");
        parse_go_duration(s)
            .ok()
            .filter(|&ns| ns > 0)
            .map(|ns| (ns as u64) / 1_000_000_000)
            .unwrap_or(15 * 60)
    }

    fn ephemeral_settle_max_secs(&self) -> u64 {
        // close_idle_tsdb_timeout (15m) + head_compaction_interval (15m default)
        // + ship_interval (1m) + DefaultCloseIdleTSDBInterval (5m) + jitter margin (10m)
        EPHEMERAL_CLOSE_IDLE_TIMEOUT_SECS
            + self.head_compaction_interval_secs().max(15 * 60)
            + 60
            + 5 * 60
            + 10 * 60
    }
}

fn print_help() {
    println!(
        "soak_vs_go — Twin Go vs Rust Mimir 24-hour containerized soak test\n\n\
         OPTIONS:\n\
           --series <N>                     Total active series across float + histogram (default: 200000)\n\
           --rate <samples/s>               Target Kafka sample ingestion rate (default: 20000)\n\
           --churn-rate <series/min>        Series churned per minute (default: 120)\n\
           --ooo-rate <0..1>                Fraction of out-of-order samples (default: 0.005)\n\
           --query-rps <rps>                Target concurrent gRPC read RPC rate per engine (default: 10)\n\
           --duration-secs <secs>           Total soak duration in seconds (default: 86400)\n\
           --sample-secs <secs>             Telemetry sampling interval in seconds (default: 60)\n\
           --warmup-secs <secs>             Warmup window before RssAnon trend check (default: clamp(duration/4, 30, 600))\n\
           --parity-interval-secs <secs>    Go-vs-Rust response parity sweep interval (default: 60)\n\
           --retention-period <dur>         TSDB retention period, e.g. 6h or 13h (default: 6h)\n\
           --head-compaction-interval <dur> Optional override for head compaction interval (default: Helm 15m)\n\
           --ephemeral-tenant-interval-secs <secs>  Spawn ephemeral 500-series idle-close tenant every N secs (default: 300, 0=off)\n\
           --out-dir <path>                 Output directory for telemetry.csv & latest-summary.txt\n\
           --strict-rss-trend               Flag monotonic RssAnon growth even on short (<30m) runs\n\
           --skip-build                     Reuse existing target/go-mimir and target/release/mimir\n\
           -h, --help                       Show this help"
    );
}

fn parse_args() -> Result<Option<SoakConfig>> {
    let mut cfg = SoakConfig::default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let next_str = |idx: &mut usize, flag: &str| -> Result<String> {
            *idx += 1;
            args.get(*idx)
                .cloned()
                .ok_or_else(|| anyhow!("missing value for {flag}"))
        };
        match args[i].as_str() {
            "-h" | "--help" => {
                print_help();
                return Ok(None);
            }
            "--series" => {
                cfg.series = next_str(&mut i, "--series")?.parse::<usize>()?.max(100);
            }
            "--rate" => {
                cfg.rate_samples_per_sec = next_str(&mut i, "--rate")?.parse::<usize>()?.max(10);
            }
            "--churn-rate" => {
                cfg.churn_rate_per_min = next_str(&mut i, "--churn-rate")?.parse::<f64>()?.max(0.0);
            }
            "--ooo-rate" => {
                cfg.ooo_rate = next_str(&mut i, "--ooo-rate")?.parse::<f64>()?.clamp(0.0, 0.5);
            }
            "--query-rps" => {
                cfg.query_rps = next_str(&mut i, "--query-rps")?.parse::<f64>()?.max(0.5);
            }
            "--duration-secs" => {
                cfg.duration_secs = next_str(&mut i, "--duration-secs")?.parse::<u64>()?.max(10);
            }
            "--sample-secs" => {
                cfg.sample_secs = next_str(&mut i, "--sample-secs")?.parse::<u64>()?.max(1);
            }
            "--warmup-secs" => {
                cfg.warmup_secs = Some(next_str(&mut i, "--warmup-secs")?.parse::<u64>()?);
            }
            "--parity-interval-secs" => {
                cfg.parity_interval_secs =
                    next_str(&mut i, "--parity-interval-secs")?.parse::<u64>()?.max(5);
            }
            "--retention-period" => {
                cfg.retention_period = next_str(&mut i, "--retention-period")?;
            }
            "--head-compaction-interval" => {
                cfg.head_compaction_interval =
                    Some(next_str(&mut i, "--head-compaction-interval")?);
            }
            "--ephemeral-tenant-interval-secs" => {
                cfg.ephemeral_tenant_interval_secs =
                    next_str(&mut i, "--ephemeral-tenant-interval-secs")?.parse::<u64>()?;
            }
            "--out-dir" => {
                cfg.out_dir = PathBuf::from(next_str(&mut i, "--out-dir")?);
            }
            "--strict-rss-trend" => {
                cfg.strict_rss_trend = true;
            }
            "--skip-build" => {
                cfg.skip_build = true;
            }
            other => bail!("unknown flag {other} (try --help)"),
        }
        i += 1;
    }
    Ok(Some(cfg))
}

// ---------------------------------------------------------------------------
// Helm Config Loader & Compose Cluster
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ZoneSts {
    args: Vec<String>,
    env: Vec<(String, String)>,
    termination_grace_secs: u64,
}

#[derive(Debug, Clone)]
struct HelmConfig {
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
    buckets: BTreeSet<String>,
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

fn host_port(addr: &str) -> Result<(String, u16)> {
    let addr = addr.trim_start_matches("dns+");
    let (h, p) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("no port in {addr}"))?;
    Ok((h.trim_end_matches('.').to_string(), p.parse()?))
}

fn patch_soak_limits(mimir_yaml: &str, cfg: &SoakConfig) -> Result<String> {
    let mut root: Yaml = serde_yaml::from_str(mimir_yaml)?;
    let root_map = root
        .as_mapping_mut()
        .ok_or_else(|| anyhow!("mimir.yaml root is not a mapping"))?;
    let limits_key = Yaml::String("limits".to_string());
    if !root_map.contains_key(&limits_key) {
        root_map.insert(limits_key.clone(), Yaml::Mapping(Default::default()));
    }
    let limits = root_map
        .get_mut(&limits_key)
        .and_then(Yaml::as_mapping_mut)
        .ok_or_else(|| anyhow!("limits is not a mapping"))?;

    let overrides = [
        ("max_global_series_per_user", Yaml::Number(0.into())),
        ("max_global_exemplars_per_user", Yaml::Number(100_000.into())),
        ("out_of_order_time_window", Yaml::String("1h".to_string())),
        ("native_histograms_ingestion_enabled", Yaml::Bool(true)),
        ("ingestion_rate", Yaml::Number(10_000_000.into())),
        ("ingestion_burst_size", Yaml::Number(10_000_000.into())),
        ("max_fetched_chunks_per_query", Yaml::Number(2_000_000.into())),
    ];
    for (k, v) in overrides {
        limits.insert(Yaml::String(k.to_string()), v);
    }

    if cfg.ephemeral_tenant_interval_secs > 0 {
        let bs_key = Yaml::String("blocks_storage".to_string());
        if !root_map.contains_key(&bs_key) {
            root_map.insert(bs_key.clone(), Yaml::Mapping(Default::default()));
        }
        let bs = root_map
            .get_mut(&bs_key)
            .and_then(Yaml::as_mapping_mut)
            .ok_or_else(|| anyhow!("blocks_storage is not a mapping"))?;
        let tsdb_key = Yaml::String("tsdb".to_string());
        if !bs.contains_key(&tsdb_key) {
            bs.insert(tsdb_key.clone(), Yaml::Mapping(Default::default()));
        }
        let tsdb = bs
            .get_mut(&tsdb_key)
            .and_then(Yaml::as_mapping_mut)
            .ok_or_else(|| anyhow!("blocks_storage.tsdb is not a mapping"))?;
        // Note: Go tsdb.TSDBConfig.CloseIdleTSDBInterval has yaml:"-" (rejected by KnownFields(true))
        // and defaults to DefaultCloseIdleTSDBInterval = 5m in both Go and Rust.
        // Setting head_compaction_idle_timeout=10m + close_idle_tsdb_timeout=15m lets idle
        // ephemeral tenants compact, ship (1m), and close+delete within ~15-25m.
        tsdb.insert(
            Yaml::String("close_idle_tsdb_timeout".to_string()),
            Yaml::String(EPHEMERAL_CLOSE_IDLE_TIMEOUT_STR.to_string()),
        );
        tsdb.insert(
            Yaml::String("head_compaction_idle_timeout".to_string()),
            Yaml::String(EPHEMERAL_HEAD_COMPACTION_IDLE_TIMEOUT_STR.to_string()),
        );
    }

    Ok(serde_yaml::to_string(&root)?)
}

impl HelmConfig {
    fn load(cfg: &SoakConfig) -> Result<Self> {
        let dir = workspace_root()
            .join(".ref/mimir/operations/helm/tests")
            .join(format!("{HELM_TEST}-generated"))
            .join("mimir-distributed/templates");
        let cfg_docs = yaml_docs(&dir.join("mimir-config.yaml"))?;
        let raw_mimir_yaml = ystr(&cfg_docs[0], &["data", "mimir.yaml"])?;
        let mimir_yaml = patch_soak_limits(raw_mimir_yaml, cfg)?;
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
        ensure!(zones.contains_key("zone-a") && zones.contains_key("zone-b"));

        let cfg: Yaml = serde_yaml::from_str(&mimir_yaml)?;
        let join = cfg["memberlist"]["join_members"][0]
            .as_str()
            .ok_or_else(|| anyhow!("no memberlist.join_members"))?;
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
            mimir_yaml,
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
            buckets,
        })
    }
}

fn free_tcp_ports(n: usize) -> Result<Vec<u16>> {
    let mut listeners = Vec::with_capacity(n);
    let mut ports = Vec::with_capacity(n);
    for _ in 0..n {
        let l = StdTcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
        ports.push(l.local_addr()?.port());
        listeners.push(l);
    }
    Ok(ports)
}

fn q(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""))
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    fn zone(self) -> &'static str {
        match self {
            Side::Go => "zone-a",
            Side::Rust => "zone-b",
        }
    }

    fn host(self) -> String {
        format!("{RELEASE}-ingester-{}-0", self.zone())
    }
}

#[derive(Clone, Debug)]
struct IngesterEndpoint {
    container: String,
    http_port: u16,
    grpc_port: u16,
}

struct SoakCluster {
    project: String,
    compose_file: PathBuf,
    _temp_dir: tempfile::TempDir,
    helm: HelmConfig,
    kafka_container: String,
    kafka_ext_port: u16,
    go: IngesterEndpoint,
    rust: IngesterEndpoint,
    cleaned: bool,
}

impl SoakCluster {
    async fn start(cfg: &SoakConfig, helm: HelmConfig) -> Result<Self> {
        let project = format!("soak-vs-go-{}", std::process::id());
        let temp_dir = tempfile::Builder::new()
            .prefix("mimir-soak-24h-")
            .tempdir()
            .context("create soak tempdir")?;
        let dir = temp_dir.path();
        let meta = fs::metadata(dir)?;
        let user = format!("{}:{}", meta.uid(), meta.gid());

        let ports = free_tcp_ports(5)?;
        let kafka_ext_port = ports[0];
        let go_http_port = ports[1];
        let go_grpc_port = ports[2];
        let rust_http_port = ports[3];
        let rust_grpc_port = ports[4];

        fs::create_dir_all(dir.join("config"))?;
        fs::create_dir_all(dir.join("runtime"))?;
        fs::write(dir.join("config/mimir.yaml"), &helm.mimir_yaml)?;
        fs::write(dir.join("runtime/runtime.yaml"), &helm.runtime_yaml)?;
        for b in &helm.buckets {
            fs::create_dir_all(dir.join("minio").join(b))?;
        }
        fs::write(
            dir.join("redpanda.yaml"),
            format!(
                "redpanda:\n  data_directory: /var/lib/redpanda/data\n  empty_seed_starts_cluster: true\n  seed_servers: []\n  rpc_server: {{address: 0.0.0.0, port: 33145}}\n  advertised_rpc_api: {{address: 127.0.0.1, port: 33145}}\n  admin: [{{address: 0.0.0.0, port: 9644}}]\n  developer_mode: true\n  kafka_api:\n    - {{name: internal, address: 0.0.0.0, port: {kp}}}\n    - {{name: external, address: 0.0.0.0, port: {ext}}}\n  advertised_kafka_api:\n    - {{name: internal, address: {alias}, port: {kp}}}\n    - {{name: external, address: 127.0.0.1, port: {ext}}}\n",
                kp = helm.kafka_port,
                ext = kafka_ext_port,
                alias = helm.kafka_alias,
            ),
        )?;

        let go_bin = fs::canonicalize(go_mimir_bin())?;
        let rust_bin = fs::canonicalize(rust_mimir_release_bin())?;
        let d = dir.display();

        let mut y = format!(
            "name: {project}\nnetworks:\n  k8s: {{}}\nservices:\n  kafka:\n    image: {rp_img}\n    container_name: {project}-kafka\n    labels: {{{lk}: {lv}}}\n    entrypoint: [\"/bin/sh\", \"-c\", \"cp /config/redpanda.yaml /etc/redpanda/redpanda.yaml && exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --reserve-memory 0M --default-log-level=warn\"]\n    volumes: [{rp_vol}]\n    ports: [\"127.0.0.1:{ext}:{ext}\"]\n    logging:\n      driver: json-file\n      options: {{max-size: \"50m\", max-file: \"3\"}}\n    healthcheck: {{test: [\"CMD\", \"rpk\", \"cluster\", \"health\", \"--exit-when-healthy\"], interval: 1s, timeout: 15s, retries: 120}}\n    networks:\n      k8s:\n        aliases: [{ka}]\n  minio:\n    image: {silo_img}\n    container_name: {project}-minio\n    labels: {{{lk}: {lv}}}\n    user: {user}\n    entrypoint: [\"/bin/sh\", \"-c\", \"exec /usr/bin/docker-entrypoint.sh minio server /export --address :{s3_port}\"]\n    environment: {{MINIO_ROOT_USER: {ak}, MINIO_ROOT_PASSWORD: {sk}, HOME: /tmp}}\n    volumes: [{s3_vol}]\n    logging:\n      driver: json-file\n      options: {{max-size: \"50m\", max-file: \"3\"}}\n    healthcheck: {{test: [\"CMD\", \"curl\", \"-sf\", \"http://127.0.0.1:{s3_port}/minio/health/live\"], interval: 1s, timeout: 5s, retries: 120}}\n    networks:\n      k8s:\n        aliases: [{s3_alias}]\n",
            rp_img = REDPANDA_IMAGE,
            lk = CONTAINER_LABEL.0,
            lv = CONTAINER_LABEL.1,
            rp_vol = q(&format!("{d}/redpanda.yaml:/config/redpanda.yaml:ro")),
            ext = kafka_ext_port,
            ka = q(&helm.kafka_alias),
            silo_img = SILO_IMAGE,
            user = q(&user),
            s3_port = helm.s3_port,
            ak = q(&helm.s3_access_key),
            sk = q(&helm.s3_secret_key),
            s3_vol = q(&format!("{d}/minio:/export")),
            s3_alias = q(&helm.s3_alias),
        );

        let pod_specs = [
            (Side::Go, &go_bin, go_http_port, go_grpc_port),
            (Side::Rust, &rust_bin, rust_http_port, rust_grpc_port),
        ];
        for (side, bin, hp, gp) in pod_specs {
            let zone = side.zone();
            let sts = &helm.zones[zone];
            let host = side.host();
            let pod_dir = dir.join("pods").join(&host);
            fs::create_dir_all(pod_dir.join("data"))?;
            fs::create_dir_all(pod_dir.join("active-queries"))?;

            let mut args_vec = sts.args.clone();
            args_vec.push(format!(
                "-blocks-storage.tsdb.retention-period={}",
                cfg.retention_period
            ));
            if let Some(ref hci) = cfg.head_compaction_interval {
                args_vec.push(format!("-blocks-storage.tsdb.head-compaction-interval={hci}"));
            }
            let args: Vec<String> = args_vec.iter().map(|a| q(a)).collect();
            let env: Vec<String> = sts
                .env
                .iter()
                .map(|(k, v)| format!("{k}: {}", q(v)))
                .collect();
            let vols = [
                format!("{}:/bin/mimir:ro", bin.display()),
                format!("{d}/config:/etc/mimir:ro"),
                format!("{d}/runtime:/var/mimir:ro"),
                format!("{}/data:/data", pod_dir.display()),
                format!("{}/active-queries:/active-query-tracker", pod_dir.display()),
            ];
            let _ = write!(
                y,
                "  ingester-{side_lbl}:\n    image: {img}\n    profiles: [pods]\n    container_name: {project}-{host}\n    hostname: {host}\n    labels: {{{lk}: {lv}}}\n    user: {user}\n    read_only: true\n    cap_drop: [ALL]\n    security_opt: [\"no-new-privileges:true\"]\n    entrypoint: [\"/bin/mimir\"]\n    command: [{args}]\n    environment: {{{env}}}\n    stop_grace_period: {grace}s\n    ports: [\"127.0.0.1:{hp}:{chp}\", \"127.0.0.1:{gp}:{cgp}\"]\n    logging:\n      driver: json-file\n      options: {{max-size: \"50m\", max-file: \"3\"}}\n    volumes: [{vols}]\n    networks:\n      k8s:\n        aliases: [{alias}]\n",
                side_lbl = side.label(),
                img = DISTROLESS_IMAGE,
                lk = CONTAINER_LABEL.0,
                lv = CONTAINER_LABEL.1,
                user = q(&user),
                args = args.join(", "),
                env = env.join(", "),
                grace = sts.termination_grace_secs.min(60),
                chp = helm.http_port,
                cgp = helm.grpc_port,
                vols = vols.iter().map(|v| q(v)).collect::<Vec<_>>().join(", "),
                alias = q(&helm.gossip_alias),
            );
        }

        let compose_file = dir.join("compose.yaml");
        fs::write(&compose_file, &y)?;
        let compose_str = compose_file.display().to_string();

        // Start Redpanda + Silo first and wait until healthy.
        docker(&["compose", "-p", &project, "-f", &compose_str, "up", "-d"]).await?;

        let cluster = Self {
            project: project.clone(),
            compose_file,
            _temp_dir: temp_dir,
            helm: helm.clone(),
            kafka_container: format!("{project}-kafka"),
            kafka_ext_port,
            go: IngesterEndpoint {
                container: format!("{project}-{}", Side::Go.host()),
                http_port: go_http_port,
                grpc_port: go_grpc_port,
            },
            rust: IngesterEndpoint {
                container: format!("{project}-{}", Side::Rust.host()),
                http_port: rust_http_port,
                grpc_port: rust_grpc_port,
            },
            cleaned: false,
        };

        cluster.wait_infra_healthy(Duration::from_secs(180)).await?;
        cluster.create_soak_topic().await?;

        // Start both ingester pods concurrently (memberlist DNS fast-join).
        docker(&[
            "compose",
            "-p",
            &project,
            "-f",
            &compose_str,
            "--profile",
            "pods",
            "up",
            "-d",
        ])
        .await?;

        Ok(cluster)
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
                    let logs = docker(&["logs", "--tail", "30", &container])
                        .await
                        .unwrap_or_default();
                    bail!(
                        "{container} not healthy after {timeout:?} (status {:?}):\n{logs}",
                        health.trim()
                    );
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        Ok(())
    }

    async fn create_soak_topic(&self) -> Result<()> {
        // Pre-create with 1 partition so both Go (zone-a-0) and Rust (zone-b-0) own partition 0
        // and consume 100% of the exact same Kafka stream, with bounded 1h / 2GiB retention.
        docker(&[
            "exec",
            &self.kafka_container,
            "rpk",
            "topic",
            "create",
            &self.helm.topic,
            "-p",
            "1",
            "-r",
            "1",
            "-c",
            "retention.ms=3600000",
            "-c",
            "retention.bytes=2147483648",
            "-c",
            "segment.bytes=67108864",
        ])
        .await?;

        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![format!("127.0.0.1:{}", self.kafka_ext_port)]),
            topic: self.helm.topic.clone(),
            ..Default::default()
        };
        let client = KafkaClient::from_config(&cfg)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let _ = client.refresh_metadata(&[self.helm.topic.as_str()]).await;
            let (leader, _) = client.cached_partition_leader(&self.helm.topic, 0);
            if leader >= 0 {
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "no partition 0 leader elected for {}",
                self.helm.topic
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_ingesters_ready(&self, http: &reqwest::Client, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        for ep in [&self.go, &self.rust] {
            loop {
                let running = docker(&["inspect", "-f", "{{.State.Running}}", &ep.container])
                    .await
                    .unwrap_or_default();
                if running.trim() != "true" {
                    let logs = docker(&["logs", "--tail", "40", &ep.container])
                        .await
                        .unwrap_or_default();
                    bail!("{} exited before becoming ready:\n{logs}", ep.container);
                }
                if let Ok(resp) = http
                    .get(format!("http://127.0.0.1:{}/ready", ep.http_port))
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await
                {
                    if resp.status().as_u16() == 200 {
                        break;
                    }
                }
                if Instant::now() >= deadline {
                    let logs = docker(&["logs", "--tail", "40", &ep.container])
                        .await
                        .unwrap_or_default();
                    bail!("{} not ready within {timeout:?}:\n{logs}", ep.container);
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }

        // Wait for memberlist & partition ring convergence so both own partition 0.
        let want_hosts: BTreeSet<String> = [Side::Go.host(), Side::Rust.host()].into_iter().collect();
        loop {
            let mut all_ok = true;
            for ep in [&self.go, &self.rust] {
                let ok = check_ring_converged(http, ep.http_port, &want_hosts).await;
                if !ok {
                    all_ok = false;
                }
            }
            if all_ok {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let mut diag = String::new();
                for ep in [&self.go, &self.rust] {
                    let ring_body = http
                        .get(format!("http://127.0.0.1:{}/ingester/partition-ring", ep.http_port))
                        .header("Accept", "application/json")
                        .timeout(Duration::from_secs(2))
                        .send()
                        .await;
                    let body_str = match ring_body {
                        Ok(r) => r.text().await.unwrap_or_default(),
                        Err(e) => format!("err: {e}"),
                    };
                    let _ = writeln!(diag, "{}: {body_str}", ep.container);
                }
                bail!("memberlist / partition ring did not converge within {timeout:?}:\n{diag}");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn teardown(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
        let compose = self.compose_file.display().to_string();
        let _ = tokio::process::Command::new("docker")
            .args([
                "compose",
                "-p",
                &self.project,
                "-f",
                &compose,
                "--profile",
                "pods",
                "down",
                "-v",
                "-t",
                "10",
                "--remove-orphans",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
}

impl Drop for SoakCluster {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        self.cleaned = true;
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
                "5",
                "--remove-orphans",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

async fn check_ring_converged(
    http: &reqwest::Client,
    http_port: u16,
    want_hosts: &BTreeSet<String>,
) -> bool {
    let Ok(resp) = http
        .get(format!("http://127.0.0.1:{http_port}/ingester/partition-ring"))
        .header("Accept", "application/json")
        .timeout(Duration::from_secs(2))
        .send()
        .await
    else {
        return false;
    };
    let Ok(v) = resp.json::<serde_json::Value>().await else {
        return false;
    };
    let mut owners = BTreeSet::new();
    let mut part0_active = false;
    for p in v["partitions"].as_array().into_iter().flatten() {
        if p["id"].as_i64() == Some(0) && p["state"].as_i64() == Some(2) {
            part0_active = true;
            for o in p["owner_ids"].as_array().into_iter().flatten() {
                if let Some(id) = o.as_str() {
                    owners.insert(id.to_string());
                }
            }
        }
    }
    if !part0_active || owners != *want_hosts {
        return false;
    }

    let Ok(r_resp) = http
        .get(format!("http://127.0.0.1:{http_port}/ingester/ring"))
        .header("Accept", "application/json")
        .timeout(Duration::from_secs(2))
        .send()
        .await
    else {
        return false;
    };
    let Ok(rv) = r_resp.json::<serde_json::Value>().await else {
        return false;
    };
    let mut active_shards = BTreeSet::new();
    for s in rv["shards"].as_array().into_iter().flatten() {
        if s["state"].as_str() == Some("ACTIVE") {
            if let Some(id) = s["id"].as_str() {
                active_shards.insert(id.to_string());
            }
        }
    }
    active_shards == *want_hosts
}

// ---------------------------------------------------------------------------
// Telemetry Collectors: Cgroup v2 + Host /proc/<pid> + Scraped /metrics
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct CgroupMemStats {
    current: u64,
    anon: u64,
    active_file: u64,
    inactive_file: u64,
    working_set: u64,
}

#[derive(Clone, Debug, Default)]
struct ProcPidStats {
    pid: u32,
    vm_rss_bytes: u64,
    rss_anon_bytes: u64,
    rss_file_bytes: u64,
    vm_data_bytes: u64,
    open_fds: u64,
    mmap_count: u64,
    deleted_mmap_count: u64,
    deleted_mmap_lines: Vec<String>,
    cpu_secs: f64,
}

#[derive(Clone, Debug, Default)]
struct MimirMetricsSample {
    active_series: f64,
    head_series: f64,
    head_chunks: f64,
    compactions_total: f64,
    compactions_failed: f64,
    wal_corruptions: f64,
    shipped_blocks: f64,
    shipper_upload_failures: f64,
    objstore_uploads: f64,
    objstore_failures: f64,
    request_duration_count: f64,
    request_duration_errors: f64,
    queried_series_count: f64,
    queried_series_sum: f64,
    last_consumed_offset: i64,
}

#[derive(Clone, Debug, Default)]
struct EngineSample {
    alive: bool,
    cgroup: CgroupMemStats,
    proc_stats: ProcPidStats,
    metrics: MimirMetricsSample,
}

fn clk_tck() -> f64 {
    static TCK: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *TCK.get_or_init(|| {
        Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse::<f64>().ok())
            .filter(|&v| v > 0.0)
            .unwrap_or(100.0)
    })
}

fn find_container_cgroup_dir(container_id: &str, pid: u32) -> Option<PathBuf> {
    if let Ok(cg) = fs::read_to_string(format!("/proc/{pid}/cgroup")) {
        for line in cg.lines() {
            if let Some(rel) = line.strip_prefix("0::") {
                let rel = rel.trim().trim_start_matches('/');
                let candidate = Path::new("/sys/fs/cgroup").join(rel);
                if candidate.join("memory.current").exists() {
                    return Some(candidate);
                }
            }
        }
    }
    let systemd_scope = PathBuf::from(format!(
        "/sys/fs/cgroup/system.slice/docker-{container_id}.scope"
    ));
    if systemd_scope.join("memory.current").exists() {
        return Some(systemd_scope);
    }
    let cgroupfs = PathBuf::from(format!("/sys/fs/cgroup/docker/{container_id}"));
    if cgroupfs.join("memory.current").exists() {
        return Some(cgroupfs);
    }
    None
}

fn read_cgroup_mem(cg_dir: &Path) -> CgroupMemStats {
    let mut out = CgroupMemStats::default();
    if let Ok(txt) = fs::read_to_string(cg_dir.join("memory.current")) {
        out.current = txt.trim().parse().unwrap_or(0);
    }
    if let Ok(txt) = fs::read_to_string(cg_dir.join("memory.stat")) {
        for line in txt.lines() {
            let Some((k, v)) = line.split_once(' ') else {
                continue;
            };
            let val: u64 = v.trim().parse().unwrap_or(0);
            match k {
                "anon" => out.anon = val,
                "active_file" => out.active_file = val,
                "inactive_file" => out.inactive_file = val,
                _ => {}
            }
        }
    }
    out.working_set = out.current.saturating_sub(out.inactive_file);
    out
}

fn read_proc_pid_stats(pid: u32) -> ProcPidStats {
    let mut out = ProcPidStats {
        pid,
        ..Default::default()
    };
    if pid == 0 {
        return out;
    }

    if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) {
        for line in status.lines() {
            let parse_kb = |s: &str| -> u64 {
                s.split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0)
                    * 1024
            };
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                out.vm_rss_bytes = parse_kb(rest);
            } else if let Some(rest) = line.strip_prefix("RssAnon:") {
                out.rss_anon_bytes = parse_kb(rest);
            } else if let Some(rest) = line.strip_prefix("RssFile:") {
                out.rss_file_bytes = parse_kb(rest);
            } else if let Some(rest) = line.strip_prefix("VmData:") {
                out.vm_data_bytes = parse_kb(rest);
            }
        }
    }

    if let Ok(entries) = fs::read_dir(format!("/proc/{pid}/fd")) {
        out.open_fds = entries.count() as u64;
    }

    if let Ok(maps) = fs::read_to_string(format!("/proc/{pid}/maps")) {
        for line in maps.lines() {
            out.mmap_count += 1;
            if line.contains("(deleted)") {
                out.deleted_mmap_count += 1;
                if out.deleted_mmap_lines.len() < 8 {
                    out.deleted_mmap_lines.push(line.to_string());
                }
            }
        }
    }

    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        if let Some(rparen) = stat.rfind(')') {
            let after = &stat[rparen + 2..];
            let fields: Vec<&str> = after.split_whitespace().collect();
            // After comm+space: index 0 is state (field 3), so utime (14) -> 11, stime (15) -> 12
            if fields.len() > 12 {
                let utime: u64 = fields[11].parse().unwrap_or(0);
                let stime: u64 = fields[12].parse().unwrap_or(0);
                out.cpu_secs = (utime + stime) as f64 / clk_tck();
            }
        }
    }

    out
}

fn parse_prom_label(labels: &str, key: &str) -> Option<String> {
    let needle = format!("{key}=\"");
    let start = labels.find(&needle)? + needle.len();
    let rest = &labels[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

async fn scrape_mimir_metrics(http: &reqwest::Client, http_port: u16) -> MimirMetricsSample {
    let mut out = MimirMetricsSample {
        last_consumed_offset: -1,
        ..Default::default()
    };
    let Ok(resp) = http
        .get(format!("http://127.0.0.1:{http_port}/metrics"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
    else {
        return out;
    };
    let Ok(text) = resp.text().await else {
        return out;
    };

    let mut has_active_series = false;
    let mut mem_series_total = 0.0;
    let mut has_tsdb_head_series = false;

    for line in text.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((metric_and_labels, val_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(val) = val_str.trim().parse::<f64>() else {
            continue;
        };
        let (name, labels) = match metric_and_labels.find('{') {
            Some(idx) => (&metric_and_labels[..idx], &metric_and_labels[idx..]),
            None => (metric_and_labels, ""),
        };
        match name {
            "cortex_ingester_active_series" => {
                has_active_series = true;
                out.active_series += val;
            }
            "cortex_ingester_memory_series" => {
                mem_series_total += val;
            }
            "cortex_ingester_tsdb_head_series" => {
                has_tsdb_head_series = true;
                out.head_series += val;
            }
            "cortex_ingester_tsdb_head_chunks" => out.head_chunks += val,
            "cortex_ingester_tsdb_compactions_total" => out.compactions_total += val,
            "cortex_ingester_tsdb_compactions_failed_total" => out.compactions_failed += val,
            "cortex_ingester_tsdb_wal_corruptions_total" => out.wal_corruptions += val,
            "cortex_ingester_shipper_uploads_total" => out.shipped_blocks += val,
            "cortex_ingester_shipper_upload_failures_total" => {
                out.shipper_upload_failures += val;
            }
            "thanos_objstore_bucket_operations_total" => {
                if parse_prom_label(labels, "operation").as_deref() == Some("upload") {
                    out.objstore_uploads += val;
                }
            }
            "thanos_objstore_bucket_operation_failures_total" => {
                out.objstore_failures += val;
            }
            "cortex_request_duration_seconds_count" => {
                out.request_duration_count += val;
                let route = parse_prom_label(labels, "route").unwrap_or_default();
                let code = parse_prom_label(labels, "status_code").unwrap_or_default();
                let is_ok = matches!(
                    code.as_str(),
                    "200" | "204" | "2xx" | "success" | "OK" | "ok"
                );
                // Ignore /ready 503 during initial startup before readiness converged.
                let is_startup_ready = route == "ready" && code == "503";
                if !is_ok && !is_startup_ready {
                    out.request_duration_errors += val;
                }
            }
            "cortex_ingester_queried_series_count" => out.queried_series_count += val,
            "cortex_ingester_queried_series_sum" => out.queried_series_sum += val,
            "cortex_ingest_storage_reader_last_consumed_offset" => {
                out.last_consumed_offset = out.last_consumed_offset.max(val as i64);
            }
            _ => {}
        }
    }

    if !has_tsdb_head_series {
        out.head_series = mem_series_total;
    }
    if !has_active_series {
        out.active_series = mem_series_total;
    }
    out
}

async fn collect_engine_sample(http: &reqwest::Client, ep: &IngesterEndpoint) -> EngineSample {
    let inspect = docker(&[
        "inspect",
        "-f",
        "{{.Id}}|{{.State.Running}}|{{.State.Pid}}",
        &ep.container,
    ])
    .await;
    let Ok(ins) = inspect else {
        return EngineSample::default();
    };
    let parts: Vec<&str> = ins.trim().split('|').collect();
    if parts.len() != 3 {
        return EngineSample::default();
    }
    let cid = parts[0];
    let alive = parts[1] == "true";
    let pid: u32 = parts[2].parse().unwrap_or(0);

    let cgroup = find_container_cgroup_dir(cid, pid)
        .map(|d| read_cgroup_mem(&d))
        .unwrap_or_default();
    let proc_stats = read_proc_pid_stats(pid);
    let metrics = if alive {
        scrape_mimir_metrics(http, ep.http_port).await
    } else {
        MimirMetricsSample::default()
    };

    EngineSample {
        alive,
        cgroup,
        proc_stats,
        metrics,
    }
}

// ---------------------------------------------------------------------------
// Workload Counters & Series Layout (Floats + Native Histograms + Exemplars + OOO + Churn)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct EphemeralTenantRecord {
    tenant_id: String,
    first_offset: i64,
    last_written_at: Instant,
}

#[derive(Default)]
struct WorkloadCounters {
    produced_records: AtomicU64,
    produced_samples: AtomicU64,
    produced_histograms: AtomicU64,
    produced_exemplars: AtomicU64,
    produced_ooo: AtomicU64,
    produced_churn_series: AtomicU64,
    ephemeral_tenants_created: AtomicU64,
    ephemeral_open_go: AtomicU64,
    ephemeral_open_rust: AtomicU64,
    ephemeral_closed_go: AtomicU64,
    ephemeral_closed_rust: AtomicU64,
    kafka_last_offset: AtomicI64,
    rpc_ok_total: AtomicU64,
    rpc_err_total: AtomicU64,
    parity_checks: AtomicU64,
    parity_failures: AtomicU64,
    ephemeral_tenants: Mutex<Vec<EphemeralTenantRecord>>,
    last_rpc_error: Mutex<Option<String>>,
    last_parity_error: Mutex<Option<String>>,
}

impl WorkloadCounters {
    fn new() -> Self {
        Self {
            kafka_last_offset: AtomicI64::new(-1),
            ..Default::default()
        }
    }

    fn record_rpc_err(&self, msg: String) {
        self.rpc_err_total.fetch_add(1, Ordering::Relaxed);
        *self.last_rpc_error.lock() = Some(msg);
    }

    fn record_parity_err(&self, msg: String) {
        self.parity_failures.fetch_add(1, Ordering::Relaxed);
        *self.last_parity_error.lock() = Some(msg);
    }
}

struct SeriesModel {
    total_series: usize,
    float_series: usize,
    num_services: usize,
    slot_gen: Vec<u64>,
    has_in_order: Vec<bool>,
    churn_cursor: usize,
    rng_state: u64,
}

impl SeriesModel {
    fn new(total_series: usize) -> Self {
        let float_series = ((total_series * 9) / 10).max(1);
        let num_services = (total_series / 200).clamp(10, 500);
        Self {
            total_series,
            float_series,
            num_services,
            slot_gen: vec![0; total_series],
            has_in_order: vec![false; total_series],
            churn_cursor: 0,
            rng_state: 0x9e37_79b9_7f4a_7c15,
        }
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        x
    }

    fn is_histogram(&self, slot: usize) -> bool {
        slot >= self.float_series
    }

    fn tenant_for_slot(&self, slot: usize) -> &'static str {
        if slot.is_multiple_of(20) {
            TENANT_CANARY
        } else {
            TENANT_PROD
        }
    }

    fn metric_name(&self, slot: usize) -> String {
        if self.is_histogram(slot) {
            let hid = (slot - self.float_series) % HIST_FAMILIES;
            format!("soak_http_request_duration_seconds_{hid:04}")
        } else {
            let fid = (slot / SERIES_PER_POD) % FLOAT_FAMILIES;
            format!("soak_http_requests_total_{fid:04}")
        }
    }

    fn labels_for_slot(&self, slot: usize) -> Vec<cortexpb::LabelPair> {
        let pod_idx = slot / SERIES_PER_POD;
        let svc_idx = pod_idx % self.num_services;
        let ns_idx = svc_idx % 10;
        let gen = self.slot_gen[slot];
        let cluster = CLUSTERS[pod_idx % CLUSTERS.len()];
        let container = CONTAINERS[pod_idx % CONTAINERS.len()];
        let status_code = STATUS_CODES[slot % STATUS_CODES.len()];
        let metric = self.metric_name(slot);
        let ns = format!("ns-{ns_idx:02}");
        let job = format!("{ns}/service-{svc_idx:04}");
        let svc = format!("svc-{svc_idx:04}");
        let instance = format!("10.42.{}.{}:8080", (pod_idx >> 8) & 0xff, pod_idx & 0xff);
        let pod = format!("pod-{pod_idx:06}-g{gen:05}");

        // Lexicographically sorted: __name__, cluster, container, instance, job, namespace, pod, service, status_code
        vec![
            cortexpb::LabelPair::from_strs("__name__", metric),
            cortexpb::LabelPair::from_static("cluster", cluster),
            cortexpb::LabelPair::from_static("container", container),
            cortexpb::LabelPair::from_strs("instance", instance),
            cortexpb::LabelPair::from_strs("job", job),
            cortexpb::LabelPair::from_strs("namespace", ns),
            cortexpb::LabelPair::from_strs("pod", pod),
            cortexpb::LabelPair::from_strs("service", svc),
            cortexpb::LabelPair::from_static("status_code", status_code),
        ]
    }

    fn churn_n(&mut self, count: usize) -> u64 {
        let mut churned = 0u64;
        for _ in 0..count {
            let slot = self.churn_cursor % self.total_series;
            self.churn_cursor = self.churn_cursor.wrapping_add(1);
            self.slot_gen[slot] += 1;
            self.has_in_order[slot] = false;
            churned += 1;
        }
        churned
    }
}

fn build_native_histogram(ts_ms: i64, step: u64) -> cortexpb::Histogram {
    // Strictly monotonic cumulative bucket counts with exact total count == zero_count + sum(buckets).
    let b0 = step;
    let zero = 3u64;
    let total_count = zero + b0 + (b0 + 1) + (b0 + 2);
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(total_count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(zero)),
        sum: (total_count as f64) * 0.25,
        schema: 2,
        zero_threshold: 0.001,
        positive_spans: vec![cortexpb::BucketSpan {
            offset: 0,
            length: 3,
        }],
        positive_deltas: vec![b0 as i64, 1, 1],
        timestamp: ts_ms,
        ..Default::default()
    }
}

fn encode_rw2_record(
    tenant: &str,
    timeseries: Vec<cortexpb::TimeSeries>,
    metadata: Vec<cortexpb::MetricMetadata>,
    ts_ms: i64,
) -> Result<ProduceRecord> {
    let rw1 = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries,
        metadata,
        ..Default::default()
    };
    let rw2 = from_write_request_to_rw2(&rw1, V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()))
        .map_err(|e| anyhow!("from_write_request_to_rw2: {e:?}"))?;
    let raw_bytes = rw2.encode_to_vec();
    let hdr_val = Bytes::copy_from_slice(&record_version_header(RecordVersion::V2));
    Ok(ProduceRecord {
        key: Some(Bytes::copy_from_slice(tenant.as_bytes())),
        value: Some(Bytes::from(raw_bytes)),
        headers: vec![(RECORD_VERSION_HEADER_KEY.to_string(), hdr_val)],
        timestamp_ms: Some(ts_ms),
    })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn run_producer_loop(
    cfg: SoakConfig,
    kafka_addr: String,
    topic: String,
    counters: Arc<WorkloadCounters>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let kafka_cfg = KafkaConfig {
        address: StringSliceCSV(vec![kafka_addr]),
        topic: topic.clone(),
        ..Default::default()
    };
    let client = KafkaClient::from_config(&kafka_cfg)?;
    let mut model = SeriesModel::new(cfg.series);

    let mut slot_cursor = 0usize;
    let mut step_seq = 1u64;
    let mut churn_accum = 0.0f64;
    let mut meta_cursor = 0usize;

    let batch_records_target = 10usize;
    let samples_per_tick = SAMPLES_PER_RECORD * batch_records_target;
    let tick_dur = Duration::from_secs_f64(
        (samples_per_tick as f64 / cfg.rate_samples_per_sec as f64).clamp(0.02, 1.0),
    );
    let churn_per_tick = (cfg.churn_rate_per_min / 60.0) * tick_dur.as_secs_f64();

    let eph_enabled = cfg.ephemeral_tenant_interval_secs > 0;
    let eph_interval = Duration::from_secs(cfg.ephemeral_tenant_interval_secs.max(1));
    let eph_active_dur =
        Duration::from_secs((cfg.ephemeral_tenant_interval_secs / 2).clamp(10, 60));
    let mut next_eph_start = Instant::now();
    let mut eph_epoch = 0u64;
    let mut current_eph: Option<(String, Instant, Instant, bool)> = None;

    let mut interval = tokio::time::interval(tick_dur);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    while !stop.load(Ordering::Relaxed) {
        interval.tick().await;
        if stop.load(Ordering::Relaxed) {
            break;
        }

        churn_accum += churn_per_tick;
        let do_churn = churn_accum.floor() as usize;
        if do_churn > 0 {
            churn_accum -= do_churn as f64;
            let c = model.churn_n(do_churn);
            counters
                .produced_churn_series
                .fetch_add(c, Ordering::Relaxed);
        }

        let now = now_ms();
        let mut prod_series = Vec::with_capacity(SAMPLES_PER_RECORD);
        let mut canary_series = Vec::with_capacity(64);
        let mut batch_records = Vec::with_capacity(batch_records_target + 2);

        let mut tick_samples = 0u64;
        let mut tick_hists = 0u64;
        let mut tick_exemplars = 0u64;
        let mut tick_ooo = 0u64;

        for _ in 0..samples_per_tick {
            let slot = slot_cursor % model.total_series;
            slot_cursor = slot_cursor.wrapping_add(1);
            step_seq += 1;

            let labels = model.labels_for_slot(slot);
            let tenant = model.tenant_for_slot(slot);

            let is_ooo = model.has_in_order[slot]
                && ((model.next_rand() as f64) / (u64::MAX as f64)) < cfg.ooo_rate;
            let ts_ms = if is_ooo {
                tick_ooo += 1;
                now - 600_000 // 10m ago (within 1h OOO window, well outside 90s parity window)
            } else {
                model.has_in_order[slot] = true;
                now
            };

            let ts = if model.is_histogram(slot) {
                tick_hists += 1;
                cortexpb::TimeSeries {
                    labels,
                    histograms: vec![build_native_histogram(ts_ms, step_seq)],
                    ..Default::default()
                }
            } else {
                let mut exemplars = Vec::new();
                if !is_ooo && slot.is_multiple_of(100) {
                    tick_exemplars += 1;
                    exemplars.push(cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_strs(
                            "trace_id",
                            format!("tr-{step_seq:012}"),
                        )],
                        value: (step_seq % 1000) as f64 * 0.01,
                        timestamp_ms: ts_ms,
                    });
                }
                cortexpb::TimeSeries {
                    labels,
                    samples: vec![cortexpb::Sample {
                        value: (slot as f64) + (step_seq as f64) * 0.25,
                        timestamp_ms: ts_ms,
                    }],
                    exemplars,
                    ..Default::default()
                }
            };
            tick_samples += 1;

            if tenant == TENANT_CANARY {
                canary_series.push(ts);
            } else {
                prod_series.push(ts);
                if prod_series.len() >= SAMPLES_PER_RECORD {
                    let meta = next_metadata_batch(&mut meta_cursor, 16);
                    batch_records.push(encode_rw2_record(
                        TENANT_PROD,
                        std::mem::take(&mut prod_series),
                        meta,
                        now,
                    )?);
                }
            }
        }

        if !prod_series.is_empty() {
            let meta = next_metadata_batch(&mut meta_cursor, 16);
            batch_records.push(encode_rw2_record(TENANT_PROD, prod_series, meta, now)?);
        }
        if !canary_series.is_empty() {
            let meta = next_metadata_batch(&mut meta_cursor, 8);
            batch_records.push(encode_rw2_record(TENANT_CANARY, canary_series, meta, now)?);
        }

        let mut eph_wrote_tenant: Option<(String, bool)> = None;
        if eph_enabled {
            let now_inst = Instant::now();
            if now_inst >= next_eph_start {
                eph_epoch += 1;
                let tid = format!("{TENANT_EPHEMERAL_PREFIX}{eph_epoch:06}");
                let active_until = now_inst + eph_active_dur;
                current_eph = Some((
                    tid,
                    active_until,
                    now_inst - Duration::from_secs(10),
                    false,
                ));
                while next_eph_start <= now_inst {
                    next_eph_start += eph_interval;
                }
            }
            if let Some((ref tid, active_until, ref mut last_write, recorded)) = current_eph {
                if now_inst <= active_until
                    && now_inst.saturating_duration_since(*last_write) >= Duration::from_secs(2)
                {
                    *last_write = now_inst;
                    let mut eph_series = Vec::with_capacity(EPHEMERAL_SERIES_COUNT);
                    for es in 0..EPHEMERAL_SERIES_COUNT {
                        let status_code = STATUS_CODES[es % STATUS_CODES.len()];
                        let labels = vec![
                            cortexpb::LabelPair::from_static(
                                "__name__",
                                "soak_ephemeral_requests_total",
                            ),
                            cortexpb::LabelPair::from_static(
                                "cluster",
                                CLUSTERS[es % CLUSTERS.len()],
                            ),
                            cortexpb::LabelPair::from_static("job", "ephemeral/job"),
                            cortexpb::LabelPair::from_static("namespace", "ephemeral"),
                            cortexpb::LabelPair::from_strs("pod", format!("eph-pod-{es:04}")),
                            cortexpb::LabelPair::from_static("service", "ephemeral-svc"),
                            cortexpb::LabelPair::from_static("status_code", status_code),
                        ];
                        eph_series.push(cortexpb::TimeSeries {
                            labels,
                            samples: vec![cortexpb::Sample {
                                value: (es as f64) + (step_seq as f64) * 0.1,
                                timestamp_ms: now,
                            }],
                            ..Default::default()
                        });
                    }
                    tick_samples += EPHEMERAL_SERIES_COUNT as u64;
                    for chunk in eph_series.chunks(SAMPLES_PER_RECORD) {
                        batch_records.push(encode_rw2_record(
                            tid,
                            chunk.to_vec(),
                            Vec::new(),
                            now,
                        )?);
                    }
                    eph_wrote_tenant = Some((tid.clone(), !recorded));
                }
            }
        }

        let n_recs = batch_records.len() as i64;
        if n_recs > 0 {
            match client
                .produce(&topic, 0, KafkaCompression::Zstd, batch_records)
                .await
            {
                Ok(base_offset) => {
                    let last_offset = base_offset + n_recs - 1;
                    counters
                        .produced_records
                        .fetch_add(n_recs as u64, Ordering::Relaxed);
                    counters
                        .produced_samples
                        .fetch_add(tick_samples, Ordering::Relaxed);
                    counters
                        .produced_histograms
                        .fetch_add(tick_hists, Ordering::Relaxed);
                    counters
                        .produced_exemplars
                        .fetch_add(tick_exemplars, Ordering::Relaxed);
                    counters
                        .produced_ooo
                        .fetch_add(tick_ooo, Ordering::Relaxed);
                    counters
                        .kafka_last_offset
                        .store(last_offset, Ordering::Release);
                    if let Some((tid, is_new)) = eph_wrote_tenant {
                        let written_now = Instant::now();
                        let mut list = counters.ephemeral_tenants.lock();
                        if is_new {
                            list.push(EphemeralTenantRecord {
                                tenant_id: tid,
                                first_offset: last_offset,
                                last_written_at: written_now,
                            });
                            counters
                                .ephemeral_tenants_created
                                .fetch_add(1, Ordering::Relaxed);
                            if let Some((_, _, _, ref mut rec_flag)) = current_eph {
                                *rec_flag = true;
                            }
                        } else if let Some(rec) = list.iter_mut().rfind(|r| r.tenant_id == tid) {
                            rec.last_written_at = written_now;
                        }
                    }
                }
                Err(e) => {
                    counters.record_rpc_err(format!("kafka produce failed: {e}"));
                }
            }
        }
    }

    Ok(())
}

fn next_metadata_batch(cursor: &mut usize, count: usize) -> Vec<cortexpb::MetricMetadata> {
    let total = FLOAT_FAMILIES + HIST_FAMILIES;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let idx = *cursor % total;
        *cursor = cursor.wrapping_add(1);
        if idx < FLOAT_FAMILIES {
            out.push(cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                metric_family_name: format!("soak_http_requests_total_{idx:04}"),
                help: "Soak test HTTP requests counter".to_string(),
                unit: "requests".to_string(),
            });
        } else {
            let hid = idx - FLOAT_FAMILIES;
            out.push(cortexpb::MetricMetadata {
                r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                metric_family_name: format!("soak_http_request_duration_seconds_{hid:04}"),
                help: "Soak test HTTP request latency native histogram".to_string(),
                unit: "seconds".to_string(),
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Read Traffic Driver (All 13 Read RPCs + Health/Check + httpgrpc) & Parity
// ---------------------------------------------------------------------------

fn with_org<T>(org: &str, msg: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.set_timeout(Duration::from_secs(20));
    if let Ok(v) = MetadataValue::try_from(org) {
        req.metadata_mut().insert(LOWER_ORG_ID_HEADER_NAME, v);
    }
    req
}

fn eq_matcher(name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::Equal as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

async fn connect_grpc(port: u16) -> Result<Channel> {
    Channel::from_shared(format!("http://127.0.0.1:{port}"))?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(25))
        .connect()
        .await
        .context("connect gRPC")
}

async fn rpc_health_check(ch: Channel) -> Result<i32> {
    use mimir_cmd::grpc::{HealthCheckRequest, HealthCheckResponse};
    let mut grpc = tonic::client::Grpc::new(ch);
    grpc.ready().await?;
    let codec = tonic::codec::ProstCodec::<HealthCheckRequest, HealthCheckResponse>::default();
    let resp = grpc
        .unary(
            tonic::Request::new(HealthCheckRequest::default()),
            axum::http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check"),
            codec,
        )
        .await?;
    Ok(resp.into_inner().status)
}

async fn rpc_httpgrpc_ready(ch: Channel) -> Result<i32> {
    let mut client = HttpClient::new(ch);
    let req = with_org(
        TENANT_PROD,
        HttpRequest {
            method: "GET".to_string(),
            url: "/ready".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        },
    );
    let resp = client.handle(req).await?.into_inner();
    Ok(resp.code)
}

async fn exercise_single_rpc(
    idx: usize,
    ch: Channel,
    last_offset: i64,
    series_count: usize,
) -> Result<()> {
    let now = now_ms();
    let start_ms = now - 15 * 60 * 1000;
    let end_ms = now + 60_000;
    let mut client = IngesterClient::new(ch.clone());

    match idx % 15 {
        0 => {
            // 1. QueryStream (float, strong consistency)
            let mut req = with_org(
                TENANT_PROD,
                ingesterpb::QueryRequest {
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    matchers: vec![
                        eq_matcher("__name__", "soak_http_requests_total_0000"),
                        eq_matcher("service", "svc-0000"),
                    ],
                    streaming_chunks_batch_size: 32,
                },
            );
            if last_offset >= 0 {
                let md = req.metadata_mut();
                md.insert(
                    "__consistency_level__",
                    MetadataValue::from_static("strong"),
                );
                md.insert(
                    "__consistency_offsets__",
                    MetadataValue::try_from(format!("v1=0:{last_offset}"))?,
                );
            }
            let mut stream = client.query_stream(req).await?.into_inner();
            while let Some(item) = stream.next().await {
                let _ = item?;
            }
        }
        1 => {
            // 1b. QueryStream (native histogram, strong consistency)
            let mut req = with_org(
                TENANT_PROD,
                ingesterpb::QueryRequest {
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    matchers: vec![
                        eq_matcher("__name__", "soak_http_request_duration_seconds_0000"),
                        eq_matcher("status_code", "200"),
                    ],
                    streaming_chunks_batch_size: 32,
                },
            );
            if last_offset >= 0 {
                let md = req.metadata_mut();
                md.insert(
                    "__consistency_level__",
                    MetadataValue::from_static("strong"),
                );
                md.insert(
                    "__consistency_offsets__",
                    MetadataValue::try_from(format!("v1=0:{last_offset}"))?,
                );
            }
            let mut stream = client.query_stream(req).await?.into_inner();
            while let Some(item) = stream.next().await {
                let _ = item?;
            }
        }
        2 => {
            // 2. QueryExemplars
            let req = with_org(
                TENANT_PROD,
                ingesterpb::ExemplarQueryRequest {
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    matchers: vec![ingesterpb::LabelMatchers {
                        matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                    }],
                },
            );
            let _ = client.query_exemplars(req).await?;
        }
        3 => {
            // 3. LabelValues
            let req = with_org(
                TENANT_PROD,
                ingesterpb::LabelValuesRequest {
                    label_name: "service".to_string(),
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    matchers: Some(ingesterpb::LabelMatchers {
                        matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                    }),
                    limit: 100,
                },
            );
            let _ = client.label_values(req).await?;
        }
        4 => {
            // 4. LabelNames
            let req = with_org(
                TENANT_PROD,
                ingesterpb::LabelNamesRequest {
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    matchers: Some(ingesterpb::LabelMatchers {
                        matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                    }),
                    limit: 100,
                },
            );
            let _ = client.label_names(req).await?;
        }
        5 => {
            // 5. LabelNamesAndValues (streaming)
            let req = with_org(
                TENANT_PROD,
                ingesterpb::LabelNamesAndValuesRequest {
                    matchers: vec![
                        eq_matcher("__name__", "soak_http_requests_total_0000"),
                        eq_matcher("service", "svc-0000"),
                    ],
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            );
            let mut stream = client.label_names_and_values(req).await?.into_inner();
            while let Some(item) = stream.next().await {
                let _ = item?;
            }
        }
        6 => {
            // 6. LabelValuesCardinality (streaming)
            let req = with_org(
                TENANT_PROD,
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: vec!["cluster".to_string(), "status_code".to_string()],
                    matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            );
            let mut stream = client.label_values_cardinality(req).await?.into_inner();
            while let Some(item) = stream.next().await {
                let _ = item?;
            }
        }
        7 => {
            // 7. ActiveSeries (streaming)
            let svc_mod = (series_count / 200).clamp(10, 500);
            let svc = format!("svc-{:04}", idx % svc_mod);
            let req = with_org(
                TENANT_PROD,
                ingesterpb::ActiveSeriesRequest {
                    matchers: vec![
                        eq_matcher("__name__", "soak_http_requests_total_0000"),
                        eq_matcher("service", &svc),
                    ],
                    r#type: ingesterpb::active_series_request::RequestType::Series as i32,
                },
            );
            let mut stream = client.active_series(req).await?.into_inner();
            while let Some(item) = stream.next().await {
                let _ = item?;
            }
        }
        8 => {
            // 8. SearchLabelNames (streaming)
            let req = with_org(
                TENANT_PROD,
                ingesterpb::SearchLabelNamesRequest {
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                    filter: Some(ingesterpb::SearchFilter {
                        terms: vec!["status".to_string()],
                        case_insensitive: true,
                        fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                        fuzz_threshold: 0,
                    }),
                    ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                    limit: 50,
                },
            );
            let mut stream = client.search_label_names(req).await?.into_inner();
            while let Some(item) = stream.next().await {
                let _ = item?;
            }
        }
        9 => {
            // 9. SearchLabelValues (streaming)
            let req = with_org(
                TENANT_PROD,
                ingesterpb::SearchLabelValuesRequest {
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    name: "service".to_string(),
                    matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                    filter: Some(ingesterpb::SearchFilter {
                        terms: vec!["svc-000".to_string()],
                        case_insensitive: true,
                        fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                        fuzz_threshold: 0,
                    }),
                    ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                    limit: 50,
                },
            );
            let mut stream = client.search_label_values(req).await?.into_inner();
            while let Some(item) = stream.next().await {
                let _ = item?;
            }
        }
        10 => {
            // 10. UserStats
            let req = with_org(
                TENANT_PROD,
                ingesterpb::UserStatsRequest {
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            );
            let _ = client.user_stats(req).await?;
        }
        11 => {
            // 11. AllUserStats
            let req = with_org(
                TENANT_PROD,
                ingesterpb::UserStatsRequest {
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            );
            let _ = client.all_user_stats(req).await?;
        }
        12 => {
            // 12. MetricsForLabelMatchers
            let req = with_org(
                TENANT_PROD,
                ingesterpb::MetricsForLabelMatchersRequest {
                    start_timestamp_ms: start_ms,
                    end_timestamp_ms: end_ms,
                    matchers_set: vec![ingesterpb::LabelMatchers {
                        matchers: vec![
                            eq_matcher("__name__", "soak_http_requests_total_0000"),
                            eq_matcher("service", "svc-0000"),
                        ],
                    }],
                    limit: 100,
                },
            );
            let _ = client.metrics_for_label_matchers(req).await?;
        }
        13 => {
            // 13. MetricsMetadata
            let req = with_org(
                TENANT_PROD,
                ingesterpb::MetricsMetadataRequest {
                    limit: 50,
                    limit_per_metric: 2,
                    metric: String::new(),
                    metric_names: Vec::new(),
                },
            );
            let _ = client.metrics_metadata(req).await?;
        }
        _ => {
            // 14. Health/Check + /httpgrpc.HTTP/Handle
            let st = rpc_health_check(ch.clone()).await?;
            ensure!(st == 1, "expected Health SERVING (1), got {st}");
            let code = rpc_httpgrpc_ready(ch).await?;
            ensure!(code == 200, "expected /httpgrpc.HTTP/Handle /ready 200, got {code}");
        }
    }
    Ok(())
}

async fn run_reader_loop(
    cfg: SoakConfig,
    go_ch: Channel,
    rust_ch: Channel,
    counters: Arc<WorkloadCounters>,
    stop: Arc<AtomicBool>,
) {
    let tick_dur = Duration::from_secs_f64((1.0 / cfg.query_rps).clamp(0.01, 1.0));
    let mut interval = tokio::time::interval(tick_dur);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut idx = 0usize;
    while !stop.load(Ordering::Relaxed) {
        interval.tick().await;
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let cur = idx;
        idx = idx.wrapping_add(1);
        let offset = counters.kafka_last_offset.load(Ordering::Acquire);
        let c_go = go_ch.clone();
        let c_rust = rust_ch.clone();
        let ctr = Arc::clone(&counters);
        let series_count = cfg.series;

        tokio::spawn(async move {
            let (rg, rr) = tokio::join!(
                exercise_single_rpc(cur, c_go, offset, series_count),
                exercise_single_rpc(cur, c_rust, offset, series_count)
            );
            match rg {
                Ok(()) => {
                    ctr.rpc_ok_total.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => ctr.record_rpc_err(format!("go rpc #{}: {e:#}", cur % 15)),
            }
            match rr {
                Ok(()) => {
                    ctr.rpc_ok_total.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => ctr.record_rpc_err(format!("rust rpc #{}: {e:#}", cur % 15)),
            }
        });
    }
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

type DecodedSeries = (Vec<(String, String)>, Vec<(i64, u64)>);

async fn query_stream_decoded(
    ch: Channel,
    offset: i64,
    start_ms: i64,
    end_ms: i64,
    matchers: Vec<ingesterpb::LabelMatcher>,
) -> Result<Vec<DecodedSeries>> {
    let mut client = IngesterClient::new(ch);
    let mut req = with_org(
        TENANT_PROD,
        ingesterpb::QueryRequest {
            start_timestamp_ms: start_ms,
            end_timestamp_ms: end_ms,
            matchers,
            streaming_chunks_batch_size: 32,
        },
    );
    if offset >= 0 {
        let md = req.metadata_mut();
        md.insert(
            "__consistency_level__",
            MetadataValue::from_static("strong"),
        );
        md.insert(
            "__consistency_offsets__",
            MetadataValue::try_from(format!("v1=0:{offset}"))?,
        );
    }
    let mut stream = client.query_stream(req).await?.into_inner();
    let mut labels: Vec<Vec<(String, String)>> = Vec::new();
    let mut samples: BTreeMap<u64, Vec<(i64, u64)>> = BTreeMap::new();
    while let Some(item) = stream.next().await {
        let msg = item?;
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
                let chunk = EncodedChunk::from_bytes(wire_enc_to_tsdb(c.encoding)?, c.data.to_vec())
                    .map_err(|e| anyhow!("decode chunk: {e:?}"))?;
                for s in decode_chunk_samples(&chunk).map_err(|e| anyhow!("{e:?}"))? {
                    match s.value {
                        SampleValue::Float(v) => entry.push((s.t, v.to_bits())),
                        SampleValue::Histogram(h) => entry.push((s.t, h.count)),
                        SampleValue::FloatHistogram(h) => entry.push((s.t, h.count.to_bits())),
                    }
                }
            }
        }
    }
    let mut out: Vec<DecodedSeries> = labels
        .into_iter()
        .enumerate()
        .map(|(i, mut l)| {
            l.sort();
            let mut v = samples.remove(&(i as u64)).unwrap_or_default();
            v.sort();
            v.dedup();
            (l, v)
        })
        .collect();
    out.sort();
    Ok(out)
}

#[derive(Clone, Debug)]
struct HttpTenantInfo {
    series: u64,
}

async fn fetch_ingester_tenants(
    http: &reqwest::Client,
    http_port: u16,
) -> Result<BTreeMap<String, HttpTenantInfo>> {
    let resp = http
        .get(format!("http://127.0.0.1:{http_port}/ingester/tenants"))
        .header("Accept", "application/json")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .context("GET /ingester/tenants")?;
    ensure!(
        resp.status().is_success(),
        "GET /ingester/tenants returned {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.context("decode /ingester/tenants JSON")?;
    let mut out = BTreeMap::new();
    if let Some(arr) = body["Tenants"].as_array() {
        for item in arr {
            if let Some(tid) = item["Tenant"].as_str() {
                let series = item["Series"].as_u64().unwrap_or(0);
                out.insert(tid.to_string(), HttpTenantInfo { series });
            }
        }
    }
    Ok(out)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EphemeralPhase {
    MustBeOpen,
    TransitionJitter,
    MustBeClosed,
}

fn classify_ephemeral_phase(
    first_offset: i64,
    bar_offset: i64,
    idle_elapsed_secs: u64,
    close_idle_timeout_secs: u64,
    settle_max_secs: u64,
) -> EphemeralPhase {
    if first_offset > bar_offset {
        return EphemeralPhase::TransitionJitter;
    }
    if idle_elapsed_secs < close_idle_timeout_secs.saturating_sub(60) {
        EphemeralPhase::MustBeOpen
    } else if idle_elapsed_secs < settle_max_secs {
        EphemeralPhase::TransitionJitter
    } else {
        EphemeralPhase::MustBeClosed
    }
}

fn verify_tenants_parity(
    side_label: &str,
    go_set: &BTreeSet<String>,
    rust_set: &BTreeSet<String>,
    eph_records: &[EphemeralTenantRecord],
    bar_offset: i64,
    now_inst: Instant,
    settle_max_secs: u64,
) -> Result<(u64, u64, u64, u64)> {
    for steady in [TENANT_PROD, TENANT_CANARY] {
        ensure!(
            go_set.contains(steady) && rust_set.contains(steady),
            "{side_label}: steady tenant {steady} missing (go={}, rust={})",
            go_set.contains(steady),
            rust_set.contains(steady)
        );
    }

    let mut tracked_ids = BTreeSet::new();
    let (mut open_go, mut open_rust, mut closed_go, mut closed_rust) = (0u64, 0u64, 0u64, 0u64);

    for rec in eph_records {
        tracked_ids.insert(rec.tenant_id.as_str());
        let in_go = go_set.contains(&rec.tenant_id);
        let in_rust = rust_set.contains(&rec.tenant_id);
        if in_go {
            open_go += 1;
        } else {
            closed_go += 1;
        }
        if in_rust {
            open_rust += 1;
        } else {
            closed_rust += 1;
        }
        let idle_secs = now_inst
            .saturating_duration_since(rec.last_written_at)
            .as_secs();
        match classify_ephemeral_phase(
            rec.first_offset,
            bar_offset,
            idle_secs,
            EPHEMERAL_CLOSE_IDLE_TIMEOUT_SECS,
            settle_max_secs,
        ) {
            EphemeralPhase::MustBeOpen => {
                ensure!(
                    in_go && in_rust,
                    "{side_label}: active ephemeral tenant {} missing before idle-close (idle={}s, go={in_go}, rust={in_rust})",
                    rec.tenant_id,
                    idle_secs
                );
            }
            EphemeralPhase::TransitionJitter => {}
            EphemeralPhase::MustBeClosed => {
                ensure!(
                    !in_go && !in_rust,
                    "{side_label}: expired ephemeral tenant {} failed to close after idle={}s (go={in_go}, rust={in_rust})",
                    rec.tenant_id,
                    idle_secs
                );
            }
        }
    }

    for tid in go_set.union(rust_set) {
        if tid != TENANT_PROD && tid != TENANT_CANARY && !tracked_ids.contains(tid.as_str()) {
            bail!(
                "{side_label}: unexpected tenant {tid} in ingester (go={}, rust={})",
                go_set.contains(tid),
                rust_set.contains(tid)
            );
        }
    }

    Ok((open_go, open_rust, closed_go, closed_rust))
}

async fn run_parity_sweep(
    http: &reqwest::Client,
    go_http_port: u16,
    rust_http_port: u16,
    go_ch: Channel,
    rust_ch: Channel,
    cfg: &SoakConfig,
    counters: &WorkloadCounters,
) -> Result<()> {
    let offset = counters.kafka_last_offset.load(Ordering::Acquire);
    if offset < 0 {
        return Ok(());
    }
    let now = now_ms();
    // Window [now-90s, now-5s] excludes both new in-order samples (>= now) and OOO samples (now-600s),
    // so once both Go and Rust satisfy strong consistency at `offset`, their contents are identical.
    let start_ms = now - 90_000;
    let end_ms = now - 5_000;
    let qs_matchers = vec![
        eq_matcher("__name__", "soak_http_requests_total_0000"),
        eq_matcher("service", "svc-0000"),
        eq_matcher("status_code", "200"),
    ];

    let (go_qs, rust_qs) = tokio::join!(
        query_stream_decoded(go_ch.clone(), offset, start_ms, end_ms, qs_matchers.clone()),
        query_stream_decoded(rust_ch.clone(), offset, start_ms, end_ms, qs_matchers)
    );
    let go_qs = go_qs.context("go QueryStream parity")?;
    let rust_qs = rust_qs.context("rust QueryStream parity")?;
    ensure!(
        go_qs == rust_qs,
        "QueryStream parity mismatch at offset {offset}: go {} series vs rust {} series",
        go_qs.len(),
        rust_qs.len()
    );

    // Native histogram QueryStream parity
    let hist_matchers = vec![
        eq_matcher("__name__", "soak_http_request_duration_seconds_0000"),
        eq_matcher("status_code", "200"),
    ];
    let (go_hq, rust_hq) = tokio::join!(
        query_stream_decoded(go_ch.clone(), offset, start_ms, end_ms, hist_matchers.clone()),
        query_stream_decoded(rust_ch.clone(), offset, start_ms, end_ms, hist_matchers)
    );
    let go_hq = go_hq.context("go histogram QueryStream parity")?;
    let rust_hq = rust_hq.context("rust histogram QueryStream parity")?;
    ensure!(
        go_hq == rust_hq,
        "Histogram QueryStream parity mismatch at offset {offset}: go {} series vs rust {} series",
        go_hq.len(),
        rust_hq.len()
    );

    // LabelValues on static label `cluster`
    let mut g_cli = IngesterClient::new(go_ch.clone());
    let mut r_cli = IngesterClient::new(rust_ch.clone());
    let lv_req = || {
        with_org(
            TENANT_PROD,
            ingesterpb::LabelValuesRequest {
                label_name: "cluster".to_string(),
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: Some(ingesterpb::LabelMatchers {
                    matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                }),
                limit: 100,
            },
        )
    };
    let (go_lv, rust_lv) = tokio::join!(g_cli.label_values(lv_req()), r_cli.label_values(lv_req()));
    let mut go_vals = go_lv?.into_inner().label_values;
    let mut rust_vals = rust_lv?.into_inner().label_values;
    go_vals.sort();
    rust_vals.sort();
    ensure!(go_vals == rust_vals, "LabelValues mismatch: {go_vals:?} vs {rust_vals:?}");

    // LabelNames
    let ln_req = || {
        with_org(
            TENANT_PROD,
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: Some(ingesterpb::LabelMatchers {
                    matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                }),
                limit: 100,
            },
        )
    };
    let (go_ln, rust_ln) = tokio::join!(g_cli.label_names(ln_req()), r_cli.label_names(ln_req()));
    let mut go_names = go_ln?.into_inner().label_names;
    let mut rust_names = rust_ln?.into_inner().label_names;
    go_names.sort();
    rust_names.sort();
    ensure!(go_names == rust_names, "LabelNames mismatch: {go_names:?} vs {rust_names:?}");

    // AllUserStats + /ingester/tenants parity (accounting for ephemeral idle-close jitter)
    let aus_req = || {
        with_org(
            TENANT_PROD,
            ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
    };
    let (go_aus, rust_aus, go_http_tenants, rust_http_tenants) = tokio::join!(
        g_cli.all_user_stats(aus_req()),
        r_cli.all_user_stats(aus_req()),
        fetch_ingester_tenants(http, go_http_port),
        fetch_ingester_tenants(http, rust_http_port)
    );
    let go_tenants: BTreeSet<String> = go_aus?
        .into_inner()
        .stats
        .into_iter()
        .map(|s| s.user_id)
        .collect();
    let rust_tenants: BTreeSet<String> = rust_aus?
        .into_inner()
        .stats
        .into_iter()
        .map(|s| s.user_id)
        .collect();
    let go_http_map = go_http_tenants?;
    let rust_http_map = rust_http_tenants?;
    let go_http_set: BTreeSet<String> = go_http_map.keys().cloned().collect();
    let rust_http_set: BTreeSet<String> = rust_http_map.keys().cloned().collect();

    let eph_snapshot = counters.ephemeral_tenants.lock().clone();
    let now_inst = Instant::now();
    let settle_max = cfg.ephemeral_settle_max_secs();

    verify_tenants_parity(
        "AllUserStats",
        &go_tenants,
        &rust_tenants,
        &eph_snapshot,
        offset,
        now_inst,
        settle_max,
    )?;
    let (open_go, open_rust, closed_go, closed_rust) = verify_tenants_parity(
        "/ingester/tenants",
        &go_http_set,
        &rust_http_set,
        &eph_snapshot,
        offset,
        now_inst,
        settle_max,
    )?;
    counters.ephemeral_open_go.store(open_go, Ordering::Relaxed);
    counters
        .ephemeral_open_rust
        .store(open_rust, Ordering::Relaxed);
    counters
        .ephemeral_closed_go
        .store(closed_go, Ordering::Relaxed);
    counters
        .ephemeral_closed_rust
        .store(closed_rust, Ordering::Relaxed);

    for steady in [TENANT_PROD, TENANT_CANARY] {
        let gs = go_http_map.get(steady).map(|t| t.series).unwrap_or(0);
        let rs = rust_http_map.get(steady).map(|t| t.series).unwrap_or(0);
        ensure!(
            gs > 0 && rs > 0,
            "/ingester/tenants: steady tenant {steady} has 0 series (go={gs}, rust={rs})"
        );
    }

    // SearchLabelNames
    let sln_req = || {
        with_org(
            TENANT_PROD,
            ingesterpb::SearchLabelNamesRequest {
                start_timestamp_ms: start_ms,
                end_timestamp_ms: end_ms,
                matchers: vec![eq_matcher("__name__", "soak_http_requests_total_0000")],
                filter: Some(ingesterpb::SearchFilter {
                    terms: vec!["status".to_string()],
                    case_insensitive: true,
                    fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                    fuzz_threshold: 0,
                }),
                ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
                limit: 50,
            },
        )
    };
    let (go_sln, rust_sln) = tokio::join!(
        g_cli.search_label_names(sln_req()),
        r_cli.search_label_names(sln_req())
    );
    let mut go_s = go_sln?.into_inner();
    let mut rust_s = rust_sln?.into_inner();
    let mut go_res = Vec::new();
    while let Some(b) = go_s.next().await {
        for r in b?.results {
            go_res.push(r.value);
        }
    }
    let mut rust_res = Vec::new();
    while let Some(b) = rust_s.next().await {
        for r in b?.results {
            rust_res.push(r.value);
        }
    }
    ensure!(
        go_res == rust_res,
        "SearchLabelNames mismatch: {go_res:?} vs {rust_res:?}"
    );

    // Health/Check & httpgrpc.HTTP/Handle
    let (gh, rh) = tokio::join!(rpc_health_check(go_ch.clone()), rpc_health_check(rust_ch.clone()));
    ensure!(gh? == 1 && rh? == 1, "Health/Check not SERVING");
    let (ghg, rhg) = tokio::join!(rpc_httpgrpc_ready(go_ch), rpc_httpgrpc_ready(rust_ch));
    ensure!(ghg? == 200 && rhg? == 200, "httpgrpc /ready not 200");

    Ok(())
}

// ---------------------------------------------------------------------------
// Anomaly Detector & CSV / Summary Reporting
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct RssPoint {
    elapsed_secs: f64,
    rss_anon_mib: f64,
    compactions_ok: u64,
}

#[derive(Clone, Debug, Default)]
struct RssTrend {
    samples: usize,
    start_mib: f64,
    end_mib: f64,
    slope_mib_per_hr: f64,
    r2: f64,
    max_monotonic_streak: usize,
    window_secs: f64,
    cycle_troughs_mib: Vec<f64>,
    trough_monotonic_run: usize,
    trough_monotonic_gain_mib: f64,
    sawtooth_leak_flagged: bool,
    short_run_flagged: bool,
    flagged: bool,
}

fn analyze_rss_trend(
    all_pts: &[RssPoint],
    warmup_secs: u64,
    retention_secs: f64,
    duration_secs: u64,
    strict: bool,
) -> RssTrend {
    let pts: Vec<RssPoint> = all_pts
        .iter()
        .copied()
        .filter(|p| p.elapsed_secs >= warmup_secs as f64)
        .collect();
    let mut out = RssTrend {
        samples: pts.len(),
        ..Default::default()
    };
    if pts.len() < 4 {
        return out;
    }
    out.start_mib = pts.first().map(|p| p.rss_anon_mib).unwrap_or(0.0);
    out.end_mib = pts.last().map(|p| p.rss_anon_mib).unwrap_or(0.0);

    let mut streak = 0usize;
    let mut max_streak = 0usize;
    for w in pts.windows(2) {
        if w[1].rss_anon_mib > w[0].rss_anon_mib {
            streak += 1;
            max_streak = max_streak.max(streak);
        } else {
            streak = 0;
        }
    }
    out.max_monotonic_streak = max_streak;

    let n = pts.len() as f64;
    let mean_x = pts.iter().map(|p| p.elapsed_secs).sum::<f64>() / n;
    let mean_y = pts.iter().map(|p| p.rss_anon_mib).sum::<f64>() / n;
    let mut sxx = 0.0;
    let mut syy = 0.0;
    let mut sxy = 0.0;
    for p in &pts {
        let dx = p.elapsed_secs - mean_x;
        let dy = p.rss_anon_mib - mean_y;
        sxx += dx * dx;
        syy += dy * dy;
        sxy += dx * dy;
    }
    if sxx > 0.0 {
        let slope_per_sec = sxy / sxx;
        out.slope_mib_per_hr = slope_per_sec * 3600.0;
        if syy > 0.0 {
            out.r2 = (sxy * sxy) / (sxx * syy);
        }
    }

    let window_secs = pts.last().unwrap().elapsed_secs - pts.first().unwrap().elapsed_secs;
    out.window_secs = window_secs;

    // Post-retention compaction-cycle troughs (2h compaction windows after retention plateau).
    // Grouping by 2h (7200s) windows prevents ephemeral-tenant compactions from slicing an
    // active 2h climb of the main TSDB into fake sub-cycles.
    let plateau_start = retention_secs.max(warmup_secs as f64);
    #[derive(Clone, Copy)]
    struct WinAcc {
        min_rss: f64,
        first_t: f64,
        last_t: f64,
        first_comp: u64,
        last_comp: u64,
        count: usize,
    }
    let mut windows: BTreeMap<usize, WinAcc> = BTreeMap::new();
    for p in all_pts.iter().filter(|p| p.elapsed_secs >= plateau_start) {
        let widx = ((p.elapsed_secs - plateau_start) / 7200.0).floor() as usize;
        windows
            .entry(widx)
            .and_modify(|w| {
                w.min_rss = w.min_rss.min(p.rss_anon_mib);
                w.last_t = p.elapsed_secs;
                w.last_comp = p.compactions_ok;
                w.count += 1;
            })
            .or_insert(WinAcc {
                min_rss: p.rss_anon_mib,
                first_t: p.elapsed_secs,
                last_t: p.elapsed_secs,
                first_comp: p.compactions_ok,
                last_comp: p.compactions_ok,
                count: 1,
            });
    }
    let max_widx = windows.keys().copied().max();
    for (widx, w) in &windows {
        let is_closed_window = max_widx.is_some_and(|m| *widx < m) && w.count >= 2;
        let span = w.last_t - w.first_t;
        let saw_compaction_and_span = w.last_comp > w.first_comp && span >= 3600.0;
        if is_closed_window || span >= 5400.0 || saw_compaction_and_span {
            out.cycle_troughs_mib.push(w.min_rss);
        }
    }

    if !out.cycle_troughs_mib.is_empty() {
        let mut run = 1usize;
        let mut run_start = out.cycle_troughs_mib[0];
        for i in 1..out.cycle_troughs_mib.len() {
            if out.cycle_troughs_mib[i] > out.cycle_troughs_mib[i - 1] {
                run += 1;
            } else {
                run = 1;
                run_start = out.cycle_troughs_mib[i];
            }
        }
        let last_trough = *out.cycle_troughs_mib.last().unwrap();
        out.trough_monotonic_run = run;
        out.trough_monotonic_gain_mib = if run >= 2 { last_trough - run_start } else { 0.0 };
        let trough_thresh = (run_start * 0.05).max(16.0);
        if run >= 3 && out.trough_monotonic_gain_mib > trough_thresh {
            out.sawtooth_leak_flagged = true;
        }
    }

    if duration_secs >= SAWTOOTH_LONG_RUN_SECS {
        out.flagged = out.sawtooth_leak_flagged;
    } else if strict {
        let total_drift_mib = (out.slope_mib_per_hr / 3600.0) * window_secs;
        let growth_threshold_mib = (out.start_mib * 0.15).max(32.0);
        let strictly_monotonic =
            max_streak + 1 == pts.len() && (out.end_mib - out.start_mib) > growth_threshold_mib;
        let strong_linear_leak =
            out.slope_mib_per_hr > 0.0 && out.r2 >= 0.85 && total_drift_mib > growth_threshold_mib;
        out.short_run_flagged = strictly_monotonic || strong_linear_leak;
        out.flagged = out.short_run_flagged;
    }
    out
}

#[derive(Clone, Debug, Default)]
struct EngineAnomalyState {
    baseline_open_fds: Option<u64>,
    consecutive_deleted_mmaps: u64,
    consecutive_fd_leak: u64,
    latched_deleted_mmap: Option<String>,
    latched_fd_leak: Option<String>,
}

impl EngineAnomalyState {
    fn observe(&mut self, name: &str, elapsed_secs: u64, settle_after_secs: u64, s: &EngineSample) {
        if s.alive && s.proc_stats.open_fds > 0 {
            if self.baseline_open_fds.is_none() && elapsed_secs >= settle_after_secs {
                self.baseline_open_fds = Some(s.proc_stats.open_fds);
            }
            if let Some(base) = self.baseline_open_fds {
                if s.proc_stats.open_fds > base.saturating_add(64) {
                    self.consecutive_fd_leak += 1;
                    if self.consecutive_fd_leak >= 2 && self.latched_fd_leak.is_none() {
                        self.latched_fd_leak = Some(format!(
                            "{name}: FD leak (open_fds={} > baseline {} + 64 across {} consecutive ticks)",
                            s.proc_stats.open_fds, base, self.consecutive_fd_leak
                        ));
                    }
                } else {
                    self.consecutive_fd_leak = 0;
                }
            }
        }

        if s.proc_stats.deleted_mmap_count > 0 {
            self.consecutive_deleted_mmaps += 1;
            if self.consecutive_deleted_mmaps >= 2 && self.latched_deleted_mmap.is_none() {
                self.latched_deleted_mmap = Some(format!(
                    "{name}: {} persistent (deleted) mmaps across {} consecutive ticks in /proc/{}/maps: {:?}",
                    s.proc_stats.deleted_mmap_count,
                    self.consecutive_deleted_mmaps,
                    s.proc_stats.pid,
                    s.proc_stats.deleted_mmap_lines
                ));
            }
        } else {
            self.consecutive_deleted_mmaps = 0;
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AnomalyTracker {
    go: EngineAnomalyState,
    rust: EngineAnomalyState,
}

impl AnomalyTracker {
    fn observe(&mut self, elapsed_secs: u64, duration_secs: u64, go: &EngineSample, rust: &EngineSample) {
        let settle_after = 30.min(duration_secs / 3);
        self.go.observe("Go", elapsed_secs, settle_after, go);
        self.rust.observe("Rust", elapsed_secs, settle_after, rust);
    }
}

fn detect_anomalies(
    cfg: &SoakConfig,
    go: &EngineSample,
    rust: &EngineSample,
    counters: &WorkloadCounters,
    go_trend: &RssTrend,
    rust_trend: &RssTrend,
    tracker: &AnomalyTracker,
) -> Vec<String> {
    let mut anomalies = Vec::new();
    for (name, s, state, trend) in [
        ("Go", go, &tracker.go, go_trend),
        ("Rust", rust, &tracker.rust, rust_trend),
    ] {
        if !s.alive {
            anomalies.push(format!("{name}: container not running"));
        }
        if s.metrics.compactions_failed > 0.0 {
            anomalies.push(format!(
                "{name}: compactions_failed = {}",
                s.metrics.compactions_failed
            ));
        }
        if s.metrics.wal_corruptions > 0.0 {
            anomalies.push(format!(
                "{name}: wal_corruptions = {}",
                s.metrics.wal_corruptions
            ));
        }
        if s.metrics.shipper_upload_failures > 0.0 {
            anomalies.push(format!(
                "{name}: shipper_upload_failures = {}",
                s.metrics.shipper_upload_failures
            ));
        }
        if s.metrics.objstore_failures > 0.0 {
            anomalies.push(format!(
                "{name}: thanos_objstore_bucket_operation_failures_total = {}",
                s.metrics.objstore_failures
            ));
        }
        if s.metrics.request_duration_errors > 0.0 {
            anomalies.push(format!(
                "{name}: cortex_request_duration_seconds errors = {}",
                s.metrics.request_duration_errors
            ));
        }
        if let Some(ref msg) = state.latched_deleted_mmap {
            anomalies.push(msg.clone());
        }
        if let Some(ref msg) = state.latched_fd_leak {
            anomalies.push(msg.clone());
        }
        if cfg.duration_secs < SAWTOOTH_LONG_RUN_SECS && trend.short_run_flagged {
            anomalies.push(format!(
                "{name}: strict monotonic RssAnon growth post-warmup ({:.1} -> {:.1} MiB, slope={:+.2} MiB/hr, R^2={:.3})",
                trend.start_mib, trend.end_mib, trend.slope_mib_per_hr, trend.r2
            ));
        }
    }

    if cfg.duration_secs >= SAWTOOTH_LONG_RUN_SECS {
        if rust_trend.sawtooth_leak_flagged {
            anomalies.push(format!(
                "Rust: post-retention compaction-cycle RssAnon trough grew monotonically across {} cycles (+{:.1} MiB, troughs={:?})",
                rust_trend.trough_monotonic_run,
                rust_trend.trough_monotonic_gain_mib,
                rust_trend.cycle_troughs_mib,
            ));
        } else if rust_trend.window_secs >= SAWTOOTH_LONG_RUN_SECS as f64
            && go_trend.window_secs >= SAWTOOTH_LONG_RUN_SECS as f64
            && go.proc_stats.rss_anon_bytes > 0
            && rust.proc_stats.rss_anon_bytes > go.proc_stats.rss_anon_bytes
            && (rust_trend.slope_mib_per_hr - go_trend.slope_mib_per_hr) > 25.0
        {
            let ratio = rust.proc_stats.rss_anon_bytes as f64 / go.proc_stats.rss_anon_bytes as f64;
            anomalies.push(format!(
                "Rust: RssAnon slope {:+.2} MiB/hr exceeds Go {:+.2} MiB/hr by >25 MiB/hr over >=4h window while RUST/GO={ratio:.2}x > 1.0",
                rust_trend.slope_mib_per_hr, go_trend.slope_mib_per_hr
            ));
        }
    }

    let rpc_err = counters.rpc_err_total.load(Ordering::Relaxed);
    if rpc_err > 0 {
        let last = counters.last_rpc_error.lock().clone().unwrap_or_default();
        anomalies.push(format!("client rpc_err_total = {rpc_err} (last: {last})"));
    }
    let parity_err = counters.parity_failures.load(Ordering::Relaxed);
    if parity_err > 0 {
        let last = counters.last_parity_error.lock().clone().unwrap_or_default();
        anomalies.push(format!(
            "parity_failures = {parity_err} (last: {last})"
        ));
    }

    anomalies
}

fn csv_header() -> String {
    let mut cols = vec![
        "timestamp_iso",
        "elapsed_secs",
        "produced_records",
        "produced_samples",
        "produced_histograms",
        "produced_exemplars",
        "produced_ooo",
        "produced_churn_series",
        "kafka_last_offset",
        "rpc_ok_total",
        "rpc_err_total",
        "parity_checks",
        "parity_failures",
        "ephemeral_tenants_created",
        "ephemeral_open_go",
        "ephemeral_open_rust",
        "ephemeral_closed_go",
        "ephemeral_closed_rust",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();

    for side in ["go", "rust"] {
        for suffix in [
            "cgroup_mem_current_bytes",
            "cgroup_anon_bytes",
            "cgroup_active_file_bytes",
            "cgroup_inactive_file_bytes",
            "cgroup_working_set_bytes",
            "proc_vm_rss_bytes",
            "proc_rss_anon_bytes",
            "proc_rss_file_bytes",
            "proc_vm_data_bytes",
            "proc_open_fds",
            "proc_mmap_count",
            "proc_deleted_mmaps",
            "proc_cpu_secs",
            "active_series",
            "head_series",
            "head_chunks",
            "compactions_total",
            "compactions_failed",
            "wal_corruptions",
            "shipped_blocks",
            "shipper_upload_failures",
            "objstore_uploads",
            "objstore_failures",
            "request_duration_count",
            "request_duration_errors",
            "queried_series_count",
            "queried_series_sum",
            "last_consumed_offset",
        ] {
            cols.push(format!("{side}_{suffix}"));
        }
    }
    cols.join(",")
}

fn append_engine_csv(row: &mut String, s: &EngineSample) {
    let _ = write!(
        row,
        ",{},{},{},{},{},{},{},{},{},{},{},{},{:.4},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{}",
        s.cgroup.current,
        s.cgroup.anon,
        s.cgroup.active_file,
        s.cgroup.inactive_file,
        s.cgroup.working_set,
        s.proc_stats.vm_rss_bytes,
        s.proc_stats.rss_anon_bytes,
        s.proc_stats.rss_file_bytes,
        s.proc_stats.vm_data_bytes,
        s.proc_stats.open_fds,
        s.proc_stats.mmap_count,
        s.proc_stats.deleted_mmap_count,
        s.proc_stats.cpu_secs,
        s.metrics.active_series,
        s.metrics.head_series,
        s.metrics.head_chunks,
        s.metrics.compactions_total,
        s.metrics.compactions_failed,
        s.metrics.wal_corruptions,
        s.metrics.shipped_blocks,
        s.metrics.shipper_upload_failures,
        s.metrics.objstore_uploads,
        s.metrics.objstore_failures,
        s.metrics.request_duration_count,
        s.metrics.request_duration_errors,
        s.metrics.queried_series_count,
        s.metrics.queried_series_sum,
        s.metrics.last_consumed_offset,
    );
}

#[allow(clippy::too_many_arguments)]
fn write_summary_file(
    out_dir: &Path,
    cfg: &SoakConfig,
    now_iso: &str,
    elapsed_secs: u64,
    go: &EngineSample,
    rust: &EngineSample,
    counters: &WorkloadCounters,
    go_trend: &RssTrend,
    rust_trend: &RssTrend,
    anomalies: &[String],
) -> Result<()> {
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    let mut s = String::new();
    let _ = writeln!(s, "=== Mimir Go vs Rust Soak Summary ({now_iso}) ===");
    let _ = writeln!(
        s,
        "elapsed={}s / {}s  warmup={}s  series={}  rate={}/s  churn={}/min  ooo={:.2}%  query_rps={}  retention={}",
        elapsed_secs,
        cfg.duration_secs,
        cfg.effective_warmup_secs(),
        cfg.series,
        cfg.rate_samples_per_sec,
        cfg.churn_rate_per_min,
        cfg.ooo_rate * 100.0,
        cfg.query_rps,
        cfg.retention_period
    );
    let _ = writeln!(
        s,
        "workload: records={} samples={} histograms={} exemplars={} ooo={} churn_series={} ephemeral(created={} open={}/{} closed={}/{}) kafka_offset={} rpc_ok={} rpc_err={} parity_ok={} parity_err={}",
        counters.produced_records.load(Ordering::Relaxed),
        counters.produced_samples.load(Ordering::Relaxed),
        counters.produced_histograms.load(Ordering::Relaxed),
        counters.produced_exemplars.load(Ordering::Relaxed),
        counters.produced_ooo.load(Ordering::Relaxed),
        counters.produced_churn_series.load(Ordering::Relaxed),
        counters.ephemeral_tenants_created.load(Ordering::Relaxed),
        counters.ephemeral_open_go.load(Ordering::Relaxed),
        counters.ephemeral_open_rust.load(Ordering::Relaxed),
        counters.ephemeral_closed_go.load(Ordering::Relaxed),
        counters.ephemeral_closed_rust.load(Ordering::Relaxed),
        counters.kafka_last_offset.load(Ordering::Relaxed),
        counters.rpc_ok_total.load(Ordering::Relaxed),
        counters.rpc_err_total.load(Ordering::Relaxed),
        counters.parity_checks.load(Ordering::Relaxed),
        counters.parity_failures.load(Ordering::Relaxed),
    );
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "{:<26} {:>14} {:>14} {:>12}",
        "METRIC", "GO (zone-a-0)", "RUST (zone-b-0)", "RUST/GO"
    );
    let ratio = |r: f64, g: f64| -> String {
        if g > 0.0 {
            format!("{:.2}x", r / g)
        } else {
            "-".to_string()
        }
    };
    let rows = [
        (
            "cgroup working_set (MiB)",
            format!("{:.1}", mib(go.cgroup.working_set)),
            format!("{:.1}", mib(rust.cgroup.working_set)),
            ratio(rust.cgroup.working_set as f64, go.cgroup.working_set as f64),
        ),
        (
            "cgroup mem.current (MiB)",
            format!("{:.1}", mib(go.cgroup.current)),
            format!("{:.1}", mib(rust.cgroup.current)),
            ratio(rust.cgroup.current as f64, go.cgroup.current as f64),
        ),
        (
            "cgroup anon (MiB)",
            format!("{:.1}", mib(go.cgroup.anon)),
            format!("{:.1}", mib(rust.cgroup.anon)),
            ratio(rust.cgroup.anon as f64, go.cgroup.anon as f64),
        ),
        (
            "cgroup active_file (MiB)",
            format!("{:.1}", mib(go.cgroup.active_file)),
            format!("{:.1}", mib(rust.cgroup.active_file)),
            "-".to_string(),
        ),
        (
            "cgroup inactive_file (MiB)",
            format!("{:.1}", mib(go.cgroup.inactive_file)),
            format!("{:.1}", mib(rust.cgroup.inactive_file)),
            "-".to_string(),
        ),
        (
            "proc VmRSS (MiB)",
            format!("{:.1}", mib(go.proc_stats.vm_rss_bytes)),
            format!("{:.1}", mib(rust.proc_stats.vm_rss_bytes)),
            ratio(rust.proc_stats.vm_rss_bytes as f64, go.proc_stats.vm_rss_bytes as f64),
        ),
        (
            "proc RssAnon (MiB)",
            format!("{:.1}", mib(go.proc_stats.rss_anon_bytes)),
            format!("{:.1}", mib(rust.proc_stats.rss_anon_bytes)),
            ratio(rust.proc_stats.rss_anon_bytes as f64, go.proc_stats.rss_anon_bytes as f64),
        ),
        (
            "proc RssFile (MiB)",
            format!("{:.1}", mib(go.proc_stats.rss_file_bytes)),
            format!("{:.1}", mib(rust.proc_stats.rss_file_bytes)),
            "-".to_string(),
        ),
        (
            "proc VmData (MiB)",
            format!("{:.1}", mib(go.proc_stats.vm_data_bytes)),
            format!("{:.1}", mib(rust.proc_stats.vm_data_bytes)),
            ratio(rust.proc_stats.vm_data_bytes as f64, go.proc_stats.vm_data_bytes as f64),
        ),
        (
            "proc open FDs",
            format!("{}", go.proc_stats.open_fds),
            format!("{}", rust.proc_stats.open_fds),
            "-".to_string(),
        ),
        (
            "proc mmaps (deleted)",
            format!("{} ({})", go.proc_stats.mmap_count, go.proc_stats.deleted_mmap_count),
            format!("{} ({})", rust.proc_stats.mmap_count, rust.proc_stats.deleted_mmap_count),
            "-".to_string(),
        ),
        (
            "CPU user+sys (s)",
            format!("{:.2}", go.proc_stats.cpu_secs),
            format!("{:.2}", rust.proc_stats.cpu_secs),
            ratio(rust.proc_stats.cpu_secs, go.proc_stats.cpu_secs),
        ),
        (
            "active_series",
            format!("{:.0}", go.metrics.active_series),
            format!("{:.0}", rust.metrics.active_series),
            "-".to_string(),
        ),
        (
            "head_series",
            format!("{:.0}", go.metrics.head_series),
            format!("{:.0}", rust.metrics.head_series),
            "-".to_string(),
        ),
        (
            "head_chunks",
            format!("{:.0}", go.metrics.head_chunks),
            format!("{:.0}", rust.metrics.head_chunks),
            "-".to_string(),
        ),
        (
            "compactions (ok/fail)",
            format!("{:.0}/{:.0}", go.metrics.compactions_total, go.metrics.compactions_failed),
            format!("{:.0}/{:.0}", rust.metrics.compactions_total, rust.metrics.compactions_failed),
            "-".to_string(),
        ),
        (
            "wal_corruptions",
            format!("{:.0}", go.metrics.wal_corruptions),
            format!("{:.0}", rust.metrics.wal_corruptions),
            "-".to_string(),
        ),
        (
            "shipped_blocks",
            format!("{:.0}", go.metrics.shipped_blocks),
            format!("{:.0}", rust.metrics.shipped_blocks),
            "-".to_string(),
        ),
        (
            "objstore uploads/fail",
            format!("{:.0}/{:.0}", go.metrics.objstore_uploads, go.metrics.objstore_failures),
            format!("{:.0}/{:.0}", rust.metrics.objstore_uploads, rust.metrics.objstore_failures),
            "-".to_string(),
        ),
        (
            "request_duration cnt/err",
            format!("{:.0}/{:.0}", go.metrics.request_duration_count, go.metrics.request_duration_errors),
            format!("{:.0}/{:.0}", rust.metrics.request_duration_count, rust.metrics.request_duration_errors),
            "-".to_string(),
        ),
        (
            "queried_series (cnt/sum)",
            format!("{:.0}/{:.0}", go.metrics.queried_series_count, go.metrics.queried_series_sum),
            format!("{:.0}/{:.0}", rust.metrics.queried_series_count, rust.metrics.queried_series_sum),
            "-".to_string(),
        ),
        (
            "last_consumed_offset",
            format!("{}", go.metrics.last_consumed_offset),
            format!("{}", rust.metrics.last_consumed_offset),
            "-".to_string(),
        ),
    ];
    for (k, g, r, rat) in rows {
        let _ = writeln!(s, "{k:<26} {g:>14} {r:>14} {rat:>12}");
    }
    let _ = writeln!(s);
    let _ = writeln!(
        s,
        "post-warmup RssAnon trend: GO(n={}, {:.1}->{:.1} MiB, slope={:+.2} MiB/hr, R2={:.3}, streak={}, cycles={}, trough_run={})  RUST(n={}, {:.1}->{:.1} MiB, slope={:+.2} MiB/hr, R2={:.3}, streak={}, cycles={}, trough_run={})",
        go_trend.samples,
        go_trend.start_mib,
        go_trend.end_mib,
        go_trend.slope_mib_per_hr,
        go_trend.r2,
        go_trend.max_monotonic_streak,
        go_trend.cycle_troughs_mib.len(),
        go_trend.trough_monotonic_run,
        rust_trend.samples,
        rust_trend.start_mib,
        rust_trend.end_mib,
        rust_trend.slope_mib_per_hr,
        rust_trend.r2,
        rust_trend.max_monotonic_streak,
        rust_trend.cycle_troughs_mib.len(),
        rust_trend.trough_monotonic_run,
    );
    if anomalies.is_empty() {
        let _ = writeln!(s, "ANOMALY DETECTOR: OK (0 anomalies)");
    } else {
        let _ = writeln!(s, "ANOMALY DETECTOR: {} ANOMALIES:", anomalies.len());
        for a in anomalies {
            let _ = writeln!(s, "  - {a}");
        }
    }

    let tmp = out_dir.join("latest-summary.txt.tmp");
    fs::write(&tmp, s.as_bytes())?;
    fs::rename(&tmp, out_dir.join("latest-summary.txt"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let Some(cfg) = parse_args()? else {
        return Ok(());
    };

    if !cfg.skip_build {
        eprintln!("[soak] building target/go-mimir + target/release/mimir...");
        ensure_go_oracle_built().context("build go-mimir")?;
        ensure_rust_mimir_release_built().context("build release mimir")?;
    }

    fs::create_dir_all(&cfg.out_dir)?;
    let csv_path = cfg.out_dir.join("telemetry.csv");
    let csv_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&csv_path)?;
    let mut csv_writer = BufWriter::new(csv_file);
    writeln!(csv_writer, "{}", csv_header())?;
    csv_writer.flush()?;

    let helm = HelmConfig::load(&cfg)?;
    eprintln!(
        "[soak] starting Redpanda + Silo + Go ({}) + Rust ({})...",
        Side::Go.host(),
        Side::Rust.host()
    );
    let mut cluster = SoakCluster::start(&cfg, helm).await?;
    let http = reqwest::Client::new();
    cluster
        .wait_ingesters_ready(&http, Duration::from_secs(120))
        .await?;
    eprintln!(
        "[soak] cluster ready! go=127.0.0.1:{} (gRPC {}) rust=127.0.0.1:{} (gRPC {}) out_dir={}",
        cluster.go.http_port,
        cluster.go.grpc_port,
        cluster.rust.http_port,
        cluster.rust.grpc_port,
        cfg.out_dir.display()
    );

    let go_ch = connect_grpc(cluster.go.grpc_port).await?;
    let rust_ch = connect_grpc(cluster.rust.grpc_port).await?;

    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(WorkloadCounters::new());

    let producer_handle = tokio::spawn(run_producer_loop(
        cfg.clone(),
        format!("127.0.0.1:{}", cluster.kafka_ext_port),
        cluster.helm.topic.clone(),
        Arc::clone(&counters),
        Arc::clone(&stop),
    ));

    // Give producer a short head start before read/parity probes start hitting ingesters.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let reader_handle = tokio::spawn(run_reader_loop(
        cfg.clone(),
        go_ch.clone(),
        rust_ch.clone(),
        Arc::clone(&counters),
        Arc::clone(&stop),
    ));

    let start = Instant::now();
    let total_dur = Duration::from_secs(cfg.duration_secs);
    let sample_dur = Duration::from_secs(cfg.sample_secs);
    let parity_dur = Duration::from_secs(cfg.parity_interval_secs);
    let warmup_secs = cfg.effective_warmup_secs();

    let mut sample_tick = tokio::time::interval(sample_dur);
    sample_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut parity_tick = tokio::time::interval(parity_dur);
    parity_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let retention_secs = cfg.retention_secs();
    let mut go_rss_pts: Vec<RssPoint> = Vec::new();
    let mut rust_rss_pts: Vec<RssPoint> = Vec::new();
    let mut anomaly_tracker = AnomalyTracker::default();

    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sighup =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

    loop {
        if start.elapsed() >= total_dur {
            break;
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("[soak] received SIGINT, stopping cleanly...");
                break;
            }
            _ = sigterm.recv() => {
                eprintln!("[soak] received SIGTERM, stopping cleanly...");
                break;
            }
            _ = sighup.recv() => {
                eprintln!("[soak] received SIGHUP, stopping cleanly...");
                break;
            }
            _ = parity_tick.tick() => {
                if let Err(e) = run_parity_sweep(
                    &http,
                    cluster.go.http_port,
                    cluster.rust.http_port,
                    go_ch.clone(),
                    rust_ch.clone(),
                    &cfg,
                    &counters,
                ).await {
                    counters.record_parity_err(format!("{e:#}"));
                    eprintln!("[soak] PARITY FAILURE: {e:#}");
                } else {
                    counters.parity_checks.fetch_add(1, Ordering::Relaxed);
                }
            }
            _ = sample_tick.tick() => {
                let elapsed = start.elapsed().as_secs();
                let (go_s, rust_s) = tokio::join!(
                    collect_engine_sample(&http, &cluster.go),
                    collect_engine_sample(&http, &cluster.rust)
                );
                let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
                go_rss_pts.push(RssPoint {
                    elapsed_secs: elapsed as f64,
                    rss_anon_mib: mib(go_s.proc_stats.rss_anon_bytes),
                    compactions_ok: go_s.metrics.compactions_total.max(0.0) as u64,
                });
                rust_rss_pts.push(RssPoint {
                    elapsed_secs: elapsed as f64,
                    rss_anon_mib: mib(rust_s.proc_stats.rss_anon_bytes),
                    compactions_ok: rust_s.metrics.compactions_total.max(0.0) as u64,
                });
                anomaly_tracker.observe(elapsed, cfg.duration_secs, &go_s, &rust_s);
                let go_trend = analyze_rss_trend(
                    &go_rss_pts,
                    warmup_secs,
                    retention_secs,
                    cfg.duration_secs,
                    cfg.strict_rss_trend,
                );
                let rust_trend = analyze_rss_trend(
                    &rust_rss_pts,
                    warmup_secs,
                    retention_secs,
                    cfg.duration_secs,
                    cfg.strict_rss_trend,
                );
                let anomalies = detect_anomalies(
                    &cfg,
                    &go_s,
                    &rust_s,
                    &counters,
                    &go_trend,
                    &rust_trend,
                    &anomaly_tracker,
                );

                let now_iso = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                let mut row = format!(
                    "{now_iso},{elapsed},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                    counters.produced_records.load(Ordering::Relaxed),
                    counters.produced_samples.load(Ordering::Relaxed),
                    counters.produced_histograms.load(Ordering::Relaxed),
                    counters.produced_exemplars.load(Ordering::Relaxed),
                    counters.produced_ooo.load(Ordering::Relaxed),
                    counters.produced_churn_series.load(Ordering::Relaxed),
                    counters.kafka_last_offset.load(Ordering::Relaxed),
                    counters.rpc_ok_total.load(Ordering::Relaxed),
                    counters.rpc_err_total.load(Ordering::Relaxed),
                    counters.parity_checks.load(Ordering::Relaxed),
                    counters.parity_failures.load(Ordering::Relaxed),
                    counters.ephemeral_tenants_created.load(Ordering::Relaxed),
                    counters.ephemeral_open_go.load(Ordering::Relaxed),
                    counters.ephemeral_open_rust.load(Ordering::Relaxed),
                    counters.ephemeral_closed_go.load(Ordering::Relaxed),
                    counters.ephemeral_closed_rust.load(Ordering::Relaxed),
                );
                append_engine_csv(&mut row, &go_s);
                append_engine_csv(&mut row, &rust_s);
                writeln!(csv_writer, "{row}")?;
                csv_writer.flush()?;

                write_summary_file(
                    &cfg.out_dir,
                    &cfg,
                    &now_iso,
                    elapsed,
                    &go_s,
                    &rust_s,
                    &counters,
                    &go_trend,
                    &rust_trend,
                    &anomalies,
                )?;

                eprintln!(
                    "[soak t={elapsed}s/{}s] GO(ws={:.1}MiB anon={:.1}MiB cpu={:.1}s series={:.0}) RUST(ws={:.1}MiB anon={:.1}MiB cpu={:.1}s series={:.0}) eph={}/{}/{} rpc_ok={} rpc_err={} parity={}/{} anomalies={}",
                    cfg.duration_secs,
                    go_s.cgroup.working_set as f64 / (1024.0 * 1024.0),
                    go_s.proc_stats.rss_anon_bytes as f64 / (1024.0 * 1024.0),
                    go_s.proc_stats.cpu_secs,
                    go_s.metrics.head_series,
                    rust_s.cgroup.working_set as f64 / (1024.0 * 1024.0),
                    rust_s.proc_stats.rss_anon_bytes as f64 / (1024.0 * 1024.0),
                    rust_s.proc_stats.cpu_secs,
                    rust_s.metrics.head_series,
                    counters.ephemeral_tenants_created.load(Ordering::Relaxed),
                    counters.ephemeral_open_go.load(Ordering::Relaxed),
                    counters.ephemeral_open_rust.load(Ordering::Relaxed),
                    counters.rpc_ok_total.load(Ordering::Relaxed),
                    counters.rpc_err_total.load(Ordering::Relaxed),
                    counters.parity_checks.load(Ordering::Relaxed),
                    counters.parity_failures.load(Ordering::Relaxed),
                    anomalies.len(),
                );
            }
        }
    }

    stop.store(true, Ordering::Release);
    let _ = tokio::time::timeout(Duration::from_secs(10), producer_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), reader_handle).await;

    // Final sample + summary (before container teardown; both containers must still be alive)
    let elapsed = start.elapsed().as_secs();
    let (last_go, last_rust) = tokio::join!(
        collect_engine_sample(&http, &cluster.go),
        collect_engine_sample(&http, &cluster.rust)
    );
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);
    go_rss_pts.push(RssPoint {
        elapsed_secs: elapsed as f64,
        rss_anon_mib: mib(last_go.proc_stats.rss_anon_bytes),
        compactions_ok: last_go.metrics.compactions_total.max(0.0) as u64,
    });
    rust_rss_pts.push(RssPoint {
        elapsed_secs: elapsed as f64,
        rss_anon_mib: mib(last_rust.proc_stats.rss_anon_bytes),
        compactions_ok: last_rust.metrics.compactions_total.max(0.0) as u64,
    });
    anomaly_tracker.observe(elapsed, cfg.duration_secs, &last_go, &last_rust);
    let go_trend = analyze_rss_trend(
        &go_rss_pts,
        warmup_secs,
        retention_secs,
        cfg.duration_secs,
        cfg.strict_rss_trend,
    );
    let rust_trend = analyze_rss_trend(
        &rust_rss_pts,
        warmup_secs,
        retention_secs,
        cfg.duration_secs,
        cfg.strict_rss_trend,
    );
    let last_anomalies = detect_anomalies(
        &cfg,
        &last_go,
        &last_rust,
        &counters,
        &go_trend,
        &rust_trend,
        &anomaly_tracker,
    );
    let now_iso = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let mut final_row = format!(
        "{now_iso},{elapsed},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        counters.produced_records.load(Ordering::Relaxed),
        counters.produced_samples.load(Ordering::Relaxed),
        counters.produced_histograms.load(Ordering::Relaxed),
        counters.produced_exemplars.load(Ordering::Relaxed),
        counters.produced_ooo.load(Ordering::Relaxed),
        counters.produced_churn_series.load(Ordering::Relaxed),
        counters.kafka_last_offset.load(Ordering::Relaxed),
        counters.rpc_ok_total.load(Ordering::Relaxed),
        counters.rpc_err_total.load(Ordering::Relaxed),
        counters.parity_checks.load(Ordering::Relaxed),
        counters.parity_failures.load(Ordering::Relaxed),
        counters.ephemeral_tenants_created.load(Ordering::Relaxed),
        counters.ephemeral_open_go.load(Ordering::Relaxed),
        counters.ephemeral_open_rust.load(Ordering::Relaxed),
        counters.ephemeral_closed_go.load(Ordering::Relaxed),
        counters.ephemeral_closed_rust.load(Ordering::Relaxed),
    );
    append_engine_csv(&mut final_row, &last_go);
    append_engine_csv(&mut final_row, &last_rust);
    writeln!(csv_writer, "{final_row}")?;
    csv_writer.flush()?;

    write_summary_file(
        &cfg.out_dir,
        &cfg,
        &now_iso,
        elapsed,
        &last_go,
        &last_rust,
        &counters,
        &go_trend,
        &rust_trend,
        &last_anomalies,
    )?;

    eprintln!("[soak] tearing down containers...");
    cluster.teardown().await;

    let summary_txt = fs::read_to_string(cfg.out_dir.join("latest-summary.txt")).unwrap_or_default();
    println!("{summary_txt}");

    if !last_anomalies.is_empty() {
        bail!("soak detected {} anomalies: {:?}", last_anomalies.len(), last_anomalies);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_sample(alive: bool, rss_mib: f64, open_fds: u64, deleted_mmaps: u64) -> EngineSample {
        EngineSample {
            alive,
            proc_stats: ProcPidStats {
                pid: 123,
                rss_anon_bytes: (rss_mib * 1024.0 * 1024.0) as u64,
                open_fds,
                deleted_mmap_count: deleted_mmaps,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn sawtooth_climb_before_first_compaction_does_not_flag() {
        let mut pts = Vec::new();
        for m in 0..=110 {
            let t = (m * 60) as f64;
            pts.push(RssPoint {
                elapsed_secs: t,
                rss_anon_mib: 400.0 + (m as f64) * 10.0,
                compactions_ok: 0,
            });
        }
        let trend = analyze_rss_trend(&pts, 3600, 6.0 * 3600.0, 24 * 3600, false);
        assert!(!trend.flagged);
        assert!(!trend.sawtooth_leak_flagged);
    }

    #[test]
    fn sawtooth_stable_cycles_after_retention_do_not_flag() {
        let mut pts = Vec::new();
        let ret = 6.0 * 3600.0;
        for h in 0..16 {
            for m in (0..120).step_by(10) {
                let t = (h * 3600 + m * 60) as f64;
                let phase = ((t % 7200.0) / 7200.0) * 300.0;
                pts.push(RssPoint {
                    elapsed_secs: t,
                    rss_anon_mib: 500.0 + phase,
                    compactions_ok: (t / 7200.0).floor() as u64,
                });
            }
        }
        let trend = analyze_rss_trend(&pts, 600, ret, 24 * 3600, false);
        assert!(trend.cycle_troughs_mib.len() >= 4);
        assert!(!trend.sawtooth_leak_flagged);
        assert!(!trend.flagged);
    }

    #[test]
    fn sawtooth_monotonic_trough_leak_after_retention_flags() {
        let mut pts = Vec::new();
        let ret = 6.0 * 3600.0;
        for cyc in 0..8 {
            for m in (0..120).step_by(10) {
                let t = (cyc * 7200 + m * 60) as f64;
                let base = if t >= ret {
                    500.0 + (cyc as f64) * 25.0
                } else {
                    500.0
                };
                let phase = (m as f64 / 120.0) * 250.0;
                pts.push(RssPoint {
                    elapsed_secs: t,
                    rss_anon_mib: base + phase,
                    compactions_ok: cyc as u64,
                });
            }
        }
        let trend = analyze_rss_trend(&pts, 600, ret, 24 * 3600, false);
        assert!(trend.trough_monotonic_run >= 3);
        assert!(trend.sawtooth_leak_flagged);
        assert!(trend.flagged);
    }

    #[test]
    fn rust_slope_faster_than_go_over_4h_with_ratio_gt_1_flags() {
        let mut cfg = SoakConfig::default();
        cfg.duration_secs = 24 * 3600;
        let go_trend = RssTrend {
            window_secs: 5.0 * 3600.0,
            slope_mib_per_hr: 5.0,
            ..Default::default()
        };
        let rust_trend = RssTrend {
            window_secs: 5.0 * 3600.0,
            slope_mib_per_hr: 35.0,
            ..Default::default()
        };
        let go_s = mk_sample(true, 600.0, 40, 0);
        let rust_s = mk_sample(true, 700.0, 40, 0);
        let counters = WorkloadCounters::new();
        let tracker = AnomalyTracker::default();
        let anomalies = detect_anomalies(&cfg, &go_s, &rust_s, &counters, &go_trend, &rust_trend, &tracker);
        assert!(anomalies.iter().any(|a| a.contains("exceeds Go")));
    }

    #[test]
    fn deleted_mmaps_require_two_consecutive_ticks_and_fd_leak_flags() {
        let mut cfg = SoakConfig::default();
        cfg.duration_secs = 120;
        let mut tracker = AnomalyTracker::default();
        let counters = WorkloadCounters::new();
        let trend = RssTrend::default();

        // Baseline tick at t=30s
        let g1 = mk_sample(true, 100.0, 40, 1); // transient deleted mmap
        let r1 = mk_sample(true, 80.0, 40, 0);
        tracker.observe(30, cfg.duration_secs, &g1, &r1);
        let a1 = detect_anomalies(&cfg, &g1, &r1, &counters, &trend, &trend, &tracker);
        assert!(a1.is_empty(), "single transient deleted_mmap must not flag: {a1:?}");

        // Next tick resets go deleted mmap, rust has fd jump + deleted mmap tick 1
        let g2 = mk_sample(true, 100.0, 40, 0);
        let r2 = mk_sample(true, 80.0, 110, 1);
        tracker.observe(45, cfg.duration_secs, &g2, &r2);
        let a2 = detect_anomalies(&cfg, &g2, &r2, &counters, &trend, &trend, &tracker);
        assert!(a2.is_empty(), "1-tick spikes must not flag yet: {a2:?}");

        // Second consecutive tick with deleted_mmaps + fd > baseline + 64
        let r3 = mk_sample(true, 80.0, 112, 1);
        tracker.observe(60, cfg.duration_secs, &g2, &r3);
        let a3 = detect_anomalies(&cfg, &g2, &r3, &counters, &trend, &trend, &tracker);
        assert!(a3.iter().any(|a| a.contains("persistent (deleted) mmaps")));
        assert!(a3.iter().any(|a| a.contains("FD leak")));
    }

    #[test]
    fn ephemeral_tenant_parity_accounts_for_idle_closing_jitter() {
        let now = Instant::now();
        let recs = vec![
            EphemeralTenantRecord {
                tenant_id: "soak-ephemeral-000001".to_string(),
                first_offset: 10,
                last_written_at: now - Duration::from_secs(3000), // > 40m -> must be closed
            },
            EphemeralTenantRecord {
                tenant_id: "soak-ephemeral-000002".to_string(),
                first_offset: 20,
                last_written_at: now - Duration::from_secs(1000), // 16m -> jitter window
            },
            EphemeralTenantRecord {
                tenant_id: "soak-ephemeral-000003".to_string(),
                first_offset: 30,
                last_written_at: now - Duration::from_secs(120), // 2m -> must be open
            },
        ];
        let go_set: BTreeSet<String> = [
            TENANT_PROD.to_string(),
            TENANT_CANARY.to_string(),
            "soak-ephemeral-000002".to_string(), // still open in Go, closed in Rust
            "soak-ephemeral-000003".to_string(),
        ]
        .into_iter()
        .collect();
        let rust_set: BTreeSet<String> = [
            TENANT_PROD.to_string(),
            TENANT_CANARY.to_string(),
            "soak-ephemeral-000003".to_string(),
        ]
        .into_iter()
        .collect();

        let res = verify_tenants_parity("test", &go_set, &rust_set, &recs, 30, now, 2760);
        assert!(res.is_ok(), "jitter window difference must pass: {res:?}");

        // Expired tenant still open on Rust must fail
        let mut bad_rust = rust_set;
        bad_rust.insert("soak-ephemeral-000001".to_string());
        let err = verify_tenants_parity("test", &go_set, &bad_rust, &recs, 30, now, 2760);
        assert!(err.is_err());
    }
}
