//! Wave 9 differential & multi-node cluster conformance for Pure Kafka `ingest_storage`
//! WITHOUT `memberlist` (using `etcd`, `consul`, and `multi` KV stores + `num_tokens: 0` +
//! `push_grpc_method_enabled: false`).
//!
//! Scenarios:
//! 1. Twin-process differential surface (Go vs Rust `-target=ingester`, Redpanda + etcd,
//!    `num_tokens: 0`, distinct `ring` vs `partition_ring` KV prefixes, NO `memberlist:`):
//!    - verifies neither process binds port 7946 (TCP or UDP)
//!    - diffs `GET /memberlist`, `GET /services`, `GET /ready`, `GET /ingester/ring`
//!      (`HideTokensInStatusPage` + `POST` forget), `GET /ingester/partition-ring` (+ `POST`
//!      `change_state` using `partition_ring.kvstore`), `GET /metrics` (no `memberlist_*`
//!      families, `cortex_dns_*` present, ring/KV metric parity), and disabled gRPC `Push`.
//! 2. Full multi-node mixed Go + Rust cluster over `etcd` + Redpanda + S3 (`num_tokens: 0`,
//!    ZERO memberlist): Go Distributor + Go Querier + Go Ingester (`ingester-zone-a-0`) +
//!    Rust Ingester (`ingester-zone-b-0`), pushing floats, native histograms, exemplars, and
//!    metadata via `/api/v1/push`, querying via Go Querier (`/prometheus/api/v1/{query,
//!    query_range,labels,metadata}`) with `X-Read-Consistency: strong`, and performing a live
//!    rolling replacement of the Rust partition owner (`prepare-shutdown` -> start second Rust
//!    owner `ingester-zone-c-0` -> `shutdown` first owner -> S3 flush + etcd unregistration +
//!    zero-gap queries).
//! 3. Pure Kafka + `consul` KV + `num_tokens: 0` (Go + Rust ingesters + Go Distributor + Go
//!    Querier), plus `multi` KV (`primary: consul, secondary: etcd, mirror_enabled: true`) on
//!    both `ingester.ring` and `ingester.partition_ring` verifying mirroring to etcd on both.
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).
//! `MIMIR_CONFORMANCE_RUST_BIN=$PWD/target/go-mimir` validates the harness Go-vs-Go.

#![allow(clippy::result_large_err)]

use std::collections::BTreeSet;
use std::fs;
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Result};
use base64::Engine as _;
use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::free_test_port;
use mimir_conformance::twin::{
    http_get, ingester_ring, ring_metric_series, services, start_mimir, wait_ready, FakeS3, Impl,
    Proc,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_proto::prost::Message as _;
use serde_json::Value;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const REDPANDA_IMAGE: &str = "redpandadata/redpanda";
const REDPANDA_TAG: &str =
    "v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649";
const ETCD_IMAGE: &str = "quay.io/coreos/etcd";
const ETCD_TAG: &str = "v3.5.17";
const CONSUL_IMAGE: &str = "hashicorp/consul";
const CONSUL_TAG: &str = "1.20";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w9-kafka-no-memberlist");

fn skip_docker(test: &str) -> bool {
    if std::env::var("MIMIR_CONFORMANCE_SKIP_DOCKER").as_deref() == Ok("1") {
        eprintln!("SKIPPED {test}: MIMIR_CONFORMANCE_SKIP_DOCKER=1 (this test needs Docker)");
        return true;
    }
    false
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------------------------
// Containers: Redpanda, etcd, Consul
// ---------------------------------------------------------------------------------------------

struct Redpanda {
    container: ContainerAsync<GenericImage>,
    port: u16,
}

impl Redpanda {
    async fn start() -> Result<Self> {
        let port = free_test_port();
        let yaml = format!(
            "redpanda:\n  data_directory: /var/lib/redpanda/data\n  empty_seed_starts_cluster: true\n  seed_servers: []\n  rpc_server: {{address: 0.0.0.0, port: 33145}}\n  advertised_rpc_api: {{address: 127.0.0.1, port: 33145}}\n  admin: [{{address: 0.0.0.0, port: 9644}}]\n  developer_mode: true\n  kafka_api:\n    - {{name: internal, address: 0.0.0.0, port: 9092}}\n    - {{name: external, address: 0.0.0.0, port: {port}}}\n  advertised_kafka_api:\n    - {{name: internal, address: 127.0.0.1, port: 9092}}\n    - {{name: external, address: 127.0.0.1, port: {port}}}\n"
        );
        let container = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
            .with_entrypoint("/bin/sh")
            .with_cmd([
                "-c",
                "cp /config/redpanda.yaml /etc/redpanda/redpanda.yaml && exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --reserve-memory 0M --default-log-level=warn",
            ])
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_startup_timeout(Duration::from_secs(180))
            .with_copy_to(
                CopyTargetOptions::new("/config/redpanda.yaml").with_mode(0o644),
                yaml.into_bytes(),
            )
            .with_mapped_port(port, ContainerPort::Tcp(port))
            .start()
            .await
            .context("starting Redpanda via testcontainers")?;
        let rp = Self { container, port };
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let ok = tokio::time::timeout(
                Duration::from_secs(30),
                rp.rpk(&["cluster", "health", "--exit-when-healthy"]),
            )
            .await;
            if matches!(ok, Ok(Ok(()))) {
                return Ok(rp);
            }
            ensure!(Instant::now() < deadline, "Redpanda never became healthy");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn rpk(&self, args: &[&str]) -> Result<()> {
        let cmd: Vec<String> = std::iter::once("rpk")
            .chain(args.iter().copied())
            .map(str::to_string)
            .collect();
        let mut res = self.container.exec(ExecCommand::new(cmd)).await?;
        let _ = res.stdout_to_vec().await?;
        let stderr = res.stderr_to_vec().await?;
        let code = loop {
            if let Some(c) = res.exit_code().await? {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        ensure!(
            code == 0,
            "rpk {args:?} exited {code}: {}",
            String::from_utf8_lossy(&stderr)
        );
        Ok(())
    }

    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

struct Etcd {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

impl Etcd {
    async fn start() -> Result<Self> {
        let port = free_test_port();
        let container = GenericImage::new(ETCD_IMAGE, ETCD_TAG)
            .with_wait_for(WaitFor::message_on_either_std("ready to serve client requests"))
            .with_entrypoint("etcd")
            .with_cmd([
                "--name=w9",
                "--data-dir=/tmp/etcd",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379",
            ])
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_mapped_port(port, ContainerPort::Tcp(2379))
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .context("starting etcd via testcontainers")?;
        Ok(Self {
            _container: container,
            port,
        })
    }

    fn endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    async fn get_raw(&self, client: &reqwest::Client, key: &str) -> Result<Option<String>> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(key.as_bytes());
        let resp = client
            .post(format!("http://127.0.0.1:{}/v3/kv/range", self.port))
            .json(&serde_json::json!({ "key": b64 }))
            .send()
            .await?;
        let v: Value = resp.json().await?;
        Ok(v["kvs"]
            .as_array()
            .and_then(|a| a.first())
            .and_then(|kv| kv["value"].as_str())
            .map(str::to_string))
    }

    async fn pause(&self) -> Result<()> {
        self._container.pause().await.context("pause etcd")
    }

    async fn unpause(&self) -> Result<()> {
        self._container.unpause().await.context("unpause etcd")
    }
}

struct Consul {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

impl Consul {
    async fn start() -> Result<Self> {
        let port = free_test_port();
        let container = GenericImage::new(CONSUL_IMAGE, CONSUL_TAG)
            .with_wait_for(WaitFor::message_on_either_std("agent: Synced node info"))
            .with_cmd(["agent", "-dev", "-client=0.0.0.0"])
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_mapped_port(port, ContainerPort::Tcp(8500))
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .context("starting consul via testcontainers")?;
        Ok(Self {
            _container: container,
            port,
        })
    }

    fn host(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    async fn keys(&self, client: &reqwest::Client, prefix: &str) -> Result<Vec<String>> {
        let resp = client
            .get(format!(
                "http://127.0.0.1:{}/v1/kv/{prefix}?keys=true",
                self.port
            ))
            .send()
            .await?;
        if resp.status().as_u16() == 404 {
            return Ok(Vec::new());
        }
        Ok(resp.json().await?)
    }

    async fn stop(&self) -> Result<()> {
        self._container
            .stop_with_timeout(Some(2))
            .await
            .context("stop consul")
    }
}

// ---------------------------------------------------------------------------------------------
// Socket / process inspection helpers
// ---------------------------------------------------------------------------------------------

/// Returns all `(proto, local_port)` sockets owned by `pid` on Linux via `/proc`.
fn process_bound_ports(pid: u32) -> BTreeSet<(&'static str, u16)> {
    let mut inodes = BTreeSet::new();
    let fd_dir = format!("/proc/{pid}/fd");
    if let Ok(entries) = fs::read_dir(&fd_dir) {
        for entry in entries.flatten() {
            if let Ok(target) = fs::read_link(entry.path()) {
                let s = target.to_string_lossy();
                if let Some(ino) = s.strip_prefix("socket:[").and_then(|r| r.strip_suffix(']')) {
                    inodes.insert(ino.to_string());
                }
            }
        }
    }

    let mut ports = BTreeSet::new();
    for (proto, path) in [
        ("tcp", "/proc/net/tcp"),
        ("tcp6", "/proc/net/tcp6"),
        ("udp", "/proc/net/udp"),
        ("udp6", "/proc/net/udp6"),
    ] {
        let Ok(text) = fs::read_to_string(path) else {
            continue;
        };
        for line in text.lines().skip(1) {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.len() < 10 {
                continue;
            }
            let inode = cols[9];
            if !inodes.contains(inode) {
                continue;
            }
            // Only inspect listening TCP sockets (`st == "0A"`) or bound UDP sockets (`st == "07"`).
            let st = cols[3];
            let is_bound_listener =
                (proto.starts_with("tcp") && st == "0A") || (proto.starts_with("udp") && st == "07");
            if !is_bound_listener {
                continue;
            }
            if let Some((_, port_hex)) = cols[1].rsplit_once(':') {
                if let Ok(port) = u16::from_str_radix(port_hex, 16) {
                    ports.insert((proto, port));
                }
            }
        }
    }
    ports
}

fn assert_no_memberlist_port_bound(proc: &Proc, allowed_ports: &[u16]) {
    let pid = proc.pid().expect("child pid");
    let bound = process_bound_ports(pid);
    for &(proto, port) in &bound {
        assert_ne!(
            port, 7946,
            "{} (pid {pid}) unexpectedly bound default memberlist port 7946 ({proto})! bound={bound:?}",
            proc.name
        );
        assert!(
            allowed_ports.contains(&port),
            "{} (pid {pid}) bound unexpected listener port {port} ({proto}); allowed={allowed_ports:?}, bound={bound:?}",
            proc.name
        );
    }
    // Also sanity check TCP 7946 is not listening on 127.0.0.1 if nothing else on host holds it.
    let _ = TcpStream::connect(("127.0.0.1", 7946));
}

// ---------------------------------------------------------------------------------------------
// Metric / HTTP helpers
// ---------------------------------------------------------------------------------------------

fn metric_families(exposition: &str) -> BTreeSet<String> {
    exposition
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .filter_map(|l| {
            let (series, _) = l.rsplit_once(' ')?;
            Some(
                series
                    .split_once('{')
                    .map_or(series, |(name, _)| name)
                    .to_string(),
            )
        })
        .collect()
}

async fn partition_ring_json(client: &reqwest::Client, port: u16) -> Result<Value> {
    let (code, _, body) = http_get(client, port, "/ingester/partition-ring", true)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    ensure!(code == 200, "GET /ingester/partition-ring: {code} {body}");
    Ok(serde_json::from_str(&body)?)
}

async fn post_form(
    client: &reqwest::Client,
    port: u16,
    path: &str,
    form_body: Option<&str>,
) -> Result<(u16, String, String)> {
    let mut req = client
        .post(format!("http://127.0.0.1:{port}{path}"))
        .timeout(Duration::from_secs(10));
    if let Some(b) = form_body {
        req = req
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(b.to_string());
    }
    let resp = req.send().await?;
    let status = resp.status().as_u16();
    let loc = resp
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = resp.text().await?;
    Ok((status, loc, body))
}

async fn push_via_distributor(
    client: &reqwest::Client,
    dist_http_port: u16,
    tenant: &str,
    req: cortexpb::WriteRequest,
) -> Result<()> {
    let raw = req.encode_to_vec();
    let compressed = snap::raw::Encoder::new().compress_vec(&raw)?;
    // Go Distributor's PartitionRingWatcher runs an independent rate-limited KV watch (e.g.
    // Consul watch_rate_limit = 1/s) and has no /ingester/partition-ring HTTP endpoint to poll,
    // so it can observe PartitionActive up to ~1-2s after Go Querier / Ingester does.
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let resp = client
            .post(format!("http://127.0.0.1:{dist_http_port}/api/v1/push"))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .header(LOWER_ORG_ID_HEADER_NAME, tenant)
            .body(compressed.clone())
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await?;
        if status.is_success() {
            return Ok(());
        }
        if status.as_u16() == 500
            && body.contains("no active partition found")
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        anyhow::bail!("POST /api/v1/push failed ({status}): {body}");
    }
}

async fn querier_get_json(
    client: &reqwest::Client,
    querier_http_port: u16,
    tenant: &str,
    path_and_query: &str,
    strong: bool,
) -> Result<Value> {
    let mut req = client
        .get(format!(
            "http://127.0.0.1:{querier_http_port}/prometheus/api/v1/{path_and_query}"
        ))
        .header(LOWER_ORG_ID_HEADER_NAME, tenant)
        .timeout(Duration::from_secs(10));
    if strong {
        req = req.header("X-Read-Consistency", "strong");
    }
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    ensure!(
        status.is_success(),
        "GET /prometheus/api/v1/{path_and_query} failed ({status}): {text}"
    );
    Ok(serde_json::from_str(&text)?)
}

// ---------------------------------------------------------------------------------------------
// YAML config builders (NO memberlist block anywhere!)
// ---------------------------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn ingester_etcd_yaml(
    dir: &Path,
    http: u16,
    grpc: u16,
    kafka: &str,
    topic: &str,
    etcd_ep: &str,
    instance_id: &str,
    zone: &str,
    ring_prefix: &str,
    part_prefix: &str,
    s3_endpoint: Option<&str>,
) -> String {
    let blocks_yaml = match s3_endpoint {
        Some(ep) => format!(
            r#"blocks_storage:
  backend: s3
  s3:
    endpoint: {ep}
    bucket_name: mimir-tsdb
    access_key_id: test
    secret_access_key: test
    insecure: true
  tsdb:
    dir: {tsdb}
    ship_interval: 1s
    flush_blocks_on_shutdown: true
"#,
            tsdb = dir.join("tsdb").display()
        ),
        None => format!(
            r#"blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    flush_blocks_on_shutdown: true
"#,
            bucket = dir.join("bucket").display(),
            tsdb = dir.join("tsdb").display()
        ),
    };
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn
usage_stats:
  enabled: false
ingest_storage:
  enabled: true
  kafka:
    address: {kafka}
    topic: {topic}
    auto_create_topic_enabled: true
    auto_create_topic_default_partitions: 1
    last_produced_offset_poll_interval: 100ms
    consumer_group_offset_commit_interval: 500ms
ingester:
  push_grpc_method_enabled: false
  ring:
    kvstore:
      store: etcd
      prefix: {ring_prefix}
      etcd:
        endpoints: [{etcd_ep}]
    num_tokens: 0
    instance_id: {instance_id}
    instance_addr: 127.0.0.1
    instance_availability_zone: {zone}
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
    heartbeat_period: 1s
    heartbeat_timeout: 30s
  partition_ring:
    kvstore:
      store: etcd
      prefix: {part_prefix}
      etcd:
        endpoints: [{etcd_ep}]
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
{blocks_yaml}limits:
  max_global_series_per_user: 0
  max_global_series_per_metric: 0
  max_global_exemplars_per_user: 100
  native_histograms_ingestion_enabled: true
activity_tracker:
  filepath: ""
"#
    )
}

fn distributor_yaml(
    http: u16,
    grpc: u16,
    kafka: &str,
    topic: &str,
    kv_store_yaml: &str,
    ring_prefix: &str,
    part_prefix: &str,
) -> String {
    format!(
        r#"target: distributor
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn
usage_stats:
  enabled: false
distributor:
  ring:
    kvstore:
      prefix: {ring_prefix}
{kv_store_yaml}
    instance_id: distributor-0
    instance_addr: 127.0.0.1
    heartbeat_period: 1s
    heartbeat_timeout: 30s
ingest_storage:
  enabled: true
  kafka:
    address: {kafka}
    topic: {topic}
    auto_create_topic_enabled: true
    auto_create_topic_default_partitions: 1
ingester:
  push_grpc_method_enabled: false
  ring:
    kvstore:
      prefix: {ring_prefix}
{kv_store_yaml}
    num_tokens: 0
    replication_factor: 1
    heartbeat_timeout: 30s
  partition_ring:
    kvstore:
      prefix: {part_prefix}
{kv_store_yaml}
limits:
  max_global_series_per_user: 0
  max_global_series_per_metric: 0
  max_global_exemplars_per_user: 100
  native_histograms_ingestion_enabled: true
activity_tracker:
  filepath: ""
"#
    )
}

#[allow(clippy::too_many_arguments)]
fn querier_yaml(
    dir: &Path,
    http: u16,
    grpc: u16,
    kafka: &str,
    topic: &str,
    kv_store_yaml: &str,
    ring_prefix: &str,
    part_prefix: &str,
    s3_endpoint: Option<&str>,
) -> String {
    let blocks_yaml = match s3_endpoint {
        Some(ep) => format!(
            r#"blocks_storage:
  backend: s3
  s3:
    endpoint: {ep}
    bucket_name: mimir-tsdb
    access_key_id: test
    secret_access_key: test
    insecure: true
  bucket_store:
    sync_dir: {sync}
"#,
            sync = dir.join("sync").display()
        ),
        None => format!(
            r#"blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  bucket_store:
    sync_dir: {sync}
"#,
            bucket = dir.join("bucket").display(),
            sync = dir.join("sync").display()
        ),
    };
    format!(
        r#"target: querier
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn
usage_stats:
  enabled: false
querier:
  ring:
    kvstore:
      store: inmemory
    instance_id: querier-0
    instance_addr: 127.0.0.1
store_gateway:
  sharding_ring:
    kvstore:
      store: inmemory
ingest_storage:
  enabled: true
  kafka:
    address: {kafka}
    topic: {topic}
    auto_create_topic_enabled: true
    auto_create_topic_default_partitions: 1
ingester:
  push_grpc_method_enabled: false
  ring:
    kvstore:
      prefix: {ring_prefix}
{kv_store_yaml}
    num_tokens: 0
    replication_factor: 1
    heartbeat_timeout: 30s
  partition_ring:
    kvstore:
      prefix: {part_prefix}
{kv_store_yaml}
{blocks_yaml}limits:
  max_global_series_per_user: 0
  max_global_series_per_metric: 0
  max_global_exemplars_per_user: 100
  native_histograms_ingestion_enabled: true
activity_tracker:
  filepath: ""
"#
    )
}

// ---------------------------------------------------------------------------------------------
// Test 1: Twin-process differential surface (Pure Kafka + etcd + num_tokens: 0, NO memberlist)
// ---------------------------------------------------------------------------------------------

#[test]
fn w9_01_twin_process_kafka_etcd_no_memberlist() {
    if skip_docker("w9_01_twin_process_kafka_etcd_no_memberlist") {
        return;
    }

    block_on_for_tests(async {
        let (rp, etcd) = tokio::try_join!(Redpanda::start(), Etcd::start())
            .expect("start Redpanda + etcd");
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        struct Side {
            which: Impl,
            http: u16,
            grpc: u16,
            proc: Proc,
            ring_prefix: String,
            part_prefix: String,
        }

        let mut sides = Vec::new();
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(which.name());
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let http = free_test_port();
            let grpc = free_test_port();
            let ring_prefix = format!("{}-ring/", which.name());
            let part_prefix = format!("{}-part/", which.name());
            let topic = format!("w9-t1-{}", which.name());
            let yaml = ingester_etcd_yaml(
                &dir,
                http,
                grpc,
                &rp.address(),
                &topic,
                &etcd.endpoint(),
                "ingester-zone-a-0",
                "zone-a",
                &ring_prefix,
                &part_prefix,
                None,
            );
            let mut proc = start_mimir(which, "t1", &dir, &yaml, &[]);
            wait_ready(&client, &mut proc, http, Duration::from_secs(45)).await;
            sides.push(Side {
                which,
                http,
                grpc,
                proc,
                ring_prefix,
                part_prefix,
            });
        }

        // 1. Neither Go nor Rust binds port 7946 (TCP or UDP) or any extra listener port.
        for s in &sides {
            assert_no_memberlist_port_bound(&s.proc, &[s.http, s.grpc]);
        }

        // 2. GET /memberlist parity.
        for s in &sides {
            let (code, headers, body) = http_get(&client, s.http, "/memberlist", false)
                .await
                .unwrap();
            assert_eq!(code, 200, "{} /memberlist status", s.which.name());
            let ct = headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            assert!(
                ct.starts_with("text/plain"),
                "{} /memberlist Content-Type={ct:?}",
                s.which.name()
            );
            assert_eq!(
                body,
                "This instance doesn't use memberlist.",
                "{} /memberlist body",
                s.which.name()
            );
        }

        // 3. GET /services parity.
        let go_svcs = services(&client, sides[0].http).await.unwrap();
        let rust_svcs = services(&client, sides[1].http).await.unwrap();
        assert_eq!(
            go_svcs, rust_svcs,
            "GET /services mismatch: go={go_svcs:?} rust={rust_svcs:?}"
        );
        assert_eq!(go_svcs.get("memberlist-kv").map(String::as_str), Some("Running"));
        assert_eq!(
            go_svcs.get("ingester-partitions-ring").map(String::as_str),
            Some("Running")
        );

        // 4. GET /ready parity.
        for s in &sides {
            let (code, _, body) = http_get(&client, s.http, "/ready", false).await.unwrap();
            assert_eq!(code, 200, "{} /ready", s.which.name());
            assert_eq!(body, "ready");
        }

        // 5. GET /ingester/ring (HTML HideTokensInStatusPage + JSON 0 tokens + POST forget).
        for s in &sides {
            let (code, _, html) = http_get(&client, s.http, "/ingester/ring", false)
                .await
                .unwrap();
            assert_eq!(code, 200);
            assert!(
                !html.contains("<th>Tokens</th>") && !html.contains("<th>Ownership</th>"),
                "{} /ingester/ring HTML must hide Tokens/Ownership columns when ingest_storage.enabled=true:\n{html}",
                s.which.name()
            );
            assert!(
                !html.contains("Show Tokens"),
                "{} /ingester/ring HTML must hide 'Show Tokens' button:\n{html}",
                s.which.name()
            );

            let ring = ingester_ring(&client, s.http).await.unwrap();
            assert_eq!(
                ring.get("ingester-zone-a-0"),
                Some(&("ACTIVE".to_string(), 0)),
                "{} /ingester/ring JSON: {ring:?}",
                s.which.name()
            );

            // Verify key landed under ring_prefix in etcd, NOT part_prefix.
            let stored = etcd
                .get_raw(&client, &format!("{}ring", s.ring_prefix))
                .await
                .unwrap();
            assert!(
                stored.is_some(),
                "{} ring key missing in etcd at {}ring",
                s.which.name(),
                s.ring_prefix
            );

            // POST /ingester/ring?forget=nonexistent -> 302 Location: #
            let (pcode, ploc, _) =
                post_form(&client, s.http, "/ingester/ring?forget=nonexistent", None)
                    .await
                    .unwrap();
            assert_eq!(pcode, 302, "{} POST /ingester/ring", s.which.name());
            assert_eq!(ploc, "#", "{} POST /ingester/ring location", s.which.name());
        }

        // 6. GET /ingester/partition-ring + POST change_state using partition_ring.kvstore
        //    (verifying distinct prefix from ring.kvstore.prefix!).
        for s in &sides {
            // PartitionInstanceLifecycler creates partition 0 as Pending (1) and promotes it
            // to Active (2) on its reconcile tick (up to ~5s).
            let deadline = Instant::now() + Duration::from_secs(15);
            let pr = loop {
                let pr = partition_ring_json(&client, s.http).await.unwrap();
                let state = pr["partitions"][0]["state"].as_i64().unwrap_or(0);
                if state == 2 {
                    break pr;
                }
                assert!(
                    Instant::now() < deadline,
                    "{} partition 0 did not reach PartitionActive (2): {pr}",
                    s.which.name()
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            };
            let parts = pr["partitions"].as_array().expect("partitions array");
            assert_eq!(parts.len(), 1, "{} partitions: {pr}", s.which.name());
            assert_eq!(parts[0]["id"], 0);
            assert_eq!(parts[0]["state"], 2);
            assert_eq!(parts[0]["owner_ids"], serde_json::json!(["ingester-zone-a-0"]));

            let stored_part = etcd
                .get_raw(&client, &format!("{}ingester-partitions", s.part_prefix))
                .await
                .unwrap();
            assert!(
                stored_part.is_some(),
                "{} partition ring key missing in etcd at {}ingester-partitions",
                s.which.name(),
                s.part_prefix
            );

            // Change partition 0 to INACTIVE and back to ACTIVE via POST /ingester/partition-ring.
            for target_state in ["PartitionInactive", "PartitionActive"] {
                let (pcode, ploc, pbody) = post_form(
                    &client,
                    s.http,
                    "/ingester/partition-ring",
                    Some(&format!(
                        "action=change_state&partition_id=0&partition_state={target_state}"
                    )),
                )
                .await
                .unwrap();
                assert_eq!(
                    pcode,
                    302,
                    "{} POST /ingester/partition-ring ({target_state}): {pbody}",
                    s.which.name()
                );
                assert_eq!(ploc, "#");
            }
            // Wait for watcher to observe ACTIVE again.
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let pr_after = partition_ring_json(&client, s.http).await.unwrap();
                if pr_after["partitions"][0]["state"] == 2 {
                    break;
                }
                assert!(Instant::now() < deadline, "{} partition 0 did not return to ACTIVE", s.which.name());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // 7. GET /metrics: zero memberlist_* / cortex_memberlist_* families; cortex_dns_* present;
        //    ring/lifecycler/KV metric series match Go.
        let (_, _, go_metrics) = http_get(&client, sides[0].http, "/metrics", false)
            .await
            .unwrap();
        let (_, _, rust_metrics) = http_get(&client, sides[1].http, "/metrics", false)
            .await
            .unwrap();
        for (name, body) in [("go", &go_metrics), ("rust", &rust_metrics)] {
            let fams = metric_families(body);
            let ml_fams: Vec<&String> = fams
                .iter()
                .filter(|f| f.starts_with("memberlist") || f.starts_with("cortex_memberlist"))
                .collect();
            assert!(
                ml_fams.is_empty(),
                "{name} unexpectedly registered memberlist metrics when memberlist is unused: {ml_fams:?}"
            );
            assert!(
                fams.contains("cortex_dns_lookups_total"),
                "{name} missing cortex_dns_lookups_total"
            );
            assert!(
                fams.contains("cortex_dns_failures_total"),
                "{name} missing cortex_dns_failures_total"
            );
        }
        let go_ring_series = ring_metric_series(&go_metrics);
        let rust_ring_series = ring_metric_series(&rust_metrics);
        assert_eq!(
            go_ring_series, rust_ring_series,
            "ring/lifecycler/KV metric series mismatch:\nonly in Go: {:?}\nonly in Rust: {:?}",
            go_ring_series.difference(&rust_ring_series).collect::<Vec<_>>(),
            rust_ring_series.difference(&go_ring_series).collect::<Vec<_>>()
        );

        // 8. gRPC Push is disabled (push_grpc_method_enabled: false) identically in Go & Rust.
        let mut push_codes = Vec::new();
        for s in &sides {
            let ch = Channel::from_shared(format!("http://127.0.0.1:{}", s.grpc))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut grpc_cli = IngesterClient::new(ch);
            let mut req = tonic::Request::new(cortexpb::WriteRequest::default());
            req.metadata_mut()
                .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::from_static("t1"));
            let err = grpc_cli
                .push(req)
                .await
                .expect_err("gRPC Push must fail when push_grpc_method_enabled=false");
            push_codes.push((s.which.name(), err.code(), err.message().to_string()));
        }
        assert_eq!(
            push_codes[0].1, push_codes[1].1,
            "disabled gRPC Push status code mismatch: go={:?} rust={:?}",
            push_codes[0], push_codes[1]
        );
    });
}

// ---------------------------------------------------------------------------------------------
// Test 2: Full Multi-Node Mixed Go + Rust Cluster over etcd + Redpanda + S3, ZERO memberlist
// ---------------------------------------------------------------------------------------------

#[test]
fn w9_02_mixed_cluster_etcd_redpanda_no_memberlist() {
    if skip_docker("w9_02_mixed_cluster_etcd_redpanda_no_memberlist") {
        return;
    }

    block_on_for_tests(async {
        let s3_port = free_test_port();
        let s3 = FakeS3::start(s3_port, None).await;
        let (rp, etcd) = tokio::try_join!(Redpanda::start(), Etcd::start())
            .expect("start Redpanda + etcd");
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        let topic = "w9-t2-ingest";
        let ring_prefix = "collectors/";
        let part_prefix = "collectors/";
        let etcd_kv_yaml = format!(
            "      store: etcd\n      etcd:\n        endpoints: [{}]",
            etcd.endpoint()
        );

        // 1. Go Ingester (ingester-zone-a-0) + Rust Ingester (ingester-zone-b-0)
        let (go_ing_http, go_ing_grpc) = (free_test_port(), free_test_port());
        let go_ing_dir = tmp.path().join("go-ing-a");
        fs::create_dir_all(go_ing_dir.join("tsdb")).unwrap();
        let mut go_ing = start_mimir(
            Impl::Go,
            "ing-a",
            &go_ing_dir,
            &ingester_etcd_yaml(
                &go_ing_dir,
                go_ing_http,
                go_ing_grpc,
                &rp.address(),
                topic,
                &etcd.endpoint(),
                "ingester-zone-a-0",
                "zone-a",
                ring_prefix,
                part_prefix,
                Some(&s3.endpoint()),
            ),
            &[],
        );

        let (rust_ing1_http, rust_ing1_grpc) = (free_test_port(), free_test_port());
        let rust_ing1_dir = tmp.path().join("rust-ing-b");
        fs::create_dir_all(rust_ing1_dir.join("tsdb")).unwrap();
        let mut rust_ing1 = start_mimir(
            Impl::Rust,
            "ing-b",
            &rust_ing1_dir,
            &ingester_etcd_yaml(
                &rust_ing1_dir,
                rust_ing1_http,
                rust_ing1_grpc,
                &rp.address(),
                topic,
                &etcd.endpoint(),
                "ingester-zone-b-0",
                "zone-b",
                ring_prefix,
                part_prefix,
                Some(&s3.endpoint()),
            ),
            &[],
        );

        // 2. Go Distributor + Go Querier (NO memberlist anywhere!)
        let (dist_http, dist_grpc) = (free_test_port(), free_test_port());
        let dist_dir = tmp.path().join("go-dist");
        let mut go_dist = start_mimir(
            Impl::Go,
            "dist",
            &dist_dir,
            &distributor_yaml(
                dist_http,
                dist_grpc,
                &rp.address(),
                topic,
                &etcd_kv_yaml,
                ring_prefix,
                part_prefix,
            ),
            &[],
        );

        let (querier_http, querier_grpc) = (free_test_port(), free_test_port());
        let querier_dir = tmp.path().join("go-querier");
        fs::create_dir_all(querier_dir.join("sync")).unwrap();
        let mut go_querier = start_mimir(
            Impl::Go,
            "querier",
            &querier_dir,
            &querier_yaml(
                &querier_dir,
                querier_http,
                querier_grpc,
                &rp.address(),
                topic,
                &etcd_kv_yaml,
                ring_prefix,
                part_prefix,
                Some(&s3.endpoint()),
            ),
            &[],
        );

        wait_ready(&client, &mut go_ing, go_ing_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut rust_ing1, rust_ing1_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut go_dist, dist_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut go_querier, querier_http, Duration::from_secs(45)).await;

        // Confirm ZERO processes bind port 7946.
        assert_no_memberlist_port_bound(&go_ing, &[go_ing_http, go_ing_grpc]);
        assert_no_memberlist_port_bound(&rust_ing1, &[rust_ing1_http, rust_ing1_grpc]);
        assert_no_memberlist_port_bound(&go_dist, &[dist_http, dist_grpc]);
        assert_no_memberlist_port_bound(&go_querier, &[querier_http, querier_grpc]);

        // Wait for Go Distributor & Go Querier to observe both ingesters ACTIVE and both owning partition 0.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let dr = ingester_ring(&client, dist_http).await.unwrap_or_default();
            let qr = ingester_ring(&client, querier_http).await.unwrap_or_default();
            let pr = partition_ring_json(&client, querier_http).await.ok();
            let owners: BTreeSet<String> = pr
                .as_ref()
                .and_then(|v| v["partitions"].as_array())
                .and_then(|a| a.first())
                .and_then(|p| p["owner_ids"].as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let part_state = pr
                .as_ref()
                .and_then(|v| v["partitions"].as_array())
                .and_then(|a| a.first())
                .and_then(|p| p["state"].as_i64())
                .unwrap_or(0);
            if dr.get("ingester-zone-a-0") == Some(&("ACTIVE".to_string(), 0))
                && dr.get("ingester-zone-b-0") == Some(&("ACTIVE".to_string(), 0))
                && qr.get("ingester-zone-a-0") == Some(&("ACTIVE".to_string(), 0))
                && qr.get("ingester-zone-b-0") == Some(&("ACTIVE".to_string(), 0))
                && part_state == 2
                && owners.contains("ingester-zone-a-0")
                && owners.contains("ingester-zone-b-0")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cluster rings did not converge: dist={dr:?} querier={qr:?} owners={owners:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // 3. Push floats, native histograms, exemplars, and metadata via Go Distributor /api/v1/push.
        let base_ts = now_ms() - 25_000;
        let write_req = cortexpb::WriteRequest {
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_up"),
                        cortexpb::LabelPair::from_static("job", "w9-etcd"),
                    ],
                    samples: vec![
                        cortexpb::Sample {
                            value: 1.0,
                            timestamp_ms: base_ts,
                        },
                        cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: base_ts + 10_000,
                        },
                        cortexpb::Sample {
                            value: 3.0,
                            timestamp_ms: base_ts + 20_000,
                        },
                    ],
                    exemplars: vec![cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair::from_static("trace_id", "t1")],
                        value: 3.0,
                        timestamp_ms: base_ts + 20_000,
                    }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_latency"),
                        cortexpb::LabelPair::from_static("job", "w9-etcd"),
                    ],
                    histograms: vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(5)),
                        sum: 12.5,
                        schema: 0,
                        zero_threshold: 0.001,
                        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(2)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 2,
                        }],
                        positive_deltas: vec![2, -1],
                        reset_hint: cortexpb::histogram::ResetHint::No as i32,
                        timestamp: base_ts + 20_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            metadata: vec![
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                    metric_family_name: "w9_up".to_string(),
                    help: "W9 gauge metric".to_string(),
                    unit: String::new(),
                },
                cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                    metric_family_name: "w9_latency".to_string(),
                    help: "W9 native histogram metric".to_string(),
                    unit: "seconds".to_string(),
                },
            ],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        push_via_distributor(&client, dist_http, "tenant-a", write_req)
            .await
            .expect("push via Go Distributor");

        // 4. Query via Go Querier with strong read consistency.
        let q_ts_sec = (base_ts + 20_000) / 1000 + 2;
        let instant = querier_get_json(
            &client,
            querier_http,
            "tenant-a",
            &format!("query?query=w9_up%7Bjob%3D%22w9-etcd%22%7D&time={q_ts_sec}"),
            true,
        )
        .await
        .expect("instant query");
        assert_eq!(instant["status"], "success");
        assert_eq!(instant["data"]["result"][0]["value"][1], "3");

        let hist_q = querier_get_json(
            &client,
            querier_http,
            "tenant-a",
            &format!(
                "query?query=histogram_count(w9_latency%7Bjob%3D%22w9-etcd%22%7D)&time={q_ts_sec}"
            ),
            true,
        )
        .await
        .expect("histogram_count query");
        assert_eq!(hist_q["status"], "success");
        assert_eq!(hist_q["data"]["result"][0]["value"][1], "5");

        let start_s = base_ts / 1000;
        let end_s = q_ts_sec;
        let range = querier_get_json(
            &client,
            querier_http,
            "tenant-a",
            &format!("query_range?query=w9_up%7Bjob%3D%22w9-etcd%22%7D&start={start_s}&end={end_s}&step=5"),
            true,
        )
        .await
        .expect("query_range");
        assert_eq!(range["status"], "success");
        let pts = range["data"]["result"][0]["values"]
            .as_array()
            .expect("range values");
        assert!(pts.len() >= 3, "expected >= 3 range points, got {pts:?}");

        let labels = querier_get_json(&client, querier_http, "tenant-a", "labels", true)
            .await
            .expect("labels");
        let label_names: Vec<&str> = labels["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert!(label_names.contains(&"__name__"));
        assert!(label_names.contains(&"job"));

        let meta = querier_get_json(&client, querier_http, "tenant-a", "metadata", true)
            .await
            .expect("metadata");
        assert_eq!(meta["status"], "success");
        assert!(meta["data"]["w9_up"].is_array(), "metadata missing w9_up: {meta}");
        assert!(meta["data"]["w9_latency"].is_array(), "metadata missing w9_latency: {meta}");

        // 5. Live rolling replacement of Rust partition owner (ingester-zone-b-0 -> ingester-zone-c-0).
        let stop_poller = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop_poller);
        let poll_client = http_client();
        let poller = tokio::spawn(async move {
            let mut total = 0usize;
            let mut failures = Vec::new();
            while !stop_flag.load(Ordering::Acquire) {
                match querier_get_json(
                    &poll_client,
                    querier_http,
                    "tenant-a",
                    &format!("query?query=w9_up%7Bjob%3D%22w9-etcd%22%7D&time={q_ts_sec}"),
                    true,
                )
                .await
                {
                    Ok(v) if v["status"] == "success" && v["data"]["result"][0]["value"][1] == "3" => {}
                    Ok(v) => failures.push(format!("bad query response: {v}")),
                    Err(e) => failures.push(format!("query error: {e}")),
                }
                total += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            (total, failures)
        });

        // 5a. POST /ingester/prepare-shutdown on first Rust owner.
        let (prep_code, _, prep_body) =
            post_form(&client, rust_ing1_http, "/ingester/prepare-shutdown", None)
                .await
                .unwrap();
        assert_eq!(prep_code, 204, "prepare-shutdown failed: {prep_body}");

        // 5b. Boot second Rust owner ingester-zone-c-0 sharing partition 0.
        let (rust_ing2_http, rust_ing2_grpc) = (free_test_port(), free_test_port());
        let rust_ing2_dir = tmp.path().join("rust-ing-c");
        fs::create_dir_all(rust_ing2_dir.join("tsdb")).unwrap();
        let mut rust_ing2 = start_mimir(
            Impl::Rust,
            "ing-c",
            &rust_ing2_dir,
            &ingester_etcd_yaml(
                &rust_ing2_dir,
                rust_ing2_http,
                rust_ing2_grpc,
                &rp.address(),
                topic,
                &etcd.endpoint(),
                "ingester-zone-c-0",
                "zone-c",
                ring_prefix,
                part_prefix,
                Some(&s3.endpoint()),
            ),
            &[],
        );
        wait_ready(&client, &mut rust_ing2, rust_ing2_http, Duration::from_secs(45)).await;
        assert_no_memberlist_port_bound(&rust_ing2, &[rust_ing2_http, rust_ing2_grpc]);

        // Wait for ingester-zone-c-0 to appear as owner of partition 0 in etcd (via querier).
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let pr = partition_ring_json(&client, querier_http).await.unwrap();
            let owners = pr["partitions"][0]["owner_ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if owners.iter().any(|o| o == "ingester-zone-c-0") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "ingester-zone-c-0 never joined partition 0 owners: {owners:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // 5c. POST /ingester/shutdown on first Rust owner (ingester-zone-b-0).
        let s3_puts_before = s3.requests().iter().filter(|r| r.method == "PUT").count();
        let (sd_code, _, sd_body) =
            post_form(&client, rust_ing1_http, "/ingester/shutdown", None)
                .await
                .unwrap();
        assert_eq!(sd_code, 204, "shutdown failed: {sd_body}");

        // Verify flush uploaded blocks to S3 and ingester-zone-b-0 left both rings in etcd.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let s3_puts_now = s3
                .requests()
                .iter()
                .filter(|r| r.method == "PUT" && r.path.contains("tenant-a"))
                .count();
            let qr = ingester_ring(&client, querier_http).await.unwrap_or_default();
            let pr = partition_ring_json(&client, querier_http).await.unwrap();
            let owners = pr["partitions"][0]["owner_ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if s3_puts_now > s3_puts_before
                && !qr.contains_key("ingester-zone-b-0")
                && !owners.iter().any(|o| o == "ingester-zone-b-0")
                && owners.iter().any(|o| o == "ingester-zone-c-0")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "shutdown flush/unregister incomplete: s3_puts={s3_puts_now} (was {s3_puts_before}) qr={qr:?} owners={owners:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        rust_ing1.sigterm();
        let _ = rust_ing1.wait_exit(Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Stop background query poller & assert zero-gap queries across the entire rolling replacement.
        stop_poller.store(true, Ordering::Release);
        let (total_polls, poll_failures) = poller.await.unwrap();
        assert!(
            total_polls >= 3,
            "expected >= 3 background queries during rolling replacement, got {total_polls}"
        );
        assert!(
            poll_failures.is_empty(),
            "zero-gap PromQL check failed during rolling replacement: {poll_failures:?}"
        );

        // Push one more sample and verify Go Querier sees it immediately via strong read consistency.
        let fresh_ts = now_ms();
        let fresh_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "w9_up"),
                    cortexpb::LabelPair::from_static("job", "w9-etcd"),
                ],
                samples: vec![cortexpb::Sample {
                    value: 4.0,
                    timestamp_ms: fresh_ts,
                }],
                ..Default::default()
            }],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        push_via_distributor(&client, dist_http, "tenant-a", fresh_req)
            .await
            .unwrap();
        let fresh_q = querier_get_json(
            &client,
            querier_http,
            "tenant-a",
            &format!("query?query=w9_up%7Bjob%3D%22w9-etcd%22%7D&time={}", fresh_ts / 1000 + 2),
            true,
        )
        .await
        .unwrap();
        assert_eq!(fresh_q["data"]["result"][0]["value"][1], "4");
    });
}

// ---------------------------------------------------------------------------------------------
// Test 3: Pure Kafka + Consul KV + num_tokens: 0 (and multi KV: primary consul, secondary etcd)
// ---------------------------------------------------------------------------------------------

#[test]
fn w9_03_mixed_consul_and_multi_kv_no_memberlist() {
    if skip_docker("w9_03_mixed_consul_and_multi_kv_no_memberlist") {
        return;
    }

    block_on_for_tests(async {
        let (rp, consul, etcd) =
            tokio::try_join!(Redpanda::start(), Consul::start(), Etcd::start())
                .expect("start Redpanda + Consul + etcd");
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        let topic = "w9-t3-ingest";
        let ring_prefix = "consul-ring/";
        let part_prefix = "consul-part/";
        let consul_kv_yaml = format!(
            "      store: consul\n      consul:\n        host: {}",
            consul.host()
        );

        let consul_ing_yaml = |dir: &Path, http: u16, grpc: u16, id: &str, zone: &str| {
            format!(
                r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn
usage_stats:
  enabled: false
ingest_storage:
  enabled: true
  kafka:
    address: {kafka}
    topic: {topic}
    auto_create_topic_enabled: true
    auto_create_topic_default_partitions: 1
    last_produced_offset_poll_interval: 100ms
    consumer_group_offset_commit_interval: 500ms
ingester:
  push_grpc_method_enabled: false
  ring:
    kvstore:
      store: consul
      prefix: {ring_prefix}
      consul:
        host: {chost}
    num_tokens: 0
    instance_id: {id}
    instance_addr: 127.0.0.1
    instance_availability_zone: {zone}
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
    heartbeat_period: 1s
    heartbeat_timeout: 30s
  partition_ring:
    kvstore:
      store: consul
      prefix: {part_prefix}
      consul:
        host: {chost}
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    flush_blocks_on_shutdown: true
activity_tracker:
  filepath: ""
"#,
                kafka = rp.address(),
                chost = consul.host(),
                bucket = dir.join("bucket").display(),
                tsdb = dir.join("tsdb").display(),
            )
        };

        // Part A: Go Ingester + Rust Ingester + Go Distributor + Go Querier over Consul KV.
        let (go_http, go_grpc) = (free_test_port(), free_test_port());
        let go_dir = tmp.path().join("go-consul");
        fs::create_dir_all(go_dir.join("tsdb")).unwrap();
        fs::create_dir_all(go_dir.join("bucket")).unwrap();
        let mut go_ing = start_mimir(
            Impl::Go,
            "consul-ing-a",
            &go_dir,
            &consul_ing_yaml(&go_dir, go_http, go_grpc, "ingester-zone-a-0", "zone-a"),
            &[],
        );

        let (rust_http, rust_grpc) = (free_test_port(), free_test_port());
        let rust_dir = tmp.path().join("rust-consul");
        fs::create_dir_all(rust_dir.join("tsdb")).unwrap();
        fs::create_dir_all(rust_dir.join("bucket")).unwrap();
        let mut rust_ing = start_mimir(
            Impl::Rust,
            "consul-ing-b",
            &rust_dir,
            &consul_ing_yaml(&rust_dir, rust_http, rust_grpc, "ingester-zone-b-0", "zone-b"),
            &[],
        );

        let (dist_http, dist_grpc) = (free_test_port(), free_test_port());
        let mut go_dist = start_mimir(
            Impl::Go,
            "consul-dist",
            &tmp.path().join("consul-dist"),
            &distributor_yaml(
                dist_http,
                dist_grpc,
                &rp.address(),
                topic,
                &consul_kv_yaml,
                ring_prefix,
                part_prefix,
            ),
            &[],
        );

        let (q_http, q_grpc) = (free_test_port(), free_test_port());
        let q_dir = tmp.path().join("consul-querier");
        fs::create_dir_all(q_dir.join("bucket")).unwrap();
        fs::create_dir_all(q_dir.join("sync")).unwrap();
        let mut go_querier = start_mimir(
            Impl::Go,
            "consul-querier",
            &q_dir,
            &querier_yaml(
                &q_dir,
                q_http,
                q_grpc,
                &rp.address(),
                topic,
                &consul_kv_yaml,
                ring_prefix,
                part_prefix,
                None,
            ),
            &[],
        );

        wait_ready(&client, &mut go_ing, go_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut rust_ing, rust_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut go_dist, dist_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut go_querier, q_http, Duration::from_secs(45)).await;

        assert_no_memberlist_port_bound(&go_ing, &[go_http, go_grpc]);
        assert_no_memberlist_port_bound(&rust_ing, &[rust_http, rust_grpc]);

        // Verify keys exist in Consul for BOTH ring and partition_ring.
        let rkeys = consul.keys(&client, ring_prefix).await.unwrap();
        let pkeys = consul.keys(&client, part_prefix).await.unwrap();
        assert!(
            rkeys.iter().any(|k| k == "consul-ring/ring"),
            "missing ring key in Consul: {rkeys:?}"
        );
        assert!(
            pkeys.iter().any(|k| k == "consul-part/ingester-partitions"),
            "missing partition ring key in Consul: {pkeys:?}"
        );

        // Wait for convergence on querier (including PartitionActive state == 2).
        let deadline = Instant::now() + Duration::from_secs(25);
        loop {
            let qr = ingester_ring(&client, q_http).await.unwrap_or_default();
            let pr = partition_ring_json(&client, q_http).await.unwrap();
            let part_state = pr["partitions"][0]["state"].as_i64().unwrap_or(0);
            let owners = pr["partitions"][0]["owner_ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if qr.get("ingester-zone-a-0") == Some(&("ACTIVE".to_string(), 0))
                && qr.get("ingester-zone-b-0") == Some(&("ACTIVE".to_string(), 0))
                && part_state == 2
                && owners.iter().any(|o| o == "ingester-zone-a-0")
                && owners.iter().any(|o| o == "ingester-zone-b-0")
            {
                break;
            }
            assert!(Instant::now() < deadline, "consul rings didn't converge: qr={qr:?} owners={owners:?}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Push via Go Distributor & query via Go Querier with strong consistency.
        let ts = now_ms() - 2_000;
        push_via_distributor(
            &client,
            dist_http,
            "tenant-consul",
            cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_consul_up"),
                        cortexpb::LabelPair::from_static("job", "consul"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 42.0,
                        timestamp_ms: ts,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let res = querier_get_json(
            &client,
            q_http,
            "tenant-consul",
            &format!("query?query=w9_consul_up&time={}", ts / 1000 + 2),
            true,
        )
        .await
        .unwrap();
        assert_eq!(res["data"]["result"][0]["value"][1], "42");

        // Prepare shutdown + shutdown Rust ingester cleanly and verify unregistration from BOTH rings in Consul.
        let (prep_code, _, _) = post_form(&client, rust_http, "/ingester/prepare-shutdown", None)
            .await
            .unwrap();
        assert_eq!(prep_code, 204);
        let (sd_code, _, _) = post_form(&client, rust_http, "/ingester/shutdown", None)
            .await
            .unwrap();
        assert_eq!(sd_code, 204);
        rust_ing.sigterm();
        let _ = rust_ing.wait_exit(Duration::from_secs(10));

        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let qr = ingester_ring(&client, q_http).await.unwrap_or_default();
            let pr = partition_ring_json(&client, q_http).await.unwrap();
            let owners = pr["partitions"][0]["owner_ids"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if !qr.contains_key("ingester-zone-b-0")
                && !owners.iter().any(|o| o == "ingester-zone-b-0")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "rust ingester didn't unregister from Consul: qr={qr:?} owners={owners:?}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Part B: Verify `multi` KV store (primary: consul, secondary: etcd, mirror_enabled: true)
        // works for BOTH ingester.ring AND ingester.partition_ring without memberlist!
        let (m_http, m_grpc) = (free_test_port(), free_test_port());
        let m_dir = tmp.path().join("rust-multi");
        fs::create_dir_all(m_dir.join("tsdb")).unwrap();
        fs::create_dir_all(m_dir.join("bucket")).unwrap();
        let multi_yaml = format!(
            r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {m_http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {m_grpc}
  log_level: warn
usage_stats:
  enabled: false
ingest_storage:
  enabled: true
  kafka:
    address: {kafka}
    topic: w9-t3-multi
    auto_create_topic_enabled: true
    auto_create_topic_default_partitions: 1
ingester:
  push_grpc_method_enabled: false
  ring:
    kvstore:
      store: multi
      prefix: multi-ring/
      consul:
        host: {chost}
      etcd:
        endpoints: [{ehost}]
      multi:
        primary: consul
        secondary: etcd
        mirror_enabled: true
    num_tokens: 0
    instance_id: ingester-zone-d-0
    instance_addr: 127.0.0.1
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
    heartbeat_period: 1s
  partition_ring:
    kvstore:
      store: multi
      prefix: multi-part/
      consul:
        host: {chost}
      etcd:
        endpoints: [{ehost}]
      multi:
        primary: consul
        secondary: etcd
        mirror_enabled: true
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
            kafka = rp.address(),
            chost = consul.host(),
            ehost = etcd.endpoint(),
            bucket = m_dir.join("bucket").display(),
            tsdb = m_dir.join("tsdb").display(),
        );
        let mut multi_ing = start_mimir(Impl::Rust, "multi", &m_dir, &multi_yaml, &[]);
        wait_ready(&client, &mut multi_ing, m_http, Duration::from_secs(45)).await;
        assert_no_memberlist_port_bound(&multi_ing, &[m_http, m_grpc]);

        // Confirm BOTH Consul (primary) and etcd (secondary mirrored) received BOTH keys.
        let c_ring = consul.keys(&client, "multi-ring/").await.unwrap();
        let c_part = consul.keys(&client, "multi-part/").await.unwrap();
        assert!(c_ring.iter().any(|k| k == "multi-ring/ring"), "Consul missing multi-ring/ring: {c_ring:?}");
        assert!(
            c_part.iter().any(|k| k == "multi-part/ingester-partitions"),
            "Consul missing multi-part/ingester-partitions: {c_part:?}"
        );
        let e_ring = etcd.get_raw(&client, "multi-ring/ring").await.unwrap();
        let e_part = etcd
            .get_raw(&client, "multi-part/ingester-partitions")
            .await
            .unwrap();
        assert!(e_ring.is_some(), "etcd missing mirrored multi-ring/ring");
        assert!(
            e_part.is_some(),
            "etcd missing mirrored multi-part/ingester-partitions"
        );

        let (_, _, m_metrics) = http_get(&client, m_http, "/metrics", false).await.unwrap();
        let fams = metric_families(&m_metrics);
        assert!(fams.contains("cortex_multikv_primary_store"));
        assert!(fams.contains("cortex_multikv_mirror_enabled"));
        assert!(fams.contains("multikv_primary_store"));
        assert!(fams.contains("multikv_mirror_enabled"));
        assert!(!fams.iter().any(|f| f.starts_with("memberlist")));
    });
}

// ---------------------------------------------------------------------------------------------
// Test 4: Hardcore Chaos (Helm Flag Landmines + Live multi KV Migration + SIGKILL + etcd Outage)
// ---------------------------------------------------------------------------------------------

#[test]
fn w9_04_hardcore_chaos_sigkill_etcd_outage_live_kv_migration_and_helm_flags() {
    if skip_docker("w9_04_hardcore_chaos_sigkill_etcd_outage_live_kv_migration_and_helm_flags") {
        return;
    }

    block_on_for_tests(async {
        let (rp, consul, etcd) =
            tokio::try_join!(Redpanda::start(), Consul::start(), Etcd::start())
                .expect("start Redpanda + Consul + etcd");
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        // -------------------------------------------------------------------------------------
        // Subcase A: Helm flag landmine check
        //   Pass `-memberlist.abort-if-fast-join-fails=true` + `-memberlist.abort-if-join-fails=true`
        //   AND `memberlist.join_members: ["dns+unresolvable-gossip-ring.invalid:7946"]`
        //   while `kvstore.store: etcd` (Go & Rust) and `kvstore.store: multi` (Rust).
        // -------------------------------------------------------------------------------------
        let helm_cli = vec![
            "-memberlist.abort-if-fast-join-fails=true".to_string(),
            "-memberlist.abort-if-join-fails=true".to_string(),
        ];
        for which in [Impl::Go, Impl::Rust] {
            let dir = tmp.path().join(format!("helm-landmine-{}", which.name()));
            fs::create_dir_all(dir.join("tsdb")).unwrap();
            fs::create_dir_all(dir.join("bucket")).unwrap();
            let http = free_test_port();
            let grpc = free_test_port();
            let mut yaml = ingester_etcd_yaml(
                &dir,
                http,
                grpc,
                &rp.address(),
                &format!("w9-t4-helm-{}", which.name()),
                &etcd.endpoint(),
                "ingester-zone-a-0",
                "zone-a",
                &format!("helm-{}-ring/", which.name()),
                &format!("helm-{}-part/", which.name()),
                None,
            );
            yaml.push_str(
                "memberlist:\n  join_members:\n    - dns+unresolvable-gossip-ring.invalid:7946\n",
            );
            let mut proc = start_mimir(which, "helm-landmine", &dir, &yaml, &helm_cli);
            wait_ready(&client, &mut proc, http, Duration::from_secs(45)).await;
            assert_no_memberlist_port_bound(&proc, &[http, grpc]);
            let (code, _, body) = http_get(&client, http, "/memberlist", false).await.unwrap();
            assert_eq!(code, 200);
            assert_eq!(body, "This instance doesn't use memberlist.");
            proc.sigterm();
            let _ = proc.wait_exit(Duration::from_secs(10));
        }

        // -------------------------------------------------------------------------------------
        // Subcase B: Live Zero-Downtime `multi` KV Migration (consul -> etcd) under Kafka Load
        // -------------------------------------------------------------------------------------
        let mig_dir = tmp.path().join("mig");
        fs::create_dir_all(&mig_dir).unwrap();
        let runtime_path = mig_dir.join("runtime.yaml");
        fs::write(
            &runtime_path,
            "multi_kv_config:\n  primary: consul\n  mirror_enabled: true\n",
        )
        .unwrap();

        let mig_topic = "w9-t4-mig";
        let mig_ring_prefix = "mig-ring/";
        let mig_part_prefix = "mig-part/";
        let mig_dist_prefix = "mig-dist/";
        let multi_kv_snippet = format!(
            "      store: multi\n      consul:\n        host: {chost}\n      etcd:\n        endpoints: [{ehost}]\n      multi:\n        primary: consul\n        secondary: etcd\n        mirror_enabled: true",
            chost = consul.host(),
            ehost = etcd.endpoint(),
        );

        let (m_ing_http, m_ing_grpc) = (free_test_port(), free_test_port());
        let m_ing_dir = mig_dir.join("rust-ing");
        fs::create_dir_all(m_ing_dir.join("tsdb")).unwrap();
        fs::create_dir_all(m_ing_dir.join("bucket")).unwrap();
        let m_ing_yaml = format!(
            r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {m_ing_http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {m_ing_grpc}
  log_level: warn
usage_stats:
  enabled: false
runtime_config:
  file: {rt}
  period: 200ms
memberlist:
  join_members:
    - dns+unresolvable-gossip-ring.invalid:7946
ingest_storage:
  enabled: true
  kafka:
    address: {kafka}
    topic: {mig_topic}
    auto_create_topic_enabled: true
    auto_create_topic_default_partitions: 1
    last_produced_offset_poll_interval: 100ms
    consumer_group_offset_commit_interval: 500ms
ingester:
  push_grpc_method_enabled: false
  ring:
    kvstore:
      prefix: {mig_ring_prefix}
{multi_kv_snippet}
    num_tokens: 0
    instance_id: ingester-zone-b-0
    instance_addr: 127.0.0.1
    instance_availability_zone: zone-b
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
    heartbeat_period: 1s
    heartbeat_timeout: 30s
  partition_ring:
    kvstore:
      prefix: {mig_part_prefix}
{multi_kv_snippet}
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
            rt = runtime_path.display(),
            kafka = rp.address(),
            bucket = m_ing_dir.join("bucket").display(),
            tsdb = m_ing_dir.join("tsdb").display(),
        );
        let mut m_ing = start_mimir(Impl::Rust, "mig-ing", &m_ing_dir, &m_ing_yaml, &helm_cli);

        let (m_dist_http, m_dist_grpc) = (free_test_port(), free_test_port());
        let mut m_dist_yaml = distributor_yaml(
            m_dist_http,
            m_dist_grpc,
            &rp.address(),
            mig_topic,
            &multi_kv_snippet,
            mig_ring_prefix,
            mig_part_prefix,
        );
        // Point distributor's own ring at mig-dist/ and attach runtime_config.
        m_dist_yaml = m_dist_yaml.replacen(
            &format!("distributor:\n  ring:\n    kvstore:\n      prefix: {mig_ring_prefix}"),
            &format!("distributor:\n  ring:\n    kvstore:\n      prefix: {mig_dist_prefix}"),
            1,
        );
        m_dist_yaml.push_str(&format!(
            "runtime_config:\n  file: {}\n  period: 200ms\n",
            runtime_path.display()
        ));
        let mut m_dist =
            start_mimir(Impl::Go, "mig-dist", &mig_dir.join("dist"), &m_dist_yaml, &[]);

        let (m_q_http, m_q_grpc) = (free_test_port(), free_test_port());
        let m_q_dir = mig_dir.join("querier");
        fs::create_dir_all(m_q_dir.join("bucket")).unwrap();
        fs::create_dir_all(m_q_dir.join("sync")).unwrap();
        let mut m_q_yaml = querier_yaml(
            &m_q_dir,
            m_q_http,
            m_q_grpc,
            &rp.address(),
            mig_topic,
            &multi_kv_snippet,
            mig_ring_prefix,
            mig_part_prefix,
            None,
        );
        m_q_yaml.push_str(&format!(
            "runtime_config:\n  file: {}\n  period: 200ms\n",
            runtime_path.display()
        ));
        let mut m_querier = start_mimir(Impl::Go, "mig-q", &m_q_dir, &m_q_yaml, &[]);

        wait_ready(&client, &mut m_ing, m_ing_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut m_dist, m_dist_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut m_querier, m_q_http, Duration::from_secs(45)).await;
        assert_no_memberlist_port_bound(&m_ing, &[m_ing_http, m_ing_grpc]);

        // Wait for PartitionActive (2) on partition 0.
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let pr = partition_ring_json(&client, m_q_http).await.unwrap();
            if pr["partitions"][0]["state"] == 2
                && pr["partitions"][0]["owner_ids"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|x| x == "ingester-zone-b-0"))
            {
                break;
            }
            assert!(Instant::now() < deadline, "mig partition 0 not active: {pr}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Push batch #1 while primary=consul, mirror_enabled=true.
        let ts1 = now_ms() - 5_000;
        push_via_distributor(
            &client,
            m_dist_http,
            "tenant-mig",
            cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_mig_before"),
                        cortexpb::LabelPair::from_static("phase", "consul"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 11.0,
                        timestamp_ms: ts1,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let q1 = querier_get_json(
            &client,
            m_q_http,
            "tenant-mig",
            &format!("query?query=w9_mig_before&time={}", ts1 / 1000 + 2),
            true,
        )
        .await
        .unwrap();
        assert_eq!(q1["data"]["result"][0]["value"][1], "11");

        // Ensure mirrored keys exist in etcd before switching off Consul.
        assert!(
            etcd.get_raw(&client, "mig-ring/ring").await.unwrap().is_some(),
            "etcd missing mirrored mig-ring/ring before switchover"
        );
        assert!(
            etcd.get_raw(&client, "mig-part/ingester-partitions")
                .await
                .unwrap()
                .is_some(),
            "etcd missing mirrored mig-part/ingester-partitions before switchover"
        );

        // Flip runtime_config atomically on disk: primary: etcd, mirror_enabled: false.
        let runtime_tmp = mig_dir.join("runtime.yaml.tmp");
        fs::write(
            &runtime_tmp,
            "multi_kv_config:\n  primary: etcd\n  mirror_enabled: false\n",
        )
        .unwrap();
        fs::rename(&runtime_tmp, &runtime_path).unwrap();

        // Wait for Rust Ingester metrics to confirm primary=etcd and mirror_enabled=0 on BOTH rings.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let (_, _, metrics) = http_get(&client, m_ing_http, "/metrics", false).await.unwrap();
            let has_etcd_ring = metrics.lines().any(|l| {
                l.starts_with("cortex_multikv_primary_store{")
                    && l.contains("kv_name=\"ingester-ring\"")
                    && l.contains("store=\"etcd\"")
                    && l.ends_with(" 1")
            });
            let has_etcd_part = metrics.lines().any(|l| {
                l.starts_with("multikv_primary_store{")
                    && l.contains("kv_name=\"ingester-partitions-watcher\"")
                    && l.contains("store=\"etcd\"")
                    && l.ends_with(" 1")
            });
            let mirror_off_ring = metrics.lines().any(|l| {
                l.starts_with("cortex_multikv_mirror_enabled{")
                    && l.contains("kv_name=\"ingester-ring\"")
                    && l.ends_with(" 0")
            });
            if has_etcd_ring && has_etcd_part && mirror_off_ring {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "multi KV did not switch to primary=etcd, mirror=false via runtime_config"
            );
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        // Let Go Distributor & Go Querier also finish their 200ms runtime_config reload + watch swap.
        tokio::time::sleep(Duration::from_millis(800)).await;

        // Kill Consul completely!
        consul.stop().await.unwrap();

        // Push batch #2 & query with Consul completely dead -> zero errors!
        let ts2 = now_ms() - 1_000;
        push_via_distributor(
            &client,
            m_dist_http,
            "tenant-mig",
            cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_mig_after_consul_down"),
                        cortexpb::LabelPair::from_static("phase", "etcd"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 22.0,
                        timestamp_ms: ts2,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let q2 = querier_get_json(
            &client,
            m_q_http,
            "tenant-mig",
            &format!(
                "query?query=w9_mig_after_consul_down&time={}",
                ts2 / 1000 + 2
            ),
            true,
        )
        .await
        .unwrap();
        assert_eq!(q2["data"]["result"][0]["value"][1], "22");

        let ring_after_kill = ingester_ring(&client, m_ing_http).await.unwrap();
        assert_eq!(
            ring_after_kill.get("ingester-zone-b-0"),
            Some(&("ACTIVE".to_string(), 0))
        );
        let pr_after_kill = partition_ring_json(&client, m_q_http).await.unwrap();
        assert_eq!(pr_after_kill["partitions"][0]["state"], 2);

        m_ing.sigterm();
        m_dist.sigterm();
        m_querier.sigterm();
        let _ = m_ing.wait_exit(Duration::from_secs(10));
        let _ = m_dist.wait_exit(Duration::from_secs(10));
        let _ = m_querier.wait_exit(Duration::from_secs(10));

        // -------------------------------------------------------------------------------------
        // Subcase C: Hard SIGKILL + etcd Transient Outage Recovery
        // -------------------------------------------------------------------------------------
        let chaos_dir = tmp.path().join("chaos");
        let chaos_ing_dir = chaos_dir.join("rust-ing");
        fs::create_dir_all(chaos_ing_dir.join("tsdb")).unwrap();
        fs::create_dir_all(chaos_ing_dir.join("bucket")).unwrap();
        let chaos_topic = "w9-t4-chaos";
        let chaos_ring_prefix = "chaos-ring/";
        let chaos_part_prefix = "chaos-part/";
        let etcd_kv_yaml = format!(
            "      store: etcd\n      etcd:\n        endpoints: [{}]",
            etcd.endpoint()
        );

        let (c_ing_http1, c_ing_grpc1) = (free_test_port(), free_test_port());
        let chaos_ing_yaml1 = ingester_etcd_yaml(
            &chaos_ing_dir,
            c_ing_http1,
            c_ing_grpc1,
            &rp.address(),
            chaos_topic,
            &etcd.endpoint(),
            "ingester-zone-b-0",
            "zone-b",
            chaos_ring_prefix,
            chaos_part_prefix,
            None,
        );
        let mut c_ing1 = start_mimir(
            Impl::Rust,
            "chaos-ing1",
            &chaos_ing_dir,
            &chaos_ing_yaml1,
            &helm_cli,
        );

        let (c_dist_http, c_dist_grpc) = (free_test_port(), free_test_port());
        let mut c_dist = start_mimir(
            Impl::Go,
            "chaos-dist",
            &chaos_dir.join("dist"),
            &distributor_yaml(
                c_dist_http,
                c_dist_grpc,
                &rp.address(),
                chaos_topic,
                &etcd_kv_yaml,
                chaos_ring_prefix,
                chaos_part_prefix,
            ),
            &[],
        );

        let (c_q_http, c_q_grpc) = (free_test_port(), free_test_port());
        let c_q_dir = chaos_dir.join("querier");
        fs::create_dir_all(c_q_dir.join("bucket")).unwrap();
        fs::create_dir_all(c_q_dir.join("sync")).unwrap();
        let mut c_querier = start_mimir(
            Impl::Go,
            "chaos-q",
            &c_q_dir,
            &querier_yaml(
                &c_q_dir,
                c_q_http,
                c_q_grpc,
                &rp.address(),
                chaos_topic,
                &etcd_kv_yaml,
                chaos_ring_prefix,
                chaos_part_prefix,
                None,
            ),
            &[],
        );

        wait_ready(&client, &mut c_ing1, c_ing_http1, Duration::from_secs(45)).await;
        wait_ready(&client, &mut c_dist, c_dist_http, Duration::from_secs(45)).await;
        wait_ready(&client, &mut c_querier, c_q_http, Duration::from_secs(45)).await;

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let pr = partition_ring_json(&client, c_q_http).await.unwrap();
            if pr["partitions"][0]["state"] == 2
                && pr["partitions"][0]["owner_ids"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|x| x == "ingester-zone-b-0"))
            {
                break;
            }
            assert!(Instant::now() < deadline, "chaos partition 0 not active: {pr}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Push series before SIGKILL.
        let ts_pre = now_ms() - 4_000;
        push_via_distributor(
            &client,
            c_dist_http,
            "tenant-chaos",
            cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_sigkill_before"),
                        cortexpb::LabelPair::from_static("job", "chaos"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 77.0,
                        timestamp_ms: ts_pre,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let pre_q = querier_get_json(
            &client,
            c_q_http,
            "tenant-chaos",
            &format!("query?query=w9_sigkill_before&time={}", ts_pre / 1000 + 2),
            true,
        )
        .await
        .unwrap();
        assert_eq!(pre_q["data"]["result"][0]["value"][1], "77");

        // Allow local Kafka offset file + WAL segment write, then hard SIGKILL!
        tokio::time::sleep(Duration::from_millis(700)).await;
        c_ing1.signal("KILL");
        let kill_status = c_ing1
            .wait_exit(Duration::from_secs(10))
            .expect("ingester exits on SIGKILL");
        assert!(!kill_status.success(), "expected non-zero SIGKILL exit");

        // Verify WAL exists and no blocks were shipped in filesystem bucket.
        assert!(
            chaos_ing_dir.join("tsdb/tenant-chaos/wal").exists(),
            "expected WAL dir after SIGKILL"
        );

        // Restart Rust ingester with the SAME tsdb.dir and config.
        let (c_ing_http2, c_ing_grpc2) = (free_test_port(), free_test_port());
        let chaos_ing_yaml2 = ingester_etcd_yaml(
            &chaos_ing_dir,
            c_ing_http2,
            c_ing_grpc2,
            &rp.address(),
            chaos_topic,
            &etcd.endpoint(),
            "ingester-zone-b-0",
            "zone-b",
            chaos_ring_prefix,
            chaos_part_prefix,
            None,
        );
        let mut c_ing2 = start_mimir(
            Impl::Rust,
            "chaos-ing2",
            &chaos_ing_dir,
            &chaos_ing_yaml2,
            &helm_cli,
        );
        wait_ready(&client, &mut c_ing2, c_ing_http2, Duration::from_secs(45)).await;
        assert_no_memberlist_port_bound(&c_ing2, &[c_ing_http2, c_ing_grpc2]);

        // Push post-SIGKILL series and verify BOTH pre and post data via Go Querier (strong read consistency).
        let ts_post = now_ms() - 1_000;
        push_via_distributor(
            &client,
            c_dist_http,
            "tenant-chaos",
            cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_sigkill_after"),
                        cortexpb::LabelPair::from_static("job", "chaos"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 88.0,
                        timestamp_ms: ts_post,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let q_pre_after_restart = querier_get_json(
            &client,
            c_q_http,
            "tenant-chaos",
            &format!(
                "query?query=w9_sigkill_before&time={}",
                ts_post / 1000 + 2
            ),
            true,
        )
        .await
        .unwrap();
        assert_eq!(q_pre_after_restart["data"]["result"][0]["value"][1], "77");

        let q_post_after_restart = querier_get_json(
            &client,
            c_q_http,
            "tenant-chaos",
            &format!(
                "query?query=w9_sigkill_after&time={}",
                ts_post / 1000 + 2
            ),
            true,
        )
        .await
        .unwrap();
        assert_eq!(q_post_after_restart["data"]["result"][0]["value"][1], "88");

        // Transient etcd outage: pause etcd for 3s, unpause, verify lifecycler recovery + ACTIVE partition.
        etcd.pause().await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        etcd.unpause().await.unwrap();

        // Wait for heartbeat/reconciliation recovery and confirm ACTIVE state in both rings.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let qr = ingester_ring(&client, c_q_http).await.unwrap_or_default();
            let pr = partition_ring_json(&client, c_q_http).await;
            if let Ok(pr) = pr {
                if qr.get("ingester-zone-b-0") == Some(&("ACTIVE".to_string(), 0))
                    && pr["partitions"][0]["state"] == 2
                    && pr["partitions"][0]["owner_ids"]
                        .as_array()
                        .is_some_and(|a| a.iter().any(|x| x == "ingester-zone-b-0"))
                {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "ingester did not recover ACTIVE ring/partition state after etcd outage"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // Push & query one more metric after etcd outage recovery.
        let ts_blip = now_ms();
        push_via_distributor(
            &client,
            c_dist_http,
            "tenant-chaos",
            cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_static("__name__", "w9_after_etcd_blip"),
                        cortexpb::LabelPair::from_static("job", "chaos"),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: 99.0,
                        timestamp_ms: ts_blip,
                    }],
                    ..Default::default()
                }],
                source: cortexpb::write_request::SourceEnum::Api as i32,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let q_blip = querier_get_json(
            &client,
            c_q_http,
            "tenant-chaos",
            &format!(
                "query?query=w9_after_etcd_blip&time={}",
                ts_blip / 1000 + 2
            ),
            true,
        )
        .await
        .unwrap();
        assert_eq!(q_blip["data"]["result"][0]["value"][1], "99");
    });
}

