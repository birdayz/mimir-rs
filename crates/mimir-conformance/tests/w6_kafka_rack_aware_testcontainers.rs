//! KAFKA-1 — Kafka `ingest_storage` against a rack-aware (KIP-392 follower fetching) cluster.
//!
//! A 3-broker Redpanda cluster is started with testcontainers-rs, one broker per rack
//! (`zone-a` / `zone-b` / `zone-c`) and `enable_rack_awareness: true` (the Redpanda Cloud multi-AZ
//! shape). The topic has one partition replicated on all three brokers, and the ingesters are
//! configured with `ingest_storage.kafka.client_rack` set to the rack of a FOLLOWER of that
//! partition — what the Helm chart's `-ingest-storage.kafka.client-rack=<zone>` does for every
//! ingester whose zone does not host the leader.
//!
//! The unmodified Go `mimir` (`target/go-mimir`) and the Rust `mimir` (`target/release/mimir`)
//! are booted as `-target=ingester` with byte-identical configuration, for both reader fetch
//! paths:
//! - `fetch_concurrency_max: 12` (default): `ConcurrentFetchers` reads from the leader without a
//!   rack (Go logs that the rack has no effect);
//! - `fetch_concurrency_max: 0`: the Kafka client consumes the partition, sends the rack and
//!   follows the broker's `preferred_read_replica` to the follower in its rack.
//!
//! Every assertion is differential (Go defines the expected behavior): both become ready, both
//! serve the backlog and the live tail, both commit the same offsets, and the Rust reader does
//! not fetch more than Go while the partition is idle (the pre-fix Rust reader re-sent the rack
//! to the leader forever, busy-looping on empty redirects and never becoming ready).
//!
//! Upstream behavior under test:
//! - github.com/grafana/mimir/pkg/storage/ingest/reader.go (`start`, `run`, `processNextFetches`)
//! - github.com/grafana/mimir/pkg/storage/ingest/fetcher.go (`ConcurrentFetchers`)
//! - github.com/grafana/mimir/pkg/storage/ingest/util.go (`commonKafkaClientOptions`: `kgo.Rack`)
//! - github.com/twmb/franz-go/pkg/kgo/source.go (preferred read replica handling)
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).
//! `MIMIR_CONFORMANCE_RUST_BIN=/path/to/mimir` tests a prebuilt Rust binary instead of
//! rebuilding `target/release/mimir`. Leftover containers carry the label
//! `org.mimir-rs.conformance=kafka-rack-aware`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
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
use mimir_config::{KafkaConfig, StringSliceCSV};
use mimir_conformance::oracle::{
    go_mimir_bin, invoke_oracle, rust_mimir_release_bin, workspace_root, KfakeRecordInput,
    LabelJson, SampleJson, TimeSeriesJson, WriteRequestJson,
};
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use mimir_storage::ingest::KafkaClient;
use mimir_tsdb::chunkenc::{EncodedChunk, Encoding};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};

const REDPANDA_IMAGE: &str = "redpandadata/redpanda";
const REDPANDA_TAG: &str =
    "v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "kafka-rack-aware");
const RACKS: [&str; 3] = ["zone-a", "zone-b", "zone-c"];

const TENANT: &str = "tenant-kafka-rack";
const SERIES_PER_RECORD: usize = 8;
const RECORDS_PER_BATCH: usize = 2;

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

/// Always rebuilds the Go oracle / Go mimir and the Rust release binary (incremental), so the
/// test never exercises a stale `target/release/mimir`.
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
            return Err(format!("cargo build --release -p mimir-cmd exited with {status}"));
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

/// Distinct free TCP ports below the Linux ephemeral range (see the w6 TLS/SASL suite).
fn free_tcp_ports(n: usize) -> Vec<u16> {
    use std::sync::atomic::{AtomicU32, Ordering};
    const LOW: u32 = 20_000;
    const SPAN: u32 = 12_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let offset = (std::process::id() * 211 + 5_000) % SPAN;
    let mut out = Vec::with_capacity(n);
    for _ in 0..SPAN {
        if out.len() == n {
            break;
        }
        let port = (LOW + (offset + NEXT.fetch_add(1, Ordering::Relaxed)) % SPAN) as u16;
        if ["127.0.0.1", "0.0.0.0"]
            .iter()
            .all(|host| StdTcpListener::bind((*host, port)).is_ok())
        {
            out.push(port);
        }
    }
    assert_eq!(out.len(), n, "no free TCP ports left");
    out
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
        }
    }

    fn exited(&mut self) -> Option<Option<i32>> {
        self.child
            .as_mut()
            .and_then(|c| c.try_wait().ok().flatten())
            .map(|s| s.code())
    }

    fn log_text(&self) -> String {
        String::from_utf8_lossy(&self.log.lock().unwrap()).into_owned()
    }

    fn log_tail(&self, lines: usize) -> String {
        let text = self.log_text();
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..]
            .iter()
            .map(|l| format!("        | {}", l.chars().take(400).collect::<String>()))
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
// 3-broker rack-aware Redpanda cluster
// ---------------------------------------------------------------------------------------------

fn redpanda_yaml(node: usize, names: &[String], ports: &[u16]) -> String {
    let mut y = if node == 0 {
        format!(
            "redpanda:\n  data_directory: /var/lib/redpanda/data\n  node_id: 0\n  rack: {rack}\n  empty_seed_starts_cluster: true\n  seed_servers: []\n",
            rack = RACKS[node]
        )
    } else {
        format!(
            "redpanda:\n  data_directory: /var/lib/redpanda/data\n  node_id: {node}\n  rack: {rack}\n  empty_seed_starts_cluster: false\n  seed_servers:\n    - host: {{address: {seed}, port: 33145}}\n",
            rack = RACKS[node],
            seed = names[0]
        )
    };
    let _ = write!(
        y,
        "  rpc_server: {{address: 0.0.0.0, port: 33145}}\n  advertised_rpc_api: {{address: {name}, port: 33145}}\n  admin: [{{address: 0.0.0.0, port: 9644}}]\n  developer_mode: true\n  kafka_api:\n    - {{name: external, address: 0.0.0.0, port: {port}}}\n  advertised_kafka_api:\n    - {{name: external, address: 127.0.0.1, port: {port}}}\n",
        name = names[node],
        port = ports[node]
    );
    y
}

struct RackCluster {
    nodes: Vec<ContainerAsync<GenericImage>>,
    ports: Vec<u16>,
}

impl RackCluster {
    fn build_node_request(
        node: usize,
        network: &str,
        names: &[String],
        ports: &[u16],
    ) -> testcontainers::ContainerRequest<GenericImage> {
        GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
            .with_entrypoint("/bin/sh")
            .with_cmd([
                "-c",
                "cp /config/redpanda.yaml /etc/redpanda/redpanda.yaml && exec rpk redpanda start --mode dev-container --smp 1 --memory 512M --reserve-memory 0M --default-log-level=warn",
            ])
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_network(network.to_string())
            .with_container_name(names[node].clone())
            .with_startup_timeout(Duration::from_secs(180))
            .with_copy_to(
                CopyTargetOptions::new("/config/redpanda.yaml").with_mode(0o644),
                redpanda_yaml(node, names, ports).into_bytes(),
            )
            .with_mapped_port(ports[node], ContainerPort::Tcp(ports[node]))
    }

    async fn start() -> Result<Self> {
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        );
        let network = format!("mimir-kafka-rack-{suffix}");
        let names: Vec<String> = (0..3).map(|n| format!("rp{n}-{suffix}")).collect();
        let ports = free_tcp_ports(3);

        let node0 = Self::build_node_request(0, &network, &names, &ports)
            .start()
            .await
            .context("starting Redpanda seed node 0 via testcontainers (is Docker running?)")?;
        let mut c = Self {
            nodes: vec![node0],
            ports: ports.clone(),
        };
        c.wait_seed_ready().await?;

        let starts = (1..3).map(|node| Self::build_node_request(node, &network, &names, &ports).start());
        for (idx, r) in futures::future::join_all(starts).await.into_iter().enumerate() {
            c.nodes.push(
                r.with_context(|| format!("starting Redpanda node {} via testcontainers", idx + 1))?,
            );
        }
        c.wait_all_brokers_ready().await?;
        c.rpk(0, &["cluster", "config", "set", "enable_rack_awareness", "true"])
            .await?;
        Ok(c)
    }

    async fn wait_seed_ready(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if matches!(self.nodes[0].is_running().await, Ok(false)) {
                let logs = self.dump_logs().await;
                bail!("Redpanda seed node 0 exited during startup:\n{logs}");
            }
            let out = tokio::time::timeout(
                Duration::from_secs(15),
                self.rpk(0, &["cluster", "info"]),
            )
            .await;
            if let Ok(Ok(text)) = &out {
                if text.contains("BROKERS") {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                let logs = self.dump_logs().await;
                bail!("Redpanda seed node 0 admin/Kafka API never became ready: {out:?}\n{logs}");
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn wait_all_brokers_ready(&self) -> Result<()> {
        let _ = self
            .rpk(
                0,
                &["cluster", "config", "set", "storage_space_alert_free_threshold_percent", "0"],
            )
            .await;
        self.wait_nodes_healthy(&["0", "1", "2"]).await
    }

    async fn dump_logs(&self) -> String {
        let mut s = String::new();
        for (i, n) in self.nodes.iter().enumerate() {
            let running = n.is_running().await.ok();
            let out = n.stdout_to_vec().await.unwrap_or_default();
            let err = n.stderr_to_vec().await.unwrap_or_default();
            let _ = writeln!(
                s,
                "--- node {i} (running={running:?}) ---\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&out),
                String::from_utf8_lossy(&err),
            );
        }
        s
    }

    async fn wait_nodes_healthy(&self, expected: &[&str]) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            for (i, n) in self.nodes.iter().enumerate() {
                if matches!(n.is_running().await, Ok(false)) {
                    let logs = self.dump_logs().await;
                    bail!("Redpanda node {i} exited before cluster became healthy:\n{logs}");
                }
            }
            let out = tokio::time::timeout(
                Duration::from_secs(30),
                self.rpk(0, &["cluster", "health"]),
            )
            .await;
            if let Ok(Ok(text)) = &out {
                let healthy = text.lines().any(|l| l.contains("Healthy:") && l.contains("true"));
                let all_up = text
                    .lines()
                    .any(|l| l.contains("All nodes:") && expected.iter().all(|n| l.contains(n)));
                if healthy && all_up {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                let logs = self.dump_logs().await;
                bail!("3-broker Redpanda cluster never became healthy (expected={expected:?}): {out:?}\n{logs}");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn rpk(&self, node: usize, args: &[&str]) -> Result<String> {
        let cmd: Vec<String> = std::iter::once("rpk")
            .chain(args.iter().copied())
            .map(str::to_string)
            .collect();
        let mut res = self.nodes[node].exec(ExecCommand::new(cmd)).await?;
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

    fn address(&self) -> String {
        self.ports
            .iter()
            .map(|p| format!("127.0.0.1:{p}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Creates the topic (1 partition, RF 3) and returns its leader once elected.
    async fn create_topic(&self, topic: &str) -> Result<i32> {
        let cfg = KafkaConfig {
            address: StringSliceCSV(vec![self.address()]),
            topic: topic.to_string(),
            ..Default::default()
        };
        let client = KafkaClient::from_config(&cfg)?;
        client.create_topics(&[topic], 1, 3).await?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let _ = client.refresh_metadata(&[topic]).await;
            let (leader, _) = client.cached_partition_leader(topic, 0);
            if leader >= 0 {
                return Ok(leader);
            }
            ensure!(Instant::now() < deadline, "no leader elected for {topic}");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Producing through the Go oracle (upstream `ingest.NewKafkaWriterClient`)
// ---------------------------------------------------------------------------------------------

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

async fn oracle_kafka(
    address: &str,
    topic: &str,
    records: Vec<KfakeRecordInput>,
    consumer_groups: &[String],
) -> Result<KafkaProduceResp> {
    let req = KafkaProduceReq {
        kafka_yaml: format!("address: {address}\ntopic: {topic}\n"),
        records,
        consumer_groups: consumer_groups.to_vec(),
    };
    tokio::task::spawn_blocking(move || invoke_oracle("kafka-produce", &req)).await?
}

type Series = (Vec<(String, String)>, Vec<(i64, u64)>);

fn series_labels(scenario: &str, i: usize) -> Vec<(String, String)> {
    vec![
        ("__name__".into(), "kafka_rack_requests_total".into()),
        ("pod".into(), format!("pod-{i:02}")),
        ("scenario".into(), scenario.into()),
    ]
}

fn sample_at(base_ts: i64, batch: usize, i: usize) -> (i64, f64) {
    (
        base_ts + batch as i64 * 15_000,
        (batch * 1000 + i) as f64 + 0.5,
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

// ---------------------------------------------------------------------------------------------
// gRPC QueryStream with strong read consistency
// ---------------------------------------------------------------------------------------------

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

/// `offset = None`: strong consistency up to the last produced offset (ListOffsets path).
async fn query_stream(grpc_port: u16, offset: Option<i64>, base_ts: i64) -> Result<Vec<Series>> {
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
    md.insert("__consistency_level__", MetadataValue::from_static("strong"));
    if let Some(off) = offset {
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
    group: String,
    proc: ProcessGuard,
}

fn spawn_instance(side: Side, root: &Path, http: u16, grpc: u16, kafka_block: &str, group: &str) -> Instance {
    let dir = root.join(side.label());
    fs::create_dir_all(dir.join("tsdb")).unwrap();
    fs::create_dir_all(dir.join("bucket")).unwrap();
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
    instance_id: ingester-zone-a-0
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
    consumer_group: {group}
{kafka_block}blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    );
    let path = dir.join("mimir.yaml");
    fs::write(&path, cfg).unwrap();
    Instance {
        side,
        http,
        grpc,
        group: group.to_string(),
        proc: ProcessGuard::spawn(side.label(), &side.bin(), &path, &dir),
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(2000))
        .pool_max_idle_per_host(0)
        .build()
        .unwrap()
}

/// Polls `/ready` until 200 (`Ok(elapsed)`), the process exits, or `timeout`.
async fn wait_ready(inst: &mut Instance, timeout: Duration) -> std::result::Result<Duration, String> {
    let http = http_client();
    let start = Instant::now();
    let mut last = String::from("no HTTP answer");
    while start.elapsed() < timeout {
        if let Some(code) = inst.proc.exited() {
            return Err(format!("exited with {code:?}"));
        }
        if let Ok(resp) = http
            .get(format!("http://127.0.0.1:{}/ready", inst.http))
            .send()
            .await
        {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            if status == 200 {
                return Ok(start.elapsed());
            }
            last = format!("/ready {status} {:?}", body.chars().take(200).collect::<String>());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("not ready after {timeout:?}: {last}"))
}

/// Sum of the samples of `name` whose labels contain all of `labels`, from `/metrics`.
async fn scrape(inst: &Instance, name: &str, labels: &[(&str, &str)]) -> Result<f64> {
    let text = http_client()
        .get(format!("http://127.0.0.1:{}/metrics", inst.http))
        .send()
        .await?
        .text()
        .await?;
    let mut sum = 0.0;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let (metric, lbls) = match series.split_once('{') {
            Some((m, l)) => (m, l.trim_end_matches('}')),
            None => (series, ""),
        };
        if metric != name {
            continue;
        }
        if labels
            .iter()
            .all(|(k, v)| lbls.split(',').any(|kv| kv == format!("{k}=\"{v}\"")))
        {
            sum += value.parse::<f64>().unwrap_or(0.0);
        }
    }
    Ok(sum)
}

// ---------------------------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------------------------

async fn run_scenario(
    cluster: &RackCluster,
    scenario: &str,
    fetch_concurrency_max: Option<i64>,
) -> Vec<String> {
    let mut errs = Vec::new();
    let address = cluster.address();
    let topic = format!("kafka-{scenario}");
    let leader = match cluster.create_topic(&topic).await {
        Ok(l) => l,
        Err(e) => return vec![format!("harness: creating topic {topic}: {e:#}")],
    };
    // The client rack is a follower's: the partition leader is in another zone.
    let follower = (leader + 1) % 3;
    let client_rack = RACKS[follower as usize];
    eprintln!(
        "{scenario}: partition leader node {leader} ({}), client_rack {client_rack} (node {follower})",
        RACKS[leader as usize]
    );
    let base_ts = now_ms() - 10 * 60_000;

    // Backlog produced before the ingesters start.
    let end0 = match oracle_kafka(&address, &topic, batch_records(scenario, base_ts, 0), &[]).await {
        Ok(r) => r.end_offsets.get("0").copied().unwrap_or(-1),
        Err(e) => return vec![format!("harness: producing backlog: {e:#}")],
    };

    let mut kafka_block = format!(
        "    address: {address}\n    topic: {topic}\n    client_rack: {client_rack}\n    consume_from_position_at_startup: start\n    consumer_group_offset_commit_interval: 100ms\n    last_produced_offset_poll_interval: 50ms\n    auto_create_topic_enabled: false\n"
    );
    if let Some(c) = fetch_concurrency_max {
        let _ = writeln!(kafka_block, "    fetch_concurrency_max: {c}");
    }
    let dir = tempfile::tempdir().unwrap();
    let ports = free_tcp_ports(4);
    let mut go = spawn_instance(
        Side::Go,
        dir.path(),
        ports[0],
        ports[1],
        &kafka_block,
        &format!("{scenario}-go"),
    );
    let mut rust = spawn_instance(
        Side::Rust,
        dir.path(),
        ports[2],
        ports[3],
        &kafka_block,
        &format!("{scenario}-rust"),
    );

    let (g, r) = tokio::join!(
        wait_ready(&mut go, Duration::from_secs(90)),
        wait_ready(&mut rust, Duration::from_secs(90))
    );
    let tails = |go: &Instance, rust: &Instance| {
        format!(
            "      go log tail:\n{}\n      rust log tail:\n{}",
            go.proc.log_tail(25),
            rust.proc.log_tail(25)
        )
    };
    if let Err(e) = g {
        errs.push(format!("harness: Go Mimir must become ready: {e}"));
        errs.push(tails(&go, &rust));
        return errs;
    }
    if let Err(e) = r {
        errs.push(format!("Rust never became ready (Go did): {e}"));
        if let Ok(f) = scrape(&rust, "cortex_ingest_storage_reader_fetches_total", &[]).await {
            errs.push(format!("Rust cortex_ingest_storage_reader_fetches_total = {f}"));
        }
        errs.push(tails(&go, &rust));
        return errs;
    }

    // Backlog through strong consistency at an explicit offset.
    let want = expected_series(scenario, base_ts, 1);
    for inst in [&go, &rust] {
        match query_stream(inst.grpc, Some(end0 - 1), base_ts).await {
            Ok(got) if got == want => {}
            Ok(got) => errs.push(format!(
                "{} backlog QueryStream: want {} series, got {} ({:?})",
                inst.side.label(),
                want.len(),
                got.len(),
                got.first()
            )),
            Err(e) => errs.push(format!("{} backlog QueryStream: {e:#}", inst.side.label())),
        }
    }

    // Idle partition: count fetches over a window. Go only counts fetches that returned records
    // (kgo PollFetches / ConcurrentFetchers.PollFetches block), so it stays flat; the pre-fix Rust
    // reader counted ~10k empty fetches/s here.
    let window = Duration::from_secs(4);
    let metric = "cortex_ingest_storage_reader_fetches_total";
    let (g0, r0) = (
        scrape(&go, metric, &[]).await.unwrap_or(f64::NAN),
        scrape(&rust, metric, &[]).await.unwrap_or(f64::NAN),
    );
    tokio::time::sleep(window).await;
    let (g1, r1) = (
        scrape(&go, metric, &[]).await.unwrap_or(f64::NAN),
        scrape(&rust, metric, &[]).await.unwrap_or(f64::NAN),
    );
    if r1 - r0 > (g1 - g0) + 2.0 || r1.is_nan() {
        errs.push(format!(
            "idle partition: Rust fetches_total grew by {} in {window:?}, Go by {}",
            r1 - r0,
            g1 - g0
        ));
    }

    // Live tail through strong consistency up to the last produced offset.
    let end1 = match oracle_kafka(&address, &topic, batch_records(scenario, base_ts, 1), &[]).await {
        Ok(r) => r.end_offsets.get("0").copied().unwrap_or(-1),
        Err(e) => {
            errs.push(format!("harness: producing live batch: {e:#}"));
            return errs;
        }
    };
    let want = expected_series(scenario, base_ts, 2);
    for inst in [&go, &rust] {
        match query_stream(inst.grpc, None, base_ts).await {
            Ok(got) if got == want => {}
            Ok(got) => errs.push(format!(
                "{} live-tail QueryStream: want {} series, got {}",
                inst.side.label(),
                want.len(),
                got.len()
            )),
            Err(e) => errs.push(format!("{} live-tail QueryStream: {e:#}", inst.side.label())),
        }
    }

    // Both commit the same consumer-group offset.
    let groups = vec![go.group.clone(), rust.group.clone()];
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last: (i64, i64);
    loop {
        match oracle_kafka(&address, &topic, Vec::new(), &groups).await {
            Ok(resp) => {
                let g = resp.committed.get(&groups[0]).copied().unwrap_or(-2);
                let r = resp.committed.get(&groups[1]).copied().unwrap_or(-2);
                if g >= end1 - 1 && g == r {
                    break;
                }
                last = (g, r);
            }
            Err(e) => {
                errs.push(format!("harness: committed offsets: {e:#}"));
                break;
            }
        }
        if Instant::now() >= deadline {
            errs.push(format!(
                "committed offsets never converged (end {end1}): (Go, Rust) = {last:?}"
            ));
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Follower fetching (client path only): both read from the follower in their rack.
    if fetch_concurrency_max == Some(0) {
        let node = follower.to_string();
        let lbl = [("component", "partition-reader"), ("node_id", node.as_str())];
        let gb = scrape(&go, "cortex_ingest_storage_reader_read_bytes_total", &lbl)
            .await
            .unwrap_or(0.0);
        let rb = scrape(&rust, "cortex_ingest_storage_reader_read_bytes_total", &lbl)
            .await
            .unwrap_or(0.0);
        if gb <= 0.0 {
            errs.push(format!(
                "harness: Go read no bytes from the zone follower (node {follower})"
            ));
        } else if rb <= 0.0 {
            errs.push(format!(
                "Rust read no bytes from the zone follower (node {follower}); Go read {gb}"
            ));
        }
    } else {
        // With concurrent fetching both log that the rack has no effect.
        let warn = "the configured Kafka client rack has no effect because concurrent fetching is enabled";
        if !go.proc.log_text().contains(warn) {
            errs.push("harness: Go did not warn that the client rack has no effect".to_string());
        } else if !rust.proc.log_text().contains(warn) {
            errs.push("Rust did not warn that the client rack has no effect (Go did)".to_string());
        }
    }

    if !errs.is_empty() {
        errs.push(tails(&go, &rust));
    }
    errs
}

/// Both reader fetch paths on a rack-aware cluster with the client rack set to a follower's.
#[test]
fn kafka_rack_aware_follower_fetching_go_rust_parity() {
    if skip_docker("kafka_rack_aware_follower_fetching_go_rust_parity") {
        return;
    }
    ensure_fresh_binaries();
    block_on_for_tests(async {
        let cluster = RackCluster::start()
            .await
            .expect("start the 3-broker rack-aware Redpanda cluster");
        let mut report = String::new();
        let mut failed = 0;
        for (scenario, conc) in [
            ("rack-concurrent-fetchers", None),
            ("rack-client-fetch", Some(0)),
        ] {
            let errs = run_scenario(&cluster, scenario, conc).await;
            let status = if errs.is_empty() { "ok  " } else { "FAIL" };
            let _ = writeln!(
                report,
                "[{status}] {scenario} (fetch_concurrency_max: {})",
                conc.map_or("12 (default)".to_string(), |c| c.to_string())
            );
            for e in &errs {
                let _ = writeln!(report, "    - {e}");
            }
            if !errs.is_empty() {
                failed += 1;
            }
        }
        eprintln!("\n===== KAFKA-1 rack-aware follower fetching =====\n{report}");
        assert_eq!(failed, 0, "{report}");
        drop(cluster);
    });
}
