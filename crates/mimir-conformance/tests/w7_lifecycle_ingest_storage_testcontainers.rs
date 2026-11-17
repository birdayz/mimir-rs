//! Ingest-storage lifecycle differential tests against a real Redpanda broker (Docker).
//!
//! The unmodified Go `mimir` (`target/go-mimir`) and the Rust `mimir` are booted as
//! `-target=ingester` with identical `ingest_storage` configuration; Go defines the expected
//! behavior.
//!
//! Upstream behavior under test:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (`New`: tokenless `BasicLifecycler` when
//!   `-ingester.ring.num-tokens=0`, the jsonnet default with ingest storage) and
//!   pkg/ingester/ingester_ring_lifecycler.go (`tokenlessLifecycler.CheckReady`) — hunt LC-1
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (`starting`: the partition instance
//!   lifecycler is started only after the partition reader caught up and the instance is
//!   ACTIVE in the ring) — hunt LC-3
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).
//! `MIMIR_CONFORMANCE_RUST_BIN=/path/to/mimir` tests a prebuilt Rust binary (e.g. the Go one,
//! to prove the harness Go-vs-Go first). Leftover containers carry the label
//! `org.mimir-rs.conformance=lifecycle`.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use testcontainers::core::{ContainerPort, ExecCommand};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};

use mimir_common::block_on_for_tests;
use mimir_conformance::twin::{
    free_tcp_ports, http_get, ingester_ring, ring_metric_series, start_mimir, wait_ready, Impl,
    Proc,
};

const REDPANDA_IMAGE: &str = "redpandadata/redpanda";
const REDPANDA_TAG: &str =
    "v26.2.7@sha256:43545ee03966431f5451e0b3c1e651b39fbe3eb383360220311021f50133b649";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "lifecycle");
const INSTANCE_ID: &str = "ingester-zone-a-0";

fn skip_docker(test: &str) -> bool {
    if std::env::var("MIMIR_CONFORMANCE_SKIP_DOCKER").as_deref() == Ok("1") {
        eprintln!("SKIPPED {test}: MIMIR_CONFORMANCE_SKIP_DOCKER=1 (this test needs Docker)");
        return true;
    }
    false
}

struct Redpanda {
    container: ContainerAsync<GenericImage>,
    port: u16,
}

impl Redpanda {
    async fn start() -> Result<Self> {
        let port = free_tcp_ports(1)[0];
        let yaml = format!(
            "redpanda:\n  data_directory: /var/lib/redpanda/data\n  empty_seed_starts_cluster: true\n  seed_servers: []\n  rpc_server: {{address: 0.0.0.0, port: 33145}}\n  advertised_rpc_api: {{address: 127.0.0.1, port: 33145}}\n  admin: [{{address: 0.0.0.0, port: 9644}}]\n  developer_mode: true\n  kafka_api:\n    - {{name: internal, address: 0.0.0.0, port: 9092}}\n    - {{name: external, address: 0.0.0.0, port: {port}}}\n  advertised_kafka_api:\n    - {{name: internal, address: 127.0.0.1, port: 9092}}\n    - {{name: external, address: 127.0.0.1, port: {port}}}\n"
        );
        let container = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
            .with_entrypoint("/bin/sh")
            .with_cmd([
                "-c",
                "cp /config/redpanda.yaml /etc/redpanda/redpanda.yaml && exec rpk redpanda start --mode dev-container --smp 1 --memory 1G --reserve-memory 0M --default-log-level=info",
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
            .context("starting Redpanda via testcontainers (is Docker running?)")?;
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
        ensure!(code == 0, "rpk {args:?} exited {code}: {}", String::from_utf8_lossy(&stderr));
        Ok(())
    }

    fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

struct Ports {
    http: u16,
    grpc: u16,
}

fn ingest_storage_yaml(p: &Ports, dir: &Path, kafka: &str, topic: &str, ring_extra: &str) -> String {
    format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
usage_stats:
  enabled: false
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
      store: inmemory
    instance_id: {INSTANCE_ID}
    instance_addr: 127.0.0.1
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
{ring_extra}  partition_ring:
    kvstore:
      store: inmemory
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
        http = p.http,
        grpc = p.grpc,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap()
}

/// `GET /ingester/partition-ring` (JSON): `(partition id, state, owners)` sorted by id.
async fn partitions(client: &reqwest::Client, port: u16) -> Result<Vec<(i64, serde_json::Value, Vec<String>)>, String> {
    let (code, _, body) = http_get(client, port, "/ingester/partition-ring", true).await?;
    if code != 200 {
        return Err(format!("{code} {body}"));
    }
    let v: serde_json::Value = serde_json::from_str(&body).map_err(|e| format!("{e}: {body}"))?;
    let mut out: Vec<(i64, serde_json::Value, Vec<String>)> = v["partitions"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|p| {
                    (
                        p["id"].as_i64().unwrap_or(-1),
                        p["state"].clone(),
                        p["owner_ids"]
                            .as_array()
                            .map(|o| o.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
                            .unwrap_or_default(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort_by_key(|p| p.0);
    Ok(out)
}

fn stop_all(procs: &mut [(Impl, Proc, Ports)]) {
    for (_, proc, _) in procs.iter() {
        proc.sigterm();
    }
    for (_, proc, _) in procs.iter_mut() {
        match proc.wait_exit(Duration::from_secs(60)) {
            Some(s) => assert!(s.success(), "{} exited with {s}:\n{}", proc.name, proc.log()),
            None => panic!("{} did not exit:\n{}", proc.name, proc.log()),
        }
    }
}

/// The scenarios each run their own broker, one at a time (keeps the Docker footprint of the
/// suite to a single `--smp 1 --memory 1G` Redpanda).
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// LC-1: `-ingester.ring.num-tokens=0` (jsonnet default with ingest storage) registers a
/// tokenless ACTIVE instance and becomes ready.
#[test]
fn lc1_tokenless_ingester_ring_becomes_ready() {
    if skip_docker("lc1_tokenless_ingester_ring_becomes_ready") {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    block_on_for_tests(async {
        let rp = Redpanda::start().await.expect("start Redpanda");
        let client = http_client();
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut procs = Vec::new();
            for which in [Impl::Go, Impl::Rust] {
                let pp = free_tcp_ports(2);
                let p = Ports { http: pp[0], grpc: pp[1] };
                let dir = tmp.path().join(format!("tokenless-{}", which.name()));
                let yaml = ingest_storage_yaml(
                    &p,
                    &dir,
                    &rp.address(),
                    &format!("lc1-{}", which.name()),
                    "    num_tokens: 0\n    tokens_file_path: \"\"\n",
                );
                let proc = start_mimir(which, "tokenless", &dir, &yaml, &[]);
                procs.push((which, proc, p));
            }
            for (_, proc, p) in procs.iter_mut() {
                wait_ready(&client, proc, p.http, Duration::from_secs(120)).await;
            }
            let mut rings = Vec::new();
            for (_, _, p) in &procs {
                rings.push(ingester_ring(&client, p.http).await.unwrap());
            }
            assert_eq!(rings[0], rings[1], "go vs rust ring in tokenless mode");
            assert_eq!(
                rings[0].get(INSTANCE_ID),
                Some(&("ACTIVE".to_string(), 0)),
                "tokenless instance must be ACTIVE without tokens"
            );
            stop_all(&mut procs);
        }
    });
}

/// LC-3: while the partition reader can't start (Kafka unreachable), the partition must not be
/// created / owned yet; it's registered only once the ingester is ACTIVE.
#[test]
fn lc3_partition_registered_only_after_ingester_is_active() {
    if skip_docker("lc3_partition_registered_only_after_ingester_is_active") {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    block_on_for_tests(async {
        let rp = Redpanda::start().await.expect("start Redpanda");
        let client = http_client();
        let tmp = tempfile::tempdir().unwrap();
        {
            rp.container.pause().await.expect("pause Redpanda");
            let mut procs = Vec::new();
            for which in [Impl::Go, Impl::Rust] {
                let pp = free_tcp_ports(2);
                let p = Ports { http: pp[0], grpc: pp[1] };
                let dir = tmp.path().join(format!("partition-{}", which.name()));
                let yaml = ingest_storage_yaml(
                    &p,
                    &dir,
                    &rp.address(),
                    &format!("lc3-{}", which.name()),
                    "    num_tokens: 16\n",
                );
                let proc = start_mimir(which, "partition", &dir, &yaml, &[]);
                procs.push((which, proc, p));
            }
            // Longer than -ingester.partition-ring.min-partition-owners-duration (10s), so a
            // prematurely registered partition would also have switched to ACTIVE.
            tokio::time::sleep(Duration::from_secs(15)).await;
            let mut views = Vec::new();
            for (_, proc, p) in procs.iter_mut() {
                assert!(proc.try_wait().is_none(), "{} exited:\n{}", proc.name, proc.log());
                let ready = http_get(&client, p.http, "/ready", false).await.unwrap();
                let parts = partitions(&client, p.http).await.unwrap();
                views.push((proc.name.clone(), ready.0, ready.2, parts));
            }
            for (name, code, body, parts) in &views {
                assert_eq!(*code, 503, "{name}: {body}");
                assert!(body.contains("ingester-service: Starting"), "{name}: {body}");
                assert!(parts.is_empty(), "{name}: partition registered before the ingester is ACTIVE: {parts:?}");
            }

            rp.container.unpause().await.expect("unpause Redpanda");
            for (_, proc, p) in procs.iter_mut() {
                wait_ready(&client, proc, p.http, Duration::from_secs(120)).await;
            }
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let mut all = Vec::new();
                for (_, _, p) in &procs {
                    all.push(partitions(&client, p.http).await.unwrap());
                }
                let registered = all
                    .iter()
                    .all(|parts| parts.len() == 1 && parts[0].0 == 0 && parts[0].2 == vec![INSTANCE_ID.to_string()]);
                if registered {
                    break;
                }
                assert!(Instant::now() < deadline, "partition 0 not registered by both: {all:?}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            // LC-8: ring, lifecycler, partition ring and KV client metrics (names + labels).
            let mut series = Vec::new();
            for (_, proc, p) in &procs {
                let (code, _, body) = http_get(&client, p.http, "/metrics", false).await.unwrap();
                assert_eq!(code, 200);
                series.push((proc.name.clone(), ring_metric_series(&body)));
            }
            let only_go: Vec<_> = series[0].1.difference(&series[1].1).collect();
            let only_rs: Vec<_> = series[1].1.difference(&series[0].1).collect();
            assert!(
                only_go.is_empty() && only_rs.is_empty(),
                "metric series differ\nonly in {}: {only_go:#?}\nonly in {}: {only_rs:#?}",
                series[0].0,
                series[1].0
            );
            assert!(
                series[0].1.iter().any(|s| s.starts_with("cortex_partition_ring_partitions{")),
                "{:#?}",
                series[0].1
            );
            stop_all(&mut procs);
        }
    });
}
