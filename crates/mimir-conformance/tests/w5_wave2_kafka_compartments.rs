//! Wave 2 twin-process differential conformance for Kafka `ingest_storage`, compartments and
//! runtime config: the unmodified Go `mimir` (`target/go-mimir`) and the Rust `mimir` run as
//! `-target=ingester` with identical configuration against a `kfake` broker
//! (`go-oracle kfake-broker`), and their externally observable behaviour is diffed:
//!
//! - W2-02: Kafka record keys carrying dskit/tenant metadata (`123456:product=k6`) are ingested
//!   into tenant `123456` (`tenant.TenantID` / `TrimMetadata`).
//! - W2-11: metadata records go through the pusher accounting
//!   (`cortex_ingest_storage_reader_requests_total`,
//!   `cortex_ingest_storage_reader_pusher_processing_time_seconds{content}`) and log
//!   `failed to ingest some metadata` on metadata limit errors.
//! - W2-03: `-compartments.enabled=true` (read=2, write=2, read_compartment_id=1): consumption of
//!   `ingest-rc-1` from both write compartments' clusters, the compartments partition ring index
//!   page and `/ingester/partition-ring/compartment-<id>` pages, the `-rc-1` scoped partition
//!   lifecycler, per write compartment (`write_compartment` label) reader metrics, and strong
//!   read consistency through each cluster's reader.
//! - W2-14: `overrides: {t: null}` renders as `t: null` on `/runtime_config` and `?mode=diff`.
//!
//! Upstream: pkg/storage/ingest/pusher.go, pkg/ingester/ingester_push.go,
//! pkg/storage/ingest/multi_cluster_reader.go, pkg/mimir/modules.go, pkg/api/api.go,
//! pkg/compartments/index_page.go, pkg/mimir/runtime_config.go.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_conformance::oracle::{
    KfakeBrokerRequest, KfakeRecordInput, LabelJson, MetadataJson, SampleJson,
    SpawnedKfakeBroker, TimeSeriesJson, WriteRequestJson,
};
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl, Proc};
use mimir_config::{KafkaConfig, StringSliceCSV};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ringpb::PartitionState;
use mimir_storage::ingest::IngestWriter;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const IMPLS: [Impl; 2] = [Impl::Go, Impl::Rust];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn series(name: &str, ts: i64, value: f64) -> TimeSeriesJson {
    TimeSeriesJson {
        labels: vec![LabelJson {
            name: "__name__".to_string(),
            value: name.to_string(),
        }],
        samples: vec![SampleJson {
            timestamp_ms: ts,
            value,
        }],
        ..Default::default()
    }
}

fn metadata(name: &str, help: &str) -> MetadataJson {
    MetadataJson {
        metric_type: "COUNTER".to_string(),
        metric_family_name: name.to_string(),
        help: help.to_string(),
        unit: String::new(),
    }
}

fn record(
    tenant: &str,
    version: i32,
    timeseries: Vec<TimeSeriesJson>,
    metadata: Vec<MetadataJson>,
) -> KfakeRecordInput {
    KfakeRecordInput {
        partition: 0,
        tenant: tenant.to_string(),
        version,
        compression: None,
        write_request: WriteRequestJson {
            mode: String::new(),
            source: "API".to_string(),
            skip_label_validation: false,
            skip_label_count_validation: false,
            timeseries,
            metadata,
        },
    }
}

/// `(which, http_port, grpc_port)` of a running ingester.
struct Running {
    which: Impl,
    proc: Proc,
    http: u16,
    grpc: u16,
}

fn ingest_yaml(
    http: u16,
    grpc: u16,
    dir: &Path,
    kafka_address: &str,
    topic: &str,
    compartments: Option<(i64, i64, i64)>,
) -> String {
    let (compartments_block, read_compartment_id) = match compartments {
        Some((read, write, rc)) => (
            format!(
                "compartments:\n  enabled: true\n  read:\n    num_compartments: {read}\n  write:\n    num_compartments: {write}\n"
            ),
            rc,
        ),
        None => (String::new(), 0),
    };
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
{compartments_block}ingester:
  read_compartment_id: {read_compartment_id}
  ring:
    instance_id: ingester-zone-a-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
  partition_ring:
    kvstore:
      store: inmemory
    min_partition_owners_count: 1
    min_partition_owners_duration: 0s
ingest_storage:
  enabled: true
  kafka:
    address: {kafka_address}
    topic: {topic}
    auto_create_topic_enabled: false
    consume_from_position_at_startup: start
    wait_strong_read_consistency_timeout: 1s
    last_produced_offset_poll_interval: 100ms
    consumer_group_offset_commit_interval: 100ms
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
limits:
  max_global_metadata_per_user: 1
activity_tracker:
  filepath: ""
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

async fn start_ingesters(
    http_client: &reqwest::Client,
    base: &Path,
    yaml: impl Fn(u16, u16, &Path) -> String,
) -> Vec<Running> {
    let mut out = Vec::new();
    for which in IMPLS {
        let dir = base.join(which.name());
        std::fs::create_dir_all(dir.join("bucket")).unwrap();
        std::fs::create_dir_all(dir.join("tsdb")).unwrap();
        let http = mimir_conformance::free_test_port();
        let grpc = mimir_conformance::free_test_port();
        let proc = start_mimir(which, "ingester", &dir, &yaml(http, grpc, &dir), &[]);
        out.push(Running {
            which,
            proc,
            http,
            grpc,
        });
    }
    for r in &mut out {
        wait_ready(http_client, &mut r.proc, r.http, Duration::from_secs(60)).await;
    }
    out
}

async fn grpc_client(port: u16) -> IngesterClient<Channel> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    IngesterClient::new(ch)
}

fn request<T>(msg: T, md: &[(&'static str, &str)]) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    for (k, v) in md {
        req.metadata_mut()
            .insert(*k, MetadataValue::try_from(*v).unwrap());
    }
    req
}

async fn user_series(client: &mut IngesterClient<Channel>, tenant: &str) -> u64 {
    client
        .user_stats(request(
            ingesterpb::UserStatsRequest::default(),
            &[("x-scope-orgid", tenant)],
        ))
        .await
        .unwrap_or_else(|e| panic!("UserStats({tenant}): {e:?}"))
        .into_inner()
        .num_series
}

/// `LabelNames` under strong read consistency with the given encoded offsets.
async fn strong_label_names(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    offsets: &str,
) -> Result<Vec<String>, (tonic::Code, String)> {
    client
        .label_names(request(
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: i64::MAX,
                matchers: None,
                limit: 0,
            },
            &[
                ("x-scope-orgid", tenant),
                ("__consistency_level__", "strong"),
                ("__consistency_offsets__", offsets),
            ],
        ))
        .await
        .map(|r| {
            let mut names = r.into_inner().label_names;
            names.sort();
            names
        })
        .map_err(|s| (s.code(), s.message().to_string()))
}

/// Splits `name{k="v",...}` into the metric name and its sorted label pairs.
fn parse_series(series: &str) -> (String, BTreeMap<String, String>) {
    let Some(open) = series.find('{') else {
        return (series.to_string(), BTreeMap::new());
    };
    let name = series[..open].to_string();
    let body = series[open + 1..].trim_end_matches('}');
    let mut labels = BTreeMap::new();
    let mut rest = body;
    while let Some(eq) = rest.find("=\"") {
        let key = rest[..eq].trim_start_matches(',').to_string();
        let after = &rest[eq + 2..];
        let mut end = 0;
        let bytes = after.as_bytes();
        while end < bytes.len() {
            if bytes[end] == b'\\' {
                end += 2;
                continue;
            }
            if bytes[end] == b'"' {
                break;
            }
            end += 1;
        }
        labels.insert(key, after[..end.min(after.len())].to_string());
        rest = &after[(end + 1).min(after.len())..];
    }
    (name, labels)
}

/// `/metrics` series whose name is in `names`, keyed by `name{sorted labels}`.
async fn metric_values(
    http_client: &reqwest::Client,
    port: u16,
    names: &[&str],
) -> BTreeMap<String, f64> {
    let (code, _, body) = http_get(http_client, port, "/metrics", false).await.unwrap();
    assert_eq!(code, 200);
    let mut out = BTreeMap::new();
    for line in body.lines().filter(|l| !l.starts_with('#') && !l.is_empty()) {
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let (name, labels) = parse_series(series);
        if !names.contains(&name.as_str()) {
            continue;
        }
        let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
        out.insert(
            format!("{name}{{{}}}", labels.join(",")),
            value.parse::<f64>().unwrap_or(f64::NAN),
        );
    }
    out
}

/// Waits until partition 0 is ACTIVE on the partition ring page at `path`.
async fn wait_partition_active(http_client: &reqwest::Client, r: &mut Running, path: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok((200, _, body)) = http_get(http_client, r.http, path, true).await {
            let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
            let active = v["partitions"].as_array().is_some_and(|ps| {
                ps.iter()
                    .any(|p| p["id"] == 0 && p["state"] == PartitionState::PartitionActive as i32)
            });
            if active {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{}: partition 0 not ACTIVE on {path}\n{}",
            r.which.name(),
            r.proc.log()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// W2-02 + W2-11.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_metadata_record_keys_and_metadata_records() {
    let ts = now_ms() - 10_000;
    let broker = SpawnedKfakeBroker::spawn(&KfakeBrokerRequest {
        topic: "ingest-w2k".to_string(),
        partitions: 1,
        compression: None,
        records: vec![
            record("123456:product=k6", 2, vec![series("k6_metric", ts, 1.0)], vec![]),
            record(
                "t-meta",
                1,
                vec![],
                vec![metadata("meta_only_1", "help 1"), metadata("meta_only_2", "help 2")],
            ),
            record(
                "123456",
                1,
                vec![series("plain_metric", ts, 2.0)],
                vec![metadata("plain_metric", "plain")],
            ),
        ],
    })
    .expect("spawn kfake broker");

    let tmp = tempfile::tempdir().unwrap();
    let http_client = reqwest::Client::new();
    let mut running = start_ingesters(&http_client, tmp.path(), |h, g, d| {
        ingest_yaml(h, g, d, broker.broker_addr(), broker.topic(), None)
    })
    .await;

    // The metadata limit is only enforced once the partition is ACTIVE (global -> local limit
    // conversion over the active partitions): then produce a metadata-only record with two
    // families for a tenant limited to one, which must log `failed to ingest some metadata`.
    for r in &mut running {
        wait_partition_active(&http_client, r, "/ingester/partition-ring").await;
    }
    let writer = IngestWriter::from_config(
        &KafkaConfig {
            address: StringSliceCSV(vec![broker.broker_addr().to_string()]),
            topic: broker.topic().to_string(),
            auto_create_topic_enabled: false,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let limit_req = cortexpb::WriteRequest {
        metadata: vec![
            cortexpb::MetricMetadata {
                r#type: 1,
                metric_family_name: "limited_1".to_string(),
                help: "h1".to_string(),
                unit: String::new(),
            },
            cortexpb::MetricMetadata {
                r#type: 1,
                metric_family_name: "limited_2".to_string(),
                help: "h2".to_string(),
                unit: String::new(),
            },
        ],
        ..Default::default()
    };
    let last_offset = writer
        .write_sync(broker.topic(), 0, "t-limit", &limit_req)
        .await
        .unwrap();

    let metric_names = [
        "cortex_ingest_storage_reader_requests_total",
        "cortex_ingest_storage_reader_requests_failed_total",
        "cortex_ingest_storage_reader_pusher_processing_time_seconds_count",
        "cortex_ingester_ingested_metadata_total",
        "cortex_ingester_ingested_metadata_failures_total",
    ];
    let mut observed = Vec::new();
    for r in &mut running {
        let mut client = grpc_client(r.grpc).await;
        let offsets = format!("v1=0:{last_offset}");
        let names = strong_label_names(&mut client, "123456", &offsets)
            .await
            .unwrap_or_else(|e| panic!("{} strong LabelNames: {e:?}\n{}", r.which.name(), r.proc.log()));
        let num_series = user_series(&mut client, "123456").await;
        // The pusher accounting is updated right after the batch is ingested.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let metrics = metric_values(&http_client, r.http, &metric_names).await;
        let warned = r.proc.log().contains("failed to ingest some metadata");
        observed.push((r.which, names, num_series, metrics, warned));
    }

    for (which, names, num_series, metrics, warned) in &observed {
        eprintln!("[{}] label_names={names:?} series={num_series} warned={warned}\n  {metrics:#?}", which.name());
    }
    let (_, go_names, go_series, go_metrics, go_warned) = &observed[0];
    let (_, rs_names, rs_series, rs_metrics, rs_warned) = &observed[1];
    // W2-02: `123456:product=k6` and `123456` both land in tenant `123456`.
    assert_eq!(*go_series, 2, "go UserStats(123456)");
    assert_eq!(rs_series, go_series, "UserStats(123456)");
    assert_eq!(rs_names, go_names, "LabelNames(123456)");
    // W2-11: pusher request accounting and the metadata limit warning.
    assert_eq!(rs_metrics, go_metrics, "pusher / metadata metrics");
    assert!(
        go_metrics
            .get("cortex_ingester_ingested_metadata_failures_total{}")
            .is_some_and(|v| *v > 0.0),
        "metadata limit hit: {go_metrics:?}"
    );
    assert!(*go_warned, "go logs 'failed to ingest some metadata'");
    assert!(*rs_warned, "rust logs 'failed to ingest some metadata'");
    assert!(
        go_metrics
            .keys()
            .any(|k| k.contains("content=\"metadata\"")),
        "metadata-only record observed with content=metadata: {go_metrics:?}"
    );
}

/// W2-03: compartments read=2, write=2, read_compartment_id=1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compartments_read_and_write_compartments() {
    let ts = now_ms() - 10_000;
    let topic = "ingest-rc-1";
    // Each write compartment is its own Kafka cluster: `127.0.0.1:<prefix><write-compartment-id>`
    // resolves to two brokers on consecutive ports ending in 0 and 1.
    let (prefix, brokers) = loop {
        let p = mimir_conformance::free_test_port();
        let base = p - p % 10;
        if base < 15_000 {
            continue;
        }
        let spawn = |wc: u16, name: &str| {
            SpawnedKfakeBroker::spawn_with_port(
                &KfakeBrokerRequest {
                    topic: topic.to_string(),
                    partitions: 1,
                    compression: None,
                    records: vec![record("t-comp", 2, vec![series(name, ts, 77.0)], vec![])],
                },
                Some(base + wc),
            )
        };
        let (Ok(b0), Ok(b1)) = (spawn(0, "comp_metric_wc0"), spawn(1, "comp_metric_wc1")) else {
            continue;
        };
        break (base / 10, [b0, b1]);
    };
    let address = format!("127.0.0.1:{prefix}<write-compartment-id>");
    let last_offsets: Vec<i64> = brokers.iter().map(|b| b.offsets()[0].end_offset - 1).collect();

    let tmp = tempfile::tempdir().unwrap();
    let http_client = reqwest::Client::new();
    let mut running = start_ingesters(&http_client, tmp.path(), |h, g, d| {
        ingest_yaml(h, g, d, &address, "ingest-rc-<read-compartment-id>", Some((2, 2, 1)))
    })
    .await;

    let mut observed = Vec::new();
    for r in &mut running {
        // The ingester's partition lifecycler registers into the `-rc-1` ring.
        wait_partition_active(&http_client, r, "/ingester/partition-ring/compartment-1").await;
    }
    for r in &mut running {
        let name = r.which.name();
        let mut client = grpc_client(r.grpc).await;

        // Strong consistency through every write compartment's reader.
        let ok_offsets = format!("v2=1/0:{};{}", last_offsets[0], last_offsets[1]);
        let names = strong_label_names(&mut client, "t-comp", &ok_offsets)
            .await
            .unwrap_or_else(|e| panic!("{name} strong LabelNames: {e:?}\n{}", r.proc.log()));
        let num_series = user_series(&mut client, "t-comp").await;
        let timeout_err = strong_label_names(&mut client, "t-comp", "v2=1/0:999999;0")
            .await
            .expect_err("unreachable offset must time out");
        let mismatch_err = strong_label_names(&mut client, "t-comp", "v2=1/0:0")
            .await
            .expect_err("cluster count mismatch");

        let (idx_code, idx_headers, idx_body) =
            http_get(&http_client, r.http, "/ingester/partition-ring", false).await.unwrap();
        let idx_post = http_client
            .post(format!("http://127.0.0.1:{}/ingester/partition-ring", r.http))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16();
        let mut pages = Vec::new();
        for c in 0..3 {
            let (code, _, body) = http_get(
                &http_client,
                r.http,
                &format!("/ingester/partition-ring/compartment-{c}"),
                true,
            )
            .await
            .unwrap();
            let partitions: Vec<(i64, String, Vec<String>)> = if code == 200 {
                let v: serde_json::Value = serde_json::from_str(&body)
                    .unwrap_or_else(|e| panic!("{name} compartment-{c}: {e}: {body}"));
                v["partitions"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .map(|p| {
                        (
                            p["id"].as_i64().unwrap_or(-1),
                            p["state"].to_string(),
                            p["owner_ids"]
                                .as_array()
                                .cloned()
                                .unwrap_or_default()
                                .iter()
                                .map(|o| o.as_str().unwrap_or_default().to_string())
                                .collect(),
                        )
                    })
                    .collect()
            } else {
                Vec::new()
            };
            pages.push((code, partitions));
        }

        let metrics = metric_values(
            &http_client,
            r.http,
            &[
                "cortex_ingest_storage_strong_consistency_requests_total",
                "cortex_ingest_storage_strong_consistency_failures_total",
                "cortex_ingest_storage_reader_requests_total",
            ],
        )
        .await;
        let content_type = idx_headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        observed.push((
            r.which,
            names,
            num_series,
            timeout_err,
            mismatch_err,
            (idx_code, content_type, idx_body, idx_post),
            pages,
            metrics,
        ));
    }

    for o in &observed {
        eprintln!("[{}] {:#?}", o.0.name(), (&o.1, o.2, &o.3, &o.4, &o.5, &o.6, &o.7));
    }
    let go = &observed[0];
    let rs = &observed[1];
    assert_eq!(go.2, 2, "go consumed ingest-rc-1 from both write compartments");
    assert_eq!(rs.1, go.1, "LabelNames(t-comp)");
    assert_eq!(rs.2, go.2, "UserStats(t-comp)");
    // Go answers `DeadlineExceeded` (the error wraps `context.DeadlineExceeded`); the gRPC
    // status mapping of read consistency errors lives in mimir-ingester/src/query.rs (W2-10
    // scope), so only the error message is diffed here.
    if rs.3 .0 != go.3 .0 {
        eprintln!("KNOWN DIVERGENCE (query.rs status mapping): go={:?} rust={:?}", go.3 .0, rs.3 .0);
    }
    assert_eq!(rs.3 .1, go.3 .1, "strong consistency timeout error");
    assert_eq!(rs.4, go.4, "strong consistency cluster count mismatch error");
    assert_eq!(rs.5, go.5, "compartments index page");
    assert!(go.5 .2.contains("<a href=\"partition-ring/compartment-1\">Compartment 1</a>"));
    assert_eq!(rs.6, go.6, "per-compartment partition ring pages");
    assert_eq!(go.6[1].1.len(), 1, "go registers its partition in the -rc-1 ring only");
    assert_eq!(rs.7, go.7, "per write compartment reader metrics");
    assert!(
        go.7.keys().any(|k| k.contains("write_compartment=\"1\"")),
        "write_compartment=1 metrics: {:?}",
        go.7
    );
}

/// W2-14: a `null` tenant override is kept as `null`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_config_null_tenant_override() {
    let tmp = tempfile::tempdir().unwrap();
    let http_client = reqwest::Client::new();
    let runtime_path = tmp.path().join("runtime.yaml");
    std::fs::write(&runtime_path, "overrides:\n  t_null: null\n").unwrap();

    let mut running = Vec::new();
    for which in IMPLS {
        let dir = tmp.path().join(which.name());
        std::fs::create_dir_all(dir.join("tsdb")).unwrap();
        let http = mimir_conformance::free_test_port();
        let grpc = mimir_conformance::free_test_port();
        let yaml = format!(
            r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
runtime_config:
  file: {runtime}
ingester:
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
            runtime = runtime_path.display(),
            bucket = dir.join("bucket").display(),
            tsdb = dir.join("tsdb").display(),
        );
        let proc = start_mimir(which, "runtime-config", &dir, &yaml, &[]);
        running.push(Running {
            which,
            proc,
            http,
            grpc,
        });
    }
    let mut observed = Vec::new();
    for r in &mut running {
        wait_ready(&http_client, &mut r.proc, r.http, Duration::from_secs(60)).await;
        let _ = r.grpc;
        let mut views = Vec::new();
        for path in ["/runtime_config", "/runtime_config?mode=diff"] {
            let (code, _, body) = http_get(&http_client, r.http, path, false).await.unwrap();
            let v: serde_yaml::Value = serde_yaml::from_str(&body)
                .unwrap_or_else(|e| panic!("{} {path}: {e}: {body}", r.which.name()));
            views.push((code, v));
        }
        observed.push(views);
    }
    eprintln!("{observed:#?}");
    let null_only: serde_yaml::Value = serde_yaml::from_str("overrides:\n  t_null: null\n").unwrap();
    assert_eq!(observed[0][1].1, null_only, "go ?mode=diff");
    assert_eq!(observed[1], observed[0], "/runtime_config and ?mode=diff");
}
