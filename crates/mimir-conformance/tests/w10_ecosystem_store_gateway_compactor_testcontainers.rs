//! Gate 2 Ecosystem Test Suite 1: Downstream Go Mimir Consumers & Bidirectional PVC Rollback.
//!
//! - Test A (`w10_01_rust_s3_blocks_go_compactor_store_gateway_querier_head_boundary`):
//!   Boots `pgsty/silo` (MinIO S3) + Rust `mimir -target=ingester`, ingests multi-window data
//!   spanning 3 historical 2h windows (floats, `xor2` ST start-timestamps, integer native
//!   histograms, float native histograms, NHCB custom-bucket histograms `schema=-53`, counter
//!   resets, `StaleNaN`, exemplars, metadata, OOO samples) + flushes & ships blocks to S3 via
//!   `POST /ingester/flush?wait=true`, then ingests live uncompacted Head samples. Boots real
//!   unmodified Go `compactor`, Go `store-gateway`, and Go `querier` wired to BOTH Go
//!   `store-gateway` and Rust `ingester`, and verifies:
//!   - Go `compactor` compacts Rust's S3 blocks with 0 failures and writes `bucket-index.json.gz`
//!   - Go `store-gateway` loads `bucket-index.json.gz` and serves Rust's blocks
//!   - Go `querier` serves exact PromQL `query`, `query_range`, `series`, `labels`,
//!     `label/__name__/values`, `query_exemplars`, `metadata` across historical S3 blocks ->
//!     live Rust Head with zero duplicates and `cortex_querier_storegateway_refetches_total == 0`.
//!
//! - Test B (`w10_02_bidirectional_pvc_upgrade_and_emergency_rollback`):
//!   Go `ingester` -> `SIGKILL` -> Rust `ingester` (same `tsdb_dir`) -> verify 100% data ->
//!   compact block + push more floats/histograms/OOO triggering `chunks_head` + WAL + WBL ->
//!   `SIGKILL` -> Go `ingester` rollback on same `tsdb_dir` -> assert 0 WAL corruptions /
//!   truncation failures and 100% of pre-upgrade + Rust-written data intact.
//!
//! Requires Docker. `MIMIR_CONFORMANCE_SKIP_DOCKER=1` skips explicitly (prints `SKIPPED`).

#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::free_test_port;
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use serde_json::Value;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const SILO_IMAGE: &str = "pgsty/silo";
const SILO_TAG: &str = "RELEASE.2026-09-03T13-18-01Z";
const CONTAINER_LABEL: (&str, &str) = ("org.mimir-rs.conformance", "w10-ecosystem");
const ACCESS_KEY: &str = "mimir";
const SECRET_KEY: &str = "supersecret";
const BUCKET: &str = "mimir-eco";
const TENANT: &str = "tenant-eco";
const TWO_HOURS_MS: i64 = 2 * 60 * 60 * 1000;

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
        .expect("clock after epoch")
        .as_millis() as i64
}

fn stale_nan_f64() -> f64 {
    f64::from_bits(0x7ff0_0000_0000_0002)
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------------------------
// Silo (MinIO S3) container
// ---------------------------------------------------------------------------------------------

struct Silo {
    container: ContainerAsync<GenericImage>,
    port: u16,
}

impl Silo {
    async fn start() -> Result<Self> {
        let port = free_test_port();
        let container = GenericImage::new(SILO_IMAGE, SILO_TAG)
            .with_wait_for(WaitFor::message_on_either_std("API:"))
            .with_entrypoint("sh")
            .with_cmd([
                "-c".to_string(),
                format!("mkdir -p /data/{BUCKET} && exec silo server /data"),
            ])
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_label(CONTAINER_LABEL.0, CONTAINER_LABEL.1)
            .with_mapped_port(port, ContainerPort::Tcp(9000))
            .with_startup_timeout(Duration::from_secs(120))
            .start()
            .await
            .context("starting pgsty/silo via testcontainers")?;
        let silo = Self { container, port };
        silo.sh(&format!(
            "mc alias set l http://127.0.0.1:9000 {ACCESS_KEY} {SECRET_KEY}"
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

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let out = self
            .sh(&format!("mc ls --recursive --json l/{BUCKET}/{prefix} || true"))
            .await?;
        let mut keys = Vec::new();
        for line in out.lines().filter(|l| !l.trim().is_empty()) {
            let v: Value = serde_json::from_str(line)?;
            if let Some(k) = v["key"].as_str() {
                keys.push(format!("{prefix}{k}"));
            }
        }
        keys.sort();
        Ok(keys)
    }
}

// ---------------------------------------------------------------------------------------------
// Proto / Metric helpers
// ---------------------------------------------------------------------------------------------

fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
    cortexpb::LabelPair::from_strs(name, value)
}

fn span(offset: i32, length: u32) -> cortexpb::BucketSpan {
    cortexpb::BucketSpan { offset, length }
}

fn sum_int_deltas(zero: u64, deltas: &[i64]) -> u64 {
    let mut total = zero;
    let mut cur = 0i64;
    for &d in deltas {
        cur += d;
        total += cur.max(0) as u64;
    }
    total
}

fn int_hist(
    ts: i64,
    schema: i32,
    _count: u64,
    zero: u64,
    sum: f64,
    deltas: Vec<i64>,
    reset_hint: i32,
) -> cortexpb::Histogram {
    let count = sum_int_deltas(zero, &deltas);
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(zero)),
        zero_threshold: 0.001,
        sum,
        schema,
        positive_spans: vec![span(0, deltas.len() as u32)],
        positive_deltas: deltas,
        reset_hint,
        timestamp: ts,
        ..Default::default()
    }
}

fn float_hist(
    ts: i64,
    schema: i32,
    _count: f64,
    zero: f64,
    sum: f64,
    counts: Vec<f64>,
    reset_hint: i32,
) -> cortexpb::Histogram {
    let count = zero + counts.iter().sum::<f64>();
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(zero)),
        zero_threshold: 0.001,
        sum,
        schema,
        positive_spans: vec![span(0, counts.len() as u32)],
        positive_counts: counts,
        reset_hint,
        timestamp: ts,
        ..Default::default()
    }
}

fn nhcb_hist(
    ts: i64,
    _count: u64,
    sum: f64,
    bounds: Vec<f64>,
    deltas: Vec<i64>,
) -> cortexpb::Histogram {
    let count = sum_int_deltas(0, &deltas);
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(count)),
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
        sum,
        schema: -53,
        positive_spans: vec![span(0, deltas.len() as u32)],
        positive_deltas: deltas,
        custom_values: bounds,
        timestamp: ts,
        ..Default::default()
    }
}

async fn grpc_push(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    req: cortexpb::WriteRequest,
) -> Result<()> {
    let mut r = tonic::Request::new(req);
    r.metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(tenant)?);
    client
        .push(r)
        .await
        .map_err(|s| anyhow::anyhow!("gRPC push failed: {s}"))?;
    Ok(())
}

async fn connect_ingester(port: u16) -> Result<IngesterClient<Channel>> {
    let ch = Channel::from_shared(format!("http://127.0.0.1:{port}"))?
        .connect()
        .await
        .context("connecting to ingester gRPC")?;
    Ok(IngesterClient::new(ch))
}

fn sum_metric_by_prefix(exposition: &str, metric_name: &str) -> f64 {
    let mut total = 0.0;
    for line in exposition.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let Some((series, val_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = series.split('{').next().unwrap_or(series);
        if name == metric_name {
            if let Ok(v) = val_str.parse::<f64>() {
                total += v;
            }
        }
    }
    total
}

fn metric_value_exact(exposition: &str, metric_name: &str) -> Option<f64> {
    for line in exposition.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let Some((series, val_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = series.split('{').next().unwrap_or(series);
        if name == metric_name {
            return val_str.parse::<f64>().ok();
        }
    }
    None
}

async fn prom_get_json(
    client: &reqwest::Client,
    port: u16,
    tenant: &str,
    path_and_query: &str,
) -> Result<Value> {
    let resp = client
        .get(format!("http://127.0.0.1:{port}{path_and_query}"))
        .header(LOWER_ORG_ID_HEADER_NAME, tenant)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;
    ensure!(
        status.is_success(),
        "GET {path_and_query} returned {status}: {body}"
    );
    let v: Value = serde_json::from_str(&body)
        .with_context(|| format!("invalid JSON from {path_and_query}: {body}"))?;
    ensure!(
        v["status"].as_str() == Some("success"),
        "PromQL status not success for {path_and_query}: {v}"
    );
    Ok(v)
}

// ---------------------------------------------------------------------------------------------
// Config YAML builders
// ---------------------------------------------------------------------------------------------

fn s3_yaml(silo_port: u16) -> String {
    format!(
        r#"blocks_storage:
  backend: s3
  s3:
    endpoint: 127.0.0.1:{silo_port}
    bucket_name: {BUCKET}
    access_key_id: {ACCESS_KEY}
    secret_access_key: {SECRET_KEY}
    insecure: true
    bucket_lookup_type: path"#
    )
}

fn ingester_yaml(
    http: u16,
    grpc: u16,
    ml_port: u16,
    tsdb_dir: &Path,
    silo_port: Option<u16>,
) -> String {
    let storage = match silo_port {
        Some(sp) => format!(
            "{}
  tsdb:
    dir: {}
    ship_interval: 1m
    head_compaction_interval: 1m
    retention_period: 13h",
            s3_yaml(sp),
            tsdb_dir.display()
        ),
        None => format!(
            "blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 0s
    head_compaction_interval: 1m
    retention_period: 13h",
            tsdb_dir.parent().unwrap().join("bucket-fs").display(),
            tsdb_dir.display()
        ),
    };

    format!(
        r#"target: ingester
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: warn

memberlist:
  node_name: ingester-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml_port}
  advertise_addr: 127.0.0.1
  advertise_port: {ml_port}

ingester:
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    num_tokens: 128
    replication_factor: 1
    heartbeat_period: 500ms
    heartbeat_timeout: 5s
    min_ready_duration: 0s
    final_sleep: 0s
    unregister_on_shutdown: true
    kvstore:
      store: memberlist

limits:
  out_of_order_time_window: 12h
  out_of_order_blocks_external_label_enabled: false
  native_histograms_ingestion_enabled: true
  float_chunk_encoding: xor2
  histogram_chunk_encoding: histogram_st
  max_global_exemplars_per_user: 1000
  max_global_series_per_user: 100000
  max_global_metadata_per_user: 10000
  query_ingesters_within: 13h

{storage}
"#
    )
}

fn compactor_yaml(http: u16, grpc: u16, data_dir: &Path, sync_dir: &Path, silo_port: u16) -> String {
    format!(
        r#"target: compactor
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: info

compactor:
  data_dir: {}
  compaction_interval: 2s
  cleanup_interval: 2s
  deletion_delay: 1h
  first_level_compaction_wait_period: 0s
  first_level_compaction_ooo_wait_period: 0s
  first_level_compaction_skip_future_max_time: false
  block_ranges: [2h, 4h, 24h]
  sharding_ring:
    instance_id: compactor-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    wait_stability_min_duration: 0s
    kvstore:
      store: inmemory

{}
  bucket_store:
    sync_dir: {}
    sync_interval: 2s
    ignore_blocks_within: 0s
"#,
        data_dir.display(),
        s3_yaml(silo_port),
        sync_dir.display(),
    )
}

fn store_gateway_yaml(
    http: u16,
    grpc: u16,
    ml_port: u16,
    ml_seed: u16,
    sync_dir: &Path,
    silo_port: u16,
) -> String {
    format!(
        r#"target: store-gateway
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: info

memberlist:
  node_name: store-gateway-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml_port}
  advertise_addr: 127.0.0.1
  advertise_port: {ml_port}
  join_members: ["127.0.0.1:{ml_seed}"]

store_gateway:
  sharding_ring:
    instance_id: store-gateway-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    heartbeat_period: 500ms
    heartbeat_timeout: 5s
    wait_stability_min_duration: 0s
    kvstore:
      store: memberlist

{}
  bucket_store:
    sync_dir: {}
    sync_interval: 2s
    ignore_blocks_within: 0s
"#,
        s3_yaml(silo_port),
        sync_dir.display(),
    )
}

fn querier_yaml(
    http: u16,
    grpc: u16,
    ml_port: u16,
    ml_seed: u16,
    sync_dir: &Path,
    silo_port: u16,
) -> String {
    format!(
        r#"target: querier
multitenancy_enabled: true
usage_stats:
  enabled: false

server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
  log_level: info

memberlist:
  node_name: querier-0
  bind_addr: ["127.0.0.1"]
  bind_port: {ml_port}
  advertise_addr: 127.0.0.1
  advertise_port: {ml_port}
  join_members: ["127.0.0.1:{ml_seed}"]

ingester:
  ring:
    replication_factor: 1
    kvstore:
      store: memberlist

store_gateway:
  sharding_ring:
    kvstore:
      store: memberlist

querier:
  query_store_after: 0s
  ring:
    instance_id: querier-0
    instance_addr: 127.0.0.1
    instance_port: {grpc}
    kvstore:
      store: memberlist

limits:
  query_ingesters_within: 13h
  max_query_lookback: 0s

{}
  bucket_store:
    sync_dir: {}
    sync_interval: 2s
    ignore_blocks_within: 0s
"#,
        s3_yaml(silo_port),
        sync_dir.display(),
    )
}

// ---------------------------------------------------------------------------------------------
// Test A: Rust Shipped S3 Blocks Consumed by Real Go Compactor + Store-Gateway + Querier
// ---------------------------------------------------------------------------------------------

#[test]
fn w10_01_rust_s3_blocks_go_compactor_store_gateway_querier_head_boundary() {
    if skip_docker("w10_01_rust_s3_blocks_go_compactor_store_gateway_querier_head_boundary") {
        return;
    }

    block_on_for_tests(async {
        let silo = Silo::start().await.expect("start Silo");
        let tmp = tempfile::tempdir().unwrap();
        let client = http_client();

        let ing_http = free_test_port();
        let ing_grpc = free_test_port();
        let ing_ml = free_test_port();
        let ing_tsdb = tmp.path().join("ingester-tsdb");
        fs::create_dir_all(&ing_tsdb).unwrap();

        let ing_cfg = ingester_yaml(ing_http, ing_grpc, ing_ml, &ing_tsdb, Some(silo.port));
        let mut ing_proc = start_mimir(
            Impl::Rust,
            "rust-ingester",
            &tmp.path().join("ingester"),
            &ing_cfg,
            &[],
        );
        wait_ready(&client, &mut ing_proc, ing_http, Duration::from_secs(45)).await;

        let mut ing_client = connect_ingester(ing_grpc).await.expect("connect ingester");

        // Align 3 historical 2h windows BEFORE current 2h boundary so at least 2 lie in the same
        // 4h UTC compaction window (exercising real Go compactor multi-block merge) and live head
        // samples stay strictly after the flushed windows.
        let now = now_ms();
        let curr_block = (now / TWO_HOURS_MS) * TWO_HOURS_MS;
        let w1 = curr_block - 4 * TWO_HOURS_MS + 120_000;
        let w2 = curr_block - 3 * TWO_HOURS_MS + 120_000;
        let w3 = curr_block - 2 * TWO_HOURS_MS + 120_000;
        // Live head timestamps strictly after w3 and before now:
        let head_t0 = (now - 120_000).max(curr_block - TWO_HOURS_MS + 600_000);
        let head_t1 = head_t0 + 30_000;
        let head_t2 = head_t0 + 60_000;
        let head_t3 = head_t0 + 90_000;

        // Push historical windows w1 -> w2 -> w3 in chronological order (with ST created_timestamp
        // for xor2 + histogram_st, counter reset, StaleNaN, int/float/NHCB histograms, exemplars,
        // metadata).
        let hist_batches = vec![
            // Window 1
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_gauge"), lp("job", "app")],
                        created_timestamp: w1 - 10_000,
                        samples: vec![
                            cortexpb::Sample { value: 11.0, timestamp_ms: w1 },
                            cortexpb::Sample { value: 12.0, timestamp_ms: w1 + 30_000 },
                            cortexpb::Sample { value: 13.0, timestamp_ms: w1 + 60_000 },
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_counter"), lp("job", "app")],
                        created_timestamp: w1 - 5_000,
                        samples: vec![
                            cortexpb::Sample { value: 10.0, timestamp_ms: w1 },
                            cortexpb::Sample { value: 25.0, timestamp_ms: w1 + 30_000 },
                            cortexpb::Sample { value: 40.0, timestamp_ms: w1 + 60_000 },
                        ],
                        exemplars: vec![cortexpb::Exemplar {
                            labels: vec![lp("trace_id", "hist-w1")],
                            value: 15.0,
                            timestamp_ms: w1 + 30_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_int_hist"), lp("job", "app")],
                        created_timestamp: w1 - 5_000,
                        histograms: vec![
                            int_hist(w1, 2, 10, 2, 20.0, vec![4, 4], 0),
                            int_hist(w1 + 60_000, 2, 20, 4, 45.0, vec![8, 8], 2),
                        ],
                        exemplars: vec![cortexpb::Exemplar {
                            labels: vec![lp("trace_id", "ih-w1")],
                            value: 2.5,
                            timestamp_ms: w1 + 60_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_hist"), lp("job", "app")],
                        created_timestamp: w1 - 5_000,
                        histograms: vec![
                            float_hist(w1, 2, 5.5, 1.0, 12.5, vec![2.0, 2.5], 0),
                            float_hist(w1 + 60_000, 2, 11.5, 2.0, 26.0, vec![4.5, 5.0], 2),
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_nhcb_hist"), lp("job", "app")],
                        created_timestamp: w1 - 5_000,
                        histograms: vec![
                            nhcb_hist(w1, 8, 9.0, vec![0.1, 0.5, 1.0, 5.0], vec![3, 2, 2, 1]),
                            nhcb_hist(w1 + 60_000, 16, 19.0, vec![0.1, 0.5, 1.0, 5.0], vec![6, 4, 4, 2]),
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_stale_series"), lp("job", "app")],
                        samples: vec![
                            cortexpb::Sample { value: 99.5, timestamp_ms: w1 },
                            cortexpb::Sample { value: stale_nan_f64(), timestamp_ms: w1 + 60_000 },
                        ],
                        ..Default::default()
                    },
                ],
                metadata: vec![
                    cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                        metric_family_name: "eco_float_counter".into(),
                        help: "ecosystem float counter".into(),
                        unit: "requests".into(),
                    },
                    cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                        metric_family_name: "eco_int_hist".into(),
                        help: "ecosystem native int histogram".into(),
                        unit: "seconds".into(),
                    },
                    cortexpb::MetricMetadata {
                        r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                        metric_family_name: "eco_nhcb_hist".into(),
                        help: "ecosystem nhcb histogram".into(),
                        unit: "seconds".into(),
                    },
                ],
                ..Default::default()
            },
            // Window 2 (includes counter reset!)
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_gauge"), lp("job", "app")],
                        samples: vec![
                            cortexpb::Sample { value: 21.0, timestamp_ms: w2 },
                            cortexpb::Sample { value: 22.0, timestamp_ms: w2 + 30_000 },
                            cortexpb::Sample { value: 23.0, timestamp_ms: w2 + 60_000 },
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_counter"), lp("job", "app")],
                        samples: vec![
                            cortexpb::Sample { value: 5.0, timestamp_ms: w2 },
                            cortexpb::Sample { value: 18.0, timestamp_ms: w2 + 30_000 },
                            cortexpb::Sample { value: 30.0, timestamp_ms: w2 + 60_000 },
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_int_hist"), lp("job", "app")],
                        histograms: vec![
                            // Counter reset (count drops from 20 to 6)
                            int_hist(w2, 2, 6, 1, 10.0, vec![2, 3], 1),
                            int_hist(w2 + 60_000, 2, 16, 3, 28.0, vec![6, 7], 2),
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_hist"), lp("job", "app")],
                        histograms: vec![
                            float_hist(w2, 2, 15.5, 3.0, 35.0, vec![6.0, 6.5], 2),
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_nhcb_hist"), lp("job", "app")],
                        histograms: vec![
                            nhcb_hist(w2, 24, 30.0, vec![0.1, 0.5, 1.0, 5.0], vec![9, 6, 6, 3]),
                        ],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            // Window 3 + base for OOO
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_gauge"), lp("job", "app")],
                        samples: vec![
                            cortexpb::Sample { value: 31.0, timestamp_ms: w3 },
                            cortexpb::Sample { value: 32.0, timestamp_ms: w3 + 30_000 },
                            cortexpb::Sample { value: 33.0, timestamp_ms: w3 + 60_000 },
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_counter"), lp("job", "app")],
                        samples: vec![
                            cortexpb::Sample { value: 45.0, timestamp_ms: w3 },
                            cortexpb::Sample { value: 60.0, timestamp_ms: w3 + 30_000 },
                            cortexpb::Sample { value: 75.0, timestamp_ms: w3 + 60_000 },
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_ooo_metric"), lp("job", "ooo")],
                        samples: vec![cortexpb::Sample { value: 300.0, timestamp_ms: w3 + 60_000 }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            // OOO sample falling back into Window 2!
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "eco_ooo_metric"), lp("job", "ooo")],
                    samples: vec![cortexpb::Sample { value: 200.0, timestamp_ms: w2 + 30_000 }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ];

        for batch in hist_batches {
            grpc_push(&mut ing_client, TENANT, batch)
                .await
                .expect("push historical batch");
        }

        // Flush & ship blocks to Silo S3.
        let flush_resp = client
            .post(format!("http://127.0.0.1:{ing_http}/ingester/flush?wait=true"))
            .send()
            .await
            .expect("POST /ingester/flush?wait=true");
        assert!(
            flush_resp.status().is_success(),
            "flush failed: {}",
            flush_resp.status()
        );

        let shipper_meta = ing_tsdb.join(TENANT).join("mimir.shipper.json");
        assert!(
            shipper_meta.exists(),
            "expected local shipper meta file at {}",
            shipper_meta.display()
        );
        let keys_after_ship = silo.list(&format!("{TENANT}/")).await.expect("list S3");
        let shipped_ulids: BTreeSet<String> = keys_after_ship
            .iter()
            .filter_map(|k| {
                let seg = k.strip_prefix(&format!("{TENANT}/"))?.split('/').next()?;
                ulid::Ulid::from_string(seg).ok().map(|_| seg.to_string())
            })
            .collect();
        assert!(
            shipped_ulids.len() >= 2,
            "expected >= 2 shipped ULID blocks in S3, got {shipped_ulids:?} ({keys_after_ship:?})"
        );

        // Now push LIVE uncompacted Head samples (spanning into Head across all types).
        grpc_push(
            &mut ing_client,
            TENANT,
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_gauge"), lp("job", "app")],
                        samples: vec![
                            cortexpb::Sample { value: 41.0, timestamp_ms: head_t0 },
                            cortexpb::Sample { value: 42.0, timestamp_ms: head_t1 },
                            cortexpb::Sample { value: 43.0, timestamp_ms: head_t2 },
                            cortexpb::Sample { value: 44.0, timestamp_ms: head_t3 },
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_counter"), lp("job", "app")],
                        samples: vec![
                            cortexpb::Sample { value: 90.0, timestamp_ms: head_t0 },
                            cortexpb::Sample { value: 105.0, timestamp_ms: head_t1 },
                            cortexpb::Sample { value: 120.0, timestamp_ms: head_t2 },
                            cortexpb::Sample { value: 135.0, timestamp_ms: head_t3 },
                        ],
                        exemplars: vec![cortexpb::Exemplar {
                            labels: vec![lp("trace_id", "head-live")],
                            value: 15.0,
                            timestamp_ms: head_t2,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_int_hist"), lp("job", "app")],
                        histograms: vec![
                            int_hist(head_t0, 2, 26, 5, 50.0, vec![10, 11], 2),
                            int_hist(head_t3, 2, 36, 7, 72.0, vec![14, 15], 2),
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_float_hist"), lp("job", "app")],
                        histograms: vec![
                            float_hist(head_t0, 2, 20.5, 4.0, 48.0, vec![8.0, 8.5], 2),
                            float_hist(head_t3, 2, 26.5, 5.0, 62.0, vec![10.5, 11.0], 2),
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_nhcb_hist"), lp("job", "app")],
                        histograms: vec![
                            nhcb_hist(head_t0, 32, 42.0, vec![0.1, 0.5, 1.0, 5.0], vec![12, 8, 8, 4]),
                            nhcb_hist(head_t3, 40, 55.0, vec![0.1, 0.5, 1.0, 5.0], vec![15, 10, 10, 5]),
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "eco_head_only"), lp("job", "live")],
                        samples: vec![
                            cortexpb::Sample { value: 7.5, timestamp_ms: head_t0 },
                            cortexpb::Sample { value: 8.5, timestamp_ms: head_t3 },
                        ],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .await
        .expect("push live head samples");

        // Boot unmodified Go compactor against Silo S3.
        let comp_http = free_test_port();
        let comp_grpc = free_test_port();
        let comp_cfg = compactor_yaml(
            comp_http,
            comp_grpc,
            &tmp.path().join("compactor-data"),
            &tmp.path().join("compactor-sync"),
            silo.port,
        );
        let mut comp_proc = start_mimir(
            Impl::Go,
            "go-compactor",
            &tmp.path().join("compactor"),
            &comp_cfg,
            &[],
        );
        wait_ready(&client, &mut comp_proc, comp_http, Duration::from_secs(45)).await;

        let comp_deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (code, _, metrics_body) = http_get(&client, comp_http, "/metrics", false)
                .await
                .unwrap_or((0, Default::default(), String::new()));
            if code == 200 {
                let comp_done = metric_value_exact(&metrics_body, "cortex_compactor_runs_completed_total")
                    .unwrap_or(0.0);
                let clean_done = metric_value_exact(
                    &metrics_body,
                    "cortex_compactor_block_cleanup_completed_total",
                )
                .unwrap_or(0.0);
                let s3_keys = silo.list(&format!("{TENANT}/")).await.unwrap_or_default();
                let has_bidx = s3_keys.iter().any(|k| k.ends_with("bucket-index.json.gz"));
                if comp_done >= 1.0 && clean_done >= 1.0 && has_bidx {
                    let tsdb_fail = sum_metric_by_prefix(
                        &metrics_body,
                        "prometheus_tsdb_compactions_failed_total",
                    );
                    let run_fail =
                        sum_metric_by_prefix(&metrics_body, "cortex_compactor_runs_failed_total");
                    assert_eq!(
                        tsdb_fail, 0.0,
                        "Go compactor had TSDB compaction failures:\n{}",
                        comp_proc.log()
                    );
                    assert_eq!(
                        run_fail, 0.0,
                        "Go compactor had compactor run failures:\n{}",
                        comp_proc.log()
                    );
                    let compactions =
                        sum_metric_by_prefix(&metrics_body, "prometheus_tsdb_compactions_total");
                    assert!(
                        compactions >= 1.0,
                        "expected Go compactor to execute >= 1 TSDB compaction on Rust blocks (got {compactions}):\n{}",
                        comp_proc.log()
                    );
                    break;
                }
            }
            if Instant::now() >= comp_deadline {
                panic!(
                    "Go compactor did not complete compaction + bucket-index.json.gz within 60s.\nS3: {:?}\nLog:\n{}",
                    silo.list(&format!("{TENANT}/")).await,
                    comp_proc.log()
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Boot Go store-gateway + Go querier joined to Rust ingester's memberlist.
        let sg_http = free_test_port();
        let sg_grpc = free_test_port();
        let sg_ml = free_test_port();
        let sg_cfg = store_gateway_yaml(
            sg_http,
            sg_grpc,
            sg_ml,
            ing_ml,
            &tmp.path().join("sg-sync"),
            silo.port,
        );
        let mut sg_proc = start_mimir(
            Impl::Go,
            "go-store-gateway",
            &tmp.path().join("store-gateway"),
            &sg_cfg,
            &[],
        );
        wait_ready(&client, &mut sg_proc, sg_http, Duration::from_secs(45)).await;

        let q_http = free_test_port();
        let q_grpc = free_test_port();
        let q_ml = free_test_port();
        let q_cfg = querier_yaml(
            q_http,
            q_grpc,
            q_ml,
            ing_ml,
            &tmp.path().join("q-sync"),
            silo.port,
        );
        let mut q_proc = start_mimir(
            Impl::Go,
            "go-querier",
            &tmp.path().join("querier"),
            &q_cfg,
            &[],
        );
        wait_ready(&client, &mut q_proc, q_http, Duration::from_secs(45)).await;

        // Wait for store-gateway to load blocks and querier to see both ingester + store-gateway.
        let ready_deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (_, _, sg_m) = http_get(&client, sg_http, "/metrics", false)
                .await
                .unwrap_or((0, Default::default(), String::new()));
            let loaded = sum_metric_by_prefix(&sg_m, "cortex_bucket_store_blocks_loaded");
            let ing_ok = match http_get(&client, q_http, "/ingester/ring", true).await {
                Ok((200, _, b)) => b.contains("ACTIVE") && b.contains("ingester-0"),
                _ => false,
            };
            let sg_ok = match http_get(&client, sg_http, "/store-gateway/ring", true).await {
                Ok((200, _, b)) => b.contains("ACTIVE") && b.contains("store-gateway-0"),
                _ => false,
            };
            let (_, _, q_m) = http_get(&client, q_http, "/metrics", false)
                .await
                .unwrap_or((0, Default::default(), String::new()));
            let q_sg_ok = q_m.lines().any(|line| {
                line.starts_with("cortex_ring_members{")
                    && line.contains(r#"name="store-gateway-client""#)
                    && line.contains(r#"state="ACTIVE""#)
                    && line
                        .rsplit_once(' ')
                        .and_then(|(_, v)| v.parse::<f64>().ok())
                        .is_some_and(|v| v >= 1.0)
            });
            if loaded >= 1.0 && ing_ok && sg_ok && q_sg_ok {
                break;
            }
            if Instant::now() >= ready_deadline {
                panic!(
                    "store-gateway/querier did not converge (loaded={loaded}, ing_ok={ing_ok}, sg_ok={sg_ok}, q_sg_ok={q_sg_ok}).\nSG log:\n{}\nQ log:\n{}",
                    sg_proc.log(),
                    q_proc.log()
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Helper for instant PromQL assertions.
        let assert_scalar = |q_res: &Value, expected: f64, ctx: &str| {
            let res = q_res["data"]["result"].as_array().expect("result array");
            assert_eq!(res.len(), 1, "{ctx}: expected 1 series, got {q_res}");
            let got: f64 = res[0]["value"][1].as_str().unwrap().parse().unwrap();
            assert!(
                (got - expected).abs() < 1e-6,
                "{ctx}: expected {expected}, got {got} ({q_res})"
            );
        };

        // 1. Instant queries across historical S3 blocks AND live Rust Head:
        // Poll up to 10s for Go querier to observe the healthy store-gateway replica in memberlist
        // (retrying transient "at least 1 healthy replica required" 500 during gossip propagation).
        let q_w1_path = format!(
            "/prometheus/api/v1/query?query=eco_float_gauge&time={}",
            (w1 + 60_000) / 1000
        );
        let q_sg_poll_deadline = Instant::now() + Duration::from_secs(10);
        let q_w1 = loop {
            match prom_get_json(&client, q_http, TENANT, &q_w1_path).await {
                Ok(v) => break v,
                Err(err)
                    if err.to_string().contains("at least 1 healthy replica required")
                        && Instant::now() < q_sg_poll_deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(err) => panic!("w1 eco_float_gauge failed: {err:#}"),
            }
        };
        assert_scalar(&q_w1, 13.0, "w1 eco_float_gauge");

        let q_w2 = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!("/prometheus/api/v1/query?query=eco_float_gauge&time={}", (w2 + 60_000) / 1000),
        )
        .await
        .unwrap();
        assert_scalar(&q_w2, 23.0, "w2 eco_float_gauge");

        let q_ooo = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!("/prometheus/api/v1/query?query=eco_ooo_metric&time={}", (w2 + 30_000) / 1000),
        )
        .await
        .unwrap();
        assert_scalar(&q_ooo, 200.0, "w2 OOO eco_ooo_metric");

        let q_head = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!("/prometheus/api/v1/query?query=eco_head_only&time={}", head_t3 / 1000 + 1),
        )
        .await
        .unwrap();
        assert_scalar(&q_head, 8.5, "live head eco_head_only");

        // StaleNaN: present at w1, absent after w1 + 60_000
        let q_stale_before = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!("/prometheus/api/v1/query?query=eco_stale_series&time={}", (w1 + 10_000) / 1000),
        )
        .await
        .unwrap();
        assert_scalar(&q_stale_before, 99.5, "eco_stale_series before StaleNaN");

        let q_stale_after = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!("/prometheus/api/v1/query?query=eco_stale_series&time={}", (w1 + 90_000) / 1000),
        )
        .await
        .unwrap();
        assert!(
            q_stale_after["data"]["result"].as_array().unwrap().is_empty(),
            "eco_stale_series should be absent after StaleNaN: {q_stale_after}"
        );

        // 2. query_range spanning from w1 across S3 blocks -> live Rust Head (step=30s):
        let start_s = w1 / 1000;
        let end_s = start_s + ((head_t3 / 1000 - start_s) / 30 + 1) * 30;
        let qr_gauge = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!(
                "/prometheus/api/v1/query_range?query=eco_float_gauge&start={start_s}&end={end_s}&step=30"
            ),
        )
        .await
        .unwrap();
        let vals = qr_gauge["data"]["result"][0]["values"]
            .as_array()
            .expect("values array");
        assert!(!vals.is_empty(), "empty query_range result: {qr_gauge}");
        let mut seen_ts = BTreeSet::new();
        for pair in vals {
            let ts = pair[0].as_f64().unwrap().to_bits();
            assert!(
                seen_ts.insert(ts),
                "duplicate timestamp in query_range across block/head boundary: {pair}"
            );
        }
        // Check that both historical S3 block values (13.0, 23.0, 33.0) AND live head (44.0)
        // appear seamlessly in the same query_range!
        let range_floats: Vec<f64> = vals
            .iter()
            .map(|p| p[1].as_str().unwrap().parse::<f64>().unwrap())
            .collect();
        for expected_hist in [13.0, 23.0, 33.0] {
            assert!(
                range_floats.iter().any(|v| (v - expected_hist).abs() < 1e-6),
                "query_range missing historical S3 block value {expected_hist}: {range_floats:?}"
            );
        }
        let last_val = *range_floats.last().unwrap();
        assert!(
            (last_val - 44.0).abs() < 1e-6,
            "expected live head value 44.0 at end of query_range, got {last_val}"
        );

        // rate(eco_float_counter[5m]) across historical blocks (incl. counter reset) -> live Head:
        let qr_rate = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!(
                "/prometheus/api/v1/query_range?query=rate(eco_float_counter%5B5m%5D)&start={start_s}&end={end_s}&step=60"
            ),
        )
        .await
        .unwrap();
        let rate_vals = qr_rate["data"]["result"][0]["values"]
            .as_array()
            .expect("rate values");
        assert!(
            rate_vals.len() >= 4,
            "expected rate values across historical blocks + head, got {qr_rate}"
        );
        for pair in rate_vals {
            let rv: f64 = pair[1].as_str().unwrap().parse().unwrap();
            assert!(rv >= 0.0, "rate() must stay non-negative across counter reset: {rv}");
        }

        // Histograms (int, float, NHCB) across historical blocks -> live Head:
        for (metric, expr) in [
            ("eco_int_hist", "histogram_quantile(0.9, rate(eco_int_hist[5m]))"),
            ("eco_float_hist", "histogram_quantile(0.9, rate(eco_float_hist[5m]))"),
            ("eco_nhcb_hist", "histogram_quantile(0.9, rate(eco_nhcb_hist[5m]))"),
        ] {
            let q_h = prom_get_json(
                &client,
                q_http,
                TENANT,
                &format!("/prometheus/api/v1/query?query={}&time={}", urlencoding(expr), end_s),
            )
            .await
            .unwrap();
            let res = q_h["data"]["result"].as_array().unwrap();
            assert_eq!(res.len(), 1, "{metric} head quantile missing: {q_h}");
            let v: f64 = res[0]["value"][1].as_str().unwrap().parse().unwrap();
            assert!(v.is_finite(), "{metric} quantile not finite: {q_h}");

            // Raw histogram selector in historical window w1 (from Go store-gateway!)
            let q_hist_raw = prom_get_json(
                &client,
                q_http,
                TENANT,
                &format!("/prometheus/api/v1/query?query={metric}&time={}", (w1 + 60_000) / 1000),
            )
            .await
            .unwrap();
            let h_res = q_hist_raw["data"]["result"].as_array().unwrap();
            assert_eq!(h_res.len(), 1, "{metric} historical block query failed: {q_hist_raw}");
            assert!(
                h_res[0].get("histogram").is_some(),
                "{metric} historical block missing native histogram payload: {q_hist_raw}"
            );
        }

        // 3. Discovery endpoints across block + head: series, labels, label/__name__/values, exemplars, metadata:
        let series_res = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!(
                "/prometheus/api/v1/series?match%5B%5D=eco_float_gauge&start={start_s}&end={end_s}"
            ),
        )
        .await
        .unwrap();
        assert_eq!(series_res["data"].as_array().unwrap().len(), 1);

        let labels_res = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!("/prometheus/api/v1/labels?start={start_s}&end={end_s}"),
        )
        .await
        .unwrap();
        let label_names: BTreeSet<String> = labels_res["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        assert!(label_names.contains("__name__") && label_names.contains("job"));

        let names_res = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!("/prometheus/api/v1/label/__name__/values?start={start_s}&end={end_s}"),
        )
        .await
        .unwrap();
        let metric_names: BTreeSet<String> = names_res["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        for expected in [
            "eco_float_gauge",
            "eco_float_counter",
            "eco_int_hist",
            "eco_float_hist",
            "eco_nhcb_hist",
            "eco_ooo_metric",
            "eco_head_only",
            "eco_stale_series",
        ] {
            assert!(
                metric_names.contains(expected),
                "missing {expected} from label/__name__/values: {metric_names:?}"
            );
        }

        let ex_res = prom_get_json(
            &client,
            q_http,
            TENANT,
            &format!(
                "/prometheus/api/v1/query_exemplars?query=eco_float_counter&start={start_s}&end={end_s}"
            ),
        )
        .await
        .unwrap();
        let ex_arr = ex_res["data"].as_array().unwrap();
        assert!(
            !ex_arr.is_empty(),
            "expected exemplars from query_exemplars: {ex_res}"
        );

        let meta_res = prom_get_json(&client, q_http, TENANT, "/prometheus/api/v1/metadata")
            .await
            .unwrap();
        assert!(
            meta_res["data"].get("eco_float_counter").is_some(),
            "missing eco_float_counter metadata: {meta_res}"
        );

        // 4. Verify Go querier had ZERO store-gateway refetches!
        let (_, _, q_metrics) = http_get(&client, q_http, "/metrics", false)
            .await
            .expect("scrape querier /metrics");
        let refetches = sum_metric_by_prefix(&q_metrics, "cortex_querier_storegateway_refetches_total");
        assert_eq!(
            refetches, 0.0,
            "Go querier experienced store-gateway refetches ({refetches})!\nQuerier log:\n{}",
            q_proc.log()
        );
        anyhow::Ok(())
    })
    .unwrap();
}

fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------------------------
// Test B: Bidirectional Same-PVC Live Upgrade & Emergency Rollback (Go -> SIGKILL -> Rust -> SIGKILL -> Go)
// ---------------------------------------------------------------------------------------------

fn wire_enc_to_tsdb(enc: i32) -> mimir_tsdb::chunkenc::Encoding {
    use mimir_tsdb::chunkenc::Encoding;
    match enc {
        4 => Encoding::Xor,
        5 => Encoding::Histogram,
        6 => Encoding::FloatHistogram,
        7 => Encoding::Xor2,
        8 => Encoding::HistogramSt,
        9 => Encoding::FloatHistogramSt,
        other => panic!("unexpected wire chunk encoding {other}"),
    }
}

async fn query_stream_values(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    metric: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<(i64, f64)>> {
    let mut req = tonic::Request::new(ingesterpb::QueryRequest {
        start_timestamp_ms: start_ms,
        end_timestamp_ms: end_ms,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: metric.to_string(),
        }],
        ..Default::default()
    });
    req.metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(tenant)?);
    let mut stream = client.query_stream(req).await?.into_inner();
    let mut chunks_by_idx: BTreeMap<u64, Vec<ingesterpb::Chunk>> = BTreeMap::new();
    while let Some(resp) = stream.next().await {
        let resp = resp?;
        for sc in resp.streaming_series_chunks {
            chunks_by_idx
                .entry(sc.series_index)
                .or_default()
                .extend(sc.chunks);
        }
    }
    let mut out = Vec::new();
    for chks in chunks_by_idx.into_values() {
        for ch in chks {
            let enc = wire_enc_to_tsdb(ch.encoding);
            let parsed = mimir_tsdb::chunkenc::EncodedChunk::from_bytes(enc, ch.data.to_vec())?;
            for ds in mimir_tsdb::querier::chunk_to_samples(&parsed)? {
                if ds.t >= start_ms && ds.t <= end_ms {
                    if let mimir_tsdb::compact::SampleValue::Float(v) = ds.value {
                        out.push((ds.t, v));
                    }
                }
            }
        }
    }
    out.sort_by_key(|(t, _)| *t);
    out.dedup_by_key(|(t, _)| *t);
    Ok(out)
}

async fn query_stream_hist_count(
    client: &mut IngesterClient<Channel>,
    tenant: &str,
    metric: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<usize> {
    let mut req = tonic::Request::new(ingesterpb::QueryRequest {
        start_timestamp_ms: start_ms,
        end_timestamp_ms: end_ms,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".to_string(),
            value: metric.to_string(),
        }],
        ..Default::default()
    });
    req.metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(tenant)?);
    let mut stream = client.query_stream(req).await?.into_inner();
    let mut chunks_by_idx: BTreeMap<u64, Vec<ingesterpb::Chunk>> = BTreeMap::new();
    while let Some(resp) = stream.next().await {
        let resp = resp?;
        for sc in resp.streaming_series_chunks {
            chunks_by_idx
                .entry(sc.series_index)
                .or_default()
                .extend(sc.chunks);
        }
    }
    let mut count = 0usize;
    for chks in chunks_by_idx.into_values() {
        for ch in chks {
            let enc = wire_enc_to_tsdb(ch.encoding);
            let parsed = mimir_tsdb::chunkenc::EncodedChunk::from_bytes(enc, ch.data.to_vec())?;
            for ds in mimir_tsdb::querier::chunk_to_samples(&parsed)? {
                if ds.t >= start_ms && ds.t <= end_ms {
                    count += 1;
                }
            }
        }
    }
    Ok(count)
}

#[test]
fn w10_02_bidirectional_pvc_upgrade_and_emergency_rollback() {
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().unwrap();
        let tsdb_dir = tmp.path().join("shared-pvc-tsdb");
        fs::create_dir_all(&tsdb_dir).unwrap();
        let client = http_client();

        let now = now_ms();
        let curr_block = (now / TWO_HOURS_MS) * TWO_HOURS_MS;
        let old_w = curr_block - 2 * TWO_HOURS_MS + 60_000;
        let live_base = now - 300_000;

        // -----------------------------------------------------------------
        // Phase 1: Go ingester on tsdb_dir -> push floats + histograms + OOO + exemplars + metadata -> SIGKILL
        // -----------------------------------------------------------------
        let go1_http = free_test_port();
        let go1_grpc = free_test_port();
        let go1_ml = free_test_port();
        let go1_cfg = ingester_yaml(go1_http, go1_grpc, go1_ml, &tsdb_dir, None);
        let mut go1 = start_mimir(
            Impl::Go,
            "go-phase1",
            &tmp.path().join("go1"),
            &go1_cfg,
            &[],
        );
        wait_ready(&client, &mut go1, go1_http, Duration::from_secs(45)).await;

        let mut c1 = connect_ingester(go1_grpc).await.unwrap();
        grpc_push(
            &mut c1,
            TENANT,
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![
                    // Historical window (will be compacted by Rust later)
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "pvc_pre_float"), lp("phase", "go")],
                        created_timestamp: old_w - 1000,
                        samples: vec![
                            cortexpb::Sample { value: 1.5, timestamp_ms: old_w },
                            cortexpb::Sample { value: 2.5, timestamp_ms: old_w + 10_000 },
                        ],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "pvc_live_float"), lp("phase", "go")],
                        created_timestamp: live_base - 1000,
                        samples: vec![
                            cortexpb::Sample { value: 10.0, timestamp_ms: live_base },
                            cortexpb::Sample { value: 20.0, timestamp_ms: live_base + 20_000 },
                        ],
                        exemplars: vec![cortexpb::Exemplar {
                            labels: vec![lp("trace_id", "go-pre")],
                            value: 20.0,
                            timestamp_ms: live_base + 20_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "pvc_pre_hist"), lp("phase", "go")],
                        histograms: vec![int_hist(live_base + 10_000, 2, 5, 1, 8.0, vec![2, 2], 0)],
                        ..Default::default()
                    },
                ],
                metadata: vec![cortexpb::MetricMetadata {
                    r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                    metric_family_name: "pvc_live_float".into(),
                    help: "pre-upgrade gauge".into(),
                    unit: "".into(),
                }],
                ..Default::default()
            },
        )
        .await
        .expect("push phase1");

        // OOO sample in Go phase 1 (WBL write)
        grpc_push(
            &mut c1,
            TENANT,
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "pvc_live_float"), lp("phase", "go")],
                    samples: vec![cortexpb::Sample {
                        value: 15.0,
                        timestamp_ms: live_base + 10_000,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .expect("push phase1 OOO");

        drop(c1);
        go1.signal("KILL");
        let st1 = go1.wait_exit(Duration::from_secs(10)).expect("go1 SIGKILL exit");
        assert!(!st1.success(), "SIGKILL must exit non-zero");

        // -----------------------------------------------------------------
        // Phase 2: Rust ingester on exact same tsdb_dir -> verify 100% of Go data ->
        // compact a block -> push >125 floats + histograms + OOO (chunks_head + WAL + WBL) -> SIGKILL
        // -----------------------------------------------------------------
        let rs_http = free_test_port();
        let rs_grpc = free_test_port();
        let rs_ml = free_test_port();
        let rs_cfg = ingester_yaml(rs_http, rs_grpc, rs_ml, &tsdb_dir, None);
        let mut rs = start_mimir(
            Impl::Rust,
            "rust-phase2",
            &tmp.path().join("rust2"),
            &rs_cfg,
            &[],
        );
        wait_ready(&client, &mut rs, rs_http, Duration::from_secs(45)).await;

        let mut c2 = connect_ingester(rs_grpc).await.unwrap();
        let got_pre_old = query_stream_values(&mut c2, TENANT, "pvc_pre_float", old_w - 10_000, now + 60_000)
            .await
            .unwrap();
        assert_eq!(
            got_pre_old,
            vec![(old_w - 1000, 0.0), (old_w, 1.5), (old_w + 10_000, 2.5)],
            "Rust missed Go pre-upgrade historical samples"
        );
        let got_pre_live = query_stream_values(&mut c2, TENANT, "pvc_live_float", old_w - 10_000, now + 60_000)
            .await
            .unwrap();
        assert_eq!(
            got_pre_live,
            vec![
                (live_base - 1000, 0.0),
                (live_base, 10.0),
                (live_base + 10_000, 15.0),
                (live_base + 20_000, 20.0),
            ],
            "Rust missed Go pre-upgrade live+OOO samples"
        );
        let got_pre_hist = query_stream_hist_count(&mut c2, TENANT, "pvc_pre_hist", old_w - 10_000, now + 60_000)
            .await
            .unwrap();
        assert_eq!(got_pre_hist, 1, "Rust missed Go pre-upgrade histogram");

        // Trigger block compaction via /ingester/flush?wait=true (creates compacted ULID block(s) on tsdb_dir)
        let flush = client
            .post(format!("http://127.0.0.1:{rs_http}/ingester/flush?wait=true"))
            .send()
            .await
            .unwrap();
        assert!(flush.status().is_success(), "Rust flush failed: {}", flush.status());

        // Now push >125 float samples on a series so head chunk rolls over into chunks_head/, plus
        // native histograms and an OOO sample (WBL + WAL).
        let post_base = live_base + 40_000;
        let bulk_samples: Vec<cortexpb::Sample> = (0..135)
            .map(|i| cortexpb::Sample {
                value: 100.0 + i as f64,
                timestamp_ms: post_base + i * 1000,
            })
            .collect();
        grpc_push(
            &mut c2,
            TENANT,
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "pvc_rust_bulk"), lp("phase", "rust")],
                        created_timestamp: post_base - 1000,
                        samples: bulk_samples.clone(),
                        exemplars: vec![cortexpb::Exemplar {
                            labels: vec![lp("trace_id", "rust-post")],
                            value: 110.0,
                            timestamp_ms: post_base + 10_000,
                        }],
                        ..Default::default()
                    },
                    cortexpb::TimeSeries {
                        labels: vec![lp("__name__", "pvc_rust_hist"), lp("phase", "rust")],
                        histograms: vec![
                            int_hist(post_base + 5_000, 2, 12, 2, 22.0, vec![5, 5], 0),
                            int_hist(post_base + 15_000, 2, 24, 4, 44.0, vec![10, 10], 2),
                        ],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        )
        .await
        .expect("push phase2 bulk + hist");

        // Rust OOO sample (writes to WBL)
        let rust_ooo_ts = post_base + 2_500;
        grpc_push(
            &mut c2,
            TENANT,
            cortexpb::WriteRequest {
                source: cortexpb::write_request::SourceEnum::Api as i32,
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![lp("__name__", "pvc_rust_bulk"), lp("phase", "rust")],
                    samples: vec![cortexpb::Sample {
                        value: 999.0,
                        timestamp_ms: rust_ooo_ts,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .expect("push phase2 OOO");

        // Verify chunks_head/, wal/, wbl/, and at least 1 ULID block exist on tsdb_dir/<TENANT> before SIGKILL!
        let tenant_dir = tsdb_dir.join(TENANT);
        let chunks_head_dir = tenant_dir.join("chunks_head");
        let wait_ch = Instant::now() + Duration::from_secs(10);
        loop {
            let has_ch = fs::read_dir(&chunks_head_dir)
                .map(|rd| rd.filter_map(|e| e.ok()).next().is_some())
                .unwrap_or(false);
            if has_ch {
                break;
            }
            if Instant::now() >= wait_ch {
                bail!("expected chunks_head files under {}", chunks_head_dir.display());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(tenant_dir.join("wal").exists(), "expected WAL dir");
        assert!(tenant_dir.join("wbl").exists(), "expected WBL dir");

        drop(c2);
        rs.signal("KILL");
        let st2 = rs.wait_exit(Duration::from_secs(10)).expect("rust SIGKILL exit");
        assert!(!st2.success(), "SIGKILL must exit non-zero");

        // -----------------------------------------------------------------
        // Phase 3: Emergency rollback to Go ingester on EXACT SAME tsdb_dir
        // -----------------------------------------------------------------
        let go3_http = free_test_port();
        let go3_grpc = free_test_port();
        let go3_ml = free_test_port();
        let go3_cfg = ingester_yaml(go3_http, go3_grpc, go3_ml, &tsdb_dir, None);
        let mut go3 = start_mimir(
            Impl::Go,
            "go-phase3-rollback",
            &tmp.path().join("go3"),
            &go3_cfg,
            &[],
        );
        wait_ready(&client, &mut go3, go3_http, Duration::from_secs(45)).await;

        let (mcode, _, go3_metrics) = http_get(&client, go3_http, "/metrics", false)
            .await
            .unwrap();
        assert_eq!(mcode, 200);
        let wal_corrupt = sum_metric_by_prefix(&go3_metrics, "prometheus_tsdb_wal_corruptions_total");
        let head_trunc_fail =
            sum_metric_by_prefix(&go3_metrics, "prometheus_tsdb_head_truncations_failed_total");
        assert_eq!(
            wal_corrupt, 0.0,
            "Go rollback had WAL corruptions ({wal_corrupt}):\n{}",
            go3.log()
        );
        assert_eq!(
            head_trunc_fail, 0.0,
            "Go rollback had head truncation failures ({head_trunc_fail}):\n{}",
            go3.log()
        );

        let mut c3 = connect_ingester(go3_grpc).await.unwrap();
        let rb_old = query_stream_values(&mut c3, TENANT, "pvc_pre_float", old_w - 10_000, now + 600_000)
            .await
            .unwrap();
        assert_eq!(
            rb_old,
            vec![(old_w - 1000, 0.0), (old_w, 1.5), (old_w + 10_000, 2.5)],
            "Go rollback missing pre-upgrade historical samples"
        );
        let rb_live = query_stream_values(&mut c3, TENANT, "pvc_live_float", old_w - 10_000, now + 600_000)
            .await
            .unwrap();
        assert_eq!(
            rb_live,
            vec![
                (live_base - 1000, 0.0),
                (live_base, 10.0),
                (live_base + 10_000, 15.0),
                (live_base + 20_000, 20.0),
            ],
            "Go rollback missing pre-upgrade live+OOO samples"
        );
        let rb_pre_hist = query_stream_hist_count(&mut c3, TENANT, "pvc_pre_hist", old_w - 10_000, now + 600_000)
            .await
            .unwrap();
        assert_eq!(rb_pre_hist, 1, "Go rollback missing pre-upgrade histogram");

        let rb_bulk = query_stream_values(&mut c3, TENANT, "pvc_rust_bulk", old_w - 10_000, now + 600_000)
            .await
            .unwrap();
        let mut expected_bulk: Vec<(i64, f64)> = bulk_samples
            .iter()
            .map(|s| (s.timestamp_ms, s.value))
            .collect();
        expected_bulk.push((post_base - 1000, 0.0));
        expected_bulk.push((rust_ooo_ts, 999.0));
        expected_bulk.sort_by_key(|(t, _)| *t);
        assert_eq!(
            rb_bulk, expected_bulk,
            "Go rollback missing Rust-written chunks_head/WAL/WBL samples"
        );

        let rb_rust_hist = query_stream_hist_count(&mut c3, TENANT, "pvc_rust_hist", old_w - 10_000, now + 600_000)
            .await
            .unwrap();
        assert_eq!(rb_rust_hist, 2, "Go rollback missing Rust-written histograms");

        let mut ex_req = tonic::Request::new(ingesterpb::ExemplarQueryRequest {
            start_timestamp_ms: old_w - 10_000,
            end_timestamp_ms: now + 600_000,
            matchers: vec![ingesterpb::LabelMatchers {
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".to_string(),
                    value: "pvc_rust_bulk".to_string(),
                }],
            }],
        });
        ex_req
            .metadata_mut()
            .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(TENANT).unwrap());
        let ex_resp = c3.query_exemplars(ex_req).await.unwrap().into_inner();
        assert!(
            !ex_resp.timeseries.is_empty(),
            "Go rollback missing Rust-written WAL exemplars"
        );
        anyhow::Ok(())
    })
    .unwrap();
}
