//! Twin-process differential test for the per-tenant TSDB registry (Wave 20, M20-04).
//!
//! Go Mimir serves the raw Prometheus registry of a tenant's `tsdb.DB` (everything `tsdb.Open`
//! registers: `dbMetrics`, `headMetrics`, `CompactorMetrics`, the WAL / WBL `wlMetrics`, the
//! exemplar storage and the chunk write queue) on `GET /ingester/tsdb_metrics`
//! (`pkg/ingester/ingester.go` `UserRegistryHandler`, `pkg/storage/tsdb/tsdb_metrics.go`). This test
//! boots the unmodified Go `mimir` and the Rust `mimir` side by side with the same configuration,
//! replays the same workload (floats, native histograms, exemplars, duplicates, out-of-bounds and
//! out-of-order samples, a head compaction) and requires both registries to expose exactly the
//! same metric families (name, TYPE, HELP) and the same series (label names and values,
//! including every `le` bucket and `quantile`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::twin::{free_tcp_ports, start_mimir, wait_ready, Impl, Proc};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const TENANT: &str = "tenant-w20";

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_gzip()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap()
}

fn yaml(http: u16, grpc: u16, dir: &Path, limits: &str) -> String {
    format!(
        r#"target: ingester
multitenancy_enabled: true
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
usage_stats:
  enabled: false
ingester:
  push_grpc_method_enabled: true
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    replication_factor: 1
    num_tokens: 16
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {blocks}
  tsdb:
    dir: {tsdb}
    ship_interval: 1h
{limits}"#,
        blocks = dir.join("blocks").display(),
        tsdb = dir.join("tsdb").display(),
    )
}

fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
    cortexpb::LabelPair {
        name: name.to_string().into(),
        value: value.to_string().into(),
    }
}

fn float_series(metric: &str, ts: i64, v: f64) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: vec![lp("__name__", metric), lp("job", "api")],
        samples: vec![cortexpb::Sample {
            timestamp_ms: ts,
            value: v,
        }],
        ..Default::default()
    }
}

fn hist_series(metric: &str, ts: i64, n: i64) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: vec![lp("__name__", metric), lp("job", "api")],
        histograms: vec![cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountInt((2 * n + 3) as u64)),
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
            zero_threshold: 0.001,
            sum: n as f64 * 1.5,
            schema: 2,
            positive_spans: vec![cortexpb::BucketSpan {
                offset: -1,
                length: 2,
            }],
            positive_deltas: vec![n, 1],
            negative_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_deltas: vec![1],
            timestamp: ts,
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Pushes one series; rejections (duplicates, out of bounds...) are part of the workload.
async fn push(client: &mut IngesterClient<Channel>, ts: cortexpb::TimeSeries) -> Result<(), tonic::Code> {
    let mut req = tonic::Request::new(cortexpb::WriteRequest {
        timeseries: vec![ts],
        ..Default::default()
    });
    req.metadata_mut()
        .insert(LOWER_ORG_ID_HEADER_NAME, MetadataValue::try_from(TENANT).unwrap());
    client.push(req).await.map(|_| ()).map_err(|s| s.code())
}

/// Returns the outcome of every push so both implementations can be required to agree on it.
async fn workload(grpc_port: u16, now_ms: i64) -> Vec<String> {
    let mut client = IngesterClient::connect(format!("http://127.0.0.1:{grpc_port}"))
        .await
        .unwrap();
    let mut outcomes = Vec::new();
    let mut record = |what: &str, res: Result<(), tonic::Code>| {
        outcomes.push(format!("{what}: {res:?}"));
    };

    for i in 0..20i64 {
        let ts = now_ms - 60_000 + i * 1_000;
        let mut floats = float_series("w20_float_total", ts, i as f64);
        if i % 5 == 0 {
            floats.exemplars = vec![cortexpb::Exemplar {
                labels: vec![lp("trace_id", &format!("tr-{i}"))],
                value: i as f64,
                timestamp_ms: ts,
            }];
        }
        record("float", push(&mut client, floats).await);
        record("hist", push(&mut client, hist_series("w20_hist", ts, 1 + i)).await);
    }
    // Staleness markers: float and histogram series.
    let stale = f64::from_bits(0x7ff0_0000_0000_0002);
    record(
        "stale float",
        push(&mut client, float_series("w20_stale_total", now_ms - 30_000, 1.0)).await,
    );
    record(
        "stale float marker",
        push(&mut client, float_series("w20_stale_total", now_ms - 29_000, stale)).await,
    );
    // Duplicate timestamp with another value, then exact duplicates.
    let dup_ts = now_ms - 60_000 + 19_000;
    record("dup float", push(&mut client, float_series("w20_float_total", dup_ts, 123.0)).await);
    record("same float", push(&mut client, float_series("w20_float_total", dup_ts, 19.0)).await);
    record("dup hist", push(&mut client, hist_series("w20_hist", dup_ts, 99)).await);
    // Out of order (inside the window when one is configured) and far too old.
    for j in 0..40i64 {
        let ts = now_ms - 300_000 + (39 - j) * 1_000;
        record("ooo float", push(&mut client, float_series("w20_float_total", ts, 1000.0 + j as f64)).await);
        if j % 4 == 0 {
            record("ooo hist", push(&mut client, hist_series("w20_hist", ts, 5 + j)).await);
        }
    }
    record(
        "too old float",
        push(&mut client, float_series("w20_float_total", now_ms - 6 * 3_600_000, 1.0)).await,
    );
    record(
        "too old hist",
        push(&mut client, hist_series("w20_hist", now_ms - 6 * 3_600_000, 1)).await,
    );
    outcomes
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Exposition {
    /// Metric family name -> (TYPE, HELP).
    families: BTreeMap<String, (String, String)>,
    /// Series identities: sample name plus its label set, without the value.
    series: BTreeSet<String>,
    /// Sample values keyed by series identity.
    values: BTreeMap<String, String>,
}

fn parse_exposition(text: &str) -> Exposition {
    let mut exp = Exposition::default();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, help) = rest.split_once(' ').unwrap_or((rest, ""));
            exp.families.entry(name.to_string()).or_default().1 = help.to_string();
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, typ) = rest.split_once(' ').unwrap_or((rest, ""));
            exp.families.entry(name.to_string()).or_default().0 = typ.to_string();
        } else if !line.is_empty() && !line.starts_with('#') {
            // Label values of these registries contain no spaces.
            let (key, value) = line.rsplit_once(' ').expect("sample line");
            assert!(exp.series.insert(key.to_string()), "duplicate series {key}");
            exp.values.insert(key.to_string(), value.to_string());
        }
    }
    exp
}

async fn tsdb_metrics(client: &reqwest::Client, proc: &Proc, http_port: u16) -> Exposition {
    let resp = client
        .get(format!("http://127.0.0.1:{http_port}/ingester/tsdb_metrics"))
        .header("X-Scope-OrgID", TENANT)
        .send()
        .await
        .unwrap_or_else(|e| panic!("{}: GET /ingester/tsdb_metrics: {e}", proc.name));
    assert_eq!(resp.status().as_u16(), 200, "{}", proc.name);
    let text = resp.text().await.unwrap();
    if let Some(dir) = std::env::var_os("W20_TSDB_METRICS_DUMP_DIR") {
        let n = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
        std::fs::write(Path::new(&dir).join(format!("{n:02}-{}.txt", proc.name)), &text).unwrap();
    }
    parse_exposition(&text)
}

/// Metrics whose value is the same in Go and Rust for the same workload: counts of what was
/// ingested and what the head holds (not durations, sizes on disk or WAL segment numbers).
const DETERMINISTIC_VALUES: &[&str] = &[
    "prometheus_tsdb_head_series",
    "prometheus_tsdb_head_series_created_total",
    "prometheus_tsdb_head_series_removed_total",
    "prometheus_tsdb_head_samples_appended_total",
    "prometheus_tsdb_head_out_of_order_samples_appended_total",
    "prometheus_tsdb_duplicate_samples_total",
    "prometheus_tsdb_sample_ooo_delta_count",
    "prometheus_tsdb_sample_ooo_delta_sum",
    "prometheus_tsdb_head_gc_duration_seconds_count",
    "prometheus_tsdb_compactions_triggered_total",
    "prometheus_tsdb_compactions_skipped_total",
    "prometheus_tsdb_head_chunks",
    "prometheus_tsdb_head_chunks_created_total",
    "prometheus_tsdb_head_chunks_removed_total",
    "prometheus_tsdb_head_active_appenders",
    "prometheus_tsdb_out_of_order_samples_total",
    "prometheus_tsdb_out_of_bound_samples_total",
    "prometheus_tsdb_too_old_samples_total",
    "prometheus_tsdb_head_stale_series",
    "prometheus_tsdb_head_native_histogram_series",
    "prometheus_tsdb_head_native_histogram_buckets",
    "prometheus_tsdb_head_min_time",
    "prometheus_tsdb_head_max_time",
    "prometheus_tsdb_lowest_timestamp",
    "prometheus_tsdb_isolation_low_watermark",
    "prometheus_tsdb_isolation_high_watermark",
    "prometheus_tsdb_wal_corruptions_total",
    "prometheus_tsdb_mmap_chunk_corruptions_total",
    "prometheus_tsdb_blocks_loaded",
    "prometheus_tsdb_compactions_total",
    "prometheus_tsdb_compactions_failed_total",
    "prometheus_tsdb_retention_limit_bytes",
    "prometheus_tsdb_retention_limit_seconds",
    "prometheus_tsdb_exemplar_exemplars_appended_total",
    "prometheus_tsdb_exemplar_exemplars_in_storage",
    "prometheus_tsdb_exemplar_series_with_exemplars_in_storage",
    "prometheus_tsdb_exemplar_max_exemplars",
    "prometheus_tsdb_exemplar_out_of_order_exemplars_total",
];

fn assert_same_registry(stage: &str, go: &Exposition, rust: &Exposition) {
    let mut diffs = Vec::new();
    for name in go.families.keys().chain(rust.families.keys()).collect::<BTreeSet<_>>() {
        match (go.families.get(name), rust.families.get(name)) {
            (Some(g), Some(r)) if g == r => {}
            (Some(g), Some(r)) => diffs.push(format!(
                "family {name}:\n    go:   TYPE {} HELP {}\n    rust: TYPE {} HELP {}",
                g.0, g.1, r.0, r.1
            )),
            (Some(g), None) => diffs.push(format!("family {name} ({}) missing in Rust", g.0)),
            (None, Some(r)) => diffs.push(format!("family {name} ({}) only in Rust", r.0)),
            (None, None) => unreachable!(),
        }
    }
    for key in go.series.symmetric_difference(&rust.series) {
        let side = if go.series.contains(key) { "missing in Rust" } else { "only in Rust" };
        diffs.push(format!("series {key} {side}"));
    }
    for (key, gv) in &go.values {
        let name = key.split('{').next().unwrap_or(key);
        if !DETERMINISTIC_VALUES.contains(&name) {
            continue;
        }
        if let Some(rv) = rust.values.get(key) {
            let same = match (gv.parse::<f64>(), rv.parse::<f64>()) {
                (Ok(g), Ok(r)) => g == r || (g.is_nan() && r.is_nan()),
                _ => gv == rv,
            };
            if !same {
                diffs.push(format!("value {key}: go={gv} rust={rv}"));
            }
        }
    }
    assert!(
        diffs.is_empty(),
        "[{stage}] /ingester/tsdb_metrics differs between Go and Rust ({} differences):\n{}",
        diffs.len(),
        diffs.join("\n")
    );
    assert!(go.families.len() >= 80, "[{stage}] Go registry has {} families", go.families.len());
}

fn run_scenario(name: &str, limits: &str) {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let root = tempfile::tempdir().unwrap();
    let ports = free_tcp_ports(4);
    block_on_for_tests(async {
        let client = http_client();
        let mut procs = Vec::new();
        for (i, which) in [Impl::Go, Impl::Rust].into_iter().enumerate() {
            let dir = root.path().join(which.name());
            let (http, grpc) = (ports[i * 2], ports[i * 2 + 1]);
            let mut proc = start_mimir(which, name, &dir, &yaml(http, grpc, &dir, limits), &[]);
            wait_ready(&client, &mut proc, http, Duration::from_secs(60)).await;
            procs.push((proc, http, grpc));
        }

        let go_outcomes = workload(procs[0].2, now_ms).await;
        let rust_outcomes = workload(procs[1].2, now_ms).await;
        assert_eq!(go_outcomes, rust_outcomes, "[{name}] push outcomes differ");

        let go = tsdb_metrics(&client, &procs[0].0, procs[0].1).await;
        let rust = tsdb_metrics(&client, &procs[1].0, procs[1].1).await;
        assert_same_registry(&format!("{name}: after ingestion"), &go, &rust);

        // Compact the head (and the out-of-order head) into blocks.
        for (proc, http, _) in &procs {
            let resp = client
                .post(format!("http://127.0.0.1:{http}/ingester/flush?wait=true"))
                .send()
                .await
                .unwrap_or_else(|e| panic!("{}: POST /ingester/flush: {e}", proc.name));
            assert!(resp.status().is_success(), "{}: flush {}", proc.name, resp.status());
        }
        let go = tsdb_metrics(&client, &procs[0].0, procs[0].1).await;
        let rust = tsdb_metrics(&client, &procs[1].0, procs[1].1).await;
        assert_same_registry(&format!("{name}: after flush"), &go, &rust);
    });
}

#[test]
fn m20_04_per_tenant_tsdb_registry_matches_go_with_default_limits() {
    run_scenario("defaults", "");
}

#[test]
fn m20_04_per_tenant_tsdb_registry_matches_go_with_ooo_window_and_exemplars() {
    run_scenario(
        "ooo",
        "limits:\n  out_of_order_time_window: 10m\n  native_histograms_ingestion_enabled: true\n  max_global_exemplars_per_user: 1000\n",
    );
}
