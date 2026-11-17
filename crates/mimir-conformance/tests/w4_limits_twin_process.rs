//! Twin-process (Go `target/go-mimir` vs Rust `target/release/mimir`) differential test for the
//! ingester-side per-tenant limits that were previously parsed but not honoured:
//! cost attribution (`/usage-metrics`), additional active series custom trackers,
//! `separate_metrics_group_label`, the sampled-error gRPC suffix and native histogram bucket limits.
//!
//! Go reference: pkg/costattribution, pkg/ingester/ingester_push.go, pkg/ingester/ingester_metrics_update.go,
//! pkg/util/active_groups.go, vendor/github.com/grafana/dskit/middleware/grpc_logging.go.

use std::collections::BTreeSet;
use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::block_on_for_tests;
use mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, ensure_rust_mimir_release_built, go_mimir_bin, rust_mimir_release_bin,
};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn write_yaml(dir: &Path, http: u16, grpc: u16) -> std::path::PathBuf {
    let yaml = format!(
        r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc}
multitenancy_enabled: true
cost_attribution_registry_path: /usage-metrics
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
  active_series_metrics_enabled: true
  active_series_metrics_update_period: 1s
limits:
  native_histograms_ingestion_enabled: true
  max_native_histogram_buckets: 5
  max_global_series_per_metric: 1
  separate_metrics_group_label: grp
  active_series_custom_trackers:
    base: '{{team="a"}}'
  active_series_additional_custom_trackers:
    addtrk: '{{grp="g1"}}'
  cost_attribution_trackers:
    by-team:
      labels:
        - input: team
          output: my_team
  max_cost_attribution_cardinality: 100
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
activity_tracker:
  filepath: ""
"#,
        bucket = dir.join("bucket").display(),
        tsdb = dir.join("tsdb").display(),
    );
    let p = dir.join("mimir.yaml");
    fs::write(&p, yaml).unwrap();
    p
}

fn spawn(bin: &Path, cfg: &Path, dir: &Path) -> Proc {
    Proc(
        Command::new(bin)
            .args([
                "-target=ingester".to_string(),
                format!("-config.file={}", cfg.display()),
            ])
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display())),
    )
}

async fn wait_ready(http: &reqwest::Client, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(r) = http.get(format!("http://127.0.0.1:{port}/ready")).send().await {
            if r.status().as_u16() == 200 {
                return;
            }
        }
        assert!(Instant::now() < deadline, "port {port} not ready within 30s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn series(labels: &[(&'static str, &'static str)], ts: i64) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: labels
            .iter()
            .map(|(n, v)| cortexpb::LabelPair::from_static(n, v))
            .collect(),
        samples: vec![cortexpb::Sample {
            value: 1.0,
            timestamp_ms: ts,
        }],
        ..Default::default()
    }
}

fn histogram_series(labels: &[(&'static str, &'static str)], ts: i64) -> cortexpb::TimeSeries {
    // 8 populated buckets (> max_native_histogram_buckets: 5).
    cortexpb::TimeSeries {
        labels: labels
            .iter()
            .map(|(n, v)| cortexpb::LabelPair::from_static(n, v))
            .collect(),
        histograms: vec![cortexpb::Histogram {
            count: Some(cortexpb::histogram::Count::CountInt(8)),
            sum: 8.0,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0)),
            positive_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: 8,
            }],
            positive_deltas: vec![1, 0, 0, 0, 0, 0, 0, 0],
            reset_hint: cortexpb::histogram::ResetHint::No as i32,
            timestamp: ts,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn request(ts: Vec<cortexpb::TimeSeries>) -> tonic::Request<cortexpb::WriteRequest> {
    let mut req = tonic::Request::new(cortexpb::WriteRequest {
        timeseries: ts,
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    });
    req.metadata_mut().insert(
        LOWER_ORG_ID_HEADER_NAME,
        MetadataValue::try_from("t1").unwrap(),
    );
    req
}

/// Normalized `name{sorted labels} value` lines for the given metric families.
fn families(text: &str, names: &[&str]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let (series, value) = line.rsplit_once(' ').unwrap();
        let (name, labels) = match series.find('{') {
            Some(i) => (&series[..i], &series[i + 1..series.len() - 1]),
            None => (series, ""),
        };
        if !names.contains(&name) {
            continue;
        }
        let mut pairs: Vec<&str> = labels.split(',').filter(|s| !s.is_empty()).collect();
        pairs.sort();
        let v: f64 = value.parse().unwrap();
        out.insert(format!("{name}{{{}}} {v}", pairs.join(",")));
    }
    out
}

#[test]
fn twin_process_ingester_limits_cost_attribution_custom_trackers_groups_parity() {
    ensure_go_oracle_built().expect("go-mimir must build");
    ensure_rust_mimir_release_built().expect("target/release/mimir must build");

    let tmp = tempfile::tempdir().unwrap();
    let (go_dir, rust_dir) = (tmp.path().join("go"), tmp.path().join("rust"));
    for d in [&go_dir, &rust_dir] {
        fs::create_dir_all(d.join("tsdb")).unwrap();
        fs::create_dir_all(d.join("bucket")).unwrap();
    }
    let (go_http, go_grpc, rust_http, rust_grpc) = (free_port(), free_port(), free_port(), free_port());
    let go_cfg = write_yaml(&go_dir, go_http, go_grpc);
    let rust_cfg = write_yaml(&rust_dir, rust_http, rust_grpc);
    let _go = spawn(&go_mimir_bin(), &go_cfg, &go_dir);
    let _rust = spawn(&rust_mimir_release_bin(), &rust_cfg, &rust_dir);

    block_on_for_tests(async {
        let http = reqwest::Client::new();
        wait_ready(&http, go_http).await;
        wait_ready(&http, rust_http).await;
        let connect = |port: u16| async move {
            IngesterClient::new(
                Channel::from_shared(format!("http://127.0.0.1:{port}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap(),
            )
        };
        let mut go = connect(go_grpc).await;
        let mut rust = connect(rust_grpc).await;

        let ts = now_ms();
        let pushes: Vec<Vec<cortexpb::TimeSeries>> = vec![
            vec![series(&[("__name__", "m1"), ("grp", "g1"), ("i", "1"), ("team", "a")], ts)],
            // Second series of m1 exceeds max_global_series_per_metric: 1 (sampled soft error).
            vec![series(&[("__name__", "m1"), ("grp", "g1"), ("i", "2"), ("team", "a")], ts)],
            vec![series(&[("__name__", "m2"), ("grp", "g1"), ("team", "b")], ts)],
            // 8 buckets > max_native_histogram_buckets: accepted by the Go ingester (distributor-only limit).
            vec![histogram_series(&[("__name__", "h1"), ("grp", "g1"), ("team", "a")], ts)],
            vec![series(&[("__name__", "m3"), ("team", "c")], ts)],
            // Group g2: exceeds per-metric limit too, discarded under group "g2".
            vec![series(&[("__name__", "m3"), ("grp", "g2"), ("team", "c")], ts)],
        ];
        for (i, p) in pushes.into_iter().enumerate() {
            let g = go.push(request(p.clone())).await;
            let r = rust.push(request(p)).await;
            match (g, r) {
                (Ok(_), Ok(_)) => {}
                (Err(ge), Err(re)) => {
                    assert_eq!(re.code(), ge.code(), "push {i}: code");
                    assert_eq!(re.message(), ge.message(), "push {i}: message");
                    let gd = grpcutilpb::extract_cortex_error_details(&ge);
                    let rd = grpcutilpb::extract_cortex_error_details(&re);
                    assert_eq!(
                        rd.map(|d| (d.cause, d.soft)),
                        gd.map(|d| (d.cause, d.soft)),
                        "push {i}: ErrorDetails"
                    );
                }
                (g, r) => panic!("push {i}: Go {g:?} vs Rust {r:?}"),
            }
        }

        // Let both run at least two active series update periods (1s).
        tokio::time::sleep(Duration::from_secs(4)).await;

        let get = |port: u16, path: &'static str| {
            let http = http.clone();
            async move {
                let r = http
                    .get(format!("http://127.0.0.1:{port}{path}"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(r.status().as_u16(), 200, "GET {path} on {port}");
                r.text().await.unwrap()
            }
        };

        let metric_names = [
            "cortex_discarded_samples_total",
            "cortex_ingester_active_series",
            "cortex_ingester_active_series_custom_tracker",
            "cortex_ingester_active_native_histogram_series",
            "cortex_ingester_active_native_histogram_series_custom_tracker",
            "cortex_ingester_active_native_histogram_buckets",
            "cortex_ingester_active_native_histogram_buckets_custom_tracker",
            "cortex_cost_attribution_active_series_tracker_cardinality",
            "cortex_cost_attribution_sample_tracker_cardinality",
            "cortex_cost_attribution_active_series_tracker_overflown",
            "cortex_cost_attribution_sample_tracker_overflown",
            "cortex_cost_attribution_tracker_creation_errors_total",
        ];
        let go_m = families(&get(go_http, "/metrics").await, &metric_names);
        let rust_m = families(&get(rust_http, "/metrics").await, &metric_names);
        assert!(
            go_m.iter().any(|l| l.contains("group=\"g1\"")),
            "Go should report group label: {go_m:#?}"
        );
        eprintln!("compared /metrics series:\n{go_m:#?}");
        assert_eq!(rust_m, go_m, "/metrics families differ");

        let usage_names = [
            "cortex_ingester_attributed_active_series",
            "cortex_ingester_attributed_active_native_histogram_series",
            "cortex_ingester_attributed_active_native_histogram_buckets",
            "cortex_attributed_series_overflow_labels",
            "cortex_discarded_attributed_samples_total",
            "cortex_distributor_received_attributed_samples_total",
        ];
        let go_u = families(&get(go_http, "/usage-metrics").await, &usage_names);
        let rust_u = families(&get(rust_http, "/usage-metrics").await, &usage_names);
        assert!(!go_u.is_empty(), "Go /usage-metrics is empty");
        eprintln!("compared /usage-metrics series:\n{go_u:#?}");
        assert_eq!(rust_u, go_u, "/usage-metrics differ");
    });
}
