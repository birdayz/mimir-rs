//! Wave 16 regression tests:
//! - OEM-04: Per-tenant metadata store created on every push (including metadata-less pushes).
//! - W16-APC-02: Non-legacy UTF-8 label & metric names quoted/escaped in Prometheus Text & OpenMetrics.
//! - W16-APC-07: `GET /ingester/tsdb_metrics` respects `-tenant-federation.enabled` / `max-tenants`.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use mimir_common::services::State;
use mimir_config::limits::CostAttributionLabel;
use mimir_config::{
    BlocksStorageConfig, CostAttributionTrackerConfig, GoDuration, IngestStorageConfig,
    IngesterConfig, LimitsConfig, Overrides,
};
use mimir_ingester::costattribution::Manager as CostAttributionManager;
use mimir_ingester::expfmt::{encode_openmetrics, encode_text};
use mimir_ingester::{ingester_router, Ingester};
use mimir_proto::cortexpb;
use mimir_proto::prost::Message as _;
use mimir_proto::zerocopy::{decode_write_request, DecodeOptions, WriteRequestScratch};
use mimir_tsdb::labels::{Label, Labels};
use prometheus::proto::{
    Bucket, Counter, Gauge, Histogram, LabelPair, Metric, MetricFamily, MetricType, Quantile,
    Summary,
};
use prometheus::Registry;
use tower::ServiceExt as _;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unix epoch")
        .as_millis() as i64
}

fn new_test_ingester(dir: &std::path::Path, reg: Option<&Registry>) -> Arc<Ingester> {
    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_id = "test-0".to_string();
    cfg.ring.instance_addr = "127.0.0.1".to_string();
    cfg.active_series.active_series_metrics_enabled = false;
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.to_string_lossy().into_owned();
    blocks_cfg.tsdb.ship_interval = GoDuration(0);
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        Overrides::new(LimitsConfig::default()),
        None,
        None,
        None,
        None,
        None,
        reg,
    )
    .expect("create ingester");
    ing.set_state_for_test(State::Running);
    ing
}

fn lp(name: &str, value: &str) -> cortexpb::LabelPair {
    cortexpb::LabelPair {
        name: Bytes::copy_from_slice(name.as_bytes()),
        value: Bytes::copy_from_slice(value.as_bytes()),
    }
}

fn sample_write_request(metric: &str, ts: i64) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![lp("__name__", metric), lp("job", "app")],
            samples: vec![cortexpb::Sample {
                timestamp_ms: ts,
                value: 42.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn find_counter_for_user(reg: &Registry, metric_name: &str, user: &str) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() != metric_name {
            continue;
        }
        for m in mf.get_metric() {
            if m.get_label()
                .iter()
                .any(|l| l.get_name() == "user" && l.get_value() == user)
            {
                return Some(m.get_counter().get_value());
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------------------------
// OEM-04: Per-tenant metadata store created on metadata-less pushes
// ---------------------------------------------------------------------------------------------

#[test]
fn oem04_metadata_store_created_on_metadata_less_owned_push() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let reg = Registry::new();
    let ing = new_test_ingester(tmp.path(), Some(&reg));

    let req = sample_write_request("up", now_ms());
    ing.push_write_request_sync("tenant-owned", &req)
        .expect("push owned");

    assert!(
        ing.get_user_metadata("tenant-owned").is_some(),
        "metadata store must be created even when push has no metadata"
    );
    ing.purge_user_metrics_metadata();
    assert_eq!(
        find_counter_for_user(
            &reg,
            "cortex_ingester_memory_metadata_removed_total",
            "tenant-owned"
        ),
        Some(0.0)
    );
}

#[test]
fn oem04_metadata_store_created_on_metadata_less_borrowed_push() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let reg = Registry::new();
    let ing = new_test_ingester(tmp.path(), Some(&reg));

    // Encode an RW2 WriteRequest with samples and NO metadata.
    let rw2 = cortexpb::WriteRequestRw2 {
        symbols: vec![
            "".to_string(),
            "__name__".to_string(),
            "up_rw2".to_string(),
            "job".to_string(),
            "app".to_string(),
        ],
        timeseries: vec![cortexpb::TimeSeriesRw2 {
            labels_refs: vec![1, 2, 3, 4],
            samples: vec![cortexpb::Sample {
                timestamp_ms: now_ms(),
                value: 7.0,
            }],
            ..Default::default()
        }],
    };
    let wire = rw2.encode_to_vec();
    let mut scratch = WriteRequestScratch::new();
    let borrowed =
        decode_write_request(&wire, DecodeOptions::rw2(), &mut scratch).expect("decode rw2");
    ing.push_borrowed_to_storage("tenant-borrowed", &borrowed, wire.len() as i64)
        .expect("push borrowed");

    assert!(ing.get_user_metadata("tenant-borrowed").is_some());
    ing.purge_user_metrics_metadata();
    assert_eq!(
        find_counter_for_user(
            &reg,
            "cortex_ingester_memory_metadata_removed_total",
            "tenant-borrowed"
        ),
        Some(0.0)
    );
}

#[tokio::test]
async fn oem04_metadata_store_created_on_metadata_less_classic_grpc_raw_push() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let reg = Registry::new();
    let ing = new_test_ingester(tmp.path(), Some(&reg));

    let req = sample_write_request("up_raw", now_ms());
    ing.push_grpc_raw("tenant-raw", Bytes::from(req.encode_to_vec()))
        .await
        .expect("push_grpc_raw");

    assert!(ing.get_user_metadata("tenant-raw").is_some());
    ing.purge_user_metrics_metadata();
    assert_eq!(
        find_counter_for_user(
            &reg,
            "cortex_ingester_memory_metadata_removed_total",
            "tenant-raw"
        ),
        Some(0.0)
    );
}

// ---------------------------------------------------------------------------------------------
// W16-APC-02: UTF-8 metric & label name quoting/escaping in expfmt Text 0.0.4 & OpenMetrics
// ---------------------------------------------------------------------------------------------

fn make_lp(name: &str, value: &str) -> LabelPair {
    let mut lp = LabelPair::default();
    lp.set_name(name.to_string());
    lp.set_value(value.to_string());
    lp
}

#[test]
fn apc02_expfmt_quotes_and_escapes_non_legacy_label_and_metric_names() {
    // 1. Legacy metric name + UTF-8 & escaped label names.
    let mut mf1 = MetricFamily::default();
    mf1.set_name("cortex_ingester_attributed_active_series".to_string());
    mf1.set_help("Attributed active series".to_string());
    mf1.set_field_type(MetricType::GAUGE);
    let mut m1 = Metric::default();
    m1.set_label(
        vec![
            make_lp("tenant", "user-1"),
            make_lp("k8s.namespace.name", "prod"),
            make_lp("team:name", "obs"),
            make_lp("weird\"\\name\nhere", "v"),
        ]
        .into(),
    );
    let mut g1 = Gauge::default();
    g1.set_value(5.0);
    m1.set_gauge(g1);
    mf1.mut_metric().push(m1);

    // 2. Non-legacy metric name with NO labels -> {"metric.name"} 2
    let mut mf2 = MetricFamily::default();
    mf2.set_name("metric.no.labels".to_string());
    mf2.set_help("Help text".to_string());
    mf2.set_field_type(MetricType::GAUGE);
    let mut m2 = Metric::default();
    let mut g2 = Gauge::default();
    g2.set_value(2.0);
    m2.set_gauge(g2);
    mf2.mut_metric().push(m2);

    // 3. Non-legacy counter _total WITH labels -> {"http.requests_total",label="val","k8s.ns"="prod"} 3
    let mut mf3 = MetricFamily::default();
    mf3.set_name("http.requests_total".to_string());
    mf3.set_help("Req total".to_string());
    mf3.set_field_type(MetricType::COUNTER);
    let mut m3 = Metric::default();
    m3.set_label(vec![make_lp("label", "val"), make_lp("k8s.ns", "prod")].into());
    let mut c3 = Counter::default();
    c3.set_value(3.0);
    m3.set_counter(c3);
    mf3.mut_metric().push(m3);

    // 4. Non-legacy histogram -> {"req.duration_bucket",le="0.5"} etc.
    let mut mf4 = MetricFamily::default();
    mf4.set_name("req.duration".to_string());
    mf4.set_help("Duration".to_string());
    mf4.set_field_type(MetricType::HISTOGRAM);
    let mut m4 = Metric::default();
    let mut h4 = Histogram::default();
    let mut b = Bucket::default();
    b.set_upper_bound(0.5);
    b.set_cumulative_count(1);
    h4.set_bucket(vec![b].into());
    h4.set_sample_count(1);
    h4.set_sample_sum(0.25);
    m4.set_histogram(h4);
    mf4.mut_metric().push(m4);

    // 5. Non-legacy summary -> {"rpc.latency",quantile="0.5"} etc.
    let mut mf5 = MetricFamily::default();
    mf5.set_name("rpc.latency".to_string());
    mf5.set_help("Latency".to_string());
    mf5.set_field_type(MetricType::SUMMARY);
    let mut m5 = Metric::default();
    let mut s5 = Summary::default();
    let mut q = Quantile::default();
    q.set_quantile(0.5);
    q.set_value(0.1);
    s5.set_quantile(vec![q].into());
    s5.set_sample_count(1);
    s5.set_sample_sum(0.1);
    m5.set_summary(s5);
    mf5.mut_metric().push(m5);

    let families = vec![mf1, mf2, mf3, mf4, mf5];
    let text = String::from_utf8(encode_text(&families)).expect("utf8");
    let expected_text = concat!(
        "# HELP cortex_ingester_attributed_active_series Attributed active series\n",
        "# TYPE cortex_ingester_attributed_active_series gauge\n",
        "cortex_ingester_attributed_active_series{\"k8s.namespace.name\"=\"prod\",\"team:name\"=\"obs\",tenant=\"user-1\",\"weird\\\"\\\\name\\nhere\"=\"v\"} 5\n",
        "# HELP \"metric.no.labels\" Help text\n",
        "# TYPE \"metric.no.labels\" gauge\n",
        "{\"metric.no.labels\"} 2\n",
        "# HELP \"http.requests_total\" Req total\n",
        "# TYPE \"http.requests_total\" counter\n",
        "{\"http.requests_total\",\"k8s.ns\"=\"prod\",label=\"val\"} 3\n",
        "# HELP \"req.duration\" Duration\n",
        "# TYPE \"req.duration\" histogram\n",
        "{\"req.duration_bucket\",le=\"0.5\"} 1\n",
        "{\"req.duration_bucket\",le=\"+Inf\"} 1\n",
        "{\"req.duration_sum\"} 0.25\n",
        "{\"req.duration_count\"} 1\n",
        "# HELP \"rpc.latency\" Latency\n",
        "# TYPE \"rpc.latency\" summary\n",
        "{\"rpc.latency\",quantile=\"0.5\"} 0.1\n",
        "{\"rpc.latency_sum\"} 0.1\n",
        "{\"rpc.latency_count\"} 1\n",
    );
    assert_eq!(text, expected_text);

    let om = String::from_utf8(encode_openmetrics(&families)).expect("utf8");
    let expected_om = concat!(
        "# HELP cortex_ingester_attributed_active_series Attributed active series\n",
        "# TYPE cortex_ingester_attributed_active_series gauge\n",
        "cortex_ingester_attributed_active_series{\"k8s.namespace.name\"=\"prod\",\"team:name\"=\"obs\",tenant=\"user-1\",\"weird\\\"\\\\name\\nhere\"=\"v\"} 5.0\n",
        "# HELP \"metric.no.labels\" Help text\n",
        "# TYPE \"metric.no.labels\" gauge\n",
        "{\"metric.no.labels\"} 2.0\n",
        "# HELP \"http.requests\" Req total\n",
        "# TYPE \"http.requests\" counter\n",
        "{\"http.requests_total\",\"k8s.ns\"=\"prod\",label=\"val\"} 3.0\n",
        "# HELP \"req.duration\" Duration\n",
        "# TYPE \"req.duration\" histogram\n",
        "{\"req.duration_bucket\",le=\"0.5\"} 1\n",
        "{\"req.duration_bucket\",le=\"+Inf\"} 1\n",
        "{\"req.duration_sum\"} 0.25\n",
        "{\"req.duration_count\"} 1\n",
        "# HELP \"rpc.latency\" Latency\n",
        "# TYPE \"rpc.latency\" summary\n",
        "{\"rpc.latency\",quantile=\"0.5\"} 0.1\n",
        "{\"rpc.latency_sum\"} 0.1\n",
        "{\"rpc.latency_count\"} 1\n",
        "# EOF\n",
    );
    assert_eq!(om, expected_om);
}

#[test]
fn apc02_cost_attribution_utf8_output_label_is_quoted_in_expfmt() {
    let mut default_limits = LimitsConfig::default();
    default_limits.cost_attribution_trackers.insert(
        "by-ns".to_string(),
        CostAttributionTrackerConfig {
            labels: vec![CostAttributionLabel {
                input: "k8s.namespace.name".to_string(),
                output: String::new(),
            }],
            internal: false,
        },
    );
    let overrides = Overrides::new(default_limits);
    let ca_reg = Registry::new();
    let mgr = CostAttributionManager::new(
        std::time::Duration::from_secs(60),
        overrides,
        None,
        &ca_reg,
    )
    .expect("create cost attribution manager");

    let tracker = mgr
        .active_series_tracker("user-1")
        .expect("active series tracker");
    let lbls = Labels::from_labels(vec![
        Label::new("__name__", "up"),
        Label::new("k8s.namespace.name", "prod"),
    ]);
    tracker.increment(&lbls, SystemTime::now(), 0);

    let families = ca_reg.gather();
    let text = String::from_utf8(encode_text(&families)).expect("utf8");
    assert!(
        text.contains(
            "cortex_ingester_attributed_active_series{\"k8s.namespace.name\"=\"prod\",tenant=\"user-1\",tracker=\"by-ns\"} 1\n"
        ),
        "expected quoted UTF-8 label in text output, got:\n{text}"
    );

    let om = String::from_utf8(encode_openmetrics(&families)).expect("utf8");
    assert!(
        om.contains(
            "cortex_ingester_attributed_active_series{\"k8s.namespace.name\"=\"prod\",tenant=\"user-1\",tracker=\"by-ns\"} 1.0\n"
        ),
        "expected quoted UTF-8 label in OpenMetrics output, got:\n{om}"
    );
}

// ---------------------------------------------------------------------------------------------
// W16-APC-07: GET /ingester/tsdb_metrics respects -tenant-federation.enabled & max-tenants
// ---------------------------------------------------------------------------------------------

async fn get_tsdb_metrics(
    app: &axum::Router,
    org_id: Option<&str>,
) -> (StatusCode, String) {
    let mut builder = Request::builder().uri("/ingester/tsdb_metrics");
    if let Some(id) = org_id {
        builder = builder.header("X-Scope-OrgID", id);
    }
    let resp = app
        .clone()
        .oneshot(builder.body(Body::empty()).expect("body"))
        .await
        .expect("oneshot");
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    (status, String::from_utf8(bytes.to_vec()).expect("utf8"))
}

#[tokio::test]
async fn apc07_tsdb_metrics_respects_tenant_federation_config() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let ing = new_test_ingester(tmp.path(), None);
    let _ = ing.get_or_create_tsdb("tenant-a").expect("tsdb a");
    let app = ingester_router(Arc::clone(&ing));

    // 1. Default (federation disabled): multiple tenants -> 422 max: 1 actual: 2.
    assert_eq!(
        get_tsdb_metrics(&app, Some("tenant-a|tenant-b")).await,
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "too many tenant IDs present in the request. max: 1 actual: 2\n".to_string()
        )
    );

    // 2. Federation enabled, max_tenants = 0 (unlimited):
    //    middleware lets multiple tenants through, UserRegistryHandler rejects via TenantID -> 401.
    ing.set_tenant_federation(true, 0);
    assert_eq!(
        get_tsdb_metrics(&app, Some("tenant-a|tenant-b")).await,
        (
            StatusCode::UNAUTHORIZED,
            "multiple org IDs present\n".to_string()
        )
    );

    // 3. Federation enabled, max_tenants = 2:
    //    - 2 tenants -> passes middleware -> 401 multiple org IDs present.
    //    - 3 tenants -> rejected by middleware -> 422 max: 2 actual: 3.
    //    - 1 tenant -> 200 OK.
    ing.set_tenant_federation(true, 2);
    assert_eq!(
        get_tsdb_metrics(&app, Some("tenant-a|tenant-b")).await,
        (
            StatusCode::UNAUTHORIZED,
            "multiple org IDs present\n".to_string()
        )
    );
    assert_eq!(
        get_tsdb_metrics(&app, Some("tenant-a|tenant-b|tenant-c")).await,
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "too many tenant IDs present in the request. max: 2 actual: 3\n".to_string()
        )
    );
    let (status, body) = get_tsdb_metrics(&app, Some("tenant-a")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("prometheus_tsdb_head_series"));

    // 4. Invalid tenant ID still returns 401 from middleware.
    let (status, body) = get_tsdb_metrics(&app, Some("tenant-a|bad/id")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body.contains("unsupported character"));

    // 5. Missing header -> 401 no org id.
    assert_eq!(
        get_tsdb_metrics(&app, None).await,
        (StatusCode::UNAUTHORIZED, "no org id\n".to_string())
    );

    // 6. Multitenancy disabled ignores X-Scope-OrgID even when federation is enabled.
    ing.set_multitenancy(false, "tenant-a");
    let (status, _) = get_tsdb_metrics(&app, Some("x|y|z")).await;
    assert_eq!(status, StatusCode::OK);
}
