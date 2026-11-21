//! Wave 23 regression tests for HTTP 405 MethodMismatch middleware ordering
//! (inflight metrics, 511 cluster validation, and OTel span name / http.route).

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};

use axum::body::Body;
use http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::{ModuleStates, StateCell};
use mimir_common::services::State;
use mimir_cmd::server::http_middleware::{
    dskit_http_middleware, HttpClusterValidation, HttpMiddlewareConfig,
};
use mimir_cmd::server::http_server::ServedOverHttp;
use mimir_cmd::server::metrics::ServerMetrics;
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::tracing::{init_otel_or_jaeger_from_env, ExportedSpan};
use mimir_config::{MimirConfig, Overrides};
use parking_lot::Mutex;
use prometheus::Registry;
use tower::ServiceExt as _;

#[derive(Default)]
struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
}

static RECORDED_SPANS: OnceLock<Arc<Mutex<Vec<ExportedSpan>>>> = OnceLock::new();

fn init_test_tracing() -> &'static Arc<Mutex<Vec<ExportedSpan>>> {
    let spans = RECORDED_SPANS.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
    let prev_exp = std::env::var("OTEL_TRACES_EXPORTER").ok();
    std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
    let sink = Arc::clone(spans);
    let _ = init_otel_or_jaeger_from_env(
        "mimir-w23-test",
        &NoopLogger,
        Some(Arc::new(move |span: ExportedSpan| {
            sink.lock().push(span);
        })),
    );
    match prev_exp {
        Some(v) => std::env::set_var("OTEL_TRACES_EXPORTER", v),
        None => std::env::remove_var("OTEL_TRACES_EXPORTER"),
    }
    spans
}

#[tokio::test]
async fn w23_csc_11_otel_span_511_and_metrics_on_method_mismatch() {
    let spans = init_test_tracing();
    spans.lock().clear();

    let mut cfg = MimirConfig::default();
    cfg.server.http_path_prefix = "/mimir".to_string();
    let cfg_arc = Arc::new(cfg);

    let modules = ModuleStates::new();
    let cell = StateCell::new(State::Running);
    modules.add("server", cell.state_fn());

    let router = build_http_router(AppHttpState {
        config: Arc::clone(&cfg_arc),
        default_config: Arc::new(MimirConfig::default()),
        registry: Registry::new(),
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: None,
        runtime_config: None,
        overrides: Overrides::new(cfg_arc.limits.clone()),
        memberlist_kv: None,
        modules,
        cost_attribution_registry: None,
    });

    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let cv = HttpClusterValidation::new(vec!["prod".to_string()], false, &[], &[]).unwrap();
    let mw_cfg = Arc::new(HttpMiddlewareConfig {
        metrics,
        logger: Arc::new(NoopLogger),
        log_request_headers: false,
        log_request_at_info_level: false,
        log_source_ips: None,
        trace_source_ips: None,
        exclude_headers: Vec::new(),
        trace_request_headers: false,
        trace_exclude_headers: Vec::new(),
        create_new_traces: false,
        cluster_validation: Some(cv),
        http_path_prefix: "/mimir".to_string(),
        cost_attribution_path: None,
        compartments_enabled: false,
    });

    let app = router.layer(axum::middleware::from_fn_with_state(
        mw_cfg,
        dskit_http_middleware,
    ));

    // 1. Unsupported DELETE /mimir/config with valid X-Cluster -> 405, OTel span "HTTP DELETE" with no http.route.
    let mut req1 = Request::builder()
        .method(Method::DELETE)
        .uri("/mimir/config")
        .header("X-Cluster", "prod")
        .header(
            "traceparent",
            "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01",
        )
        .body(Body::empty())
        .unwrap();
    req1.extensions_mut().insert(ServedOverHttp {
        remote_addr: "127.0.0.1:9999".parse().unwrap(),
    });
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::METHOD_NOT_ALLOWED);
    let _ = resp1.into_body().collect().await.unwrap();

    // 2. Unsupported POST /mimir/services with missing X-Cluster -> 511 JSON with "<unknown-route>".
    let mut req2 = Request::builder()
        .method(Method::POST)
        .uri("/mimir/services")
        .header(
            "traceparent",
            "00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2222222222222222-01",
        )
        .body(Body::empty())
        .unwrap();
    req2.extensions_mut().insert(ServedOverHttp {
        remote_addr: "127.0.0.1:9999".parse().unwrap(),
    });
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::NETWORK_AUTHENTICATION_REQUIRED);
    let body2 = String::from_utf8(
        resp2
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let json2: serde_json::Value = serde_json::from_str(&body2).unwrap();
    assert_eq!(json2["route"], "<unknown-route>");

    // 3. Supported GET /mimir/config with valid X-Cluster -> 200, OTel span "HTTP GET - mimir_config" with http.route.
    let mut req3 = Request::builder()
        .method(Method::GET)
        .uri("/mimir/config")
        .header("X-Cluster", "prod")
        .header(
            "traceparent",
            "00-cccccccccccccccccccccccccccccccc-3333333333333333-01",
        )
        .body(Body::empty())
        .unwrap();
    req3.extensions_mut().insert(ServedOverHttp {
        remote_addr: "127.0.0.1:9999".parse().unwrap(),
    });
    let resp3 = app.oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::OK);
    let _ = resp3.into_body().collect().await.unwrap();

    let exported = spans.lock().clone();
    let s1 = exported
        .iter()
        .find(|s| s.trace_id[0] == 0xaa)
        .expect("DELETE span");
    assert_eq!(s1.name, "HTTP DELETE");
    assert!(!s1.attributes.iter().any(|a| a.key == "http.route"));

    let s2 = exported
        .iter()
        .find(|s| s.trace_id[0] == 0xbb)
        .expect("HEAD span");
    assert_eq!(s2.name, "HTTP POST");
    assert!(!s2.attributes.iter().any(|a| a.key == "http.route"));

    let s3 = exported
        .iter()
        .find(|s| s.trace_id[0] == 0xcc)
        .expect("GET span");
    assert_eq!(s3.name, "HTTP GET - mimir_config");
    assert!(s3.attributes.iter().any(|a| a.key == "http.route"));

    // Verify no bogus route="mimir_services" or route="mimir_config" on 405/511 metrics.
    for mf in reg.gather() {
        if mf.get_name() == "cortex_inflight_requests"
            || mf.get_name() == "cortex_request_duration_seconds"
        {
            for m in mf.get_metric() {
                let method = m
                    .get_label()
                    .iter()
                    .find(|l| l.get_name() == "method")
                    .map(|l| l.get_value())
                    .unwrap_or("");
                let route = m
                    .get_label()
                    .iter()
                    .find(|l| l.get_name() == "route")
                    .map(|l| l.get_value())
                    .unwrap_or("");
                if method == "DELETE" || method == "POST" {
                    assert_eq!(route, "other", "unexpected route in {}", mf.get_name());
                }
            }
        }
    }
}

#[tokio::test]
async fn w24_f8_f9_compartments_partition_ring_post_and_prefixed_root_head_405() {
    use mimir_ingester::Ingester;
    use mimir_ring::kv::inmemory::InMemoryKv;
    use mimir_ring::kv::{KvClient, ProtoSnappyCodec};
    use mimir_ring::partition_ring::{
        PartitionRingOptions, PartitionRingWatcher, PartitionRingWatchers,
    };

    let spans = init_test_tracing();
    spans.lock().clear();

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = MimirConfig::default();
    cfg.server.http_path_prefix = "/mimir".to_string();
    cfg.compartments.enabled = true;
    cfg.ingester.ring.instance_id = "test-0".to_string();
    cfg.blocks_storage.tsdb.dir = tmp.path().to_string_lossy().into_owned();
    let cfg_arc = Arc::new(cfg);

    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(Arc::new(
        ProtoSnappyCodec::<mimir_proto::ringpb::PartitionRingDesc>::new("partitionRingDesc"),
    )));
    let watcher = PartitionRingWatcher::new(
        "ingester-partitions-rc-0",
        "ingester-partitions-rc-0",
        kv,
        PartitionRingOptions::default(),
        None,
    );
    let watchers = PartitionRingWatchers::new(vec![Arc::clone(&watcher)]).unwrap();

    let ing = Ingester::new(
        cfg_arc.ingester.clone(),
        cfg_arc.blocks_storage.clone(),
        cfg_arc.ingest_storage.clone(),
        Overrides::new(cfg_arc.limits.clone()),
        None,
        None,
        Some(watcher),
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_compartments(cfg_arc.compartments.clone());
    ing.set_partition_ring_watchers(Some(watchers));

    let modules = ModuleStates::new();
    let cell = StateCell::new(State::Running);
    modules.add("server", cell.state_fn());

    let router = build_http_router(AppHttpState {
        config: Arc::clone(&cfg_arc),
        default_config: Arc::new(MimirConfig::default()),
        registry: Registry::new(),
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: Some(ing),
        runtime_config: None,
        overrides: Overrides::new(cfg_arc.limits.clone()),
        memberlist_kv: None,
        modules,
        cost_attribution_registry: None,
    });

    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let mw_cfg = Arc::new(HttpMiddlewareConfig {
        metrics,
        logger: Arc::new(NoopLogger),
        log_request_headers: false,
        log_request_at_info_level: false,
        log_source_ips: None,
        trace_source_ips: None,
        exclude_headers: Vec::new(),
        trace_request_headers: false,
        trace_exclude_headers: Vec::new(),
        create_new_traces: false,
        cluster_validation: None,
        http_path_prefix: "/mimir".to_string(),
        cost_attribution_path: None,
        compartments_enabled: true,
    });
    let app = router.layer(axum::middleware::from_fn_with_state(
        mw_cfg,
        dskit_http_middleware,
    ));

    // F8: POST /mimir/ingester/partition-ring when compartments.enabled == true -> 405, route="other", no http.route span attr.
    let mut req_f8 = Request::builder()
        .method(Method::POST)
        .uri("/mimir/ingester/partition-ring")
        .header(
            "traceparent",
            "00-dddddddddddddddddddddddddddddddd-4444444444444444-01",
        )
        .body(Body::empty())
        .unwrap();
    req_f8.extensions_mut().insert(ServedOverHttp {
        remote_addr: "127.0.0.1:9999".parse().unwrap(),
    });
    let resp_f8 = app.clone().oneshot(req_f8).await.unwrap();
    assert_eq!(resp_f8.status(), StatusCode::METHOD_NOT_ALLOWED);
    let _ = resp_f8.into_body().collect().await.unwrap();

    // F9: HEAD /mimir/ -> 405, empty body, no Allow header, route="other", no http.route span attr.
    let mut req_f9 = Request::builder()
        .method(Method::HEAD)
        .uri("/mimir/")
        .header(
            "traceparent",
            "00-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-5555555555555555-01",
        )
        .body(Body::empty())
        .unwrap();
    req_f9.extensions_mut().insert(ServedOverHttp {
        remote_addr: "127.0.0.1:9999".parse().unwrap(),
    });
    let resp_f9 = app.oneshot(req_f9).await.unwrap();
    assert_eq!(resp_f9.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(!resp_f9.headers().contains_key(http::header::ALLOW));
    let body_f9 = resp_f9.into_body().collect().await.unwrap().to_bytes();
    assert!(body_f9.is_empty());

    let exported = spans.lock().clone();
    let s_f8 = exported
        .iter()
        .find(|s| s.trace_id[0] == 0xdd)
        .expect("F8 span");
    assert_eq!(s_f8.name, "HTTP POST");
    assert!(!s_f8.attributes.iter().any(|a| a.key == "http.route"));

    let s_f9 = exported
        .iter()
        .find(|s| s.trace_id[0] == 0xee)
        .expect("F9 span");
    assert_eq!(s_f9.name, "HTTP HEAD");
    assert!(!s_f9.attributes.iter().any(|a| a.key == "http.route"));

    for mf in reg.gather() {
        if mf.get_name() == "cortex_request_duration_seconds" {
            for m in mf.get_metric() {
                let route = m
                    .get_label()
                    .iter()
                    .find(|l| l.get_name() == "route")
                    .map(|l| l.get_value())
                    .unwrap_or("");
                assert_eq!(route, "other", "unexpected route label in {}", mf.get_name());
            }
        }
    }
}
