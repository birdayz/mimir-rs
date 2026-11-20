//! Regression tests for Wave 19 Track 3 & cmd-side Track 2 fixes:
//! - W19-RKN-03: `?name[]=` filtering & multi-value `Accept-Encoding` negotiation
//! - W19-RKN-04: `gorilla/mux` `cleanPath` 301 redirects, `#fragment`, span & shutdown ordering
//! - W19-RKN-05: UTF-8 response headers/`X-ErrorMessage`/`User-Agent` & control-byte `X-Scope-OrgID`
//! - W19-RKN-08: gRPC server span `status.code = 2` & parent `httpgrpc.HTTP/Handle` HTTP attributes
//! - W19-RKN-10: OTLP headers/TLS/mTLS/timeout & `JAEGER_ENDPOINT` (+ Basic Auth)
//! - W19-MOD-03: `-activity-tracker.max-entries <= 0`, lifecycle logs, & `sanity_err` cleanup
//! - W19-MOD-04: strict `usagestats::parse_rfc3339` & `usagestats::set_mode` reset

use std::io::Read as _;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use flate2::read::GzDecoder;
use http_body_util::BodyExt;
use mimir_cmd::app::{
    build_span_exporter, run_mimir, span_to_otlp_json, validate_activity_tracker_max_entries,
};
use mimir_cmd::grpc::{HttpGrpcBridge, HttpGrpcStatusFixService};
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::ModuleStates;
use mimir_cmd::parse_cli_for_run;
use mimir_cmd::server::Server;
use mimir_cmd::usagestats::{
    init_seed_file, parse_rfc3339, read_seed_file, CLUSTER_SEED_FILE_NAME,
};
use mimir_common::activitytracker::{ActivityTracker, ActivityTrackerConfig};
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::services::{ServiceContext, State};
use mimir_common::tracing::{
    detected_exporter_kind, init_otel_or_jaeger_from_env, jaeger_endpoint_from_env,
    otlp_headers_from_env, otlp_timeout_from_env, otlp_tls_paths_from_env, parse_otlp_headers_str,
    ExportedSpan, SpanAttrValue,
};
use mimir_common::usagestats as common_usagestats;
use mimir_config::{
    BlocksStorageConfig, IngestStorageConfig, IngesterConfig, LimitsConfig, MimirConfig, Overrides,
    ServerConfig,
};
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::httpgrpcpb::http_server::{Http, HttpServer};
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest};
use mimir_storage::bucket::BucketClient;
use prometheus::{IntCounter, Opts, Registry};
use tempfile::tempdir;
use tower::ServiceExt;

static ENV_LOCK: Mutex<()> = Mutex::new(());
static RECORDED_SPANS: OnceLock<Arc<Mutex<Vec<ExportedSpan>>>> = OnceLock::new();

#[derive(Default)]
struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
}

fn ensure_recording_tracing() -> &'static Arc<Mutex<Vec<ExportedSpan>>> {
    let spans = RECORDED_SPANS.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let prev_exp = std::env::var("OTEL_TRACES_EXPORTER").ok();
    let prev_prop = std::env::var("OTEL_PROPAGATORS").ok();
    let prev_samp = std::env::var("OTEL_TRACES_SAMPLER").ok();
    std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
    std::env::set_var("OTEL_PROPAGATORS", "jaeger,tracecontext");
    std::env::set_var("OTEL_TRACES_SAMPLER", "parentbased_always_on");
    let sink = Arc::clone(spans);
    let _ = init_otel_or_jaeger_from_env(
        "mimir-ingester",
        &NoopLogger,
        Some(Arc::new(move |span| {
            sink.lock().unwrap_or_else(|e| e.into_inner()).push(span);
        })),
    );
    match prev_exp {
        Some(v) => std::env::set_var("OTEL_TRACES_EXPORTER", v),
        None => std::env::remove_var("OTEL_TRACES_EXPORTER"),
    }
    match prev_prop {
        Some(v) => std::env::set_var("OTEL_PROPAGATORS", v),
        None => std::env::remove_var("OTEL_PROPAGATORS"),
    }
    match prev_samp {
        Some(v) => std::env::set_var("OTEL_TRACES_SAMPLER", v),
        None => std::env::remove_var("OTEL_TRACES_SAMPLER"),
    }
    spans
}

fn make_test_http_state(
    reg: Registry,
    cost_reg: Option<(String, Registry)>,
    ingester: Option<Arc<Ingester>>,
    shutdown: Arc<AtomicBool>,
) -> AppHttpState {
    let cfg = MimirConfig::default();
    AppHttpState {
        config: Arc::new(cfg.clone()),
        default_config: Arc::new(cfg),
        registry: reg,
        shutdown_requested: shutdown,
        ingester,
        runtime_config: None,
        overrides: Overrides::new(LimitsConfig::default()),
        memberlist_kv: None,
        modules: ModuleStates::new(),
        cost_attribution_registry: cost_reg,
    }
}

fn make_test_ingester(dir: &std::path::Path, multitenancy: bool) -> Arc<Ingester> {
    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_id = "w19-ing-0".to_string();
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.to_string_lossy().into_owned();
    let ing =
        Ingester::new(
            cfg,
            blocks_cfg,
            IngestStorageConfig::default(),
            Overrides::new(LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    ing.set_multitenancy(multitenancy, "anonymous".to_string());
    ing.set_state_for_test(State::Running);
    ing
}

fn gunzip(bytes: &[u8]) -> String {
    let mut dec = GzDecoder::new(bytes);
    let mut s = String::new();
    dec.read_to_string(&mut s).unwrap();
    s
}

#[tokio::test]
async fn w19_rkn_03_metrics_name_filter_and_multi_accept_encoding() {
    let tmp = tempdir().unwrap();
    let reg = Registry::new();
    let c_alpha = IntCounter::with_opts(Opts::new("w19_alpha_total", "alpha help")).unwrap();
    let c_beta = IntCounter::with_opts(Opts::new("w19_beta_total", "beta help")).unwrap();
    let c_gamma = IntCounter::with_opts(Opts::new("w19_gamma_total", "gamma help")).unwrap();
    c_alpha.inc_by(3);
    c_beta.inc_by(5);
    c_gamma.inc_by(7);
    reg.register(Box::new(c_alpha)).unwrap();
    reg.register(Box::new(c_beta)).unwrap();
    reg.register(Box::new(c_gamma)).unwrap();

    let cost_reg = Registry::new();
    let cost_a = IntCounter::with_opts(Opts::new("cost_alpha_total", "cost a")).unwrap();
    let cost_b = IntCounter::with_opts(Opts::new("cost_beta_total", "cost b")).unwrap();
    cost_a.inc_by(11);
    cost_b.inc_by(13);
    cost_reg.register(Box::new(cost_a)).unwrap();
    cost_reg.register(Box::new(cost_b)).unwrap();

    let ing = make_test_ingester(tmp.path(), true);
    let wr = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: Bytes::from_static(b"__name__"),
                value: Bytes::from_static(b"w19_tsdb_metric"),
            }],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: 1_700_000_000_000,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    ing.push_grpc("tenant-a", &wr).await.unwrap();

    let router = build_http_router(make_test_http_state(
        reg,
        Some(("/cost_attribution".to_string(), cost_reg)),
        Some(Arc::clone(&ing)),
        Arc::new(AtomicBool::new(false)),
    ));

    // 1. Direct HTTP /metrics?name[]=...&name%5B%5D=... with two Accept-Encoding headers.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics?name[]=w19_alpha_total&name%5B%5D=w19_gamma_total")
        .header(header::ACCEPT_ENCODING, "deflate")
        .header(header::ACCEPT_ENCODING, "gzip")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok()),
        Some("gzip")
    );
    let body = gunzip(&resp.into_body().collect().await.unwrap().to_bytes());
    assert!(body.contains("w19_alpha_total 3"), "{body}");
    assert!(body.contains("w19_gamma_total 7"), "{body}");
    assert!(!body.contains("w19_beta_total"), "{body}");

    // 2. Direct HTTP /metrics?name[]=does_not_exist returns empty text body.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics?name[]=does_not_exist")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec())
        .unwrap();
    assert!(body.is_empty(), "expected empty filtered body, got {body:?}");

    // 3. Cost attribution route with ?name[]= and multi-value Accept-Encoding.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/cost_attribution?name[]=cost_a_missing&name[]=cost_alpha_total")
        .header(header::ACCEPT_ENCODING, "deflate")
        .header(header::ACCEPT_ENCODING, "gzip")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok()),
        Some("gzip")
    );
    let body = gunzip(&resp.into_body().collect().await.unwrap().to_bytes());
    assert!(body.contains("cost_alpha_total 11"), "{body}");
    assert!(!body.contains("cost_beta_total"), "{body}");

    // 4. HttpGrpcBridge /metrics?name[]=... with HttpGrpcHeader values: ["deflate", "gzip"].
    let bridge = HttpGrpcBridge::new(router.clone());
    let hresp = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/metrics?name[]=w19_beta_total".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "Accept-Encoding".to_string(),
                values: vec!["deflate".to_string(), "gzip".to_string()],
            }],
            body: Bytes::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(hresp.code, 200);
    assert!(
        hresp
            .headers
            .iter()
            .any(|h| h.key == "Content-Encoding" && h.values == vec!["gzip".to_string()]),
        "missing gzip Content-Encoding: {:?}",
        hresp.headers
    );
    let body = gunzip(&hresp.body);
    assert!(body.contains("w19_beta_total 5"), "{body}");
    assert!(!body.contains("w19_alpha_total"), "{body}");

    // 5. /ingester/tsdb_metrics?name[]=... filters per-tenant TSDB metrics.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/ingester/tsdb_metrics?name[]=prometheus_tsdb_head_series")
        .header("X-Scope-OrgID", "tenant-a")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec())
        .unwrap();
    assert!(body.contains("prometheus_tsdb_head_series"), "{body}");
    assert!(!body.contains("prometheus_tsdb_head_samples_appended_total"), "{body}");
}

#[tokio::test]
async fn w19_rkn_04_gorilla_mux_clean_path_redirects_fragment_span_and_shutdown() {
    let spans = ensure_recording_tracing();

    let shutdown = Arc::new(AtomicBool::new(true));
    let router = build_http_router(make_test_http_state(
        Registry::new(),
        None,
        None,
        Arc::clone(&shutdown),
    ));

    // Direct HTTP cleanPath 301 redirects with Connection: close.
    for (uri, expected_loc) in [
        ("//ready", "/ready"),
        ("/ingester//ring?a=1", "/ingester/ring?a=1"),
        ("/ready/.", "/ready"),
        ("/ingester/../ready", "/ready"),
        ("/ready/./", "/ready/"),
        // `UseEncodedPath`: `cleanPath` runs on the escaped path, which `URL.String()` escapes
        // once more; encoded dot segments and slashes are not cleaned.
        ("//a%20b", "/a%2520b"),
        ("//foo(bar)", "/foo%28bar%29"),
    ] {
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY, "uri={uri}");
        assert_eq!(
            resp.headers()
                .get(header::LOCATION)
                .and_then(|v| v.to_str().ok()),
            Some(expected_loc),
            "uri={uri}"
        );
        assert_eq!(
            resp.headers()
                .get(header::CONNECTION)
                .and_then(|v| v.to_str().ok()),
            Some("close"),
            "uri={uri}"
        );
    }

    // HttpGrpcBridge cleanPath 301 redirects preserve #fragment, run inside shutdown Connection: close,
    // and export child HTTP span.
    let bridge = HttpGrpcBridge::new(router);
    let tid = "4bf92f3577b34da6a3ce929d0e0e4704";
    let sid = "00f067aa0ba902b7";
    // Expected `Location` values are Go's (`url.Parse` + `gorilla/mux` `cleanPath` +
    // `URL.String()`): a scheme-less `//ready` is the authority `ready` with an empty path.
    for (url, expected_loc) in [
        ("//ready", "//ready/"),
        ("ready", "/ready"),
        ("/ingester//ring?a=1", "/ingester/ring?a=1"),
        ("/ready/.", "/ready"),
        ("/ingester/../ready", "/ready"),
        ("http://example.com?mode=diff", "http://example.com/?mode=diff"),
        ("ingester//ring#sec", "/ingester/ring#sec"),
        ("//ready#frag", "//ready/#frag"),
        ("///ready#frag", "/ready#frag"),
        ("/ingester//a%20b", "/ingester/a%2520b"),
    ] {
        let hresp = bridge
            .handle(tonic::Request::new(HttpRequest {
                method: "GET".to_string(),
                url: url.to_string(),
                headers: vec![HttpGrpcHeader {
                    key: "traceparent".to_string(),
                    values: vec![format!("00-{tid}-{sid}-01")],
                }],
                body: Bytes::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(hresp.code, 301, "url={url}");
        assert!(
            hresp
                .headers
                .iter()
                .any(|h| h.key == "Location" && h.values == vec![expected_loc.to_string()]),
            "url={url}, headers={:?}",
            hresp.headers
        );
        assert!(
            hresp
                .headers
                .iter()
                .any(|h| h.key == "Connection" && h.values == vec!["close".to_string()]),
            "missing Connection: close on 301 redirect for url={url}: {:?}",
            hresp.headers
        );
    }

    // UseEncodedPath: %2F in path is NOT cleaned/redirected.
    let err = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/ingester/tsdb/a%2Fb".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    let expected_tid = [
        0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x04,
    ];
    let recorded: Vec<ExportedSpan> = spans
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|s| s.trace_id == expected_tid)
        .cloned()
        .collect();
    assert_eq!(recorded.len(), 10, "expected 10 redirect spans: {recorded:?}");
    assert_eq!(recorded[6].name, "HTTP GET");
    assert!(recorded[6].attributes.iter().any(|a| {
        a.key == "url.path" && matches!(&a.value, SpanAttrValue::String(v) if v == "ingester//ring")
    }));
}

#[tokio::test]
async fn w19_rkn_05_utf8_headers_and_control_byte_org_id_over_httpgrpc() {
    let spans = ensure_recording_tracing();

    let tmp = tempdir().unwrap();
    let ing = make_test_ingester(tmp.path(), true);

    let utf8_router = Router::new()
        .route(
            "/utf8-ok",
            get(|| async {
                let mut resp = axum::response::Response::new(Body::from("ok"));
                resp.headers_mut().insert(
                    "x-custom-utf8",
                    HeaderValue::from_bytes("café-日本語".as_bytes()).unwrap(),
                );
                resp
            }),
        )
        .route(
            "/utf8-err",
            get(|| async {
                let mut resp = axum::response::Response::new(Body::from("raw"));
                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                resp.headers_mut().insert(
                    "x-errormessage",
                    HeaderValue::from_bytes("błąd-erreur-エラー".as_bytes()).unwrap(),
                );
                resp
            }),
        )
        .merge(mimir_ingester::ingester_router(ing));

    let bridge = HttpGrpcBridge::new(utf8_router);

    // 1. Valid non-ASCII UTF-8 response header & User-Agent span attribute.
    let hresp = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/utf8-ok".to_string(),
            headers: vec![
                HttpGrpcHeader {
                    key: "User-Agent".to_string(),
                    values: vec!["mimir-日本語/1.0".to_string()],
                },
                HttpGrpcHeader {
                    key: "traceparent".to_string(),
                    values: vec!["00-4bf92f3577b34da6a3ce929d0e0e4705-00f067aa0ba902b7-01".to_string()],
                },
            ],
            body: Bytes::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(hresp.code, 200);
    assert!(
        hresp
            .headers
            .iter()
            .any(|h| h.key == "X-Custom-Utf8" && h.values == vec!["café-日本語".to_string()]),
        "UTF-8 response header dropped: {:?}",
        hresp.headers
    );

    let expected_tid = [
        0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x05,
    ];
    let recorded: Vec<ExportedSpan> = spans
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|s| s.trace_id == expected_tid)
        .cloned()
        .collect();
    assert!(
        recorded.last().unwrap().attributes.iter().any(|a| {
            a.key == "user_agent.original"
                && matches!(&a.value, SpanAttrValue::String(s) if s == "mimir-日本語/1.0")
        }),
        "UTF-8 User-Agent dropped: {:?}",
        recorded.last()
    );

    // 2. Valid non-ASCII UTF-8 X-ErrorMessage preserved in Status.message().
    let err_status = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/utf8-err".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err_status.message(), "błąd-erreur-エラー");

    // 3. Control/DEL byte in X-Scope-OrgID yields Go exact `contains unsupported character` 401.
    let org_err = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/ingester/tsdb_metrics".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "X-Scope-OrgID".to_string(),
                values: vec!["bad\x7ftenant".to_string()],
            }],
            body: Bytes::new(),
        }))
        .await
        .unwrap_err();
    assert!(
        org_err
            .message()
            .contains("tenant ID 'bad\x7ftenant' contains unsupported character '\x7f'"),
        "unexpected error message: {:?}",
        org_err.message()
    );
}

#[tokio::test]
async fn w19_rkn_08_grpc_server_span_error_status_and_httpgrpc_parent_attrs() {
    let spans = ensure_recording_tracing();

    let srv_cfg = ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        ..Default::default()
    };

    let reg = Registry::new();
    let server = Server::new(&srv_cfg, &reg, Arc::new(NoopLogger)).unwrap();
    let grpc_addr = server.grpc_addr().unwrap();

    let http_router = Router::new()
        .route("/ready", get(|| async { (StatusCode::OK, "ready") }))
        .route(
            "/boom",
            get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
        );
    let bridge = HttpGrpcStatusFixService::new(HttpServer::new(HttpGrpcBridge::new(http_router)));
    let health_modules = ModuleStates::new();
    let srv_state = mimir_cmd::modules::StateCell::new(State::Running);
    health_modules.add("server", srv_state.state_fn());
    let health = mimir_cmd::grpc::HealthService::new(
        Arc::new(AtomicBool::new(false)),
        health_modules,
    );
    let routes = tonic::service::Routes::new(bridge).add_service(health);
    let running = server.start(Router::new(), routes).unwrap();

    let ep = format!("http://{grpc_addr}");
    let channel = tonic::transport::Endpoint::from_shared(ep)
        .unwrap()
        .connect()
        .await
        .unwrap();

    let start_idx = spans.lock().unwrap_or_else(|e| e.into_inner()).len();

    // 1. Health/Watch -> Unimplemented (code 12) => status.code = 2 (status_error = true).
    let mut raw_client = mimir_proto::httpgrpcpb::http_client::HttpClient::new(channel.clone());
    let mut watch_req = tonic::Request::new(mimir_cmd::grpc::HealthCheckRequest {
        service: String::new(),
    });
    watch_req.metadata_mut().insert(
        "traceparent",
        "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-1111111111111111-01"
            .parse()
            .unwrap(),
    );
    let watch_res: Result<tonic::Response<mimir_cmd::grpc::HealthCheckResponse>, tonic::Status> = {
        let mut g = tonic::client::Grpc::new(channel.clone());
        g.ready().await.unwrap();
        g.unary(
            watch_req,
            http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Watch"),
            tonic::codec::ProstCodec::default(),
        )
        .await
    };
    assert_eq!(watch_res.unwrap_err().code(), tonic::Code::Unimplemented);

    // 2. Health/Check -> OK (code 0) => status_error = false.
    let mut check_req = tonic::Request::new(mimir_cmd::grpc::HealthCheckRequest {
        service: String::new(),
    });
    check_req.metadata_mut().insert(
        "traceparent",
        "00-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-2222222222222222-01"
            .parse()
            .unwrap(),
    );
    let check_res: tonic::Response<mimir_cmd::grpc::HealthCheckResponse> = {
        let mut g = tonic::client::Grpc::new(channel);
        g.ready().await.unwrap();
        g.unary(
            check_req,
            http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check"),
            tonic::codec::ProstCodec::default(),
        )
        .await
        .unwrap()
    };
    assert_eq!(check_res.into_inner().status, 1);

    // 3. httpgrpc 500 (/boom) => parent span httpgrpc.HTTP/Handle carries the HTTP attrs. Its gRPC
    //    code is the HTTP status (500), which otelgrpc `serverStatus` leaves `Unset`.
    let mut boom_req = tonic::Request::new(HttpRequest {
        method: "GET".to_string(),
        url: "/boom".to_string(),
        headers: vec![HttpGrpcHeader {
            key: "User-Agent".to_string(),
            values: vec!["w19-httpgrpc/2.0".to_string()],
        }],
        body: Bytes::new(),
    });
    boom_req.metadata_mut().insert(
        "traceparent",
        "00-cccccccccccccccccccccccccccccccc-3333333333333333-01"
            .parse()
            .unwrap(),
    );
    let boom_err = raw_client.handle(boom_req).await.unwrap_err();
    assert!(boom_err.message().contains("boom"), "{boom_err:?}");

    // 4. httpgrpc 404 (/missing) => parent span httpgrpc.HTTP/Handle has status_error = false AND http.route = "notfound".
    let mut miss_req = tonic::Request::new(HttpRequest {
        method: "GET".to_string(),
        url: "/missing".to_string(),
        headers: Vec::new(),
        body: Bytes::new(),
    });
    miss_req.metadata_mut().insert(
        "traceparent",
        "00-dddddddddddddddddddddddddddddddd-4444444444444444-01"
            .parse()
            .unwrap(),
    );
    let _miss_err = raw_client.handle(miss_req).await.unwrap_err();

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let count = spans.lock().unwrap_or_else(|e| e.into_inner()).len() - start_idx;
        if count >= 6 {
            break;
        }
        assert!(Instant::now() < deadline, "timed out waiting for spans (got {count})");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let exported = spans.lock().unwrap_or_else(|e| e.into_inner())[start_idx..].to_vec();

    let find_span = |tid_byte: u8, name: &str| -> ExportedSpan {
        exported
            .iter()
            .find(|s| s.trace_id == [tid_byte; 16] && s.name == name)
            .cloned()
            .unwrap_or_else(|| panic!("missing span ({tid_byte:#x}, {name}) in {exported:?}"))
    };
    let has_attr = |s: &ExportedSpan, k: &str, v: &str| {
        s.attributes
            .iter()
            .any(|a| a.key == k && matches!(&a.value, SpanAttrValue::String(val) if val == v))
    };

    let watch_span = find_span(0xaa, "grpc.health.v1.Health/Watch");
    assert!(watch_span.status_error);
    assert_eq!(
        span_to_otlp_json(&watch_span)["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["status"]["code"],
        2
    );

    let check_span = find_span(0xbb, "grpc.health.v1.Health/Check");
    assert!(!check_span.status_error);

    let boom_parent = find_span(0xcc, "httpgrpc.HTTP/Handle");
    assert!(
        !boom_parent.status_error,
        "httpgrpc 5xx (gRPC code 500) is not an otelgrpc serverStatus error code"
    );
    assert!(has_attr(&boom_parent, "http.request.method", "GET"));
    assert!(has_attr(&boom_parent, "http.route", "boom"));
    assert!(has_attr(&boom_parent, "url.path", "/boom"));
    assert!(has_attr(&boom_parent, "user_agent.original", "w19-httpgrpc/2.0"));

    let miss_parent = find_span(0xdd, "httpgrpc.HTTP/Handle");
    assert!(!miss_parent.status_error, "404 httpgrpc parent span should not be status_error");
    assert!(has_attr(&miss_parent, "http.route", "notfound"));

    running.shutdown().await;
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn w19_rkn_10_otlp_headers_tls_mtls_timeout_and_jaeger_endpoint_exporter() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let clear_env = || {
        for k in [
            "OTEL_TRACES_EXPORTER",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_HEADERS",
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
            "OTEL_EXPORTER_OTLP_TIMEOUT",
            "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
            "OTEL_EXPORTER_OTLP_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_CLIENT_KEY",
            "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
            "JAEGER_ENDPOINT",
            "OTEL_EXPORTER_JAEGER_ENDPOINT",
            "OTEL_EXPORTER_JAEGER_USER",
            "OTEL_EXPORTER_JAEGER_PASSWORD",
            "JAEGER_AGENT_HOST",
            "JAEGER_SAMPLER_MANAGER_HOST_PORT",
        ] {
            std::env::remove_var(k);
        }
    };
    clear_env();

    // 1. Header parsing (+ preservation & %20 unescaping) + TRACES_HEADERS precedence.
    let parsed = parse_otlp_headers_str("Authorization=Bearer%20tok%2Ben==, X-Org = prod ");
    assert_eq!(
        parsed,
        vec![
            ("Authorization".to_string(), "Bearer tok+en==".to_string()),
            ("X-Org".to_string(), "prod".to_string()),
        ]
    );
    std::env::set_var("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Basic%20old,X-Base=1");
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "Authorization=Basic%20new");
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "2500");
    assert_eq!(
        otlp_headers_from_env(),
        vec![
            ("Authorization".to_string(), "Basic new".to_string()),
            ("X-Base".to_string(), "1".to_string()),
        ]
    );
    assert_eq!(otlp_timeout_from_env(), Some(Duration::from_millis(2500)));

    // 2. OTLP HTTP exporter sends configured auth headers.
    let captured: Arc<Mutex<Vec<(HeaderMap, serde_json::Value)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let cap_clone = Arc::clone(&captured);
    let mock_app = Router::new().route(
        "/v1/traces",
        axum::routing::post(move |headers: HeaderMap, body: Bytes| {
            let cap = Arc::clone(&cap_clone);
            async move {
                let json = serde_json::from_slice(&body).unwrap();
                cap.lock().unwrap_or_else(|e| e.into_inner()).push((headers, json));
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_task = tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    std::env::set_var(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        format!("http://{addr}/v1/traces"),
    );
    let exporter = build_span_exporter().expect("OTLP exporter built");
    let sample_span = ExportedSpan {
        service_name: "mimir-ingester".to_string(),
        trace_id: [1u8; 16],
        span_id: [2u8; 8],
        parent_span_id: None,
        name: "test-span".to_string(),
        kind: 2,
        start_time_unix_nano: 100,
        end_time_unix_nano: 200,
        attributes: Vec::new(),
        status_error: false,
    };
    exporter(sample_span.clone());
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if !captured.lock().unwrap_or_else(|e| e.into_inner()).is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "OTLP span not received");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let (hdrs, _) = captured.lock().unwrap_or_else(|e| e.into_inner()).remove(0);
    assert_eq!(hdrs.get("authorization").unwrap(), "Basic new");
    assert_eq!(hdrs.get("x-base").unwrap(), "1");
    srv_task.abort();

    // 3. OTLP HTTPS mTLS using OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE / CLIENT_CERTIFICATE / CLIENT_KEY.
    clear_env();
    let pki_dir = tempdir().unwrap();
    let ca_key = pki_dir.path().join("ca.key");
    let ca_crt = pki_dir.path().join("ca.crt");
    let srv_key = pki_dir.path().join("srv.key");
    let srv_csr = pki_dir.path().join("srv.csr");
    let srv_crt = pki_dir.path().join("srv.crt");
    let srv_ext = pki_dir.path().join("srv.ext");
    let cli_key = pki_dir.path().join("cli.key");
    let cli_csr = pki_dir.path().join("cli.csr");
    let cli_crt = pki_dir.path().join("cli.crt");
    std::fs::write(&srv_ext, "subjectAltName=IP:127.0.0.1,DNS:localhost\n").unwrap();

    let run_openssl = |args: &[&str]| {
        let out = Command::new("openssl").args(args).output().unwrap();
        assert!(
            out.status.success(),
            "openssl {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run_openssl(&[
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=w19-ca",
        "-keyout", ca_key.to_str().unwrap(),
        "-out", ca_crt.to_str().unwrap(),
    ]);
    run_openssl(&[
        "req", "-newkey", "rsa:2048", "-nodes",
        "-subj", "/CN=localhost",
        "-keyout", srv_key.to_str().unwrap(),
        "-out", srv_csr.to_str().unwrap(),
    ]);
    run_openssl(&[
        "x509", "-req", "-days", "1",
        "-in", srv_csr.to_str().unwrap(),
        "-CA", ca_crt.to_str().unwrap(),
        "-CAkey", ca_key.to_str().unwrap(),
        "-CAcreateserial",
        "-extfile", srv_ext.to_str().unwrap(),
        "-out", srv_crt.to_str().unwrap(),
    ]);
    run_openssl(&[
        "req", "-newkey", "rsa:2048", "-nodes",
        "-subj", "/CN=w19-client",
        "-keyout", cli_key.to_str().unwrap(),
        "-out", cli_csr.to_str().unwrap(),
    ]);
    run_openssl(&[
        "x509", "-req", "-days", "1",
        "-in", cli_csr.to_str().unwrap(),
        "-CA", ca_crt.to_str().unwrap(),
        "-CAkey", ca_key.to_str().unwrap(),
        "-CAcreateserial",
        "-out", cli_crt.to_str().unwrap(),
    ]);

    use rustls_pki_types::pem::PemObject as _;
    let mut root_store = rustls::RootCertStore::empty();
    for der in rustls_pki_types::CertificateDer::pem_slice_iter(&std::fs::read(&ca_crt).unwrap())
        .filter_map(Result::ok)
    {
        root_store.add(der).unwrap();
    }
    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .unwrap();
    let srv_certs: Vec<_> =
        rustls_pki_types::CertificateDer::pem_slice_iter(&std::fs::read(&srv_crt).unwrap())
            .filter_map(Result::ok)
            .collect();
    let srv_p_key =
        rustls_pki_types::PrivateKeyDer::from_pem_slice(&std::fs::read(&srv_key).unwrap()).unwrap();
    let tls_cfg = Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(srv_certs, srv_p_key)
        .unwrap(),
    );
    let tls_acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
    let tls_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tls_addr = tls_listener.local_addr().unwrap();
    let tls_received = Arc::new(AtomicBool::new(false));
    let tls_rec_clone = Arc::clone(&tls_received);
    let tls_task = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        if let Ok((tcp, _)) = tls_listener.accept().await {
            if let Ok(mut tls_stream) = tls_acceptor.accept(tcp).await {
                let mut buf = [0u8; 4096];
                let _ = tls_stream.read(&mut buf).await;
                tls_rec_clone.store(true, Ordering::Release);
                let _ = tls_stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            }
        }
    });

    std::env::set_var(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        format!("https://{tls_addr}/v1/traces"),
    );
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE", ca_crt.to_str().unwrap());
    std::env::set_var(
        "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
        cli_crt.to_str().unwrap(),
    );
    std::env::set_var(
        "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
        cli_key.to_str().unwrap(),
    );
    assert_eq!(
        otlp_tls_paths_from_env().ca_cert_path.as_deref(),
        Some(ca_crt.to_str().unwrap())
    );
    let tls_exp = build_span_exporter().expect("mTLS OTLP exporter");
    tls_exp(sample_span.clone());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !tls_received.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "mTLS OTLP span never arrived");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tls_task.abort();

    // 4. JAEGER_ENDPOINT activates exporter and sends Basic Auth when user/password set.
    clear_env();
    let j_captured: Arc<Mutex<Vec<HeaderMap>>> = Arc::new(Mutex::new(Vec::new()));
    let j_cap = Arc::clone(&j_captured);
    let j_app = Router::new().route(
        "/api/traces",
        axum::routing::post(move |headers: HeaderMap, _body: Bytes| {
            let j_cap = Arc::clone(&j_cap);
            async move {
                j_cap.lock().unwrap_or_else(|e| e.into_inner()).push(headers);
                StatusCode::ACCEPTED
            }
        }),
    );
    let j_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let j_addr = j_listener.local_addr().unwrap();
    let j_task = tokio::spawn(async move {
        axum::serve(j_listener, j_app).await.unwrap();
    });

    let j_ep = format!("http://{j_addr}/api/traces");
    std::env::set_var("JAEGER_ENDPOINT", &j_ep);
    std::env::set_var("OTEL_EXPORTER_JAEGER_USER", "alice");
    std::env::set_var("OTEL_EXPORTER_JAEGER_PASSWORD", "secret");
    assert_eq!(detected_exporter_kind().as_deref(), Some("jaeger"));
    assert_eq!(jaeger_endpoint_from_env().as_deref(), Some(j_ep.as_str()));

    let j_exp = build_span_exporter().expect("Jaeger exporter activated");
    j_exp(sample_span);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if !j_captured.lock().unwrap_or_else(|e| e.into_inner()).is_empty() {
            break;
        }
        assert!(Instant::now() < deadline, "Jaeger span not received");
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    let j_hdrs = j_captured.lock().unwrap_or_else(|e| e.into_inner()).remove(0);
    // base64("alice:secret") == "YWxpY2U6c2VjcmV0"
    assert_eq!(j_hdrs.get("authorization").unwrap(), "Basic YWxpY2U6c2VjcmV0");
    j_task.abort();
    clear_env();
}

#[test]
fn w19_mod_03_activity_tracker_max_entries_and_sanity_err_lifecycle_logs() {
    // 1. validate_activity_tracker_max_entries rejects <= 0 and overflow.
    assert!(validate_activity_tracker_max_entries(0)
        .unwrap_err()
        .to_string()
        .contains("failed to mmap activity file"));
    assert!(validate_activity_tracker_max_entries(-5)
        .unwrap_err()
        .to_string()
        .contains("failed to truncate activity file"));
    assert!(validate_activity_tracker_max_entries(i64::MAX)
        .unwrap_err()
        .to_string()
        .contains("failed to truncate activity file"));
    assert_eq!(validate_activity_tracker_max_entries(1024).unwrap(), 1024);

    // 2. Real binary exits non-zero on -activity-tracker.max-entries=0 & -1.
    let tmp = tempdir().unwrap();
    let act_path = tmp.path().join("activity.log");
    for max_entries in ["0", "-1"] {
        let out = Command::new(env!("CARGO_BIN_EXE_mimir"))
            .args([
                "-target=ingester",
                &format!("-activity-tracker.filepath={}", act_path.display()),
                &format!("-activity-tracker.max-entries={max_entries}"),
                "-server.http-listen-port=0",
                "-server.grpc-listen-port=0",
            ])
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "max-entries={max_entries} should fail startup"
        );
    }

    // 3. Real binary logs `starting` + `module stopped` for `activity-tracker` and runs
    //    unfinished-entries shutdown cleanup even when sanity-check fails.
    {
        let tracker = ActivityTracker::new(
            ActivityTrackerConfig {
                filepath: act_path.clone(),
                max_entries: 16,
            },
            None,
        )
        .unwrap()
        .unwrap();
        let idx = tracker.insert(|| "unfinished-w19-task".to_string());
        assert!(idx >= 0);
        let _ = tracker.flush();
        std::mem::forget(tracker);
    }
    let not_a_dir = tmp.path().join("not-a-dir");
    std::fs::write(&not_a_dir, b"file").unwrap();
    let bad_s3_cfg = tmp.path().join("bad-sanity.yaml");
    std::fs::write(
        &bad_s3_cfg,
        format!(
            "blocks_storage:\n  backend: filesystem\n  filesystem:\n    dir: {}\n  tsdb:\n    dir: {}\ningester:\n  ring:\n    kvstore:\n      store: inmemory\n",
            tmp.path().join("blocks").display(),
            not_a_dir.display(),
        ),
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_mimir"))
        .args([
            "-target=ingester",
            &format!("-config.file={}", bad_s3_cfg.display()),
            &format!("-activity-tracker.filepath={}", act_path.display()),
            "-activity-tracker.max-entries=16",
            "-server.http-listen-port=0",
            "-server.grpc-listen-port=0",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let logs = String::from_utf8_lossy(&out.stderr);
    assert!(
        logs.contains(r#"msg=starting module=activity-tracker"#),
        "missing activity-tracker starting log:\n{logs}"
    );
    assert!(
        logs.contains(r#"msg="module stopped" module=activity-tracker"#),
        "missing activity-tracker module stopped log on sanity_err:\n{logs}"
    );
    assert!(
        logs.contains("unfinished-w19-task"),
        "missing unfinished activity log:\n{logs}"
    );
}

#[tokio::test]
async fn w19_mod_04_strict_rfc3339_and_usagestats_mode_reset() {
    // 1. Strict RFC3339 rejection on invalid dates/times/formats.
    assert!(parse_rfc3339("2024-00-99T99:99:99Z").is_none());
    assert!(parse_rfc3339("2024-13-45T00:00:00+99:99").is_none());
    assert!(parse_rfc3339("2024-02-30T00:00:00Z").is_none());
    assert!(parse_rfc3339("2024-01-01T00:00:00.Z").is_none());
    assert!(parse_rfc3339("2024-01-01T00:00:00").is_none());
    assert!(parse_rfc3339("2024-02-29T12:34:56.789Z").is_some());
    assert!(parse_rfc3339("2026-11-17T00:24:13.728715478+01:00").is_some());

    // 2. Corrupted seed JSON with invalid calendar date triggers re-creation in init_seed_file.
    let bucket = BucketClient::new_in_memory("w19-seed");
    bucket
        .upload(
            CLUSTER_SEED_FILE_NAME,
            br#"{"UID":"bad-uid","created_at":"2024-00-99T99:99:99Z"}"#.to_vec(),
        )
        .await
        .unwrap();
    assert!(read_seed_file(&bucket).await.is_err());
    let ctx = ServiceContext::background();
    let recreated = init_seed_file(&ctx, &bucket, Duration::ZERO)
        .await
        .unwrap();
    assert_ne!(recreated.uid, "bad-uid");

    // 3. usagestats::set_mode resets MODE_CLASSIC when ingest_storage.enabled=false.
    common_usagestats::set_mode(common_usagestats::MODE_INGEST_STORAGE);
    assert_eq!(
        common_usagestats::get_string(common_usagestats::MODE_KEY).value(),
        common_usagestats::MODE_INGEST_STORAGE
    );
    let tmp = tempdir().unwrap();
    let not_a_dir = tmp.path().join("not-a-dir");
    std::fs::write(&not_a_dir, b"file").unwrap();
    let cfg_path = tmp.path().join("classic.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            "blocks_storage:\n  backend: filesystem\n  filesystem:\n    dir: {}\n  tsdb:\n    dir: {}\ningester:\n  ring:\n    kvstore:\n      store: inmemory\n",
            tmp.path().join("blocks").display(),
            not_a_dir.display(),
        ),
    )
    .unwrap();
    let args: Vec<String> = vec![
        "-target=ingester".to_string(),
        format!("-config.file={}", cfg_path.display()),
        "-activity-tracker.filepath=".to_string(),
        "-server.http-listen-port=0".to_string(),
        "-server.grpc-listen-port=0".to_string(),
    ];
    let outcome = parse_cli_for_run("mimir", &args);
    assert!(outcome.config.is_some(), "CLI parse failed: {:?}", outcome.action);
    let _ = run_mimir(outcome).await;
    assert_eq!(
        common_usagestats::get_string(common_usagestats::MODE_KEY).value(),
        common_usagestats::MODE_CLASSIC
    );
}
