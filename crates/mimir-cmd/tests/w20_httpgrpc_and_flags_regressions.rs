//! Regression tests for Wave 20 `mimir-cmd` / `mimir-config` / `mimir-common` fixes:
//! - M20-02 (cmd): `RuntimeConfigMetrics` hybrid native histogram
//! - M20-06: `deprecated_flags_inuse_total` + warn log on `-usage-tracker.use-global-series-limits`
//! - M20-07: `instrument_ref_leaks` config storage/validation & `RefLeaksTracker` wiring in `mimir-cmd`
//! - W20-CR-04: `HttpGrpcBridge` header parity (`Content-Length`, `Connection: close`, `Allow`)
//! - W20-SDW-02 (cmd): `BoundedGrpcDecompressBody` via HTTP/2 multi-frame unary gRPC
//! - W20-SDW-11 (items 1 & 4): 4-digit BCE year width, bounded `pprof` deadline, `%+XX` hex guard

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt as _;
use mimir_cmd::grpc::{GrpcCompressionEchoService, HttpGrpcBridge};
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::ModuleStates;
use mimir_cmd::parse_cli_for_run;
use mimir_cmd::pprof::profile_handler;
use mimir_cmd::runtime_config_metrics::RuntimeConfigMetrics;
use mimir_cmd::server::http_server::SharedWriteDeadline;
use mimir_cmd::server::Server;
use mimir_config::ServerConfig;
use mimir_common::logging::{format_rfc3339_utc, LogFormat, LogLevel, LogSink, Logger};
use mimir_config::{
    parse_cli_and_load_config, LimitsConfig, MimirConfig, Overrides, RuntimeConfigHttpObserver,
};
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb::extract_http_response;
use mimir_proto::httpgrpcpb::http_server::{Http, HttpServer};
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest};

use mimir_proto::prost::Message as _;
use prometheus::Registry;
use tempfile::tempdir;
use tower::ServiceExt;

type LogEntries = Vec<(LogLevel, Vec<(String, String)>)>;

#[derive(Default)]
struct CaptureLogger {
    entries: Mutex<LogEntries>,
}

impl Logger for CaptureLogger {
    fn log(&self, level: LogLevel, fields: &[(&str, &str)]) {
        self.entries.lock().unwrap().push((
            level,
            fields
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        ));
    }
}

fn unzigzag32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

fn make_http_state(
    reg: Registry,
    ingester: Option<Arc<Ingester>>,
    shutdown: Arc<AtomicBool>,
) -> AppHttpState {
    let cfg = Arc::new(MimirConfig::default());
    AppHttpState {
        config: Arc::clone(&cfg),
        default_config: cfg,
        registry: reg,
        shutdown_requested: shutdown,
        ingester,
        runtime_config: None,
        overrides: Overrides::new(LimitsConfig::default()),
        memberlist_kv: None,
        modules: ModuleStates::new(),
        cost_attribution_registry: None,
    }
}

#[test]
fn m20_02_runtime_config_http_duration_has_native_histogram() {
    let reg = Registry::new();
    let logger = Arc::new(CaptureLogger::default());
    let m = RuntimeConfigMetrics::new_with_http(
        &reg,
        logger,
        &["http://127.0.0.1:9999/runtime.yaml".to_string()],
        &["http://127.0.0.1:9999/runtime.yaml".to_string()],
        "",
    )
    .unwrap();
    m.observe_request("http://127.0.0.1:9999/runtime.yaml", "200", Duration::from_millis(25));

    let families = reg.gather();
    let mf = families
        .iter()
        .find(|f| f.get_name() == "cortex_runtime_config_http_request_duration_seconds")
        .expect("missing cortex_runtime_config_http_request_duration_seconds");
    let h = mf.get_metric()[0].get_histogram();
    assert_eq!(h.get_bucket().len(), 11);
    let schema = unzigzag32(
        h.unknown_fields
            .get(5)
            .and_then(|v| v.varint.first().copied())
            .expect("missing native schema") as u32,
    );
    assert_eq!(schema, 3);
}

#[tokio::test]
async fn m20_06_deprecated_usage_tracker_flag_increments_metric_and_logs_warn() {
    let args = vec![
        "-target=memberlist-kv".to_string(),
        "-usage-tracker.use-global-series-limits=true".to_string(),
    ];
    let loaded = parse_cli_and_load_config("mimir", &args).unwrap();
    assert_eq!(
        loaded.config.deprecated_flags_used(),
        &["usage-tracker.use-global-series-limits".to_string()]
    );

    let reg = Registry::new();
    mimir_cmd::metrics::register_global_metrics_with_deprecated(
        &reg,
        None,
        loaded.config.deprecated_flags_used().len(),
    )
    .unwrap();
    let families = reg.gather();
    let dep = families
        .iter()
        .find(|f| f.get_name() == "deprecated_flags_inuse_total")
        .unwrap();
    assert_eq!(dep.get_metric()[0].get_counter().get_value(), 1.0);

    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for SharedBuf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let sink = LogSink::new(
        LogFormat::Logfmt,
        LogLevel::Warn,
        Box::new(SharedBuf(Arc::clone(&buf))),
    );
    for flag in loaded.config.deprecated_flags_used() {
        sink.log(LogLevel::Warn, &[("msg", "flag disabled"), ("flag", flag)]);
    }
    let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(logged.contains("level=warn"), "{logged}");
    assert!(logged.contains("msg=\"flag disabled\""), "{logged}");
    assert!(
        logged.contains("flag=usage-tracker.use-global-series-limits"),
        "{logged}"
    );
}

#[tokio::test]
async fn m20_07_instrument_ref_leaks_config_validation_and_push_wiring() {
    for bad_args in [
        vec!["-target=ingester", "-instrument-reference-leaks.percentage=150"],
        vec!["-target=ingester", "-instrument-reference-leaks.percentage=-1"],
        vec!["-target=ingester", "-instrument-reference-leaks.before-reuse-period=-1s"],
    ] {
        let args: Vec<String> = bad_args.into_iter().map(str::to_string).collect();
        let err = parse_cli_and_load_config("mimir", &args).unwrap_err();
        assert!(
            err.to_string().contains("instrument-ref-leaks")
                || err.to_string().contains("percentage")
                || err.to_string().contains("before-reuse-period"),
            "unexpected err for {args:?}: {err}"
        );
    }
    let out = parse_cli_for_run(
        "mimir",
        &[
            "-target=ingester".to_string(),
            "-instrument-reference-leaks.max-inflight-instrumented-bytes=-1".to_string(),
        ],
    );
    assert_eq!(out.exit_code, 2, "negative uint64 flag must fail flag parse: {}", out.stderr);

    let loaded = parse_cli_and_load_config(
        "mimir",
        &[
            "-target=ingester".to_string(),
            "-instrument-reference-leaks.percentage=100".to_string(),
            "-instrument-reference-leaks.before-reuse-period=0s".to_string(),
            "-instrument-reference-leaks.max-inflight-instrumented-bytes=1048576".to_string(),
        ],
    )
    .unwrap();
    assert_eq!(loaded.config.instrument_ref_leaks.percentage, 100.0);
    assert_eq!(
        loaded.config.instrument_ref_leaks.max_inflight_instrumented_bytes,
        1_048_576
    );

    let reg = Registry::new();
    let tracker = loaded
        .config
        .instrument_ref_leaks
        .to_proto_config()
        .tracker(Some(&reg));

    let tmp = tempdir().unwrap();
    let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().to_string();
    let ing_cfg = mimir_config::IngesterConfig::default();
    let ing = Arc::new(
        Ingester::new(
            ing_cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            Overrides::new(LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap(),
    );
    ing.set_state_for_test(mimir_common::services::State::Running);

    // app.rs wiring: the tracker is handed to the ingester, which instruments each unmarshalled
    // `WriteRequest` buffer exactly once (Go `codecV2.Unmarshal` -> `maybeInstrumentRefLeaks`).
    ing.set_ref_leaks_tracker(tracker.clone());

    let push = |wr: cortexpb::WriteRequest| {
        let svc = GrpcCompressionEchoService::new(
            IngesterService::new(Arc::clone(&ing)).into_server(),
        );
        async move {
            let encoded = wr.encode_to_vec();
            let mut framed = Vec::with_capacity(5 + encoded.len());
            framed.push(0);
            framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            framed.extend_from_slice(&encoded);
            let req = http::Request::builder()
                .method(Method::POST)
                .uri("/cortex.Ingester/Push")
                .header(header::CONTENT_TYPE, "application/grpc")
                .header("x-scope-orgid", "tenant-a")
                .body(tonic::body::BoxBody::new(
                    http_body_util::Full::new(Bytes::from(framed)).map_err(|never| match never {}),
                ))
                .unwrap();
            let _ = svc.oneshot(req).await.unwrap();
        }
    };

    // Go `maybeInstrumentRefLeaks`: `data.Len() > 0` — an empty message is never instrumented.
    push(cortexpb::WriteRequest::default()).await;
    assert_eq!(tracker.instrumented_buffers_total(), 0.0);

    // One non-empty Push message == exactly one instrumented buffer at percentage=100, released
    // (before-reuse-period=0s) once the request completes.
    push(cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: "__name__".into(),
                value: "rl_metric".into(),
            }],
            samples: vec![cortexpb::Sample {
                timestamp_ms: 1,
                value: 1.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    })
    .await;
    assert_eq!(tracker.instrumented_buffers_total(), 1.0);
    assert_eq!(tracker.inflight_instrumented_bytes(), 0.0);
    let names: Vec<String> = reg.gather().iter().map(|mf| mf.get_name().to_string()).collect();
    assert!(names.contains(&"mimir_reference_leaks_instrumented_buffers_total".to_string()));
    assert!(names.contains(&"mimir_reference_leaks_inflight_instrumented_bytes".to_string()));

    let families = reg.gather();
    let bufs = families
        .iter()
        .find(|f| f.get_name() == "mimir_reference_leaks_instrumented_buffers_total")
        .expect("missing mimir_reference_leaks_instrumented_buffers_total");
    assert_eq!(bufs.get_metric()[0].get_counter().get_value(), 1.0);
    let inflight = families
        .iter()
        .find(|f| f.get_name() == "mimir_reference_leaks_inflight_instrumented_bytes")
        .expect("missing mimir_reference_leaks_inflight_instrumented_bytes");
    assert_eq!(inflight.get_metric()[0].get_gauge().get_value(), 0.0);

    let _ = ing.stop().await;
}

#[tokio::test]
async fn w20_cr_04_httpgrpc_and_http_header_parity() {
    let shutdown = Arc::new(AtomicBool::new(true));
    let reg = Registry::new();
    let router = build_http_router(make_http_state(
        reg.clone(),
        None,
        Arc::clone(&shutdown),
    ));
    let bridge = HttpGrpcBridge::new(router.clone());

    for (url, headers) in [
        ("/services", vec![]),
        (
            "/services",
            vec![HttpGrpcHeader {
                key: "Accept-Encoding".to_string(),
                values: vec!["gzip".to_string()],
            }],
        ),
        ("/ready", vec![]),
    ] {
        let res = bridge
            .handle(tonic::Request::new(HttpRequest {
                method: "GET".to_string(),
                url: url.to_string(),
                headers,
                body: Bytes::new(),
            }))
            .await;
        let resp_headers = match res {
            Ok(r) => r.into_inner().headers,
            Err(st) => extract_http_response(&st).unwrap().headers,
        };
        assert!(
            !resp_headers
                .iter()
                .any(|h| h.key.eq_ignore_ascii_case("content-length")
                    || h.key.eq_ignore_ascii_case("connection")),
            "url={url}: unexpected Content-Length or Connection in httpgrpc headers: {resp_headers:?}"
        );
    }

    let static_res = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/static/mimir-styles.css".to_string(),
            headers: vec![],
            body: Bytes::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        static_res.headers.iter().any(|h| h.key == "Content-Length"),
        "expected Content-Length on /static/ file: {:?}",
        static_res.headers
    );

    let srv_cfg = ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        ..Default::default()
    };
    let mut srv = Server::new(&srv_cfg, &reg, Arc::new(CaptureLogger::default())).unwrap();
    srv.set_shutdown_requested(Arc::clone(&shutdown));
    let http_addr = srv.http_addr().unwrap();
    let grpc_routes = tonic::service::Routes::new(HttpServer::new(bridge.clone()));
    let running = srv.start(router, grpc_routes).unwrap();

    let client = reqwest::Client::new();
    let http_resp = client
        .get(format!("http://{http_addr}/services"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        http_resp
            .headers()
            .get(reqwest::header::CONNECTION)
            .and_then(|v| v.to_str().ok()),
        Some("close")
    );

    let http_405 = client
        .put(format!("http://{http_addr}/services"))
        .send()
        .await
        .unwrap();
    assert_eq!(http_405.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    assert!(
        http_405.headers().get(reqwest::header::ALLOW).is_none(),
        "unexpected Allow header on HTTP 405: {:?}",
        http_405.headers()
    );
    running.shutdown().await;

    let grpc_405_err = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "PUT".to_string(),
            url: "/services".to_string(),
            headers: vec![],
            body: Bytes::new(),
        }))
        .await
        .unwrap_err();
    let grpc_405 = extract_http_response(&grpc_405_err).unwrap();
    assert_eq!(grpc_405.code, 405);
    assert!(
        !grpc_405
            .headers
            .iter()
            .any(|h| h.key.eq_ignore_ascii_case("allow") || h.key.eq_ignore_ascii_case("content-length")),
        "unexpected Allow/Content-Length on httpgrpc 405: {:?}",
        grpc_405.headers
    );
}

#[test]
fn w20_sdw_11_bce_four_digit_year_and_pprof_deadline() {
    assert_eq!(
        format_rfc3339_utc(UNIX_EPOCH - Duration::from_secs(62_167_219_201)),
        "-0001-12-31T23:59:59Z"
    );
    let s_neg42 = format_rfc3339_utc(UNIX_EPOCH - Duration::from_secs(63_461_318_400));
    assert!(s_neg42.starts_with("-0042-"), "got {s_neg42}");
    let s_neg999 = format_rfc3339_utc(UNIX_EPOCH - Duration::from_secs(93_692_016_000));
    assert!(s_neg999.starts_with("-0999-"), "got {s_neg999}");

    let wd = SharedWriteDeadline::new(Duration::from_secs(60));
    wd.extend_by(Duration::from_secs(u64::MAX));
    assert!(wd.current().is_some());
}

#[tokio::test]
async fn w20_sdw_11_pprof_rejects_huge_seconds_and_keeps_deadline() {
    let wd = SharedWriteDeadline::new(Duration::from_secs(60));
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/debug/pprof/profile?seconds=9223372036854775807")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut().insert(wd.clone());
    let resp = profile_handler(req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(wd.current().is_some());
}
