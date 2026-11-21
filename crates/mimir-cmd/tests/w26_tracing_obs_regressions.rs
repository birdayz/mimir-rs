//! Wave 26 Track 3 regression tests for S2-01, S2-02, and S2-05:
//! - Disabled-mode no-op trace extraction on HTTP & gRPC (no trace_id / trace_id_unsampled)
//! - Error-disabling on invalid OTEL_TRACES_EXPORTER / OTEL_EXPORTER_OTLP[_TRACES]_PROTOCOL
//! - OTLP http/protobuf (default), http/json, and gRPC (HTTP/2) exporters + 5s shutdown flush
//! - Jaeger HTTP Thrift collector (application/x-thrift) and UDP Compact Thrift agent
//! - -server.create-new-traces Span Links, HTTP/gRPC semconv attrs, and >100 headers overflow event

use std::fs;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::routing::on;
use axum::routing::MethodFilter;
use axum::Router;
use bytes::Bytes;
use http::{HeaderMap, Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use mimir_cmd::app::{try_build_span_exporter_with_handle, SpanExporterHandle};
use mimir_cmd::modules::{ModuleStates, StateCell};
use mimir_cmd::server::http_middleware::{dskit_http_middleware, HttpMiddlewareConfig};
use mimir_cmd::server::http_server::ServedOverHttp;
use mimir_cmd::server::metrics::ServerMetrics;
use mimir_cmd::server::Server;
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::services::State;
use mimir_common::tracing::{
    init_otel_or_jaeger_from_env, ExportedSpan, SpanAttrValue, SpanAttribute, SpanEvent, SpanLink,
};
use mimir_config::ServerConfig;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_client::IngesterClient;
use prometheus::Registry;
use serde_json::Value;
use tempfile::tempdir;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tower::ServiceExt as _;

static ENV_LOCK: Mutex<()> = Mutex::new(());
static RECORDED_SPANS: OnceLock<Arc<parking_lot::Mutex<Vec<ExportedSpan>>>> = OnceLock::new();

#[derive(Default)]
struct NoopLogger;
impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
}

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn clear_all_tracing_env() {
    for k in [
        "OTEL_TRACES_EXPORTER",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_INSECURE",
        "OTEL_EXPORTER_OTLP_TRACES_INSECURE",
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
        "OTEL_RESOURCE_ATTRIBUTES",
        "OTEL_TRACES_SAMPLER",
        "OTEL_TRACES_SAMPLER_ARG",
        "OTEL_PROPAGATORS",
        "JAEGER_AGENT_HOST",
        "JAEGER_AGENT_PORT",
        "JAEGER_ENDPOINT",
        "OTEL_EXPORTER_JAEGER_ENDPOINT",
        "OTEL_EXPORTER_JAEGER_USER",
        "OTEL_EXPORTER_JAEGER_PASSWORD",
        "JAEGER_SAMPLER_MANAGER_HOST_PORT",
        "JAEGER_SAMPLING_ENDPOINT",
        "JAEGER_SAMPLER_TYPE",
        "JAEGER_SAMPLER_PARAM",
        "JAEGER_SAMPLER_NOT_PARENT_BASED",
        "JAEGER_TAGS",
    ] {
        std::env::remove_var(k);
    }
}

fn minimal_mimir_yaml(dir: &Path, http_port: u16, grpc_port: u16) -> String {
    let tsdb_dir = dir.join("tsdb");
    let bucket_dir = dir.join("bucket");
    fs::create_dir_all(&tsdb_dir).unwrap();
    fs::create_dir_all(&bucket_dir).unwrap();
    format!(
        r#"target: ingester
multitenancy_enabled: false
no_auth_tenant: tenant-default
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_port}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_port}
  log_level: debug
  log_format: json
  cluster_validation:
    label: expected-cluster
    http:
      enabled: true
      soft_validation: true
    grpc:
      enabled: true
      soft_validation: true
ingester:
  ring:
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
activity_tracker:
  filepath: ""
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
usage_stats:
  enabled: false
"#,
        bucket = bucket_dir.display(),
        tsdb = tsdb_dir.display(),
    )
}

async fn run_mimir_subproc_and_collect_logs(extra_env: &[(&str, &str)]) -> Vec<Value> {
    let dir = tempdir().unwrap();
    let http_port = free_port();
    let grpc_port = free_port();
    let cfg_path = dir.path().join("mimir.yaml");
    fs::write(&cfg_path, minimal_mimir_yaml(dir.path(), http_port, grpc_port)).unwrap();

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mimir"));
    cmd.arg(format!("-config.file={}", cfg_path.display()));
    for k in [
        "OTEL_TRACES_EXPORTER",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_PROPAGATORS",
        "JAEGER_AGENT_HOST",
        "JAEGER_ENDPOINT",
        "JAEGER_SAMPLER_MANAGER_HOST_PORT",
    ] {
        cmd.env_remove(k);
    }
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.stdout(Stdio::null()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn mimir");
    let stderr = child.stderr.take().unwrap();
    let logs_buf = Arc::new(parking_lot::Mutex::new(String::new()));
    let logs_clone = Arc::clone(&logs_buf);
    let reader_handle = std::thread::spawn(move || {
        let br = BufReader::new(stderr);
        for line in br.lines().map_while(Result::ok) {
            let mut g = logs_clone.lock();
            g.push_str(&line);
            g.push('\n');
        }
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let ready_url = format!("http://127.0.0.1:{http_port}/ready");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(r) = client.get(&ready_url).send().await {
            if r.status().as_u16() == 200 {
                break;
            }
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader_handle.join();
            panic!("mimir failed to become ready; stderr:\n{}", logs_buf.lock());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = client
        .get(format!("http://127.0.0.1:{http_port}/nonexistent-disabled"))
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .header("uber-trace-id", "48475050943e8e05:70e8b02d28e4337b:0:3")
        .header("Jaeger-Debug-Id", "dbg-disabled-1")
        .send()
        .await
        .unwrap();

    let ch = Channel::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut ing_client = IngesterClient::new(ch);
    let mut greq = tonic::Request::new(ingesterpb::LabelNamesRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 1000,
        matchers: None,
        limit: 0,
    });
    greq.metadata_mut().insert(
        "traceparent",
        MetadataValue::try_from("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .unwrap(),
    );
    let _ = ing_client.label_names(greq).await.unwrap();
    drop(ing_client);
    drop(client);

    tokio::time::sleep(Duration::from_millis(100)).await;
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let _ = child.wait();
    let _ = reader_handle.join();

    let raw = logs_buf.lock().clone();
    raw.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line.trim()).ok())
        .collect()
}

#[tokio::test]
async fn s2_02_disabled_mode_emits_no_trace_ids_on_http_and_grpc() {
    let logs = run_mimir_subproc_and_collect_logs(&[]).await;

    let http_404 = logs
        .iter()
        .find(|v| {
            v["msg"]
                .as_str()
                .is_some_and(|m| m.contains("/nonexistent-disabled (404)"))
        })
        .expect("expected HTTP 404 log");
    assert!(http_404.get("trace_id").is_none(), "unexpected trace_id: {http_404}");
    assert!(
        http_404.get("trace_id_unsampled").is_none(),
        "unexpected trace_id_unsampled: {http_404}"
    );

    let http_cv = logs
        .iter()
        .find(|v| {
            v["path"] == "/nonexistent-disabled"
                && v["msg"] == "request with no cluster validation label"
        })
        .expect("expected HTTP cluster validation warning");
    assert!(http_cv.get("trace_id").is_none(), "unexpected trace_id: {http_cv}");

    let grpc_cv = logs
        .iter()
        .find(|v| {
            v["method"] == "/cortex.Ingester/LabelNames"
                && v["msg"] == "request with no cluster validation label"
        })
        .expect("expected gRPC cluster validation warning");
    assert!(grpc_cv.get("trace_id").is_none(), "unexpected trace_id: {grpc_cv}");
}

#[tokio::test]
async fn s2_01_s2_05_invalid_protocol_or_exporter_logs_error_and_disables_tracing() {
    for env in [
        &[
            ("OTEL_TRACES_EXPORTER", "otlp"),
            ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "bogus"),
        ][..],
        &[("OTEL_TRACES_EXPORTER", "bogus-exporter")][..],
    ] {
        let logs = run_mimir_subproc_and_collect_logs(env).await;
        assert!(
            logs.iter()
                .any(|v| v["level"] == "error" && v["msg"] == "Failed to setup tracing"),
            "expected Failed to setup tracing error log for {env:?}"
        );
        let http_404 = logs
            .iter()
            .find(|v| {
                v["msg"]
                    .as_str()
                    .is_some_and(|m| m.contains("/nonexistent-disabled (404)"))
            })
            .expect("expected HTTP 404 log");
        assert!(
            http_404.get("trace_id").is_none(),
            "tracing should remain disabled after setup error: {http_404}"
        );
        assert!(
            http_404.get("trace_id_unsampled").is_none(),
            "tracing should remain disabled after setup error: {http_404}"
        );
    }
}

fn sample_span_with_link_and_event() -> ExportedSpan {
    let mut span = ExportedSpan {
        service_name: "mimir-ingester".to_string(),
        trace_id: [0x12; 16],
        span_id: [0x34; 8],
        parent_span_id: None,
        name: "HTTP GET - ready".to_string(),
        kind: 2,
        start_time_unix_nano: 1_700_000_000_000_000_000,
        end_time_unix_nano: 1_700_000_000_002_000_000,
        attributes: vec![SpanAttribute {
            key: "http.request.method".to_string(),
            value: SpanAttrValue::String("GET".to_string()),
        }],
        status_error: false,
    };
    span.push_link(SpanLink {
        trace_id: [0xab; 16],
        span_id: [0xcd; 8],
        trace_state: Some("rojo=1".to_string()),
        attributes: Vec::new(),
    });
    span.push_event(SpanEvent {
        name: "test-event".to_string(),
        time_unix_nano: 1_700_000_000_001_000_000,
        attributes: vec![SpanAttribute {
            key: "count".to_string(),
            value: SpanAttrValue::Int(42),
        }],
    });
    span
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn s2_01_otlp_protocols_and_5s_shutdown_flush() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_all_tracing_env();

    // 1. OTLP http/json + shutdown flush of multiple queued spans.
    let json_captured: Arc<parking_lot::Mutex<Vec<(HeaderMap, Value)>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let jc = Arc::clone(&json_captured);
    let json_app = Router::new().route(
        "/v1/traces",
        axum::routing::post(move |headers: HeaderMap, body: Bytes| {
            let jc = Arc::clone(&jc);
            async move {
                let v: Value = serde_json::from_slice(&body).unwrap();
                jc.lock().push((headers, v));
                StatusCode::OK
            }
        }),
    );
    let jl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ja = jl.local_addr().unwrap();
    let jt = tokio::spawn(async move {
        axum::serve(jl, json_app).await.unwrap();
    });

    std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json");
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", format!("http://{ja}"));
    let (exp, handle) = try_build_span_exporter_with_handle().unwrap();
    let exp = exp.expect("http/json exporter");
    let handle: SpanExporterHandle = handle.expect("http/json handle");

    for _ in 0..3 {
        exp(sample_span_with_link_and_event());
    }
    handle.shutdown(Duration::from_secs(5), &NoopLogger).await;
    let items = json_captured.lock().clone();
    assert_eq!(items.len(), 3, "shutdown must flush all 3 queued spans");
    assert_eq!(items[0].0.get("content-type").unwrap(), "application/json");
    assert!(items[0].1["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["links"].is_array());
    assert!(items[0].1["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["events"].is_array());
    jt.abort();

    // 2. OTLP grpc over HTTP/2 with bare host:port endpoint.
    clear_all_tracing_env();
    let grpc_captured: Arc<parking_lot::Mutex<Vec<(String, HeaderMap, Bytes)>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let gc = Arc::clone(&grpc_captured);
    let gl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ga = gl.local_addr().unwrap();
    let gt = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = gl.accept().await else {
                break;
            };
            let gc = Arc::clone(&gc);
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let svc = hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                    let gc = Arc::clone(&gc);
                    async move {
                        let path = req.uri().path().to_string();
                        let hdrs = req.headers().clone();
                        let body = req.into_body().collect().await.unwrap().to_bytes();
                        gc.lock().push((path, hdrs, body));
                        Ok::<_, hyper::Error>(
                            http::Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc")
                                .header("grpc-status", "0")
                                .body(http_body_util::Full::new(Bytes::new()))
                                .unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc");
    std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", ga.to_string());
    let (g_exp, g_handle) = try_build_span_exporter_with_handle().unwrap();
    let g_exp = g_exp.expect("grpc exporter");
    let g_handle = g_handle.expect("grpc handle");
    g_exp(sample_span_with_link_and_event());
    g_handle.shutdown(Duration::from_secs(5), &NoopLogger).await;

    let g_items = grpc_captured.lock().clone();
    assert_eq!(g_items.len(), 1);
    assert_eq!(
        g_items[0].0,
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
    );
    assert_eq!(g_items[0].1.get("content-type").unwrap(), "application/grpc");
    assert_eq!(g_items[0].2[0], 0);
    let msg_len = u32::from_be_bytes(g_items[0].2[1..5].try_into().unwrap()) as usize;
    assert_eq!(g_items[0].2.len(), 5 + msg_len);
    gt.abort();
    clear_all_tracing_env();
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn s2_01_jaeger_http_thrift_and_udp_compact_thrift_exporters() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_all_tracing_env();

    // 1. Jaeger HTTP collector via OTEL_TRACES_EXPORTER=jaeger + OTEL_EXPORTER_JAEGER_ENDPOINT.
    let thrift_captured: Arc<parking_lot::Mutex<Vec<(HeaderMap, Bytes)>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let tc = Arc::clone(&thrift_captured);
    let app = Router::new().route(
        "/api/traces",
        axum::routing::post(move |headers: HeaderMap, body: Bytes| {
            let tc = Arc::clone(&tc);
            async move {
                tc.lock().push((headers, body));
                StatusCode::ACCEPTED
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::env::set_var("OTEL_TRACES_EXPORTER", "jaeger");
    std::env::set_var(
        "OTEL_EXPORTER_JAEGER_ENDPOINT",
        format!("http://{addr}/api/traces"),
    );
    let (exp, handle) = try_build_span_exporter_with_handle().unwrap();
    exp.unwrap()(sample_span_with_link_and_event());
    handle
        .unwrap()
        .shutdown(Duration::from_secs(5), &NoopLogger)
        .await;

    let got = thrift_captured.lock().clone();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0.get("content-type").unwrap(), "application/x-thrift");
    assert!(got[0]
        .1
        .windows("HTTP GET - ready".len())
        .any(|w| w == b"HTTP GET - ready"));
    srv.abort();

    // 2. Jaeger UDP Compact Thrift agent via JAEGER_AGENT_HOST / JAEGER_AGENT_PORT.
    clear_all_tracing_env();
    let udp_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_addr = udp_sock.local_addr().unwrap();
    std::env::set_var("JAEGER_AGENT_HOST", "127.0.0.1");
    std::env::set_var("JAEGER_AGENT_PORT", udp_addr.port().to_string());

    let (u_exp, u_handle) = try_build_span_exporter_with_handle().unwrap();
    u_exp.unwrap()(sample_span_with_link_and_event());
    u_handle
        .unwrap()
        .shutdown(Duration::from_secs(5), &NoopLogger)
        .await;

    let mut buf = [0u8; 4096];
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), udp_sock.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let pkt = &buf[..n];
    assert_eq!(pkt[0], 0x82);
    assert_eq!(pkt[1], 0x21);
    assert!(pkt.windows("emitBatch".len()).any(|w| w == b"emitBatch"));
    assert!(pkt
        .windows("HTTP GET - ready".len())
        .any(|w| w == b"HTTP GET - ready"));

    clear_all_tracing_env();
}

#[tokio::test]
async fn s2_05_create_new_traces_links_http_semconv_overflow_event_and_grpc_semconv() {
    let spans = RECORDED_SPANS.get_or_init(|| Arc::new(parking_lot::Mutex::new(Vec::new())));
    {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var("OTEL_TRACES_EXPORTER").ok();
        std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
        let sink = Arc::clone(spans);
        let _ = init_otel_or_jaeger_from_env(
            "mimir-w26-test",
            &NoopLogger,
            Some(Arc::new(move |s: ExportedSpan| {
                sink.lock().push(s);
            })),
        );
        match prev {
            Some(v) => std::env::set_var("OTEL_TRACES_EXPORTER", v),
            None => std::env::remove_var("OTEL_TRACES_EXPORTER"),
        }
    }
    spans.lock().clear();

    // 1. HTTP middleware with create_new_traces=true and >100 headers.
    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let cfg = Arc::new(HttpMiddlewareConfig {
        metrics,
        logger: Arc::new(NoopLogger),
        log_request_headers: false,
        log_request_at_info_level: false,
        log_source_ips: None,
        trace_source_ips: None,
        exclude_headers: Vec::new(),
        trace_request_headers: true,
        trace_exclude_headers: Vec::new(),
        create_new_traces: true,
        cluster_validation: None,
        http_path_prefix: String::new(),
        cost_attribution_path: None,
        compartments_enabled: false,
    });
    let app = Router::new()
        .route("/config", on(MethodFilter::GET, || async { "ok" }))
        .layer(axum::middleware::from_fn_with_state(cfg, dskit_http_middleware));

    let incoming_tid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let incoming_sid = "1111111111111111";
    let mut builder = Request::builder()
        .method(Method::GET)
        .uri("/config")
        .header("traceparent", format!("00-{incoming_tid}-{incoming_sid}-01"))
        .header("tracestate", "congo=t61rcWkgMzE");
    for i in 0..103 {
        builder = builder.header(format!("x-extra-{i:03}"), "v");
    }
    let mut req = builder.body(Body::empty()).unwrap();
    req.extensions_mut().insert(ServedOverHttp {
        remote_addr: "127.0.0.1:9999".parse().unwrap(),
    });
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = resp.into_body().collect().await.unwrap();

    let http_span = spans
        .lock()
        .iter()
        .find(|s| s.name.starts_with("HTTP GET"))
        .cloned()
        .expect("exported HTTP span");
    assert_ne!(http_span.trace_id, [0xaa; 16], "create_new_traces must start new root trace");
    assert!(http_span.parent_span_id.is_none());
    assert_eq!(http_span.links().len(), 1, "expected 1 SpanLink to incoming trace");
    assert_eq!(http_span.links()[0].trace_id, [0xaa; 16]);
    assert_eq!(http_span.links()[0].span_id, [0x11; 8]);
    assert_eq!(http_span.links()[0].trace_state.as_deref(), Some("congo=t61rcWkgMzE"));

    let has_str = |s: &ExportedSpan, k: &str, v: &str| {
        s.regular_attributes()
            .any(|a| a.key == k && matches!(&a.value, SpanAttrValue::String(val) if val == v))
    };
    let has_int = |s: &ExportedSpan, k: &str, v: i64| {
        s.regular_attributes()
            .any(|a| a.key == k && matches!(a.value, SpanAttrValue::Int(val) if val == v))
    };
    assert!(has_str(&http_span, "http.request.method", "GET"));
    assert!(has_str(&http_span, "url.path", "/config"));
    assert!(has_str(&http_span, "url.scheme", "http"));
    assert!(has_str(&http_span, "network.protocol.version", "1.1"));
    assert!(has_int(&http_span, "http.response.status_code", 200));

    let events = http_span.events();
    assert_eq!(events.len(), 1, "expected >100 headers overflow span event");
    assert_eq!(
        events[0].name,
        "Client sent too many headers, some of them were not included in the span attributes: "
    );
    assert!(events[0].attributes.iter().any(|a| {
        a.key == "headers_total" && matches!(a.value, SpanAttrValue::Int(105))
    }));
    assert!(events[0].attributes.iter().any(|a| {
        a.key == "headers_not_added_as_span_attributes"
            && matches!(&a.value, SpanAttrValue::StringArray(v) if v.len() == 5)
    }));

    // 2. gRPC server span semconv attributes (rpc.system, rpc.service, rpc.method, rpc.grpc.status_code).
    spans.lock().clear();
    let srv_cfg = ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        ..Default::default()
    };
    let grpc_reg = Registry::new();
    let server = Server::new(&srv_cfg, &grpc_reg, Arc::new(NoopLogger)).unwrap();
    let grpc_addr = server.grpc_addr().unwrap();
    let health_modules = ModuleStates::new();
    let srv_state = StateCell::new(State::Running);
    health_modules.add("server", srv_state.state_fn());
    let health = mimir_cmd::grpc::HealthService::new(Arc::new(AtomicBool::new(false)), health_modules);
    let routes = tonic::service::Routes::new(health);
    let running = server.start(Router::new(), routes).unwrap();

    let ch = Channel::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut check_req = tonic::Request::new(mimir_cmd::grpc::HealthCheckRequest {
        service: String::new(),
    });
    check_req.metadata_mut().insert(
        "traceparent",
        "00-cccccccccccccccccccccccccccccccc-2222222222222222-01"
            .parse()
            .unwrap(),
    );
    let mut g = tonic::client::Grpc::new(ch);
    g.ready().await.unwrap();
    let _: tonic::Response<mimir_cmd::grpc::HealthCheckResponse> = g
        .unary(
            check_req,
            http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check"),
            tonic::codec::ProstCodec::default(),
        )
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let grpc_span = loop {
        if let Some(s) = spans
            .lock()
            .iter()
            .find(|s| s.name == "grpc.health.v1.Health/Check")
            .cloned()
        {
            break s;
        }
        assert!(Instant::now() < deadline, "timed out waiting for gRPC span");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(has_str(&grpc_span, "rpc.system", "grpc"));
    assert!(has_str(&grpc_span, "rpc.service", "grpc.health.v1.Health"));
    assert!(has_str(&grpc_span, "rpc.method", "Check"));
    assert!(has_int(&grpc_span, "rpc.grpc.status_code", 0));
    running.shutdown().await;
}
