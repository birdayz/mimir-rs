//! Regression tests for Wave 17 Track 2 observability, CLI, config & server fixes (W17-OBS-01..08).

use std::fs;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use mimir_cmd::app::span_to_otlp_json_with_resource;
use mimir_cmd::grpc::HttpGrpcBridge;
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::{ModuleStates, StateCell};
use mimir_cmd::pprof::profile_handler;
use mimir_cmd::server::http_server::SharedWriteDeadline;
use mimir_cmd::{dump_yaml, parse_cli_for_run, run_cli_test_mode};
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::services::State;
use mimir_common::tracing::{
    build_tracing_runtime_from_env, init_otel_or_jaeger_from_env, otlp_traces_endpoint_from_env,
    parse_traceparent, ExportedSpan, SpanAttrValue,
};
use mimir_config::{
    BlocksStorageConfig, IngestStorageConfig, IngesterConfig, LimitsConfig, MimirConfig, Overrides,
    RuntimeConfigManager,
};
use mimir_ingester::ingest_storage::IngesterShardedBatchConsumer;
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::httpgrpcpb::http_server::Http;
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest};
use mimir_storage::ingest::{
    FetchedRecord, PusherConsumerMetrics, RecordSerializer, RecordVersion, SampledOnlyPropagator,
    TraceSpanContext,
};
use prometheus::Registry;
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
    std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
    std::env::set_var("OTEL_PROPAGATORS", "jaeger,tracecontext");
    let sink = Arc::clone(spans);
    let _ = init_otel_or_jaeger_from_env(
        "mimir-test",
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
    spans
}

fn parse_config_from_yaml(yaml: &str) -> MimirConfig {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mimir.yaml");
    fs::write(&path, yaml).unwrap();
    let outcome = parse_cli_for_run(
        "mimir",
        &[format!("-config.file={}", path.display())],
    );
    outcome.config.expect("parsed MimirConfig")
}

fn build_state(
    cfg: MimirConfig,
    runtime_config: Option<Arc<RuntimeConfigManager>>,
    ingester: Option<Arc<Ingester>>,
) -> AppHttpState {
    let modules = ModuleStates::new();
    modules.add("server", StateCell::new(State::Running).state_fn());
    let overrides = Overrides::new(cfg.limits.clone());
    AppHttpState {
        config: Arc::new(cfg),
        default_config: Arc::new(MimirConfig::default()),
        registry: Registry::new(),
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester,
        runtime_config,
        overrides,
        memberlist_kv: None,
        modules,
        cost_attribution_registry: None,
    }
}

/// W17-OBS-01: AWS MSK IAM access_key & session_token must be masked as ********
/// across all 4 usage_tracker Kafka blocks in -print.config, GET /config, and GET /config?mode=diff.
#[tokio::test]
async fn w17_obs_01_usage_tracker_msk_iam_secrets_are_redacted() {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    let session_tok = "IQoJb3JpZ2luX2VjEJrExampleSessionTokenValue";

    let cfg = parse_config_from_yaml(&format!(
        r#"
target: ingester
usage_tracker:
  events_storage_writer:
    sasl_msk_iam_access_key: {access_key}
    sasl_msk_iam_secret_key: {secret_key}
    sasl_msk_iam_session_token: {session_tok}
  events_storage_reader:
    sasl_msk_iam_access_key: {access_key}
    sasl_msk_iam_secret_key: {secret_key}
    sasl_msk_iam_session_token: {session_tok}
  snapshots_metadata_writer:
    sasl_msk_iam_access_key: {access_key}
    sasl_msk_iam_secret_key: {secret_key}
    sasl_msk_iam_session_token: {session_tok}
  snapshots_metadata_reader:
    sasl_msk_iam_access_key: {access_key}
    sasl_msk_iam_secret_key: {secret_key}
    sasl_msk_iam_session_token: {session_tok}
"#
    ));

    let dumped = dump_yaml(&cfg);
    assert!(!dumped.contains(access_key), "dump_yaml leaked access_key:\n{dumped}");
    assert!(!dumped.contains(secret_key), "dump_yaml leaked secret_key:\n{dumped}");
    assert!(!dumped.contains(session_tok), "dump_yaml leaked session_token:\n{dumped}");
    assert_eq!(dumped.matches("********").count(), 12, "expected 12 masked MSK secrets:\n{dumped}");

    let router = build_http_router(build_state(cfg, None, None));
    for uri in ["/config", "/config?mode=diff"] {
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = String::from_utf8(
            resp.into_body().collect().await.unwrap().to_bytes().to_vec(),
        )
        .unwrap();
        assert!(!body.contains(access_key), "{uri} leaked access_key:\n{body}");
        assert!(!body.contains(secret_key), "{uri} leaked secret_key:\n{body}");
        assert!(!body.contains(session_tok), "{uri} leaked session_token:\n{body}");
        assert!(body.contains("********"), "{uri} missing secret mask:\n{body}");
    }
}

/// W17-OBS-02: SharedWriteDeadline::extend_by and profile_handler must not panic on
/// large ?seconds=9223372036854775807.
#[tokio::test]
async fn w17_obs_02_large_pprof_seconds_does_not_overflow_instant() {
    let wd = SharedWriteDeadline::new(Duration::from_secs(120));
    assert!(wd.current().is_some());
    wd.extend_by(Duration::from_secs(i64::MAX as u64));
    assert!(
        wd.current().is_none(),
        "overflowing deadline extension must disable write deadline (None) instead of panicking"
    );

    let wd_for_req = SharedWriteDeadline::new(Duration::from_secs(120));
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/debug/pprof/profile?seconds=9223372036854775807")
        .body(Body::empty())
        .unwrap();
    req.extensions_mut().insert(wd_for_req.clone());

    // profile_handler only needs to get past configureWriteDeadline without panicking;
    // cancel the long sleep after a short wait.
    let _ = tokio::time::timeout(Duration::from_millis(20), profile_handler(req)).await;
    assert!(
        wd_for_req.current().is_none(),
        "profile_handler should clear write deadline on overflow without panicking"
    );
}

/// W17-OBS-03: GET /config?mode=diff and GET /runtime_config?mode=diff must normalize via
/// order_like_go_config / order_like_go_type before diffing.
#[tokio::test]
async fn w17_obs_03_config_and_runtime_config_diff_normalize_like_go() {
    let dir = tempdir().unwrap();
    let rt_path = dir.path().join("runtime.yaml");
    fs::write(
        &rt_path,
        r#"
overrides:
  tenant-a:
    ingestion_rate: 5000
"#,
    )
    .unwrap();

    let cfg = parse_config_from_yaml(&format!(
        r#"
target: ingester
limits:
  metric_relabel_configs:
    - regex: "foo.*"
      action: drop
vault:
  auth:
    approle:
      role_id: my-role
alertmanager:
  tenants_exempt_from_config_validation_for_existing_config:
    - t1
    - t2
runtime_config:
  file: {}
"#,
        rt_path.display()
    ));

    let rt_mgr = RuntimeConfigManager::new(
        cfg.runtime_config.clone(),
        cfg.limits.clone(),
        Default::default(),
    )
    .expect("runtime config manager");
    let router = build_http_router(build_state(cfg, Some(rt_mgr), None));

    // 1. /config?mode=diff
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/config?mode=diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cfg_diff =
        String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(
        !cfg_diff.contains("modulus:") && !cfg_diff.contains("source_labels:") && !cfg_diff.contains("target_label:"),
        "MetricRelabelConfig zero omitempty fields leaked into /config?mode=diff:\n{cfg_diff}"
    );
    assert!(
        cfg_diff.contains(r#"secret_id: """#),
        "vault.auth.approle zero struct pruning should expose secret_id in /config?mode=diff:\n{cfg_diff}"
    );
    assert!(
        cfg_diff.contains("tenants_exempt_from_config_validation_for_existing_config: t1,t2"),
        "StringSliceCSVMulti field should diff as comma-joined string:\n{cfg_diff}"
    );

    // 2. /runtime_config?mode=diff
    let resp = router
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/runtime_config?mode=diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let rt_diff =
        String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(
        rt_diff.contains("ingestion_rate: 5000"),
        "expected ingestion_rate override in /runtime_config?mode=diff:\n{rt_diff}"
    );
    assert!(
        !rt_diff.contains("metric_relabel_configs"),
        "spurious metric_relabel_configs in /runtime_config?mode=diff:\n{rt_diff}"
    );
}

/// W17-OBS-04: Kafka ingest_storage always extracts W3C TraceContext (even if OTEL_PROPAGATORS=jaeger),
/// preserves trace_id on series-only batches without metadata, and logs canonical tenant_id + trace_id.
#[test]
fn w17_obs_04_ingest_storage_trace_and_error_log_parity() {
    let _spans = ensure_recording_tracing();

    // 1. SampledOnlyPropagator preserves tracestate & 0x02/0x03 flags and rejects 0x04/0xff/uppercase.
    let prop = SampledOnlyPropagator;
    let tc = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03").unwrap();
    assert_eq!(tc.to_traceparent(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03");
    let mut tc_with_state = tc.clone();
    tc_with_state.tracestate = Some("rojo=00f067aa0ba902b7".to_string());
    let mut headers = Vec::new();
    prop.inject_trace_context(Some(&tc_with_state), &mut headers);
    let extracted = prop.extract_trace_context(&headers).unwrap();
    assert_eq!(extracted.tracestate.as_deref(), Some("rojo=00f067aa0ba902b7"));
    assert_eq!(extracted.trace_flags, 0x03);
    assert!(prop.extract(&[("traceparent".into(), Bytes::from_static(b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff"))]).is_none());
    assert!(prop.extract(&[("traceparent".into(), Bytes::from_static(b"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"))]).is_none());
    let sc = TraceSpanContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").unwrap();
    assert!(sc.sampled);

    // 2. End-to-end IngesterShardedBatchConsumer on series-only record with tenant metadata suffix
    //    ("123456:product=k6") and W3C traceparent.
    let tmp = tempdir().unwrap();
    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_id = "test-0".to_string();
    cfg.error_sample_rate = 1;
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
    let overrides = Overrides::new(LimitsConfig::default());
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
        overrides,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_state_for_test(State::Running);

    let log_sink = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let reg = Registry::new();
    let consumer = IngesterShardedBatchConsumer::new(
        Arc::clone(&ing),
        IngestStorageConfig::default().kafka,
        PusherConsumerMetrics::new(Some(&reg)),
    )
    .with_error_log_sink(Arc::clone(&log_sink));

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    let traceparent = format!("00-{trace_id}-00f067aa0ba902b7-01");

    // Seed an initial sample so the second record triggers sample_out_of_order (client error).
    let req1 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: "__name__".into(),
                value: Bytes::from_static(b"up"),
            }],
            samples: vec![cortexpb::Sample {
                timestamp_ms: now_ms,
                value: 1.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let req2 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: "__name__".into(),
                value: Bytes::from_static(b"up"),
            }],
            samples: vec![cortexpb::Sample {
                timestamp_ms: now_ms + 3_600_000_000,
                value: 2.0,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let ser = RecordSerializer::new(RecordVersion::V2);
    let (r1, _) = ser.to_records("t", 0, "123456:product=k6", &req1, 16 << 20).unwrap();
    let (r2, _) = ser.to_records("t", 0, "123456:product=k6", &req2, 16 << 20).unwrap();

    let rec1 = FetchedRecord {
        key: r1[0].key.clone(),
        value: r1[0].value.clone(),
        headers: r1[0].headers.clone(),
        offset: 1,
        timestamp_ms: 0,
    };
    consumer.consume_records_sync(&[rec1]).unwrap();

    let mut headers2 = r2[0].headers.clone();
    headers2.push(("traceparent".to_string(), Bytes::from(traceparent)));
    let rec2 = FetchedRecord {
        key: r2[0].key.clone(),
        value: r2[0].value.clone(),
        headers: headers2,
        offset: 2,
        timestamp_ms: 0,
    };
    consumer.consume_records_sync(&[rec2]).unwrap();

    let client_errs: f64 = reg
        .gather()
        .into_iter()
        .find(|mf| mf.get_name() == "cortex_ingest_storage_reader_requests_failed_total")
        .and_then(|mf| mf.get_metric().first().map(|m| m.get_counter().get_value()))
        .unwrap_or(-1.0);
    let logs = log_sink.lock();
    assert_eq!(logs.len(), 1, "expected 1 client error log (client_err_requests={client_errs}), got: {logs:?}");
    let line = &logs[0];
    assert!(line.contains("user=123456 "), "expected canonical tenant_id in log: {line}");
    assert!(!line.contains("123456:product=k6"), "raw tenant_key leaked into log: {line}");
    assert!(
        line.contains(&format!("trace_id={trace_id} ")),
        "missing sampled trace_id in series-only client error log: {line}"
    );
}

/// W17-OBS-05: -server.http-listen-port=0 -server.grpc-listen-port=0 must stay 0 in
/// GET /config and GET /config?mode=diff instead of exposing ephemeral bound ports.
#[tokio::test]
async fn w17_obs_05_ephemeral_zero_ports_preserved_in_config_endpoint() {
    let bin = env!("CARGO_BIN_EXE_mimir");
    let dir = tempdir().unwrap();
    let config_path = dir.path().join("mimir.yaml");
    fs::write(
        &config_path,
        format!(
            r#"
target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: 0
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: 0
ingester:
  ring:
    kvstore:
      store: inmemory
    instance_id: ingester-0
    instance_addr: 127.0.0.1
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {}
  tsdb:
    dir: {}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
            dir.path().join("blocks").display(),
            dir.path().join("tsdb").display()
        ),
    )
    .unwrap();

    let mut child = Command::new(bin)
        .arg(format!("-config.file={}", config_path.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mimir");

    let stderr = child.stderr.take().expect("stderr");
    let (tx, rx) = std::sync::mpsc::channel::<u16>();
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("server listening on addresses") {
                if let Some(rest) = line.split("http=127.0.0.1:").nth(1) {
                    if let Some(port_s) = rest.split_whitespace().next() {
                        if let Ok(port) = port_s.parse::<u16>() {
                            let _ = tx.send(port);
                        }
                    }
                }
            }
        }
    });

    let http_port = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("timed out waiting for bound http port log");

    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(resp) = client
            .get(format!("http://127.0.0.1:{http_port}/ready"))
            .send()
            .await
        {
            if resp.status().as_u16() == 200 {
                break;
            }
        }
        assert!(Instant::now() < deadline, "did not become ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let cfg_body = client
        .get(format!("http://127.0.0.1:{http_port}/config"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(cfg_body.contains("http_listen_port: 0
"), "expected http_listen_port: 0 in /config:\n{cfg_body}");
    assert!(cfg_body.contains("grpc_listen_port: 0
"), "expected grpc_listen_port: 0 in /config:\n{cfg_body}");

    let diff_body = client
        .get(format!("http://127.0.0.1:{http_port}/config?mode=diff"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(diff_body.contains("http_listen_port: 0
"), "expected http_listen_port: 0 in /config?mode=diff:\n{diff_body}");
    assert!(diff_body.contains("grpc_listen_port: 0
"), "expected grpc_listen_port: 0 in /config?mode=diff:\n{diff_body}");

    drop(client);
    #[cfg(unix)]
    {
        let pid = child.id().to_string();
        let _ = Command::new("kill").args(["-TERM", &pid]).status();
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(0));
}

/// W17-OBS-06: Hidden Go CLI flags (-usage-tracker.*, -block-builder.scheduler.grpc-client-config.*,
/// -distributor.usage-tracker-client.*) are registered, typed/validated like Go, and stay hidden from -help-all.
#[test]
fn w17_obs_06_hidden_cli_flags_registered_validated_and_hidden_from_help() {
    let dir = tempdir().unwrap();
    let cfg_path = dir.path().join("mimir.yaml");
    fs::write(&cfg_path, "target: ingester
").unwrap();
    let cfg_arg = format!("-config.file={}", cfg_path.display());

    let ok = run_cli_test_mode(
        "mimir",
        &[
            cfg_arg.clone(),
            "-usage-tracker.idle-timeout=15m".to_string(),
            "-usage-tracker.use-global-series-limits=true".to_string(),
            "-block-builder.scheduler.grpc-client-config.connect-timeout=3s".to_string(),
            "-distributor.usage-tracker-client.grpc-client-config.backoff-retries=5".to_string(),
            "-distributor.usage-tracker-client.batch-delay=500ms".to_string(),
            "-usage-tracker.snapshots-storage.s3.part-size=10485760".to_string(),
            "-print.config".to_string(),
        ],
    );
    assert_eq!(ok.exit_code, 0, "expected exit 0, stderr:\n{}", ok.stderr);
    assert!(ok.stdout.contains("idle_timeout: 15m"));
    assert!(ok.stdout.contains("part_size: 10485760"));
    assert!(ok.stdout.contains("batch_delay: 500ms"));

    for bad_flag in [
        "-usage-tracker.snapshots-storage.s3.part-size=abc",
        "-usage-tracker.snapshots-storage.s3.http.idle-conn-timeout=notaduration",
        "-usage-tracker.idle-timeout=bad",
        "-block-builder.scheduler.grpc-client-config.backoff-retries=notanint",
    ] {
        let bad = run_cli_test_mode("mimir", &[cfg_arg.clone(), bad_flag.to_string()]);
        assert_eq!(bad.exit_code, 2, "expected exit 2 for {bad_flag}, stderr: {}", bad.stderr);
        assert!(
            bad.stderr.contains("parse error"),
            "expected Go parse error for {bad_flag}, got: {}",
            bad.stderr
        );
    }

    let help_all = run_cli_test_mode("mimir", &["-help-all".to_string()]);
    assert_eq!(help_all.exit_code, 2);
    assert!(!help_all.stderr.contains("-usage-tracker.idle-timeout"));
    assert!(!help_all.stderr.contains("-block-builder.instance-id"));
}

/// W17-OBS-07: W3C traceparent flags/validation, HttpGrpcBridge parent_span_id + route + attrs,
/// JAEGER_TAGS resource attributes, jaeger-debug-id span attribute, and default OTLP endpoint.
#[tokio::test]
async fn w17_obs_07_traceparent_httpgrpc_and_otel_jaeger_parity() {
    let spans = ensure_recording_tracing();

    // 1. parse_traceparent reserved v0 flags & FlagsRandom (0x02) preservation.
    assert!(parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-04").is_none());
    assert!(parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff").is_none());
    let tc02 = parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02").unwrap();
    assert!(!tc02.is_sampled());
    assert_eq!(tc02.to_traceparent(), "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02");

    // 2. HttpGrpcBridge child-span parent_span_id, span name, and HTTP attributes.
    let router = build_http_router(build_state(MimirConfig::default(), None, None));
    let bridge = HttpGrpcBridge::new(router);

    spans.lock().unwrap_or_else(|e| e.into_inner()).clear();
    let parent_tid = "4bf92f3577b34da6a3ce929d0e0e4736";
    let parent_sid = "00f067aa0ba902b7";
    let resp = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/ready".to_string(),
            headers: vec![
                HttpGrpcHeader {
                    key: "traceparent".to_string(),
                    values: vec![format!("00-{parent_tid}-{parent_sid}-01")],
                },
                HttpGrpcHeader {
                    key: "User-Agent".to_string(),
                    values: vec!["w17-test/1.0".to_string()],
                },
            ],
            body: Bytes::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.code, 200);

    // Also test jaeger-debug-id propagation on uber-trace-id debug flag (0x02).
    let dbg_resp = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/ready".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "uber-trace-id".to_string(),
                values: vec!["70e8b02d28e4337b:70e8b02d28e4337c:0:3".to_string()],
            }],
            body: Bytes::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dbg_resp.code, 200);

    let notfound_err = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/no-such-w17-route".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "traceparent".to_string(),
                values: vec![format!("00-{parent_tid}-{parent_sid}-01")],
            }],
            body: Bytes::new(),
        }))
        .await
        .unwrap_err();
    assert_eq!(notfound_err.code(), tonic::Code::NotFound);

    let recorded = spans.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(recorded.len(), 3, "expected 3 exported spans");

    let s0 = &recorded[0];
    assert_eq!(
        s0.parent_span_id,
        Some([0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]),
        "HttpGrpcBridge dropped inner traceparent parent_span_id"
    );
    assert_eq!(s0.name, "HTTP GET - ready");
    let has_attr = |span: &ExportedSpan, k: &str, v: &str| {
        span.attributes.iter().any(|a| {
            a.key == k && matches!(&a.value, SpanAttrValue::String(s) if s == v)
        })
    };
    assert!(has_attr(s0, "http.request.method", "GET"));
    assert!(has_attr(s0, "http.route", "ready"));
    assert!(has_attr(s0, "user_agent.original", "w17-test/1.0"));
    assert!(has_attr(s0, "url.path", "/ready"));

    let s1 = &recorded[1];
    assert!(
        has_attr(s1, "jaeger-debug-id", "1"),
        "expected jaeger-debug-id span attribute, got: {:?}",
        s1.attributes
    );

    let s2 = &recorded[2];
    assert_eq!(s2.name, "HTTP GET - notfound");
    assert!(has_attr(s2, "http.route", "notfound"));

    // 3. JAEGER_TAGS resource attributes & default OTLP endpoint when OTEL_TRACES_EXPORTER=otlp.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("JAEGER_ENDPOINT", "http://127.0.0.1:14268/api/traces");
    std::env::set_var("JAEGER_TAGS", "cluster=prod,region=eu");
    let rt = build_tracing_runtime_from_env("mimir-ingester", &NoopLogger, None).unwrap();
    let otlp = span_to_otlp_json_with_resource(s0, &rt.resource_attributes);
    let res_attrs = otlp["resourceSpans"][0]["resource"]["attributes"]
        .as_array()
        .unwrap();
    assert!(res_attrs.iter().any(|a| a["key"] == "cluster" && a["value"]["stringValue"] == "prod"));
    assert!(res_attrs.iter().any(|a| a["key"] == "region" && a["value"]["stringValue"] == "eu"));

    std::env::remove_var("JAEGER_ENDPOINT");
    std::env::remove_var("JAEGER_TAGS");
    std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
    assert_eq!(
        otlp_traces_endpoint_from_env().as_deref(),
        Some("http://localhost:4318/v1/traces")
    );
    std::env::remove_var("OTEL_TRACES_EXPORTER");
}

/// W17-OBS-08: GET / admin index page lists Partition ring status before Ingester Tenants
/// before Ring status when ingest_storage.enabled=true.
#[tokio::test]
async fn w17_obs_08_admin_index_orders_partition_ring_before_ingester_tenants() {
    let dir = tempdir().unwrap();
    let mut cfg = MimirConfig::default();
    cfg.ingester.ring.kvstore.store = "inmemory".to_string();
    cfg.ingester.partition_ring.kvstore.store = "inmemory".to_string();
    cfg.ingester.ring.instance_id = "ingester-0".to_string();
    cfg.ingester.ring.instance_addr = "127.0.0.1".to_string();
    cfg.ingest_storage.enabled = true;
    cfg.blocks_storage.bucket.backend_cfg.backend = "filesystem".to_string();
    cfg.blocks_storage.bucket.backend_cfg.filesystem.dir =
        dir.path().join("blocks").to_string_lossy().into_owned();
    cfg.blocks_storage.tsdb.dir = dir.path().join("tsdb").to_string_lossy().into_owned();

    let kv_client = mimir_ring::kv::build_kv_client::<Registry>(
        &cfg.ingester.partition_ring.kvstore,
        mimir_ring::partition_ring::partition_ring_desc_codec(),
        None,
        None,
    )
    .unwrap();
    let pr_watcher = mimir_ring::partition_ring::PartitionRingWatcher::new(
        "ingester-partitions",
        "ingester-partitions",
        kv_client,
        mimir_ring::partition_ring::PartitionRingOptions::default(),
        None,
    );
    let ing = Ingester::new(
        cfg.ingester.clone(),
        cfg.blocks_storage.clone(),
        cfg.ingest_storage.clone(),
        Overrides::new(cfg.limits.clone()),
        None,
        None,
        Some(pr_watcher),
        None,
        None,
        None,
    )
    .unwrap();
    let router = build_http_router(build_state(cfg, None, Some(ing)));
    let resp = router
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let html = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    let p_part = html.find("Partition ring status").expect("Partition ring status link");
    let p_tenants = html.find("Ingester Tenants").expect("Ingester Tenants link");
    assert!(
        p_part < p_tenants,
        "expected Partition ring status before Ingester Tenants in GET /:\n{html}"
    );
}
