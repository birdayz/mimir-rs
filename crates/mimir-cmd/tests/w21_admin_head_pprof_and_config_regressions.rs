//! Wave 21 regression tests for `mimir-cmd`, `mimir-config`, and `mimir-ring`:
//! - W21-DF-01 (cmd): non-client-streaming gRPC cardinality (0 frames & 2+ frames) across all
//!   services (`httpgrpc.HTTP`, `grpc.health.v1.Health`, `cortex.Ingester`) and encodings
//!   (`identity`, `gzip`, `zstd`, `snappy`, `s2`) before handler execution
//! - W21-DF-08 (cmd): pre-body `TapHandle` admission runs before `RpcGuard`, `auth_rejection`,
//!   and `check_cluster`, returning `Code::Unavailable` with zero `cortex_request_duration_seconds` samples
//! - W21-APS-01: Gorilla `mux` `GET` routes reject `HEAD` with 405 (`route="other"`, no `Allow`)
//!   while `/ready`, `/metrics`, and `/debug/pprof/*` keep `HEAD` enabled
//! - W21-APS-02 & W21-APS-08: immediate 500 on concurrent `/debug/pprof/profile`, bounded
//!   `seconds` + `strconv.Atoi` overflow parity on `/debug/fgprof`, no dead `goroutine?debug=2` link
//! - W21-APS-03: interruptible `RuntimeConfigManager` reloader + `runtime-config` start/stop logs
//! - W21-APS-05: structured `-target=""` / `-target=server` fatal logs + `-target=memberlist-kv`
//!   TSDB support gating
//! - W21-APS-07: `RawScopeOrgId` on `GET /api/v1/user_limits` over `httpgrpc`

use std::fs;
use std::io::{BufRead, BufReader, Write as _};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use bytes::{BufMut, Bytes, BytesMut};
use http_body_util::BodyExt as _;
use mimir_cmd::grpc::{
    GrpcCompressionEchoService, HealthCheckRequest, HealthService, HttpGrpcBridge,
    HttpGrpcStatusFixService,
};
use mimir_cmd::http::{build_http_router, AppHttpState};
use mimir_cmd::modules::{ModuleStates, StateCell};
use mimir_cmd::pprof::{fgprof_handler, index_handler, profile_handler};
use mimir_cmd::server::Server;
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::services::State;
use mimir_config::{
    LimitsConfig, MimirConfig, Overrides, RuntimeConfigManager, ServerConfig,
};
use mimir_ingester::grpc::SnappyDecompressService;
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb::extract_http_response;
use mimir_proto::httpgrpcpb::http_server::{Http, HttpServer};
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest};
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message as _;

use prometheus::Registry;
use tempfile::tempdir;
use tonic::codec::CompressionEncoding;
use tower::ServiceExt as _;

#[derive(Default)]
struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
}

fn frame_grpc(flag: u8, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(flag);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

fn compress_payload(enc: &str, raw: &[u8]) -> Vec<u8> {
    match enc {
        "gzip" => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(raw).unwrap();
            encoder.finish().unwrap()
        }
        "zstd" => zstd::stream::encode_all(raw, 0).unwrap(),
        "snappy" => mimir_proto::snappy::compress_framed(raw).unwrap(),
        "s2" => {
            let mut out = mimir_proto::snappy::compress_framed(raw).unwrap();
            if out.len() >= 10 {
                out[4..10].copy_from_slice(b"S2sTwO");
            }
            out
        }
        _ => raw.to_vec(),
    }
}

fn encoded_frame(enc: Option<&str>, raw: &[u8]) -> Bytes {
    match enc {
        None | Some("identity") => frame_grpc(0, raw),
        Some(e) => frame_grpc(1, &compress_payload(e, raw)),
    }
}

async fn extract_grpc_status(resp: http::Response<tonic::body::BoxBody>) -> tonic::Status {
    if let Some(st) = tonic::Status::from_header_map(resp.headers()) {
        return st;
    }
    let collected = resp.into_body().collect().await.unwrap();
    let trailers = collected.trailers().cloned().unwrap_or_else(HeaderMap::new);
    tonic::Status::from_header_map(&trailers).unwrap_or_else(|| tonic::Status::ok(""))
}

fn make_test_ingester(dir: &std::path::Path) -> Arc<Ingester> {
    let mut cfg = mimir_config::IngesterConfig::default();
    cfg.ring.instance_id = "w21-test-ing".to_string();
    cfg.push_grpc_method_enabled = true;
    let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.to_string_lossy().into_owned();
    let ing = Ingester::new(
        cfg,
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
    .unwrap();
    ing.set_state_for_test(State::Running);
    ing
}

#[tokio::test]
async fn w21_df_01_non_client_streaming_grpc_cardinality_all_services_and_encodings() {
    let max_recv = 1024usize;
    let max_send = 4 << 20;

    let tmp = tempdir().unwrap();
    let ing = make_test_ingester(tmp.path());
    let ing_svc = GrpcCompressionEchoService::new_with_limits(
        IngesterService::new(Arc::clone(&ing))
            .into_server()
            .max_decoding_message_size(max_recv)
            .max_encoding_message_size(usize::MAX)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd),
        max_recv,
        max_send,
    );

    let modules = ModuleStates::new();
    let cell = StateCell::new(State::Running);
    modules.add("server", cell.state_fn());
    let shutdown = Arc::new(AtomicBool::new(false));
    let health_svc = GrpcCompressionEchoService::new_with_limits(
        SnappyDecompressService::new(
            HealthService::new(shutdown, modules)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            max_recv,
        ),
        max_recv,
        max_send,
    );

    let http_svc = GrpcCompressionEchoService::new_with_limits(
        HttpGrpcStatusFixService::new(SnappyDecompressService::new(
            HttpServer::new(HttpGrpcBridge::new(
                Router::new().route("/ok", get(|| async { "ok" })),
            ))
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX),
            max_recv,
        )),
        max_recv,
        max_send,
    );

    let push_payload = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: Bytes::from_static(b"__name__"),
                value: Bytes::from_static(b"w21_df_01_metric"),
            }],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: 1_700_000_000_000,
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    let query_payload = ingesterpb::QueryRequest {
        start_timestamp_ms: 1_699_999_000_000,
        end_timestamp_ms: 1_700_001_000_000,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".into(),
            value: "w21_df_01_metric".into(),
        }],
        streaming_chunks_batch_size: 16,
    }
    .encode_to_vec();
    let health_payload = HealthCheckRequest {
        service: String::new(),
    }
    .encode_to_vec();
    let httpgrpc_payload = HttpRequest {
        method: "GET".to_string(),
        url: "/ok".to_string(),
        body: Bytes::new(),
        headers: Vec::new(),
    }
    .encode_to_vec();

    let empty_msg_err = "cardinality violation: received no request message from non-client-streaming RPC";
    let multi_msg_err = "cardinality violation: received multiple request messages for non-client-streaming RPC";

    for enc in [None, Some("identity"), Some("gzip"), Some("zstd"), Some("snappy"), Some("s2")] {
        // 1. 0 frames across all services & encodings.
        for (svc, uri, extra_hdr) in [
            (health_svc.clone().boxed_clone(), "/grpc.health.v1.Health/Check", None),
            (http_svc.clone().boxed_clone(), "/httpgrpc.HTTP/Handle", None),
            (ing_svc.clone().boxed_clone(), "/cortex.Ingester/Push", Some(("x-scope-orgid", "t1"))),
            (ing_svc.clone().boxed_clone(), "/cortex.Ingester/QueryStream", Some(("x-scope-orgid", "t1"))),
        ] {
            let mut b = http::Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/grpc");
            if let Some(e) = enc {
                b = b.header("grpc-encoding", e);
            }
            if let Some((hk, hv)) = extra_hdr {
                b = b.header(hk, hv);
            }
            let req = b
                .body(tonic::body::BoxBody::new(
                    http_body_util::Empty::<Bytes>::new().map_err(|never| match never {}),
                ))
                .unwrap();
            let st = extract_grpc_status(svc.oneshot(req).await.unwrap()).await;
            assert_eq!(st.code(), tonic::Code::Internal, "0-frame {uri} {enc:?}: {st:?}");
            assert_eq!(st.message(), empty_msg_err, "0-frame {uri} {enc:?}");
        }

        // 2. 2 valid frames across all services & encodings -> Internal cardinality violation,
        // and Push must NOT ingest frame 1!
        for (svc, uri, payload, extra_hdr) in [
            (
                health_svc.clone().boxed_clone(),
                "/grpc.health.v1.Health/Check",
                health_payload.as_slice(),
                None,
            ),
            (
                http_svc.clone().boxed_clone(),
                "/httpgrpc.HTTP/Handle",
                httpgrpc_payload.as_slice(),
                None,
            ),
            (
                ing_svc.clone().boxed_clone(),
                "/cortex.Ingester/Push",
                push_payload.as_slice(),
                Some(("x-scope-orgid", "t-card")),
            ),
            (
                ing_svc.clone().boxed_clone(),
                "/cortex.Ingester/QueryStream",
                query_payload.as_slice(),
                Some(("x-scope-orgid", "t-card")),
            ),
        ] {
            let f1 = encoded_frame(enc, payload);
            let f2 = encoded_frame(enc, payload);
            let mut two = BytesMut::with_capacity(f1.len() + f2.len());
            two.extend_from_slice(&f1);
            two.extend_from_slice(&f2);

            let mut b = http::Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/grpc");
            if let Some(e) = enc {
                b = b.header("grpc-encoding", e);
            }
            if let Some((hk, hv)) = extra_hdr {
                b = b.header(hk, hv);
            }
            let req = b
                .body(tonic::body::BoxBody::new(
                    http_body_util::Full::new(two.freeze()).map_err(|never| match never {}),
                ))
                .unwrap();
            let st = extract_grpc_status(svc.oneshot(req).await.unwrap()).await;
            assert_eq!(st.code(), tonic::Code::Internal, "2-frame {uri} {enc:?}: {st:?}");
            assert_eq!(st.message(), multi_msg_err, "2-frame {uri} {enc:?}");
        }
    }

    // Confirm no Push with 2 frames ever executed frame 1.
    assert!(
        ing.get_tsdb("t-card").is_none(),
        "frame 1 must never execute on 2-frame Push cardinality violation"
    );

    // 3. Two snappy/s2 frames each <= max_recv (600B each, total 1210 > max_recv=1024) must fail
    // with Internal cardinality violation, NOT cumulative ResourceExhausted!
    for enc in ["snappy", "s2"] {
        // Pseudo-random incompressible bytes so each compressed snappy/s2 frame is ~600B
        // (<= max_recv=1024 per frame, > max_recv=1024 cumulatively).
        let mut raw_600 = Vec::with_capacity(600);
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..600 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            raw_600.push((x >> 56) as u8);
        }
        let f1 = encoded_frame(Some(enc), &raw_600);
        let f2 = encoded_frame(Some(enc), &raw_600);
        assert!(f1.len() <= max_recv + 5 && f2.len() <= max_recv + 5);
        assert!(f1.len() + f2.len() > max_recv + 5);
        let mut two = BytesMut::with_capacity(f1.len() + f2.len());
        two.extend_from_slice(&f1);
        two.extend_from_slice(&f2);

        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/cortex.Ingester/Push")
            .header(header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", enc)
            .header("x-scope-orgid", "t-snappy-two")
            .body(tonic::body::BoxBody::new(
                http_body_util::Full::new(two.freeze()).map_err(|never| match never {}),
            ))
            .unwrap();
        let st = extract_grpc_status(ing_svc.clone().oneshot(req).await.unwrap()).await;
        assert_eq!(st.code(), tonic::Code::Internal, "{enc}: {st:?}");
        assert_eq!(st.message(), multi_msg_err);
    }

    // 4. Frame 1 valid + Frame 2 exceeding max_recv -> ResourceExhausted without executing Frame 1!
    let huge = vec![b'A'; 4096];
    let f1 = encoded_frame(Some("gzip"), &push_payload);
    let f2 = encoded_frame(Some("gzip"), &huge);
    let mut two = BytesMut::with_capacity(f1.len() + f2.len());
    two.extend_from_slice(&f1);
    two.extend_from_slice(&f2);
    let req = http::Request::builder()
        .method(Method::POST)
        .uri("/cortex.Ingester/Push")
        .header(header::CONTENT_TYPE, "application/grpc")
        .header("grpc-encoding", "gzip")
        .header("x-scope-orgid", "t-oversized-f2")
        .body(tonic::body::BoxBody::new(
            http_body_util::Full::new(two.freeze()).map_err(|never| match never {}),
        ))
        .unwrap();
    let st = extract_grpc_status(ing_svc.clone().oneshot(req).await.unwrap()).await;
    assert_eq!(st.code(), tonic::Code::ResourceExhausted);
    assert!(ing.get_tsdb("t-oversized-f2").is_none());

    let _ = ing.stop().await;
}



#[tokio::test]
async fn w21_df_08_tap_handle_runs_before_rpc_guard_auth_and_cluster_validation() {
    let dir = tempdir().unwrap();
    let mut ing_cfg = mimir_config::IngesterConfig::default();
    ing_cfg.ring.instance_id = "w21-tap".to_string();
    ing_cfg.push_grpc_method_enabled = true;
    let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
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
    // Start in State::New (not ready) -> TapHandle must reject with Unavailable!

    let reg = Registry::new();
    let mut srv_cfg = ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        ..Default::default()
    };
    srv_cfg.cluster_validation.label = "expected-cluster".to_string();
    srv_cfg.cluster_validation.grpc.enabled = true;

    let mut server = Server::new(&srv_cfg, &reg, Arc::new(NoopLogger)).unwrap();
    server.set_grpc_multitenancy(true, "anonymous");
    server.set_grpc_tap_ingester(Arc::clone(&ing));
    let grpc_addr = server.grpc_addr().unwrap();

    let ing_svc = GrpcCompressionEchoService::new(IngesterService::new(Arc::clone(&ing)).into_server());
    let routes = tonic::service::Routes::new(ing_svc);
    let running = server.start(Router::new(), routes).unwrap();

    let ep = format!("http://{grpc_addr}");
    let mut client = ingesterpb::ingester_client::IngesterClient::connect(ep).await.unwrap();

    // Trigger A1: Ingester not ready + missing x-scope-orgid on read RPC -> Unavailable (ahead of Unknown "no org id").
    let err = client
        .label_names(ingesterpb::LabelNamesRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "got {err:?}");

    // Trigger A2: Ingester not ready + wrong x-cluster on Push -> Unavailable (ahead of FailedPrecondition).
    let mut push_req = tonic::Request::new(cortexpb::WriteRequest::default());
    push_req
        .metadata_mut()
        .insert("x-scope-orgid", "t1".parse().unwrap());
    push_req
        .metadata_mut()
        .insert("x-cluster", "wrong-cluster".parse().unwrap());
    let err = client.push(push_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "got {err:?}");

    // Trigger B: Valid Push while ingester is Stopping -> Unavailable from IngesterZeroCopyServer tap,
    // and cortex_request_duration_seconds STILL has 0 samples!
    ing.set_state_for_test(State::Stopping);
    let mut push_ok_hdrs = tonic::Request::new(cortexpb::WriteRequest::default());
    push_ok_hdrs
        .metadata_mut()
        .insert("x-scope-orgid", "t1".parse().unwrap());
    push_ok_hdrs
        .metadata_mut()
        .insert("x-cluster", "expected-cluster".parse().unwrap());
    let err = client.push(push_ok_hdrs).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "got {err:?}");

    let duration_count = || -> u64 {
        reg.gather()
            .into_iter()
            .find(|mf| mf.get_name() == "cortex_request_duration_seconds")
            .map(|mf| {
                mf.get_metric()
                    .iter()
                    .filter(|m| m.get_label().iter().any(|lp| lp.get_name() == "method" && lp.get_value() == "gRPC"))
                    .map(|m| m.get_histogram().get_sample_count())
                    .sum()
            })
            .unwrap_or(0)
    };
    assert_eq!(
        duration_count(),
        0,
        "pre-body TapHandle rejections must NOT record cortex_request_duration_seconds"
    );

    // Once TapHandle admits (State::Running), auth rejection DOES record duration!
    ing.set_state_for_test(State::Running);
    let err = client
        .label_names(ingesterpb::LabelNamesRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unknown, "got {err:?}");
    // Allow brief async response body finish to run RpcGuard.
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(duration_count(), 1);

    running.shutdown().await;
    let _ = ing.stop().await;
}

#[tokio::test]
async fn w21_aps_01_head_rejected_405_on_gorilla_mux_routes_and_allowed_on_ready_metrics_pprof() {
    let tmp = tempdir().unwrap();
    let rt_file = tmp.path().join("runtime.yaml");
    fs::write(&rt_file, "overrides:
  t1:
    max_global_series_per_user: 100
").unwrap();

    let mut cfg = MimirConfig::default();
    cfg.target.0 = vec!["ingester".to_string()];
    cfg.server.http_listen_address = "127.0.0.1".to_string();
    cfg.server.http_listen_port = 0;
    cfg.server.grpc_listen_address = "127.0.0.1".to_string();
    cfg.server.grpc_listen_port = 0;
    cfg.runtime_config.file.0 = vec![rt_file.to_string_lossy().into_owned()];
    let rt_mgr = RuntimeConfigManager::new(
        cfg.runtime_config.clone(),
        cfg.limits.clone(),
        cfg.ingester.instance_limits.clone(),
    )
    .unwrap();


    let ing = make_test_ingester(tmp.path());
    let reg = Registry::new();
    let modules = ModuleStates::new();
    let cell = StateCell::new(State::Running);
    modules.add("server", cell.state_fn());

    let router = build_http_router(AppHttpState {
        config: Arc::new(cfg.clone()),
        default_config: Arc::new(MimirConfig::default()),
        registry: reg.clone(),
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: Some(Arc::clone(&ing)),
        runtime_config: Some(rt_mgr.clone()),
        overrides: rt_mgr.overrides(),
        memberlist_kv: None,
        modules,
        cost_attribution_registry: None,
    });

    let server = Server::new(&cfg.server, &reg, Arc::new(NoopLogger)).unwrap();
    let http_addr = server.http_addr().unwrap();
    let running = server
        .start(router, tonic::service::Routes::new(HealthService::new(Arc::new(AtomicBool::new(false)), ModuleStates::new())))
        .unwrap();

    let client = reqwest::Client::new();
    let base = format!("http://{http_addr}");

    // HEAD IS enabled on /ready, /metrics, /debug/pprof/*
    for (path, expected) in [
        ("/ready", StatusCode::OK),
        ("/metrics", StatusCode::OK),
        ("/debug/pprof/", StatusCode::OK),
        ("/debug/pprof/symbol", StatusCode::OK),
        ("/debug/pprof/heap", StatusCode::NOT_FOUND),
    ] {
        let resp = client.head(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), expected.as_u16(), "HEAD {path}");
    }

    // HEAD MUST return 405 with empty body and NO Allow header on all Gorilla mux GET routes!
    // Notably, HEAD /debug/fgprof must NOT start the profiler, and HEAD /ingester/flush must NOT run flush.
    for path in [
        "/",
        "/services",
        "/config",
        "/api/v1/status/buildinfo",
        "/api/v1/status/config",
        "/api/v1/status/flags",
        "/debug/fgprof",
        "/static/",
        "/runtime_config",
        "/api/v1/user_limits",
        "/memberlist",
        "/ingester/flush",
        "/ingester/prepare-shutdown",
        "/ingester/tenants",
        "/ingester/tsdb_metrics",
    ] {
        let resp = client.head(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 405, "HEAD {path} must be 405");
        assert!(
            !resp.headers().contains_key(reqwest::header::ALLOW),
            "HEAD {path} must not emit Allow header: {:?}",
            resp.headers()
        );
        let body = resp.bytes().await.unwrap();
        assert!(body.is_empty(), "HEAD {path} body must be empty");
    }

    // Check route="other" label in cortex_request_duration_seconds for those 405s.
    let families = reg.gather();
    let req_dur = families
        .iter()
        .find(|mf| mf.get_name() == "cortex_request_duration_seconds")
        .expect("cortex_request_duration_seconds");
    assert!(
        req_dur.get_metric().iter().any(|m| {
            m.get_label().iter().any(|lp| lp.get_name() == "route" && lp.get_value() == "other")
                && m.get_label().iter().any(|lp| lp.get_name() == "method" && lp.get_value() == "HEAD")
                && m.get_label().iter().any(|lp| lp.get_name() == "status_code" && lp.get_value() == "405")
        }),
        "expected route=other,method=HEAD,status_code=405 metric"
    );

    running.shutdown().await;
    rt_mgr.stop();
    let _ = ing.stop().await;
}

#[tokio::test]
async fn w21_aps_02_and_08_pprof_concurrent_500_fgprof_bounds_atoi_and_index_link() {
    // 1. /debug/pprof/ index HTML must NOT include broken goroutine?debug=2 link.
    let idx_resp = index_handler(Method::GET).await;
    assert_eq!(idx_resp.status(), StatusCode::OK);
    let idx_body = axum::body::to_bytes(idx_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let idx_html = std::str::from_utf8(&idx_body).unwrap();
    assert!(
        !idx_html.contains("goroutine?debug=2"),
        "/debug/pprof/ must not render broken goroutine?debug=2 link: {idx_html}"
    );

    // 2. /debug/fgprof strconv.Atoi overflow + MAX_PROFILE_SECONDS cap.
    for (query, want_status, want_body) in [
        (
            "seconds=99999999999999999999",
            StatusCode::BAD_REQUEST,
            "bad seconds: 9223372036854775807: strconv.Atoi: parsing \"99999999999999999999\": value out of range\n",
        ),
        (
            "seconds=-99999999999999999999",
            StatusCode::BAD_REQUEST,
            "bad seconds: -9223372036854775808: strconv.Atoi: parsing \"-99999999999999999999\": value out of range\n",
        ),
        (
            "seconds=0",
            StatusCode::BAD_REQUEST,
            "bad seconds: 0: %!s(<nil>)\n",
        ),
        (
            "seconds=abc",
            StatusCode::BAD_REQUEST,
            "bad seconds: 0: strconv.Atoi: parsing \"abc\": invalid syntax\n",
        ),
        (
            "seconds=9223372036854775807",
            StatusCode::BAD_REQUEST,
            "profile seconds 9223372036854775807 exceeds maximum 86400
",
        ),
    ] {
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/debug/fgprof?{query}"))
            .body(Body::empty())
            .unwrap();
        let resp = fgprof_handler(req).await;
        assert_eq!(resp.status(), want_status, "query={query}");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), want_body, "query={query}");
    }

    // 3. Concurrent /debug/pprof/profile returns 500 immediately (does NOT sleep 5s).
    #[cfg(unix)]
    {
        let _active_guard = pprof::ProfilerGuardBuilder::default()
            .frequency(100)
            .build()
            .unwrap();
        let start = Instant::now();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/debug/pprof/profile?seconds=5")
            .body(Body::empty())
            .unwrap();
        let resp = profile_handler(req).await;
        let elapsed = start.elapsed();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            elapsed < Duration::from_millis(500),
            "concurrent profile must fail immediately, took {elapsed:?}"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let txt = std::str::from_utf8(&body).unwrap();
        assert!(txt.contains("Could not enable CPU profiling:"), "{txt}");
    }
}

struct RunningMimir {
    child: Child,
    stderr: Arc<Mutex<Vec<String>>>,
    reader: std::thread::JoinHandle<()>,
}

impl RunningMimir {
    fn spawn(args: &[String]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mimir"))
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn mimir");
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let pipe = child.stderr.take().unwrap();
        let sink = Arc::clone(&stderr);
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(pipe).lines() {
                match line {
                    Ok(l) => sink.lock().unwrap().push(l),
                    Err(_) => break,
                }
            }
        });
        Self {
            child,
            stderr,
            reader,
        }
    }

    fn wait_for_line(&mut self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self.stderr.lock().unwrap().iter().any(|l| l.contains(needle)) {
                return;
            }
            if let Some(st) = self.child.try_wait().unwrap() {
                panic!(
                    "mimir exited {st} before {needle:?}; stderr:
{}",
                    self.stderr.lock().unwrap().join("
")
                );
            }
            assert!(
                Instant::now() < deadline,
                "timeout waiting for {needle:?}; stderr:
{}",
                self.stderr.lock().unwrap().join("
")
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn sigterm_and_wait(mut self) -> (Option<i32>, Vec<String>) {
        let pid = self.child.id() as libc::pid_t;
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        let status = self.child.wait().unwrap();
        let _ = self.reader.join();
        let lines = self.stderr.lock().unwrap().clone();
        (status.code(), lines)
    }
}

#[test]
fn w21_aps_03_and_05_runtime_config_logs_fast_shutdown_and_structured_target_fatal_logs() {
    // 1. W21-APS-05: -target="" (and with -print.config) emits structured "initializing application" fatal log.
    let out_empty = Command::new(env!("CARGO_BIN_EXE_mimir"))
        .args(["-target="])
        .output()
        .unwrap();
    assert_eq!(out_empty.status.code(), Some(1));
    let err_empty = String::from_utf8_lossy(&out_empty.stderr);
    assert!(
        err_empty.contains("msg=\"error initializing application\"")
            && err_empty.contains("no target module specified"),
        "stderr:\n{err_empty}"
    );

    let out_empty_pc = Command::new(env!("CARGO_BIN_EXE_mimir"))
        .args(["-target=", "-print.config"])
        .output()
        .unwrap();
    assert_eq!(out_empty_pc.status.code(), Some(1));
    assert!(out_empty_pc.stdout.is_empty(), "must fail before printing config");

    // 2. W21-APS-05: -target=server with -log.format=json emits structured "running application" fatal log.
    let out_server = Command::new(env!("CARGO_BIN_EXE_mimir"))
        .args(["-target=server", "-log.format=json"])
        .output()
        .unwrap();
    assert_eq!(out_server.status.code(), Some(1));
    let err_server = String::from_utf8_lossy(&out_server.stderr);
    assert!(
        err_server.contains("\"msg\":\"error running application\"")
            && err_server.contains("selected target (server) is an internal module, which is not allowed"),
        "stderr:\n{err_server}"
    );

    // 3. W21-APS-05: -target=memberlist-kv ignores TSDB ingester-only support gates.
    let out_ml_pc = Command::new(env!("CARGO_BIN_EXE_mimir"))
        .args([
            "-target=memberlist-kv",
            "-blocks-storage.tsdb.block-ranges-period=2h,12h",
            "-blocks-storage.tsdb.memory-snapshot-on-shutdown=true",
            "-print.config",
        ])
        .output()
        .unwrap();
    assert_eq!(
        out_ml_pc.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out_ml_pc.stderr)
    );

    // 4. W21-APS-03: runtime-config starting & module stopped logs + fast exit despite 1h reload period.
    let dir = tempdir().unwrap();
    let rt_path = dir.path().join("rt.yaml");
    fs::write(&rt_path, "overrides: {}
").unwrap();
    let cfg_path = dir.path().join("mimir.yaml");
    fs::write(
        &cfg_path,
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
    instance_id: w21-rt
    instance_addr: 127.0.0.1
    replication_factor: 1
    min_ready_duration: 0s
    final_sleep: 0s
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {dir}/bucket
  tsdb:
    dir: {dir}/tsdb
    ship_interval: 0s
runtime_config:
  file: {rt}
  period: 1h
activity_tracker:
  filepath: ""
"#,
            dir = dir.path().display(),
            rt = rt_path.display()
        ),
    )
    .unwrap();

    let mut proc = RunningMimir::spawn(&[format!("-config.file={}", cfg_path.display())]);
    proc.wait_for_line("Application started", Duration::from_secs(15));
    let shutdown_t0 = Instant::now();
    let (code, lines) = proc.sigterm_and_wait();
    let shutdown_elapsed = shutdown_t0.elapsed();
    assert_eq!(code, Some(0), "stderr:
{}", lines.join("
"));
    assert!(
        shutdown_elapsed < Duration::from_secs(5),
        "shutdown blocked on 1h runtime_config period: {shutdown_elapsed:?}"
    );
    let joined = lines.join("
");
    assert!(
        joined.contains("msg=starting module=runtime-config")
            || joined.contains("msg=\"starting\" module=runtime-config"),
        "missing runtime-config starting log:\n{joined}"
    );
    assert!(
        joined.contains("msg=\"module stopped\" module=runtime-config"),
        "missing runtime-config module stopped log:\n{joined}"
    );
}

#[tokio::test]
async fn w21_aps_07_user_limits_honours_raw_scope_org_id_over_httpgrpc() {
    let dir = tempdir().unwrap();
    let rt_path = dir.path().join("rt.yaml");
    fs::write(&rt_path, "overrides: {}
").unwrap();

    let mut cfg = MimirConfig::default();
    cfg.multitenancy_enabled = true;
    cfg.runtime_config.file.0 = vec![rt_path.to_string_lossy().into_owned()];
    let rt_mgr = RuntimeConfigManager::new(
        cfg.runtime_config.clone(),
        cfg.limits.clone(),
        cfg.ingester.instance_limits.clone(),
    )
    .unwrap();

    let router = build_http_router(AppHttpState {
        config: Arc::new(cfg),
        default_config: Arc::new(MimirConfig::default()),
        registry: Registry::new(),
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: None,
        runtime_config: Some(rt_mgr.clone()),
        overrides: rt_mgr.overrides(),
        memberlist_kv: None,
        modules: ModuleStates::new(),
        cost_attribution_registry: None,
    });

    let bridge = HttpGrpcBridge::new(router);
    let res = bridge
        .handle(tonic::Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/api/v1/user_limits".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "X-Scope-OrgID".to_string(),
                values: vec!["badtenant".to_string()],
            }],
            body: Bytes::new(),
        }))
        .await;

    let http_resp = match res {
        Ok(r) => r.into_inner(),
        Err(st) => extract_http_response(&st).unwrap(),
    };
    assert_eq!(http_resp.code, 401);
    let body = String::from_utf8_lossy(&http_resp.body);
    assert!(
        body.contains("unsupported character") && !body.contains("no org id"),
        "expected invalid tenant character error, got: {body:?}"
    );

    rt_mgr.stop();
}
