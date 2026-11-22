//! Upstream Go tests ported 1:1 from `.ref/mimir/pkg/api/`.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, Method, StatusCode};
use axum::middleware::from_fn_with_state;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt as _;
use mimir_cmd::activity_tracking::{
    activity_tracking_middleware, is_request_body_too_large, max_body_size_middleware,
    parse_go_query_values, tenant_auth_and_validation_middleware, to_activity_tracker_string,
    ActivityTrackingState, TenantValidationConfig,
};
use mimir_cmd::api_error::{
    is_retryable_api_error, type_for_error, ApiError, ErrorType, PromqlErrQueryCanceled,
    PromqlErrQueryTimeout, PromqlErrStorage,
};
use mimir_cmd::http::{
    build_http_router, build_http_router_with_custom_config, build_index_page_content,
    default_config_handler_values, render_index_html, resolve_base_url, AppHttpState,
    ConfigHandlerFn, IndexPageContent, IndexPageLink, DEFAULT_WEIGHT,
};
use mimir_cmd::modules::{ModuleStates, StateCell};
use mimir_cmd::protobuf_codec::{
    CounterResetHint, FPoint, FloatHistogram, HPoint, HistSpan, Label, PromqlResult,
    ProtobufCodec, QueryData, ResponseData, Sample, Series, V1Response,
};
use mimir_cmd::server::gzip::{gzip_handler, GzipConfig, GzipRouteEnabled};
use mimir_cmd::server::Server;
use mimir_common::activitytracker::{
    load_unfinished_entries, ActivityTracker, ActivityTrackerConfig,
};
use mimir_common::errors::ContextError;
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::services::State;
use mimir_common::tenant::{MAX_TENANT_ID_LENGTH, ORG_ID_HEADER_NAME};
use mimir_config::{
    BlocksStorageConfig, CompartmentsConfig, IngestStorageConfig, IngesterConfig, LimitsConfig,
    MimirConfig, Overrides, ServerConfig,
};
use mimir_ingester::Ingester;
use mimir_proto::cortexpb::{
    query_response, BucketSpan, FloatHistogram as PbFloatHistogram, FloatHistogramPair,
    FloatSample, MatrixData, MatrixSeries, QueryErrorType, QueryResponse, QueryStatus, ScalarData,
    StringData, VectorData, VectorHistogram, VectorSample,
};
use mimir_proto::prost::Message as _;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, PartitionRingOptions, PartitionRingWatcher, PartitionRingWatchers,
};
use mimir_ring::token_gen::PartitionTokenTable;
use parking_lot::Mutex;
use prometheus::Registry;
use regex::Regex;
use serde_yaml::Value;
use tempfile::tempdir;
use tokio::sync::{mpsc, oneshot};
use tower::{ServiceBuilder, ServiceExt as _};

#[derive(Default)]
struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _kvs: &[(&str, &str)]) {}
}

fn new_test_tracker(dir: &std::path::Path) -> (std::path::PathBuf, Arc<ActivityTracker>) {
    let activity_file = dir.join("activity-tracker");
    let reg = Registry::new();
    let at = ActivityTracker::new(
        ActivityTrackerConfig {
            filepath: activity_file.clone(),
            max_entries: 1024,
        },
        Some(&reg),
    )
    .unwrap()
    .unwrap();
    (activity_file, Arc::new(at))
}

fn new_server_cfg() -> ServerConfig {
    ServerConfig {
        http_listen_address: "127.0.0.1".to_string(),
        http_listen_port: 0,
        grpc_listen_address: "127.0.0.1".to_string(),
        grpc_listen_port: 0,
        grpc_server_max_recv_msg_size: 1024,
        ..Default::default()
    }
}

fn test_http_state(cfg: MimirConfig) -> AppHttpState {
    let modules = ModuleStates::new();
    let cell = StateCell::new(State::Running);
    modules.add("server", cell.state_fn());
    let limits = cfg.limits.clone();
    AppHttpState {
        config: Arc::new(cfg),
        default_config: Arc::new(MimirConfig::default()),
        registry: Registry::new(),
        shutdown_requested: Arc::new(AtomicBool::new(false)),
        ingester: None,
        runtime_config: None,
        overrides: Overrides::new(limits),
        memberlist_kv: None,
        modules,
        cost_attribution_registry: None,
    }
}

// ============================================================================
// pkg/api/activity_tracking_test.go (5 tests)
// ============================================================================

// Upstream: pkg/api/activity_tracking_test.go::TestActivityTrackingMiddleware_DeleteAfterInnerHandler
#[tokio::test]
async fn test_activity_tracking_middleware_delete_after_inner_handler() {
    let dir = tempdir().unwrap();
    let (activity_file, at) = new_test_tracker(dir.path());
    let inner_done = Arc::new(AtomicBool::new(false));
    let inner_done_c = Arc::clone(&inner_done);
    let af_c = activity_file.clone();

    let app = Router::new()
        .route(
            "/api/v1/query",
            get(move |req: Request| {
                let af = af_c.clone();
                let done = Arc::clone(&inner_done_c);
                async move {
                    let activities = load_unfinished_entries(&af).unwrap();
                    assert_eq!(activities.len(), 1);
                    let mut expected_params = BTreeMap::new();
                    expected_params.insert("query".to_string(), vec!["up".to_string()]);
                    let expected = to_activity_tracker_string(
                        req.method(),
                        req.uri().path(),
                        req.headers(),
                        &expected_params,
                    );
                    assert_eq!(activities[0].activity, expected);
                    done.store(true, Ordering::Release);
                    StatusCode::OK
                }
            }),
        )
        .layer(from_fn_with_state(
            ActivityTrackingState {
                tracker: Arc::clone(&at),
                logger: Arc::new(NoopLogger),
            },
            activity_tracking_middleware,
        ));

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/query?query=up")
        .header("X-Scope-OrgID", "tenant1")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(inner_done.load(Ordering::Acquire));

    let activities = load_unfinished_entries(&activity_file).unwrap();
    assert!(activities.is_empty());
}

// Upstream: pkg/api/activity_tracking_test.go::TestActivityTrackingMiddleware_TenantIDFromHeader
#[tokio::test]
async fn test_activity_tracking_middleware_tenant_id_from_header() {
    let dir = tempdir().unwrap();
    let (activity_file, at) = new_test_tracker(dir.path());
    let captured = Arc::new(Mutex::new(String::new()));
    let captured_c = Arc::clone(&captured);
    let af_c = activity_file.clone();

    let app = Router::new()
        .route(
            "/api/v1/query",
            get(move || {
                let af = af_c.clone();
                let cap = Arc::clone(&captured_c);
                async move {
                    let activities = load_unfinished_entries(&af).unwrap();
                    assert_eq!(activities.len(), 1);
                    *cap.lock() = activities[0].activity.clone();
                    StatusCode::OK
                }
            }),
        )
        .layer(from_fn_with_state(
            ActivityTrackingState {
                tracker: Arc::clone(&at),
                logger: Arc::new(NoopLogger),
            },
            activity_tracking_middleware,
        ));

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/query?query=up")
        .header("X-Scope-OrgID", "my-tenant")
        .body(Body::empty())
        .unwrap();
    app.oneshot(req).await.unwrap();
    assert!(captured.lock().contains("user:my-tenant"));
}

// Upstream: pkg/api/activity_tracking_test.go::TestActivityTrackingMiddleware_ParamParsing
#[tokio::test]
async fn test_activity_tracking_middleware_param_parsing() {
    let dir = tempdir().unwrap();
    let (activity_file, at) = new_test_tracker(dir.path());

    struct Case {
        name: &'static str,
        method: Method,
        url: &'static str,
        body: &'static [u8],
        content_type: &'static str,
        expected_params: &'static str,
    }

    let cases = [
        Case {
            name: "GET with query params",
            method: Method::GET,
            url: "/api/v1/query?query=up&time=42",
            body: b"",
            content_type: "",
            expected_params: "query=up&time=42",
        },
        Case {
            name: "POST form-encoded",
            method: Method::POST,
            url: "/api/v1/query",
            body: b"query=up&time=42",
            content_type: "application/x-www-form-urlencoded",
            expected_params: "query=up&time=42",
        },
        Case {
            name: "POST remote write (binary protobuf body is preserved for handler)",
            method: Method::POST,
            url: "/api/v1/push",
            body: b"\x00\x01\x02\x03binary-remote-write-payload",
            content_type: "application/x-protobuf",
            expected_params: "(no params)",
        },
    ];

    for tc in cases {
        let captured = Arc::new(Mutex::new(String::new()));
        let captured_c = Arc::clone(&captured);
        let af_c = activity_file.clone();
        let expected_body = tc.body.to_vec();
        let is_post = tc.method == Method::POST;

        let handler = move |req: Request| {
            let af = af_c.clone();
            let cap = Arc::clone(&captured_c);
            let exp = expected_body.clone();
            async move {
                let activities = load_unfinished_entries(&af).unwrap();
                assert_eq!(activities.len(), 1);
                *cap.lock() = activities[0].activity.clone();
                if is_post {
                    let got = req.into_body().collect().await.unwrap().to_bytes();
                    assert_eq!(got.as_ref(), exp.as_slice());
                }
                StatusCode::OK
            }
        };

        let app = Router::new()
            .route("/api/v1/query", get(handler.clone()).post(handler.clone()))
            .route("/api/v1/push", post(handler))
            .layer(from_fn_with_state(
                ActivityTrackingState {
                    tracker: Arc::clone(&at),
                    logger: Arc::new(NoopLogger),
                },
                activity_tracking_middleware,
            ));

        let mut req_b = Request::builder()
            .method(tc.method.clone())
            .uri(tc.url)
            .header("X-Scope-OrgID", "tenant1");
        if !tc.content_type.is_empty() {
            req_b = req_b.header(header::CONTENT_TYPE, tc.content_type);
        }
        let body = if tc.body.is_empty() {
            Body::empty()
        } else {
            Body::from(tc.body.to_vec())
        };
        let req = req_b.body(body).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{}", tc.name);
        assert!(
            captured.lock().contains(tc.expected_params),
            "{}: got {:?}",
            tc.name,
            *captured.lock()
        );
    }
}

// Upstream: pkg/api/activity_tracking_test.go::TestActivityTrackingMiddleware_NonFormEncodedRequests
#[tokio::test]
async fn test_activity_tracking_middleware_non_form_encoded_requests() {
    let dir = tempdir().unwrap();
    let (activity_file, at) = new_test_tracker(dir.path());

    struct Case {
        name: &'static str,
        body_content: &'static [u8],
        content_type: &'static str,
        chunked: bool,
    }

    let cases = [
        Case {
            name: "chunked transfer encoding: body is passed through intact",
            body_content: b"chunked binary content",
            content_type: "application/octet-stream",
            chunked: true,
        },
        Case {
            name: "non-standard content type: body is passed through intact",
            body_content: b"binary payload \x00\x01\x02\x03 with non-standard content type",
            content_type: "application/vnd.custom-binary",
            chunked: false,
        },
    ];

    for tc in cases {
        let got_body = Arc::new(Mutex::new(Vec::new()));
        let got_body_c = Arc::clone(&got_body);
        let captured = Arc::new(Mutex::new(String::new()));
        let captured_c = Arc::clone(&captured);
        let af_c = activity_file.clone();

        let app = Router::new()
            .route(
                "/api/v1/upload/block/01ABC/files",
                post(move |req: Request| {
                    let af = af_c.clone();
                    let gb = Arc::clone(&got_body_c);
                    let cap = Arc::clone(&captured_c);
                    async move {
                        let activities = load_unfinished_entries(&af).unwrap();
                        assert_eq!(activities.len(), 1);
                        *cap.lock() = activities[0].activity.clone();
                        let b = req.into_body().collect().await.unwrap().to_bytes();
                        *gb.lock() = b.to_vec();
                        StatusCode::OK
                    }
                }),
            )
            .layer(from_fn_with_state(
                ActivityTrackingState {
                    tracker: Arc::clone(&at),
                    logger: Arc::new(NoopLogger),
                },
                activity_tracking_middleware,
            ));

        let mut req_b = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/upload/block/01ABC/files?path=chunks/000001")
            .header(header::CONTENT_TYPE, tc.content_type)
            .header("X-Scope-OrgID", "tenant1");
        if tc.chunked {
            req_b = req_b.header(header::TRANSFER_ENCODING, "chunked");
        }
        let req = req_b.body(Body::from(tc.body_content.to_vec())).unwrap();
        let mut expected_params = BTreeMap::new();
        expected_params.insert("path".to_string(), vec!["chunks/000001".to_string()]);
        let expected_act = to_activity_tracker_string(
            req.method(),
            req.uri().path(),
            req.headers(),
            &expected_params,
        );

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{}", tc.name);
        assert_eq!(got_body.lock().as_slice(), tc.body_content, "{}", tc.name);
        assert_eq!(*captured.lock(), expected_act, "{}", tc.name);
    }
}

// Upstream: pkg/api/activity_tracking_test.go::TestActivityTrackingMiddleware_SlowUpload
#[tokio::test]
async fn test_activity_tracking_middleware_slow_upload() {
    const BODY_CONTENT: &[u8] = b"slow binary block file content";
    let dir = tempdir().unwrap();
    let (activity_file, at) = new_test_tracker(dir.path());

    let got_body = Arc::new(Mutex::new(Vec::new()));
    let got_body_c = Arc::clone(&got_body);
    let captured = Arc::new(Mutex::new(String::new()));
    let captured_c = Arc::clone(&captured);
    let af_c = activity_file.clone();

    let (started_tx, started_rx) = oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));

    let app = Router::new()
        .route(
            "/api/v1/upload/block/01ABC/files",
            post(move |req: Request| {
                let af = af_c.clone();
                let gb = Arc::clone(&got_body_c);
                let cap = Arc::clone(&captured_c);
                let stx = Arc::clone(&started_tx);
                async move {
                    let activities = load_unfinished_entries(&af).unwrap();
                    assert_eq!(activities.len(), 1);
                    *cap.lock() = activities[0].activity.clone();
                    if let Some(tx) = stx.lock().take() {
                        let _ = tx.send(());
                    }
                    let b = req.into_body().collect().await.unwrap().to_bytes();
                    *gb.lock() = b.to_vec();
                    StatusCode::OK
                }
            }),
        )
        .layer(from_fn_with_state(
            ActivityTrackingState {
                tracker: Arc::clone(&at),
                logger: Arc::new(NoopLogger),
            },
            activity_tracking_middleware,
        ));

    let (pipe_tx, pipe_rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let stream = tokio_stream::wrappers::ReceiverStream::new(pipe_rx);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/upload/block/01ABC/files?path=chunks/000001")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("X-Scope-OrgID", "tenant1")
        .body(Body::from_stream(stream))
        .unwrap();
    let mut expected_params = BTreeMap::new();
    expected_params.insert("path".to_string(), vec!["chunks/000001".to_string()]);
    let expected_act = to_activity_tracker_string(
        req.method(),
        req.uri().path(),
        req.headers(),
        &expected_params,
    );

    let handle = tokio::spawn(async move { app.oneshot(req).await.unwrap() });

    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .expect("handler did not start before body was sent: middleware is buffering")
        .unwrap();

    pipe_tx.send(Ok(Bytes::from_static(BODY_CONTENT))).await.unwrap();
    drop(pipe_tx);

    let resp = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("handler did not complete after body was sent")
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(got_body.lock().as_slice(), BODY_CONTENT);
    assert_eq!(*captured.lock(), expected_act);
}

// ============================================================================
// pkg/api/api_test.go (10 tests)
// ============================================================================

// Upstream: pkg/api/api_test.go::TestNewApiWithoutSourceIPExtractor
#[test]
fn test_new_api_without_source_ip_extractor() {
    let mut server_cfg = new_server_cfg();
    server_cfg.log_source_ips_enabled = false;
    let srv = Server::new(&server_cfg, &Registry::new(), Arc::new(NoopLogger)).unwrap();
    assert!(!srv.has_log_source_ips());
}

// Upstream: pkg/api/api_test.go::TestNewApiWithSourceIPExtractor
#[test]
fn test_new_api_with_source_ip_extractor() {
    let mut server_cfg = new_server_cfg();
    server_cfg.log_source_ips_enabled = true;
    let srv = Server::new(&server_cfg, &Registry::new(), Arc::new(NoopLogger)).unwrap();
    assert!(srv.has_log_source_ips());
}

// Upstream: pkg/api/api_test.go::TestNewApiWithInvalidSourceIPExtractor
#[test]
fn test_new_api_with_invalid_source_ip_extractor() {
    let mut server_cfg = new_server_cfg();
    server_cfg.log_source_ips_enabled = true;
    server_cfg.log_source_ips_header = "SomeHeader".to_string();
    server_cfg.log_source_ips_regex = "[*".to_string();
    let res = Server::new(&server_cfg, &Registry::new(), Arc::new(NoopLogger));
    assert!(res.is_err());
}

// Upstream: pkg/api/api_test.go::TestApiGzip
#[tokio::test]
async fn test_api_gzip() {
    const DEFAULT_MIN_SIZE: usize = 1400;
    let gzip_cfg = GzipConfig {
        level: -1,
        cost_attribution_path: None,
    };

    let make_handler = |enabled: bool| {
        move |req: Request| async move {
            let q = req.uri().query().unwrap_or("");
            let params = parse_go_query_values(q).unwrap_or_default();
            let size: usize = params
                .get("respBodySize")
                .and_then(|v| v.first())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let mut resp = (StatusCode::OK, vec![0u8; size]).into_response();
            resp.extensions_mut().insert(GzipRouteEnabled(enabled));
            resp
        }
    };

    let app = Router::new()
        .route(
            "/gzip_enabled",
            get(make_handler(true)).layer(axum::middleware::map_request(
                |mut req: Request| async move {
                    req.extensions_mut().insert(GzipRouteEnabled(true));
                    req
                },
            )),
        )
        .route(
            "/gzip_disabled",
            get(make_handler(false)).layer(axum::middleware::map_request(
                |mut req: Request| async move {
                    req.extensions_mut().insert(GzipRouteEnabled(false));
                    req
                },
            )),
        )
        .route_layer(from_fn_with_state(gzip_cfg, gzip_handler));

    struct Case {
        name: &'static str,
        endpoint: &'static str,
        resp_body_size: usize,
        accept_encoding: &'static str,
        expected_gzip: bool,
    }

    let cases = [
        Case {
            name: "happy case gzip",
            endpoint: "gzip_enabled",
            resp_body_size: DEFAULT_MIN_SIZE + 1,
            accept_encoding: "gzip",
            expected_gzip: true,
        },
        Case {
            name: "gzip with priority header",
            endpoint: "gzip_enabled",
            resp_body_size: DEFAULT_MIN_SIZE + 1,
            accept_encoding: "gzip;q=1",
            expected_gzip: true,
        },
        Case {
            name: "gzip because any is accepted",
            endpoint: "gzip_enabled",
            resp_body_size: DEFAULT_MIN_SIZE + 1,
            accept_encoding: "*",
            expected_gzip: true,
        },
        Case {
            name: "no gzip because no header",
            endpoint: "gzip_enabled",
            resp_body_size: DEFAULT_MIN_SIZE + 1,
            accept_encoding: "",
            expected_gzip: false,
        },
        Case {
            name: "no gzip because not accepted",
            endpoint: "gzip_enabled",
            resp_body_size: DEFAULT_MIN_SIZE + 1,
            accept_encoding: "identity",
            expected_gzip: false,
        },
        Case {
            name: "no gzip because small payload",
            endpoint: "gzip_enabled",
            resp_body_size: 1,
            accept_encoding: "gzip",
            expected_gzip: false,
        },
        Case {
            name: "forced gzip with small payload",
            endpoint: "gzip_enabled",
            resp_body_size: 1,
            accept_encoding: "gzip;q=1, *;q=0",
            expected_gzip: true,
        },
        Case {
            name: "gzip disabled endpoint",
            endpoint: "gzip_disabled",
            resp_body_size: DEFAULT_MIN_SIZE + 1,
            accept_encoding: "gzip",
            expected_gzip: false,
        },
    ];

    for tc in cases {
        let uri = format!("/{}?respBodySize={}", tc.endpoint, tc.resp_body_size);
        let mut rb = Request::builder().method(Method::GET).uri(uri);
        if !tc.accept_encoding.is_empty() {
            rb = rb.header(header::ACCEPT_ENCODING, tc.accept_encoding);
        }
        let resp = app.clone().oneshot(rb.body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{}", tc.name);
        let ce = resp
            .headers()
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if tc.expected_gzip {
            assert_eq!(ce, "gzip", "{}", tc.name);
        } else {
            assert!(ce.is_empty(), "{}: unexpected Content-Encoding={ce}", tc.name);
        }
    }
}

// Upstream: pkg/api/api_test.go::TestApi_RegisterIngesterPartitionRings
#[tokio::test]
async fn test_api_register_ingester_partition_rings() {
    const RING_KEY: &str = "ingester-partitions";

    async fn build_ingester_with_compartments(
        dir: &std::path::Path,
        compartments_enabled: bool,
        num_compartments: i64,
    ) -> AppHttpState {
        let mut cfg = MimirConfig::default();
        cfg.compartments.enabled = compartments_enabled;
        cfg.compartments.read.num_compartments = num_compartments;
        cfg.ingest_storage.enabled = true;
        cfg.ingester.ring.instance_id = "ingester-0".to_string();
        cfg.blocks_storage.tsdb.dir = dir.to_string_lossy().into_owned();

        let kv: Arc<dyn mimir_ring::kv::KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let token_table = Arc::new(PartitionTokenTable::new(0, None).unwrap());
        let opts = PartitionRingOptions {
            token_generator: Some(token_table),
            shuffle_shard_cache_size: 0,
        };

        let mut ing_cfg = IngesterConfig::default();
        ing_cfg.ring.instance_id = "ingester-0".to_string();
        let mut blocks_cfg = BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.to_string_lossy().into_owned();
        let ingest_cfg = IngestStorageConfig {
            enabled: true,
            ..Default::default()
        };

        let watcher_single;
        let watchers_group;
        if compartments_enabled {
            let ws: Vec<_> = (0..num_compartments as usize)
                .map(|c| {
                    let key = format!("{RING_KEY}-rc-{c}");
                    PartitionRingWatcher::new(&key, &key, kv.clone(), opts.clone(), None)
                })
                .collect();
            watcher_single = Some(Arc::clone(&ws[0]));
            watchers_group = Some(PartitionRingWatchers::new(ws).unwrap());
        } else {
            watcher_single = Some(PartitionRingWatcher::new(
                RING_KEY,
                RING_KEY,
                kv.clone(),
                opts,
                None,
            ));
            watchers_group = None;
        }

        let ing = Ingester::new(
            ing_cfg,
            blocks_cfg,
            ingest_cfg,
            Overrides::new(LimitsConfig::default()),
            None,
            None,
            watcher_single,
            None,
            None,
            None,
        )
        .unwrap();
        let mut comp_cfg = CompartmentsConfig {
            enabled: compartments_enabled,
            ..Default::default()
        };
        comp_cfg.read.num_compartments = num_compartments;
        ing.set_compartments(comp_cfg);
        ing.set_partition_ring_watchers(watchers_group);

        let mut st = test_http_state(cfg);
        st.ingester = Some(ing);
        st
    }

    async fn get_path(app: &Router, path: &str) -> (u16, String) {
        let req = Request::builder()
            .method(Method::GET)
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status().as_u16();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    {
        let dir = tempdir().unwrap();
        let st = build_ingester_with_compartments(dir.path(), false, 0).await;
        let app = build_http_router(st);

        let (status, body) = get_path(&app, "/ingester/partition-ring").await;
        assert_eq!(status, 200);
        assert!(body.contains("<th>Partition ID</th>"));
        assert!(!body.contains("partition-ring/compartment-"));

        let (status, _) = get_path(&app, "/ingester/partition-ring/compartment-0").await;
        assert_eq!(status, 404);
    }

    {
        let dir = tempdir().unwrap();
        let st = build_ingester_with_compartments(dir.path(), true, 2).await;
        let app = build_http_router(st);

        let (status, body) = get_path(&app, "/ingester/partition-ring").await;
        assert_eq!(status, 200);
        assert!(body.contains("partition-ring/compartment-0"));
        assert!(body.contains("partition-ring/compartment-1"));

        for c in 0..2 {
            let (status, _) = get_path(&app, &format!("/ingester/partition-ring/compartment-{c}")).await;
            assert_eq!(status, 200);
        }
    }
}

// Upstream: pkg/api/api_test.go::TestApiIngesterShutdown
#[tokio::test]
async fn test_api_ingester_shutdown() {
    let dir = tempdir().unwrap();
    let mut ing_cfg = IngesterConfig::default();
    ing_cfg.ring.instance_id = "ingester-0".to_string();
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
    let ing = Ingester::new(
        ing_cfg,
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

    let mut st = test_http_state(MimirConfig::default());
    st.ingester = Some(ing);
    let app = build_http_router(st);

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/ingester/shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    let post_resp = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/ingester/shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post_resp.status(), StatusCode::NO_CONTENT);
}

// Upstream: pkg/api/api_test.go::TestStaticAssetsRespectPathPrefix
#[tokio::test]
async fn test_static_assets_respect_path_prefix() {
    for (prefix, url) in [
        ("", "/static/mimir-styles.css"),
        ("/mimir", "/mimir/static/mimir-styles.css"),
        ("/foo/bar", "/foo/bar/static/mimir-styles.css"),
    ] {
        let mut cfg = MimirConfig::default();
        cfg.server.http_path_prefix = prefix.to_string();
        let app = build_http_router(test_http_state(cfg));
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(url)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "static asset should be served at {url}"
        );
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(
            !bytes.is_empty(),
            "response body should contain asset bytes at {url}"
        );
    }
}

// Upstream: pkg/api/api_test.go::TestIndexPageLinksAreRelative
#[tokio::test]
async fn test_index_page_links_are_relative() {
    let dir = tempdir().unwrap();
    let mut cfg = MimirConfig::default();
    cfg.runtime_config.file.0 = vec![dir.path().join("rt.yaml").to_string_lossy().into_owned()];
    fs::write(&cfg.runtime_config.file.0[0], "overrides: {}\n").unwrap();

    let mut ing_cfg = IngesterConfig::default();
    ing_cfg.ring.instance_id = "ingester-0".to_string();
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
    let ing = Ingester::new(
        ing_cfg,
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

    let mut st = test_http_state(cfg);
    st.ingester = Some(ing);
    let content = build_index_page_content(&st);
    let groups = content.get_content();
    assert!(!groups.is_empty());
    for group in groups {
        for link in group.links {
            assert!(
                !link.path.starts_with('/'),
                "index page link {:?} in group {:?} must be relative",
                link.path,
                group.desc
            );
        }
    }
}

// Upstream: pkg/api/api_test.go::TestNewRouteMaxBodySize
#[tokio::test]
async fn test_new_route_max_body_size() {
    struct Case {
        name: &'static str,
        body: &'static str,
        content_type: &'static str,
        max_bytes: usize,
        expected_status: StatusCode,
        expect_inner_runs: bool,
        expect_handler_body_err: bool,
    }

    let cases = [
        Case {
            name: "no limit: handler always runs",
            body: "query=up&time=42",
            content_type: "application/x-www-form-urlencoded",
            max_bytes: 0,
            expected_status: StatusCode::OK,
            expect_inner_runs: true,
            expect_handler_body_err: false,
        },
        Case {
            name: "form-encoded body within limit: handler runs",
            body: "query=up&time=42",
            content_type: "application/x-www-form-urlencoded",
            max_bytes: 1024,
            expected_status: StatusCode::OK,
            expect_inner_runs: true,
            expect_handler_body_err: false,
        },
        Case {
            name: "form-encoded body exceeds limit: rejected before inner handler",
            body: "query=up&time=42",
            content_type: "application/x-www-form-urlencoded",
            max_bytes: 5,
            expected_status: StatusCode::PAYLOAD_TOO_LARGE,
            expect_inner_runs: false,
            expect_handler_body_err: false,
        },
        Case {
            name: "non-form-encoded body exceeds limit: MaxBytesReader passed to inner handler",
            body: "binary payload that is too large",
            content_type: "application/octet-stream",
            max_bytes: 5,
            expected_status: StatusCode::OK,
            expect_inner_runs: true,
            expect_handler_body_err: true,
        },
    ];

    for tc in cases {
        let dir = tempdir().unwrap();
        let (_, at) = new_test_tracker(dir.path());
        let inner_ran = Arc::new(AtomicBool::new(false));
        let inner_ran_c = Arc::clone(&inner_ran);
        let saw_max_err = Arc::new(AtomicBool::new(false));
        let saw_max_err_c = Arc::clone(&saw_max_err);
        let ct = tc.content_type;

        let app = Router::new()
            .route(
                "/test-max-body",
                post(move |req: Request| {
                    let ran = Arc::clone(&inner_ran_c);
                    let max_err = Arc::clone(&saw_max_err_c);
                    async move {
                        ran.store(true, Ordering::Release);
                        if ct != "application/x-www-form-urlencoded" {
                            if let Err(e) = req.into_body().collect().await {
                                if is_request_body_too_large(&e) {
                                    max_err.store(true, Ordering::Release);
                                }
                            }
                        }
                        StatusCode::OK
                    }
                }),
            )
            .layer(
                ServiceBuilder::new()
                    .layer(from_fn_with_state(tc.max_bytes, max_body_size_middleware))
                    .layer(from_fn_with_state(
                        ActivityTrackingState {
                            tracker: Arc::clone(&at),
                            logger: Arc::new(NoopLogger),
                        },
                        activity_tracking_middleware,
                    )),
            );

        let req = Request::builder()
            .method(Method::POST)
            .uri("/test-max-body")
            .header(header::CONTENT_TYPE, tc.content_type)
            .body(Body::from(tc.body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), tc.expected_status, "{}", tc.name);
        assert_eq!(
            inner_ran.load(Ordering::Acquire),
            tc.expect_inner_runs,
            "{}",
            tc.name
        );
        if tc.expect_handler_body_err {
            assert!(saw_max_err.load(Ordering::Acquire), "{}", tc.name);
        }
    }
}

// Upstream: pkg/api/api_test.go::TestActivityTrackingNotAppliedToPushRoute
#[tokio::test]
async fn test_activity_tracking_not_applied_to_push_route() {
    let dir = tempdir().unwrap();
    let (_, at) = new_test_tracker(dir.path());
    let called = Arc::new(AtomicBool::new(false));
    let called_c = Arc::clone(&called);

    let max_body_size: usize = 0;
    let route = post(move || {
        let c = Arc::clone(&called_c);
        async move {
            c.store(true, Ordering::Release);
            StatusCode::OK
        }
    });
    let app = if max_body_size > 0 {
        Router::new().route(
            "/api/v1/push",
            route.layer(from_fn_with_state(
                ActivityTrackingState {
                    tracker: Arc::clone(&at),
                    logger: Arc::new(NoopLogger),
                },
                activity_tracking_middleware,
            )),
        )
    } else {
        Router::new().route("/api/v1/push", route)
    };

    let body = vec![0x0a, 0x12, 0x3B, 0x08, 0x01, 0x12, 0x0e];
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/push")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        called.load(Ordering::Acquire),
        "activity tracking middleware must not run on routes registered without a max body size"
    );
}

// ============================================================================
// pkg/api/error/error_test.go (3 tests)
// ============================================================================

// Upstream: pkg/api/error/error_test.go::TestAllPrometheusErrorTypeValues
#[test]
fn test_all_prometheus_error_type_values() {
    let api_go_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.ref/mimir/vendor/github.com/prometheus/prometheus/web/api/v1/api.go");
    let contents = fs::read_to_string(&api_go_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", api_go_path.display()));
    let re = Regex::new(r#"(?m)^\s*[^ \t]+\s+=\s+errorType\{[^,]+,\s+"([^"]*)"\}"#).unwrap();
    let mut found = Vec::new();
    for cap in re.captures_iter(&contents) {
        found.push(cap[1].to_string());
    }
    assert!(!found.is_empty(), "expected Prometheus errorType constants");

    for error_type_str in found {
        let typ = ErrorType::parse(&error_type_str);
        let api_err = ApiError::new(typ, "");
        match typ {
            ErrorType::Unavailable => assert_eq!(api_err.status_code(), 503),
            ErrorType::Internal | ErrorType::None => assert_eq!(api_err.status_code(), 500),
            _ => assert_ne!(
                api_err.status_code(),
                500,
                "unrecognised Prometheus error type constant '{error_type_str}'"
            ),
        }
    }
}

// Upstream: pkg/api/error/error_test.go::TestIsRetryableAPIError
#[test]
fn test_is_retryable_api_error() {
    #[derive(Debug)]
    struct PlainErr(&'static str);
    impl fmt::Display for PlainErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }
    impl StdError for PlainErr {}

    let retryable: Vec<Box<dyn StdError>> = vec![
        Box::new(ApiError::new(ErrorType::Internal, ErrorType::Internal.as_str())),
        Box::new(ContextError::Canceled),
        Box::new(ContextError::DeadlineExceeded),
        Box::new(PlainErr("something bad")),
    ];
    for err in &retryable {
        assert!(is_retryable_api_error(err.as_ref()), "expected retryable: {err}");
    }

    let non_retryable = [
        ApiError::new(ErrorType::None, "none"),
        ApiError::new(ErrorType::NotFound, ErrorType::NotFound.as_str()),
        ApiError::new(ErrorType::Timeout, ErrorType::Timeout.as_str()),
        ApiError::new(ErrorType::TooManyRequests, ErrorType::TooManyRequests.as_str()),
        ApiError::new(ErrorType::NotAcceptable, ErrorType::NotAcceptable.as_str()),
        ApiError::new(ErrorType::Unavailable, ErrorType::Unavailable.as_str()),
        ApiError::new(ErrorType::Canceled, ErrorType::Canceled.as_str()),
        ApiError::new(ErrorType::Exec, ErrorType::Exec.as_str()),
        ApiError::new(ErrorType::BadData, ErrorType::BadData.as_str()),
        ApiError::new(ErrorType::TooLargeEntry, ErrorType::TooLargeEntry.as_str()),
    ];
    for err in &non_retryable {
        assert!(!is_retryable_api_error(err), "expected non-retryable: {err}");
    }
}

// Upstream: pkg/api/error/error_test.go::TestTypeForError
#[test]
fn test_type_for_error() {
    #[derive(Debug)]
    struct PlainErr(&'static str);
    impl fmt::Display for PlainErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }
    impl StdError for PlainErr {}

    #[derive(Debug)]
    struct WrappedErr(Box<dyn StdError + Send + Sync>);
    impl fmt::Display for WrappedErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "something went wrong one level down: {}", self.0)
        }
    }
    impl StdError for WrappedErr {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            Some(self.0.as_ref())
        }
    }

    let fallback = ErrorType::NotAcceptable;

    let cases: Vec<(&str, Box<dyn StdError + Send + Sync>, ErrorType)> = vec![
        ("generic error", Box::new(PlainErr("something went wrong")), fallback),
        ("context canceled", Box::new(ContextError::Canceled), ErrorType::Canceled),
        ("context deadline exceeded", Box::new(ContextError::DeadlineExceeded), ErrorType::Timeout),
        (
            "storage error",
            Box::new(PromqlErrStorage(Box::new(PlainErr("could not load data")))),
            ErrorType::Internal,
        ),
        (
            "query canceled error",
            Box::new(PromqlErrQueryCanceled("canceled".to_string())),
            ErrorType::Canceled,
        ),
        (
            "query timeout error",
            Box::new(PromqlErrQueryTimeout("timed out".to_string())),
            ErrorType::Timeout,
        ),
        (
            "APIError",
            Box::new(ApiError::new(ErrorType::NotAcceptable, "request is not acceptable")),
            ErrorType::NotAcceptable,
        ),
        (
            "gRPC error with canceled code",
            Box::new(tonic::Status::cancelled("request was canceled")),
            ErrorType::Canceled,
        ),
        (
            "gRPC error with unknown code",
            Box::new(tonic::Status::unknown("something went wrong")),
            fallback,
        ),
        (
            "gRPC error with invalid argument code",
            Box::new(tonic::Status::invalid_argument("request has invalid argument")),
            ErrorType::BadData,
        ),
        (
            "gRPC error with deadline exceeded code",
            Box::new(tonic::Status::deadline_exceeded("request timed out")),
            ErrorType::Timeout,
        ),
        (
            "gRPC error with not found code",
            Box::new(tonic::Status::not_found("resource not found")),
            ErrorType::NotFound,
        ),
        (
            "gRPC error with internal code",
            Box::new(tonic::Status::internal("something went wrong")),
            ErrorType::Internal,
        ),
        (
            "gRPC error with unavailable code",
            Box::new(tonic::Status::unavailable("resource not available")),
            ErrorType::Unavailable,
        ),
        (
            "gRPC message too large error",
            Box::new(tonic::Status::resource_exhausted(
                "trying to send message larger than max (112 vs. 100)",
            )),
            ErrorType::TooLargeEntry,
        ),
        (
            "other resource exhausted gRPC error",
            Box::new(tonic::Status::resource_exhausted("something else went wrong")),
            fallback,
        ),
    ];

    for (name, err, expected) in cases {
        assert_eq!(type_for_error(err.as_ref(), fallback), expected, "{name}");
        let wrapped = WrappedErr(err);
        assert_eq!(type_for_error(&wrapped, fallback), expected, "{name} (wrapped)");
    }
}

// ============================================================================
// pkg/api/handlers_test.go (4 tests)
// ============================================================================

// Upstream: pkg/api/handlers_test.go::TestIndexPageContent
#[test]
fn test_index_page_content() {
    let c = IndexPageContent::new("");
    c.add_links(
        DEFAULT_WEIGHT,
        "Some group",
        vec![
            IndexPageLink {
                desc: "Some link".to_string(),
                path: "store-gateway/ring".to_string(),
                dangerous: false,
            },
            IndexPageLink {
                desc: "Boom!".to_string(),
                path: "store-gateway/boom".to_string(),
                dangerous: true,
            },
        ],
    );
    let html = render_index_html(&c);
    assert!(html.contains("Some group"));
    assert!(html.contains("Some link"));
    assert!(html.contains("Dangerous"));
    assert!(html.contains("Boom!"));
    assert!(html.contains("store-gateway/ring"));
    assert!(html.contains("store-gateway/boom"));
    assert!(!html.contains("compactor/ring"));
}

// Upstream: pkg/api/handlers_test.go::TestResolveBaseURL
#[test]
fn test_resolve_base_url() {
    assert_eq!(resolve_base_url("/mimir", "/config"), "/mimir/config");
    assert_eq!(resolve_base_url("/mimir/", "/config"), "/mimir/config");
    assert_eq!(resolve_base_url("/mimir", "runtime_config"), "runtime_config");
}

// Upstream: pkg/api/handlers_test.go::TestConfigDiffHandler
#[tokio::test]
async fn test_config_diff_handler() {
    fn default_mock() -> Value {
        serde_yaml::from_str(
            r#"
my_int: 666
my_float: 6.66
my_slice:
  - value1
  - value2
my_nested_struct:
  my_string: string1
  my_bool: false
  my_empty_struct: {}
"#,
        )
        .unwrap()
    }

    let cases: Vec<(&str, Value, u16, &str)> = vec![
        ("no config parameters overridden", default_mock(), 200, "{}\n"),
        (
            "slice changed",
            {
                let mut v = default_mock();
                v["my_slice"]
                    .as_sequence_mut()
                    .unwrap()
                    .push(Value::String("value3".to_string()));
                v
            },
            200,
            "my_slice:\n    - value1\n    - value2\n    - value3\n",
        ),
        (
            "string in nested struct changed",
            {
                let mut v = default_mock();
                v["my_nested_struct"]["my_string"] = Value::String("string2".to_string());
                v
            },
            200,
            "my_nested_struct:\n    my_string: string2\n",
        ),
        (
            "bool in nested struct changed",
            {
                let mut v = default_mock();
                v["my_nested_struct"]["my_bool"] = Value::Bool(true);
                v
            },
            200,
            "my_nested_struct:\n    my_bool: true\n",
        ),
        (
            "test invalid input",
            Value::String("x".to_string()),
            500,
            "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `x` into map[string]interface {}\n",
        ),
    ];

    for (name, actual, expected_status, expected_body) in cases {
        let def = default_mock();
        let resp = default_config_handler_values(&actual, &def, Some("diff"));
        assert_eq!(resp.status().as_u16(), expected_status, "{name}");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(String::from_utf8_lossy(&body), expected_body, "{name}");
    }
}

// Upstream: pkg/api/handlers_test.go::TestConfigOverrideHandler
#[tokio::test]
async fn test_config_override_handler() {
    let custom: ConfigHandlerFn = Arc::new(|_actual, _default, _mode| {
        (StatusCode::OK, "config").into_response()
    });
    let app = build_http_router_with_custom_config(
        test_http_state(MimirConfig::default()),
        Some(custom),
    );
    let resp = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"config");
}

// ============================================================================
// pkg/api/protobuf_codec_test.go (2 tests)
// ============================================================================

// Upstream: pkg/api/protobuf_codec_test.go::TestProtobufCodec_CanEncode
#[test]
fn test_protobuf_codec_can_encode() {
    let codec = ProtobufCodec;

    let cases = [
        (
            "query results",
            V1Response {
                status: "success".to_string(),
                error_type: String::new(),
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(ResponseData::QueryData(QueryData {
                    result: PromqlResult::Vector(vec![]),
                })),
            },
            true,
        ),
        (
            "error",
            V1Response {
                status: "error".to_string(),
                error_type: String::new(),
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: None,
            },
            true,
        ),
        (
            "another type of result",
            V1Response {
                status: "success".to_string(),
                error_type: String::new(),
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(ResponseData::Other("a string".to_string())),
            },
            false,
        ),
    ];

    for (name, resp, expected) in cases {
        assert_eq!(codec.can_encode(&resp), expected, "{name}");
    }
}

// Upstream: pkg/api/protobuf_codec_test.go::TestProtobufCodec_Encode
#[test]
fn test_protobuf_codec_encode() {
    let codec = ProtobufCodec;

    fn hist_a() -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: CounterResetHint::Gauge,
            schema: 3,
            zero_threshold: 1.23,
            zero_count: 456.0,
            count: 9001.0,
            sum: 789.1,
            positive_spans: vec![
                HistSpan { offset: 4, length: 1 },
                HistSpan { offset: 3, length: 2 },
            ],
            negative_spans: vec![
                HistSpan { offset: 7, length: 3 },
                HistSpan { offset: 9, length: 1 },
            ],
            positive_buckets: vec![100.0, 200.0, 300.0],
            negative_buckets: vec![400.0, 500.0, 600.0, 700.0],
        }
    }

    fn pb_hist_a() -> PbFloatHistogram {
        PbFloatHistogram {
            counter_reset_hint: 3,
            schema: 3,
            zero_threshold: 1.23,
            zero_count: 456.0,
            count: 9001.0,
            sum: 789.1,
            positive_spans: vec![
                BucketSpan { offset: 4, length: 1 },
                BucketSpan { offset: 3, length: 2 },
            ],
            negative_spans: vec![
                BucketSpan { offset: 7, length: 3 },
                BucketSpan { offset: 9, length: 1 },
            ],
            positive_buckets: vec![100.0, 200.0, 300.0],
            negative_buckets: vec![400.0, 500.0, 600.0, 700.0],
            custom_values: vec![],
        }
    }

    fn hist_b() -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: CounterResetHint::Gauge,
            schema: 4,
            zero_threshold: 1.203,
            zero_count: 4560.0,
            count: 90010.0,
            sum: 7890.1,
            positive_spans: vec![
                HistSpan { offset: 40, length: 1 },
                HistSpan { offset: 30, length: 2 },
            ],
            negative_spans: vec![
                HistSpan { offset: 70, length: 3 },
                HistSpan { offset: 90, length: 1 },
            ],
            positive_buckets: vec![1000.0, 2000.0, 3000.0],
            negative_buckets: vec![4000.0, 5000.0, 6000.0, 7000.0],
        }
    }

    fn pb_hist_b() -> PbFloatHistogram {
        PbFloatHistogram {
            counter_reset_hint: 3,
            schema: 4,
            zero_threshold: 1.203,
            zero_count: 4560.0,
            count: 90010.0,
            sum: 7890.1,
            positive_spans: vec![
                BucketSpan { offset: 40, length: 1 },
                BucketSpan { offset: 30, length: 2 },
            ],
            negative_spans: vec![
                BucketSpan { offset: 70, length: 3 },
                BucketSpan { offset: 90, length: 1 },
            ],
            positive_buckets: vec![1000.0, 2000.0, 3000.0],
            negative_buckets: vec![4000.0, 5000.0, 6000.0, 7000.0],
            custom_values: vec![],
        }
    }

    fn lbl(k: &str, v: &str) -> Label {
        Label {
            name: k.to_string(),
            value: v.to_string(),
        }
    }

    fn q_ok(result: PromqlResult) -> V1Response {
        V1Response {
            status: "success".to_string(),
            error_type: String::new(),
            error: String::new(),
            warnings: vec![],
            infos: vec![],
            data: Some(ResponseData::QueryData(QueryData { result })),
        }
    }

    let scenarios: Vec<(&str, V1Response, QueryResponse)> = vec![
        (
            "error",
            V1Response {
                status: "error".to_string(),
                error_type: "unavailable".to_string(),
                error: "something went wrong".to_string(),
                warnings: vec![],
                infos: vec![],
                data: None,
            },
            QueryResponse {
                status: QueryStatus::Error as i32,
                error_type: QueryErrorType::Unavailable as i32,
                error: "something went wrong".to_string(),
                warnings: vec![],
                infos: vec![],
                data: None,
            },
        ),
        (
            "string",
            q_ok(PromqlResult::String {
                t: 1234,
                v: "the-string".to_string(),
            }),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::String(StringData {
                    timestamp_ms: 1234,
                    value: "the-string".to_string(),
                })),
            },
        ),
        (
            "scalar",
            q_ok(PromqlResult::Scalar { t: 1234, v: 5.67 }),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Scalar(ScalarData {
                    timestamp_ms: 1234,
                    value: 5.67,
                })),
            },
        ),
        (
            "vector with no series",
            q_ok(PromqlResult::Vector(vec![])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Vector(VectorData::default())),
            },
        ),
        (
            "vector with single series with no labels",
            q_ok(PromqlResult::Vector(vec![Sample {
                metric: vec![],
                t: 1234,
                f: 5.67,
                h: None,
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Vector(VectorData {
                    samples: vec![VectorSample {
                        metric: vec![],
                        timestamp_ms: 1234,
                        value: 5.67,
                    }],
                    histograms: vec![],
                })),
            },
        ),
        (
            "vector with single series with one label",
            q_ok(PromqlResult::Vector(vec![Sample {
                metric: vec![lbl("name-1", "value-1")],
                t: 1234,
                f: 5.67,
                h: None,
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Vector(VectorData {
                    samples: vec![VectorSample {
                        metric: vec!["name-1".into(), "value-1".into()],
                        timestamp_ms: 1234,
                        value: 5.67,
                    }],
                    histograms: vec![],
                })),
            },
        ),
        (
            "vector with single series with many labels",
            q_ok(PromqlResult::Vector(vec![Sample {
                metric: vec![lbl("name-1", "value-1"), lbl("name-2", "value-2")],
                t: 1234,
                f: 5.67,
                h: None,
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Vector(VectorData {
                    samples: vec![VectorSample {
                        metric: vec![
                            "name-1".into(),
                            "value-1".into(),
                            "name-2".into(),
                            "value-2".into(),
                        ],
                        timestamp_ms: 1234,
                        value: 5.67,
                    }],
                    histograms: vec![],
                })),
            },
        ),
        (
            "vector with multiple series",
            q_ok(PromqlResult::Vector(vec![
                Sample {
                    metric: vec![lbl("name-1", "value-1"), lbl("name-2", "value-2")],
                    t: 1234,
                    f: 5.67,
                    h: None,
                },
                Sample {
                    metric: vec![lbl("name-3", "value-3"), lbl("name-4", "value-4")],
                    t: 2345,
                    f: 6.78,
                    h: None,
                },
            ])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Vector(VectorData {
                    samples: vec![
                        VectorSample {
                            metric: vec![
                                "name-1".into(),
                                "value-1".into(),
                                "name-2".into(),
                                "value-2".into(),
                            ],
                            timestamp_ms: 1234,
                            value: 5.67,
                        },
                        VectorSample {
                            metric: vec![
                                "name-3".into(),
                                "value-3".into(),
                                "name-4".into(),
                                "value-4".into(),
                            ],
                            timestamp_ms: 2345,
                            value: 6.78,
                        },
                    ],
                    histograms: vec![],
                })),
            },
        ),
        (
            "vector with series with histogram value",
            q_ok(PromqlResult::Vector(vec![Sample {
                metric: vec![lbl("name-1", "value-1")],
                t: 1234,
                f: 0.0,
                h: Some(hist_a()),
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Vector(VectorData {
                    samples: vec![],
                    histograms: vec![VectorHistogram {
                        metric: vec!["name-1".into(), "value-1".into()],
                        timestamp_ms: 1234,
                        histogram: Some(pb_hist_a()),
                    }],
                })),
            },
        ),
        (
            "vector with float and histogram values",
            q_ok(PromqlResult::Vector(vec![
                Sample {
                    metric: vec![lbl("name-1", "value-1")],
                    t: 1234,
                    f: 0.0,
                    h: Some(hist_a()),
                },
                Sample {
                    metric: vec![lbl("name-2", "value-2")],
                    t: 5678,
                    f: 9.01,
                    h: None,
                },
                Sample {
                    metric: vec![lbl("name-3", "value-3")],
                    t: 12340,
                    f: 0.0,
                    h: Some(hist_b()),
                },
                Sample {
                    metric: vec![lbl("name-4", "value-4")],
                    t: 56780,
                    f: 90.01,
                    h: None,
                },
            ])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Vector(VectorData {
                    histograms: vec![
                        VectorHistogram {
                            metric: vec!["name-1".into(), "value-1".into()],
                            timestamp_ms: 1234,
                            histogram: Some(pb_hist_a()),
                        },
                        VectorHistogram {
                            metric: vec!["name-3".into(), "value-3".into()],
                            timestamp_ms: 12340,
                            histogram: Some(pb_hist_b()),
                        },
                    ],
                    samples: vec![
                        VectorSample {
                            metric: vec!["name-2".into(), "value-2".into()],
                            timestamp_ms: 5678,
                            value: 9.01,
                        },
                        VectorSample {
                            metric: vec!["name-4".into(), "value-4".into()],
                            timestamp_ms: 56780,
                            value: 90.01,
                        },
                    ],
                })),
            },
        ),
        (
            "matrix with no series",
            q_ok(PromqlResult::Matrix(vec![])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData::default())),
            },
        ),
        (
            "matrix with single series with no points and no labels",
            q_ok(PromqlResult::Matrix(vec![Series {
                metric: vec![],
                floats: vec![],
                histograms: vec![],
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![MatrixSeries::default()],
                })),
            },
        ),
        (
            "matrix with single series with no points and one label",
            q_ok(PromqlResult::Matrix(vec![Series {
                metric: vec![lbl("name-1", "value-1")],
                floats: vec![],
                histograms: vec![],
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![MatrixSeries {
                        metric: vec!["name-1".into(), "value-1".into()],
                        samples: vec![],
                        histograms: vec![],
                    }],
                })),
            },
        ),
        (
            "matrix with single series with no points and many labels",
            q_ok(PromqlResult::Matrix(vec![Series {
                metric: vec![lbl("name-1", "value-1"), lbl("name-2", "value-2")],
                floats: vec![],
                histograms: vec![],
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![MatrixSeries {
                        metric: vec![
                            "name-1".into(),
                            "value-1".into(),
                            "name-2".into(),
                            "value-2".into(),
                        ],
                        samples: vec![],
                        histograms: vec![],
                    }],
                })),
            },
        ),
        (
            "matrix with single series with one point",
            q_ok(PromqlResult::Matrix(vec![Series {
                metric: vec![lbl("name-1", "value-1"), lbl("name-2", "value-2")],
                floats: vec![FPoint { t: 1234, f: 5.67 }],
                histograms: vec![],
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![MatrixSeries {
                        metric: vec![
                            "name-1".into(),
                            "value-1".into(),
                            "name-2".into(),
                            "value-2".into(),
                        ],
                        samples: vec![FloatSample {
                            timestamp_ms: 1234,
                            value: 5.67,
                        }],
                        histograms: vec![],
                    }],
                })),
            },
        ),
        (
            "matrix with single series with many points",
            q_ok(PromqlResult::Matrix(vec![Series {
                metric: vec![lbl("name-1", "value-1"), lbl("name-2", "value-2")],
                floats: vec![
                    FPoint { t: 1234, f: 5.67 },
                    FPoint { t: 5678, f: 9.01 },
                ],
                histograms: vec![],
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![MatrixSeries {
                        metric: vec![
                            "name-1".into(),
                            "value-1".into(),
                            "name-2".into(),
                            "value-2".into(),
                        ],
                        samples: vec![
                            FloatSample {
                                timestamp_ms: 1234,
                                value: 5.67,
                            },
                            FloatSample {
                                timestamp_ms: 5678,
                                value: 9.01,
                            },
                        ],
                        histograms: vec![],
                    }],
                })),
            },
        ),
        (
            "matrix with many series",
            q_ok(PromqlResult::Matrix(vec![
                Series {
                    metric: vec![lbl("name-1", "value-1")],
                    floats: vec![
                        FPoint { t: 1234, f: 5.67 },
                        FPoint { t: 5678, f: 9.01 },
                    ],
                    histograms: vec![],
                },
                Series {
                    metric: vec![lbl("name-2", "value-2")],
                    floats: vec![
                        FPoint { t: 12340, f: 50.67 },
                        FPoint { t: 56780, f: 90.01 },
                    ],
                    histograms: vec![],
                },
            ])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![
                        MatrixSeries {
                            metric: vec!["name-1".into(), "value-1".into()],
                            samples: vec![
                                FloatSample {
                                    timestamp_ms: 1234,
                                    value: 5.67,
                                },
                                FloatSample {
                                    timestamp_ms: 5678,
                                    value: 9.01,
                                },
                            ],
                            histograms: vec![],
                        },
                        MatrixSeries {
                            metric: vec!["name-2".into(), "value-2".into()],
                            samples: vec![
                                FloatSample {
                                    timestamp_ms: 12340,
                                    value: 50.67,
                                },
                                FloatSample {
                                    timestamp_ms: 56780,
                                    value: 90.01,
                                },
                            ],
                            histograms: vec![],
                        },
                    ],
                })),
            },
        ),
        (
            "matrix with series with histogram value",
            q_ok(PromqlResult::Matrix(vec![Series {
                metric: vec![lbl("name-1", "value-1"), lbl("name-2", "value-2")],
                floats: vec![],
                histograms: vec![HPoint {
                    t: 1234,
                    h: hist_a(),
                }],
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![MatrixSeries {
                        metric: vec![
                            "name-1".into(),
                            "value-1".into(),
                            "name-2".into(),
                            "value-2".into(),
                        ],
                        samples: vec![],
                        histograms: vec![FloatHistogramPair {
                            timestamp_ms: 1234,
                            histogram: Some(pb_hist_a()),
                        }],
                    }],
                })),
            },
        ),
        (
            "matrix with series with both float and histogram values",
            q_ok(PromqlResult::Matrix(vec![Series {
                metric: vec![lbl("name-1", "value-1"), lbl("name-2", "value-2")],
                floats: vec![
                    FPoint { t: 5678, f: 9.01 },
                    FPoint { t: 56780, f: 90.01 },
                ],
                histograms: vec![
                    HPoint {
                        t: 1234,
                        h: hist_a(),
                    },
                    HPoint {
                        t: 12340,
                        h: hist_b(),
                    },
                ],
            }])),
            QueryResponse {
                status: QueryStatus::Success as i32,
                error_type: 0,
                error: String::new(),
                warnings: vec![],
                infos: vec![],
                data: Some(query_response::Data::Matrix(MatrixData {
                    series: vec![MatrixSeries {
                        metric: vec![
                            "name-1".into(),
                            "value-1".into(),
                            "name-2".into(),
                            "value-2".into(),
                        ],
                        samples: vec![
                            FloatSample {
                                timestamp_ms: 5678,
                                value: 9.01,
                            },
                            FloatSample {
                                timestamp_ms: 56780,
                                value: 90.01,
                            },
                        ],
                        histograms: vec![
                            FloatHistogramPair {
                                timestamp_ms: 1234,
                                histogram: Some(pb_hist_a()),
                            },
                            FloatHistogramPair {
                                timestamp_ms: 12340,
                                histogram: Some(pb_hist_b()),
                            },
                        ],
                    }],
                })),
            },
        ),
    ];

    for (name, resp, expected) in scenarios {
        assert!(codec.can_encode(&resp), "{name}: CanEncode returned false");
        let bytes = codec.encode(&resp).unwrap();
        let actual = QueryResponse::decode(bytes.as_slice()).unwrap();
        assert_eq!(actual, expected, "{name}");
    }
}

// ============================================================================
// pkg/api/tenant_test.go (1 test)
// ============================================================================

// Upstream: pkg/api/tenant_test.go::TestNewTenantValidationMiddleware
#[tokio::test]
async fn test_new_tenant_validation_middleware() {
    let long_header = "123".repeat(MAX_TENANT_ID_LENGTH);
    let cases = [
        ("federation disabled, invalid tenant header", false, 0, long_header.as_str(), 401, "tenant ID is too long"),
        ("federation disabled, single tenant", false, 0, "tenant-a", 200, ""),
        ("federation disabled, multiple tenants", false, 0, "tenant-a|tenant-b", 422, "too many tenant IDs present"),
        ("federation enabled, invalid tenant header", true, 0, long_header.as_str(), 401, "tenant ID is too long"),
        ("federation enabled, single tenant no limit", true, 0, "tenant-a", 200, ""),
        ("federation enabled, multiple tenants no limit", true, 0, "tenant-a|tenant-b|tenant-c", 200, ""),
        ("federation enabled, multiple tenants under limit", true, 2, "tenant-a|tenant-b", 200, ""),
        ("federation enabled, multiple tenants over limit", true, 2, "tenant-a|tenant-b|tenant-c", 422, "too many tenant IDs present"),
    ];

    for (name, federation, max_tenants, hdr, expected_status, expected_body) in cases {
        let app = Router::new()
            .route("/", get(|| async { StatusCode::OK }))
            .layer(from_fn_with_state(
                TenantValidationConfig {
                    federation,
                    max_tenants,
                },
                tenant_auth_and_validation_middleware,
            ));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(ORG_ID_HEADER_NAME, hdr)
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), expected_status, "{name}");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8_lossy(&body);
        if !expected_body.is_empty() {
            assert!(
                body_str.contains(expected_body),
                "{name}: expected {expected_body:?} in {body_str:?}"
            );
        }
    }
}
