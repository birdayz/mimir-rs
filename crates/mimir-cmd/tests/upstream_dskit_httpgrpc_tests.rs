//! Ported from:
//! - github.com/grafana/dskit/httpgrpc/httpgrpc_test.go (5 tests)
//! - github.com/grafana/dskit/httpgrpc/server/server_test.go (10 tests)

use axum::extract::Request as AxumRequest;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::routing::{any, get};
use axum::Router;
use bytes::Bytes;
use mimir_cmd::grpc::{
    http_canonical_header_key, httpgrpc_error, httpgrpc_error_from_response,
    httpgrpc_response_from_error, httpgrpc_to_headers, is_handled_by_httpgrpc_server,
    parse_httpgrpc_url, ErrAbortHandler, HttpGrpcBridge, HttpGrpcStatusFixService,
};
use mimir_cmd::grpc_call_check::{
    append_request_metadata_to_context, HTTPGRPC_METADATA_METHOD, HTTPGRPC_METADATA_URL,
};
use mimir_common::grpc_log::GrpcErrorLog;
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::tracing::{
    current_trace_context, extract_from_http_headers, init_otel_or_jaeger_from_env, TraceContext,
};
use mimir_proto::grpcutilpb::{decode_status_details, extract_http_response};
use mimir_proto::httpgrpcpb::http_client::HttpClient;
use mimir_proto::httpgrpcpb::http_server::{Http, HttpServer};
use mimir_proto::httpgrpcpb::{Header as HttpGrpcHeader, HttpRequest, HttpResponse};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataMap;
use tonic::transport::Server as TonicServer;
use tonic::{Request, Status};

async fn start_httpgrpc_test_server(bridge: HttpGrpcBridge) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = HttpGrpcStatusFixService::new(HttpServer::new(bridge));
    let handle = tokio::spawn(async move {
        let _ = TonicServer::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (format!("http://{addr}"), handle)
}

// ============================================================================
// github.com/grafana/dskit/httpgrpc/httpgrpc_test.go (5 tests)
// ============================================================================

// Upstream: github.com/grafana/dskit/httpgrpc/httpgrpc_test.go::TestAppendMessageSizeToOutgoingContext
#[test]
fn test_append_message_size_to_outgoing_context() {
    let mut md = MetadataMap::new();
    let req = HttpRequest {
        method: "GET".to_string(),
        url: "/test".to_string(),
        headers: Vec::new(),
        body: Bytes::new(),
    };
    append_request_metadata_to_context(&mut md, &req);
    let methods: Vec<_> = md
        .get_all(HTTPGRPC_METADATA_METHOD)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    let urls: Vec<_> = md
        .get_all(HTTPGRPC_METADATA_URL)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    assert_eq!(methods, vec!["GET"]);
    assert_eq!(urls, vec!["/test"]);
}

// Upstream: github.com/grafana/dskit/httpgrpc/httpgrpc_test.go::TestToHeader
#[test]
fn test_to_header() {
    let grpc_headers = vec![
        HttpGrpcHeader {
            key: "X-Header".to_string(),
            values: vec!["a".into(), "b".into(), "c".into()],
        },
        HttpGrpcHeader {
            key: "traceparent".to_string(),
            values: vec!["01234".into()],
        },
    ];
    let mut http_headers = HeaderMap::new();
    httpgrpc_to_headers(&grpc_headers, &mut http_headers);

    let x_vals: Vec<_> = http_headers
        .get_all("X-Header")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    assert_eq!(x_vals, vec!["a", "b", "c"]);
    assert_eq!(
        http_headers.get("traceparent").unwrap().to_str().unwrap(),
        "01234"
    );
    assert_eq!(http_canonical_header_key("traceparent"), "Traceparent");
}

// Upstream: github.com/grafana/dskit/httpgrpc/httpgrpc_test.go::TestErrorf
#[test]
fn test_errorf() {
    let code = 400;
    let err_msg = "this is an error";
    let expected = HttpResponse {
        code,
        headers: Vec::new(),
        body: Bytes::from_static(err_msg.as_bytes()),
    };
    let status = httpgrpc_error(code, err_msg);
    let rpc_status = decode_status_details(&status).expect("google.rpc.Status details");
    assert_eq!(rpc_status.code, code);
    assert_eq!(status.message(), err_msg);
    let extracted = extract_http_response(&status).expect("HttpResponse in details");
    assert_eq!(extracted, expected);
}

// Upstream: github.com/grafana/dskit/httpgrpc/httpgrpc_test.go::TestErrorFromHTTPResponse
#[test]
fn test_error_from_http_response() {
    let code = 400;
    let err_msg = "this is an error";
    let headers = vec![HttpGrpcHeader {
        key: "X-Header".to_string(),
        values: vec!["a".into(), "b".into(), "c".into()],
    }];
    let resp = HttpResponse {
        code,
        headers,
        body: Bytes::from_static(err_msg.as_bytes()),
    };
    let status = httpgrpc_error_from_response(&resp);
    let rpc_status = decode_status_details(&status).expect("google.rpc.Status details");
    assert_eq!(rpc_status.code, code);
    assert_eq!(status.message(), err_msg);
    let extracted = extract_http_response(&status).expect("HttpResponse in details");
    assert_eq!(extracted, resp);
}

// Upstream: github.com/grafana/dskit/httpgrpc/httpgrpc_test.go::TestHTTPResponseFromError
#[test]
fn test_http_response_from_error() {
    let msg_err = "this is an error";
    assert!(httpgrpc_response_from_error(&Status::ok("")).is_none());
    assert!(httpgrpc_response_from_error(&Status::internal(msg_err)).is_none());

    let st = httpgrpc_error(400, msg_err);
    let extracted = httpgrpc_response_from_error(&st).expect("httpgrpc error");
    assert_eq!(extracted.code, 400);
    assert_eq!(extracted.body.as_ref(), msg_err.as_bytes());
}

// ============================================================================
// github.com/grafana/dskit/httpgrpc/server/server_test.go (10 tests)
// ============================================================================

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestReturn4XXErrorsOption
#[test]
fn test_return_4xx_errors_option() {
    let router = Router::new().route("/", get(|| async { "test" }));
    let server = HttpGrpcBridge::new(router.clone()).with_return_4xx_errors(false);
    assert!(!server.return_4xx_errors());

    let server = HttpGrpcBridge::new(router).with_return_4xx_errors(true);
    assert!(server.return_4xx_errors());
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestBasic
#[tokio::test]
async fn test_basic() {
    let router = Router::new().route(
        "/hello",
        get(|headers: HeaderMap| async move {
            assert_eq!(
                headers.get("x-scope-orgid").and_then(|v| v.to_str().ok()),
                Some("1")
            );
            "world"
        }),
    );
    let (addr, handle) = start_httpgrpc_test_server(HttpGrpcBridge::new(router)).await;
    let mut client = HttpClient::connect(addr).await.unwrap();
    let resp = client
        .handle(HttpRequest {
            method: "GET".to_string(),
            url: "/hello".to_string(),
            headers: vec![HttpGrpcHeader {
                key: "X-Scope-OrgID".to_string(),
                values: vec!["1".to_string()],
            }],
            body: Bytes::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.code, 200);
    assert_eq!(resp.body.as_ref(), b"world");
    handle.abort();
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestError
#[tokio::test]
async fn test_error() {
    for do_not_log in [true, false] {
        let router = Router::new().route(
            "/hello",
            get(move || async move {
                let mut headers = HeaderMap::new();
                if do_not_log {
                    headers.insert("X-DoNotLogError", HeaderValue::from_static("true"));
                }
                (StatusCode::INTERNAL_SERVER_ERROR, headers, "foo\n")
            }),
        );
        let (addr, handle) = start_httpgrpc_test_server(HttpGrpcBridge::new(router)).await;
        let mut client = HttpClient::connect(addr).await.unwrap();
        let status = client
            .handle(HttpRequest {
                method: "GET".to_string(),
                url: "/hello".to_string(),
                headers: Vec::new(),
                body: Bytes::new(),
            })
            .await
            .unwrap_err();
        let resp = httpgrpc_response_from_error(&status).expect("HttpResponse in error");
        assert_eq!(resp.code, 500);
        assert_eq!(resp.body.as_ref(), b"foo\n");
        assert!(
            !resp.headers.iter().any(|h| h.key.eq_ignore_ascii_case("X-DoNotLogError")),
            "X-DoNotLogError must be stripped"
        );
        handle.abort();
    }
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestServerHandleDoNotLogError
#[tokio::test]
async fn test_server_handle_do_not_log_error() {
    let err_msg = "this is an error\n";
    let cases = [
        (StatusCode::INTERNAL_SERVER_ERROR, true, true),
        (StatusCode::INTERNAL_SERVER_ERROR, false, true),
        (StatusCode::BAD_REQUEST, true, false),
        (StatusCode::BAD_REQUEST, false, false),
    ];
    for (code, do_not_log, expected_error) in cases {
        let router = Router::new().route(
            "/test",
            get(move || async move {
                let mut headers = HeaderMap::new();
                if do_not_log {
                    headers.insert("X-DoNotLogError", HeaderValue::from_static("true"));
                }
                (code, headers, err_msg)
            }),
        );
        let bridge = HttpGrpcBridge::new(router).with_return_4xx_errors(false);
        let mut req = Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/test".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        });
        let err_log = GrpcErrorLog::default();
        req.extensions_mut().insert(err_log.clone());
        let res = bridge.handle(req).await;
        if expected_error {
            let status = res.unwrap_err();
            if do_not_log {
                let entry = err_log.take().expect("DoNotLogError recorded");
                assert!(!entry.keep);
            } else {
                assert!(err_log.take().is_none());
            }
            let resp = httpgrpc_response_from_error(&status).unwrap();
            assert_eq!(resp.code, code.as_u16() as i32);
            assert_eq!(resp.body.as_ref(), err_msg.as_bytes());
            assert!(!resp.headers.iter().any(|h| h.key == "X-DoNotLogError"));
        } else {
            let resp = res.unwrap().into_inner();
            assert_eq!(resp.code, code.as_u16() as i32);
            assert_eq!(resp.body.as_ref(), err_msg.as_bytes());
            assert!(!resp.headers.iter().any(|h| h.key == "X-DoNotLogError"));
        }
    }
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestServerHandleReturn4XXErrors
#[tokio::test]
async fn test_server_handle_return_4xx_errors() {
    let err_msg = "this is an error\n";
    let cases = [
        (StatusCode::INTERNAL_SERVER_ERROR, true, true),
        (StatusCode::INTERNAL_SERVER_ERROR, false, true),
        (StatusCode::BAD_REQUEST, true, true),
        (StatusCode::BAD_REQUEST, false, false),
        (StatusCode::NO_CONTENT, true, false),
        (StatusCode::NO_CONTENT, false, false),
    ];
    for (code, return_4xx, expected_error) in cases {
        let router = Router::new().route(
            "/test",
            get(move || async move { (code, err_msg) }),
        );
        let bridge = HttpGrpcBridge::new(router).with_return_4xx_errors(return_4xx);
        let res = bridge
            .handle(Request::new(HttpRequest {
                method: "GET".to_string(),
                url: "/test".to_string(),
                headers: Vec::new(),
                body: Bytes::new(),
            }))
            .await;
        if expected_error {
            let status = res.unwrap_err();
            let resp = httpgrpc_response_from_error(&status).unwrap();
            assert_eq!(resp.code, code.as_u16() as i32);
            assert_eq!(resp.body.as_ref(), err_msg.as_bytes());
        } else {
            let resp = res.unwrap().into_inner();
            assert_eq!(resp.code, code.as_u16() as i32);
            assert_eq!(resp.body.as_ref(), err_msg.as_bytes());
        }
    }
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestParseURL
#[test]
fn test_parse_url() {
    let cases = [
        ("direct://foo", Ok("foo")),
        ("kubernetes://foo:123", Ok("kubernetes:///foo:123")),
        ("querier.cortex:995", Ok("kubernetes:///querier.cortex:995")),
        ("foo.bar.svc.local:995", Ok("kubernetes:///foo.bar.svc.local:995")),
        ("kubernetes:///foo:123", Ok("kubernetes:///foo:123")),
        ("dns:///foo.bar.svc.local:995", Ok("dns:///foo.bar.svc.local:995")),
        ("monster://foo:995", Err("unrecognised scheme: monster")),
    ];
    for (input, expected) in cases {
        let got = parse_httpgrpc_url(input);
        match expected {
            Ok(exp) => assert_eq!(got.unwrap(), exp, "input={input}"),
            Err(exp_err) => assert_eq!(got.unwrap_err(), exp_err, "input={input}"),
        }
    }
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestTracePropagation
#[tokio::test]
async fn test_trace_propagation() {
    struct NoopLogger;
    impl Logger for NoopLogger {
        fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
    }
    std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
    let _ = init_otel_or_jaeger_from_env("test", &NoopLogger, None);

    let router = Router::new().route(
        "/hello",
        get(|headers: HeaderMap| async move {
            let from_ctx = current_trace_context().unwrap();
            let from_hdr = extract_from_http_headers(&headers);
            assert!(from_ctx.is_valid());
            assert_eq!(from_ctx.trace_id, from_hdr.trace_id);
            from_ctx.tracestate.unwrap_or_default()
        }),
    );
    let (addr, handle) = start_httpgrpc_test_server(HttpGrpcBridge::new(router)).await;
    let mut client = HttpClient::connect(addr).await.unwrap();

    let ctx = TraceContext {
        trace_id: [1; 16],
        span_id: [2; 8],
        sampled: true,
        trace_flags: 1,
        remote: false,
        jaeger_debug_id: None,
        tracestate: Some("world".to_string()),
    };

    let httpgrpc_headers = vec![
        HttpGrpcHeader {
            key: "Traceparent".to_string(),
            values: vec![ctx.to_traceparent()],
        },
        HttpGrpcHeader {
            key: "Tracestate".to_string(),
            values: vec!["world".to_string()],
        },
    ];

    let resp = client
        .handle(HttpRequest {
            method: "GET".to_string(),
            url: "/hello".to_string(),
            headers: httpgrpc_headers,
            body: Bytes::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.code, 200);
    assert_eq!(resp.body.as_ref(), b"world");
    handle.abort();
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestGrpcErrorsHaveCorrectMessage
#[tokio::test]
async fn test_grpc_errors_have_correct_message() {
    let cases: &[(&[u8], Option<&str>, &str, bool)] = &[
        (b"hello world", None, "hello world", true),
        (
            b"\x08\x08\x12\xc7\x03the request has been rejected",
            None,
            "�",
            false,
        ),
        (
            b"\x08\x08\x12\xc7\x03the request has been rejected",
            Some("hello world"),
            "hello world",
            true,
        ),
    ];
    for &(body_bytes, header_msg, expected_msg_substr, has_details) in cases {
        let router = Router::new().route(
            "/test",
            get(move || async move {
                let mut headers = HeaderMap::new();
                if let Some(m) = header_msg {
                    headers.insert("X-ErrorMessage", HeaderValue::from_str(m).unwrap());
                }
                (StatusCode::INTERNAL_SERVER_ERROR, headers, Bytes::from_static(body_bytes))
            }),
        );
        let bridge = HttpGrpcBridge::new(router);
        let err = bridge
            .handle(Request::new(HttpRequest {
                method: "GET".to_string(),
                url: "/test".to_string(),
                headers: Vec::new(),
                body: Bytes::new(),
            }))
            .await
            .unwrap_err();
        assert!(
            err.message().contains(expected_msg_substr),
            "got message={:?}",
            err.message()
        );
        if has_details {
            let http_resp = httpgrpc_response_from_error(&err).expect("HttpResponse details");
            assert!(
                !http_resp
                    .headers
                    .iter()
                    .any(|h| h.key.eq_ignore_ascii_case("X-ErrorMessage")),
                "X-ErrorMessage must be removed: {:?}",
                http_resp.headers
            );
        } else {
            assert!(err.details().is_empty());
        }
    }
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestIsHandledByHttpgrpcServer
#[tokio::test]
async fn test_is_handled_by_httpgrpc_server() {
    assert!(!is_handled_by_httpgrpc_server(None));

    const TEST_HEADER: &str = "X-HandledByHttpgrpcServer";
    let handler = |req: AxumRequest| async move {
        let handled = is_handled_by_httpgrpc_server(Some(&req));
        let mut resp = axum::response::Response::new(axum::body::Body::empty());
        resp.headers_mut().insert(
            TEST_HEADER,
            HeaderValue::from_str(if handled { "true" } else { "false" }).unwrap(),
        );
        resp
    };

    // Running outside httpgrpc.Server.
    use tower::ServiceExt as _;
    let direct_router = Router::new().route("/test", get(handler));
    let direct_resp = direct_router
        .clone()
        .oneshot(AxumRequest::builder().uri("/test").body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(direct_resp.headers().get(TEST_HEADER).unwrap(), "false");

    // Running inside httpgrpc.Server (HttpGrpcBridge).
    let bridge = HttpGrpcBridge::new(direct_router);
    let resp = bridge
        .handle(Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/test".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.code, 200);
    assert_eq!(
        resp.headers,
        vec![HttpGrpcHeader {
            key: http_canonical_header_key(TEST_HEADER),
            values: vec!["true".to_string()],
        }]
    );
}

// Upstream: github.com/grafana/dskit/httpgrpc/server/server_test.go::TestErrAbortHandlerIsHandled
#[tokio::test]
async fn test_err_abort_handler_is_handled() {
    let router = Router::new().route(
        "/test",
        any(|| async move {
            std::panic::panic_any(ErrAbortHandler);
            #[allow(unreachable_code)]
            (StatusCode::OK, "")
        }),
    );
    let bridge = HttpGrpcBridge::new(router);
    let err = bridge
        .handle(Request::new(HttpRequest {
            method: "GET".to_string(),
            url: "/test".to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        }))
        .await
        .unwrap_err();
    let http_resp = httpgrpc_response_from_error(&err).expect("HttpResponse in error");
    assert_eq!(http_resp.code, 500);
    assert_eq!(http_resp.body.as_ref(), b"response body truncated");
}
