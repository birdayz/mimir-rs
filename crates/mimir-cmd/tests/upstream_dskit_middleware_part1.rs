//! Ported from github.com/grafana/dskit/middleware/*_test.go (Part 1: 24 tests).

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request as HttpRequest, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt as _;
use mimir_cmd::server::dskit_mw::{
    new_invalid_cluster_requests_counter, noop_cluster_validation_reporter, timeout_middleware,
    ClusterUnaryClientInterceptor, ClusterUnaryServerInterceptor, ClusterValidationErrorBody,
    ClusterValidationRoundTripper,
};
use mimir_cmd::server::grpc_server::{serve as serve_grpc, GrpcContext, GrpcServerConfig};
use mimir_cmd::server::http_middleware::{
    dskit_http_middleware, HttpClusterValidation, HttpMiddlewareConfig,
};
use mimir_cmd::server::http_server::{
    serve as serve_http, HttpServerConfig, ServedOverHttp, SharedWriteDeadline,
};
use mimir_cmd::server::listener::{AcceptOptions, Acceptor, CountingListener, NetConn, NetListener};
use mimir_cmd::server::metrics::{
    apply_instrumentation_options, error_to_grpc_status_code,
    instrumentation_status_code_to_string, InstrumentationOption, InstrumentedClientStream,
    MockableClientStream, ServerMetrics, StreamOpError, StreamTracker,
};
use mimir_common::errors::DoNotLogError;
use mimir_common::grpc_log::{GrpcErrorLog, GrpcErrorLogEntry};
use mimir_common::logging::{LogFormat, LogLevel, LogSink, Logger, OptionalLogging, SampledError, Sampler};
use mimir_proto::grpcutilpb::{
    status_with_grpcutil_error_details, ErrorCause, ErrorDetails,
};
use parking_lot::Mutex;
use mimir_common::native_histogram::{HistogramOpts as NativeHistOpts, HistogramVec as NativeHistVec};
use prometheus::{Gauge, Opts, Registry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};
use tower::ServiceExt as _;

#[derive(Default)]
struct CaptureLogger {
    buf: Arc<Mutex<Vec<u8>>>,
    sink: Option<LogSink>,
}

impl CaptureLogger {
    fn new(level: LogLevel) -> Self {
        let buf = Arc::new(Mutex::new(Vec::new()));
        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl io::Write for BufWriter {
            fn write(&mut self, data: &[u8]) -> io::Result<usize> {
                self.0.lock().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let sink = LogSink::new(LogFormat::Logfmt, level, Box::new(BufWriter(Arc::clone(&buf))));
        Self {
            buf,
            sink: Some(sink),
        }
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock()).into_owned()
    }

    fn clear(&self) {
        self.buf.lock().clear();
    }
}

impl Logger for CaptureLogger {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        if let Some(ref s) = self.sink {
            s.log(level, kvs);
        }
    }
}

fn gather_counter(reg: &Registry, name: &str) -> Vec<(Vec<(String, String)>, f64)> {
    let mut out = Vec::new();
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                let labels = m
                    .get_label()
                    .iter()
                    .map(|l| (l.get_name().to_string(), l.get_value().to_string()))
                    .collect();
                out.push((labels, m.get_counter().get_value()));
            }
        }
    }
    out
}

fn gather_histogram_count(reg: &Registry, name: &str, label_filter: (&str, &str)) -> u64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if m.get_label()
                    .iter()
                    .any(|l| l.get_name() == label_filter.0 && l.get_value() == label_filter.1)
                {
                    return m.get_histogram().get_sample_count();
                }
            }
        }
    }
    0
}

fn gather_histogram_sum(reg: &Registry, name: &str, route: &str) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "route" && l.get_value() == route)
                {
                    return m.get_histogram().get_sample_sum();
                }
            }
        }
    }
    0.0
}

fn gather_gauge(reg: &Registry, name: &str, route: Option<&str>) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if let Some(r) = route {
                    if !m
                        .get_label()
                        .iter()
                        .any(|l| l.get_name() == "route" && l.get_value() == r)
                    {
                        continue;
                    }
                }
                return Some(m.get_gauge().get_value());
            }
        }
    }
    None
}

// ============================================================================
// counting_listener_test.go (1)
// ============================================================================

struct FakeConn {
    close_err: Option<String>,
}

impl NetConn for FakeConn {
    fn close(&mut self) -> io::Result<()> {
        match &self.close_err {
            Some(e) => Err(io::Error::other(e.clone())),
            None => Ok(()),
        }
    }
}

struct FakeListener {
    accept_err: Option<String>,
    close_err: Option<String>,
}

impl NetListener for FakeListener {
    type Conn = FakeConn;
    fn accept_conn(&mut self) -> io::Result<Self::Conn> {
        match &self.accept_err {
            Some(e) => Err(io::Error::other(e.clone())),
            None => Ok(FakeConn {
                close_err: self.close_err.clone(),
            }),
        }
    }
}

// Upstream: github.com/grafana/dskit/middleware/counting_listener_test.go::TestCountingListener
#[test]
fn test_counting_listener() {
    let reg = Registry::new();
    let g = Gauge::with_opts(Opts::new("gauge", "test").namespace("test")).unwrap();
    reg.register(Box::new(g.clone())).unwrap();

    let mut l = CountingListener::new(
        FakeListener {
            accept_err: None,
            close_err: None,
        },
        g.clone(),
    );
    assert_eq!(g.get(), 0.0);

    // Accepting connections should increment the gauge.
    let mut c1 = l.accept().unwrap();
    assert_eq!(g.get(), 1.0);
    let mut c2 = l.accept().unwrap();
    assert_eq!(g.get(), 2.0);

    // Closing connections should decrement the gauge.
    assert!(c1.close().is_ok());
    assert_eq!(g.get(), 1.0);
    assert!(c2.close().is_ok());
    assert_eq!(g.get(), 0.0);

    // Duplicate calls to close should not decrement.
    assert!(c1.close().is_ok());
    assert_eq!(g.get(), 0.0);

    // Accept errors should not cause an increment.
    let mut l = CountingListener::new(
        FakeListener {
            accept_err: Some("accept".to_string()),
            close_err: None,
        },
        g.clone(),
    );
    assert!(l.accept().is_err());
    assert_eq!(g.get(), 0.0);

    // Close errors should still decrement.
    let mut l = CountingListener::new(
        FakeListener {
            accept_err: None,
            close_err: Some("close".to_string()),
        },
        g.clone(),
    );
    let mut c3 = l.accept().unwrap();
    assert_eq!(g.get(), 1.0);
    assert!(c3.close().is_err());
    assert_eq!(g.get(), 0.0);
}

// ============================================================================
// grpc_cluster_test.go (3)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/grpc_cluster_test.go::TestClusterUnaryClientInterceptor
#[test]
fn test_cluster_unary_client_interceptor() {
    // 1. Empty cluster panics.
    let panicked = std::panic::catch_unwind(|| {
        ClusterUnaryClientInterceptor::new("", noop_cluster_validation_reporter());
    });
    assert!(panicked.is_err());

    // 2. Cluster propagated to invoker.
    let interceptor =
        ClusterUnaryClientInterceptor::new("cluster", noop_cluster_validation_reporter());
    let mut md = MetadataMap::new();
    let res = interceptor.intercept(&mut md, "GET", |md| {
        assert_eq!(md.get("x-cluster").unwrap().to_str().unwrap(), "cluster");
        Ok(())
    });
    assert!(res.is_ok());

    // 3. Wrong cluster error translated to Internal + reported + metric incremented.
    let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
    let reg = Registry::new();
    let counter =
        prometheus::IntCounterVec::new(Opts::new("test_request_invalid_cluster_validation_labels_total", "help"), &["method"])
            .unwrap();
    reg.register(Box::new(counter.clone())).unwrap();
    let l_c = Arc::clone(&logger);
    let c_c = counter.clone();
    let interceptor = ClusterUnaryClientInterceptor::new(
        "cluster",
        Arc::new(move |msg, method| {
            l_c.log(
                LogLevel::Warn,
                &[
                    ("msg", msg),
                    ("method", method),
                    ("cluster_validation_labels", "[cluster]"),
                ],
            );
            c_c.with_label_values(&[method]).inc();
        }),
    );
    let mut md = MetadataMap::new();
    let err = interceptor
        .intercept(&mut md, "GET", |_| {
            Err(Box::new(status_with_grpcutil_error_details(
                Code::FailedPrecondition,
                r#"request intended for cluster "cluster" - this is cluster "another-cluster""#,
                &ErrorDetails {
                    cause: ErrorCause::WrongClusterValidationLabel as i32,
                },
            )))
        })
        .unwrap_err();
    let st = err.downcast_ref::<Status>().unwrap();
    assert_eq!(st.code(), Code::Internal);
    assert_eq!(
        st.message(),
        r#"request rejected by the server: request intended for cluster "cluster" - this is cluster "another-cluster""#
    );
    assert_eq!(counter.with_label_values(&["GET"]).get(), 1);
    assert!(logger.text().contains("level=warn"));
    assert!(logger.text().contains("method=GET"));

    // 4. Generic error propagated unchanged.
    #[derive(Debug)]
    struct GenericErr;
    impl std::fmt::Display for GenericErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("generic error")
        }
    }
    impl std::error::Error for GenericErr {}
    let mut md = MetadataMap::new();
    let err = interceptor
        .intercept(&mut md, "GET", |_| Err(Box::new(GenericErr)))
        .unwrap_err();
    assert_eq!(err.to_string(), "generic error");
}

// Upstream: github.com/grafana/dskit/middleware/grpc_cluster_test.go::TestClusterUnaryServerInterceptor
#[test]
fn test_cluster_unary_server_interceptor() {
    // Empty server cluster panics.
    let reg = Registry::new();
    let counter = new_invalid_cluster_requests_counter(&reg, "test").unwrap();
    let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ClusterUnaryServerInterceptor::new(vec![], false, counter, logger);
    }));
    assert!(panicked.is_err());

    for soft in [false, true] {
        // Matching cluster succeeds.
        let reg = Registry::new();
        let counter = new_invalid_cluster_requests_counter(&reg, "test").unwrap();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let interceptor = ClusterUnaryServerInterceptor::new(
            vec!["cluster".to_string()],
            soft,
            counter,
            Arc::clone(&logger) as Arc<dyn Logger>,
        );
        let mut md = MetadataMap::new();
        md.insert("x-cluster", "cluster".parse().unwrap());
        assert!(interceptor.check(&md, "/Test/Me", false, None, None).is_ok());
        assert!(logger.text().is_empty());

        // Wrong cluster.
        let reg = Registry::new();
        let counter = new_invalid_cluster_requests_counter(&reg, "test").unwrap();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let interceptor = ClusterUnaryServerInterceptor::new(
            vec!["cluster".to_string()],
            soft,
            counter.clone(),
            Arc::clone(&logger) as Arc<dyn Logger>,
        );
        let mut md = MetadataMap::new();
        md.insert("x-cluster", "wrong-cluster".parse().unwrap());
        let res = interceptor.check(&md, "/Test/Me", false, None, None);
        if soft {
            assert!(res.is_ok());
        } else {
            let st = res.unwrap_err();
            assert_eq!(st.code(), Code::FailedPrecondition);
            assert!(st.message().contains("wrong-cluster"));
        }
        assert_eq!(
            counter
                .with_label_values(&["grpc", "/Test/Me", "cluster", "wrong-cluster"])
                .get(),
            1
        );
        assert!(logger.text().contains("request with wrong cluster validation label"));

        // Empty or missing cluster.
        for empty_md in [
            {
                let mut m = MetadataMap::new();
                m.insert("x-cluster", "".parse().unwrap());
                m
            },
            MetadataMap::new(),
        ] {
            let reg = Registry::new();
            let counter = new_invalid_cluster_requests_counter(&reg, "test").unwrap();
            let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
            let interceptor = ClusterUnaryServerInterceptor::new(
                vec!["cluster".to_string()],
                soft,
                counter.clone(),
                Arc::clone(&logger) as Arc<dyn Logger>,
            );
            let res = interceptor.check(&empty_md, "/Test/Me", false, None, None);
            if soft {
                assert!(res.is_ok());
            } else {
                let st = res.unwrap_err();
                assert_eq!(st.code(), Code::FailedPrecondition);
                assert!(st.message().contains("empty cluster validation label"));
            }
            assert_eq!(
                counter
                    .with_label_values(&["grpc", "/Test/Me", "cluster", ""])
                    .get(),
                1
            );
            assert!(logger.text().contains("request with no cluster validation label"));
        }

        // Multiple cluster values.
        let reg = Registry::new();
        let counter = new_invalid_cluster_requests_counter(&reg, "test").unwrap();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let interceptor = ClusterUnaryServerInterceptor::new(
            vec!["cluster".to_string()],
            soft,
            counter.clone(),
            Arc::clone(&logger) as Arc<dyn Logger>,
        );
        let mut md = MetadataMap::new();
        md.append("x-cluster", "cluster".parse().unwrap());
        md.append("x-cluster", "another-cluster".parse().unwrap());
        let res = interceptor.check(&md, "/Test/Me", false, None, None);
        if soft {
            assert!(res.is_ok());
        } else {
            let st = res.unwrap_err();
            assert_eq!(st.code(), Code::FailedPrecondition);
            assert!(st
                .message()
                .contains("gRPC metadata should contain exactly 1 value for key \"x-cluster\""));
        }
        assert!(logger
            .text()
            .contains("detected error during cluster validation label extraction"));
    }
}

// Upstream: github.com/grafana/dskit/middleware/grpc_cluster_test.go::TestClusterUnaryServerInterceptorWithHealthServer
#[test]
fn test_cluster_unary_server_interceptor_with_health_server() {
    let reg = Registry::new();
    let counter = new_invalid_cluster_requests_counter(&reg, "test").unwrap();
    let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
    let interceptor = ClusterUnaryServerInterceptor::new(
        vec!["good-cluster".to_string()],
        false,
        counter.clone(),
        logger,
    );
    let mut md = MetadataMap::new();
    md.insert("x-cluster", "bad-cluster".parse().unwrap());

    assert!(interceptor.check(&md, "/Test/Me", true, None, None).is_ok());
    assert!(gather_counter(&reg, "test_server_invalid_cluster_validation_label_requests_total").is_empty());

    let st = interceptor.check(&md, "/Test/Me", false, None, None).unwrap_err();
    assert_eq!(st.code(), Code::FailedPrecondition);
    assert_eq!(
        counter
            .with_label_values(&["grpc", "/Test/Me", "good-cluster", "bad-cluster"])
            .get(),
        1
    );
}

// ============================================================================
// grpc_instrumentation_test.go (8)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestInstrumentationLabel_StatusCodeToString
#[test]
fn test_instrumentation_label_status_code_to_string() {
    let cases = [
        (0, true, "OK", "OK"),
        (0, false, "2xx", "success"),
        (500, true, "5xx", "500"),
        (500, false, "5xx", "500"),
        (503, true, "5xx", "503"),
        (503, false, "5xx", "503"),
        (400, true, "4xx", "400"),
        (400, false, "4xx", "400"),
        (429, true, "4xx", "429"),
        (429, false, "4xx", "429"),
        (Code::Internal as i32, true, "Internal", "Internal"),
        (Code::Internal as i32, false, "error", "error"),
        (Code::Unavailable as i32, true, "Unavailable", "Unavailable"),
        (Code::Unavailable as i32, false, "error", "error"),
        (Code::FailedPrecondition as i32, true, "FailedPrecondition", "FailedPrecondition"),
        (Code::FailedPrecondition as i32, false, "error", "error"),
        (Code::ResourceExhausted as i32, true, "ResourceExhausted", "ResourceExhausted"),
        (Code::ResourceExhausted as i32, false, "error", "error"),
        (Code::Cancelled as i32, true, "Canceled", "Canceled"),
        (Code::Cancelled as i32, false, "cancel", "cancel"),
        (Code::Unknown as i32, true, "Unknown", "Unknown"),
        (Code::Unknown as i32, false, "error", "error"),
    ];
    for (code, report_grpc, exp_masked, exp_unmasked) in cases {
        assert_eq!(
            instrumentation_status_code_to_string(code, report_grpc, true),
            exp_masked,
            "code={code} report_grpc={report_grpc} masked"
        );
        assert_eq!(
            instrumentation_status_code_to_string(code, report_grpc, false),
            exp_unmasked,
            "code={code} report_grpc={report_grpc} unmasked"
        );
    }
}

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestInstrumentationLabel_ErrorToStatusCode
#[test]
fn test_instrumentation_label_error_to_status_code() {
    #[derive(Debug)]
    struct WrappedErr(&'static str, Box<dyn std::error::Error + 'static>);
    impl std::fmt::Display for WrappedErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for WrappedErr {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.1.as_ref())
        }
    }
    #[derive(Debug)]
    struct SimpleErr(&'static str);
    impl std::fmt::Display for SimpleErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for SimpleErr {}

    assert_eq!(error_to_grpc_status_code(None), Code::Ok as i32);
    let st_internal = Status::internal("fail");
    assert_eq!(error_to_grpc_status_code(Some(&st_internal)), Code::Internal as i32);
    let wrapped_fp = WrappedErr("wrapped", Box::new(Status::failed_precondition("fail")));
    assert_eq!(
        error_to_grpc_status_code(Some(&wrapped_fp)),
        Code::FailedPrecondition as i32
    );
    let st_cancelled = Status::cancelled("context canceled");
    assert_eq!(
        error_to_grpc_status_code(Some(&st_cancelled)),
        Code::Cancelled as i32
    );
    let ctx_canceled = SimpleErr("context canceled");
    assert_eq!(
        error_to_grpc_status_code(Some(&ctx_canceled)),
        Code::Cancelled as i32
    );
    let generic = SimpleErr("fail");
    assert_eq!(
        error_to_grpc_status_code(Some(&generic)),
        Code::Unknown as i32
    );
    let http_500 = mimir_cmd::grpc::httpgrpc_error(500, "fail");
    assert_eq!(error_to_grpc_status_code(Some(&http_500)), 500);
    let http_429 = mimir_cmd::grpc::httpgrpc_error(429, "fail");
    assert_eq!(error_to_grpc_status_code(Some(&http_429)), 429);
}

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestApplyInstrumentationLabelOptions
#[test]
fn test_apply_instrumentation_label_options() {
    let cfg = apply_instrumentation_options(false, &[]);
    assert!(!cfg.report_grpc_status);
    assert!(!cfg.mask_http_status);

    let cfg = apply_instrumentation_options(false, &[InstrumentationOption::ReportGrpcStatus]);
    assert!(cfg.report_grpc_status);
    assert!(!cfg.mask_http_status);

    let cfg = apply_instrumentation_options(true, &[InstrumentationOption::ReportGrpcStatus]);
    assert!(cfg.report_grpc_status);
    assert!(cfg.mask_http_status);
}

#[derive(Default)]
struct MockStream {
    recv_err: Option<StreamOpError>,
    send_err: Option<StreamOpError>,
    header_err: Option<StreamOpError>,
    close_send_err: Option<StreamOpError>,
}

impl MockableClientStream for MockStream {
    fn send_msg(&mut self) -> Result<(), StreamOpError> {
        self.send_err.clone().map_or(Ok(()), Err)
    }
    fn recv_msg(&mut self) -> Result<(), StreamOpError> {
        self.recv_err.clone().map_or(Ok(()), Err)
    }
    fn header(&mut self) -> Result<(), StreamOpError> {
        self.header_err.clone().map_or(Ok(()), Err)
    }
    fn close_send(&mut self) -> Result<(), StreamOpError> {
        self.close_send_err.clone().map_or(Ok(()), Err)
    }
}

fn setup_stream_test(server_streams: bool) -> (InstrumentedClientStream<MockStream>, Registry) {
    let reg = Registry::new();
    let hist = NativeHistVec::new(
        NativeHistOpts::new("client_request_duration_seconds", "help"),
        &["method", "result"],
    )
    .unwrap();
    reg.register(Box::new(hist.clone())).unwrap();
    let stream = InstrumentedClientStream::new(
        MockStream::default(),
        hist,
        "/thing.Server/DoThing",
        server_streams,
        &[],
    );
    (stream, reg)
}

fn assert_does_not_increment_after_completion(
    stream: &mut InstrumentedClientStream<MockStream>,
    reg: &Registry,
    result: &str,
) {
    stream.inner_mut().header_err = Some(StreamOpError::Other("header fail".into()));
    assert!(stream.header().is_err());
    assert_eq!(
        gather_histogram_count(reg, "client_request_duration_seconds", ("result", result)),
        1
    );

    stream.inner_mut().send_err = Some(StreamOpError::Other("send fail".into()));
    assert!(stream.send_msg().is_err());
    assert_eq!(
        gather_histogram_count(reg, "client_request_duration_seconds", ("result", result)),
        1
    );

    stream.inner_mut().recv_err = Some(StreamOpError::Other("recv fail".into()));
    assert!(stream.recv_msg().is_err());
    assert_eq!(
        gather_histogram_count(reg, "client_request_duration_seconds", ("result", result)),
        1
    );

    stream.inner_mut().recv_err = Some(StreamOpError::Eof);
    assert_eq!(stream.recv_msg().unwrap_err(), StreamOpError::Eof);
    assert_eq!(
        gather_histogram_count(reg, "client_request_duration_seconds", ("result", result)),
        1
    );

    stream.inner_mut().close_send_err = Some(StreamOpError::Other("close fail".into()));
    assert!(stream.close_send().is_err());
    assert_eq!(
        gather_histogram_count(reg, "client_request_duration_seconds", ("result", result)),
        1
    );

    stream.cancel();
    assert_eq!(
        gather_histogram_count(reg, "client_request_duration_seconds", ("result", result)),
        1
    );
}

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestStreamClientInstrumentInterceptor_SendMsg
#[test]
fn test_stream_client_instrument_interceptor_send_msg() {
    let (mut stream, reg) = setup_stream_test(true);
    assert!(stream.send_msg().is_ok());
    assert!(reg.gather().is_empty());

    stream.inner_mut().send_err = Some(StreamOpError::Eof);
    assert_eq!(stream.send_msg().unwrap_err(), StreamOpError::Eof);
    assert!(reg.gather().is_empty());

    stream.inner_mut().send_err = Some(StreamOpError::Other("boom".into()));
    assert!(stream.send_msg().is_err());
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "error")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "error");
}

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestStreamClientInstrumentInterceptor_RecvMsg
#[test]
fn test_stream_client_instrument_interceptor_recv_msg() {
    // Server streaming: EOF -> 2xx.
    let (mut stream, reg) = setup_stream_test(true);
    assert!(stream.recv_msg().is_ok());
    assert!(reg.gather().is_empty());
    stream.inner_mut().recv_err = Some(StreamOpError::Eof);
    assert_eq!(stream.recv_msg().unwrap_err(), StreamOpError::Eof);
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "2xx")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "2xx");

    // Server streaming: error -> error.
    let (mut stream, reg) = setup_stream_test(true);
    assert!(stream.recv_msg().is_ok());
    stream.inner_mut().recv_err = Some(StreamOpError::Other("boom".into()));
    assert!(stream.recv_msg().is_err());
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "error")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "error");

    // Unary: ok -> 2xx.
    let (mut stream, reg) = setup_stream_test(false);
    assert!(stream.recv_msg().is_ok());
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "2xx")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "2xx");

    // Unary: error -> error.
    let (mut stream, reg) = setup_stream_test(false);
    stream.inner_mut().recv_err = Some(StreamOpError::Other("boom".into()));
    assert!(stream.recv_msg().is_err());
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "error")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "error");
}

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestStreamClientInstrumentInterceptor_Header
#[test]
fn test_stream_client_instrument_interceptor_header() {
    let (mut stream, reg) = setup_stream_test(true);
    assert!(stream.header().is_ok());
    assert!(reg.gather().is_empty());
    stream.inner_mut().header_err = Some(StreamOpError::Other("boom".into()));
    assert!(stream.header().is_err());
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "error")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "error");
}

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestStreamClientInstrumentInterceptor_CloseSend
#[test]
fn test_stream_client_instrument_interceptor_close_send() {
    let (mut stream, reg) = setup_stream_test(true);
    assert!(stream.close_send().is_ok());
    assert!(reg.gather().is_empty());
    stream.inner_mut().close_send_err = Some(StreamOpError::Other("boom".into()));
    assert!(stream.close_send().is_err());
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "error")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "error");
}

// Upstream: github.com/grafana/dskit/middleware/grpc_instrumentation_test.go::TestStreamClientInstrumentInterceptor_ContextCancelled
#[test]
fn test_stream_client_instrument_interceptor_context_cancelled() {
    let (mut stream, reg) = setup_stream_test(true);
    stream.cancel();
    assert_eq!(
        gather_histogram_count(&reg, "client_request_duration_seconds", ("result", "cancel")),
        1
    );
    assert_does_not_increment_after_completion(&mut stream, &reg, "cancel");
}

// ============================================================================
// grpc_logging_test.go (1)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/grpc_logging_test.go::TestGrpcLogging
#[tokio::test]
async fn test_grpc_logging() {
    // 1. context canceled -> debug; generic error -> warn; DoNotLogError -> dropped;
    //    SampledError(should_log=true) -> logged with "(sampled 1/10)"; SampledError(false) -> dropped.
    #[derive(Clone)]
    struct LoggingTestSvc;
    impl tonic::server::NamedService for LoggingTestSvc {
        const NAME: &'static str = "test.Logging";
    }
    impl tower::Service<http::Request<tonic::body::BoxBody>> for LoggingTestSvc {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
            let log = req.extensions().get::<GrpcErrorLog>().cloned();
            let path = req.uri().path().to_string();
            Box::pin(async move {
                match path.as_str() {
                    "/test.Logging/Cancel" => Ok(Status::cancelled("context canceled").into_http()),
                    "/test.Logging/Fail" => {
                        if let Some(l) = log {
                            l.set(GrpcErrorLogEntry {
                                err: "yolo".to_string(),
                                keep: true,
                                reason: None,
                            });
                        }
                        Ok(Status::unknown("yolo").into_http())
                    }
                    "/test.Logging/DoNotLog" => {
                        let dnl = DoNotLogError(std::io::Error::other("yolo"));
                        if let Some(l) = log {
                            l.set(GrpcErrorLogEntry {
                                err: dnl.to_string(),
                                keep: false,
                                reason: None,
                            });
                        }
                        Ok(Status::unknown(dnl.to_string()).into_http())
                    }
                    "/test.Logging/SampledLog" => {
                        let sampler = Sampler::new(10);
                        let se = SampledError::new(
                            std::io::Error::other("yolo"),
                            sampler,
                        );
                        let (keep, reason) = se.should_log();
                        if let Some(l) = log {
                            l.set(GrpcErrorLogEntry {
                                err: se.to_string(),
                                keep,
                                reason: Some(reason.to_string()),
                            });
                        }
                        Ok(Status::unknown(format!("{se} ({reason})")).into_http())
                    }
                    "/test.Logging/SampledDrop" => {
                        let sampler = Sampler::new(10);
                        // Advance sampler once so next call is dropped (2nd of 10).
                        let _ = SampledError::new(std::io::Error::other("skip"), sampler.clone()).should_log();
                        let se = SampledError::new(
                            std::io::Error::other("yolo"),
                            sampler,
                        );
                        let (keep, reason) = se.should_log();
                        if let Some(l) = log {
                            l.set(GrpcErrorLogEntry {
                                err: se.to_string(),
                                keep,
                                reason: Some(reason.to_string()),
                            });
                        }
                        Ok(Status::unknown(format!("{se} ({reason})")).into_http())
                    }
                    _ => Ok(Status::ok("").into_http()),
                }
            })
        }
    }

    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let logger = Arc::new(CaptureLogger::new(LogLevel::Debug));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let acceptor = Acceptor::new(
        listener,
        AcceptOptions {
            conn_limit: 0,
            connections_gauge: metrics.tcp_connections.with_label_values(&["grpc"]),
            proxy_protocol: false,
            proxy_header_timeout: Duration::ZERO,
        },
    );
    let routes = tonic::service::Routes::new(LoggingTestSvc);
    let cfg = GrpcServerConfig {
        max_concurrent_streams: 0,
        keepalive_time: None,
        keepalive_timeout: Duration::from_secs(20),
        max_connection_idle: None,
        max_connection_age: None,
        max_connection_age_grace: None,
        tls: None,
        stats_tracking: false,
        report_grpc_status: true,
        cluster_validation: None,
        multitenancy_enabled: false,
        no_auth_tenant: "anonymous".to_string(),
        tap_ingester: None,
    };
    let srv_handle = tokio::spawn(serve_grpc(
        acceptor,
        cfg,
        routes,
        GrpcContext {
            metrics,
            logger: Arc::clone(&logger) as Arc<dyn Logger>,
        },
        shutdown_rx,
    ));

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    async fn raw_unary(ch: &tonic::transport::Channel, path: &str) -> Status {
        let mut client = tonic::client::Grpc::new(ch.clone());
        client.ready().await.unwrap();
        let req = tonic::Request::new(prost_types::Any::default());
        let p: http::uri::PathAndQuery = path.parse().unwrap();
        let codec = tonic::codec::ProstCodec::<prost_types::Any, prost_types::Any>::default();
        client.unary(req, p, codec).await.unwrap_err()
    }

    logger.clear();
    let _ = raw_unary(&channel, "/test.Logging/Cancel").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t = logger.text();
    assert!(t.contains("level=debug"), "{t}");
    assert!(t.contains("context canceled"), "{t}");

    logger.clear();
    let _ = raw_unary(&channel, "/test.Logging/Fail").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t = logger.text();
    assert!(t.contains("level=warn"), "{t}");
    assert!(t.contains("err=yolo"), "{t}");

    logger.clear();
    let _ = raw_unary(&channel, "/test.Logging/DoNotLog").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(logger.text().is_empty(), "expected empty log, got: {}", logger.text());

    logger.clear();
    let st = raw_unary(&channel, "/test.Logging/SampledLog").await;
    assert!(st.message().contains("(sampled 1/10)"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let t = logger.text();
    assert!(t.contains(r#"err="yolo (sampled 1/10)""#), "{t}");

    logger.clear();
    let st = raw_unary(&channel, "/test.Logging/SampledDrop").await;
    assert!(st.message().contains("(sampled 1/10)"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(logger.text().is_empty(), "expected empty log, got: {}", logger.text());

    let _ = shutdown_tx.send(true);
    let _ = srv_handle.await;
}

// ============================================================================
// grpc_stats_test.go (5)
// ============================================================================

#[derive(Clone, PartialEq, prost::Message)]
struct EchoMsg {
    #[prost(bytes = "bytes", tag = "1")]
    body: Bytes,
}

#[derive(Clone)]
struct MockHealthSvc;

impl tonic::server::NamedService for MockHealthSvc {
    const NAME: &'static str = "grpc.health.v1.Health";
}

impl tower::Service<http::Request<tonic::body::BoxBody>> for MockHealthSvc {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        Box::pin(async move {
            if req.uri().path() == "/grpc.health.v1.Health/Check" {
                struct Check;
                impl tonic::server::UnaryService<mimir_cmd::grpc::HealthCheckRequest> for Check {
                    type Response = mimir_cmd::grpc::HealthCheckResponse;
                    type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<Self::Response>, Status>> + Send + 'static>>;
                    fn call(&mut self, req: tonic::Request<mimir_cmd::grpc::HealthCheckRequest>) -> Self::Future {
                        let svc = req.into_inner().service;
                        Box::pin(async move {
                            if !svc.is_empty() {
                                return Err(Status::not_found("unknown service"));
                            }
                            Ok(tonic::Response::new(mimir_cmd::grpc::HealthCheckResponse { status: 1 }))
                        })
                    }
                }
                let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                    .max_decoding_message_size(16 * 1024 * 1024);
                Ok(grpc.unary(Check, req).await)
            } else {
                Ok(Status::unimplemented("unknown").into_http())
            }
        })
    }
}

#[derive(Clone)]
struct HealthAndEchoSvc {
    echo_delay: Duration,
}

impl tonic::server::NamedService for HealthAndEchoSvc {
    const NAME: &'static str = "middleware.EchoServer";
}

impl tower::Service<http::Request<tonic::body::BoxBody>> for HealthAndEchoSvc {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let echo_delay = self.echo_delay;
        let path = req.uri().path().to_string();
        Box::pin(async move {
            match path.as_str() {
                "/grpc.health.v1.Health/Check" => {
                    struct Check;
                    impl tonic::server::UnaryService<mimir_cmd::grpc::HealthCheckRequest> for Check {
                        type Response = mimir_cmd::grpc::HealthCheckResponse;
                        type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<Self::Response>, Status>> + Send + 'static>>;
                        fn call(&mut self, req: tonic::Request<mimir_cmd::grpc::HealthCheckRequest>) -> Self::Future {
                            let svc = req.into_inner().service;
                            Box::pin(async move {
                                if !svc.is_empty() {
                                    return Err(Status::not_found("unknown service"));
                                }
                                Ok(tonic::Response::new(mimir_cmd::grpc::HealthCheckResponse { status: 1 }))
                            })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .max_decoding_message_size(16 * 1024 * 1024);
                    Ok(grpc.unary(Check, req).await)
                }
                "/middleware.EchoServer/Process" => {
                    struct EchoProc(Duration);
                    impl tonic::server::StreamingService<EchoMsg> for EchoProc {
                        type Response = EchoMsg;
                        type ResponseStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<EchoMsg, Status>> + Send + 'static>>;
                        type Future = Pin<Box<dyn Future<Output = Result<tonic::Response<Self::ResponseStream>, Status>> + Send + 'static>>;
                        fn call(&mut self, req: tonic::Request<tonic::Streaming<EchoMsg>>) -> Self::Future {
                            let delay = self.0;
                            let mut inbound = req.into_inner();
                            let (out_tx, out_rx) = tokio::sync::mpsc::channel::<Result<EchoMsg, Status>>(8);
                            tokio::spawn(async move {
                                while let Some(res) = tokio_stream::StreamExt::next(&mut inbound).await {
                                    match res {
                                        Ok(msg) => {
                                            if !delay.is_zero() {
                                                tokio::time::sleep(delay).await;
                                            }
                                            let half = msg.body.slice(..msg.body.len() / 2);
                                            if out_tx.send(Ok(EchoMsg { body: half })).await.is_err() {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            let _ = out_tx.send(Err(e)).await;
                                            break;
                                        }
                                    }
                                }
                            });
                            let out = tokio_stream::wrappers::ReceiverStream::new(out_rx);
                            Box::pin(async move {
                                Ok(tonic::Response::new(Box::pin(out) as Self::ResponseStream))
                            })
                        }
                    }
                    let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .max_decoding_message_size(16 * 1024 * 1024)
                        .max_encoding_message_size(16 * 1024 * 1024);
                    Ok(grpc.streaming(EchoProc(echo_delay), req).await)
                }
                _ => Ok(Status::unimplemented("unknown").into_http()),
            }
        })
    }
}

async fn start_stats_grpc_server(
    stats_tracking: bool,
    collect_max_streams_by_conn: bool,
    echo_delay: Duration,
) -> (SocketAddr, Registry, watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, stats_tracking, collect_max_streams_by_conn).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let acceptor = Acceptor::new(
        listener,
        AcceptOptions {
            conn_limit: 0,
            connections_gauge: metrics.tcp_connections.with_label_values(&["grpc"]),
            proxy_protocol: false,
            proxy_header_timeout: Duration::ZERO,
        },
    );
    let routes = tonic::service::Routes::new(HealthAndEchoSvc { echo_delay }).add_service(MockHealthSvc);
    let cfg = GrpcServerConfig {
        max_concurrent_streams: 0,
        keepalive_time: None,
        keepalive_timeout: Duration::from_secs(20),
        max_connection_idle: None,
        max_connection_age: None,
        max_connection_age_grace: None,
        tls: None,
        stats_tracking,
        report_grpc_status: true,
        cluster_validation: None,
        multitenancy_enabled: false,
        no_auth_tenant: "anonymous".to_string(),
        tap_ingester: None,
    };
    let handle = tokio::spawn(serve_grpc(
        acceptor,
        cfg,
        routes,
        GrpcContext {
            metrics,
            logger: Arc::new(CaptureLogger::new(LogLevel::Error)),
        },
        shutdown_rx,
    ));
    (addr, reg, shutdown_tx, handle)
}

// Upstream: github.com/grafana/dskit/middleware/grpc_stats_test.go::TestGrpcStats
#[tokio::test]
async fn test_grpc_stats() {
    let (addr, reg, shutdown_tx, handle) = start_stats_grpc_server(true, true, Duration::ZERO).await;
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let call_check = |service: String| {
        let ch = ch.clone();
        async move {
            let mut client = tonic::client::Grpc::new(ch)
                .max_decoding_message_size(16 * 1024 * 1024)
                .max_encoding_message_size(16 * 1024 * 1024);
            client.ready().await.unwrap();
            let codec = tonic::codec::ProstCodec::<
                mimir_cmd::grpc::HealthCheckRequest,
                mimir_cmd::grpc::HealthCheckResponse,
            >::default();
            let p: http::uri::PathAndQuery = "/grpc.health.v1.Health/Check".parse().unwrap();
            client
                .unary(
                    tonic::Request::new(mimir_cmd::grpc::HealthCheckRequest { service }),
                    p,
                    codec,
                )
                .await
        }
    };

    let resp = call_check(String::new()).await.unwrap().into_inner();
    assert_eq!(resp.status, 1);

    let err = call_check("a".repeat(8 * 1024 * 1024)).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    assert_eq!(
        gather_histogram_count(
            &reg,
            "cortex_request_message_bytes",
            ("route", "/grpc.health.v1.Health/Check")
        ),
        2
    );
    assert!(
        gather_histogram_sum(
            &reg,
            "cortex_request_message_bytes",
            "/grpc.health.v1.Health/Check"
        ) > 8_388_600.0
    );
    assert_eq!(
        gather_histogram_count(
            &reg,
            "cortex_response_message_bytes",
            ("route", "/grpc.health.v1.Health/Check")
        ),
        1
    );

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
}

// Upstream: github.com/grafana/dskit/middleware/grpc_stats_test.go::TestGrpcStatsStreaming
#[tokio::test]
async fn test_grpc_stats_streaming() {
    let (addr, reg, shutdown_tx, handle) = start_stats_grpc_server(true, true, Duration::ZERO).await;
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel::<EchoMsg>(8);
    let mut client = tonic::client::Grpc::new(ch)
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024);
    client.ready().await.unwrap();
    let codec = tonic::codec::ProstCodec::<EchoMsg, EchoMsg>::default();
    let p: http::uri::PathAndQuery = "/middleware.EchoServer/Process".parse().unwrap();
    let out_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut resp_stream = client
        .streaming(tonic::Request::new(out_stream), p, codec)
        .await
        .unwrap()
        .into_inner();

    for ix in 0..5usize {
        let body = Bytes::from(vec![b'x'; (ix + 1) * 1024 * 1024]);
        tx.send(EchoMsg { body }).await.unwrap();
        let reply = resp_stream.message().await.unwrap().unwrap();
        assert_eq!(reply.body.len(), (ix + 1) * 1024 * 1024 / 2);
        assert_eq!(
            gather_gauge(&reg, "cortex_inflight_requests", Some("/middleware.EchoServer/Process")),
            Some(1.0)
        );
    }
    drop(tx);
    while resp_stream.message().await.unwrap().is_some() {}

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        gather_gauge(&reg, "cortex_inflight_requests", Some("/middleware.EchoServer/Process")),
        Some(0.0)
    );
    assert_eq!(
        gather_histogram_count(
            &reg,
            "cortex_request_message_bytes",
            ("route", "/middleware.EchoServer/Process")
        ),
        5
    );
    assert_eq!(
        gather_histogram_count(
            &reg,
            "cortex_response_message_bytes",
            ("route", "/middleware.EchoServer/Process")
        ),
        5
    );

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
}

async fn open_n_echo_streams(
    addr: SocketAddr,
    n: usize,
) -> Vec<(tokio::sync::mpsc::Sender<EchoMsg>, tonic::Streaming<EchoMsg>)> {
    let ch = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut streams = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = tokio::sync::mpsc::channel::<EchoMsg>(4);
        let mut client = tonic::client::Grpc::new(ch.clone());
        client.ready().await.unwrap();
        let codec = tonic::codec::ProstCodec::<EchoMsg, EchoMsg>::default();
        let p: http::uri::PathAndQuery = "/middleware.EchoServer/Process".parse().unwrap();
        tx.send(EchoMsg {
            body: Bytes::from_static(b"ping"),
        })
        .await
        .unwrap();
        let resp = client
            .streaming(
                tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx)),
                p,
                codec,
            )
            .await
            .unwrap()
            .into_inner();
        streams.push((tx, resp));
    }
    streams
}

// Upstream: github.com/grafana/dskit/middleware/grpc_stats_test.go::TestGrpcStatsMaxStreamsDisabled
#[tokio::test]
async fn test_grpc_stats_max_streams_disabled() {
    let (addr, reg, shutdown_tx, handle) =
        start_stats_grpc_server(true, false, Duration::from_millis(50)).await;
    let streams = open_n_echo_streams(addr, 5).await;
    assert!(gather_gauge(&reg, "grpc_concurrent_streams_by_conn_max", None).is_none());
    drop(streams);
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
}

// Upstream: github.com/grafana/dskit/middleware/grpc_stats_test.go::TestGrpcStatsMaxStreams
#[tokio::test]
async fn test_grpc_stats_max_streams() {
    let (addr, reg, shutdown_tx, handle) =
        start_stats_grpc_server(true, true, Duration::from_millis(200)).await;

    let s1 = open_n_echo_streams(addr, 10).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        gather_gauge(&reg, "grpc_concurrent_streams_by_conn_max", None),
        Some(10.0)
    );

    let s2 = open_n_echo_streams(addr, 5).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        gather_gauge(&reg, "grpc_concurrent_streams_by_conn_max", None),
        Some(10.0)
    );

    drop(s1);
    for _ in 0..40 {
        if gather_gauge(&reg, "grpc_concurrent_streams_by_conn_max", None) == Some(5.0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        gather_gauge(&reg, "grpc_concurrent_streams_by_conn_max", None),
        Some(5.0)
    );

    drop(s2);
    for _ in 0..40 {
        if gather_gauge(&reg, "grpc_concurrent_streams_by_conn_max", None) == Some(0.0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        gather_gauge(&reg, "grpc_concurrent_streams_by_conn_max", None),
        Some(0.0)
    );

    let _ = shutdown_tx.send(true);
    let _ = handle.await;
}

// Upstream: github.com/grafana/dskit/middleware/grpc_stats_test.go::TestGrpcStreamTracker
#[test]
fn test_grpc_stream_tracker() {
    let tracker = Arc::new(StreamTracker::new().unwrap());
    let conn0 = 0u64;
    let conn1 = 1u64;

    tracker.open_stream(conn0);
    assert_eq!(tracker.max_streams(), 1);
    tracker.open_stream(conn0);
    assert_eq!(tracker.max_streams(), 2);

    tracker.open_stream(conn1);
    assert_eq!(tracker.max_streams(), 2);
    tracker.open_stream(conn1);
    assert_eq!(tracker.max_streams(), 2);
    tracker.open_stream(conn1);
    assert_eq!(tracker.max_streams(), 3);

    tracker.close_stream(conn1);
    assert_eq!(tracker.max_streams(), 2);
    tracker.close_stream(conn0);
    assert_eq!(tracker.max_streams(), 2);
    tracker.close_stream(conn0);
    assert_eq!(tracker.max_streams(), 2);
    tracker.close_stream(conn1);
    assert_eq!(tracker.max_streams(), 1);
    tracker.close_stream(conn1);
    assert_eq!(tracker.max_streams(), 0);

    // Concurrent operations.
    let streams_per_conn = 300usize;
    let mut handles = Vec::new();
    for conn_idx in 0..3u64 {
        let t = Arc::clone(&tracker);
        handles.push(std::thread::spawn(move || {
            for _ in 0..(streams_per_conn + conn_idx as usize) {
                t.open_stream(conn_idx);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(tracker.max_streams(), (streams_per_conn + 2) as i64);

    let mut handles = Vec::new();
    for conn_idx in 0..3u64 {
        let t = Arc::clone(&tracker);
        handles.push(std::thread::spawn(move || {
            for _ in 0..(streams_per_conn / 2) {
                t.close_stream(conn_idx);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(tracker.max_streams(), (streams_per_conn / 2 + 2) as i64);

    for _ in 0..streams_per_conn {
        tracker.close_stream(0);
        tracker.close_stream(2);
    }
    assert_eq!(tracker.max_streams(), (streams_per_conn / 2 + 1) as i64);
    while tracker.max_streams() > 1 {
        let cur = tracker.max_streams();
        tracker.close_stream(1);
        assert_eq!(tracker.max_streams(), cur - 1);
    }
}

// ============================================================================
// http_cluster_test.go (4)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/http_cluster_test.go::TestClusterValidationRoundTripper
#[tokio::test]
async fn test_cluster_validation_round_tripper() {
    let panicked = std::panic::catch_unwind(|| {
        ClusterValidationRoundTripper::new("", noop_cluster_validation_reporter());
    });
    assert!(panicked.is_err());

    let mode = Arc::new(AtomicU64::new(0));
    let mode_srv = Arc::clone(&mode);
    let app = Router::new().route(
        "/Test/Me",
        get(move |headers: axum::http::HeaderMap| {
            let mode_srv = Arc::clone(&mode_srv);
            async move {
                assert_eq!(
                    headers.get("X-Cluster").and_then(|v| v.to_str().ok()),
                    Some("cluster")
                );
                match mode_srv.load(Ordering::Relaxed) {
                    0 => (StatusCode::OK, [(header::CONTENT_TYPE, "text/plain")], "".to_string()),
                    1 => (
                        StatusCode::NETWORK_AUTHENTICATION_REQUIRED,
                        [(header::CONTENT_TYPE, "application/json")],
                        serde_json::to_string(&ClusterValidationErrorBody {
                            cluster_validation_error_message: "this is a cluster validation error".into(),
                            route: "test_argument".into(),
                        })
                        .unwrap(),
                    ),
                    2 => (
                        StatusCode::NETWORK_AUTHENTICATION_REQUIRED,
                        [(header::CONTENT_TYPE, "application/json")],
                        serde_json::to_string(&ClusterValidationErrorBody {
                            cluster_validation_error_message: "this is a cluster validation error".into(),
                            route: "<unknown-route>".into(),
                        })
                        .unwrap(),
                    ),
                    3 => (
                        StatusCode::NETWORK_AUTHENTICATION_REQUIRED,
                        [(header::CONTENT_TYPE, "text/plain")],
                        "generic error".into(),
                    ),
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [(header::CONTENT_TYPE, "text/plain")],
                        "generic error".into(),
                    ),
                }
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
    let counter = prometheus::IntCounterVec::new(
        Opts::new("test_request_invalid_cluster_validation_labels_total", "help"),
        &["method"],
    )
    .unwrap();
    let l_c = Arc::clone(&logger);
    let c_c = counter.clone();
    let rt = ClusterValidationRoundTripper::new(
        "cluster",
        Arc::new(move |msg, route| {
            l_c.log(
                LogLevel::Warn,
                &[
                    ("msg", msg),
                    ("method", route),
                    ("cluster_validation_labels", "[cluster]"),
                ],
            );
            c_c.with_label_values(&[route]).inc();
        }),
    );

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/Test/Me");

    // 0: 200 OK
    mode.store(0, Ordering::Relaxed);
    let resp = rt.send(client.get(&url).build().unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(logger.text().is_empty());

    // 1: 511 JSON with route
    mode.store(1, Ordering::Relaxed);
    let err = rt.send(client.get(&url).build().unwrap()).await.unwrap_err();
    assert!(err.contains("request rejected by the server: this is a cluster validation error"));
    assert_eq!(counter.with_label_values(&["test_argument"]).get(), 1);
    assert!(logger.text().contains("method=test_argument"));

    // 2: 511 JSON with <unknown-route>
    logger.clear();
    mode.store(2, Ordering::Relaxed);
    let err = rt.send(client.get(&url).build().unwrap()).await.unwrap_err();
    assert!(err.contains("request rejected by the server: this is a cluster validation error"));
    assert_eq!(counter.with_label_values(&["<unknown-route>"]).get(), 1);
    assert!(logger.text().contains("method=<unknown-route>"));

    // 3: 511 non-JSON generic error
    mode.store(3, Ordering::Relaxed);
    let resp = rt.send(client.get(&url).build().unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NETWORK_AUTHENTICATION_REQUIRED);
    assert_eq!(resp.text().await.unwrap(), "generic error");

    // 4: 500 generic error
    mode.store(4, Ordering::Relaxed);
    let resp = rt.send(client.get(&url).build().unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(resp.text().await.unwrap(), "generic error");

    srv.abort();
}

fn build_http_mw_app(
    cv: Option<HttpClusterValidation>,
    reg: &Registry,
    logger: Arc<dyn Logger>,
) -> Router {
    let metrics = ServerMetrics::new(reg, false, false).unwrap();
    let cfg = Arc::new(HttpMiddlewareConfig {
        metrics,
        logger,
        log_request_headers: false,
        log_request_at_info_level: false,
        log_source_ips: None,
        trace_source_ips: None,
        exclude_headers: Vec::new(),
        trace_request_headers: false,
        trace_exclude_headers: Vec::new(),
        create_new_traces: false,
        cluster_validation: cv,
        http_path_prefix: String::new(),
        cost_attribution_path: None,
        compartments_enabled: false,
    });
    Router::new()
        .route("/Test/:argument", get(|| async { StatusCode::OK }))
        .route("/metrics", get(|| async { StatusCode::OK }))
        .layer(axum::middleware::from_fn_with_state(cfg, dskit_http_middleware))
}

// Upstream: github.com/grafana/dskit/middleware/http_cluster_test.go::TestClusterValidationMiddleware
#[tokio::test]
async fn test_cluster_validation_middleware() {
    assert!(HttpClusterValidation::new(vec![], false, &[], &[]).is_err());

    for soft in [false, true] {
        // Equal cluster succeeds.
        let reg = Registry::new();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let cv = HttpClusterValidation::new(vec!["cluster".into()], soft, &[], &[]).unwrap();
        let app = build_http_mw_app(Some(cv), &reg, Arc::clone(&logger) as Arc<dyn Logger>);
        let mut req = HttpRequest::builder()
            .uri("/Test/Me")
            .header("X-Cluster", "cluster")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ServedOverHttp {
            remote_addr: "127.0.0.1:1000".parse().unwrap(),
        });
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(logger.text().is_empty());

        // Wrong cluster.
        let reg = Registry::new();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let cv = HttpClusterValidation::new(vec!["cluster".into()], soft, &[], &[]).unwrap();
        let app = build_http_mw_app(Some(cv), &reg, Arc::clone(&logger) as Arc<dyn Logger>);
        let mut req = HttpRequest::builder()
            .uri("/Test/Me")
            .header("X-Cluster", "wrong-cluster")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ServedOverHttp {
            remote_addr: "127.0.0.1:1000".parse().unwrap(),
        });
        let resp = app.oneshot(req).await.unwrap();
        if soft {
            assert_eq!(resp.status(), StatusCode::OK);
        } else {
            assert_eq!(resp.status(), StatusCode::NETWORK_AUTHENTICATION_REQUIRED);
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            let parsed: ClusterValidationErrorBody = serde_json::from_slice(&body).unwrap();
            assert_eq!(parsed.route, "test_argument");
            assert!(parsed.cluster_validation_error_message.contains("wrong-cluster"));
        }
        assert!(logger.text().contains("request with wrong cluster validation label"));

        // Multiple cluster values.
        let reg = Registry::new();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let cv = HttpClusterValidation::new(vec!["cluster".into()], soft, &[], &[]).unwrap();
        let app = build_http_mw_app(Some(cv), &reg, Arc::clone(&logger) as Arc<dyn Logger>);
        let mut req = HttpRequest::builder()
            .uri("/Test/Me")
            .header("X-Cluster", "cluster")
            .header("X-Cluster", "another-cluster")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ServedOverHttp {
            remote_addr: "127.0.0.1:1000".parse().unwrap(),
        });
        let resp = app.oneshot(req).await.unwrap();
        if soft {
            assert_eq!(resp.status(), StatusCode::OK);
        } else {
            assert_eq!(resp.status(), StatusCode::NETWORK_AUTHENTICATION_REQUIRED);
        }
        assert!(logger
            .text()
            .contains("detected error during cluster validation label extraction"));
    }
}

// Upstream: github.com/grafana/dskit/middleware/http_cluster_test.go::TestClusterValidationMiddlewareWithExcludedPaths
#[tokio::test]
async fn test_cluster_validation_middleware_with_excluded_paths() {
    let excluded = vec![
        "Exclude/Me".to_string(),
        "Test/Me".to_string(),
        "Do/Not/Test/Me".to_string(),
    ];
    for (soft, path, excl, expected_code) in [
        (true, "/Test/Me", excluded.clone(), StatusCode::OK),
        (false, "/Test/Me", excluded.clone(), StatusCode::OK),
        (false, "/metrics", excluded.clone(), StatusCode::OK),
        (
            true,
            "/Test/Me",
            vec!["Exclude/Me".into(), "Do/Not/Test/Me".into()],
            StatusCode::OK,
        ),
        (
            false,
            "/Test/Me",
            vec!["Exclude/Me".into(), "Do/Not/Test/Me".into()],
            StatusCode::NETWORK_AUTHENTICATION_REQUIRED,
        ),
    ] {
        let reg = Registry::new();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let cv = HttpClusterValidation::new(vec!["server-cluster".into()], soft, &excl, &[]).unwrap();
        let app = build_http_mw_app(Some(cv), &reg, logger);
        let mut req = HttpRequest::builder()
            .uri(path)
            .header("X-Cluster", "client-cluster")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ServedOverHttp {
            remote_addr: "127.0.0.1:1000".parse().unwrap(),
        });
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), expected_code);
    }
}

// Upstream: github.com/grafana/dskit/middleware/http_cluster_test.go::TestClusterValidationMiddlewareWithExcludedUserAgents
#[tokio::test]
async fn test_cluster_validation_middleware_with_excluded_user_agents() {
    let excl_uas = vec!["curl/.*".to_string(), "wget/.*".to_string()];
    for (soft, ua, expected_code) in [
        (true, "curl/8.7.1", StatusCode::OK),
        (false, "curl/8.7.1", StatusCode::OK),
        (true, "Mozilla/5.0", StatusCode::OK),
        (false, "Mozilla/5.0", StatusCode::NETWORK_AUTHENTICATION_REQUIRED),
    ] {
        let reg = Registry::new();
        let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
        let cv =
            HttpClusterValidation::new(vec!["server-cluster".into()], soft, &[], &excl_uas)
                .unwrap();
        let app = build_http_mw_app(Some(cv), &reg, logger);
        let mut req = HttpRequest::builder()
            .uri("/Test/Me")
            .header("User-Agent", ua)
            .header("X-Cluster", "client-cluster")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ServedOverHttp {
            remote_addr: "127.0.0.1:1000".parse().unwrap(),
        });
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), expected_code);
    }
}

// ============================================================================
// http_timeout_test.go (1)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/http_timeout_test.go::TestTimeoutMiddleware
#[tokio::test]
async fn test_timeout_middleware() {
    let server_write_timeout = Duration::from_millis(80);
    let mw_timeout = Duration::from_millis(250);
    let mw_msg = "foo";

    let wd_cleared = Arc::new(AtomicBool::new(false));
    let wd_cleared_c = Arc::clone(&wd_cleared);
    let handler_cancelled = Arc::new(AtomicBool::new(false));
    let handler_cancelled_c = Arc::clone(&handler_cancelled);

    struct CancelDropGuard(Arc<AtomicBool>);
    impl Drop for CancelDropGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let app = Router::new().route(
        "/",
        get(move |req: axum::extract::Request| {
            let wd_c = Arc::clone(&wd_cleared_c);
            let hc_c = Arc::clone(&handler_cancelled_c);
            async move {
                let wd = req.extensions().get::<SharedWriteDeadline>().cloned();
                timeout_middleware(mw_timeout, mw_msg, req, async move {
                    if let Some(w) = wd {
                        wd_c.store(w.current().is_none(), Ordering::SeqCst);
                    }
                    let _guard = CancelDropGuard(hc_c);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    StatusCode::NO_CONTENT.into_response()
                })
                .await
            }
        }),
    );

    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let acceptor = Acceptor::new(
        listener,
        AcceptOptions {
            conn_limit: 0,
            connections_gauge: metrics.tcp_connections.with_label_values(&["http"]),
            proxy_protocol: false,
            proxy_header_timeout: Duration::ZERO,
        },
    );
    let http_cfg = HttpServerConfig {
        read_timeout: Duration::from_secs(5),
        read_header_timeout: Duration::from_secs(5),
        write_timeout: server_write_timeout,
        idle_timeout: Duration::from_secs(5),
        tls: None,
        graceful_shutdown_timeout: Duration::from_secs(1),
        zero_response_logger: None,
    };
    let srv = tokio::spawn(serve_http(acceptor, http_cfg, app, shutdown_rx));

    let start = std::time::Instant::now();
    let resp = reqwest::get(format!("http://{addr}/")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.text().await.unwrap(), mw_msg);
    assert!(start.elapsed() >= mw_timeout);
    assert!(wd_cleared.load(Ordering::SeqCst));
    assert!(handler_cancelled.load(Ordering::SeqCst));

    let _ = shutdown_tx.send(true);
    let _ = srv.await;
}

// ============================================================================
// zero_response_test.go (1)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/zero_response_test.go::TestZeroResponseLogger
#[tokio::test]
async fn test_zero_response_logger() {
    let logger = Arc::new(CaptureLogger::new(LogLevel::Warn));
    let reg = Registry::new();
    let metrics = ServerMetrics::new(&reg, false, false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let acceptor = Acceptor::new(
        listener,
        AcceptOptions {
            conn_limit: 0,
            connections_gauge: metrics.tcp_connections.with_label_values(&["http"]),
            proxy_protocol: false,
            proxy_header_timeout: Duration::ZERO,
        },
    );
    let http_cfg = HttpServerConfig {
        read_timeout: Duration::ZERO,
        read_header_timeout: Duration::from_millis(250),
        write_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(5),
        tls: None,
        graceful_shutdown_timeout: Duration::from_secs(1),
        zero_response_logger: Some(Arc::clone(&logger) as Arc<dyn Logger>),
    };
    let app = Router::new().route("/", get(|| async { "Hello" }));
    let srv = tokio::spawn(serve_http(acceptor, http_cfg, app, shutdown_rx));

    // Multiple quick requests do not log.
    for _ in 0..10 {
        let resp = reqwest::get(format!("http://{addr}/")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), "Hello");
    }
    assert!(logger.text().is_empty());

    // Slow request logs read bytes.
    logger.clear();
    {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\nHost: somehost:12345\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        assert!(buf.is_empty());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t = logger.text();
    assert!(t.contains("read timeout, connection closed with no response"), "{t}");
    assert!(t.contains(r#"GET / HTTP/1.1\\r\\nHost: somehost:12345\\r\\n"#), "{t}");
    assert!(t.contains("remote="), "{t}");

    // Slow request with multiple writes.
    logger.clear();
    {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        s.write_all(b"X-Org-ScopeID: 54321\r\n").await.unwrap();
        s.write_all(b"Host: somehost:12345\r\n").await.unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        assert!(buf.is_empty());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t = logger.text();
    assert!(t.contains("read timeout, connection closed with no response"), "{t}");
    assert!(
        t.contains(r#"GET / HTTP/1.1\\r\\nX-Org-ScopeID: 54321\\r\\nHost: somehost:12345\\r\\n"#),
        "{t}"
    );

    // Slow request with Authorization header redacted.
    logger.clear();
    {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\n").await.unwrap();
        s.write_all(b"X-Org-ScopeID: 54321\r\n").await.unwrap();
        s.write_all(b"Authorization: Basic YWFhOmJiYg==\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await.unwrap();
        assert!(buf.is_empty());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let t = logger.text();
    assert!(t.contains("read timeout, connection closed with no response"), "{t}");
    assert!(
        t.contains(r#"GET / HTTP/1.1\\r\\nX-Org-ScopeID: 54321\\r\\nAuthorization: Basic ***\\r\\n"#),
        "{t}"
    );

    let _ = shutdown_tx.send(true);
    let _ = srv.await;
}
