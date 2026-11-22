//! Ported from github.com/grafana/dskit/middleware/*_test.go (Part 2: 23 tests).

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use mimir_cmd::server::dskit_mw::{
    extract_value_from_multi_value_header, observe_throughput_from_response, route_injector_name,
    BadResponseLoggingWriter, ConnectionTtlError, HttpConnectionTtlMiddleware, LogMiddleware,
};
use mimir_cmd::server::http_middleware::SourceIps;
use mimir_cmd::server::metrics::make_label_value;
use mimir_common::logging::{LogFormat, LogLevel, LogSink, Logger};
use parking_lot::Mutex;
use prometheus::{HistogramOpts, HistogramVec, Registry};

struct CaptureLogger {
    buf: Arc<Mutex<Vec<u8>>>,
    sink: LogSink,
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
        Self { buf, sink }
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf.lock()).into_owned()
    }
}

impl Logger for CaptureLogger {
    fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
        self.sink.log(level, kvs);
    }
}

fn counter_value(reg: &Registry, name: &str, reason: Option<&str>) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if let Some(r) = reason {
                    if !m
                        .get_label()
                        .iter()
                        .any(|l| l.get_name() == "reason" && l.get_value() == r)
                    {
                        continue;
                    }
                }
                return m.get_counter().get_value();
            }
        }
    }
    0.0
}

fn histogram_count(reg: &Registry, name: &str) -> u64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            if let Some(m) = mf.get_metric().first() {
                return m.get_histogram().get_sample_count();
            }
        }
    }
    0
}

// ============================================================================
// http_connection_ttl_middleware_test.go (7)
// ============================================================================

const CONN1: &str = "1.2.3.4:5";
const CONN2: &str = "1.2.3.4:6";

// Upstream: github.com/grafana/dskit/middleware/http_connection_ttl_middleware_test.go::TestNewHTTPConnectionTTLMiddleware
#[test]
fn test_new_http_connection_ttl_middleware() {
    let reg = Registry::new();
    let m = HttpConnectionTtlMiddleware::new(
        Duration::ZERO,
        Duration::ZERO,
        Duration::from_secs(1),
        &reg,
    );
    assert!(m.is_ok());

    let m = HttpConnectionTtlMiddleware::new(
        Duration::ZERO,
        Duration::ZERO,
        Duration::ZERO,
        &Registry::new(),
    );
    assert!(m.is_ok());

    let err = HttpConnectionTtlMiddleware::new(
        Duration::from_nanos(10),
        Duration::ZERO,
        Duration::from_secs(1),
        &Registry::new(),
    )
    .err()
    .unwrap();
    assert_eq!(err, ConnectionTtlError::MinLessOrEqualThanMax);

    let err = HttpConnectionTtlMiddleware::new(
        Duration::ZERO,
        Duration::from_nanos(5),
        Duration::ZERO,
        &Registry::new(),
    )
    .err()
    .unwrap();
    assert_eq!(
        err,
        ConnectionTtlError::IdleConnectionCheckFrequencyMustBePositive
    );

    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_nanos(5),
        Duration::from_nanos(10),
        Duration::from_secs(1),
        &Registry::new(),
    );
    assert!(m.is_ok());
}

// Upstream: github.com/grafana/dskit/middleware/http_connection_ttl_middleware_test.go::TestHTTPConnectionTTLMiddleware_CalculateTTL
#[test]
fn test_http_connection_ttl_middleware_calculate_ttl() {
    const ADDR: &str = "1.2.3.4:567";

    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_secs(10),
        Duration::from_secs(100),
        Duration::from_secs(1),
        &Registry::new(),
    )
    .unwrap();
    let ttl = m.calculate_ttl(ADDR);
    assert!(ttl >= Duration::from_secs(10) && ttl <= Duration::from_secs(100));

    let m = HttpConnectionTtlMiddleware::new(
        Duration::ZERO,
        Duration::ZERO,
        Duration::from_secs(1),
        &Registry::new(),
    )
    .unwrap();
    assert_eq!(m.calculate_ttl(ADDR), Duration::ZERO);

    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_secs(10),
        Duration::from_secs(10),
        Duration::from_secs(1),
        &Registry::new(),
    )
    .unwrap();
    assert_eq!(m.calculate_ttl(ADDR), Duration::from_secs(10));
}

// Upstream: github.com/grafana/dskit/middleware/http_connection_ttl_middleware_test.go::TestHTTPConnectionTTLMiddleware_Wrap
#[test]
fn test_http_connection_ttl_middleware_wrap() {
    // 1. Disabled when maxTTL == 0.
    let reg = Registry::new();
    let m = HttpConnectionTtlMiddleware::new(
        Duration::ZERO,
        Duration::ZERO,
        Duration::from_secs(1),
        &reg,
    )
    .unwrap();
    for addr in [CONN1, CONN2] {
        let resp = m.handle_request(addr, StatusCode::OK.into_response());
        assert!(resp.headers().get(header::CONNECTION).is_none());
    }
    assert_eq!(counter_value(&reg, "open_connections_with_ttl_total", None), 0.0);

    // 2. Connection TTL not reached.
    let reg = Registry::new();
    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_millis(10),
        Duration::from_millis(30),
        Duration::from_secs(1),
        &reg,
    )
    .unwrap();
    for addr in [CONN1, CONN2, CONN1, CONN2] {
        let resp = m.handle_request(addr, StatusCode::OK.into_response());
        assert!(resp.headers().get(header::CONNECTION).is_none());
        assert!(m.is_tracked(addr));
    }
    assert_eq!(counter_value(&reg, "open_connections_with_ttl_total", None), 2.0);

    // 3. Connection TTL reached -> Connection: close.
    let reg = Registry::new();
    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_millis(100),
        Duration::from_millis(200),
        Duration::from_secs(1),
        &reg,
    )
    .unwrap();
    for (i, addr) in [CONN1, CONN2, CONN1, CONN1, CONN2].into_iter().enumerate() {
        if i == 3 {
            m.advance_clock_for_test(Duration::from_millis(250));
        }
        let resp = m.handle_request(addr, StatusCode::OK.into_response());
        let expected_close = i >= 3;
        assert_eq!(
            resp.headers()
                .get(header::CONNECTION)
                .and_then(|v| v.to_str().ok()),
            expected_close.then_some("close")
        );
        assert_eq!(m.is_tracked(addr), !expected_close);
    }
    assert_eq!(counter_value(&reg, "open_connections_with_ttl_total", None), 2.0);
    assert_eq!(
        counter_value(&reg, "closed_connections_with_ttl_total", Some("limit")),
        2.0
    );
}

// Upstream: github.com/grafana/dskit/middleware/http_connection_ttl_middleware_test.go::TestHTTPConnectionTTLMiddleware_RemoveIdleExpiredConnections
#[test]
fn test_http_connection_ttl_middleware_remove_idle_expired_connections() {
    let reg = Registry::new();
    let freq = Duration::from_millis(10);
    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_millis(80),
        Duration::from_millis(120),
        freq,
        &reg,
    )
    .unwrap();

    let resp = m.handle_request(CONN1, StatusCode::OK.into_response());
    assert!(resp.headers().get(header::CONNECTION).is_none());
    assert!(m.is_tracked(CONN1));

    for _ in 0..80 {
        if !m.is_tracked(CONN1) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!m.is_tracked(CONN1));
    assert_eq!(counter_value(&reg, "open_connections_with_ttl_total", None), 1.0);
    assert_eq!(
        counter_value(&reg, "closed_connections_with_ttl_total", Some("idle timeout")),
        1.0
    );
}

// Upstream: github.com/grafana/dskit/middleware/http_connection_ttl_middleware_test.go::TestHTTPConnectionTTLMiddleware_RemoveIdleExpiredConnectionsKeepsActiveConnections
#[test]
fn test_http_connection_ttl_middleware_remove_idle_expired_connections_keeps_active_connections() {
    let reg = Registry::new();
    let min_ttl = Duration::from_millis(120);
    let max_ttl = Duration::from_millis(160);
    let m = Arc::new(
        HttpConnectionTtlMiddleware::new(min_ttl, max_ttl, Duration::from_millis(15), &reg)
            .unwrap(),
    );

    // Send requests frequently until TTL expires; idle cleanup must not drop active conn.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let m_c = Arc::clone(&m);
    let h = std::thread::spawn(move || {
        while !stop_c.load(Ordering::Relaxed) {
            let resp = m_c.handle_request(CONN1, StatusCode::OK.into_response());
            if resp
                .headers()
                .get(header::CONNECTION)
                .and_then(|v| v.to_str().ok())
                == Some("close")
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    h.join().unwrap();
    stop.store(true, Ordering::Relaxed);

    assert_eq!(counter_value(&reg, "open_connections_with_ttl_total", None), 1.0);
    assert_eq!(
        counter_value(&reg, "closed_connections_with_ttl_total", Some("limit")),
        1.0
    );
    assert_eq!(
        counter_value(&reg, "closed_connections_with_ttl_total", Some("idle timeout")),
        0.0
    );
}

// Upstream: github.com/grafana/dskit/middleware/http_connection_ttl_middleware_test.go::TestHTTPConnectionTTLMiddleware_ConcurrentRemoveExpiredConnection
#[test]
fn test_http_connection_ttl_middleware_concurrent_remove_expired_connection() {
    let reg = Registry::new();
    let m = Arc::new(
        HttpConnectionTtlMiddleware::new(
            Duration::from_millis(50),
            Duration::from_millis(50),
            Duration::from_secs(1),
            &reg,
        )
        .unwrap(),
    );

    assert!(!m.remove_expired_connection(CONN1));
    m.advance_clock_for_test(Duration::from_millis(60));

    let removed_count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..10 {
        let m_c = Arc::clone(&m);
        let rc = Arc::clone(&removed_count);
        handles.push(std::thread::spawn(move || {
            if m_c.remove_expired_connection(CONN1) {
                rc.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(removed_count.load(Ordering::Relaxed), 1);
    assert_eq!(
        counter_value(&reg, "closed_connections_with_ttl_total", Some("limit")),
        1.0
    );
}

// Upstream: github.com/grafana/dskit/middleware/http_connection_ttl_middleware_test.go::TestHTTPConnectionTTLMiddleware_IdleCleanupThenRequest
#[test]
fn test_http_connection_ttl_middleware_idle_cleanup_then_request() {
    // 1. Marked idleExpired on first cleanup pass, next request still enforces TTL -> Connection: close.
    let reg = Registry::new();
    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_millis(50),
        Duration::from_millis(50),
        Duration::from_secs(10),
        &reg,
    )
    .unwrap();
    let resp = m.handle_request(CONN1, StatusCode::OK.into_response());
    assert!(resp.headers().get(header::CONNECTION).is_none());

    m.advance_clock_for_test(Duration::from_millis(60));
    m.remove_idle_expired_connections();
    assert_eq!(m.is_idle_expired_for_test(CONN1), Some(true));

    let resp = m.handle_request(CONN1, StatusCode::OK.into_response());
    assert_eq!(
        resp.headers()
            .get(header::CONNECTION)
            .and_then(|v| v.to_str().ok()),
        Some("close")
    );
    assert_eq!(counter_value(&reg, "open_connections_with_ttl_total", None), 1.0);
    assert_eq!(
        counter_value(&reg, "closed_connections_with_ttl_total", Some("idle timeout")),
        0.0
    );
    assert_eq!(
        counter_value(&reg, "closed_connections_with_ttl_total", Some("limit")),
        1.0
    );

    // 2. Request arrives before TTL expires -> idleExpired reset to false.
    let reg = Registry::new();
    let m = HttpConnectionTtlMiddleware::new(
        Duration::from_millis(500),
        Duration::from_millis(500),
        Duration::from_secs(10),
        &reg,
    )
    .unwrap();
    let _ = m.handle_request(CONN1, StatusCode::OK.into_response());
    m.set_idle_expired_for_test(CONN1, true);
    let resp = m.handle_request(CONN1, StatusCode::OK.into_response());
    assert!(resp.headers().get(header::CONNECTION).is_none());
    assert_eq!(m.is_idle_expired_for_test(CONN1), Some(false));
}

// ============================================================================
// instrument_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/instrument_test.go::TestThroughputMetricHistogram
#[test]
fn test_throughput_metric_histogram() {
    let latency_cutoff = Duration::from_millis(100);
    let cases = [
        (true, "unit;val=0, other_unit;val=2", true),
        (false, "unit;val=0, other_unit;val=2", false),
        (true, "", false),
        (false, "", false),
    ];
    for (sleep, timing_hdr, expected_observed) in cases {
        let reg = Registry::new();
        let hist = HistogramVec::new(
            HistogramOpts::new("request_throughput_unit", "Server throughput running requests.")
                .buckets(vec![1.0, 5.0, 10.0])
                .const_label("cutoff_ms", "100"),
            &["method", "route"],
        )
        .unwrap();
        reg.register(Box::new(hist.clone())).unwrap();

        let mut resp = StatusCode::OK.into_response();
        if !timing_hdr.is_empty() {
            resp.headers_mut()
                .insert("Server-Timing", HeaderValue::from_str(timing_hdr).unwrap());
        }
        let elapsed = if sleep {
            Duration::from_millis(105)
        } else {
            Duration::from_millis(1)
        };
        observe_throughput_from_response(&resp, elapsed, latency_cutoff, "unit", &hist, "GET", "other");
        let count = histogram_count(&reg, "request_throughput_unit");
        assert_eq!(count, u64::from(expected_observed));
    }
}

// Upstream: github.com/grafana/dskit/middleware/instrument_test.go::TestExtractValueFromMultiValueHeader
#[test]
fn test_extract_value_from_multi_value_header() {
    let cases = [
        ("name0;key0=0.0;key1=1.1, name1;key0=1.1", "name0", "key0", Ok(0.0)),
        ("name0;key0=0.0;key1=1.1, name1;key1=1.1", "name0", "key1", Ok(1.1)),
        ("name0;key0=0.0;key1=1.1, name1;key0=1.1", "name2", "key0", Err(())),
        ("name0;key0=0.0;key1=1.1, name1;key1=1.1", "name2", "key1", Err(())),
        ("name0;key0=0.0;key1=1.1", "name0", "key2", Err(())),
        ("name0;key0=str;key1=1.1", "name0", "key0", Err(())),
        ("", "name0", "key0", Err(())),
        ("key0=0.0, key1=1.1", "key0", "key0", Err(())),
    ];
    for (hdr, name, key, expected) in cases {
        let res = extract_value_from_multi_value_header(hdr, name, key);
        match expected {
            Ok(v) => assert_eq!(res.unwrap(), v, "hdr={hdr} name={name} key={key}"),
            Err(()) => assert!(res.is_err(), "hdr={hdr} name={name} key={key}"),
        }
    }
}

// ============================================================================
// logging_test.go (5)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/logging_test.go::TestBadWriteLogging
#[test]
fn test_bad_write_logging() {
    for (write_err, expected) in [
        (Some("context canceled"), vec!["debug", "request cancelled: context canceled"]),
        (Some("yolo"), vec!["warn", "error: yolo"]),
        (None, vec!["debug", "GET http://example.com/foo (200)"]),
    ] {
        let logger = Arc::new(CaptureLogger::new(LogLevel::Debug));
        let mw = LogMiddleware::new(Arc::clone(&logger) as Arc<dyn Logger>, false, false, None, &[]);
        mw.log_outcome(
            "GET",
            "http://example.com/foo",
            200,
            Duration::from_millis(1),
            false,
            b"<html><body>Hello World!</body></html>",
            &HeaderMap::new(),
            "",
            write_err,
        );
        let text = logger.text();
        for sub in expected {
            assert!(text.contains(sub), "missing {sub:?} in {text}");
        }
    }
}

// Upstream: github.com/grafana/dskit/middleware/logging_test.go::TestDisabledSuccessfulRequestsLogging
#[test]
fn test_disabled_successful_requests_logging() {
    for disable_log in [false, true] {
        let logger = Arc::new(CaptureLogger::new(LogLevel::Debug));
        let mut mw = LogMiddleware::new(Arc::clone(&logger) as Arc<dyn Logger>, false, false, None, &[]);
        mw.disable_request_success_log = disable_log;
        mw.log_outcome(
            "GET",
            "http://example.com/foo",
            200,
            Duration::from_millis(1),
            false,
            b"<html><body>Hello World!</body></html>",
            &HeaderMap::new(),
            "",
            None,
        );
        let text = logger.text();
        if !disable_log {
            assert!(text.contains("GET http://example.com/foo (200)"), "{text}");
        } else {
            assert!(text.is_empty(), "expected empty log, got {text}");
        }
    }
}

// Upstream: github.com/grafana/dskit/middleware/logging_test.go::TestLoggingRequestsAtInfoLevel
#[test]
fn test_logging_requests_at_info_level() {
    for (write_err, expected) in [
        (Some("context canceled"), vec!["info", "request cancelled: context canceled"]),
        (None, vec!["info", "GET http://example.com/foo (200)"]),
    ] {
        let logger = Arc::new(CaptureLogger::new(LogLevel::Info));
        let mw = LogMiddleware::new(Arc::clone(&logger) as Arc<dyn Logger>, false, true, None, &[]);
        mw.log_outcome(
            "GET",
            "http://example.com/foo",
            200,
            Duration::from_millis(1),
            false,
            b"<html><body>Hello World!</body></html>",
            &HeaderMap::new(),
            "",
            write_err,
        );
        let text = logger.text();
        for sub in expected {
            assert!(text.contains(sub), "missing {sub:?} in {text}");
        }
    }
}

// Upstream: github.com/grafana/dskit/middleware/logging_test.go::TestLoggingRequestWithExcludedHeaders
#[test]
fn test_logging_request_with_excluded_headers() {
    let default_headers = [
        "Authorization",
        "Cookie",
        "X-Csrf-Token",
        "X-Grafana-Id",
        "X-Access-Token",
    ];
    let cases = [
        (default_headers.to_vec(), vec![], default_headers.to_vec(), 200, true),
        (
            [default_headers.as_slice(), &["X-Secret-Header"]].concat(),
            vec!["X-Secret-Header"],
            [default_headers.as_slice(), &["X-Secret-Header"]].concat(),
            200,
            true,
        ),
        (
            [default_headers.as_slice(), &["X-Secret-Header", "X-Secret-Header-2"]].concat(),
            vec!["X-Secret-Header", "X-Secret-Header-2"],
            [default_headers.as_slice(), &["X-Secret-Header", "X-Secret-Header-2"]].concat(),
            200,
            true,
        ),
        (
            [default_headers.as_slice(), &["X-Secret-Header"]].concat(),
            vec![],
            [default_headers.as_slice(), &["X-Secret-Header"]].concat(),
            500,
            false,
        ),
    ];
    for (set_hdrs, exclude_hdrs, must_not_contain, status_code, log_headers) in cases {
        let logger = Arc::new(CaptureLogger::new(LogLevel::Debug));
        let mw = LogMiddleware::new(
            Arc::clone(&logger) as Arc<dyn Logger>,
            log_headers,
            false,
            None,
            &exclude_hdrs,
        );
        let mut req_headers = HeaderMap::new();
        for h in set_hdrs {
            req_headers.insert(
                axum::http::HeaderName::from_bytes(h.as_bytes()).unwrap(),
                HeaderValue::from_str(h).unwrap(),
            );
        }
        mw.log_outcome(
            "GET",
            "http://example.com/foo",
            status_code,
            Duration::from_millis(1),
            false,
            b"<html><body>Hello world!</body></html>",
            &req_headers,
            "",
            None,
        );
        let text = logger.text();
        for h in must_not_contain {
            assert!(!text.contains(h), "header {h} leaked into log: {text}");
        }
    }
}

// Upstream: github.com/grafana/dskit/middleware/logging_test.go::TestResponseBodyEscapingInLogs
#[test]
fn test_response_body_escaping_in_logs() {
    let body = br#"{"status":"error","errorType":"timeout","error":"rpc error: code = DeadlineExceeded desc = context deadline exceeded"}
"#;
    let logger = Arc::new(CaptureLogger::new(LogLevel::Debug));
    let mw = LogMiddleware::new(Arc::clone(&logger) as Arc<dyn Logger>, false, false, None, &[]);
    mw.log_outcome(
        "POST",
        "/loki/api/v1/push",
        504,
        Duration::from_millis(1),
        false,
        body,
        &HeaderMap::new(),
        "",
        None,
    );
    let text = logger.text();
    assert!(text.contains("level=warn"), "{text}");
    assert!(text.contains("POST /loki/api/v1/push (504)"), "{text}");
    assert!(
        text.contains(r#"Response: {\"status\":\"error\",\"errorType\":\"timeout\",\"error\":\"rpc error: code = DeadlineExceeded desc = context deadline exceeded\"}\n"#),
        "{text}"
    );
}

// ============================================================================
// response_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/response_test.go::TestBadResponseLoggingWriter
#[test]
fn test_bad_response_logging_writer() {
    let cases = [
        (200, "", ""),
        (200, "some data", ""),
        (422, "unprocessable", ""),
        (500, "", ""),
        (500, "bad juju\n", "bad juju\n"),
    ];
    for (status, data, expected) in cases {
        let mut w = BadResponseLoggingWriter::new((), false);
        w.write_header(status);
        if !data.is_empty() {
            w.write(data.as_bytes(), None);
        }
        assert_eq!(w.status_code(), status);
        assert_eq!(std::str::from_utf8(w.captured_body()).unwrap(), expected);
    }
}

// Upstream: github.com/grafana/dskit/middleware/response_test.go::TestBadResponseLoggingWriter_WithAndWithoutFlusher
#[test]
fn test_bad_response_logging_writer_with_and_without_flusher() {
    let mut nf = BadResponseLoggingWriter::new((), false);
    assert!(!nf.flush());

    let mut f = BadResponseLoggingWriter::new((), true);
    assert!(f.flush());
    assert!(f.was_flushed());
}

// ============================================================================
// route_injector_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/middleware/route_injector_test.go::TestRouteInjector
#[test]
fn test_route_injector() {
    assert_eq!(route_injector_name(Some("/"), None), "root");
    assert_eq!(
        route_injector_name(Some("/foo/bar/blah"), None),
        "foo_bar_blah"
    );
    assert_eq!(
        route_injector_name(Some("/templated/:name/thing"), None),
        "templated_name_thing"
    );
    assert_eq!(
        route_injector_name(Some("/named"), Some("my-named-route")),
        "my-named-route"
    );
    assert_eq!(route_injector_name(None, None), "notfound");
}

// Upstream: github.com/grafana/dskit/middleware/route_injector_test.go::TestMakeLabelValue
#[test]
fn test_make_label_value() {
    for (input, want) in [
        ("/", "root"),
        ("//", "root"),
        ("a", "a"),
        ("/foo", "foo"),
        ("foo/", "foo"),
        ("/foo/", "foo"),
        ("/foo/bar", "foo_bar"),
        ("foo/bar/", "foo_bar"),
        ("/foo/bar/", "foo_bar"),
        ("/foo/{orgName}/Bar", "foo_orgname_bar"),
        ("/foo/{org_name}/Bar", "foo_org_name_bar"),
        ("/foo/{org__name}/Bar", "foo_org_name_bar"),
        ("/foo/{org___name}/_Bar", "foo_org_name_bar"),
        ("/foo.bar/baz.qux/", "foo_bar_baz_qux"),
    ] {
        assert_eq!(make_label_value(input), want, "input={input}");
    }
}

// ============================================================================
// source_ips_test.go (5)
// ============================================================================

fn hdrs(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(
            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    h
}

// Upstream: github.com/grafana/dskit/middleware/source_ips_test.go::TestGetSourceIPs
#[test]
fn test_get_source_ips() {
    let s = SourceIps::new("", "", false).unwrap();
    for (pairs, remote, want) in [
        (&[][..], "192.168.1.100:3454", "192.168.1.100"),
        (&[][..], "192.168.1.100", "192.168.1.100"),
        (&[][..], "192.168.100", "192.168.100"),
        (&[("X-Forwarded-For", "172.16.1.1")][..], "192.168.1.100:3454", "172.16.1.1, 192.168.1.100"),
        (&[("X-Forwarded-For", "192.168.1.100")][..], "192.168.1.100:3454", "192.168.1.100"),
        (&[("X-Forwarded-For", "2001:db8::1")][..], "[2001:db9::1]:3454", "2001:db8::1, 2001:db9::1"),
        (&[("X-Forwarded-For", "172.16.1.1")][..], "", "172.16.1.1"),
        (&[("X-Forwarded-For", "172.16.1.1, 10.10.13.20")][..], "192.168.1.100:3454", "172.16.1.1, 192.168.1.100"),
        (&[("X-Forwarded-For", "172.16.1.1,10.10.13.20,10.11.16.46")][..], "192.168.1.100:3454", "172.16.1.1, 192.168.1.100"),
        (&[("X-Forwarded-For", "[2001:db8:cafe::17]:4711, 10.10.13.20")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 192.168.1.100"),
        (&[][..], "", ""),
        (&[("X-Real-IP", "[2001:db8:cafe::17]:4711")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 192.168.1.100"),
        (&[("X-Real-IP", "192.169.1.200")][..], "192.168.1.100:3454", "192.169.1.200, 192.168.1.100"),
        (&[("X-Forwarded-For", "[2001:db8:cafe::17]:4711, 10.10.13.20"), ("X-Real-IP", "192.169.1.200")][..], "192.168.1.100:3454", "192.169.1.200, 192.168.1.100"),
        (&[("Forwarded", "for=192.169.1.200")][..], "192.168.1.100:3454", "192.169.1.200, 192.168.1.100"),
        (&[("Forwarded", "for=192.0.2.60;proto=http;by=203.0.113.43")][..], "192.168.1.100:3454", "192.0.2.60, 192.168.1.100"),
        (&[("Forwarded", "for=[2001:db8:cafe::17]:4711,for=192.169.1.200")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 192.168.1.100"),
        (&[("X-Forwarded-For", "[2001:db8:cafe::17]:4711, 10.10.13.20"), ("X-Real-IP", "192.169.1.200"), ("Forwarded", "for=[2001:db8:cafe::17]:4711,for=192.169.1.200")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 192.168.1.100"),
        (&[("Forwarded", "for=workstation.local")][..], "192.168.1.100:3454", "workstation.local, 192.168.1.100"),
    ] {
        assert_eq!(s.get(&hdrs(pairs), remote), want);
    }
}

// Upstream: github.com/grafana/dskit/middleware/source_ips_test.go::TestGetSourceIPsFullExtract
#[test]
fn test_get_source_ips_full_extract() {
    let s = SourceIps::new("", "", true).unwrap();
    for (pairs, remote, want) in [
        (&[][..], "192.168.1.100:3454", "192.168.1.100"),
        (&[][..], "192.168.1.100", "192.168.1.100"),
        (&[][..], "192.168.100", "192.168.100"),
        (&[("X-Forwarded-For", "172.16.1.1")][..], "192.168.1.100:3454", "172.16.1.1, 192.168.1.100"),
        (&[("X-Forwarded-For", "192.168.1.100")][..], "192.168.1.100:3454", "192.168.1.100"),
        (&[("X-Forwarded-For", "2001:db8::1")][..], "[2001:db9::1]:3454", "2001:db8::1, 2001:db9::1"),
        (&[("X-Forwarded-For", "172.16.1.1")][..], "", "172.16.1.1"),
        (&[("X-Forwarded-For", "172.16.1.1, 10.10.13.20")][..], "192.168.1.100:3454", "172.16.1.1, 10.10.13.20, 192.168.1.100"),
        (&[("X-Forwarded-For", "172.16.1.1,10.10.13.20,10.11.16.46")][..], "192.168.1.100:3454", "172.16.1.1, 10.10.13.20, 10.11.16.46, 192.168.1.100"),
        (&[("X-Forwarded-For", "[2001:db8:cafe::17]:4711, 10.10.13.20")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 10.10.13.20, 192.168.1.100"),
        (&[][..], "", ""),
        (&[("X-Real-IP", "[2001:db8:cafe::17]:4711")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 192.168.1.100"),
        (&[("X-Real-IP", "192.169.1.200")][..], "192.168.1.100:3454", "192.169.1.200, 192.168.1.100"),
        (&[("X-Forwarded-For", "[2001:db8:cafe::17]:4711, 10.10.13.20"), ("X-Real-IP", "192.169.1.200")][..], "192.168.1.100:3454", "192.169.1.200, 192.168.1.100"),
        (&[("Forwarded", "for=192.169.1.200")][..], "192.168.1.100:3454", "192.169.1.200, 192.168.1.100"),
        (&[("Forwarded", "for=192.0.2.60;proto=http;by=203.0.113.43")][..], "192.168.1.100:3454", "192.0.2.60, 192.168.1.100"),
        (&[("Forwarded", "for=[2001:db8:cafe::17]:4711,for=192.169.1.200")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 192.169.1.200, 192.168.1.100"),
        (&[("X-Forwarded-For", "[2001:db8:cafe::17]:4711, 10.10.13.20"), ("X-Real-IP", "192.169.1.200"), ("Forwarded", "for=[2001:db8:cafe::17]:4711,for=192.169.1.200")][..], "192.168.1.100:3454", "2001:db8:cafe::17, 192.169.1.200, 192.168.1.100"),
        (&[("Forwarded", "for=workstation.local")][..], "192.168.1.100:3454", "workstation.local, 192.168.1.100"),
        (&[("Forwarded", "For=192.168.1.1")][..], "192.168.1.100:3454", "192.168.1.1, 192.168.1.100"),
    ] {
        assert_eq!(s.get(&hdrs(pairs), remote), want);
    }
}

// Upstream: github.com/grafana/dskit/middleware/source_ips_test.go::TestGetSourceIPsWithCustomRegex
#[test]
fn test_get_source_ips_with_custom_regex() {
    let s = SourceIps::new("SomeHeader", r"((?:[0-9]{1,3}.){3}[0-9]{1,3})", false).unwrap();
    assert_eq!(s.get(&hdrs(&[]), "192.168.1.100:3454"), "192.168.1.100");
    assert_eq!(
        s.get(&hdrs(&[("SomeHeader", "not matching")]), "192.168.1.100:3454"),
        "192.168.1.100"
    );
    assert_eq!(
        s.get(&hdrs(&[("SomeHeader", "172.16.1.1")]), "192.168.1.100:3454"),
        "172.16.1.1, 192.168.1.100"
    );
    assert_eq!(
        s.get(&hdrs(&[("SomeHeader", "172.16.1.1,172.16.2.1")]), "192.168.1.100:3454"),
        "172.16.1.1, 192.168.1.100"
    );
}

// Upstream: github.com/grafana/dskit/middleware/source_ips_test.go::TestGetSourceIPsWithCustomRegexFullExtract
#[test]
fn test_get_source_ips_with_custom_regex_full_extract() {
    let s = SourceIps::new("SomeHeader", r"((?:[0-9]{1,3}.){3}[0-9]{1,3})", true).unwrap();
    assert_eq!(s.get(&hdrs(&[]), "192.168.1.100:3454"), "192.168.1.100");
    assert_eq!(
        s.get(&hdrs(&[("SomeHeader", "not matching")]), "192.168.1.100:3454"),
        "192.168.1.100"
    );
    assert_eq!(
        s.get(&hdrs(&[("SomeHeader", "172.16.1.1")]), "192.168.1.100:3454"),
        "172.16.1.1, 192.168.1.100"
    );
    assert_eq!(
        s.get(&hdrs(&[("SomeHeader", "172.16.1.1,172.16.2.1")]), "192.168.1.100:3454"),
        "172.16.1.1, 172.16.2.1, 192.168.1.100"
    );
}

// Upstream: github.com/grafana/dskit/middleware/source_ips_test.go::TestInvalid
#[test]
fn test_invalid() {
    assert!(SourceIps::new("Header", "", false).is_err());
    assert!(SourceIps::new("", "a(.*)b", false).is_err());
    assert!(SourceIps::new("Header", "[*", false).is_err());
}
