//! Additional dskit middleware components (cluster interceptors, connection TTL, timeout,
//! throughput instrumentation, bad-response writer, HTTP logging).
//!
//! Ported from:
//! - github.com/grafana/dskit/middleware/grpc_cluster.go
//! - github.com/grafana/dskit/middleware/http_cluster.go
//! - github.com/grafana/dskit/middleware/http_connection_ttl_middleware.go
//! - github.com/grafana/dskit/middleware/http_timeout.go
//! - github.com/grafana/dskit/middleware/instrument.go
//! - github.com/grafana/dskit/middleware/logging.go
//! - github.com/grafana/dskit/middleware/response.go
//! - github.com/grafana/dskit/middleware/route_injector.go

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use axum::extract::Request;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use mimir_common::logging::{LogLevel, LogValue, Logger};
use mimir_config::GoDuration;
use mimir_proto::grpcutilpb::{
    extract_grpcutil_error_details, status_with_grpcutil_error_details, ErrorCause, ErrorDetails,
};
use parking_lot::Mutex;
use prometheus::{HistogramVec, IntCounter, IntCounterVec, Opts, Registry};
use serde::{Deserialize, Serialize};
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

use super::http_middleware::SourceIps;
use super::http_server::SharedWriteDeadline;
use super::metrics::make_label_value;
use crate::grpc::http_canonical_header_key;

// ============================================================================
// dskit/middleware/grpc_cluster.go & http_cluster.go
// ============================================================================

pub type InvalidClusterValidationReporter = Arc<dyn Fn(&str, &str) + Send + Sync>;

pub fn noop_cluster_validation_reporter() -> InvalidClusterValidationReporter {
    Arc::new(|_, _| {})
}

/// Ported from `dskit/middleware/grpc_cluster.go` (`ClusterUnaryClientInterceptor`).
#[derive(Clone)]
pub struct ClusterUnaryClientInterceptor {
    cluster: String,
    reporter: InvalidClusterValidationReporter,
}

impl ClusterUnaryClientInterceptor {
    pub fn new(cluster: impl Into<String>, reporter: InvalidClusterValidationReporter) -> Self {
        let cluster = cluster.into();
        assert!(!cluster.is_empty(), "no cluster label provided");
        Self { cluster, reporter }
    }

    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    #[allow(clippy::result_large_err)]
    pub fn intercept<F>(
        &self,
        md: &mut MetadataMap,
        method: &str,
        invoker: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(&MetadataMap) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        if let Ok(val) = tonic::metadata::MetadataValue::try_from(self.cluster.as_str()) {
            md.insert("x-cluster", val);
        }
        match invoker(md) {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Some(st) = err.downcast_ref::<Status>() {
                    if let Some(details) = extract_grpcutil_error_details(st) {
                        if details.cause == ErrorCause::WrongClusterValidationLabel as i32 {
                            let msg = format!("request rejected by the server: {}", st.message());
                            (self.reporter)(&msg, method);
                            return Err(Box::new(Status::internal(msg)));
                        }
                    }
                }
                Err(err)
            }
        }
    }
}

/// Ported from `dskit/middleware/grpc_cluster.go` (`NewInvalidClusterRequests`).
pub fn new_invalid_cluster_requests_counter(
    reg: &Registry,
    namespace: &str,
) -> prometheus::Result<IntCounterVec> {
    let c = IntCounterVec::new(
        Opts::new(
            "server_invalid_cluster_validation_label_requests_total",
            "Number of requests received by server with invalid cluster validation label.",
        )
        .namespace(namespace),
        &[
            "protocol",
            "method",
            "cluster_validation_label",
            "request_cluster_validation_label",
        ],
    )?;
    reg.register(Box::new(c.clone()))?;
    Ok(c)
}

/// Ported from `dskit/middleware/grpc_cluster.go` (`ClusterUnaryServerInterceptor`).
#[derive(Clone)]
pub struct ClusterUnaryServerInterceptor {
    clusters: Vec<String>,
    soft_validation: bool,
    invalid_cluster_requests: IntCounterVec,
    logger: Arc<dyn Logger>,
}

impl ClusterUnaryServerInterceptor {
    pub fn new(
        clusters: Vec<String>,
        soft_validation: bool,
        invalid_cluster_requests: IntCounterVec,
        logger: Arc<dyn Logger>,
    ) -> Self {
        assert!(!clusters.is_empty(), "no cluster labels provided");
        Self {
            clusters,
            soft_validation,
            invalid_cluster_requests,
            logger,
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn check(
        &self,
        md: &MetadataMap,
        method: &str,
        is_health_server: bool,
        tenant: Option<&str>,
        client_addr: Option<SocketAddr>,
    ) -> Result<(), Status> {
        if is_health_server {
            return Ok(());
        }
        let values: Vec<String> = md
            .get_all("x-cluster")
            .iter()
            .filter_map(|v| v.to_str().ok().map(str::to_string))
            .collect();
        let expected = format!("[{}]", self.clusters.join(" "));
        let first = self.clusters.first().map(String::as_str).unwrap_or("");
        let reject = |msg: String| {
            status_with_grpcutil_error_details(
                Code::FailedPrecondition,
                msg,
                &ErrorDetails {
                    cause: ErrorCause::WrongClusterValidationLabel as i32,
                },
            )
        };
        let client_str = client_addr.map(|a| a.to_string());
        let log_warn = |extra: &[(&str, &str)]| {
            let mut kvs: Vec<(&str, LogValue<'_>)> = vec![
                ("method", LogValue::from(method)),
                ("cluster_validation_labels", LogValue::from(expected.as_str())),
                ("soft_validation", LogValue::Bool(self.soft_validation)),
            ];
            if let Some(t) = tenant {
                kvs.push(("tenant", LogValue::from(t)));
            }
            if let Some(ref c) = client_str {
                kvs.push(("client_address", LogValue::from(c.as_str())));
            }
            for &(k, v) in extra {
                kvs.push((k, LogValue::from(v)));
            }
            self.logger.log_values(LogLevel::Warn, &kvs);
        };

        if values.len() > 1 {
            let err_msg = format!(
                "gRPC metadata should contain exactly 1 value for key \"x-cluster\", but it contains [{}]",
                values.join(" ")
            );
            self.invalid_cluster_requests
                .with_label_values(&["grpc", method, first, ""])
                .inc();
            log_warn(&[
                ("msg", "detected error during cluster validation label extraction"),
                ("err", &err_msg),
            ]);
            if self.soft_validation {
                return Ok(());
            }
            return Err(reject(format!("rejected request: {err_msg}")));
        }

        match values.first().filter(|v| !v.is_empty()) {
            Some(c) if self.clusters.iter().any(|a| a == c) => Ok(()),
            Some(c) => {
                self.invalid_cluster_requests
                    .with_label_values(&["grpc", method, first, c])
                    .inc();
                log_warn(&[
                    ("msg", "request with wrong cluster validation label"),
                    ("request_cluster_validation_label", c),
                ]);
                if self.soft_validation {
                    return Ok(());
                }
                Err(reject(format!(
                    "rejected request with wrong cluster validation label {c:?} - it should be one of {expected}"
                )))
            }
            None => {
                self.invalid_cluster_requests
                    .with_label_values(&["grpc", method, first, ""])
                    .inc();
                log_warn(&[("msg", "request with no cluster validation label")]);
                if self.soft_validation {
                    return Ok(());
                }
                Err(reject(format!(
                    "rejected request with empty cluster validation label - it should be one of {expected}"
                )))
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterValidationErrorBody {
    pub cluster_validation_error_message: String,
    pub route: String,
}

/// Ported from `dskit/middleware/http_cluster.go` (`ClusterValidationRoundTripper`).
#[derive(Clone)]
pub struct ClusterValidationRoundTripper {
    cluster: String,
    reporter: InvalidClusterValidationReporter,
    client: reqwest::Client,
}

impl ClusterValidationRoundTripper {
    pub fn new(
        cluster: impl Into<String>,
        reporter: InvalidClusterValidationReporter,
    ) -> Self {
        let cluster = cluster.into();
        assert!(!cluster.is_empty(), "no cluster label provided");
        Self {
            cluster,
            reporter,
            client: reqwest::Client::new(),
        }
    }

    pub async fn send(&self, mut req: reqwest::Request) -> Result<reqwest::Response, String> {
        req.headers_mut().insert(
            "X-Cluster",
            http::HeaderValue::from_str(&self.cluster).map_err(|e| e.to_string())?,
        );
        let resp = self.client.execute(req).await.map_err(|e| e.to_string())?;
        if resp.status() != StatusCode::NETWORK_AUTHENTICATION_REQUIRED {
            return Ok(resp);
        }
        let is_json = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == "application/json");
        if !is_json {
            return Ok(resp);
        }
        let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
        let parsed: ClusterValidationErrorBody = serde_json::from_slice(&bytes)
            .map_err(|e| format!("failed to decode cluster validation error: {e}"))?;
        let msg = format!(
            "request rejected by the server: {}",
            parsed.cluster_validation_error_message
        );
        (self.reporter)(&msg, &parsed.route);
        Err(msg)
    }
}

// ============================================================================
// dskit/middleware/http_connection_ttl_middleware.go
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionTtlError {
    IdleConnectionCheckFrequencyMustBePositive,
    MinLessOrEqualThanMax,
}

impl std::fmt::Display for ConnectionTtlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdleConnectionCheckFrequencyMustBePositive => {
                f.write_str("idle connection check frequency must be positive")
            }
            Self::MinLessOrEqualThanMax => f.write_str(
                "minimum TTL of TCP connections must be less or equal to the maximum TTL of TCP connections",
            ),
        }
    }
}

impl std::error::Error for ConnectionTtlError {}

struct ConnStateEntry {
    ttl: Duration,
    created: Instant,
    last_seen: Instant,
    idle_expired: bool,
}

impl ConnStateEntry {
    fn is_expired_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.created) > self.ttl
    }

    fn is_idle_expired_at(&self, now: Instant, timeout: Duration) -> bool {
        now.saturating_duration_since(self.last_seen) > timeout
    }
}

struct ConnTtlInner {
    min_ttl: Duration,
    max_ttl: Duration,
    connections: Mutex<HashMap<String, ConnStateEntry>>,
    total_open: IntCounter,
    total_closed: IntCounterVec,
    now_offset_ms: std::sync::atomic::AtomicU64,
    stop: AtomicBool,
    stop_mu: std::sync::Mutex<bool>,
    stop_cv: std::sync::Condvar,
}

impl ConnTtlInner {
    fn now(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.now_offset_ms.load(Ordering::Relaxed))
    }

    fn calculate_ttl(&self, conn: &str) -> Duration {
        // Go hash/fnv.New64 (FNV-1 64-bit: offset basis 14695981039346656037, prime 1099511628211).
        let mut h: u64 = 14695981039346656037;
        for &b in conn.as_bytes() {
            h = h.wrapping_mul(1099511628211);
            h ^= b as u64;
        }
        let min_ms = self.min_ttl.as_millis() as u64;
        let max_ms = self.max_ttl.as_millis() as u64;
        let ttl_ms = min_ms + (h % (max_ms + 1 - min_ms));
        Duration::from_millis(ttl_ms)
    }

    fn remove_idle_expired_connections(&self) {
        let now = self.now();
        let mut count = 0u64;
        {
            let mut conns = self.connections.lock();
            conns.retain(|_, state| {
                if state.is_idle_expired_at(now, self.max_ttl) {
                    if state.idle_expired {
                        count += 1;
                        return false;
                    }
                    state.idle_expired = true;
                }
                true
            });
        }
        if count > 0 {
            self.total_closed
                .with_label_values(&["idle timeout"])
                .inc_by(count);
        }
    }

    fn remove_expired_connection(&self, conn: &str) -> bool {
        let now = self.now();
        let mut conns = self.connections.lock();
        if let Some(state) = conns.get_mut(conn) {
            if !state.is_expired_at(now) {
                state.last_seen = now;
                state.idle_expired = false;
                return false;
            }
            conns.remove(conn);
            drop(conns);
            self.total_closed.with_label_values(&["limit"]).inc();
            return true;
        }
        let ttl = self.calculate_ttl(conn);
        conns.insert(
            conn.to_string(),
            ConnStateEntry {
                ttl,
                created: now,
                last_seen: now,
                idle_expired: false,
            },
        );
        drop(conns);
        self.total_open.inc();
        false
    }
}

/// Ported from `github.com/grafana/dskit/middleware/http_connection_ttl_middleware.go`.
pub struct HttpConnectionTtlMiddleware {
    inner: Arc<ConnTtlInner>,
    bg_thread: Option<std::thread::JoinHandle<()>>,
}

impl HttpConnectionTtlMiddleware {
    pub fn new(
        min_ttl: Duration,
        max_ttl: Duration,
        idle_check_frequency: Duration,
        reg: &Registry,
    ) -> Result<Self, ConnectionTtlError> {
        if min_ttl > max_ttl {
            return Err(ConnectionTtlError::MinLessOrEqualThanMax);
        }
        let total_open = IntCounter::new(
            "open_connections_with_ttl_total",
            "Number of connections that connection with TTL middleware started tracking",
        )
        .expect("valid counter");
        let total_closed = IntCounterVec::new(
            Opts::new(
                "closed_connections_with_ttl_total",
                "Number of connections that connection with TTL middleware closed or stopped tracking",
            ),
            &["reason"],
        )
        .expect("valid counter vec");
        let _ = reg.register(Box::new(total_open.clone()));
        let _ = reg.register(Box::new(total_closed.clone()));

        if !max_ttl.is_zero() && idle_check_frequency.is_zero() {
            return Err(ConnectionTtlError::IdleConnectionCheckFrequencyMustBePositive);
        }

        let inner = Arc::new(ConnTtlInner {
            min_ttl,
            max_ttl,
            connections: Mutex::new(HashMap::new()),
            total_open,
            total_closed,
            now_offset_ms: std::sync::atomic::AtomicU64::new(0),
            stop: AtomicBool::new(false),
            stop_mu: std::sync::Mutex::new(false),
            stop_cv: std::sync::Condvar::new(),
        });

        let bg_thread = if !max_ttl.is_zero() {
            let inner_bg = Arc::clone(&inner);
            Some(std::thread::spawn(move || {
                let mut guard = inner_bg.stop_mu.lock().unwrap_or_else(|e| e.into_inner());
                while !*guard && !inner_bg.stop.load(Ordering::Relaxed) {
                    let (next_guard, res) = inner_bg
                        .stop_cv
                        .wait_timeout(guard, idle_check_frequency)
                        .unwrap_or_else(|e| e.into_inner());
                    guard = next_guard;
                    if *guard || inner_bg.stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if res.timed_out() {
                        drop(guard);
                        inner_bg.remove_idle_expired_connections();
                        guard = inner_bg.stop_mu.lock().unwrap_or_else(|e| e.into_inner());
                    }
                }
            }))
        } else {
            None
        };

        Ok(Self { inner, bg_thread })
    }

    pub fn calculate_ttl(&self, conn: &str) -> Duration {
        self.inner.calculate_ttl(conn)
    }

    pub fn remove_expired_connection(&self, conn: &str) -> bool {
        self.inner.remove_expired_connection(conn)
    }

    pub fn remove_idle_expired_connections(&self) {
        self.inner.remove_idle_expired_connections();
    }

    pub fn advance_clock_for_test(&self, d: Duration) {
        self.inner
            .now_offset_ms
            .fetch_add(d.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn is_tracked(&self, conn: &str) -> bool {
        self.inner.connections.lock().contains_key(conn)
    }

    pub fn set_idle_expired_for_test(&self, conn: &str, val: bool) {
        if let Some(s) = self.inner.connections.lock().get_mut(conn) {
            s.idle_expired = val;
        }
    }

    pub fn is_idle_expired_for_test(&self, conn: &str) -> Option<bool> {
        self.inner
            .connections
            .lock()
            .get(conn)
            .map(|s| s.idle_expired)
    }

    pub fn handle_request(&self, remote_addr: &str, mut resp: Response) -> Response {
        if self.inner.max_ttl.is_zero() {
            return resp;
        }
        if self.inner.remove_expired_connection(remote_addr) {
            resp.headers_mut()
                .insert(header::CONNECTION, header::HeaderValue::from_static("close"));
        }
        resp
    }

    pub fn stop(&mut self) {
        self.inner.stop.store(true, Ordering::Release);
        {
            let mut g = self.inner.stop_mu.lock().unwrap_or_else(|e| e.into_inner());
            *g = true;
            self.inner.stop_cv.notify_all();
        }
        if let Some(h) = self.bg_thread.take() {
            let _ = h.join();
        }
    }
}

impl Drop for HttpConnectionTtlMiddleware {
    fn drop(&mut self) {
        self.stop();
    }
}

// ============================================================================
// dskit/middleware/http_timeout.go
// ============================================================================

/// Ported from `github.com/grafana/dskit/middleware/http_timeout.go`.
pub async fn timeout_middleware(
    timeout: Duration,
    msg: &'static str,
    req: Request,
    next: impl Future<Output = Response>,
) -> Response {
    if let Some(wd) = req.extensions().get::<SharedWriteDeadline>() {
        wd.disable();
    }
    match tokio::time::timeout(timeout, next).await {
        Ok(resp) => resp,
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, msg).into_response(),
    }
}

// ============================================================================
// dskit/middleware/instrument.go
// ============================================================================

/// Ported from `github.com/grafana/dskit/middleware/instrument.go` (`extractValueFromMultiValueHeader`).
pub fn extract_value_from_multi_value_header(
    h: &str,
    name: &str,
    key: &str,
) -> Result<f64, String> {
    for part in h.split(", ") {
        if let Some(rest) = part.strip_prefix(name) {
            for spart in rest.split(';') {
                if !spart.starts_with(key) {
                    continue;
                }
                let prefix = format!("{key}=");
                if let Some(val_str) = spart.strip_prefix(&prefix) {
                    // Parse leading float prefix like fmt.Sscanf(spart, key+"=%f", &value).
                    let end = val_str
                        .find(|c: char| !matches!(c, '0'..='9' | '.' | '-' | '+' | 'e' | 'E'))
                        .unwrap_or(val_str.len());
                    if let Ok(v) = val_str[..end].parse::<f64>() {
                        return Ok(v);
                    }
                    return Err(format!("failed to parse value from header: {spart}"));
                }
            }
        }
    }
    Err("desired name not found in header".to_string())
}

/// Ported from `github.com/grafana/dskit/middleware/instrument.go` (throughput metric observation).
pub fn observe_throughput_from_response(
    resp: &Response,
    elapsed: Duration,
    latency_cutoff: Duration,
    throughput_unit: &str,
    request_throughput: &HistogramVec,
    method: &str,
    route: &str,
) {
    if !latency_cutoff.is_zero() && elapsed > latency_cutoff {
        let server_timing = resp
            .headers()
            .get("Server-Timing")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if let Ok(volume) = extract_value_from_multi_value_header(server_timing, throughput_unit, "val") {
            let secs = elapsed.as_secs_f64();
            if secs > 0.0 {
                request_throughput
                    .with_label_values(&[method, route])
                    .observe(volume / secs);
            }
        }
    }
}

// ============================================================================
// dskit/middleware/response.go
// ============================================================================

const MAX_RESPONSE_BODY_IN_LOGS: usize = 4096;

/// Ported from `github.com/grafana/dskit/middleware/response.go` (`badResponseLoggingWriter`).
pub struct BadResponseLoggingWriter<W> {
    inner: W,
    buf: Vec<u8>,
    log_body: bool,
    body_bytes_left: usize,
    status_code: u16,
    write_error: Option<String>,
    supports_flush: bool,
    flushed: bool,
}

impl<W> BadResponseLoggingWriter<W> {
    pub fn new(inner: W, supports_flush: bool) -> Self {
        Self {
            inner,
            buf: Vec::new(),
            log_body: false,
            body_bytes_left: MAX_RESPONSE_BODY_IN_LOGS,
            status_code: 200,
            write_error: None,
            supports_flush,
            flushed: false,
        }
    }

    pub fn write_header(&mut self, status_code: u16) {
        self.status_code = status_code;
        if status_code >= 500 {
            self.log_body = true;
        }
    }

    pub fn write(&mut self, data: &[u8], inner_err: Option<&str>) {
        if self.status_code == 0 {
            self.write_header(200);
        }
        if self.log_body {
            if data.len() > self.body_bytes_left {
                self.buf.extend_from_slice(&data[..self.body_bytes_left]);
                self.buf.extend_from_slice(b"...");
                self.body_bytes_left = 0;
                self.log_body = false;
            } else {
                self.buf.extend_from_slice(data);
                self.body_bytes_left -= data.len();
            }
        }
        if let Some(e) = inner_err {
            self.write_error = Some(e.to_string());
        }
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn write_error(&self) -> Option<&str> {
        self.write_error.as_deref()
    }

    pub fn captured_body(&self) -> &[u8] {
        &self.buf
    }

    pub fn flush(&mut self) -> bool {
        if self.supports_flush {
            self.flushed = true;
            true
        } else {
            false
        }
    }

    pub fn was_flushed(&self) -> bool {
        self.flushed
    }

    pub fn unwrap_inner(&self) -> &W {
        &self.inner
    }
}

// ============================================================================
// dskit/middleware/route_injector.go
// ============================================================================

/// Ported from `github.com/grafana/dskit/middleware/route_injector.go`.
pub fn route_injector_name(matched_template: Option<&str>, named_route: Option<&str>) -> String {
    if let Some(name) = named_route {
        if !name.is_empty() {
            return name.to_string();
        }
    }
    match matched_template {
        Some(t) if !t.is_empty() => make_label_value(&t.replace(':', "")),
        _ => "notfound".to_string(),
    }
}

// ============================================================================
// dskit/middleware/logging.go
// ============================================================================

const ALWAYS_EXCLUDED_HEADERS: &[&str] = &[
    "Cookie",
    "X-Csrf-Token",
    "Authorization",
    "X-Grafana-Id",
    "X-Access-Token",
];

/// Ported from `github.com/grafana/dskit/middleware/logging.go` (`Log`).
#[derive(Clone)]
pub struct LogMiddleware {
    pub logger: Arc<dyn Logger>,
    pub disable_request_success_log: bool,
    pub log_request_headers: bool,
    pub log_request_at_info_level: bool,
    pub source_ips: Option<SourceIps>,
    pub exclude_headers: HashSet<String>,
}

impl LogMiddleware {
    pub fn new(
        logger: Arc<dyn Logger>,
        log_request_headers: bool,
        log_request_at_info_level: bool,
        source_ips: Option<SourceIps>,
        extra_exclude_headers: &[&str],
    ) -> Self {
        let mut exclude_headers: HashSet<String> = ALWAYS_EXCLUDED_HEADERS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        for h in extra_exclude_headers {
            exclude_headers.insert(http_canonical_header_key(h));
        }
        Self {
            logger,
            disable_request_success_log: false,
            log_request_headers,
            log_request_at_info_level,
            source_ips,
            exclude_headers,
        }
    }

    pub fn dump_headers(&self, headers: &HeaderMap) -> String {
        let mut keys: Vec<String> = headers
            .keys()
            .map(|k| http_canonical_header_key(k.as_str()))
            .collect();
        keys.sort();
        keys.dedup();
        let mut out = String::new();
        for k in keys {
            if ALWAYS_EXCLUDED_HEADERS.contains(&k.as_str()) || self.exclude_headers.contains(&k) {
                continue;
            }
            for v in headers.get_all(k.as_str()) {
                out.push_str(&format!("{k}: {}; ", String::from_utf8_lossy(v.as_bytes())));
            }
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub fn log_outcome(
        &self,
        method: &str,
        uri: &str,
        status_code: u16,
        elapsed: Duration,
        ws: bool,
        response_body: &[u8],
        req_headers: &HeaderMap,
        remote: &str,
        write_err: Option<&str>,
    ) {
        let dur = GoDuration(elapsed.as_nanos() as i64).to_string();
        let hdrs = self.log_request_headers.then(|| self.dump_headers(req_headers));
        let source_ips = self
            .source_ips
            .as_ref()
            .map(|s| s.get(req_headers, remote))
            .filter(|s| !s.is_empty());

        let (level, msg) = if let Some(err) = write_err {
            if err == "context canceled" {
                let lvl = if self.log_request_at_info_level {
                    LogLevel::Info
                } else {
                    LogLevel::Debug
                };
                let msg = match hdrs {
                    Some(ref h) => format!("{method} {uri} {dur}, request cancelled: {err} ws: {ws}; {h}"),
                    None => format!("{method} {uri} {dur}, request cancelled: {err} ws: {ws}"),
                };
                (lvl, msg)
            } else {
                let msg = match hdrs {
                    Some(ref h) => format!("{method} {uri} {dur}, error: {err} ws: {ws}; {h}"),
                    None => format!("{method} {uri} {dur}, error: {err} ws: {ws}"),
                };
                (LogLevel::Warn, msg)
            }
        } else if (200..300).contains(&status_code) && self.disable_request_success_log {
            return;
        } else if (100..500).contains(&status_code) || status_code == 502 || status_code == 503 {
            let lvl = if self.log_request_at_info_level {
                LogLevel::Info
            } else {
                LogLevel::Debug
            };
            let msg = match hdrs {
                Some(ref h) => format!("{method} {uri} ({status_code}) {dur} ws: {ws}; {h}"),
                None => format!("{method} {uri} ({status_code}) {dur}"),
            };
            (lvl, msg)
        } else {
            let b = String::from_utf8_lossy(response_body);
            let msg = match hdrs {
                Some(ref h) => format!("{method} {uri} ({status_code}) {dur} Response: {b} ws: {ws}; {h}"),
                None => format!("{method} {uri} ({status_code}) {dur} Response: {b}"),
            };
            (LogLevel::Warn, msg)
        };

        let mut kvs: Vec<(&str, &str)> = Vec::new();
        if let Some(ref ips) = source_ips {
            kvs.push(("sourceIPs", ips));
        }
        kvs.push(("msg", &msg));
        self.logger.log(level, &kvs);
    }
}
