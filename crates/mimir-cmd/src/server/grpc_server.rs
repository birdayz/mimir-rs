//! The gRPC server (dskit `Server.GRPC`, a grpc-go `grpc.Server`).
//!
//! Ported from:
//! - github.com/grafana/dskit/server/server.go (grpc options: `KeepaliveParams`
//!   (`MaxConnectionIdle`, `MaxConnectionAge`, `MaxConnectionAgeGrace`, `Time`, `Timeout`),
//!   `MaxConcurrentStreams` (0 = unlimited), `MaxRecvMsgSize` / `MaxSendMsgSize` (applied on the
//!   tonic services), TLS credentials, `GracefulStop` on shutdown)
//! - github.com/grafana/dskit/middleware/grpc_instrumentation.go
//!   (`UnaryServerInstrumentInterceptor` / `StreamServerInstrumentInterceptor`:
//!   `cortex_request_duration_seconds{method="gRPC",route=<full method>,status_code,ws="false"}`)
//! - github.com/grafana/dskit/middleware/grpc_stats.go (`cortex_request_message_bytes`,
//!   `cortex_response_message_bytes` per message wire length, `cortex_inflight_requests`,
//!   `grpc_concurrent_streams_by_conn_max`; only with `-server.grpc.stats-tracking-enabled`)
//! - github.com/grafana/dskit/middleware/grpc_logging.go (`GRPCServerLog` with
//!   `DisableRequestSuccessLog`: failed RPCs logged at warn, canceled at debug)
//! - github.com/grafana/dskit/middleware/grpc_cluster.go (`ClusterUnaryServerInterceptor`)
//! - google.golang.org/grpc/internal/transport/http2_server.go (`keepalive`: GOAWAY after
//!   `MaxConnectionIdle` without active streams; GOAWAY at `MaxConnectionAge` ±10% jitter and
//!   forced close after `MaxConnectionAgeGrace`)
//!
//! grpc-go's keepalive *enforcement policy* (`-server.grpc.keepalive.min-time-between-pings`,
//! `-server.grpc.keepalive.ping-without-stream-allowed`) is not available in the hyper/h2 stack;
//! non-default values are rejected at startup (see `super::validate_supported`).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body::Frame;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use mimir_common::grpc_deadline;
use mimir_common::grpc_log::{GrpcErrorLog, GrpcErrorLogEntry};
use mimir_common::logging::{LogLevel, Logger};
use mimir_config::GoDuration;
use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tower::ServiceExt;

use super::body::{GrpcFrameCounter, TrackedBody};
use super::listener::{finish_accept, Acceptor, PrefixedIo};
use super::metrics::{grpc_status_label, ServerMetrics};

#[derive(Clone)]
pub struct GrpcServerConfig {
    /// 0 means unlimited (grpc-go `MaxConcurrentStreams(0)` = `math.MaxUint32`).
    pub max_concurrent_streams: u32,
    pub keepalive_time: Option<Duration>,
    pub keepalive_timeout: Duration,
    pub max_connection_idle: Option<Duration>,
    pub max_connection_age: Option<Duration>,
    pub max_connection_age_grace: Option<Duration>,
    pub tls: Option<Arc<rustls::ServerConfig>>,
    pub stats_tracking: bool,
    pub report_grpc_status: bool,
    pub cluster_validation: Option<GrpcClusterValidation>,
    /// `-auth.multitenancy-enabled`: require `x-scope-orgid` on every RPC except Mimir's
    /// `noGRPCAuthOn` methods.
    pub multitenancy_enabled: bool,
}

#[derive(Clone)]
pub struct GrpcClusterValidation {
    pub allowed: Vec<String>,
    pub soft: bool,
}

/// Converts a Go duration flag to `None` for grpc-go's "infinity" (`math.MaxInt64`) or 0.
pub fn finite(d: GoDuration) -> Option<Duration> {
    if d.0 <= 0 || d.0 == i64::MAX {
        None
    } else {
        Some(Duration::from_nanos(d.0 as u64))
    }
}

/// Streaming RPCs: dskit's cluster validation interceptor is unary-only.
const STREAMING_METHODS: &[&str] = &[
    "/cortex.Ingester/QueryStream",
    "/cortex.Ingester/LabelNamesAndValues",
    "/cortex.Ingester/LabelValuesCardinality",
    "/cortex.Ingester/ActiveSeries",
    "/grpc.health.v1.Health/Watch",
];

struct Conn {
    id: u64,
    active: AtomicUsize,
    idle_since: Mutex<Instant>,
}

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct GrpcContext {
    pub metrics: ServerMetrics,
    pub logger: Arc<dyn Logger>,
}

/// Serves `routes` until `shutdown` flips to true, then stops gracefully: GOAWAY on every
/// connection and wait for in-flight RPCs without a deadline (grpc-go `GracefulStop`).
pub async fn serve(
    acceptor: Acceptor,
    cfg: GrpcServerConfig,
    routes: tonic::service::Routes,
    ctx: GrpcContext,
    mut shutdown: watch::Receiver<bool>,
) {
    ctx.metrics
        .grpc_concurrent_streams_limit
        .with_label_values(&[])
        .set(f64::from(cfg.max_concurrent_streams));
    let acceptor = Arc::new(acceptor);
    let routes = routes.prepare();
    let mut conns: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = super::wait_shutdown(&mut shutdown) => break,
            res = acceptor.accept() => {
                let Ok((stream, addr, guard)) = res else { continue };
                let acc = Arc::clone(&acceptor);
                let cfg = cfg.clone();
                let routes = routes.clone();
                let ctx = ctx.clone();
                let shutdown = shutdown.clone();
                conns.spawn(async move {
                    let Ok(accepted) = finish_accept(stream, addr, guard, acc.options()).await else { return };
                    let remote = accepted.remote_addr;
                    let _guard = accepted.guard;
                    let io = PrefixedIo::new(accepted.prefix, accepted.stream);
                    if let Some(tls) = cfg.tls.clone() {
                        // grpc-go's default ConnectionTimeout for the handshake is 120s.
                        let hs = tokio_rustls::TlsAcceptor::from(tls).accept(io);
                        if let Ok(Ok(s)) = tokio::time::timeout(Duration::from_secs(120), hs).await {
                            serve_conn(s, remote, cfg, routes, ctx, shutdown).await;
                        }
                    } else {
                        serve_conn(io, remote, cfg, routes, ctx, shutdown).await;
                    }
                });
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
    while conns.join_next().await.is_some() {}
}

fn jittered(d: Duration) -> Duration {
    // grpc-go getJitter: +/-10%.
    let r = d.as_nanos() as i64 / 10;
    if r <= 0 {
        return d;
    }
    let j = rand::thread_rng().gen_range(-r..r);
    Duration::from_nanos((d.as_nanos() as i64 + j).max(0) as u64)
}

async fn serve_conn<T>(
    io: T,
    remote: SocketAddr,
    cfg: GrpcServerConfig,
    routes: tonic::service::Routes,
    ctx: GrpcContext,
    mut shutdown: watch::Receiver<bool>,
) where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let conn_state = Arc::new(Conn {
        id: NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed),
        active: AtomicUsize::new(0),
        idle_since: Mutex::new(Instant::now()),
    });
    let svc = {
        let conn_state = Arc::clone(&conn_state);
        let cfg = cfg.clone();
        hyper::service::service_fn(move |req: hyper::Request<Incoming>| {
            handle(req, remote, Arc::clone(&conn_state), routes.clone(), cfg.clone(), ctx.clone())
        })
    };

    let mut b = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
    b.timer(TokioTimer::new())
        .max_concurrent_streams(if cfg.max_concurrent_streams == 0 {
            u32::MAX
        } else {
            cfg.max_concurrent_streams
        })
        // grpc-go defaultServerMaxHeaderListSize.
        .max_header_list_size(16 << 20)
        // grpc-go adjusts windows with BDP estimation.
        .adaptive_window(true)
        .keep_alive_interval(cfg.keepalive_time)
        .keep_alive_timeout(cfg.keepalive_timeout);
    let conn = b.serve_connection(TokioIo::new(io), svc);
    tokio::pin!(conn);

    let started = Instant::now();
    let age_deadline = cfg.max_connection_age.map(|a| started + jittered(a));
    let sleep_until_opt = |d: Option<Instant>| async move {
        match d {
            Some(d) => tokio::time::sleep_until(d.into()).await,
            None => std::future::pending::<()>().await,
        }
    };

    loop {
        let idle_deadline = cfg.max_connection_idle.map(|idle| {
            if conn_state.active.load(Ordering::Acquire) == 0 {
                *conn_state.idle_since.lock() + idle
            } else {
                Instant::now() + idle
            }
        });
        tokio::select! {
            _ = conn.as_mut() => return,
            _ = super::wait_shutdown(&mut shutdown) => {
                conn.as_mut().graceful_shutdown();
                let _ = conn.as_mut().await;
                return;
            }
            _ = sleep_until_opt(age_deadline) => {
                conn.as_mut().graceful_shutdown();
                match cfg.max_connection_age_grace {
                    Some(grace) => { let _ = tokio::time::timeout(grace, conn.as_mut()).await; }
                    None => { let _ = conn.as_mut().await; }
                }
                return;
            }
            _ = sleep_until_opt(idle_deadline) => {
                let idle = cfg.max_connection_idle.unwrap_or_default();
                if conn_state.active.load(Ordering::Acquire) == 0
                    && conn_state.idle_since.lock().elapsed() >= idle
                {
                    conn.as_mut().graceful_shutdown();
                    let _ = conn.as_mut().await;
                    return;
                }
            }
        }
    }
}

type GrpcResponse = hyper::Response<tonic::body::BoxBody>;

/// Methods exempt from the multitenant gRPC auth interceptor (Mimir's `noGRPCAuthOn` list in
/// `pkg/mimir/mimir.go`, passed to `noauth.SetupAuthMiddleware`).
const NO_GRPC_AUTH_ON: &[&str] = &[
    "/grpc.health.v1.Health/Check",
    "/frontend.Frontend/Process",
    "/frontend.Frontend/NotifyClientShutdown",
    "/ruler.Ruler/SyncRules",
    "/schedulerpb.SchedulerForFrontend/FrontendLoop",
    "/schedulerpb.SchedulerForQuerier/QuerierLoop",
    "/schedulerpb.SchedulerForQuerier/NotifyQuerierShutdown",
    "/schedulerpb.BlockBuilderScheduler/AssignJob",
    "/schedulerpb.BlockBuilderScheduler/UpdateJob",
    "/usagetrackerpb.UsageTracker/GetUsersCloseToLimit",
    "/usagetrackerpb.UsageTracker/TrackSeriesBatch",
];

/// dskit `middleware.ServerUserHeaderInterceptor` / `StreamServerUserHeaderInterceptor`
/// (`user.ExtractFromGRPCRequest`): exactly one `x-scope-orgid` metadata value.
fn check_org_id(headers: &http::HeaderMap, method: &str) -> Option<tonic::Status> {
    if NO_GRPC_AUTH_ON.contains(&method) || headers.get_all("x-scope-orgid").iter().count() == 1 {
        return None;
    }
    Some(tonic::Status::unknown("no org id"))
}

/// Per-RPC bookkeeping started when the request arrives and finalized exactly once: when the
/// response body completes, or on drop (handler future or response body dropped because the
/// client reset the stream), like the `defer`s of dskit's interceptors and stats handler.
struct RpcGuard {
    method: String,
    begin: Instant,
    conn: Arc<Conn>,
    stream_tracker: Option<Arc<super::metrics::StreamTracker>>,
    inflight: Option<prometheus::Gauge>,
    duration_vec: mimir_common::native_histogram::HistogramVec,
    report_grpc_status: bool,
    logger: Arc<dyn Logger>,
    error_log: GrpcErrorLog,
    status: Arc<Mutex<(Option<i32>, Option<String>)>>,
    done: bool,
}

impl RpcGuard {
    fn finish(&mut self, completed: bool) {
        if std::mem::replace(&mut self.done, true) {
            return;
        }
        let (code, msg) = self.status.lock().clone();
        let code = match code {
            Some(c) => c,
            None if completed => 0,
            // A stream torn down before its status was sent (client reset, or the request
            // deadline expired) is a canceled handler context in grpc-go.
            None => 1,
        };
        let elapsed = self.begin.elapsed();
        // dskit derives the label from the handler's Go error, which may differ from the code
        // grpc-go sends (e.g. a wrapped context.DeadlineExceeded is labelled Unknown).
        let label_code = match self.error_log.instrumentation_code() {
            Some(c) if code != 0 => c,
            _ => code,
        };
        self.duration_vec
            .with_label_values(&[
                "gRPC",
                &self.method,
                &grpc_status_label(label_code, self.report_grpc_status),
                "false",
            ])
            .observe(elapsed.as_secs_f64());
        if let Some(g) = &self.inflight {
            g.dec();
        }
        if let Some(t) = &self.stream_tracker {
            t.close_stream(self.conn.id);
        }
        if self.conn.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            *self.conn.idle_since.lock() = Instant::now();
        }
        if code != 0 {
            // The handler's view of its error (Go error text, OptionalLogging), or a plain
            // `status.Error` as grpc-go would format it.
            let (err, keep) = match self.error_log.take() {
                Some(e) => match e.reason {
                    Some(r) => (format!("{} ({r})", e.err), e.keep),
                    None => (e.err, e.keep),
                },
                None => (
                    format!(
                        "rpc error: code = {} desc = {}",
                        grpc_status_label(code, true),
                        percent_decode(msg.as_deref().unwrap_or(""))
                    ),
                    true,
                ),
            };
            if keep {
                let dur = GoDuration(elapsed.as_nanos() as i64).to_string();
                let level = if code == 1 { LogLevel::Debug } else { LogLevel::Warn };
                self.logger.log(
                    level,
                    &[("method", &self.method), ("duration", &dur), ("msg", "gRPC"), ("err", &err)],
                );
            }
        }
    }
}

impl Drop for RpcGuard {
    fn drop(&mut self) {
        self.finish(false);
    }
}

/// A trailers-only gRPC error response with an explicit HTTP status (grpc-go `writeEarlyAbort`).
fn early_abort(status: tonic::Status, http_status: http::StatusCode) -> GrpcResponse {
    let mut resp = status.into_http();
    *resp.status_mut() = http_status;
    resp
}

async fn handle<B>(
    mut req: hyper::Request<B>,
    remote: SocketAddr,
    conn: Arc<Conn>,
    routes: tonic::service::Routes,
    cfg: GrpcServerConfig,
    ctx: GrpcContext,
) -> Result<GrpcResponse, std::convert::Infallible>
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<super::body::BoxError>,
{
    let method = req.uri().path().to_string();
    let begin = Instant::now();
    let m = &ctx.metrics;

    // grpc-go operateHeaders: a malformed or already expired `grpc-timeout` aborts the stream
    // before any interceptor or stats handler runs.
    let deadline = match req.headers().get("grpc-timeout") {
        None => None,
        Some(v) => {
            match v.to_str().map_err(|e| e.to_string()).and_then(grpc_deadline::decode_timeout) {
                Err(e) => {
                    return Ok(early_abort(
                        tonic::Status::internal(format!("malformed grpc-timeout: {e}")),
                        http::StatusCode::BAD_REQUEST,
                    ))
                }
                Ok(d) if d.is_zero() => {
                    return Ok(early_abort(
                        tonic::Status::deadline_exceeded("context deadline exceeded"),
                        http::StatusCode::OK,
                    ))
                }
                // Overflowing deadlines (e.g. 99999999H) never fire.
                Ok(d) => tokio::time::Instant::now().checked_add(d),
            }
        }
    };

    conn.active.fetch_add(1, Ordering::AcqRel);
    let stream_tracker = if cfg.stats_tracking { m.stream_tracker.clone() } else { None };
    if let Some(t) = &stream_tracker {
        t.open_stream(conn.id);
    }
    let inflight = cfg
        .stats_tracking
        .then(|| m.inflight_requests.with_label_values(&["gRPC", &method]));
    if let Some(g) = &inflight {
        g.inc();
    }
    let error_log = GrpcErrorLog::default();
    // From here on every exit path (including this future being dropped when the client
    // resets the stream) finalizes the RPC exactly once.
    let guard = RpcGuard {
        method: method.clone(),
        begin,
        conn,
        stream_tracker,
        inflight,
        duration_vec: m.request_duration.clone(),
        report_grpc_status: cfg.report_grpc_status,
        logger: Arc::clone(&ctx.logger),
        error_log: error_log.clone(),
        status: Arc::new(Mutex::new((None, None))),
        done: false,
    };

    req.extensions_mut().insert(tonic::transport::server::TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(remote),
    });
    req.extensions_mut().insert(error_log);

    // Interceptor order (dskit server.New): ..., Mimir's GRPCMiddleware (multitenant auth), then
    // cluster validation (unary RPCs only, never health checks).
    let auth_rejection = cfg
        .multitenancy_enabled
        .then(|| check_org_id(req.headers(), &method))
        .flatten();
    if auth_rejection.is_some() {
        // `user.ErrNoOrgID` is a plain Go error: logged as its text.
        guard.error_log.set(GrpcErrorLogEntry {
            err: "no org id".to_string(),
            keep: true,
            reason: None,
        });
    }
    let rejection = auth_rejection.or_else(|| {
            cfg.cluster_validation.as_ref().and_then(|cv| {
                if STREAMING_METHODS.contains(&method.as_str())
                    || method.starts_with("/grpc.health.v1.Health/")
                {
                    return None;
                }
                check_cluster(req.headers(), &method, cv, &ctx)
            })
        });

    let req = if cfg.stats_tracking {
        let recv = m.received_message_size.with_label_values(&["gRPC", &method]);
        let mut counter = GrpcFrameCounter::default();
        req.map(|b| {
            tonic::body::boxed(TrackedBody::new(b).on_frame(Box::new(move |f: &Frame<Bytes>| {
                if let Some(d) = f.data_ref() {
                    counter.feed(d, |n| recv.observe(n as f64));
                }
            })))
        })
    } else {
        req.map(tonic::body::boxed)
    };

    let resp: GrpcResponse = match rejection {
        Some(status) => status.into_http(),
        None => {
            let call = routes.oneshot(req);
            let res = match deadline {
                // grpc-go: at the deadline the stream is closed with RST_STREAM(CANCEL) and the
                // handler's context is canceled; dropping the handler future cancels its waits.
                Some(d) => match tokio::time::timeout_at(d, call).await {
                    Ok(r) => r,
                    Err(_) => return Ok(reset_stream_response()),
                },
                None => call.await,
            };
            match res {
                Ok(r) => r,
                Err(e) => tonic::Status::internal(e.to_string()).into_http(),
            }
        }
    };

    // Trailers-only responses carry grpc-status in the headers.
    let header_status = resp
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i32>().ok());
    let header_message = resp
        .headers()
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    *guard.status.lock() = (header_status, header_message);

    let sent = cfg
        .stats_tracking
        .then(|| m.sent_message_size.with_label_values(&["gRPC", &method]));
    let mut out_counter = GrpcFrameCounter::default();
    let status_f = Arc::clone(&guard.status);
    let on_frame = Box::new(move |f: &Frame<Bytes>| {
        if let Some(d) = f.data_ref() {
            if let Some(h) = &sent {
                out_counter.feed(d, |n| h.observe(n as f64));
            }
        } else if let Some(t) = f.trailers_ref() {
            let code = t
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i32>().ok());
            let msg = t.get("grpc-message").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
            let mut s = status_f.lock();
            if code.is_some() {
                s.0 = code;
                s.1 = msg;
            }
        }
    });
    let on_done = Box::new(move |completed: bool| {
        let mut guard = guard;
        guard.finish(completed);
    });
    // grpc-go closes the stream (RST_STREAM CANCEL) when the deadline expires mid-response.
    let body_deadline = deadline.map(tokio::time::Instant::into_std);
    Ok(resp.map(|b| {
        tonic::body::boxed(
            TrackedBody::new(b)
                .on_frame(on_frame)
                .on_done(on_done)
                .deadline_with(body_deadline, cancel_stream_error),
        )
    }))
}

/// The error making hyper reset the HTTP/2 stream with `RST_STREAM(CANCEL)`.
fn cancel_stream_error() -> super::body::BoxError {
    Box::new(h2::Error::from(h2::Reason::CANCEL))
}

/// A response whose body immediately fails with [`cancel_stream_error`]: the stream is reset
/// like grpc-go does when the request deadline expires before the handler responded.
fn reset_stream_response() -> GrpcResponse {
    struct ResetBody;
    impl http_body::Body for ResetBody {
        type Data = Bytes;
        type Error = super::body::BoxError;
        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            std::task::Poll::Ready(Some(Err(cancel_stream_error())))
        }
    }
    let mut resp = hyper::Response::new(tonic::body::boxed(ResetBody));
    resp.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/grpc"),
    );
    resp
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// dskit `checkClusterFromIncomingContext`.
fn check_cluster(
    headers: &http::HeaderMap,
    method: &str,
    cv: &GrpcClusterValidation,
    ctx: &GrpcContext,
) -> Option<tonic::Status> {
    let values: Vec<&str> = headers
        .get_all("x-cluster")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    let expected = format!("[{}]", cv.allowed.join(" "));
    let first = cv.allowed.first().map_or("", String::as_str);
    let reject = |msg: String| {
        mimir_proto::grpcutilpb::status_with_grpcutil_error_details(
            tonic::Code::FailedPrecondition,
            msg,
            &mimir_proto::grpcutilpb::ErrorDetails {
                cause: mimir_proto::grpcutilpb::ErrorCause::WrongClusterValidationLabel as i32,
            },
        )
    };
    let log = |extra: &[(&str, &str)]| {
        let soft = cv.soft.to_string();
        let mut kvs: Vec<(&str, &str)> = vec![
            ("method", method),
            ("cluster_validation_labels", &expected),
            ("soft_validation", &soft),
        ];
        kvs.extend_from_slice(extra);
        ctx.logger.log(LogLevel::Warn, &kvs);
    };
    if values.len() > 1 {
        let msg = format!(
            "gRPC metadata should contain exactly 1 value for key \"x-cluster\", but it contains [{}]",
            values.join(" ")
        );
        ctx.metrics
            .invalid_cluster_requests
            .with_label_values(&["grpc", method, first, ""])
            .inc();
        log(&[("msg", "detected error during cluster validation label extraction"), ("err", &msg)]);
        return (!cv.soft).then(|| reject(format!("rejected request: {msg}")));
    }
    match values.first().copied().filter(|v| !v.is_empty()) {
        Some(req_cluster) if cv.allowed.iter().any(|a| a == req_cluster) => None,
        Some(req_cluster) => {
            ctx.metrics
                .invalid_cluster_requests
                .with_label_values(&["grpc", method, first, req_cluster])
                .inc();
            log(&[
                ("msg", "request with wrong cluster validation label"),
                ("request_cluster_validation_label", req_cluster),
            ]);
            (!cv.soft).then(|| {
                reject(format!(
                    "rejected request with wrong cluster validation label {req_cluster:?} - it should be one of {expected}"
                ))
            })
        }
        None => {
            ctx.metrics
                .invalid_cluster_requests
                .with_label_values(&["grpc", method, first, ""])
                .inc();
            log(&[("msg", "request with no cluster validation label")]);
            (!cv.soft).then(|| {
                reject(format!(
                    "rejected request with empty cluster validation label - it should be one of {expected}"
                ))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A gRPC service whose unary handler never completes (like a strong read consistency wait).
    #[derive(Clone)]
    struct HangingSvc;

    impl tonic::server::NamedService for HangingSvc {
        const NAME: &'static str = "test.Hanging";
    }

    impl tower::Service<http::Request<tonic::body::BoxBody>> for HangingSvc {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
            Box::pin(async move {
                if req.uri().path() == "/test.Hanging/Fast" {
                    return Ok(tonic::Status::ok("").into_http());
                }
                std::future::pending().await
            })
        }
    }

    #[derive(Default)]
    struct VecLogger(Mutex<Vec<String>>);

    impl Logger for VecLogger {
        fn log(&self, level: LogLevel, kvs: &[(&str, &str)]) {
            let kv: Vec<String> = kvs.iter().map(|(k, v)| format!("{k}={v}")).collect();
            self.0.lock().push(format!("{level:?} {}", kv.join(" ")));
        }
    }

    struct Fixture {
        reg: prometheus::Registry,
        conn: Arc<Conn>,
        ctx: GrpcContext,
        cfg: GrpcServerConfig,
        routes: tonic::service::Routes,
    }

    fn fixture(multitenancy_enabled: bool) -> Fixture {
        let reg = prometheus::Registry::new();
        let metrics = ServerMetrics::new(&reg, true, true).unwrap();
        Fixture {
            reg,
            conn: Arc::new(Conn {
                id: 1,
                active: AtomicUsize::new(0),
                idle_since: Mutex::new(Instant::now()),
            }),
            ctx: GrpcContext {
                metrics,
                logger: Arc::new(VecLogger::default()),
            },
            cfg: GrpcServerConfig {
                max_concurrent_streams: 0,
                keepalive_time: None,
                keepalive_timeout: Duration::from_secs(20),
                max_connection_idle: None,
                max_connection_age: None,
                max_connection_age_grace: None,
                tls: None,
                stats_tracking: true,
                report_grpc_status: true,
                cluster_validation: None,
                multitenancy_enabled,
            },
            routes: tonic::service::Routes::new(HangingSvc).prepare(),
        }
    }

    impl Fixture {
        fn call(
            &self,
            path: &str,
            headers: &[(&str, &str)],
        ) -> impl Future<Output = Result<GrpcResponse, std::convert::Infallible>> + Send + 'static {
            let mut b = http::Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/grpc");
            for (k, v) in headers {
                b = b.header(*k, *v);
            }
            let req = b.body(tonic::body::empty_body()).unwrap();
            handle(
                req,
                "127.0.0.1:1".parse().unwrap(),
                Arc::clone(&self.conn),
                self.routes.clone(),
                self.cfg.clone(),
                self.ctx.clone(),
            )
        }

        fn gauge(&self, name: &str, route: &str) -> f64 {
            self.sample(name, route, None, |m| m.get_gauge().get_value())
        }

        fn duration_count(&self, route: &str, status: &str) -> f64 {
            self.sample("cortex_request_duration_seconds", route, Some(status), |m| {
                m.get_histogram().get_sample_count() as f64
            })
        }

        fn sample(
            &self,
            name: &str,
            route: &str,
            status: Option<&str>,
            f: impl Fn(&prometheus::proto::Metric) -> f64,
        ) -> f64 {
            for mf in self.reg.gather() {
                if mf.get_name() != name {
                    continue;
                }
                for m in mf.get_metric() {
                    let has = |k: &str, v: &str| {
                        m.get_label().iter().any(|l| l.get_name() == k && l.get_value() == v)
                    };
                    if has("route", route) && status.is_none_or(|s| has("status_code", s)) {
                        return f(m);
                    }
                }
            }
            0.0
        }
    }

    async fn drain(resp: GrpcResponse) -> Option<String> {
        use http_body_util::BodyExt;
        let header = resp.headers().get("grpc-status").map(|v| v.to_str().unwrap().to_string());
        let collected = resp.into_body().collect().await.ok()?;
        header.or_else(|| {
            collected
                .trailers()
                .and_then(|t| t.get("grpc-status"))
                .map(|v| v.to_str().unwrap().to_string())
        })
    }

    #[tokio::test]
    async fn canceled_rpc_does_not_leak_inflight_streams_or_conn_activity() {
        let fx = fixture(false);
        let route = "/test.Hanging/Wait";
        let task = tokio::spawn(fx.call(route, &[]));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fx.gauge("cortex_inflight_requests", route), 1.0);
        assert_eq!(fx.conn.active.load(Ordering::Acquire), 1);

        // Client RST_STREAM: hyper drops the handler future.
        task.abort();
        let _ = task.await;
        assert_eq!(fx.gauge("cortex_inflight_requests", route), 0.0);
        assert_eq!(fx.conn.active.load(Ordering::Acquire), 0);
        assert_eq!(fx.duration_count(route, "Canceled"), 1.0);
    }

    #[tokio::test]
    async fn grpc_timeout_resets_the_stream_like_grpc_go() {
        use http_body_util::BodyExt;

        let fx = fixture(false);
        let route = "/test.Hanging/Wait";
        let started = Instant::now();
        let resp = fx.call(route, &[("grpc-timeout", "100m")]).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(started.elapsed() < Duration::from_secs(5));
        // The handler was dropped (its waits canceled): nothing is in flight any more.
        assert_eq!(fx.gauge("cortex_inflight_requests", route), 0.0);
        assert_eq!(fx.conn.active.load(Ordering::Acquire), 0);
        // RST_STREAM(CANCEL) instead of a status; recorded as a canceled RPC.
        let Err(err) = resp.into_body().collect().await else {
            panic!("expected a stream reset");
        };
        // hyper picks the reset reason from the h2::Error in the body error's source chain.
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&err);
        let mut reason = None;
        while let Some(e) = source {
            if let Some(h2e) = e.downcast_ref::<h2::Error>() {
                reason = h2e.reason();
                break;
            }
            source = e.source();
        }
        assert_eq!(reason, Some(h2::Reason::CANCEL));
        assert_eq!(fx.duration_count(route, "Canceled"), 1.0);
    }

    #[tokio::test]
    async fn malformed_or_expired_grpc_timeout_aborts_early() {
        let fx = fixture(false);
        let route = "/test.Hanging/Fast";
        let resp = fx.call(route, &[("grpc-timeout", "1x")]).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
        let st = tonic::Status::from_header_map(resp.headers()).unwrap();
        assert_eq!(st.code(), tonic::Code::Internal);
        assert_eq!(
            st.message(),
            "malformed grpc-timeout: transport: timeout unit is not recognized: \"1x\""
        );
        let resp = fx.call(route, &[("grpc-timeout", "0n")]).await.unwrap();
        let st = tonic::Status::from_header_map(resp.headers()).unwrap();
        assert_eq!(st.code(), tonic::Code::DeadlineExceeded);
        // Early aborts never reach the interceptors / stats handler.
        assert_eq!(fx.duration_count(route, "Internal"), 0.0);
        assert_eq!(fx.conn.active.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn multitenancy_requires_exactly_one_org_id_except_no_auth_methods() {
        let fx = fixture(true);
        let route = "/test.Hanging/Fast";
        let resp = fx.call(route, &[]).await.unwrap();
        let st = tonic::Status::from_header_map(resp.headers()).unwrap();
        assert_eq!(st.code(), tonic::Code::Unknown);
        assert_eq!(st.message(), "no org id");
        assert_eq!(drain(resp).await.as_deref(), Some("2"));
        let resp = fx
            .call(route, &[("x-scope-orgid", "a"), ("x-scope-orgid", "a")])
            .await
            .unwrap();
        assert_eq!(drain(resp).await.as_deref(), Some("2"));
        let resp = fx.call(route, &[("x-scope-orgid", "a|b")]).await.unwrap();
        assert_eq!(drain(resp).await.as_deref(), Some("0"));
        assert_eq!(fx.duration_count(route, "Unknown"), 2.0);
        assert!(check_org_id(&http::HeaderMap::new(), "/grpc.health.v1.Health/Check").is_none());

        let fx = fixture(false);
        let resp = fx.call(route, &[]).await.unwrap();
        assert_eq!(drain(resp).await.as_deref(), Some("0"));
    }
}
