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

use std::borrow::Cow;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body::Frame;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use mimir_common::grpc_deadline;
use mimir_common::grpc_log::{GrpcErrorLog, GrpcErrorLogEntry};
use mimir_common::logging::{LogLevel, LogValue, Logger};
use mimir_common::tracing::{
    export_span, extract_from_http_headers, global_tracing, now_unix_nanos,
    start_server_span_context, with_trace_context, ExportedSpan, SpanAttrValue, SpanAttribute,
    TraceContext,
};
use mimir_common::native_histogram::Histogram;
use mimir_config::GoDuration;
use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tower::ServiceExt;

use super::body::{BodyObserver, GrpcFrameCounter, TrackedBody};
use super::listener::{finish_accept, is_fatal_accept_error, Acceptor, PrefixedIo};
use super::metrics::ServerMetrics;

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
    pub no_auth_tenant: String,
    pub tap_ingester: Option<Arc<mimir_ingester::Ingester>>,
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
    "/cortex.Ingester/SearchLabelNames",
    "/cortex.Ingester/SearchLabelValues",
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

/// Known gRPC routes mapped to `&'static str` + fixed slot indices so hot RPCs avoid
/// `String` allocation and `RwLock` + `HashMap` metric lookups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
enum KnownGrpcMethod {
    IngesterPush = 0,
    IngesterQueryStream = 1,
    IngesterQueryExemplars = 2,
    IngesterLabelValues = 3,
    IngesterLabelNames = 4,
    IngesterUserStats = 5,
    IngesterAllUserStats = 6,
    IngesterMetricsForLabelMatchers = 7,
    IngesterMetricsMetadata = 8,
    IngesterLabelNamesAndValues = 9,
    IngesterLabelValuesCardinality = 10,
    IngesterActiveSeries = 11,
    IngesterSearchLabelNames = 12,
    IngesterSearchLabelValues = 13,
    HttpGrpcHandle = 14,
    HealthCheck = 15,
    HealthWatch = 16,
    HealthList = 17,
}

const NUM_KNOWN_GRPC_METHODS: usize = 18;

impl KnownGrpcMethod {
    #[inline]
    const fn path(self) -> &'static str {
        match self {
            Self::IngesterPush => "/cortex.Ingester/Push",
            Self::IngesterQueryStream => "/cortex.Ingester/QueryStream",
            Self::IngesterQueryExemplars => "/cortex.Ingester/QueryExemplars",
            Self::IngesterLabelValues => "/cortex.Ingester/LabelValues",
            Self::IngesterLabelNames => "/cortex.Ingester/LabelNames",
            Self::IngesterUserStats => "/cortex.Ingester/UserStats",
            Self::IngesterAllUserStats => "/cortex.Ingester/AllUserStats",
            Self::IngesterMetricsForLabelMatchers => "/cortex.Ingester/MetricsForLabelMatchers",
            Self::IngesterMetricsMetadata => "/cortex.Ingester/MetricsMetadata",
            Self::IngesterLabelNamesAndValues => "/cortex.Ingester/LabelNamesAndValues",
            Self::IngesterLabelValuesCardinality => "/cortex.Ingester/LabelValuesCardinality",
            Self::IngesterActiveSeries => "/cortex.Ingester/ActiveSeries",
            Self::IngesterSearchLabelNames => "/cortex.Ingester/SearchLabelNames",
            Self::IngesterSearchLabelValues => "/cortex.Ingester/SearchLabelValues",
            Self::HttpGrpcHandle => "/httpgrpc.HTTP/Handle",
            Self::HealthCheck => "/grpc.health.v1.Health/Check",
            Self::HealthWatch => "/grpc.health.v1.Health/Watch",
            Self::HealthList => "/grpc.health.v1.Health/List",
        }
    }

    #[inline]
    const fn is_ingester(self) -> bool {
        !matches!(
            self,
            Self::HttpGrpcHandle | Self::HealthCheck | Self::HealthWatch | Self::HealthList
        )
    }

    #[inline]
    const fn is_no_grpc_auth(self) -> bool {
        matches!(self, Self::HealthCheck)
    }

    #[inline]
    const fn is_streaming_or_health(self) -> bool {
        matches!(
            self,
            Self::IngesterQueryStream
                | Self::IngesterLabelNamesAndValues
                | Self::IngesterLabelValuesCardinality
                | Self::IngesterActiveSeries
                | Self::IngesterSearchLabelNames
                | Self::IngesterSearchLabelValues
                | Self::HealthCheck
                | Self::HealthWatch
                | Self::HealthList
        )
    }
}

#[inline]
fn classify_grpc_method(path: &str) -> (Option<KnownGrpcMethod>, Cow<'static, str>) {
    let known = match path {
        "/cortex.Ingester/Push" => Some(KnownGrpcMethod::IngesterPush),
        "/cortex.Ingester/QueryStream" => Some(KnownGrpcMethod::IngesterQueryStream),
        "/cortex.Ingester/QueryExemplars" => Some(KnownGrpcMethod::IngesterQueryExemplars),
        "/cortex.Ingester/LabelValues" => Some(KnownGrpcMethod::IngesterLabelValues),
        "/cortex.Ingester/LabelNames" => Some(KnownGrpcMethod::IngesterLabelNames),
        "/cortex.Ingester/UserStats" => Some(KnownGrpcMethod::IngesterUserStats),
        "/cortex.Ingester/AllUserStats" => Some(KnownGrpcMethod::IngesterAllUserStats),
        "/cortex.Ingester/MetricsForLabelMatchers" => {
            Some(KnownGrpcMethod::IngesterMetricsForLabelMatchers)
        }
        "/cortex.Ingester/MetricsMetadata" => Some(KnownGrpcMethod::IngesterMetricsMetadata),
        "/cortex.Ingester/LabelNamesAndValues" => {
            Some(KnownGrpcMethod::IngesterLabelNamesAndValues)
        }
        "/cortex.Ingester/LabelValuesCardinality" => {
            Some(KnownGrpcMethod::IngesterLabelValuesCardinality)
        }
        "/cortex.Ingester/ActiveSeries" => Some(KnownGrpcMethod::IngesterActiveSeries),
        "/cortex.Ingester/SearchLabelNames" => Some(KnownGrpcMethod::IngesterSearchLabelNames),
        "/cortex.Ingester/SearchLabelValues" => Some(KnownGrpcMethod::IngesterSearchLabelValues),
        "/httpgrpc.HTTP/Handle" => Some(KnownGrpcMethod::HttpGrpcHandle),
        "/grpc.health.v1.Health/Check" => Some(KnownGrpcMethod::HealthCheck),
        "/grpc.health.v1.Health/Watch" => Some(KnownGrpcMethod::HealthWatch),
        "/grpc.health.v1.Health/List" => Some(KnownGrpcMethod::HealthList),
        _ => None,
    };
    match known {
        Some(m) => (Some(m), Cow::Borrowed(m.path())),
        None => (None, Cow::Owned(path.to_owned())),
    }
}

#[derive(Default)]
struct MethodMetricsSlot {
    recv_size: OnceLock<Arc<Histogram>>,
    sent_size: OnceLock<Arc<Histogram>>,
    inflight: OnceLock<prometheus::Gauge>,
    duration_ok: OnceLock<Arc<Histogram>>,
    duration_canceled: OnceLock<Arc<Histogram>>,
}

struct GrpcMetricsCache {
    slots: [MethodMetricsSlot; NUM_KNOWN_GRPC_METHODS],
    metrics: ServerMetrics,
    logger: Arc<dyn Logger>,
    stats_tracking: bool,
    report_grpc_status: bool,
}

impl GrpcMetricsCache {
    fn new(ctx: &GrpcContext, stats_tracking: bool, report_grpc_status: bool) -> Self {
        Self {
            slots: std::array::from_fn(|_| MethodMetricsSlot::default()),
            metrics: ctx.metrics.clone(),
            logger: Arc::clone(&ctx.logger),
            stats_tracking,
            report_grpc_status,
        }
    }

    #[inline]
    fn recv_size(&self, known: Option<KnownGrpcMethod>, method: &str) -> Arc<Histogram> {
        match known {
            Some(k) => Arc::clone(
                self.slots[k as usize]
                    .recv_size
                    .get_or_init(|| self.metrics.received_message_size.with_label_values(&["gRPC", k.path()])),
            ),
            None => self.metrics.received_message_size.with_label_values(&["gRPC", method]),
        }
    }

    #[inline]
    fn sent_size(&self, known: Option<KnownGrpcMethod>, method: &str) -> Arc<Histogram> {
        match known {
            Some(k) => Arc::clone(
                self.slots[k as usize]
                    .sent_size
                    .get_or_init(|| self.metrics.sent_message_size.with_label_values(&["gRPC", k.path()])),
            ),
            None => self.metrics.sent_message_size.with_label_values(&["gRPC", method]),
        }
    }

    #[inline]
    fn inc_inflight(&self, known: Option<KnownGrpcMethod>, method: &str) -> Option<prometheus::Gauge> {
        match known {
            Some(k) => {
                self.slots[k as usize]
                    .inflight
                    .get_or_init(|| self.metrics.inflight_requests.with_label_values(&["gRPC", k.path()]))
                    .inc();
                None
            }
            None => {
                let g = self.metrics.inflight_requests.with_label_values(&["gRPC", method]);
                g.inc();
                Some(g)
            }
        }
    }

    #[inline]
    fn dec_inflight(
        &self,
        known: Option<KnownGrpcMethod>,
        unknown_inflight: Option<&prometheus::Gauge>,
    ) {
        if let Some(k) = known {
            if let Some(g) = self.slots[k as usize].inflight.get() {
                g.dec();
            }
        } else if let Some(g) = unknown_inflight {
            g.dec();
        }
    }

    #[inline]
    fn observe_duration(
        &self,
        known: Option<KnownGrpcMethod>,
        method: &str,
        label_code: i32,
        secs: f64,
    ) {
        if let Some(k) = known {
            if label_code == 0 {
                let ok_label = if self.report_grpc_status { "OK" } else { "success" };
                self.slots[k as usize]
                    .duration_ok
                    .get_or_init(|| {
                        self.metrics
                            .request_duration
                            .with_label_values(&["gRPC", k.path(), ok_label, "false"])
                    })
                    .observe(secs);
                return;
            }
            if label_code == 1 {
                let cancel_label = if self.report_grpc_status {
                    "Canceled"
                } else {
                    "cancel"
                };
                self.slots[k as usize]
                    .duration_canceled
                    .get_or_init(|| {
                        self.metrics
                            .request_duration
                            .with_label_values(&["gRPC", k.path(), cancel_label, "false"])
                    })
                    .observe(secs);
                return;
            }
        }
        let status = grpc_status_label_cow(label_code, self.report_grpc_status);
        self.metrics
            .request_duration
            .with_label_values(&["gRPC", method, status.as_ref(), "false"])
            .observe(secs);
    }
}

#[inline]
fn grpc_status_label_cow(code: i32, report_grpc_status: bool) -> Cow<'static, str> {
    if (100..600).contains(&code) {
        return Cow::Owned(code.to_string());
    }
    if report_grpc_status {
        let s = match code {
            0 => "OK",
            1 => "Canceled",
            2 => "Unknown",
            3 => "InvalidArgument",
            4 => "DeadlineExceeded",
            5 => "NotFound",
            6 => "AlreadyExists",
            7 => "PermissionDenied",
            8 => "ResourceExhausted",
            9 => "FailedPrecondition",
            10 => "Aborted",
            11 => "OutOfRange",
            12 => "Unimplemented",
            13 => "Internal",
            14 => "Unavailable",
            15 => "DataLoss",
            16 => "Unauthenticated",
            other => return Cow::Owned(format!("Code({other})")),
        };
        return Cow::Borrowed(s);
    }
    Cow::Borrowed(match code {
        0 => "success",
        1 => "cancel",
        _ => "error",
    })
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
    let cache = Arc::new(GrpcMetricsCache::new(
        &ctx,
        cfg.stats_tracking,
        cfg.report_grpc_status,
    ));
    let acceptor = Arc::new(acceptor);
    let routes = routes.prepare();
    let mut conns: JoinSet<()> = JoinSet::new();
    let mut accept_backoff = Duration::from_millis(5);
    loop {
        while conns.try_join_next().is_some() {}
        tokio::select! {
            biased;
            _ = super::wait_shutdown(&mut shutdown) => break,
            res = acceptor.accept() => {
                let (stream, addr, guard) = match res {
                    Ok(v) => {
                        accept_backoff = Duration::from_millis(5);
                        v
                    }
                    Err(err) => {
                        tracing::error!(err = %err, "grpc server accept error");
                        if is_fatal_accept_error(&err) {
                            break;
                        }
                        tokio::select! {
                            _ = super::wait_shutdown(&mut shutdown) => break,
                            _ = tokio::time::sleep(accept_backoff) => {}
                        }
                        accept_backoff = (accept_backoff * 2).min(Duration::from_secs(1));
                        continue;
                    }
                };
                let acc = Arc::clone(&acceptor);
                let cfg = cfg.clone();
                let routes = routes.clone();
                let cache = Arc::clone(&cache);
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
                            serve_conn(s, remote, cfg, routes, cache, shutdown).await;
                        }
                    } else {
                        serve_conn(io, remote, cfg, routes, cache, shutdown).await;
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
    cache: Arc<GrpcMetricsCache>,
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
            handle(
                req,
                remote,
                Arc::clone(&conn_state),
                routes.clone(),
                cfg.clone(),
                Arc::clone(&cache),
            )
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
fn check_org_id_known(
    headers: &http::HeaderMap,
    known: Option<KnownGrpcMethod>,
    method: &str,
) -> Option<tonic::Status> {
    let exempt = match known {
        Some(k) => k.is_no_grpc_auth(),
        None => NO_GRPC_AUTH_ON.contains(&method),
    };
    if exempt || headers.get_all("x-scope-orgid").iter().count() == 1 {
        return None;
    }
    Some(tonic::Status::unknown("no org id"))
}

#[cfg(test)]
fn check_org_id(headers: &http::HeaderMap, method: &str) -> Option<tonic::Status> {
    let (known, _) = classify_grpc_method(method);
    check_org_id_known(headers, known, method)
}

/// Per-RPC bookkeeping started when the request arrives and finalized exactly once: when the
/// response body completes, or on drop (handler future or response body dropped because the
/// client reset the stream), like the `defer`s of dskit's interceptors and stats handler.
/// Shared mutable attributes attached to the parent gRPC server span by inner handlers
/// (e.g. `HttpGrpcBridge` attaching encapsulated HTTP attributes to `httpgrpc.HTTP/Handle`).
#[derive(Clone, Default)]
pub struct GrpcSpanAttributes(pub Arc<Mutex<Vec<SpanAttribute>>>);

/// otelgrpc `serverStatus` (interceptor.go): a server span gets `codes.Error` only for
/// `Unknown`, `DeadlineExceeded`, `Unimplemented`, `Internal`, `Unavailable` and `DataLoss`.
/// Every other status stays `Unset`, including the HTTP status codes (`4xx`/`5xx`) that
/// `httpgrpc` errors carry as their gRPC code.
#[inline]
fn is_grpc_server_span_error(code: i32) -> bool {
    matches!(code, 2 | 4 | 12 | 13 | 14 | 15)
}

struct RpcGuard {
    known_method: Option<KnownGrpcMethod>,
    method: Cow<'static, str>,
    begin: Instant,
    conn: Arc<Conn>,
    cache: Arc<GrpcMetricsCache>,
    unknown_inflight: Option<prometheus::Gauge>,
    error_log: GrpcErrorLog,
    status_code: Option<i32>,
    status_message: Option<String>,
    span_ctx: TraceContext,
    parent_span_id: Option<[u8; 8]>,
    span_start_unix_nano: u64,
    span_attributes: GrpcSpanAttributes,
    suppressed: bool,
    done: bool,
}

impl RpcGuard {
    fn suppress(&mut self) {
        self.suppressed = true;
    }

    fn finish(&mut self, completed: bool) {
        if std::mem::replace(&mut self.done, true) {
            return;
        }
        if self.cache.stats_tracking {
            self.cache
                .dec_inflight(self.known_method, self.unknown_inflight.as_ref());
            if let Some(t) = &self.cache.metrics.stream_tracker {
                t.close_stream(self.conn.id);
            }
        }
        if self.conn.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            *self.conn.idle_since.lock() = Instant::now();
        }
        if self.suppressed {
            return;
        }
        let code = match self.status_code {
            Some(c) => c,
            None if completed => 0,
            // A stream torn down before its status was sent (client reset, or the request
            // deadline expired) is a canceled handler context in grpc-go.
            None => 1,
        };
        let elapsed = self.begin.elapsed();
        // dskit derives the label from the handler's Go error, which may differ from the code
        // grpc-go sends (e.g. a wrapped context.DeadlineExceeded is labelled Unknown).
        let label_code = if code != 0 {
            self.error_log.instrumentation_code().unwrap_or(code)
        } else {
            0
        };
        self.cache.observe_duration(
            self.known_method,
            &self.method,
            label_code,
            elapsed.as_secs_f64(),
        );
        if self.span_ctx.is_sampled() {
            if let Some(rt) = global_tracing() {
                if rt.exporter.is_some() {
                    let trimmed = self.method.trim_start_matches('/');
                    let (rpc_service, rpc_method) = trimmed
                        .split_once('/')
                        .unwrap_or(("", trimmed));
                    let mut attributes = vec![SpanAttribute {
                        key: "rpc.system".to_string(),
                        value: SpanAttrValue::String("grpc".to_string()),
                    }];
                    if !rpc_service.is_empty() {
                        attributes.push(SpanAttribute {
                            key: "rpc.service".to_string(),
                            value: SpanAttrValue::String(rpc_service.to_string()),
                        });
                    }
                    if !rpc_method.is_empty() {
                        attributes.push(SpanAttribute {
                            key: "rpc.method".to_string(),
                            value: SpanAttrValue::String(rpc_method.to_string()),
                        });
                    }
                    attributes.push(SpanAttribute {
                        key: "rpc.grpc.status_code".to_string(),
                        value: SpanAttrValue::Int(i64::from(code)),
                    });
                    attributes.extend(self.span_attributes.0.lock().iter().cloned());
                    export_span(ExportedSpan {
                        service_name: rt.service_name.clone(),
                        trace_id: self.span_ctx.trace_id,
                        span_id: self.span_ctx.span_id,
                        parent_span_id: self.parent_span_id,
                        name: trimmed.to_string(),
                        kind: 2,
                        start_time_unix_nano: self.span_start_unix_nano,
                        end_time_unix_nano: now_unix_nanos(),
                        attributes,
                        status_error: is_grpc_server_span_error(code),
                    });
                }
            }
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
                        grpc_status_label_cow(code, true),
                        percent_decode(self.status_message.as_deref().unwrap_or(""))
                    ),
                    true,
                ),
            };
            if keep {
                let dur = GoDuration(elapsed.as_nanos() as i64).to_string();
                let level = if code == 1 { LogLevel::Debug } else { LogLevel::Warn };
                self.cache.logger.log(
                    level,
                    &[
                        ("method", self.method.as_ref()),
                        ("duration", &dur),
                        ("msg", "gRPC"),
                        ("err", &err),
                    ],
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

struct GrpcRecvObserver {
    counter: GrpcFrameCounter,
    recv: Arc<Histogram>,
}

impl BodyObserver for GrpcRecvObserver {
    #[inline]
    fn on_frame(&mut self, frame: &Frame<Bytes>) {
        if let Some(d) = frame.data_ref() {
            self.counter.feed(d, |n| self.recv.observe(n as f64));
        }
    }

    #[inline]
    fn on_done(&mut self, _completed: bool) {}
}

struct GrpcRespObserver {
    guard: RpcGuard,
    sent: Option<Arc<Histogram>>,
    out_counter: GrpcFrameCounter,
}

impl BodyObserver for GrpcRespObserver {
    #[inline]
    fn on_frame(&mut self, frame: &Frame<Bytes>) {
        if let Some(d) = frame.data_ref() {
            if let Some(h) = &self.sent {
                self.out_counter.feed(d, |n| h.observe(n as f64));
            }
        } else if let Some(t) = frame.trailers_ref() {
            let code = t
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i32>().ok());
            if let Some(code) = code {
                self.guard.status_code = Some(code);
                self.guard.status_message = t
                    .get("grpc-message")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
            }
        }
    }

    #[inline]
    fn on_done(&mut self, completed: bool) {
        self.guard.finish(completed);
    }
}

/// A trailers-only gRPC error response with an explicit HTTP status (grpc-go `writeEarlyAbort`).
fn early_abort(status: tonic::Status, http_status: http::StatusCode) -> GrpcResponse {
    let mut resp = status.into_http();
    *resp.status_mut() = http_status;
    resp
}

fn parse_tap_message_size_header(headers: &http::HeaderMap) -> i64 {
    let mut iter = headers.get_all("message-size").iter();
    let Some(first) = iter.next() else {
        return 0;
    };
    if iter.next().is_some() {
        return 0;
    }
    first
        .to_str()
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

fn unavailable_tap_response(err: impl std::fmt::Display) -> GrpcResponse {
    let mut resp = tonic::Status::unavailable(err.to_string()).into_http();
    resp.headers_mut().insert(
        "grpc-accept-encoding",
        http::HeaderValue::from_static("snappy,s2,gzip,identity"),
    );
    resp.extensions_mut().insert(mimir_ingester::grpc::TapRejected);
    resp
}

async fn handle<B>(
    mut req: hyper::Request<B>,
    remote: SocketAddr,
    conn: Arc<Conn>,
    routes: tonic::service::Routes,
    cfg: GrpcServerConfig,
    cache: Arc<GrpcMetricsCache>,
) -> Result<GrpcResponse, std::convert::Infallible>
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<super::body::BoxError>,
{
    let (known_method, method) = classify_grpc_method(req.uri().path());
    let begin = Instant::now();

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

    let extracted_trace = extract_from_http_headers(req.headers());
    let (span_ctx, parent_span_id) = start_server_span_context(&extracted_trace, false);
    let span_start_unix_nano = now_unix_nanos();

    // Interceptor order (dskit server.New): TapHandle admission (`RPCCallStarting`) runs in
    // grpc-go transport header processing BEFORE `grpcStatsHandler` (`RpcGuard`), Mimir's
    // GRPCMiddleware (multitenant auth), and `ClusterUnaryServerInterceptor` (`check_cluster`).
    // Acquire the tap permit once here (when `tap_ingester` is wired) and forward it via
    // request extensions so `IngesterZeroCopyServer` does not double-acquire.
    let pre_tap = if let Some(ref ing) = cfg.tap_ingester {
        if known_method.is_some_and(KnownGrpcMethod::is_ingester) {
            if known_method == Some(KnownGrpcMethod::IngesterPush) {
                let req_size = parse_tap_message_size_header(req.headers());
                match ing.try_start_push_tap(req_size) {
                    Ok(p) => Some(mimir_ingester::grpc::PreAcquiredTap::Push(Arc::new(
                        Mutex::new(Some(p)),
                    ))),
                    Err(e) => return Ok(unavailable_tap_response(e)),
                }
            } else {
                match ing.try_start_read_tap(req.headers()) {
                    Ok(p) => Some(mimir_ingester::grpc::PreAcquiredTap::Read(Arc::new(
                        Mutex::new(Some(p)),
                    ))),
                    Err(e) => return Ok(unavailable_tap_response(e)),
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // For IngesterPush, `RawPushSvc` runs `dec(in)` before `ServerUserHeaderInterceptor` so
    // malformed protobuf errors beat `no org id`. Still check `check_org_id_known` before
    // `check_cluster` so missing `x-scope-orgid` does not spuriously trip `check_cluster`.
    let auth_rejection = (cfg.multitenancy_enabled
        && known_method != Some(KnownGrpcMethod::IngesterPush))
        .then(|| check_org_id_known(req.headers(), known_method, &method))
        .flatten();
    let push_auth_would_reject = cfg.multitenancy_enabled
        && known_method == Some(KnownGrpcMethod::IngesterPush)
        && check_org_id_known(req.headers(), known_method, &method).is_some();
    let rejection = auth_rejection.clone().or_else(|| {
        if push_auth_would_reject {
            return None;
        }
        cfg.cluster_validation.as_ref().and_then(|cv| {
            let skip = match known_method {
                Some(k) => k.is_streaming_or_health(),
                None => {
                    STREAMING_METHODS.contains(&method.as_ref())
                        || method.starts_with("/grpc.health.v1.Health/")
                }
            };
            if skip {
                return None;
            }
            check_cluster(
                req.headers(),
                &method,
                remote,
                cv,
                &cache,
                cfg.multitenancy_enabled,
                &cfg.no_auth_tenant,
                &span_ctx,
            )
        })
    });

    conn.active.fetch_add(1, Ordering::AcqRel);
    // Pre-resolve all labeled metric handles once per stream (`grpcStatsHandler.TagRPC` in
    // `dskit/middleware/grpc_stats.go`), so `cortex_request_message_bytes` and
    // `cortex_response_message_bytes` expose `count=0` even if the RPC is canceled before
    // receiving or sending a frame.
    let (recv_size, sent_size, unknown_inflight) = if cache.stats_tracking {
        if let Some(t) = &cache.metrics.stream_tracker {
            t.open_stream(conn.id);
        }
        let recv = cache.recv_size(known_method, &method);
        let sent = cache.sent_size(known_method, &method);
        let unknown_inflight = cache.inc_inflight(known_method, &method);
        (Some(recv), Some(sent), unknown_inflight)
    } else {
        (None, None, None)
    };
    let error_log = GrpcErrorLog::default();
    if auth_rejection.is_some() {
        // `user.ErrNoOrgID` is a plain Go error: logged as its text.
        error_log.set(GrpcErrorLogEntry {
            err: "no org id".to_string(),
            keep: true,
            reason: None,
        });
    }
    let span_attributes = GrpcSpanAttributes::default();
    if span_ctx.is_valid() {
        req.extensions_mut().insert(span_ctx.clone());
    }
    req.extensions_mut().insert(span_attributes.clone());
    // From here on every exit path (including this future being dropped when the client
    // resets the stream) finalizes the RPC exactly once.
    let mut guard = RpcGuard {
        known_method,
        method,
        begin,
        conn,
        cache: Arc::clone(&cache),
        unknown_inflight,
        error_log: error_log.clone(),
        status_code: None,
        status_message: None,
        span_ctx: span_ctx.clone(),
        parent_span_id,
        span_start_unix_nano,
        span_attributes,
        suppressed: false,
        done: false,
    };
    req.extensions_mut().insert(tonic::transport::server::TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(remote),
    });
    req.extensions_mut().insert(error_log);
    let deadline_ctx = mimir_ingester::grpc::RpcDeadlineContext::new(
        deadline.map(tokio::time::Instant::into_std),
    );
    req.extensions_mut().insert(Arc::clone(&deadline_ctx));

    if rejection.is_none() {
        if let Some(tap) = pre_tap.as_ref() {
            req.extensions_mut().insert(tap.clone());
        }
    }

    let req = if let Some(recv) = recv_size {
        req.map(|b| {
            tonic::body::boxed(TrackedBody::with_observer(
                b,
                GrpcRecvObserver {
                    counter: GrpcFrameCounter::default(),
                    recv,
                },
            ))
        })
    } else {
        req.map(tonic::body::boxed)
    };

    let resp: GrpcResponse = match rejection {
        Some(status) => {
            if let Some(tap) = pre_tap {
                match tap {
                    mimir_ingester::grpc::PreAcquiredTap::Push(slot) => {
                        drop(slot.lock().take());
                    }
                    mimir_ingester::grpc::PreAcquiredTap::Read(slot) => {
                        if let Some(p) = slot.lock().take() {
                            p.abort();
                        }
                    }
                }
            }
            status.into_http()
        }
        None => {
            let call = with_trace_context(span_ctx.clone(), routes.oneshot(req));
            let res = match deadline {
                // grpc-go: when the transport deadline timer closes the stream before the handler
                // returns `DeadlineExceeded`, `closeStream` sends RST_STREAM(CANCEL) and cancels
                // `s.ctx` with `context.Canceled`, so RpcGuard records `"Canceled"` and tap
                // drop guards treat the aborted future as canceled (not `DeadlineExceeded`).
                Some(d) => tokio::select! {
                    biased;
                    r = call => r,
                    _ = tokio::time::sleep_until(d) => {
                        return Ok(reset_stream_response());
                    }
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
    guard.status_code = resp
        .headers()
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i32>().ok());
    guard.status_message = resp
        .headers()
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    // Genuine `TapHandle` rejections (`RPCCallStarting`) carry the internal `TapRejected`
    // extension and must bypass `cortex_request_duration_seconds` / `GRPCServerLog` / OTel
    // (matching `dskit/server/limits.go:60-63`). Post-TapHandle `Unavailable` errors
    // (InstanceLimit, reactive limiter timeouts/rejections, etc.) MUST be recorded.
    if resp
        .extensions()
        .get::<mimir_ingester::grpc::TapRejected>()
        .is_some()
    {
        guard.suppress();
    }

    // grpc-go closes the stream (RST_STREAM CANCEL) when the deadline expires mid-response,
    // unless the handler already returned a completed trailers-only status response.
    let body_deadline = if guard.status_code.is_some() {
        None
    } else {
        deadline.map(tokio::time::Instant::into_std)
    };
    Ok(resp.map(|b| {
        tonic::body::boxed(
            TrackedBody::with_observer(
                b,
                GrpcRespObserver {
                    guard,
                    sent: sent_size,
                    out_counter: GrpcFrameCounter::default(),
                },
            )
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
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
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
#[allow(clippy::too_many_arguments)]
fn check_cluster(
    headers: &http::HeaderMap,
    method: &str,
    remote: SocketAddr,
    cv: &GrpcClusterValidation,
    cache: &GrpcMetricsCache,
    multitenancy_enabled: bool,
    no_auth_tenant: &str,
    span_ctx: &TraceContext,
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
    let client_addr = remote.to_string();
    let tenant_id = if multitenancy_enabled {
        let mut orgs = headers.get_all(mimir_common::tenant::LOWER_ORG_ID_HEADER_NAME).iter();
        match (orgs.next(), orgs.next()) {
            (Some(v), None) => v.to_str().ok().map(str::to_owned),
            _ => None,
        }
    } else {
        Some(no_auth_tenant.to_string())
    };
    let (trace_id, trace_sampled) = span_ctx.extract_sampled_trace_id();
    let log = |extra: &[(&str, &str)]| {
        let mut kvs: Vec<(&str, LogValue<'_>)> = vec![
            ("method", LogValue::from(method)),
            ("cluster_validation_labels", LogValue::from(expected.as_str())),
            ("soft_validation", LogValue::Bool(cv.soft)),
        ];
        if let Some(ref tid) = tenant_id {
            kvs.push(("tenant", LogValue::from(tid.as_str())));
        }
        kvs.push(("client_address", LogValue::from(client_addr.as_str())));
        if trace_sampled {
            kvs.push(("trace_id", LogValue::from(trace_id.as_str())));
        }
        for &(k, v) in extra {
            kvs.push((k, LogValue::from(v)));
        }
        cache.logger.log_values(LogLevel::Warn, &kvs);
    };
    if values.len() > 1 {
        let msg = format!(
            "gRPC metadata should contain exactly 1 value for key \"x-cluster\", but it contains [{}]",
            values.join(" ")
        );
        cache
            .metrics
            .invalid_cluster_requests
            .with_label_values(&["grpc", method, first, ""])
            .inc();
        log(&[("msg", "detected error during cluster validation label extraction"), ("err", &msg)]);
        return (!cv.soft).then(|| reject(format!("rejected request: {msg}")));
    }
    match values.first().copied().filter(|v| !v.is_empty()) {
        Some(req_cluster) if cv.allowed.iter().any(|a| a == req_cluster) => None,
        Some(req_cluster) => {
            cache
                .metrics
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
            cache
                .metrics
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
                if req.uri().path() == "/test.Hanging/DeadlineStatus" {
                    if let Some(ctx) = req
                        .extensions()
                        .get::<Arc<mimir_ingester::grpc::RpcDeadlineContext>>()
                    {
                        if let Some(dl) = ctx.deadline {
                            // Wait past the deadline without yielding so the trailers-only
                            // DeadlineExceeded response is returned right after `deadline`.
                            let rem = dl.saturating_duration_since(std::time::Instant::now());
                            std::thread::sleep(rem + Duration::from_millis(2));
                        }
                    }
                    return Ok(tonic::Status::deadline_exceeded("handler deadline").into_http());
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
        cache: Arc<GrpcMetricsCache>,
        cfg: GrpcServerConfig,
        routes: tonic::service::Routes,
    }

    fn fixture(multitenancy_enabled: bool) -> Fixture {
        let reg = prometheus::Registry::new();
        let metrics = ServerMetrics::new(&reg, true, true).unwrap();
        let ctx = GrpcContext {
            metrics,
            logger: Arc::new(VecLogger::default()),
        };
        let cfg = GrpcServerConfig {
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
            no_auth_tenant: "anonymous".to_string(),
            tap_ingester: None,
        };
        let cache = Arc::new(GrpcMetricsCache::new(
            &ctx,
            cfg.stats_tracking,
            cfg.report_grpc_status,
        ));
        Fixture {
            reg,
            conn: Arc::new(Conn {
                id: 1,
                active: AtomicUsize::new(0),
                idle_since: Mutex::new(Instant::now()),
            }),
            cache,
            cfg,
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
                Arc::clone(&self.cache),
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

        fn histogram_sample_count(&self, name: &str, route: &str) -> Option<u64> {
            for mf in self.reg.gather() {
                if mf.get_name() != name {
                    continue;
                }
                for m in mf.get_metric() {
                    let has = |k: &str, v: &str| {
                        m.get_label().iter().any(|l| l.get_name() == k && l.get_value() == v)
                    };
                    if has("method", "gRPC") && has("route", route) {
                        return Some(m.get_histogram().get_sample_count());
                    }
                }
            }
            None
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
        assert_eq!(
            fx.histogram_sample_count("cortex_request_message_bytes", route),
            Some(0)
        );
        assert_eq!(
            fx.histogram_sample_count("cortex_response_message_bytes", route),
            Some(0)
        );
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
        assert_eq!(
            fx.histogram_sample_count("cortex_response_message_bytes", route),
            Some(0)
        );
    }

    #[tokio::test]
    async fn completed_trailers_only_deadline_response_not_reset_by_tracked_body() {
        let fx = fixture(false);
        let route = "/test.Hanging/DeadlineStatus";
        let resp = fx.call(route, &[("grpc-timeout", "5m")]).await.unwrap();
        assert_eq!(drain(resp).await.as_deref(), Some("4"));
        assert_eq!(fx.duration_count(route, "DeadlineExceeded"), 1.0);
        assert_eq!(fx.gauge("cortex_inflight_requests", route), 0.0);
        assert_eq!(fx.conn.active.load(Ordering::Acquire), 0);
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
        let resp = fx.call(route, &[("x-scope-orgid", "")]).await.unwrap();
        assert_eq!(drain(resp).await.as_deref(), Some("0"));
        assert_eq!(fx.duration_count(route, "Unknown"), 2.0);
        assert!(check_org_id(&http::HeaderMap::new(), "/grpc.health.v1.Health/Check").is_none());

        let fx = fixture(false);
        let resp = fx.call(route, &[]).await.unwrap();
        assert_eq!(drain(resp).await.as_deref(), Some("0"));
    }

    #[test]
    fn classify_grpc_method_avoids_alloc_for_all_known_routes() {
        let known = [
            "/cortex.Ingester/Push",
            "/cortex.Ingester/QueryStream",
            "/cortex.Ingester/QueryExemplars",
            "/cortex.Ingester/LabelValues",
            "/cortex.Ingester/LabelNames",
            "/cortex.Ingester/UserStats",
            "/cortex.Ingester/AllUserStats",
            "/cortex.Ingester/MetricsForLabelMatchers",
            "/cortex.Ingester/MetricsMetadata",
            "/cortex.Ingester/LabelNamesAndValues",
            "/cortex.Ingester/LabelValuesCardinality",
            "/cortex.Ingester/ActiveSeries",
            "/cortex.Ingester/SearchLabelNames",
            "/cortex.Ingester/SearchLabelValues",
            "/httpgrpc.HTTP/Handle",
            "/grpc.health.v1.Health/Check",
            "/grpc.health.v1.Health/Watch",
            "/grpc.health.v1.Health/List",
        ];
        for path in known {
            let (kind, cow) = classify_grpc_method(path);
            assert!(kind.is_some(), "expected KnownGrpcMethod for {path}");
            assert!(matches!(cow, Cow::Borrowed(_)), "expected Borrowed for {path}");
            assert_eq!(cow.as_ref(), path);
        }
        let (kind, cow) = classify_grpc_method("/custom.Svc/Unknown");
        assert!(kind.is_none());
        assert!(matches!(cow, Cow::Owned(_)));
        assert_eq!(cow.as_ref(), "/custom.Svc/Unknown");
    }

    #[tokio::test]
    async fn cached_known_method_metrics_match_exact_labels_and_dont_pollute_uncalled() {
        #[derive(Clone)]
        struct EchoIngesterSvc;
        impl tonic::server::NamedService for EchoIngesterSvc {
            const NAME: &'static str = "cortex.Ingester";
        }
        impl tower::Service<http::Request<tonic::body::BoxBody>> for EchoIngesterSvc {
            type Response = http::Response<tonic::body::BoxBody>;
            type Error = std::convert::Infallible;
            type Future = Pin<
                Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>,
            >;
            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
                Box::pin(async move {
                    let _ = http_body_util::BodyExt::collect(req.into_body()).await;
                    let frame = Bytes::from_static(&[0, 0, 0, 0, 3, b'a', b'b', b'c']);
                    let body = http_body_util::Full::new(frame)
                        .map_err(|e| -> tonic::Status { match e {} });
                    let mut resp = http::Response::new(tonic::body::boxed(body));
                    resp.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        http::HeaderValue::from_static("application/grpc"),
                    );
                    Ok(resp)
                })
            }
        }

        use http_body_util::BodyExt;
        let reg = prometheus::Registry::new();
        let metrics = ServerMetrics::new(&reg, true, true).unwrap();
        let ctx = GrpcContext {
            metrics,
            logger: Arc::new(VecLogger::default()),
        };
        let cfg = GrpcServerConfig {
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
            multitenancy_enabled: false,
            no_auth_tenant: "anonymous".to_string(),
            tap_ingester: None,
        };
        let cache = Arc::new(GrpcMetricsCache::new(
            &ctx,
            cfg.stats_tracking,
            cfg.report_grpc_status,
        ));
        let conn = Arc::new(Conn {
            id: 7,
            active: AtomicUsize::new(0),
            idle_since: Mutex::new(Instant::now()),
        });
        let routes = tonic::service::Routes::new(EchoIngesterSvc).prepare();

        for _ in 0..3 {
            let req_frame = Bytes::from_static(&[0, 0, 0, 0, 2, 1, 2]);
            let req = http::Request::builder()
                .method("POST")
                .uri("/cortex.Ingester/Push")
                .header("content-type", "application/grpc")
                .body(http_body_util::Full::new(req_frame))
                .unwrap();
            let resp = handle(
                req,
                "127.0.0.1:1".parse().unwrap(),
                Arc::clone(&conn),
                routes.clone(),
                cfg.clone(),
                Arc::clone(&cache),
            )
            .await
            .unwrap();
            let _ = resp.into_body().collect().await.unwrap();
        }

        let mfs = reg.gather();
        let duration_mf = mfs
            .iter()
            .find(|mf| mf.get_name() == "cortex_request_duration_seconds")
            .unwrap();
        // Only /cortex.Ingester/Push with status OK should be registered; uncalled routes must not appear.
        assert_eq!(duration_mf.get_metric().len(), 1);
        let metric = &duration_mf.get_metric()[0];
        assert_eq!(metric.get_histogram().get_sample_count(), 3);
        let has_label = |k: &str, v: &str| {
            metric
                .get_label()
                .iter()
                .any(|l| l.get_name() == k && l.get_value() == v)
        };
        assert!(has_label("method", "gRPC"));
        assert!(has_label("route", "/cortex.Ingester/Push"));
        assert!(has_label("status_code", "OK"));
        assert!(has_label("ws", "false"));

        let req_bytes_mf = mfs
            .iter()
            .find(|mf| mf.get_name() == "cortex_request_message_bytes")
            .unwrap();
        assert_eq!(req_bytes_mf.get_metric()[0].get_histogram().get_sample_count(), 3);
        assert_eq!(req_bytes_mf.get_metric()[0].get_histogram().get_sample_sum(), 21.0);

        let resp_bytes_mf = mfs
            .iter()
            .find(|mf| mf.get_name() == "cortex_response_message_bytes")
            .unwrap();
        assert_eq!(resp_bytes_mf.get_metric()[0].get_histogram().get_sample_count(), 3);
        assert_eq!(resp_bytes_mf.get_metric()[0].get_histogram().get_sample_sum(), 24.0);
    }

    #[test]
    fn w20_sdw_11_grpc_percent_decode_rejects_plus_sign() {
        assert_eq!(percent_decode("foo%20bar"), "foo bar");
        assert_eq!(percent_decode("foo%+a%-1bar"), "foo%+a%-1bar");
    }

    #[tokio::test]
    async fn b01_pre_handler_auth_and_cluster_rejection_abort_half_open_read_cb() {
        let mut fx = fixture(true);
        let tmp = tempfile::tempdir().unwrap();
        let mut ing_cfg = mimir_config::IngesterConfig::default();
        ing_cfg.ring.instance_id = "test-b01-cmd".to_string();
        ing_cfg.read_circuit_breaker = mimir_config::CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 50,
            failure_execution_threshold: 2,
            thresholding_period: mimir_config::GoDuration::from_secs(0),
            cooldown_period: mimir_config::GoDuration::from_millis(20),
            request_timeout: mimir_config::GoDuration::from_millis(10),
            ..Default::default()
        };
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = mimir_ingester::Ingester::new(
            ing_cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let read_cb = ing.circuit_breaker.read.as_ref().unwrap();
        read_cb.activate();
        for _ in 0..2 {
            let p = read_cb.try_acquire_permit().unwrap().unwrap();
            p.finish(Duration::from_millis(25), true);
        }
        assert_eq!(read_cb.state(), mimir_ingester::circuit_breaker::BreakerState::Open);
        std::thread::sleep(Duration::from_millis(30));

        fx.cfg.tap_ingester = Some(Arc::clone(&ing));
        fx.cfg.cluster_validation = Some(GrpcClusterValidation {
            allowed: vec!["prod".to_string()],
            soft: false,
        });

        // 1) Pre-handler auth rejection (missing x-scope-orgid) -> Unknown, HalfOpen unchanged.
        let resp = fx.call("/cortex.Ingester/LabelNames", &[]).await.unwrap();
        assert_eq!(
            tonic::Status::from_header_map(resp.headers()).unwrap().code(),
            tonic::Code::Unknown
        );
        assert_eq!(
            read_cb.state(),
            mimir_ingester::circuit_breaker::BreakerState::HalfOpen
        );
        assert_eq!(read_cb.recorded_successes(), 0);

        // 2) Pre-handler cluster validation rejection -> FailedPrecondition, HalfOpen unchanged.
        let resp = fx
            .call(
                "/cortex.Ingester/LabelNames",
                &[
                    ("x-scope-orgid", "t1"),
                    ("x-cortex-cluster", "wrong-cluster"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            tonic::Status::from_header_map(resp.headers()).unwrap().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            read_cb.state(),
            mimir_ingester::circuit_breaker::BreakerState::HalfOpen
        );
        assert_eq!(read_cb.recorded_successes(), 0);
    }
}
