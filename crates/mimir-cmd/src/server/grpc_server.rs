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
use mimir_common::grpc_log::GrpcErrorLog;
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

async fn handle(
    mut req: hyper::Request<Incoming>,
    remote: SocketAddr,
    conn: Arc<Conn>,
    routes: tonic::service::Routes,
    cfg: GrpcServerConfig,
    ctx: GrpcContext,
) -> Result<GrpcResponse, std::convert::Infallible> {
    let method = req.uri().path().to_string();
    let begin = Instant::now();
    let m = &ctx.metrics;

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
    req.extensions_mut().insert(tonic::transport::server::TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(remote),
    });
    let error_log = GrpcErrorLog::default();
    req.extensions_mut().insert(error_log.clone());

    // Cluster validation (unary RPCs only, never health checks).
    let rejection = cfg.cluster_validation.as_ref().and_then(|cv| {
        if STREAMING_METHODS.contains(&method.as_str()) || method.starts_with("/grpc.health.v1.Health/") {
            return None;
        }
        check_cluster(req.headers(), &method, cv, &ctx)
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
        None => match routes.oneshot(req).await {
            Ok(r) => r,
            Err(e) => tonic::Status::internal(e.to_string()).into_http(),
        },
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

    let status: Arc<Mutex<(Option<i32>, Option<String>)>> = Arc::new(Mutex::new((header_status, header_message)));
    let sent = cfg
        .stats_tracking
        .then(|| m.sent_message_size.with_label_values(&["gRPC", &method]));
    let mut out_counter = GrpcFrameCounter::default();
    let status_f = Arc::clone(&status);
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
    let duration_vec = m.request_duration.clone();
    let report = cfg.report_grpc_status;
    let logger = Arc::clone(&ctx.logger);
    let on_done = Box::new(move |completed: bool| {
        let (code, msg) = status.lock().clone();
        // A stream torn down before its status was sent was canceled by the client.
        let code = match code {
            Some(c) => c,
            None if completed => 0,
            None => 1,
        };
        let elapsed = begin.elapsed();
        duration_vec
            .with_label_values(&["gRPC", &method, &grpc_status_label(code, report), "false"])
            .observe(elapsed.as_secs_f64());
        if let Some(g) = &inflight {
            g.dec();
        }
        if let Some(t) = &stream_tracker {
            t.close_stream(conn.id);
        }
        if conn.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            *conn.idle_since.lock() = Instant::now();
        }
        if code != 0 {
            // The handler's view of its error (Go error text, OptionalLogging), or a plain
            // `status.Error` as grpc-go would format it.
            let (err, keep) = match error_log.take() {
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
                logger.log(
                    level,
                    &[("method", &method), ("duration", &dur), ("msg", "gRPC"), ("err", &err)],
                );
            }
        }
    });
    Ok(resp.map(|b| tonic::body::boxed(TrackedBody::new(b).on_frame(on_frame).on_done(on_done))))
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
