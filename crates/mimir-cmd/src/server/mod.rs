//! dskit `server.Server`: HTTP + gRPC listeners, TLS, connection limits, timeouts, keepalive,
//! instrumentation and logging middleware, graceful shutdown.
//!
//! Ported from github.com/grafana/dskit/server/server.go (`newServer`, `Run`, `Shutdown`) as
//! configured by Mimir (pkg/mimir/mimir.go: `MetricsNamespace = "cortex"`,
//! `MetricsNativeHistogramFactor = 1.1`, `ExcludeRequestInLog = true`,
//! `DisableRequestSuccessLog = true`; pkg/mimir/modules.go `initServer`).
//!
//! [`Server::new`] mirrors `newServer`: it registers the server metrics, binds both listeners
//! (failing early if a port is in use), builds the TLS configs and the HTTP middleware, so a
//! broken server configuration fails startup before any other module starts, like Go.
//! [`Server::start`] mirrors `Server.Run`.
//!
//! Server options that cannot be honoured are rejected by [`validate_supported`] rather than
//! silently ignored (see PORT_STATUS.md).

pub mod body;
pub mod gzip;
pub mod grpc_server;
pub mod http_middleware;
pub mod http_server;
pub mod listener;
pub mod metrics;
pub mod tls;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use axum::Router;
use mimir_common::logging::{LogLevel, Logger};
use mimir_config::server::ServerConfig;
use mimir_config::GoDuration;
use prometheus::Registry;
use tokio::sync::{oneshot, watch};

use self::grpc_server::{finite, GrpcClusterValidation, GrpcContext, GrpcServerConfig};
use self::http_middleware::{HttpClusterValidation, HttpMiddlewareConfig, SourceIps};
use self::http_server::HttpServerConfig;
use self::listener::{AcceptOptions, Acceptor};
use self::metrics::ServerMetrics;
use self::tls::{server_config, tls_enabled, TlsOptions};

/// Rejects server options mimir-rs cannot honour (instead of silently ignoring them).
pub fn validate_supported(cfg: &ServerConfig) -> Result<()> {
    // grpc-go keepalive enforcement policy (EnforcementPolicy{MinTime, PermitWithoutStream}):
    // the hyper/h2 server answers every client PING and cannot enforce a ping policy. Mimir's
    // defaults (10s, true) are the values Helm/jsonnet use; only stricter or looser policies
    // would change client-observable behaviour, so any other value is refused.
    if cfg.grpc_server_min_time_between_pings != GoDuration::from_secs(10) {
        bail!(
            "-server.grpc.keepalive.min-time-between-pings={} is not supported by mimir-rs: the gRPC server cannot enforce a keepalive ping policy (only the default 10s is accepted)",
            cfg.grpc_server_min_time_between_pings
        );
    }
    if !cfg.grpc_server_ping_without_stream_allowed {
        bail!("-server.grpc.keepalive.ping-without-stream-allowed=false is not supported by mimir-rs: the gRPC server cannot enforce a keepalive ping policy");
    }
    Ok(())
}

fn opt_duration(d: GoDuration) -> Duration {
    d.as_std().unwrap_or_default()
}

/// dskit `ClusterValidationConfig.GetAllowedClusterLabels`.
fn allowed_cluster_labels(cfg: &ServerConfig) -> Vec<String> {
    let cv = &cfg.cluster_validation;
    let mut v = Vec::with_capacity(1 + cv.additional_labels.0.len());
    if !cv.label.is_empty() {
        v.push(cv.label.clone());
    }
    v.extend(cv.additional_labels.0.iter().cloned());
    v
}

/// A configured, bound, not yet serving dskit server (Go `server.New`).
pub struct Server {
    http_listener: std::net::TcpListener,
    grpc_listener: std::net::TcpListener,
    http_cfg: HttpServerConfig,
    grpc_cfg: GrpcServerConfig,
    http_accept: AcceptOptions,
    grpc_accept: AcceptOptions,
    middleware: Arc<HttpMiddlewareConfig>,
    grpc_ctx: GrpcContext,
}

impl Server {
    /// Go `server.NewServerMetrics` + `server.NewWithMetrics`.
    pub fn new(cfg: &ServerConfig, registry: &Registry, logger: Arc<dyn Logger>) -> Result<Self> {
        validate_supported(cfg)?;

        let metrics = ServerMetrics::new(
            registry,
            cfg.grpc_server_stats_tracking_enabled,
            cfg.grpc_collect_max_streams_by_conn,
        )
        .context("registering server metrics")?;

        let http_listener =
            listener::listen(&cfg.http_listen_network, &cfg.http_listen_address, cfg.http_listen_port)?;
        metrics
            .tcp_connections_limit
            .with_label_values(&["http"])
            .set(cfg.http_listen_conn_limit as f64);
        let grpc_listener =
            listener::listen(&cfg.grpc_listen_network, &cfg.grpc_listen_address, cfg.grpc_listen_port)?;
        metrics
            .tcp_connections_limit
            .with_label_values(&["grpc"])
            .set(cfg.grpc_listen_conn_limit as f64);

        let tls_opts = TlsOptions {
            cipher_suites: cfg.tls_cipher_suites.clone(),
            min_version: cfg.tls_min_version.clone(),
            curve_preferences: cfg.tls_curve_preferences.clone(),
        };
        tls_opts.validate()?;
        let http_tls = if tls_enabled(&cfg.http_tls_config) {
            Some(
                server_config(&cfg.http_tls_config, &tls_opts, &[b"h2", b"http/1.1"])
                    .map_err(|e| anyhow!("error generating http tls config: {e}"))?,
            )
        } else {
            None
        };
        let grpc_tls = if tls_enabled(&cfg.grpc_tls_config) {
            Some(
                server_config(&cfg.grpc_tls_config, &tls_opts, &[b"h2"])
                    .map_err(|e| anyhow!("error generating grpc tls config: {e}"))?,
            )
        } else {
            None
        };

        let http_addr = http_listener.local_addr()?.to_string();
        let grpc_addr = grpc_listener.local_addr()?.to_string();
        logger.log(
            LogLevel::Info,
            &[("msg", "server listening on addresses"), ("http", &http_addr), ("grpc", &grpc_addr)],
        );

        let allowed_clusters = allowed_cluster_labels(cfg);
        let grpc_cv = if cfg.cluster_validation.grpc.enabled {
            if allowed_clusters.is_empty() {
                bail!("no cluster labels provided");
            }
            Some(GrpcClusterValidation {
                allowed: allowed_clusters.clone(),
                soft: cfg.cluster_validation.grpc.soft_validation,
            })
        } else {
            None
        };

        if cfg.grpc_server_recv_buffer_pools_enabled {
            logger.log(
                LogLevel::Warn,
                &[(
                    "msg",
                    "'server.grpc.recv-buffer-pools-enabled' is a deprecated option that currently has no effect and will be removed in a future version",
                )],
            );
        }

        // BuildHTTPMiddleware.
        let source_ips = SourceIps::new(&cfg.log_source_ips_header, &cfg.log_source_ips_regex, cfg.log_source_ips_full)
            .map_err(|e| anyhow!("error building http middleware: error setting up source IP extraction: {e}"))?;
        let http_cv = if cfg.cluster_validation.http.protocol.enabled {
            Some(
                HttpClusterValidation::new(
                    allowed_clusters,
                    cfg.cluster_validation.http.protocol.soft_validation,
                    &cfg.cluster_validation.http.excluded_paths.0,
                    &cfg.cluster_validation.http.excluded_user_agents.0,
                )
                .map_err(|e| anyhow!("error building http middleware: {e}"))?,
            )
        } else {
            None
        };
        let middleware = Arc::new(HttpMiddlewareConfig {
            metrics: metrics.clone(),
            logger: Arc::clone(&logger),
            log_request_headers: cfg.log_request_headers,
            log_request_at_info_level: cfg.log_request_at_info_level_enabled,
            log_source_ips: cfg.log_source_ips_enabled.then_some(source_ips.clone()),
            trace_source_ips: Some(source_ips),
            exclude_headers: cfg
                .log_request_exclude_headers_list
                .split(',')
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            trace_request_headers: cfg.trace_request_headers,
            trace_exclude_headers: cfg
                .trace_request_exclude_headers_list
                .split(',')
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            create_new_traces: cfg.create_new_traces,
            cluster_validation: http_cv,
        });

        let http_cfg = HttpServerConfig {
            read_timeout: opt_duration(cfg.http_server_read_timeout),
            read_header_timeout: opt_duration(cfg.http_server_read_header_timeout),
            write_timeout: opt_duration(cfg.http_server_write_timeout),
            idle_timeout: opt_duration(cfg.http_server_idle_timeout),
            tls: http_tls,
            graceful_shutdown_timeout: opt_duration(cfg.graceful_shutdown_timeout),
            zero_response_logger: cfg
                .http_log_closed_connections_without_response_enabled
                .then(|| Arc::clone(&logger)),
        };
        let grpc_cfg = GrpcServerConfig {
            max_concurrent_streams: u32::try_from(cfg.grpc_server_max_concurrent_streams).unwrap_or(u32::MAX),
            // grpc-go enforces a 1s minimum keepalive time; "infinity" disables pings.
            keepalive_time: finite(cfg.grpc_server_keepalive_time).map(|d| d.max(Duration::from_secs(1))),
            keepalive_timeout: finite(cfg.grpc_server_keepalive_timeout).unwrap_or(Duration::from_secs(20)),
            max_connection_idle: finite(cfg.grpc_server_max_connection_idle),
            max_connection_age: finite(cfg.grpc_server_max_connection_age),
            max_connection_age_grace: finite(cfg.grpc_server_max_connection_age_grace),
            tls: grpc_tls,
            stats_tracking: cfg.grpc_server_stats_tracking_enabled,
            report_grpc_status: cfg.report_grpc_codes_in_instrumentation_label_enabled,
            cluster_validation: grpc_cv,
            multitenancy_enabled: false,
            no_auth_tenant: "anonymous".to_string(),
        };

        let header_timeout = opt_duration(cfg.http_server_read_header_timeout);
        let http_accept = AcceptOptions {
            conn_limit: usize::try_from(cfg.http_listen_conn_limit).unwrap_or(0),
            connections_gauge: metrics.tcp_connections.with_label_values(&["http"]),
            proxy_protocol: cfg.proxy_protocol_enabled,
            proxy_header_timeout: header_timeout,
        };
        let grpc_accept = AcceptOptions {
            conn_limit: usize::try_from(cfg.grpc_listen_conn_limit).unwrap_or(0),
            connections_gauge: metrics.tcp_connections.with_label_values(&["grpc"]),
            proxy_protocol: cfg.proxy_protocol_enabled,
            proxy_header_timeout: header_timeout,
        };

        Ok(Self {
            http_listener,
            grpc_listener,
            http_cfg,
            grpc_cfg,
            http_accept,
            grpc_accept,
            middleware,
            grpc_ctx: GrpcContext { metrics, logger },
        })
    }

    pub fn http_addr(&self) -> std::io::Result<SocketAddr> {
        self.http_listener.local_addr()
    }

    pub fn grpc_addr(&self) -> std::io::Result<SocketAddr> {
        self.grpc_listener.local_addr()
    }

    /// Mimir's `noauth.SetupAuthMiddleware` gRPC interceptors: with multitenancy enabled every
    /// RPC outside `noGRPCAuthOn` must carry exactly one `x-scope-orgid`.
    pub fn set_grpc_multitenancy(&mut self, enabled: bool, no_auth_tenant: impl Into<String>) {
        self.grpc_cfg.multitenancy_enabled = enabled;
        self.grpc_cfg.no_auth_tenant = no_auth_tenant.into();
    }

    /// Go `Server.Run`: serves `router` (wrapped in the dskit HTTP middleware) and the gRPC
    /// `routes` on a dedicated runtime, keeping network I/O apart from TSDB work.
    pub fn start(self, router: Router, grpc_routes: tonic::service::Routes) -> Result<RunningServers> {
        let Self {
            http_listener,
            grpc_listener,
            http_cfg,
            grpc_cfg,
            http_accept,
            grpc_accept,
            middleware,
            grpc_ctx,
        } = self;
        let http_router =
            router.layer(axum::middleware::from_fn_with_state(middleware, http_middleware::dskit_http_middleware));

        let rpc_workers = std::thread::available_parallelism().map_or(4, |n| n.get().clamp(2, 16));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(rpc_workers)
            .thread_name("mimir-rpc")
            .enable_all()
            .build()
            .context("building mimir-rpc runtime")?;
        http_listener.set_nonblocking(true)?;
        grpc_listener.set_nonblocking(true)?;
        let (http_l, grpc_l) = {
            // `from_std` registers the sockets with the runtime's reactor.
            let _enter = rt.enter();
            (
                tokio::net::TcpListener::from_std(http_listener).context("registering HTTP listener")?,
                tokio::net::TcpListener::from_std(grpc_listener).context("registering gRPC listener")?,
            )
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (done_tx, done_rx) = oneshot::channel::<()>();
        std::thread::Builder::new()
            .name("mimir-rpc-rt".to_string())
            .spawn(move || {
                rt.block_on(async move {
                    let http = tokio::spawn(http_server::serve(
                        Acceptor::new(http_l, http_accept),
                        http_cfg,
                        http_router,
                        shutdown_rx.clone(),
                    ));
                    let grpc = tokio::spawn(grpc_server::serve(
                        Acceptor::new(grpc_l, grpc_accept),
                        grpc_cfg,
                        grpc_routes,
                        grpc_ctx,
                        shutdown_rx,
                    ));
                    // dskit Shutdown: HTTP first (bounded), then gRPC GracefulStop.
                    let _ = http.await;
                    let _ = grpc.await;
                });
                let _ = done_tx.send(());
            })
            .context("spawning mimir-rpc runtime thread")?;

        Ok(RunningServers { shutdown_tx, done_rx })
    }
}

/// Handle to running servers.
pub struct RunningServers {
    shutdown_tx: watch::Sender<bool>,
    done_rx: oneshot::Receiver<()>,
}

impl RunningServers {
    /// dskit `Server.Shutdown`: HTTP shuts down gracefully within its timeout, gRPC stops
    /// gracefully (waiting for in-flight RPCs).
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.done_rx.await;
    }
}

/// Resolves once the shutdown flag is set (never holds the watch guard across an await).
pub(crate) async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|v| *v).await;
}
