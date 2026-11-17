//! Application lifecycle, module initialization, HTTP + gRPC servers, and graceful shutdown.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/mimir/mimir.go
//! - github.com/grafana/mimir/pkg/mimir/modules.go
//! - github.com/grafana/dskit/server/server.go

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use mimir_common::activitytracker::{
    load_unfinished_entries, ActivityTracker, ActivityTrackerConfig,
};
use mimir_common::logging::{
    format_rfc3339_nano_utc, global_logger, init_global_logger, LogFormat, LogLevel, LogSink,
    Logger,
};
use mimir_common::services::{FnListener, ManagedBasicService, Service, ServiceContext, State};
use mimir_config::ingester::KVStoreConfig;
use mimir_config::{MimirConfig, Overrides, RuntimeConfigManager};
use mimir_ingester::ingest_storage::{
    ingester_partition_id_from_instance_id, IngesterPartitionRingDelegate,
};
use mimir_ingester::{Ingester, IngesterLifecycler, IngesterService, TokenlessLifecycler};
use mimir_proto::httpgrpcpb::http_server::HttpServer;
use mimir_ring::kv::ha_replica::replica_desc_codec;
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv};
use mimir_ring::kv::{build_kv_client, registerer_with_kv_name, Codec, Registerer};
use mimir_ring::lifecycler::{BasicLifecyclerConfig, Lifecycler};
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, PartitionInstanceLifecycler, PartitionInstanceLifecyclerConfig,
    PartitionRingOptions, PartitionRingWatcher,
};
use mimir_ring::ring::model::ring_desc_codec;
use mimir_ring::ring::Ring;
use mimir_ring::token_gen::{PartitionTokenGenerator, PartitionTokenTable};
use mimir_storage::bucket::BucketClient;
use prometheus::Registry;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codec::CompressionEncoding;

use crate::cli::CliOutcome;
use crate::grpc::{HealthService, HttpGrpcBridge, HttpGrpcStatusFixService};
use crate::http::{build_http_router, AppHttpState};
use crate::metrics::{register_global_metrics, version_info};
use crate::modules::{ModuleStates, StateCell, StateFn};
use crate::sanity_check;

const INGEST_RING_NAME: &str = "ingester";
const INGEST_RING_KEY: &str = "ring";
const PARTITION_RING_NAME: &str = "ingester-partitions";
const PARTITION_RING_KEY: &str = "ingester-partitions";

fn kv_uses_memberlist(kv: &KVStoreConfig) -> bool {
    kv.store == "memberlist"
        || (kv.store == "multi"
            && (kv.multi.primary == "memberlist" || kv.multi.secondary == "memberlist"))
}

fn convert_log_level(lvl: mimir_config::LogLevel) -> LogLevel {
    match lvl {
        mimir_config::LogLevel::Debug => LogLevel::Debug,
        mimir_config::LogLevel::Info => LogLevel::Info,
        mimir_config::LogLevel::Warn => LogLevel::Warn,
        mimir_config::LogLevel::Error => LogLevel::Error,
    }
}

/// Builds the process logger like `util_log.InitLogger(cfg.Server.LogFormat, cfg.Server.LogLevel,
/// ..., RateLimitedLoggerCfg{...})` in `cmd/mimir/main.go`.
fn build_log_sink(outcome: &CliOutcome) -> LogSink {
    let (format, level) = match outcome.config.as_ref() {
        Some(cfg) => (
            LogFormat::parse(&cfg.server.log_format),
            convert_log_level(cfg.server.log_level),
        ),
        None => (LogFormat::Logfmt, LogLevel::Info),
    };
    let sink = LogSink::new(format, level, Box::new(std::io::stderr()));
    if outcome.rate_limited_logs_enabled {
        sink.with_rate_limit(
            outcome.rate_limited_logs_per_second,
            outcome.rate_limited_logs_burst_size.max(0) as usize,
        )
    } else {
        sink
    }
}

/// Installs the process-wide logger and the `tracing` subscriber that renders every `tracing`
/// event through it (Go `util_log.InitLogger`). Must be called once, before `run_mimir`.
pub fn init_logging(outcome: &CliOutcome) -> Arc<LogSink> {
    init_global_logger(build_log_sink(outcome))
}

fn validate_supported_targets(cfg: &MimirConfig) -> Result<()> {
    for target in &cfg.target.0 {
        match target.as_str() {
            "ingester" | "memberlist-kv" | "all" => {}
            other => {
                bail!(
                    "target module {other:?} is not yet implemented in mimir-rs; supported targets: ingester, memberlist-kv, all"
                );
            }
        }
    }
    Ok(())
}

fn resolve_bind_addr(addr: &str, port: i64) -> Result<SocketAddr> {
    let host = if addr.is_empty() { "0.0.0.0" } else { addr };
    let ip: IpAddr = host
        .parse()
        .with_context(|| format!("invalid listen IP address {host:?}"))?;
    let port_u16 = u16::try_from(port)
        .with_context(|| format!("invalid listen port {port}"))?;
    Ok(SocketAddr::new(ip, port_u16))
}

/// Runs the configured Mimir application until SIGTERM/SIGINT or `/ingester/shutdown`.
pub async fn run_mimir(outcome: CliOutcome) -> Result<()> {
    let mut cfg = outcome
        .config
        .clone()
        .context("missing parsed MimirConfig")?;

    validate_supported_targets(&cfg)?;

    let _ballast = if outcome.ballast_bytes > 0 {
        vec![0u8; outcome.ballast_bytes as usize]
    } else {
        Vec::new()
    };

    let registry = Registry::new();
    // The binary installs the global logger in main() (init_logging); in-process callers that
    // did not install one get a private stderr logger with the same settings.
    let logger = global_logger().unwrap_or_else(|| Arc::new(build_log_sink(&outcome)));
    logger.register_metrics(&registry);

    register_global_metrics(&registry, outcome.config_hash_sha256.as_deref())
        .context("registering global Prometheus metrics")?;

    let version = version_info();
    logger.log(
        LogLevel::Info,
        &[("msg", "Starting application"), ("version", &version)],
    );

    // 1. Activity tracker (matching initActivityTracker in pkg/mimir/modules.go).
    let activity_tracker: Option<Arc<ActivityTracker>> = if !cfg.activity_tracker.filepath.is_empty()
    {
        match load_unfinished_entries(&cfg.activity_tracker.filepath) {
            Ok(entries) => {
                if !entries.is_empty() {
                    let count_str = entries.len().to_string();
                    logger.log(
                        LogLevel::Warn,
                        &[
                            ("component", "activity-tracker"),
                            ("msg", "found unfinished activities from previous run"),
                            ("count", &count_str),
                        ],
                    );
                    for e in &entries {
                        let ts = format_rfc3339_nano_utc(e.timestamp);
                        logger.log(
                            LogLevel::Warn,
                            &[
                                ("component", "activity-tracker"),
                                ("start", &ts),
                                ("activity", &e.activity),
                            ],
                        );
                    }
                }
            }
            Err(err) => {
                let err_s = err.to_string();
                logger.log(
                    LogLevel::Warn,
                    &[
                        ("component", "activity-tracker"),
                        ("msg", "failed to fully read file with unfinished activities"),
                        ("err", &err_s),
                    ],
                );
            }
        }

        let tracker = ActivityTracker::new(
            ActivityTrackerConfig {
                filepath: PathBuf::from(&cfg.activity_tracker.filepath),
                max_entries: cfg.activity_tracker.max_entries.max(1) as usize,
            },
            Some(&registry),
        )
        .context("initializing activity tracker")?;
        tracker.map(Arc::new)
    } else {
        None
    };

    // 2. RuntimeConfigManager & Overrides.
    let (runtime_config_mgr, overrides) = if !cfg.runtime_config.file.0.is_empty() {
        let mgr = RuntimeConfigManager::new(
            cfg.runtime_config.clone(),
            cfg.limits.clone(),
            cfg.ingester.instance_limits.clone(),
        )
        .context("initializing runtime config manager")?;
        let _reloader = mgr.start_reloader();
        let ov = mgr.overrides();
        (Some(mgr), ov)
    } else {
        (None, Overrides::new(cfg.limits.clone()))
    };

    // 3. Bind HTTP & gRPC listeners early so port 0 resolves before ring registration.
    let http_addr = resolve_bind_addr(
        &cfg.server.http_listen_address,
        cfg.server.http_listen_port,
    )?;
    let grpc_addr = resolve_bind_addr(
        &cfg.server.grpc_listen_address,
        cfg.server.grpc_listen_port,
    )?;
    let http_listener = TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("binding HTTP listener on {http_addr}"))?;
    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("binding gRPC listener on {grpc_addr}"))?;

    let bound_http_addr = http_listener.local_addr()?;
    let bound_grpc_addr = grpc_listener.local_addr()?;
    cfg.server.http_listen_port = bound_http_addr.port() as i64;
    cfg.server.grpc_listen_port = bound_grpc_addr.port() as i64;

    let http_addr_s = bound_http_addr.to_string();
    let grpc_addr_s = bound_grpc_addr.to_string();
    logger.log(
        LogLevel::Info,
        &[
            ("msg", "server listening on addresses"),
            ("http", &http_addr_s),
            ("grpc", &grpc_addr_s),
        ],
    );

    let ingester_enabled = cfg.is_ingester_enabled();
    let service_ctx = ServiceContext::background();

    // Module services of the configured targets (Go `Mimir.ServiceMap`), used by /ready,
    // /services, the gRPC health check and failure propagation.
    let modules = ModuleStates::new();
    let server_state = StateCell::new(State::New);
    modules.add("server", server_state.state_fn());

    // Install SIGTERM/SIGINT listeners before any module starts: Go's signal handler runs for
    // the whole lifetime of the service manager, including module startup.
    let mut shutdown_sig = install_shutdown_signal(Arc::clone(&logger));
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_delay = Duration::from_nanos(cfg.shutdown_delay.0.max(0) as u64);

    // Anonymous usage statistics (`initUsageStats`), only for components accessing the blocks
    // storage. `Server` depends on `UsageStats` in Go.
    mimir_common::usagestats::set_target(&cfg.target.0.join(","));
    if cfg.ingest_storage.enabled && ingester_enabled {
        mimir_common::usagestats::set_mode(mimir_common::usagestats::MODE_INGEST_STORAGE);
    }
    let usage_stats_svc = if cfg.usage_stats.enabled && ingester_enabled {
        // bucket.NewClient(ctx, cfg.BlocksStorage.Bucket, UsageStats, logger, t.Registerer)
        let bucket =
            BucketClient::new_client(&cfg.blocks_storage.bucket, "usage-stats", Some(&registry))
                .context("create usage-stats bucket client")?;
        mimir_common::usagestats::get_string("blocks_storage_backend")
            .set(&cfg.blocks_storage.bucket.backend_cfg.backend);
        mimir_common::usagestats::get_string("installation_mode")
            .set(&cfg.usage_stats.installation_mode);
        let svc = crate::usagestats::Reporter::new(
            crate::usagestats::ReporterConfig::default(),
            &bucket,
            Some(&registry),
        )
        .into_service();
        log_module_starting(&*logger, "usage-stats");
        svc.start_async(service_ctx.child())
            .await
            .context("starting module usage-stats")?;
        modules.add("usage-stats", service_state_fn(&svc));
        Some(svc)
    } else {
        None
    };

    // `Server` depends on `SanityCheck`: nothing is served and no other module starts until
    // the directories and object storage checks pass.
    let sanity_state = StateCell::new(State::Starting);
    modules.add("sanity-check", sanity_state.state_fn());
    log_module_starting(&*logger, "sanity-check");
    if ingester_enabled {
        let sanity_err = tokio::select! {
            res = sanity_check::run_sanity_check(&cfg, logger.as_ref()) => {
                res.err().map(|err| (false, format!("starting module sanity-check: {err}")))
            }
            _ = &mut shutdown_sig => {
                request_shutdown(&shutdown_requested, shutdown_delay).await;
                Some((true, "starting module sanity-check: context canceled".to_string()))
            }
        };
        if let Some((signaled, err_s)) = sanity_err {
            sanity_state.set(State::Failed);
            if signaled {
                logger.log(
                    LogLevel::Info,
                    &[
                        ("msg", "module startup aborted by shutdown signal"),
                        ("module", "sanity-check"),
                        ("err", &err_s),
                    ],
                );
            } else {
                logger.log(
                    LogLevel::Error,
                    &[("msg", "module failed"), ("module", "sanity-check"), ("err", &err_s)],
                );
            }
            // The service manager stops every other (already started) module.
            if let Some(svc) = usage_stats_svc {
                svc.stop_async();
                let _ = svc.await_terminated().await;
                log_module_stopped(&*logger, "usage-stats");
            }
            logger.log(LogLevel::Info, &[("msg", "Application stopped")]);
            if signaled {
                return Ok(());
            }
            bail!("failed services");
        }
    }
    sanity_state.set(State::Running);

    // 4. Memberlist KV (if required by ring/partition_ring or explicitly targeted).
    let needs_memberlist = cfg.is_module_explicitly_targeted("memberlist-kv")
        || (ingester_enabled
            && (kv_uses_memberlist(&cfg.ingester.ring.kvstore)
                || (cfg.ingest_storage.enabled
                    && kv_uses_memberlist(&cfg.ingester.partition_ring.kvstore))));

    // Created here, started (dskit `starting`: transport + fast-join) only after the HTTP/gRPC
    // servers are serving, like the Go module graph where memberlist-kv starts alongside server.
    // Mimir: MetricsNamespace "" and codecs ring, partition ring, HA tracker replica desc.
    let memberlist_kv = if needs_memberlist {
        let codecs: Vec<Arc<dyn Codec>> = vec![
            ring_desc_codec(),
            partition_ring_desc_codec(),
            replica_desc_codec(),
        ];
        let ml = MemberlistKv::new(
            cfg.memberlist.clone(),
            codecs,
            "",
            Some(&registry),
            KvOptions::default(),
        )
        .context("creating memberlist KV")?;
        Some(ml)
    } else {
        None
    };
    // Go always runs the `memberlist-kv` module for the ingester (`memberlist.KVInitService`,
    // which creates the memberlist client lazily): its state is the memberlist service's one when
    // memberlist is used, otherwise it is an idle service.
    let memberlist_module = ingester_enabled || memberlist_kv.is_some();
    let memberlist_state = StateCell::new(State::New);
    let memberlist_svc = memberlist_kv.as_ref().map(|ml| ml.service());
    if memberlist_module {
        match memberlist_svc {
            Some(ref svc) => modules.add("memberlist-kv", service_state_fn(svc)),
            None => modules.add("memberlist-kv", memberlist_state.state_fn()),
        }
    }
    // Go: dskit ring, lifecycler and partition ring metrics and the ingester ring/lifecycler KV
    // clients use `prometheus.WrapRegistererWithPrefix("cortex_", t.Registerer)`; the partition
    // ring watcher/lifecycler KV clients the unprefixed registerer (kv_request_duration_seconds).
    let base_registerer = Registerer::new(&registry);
    let cortex_registerer = base_registerer.wrap_with_prefix("cortex_");

    // 5. The blocks storage bucket client is created by the ingester (Go `newIngester`); set the
    // `thanos-<component>/<version> (<runtime>)` part of its S3 User-Agent.
    mimir_storage::bucket::s3::set_app_version(
        crate::metrics::version_str(),
        &crate::metrics::go_version_str(),
    );

    // 6. Ring, PartitionRingWatcher, Lifecycler, PartitionInstanceLifecycler, and Ingester.
    let mut ring_svc = None;
    let mut watcher_svc = None;

    let ingester_opt: Option<Arc<Ingester>> = if ingester_enabled {
        let instance_ring = Ring::new(
            &cfg.ingester.ring,
            INGEST_RING_NAME,
            INGEST_RING_KEY,
            memberlist_kv.clone(),
            Some(&cortex_registerer),
        )
        .context("creating ingester ring")?;

        let rsvc = Arc::clone(&instance_ring).into_service();
        modules.add("ingester-ring", service_state_fn(&rsvc));
        ring_svc = Some(rsvc);

        // In tokenless mode (num_tokens = 0) the ingester registers through a BasicLifecycler
        // without tokens, otherwise through the classic Lifecycler (`ingester.New`).
        let lifecycler: Arc<dyn IngesterLifecycler> = if cfg.ingester.ring.num_tokens == 0 {
            if !cfg.ingest_storage.enabled {
                bail!("ring tokens can only be disabled when ingest storage is enabled");
            }
            let ring_kv = build_kv_client(
                &cfg.ingester.ring.kvstore,
                ring_desc_codec(),
                memberlist_kv.clone(),
                registerer_with_kv_name(
                    Some(&cortex_registerer),
                    &format!("{INGEST_RING_NAME}-lifecycler"),
                )
                .as_ref(),
            )
            .context("creating KV store for ingester ring in tokenless mode")?;
            let lifecycler_cfg = BasicLifecyclerConfig::from_ring_config_tokenless(
                &cfg.ingester.ring,
                bound_grpc_addr.port(),
            )
            .context("creating basic lifecycler config for ingester ring in tokenless mode")?;
            TokenlessLifecycler::new(
                lifecycler_cfg,
                INGEST_RING_NAME,
                INGEST_RING_KEY,
                ring_kv,
                cfg.ingester.ring.min_ready_duration.as_std().unwrap_or(Duration::ZERO),
                cfg.ingester.ring.final_sleep.as_std().unwrap_or(Duration::ZERO),
                cfg.blocks_storage.tsdb.flush_blocks_on_shutdown,
                None,
                Some(&cortex_registerer),
            )
        } else {
            Lifecycler::new(
                &cfg.ingester.ring,
                bound_grpc_addr.port(),
                None,
                INGEST_RING_NAME,
                INGEST_RING_KEY,
                cfg.blocks_storage.tsdb.flush_blocks_on_shutdown,
                memberlist_kv.clone(),
                Some(&cortex_registerer),
            )
            .context("creating ingester lifecycler")?
        };

        let (partition_ring_watcher, partition_lifecycler) = if cfg.ingest_storage.enabled {
            let watcher_kv = build_kv_client(
                &cfg.ingester.partition_ring.kvstore,
                partition_ring_desc_codec(),
                memberlist_kv.clone(),
                registerer_with_kv_name(
                    Some(&base_registerer),
                    &format!("{PARTITION_RING_NAME}-watcher"),
                )
                .as_ref(),
            )
            .context("creating KV store for ingester partitions ring watcher")?;

            let token_table = PartitionTokenTable::new(
                cfg.ingester.partition_ring.max_derived_token_partitions as i32,
                Some(&cortex_registerer),
            )
            .context("creating ingester partition token table")?;

            let watcher = PartitionRingWatcher::new(
                PARTITION_RING_NAME,
                PARTITION_RING_KEY,
                watcher_kv,
                PartitionRingOptions {
                    token_generator: Some(Arc::new(token_table) as Arc<dyn PartitionTokenGenerator>),
                    shuffle_shard_cache_size: 0,
                },
                Some(&cortex_registerer),
            );
            let wsvc = Arc::clone(&watcher).into_service();
            modules.add("ingester-partitions-ring", service_state_fn(&wsvc));
            watcher_svc = Some(wsvc);

            let partition_id =
                ingester_partition_id_from_instance_id(&cfg.ingester.ring.instance_id)
                    .map_err(|e| anyhow::anyhow!("calculating ingester partition ID: {e}"))?;

            let plc_kv = build_kv_client(
                &cfg.ingester.partition_ring.kvstore,
                partition_ring_desc_codec(),
                memberlist_kv.clone(),
                registerer_with_kv_name(
                    Some(&base_registerer),
                    &format!("{PARTITION_RING_NAME}-lifecycler"),
                )
                .as_ref(),
            )
            .context("creating KV store for ingester partition ring")?;

            let plc_cfg = PartitionInstanceLifecyclerConfig {
                partition_id,
                create_partitions_with_derived_tokens: cfg
                    .ingester
                    .partition_ring
                    .create_partitions_with_derived_tokens,
                max_derived_token_partitions: cfg
                    .ingester
                    .partition_ring
                    .max_derived_token_partitions
                    as i32,
                instance_id: cfg.ingester.ring.instance_id.clone(),
                multi_partition_ownership: false,
                wait_owners_count_on_pending: cfg
                    .ingester
                    .partition_ring
                    .min_partition_owners_count
                    .max(0) as usize,
                wait_owners_duration_on_pending: cfg
                    .ingester
                    .partition_ring
                    .min_partition_owners_duration
                    .as_std()
                    .unwrap_or(Duration::from_secs(10)),
                delete_inactive_partition_after: cfg
                    .ingester
                    .partition_ring
                    .delete_inactive_partition_after
                    .as_std()
                    .unwrap_or(Duration::from_secs(13 * 3600)),
                polling_interval: Duration::from_secs(5),
            };
            let plc = PartitionInstanceLifecycler::new(
                plc_cfg,
                PARTITION_RING_NAME,
                PARTITION_RING_KEY,
                plc_kv,
                Some(&cortex_registerer),
            )
            .context("creating ingester partition lifecycler")?;

            // The partition instance lifecycler is a subservice of the ingester: it is started
            // only once the ingester is ACTIVE in the ring (after WAL replay, partition catch-up
            // and the shutdown marker check), and stopped before the ring lifecycler.
            (Some(watcher), Some(plc))
        } else {
            (None, None)
        };

        let ingester = Ingester::new_with_ingester_lifecycler(
            cfg.ingester.clone(),
            cfg.blocks_storage.clone(),
            cfg.ingest_storage.clone(),
            overrides.clone(),
            Some(instance_ring),
            Some(lifecycler),
            partition_ring_watcher.clone(),
            partition_lifecycler,
            runtime_config_mgr.clone(),
            Some(&registry),
        )
        .context("creating ingester")?;
        ingester.set_multitenancy(cfg.multitenancy_enabled, &cfg.no_auth_tenant);

        if let Some(ref watcher) = partition_ring_watcher {
            watcher.with_delegate(IngesterPartitionRingDelegate::new(Arc::clone(&ingester)));
        }

        {
            let ing = Arc::clone(&ingester);
            modules.add("ingester-service", Arc::new(move || ing.state()));
        }

        Some(ingester)
    } else {
        None
    };

    let runtime_config_state = StateCell::new(State::Running);
    if runtime_config_mgr.is_some() {
        modules.add("runtime-config", runtime_config_state.state_fn());
    }

    let config_arc = Arc::new(cfg.clone());
    let default_config_arc = Arc::new(MimirConfig::default());

    let http_router = build_http_router(AppHttpState {
        config: Arc::clone(&config_arc),
        default_config: default_config_arc,
        registry: registry.clone(),
        shutdown_requested: Arc::clone(&shutdown_requested),
        ingester: ingester_opt.clone(),
        runtime_config: runtime_config_mgr.clone(),
        overrides,
        memberlist_kv: memberlist_kv.clone(),
        modules: Arc::clone(&modules),
    });

    // 7. Start HTTP & gRPC servers BEFORE awaiting ingester.start() so /metrics and /ready (503 -> 200)
    // are live during WAL replay and Kafka catch-up.
    let max_recv = (cfg.server.grpc_server_max_recv_msg_size.max(1024)) as usize;
    let max_send = (cfg.server.grpc_server_max_send_msg_size.max(1024)) as usize;

    let http_grpc_svc = HttpGrpcStatusFixService::new(
        HttpServer::new(HttpGrpcBridge::new(http_router.clone()))
            .max_decoding_message_size(max_recv)
            .max_encoding_message_size(max_send)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd),
    );

    let health_svc = HealthService::new(Arc::clone(&shutdown_requested), Arc::clone(&modules));

    let mut grpc_builder = tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .add_service(health_svc)
        .add_service(http_grpc_svc);

    if let Some(ref ing) = ingester_opt {
        let ing_svc = IngesterService::new(Arc::clone(ing))
            .into_server()
            .max_decoding_message_size(max_recv)
            .max_encoding_message_size(max_send)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);
        grpc_builder = grpc_builder.add_service(ing_svc);
    }

    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel::<()>();
    let (grpc_shutdown_tx, grpc_shutdown_rx) = oneshot::channel::<()>();
    let (server_done_tx, server_done_rx) = oneshot::channel::<()>();

    let http_std = http_listener.into_std()?;
    let grpc_std = grpc_listener.into_std()?;
    http_std.set_nonblocking(true)?;
    grpc_std.set_nonblocking(true)?;

    log_module_starting(&*logger, "server");

    std::thread::Builder::new()
        .name("mimir-rpc-rt".to_string())
        .spawn(move || {
            let rpc_workers =
                std::thread::available_parallelism().map_or(4, |n| n.get().clamp(4, 16));
            let rpc_rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(rpc_workers)
                .thread_name("mimir-rpc")
                .enable_all()
                .build()
                .expect("build mimir-rpc runtime");
            rpc_rt.block_on(async move {
                let http_l = TcpListener::from_std(http_std).expect("from_std http listener");
                let grpc_l = TcpListener::from_std(grpc_std).expect("from_std grpc listener");

                let http_task = tokio::spawn(async move {
                    axum::serve(http_l, http_router)
                        .with_graceful_shutdown(async {
                            let _ = http_shutdown_rx.await;
                        })
                        .await
                });

                let incoming =
                    tokio_stream::StreamExt::map(TcpListenerStream::new(grpc_l), |res| {
                        if let Ok(ref stream) = res {
                            let _ = stream.set_nodelay(true);
                        }
                        res
                    });

                let grpc_task = tokio::spawn(async move {
                    grpc_builder
                        .serve_with_incoming_shutdown(incoming, async {
                            let _ = grpc_shutdown_rx.await;
                        })
                        .await
                });

                let _ = tokio::join!(http_task, grpc_task);
            });
            let _ = server_done_tx.send(());
        })
        .context("spawning mimir-rpc runtime thread")?;
    server_state.set(State::Running);

    // 8. Start ring, partition ring, and ingester services while honoring SIGTERM/SIGINT cancellation.
    // Failures of module services (after they started) are reported here, like the service
    // manager's `serviceFailed` listener in Go, which stops the whole application.
    let (module_failed_tx, mut module_failed_rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let mut listener_handles = Vec::new();
    for (name, svc) in [
        ("memberlist-kv", &memberlist_svc),
        ("ingester-ring", &ring_svc),
        ("ingester-partitions-ring", &watcher_svc),
        ("usage-stats", &usage_stats_svc),
    ] {
        if let Some(svc) = svc {
            let tx = module_failed_tx.clone();
            let name = name.to_string();
            listener_handles.push(svc.add_listener(FnListener::new(
                None,
                None,
                None,
                None,
                Some(Box::new(move |_, err| {
                    let _ = tx.send((name.clone(), err.to_string()));
                })),
            )));
        }
    }

    let mut startup_err: Option<(String, anyhow::Error)> = None;
    let mut signaled = false;
    {
        let startup_fut = async {
            // memberlist-kv: starting (transport + fast-join) runs while HTTP/gRPC already serve;
            // /ready reports "memberlist-kv: Starting" meanwhile. Rings depend on it.
            if memberlist_module {
                log_module_starting(&*logger, "memberlist-kv");
            }
            if let Some(ref svc) = memberlist_svc {
                svc.start_async(service_ctx.child())
                    .await
                    .map_err(|e| ("memberlist-kv".to_string(), anyhow::anyhow!(e)))?;
                if let Err(err) = svc.await_running().await {
                    let cause = svc
                        .failure_case()
                        .map(|f| f.to_string())
                        .unwrap_or_else(|| err.to_string());
                    return Err((
                        "memberlist-kv".to_string(),
                        anyhow::anyhow!("starting module memberlist-kv: {cause}"),
                    ));
                }
            } else {
                memberlist_state.set(State::Running);
            }
            if let Some(ref rsvc) = ring_svc {
                log_module_starting(&*logger, "ingester-ring");
                rsvc.start_async(service_ctx.child())
                    .await
                    .map_err(|e| ("ingester-ring".to_string(), anyhow::anyhow!(e)))?;
                rsvc.await_running().await.map_err(|e| {
                    (
                        "ingester-ring".to_string(),
                        anyhow::anyhow!("starting module ingester-ring: {e}"),
                    )
                })?;
            }
            if let Some(ref wsvc) = watcher_svc {
                log_module_starting(&*logger, "ingester-partitions-ring");
                wsvc.start_async(service_ctx.child())
                    .await
                    .map_err(|e| ("ingester-partitions-ring".to_string(), anyhow::anyhow!(e)))?;
                wsvc.await_running().await.map_err(|e| {
                    (
                        "ingester-partitions-ring".to_string(),
                        anyhow::anyhow!("starting module ingester-partitions-ring: {e}"),
                    )
                })?;
            }
            if let Some(ref ing) = ingester_opt {
                log_module_starting(&*logger, "ingester-service");
                ing.start().await.map_err(|e| {
                    (
                        "ingester-service".to_string(),
                        anyhow::anyhow!("starting module ingester-service: {e}"),
                    )
                })?;
            }
            Ok::<(), (String, anyhow::Error)>(())
        };
        tokio::pin!(startup_fut);

        tokio::select! {
            res = &mut startup_fut => {
                if let Err(err) = res {
                    startup_err = Some(err);
                }
            }
            _ = &mut shutdown_sig => {
                signaled = true;
                // Go keeps starting modules during -shutdown-delay, then stops the manager.
                shutdown_requested.store(true, Ordering::Release);
                tokio::select! {
                    _ = tokio::time::sleep(shutdown_delay) => {}
                    res = &mut startup_fut => {
                        if res.is_ok() {
                            tokio::time::sleep(shutdown_delay).await;
                        }
                    }
                }
            }
        }
    }

    let mut runtime_failure: Option<(String, String)> = None;
    if !signaled && startup_err.is_none() {
        logger.log(LogLevel::Info, &[("msg", "Application started")]);
        // 9. Wait for SIGTERM / SIGINT, or for any module to fail (POST /ingester/shutdown stops
        // only the Ingester service, keeping HTTP/gRPC alive until SIGTERM, matching Go Mimir).
        let ingester_failure = async {
            match ingester_opt {
                Some(ref ing) => ing.wait_subservice_failure().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = &mut shutdown_sig => {
                request_shutdown(&shutdown_requested, shutdown_delay).await;
            }
            err = ingester_failure => {
                runtime_failure = Some(("ingester-service".to_string(), err));
            }
            Some(failure) = module_failed_rx.recv() => {
                runtime_failure = Some(failure);
            }
        }
    }
    for h in listener_handles {
        h.stop();
    }

    // Modules started in dependency order after the server; each depends on all the previous
    // ones in Go (`IngesterRing` -> `MemberlistKV`, `IngesterPartitionRing` -> `IngesterRing`,
    // `IngesterService` -> all of them).
    let start_order: Vec<&str> = [
        (memberlist_module, "memberlist-kv"),
        (ring_svc.is_some(), "ingester-ring"),
        (watcher_svc.is_some(), "ingester-partitions-ring"),
        (ingester_opt.is_some(), "ingester-service"),
    ]
    .into_iter()
    .filter_map(|(present, name)| present.then_some(name))
    .collect();
    // Index in `start_order` of the module whose startup failed: it and the modules after it
    // never reached Running, so dskit never runs their `stop()` (no "module stopped" line).
    let mut first_not_running = start_order.len();
    if let Some((ref module, ref err)) = startup_err {
        let err_s = format!("{err:#}");
        if signaled {
            logger.log(
                LogLevel::Info,
                &[
                    ("msg", "module startup aborted by shutdown signal"),
                    ("module", module),
                    ("err", &err_s),
                ],
            );
        } else {
            logger.log(
                LogLevel::Error,
                &[("msg", "module failed"), ("module", module), ("err", &err_s)],
            );
            if let Some(pos) = start_order.iter().position(|m| m == module) {
                first_not_running = pos;
                // dskit module_service.go start(): modules waiting for the failed one fail too.
                for dependent in &start_order[pos + 1..] {
                    let dep_err = format!(
                        "failed to start {dependent}, because it depends on module {module}, \
                         which has failed: invalid service state: Failed, expected: Running, \
                         failure: {err_s}"
                    );
                    logger.log(
                        LogLevel::Error,
                        &[("msg", "module failed"), ("module", dependent), ("err", &dep_err)],
                    );
                }
            }
        }
    }
    let reached_running = |module: &str| {
        start_order
            .iter()
            .position(|m| *m == module)
            .is_none_or(|pos| pos < first_not_running)
    };
    if let Some((ref module, ref err)) = runtime_failure {
        logger.log(
            LogLevel::Error,
            &[("msg", "module failed"), ("module", module), ("err", err)],
        );
    }
    shutdown_requested.store(true, Ordering::Release);

    // 10. Shutdown order (reverse module dependencies): stop the Ingester FIRST (so the lifecycler
    // transitions to LEAVING, flushes blocks and unregisters while HTTP/gRPC and memberlist are
    // still running), THEN the ring services, memberlist, the HTTP/gRPC servers, sanity-check
    // and usage-stats.
    let mut ingester_failed = false;
    if let Some(ref ing) = ingester_opt {
        match ing.stop().await {
            Ok(()) if reached_running("ingester-service") => {
                log_module_stopped(&*logger, "ingester-service")
            }
            Ok(()) => {}
            Err(err) => {
                ingester_failed = true;
                if !signaled && startup_err.is_none() && runtime_failure.is_none() {
                    // dskit modules/module_service.go stop()
                    let err_s = err.to_string();
                    logger.log(
                        LogLevel::Warn,
                        &[
                            ("msg", "module failed with error"),
                            ("module", "ingester-service"),
                            ("err", &err_s),
                        ],
                    );
                }
            }
        }
    }

    for (name, svc) in [
        ("ingester-partitions-ring", watcher_svc),
        ("ingester-ring", ring_svc),
    ] {
        if let Some(svc) = svc {
            stop_module_service(&*logger, name, &svc, reached_running(name)).await;
        }
    }
    service_ctx.cancel();
    runtime_config_state.set(State::Stopping);
    memberlist_state.set(State::Stopping);
    server_state.set(State::Stopping);

    // `MemberlistKV` depends on `API` -> `Server`: memberlist leaves the cluster and stops
    // before the HTTP/gRPC servers.
    let memberlist_failure = memberlist_svc
        .as_ref()
        .filter(|svc| svc.state() == State::Failed)
        .map(|svc| svc.failure_case().map(|e| e.to_string()).unwrap_or_default());
    if let Some(ml) = memberlist_kv {
        ml.shutdown().await;
    }
    memberlist_state.set(State::Terminated);
    if memberlist_module && reached_running("memberlist-kv") {
        match memberlist_failure {
            Some(err_s) => logger.log(
                LogLevel::Warn,
                &[("msg", "module failed with error"), ("module", "memberlist-kv"), ("err", &err_s)],
            ),
            None => log_module_stopped(&*logger, "memberlist-kv"),
        }
    }

    let _ = http_shutdown_tx.send(());
    let _ = grpc_shutdown_tx.send(());

    let graceful_timeout = Duration::from_nanos(
        cfg.server
            .graceful_shutdown_timeout
            .0
            .max(100_000_000) as u64,
    );
    let _ = tokio::time::timeout(graceful_timeout, server_done_rx).await;
    server_state.set(State::Terminated);
    // dskit server_service.go
    logger.log(LogLevel::Info, &[("msg", "server stopped")]);
    log_module_stopped(&*logger, "server");
    // `Server` depends on `SanityCheck` and `UsageStats`: they stop after it.
    sanity_state.set(State::Terminated);
    log_module_stopped(&*logger, "sanity-check");
    if let Some(svc) = usage_stats_svc {
        svc.stop_async();
        let _ = svc.await_terminated().await;
        log_module_stopped(&*logger, "usage-stats");
    }
    if let Some(rt) = runtime_config_mgr {
        rt.stop();
    }
    runtime_config_state.set(State::Terminated);
    if let Some(at) = activity_tracker {
        if !cfg.activity_tracker.filepath.is_empty() {
            if let Ok(entries) = load_unfinished_entries(&cfg.activity_tracker.filepath) {
                if !entries.is_empty() {
                    let count_s = entries.len().to_string();
                    logger.log(
                        LogLevel::Warn,
                        &[
                            ("component", "activity-tracker"),
                            ("msg", "found unfinished activities during shutdown"),
                            ("count", &count_s),
                        ],
                    );
                    for e in &entries {
                        let ts = format_rfc3339_nano_utc(e.timestamp);
                        logger.log(
                            LogLevel::Warn,
                            &[
                                ("component", "activity-tracker"),
                                ("start", &ts),
                                ("activity", &e.activity),
                            ],
                        );
                    }
                }
            }
        }
        let _ = at.flush();
    }

    logger.log(LogLevel::Info, &[("msg", "Application stopped")]);
    // A shutdown signal received while services are still starting is a requested shutdown,
    // not a failure (exit 0); any other module failure makes the process exit non-zero.
    if signaled {
        return Ok(());
    }
    if startup_err.is_some() || runtime_failure.is_some() || ingester_failed {
        bail!("failed services");
    }
    Ok(())
}

/// Go's signal handler: report "not ready" (and disable keep-alives) right away, then wait
/// `-shutdown-delay` before stopping the modules.
async fn request_shutdown(shutdown_requested: &AtomicBool, shutdown_delay: Duration) {
    shutdown_requested.store(true, Ordering::Release);
    if !shutdown_delay.is_zero() {
        tokio::time::sleep(shutdown_delay).await;
    }
}

/// State of a dskit-style module service.
fn service_state_fn(svc: &Arc<ManagedBasicService>) -> StateFn {
    let svc = Arc::clone(svc);
    Arc::new(move || svc.state())
}

/// dskit modules/module_service.go stop(): stops a module service that reached Running and logs
/// its outcome; a service that failed (while starting or running) only reports its failure.
async fn stop_module_service(
    logger: &dyn Logger,
    module: &str,
    svc: &Arc<ManagedBasicService>,
    reached_running: bool,
) {
    let failed_before_stop = svc.state() == State::Failed;
    svc.stop_async();
    let _ = svc.await_terminated().await;
    if failed_before_stop {
        if reached_running {
            let err_s = svc.failure_case().map(|e| e.to_string()).unwrap_or_default();
            logger.log(
                LogLevel::Warn,
                &[("msg", "module failed with error"), ("module", module), ("err", &err_s)],
            );
        }
    } else if reached_running {
        log_module_stopped(logger, module);
    }
}

/// dskit modules/module_service.go start().
fn log_module_starting(logger: &dyn Logger, module: &str) {
    logger.log(LogLevel::Info, &[("msg", "starting"), ("module", module)]);
}

/// dskit modules/module_service.go stop().
fn log_module_stopped(logger: &dyn Logger, module: &str) {
    logger.log(LogLevel::Info, &[("msg", "module stopped"), ("module", module)]);
}

fn install_shutdown_signal(
    logger: Arc<LogSink>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("install SIGINT handler");
        Box::pin(async move {
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
            }
            // dskit signals/signals.go Handler.Loop()
            logger.log(
                LogLevel::Info,
                &[("msg", "=== received SIGINT/SIGTERM ===\n*** exiting")],
            );
        })
    }

    #[cfg(not(unix))]
    {
        Box::pin(async move {
            let _ = tokio::signal::ctrl_c().await;
            logger.log(
                LogLevel::Info,
                &[("msg", "=== received SIGINT/SIGTERM ===\n*** exiting")],
            );
        })
    }
}
