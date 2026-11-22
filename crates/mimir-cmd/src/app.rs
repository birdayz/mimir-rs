//! Application lifecycle, module initialization, HTTP + gRPC servers, and graceful shutdown.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/mimir/mimir.go
//! - github.com/grafana/mimir/pkg/mimir/modules.go
//! - github.com/grafana/dskit/server/server.go

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use mimir_common::activitytracker::{
    load_unfinished_entries_partial, ActivityTracker, ActivityTrackerConfig,
};
use mimir_common::logging::{
    format_rfc3339_nano_utc, global_logger, init_global_logger, LogFormat, LogLevel, LogSink,
    LogValue, Logger,
};
use mimir_common::tracing::{
    detected_exporter_kind, encode_jaeger_agent_emit_batch_compact,
    encode_jaeger_thrift_batch_binary, encode_otlp_grpc_frame, encode_otlp_trace_request_protobuf,
    global_tracing, init_otel_or_jaeger_from_env, jaeger_basic_auth_from_env,
    jaeger_target_from_env, otlp_headers_from_env, otlp_protocol_from_env,
    otlp_timeout_from_env, otlp_tls_paths_from_env, otlp_traces_endpoint_for_protocol,
    resolve_service_name, ExportedSpan, JaegerTarget, OtlpProtocol, SpanAttrValue,
    SpanExporterFn,
};
use mimir_common::services::{FnListener, ManagedBasicService, Service, ServiceContext, State};
use mimir_config::ingester::KVStoreConfig;
use mimir_config::{
    MimirConfig, Overrides, RuntimeConfigHttpObserver, RuntimeConfigManager,
    RuntimeConfigManagerOptions, RuntimeConfigObserver,
};
use mimir_ingester::ingest_storage::{
    ingester_partition_id_from_instance_id, IngesterPartitionRingDelegate,
};
use mimir_ingester::grpc::SnappyDecompressService;
use mimir_ingester::{Ingester, IngesterLifecycler, IngesterService, TokenlessLifecycler};
use mimir_proto::httpgrpcpb::http_server::HttpServer;
use mimir_ring::kv::ha_replica::replica_desc_codec;
use mimir_ring::kv::memberlist::{KvOptions, MemberlistKv};
use mimir_ring::kv::multi::runtime_config_provider;
use mimir_ring::kv::{
    build_kv_client_with_multi_config_provider, registerer_with_kv_name, Codec, Registerer,
};
use mimir_ring::lifecycler::{BasicLifecyclerConfig, Lifecycler, LifecyclerConfig};
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, PartitionInstanceLifecycler, PartitionInstanceLifecyclerConfig,
    PartitionRingOptions, PartitionRingWatcher, PartitionRingWatchers,
};
use mimir_ring::ring::model::ring_desc_codec;
use mimir_ring::ring::replication_strategy::DefaultReplicationStrategy;
use mimir_ring::ring::{Ring, RingSettings};
use mimir_ring::token_gen::{PartitionTokenGenerator, PartitionTokenTable};
use mimir_storage::bucket::BucketClient;
use mimir_storage::ingest::with_read_compartment_suffix;
use prometheus::Registry;
use tonic::codec::CompressionEncoding;

use crate::cli::CliOutcome;
use crate::grpc::{
    GrpcCompressionEchoService, HealthService, HttpGrpcBridge, HttpGrpcStatusFixService,
};
use crate::http::{build_http_router, AppHttpState};
use crate::metrics::{register_global_metrics_with_deprecated, version_info};
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
            "ingester" | "memberlist-kv" => {}
            // `all` (Mimir's default target) includes the distributor, querier, store-gateway,
            // compactor, ruler, ... which mimir-rs does not implement: refuse to start rather
            // than serving a "Ready" process that only runs the ingester.
            other => {
                bail!(
                    "target module {other:?} is not yet implemented in mimir-rs; supported targets: ingester, memberlist-kv"
                );
            }
        }
    }
    Ok(())
}

/// Runs the configured Mimir application until SIGTERM/SIGINT or `/ingester/shutdown`.
pub async fn run_mimir(outcome: CliOutcome) -> Result<()> {
    let cfg = outcome
        .config
        .clone()
        .context("missing parsed MimirConfig")?;

    cfg.validate_targets()?;
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

    for flag in cfg.deprecated_flags_used() {
        logger.log(
            LogLevel::Warn,
            &[("msg", "flag disabled"), ("flag", flag)],
        );
    }

    register_global_metrics_with_deprecated(
        &registry,
        outcome.config_hash_sha256.as_deref(),
        cfg.deprecated_flags_used().len(),
    )
    .context("registering global Prometheus metrics")?;
    crate::metrics::register_process_collector(&registry)
        .context("registering process collector")?;

    let ref_leaks_tracker = cfg
        .instrument_ref_leaks
        .to_proto_config()
        .tracker(Some(&registry));

    let tracing_service = resolve_service_name(&cfg.target.0);
    let mut span_exporter_handle: Option<SpanExporterHandle> = None;
    match try_build_span_exporter_with_handle() {
        Ok((span_exporter, handle)) => {
            if let Err(err) =
                init_otel_or_jaeger_from_env(&tracing_service, logger.as_ref(), span_exporter)
            {
                if let Some(h) = handle {
                    h.shutdown(Duration::from_secs(5), logger.as_ref()).await;
                }
                logger.log(
                    LogLevel::Error,
                    &[("msg", "Failed to setup tracing"), ("err", &err)],
                );
            } else {
                span_exporter_handle = handle;
            }
        }
        Err(err) => {
            logger.log(
                LogLevel::Error,
                &[("msg", "Failed to setup tracing"), ("err", &err)],
            );
        }
    }

    let version = version_info();
    logger.log(
        LogLevel::Info,
        &[("msg", "Starting application"), ("version", &version)],
    );

    let ingester_enabled = cfg.is_ingester_enabled();
    let service_ctx = ServiceContext::background();
    let modules = ModuleStates::new();
    let server_state = StateCell::new(State::New);
    modules.add("server", server_state.state_fn());
    let active_groups_state = StateCell::new(State::New);
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_delay = Duration::from_nanos(cfg.shutdown_delay.0.max(0) as u64);

    let mut activity_tracker: Option<Arc<ActivityTracker>> = None;
    let mut activity_tracker_state: Option<Arc<StateCell>> = None;
    let mut runtime_config_mgr: Option<Arc<RuntimeConfigManager>> = None;
    let runtime_config_state = StateCell::new(State::New);
    let mut active_groups_timer: Option<Option<tokio::task::JoinHandle<()>>> = None;
    let mut usage_stats_svc: Option<Arc<ManagedBasicService>> = None;

    let pre_sanity = async {
        // 1. Activity tracker (matching initActivityTracker in pkg/mimir/modules.go).
        if !cfg.activity_tracker.filepath.is_empty() {
            let (entries, err) = load_unfinished_entries_partial(&cfg.activity_tracker.filepath);
            if let Some(err) = err {
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
            if !entries.is_empty() {
                logger.log_values(
                    LogLevel::Warn,
                    &[
                        ("component", LogValue::from("activity-tracker")),
                        (
                            "msg",
                            LogValue::from("found unfinished activities from previous run"),
                        ),
                        ("count", LogValue::U64(entries.len() as u64)),
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

            let max_entries =
                validate_activity_tracker_max_entries(cfg.activity_tracker.max_entries)
                    .context("initializing activity tracker")?;
            let tracker = ActivityTracker::new(
                ActivityTrackerConfig {
                    filepath: PathBuf::from(&cfg.activity_tracker.filepath),
                    max_entries,
                },
                Some(&registry),
            )
            .context("initializing activity tracker")?;
            activity_tracker = tracker.map(Arc::new);
        }

        // 2. RuntimeConfigManager & Overrides.
        let overrides = if ingester_enabled && !cfg.runtime_config.file.0.is_empty() {
            let sources = mimir_config::runtime_config_source_ids(&cfg.runtime_config)
                .context("initializing runtime config manager")?;
            let observer = Arc::new(
                crate::runtime_config_metrics::RuntimeConfigMetrics::new_with_http(
                    &registry,
                    Arc::clone(&logger) as Arc<dyn Logger>,
                    &sources,
                    &cfg.runtime_config.file.0,
                    &cfg.runtime_config.http_client_cluster_validation.label,
                )
                .context("registering runtime config metrics")?,
            );
            let default_distributor_limits = cfg
                .distributor_instance_limits()
                .context("reading distributor.instance_limits")?;
            let querier_query_store_after = cfg.querier_query_store_after();
            let mgr = RuntimeConfigManager::new_with_options(RuntimeConfigManagerOptions {
                cfg: cfg.runtime_config.clone(),
                default_limits: cfg.limits.clone(),
                default_ingester_limits: cfg.ingester.instance_limits.clone(),
                default_distributor_limits,
                querier_query_store_after,
                observer: Some(Arc::clone(&observer) as Arc<dyn RuntimeConfigObserver>),
                http_observer: Some(observer as Arc<dyn RuntimeConfigHttpObserver>),
            })
            .context("initializing runtime config manager")?;
            let ov = mgr.overrides();
            runtime_config_mgr = Some(mgr);
            ov
        } else {
            Overrides::new(cfg.limits.clone())
        };

        // 3. Server module (dskit server.New): validates server config, binds HTTP & gRPC
        //    listeners early so port 0 resolves before ring registration, builds TLS configs and middleware.
        cfg.server
            .validate()
            .context("error initialising module: server")?;
        let mut server =
            crate::server::Server::new(&cfg.server, &registry, Arc::clone(&logger) as Arc<dyn Logger>)
                .context("error initialising module: server")?;
        server.set_grpc_multitenancy(cfg.multitenancy_enabled, &cfg.no_auth_tenant);
        server.set_compartments_enabled(cfg.compartments.enabled);
        let _bound_http_addr = server.http_addr()?;
        let bound_grpc_addr = server.grpc_addr()?;

        activity_tracker_state = activity_tracker
            .as_ref()
            .map(|_| StateCell::new(State::Running));
        if let Some(ref st) = activity_tracker_state {
            log_module_starting(&*logger, "activity-tracker");
            modules.add("activity-tracker", st.state_fn());
        }

        // Install SIGTERM/SIGINT/SIGQUIT listeners before any module starts: Go's signal handler
        // runs for the whole lifetime of the service manager, including module startup.
        let (shutdown_sig, second_signal_guard) = install_shutdown_signal(Arc::clone(&logger))?;
        server.set_shutdown_requested(Arc::clone(&shutdown_requested));

        // Active groups cleanup service (pkg/mimir/modules.go initActiveGroupsCleanupService): a
        // dependency-free timer module of the `ingester` target, started at once with the other
        // leaf modules and stopped first.
        let active_groups = if ingester_enabled {
            let ag = mimir_ingester::active_groups::ActiveGroupsCleanupService::new(
                Duration::from_secs(3 * 60),
                go_duration_std(cfg.ingester.active_series.active_series_metrics_idle_timeout),
                cfg.max_separate_metrics_groups_per_user.max(0) as usize,
            );
            modules.add("active-groups-cleanup-service", active_groups_state.state_fn());
            Some(ag)
        } else {
            None
        };
        active_groups_timer = active_groups.as_ref().map(|ag| {
            log_module_starting(&*logger, "active-groups-cleanup-service");
            let ag = Arc::clone(ag);
            let timer = spawn_timer(ag.cleanup_interval(), move || {
                ag.iteration(std::time::SystemTime::now());
            });
            active_groups_state.set(State::Running);
            timer
        });

        // Anonymous usage statistics (`initUsageStats`), only for components accessing the blocks
        // storage. `Server` depends on `UsageStats` in Go.
        mimir_common::usagestats::set_target(&cfg.target.0.join(","));
        mimir_common::usagestats::set_mode(
            if cfg.ingest_storage.enabled && ingester_enabled {
                mimir_common::usagestats::MODE_INGEST_STORAGE
            } else {
                mimir_common::usagestats::MODE_CLASSIC
            },
        );
        if cfg.usage_stats.enabled && ingester_enabled {
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
            svc.start_async(ServiceContext::background())
                .await
                .context("starting module usage-stats")?;
            modules.add("usage-stats", service_state_fn(&svc));
            usage_stats_svc = Some(svc);
        }

        Ok::<_, anyhow::Error>((
            overrides,
            server,
            bound_grpc_addr,
            shutdown_sig,
            second_signal_guard,
            active_groups,
        ))
    }
    .await;

    let (
        overrides,
        mut server,
        bound_grpc_addr,
        mut shutdown_sig,
        _second_signal_guard,
        active_groups,
    ) = match pre_sanity {
        Ok(v) => v,
        Err(e) => {
            cleanup_pre_server_modules(
                &*logger,
                active_groups_timer,
                &active_groups_state,
                usage_stats_svc,
                runtime_config_mgr,
                &cfg.activity_tracker.filepath,
                activity_tracker,
                activity_tracker_state.as_deref(),
                span_exporter_handle.take(),
            )
            .await;
            return Err(e);
        }
    };

    // `Server` depends on `SanityCheck`: nothing is served and no other module starts until
    // the directories and object storage checks pass.
    let sanity_state = StateCell::new(State::Starting);
    modules.add("sanity-check", sanity_state.state_fn());
    log_module_starting(&*logger, "sanity-check");
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
        cleanup_pre_server_modules(
            &*logger,
            active_groups_timer,
            &active_groups_state,
            usage_stats_svc,
            runtime_config_mgr,
            &cfg.activity_tracker.filepath,
            activity_tracker,
            activity_tracker_state.as_deref(),
            span_exporter_handle.take(),
        )
        .await;
        logger.log(LogLevel::Info, &[("msg", "Application stopped")]);
        sd_notify("STOPPING=1");
        if signaled {
            return Ok(());
        }
        bail!("failed services");
    }
    sanity_state.set(State::Running);

    let post_sanity = async {
    // 4. Memberlist KV (if required by ring/partition_ring or explicitly targeted).
    // Go always runs `initMemberlistKV` for the ingester (`memberlist.KVInitService`, which
    // creates the memberlist client lazily on `GetMemberlistKV`, while eagerly creating its
    // `dns.Provider` with `cortex_` prefix and `component="memberlist"`).
    let needs_memberlist = cfg.is_module_explicitly_targeted("memberlist-kv")
        || (ingester_enabled
            && (kv_uses_memberlist(&cfg.ingester.ring.kvstore)
                || (cfg.ingest_storage.enabled
                    && kv_uses_memberlist(&cfg.ingester.partition_ring.kvstore))));

    let memberlist_module = ingester_enabled || needs_memberlist;
    let memberlist_dns_provider: Option<Arc<dyn mimir_ring::dns::DnsProvider>> =
        if memberlist_module {
            let labels = std::collections::HashMap::from([(
                "component".to_string(),
                "memberlist".to_string(),
            )]);
            let provider = mimir_ring::dns::Provider::new(
                &mimir_ring::dns::GOLANG_RESOLVER_TYPE,
                0,
                Some(&registry),
                "cortex_",
                labels,
            )
            .context("creating memberlist DNS provider")?;
            Some(Arc::new(provider))
        } else {
            None
        };

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
            KvOptions {
                dns_provider: memberlist_dns_provider.clone(),
                ..Default::default()
            },
        )
        .map_err(|e| {
            if cfg.is_module_explicitly_targeted("memberlist-kv") {
                anyhow::anyhow!("failed to initialise memberlist client instance: {e}")
            } else {
                anyhow::anyhow!("creating memberlist KV: {e}")
            }
        })?;
        Some(ml)
    } else {
        None
    };
    let memberlist_state = StateCell::new(State::New);
    let memberlist_svc = memberlist_kv.as_ref().map(|ml| ml.service());
    if memberlist_module {
        modules.add("memberlist-kv", memberlist_state.state_fn());
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

    let mut cost_attribution_registry: Option<(String, Registry)> = None;
    let mut cost_attribution_mgr: Option<Arc<mimir_ingester::costattribution::Manager>> = None;
    let cost_attribution_state = StateCell::new(State::New);
    let ingester_opt: Option<Arc<Ingester>> = if ingester_enabled {
        // modules.go initRuntimeConfig: `multi` KV clients of the ingester ring and partition
        // ring follow `multi_kv_config` from the runtime config (multiClientRuntimeConfigChannel).
        // KV metrics get dskit's kv_name label and the registerer prefix of each client.
        let multi_cfg_provider = runtime_config_mgr.as_ref().map(runtime_config_provider);
        let instance_ring_kv = build_kv_client_with_multi_config_provider(
            &cfg.ingester.ring.kvstore,
            ring_desc_codec(),
            memberlist_kv.clone(),
            registerer_with_kv_name(Some(&cortex_registerer), "ingester-ring").as_ref(),
            multi_cfg_provider.clone(),
        )
        .context("creating ingester ring")?;
        let ring_settings =
            RingSettings::try_from_config(&cfg.ingester.ring).context("creating ingester ring")?;
        let instance_ring = Ring::with_store_and_strategy(
            ring_settings,
            INGEST_RING_NAME,
            INGEST_RING_KEY,
            instance_ring_kv,
            Arc::new(DefaultReplicationStrategy),
            Some(&cortex_registerer),
        )
        .context("creating ingester ring")?;

        let rsvc = Arc::clone(&instance_ring).into_service();
        modules.add("ingester-ring", service_state_fn(&rsvc));
        ring_svc = Some(rsvc);

        // In tokenless mode (num_tokens = 0) the ingester registers through a BasicLifecycler
        // without tokens, otherwise through the classic Lifecycler (`ingester.New`). Both use
        // the `<ring>-lifecycler` KV client on the cortex_-prefixed registerer.
        let lifecycler_kv = build_kv_client_with_multi_config_provider(
            &cfg.ingester.ring.kvstore,
            ring_desc_codec(),
            memberlist_kv.clone(),
            registerer_with_kv_name(
                Some(&cortex_registerer),
                &format!("{INGEST_RING_NAME}-lifecycler"),
            )
            .as_ref(),
            multi_cfg_provider.clone(),
        );
        let lifecycler: Arc<dyn IngesterLifecycler> = if cfg.ingester.ring.num_tokens == 0 {
            if !cfg.ingest_storage.enabled {
                bail!("ring tokens can only be disabled when ingest storage is enabled");
            }
            let ring_kv =
                lifecycler_kv.context("creating KV store for ingester ring in tokenless mode")?;
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
            let lc_cfg = LifecyclerConfig::from_ring_config(&cfg.ingester.ring, bound_grpc_addr.port())
                .context("creating ingester lifecycler")?;
            lc_cfg
                .validate()
                .map_err(|e| anyhow::anyhow!("error initialising module: ingester-service: {e}"))?;
            Lifecycler::with_store(
                lc_cfg,
                lifecycler_kv.context("creating ingester lifecycler")?,
                None,
                INGEST_RING_NAME,
                INGEST_RING_KEY,
                cfg.blocks_storage.tsdb.flush_blocks_on_shutdown,
                Some(&cortex_registerer),
            )
        };

        let mut partition_ring_watchers: Option<Arc<PartitionRingWatchers>> = None;
        let (partition_ring_watcher, partition_lifecycler) = if cfg.ingest_storage.enabled {
            let watcher_kv = build_kv_client_with_multi_config_provider(
                &cfg.ingester.partition_ring.kvstore,
                partition_ring_desc_codec(),
                memberlist_kv.clone(),
                registerer_with_kv_name(
                    Some(&base_registerer),
                    &format!("{PARTITION_RING_NAME}-watcher"),
                )
                .as_ref(),
                multi_cfg_provider.clone(),
            )
            .context("creating KV store for ingester partitions ring watcher")?;

            let token_table = PartitionTokenTable::new(
                cfg.ingester.partition_ring.max_derived_token_partitions as i32,
                Some(&cortex_registerer),
            )
            .context("creating ingester partition token table")?;

            let ring_opts = PartitionRingOptions {
                token_generator: Some(Arc::new(token_table) as Arc<dyn PartitionTokenGenerator>),
                shuffle_shard_cache_size: 0,
            };
            // `ingest.NewPartitionRingWatchers`: with compartments enabled, one watcher per read
            // compartment on its `-rc-<id>` ring; the ingester owns only its own compartment's
            // ring, while the status pages serve all of them.
            let (watcher, wsvc) = if cfg.compartments.enabled {
                let watchers = (0..cfg.compartments.read.num_compartments.max(0) as usize)
                    .map(|c| {
                        PartitionRingWatcher::new(
                            &with_read_compartment_suffix(PARTITION_RING_NAME, c),
                            &with_read_compartment_suffix(PARTITION_RING_KEY, c),
                            Arc::clone(&watcher_kv),
                            ring_opts.clone(),
                            Some(&cortex_registerer),
                        )
                    })
                    .collect();
                let group = PartitionRingWatchers::new(watchers).map_err(|e| {
                    anyhow::anyhow!("creating ingester partition rings watcher: {e}")
                })?;
                let watcher = group
                    .watcher(cfg.ingester.read_compartment_id.max(0) as usize)
                    .cloned()
                    .with_context(|| {
                        format!(
                            "ingester read compartment ID {} is out of range [0, {})",
                            cfg.ingester.read_compartment_id,
                            cfg.compartments.read.num_compartments
                        )
                    })?;
                partition_ring_watchers = Some(Arc::clone(&group));
                let wsvc = group.into_service().map_err(|e| {
                    anyhow::anyhow!("creating ingester partition rings watcher: {e}")
                })?;
                (watcher, wsvc)
            } else {
                let watcher = PartitionRingWatcher::new(
                    PARTITION_RING_NAME,
                    PARTITION_RING_KEY,
                    watcher_kv,
                    ring_opts,
                    Some(&cortex_registerer),
                );
                let wsvc = Arc::clone(&watcher).into_service();
                (watcher, wsvc)
            };
            modules.add("ingester-partitions-ring", service_state_fn(&wsvc));
            watcher_svc = Some(wsvc);

            let partition_id =
                ingester_partition_id_from_instance_id(&cfg.ingester.ring.instance_id)
                    .map_err(|e| anyhow::anyhow!("calculating ingester partition ID: {e}"))?;

            let plc_kv = build_kv_client_with_multi_config_provider(
                &cfg.ingester.partition_ring.kvstore,
                partition_ring_desc_codec(),
                memberlist_kv.clone(),
                registerer_with_kv_name(
                    Some(&base_registerer),
                    &format!("{PARTITION_RING_NAME}-lifecycler"),
                )
                .as_ref(),
                multi_cfg_provider,
            )
            .context("creating KV store for ingester partition ring")?;

            let plc_cfg = PartitionInstanceLifecyclerConfig::from_partition_ring_config(
                &cfg.ingester.partition_ring,
                partition_id,
                &cfg.ingester.ring.instance_id,
                Duration::from_secs(5),
            );
            // With compartments enabled the ingester registers its partition into the partition
            // ring of its own read compartment (Go `ingester.New`).
            let (plc_ring_name, plc_ring_key) = if cfg.compartments.enabled {
                let rc = cfg.ingester.read_compartment_id.max(0) as usize;
                (
                    with_read_compartment_suffix(PARTITION_RING_NAME, rc),
                    with_read_compartment_suffix(PARTITION_RING_KEY, rc),
                )
            } else {
                (PARTITION_RING_NAME.to_string(), PARTITION_RING_KEY.to_string())
            };
            let plc = PartitionInstanceLifecycler::new(
                plc_cfg,
                &plc_ring_name,
                &plc_ring_key,
                plc_kv,
                Some(&cortex_registerer),
            )
            .context("creating ingester partition lifecycler")?;
            plc.apply_min_partition_owners_duration(
                cfg.ingester.partition_ring.min_partition_owners_duration,
            );

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
        let (tf_enabled, tf_max_tenants) = cfg.tenant_federation_config();
        ingester.set_tenant_federation(tf_enabled, tf_max_tenants);
        // modules.go initIngesterService: `t.Cfg.Ingester.Compartments = t.Cfg.Compartments`.
        ingester.set_compartments(cfg.compartments.clone());
        ingester.set_partition_ring_watchers(partition_ring_watchers.take());
        ingester.set_activity_tracker(activity_tracker.clone());
        // mimir.go: `mimirpb.CustomCodecConfig{InstrumentRefLeaksConfig}.RegisterGlobally(reg)`
        // -> `codecV2.Unmarshal` instruments each unmarshalled `WriteRequest` buffer.
        if cfg.instrument_ref_leaks.percentage > 0.0 {
            ingester.set_ref_leaks_tracker(ref_leaks_tracker.clone());
        }

        // Cost attribution service (pkg/mimir/modules.go initCostAttributionService): only enabled
        // when -cost-attribution.registry-path is set.
        if !cfg.cost_attribution_registry_path.is_empty() {
            let ca_reg = Registry::new();
            let mgr = mimir_ingester::costattribution::Manager::new(
                go_duration_std(cfg.cost_attribution_eviction_interval),
                overrides.clone(),
                Some(&registry),
                &ca_reg,
            )
            .map_err(|e| anyhow::anyhow!("creating cost attribution manager: {e}"))?;
            ingester.set_cost_attribution_manager(Some(Arc::clone(&mgr)));
            cost_attribution_mgr = Some(mgr);
            modules.add("cost-attribution-service", cost_attribution_state.state_fn());
            cost_attribution_registry =
                Some((cfg.cost_attribution_registry_path.clone(), ca_reg));
        }

        ingester.set_active_groups_cleanup_service(active_groups.as_ref().map(Arc::clone));

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

    if runtime_config_mgr.is_some() {
        modules.add("runtime-config", runtime_config_state.state_fn());
    }

    let config_arc = Arc::new(cfg.clone());
    let default_config_arc = Arc::new(MimirConfig::default());

    if let Some((ref ca_path, _)) = cost_attribution_registry {
        server.set_cost_attribution_path(ca_path);
    }

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
        cost_attribution_registry,
    });

    // 7. Start HTTP & gRPC servers BEFORE awaiting ingester.start() so /metrics and /ready (503 -> 200)
    // are live during WAL replay and Kafka catch-up.
    let max_recv = cfg.server.grpc_server_max_recv_msg_size.max(0) as usize;
    let max_send = cfg.server.grpc_server_max_send_msg_size.max(0) as usize;

    let http_grpc_svc = GrpcCompressionEchoService::new_with_limits(
        HttpGrpcStatusFixService::new(SnappyDecompressService::new(
            HttpServer::new(HttpGrpcBridge::new(http_router.clone()))
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            max_recv,
        )),
        max_recv,
        max_send,
    );

    let health_svc = GrpcCompressionEchoService::new_with_limits(
        SnappyDecompressService::new(
            HealthService::new(Arc::clone(&shutdown_requested), Arc::clone(&modules))
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            max_recv,
        ),
        max_recv,
        max_send,
    );

    let mut grpc_routes = tonic::service::Routes::new(health_svc).add_service(http_grpc_svc);

    if let Some(ref ing) = ingester_opt {
        server.set_grpc_tap_ingester(Arc::clone(ing));
        let ing_inner = IngesterService::new(Arc::clone(ing))
            .into_server()
            .max_decoding_message_size(max_recv)
            .max_encoding_message_size(usize::MAX)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);
        let ing_svc =
            GrpcCompressionEchoService::new_with_limits(ing_inner, max_recv, max_send);
        grpc_routes = grpc_routes.add_service(ing_svc);
    }

    log_module_starting(&*logger, "server");
    let servers = server.start(http_router, grpc_routes)?;
    server_state.set(State::Running);

    Ok::<_, anyhow::Error>((
        memberlist_module,
        memberlist_kv,
        memberlist_svc,
        memberlist_state,
        ring_svc,
        watcher_svc,
        cost_attribution_mgr,
        cost_attribution_state,
        ingester_opt,
        servers,
    ))
    }
    .await;

    let (
        memberlist_module,
        memberlist_kv,
        memberlist_svc,
        memberlist_state,
        ring_svc,
        watcher_svc,
        cost_attribution_mgr,
        cost_attribution_state,
        ingester_opt,
        servers,
    ) = match post_sanity {
        Ok(v) => v,
        Err(e) => {
            cleanup_pre_server_modules(
                &*logger,
                active_groups_timer,
                &active_groups_state,
                usage_stats_svc,
                runtime_config_mgr,
                &cfg.activity_tracker.filepath,
                activity_tracker,
                activity_tracker_state.as_deref(),
                span_exporter_handle.take(),
            )
            .await;
            return Err(e);
        }
    };

    if let Some(ref rt) = runtime_config_mgr {
        log_module_starting(&*logger, "runtime-config");
        rt.start_reloader();
        runtime_config_state.set(State::Running);
    }

    // Cost attribution service (`CostAttributionService` -> `API` -> `Server`): a timer service
    // purging inactive attributions; `IngesterService` depends on it.
    let cost_attribution_timer = cost_attribution_mgr.map(|mgr| {
        log_module_starting(&*logger, "cost-attribution-service");
        let timer = spawn_timer(go_duration_std(cfg.cost_attribution_cleanup_interval), move || {
            mgr.purge_inactive_attributions_until(std::time::SystemTime::now());
        });
        cost_attribution_state.set(State::Running);
        timer
    });

    // 8. Start ring, partition ring, and ingester services while honoring SIGTERM/SIGINT cancellation.
    // Failures of module services (after they started) are reported here, like the service
    // manager's `serviceFailed` listener in Go, which stops the whole application.
    let (module_failed_tx, mut module_failed_rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let server_stopping = Arc::new(AtomicBool::new(false));
    let server_monitor_task = {
        let mut server_stopped_rx = servers.subscribe_stopped();
        let server_stopping_flag = Arc::clone(&server_stopping);
        let server_state_monitor = Arc::clone(&server_state);
        let server_failed_tx = module_failed_tx.clone();
        tokio::spawn(async move {
            if server_stopped_rx.wait_for(|v| *v).await.is_ok()
                && !server_stopping_flag.load(Ordering::Acquire)
            {
                server_state_monitor.set(State::Failed);
                let _ = server_failed_tx.send((
                    "server".to_string(),
                    "server stopped unexpectedly".to_string(),
                ));
            }
        })
    };
    let mut listener_handles = Vec::new();
    for (name, svc) in [
        ("memberlist-kv", &memberlist_svc),
        ("ingester-ring", &ring_svc),
        ("ingester-partitions-ring", &watcher_svc),
        ("usage-stats", &usage_stats_svc),
    ] {
        if let Some(svc) = svc {
            let tx = module_failed_tx.clone();
            let name_s = name.to_string();
            let ml_state = (name == "memberlist-kv").then(|| Arc::clone(&memberlist_state));
            listener_handles.push(svc.add_listener(FnListener::new(
                None,
                None,
                None,
                None,
                Some(Box::new(move |_, err| {
                    if let Some(ref ml_state) = ml_state {
                        ml_state.set(State::Failed);
                    }
                    let _ = tx.send((name_s.clone(), err.to_string()));
                })),
            )));
            if svc.state() == State::Failed {
                let err_s = svc.failure_case().map(|e| e.to_string()).unwrap_or_default();
                let _ = module_failed_tx.send((name.to_string(), err_s));
            }
        }
    }

    let current_startup_module: parking_lot::Mutex<Option<&'static str>> =
        parking_lot::Mutex::new(None);
    let reached_running_set: parking_lot::Mutex<std::collections::HashSet<&'static str>> =
        parking_lot::Mutex::new(std::collections::HashSet::new());

    let (signaled, startup_err, mut runtime_failure) = {
        let startup_fut = async {
            // memberlist-kv: mirrors Go KVInitService — transitions to Running immediately while
            // inner MemberlistKv starts (transport + fast-join) in the background, failing only if
            // MemberlistKv fails. Rings still await MemberlistKv via await_kv_running_or_stopping().
            if memberlist_module {
                *current_startup_module.lock() = Some("memberlist-kv");
                log_module_starting(&*logger, "memberlist-kv");
                memberlist_state.set(State::Running);
            }
            if let Some(ref svc) = memberlist_svc {
                if let Err(e) = svc.start_async(service_ctx.child()).await {
                    memberlist_state.set(State::Failed);
                    return Err(("memberlist-kv".to_string(), anyhow::anyhow!(e)));
                }
            }
            if memberlist_module {
                reached_running_set.lock().insert("memberlist-kv");
            }
            if let Some(ref rsvc) = ring_svc {
                *current_startup_module.lock() = Some("ingester-ring");
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
                reached_running_set.lock().insert("ingester-ring");
            }
            if let Some(ref wsvc) = watcher_svc {
                *current_startup_module.lock() = Some("ingester-partitions-ring");
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
                reached_running_set.lock().insert("ingester-partitions-ring");
            }
            if let Some(ref ing) = ingester_opt {
                *current_startup_module.lock() = Some("ingester-service");
                log_module_starting(&*logger, "ingester-service");
                ing.start().await.map_err(|e| {
                    (
                        "ingester-service".to_string(),
                        anyhow::anyhow!("starting module ingester-service: {e}"),
                    )
                })?;
                reached_running_set.lock().insert("ingester-service");
            }
            *current_startup_module.lock() = None;
            Ok::<(), (String, anyhow::Error)>(())
        };
        await_startup_with_shutdown(
            startup_fut,
            &mut shutdown_sig,
            &shutdown_requested,
            shutdown_delay,
            &mut module_failed_rx,
            &service_ctx,
            Some(&current_startup_module),
        )
        .await
    };

    if !signaled && startup_err.is_none() && runtime_failure.is_none() {
        logger.log(LogLevel::Info, &[("msg", "Application started")]);
        sd_notify("READY=1");
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
    let reached_running = |module: &str| reached_running_set.lock().contains(module);
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
    // and usage-stats. Nothing depends on active-groups-cleanup-service: it stops first.
    if let Some(timer) = active_groups_timer {
        stop_timer_module(&*logger, "active-groups-cleanup-service", &active_groups_state, timer);
    }
    let mut ingester_failed = false;
    if let Some(ref ing) = ingester_opt {
        match ing.stop().await {
            Ok(()) if reached_running("ingester-service") => {
                log_module_stopped(&*logger, "ingester-service")
            }
            Ok(()) => {}
            Err(err) => {
                ingester_failed = true;
                if reached_running("ingester-service") {
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
    if let Some(timer) = cost_attribution_timer {
        stop_timer_module(&*logger, "cost-attribution-service", &cost_attribution_state, timer);
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
    if let Some(rt) = runtime_config_mgr {
        runtime_config_state.set(State::Stopping);
        rt.stop();
        runtime_config_state.set(State::Terminated);
        log_module_stopped(&*logger, "runtime-config");
    }
    // Go KVInitService: a failed inner KV makes `running` return its error, so the module ends
    // up Failed (never Terminated).
    if memberlist_state.get() != State::Failed {
        memberlist_state.set(State::Stopping);
    }
    if server_state.get() != State::Failed {
        server_state.set(State::Stopping);
    }

    // `MemberlistKV` depends on `API` -> `Server`: memberlist leaves the cluster and stops
    // before the HTTP/gRPC servers.
    let memberlist_failure = memberlist_svc
        .as_ref()
        .filter(|svc| svc.state() == State::Failed)
        .map(|svc| svc.failure_case().map(|e| e.to_string()).unwrap_or_default());
    if let Some(ml) = memberlist_kv {
        ml.shutdown().await;
    }
    if memberlist_failure.is_some() || memberlist_state.get() == State::Failed {
        memberlist_state.set(State::Failed);
    } else {
        memberlist_state.set(State::Terminated);
    }
    if memberlist_module && reached_running("memberlist-kv") {
        match memberlist_failure {
            Some(err_s) => logger.log(
                LogLevel::Warn,
                &[("msg", "module failed with error"), ("module", "memberlist-kv"), ("err", &err_s)],
            ),
            None => log_module_stopped(&*logger, "memberlist-kv"),
        }
    }

    server_stopping.store(true, Ordering::Release);
    server_monitor_task.abort();
    servers.shutdown().await;
    if server_state.get() == State::Failed {
        logger.log(
            LogLevel::Warn,
            &[
                ("msg", "module failed with error"),
                ("module", "server"),
                ("err", "server stopped unexpectedly"),
            ],
        );
    } else {
        server_state.set(State::Terminated);
        // dskit server_service.go
        logger.log(LogLevel::Info, &[("msg", "server stopped")]);
        log_module_stopped(&*logger, "server");
    }
    // `Server` depends on `SanityCheck` and `UsageStats`: they stop after it.
    sanity_state.set(State::Terminated);
    log_module_stopped(&*logger, "sanity-check");
    if let Some(svc) = usage_stats_svc {
        stop_module_service(&*logger, "usage-stats", &svc, true).await;
    }

    stop_activity_tracker_module(
        &*logger,
        &cfg.activity_tracker.filepath,
        activity_tracker,
        activity_tracker_state.as_deref(),
    );
    if let Some(h) = span_exporter_handle.take() {
        h.shutdown(Duration::from_secs(5), logger.as_ref()).await;
    }

    logger.log(LogLevel::Info, &[("msg", "Application stopped")]);
    sd_notify("STOPPING=1");
    // Go Mimir.Run suppresses "failed services" whenever a shutdown signal was received
    // (shutdownRequested == true), exiting 0 instead of 1.
    if signaled || shutdown_requested.load(Ordering::Acquire) {
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

pub fn validate_activity_tracker_max_entries(max_entries: i64) -> Result<usize> {
    mimir_common::activitytracker::validate_raw_max_entries(max_entries)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn stop_activity_tracker_module(
    logger: &dyn Logger,
    filepath: &str,
    activity_tracker: Option<Arc<ActivityTracker>>,
    state: Option<&StateCell>,
) {
    if let Some(at) = activity_tracker {
        if !filepath.is_empty() {
            let (entries, err) = load_unfinished_entries_partial(filepath);
            if let Some(err) = err {
                let err_s = err.to_string();
                logger.log(
                    LogLevel::Warn,
                    &[
                        ("component", "activity-tracker"),
                        (
                            "msg",
                            "failed to fully read file with unfinished activities during shutdown",
                        ),
                        ("err", &err_s),
                    ],
                );
            }
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
        let _ = at.flush();
        if let Some(st) = state {
            st.set(State::Terminated);
            log_module_stopped(logger, "activity-tracker");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn cleanup_pre_server_modules(
    logger: &dyn Logger,
    active_groups_timer: Option<Option<tokio::task::JoinHandle<()>>>,
    active_groups_state: &StateCell,
    usage_stats_svc: Option<Arc<ManagedBasicService>>,
    runtime_config_mgr: Option<Arc<RuntimeConfigManager>>,
    activity_tracker_filepath: &str,
    activity_tracker: Option<Arc<ActivityTracker>>,
    activity_tracker_state: Option<&StateCell>,
    span_exporter_handle: Option<SpanExporterHandle>,
) {
    if let Some(timer) = active_groups_timer {
        stop_timer_module(
            logger,
            "active-groups-cleanup-service",
            active_groups_state,
            timer,
        );
    }
    if let Some(svc) = usage_stats_svc {
        stop_module_service(logger, "usage-stats", &svc, true).await;
    }
    if let Some(rt) = runtime_config_mgr {
        rt.stop();
    }
    stop_activity_tracker_module(
        logger,
        activity_tracker_filepath,
        activity_tracker,
        activity_tracker_state,
    );
    if let Some(h) = span_exporter_handle {
        h.shutdown(Duration::from_secs(5), logger).await;
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

fn go_duration_std(d: mimir_config::GoDuration) -> Duration {
    Duration::from_nanos(d.0.max(0) as u64)
}

/// Runs `f` every `interval` (first run after one interval), like dskit's `NewTimerService`.
/// Returns the timer task (`None` for a zero interval), aborted when the module stops.
fn spawn_timer(
    interval: Duration,
    f: impl Fn() + Send + 'static,
) -> Option<tokio::task::JoinHandle<()>> {
    if interval.is_zero() {
        return None;
    }
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            f();
        }
    }))
}

/// Stops a dskit timer-service module (`services.NewTimerService`) and logs it like
/// modules/module_service.go stop().
fn stop_timer_module(
    logger: &dyn Logger,
    module: &str,
    state: &StateCell,
    timer: Option<tokio::task::JoinHandle<()>>,
) {
    if let Some(timer) = timer {
        timer.abort();
    }
    state.set(State::Terminated);
    log_module_stopped(logger, module);
}

/// Sends a systemd `sd_notify` datagram (e.g. `READY=1`, `STOPPING=1`) to `NOTIFY_SOCKET`,
/// matching `github.com/okzk/sdnotify.SdNotify` called in `pkg/mimir/mimir.go`.
/// Best-effort: silently no-ops if `NOTIFY_SOCKET` is unset or unreachable.
fn sd_notify(state: &str) {
    #[cfg(unix)]
    {
        let Ok(socket_path) = std::env::var("NOTIFY_SOCKET") else {
            return;
        };
        if socket_path.is_empty() {
            return;
        }
        let Ok(sock) = std::os::unix::net::UnixDatagram::unbound() else {
            return;
        };
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if let Some(abstract_name) = socket_path.strip_prefix('@') {
            use std::os::linux::net::SocketAddrExt;
            if let Ok(addr) = std::os::unix::net::SocketAddr::from_abstract_name(abstract_name) {
                let _ = sock.send_to_addr(state.as_bytes(), &addr);
            }
            return;
        }
        let _ = sock.send_to(state.as_bytes(), &socket_path);
    }
    #[cfg(not(unix))]
    {
        let _ = state;
    }
}

struct SecondSignalGuard {
    second_task: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    sigquit_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for SecondSignalGuard {
    fn drop(&mut self) {
        if let Some(h) = self.second_task.lock().take() {
            h.abort();
        }
        if let Some(h) = self.sigquit_task.take() {
            h.abort();
        }
    }
}

type ShutdownSignalFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

fn install_shutdown_signal(logger: Arc<LogSink>) -> Result<(ShutdownSignalFuture, SecondSignalGuard)> {
    let second_task = Arc::new(parking_lot::Mutex::new(None));

    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("install SIGTERM handler")?;
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .context("install SIGINT handler")?;
        // dskit signals/signals.go Handler.Loop(): catch SIGQUIT, log, and continue running
        // instead of terminating the process with a core dump.
        let mut sigquit = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::quit())
            .context("install SIGQUIT handler")?;
        let quit_logger = Arc::clone(&logger);
        let sigquit_task = Some(tokio::spawn(async move {
            while sigquit.recv().await.is_some() {
                quit_logger.log(
                    LogLevel::Info,
                    &[(
                        "msg",
                        "=== received SIGQUIT ===\n*** goroutine dump...\n(rust process; see /debug/pprof/profile for stack profiles)\n*** end",
                    )],
                );
            }
        }));
        let guard = SecondSignalGuard {
            second_task: Arc::clone(&second_task),
            sigquit_task,
        };
        let fut = Box::pin(async move {
            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
            }
            // dskit signals/signals.go Handler.Loop()
            logger.log(
                LogLevel::Info,
                &[("msg", "=== received SIGINT/SIGTERM ===\n*** exiting")],
            );
            // Keep signal receivers alive and reset to SIG_DFL + re-raise on a 2nd signal,
            // matching Go signals/signals.go (`defer signal.Stop(sigs)`).
            let h = tokio::spawn(async move {
                let sig = tokio::select! {
                    Some(_) = sigterm.recv() => libc::SIGTERM,
                    Some(_) = sigint.recv() => libc::SIGINT,
                    else => return,
                };
                // SAFETY: Resetting signal disposition to SIG_DFL and re-raising the caught
                // signal terminates the process immediately with the signal status.
                unsafe {
                    libc::signal(sig, libc::SIG_DFL);
                    libc::raise(sig);
                }
                std::process::exit(128 + sig);
            });
            *second_task.lock() = Some(h);
        });
        Ok((fut, guard))
    }

    #[cfg(not(unix))]
    {
        let guard = SecondSignalGuard {
            second_task: Arc::clone(&second_task),
            sigquit_task: None,
        };
        let fut = Box::pin(async move {
            let _ = tokio::signal::ctrl_c().await;
            logger.log(
                LogLevel::Info,
                &[("msg", "=== received SIGINT/SIGTERM ===\n*** exiting")],
            );
        });
        Ok((fut, guard))
    }
}


fn span_attr_to_otlp_json(a: &mimir_common::tracing::SpanAttribute) -> serde_json::Value {
    let v = match &a.value {
        SpanAttrValue::String(s) => serde_json::json!({ "stringValue": s }),
        SpanAttrValue::Bool(b) => serde_json::json!({ "boolValue": b }),
        SpanAttrValue::Int(i) => serde_json::json!({ "intValue": i.to_string() }),
        SpanAttrValue::Float(f) => serde_json::json!({ "doubleValue": f }),
        SpanAttrValue::StringArray(arr) => serde_json::json!({
            "arrayValue": {
                "values": arr.iter().map(|s| serde_json::json!({ "stringValue": s })).collect::<Vec<_>>()
            }
        }),
        SpanAttrValue::Links(_) | SpanAttrValue::Events(_) => serde_json::json!({}),
    };
    serde_json::json!({ "key": a.key, "value": v })
}

pub fn span_to_otlp_json_with_resource(
    span: &ExportedSpan,
    extra_resource_attrs: &[mimir_common::tracing::SpanAttribute],
) -> serde_json::Value {
    let hex_bytes = |b: &[u8]| -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::with_capacity(b.len() * 2);
        for &x in b {
            s.push(HEX[(x >> 4) as usize] as char);
            s.push(HEX[(x & 0x0f) as usize] as char);
        }
        s
    };
    let attrs: Vec<serde_json::Value> = span
        .regular_attributes()
        .map(span_attr_to_otlp_json)
        .collect();
    let mut span_obj = serde_json::json!({
        "traceId": hex_bytes(&span.trace_id),
        "spanId": hex_bytes(&span.span_id),
        "name": span.name,
        "kind": span.kind,
        "startTimeUnixNano": span.start_time_unix_nano.to_string(),
        "endTimeUnixNano": span.end_time_unix_nano.to_string(),
        "attributes": attrs,
        "status": { "code": if span.status_error { 2 } else { 0 } },
    });
    if let Some(pid) = span.parent_span_id {
        if let Some(obj) = span_obj.as_object_mut() {
            obj.insert("parentSpanId".to_string(), serde_json::json!(hex_bytes(&pid)));
        }
    }
    let links = span.links();
    if !links.is_empty() {
        let json_links: Vec<serde_json::Value> = links
            .iter()
            .map(|l| {
                let mut lo = serde_json::json!({
                    "traceId": hex_bytes(&l.trace_id),
                    "spanId": hex_bytes(&l.span_id),
                });
                if let Some(ref ts) = l.trace_state {
                    if !ts.is_empty() {
                        if let Some(obj) = lo.as_object_mut() {
                            obj.insert("traceState".to_string(), serde_json::json!(ts));
                        }
                    }
                }
                if !l.attributes.is_empty() {
                    if let Some(obj) = lo.as_object_mut() {
                        obj.insert(
                            "attributes".to_string(),
                            serde_json::Value::Array(
                                l.attributes.iter().map(span_attr_to_otlp_json).collect(),
                            ),
                        );
                    }
                }
                lo
            })
            .collect();
        if let Some(obj) = span_obj.as_object_mut() {
            obj.insert("links".to_string(), serde_json::Value::Array(json_links));
        }
    }
    let events = span.events();
    if !events.is_empty() {
        let json_events: Vec<serde_json::Value> = events
            .iter()
            .map(|e| {
                let ev_attrs: Vec<serde_json::Value> =
                    e.attributes.iter().map(span_attr_to_otlp_json).collect();
                serde_json::json!({
                    "timeUnixNano": e.time_unix_nano.to_string(),
                    "name": e.name,
                    "attributes": ev_attrs,
                })
            })
            .collect();
        if let Some(obj) = span_obj.as_object_mut() {
            obj.insert("events".to_string(), serde_json::Value::Array(json_events));
        }
    }
    let mut resource_attrs = Vec::with_capacity(1 + extra_resource_attrs.len());
    resource_attrs.push(serde_json::json!({
        "key": "service.name",
        "value": { "stringValue": span.service_name }
    }));
    for attr in extra_resource_attrs {
        if attr.key != "service.name" {
            resource_attrs.push(span_attr_to_otlp_json(attr));
        }
    }
    serde_json::json!({
        "resourceSpans": [{
            "resource": {
                "attributes": resource_attrs
            },
            "scopeSpans": [{
                "scope": { "name": "mimir-rs" },
                "spans": [span_obj]
            }]
        }]
    })
}

pub fn span_to_otlp_json(span: &ExportedSpan) -> serde_json::Value {
    let extra = mimir_common::tracing::global_tracing()
        .map(|rt| rt.resource_attributes.as_slice())
        .unwrap_or(&[]);
    span_to_otlp_json_with_resource(span, extra)
}

pub struct SpanExporterHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl SpanExporterHandle {
    pub async fn shutdown(mut self, timeout: Duration, logger: &dyn Logger) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            if tokio::time::timeout(timeout, join).await.is_err() {
                logger.log(
                    LogLevel::Error,
                    &[
                        ("msg", "OpenTelemetry trace provider failed to shutdown"),
                        ("err", "context deadline exceeded"),
                    ],
                );
            }
        }
    }
}

#[derive(Clone)]
enum SpanTransport {
    OtlpHttpProtobuf {
        client: reqwest::Client,
        endpoint: String,
    },
    OtlpHttpJson {
        client: reqwest::Client,
        endpoint: String,
    },
    OtlpGrpc {
        client: reqwest::Client,
        export_url: String,
    },
    JaegerHttpThrift {
        client: reqwest::Client,
        endpoint: String,
        auth: Option<(String, String)>,
    },
    JaegerUdpAgent {
        socket: Arc<tokio::net::UdpSocket>,
        addr: std::net::SocketAddr,
    },
}

async fn send_exported_span(transport: &SpanTransport, span: &ExportedSpan) {
    let extra = global_tracing()
        .map(|rt| rt.resource_attributes.as_slice())
        .unwrap_or(&[]);
    match transport {
        SpanTransport::OtlpHttpProtobuf { client, endpoint } => {
            let body = encode_otlp_trace_request_protobuf(span, extra);
            let _ = client
                .post(endpoint)
                .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
                .body(body)
                .send()
                .await;
        }
        SpanTransport::OtlpHttpJson { client, endpoint } => {
            let body = span_to_otlp_json_with_resource(span, extra);
            let _ = client.post(endpoint).json(&body).send().await;
        }
        SpanTransport::OtlpGrpc { client, export_url } => {
            let pb = encode_otlp_trace_request_protobuf(span, extra);
            let framed = encode_otlp_grpc_frame(&pb);
            let _ = client
                .post(export_url)
                .header(reqwest::header::CONTENT_TYPE, "application/grpc")
                .header("te", "trailers")
                .body(framed)
                .send()
                .await;
        }
        SpanTransport::JaegerHttpThrift {
            client,
            endpoint,
            auth,
        } => {
            let body = encode_jaeger_thrift_batch_binary(span, extra);
            let mut req = client
                .post(endpoint)
                .header(reqwest::header::CONTENT_TYPE, "application/x-thrift")
                .body(body);
            if let Some((ref user, ref pass)) = auth {
                req = req.basic_auth(user, Some(pass));
            }
            let _ = req.send().await;
        }
        SpanTransport::JaegerUdpAgent { socket, addr } => {
            let pkt = encode_jaeger_agent_emit_batch_compact(span, extra);
            if pkt.len() <= 65000 {
                let _ = socket.send_to(&pkt, *addr).await;
            }
        }
    }
}

fn build_otlp_http_client(
    is_jaeger: bool,
    http2_prior_knowledge: bool,
) -> Result<reqwest::Client, String> {
    let timeout = otlp_timeout_from_env().unwrap_or(Duration::from_secs(10));
    let mut builder = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(timeout);
    if http2_prior_knowledge {
        builder = builder.http2_prior_knowledge();
    }
    if !is_jaeger {
        let hdrs = otlp_headers_from_env();
        if !hdrs.is_empty() {
            let mut header_map = reqwest::header::HeaderMap::new();
            for (k, v) in hdrs {
                if let (Ok(name), Ok(val)) = (
                    reqwest::header::HeaderName::from_bytes(k.as_bytes()),
                    reqwest::header::HeaderValue::from_bytes(v.as_bytes()),
                ) {
                    header_map.insert(name, val);
                }
            }
            builder = builder.default_headers(header_map);
        }
    }
    let tls_paths = otlp_tls_paths_from_env();
    if let Some(ref ca_path) = tls_paths.ca_cert_path {
        let pem = std::fs::read(ca_path)
            .map_err(|e| format!("reading OTLP CA certificate {ca_path:?}: {e}"))?;
        use rustls_pki_types::pem::PemObject as _;
        for der in rustls_pki_types::CertificateDer::pem_slice_iter(&pem).filter_map(Result::ok) {
            let cert = reqwest::Certificate::from_der(der.as_ref())
                .map_err(|e| format!("parsing OTLP CA certificate {ca_path:?}: {e}"))?;
            builder = builder.add_root_certificate(cert);
        }
    }
    if let (Some(ref cert_path), Some(ref key_path)) =
        (tls_paths.client_cert_path, tls_paths.client_key_path)
    {
        let mut combined = std::fs::read(cert_path)
            .map_err(|e| format!("reading OTLP client certificate {cert_path:?}: {e}"))?;
        let key_pem = std::fs::read(key_path)
            .map_err(|e| format!("reading OTLP client key {key_path:?}: {e}"))?;
        combined.push(b'\n');
        combined.extend_from_slice(&key_pem);
        let id = reqwest::Identity::from_pem(&combined).map_err(|e| {
            format!("parsing OTLP client identity ({cert_path:?}, {key_path:?}): {e}")
        })?;
        builder = builder.identity(id);
    }
    builder
        .build()
        .map_err(|e| format!("building OTLP/Jaeger HTTP client: {:#}", anyhow::Error::new(e)))
}

pub fn try_build_span_exporter_with_handle(
) -> Result<(Option<SpanExporterFn>, Option<SpanExporterHandle>), String> {
    let Some(kind) = detected_exporter_kind() else {
        return Ok((None, None));
    };
    let kind_trimmed = kind.trim();
    let transport = match kind_trimmed.to_ascii_lowercase().as_str() {
        "none" => return Ok((None, None)),
        "console" => {
            return Ok((
                Some(Arc::new(|span: ExportedSpan| {
                    let payload = span_to_otlp_json(&span);
                    println!("{payload}");
                })),
                None,
            ));
        }
        "jaeger" => {
            let Some(target) = jaeger_target_from_env()? else {
                return Ok((None, None));
            };
            match target {
                JaegerTarget::HttpCollector(endpoint) => {
                    let client = build_otlp_http_client(true, false)?;
                    let auth = jaeger_basic_auth_from_env();
                    SpanTransport::JaegerHttpThrift {
                        client,
                        endpoint,
                        auth,
                    }
                }
                JaegerTarget::UdpAgent { host, port } => {
                    use std::net::ToSocketAddrs as _;
                    let _rt_guard = tokio::runtime::Handle::try_current()
                        .map_err(|e| format!("no active tokio runtime for span exporter: {e}"))?;
                    let port_num: u16 = port
                        .parse()
                        .map_err(|e| format!("invalid JAEGER_AGENT_PORT={port:?}: {e}"))?;
                    let addr = (host.as_str(), port_num)
                        .to_socket_addrs()
                        .map_err(|e| format!("resolving Jaeger agent {host}:{port}: {e}"))?
                        .next()
                        .ok_or_else(|| {
                            format!("no address resolved for Jaeger agent {host}:{port}")
                        })?;
                    let bind_addr = if addr.is_ipv6() { "[::]:0" } else { "0.0.0.0:0" };
                    let std_sock = std::net::UdpSocket::bind(bind_addr)
                        .map_err(|e| format!("binding Jaeger UDP socket: {e}"))?;
                    std_sock
                        .set_nonblocking(true)
                        .map_err(|e| format!("configuring Jaeger UDP socket: {e}"))?;
                    let socket = tokio::net::UdpSocket::from_std(std_sock)
                        .map_err(|e| format!("wrapping Jaeger UDP socket: {e}"))?;
                    SpanTransport::JaegerUdpAgent {
                        socket: Arc::new(socket),
                        addr,
                    }
                }
            }
        }
        "otlp" => {
            let proto = otlp_protocol_from_env()?;
            let Some(endpoint) = otlp_traces_endpoint_for_protocol(proto) else {
                return Ok((None, None));
            };
            if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                return Err(format!("invalid OTLP endpoint {endpoint:?}"));
            }
            match proto {
                OtlpProtocol::HttpProtobuf => {
                    let client = build_otlp_http_client(false, false)?;
                    SpanTransport::OtlpHttpProtobuf { client, endpoint }
                }
                OtlpProtocol::HttpJson => {
                    let client = build_otlp_http_client(false, false)?;
                    SpanTransport::OtlpHttpJson { client, endpoint }
                }
                OtlpProtocol::Grpc => {
                    let h2_prior = endpoint.starts_with("http://");
                    let client = build_otlp_http_client(false, h2_prior)?;
                    let base = endpoint.trim_end_matches('/');
                    let export_url = format!(
                        "{base}/opentelemetry.proto.collector.trace.v1.TraceService/Export"
                    );
                    SpanTransport::OtlpGrpc { client, export_url }
                }
            }
        }
        _ => {
            return Err(format!(
                "unknown exporter or metrics producer {kind_trimmed:?}"
            ));
        }
    };

    let handle = tokio::runtime::Handle::try_current()
        .map_err(|e| format!("no active tokio runtime for span exporter: {e}"))?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ExportedSpan>(4096);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let join = handle.spawn(async move {
        let mut shutdown_rx = Some(shutdown_rx);
        loop {
            tokio::select! {
                biased;
                maybe_span = rx.recv() => {
                    match maybe_span {
                        Some(span) => send_exported_span(&transport, &span).await,
                        None => break,
                    }
                }
                res = async {
                    match &mut shutdown_rx {
                        Some(s_rx) => s_rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    match res {
                        Ok(()) => {
                            rx.close();
                            while let Some(span) = rx.recv().await {
                                send_exported_span(&transport, &span).await;
                            }
                            break;
                        }
                        Err(_) => {
                            // SpanExporterHandle was dropped without calling shutdown()
                            // (e.g. via try_build_span_exporter / build_span_exporter); keep
                            // draining spans until all SpanExporterFn senders drop.
                            shutdown_rx = None;
                        }
                    }
                }
            }
        }
    });
    Ok((
        Some(Arc::new(move |span: ExportedSpan| {
            let _ = tx.try_send(span);
        })),
        Some(SpanExporterHandle {
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
        }),
    ))
}

pub fn try_build_span_exporter() -> Result<Option<SpanExporterFn>, String> {
    try_build_span_exporter_with_handle().map(|(exp, _)| exp)
}

async fn await_startup_with_shutdown<F, S>(
    startup_fut: F,
    shutdown_sig: &mut S,
    shutdown_requested: &AtomicBool,
    shutdown_delay: Duration,
    module_failed_rx: &mut tokio::sync::mpsc::UnboundedReceiver<(String, String)>,
    service_ctx: &ServiceContext,
    current_module: Option<&parking_lot::Mutex<Option<&'static str>>>,
) -> (bool, Option<(String, anyhow::Error)>, Option<(String, String)>)
where
    F: std::future::Future<Output = std::result::Result<(), (String, anyhow::Error)>>,
    S: std::future::Future<Output = ()> + Unpin,
{
    tokio::pin!(startup_fut);
    tokio::select! {
        res = &mut startup_fut => (false, res.err(), None),
        Some(failure) = module_failed_rx.recv() => {
            service_ctx.cancel();
            (false, None, Some(failure))
        }
        _ = shutdown_sig => {
            // Go keeps starting modules during -shutdown-delay, then stops the manager.
            shutdown_requested.store(true, Ordering::Release);
            let delay_deadline = tokio::time::Instant::now() + shutdown_delay;
            tokio::select! {
                _ = tokio::time::sleep_until(delay_deadline) => {
                    let aborted = current_module.and_then(|m| *m.lock()).map(|m| {
                        (
                            m.to_string(),
                            anyhow::anyhow!("starting module {m}: context canceled"),
                        )
                    });
                    (true, aborted, None)
                }
                res = &mut startup_fut => {
                    match res {
                        Ok(()) => {
                            tokio::select! {
                                _ = tokio::time::sleep_until(delay_deadline) => {}
                                Some(failure) = module_failed_rx.recv() => {
                                    service_ctx.cancel();
                                    return (true, None, Some(failure));
                                }
                            }
                            (true, None, None)
                        }
                        Err(e) => (true, Some(e), None),
                    }
                }
                Some(failure) = module_failed_rx.recv() => {
                    service_ctx.cancel();
                    (true, None, Some(failure))
                }
            }
        }
    }
}

pub fn build_span_exporter() -> Option<SpanExporterFn> {
    match try_build_span_exporter() {
        Ok(exp) => exp,
        Err(err) => {
            tracing::warn!(%err, "failed to build span exporter");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_targets(targets: &[&str]) -> MimirConfig {
        let mut cfg = MimirConfig::default();
        cfg.target.0 = targets.iter().map(|t| t.to_string()).collect();
        cfg
    }

    #[test]
    fn unimplemented_targets_are_rejected() {
        assert!(validate_supported_targets(&with_targets(&["ingester"])).is_ok());
        assert!(validate_supported_targets(&with_targets(&["ingester", "memberlist-kv"])).is_ok());
        let err = validate_supported_targets(&with_targets(&["all"])).unwrap_err().to_string();
        assert_eq!(
            err,
            "target module \"all\" is not yet implemented in mimir-rs; supported targets: ingester, memberlist-kv"
        );
        let err = validate_supported_targets(&with_targets(&["ingester", "distributor"]))
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("target module \"distributor\" is not yet implemented"), "{err}");
    }

    #[tokio::test]
    async fn c04_run_mimir_rejects_non_positive_replication_factor() {
        for rf in [0, -1] {
            let dir = tempfile::tempdir().unwrap();
            let args = vec![
                "-target=ingester".to_string(),
                "-server.http-listen-address=127.0.0.1".to_string(),
                "-server.http-listen-port=0".to_string(),
                "-server.grpc-listen-address=127.0.0.1".to_string(),
                "-server.grpc-listen-port=0".to_string(),
                "-ingester.ring.store=inmemory".to_string(),
                "-ingester.ring.instance-id=c04-test".to_string(),
                "-ingester.ring.instance-addr=127.0.0.1".to_string(),
                "-activity-tracker.filepath=".to_string(),
                format!("-ingester.ring.replication-factor={rf}"),
                format!("-blocks-storage.tsdb.dir={}", dir.path().display()),
            ];
            let outcome = crate::cli::parse_cli_for_run("mimir", &args);
            assert_eq!(outcome.exit_code, 0, "stderr: {}", outcome.stderr);
            let err = run_mimir(outcome).await.unwrap_err();
            let chain = format!("{err:#}");
            assert!(
                chain.contains(&format!("ReplicationFactor must be greater than zero: {rf}")),
                "expected replication factor error for {rf}, got: {chain}"
            );
        }
    }

    #[tokio::test]
    async fn d2_06_shutdown_during_startup_sleeps_only_remaining_shutdown_delay() {
        let shutdown_requested = AtomicBool::new(false);
        let shutdown_delay = Duration::from_millis(120);

        let startup_fut = async {
            // Finishes after 80ms (> 0 and < shutdown_delay=120ms) with Ok(()).
            tokio::time::sleep(Duration::from_millis(80)).await;
            Ok::<(), (String, anyhow::Error)>(())
        };
        let mut sig = Box::pin(async {
            // Fires 10ms after startup begins.
            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        let (_tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let service_ctx = ServiceContext::background();
        let start = tokio::time::Instant::now();
        let (signaled, err, bg_failure) = await_startup_with_shutdown(
            startup_fut,
            &mut sig,
            &shutdown_requested,
            shutdown_delay,
            &mut rx,
            &service_ctx,
            None,
        )
        .await;
        let elapsed = start.elapsed();

        assert!(signaled);
        assert!(err.is_none());
        assert!(bg_failure.is_none());
        assert!(shutdown_requested.load(Ordering::Acquire));
        // Signal fired at ~10ms, delay_deadline is ~130ms. Startup finished at ~80ms, so we must
        // sleep only the remaining ~50ms — total ~130ms, well below 2*shutdown_delay (240ms)!
        assert!(
            elapsed >= Duration::from_millis(115),
            "must wait full shutdown_delay from signal ({elapsed:?})"
        );
        assert!(
            elapsed < Duration::from_millis(190),
            "must NOT sleep 2 * shutdown_delay ({elapsed:?})"
        );
    }

    #[tokio::test]
    async fn cmd_06_await_startup_preserves_error_on_signal_during_shutdown_delay() {
        let shutdown_requested = AtomicBool::new(false);
        let shutdown_delay = Duration::from_millis(200);
        let current_module = parking_lot::Mutex::new(None::<&'static str>);
        let reached_running_set = parking_lot::Mutex::new(std::collections::HashSet::<&'static str>::new());

        let startup_fut = async {
            *current_module.lock() = Some("memberlist-kv");
            reached_running_set.lock().insert("memberlist-kv");
            *current_module.lock() = Some("ingester-ring");
            tokio::time::sleep(Duration::from_millis(40)).await;
            Err::<(), (String, anyhow::Error)>((
                "ingester-ring".to_string(),
                anyhow::anyhow!("ring init failed during shutdown delay"),
            ))
        };
        let mut sig = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        let (_tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let service_ctx = ServiceContext::background();
        let (signaled, err, bg_failure) = await_startup_with_shutdown(
            startup_fut,
            &mut sig,
            &shutdown_requested,
            shutdown_delay,
            &mut rx,
            &service_ctx,
            Some(&current_module),
        )
        .await;

        assert!(signaled);
        assert!(bg_failure.is_none());
        let (mod_name, err) = err.expect("startup error during shutdown_delay must be preserved");
        assert_eq!(mod_name, "ingester-ring");
        assert!(err.to_string().contains("ring init failed"), "{err}");
        let running = reached_running_set.lock();
        assert!(running.contains("memberlist-kv"));
        assert!(!running.contains("ingester-ring"));
        assert!(!running.contains("ingester-service"));
    }

    #[tokio::test]
    async fn cmd_06_await_startup_synthesizes_canceled_module_when_shutdown_delay_expires() {
        let shutdown_requested = AtomicBool::new(false);
        let shutdown_delay = Duration::from_millis(30);
        let current_module = parking_lot::Mutex::new(None::<&'static str>);
        let reached_running_set = parking_lot::Mutex::new(std::collections::HashSet::<&'static str>::new());

        let startup_fut = async {
            *current_module.lock() = Some("memberlist-kv");
            reached_running_set.lock().insert("memberlist-kv");
            *current_module.lock() = Some("ingester-service");
            std::future::pending::<()>().await;
            Ok::<(), (String, anyhow::Error)>(())
        };
        let mut sig = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
        });

        let (_tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let service_ctx = ServiceContext::background();
        let (signaled, err, bg_failure) = await_startup_with_shutdown(
            startup_fut,
            &mut sig,
            &shutdown_requested,
            shutdown_delay,
            &mut rx,
            &service_ctx,
            Some(&current_module),
        )
        .await;

        assert!(signaled);
        assert!(bg_failure.is_none());
        let (mod_name, err) = err.expect("canceled in-flight module must be reported");
        assert_eq!(mod_name, "ingester-service");
        assert!(err.to_string().contains("context canceled"), "{err}");
        let running = reached_running_set.lock();
        assert!(running.contains("memberlist-kv"));
        assert!(!running.contains("ingester-service"));
    }

    #[tokio::test]
    async fn cmd_04_await_startup_aborts_immediately_on_background_module_failure() {
        let shutdown_requested = AtomicBool::new(false);
        let shutdown_delay = Duration::from_secs(5);
        let current_module = parking_lot::Mutex::new(None::<&'static str>);

        let startup_fut = async {
            *current_module.lock() = Some("ingester-service");
            std::future::pending::<()>().await;
            Ok::<(), (String, anyhow::Error)>(())
        };
        let mut sig = Box::pin(std::future::pending::<()>());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let service_ctx = ServiceContext::background();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tx.send((
                "server".to_string(),
                "server stopped unexpectedly".to_string(),
            ));
        });

        let start = tokio::time::Instant::now();
        let (signaled, err, bg_failure) = await_startup_with_shutdown(
            startup_fut,
            &mut sig,
            &shutdown_requested,
            shutdown_delay,
            &mut rx,
            &service_ctx,
            Some(&current_module),
        )
        .await;

        assert!(!signaled);
        assert!(err.is_none());
        assert_eq!(
            bg_failure,
            Some((
                "server".to_string(),
                "server stopped unexpectedly".to_string()
            ))
        );
        assert!(service_ctx.is_canceled(), "service_ctx must be canceled on background failure");
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn cmd_04_running_servers_subscribe_stopped_notifies_on_shutdown() {
        struct NoopLogger;
        impl Logger for NoopLogger {
            fn log(&self, _level: LogLevel, _kvs: &[(&str, &str)]) {}
        }
        let cfg = crate::server::Server::new(
            &mimir_config::server::ServerConfig {
                http_listen_address: "127.0.0.1".into(),
                http_listen_port: 0,
                grpc_listen_address: "127.0.0.1".into(),
                grpc_listen_port: 0,
                ..Default::default()
            },
            &Registry::new(),
            Arc::new(NoopLogger),
        )
        .unwrap();
        let running = cfg
            .start(axum::Router::new(), tonic::service::Routes::default())
            .unwrap();
        let mut stopped_rx = running.subscribe_stopped();
        assert!(!*stopped_rx.borrow());
        running.shutdown().await;
        stopped_rx.wait_for(|v| *v).await.unwrap();
        assert!(*stopped_rx.borrow());
    }

    static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test]
    async fn build_span_exporter_stays_alive_across_async_yields() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let received_srv = Arc::clone(&received);

        let srv = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let rec = Arc::clone(&received_srv);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        rec.fetch_add(1, Ordering::SeqCst);
                        let _ = sock
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                            .await;
                    }
                });
            }
        });

        let exp = {
            let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            let prev_exp = std::env::var("OTEL_TRACES_EXPORTER").ok();
            let prev_proto = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok();
            let prev_ep = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").ok();

            std::env::set_var("OTEL_TRACES_EXPORTER", "OTLP");
            std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf");
            std::env::set_var(
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                format!("http://{addr}/v1/traces"),
            );

            let exp = build_span_exporter().expect("exporter must build");

            match prev_exp {
                Some(v) => std::env::set_var("OTEL_TRACES_EXPORTER", v),
                None => std::env::remove_var("OTEL_TRACES_EXPORTER"),
            }
            match prev_proto {
                Some(v) => std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", v),
                None => std::env::remove_var("OTEL_EXPORTER_OTLP_PROTOCOL"),
            }
            match prev_ep {
                Some(v) => std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", v),
                None => std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            }
            exp
        };

        // Yield several times so the dropped SpanExporterHandle oneshot cancellation
        // is guaranteed to have been polled before we emit spans.
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        for i in 0..3u8 {
            exp(ExportedSpan {
                service_name: "mimir-test".to_string(),
                trace_id: [0x11; 16],
                span_id: [i + 1; 8],
                parent_span_id: None,
                name: format!("span-{i}"),
                kind: 2,
                start_time_unix_nano: 1_000_000,
                end_time_unix_nano: 2_000_000,
                attributes: Vec::new(),
                status_error: false,
            });
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        drop(exp);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while received.load(Ordering::SeqCst) < 3 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "expected 3 exported spans, got {}",
                received.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        srv.abort();
    }

    #[test]
    fn jaeger_udp_agent_without_tokio_runtime_returns_err_instead_of_panicking() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev_host = std::env::var("JAEGER_AGENT_HOST").ok();
        let prev_exp = std::env::var("OTEL_TRACES_EXPORTER").ok();
        std::env::set_var("JAEGER_AGENT_HOST", "127.0.0.1");
        std::env::remove_var("OTEL_TRACES_EXPORTER");

        let res = try_build_span_exporter_with_handle();

        match prev_host {
            Some(v) => std::env::set_var("JAEGER_AGENT_HOST", v),
            None => std::env::remove_var("JAEGER_AGENT_HOST"),
        }
        match prev_exp {
            Some(v) => std::env::set_var("OTEL_TRACES_EXPORTER", v),
            None => std::env::remove_var("OTEL_TRACES_EXPORTER"),
        }

        let err = match res {
            Ok(_) => panic!("expected Err outside tokio runtime"),
            Err(e) => e,
        };
        assert!(err.contains("no active tokio runtime"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn sd_notify_sends_ready_and_stopping_to_notify_socket() {
        let _guard = TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("notify.sock");
        let receiver = std::os::unix::net::UnixDatagram::bind(&sock_path).unwrap();
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let prev = std::env::var("NOTIFY_SOCKET").ok();
        std::env::set_var("NOTIFY_SOCKET", &sock_path);
        sd_notify("READY=1");
        sd_notify("STOPPING=1");
        match prev {
            Some(v) => std::env::set_var("NOTIFY_SOCKET", v),
            None => std::env::remove_var("NOTIFY_SOCKET"),
        }

        let mut buf = [0u8; 64];
        let n1 = receiver.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n1], b"READY=1");
        let n2 = receiver.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n2], b"STOPPING=1");
    }
}
