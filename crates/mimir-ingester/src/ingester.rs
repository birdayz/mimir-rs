//! Core Ingester service coordinator, UserTSDB manager, and lifecycle management.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_tsdb.go

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use mimir_common::activitytracker::ActivityTracker;
use mimir_common::limiter::InflightLimiter;
use mimir_common::services::{
    describe_service, FnListener, ManagedBasicService, Service, ServiceContext, State,
};
use mimir_common::shutdownmarker;
use mimir_common::usagestats;
use mimir_config::{
    BlocksStorageConfig, CompartmentsConfig, IngestStorageConfig, IngesterConfig, InstanceLimits,
    Overrides, RuntimeConfigManager,
};
use mimir_proto::ringpb::InstanceState;
use mimir_ring::lifecycler::Lifecycler;
use mimir_ring::partition_ring::{
    PartitionInstanceLifecycler, PartitionRingWatcher, PartitionRingWatchers,
};
use mimir_ring::ring::Ring;
use mimir_storage::bucket::BucketClient;
use mimir_storage::shipper::ShipperMetrics;
use mimir_storage::TSDBMetrics;
use parking_lot::{Mutex, RwLock};
use prometheus::Registry;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::circuit_breaker::IngesterCircuitBreaker;
use crate::compaction::{
    spawn_compaction_loop, spawn_idle_tsdb_loop, spawn_metadata_purger_loop,
    spawn_metrics_updater_loop, spawn_owned_series_loop, spawn_shipping_loop,
};
use crate::errors::{ErrorSamplers, IngesterError};
use crate::ingest_storage::{ingester_partition_id_from_instance_id, IngestReaderHandle};
use crate::ingester_ring_lifecycler::IngesterLifecycler;
use crate::limiter::{
    IngesterRingLimiterStrategy, Limiter, LimiterRingStrategy, NoopRingLimiterStrategy,
    PartitionRingLimiterStrategy,
};
use crate::metrics::{IngesterMetrics, REASON_MAX_TENANTS};
use crate::owned_series::{
    OwnedSeriesIngesterRingStrategy, OwnedSeriesMetrics, OwnedSeriesPartitionRingStrategy,
    OwnedSeriesRingStrategy,
};
use crate::reactive_limiter::IngesterReactiveLimiter;
use crate::user_tsdb::{EwmaRate, TenantMetadataStore, UserTsdb};

pub const INGEST_STORAGE_DEFAULT_READ_CONSISTENCY_TIMEOUT: Duration = Duration::from_secs(30);
pub const INSTANCE_INGESTION_RATE_TICK_INTERVAL: Duration = Duration::from_secs(1);
pub const COMPACTION_IDLE_TIMEOUT_JITTER: f64 = 0.25;
pub const MAX_TSDB_OPEN_WITHOUT_CONCURRENCY: usize = 10;
pub const DEFAULT_MAX_TSDB_OPENING_CONCURRENCY_ON_STARTUP: usize = 10;

// Usage-stats names (`pkg/ingester/ingester.go`).
pub const REPLICATION_FACTOR_STATS_NAME: &str = "ingester_replication_factor";
pub const RING_STORE_STATS_NAME: &str = "ingester_ring_store";
pub const MEMORY_SERIES_STATS_NAME: &str = "ingester_inmemory_series";
pub const ACTIVE_SERIES_STATS_NAME: &str = "ingester_active_series";
pub const MEMORY_TENANTS_STATS_NAME: &str = "ingester_inmemory_tenants";
pub const APPENDED_SAMPLES_STATS_NAME: &str = "ingester_appended_samples";
pub const APPENDED_EXEMPLARS_STATS_NAME: &str = "ingester_appended_exemplars";
pub const TENANTS_WITH_OOO_ENABLED_STAT_NAME: &str = "ingester_ooo_enabled_tenants";
pub const MIN_OOO_TIME_WINDOW_SECONDS_STAT_NAME: &str = "ingester_ooo_min_window";
pub const MAX_OOO_TIME_WINDOW_SECONDS_STAT_NAME: &str = "ingester_ooo_max_window";

pub trait UtilizationBasedLimiter: Send + Sync {
    fn limiting_reason(&self) -> Option<String>;
}

/// Usage statistics snapshot matching `pkg/ingester/ingester_metrics_update.go:updateUsageStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UsageStatsSnapshot {
    pub memory_users: i64,
    pub memory_series: i64,
    pub active_series: i64,
    pub tenants_with_out_of_order_enabled: i64,
    pub min_out_of_order_time_window_seconds: i64,
    pub max_out_of_order_time_window_seconds: i64,
    pub appended_samples: i64,
    pub appended_exemplars: i64,
}

pub fn get_open_tsdbs_concurrency_config(
    wal_replay_concurrency: i64,
    user_count: usize,
) -> (usize, usize) {
    let mut tsdb_open_concurrency = DEFAULT_MAX_TSDB_OPENING_CONCURRENCY_ON_STARTUP;
    let mut tsdb_wal_replay_concurrency = 0usize;
    if wal_replay_concurrency > 0 {
        if user_count <= MAX_TSDB_OPEN_WITHOUT_CONCURRENCY {
            tsdb_open_concurrency = 1;
            tsdb_wal_replay_concurrency = wal_replay_concurrency as usize;
        } else {
            tsdb_open_concurrency = wal_replay_concurrency as usize;
            tsdb_wal_replay_concurrency = 1;
        }
    }
    (tsdb_open_concurrency.max(1), tsdb_wal_replay_concurrency)
}

pub struct TriggerRequest {
    pub users: Option<HashSet<String>>, // None means all tenants
    pub callback: oneshot::Sender<()>,
}

type InstanceLimitsFn = Arc<dyn Fn() -> Option<InstanceLimits> + Send + Sync>;

/// Core Mimir Ingester service (`pkg/ingester/ingester.go:Ingester`).
pub struct Ingester {
    pub cfg: IngesterConfig,
    pub blocks_cfg: BlocksStorageConfig,
    pub ingest_cfg: IngestStorageConfig,
    pub overrides: Overrides,
    pub runtime_cfg: Option<Arc<RuntimeConfigManager>>,

    pub registry: Option<Registry>,
    /// `lookupplan.NewMetrics(registerer)`: `cortex_tsdb_index_lookup_planning_*`.
    pub lookup_plan_metrics: crate::lookupplan::Metrics,
    pub metrics: Arc<IngesterMetrics>,
    pub tsdb_metrics: Arc<TSDBMetrics>,
    pub shipper_metrics: ShipperMetrics,
    pub shared_tsdb_metrics: crate::user_tsdb::SharedTsdbMetrics,
    pub owned_series_metrics: OwnedSeriesMetrics,

    pub instance_ring: Option<Arc<Ring>>,
    pub lifecycler: Option<Arc<dyn IngesterLifecycler>>,
    pub partition_ring_watcher: Option<Arc<PartitionRingWatcher>>,
    pub partition_lifecycler: Option<Arc<PartitionInstanceLifecycler>>,
    pub ingest_partition_id: i32,
    pub ingest_reader: RwLock<Option<Arc<IngestReaderHandle>>>,
    compartments: RwLock<CompartmentsConfig>,
    /// Every read compartment's partition ring watcher (Go `IngesterPartitionRingWatchers`),
    /// served by the `/ingester/partition-ring/compartment-<id>` pages.
    partition_ring_watchers: RwLock<Option<Arc<PartitionRingWatchers>>>,

    pub bucket: RwLock<Option<Arc<BucketClient>>>,
    pub limiter: Arc<Limiter>,
    pub owned_series_strategy: Option<Arc<dyn OwnedSeriesRingStrategy>>,
    pub circuit_breaker: IngesterCircuitBreaker,
    pub reactive_limiter: IngesterReactiveLimiter,
    pub inflight_limiter: InflightLimiter,
    pub utilization_based_limiter: RwLock<Option<Arc<dyn UtilizationBasedLimiter>>>,
    /// Concrete read-path utilization limiter service (Go: `NewUtilizationBasedLimiter`).
    utilization_limiter_svc: Option<Arc<crate::utilization::UtilizationBasedLimiter>>,
    /// Shared tenant-fair compute worker pool (Go: `computeWorkerPool`, name `ingester-compute`).
    pub compute_worker_pool: Arc<crate::workerpool::Pool>,

    pub tsdbs: DashMap<String, Arc<UserTsdb>>,
    pub users_metadata: DashMap<String, Arc<TenantMetadataStore>>,
    pub(crate) tsdb_create_mtx: Mutex<()>,
    closing_tsdbs: AtomicBool,

    pub series_count: Arc<AtomicI64>,
    pub inflight_push_requests: Arc<AtomicI64>,
    pub inflight_push_requests_bytes: Arc<AtomicI64>,
    pub ingestion_rate: Arc<EwmaRate>,
    pub num_compactions_in_progress: AtomicU32,
    pub compaction_idle_timeout: Duration,
    pub flush_on_shutdown: AtomicBool,
    pub appended_samples: AtomicI64,
    pub appended_exemplars: AtomicI64,
    /// Usage-stats counters (`ingester_appended_samples` / `ingester_appended_exemplars`),
    /// updated on every push like Go's `appendedSamplesStats` / `appendedExemplarsStats`.
    pub usage_appended_samples: Arc<usagestats::CounterStat>,
    pub usage_appended_exemplars: Arc<usagestats::CounterStat>,

    pub error_samplers: ErrorSamplers,
    pub ignored_series_limit_metrics: HashSet<String>,

    state: RwLock<State>,
    service_ctx: RwLock<Option<ServiceContext>>,
    startup_handle: Mutex<Option<JoinHandle<()>>>,
    startup_done_tx: tokio::sync::watch::Sender<Option<Result<(), String>>>,
    _startup_done_rx: tokio::sync::watch::Receiver<Option<Result<(), String>>>,
    stop_done_tx: tokio::sync::watch::Sender<Option<Result<(), String>>>,
    _stop_done_rx: tokio::sync::watch::Receiver<Option<Result<(), String>>>,
    pub(crate) shutdown_mtx: tokio::sync::Mutex<()>,
    lifecycler_svc: Mutex<Option<Arc<ManagedBasicService>>>,
    partition_lifecycler_svc: Mutex<Option<Arc<ManagedBasicService>>>,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    /// First failure of a watched subservice (`subservicesWatcher` in Go); makes the
    /// ingester service fail and the whole process stop.
    subservice_failure: tokio::sync::watch::Sender<Option<String>>,
    pub usage_stats: Mutex<UsageStatsSnapshot>,

    compaction_service_state: AtomicU8,
    owned_series_service_state: AtomicU8,
    owned_series_initial_ring_check_succeeded: AtomicBool,
    owned_series_paused: AtomicBool,
    metrics_updater_service_state: AtomicU8,
    metadata_purger_service_state: AtomicU8,

    head_compaction_interval_while_starting: RwLock<Duration>,
    head_compaction_interval_jitter_enabled: AtomicBool,
    min_compaction_loop_delay: RwLock<Duration>,
    close_idle_tsdb_interval: RwLock<Duration>,
    limit_metrics_update_period: RwLock<Duration>,
    instance_limits_fn: RwLock<Option<InstanceLimitsFn>>,

    pub force_compact_tx: RwLock<Option<mpsc::Sender<TriggerRequest>>>,
    pub ship_tx: RwLock<Option<mpsc::Sender<TriggerRequest>>>,

    pub verify_ring_ownership: AtomicBool,
    pub read_only_override: AtomicBool,
    weak_self: Arc<std::sync::OnceLock<Weak<Ingester>>>,
    multitenancy_enabled: AtomicBool,
    no_auth_tenant: RwLock<String>,
    tenant_federation_enabled: AtomicBool,
    tenant_federation_max_tenants: AtomicI64,
    cost_attribution_mgr: RwLock<Option<Arc<crate::costattribution::Manager>>>,
    active_groups: RwLock<Option<Arc<crate::active_groups::ActiveGroupsCleanupService>>>,
    activity_tracker: RwLock<Option<Arc<ActivityTracker>>>,
    ref_leaks_tracker: RwLock<Option<mimir_proto::ref_leaks::RefLeaksTracker>>,
}

pub(crate) struct ActivityGuard {
    tracker: Arc<ActivityTracker>,
    idx: isize,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.tracker.delete(self.idx);
    }
}

impl Ingester {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: IngesterConfig,
        blocks_cfg: BlocksStorageConfig,
        ingest_cfg: IngestStorageConfig,
        overrides: Overrides,
        instance_ring: Option<Arc<Ring>>,
        lifecycler: Option<Arc<Lifecycler>>,
        partition_ring_watcher: Option<Arc<PartitionRingWatcher>>,
        partition_lifecycler: Option<Arc<PartitionInstanceLifecycler>>,
        runtime_cfg: Option<Arc<RuntimeConfigManager>>,
        registry: Option<&Registry>,
    ) -> Result<Arc<Self>, IngesterError> {
        Self::new_with_ingester_lifecycler(
            cfg,
            blocks_cfg,
            ingest_cfg,
            overrides,
            instance_ring,
            lifecycler.map(|l| l as Arc<dyn IngesterLifecycler>),
            partition_ring_watcher,
            partition_lifecycler,
            runtime_cfg,
            registry,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_ingester_lifecycler(
        cfg: IngesterConfig,
        blocks_cfg: BlocksStorageConfig,
        ingest_cfg: IngestStorageConfig,
        overrides: Overrides,
        instance_ring: Option<Arc<Ring>>,
        lifecycler: Option<Arc<dyn IngesterLifecycler>>,
        partition_ring_watcher: Option<Arc<PartitionRingWatcher>>,
        partition_lifecycler: Option<Arc<PartitionInstanceLifecycler>>,
        runtime_cfg: Option<Arc<RuntimeConfigManager>>,
        registry: Option<&Registry>,
    ) -> Result<Arc<Self>, IngesterError> {
        // Go newIngester: the bucket client is always created (whatever ship_interval is) and
        // a construction error fails the ingester.
        if blocks_cfg.bucket.backend_cfg.backend == "filesystem" {
            warn!("-blocks-storage.backend=filesystem is for development and testing only; you should switch to an external object store for production use or use a shared filesystem");
        }
        if blocks_cfg.tsdb.offset_catalogue.enabled && !ingest_cfg.enabled {
            return Err(IngesterError::Internal {
                message: "kafka offset catalogue can only be enabled when ingest storage is enabled"
                    .to_string(),
            });
        }

        let bucket = match BucketClient::new_client(&blocks_cfg.bucket, "ingester", registry) {
            Ok(b) => Some(Arc::new(b)),
            Err(e) => {
                return Err(IngesterError::Internal {
                    message: format!("failed to create the bucket client: {e}"),
                });
            }
        };

        let ingest_partition_id = if ingest_cfg.enabled {
            ingester_partition_id_from_instance_id(&cfg.ring.instance_id).map_err(|e| {
                IngesterError::Internal {
                    message: format!("calculating ingester partition ID: {e}"),
                }
            })?
        } else {
            0
        };

        let limiter_strategy: Arc<dyn LimiterRingStrategy> = if ingest_cfg.enabled {
            if let Some(ref watcher) = partition_ring_watcher {
                Arc::new(PartitionRingLimiterStrategy::new(
                    Arc::clone(watcher),
                    overrides.clone(),
                ))
            } else {
                Arc::new(NoopRingLimiterStrategy)
            }
        } else if let Some(ref ring) = instance_ring {
            Arc::new(IngesterRingLimiterStrategy::new(
                Arc::clone(ring),
                cfg.ring.replication_factor.max(1) as usize,
                cfg.ring.zone_awareness_enabled,
                cfg.ring.instance_availability_zone.clone(),
                overrides.clone(),
            ))
        } else {
            Arc::new(NoopRingLimiterStrategy)
        };

        let owned_series_strategy: Option<Arc<dyn OwnedSeriesRingStrategy>> =
            if cfg.use_ingester_owned_series_for_limits || cfg.track_ingester_owned_series {
                if ingest_cfg.enabled {
                    partition_ring_watcher.as_ref().map(|w| {
                        Arc::new(OwnedSeriesPartitionRingStrategy::new(
                            ingest_partition_id,
                            Arc::clone(w),
                            overrides.clone(),
                        )) as Arc<dyn OwnedSeriesRingStrategy>
                    })
                } else {
                    instance_ring.as_ref().map(|r| {
                        Arc::new(OwnedSeriesIngesterRingStrategy::new(
                            cfg.ring.instance_id.clone(),
                            Arc::clone(r),
                            overrides.clone(),
                        )) as Arc<dyn OwnedSeriesRingStrategy>
                    })
                }
            } else {
                None
            };

        let limiter = Arc::new(Limiter::new(overrides.clone(), limiter_strategy));
        let series_count = Arc::new(AtomicI64::new(0));
        let inflight_push_requests = Arc::new(AtomicI64::new(0));
        let inflight_push_requests_bytes = Arc::new(AtomicI64::new(0));
        let ingestion_rate = Arc::new(EwmaRate::new(0.2, INSTANCE_INGESTION_RATE_TICK_INTERVAL));

        let circuit_breaker = IngesterCircuitBreaker::new(
            &cfg.push_circuit_breaker,
            &cfg.read_circuit_breaker,
            registry,
        )?;
        let reactive_limiter = IngesterReactiveLimiter::new(
            &cfg.rejection_prioritizer,
            &cfg.push_reactive_limiter,
            &cfg.read_reactive_limiter,
            registry,
        )
        .map_err(|message| IngesterError::Internal { message })?;

        let utilization_limiter_svc = (cfg.read_path_cpu_utilization_limit > 0.0
            || cfg.read_path_memory_utilization_limit > 0)
            .then(|| {
                crate::utilization::UtilizationBasedLimiter::new(
                    cfg.read_path_cpu_utilization_limit,
                    cfg.read_path_memory_utilization_limit,
                    true,
                    "cortex_ingester_",
                    registry,
                )
            });
        let compute_worker_pool =
            crate::workerpool::Pool::new(cfg.compute_workers, "ingester-compute", registry)
                .map_err(|e| IngesterError::Internal {
                    message: format!("creating ingester compute worker pool: {e}"),
                })?;

        let shipper_metrics = ShipperMetrics::new(registry);
        let shared_tsdb_metrics = crate::user_tsdb::SharedTsdbMetrics::new_with_blocks_cfg(registry, &blocks_cfg);
        let tsdb_metrics = TSDBMetrics::new("cortex_ingester_", registry);
        let owned_series_metrics =
            OwnedSeriesMetrics::new(owned_series_strategy.as_ref().and(registry));

        let ignored_series_limit_metrics = cfg
            .ignore_series_limit_for_metric_names
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect::<HashSet<_>>();

        let idle_timeout_ns = blocks_cfg.tsdb.head_compaction_idle_timeout.as_nanos().max(0) as u64;
        let compaction_idle_timeout = if idle_timeout_ns > 0 {
            if blocks_cfg.tsdb.head_compaction_interval_jitter_enabled {
                let jitter_frac: f64 = rand::random::<f64>() * COMPACTION_IDLE_TIMEOUT_JITTER;
                Duration::from_nanos(((idle_timeout_ns as f64) * (1.0 + jitter_frac)) as u64)
            } else {
                Duration::from_nanos(idle_timeout_ns)
            }
        } else {
            Duration::ZERO
        };

        let weak_slot: Arc<std::sync::OnceLock<Weak<Ingester>>> =
            Arc::new(std::sync::OnceLock::new());
        let w_limits = Arc::clone(&weak_slot);
        let w_min = Arc::clone(&weak_slot);
        let w_max = Arc::clone(&weak_slot);
        let w_ship = Arc::clone(&weak_slot);

        let rate_ref = Arc::clone(&ingestion_rate);
        let metrics = Arc::new(IngesterMetrics::new(
            registry,
            cfg.active_series.active_series_metrics_enabled,
            Arc::clone(&inflight_push_requests),
            Arc::clone(&inflight_push_requests_bytes),
            move || rate_ref.rate(),
            move || {
                w_limits
                    .get()
                    .and_then(Weak::upgrade)
                    .and_then(|ing| ing.get_instance_limits())
            },
            move || {
                w_min
                    .get()
                    .and_then(Weak::upgrade)
                    .map_or(0.0, |ing| ing.min_tsdb_head_timestamp())
            },
            move || {
                w_max
                    .get()
                    .and_then(Weak::upgrade)
                    .map_or(0.0, |ing| ing.max_tsdb_head_timestamp())
            },
            move || {
                w_ship
                    .get()
                    .and_then(Weak::upgrade)
                    .map_or(0.0, |ing| ing.oldest_unshipped_block_timestamp())
            },
        ));

        let error_samplers = ErrorSamplers::new(cfg.error_sample_rate);

        let head_compaction_interval_jitter_enabled =
            blocks_cfg.tsdb.head_compaction_interval_jitter_enabled;
        let close_idle_tsdb_interval = if blocks_cfg.tsdb.close_idle_tsdb_interval.0 <= 0 {
            Duration::from_secs(5 * 60)
        } else {
            Duration::from_nanos(blocks_cfg.tsdb.close_idle_tsdb_interval.as_nanos() as u64)
        };
        let flush_on_shutdown = AtomicBool::new(blocks_cfg.tsdb.flush_blocks_on_shutdown);
        usagestats::get_int(REPLICATION_FACTOR_STATS_NAME).set(cfg.ring.replication_factor);
        usagestats::get_string(RING_STORE_STATS_NAME).set(&cfg.ring.kvstore.store);
        // Register and zero all periodic TSDB/OOO gauges and counters at startup
        // (pkg/ingester/ingester.go:128-141) so early usage reports include them as 0.
        usagestats::get_and_reset_int(MEMORY_SERIES_STATS_NAME);
        usagestats::get_and_reset_int(MEMORY_TENANTS_STATS_NAME);
        usagestats::get_and_reset_int(ACTIVE_SERIES_STATS_NAME);
        usagestats::get_and_reset_int(TENANTS_WITH_OOO_ENABLED_STAT_NAME);
        usagestats::get_and_reset_int(MIN_OOO_TIME_WINDOW_SECONDS_STAT_NAME);
        usagestats::get_and_reset_int(MAX_OOO_TIME_WINDOW_SECONDS_STAT_NAME);
        let (startup_done_tx, _startup_done_rx) = tokio::sync::watch::channel(None);
        let (stop_done_tx, _stop_done_rx) = tokio::sync::watch::channel(None);
        let ingester = Arc::new(Self {
            cfg,
            blocks_cfg,
            ingest_cfg,
            overrides,
            runtime_cfg,
            registry: registry.cloned(),
            lookup_plan_metrics: crate::lookupplan::Metrics::new(registry),
            metrics,
            tsdb_metrics,
            shipper_metrics,
            shared_tsdb_metrics,
            owned_series_metrics,
            instance_ring,
            lifecycler,
            partition_ring_watcher,
            partition_lifecycler,
            ingest_partition_id,
            ingest_reader: RwLock::new(None),
            compartments: RwLock::new(CompartmentsConfig::default()),
            partition_ring_watchers: RwLock::new(None),
            bucket: RwLock::new(bucket),
            limiter,
            owned_series_strategy,
            circuit_breaker,
            reactive_limiter,
            inflight_limiter: InflightLimiter::new(),
            utilization_based_limiter: RwLock::new(
                utilization_limiter_svc
                    .clone()
                    .map(|l| l as Arc<dyn UtilizationBasedLimiter>),
            ),
            utilization_limiter_svc,
            compute_worker_pool,
            tsdbs: DashMap::new(),
            users_metadata: DashMap::new(),
            tsdb_create_mtx: Mutex::new(()),
            closing_tsdbs: AtomicBool::new(false),
            series_count,
            inflight_push_requests,
            inflight_push_requests_bytes,
            ingestion_rate,
            num_compactions_in_progress: AtomicU32::new(0),
            compaction_idle_timeout,
            flush_on_shutdown,
            appended_samples: AtomicI64::new(0),
            appended_exemplars: AtomicI64::new(0),
            usage_appended_samples: usagestats::get_and_reset_counter(APPENDED_SAMPLES_STATS_NAME),
            usage_appended_exemplars: usagestats::get_and_reset_counter(APPENDED_EXEMPLARS_STATS_NAME),
            error_samplers,
            ignored_series_limit_metrics,
            state: RwLock::new(State::New),
            service_ctx: RwLock::new(None),
            startup_handle: Mutex::new(None),
            startup_done_tx,
            _startup_done_rx,
            stop_done_tx,
            _stop_done_rx,
            shutdown_mtx: tokio::sync::Mutex::new(()),
            lifecycler_svc: Mutex::new(None),
            partition_lifecycler_svc: Mutex::new(None),
            background_tasks: Mutex::new(Vec::new()),
            subservice_failure: tokio::sync::watch::channel(None).0,
            usage_stats: Mutex::new(UsageStatsSnapshot::default()),
            compaction_service_state: AtomicU8::new(State::New as u8),
            owned_series_service_state: AtomicU8::new(State::New as u8),
            owned_series_initial_ring_check_succeeded: AtomicBool::new(false),
            owned_series_paused: AtomicBool::new(false),
            metrics_updater_service_state: AtomicU8::new(State::New as u8),
            metadata_purger_service_state: AtomicU8::new(State::New as u8),
            head_compaction_interval_while_starting: RwLock::new(Duration::from_secs(30)),
            head_compaction_interval_jitter_enabled: AtomicBool::new(
                head_compaction_interval_jitter_enabled,
            ),
            min_compaction_loop_delay: RwLock::new(Duration::from_secs(10)),
            close_idle_tsdb_interval: RwLock::new(close_idle_tsdb_interval),
            limit_metrics_update_period: RwLock::new(Duration::from_secs(15)),
            instance_limits_fn: RwLock::new(None),
            force_compact_tx: RwLock::new(None),
            ship_tx: RwLock::new(None),
            verify_ring_ownership: AtomicBool::new(false),
            read_only_override: AtomicBool::new(false),
            weak_self: Arc::clone(&weak_slot),
            multitenancy_enabled: AtomicBool::new(true),
            no_auth_tenant: RwLock::new("fake".to_string()),
            tenant_federation_enabled: AtomicBool::new(false),
            tenant_federation_max_tenants: AtomicI64::new(0),
            cost_attribution_mgr: RwLock::new(None),
            active_groups: RwLock::new(None),
            activity_tracker: RwLock::new(None),
            ref_leaks_tracker: RwLock::new(None),
        });

        let _ = weak_slot.set(Arc::downgrade(&ingester));

        // Go passes the Ingester as the lifecycler's FlushTransferer (classic) / flush callback
        // (tokenless): on shutdown the lifecycler switches to LEAVING, then calls `Flush()` while
        // still heartbeating, then unregisters.
        if let Some(ref lc) = ingester.lifecycler {
            let weak = Arc::downgrade(&ingester);
            lc.set_ingester_flush(Arc::new(move || {
                let weak = weak.clone();
                Box::pin(async move {
                    if let Some(ing) = weak.upgrade() {
                        ing.flush().await;
                    }
                })
            }));
        }
        Ok(ingester)
    }

    pub fn set_ref_leaks_tracker(&self, tracker: mimir_proto::ref_leaks::RefLeaksTracker) {
        *self.ref_leaks_tracker.write() = Some(tracker);
    }

    pub fn ref_leaks_tracker(&self) -> Option<mimir_proto::ref_leaks::RefLeaksTracker> {
        self.ref_leaks_tracker.read().clone()
    }

    pub fn set_compartments(&self, compartments: CompartmentsConfig) {
        *self.compartments.write() = compartments;
    }

    pub fn compartments_config(&self) -> CompartmentsConfig {
        self.compartments.read().clone()
    }

    /// Sets the partition ring watchers of all read compartments (for the status pages).
    pub fn set_partition_ring_watchers(&self, watchers: Option<Arc<PartitionRingWatchers>>) {
        *self.partition_ring_watchers.write() = watchers;
    }

    pub fn partition_ring_watchers(&self) -> Option<Arc<PartitionRingWatchers>> {
        self.partition_ring_watchers.read().clone()
    }

    pub fn set_head_compaction_interval_while_starting(&self, d: Duration) {
        *self.head_compaction_interval_while_starting.write() = d;
    }

    pub fn head_compaction_interval_while_starting(&self) -> Duration {
        *self.head_compaction_interval_while_starting.read()
    }

    pub fn set_head_compaction_interval_jitter_enabled(&self, enabled: bool) {
        self.head_compaction_interval_jitter_enabled
            .store(enabled, Ordering::Release);
    }

    pub fn head_compaction_interval_jitter_enabled(&self) -> bool {
        self.head_compaction_interval_jitter_enabled
            .load(Ordering::Acquire)
    }

    pub fn set_min_compaction_loop_delay(&self, d: Duration) {
        *self.min_compaction_loop_delay.write() = d;
    }

    pub fn min_compaction_loop_delay(&self) -> Duration {
        *self.min_compaction_loop_delay.read()
    }

    pub fn set_close_idle_tsdb_interval(&self, d: Duration) {
        *self.close_idle_tsdb_interval.write() = d;
    }

    pub fn close_idle_tsdb_interval(&self) -> Duration {
        *self.close_idle_tsdb_interval.read()
    }

    pub fn set_limit_metrics_update_period(&self, d: Duration) {
        *self.limit_metrics_update_period.write() = d;
    }

    pub fn limit_metrics_update_period(&self) -> Duration {
        *self.limit_metrics_update_period.read()
    }

    pub fn set_instance_limits_fn(
        &self,
        f: Arc<dyn Fn() -> Option<InstanceLimits> + Send + Sync>,
    ) {
        *self.instance_limits_fn.write() = Some(f);
    }

    /// Wires the cost attribution manager (Go: `costAttributionMgr` passed to `ingester.New`).
    /// `None` disables cost attribution tracking.
    pub fn set_cost_attribution_manager(&self, mgr: Option<Arc<crate::costattribution::Manager>>) {
        *self.cost_attribution_mgr.write() = mgr;
    }

    pub fn cost_attribution_manager(&self) -> Option<Arc<crate::costattribution::Manager>> {
        self.cost_attribution_mgr.read().clone()
    }

    /// Wires the active groups cleanup service used for `-validation.separate-metrics-group-label`
    /// and registers the per-group metrics cleanup (Go: `ActiveGroupsCleanupService.Register(i)`).
    pub fn set_active_groups_cleanup_service(
        &self,
        svc: Option<Arc<crate::active_groups::ActiveGroupsCleanupService>>,
    ) {
        if let Some(ref s) = svc {
            let metrics = Arc::clone(&self.metrics);
            s.register(Box::new(move |user, group| {
                metrics.discarded.delete_label_values(user, group);
            }));
        }
        *self.active_groups.write() = svc;
    }

    pub(crate) fn active_groups(&self) -> Option<Arc<crate::active_groups::ActiveGroupsCleanupService>> {
        self.active_groups.read().clone()
    }

    pub fn set_multitenancy(&self, enabled: bool, no_auth_tenant: impl Into<String>) {
        self.multitenancy_enabled.store(enabled, Ordering::Release);
        *self.no_auth_tenant.write() = no_auth_tenant.into();
    }

    pub fn is_multitenancy_enabled(&self) -> bool {
        self.multitenancy_enabled.load(Ordering::Acquire)
    }

    pub fn no_auth_tenant(&self) -> String {
        self.no_auth_tenant.read().clone()
    }

    pub fn set_tenant_federation(&self, enabled: bool, max_tenants: i64) {
        self.tenant_federation_enabled
            .store(enabled, Ordering::Release);
        self.tenant_federation_max_tenants
            .store(max_tenants, Ordering::Release);
    }

    pub fn tenant_federation(&self) -> (bool, i64) {
        (
            self.tenant_federation_enabled.load(Ordering::Acquire),
            self.tenant_federation_max_tenants.load(Ordering::Acquire),
        )
    }

    fn state_from_u8(v: u8) -> State {
        match v {
            0 => State::New,
            1 => State::Starting,
            2 => State::Running,
            3 => State::Stopping,
            4 => State::Terminated,
            _ => State::Failed,
        }
    }

    pub fn set_compaction_service_state(&self, s: State) {
        self.compaction_service_state
            .store(s as u8, Ordering::Release);
    }

    pub fn compaction_service_state(&self) -> State {
        Self::state_from_u8(self.compaction_service_state.load(Ordering::Acquire))
    }

    pub fn set_owned_series_service_state(&self, s: State) {
        self.owned_series_service_state
            .store(s as u8, Ordering::Release);
    }

    pub fn owned_series_service_state(&self) -> State {
        Self::state_from_u8(self.owned_series_service_state.load(Ordering::Acquire))
    }

    pub fn set_owned_series_paused(&self, paused: bool) {
        self.owned_series_paused.store(paused, Ordering::Release);
    }

    pub fn is_owned_series_paused(&self) -> bool {
        self.owned_series_paused.load(Ordering::Acquire)
    }

    pub fn owned_series_initial_ring_check_succeeded(&self) -> bool {
        self.owned_series_initial_ring_check_succeeded
            .load(Ordering::Acquire)
    }

    /// Attaches an [`ActivityTracker`] for recording active gRPC read RPCs and HTTP admin handlers
    /// (`ingester.NewIngesterActivityTracker`).
    pub fn set_activity_tracker(&self, tracker: Option<Arc<ActivityTracker>>) {
        *self.activity_tracker.write() = tracker;
    }

    pub fn activity_tracker(&self) -> Option<Arc<ActivityTracker>> {
        self.activity_tracker.read().clone()
    }

    pub(crate) fn insert_activity(&self, f: impl FnOnce() -> String) -> Option<ActivityGuard> {
        let tracker = self.activity_tracker.read().clone()?;
        let idx = tracker.insert(f);
        Some(ActivityGuard { tracker, idx })
    }

    pub fn set_metrics_updater_service_state(&self, s: State) {
        self.metrics_updater_service_state
            .store(s as u8, Ordering::Release);
    }

    pub fn metrics_updater_service_state(&self) -> State {
        Self::state_from_u8(self.metrics_updater_service_state.load(Ordering::Acquire))
    }

    pub fn set_metadata_purger_service_state(&self, s: State) {
        self.metadata_purger_service_state
            .store(s as u8, Ordering::Release);
    }

    pub fn metadata_purger_service_state(&self) -> State {
        Self::state_from_u8(self.metadata_purger_service_state.load(Ordering::Acquire))
    }

    pub fn lifecycler_service_state(&self) -> State {
        self.lifecycler_svc
            .lock()
            .as_ref()
            .map_or(State::New, |s| s.state())
    }

    pub fn partition_lifecycler_service_state(&self) -> State {
        self.partition_lifecycler_svc
            .lock()
            .as_ref()
            .map_or(State::New, |s| s.state())
    }

    #[inline]
    pub fn state(&self) -> State {
        *self.state.read()
    }

    pub fn set_state_for_test(&self, state: State) {
        *self.state.write() = state;
    }

    pub fn service_context(&self) -> Option<ServiceContext> {
        self.service_ctx.read().clone()
    }

    pub fn get_instance_limits(&self) -> Option<InstanceLimits> {
        if self.state() == State::Starting {
            return None;
        }
        if let Some(ref f) = *self.instance_limits_fn.read() {
            if let Some(lim) = f() {
                return Some(lim);
            }
        }
        if let Some(ref rt) = self.runtime_cfg {
            if let Some(lim) = rt.ingester_limits() {
                return Some(lim);
            }
        }
        Some(self.cfg.instance_limits.clone())
    }

    /// Checks if the ingester is available for reads (`services.Running`).
    pub fn check_available_for_read(&self) -> Result<(), IngesterError> {
        let s = self.state();
        if s == State::Running {
            return Ok(());
        }
        Err(IngesterError::Unavailable { state: s })
    }

    /// Checks if the ingester is available for pushes (`Running` or `Starting` with active Kafka reader).
    pub fn check_available_for_push(&self) -> Result<(), IngesterError> {
        let s = self.state();
        if s == State::Running {
            return Ok(());
        }
        if s == State::Starting && self.ingest_reader.read().is_some() {
            return Ok(());
        }
        Err(IngesterError::Unavailable { state: s })
    }

    pub async fn check_ready(&self) -> Result<(), String> {
        self.check_available_for_push()
            .map_err(|e| format!("ingester not ready for pushes: {e}"))?;
        self.check_available_for_read()
            .map_err(|e| format!("ingester not ready for reads: {e}"))?;
        if let Some(ref lc) = self.lifecycler {
            lc.check_ready().await?;
        }
        Ok(())
    }

    pub fn set_prepare_shutdown(&self) {
        self.flush_on_shutdown.store(true, Ordering::Release);
        if let Some(ref lc) = self.lifecycler {
            lc.set_unregister_on_shutdown(true);
            lc.set_flush_on_shutdown(true);
        }
        if let Some(ref plc) = self.partition_lifecycler {
            plc.set_create_partition_on_startup(false);
            plc.set_remove_owner_on_shutdown(true);
        }
        self.metrics.shutdown_marker.set(1.0);
    }

    pub fn unset_prepare_shutdown(&self) {
        self.flush_on_shutdown.store(
            self.blocks_cfg.tsdb.flush_blocks_on_shutdown,
            Ordering::Release,
        );
        if let Some(ref lc) = self.lifecycler {
            lc.set_unregister_on_shutdown(self.cfg.ring.unregister_on_shutdown);
            lc.set_flush_on_shutdown(self.blocks_cfg.tsdb.flush_blocks_on_shutdown);
        }
        self.metrics.shutdown_marker.set(0.0);
    }

    pub fn set_bucket(&self, bucket: Option<Arc<BucketClient>>) {
        *self.bucket.write() = bucket;
    }

    pub fn set_utilization_based_limiter(&self, limiter: Option<Arc<dyn UtilizationBasedLimiter>>) {
        *self.utilization_based_limiter.write() = limiter;
    }

    /// `generateHeadStatisticsForAllUsers`.
    pub fn generate_head_statistics_for_all_users(&self) {
        let dbs: Vec<Arc<UserTsdb>> = self.tsdbs.iter().map(|e| Arc::clone(e.value())).collect();
        for db in dbs {
            db.generate_head_statistics();
        }
    }

    pub fn get_tsdb(&self, user_id: &str) -> Option<Arc<UserTsdb>> {
        self.tsdbs.get(user_id).map(|v| Arc::clone(v.value()))
    }

    pub fn get_tsdb_users(&self) -> Vec<String> {
        self.tsdbs.iter().map(|e| e.key().clone()).collect()
    }

    pub fn get_or_create_tsdb(&self, user_id: &str) -> Result<Arc<UserTsdb>, IngesterError> {
        if let Some(db) = self.get_tsdb(user_id) {
            return Ok(db);
        }

        let _guard = self.tsdb_create_mtx.lock();
        if self.closing_tsdbs.load(Ordering::Acquire) {
            return Err(IngesterError::Unavailable {
                state: self.state(),
            });
        }
        if let Some(db) = self.get_tsdb(user_id) {
            return Ok(db);
        }

        if let Some(gl) = self.get_instance_limits() {
            if gl.max_tenants > 0 && (self.tsdbs.len() as i64) >= gl.max_tenants {
                self.metrics
                    .rejected
                    .with_label_values(&[REASON_MAX_TENANTS])
                    .inc();
                return Err(IngesterError::max_tenants_reached());
            }
        }

        let db = self.create_tsdb(user_id, 0)?;
        self.tsdbs.insert(user_id.to_string(), Arc::clone(&db));
        self.metrics.mem_users.inc();
        Ok(db)
    }

    fn create_tsdb(
        &self,
        user_id: &str,
        wal_replay_concurrency: usize,
    ) -> Result<Arc<UserTsdb>, IngesterError> {
        let series_count = Arc::clone(&self.series_count);
        let rejected = self.metrics.rejected.clone();
        let owned_shard_size = self
            .owned_series_strategy
            .as_ref()
            .map_or(0, |s| s.shard_size_for_user(user_id));

        let weak_self = {
            let weak_slot = Arc::clone(&self.weak_self);
            let max_series_fn: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
                match weak_slot.get().and_then(Weak::upgrade) {
                    Some(ing) => ing.get_instance_limits().map_or(0, |l| l.max_series),
                    None => 0,
                }
            });
            max_series_fn
        };

        let tsdb_prom_reg = Arc::new(Registry::new());
        let effective_wal_concurrency = if wal_replay_concurrency > 0 {
            wal_replay_concurrency
        } else {
            std::thread::available_parallelism().map_or(4, |n| n.get().clamp(1, 8))
        };

        let bucket_guard = self.bucket.read();
        let db = UserTsdb::open(
            user_id.to_string(),
            &self.cfg,
            &self.blocks_cfg,
            self.overrides.clone(),
            Arc::clone(&self.limiter),
            bucket_guard.as_deref(),
            self.shipper_metrics.clone(),
            &self.shared_tsdb_metrics,
            Some(Arc::clone(&tsdb_prom_reg)),
            effective_wal_concurrency,
            series_count,
            weak_self,
            rejected,
            self.ignored_series_limit_metrics.clone(),
            self.ingest_partition_id,
            owned_shard_size,
            self.cost_attribution_manager()
                .and_then(|m| m.active_series_tracker(user_id)),
        )
        .map_err(|e| IngesterError::Internal {
            message: format!("failed to open TSDB for user {user_id}: {e}"),
        })?;
        let planning = &self.blocks_cfg.tsdb.index_lookup_planning;
        if planning.index_lookup_planning_enabled {
            db.set_planner_provider(Arc::new(crate::lookupplan::PlannerProvider::new(Arc::new(
                crate::lookupplan::PlannerFactory::new(
                    self.lookup_plan_metrics.for_user(user_id),
                    crate::lookupplan::StatisticsGenerator::new(),
                    planning.into(),
                ),
            ))));
            // Initial statistics only once the TSDB has been opened and its WAL replayed.
            db.generate_head_statistics();
        }
        self.tsdb_metrics
            .set_registry_for_tenant(user_id, tsdb_prom_reg);
        Ok(db)
    }

    pub fn get_or_create_user_metadata(&self, user_id: &str) -> Arc<TenantMetadataStore> {
        if let Some(m) = self.users_metadata.get(user_id) {
            return Arc::clone(m.value());
        }
        let entry = self
            .users_metadata
            .entry(user_id.to_string())
            .or_insert_with(|| {
                Arc::new(TenantMetadataStore::new(
                    user_id.to_string(),
                    Arc::clone(&self.limiter),
                    self.metrics.mem_metadata.clone(),
                    self.metrics.mem_metadata_created_total.clone(),
                    self.metrics.mem_metadata_removed_total.clone(),
                    self.metrics.discarded_metadata_per_user_limit.clone(),
                    self.metrics.discarded_metadata_per_metric_limit.clone(),
                ))
            });
        Arc::clone(entry.value())
    }

    pub fn get_user_metadata(&self, user_id: &str) -> Option<Arc<TenantMetadataStore>> {
        self.users_metadata
            .get(user_id)
            .map(|v| Arc::clone(v.value()))
    }

    pub fn delete_user_metadata(&self, user_id: &str) {
        if let Some((_, um)) = self.users_metadata.remove(user_id) {
            um.purge(None);
        }
    }

    pub fn purge_user_metrics_metadata(&self) {
        let retain = Duration::from_nanos(
            self.cfg.metadata_retain_period.as_nanos().max(0) as u64,
        );
        let deadline = std::time::SystemTime::now()
            .checked_sub(retain)
            .unwrap_or(std::time::UNIX_EPOCH);
        let keys: Vec<String> = self
            .users_metadata
            .iter()
            .map(|e| e.key().clone())
            .collect();
        for user_id in keys {
            if let Some(um) = self.get_user_metadata(&user_id) {
                um.purge(Some(deadline));
            }
        }
    }

    pub fn apply_tsdb_settings(&self) {
        for user_id in self.get_tsdb_users() {
            if let Some(db) = self.get_tsdb(&user_id) {
                db.apply_tsdb_settings();
            }
        }
    }

    pub fn min_tsdb_head_timestamp(&self) -> f64 {
        let dbs: Vec<Arc<UserTsdb>> = self.tsdbs.iter().map(|e| Arc::clone(e.value())).collect();
        let mut min_t = i64::MAX;
        for db in dbs {
            let h = db.db.head();
            if h.initialized() {
                min_t = min_t.min(h.min_time());
            }
        }
        if min_t == i64::MAX {
            0.0
        } else {
            (min_t as f64) / 1000.0
        }
    }

    pub fn max_tsdb_head_timestamp(&self) -> f64 {
        let dbs: Vec<Arc<UserTsdb>> = self.tsdbs.iter().map(|e| Arc::clone(e.value())).collect();
        let mut max_t = i64::MIN;
        for db in dbs {
            let h = db.db.head();
            if h.initialized() {
                max_t = max_t.max(h.max_time());
            }
        }
        if max_t == i64::MIN {
            0.0
        } else {
            (max_t as f64) / 1000.0
        }
    }

    pub fn oldest_unshipped_block_timestamp(&self) -> f64 {
        let dbs: Vec<Arc<UserTsdb>> = self.tsdbs.iter().map(|e| Arc::clone(e.value())).collect();
        let mut oldest = 0u64;
        for db in dbs {
            let ts = db.oldest_unshipped_block_time_ms();
            if ts > 0 && (oldest == 0 || ts < oldest) {
                oldest = ts;
            }
        }
        (oldest / 1000) as f64
    }

    fn find_user_ids_with_tsdb_on_filesystem(&self) -> Result<Vec<String>, IngesterError> {
        let root = Path::new(&self.blocks_cfg.tsdb.dir);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut users = Vec::new();
        let entries = fs::read_dir(root).map_err(|e| IngesterError::Internal {
            message: format!("unable to read TSDB dir {}: {e}", root.display()),
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| IngesterError::Internal {
                message: format!("reading TSDB dir entry: {e}"),
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            // Skip empty directories
            let has_entries = fs::read_dir(&path)
                .map(|mut rd| rd.next().is_some())
                .unwrap_or(false);
            if has_entries {
                users.push(name.to_string());
            }
        }
        Ok(users)
    }

    /// Opens (and replays the WAL of) every tenant TSDB found on disk with a pool of
    /// `tsdbOpenConcurrency` workers (`openExistingTSDB` in `ingester_tsdb.go`). Opening is
    /// blocking I/O + CPU work, so the workers run on the blocking thread pool instead of
    /// stalling async runtime workers.
    pub async fn open_existing_tsdbs(self: &Arc<Self>) -> Result<(), IngesterError> {
        self.open_existing_tsdbs_with_context(None).await
    }

    pub async fn open_existing_tsdbs_with_context(
        self: &Arc<Self>,
        ctx: Option<&ServiceContext>,
    ) -> Result<(), IngesterError> {
        info!("opening existing TSDBs");
        let start = Instant::now();
        let user_ids = self.find_user_ids_with_tsdb_on_filesystem().map_err(|e| {
            error!(err = %e, "error while finding existing TSDBs");
            e
        })?;
        if user_ids.is_empty() {
            return Ok(());
        }
        let (open_concurrency, wal_replay_concurrency) = get_open_tsdbs_concurrency_config(
            self.blocks_cfg.tsdb.wal_replay_concurrency,
            user_ids.len(),
        );

        let queue = Arc::new(Mutex::new(user_ids.into_iter()));
        let aborted = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(open_concurrency);
        for _ in 0..open_concurrency {
            let ing = Arc::clone(self);
            let queue = Arc::clone(&queue);
            let aborted = Arc::clone(&aborted);
            let worker_ctx = ctx.cloned();
            workers.push(tokio::task::spawn_blocking(move || -> Result<(), IngesterError> {
                loop {
                    if aborted.load(Ordering::Acquire)
                        || worker_ctx.as_ref().is_some_and(ServiceContext::is_canceled)
                        || ing.closing_tsdbs.load(Ordering::Acquire)
                    {
                        return Err(IngesterError::Internal {
                            message: "opening existing TSDBs canceled".to_string(),
                        });
                    }
                    let Some(user_id) = queue.lock().next() else {
                        return Ok(());
                    };
                    match ing.create_tsdb(&user_id, wal_replay_concurrency) {
                        Ok(db) => {
                            let inserted = {
                                let _g = ing.tsdb_create_mtx.lock();
                                if ing.closing_tsdbs.load(Ordering::Acquire)
                                    || worker_ctx.as_ref().is_some_and(ServiceContext::is_canceled)
                                {
                                    false
                                } else {
                                    ing.tsdbs.insert(user_id.clone(), db.clone());
                                    ing.metrics.mem_users.inc();
                                    true
                                }
                            };
                            if !inserted {
                                let _ = db.close();
                                return Err(IngesterError::Internal {
                                    message: "opening existing TSDBs canceled".to_string(),
                                });
                            }
                        }
                        Err(e) => {
                            aborted.store(true, Ordering::Release);
                            error!(err = %e, user = %user_id, "unable to open TSDB");
                            return Err(IngesterError::Internal {
                                message: format!("unable to open TSDB for user {user_id}: {e}"),
                            });
                        }
                    }
                }
            }));
        }

        let mut first_err = None;
        for w in workers {
            let res = match w.await {
                Ok(r) => r,
                Err(e) => Err(IngesterError::Internal {
                    message: format!("TSDB opening worker panicked: {e}"),
                }),
            };
            if let Err(e) = res {
                first_err.get_or_insert(e);
            }
        }
        if let Some(err) = first_err {
            error!(err = %err, "error while opening existing TSDBs");
            return Err(err);
        }

        // Update the usage statistics once all TSDBs have been opened.
        self.update_usage_stats();
        // Return transient WAL-replay / initial-compaction dirty pages across arenas to the OS.
        mimir_common::purge_allocator_arenas();

        self.metrics
            .open_existing_tsdb
            .inc_by(start.elapsed().as_secs_f64());
        info!("successfully opened existing TSDBs");
        Ok(())
    }

    pub async fn close_all_tsdbs(&self) {
        let snapshot: Vec<(String, Arc<UserTsdb>)> = {
            let _g = self.tsdb_create_mtx.lock();
            self.closing_tsdbs.store(true, Ordering::Release);
            let mut items = Vec::with_capacity(self.tsdbs.len());
            for entry in &self.tsdbs {
                entry.value().set_closing_state();
                items.push((entry.key().clone(), Arc::clone(entry.value())));
            }
            items
        };
        for (_, db) in &snapshot {
            db.wait_in_flight_appends().await;
        }
        let mut join_set = tokio::task::JoinSet::new();
        for (k, db) in snapshot {
            let db_close = Arc::clone(&db);
            join_set.spawn_blocking(move || {
                let res = db_close.close();
                (k, db_close, res)
            });
        }
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok((k, db, Ok(()))) => {
                    let removed = {
                        let _g = self.tsdb_create_mtx.lock();
                        self.tsdbs.remove_if(&k, |_, v| Arc::ptr_eq(v, &db))
                    };
                    if removed.is_some() {
                        let tracker_names = db.active_series.current_matcher_names();
                        self.metrics.mem_users.dec();
                        self.tsdb_metrics.remove_registry_for_tenant(&k);
                        self.metrics
                            .delete_per_user_custom_tracker_metrics(&k, &tracker_names);
                    }
                }
                Ok((k, _, Err(e))) => {
                    warn!(user = %k, error = %e, "unable to close TSDB");
                }
                Err(e) => {
                    warn!(error = %e, "TSDB close task failed");
                }
            }
        }
        if matches!(self.state(), State::New | State::Running) {
            let _g = self.tsdb_create_mtx.lock();
            if matches!(self.state(), State::New | State::Running) {
                self.closing_tsdbs.store(false, Ordering::Release);
            }
        }
    }

    /// Computes anonymous usage statistics across all open tenant TSDBs (`updateUsageStats`).
    pub fn update_usage_stats(&self) -> UsageStatsSnapshot {
        let mut snap = UsageStatsSnapshot::default();
        let mut min_ooo_ns: i64 = 0;
        let mut max_ooo_ns: i64 = 0;
        for user_id in self.get_tsdb_users() {
            let Some(user_db) = self.get_tsdb(&user_id) else {
                continue;
            };
            let num_series = user_db.db.head().num_series();
            if num_series == 0 {
                continue;
            }
            snap.memory_users += 1;
            snap.memory_series += num_series as i64;
            snap.active_series += user_db.active_series.active().0 as i64;

            let ooo_ns = self.overrides.out_of_order_time_window_nanos(&user_id);
            if ooo_ns > 0 {
                snap.tenants_with_out_of_order_enabled += 1;
                if min_ooo_ns == 0 || ooo_ns < min_ooo_ns {
                    min_ooo_ns = ooo_ns;
                }
                if ooo_ns > max_ooo_ns {
                    max_ooo_ns = ooo_ns;
                }
            }
        }
        snap.min_out_of_order_time_window_seconds = min_ooo_ns / 1_000_000_000;
        snap.max_out_of_order_time_window_seconds = max_ooo_ns / 1_000_000_000;
        snap.appended_samples = self.appended_samples.load(Ordering::Relaxed);
        snap.appended_exemplars = self.appended_exemplars.load(Ordering::Relaxed);
        *self.usage_stats.lock() = snap;

        usagestats::get_int(MEMORY_SERIES_STATS_NAME).set(snap.memory_series);
        usagestats::get_int(ACTIVE_SERIES_STATS_NAME).set(snap.active_series);
        usagestats::get_int(MEMORY_TENANTS_STATS_NAME).set(snap.memory_users);
        usagestats::get_int(TENANTS_WITH_OOO_ENABLED_STAT_NAME)
            .set(snap.tenants_with_out_of_order_enabled);
        usagestats::get_int(MIN_OOO_TIME_WINDOW_SECONDS_STAT_NAME)
            .set(snap.min_out_of_order_time_window_seconds);
        usagestats::get_int(MAX_OOO_TIME_WINDOW_SECONDS_STAT_NAME)
            .set(snap.max_out_of_order_time_window_seconds);
        snap
    }

    /// Flushes WAL & WBL segments for all open tenant TSDBs (`NotifyPreCommit`).
    pub fn notify_pre_commit(&self) -> Result<(), IngesterError> {
        self.notify_pre_commit_with(|_, db| db.db.head().sync().map_err(|e| e.to_string()))
    }

    #[doc(hidden)]
    pub fn notify_pre_commit_with<F>(&self, sync_user: F) -> Result<(), IngesterError>
    where
        F: Fn(&str, &Arc<UserTsdb>) -> Result<(), String> + Sync,
    {
        let users = self.get_tsdb_users();
        if users.is_empty() {
            return Ok(());
        }
        let concurrency = self
            .ingest_cfg
            .write_logs_fsync_before_kafka_commit_concurrency
            .max(1) as usize;
        let errs: Vec<String> = if concurrency <= 1 || users.len() == 1 {
            let mut errs = Vec::new();
            for user_id in users {
                if let Some(db) = self.get_tsdb(&user_id) {
                    // Go `concurrency.ForEachUser` adds `db.Head().Sync()` errors unwrapped.
                    if let Err(e) = sync_user(&user_id, &db) {
                        errs.push(e);
                    }
                }
            }
            errs
        } else {
            let next_idx = std::sync::atomic::AtomicUsize::new(0);
            let errs: Mutex<Vec<String>> = Mutex::new(Vec::new());
            let workers = concurrency.min(users.len());
            std::thread::scope(|s| {
                for _ in 0..workers {
                    s.spawn(|| loop {
                        let idx = next_idx.fetch_add(1, Ordering::Relaxed);
                        let Some(user_id) = users.get(idx) else {
                            break;
                        };
                        if let Some(db) = self.get_tsdb(user_id) {
                            if let Err(e) = sync_user(user_id, &db) {
                                errs.lock().push(e);
                            }
                        }
                    });
                }
            });
            errs.into_inner()
        };
        match errs.len() {
            0 => Ok(()),
            1 => Err(IngesterError::Internal {
                message: errs.into_iter().next().unwrap_or_default(),
            }),
            n => Err(IngesterError::Internal {
                message: format!("{n} errors: {}", errs.join("; ")),
            }),
        }
    }

    /// Records the failure of a watched subservice (Go: `subservicesWatcher.Chan()` makes
    /// `ingesterRunning` return `ingester subservice failed: ...`). Only the first one is kept.
    pub fn report_subservice_failure(&self, err: String) {
        // Failures observed while the ingester itself is stopping are only logged by Go.
        if !matches!(self.state(), State::Starting | State::Running) {
            warn!(err = %err, "ingester subservice failed while stopping");
            return;
        }
        self.subservice_failure.send_if_modified(|cur| {
            if cur.is_none() {
                *cur = Some(err);
                true
            } else {
                false
            }
        });
    }

    /// The recorded subservice failure, if any.
    pub fn subservice_failure(&self) -> Option<String> {
        self.subservice_failure.borrow().clone()
    }

    /// Resolves with the failure case once a watched subservice failed while the ingester was
    /// starting or running.
    pub async fn wait_subservice_failure(&self) -> String {
        let mut rx = self.subservice_failure.subscribe();
        loop {
            if let Some(err) = rx.borrow_and_update().clone() {
                return err;
            }
            if rx.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Wraps a spawned subservice loop so that a panic is reported as a subservice failure
    /// (in Go a panicking goroutine crashes the whole process) instead of being swallowed by
    /// the tokio runtime.
    pub fn watch_subservice_task(
        self: &Arc<Self>,
        name: &'static str,
        handle: JoinHandle<()>,
    ) -> JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            if let Err(err) = handle.await {
                if err.is_panic() {
                    let payload = err.into_panic();
                    let msg = payload
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_string())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    if let Some(ing) = weak.upgrade() {
                        ing.report_subservice_failure(format!(
                            "ingester subservice failed: service {name} panicked: {msg}"
                        ));
                    }
                }
            }
        })
    }

    /// Starts the Ingester service asynchronously, returning immediately while startup progresses.
    pub async fn start_async(self: &Arc<Self>) -> Result<(), IngesterError> {
        self.start_async_with_context(ServiceContext::background())
            .await
    }

    /// Starts the Ingester service asynchronously bound to a parent [`ServiceContext`].
    pub async fn start_async_with_context(
        self: &Arc<Self>,
        ctx: ServiceContext,
    ) -> Result<(), IngesterError> {
        let mut st = self.state.write();
        if *st != State::New {
            return Err(IngesterError::Internal {
                message: format!("invalid ingester state on start: {}", *st),
            });
        }
        *st = State::Starting;
        self.startup_done_tx.send_replace(None);
        *self.service_ctx.write() = Some(ctx.clone());

        let ing = Arc::clone(self);
        let handle = tokio::spawn(async move {
            struct StartupDoneGuard {
                tx: tokio::sync::watch::Sender<Option<Result<(), String>>>,
                sent: bool,
            }
            impl Drop for StartupDoneGuard {
                fn drop(&mut self) {
                    if !self.sent {
                        self.tx.send_replace(Some(Err(
                            "ingester startup task panicked".to_string(),
                        )));
                    }
                }
            }
            let mut guard = StartupDoneGuard {
                tx: ing.startup_done_tx.clone(),
                sent: false,
            };
            let res = ing.run_starting(ctx).await;
            guard.sent = true;
            ing.startup_done_tx.send_replace(Some(
                res.as_ref().map(|&()| ()).map_err(ToString::to_string),
            ));
        });
        *self.startup_handle.lock() = Some(handle);
        Ok(())
    }

    /// Starts the Ingester service bound to a parent [`ServiceContext`] and waits until running.
    pub async fn start_with_context(
        self: &Arc<Self>,
        ctx: ServiceContext,
    ) -> Result<(), IngesterError> {
        self.start_async_with_context(ctx).await?;
        self.await_running().await
    }

    /// Waits for `start_async` to finish transitioning the Ingester into `State::Running`.
    pub async fn await_running(&self) -> Result<(), IngesterError> {
        if *self.state.read() == State::New {
            return Ok(());
        }
        let mut rx = self.startup_done_tx.subscribe();
        loop {
            if let Some(res) = rx.borrow_and_update().clone() {
                return res.map_err(|message| IngesterError::Internal { message });
            }
            if rx.changed().await.is_err() {
                return Ok(());
            }
        }
    }

    /// Starts the Ingester service and waits until it reaches `State::Running`.
    pub async fn start(self: &Arc<Self>) -> Result<(), IngesterError> {
        self.start_async().await?;
        self.await_running().await
    }

    async fn run_starting(self: &Arc<Self>, ctx: ServiceContext) -> Result<(), IngesterError> {
        let res = self.run_starting_inner(&ctx).await;
        if let Err(ref _e) = res {
            ctx.cancel();
            let reader_opt = self.ingest_reader.write().take();
            if let Some(r) = reader_opt {
                r.stop().await;
            }
            let lc_svc_opt = self.lifecycler_svc.lock().clone();
            if let Some(svc) = lc_svc_opt {
                svc.stop_async();
                let _ = svc.await_terminated().await;
            }
            let plc_svc_opt = self.partition_lifecycler_svc.lock().clone();
            if let Some(svc) = plc_svc_opt {
                svc.stop_async();
                let _ = svc.await_terminated().await;
            }
            let tasks = std::mem::take(&mut *self.background_tasks.lock());
            for t in tasks {
                let _ = t.await;
            }
            self.close_all_tsdbs().await;
            let mut st = self.state.write();
            if *st == State::Starting {
                *st = State::Failed;
            }
        }
        res
    }

    async fn run_starting_inner(self: &Arc<Self>, ctx: &ServiceContext) -> Result<(), IngesterError> {
        if ctx.is_canceled() {
            return Err(IngesterError::Internal {
                message: "context canceled".to_string(),
            });
        }
        let tsdb_dir = PathBuf::from(&self.blocks_cfg.tsdb.dir);
        fs::create_dir_all(&tsdb_dir).map_err(|e| IngesterError::Internal {
            message: format!("failed to create TSDB dir: {e}"),
        })?;

        let marker_path = shutdownmarker::get_path(&tsdb_dir);
        match shutdownmarker::exists(&marker_path) {
            Ok(true) => {
                info!(path = %marker_path.display(), "detected existing shutdown marker");
                self.set_prepare_shutdown();
            }
            Ok(false) => {}
            Err(e) => {
                return Err(IngesterError::Internal {
                    message: format!("failed to check ingester shutdown marker: {e}"),
                });
            }
        }

        self.open_existing_tsdbs_with_context(Some(ctx)).await?;

        // Start owned series loop before partition replay and lifecycler.
        if self.owned_series_strategy.is_some() {
            self.run_initial_owned_series_check()?;
            let h = self.watch_subservice_task(
                "ingester-owned-series",
                spawn_owned_series_loop(Arc::clone(self), ctx.child()),
            );
            self.background_tasks.lock().push(h);
        }

        let (fc_tx, fc_rx) = mpsc::channel(16);
        *self.force_compact_tx.write() = Some(fc_tx);

        // Start compaction, metrics updater, and metadata purger before partition replay.
        {
            let compaction = self.watch_subservice_task(
                "ingester-compaction",
                spawn_compaction_loop(Arc::clone(self), fc_rx, ctx.child()),
            );
            let metrics_updater = self.watch_subservice_task(
                "ingester-metrics-updater",
                spawn_metrics_updater_loop(Arc::clone(self), ctx.child()),
            );
            let metadata_purger = self.watch_subservice_task(
                "ingester-metadata-purger",
                spawn_metadata_purger_loop(Arc::clone(self), ctx.child()),
            );
            let mut tasks = self.background_tasks.lock();
            tasks.push(compaction);
            tasks.push(metrics_updater);
            tasks.push(metadata_purger);
        }

        // Start Kafka ingest storage reader BEFORE lifecycler enters ACTIVE.
        if self.ingest_cfg.enabled {
            let reader_fut = crate::ingest_storage::start_ingest_reader(Arc::clone(self));
            let res = tokio::select! {
                _ = ctx.canceled() => {
                    let reader_opt = self.ingest_reader.write().take();
                    if let Some(r) = reader_opt {
                        r.stop().await;
                    }
                    return Err(IngesterError::Internal {
                        message: "failed to start partition reader: context canceled".to_string(),
                    });
                }
                r = reader_fut => r,
            };
            res.map_err(|e| IngesterError::Internal {
                message: format!("failed to start ingest reader: {e}"),
            })?;
        }

        if let Some(ref lc) = self.lifecycler {
            let svc = Arc::clone(lc).into_managed_service();
            *self.lifecycler_svc.lock() = Some(Arc::clone(&svc));
            // Go: `i.subservicesWatcher.WatchService(i.lifecycler)`.
            {
                let weak = Arc::downgrade(self);
                let name = describe_service(svc.as_ref());
                let listener = FnListener::new(
                    None,
                    None,
                    None,
                    None,
                    Some(Box::new(move |_, err| {
                        if let Some(ing) = weak.upgrade() {
                            ing.report_subservice_failure(format!(
                                "ingester subservice failed: service {name} failed: {err}"
                            ));
                        }
                    })),
                );
                // The listener stays registered for the lifetime of the lifecycler service.
                std::mem::forget(svc.add_listener(listener));
            }
            let lc_ctx = ServiceContext::background();
            svc.start_async(lc_ctx)
                .await
                .map_err(|e| IngesterError::Internal {
                    message: format!("failed to start lifecycler: {e}"),
                })?;
            tokio::select! {
                _ = ctx.canceled() => {
                    return Err(IngesterError::Internal {
                        message: "failed to wait for lifecycler to be running: context canceled".to_string(),
                    });
                }
                r = svc.await_running() => {
                    if let Err(e) = r {
                        return Err(IngesterError::Internal {
                            message: format!("lifecycler failed to reach running: {e}"),
                        });
                    }
                }
            }
            if let Some(ref ring) = self.instance_ring {
                let deadline = Instant::now() + Duration::from_secs(7 * 60);
                loop {
                    if ring
                        .get_instance_state(&self.cfg.ring.instance_id)
                        .ok()
                        == Some(InstanceState::Active)
                    {
                        break;
                    }
                    if ctx.is_canceled() || Instant::now() >= deadline {
                        return Err(IngesterError::Internal {
                            message: "failed to wait for instance to be active in ring".to_string(),
                        });
                    }
                    tokio::select! {
                        _ = ctx.canceled() => {
                            return Err(IngesterError::Internal {
                                message: "failed to wait for instance to be active in ring: context canceled".to_string(),
                            });
                        }
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                }
            }
        }

        // Start post-lifecycler services (shipping, idle TSDB closer, TSDB settings, partition lifecycler).
        self.compute_worker_pool
            .start()
            .map_err(|message| IngesterError::Internal { message })?;
        if let Some(ref l) = self.utilization_limiter_svc {
            l.starting()
                .map_err(|message| IngesterError::Internal { message })?;
        }
        {
            let mut new_tasks = Vec::new();
            if let Some(ref l) = self.utilization_limiter_svc {
                let sctx = ctx.child();
                new_tasks.push(self.watch_subservice_task(
                    "ingester-utilization-limiter",
                    tokio::spawn(Arc::clone(l).run(async move {
                        sctx.canceled().await;
                    })),
                ));
            }
            if let Some(ref p) = self.reactive_limiter.prioritizer {
                let sctx = ctx.child();
                new_tasks.push(self.watch_subservice_task(
                    "ingester-reactive-limiter-calibration",
                    tokio::spawn(IngesterReactiveLimiter::run_calibration(
                        Arc::clone(p),
                        self.reactive_limiter.calibration_interval,
                        async move {
                            sctx.canceled().await;
                        },
                    )),
                ));
            }
            if self.blocks_cfg.tsdb.ship_interval.0 > 0 {
                let (ship_tx, ship_rx) = mpsc::channel(16);
                *self.ship_tx.write() = Some(ship_tx);
                new_tasks.push(self.watch_subservice_task(
                    "ingester-shipping",
                    spawn_shipping_loop(Arc::clone(self), ship_rx, ctx.child()),
                ));
            }

            if self.blocks_cfg.tsdb.close_idle_tsdb_timeout.0 > 0 {
                new_tasks.push(self.watch_subservice_task(
                    "ingester-close-idle-tsdb",
                    spawn_idle_tsdb_loop(Arc::clone(self), ctx.child()),
                ));
            }
            let mut tasks = self.background_tasks.lock();
            tasks.extend(new_tasks);

            {
                let ing = Arc::clone(self);
                let sctx = ctx.child();
                let period = Duration::from_nanos(
                    self.cfg.tsdb_config_update_period.as_nanos().max(10_000_000) as u64,
                );
                tasks.push(tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(period);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            _ = sctx.canceled() => break,
                            _ = ticker.tick() => ing.apply_tsdb_settings(),
                        }
                    }
                }));
            }

            // `statisticsService`: a timer service regenerating every tenant's head statistics.
            let planning = &self.blocks_cfg.tsdb.index_lookup_planning;
            if let Some(period) = planning
                .index_lookup_planning_enabled
                .then(|| planning.statistics_collection_frequency.as_std())
                .flatten()
                .filter(|p| !p.is_zero())
            {
                let ing = Arc::clone(self);
                let sctx = ctx.child();
                tasks.push(tokio::spawn(async move {
                    let start = tokio::time::Instant::now() + period;
                    let mut ticker = tokio::time::interval_at(start, period);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            _ = sctx.canceled() => break,
                            _ = ticker.tick() => {
                                let ing = Arc::clone(&ing);
                                let _ = tokio::task::spawn_blocking(move || {
                                    ing.generate_head_statistics_for_all_users();
                                })
                                .await;
                            }
                        }
                    }
                }));
            }
        }

        if let Some(ref plc) = self.partition_lifecycler {
            if !plc.is_started() {
                let psvc = Arc::clone(plc).into_service();
                *self.partition_lifecycler_svc.lock() = Some(Arc::clone(&psvc));
                psvc.start_async(ctx.child())
                    .await
                    .map_err(|e| IngesterError::Internal {
                        message: format!("failed to start partition lifecycler: {e}"),
                    })?;
                tokio::select! {
                    _ = ctx.canceled() => {
                        return Err(IngesterError::Internal {
                            message: "failed to wait for partition lifecycler: context canceled".to_string(),
                        });
                    }
                    r = psvc.await_running() => {
                        r.map_err(|e| IngesterError::Internal {
                            message: format!("partition lifecycler failed to reach running: {e}"),
                        })?;
                    }
                }
            }
        }

        if let Some(ref r_cb) = self.circuit_breaker.read {
            r_cb.activate();
        }
        let is_ro = self
            .lifecycler
            .as_ref()
            .is_some_and(|lc| lc.get_read_only_state().0)
            || self.read_only_override.load(Ordering::Relaxed);
        if !is_ro {
            if let Some(ref p_cb) = self.circuit_breaker.push {
                p_cb.activate();
            }
        }

        let mut st = self.state.write();
        if *st == State::Starting {
            *st = State::Running;
        }
        Ok(())
    }

    /// Gracefully stops the Ingester service, flushing if configured and closing all TSDBs.
    pub async fn stop(self: &Arc<Self>) -> Result<(), IngesterError> {
        let mut rx = self.stop_done_tx.subscribe();
        let should_run_stop = {
            let mut st = self.state.write();
            match *st {
                State::Terminated => return Ok(()),
                State::Stopping => false,
                State::Failed => {
                    if let Some(res) = rx.borrow().clone() {
                        return res.map_err(|message| IngesterError::Internal { message });
                    }
                    *st = State::Stopping;
                    true
                }
                State::New => {
                    *st = State::Terminated;
                    self.stop_done_tx.send_replace(Some(Ok(())));
                    return Ok(());
                }
                State::Starting | State::Running => {
                    *st = State::Stopping;
                    true
                }
            }
        };

        if should_run_stop {
            let ing = Arc::clone(self);
            tokio::spawn(async move {
                struct StopDoneGuard {
                    tx: tokio::sync::watch::Sender<Option<Result<(), String>>>,
                    sent: bool,
                }
                impl Drop for StopDoneGuard {
                    fn drop(&mut self) {
                        if !self.sent {
                            self.tx.send_replace(Some(Err(
                                "ingester stop aborted before completion".to_string(),
                            )));
                        }
                    }
                }
                let mut stop_guard = StopDoneGuard {
                    tx: ing.stop_done_tx.clone(),
                    sent: false,
                };

                let res = ing.stop_inner().await;
                stop_guard.sent = true;
                ing.stop_done_tx.send_replace(Some(
                    res.as_ref().map(|&()| ()).map_err(ToString::to_string),
                ));
            });
        }

        loop {
            if let Some(res) = rx.borrow_and_update().clone() {
                return res.map_err(|message| IngesterError::Internal { message });
            }
            if rx.changed().await.is_err() {
                return Ok(());
            }
        }
    }

    async fn stop_inner(self: &Arc<Self>) -> Result<(), IngesterError> {
        // 1. Stop Kafka reader first so any blocked catchup exits, while keeping `self.ingest_reader`
        // populated so Step 5's lifecycler `flush()` -> `sync_offset_catalogues()` can still read
        // `last_seen_offsets()` and persist `offset-catalogue.json` before shipping blocks.
        let reader_opt = self.ingest_reader.read().clone();
        if let Some(reader) = reader_opt {
            reader.stop().await;
        }

        // 2. Cancel service context & await startup task (if still starting).
        let ctx_opt = self.service_ctx.write().take();
        if let Some(ctx) = ctx_opt {
            ctx.cancel();
        }

        let startup_res = {
            let handle_opt = self.startup_handle.lock().take();
            if let Some(handle) = handle_opt {
                match handle.await {
                    Ok(()) => match self.startup_done_tx.borrow().clone() {
                        Some(Ok(())) | None => Ok(()),
                        Some(Err(message)) => Err(IngesterError::Internal { message }),
                    },
                    Err(e) => Err(IngesterError::Internal {
                        message: format!("ingester startup task panicked: {e}"),
                    }),
                }
            } else {
                match self.startup_done_tx.borrow().clone() {
                    Some(Ok(())) | None => Ok(()),
                    Some(Err(message)) => Err(IngesterError::Internal { message }),
                }
            }
        };

        // 3. Await background loops.
        let tasks = std::mem::take(&mut *self.background_tasks.lock());
        for t in tasks {
            let _ = t.await;
        }
        let pool = Arc::clone(&self.compute_worker_pool);
        let _ = tokio::task::spawn_blocking(move || pool.stop()).await;

        // 4. Stop the partition lifecycler (part of Go's subservicesAfterIngesterRingLifecycler,
        // stopped before the ingester ring lifecycler).
        let plc_svc_opt = self.partition_lifecycler_svc.lock().take();
        if let Some(svc) = plc_svc_opt {
            svc.stop_async();
            let _ = svc.await_terminated().await;
        }

        // 5. Graceful exit from the ring: the lifecycler switches to LEAVING, flushes via the
        // Ingester's FlushTransferer (if flush-on-shutdown is set) while heartbeating, sleeps
        // final_sleep and unregisters (dskit `Lifecycler.stopping`).
        // Without a lifecycler (tests only) flush directly.
        if self.lifecycler.is_none()
            && startup_res.is_ok()
            && self.flush_on_shutdown.load(Ordering::Acquire)
        {
            self.flush().await;
        }

        let lc_svc_opt = self.lifecycler_svc.lock().take();
        if let Some(svc) = lc_svc_opt {
            svc.stop_async();
            let _ = svc.await_terminated().await;
        }

        let reader_opt = self.ingest_reader.write().take();
        if let Some(r) = reader_opt {
            r.stop().await;
        }

        // 6. Remove shutdown marker only if startup succeeded (so a canceled/failed startup that
        // never flushed preserves the marker for the next restart).
        if startup_res.is_ok() {
            let marker_path = shutdownmarker::get_path(&self.blocks_cfg.tsdb.dir);
            let _ = shutdownmarker::remove(&marker_path);
        }

        // 7. Close all TSDBs unless KeepUserTSDBOpenOnShutdown.
        if !self.blocks_cfg.tsdb.keep_user_tsdb_open_on_shutdown {
            self.close_all_tsdbs().await;
        }

        if startup_res.is_err() {
            *self.state.write() = State::Failed;
            return startup_res;
        }

        // A subservice failure makes the ingester service fail (Go: `ingesterRunning` returns
        // the error, `stopping` runs, then the service ends up Failed).
        if let Some(err) = self.subservice_failure() {
            *self.state.write() = State::Failed;
            return Err(IngesterError::Internal { message: err });
        }

        *self.state.write() = State::Terminated;
        Ok(())
    }
}

impl Ingester {
    fn run_initial_owned_series_check(&self) -> Result<(), IngesterError> {
        if let Some(ref strategy) = self.owned_series_strategy {
            match strategy.check_ring_for_changes() {
                Ok(_) => {
                    self.owned_series_initial_ring_check_succeeded
                        .store(true, Ordering::Release);
                    let start = Instant::now();
                    for user_id in self.get_tsdb_users() {
                        if let Some(db) = self.get_tsdb(&user_id) {
                            db.update_owned_series_if_needed(strategy.as_ref(), true);
                        }
                    }
                    self.owned_series_metrics
                        .check_duration
                        .observe(start.elapsed().as_secs_f64());
                }
                Err(e) if e.contains("empty") => {
                    warn!("skipped initial owned series computation, ring is empty");
                    self.owned_series_initial_ring_check_succeeded
                        .store(false, Ordering::Release);
                }
                Err(e) => {
                    return Err(IngesterError::Internal {
                        message: format!("can't read ring: {e}"),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Ported from `pkg/ingester/ingester.go:Config.getIgnoreSeriesLimitForMetricNamesMap`.
pub fn get_ignore_series_limit_for_metric_names_map(raw: &str) -> Option<HashSet<String>> {
    let set: HashSet<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

/// Ported from `pkg/ingester/ingester_tsdb.go:blockGenerationCalculator`.
pub fn block_generation_calculator(
    db: &UserTsdb,
    block_range_ms: i64,
    is_head_range: bool,
    block_min_time: i64,
) -> String {
    if is_head_range {
        return "0".to_string();
    }
    let head_min_time = db.db.head().min_time();
    if head_min_time == i64::MAX {
        return "unknown".to_string();
    }
    let gen = 1i64.max(head_min_time.saturating_sub(block_min_time) / block_range_ms.max(1));
    if gen > 100 {
        "100+".to_string()
    } else {
        gen.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_common::services::start_and_await_running;
    use mimir_proto::ringpb::{PartitionRingDesc, PartitionState};
    use mimir_ring::kv::inmemory::InMemoryKv;
    use mimir_ring::kv::{cas_typed, KvClient};
    use mimir_ring::partition_ring::{
        partition_ring_desc_codec, PartitionRingDescExt, PartitionRingOptions, PartitionRingWatcher,
    };

    #[tokio::test]
    async fn b05_initial_owned_series_reconciles_even_when_check_ring_returns_false() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = prometheus::Registry::new();

        // Populate partition ring with 1 PENDING partition (0 ACTIVE partitions), so
        // OwnedSeriesPartitionRingStrategy::check_ring_for_changes() returns Ok(false).
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        cas_typed::<PartitionRingDesc, _>(kv.as_ref(), "ingester-partitions", |cur| {
            let mut d = cur.cloned().unwrap_or_default();
            d.add_partition(
                0,
                PartitionState::PartitionPending,
                std::time::SystemTime::now(),
            );
            Ok(Some((d, true)))
        })
        .await
        .unwrap();

        let watcher = PartitionRingWatcher::new(
            "ingester-partitions",
            "ingester-partitions",
            Arc::clone(&kv),
            PartitionRingOptions::default(),
            None,
        );
        let w_svc = Arc::clone(&watcher).into_service();
        start_and_await_running(ServiceContext::background(), w_svc.as_ref())
            .await
            .unwrap();

        let mut ing_cfg = mimir_config::IngesterConfig::default();
        ing_cfg.ring.instance_id = "ingester-zone-a-0".to_string();
        ing_cfg.track_ingester_owned_series = true;
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ingest_cfg = mimir_config::IngestStorageConfig {
            enabled: true,
            ..Default::default()
        };

        let ing = Ingester::new(
            ing_cfg,
            blocks_cfg,
            ingest_cfg,
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            Some(Arc::clone(&watcher)),
            None,
            None,
            Some(&reg),
        )
        .unwrap();

        let db = ing.get_or_create_tsdb("t-b05").unwrap();
        // Give the tenant non-empty token ranges and clear any pending recompute reason.
        let strat = ing.owned_series_strategy.as_ref().unwrap();
        assert!(db.update_owned_series_if_needed(strat.as_ref(), true));
        assert!(db.update_token_ranges(vec![0, u32::MAX]));

        // With ring_changed=false, stale ranges are NOT updated.
        assert!(!db.update_owned_series_if_needed(strat.as_ref(), false));

        // Now run_initial_owned_series_check: check_ring_for_changes() returns Ok(false)
        // because there are 0 ACTIVE partitions, yet update_owned_series_if_needed(..., true)
        // still reconciles the stale [0, u32::MAX] ranges back to [].
        ing.run_initial_owned_series_check().unwrap();
        assert!(
            !db.update_token_ranges(vec![]),
            "run_initial_owned_series_check must pass ring_changed=true and reconcile token_ranges to [] even when check_ring_for_changes returns Ok(false)"
        );

        w_svc.stop_async();
        let _ = w_svc.await_terminated().await;
    }
}
