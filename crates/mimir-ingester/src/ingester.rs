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
use mimir_common::limiter::InflightLimiter;
use mimir_common::services::{ManagedBasicService, Service, ServiceContext, State};
use mimir_common::shutdownmarker;
use mimir_config::{
    BlocksStorageConfig, CompartmentsConfig, IngestStorageConfig, IngesterConfig, InstanceLimits,
    Overrides, RuntimeConfigManager,
};
use mimir_proto::ringpb::InstanceState;
use mimir_ring::lifecycler::Lifecycler;
use mimir_ring::partition_ring::{PartitionInstanceLifecycler, PartitionRingWatcher};
use mimir_ring::ring::Ring;
use mimir_storage::bucket::BucketClient;
use mimir_storage::shipper::ShipperMetrics;
use parking_lot::{Mutex, RwLock};
use prometheus::Registry;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{info, warn};

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
    pub metrics: Arc<IngesterMetrics>,
    pub shipper_metrics: ShipperMetrics,
    pub owned_series_metrics: OwnedSeriesMetrics,

    pub instance_ring: Option<Arc<Ring>>,
    pub lifecycler: Option<Arc<dyn IngesterLifecycler>>,
    pub partition_ring_watcher: Option<Arc<PartitionRingWatcher>>,
    pub partition_lifecycler: Option<Arc<PartitionInstanceLifecycler>>,
    pub ingest_partition_id: i32,
    pub ingest_reader: RwLock<Option<Arc<IngestReaderHandle>>>,
    compartments: RwLock<CompartmentsConfig>,

    pub bucket: Option<Arc<BucketClient>>,
    pub limiter: Arc<Limiter>,
    pub owned_series_strategy: Option<Arc<dyn OwnedSeriesRingStrategy>>,
    pub circuit_breaker: IngesterCircuitBreaker,
    pub reactive_limiter: IngesterReactiveLimiter,
    pub inflight_limiter: InflightLimiter,

    pub tsdbs: DashMap<String, Arc<UserTsdb>>,
    pub users_metadata: DashMap<String, Arc<TenantMetadataStore>>,
    tsdb_create_mtx: Mutex<()>,

    pub series_count: Arc<AtomicI64>,
    pub inflight_push_requests: Arc<AtomicI64>,
    pub inflight_push_requests_bytes: Arc<AtomicI64>,
    pub ingestion_rate: Arc<EwmaRate>,
    pub num_compactions_in_progress: AtomicU32,
    pub compaction_idle_timeout: Duration,

    pub error_samplers: ErrorSamplers,
    pub ignored_series_limit_metrics: HashSet<String>,

    state: RwLock<State>,
    service_ctx: RwLock<Option<ServiceContext>>,
    startup_handle: tokio::sync::Mutex<Option<JoinHandle<Result<(), IngesterError>>>>,
    lifecycler_svc: Mutex<Option<Arc<ManagedBasicService>>>,
    partition_lifecycler_svc: Mutex<Option<Arc<ManagedBasicService>>>,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,

    compaction_service_state: AtomicU8,
    owned_series_service_state: AtomicU8,
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
        let bucket = match BucketClient::from_config(&blocks_cfg.bucket) {
            Ok(b) => Some(Arc::new(b)),
            Err(e) => {
                if blocks_cfg.tsdb.ship_interval.0 > 0 {
                    return Err(IngesterError::Internal {
                        message: format!("failed to create bucket client: {e}"),
                    });
                }
                warn!(error = %e, "bucket client unavailable (shipping disabled)");
                None
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
        );
        let reactive_limiter = IngesterReactiveLimiter::new(
            &cfg.push_reactive_limiter,
            &cfg.read_reactive_limiter,
            registry,
        )
        .map_err(|message| IngesterError::Internal { message })?;

        let shipper_metrics = ShipperMetrics::new(registry);
        let owned_series_metrics = OwnedSeriesMetrics::new(registry);

        let ignored_series_limit_metrics = cfg
            .ignore_series_limit_for_metric_names
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect::<HashSet<_>>();

        let idle_timeout_ns = blocks_cfg.tsdb.head_compaction_idle_timeout.as_nanos().max(0) as u64;
        let compaction_idle_timeout = if idle_timeout_ns > 0 {
            let jitter_frac: f64 = rand::random::<f64>() * 0.25;
            Duration::from_nanos(((idle_timeout_ns as f64) * (1.0 + jitter_frac)) as u64)
        } else {
            Duration::ZERO
        };

        let weak_slot: Arc<Mutex<Option<Weak<Ingester>>>> = Arc::new(Mutex::new(None));
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
                let g = w_limits.lock();
                g.as_ref()
                    .and_then(Weak::upgrade)
                    .and_then(|ing| ing.get_instance_limits())
            },
            move || {
                let g = w_min.lock();
                g.as_ref()
                    .and_then(Weak::upgrade)
                    .map_or(0.0, |ing| ing.min_tsdb_head_timestamp())
            },
            move || {
                let g = w_max.lock();
                g.as_ref()
                    .and_then(Weak::upgrade)
                    .map_or(0.0, |ing| ing.max_tsdb_head_timestamp())
            },
            move || {
                let g = w_ship.lock();
                g.as_ref()
                    .and_then(Weak::upgrade)
                    .map_or(0.0, |ing| ing.oldest_unshipped_block_timestamp())
            },
        ));

        let error_samplers = ErrorSamplers::new(cfg.error_sample_rate);

        let ingester = Arc::new(Self {
            cfg,
            blocks_cfg,
            ingest_cfg,
            overrides,
            runtime_cfg,
            registry: registry.cloned(),
            metrics,
            shipper_metrics,
            owned_series_metrics,
            instance_ring,
            lifecycler,
            partition_ring_watcher,
            partition_lifecycler,
            ingest_partition_id,
            ingest_reader: RwLock::new(None),
            compartments: RwLock::new(CompartmentsConfig::default()),
            bucket,
            limiter,
            owned_series_strategy,
            circuit_breaker,
            reactive_limiter,
            inflight_limiter: InflightLimiter::new(),
            tsdbs: DashMap::new(),
            users_metadata: DashMap::new(),
            tsdb_create_mtx: Mutex::new(()),
            series_count,
            inflight_push_requests,
            inflight_push_requests_bytes,
            ingestion_rate,
            num_compactions_in_progress: AtomicU32::new(0),
            compaction_idle_timeout,
            error_samplers,
            ignored_series_limit_metrics,
            state: RwLock::new(State::New),
            service_ctx: RwLock::new(None),
            startup_handle: tokio::sync::Mutex::new(None),
            lifecycler_svc: Mutex::new(None),
            partition_lifecycler_svc: Mutex::new(None),
            background_tasks: Mutex::new(Vec::new()),
            compaction_service_state: AtomicU8::new(State::New as u8),
            owned_series_service_state: AtomicU8::new(State::New as u8),
            owned_series_paused: AtomicBool::new(false),
            metrics_updater_service_state: AtomicU8::new(State::New as u8),
            metadata_purger_service_state: AtomicU8::new(State::New as u8),
            head_compaction_interval_while_starting: RwLock::new(Duration::from_secs(30)),
            head_compaction_interval_jitter_enabled: AtomicBool::new(true),
            min_compaction_loop_delay: RwLock::new(Duration::from_secs(10)),
            close_idle_tsdb_interval: RwLock::new(Duration::from_secs(5 * 60)),
            limit_metrics_update_period: RwLock::new(Duration::from_secs(15)),
            instance_limits_fn: RwLock::new(None),
            force_compact_tx: RwLock::new(None),
            ship_tx: RwLock::new(None),
            verify_ring_ownership: AtomicBool::new(false),
            read_only_override: AtomicBool::new(false),
        });

        *weak_slot.lock() = Some(Arc::downgrade(&ingester));
        Ok(ingester)
    }

    pub fn set_compartments(&self, compartments: CompartmentsConfig) {
        *self.compartments.write() = compartments;
    }

    pub fn compartments_config(&self) -> CompartmentsConfig {
        self.compartments.read().clone()
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
        if let Some(ref lc) = self.lifecycler {
            lc.set_unregister_on_shutdown(self.cfg.ring.unregister_on_shutdown);
            lc.set_flush_on_shutdown(self.blocks_cfg.tsdb.flush_blocks_on_shutdown);
        }
        self.metrics.shutdown_marker.set(0.0);
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

        let db = self.create_tsdb(user_id)?;
        self.tsdbs.insert(user_id.to_string(), Arc::clone(&db));
        self.metrics.mem_users.inc();
        Ok(db)
    }

    fn create_tsdb(&self, user_id: &str) -> Result<Arc<UserTsdb>, IngesterError> {
        let series_count = Arc::clone(&self.series_count);
        let rejected = self.metrics.rejected.clone();
        let owned_shard_size = self
            .owned_series_strategy
            .as_ref()
            .map_or(0, |s| s.shard_size_for_user(user_id));

        let weak_self = {
            // Reconstruct weak check via runtime_cfg / state check
            let cfg_limits = self.cfg.instance_limits.clone();
            let rt = self.runtime_cfg.clone();
            let state_ref = &self.state as *const RwLock<State>;
            let _ = state_ref;
            let max_series_fn: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(move || {
                if let Some(ref r) = rt {
                    if let Some(l) = r.ingester_limits() {
                        return l.max_series;
                    }
                }
                cfg_limits.max_series
            });
            max_series_fn
        };

        UserTsdb::open(
            user_id.to_string(),
            &self.cfg,
            &self.blocks_cfg,
            self.overrides.clone(),
            Arc::clone(&self.limiter),
            self.bucket.as_deref(),
            self.shipper_metrics.clone(),
            self.registry.as_ref(),
            series_count,
            weak_self,
            rejected,
            self.ignored_series_limit_metrics.clone(),
            self.ingest_partition_id,
            owned_shard_size,
        )
        .map_err(|e| IngesterError::Internal {
            message: format!("failed to open TSDB for user {user_id}: {e}"),
        })
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
        let mut min_t = i64::MAX;
        for entry in self.tsdbs.iter() {
            let h = entry.value().db.head();
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
        let mut max_t = i64::MIN;
        for entry in self.tsdbs.iter() {
            let h = entry.value().db.head();
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
        let mut oldest = 0u64;
        for entry in self.tsdbs.iter() {
            let ts = entry.value().oldest_unshipped_block_time_ms();
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

    pub async fn open_existing_tsdbs(&self) -> Result<(), IngesterError> {
        let start = Instant::now();
        let user_ids = self.find_user_ids_with_tsdb_on_filesystem()?;
        if user_ids.is_empty() {
            return Ok(());
        }
        info!(count = user_ids.len(), "opening existing TSDBs");
        for user_id in user_ids {
            let db = self.create_tsdb(&user_id)?;
            self.tsdbs.insert(user_id, db);
            self.metrics.mem_users.inc();
        }
        self.metrics
            .open_existing_tsdb
            .inc_by(start.elapsed().as_secs_f64());
        Ok(())
    }

    pub async fn close_all_tsdbs(&self) {
        let keys = self.get_tsdb_users();
        for k in &keys {
            if let Some(db) = self.get_tsdb(k) {
                db.set_closing_state();
            }
        }
        for k in &keys {
            if let Some(db) = self.get_tsdb(k) {
                db.wait_in_flight_appends().await;
            }
        }
        for k in keys {
            if let Some((_, db)) = self.tsdbs.remove(&k) {
                let tracker_names = db.active_series.current_matcher_names();
                if let Err(e) = db.close() {
                    warn!(user = %k, error = %e, "unable to close TSDB");
                }
                self.metrics.mem_users.dec();
                self.metrics
                    .delete_per_user_custom_tracker_metrics(&k, &tracker_names);
            }
        }
    }

    /// Starts the Ingester service asynchronously, returning immediately while startup progresses.
    pub async fn start_async(self: &Arc<Self>) -> Result<(), IngesterError> {
        {
            let mut st = self.state.write();
            if *st != State::New {
                return Err(IngesterError::Internal {
                    message: format!("invalid ingester state on start: {}", *st),
                });
            }
            *st = State::Starting;
        }

        let ctx = ServiceContext::background();
        *self.service_ctx.write() = Some(ctx.clone());

        let ing = Arc::clone(self);
        let handle = tokio::spawn(async move { ing.run_starting(ctx).await });
        *self.startup_handle.lock().await = Some(handle);
        Ok(())
    }

    /// Waits for `start_async` to finish transitioning the Ingester into `State::Running`.
    pub async fn await_running(&self) -> Result<(), IngesterError> {
        let handle_opt = self.startup_handle.lock().await.take();
        if let Some(handle) = handle_opt {
            match handle.await {
                Ok(res) => res,
                Err(e) => Err(IngesterError::Internal {
                    message: format!("ingester startup task panicked: {e}"),
                }),
            }
        } else {
            Ok(())
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
            // Stop lifecycler if starting failed, matching Go's defer in starting().
            let lc_svc_opt = self.lifecycler_svc.lock().take();
            if let Some(svc) = lc_svc_opt {
                svc.stop_async();
                let _ = svc.await_terminated().await;
            }
            let plc_svc_opt = self.partition_lifecycler_svc.lock().take();
            if let Some(svc) = plc_svc_opt {
                svc.stop_async();
                let _ = svc.await_terminated().await;
            }
            *self.state.write() = State::Failed;
        }
        res
    }

    async fn run_starting_inner(self: &Arc<Self>, ctx: &ServiceContext) -> Result<(), IngesterError> {
        let tsdb_dir = PathBuf::from(&self.blocks_cfg.tsdb.dir);
        fs::create_dir_all(&tsdb_dir).map_err(|e| IngesterError::Internal {
            message: format!("failed to create TSDB dir: {e}"),
        })?;

        let marker_path = shutdownmarker::get_path(&tsdb_dir);
        if shutdownmarker::exists(&marker_path).unwrap_or(false) {
            info!(path = %marker_path.display(), "detected existing shutdown marker");
            self.set_prepare_shutdown();
        }

        if let Err(e) = self.open_existing_tsdbs().await {
            self.close_all_tsdbs().await;
            return Err(e);
        }

        // Start owned series loop before partition replay and lifecycler.
        if self.owned_series_strategy.is_some() {
            if let Some(ref strategy) = self.owned_series_strategy {
                if let Ok(changed) = strategy.check_ring_for_changes() {
                    for user_id in self.get_tsdb_users() {
                        if let Some(db) = self.get_tsdb(&user_id) {
                            db.update_owned_series_if_needed(strategy.as_ref(), changed);
                        }
                    }
                }
            }
            self.background_tasks
                .lock()
                .push(spawn_owned_series_loop(Arc::clone(self), ctx.child()));
        }

        let (fc_tx, fc_rx) = mpsc::channel(16);
        *self.force_compact_tx.write() = Some(fc_tx);

        // Start compaction, metrics updater, and metadata purger before partition replay.
        {
            let mut tasks = self.background_tasks.lock();
            tasks.push(spawn_compaction_loop(Arc::clone(self), fc_rx, ctx.child()));
            tasks.push(spawn_metrics_updater_loop(Arc::clone(self), ctx.child()));
            tasks.push(spawn_metadata_purger_loop(Arc::clone(self), ctx.child()));
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
        {
            let mut tasks = self.background_tasks.lock();
            if self.blocks_cfg.tsdb.ship_interval.0 > 0 {
                let (ship_tx, ship_rx) = mpsc::channel(16);
                *self.ship_tx.write() = Some(ship_tx);
                tasks.push(spawn_shipping_loop(Arc::clone(self), ship_rx, ctx.child()));
            }

            if self.blocks_cfg.tsdb.close_idle_tsdb_timeout.0 > 0 {
                tasks.push(spawn_idle_tsdb_loop(Arc::clone(self), ctx.child()));
            }

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

        *self.state.write() = State::Running;
        Ok(())
    }

    /// Gracefully stops the Ingester service, flushing if configured and closing all TSDBs.
    pub async fn stop(self: &Arc<Self>) -> Result<(), IngesterError> {
        {
            let mut st = self.state.write();
            if *st == State::Terminated || *st == State::Stopping {
                return Ok(());
            }
            *st = State::Stopping;
        }

        // 1. Stop Kafka reader first so any blocked catchup exits.
        let reader_opt = self.ingest_reader.write().take();
        if let Some(reader) = reader_opt {
            reader.stop().await;
        }

        // 2. Cancel service context & await startup task (if still starting).
        let ctx_opt = self.service_ctx.write().take();
        if let Some(ctx) = ctx_opt {
            ctx.cancel();
        }

        let startup_res = {
            let handle_opt = self.startup_handle.lock().await.take();
            if let Some(handle) = handle_opt {
                match handle.await {
                    Ok(r) => r,
                    Err(e) => Err(IngesterError::Internal {
                        message: format!("ingester startup task panicked: {e}"),
                    }),
                }
            } else {
                Ok(())
            }
        };

        // 3. Await background loops.
        let tasks = std::mem::take(&mut *self.background_tasks.lock());
        for t in tasks {
            let _ = t.await;
        }

        // 4. Flush if configured via lifecycler / blocks config (only if startup succeeded).
        if startup_res.is_ok() {
            let should_flush = self
                .lifecycler
                .as_ref()
                .map_or(self.blocks_cfg.tsdb.flush_blocks_on_shutdown, |lc| {
                    lc.flush_on_shutdown()
                });
            if should_flush {
                self.flush().await;
            }
        }

        // 5. Stop partition lifecycler & classic lifecycler.
        let plc_svc_opt = self.partition_lifecycler_svc.lock().take();
        if let Some(svc) = plc_svc_opt {
            svc.stop_async();
            let _ = svc.await_terminated().await;
        }

        let lc_svc_opt = self.lifecycler_svc.lock().take();
        if let Some(svc) = lc_svc_opt {
            svc.stop_async();
            let _ = svc.await_terminated().await;
        }

        // 6. Remove shutdown marker.
        let marker_path = shutdownmarker::get_path(&self.blocks_cfg.tsdb.dir);
        let _ = shutdownmarker::remove(&marker_path);

        // 7. Close all TSDBs.
        self.close_all_tsdbs().await;

        if startup_res.is_err() {
            *self.state.write() = State::Failed;
            return startup_res;
        }

        *self.state.write() = State::Terminated;
        Ok(())
    }
}
