//! Per-tenant TSDB wrapper, EWMA rate counters, lifecycle callbacks, and compaction/shipper state.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/user_tsdb.go
//! - github.com/grafana/mimir/pkg/ingester/user_metrics_metadata.go
//! - github.com/grafana/mimir/pkg/util/math/rate.go

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_config::{BlocksStorageConfig, IngesterConfig, Overrides};
use mimir_proto::cortexpb;
use mimir_storage::bucket::{tenant_deletion_mark_exists, BucketClient};
use mimir_storage::ingest::{OffsetCatalogue, OffsetCatalogueMetrics};
use mimir_storage::shipper::{read_shipped_blocks, Shipper, ShipperError, ShipperMetrics, SourceType};
use mimir_tsdb::chunkenc::Encoding;
use mimir_tsdb::db::{BlocksReloadedFn, BlocksToDeleteFn, Tsdb, TsdbError, TsdbExtraOptions, TsdbOptions};
use mimir_tsdb::head::{HeadError, HeadOptions, SeriesLifecycleCallback};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::metadata::MetadataLimits;
use mimir_tsdb::postings_cache::{
    PostingsForMatchersCache, PostingsForMatchersCacheConfig, PostingsForMatchersCacheMetrics,
};
use mimir_tsdb::wlog::CompressionType;
use parking_lot::{Mutex, RwLock};
use prometheus::{CounterVec, Registry};
use tokio::sync::Notify;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::active_series::{
    build_custom_trackers_config, merged_custom_trackers, ActiveSeries, CostAttributionTracker,
    Matchers,
};
use crate::errors::IngesterError;
use crate::label_churn::{SeriesRefHistory, LABEL_CHURN_RECORDS};
use crate::limiter::Limiter;
use crate::lookupplan::{
    head_planning_meta, HeadIndexReader, LookupPlanner, NoopPlanner, PlannerProvider,
};
use crate::metric_counter::MetricCounter;
use crate::metrics::REASON_MAX_IN_MEMORY_SERIES;
use crate::owned_series::{
    secondary_tsdb_hash_for_user, token_ranges_include_key, OwnedSeriesRingStrategy,
    RECOMPUTE_OWNED_SERIES_REASON_GET_TOKEN_RANGES_FAILED,
    RECOMPUTE_OWNED_SERIES_REASON_LOCAL_LIMIT_CHANGED,
    RECOMPUTE_OWNED_SERIES_REASON_NEW_USER, RECOMPUTE_OWNED_SERIES_REASON_RING_CHANGED,
    RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED,
};

/// EWMA rate tracker (`pkg/util/math/rate.go`).
pub struct EwmaRate {
    alpha: f64,
    interval_secs: f64,
    new_events: AtomicI64,
    state: Mutex<(f64, bool)>,
}

impl EwmaRate {
    pub fn new(alpha: f64, interval: Duration) -> Self {
        let secs = interval.as_secs_f64().max(0.001);
        Self {
            alpha,
            interval_secs: secs,
            new_events: AtomicI64::new(0),
            state: Mutex::new((0.0, false)),
        }
    }

    pub fn add(&self, delta: i64) {
        self.new_events.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.add(1);
    }

    pub fn rate(&self) -> f64 {
        self.state.lock().0
    }

    pub fn tick(&self) {
        let new_events = self.new_events.swap(0, Ordering::AcqRel);
        let instant_rate = (new_events as f64) / self.interval_secs;
        let mut st = self.state.lock();
        if st.1 {
            st.0 += self.alpha * (instant_rate - st.0);
        } else {
            st.1 = true;
            st.0 = instant_rate;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum TsdbState {
    Active = 0,
    ActiveShipping = 1,
    ForceCompacting = 2,
    Closing = 3,
    Closed = 4,
}

impl TsdbState {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Active,
            1 => Self::ActiveShipping,
            2 => Self::ForceCompacting,
            3 => Self::Closing,
            _ => Self::Closed,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::ActiveShipping => "activeShipping",
            Self::ForceCompacting => "forceCompacting",
            Self::Closing => "closing",
            Self::Closed => "closed",
        }
    }
}

/// RAII guard restoring `TsdbState::Active` from `from` on drop unless disarmed
/// (e.g. after `Closing -> Closed` succeeds). Holds an `Arc<UserTsdb>` so it is
/// `Send + 'static` and can be moved into `tokio::task::spawn_blocking` closures.
pub struct TsdbStateGuard {
    user_db: Option<Arc<UserTsdb>>,
    from: TsdbState,
    armed: bool,
}

impl TsdbStateGuard {
    pub fn new(user_db: Arc<UserTsdb>, from: TsdbState) -> Self {
        Self {
            user_db: Some(user_db),
            from,
            armed: true,
        }
    }

    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TsdbStateGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Some(ref user_db) = self.user_db {
                let _ = user_db.change_state(self.from, TsdbState::Active);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TsdbCloseCheckResult {
    Idle,
    ShippingDisabled,
    NotIdle,
    NotCompacted,
    NotShipped,
    CheckFailed,
    CloseFailed,
    NotActive,
    DataRemovalFailed,
    TenantMarkedForDeletion,
    IdleClosed,
}

impl TsdbCloseCheckResult {
    pub fn should_close(self) -> bool {
        matches!(self, Self::Idle | Self::TenantMarkedForDeletion)
    }

    pub fn as_label(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::ShippingDisabled => "shipping_disabled",
            Self::NotIdle => "not_idle",
            Self::NotCompacted => "not_compacted",
            Self::NotShipped => "not_shipped",
            Self::CheckFailed => "check_failed",
            Self::CloseFailed => "close_failed",
            Self::NotActive => "not_active",
            Self::DataRemovalFailed => "data_removal_failed",
            Self::TenantMarkedForDeletion => "tenant_marked_for_deletion",
            Self::IdleClosed => "idle_closed",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct OwnedSeriesState {
    pub owned_series_count: usize,
    pub shard_size: usize,
    pub local_series_limit: usize,
}

pub struct AppendLockGuard<'a> {
    user_tsdb: &'a UserTsdb,
    acquired_state: TsdbState,
}

impl Drop for AppendLockGuard<'_> {
    fn drop(&mut self) {
        self.user_tsdb.release_append_lock(self.acquired_state);
    }
}

struct LifecycleCallbackAdapter {
    user_id: String,
    limiter_slot: RwLock<Option<Arc<Limiter>>>,
    series_in_metric: Arc<MetricCounter>,
    active_series: Arc<ActiveSeries>,
    instance_series_count: Arc<AtomicI64>,
    max_in_memory_series_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
    rejected_counter: CounterVec,
    owned_state: Arc<Mutex<OwnedSeriesState>>,
    use_owned_series_for_limits: AtomicBool,
    num_series_fn: RwLock<Option<Arc<dyn Fn() -> u64 + Send + Sync>>>,
    pending_series_removals: Arc<AtomicI64>,
    postings_cache_invalidations: Option<prometheus::Counter>,
    postings_cache_invalidation_enabled: Arc<AtomicBool>,
}

impl SeriesLifecycleCallback for LifecycleCallbackAdapter {
    fn pre_creation(&self, pairs: &[(&str, &str)]) -> Result<(), HeadError> {
        let limiter_guard = self.limiter_slot.read();
        let Some(ref limiter) = *limiter_guard else {
            return Ok(());
        };

        let max_instance = (self.max_in_memory_series_fn)();
        if max_instance > 0 && self.instance_series_count.load(Ordering::Relaxed) >= max_instance {
            self.rejected_counter
                .with_label_values(&[REASON_MAX_IN_MEMORY_SERIES])
                .inc();
            return Err(HeadError::MaxInMemorySeriesReached);
        }

        let (series_count, min_local_limit) =
            if self.use_owned_series_for_limits.load(Ordering::Acquire) {
                let os = self.owned_state.lock();
                (os.owned_series_count, os.local_series_limit)
            } else {
                let head_series = self
                    .num_series_fn
                    .read()
                    .as_ref()
                    .map_or(0, |f| f() as usize);
                (head_series, 0)
            };

        if !limiter.is_within_max_series_per_user(&self.user_id, series_count, min_local_limit) {
            return Err(HeadError::MaxSeriesPerUser);
        }

        let metric_name = pairs
            .iter()
            .find(|(k, _)| *k == "__name__")
            .map(|(_, v)| *v)
            .unwrap_or("");
        if metric_name.is_empty() {
            return Err(HeadError::MissingMetricName);
        }
        if !self
            .series_in_metric
            .can_add_series_for(&self.user_id, metric_name)
        {
            return Err(HeadError::MaxSeriesPerMetric);
        }

        Ok(())
    }

    fn post_creation(&self, _ref_id: u64, labels: &Labels) {
        self.instance_series_count.fetch_add(1, Ordering::Relaxed);
        {
            let mut os = self.owned_state.lock();
            os.owned_series_count += 1;
        }
        let metric_name = labels.get("__name__");
        if !metric_name.is_empty() {
            self.series_in_metric.increase_series_for_metric(metric_name);
            if self.postings_cache_invalidation_enabled.load(Ordering::Acquire) {
                if let Some(ref c) = self.postings_cache_invalidations {
                    c.inc();
                }
            }
        }
    }

    fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        if deleted.is_empty() {
            return;
        }
        self.instance_series_count
            .fetch_sub(deleted.len() as i64, Ordering::Relaxed);
        self.pending_series_removals
            .fetch_add(1, Ordering::Relaxed);
        for (_, lbls) in deleted {
            let metric_name = lbls.get("__name__");
            if !metric_name.is_empty() {
                self.series_in_metric.decrease_series_for_metric(metric_name);
            }
        }
        self.active_series.post_deletion(deleted);
    }
}

fn compute_oldest_unshipped_ms(
    blocks: &[Arc<mimir_tsdb::block::Block>],
    shipped: &HashMap<Ulid, SystemTime>,
) -> u64 {
    let mut oldest = 0u64;
    for b in blocks {
        let uid = b.ulid();
        if shipped.contains_key(&uid) {
            continue;
        }
        let ts = uid.timestamp_ms();
        if oldest == 0 || ts < oldest {
            oldest = ts;
        }
    }
    oldest
}

/// Async block shipper interface matching Go `BlocksUploader` (`pkg/ingester/user_tsdb.go`).
pub trait BlockShipper: Send + Sync {
    fn sync(&self) -> Pin<Box<dyn Future<Output = Result<usize, ShipperError>> + Send + '_>>;
}

impl BlockShipper for Shipper {
    fn sync(&self) -> Pin<Box<dyn Future<Output = Result<usize, ShipperError>> + Send + '_>> {
        Box::pin(Shipper::sync(self))
    }
}

/// Per-tenant TSDB + tracking state (`pkg/ingester/user_tsdb.go`).
pub struct UserTsdb {
    pub user_id: String,
    pub db: Arc<Tsdb>,
    pub active_series: Arc<ActiveSeries>,
    pub series_in_metric: Arc<MetricCounter>,
    pub limiter: Arc<Limiter>,
    pub overrides: Overrides,

    pub ingested_api_samples: EwmaRate,
    pub ingested_rule_samples: EwmaRate,
    pub series_ref_history: SeriesRefHistory,

    pub shipper: RwLock<Option<Arc<dyn BlockShipper>>>,
    pub offset_catalogue: Option<OffsetCatalogue>,

    state_mtx: parking_lot::RwLock<()>,
    pub(crate) compaction_mtx: tokio::sync::Mutex<()>,
    state: AtomicU8,
    forced_compaction_max_time: AtomicI64,
    in_flight_appends: AtomicI64,
    in_flight_appends_before_forced: AtomicI64,
    in_flight_notify: Notify,
    in_flight_before_forced_notify: Notify,

    last_update_ms: AtomicI64,
    pub deletion_mark_found: AtomicBool,
    pub last_deletion_mark_check_sec: AtomicI64,
    last_early_compaction_ms: AtomicI64,
    pub pending_series_removals: Arc<AtomicI64>,
    pub last_series_removal_ms: AtomicI64,

    block_duration_ms: i64,
    shipped_blocks: Arc<Mutex<HashMap<Ulid, SystemTime>>>,
    oldest_unshipped_block_ts_ms: Arc<AtomicU64>,

    owned_state: Arc<Mutex<OwnedSeriesState>>,
    owned_token_ranges: Mutex<Vec<u32>>,
    requires_owned_series_update: Mutex<Option<String>>,
    pending_non_owned_refs: Mutex<HashMap<u64, SystemTime>>,
    early_compaction_non_owned_enabled: AtomicBool,

    lifecycle_adapter: Arc<LifecycleCallbackAdapter>,
    current_metadata_limits: Arc<Mutex<MetadataLimits>>,

    /// `userTSDB.plannerProvider`: set when index lookup planning is enabled.
    planner_provider: std::sync::OnceLock<Arc<PlannerProvider>>,
    ingested_samples_counter: std::sync::OnceLock<prometheus::Counter>,
    ingested_samples_fail_counter: std::sync::OnceLock<prometheus::Counter>,
}

impl UserTsdb {
    #[inline]
    pub fn ingested_samples_counter(&self, vec: &CounterVec) -> &prometheus::Counter {
        self.ingested_samples_counter
            .get_or_init(|| vec.with_label_values(&[&self.user_id]))
    }

    #[inline]
    pub fn ingested_samples_fail_counter(&self, vec: &CounterVec) -> &prometheus::Counter {
        self.ingested_samples_fail_counter
            .get_or_init(|| vec.with_label_values(&[&self.user_id]))
    }
    /// Enables index lookup planning for this tenant (`newPlannerProvider(plannerFactory)`).
    pub fn set_planner_provider(&self, provider: Arc<PlannerProvider>) {
        let _ = self.planner_provider.set(provider);
    }

    /// `userTSDB.generateHeadStatistics`: computes and caches the Head's lookup planner.
    pub fn generate_head_statistics(&self) {
        if let Some(provider) = self.planner_provider.get() {
            let head = self.db.head();
            provider.generate_and_store_planner(&head_planning_meta(head), &HeadIndexReader(head));
        }
    }

    /// `getIndexLookupPlannerFunc` for the Head: the cached planner (or one generated on demand),
    /// `NoopPlanner` when planning is disabled.
    pub fn head_lookup_planner(&self) -> Arc<dyn LookupPlanner> {
        match self.planner_provider.get() {
            Some(provider) => {
                let head = self.db.head();
                provider.get_planner(&head_planning_meta(head), &HeadIndexReader(head))
            }
            None => Arc::new(NoopPlanner),
        }
    }

    /// `getIndexLookupPlannerFunc` for a persisted ULID block (cached once directly on `Block`,
    /// matching Go `index.Reader.lookupPlanner`).
    pub fn block_lookup_planner(&self, block: &mimir_tsdb::block::Block) -> Arc<dyn LookupPlanner> {
        let Some(provider) = self.planner_provider.get() else {
            return Arc::new(NoopPlanner);
        };
        let slot = block.get_or_init_lookup_planner(|| {
            let planner: Arc<dyn LookupPlanner> =
                provider.get_planner(&block.meta().into(), block.index());
            Arc::new(planner)
        });
        match slot.downcast_ref::<Arc<dyn LookupPlanner>>() {
            Some(planner) => Arc::clone(planner),
            None => Arc::new(NoopPlanner),
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        user_id: String,
        ingester_cfg: &IngesterConfig,
        blocks_cfg: &BlocksStorageConfig,
        overrides: Overrides,
        limiter: Arc<Limiter>,
        bucket: Option<&BucketClient>,
        shipper_metrics: ShipperMetrics,
        shared_metrics: &SharedTsdbMetrics,
        tsdb_prom_reg: Option<Arc<Registry>>,
        wal_replay_concurrency: usize,
        instance_series_count: Arc<AtomicI64>,
        max_in_memory_series_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
        rejected_counter: CounterVec,
        ignored_metrics: HashSet<String>,
        ingest_partition_id: i32,
        owned_shard_size: usize,
        cost_attribution_tracker: Option<Arc<CostAttributionTracker>>,
    ) -> Result<Arc<Self>, TsdbError> {
        let udir = PathBuf::from(&blocks_cfg.tsdb.dir).join(&user_id);
        let rate_period = Duration::from_nanos(
            ingester_cfg.rate_update_period.as_nanos().max(1_000_000) as u64,
        );
        let active_idle = Duration::from_nanos(
            ingester_cfg
                .active_series
                .active_series_metrics_idle_timeout
                .as_nanos()
                .max(0) as u64,
        );
        let tenant_limits = overrides.for_tenant(&user_id);
        let custom_trackers = build_custom_trackers_config(&merged_custom_trackers(
            &tenant_limits.active_series_custom_trackers,
            &tenant_limits.active_series_additional_custom_trackers,
        ));

        let active_series = Arc::new(ActiveSeries::with_matchers(
            Matchers::new(custom_trackers),
            active_idle,
            cost_attribution_tracker,
        ));
        let series_in_metric = Arc::new(MetricCounter::new(Arc::clone(&limiter), ignored_metrics));
        let initial_local_limit = limiter.max_series_per_user(&user_id, 0);

        let owned_state = Arc::new(Mutex::new(OwnedSeriesState {
            owned_series_count: 0,
            shard_size: owned_shard_size,
            local_series_limit: initial_local_limit,
        }));
        let pending_series_removals = Arc::new(AtomicI64::new(0));
        let postings_cache_invalidation_enabled = Arc::new(AtomicBool::new(false));
        let postings_cache_invalidations = if blocks_cfg.tsdb.shared_postings_for_matchers_cache
            && blocks_cfg.tsdb.head_postings_for_matchers_cache_invalidation
        {
            shared_metrics
                .head_postings_cache
                .as_ref()
                .map(|m| m.invalidations().clone())
        } else {
            None
        };

        let lifecycle_adapter = Arc::new(LifecycleCallbackAdapter {
            user_id: user_id.clone(),
            limiter_slot: RwLock::new(None), // disabled during WAL replay
            series_in_metric: Arc::clone(&series_in_metric),
            active_series: Arc::clone(&active_series),
            instance_series_count: Arc::clone(&instance_series_count),
            max_in_memory_series_fn,
            rejected_counter,
            owned_state: Arc::clone(&owned_state),
            use_owned_series_for_limits: AtomicBool::new(
                ingester_cfg.use_ingester_owned_series_for_limits,
            ),
            num_series_fn: RwLock::new(None),
            pending_series_removals: Arc::clone(&pending_series_removals),
            postings_cache_invalidations,
            postings_cache_invalidation_enabled: Arc::clone(&postings_cache_invalidation_enabled),
        });

        let block_duration_ms = blocks_cfg
            .tsdb
            .block_ranges_period
            .first()
            .map(|d| d.as_millis().max(1))
            .unwrap_or(2 * 3600 * 1000);
        let ooo_window_ms = (tenant_limits.out_of_order_time_window.0 / 1_000_000).max(0);
        let max_exemplars = limiter.max_exemplars_per_user(&user_id);
        let float_chunk_encoding = if tenant_limits.float_chunk_encoding == "xor2" {
            Encoding::Xor2
        } else {
            Encoding::Xor
        };
        let enable_histogram_st_encoding =
            tenant_limits.histogram_chunk_encoding == "histogram_st";

        let initial_metadata_limits = MetadataLimits {
            max_metrics_with_metadata_per_user: limiter.max_metadata_per_user(&user_id),
            max_metadata_per_metric: limiter.max_metadata_per_metric(&user_id),
        };
        let head_opts = HeadOptions {
            chunk_range_ms: block_duration_ms,
            samples_per_chunk: mimir_tsdb::head::DEFAULT_SAMPLES_PER_CHUNK,
            stripe_size: (blocks_cfg.tsdb.stripe_size.max(2)) as usize,
            enable_exemplar_storage: true,
            max_exemplars,
            ooo_time_window_ms: ooo_window_ms,
            ooo_cap_max: (blocks_cfg.tsdb.out_of_order_capacity_max.clamp(1, 255)) as usize,
            enable_st_storage: false,
            float_chunk_encoding,
            enable_histogram_st_encoding,
            wal_compression: if blocks_cfg.tsdb.wal_compression_enabled {
                CompressionType::Snappy
            } else {
                CompressionType::None
            },
            // Go `wlog.NewSize` rejects sizes that are not a multiple of the 32 KiB page.
            wal_segment_size: blocks_cfg.tsdb.wal_segment_size_bytes.max(0) as usize,
            wal_replay_concurrency,
            max_head_chunk_file_size: mimir_tsdb::head_chunks::MAX_HEAD_CHUNK_FILE_SIZE,
            head_chunks_write_queue_size: blocks_cfg.tsdb.head_chunks_write_queue_size.max(0)
                as usize,
            head_chunks_write_buffer_size: blocks_cfg.tsdb.head_chunks_write_buffer_size_bytes.max(0)
                as usize,
            timely_compaction: blocks_cfg.tsdb.timely_head_compaction_enabled,
            // Go `headPostingsForMatchersCacheFactory` (MaxItems: math.MaxInt).
            postings_cache: head_postings_for_matchers_cache_config(blocks_cfg, shared_metrics),
            shared_postings_cache: blocks_cfg
                .tsdb
                .shared_postings_for_matchers_cache
                .then(|| shared_metrics.get_or_init_shared_head_cache(blocks_cfg)),
            metadata_limits: initial_metadata_limits,
            series_lifecycle_callback: Some(lifecycle_adapter.clone()),
            registry: tsdb_prom_reg.clone(),
        }
        .with_chunk_end_time_variance(blocks_cfg.tsdb.head_chunks_end_time_variance);

        // Go `blocksToDelete` closure + `userTSDB.blocksToDelete`: nothing is deleted until
        // the TSDB is ready (after the post-replay compaction); then only blocks deletable by
        // Prometheus' default rules (compacted / beyond `retention_period`) that were shipped
        // (or, without shipping, created) more than `retention_period` ago.
        let block_min_retention = Duration::from_nanos(
            blocks_cfg.tsdb.retention_period.as_nanos().max(0) as u64,
        );
        let shipping_enabled = blocks_cfg.tsdb.ship_interval.0 > 0;
        let has_shipper = shipping_enabled && bucket.is_some();
        let shipped_blocks: Arc<Mutex<HashMap<Ulid, SystemTime>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let oldest_unshipped_block_ts_ms = Arc::new(AtomicU64::new(0));
        let user_db_ready = Arc::new(AtomicBool::new(false));
        let on_blocks_reload: BlocksReloadedFn = {
            let shipped_blocks = Arc::clone(&shipped_blocks);
            let oldest_ts = Arc::clone(&oldest_unshipped_block_ts_ms);
            Arc::new(move |get_blocks| {
                let shipped = shipped_blocks.lock();
                let blocks = get_blocks();
                let oldest = compute_oldest_unshipped_ms(&blocks, &shipped);
                oldest_ts.store(oldest, Ordering::Release);
            })
        };
        let blocks_to_delete: BlocksToDeleteFn = {
            let shipped_blocks = Arc::clone(&shipped_blocks);
            let user_db_ready = Arc::clone(&user_db_ready);
            Arc::new(move |blocks, default_blocks_to_delete| {
                if !user_db_ready.load(Ordering::Acquire) {
                    return HashSet::new();
                }
                let deletable = default_blocks_to_delete(blocks);
                let deadline = SystemTime::now()
                    .checked_sub(block_min_retention)
                    .unwrap_or(UNIX_EPOCH);
                if has_shipper {
                    let shipped = shipped_blocks.lock();
                    return deletable
                        .into_iter()
                        .filter(|id| shipped.get(id).is_some_and(|&t| t < deadline))
                        .collect();
                }
                deletable
                    .into_iter()
                    .filter(|id| UNIX_EPOCH + Duration::from_millis(id.timestamp_ms()) < deadline)
                    .collect()
            })
        };

        let tsdb_opts = TsdbOptions {
            head: head_opts,
            max_block_chunk_segment_size: 0,
            no_lockfile: true,
            retention_duration_ms: block_min_retention.as_millis() as i64,
            blocks_to_delete: Some(blocks_to_delete),
            on_blocks_reload: Some(on_blocks_reload),
            enable_bigger_ooo_block_for_old_samples: blocks_cfg
                .tsdb
                .bigger_out_of_order_blocks_for_old_samples,
            // Go `blockPostingsForMatchersCacheFactory` (no invalidation, MaxItems: math.MaxInt).
            block_postings_cache: block_postings_for_matchers_cache_config(blocks_cfg, shared_metrics),
            shared_block_postings_cache: blocks_cfg
                .tsdb
                .shared_postings_for_matchers_cache
                .then(|| shared_metrics.get_or_init_shared_block_cache(blocks_cfg)),
            compaction_delay: Duration::ZERO,
        };

        if let Some(ref reg) = tsdb_prom_reg {
            if let Some(collector) = HeadChunksStorageSizeCollector::new(udir.join("chunks_head")) {
                let _ = reg.register(Box::new(collector));
            }
        }

        // Upstream Go Mimir ingester explicitly configures:
        //   IsolationDisabled: true (ingester_tsdb.go:217)
        //   EnableOverlappingCompaction: false (ingester_tsdb.go:219: only upload lvl 1 blocks)
        let db = Arc::new(Tsdb::open_with_extra(
            &udir,
            tsdb_opts,
            TsdbExtraOptions {
                isolation_disabled: true,
                ..Default::default()
            },
        )?);
        db.set_enable_overlapping_compaction(false);
        // Go `createTSDB` only forwards `MinBlockDuration: blockRanges[0]` and
        // `MaxBlockDuration: blockRanges[len-1]`; Prometheus `tsdb.validateOpts` + `open` then
        // derive the compactor ranges as `ExponentialBlockRanges(min, 10, 3)` truncated at the
        // first range exceeding `MaxBlockDuration` (intermediate configured entries are unused).
        let configured_ranges_ms: Vec<i64> = blocks_cfg
            .tsdb
            .block_ranges_period
            .iter()
            .map(|d| d.as_millis().max(1))
            .collect();
        if let (Some(&min_ms), Some(&last_ms)) =
            (configured_ranges_ms.first(), configured_ranges_ms.last())
        {
            let max_ms = last_ms.max(min_ms);
            let mut block_ranges_ms = Vec::with_capacity(10);
            let mut cur = min_ms;
            for _ in 0..10 {
                if cur > max_ms {
                    break;
                }
                block_ranges_ms.push(cur);
                match cur.checked_mul(3) {
                    Some(next) => cur = next,
                    None => break,
                }
            }
            db.set_block_ranges(block_ranges_ms);
        }
        db.set_postings_cache_eligibility(
            blocks_cfg.tsdb.head_postings_for_matchers_cache_force,
            blocks_cfg.tsdb.shared_postings_for_matchers_cache,
            blocks_cfg.tsdb.block_postings_for_matchers_cache_force,
            blocks_cfg.tsdb.shared_postings_for_matchers_cache,
        );
        let weak_db = Arc::downgrade(&db);
        *lifecycle_adapter.num_series_fn.write() = Some(Arc::new(move || {
            weak_db.upgrade().map_or(0, |d| d.head().num_series())
        }));

        // Compact ready blocks after WAL replay
        let _ = db.compact()?;
        pending_series_removals.store(0, Ordering::Release);
        postings_cache_invalidation_enabled.store(true, Ordering::Release);

        // Enable series limits now that WAL replay is complete
        *lifecycle_adapter.limiter_slot.write() = Some(Arc::clone(&limiter));

        let mut last_update_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        if db.head().num_series() > 0 {
            let newest = newest_data_time(&db);
            if newest != i64::MIN && newest < last_update_ms {
                last_update_ms = newest;
            }
        }

        let shipper: Option<Arc<dyn BlockShipper>> = if shipping_enabled {
            bucket.map(|b| {
                let ooo_ext = tenant_limits.out_of_order_blocks_external_label_enabled;
                let ooo_overrides = overrides.clone();
                let ooo_user = user_id.clone();
                Arc::new(
                    Shipper::new_for_user_with_config(
                        &user_id,
                        &udir,
                        b,
                        Arc::new(overrides.clone()),
                        SourceType::Receive,
                        ooo_ext,
                        shipper_metrics,
                    )
                    .with_out_of_order_external_label_provider(Arc::new(move || {
                        ooo_overrides
                            .for_tenant(&ooo_user)
                            .out_of_order_blocks_external_label_enabled
                    })),
                ) as Arc<dyn BlockShipper>
            })
        } else {
            None
        };

        if shipper.is_some() {
            *shipped_blocks.lock() = read_shipped_blocks(&udir)
                .map(|m| {
                    m.into_iter()
                        .map(|(k, mt)| (k, UNIX_EPOCH + Duration::from_millis(mt.0.max(0) as u64)))
                        .collect()
                })
                .unwrap_or_default();
        }
        {
            let blocks = db.blocks();
            let shipped = shipped_blocks.lock();
            let oldest = compute_oldest_unshipped_ms(&blocks, &shipped);
            oldest_unshipped_block_ts_ms.store(oldest, Ordering::Release);
        }

        let offset_catalogue = if blocks_cfg.tsdb.offset_catalogue.enabled {
            Some(OffsetCatalogue::new(
                &udir,
                &user_id,
                ingest_partition_id,
                shared_metrics.offset_catalogue.clone(),
            ))
        } else {
            None
        };

        let user_tsdb = Arc::new(Self {
            user_id,
            db,
            active_series,
            series_in_metric,
            limiter,
            overrides,
            ingested_api_samples: EwmaRate::new(0.2, rate_period),
            ingested_rule_samples: EwmaRate::new(0.2, rate_period),
            series_ref_history: SeriesRefHistory::new(LABEL_CHURN_RECORDS),
            shipper: RwLock::new(shipper),
            offset_catalogue,
            state_mtx: parking_lot::RwLock::new(()),
            compaction_mtx: tokio::sync::Mutex::new(()),
            state: AtomicU8::new(TsdbState::Active as u8),
            forced_compaction_max_time: AtomicI64::new(i64::MAX),
            in_flight_appends: AtomicI64::new(0),
            in_flight_appends_before_forced: AtomicI64::new(0),
            in_flight_notify: Notify::new(),
            in_flight_before_forced_notify: Notify::new(),
            last_update_ms: AtomicI64::new(last_update_ms),
            deletion_mark_found: AtomicBool::new(false),
            last_deletion_mark_check_sec: AtomicI64::new(0),
            last_early_compaction_ms: AtomicI64::new(0),
            pending_series_removals,
            last_series_removal_ms: AtomicI64::new(0),
            block_duration_ms,
            shipped_blocks,
            oldest_unshipped_block_ts_ms,
            owned_state,
            owned_token_ranges: Mutex::new(Vec::new()),
            requires_owned_series_update: Mutex::new(Some(
                RECOMPUTE_OWNED_SERIES_REASON_NEW_USER.to_string(),
            )),
            pending_non_owned_refs: Mutex::new(HashMap::new()),
            early_compaction_non_owned_enabled: AtomicBool::new(
                ingester_cfg.early_compaction_non_owned_series_enabled,
            ),
            lifecycle_adapter,
            current_metadata_limits: Arc::new(Mutex::new(initial_metadata_limits)),
            planner_provider: std::sync::OnceLock::new(),
            ingested_samples_counter: std::sync::OnceLock::new(),
            ingested_samples_fail_counter: std::sync::OnceLock::new(),
        });
        user_db_ready.store(true, Ordering::Release);

        Ok(user_tsdb)
    }

    pub fn state(&self) -> TsdbState {
        TsdbState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn change_state(&self, from: TsdbState, to: TsdbState) -> (bool, TsdbState) {
        let _guard = self.state_mtx.write();
        let cur = self.state();
        if cur != from {
            return (false, cur);
        }
        self.state.store(to as u8, Ordering::Release);
        (true, to)
    }

    pub fn change_state_to_forced_compaction(
        &self,
        from: TsdbState,
        forced_max_time: i64,
    ) -> (bool, TsdbState) {
        let _guard = self.state_mtx.write();
        let cur = self.state();
        if cur != from {
            return (false, cur);
        }
        self.forced_compaction_max_time
            .store(forced_max_time, Ordering::Release);
        self.state
            .store(TsdbState::ForceCompacting as u8, Ordering::Release);
        (true, TsdbState::ForceCompacting)
    }

    pub fn set_closing_state(&self) {
        let _guard = self.state_mtx.write();
        let cur = self.state();
        if cur != TsdbState::Closing && cur != TsdbState::Closed {
            self.state.store(TsdbState::Closing as u8, Ordering::Release);
        }
    }

    pub fn acquire_append_lock(&self, min_timestamp: i64) -> Result<AppendLockGuard<'_>, IngesterError> {
        let _guard = self.state_mtx.read();
        let cur = self.state();
        match cur {
            TsdbState::Active | TsdbState::ActiveShipping => {}
            TsdbState::ForceCompacting => {
                let max_t = self.forced_compaction_max_time.load(Ordering::Acquire);
                if max_t == i64::MAX {
                    return Err(IngesterError::tsdb_forced_compaction());
                }
                if min_timestamp <= max_t {
                    return Err(IngesterError::tsdb_early_compaction(
                        min_timestamp,
                        max_t.saturating_add(1),
                    ));
                }
            }
            TsdbState::Closing => return Err(IngesterError::tsdb_closing()),
            TsdbState::Closed => return Err(IngesterError::tsdb_not_active()),
        }

        self.in_flight_appends.fetch_add(1, Ordering::AcqRel);
        if cur != TsdbState::ForceCompacting {
            self.in_flight_appends_before_forced
                .fetch_add(1, Ordering::AcqRel);
        }

        Ok(AppendLockGuard {
            user_tsdb: self,
            acquired_state: cur,
        })
    }

    fn release_append_lock(&self, acquired_state: TsdbState) {
        if self.in_flight_appends.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.in_flight_notify.notify_waiters();
        }
        if acquired_state != TsdbState::ForceCompacting
            && self
                .in_flight_appends_before_forced
                .fetch_sub(1, Ordering::AcqRel)
                == 1
        {
            self.in_flight_before_forced_notify.notify_waiters();
        }
    }

    pub async fn wait_in_flight_appends(&self) {
        loop {
            let notified = self.in_flight_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.in_flight_appends.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
    }

    pub async fn wait_in_flight_before_forced_compaction(&self) {
        loop {
            let notified = self.in_flight_before_forced_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.in_flight_appends_before_forced.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
    }

    pub fn set_last_update(&self, t: SystemTime) {
        let ms = t
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.last_update_ms.store(ms, Ordering::Release);
    }

    pub fn last_update(&self) -> SystemTime {
        let ms = self.last_update_ms.load(Ordering::Acquire).max(0) as u64;
        UNIX_EPOCH + Duration::from_millis(ms)
    }

    pub fn is_idle(&self, now: SystemTime, idle: Duration) -> bool {
        self.last_update() + idle < now
    }

    pub fn set_last_early_compaction(&self, t: SystemTime) {
        let ms = t
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.last_early_compaction_ms.store(ms, Ordering::Release);
    }

    pub fn last_early_compaction_ms(&self) -> i64 {
        self.last_early_compaction_ms.load(Ordering::Acquire)
    }

    pub fn update_last_series_removal(&self) {
        if self.pending_series_removals.load(Ordering::Acquire) > 0 {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            self.last_series_removal_ms.store(now_ms, Ordering::Release);
            self.pending_series_removals.store(0, Ordering::Release);
        }
    }

    pub fn update_cached_shipped_blocks(&self) -> Result<(), String> {
        let m = read_shipped_blocks(self.db.dir()).map_err(|e| e.to_string())?;
        let new_map: HashMap<Ulid, SystemTime> = m
            .into_iter()
            .map(|(k, mt)| (k, UNIX_EPOCH + Duration::from_millis(mt.0.max(0) as u64)))
            .collect();
        let mut guard = self.shipped_blocks.lock();
        let blocks = self.db.blocks();
        *guard = new_map;
        let oldest = compute_oldest_unshipped_ms(&blocks, &guard);
        self.oldest_unshipped_block_ts_ms
            .store(oldest, Ordering::Release);
        Ok(())
    }

    pub fn refresh_oldest_unshipped_block_time_ms(&self) {
        let shipped = self.shipped_blocks.lock();
        let blocks = self.db.blocks();
        let oldest = compute_oldest_unshipped_ms(&blocks, &shipped);
        self.oldest_unshipped_block_ts_ms
            .store(oldest, Ordering::Release);
    }

    pub fn cached_shipped_blocks(&self) -> HashMap<Ulid, SystemTime> {
        self.shipped_blocks.lock().clone()
    }

    pub fn oldest_unshipped_block_time_ms(&self) -> u64 {
        if self.shipper.read().is_none() {
            return 0;
        }
        self.oldest_unshipped_block_ts_ms.load(Ordering::Acquire)
    }

    pub fn should_close_tsdb(&self, idle_timeout: Duration) -> TsdbCloseCheckResult {
        if self.deletion_mark_found.load(Ordering::Acquire) {
            return TsdbCloseCheckResult::TenantMarkedForDeletion;
        }
        if !self.is_idle(SystemTime::now(), idle_timeout) {
            return TsdbCloseCheckResult::NotIdle;
        }
        if self.db.head().num_series() > 0 {
            return TsdbCloseCheckResult::NotCompacted;
        }
        if self.oldest_unshipped_block_time_ms() > 0 {
            return TsdbCloseCheckResult::NotShipped;
        }
        TsdbCloseCheckResult::Idle
    }

    pub async fn check_deletion_mark(&self, bucket: Option<&BucketClient>) -> bool {
        if self.deletion_mark_found.load(Ordering::Acquire) {
            return true;
        }
        let Some(bkt) = bucket else {
            return false;
        };
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let last = self.last_deletion_mark_check_sec.load(Ordering::Acquire);
        if now_sec - last < 60 * 60 {
            return false;
        }
        self.last_deletion_mark_check_sec
            .store(now_sec, Ordering::Release);
        match tenant_deletion_mark_exists(bkt, &self.user_id).await {
            Ok(true) => {
                self.deletion_mark_found.store(true, Ordering::Release);
                info!(user = %self.user_id, "tenant deletion mark exists, not shipping blocks");
                true
            }
            Ok(false) => false,
            Err(e) => {
                warn!(user = %self.user_id, err = %e, "failed to check tenant deletion mark");
                false
            }
        }
    }

    /// Compact ready blocks via normal Prometheus TSDB compaction. Obsolete / retention-
    /// expired blocks are deleted by the TSDB on every block reload through the
    /// `blocks_to_delete` filter (Go `userTSDB.blocksToDelete`).
    pub fn compact(&self) -> Result<Vec<Ulid>, TsdbError> {
        let res = self.db.compact();
        self.refresh_oldest_unshipped_block_time_ms();
        res
    }

    /// Force-compact TSDB head up to `forced_compaction_max_time` (or `i64::MAX` for full flush).
    pub async fn compact_head(
        self: &Arc<Self>,
        forced_compaction_max_time: i64,
    ) -> Result<(), String> {
        let (ok, s) =
            self.change_state_to_forced_compaction(TsdbState::Active, forced_compaction_max_time);
        if !ok {
            return Err(format!(
                "TSDB head cannot be compacted because it is not in active state: {}",
                s.as_str()
            ));
        }
        let state_guard = TsdbStateGuard::new(Arc::clone(self), TsdbState::ForceCompacting);

        self.wait_in_flight_before_forced_compaction().await;

        let db = Arc::clone(&self.db);
        let block_duration_ms = self.block_duration_ms;
        let (res, state_guard) = tokio::task::spawn_blocking(move || {
            let inner = || -> Result<(), TsdbError> {
                let head = db.head();
                loop {
                    let h_min = head.min_time();
                    let h_max = head.max_time();
                    let Some((mint, maxt_incl, is_last)) = next_forced_head_compaction_range(
                        block_duration_ms,
                        h_min,
                        h_max,
                        forced_compaction_max_time,
                    ) else {
                        if h_min == i64::MAX
                            && h_max == i64::MAX
                            && forced_compaction_max_time == i64::MAX
                            && head.num_series() > 0
                        {
                            db.compact_head_range_inclusive(i64::MAX, i64::MAX)?;
                        }
                        break;
                    };
                    db.compact_head_range_inclusive(mint, maxt_incl)?;
                    if is_last {
                        break;
                    }
                }
                let _ = db.compact_ooo_head()?;
                Ok(())
            };
            (inner(), state_guard)
        })
        .await
        .unwrap_or_else(|e| {
            (
                Err(TsdbError::Io(std::io::Error::other(format!(
                    "compact_head join error: {e}"
                )))),
                TsdbStateGuard::new(Arc::clone(self), TsdbState::ForceCompacting),
            )
        });

        self.refresh_oldest_unshipped_block_time_ms();
        drop(state_guard);
        res.map_err(|e| e.to_string())
    }

    pub fn current_metadata_limits(&self) -> MetadataLimits {
        *self.current_metadata_limits.lock()
    }

    pub fn apply_tsdb_settings(&self) {
        let limits = self.overrides.for_tenant(&self.user_id);
        let ooo_ms = (limits.out_of_order_time_window.0 / 1_000_000).max(0);
        if let Err(e) = self.db.head().set_ooo_time_window(ooo_ms) {
            warn!(user = %self.user_id, err = %e, "failed to update OOO time window");
        }
        let max_exemplars = self.limiter.max_exemplars_per_user(&self.user_id);
        self.db.head().set_max_exemplars(max_exemplars);
        let float_enc = if limits.float_chunk_encoding == "xor2" {
            Encoding::Xor2
        } else {
            Encoding::Xor
        };
        self.db.head().set_float_chunk_encoding(float_enc);
        let metadata_limits = MetadataLimits {
            max_metrics_with_metadata_per_user: self.limiter.max_metadata_per_user(&self.user_id),
            max_metadata_per_metric: self.limiter.max_metadata_per_metric(&self.user_id),
        };
        self.db.head().set_metadata_limits(metadata_limits);
        *self.current_metadata_limits.lock() = metadata_limits;
    }

    pub fn owned_series_state(&self) -> OwnedSeriesState {
        *self.owned_state.lock()
    }

    pub fn trigger_recompute_owned_series(&self, reason: &str) {
        let mut guard = self.requires_owned_series_update.lock();
        if guard.is_none() {
            *guard = Some(reason.to_string());
        }
    }

    pub fn peek_reason_for_recompute_owned_series(&self) -> Option<String> {
        self.requires_owned_series_update.lock().clone()
    }

    pub fn set_owned_series_state(&self, state: OwnedSeriesState) {
        *self.owned_state.lock() = state;
    }

    pub fn add_owned_series_count(&self, delta: usize) {
        self.owned_state.lock().owned_series_count += delta;
    }

    pub fn get_series_count_and_min_local_limit(&self) -> (usize, usize) {
        if self
            .lifecycle_adapter
            .use_owned_series_for_limits
            .load(Ordering::Acquire)
        {
            let os = self.owned_state.lock();
            (os.owned_series_count, os.local_series_limit)
        } else {
            (self.db.head().num_series() as usize, 0)
        }
    }

    pub fn set_use_owned_series_for_limits(&self, v: bool) {
        self.lifecycle_adapter
            .use_owned_series_for_limits
            .store(v, Ordering::Release);
    }

    pub fn set_early_compaction_non_owned_enabled(&self, v: bool) {
        self.early_compaction_non_owned_enabled
            .store(v, Ordering::Release);
    }

    pub fn take_reason_for_recompute_owned_series(&self) -> Option<String> {
        self.requires_owned_series_update.lock().take()
    }

    pub fn update_token_ranges(&self, new_ranges: Vec<u32>) -> bool {
        let mut guard = self.owned_token_ranges.lock();
        if *guard == new_ranges {
            false
        } else {
            *guard = new_ranges;
            true
        }
    }

    pub fn set_owned_token_ranges(&self, new_ranges: Vec<u32>) {
        *self.owned_token_ranges.lock() = new_ranges;
    }

    pub fn pending_non_owned_refs_len(&self) -> usize {
        self.pending_non_owned_refs.lock().len()
    }

    pub fn pending_non_owned_refs_snapshot(&self) -> HashMap<u64, SystemTime> {
        self.pending_non_owned_refs.lock().clone()
    }

    pub fn backdate_pending_non_owned_refs(&self, ts: SystemTime) {
        let mut guard = self.pending_non_owned_refs.lock();
        for v in guard.values_mut() {
            *v = ts;
        }
    }

    pub fn add_pending_non_owned_refs(&self, refs: &HashSet<u64>) {
        let mut guard = self.pending_non_owned_refs.lock();
        if refs.is_empty() {
            guard.clear();
            return;
        }
        guard.retain(|r, _| refs.contains(r));
        let now = SystemTime::now();
        for &r in refs {
            guard.entry(r).or_insert(now);
        }
    }

    pub fn take_pending_non_owned_refs(&self, not_after: SystemTime) -> Vec<u64> {
        let mut guard = self.pending_non_owned_refs.lock();
        let mut out = Vec::new();
        guard.retain(|&r, &mut ts| {
            if ts <= not_after {
                out.push(r);
                false
            } else {
                true
            }
        });
        out
    }

    pub fn compute_owned_series(&self) -> usize {
        let ranges = self.owned_token_ranges.lock().clone();
        let all_non_owned = ranges.is_empty();
        if all_non_owned {
            self.active_series.clear();
        }
        let mut count = 0usize;
        let mut non_owned_refs = HashSet::new();
        let track_non_owned = self
            .early_compaction_non_owned_enabled
            .load(Ordering::Acquire);
        let user_id = &self.user_id;

        self.db.head().for_each_series(|ref_id, labels| {
            if all_non_owned {
                if track_non_owned {
                    non_owned_refs.insert(ref_id);
                }
                return;
            }
            let sh = secondary_tsdb_hash_for_user(user_id, labels);
            if token_ranges_include_key(&ranges, sh) {
                count += 1;
            } else {
                self.active_series
                    .delete_with_index(ref_id, Some(self.db.head()));
                if track_non_owned {
                    non_owned_refs.insert(ref_id);
                }
            }
        });

        if track_non_owned {
            self.add_pending_non_owned_refs(&non_owned_refs);
        }
        count
    }

    pub fn recompute_owned_series(&self, shard_size: usize, reason: &str) -> bool {
        self.recompute_owned_series_with_compute_fn(shard_size, reason, || {
            self.compute_owned_series()
        })
        .0
    }

    pub fn recompute_owned_series_with_compute_fn(
        &self,
        shard_size: usize,
        reason: &str,
        mut compute_fn: impl FnMut() -> usize,
    ) -> (bool, usize) {
        pub const MAX_ATTEMPTS: usize = 3;
        pub const MAX_DIFF: isize = 1000;

        let start = Instant::now();
        let mut attempts = 0usize;
        let mut success = false;
        let mut owned_before = 0usize;
        let mut shard_before = 0usize;
        let mut local_limit_before = 0usize;
        let mut owned_new = 0usize;
        let mut local_limit_new = 0usize;

        while !success && attempts < MAX_ATTEMPTS {
            attempts += 1;
            let prev = self.owned_series_state();
            owned_before = prev.owned_series_count;
            shard_before = prev.shard_size;
            local_limit_before = prev.local_series_limit;

            local_limit_new = self.limiter.max_series_per_user(&self.user_id, 0);
            owned_new = compute_fn();

            let mut os = self.owned_state.lock();
            let diff = (os.owned_series_count as isize) - (owned_before as isize);
            os.owned_series_count = owned_new;
            os.shard_size = shard_size;
            os.local_series_limit = local_limit_new;
            if (0..=MAX_DIFF).contains(&diff) {
                success = true;
            }
        }
        let duration_ms = start.elapsed().as_millis() as u64;
        if success {
            info!(
                user = %self.user_id,
                reason = %reason,
                owned_series_before = owned_before,
                owned_series_new = owned_new,
                shard_size_before = shard_before,
                shard_size_new = shard_size,
                local_limit_before = local_limit_before,
                local_limit_new = local_limit_new,
                duration_ms,
                attempts,
                success,
                "owned series: recomputed owned series for user"
            );
        } else {
            warn!(
                user = %self.user_id,
                reason = %reason,
                owned_series_before = owned_before,
                owned_series_new = owned_new,
                shard_size_before = shard_before,
                shard_size_new = shard_size,
                local_limit_before = local_limit_before,
                local_limit_new = local_limit_new,
                duration_ms,
                attempts,
                success,
                "owned series: recomputed owned series for user"
            );
        }
        (success, attempts)
    }

    pub fn update_owned_series_if_needed(
        &self,
        ring_strategy: &dyn OwnedSeriesRingStrategy,
        ring_changed: bool,
    ) -> bool {
        let shard_size = ring_strategy.shard_size_for_user(&self.user_id);
        let local_limit = self.limiter.max_series_per_user(&self.user_id, 0);
        let mut reason = self.take_reason_for_recompute_owned_series();

        if reason.is_none() {
            let os = self.owned_series_state();
            if shard_size != os.shard_size {
                reason = Some(RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED.to_string());
            } else if local_limit != os.local_series_limit {
                reason = Some(RECOMPUTE_OWNED_SERIES_REASON_LOCAL_LIMIT_CHANGED.to_string());
            }
        }

        if !ring_changed && reason.is_none() {
            return false;
        }

        let ranges = match ring_strategy.token_ranges_for_user(&self.user_id, shard_size) {
            Ok(r) => r,
            Err(e) => {
                let (k, v) = ring_strategy.owner_key_and_value();
                warn!(
                    user = %self.user_id,
                    owner_key = k,
                    owner_val = %v,
                    err = %e,
                    "failed to get token ranges for owned series"
                );
                self.trigger_recompute_owned_series(
                    reason
                        .as_deref()
                        .unwrap_or(RECOMPUTE_OWNED_SERIES_REASON_GET_TOKEN_RANGES_FAILED),
                );
                return false;
            }
        };

        if self.update_token_ranges(ranges) && reason.is_none() {
            reason = Some(RECOMPUTE_OWNED_SERIES_REASON_RING_CHANGED.to_string());
        }

        if let Some(r) = reason {
            if !self.recompute_owned_series(shard_size, &r) {
                self.trigger_recompute_owned_series(&r);
            }
            return true;
        }
        false
    }

    pub fn close(&self) -> Result<(), TsdbError> {
        self.lifecycle_adapter.limiter_slot.write().take();
        let res = self.db.close();
        self.oldest_unshipped_block_ts_ms.store(0, Ordering::Release);
        self.lifecycle_adapter.num_series_fn.write().take();
        self.db.head().clear_lifecycle_callback();
        res
    }
}

/// Ingester-level metrics every tenant TSDB updates, registered once on the ingester registry
/// like Go `newIngester`:
/// - `headPostingsForMatchersCacheFactory` / `blockPostingsForMatchersCacheFactory` metrics
///   (`cortex_ingester_tsdb_{head,block}_postings_for_matchers_cache_*`);
/// - `offsetCatalogueMetrics` (`cortex_ingester_offset_catalogue_*`).
#[derive(Clone)]
pub struct SharedTsdbMetrics {
    pub head_postings_cache: Option<PostingsForMatchersCacheMetrics>,
    pub block_postings_cache: Option<PostingsForMatchersCacheMetrics>,
    pub offset_catalogue: OffsetCatalogueMetrics,
    shared_head_postings_cache: Arc<OnceLock<Arc<PostingsForMatchersCache>>>,
    shared_block_postings_cache: Arc<OnceLock<Arc<PostingsForMatchersCache>>>,
}

impl SharedTsdbMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        Self {
            head_postings_cache: Some(PostingsForMatchersCacheMetrics::new(
                "cortex_ingester_tsdb_head_",
                reg,
            )),
            block_postings_cache: Some(PostingsForMatchersCacheMetrics::new(
                "cortex_ingester_tsdb_block_",
                reg,
            )),
            offset_catalogue: OffsetCatalogueMetrics::new(reg),
            shared_head_postings_cache: Arc::new(OnceLock::new()),
            shared_block_postings_cache: Arc::new(OnceLock::new()),
        }
    }

    pub fn new_with_blocks_cfg(reg: Option<&Registry>, blocks_cfg: &BlocksStorageConfig) -> Self {
        let this = Self::new(reg);
        if blocks_cfg.tsdb.shared_postings_for_matchers_cache {
            let _ = this.get_or_init_shared_head_cache(blocks_cfg);
            let _ = this.get_or_init_shared_block_cache(blocks_cfg);
        }
        this
    }

    pub fn get_or_init_shared_head_cache(
        &self,
        blocks_cfg: &BlocksStorageConfig,
    ) -> Arc<PostingsForMatchersCache> {
        Arc::clone(self.shared_head_postings_cache.get_or_init(|| {
            let cache = Arc::new(PostingsForMatchersCache::new(
                head_postings_for_matchers_cache_config(blocks_cfg, self),
            ));
            cache.set_eligibility(
                blocks_cfg.tsdb.head_postings_for_matchers_cache_force,
                true,
            );
            cache
        }))
    }

    pub fn get_or_init_shared_block_cache(
        &self,
        blocks_cfg: &BlocksStorageConfig,
    ) -> Arc<PostingsForMatchersCache> {
        Arc::clone(self.shared_block_postings_cache.get_or_init(|| {
            let cache = Arc::new(PostingsForMatchersCache::new(
                block_postings_for_matchers_cache_config(blocks_cfg, self),
            ));
            cache.set_eligibility(
                blocks_cfg.tsdb.block_postings_for_matchers_cache_force,
                true,
            );
            cache
        }))
    }
}

impl Default for SharedTsdbMetrics {
    /// Unregistered metrics.
    fn default() -> Self {
        Self::new(None)
    }
}

pub fn newest_data_time(db: &Tsdb) -> i64 {
    let head = db.head();
    let mut t = head.max_time().max(head.max_ooo_time());
    for b in db.blocks() {
        t = t.max(b.max_time());
    }
    t
}

pub fn next_forced_head_compaction_range(
    block_duration: i64,
    head_min_time: i64,
    head_max_time: i64,
    forced_max_time: i64,
) -> Option<(i64, i64, bool)> {
    if head_min_time == i64::MAX || head_max_time == i64::MIN {
        return None;
    }
    let min_time = head_min_time;
    let max_time = head_max_time.min(forced_max_time);
    if max_time < min_time {
        return None;
    }
    if block_duration <= 0 {
        return Some((min_time, max_time, true));
    }
    let min_bucket = min_time / block_duration;
    let max_bucket = max_time / block_duration;
    if min_bucket.saturating_mul(block_duration) != max_bucket.saturating_mul(block_duration) {
        let split_max = (min_bucket + 1)
            .saturating_mul(block_duration)
            .saturating_sub(1);
        Some((min_time, split_max, false))
    } else {
        Some((min_time, max_time, true))
    }
}

struct MetadataEntry {
    raw_type: i32,
    help: String,
    unit: String,
    last_seen_nanos: AtomicI64,
}

#[derive(Default)]
struct FastMetadataInner {
    metrics: HashMap<String, Vec<MetadataEntry>>,
}

#[inline]
fn system_time_to_nanos(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}



/// Per-tenant metric metadata wrapper tracking metrics & limits (`user_metrics_metadata.go`).
pub struct TenantMetadataStore {
    user_id: String,
    inner: RwLock<FastMetadataInner>,
    limiter: Arc<Limiter>,
    mem_metadata: prometheus::Gauge,
    mem_created: prometheus::CounterVec,
    mem_removed: prometheus::CounterVec,
    discarded_per_user: prometheus::CounterVec,
    discarded_per_metric: prometheus::CounterVec,
}

impl TenantMetadataStore {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        user_id: String,
        limiter: Arc<Limiter>,
        mem_metadata: prometheus::Gauge,
        mem_created: prometheus::CounterVec,
        mem_removed: prometheus::CounterVec,
        discarded_per_user: prometheus::CounterVec,
        discarded_per_metric: prometheus::CounterVec,
    ) -> Self {
        Self {
            user_id,
            inner: RwLock::new(FastMetadataInner::default()),
            limiter,
            mem_metadata,
            mem_created,
            mem_removed,
            discarded_per_user,
            discarded_per_metric,
        }
    }

    pub fn add(&self, m: &cortexpb::MetricMetadata) -> Result<(), IngesterError> {
        self.add_parts_at(
            m.r#type,
            &m.metric_family_name,
            &m.help,
            &m.unit,
            system_time_to_nanos(SystemTime::now()),
        )
    }

    pub fn add_parts_at(
        &self,
        raw_type: i32,
        metric_family_name: &str,
        help: &str,
        unit: &str,
        now_nanos: i64,
    ) -> Result<(), IngesterError> {

        let max_per_user = self.limiter.max_metadata_per_user(&self.user_id);
        let max_per_metric = self.limiter.max_metadata_per_metric(&self.user_id);

        // Lock-free-for-readers fast path: 97%+ of metadata updates refresh an existing entry.
        {
            let guard = self.inner.read();
            if let Some(entries) = guard.metrics.get(metric_family_name) {
                if max_per_metric == 0 || entries.len() < max_per_metric {
                    for e in entries {
                        if e.raw_type == raw_type && e.help == help && e.unit == unit {
                            e.last_seen_nanos.store(now_nanos, Ordering::Relaxed);
                            return Ok(());
                        }
                    }
                }
            }
        }

        let mut guard = self.inner.write();
        if let Some(entries) = guard.metrics.get(metric_family_name) {
            if max_per_metric > 0 && entries.len() >= max_per_metric {
                self.discarded_per_metric
                    .with_label_values(&[&self.user_id])
                    .inc();
                let global_limit = self
                    .limiter
                    .max_global_metadata_per_metric(&self.user_id)
                    .max(0) as usize;
                return Err(IngesterError::per_metric_metadata_limit_reached(
                    global_limit,
                    metric_family_name.to_string(),
                ));
            }
            for e in entries {
                if e.raw_type == raw_type && e.help == help && e.unit == unit {
                    e.last_seen_nanos.store(now_nanos, Ordering::Relaxed);
                    return Ok(());
                }
            }
        } else if max_per_user > 0 && guard.metrics.len() >= max_per_user {
            self.discarded_per_user
                .with_label_values(&[&self.user_id])
                .inc();
            let global_limit = self
                .limiter
                .max_global_metrics_with_metadata_per_user(&self.user_id)
                .max(0) as usize;
            return Err(IngesterError::per_user_metadata_limit_reached(global_limit));
        }

        let entry = MetadataEntry {
            raw_type,
            help: help.to_owned(),
            unit: unit.to_owned(),
            last_seen_nanos: AtomicI64::new(now_nanos),
        };
        if let Some(entries) = guard.metrics.get_mut(metric_family_name) {
            entries.push(entry);
        } else {
            guard
                .metrics
                .insert(metric_family_name.to_owned(), vec![entry]);
        }
        drop(guard);

        self.mem_metadata.inc();
        self.mem_created.with_label_values(&[&self.user_id]).inc();
        Ok(())
    }

    pub fn purge(&self, deadline: Option<SystemTime>) -> usize {
        let dl_nanos = deadline.map(system_time_to_nanos);
        let mut guard = self.inner.write();
        let mut deleted = 0usize;
        guard.metrics.retain(|_, entries| {
            let before = entries.len();
            match dl_nanos {
                None => entries.clear(),
                Some(dl) => entries.retain(|e| e.last_seen_nanos.load(Ordering::Relaxed) >= dl),
            }
            deleted += before - entries.len();
            !entries.is_empty()
        });
        drop(guard);

        if deleted > 0 {
            self.mem_metadata.sub(deleted as f64);
        }
        self.mem_removed
            .with_label_values(&[&self.user_id])
            .inc_by(deleted as f64);
        deleted
    }

    pub fn query(
        &self,
        metric_names: &[String],
        single_metric: &str,
        limit: i32,
        limit_per_metric: i32,
    ) -> Vec<cortexpb::MetricMetadata> {
        if limit == 0 {
            return Vec::new();
        }
        let guard = self.inner.read();
        let mut out = Vec::new();

        let push_entries =
            |out: &mut Vec<cortexpb::MetricMetadata>, name: &str, entries: &[MetadataEntry]| {
                for (per_metric, e) in entries.iter().enumerate() {
                    if limit_per_metric > 0 && (per_metric as i32) >= limit_per_metric {
                        break;
                    }
                    out.push(cortexpb::MetricMetadata {
                        r#type: e.raw_type,
                        metric_family_name: name.to_owned(),
                        help: e.help.clone(),
                        unit: e.unit.clone(),
                    });
                }
            };

        if !metric_names.is_empty() {
            let mut num_metrics = 0i32;
            for name in metric_names {
                if limit > 0 && num_metrics >= limit {
                    break;
                }
                if let Some(entries) = guard.metrics.get(name) {
                    push_entries(&mut out, name, entries);
                    num_metrics += 1;
                }
            }
            return out;
        }

        if !single_metric.is_empty() {
            if let Some(entries) = guard.metrics.get(single_metric) {
                push_entries(&mut out, single_metric, entries);
            }
            return out;
        }

        for (num_metrics, (name, entries)) in guard.metrics.iter().enumerate() {
            if limit > 0 && (num_metrics as i32) >= limit {
                break;
            }
            push_entries(&mut out, name, entries);
        }
        out
    }
}

/// Builds the Head `PostingsForMatchersCacheConfig` matching Go's `headPostingsForMatchersCacheFactory`.
///
/// Go only allocates `metricVersions` (`head_postings_for_matchers_cache_versions`, default 2M * 8B = 16 MiB)
/// when both `shared_postings_for_matchers_cache` and `head_postings_for_matchers_cache_invalidation` are true.
/// Passing 0 otherwise ensures `Head::open` allocates only the minimum 64-slot table (512 B per tenant).
pub(crate) fn head_postings_for_matchers_cache_config(
    blocks_cfg: &BlocksStorageConfig,
    shared_metrics: &SharedTsdbMetrics,
) -> PostingsForMatchersCacheConfig {
    let invalidation = blocks_cfg.tsdb.head_postings_for_matchers_cache_invalidation;
    let shared_invalidation =
        blocks_cfg.tsdb.shared_postings_for_matchers_cache && invalidation;
    PostingsForMatchersCacheConfig {
        ttl: Duration::from_nanos(
            blocks_cfg
                .tsdb
                .head_postings_for_matchers_cache_ttl
                .as_nanos()
                .max(0) as u64,
        ),
        max_items: usize::MAX,
        max_bytes: blocks_cfg
            .tsdb
            .head_postings_for_matchers_cache_max_bytes
            .max(0) as usize,
        // Go postings_for_matchers_cache.go:146-151: invalidation is only enabled when
        // both Shared and Invalidation are true.
        enable_invalidation: shared_invalidation,
        num_metric_versions: if shared_invalidation {
            blocks_cfg
                .tsdb
                .head_postings_for_matchers_cache_versions
                .max(0) as usize
        } else {
            0
        },
        metrics: shared_metrics.head_postings_cache.clone(),
        report_invalidations: false,
    }
}

pub(crate) fn block_postings_for_matchers_cache_config(
    blocks_cfg: &BlocksStorageConfig,
    shared_metrics: &SharedTsdbMetrics,
) -> PostingsForMatchersCacheConfig {
    PostingsForMatchersCacheConfig {
        ttl: Duration::from_nanos(
            blocks_cfg
                .tsdb
                .block_postings_for_matchers_cache_ttl
                .as_nanos()
                .max(0) as u64,
        ),
        max_items: usize::MAX,
        max_bytes: blocks_cfg
            .tsdb
            .block_postings_for_matchers_cache_max_bytes
            .max(0) as usize,
        enable_invalidation: false,
        num_metric_versions: 0,
        metrics: shared_metrics.block_postings_cache.clone(),
        report_invalidations: false,
    }
}

/// Dynamic gauge collector for `prometheus_tsdb_head_chunks_storage_size_bytes`
/// (Go `prometheus/tsdb/head.go:850` -> `h.chunkDiskMapper.Size()`).
struct HeadChunksStorageSizeCollector {
    desc: prometheus::core::Desc,
    chunks_head_dir: PathBuf,
}

impl HeadChunksStorageSizeCollector {
    fn new(chunks_head_dir: PathBuf) -> Option<Self> {
        let desc = prometheus::core::Desc::new(
            "prometheus_tsdb_head_chunks_storage_size_bytes".to_string(),
            "Size of the chunks_head directory.".to_string(),
            Vec::new(),
            HashMap::new(),
        )
        .ok()?;
        Some(Self {
            desc,
            chunks_head_dir,
        })
    }
}

fn dir_size_bytes(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total = total.saturating_add(meta.len());
            }
        }
    }
    total
}

impl prometheus::core::Collector for HeadChunksStorageSizeCollector {
    fn desc(&self) -> Vec<&prometheus::core::Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        let size = dir_size_bytes(&self.chunks_head_dir);
        let mut g = prometheus::proto::Gauge::default();
        g.set_value(size as f64);
        let mut m = prometheus::proto::Metric::default();
        m.set_gauge(g);
        let mut mf = prometheus::proto::MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(prometheus::proto::MetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

pub fn debug_log_push_head_error(user_id: &str, err: &HeadError) {
    debug!(user = %user_id, err = %err, "head append error");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limiter::LimiterRingStrategy;

    struct OneInstanceStrategy;
    impl LimiterRingStrategy for OneInstanceStrategy {
        fn convert_global_to_local_limit(&self, _user_id: &str, global_limit: usize) -> usize {
            global_limit
        }
        fn get_shard_size(&self, _user_id: &str) -> usize {
            0
        }
    }

    #[test]
    fn metadata_store_checks_per_metric_limit_before_refresh() {
        let limits = mimir_config::LimitsConfig {
            max_global_metadata_per_metric: 2,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let limiter = Arc::new(Limiter::new(
            overrides.clone(),
            Arc::new(OneInstanceStrategy),
        ));
        let reg = Registry::new();
        let mem = prometheus::Gauge::new("t_mem", "h").unwrap();
        let created = prometheus::CounterVec::new(prometheus::Opts::new("t_cr", "h"), &["user"]).unwrap();
        let removed = prometheus::CounterVec::new(prometheus::Opts::new("t_rm", "h"), &["user"]).unwrap();
        let d_user = prometheus::CounterVec::new(prometheus::Opts::new("t_du", "h"), &["user"]).unwrap();
        let d_metric = prometheus::CounterVec::new(prometheus::Opts::new("t_dm", "h"), &["user"]).unwrap();
        let _ = reg.register(Box::new(mem.clone()));

        let store = TenantMetadataStore::new(
            "t1".to_string(),
            limiter,
            mem,
            created,
            removed,
            d_user,
            d_metric,
        );
        store.add_parts_at(1, "up", "h1", "", 100).unwrap();
        store.add_parts_at(1, "up", "h2", "", 100).unwrap();

        // Dynamically lower limit to 1 -> refreshing existing ("up", "h1") must now fail!
        let lowered = mimir_config::LimitsConfig {
            max_global_metadata_per_metric: 1,
            ..Default::default()
        };
        overrides.set_defaults(lowered);

        let err = store.add_parts_at(1, "up", "h1", "", 200).unwrap_err();
        assert!(matches!(err, IngesterError::PerMetricMetadataLimitReached { .. }));
    }

    #[test]
    fn user_tsdb_wires_chunk_end_time_variance_and_dynamic_metadata_limits() {
        let dir = tempfile::tempdir().unwrap();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
        blocks_cfg.tsdb.ship_interval = mimir_config::GoDuration::ZERO;
        blocks_cfg.tsdb.head_chunks_end_time_variance = 0.25;

        let limits = mimir_config::LimitsConfig {
            max_global_metadata_per_user: 10,
            max_global_metadata_per_metric: 3,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let limiter = Arc::new(Limiter::new(
            overrides.clone(),
            Arc::new(OneInstanceStrategy),
        ));
        let reg = Registry::new();
        let shared = SharedTsdbMetrics::new(Some(&reg));
        let shipper_metrics = ShipperMetrics::new(Some(&reg));
        let rejected = CounterVec::new(prometheus::Opts::new("t_rej", "h"), &["reason"]).unwrap();
        let ingester_cfg = mimir_config::IngesterConfig::default();
        let db = UserTsdb::open(
            "u-w13".to_string(),
            &ingester_cfg,
            &blocks_cfg,
            overrides.clone(),
            limiter,
            None,
            shipper_metrics,
            &shared,
            None,
            1,
            Arc::new(AtomicI64::new(0)),
            Arc::new(|| 0),
            rejected,
            HashSet::new(),
            0,
            0,
            None,
        )
        .unwrap();

        assert_eq!(db.db.head().chunk_end_time_variance(), 0.25);
        assert_eq!(
            db.db.head().metadata_limits(),
            MetadataLimits {
                max_metrics_with_metadata_per_user: 10,
                max_metadata_per_metric: 3,
            }
        );

        overrides.set_defaults(mimir_config::LimitsConfig {
            max_global_metadata_per_user: 42,
            max_global_metadata_per_metric: 7,
            ..Default::default()
        });
        db.apply_tsdb_settings();

        let expected = MetadataLimits {
            max_metrics_with_metadata_per_user: 42,
            max_metadata_per_metric: 7,
        };
        assert_eq!(db.current_metadata_limits(), expected);
        assert_eq!(db.db.head().metadata_limits(), expected);
    }

    #[test]
    fn head_postings_cache_config_respects_invalidation_and_shared_flags() {
        let shared = SharedTsdbMetrics::default();
        let default_cfg = mimir_config::BlocksStorageConfig::default();
        let pc_default = head_postings_for_matchers_cache_config(&default_cfg, &shared);
        assert!(!pc_default.enable_invalidation);
        assert_eq!(pc_default.num_metric_versions, 0);
        assert!(!pc_default.report_invalidations);

        let mut inv_only_cfg = mimir_config::BlocksStorageConfig::default();
        inv_only_cfg.tsdb.head_postings_for_matchers_cache_invalidation = true;
        let pc_inv = head_postings_for_matchers_cache_config(&inv_only_cfg, &shared);
        assert!(!pc_inv.enable_invalidation);
        assert_eq!(pc_inv.num_metric_versions, 0);
        assert!(!pc_inv.report_invalidations);

        let mut shared_inv_cfg = mimir_config::BlocksStorageConfig::default();
        shared_inv_cfg.tsdb.shared_postings_for_matchers_cache = true;
        shared_inv_cfg.tsdb.head_postings_for_matchers_cache_invalidation = true;
        shared_inv_cfg.tsdb.head_postings_for_matchers_cache_versions = 4096;
        let pc_shared = head_postings_for_matchers_cache_config(&shared_inv_cfg, &shared);
        assert!(pc_shared.enable_invalidation);
        assert_eq!(pc_shared.num_metric_versions, 4096);
        assert!(!pc_shared.report_invalidations);
    }

    #[test]
    fn tenant_tsdb_registry_exposes_head_chunks_storage_size_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
        blocks_cfg.tsdb.ship_interval = mimir_config::GoDuration::ZERO;

        let main_reg = Registry::new();
        let tenant_reg = Arc::new(Registry::new());
        let shared = SharedTsdbMetrics::new(Some(&main_reg));
        let shipper_metrics = ShipperMetrics::new(Some(&main_reg));
        let overrides = Overrides::new(mimir_config::LimitsConfig::default());
        let limiter = Arc::new(Limiter::new(
            overrides.clone(),
            Arc::new(OneInstanceStrategy),
        ));
        let rejected = CounterVec::new(prometheus::Opts::new("t_rej2", "h"), &["reason"]).unwrap();

        let _db = UserTsdb::open(
            "t-chunks-size".to_string(),
            &mimir_config::IngesterConfig::default(),
            &blocks_cfg,
            overrides,
            limiter,
            None,
            shipper_metrics,
            &shared,
            Some(Arc::clone(&tenant_reg)),
            1,
            Arc::new(AtomicI64::new(0)),
            Arc::new(|| 0),
            rejected,
            HashSet::new(),
            0,
            0,
            None,
        )
        .unwrap();

        let chunks_head = dir.path().join("t-chunks-size").join("chunks_head");
        std::fs::create_dir_all(&chunks_head).unwrap();
        std::fs::write(chunks_head.join("000001"), vec![0u8; 4096]).unwrap();

        let fam = tenant_reg
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "prometheus_tsdb_head_chunks_storage_size_bytes")
            .expect("prometheus_tsdb_head_chunks_storage_size_bytes must be in tenant registry");
        assert!(fam.get_metric()[0].get_gauge().get_value() >= 4096.0);

        assert!(
            main_reg
                .gather()
                .into_iter()
                .all(|mf| mf.get_name() != "prometheus_tsdb_head_chunks_storage_size_bytes"),
            "prometheus_tsdb_head_chunks_storage_size_bytes must not be registered on main /metrics"
        );
    }

    #[tokio::test]
    async fn user_tsdb_compact_head_preserves_i64_max_samples() {
        use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};

        // Case 1: sample at (i64::MAX - 100) and i64::MAX in the same block range.
        let dir1 = tempfile::tempdir().unwrap();
        let mut blocks_cfg1 = mimir_config::BlocksStorageConfig::default();
        blocks_cfg1.tsdb.dir = dir1.path().to_string_lossy().into_owned();
        blocks_cfg1.tsdb.ship_interval = mimir_config::GoDuration::ZERO;
        let main_reg1 = Registry::new();
        let shared1 = SharedTsdbMetrics::new(Some(&main_reg1));
        let shipper1 = ShipperMetrics::new(Some(&main_reg1));
        let overrides1 = Overrides::new(mimir_config::LimitsConfig::default());
        let limiter1 = Arc::new(Limiter::new(overrides1.clone(), Arc::new(OneInstanceStrategy)));
        let rejected1 = CounterVec::new(prometheus::Opts::new("t_rej_max1", "h"), &["reason"]).unwrap();

        let db1 = UserTsdb::open(
            "t-max-1".to_string(),
            &mimir_config::IngesterConfig::default(),
            &blocks_cfg1,
            overrides1,
            limiter1,
            None,
            shipper1,
            &shared1,
            None,
            1,
            Arc::new(AtomicI64::new(0)),
            Arc::new(|| 0),
            rejected1,
            HashSet::new(),
            0,
            0,
            None,
        )
        .unwrap();
        {
            let mut app = db1.db.appender();
            app.append_float(None, &[("__name__", "m_max")], i64::MAX - 100, 1.0).unwrap();
            app.append_float(None, &[("__name__", "m_max")], i64::MAX, 2.0).unwrap();
            app.commit().unwrap();
        }
        db1.compact_head(i64::MAX).await.unwrap();
        {
            let blocks = db1.db.blocks();
            assert_eq!(blocks.len(), 1);
            let idx = blocks[0].index();
            let chk_r = blocks[0].chunks();
            let ids = idx.postings("__name__", &["m_max"]).unwrap();
            let (_, chks) = idx.series(ids[0]).unwrap();
            let samples: Vec<(i64, SampleValue)> = chks
                .iter()
                .flat_map(|c| decode_chunk_samples(&chk_r.chunk(c.ref_id).unwrap()).unwrap())
                .map(|s| (s.t, s.value))
                .collect();
            assert_eq!(
                samples,
                vec![
                    (i64::MAX - 100, SampleValue::Float(1.0)),
                    (i64::MAX, SampleValue::Float(2.0)),
                ]
            );
        }

        // Case 2: single sample strictly at t == i64::MAX (mint == maxt_incl == i64::MAX).
        let dir2 = tempfile::tempdir().unwrap();
        let mut blocks_cfg2 = mimir_config::BlocksStorageConfig::default();
        blocks_cfg2.tsdb.dir = dir2.path().to_string_lossy().into_owned();
        blocks_cfg2.tsdb.ship_interval = mimir_config::GoDuration::ZERO;
        let main_reg2 = Registry::new();
        let shared2 = SharedTsdbMetrics::new(Some(&main_reg2));
        let shipper2 = ShipperMetrics::new(Some(&main_reg2));
        let overrides2 = Overrides::new(mimir_config::LimitsConfig::default());
        let limiter2 = Arc::new(Limiter::new(overrides2.clone(), Arc::new(OneInstanceStrategy)));
        let rejected2 = CounterVec::new(prometheus::Opts::new("t_rej_max2", "h"), &["reason"]).unwrap();

        let db2 = UserTsdb::open(
            "t-max-2".to_string(),
            &mimir_config::IngesterConfig::default(),
            &blocks_cfg2,
            overrides2,
            limiter2,
            None,
            shipper2,
            &shared2,
            None,
            1,
            Arc::new(AtomicI64::new(0)),
            Arc::new(|| 0),
            rejected2,
            HashSet::new(),
            0,
            0,
            None,
        )
        .unwrap();
        {
            let mut app = db2.db.appender();
            app.append_float(None, &[("__name__", "m_only_max")], i64::MAX, 9.5).unwrap();
            app.commit().unwrap();
        }
        db2.compact_head(i64::MAX).await.unwrap();
        {
            let blocks = db2.db.blocks();
            assert_eq!(blocks.len(), 1);
            assert_eq!(blocks[0].min_time(), i64::MAX);
            assert_eq!(blocks[0].max_time(), i64::MAX);
            let idx = blocks[0].index();
            let chk_r = blocks[0].chunks();
            let ids = idx.postings("__name__", &["m_only_max"]).unwrap();
            let (_, chks) = idx.series(ids[0]).unwrap();
            let samples = decode_chunk_samples(&chk_r.chunk(chks[0].ref_id).unwrap()).unwrap();
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].t, i64::MAX);
            assert_eq!(samples[0].value, SampleValue::Float(9.5));
        }
    }

    #[tokio::test]
    async fn f2_shared_postings_cache_uses_one_instance_across_tenants_and_blocks_without_collisions() {
        let dir = tempfile::tempdir().unwrap();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
        blocks_cfg.tsdb.ship_interval = mimir_config::GoDuration::ZERO;
        blocks_cfg.tsdb.shared_postings_for_matchers_cache = true;
        blocks_cfg.tsdb.head_postings_for_matchers_cache_invalidation = true;
        blocks_cfg.tsdb.head_postings_for_matchers_cache_versions = 1024;
        blocks_cfg.tsdb.head_postings_for_matchers_cache_force = true;
        blocks_cfg.tsdb.block_postings_for_matchers_cache_force = true;

        let reg = Registry::new();
        let shared = SharedTsdbMetrics::new_with_blocks_cfg(Some(&reg), &blocks_cfg);

        let open_tenant = |uid: &str| {
            let overrides = Overrides::new(mimir_config::LimitsConfig::default());
            let limiter = Arc::new(Limiter::new(
                overrides.clone(),
                Arc::new(crate::limiter::NoopRingLimiterStrategy),
            ));
            let shipper = ShipperMetrics::new(None);
            let rej = CounterVec::new(prometheus::Opts::new(format!("rej_{uid}"), "h"), &["reason"]).unwrap();
            UserTsdb::open(
                uid.to_string(),
                &mimir_config::IngesterConfig::default(),
                &blocks_cfg,
                overrides,
                limiter,
                None,
                shipper,
                &shared,
                None,
                1,
                Arc::new(AtomicI64::new(0)),
                Arc::new(|| 0),
                rej,
                HashSet::new(),
                0,
                0,
                None,
            )
            .unwrap()
        };

        let db_a = open_tenant("tenant_a");
        let db_b = open_tenant("tenant_b");

        // Both tenant Heads must point to the exact same shared PostingsForMatchersCache instance.
        assert!(std::ptr::eq(db_a.db.head().postings_cache(), db_b.db.head().postings_cache()));

        // Append 1 series for "up" in tenant-a and 2 series for "up" in tenant-b.
        {
            let mut app = db_a.db.appender();
            app.append_float(None, &[("__name__", "up"), ("i", "1")], 1_000, 1.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = db_b.db.appender();
            app.append_float(None, &[("__name__", "up"), ("i", "1")], 1_000, 1.0).unwrap();
            app.append_float(None, &[("__name__", "up"), ("i", "2")], 2_000, 2.0).unwrap();
            app.commit().unwrap();
        }

        let m_up = [mimir_tsdb::matcher::LabelMatcher::new(
            mimir_tsdb::matcher::MatchType::Equal,
            "__name__",
            "up",
        )
        .unwrap()];
        assert_eq!(db_a.db.head().postings_for_matchers_arc(&m_up).unwrap().len(), 1);
        assert_eq!(db_b.db.head().postings_for_matchers_arc(&m_up).unwrap().len(), 2);

        // Compact both into Blocks and verify both Blocks share the ONE shared block postings cache
        // and still never collide on identical matchers.
        db_a.compact_head(1_000).await.unwrap();
        db_b.compact_head(2_000).await.unwrap();
        let ba = &db_a.db.blocks()[0];
        let bb = &db_b.db.blocks()[0];
        assert!(std::ptr::eq(ba.postings_cache(), bb.postings_cache()));
        assert!(!std::ptr::eq(ba.postings_cache(), db_a.db.head().postings_cache()));
        assert_eq!(ba.postings_for_matchers(&m_up).unwrap().len(), 1);
        assert_eq!(bb.postings_for_matchers(&m_up).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn tsdb_state_guard_restores_active_on_drop_and_compact_head_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let mk_db = |uid: &str| {
            let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
            blocks_cfg.tsdb.dir = dir.path().to_string_lossy().into_owned();
            blocks_cfg.tsdb.ship_interval = mimir_config::GoDuration::ZERO;
            let main_reg = Registry::new();
            let shared = SharedTsdbMetrics::new(Some(&main_reg));
            let shipper = ShipperMetrics::new(Some(&main_reg));
            let overrides = Overrides::new(mimir_config::LimitsConfig::default());
            let limiter = Arc::new(Limiter::new(overrides.clone(), Arc::new(OneInstanceStrategy)));
            let rejected = CounterVec::new(prometheus::Opts::new("t_rej_guard", "h"), &["reason"]).unwrap();
            UserTsdb::open(
                uid.to_string(),
                &mimir_config::IngesterConfig::default(),
                &blocks_cfg,
                overrides,
                limiter,
                None,
                shipper,
                &shared,
                None,
                1,
                Arc::new(AtomicI64::new(0)),
                Arc::new(|| 0),
                rejected,
                HashSet::new(),
                0,
                0,
                None,
            )
            .unwrap()
        };
        let db = mk_db("guard-u");

        assert!(db.change_state(TsdbState::Active, TsdbState::ActiveShipping).0);
        {
            let _g = TsdbStateGuard::new(Arc::clone(&db), TsdbState::ActiveShipping);
            assert_eq!(db.state(), TsdbState::ActiveShipping);
        }
        assert_eq!(db.state(), TsdbState::Active);

        // Disarm keeps target state (used on Closing -> Closed).
        assert!(db.change_state(TsdbState::Active, TsdbState::Closing).0);
        {
            let mut g = TsdbStateGuard::new(Arc::clone(&db), TsdbState::Closing);
            assert!(db.change_state(TsdbState::Closing, TsdbState::Closed).0);
            g.disarm();
        }
        assert_eq!(db.state(), TsdbState::Closed);

        // Dropping compact_head future mid-await (while an append lock is held) restores Active.
        let db2 = mk_db("guard-u2");
        let append_guard = db2.acquire_append_lock(1_000).unwrap();
        {
            let fut = db2.compact_head(i64::MAX);
            tokio::pin!(fut);
            let timed = tokio::time::timeout(std::time::Duration::from_millis(15), &mut fut).await;
            assert!(timed.is_err());
            assert_eq!(db2.state(), TsdbState::ForceCompacting);
        }
        drop(append_guard);
        assert_eq!(
            db2.state(),
            TsdbState::Active,
            "dropped compact_head future must restore TsdbState::Active via TsdbStateGuard"
        );
    }
}
