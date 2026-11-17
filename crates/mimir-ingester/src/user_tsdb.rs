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
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_config::{BlocksStorageConfig, IngesterConfig, Overrides};
use mimir_proto::cortexpb;
use mimir_storage::bucket::{tenant_deletion_mark_exists, BucketClient};
use mimir_storage::ingest::{OffsetCatalogue, OffsetCatalogueMetrics};
use mimir_storage::shipper::{read_shipped_blocks, Shipper, ShipperError, ShipperMetrics, SourceType};
use mimir_tsdb::chunkenc::Encoding;
use mimir_tsdb::db::{BlocksToDeleteFn, Tsdb, TsdbError, TsdbOptions};
use mimir_tsdb::head::{HeadError, HeadOptions, SeriesLifecycleCallback};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::metadata::MetadataLimits;
use mimir_tsdb::postings_cache::{PostingsForMatchersCacheConfig, PostingsForMatchersCacheMetrics};
use mimir_tsdb::record::MetricType;
use mimir_tsdb::wlog::CompressionType;
use parking_lot::{Mutex, RwLock};
use prometheus::{CounterVec, Registry};
use tokio::sync::Notify;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::active_series::{
    merged_custom_trackers, parse_custom_tracker_matchers, ActiveSeries, CostAttributionTracker,
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

    owned_state: Arc<Mutex<OwnedSeriesState>>,
    owned_token_ranges: Mutex<Vec<u32>>,
    requires_owned_series_update: Mutex<Option<String>>,
    pending_non_owned_refs: Mutex<HashMap<u64, SystemTime>>,
    early_compaction_non_owned_enabled: AtomicBool,

    lifecycle_adapter: Arc<LifecycleCallbackAdapter>,

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
        let custom_matchers =
            parse_custom_tracker_matchers(&merged_custom_trackers(
                &tenant_limits.active_series_custom_trackers,
                &tenant_limits.active_series_additional_custom_trackers,
            ));

        let active_series = Arc::new(ActiveSeries::with_matchers(
            Matchers::from_custom_trackers(&custom_matchers),
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
            postings_cache: PostingsForMatchersCacheConfig {
                ttl: Duration::from_nanos(
                    blocks_cfg.tsdb.head_postings_for_matchers_cache_ttl.as_nanos().max(0) as u64,
                ),
                max_items: usize::MAX,
                max_bytes: blocks_cfg.tsdb.head_postings_for_matchers_cache_max_bytes.max(0)
                    as usize,
                enable_invalidation: true,
                num_metric_versions: blocks_cfg.tsdb.head_postings_for_matchers_cache_versions.max(0)
                    as usize,
                metrics: shared_metrics.head_postings_cache.clone(),
                // Go tracks metric versions only for a shared cache with invalidation.
                report_invalidations: blocks_cfg.tsdb.shared_postings_for_matchers_cache
                    && blocks_cfg.tsdb.head_postings_for_matchers_cache_invalidation,
            },
            metadata_limits: MetadataLimits {
                max_metrics_with_metadata_per_user: limiter.max_metadata_per_user(&user_id),
                max_metadata_per_metric: limiter.max_metadata_per_metric(&user_id),
            },
            series_lifecycle_callback: Some(lifecycle_adapter.clone()),
            registry: tsdb_prom_reg,
        };

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
        let user_db_ready = Arc::new(AtomicBool::new(false));
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
            enable_bigger_ooo_block_for_old_samples: blocks_cfg
                .tsdb
                .bigger_out_of_order_blocks_for_old_samples,
            // Go `blockPostingsForMatchersCacheFactory` (no invalidation, MaxItems: math.MaxInt).
            block_postings_cache: PostingsForMatchersCacheConfig {
                ttl: Duration::from_nanos(
                    blocks_cfg.tsdb.block_postings_for_matchers_cache_ttl.as_nanos().max(0) as u64,
                ),
                max_items: usize::MAX,
                max_bytes: blocks_cfg.tsdb.block_postings_for_matchers_cache_max_bytes.max(0)
                    as usize,
                enable_invalidation: false,
                num_metric_versions: 0,
                metrics: shared_metrics.block_postings_cache.clone(),
                report_invalidations: false,
            },
        };

        let db = Arc::new(Tsdb::open(&udir, tsdb_opts)?);
        let db_for_fn = Arc::clone(&db);
        *lifecycle_adapter.num_series_fn.write() =
            Some(Arc::new(move || db_for_fn.head().num_series()));

        // Compact ready blocks after WAL replay
        let _ = db.compact()?;
        pending_series_removals.store(0, Ordering::Release);

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
        let mut guard = self.shipped_blocks.lock();
        *guard = m
            .into_iter()
            .map(|(k, mt)| (k, UNIX_EPOCH + Duration::from_millis(mt.0.max(0) as u64)))
            .collect();
        Ok(())
    }

    pub fn cached_shipped_blocks(&self) -> HashMap<Ulid, SystemTime> {
        self.shipped_blocks.lock().clone()
    }

    pub fn oldest_unshipped_block_time_ms(&self) -> u64 {
        if self.shipper.read().is_none() {
            return 0;
        }
        let shipped = self.shipped_blocks.lock();
        let mut oldest = 0u64;
        for b in self.db.blocks() {
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
        self.db.compact()
    }

    /// Force-compact TSDB head up to `forced_compaction_max_time` (or `i64::MAX` for full flush).
    pub async fn compact_head(&self, forced_compaction_max_time: i64) -> Result<(), String> {
        let (ok, s) =
            self.change_state_to_forced_compaction(TsdbState::Active, forced_compaction_max_time);
        if !ok {
            return Err(format!(
                "TSDB head cannot be compacted because it is not in active state: {}",
                s.as_str()
            ));
        }

        self.wait_in_flight_before_forced_compaction().await;

        let db = Arc::clone(&self.db);
        let block_duration_ms = self.block_duration_ms;
        let res = tokio::task::spawn_blocking(move || -> Result<(), TsdbError> {
            let head = db.head();
            if head.initialized() && head.num_series() > 0 {
                loop {
                    let h_min = head.min_time();
                    let h_max = head.max_time();
                    let Some((mint, maxt_incl, is_last)) = next_forced_head_compaction_range(
                        block_duration_ms,
                        h_min,
                        h_max,
                        forced_compaction_max_time,
                    ) else {
                        break;
                    };
                    db.compact_head_range(mint, maxt_incl.saturating_add(1))?;
                    if is_last || !head.initialized() || head.num_series() == 0 {
                        break;
                    }
                }
            }
            let _ = db.compact_ooo_head()?;
            Ok(())
        })
        .await
        .unwrap_or_else(|e| {
            Err(TsdbError::Io(std::io::Error::other(format!(
                "compact_head join error: {e}"
            ))))
        });

        let _ = self.change_state(TsdbState::ForceCompacting, TsdbState::Active);
        res.map_err(|e| e.to_string())
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
                self.active_series.delete(ref_id);
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
        _reason: &str,
        mut compute_fn: impl FnMut() -> usize,
    ) -> (bool, usize) {
        pub const MAX_ATTEMPTS: usize = 3;
        pub const MAX_DIFF: isize = 1000;

        let mut attempts = 0usize;
        for _ in 0..MAX_ATTEMPTS {
            attempts += 1;
            let owned_before = self.owned_series_state().owned_series_count as isize;
            let local_limit_new = self.limiter.max_series_per_user(&self.user_id, 0);
            let owned_new = compute_fn();

            let mut os = self.owned_state.lock();
            let diff = (os.owned_series_count as isize) - owned_before;
            os.owned_series_count = owned_new;
            os.shard_size = shard_size;
            os.local_series_limit = local_limit_new;
            if (0..=MAX_DIFF).contains(&diff) {
                return (true, attempts);
            }
        }
        (false, attempts)
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
        self.db.close()
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
        }
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
    let min_bucket = min_time.div_euclid(block_duration);
    let max_bucket = max_time.div_euclid(block_duration);
    if min_bucket != max_bucket {
        let split_max = (min_bucket + 1) * block_duration - 1;
        Some((min_time, split_max, false))
    } else {
        Some((min_time, max_time, true))
    }
}

struct MetadataEntry {
    metric_type: MetricType,
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

#[inline]
fn proto_metric_type_to_tsdb(t: i32) -> MetricType {
    match cortexpb::metric_metadata::MetricType::try_from(t) {
        Ok(cortexpb::metric_metadata::MetricType::Counter) => MetricType::Counter,
        Ok(cortexpb::metric_metadata::MetricType::Gauge) => MetricType::Gauge,
        Ok(cortexpb::metric_metadata::MetricType::Histogram) => MetricType::HistogramSample,
        Ok(cortexpb::metric_metadata::MetricType::Gaugehistogram) => MetricType::GaugeHistogram,
        Ok(cortexpb::metric_metadata::MetricType::Summary) => MetricType::Summary,
        Ok(cortexpb::metric_metadata::MetricType::Info) => MetricType::Info,
        Ok(cortexpb::metric_metadata::MetricType::Stateset) => MetricType::Stateset,
        _ => MetricType::Unknown,
    }
}

#[inline]
fn tsdb_metric_type_to_proto(t: MetricType) -> i32 {
    match t {
        MetricType::Counter => cortexpb::metric_metadata::MetricType::Counter as i32,
        MetricType::Gauge => cortexpb::metric_metadata::MetricType::Gauge as i32,
        MetricType::HistogramSample => cortexpb::metric_metadata::MetricType::Histogram as i32,
        MetricType::GaugeHistogram => cortexpb::metric_metadata::MetricType::Gaugehistogram as i32,
        MetricType::Summary => cortexpb::metric_metadata::MetricType::Summary as i32,
        MetricType::Info => cortexpb::metric_metadata::MetricType::Info as i32,
        MetricType::Stateset => cortexpb::metric_metadata::MetricType::Stateset as i32,
        MetricType::Unknown => cortexpb::metric_metadata::MetricType::Unknown as i32,
    }
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
        let metric_type = proto_metric_type_to_tsdb(raw_type);

        let max_per_user = self.limiter.max_metadata_per_user(&self.user_id);
        let max_per_metric = self.limiter.max_metadata_per_metric(&self.user_id);

        // Lock-free-for-readers fast path: 97%+ of metadata updates refresh an existing entry.
        {
            let guard = self.inner.read();
            if let Some(entries) = guard.metrics.get(metric_family_name) {
                if max_per_metric == 0 || entries.len() < max_per_metric {
                    for e in entries {
                        if e.metric_type == metric_type && e.help == help && e.unit == unit {
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
                return Err(IngesterError::per_metric_metadata_limit_reached(
                    max_per_metric,
                    metric_family_name.to_string(),
                ));
            }
            for e in entries {
                if e.metric_type == metric_type && e.help == help && e.unit == unit {
                    e.last_seen_nanos.store(now_nanos, Ordering::Relaxed);
                    return Ok(());
                }
            }
        } else if max_per_user > 0 && guard.metrics.len() >= max_per_user {
            self.discarded_per_user
                .with_label_values(&[&self.user_id])
                .inc();
            return Err(IngesterError::per_user_metadata_limit_reached(max_per_user));
        }

        let entry = MetadataEntry {
            metric_type,
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
            self.mem_removed
                .with_label_values(&[&self.user_id])
                .inc_by(deleted as f64);
        }
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
                        r#type: tsdb_metric_type_to_proto(e.metric_type),
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
}
