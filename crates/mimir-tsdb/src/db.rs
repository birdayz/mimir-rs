//! Unified Prometheus TSDB (`Head` + on-disk ULID `Block`s + Compaction + Querier).
//!
//! Ported from github.com/prometheus/prometheus/tsdb/db.go and the `LeveledCompactor`
//! metrics of github.com/prometheus/prometheus/tsdb/compact.go.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_common::native_histogram::{exponential_buckets, SingleHistogram};
use parking_lot::{Condvar, Mutex, RwLock};
use prometheus::Registry;
use thiserror::Error;
use ulid::Ulid;

use crate::block::{
    parse_block_dir_name, read_meta_file, Block, BlockDesc, BlockError, BlockMeta,
    BlockMetaCompaction,
};
use crate::chunkenc::Encoding;
use crate::compact::{
    compact_blocks_with_float_encoding_and_segment_size_and_metrics, plan_block_compactions,
    write_block_from_block_with_base, write_head_block_streaming_entries_with_options,
    write_head_block_streaming_with_options, CompactError,
    CompactorMetrics,
};
use crate::exemplar::{CircularExemplarStorage, ExemplarQueryResult};
use crate::head::{
    range_for_timestamp, CompactNotifier, EvictedSeries, Head, HeadAppender, HeadError,
    HeadOptions,
};
use crate::matcher::LabelMatcher;
use crate::metrics::{counter, gauge, native_histogram, FnGaugeCollector};
use crate::postings_cache::{PostingsForMatchersCache, PostingsForMatchersCacheConfig};
use crate::querier::{
    snapshot_head_query_source, snapshot_query_sources_with_lock, HeadQuerySnapshot,
    HeadSnapshotMode, QuerySourceSnapshot, TsdbChunkQuerier, TsdbQuerier,
};

/// Go `tmpForDeletionBlockDirSuffix`.
pub const TMP_FOR_DELETION_BLOCK_DIR_SUFFIX: &str = ".tmp-for-deletion";
/// Go `tmpForCreationBlockDirSuffix`.
pub const TMP_FOR_CREATION_BLOCK_DIR_SUFFIX: &str = ".tmp-for-creation";
/// Go `tmpLegacy`.
pub const TMP_LEGACY_SUFFIX: &str = ".tmp";
/// Go `chunkSnapshotPrefix`.
pub const CHUNK_SNAPSHOT_PREFIX: &str = "chunk_snapshot.";
/// Go `defaultBlockReloadInterval` (`1m`).
pub const DEFAULT_BLOCK_RELOAD_INTERVAL: Duration = Duration::from_secs(60);

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Error)]
pub enum TsdbError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("head error: {0}")]
    Head(#[from] HeadError),
    #[error("block error: {0}")]
    Block(#[from] BlockError),
    #[error("compaction error: {0}")]
    Compact(#[from] CompactError),
    #[error("DB closed")]
    Closed,
    #[error("{0}")]
    Other(String),
}

/// TimeRange specifies minTime and maxTime range (Go tsdb.TimeRange).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeRange {
    pub min: i64,
    pub max: i64,
}

/// Returns all overlapping blocks from given meta slice (Go tsdb.OverlappingBlocks).
pub fn overlapping_blocks(bm: &[BlockMeta]) -> BTreeMap<TimeRange, Vec<BlockMeta>> {
    if bm.len() <= 1 {
        return BTreeMap::new();
    }
    let mut overlaps: Vec<Vec<BlockMeta>> = Vec::new();
    let mut pending: Vec<BlockMeta> = vec![bm[0].clone()];
    let mut continuous_pending = true;

    for b in &bm[1..] {
        let mut new_pending: Vec<BlockMeta> = Vec::new();
        for p in &pending {
            if b.min_time >= p.max_time {
                continuous_pending = false;
                continue;
            }
            new_pending.push(p.clone());
        }
        pending = new_pending.clone();
        pending.push(b.clone());
        if new_pending.is_empty() {
            continue;
        }
        if continuous_pending && !overlaps.is_empty() {
            if let Some(last) = overlaps.last_mut() {
                last.push(b.clone());
            }
            continue;
        }
        new_pending.push(b.clone());
        overlaps.push(new_pending);
        continuous_pending = true;
    }

    let mut overlap_groups = BTreeMap::new();
    for overlap in overlaps {
        let mut min_range = TimeRange {
            min: 0,
            max: i64::MAX,
        };
        for b in &overlap {
            if min_range.max > b.max_time {
                min_range.max = b.max_time;
            }
            if min_range.min < b.min_time {
                min_range.min = b.min_time;
            }
        }
        overlap_groups.insert(min_range, overlap);
    }
    overlap_groups
}

/// Clamps BlockReloadInterval matching Go validateOpts:
/// 0 -> DEFAULT_BLOCK_RELOAD_INTERVAL (1m), < 1s -> 1s.
pub fn effective_block_reload_interval(d: Duration) -> Duration {
    if d.is_zero() {
        DEFAULT_BLOCK_RELOAD_INTERVAL
    } else if d < Duration::from_secs(1) {
        Duration::from_secs(1)
    } else {
        d
    }
}

fn compute_compaction_delay(chunk_range_ms: i64, max_percent: i32) -> Duration {
    if chunk_range_ms <= 0 || max_percent <= 0 {
        return Duration::ZERO;
    }
    let upper = (chunk_range_ms.saturating_mul(i64::from(max_percent)) / 100).max(0) as u64;
    if upper == 0 {
        return Duration::ZERO;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() as u64);
    let mix = nanos.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    Duration::from_millis(mix % upper)
}

/// OS filesystem size in bytes for percentage-based size retention (Go prom_runtime.FsSize).
fn os_fs_size(path: &Path) -> u64 {
    #[cfg(all(unix, not(miri)))]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        if let Ok(cstr) = CString::new(path.as_os_str().as_bytes()) {
            // SAFETY: zeroed statvfs buffer filled by libc::statvfs.
            let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
            // SAFETY: valid NUL-terminated path and valid statvfs pointer.
            if unsafe { libc::statvfs(cstr.as_ptr(), &mut st) } == 0 {
                #[allow(clippy::unnecessary_cast)]
                return (st.f_blocks as u64).saturating_mul(st.f_frsize as u64);
            }
        }
    }
    let _ = path;
    0
}

/// Custom compactor override hook (matching Go Options.NewCompactorFunc / db.compactor tests).
pub trait TsdbCompactorOverride: Send + Sync {
    fn plan(&self, _dir: &Path) -> Result<Vec<PathBuf>, TsdbError> {
        Ok(Vec::new())
    }
    fn write(
        &self,
        _dest: &Path,
        _block: &Block,
        _mint: i64,
        _maxt: i64,
        _base: Option<&BlockMeta>,
    ) -> Result<Vec<Ulid>, TsdbError> {
        Ok(Vec::new())
    }
    fn compact(
        &self,
        _dest: &Path,
        _dirs: &[PathBuf],
        _blocks: &[&Block],
    ) -> Result<Vec<Ulid>, TsdbError> {
        Ok(Vec::new())
    }
    fn compact_ooo(&self, _dest: &Path) -> Result<Vec<Ulid>, TsdbError> {
        Err(TsdbError::Other(
            "mock compaction failing CompactOOO".to_string(),
        ))
    }
}

/// Runtime TSDB configuration update passed to Tsdb::apply_config (Go DB.ApplyConfig).
#[derive(Clone, Debug, Default)]
pub struct TsdbRuntimeConfig {
    pub retention_duration_ms: Option<i64>,
    pub max_bytes: Option<i64>,
    pub max_percentage: Option<f64>,
    pub float_chunk_encoding: Option<Encoding>,
    pub ooo_time_window_ms: Option<i64>,
    pub max_exemplars: Option<usize>,
    pub stale_series_compaction_threshold: Option<f64>,
    /// When true, resets encoding/ooo/stale_threshold as if TSDBConfig == nil in Go ApplyConfig.
    pub clear_tsdb_config: bool,
}

/// Extra non-breaking options for Tsdb::open_with_extra.
pub type FsSizeFn = Arc<dyn Fn(&Path) -> u64 + Send + Sync>;
pub type BlockFilterFn = Arc<dyn Fn(&Block) -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub struct TsdbExtraOptions {
    pub disable_wal: bool,
    pub enable_metadata_wal_records: Option<bool>,
    pub enable_memory_snapshot_on_shutdown: bool,
    pub max_bytes: i64,
    pub max_percentage: f64,
    pub fs_size_func: Option<FsSizeFn>,
    pub enable_delayed_compaction: bool,
    pub compaction_delay_max_percent: i32,
    pub block_reload_interval: Option<Duration>,
    pub block_querier_filter: Option<BlockFilterFn>,
    pub block_chunk_querier_filter: Option<BlockFilterFn>,
    pub stale_series_compaction_threshold: f64,
    pub compactor_override: Option<Arc<dyn TsdbCompactorOverride>>,
    pub isolation_disabled: bool,
}

/// Go `BlocksToDeleteFunc`, given the loadable blocks and Go `DefaultBlocksToDelete(db)`
/// (`Compaction.Deletable` + time retention, counting `prometheus_tsdb_time_retentions_total`),
/// which the callback may or may not invoke (Mimir's `userTSDB.blocksToDelete` filters it).
pub type BlocksToDeleteFn = Arc<
    dyn Fn(&[Arc<Block>], &dyn Fn(&[Arc<Block>]) -> HashSet<Ulid>) -> HashSet<Ulid> + Send + Sync,
>;

/// Optional callback invoked outside `blocks.write()` whenever `reload_blocks_locked`
/// swaps in a changed block slice. Receives a closure that reads the live `blocks`
/// snapshot so callers can serialize external locks (e.g. `shipped_blocks`) BEFORE
/// reading `blocks`.
pub type BlocksReloadedFn = Arc<dyn Fn(&dyn Fn() -> Vec<Arc<Block>>) + Send + Sync>;

#[derive(Clone)]
pub struct TsdbOptions {
    pub head: HeadOptions,
    pub max_block_chunk_segment_size: usize,
    pub no_lockfile: bool,
    /// Go `Options.RetentionDuration` in milliseconds (0 disables time retention).
    pub retention_duration_ms: i64,
    /// Go `Options.BlocksToDelete` (`None` = `DefaultBlocksToDelete`).
    pub blocks_to_delete: Option<BlocksToDeleteFn>,
    /// Invoked after `blocks` changes on reload.
    pub on_blocks_reload: Option<BlocksReloadedFn>,
    /// Go `Options.EnableBiggerOOOBlockForOldSamples`.
    pub enable_bigger_ooo_block_for_old_samples: bool,
    /// Per-block `PostingsForMatchersCache` (Mimir `block_postings_for_matchers_cache_*`).
    pub block_postings_cache: PostingsForMatchersCacheConfig,
    /// Optional shared `PostingsForMatchersCache` across all tenant Blocks when
    /// `shared_postings_for_matchers_cache: true`.
    pub shared_block_postings_cache: Option<Arc<PostingsForMatchersCache>>,
    /// Go `Options.CompactionDelay` (delays auto-compaction of Head after becoming compactable).
    pub compaction_delay: Duration,
}

impl std::fmt::Debug for TsdbOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TsdbOptions")
            .field("head", &self.head)
            .field(
                "max_block_chunk_segment_size",
                &self.max_block_chunk_segment_size,
            )
            .field("no_lockfile", &self.no_lockfile)
            .field("retention_duration_ms", &self.retention_duration_ms)
            .field(
                "blocks_to_delete",
                &self.blocks_to_delete.as_ref().map(|_| "..."),
            )
            .field(
                "enable_bigger_ooo_block_for_old_samples",
                &self.enable_bigger_ooo_block_for_old_samples,
            )
            .field("block_postings_cache", &self.block_postings_cache)
            .field(
                "shared_block_postings_cache",
                &self.shared_block_postings_cache.as_ref().map(|_| "..."),
            )
            .finish()
    }
}

impl Default for TsdbOptions {
    fn default() -> Self {
        Self {
            head: HeadOptions::default(),
            max_block_chunk_segment_size: 0,
            no_lockfile: true,
            retention_duration_ms: 0,
            blocks_to_delete: None,
            on_blocks_reload: None,
            enable_bigger_ooo_block_for_old_samples: false,
            block_postings_cache: PostingsForMatchersCacheConfig::default(),
            shared_block_postings_cache: None,
            compaction_delay: Duration::ZERO,
        }
    }
}

/// Go `dbMetrics` + `CompactorMetrics`.
struct DbMetrics {
    compactions_total: prometheus::Counter,
    compaction_duration: SingleHistogram,
    compactions_triggered: prometheus::Counter,
    compactions_skipped: prometheus::Counter,
    compactions_failed: prometheus::Counter,
    reloads: prometheus::Counter,
    reloads_failed: prometheus::Counter,
    time_retentions: prometheus::Counter,
    size_retentions: prometheus::Counter,
    tombstone_cleanup_duration: SingleHistogram,
    blocks_bytes: prometheus::Gauge,
    blocks_loaded: prometheus::Gauge,
    symbol_table_size: prometheus::Gauge,
    retention_limit_bytes: prometheus::Gauge,
    retention_limit_percentage: prometheus::Gauge,
    retention_limit_seconds: prometheus::Gauge,
    stale_series_compactions_triggered: prometheus::Counter,
    stale_series_compactions_failed: prometheus::Counter,
    stale_series_compaction_duration: SingleHistogram,
    selected_series_compactions_triggered: prometheus::Counter,
    selected_series_compactions_failed: prometheus::Counter,
    selected_series_compaction_duration: SingleHistogram,
    clean_start: prometheus::Gauge,
    compactor: CompactorMetrics,
}

impl DbMetrics {
    fn new(reg: Option<&Registry>) -> Self {
        let compaction_duration = native_histogram(
            reg,
            "prometheus_tsdb_compaction_duration_seconds",
            "Duration of compaction runs",
            Some(exponential_buckets(1.0, 2.0, 14)),
        );
        let tombstone_cleanup_duration = native_histogram(
            reg,
            "prometheus_tsdb_tombstone_cleanup_seconds",
            "The time taken to recompact blocks to remove tombstones.",
            None,
        );
        let compactions_skipped = counter(
            reg,
            "prometheus_tsdb_compactions_skipped_total",
            "Total number of skipped compactions due to disabled auto compaction.",
        );
        let size_retentions = counter(
            reg,
            "prometheus_tsdb_size_retentions_total",
            "The number of times that blocks were deleted because the maximum number of bytes was exceeded.",
        );
        let stale_series_compactions_triggered = counter(
            reg,
            "prometheus_tsdb_stale_series_compactions_triggered_total",
            "Total number of triggered stale series compactions.",
        );
        let stale_series_compactions_failed = counter(
            reg,
            "prometheus_tsdb_stale_series_compactions_failed_total",
            "Total number of stale series compactions that failed.",
        );
        let stale_series_compaction_duration = native_histogram(
            reg,
            "prometheus_tsdb_stale_series_compaction_duration_seconds",
            "Duration of stale series compaction runs.",
            Some(exponential_buckets(1.0, 2.0, 14)),
        );
        let selected_series_compaction_duration = native_histogram(
            reg,
            "prometheus_tsdb_selected_series_compaction_duration_seconds",
            "Duration of compactions triggered for an explicit caller-provided list of series references.",
            Some(exponential_buckets(1.0, 2.0, 14)),
        );
        let compactor = CompactorMetrics::new(reg);
        let clean_start = gauge(
            reg,
            "prometheus_tsdb_clean_start",
            "-1: lockfile is disabled. 0: a lockfile from a previous execution was replaced. 1: lockfile creation was clean",
        );
        clean_start.set(-1.0);
        Self {
            compactions_total: counter(
                reg,
                "prometheus_tsdb_compactions_total",
                "Total number of compactions that were executed for the partition.",
            ),
            compaction_duration,
            compactions_triggered: counter(
                reg,
                "prometheus_tsdb_compactions_triggered_total",
                "Total number of triggered compactions for the partition.",
            ),
            compactions_skipped,
            compactions_failed: counter(
                reg,
                "prometheus_tsdb_compactions_failed_total",
                "Total number of compactions that failed for the partition.",
            ),
            reloads: counter(
                reg,
                "prometheus_tsdb_reloads_total",
                "Number of times the database reloaded block data from disk.",
            ),
            reloads_failed: counter(
                reg,
                "prometheus_tsdb_reloads_failures_total",
                "Number of times the database failed to reloadBlocks block data from disk.",
            ),
            time_retentions: counter(
                reg,
                "prometheus_tsdb_time_retentions_total",
                "The number of times that blocks were deleted because the maximum time limit was exceeded.",
            ),
            size_retentions,
            tombstone_cleanup_duration,
            blocks_bytes: gauge(
                reg,
                "prometheus_tsdb_storage_blocks_bytes",
                "The number of bytes that are currently used for local storage by all blocks.",
            ),
            blocks_loaded: gauge(
                reg,
                "prometheus_tsdb_blocks_loaded",
                "Number of currently loaded data blocks",
            ),
            symbol_table_size: gauge(
                reg,
                "prometheus_tsdb_symbol_table_size_bytes",
                "Size of symbol table in memory for loaded blocks",
            ),
            retention_limit_bytes: gauge(
                reg,
                "prometheus_tsdb_retention_limit_bytes",
                "Max number of bytes to be retained in the tsdb blocks, configured 0 means disabled",
            ),
            retention_limit_percentage: gauge(
                reg,
                "prometheus_tsdb_retention_limit_percentage",
                "Max percentage of total storage space to be retained in the tsdb blocks, configured 0 means disabled",
            ),
            retention_limit_seconds: gauge(
                reg,
                "prometheus_tsdb_retention_limit_seconds",
                "How long to retain samples in storage.",
            ),
            stale_series_compactions_triggered,
            stale_series_compactions_failed,
            stale_series_compaction_duration,
            selected_series_compactions_triggered: counter(
                reg,
                "prometheus_tsdb_selected_series_compactions_triggered_total",
                "Total number of compactions triggered for an explicit caller-provided list of series references.",
            ),
            selected_series_compactions_failed: counter(
                reg,
                "prometheus_tsdb_selected_series_compactions_failed_total",
                "Total number of compactions triggered for an explicit caller-provided list of series references that failed.",
            ),
            selected_series_compaction_duration,
            clean_start,
            compactor,
        }
    }
}

/// Go `isTmpDir`.
fn is_tmp_dir_name(name: &str) -> bool {
    let ext = match name.rfind('.') {
        Some(i) => &name[i..],
        None => return false,
    };
    if ext != TMP_FOR_DELETION_BLOCK_DIR_SUFFIX
        && ext != TMP_FOR_CREATION_BLOCK_DIR_SUFFIX
        && ext != TMP_LEGACY_SUFFIX
    {
        return false;
    }
    if name.starts_with(CHUNK_SNAPSHOT_PREFIX) {
        return true;
    }
    let stem = &name[..name.len() - ext.len()];
    parse_block_dir_name(stem).is_some()
}

/// Go `tsdbutil.RemoveTmpDirs(l, dir, isTmpDir)`: best-effort removal of leftover
/// `<ulid>.tmp-for-creation` / `<ulid>.tmp-for-deletion` / `<ulid>.tmp` directories.
fn remove_tmp_dirs(dir: &Path) -> Result<(), TsdbError> {
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_tmp_dir_name(name) {
            continue;
        }
        let p = entry.path();
        match fs::remove_dir_all(&p) {
            Ok(()) => tracing::info!(dir = %p.display(), "Found and deleted tmp block dir"),
            Err(e) => {
                tracing::error!(dir = %p.display(), err = %e, "failed to delete tmp block dir")
            }
        }
    }
    Ok(())
}

/// Go `tsdbutil.DirLocker`: holds an exclusive non-blocking `flock` on `<dir>/lock`.
struct DirLocker {
    path: PathBuf,
    file: Option<fs::File>,
}

impl DirLocker {
    fn acquire(dir: &Path, clean_start: &prometheus::Gauge) -> Result<Self, TsdbError> {
        let abs_dir = std::path::absolute(dir)?;
        let lock_path = abs_dir.join("lock");
        if fs::metadata(&lock_path).is_ok() {
            tracing::warn!(
                file = %lock_path.display(),
                "A lockfile from a previous execution already existed. It was replaced"
            );
            clean_start.set(0.0);
        } else {
            clean_start.set(1.0);
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        if let Err(e) = file.try_lock() {
            return Err(TsdbError::Other(format!("lock DB directory: {e}")));
        }
        Ok(Self {
            path: lock_path,
            file: Some(file),
        })
    }

    fn release(&mut self) -> Result<(), TsdbError> {
        let Some(file) = self.file.take() else {
            return Ok(());
        };
        let unlock_res = file.unlock();
        drop(file);
        let remove_res = fs::remove_file(&self.path);
        match (unlock_res, remove_res) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(u), Ok(())) => Err(TsdbError::Io(u)),
            (Ok(()), Err(r)) => Err(TsdbError::Io(r)),
            (Err(u), Err(r)) => Err(TsdbError::Other(format!("{u}; {r}"))),
        }
    }
}

impl Drop for DirLocker {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

struct RetentionSettings {
    retention_duration_ms: i64,
    max_bytes: i64,
    max_percentage: f64,
}

struct TsdbInner {
    dir: PathBuf,
    opts: TsdbOptions,
    extra: TsdbExtraOptions,
    retention: RwLock<RetentionSettings>,
    configured_block_reload_interval: Duration,
    compaction_delay: Mutex<Duration>,
    head: Arc<Head>,
    blocks: Arc<RwLock<Vec<Arc<Block>>>>,
    /// Protected by blocks.write() on updates and read under blocks.read() when snapshotting
    /// query sources (Go DB.lastGarbageCollectedMmapRef, tsdb/db.go:441).
    last_garbage_collected_mmap_ref: AtomicU64,
    closed: AtomicBool,
    compact_mtx: Mutex<()>,
    compact_pending: AtomicBool,
    auto_compactions_enabled: AtomicBool,
    time_when_compaction_delay_started: Mutex<Option<Instant>>,
    bg_stop: Arc<(Mutex<bool>, Condvar)>,
    block_ranges_ms: RwLock<Vec<i64>>,
    enable_overlapping_compaction: AtomicBool,
    block_postings_cache_force: AtomicBool,
    block_postings_cache_shared: AtomicBool,
    compact_head_view_before_evict_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    compact_ooo_head_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    compactor_override: Mutex<Option<Arc<dyn TsdbCompactorOverride>>>,
    metrics: DbMetrics,
    /// Kept last so Head and Blocks always drop before `<dir>/lock` is released on drop/unwind.
    dir_lock: Mutex<Option<DirLocker>>,
}

impl CompactNotifier for TsdbInner {
    #[inline]
    fn notify_compact(&self) {
        if !self.compact_pending.swap(true, Ordering::AcqRel) {
            let _guard = self.bg_stop.0.lock();
            self.bg_stop.1.notify_all();
        }
    }
}

/// Queryable restricted to single-source non-OOO native readers (`tsdb.costSamplingQueryable`).
pub struct CostSamplingQueryable<'a> {
    db: &'a Tsdb,
}

impl<'a> CostSamplingQueryable<'a> {
    fn check_layout(&self, mint: i64, maxt: i64) -> Result<(), crate::querier::QuerierError> {
        let snap = self.db.try_snapshot_query_sources(mint, maxt)?;
        let head_has_in_order = snap.head.as_ref().is_some_and(|_| {
            self.db.head().initialized()
                && self.db.head().min_time() <= maxt
                && mint <= self.db.head().max_time()
        });
        let head_has_ooo = snap.head.as_ref().is_some_and(|_| {
            let mo = self.db.head().min_ooo_time();
            let xo = self.db.head().max_ooo_time();
            mo != i64::MAX && mo <= maxt && mint <= xo
        });
        if head_has_ooo {
            return Err(crate::querier::QuerierError::CostSamplingUnsupported);
        }
        let head_sources = usize::from(head_has_in_order);
        let total_sources = snap.blocks.len() + head_sources;
        if total_sources > 1 {
            return Err(crate::querier::QuerierError::CostSamplingUnsupported);
        }
        Ok(())
    }

    pub fn querier(&self, mint: i64, maxt: i64) -> Result<TsdbQuerier<'a>, crate::querier::QuerierError> {
        self.check_layout(mint, maxt)?;
        Ok(self.db.querier(mint, maxt))
    }

    pub fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<TsdbChunkQuerier<'a>, crate::querier::QuerierError> {
        self.check_layout(mint, maxt)?;
        Ok(self.db.chunk_querier(mint, maxt))
    }
}

/// Unified TSDB instance managing an active [`Head`] and immutable on-disk ULID [`Block`]s.
pub struct Tsdb {
    inner: Arc<TsdbInner>,
    bg_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Tsdb {
    /// Opens (or creates) a TSDB in `dir`, loading all on-disk ULID blocks and replaying WAL/WBL/chunks_head.
    /// Runs the Go `DB.run()` 1-minute maintenance loop (`reload_blocks` + `mmap_head_chunks`).
    pub fn open(dir: impl AsRef<Path>, opts: TsdbOptions) -> Result<Self, TsdbError> {
        Self::open_with_reload_interval(dir, opts, DEFAULT_BLOCK_RELOAD_INTERVAL)
    }

    /// Opens a TSDB with TsdbExtraOptions.
    pub fn open_with_extra(
        dir: impl AsRef<Path>,
        opts: TsdbOptions,
        extra: TsdbExtraOptions,
    ) -> Result<Self, TsdbError> {
        let interval = extra
            .block_reload_interval
            .map(effective_block_reload_interval)
            .unwrap_or(DEFAULT_BLOCK_RELOAD_INTERVAL);
        Self::open_with_reload_interval_and_extra(dir, opts, interval, extra)
    }

    /// Opens (or creates) a TSDB with a custom background block_reload_interval
    /// (passing Duration::ZERO disables the background loop).
    pub fn open_with_reload_interval(
        dir: impl AsRef<Path>,
        opts: TsdbOptions,
        block_reload_interval: Duration,
    ) -> Result<Self, TsdbError> {
        Self::open_with_reload_interval_and_extra(
            dir,
            opts,
            block_reload_interval,
            TsdbExtraOptions::default(),
        )
    }

    /// Opens a TSDB with custom background reload interval and TsdbExtraOptions.
    pub fn open_with_reload_interval_and_extra(
        dir: impl AsRef<Path>,
        mut opts: TsdbOptions,
        block_reload_interval: Duration,
        extra: TsdbExtraOptions,
    ) -> Result<Self, TsdbError> {
        // Go `validateOpts` (db.go:1067-1075).
        if opts.head.float_chunk_encoding == Encoding::None {
            opts.head.float_chunk_encoding = Encoding::Xor;
        }
        match opts.head.float_chunk_encoding {
            Encoding::Xor | Encoding::Xor2 => {}
            other => {
                return Err(TsdbError::Other(format!(
                    "unsupported float chunk encoding {other:?}; valid values are \"xor\" and \"xor2\""
                )));
            }
        }
        if opts.head.enable_st_storage && opts.head.float_chunk_encoding == Encoding::Xor {
            return Err(TsdbError::Other(
                "float chunk encoding \"xor\" is incompatible with start-timestamp storage; XOR chunks do not store start timestamps, use \"xor2\""
                    .to_string(),
            ));
        }

        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let metrics = DbMetrics::new(opts.head.registry.as_deref());
        // Acquire the directory lockfile before touching WAL/tmp/blocks (Go db.go:1219-1227).
        let dir_lock = if !opts.no_lockfile {
            Some(DirLocker::acquire(&dir, &metrics.clean_start)?)
        } else {
            None
        };

        // Go `DB.Open`: remove leftover tmp block dirs (crashed compactions / deletions)
        // and interrupted temporary checkpoints.
        for tmp_dir in [dir.join("wal"), dir.clone()] {
            remove_tmp_dirs(&tmp_dir)?;
            crate::wlog::delete_temp_checkpoints(&tmp_dir).map_err(|e| {
                TsdbError::Other(format!("delete temp checkpoints: {e}"))
            })?;
        }
        metrics
            .retention_limit_bytes
            .set((extra.max_bytes.max(0)) as f64);
        metrics
            .retention_limit_percentage
            .set(extra.max_percentage.max(0.0));
        metrics
            .retention_limit_seconds
            .set((opts.retention_duration_ms.max(0) as f64) / 1000.0);

        let retention_init = RetentionSettings {
            retention_duration_ms: opts.retention_duration_ms,
            max_bytes: extra.max_bytes,
            max_percentage: extra.max_percentage,
        };

        // Go DB.Open -> repairBadIndexVersion (Prometheus 2.1 index/meta version fix).
        crate::block::repair_bad_index_version(&dir)?;

        // Go DB.Open -> db.reload() -> reloadBlocks() (before the head is replayed).
        let blocks_vec = {
            let res = (|| -> Result<Vec<Arc<Block>>, TsdbError> {
                let (to_load, deletable) = TsdbInner::prepare_reload_blocks(
                    &dir,
                    &opts,
                    &extra,
                    &retention_init,
                    0,
                    &metrics,
                    &[],
                )?;
                TsdbInner::delete_blocks(&dir, &deletable)?;
                Ok(to_load)
            })();
            if res.is_err() {
                metrics.reloads_failed.inc();
            }
            metrics.reloads.inc();
            res?
        };
        let in_order_max_time = blocks_vec
            .iter()
            .filter(|b| {
                !b.meta().out_of_order
                    && !b.meta().compaction.from_out_of_order()
                    && !b.meta().compaction.from_stale_series()
                    && !b.meta().compaction.from_selected_series()
            })
            .map(|b| b.max_time())
            .max();
        let mut min_valid_time = in_order_max_time.unwrap_or(i64::MIN);

        if !extra.disable_wal {
            if let Ok(Some(stored_mvt)) = crate::wlog::read_min_valid_time(&dir.join("wal")) {
                min_valid_time = min_valid_time.max(stored_mvt);
            }
        }

        let initial_ranges = vec![opts.head.chunk_range_ms];
        let enable_meta_wal = extra.enable_metadata_wal_records.unwrap_or(true);
        let head = Arc::new(Head::open_with_extra_ext(
            &dir,
            opts.head.clone(),
            min_valid_time,
            in_order_max_time,
            extra.disable_wal,
            enable_meta_wal,
            extra.enable_memory_snapshot_on_shutdown,
            extra.isolation_disabled,
        )?);
        let blocks = Arc::new(RwLock::new(blocks_vec));
        {
            let blks = Arc::downgrade(&blocks);
            let hd = Arc::downgrade(&head);
            FnGaugeCollector::register(
                opts.head.registry.as_deref(),
                "prometheus_tsdb_lowest_timestamp",
                "Lowest timestamp value stored in the database. The unit is decided by the library consumer.",
                move || {
                    let Some(blks) = blks.upgrade() else {
                        return i64::MAX as f64;
                    };
                    let guard = blks.read();
                    if let Some(first) = guard.first() {
                        first.min_time() as f64
                    } else if let Some(hd) = hd.upgrade() {
                        hd.min_time() as f64
                    } else {
                        i64::MAX as f64
                    }
                },
            );
        }

        let init_delay = if extra.enable_delayed_compaction
            && opts.compaction_delay.is_zero()
            && extra.compaction_delay_max_percent > 0
        {
            compute_compaction_delay(opts.head.chunk_range_ms, extra.compaction_delay_max_percent)
        } else {
            opts.compaction_delay
        };
        let compactor_override = Mutex::new(extra.compactor_override.clone());
        let bg_stop = Arc::new((Mutex::new(false), Condvar::new()));
        let inner = Arc::new(TsdbInner {
            dir,
            opts,
            extra,
            retention: RwLock::new(retention_init),
            configured_block_reload_interval: block_reload_interval,
            compaction_delay: Mutex::new(init_delay),
            head,
            blocks,
            last_garbage_collected_mmap_ref: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            compact_mtx: Mutex::new(()),
            compact_pending: AtomicBool::new(false),
            auto_compactions_enabled: AtomicBool::new(false),
            time_when_compaction_delay_started: Mutex::new(None),
            bg_stop: Arc::clone(&bg_stop),
            block_ranges_ms: RwLock::new(initial_ranges),
            enable_overlapping_compaction: AtomicBool::new(true),
            block_postings_cache_force: AtomicBool::new(false),
            block_postings_cache_shared: AtomicBool::new(false),
            compact_head_view_before_evict_hook: Mutex::new(None),
            compact_ooo_head_hook: Mutex::new(None),
            compactor_override,
            metrics,
            dir_lock: Mutex::new(dir_lock),
        });
        let bg_handle = if !block_reload_interval.is_zero() {
            let inner_bg = Arc::clone(&inner);
            let stop_bg = Arc::clone(&bg_stop);
            let handle = std::thread::Builder::new()
                .name("tsdb-reload".to_string())
                .spawn(move || {
                    let mut next_tick = Instant::now() + block_reload_interval;
                    let mut stopped = stop_bg.0.lock();
                    while !*stopped {
                        if inner_bg.compact_pending.swap(false, Ordering::AcqRel) {
                            drop(stopped);
                            inner_bg.metrics.compactions_triggered.inc();
                            if !inner_bg.auto_compactions_enabled.load(Ordering::Acquire) {
                                inner_bg.metrics.compactions_skipped.inc();
                            } else {
                                let _ = inner_bg.auto_compact_step();
                            }
                            if Instant::now() < next_tick {
                                stopped = stop_bg.0.lock();
                                continue;
                            }
                            stopped = stop_bg.0.lock();
                            if *stopped {
                                break;
                            }
                        }
                        let wait_res = stop_bg.1.wait_until(&mut stopped, next_tick);
                        if *stopped {
                            break;
                        }
                        if wait_res.timed_out() {
                            drop(stopped);
                            if let Err(e) = inner_bg.reload_blocks() {
                                if !matches!(e, TsdbError::Closed) {
                                    tracing::error!(err = %e, "reloadBlocks");
                                }
                            }
                            // Go `DB.run`: every block reload tick (and every `Commit` while
                            // `head.is_compactable()`) signals `compactc`, which counts as a
                            // triggered compaction and, as Mimir disables auto compactions
                            // (`db.DisableCompactions()`), as a skipped one.
                            inner_bg.compact_pending.store(true, Ordering::Release);
                            if let Err(e) = inner_bg.head.mmap_head_chunks() {
                                tracing::error!(err = %e, "mmapHeadChunks");
                            }
                            next_tick = Instant::now() + block_reload_interval;
                            stopped = stop_bg.0.lock();
                        }
                    }
                })?;
            Some(handle)
        } else {
            None
        };

        Ok(Self {
            inner,
            bg_thread: Mutex::new(bg_handle),
        })
    }

    fn stop_background_loop(&self) {
        let mut handle_guard = self.bg_thread.lock();
        if let Some(handle) = handle_guard.take() {
            {
                let mut stopped = self.inner.bg_stop.0.lock();
                *stopped = true;
                self.inner.bg_stop.1.notify_all();
            }
            let _ = handle.join();
        }
    }

    /// Reloads on-disk blocks, applying the block deletion filter (Go `DB.reloadBlocks`).
    pub fn reload_blocks(&self) -> Result<(), TsdbError> {
        self.inner.reload_blocks()
    }

    #[inline]
    pub fn dir(&self) -> &Path {
        &self.inner.dir
    }

    #[inline]
    pub fn head(&self) -> &Head {
        &self.inner.head
    }

    pub fn blocks(&self) -> Vec<Arc<Block>> {
        self.inner.blocks()
    }

    pub fn block_ulids(&self) -> Vec<Ulid> {
        self.inner.block_ulids()
    }

    /// Returns a new [`HeadAppender`] for writing samples, histograms, exemplars, and metadata.
    pub fn appender(&self) -> HeadAppender<'_> {
        self.inner
            .head
            .appender()
            .with_compact_notifier(&*self.inner)
    }

    pub fn appender_with_capacity(&self, series_hint: usize) -> HeadAppender<'_> {
        self.inner
            .head
            .appender_with_capacity(series_hint)
            .with_compact_notifier(&*self.inner)
    }

    /// Enables background auto-compactions (`DB.EnableCompactions`).
    pub fn enable_compactions(&self) {
        self.inner
            .auto_compactions_enabled
            .store(true, Ordering::Release);
    }

    /// Disables background auto-compactions (`DB.DisableCompactions`).
    pub fn disable_compactions(&self) {
        self.inner
            .auto_compactions_enabled
            .store(false, Ordering::Release);
    }

    /// Synchronously executes one auto-compaction trigger step (respecting `compaction_delay`).
    pub fn trigger_auto_compaction(&self) -> Result<Vec<Ulid>, TsdbError> {
        self.inner.metrics.compactions_triggered.inc();
        if !self.inner.auto_compactions_enabled.load(Ordering::Acquire) {
            self.inner.metrics.compactions_skipped.inc();
            return Ok(Vec::new());
        }
        self.inner.auto_compact_step()
    }

    /// Returns the timestamp when `compaction_delay` started, if currently waiting.
    pub fn compaction_delay_started(&self) -> Option<Instant> {
        *self.inner.time_when_compaction_delay_started.lock()
    }

    /// Go `DB.Compact`: compacts every ready `chunk_range` window of the Head into blocks,
    /// truncates the WAL (also when a later step fails, via the deferred truncation), then
    /// compacts the OOO head. Errors count `prometheus_tsdb_compactions_failed_total`.
    pub fn compact(&self) -> Result<Vec<Ulid>, TsdbError> {
        self.inner.compact()
    }

    /// Configures multi-level on-disk compaction block ranges in milliseconds
    /// (Mimir `-blocks-storage.tsdb.block-ranges-period`).
    pub fn set_block_ranges(&self, ranges_ms: Vec<i64>) {
        if !ranges_ms.is_empty() {
            *self.inner.block_ranges_ms.write() = ranges_ms;
        }
    }

    /// Returns the configured on-disk compaction block ranges in milliseconds.
    pub fn block_ranges(&self) -> Vec<i64> {
        self.inner.block_ranges_ms.read().clone()
    }

    /// Configures whether overlapping on-disk block compaction is enabled.
    pub fn set_enable_overlapping_compaction(&self, enabled: bool) {
        self.inner
            .enable_overlapping_compaction
            .store(enabled, Ordering::Release);
    }

    /// Runs on-disk multi-block and tombstone compaction (`DB.compactBlocks`).
    pub fn compact_blocks(&self) -> Result<Vec<Ulid>, TsdbError> {
        let _guard = self.inner.compact_mtx.lock();
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TsdbError::Closed);
        }
        let mut created = Vec::new();
        self.inner.compact_blocks_locked(&mut created)?;
        Ok(created)
    }

    /// Compacts the half-open time range `[mint, maxt)` of Head into a ULID block on disk,
    /// reloading blocks and truncating Head memory & WAL up to `maxt`.
    pub fn compact_head_range(&self, mint: i64, maxt: i64) -> Result<Option<Ulid>, TsdbError> {
        self.inner.compact_head_range(mint, maxt)
    }

    /// Compacts the closed time range `[mint, maxt_incl]` of Head into a ULID block on disk
    /// without truncating away `t == i64::MAX` via `maxt_incl.saturating_add(1) - 1`.
    pub fn compact_head_range_inclusive(
        &self,
        mint: i64,
        maxt_incl: i64,
    ) -> Result<Option<Ulid>, TsdbError> {
        self.inner.compact_head_range_inclusive(mint, maxt_incl)
    }

    /// Compacts the entire active Head `[head.min_time(), head.max_time() + 1)` into a ULID block.
    pub fn compact_head_all(&self) -> Result<Option<Ulid>, TsdbError> {
        self.inner.compact_head_all()
    }

    /// Compacts all out-of-order samples in Head into OOO ULID blocks (one per block range)
    /// and clears the compacted OOO state.
    pub fn compact_ooo_head(&self) -> Result<Vec<Ulid>, TsdbError> {
        self.inner.compact_ooo_head()
    }

    /// Go `DB.CompactSelectedSeries` -> `compactHeadViewLocked`: writes the selected series
    /// into one block per `chunk_range` window between the head's aligned min time and
    /// its max time, then evicts them from Head.
    pub fn compact_selected_series(&self, refs: &[u64]) -> Result<Vec<Ulid>, TsdbError> {
        self.inner.compact_selected_series(refs)
    }

    /// Go `DB.CompactStaleHead`: writes stale series without OOO data into blocks and
    /// evicts those that remain stale and unmodified.
    pub fn compact_stale_head(&self) -> Result<Vec<Ulid>, TsdbError> {
        self.inner.compact_stale_head()
    }

    #[doc(hidden)]
    pub fn set_compact_head_view_before_evict_hook_for_test(
        &self,
        hook: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        *self.inner.compact_head_view_before_evict_hook.lock() = hook;
    }

    #[doc(hidden)]
    pub fn try_lock_compaction_for_test(&self) -> bool {
        self.inner.compact_mtx.try_lock().is_some()
    }

    #[doc(hidden)]
    pub fn selected_series_compaction_metrics_for_test(&self) -> (u64, u64) {
        (
            self.inner.metrics.selected_series_compactions_triggered.get() as u64,
            self.inner.metrics.selected_series_compactions_failed.get() as u64,
        )
    }

    /// Truncates Head memory and WAL older than `mint`.
    pub fn truncate(&self, mint: i64) -> Result<(), TsdbError> {
        self.inner.truncate(mint)
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Configures `PostingsForMatchersCache` `force` / `shared` eligibility flags on Head and all
    /// current & future reloaded Blocks (matching Go `headPostingsForMatchersCacheFactory` /
    /// `blockPostingsForMatchersCacheFactory`).
    pub fn set_postings_cache_eligibility(
        &self,
        head_force: bool,
        head_shared: bool,
        block_force: bool,
        block_shared: bool,
    ) {
        self.inner
            .head
            .set_postings_cache_eligibility(head_force, head_shared);
        self.inner
            .block_postings_cache_force
            .store(block_force, Ordering::Release);
        self.inner
            .block_postings_cache_shared
            .store(block_shared, Ordering::Release);
        for b in self.inner.blocks.read().iter() {
            b.set_postings_cache_eligibility(block_force, block_shared);
        }
    }

    /// Atomically snapshots overlapping ULID blocks (with `BlockReadGuard`s) and Head
    /// (`HeadReadGuard` + `HeadReadIsolation`) under `blocks.read()`, matching Go
    /// `DB.blockChunkQuerierForRange` (`tsdb/db.go:2926-2998`).
    pub fn snapshot_query_sources(&self, mint: i64, maxt: i64) -> QuerySourceSnapshot {
        self.try_snapshot_query_sources(mint, maxt)
            .unwrap_or_else(|_| QuerySourceSnapshot {
                blocks: Vec::new(),
                block_guards: Vec::new(),
                head: None,
            })
    }

    /// Fallible variant of [`Self::snapshot_query_sources`] returning `Err(QuerierError::Closed)`
    /// when the TSDB or any overlapping block is closed.
    pub fn try_snapshot_query_sources(
        &self,
        mint: i64,
        maxt: i64,
    ) -> Result<QuerySourceSnapshot, crate::querier::QuerierError> {
        snapshot_query_sources_with_lock(
            Some(&self.inner.head),
            &[],
            Some(&self.inner.blocks),
            Some(&self.inner.last_garbage_collected_mmap_ref),
            Some(&self.inner.closed),
            mint,
            maxt,
            HeadSnapshotMode::Effective,
        )
    }

    /// Atomically snapshots Head (`HeadReadGuard` + `HeadReadIsolation`) under `blocks.read()`.
    pub fn snapshot_head_query_source(&self, mint: i64, maxt: i64) -> Option<HeadQuerySnapshot> {
        let _lock = self.inner.blocks.read();
        if self.inner.closed.load(Ordering::Acquire) {
            return None;
        }
        snapshot_head_query_source(
            &self.inner.head,
            Some(&self.inner.last_garbage_collected_mmap_ref),
            mint,
            maxt,
            HeadSnapshotMode::Effective,
        )
    }

    /// Compacts the half-open time range [mint, maxt) of Head without truncating Head memory or WAL
    /// (Go DB.CompactHeadWithoutTruncation).
    pub fn compact_head_range_without_truncation(
        &self,
        mint: i64,
        maxt: i64,
    ) -> Result<Option<Ulid>, TsdbError> {
        self.inner.compact_head_range_without_truncation(mint, maxt)
    }

    /// Snapshots all loaded blocks (and optionally Head) into dest_dir (Go DB.Snapshot).
    pub fn snapshot(&self, dest_dir: impl AsRef<Path>, with_head: bool) -> Result<(), TsdbError> {
        self.inner.snapshot(dest_dir.as_ref(), with_head)
    }

    /// Rewrites every block that has tombstones, removing deleted samples (Go DB.CleanTombstones).
    pub fn clean_tombstones(&self) -> Result<(), TsdbError> {
        self.inner.clean_tombstones()
    }

    /// Applies runtime configuration updates (Go DB.ApplyConfig).
    pub fn apply_config(&self, cfg: &TsdbRuntimeConfig) -> Result<(), TsdbError> {
        self.inner.apply_config(cfg)
    }

    /// Returns the maximum time among non-OOO, non-stale, non-selected blocks (Go DB.inOrderBlocksMaxTime).
    pub fn in_order_blocks_max_time(&self) -> Option<i64> {
        self.inner
            .blocks
            .read()
            .iter()
            .filter(|b| {
                !b.meta().out_of_order
                    && !b.meta().compaction.from_out_of_order()
                    && !b.meta().compaction.from_stale_series()
                    && !b.meta().compaction.from_selected_series()
            })
            .map(|b| b.max_time())
            .max()
    }

    /// Returns the effective compaction delay configured/generated at DB open.
    pub fn effective_compaction_delay(&self) -> Duration {
        *self.inner.compaction_delay.lock()
    }

    /// Generates a random compaction delay in [0, chunk_range_ms * max_percent / 100] (Go DB.generateCompactionDelay).
    pub fn generate_compaction_delay(&self) -> Duration {
        compute_compaction_delay(
            self.inner.head.options().chunk_range_ms,
            self.inner.extra.compaction_delay_max_percent,
        )
    }

    /// Returns the configured background block reload interval.
    pub fn block_reload_interval(&self) -> Duration {
        self.inner.configured_block_reload_interval
    }

    /// Flushes sealed head chunks to chunks_head (Go DB.ForceHeadMMap).
    pub fn force_head_mmap(&self) -> Result<usize, TsdbError> {
        Ok(self.inner.head.mmap_head_chunks().map_err(HeadError::from)?)
    }

    /// Returns the current retention duration in milliseconds.
    pub fn retention_duration_ms(&self) -> i64 {
        self.inner.retention.read().retention_duration_ms
    }

    /// Updates retention duration in milliseconds (for runtime/test usage).
    pub fn set_retention_duration_ms_for_test(&self, ms: i64) {
        self.inner.retention.write().retention_duration_ms = ms;
        self.inner
            .metrics
            .retention_limit_seconds
            .set((ms.max(0) as f64) / 1000.0);
    }

    /// Updates max_bytes size retention limit (for runtime/test usage).
    pub fn set_max_bytes_for_test(&self, max_bytes: i64) {
        self.inner.retention.write().max_bytes = max_bytes;
        self.inner
            .metrics
            .retention_limit_bytes
            .set((max_bytes.max(0)) as f64);
    }

    /// Computes which block ULIDs would be deleted by BeyondSizeRetention for the given (ulid, size) pairs
    /// (newest-to-oldest order matching Go BeyondSizeRetention).
    pub fn beyond_size_retention_for_test(&self, blocks: &[(Ulid, u64)]) -> HashSet<Ulid> {
        let ret = self.inner.retention.read();
        TsdbInner::beyond_size_retention_sizes(
            &self.inner.dir,
            &self.inner.extra,
            &ret,
            self.inner.head.size(),
            &self.inner.metrics,
            blocks,
        )
    }

    #[doc(hidden)]
    pub fn set_compact_ooo_head_hook_for_test(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self.inner.compact_ooo_head_hook.lock() = hook;
    }

    #[doc(hidden)]
    pub fn set_compactor_override_for_test(&self, ov: Option<Arc<dyn TsdbCompactorOverride>>) {
        *self.inner.compactor_override.lock() = ov;
    }

    #[doc(hidden)]
    pub fn compactor_override(&self) -> Option<Arc<dyn TsdbCompactorOverride>> {
        self.inner.compactor_override.lock().clone()
    }

    #[doc(hidden)]
    pub fn last_garbage_collected_mmap_ref_for_test(&self) -> u64 {
        self.inner
            .last_garbage_collected_mmap_ref
            .load(Ordering::Acquire)
    }

    /// Returns a TsdbQuerier over Head + overlapping Blocks for [mint, maxt].
    pub fn querier(&self, mint: i64, maxt: i64) -> TsdbQuerier<'_> {
        if let Some(ref filter) = self.inner.extra.block_querier_filter {
            let blocks: Vec<Arc<Block>> = self
                .inner
                .blocks
                .read()
                .iter()
                .filter(|b| filter(b))
                .cloned()
                .collect();
            return TsdbQuerier::new(Some(&self.inner.head), blocks, mint, maxt)
                .with_closed(&self.inner.closed)
                .with_last_garbage_collected_mmap_ref(&self.inner.last_garbage_collected_mmap_ref);
        }
        TsdbQuerier::with_blocks_lock_and_closed(
            Some(&self.inner.head),
            &self.inner.blocks,
            &self.inner.closed,
            mint,
            maxt,
        )
        .with_last_garbage_collected_mmap_ref(&self.inner.last_garbage_collected_mmap_ref)
    }

    /// Returns a TsdbChunkQuerier over Head + overlapping Blocks for [mint, maxt].
    pub fn chunk_querier(&self, mint: i64, maxt: i64) -> TsdbChunkQuerier<'_> {
        if let Some(ref filter) = self.inner.extra.block_chunk_querier_filter {
            let blocks: Vec<Arc<Block>> = self
                .inner
                .blocks
                .read()
                .iter()
                .filter(|b| filter(b))
                .cloned()
                .collect();
            return TsdbChunkQuerier::new(Some(&self.inner.head), blocks, mint, maxt)
                .with_closed(&self.inner.closed)
                .with_last_garbage_collected_mmap_ref(&self.inner.last_garbage_collected_mmap_ref);
        }
        TsdbChunkQuerier::with_blocks_lock_and_closed(
            Some(&self.inner.head),
            &self.inner.blocks,
            &self.inner.closed,
            mint,
            maxt,
        )
        .with_last_garbage_collected_mmap_ref(&self.inner.last_garbage_collected_mmap_ref)
    }

    /// Returns an unordered [`TsdbChunkQuerier`] (concatenating chunks across Head and Blocks
    /// without cross-block compaction, matching Prometheus `UnorderedChunkQuerier`).
    pub fn unordered_chunk_querier(&self, mint: i64, maxt: i64) -> TsdbChunkQuerier<'_> {
        TsdbChunkQuerier::with_blocks_lock_unordered_and_closed(
            Some(&self.inner.head),
            &self.inner.blocks,
            &self.inner.closed,
            mint,
            maxt,
        )
        .with_last_garbage_collected_mmap_ref(&self.inner.last_garbage_collected_mmap_ref)
    }

    /// Deletes samples in `[mint, maxt]` matching `matchers` from Head and overlapping Blocks (`DB.Delete`).
    pub fn delete(&self, mint: i64, maxt: i64, matchers: &[LabelMatcher]) -> Result<(), TsdbError> {
        self.inner.head.delete(mint, maxt, matchers)?;
        let blocks = self.inner.blocks.read().clone();
        for b in blocks {
            if b.overlaps_closed_interval(mint, maxt) {
                b.delete(mint, maxt, matchers)?;
            }
        }
        Ok(())
    }

    /// Returns a cost-sampling queryable restricted to single-source non-OOO layouts (`DB.CostSamplingQueryable`).
    pub fn cost_sampling_queryable(&self) -> (CostSamplingQueryable<'_>, bool) {
        (CostSamplingQueryable { db: self }, true)
    }

    /// Returns the Head's [`CircularExemplarStorage`].
    pub fn exemplar_querier(&self) -> &CircularExemplarStorage {
        self.inner.head.exemplars()
    }

    /// Convenience method querying exemplars in `[start, end]`.
    pub fn select_exemplars(
        &self,
        start: i64,
        end: i64,
        matcher_sets: &[&[LabelMatcher]],
    ) -> Vec<ExemplarQueryResult> {
        self.inner.head.exemplars().select(start, end, matcher_sets)
    }

    /// Stops the background reload/mmap loop, serializes with in-flight compactions,
    /// waits for all block readers to close, and flushes & closes Head.
    pub fn close(&self) -> Result<(), TsdbError> {
        self.inner.closed.store(true, Ordering::Release);
        self.stop_background_loop();
        let _compact_guard = self.inner.compact_mtx.lock();
        let blocks = std::mem::take(&mut *self.inner.blocks.write());
        for b in &blocks {
            b.close_and_wait();
        }
        drop(blocks);
        let head_res = self.inner.head.close();
        let lock_res = self.inner.release_lock();
        head_res?;
        lock_res?;
        Ok(())
    }
}

impl Drop for Tsdb {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl TsdbInner {

    fn beyond_size_retention_sizes(
        dir: &Path,
        extra: &TsdbExtraOptions,
        ret: &RetentionSettings,
        head_size: u64,
        metrics: &DbMetrics,
        blocks_newest_first: &[(Ulid, u64)],
    ) -> HashSet<Ulid> {
        let mut deletable = HashSet::new();
        if blocks_newest_first.is_empty() {
            return deletable;
        }
        let mut max_bytes = ret.max_bytes;
        if ret.max_percentage > 0.0 {
            let disk_size = match extra.fs_size_func.as_ref() {
                Some(f) => f(dir),
                None => os_fs_size(dir),
            };
            if disk_size > 0 {
                max_bytes = ((disk_size as f64) * ret.max_percentage / 100.0) as i64;
            }
        }
        if max_bytes <= 0 {
            return deletable;
        }
        let mut blocks_size = i64::try_from(head_size).unwrap_or(i64::MAX);
        for (i, &(_uid, sz)) in blocks_newest_first.iter().enumerate() {
            blocks_size = blocks_size.saturating_add(i64::try_from(sz).unwrap_or(i64::MAX));
            if blocks_size > max_bytes {
                for &(d_uid, _) in &blocks_newest_first[i..] {
                    deletable.insert(d_uid);
                }
                metrics.size_retentions.inc();
                break;
            }
        }
        deletable
    }

    /// Go DefaultBlocksToDelete(db) / deletableBlocks: blocks marked deletable by
    /// compaction plus those beyond time/size retention (BeyondTimeRetention + BeyondSizeRetention).
    fn default_blocks_to_delete(
        dir: &Path,
        extra: &TsdbExtraOptions,
        ret: &RetentionSettings,
        head_size: u64,
        metrics: &DbMetrics,
        blocks: &[Arc<Block>],
    ) -> HashSet<Ulid> {
        let mut deletable = HashSet::new();
        // Sort the blocks by time - newest to oldest (largest to smallest timestamp).
        let mut sorted: Vec<&Arc<Block>> = blocks.iter().collect();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.max_time()));
        for b in &sorted {
            if b.is_deletable() {
                deletable.insert(b.ulid());
            }
        }
        // BeyondTimeRetention.
        let retention = ret.retention_duration_ms;
        if !sorted.is_empty() && retention != 0 {
            let newest = sorted[0].max_time();
            for (i, b) in sorted.iter().enumerate() {
                if i > 0 && (newest as i128) - (b.max_time() as i128) >= (retention as i128) {
                    for d in &sorted[i..] {
                        deletable.insert(d.ulid());
                    }
                    metrics.time_retentions.inc();
                    break;
                }
            }
        }
        // BeyondSizeRetention.
        let size_pairs: Vec<(Ulid, u64)> = sorted.iter().map(|b| (b.ulid(), b.size())).collect();
        deletable.extend(Self::beyond_size_retention_sizes(
            dir,
            extra,
            ret,
            head_size,
            metrics,
            &size_pairs,
        ));
        deletable
    }

    /// Go DB.reloadBlocks: (re)opens the block dirs (re-using already open blocks),
    /// computes the kept blocks and the obsolete / retention-expired block ULIDs, and
    /// drops any references to deletable blocks inside loadable before returning so the
    /// caller can swap out self.blocks and unmap everything prior to delete_blocks.
    #[allow(clippy::too_many_arguments)]
    fn prepare_reload_blocks(
        dir: &Path,
        opts: &TsdbOptions,
        extra: &TsdbExtraOptions,
        ret: &RetentionSettings,
        head_size: u64,
        metrics: &DbMetrics,
        loaded: &[Arc<Block>],
    ) -> Result<(Vec<Arc<Block>>, HashSet<Ulid>), TsdbError> {
        let (loadable, mut corrupted) = Self::open_blocks(
            dir,
            loaded,
            &opts.block_postings_cache,
            opts.shared_block_postings_cache.as_ref(),
        )?;

        let default_fn = |blocks: &[Arc<Block>]| {
            Self::default_blocks_to_delete(dir, extra, ret, head_size, metrics, blocks)
        };
        let deletable_ulids = match opts.blocks_to_delete.as_ref() {
            Some(cb) => cb(&loadable, &default_fn),
            None => default_fn(&loadable),
        };
        let mut deletable: HashSet<Ulid> = HashSet::new();
        // Mark all parents of loaded blocks as deletable (no matter if they exist).
        for b in &loadable {
            if deletable_ulids.contains(&b.ulid()) {
                deletable.insert(b.ulid());
            }
            for parent in &b.meta().compaction.parents {
                if corrupted.remove(&parent.ulid).is_some() {
                    tracing::warn!(
                        block = %parent.ulid,
                        "Found corrupted block, but replaced by compacted one so it's safe to delete. This should not happen with atomic deletes."
                    );
                }
                deletable.insert(parent.ulid);
            }
        }
        if !corrupted.is_empty() {
            let mut ids: Vec<_> = corrupted.into_iter().collect();
            ids.sort_by_key(|(id, _)| *id);
            let msg = ids
                .into_iter()
                .map(|(id, e)| format!("corrupted block {id}: {e}"))
                .collect::<Vec<_>>()
                .join("\n");
            return Err(TsdbError::Other(msg));
        }

        let mut to_load = Vec::with_capacity(loadable.len());
        let mut blocks_size = 0u64;
        for b in loadable {
            if deletable.contains(&b.ulid()) {
                continue;
            }
            blocks_size += b.size();
            to_load.push(b);
        }
        metrics.blocks_bytes.set(blocks_size as f64);
        to_load.sort_by_key(|b| b.min_time());
        metrics.blocks_loaded.set(to_load.len() as f64);
        metrics
            .symbol_table_size
            .set(to_load.iter().map(|b| b.symbol_table_size()).sum::<u64>() as f64);

        Ok((to_load, deletable))
    }

    /// Go `openBlocks`: blocks whose `meta.json` cannot be read are skipped; blocks that
    /// fail to open are reported as corrupted.
    #[allow(clippy::type_complexity)]
    fn open_blocks(
        dir: &Path,
        loaded: &[Arc<Block>],
        postings_cache: &PostingsForMatchersCacheConfig,
        shared_postings_cache: Option<&Arc<PostingsForMatchersCache>>,
    ) -> Result<(Vec<Arc<Block>>, HashMap<Ulid, BlockError>), TsdbError> {
        let mut bdirs: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if parse_block_dir_name(name).is_some() && entry.file_type()?.is_dir() {
                bdirs.push(entry.path());
            }
        }
        bdirs.sort();

        let mut to_open: Vec<(Ulid, PathBuf)> = Vec::new();
        let mut blocks: Vec<Arc<Block>> = Vec::with_capacity(bdirs.len());
        for bdir in bdirs {
            let meta = match read_meta_file(&bdir) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        dir = %bdir.display(),
                        err = %e,
                        "Failed to read meta.json for a block during reloadBlocks. Skipping"
                    );
                    continue;
                }
            };
            match loaded.iter().find(|b| b.ulid() == meta.ulid && b.meta() == &meta) {
                Some(b) => blocks.push(Arc::clone(b)),
                None => to_open.push((meta.ulid, bdir)),
            }
        }

        let mut corrupted = HashMap::new();
        if to_open.len() <= 1 {
            for (uid, bdir) in to_open {
                let opened = match shared_postings_cache {
                    Some(shared) => Block::open_with_shared_postings_cache(&bdir, Arc::clone(shared)),
                    None => Block::open_with_postings_cache(&bdir, postings_cache.clone()),
                };
                match opened {
                    Ok(b) => blocks.push(Arc::new(b)),
                    Err(e) => {
                        corrupted.insert(uid, e);
                    }
                }
            }
            return Ok((blocks, corrupted));
        }

        let workers = to_open.len().min(
            std::thread::available_parallelism()
                .map_or(4, |n| n.get())
                .clamp(2, 16),
        );
        let next_idx = std::sync::atomic::AtomicUsize::new(0);
        let results: Mutex<Vec<(Ulid, Result<Arc<Block>, BlockError>)>> =
            Mutex::new(Vec::with_capacity(to_open.len()));
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| loop {
                    let idx = next_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some((uid, ref bdir)) = to_open.get(idx) else {
                        break;
                    };
                    let res = match shared_postings_cache {
                        Some(shared) => Block::open_with_shared_postings_cache(bdir, Arc::clone(shared)),
                        None => Block::open_with_postings_cache(bdir, postings_cache.clone()),
                    }
                    .map(Arc::new);
                    results.lock().push((*uid, res));
                });
            }
        });
        for (uid, res) in results.into_inner() {
            match res {
                Ok(b) => blocks.push(b),
                Err(e) => {
                    corrupted.insert(uid, e);
                }
            }
        }
        Ok((blocks, corrupted))
    }

    /// Go `DB.deleteBlocks`: atomically renames each block to `<ulid>.tmp-for-deletion`
    /// before removing it, so a crash never leaves a partial block directory behind.
    fn delete_blocks(dir: &Path, ulids: &HashSet<Ulid>) -> Result<(), TsdbError> {
        let mut ids: Vec<&Ulid> = ulids.iter().collect();
        ids.sort();
        for uid in ids {
            let to_delete = dir.join(uid.to_string());
            match fs::metadata(&to_delete) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(TsdbError::Other(format!(
                        "stat dir {}: {e}",
                        to_delete.display()
                    )))
                }
                Ok(_) => {}
            }
            let tmp_to_delete = dir.join(format!("{uid}{TMP_FOR_DELETION_BLOCK_DIR_SUFFIX}"));
            // fileutil.Replace: remove any stale destination, rename, fsync parent.
            if tmp_to_delete.exists() {
                fs::remove_dir_all(&tmp_to_delete).map_err(|e| {
                    TsdbError::Other(format!("replace of obsolete block for deletion {uid}: {e}"))
                })?;
            }
            fs::rename(&to_delete, &tmp_to_delete)
                .and_then(|()| crate::block::sync_dir(dir))
                .map_err(|e| {
                    TsdbError::Other(format!("replace of obsolete block for deletion {uid}: {e}"))
                })?;
            fs::remove_dir_all(&tmp_to_delete)
                .map_err(|e| TsdbError::Other(format!("delete obsolete block {uid}: {e}")))?;
            tracing::info!(block = %uid, "Deleting obsolete block");
        }
        Ok(())
    }

    /// Go `DB.reloadBlocks` under `cmtx`: skips `blocks.write()` when nothing changed,
    /// otherwise swaps in `to_load`, drops `blocks.write()` immediately, waits for active
    /// `BlockReadGuard`s on `deletable` blocks outside `blocks.write()`, drops `old_blocks`
    /// so every obsolete mmap is unmapped, and only then unlinks `deletable` directories.
    fn reload_blocks_locked(&self) -> Result<(), TsdbError> {
        self.reload_blocks_with_hook_locked(|| {})
    }

    fn reload_blocks_with_hook_locked(
        &self,
        mut on_blocks_visible: impl FnMut(),
    ) -> Result<(), TsdbError> {
        let res = (|| -> Result<(), TsdbError> {
            let (to_load, deletable, unchanged) = {
                let current = self.blocks.read().clone();
                let ret_guard = self.retention.read();
                let ret_snap = RetentionSettings {
                    retention_duration_ms: ret_guard.retention_duration_ms,
                    max_bytes: ret_guard.max_bytes,
                    max_percentage: ret_guard.max_percentage,
                };
                drop(ret_guard);
                let (to_load, deletable) = Self::prepare_reload_blocks(
                    &self.dir,
                    &self.opts,
                    &self.extra,
                    &ret_snap,
                    self.head.size(),
                    &self.metrics,
                    &current,
                )?;
                let bf = self.block_postings_cache_force.load(Ordering::Acquire);
                let bs = self.block_postings_cache_shared.load(Ordering::Acquire);
                if bf || bs {
                    for b in &to_load {
                        b.set_postings_cache_eligibility(bf, bs);
                    }
                }
                let unchanged = deletable.is_empty()
                    && current.len() == to_load.len()
                    && current.iter().zip(&to_load).all(|(a, b)| Arc::ptr_eq(a, b));
                (to_load, deletable, unchanged)
            };
            if unchanged {
                let _guard = self.blocks.write();
                on_blocks_visible();
                return Ok(());
            }
            let old_blocks = {
                let mut guard = self.blocks.write();
                let prev = std::mem::replace(&mut *guard, to_load);
                on_blocks_visible();
                prev
            };
            if let Some(ref cb) = self.opts.on_blocks_reload {
                let get_blocks = || self.blocks.read().clone();
                cb(&get_blocks);
            }
            for b in &old_blocks {
                if deletable.contains(&b.ulid()) {
                    b.close_and_wait();
                }
            }
            drop(old_blocks);
            Self::delete_blocks(&self.dir, &deletable)?;
            Ok(())
        })();
        if res.is_err() {
            self.metrics.reloads_failed.inc();
        }
        self.metrics.reloads.inc();
        res
    }

    /// Reloads on-disk blocks, applying the block deletion filter (Go `DB.reloadBlocks`).
    fn reload_blocks(&self) -> Result<(), TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(TsdbError::Closed);
        }
        self.reload_blocks_locked()
    }

    fn release_lock(&self) -> Result<(), TsdbError> {
        if let Some(mut locker) = self.dir_lock.lock().take() {
            locker.release()?;
        }
        Ok(())
    }

    /// Go `LeveledCompactor.write` bookkeeping: every block write attempt counts as a
    /// compaction run (`prometheus_tsdb_compactions_total`) and observes its duration.
    fn timed_write<T>(&self, f: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let res = f();
        self.metrics.compactions_total.inc();
        self.metrics
            .compaction_duration
            .observe(start.elapsed().as_secs_f64());
        res
    }

    /// Accounts for empty aligned windows skipped via `next_interval_action` so
    /// `prometheus_tsdb_compactions_total` and `prometheus_tsdb_compaction_duration_seconds`
    /// match Go's per-window `c.write(...)` invocation loop.
    fn record_skipped_empty_windows(&self, skipped: i128) {
        if skipped <= 0 {
            return;
        }
        let n = skipped as u64;
        self.metrics.compactions_total.inc_by(n as f64);
        for _ in 0..n.min(10_000) {
            self.metrics.compaction_duration.observe(0.0);
        }
    }

    /// Reloads blocks after writing `uids`; on failure removes them (Go `compactHead`).
    fn reload_after_write(&self, uids: &[Ulid]) -> Result<(), TsdbError> {
        self.reload_after_write_with_hook(uids, || {})
    }

    fn reload_after_write_with_hook(
        &self,
        uids: &[Ulid],
        on_blocks_visible: impl FnMut(),
    ) -> Result<(), TsdbError> {
        if let Err(e) = self.reload_blocks_with_hook_locked(on_blocks_visible) {
            let mut msg = format!("reloadBlocks blocks: {e}");
            for uid in uids {
                if let Err(re) = fs::remove_dir_all(self.dir.join(uid.to_string())) {
                    msg.push_str(&format!(
                        "\ndelete persisted head block after failed db reloadBlocks:{uid}: {re}"
                    ));
                }
            }
            return Err(TsdbError::Other(msg));
        }
        Ok(())
    }

    fn blocks(&self) -> Vec<Arc<Block>> {
        self.blocks.read().clone()
    }

    fn block_ulids(&self) -> Vec<Ulid> {
        let mut ids: Vec<Ulid> = self.blocks.read().iter().map(|b| b.ulid()).collect();
        ids.sort();
        ids
    }

    fn waiting_for_compaction_delay(&self) -> bool {
        let delay = *self.compaction_delay.lock();
        if delay.is_zero() {
            return false;
        }
        let guard = self.time_when_compaction_delay_started.lock();
        guard.is_some_and(|t0| t0.elapsed() < delay)
    }

    fn auto_compact_step(&self) -> Result<Vec<Ulid>, TsdbError> {
        self.compact()
    }

    /// Go `DB.Compact`: compacts every ready `chunk_range` window of the Head into blocks,
    /// truncates the WAL (also when a later step fails, via the deferred truncation), then
    /// compacts the OOO head. Errors count `prometheus_tsdb_compactions_failed_total`.
    fn compact(&self) -> Result<Vec<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let mut created = Vec::new();
        let mut last_block_maxt = i64::MIN;

        let res = (|| -> Result<(), TsdbError> {
            loop {
                if self.closed.load(Ordering::Acquire) {
                    break;
                }
                if !self.head.compactable() {
                    *self.time_when_compaction_delay_started.lock() = None;
                    break;
                }
                let delay = *self.compaction_delay.lock();
                if !delay.is_zero() {
                    let mut started = self.time_when_compaction_delay_started.lock();
                    let t0 = *started.get_or_insert_with(Instant::now);
                    if t0.elapsed() < delay {
                        break;
                    }
                }
                let mint = self.head.min_time();
                let maxt = range_for_timestamp(mint, self.head.options().chunk_range_ms);
                if let Some(uid) =
                    self.compact_head_range_locked(mint, maxt, maxt - 1, true)?
                {
                    created.push(uid);
                }
                // Consider only successful compactions for WAL truncation.
                last_block_maxt = maxt;
            }
            if self.closed.load(Ordering::Acquire) {
                return Ok(());
            }
            // Clear some disk space before compacting blocks.
            self.head.truncate_wal(last_block_maxt)?;
            if last_block_maxt != i64::MIN {
                // The head was compacted, so we compact OOO head as well.
                created.extend(self.compact_ooo_head_locked()?);
            }
            self.head.mmap_head_chunks().map_err(HeadError::from)?;
            self.compact_blocks_locked(&mut created)?;
            Ok(())
        })();

        // Deferred `truncateWAL(lastBlockMaxt)` (a no-op if it already ran).
        let defer_res = self.head.truncate_wal(last_block_maxt);
        let res = match (res, defer_res) {
            (Ok(()), Ok(())) => Ok(created),
            (Err(e), Ok(())) => Err(e),
            (Ok(()), Err(d)) => Err(TsdbError::Other(format!(
                "WAL truncation in Compact defer: {d}"
            ))),
            (Err(e), Err(d)) => Err(TsdbError::Other(format!(
                "{e}\nWAL truncation in Compact defer: {d}"
            ))),
        };
        if res.is_err() {
            self.metrics.compactions_failed.inc();
        }
        res
    }

    /// Compacts the half-open time range `[mint, maxt)` of Head into a ULID block on disk,
    /// reloading blocks and truncating Head memory & WAL up to `maxt`.
    fn compact_head_range(&self, mint: i64, maxt: i64) -> Result<Option<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        if maxt <= mint {
            return Ok(None);
        }
        let uid = self.compact_head_range_locked(mint, maxt, maxt - 1, true)?;
        if maxt > mint {
            self.head.truncate_wal(maxt)?;
        }
        Ok(uid)
    }

    /// Compacts the closed time range `[mint, maxt_incl]` of Head into a ULID block on disk,
    /// handling `maxt_incl == i64::MAX` (including `mint == i64::MAX`) without losing `t == i64::MAX`.
    fn compact_head_range_inclusive(
        &self,
        mint: i64,
        maxt_incl: i64,
    ) -> Result<Option<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        if maxt_incl < mint {
            return Ok(None);
        }
        let maxt = maxt_incl.saturating_add(1);
        let uid = self.compact_head_range_locked(mint, maxt, maxt_incl, true)?;
        if maxt > mint {
            self.head.truncate_wal(maxt)?;
        }
        Ok(uid)
    }

    fn compact_head_range_without_truncation(
        &self,
        mint: i64,
        maxt: i64,
    ) -> Result<Option<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(TsdbError::Closed);
        }
        if maxt <= mint {
            return Ok(None);
        }
        self.compact_head_range_locked(mint, maxt, maxt - 1, false)
    }

    fn snapshot(&self, dest_dir: &Path, with_head: bool) -> Result<(), TsdbError> {
        if dest_dir == self.dir {
            return Err(TsdbError::Other(
                "cannot snapshot into base directory".to_string(),
            ));
        }
        if let Some(s) = dest_dir.to_str() {
            if Ulid::from_string(s).is_ok() {
                return Err(TsdbError::Other(
                    "dir must not be a valid ULID".to_string(),
                ));
            }
        }
        let _compact_guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(TsdbError::Closed);
        }
        let blocks = self.blocks.read().clone();
        for b in &blocks {
            tracing::info!(block = %b.ulid(), "Snapshotting block");
            b.snapshot(dest_dir)?;
        }
        if !with_head || !self.head.initialized() || self.head.num_series() == 0 {
            return Ok(());
        }
        let mint = self.head.min_time();
        let maxt_incl = self.head.max_time();
        if mint > maxt_incl {
            return Ok(());
        }
        let maxt = maxt_incl.saturating_add(1);
        let candidates = self.head.collect_compaction_candidate_entries(mint, maxt_incl)?;
        let _ = write_head_block_streaming_entries_with_options(
            dest_dir,
            mint,
            maxt,
            maxt_incl,
            candidates,
            BlockMetaCompaction::default(),
            false,
            self.opts.max_block_chunk_segment_size,
            Some(&self.metrics.compactor),
            |entries, bufs| {
                self.head
                    .load_entries_batch_in_order_chunks_into(entries, mint, maxt_incl, bufs)
                    .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
            },
        )?;
        Ok(())
    }

    fn clean_tombstones(&self) -> Result<(), TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(TsdbError::Closed);
        }
        let start = Instant::now();
        let res = (|| -> Result<(), TsdbError> {
            loop {
                let mut cleaned_any = false;
                let blocks = self.blocks.read().clone();
                let ov = self.compactor_override.lock().clone();
                for pb in &blocks {
                    if pb.num_tombstones() == 0 {
                        continue;
                    }
                    let uids = if let Some(ref custom) = ov {
                        custom.write(
                            &self.dir,
                            pb,
                            pb.min_time(),
                            pb.max_time(),
                            Some(pb.meta()),
                        )?
                    } else {
                        let meta_opt = write_block_from_block_with_base(
                            &self.dir,
                            pb,
                            pb.min_time(),
                            pb.max_time(),
                            Some(pb.meta()),
                            self.head.float_chunk_encoding(),
                            self.opts.max_block_chunk_segment_size,
                            Some(&self.metrics.compactor),
                        )?;
                        meta_opt.into_iter().map(|m| m.ulid).collect()
                    };
                    pb.set_deletable();
                    cleaned_any = true;
                    if let Err(e) = self.reload_blocks_locked() {
                        for uid in &uids {
                            let _ = fs::remove_dir_all(self.dir.join(uid.to_string()));
                        }
                        return Err(e);
                    }
                    break;
                }
                if !cleaned_any {
                    break;
                }
            }
            Ok(())
        })();
        self.metrics
            .tombstone_cleanup_duration
            .observe(start.elapsed().as_secs_f64());
        res
    }

    fn apply_config(&self, cfg: &TsdbRuntimeConfig) -> Result<(), TsdbError> {
        let startup_enc = self.opts.head.float_chunk_encoding;
        let effective_encoding = if cfg.clear_tsdb_config {
            startup_enc
        } else {
            match cfg.float_chunk_encoding {
                None | Some(Encoding::None) => startup_enc,
                Some(Encoding::Xor) => Encoding::Xor,
                Some(Encoding::Xor2) => Encoding::Xor2,
                Some(other) => {
                    return Err(TsdbError::Other(format!(
                        "unsupported float chunk encoding {other:?}; valid values are \"xor\" and \"xor2\""
                    )));
                }
            }
        };
        if self.opts.head.enable_st_storage && effective_encoding == Encoding::Xor {
            return Err(TsdbError::Other(
                "float chunk encoding \"xor\" is incompatible with start-timestamp storage; XOR chunks do not store start timestamps, use \"xor2\""
                    .to_string(),
            ));
        }

        if cfg.retention_duration_ms.is_some()
            || cfg.max_bytes.is_some()
            || cfg.max_percentage.is_some()
        {
            let mut ret = self.retention.write();
            if let Some(ms) = cfg.retention_duration_ms {
                ret.retention_duration_ms = ms;
                self.metrics
                    .retention_limit_seconds
                    .set((ms.max(0) as f64) / 1000.0);
            }
            if let Some(mb) = cfg.max_bytes {
                ret.max_bytes = mb;
                self.metrics.retention_limit_bytes.set((mb.max(0)) as f64);
            }
            if let Some(mp) = cfg.max_percentage {
                ret.max_percentage = mp;
                self.metrics.retention_limit_percentage.set(mp.max(0.0));
            }
        }

        self.head.set_float_chunk_encoding(effective_encoding);
        let ooo_ms = if cfg.clear_tsdb_config {
            0
        } else {
            cfg.ooo_time_window_ms.unwrap_or(0).max(0)
        };
        self.head.set_ooo_time_window(ooo_ms)?;
        if let Some(max_ex) = cfg.max_exemplars {
            self.head.set_max_exemplars(max_ex);
        }
        Ok(())
    }

    /// Compacts the entire active Head [head.min_time(), head.max_time() + 1) into a ULID block.
    fn compact_head_all(&self) -> Result<Option<Ulid>, TsdbError> {
        if !self.head.initialized() || self.head.num_series() == 0 {
            return Ok(None);
        }
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mint = self.head.min_time();
        let head_maxt = self.head.max_time();
        let maxt = head_maxt.saturating_add(1);
        let uid = self.compact_head_range_locked(mint, maxt, head_maxt, true)?;
        if maxt > mint {
            self.head.truncate_wal(maxt)?;
        }
        Ok(uid)
    }

    /// Go `DB.compactHead`.
    fn compact_head_range_locked(
        &self,
        mint: i64,
        maxt: i64,
        maxt_inclusive: i64,
        truncate_memory: bool,
    ) -> Result<Option<Ulid>, TsdbError> {
        if maxt < mint || maxt_inclusive < mint {
            return Ok(None);
        }
        let candidates = self.head.collect_compaction_candidate_entries(mint, maxt_inclusive)?;
        let meta_opt = self.timed_write(|| {
            write_head_block_streaming_entries_with_options(
                &self.dir,
                mint,
                maxt,
                maxt_inclusive,
                candidates,
                BlockMetaCompaction::default(),
                false,
                self.opts.max_block_chunk_segment_size,
                Some(&self.metrics.compactor),
                |entries, bufs| {
                    self.head
                        .load_entries_batch_in_order_chunks_into(entries, mint, maxt_inclusive, bufs)
                        .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
                },
            )
        })?;

        let uids: Vec<Ulid> = meta_opt.iter().map(|m| m.ulid).collect();
        struct MemTruncGuard<'a>(&'a Head);
        impl Drop for MemTruncGuard<'_> {
            fn drop(&mut self) {
                self.0.end_memory_truncation();
            }
        }
        let _mem_trunc_guard = if truncate_memory {
            let g = MemTruncGuard(&self.head);
            self.reload_after_write_with_hook(&uids, || {
                self.head.begin_memory_truncation(maxt);
            })?;
            Some(g)
        } else {
            self.reload_after_write(&uids)?;
            None
        };
        if truncate_memory {
            self.head.truncate_memory(maxt)?;
        }
        Ok(uids.first().copied())
    }

    /// Compacts all out-of-order samples in Head into OOO ULID blocks (one per block range)
    /// and clears the compacted OOO state.
    fn compact_ooo_head(&self) -> Result<Vec<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(TsdbError::Closed);
        }
        self.compact_ooo_head_locked()
    }

    /// Go `DB.compactOOOHead` + `DB.compactOOO`: writes one block per aligned
    /// `chunk_range` window between the OOO head's min and max time (and, with
    /// `enable_bigger_ooo_block_for_old_samples`, aligned 24h blocks for the days before the
    /// in-order head's max time).
    fn compact_ooo_head_locked(&self) -> Result<Vec<Ulid>, TsdbError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        if !self.head.ooo_was_enabled() {
            return Ok(Vec::new());
        }
        let snap = self.head.collect_ooo_compaction_candidates_with_watermark()?;
        if let Some(hook) = self.compact_ooo_head_hook.lock().take() {
            hook();
        }
        if let Some(ref ov) = *self.compactor_override.lock() {
            return ov.compact_ooo(&self.dir);
        }
        if snap.candidates.is_empty() || snap.min_time > snap.max_time {
            if snap.last_mmap_ref.as_u64() > 0 {
                let _guard = self.blocks.write();
                self.last_garbage_collected_mmap_ref
                    .fetch_max(snap.last_mmap_ref.as_u64(), Ordering::Release);
            }
            self.head
                .truncate_ooo(snap.last_wbl_file, snap.last_mmap_ref)?;
            return Ok(Vec::new());
        }
        let ooo_mint = snap.min_time;
        let ooo_maxt = snap.max_time;
        let max_mmap_ref = snap.last_mmap_ref;
        tracing::info!("out-of-order compaction started");
        let start = Instant::now();
        let block_size = self.head.options().chunk_range_ms;
        let mut ulids: Vec<Ulid> = Vec::new();

        let ooo_intervals = self
            .head
            .collect_ooo_compaction_intervals(&snap.candidates, max_mmap_ref, block_size);
        let res = (|| -> Result<(), TsdbError> {
            let mut run = |mint: i64, maxt: i64, maxt_inclusive: i64| -> Result<(), TsdbError> {
                let mut compaction = BlockMetaCompaction::default();
                compaction.set_out_of_order();
                compaction.parents.push(BlockDesc {
                    ulid: Ulid::nil(),
                    min_time: 0,
                    max_time: 0,
                });
                let meta = self.timed_write(|| {
                    write_head_block_streaming_with_options(
                        &self.dir,
                        mint,
                        maxt,
                        maxt_inclusive,
                        snap.candidates.clone(),
                        compaction,
                        false,
                        self.opts.max_block_chunk_segment_size,
                        Some(&self.metrics.compactor),
                        |ref_id, chks, tombs| {
                            self.head
                                .load_series_ooo_chunks_into_up_to(
                                    ref_id,
                                    max_mmap_ref,
                                    chks,
                                    tombs,
                                )
                                .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
                        },
                    )
                })?;
                if let Some(m) = meta {
                    ulids.push(m.ulid);
                }
                Ok(())
            };

            let mut ooo_start = ooo_mint;
            if self.opts.enable_bigger_ooo_block_for_old_samples {
                let day128 = DAY_MS as i128;
                let maxt_for_24h_block = floor_align_i128(self.head.max_time(), DAY_MS);
                // 24h blocks for data that is for the previous days.
                let mut t = floor_align_i128(ooo_mint, DAY_MS);
                while t < maxt_for_24h_block {
                    let mint = t.max(i64::MIN as i128) as i64;
                    let maxt = (t + day128).min(i64::MAX as i128) as i64;
                    let maxt_inclusive = if t + day128 > i64::MAX as i128 {
                        i64::MAX
                    } else {
                        maxt.saturating_sub(1)
                    };
                    match next_interval_action(&ooo_intervals, mint, maxt_inclusive) {
                        IntervalAction::Done => {
                            self.record_skipped_empty_windows((maxt_for_24h_block - t) / day128);
                            break;
                        }
                        IntervalAction::SkipTo(next_mint) => {
                            let next_t = floor_align_i128(next_mint, DAY_MS);
                            self.record_skipped_empty_windows(
                                (next_t.min(maxt_for_24h_block) - t) / day128,
                            );
                            t = next_t;
                            continue;
                        }
                        IntervalAction::Overlaps => {}
                    }
                    run(mint, maxt, maxt_inclusive)?;
                    t += day128;
                }
                if (ooo_start as i128) < maxt_for_24h_block {
                    ooo_start = maxt_for_24h_block.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
                }
            }
            let block_size128 = block_size as i128;
            let ooo_end_t = floor_align_i128(ooo_maxt, block_size) + block_size128;
            let mut t = floor_align_i128(ooo_start, block_size);
            while t <= ooo_maxt as i128 {
                let mint = t.max(i64::MIN as i128) as i64;
                let maxt = (t + block_size128).min(i64::MAX as i128) as i64;
                let maxt_inclusive = if t + block_size128 > i64::MAX as i128 {
                    i64::MAX
                } else {
                    maxt.saturating_sub(1)
                };
                match next_interval_action(&ooo_intervals, mint, maxt_inclusive) {
                    IntervalAction::Done => {
                        self.record_skipped_empty_windows((ooo_end_t - t) / block_size128);
                        break;
                    }
                    IntervalAction::SkipTo(next_mint) => {
                        let next_t = floor_align_i128(next_mint, block_size);
                        self.record_skipped_empty_windows(
                            (next_t.min(ooo_end_t) - t) / block_size128,
                        );
                        t = next_t;
                        continue;
                    }
                    IntervalAction::Overlaps => {}
                }
                run(mint, maxt, maxt_inclusive)?;
                t += block_size128;
            }
            Ok(())
        })();
        if let Err(e) = res {
            // Best effort removal of created blocks on any error.
            for uid in &ulids {
                let _ = fs::remove_dir_all(self.dir.join(uid.to_string()));
            }
            return Err(e);
        }
        if ulids.is_empty() {
            tracing::info!(duration = ?start.elapsed(), "compact ooo head resulted in no blocks");
        } else {
            tracing::info!(
                duration = ?start.elapsed(),
                ulids = ?ulids,
                "out-of-order compaction completed"
            );
        }

        if let Err(e) = self.reload_blocks_with_hook_locked(|| {
            if snap.last_mmap_ref.as_u64() > 0 {
                self.last_garbage_collected_mmap_ref
                    .fetch_max(snap.last_mmap_ref.as_u64(), Ordering::Release);
            }
        }) {
            let mut msg = format!("reloadBlocks blocks after failed compact ooo head: {e}");
            for uid in &ulids {
                if let Err(re) = fs::remove_dir_all(self.dir.join(uid.to_string())) {
                    msg.push_str(&format!("\n{re}"));
                }
            }
            return Err(TsdbError::Other(msg));
        }
        self.head
            .truncate_ooo(snap.last_wbl_file, snap.last_mmap_ref)?;
        Ok(ulids)
    }

    /// Go `DB.CompactSelectedSeries` -> `compactHeadViewLocked`: writes the selected series
    /// into one block per `chunk_range` window between the head's aligned min time and
    /// its max time, then evicts them from Head.
    fn compact_blocks_locked(&self, created: &mut Vec<Ulid>) -> Result<(), TsdbError> {
        let ranges = self.block_ranges_ms.read().clone();
        let enable_overlapping = self.enable_overlapping_compaction.load(Ordering::Acquire);
        loop {
            if self.closed.load(Ordering::Acquire)
                || (self.head.compactable() && !self.waiting_for_compaction_delay())
            {
                return Ok(());
            }
            let ov = self.compactor_override.lock().clone();
            if let Some(ref custom) = ov {
                let plan_dirs = custom.plan(&self.dir)?;
                if plan_dirs.is_empty() {
                    break;
                }
                let created_uids = custom.compact(&self.dir, &plan_dirs, &[])?;
                created.extend(created_uids);
                continue;
            }
            let Some(plan_dirs) = plan_block_compactions(&self.dir, &ranges, enable_overlapping)? else {
                break;
            };
            let loaded_blocks = self.blocks.read().clone();
            let mut plan_blocks: Vec<Arc<Block>> = Vec::with_capacity(plan_dirs.len());
            for dir in &plan_dirs {
                let meta = read_meta_file(dir)?;
                if let Some(existing) = loaded_blocks
                    .iter()
                    .find(|b| b.ulid() == meta.ulid && b.meta() == &meta)
                {
                    plan_blocks.push(Arc::clone(existing));
                    continue;
                }
                let blk = match self.opts.shared_block_postings_cache.as_ref() {
                    Some(shared) => Block::open_with_shared_postings_cache(dir, Arc::clone(shared))?,
                    None => Block::open_with_postings_cache(
                        dir,
                        self.opts.block_postings_cache.clone(),
                    )?,
                };
                plan_blocks.push(Arc::new(blk));
            }
            let block_refs: Vec<&Block> = plan_blocks.iter().map(|b| b.as_ref()).collect();
            let compact_res = self.timed_write(|| {
                compact_blocks_with_float_encoding_and_segment_size_and_metrics(
                    &self.dir,
                    &block_refs,
                    self.head.float_chunk_encoding(),
                    self.opts.max_block_chunk_segment_size,
                    Some(&self.metrics.compactor),
                )
            });
            let meta_opt = match compact_res {
                Ok(v) => v,
                Err(err) => {
                    drop(plan_blocks);
                    drop(loaded_blocks);
                    let _ = self.reload_blocks_locked();
                    return Err(TsdbError::Compact(err));
                }
            };
            match meta_opt {
                Some(meta) => {
                    drop(plan_blocks);
                    drop(loaded_blocks);
                    self.reload_after_write(&[meta.ulid])?;
                    created.push(meta.ulid);
                }
                None => {
                    // compact_blocks* already persisted `compaction.deletable = true` on source blocks.
                    drop(plan_blocks);
                    drop(loaded_blocks);
                    self.reload_blocks_locked()?;
                }
            }
        }
        Ok(())
    }

    fn compact_head_view_locked(
        &self,
        refs: &[u64],
        stale_only: bool,
    ) -> Result<Vec<Ulid>, TsdbError> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        let append_id_watermark = if self.head.isolation().is_disabled() {
            u64::MAX
        } else {
            self.head.isolation().committed_append_id()
        };
        let iso_state = (!self.head.isolation().is_disabled())
            .then(|| self.head.isolation().state(i64::MIN, i64::MAX));
        let ((candidates, data_mint, data_maxt), fingerprints) = self
            .head
            .collect_selected_compaction_candidates_with_watermark_and_fingerprints(
                refs,
                append_id_watermark,
            )?;
        if fingerprints.is_empty() {
            return Ok(Vec::new());
        }
        let head_maxt = self.head.max_time();
        let mut created = Vec::new();
        if !candidates.is_empty() && data_mint <= data_maxt && self.head.initialized() {
            let range = self.head.options().chunk_range_ms;
            let intervals = self
                .head
                .collect_selected_compaction_intervals(&candidates, range);
            let range128 = range as i128;
            let end_t = floor_align_i128(data_maxt, range) + range128;
            let mut t = floor_align_i128(data_mint, range);
            let write_res = (|| -> Result<(), TsdbError> {
                while t <= data_maxt as i128 {
                    let mint = t.max(i64::MIN as i128) as i64;
                    let maxt = (t + range128).min(i64::MAX as i128) as i64;
                    let maxt_inclusive = if t + range128 > i64::MAX as i128 {
                        i64::MAX
                    } else {
                        maxt.saturating_sub(1)
                    };
                    match next_interval_action(&intervals, mint, maxt_inclusive) {
                        IntervalAction::Done => {
                            self.record_skipped_empty_windows((end_t - t) / range128);
                            break;
                        }
                        IntervalAction::SkipTo(next_mint) => {
                            let next_t = floor_align_i128(next_mint, range);
                            self.record_skipped_empty_windows(
                                (next_t.min(end_t) - t) / range128,
                            );
                            t = next_t;
                            continue;
                        }
                        IntervalAction::Overlaps => {}
                    }
                    let mut compaction = BlockMetaCompaction::default();
                    if stale_only {
                        compaction.set_stale_series();
                    } else {
                        compaction.set_selected_series();
                    }
                    compaction.parents.push(BlockDesc {
                        ulid: Ulid::nil(),
                        min_time: 0,
                        max_time: 0,
                    });
                    let meta = self.timed_write(|| {
                        write_head_block_streaming_with_options(
                            &self.dir,
                            mint,
                            maxt,
                            maxt_inclusive,
                            candidates.clone(),
                            compaction,
                            false,
                            self.opts.max_block_chunk_segment_size,
                            Some(&self.metrics.compactor),
                            |ref_id, chks, tombs| {
                                self.head
                                    .load_series_in_order_chunks_into_with_tx_isolation(
                                        ref_id,
                                        mint,
                                        maxt_inclusive,
                                        iso_state.as_ref().map(|s| s.info()),
                                        chks,
                                        tombs,
                                    )
                                    .map_err(|e| {
                                        CompactError::Io(io::Error::other(e.to_string()))
                                    })
                            },
                        )
                    })?;
                    t += range128;
                    if let Some(meta) = meta {
                        created.push(meta.ulid);
                    }
                }
                Ok(())
            })();
            drop(iso_state);
            if let Err(e) = write_res {
                for uid in &created {
                    let _ = fs::remove_dir_all(self.dir.join(uid.to_string()));
                }
                return Err(e);
            }
        } else {
            drop(iso_state);
        }
        if let Some(hook) = self.compact_head_view_before_evict_hook.lock().take() {
            hook();
        }
        if !created.is_empty() {
            struct ClearEvictingGuard<'a>(&'a Head);
            impl Drop for ClearEvictingGuard<'_> {
                fn drop(&mut self) {
                    self.0.set_evicting_series(None);
                }
            }
            let fps_arc = Arc::new(fingerprints);
            let evicting = Arc::new(EvictedSeries::new(
                refs.to_vec(),
                head_maxt,
                Some(Arc::clone(&fps_arc)),
            ));
            let _evict_guard = ClearEvictingGuard(&self.head);
            self.reload_after_write_with_hook(&created, || {
                self.head.set_evicting_series(Some(Arc::clone(&evicting)));
            })?;
            if stale_only {
                self.head.evict_stale_series_refs_with_fingerprints_prepublished(
                    refs,
                    head_maxt,
                    &fps_arc,
                )?;
            } else {
                self.head.evict_series_refs_with_fingerprints_prepublished(
                    refs,
                    head_maxt,
                    &fps_arc,
                )?;
            }
        } else if stale_only {
            self.head
                .truncate_stale_series(refs, head_maxt, &fingerprints)?;
        } else {
            self.head
                .evict_series_refs_with_fingerprints(refs, head_maxt, &fingerprints)?;
        }
        Ok(created)
    }

    fn compact_selected_series(&self, refs: &[u64]) -> Result<Vec<Ulid>, TsdbError> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        self.metrics.selected_series_compactions_triggered.inc();
        let start = Instant::now();
        let res = self.compact_head_view_locked(refs, false);
        match &res {
            Ok(_) => self
                .metrics
                .selected_series_compaction_duration
                .observe(start.elapsed().as_secs_f64()),
            Err(_) => self.metrics.selected_series_compactions_failed.inc(),
        }
        res
    }

    fn compact_stale_head(&self) -> Result<Vec<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        self.metrics.stale_series_compactions_triggered.inc();
        let start = Instant::now();
        let refs = self.head.stale_series_refs_no_ooo();
        let res = self.compact_head_view_locked(&refs, true);
        match &res {
            Ok(_) => self
                .metrics
                .stale_series_compaction_duration
                .observe(start.elapsed().as_secs_f64()),
            Err(_) => self.metrics.stale_series_compactions_failed.inc(),
        }
        res
    }

    /// Truncates Head memory and WAL older than `mint`.
    fn truncate(&self, mint: i64) -> Result<(), TsdbError> {
        let _guard = self.compact_mtx.lock();
        if self.closed.load(Ordering::Acquire) {
            return Err(TsdbError::Closed);
        }
        self.head.truncate(mint)?;
        Ok(())
    }
}

#[inline]
fn floor_align_i128(t: i64, range: i64) -> i128 {
    let r = range as i128;
    (t as i128).div_euclid(r) * r
}

enum IntervalAction {
    Overlaps,
    SkipTo(i64),
    Done,
}

#[inline]
fn next_interval_action(
    intervals: &[(i64, i64)],
    mint: i64,
    maxt_inclusive: i64,
) -> IntervalAction {
    let idx = intervals.partition_point(|&(_, iv_max)| iv_max < mint);
    let Some(&(iv_min, _)) = intervals.get(idx) else {
        return IntervalAction::Done;
    };
    if iv_min > maxt_inclusive {
        IntervalAction::SkipTo(iv_min)
    } else {
        IntervalAction::Overlaps
    }
}

/// Read-only TSDB handle (Go tsdb.DBReadOnly) that never modifies db_dir.
pub struct TsdbReadOnly {
    dir: PathBuf,
    sandbox_dir: PathBuf,
    closed: AtomicBool,
    opened_heads: Mutex<Vec<Arc<Head>>>,
}

impl TsdbReadOnly {
    /// Opens dir in read-only mode, creating a temporary sandbox directory inside
    /// sandbox_dir_root (or dir if None).
    pub fn open(dir: impl AsRef<Path>, sandbox_dir_root: Option<&Path>) -> Result<Self, TsdbError> {
        let dir = dir.as_ref().to_path_buf();
        if !dir.exists() {
            return Err(TsdbError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("opening the db dir: {}", dir.display()),
            )));
        }
        let root = match sandbox_dir_root {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => dir.clone(),
        };
        let suffix = Ulid::new();
        let sandbox_dir = root.join(format!("tmp_dbro_sandbox_{suffix}"));
        fs::create_dir_all(&sandbox_dir)?;
        Ok(Self {
            dir,
            sandbox_dir,
            closed: AtomicBool::new(false),
            opened_heads: Mutex::new(Vec::new()),
        })
    }

    pub fn sandbox_dir(&self) -> &Path {
        &self.sandbox_dir
    }

    fn ensure_open(&self) -> Result<(), TsdbError> {
        if self.closed.load(Ordering::Acquire) {
            Err(TsdbError::Closed)
        } else {
            Ok(())
        }
    }

    pub fn blocks(&self) -> Result<Vec<Arc<Block>>, TsdbError> {
        self.ensure_open()?;
        let (mut loadable, mut corrupted) = TsdbInner::open_blocks(
            &self.dir,
            &[],
            &PostingsForMatchersCacheConfig::default(),
            None,
        )?;
        for b in &loadable {
            for p in &b.meta().compaction.parents {
                corrupted.remove(&p.ulid);
            }
        }
        if !corrupted.is_empty() {
            let mut ids: Vec<_> = corrupted.into_iter().collect();
            ids.sort_by_key(|(id, _)| *id);
            let msg = ids
                .into_iter()
                .map(|(id, e)| format!("corrupted block {id}: {e}"))
                .collect::<Vec<_>>()
                .join("\n");
            return Err(TsdbError::Other(msg));
        }
        loadable.sort_by_key(|b| b.min_time());
        Ok(loadable)
    }

    pub fn block(&self, block_id: &str) -> Result<Arc<Block>, TsdbError> {
        self.ensure_open()?;
        let uid = Ulid::from_string(block_id)
            .map_err(|_| TsdbError::Other(format!("invalid block ID {block_id}")))?;
        let bdir = self.dir.join(uid.to_string());
        if !bdir.exists() {
            return Err(TsdbError::Other(format!("invalid block ID {block_id}")));
        }
        Ok(Arc::new(Block::open(&bdir)?))
    }

    pub fn last_block_id(&self) -> Result<String, TsdbError> {
        self.ensure_open()?;
        // Go os.ReadDir sorts entries lexicographically by filename and checks `timestamp > maxT`.
        let mut names: Vec<String> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .collect();
        names.sort();
        let mut max_ts = 0u64;
        let mut last = String::new();
        for name_str in names {
            let Ok(uid) = Ulid::from_string(&name_str) else {
                continue;
            };
            let ts = uid.timestamp_ms();
            if ts > max_ts {
                max_ts = ts;
                last = name_str;
            }
        }
        if last.is_empty() {
            return Err(TsdbError::Other("no blocks found".to_string()));
        }
        Ok(last)
    }

    pub fn flush_wal(&self, dest_dir: impl AsRef<Path>) -> Result<(), TsdbError> {
        self.ensure_open()?;
        let blocks = self.blocks()?;
        let max_block_time = blocks.last().map_or(i64::MIN, |b| b.max_time());
        let head = Head::open_read_only(&self.dir, &self.sandbox_dir, max_block_time, true)?;
        let res = (|| -> Result<(), TsdbError> {
            if !head.initialized() || head.num_series() == 0 {
                return Ok(());
            }
            let mint = head.min_time();
            let maxt_incl = head.max_time();
            let maxt = maxt_incl.saturating_add(1);
            let candidates = head.collect_compaction_candidate_entries(mint, maxt_incl)?;
            let _ = write_head_block_streaming_entries_with_options(
                dest_dir.as_ref(),
                mint,
                maxt,
                maxt_incl,
                candidates,
                BlockMetaCompaction::default(),
                false,
                0,
                None,
                |entries, bufs| {
                    head.load_entries_batch_in_order_chunks_into(entries, mint, maxt_incl, bufs)
                        .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
                },
            )?;
            Ok(())
        })();
        let close_res = head.close();
        res?;
        close_res?;
        Ok(())
    }

    fn load_sources(&self, maxt: i64) -> Result<(Vec<Arc<Block>>, Arc<Head>), TsdbError> {
        self.ensure_open()?;
        let blocks = self.blocks()?;
        let max_block_time = blocks.last().map_or(i64::MIN, |b| b.max_time());
        let replay_wal = max_block_time <= maxt;
        let head = Arc::new(Head::open_read_only(
            &self.dir,
            &self.sandbox_dir,
            max_block_time,
            replay_wal,
        )?);
        self.opened_heads.lock().push(Arc::clone(&head));
        Ok((blocks, head))
    }

    pub fn querier(&self, mint: i64, maxt: i64) -> Result<TsdbReadOnlyQuerier, TsdbError> {
        let (blocks, head) = self.load_sources(maxt)?;
        Ok(TsdbReadOnlyQuerier {
            head,
            blocks,
            mint,
            maxt,
        })
    }

    pub fn chunk_querier(
        &self,
        mint: i64,
        maxt: i64,
    ) -> Result<TsdbReadOnlyChunkQuerier, TsdbError> {
        let (blocks, head) = self.load_sources(maxt)?;
        Ok(TsdbReadOnlyChunkQuerier {
            head,
            blocks,
            mint,
            maxt,
        })
    }

    pub fn close(&self) -> Result<(), TsdbError> {
        let was_closed = self.closed.swap(true, Ordering::AcqRel);
        let heads = std::mem::take(&mut *self.opened_heads.lock());
        for h in heads {
            let _ = h.close();
        }
        let _ = fs::remove_dir_all(&self.sandbox_dir);
        if was_closed {
            return Err(TsdbError::Closed);
        }
        Ok(())
    }
}

impl Drop for TsdbReadOnly {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            let heads = std::mem::take(&mut *self.opened_heads.lock());
            for h in heads {
                let _ = h.close();
            }
            let _ = fs::remove_dir_all(&self.sandbox_dir);
        }
    }
}

pub struct TsdbReadOnlyQuerier {
    head: Arc<Head>,
    blocks: Vec<Arc<Block>>,
    mint: i64,
    maxt: i64,
}

impl TsdbReadOnlyQuerier {
    pub fn select(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<crate::querier::QuerySeries>, crate::querier::QuerierError> {
        TsdbQuerier::new(Some(&self.head), self.blocks.clone(), self.mint, self.maxt)
            .select(sort_series, matchers)
    }

    pub fn label_names(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, crate::querier::QuerierError> {
        TsdbQuerier::new(Some(&self.head), self.blocks.clone(), self.mint, self.maxt)
            .label_names(matchers)
    }

    pub fn label_values(
        &self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, crate::querier::QuerierError> {
        TsdbQuerier::new(Some(&self.head), self.blocks.clone(), self.mint, self.maxt)
            .label_values(name, None, matchers)
    }
}

pub struct TsdbReadOnlyChunkQuerier {
    head: Arc<Head>,
    blocks: Vec<Arc<Block>>,
    mint: i64,
    maxt: i64,
}

impl TsdbReadOnlyChunkQuerier {
    pub fn select(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<crate::querier::QueryChunkSeries>, crate::querier::QuerierError> {
        TsdbChunkQuerier::new(Some(&self.head), self.blocks.clone(), self.mint, self.maxt)
            .select(sort_series, matchers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::SampleValue;
    use crate::exemplar::Exemplar;
    use crate::labels::Labels;
    use crate::matcher::MatchType;
    use crate::search::{FuzzAlg, SearchHints, SearchOrdering};

    #[test]
    fn tsdb_end_to_end_append_query_compact_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.samples_per_chunk = 50;

        {
            let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
            let mut app = db.appender();
            let p1 = [("__name__", "http_requests_total"), ("job", "api")];
            let p2 = [("__name__", "up"), ("job", "db")];

            let mut r1 = None;
            for i in 0..160i64 {
                let id = app.append_float(r1, &p1, 1_000_000 + i * 1000, i as f64).unwrap();
                r1 = Some(id);
            }
            let r2 = app.append_float(None, &p2, 1_000_000, 1.0).unwrap();
            app.append_exemplar(
                Some(r2),
                &p2,
                Exemplar::new(Labels::from_pairs([("trace_id", "abc")]), 1.0, 1_000_000),
            )
            .unwrap();
            app.commit().unwrap();

            // Query before compaction
            let q = db.querier(i64::MIN, i64::MAX);
            let m_all = LabelMatcher::new(MatchType::RegexMatch, "__name__", ".*").unwrap();
            let series = q.select(true, std::slice::from_ref(&m_all)).unwrap();
            assert_eq!(series.len(), 2);
            assert_eq!(series[0].samples.len(), 160);
            assert_eq!(series[1].samples.len(), 1);

            // Search label names/values
            let s_res = q
                .search_label_values(
                    "__name__",
                    &SearchHints {
                        terms: vec!["hrt".into()],
                        case_sensitive: false,
                        fuzz_alg: FuzzAlg::Subsequence,
                        fuzz_threshold: 0,
                        order_by: SearchOrdering::ScoreDesc,
                        limit: 5,
                    },
                    &[],
                )
                .unwrap();
            assert_eq!(s_res.len(), 1);
            assert_eq!(s_res[0].value, "http_requests_total");

            // Compact head into ULID block
            let uid = db.compact_head_all().unwrap().expect("block created");
            assert_eq!(db.block_ulids(), vec![uid]);
            db.close().unwrap();
        }

        // Reopen and query persisted block
        let db = Tsdb::open(dir.path(), opts).unwrap();
        assert_eq!(db.block_ulids().len(), 1);
        let q = db.querier(i64::MIN, i64::MAX);
        let m_req = LabelMatcher::new(MatchType::Equal, "__name__", "http_requests_total").unwrap();
        let series = q.select(true, &[m_req]).unwrap();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].samples.len(), 160);
        assert_eq!(series[0].samples[159].value, SampleValue::Float(159.0));
        db.close().unwrap();
    }

    #[test]
    fn torn_multipage_wal_tail_recovers_on_tsdb_open() {
        use crate::record::{RecordEncoder, RefSample, RefSeries};
        use crate::wlog::{segment_name, CompressionType, WlogWriter, PAGE_SIZE};

        for concurrency in [1usize, 4usize] {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.wal_compression = CompressionType::Snappy;
            opts.head.wal_replay_concurrency = concurrency;

            // 1. Write valid series and samples via Tsdb and close cleanly.
            {
                let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
                let mut app = db.appender();
                for i in 0..10u64 {
                    let name = format!("valid_metric_{i}");
                    app.append_float(None, &[("__name__", &name)], 1_000_000, i as f64)
                        .unwrap();
                }
                app.commit().unwrap();
                db.close().unwrap();
            }

            // 2. Append a large multi-page (>64 KiB) WAL record to a new WAL segment,
            // then truncate the file mid-record to simulate a SIGKILL mid-multi-page write.
            let wal_dir = dir.path().join("wal");
            {
                let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
                let enc = RecordEncoder::new(false);
                let mut big_series = Vec::new();
                let mut big_samples = Vec::new();
                for i in 0..2000u64 {
                    let ref_id = 1000 + i;
                    big_series.push(RefSeries {
                        ref_id,
                        labels: Labels::from_pairs([
                            ("__name__", "torn_series_metric"),
                            ("idx", &format!("{i:05}")),
                            ("pad", "padding_to_force_multipage_wal_record_over_64kb"),
                        ]),
                    });
                    big_samples.push(RefSample {
                        ref_id,
                        st: 0,
                        t: 2_000_000,
                        v: i as f64,
                    });
                }
                let s_rec = enc.series(&big_series, Vec::new());
                let samp_rec = enc.samples(&big_samples, Vec::new());
                assert!(s_rec.len() > PAGE_SIZE * 2);
                w.log(&[&s_rec, &samp_rec]).unwrap();
                w.close().unwrap();
            }

            let seg_idx = crate::wlog::segments_range(&wal_dir).unwrap().unwrap().1;
            let seg_path = segment_name(&wal_dir, seg_idx);
            let orig_len = std::fs::metadata(&seg_path).unwrap().len();
            assert!(orig_len > (PAGE_SIZE * 2) as u64);
            // Truncate into the middle of page 2 (leaving REC_FIRST + REC_MIDDLE without REC_LAST).
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&seg_path)
                .unwrap();
            f.set_len((PAGE_SIZE + 512) as u64).unwrap();
            drop(f);

            // 3. Tsdb::open must repair the torn WAL tail and recover all earlier committed data.
            {
                let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
                let q = db.querier(i64::MIN, i64::MAX);
                let m = LabelMatcher::new(MatchType::RegexMatch, "__name__", "valid_metric_.*")
                    .unwrap();
                let series = q.select(true, &[m]).unwrap();
                assert_eq!(series.len(), 10);

                // Verify new writes still work after repair.
                let mut app = db.appender();
                app.append_float(None, &[("__name__", "post_repair")], 3_000_000, 99.0)
                    .unwrap();
                app.commit().unwrap();
                db.close().unwrap();
            }

            // 4. Subsequent reopen succeeds cleanly with both pre-crash and post-repair data.
            let db = Tsdb::open(dir.path(), opts).unwrap();
            let q = db.querier(i64::MIN, i64::MAX);
            let m_all = LabelMatcher::new(MatchType::RegexMatch, "__name__", ".*").unwrap();
            let series = q.select(true, &[m_all]).unwrap();
            assert_eq!(series.len(), 11);
            db.close().unwrap();
        }
    }

    #[test]
    fn from_selected_series_block_does_not_advance_min_valid_time_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let opts = TsdbOptions::default();

        let ref_s1;
        {
            let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
            let mut app = db.appender();
            let p1 = [("__name__", "kept_series"), ("job", "a")];
            let p2 = [("__name__", "selected_series"), ("job", "b")];
            let r1 = app.append_float(None, &p1, 1_000, 1.0).unwrap();
            app.append_float(Some(r1), &p1, 5_000, 5.0).unwrap();
            ref_s1 = r1;

            let r2 = app.append_float(None, &p2, 1_000, 10.0).unwrap();
            app.append_float(Some(r2), &p2, 10_000, 20.0).unwrap();
            app.commit().unwrap();

            // Compact only r2 -> creates one block with `from-selected-series` hint covering
            // the aligned chunk range [0, 2h) (Go `compactHeadViewLocked`).
            let uids = db.compact_selected_series(&[r2]).unwrap();
            assert_eq!(uids.len(), 1, "selected block created");
            let uid = uids[0];
            let blk = db
                .blocks()
                .into_iter()
                .find(|b| b.ulid() == uid)
                .unwrap();
            assert!(blk.meta().compaction.from_selected_series());
            assert_eq!(blk.min_time(), 0);
            assert_eq!(blk.max_time(), crate::head::DEFAULT_BLOCK_DURATION_MS);
            db.close().unwrap();
        }

        // Reopen: `from-selected-series` block must NOT advance Head's `min_valid_time`.
        let db = Tsdb::open(dir.path(), opts).unwrap();
        assert_eq!(db.head().min_valid_time(), i64::MIN);

        let q = db.querier(i64::MIN, i64::MAX);
        let m1 = LabelMatcher::new(MatchType::Equal, "__name__", "kept_series").unwrap();
        let s1 = q.select(true, &[m1]).unwrap();
        assert_eq!(s1.len(), 1);
        assert_eq!(s1[0].samples.len(), 2);

        // Appending at t=6000 (< 10_001) to kept_series must succeed!
        let mut app = db.appender();
        let _ = ref_s1;
        app.append_float(
            None,
            &[("__name__", "kept_series"), ("job", "a")],
            6_000,
            6.0,
        )
        .unwrap();
        app.commit().unwrap();
        db.close().unwrap();
    }

    #[test]
    fn background_reload_loop_deletes_expired_blocks_without_new_writes_and_mmaps_head() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions {
            retention_duration_ms: 1000,
            ..TsdbOptions::default()
        };
        opts.head.samples_per_chunk = 5;

        // Seed two persisted blocks: [0, 1000) and [2000, 3000) while retention is disabled,
        // so both remain on disk initially.
        {
            let mut seed_opts = opts.clone();
            seed_opts.retention_duration_ms = 0;
            let db = Tsdb::open_with_reload_interval(dir.path(), seed_opts, Duration::ZERO).unwrap();
            let pairs = [("__name__", "retention_bg_metric"), ("job", "test")];
            {
                let mut app = db.appender();
                app.append_float(None, &pairs, 100, 1.0).unwrap();
                app.commit().unwrap();
            }
            let b1 = db.compact_head_range(0, 1000).unwrap().unwrap();
            {
                let mut app = db.appender();
                app.append_float(None, &pairs, 2_000, 2.0).unwrap();
                app.commit().unwrap();
            }
            let b2 = db.compact_head_range(2_000, 3_000).unwrap().unwrap();
            assert_eq!(db.block_ulids().len(), 2);
            assert!(dir.path().join(b1.to_string()).exists());
            assert!(dir.path().join(b2.to_string()).exists());
            db.close().unwrap();
        }

        // Simulate Mimir's BlocksToDelete filter: allow deleting any block returned by default
        // retention (newest - b.max_time() >= 1000 -> b1 is expired since 3000 - 1000 >= 1000).
        // Start with a 20ms background reload interval and do NOT perform any writes or compactions.
        let mut ret_opts = opts.clone();
        ret_opts.blocks_to_delete = Some(Arc::new(|blocks, default_fn| default_fn(blocks)));
        // Initially keep retention disabled? No, enable retention immediately on open, or better:
        // verify dynamic expiry via a custom BlocksToDelete gate that flips after open so the
        // deletion MUST happen on a background tick (not during Tsdb::open)!
        let allow_delete = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let allow_delete_cb = Arc::clone(&allow_delete);
        ret_opts.blocks_to_delete = Some(Arc::new(move |blocks, default_fn| {
            if !allow_delete_cb.load(std::sync::atomic::Ordering::Acquire) {
                return HashSet::new();
            }
            default_fn(blocks)
        }));

        let db = Tsdb::open_with_reload_interval(
            dir.path(),
            ret_opts,
            Duration::from_millis(20),
        )
        .unwrap();
        assert_eq!(db.block_ulids().len(), 2, "both blocks kept initially");

        // Append sealed chunks to Head (> samples_per_chunk=5) without calling compact().
        {
            let pairs = [("__name__", "retention_bg_head"), ("job", "test")];
            let mut app = db.appender();
            let mut r = None;
            for i in 0..12i64 {
                let id = app.append_float(r, &pairs, 4_000 + i * 10, i as f64).unwrap();
                r = Some(id);
            }
            app.commit().unwrap();
        }

        // Flip retention expiry gate without calling compact() or reload_blocks() manually.
        allow_delete.store(true, std::sync::atomic::Ordering::Release);

        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if db.block_ulids().len() == 1
                && db.head().dir().join("chunks_head/000001").exists()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(15));
        }
        let remaining = db.block_ulids();
        assert_eq!(
            remaining.len(),
            1,
            "expired ULID block should be deleted by background reload loop without compactions"
        );
        assert!(
            db.head().dir().join("chunks_head/000001").exists(),
            "sealed head chunks should be mmapped by background loop"
        );
        db.close().unwrap();
    }

    #[test]
    fn open_and_drop_many_tsdbs_has_zero_shutdown_latency() {
        let start = Instant::now();
        for _ in 0..50 {
            let dir = tempfile::tempdir().unwrap();
            let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
            drop(db);
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "dropping Tsdb instances with 60s background reload loop stalled: {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn closed_tsdb_rejects_queries_with_closed_error() {
        use crate::querier::QuerierError;

        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO).unwrap();
        let pairs = [("__name__", "closed_tsdb_metric"), ("job", "test")];
        {
            let mut app = db.appender();
            app.append_float(None, &pairs, 1_000, 1.0).unwrap();
            app.commit().unwrap();
        }
        db.compact_head_range(1_000, 2_000).unwrap().unwrap();
        assert_eq!(db.block_ulids().len(), 1);

        let matchers = [LabelMatcher::new(crate::matcher::MatchType::Equal, "__name__", "closed_tsdb_metric").unwrap()];

        db.close().unwrap();
        assert!(db.is_closed());
        assert!(matches!(db.try_snapshot_query_sources(i64::MIN, i64::MAX), Err(QuerierError::Closed)));

        let q = db.querier(i64::MIN, i64::MAX);
        assert!(matches!(q.select(false, &matchers), Err(QuerierError::Closed)));
        assert!(matches!(q.select_chunks(false, &matchers), Err(QuerierError::Closed)));
        assert!(matches!(q.select_raw_labels(false, &matchers), Err(QuerierError::Closed)));
        assert!(matches!(q.label_names(&[]), Err(QuerierError::Closed)));
        assert!(matches!(q.label_values("__name__", None, &[]), Err(QuerierError::Closed)));
        assert!(matches!(q.prepare_select_chunks(&matchers, false), Err(QuerierError::Closed)));
        assert!(matches!(q.select_chunks_adaptive(false, &matchers, false, 10, 10), Err(QuerierError::Closed)));
        assert!(matches!(q.try_select_chunks_adaptive(false, &matchers, false, 10, 10), Err(QuerierError::Closed)));

        let cq = db.chunk_querier(i64::MIN, i64::MAX);
        assert!(matches!(cq.select(false, &matchers), Err(QuerierError::Closed)));
        let ucq = db.unordered_chunk_querier(i64::MIN, i64::MAX);
        assert!(matches!(ucq.select(false, &matchers), Err(QuerierError::Closed)));
    }

    #[test]
    fn w13_p0_5_negative_timestamp_ooo_and_selected_series_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.chunk_range_ms = 7_200_000;
        opts.head.ooo_time_window_ms = 7_200_000;
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();

        let pairs_sel = [("__name__", "neg_sel"), ("job", "a")];
        let pairs_ooo = [("__name__", "neg_ooo"), ("job", "b")];
        {
            let mut app = db.appender();
            app.append_float(None, &pairs_sel, -1_000, 11.0).unwrap();
            app.append_float(None, &pairs_sel, -500, 22.0).unwrap();
            // Establish in-order high-water mark for neg_ooo, then insert OOO negative samples.
            app.append_float(None, &pairs_ooo, -100, 33.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = db.appender();
            app.append_float(None, &pairs_ooo, -1_000, 44.0).unwrap();
            app.append_float(None, &pairs_ooo, -500, 55.0).unwrap();
            app.commit().unwrap();
        }

        let sel_ref = db.head().get_entry_by_sorted_pairs(&pairs_sel).unwrap().ref_id;
        let sel_ulids = db.compact_selected_series(&[sel_ref]).unwrap();
        assert!(!sel_ulids.is_empty(), "selected negative series must compact into block");

        let ooo_ulids = db.compact_ooo_head().unwrap();
        assert!(!ooo_ulids.is_empty(), "negative OOO head must compact into block");

        let m_sel = LabelMatcher::new(crate::matcher::MatchType::Equal, "__name__", "neg_sel").unwrap();
        let res_sel = db.querier(i64::MIN, i64::MAX).select(true, &[m_sel]).unwrap();
        assert_eq!(res_sel.len(), 1);
        assert_eq!(res_sel[0].samples.len(), 2);

        let m_ooo = LabelMatcher::new(crate::matcher::MatchType::Equal, "__name__", "neg_ooo").unwrap();
        let res_ooo = db.querier(i64::MIN, i64::MAX).select(true, &[m_ooo]).unwrap();
        assert_eq!(res_ooo.len(), 1);
        assert_eq!(res_ooo[0].samples.len(), 3);

        db.close().unwrap();
    }

    #[test]
    fn w16_icw_02_compact_blocks_honors_float_chunk_encoding_xor2() {
        use crate::chunkenc::Encoding;

        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.chunk_range_ms = 7_200_000;
        opts.head.ooo_time_window_ms = 7_200_000;
        opts.head.float_chunk_encoding = Encoding::Xor2;
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        db.set_enable_overlapping_compaction(true);

        let pairs = [("__name__", "xor2_overlap"), ("job", "a")];
        {
            let mut app = db.appender();
            let r = app.append_float_with_st(None, &pairs, 500, 1_000, 1.0).unwrap();
            app.append_float_with_st(Some(r), &pairs, 500, 3_000, 3.0).unwrap();
            app.append_float_with_st(Some(r), &pairs, 500, 7_200_000, 4.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = db.appender();
            // OOO sample in the same [0, 7_200_000) block window as 1_000 and 3_000.
            app.append_float_with_st(None, &pairs, 500, 2_000, 2.0).unwrap();
            app.commit().unwrap();
        }

        let head_uid = db
            .compact_head_range(0, 7_200_000)
            .unwrap()
            .expect("head block created");
        let ooo_ulids = db.compact_ooo_head().unwrap();
        assert_eq!(ooo_ulids.len(), 1);
        assert_eq!(db.block_ulids().len(), 2);
        assert_ne!(head_uid, ooo_ulids[0]);

        let merged_ulids = db.compact_blocks().unwrap();
        assert_eq!(merged_ulids.len(), 1);
        let blocks = db.blocks();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].ulid(), merged_ulids[0]);

        let idx = blocks[0].index();
        let chk_r = blocks[0].chunks();
        let ids = idx.postings("", &[""]).unwrap();
        assert_eq!(ids.len(), 1);
        let (_lbls, chks) = idx.series(ids[0]).unwrap();
        assert!(!chks.is_empty());
        for c in chks {
            let chunk = chk_r.chunk(c.ref_id).unwrap();
            assert_eq!(chunk.encoding(), Encoding::Xor2);
        }

        let m = LabelMatcher::new(MatchType::Equal, "__name__", "xor2_overlap").unwrap();
        let res = db.querier(0, 7_199_999).select(true, &[m]).unwrap();
        assert_eq!(res.len(), 1);
        let got: Vec<(i64, i64, SampleValue)> = res[0]
            .samples
            .iter()
            .map(|s| (s.st, s.t, s.value.clone()))
            .collect();
        // With enable_st_storage = false (default), st is normalized to 0, yet float_chunk_encoding = Xor2
        // must still produce Encoding::Xor2 chunks after overlapping block compaction.
        assert_eq!(
            got,
            vec![
                (0, 1_000, SampleValue::Float(1.0)),
                (0, 2_000, SampleValue::Float(2.0)),
                (0, 3_000, SampleValue::Float(3.0)),
            ]
        );

        db.close().unwrap();
    }

    #[test]
    fn compact_head_range_inclusive_preserves_i64_max_samples() {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO).unwrap();

        let mut app = db.appender();
        app.append_float(None, &[("__name__", "max_ts_metric")], i64::MAX, 123.5)
            .unwrap();
        app.commit().unwrap();

        let uid = db
            .compact_head_range_inclusive(i64::MAX, i64::MAX)
            .unwrap()
            .expect("block must be created for mint == maxt_incl == i64::MAX");
        {
            let blocks = db.blocks();
            let blk = blocks.iter().find(|b| b.ulid() == uid).unwrap();
            assert_eq!(blk.min_time(), i64::MAX);
            assert_eq!(blk.max_time(), i64::MAX);

            let idx = blk.index();
            let chk_r = blk.chunks();
            let ids = idx.postings("__name__", &["max_ts_metric"]).unwrap();
            assert_eq!(ids.len(), 1);
            let (_lbls, chks) = idx.series(ids[0]).unwrap();
            assert_eq!(chks.len(), 1);
            let samples =
                crate::compact::decode_chunk_samples(&chk_r.chunk(chks[0].ref_id).unwrap()).unwrap();
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].t, i64::MAX);
            assert_eq!(samples[0].value, SampleValue::Float(123.5));
        }

        db.close().unwrap();
    }

    #[test]
    fn f5_corrupt_block_compaction_persists_failed_true_and_skips_range_on_retry() {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open_with_reload_interval(
            dir.path(),
            TsdbOptions::default(),
            Duration::ZERO,
        )
        .unwrap();
        let range = 7_200_000i64;
        db.set_block_ranges(vec![range, range * 3]);

        // Create 3 level-1 blocks in the first 3x range [0, 3*range) + 1 trailing block so
        // plan_block_compactions selects the first range, then 3 healthy blocks in the second
        // 3x range [3*range, 6*range) + a trailing block.
        for i in 0..7i64 {
            let t = i * range + 1_000;
            let mut app = db.appender();
            app.append_float(None, &[("__name__", "m_f5"), ("i", &i.to_string())], t, i as f64)
                .unwrap();
            app.commit().unwrap();
            db.compact_head_range(i * range, (i + 1) * range)
                .unwrap()
                .unwrap();
        }

        let blocks = db.blocks();
        assert_eq!(blocks.len(), 7);
        // Corrupt chunks/000001 of the second block (in the first 3x range [0, 3*range))
        // by truncating it to its 8-byte header so Block::open still succeeds while reading
        // chunk bytes fails during compaction.
        let corrupt_dir = blocks[1].dir().to_path_buf();
        let chunk_seg = corrupt_dir.join("chunks").join("000001");
        let f = std::fs::OpenOptions::new().write(true).open(&chunk_seg).unwrap();
        f.set_len(8).unwrap();
        drop(f);
        drop(blocks);

        // First compact_blocks() selects [0, 3*range), fails on the corrupted chunk,
        // marks those 3 blocks failed = true in meta.json & in memory, and returns Err.
        let err = db.compact_blocks().unwrap_err();
        let _ = err;

        for b in &db.blocks()[..3] {
            assert!(b.meta().compaction.failed, "in-memory block must reflect failed=true");
            let on_disk = crate::block::read_meta_file(b.dir()).unwrap();
            assert!(on_disk.compaction.failed, "on-disk meta.json must persist failed=true");
        }

        // Next compact_blocks() skips the failed [0, 3*range) range and succeeds on
        // the healthy [3*range, 6*range) range!
        let created = db.compact_blocks().unwrap();
        assert_eq!(created.len(), 1, "healthy range must compact after broken range marked failed");

        db.close().unwrap();
    }

    #[test]
    fn a1_1_compact_blocks_locked_reloads_when_on_disk_meta_changed_no_tombstone_resurrection() {
        use crate::tombstones::{write_tombstones_file, Interval, MemTombstones};

        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO)
            .unwrap();
        let range = 7_200_000i64;
        db.set_block_ranges(vec![range]);

        // Create a block in [0, range) with 2 series, plus a trailing block so plan_block_compactions
        // leaves block[0] eligible for tombstone compaction (>5% tombstones) without needing reload_blocks.
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "keep_me")], 1_000, 1.0).unwrap();
        app.append_float(None, &[("__name__", "delete_me")], 1_000, 99.0).unwrap();
        app.append_float(None, &[("__name__", "trailing")], range + 1_000, 2.0).unwrap();
        app.commit().unwrap();
        let uid0 = db.compact_head_range(0, range).unwrap().unwrap();
        let _uid1 = db.compact_head_range(range, range * 2).unwrap().unwrap();

        let bdir0 = dir.path().join(uid0.to_string());
        let blk0 = db.blocks().into_iter().find(|b| b.ulid() == uid0).unwrap();
        let ref_del = blk0
            .postings_for_matchers(&[LabelMatcher::new(MatchType::Equal, "__name__", "delete_me").unwrap()])
            .unwrap()[0];
        drop(blk0);

        // Write on-disk tombstones + updated meta.json (numTombstones = 1) WITHOUT calling reload_blocks!
        let tombs = MemTombstones::new();
        tombs.add_interval(ref_del, Interval::new(i64::MIN, i64::MAX));
        write_tombstones_file(&bdir0, &tombs).unwrap();
        let mut m0 = read_meta_file(&bdir0).unwrap();
        m0.stats.num_tombstones = 1;
        crate::block::write_meta_file(&bdir0, &mut m0).unwrap();

        let created = db.compact_blocks().unwrap();
        assert_eq!(created.len(), 1);

        let q = db.querier(i64::MIN, i64::MAX);
        let res_del = q
            .select(false, &[LabelMatcher::new(MatchType::Equal, "__name__", "delete_me").unwrap()])
            .unwrap();
        assert!(res_del.is_empty(), "on-disk tombstones must not be resurrected by stale in-memory Block");
        let res_keep = q
            .select(false, &[LabelMatcher::new(MatchType::Equal, "__name__", "keep_me").unwrap()])
            .unwrap();
        assert_eq!(res_keep.len(), 1);

        db.close().unwrap();
    }

    #[test]
    fn a1_2_missing_chunks_dir_fails_reload_and_increments_reloads_failed() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let mut opts = TsdbOptions::default();
        opts.head.registry = Some(Arc::clone(&reg));
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();

        let mut app = db.appender();
        app.append_float(None, &[("__name__", "m_chunks_missing")], 1_000, 1.0).unwrap();
        app.commit().unwrap();
        let uid = db.compact_head_range(0, 7_200_000).unwrap().unwrap();
        db.close().unwrap();

        // Remove chunks/ from the persisted block.
        fs::remove_dir_all(dir.path().join(uid.to_string()).join("chunks")).unwrap();

        // Reopening (or reloading) must fail on the corrupted block and count reloads_failures_total.
        let reg2 = Arc::new(prometheus::Registry::new());
        let mut opts2 = TsdbOptions::default();
        opts2.head.registry = Some(Arc::clone(&reg2));
        let err = Tsdb::open_with_reload_interval(dir.path(), opts2, Duration::ZERO)
            .err()
            .expect("missing chunks/ dir must fail TSDB open/reload");
        let _ = err;
        let fails = reg2
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "prometheus_tsdb_reloads_failures_total")
            .unwrap()
            .get_metric()[0]
            .get_counter()
            .get_value();
        assert_eq!(fails, 1.0);
    }

    #[test]
    fn a1_3_db_compact_blocks_respects_max_block_chunk_segment_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.samples_per_chunk = 20;
        opts.max_block_chunk_segment_size = 200;
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        let range = 7_200_000i64;
        db.set_block_ranges(vec![range, range * 3]);

        for i in 0..4i64 {
            let mut app = db.appender();
            for j in 0..60i64 {
                let t = i * range + j * 1_000;
                app.append_float(None, &[("__name__", "seg_db_m"), ("s", "1")], t, j as f64)
                    .unwrap();
            }
            app.commit().unwrap();
            db.compact_head_range(i * range, (i + 1) * range).unwrap().unwrap();
        }

        let created = db.compact_blocks().unwrap();
        assert_eq!(created.len(), 1);
        let seg_count = fs::read_dir(dir.path().join(created[0].to_string()).join("chunks"))
            .unwrap()
            .count();
        assert!(seg_count >= 2, "expected L2 block to split chunk segments at 200 bytes, got {seg_count}");
        db.close().unwrap();
    }

    #[test]
    fn a1_5_validates_st_storage_and_xor_encoding_and_dir_lockfile() {
        fn clean_start_val(reg: &prometheus::Registry) -> f64 {
            reg.gather()
                .into_iter()
                .find(|mf| mf.get_name() == "prometheus_tsdb_clean_start")
                .unwrap()
                .get_metric()[0]
                .get_gauge()
                .get_value()
        }

        // 1. enable_st_storage + Encoding::Xor (including default) must fail.
        {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.enable_st_storage = true;
            let err = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO)
                .err()
                .expect("enable_st_storage + Xor must be rejected");
            assert!(err.to_string().contains("incompatible with start-timestamp storage"));
        }
        // Unsupported float encoding must fail.
        {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.float_chunk_encoding = Encoding::Histogram;
            let err = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO)
                .err()
                .expect("non-XOR float encoding must be rejected");
            assert!(err.to_string().contains("unsupported float chunk encoding"));
        }
        // enable_st_storage + Encoding::Xor2 must succeed.
        {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.enable_st_storage = true;
            opts.head.float_chunk_encoding = Encoding::Xor2;
            let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
            db.close().unwrap();
        }

        // 2. no_lockfile: true -> clean_start = -1.0, no lock file created.
        {
            let dir = tempfile::tempdir().unwrap();
            let reg = Arc::new(prometheus::Registry::new());
            let mut opts = TsdbOptions {
                no_lockfile: true,
                ..TsdbOptions::default()
            };
            opts.head.registry = Some(Arc::clone(&reg));
            let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
            assert!(!dir.path().join("lock").exists());
            assert_eq!(clean_start_val(&reg), -1.0);
            db.close().unwrap();
        }

        // 3. no_lockfile: false -> clean_start = 1.0, lock held, second opener fails, close removes lock.
        {
            let dir = tempfile::tempdir().unwrap();
            let reg1 = Arc::new(prometheus::Registry::new());
            let mut opts1 = TsdbOptions {
                no_lockfile: false,
                ..TsdbOptions::default()
            };
            opts1.head.registry = Some(Arc::clone(&reg1));
            let db1 = Tsdb::open_with_reload_interval(dir.path(), opts1, Duration::ZERO).unwrap();
            assert!(dir.path().join("lock").exists());
            assert_eq!(clean_start_val(&reg1), 1.0);

            let opts2 = TsdbOptions {
                no_lockfile: false,
                ..TsdbOptions::default()
            };
            assert!(
                Tsdb::open_with_reload_interval(dir.path(), opts2, Duration::ZERO).is_err(),
                "concurrent open with lockfile enabled must fail"
            );
            assert!(dir.path().join("lock").exists(), "failed second open must not delete active lock");

            db1.close().unwrap();
            assert!(!dir.path().join("lock").exists(), "close() must unlink lockfile");
        }

        // 4. Stale lockfile from crashed execution -> clean_start = 0.0, lock replaced and cleaned on drop.
        {
            let dir = tempfile::tempdir().unwrap();
            fs::write(dir.path().join("lock"), b"").unwrap();
            let reg = Arc::new(prometheus::Registry::new());
            let mut opts = TsdbOptions {
                no_lockfile: false,
                ..TsdbOptions::default()
            };
            opts.head.registry = Some(Arc::clone(&reg));
            let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
            assert_eq!(clean_start_val(&reg), 0.0);
            assert!(dir.path().join("lock").exists());
            drop(db);
            assert!(!dir.path().join("lock").exists(), "Drop must unlink lockfile");
        }
    }

    #[test]
    fn a1_7a_compact_stops_between_head_windows_when_closed() {
        let dir = tempfile::tempdir().unwrap();
        let hook_target: Arc<std::sync::OnceLock<std::sync::Weak<TsdbInner>>> =
            Arc::new(std::sync::OnceLock::new());
        let hook_target_cb = Arc::clone(&hook_target);
        let mut opts = TsdbOptions::default();
        opts.head.chunk_range_ms = 1_000;
        opts.on_blocks_reload = Some(Arc::new(move |get_blocks| {
            if !get_blocks().is_empty() {
                if let Some(weak) = hook_target_cb.get() {
                    if let Some(inner) = weak.upgrade() {
                        inner.closed.store(true, Ordering::Release);
                    }
                }
            }
        }));
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();
        hook_target.set(Arc::downgrade(&db.inner)).ok().unwrap();

        let mut app = db.appender();
        for i in 0..5i64 {
            app.append_float(None, &[("__name__", "stop_m")], i * 1_000 + 100, i as f64)
                .unwrap();
        }
        app.commit().unwrap();
        assert!(db.head().compactable());

        let created = db.inner.compact().unwrap();
        assert_eq!(
            created.len(),
            1,
            "compact() must stop after first head window once closed is set"
        );
        assert!(db.head().compactable(), "remaining head windows must not have compacted");
        db.inner.closed.store(false, Ordering::Release);
        db.close().unwrap();
    }

    #[test]
    fn a1_10_empty_intermediate_windows_increment_compactions_total() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let mut opts = TsdbOptions::default();
        opts.head.chunk_range_ms = 1_000;
        opts.head.ooo_time_window_ms = 100_000;
        opts.head.registry = Some(Arc::clone(&reg));
        let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();

        let compactions_total = || -> f64 {
            reg.gather()
                .into_iter()
                .find(|mf| mf.get_name() == "prometheus_tsdb_compactions_total")
                .unwrap()
                .get_metric()[0]
                .get_counter()
                .get_value()
        };

        // Selected-series compaction across windows [0, 1000), [1000, 2000), [2000, 3000), [3000, 4000)
        // where [1000, 2000) and [2000, 3000) are empty -> 4 compactions_total.
        let r_sel = {
            let mut app = db.appender();
            let r = app.append_float(None, &[("__name__", "sel_gap")], 100, 1.0).unwrap();
            app.append_float(Some(r), &[("__name__", "sel_gap")], 3_100, 2.0).unwrap();
            app.append_float(None, &[("__name__", "ooo_gap")], 10_000, 1.0).unwrap();
            app.commit().unwrap();
            r
        };
        let created_sel = db.compact_selected_series(&[r_sel]).unwrap();
        assert_eq!(created_sel.len(), 2);
        assert_eq!(compactions_total(), 4.0);

        // OOO compaction across [0, 1000), [1000, 2000), [2000, 3000) where [1000, 2000) is empty -> +3 = 7.0.
        {
            let mut app = db.appender();
            app.append_float(None, &[("__name__", "ooo_gap")], 100, 1.0).unwrap();
            app.append_float(None, &[("__name__", "ooo_gap")], 2_100, 2.0).unwrap();
            app.commit().unwrap();
        }
        let created_ooo = db.compact_ooo_head().unwrap();
        assert_eq!(created_ooo.len(), 2);
        assert_eq!(compactions_total(), 7.0);

        db.close().unwrap();
    }

    #[test]
    fn w35_tsdb2_drop_closes_tsdb_releases_lock_and_registry_has_zero_arc_leak() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = TsdbOptions {
            no_lockfile: false,
            head: HeadOptions {
                chunk_range_ms: 10_000,
                registry: Some(Arc::clone(&reg)),
                ..HeadOptions::default()
            },
            ..TsdbOptions::default()
        };

        let head_weak;
        let blocks_weak;
        {
            let db = Tsdb::open_with_reload_interval(
                dir.path(),
                opts.clone(),
                Duration::from_millis(25),
            )
            .unwrap();
            head_weak = Arc::downgrade(&db.inner.head);
            blocks_weak = Arc::downgrade(&db.inner.blocks);

            let mut app = db.appender();
            app.append_float(None, &[("__name__", "drop_test"), ("job", "a")], 1_000, 42.0)
                .unwrap();
            app.append_float(None, &[("__name__", "drop_test"), ("job", "a")], 15_000, 43.0)
                .unwrap();
            app.commit().unwrap();
            db.compact().unwrap();

            let mut app2 = db.appender();
            app2.append_float(None, &[("__name__", "drop_test_head"), ("job", "b")], 25_000, 99.0)
                .unwrap();
            app2.commit().unwrap();
            assert!(dir.path().join("lock").exists());
            // Intentionally drop db without calling db.close().
        }

        assert!(
            head_weak.upgrade().is_none(),
            "Registry FnGaugeCollector must not leak Arc<Head> after Tsdb drop"
        );
        assert!(
            blocks_weak.upgrade().is_none(),
            "Registry FnGaugeCollector must not leak Arc<RwLock<Vec<Arc<Block>>>> after Tsdb drop"
        );
        assert!(
            !dir.path().join("lock").exists(),
            "<dir>/lock must be released when Tsdb is dropped without explicit close()"
        );

        let lowest_ts = reg
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "prometheus_tsdb_lowest_timestamp")
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value();
        assert_eq!(lowest_ts, i64::MAX as f64);

        // Immediate reopen succeeds and preserves both compacted and WAL-buffered head data.
        let mut opts2 = TsdbOptions::default();
        opts2.head.chunk_range_ms = 10_000;
        let db2 = Tsdb::open_with_reload_interval(dir.path(), opts2, Duration::ZERO).unwrap();
        let q = db2.querier(0, 30_000);
        let res = q
            .select(
                true,
                &[LabelMatcher::new(MatchType::RegexMatch, "__name__", "drop_test.*").unwrap()],
            )
            .unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].samples.len(), 2);
        assert_eq!(res[1].samples.len(), 1);
        assert_eq!(res[1].samples[0].value, SampleValue::Float(99.0));

        // Explicit close() followed by Drop must be idempotent.
        db2.close().unwrap();
        drop(db2);
        assert!(!dir.path().join("lock").exists());
    }

    #[test]
    fn w35_tsdb7_closed_tsdb_rejects_maintenance_and_compaction_ops() {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO)
            .unwrap();
        {
            let mut app = db.appender();
            app.append_float(None, &[("__name__", "m")], 1_000, 1.0).unwrap();
            app.commit().unwrap();
        }
        db.close().unwrap();

        assert!(matches!(db.reload_blocks(), Err(TsdbError::Closed)));
        assert!(matches!(db.compact_blocks(), Err(TsdbError::Closed)));
        assert!(matches!(db.compact_ooo_head(), Err(TsdbError::Closed)));
        assert!(matches!(db.clean_tombstones(), Err(TsdbError::Closed)));
        assert!(matches!(
            db.compact_head_range_without_truncation(0, 10_000),
            Err(TsdbError::Closed)
        ));
        let snap_dir = tempfile::tempdir().unwrap();
        assert!(matches!(db.snapshot(snap_dir.path(), true), Err(TsdbError::Closed)));
        assert!(matches!(db.truncate(5_000), Err(TsdbError::Closed)));
        // compact() intentionally stays Ok(Vec::new()) when closed.
        assert_eq!(db.compact().unwrap(), Vec::<Ulid>::new());
    }

    #[test]
    fn w35_tsdb9_and_tsdb10_open_cleans_chunk_snapshot_tmp_dirs_and_chunks_head_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let tmp_snap = dir.path().join("chunk_snapshot.000005.0000001234.tmp");
        fs::create_dir_all(&tmp_snap).unwrap();
        fs::write(tmp_snap.join("00000000"), b"leftover").unwrap();

        let chunks_dir = dir.path().join("chunks_head");
        fs::create_dir_all(&chunks_dir).unwrap();
        let tmp_chunk = chunks_dir.join(".000007.tmp");
        fs::write(&tmp_chunk, vec![0u8; 4096]).unwrap();

        let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO)
            .unwrap();
        assert!(!tmp_snap.exists(), "stale chunk_snapshot.*.tmp dir must be removed on Tsdb::open");
        assert!(!tmp_chunk.exists(), "stale chunks_head/.NNNNNN.tmp file must be removed on Tsdb::open");
        db.close().unwrap();
    }
}
