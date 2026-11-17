//! Unified Prometheus TSDB (`Head` + on-disk ULID `Block`s + Compaction + Querier).
//!
//! Ported from github.com/prometheus/prometheus/tsdb/db.go and the `LeveledCompactor`
//! metrics of github.com/prometheus/prometheus/tsdb/compact.go.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use prometheus::{Histogram, HistogramOpts, Registry};
use thiserror::Error;
use ulid::Ulid;

use crate::block::{parse_block_dir_name, read_meta_file, Block, BlockError, BlockMetaCompaction};
use crate::compact::{write_head_block_streaming, CompactError};
use crate::exemplar::{CircularExemplarStorage, ExemplarQueryResult};
use crate::head::{range_for_timestamp, Head, HeadAppender, HeadError, HeadOptions};
use crate::matcher::LabelMatcher;
use crate::metrics::{counter, gauge};
use crate::querier::{TsdbChunkQuerier, TsdbQuerier};

/// Go `tmpForDeletionBlockDirSuffix`.
pub const TMP_FOR_DELETION_BLOCK_DIR_SUFFIX: &str = ".tmp-for-deletion";
/// Go `tmpForCreationBlockDirSuffix`.
pub const TMP_FOR_CREATION_BLOCK_DIR_SUFFIX: &str = ".tmp-for-creation";
/// Go `tmpLegacy`.
pub const TMP_LEGACY_SUFFIX: &str = ".tmp";
/// Go `chunkSnapshotPrefix`.
pub const CHUNK_SNAPSHOT_PREFIX: &str = "chunk_snapshot.";

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
    #[error("{0}")]
    Other(String),
}

/// Go `BlocksToDeleteFunc`, given the loadable blocks and Go `DefaultBlocksToDelete(db)`
/// (`Compaction.Deletable` + time retention, counting `prometheus_tsdb_time_retentions_total`),
/// which the callback may or may not invoke (Mimir's `userTSDB.blocksToDelete` filters it).
pub type BlocksToDeleteFn = Arc<
    dyn Fn(&[Arc<Block>], &dyn Fn(&[Arc<Block>]) -> HashSet<Ulid>) -> HashSet<Ulid> + Send + Sync,
>;

#[derive(Clone)]
pub struct TsdbOptions {
    pub head: HeadOptions,
    pub max_block_chunk_segment_size: usize,
    pub no_lockfile: bool,
    /// Go `Options.RetentionDuration` in milliseconds (0 disables time retention).
    pub retention_duration_ms: i64,
    /// Go `Options.BlocksToDelete` (`None` = `DefaultBlocksToDelete`).
    pub blocks_to_delete: Option<BlocksToDeleteFn>,
    /// Go `Options.EnableBiggerOOOBlockForOldSamples`.
    pub enable_bigger_ooo_block_for_old_samples: bool,
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
            enable_bigger_ooo_block_for_old_samples: false,
        }
    }
}

/// Go `dbMetrics` + `CompactorMetrics` (the subset Mimir aggregates, plus
/// `compactions_failed_total`).
struct DbMetrics {
    compactions_total: prometheus::Counter,
    compaction_duration: Histogram,
    compactions_failed: prometheus::Counter,
    reloads: prometheus::Counter,
    reloads_failed: prometheus::Counter,
    time_retentions: prometheus::Counter,
    blocks_bytes: prometheus::Gauge,
    blocks_loaded: prometheus::Gauge,
    symbol_table_size: prometheus::Gauge,
}

impl DbMetrics {
    fn new(reg: Option<&Registry>) -> Self {
        let compaction_duration = Histogram::with_opts(
            HistogramOpts::new(
                "prometheus_tsdb_compaction_duration_seconds",
                "Duration of compaction runs",
            )
            .buckets(prometheus::exponential_buckets(1.0, 2.0, 14).expect("valid buckets")),
        )
        .expect("valid compaction duration histogram");
        if let Some(r) = reg {
            let _ = r.register(Box::new(compaction_duration.clone()));
        }
        Self {
            compactions_total: counter(
                reg,
                "prometheus_tsdb_compactions_total",
                "Total number of compactions that were executed for the partition.",
            ),
            compaction_duration,
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

/// Unified TSDB instance managing an active [`Head`] and immutable on-disk ULID [`Block`]s.
pub struct Tsdb {
    dir: PathBuf,
    opts: TsdbOptions,
    head: Head,
    blocks: RwLock<Vec<Arc<Block>>>,
    compact_mtx: Mutex<()>,
    metrics: DbMetrics,
}

impl Tsdb {
    /// Opens (or creates) a TSDB in `dir`, loading all on-disk ULID blocks and replaying WAL/WBL/chunks_head.
    pub fn open(dir: impl AsRef<Path>, opts: TsdbOptions) -> Result<Self, TsdbError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Go `DB.Open`: remove leftover tmp block dirs (crashed compactions / deletions)
        // and interrupted temporary checkpoints.
        for tmp_dir in [dir.join("wal"), dir.clone()] {
            remove_tmp_dirs(&tmp_dir)?;
            crate::wlog::delete_temp_checkpoints(&tmp_dir).map_err(|e| {
                TsdbError::Other(format!("delete temp checkpoints: {e}"))
            })?;
        }

        let metrics = DbMetrics::new(opts.head.registry.as_deref());

        // Go `DB.Open` -> `db.reload()` -> `reloadBlocks()` (before the head is replayed).
        let blocks = Self::reload_blocks_impl(&dir, &opts, &metrics, &[])?;
        let mut min_valid_time = blocks
            .iter()
            .filter(|b| {
                !b.meta().out_of_order
                    && !b.meta().compaction.from_out_of_order()
                    && !b.meta().compaction.from_stale_series()
                    && !b.meta().compaction.from_selected_series()
            })
            .map(|b| b.max_time())
            .max()
            .unwrap_or(i64::MIN);

        if let Ok(Some(stored_mvt)) = crate::wlog::read_min_valid_time(&dir.join("wal")) {
            min_valid_time = min_valid_time.max(stored_mvt);
        }

        let head = Head::open(&dir, opts.head.clone(), min_valid_time)?;

        Ok(Self {
            dir,
            opts,
            head,
            blocks: RwLock::new(blocks),
            compact_mtx: Mutex::new(()),
            metrics,
        })
    }

    /// Go `DefaultBlocksToDelete(db)` / `deletableBlocks`: blocks marked deletable by
    /// compaction plus those beyond the time retention (`BeyondTimeRetention`).
    fn default_blocks_to_delete(
        opts: &TsdbOptions,
        metrics: &DbMetrics,
        blocks: &[Arc<Block>],
    ) -> HashSet<Ulid> {
        let mut deletable = HashSet::new();
        // Sort the blocks by time - newest to oldest (largest to smallest timestamp).
        let mut sorted: Vec<&Arc<Block>> = blocks.iter().collect();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.max_time()));
        for b in &sorted {
            if b.meta().compaction.deletable {
                deletable.insert(b.ulid());
            }
        }
        // BeyondTimeRetention.
        let retention = opts.retention_duration_ms;
        if !sorted.is_empty() && retention != 0 {
            let newest = sorted[0].max_time();
            for (i, b) in sorted.iter().enumerate() {
                if i > 0 && newest - b.max_time() >= retention {
                    for d in &sorted[i..] {
                        deletable.insert(d.ulid());
                    }
                    metrics.time_retentions.inc();
                    break;
                }
            }
        }
        deletable
    }

    /// Go `DB.reloadBlocks`: (re)opens the block dirs (re-using already open blocks),
    /// deletes obsolete / retention-expired blocks atomically and returns the blocks to
    /// serve, sorted by `min_time`. Counts `reloads_total` / `reloads_failures_total`.
    fn reload_blocks_impl(
        dir: &Path,
        opts: &TsdbOptions,
        metrics: &DbMetrics,
        loaded: &[Arc<Block>],
    ) -> Result<Vec<Arc<Block>>, TsdbError> {
        let res = Self::reload_blocks_inner(dir, opts, metrics, loaded);
        if res.is_err() {
            metrics.reloads_failed.inc();
        }
        metrics.reloads.inc();
        res
    }

    fn reload_blocks_inner(
        dir: &Path,
        opts: &TsdbOptions,
        metrics: &DbMetrics,
        loaded: &[Arc<Block>],
    ) -> Result<Vec<Arc<Block>>, TsdbError> {
        let (loadable, mut corrupted) = Self::open_blocks(dir, loaded)?;

        let default_fn =
            |blocks: &[Arc<Block>]| Self::default_blocks_to_delete(opts, metrics, blocks);
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

        Self::delete_blocks(dir, &deletable)?;
        Ok(to_load)
    }

    /// Go `openBlocks`: blocks whose `meta.json` cannot be read are skipped; blocks that
    /// fail to open are reported as corrupted.
    #[allow(clippy::type_complexity)]
    fn open_blocks(
        dir: &Path,
        loaded: &[Arc<Block>],
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
            match loaded.iter().find(|b| b.ulid() == meta.ulid) {
                Some(b) => blocks.push(Arc::clone(b)),
                None => to_open.push((meta.ulid, bdir)),
            }
        }

        let mut corrupted = HashMap::new();
        if to_open.len() <= 1 {
            for (uid, bdir) in to_open {
                match Block::open(&bdir) {
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
                    let res = Block::open(bdir).map(Arc::new);
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

    /// Go `DB.reloadBlocks` under `cmtx`: swaps in the reloaded block set.
    fn reload_blocks_locked(&self) -> Result<(), TsdbError> {
        let current = self.blocks.read().clone();
        let reloaded = Self::reload_blocks_impl(&self.dir, &self.opts, &self.metrics, &current)?;
        *self.blocks.write() = reloaded;
        Ok(())
    }

    /// Reloads on-disk blocks, applying the block deletion filter (Go `DB.reloadBlocks`).
    pub fn reload_blocks(&self) -> Result<(), TsdbError> {
        let _guard = self.compact_mtx.lock();
        self.reload_blocks_locked()
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

    /// Reloads blocks after writing `uids`; on failure removes them (Go `compactHead`).
    fn reload_after_write(&self, uids: &[Ulid]) -> Result<(), TsdbError> {
        if let Err(e) = self.reload_blocks_locked() {
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

    #[inline]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    #[inline]
    pub fn head(&self) -> &Head {
        &self.head
    }

    pub fn blocks(&self) -> Vec<Arc<Block>> {
        self.blocks.read().clone()
    }

    pub fn block_ulids(&self) -> Vec<Ulid> {
        let mut ids: Vec<Ulid> = self.blocks.read().iter().map(|b| b.ulid()).collect();
        ids.sort();
        ids
    }

    /// Returns a new [`HeadAppender`] for writing samples, histograms, exemplars, and metadata.
    pub fn appender(&self) -> HeadAppender<'_> {
        self.head.appender()
    }

    pub fn appender_with_capacity(&self, series_hint: usize) -> HeadAppender<'_> {
        self.head.appender_with_capacity(series_hint)
    }

    /// Go `DB.Compact`: compacts every ready `chunk_range` window of the Head into blocks,
    /// truncates the WAL (also when a later step fails, via the deferred truncation), then
    /// compacts the OOO head. Errors count `prometheus_tsdb_compactions_failed_total`.
    pub fn compact(&self) -> Result<Vec<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        let mut created = Vec::new();
        let mut last_block_maxt = i64::MIN;

        let res = (|| -> Result<(), TsdbError> {
            while self.head.compactable() {
                let mint = self.head.min_time();
                let maxt = range_for_timestamp(mint, self.head.options().chunk_range_ms);
                if let Some(uid) = self.compact_head_range_locked(mint, maxt, true)? {
                    created.push(uid);
                }
                // Consider only successful compactions for WAL truncation.
                last_block_maxt = maxt;
            }
            // Clear some disk space before compacting blocks.
            self.head.truncate_wal(last_block_maxt)?;
            if last_block_maxt != i64::MIN {
                // The head was compacted, so we compact OOO head as well.
                created.extend(self.compact_ooo_head_locked()?);
            }
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
    pub fn compact_head_range(&self, mint: i64, maxt: i64) -> Result<Option<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        let uid = self.compact_head_range_locked(mint, maxt, true)?;
        if maxt > mint {
            self.head.truncate_wal(maxt)?;
        }
        Ok(uid)
    }

    /// Compacts the entire active Head `[head.min_time(), head.max_time() + 1)` into a ULID block.
    pub fn compact_head_all(&self) -> Result<Option<Ulid>, TsdbError> {
        if !self.head.initialized() || self.head.num_series() == 0 {
            return Ok(None);
        }
        let mint = self.head.min_time();
        let maxt = self.head.max_time().saturating_add(1);
        self.compact_head_range(mint, maxt)
    }

    /// Go `DB.compactHead`.
    fn compact_head_range_locked(
        &self,
        mint: i64,
        maxt: i64,
        truncate_memory: bool,
    ) -> Result<Option<Ulid>, TsdbError> {
        if maxt <= mint {
            return Ok(None);
        }
        let maxt_inclusive = maxt - 1;
        let candidates = self.head.collect_compaction_candidates(mint, maxt_inclusive)?;
        let meta_opt = self.timed_write(|| {
            write_head_block_streaming(
                &self.dir,
                mint,
                maxt,
                candidates,
                BlockMetaCompaction::default(),
                false,
                self.opts.max_block_chunk_segment_size,
                |ref_id, chks, tombs| {
                    self.head
                        .load_series_in_order_chunks_into(ref_id, mint, maxt_inclusive, chks, tombs)
                        .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
                },
            )
        })?;

        let uids: Vec<Ulid> = meta_opt.iter().map(|m| m.ulid).collect();
        self.reload_after_write(&uids)?;
        if truncate_memory {
            self.head.truncate_memory(maxt)?;
        }
        Ok(uids.first().copied())
    }

    /// Compacts all out-of-order samples in Head into OOO ULID blocks (one per block range)
    /// and clears the compacted OOO state.
    pub fn compact_ooo_head(&self) -> Result<Vec<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        self.compact_ooo_head_locked()
    }

    /// Go `DB.compactOOOHead` + `DB.compactOOO`: writes one block per aligned
    /// `chunk_range` window between the OOO head's min and max time (and, with
    /// `enable_bigger_ooo_block_for_old_samples`, aligned 24h blocks for the days before the
    /// in-order head's max time).
    fn compact_ooo_head_locked(&self) -> Result<Vec<Ulid>, TsdbError> {
        let (candidates, ooo_mint, ooo_maxt) = self.head.collect_ooo_compaction_candidates()?;
        if candidates.is_empty() || ooo_mint > ooo_maxt {
            return Ok(Vec::new());
        }
        tracing::info!("out-of-order compaction started");
        let start = Instant::now();
        let block_size = self.head.options().chunk_range_ms;
        let mut ulids: Vec<Ulid> = Vec::new();

        let res = (|| -> Result<(), TsdbError> {
            let mut run = |mint: i64, maxt: i64| -> Result<(), TsdbError> {
                let mut compaction = BlockMetaCompaction::default();
                compaction.set_out_of_order();
                let meta = self.timed_write(|| {
                    write_head_block_streaming(
                        &self.dir,
                        mint,
                        maxt,
                        candidates.clone(),
                        compaction,
                        true,
                        self.opts.max_block_chunk_segment_size,
                        |ref_id, chks, tombs| {
                            self.head
                                .load_series_ooo_chunks_into(ref_id, chks, tombs)
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
                let maxt_for_24h_block = DAY_MS * (self.head.max_time() / DAY_MS);
                // 24h blocks for data that is for the previous days.
                let mut t = DAY_MS * (ooo_mint / DAY_MS);
                while t < maxt_for_24h_block {
                    run(t, t + DAY_MS)?;
                    t += DAY_MS;
                }
                if ooo_start < maxt_for_24h_block {
                    ooo_start = maxt_for_24h_block;
                }
            }
            let mut t = block_size * (ooo_start / block_size);
            while t <= ooo_maxt {
                run(t, t + block_size)?;
                t += block_size;
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

        if let Err(e) = self.reload_blocks_locked() {
            let mut msg = format!("reloadBlocks blocks after failed compact ooo head: {e}");
            for uid in &ulids {
                if let Err(re) = fs::remove_dir_all(self.dir.join(uid.to_string())) {
                    msg.push_str(&format!("\n{re}"));
                }
            }
            return Err(TsdbError::Other(msg));
        }
        self.head.clear_ooo_data_before(ooo_maxt)?;
        Ok(ulids)
    }

    /// Go `DB.CompactSelectedSeries` -> `compactHeadViewLocked`: writes the selected series
    /// into one block per `chunk_range` window between the head's aligned min time and
    /// its max time, then evicts them from Head.
    pub fn compact_selected_series(&self, refs: &[u64]) -> Result<Vec<Ulid>, TsdbError> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        let _guard = self.compact_mtx.lock();
        let (candidates, data_mint, data_maxt) =
            self.head.collect_selected_compaction_candidates(refs)?;
        if candidates.is_empty() || data_mint > data_maxt {
            return Ok(Vec::new());
        }
        let range = self.head.options().chunk_range_ms;
        let head_maxt = self.head.max_time();
        let mut created = Vec::new();
        let mut t = range * (self.head.min_time() / range);
        while t <= head_maxt {
            let mint = t;
            let maxt = t + range;
            let maxt_inclusive = maxt - 1;
            let mut compaction = BlockMetaCompaction::default();
            compaction.set_selected_series();
            let meta = self.timed_write(|| {
                write_head_block_streaming(
                    &self.dir,
                    mint,
                    maxt,
                    candidates.clone(),
                    compaction,
                    false,
                    self.opts.max_block_chunk_segment_size,
                    |ref_id, chks, tombs| {
                        self.head
                            .load_series_in_order_chunks_into(
                                ref_id,
                                mint,
                                maxt_inclusive,
                                chks,
                                tombs,
                            )
                            .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
                    },
                )
            })?;
            t += range;
            // No block when the window holds no data for the selected series.
            let Some(meta) = meta else {
                continue;
            };
            self.reload_after_write(&[meta.ulid])?;
            created.push(meta.ulid);
        }
        self.head.evict_series_refs_before(refs, head_maxt)?;
        Ok(created)
    }

    /// Truncates Head memory and WAL older than `mint`.
    pub fn truncate(&self, mint: i64) -> Result<(), TsdbError> {
        let _guard = self.compact_mtx.lock();
        self.head.truncate(mint)?;
        Ok(())
    }

    /// Returns a [`TsdbQuerier`] over Head + overlapping Blocks for `[mint, maxt]`.
    pub fn querier(&self, mint: i64, maxt: i64) -> TsdbQuerier<'_> {
        TsdbQuerier::with_blocks_lock(Some(&self.head), &self.blocks, mint, maxt)
    }

    /// Returns a [`TsdbChunkQuerier`] over Head + overlapping Blocks for `[mint, maxt]`.
    pub fn chunk_querier(&self, mint: i64, maxt: i64) -> TsdbChunkQuerier<'_> {
        TsdbChunkQuerier::with_blocks_lock(Some(&self.head), &self.blocks, mint, maxt)
    }

    /// Returns an unordered [`TsdbChunkQuerier`] (concatenating chunks across Head and Blocks
    /// without cross-block compaction, matching Prometheus `UnorderedChunkQuerier`).
    pub fn unordered_chunk_querier(&self, mint: i64, maxt: i64) -> TsdbChunkQuerier<'_> {
        TsdbChunkQuerier::with_blocks_lock_unordered(Some(&self.head), &self.blocks, mint, maxt)
    }

    /// Returns the Head's [`CircularExemplarStorage`].
    pub fn exemplar_querier(&self) -> &CircularExemplarStorage {
        self.head.exemplars()
    }

    /// Convenience method querying exemplars in `[start, end]`.
    pub fn select_exemplars(
        &self,
        start: i64,
        end: i64,
        matcher_sets: &[&[LabelMatcher]],
    ) -> Vec<ExemplarQueryResult> {
        self.head.exemplars().select(start, end, matcher_sets)
    }

    /// Flushes sealed chunks to `chunks_head` and closes WAL, WBL, and `HeadChunksWriter`.
    pub fn close(&self) -> Result<(), TsdbError> {
        self.head.close()?;
        Ok(())
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
}
