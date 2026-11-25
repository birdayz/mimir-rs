//! Unified Querier, ChunkQuerier, and Searcher across TSDB Head and on-disk ULID Blocks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/querier.go
//! and github.com/prometheus/prometheus/tsdb/db.go.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

use mimir_common::thread_pool::ScopedWorkerPool;
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;
use thiserror::Error;

use crate::block::{Block, BlockError, BlockReadGuard};
use crate::chunkenc::{ChunkError, EncodedChunk, Encoding};
use crate::chunks_block::{BlockChunksError, ChunkMeta};
use crate::compact::{
    compact_chunk_series_merge, decode_chunk_samples, merge_chunks_with_tombstones,
    merge_chunks_with_tombstones_and_float_encoding,
    merge_head_chunks_with_tombstones_and_float_encoding,
    reencode_chunk_without_tombstones, DecodedSample,
};
use crate::head::{
    Head, HeadChunkSnapshot, HeadError, HeadReadGuard, HeadReadIsolation, SeriesEntry,
};
use crate::head_chunks::ChunkDiskMapperRef;
use crate::isolation::{IsolationState, IsolationStateInfo};
use crate::index::IndexError;
use crate::labels::{decode_stringlabel_size, Labels};
use crate::matcher::LabelMatcher;
use crate::postings::PostingsError;
use crate::search::{apply_search_hints, SearchError, SearchHints, SearchResult};
use crate::tombstones::Interval;

#[derive(Debug, Error)]
pub enum QuerierError {
    #[error("head error: {0}")]
    Head(#[from] HeadError),
    #[error("block error: {0}")]
    Block(#[from] BlockError),
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("block chunks error: {0}")]
    BlockChunks(#[from] BlockChunksError),
    #[error("chunk error: {0}")]
    Chunk(#[from] ChunkError),
    #[error("postings error: {0}")]
    Postings(#[from] PostingsError),
    #[error("search error: {0}")]
    Search(#[from] SearchError),
    #[error("TSDB is closed")]
    Closed,
    #[error("cost sampling unsupported")]
    CostSamplingUnsupported,
    #[error("invalid query shard: index {idx} out of {count}")]
    InvalidShard { idx: u64, count: u64 },
}

/// A queried series yielding decoded samples in `[mint, maxt]`.
#[derive(Clone, Debug, PartialEq)]
pub struct QuerySeries {
    pub labels: Labels,
    pub samples: Vec<DecodedSample>,
}

/// A queried series yielding encoded chunks overlapping `[mint, maxt]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryChunkSeries {
    pub labels: Labels,
    pub chunks: Vec<ChunkMeta>,
}

/// Compact source locator for lazy Phase 2 chunk streaming (`0` = Head, `1 + idx` = `blocks[idx]`).
/// Pre-filtered block chunk reference retained from Pass 1 so Phase 2 loads chunk bytes
/// directly from `BlockChunksReader` without decoding `IndexReader::series` a second time.
#[derive(Clone, Debug)]
pub struct LazyBlockChunkRef {
    pub block_idx: u32,
    pub series_ref: u64,
    pub chunk_ref: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub has_tombstones: bool,
}

/// Phase 2 chunk backing source for a `LazySeriesEntry`.
#[derive(Clone, Debug)]
pub enum LazyChunkSource {
    Head(u64),
    Block { block_idx: u16, ref_id: u64 },
    Multi(Box<[(u16, u64)]>),
    /// Resolved block chunk metas (with optional Head ref) captured during Pass 1.
    Resolved {
        head_ref: Option<u64>,
        block_chks: Box<[LazyBlockChunkRef]>,
    },
    Precomputed(Vec<ChunkMeta>),
}

/// Lightweight Phase 1 series descriptor holding zero chunk bytes on the fast path.
#[derive(Clone, Debug)]
pub struct LazySeriesEntry {
    pub labels: Labels,
    pub head_entry: Option<Arc<SeriesEntry>>,
    pub head_snapshot: Option<HeadChunkSnapshot>,
    pub source: LazyChunkSource,
    pub chunk_count: u32,
}

impl LazySeriesEntry {
    #[inline]
    pub fn entry_labels(&self) -> &Labels {
        match self.head_entry {
            Some(ref entry) => &entry.labels,
            None => &self.labels,
        }
    }
}

/// Incremental event emitted by [`TsdbQuerier::stream_prepared_chunks_lazy`].
pub enum LazyStreamEvent<'a> {
    Series(&'a Labels, u32),
    SeriesEnd,
    SeriesChunks(usize, Vec<ChunkMeta>),
}

/// Pre-evaluated postings selection across TSDB `Head` and overlapping `Block`s.
///
/// `'static + Send + Sync` so callers can evaluate postings inline (hitting caches in microseconds)
/// and decide whether to materialize chunks inline or offload to a blocking worker pool.
/// Atomic Head snapshot captured under `blocks.read()`.
#[derive(Clone, Debug)]
pub struct HeadQuerySnapshot {
    pub guard: HeadReadGuard,
    pub isolation: HeadReadIsolation,
    pub overlaps_in_order: bool,
    pub overlaps_ooo: bool,
    pub min_time: i64,
    pub max_time: i64,
}

/// Atomic snapshot of overlapping ULID blocks and Head state captured under `blocks.read()`.
///
/// Field declaration order ensures `blocks` (`Arc<Block>` mmaps) drop BEFORE `block_guards`
/// wakes `Block::close_and_wait()`.
#[derive(Debug)]
pub struct QuerySourceSnapshot {
    pub blocks: Vec<Arc<Block>>,
    pub block_guards: Vec<BlockReadGuard>,
    pub head: Option<HeadQuerySnapshot>,
}

#[derive(Clone, Copy)]
pub(crate) enum HeadSnapshotMode {
    Effective,
    InOrderOnly,
    FullRange,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn snapshot_head_query_source(
    head: &Head,
    last_gc_mmap_ref: Option<&AtomicU64>,
    mint: i64,
    maxt: i64,
    mode: HeadSnapshotMode,
) -> Option<HeadQuerySnapshot> {
    if head.is_closed() {
        return None;
    }
    let max_ooo_mmap_ref_to_skip = last_gc_mmap_ref
        .map_or(ChunkDiskMapperRef(0), |a| {
            ChunkDiskMapperRef(a.load(AtomicOrdering::Acquire))
        });
    let evicting = head.evicting_series();
    let head_min_time = head.min_time();
    let head_max_time = head.max_time();

    match mode {
        HeadSnapshotMode::FullRange => {
            let guard = head.start_read(i64::MIN, i64::MAX);
            Some(HeadQuerySnapshot {
                guard,
                isolation: HeadReadIsolation {
                    ino_mint: i64::MIN,
                    max_ooo_mmap_ref_to_skip,
                    evicting,
                },
                overlaps_in_order: true,
                overlaps_ooo: true,
                min_time: head_min_time,
                max_time: head_max_time,
            })
        }
        HeadSnapshotMode::InOrderOnly => {
            if !(head.initialized() && head_min_time <= maxt && mint <= head_max_time) {
                return None;
            }
            let mut ino_mint = mint.max(head_min_time);
            let mut guard = head.start_read(mint, maxt);
            let (should_close, get_new, new_mint) =
                head.is_querier_colliding_with_truncation(mint, maxt);
            if should_close {
                ino_mint = ino_mint.max(new_mint);
                if get_new {
                    let new_guard = head.start_read(new_mint, maxt);
                    guard = new_guard;
                } else {
                    drop(guard);
                    return None;
                }
            }
            // Match Go `headIndexReader.LabelValues`/`LabelNames` (`head_read.go:89,103`):
            // `!(h.maxt < h.head.MinTime() || h.mint > h.head.MaxTime())`, allowing inverted
            // ranges (`mint > maxt`) that still overlap `[head_min_time, head_max_time]`.
            let overlaps_in_order = head.min_time() <= maxt && ino_mint <= head.max_time();
            if !overlaps_in_order {
                drop(guard);
                return None;
            }
            Some(HeadQuerySnapshot {
                guard,
                isolation: HeadReadIsolation {
                    ino_mint,
                    max_ooo_mmap_ref_to_skip,
                    evicting,
                },
                overlaps_in_order: true,
                overlaps_ooo: false,
                min_time: head_min_time,
                max_time: head_max_time,
            })
        }
        HeadSnapshotMode::Effective => {
            let min_ooo = head.min_ooo_time();
            let overlaps_ooo =
                min_ooo != i64::MAX && min_ooo <= maxt && mint <= head.max_ooo_time();
            if !(maxt >= head_min_time || overlaps_ooo) {
                return None;
            }
            let mut ino_mint = mint.max(head_min_time);
            let mut guard = head.start_read(mint, maxt);
            let mut collided_past_maxt = false;
            let (should_close, get_new, new_mint) =
                head.is_querier_colliding_with_truncation(mint, maxt);
            if should_close {
                ino_mint = ino_mint.max(new_mint);
                if get_new {
                    if !overlaps_ooo {
                        let new_guard = head.start_read(new_mint, maxt);
                        guard = new_guard;
                    }
                } else {
                    collided_past_maxt = true;
                    if !overlaps_ooo {
                        drop(guard);
                        return None;
                    }
                }
            }
            // Match Go `DB.Querier`/`DB.ChunkQuerier` (`db.go:2868,2951`): in-order Head is
            // active whenever `maxt >= head_min_time` (unless truncation collided past `maxt`),
            // including inverted ranges (`mint > maxt`) whose chunk checks use
            // `overlaps_closed_interval(ino_mint, maxt)`.
            let overlaps_in_order =
                head.initialized() && !collided_past_maxt && maxt >= head_min_time;
            Some(HeadQuerySnapshot {
                guard,
                isolation: HeadReadIsolation {
                    ino_mint,
                    max_ooo_mmap_ref_to_skip,
                    evicting,
                },
                overlaps_in_order,
                overlaps_ooo,
                min_time: head_min_time,
                max_time: head_max_time,
            })
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn snapshot_query_sources_with_lock(
    head: Option<&Head>,
    static_blocks: &[Arc<Block>],
    blocks_lock: Option<&RwLock<Vec<Arc<Block>>>>,
    last_gc_mmap_ref: Option<&AtomicU64>,
    closed: Option<&AtomicBool>,
    mint: i64,
    maxt: i64,
    mode: HeadSnapshotMode,
) -> Result<QuerySourceSnapshot, QuerierError> {
    let lock_guard = blocks_lock.map(|l| l.read());
    if closed.is_some_and(|c| c.load(AtomicOrdering::Acquire))
        || head.is_some_and(Head::is_closed)
    {
        drop(lock_guard);
        return Err(QuerierError::Closed);
    }
    let slice: &[Arc<Block>] = match lock_guard.as_ref() {
        Some(g) => g.as_slice(),
        None => static_blocks,
    };
    let mut blocks = Vec::new();
    let mut block_guards = Vec::new();
    for b in slice {
        if b.overlaps_closed_interval(mint, maxt) {
            let Some(bg) = Block::start_read(b) else {
                drop(lock_guard);
                return Err(QuerierError::Closed);
            };
            blocks.push(Arc::clone(b));
            block_guards.push(bg);
        }
    }
    let head_snap = head.and_then(|h| {
        snapshot_head_query_source(h, last_gc_mmap_ref, mint, maxt, mode)
    });
    drop(lock_guard);
    Ok(QuerySourceSnapshot {
        blocks,
        block_guards,
        head: head_snap,
    })
}

#[derive(Clone, Debug)]
pub struct PreparedChunkSelect {
    pub head_refs: Option<Arc<[u64]>>,
    head_shard_hashes: Option<crate::postings_cache::ShardHashSlot>,
    pub block_refs: Vec<(Arc<Block>, Arc<[u64]>)>,
    block_shard_hashes: Vec<Option<crate::postings_cache::ShardHashSlot>>,
    overlapping_blocks: Vec<Arc<Block>>,
    pub mint: i64,
    pub maxt: i64,
    pub unordered: bool,
    head_overlaps_ooo: bool,
    head_isolation: Option<HeadReadIsolation>,
    tx_iso: Option<IsolationStateInfo>,
    _block_guards: Vec<BlockReadGuard>,
    _head_guard: Option<HeadReadGuard>,
}

impl PreparedChunkSelect {
    #[inline]
    pub fn total_refs(&self) -> usize {
        self.head_refs.as_ref().map_or(0, |r| r.len())
            + self.block_refs.iter().map(|(_, r)| r.len()).sum::<usize>()
    }

    #[inline]
    pub fn max_source_series_len(&self) -> usize {
        let h = self.head_refs.as_ref().map_or(0, |r| r.len());
        let b = self
            .block_refs
            .iter()
            .map(|(_, r)| r.len())
            .max()
            .unwrap_or(0);
        h.max(b)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_refs() == 0
    }
}

/// Postings selected from the index and series returned by one block querier (the Head or one
/// persisted block) for a chunk `Select`: what Mimir's `statsTrackingChunkSeriesSet` reports as
/// `cortex_ingester_queried_series{stage="single_block_index"|"single_block_filter"}`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockSelectStats {
    pub selected_postings: u64,
    pub series: u64,
    /// Whether these are the Head's stats (vs. a persisted block's).
    pub head: bool,
    /// Whether this query range overlaps `Head::min_ooo_time..=Head::max_ooo_time` (Go `overlapsOOO`).
    pub overlaps_ooo: bool,
    /// `BlockMeta::min_time` for persisted blocks (`0` when `head == true`).
    pub block_min_time: i64,
}

/// Restricts which returned series [`BlockSelectStats::series`] counts (e.g. the query shard,
/// applied by the caller after the select).
pub type SeriesStatsFilter = Arc<dyn Fn(&Labels) -> bool + Send + Sync>;

/// Collects one [`BlockSelectStats`] per block querier of every chunk select it is attached to.
pub type BlockSelectStatsSink = Arc<parking_lot::Mutex<Vec<BlockSelectStats>>>;

/// Unified read view over `Head` and overlapping `Block`s for `[mint, maxt]`.
pub struct TsdbQuerier<'a> {
    head: Option<&'a Head>,
    blocks: Vec<Arc<Block>>,
    blocks_lock: Option<&'a RwLock<Vec<Arc<Block>>>>,
    last_garbage_collected_mmap_ref: Option<&'a AtomicU64>,
    closed: Option<&'a AtomicBool>,
    mint: i64,
    maxt: i64,
    concurrent: bool,
    tx_iso: Option<Arc<IsolationState>>,
    select_stats: Option<BlockSelectStatsSink>,
    select_stats_filter: Option<SeriesStatsFilter>,
}

impl<'a> TsdbQuerier<'a> {
    pub fn new(head: Option<&'a Head>, blocks: Vec<Arc<Block>>, mint: i64, maxt: i64) -> Self {
        let head = head.filter(|h| {
            let ooo = h.min_ooo_time() != i64::MAX
                && h.min_ooo_time() <= maxt
                && mint <= h.max_ooo_time();
            h.min_time() <= maxt || ooo
        });
        let tx_iso = head
            .filter(|h| !h.isolation().is_disabled())
            .map(|h| Arc::new(h.isolation().state(mint, maxt)));
        let blocks = blocks
            .into_iter()
            .filter(|b| b.overlaps_closed_interval(mint, maxt))
            .collect();
        Self {
            head,
            blocks,
            blocks_lock: None,
            last_garbage_collected_mmap_ref: None,
            closed: None,
            mint,
            maxt,
            concurrent: false,
            tx_iso,
            select_stats: None,
            select_stats_filter: None,
        }
    }

    pub fn with_blocks_lock(
        head: Option<&'a Head>,
        blocks_lock: &'a RwLock<Vec<Arc<Block>>>,
        mint: i64,
        maxt: i64,
    ) -> Self {
        let tx_iso = head
            .filter(|h| !h.isolation().is_disabled())
            .map(|h| Arc::new(h.isolation().state(mint, maxt)));
        Self {
            head,
            blocks: Vec::new(),
            blocks_lock: Some(blocks_lock),
            last_garbage_collected_mmap_ref: None,
            closed: None,
            mint,
            maxt,
            concurrent: false,
            tx_iso,
            select_stats: None,
            select_stats_filter: None,
        }
    }

    pub fn with_blocks_lock_and_closed(
        head: Option<&'a Head>,
        blocks_lock: &'a RwLock<Vec<Arc<Block>>>,
        closed: &'a AtomicBool,
        mint: i64,
        maxt: i64,
    ) -> Self {
        let tx_iso = head
            .filter(|h| !h.isolation().is_disabled())
            .map(|h| Arc::new(h.isolation().state(mint, maxt)));
        Self {
            head,
            blocks: Vec::new(),
            blocks_lock: Some(blocks_lock),
            last_garbage_collected_mmap_ref: None,
            closed: Some(closed),
            mint,
            maxt,
            concurrent: false,
            tx_iso,
            select_stats: None,
            select_stats_filter: None,
        }
    }

    pub fn with_closed(mut self, closed: &'a AtomicBool) -> Self {
        self.closed = Some(closed);
        self
    }

    pub fn with_last_garbage_collected_mmap_ref(
        mut self,
        last_garbage_collected_mmap_ref: &'a AtomicU64,
    ) -> Self {
        self.last_garbage_collected_mmap_ref = Some(last_garbage_collected_mmap_ref);
        self
    }

    /// Sets Go `sharded` / `hints.ShardCount > 0` (`concurrent` param on
    /// `PostingsForMatchersCache.PostingsForMatchers`).
    pub fn with_concurrent(mut self, concurrent: bool) -> Self {
        self.concurrent = concurrent;
        self
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.closed.is_some_and(|c| c.load(AtomicOrdering::Acquire))
            || self.head.is_some_and(Head::is_closed)
    }



    /// Records per block querier [`BlockSelectStats`] of chunk selects into `sink`.
    pub fn set_select_stats(&mut self, sink: BlockSelectStatsSink, filter: Option<SeriesStatsFilter>) {
        self.select_stats = Some(sink);
        self.select_stats_filter = filter;
    }

    /// Whether a returned series counts towards [`BlockSelectStats::series`].
    fn counts_for_stats(&self, labels: &Labels) -> bool {
        self.select_stats.is_some() && self.select_stats_filter.as_ref().is_none_or(|f| f(labels))
    }

    /// Reports the per-source stats of one chunk select: the Head (if queried), every block with
    /// matching postings, and `0/0` for overlapping blocks without any.
    fn record_select_stats(
        &self,
        head: Option<BlockSelectStats>,
        blocks: &[BlockSelectStats],
        empty_block_min_times: &[i64],
    ) {
        let Some(sink) = &self.select_stats else {
            return;
        };
        let mut g = sink.lock();
        g.extend(head.map(|h| BlockSelectStats {
            head: true,
            block_min_time: 0,
            ..h
        }));
        g.extend_from_slice(blocks);
        g.extend(empty_block_min_times.iter().map(|&min_t| BlockSelectStats {
            selected_postings: 0,
            series: 0,
            head: false,
            overlaps_ooo: false,
            block_min_time: min_t,
        }));
    }

    fn snapshot_blocks(
        &self,
        guard: Option<&parking_lot::RwLockReadGuard<'a, Vec<Arc<Block>>>>,
    ) -> Result<(Vec<Arc<Block>>, Vec<BlockReadGuard>), QuerierError> {
        let slice: &[Arc<Block>] = match guard {
            Some(g) => g.as_slice(),
            None => self.blocks.as_slice(),
        };
        let mut blocks = Vec::new();
        let mut guards = Vec::new();
        for b in slice {
            if b.overlaps_closed_interval(self.mint, self.maxt) {
                let Some(bg) = Block::start_read(b) else {
                    return Err(QuerierError::Closed);
                };
                blocks.push(Arc::clone(b));
                guards.push(bg);
            }
        }
        Ok((blocks, guards))
    }

    /// Holds `blocks_lock.read()` ONLY long enough to check `is_closed()`, clone
    /// overlapping `Block`s with `BlockReadGuard`s, register a `HeadReadGuard`, and capture
    /// `HeadReadIsolation`, then drops `blocks_lock.read()` immediately.
    fn snapshot_sources(&self, mode: HeadSnapshotMode) -> Result<QuerySourceSnapshot, QuerierError> {
        snapshot_query_sources_with_lock(
            self.head,
            &self.blocks,
            self.blocks_lock,
            self.last_garbage_collected_mmap_ref,
            self.closed,
            self.mint,
            self.maxt,
            mode,
        )
    }

    fn try_snapshot_sources(&self) -> Result<Option<QuerySourceSnapshot>, QuerierError> {
        let lock_guard = match self.blocks_lock {
            Some(l) => match l.try_read() {
                Some(g) => Some(g),
                None => return Ok(None),
            },
            None => None,
        };
        if self.is_closed() {
            drop(lock_guard);
            return Err(QuerierError::Closed);
        }
        let (blocks, block_guards) = match self.snapshot_blocks(lock_guard.as_ref()) {
            Ok(pair) => pair,
            Err(e) => {
                drop(lock_guard);
                return Err(e);
            }
        };
        let head_snap = self.head.and_then(|h| {
            snapshot_head_query_source(
                h,
                self.last_garbage_collected_mmap_ref,
                self.mint,
                self.maxt,
                HeadSnapshotMode::Effective,
            )
        });
        drop(lock_guard);
        Ok(Some(QuerySourceSnapshot {
            blocks,
            block_guards,
            head: head_snap,
        }))
    }

    fn blocks_match_prepared(&self, prepared: &PreparedChunkSelect) -> Result<bool, QuerierError> {
        let Some(lock) = self.blocks_lock else {
            if self.is_closed() {
                return Err(QuerierError::Closed);
            }
            return Ok(true);
        };
        let lock_guard = lock.read();
        if self.is_closed() {
            return Err(QuerierError::Closed);
        }
        let mut idx = 0usize;
        let mut all_match = true;
        for b in lock_guard.iter() {
            if b.overlaps_closed_interval(self.mint, self.maxt) {
                match prepared.overlapping_blocks.get(idx) {
                    Some(prev) if Arc::ptr_eq(b, prev) => idx += 1,
                    _ => {
                        all_match = false;
                        break;
                    }
                }
            }
        }
        Ok(all_match && idx == prepared.overlapping_blocks.len())
    }



    /// Queries samples for all series matching `matchers` in `[mint, maxt]`.
    pub fn select(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<QuerySeries>, QuerierError> {
        let chunk_series = self.select_chunks(sort_series, matchers)?;
        let mut out = Vec::with_capacity(chunk_series.len());

        for cs in chunk_series {
            let mut samples = Vec::new();
            for cm in &cs.chunks {
                let Some(ref enc) = cm.chunk else {
                    continue;
                };
                for s in decode_chunk_samples(enc)? {
                    if s.t < self.mint || s.t > self.maxt {
                        continue;
                    }
                    samples.push(s);
                }
            }
            if samples.is_empty() {
                continue;
            }
            // Ensure sorted and deduplicated when multiple chunks or blocks overlap.
            if samples.windows(2).any(|w| w[1].t <= w[0].t) {
                samples.sort_by_key(|s| s.t);
                samples.dedup_by_key(|s| s.t);
            }
            out.push(QuerySeries {
                labels: cs.labels,
                samples,
            });
        }
        Ok(out)
    }

    /// Queries encoded chunks for all series matching `matchers` in `[mint, maxt]`.
    pub fn select_chunks(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        self.select_chunks_with_mode(sort_series, matchers, false)
    }

    fn prepare_select_locked(
        &self,
        snapshot: QuerySourceSnapshot,
        matchers: &[LabelMatcher],
        unordered: bool,
    ) -> Result<PreparedChunkSelect, QuerierError> {
        let block_guards = snapshot.block_guards;
        let blocks = snapshot.blocks;
        let (head_guard, head_isolation, head_overlaps_ooo) = match snapshot.head {
            Some(hs) => (Some(hs.guard), Some(hs.isolation), hs.overlaps_ooo),
            None => (None, None, false),
        };
        let head = head_guard.as_ref().and(self.head);
        let (head_refs, head_shard_hashes) = match head {
            Some(h) => {
                let (refs, slot) =
                    h.postings_for_matchers_with_shard_slot(matchers, self.concurrent)?;
                (Some(refs), slot)
            }
            None => (None, None),
        };
        let mut block_refs = Vec::with_capacity(blocks.len());
        let mut block_shard_hashes = Vec::with_capacity(blocks.len());
        for blk in &blocks {
            let (refs, slot) =
                blk.postings_for_matchers_with_shard_slot(matchers, self.concurrent)?;
            if !refs.is_empty() {
                block_refs.push((Arc::clone(blk), refs));
                block_shard_hashes.push(slot);
            }
        }
        Ok(PreparedChunkSelect {
            head_refs,
            head_shard_hashes,
            block_refs,
            block_shard_hashes,
            overlapping_blocks: blocks,
            _block_guards: block_guards,
            _head_guard: head_guard,
            head_isolation,
            tx_iso: self.tx_iso.as_ref().map(|s| s.info().clone()),
            mint: self.mint,
            maxt: self.maxt,
            unordered,
            head_overlaps_ooo,
        })
    }

    /// Evaluates postings matchers across `Head` and overlapping `Block`s (hitting
    /// `PostingsForMatchersCache` when cached) without loading any chunk bytes or holding
    /// `blocks_lock.read()`.
    pub fn prepare_select_chunks(
        &self,
        matchers: &[LabelMatcher],
        unordered: bool,
    ) -> Result<PreparedChunkSelect, QuerierError> {
        let snapshot = self.snapshot_sources(HeadSnapshotMode::Effective)?;
        self.prepare_select_locked(snapshot, matchers, unordered)
    }

    /// Snapshots blocks & guards under `blocks_lock.read()`, drops the lock immediately, and
    /// evaluates postings. If matched series are small (`<= max_source_series` and
    /// `<= max_total_refs`), materializes chunks inline; otherwise returns `Ok(Err(prepared))`.
    pub fn select_chunks_adaptive(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        unordered: bool,
        max_source_series: usize,
        max_total_refs: usize,
    ) -> Result<Result<Vec<QueryChunkSeries>, PreparedChunkSelect>, QuerierError> {
        let snapshot = self.snapshot_sources(HeadSnapshotMode::Effective)?;
        let prepared = self.prepare_select_locked(snapshot, matchers, unordered)?;
        if prepared.is_empty() {
            let empty_min_times: Vec<i64> = prepared
                .overlapping_blocks
                .iter()
                .map(|b| b.min_time())
                .collect();
            self.record_select_stats(
                prepared._head_guard.as_ref().map(|_| BlockSelectStats {
                    selected_postings: 0,
                    series: 0,
                    head: true,
                    overlaps_ooo: prepared.head_overlaps_ooo,
                    block_min_time: 0,
                }),
                &[],
                &empty_min_times,
            );
            return Ok(Ok(Vec::new()));
        }
        if prepared.max_source_series_len() <= max_source_series
            && prepared.total_refs() <= max_total_refs
        {
            let series = self.execute_prepared_select_locked(prepared, sort_series)?;
            return Ok(Ok(series));
        }
        Ok(Err(prepared))
    }

    /// Non-blocking variant of [`Self::select_chunks_adaptive`]: returns `Ok(None)` immediately
    /// if `blocks_lock` is currently contended by a compaction writer, and otherwise drops
    /// `blocks_lock.read()` immediately after snapshotting block/head guards.
    pub fn try_select_chunks_adaptive(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        unordered: bool,
        max_source_series: usize,
        max_total_refs: usize,
    ) -> Result<Option<Result<Vec<QueryChunkSeries>, PreparedChunkSelect>>, QuerierError> {
        let Some(snapshot) = self.try_snapshot_sources()? else {
            return Ok(None);
        };
        let prepared = self.prepare_select_locked(snapshot, matchers, unordered)?;
        if prepared.is_empty() {
            let empty_min_times: Vec<i64> = prepared
                .overlapping_blocks
                .iter()
                .map(|b| b.min_time())
                .collect();
            self.record_select_stats(
                prepared._head_guard.as_ref().map(|_| BlockSelectStats {
                    selected_postings: 0,
                    series: 0,
                    head: true,
                    overlaps_ooo: prepared.head_overlaps_ooo,
                    block_min_time: 0,
                }),
                &[],
                &empty_min_times,
            );
            return Ok(Some(Ok(Vec::new())));
        }
        if prepared.max_source_series_len() <= max_source_series
            && prepared.total_refs() <= max_total_refs
        {
            let series = self.execute_prepared_select_locked(prepared, sort_series)?;
            return Ok(Some(Ok(series)));
        }
        Ok(Some(Err(prepared)))
    }

    /// Materializes chunks for a [`PreparedChunkSelect`] without holding `blocks_lock.read()`,
    /// refreshing the selection if concurrent compaction updated `blocks`.
    pub fn execute_prepared_select_with_matchers(
        &self,
        mut prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        if !self.blocks_match_prepared(&prepared)? {
            let unordered = prepared.unordered;
            drop(prepared);
            let snapshot = self.snapshot_sources(HeadSnapshotMode::Effective)?;
            prepared = self.prepare_select_locked(snapshot, matchers, unordered)?;
        }
        self.execute_prepared_select_locked(prepared, sort_series)
    }

    /// Materializes chunks for a [`PreparedChunkSelect`] produced by [`Self::prepare_select_chunks`].
    pub fn execute_prepared_select(
        &self,
        prepared: PreparedChunkSelect,
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        if self.is_closed() {
            return Err(QuerierError::Closed);
        }
        self.execute_prepared_select_locked(prepared, sort_series)
    }

    fn execute_prepared_select_locked(
        &self,
        prepared: PreparedChunkSelect,
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        if self.is_closed() {
            return Err(QuerierError::Closed);
        }
        let head = prepared._head_guard.as_ref().and(self.head);
        let head_iso = prepared.head_isolation.as_ref();
        let tx_iso = prepared.tx_iso.as_ref();
        let float_enc = self.head.map_or(Encoding::Xor, Head::float_chunk_encoding);
        let mint = prepared.mint;
        let maxt = prepared.maxt;
        let unordered = prepared.unordered;
        let head_overlaps_ooo = prepared.head_overlaps_ooo;

        let empty_block_min_times: Vec<i64> = prepared
            .overlapping_blocks
            .iter()
            .filter(|b| !prepared.block_refs.iter().any(|(br, _)| Arc::ptr_eq(b, br)))
            .map(|b| b.min_time())
            .collect();
        if prepared.block_refs.is_empty() {
            let (Some(head), Some(refs)) = (head, prepared.head_refs) else {
                self.record_select_stats(None, &[], &empty_block_min_times);
                return Ok(Vec::new());
            };
            let mut out = Vec::with_capacity(refs.len());
            for &r in refs.iter() {
                let res = head.series_chunks_with_tx_isolation(r, mint, maxt, head_iso, tx_iso)?;
                if let Some((labels, chks, tombs)) = res {
                    // Go `HeadAndOOOChunkReader` / `populateChunksFromIterable` (`querier.go:1271`)
                    // uses `Xor` when `st == 0`, and preserves `Xor2` when ST storage is enabled.
                    let head_float_enc = if head.options().enable_st_storage {
                        head.float_chunk_encoding()
                    } else {
                        Encoding::Xor
                    };
                    let final_chunks = merge_head_chunks_with_tombstones_and_float_encoding(
                        chks,
                        &tombs,
                        head_float_enc,
                    )?;
                    if !final_chunks.is_empty() {
                        out.push(QueryChunkSeries {
                            labels,
                            chunks: final_chunks,
                        });
                    }
                }
            }
            self.record_select_stats(
                Some(BlockSelectStats {
                    selected_postings: refs.len() as u64,
                    series: out.iter().filter(|s| self.counts_for_stats(&s.labels)).count() as u64,
                    head: true,
                    overlaps_ooo: head_overlaps_ooo,
                    block_min_time: 0,
                }),
                &[],
                &empty_block_min_times,
            );
            if sort_series {
                out.sort_by(|a, b| a.labels.cmp(&b.labels));
            }
            return Ok(out);
        }

        struct SeriesSources {
            head: Vec<(ChunkMeta, Vec<Interval>)>,
            blocks: Vec<(ChunkMeta, Vec<Interval>)>,
        }

        let est = prepared
            .head_refs
            .as_ref()
            .map_or(0, |r| r.len())
            .max(
                prepared
                    .block_refs
                    .iter()
                    .map(|(_, r)| r.len())
                    .max()
                    .unwrap_or(0),
            );
        let mut per_series: hashbrown::HashMap<Labels, SeriesSources> =
            hashbrown::HashMap::with_capacity(est);

        let mut head_stats = None;
        if let (Some(head), Some(refs)) = (head, prepared.head_refs) {
            let mut stats = BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: 0,
                head: true,
                overlaps_ooo: head_overlaps_ooo,
                block_min_time: 0,
            };
            for &r in refs.iter() {
                let res = head.series_chunks_with_tx_isolation(r, mint, maxt, head_iso, tx_iso)?;
                if let Some((labels, chks, tombs)) = res {
                    if !chks.is_empty() && self.counts_for_stats(&labels) {
                        stats.series += 1;
                    }
                    let entry = per_series.entry(labels).or_insert_with(|| SeriesSources {
                        head: Vec::new(),
                        blocks: Vec::new(),
                    });
                    for c in chks {
                        entry.head.push((c, tombs.clone()));
                    }
                }
            }
            head_stats = Some(stats);
        }

        let mut block_stats: SmallVec<[BlockSelectStats; 4]> = SmallVec::new();
        for (blk, refs) in prepared.block_refs {
            let idx = blk.index();
            let chkr = blk.chunks();
            let tombr = blk.tombstones();
            let mut stats = BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: 0,
                head: false,
                overlaps_ooo: false,
                block_min_time: blk.min_time(),
            };
            for &r in refs.iter() {
                let (labels, chks) = idx.series(r)?;
                let tombs = tombr.get(r);
                let mut loaded = Vec::new();
                for mut cm in chks {
                    if !cm.overlaps_closed_interval(mint, maxt) {
                        continue;
                    }
                    if tombs
                        .iter()
                        .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
                    {
                        continue;
                    }
                    cm.chunk = Some(chkr.chunk(cm.ref_id)?);
                    loaded.push((cm, tombs.clone()));
                }
                if !loaded.is_empty() {
                    if self.counts_for_stats(&labels) {
                        stats.series += 1;
                    }
                    let entry = per_series.entry(labels).or_insert_with(|| SeriesSources {
                        head: Vec::new(),
                        blocks: Vec::new(),
                    });
                    entry.blocks.extend(loaded);
                }
            }
            block_stats.push(stats);
        }
        self.record_select_stats(head_stats, &block_stats, &empty_block_min_times);

        let head_float_enc = if head.is_some_and(|h| h.options().enable_st_storage) {
            float_enc
        } else {
            Encoding::Xor
        };
        let merge_head = |items: Vec<(ChunkMeta, Vec<Interval>)>| -> Result<Vec<ChunkMeta>, QuerierError> {
            Ok(merge_chunks_with_tombstones_and_float_encoding(
                items,
                false,
                head_float_enc,
            )?)
        };
        let merge_cross_source = |items: Vec<(ChunkMeta, Vec<Interval>)>,
                                  allow_non_overlapping_raw: bool|
         -> Result<Vec<ChunkMeta>, QuerierError> {
            Ok(merge_chunks_with_tombstones_and_float_encoding(
                items,
                allow_non_overlapping_raw,
                float_enc,
            )?)
        };

        let mut out = Vec::with_capacity(per_series.len());
        for (labels, sources) in per_series {
            let final_chunks = if unordered {
                let mut combined = merge_head(sources.head)?;
                let blk_chks = merge_cross_source(sources.blocks, true)?;
                combined.extend(blk_chks);
                combined
            } else if sources.blocks.is_empty() {
                merge_head(sources.head)?
            } else {
                let mut all_chks = merge_head(sources.head)?;
                for (cm, tombs) in sources.blocks {
                    if tombs
                        .iter()
                        .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
                    {
                        continue;
                    }
                    if let Some(c) = reencode_chunk_without_tombstones(cm, &tombs)? {
                        all_chks.push(c);
                    }
                }
                compact_chunk_series_merge(all_chks, float_enc)?
            };
            if !final_chunks.is_empty() {
                out.push(QueryChunkSeries {
                    labels,
                    chunks: final_chunks,
                });
            }
        }

        if sort_series {
            out.sort_by(|a, b| a.labels.cmp(&b.labels));
        }
        Ok(out)
    }

    /// Two-phase lazy `QueryStream` executor without holding `blocks_lock.read()` across
    /// Phase 1 / Phase 2 or `on_event` (`tx.blocking_send`):
    /// - Phase 1 builds lightweight `LazySeriesEntry` descriptors (`chunk_count` without cloning chunk bytes
    ///   on the fast path), sorts if requested, streams `(Labels, chunk_count)` via `on_series` (dropping
    ///   `Labels` immediately), and calls `on_series_end`.
    /// - Phase 2 loads `Vec<ChunkMeta>` for ONE series at a time and yields it to `on_series_chunks`.
    #[allow(clippy::too_many_arguments)]
    pub fn stream_prepared_chunks_lazy<F>(
        &self,
        prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
        series_filter: Option<&dyn Fn(&Labels) -> bool>,
        on_event: F,
    ) -> Result<(), QuerierError>
    where
        F: FnMut(LazyStreamEvent<'_>) -> Result<bool, QuerierError>,
    {
        self.stream_prepared_chunks_lazy_with_shard(
            prepared,
            matchers,
            sort_series,
            None,
            series_filter,
            on_event,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn stream_prepared_chunks_lazy_with_shard<F>(
        &self,
        mut prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
        shard: Option<(u64, u64)>,
        series_filter: Option<&dyn Fn(&Labels) -> bool>,
        mut on_event: F,
    ) -> Result<(), QuerierError>
    where
        F: FnMut(LazyStreamEvent<'_>) -> Result<bool, QuerierError>,
    {
        if let Some((idx, count)) = shard {
            if count == 0 || idx >= count {
                return Err(QuerierError::InvalidShard { idx, count });
            }
        }
        if !self.blocks_match_prepared(&prepared)? {
            let unordered = prepared.unordered;
            drop(prepared);
            let snapshot = self.snapshot_sources(HeadSnapshotMode::Effective)?;
            prepared = self.prepare_select_locked(snapshot, matchers, unordered)?;
        }

        let head = prepared._head_guard.as_ref().and(self.head);
        let head_iso = prepared.head_isolation.as_ref();
        let tx_iso = prepared.tx_iso.as_ref();
        let float_enc = self.head.map_or(Encoding::Xor, Head::float_chunk_encoding);
        let mint = prepared.mint;
        let maxt = prepared.maxt;
        let unordered = prepared.unordered;
        let head_overlaps_ooo = prepared.head_overlaps_ooo;
        let empty_block_min_times: Vec<i64> = prepared
            .overlapping_blocks
            .iter()
            .filter(|b| !prepared.block_refs.iter().any(|(br, _)| Arc::ptr_eq(b, br)))
            .map(|b| b.min_time())
            .collect();

        let merge_or_collect = |items: Vec<(ChunkMeta, Vec<Interval>)>,
                                allow_non_overlapping_raw: bool|
         -> Result<Vec<ChunkMeta>, QuerierError> {
            Ok(merge_chunks_with_tombstones_and_float_encoding(
                items,
                allow_non_overlapping_raw,
                float_enc,
            )?)
        };

        if prepared.block_refs.is_empty() {
            struct HeadOnlyEntry {
                ref_id: u64,
                entry: Option<Arc<SeriesEntry>>,
                snapshot: Option<HeadChunkSnapshot>,
                chunk_count: u32,
            }
            let selected_postings = prepared.head_refs.as_ref().map(|r| r.len() as u64);
            let mut entries: Vec<HeadOnlyEntry> = match (head, prepared.head_refs) {
                (Some(h), Some(refs)) => {
                    let est_cap = match shard {
                        Some((_, count)) if count > 1 => (refs.len() / (count as usize)).max(16),
                        _ => refs.len(),
                    };
                    let cached_head_hashes = if shard.is_some() {
                        h.resolve_shard_hashes_in_slot(&refs, prepared.head_shard_hashes.as_ref())
                            .filter(|hashes| hashes.len() == refs.len())
                    } else {
                        None
                    };
                    let mut v = Vec::with_capacity(est_cap);
                    for (pos_i, &r) in refs.iter().enumerate() {
                        let inner_shard = if let (Some((shard_idx, shard_count)), Some(ref hashes)) =
                            (shard, &cached_head_hashes)
                        {
                            if hashes[pos_i] % shard_count != shard_idx {
                                continue;
                            }
                            None
                        } else {
                            shard
                        };
                        let res = h.series_lazy_chunk_count_with_snapshot_and_shard(
                            r,
                            mint,
                            maxt,
                            inner_shard,
                            series_filter,
                            head_iso,
                            tx_iso,
                        )?;
                        if let Some((entry, chunk_count, head_snapshot)) = res {
                            if chunk_count > 0 {
                                v.push(HeadOnlyEntry {
                                    ref_id: r,
                                    entry: Some(entry),
                                    snapshot: Some(head_snapshot),
                                    chunk_count,
                                });
                            }
                        }
                    }
                    v
                }
                _ => Vec::new(),
            };
            self.record_select_stats(
                head.and(selected_postings).map(|selected_postings| BlockSelectStats {
                    selected_postings,
                    series: entries.len() as u64,
                    head: true,
                    overlaps_ooo: head_overlaps_ooo,
                    block_min_time: 0,
                }),
                &[],
                &empty_block_min_times,
            );

            fn get_head_lbls(e: &HeadOnlyEntry) -> &Labels {
                match e.entry.as_deref() {
                    Some(se) => &se.labels,
                    None => {
                        static EMPTY: std::sync::OnceLock<Labels> = std::sync::OnceLock::new();
                        EMPTY.get_or_init(Labels::empty)
                    }
                }
            }
            let order: Option<Vec<u32>> = if sort_series && entries.len() > 1 {
                sort_label_indices(&entries, get_head_lbls)
            } else {
                None
            };
            if order.is_none() && sort_series && entries.len() > 1 {
                entries.sort_unstable_by(|a, b| get_head_lbls(a).cmp(get_head_lbls(b)));
            }

            if let Some(ref ord) = order {
                for &idx in ord {
                    let entry = &entries[idx as usize];
                    if !on_event(LazyStreamEvent::Series(
                        get_head_lbls(entry),
                        entry.chunk_count,
                    ))? {
                        return Ok(());
                    }
                }
            } else {
                for entry in entries.iter() {
                    if !on_event(LazyStreamEvent::Series(
                        get_head_lbls(entry),
                        entry.chunk_count,
                    ))? {
                        return Ok(());
                    }
                }
            }
            if !on_event(LazyStreamEvent::SeriesEnd)? {
                return Ok(());
            }

            if let Some(h) = head {
                // Strict 3-pass Phase 2 batching preserving C2 lock ordering and zero lock-held
                // backpressure:
                //   Pass A: snapshot each series under `entry.inner` ONLY (no `chunk_reader` lock)
                //   Pass B: acquire leaf `chunk_reader.read()` once for all mmapped chunks in batch,
                //           then DROP it immediately before any retry or callback
                //   Pass C: emit `on_event` with ZERO TSDB locks held
                const READER_BATCH: usize = 64;
                enum HeadBatchItem {
                    Ready(Vec<ChunkMeta>),
                    NeedsRead {
                        ref_id: u64,
                        entry: Option<Arc<SeriesEntry>>,
                        snap: HeadChunkSnapshot,
                        mmapped: Vec<(u64, crate::head::MmappedChunk, Option<usize>)>,
                        head_chks: Vec<ChunkMeta>,
                    },
                    Retry {
                        ref_id: u64,
                        entry: Option<Arc<SeriesEntry>>,
                        snap: Option<HeadChunkSnapshot>,
                    },
                }

                let total = entries.len();
                let mut batch: Vec<(usize, HeadBatchItem)> = Vec::with_capacity(READER_BATCH);
                let mut pos = 0usize;
                while pos < total {
                    let end = (pos + READER_BATCH).min(total);
                    batch.clear();
                    let mut any_mmapped = false;

                    // Pass A: snapshot under `entry.inner` with NO `chunk_reader` guard held.
                    for series_idx in pos..end {
                        let e_idx = order.as_ref().map_or(series_idx, |o| o[series_idx] as usize);
                        let entry = &mut entries[e_idx];
                        let r = entry.ref_id;
                        let head_entry = entry.entry.take();
                        let snap = entry.snapshot.take();
                        let item = match snap {
                            Some(HeadChunkSnapshot::Precomputed(chks)) => {
                                HeadBatchItem::Ready(chks)
                            }
                            Some(HeadChunkSnapshot::InOrderByMinTime(min_times)) => {
                                match h.snapshot_in_order_chunks_for_entry(
                                    head_entry.as_deref(),
                                    r,
                                    &min_times,
                                    tx_iso,
                                )? {
                                    None => HeadBatchItem::Ready(Vec::new()),
                                    Some((mmapped, head_chks)) if mmapped.is_empty() => {
                                        HeadBatchItem::Ready(head_chks)
                                    }
                                    Some((mmapped, head_chks)) => {
                                        any_mmapped = true;
                                        HeadBatchItem::NeedsRead {
                                            ref_id: r,
                                            entry: head_entry,
                                            snap: HeadChunkSnapshot::InOrderByMinTime(min_times),
                                            mmapped,
                                            head_chks,
                                        }
                                    }
                                }
                            }
                            None => HeadBatchItem::Retry {
                                ref_id: r,
                                entry: head_entry,
                                snap: None,
                            },
                        };
                        batch.push((series_idx, item));
                    }

                    // Pass B: acquire leaf `chunk_reader.read()` once (NO `entry.inner` lock),
                    // decode all mmapped chunks, then drop guard before any fallback or callback.
                    if any_mmapped {
                        let guard = h.read_chunk_reader_guard();
                        if let Some(reader) = guard.as_ref() {
                            for (_, item) in &mut batch {
                                if let HeadBatchItem::NeedsRead {
                                    mmapped, head_chks, ..
                                } = item
                                {
                                    if let Some(chks) = h.decode_mmapped_plan_with_reader(
                                        mmapped, head_chks, reader,
                                    )? {
                                        *item = HeadBatchItem::Ready(chks);
                                    }
                                }
                            }
                        } else {
                            drop(guard);
                            return Err(QuerierError::Head(crate::head::HeadError::from(crate::head_chunks::HeadChunksError::Closed)));
                        }
                        drop(guard);
                    }

                    // Pass C: resolve any rotated/fallback items (no guard held) and emit `on_event`
                    // with ZERO TSDB locks held.
                    for (series_idx, item) in batch.drain(..) {
                        let chks = match item {
                            HeadBatchItem::Ready(c) => c,
                            HeadBatchItem::NeedsRead {
                                ref_id,
                                entry,
                                snap,
                                ..
                            } => h.series_final_chunks_from_snapshot_with_tx_isolation(
                                entry.as_deref(),
                                ref_id,
                                snap,
                                tx_iso,
                            )?,
                            HeadBatchItem::Retry {
                                ref_id,
                                entry,
                                snap,
                            } => match snap {
                                Some(s) => h.series_final_chunks_from_snapshot_with_tx_isolation(
                                    entry.as_deref(),
                                    ref_id,
                                    s,
                                    tx_iso,
                                )?,
                                None => match entry {
                                    Some(ref he) => h.series_final_chunks_for_entry_with_tx_isolation(
                                        he, mint, maxt, head_iso, tx_iso,
                                    )?,
                                    None => h.series_final_chunks_only_with_tx_isolation(
                                        ref_id, mint, maxt, head_iso, tx_iso,
                                    )?,
                                },
                            },
                        };
                        if !on_event(LazyStreamEvent::SeriesChunks(series_idx, chks))? {
                            return Ok(());
                        }
                    }
                    pos = end;
                }
            }
            return Ok(());
        }

        // Multi-source path (Head and/or one or more ULID Blocks):
        let blocks: Vec<Arc<Block>> = prepared
            .block_refs
            .iter()
            .map(|(b, _)| Arc::clone(b))
            .collect();

        struct MultiMeta {
            head_ref: Option<u64>,
            head_entry: Option<Arc<SeriesEntry>>,
            head_ivs: SmallVec<[(i64, i64); 4]>,
            block_chks: SmallVec<[LazyBlockChunkRef; 4]>,
            head_snapshot: Option<HeadChunkSnapshot>,
            has_block_tombstone_overlap: bool,
        }

        let raw_est = prepared
            .head_refs
            .as_ref()
            .map_or(0, |r| r.len())
            .max(
                prepared
                    .block_refs
                    .iter()
                    .map(|(_, r)| r.len())
                    .max()
                    .unwrap_or(0),
            );
        let est = match shard {
            Some((_, count)) if count > 1 => (raw_est / (count as usize)).max(16),
            _ => raw_est,
        };
        let mut per_series: hashbrown::HashMap<Labels, MultiMeta> =
            hashbrown::HashMap::with_capacity(est);

        let mut head_stats = None;
        if let (Some(h), Some(refs)) = (head, prepared.head_refs) {
            let mut stats = BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: 0,
                head: true,
                overlaps_ooo: head_overlaps_ooo,
                block_min_time: 0,
            };
            let cached_head_hashes = if shard.is_some() {
                h.resolve_shard_hashes_in_slot(&refs, prepared.head_shard_hashes.as_ref())
                    .filter(|hashes| hashes.len() == refs.len())
            } else {
                None
            };
            for (pos_i, &r) in refs.iter().enumerate() {
                let inner_shard = if let (Some((shard_idx, shard_count)), Some(ref hashes)) =
                    (shard, &cached_head_hashes)
                {
                    if hashes[pos_i] % shard_count != shard_idx {
                        continue;
                    }
                    None
                } else {
                    shard
                };
                let res = h.series_chunk_metas_no_bytes_with_entry_snapshot_and_shard(
                    r,
                    mint,
                    maxt,
                    inner_shard,
                    series_filter,
                    head_iso,
                    tx_iso,
                )?;
                if let Some((head_entry, labels, ivs, head_snapshot)) = res {
                    if !ivs.is_empty() {
                        stats.series += 1;
                    }
                    let entry = per_series.entry(labels).or_insert_with(|| MultiMeta {
                        head_ref: None,
                        head_entry: None,
                        head_ivs: SmallVec::new(),
                        block_chks: SmallVec::new(),
                        head_snapshot: None,
                        has_block_tombstone_overlap: false,
                    });
                    entry.head_ref = Some(r);
                    entry.head_entry = Some(head_entry);
                    entry.head_ivs.extend(ivs);
                    entry.head_snapshot = Some(head_snapshot);
                }
            }
            head_stats = Some(stats);
        }

        let mut block_stats: SmallVec<[BlockSelectStats; 4]> = SmallVec::new();
        for (b_idx, (blk, refs)) in prepared.block_refs.iter().enumerate() {
            let idx = blk.index();
            let tombr = blk.tombstones();
            let blk_has_any_tombs = tombr.has_any();
            let blk_u32 = u32::try_from(b_idx).map_err(|_| {
                QuerierError::Index(IndexError::Corrupt(format!(
                    "overlapping block index {b_idx} exceeds u32::MAX"
                )))
            })?;
            let mut stats = BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: 0,
                head: false,
                overlaps_ooo: false,
                block_min_time: blk.min_time(),
            };
            let cached_shard_hashes = if shard.is_some() {
                let slot = prepared.block_shard_hashes.get(b_idx).and_then(|s| s.as_ref());
                blk.resolve_shard_hashes_in_slot(refs, slot)?
                    .filter(|hashes| hashes.len() == refs.len())
            } else {
                None
            };
            for (pos_i, &r) in refs.iter().enumerate() {
                if let Some((shard_idx, shard_count)) = shard {
                    let h = match cached_shard_hashes {
                        Some(ref hashes) => hashes[pos_i],
                        None => idx.series_stable_hash(r)?,
                    };
                    if h % shard_count != shard_idx {
                        continue;
                    }
                }
                let (labels, chks) = idx.series(r)?;
                if let Some(pred) = series_filter {
                    if !pred(&labels) {
                        continue;
                    }
                }
                let tombs = if blk_has_any_tombs {
                    tombr.get(r)
                } else {
                    Vec::new()
                };
                let mut local_chks: SmallVec<[LazyBlockChunkRef; 4]> = SmallVec::new();
                let mut partial_tomb = false;
                for cm in chks {
                    if !cm.overlaps_closed_interval(mint, maxt) {
                        continue;
                    }
                    if tombs
                        .iter()
                        .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
                    {
                        continue;
                    }
                    let has_tomb = !tombs.is_empty()
                        && tombs.iter().any(|iv| iv.overlaps(cm.min_time, cm.max_time));
                    if has_tomb {
                        partial_tomb = true;
                    }
                    local_chks.push(LazyBlockChunkRef {
                        block_idx: blk_u32,
                        series_ref: r,
                        chunk_ref: cm.ref_id,
                        min_time: cm.min_time,
                        max_time: cm.max_time,
                        has_tombstones: has_tomb,
                    });
                }
                if !local_chks.is_empty() {
                    stats.series += 1;
                    let entry = per_series.entry(labels).or_insert_with(|| MultiMeta {
                        head_ref: None,
                        head_entry: None,
                        head_ivs: SmallVec::new(),
                        block_chks: SmallVec::new(),
                        head_snapshot: None,
                        has_block_tombstone_overlap: false,
                    });
                    entry.block_chks.extend(local_chks);
                    if partial_tomb {
                        entry.has_block_tombstone_overlap = true;
                    }
                }
            }
            block_stats.push(stats);
        }
        self.record_select_stats(head_stats, &block_stats, &empty_block_min_times);

        let combine_head_and_blocks = |head_chks: Vec<ChunkMeta>,
                                       block_chks: &[LazyBlockChunkRef]|
         -> Result<Vec<ChunkMeta>, QuerierError> {
            let mut head_items: Vec<(ChunkMeta, Vec<Interval>)> =
                Vec::with_capacity(head_chks.len());
            for c in head_chks {
                head_items.push((c, Vec::new()));
            }
            let mut block_items: Vec<(ChunkMeta, Vec<Interval>)> =
                Vec::with_capacity(block_chks.len());
            for bc in block_chks {
                let blk = &blocks[bc.block_idx as usize];
                // Always re-check `has_any()` (1 ns atomic load) in Phase 2 so concurrent
                // `Block::delete` mutations between Pass 1 and Phase 2 cannot emit deleted samples.
                let tombs = if bc.has_tombstones || blk.tombstones().has_any() {
                    blk.tombstones().get(bc.series_ref)
                } else {
                    Vec::new()
                };
                if !tombs.is_empty()
                    && tombs
                        .iter()
                        .any(|iv| iv.is_subrange(bc.min_time, bc.max_time))
                {
                    continue;
                }
                let enc = blk.chunks().chunk(bc.chunk_ref)?;
                block_items.push((
                    ChunkMeta {
                        ref_id: bc.chunk_ref,
                        min_time: bc.min_time,
                        max_time: bc.max_time,
                        chunk: Some(enc),
                    },
                    tombs,
                ));
            }
            if unordered {
                let mut combined = merge_chunks_with_tombstones(head_items, false)?;
                let blk_chks = merge_or_collect(block_items, true)?;
                combined.extend(blk_chks);
                Ok(combined)
            } else if block_items.is_empty() {
                Ok(merge_chunks_with_tombstones(head_items, false)?)
            } else {
                let mut all_chks = merge_chunks_with_tombstones(head_items, false)?;
                for (cm, tombs) in block_items {
                    if let Some(c) = reencode_chunk_without_tombstones(cm, &tombs)? {
                        all_chks.push(c);
                    }
                }
                Ok(compact_chunk_series_merge(all_chks, float_enc)?)
            }
        };

        let mut entries: Vec<LazySeriesEntry> = Vec::with_capacity(per_series.len());
        for (labels, mut meta) in per_series {
            let can_count_without_load = if meta.has_block_tombstone_overlap {
                false
            } else if unordered {
                // `head_ivs` is already post-merge from `series_chunk_metas_no_bytes_with_snapshot`;
                // `blocks` in unordered mode concatenate without overlap merging.
                true
            } else {
                let mut all_ivs: SmallVec<[(i64, i64); 8]> =
                    SmallVec::with_capacity(meta.head_ivs.len() + meta.block_chks.len());
                all_ivs.extend_from_slice(&meta.head_ivs);
                all_ivs.extend(meta.block_chks.iter().map(|c| (c.min_time, c.max_time)));
                if all_ivs.len() <= 1 {
                    true
                } else {
                    all_ivs.sort_unstable();
                    all_ivs.windows(2).all(|w| w[1].0 > w[0].1)
                }
            };

            let (chunk_count, head_entry, head_snapshot, source) = if can_count_without_load {
                let cnt = (meta.head_ivs.len() + meta.block_chks.len()) as u32;
                let source = if meta.block_chks.is_empty() {
                    if let Some(r) = meta.head_ref {
                        LazyChunkSource::Head(r)
                    } else {
                        LazyChunkSource::Precomputed(Vec::new())
                    }
                } else {
                    LazyChunkSource::Resolved {
                        head_ref: meta.head_ref,
                        block_chks: meta.block_chks.into_vec().into_boxed_slice(),
                    }
                };
                (cnt, meta.head_entry.take(), meta.head_snapshot.take(), source)
            } else {
                let head_chks = if let (Some(h), Some(r)) = (head, meta.head_ref) {
                    match meta.head_snapshot.take() {
                        Some(snap) => h.series_final_chunks_from_snapshot_with_tx_isolation(
                            meta.head_entry.as_deref(),
                            r,
                            snap,
                            tx_iso,
                        )?,
                        None => h.series_final_chunks_only_with_tx_isolation(
                            r, mint, maxt, head_iso, tx_iso,
                        )?,
                    }
                } else {
                    Vec::new()
                };
                let chks = combine_head_and_blocks(head_chks, &meta.block_chks)?;
                let cnt = chks.len() as u32;
                (cnt, None, None, LazyChunkSource::Precomputed(chks))
            };
            if chunk_count == 0 {
                continue;
            }
            entries.push(LazySeriesEntry {
                labels,
                head_entry,
                head_snapshot,
                source,
                chunk_count,
            });
        }

        let order: Option<Vec<u32>> = if sort_series && entries.len() > 1 {
            sort_label_indices(&entries, LazySeriesEntry::entry_labels)
        } else {
            None
        };
        if order.is_none() && sort_series && entries.len() > 1 {
            entries.sort_unstable_by(|a, b| a.entry_labels().cmp(b.entry_labels()));
        }

        if let Some(ref ord) = order {
            for &idx in ord {
                let entry = &entries[idx as usize];
                if !on_event(LazyStreamEvent::Series(entry.entry_labels(), entry.chunk_count))? {
                    return Ok(());
                }
            }
        } else {
            for entry in entries.iter() {
                if !on_event(LazyStreamEvent::Series(entry.entry_labels(), entry.chunk_count))? {
                    return Ok(());
                }
            }
        }
        if !on_event(LazyStreamEvent::SeriesEnd)? {
            return Ok(());
        }

        // Strict 3-pass Phase 2 batching (C2 lock order + zero locks held across `on_event`):
        //   Pass A: snapshot Head entries under `entry.inner` ONLY (no `chunk_reader` lock)
        //   Pass B: acquire leaf `chunk_reader.read()` once for all mmapped Head chunks in batch,
        //           decode, then drop guard immediately
        //   Pass C: combine Head + Block chunks and emit `on_event` with ZERO TSDB locks held
        const READER_BATCH: usize = 64;
        enum MultiHeadState {
            Precomputed(Vec<ChunkMeta>),
            Ready {
                head_chks: Vec<ChunkMeta>,
                block_chks: Box<[LazyBlockChunkRef]>,
            },
            NeedsRead {
                ref_id: u64,
                entry: Option<Arc<SeriesEntry>>,
                snap: HeadChunkSnapshot,
                mmapped: Vec<(u64, crate::head::MmappedChunk, Option<usize>)>,
                head_chks: Vec<ChunkMeta>,
                block_chks: Box<[LazyBlockChunkRef]>,
            },
            Retry {
                ref_id: u64,
                entry: Option<Arc<SeriesEntry>>,
                snap: Option<HeadChunkSnapshot>,
                block_chks: Box<[LazyBlockChunkRef]>,
            },
        }

        let total = entries.len();
        let mut batch: Vec<(usize, MultiHeadState)> = Vec::with_capacity(READER_BATCH);
        let mut pos = 0usize;
        while pos < total {
            let end = (pos + READER_BATCH).min(total);
            batch.clear();
            let mut any_mmapped = false;

            // Pass A: snapshot Head entries under `entry.inner` with NO `chunk_reader` lock.
            for series_idx in pos..end {
                let e_idx = order.as_ref().map_or(series_idx, |o| o[series_idx] as usize);
                let entry = &mut entries[e_idx];
                let head_entry = entry.head_entry.take();
                let snap = entry.head_snapshot.take();
                let src =
                    std::mem::replace(&mut entry.source, LazyChunkSource::Precomputed(Vec::new()));
                let state = match src {
                    LazyChunkSource::Precomputed(chks) => MultiHeadState::Precomputed(chks),
                    LazyChunkSource::Head(r) => match (head, snap) {
                        (None, _) => MultiHeadState::Ready {
                            head_chks: Vec::new(),
                            block_chks: Box::new([]),
                        },
                        (Some(_), Some(HeadChunkSnapshot::Precomputed(chks))) => {
                            MultiHeadState::Ready {
                                head_chks: chks,
                                block_chks: Box::new([]),
                            }
                        }
                        (Some(h), Some(HeadChunkSnapshot::InOrderByMinTime(min_times))) => {
                            match h.snapshot_in_order_chunks_for_entry(
                                head_entry.as_deref(),
                                r,
                                &min_times,
                                tx_iso,
                            )? {
                                None => MultiHeadState::Ready {
                                    head_chks: Vec::new(),
                                    block_chks: Box::new([]),
                                },
                                Some((mmapped, head_chks)) if mmapped.is_empty() => {
                                    MultiHeadState::Ready {
                                        head_chks,
                                        block_chks: Box::new([]),
                                    }
                                }
                                Some((mmapped, head_chks)) => {
                                    any_mmapped = true;
                                    MultiHeadState::NeedsRead {
                                        ref_id: r,
                                        entry: head_entry,
                                        snap: HeadChunkSnapshot::InOrderByMinTime(min_times),
                                        mmapped,
                                        head_chks,
                                        block_chks: Box::new([]),
                                    }
                                }
                            }
                        }
                        (Some(_), None) => MultiHeadState::Retry {
                            ref_id: r,
                            entry: head_entry,
                            snap: None,
                            block_chks: Box::new([]),
                        },
                    },
                    LazyChunkSource::Resolved {
                        head_ref,
                        block_chks,
                    } => match (head, head_ref, snap) {
                        (Some(_), Some(_), Some(HeadChunkSnapshot::Precomputed(chks))) => {
                            MultiHeadState::Ready {
                                head_chks: chks,
                                block_chks,
                            }
                        }
                        (
                            Some(h),
                            Some(r),
                            Some(HeadChunkSnapshot::InOrderByMinTime(min_times)),
                        ) => match h.snapshot_in_order_chunks_for_entry(
                            head_entry.as_deref(),
                            r,
                            &min_times,
                            tx_iso,
                        )? {
                            None => MultiHeadState::Ready {
                                head_chks: Vec::new(),
                                block_chks,
                            },
                            Some((mmapped, head_chks)) if mmapped.is_empty() => {
                                MultiHeadState::Ready {
                                    head_chks,
                                    block_chks,
                                }
                            }
                            Some((mmapped, head_chks)) => {
                                any_mmapped = true;
                                MultiHeadState::NeedsRead {
                                    ref_id: r,
                                    entry: head_entry,
                                    snap: HeadChunkSnapshot::InOrderByMinTime(min_times),
                                    mmapped,
                                    head_chks,
                                    block_chks,
                                }
                            }
                        },
                        (Some(_), Some(r), None) => MultiHeadState::Retry {
                            ref_id: r,
                            entry: head_entry,
                            snap: None,
                            block_chks,
                        },
                        _ => MultiHeadState::Ready {
                            head_chks: Vec::new(),
                            block_chks,
                        },
                    },
                    LazyChunkSource::Block { .. } | LazyChunkSource::Multi(_) => {
                        return Err(QuerierError::Index(IndexError::Corrupt(
                            "unexpected unresolved LazyChunkSource variant in multi-source lazy stream"
                                .to_string(),
                        )));
                    }
                };
                batch.push((series_idx, state));
            }

            // Pass B: acquire leaf `chunk_reader.read()` once (NO `entry.inner` lock),
            // decode all mmapped Head chunks, then drop guard immediately.
            if any_mmapped {
                if let Some(h) = head {
                    let guard = h.read_chunk_reader_guard();
                    if let Some(reader) = guard.as_ref() {
                        for (_, state) in &mut batch {
                            if let MultiHeadState::NeedsRead {
                                mmapped, head_chks, ..
                            } = state
                            {
                                if let Some(chks) =
                                    h.decode_mmapped_plan_with_reader(mmapped, head_chks, reader)?
                                {
                                    let prev = std::mem::replace(
                                        state,
                                        MultiHeadState::Precomputed(Vec::new()),
                                    );
                                    if let MultiHeadState::NeedsRead { block_chks, .. } = prev {
                                        *state = MultiHeadState::Ready {
                                            head_chks: chks,
                                            block_chks,
                                        };
                                    }
                                }
                            }
                        }
                    } else {
                        drop(guard);
                        return Err(QuerierError::Head(crate::head::HeadError::from(crate::head_chunks::HeadChunksError::Closed)));
                    }
                    drop(guard);
                }
            }

            // Pass C: resolve fallbacks + load Block chunks + emit `on_event` with ZERO Head locks held.
            for (series_idx, state) in batch.drain(..) {
                let chks = match state {
                    MultiHeadState::Precomputed(c) => c,
                    MultiHeadState::Ready {
                        head_chks,
                        block_chks,
                    } => combine_head_and_blocks(head_chks, &block_chks)?,
                    MultiHeadState::NeedsRead {
                        ref_id,
                        entry,
                        snap,
                        block_chks,
                        ..
                    } => {
                        let hc = match head {
                            Some(h) => h.series_final_chunks_from_snapshot_with_tx_isolation(
                                entry.as_deref(),
                                ref_id,
                                snap,
                                tx_iso,
                            )?,
                            None => Vec::new(),
                        };
                        combine_head_and_blocks(hc, &block_chks)?
                    }
                    MultiHeadState::Retry {
                        ref_id,
                        entry,
                        snap,
                        block_chks,
                    } => {
                        let hc = match head {
                            Some(h) => match snap {
                                Some(s) => h.series_final_chunks_from_snapshot_with_tx_isolation(
                                    entry.as_deref(),
                                    ref_id,
                                    s,
                                    tx_iso,
                                )?,
                                None => h.series_final_chunks_only_with_tx_isolation(
                                    ref_id, mint, maxt, head_iso, tx_iso,
                                )?,
                            },
                            None => Vec::new(),
                        };
                        combine_head_and_blocks(hc, &block_chks)?
                    }
                };
                if !on_event(LazyStreamEvent::SeriesChunks(series_idx, chks))? {
                    return Ok(());
                }
            }
            pos = end;
        }
        Ok(())
    }

    /// Queries encoded chunks for all series matching `matchers` in `[mint, maxt]`.
    /// When `unordered` is true, chunks from Head (in-order + OOO merged) and each Block are
    /// concatenated without cross-block compaction, matching Prometheus `UnorderedChunkQuerier`.
    pub fn select_chunks_with_mode(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        unordered: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        let snapshot = self.snapshot_sources(HeadSnapshotMode::Effective)?;
        let prepared = self.prepare_select_locked(snapshot, matchers, unordered)?;
        self.execute_prepared_select_locked(prepared, sort_series)
    }

    /// Queries matching series labels directly (without decoding or cloning chunk bytes).
    pub fn select_raw_labels(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>, QuerierError> {
        let snapshot = self.snapshot_sources(HeadSnapshotMode::Effective)?;
        let _block_guards = snapshot.block_guards;
        let blocks = snapshot.blocks;
        let head_snap = snapshot.head;
        let head = head_snap.as_ref().and(self.head);
        let head_iso = head_snap.as_ref().map(|hs| &hs.isolation);

        if blocks.is_empty() {
            let Some(head) = head else {
                return Ok(Vec::new());
            };
            let refs = head.postings_for_matchers_with_concurrent(matchers, self.concurrent)?;
            let mut out = Vec::with_capacity(refs.len());
            for &r in refs.iter() {
                let labels_opt = match head_iso {
                    Some(iso) => head.series_has_chunks_in_range_with_isolation(
                        r,
                        self.mint,
                        self.maxt,
                        iso,
                    ),
                    None => head.series_has_chunks_in_range(r, self.mint, self.maxt),
                };
                if let Some(labels) = labels_opt {
                    out.push(labels);
                }
            }
            if sort_series && out.len() > 1 {
                sort_labels(&mut out);
            }
            return Ok(out);
        }

        let mut set: BTreeSet<Labels> = BTreeSet::new();
        if let Some(head) = head {
            let refs = head.postings_for_matchers_with_concurrent(matchers, self.concurrent)?;
            for &r in refs.iter() {
                let labels_opt = match head_iso {
                    Some(iso) => head.series_has_chunks_in_range_with_isolation(
                        r,
                        self.mint,
                        self.maxt,
                        iso,
                    ),
                    None => head.series_has_chunks_in_range(r, self.mint, self.maxt),
                };
                if let Some(labels) = labels_opt {
                    set.insert(labels);
                }
            }
        }
        for blk in &blocks {
            let idx = blk.index();
            let tombr = blk.tombstones();
            let refs = blk.postings_for_matchers_with_concurrent(matchers, self.concurrent)?;
            for &r in refs.iter() {
                let (labels, chks) = idx.series(r)?;
                let tombs = tombr.get(r);
                let any_chunk = chks.iter().any(|cm| {
                    cm.overlaps_closed_interval(self.mint, self.maxt)
                        && !tombs.iter().any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
                });
                if any_chunk {
                    set.insert(labels);
                }
            }
        }
        Ok(set.into_iter().collect())
    }

    /// Returns sorted unique label names across in-order Head and overlapping Blocks matching `matchers`.
    pub fn label_names(&self, matchers: &[LabelMatcher]) -> Result<Vec<String>, QuerierError> {
        let snapshot = self.snapshot_sources(HeadSnapshotMode::InOrderOnly)?;
        let _block_guards = snapshot.block_guards;
        let blocks = snapshot.blocks;
        let _head_snap = snapshot.head;
        let head = _head_snap.as_ref().and(self.head);
        let mut set: BTreeSet<String> = BTreeSet::new();

        if let Some(head) = head {
            for name in head.label_names(matchers)? {
                set.insert(name);
            }
        }
        for blk in &blocks {
            let idx = blk.index();
            if matchers.is_empty() {
                for name in idx.label_names() {
                    set.insert(name);
                }
            } else {
                let refs = blk.postings_for_matchers_direct(matchers)?;
                for name in idx.label_names_for(&refs)? {
                    set.insert(name);
                }
            }
        }

        Ok(set.into_iter().collect())
    }

    /// Like [`Self::label_names`], truncating to `limit` when `limit` is `Some(n)` with `n > 0`.
    pub fn label_names_with_limit(
        &self,
        limit: Option<usize>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, QuerierError> {
        let mut names = self.label_names(matchers)?;
        if let Some(lim) = limit {
            if lim > 0 && names.len() > lim {
                names.truncate(lim);
            }
        }
        Ok(names)
    }

    /// Returns sorted unique label values for `name` across in-order Head and overlapping Blocks matching `matchers`.
    pub fn label_values(
        &self,
        name: &str,
        limit: Option<usize>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, QuerierError> {
        let snapshot = self.snapshot_sources(HeadSnapshotMode::InOrderOnly)?;
        let _block_guards = snapshot.block_guards;
        let blocks = snapshot.blocks;
        let _head_snap = snapshot.head;
        let head = _head_snap.as_ref().and(self.head);

        if blocks.is_empty() {
            return match head {
                Some(h) => Ok(h.label_values_with_limit(name, limit, matchers)?),
                None => Ok(Vec::new()),
            };
        }

        let mut set: BTreeSet<String> = BTreeSet::new();

        if let Some(head) = head {
            for val in head.label_values_with_limit(name, None, matchers)? {
                set.insert(val);
            }
        }
        for blk in &blocks {
            let idx = blk.index();
            if matchers.is_empty() {
                for val in idx.label_values(name)? {
                    set.insert(val);
                }
            } else if name.is_empty() {
                if !idx.label_values("")?.is_empty()
                    && matchers
                        .iter()
                        .filter(|m| m.name.is_empty())
                        .all(|m| m.matches(""))
                    && (!matchers.iter().any(|m| !m.name.is_empty())
                        || !blk.postings_for_matchers_direct(matchers)?.is_empty())
                {
                    set.insert(String::new());
                }
            } else {
                let refs = blk.postings_for_matchers_direct(matchers)?;
                for &r in refs.iter() {
                    let lbls = idx.series_labels(r)?;
                    let v = lbls.get(name);
                    if !v.is_empty() {
                        set.insert(v.to_string());
                    }
                }
            }
        }

        let mut out: Vec<String> = set.into_iter().collect();
        if let Some(lim) = limit {
            if lim > 0 && out.len() > lim {
                out.truncate(lim);
            }
        }
        Ok(out)
    }

    /// Returns a sorted map of every label name -> sorted unique label values matching `matchers`.
    pub fn label_names_and_values(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeMap<String, Vec<String>>, QuerierError> {
        let names = self.label_names(matchers)?;
        let mut out = BTreeMap::new();
        for name in names {
            let vals = self.label_values(&name, None, matchers)?;
            out.insert(name, vals);
        }
        Ok(out)
    }

    /// Returns the number of distinct values for `name` matching `matchers`.
    pub fn label_values_cardinality(
        &self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<usize, QuerierError> {
        Ok(self.label_values(name, None, matchers)?.len())
    }

    /// Returns the number of active series in Head (plus series in overlapping blocks if queried).
    pub fn series_count(&self) -> Result<usize, QuerierError> {
        let snapshot = self.snapshot_sources(HeadSnapshotMode::FullRange)?;
        let _block_guards = snapshot.block_guards;
        let blocks = snapshot.blocks;
        let _head_snap = snapshot.head;
        let mut set: BTreeSet<Labels> = BTreeSet::new();
        if let Some(head) = _head_snap.as_ref().and(self.head) {
            let all = LabelMatcher::new(crate::matcher::MatchType::RegexMatch, "__name__", ".*")
                .map_err(PostingsError::from)?;
            for &r in head.postings_for_matchers_direct_arc(&[all])?.iter() {
                if let Some(l) = head.series_labels(r) {
                    set.insert(l);
                }
            }
        }
        for blk in &blocks {
            let idx = blk.index();
            for r in idx.postings("", &[""])? {
                set.insert(idx.series_labels(r)?);
            }
        }
        Ok(set.len())
    }

    /// Fuzzy-searches label names using [`SearchHints`].
    pub fn search_label_names(
        &self,
        hints: &SearchHints,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SearchResult>, QuerierError> {
        let names = self.label_names(matchers)?;
        Ok(apply_search_hints(&names, hints)?)
    }

    /// Fuzzy-searches label values for `name` using [`SearchHints`].
    pub fn search_label_values(
        &self,
        name: &str,
        hints: &SearchHints,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<SearchResult>, QuerierError> {
        let values = self.label_values(name, None, matchers)?;
        Ok(apply_search_hints(&values, hints)?)
    }
}

/// Chunk-focused querier alias over [`TsdbQuerier`].
pub struct TsdbChunkQuerier<'a> {
    inner: TsdbQuerier<'a>,
    unordered: bool,
}

impl<'a> TsdbChunkQuerier<'a> {
    pub fn new(head: Option<&'a Head>, blocks: Vec<Arc<Block>>, mint: i64, maxt: i64) -> Self {
        Self {
            inner: TsdbQuerier::new(head, blocks, mint, maxt),
            unordered: false,
        }
    }

    pub fn new_unordered(
        head: Option<&'a Head>,
        blocks: Vec<Arc<Block>>,
        mint: i64,
        maxt: i64,
    ) -> Self {
        Self {
            inner: TsdbQuerier::new(head, blocks, mint, maxt),
            unordered: true,
        }
    }

    pub fn with_blocks_lock(
        head: Option<&'a Head>,
        blocks_lock: &'a RwLock<Vec<Arc<Block>>>,
        mint: i64,
        maxt: i64,
    ) -> Self {
        Self {
            inner: TsdbQuerier::with_blocks_lock(head, blocks_lock, mint, maxt),
            unordered: false,
        }
    }

    pub fn with_blocks_lock_unordered(
        head: Option<&'a Head>,
        blocks_lock: &'a RwLock<Vec<Arc<Block>>>,
        mint: i64,
        maxt: i64,
    ) -> Self {
        Self {
            inner: TsdbQuerier::with_blocks_lock(head, blocks_lock, mint, maxt),
            unordered: true,
        }
    }

    pub fn with_blocks_lock_and_closed(
        head: Option<&'a Head>,
        blocks_lock: &'a RwLock<Vec<Arc<Block>>>,
        closed: &'a AtomicBool,
        mint: i64,
        maxt: i64,
    ) -> Self {
        Self {
            inner: TsdbQuerier::with_blocks_lock_and_closed(head, blocks_lock, closed, mint, maxt),
            unordered: false,
        }
    }

    pub fn with_blocks_lock_unordered_and_closed(
        head: Option<&'a Head>,
        blocks_lock: &'a RwLock<Vec<Arc<Block>>>,
        closed: &'a AtomicBool,
        mint: i64,
        maxt: i64,
    ) -> Self {
        Self {
            inner: TsdbQuerier::with_blocks_lock_and_closed(head, blocks_lock, closed, mint, maxt),
            unordered: true,
        }
    }

    pub fn with_closed(mut self, closed: &'a AtomicBool) -> Self {
        self.inner = self.inner.with_closed(closed);
        self
    }

    pub fn with_last_garbage_collected_mmap_ref(
        mut self,
        last_garbage_collected_mmap_ref: &'a AtomicU64,
    ) -> Self {
        self.inner = self
            .inner
            .with_last_garbage_collected_mmap_ref(last_garbage_collected_mmap_ref);
        self
    }

    /// Sets Go `sharded` / `hints.ShardCount > 0` (`concurrent` param on
    /// `PostingsForMatchersCache.PostingsForMatchers`).
    pub fn with_concurrent(mut self, concurrent: bool) -> Self {
        self.inner = self.inner.with_concurrent(concurrent);
        self
    }

    /// Records per block querier [`BlockSelectStats`] of every chunk select into `sink`.
    pub fn with_select_stats(mut self, sink: BlockSelectStatsSink) -> Self {
        self.inner.set_select_stats(sink, None);
        self
    }

    /// Like [`Self::with_select_stats`], counting only returned series accepted by `filter`
    /// (the lazy streaming path applies its own series filter instead).
    pub fn with_filtered_select_stats(
        mut self,
        sink: BlockSelectStatsSink,
        filter: Option<SeriesStatsFilter>,
    ) -> Self {
        self.inner.set_select_stats(sink, filter);
        self
    }

    pub fn select_adaptive(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        max_source_series: usize,
        max_total_refs: usize,
    ) -> Result<Result<Vec<QueryChunkSeries>, PreparedChunkSelect>, QuerierError> {
        self.inner.select_chunks_adaptive(
            sort_series,
            matchers,
            self.unordered,
            max_source_series,
            max_total_refs,
        )
    }

    pub fn try_select_adaptive(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        max_source_series: usize,
        max_total_refs: usize,
    ) -> Result<Option<Result<Vec<QueryChunkSeries>, PreparedChunkSelect>>, QuerierError> {
        self.inner.try_select_chunks_adaptive(
            sort_series,
            matchers,
            self.unordered,
            max_source_series,
            max_total_refs,
        )
    }

    pub fn execute_prepared_select_with_matchers(
        &self,
        prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        self.inner
            .execute_prepared_select_with_matchers(prepared, matchers, sort_series)
    }

    pub fn prepare_select(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<PreparedChunkSelect, QuerierError> {
        self.inner.prepare_select_chunks(matchers, self.unordered)
    }

    pub fn execute_prepared_select(
        &self,
        prepared: PreparedChunkSelect,
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        self.inner.execute_prepared_select(prepared, sort_series)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn stream_prepared_chunks_lazy<F>(
        &self,
        prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
        series_filter: Option<&dyn Fn(&Labels) -> bool>,
        on_event: F,
    ) -> Result<(), QuerierError>
    where
        F: FnMut(LazyStreamEvent<'_>) -> Result<bool, QuerierError>,
    {
        self.inner.stream_prepared_chunks_lazy(
            prepared,
            matchers,
            sort_series,
            series_filter,
            on_event,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn stream_prepared_chunks_lazy_with_shard<F>(
        &self,
        prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
        shard: Option<(u64, u64)>,
        series_filter: Option<&dyn Fn(&Labels) -> bool>,
        on_event: F,
    ) -> Result<(), QuerierError>
    where
        F: FnMut(LazyStreamEvent<'_>) -> Result<bool, QuerierError>,
    {
        self.inner.stream_prepared_chunks_lazy_with_shard(
            prepared,
            matchers,
            sort_series,
            shard,
            series_filter,
            on_event,
        )
    }

    pub fn select(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        self.inner
            .select_chunks_with_mode(sort_series, matchers, self.unordered)
    }

    pub fn select_raw_labels(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>, QuerierError> {
        self.inner.select_raw_labels(sort_series, matchers)
    }

    pub fn label_names(&self, matchers: &[LabelMatcher]) -> Result<Vec<String>, QuerierError> {
        self.inner.label_names(matchers)
    }

    pub fn label_values(
        &self,
        name: &str,
        limit: Option<usize>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, QuerierError> {
        self.inner.label_values(name, limit, matchers)
    }
}

/// Helper to iterate samples out of an [`EncodedChunk`] slice.
pub fn chunk_to_samples(chunk: &EncodedChunk) -> Result<Vec<DecodedSample>, ChunkError> {
    decode_chunk_samples(chunk)
}

const PARALLEL_SORT_THRESHOLD: usize = 16_384;

pub(crate) fn read_sort_pool() -> &'static ScopedWorkerPool {
    static POOL: OnceLock<ScopedWorkerPool> = OnceLock::new();
    // 3 background workers + calling thread = 4 sort threads.
    POOL.get_or_init(|| ScopedWorkerPool::new("read-sort", 3))
}

#[inline]
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let limit = a.len().min(b.len());
    let mut i = 0usize;
    while i + 8 <= limit {
        let wa = u64::from_ne_bytes([
            a[i],
            a[i + 1],
            a[i + 2],
            a[i + 3],
            a[i + 4],
            a[i + 5],
            a[i + 6],
            a[i + 7],
        ]);
        let wb = u64::from_ne_bytes([
            b[i],
            b[i + 1],
            b[i + 2],
            b[i + 3],
            b[i + 4],
            b[i + 5],
            b[i + 6],
            b[i + 7],
        ]);
        if wa != wb {
            break;
        }
        i += 8;
    }
    while i < limit && a[i] == b[i] {
        i += 1;
    }
    i
}

#[inline]
fn pack_24_be(buf: &[u8; 24]) -> [u64; 3] {
    [
        u64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]),
        u64::from_be_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]]),
        u64::from_be_bytes([buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23]]),
    ]
}

#[inline]
fn extract_sort_key_24(
    b: &[u8],
    skip_bytes: usize,
    value_offset: Option<usize>,
) -> ([u64; 3], bool) {
    let mut buf = [0u8; 24];
    let mut out = 0usize;
    let mut has_nul = false;

    let mut pos = if let Some(vlen_pos) = value_offset {
        if vlen_pos >= b.len() {
            return ([0; 3], false);
        }
        let (vlen, vstart) = decode_stringlabel_size(b, vlen_pos);
        let vend = vstart + vlen;
        for &byte in &b[vstart..vend] {
            if byte == 0 {
                has_nul = true;
            }
            if out < 24 {
                buf[out] = byte;
                out += 1;
            } else {
                break;
            }
        }
        if out < 24 {
            out += 1;
        }
        vend
    } else {
        skip_bytes
    };

    while out < 24 && pos < b.len() {
        let (nlen, nstart) = decode_stringlabel_size(b, pos);
        let nend = nstart + nlen;
        for &byte in &b[nstart..nend] {
            if byte == 0 {
                has_nul = true;
            }
            if out < 24 {
                buf[out] = byte;
                out += 1;
            } else {
                break;
            }
        }
        if out >= 24 {
            break;
        }
        out += 1;
        if out >= 24 {
            break;
        }
        let (vlen, vstart) = decode_stringlabel_size(b, nend);
        let vend = vstart + vlen;
        for &byte in &b[vstart..vend] {
            if byte == 0 {
                has_nul = true;
            }
            if out < 24 {
                buf[out] = byte;
                out += 1;
            } else {
                break;
            }
        }
        if out < 24 {
            out += 1;
        }
        pos = vend;
    }

    (pack_24_be(&buf), has_nul)
}

#[inline(always)]
fn cmp_raw_stringlabels_from(a: &[u8], b: &[u8], pair_offset: usize) -> Ordering {
    let mut pa = pair_offset;
    let mut pb = pair_offset;
    while pa < a.len() && pb < b.len() {
        let ba = a[pa];
        let bb = b[pb];
        if ba < 128 && bb < 128 {
            let an_len = ba as usize;
            let bn_len = bb as usize;
            let an_start = pa + 1;
            let bn_start = pb + 1;
            let an_end = an_start + an_len;
            let bn_end = bn_start + bn_len;
            let vba = a[an_end];
            let vbb = b[bn_end];
            if vba < 128 && vbb < 128 {
                let av_end = an_end + 1 + vba as usize;
                let bv_end = bn_end + 1 + vbb as usize;
                // Fast skip if the entire (name, val) pair including lengths is byte-identical.
                if av_end - pa == bv_end - pb && a[pa..av_end] == b[pb..bv_end] {
                    pa = av_end;
                    pb = bv_end;
                    continue;
                }
                match a[an_start..an_end].cmp(&b[bn_start..bn_end]) {
                    Ordering::Equal => {
                        match a[an_end + 1..av_end].cmp(&b[bn_end + 1..bv_end]) {
                            Ordering::Equal => {
                                pa = av_end;
                                pb = bv_end;
                                continue;
                            }
                            ord => return ord,
                        }
                    }
                    ord => return ord,
                }
            }
        }
        let (an_len, an_start) = decode_stringlabel_size(a, pa);
        let (bn_len, bn_start) = decode_stringlabel_size(b, pb);
        let an_end = an_start + an_len;
        let bn_end = bn_start + bn_len;
        match a[an_start..an_end].cmp(&b[bn_start..bn_end]) {
            Ordering::Equal => {}
            ord => return ord,
        }
        let (av_len, av_start) = decode_stringlabel_size(a, an_end);
        let (bv_len, bv_start) = decode_stringlabel_size(b, bn_end);
        let av_end = av_start + av_len;
        let bv_end = bv_start + bv_len;
        match a[av_start..av_end].cmp(&b[bv_start..bv_end]) {
            Ordering::Equal => {}
            ord => return ord,
        }
        pa = av_end;
        pb = bv_end;
    }
    (pa < a.len()).cmp(&(pb < b.len()))
}

/// Packs up to 16 bytes of values from uniform-schema columns where `varying_mask` has bit `j` set.
/// Returns `(key, has_nul)`.
#[inline]
fn extract_uniform_varying_key_24(
    b: &[u8],
    skip_bytes: usize,
    varying_mask: u32,
) -> ([u64; 3], bool) {
    let mut buf = [0u8; 24];
    let mut out = 0usize;
    let mut has_nul = false;
    let mut pos = skip_bytes;
    let mut col = 0u32;
    while out < 24 && pos < b.len() {
        let (nlen, nstart) = decode_stringlabel_size(b, pos);
        let nend = nstart + nlen;
        let (vlen, vstart) = decode_stringlabel_size(b, nend);
        let vend = vstart + vlen;
        if (varying_mask & (1u32 << col)) != 0 {
            for &byte in &b[vstart..vend] {
                if byte == 0 {
                    has_nul = true;
                }
                if out < 24 {
                    buf[out] = byte;
                    out += 1;
                } else {
                    break;
                }
            }
            if out < 24 {
                out += 1;
            }
        }
        pos = vend;
        col += 1;
    }
    (pack_24_be(&buf), has_nul)
}

#[inline]
fn cmp_sort_keys_raw(
    slices: &[&[u8]],
    skip_bytes: usize,
    a: &([u64; 3], u32),
    b: &([u64; 3], u32),
) -> Ordering {
    if a.0 != b.0 {
        return a.0.cmp(&b.0);
    }
    cmp_raw_stringlabels_from(slices[a.1 as usize], slices[b.1 as usize], skip_bytes)
}

fn merge_sorted_keys_raw(
    slices: &[&[u8]],
    skip_bytes: usize,
    left: &[([u64; 3], u32)],
    right: &[([u64; 3], u32)],
    out: &mut Vec<([u64; 3], u32)>,
) {
    out.clear();
    out.reserve(left.len() + right.len());
    let mut i = 0usize;
    let mut j = 0usize;
    while i < left.len() && j < right.len() {
        if cmp_sort_keys_raw(slices, skip_bytes, &left[i], &right[j]) != Ordering::Greater {
            out.push(left[i]);
            i += 1;
        } else {
            out.push(right[j]);
            j += 1;
        }
    }
    if i < left.len() {
        out.extend_from_slice(&left[i..]);
    }
    if j < right.len() {
        out.extend_from_slice(&right[j..]);
    }
}

/// Returns sorted index permutation `Vec<u32>` in exact `Labels::cmp` lexicographical order
/// without copying or swapping `items` (returns `None` if `items.len() > u32::MAX` or any
/// label contains embedded NUL bytes, letting the caller fall back to in-place sort).
fn sort_label_indices<T, F>(items: &[T], get_labels: F) -> Option<Vec<u32>>
where
    T: Sync,
    F: Fn(&T) -> &Labels + Sync,
{
    let n = items.len();
    if n <= 1 || u32::try_from(n).is_err() {
        return None;
    }
    let mut keys: Vec<([u64; 3], u32)>;
    let mut any_nul = false;
    {
        // Collect raw byte slices once so the entire sort avoids entry_labels() / Bytes::as_slice().
        let slices: Vec<&[u8]> = items.iter().map(|it| get_labels(it).as_bytes()).collect();

        // 1. Find longest common byte prefix across all entries in 1 linear pass.
        let b0 = slices[0];
        let mut lcp = b0.len();
        for &s in &slices[1..] {
            lcp = common_prefix_len(&b0[..lcp], s);
            if lcp == 0 {
                break;
            }
        }

        // 2. Snap `lcp` down to the largest pair boundary in `b0` and check if the next
        //    label's name is also identical across all entries.
        let mut skip_bytes = 0usize;
        let mut pos = 0usize;
        let mut value_offset: Option<usize> = None;
        while pos < b0.len() {
            let (nlen, nstart) = decode_stringlabel_size(b0, pos);
            let name_end = nstart + nlen;
            let (vlen, vstart) = decode_stringlabel_size(b0, name_end);
            let next_pos = vstart + vlen;
            if next_pos <= lcp {
                skip_bytes = next_pos;
                pos = next_pos;
            } else {
                if name_end <= lcp {
                    value_offset = Some(name_end);
                }
                break;
            }
        }

        // 2b. Fast uniform-schema check: if all series share identical label names after `skip_bytes`
        //     (<= 32 labels), pack ONLY varying values into `u128` (skipping identical columns like
        //     `cluster="prod-eu"`, `env="prod"`, `namespace="ns-3"`).
        let mut schema: SmallVec<[(&[u8], &[u8]); 16]> = SmallVec::new();
        let mut p = skip_bytes;
        while p < b0.len() && schema.len() < 32 {
            let (nlen, nstart) = decode_stringlabel_size(b0, p);
            let nend = nstart + nlen;
            let (vlen, vstart) = decode_stringlabel_size(b0, nend);
            let vend = vstart + vlen;
            schema.push((&b0[nstart..nend], &b0[vstart..vend]));
            p = vend;
        }
        let mut uniform_schema = p == b0.len() && !schema.is_empty();
        let mut varying_mask = 0u32;
        if uniform_schema {
            for &s in &slices[1..] {
                let mut sp = skip_bytes;
                let mut ok = true;
                for (col, &(sname, sval0)) in schema.iter().enumerate() {
                    if sp >= s.len() {
                        ok = false;
                        break;
                    }
                    let (nlen, nstart) = decode_stringlabel_size(s, sp);
                    let nend = nstart + nlen;
                    if &s[nstart..nend] != sname {
                        ok = false;
                        break;
                    }
                    let (vlen, vstart) = decode_stringlabel_size(s, nend);
                    let vend = vstart + vlen;
                    if &s[vstart..vend] != sval0 {
                        varying_mask |= 1u32 << col;
                    }
                    sp = vend;
                }
                if !ok || sp != s.len() {
                    uniform_schema = false;
                    break;
                }
            }
        }
        drop(schema);

        // 3. Extract 24-byte `[u64; 3]` sort keys once per entry.
        keys = if uniform_schema {
            slices
                .iter()
                .enumerate()
                .map(|(i, &s)| {
                    let (k, has_nul) = extract_uniform_varying_key_24(s, skip_bytes, varying_mask);
                    if has_nul {
                        any_nul = true;
                    }
                    (k, i as u32)
                })
                .collect()
        } else {
            slices
                .iter()
                .enumerate()
                .map(|(i, &s)| {
                    let (k, has_nul) = extract_sort_key_24(s, skip_bytes, value_offset);
                    if has_nul {
                        any_nul = true;
                    }
                    (k, i as u32)
                })
                .collect()
        };

        if !any_nul {
            let slices_ref: &[&[u8]] = &slices;
            if n < PARALLEL_SORT_THRESHOLD {
                keys.sort_unstable_by(|a, b| cmp_sort_keys_raw(slices_ref, skip_bytes, a, b));
            } else {
                let pool = read_sort_pool();
                let q1_end = n / 4;
                let q2_end = n / 2;
                let q3_end = (3 * n) / 4;
                let (s0, rest) = keys.split_at_mut(q1_end);
                let (s1, rest) = rest.split_at_mut(q2_end - q1_end);
                let (s2, s3) = rest.split_at_mut(q3_end - q2_end);

                let slice_tasks = [
                    Mutex::new(Some(s0)),
                    Mutex::new(Some(s1)),
                    Mutex::new(Some(s2)),
                    Mutex::new(Some(s3)),
                ];
                pool.scoped_execute(4, |tid| {
                    if let Some(slice) = slice_tasks[tid].lock().take() {
                        slice.sort_unstable_by(|a, b| {
                            cmp_sort_keys_raw(slices_ref, skip_bytes, a, b)
                        });
                    }
                });

                let (left_in, right_in) = keys.split_at(q2_end);
                let (q0, q1) = left_in.split_at(q1_end);
                let (q2, q3) = right_in.split_at(q3_end - q2_end);

                let mut left = Vec::with_capacity(q0.len() + q1.len());
                let mut right = Vec::with_capacity(q2.len() + q3.len());
                {
                    let pairs = [(q0, q1), (q2, q3)];
                    let outs = [Mutex::new(Some(&mut left)), Mutex::new(Some(&mut right))];
                    pool.scoped_execute(2, |tid| {
                        if let Some(out) = outs[tid].lock().take() {
                            merge_sorted_keys_raw(
                                slices_ref,
                                skip_bytes,
                                pairs[tid].0,
                                pairs[tid].1,
                                out,
                            );
                        }
                    });
                }
                merge_sorted_keys_raw(slices_ref, skip_bytes, &left, &right, &mut keys);
            }
        }
    }
    if any_nul {
        return None;
    }
    Some(keys.into_iter().map(|(_, idx)| idx).collect())
}

/// Sorts `items` in exact `Labels::cmp` lexicographical order, skipping the shared
/// pair-aligned prefix across all entries, packing a 24-byte `[u64; 3]` prefix key
/// for fast comparisons, and sorting in parallel across 4 threads when large.
pub(crate) fn sort_by_labels<T, F>(items: &mut [T], get_labels: F)
where
    T: Sync,
    F: Fn(&T) -> &Labels + Sync,
{
    let n = items.len();
    if n <= 1 {
        return;
    }
    let Some(mut order) = sort_label_indices(items, &get_labels) else {
        items.sort_unstable_by(|a, b| get_labels(a).cmp(get_labels(b)));
        return;
    };

    // Permute `items` in-place via cycle decomposition (0 extra allocations).
    for i in 0..n {
        let mut cur = i;
        loop {
            let next = order[cur] as usize;
            order[cur] = cur as u32;
            if next == i {
                break;
            }
            items.swap(cur, next);
            cur = next;
        }
    }
}

#[cfg(test)]
fn sort_lazy_entries(entries: &mut [LazySeriesEntry]) {
    sort_by_labels(entries, LazySeriesEntry::entry_labels);
}

/// Tombstone-filtering sample iterator (`tsdb.DeletedIterator`).
pub struct DeletedIterator {
    samples: Vec<DecodedSample>,
    intervals: crate::tombstones::Intervals,
    pos: Option<usize>,
    iv_idx: usize,
}

impl DeletedIterator {
    pub fn new(chunk: &EncodedChunk, intervals: crate::tombstones::Intervals) -> Result<Self, ChunkError> {
        let samples = crate::compact::decode_chunk_samples(chunk)?;
        Ok(Self {
            samples,
            intervals,
            pos: None,
            iv_idx: 0,
        })
    }

    pub fn reset(&mut self, chunk: &EncodedChunk, intervals: crate::tombstones::Intervals) -> Result<(), ChunkError> {
        self.samples = crate::compact::decode_chunk_samples(chunk)?;
        self.intervals = intervals;
        self.pos = None;
        self.iv_idx = 0;
        Ok(())
    }

    fn skip_deleted(&mut self, mut i: usize) -> Option<usize> {
        while i < self.samples.len() {
            let t = self.samples[i].t;
            while self.iv_idx < self.intervals.len() && self.intervals[self.iv_idx].maxt < t {
                self.iv_idx += 1;
            }
            if self.iv_idx < self.intervals.len() && self.intervals[self.iv_idx].in_bounds(t) {
                i += 1;
                continue;
            }
            return Some(i);
        }
        None
    }

    pub fn next_sample(&mut self) -> Option<&DecodedSample> {
        let start = match self.pos {
            None => 0,
            Some(i) => i + 1,
        };
        self.pos = self.skip_deleted(start);
        self.pos.map(|i| &self.samples[i])
    }

    pub fn seek(&mut self, t: i64) -> Option<&DecodedSample> {
        let start = match self.pos {
            None => self.samples.partition_point(|s| s.t < t),
            Some(cur) => {
                if self.samples[cur].t >= t {
                    return Some(&self.samples[cur]);
                }
                cur + 1 + self.samples[cur + 1..].partition_point(|s| s.t < t)
            }
        };
        self.pos = self.skip_deleted(start);
        self.pos.map(|i| &self.samples[i])
    }

    pub fn at(&self) -> Option<&DecodedSample> {
        self.pos.map(|i| &self.samples[i])
    }
}

impl<'a> TsdbQuerier<'a> {
    /// Evaluates `select` using a [`crate::index::IndexLookupPlanner`]: falls back to all `matchers`
    /// if the planner errors, or filters index postings by `index_matchers` and post-filters by
    /// `scan_matchers` (`querier.go`).
    pub fn select_with_planner(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        planner: &dyn crate::index::IndexLookupPlanner,
    ) -> Result<Vec<QuerySeries>, QuerierError> {
        let plan = crate::index::IndexOnlyLookupPlan(matchers.to_vec());
        match planner.plan_index_lookup(&plan) {
            Ok(concrete) => {
                let mut res = self.select(sort_series, &concrete.index_matchers)?;
                if !concrete.scan_matchers.is_empty() {
                    res.retain(|s| concrete.scan_matchers.iter().all(|m| m.matches(s.labels.get(&m.name))));
                }
                Ok(res)
            }
            Err(_) => self.select(sort_series, matchers),
        }
    }
}

impl<'a> TsdbChunkQuerier<'a> {
    /// Chunk-querier counterpart of [`TsdbQuerier::select_with_planner`].
    pub fn select_with_planner(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        planner: &dyn crate::index::IndexLookupPlanner,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        let plan = crate::index::IndexOnlyLookupPlan(matchers.to_vec());
        match planner.plan_index_lookup(&plan) {
            Ok(concrete) => {
                let mut res = self.select(sort_series, &concrete.index_matchers)?;
                if !concrete.scan_matchers.is_empty() {
                    res.retain(|s| concrete.scan_matchers.iter().all(|m| m.matches(s.labels.get(&m.name))));
                }
                Ok(res)
            }
            Err(_) => self.select(sort_series, matchers),
        }
    }
}

pub fn sort_labels(labels: &mut [Labels]) {
    sort_by_labels(labels, |l| l);
}

#[cfg(test)]
mod sort_tests {
    use super::*;
    use bytes::Bytes;

    fn make_entry(lbls: Labels) -> LazySeriesEntry {
        LazySeriesEntry {
            labels: lbls,
            head_entry: None,
            head_snapshot: None,
            source: LazyChunkSource::Head(0),
            chunk_count: 1,
        }
    }

    #[test]
    fn sort_lazy_entries_matches_labels_cmp_edge_cases_and_parallel() {
        // 1. Shared prefix + varying label counts + >255B names/values + embedded NUL bytes
        let long_name_a = format!("lbl_{}", "x".repeat(300));
        let long_name_b = format!("lbl_{}", "y".repeat(300));
        let long_a = format!("val-{}", "a".repeat(300));
        let long_b = format!("val-{}", "b".repeat(300));
        let mut raw_nul_1 = Vec::new();
        crate::labels::append_stringlabel_pair(&mut raw_nul_1, b"__name__", b"m");
        crate::labels::append_stringlabel_pair(&mut raw_nul_1, b"pod", b"a\0c");
        let mut raw_nul_2 = Vec::new();
        crate::labels::append_stringlabel_pair(&mut raw_nul_2, b"__name__", b"m");
        crate::labels::append_stringlabel_pair(&mut raw_nul_2, b"pod", b"a\0");
        let mut raw_nul_3 = Vec::new();
        crate::labels::append_stringlabel_pair(&mut raw_nul_3, b"__name__", b"m");
        crate::labels::append_stringlabel_pair(&mut raw_nul_3, b"pod", b"a");
        let mut raw_nul_4 = Vec::new();
        crate::labels::append_stringlabel_pair(&mut raw_nul_4, b"__name__", b"m");
        crate::labels::append_stringlabel_pair(&mut raw_nul_4, b"pod", b"");

        let mut samples = vec![
            Labels::from_pairs([("__name__", "m"), ("pod", "pod-99999999")]),
            Labels::from_pairs([("__name__", "m"), ("pod", "pod-10000000")]),
            Labels::from_pairs([("__name__", "m"), ("pod", "pod-10000001")]),
            Labels::from_pairs([("__name__", "m")]),
            Labels::from_pairs([("__name__", "m"), ("extra", "1"), ("pod", "pod-0")]),
            Labels::from_pairs([("__name__", "m"), ("pod", &long_b)]),
            Labels::from_pairs([("__name__", "m"), ("pod", &long_a)]),
            Labels::from_pairs([("__name__", "m"), (&long_name_b, "1")]),
            Labels::from_pairs([("__name__", "m"), (&long_name_a, "2")]),
            Labels::from_pairs([("__name__", "m"), (&long_name_a, "1")]),
            Labels::from_raw_stringlabels_bytes(Bytes::from(raw_nul_1)),
            Labels::from_raw_stringlabels_bytes(Bytes::from(raw_nul_2)),
            Labels::from_raw_stringlabels_bytes(Bytes::from(raw_nul_3)),
            Labels::from_raw_stringlabels_bytes(Bytes::from(raw_nul_4)),
        ];
        samples.reverse();

        let mut expected: Vec<LazySeriesEntry> = samples.iter().cloned().map(make_entry).collect();
        expected.sort_unstable_by(|a, b| a.labels.cmp(&b.labels));

        let mut actual: Vec<LazySeriesEntry> = samples.into_iter().map(make_entry).collect();
        sort_lazy_entries(&mut actual);
        assert_eq!(
            actual.iter().map(|e| &e.labels).collect::<Vec<_>>(),
            expected.iter().map(|e| &e.labels).collect::<Vec<_>>()
        );

        // 2. Parallel sort (> PARALLEL_SORT_THRESHOLD) with 10-label shared prefix + ties
        let total = PARALLEL_SORT_THRESHOLD + 4_000;
        let mut big_actual = Vec::with_capacity(total);
        for i in (0..total).rev() {
            let pod = format!("pod-{:04}-suffix-{}", (i * 7919) % 500, i);
            let lbls = Labels::from_pairs([
                ("__name__", "container_cpu_usage_seconds_total"),
                ("cluster", "prod-eu"),
                ("container", "app"),
                ("env", "prod"),
                ("namespace", &format!("ns-{}", i % 10)),
                ("pod", &pod),
                ("series", &format!("{i:06}")),
            ]);
            big_actual.push(make_entry(lbls));
        }
        let mut big_expected: Vec<LazySeriesEntry> = big_actual
            .iter()
            .map(|e| make_entry(e.labels.clone()))
            .collect();
        big_expected.sort_unstable_by(|a, b| a.labels.cmp(&b.labels));

        sort_lazy_entries(&mut big_actual);
        for (idx, (a, e)) in big_actual.iter().zip(big_expected.iter()).enumerate() {
            assert_eq!(a.labels, e.labels, "mismatch at sorted index {idx}");
        }
    }
}

#[cfg(test)]
mod wave3_tests {
    use super::*;
    use crate::head::HeadOptions;

    #[test]
    fn label_discovery_uses_in_order_head_bounds_not_ooo_bounds() {
        // W3-04: When `[mint, maxt]` overlaps `[min_ooo_time, max_ooo_time]` but does NOT overlap
        // `[head.min_time(), head.max_time()]`, `label_names`, `label_values`, `search_label_names`,
        // and `search_label_values` must NOT return Head labels/values (`ooo_head_read.go:648-680`),
        // while `select` / `select_raw_labels` MUST still find the OOO series.
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 3_600_000,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        {
            let mut app = head.appender();
            app.append_float(
                None,
                &[("__name__", "m_head"), ("head_lbl", "hv")],
                1_000_000,
                1.0,
            )
            .unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float(
                None,
                &[("__name__", "m_head"), ("head_lbl", "hv")],
                500_000,
                2.0,
            )
            .unwrap();
            app.commit().unwrap();
        }
        assert_eq!(head.min_time(), 1_000_000);
        assert_eq!(head.min_ooo_time(), 500_000);

        // Pre-Head.MinTime() OOO-only window [400_000, 600_000]:
        let q_ooo = TsdbQuerier::new(Some(&head), Vec::new(), 400_000, 600_000);
        assert!(q_ooo.label_names(&[]).unwrap().is_empty());
        assert!(q_ooo.label_values("head_lbl", None, &[]).unwrap().is_empty());
        assert!(q_ooo
            .search_label_names(&SearchHints::default(), &[])
            .unwrap()
            .is_empty());
        assert!(q_ooo
            .search_label_values("head_lbl", &SearchHints::default(), &[])
            .unwrap()
            .is_empty());
        let all_m = [LabelMatcher::new(crate::matcher::MatchType::RegexMatch, "__name__", ".+").unwrap()];
        assert_eq!(q_ooo.select(true, &all_m).unwrap().len(), 1);
        assert_eq!(q_ooo.select_raw_labels(true, &all_m).unwrap().len(), 1);

        // Trailing window strictly after Head.MaxTime() [2_000_000, 3_000_000] also returns empty
        // for label discovery (matching Go headIndexReader.{LabelNames,LabelValues} in head_read.go:88-115).
        let q_after = TsdbQuerier::new(Some(&head), Vec::new(), 2_000_000, 3_000_000);
        assert!(q_after.label_names(&[]).unwrap().is_empty());
        assert!(q_after.label_values("head_lbl", None, &[]).unwrap().is_empty());
        assert!(q_after
            .search_label_names(&SearchHints::default(), &[])
            .unwrap()
            .is_empty());
        assert!(q_after
            .search_label_values("head_lbl", &SearchHints::default(), &[])
            .unwrap()
            .is_empty());

        // Overlapping in-order Head [1_000_000, 1_000_000]:
        let q_ino = TsdbQuerier::new(Some(&head), Vec::new(), 1_000_000, 1_000_000);
        assert_eq!(q_ino.label_names(&[]).unwrap(), vec!["__name__", "head_lbl"]);
        assert_eq!(q_ino.label_values("head_lbl", None, &[]).unwrap(), vec!["hv"]);
        head.close().unwrap();
    }
}

#[cfg(test)]
mod wave4_tests {
    use super::*;
    use crate::block::BlockMetaCompaction;
    use crate::chunkenc::{
        CounterResetHeader, CounterResetHint, Encoding, FloatHistogram, Histogram, HistogramSpan,
    };
    use crate::chunks_block::DEFAULT_CHUNK_SEGMENT_SIZE;
    use crate::compact::{
        encode_samples_into_chunks_with_encodings, write_block_from_series, CompactionSeries,
        SampleValue,
    };
    use crate::head::HeadOptions;
    use crate::matcher::MatchType;
    use crate::record::{RecordEncoder, Tombstone};
    use crate::tombstones::{write_tombstones_file, Interval, MemTombstones};
    use crate::wlog::{CompressionType, WlogWriter};

    fn make_ih(d0: i64, d1: i64, sum: f64, schema: i32, hint: CounterResetHint) -> Histogram {
        let b0 = d0;
        let b1 = d0 + d1;
        let count = (1 + b0 + b1) as u64;
        Histogram {
            count,
            zero_count: 1,
            zero_threshold: 0.001,
            sum,
            schema,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![d0, d1],
            negative_spans: Vec::new(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
            counter_reset_hint: hint,
        }
    }

    fn make_fh(b0: f64, b1: f64, sum: f64, schema: i32, hint: CounterResetHint) -> FloatHistogram {
        let count = 0.5 + b0 + b1;
        FloatHistogram {
            count,
            zero_count: 0.5,
            zero_threshold: 0.001,
            sum,
            schema,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 2,
            }],
            positive_buckets: vec![b0, b1],
            negative_spans: Vec::new(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
            counter_reset_hint: hint,
        }
    }

    #[test]
    fn w4_01_partial_tombstones_block_and_head_wal_preserve_encoding_and_disable_trimming() {
        let dir = tempfile::tempdir().unwrap();

        // 1. ULID block with Xor2, HistogramSt, and FloatHistogramSt chunks + partial tombstones.
        let blk_dir = dir.path().join("blocks");
        std::fs::create_dir_all(&blk_dir).unwrap();
        let lbl_x2 = Labels::from_pairs(vec![("__name__", "s_x2")]);
        let lbl_hst = Labels::from_pairs(vec![("__name__", "s_hst")]);
        let lbl_gone = Labels::from_pairs(vec![("__name__", "s_gone")]);

        let chks_x2 = encode_samples_into_chunks_with_encodings(
            &[
                DecodedSample {
                    st: 500,
                    t: 1_000,
                    value: SampleValue::Float(10.0),
                },
                DecodedSample {
                    st: 500,
                    t: 2_000,
                    value: SampleValue::Float(20.0),
                },
                DecodedSample {
                    st: 500,
                    t: 3_000,
                    value: SampleValue::Float(30.0),
                },
                DecodedSample {
                    st: 500,
                    t: 4_000,
                    value: SampleValue::Float(40.0),
                },
            ],
            120,
            Encoding::Xor2,
            Encoding::HistogramSt,
            Encoding::FloatHistogramSt,
        )
        .unwrap();

        let chks_hst = encode_samples_into_chunks_with_encodings(
            &[
                DecodedSample {
                    st: 500,
                    t: 1_000,
                    value: SampleValue::Histogram(Box::new(make_ih(
                        10,
                        2,
                        10.0,
                        2,
                        CounterResetHint::CounterReset,
                    ))),
                },
                DecodedSample {
                    st: 500,
                    t: 2_000,
                    value: SampleValue::Histogram(Box::new(make_ih(
                        12,
                        2,
                        12.0,
                        2,
                        CounterResetHint::NotCounterReset,
                    ))),
                },
                DecodedSample {
                    st: 500,
                    t: 4_000,
                    value: SampleValue::Histogram(Box::new(make_ih(
                        14,
                        2,
                        14.0,
                        2,
                        CounterResetHint::NotCounterReset,
                    ))),
                },
            ],
            120,
            Encoding::Xor2,
            Encoding::HistogramSt,
            Encoding::FloatHistogramSt,
        )
        .unwrap();

        let chks_gone = encode_samples_into_chunks_with_encodings(
            &[
                DecodedSample {
                    st: 0,
                    t: 1_000,
                    value: SampleValue::Float(1.0),
                },
                DecodedSample {
                    st: 0,
                    t: 4_000,
                    value: SampleValue::Float(4.0),
                },
            ],
            120,
            Encoding::Xor,
            Encoding::Histogram,
            Encoding::FloatHistogram,
        )
        .unwrap();

        let meta = write_block_from_series(
            &blk_dir,
            1_000,
            4_001,
            vec![
                CompactionSeries {
                    labels: lbl_gone.clone(),
                    chunks: chks_gone,
                    tombstones: Vec::new(),
                },
                CompactionSeries {
                    labels: lbl_hst.clone(),
                    chunks: chks_hst,
                    tombstones: Vec::new(),
                },
                CompactionSeries {
                    labels: lbl_x2.clone(),
                    chunks: chks_x2,
                    tombstones: Vec::new(),
                },
            ],
            BlockMetaCompaction::default(),
            false,
            DEFAULT_CHUNK_SEGMENT_SIZE,
        )
        .unwrap()
        .unwrap();

        // Write partial tombstones directly on disk to simulate post-compaction db.Delete().
        let block_path = blk_dir.join(meta.ulid.to_string());
        let blk_opened = Block::open(&block_path).unwrap();
        let ref_x2 = blk_opened
            .postings_for_matchers(&[LabelMatcher::new(MatchType::Equal, "__name__", "s_x2").unwrap()])
            .unwrap()[0];
        let ref_hst = blk_opened
            .postings_for_matchers(&[LabelMatcher::new(MatchType::Equal, "__name__", "s_hst").unwrap()])
            .unwrap()[0];
        let ref_gone = blk_opened
            .postings_for_matchers(&[LabelMatcher::new(MatchType::Equal, "__name__", "s_gone").unwrap()])
            .unwrap()[0];
        drop(blk_opened);

        let tombs_map = MemTombstones::new();
        tombs_map.add_interval(ref_x2, Interval::new(1_500, 3_500));
        tombs_map.add_interval(ref_hst, Interval::new(1_500, 2_500));
        // Two disjoint intervals that together delete every sample of s_gone without being a subrange of [1000, 4000]
        tombs_map.add_interval(ref_gone, Interval::new(900, 1_100));
        tombs_map.add_interval(ref_gone, Interval::new(3_900, 4_100));
        write_tombstones_file(&block_path, &tombs_map).unwrap();

        let blk = Arc::new(Block::open(&block_path).unwrap());

        // Query narrow window [3_800, 4_200] -> DisableTrimming must keep t=1_000 and t=4_000,
        // omit deleted [1_500, 3_500], preserve Xor2 & HistogramSt, and omit s_gone completely.
        let q = TsdbQuerier::new(None, vec![blk], 3_800, 4_200);
        let all_m = [LabelMatcher::new(MatchType::RegexMatch, "__name__", ".+").unwrap()];
        let res = q.select_chunks(true, &all_m).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].labels.get("__name__"), "s_hst");
        assert_eq!(res[0].chunks.len(), 1);
        assert_eq!(
            res[0].chunks[0].chunk.as_ref().unwrap().encoding(),
            Encoding::HistogramSt
        );
        let hst_samples = chunk_to_samples(res[0].chunks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(
            hst_samples.iter().map(|s| (s.st, s.t)).collect::<Vec<_>>(),
            vec![(500, 1_000), (500, 4_000)]
        );

        assert_eq!(res[1].labels.get("__name__"), "s_x2");
        assert_eq!(res[1].chunks.len(), 1);
        assert_eq!(
            res[1].chunks[0].chunk.as_ref().unwrap().encoding(),
            Encoding::Xor2
        );
        let x2_samples = chunk_to_samples(res[1].chunks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(
            x2_samples
                .iter()
                .map(|s| (s.st, s.t, s.value.clone()))
                .collect::<Vec<_>>(),
            vec![
                (500, 1_000, SampleValue::Float(10.0)),
                (500, 4_000, SampleValue::Float(40.0)),
            ]
        );

        // Verify lazy stream matches eager select_chunks!
        let prep = q.prepare_select_chunks(&all_m, false).unwrap();
        let mut lazy_series = Vec::new();
        let mut lazy_chunks: Vec<Vec<ChunkMeta>> = Vec::new();
        q.stream_prepared_chunks_lazy(prep, &all_m, true, None, |ev| {
            match ev {
                LazyStreamEvent::Series(l, c) => lazy_series.push((l.clone(), c)),
                LazyStreamEvent::SeriesEnd => {}
                LazyStreamEvent::SeriesChunks(_, chks) => lazy_chunks.push(chks),
            }
            Ok(true)
        })
        .unwrap();
        assert_eq!(lazy_series.len(), 2);
        assert_eq!(lazy_series[0].1, 1);
        assert_eq!(lazy_series[1].1, 1);
        assert_eq!(lazy_chunks[0][0].min_time, 1_000);
        assert_eq!(lazy_chunks[0][0].max_time, 4_000);

        // 2. Replayed Head WAL partial tombstone.
        let head_dir = dir.path().join("head");
        let ref_id = {
            let opts = HeadOptions {
                float_chunk_encoding: Encoding::Xor2,
                ..HeadOptions::default()
            };
            let head = Head::open(&head_dir, opts, i64::MIN).unwrap();
            let r = {
                let mut app = head.appender();
                let mut r = None;
                for (t, v) in [(1_000, 1.0), (2_000, 2.0), (3_000, 3.0), (4_000, 4.0)] {
                    r = Some(
                        app.append_float(r, &[("__name__", "hd_part")], t, v)
                            .unwrap(),
                    );
                }
                app.commit().unwrap();
                r.unwrap()
            };
            head.close().unwrap();
            r
        };
        {
            let mut wal = WlogWriter::with_segment_size(
                head_dir.join("wal"),
                128 * 1024,
                CompressionType::None,
            )
            .unwrap();
            let rec = RecordEncoder::new(false).tombstones(
                &[Tombstone {
                    ref_id,
                    mint: 1_500,
                    maxt: 3_500,
                }],
                Vec::new(),
            );
            wal.log(&[&rec]).unwrap();
            wal.close().unwrap();
        }
        let head = Head::open(
            &head_dir,
            HeadOptions {
                float_chunk_encoding: Encoding::Xor2,
                ..HeadOptions::default()
            },
            i64::MIN,
        )
        .unwrap();
        let qh = TsdbQuerier::new(Some(&head), Vec::new(), 3_800, 4_200);
        let m = [LabelMatcher::new(MatchType::Equal, "__name__", "hd_part").unwrap()];
        let hres = qh.select_chunks(true, &m).unwrap();
        assert_eq!(hres.len(), 1);
        assert_eq!(
            hres[0].chunks[0].chunk.as_ref().unwrap().encoding(),
            Encoding::Xor2
        );
        let hsamps = chunk_to_samples(hres[0].chunks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(
            hsamps.iter().map(|s| s.t).collect::<Vec<_>>(),
            vec![1_000, 4_000]
        );
        head.close().unwrap();
    }

    #[test]
    fn w4_02_overlapping_ooo_and_in_order_head_no_trimming_and_gap_queries() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 50_000,
            ooo_time_window_ms: 2 * 3_600_000,
            float_chunk_encoding: Encoding::Xor2,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        // In-order chunk 1: [10_000, 30_000]; OOO chunk: [12_000, 28_000].
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "gap_m")], 10_000, 1.0)
                .unwrap();
            app.append_float(None, &[("__name__", "gap_m")], 30_000, 4.0)
                .unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "gap_m")], 12_000, 2.0)
                .unwrap();
            app.append_float(None, &[("__name__", "gap_m")], 28_000, 3.0)
                .unwrap();
            app.commit().unwrap();
        }

        // Non-overlapping later in-order chunk 2: [60_000, 70_000] (past chunk_range_ms=50_000).
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "gap_m")], 60_000, 5.0)
                .unwrap();
            app.append_float(None, &[("__name__", "gap_m")], 70_000, 6.0)
                .unwrap();
            app.commit().unwrap();
        }

        // Query a gap window [18_000, 22_000] inside the overlapping cluster [10_000..30_000]:
        // Must return 1 series with all 4 samples [10_000, 12_000, 28_000, 30_000] (lookback preserved!).
        let q_gap = TsdbQuerier::new(Some(&head), Vec::new(), 18_000, 22_000);
        let m = [LabelMatcher::new(MatchType::Equal, "__name__", "gap_m").unwrap()];
        let res_gap = q_gap.select_chunks(true, &m).unwrap();
        assert_eq!(res_gap.len(), 1);
        assert_eq!(res_gap[0].chunks.len(), 1);
        let samps = chunk_to_samples(res_gap[0].chunks[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(
            samps.iter().map(|s| s.t).collect::<Vec<_>>(),
            vec![10_000, 12_000, 28_000, 30_000]
        );

        // Verify lazy stream matches!
        let prep = q_gap.prepare_select_chunks(&m, false).unwrap();
        let mut lazy_cnt = 0u32;
        let mut lazy_chks = Vec::new();
        q_gap
            .stream_prepared_chunks_lazy(prep, &m, true, None, |ev| {
                match ev {
                    LazyStreamEvent::Series(_, c) => lazy_cnt = c,
                    LazyStreamEvent::SeriesChunks(_, cs) => lazy_chks = cs,
                    LazyStreamEvent::SeriesEnd => {}
                }
                Ok(true)
            })
            .unwrap();
        assert_eq!(lazy_cnt, 1);
        assert_eq!(lazy_chks.len(), 1);
        assert_eq!(lazy_chks[0].min_time, 10_000);
        assert_eq!(lazy_chks[0].max_time, 30_000);

        head.close().unwrap();
    }

    #[test]
    fn w4_04_histogram_rollover_preserves_counter_reset_header_all_4_encodings() {
        const TWO_HOURS_MS: i64 = 2 * 3_600_000;
        let t_before = TWO_HOURS_MS - 10_000;
        let t_after = TWO_HOURS_MS + 5_000;

        for enable_hist_st in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let opts = HeadOptions {
                chunk_range_ms: TWO_HOURS_MS,
                enable_histogram_st_encoding: enable_hist_st,
                ..HeadOptions::default()
            };
            let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

            // 6 monotonic samples before 2h boundary, then 1 monotonic sample after boundary -> crh = NotCounterReset!
            // Second series drops count after boundary -> crh = CounterReset!
            // Third series is float histogram monotonic -> crh = NotCounterReset!
            // Fourth series is float histogram count drop -> crh = CounterReset!
            for k in 0..6i64 {
                let ts = t_before + k * 1_000;
                let mut app = head.appender();
                app.append_histogram(
                    None,
                    &[("__name__", "ih_noreset")],
                    0,
                    ts,
                    make_ih(5 + k, 2, (20 + k) as f64, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.append_histogram(
                    None,
                    &[("__name__", "ih_reset")],
                    0,
                    ts,
                    make_ih(10 + k, 2, (30 + k) as f64, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.append_float_histogram(
                    None,
                    &[("__name__", "fh_noreset")],
                    0,
                    ts,
                    make_fh(5.0 + k as f64, 2.0, 20.0 + k as f64, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.append_float_histogram(
                    None,
                    &[("__name__", "fh_reset")],
                    0,
                    ts,
                    make_fh(10.0 + k as f64, 2.0, 30.0 + k as f64, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.commit().unwrap();
            }

            {
                let mut app = head.appender();
                app.append_histogram(
                    None,
                    &[("__name__", "ih_noreset")],
                    0,
                    t_after,
                    make_ih(20, 3, 50.0, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.append_histogram(
                    None,
                    &[("__name__", "ih_reset")],
                    0,
                    t_after,
                    make_ih(1, 0, 2.0, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.append_float_histogram(
                    None,
                    &[("__name__", "fh_noreset")],
                    0,
                    t_after,
                    make_fh(20.0, 3.0, 50.0, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.append_float_histogram(
                    None,
                    &[("__name__", "fh_reset")],
                    0,
                    t_after,
                    make_fh(1.0, 0.0, 2.0, 2, CounterResetHint::UnknownCounterReset),
                )
                .unwrap();
                app.commit().unwrap();
            }

            let crh_of = |enc: &EncodedChunk| -> CounterResetHeader {
                match enc {
                    EncodedChunk::Histogram(c) => c.counter_reset_header(),
                    EncodedChunk::FloatHistogram(c) => c.counter_reset_header(),
                    EncodedChunk::HistogramSt(c) => c.counter_reset_header(),
                    EncodedChunk::FloatHistogramSt(c) => c.counter_reset_header(),
                    _ => unreachable!(),
                }
            };

            let q = TsdbQuerier::new(Some(&head), Vec::new(), 0, TWO_HOURS_MS * 2);
            let all_m = [LabelMatcher::new(MatchType::RegexMatch, "__name__", ".+").unwrap()];
            let res = q.select_chunks(true, &all_m).unwrap();
            assert_eq!(res.len(), 4);

            // Alphabetical: fh_noreset, fh_reset, ih_noreset, ih_reset
            assert_eq!(res[0].chunks.len(), 2);
            assert_eq!(
                crh_of(res[0].chunks[1].chunk.as_ref().unwrap()),
                CounterResetHeader::NotCounterReset
            );

            assert_eq!(res[1].chunks.len(), 2);
            assert_eq!(
                crh_of(res[1].chunks[1].chunk.as_ref().unwrap()),
                CounterResetHeader::CounterReset
            );

            assert_eq!(res[2].chunks.len(), 2);
            assert_eq!(
                crh_of(res[2].chunks[1].chunk.as_ref().unwrap()),
                CounterResetHeader::NotCounterReset
            );

            assert_eq!(res[3].chunks.len(), 2);
            assert_eq!(
                crh_of(res[3].chunks[1].chunk.as_ref().unwrap()),
                CounterResetHeader::CounterReset
            );

            head.close().unwrap();
        }
    }

    #[test]
    fn w4_07_runtime_float_chunk_encoding_compatible_values_does_not_cut_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            float_chunk_encoding: Encoding::Xor2,
            samples_per_chunk: 120,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        // Append 1 sample under xor2.
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "rt_enc")], 1_000, 1.0)
                .unwrap();
            app.commit().unwrap();
        }

        // Switch runtime encoding xor2 -> xor (enable_st_storage == false).
        // Must continue appending to active Xor2 chunk until normal rollover (2 * 120 = 240 samples).
        head.set_float_chunk_encoding(Encoding::Xor);
        for k in 1..=245i64 {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "rt_enc")], 1_000 + k * 10, k as f64)
                .unwrap();
            app.commit().unwrap();
        }

        let q = TsdbQuerier::new(Some(&head), Vec::new(), 0, 100_000);
        let m = [LabelMatcher::new(MatchType::Equal, "__name__", "rt_enc").unwrap()];
        let res = q.select_chunks(true, &m).unwrap();
        assert_eq!(res.len(), 1);
        let encs: Vec<Encoding> = res[0]
            .chunks
            .iter()
            .map(|c| c.chunk.as_ref().unwrap().encoding())
            .collect();
        // First chunk finishes filling with Xor2 (~120 samples via compute_chunk_end_time),
        // subsequent chunks use Xor. (Without CompatibleValues, chunk 0 would have only 1 sample.)
        assert_eq!(encs, vec![Encoding::Xor2, Encoding::Xor, Encoding::Xor]);
        assert!(
            res[0].chunks[0].chunk.as_ref().unwrap().num_samples() > 1,
            "xor2 chunk must continue filling until normal rollover"
        );
        assert_eq!(
            res[0]
                .chunks
                .iter()
                .map(|c| c.chunk.as_ref().unwrap().num_samples())
                .sum::<usize>(),
            246
        );

        head.close().unwrap();
    }

    #[test]
    fn sort_labels_multi_label_u128_matches_std_sort() {
        let mut actual = Vec::new();
        for app_id in 0..4 {
            for ns_id in (0..8).rev() {
                for pod_id in (0..50).rev() {
                    actual.push(Labels::from_pairs([
                        ("__name__", "container_cpu_usage_seconds_total"),
                        ("app", &format!("svc-{app_id}")),
                        ("namespace", &format!("ns-{ns_id}")),
                        ("pod", &format!("pod-{pod_id:04}")),
                    ]));
                }
            }
        }
        // Also include label name prefix edge case: a="x", b="y" vs aa="x"
        actual.push(Labels::from_pairs([("aa", "x")]));
        actual.push(Labels::from_pairs([("a", "x"), ("b", "y")]));
        actual.push(Labels::from_pairs([("a", "x")]));

        let mut expected = actual.clone();
        expected.sort();
        sort_labels(&mut actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn a01_inverted_time_range_head_overlap_all_modes() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "inv_m"), ("env", "prod")], 1_000, 1.0)
                .unwrap();
            app.append_float(None, &[("__name__", "inv_m"), ("env", "prod")], 5_000, 1.5)
                .unwrap();
            app.append_float(None, &[("__name__", "inv_m"), ("env", "dev")], 1_000, 2.0)
                .unwrap();
            app.append_float(None, &[("__name__", "inv_m"), ("env", "dev")], 5_000, 2.5)
                .unwrap();
            app.commit().unwrap();
        }

        // Inverted query [3000, 2000] overlapping Head [1000, 5000] must include Head.
        let q = TsdbQuerier::new(Some(&head), Vec::new(), 3_000, 2_000);
        assert_eq!(q.label_names(&[]).unwrap(), vec!["__name__", "env"]);
        assert_eq!(q.label_values("env", None, &[]).unwrap(), vec!["dev", "prod"]);
        let sn = q
            .search_label_names(&SearchHints::default(), &[])
            .unwrap();
        assert_eq!(
            sn.iter().map(|r| r.value.as_str()).collect::<Vec<_>>(),
            vec!["__name__", "env"]
        );
        let sv = q
            .search_label_values("env", &SearchHints::default(), &[])
            .unwrap();
        assert_eq!(
            sv.iter().map(|r| r.value.as_str()).collect::<Vec<_>>(),
            vec!["dev", "prod"]
        );
        let all_m = [LabelMatcher::new(MatchType::Equal, "__name__", "inv_m").unwrap()];
        let chunks = q.select_chunks(true, &all_m).unwrap();
        assert_eq!(chunks.len(), 2);

        // Inverted query [6000, 2000] with mint > head.max_time() returns empty.
        let q_past_max = TsdbQuerier::new(Some(&head), Vec::new(), 6_000, 2_000);
        assert!(q_past_max.label_names(&[]).unwrap().is_empty());
        assert!(q_past_max.label_values("env", None, &[]).unwrap().is_empty());
        assert!(q_past_max.select_chunks(true, &all_m).unwrap().is_empty());

        // Inverted query [3000, 500] with maxt < head.min_time() returns empty.
        let q_before_min = TsdbQuerier::new(Some(&head), Vec::new(), 3_000, 500);
        assert!(q_before_min.label_names(&[]).unwrap().is_empty());
        assert!(q_before_min.label_values("env", None, &[]).unwrap().is_empty());
        assert!(q_before_min.select_chunks(true, &all_m).unwrap().is_empty());

        head.close().unwrap();
    }

    #[test]
    fn stream_prepared_chunks_lazy_with_shard_matches_filter_on_head_and_multiple_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::db::Tsdb::open(dir.path(), crate::db::TsdbOptions::default()).unwrap();
        let block_ms = 60_000i64;
        // Build 3 compacted blocks + active Head with 120 series.
        for blk in 0..4i64 {
            let mut app = db.appender();
            for s in 0..120usize {
                let app_v = format!("app-{}", s % 5);
                let pod_v = format!("pod-{s:03}");
                let pairs = [
                    ("__name__", "sharded_qs_metric"),
                    ("app", app_v.as_str()),
                    ("pod", pod_v.as_str()),
                ];
                for k in 0..3i64 {
                    app.append_float(
                        None,
                        &pairs,
                        blk * block_ms + k * 10_000 + 1_000,
                        (blk * 100 + s as i64) as f64,
                    )
                    .unwrap();
                }
            }
            app.commit().unwrap();
            if blk < 3 {
                db.compact_head_range(blk * block_ms, (blk + 1) * block_ms)
                    .unwrap();
            }
        }
        assert_eq!(db.blocks().len(), 3);

        // Delete a partial range from block 0 to exercise block tombstones too.
        let del_m = [LabelMatcher::new(MatchType::Equal, "app", "app-0").unwrap()];
        db.blocks()[0].delete(5_000, 15_000, &del_m).unwrap();

        let matchers =
            [LabelMatcher::new(MatchType::Equal, "__name__", "sharded_qs_metric").unwrap()];
        let shard_count = 8u64;

        for unordered in [false, true] {
            let mut total_series_seen = 0usize;
            for shard_idx in 0..shard_count {
                let make_cq = || {
                    let cq = if unordered {
                        db.unordered_chunk_querier(0, 4 * block_ms)
                    } else {
                        db.chunk_querier(0, 4 * block_ms)
                    };
                    cq.with_concurrent(true)
                };

                // Path A: new stream_prepared_chunks_lazy_with_shard
                let cq_a = make_cq();
                let prep_a = cq_a.prepare_select(&matchers).unwrap();
                let mut a_series: Vec<(Labels, u32)> = Vec::new();
                type ChunkDump = Vec<(usize, Vec<(i64, i64, Vec<u8>)>)>;
                let mut a_chunks: ChunkDump = Vec::new();
                cq_a.stream_prepared_chunks_lazy_with_shard(
                    prep_a,
                    &matchers,
                    true,
                    Some((shard_idx, shard_count)),
                    None,
                    |ev| {
                        match ev {
                            LazyStreamEvent::Series(l, cnt) => a_series.push((l.clone(), cnt)),
                            LazyStreamEvent::SeriesEnd => {}
                            LazyStreamEvent::SeriesChunks(i, chks) => {
                                a_chunks.push((
                                    i,
                                    chks.into_iter()
                                        .map(|c| {
                                            (
                                                c.min_time,
                                                c.max_time,
                                                c.chunk.unwrap().as_bytes().to_vec(),
                                            )
                                        })
                                        .collect(),
                                ));
                            }
                        }
                        Ok(true)
                    },
                )
                .unwrap();

                // Path B: completely independent non-lazy `select()` oracle filtered by stable_hash
                let cq_b = make_cq();
                let all_selected = cq_b.select(true, &matchers).unwrap();
                let mut b_series: Vec<(Labels, u32)> = Vec::new();
                let mut b_chunks: ChunkDump = Vec::new();
                for cs in all_selected {
                    if cs.labels.stable_hash() % shard_count != shard_idx {
                        continue;
                    }
                    let idx = b_series.len();
                    b_series.push((cs.labels, cs.chunks.len() as u32));
                    b_chunks.push((
                        idx,
                        cs.chunks
                            .into_iter()
                            .map(|c| {
                                (
                                    c.min_time,
                                    c.max_time,
                                    c.chunk.unwrap().as_bytes().to_vec(),
                                )
                            })
                            .collect(),
                    ));
                }

                assert_eq!(
                    a_series, b_series,
                    "series mismatch vs select() oracle unordered={unordered} shard={shard_idx}"
                );
                assert_eq!(
                    a_chunks, b_chunks,
                    "chunks mismatch vs select() oracle unordered={unordered} shard={shard_idx}"
                );
                total_series_seen += a_series.len();
            }
            assert_eq!(total_series_seen, 120);
        }

        // Invalid shard parameters must return InvalidShard error, never panic on modulo by zero.
        let cq_bad = db.chunk_querier(0, 4 * block_ms);
        let prep_bad = cq_bad.prepare_select(&matchers).unwrap();
        assert!(matches!(
            cq_bad.stream_prepared_chunks_lazy_with_shard(
                prep_bad,
                &matchers,
                true,
                Some((0, 0)),
                None,
                |_| Ok(true),
            ),
            Err(QuerierError::InvalidShard { idx: 0, count: 0 })
        ));
        let prep_bad2 = cq_bad.prepare_select(&matchers).unwrap();
        assert!(matches!(
            cq_bad.stream_prepared_chunks_lazy_with_shard(
                prep_bad2,
                &matchers,
                true,
                Some((8, 8)),
                None,
                |_| Ok(true),
            ),
            Err(QuerierError::InvalidShard { idx: 8, count: 8 })
        ));

        db.close().unwrap();
    }

    #[test]
    fn lazy_phase2_batching_has_no_deadlock_with_append_refresh_or_slow_callback() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrd};
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let mut opts = crate::db::TsdbOptions::default();
        opts.head.max_head_chunk_file_size = 64 * 1024;
        let db = Arc::new(crate::db::Tsdb::open(dir.path(), opts).unwrap());

        // Create 80 series with mmapped chunks so Phase 2 takes the chunk_reader path.
        for s in 0..80usize {
            let pod = format!("p-{s:03}");
            let pairs = [("__name__", "dl_metric"), ("pod", pod.as_str())];
            let mut app = db.appender();
            for k in 0..130i64 {
                app.append_float(None, &pairs, 1_000 + k * 1_000, k as f64)
                    .unwrap();
            }
            app.commit().unwrap();
        }
        db.head().mmap_head_chunks().unwrap();

        // 1. Slow `on_event` callback must NOT block a concurrent `chunk_reader.write()`
        //    (verifies `chunk_reader.read()` is dropped before `on_event` runs).
        let (cb_entered_tx, cb_entered_rx) = std::sync::mpsc::channel();
        let (cb_release_tx, cb_release_rx) = std::sync::mpsc::channel();
        let db_q = Arc::clone(&db);
        let q_handle = std::thread::spawn(move || {
            let matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "dl_metric").unwrap()];
            let cq = db_q.chunk_querier(0, 500_000);
            let prep = cq.prepare_select(&matchers).unwrap();
            let mut signaled = false;
            cq.stream_prepared_chunks_lazy_with_shard(
                prep,
                &matchers,
                true,
                None,
                None,
                |ev| {
                    if matches!(ev, LazyStreamEvent::SeriesChunks(_, _)) && !signaled {
                        signaled = true;
                        let _ = cb_entered_tx.send(());
                        let _ = cb_release_rx.recv();
                    }
                    Ok(true)
                },
            )
            .unwrap();
        });
        cb_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("query must reach on_event(SeriesChunks)");
        // While on_event is parked, sealing/refreshing chunks_head (which acquires chunk_reader.write())
        // must complete promptly without stalling behind the slow query callback!
        let db_r = Arc::clone(&db);
        let (refreshed_tx, refreshed_rx) = std::sync::mpsc::channel();
        let r_handle = std::thread::spawn(move || {
            db_r.head().acquire_chunk_reader_write_for_test();
            let _ = refreshed_tx.send(());
        });
        refreshed_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("chunk_reader.write() must NOT stall while on_event is blocked");
        let _ = cb_release_tx.send(());
        q_handle.join().unwrap();
        r_handle.join().unwrap();

        // 2. Concurrent stress: QueryStream + duplicate appends + chunk_reader refreshes.
        let stop = Arc::new(AtomicBool::new(false));
        let stop_a = Arc::clone(&stop);
        let db_a = Arc::clone(&db);
        let t_append = std::thread::spawn(move || {
            let mut iter = 0i64;
            while !stop_a.load(AtomicOrd::Relaxed) {
                let mut app = db_a.appender();
                for s in 0..80usize {
                    let pod = format!("p-{s:03}");
                    let pairs = [("__name__", "dl_metric"), ("pod", pod.as_str())];
                    let _ = app.append_float(None, &pairs, 1_000 + 129 * 1_000, iter as f64);
                }
                let _ = app.commit();
                iter += 1;
            }
        });
        let stop_w = Arc::clone(&stop);
        let db_w = Arc::clone(&db);
        let t_refresh = std::thread::spawn(move || {
            while !stop_w.load(AtomicOrd::Relaxed) {
                db_w.head().acquire_chunk_reader_write_for_test();
                std::thread::yield_now();
            }
        });
        let matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "dl_metric").unwrap()];
        for _ in 0..25 {
            let cq = db.chunk_querier(0, 500_000);
            let prep = cq.prepare_select(&matchers).unwrap();
            let mut count = 0usize;
            cq.stream_prepared_chunks_lazy_with_shard(prep, &matchers, true, None, None, |ev| {
                if matches!(ev, LazyStreamEvent::SeriesChunks(_, _)) {
                    count += 1;
                }
                Ok(true)
            })
            .unwrap();
            assert_eq!(count, 80);
        }
        stop.store(true, AtomicOrd::Relaxed);
        t_append.join().unwrap();
        t_refresh.join().unwrap();
        db.close().unwrap();
    }
}
