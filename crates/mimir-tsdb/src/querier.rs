//! Unified Querier, ChunkQuerier, and Searcher across TSDB Head and on-disk ULID Blocks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/querier.go
//! and github.com/prometheus/prometheus/tsdb/db.go.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::RwLock;
use smallvec::SmallVec;
use thiserror::Error;

use crate::block::{Block, BlockError};
use crate::chunkenc::{ChunkError, EncodedChunk};
use crate::chunks_block::{BlockChunksError, ChunkMeta};
use crate::compact::{decode_chunk_samples, encode_samples_into_chunks, DecodedSample};
use crate::head::{Head, HeadError, DEFAULT_SAMPLES_PER_CHUNK};
use crate::index::IndexError;
use crate::labels::Labels;
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
#[derive(Clone, Debug)]
pub enum LazyChunkSource {
    Head(u64),
    Block { block_idx: u16, ref_id: u64 },
    Multi(Box<[(u16, u64)]>),
}

/// Lightweight Phase 1 series descriptor holding zero chunk bytes.
#[derive(Clone, Debug)]
pub struct LazySeriesEntry {
    pub labels: Labels,
    pub source: LazyChunkSource,
    pub chunk_count: u32,
}

/// Incremental event emitted by [`TsdbQuerier::stream_prepared_chunks_lazy`].
pub enum LazyStreamEvent {
    Series(Labels, u32),
    SeriesEnd,
    SeriesChunks(usize, Vec<ChunkMeta>),
}

/// Pre-evaluated postings selection across TSDB `Head` and overlapping `Block`s.
///
/// `'static + Send + Sync` so callers can evaluate postings inline (hitting caches in microseconds)
/// and decide whether to materialize chunks inline or offload to a blocking worker pool.
#[derive(Clone, Debug)]
pub struct PreparedChunkSelect {
    pub head_refs: Option<Arc<[u64]>>,
    pub block_refs: Vec<(Arc<Block>, Arc<[u64]>)>,
    overlapping_blocks: Vec<Arc<Block>>,
    pub mint: i64,
    pub maxt: i64,
    pub unordered: bool,
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
    mint: i64,
    maxt: i64,
    select_stats: Option<BlockSelectStatsSink>,
    select_stats_filter: Option<SeriesStatsFilter>,
}

impl<'a> TsdbQuerier<'a> {
    pub fn new(head: Option<&'a Head>, blocks: Vec<Arc<Block>>, mint: i64, maxt: i64) -> Self {
        let head = head.filter(|h| {
            let in_order = h.initialized() && h.min_time() <= maxt && mint <= h.max_time();
            let ooo = h.min_ooo_time() != i64::MAX
                && h.min_ooo_time() <= maxt
                && mint <= h.max_ooo_time();
            in_order || ooo
        });
        let blocks = blocks
            .into_iter()
            .filter(|b| b.overlaps_closed_interval(mint, maxt))
            .collect();
        Self {
            head,
            blocks,
            blocks_lock: None,
            mint,
            maxt,
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
        Self {
            head,
            blocks: Vec::new(),
            blocks_lock: Some(blocks_lock),
            mint,
            maxt,
            select_stats: None,
            select_stats_filter: None,
        }
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
        empty_blocks: usize,
    ) {
        let Some(sink) = &self.select_stats else {
            return;
        };
        let mut g = sink.lock();
        g.extend(head.map(|h| BlockSelectStats { head: true, ..h }));
        g.extend_from_slice(blocks);
        g.extend(std::iter::repeat_n(BlockSelectStats::default(), empty_blocks));
    }

    fn effective_blocks<'b>(
        &'b self,
        guard: &'b Option<parking_lot::RwLockReadGuard<'a, Vec<Arc<Block>>>>,
    ) -> Vec<Arc<Block>> {
        match guard {
            Some(g) => g
                .iter()
                .filter(|b| b.overlaps_closed_interval(self.mint, self.maxt))
                .cloned()
                .collect(),
            None => self.blocks.clone(),
        }
    }

    fn effective_in_order_head(&self) -> Option<&'a Head> {
        self.head
            .filter(|h| h.initialized() && h.min_time() <= self.maxt && self.mint <= h.max_time())
    }

    fn effective_head(&self) -> Option<&'a Head> {
        self.head.filter(|h| {
            let in_order =
                h.initialized() && h.min_time() <= self.maxt && self.mint <= h.max_time();
            let ooo = h.min_ooo_time() != i64::MAX
                && h.min_ooo_time() <= self.maxt
                && self.mint <= h.max_ooo_time();
            in_order || ooo
        })
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
        blocks: Vec<Arc<Block>>,
        matchers: &[LabelMatcher],
        unordered: bool,
    ) -> Result<PreparedChunkSelect, QuerierError> {
        let head = self.effective_head();
        let head_refs = match head {
            Some(h) => Some(h.postings_for_matchers_arc(matchers)?),
            None => None,
        };
        let mut block_refs = Vec::with_capacity(blocks.len());
        for blk in &blocks {
            let refs = blk.postings_for_matchers(matchers)?;
            if !refs.is_empty() {
                block_refs.push((Arc::clone(blk), refs));
            }
        }
        Ok(PreparedChunkSelect {
            head_refs,
            block_refs,
            overlapping_blocks: blocks,
            mint: self.mint,
            maxt: self.maxt,
            unordered,
        })
    }

    /// Evaluates postings matchers across `Head` and overlapping `Block`s (hitting
    /// `PostingsForMatchersCache` when cached) without loading any chunk bytes.
    pub fn prepare_select_chunks(
        &self,
        matchers: &[LabelMatcher],
        unordered: bool,
    ) -> Result<PreparedChunkSelect, QuerierError> {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        drop(lock_guard);
        self.prepare_select_locked(blocks, matchers, unordered)
    }

    /// Evaluates postings inline under `blocks_lock.read()`. If the matched series count is small
    /// (`<= max_source_series` and `<= max_total_refs`), materializes chunks immediately while still
    /// holding `blocks_lock.read()` and returns `Ok(Ok(series))`. Otherwise releases the lock and
    /// returns `Ok(Err(prepared))` so the caller can offload chunk loading to a blocking pool.
    pub fn select_chunks_adaptive(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        unordered: bool,
        max_source_series: usize,
        max_total_refs: usize,
    ) -> Result<Result<Vec<QueryChunkSeries>, PreparedChunkSelect>, QuerierError> {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        let prepared = self.prepare_select_locked(blocks, matchers, unordered)?;
        if prepared.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        if prepared.max_source_series_len() <= max_source_series
            && prepared.total_refs() <= max_total_refs
        {
            let series = self.execute_prepared_select_locked(prepared, sort_series)?;
            return Ok(Ok(series));
        }
        drop(lock_guard);
        Ok(Err(prepared))
    }

    /// Non-blocking variant of [`Self::select_chunks_adaptive`]: returns `Ok(None)` immediately
    /// if `blocks_lock` is currently contended by a compaction writer, so async RPC threads never
    /// block on `blocks_lock.read()`.
    pub fn try_select_chunks_adaptive(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        unordered: bool,
        max_source_series: usize,
        max_total_refs: usize,
    ) -> Result<Option<Result<Vec<QueryChunkSeries>, PreparedChunkSelect>>, QuerierError> {
        let lock_guard = match self.blocks_lock {
            Some(l) => match l.try_read() {
                Some(g) => Some(g),
                None => return Ok(None),
            },
            None => None,
        };
        let blocks = self.effective_blocks(&lock_guard);
        let prepared = self.prepare_select_locked(blocks, matchers, unordered)?;
        if prepared.is_empty() {
            return Ok(Some(Ok(Vec::new())));
        }
        if prepared.max_source_series_len() <= max_source_series
            && prepared.total_refs() <= max_total_refs
        {
            let series = self.execute_prepared_select_locked(prepared, sort_series)?;
            return Ok(Some(Ok(series)));
        }
        drop(lock_guard);
        Ok(Some(Err(prepared)))
    }

    /// Materializes chunks for a [`PreparedChunkSelect`] while holding `blocks_lock.read()`,
    /// refreshing the selection if concurrent compaction updated `blocks`.
    pub fn execute_prepared_select_with_matchers(
        &self,
        mut prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        if self.blocks_lock.is_some() {
            let current_blocks = self.effective_blocks(&lock_guard);
            let same = current_blocks.len() == prepared.overlapping_blocks.len()
                && current_blocks
                    .iter()
                    .zip(prepared.overlapping_blocks.iter())
                    .all(|(a, b)| Arc::ptr_eq(a, b));
            if !same {
                prepared =
                    self.prepare_select_locked(current_blocks, matchers, prepared.unordered)?;
            }
        }
        self.execute_prepared_select_locked(prepared, sort_series)
    }

    /// Materializes chunks for a [`PreparedChunkSelect`] produced by [`Self::prepare_select_chunks`].
    pub fn execute_prepared_select(
        &self,
        prepared: PreparedChunkSelect,
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        let _lock_guard = self.blocks_lock.map(|l| l.read());
        self.execute_prepared_select_locked(prepared, sort_series)
    }

    fn execute_prepared_select_locked(
        &self,
        prepared: PreparedChunkSelect,
        sort_series: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
        let head = self.effective_head();
        let mint = prepared.mint;
        let maxt = prepared.maxt;
        let unordered = prepared.unordered;

        let empty_blocks = prepared
            .overlapping_blocks
            .len()
            .saturating_sub(prepared.block_refs.len());
        if prepared.block_refs.is_empty() {
            let (Some(head), Some(refs)) = (head, prepared.head_refs) else {
                self.record_select_stats(None, &[], empty_blocks);
                return Ok(Vec::new());
            };
            let mut out = Vec::with_capacity(refs.len());
            for &r in refs.iter() {
                if let Some((labels, chks, tombs)) = head.series_chunks(r, mint, maxt)? {
                    if chks.is_empty() {
                        continue;
                    }
                    let has_tombstones = !tombs.is_empty()
                        && chks
                            .iter()
                            .any(|cm| tombs.iter().any(|iv| iv.overlaps(cm.min_time, cm.max_time)));
                    let final_chunks = if has_tombstones {
                        let mut samples = Vec::new();
                        for cm in &chks {
                            if let Some(ref enc) = cm.chunk {
                                for s in decode_chunk_samples(enc)? {
                                    if s.t >= mint && s.t <= maxt {
                                        samples.push(s);
                                    }
                                }
                            }
                        }
                        samples.sort_by_key(|s| s.t);
                        samples.dedup_by_key(|s| s.t);
                        encode_samples_into_chunks(&samples, DEFAULT_SAMPLES_PER_CHUNK)?
                    } else if chks.len() > 1 {
                        let mut chks = chks;
                        let already_sorted_disjoint = chks
                            .windows(2)
                            .all(|w| w[0].min_time <= w[1].min_time && w[0].max_time < w[1].min_time);
                        if already_sorted_disjoint {
                            chks
                        } else {
                            chks.sort_by_key(|c| (c.min_time, c.max_time));
                            let has_overlap =
                                chks.windows(2).any(|w| w[1].min_time <= w[0].max_time);
                            if has_overlap {
                                let mut samples = Vec::new();
                                for cm in &chks {
                                    if let Some(ref enc) = cm.chunk {
                                        for s in decode_chunk_samples(enc)? {
                                            if s.t >= mint && s.t <= maxt {
                                                samples.push(s);
                                            }
                                        }
                                    }
                                }
                                samples.sort_by_key(|s| s.t);
                                samples.dedup_by_key(|s| s.t);
                                encode_samples_into_chunks(&samples, DEFAULT_SAMPLES_PER_CHUNK)?
                            } else {
                                chks
                            }
                        }
                    } else {
                        chks
                    };
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
                }),
                &[],
                empty_blocks,
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
                head: false,
            };
            for &r in refs.iter() {
                if let Some((labels, chks, tombs)) = head.series_chunks(r, mint, maxt)? {
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
        self.record_select_stats(head_stats, &block_stats, empty_blocks);

        let merge_or_collect = |items: Vec<(ChunkMeta, Vec<Interval>)>,
                                allow_non_overlapping_raw: bool|
         -> Result<Vec<ChunkMeta>, QuerierError> {
            if items.is_empty() {
                return Ok(Vec::new());
            }
            let mut chks: Vec<ChunkMeta> = Vec::with_capacity(items.len());
            let mut has_tombstones = false;
            for (cm, tombs) in items {
                if tombs
                    .iter()
                    .any(|iv| iv.overlaps(cm.min_time, cm.max_time))
                {
                    has_tombstones = true;
                }
                chks.push(cm);
            }
            if has_tombstones {
                let mut samples = Vec::new();
                for cm in &chks {
                    let Some(ref enc) = cm.chunk else {
                        continue;
                    };
                    for s in decode_chunk_samples(enc)? {
                        if s.t < mint || s.t > maxt {
                            continue;
                        }
                        samples.push(s);
                    }
                }
                samples.sort_by_key(|s| s.t);
                samples.dedup_by_key(|s| s.t);
                Ok(encode_samples_into_chunks(&samples, DEFAULT_SAMPLES_PER_CHUNK)?)
            } else if allow_non_overlapping_raw {
                Ok(chks)
            } else {
                chks.sort_by_key(|c| (c.min_time, c.max_time));
                let has_overlap = chks.windows(2).any(|w| w[1].min_time <= w[0].max_time);
                if has_overlap {
                    let mut samples = Vec::new();
                    for cm in &chks {
                        let Some(ref enc) = cm.chunk else {
                            continue;
                        };
                        for s in decode_chunk_samples(enc)? {
                            if s.t < mint || s.t > maxt {
                                continue;
                            }
                            samples.push(s);
                        }
                    }
                    samples.sort_by_key(|s| s.t);
                    samples.dedup_by_key(|s| s.t);
                    Ok(encode_samples_into_chunks(&samples, DEFAULT_SAMPLES_PER_CHUNK)?)
                } else {
                    Ok(chks)
                }
            }
        };

        let mut out = Vec::with_capacity(per_series.len());
        for (labels, sources) in per_series {
            let final_chunks = if unordered {
                let mut combined = merge_or_collect(sources.head, false)?;
                let blk_chks = merge_or_collect(sources.blocks, true)?;
                combined.extend(blk_chks);
                combined
            } else {
                let mut all_items = sources.head;
                all_items.extend(sources.blocks);
                merge_or_collect(all_items, false)?
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

    /// Two-phase lazy `QueryStream` executor holding `blocks_lock.read()` across Phase 1 and Phase 2:
    /// - Phase 1 builds lightweight `LazySeriesEntry` descriptors (`chunk_count` without cloning chunk bytes
    ///   on the fast path), sorts if requested, streams `(Labels, chunk_count)` via `on_series` (dropping
    ///   `Labels` immediately), and calls `on_series_end`.
    /// - Phase 2 loads `Vec<ChunkMeta>` for ONE series at a time and yields it to `on_series_chunks`.
    #[allow(clippy::too_many_arguments)]
    pub fn stream_prepared_chunks_lazy<F>(
        &self,
        mut prepared: PreparedChunkSelect,
        matchers: &[LabelMatcher],
        sort_series: bool,
        series_filter: Option<&dyn Fn(&Labels) -> bool>,
        mut on_event: F,
    ) -> Result<(), QuerierError>
    where
        F: FnMut(LazyStreamEvent) -> Result<bool, QuerierError>,
    {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        if self.blocks_lock.is_some() {
            let current_blocks = self.effective_blocks(&lock_guard);
            let same = current_blocks.len() == prepared.overlapping_blocks.len()
                && current_blocks
                    .iter()
                    .zip(prepared.overlapping_blocks.iter())
                    .all(|(a, b)| Arc::ptr_eq(a, b));
            if !same {
                prepared =
                    self.prepare_select_locked(current_blocks, matchers, prepared.unordered)?;
            }
        }

        let head = self.effective_head();
        let mint = prepared.mint;
        let maxt = prepared.maxt;
        let unordered = prepared.unordered;
        let empty_blocks = prepared
            .overlapping_blocks
            .len()
            .saturating_sub(prepared.block_refs.len());

        let merge_or_collect = |items: Vec<(ChunkMeta, Vec<Interval>)>,
                                allow_non_overlapping_raw: bool|
         -> Result<Vec<ChunkMeta>, QuerierError> {
            if items.is_empty() {
                return Ok(Vec::new());
            }
            let mut chks: Vec<ChunkMeta> = Vec::with_capacity(items.len());
            let mut has_tombstones = false;
            for (cm, tombs) in items {
                if tombs
                    .iter()
                    .any(|iv| iv.overlaps(cm.min_time, cm.max_time))
                {
                    has_tombstones = true;
                }
                chks.push(cm);
            }
            if has_tombstones {
                let mut samples = Vec::new();
                for cm in &chks {
                    let Some(ref enc) = cm.chunk else {
                        continue;
                    };
                    for s in decode_chunk_samples(enc)? {
                        if s.t < mint || s.t > maxt {
                            continue;
                        }
                        samples.push(s);
                    }
                }
                samples.sort_by_key(|s| s.t);
                samples.dedup_by_key(|s| s.t);
                Ok(encode_samples_into_chunks(&samples, DEFAULT_SAMPLES_PER_CHUNK)?)
            } else if allow_non_overlapping_raw {
                Ok(chks)
            } else {
                chks.sort_by_key(|c| (c.min_time, c.max_time));
                let has_overlap = chks.windows(2).any(|w| w[1].min_time <= w[0].max_time);
                if has_overlap {
                    let mut samples = Vec::new();
                    for cm in &chks {
                        let Some(ref enc) = cm.chunk else {
                            continue;
                        };
                        for s in decode_chunk_samples(enc)? {
                            if s.t < mint || s.t > maxt {
                                continue;
                            }
                            samples.push(s);
                        }
                    }
                    samples.sort_by_key(|s| s.t);
                    samples.dedup_by_key(|s| s.t);
                    Ok(encode_samples_into_chunks(&samples, DEFAULT_SAMPLES_PER_CHUNK)?)
                } else {
                    Ok(chks)
                }
            }
        };

        if prepared.block_refs.is_empty() {
            let selected_postings = prepared.head_refs.as_ref().map(|r| r.len() as u64);
            let mut entries: Vec<LazySeriesEntry> = match (head, prepared.head_refs) {
                (Some(h), Some(refs)) => {
                    let mut v = Vec::with_capacity(refs.len());
                    for &r in refs.iter() {
                        if let Some((labels, chunk_count)) =
                            h.series_lazy_chunk_count(r, mint, maxt, series_filter)?
                        {
                            if chunk_count > 0 {
                                v.push(LazySeriesEntry {
                                    labels,
                                    source: LazyChunkSource::Head(r),
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
                }),
                &[],
                empty_blocks,
            );

            if sort_series && entries.len() > 1 {
                entries.sort_unstable_by(|a, b| a.labels.cmp(&b.labels));
            }

            for entry in entries.iter_mut() {
                let labels = std::mem::take(&mut entry.labels);
                if !on_event(LazyStreamEvent::Series(labels, entry.chunk_count))? {
                    return Ok(());
                }
            }
            if !on_event(LazyStreamEvent::SeriesEnd)? {
                return Ok(());
            }

            if let Some(h) = head {
                for (series_idx, entry) in entries.into_iter().enumerate() {
                    let r = match entry.source {
                        LazyChunkSource::Head(r) => r,
                        _ => continue,
                    };
                    let chks = match h.series_final_chunks(r, mint, maxt)? {
                        Some((_, c)) => c,
                        None => Vec::new(),
                    };
                    if !on_event(LazyStreamEvent::SeriesChunks(series_idx, chks))? {
                        return Ok(());
                    }
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
            sources: SmallVec<[(u16, u64); 2]>,
            head_ivs: SmallVec<[(i64, i64); 4]>,
            block_ivs: SmallVec<[(i64, i64); 4]>,
            has_block_tombstone_overlap: bool,
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
        let mut per_series: hashbrown::HashMap<Labels, MultiMeta> =
            hashbrown::HashMap::with_capacity(est);

        let mut head_stats = None;
        if let (Some(h), Some(refs)) = (head, prepared.head_refs) {
            let mut stats = BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: 0,
                head: false,
            };
            for &r in refs.iter() {
                if let Some((labels, ivs, _had_slow)) =
                    h.series_chunk_metas_no_bytes(r, mint, maxt, series_filter)?
                {
                    if !ivs.is_empty() {
                        stats.series += 1;
                    }
                    let entry = per_series.entry(labels).or_insert_with(|| MultiMeta {
                        sources: SmallVec::new(),
                        head_ivs: SmallVec::new(),
                        block_ivs: SmallVec::new(),
                        has_block_tombstone_overlap: false,
                    });
                    entry.sources.push((0, r));
                    entry.head_ivs.extend(ivs);
                }
            }
            head_stats = Some(stats);
        }

        let mut block_stats: SmallVec<[BlockSelectStats; 4]> = SmallVec::new();
        for (b_idx, (blk, refs)) in prepared.block_refs.iter().enumerate() {
            let idx = blk.index();
            let tombr = blk.tombstones();
            let blk_has_any_tombs = tombr.has_any();
            let src_tag = 1u16 + (b_idx as u16);
            let mut stats = BlockSelectStats {
                selected_postings: refs.len() as u64,
                series: 0,
                head: false,
            };
            for &r in refs.iter() {
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
                let mut local_ivs: SmallVec<[(i64, i64); 4]> = SmallVec::new();
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
                    if !tombs.is_empty()
                        && tombs.iter().any(|iv| iv.overlaps(cm.min_time, cm.max_time))
                    {
                        partial_tomb = true;
                    }
                    local_ivs.push((cm.min_time, cm.max_time));
                }
                if !local_ivs.is_empty() {
                    stats.series += 1;
                    let entry = per_series.entry(labels).or_insert_with(|| MultiMeta {
                        sources: SmallVec::new(),
                        head_ivs: SmallVec::new(),
                        block_ivs: SmallVec::new(),
                        has_block_tombstone_overlap: false,
                    });
                    entry.sources.push((src_tag, r));
                    entry.block_ivs.extend(local_ivs);
                    if partial_tomb {
                        entry.has_block_tombstone_overlap = true;
                    }
                }
            }
            block_stats.push(stats);
        }
        self.record_select_stats(head_stats, &block_stats, empty_blocks);

        let load_multi_chunks = |srcs: &[(u16, u64)]| -> Result<Vec<ChunkMeta>, QuerierError> {
            let mut head_items: Vec<(ChunkMeta, Vec<Interval>)> = Vec::new();
            let mut block_items: Vec<(ChunkMeta, Vec<Interval>)> = Vec::new();
            for &(tag, r) in srcs {
                if tag == 0 {
                    if let Some(h) = head {
                        if let Some((_, chks, tombs)) = h.series_chunks(r, mint, maxt)? {
                            for c in chks {
                                head_items.push((c, tombs.clone()));
                            }
                        }
                    }
                } else {
                    let blk = &blocks[(tag - 1) as usize];
                    let (_, chks) = blk.index().series(r)?;
                    let chkr = blk.chunks();
                    let tombs = blk.tombstones().get(r);
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
                        block_items.push((cm, tombs.clone()));
                    }
                }
            }
            if unordered {
                let mut combined = merge_or_collect(head_items, false)?;
                let blk_chks = merge_or_collect(block_items, true)?;
                combined.extend(blk_chks);
                Ok(combined)
            } else {
                head_items.extend(block_items);
                merge_or_collect(head_items, false)
            }
        };

        let mut entries: Vec<LazySeriesEntry> = Vec::with_capacity(per_series.len());
        for (labels, meta) in per_series {
            let can_count_without_load = if meta.has_block_tombstone_overlap {
                false
            } else if unordered {
                // `head_ivs` is already post-merge from `series_chunk_metas_no_bytes`;
                // `blocks` in unordered mode concatenate without overlap merging.
                true
            } else {
                let mut all_ivs: SmallVec<[(i64, i64); 8]> =
                    SmallVec::with_capacity(meta.head_ivs.len() + meta.block_ivs.len());
                all_ivs.extend_from_slice(&meta.head_ivs);
                all_ivs.extend_from_slice(&meta.block_ivs);
                if all_ivs.len() <= 1 {
                    true
                } else {
                    all_ivs.sort_unstable();
                    all_ivs.windows(2).all(|w| w[1].0 > w[0].1)
                }
            };

            let chunk_count = if can_count_without_load {
                (meta.head_ivs.len() + meta.block_ivs.len()) as u32
            } else {
                let chks = load_multi_chunks(&meta.sources)?;
                chks.len() as u32
            };
            if chunk_count == 0 {
                continue;
            }
            let source = match meta.sources.as_slice() {
                [(0, r)] => LazyChunkSource::Head(*r),
                [(tag, r)] => LazyChunkSource::Block {
                    block_idx: *tag - 1,
                    ref_id: *r,
                },
                many => LazyChunkSource::Multi(many.to_vec().into_boxed_slice()),
            };
            entries.push(LazySeriesEntry {
                labels,
                source,
                chunk_count,
            });
        }

        if sort_series && entries.len() > 1 {
            entries.sort_unstable_by(|a, b| a.labels.cmp(&b.labels));
        }

        for entry in entries.iter_mut() {
            let labels = std::mem::take(&mut entry.labels);
            if !on_event(LazyStreamEvent::Series(labels, entry.chunk_count))? {
                return Ok(());
            }
        }
        if !on_event(LazyStreamEvent::SeriesEnd)? {
            return Ok(());
        }

        for (series_idx, entry) in entries.into_iter().enumerate() {
            let chks = match entry.source {
                LazyChunkSource::Head(r) => {
                    let single = [(0u16, r)];
                    load_multi_chunks(&single)?
                }
                LazyChunkSource::Block { block_idx, ref_id } => {
                    let single = [(1u16 + block_idx, ref_id)];
                    load_multi_chunks(&single)?
                }
                LazyChunkSource::Multi(srcs) => load_multi_chunks(&srcs)?,
            };
            if !on_event(LazyStreamEvent::SeriesChunks(series_idx, chks))? {
                return Ok(());
            }
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
        let _lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&_lock_guard);
        let prepared = self.prepare_select_locked(blocks, matchers, unordered)?;
        self.execute_prepared_select_locked(prepared, sort_series)
    }

    /// Queries matching series labels directly (without decoding or cloning chunk bytes).
    pub fn select_raw_labels(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>, QuerierError> {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        let head = self.effective_head();

        if blocks.is_empty() {
            let Some(head) = head else {
                return Ok(Vec::new());
            };
            let refs = head.postings_for_matchers_arc(matchers)?;
            let mut out = Vec::with_capacity(refs.len());
            for &r in refs.iter() {
                if let Some(labels) = head.series_has_chunks_in_range(r, self.mint, self.maxt) {
                    out.push(labels);
                }
            }
            if sort_series {
                out.sort();
            }
            return Ok(out);
        }

        let mut set: BTreeSet<Labels> = BTreeSet::new();
        if let Some(head) = head {
            let refs = head.postings_for_matchers_arc(matchers)?;
            for &r in refs.iter() {
                if let Some(labels) = head.series_has_chunks_in_range(r, self.mint, self.maxt) {
                    set.insert(labels);
                }
            }
        }
        for blk in &blocks {
            let idx = blk.index();
            let tombr = blk.tombstones();
            let refs = blk.postings_for_matchers(matchers)?;
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
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        let head = self.effective_in_order_head();
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
                let refs = blk.postings_for_matchers(matchers)?;
                for name in idx.label_names_for(&refs)? {
                    set.insert(name);
                }
            }
        }

        Ok(set.into_iter().collect())
    }

    /// Returns sorted unique label values for `name` across in-order Head and overlapping Blocks matching `matchers`.
    pub fn label_values(
        &self,
        name: &str,
        limit: Option<usize>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, QuerierError> {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        let head = self.effective_in_order_head();

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
            } else {
                let refs = blk.postings_for_matchers(matchers)?;
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
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        let mut set: BTreeSet<Labels> = BTreeSet::new();
        if let Some(head) = self.head {
            let all = LabelMatcher::new(crate::matcher::MatchType::RegexMatch, "__name__", ".*")
                .map_err(PostingsError::from)?;
            for &r in head.postings_for_matchers_arc(&[all])?.iter() {
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
        F: FnMut(LazyStreamEvent) -> Result<bool, QuerierError>,
    {
        self.inner.stream_prepared_chunks_lazy(
            prepared,
            matchers,
            sort_series,
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

        // Overlapping in-order Head [1_000_000, 1_000_000]:
        let q_ino = TsdbQuerier::new(Some(&head), Vec::new(), 1_000_000, 1_000_000);
        assert_eq!(q_ino.label_names(&[]).unwrap(), vec!["__name__", "head_lbl"]);
        assert_eq!(q_ino.label_values("head_lbl", None, &[]).unwrap(), vec!["hv"]);
        head.close().unwrap();
    }
}
