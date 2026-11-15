//! Unified Querier, ChunkQuerier, and Searcher across TSDB Head and on-disk ULID Blocks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/querier.go
//! and github.com/prometheus/prometheus/tsdb/db.go.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::RwLock;
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

/// Unified read view over `Head` and overlapping `Block`s for `[mint, maxt]`.
pub struct TsdbQuerier<'a> {
    head: Option<&'a Head>,
    blocks: Vec<Arc<Block>>,
    blocks_lock: Option<&'a RwLock<Vec<Arc<Block>>>>,
    mint: i64,
    maxt: i64,
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
        }
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

    /// Queries encoded chunks for all series matching `matchers` in `[mint, maxt]`.
    /// When `unordered` is true, chunks from Head (in-order + OOO merged) and each Block are
    /// concatenated without cross-block compaction, matching Prometheus `UnorderedChunkQuerier`.
    pub fn select_chunks_with_mode(
        &self,
        sort_series: bool,
        matchers: &[LabelMatcher],
        unordered: bool,
    ) -> Result<Vec<QueryChunkSeries>, QuerierError> {
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
                if let Some((labels, chks, tombs)) = head.series_chunks(r, self.mint, self.maxt)? {
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
                                    if s.t >= self.mint && s.t <= self.maxt {
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
                                            if s.t >= self.mint && s.t <= self.maxt {
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
            if sort_series {
                out.sort_by(|a, b| a.labels.cmp(&b.labels));
            }
            return Ok(out);
        }

        struct SeriesSources {
            head: Vec<(ChunkMeta, Vec<Interval>)>,
            blocks: Vec<(ChunkMeta, Vec<Interval>)>,
        }

        let mut per_series: BTreeMap<Labels, SeriesSources> = BTreeMap::new();

        if let Some(head) = head {
            let refs = head.postings_for_matchers_arc(matchers)?;
            for &r in refs.iter() {
                if let Some((labels, chks, tombs)) = head.series_chunks(r, self.mint, self.maxt)? {
                    let entry = per_series.entry(labels).or_insert_with(|| SeriesSources {
                        head: Vec::new(),
                        blocks: Vec::new(),
                    });
                    for c in chks {
                        entry.head.push((c, tombs.clone()));
                    }
                }
            }
        }

        for blk in &blocks {
            let idx = blk.index();
            let chkr = blk.chunks();
            let tombr = blk.tombstones();
            let refs = blk.postings_for_matchers(matchers)?;
            for &r in refs.iter() {
                let (labels, chks) = idx.series(r)?;
                let tombs = tombr.get(r);
                let mut loaded = Vec::new();
                for mut cm in chks {
                    if !cm.overlaps_closed_interval(self.mint, self.maxt) {
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
                    let entry = per_series.entry(labels).or_insert_with(|| SeriesSources {
                        head: Vec::new(),
                        blocks: Vec::new(),
                    });
                    entry.blocks.extend(loaded);
                }
            }
        }

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
                        if s.t < self.mint || s.t > self.maxt {
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
                            if s.t < self.mint || s.t > self.maxt {
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

        if !sort_series {
            // `per_series` is already sorted by Labels, which always satisfies both sorted & unsorted callers.
        }
        Ok(out)
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

    /// Returns sorted unique label names across Head and overlapping Blocks matching `matchers`.
    pub fn label_names(&self, matchers: &[LabelMatcher]) -> Result<Vec<String>, QuerierError> {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        let head = self.effective_head();
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

    /// Returns sorted unique label values for `name` across Head and overlapping Blocks matching `matchers`.
    pub fn label_values(
        &self,
        name: &str,
        limit: Option<usize>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, QuerierError> {
        let lock_guard = self.blocks_lock.map(|l| l.read());
        let blocks = self.effective_blocks(&lock_guard);
        let head = self.effective_head();

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
