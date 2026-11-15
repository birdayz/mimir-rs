//! Unified Prometheus TSDB (`Head` + on-disk ULID `Block`s + Compaction + Querier).
//!
//! Ported from github.com/prometheus/prometheus/tsdb/db.go.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use ulid::Ulid;

use crate::block::{list_block_dirs, Block, BlockError, BlockMetaCompaction};
use crate::compact::{write_head_block_streaming, CompactError};
use crate::exemplar::{CircularExemplarStorage, ExemplarQueryResult};
use crate::head::{range_for_timestamp, Head, HeadAppender, HeadError, HeadOptions};
use crate::matcher::LabelMatcher;
use crate::querier::{TsdbChunkQuerier, TsdbQuerier};

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
}

#[derive(Clone, Debug)]
pub struct TsdbOptions {
    pub head: HeadOptions,
    pub max_block_chunk_segment_size: usize,
    pub no_lockfile: bool,
}

impl Default for TsdbOptions {
    fn default() -> Self {
        Self {
            head: HeadOptions::default(),
            max_block_chunk_segment_size: 0,
            no_lockfile: true,
        }
    }
}

/// Unified TSDB instance managing an active [`Head`] and immutable on-disk ULID [`Block`]s.
pub struct Tsdb {
    dir: PathBuf,
    opts: TsdbOptions,
    head: Head,
    blocks: RwLock<Vec<Arc<Block>>>,
    compact_mtx: Mutex<()>,
    compactions_total: Option<prometheus::Counter>,
}

impl Tsdb {
    /// Opens (or creates) a TSDB in `dir`, loading all on-disk ULID blocks and replaying WAL/WBL/chunks_head.
    pub fn open(dir: impl AsRef<Path>, opts: TsdbOptions) -> Result<Self, TsdbError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let blocks = Self::load_blocks(&dir)?;
        let min_valid_time = blocks
            .iter()
            .filter(|b| !b.meta().out_of_order && !b.meta().compaction.from_out_of_order())
            .map(|b| b.max_time())
            .max()
            .unwrap_or(i64::MIN);

        let compactions_total = opts.head.registry.as_ref().map(|reg| {
            let c = prometheus::Counter::new(
                "prometheus_tsdb_compactions_total",
                "Total number of compactions that were executed for the partition.",
            )
            .expect("valid counter");
            let _ = reg.register(Box::new(c.clone()));
            c
        });

        let head = Head::open(&dir, opts.head.clone(), min_valid_time)?;

        Ok(Self {
            dir,
            opts,
            head,
            blocks: RwLock::new(blocks),
            compact_mtx: Mutex::new(()),
            compactions_total,
        })
    }

    fn load_blocks(dir: &Path) -> Result<Vec<Arc<Block>>, TsdbError> {
        let dirs = list_block_dirs(dir)?;
        if dirs.len() <= 1 {
            let mut blocks = Vec::with_capacity(dirs.len());
            for (_, bdir) in dirs {
                let blk = Block::open(&bdir)?;
                blocks.push(Arc::new(blk));
            }
            blocks.sort_by_key(|b| b.min_time());
            return Ok(blocks);
        }

        let workers = dirs.len().min(
            std::thread::available_parallelism()
                .map_or(4, |n| n.get())
                .clamp(2, 16),
        );
        let next_idx = std::sync::atomic::AtomicUsize::new(0);
        let results: Mutex<Vec<Result<Arc<Block>, BlockError>>> =
            Mutex::new(Vec::with_capacity(dirs.len()));

        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| loop {
                    let idx = next_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some((_, ref bdir)) = dirs.get(idx) else {
                        break;
                    };
                    let res = Block::open(bdir).map(Arc::new);
                    results.lock().push(res);
                });
            }
        });

        let mut blocks = Vec::with_capacity(dirs.len());
        for res in results.into_inner() {
            blocks.push(res?);
        }
        blocks.sort_by_key(|b| b.min_time());
        Ok(blocks)
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

    /// Compacts any ready `chunk_range` windows from Head into ULID blocks on disk.
    pub fn compact(&self) -> Result<Vec<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        let mut created = Vec::new();
        let mut last_block_maxt = i64::MIN;

        while self.head.compactable() {
            let mint = self.head.min_time();
            let maxt = range_for_timestamp(mint, self.head.options().chunk_range_ms);
            if let Some(uid) = self.compact_head_range_locked(mint, maxt, true)? {
                created.push(uid);
            }
            last_block_maxt = maxt;
        }

        if last_block_maxt != i64::MIN {
            self.head.truncate_wal(last_block_maxt)?;
            if let Some(uid) = self.compact_ooo_head_locked()? {
                created.push(uid);
            }
        }

        Ok(created)
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
        let meta_opt = write_head_block_streaming(
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
        )?;

        let uid = if let Some(meta) = meta_opt {
            if let Some(ref c) = self.compactions_total {
                c.inc();
            }
            let reloaded = Self::load_blocks(&self.dir)?;
            let mut blocks_guard = self.blocks.write();
            *blocks_guard = reloaded;
            if truncate_memory {
                self.head.truncate_memory(maxt)?;
            }
            Some(meta.ulid)
        } else {
            if truncate_memory {
                self.head.truncate_memory(maxt)?;
            }
            None
        };
        Ok(uid)
    }

    /// Compacts all out-of-order samples in Head into an OOO ULID block on disk and clears OOO state.
    pub fn compact_ooo_head(&self) -> Result<Option<Ulid>, TsdbError> {
        let _guard = self.compact_mtx.lock();
        self.compact_ooo_head_locked()
    }

    fn compact_ooo_head_locked(&self) -> Result<Option<Ulid>, TsdbError> {
        let (candidates, mint, maxt_inclusive) = self.head.collect_ooo_compaction_candidates()?;
        if candidates.is_empty() || mint > maxt_inclusive {
            return Ok(None);
        }
        let maxt = maxt_inclusive.saturating_add(1);
        let mut compaction = BlockMetaCompaction::default();
        compaction.set_out_of_order();
        let Some(meta) = write_head_block_streaming(
            &self.dir,
            mint,
            maxt,
            candidates,
            compaction,
            true,
            self.opts.max_block_chunk_segment_size,
            |ref_id, chks, tombs| {
                self.head
                    .load_series_ooo_chunks_into(ref_id, chks, tombs)
                    .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
            },
        )?
        else {
            return Ok(None);
        };
        let uid = meta.ulid;
        if let Some(ref c) = self.compactions_total {
            c.inc();
        }
        let reloaded = Self::load_blocks(&self.dir)?;
        let mut blocks_guard = self.blocks.write();
        *blocks_guard = reloaded;
        self.head.clear_ooo_data()?;
        Ok(Some(uid))
    }

    /// Compacts the selected series refs into a ULID block on disk and evicts them from Head.
    pub fn compact_selected_series(&self, refs: &[u64]) -> Result<Option<Ulid>, TsdbError> {
        if refs.is_empty() {
            return Ok(None);
        }
        let _guard = self.compact_mtx.lock();
        let (candidates, mint, maxt_inclusive) =
            self.head.collect_selected_compaction_candidates(refs)?;
        if candidates.is_empty() || mint > maxt_inclusive {
            return Ok(None);
        }
        let maxt = maxt_inclusive.saturating_add(1);
        let mut compaction = BlockMetaCompaction::default();
        compaction.set_selected_series();
        let Some(meta) = write_head_block_streaming(
            &self.dir,
            mint,
            maxt,
            candidates,
            compaction,
            false,
            self.opts.max_block_chunk_segment_size,
            |ref_id, chks, tombs| {
                self.head
                    .load_series_in_order_chunks_into(ref_id, mint, maxt_inclusive, chks, tombs)
                    .map_err(|e| CompactError::Io(io::Error::other(e.to_string())))
            },
        )?
        else {
            return Ok(None);
        };
        let uid = meta.ulid;
        if let Some(ref c) = self.compactions_total {
            c.inc();
        }
        let reloaded = Self::load_blocks(&self.dir)?;
        let mut blocks_guard = self.blocks.write();
        *blocks_guard = reloaded;
        self.head.evict_series_refs(refs)?;
        Ok(Some(uid))
    }

    /// Reloads on-disk ULID blocks (e.g., after retention deletion).
    pub fn reload_blocks(&self) -> Result<(), TsdbError> {
        let _guard = self.compact_mtx.lock();
        let reloaded = Self::load_blocks(&self.dir)?;
        *self.blocks.write() = reloaded;
        Ok(())
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
}
