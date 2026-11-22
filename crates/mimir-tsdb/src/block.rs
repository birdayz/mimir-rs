//! Prometheus TSDB on-disk ULID block reader and `meta.json` schema (including Thanos extension).
//!
//! Ported from github.com/prometheus/prometheus/tsdb/block.go
//! and github.com/grafana/mimir/pkg/storage/tsdb/block/meta.go.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Condvar, Mutex};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use ulid::Ulid;

use crate::chunks_block::{BlockChunksError, BlockChunksReader};
use crate::index::{IndexError, IndexReader, INDEX_FILENAME, INDEX_FORMAT_V2};
use crate::matcher::LabelMatcher;
use crate::postings::{postings_for_matchers, PostingsError};
use crate::postings_cache::{
    next_postings_cache_source_id, PostingsForMatchersCache, PostingsForMatchersCacheConfig,
};
use crate::tombstones::{read_tombstones_file, write_tombstones_file, Interval, MemTombstones, TombstoneError, TOMBSTONES_FILENAME};

pub const META_FILENAME: &str = "meta.json";
pub const META_VERSION_1: i32 = 1;

pub const COMPACTION_HINT_FROM_OUT_OF_ORDER: &str = "from-out-of-order";
pub const COMPACTION_HINT_FROM_STALE_SERIES: &str = "from-stale-series";
pub const COMPACTION_HINT_FROM_SELECTED_SERIES: &str = "from-selected-series";

#[derive(Debug, Error)]
pub enum BlockError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unexpected meta file version {0}")]
    UnsupportedMetaVersion(i32),
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("postings error: {0}")]
    Postings(#[from] PostingsError),
    #[error("chunks error: {0}")]
    Chunks(#[from] BlockChunksError),
    #[error("tombstones error: {0}")]
    Tombstones(#[from] TombstoneError),
}

mod ulid_serde {
    use super::*;

    pub fn serialize<S: Serializer>(val: &Ulid, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&val.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Ulid, D::Error> {
        let s = String::deserialize(de)?;
        Ulid::from_string(&s).map_err(serde::de::Error::custom)
    }
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

mod ulid_vec_serde {
    use super::*;

    pub fn serialize<S: Serializer>(vals: &[Ulid], ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = ser.serialize_seq(Some(vals.len()))?;
        for v in vals {
            seq.serialize_element(&v.to_string())?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<Ulid>, D::Error> {
        let strs = Option::<Vec<String>>::deserialize(de)?.unwrap_or_default();
        strs.into_iter()
            .map(|s| Ulid::from_string(&s).map_err(serde::de::Error::custom))
            .collect()
    }
}

/// Statistics about a block's contents (`tsdb.BlockStats`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockStats {
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_u64")]
    pub num_samples: u64,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_u64")]
    pub num_float_samples: u64,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_u64")]
    pub num_histogram_samples: u64,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_u64")]
    pub num_series: u64,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_u64")]
    pub num_chunks: u64,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_u64")]
    pub num_tombstones: u64,
}

impl BlockStats {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// Short description of a parent block (`tsdb.BlockDesc`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockDesc {
    #[serde(with = "ulid_serde")]
    pub ulid: Ulid,
    pub min_time: i64,
    pub max_time: i64,
}

/// Compaction metadata (`tsdb.BlockMetaCompaction`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockMetaCompaction {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub level: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "ulid_vec_serde")]
    pub sources: Vec<Ulid>,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_false")]
    pub deletable: bool,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub parents: Vec<BlockDesc>,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_false")]
    pub failed: bool,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<String>,
}

impl BlockMetaCompaction {
    pub fn set_out_of_order(&mut self) {
        if !self.from_out_of_order() {
            self.hints.push(COMPACTION_HINT_FROM_OUT_OF_ORDER.to_string());
            self.hints.sort();
        }
    }

    pub fn from_out_of_order(&self) -> bool {
        self.hints.iter().any(|h| h == COMPACTION_HINT_FROM_OUT_OF_ORDER)
    }

    pub fn set_stale_series(&mut self) {
        if !self.from_stale_series() {
            self.hints.push(COMPACTION_HINT_FROM_STALE_SERIES.to_string());
            self.hints.sort();
        }
    }

    pub fn from_stale_series(&self) -> bool {
        self.hints.iter().any(|h| h == COMPACTION_HINT_FROM_STALE_SERIES)
    }

    pub fn set_selected_series(&mut self) {
        if !self.from_selected_series() {
            self.hints
                .push(COMPACTION_HINT_FROM_SELECTED_SERIES.to_string());
            self.hints.sort();
        }
    }

    pub fn from_selected_series(&self) -> bool {
        self.hints
            .iter()
            .any(|h| h == COMPACTION_HINT_FROM_SELECTED_SERIES)
    }
}

/// Thanos downsample metadata (`block.ThanosDownsample`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThanosDownsample {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub resolution: i64,
}

/// Thanos file entry (`block.File`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThanosFile {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub rel_path: String,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_i64")]
    pub size_bytes: i64,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

/// Thanos extension in `meta.json` (`block.ThanosMeta`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThanosMeta {
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "is_zero_i32")]
    pub version: i32,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub downsample: ThanosDownsample,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub source: String,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub segment_files: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<ThanosFile>,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

/// Prometheus TSDB `meta.json` schema with optional Thanos extension.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    #[serde(with = "ulid_serde")]
    pub ulid: Ulid,
    #[serde(rename = "minTime")]
    pub min_time: i64,
    #[serde(rename = "maxTime")]
    pub max_time: i64,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub stats: BlockStats,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub compaction: BlockMetaCompaction,
    pub version: i32,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub out_of_order: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thanos: Option<ThanosMeta>,
}

impl BlockMeta {
    /// Checks whether the half-open block interval `[min_time, max_time)` overlaps closed `[mint, maxt]`.
    #[inline]
    pub const fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time <= maxt && mint < self.max_time
    }
}

/// Reads `<dir>/meta.json` and validates version == 1.
pub fn read_meta_file(dir: &Path) -> Result<BlockMeta, BlockError> {
    let bytes = fs::read(dir.join(META_FILENAME))?;
    let meta: BlockMeta = serde_json::from_slice(&bytes)?;
    if meta.version != META_VERSION_1 {
        return Err(BlockError::UnsupportedMetaVersion(meta.version));
    }
    Ok(meta)
}

/// Fsyncs a directory entry so renames/creations inside `dir` are durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

/// Atomically writes `<dir>/meta.json` with `version = 1` and tab indentation (matching Prometheus).
pub fn write_meta_file(dir: &Path, meta: &mut BlockMeta) -> Result<usize, BlockError> {
    meta.version = META_VERSION_1;
    let path = dir.join(META_FILENAME);
    let tmp = dir.join(format!("{META_FILENAME}.tmp"));
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"\t");
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
    meta.serialize(&mut ser)?;
    buf.push(b'\n');
    let write_res = (|| -> io::Result<()> {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, &path)?;
        Ok(())
    })();
    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp);
        return Err(BlockError::Io(e));
    }
    sync_dir(dir)?;
    Ok(buf.len())
}

/// Returns `Some(Ulid)` if `name` is a valid 26-character ULID block directory name.
pub fn parse_block_dir_name(name: &str) -> Option<Ulid> {
    if name.len() != 26 {
        return None;
    }
    Ulid::from_string(name).ok()
}

/// Lists all valid ULID block subdirectories in `dir`, sorted by `min_time` ascending.
pub fn list_block_dirs(dir: &Path) -> Result<Vec<(Ulid, PathBuf)>, BlockError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut blocks = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(name_str) = fname.to_str() else {
            continue;
        };
        let Some(ulid) = parse_block_dir_name(name_str) else {
            continue;
        };
        if entry.file_type()?.is_dir() {
            blocks.push((ulid, entry.path()));
        }
    }
    blocks.sort_by_key(|(id, _)| *id);
    Ok(blocks)
}

#[derive(Debug, Default)]
struct BlockReaderState {
    closing: bool,
    pending: usize,
}

#[derive(Debug, Default)]
pub(crate) struct BlockReaders {
    state: Mutex<BlockReaderState>,
    cv: Condvar,
}

impl BlockReaders {
    fn try_start_read(&self) -> bool {
        let mut st = self.state.lock();
        if st.closing {
            return false;
        }
        st.pending += 1;
        true
    }

    fn inc_unconditional(&self) {
        let mut st = self.state.lock();
        st.pending += 1;
    }

    fn finish_read(&self) {
        let mut st = self.state.lock();
        st.pending = st.pending.saturating_sub(1);
        if st.pending == 0 {
            self.cv.notify_all();
        }
    }

    fn close_and_wait(&self) {
        let mut st = self.state.lock();
        st.closing = true;
        while st.pending > 0 {
            self.cv.wait(&mut st);
        }
    }
}

/// Active reader guard on a [`Block`], ensuring `reload_blocks_locked` waits for
/// concurrent queriers to finish and drop their `Arc<Block>` mmaps before unlinking
/// obsolete block directories (matching Go `Block.Close()`).
#[derive(Debug)]
pub struct BlockReadGuard {
    block: Option<Arc<Block>>,
    readers: Arc<BlockReaders>,
}

impl Clone for BlockReadGuard {
    fn clone(&self) -> Self {
        self.readers.inc_unconditional();
        Self {
            block: self.block.clone(),
            readers: Arc::clone(&self.readers),
        }
    }
}

impl Drop for BlockReadGuard {
    fn drop(&mut self) {
        // Drop the `Arc<Block>` BEFORE decrementing `pending` so the caller of
        // `close_and_wait()` is guaranteed no reader thread still holds a live mmap.
        drop(self.block.take());
        self.readers.finish_read();
    }
}

/// Repairs Prometheus 2.1 blocks whose `meta.json` has `version == 2` and whose `index`
/// file header mistakenly carries version `1` instead of `2`.
///
/// Ported from github.com/prometheus/prometheus/tsdb/repair.go (`repairBadIndexVersion`).
pub fn repair_bad_index_version(dir: &Path) -> Result<(), BlockError> {
    let dirs = list_block_dirs(dir)?;
    struct TmpGuard(Vec<PathBuf>);
    impl Drop for TmpGuard {
        fn drop(&mut self) {
            for p in &self.0 {
                let _ = fs::remove_file(p);
            }
        }
    }
    let mut tmp_guard = TmpGuard(Vec::new());
    for (_, d) in dirs {
        let meta_path = d.join(META_FILENAME);
        let Ok(bytes) = fs::read(&meta_path) else {
            continue;
        };
        let Ok(mut meta) = serde_json::from_slice::<BlockMeta>(&bytes) else {
            continue;
        };
        if meta.version != META_VERSION_1 && meta.version != 2 {
            continue;
        }
        if meta.version == META_VERSION_1 {
            continue;
        }
        let repl_path = d.join("index.repaired");
        tmp_guard.0.push(repl_path.clone());
        let idx_path = d.join(INDEX_FILENAME);
        {
            let mut broken = File::open(&idx_path)?;
            let mut repl = File::create(&repl_path)?;
            io::copy(&mut broken, &mut repl)?;
            repl.seek(SeekFrom::Start(4))?;
            repl.write_all(&[INDEX_FORMAT_V2])?;
            repl.sync_all()?;
        }
        fs::rename(&repl_path, &idx_path)?;
        meta.version = META_VERSION_1;
        write_meta_file(&d, &mut meta)?;
    }
    Ok(())
}

/// Opened immutable on-disk Prometheus ULID block (`tsdb.Block`).
#[derive(Debug)]
pub struct Block {
    dir: PathBuf,
    meta: BlockMeta,
    index: IndexReader,
    chunks: BlockChunksReader,
    tombstones: MemTombstones,
    total_size: AtomicU64,
    delete_mtx: Mutex<()>,
    postings_cache: Arc<PostingsForMatchersCache>,
    postings_cache_source_id: u64,
    compaction_failed: AtomicBool,
    compaction_deletable: AtomicBool,
    readers: Arc<BlockReaders>,
    lookup_planner: OnceLock<Arc<dyn Any + Send + Sync>>,
}

impl Block {
    /// Opens an on-disk ULID block directory, validating `meta.json`, `index`, `chunks/`, and `tombstones`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, BlockError> {
        Self::open_with_postings_cache(dir, PostingsForMatchersCacheConfig::default())
    }

    /// Opens a block with the given per-block `PostingsForMatchersCache` configuration
    /// (Mimir `block_postings_for_matchers_cache_*`).
    pub fn open_with_postings_cache(
        dir: impl AsRef<Path>,
        postings_cache: PostingsForMatchersCacheConfig,
    ) -> Result<Self, BlockError> {
        Self::open_with_shared_postings_cache(
            dir,
            Arc::new(PostingsForMatchersCache::new(postings_cache)),
        )
    }

    /// Opens a block sharing the supplied `Arc<PostingsForMatchersCache>` (used when
    /// `shared_postings_for_matchers_cache: true`), assigning a unique `source_id` so
    /// cache keys never collide across blocks or tenants.
    pub fn open_with_shared_postings_cache(
        dir: impl AsRef<Path>,
        postings_cache: Arc<PostingsForMatchersCache>,
    ) -> Result<Self, BlockError> {
        let dir = dir.as_ref().to_path_buf();
        let meta = read_meta_file(&dir)?;
        let compaction_failed = AtomicBool::new(meta.compaction.failed);
        let compaction_deletable = AtomicBool::new(meta.compaction.deletable);
        let index = IndexReader::open(dir.join(INDEX_FILENAME))?;
        let chunks = BlockChunksReader::open(dir.join("chunks"))?;
        let tombstones = read_tombstones_file(&dir)?;

        let tomb_size = fs::metadata(dir.join(TOMBSTONES_FILENAME))
            .map(|m| m.len())
            .unwrap_or(0);
        let meta_size = fs::metadata(dir.join(META_FILENAME))
            .map(|m| m.len())
            .unwrap_or(0);
        let total_size =
            (index.size() as u64) + (chunks.total_size() as u64) + tomb_size + meta_size;

        Ok(Self {
            dir,
            meta,
            index,
            chunks,
            tombstones,
            total_size: AtomicU64::new(total_size),
            delete_mtx: Mutex::new(()),
            postings_cache,
            postings_cache_source_id: next_postings_cache_source_id(),
            compaction_failed,
            compaction_deletable,
            readers: Arc::new(BlockReaders::default()),
            lookup_planner: OnceLock::new(),
        })
    }

    /// Persists `compaction.failed = true` to `<dir>/meta.json` and marks this block
    /// failed in memory (matching Go `Block.setCompactionFailed` in `tsdb/block.go:519`).
    pub fn set_compaction_failed(&self) -> Result<(), BlockError> {
        let mut m = self.meta.clone();
        m.compaction.failed = true;
        write_meta_file(&self.dir, &mut m)?;
        self.compaction_failed.store(true, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn compaction_failed(&self) -> bool {
        self.meta.compaction.failed || self.compaction_failed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_deletable(&self) {
        self.compaction_deletable.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_deletable(&self) -> bool {
        self.meta.compaction.deletable || self.compaction_deletable.load(Ordering::Acquire)
    }

    #[inline]
    pub fn num_tombstones(&self) -> u64 {
        self.tombstones.total()
    }

    /// Creates a hardlinked snapshot of this block in `dest_dir/<ulid>` (Go `Block.Snapshot`).
    pub fn snapshot(&self, dest_dir: &Path) -> Result<(), BlockError> {
        let block_dir = dest_dir.join(self.meta.ulid.to_string());
        let chunks_dir = block_dir.join("chunks");
        fs::create_dir_all(&chunks_dir)?;
        for fname in [META_FILENAME, INDEX_FILENAME, TOMBSTONES_FILENAME] {
            let src = self.dir.join(fname);
            if fname == TOMBSTONES_FILENAME && !src.exists() {
                write_tombstones_file(&block_dir, &MemTombstones::new())?;
            } else {
                fs::hard_link(&src, block_dir.join(fname))?;
            }
        }
        let cur_chunks_dir = self.dir.join("chunks");
        for entry in fs::read_dir(&cur_chunks_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                fs::hard_link(entry.path(), chunks_dir.join(entry.file_name()))?;
            }
        }
        Ok(())
    }

    /// Registers an active reader on this block unless it is already closing.
    pub fn start_read(block: &Arc<Block>) -> Option<BlockReadGuard> {
        if block.readers.try_start_read() {
            Some(BlockReadGuard {
                block: Some(Arc::clone(block)),
                readers: Arc::clone(&block.readers),
            })
        } else {
            None
        }
    }

    /// Marks this block as closing and waits until all active [`BlockReadGuard`]s drop.
    pub fn close_and_wait(&self) {
        self.readers.close_and_wait();
    }

    /// Returns the cached index lookup planner for this block, initializing it once on first use.
    #[inline]
    pub fn get_or_init_lookup_planner<F>(
        &self,
        init: F,
    ) -> &Arc<dyn Any + Send + Sync>
    where
        F: FnOnce() -> Arc<dyn Any + Send + Sync>,
    {
        self.lookup_planner.get_or_init(init)
    }

    #[inline]
    pub fn postings_cache(&self) -> &PostingsForMatchersCache {
        &self.postings_cache
    }

    pub fn set_postings_cache_eligibility(&self, force: bool, shared: bool) {
        self.postings_cache.set_eligibility(force, shared);
    }

    /// Evaluates `matchers` against this block's index using its `PostingsForMatchersCache`.
    pub fn postings_for_matchers(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<Arc<[u64]>, PostingsError> {
        self.postings_for_matchers_with_concurrent(matchers, false)
    }

    /// Evaluates `matchers` against this block's index using its `PostingsForMatchersCache`,
    /// passing `concurrent` (Go `sharded` / `hints.ShardCount > 0`).
    pub fn postings_for_matchers_with_concurrent(
        &self,
        matchers: &[LabelMatcher],
        concurrent: bool,
    ) -> Result<Arc<[u64]>, PostingsError> {
        self.postings_cache.get_or_compute_for_source(
            self.postings_cache_source_id,
            matchers,
            concurrent,
            || postings_for_matchers(&self.index, matchers),
        )
    }

    /// Evaluates `matchers` directly against this block's index, bypassing `PostingsForMatchersCache`.
    pub fn postings_for_matchers_direct(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<u64>, PostingsError> {
        postings_for_matchers(&self.index, matchers)
    }

    #[inline]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    #[inline]
    pub fn meta(&self) -> &BlockMeta {
        &self.meta
    }

    #[inline]
    pub fn ulid(&self) -> Ulid {
        self.meta.ulid
    }

    #[inline]
    pub fn min_time(&self) -> i64 {
        self.meta.min_time
    }

    #[inline]
    pub fn max_time(&self) -> i64 {
        self.meta.max_time
    }

    #[inline]
    pub fn index(&self) -> &IndexReader {
        &self.index
    }

    #[inline]
    pub fn chunks(&self) -> &BlockChunksReader {
        &self.chunks
    }

    #[inline]
    pub fn tombstones(&self) -> &MemTombstones {
        &self.tombstones
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.total_size.load(Ordering::Acquire)
    }

    /// Persists tombstones for series matching `matchers` with chunks overlapping `[mint, maxt]`
    /// (`Block.Delete` in `block.go:648-713`).
    pub fn delete(
        &self,
        mint: i64,
        maxt: i64,
        matchers: &[LabelMatcher],
    ) -> Result<(), BlockError> {
        let _guard = self.delete_mtx.lock();
        let refs = postings_for_matchers(&self.index, matchers)?;
        for r in refs {
            let (_, chks) = self.index.series(r)?;
            if chks.is_empty() {
                continue;
            }
            if chks.iter().any(|c| c.overlaps_closed_interval(mint, maxt)) {
                let first_min = chks[0].min_time;
                let last_max = chks[chks.len() - 1].max_time;
                let tmin = mint.max(first_min);
                let tmax = maxt.min(last_max);
                self.tombstones.add_interval(r, Interval { mint: tmin, maxt: tmax });
            }
        }
        let tomb_n = write_tombstones_file(&self.dir, &self.tombstones)?;
        let mut m = self.meta.clone();
        m.stats.num_tombstones = self.tombstones.total();
        let meta_n = write_meta_file(&self.dir, &mut m)?;
        let new_size = (self.index.size() as u64)
            + (self.chunks.total_size() as u64)
            + (tomb_n as u64)
            + (meta_n as u64);
        self.total_size.store(new_size, Ordering::Release);
        Ok(())
    }

    /// Go `Block.GetSymbolTableSize` (`index.Reader.SymbolTableSize`).
    #[inline]
    pub fn symbol_table_size(&self) -> u64 {
        self.index.symbol_table_size()
    }

    #[inline]
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.meta.overlaps_closed_interval(mint, maxt)
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        self.postings_cache
            .remove_source(self.postings_cache_source_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_json_roundtrip_prometheus_and_thanos() {
        let dir = tempfile::tempdir().unwrap();
        let ulid = Ulid::new();
        let mut meta = BlockMeta {
            ulid,
            min_time: 1000,
            max_time: 7_201_000,
            stats: BlockStats {
                num_samples: 10,
                num_float_samples: 10,
                num_histogram_samples: 0,
                num_series: 2,
                num_chunks: 2,
                num_tombstones: 0,
            },
            compaction: BlockMetaCompaction {
                level: 1,
                sources: vec![ulid],
                deletable: false,
                parents: Vec::new(),
                failed: false,
                hints: Vec::new(),
            },
            version: META_VERSION_1,
            out_of_order: false,
            thanos: None,
        };

        write_meta_file(dir.path(), &mut meta).unwrap();
        let text = fs::read_to_string(dir.path().join(META_FILENAME)).unwrap();
        assert!(!text.contains("\"thanos\""));
        assert!(text.contains(&ulid.to_string()));

        let read_back = read_meta_file(dir.path()).unwrap();
        assert_eq!(read_back, meta);
    }

    #[test]
    fn meta_json_serializes_empty_stats_and_deserializes_omitted_stats() {
        let dir = tempfile::tempdir().unwrap();
        let ulid = Ulid::new();
        let mut meta = BlockMeta {
            ulid,
            min_time: 1000,
            max_time: 7_201_000,
            stats: BlockStats::default(),
            compaction: BlockMetaCompaction {
                level: 1,
                sources: vec![ulid],
                deletable: false,
                parents: Vec::new(),
                failed: false,
                hints: Vec::new(),
            },
            version: META_VERSION_1,
            out_of_order: false,
            thanos: None,
        };
        write_meta_file(dir.path(), &mut meta).unwrap();
        let text = fs::read_to_string(dir.path().join(META_FILENAME)).unwrap();
        assert!(text.contains(r#""stats": {}"#), "expected empty stats object in {text}");
        assert_eq!(read_meta_file(dir.path()).unwrap(), meta);

        // Backward-compatibility: JSON omitting "stats" still deserializes to default BlockStats.
        let without_stats = format!(
            "{{\n\t\"ulid\": \"{ulid}\",\n\t\"minTime\": 1000,\n\t\"maxTime\": 7201000,\n\t\"compaction\": {{\n\t\t\"level\": 1,\n\t\t\"sources\": [\"{ulid}\"]\n\t}},\n\t\"version\": 1,\n\t\"out_of_order\": false\n}}\n"
        );
        fs::write(dir.path().join(META_FILENAME), without_stats).unwrap();
        assert_eq!(read_meta_file(dir.path()).unwrap(), meta);
    }

    #[test]
    fn block_open_fails_when_chunks_dir_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let ulid = Ulid::new();
        let bdir = dir.path().join(ulid.to_string());
        fs::create_dir_all(&bdir).unwrap();
        let mut meta = BlockMeta {
            ulid,
            min_time: 0,
            max_time: 1000,
            stats: BlockStats::default(),
            compaction: BlockMetaCompaction::default(),
            version: META_VERSION_1,
            out_of_order: false,
            thanos: None,
        };
        write_meta_file(&bdir, &mut meta).unwrap();
        let iw = crate::index::IndexWriter::new(bdir.join(INDEX_FILENAME)).unwrap();
        iw.close().unwrap();
        crate::tombstones::write_tombstones_file(&bdir, &MemTombstones::new()).unwrap();
        // Deliberately omit chunks/ directory.
        let err = Block::open(&bdir).expect_err("missing chunks/ must fail Block::open");
        assert!(matches!(err, BlockError::Chunks(BlockChunksError::Io(_))));
    }
}
