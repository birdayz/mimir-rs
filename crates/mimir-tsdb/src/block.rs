//! Prometheus TSDB on-disk ULID block reader and `meta.json` schema (including Thanos extension).
//!
//! Ported from github.com/prometheus/prometheus/tsdb/block.go
//! and github.com/grafana/mimir/pkg/storage/tsdb/block/meta.go.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use ulid::Ulid;

use crate::chunks_block::{BlockChunksError, BlockChunksReader};
use crate::index::{IndexError, IndexReader, INDEX_FILENAME};
use crate::matcher::LabelMatcher;
use crate::postings::{postings_for_matchers, PostingsError};
use crate::postings_cache::{PostingsForMatchersCache, PostingsForMatchersCacheConfig};
use crate::tombstones::{read_tombstones_file, MemTombstones, TombstoneError, TOMBSTONES_FILENAME};

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
    #[serde(default, deserialize_with = "deserialize_null_default", skip_serializing_if = "BlockStats::is_empty")]
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

/// Opened immutable on-disk Prometheus ULID block (`tsdb.Block`).
#[derive(Debug)]
pub struct Block {
    dir: PathBuf,
    meta: BlockMeta,
    index: IndexReader,
    chunks: BlockChunksReader,
    tombstones: MemTombstones,
    total_size: u64,
    postings_cache: PostingsForMatchersCache,
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
        let dir = dir.as_ref().to_path_buf();
        let meta = read_meta_file(&dir)?;
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
            total_size,
            postings_cache: PostingsForMatchersCache::new(postings_cache),
        })
    }

    /// Evaluates `matchers` against this block's index using its `PostingsForMatchersCache`.
    pub fn postings_for_matchers(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<Arc<[u64]>, PostingsError> {
        self.postings_cache
            .get_or_compute(matchers, || postings_for_matchers(&self.index, matchers))
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
        self.total_size
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
}
