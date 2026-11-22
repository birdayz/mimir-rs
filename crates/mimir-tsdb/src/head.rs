//! Striped TSDB Head with zero-allocation hot-path appends, OOO support, mmap `chunks_head`,
//! WAL/WBL logging & crash recovery replay, and truncation/checkpointing.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/head.go,
//! github.com/prometheus/prometheus/tsdb/head_append.go,
//! github.com/prometheus/prometheus/tsdb/head_read.go,
//! and github.com/prometheus/prometheus/tsdb/head_wal.go.

use std::cell::UnsafeCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicPtr, AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
    Ordering,
};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hashbrown::HashTable;
use parking_lot::{Condvar, Mutex, RwLock};
use mimir_common::native_histogram::SingleHistogram;
use prometheus::{CounterVec, Opts, Registry};
use smallvec::SmallVec;
use thiserror::Error;

use crate::chunkenc::bstream::BitStream;
use crate::chunkenc::float_histogram::AppendFloatHistogramOutcome;
use crate::chunkenc::float_histogram_st::AppendFloatHistogramStOutcome;
use crate::chunkenc::histogram::AppendHistogramOutcome;
use crate::chunkenc::histogram_st::AppendHistogramStOutcome;
use crate::chunkenc::{
    is_stale_nan, ChunkError, EncodedChunk, Encoding, FloatHistogram, FloatHistogramAppenderState,
    FloatHistogramChunk, FloatHistogramStChunk, Histogram, HistogramAppenderState, HistogramChunk,
    HistogramStChunk, OooChunk, OooSampleValue, StEncoder, Xor2AppendState, Xor2Chunk,
    XorAppendState, XorChunk, MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND,
    MIN_SAMPLES_PER_HISTOGRAM_CHUNK, QUIET_ZERO_NAN_BITS, TARGET_BYTES_PER_HISTOGRAM_CHUNK,
};
use crate::chunks_block::ChunkMeta;
use crate::compact::{
    decode_chunk_samples, merge_head_chunks_with_tombstones, slice_chunk_prefix,
    CompactionSeries, SampleValue,
};
use crate::exemplar::{CircularExemplarStorage, Exemplar, ExemplarError};
use crate::head_chunks::{
    ChunkDiskMapperRef, FlushedRefWatermark, HeadChunksError, HeadChunksReader,
    HeadChunksWriter, MAX_HEAD_CHUNK_FILE_SIZE, OOO_CHUNK_ID_MASK,
};
use crate::isolation::{Isolation, IsolationStateInfo, TxRing};
use crate::labels::{encode_stringlabel_size, iter_stringlabels, Labels, SymbolLabels, METRIC_NAME};
use crate::metadata::{MetadataError, MetadataLimits, UserMetricsMetadata};
use crate::metrics::{native_histogram, FnGaugeCollector};
use crate::postings::{
    postings_for_matchers, MemPostings, PostingsBatchScratch, PostingsError,
};
use crate::postings_cache::{
    next_postings_cache_source_id, PostingsForMatchersCache, PostingsForMatchersCacheConfig,
};
use crate::matcher::LabelMatcher;
use crate::encoding::{Decbuf, Encbuf};
use crate::record::{
    decode_float_histogram, decode_histogram, decode_labels, encode_float_histogram,
    encode_histogram, encode_labels, DecodedRecord, MetricType, RecordDecoder, RecordEncoder,
    RecordError, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata,
    RefMmapMarker, RefSample, RefSeries,
};
use crate::symbols::{SymbolId, SymbolTable, SymbolTableError};
use crate::tombstones::{decode_tombstones, encode_tombstones, is_completely_deleted, Interval, MemTombstones};
use crate::wlog::{
    create_checkpoint, decompress_record_into, delete_checkpoints,
    delete_temp_checkpoints, last_checkpoint, read_min_valid_time, segments_range,
    CompressionType, SegmentTracker, WlogError, WlogReader, WlogWriter,
    DEFAULT_SEGMENT_SIZE, WBL_DIR_NAME,
};

pub const DEFAULT_BLOCK_DURATION_MS: i64 = 2 * 60 * 60 * 1000; // 2h
pub const DEFAULT_SAMPLES_PER_CHUNK: usize = 120;
pub const DEFAULT_STRIPE_SIZE: usize = 16384;
pub const DEFAULT_MAX_EXEMPLARS: usize = 100_000;
const DEFAULT_SYMBOL_GC_ARENA_GROWTH_BYTES: u64 = 128 * 1024 * 1024;
pub const DEFAULT_OOO_CAP_MAX: usize = 32;
const CHUNK_SNAPSHOT_PREFIX: &str = "chunk_snapshot.";
const CHUNK_SNAPSHOT_RECORD_TYPE_SERIES: u8 = 1;
const CHUNK_SNAPSHOT_RECORD_TYPE_TOMBSTONES: u8 = 2;
const CHUNK_SNAPSHOT_RECORD_TYPE_EXEMPLARS: u8 = 3;
const SERIES_STATE_FILENAME: &str = "series_state.json";

/// Series lifecycle state persisted in `wal/series_state.json` (Go `SeriesLifecycleState`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeriesLifecycleState {
    pub last_series_id: u64,
    pub last_wal_segment: i64,
    pub clean_shutdown: bool,
}

#[derive(Debug, Error)]
pub enum HeadError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("WAL/WBL error: {0}")]
    Wlog(#[from] WlogError),
    #[error("record error: {0}")]
    Record(#[from] RecordError),
    #[error("head chunks error: {0}")]
    HeadChunks(#[from] HeadChunksError),
    #[error("chunk error: {0}")]
    Chunk(#[from] ChunkError),
    #[error("postings error: {0}")]
    Postings(#[from] PostingsError),
    #[error("exemplar error: {0}")]
    Exemplar(#[from] ExemplarError),
    #[error("metadata error: {0}")]
    Metadata(#[from] MetadataError),
    #[error("symbol table error: {0}")]
    SymbolTable(#[from] SymbolTableError),
    #[error("invalid head option: {0}")]
    InvalidOption(String),
    #[error("{0}")]
    Repair(String),
    #[error("out of bounds")]
    OutOfBounds,
    #[error("out of order sample")]
    OutOfOrderSample,
    #[error("too old sample")]
    TooOldSample,
    #[error("duplicate sample for timestamp")]
    DuplicateSampleForTimestamp,
    #[error("{0}")]
    DuplicateSampleWithDetails(String),
    #[error("{0}")]
    HistogramValidation(crate::chunkenc::HistogramValidationError),
    #[error("start timestamp newer than sample")]
    StNewerThanSample,
    #[error("unknown series ref {0}")]
    UnknownSeriesRef(u64),
    #[error("appender closed")]
    AppenderClosed,
    #[error("head closed")]
    Closed,
    #[error("ingester max in-memory series limit reached")]
    MaxInMemorySeriesReached,
    #[error("per-user series limit exceeded")]
    MaxSeriesPerUser,
    #[error("per-metric series limit exceeded")]
    MaxSeriesPerMetric,
    /// `errNoMetricNameLabel` (`extract.MetricNameFromLabels` in Mimir's `PreCreation`).
    #[error("no metric name label")]
    MissingMetricName,
    /// `getOrCreate`: `fmt.Errorf("empty labelset: %w", ErrInvalidSample)`.
    #[error("empty labelset: invalid sample")]
    EmptyLabelset,
    #[error("label name \"{0}\" is not unique: invalid sample")]
    DuplicateLabelName(String),
}

impl HeadError {
    /// Go `errors.Is(err, storage.ErrDuplicateSampleForTimestamp)`.
    #[inline]
    pub fn is_duplicate_sample(&self) -> bool {
        matches!(
            self,
            HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_)
        )
    }
}

/// Lifecycle callback trait matching `prometheus/tsdb.SeriesLifecycleCallback`.
pub trait SeriesLifecycleCallback: Send + Sync {
    fn pre_creation(&self, pairs: &[(&str, &str)]) -> Result<(), HeadError>;
    fn post_creation(&self, ref_id: u64, labels: &Labels);
    fn post_deletion(&self, deleted: &[(u64, Labels)]);

    #[doc(hidden)]
    fn chunk_end_time_variance_override(&self) -> Option<f64> {
        None
    }

    #[doc(hidden)]
    fn inner_lifecycle_callback(&self) -> Option<&Arc<dyn SeriesLifecycleCallback>> {
        None
    }
}

struct ChunkEndTimeVarianceCallback {
    variance: f64,
    inner: Option<Arc<dyn SeriesLifecycleCallback>>,
}

impl SeriesLifecycleCallback for ChunkEndTimeVarianceCallback {
    fn pre_creation(&self, pairs: &[(&str, &str)]) -> Result<(), HeadError> {
        match self.inner {
            Some(ref inner) => inner.pre_creation(pairs),
            None => Ok(()),
        }
    }

    fn post_creation(&self, ref_id: u64, labels: &Labels) {
        if let Some(ref inner) = self.inner {
            inner.post_creation(ref_id, labels);
        }
    }

    fn post_deletion(&self, deleted: &[(u64, Labels)]) {
        if let Some(ref inner) = self.inner {
            inner.post_deletion(deleted);
        }
    }

    fn chunk_end_time_variance_override(&self) -> Option<f64> {
        Some(self.variance)
    }

    fn inner_lifecycle_callback(&self) -> Option<&Arc<dyn SeriesLifecycleCallback>> {
        self.inner.as_ref()
    }
}

#[derive(Clone)]
pub struct HeadOptions {
    pub chunk_range_ms: i64,
    pub samples_per_chunk: usize,
    pub stripe_size: usize,
    pub enable_exemplar_storage: bool,
    pub max_exemplars: usize,
    pub ooo_time_window_ms: i64,
    pub ooo_cap_max: usize,
    pub enable_st_storage: bool,
    pub float_chunk_encoding: Encoding,
    pub enable_histogram_st_encoding: bool,
    pub wal_compression: CompressionType,
    pub wal_segment_size: usize,
    pub wal_replay_concurrency: usize,
    pub max_head_chunk_file_size: usize,
    /// Go `HeadChunksWriteQueueSize`: m-mapped chunks are written synchronously in Rust;
    /// a non-zero size registers the Go `chunk_write_queue` operation counters.
    pub head_chunks_write_queue_size: usize,
    /// Go `HeadChunksWriteBufferSize`: `chunks_head` writer buffer size in bytes.
    pub head_chunks_write_buffer_size: usize,
    /// Go `Options.TimelyCompaction` (Mimir `timely_head_compaction_enabled`, default off).
    pub timely_compaction: bool,
    /// Mimir `head_postings_for_matchers_cache_{ttl,max_bytes,versions}`.
    pub postings_cache: PostingsForMatchersCacheConfig,
    /// Optional shared `PostingsForMatchersCache` across all tenant Heads when
    /// `shared_postings_for_matchers_cache: true`.
    pub shared_postings_cache: Option<Arc<PostingsForMatchersCache>>,
    pub metadata_limits: MetadataLimits,
    pub series_lifecycle_callback: Option<Arc<dyn SeriesLifecycleCallback>>,
    pub registry: Option<Arc<prometheus::Registry>>,
}

impl std::fmt::Debug for HeadOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeadOptions")
            .field("chunk_range_ms", &self.chunk_range_ms)
            .field("samples_per_chunk", &self.samples_per_chunk)
            .field("stripe_size", &self.stripe_size)
            .field("enable_exemplar_storage", &self.enable_exemplar_storage)
            .field("max_exemplars", &self.max_exemplars)
            .field("ooo_time_window_ms", &self.ooo_time_window_ms)
            .field("ooo_cap_max", &self.ooo_cap_max)
            .field("enable_st_storage", &self.enable_st_storage)
            .field("float_chunk_encoding", &self.float_chunk_encoding)
            .field(
                "enable_histogram_st_encoding",
                &self.enable_histogram_st_encoding,
            )
            .field("wal_compression", &self.wal_compression)
            .field("wal_segment_size", &self.wal_segment_size)
            .field("wal_replay_concurrency", &self.wal_replay_concurrency)
            .field("max_head_chunk_file_size", &self.max_head_chunk_file_size)
            .field(
                "head_chunks_write_queue_size",
                &self.head_chunks_write_queue_size,
            )
            .field(
                "head_chunks_write_buffer_size",
                &self.head_chunks_write_buffer_size,
            )
            .field("timely_compaction", &self.timely_compaction)
            .field("postings_cache", &self.postings_cache)
            .field(
                "shared_postings_cache",
                &self.shared_postings_cache.as_ref().map(|_| "..."),
            )
            .field("metadata_limits", &self.metadata_limits)
            .field(
                "series_lifecycle_callback",
                &self.series_lifecycle_callback.as_ref().map(|_| "..."),
            )
            .field("registry", &self.registry.as_ref().map(|_| "..."))
            .finish()
    }
}

impl HeadOptions {
    /// Configures `ChunkEndTimeVariance` (0.0 disables jitter) without adding a struct field
    /// that would break existing `HeadOptions` struct literals.
    pub fn with_chunk_end_time_variance(mut self, v: f64) -> Self {
        let inner = match self.series_lifecycle_callback.take() {
            Some(cb) if cb.chunk_end_time_variance_override().is_some() => {
                cb.inner_lifecycle_callback().cloned()
            }
            other => other,
        };
        self.series_lifecycle_callback = Some(Arc::new(ChunkEndTimeVarianceCallback {
            variance: v,
            inner,
        }));
        self
    }

    /// Returns the configured `ChunkEndTimeVariance` (default `0.0`).
    #[inline]
    pub fn chunk_end_time_variance(&self) -> f64 {
        self.series_lifecycle_callback
            .as_ref()
            .and_then(|cb| cb.chunk_end_time_variance_override())
            .unwrap_or(0.0)
    }
}

impl Default for HeadOptions {
    fn default() -> Self {
        Self {
            chunk_range_ms: DEFAULT_BLOCK_DURATION_MS,
            samples_per_chunk: DEFAULT_SAMPLES_PER_CHUNK,
            stripe_size: DEFAULT_STRIPE_SIZE,
            enable_exemplar_storage: true,
            max_exemplars: DEFAULT_MAX_EXEMPLARS,
            ooo_time_window_ms: 0,
            ooo_cap_max: DEFAULT_OOO_CAP_MAX,
            enable_st_storage: false,
            float_chunk_encoding: Encoding::Xor,
            enable_histogram_st_encoding: false,
            wal_compression: CompressionType::None,
            wal_segment_size: DEFAULT_SEGMENT_SIZE,
            wal_replay_concurrency: 0,
            max_head_chunk_file_size: MAX_HEAD_CHUNK_FILE_SIZE,
            head_chunks_write_queue_size: 0,
            head_chunks_write_buffer_size: crate::head_chunks::DEFAULT_WRITE_BUFFER_SIZE,
            timely_compaction: false,
            postings_cache: PostingsForMatchersCacheConfig::default(),
            shared_postings_cache: None,
            metadata_limits: MetadataLimits::default(),
            series_lifecycle_callback: None,
            registry: None,
        }
    }
}

fn format_go_g(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() { "+Inf" } else { "-Inf" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    let sci = format!("{v:e}");
    let Some((mant, exp_str)) = sci.split_once('e') else {
        return sci;
    };
    let Ok(exp) = exp_str.parse::<i32>() else {
        return sci;
    };
    if !(-4..6).contains(&exp) {
        return if exp >= 0 {
            format!("{mant}e+{exp:02}")
        } else {
            format!("{mant}e-{:02}", exp.unsigned_abs())
        };
    }
    let (sign, unsigned_mant) = match mant.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", mant),
    };
    let digits: String = unsigned_mant.chars().filter(|&c| c != '.').collect();
    let dp = exp + 1;
    if dp <= 0 {
        format!("{sign}0.{}{digits}", "0".repeat((-dp) as usize))
    } else {
        let dp_usize = dp as usize;
        if dp_usize >= digits.len() {
            format!("{sign}{digits}{}", "0".repeat(dp_usize - digits.len()))
        } else {
            format!("{sign}{}.{}", &digits[..dp_usize], &digits[dp_usize..])
        }
    }
}

#[inline]
pub const fn range_for_timestamp(t: i64, width: i64) -> i64 {
    if width <= 0 {
        return t;
    }
    let t128 = t as i128;
    let w128 = width as i128;
    let res = (t128 / w128) * w128 + w128;
    if res > i64::MAX as i128 {
        i64::MAX
    } else if res < i64::MIN as i128 {
        i64::MIN
    } else {
        res as i64
    }
}

#[inline]
fn compute_chunk_end_time(start: i64, cur: i64, max_t: i64, ratio_to_full: f64) -> i64 {
    let span = (max_t as i128).saturating_sub(start as i128);
    let elapsed = (cur as i128).saturating_sub(start as i128).saturating_add(1);
    if span <= 0 || elapsed <= 0 || ratio_to_full <= 0.0 {
        return max_t;
    }
    let n = (span as f64) / ((elapsed as f64) * ratio_to_full);
    if !n.is_finite() || n <= 1.0 {
        return max_t;
    }
    let step = ((span as f64) / n.floor()) as i128;
    let end = (start as i128).saturating_add(step);
    end.clamp(start as i128, max_t as i128) as i64
}

/// Ported from `addJitterToChunkEndTime` in `prometheus/tsdb/head_append.go`.
#[inline]
pub fn add_jitter_to_chunk_end_time(
    series_hash: u64,
    chunk_min_time: i64,
    next_at: i64,
    max_next_at: i64,
    variance: f64,
) -> i64 {
    if !variance.is_finite() || variance <= 0.0 || next_at >= max_next_at {
        return next_at;
    }
    let chunk_duration = (next_at as i128) - (chunk_min_time as i128);
    if chunk_duration <= 0 {
        return next_at;
    }
    let max_var_f = (chunk_duration as f64) * variance;
    if !max_var_f.is_finite() || max_var_f < 1.0 {
        return next_at;
    }
    let chunk_duration_max_variance = max_var_f.min(i64::MAX as f64) as i64;
    if chunk_duration_max_variance <= 0 {
        return next_at;
    }
    let chunk_duration_variance = (series_hash % (chunk_duration_max_variance as u64)) as i128;
    let adjusted =
        (next_at as i128) + chunk_duration_variance - ((chunk_duration_max_variance as i128) / 2);
    adjusted
        .min(max_next_at as i128)
        .clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Zero-allocation hash of sorted `(&str, &str)` pairs matching Prometheus `stringlabels.Labels.Hash()`.
#[inline]
pub fn hash_sorted_pairs(pairs: &[(&str, &str)]) -> u64 {
    let mut stack_buf = [MaybeUninit::<u8>::uninit(); 512];
    let mut pos = 0usize;
    let mut heap_buf: Option<Vec<u8>> = None;

    for &(k, v) in pairs {
        let need = (if k.len() < 255 { 1 } else { 4 })
            + k.len()
            + (if v.len() < 255 { 1 } else { 4 })
            + v.len();

        if let Some(ref mut hb) = heap_buf {
            encode_stringlabel_size(hb, k.len());
            hb.extend_from_slice(k.as_bytes());
            encode_stringlabel_size(hb, v.len());
            hb.extend_from_slice(v.as_bytes());
        } else if pos + need <= stack_buf.len() {
            pos = write_stringlabel_to_uninit(&mut stack_buf, pos, k.as_bytes());
            pos = write_stringlabel_to_uninit(&mut stack_buf, pos, v.as_bytes());
        } else {
            let mut hb = Vec::with_capacity((pos + need) * 2);
            // SAFETY: `stack_buf[..pos]` was initialized byte-for-byte in prior iterations.
            let prefix = unsafe { std::slice::from_raw_parts(stack_buf.as_ptr() as *const u8, pos) };
            hb.extend_from_slice(prefix);
            encode_stringlabel_size(&mut hb, k.len());
            hb.extend_from_slice(k.as_bytes());
            encode_stringlabel_size(&mut hb, v.len());
            hb.extend_from_slice(v.as_bytes());
            heap_buf = Some(hb);
        }
    }

    match heap_buf {
        Some(ref hb) => xxh64_oneshot(hb),
        None => {
            // SAFETY: `stack_buf[..pos]` was initialized byte-for-byte above.
            let slice = unsafe { std::slice::from_raw_parts(stack_buf.as_ptr() as *const u8, pos) };
            xxh64_oneshot(slice)
        }
    }
}

#[inline]
fn xxh64_oneshot(data: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(data, 0)
}

#[inline]
fn write_stringlabel_to_uninit(
    dst: &mut [MaybeUninit<u8>; 512],
    mut pos: usize,
    bytes: &[u8],
) -> usize {
    let len = bytes.len();
    let ptr = dst.as_mut_ptr() as *mut u8;
    // SAFETY: caller ensures `pos + need <= 512`.
    unsafe {
        if len < 255 {
            *ptr.add(pos) = len as u8;
            pos += 1;
        } else {
            *ptr.add(pos) = 255;
            *ptr.add(pos + 1) = (len & 0xff) as u8;
            *ptr.add(pos + 2) = ((len >> 8) & 0xff) as u8;
            *ptr.add(pos + 3) = ((len >> 16) & 0xff) as u8;
            pos += 4;
        }
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.add(pos), len);
    }
    pos + len
}

#[derive(Clone, Debug)]
struct MemChunk {
    chunk: EncodedChunk,
    min_time: i64,
    max_time: i64,
}

#[derive(Clone, Copy, Debug)]
struct MmappedChunk {
    ref_id: ChunkDiskMapperRef,
    min_time: i64,
    max_time: i64,
    num_samples: u16,
    encoding: Encoding,
}

impl MmappedChunk {
    #[inline]
    fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time <= maxt && mint <= self.max_time
    }
}

#[derive(Debug)]
struct OooHeadChunk {
    chunk: OooChunk,
    min_time: i64,
    max_time: i64,
}

impl OooHeadChunk {
    #[inline]
    fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time <= maxt && mint <= self.max_time
    }
}

#[derive(Debug, Clone)]
struct PendingOooMmap {
    mc: MmappedChunk,
    /// `HeadChunksWriter::discard_epoch()` when `mc.ref_id` was buffered, or `None` if
    /// discarded/unassigned and awaiting re-write before promotion.
    write_epoch: Option<u64>,
    chunk: EncodedChunk,
}

#[derive(Debug, Default)]
struct SeriesOooState {
    ooo_head_chunk: Option<OooHeadChunk>,
    /// Encoded chunks from cut `ooo_head_chunk`s written to `HeadChunksWriter`, awaiting
    /// `writer.flush()` and `chunk_reader` refresh before promotion to `ooo_mmapped_chunks`.
    ooo_pending_mmapped: Vec<PendingOooMmap>,
    ooo_mmapped_chunks: Vec<MmappedChunk>,
    first_ooo_chunk_id: u32,
}

#[inline]
const fn next_mmapped_cap(cur_cap: usize) -> usize {
    match cur_cap {
        0 => 2,
        1..=3 => 4,
        4..=5 => 6,
        6..=7 => 8,
        8..=11 => 12,
        n => n + (n / 2),
    }
}

#[inline]
fn push_mmapped_chunk(v: &mut Vec<MmappedChunk>, mc: MmappedChunk) {
    if v.len() == v.capacity() {
        let target = next_mmapped_cap(v.capacity());
        v.reserve_exact(target - v.len());
    }
    v.push(mc);
}

#[inline]
fn shrink_mmapped_vec(v: &mut Vec<MmappedChunk>) {
    if v.is_empty() {
        if v.capacity() > 0 {
            *v = Vec::new();
        }
    } else if v.capacity() > next_mmapped_cap(v.len()) {
        v.shrink_to(v.len());
    }
}

impl SeriesOooState {
    #[inline]
    fn is_empty(&self) -> bool {
        self.ooo_head_chunk.is_none()
            && self.ooo_pending_mmapped.is_empty()
            && self.ooo_mmapped_chunks.is_empty()
    }

    fn shrink_oversized_vecs(&mut self) {
        shrink_mmapped_vec(&mut self.ooo_mmapped_chunks);
        if self.ooo_pending_mmapped.is_empty() {
            if self.ooo_pending_mmapped.capacity() > 0 {
                self.ooo_pending_mmapped = Vec::new();
            }
        } else if self.ooo_pending_mmapped.capacity()
            > (self.ooo_pending_mmapped.len() * 2).max(4)
        {
            self.ooo_pending_mmapped
                .shrink_to(self.ooo_pending_mmapped.len());
        }
    }

    /// Promotes pending OOO chunks verified flushed by `wm`, returning `true` if no pending
    /// entries remain on this series. Any entry whose `ref_id` was discarded by an earlier
    /// `flush_write_buf()` failure has its `write_epoch` cleared to `None` and `ref_id` reset
    /// to `ChunkDiskMapperRef(0)` so it can never alias another series's disk chunk.
    fn promote_pending_with_watermark(&mut self, wm: &FlushedRefWatermark) -> bool {
        if self.ooo_pending_mmapped.is_empty() {
            return true;
        }
        for mut p in std::mem::take(&mut self.ooo_pending_mmapped) {
            if wm.is_flushed(p.mc.ref_id, p.write_epoch) {
                push_mmapped_chunk(&mut self.ooo_mmapped_chunks, p.mc);
            } else {
                if wm.is_discarded(p.mc.ref_id, p.write_epoch) {
                    p.write_epoch = None;
                    p.mc.ref_id = ChunkDiskMapperRef(0);
                }
                self.ooo_pending_mmapped.push(p);
            }
        }
        self.ooo_pending_mmapped.is_empty()
    }
}

#[derive(Clone, Debug)]
enum LastSample {
    None,
    Float(f64),
    Histogram(Box<Histogram>),
    FloatHistogram(Box<FloatHistogram>),
}

const _: () = assert!(std::mem::size_of::<LastSample>() <= 16);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesMetadata {
    pub metric_type: MetricType,
    pub unit: String,
    pub help: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CompactSeriesMetadata {
    metric_type: MetricType,
    unit: SymbolId,
    help: SymbolId,
}

const ACTIVE_DELTA_NONE: u32 = u32::MAX;
const ACTIVE_DELTA_COLD: u32 = u32::MAX - 1;

// `flags` bit layout in `SeriesState`:
// - bits 0..=5   (6 bits): xor_leading (0..=31, or 63 = 0xff uninitialized)
// - bits 6..=11  (6 bits): xor_trailing (0..=63)
// - bit  12      (1 bit):  FLAG_XOR_VALID
// - bits 13..=14 (2 bits): LastSampleKind (0=None, 1=Float, 2=Histogram, 3=FloatHistogram)
// - bits 15..=17 (3 bits): MetricType (0..=7)
// - bit  18      (1 bit):  FLAG_HAS_METADATA
// - bit  19      (1 bit):  FLAG_COMPUTED_HIST_END_TIME
// - bit  20      (1 bit):  FLAG_GCED
// - bit  21      (1 bit):  FLAG_HAS_LAST_EXEMPLAR
// - bits 22..=24 (3 bits): active_chunk Encoding (0=None..=6=FloatHistogramSt)
const XOR_LEADING_MASK: u32 = 0x3f;
const XOR_LEADING_UNINIT: u32 = 0x3f;
const XOR_TRAILING_SHIFT: u32 = 6;
const XOR_TRAILING_MASK: u32 = 0x3f << XOR_TRAILING_SHIFT;
const FLAG_XOR_VALID: u32 = 1 << 12;
const LAST_SAMPLE_KIND_SHIFT: u32 = 13;
const LAST_SAMPLE_KIND_MASK: u32 = 0b11 << LAST_SAMPLE_KIND_SHIFT;
const META_TYPE_SHIFT: u32 = 15;
const META_TYPE_MASK: u32 = 0b111 << META_TYPE_SHIFT;
const FLAG_HAS_METADATA: u32 = 1 << 18;
const FLAG_COMPUTED_HIST_END_TIME: u32 = 1 << 19;
const FLAG_GCED: u32 = 1 << 20;
const FLAG_HAS_LAST_EXEMPLAR: u32 = 1 << 21;
const ACTIVE_ENC_SHIFT: u32 = 22;
const ACTIVE_ENC_MASK: u32 = 0b111 << ACTIVE_ENC_SHIFT;

#[inline(always)]
pub fn prefetch_read_data<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr.cast::<i8>(), core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, readonly, preserves_flags)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

const PENDING_COMMIT_GCED_BIT: u32 = 1 << 31;
const PENDING_COMMIT_COUNT_MASK: u32 = !PENDING_COMMIT_GCED_BIT;

pub type AfterSeriesLookupHook = Arc<dyn Fn(Option<&SeriesEntry>) + Send + Sync>;

#[derive(Debug)]
pub struct SeriesEntry {
    pub ref_id: u64,
    pub hash: u64,
    pub labels: Labels,
    active_cache: AtomicU64,
    in_order_max_time: AtomicI64,
    pending_commits: AtomicU32,
    inner: Mutex<SeriesState>,
}

impl SeriesEntry {
    #[inline(always)]
    fn new(ref_id: u64, hash: u64, labels: Labels, init_pending: u32) -> Self {
        Self {
            ref_id,
            hash,
            labels,
            active_cache: AtomicU64::new(0),
            in_order_max_time: AtomicI64::new(i64::MIN),
            pending_commits: AtomicU32::new(init_pending),
            inner: Mutex::new(SeriesState::new()),
        }
    }

    #[doc(hidden)]
    pub fn new_for_test(ref_id: u64, hash: u64, labels: Labels) -> Self {
        Self::new(ref_id, hash, labels, 0)
    }

    #[inline(always)]
    pub fn active_cache(&self) -> u64 {
        self.active_cache.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn set_active_cache(&self, packed: u64) {
        self.active_cache.store(packed, Ordering::Release);
    }

    #[inline(always)]
    pub fn is_gced(&self) -> bool {
        (self.pending_commits.load(Ordering::Acquire) & PENDING_COMMIT_GCED_BIT) != 0
    }

    #[inline(always)]
    fn mark_gced(&self) {
        self.pending_commits
            .fetch_or(PENDING_COMMIT_GCED_BIT, Ordering::Release);
    }

    #[inline(always)]
    pub fn pending_commits_count(&self) -> u32 {
        self.pending_commits.load(Ordering::Acquire) & PENDING_COMMIT_COUNT_MASK
    }

    #[inline(always)]
    fn try_inc_pending_commit(&self) -> bool {
        let mut cur = self.pending_commits.load(Ordering::Acquire);
        loop {
            if (cur & PENDING_COMMIT_GCED_BIT) != 0 {
                return false;
            }
            match self.pending_commits.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(next) => cur = next,
            }
        }
    }

    #[inline(always)]
    fn try_mark_gced(&self) -> bool {
        self.pending_commits
            .compare_exchange(0, PENDING_COMMIT_GCED_BIT, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    #[inline(always)]
    fn unmark_pending_commit(&self) -> bool {
        let mut cur = self.pending_commits.load(Ordering::Acquire);
        loop {
            if (cur & PENDING_COMMIT_COUNT_MASK) == 0 {
                return false;
            }
            let next = (cur & PENDING_COMMIT_GCED_BIT) | ((cur & PENDING_COMMIT_COUNT_MASK) - 1);
            match self.pending_commits.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    #[doc(hidden)]
    pub fn unmark_pending_commit_for_test(&self) -> bool {
        self.unmark_pending_commit()
    }
}

struct PendingCommitGuard<'g>(&'g SeriesEntry, &'g HeadMetrics);

impl Drop for PendingCommitGuard<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        if !self.0.unmark_pending_commit() {
            self.1.pending_commit_underflow.inc();
        }
    }
}

#[doc(hidden)]
type CommitPauseLatch = Arc<(Mutex<bool>, Condvar)>;
type ReplayMmappedParts = (Vec<(u64, MmappedChunk, Option<usize>)>, Vec<ChunkMeta>);

pub struct TestCommitPauseHandle<'a> {
    head_map: &'a Mutex<HashMap<u64, CommitPauseLatch>>,
    has_flag: &'a AtomicBool,
    ref_id: u64,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl TestCommitPauseHandle<'_> {
    pub fn resume(&self) {
        let mut g = self.gate.0.lock();
        *g = true;
        self.gate.1.notify_all();
        let mut map = self.head_map.lock();
        map.remove(&self.ref_id);
        self.has_flag.store(!map.is_empty(), Ordering::Release);
    }
}

impl Drop for TestCommitPauseHandle<'_> {
    fn drop(&mut self) {
        self.resume();
    }
}

struct TxCleanupGuard<'a> {
    state: parking_lot::MutexGuard<'a, SeriesState>,
    head: &'a Head,
    ref_id: u64,
    bound: u64,
}

impl std::ops::Deref for TxCleanupGuard<'_> {
    type Target = SeriesState;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl std::ops::DerefMut for TxCleanupGuard<'_> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl Drop for TxCleanupGuard<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        if self.bound > 0 {
            self.head.txs_cleanup_below(self.ref_id, self.bound);
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FastU64Hasher(u64);

impl std::hash::Hasher for FastU64Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self
                .0
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(b as u64);
        }
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 32;
        self.0 = x;
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FastU64BuildHasher;

impl std::hash::BuildHasher for FastU64BuildHasher {
    type Hasher = FastU64Hasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        FastU64Hasher(0)
    }
}

type FastU64Map<V> = hashbrown::HashMap<u64, V, FastU64BuildHasher>;

#[derive(Clone, Debug)]
enum HistAppendCache {
    Histogram(Option<HistogramAppenderState>),
    HistogramSt(Option<(HistogramAppenderState, StEncoder)>),
    FloatHistogram(Option<FloatHistogramAppenderState>),
    FloatHistogramSt(Option<(FloatHistogramAppenderState, StEncoder)>),
}

#[derive(Debug, Default)]
enum LastColdSample {
    #[default]
    None,
    Histogram(Box<Histogram>),
    FloatHistogram(Box<FloatHistogram>),
}

#[derive(Debug, Default)]
struct SeriesColdState {
    extra_head_chunks: Vec<MemChunk>,
    ooo: Option<Box<SeriesOooState>>,
    first_chunk_id: u32,
    created_timestamp: i64,
    last_exemplar_ts: i64,
    active_max_time_override: i64,
    next_at_override: i64,
    last_hist: LastColdSample,
    xor2_state: Option<Box<Xor2AppendState>>,
    hist_state: Option<Box<HistAppendCache>>,
}

#[derive(Debug)]
struct SeriesState {
    active_bstream: BitStream,
    active_min_time: i64,
    last_float_bits: u64,
    mmapped_chunks: Vec<MmappedChunk>,
    cold: Option<Box<SeriesColdState>>,
    active_max_time_delta: u32,
    next_at_delta: u32,
    xor_t_delta: u32,
    flags: u32,
    meta_unit: SymbolId,
    meta_help: SymbolId,
}

impl SeriesState {
    fn new() -> Self {
        Self {
            active_bstream: BitStream::new(),
            active_min_time: i64::MAX,
            last_float_bits: 0,
            mmapped_chunks: Vec::new(),
            cold: None,
            active_max_time_delta: ACTIVE_DELTA_NONE,
            next_at_delta: ACTIVE_DELTA_NONE,
            xor_t_delta: 0,
            flags: 0,
            meta_unit: SymbolId::EMPTY,
            meta_help: SymbolId::EMPTY,
        }
    }

    #[inline(always)]
    fn active_encoding(&self) -> Encoding {
        match (self.flags & ACTIVE_ENC_MASK) >> ACTIVE_ENC_SHIFT {
            1 => Encoding::Xor,
            2 => Encoding::Histogram,
            3 => Encoding::FloatHistogram,
            4 => Encoding::Xor2,
            5 => Encoding::HistogramSt,
            6 => Encoding::FloatHistogramSt,
            _ => Encoding::None,
        }
    }

    #[inline(always)]
    fn set_active_encoding(&mut self, enc: Encoding) {
        self.flags = (self.flags & !ACTIVE_ENC_MASK) | (u32::from(enc.as_u8()) << ACTIVE_ENC_SHIFT);
    }

    #[inline(always)]
    fn has_active_chunk(&self) -> bool {
        (self.flags & ACTIVE_ENC_MASK) != 0
    }

    #[inline]
    fn active_chunk_num_samples(&self) -> usize {
        let enc = self.active_encoding();
        let min_hdr = match enc {
            Encoding::None => return 0,
            Encoding::Xor => 2,
            _ => 3,
        };
        let bytes = self.active_bstream.bytes();
        if bytes.len() < min_hdr {
            return 0;
        }
        let raw = u16::from_be_bytes([bytes[0], bytes[1]]);
        match enc {
            Encoding::HistogramSt | Encoding::FloatHistogramSt => {
                (raw & crate::chunkenc::st::HISTOGRAM_ST_SAMPLE_COUNT_MASK) as usize
            }
            _ => raw as usize,
        }
    }

    #[inline]
    fn take_active_chunk(&mut self) -> Option<EncodedChunk> {
        let enc = self.active_encoding();
        if enc == Encoding::None {
            return None;
        }
        let b = std::mem::take(&mut self.active_bstream);
        self.set_active_encoding(Encoding::None);
        Some(EncodedChunk::from_bstream(enc, b))
    }

    #[inline]
    fn set_active_chunk(&mut self, chunk: EncodedChunk) {
        let (enc, b) = chunk.into_bstream();
        self.set_active_encoding(enc);
        self.active_bstream = b;
    }

    #[inline]
    fn clear_active_chunk(&mut self) {
        self.active_bstream = BitStream::new();
        self.set_active_encoding(Encoding::None);
    }

    #[inline]
    fn cold_mut(&mut self) -> &mut SeriesColdState {
        self.cold.get_or_insert_with(Default::default)
    }

    #[inline]
    fn ooo(&self) -> Option<&SeriesOooState> {
        self.cold.as_ref().and_then(|c| c.ooo.as_deref())
    }

    #[inline]
    fn ooo_mut(&mut self) -> Option<&mut SeriesOooState> {
        self.cold.as_mut().and_then(|c| c.ooo.as_deref_mut())
    }

    #[inline]
    fn ooo_or_insert(&mut self) -> &mut SeriesOooState {
        self.cold_mut()
            .ooo
            .get_or_insert_with(|| Box::new(SeriesOooState::default()))
    }

    #[inline]
    fn clear_ooo(&mut self) {
        if let Some(ref mut c) = self.cold {
            c.ooo = None;
        }
        self.maybe_drop_cold();
    }

    #[inline]
    fn first_chunk_id(&self) -> u32 {
        self.cold.as_ref().map_or(0, |c| c.first_chunk_id)
    }

    #[inline]
    fn advance_first_chunk_id(&mut self, removed: usize) {
        if removed == 0 {
            return;
        }
        let cur = self.first_chunk_id();
        if cur == 0 {
            return;
        }
        let next = ((cur as usize + removed) % (OOO_CHUNK_ID_MASK as usize)) as u32;
        self.cold_mut().first_chunk_id = next;
    }

    fn collect_in_order_chunks_for_tx_iso(
        &self,
        tx_ring: Option<&TxRing>,
        mint: i64,
        maxt_inclusive: i64,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<ReplayMmappedParts, HeadError> {
        let use_iso = tx_iso.filter(|info| !tx_is_fast_path(tx_ring, info));
        let mut total_samples = 0usize;
        if use_iso.is_some() {
            for mc in self.mmapped() {
                total_samples += mc.num_samples as usize;
            }
            if let Some(ref c) = self.cold {
                for hc in &c.extra_head_chunks {
                    total_samples += hc.chunk.num_samples();
                }
            }
            if self.has_active_chunk() {
                total_samples += self.active_chunk_num_samples();
            }
        }

        let mut prev_samples = 0usize;
        let mut in_order_pos = 0u64;
        let mut mmapped_in_order = Vec::new();
        for mc in self.mmapped() {
            let num_s = mc.num_samples as usize;
            let stop_after = match use_iso {
                Some(info) => tx_stop_after_for_chunk(tx_ring, info, total_samples, prev_samples, num_s),
                None => num_s,
            };
            prev_samples += num_s;
            if mc.overlaps_closed_interval(mint, maxt_inclusive) && stop_after > 0 {
                let trim = (stop_after < num_s).then_some(stop_after);
                mmapped_in_order.push((in_order_pos, *mc, trim));
                in_order_pos += 1;
            }
        }

        let mut head_chks = Vec::with_capacity(self.head_chunks_len());
        let mut push_hc = |hc_chunk: EncodedChunk, hc_min: i64, hc_max: i64| -> Result<(), HeadError> {
            let num_s = hc_chunk.num_samples();
            let stop_after = match use_iso {
                Some(info) => tx_stop_after_for_chunk(tx_ring, info, total_samples, prev_samples, num_s),
                None => num_s,
            };
            prev_samples += num_s;
            if hc_min <= maxt_inclusive && mint <= hc_max && stop_after > 0 {
                if stop_after < num_s {
                    if let Some((sliced, smint, smaxt)) = slice_chunk_prefix(&hc_chunk, stop_after)? {
                        if smint <= maxt_inclusive && mint <= smaxt {
                            head_chks.push(ChunkMeta {
                                ref_id: in_order_pos,
                                min_time: smint,
                                max_time: smaxt,
                                chunk: Some(sliced),
                            });
                            in_order_pos += 1;
                        }
                    }
                } else {
                    head_chks.push(ChunkMeta {
                        ref_id: in_order_pos,
                        min_time: hc_min,
                        max_time: hc_max,
                        chunk: Some(hc_chunk),
                    });
                    in_order_pos += 1;
                }
            }
            Ok(())
        };
        if let Some(ref c) = self.cold {
            for hc in &c.extra_head_chunks {
                push_hc(hc.chunk.clone(), hc.min_time, hc.max_time)?;
            }
        }
        let enc = self.active_encoding();
        if enc != Encoding::None {
            let chunk = EncodedChunk::from_bstream(enc, self.active_bstream.clone());
            push_hc(chunk, self.active_min_time, self.active_max_time())?;
        }
        Ok((mmapped_in_order, head_chks))
    }
}

fn tx_is_fast_path(ring: Option<&TxRing>, info: &IsolationStateInfo) -> bool {
    let last = ring.and_then(TxRing::last_append_id);
    if info.incomplete_appends.is_empty() {
        last.is_none_or(|id| id <= info.max_append_id)
    } else {
        last.is_none()
    }
}

fn tx_has_append_id_above(ring: Option<&TxRing>, bound: u64) -> bool {
    let Some(ring) = ring else {
        return false;
    };
    let mut it = ring.iterator();
    for _ in 0..ring.len() {
        if it.at() > bound {
            return true;
        }
        it.next_pos();
    }
    false
}

fn tx_stop_after_for_chunk(
    ring: Option<&TxRing>,
    info: &IsolationStateInfo,
    total_samples: usize,
    previous_samples: usize,
    num_samples: usize,
) -> usize {
    let Some(ring) = ring else {
        return num_samples;
    };
        let tx_count = ring.len();
        let samples_after = total_samples.saturating_sub(previous_samples + num_samples);
        if tx_count <= samples_after {
            return num_samples;
        }
    let consider = tx_count - samples_after;
    let mut it = ring.iterator();
    for index in 0..consider {
        let append_id = it.at();
        if append_id <= info.max_append_id && !info.incomplete_appends.contains(&append_id) {
            it.next_pos();
            continue;
        }
        return num_samples.saturating_sub(consider - index);
    }
    num_samples
}

impl SeriesState {

    #[inline]
    fn clear_ooo_if_empty(&mut self) {
        let should_clear = self.ooo().is_some_and(SeriesOooState::is_empty);
        if should_clear {
            self.clear_ooo();
        } else if let Some(ooo) = self.ooo_mut() {
            ooo.shrink_oversized_vecs();
        }
    }

    #[inline]
    fn maybe_drop_cold(&mut self) {
        let should_drop = if let Some(ref c) = self.cold {
            c.extra_head_chunks.is_empty()
                && c.ooo.is_none()
                && c.first_chunk_id == 0
                && c.created_timestamp == 0
                && (self.flags & FLAG_HAS_LAST_EXEMPLAR) == 0
                && self.active_max_time_delta != ACTIVE_DELTA_COLD
                && self.next_at_delta != ACTIVE_DELTA_COLD
                && matches!(c.last_hist, LastColdSample::None)
                && c.xor2_state.is_none()
                && c.hist_state.is_none()
        } else {
            false
        };
        if should_drop {
            self.cold = None;
        }
    }

    #[inline]
    fn created_timestamp(&self) -> i64 {
        self.cold.as_ref().map_or(0, |c| c.created_timestamp)
    }

    #[inline]
    fn record_created_timestamp(&mut self, st: i64) {
        if st != 0 && self.created_timestamp() == 0 {
            self.cold_mut().created_timestamp = st;
        }
    }

    #[inline]
    fn active_max_time(&self) -> i64 {
        match self.active_max_time_delta {
            ACTIVE_DELTA_NONE => i64::MIN,
            ACTIVE_DELTA_COLD => self
                .cold
                .as_ref()
                .map_or(i64::MIN, |c| c.active_max_time_override),
            delta => self.active_min_time.wrapping_add(i64::from(delta)),
        }
    }

    #[inline]
    fn set_active_max_time(&mut self, max_time: i64) {
        if max_time == i64::MIN {
            self.active_max_time_delta = ACTIVE_DELTA_NONE;
            return;
        }
        if max_time >= self.active_min_time {
            let diff = i128::from(max_time) - i128::from(self.active_min_time);
            if diff < i128::from(ACTIVE_DELTA_COLD) {
                self.active_max_time_delta = diff as u32;
                return;
            }
        }
        self.active_max_time_delta = ACTIVE_DELTA_COLD;
        self.cold_mut().active_max_time_override = max_time;
    }

    #[inline]
    fn next_at(&self) -> i64 {
        match self.next_at_delta {
            ACTIVE_DELTA_NONE => i64::MIN,
            ACTIVE_DELTA_COLD => self.cold.as_ref().map_or(i64::MIN, |c| c.next_at_override),
            delta => self.active_min_time.wrapping_add(i64::from(delta)),
        }
    }

    #[inline]
    fn set_next_at(&mut self, next_at: i64) {
        if next_at == i64::MIN {
            self.next_at_delta = ACTIVE_DELTA_NONE;
            return;
        }
        if next_at >= self.active_min_time {
            let diff = i128::from(next_at) - i128::from(self.active_min_time);
            if diff < i128::from(ACTIVE_DELTA_COLD) {
                self.next_at_delta = diff as u32;
                return;
            }
        }
        self.next_at_delta = ACTIVE_DELTA_COLD;
        self.cold_mut().next_at_override = next_at;
    }

    #[inline]
    fn head_chunks_is_empty(&self) -> bool {
        !self.has_active_chunk()
    }

    #[inline]
    fn head_chunks_len(&self) -> usize {
        usize::from(self.has_active_chunk())
            + self.cold.as_ref().map_or(0, |c| c.extra_head_chunks.len())
    }

    #[inline]
    fn head_chunks_samples(&self) -> u64 {
        let mut samples = self.active_chunk_num_samples() as u64;
        if let Some(ref c) = self.cold {
            for mc in &c.extra_head_chunks {
                samples += mc.chunk.num_samples() as u64;
            }
        }
        samples
    }

    #[inline]
    fn head_chunks_first_min_time(&self) -> Option<i64> {
        if let Some(c) = self
            .cold
            .as_ref()
            .and_then(|c| c.extra_head_chunks.first())
        {
            Some(c.min_time)
        } else if self.has_active_chunk() {
            Some(self.active_min_time)
        } else {
            None
        }
    }

    #[inline]
    fn for_each_head_chunk_cloned(&self, mut f: impl FnMut(EncodedChunk, i64, i64)) {
        if let Some(ref c) = self.cold {
            for mc in &c.extra_head_chunks {
                f(mc.chunk.clone(), mc.min_time, mc.max_time);
            }
        }
        let enc = self.active_encoding();
        if enc != Encoding::None {
            let chunk = EncodedChunk::from_bstream(enc, self.active_bstream.clone());
            f(chunk, self.active_min_time, self.active_max_time());
        }
    }

    #[inline]
    fn for_each_head_chunk_bounds(&self, mut f: impl FnMut(i64, i64)) {
        if let Some(ref c) = self.cold {
            for mc in &c.extra_head_chunks {
                f(mc.min_time, mc.max_time);
            }
        }
        if self.has_active_chunk() {
            f(self.active_min_time, self.active_max_time());
        }
    }

    #[inline]
    fn any_head_chunk_bounds(&self, mut pred: impl FnMut(i64, i64) -> bool) -> bool {
        if let Some(ref c) = self.cold {
            for mc in &c.extra_head_chunks {
                if pred(mc.min_time, mc.max_time) {
                    return true;
                }
            }
        }
        self.has_active_chunk() && pred(self.active_min_time, self.active_max_time())
    }

    fn push_head_chunk(&mut self, chunk: EncodedChunk, min_time: i64, max_time: i64) {
        if let Some(prev_chunk) = self.take_active_chunk() {
            let prev_min = self.active_min_time;
            let prev_max = self.active_max_time();
            self.cold_mut().extra_head_chunks.push(MemChunk {
                chunk: prev_chunk,
                min_time: prev_min,
                max_time: prev_max,
            });
        }
        self.set_active_chunk(chunk);
        self.active_min_time = min_time;
        self.set_active_max_time(max_time);
    }

    fn clear_head_chunks(&mut self) {
        self.clear_active_chunk();
        self.active_min_time = i64::MAX;
        self.active_max_time_delta = ACTIVE_DELTA_NONE;
        self.next_at_delta = ACTIVE_DELTA_NONE;
        if let Some(ref mut c) = self.cold {
            if c.extra_head_chunks.capacity() > 0 {
                c.extra_head_chunks = Vec::new();
            }
        }
        self.maybe_drop_cold();
    }

    fn retain_head_chunks(&mut self, mint: i64) -> usize {
        let mut removed = 0usize;
        if let Some(ref mut c) = self.cold {
            let before = c.extra_head_chunks.len();
            c.extra_head_chunks.retain(|hc| hc.max_time >= mint);
            removed += before - c.extra_head_chunks.len();
        }
        if self.has_active_chunk() && self.active_max_time() < mint {
            self.clear_active_chunk();
            self.active_min_time = i64::MAX;
            self.active_max_time_delta = ACTIVE_DELTA_NONE;
            self.next_at_delta = ACTIVE_DELTA_NONE;
            removed += 1;
            if let Some(ref mut c) = self.cold {
                if let Some(promoted) = c.extra_head_chunks.pop() {
                    self.set_active_chunk(promoted.chunk);
                    self.active_min_time = promoted.min_time;
                    self.set_active_max_time(promoted.max_time);
                }
            }
        }
        if let Some(ref mut c) = self.cold {
            if c.extra_head_chunks.is_empty() && c.extra_head_chunks.capacity() > 0 {
                c.extra_head_chunks = Vec::new();
            }
        }
        removed
    }

    #[inline]
    fn xor_state(&self) -> XorAppendState {
        let leading_bits = self.flags & XOR_LEADING_MASK;
        let leading = if leading_bits == XOR_LEADING_UNINIT {
            0xff
        } else {
            leading_bits as u8
        };
        let trailing = ((self.flags & XOR_TRAILING_MASK) >> XOR_TRAILING_SHIFT) as u8;
        let valid = (self.flags & FLAG_XOR_VALID) != 0;
        XorAppendState {
            t_delta: self.xor_t_delta,
            leading,
            trailing,
            valid,
        }
    }

    #[inline]
    fn set_xor_state(&mut self, st: XorAppendState) {
        self.xor_t_delta = st.t_delta;
        let l = if st.leading == 0xff {
            XOR_LEADING_UNINIT
        } else {
            u32::from(st.leading) & XOR_LEADING_MASK
        };
        let t = (u32::from(st.trailing) << XOR_TRAILING_SHIFT) & XOR_TRAILING_MASK;
        self.flags = (self.flags & !(XOR_LEADING_MASK | XOR_TRAILING_MASK | FLAG_XOR_VALID))
            | l
            | t
            | if st.valid { FLAG_XOR_VALID } else { 0 };
    }

    #[inline]
    fn invalidate_xor_state(&mut self) {
        self.flags &= !FLAG_XOR_VALID;
    }

    #[inline]
    fn reset_xor2_state(&mut self) {
        if let Some(ref mut c) = self.cold {
            if let Some(ref mut s) = c.xor2_state {
                **s = Xor2AppendState::new_empty();
                return;
            }
        }
        self.cold_mut().xor2_state = Some(Box::new(Xor2AppendState::new_empty()));
    }

    #[inline]
    fn invalidate_xor2_state(&mut self) {
        if let Some(ref mut c) = self.cold {
            if let Some(ref mut s) = c.xor2_state {
                s.valid = false;
            }
        }
    }

    #[inline]
    fn clear_xor2_state(&mut self) {
        if let Some(ref mut c) = self.cold {
            c.xor2_state = None;
        }
    }

    #[inline]
    fn clear_hist_state(&mut self) {
        if let Some(ref mut c) = self.cold {
            c.hist_state = None;
        }
    }

    #[inline]
    fn metadata(&self) -> Option<CompactSeriesMetadata> {
        if (self.flags & FLAG_HAS_METADATA) == 0 {
            None
        } else {
            let mt_raw = ((self.flags & META_TYPE_MASK) >> META_TYPE_SHIFT) as u8;
            Some(CompactSeriesMetadata {
                metric_type: MetricType::from_u8(mt_raw),
                unit: self.meta_unit,
                help: self.meta_help,
            })
        }
    }

    #[inline]
    fn set_metadata(&mut self, m: CompactSeriesMetadata) {
        let mt_bits = (u32::from(m.metric_type as u8) << META_TYPE_SHIFT) & META_TYPE_MASK;
        self.flags = (self.flags & !META_TYPE_MASK) | mt_bits | FLAG_HAS_METADATA;
        self.meta_unit = m.unit;
        self.meta_help = m.help;
    }

    #[inline]
    fn remap_metadata(&mut self, remap: &[SymbolId]) {
        if (self.flags & FLAG_HAS_METADATA) != 0 {
            self.meta_unit = remap[self.meta_unit.0 as usize];
            self.meta_help = remap[self.meta_help.0 as usize];
        }
    }

    #[inline]
    fn last_sample_kind(&self) -> u32 {
        (self.flags & LAST_SAMPLE_KIND_MASK) >> LAST_SAMPLE_KIND_SHIFT
    }

    #[inline]
    fn last_sample_is_none(&self) -> bool {
        self.last_sample_kind() == 0
    }

    #[inline]
    fn last_sample_float(&self) -> Option<f64> {
        (self.last_sample_kind() == 1).then(|| f64::from_bits(self.last_float_bits))
    }

    #[inline]
    fn clear_last_sample(&mut self) {
        if self.last_sample_kind() >= 2 {
            if let Some(ref mut c) = self.cold {
                c.last_hist = LastColdSample::None;
            }
        }
        self.flags &= !LAST_SAMPLE_KIND_MASK;
    }

    #[inline]
    fn set_last_sample_float(&mut self, v: f64) {
        if self.last_sample_kind() >= 2 {
            if let Some(ref mut c) = self.cold {
                c.last_hist = LastColdSample::None;
            }
        }
        self.flags = (self.flags & !LAST_SAMPLE_KIND_MASK) | (1 << LAST_SAMPLE_KIND_SHIFT);
        self.last_float_bits = v.to_bits();
    }

    fn set_last_sample_histogram(&mut self, h: Histogram) {
        self.flags = (self.flags & !LAST_SAMPLE_KIND_MASK) | (2 << LAST_SAMPLE_KIND_SHIFT);
        self.cold_mut().last_hist = LastColdSample::Histogram(Box::new(h));
    }

    fn set_last_sample_float_histogram(&mut self, fh: FloatHistogram) {
        self.flags = (self.flags & !LAST_SAMPLE_KIND_MASK) | (3 << LAST_SAMPLE_KIND_SHIFT);
        self.cold_mut().last_hist = LastColdSample::FloatHistogram(Box::new(fh));
    }

    fn last_sample_clone(&self) -> LastSample {
        match self.last_sample_kind() {
            1 => LastSample::Float(f64::from_bits(self.last_float_bits)),
            2 => match self.cold.as_ref().map(|c| &c.last_hist) {
                Some(LastColdSample::Histogram(h)) => LastSample::Histogram(h.clone()),
                _ => LastSample::None,
            },
            3 => match self.cold.as_ref().map(|c| &c.last_hist) {
                Some(LastColdSample::FloatHistogram(fh)) => LastSample::FloatHistogram(fh.clone()),
                _ => LastSample::None,
            },
            _ => LastSample::None,
        }
    }

    #[inline]
    fn sample_state(&self) -> (bool, bool, usize) {
        match self.last_sample_kind() {
            1 => (is_stale_nan(f64::from_bits(self.last_float_bits)), false, 0),
            2 => match self.cold.as_ref().map(|c| &c.last_hist) {
                Some(LastColdSample::Histogram(h)) => (
                    is_stale_nan(h.sum),
                    true,
                    h.positive_buckets.len() + h.negative_buckets.len(),
                ),
                _ => (false, false, 0),
            },
            3 => match self.cold.as_ref().map(|c| &c.last_hist) {
                Some(LastColdSample::FloatHistogram(fh)) => (
                    is_stale_nan(fh.sum),
                    true,
                    fh.positive_buckets.len() + fh.negative_buckets.len(),
                ),
                _ => (false, false, 0),
            },
            _ => (false, false, 0),
        }
    }

    #[inline]
    fn mmapped(&self) -> &[MmappedChunk] {
        &self.mmapped_chunks
    }

    #[inline]
    fn computed_hist_end_time(&self) -> bool {
        (self.flags & FLAG_COMPUTED_HIST_END_TIME) != 0
    }

    #[inline]
    fn set_computed_hist_end_time(&mut self, val: bool) {
        if val {
            self.flags |= FLAG_COMPUTED_HIST_END_TIME;
        } else {
            self.flags &= !FLAG_COMPUTED_HIST_END_TIME;
        }
    }

    #[inline]
    fn set_gced(&mut self, val: bool) {
        if val {
            self.flags |= FLAG_GCED;
        } else {
            self.flags &= !FLAG_GCED;
        }
    }

    #[inline]
    fn last_exemplar_ts(&self) -> Option<i64> {
        if (self.flags & FLAG_HAS_LAST_EXEMPLAR) != 0 {
            self.cold.as_ref().map(|c| c.last_exemplar_ts)
        } else {
            None
        }
    }

    #[inline]
    fn update_last_exemplar_ts(&mut self, ts: i64) {
        if (self.flags & FLAG_HAS_LAST_EXEMPLAR) != 0 {
            let cold = self.cold_mut();
            cold.last_exemplar_ts = cold.last_exemplar_ts.max(ts);
        } else {
            self.cold_mut().last_exemplar_ts = ts;
            self.flags |= FLAG_HAS_LAST_EXEMPLAR;
        }
    }

    #[inline]
    fn has_any_in_order_chunks(&self) -> bool {
        !self.head_chunks_is_empty() || !self.mmapped().is_empty()
    }

    #[inline]
    fn max_time(&self) -> i64 {
        if self.has_active_chunk() {
            self.active_max_time()
        } else if let Some(c) = self.mmapped().last() {
            c.max_time
        } else {
            i64::MIN
        }
    }

    #[inline]
    fn min_time(&self) -> i64 {
        if let Some(c) = self.mmapped().first() {
            c.min_time
        } else {
            self.head_chunks_first_min_time().unwrap_or(i64::MAX)
        }
    }

    #[inline]
    fn mm_max_time(&self) -> i64 {
        self.mmapped()
            .last()
            .map_or(i64::MIN, |c| c.max_time)
    }

    #[inline]
    fn latest_in_order_encoding(&self) -> Option<Encoding> {
        let enc = self.active_encoding();
        if enc != Encoding::None {
            Some(enc)
        } else {
            self.mmapped().last().map(|c| c.encoding)
        }
    }

    fn last_in_order_sample(&self, chunks_reader: Option<&HeadChunksReader>) -> Option<LastSample> {
        if !self.last_sample_is_none() {
            return Some(self.last_sample_clone());
        }
        let mmc = self.mmapped().last()?;
        let reader = chunks_reader?;
        let chunk = reader.chunk(mmc.ref_id).ok()?;
        let samples = decode_chunk_samples(&chunk).ok()?;
        let last = samples.into_iter().last()?;
        Some(match last.value {
            SampleValue::Float(v) => LastSample::Float(v),
            SampleValue::Histogram(h) => LastSample::Histogram(h),
            SampleValue::FloatHistogram(fh) => LastSample::FloatHistogram(fh),
        })
    }

    #[inline]
    fn needs_chunk_reader_for_t(&self, t: i64) -> bool {
        !self.mmapped().is_empty() && self.last_sample_is_none() && t == self.max_time()
    }

    fn appendable_float(
        &self,
        t: i64,
        v: f64,
        head_maxt: i64,
        min_valid_time: i64,
        ooo_time_window: i64,
        chunks_reader: Option<&HeadChunksReader>,
    ) -> Result<(bool, bool), HeadError> {
        // Returns Ok((is_ooo, is_exact_dup))
        if t >= min_valid_time {
            if !self.has_any_in_order_chunks() {
                return Ok((false, false));
            }
            let maxt = self.max_time();
            if t > maxt {
                return Ok((false, false));
            }
            if t == maxt {
                if let Some(last) = self.last_in_order_sample(chunks_reader) {
                    return match last {
                        LastSample::Histogram(_) | LastSample::FloatHistogram(_) => {
                            Err(HeadError::DuplicateSampleWithDetails(format!(
                                "duplicate sample for timestamp {t}; overrides not allowed: existing is a histogram, new value {}",
                                format_go_g(v)
                            )))
                        }
                        LastSample::Float(prev_v) => {
                            if prev_v.to_bits() != v.to_bits()
                                && v.to_bits() != QUIET_ZERO_NAN_BITS
                            {
                                Err(HeadError::DuplicateSampleWithDetails(format!(
                                    "duplicate sample for timestamp {t}; overrides not allowed: existing {}, new value {}",
                                    format_go_g(prev_v),
                                    format_go_g(v)
                                )))
                            } else {
                                Ok((false, true))
                            }
                        }
                        LastSample::None => Ok((false, false)),
                    };
                }
            }
        }

        if v.to_bits() == QUIET_ZERO_NAN_BITS {
            return Ok((true, false));
        }
        if ooo_time_window > 0 && t >= head_maxt.saturating_sub(ooo_time_window) {
            return Ok((true, false));
        }
        if ooo_time_window > 0 {
            return Err(HeadError::TooOldSample);
        }
        if t < min_valid_time {
            return Err(HeadError::OutOfBounds);
        }
        Err(HeadError::OutOfOrderSample)
    }

    fn appendable_histogram(
        &self,
        t: i64,
        h: &Histogram,
        head_maxt: i64,
        min_valid_time: i64,
        ooo_time_window: i64,
        chunks_reader: Option<&HeadChunksReader>,
    ) -> Result<(bool, bool), HeadError> {
        if t >= min_valid_time {
            if !self.has_any_in_order_chunks() {
                return Ok((false, false));
            }
            let maxt = self.max_time();
            if t > maxt {
                return Ok((false, false));
            }
            if t == maxt {
                if let Some(last) = self.last_in_order_sample(chunks_reader) {
                    return match last {
                        LastSample::Histogram(prev_h) if prev_h.prom_equals(h) => Ok((false, true)),
                        _ => Err(HeadError::DuplicateSampleForTimestamp),
                    };
                }
            }
        }
        if ooo_time_window > 0 && t >= head_maxt.saturating_sub(ooo_time_window) {
            return Ok((true, false));
        }
        if ooo_time_window > 0 {
            return Err(HeadError::TooOldSample);
        }
        if t < min_valid_time {
            return Err(HeadError::OutOfBounds);
        }
        Err(HeadError::OutOfOrderSample)
    }

    fn appendable_float_histogram(
        &self,
        t: i64,
        fh: &FloatHistogram,
        head_maxt: i64,
        min_valid_time: i64,
        ooo_time_window: i64,
        chunks_reader: Option<&HeadChunksReader>,
    ) -> Result<(bool, bool), HeadError> {
        if t >= min_valid_time {
            if !self.has_any_in_order_chunks() {
                return Ok((false, false));
            }
            let maxt = self.max_time();
            if t > maxt {
                return Ok((false, false));
            }
            if t == maxt {
                if let Some(last) = self.last_in_order_sample(chunks_reader) {
                    return match last {
                        LastSample::FloatHistogram(prev_fh) if prev_fh.prom_equals(fh) => {
                            Ok((false, true))
                        }
                        _ => Err(HeadError::DuplicateSampleForTimestamp),
                    };
                }
            }
        }
        if ooo_time_window > 0 && t >= head_maxt.saturating_sub(ooo_time_window) {
            return Ok((true, false));
        }
        if ooo_time_window > 0 {
            return Err(HeadError::TooOldSample);
        }
        if t < min_valid_time {
            return Err(HeadError::OutOfBounds);
        }
        Err(HeadError::OutOfOrderSample)
    }

    fn cut_new_head_chunk(&mut self, mint: i64, enc: Encoding, chunk_range_ms: i64) {
        let chunk = match enc {
            Encoding::Xor2 => {
                self.reset_xor2_state();
                EncodedChunk::Xor2(Xor2Chunk::new())
            }
            Encoding::Histogram => {
                self.clear_hist_state();
                EncodedChunk::Histogram(HistogramChunk::new())
            }
            Encoding::HistogramSt => {
                self.clear_hist_state();
                EncodedChunk::HistogramSt(HistogramStChunk::new())
            }
            Encoding::FloatHistogram => {
                self.clear_hist_state();
                EncodedChunk::FloatHistogram(FloatHistogramChunk::new())
            }
            Encoding::FloatHistogramSt => {
                self.clear_hist_state();
                EncodedChunk::FloatHistogramSt(FloatHistogramStChunk::new())
            }
            _ => {
                self.set_xor_state(XorAppendState::new_empty());
                EncodedChunk::Xor(XorChunk::new())
            }
        };
        self.push_head_chunk(chunk, mint, i64::MIN);
        self.set_next_at(range_for_timestamp(mint, chunk_range_ms));
        self.set_computed_hist_end_time(false);
    }

    #[allow(clippy::too_many_arguments)]
    fn append_float_in_order(
        &mut self,
        series_labels: &Labels,
        st: i64,
        t: i64,
        v: f64,
        chunk_range_ms: i64,
        samples_per_chunk: usize,
        float_enc: Encoding,
        store_st: bool,
        chunk_end_time_variance: f64,
    ) -> Result<(bool, bool), ChunkError> {
        let target_enc = if float_enc == Encoding::Xor2 {
            Encoding::Xor2
        } else {
            Encoding::Xor
        };
        // Returns (appended, chunk_created)
        let mut chunk_created = false;
        if !self.has_active_chunk() {
            if let Some(last_mm) = self.mmapped().last() {
                if last_mm.max_time >= t {
                    return Ok((false, false));
                }
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let pre_cut = {
            let cur_max = self.active_max_time();
            if cur_max >= t {
                return Ok((false, chunk_created));
            }
            let cur_enc = self.active_encoding();
            (!chunk_created
                && self.active_bstream.bytes().len() > MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND)
                || (cur_enc != target_enc
                    && (!cur_enc.compatible_values(target_enc) || store_st))
        };

        if pre_cut {
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let num_samples = self.active_chunk_num_samples();
        if num_samples == 0 {
            self.active_min_time = t;
            self.set_next_at(range_for_timestamp(t, chunk_range_ms));
        } else if num_samples == samples_per_chunk / 4 {
            let max_next_at = self.next_at();
            let computed = compute_chunk_end_time(
                self.active_min_time,
                self.active_max_time(),
                max_next_at,
                4.0,
            );
            let next_at = if chunk_end_time_variance > 0.0 {
                add_jitter_to_chunk_end_time(
                    series_labels.stable_hash(),
                    self.active_min_time,
                    computed,
                    max_next_at,
                    chunk_end_time_variance,
                )
            } else {
                computed
            };
            self.set_next_at(next_at);
        }

        if num_samples > 0 && (t >= self.next_at() || num_samples >= samples_per_chunk * 2) {
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let prev_v = if let Some(pv) = self.last_sample_float() {
            pv
        } else {
            self.invalidate_xor_state();
            0.0
        };
        let cur_max_time = self.active_max_time();

        let mut xor_st = self.xor_state();
        match self.active_encoding() {
            Encoding::Xor => {
                let mut xc = XorChunk {
                    b: std::mem::take(&mut self.active_bstream),
                };
                let res = xc.append_with_state(&mut xor_st, cur_max_time, prev_v, t, v);
                self.active_bstream = xc.b;
                res?;
                self.set_xor_state(xor_st);
            }
            Encoding::Xor2 => {
                let mut xc2 = Xor2Chunk {
                    b: std::mem::take(&mut self.active_bstream),
                };
                let res = {
                    let s = self
                        .cold_mut()
                        .xor2_state
                        .get_or_insert_with(|| Box::new(Xor2AppendState::default()));
                    xc2.append_with_state(s, st, t, v)
                };
                self.active_bstream = xc2.b;
                res?;
            }
            _ => unreachable!(),
        }
        self.set_active_max_time(t);
        self.set_last_sample_float(v);
        self.record_created_timestamp(st);
        Ok((true, chunk_created))
    }

    fn prev_int_histogram_state(&mut self) -> Result<Option<HistogramAppenderState>, ChunkError> {
        if self.active_chunk_num_samples() == 0 {
            return Ok(None);
        }
        match self.active_encoding() {
            Encoding::Histogram => {
                if let Some(HistAppendCache::Histogram(Some(ref st))) =
                    self.cold.as_ref().and_then(|c| c.hist_state.as_deref())
                {
                    return Ok(Some(st.clone()));
                }
                let mut hc = HistogramChunk {
                    b: std::mem::take(&mut self.active_bstream),
                };
                let res = hc.appender().map(|a| Some(a.into_state()));
                self.active_bstream = hc.b;
                res
            }
            Encoding::HistogramSt => {
                if let Some(HistAppendCache::HistogramSt(Some((ref st, _)))) =
                    self.cold.as_ref().and_then(|c| c.hist_state.as_deref())
                {
                    return Ok(Some(st.clone()));
                }
                let mut hc = HistogramStChunk {
                    b: std::mem::take(&mut self.active_bstream),
                };
                let res = hc.appender().map(|a| Some(a.into_state()));
                self.active_bstream = hc.b;
                res
            }
            _ => Ok(None),
        }
    }

    fn prev_float_histogram_state(
        &mut self,
    ) -> Result<Option<FloatHistogramAppenderState>, ChunkError> {
        if self.active_chunk_num_samples() == 0 {
            return Ok(None);
        }
        match self.active_encoding() {
            Encoding::FloatHistogram => {
                if let Some(HistAppendCache::FloatHistogram(Some(ref st))) =
                    self.cold.as_ref().and_then(|c| c.hist_state.as_deref())
                {
                    return Ok(Some(st.clone()));
                }
                let mut fhc = FloatHistogramChunk {
                    b: std::mem::take(&mut self.active_bstream),
                };
                let res = fhc.appender().map(|a| Some(a.into_state()));
                self.active_bstream = fhc.b;
                res
            }
            Encoding::FloatHistogramSt => {
                if let Some(HistAppendCache::FloatHistogramSt(Some((ref st, _)))) =
                    self.cold.as_ref().and_then(|c| c.hist_state.as_deref())
                {
                    return Ok(Some(st.clone()));
                }
                let mut fhc = FloatHistogramStChunk {
                    b: std::mem::take(&mut self.active_bstream),
                };
                let res = fhc.appender().map(|a| Some(a.into_state()));
                self.active_bstream = fhc.b;
                res
            }
            _ => Ok(None),
        }
    }

    fn append_histogram_in_order(
        &mut self,
        st: i64,
        t: i64,
        h: &Histogram,
        chunk_range_ms: i64,
        enable_hist_st: bool,
    ) -> Result<(bool, bool), ChunkError> {
        let target_enc = if enable_hist_st {
            Encoding::HistogramSt
        } else {
            Encoding::Histogram
        };
        let mut chunk_created = false;
        if !self.has_active_chunk() {
            if let Some(last_mm) = self.mmapped().last() {
                if last_mm.max_time >= t {
                    return Ok((false, false));
                }
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        if self.active_max_time() >= t {
            return Ok((false, chunk_created));
        }

        let mut prev_state: Option<HistogramAppenderState> = None;
        let cur_enc = self.active_encoding();
        if cur_enc != target_enc {
            prev_state = self.prev_int_histogram_state()?;
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let (num_samples, num_bytes, c_min, c_max) = {
            let ns = self.active_chunk_num_samples();
            if ns == 0 {
                self.active_min_time = t;
                self.set_next_at(range_for_timestamp(t, chunk_range_ms));
            }
            (
                ns,
                self.active_bstream.bytes().len(),
                self.active_min_time,
                self.active_max_time(),
            )
        };

        let next_chunk_range_start = if self.computed_hist_end_time() {
            range_for_timestamp(c_min, chunk_range_ms)
        } else {
            self.next_at()
        };

        if !self.computed_hist_end_time() && num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK / 4 {
            let ratio = (TARGET_BYTES_PER_HISTOGRAM_CHUNK as f64) / (num_bytes as f64);
            let computed = compute_chunk_end_time(c_min, c_max, self.next_at(), ratio);
            self.set_next_at(computed);
            self.set_computed_hist_end_time(true);
        }

        if (t >= self.next_at() || num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2)
            && (num_samples >= MIN_SAMPLES_PER_HISTOGRAM_CHUNK || t >= next_chunk_range_start)
        {
            if prev_state.is_none() {
                prev_state = self.prev_int_histogram_state()?;
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let prev_ref = if chunk_created {
            prev_state.as_ref()
        } else {
            None
        };
        let active_enc = self.active_encoding();
        let active_bs = std::mem::take(&mut self.active_bstream);
        let (returned_bs, outcome) = match active_enc {
            Encoding::Histogram => {
                let mut hc = HistogramChunk { b: active_bs };
                let cache = self
                    .cold_mut()
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::Histogram(None)));
                if !matches!(**cache, HistAppendCache::Histogram(_)) {
                    **cache = HistAppendCache::Histogram(None);
                }
                let HistAppendCache::Histogram(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                let outcome = match hc.append_with_state_prev(st_opt, prev_ref, st, t, h) {
                    Ok(AppendHistogramOutcome::Appended { .. }) => Ok(None),
                    Ok(AppendHistogramOutcome::NewChunk(new_chk)) => {
                        Ok(Some(EncodedChunk::Histogram(new_chk)))
                    }
                    Err(e) => Err(e),
                };
                (hc.b, outcome)
            }
            Encoding::HistogramSt => {
                let mut hc = HistogramStChunk { b: active_bs };
                let cache = self
                    .cold_mut()
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::HistogramSt(None)));
                if !matches!(**cache, HistAppendCache::HistogramSt(_)) {
                    **cache = HistAppendCache::HistogramSt(None);
                }
                let HistAppendCache::HistogramSt(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                let outcome = match hc.append_with_state_prev(st_opt, prev_ref, st, t, h) {
                    Ok(AppendHistogramStOutcome::Appended { .. }) => Ok(None),
                    Ok(AppendHistogramStOutcome::NewChunk(new_chk)) => {
                        Ok(Some(EncodedChunk::HistogramSt(new_chk)))
                    }
                    Err(e) => Err(e),
                };
                (hc.b, outcome)
            }
            _ => unreachable!(),
        };
        self.active_bstream = returned_bs;
        match outcome? {
            None => self.set_active_max_time(t),
            Some(new_chk) => {
                self.push_head_chunk(new_chk, t, t);
                self.set_next_at(range_for_timestamp(t, chunk_range_ms));
                self.set_computed_hist_end_time(false);
                chunk_created = true;
            }
        }
        self.set_last_sample_histogram(h.clone());
        self.record_created_timestamp(st);
        Ok((true, chunk_created))
    }

    fn append_float_histogram_in_order(
        &mut self,
        st: i64,
        t: i64,
        fh: &FloatHistogram,
        chunk_range_ms: i64,
        enable_hist_st: bool,
    ) -> Result<(bool, bool), ChunkError> {
        let target_enc = if enable_hist_st {
            Encoding::FloatHistogramSt
        } else {
            Encoding::FloatHistogram
        };
        let mut chunk_created = false;
        if !self.has_active_chunk() {
            if let Some(last_mm) = self.mmapped().last() {
                if last_mm.max_time >= t {
                    return Ok((false, false));
                }
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        if self.active_max_time() >= t {
            return Ok((false, chunk_created));
        }

        let mut prev_state: Option<FloatHistogramAppenderState> = None;
        let cur_enc = self.active_encoding();
        if cur_enc != target_enc {
            prev_state = self.prev_float_histogram_state()?;
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let (num_samples, num_bytes, c_min, c_max) = {
            let ns = self.active_chunk_num_samples();
            if ns == 0 {
                self.active_min_time = t;
                self.set_next_at(range_for_timestamp(t, chunk_range_ms));
            }
            (
                ns,
                self.active_bstream.bytes().len(),
                self.active_min_time,
                self.active_max_time(),
            )
        };

        let next_chunk_range_start = if self.computed_hist_end_time() {
            range_for_timestamp(c_min, chunk_range_ms)
        } else {
            self.next_at()
        };

        if !self.computed_hist_end_time() && num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK / 4 {
            let ratio = (TARGET_BYTES_PER_HISTOGRAM_CHUNK as f64) / (num_bytes as f64);
            let computed = compute_chunk_end_time(c_min, c_max, self.next_at(), ratio);
            self.set_next_at(computed);
            self.set_computed_hist_end_time(true);
        }

        if (t >= self.next_at() || num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2)
            && (num_samples >= MIN_SAMPLES_PER_HISTOGRAM_CHUNK || t >= next_chunk_range_start)
        {
            if prev_state.is_none() {
                prev_state = self.prev_float_histogram_state()?;
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let prev_ref = if chunk_created {
            prev_state.as_ref()
        } else {
            None
        };
        let active_enc = self.active_encoding();
        let active_bs = std::mem::take(&mut self.active_bstream);
        let (returned_bs, outcome) = match active_enc {
            Encoding::FloatHistogram => {
                let mut fhc = FloatHistogramChunk { b: active_bs };
                let cache = self
                    .cold_mut()
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::FloatHistogram(None)));
                if !matches!(**cache, HistAppendCache::FloatHistogram(_)) {
                    **cache = HistAppendCache::FloatHistogram(None);
                }
                let HistAppendCache::FloatHistogram(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                let outcome = match fhc.append_with_state_prev(st_opt, prev_ref, st, t, fh) {
                    Ok(AppendFloatHistogramOutcome::Appended { .. }) => Ok(None),
                    Ok(AppendFloatHistogramOutcome::NewChunk(new_chk)) => {
                        Ok(Some(EncodedChunk::FloatHistogram(new_chk)))
                    }
                    Err(e) => Err(e),
                };
                (fhc.b, outcome)
            }
            Encoding::FloatHistogramSt => {
                let mut fhc = FloatHistogramStChunk { b: active_bs };
                let cache = self
                    .cold_mut()
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::FloatHistogramSt(None)));
                if !matches!(**cache, HistAppendCache::FloatHistogramSt(_)) {
                    **cache = HistAppendCache::FloatHistogramSt(None);
                }
                let HistAppendCache::FloatHistogramSt(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                let outcome = match fhc.append_with_state_prev(st_opt, prev_ref, st, t, fh) {
                    Ok(AppendFloatHistogramStOutcome::Appended { .. }) => Ok(None),
                    Ok(AppendFloatHistogramStOutcome::NewChunk(new_chk)) => {
                        Ok(Some(EncodedChunk::FloatHistogramSt(new_chk)))
                    }
                    Err(e) => Err(e),
                };
                (fhc.b, outcome)
            }
            _ => unreachable!(),
        };
        self.active_bstream = returned_bs;
        match outcome? {
            None => self.set_active_max_time(t),
            Some(new_chk) => {
                self.push_head_chunk(new_chk, t, t);
                self.set_next_at(range_for_timestamp(t, chunk_range_ms));
                self.set_computed_hist_end_time(false);
                chunk_created = true;
            }
        }
        self.set_last_sample_float_histogram(fh.clone());
        self.record_created_timestamp(st);
        Ok((true, chunk_created))
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_ooo(
        &mut self,
        ref_id: u64,
        st: i64,
        t: i64,
        val: OooSampleValue,
        ooo_cap_max: usize,
        use_xor2: bool,
        use_histogram_st: bool,
        chunk_writer: Option<&mut HeadChunksWriter>,
    ) -> Result<(bool, bool, Vec<ChunkDiskMapperRef>, Option<u64>), HeadChunksError> {
        let ooo = self.ooo_or_insert();
        let mut chunk_created = false;
        let mut mmap_refs = Vec::new();
        let mut write_epoch = None;

        let need_cut = ooo
            .ooo_head_chunk
            .as_ref()
            .is_none_or(|c| c.chunk.num_samples() >= ooo_cap_max);

        if need_cut {
            if let Some(ref old) = ooo.ooo_head_chunk {
                if let Some(writer) = chunk_writer {
                    let cut_chunks = old.chunk.to_encoded_chunks(
                        i64::MIN,
                        i64::MAX,
                        use_xor2,
                        use_histogram_st,
                    )?;
                    let snap = writer.take_snapshot();
                    let mut staged_refs: SmallVec<[ChunkDiskMapperRef; 4]> = SmallVec::new();
                    let mut staged_pending: SmallVec<[PendingOooMmap; 4]> = SmallVec::new();
                    for cc in cut_chunks {
                        let r = match writer.write_chunk(
                            ref_id,
                            cc.min_time,
                            cc.max_time,
                            &cc.chunk,
                            true,
                        ) {
                            Ok(r) => r,
                            Err(e) => {
                                writer.rollback_snapshot(snap);
                                return Err(e);
                            }
                        };
                        staged_refs.push(r);
                        let mc = MmappedChunk {
                            ref_id: r,
                            min_time: cc.min_time,
                            max_time: cc.max_time,
                            num_samples: cc.chunk.num_samples() as u16,
                            encoding: cc.chunk.encoding(),
                        };
                        staged_pending.push(PendingOooMmap {
                            mc,
                            write_epoch: None,
                            chunk: cc.chunk,
                        });
                    }
                    let ep = writer.discard_epoch();
                    for p in &mut staged_pending {
                        p.write_epoch = Some(ep);
                    }
                    write_epoch = Some(ep);
                    ooo.ooo_pending_mmapped.extend(staged_pending);
                    mmap_refs.extend(staged_refs);
                }
            }
            ooo.ooo_head_chunk = Some(OooHeadChunk {
                chunk: OooChunk::new(),
                min_time: t,
                max_time: i64::MIN,
            });
            chunk_created = true;
        }

        let cur = ooo.ooo_head_chunk.as_mut().unwrap();
        let inserted = cur.chunk.insert_capped(st, t, val, ooo_cap_max);
        if inserted {
            if chunk_created || t < cur.min_time {
                cur.min_time = t;
            }
            if chunk_created || t > cur.max_time {
                cur.max_time = t;
            }
        }
        Ok((inserted, chunk_created, mmap_refs, write_epoch))
    }

    fn write_sealed_head_chunks(
        &mut self,
        ref_id: u64,
        writer: &mut HeadChunksWriter,
    ) -> Result<Vec<MmappedChunk>, HeadChunksError> {
        let Some(ref mut cold) = self.cold else {
            return Ok(Vec::new());
        };
        if cold.extra_head_chunks.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(cold.extra_head_chunks.len());
        for mc in &mut cold.extra_head_chunks {
            match mc.chunk {
                EncodedChunk::Xor(ref mut c) => c.compact(),
                EncodedChunk::Xor2(ref mut c) => c.compact(),
                EncodedChunk::Histogram(ref mut c) => c.compact(),
                EncodedChunk::HistogramSt(ref mut c) => c.compact(),
                EncodedChunk::FloatHistogram(ref mut c) => c.compact(),
                EncodedChunk::FloatHistogramSt(ref mut c) => c.compact(),
            }
            let r = writer.write_chunk(ref_id, mc.min_time, mc.max_time, &mc.chunk, false)?;
            out.push(MmappedChunk {
                ref_id: r,
                min_time: mc.min_time,
                max_time: mc.max_time,
                num_samples: mc.chunk.num_samples() as u16,
                encoding: mc.chunk.encoding(),
            });
        }
        Ok(out)
    }

    fn promote_sealed_head_chunks(&mut self, mmapped: Vec<MmappedChunk>) {
        let mut matched = 0usize;
        for mc in mmapped {
            let found = if let Some(ref mut cold) = self.cold {
                if !cold.extra_head_chunks.is_empty()
                    && cold.extra_head_chunks[0].min_time == mc.min_time
                    && cold.extra_head_chunks[0].max_time == mc.max_time
                {
                    cold.extra_head_chunks.remove(0);
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if found {
                push_mmapped_chunk(&mut self.mmapped_chunks, mc);
                matched += 1;
            }
        }
        if let Some(ref mut cold) = self.cold {
            if cold.extra_head_chunks.is_empty() && cold.extra_head_chunks.capacity() > 0 {
                cold.extra_head_chunks = Vec::new();
            }
        }
        self.maybe_drop_cold();
        if matched > 0 && self.mmapped_chunks.len() > 1 {
            self.mmapped_chunks.sort_by_key(|c| (c.min_time, c.max_time));
        }
    }
}

const BY_ID_PAGE_SHIFT: usize = 12;
const BY_ID_PAGE_SIZE: usize = 1 << BY_ID_PAGE_SHIFT;
const BY_ID_PAGE_MASK: usize = BY_ID_PAGE_SIZE - 1;
const BY_ID_DIR_SHIFT: usize = 8;
const BY_ID_DIR_SIZE: usize = 1 << BY_ID_DIR_SHIFT;
const BY_ID_DIR_MASK: usize = BY_ID_DIR_SIZE - 1;
const BY_ID_TOP_SIZE: usize = 256;
const MAX_PAGED_SERIES_ID: u64 = (BY_ID_TOP_SIZE * BY_ID_DIR_SIZE * BY_ID_PAGE_SIZE) as u64;

#[repr(transparent)]
struct ByIdPage {
    slots: [UnsafeCell<Option<Arc<SeriesEntry>>>; BY_ID_PAGE_SIZE],
}

impl ByIdPage {
    fn new_boxed() -> Box<Self> {
        let mut v: Vec<UnsafeCell<Option<Arc<SeriesEntry>>>> = Vec::with_capacity(BY_ID_PAGE_SIZE);
        v.resize_with(BY_ID_PAGE_SIZE, || UnsafeCell::new(None));
        let raw = Box::into_raw(v.into_boxed_slice()) as *mut Self;
        // SAFETY: `[T; N]` and `#[repr(transparent)] ByIdPage([T; N])` have identical layout.
        unsafe { Box::from_raw(raw) }
    }
}

struct ByIdPageDir {
    pages: [AtomicPtr<ByIdPage>; BY_ID_DIR_SIZE],
    live_counts: [AtomicU16; BY_ID_DIR_SIZE],
}

impl ByIdPageDir {
    fn new_boxed() -> Box<Self> {
        Box::new(Self {
            pages: [const { AtomicPtr::new(std::ptr::null_mut()) }; BY_ID_DIR_SIZE],
            live_counts: [const { AtomicU16::new(0) }; BY_ID_DIR_SIZE],
        })
    }
}

/// 2-level paged `by_id` table for O(1) direct indexing on `ref_id < MAX_PAGED_SERIES_ID` (268M).
///
/// Every slot `ref_id` is guarded by `StripeSeries::stripes[stripe_idx(ref_id)]` `RwLock`:
/// - Readers (`get_by_id`, `active_cache_for_id`) hold `stripes[idx].read()`.
/// - Writers (`insert`, `remove`, `remove_if_gced_under_entry_lock`) hold `stripes[idx].write()`.
/// - Page reclamation (`shrink_empty_pages_under_all_stripe_locks`) holds all stripe write locks.
struct PagedById {
    dirs: [AtomicPtr<ByIdPageDir>; BY_ID_TOP_SIZE],
    alloc_mtx: Mutex<()>,
}

// SAFETY: Slot access is synchronized by `StripeSeries::stripes` `RwLock`s, and directory/page
// pointer publication uses `Acquire`/`Release` atomic operations.
unsafe impl Send for PagedById {}
unsafe impl Sync for PagedById {}

impl Drop for PagedById {
    fn drop(&mut self) {
        for dir_atomic in &mut self.dirs {
            let dir_ptr = *dir_atomic.get_mut();
            if !dir_ptr.is_null() {
                // SAFETY: `&mut self` grants exclusive ownership during drop.
                let mut dir = unsafe { Box::from_raw(dir_ptr) };
                for page_atomic in &mut dir.pages {
                    let page_ptr = *page_atomic.get_mut();
                    if !page_ptr.is_null() {
                        unsafe { drop(Box::from_raw(page_ptr)) };
                    }
                }
            }
        }
    }
}

impl PagedById {
    fn new() -> Self {
        Self {
            dirs: [const { AtomicPtr::new(std::ptr::null_mut()) }; BY_ID_TOP_SIZE],
            alloc_mtx: Mutex::new(()),
        }
    }

    #[inline(always)]
    fn split_id(ref_id: u64) -> (usize, usize, usize) {
        let top = (ref_id >> (BY_ID_PAGE_SHIFT + BY_ID_DIR_SHIFT)) as usize;
        let dir = ((ref_id >> BY_ID_PAGE_SHIFT) as usize) & BY_ID_DIR_MASK;
        let offset = (ref_id as usize) & BY_ID_PAGE_MASK;
        (top, dir, offset)
    }

    #[inline]
    unsafe fn get_under_stripe_lock(&self, ref_id: u64) -> Option<Arc<SeriesEntry>> {
        let (top, dir, offset) = Self::split_id(ref_id);
        let dir_ptr = self.dirs[top].load(Ordering::Acquire);
        if dir_ptr.is_null() {
            return None;
        }
        let page_ptr = unsafe { (*dir_ptr).pages[dir].load(Ordering::Acquire) };
        if page_ptr.is_null() {
            return None;
        }
        unsafe { (*(*page_ptr).slots[offset].get()).clone() }
    }

    #[inline]
    unsafe fn active_cache_under_stripe_lock(&self, ref_id: u64) -> u64 {
        let (top, dir, offset) = Self::split_id(ref_id);
        let dir_ptr = self.dirs[top].load(Ordering::Acquire);
        if dir_ptr.is_null() {
            return 0;
        }
        let page_ptr = unsafe { (*dir_ptr).pages[dir].load(Ordering::Acquire) };
        if page_ptr.is_null() {
            return 0;
        }
        unsafe {
            (*(*page_ptr).slots[offset].get())
                .as_ref()
                .map_or(0, |e| e.active_cache())
        }
    }

    unsafe fn insert_under_stripe_lock(&self, ref_id: u64, entry: Arc<SeriesEntry>) {
        let (top, dir, offset) = Self::split_id(ref_id);
        let mut dir_ptr = self.dirs[top].load(Ordering::Acquire);
        if dir_ptr.is_null() {
            let _g = self.alloc_mtx.lock();
            dir_ptr = self.dirs[top].load(Ordering::Acquire);
            if dir_ptr.is_null() {
                dir_ptr = Box::into_raw(ByIdPageDir::new_boxed());
                self.dirs[top].store(dir_ptr, Ordering::Release);
            }
        }
        let mut page_ptr = unsafe { (*dir_ptr).pages[dir].load(Ordering::Acquire) };
        if page_ptr.is_null() {
            let _g = self.alloc_mtx.lock();
            page_ptr = unsafe { (*dir_ptr).pages[dir].load(Ordering::Acquire) };
            if page_ptr.is_null() {
                page_ptr = Box::into_raw(ByIdPage::new_boxed());
                unsafe { (*dir_ptr).pages[dir].store(page_ptr, Ordering::Release) };
            }
        }
        let slot = unsafe { &mut *(*page_ptr).slots[offset].get() };
        let was_empty = slot.is_none();
        *slot = Some(entry);
        if was_empty {
            unsafe { (*dir_ptr).live_counts[dir].fetch_add(1, Ordering::Relaxed) };
        }
    }

    unsafe fn remove_under_stripe_lock(&self, ref_id: u64) {
        let (top, dir, offset) = Self::split_id(ref_id);
        let dir_ptr = self.dirs[top].load(Ordering::Acquire);
        if dir_ptr.is_null() {
            return;
        }
        let page_ptr = unsafe { (*dir_ptr).pages[dir].load(Ordering::Acquire) };
        if page_ptr.is_null() {
            return;
        }
        let slot = unsafe { &mut *(*page_ptr).slots[offset].get() };
        if slot.take().is_some() {
            unsafe { (*dir_ptr).live_counts[dir].fetch_sub(1, Ordering::Release) };
        }
    }

    /// Caller must hold `StripeSeries::shrink_mtx` so directories are not freed concurrently.
    unsafe fn has_empty_page_under_shrink_lock(&self) -> bool {
        for dir_atomic in &self.dirs {
            let dir_ptr = dir_atomic.load(Ordering::Acquire);
            if dir_ptr.is_null() {
                continue;
            }
            for i in 0..BY_ID_DIR_SIZE {
                let page_ptr = unsafe { (*dir_ptr).pages[i].load(Ordering::Acquire) };
                if !page_ptr.is_null()
                    && unsafe { (*dir_ptr).live_counts[i].load(Ordering::Acquire) } == 0
                {
                    return true;
                }
            }
        }
        false
    }

    unsafe fn shrink_empty_pages_under_all_stripe_locks(&self) {
        for dir_atomic in &self.dirs {
            let dir_ptr = dir_atomic.load(Ordering::Acquire);
            if dir_ptr.is_null() {
                continue;
            }
            let mut any_page_left = false;
            for i in 0..BY_ID_DIR_SIZE {
                let page_ptr = unsafe { (*dir_ptr).pages[i].load(Ordering::Acquire) };
                if page_ptr.is_null() {
                    continue;
                }
                if unsafe { (*dir_ptr).live_counts[i].load(Ordering::Acquire) } == 0 {
                    unsafe {
                        (*dir_ptr).pages[i].store(std::ptr::null_mut(), Ordering::Release);
                        drop(Box::from_raw(page_ptr));
                    }
                } else {
                    any_page_left = true;
                }
            }
            if !any_page_left {
                dir_atomic.store(std::ptr::null_mut(), Ordering::Release);
                unsafe { drop(Box::from_raw(dir_ptr)) };
            }
        }
    }

    #[cfg(test)]
    unsafe fn allocated_page_count_under_shrink_lock(&self) -> usize {
        let mut count = 0usize;
        for dir_atomic in &self.dirs {
            let dir_ptr = dir_atomic.load(Ordering::Acquire);
            if dir_ptr.is_null() {
                continue;
            }
            for i in 0..BY_ID_DIR_SIZE {
                if !unsafe { (*dir_ptr).pages[i].load(Ordering::Acquire) }.is_null() {
                    count += 1;
                }
            }
        }
        count
    }
}

struct SeriesStripe {
    by_hash: HashTable<(u64, Arc<SeriesEntry>)>,
    #[allow(clippy::box_collection)]
    by_id_overflow: Option<Box<FastU64Map<Arc<SeriesEntry>>>>,
}

impl SeriesStripe {
    fn new() -> Self {
        Self {
            by_hash: HashTable::new(),
            by_id_overflow: None,
        }
    }

    #[inline]
    fn insert_overflow(&mut self, ref_id: u64, entry: Arc<SeriesEntry>) {
        self.by_id_overflow
            .get_or_insert_with(Default::default)
            .insert(ref_id, entry);
    }

    #[inline]
    fn remove_overflow(&mut self, ref_id: u64) {
        if let Some(ref mut map) = self.by_id_overflow {
            map.remove(&ref_id);
        }
    }

    #[inline]
    fn get_overflow(&self, ref_id: u64) -> Option<&Arc<SeriesEntry>> {
        self.by_id_overflow.as_ref().and_then(|m| m.get(&ref_id))
    }
}

/// Synchronizes WAL-backed series eviction tombstones with concurrent `get_or_create`
/// (matching Prometheus `pendingTombstones` in `head.go:219-275`).
struct PendingTombstones {
    active: AtomicBool,
    inner: Mutex<PendingTombstonesInner>,
    condvar: Condvar,
}

#[derive(Default)]
struct PendingTombstonesInner {
    reserved: bool,
    hashes: HashSet<u64>,
}

impl PendingTombstones {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            inner: Mutex::new(PendingTombstonesInner::default()),
            condvar: Condvar::new(),
        }
    }

    fn reserve_guard(&self, cap: usize) -> PendingTombstonesGuard<'_> {
        let mut g = self.inner.lock();
        while g.reserved {
            self.condvar.wait(&mut g);
        }
        g.reserved = true;
        g.hashes.reserve(cap);
        PendingTombstonesGuard { pt: self }
    }

    fn add(&self, hash: u64) {
        let mut g = self.inner.lock();
        if g.reserved {
            g.hashes.insert(hash);
            self.active.store(true, Ordering::Release);
        }
    }

    #[inline]
    fn is_pending(&self, hash: u64) -> bool {
        if !self.active.load(Ordering::Acquire) {
            return false;
        }
        let g = self.inner.lock();
        g.hashes.contains(&hash)
    }

    fn wait_while_pending(&self, hash: u64) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let mut g = self.inner.lock();
        while g.hashes.contains(&hash) {
            self.condvar.wait(&mut g);
        }
    }

    fn release_all(&self) {
        let mut g = self.inner.lock();
        g.reserved = false;
        g.hashes.clear();
        self.active.store(false, Ordering::Release);
        self.condvar.notify_all();
    }
}

struct PendingTombstonesGuard<'a> {
    pt: &'a PendingTombstones,
}

impl Drop for PendingTombstonesGuard<'_> {
    fn drop(&mut self) {
        self.pt.release_all();
    }
}

/// Power-of-two striped series map matching `tsdb.stripeSeries`.
struct StripeSeries {
    mask: usize,
    stripes: Vec<RwLock<SeriesStripe>>,
    by_id_pages: PagedById,
    shrink_mtx: Mutex<()>,
    pending_tombstones: PendingTombstones,
}

impl StripeSeries {
    fn new(stripe_size: usize) -> Self {
        let size = stripe_size.max(1).next_power_of_two();
        let mut stripes = Vec::with_capacity(size);
        for _ in 0..size {
            stripes.push(RwLock::new(SeriesStripe::new()));
        }
        Self {
            mask: size - 1,
            stripes,
            by_id_pages: PagedById::new(),
            shrink_mtx: Mutex::new(()),
            pending_tombstones: PendingTombstones::new(),
        }
    }

    #[inline]
    fn stripe_idx(&self, key: u64) -> usize {
        (key as usize) & self.mask
    }

    #[inline]
    fn get_by_id(&self, ref_id: u64) -> Option<Arc<SeriesEntry>> {
        let idx = self.stripe_idx(ref_id);
        let stripe = self.stripes[idx].read();
        if ref_id < MAX_PAGED_SERIES_ID {
            // SAFETY: `stripe` read guard prevents concurrent slot mutation and page reclamation.
            return unsafe { self.by_id_pages.get_under_stripe_lock(ref_id) };
        }
        stripe.get_overflow(ref_id).cloned()
    }

    #[inline]
    fn active_cache_for_id(&self, ref_id: u64) -> u64 {
        let idx = self.stripe_idx(ref_id);
        let stripe = self.stripes[idx].read();
        if ref_id < MAX_PAGED_SERIES_ID {
            // SAFETY: `stripe` read guard prevents concurrent slot mutation and page reclamation.
            return unsafe { self.by_id_pages.active_cache_under_stripe_lock(ref_id) };
        }
        stripe.get_overflow(ref_id).map_or(0, |e| e.active_cache())
    }

    fn get_by_sorted_pairs(&self, hash: u64, pairs: &[(&str, &str)]) -> Option<Arc<SeriesEntry>> {
        let idx = self.stripe_idx(hash);
        let stripe = self.stripes[idx].read();
        stripe
            .by_hash
            .find(hash, |&(h, ref entry)| {
                h == hash && entry.labels.eq_sorted_pairs(pairs)
            })
            .map(|(_, entry)| Arc::clone(entry))
    }

    #[inline]
    fn get_by_encoded_labels(&self, hash: u64, encoded_labels: &[u8]) -> Option<Arc<SeriesEntry>> {
        let idx = self.stripe_idx(hash);
        let stripe = self.stripes[idx].read();
        stripe
            .by_hash
            .find(hash, |&(h, ref entry)| {
                h == hash && entry.labels.as_bytes() == encoded_labels
            })
            .map(|(_, entry)| Arc::clone(entry))
    }

    fn resolve_prehashed_batch<T>(
        &self,
        items: &[T],
        get_hp: impl Fn(&T) -> Option<(u64, &[(&str, &str)])>,
        out: &mut Vec<Option<Arc<SeriesEntry>>>,
    ) {
        out.clear();
        let n = items.len();
        if n == 0 {
            return;
        }
        out.resize_with(n, || None);

        const CHUNK: usize = 16;
        const STRIPE_PREFETCH_AHEAD: usize = 4;

        let mut start = 0usize;
        while start < n {
            let end = (start + CHUNK).min(n);

            // Pass 1: prefetch stripes ahead, probe H2 + inline 64-bit `h` in HashTable,
            // clone candidate, and prefetch `labels.as_bytes()` + `SeriesState` for Pass 2.
            for i in start..end {
                if i + STRIPE_PREFETCH_AHEAD < end {
                    if let Some((h_ahead, _)) = get_hp(&items[i + STRIPE_PREFETCH_AHEAD]) {
                        let idx_ahead = self.stripe_idx(h_ahead);
                        prefetch_read_data(&self.stripes[idx_ahead]);
                    }
                }
                let Some((hash, _)) = get_hp(&items[i]) else {
                    continue;
                };
                let idx = self.stripe_idx(hash);
                let stripe = self.stripes[idx].read();
                let mut found: Option<Arc<SeriesEntry>> = None;
                for &(h, ref cand) in stripe.by_hash.iter_hash(hash) {
                    if h == hash {
                        found = Some(Arc::clone(cand));
                        break;
                    }
                }
                drop(stripe);
                if let Some(cloned) = found {
                    let ptr = Arc::as_ptr(&cloned);
                    let lbl_bytes = cloned.labels.as_bytes();
                    if !lbl_bytes.is_empty() {
                        prefetch_read_data(lbl_bytes.as_ptr());
                    }
                    prefetch_read_data((ptr as *const u8).wrapping_add(64));
                    out[i] = Some(cloned);
                }
            }

            // Pass 2: verify candidate `hash` and `eq_sorted_pairs` while `labels` bytes are hot in L1.
            for i in start..end {
                let Some((hash, pairs)) = get_hp(&items[i]) else {
                    continue;
                };
                if let Some(ref cand) = out[i] {
                    if cand.hash == hash && cand.labels.eq_sorted_pairs(pairs) {
                        continue;
                    }
                    out[i] = self.get_by_sorted_pairs(hash, pairs);
                }
            }

            start = end;
        }
    }

    fn resolve_prehashed_encoded_batch<T>(
        &self,
        items: &[T],
        get_he: impl Fn(&T) -> Option<(u64, &[u8])>,
        out: &mut Vec<Option<Arc<SeriesEntry>>>,
    ) {
        out.clear();
        let n = items.len();
        if n == 0 {
            return;
        }
        out.resize_with(n, || None);

        const CHUNK: usize = 16;
        const STRIPE_PREFETCH_AHEAD: usize = 4;

        let mut start = 0usize;
        while start < n {
            let end = (start + CHUNK).min(n);

            // Pass 1: prefetch stripes ahead, probe H2 + inline 64-bit `h` in HashTable,
            // clone candidate, and prefetch `labels.as_bytes()` + `SeriesState` for Pass 2.
            for i in start..end {
                if i + STRIPE_PREFETCH_AHEAD < end {
                    if let Some((h_ahead, _)) = get_he(&items[i + STRIPE_PREFETCH_AHEAD]) {
                        let idx_ahead = self.stripe_idx(h_ahead);
                        prefetch_read_data(&self.stripes[idx_ahead]);
                    }
                }
                let Some((hash, _)) = get_he(&items[i]) else {
                    continue;
                };
                let idx = self.stripe_idx(hash);
                let stripe = self.stripes[idx].read();
                let mut found: Option<Arc<SeriesEntry>> = None;
                for &(h, ref cand) in stripe.by_hash.iter_hash(hash) {
                    if h == hash {
                        found = Some(Arc::clone(cand));
                        break;
                    }
                }
                drop(stripe);
                if let Some(cloned) = found {
                    let ptr = Arc::as_ptr(&cloned);
                    let lbl_bytes = cloned.labels.as_bytes();
                    if !lbl_bytes.is_empty() {
                        prefetch_read_data(lbl_bytes.as_ptr());
                    }
                    prefetch_read_data((ptr as *const u8).wrapping_add(64));
                    out[i] = Some(cloned);
                }
            }

            // Pass 2: verify `cand.labels.as_bytes() == encoded_labels` via single SIMD memcmp.
            for i in start..end {
                let Some((hash, encoded_labels)) = get_he(&items[i]) else {
                    continue;
                };
                if let Some(ref cand) = out[i] {
                    if cand.labels.as_bytes() == encoded_labels {
                        continue;
                    }
                    out[i] = self.get_by_encoded_labels(hash, encoded_labels);
                }
            }

            start = end;
        }
    }

    fn get_by_hash_and_labels(&self, hash: u64, labels: &Labels) -> Option<Arc<SeriesEntry>> {
        let idx = self.stripe_idx(hash);
        let stripe = self.stripes[idx].read();
        stripe
            .by_hash
            .find(hash, |&(h, ref entry)| h == hash && &entry.labels == labels)
            .map(|(_, entry)| Arc::clone(entry))
    }

    fn insert_unless_pending(
        &self,
        entry: Arc<SeriesEntry>,
    ) -> Result<(), Option<Arc<SeriesEntry>>> {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);
        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            if let Some((_, existing)) = s
                .by_hash
                .find(entry.hash, |&(h, ref e)| h == entry.hash && e.labels == entry.labels)
            {
                return Err(Some(Arc::clone(existing)));
            }
            if self.pending_tombstones.is_pending(entry.hash) {
                return Err(None);
            }
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages
                        .insert_under_stripe_lock(entry.ref_id, Arc::clone(&entry));
                }
            } else {
                s.insert_overflow(entry.ref_id, Arc::clone(&entry));
            }
            let hash = entry.hash;
            s.by_hash.insert_unique(hash, (hash, entry), |&(h, _)| h);
            Ok(())
        } else {
            let (first_idx, second_idx) = if h_idx < r_idx {
                (h_idx, r_idx)
            } else {
                (r_idx, h_idx)
            };
            let mut first = self.stripes[first_idx].write();
            let mut second = self.stripes[second_idx].write();
            let (hs, rs) = if h_idx < r_idx {
                (&mut *first, &mut *second)
            } else {
                (&mut *second, &mut *first)
            };
            if let Some((_, existing)) = hs
                .by_hash
                .find(entry.hash, |&(h, ref e)| h == entry.hash && e.labels == entry.labels)
            {
                return Err(Some(Arc::clone(existing)));
            }
            if self.pending_tombstones.is_pending(entry.hash) {
                return Err(None);
            }
            let hash = entry.hash;
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages
                        .insert_under_stripe_lock(entry.ref_id, Arc::clone(&entry));
                }
                hs.by_hash.insert_unique(hash, (hash, entry), |&(h, _)| h);
            } else {
                hs.by_hash
                    .insert_unique(hash, (hash, Arc::clone(&entry)), |&(h, _)| h);
                rs.insert_overflow(entry.ref_id, entry);
            }
            Ok(())
        }
    }

    fn insert(&self, entry: Arc<SeriesEntry>) {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);

        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages
                        .insert_under_stripe_lock(entry.ref_id, Arc::clone(&entry));
                }
            } else {
                s.insert_overflow(entry.ref_id, Arc::clone(&entry));
            }
            let hash = entry.hash;
            s.by_hash.insert_unique(hash, (hash, entry), |&(h, _)| h);
        } else {
            let (first_idx, second_idx) = if h_idx < r_idx {
                (h_idx, r_idx)
            } else {
                (r_idx, h_idx)
            };
            let mut first = self.stripes[first_idx].write();
            let mut second = self.stripes[second_idx].write();
            let (hs, rs) = if h_idx < r_idx {
                (&mut *first, &mut *second)
            } else {
                (&mut *second, &mut *first)
            };
            let hash = entry.hash;
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages
                        .insert_under_stripe_lock(entry.ref_id, Arc::clone(&entry));
                }
                hs.by_hash.insert_unique(hash, (hash, entry), |&(h, _)| h);
            } else {
                hs.by_hash
                    .insert_unique(hash, (hash, Arc::clone(&entry)), |&(h, _)| h);
                rs.insert_overflow(entry.ref_id, entry);
            }
        }
    }

    fn remove(&self, entry: &SeriesEntry) {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);

        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages.remove_under_stripe_lock(entry.ref_id);
                }
            } else {
                s.remove_overflow(entry.ref_id);
            }
            if let Ok(found) = s
                .by_hash
                .find_entry(entry.hash, |&(h, ref e)| {
                    h == entry.hash && e.ref_id == entry.ref_id
                })
            {
                found.remove();
            }
        } else {
            let (first_idx, second_idx) = if h_idx < r_idx {
                (h_idx, r_idx)
            } else {
                (r_idx, h_idx)
            };
            let mut first = self.stripes[first_idx].write();
            let mut second = self.stripes[second_idx].write();
            let (hs, rs) = if h_idx < r_idx {
                (&mut *first, &mut *second)
            } else {
                (&mut *second, &mut *first)
            };
            if let Ok(found) = hs
                .by_hash
                .find_entry(entry.hash, |&(h, ref e)| {
                    h == entry.hash && e.ref_id == entry.ref_id
                })
            {
                found.remove();
            }
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages.remove_under_stripe_lock(entry.ref_id);
                }
            } else {
                rs.remove_overflow(entry.ref_id);
            }
        }
    }

    /// Atomically verifies `pending_commits == 0`, marks `entry` & `st` as GCed,
    /// and unlinks `entry` from `by_id` and `by_hash` while holding both `entry.inner`
    /// and the stripe write lock(s).
    fn remove_if_gced_under_entry_lock(&self, entry: &SeriesEntry, st: &mut SeriesState) -> bool {
        self.remove_if_gced_under_entry_lock_inner(entry, st, false)
    }

    /// Same as `remove_if_gced_under_entry_lock`, additionally registering `entry.hash` in
    /// `pending_tombstones` under the hash stripe lock before unlinking.
    fn remove_if_gced_with_pending_tombstone(
        &self,
        entry: &SeriesEntry,
        st: &mut SeriesState,
    ) -> bool {
        self.remove_if_gced_under_entry_lock_inner(entry, st, true)
    }

    fn remove_if_gced_under_entry_lock_inner(
        &self,
        entry: &SeriesEntry,
        st: &mut SeriesState,
        record_pending_tombstone: bool,
    ) -> bool {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);

        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            if !entry.try_mark_gced() {
                return false;
            }
            if record_pending_tombstone {
                self.pending_tombstones.add(entry.hash);
            }
            st.set_gced(true);
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages.remove_under_stripe_lock(entry.ref_id);
                }
            } else {
                s.remove_overflow(entry.ref_id);
            }
            if let Ok(found) = s
                .by_hash
                .find_entry(entry.hash, |&(h, ref e)| {
                    h == entry.hash && e.ref_id == entry.ref_id
                })
            {
                found.remove();
            }
            true
        } else {
            let (first_idx, second_idx) = if h_idx < r_idx {
                (h_idx, r_idx)
            } else {
                (r_idx, h_idx)
            };
            let mut first = self.stripes[first_idx].write();
            let mut second = self.stripes[second_idx].write();
            if !entry.try_mark_gced() {
                return false;
            }
            if record_pending_tombstone {
                self.pending_tombstones.add(entry.hash);
            }
            st.set_gced(true);
            let (hs, rs) = if h_idx < r_idx {
                (&mut *first, &mut *second)
            } else {
                (&mut *second, &mut *first)
            };
            if let Ok(found) = hs
                .by_hash
                .find_entry(entry.hash, |&(h, ref e)| {
                    h == entry.hash && e.ref_id == entry.ref_id
                })
            {
                found.remove();
            }
            if entry.ref_id < MAX_PAGED_SERIES_ID {
                // SAFETY: `r_idx` write lock is held.
                unsafe {
                    self.by_id_pages.remove_under_stripe_lock(entry.ref_id);
                }
            } else {
                rs.remove_overflow(entry.ref_id);
            }
            true
        }
    }

    fn unlink_hash(&self, hash: u64, ref_id: u64) {
        let h_idx = self.stripe_idx(hash);
        let mut hs = self.stripes[h_idx].write();
        if let Ok(found) = hs
            .by_hash
            .find_entry(hash, |&(h, ref e)| h == hash && e.ref_id == ref_id)
        {
            found.remove();
        }
    }

    fn collect_stripe_into(stripe: &RwLock<SeriesStripe>, out: &mut Vec<Arc<SeriesEntry>>) {
        let guard = stripe.read();
        if guard.by_hash.is_empty() {
            return;
        }
        out.extend(guard.by_hash.iter().map(|(_, e)| Arc::clone(e)));
    }

    fn all_entries(&self) -> Vec<Arc<SeriesEntry>> {
        let mut out = Vec::new();
        for s in &self.stripes {
            Self::collect_stripe_into(s, &mut out);
        }
        out
    }

    fn shrink_one_stripe(s: &mut SeriesStripe) {
        if let Some(ref mut map) = s.by_id_overflow {
            if map.is_empty() {
                s.by_id_overflow = None;
            } else {
                let id_len = map.len();
                if map.capacity() > (id_len * 4).max(64) {
                    map.shrink_to(id_len * 2);
                }
            }
        }
        if s.by_hash.is_empty() {
            if s.by_hash.capacity() > 0 {
                s.by_hash = HashTable::new();
            }
        } else {
            let hash_len = s.by_hash.len();
            if s.by_hash.capacity() > (hash_len * 4).max(64) {
                s.by_hash.shrink_to(hash_len * 2, |&(h, _)| h);
            }
        }
    }

    fn shrink_oversized_stripes(&self) {
        let _shrink_guard = self.shrink_mtx.lock();
        // SAFETY: `shrink_mtx` is held, so no other thread can free `ByIdPageDir` concurrently.
        if unsafe { self.by_id_pages.has_empty_page_under_shrink_lock() } {
            let mut guards = Vec::with_capacity(self.stripes.len());
            for stripe in &self.stripes {
                guards.push(stripe.write());
            }
            // SAFETY: all stripe write locks are held, blocking all readers/writers on `by_id_pages`.
            unsafe {
                self.by_id_pages
                    .shrink_empty_pages_under_all_stripe_locks();
            }
            for s in &mut guards {
                Self::shrink_one_stripe(s);
            }
            return;
        }
        for stripe in &self.stripes {
            let mut s = stripe.write();
            Self::shrink_one_stripe(&mut s);
        }
    }
    #[cfg(test)]
    fn allocated_page_count(&self) -> usize {
        let _g = self.shrink_mtx.lock();
        // SAFETY: `shrink_mtx` prevents `shrink_empty_pages_under_all_stripe_locks` from
        // freeing any `ByIdPageDir` concurrently.
        unsafe { self.by_id_pages.allocated_page_count_under_shrink_lock() }
    }
}

/// Series chunks + tombstones returned by [`Head::series_chunks`].
pub type SeriesChunksResult = Option<(Labels, Vec<ChunkMeta>, Vec<Interval>)>;

/// Lightweight series chunk intervals returned by [`Head::series_chunk_metas_no_bytes`].
pub type SeriesChunkMetasNoBytes = Option<(Labels, SmallVec<[(i64, i64); 4]>, bool)>;

/// Lightweight series chunk intervals + Phase 1 snapshot returned by
/// [`Head::series_chunk_metas_no_bytes_with_snapshot`].
pub type SeriesChunkMetasWithSnapshot =
    Option<(Labels, SmallVec<[(i64, i64); 4]>, HeadChunkSnapshot)>;

/// Phase 1 snapshot of Head chunks for a single series in two-phase lazy `QueryStream`
/// (guaranteeing `QueryStreamSeries.chunk_count == QueryStreamSeriesChunks.chunks.len()`).
#[derive(Clone, Debug)]
pub enum HeadChunkSnapshot {
    /// Fast path: sorted disjoint in-order chunks identified by their immutable `min_time`s.
    InOrderByMinTime(SmallVec<[i64; 4]>),
    /// Slow path (OOO, tombstones, or overlapping chunks): pre-merged chunks captured in Phase 1.
    Precomputed(Vec<ChunkMeta>),
}

/// Lightweight candidate list + `[min_time, max_time_inclusive]` range for streaming compaction.
pub type CompactionCandidatesRange = (Vec<(u64, Labels)>, i64, i64);

/// Chunk-shape snapshot of a series used to detect concurrent appends between
/// candidate selection and post-compaction eviction (Go `Head.seriesFingerprint`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeriesFingerprint {
    pub total_chunks: usize,
    pub active_min_time: i64,
    pub active_chunk_samples: usize,
    pub had_pending_commits: bool,
    pub watermark_violated_at_snapshot: bool,
}

impl SeriesFingerprint {
    fn capture(
        entry: &SeriesEntry,
        st: &SeriesState,
        tx_ring: Option<&TxRing>,
        append_id_watermark: u64,
    ) -> Self {
        let total_chunks = st.mmapped().len() + st.head_chunks_len();
        let (active_chunk_samples, active_min_time) = if st.has_active_chunk() {
            (st.active_chunk_num_samples(), st.active_min_time)
        } else {
            (0, i64::MAX)
        };
        Self {
            total_chunks,
            active_min_time,
            active_chunk_samples,
            had_pending_commits: entry.pending_commits_count() > 0,
            watermark_violated_at_snapshot: append_id_watermark < u64::MAX
                && tx_has_append_id_above(tx_ring, append_id_watermark),
        }
    }

    fn has_mutated(
        entry: &SeriesEntry,
        st: &SeriesState,
        fingerprints: &HashMap<u64, SeriesFingerprint>,
    ) -> bool {
        let Some(fp) = fingerprints.get(&entry.ref_id) else {
            return true;
        };
        if fp.had_pending_commits || fp.watermark_violated_at_snapshot {
            return true;
        }
        let total_chunks = st.mmapped().len() + st.head_chunks_len();
        if total_chunks != fp.total_chunks {
            return true;
        }
        let (active_chunk_samples, active_min_time) = if st.has_active_chunk() {
            (st.active_chunk_num_samples(), st.active_min_time)
        } else {
            (0, i64::MAX)
        };
        active_chunk_samples != fp.active_chunk_samples || active_min_time != fp.active_min_time
    }
}

/// Set of series refs currently being evicted by `evict_series_refs_inner` (Go `evictedSeries`).
#[derive(Debug)]
pub struct EvictedSeries {
    /// Sorted, deduplicated series refs.
    refs: Vec<u64>,
    maxt_inclusive: i64,
    fingerprints: Option<Arc<HashMap<u64, SeriesFingerprint>>>,
    /// Series kept alive for exemplar replay whose chunks were cleared during eviction.
    truncated_for_exemplar: Mutex<HashSet<u64>>,
}

impl EvictedSeries {
    pub fn new(
        mut refs: Vec<u64>,
        maxt_inclusive: i64,
        fingerprints: Option<Arc<HashMap<u64, SeriesFingerprint>>>,
    ) -> Self {
        refs.sort_unstable();
        refs.dedup();
        Self {
            refs,
            maxt_inclusive,
            fingerprints,
            truncated_for_exemplar: Mutex::new(HashSet::new()),
        }
    }

    #[inline]
    pub fn contains(&self, ref_id: u64) -> bool {
        self.refs.binary_search(&ref_id).is_ok()
    }

    #[inline]
    pub(crate) fn mark_truncated_for_exemplar(&self, ref_id: u64) {
        self.truncated_for_exemplar.lock().insert(ref_id);
    }

    #[inline]
    fn is_truncated_for_exemplar_empty(&self, ref_id: u64, st: &SeriesState) -> bool {
        st.head_chunks_is_empty()
            && st.mmapped().is_empty()
            && self.truncated_for_exemplar.lock().contains(&ref_id)
    }

    #[inline]
    fn should_skip_in_order(
        &self,
        entry: &SeriesEntry,
        st: &SeriesState,
        ino_mint: i64,
    ) -> bool {
        if ino_mint > self.maxt_inclusive || !self.contains(entry.ref_id) {
            return false;
        }
        if entry.pending_commits_count() > 0 || st.max_time() > self.maxt_inclusive {
            return false;
        }
        if !self.is_truncated_for_exemplar_empty(entry.ref_id, st)
            && self
                .fingerprints
                .as_ref()
                .is_some_and(|fps| SeriesFingerprint::has_mutated(entry, st, fps))
        {
            return false;
        }
        let has_ooo = st.ooo().is_some_and(|o| {
            o.ooo_head_chunk.is_some()
                || !o.ooo_mmapped_chunks.is_empty()
                || !o.ooo_pending_mmapped.is_empty()
        });
        !has_ooo
    }
}

/// Snapshot of Head compaction-isolation state captured atomically under `blocks.read()`.
#[derive(Clone, Debug, Default)]
pub struct HeadReadIsolation {
    /// Effective in-order query min time (clamped by `last_memory_truncation_time` when
    /// `mem_truncation_in_process` was active at snapshot time).
    pub ino_mint: i64,
    /// Maximum OOO chunk disk ref already compacted into reloaded blocks at snapshot time
    /// (Go `db.lastGarbageCollectedMmapRef`); OOO chunks with `ref_id <= max_ooo_mmap_ref_to_skip`
    /// are skipped in Head so newly opened queriers do not emit duplicate OOO chunks.
    pub max_ooo_mmap_ref_to_skip: ChunkDiskMapperRef,
    /// Snapshot of `head.evicting_series` captured at query snapshot time (Go `rh.evicting`).
    pub evicting: Option<Arc<EvictedSeries>>,
}

impl HeadReadIsolation {
    #[inline]
    fn should_skip_in_order(&self, entry: &SeriesEntry, st: &SeriesState) -> bool {
        self.evicting
            .as_ref()
            .is_some_and(|e| e.should_skip_in_order(entry, st, self.ino_mint))
    }

    #[inline]
    pub(crate) fn evicting_contains(&self, ref_id: u64) -> bool {
        self.evicting.as_ref().is_some_and(|e| e.contains(ref_id))
    }
}

/// Snapshot of OOO compaction candidates bounded by `last_mmap_ref` and `last_wbl_file`
/// (matching Prometheus `OOOCompactionHead`).
#[derive(Clone, Debug)]
pub struct OooCompactionSnapshot {
    pub candidates: Vec<(u64, Labels)>,
    pub min_time: i64,
    pub max_time: i64,
    pub last_mmap_ref: ChunkDiskMapperRef,
    pub last_wbl_file: Option<usize>,
}

/// Maps a WAL/WBL record decode error to a repairable `wlog.CorruptionErr` positioned at
/// the reader's current segment/offset (Go `loadWAL` / `loadWBL`).
fn record_decode_corruption<R: io::Read + SegmentTracker>(
    r: &WlogReader<R>,
    e: &dyn std::fmt::Display,
) -> HeadError {
    HeadError::Wlog(WlogError::Corruption {
        dir: r.segment_dir().map(Path::to_path_buf),
        segment: r.segment(),
        offset: r.offset(),
        message: format!("decode record: {e}"),
    })
}

/// Go `loadWAL` / `loadWBL` unknown series reference accounting (per replay pass; Go
/// accounts per WAL segment).
#[derive(Default)]
struct UnknownRefs {
    samples: AtomicU64,
    histograms: AtomicU64,
    exemplars: AtomicU64,
    metadata: AtomicU64,
    tombstones: AtomicU64,
    mmap_markers: AtomicU64,
    series: Mutex<HashSet<u64>>,
}

impl UnknownRefs {
    #[cold]
    fn miss(&self, kind: &AtomicU64, series_ref: u64) {
        kind.fetch_add(1, Ordering::Relaxed);
        self.series.lock().insert(series_ref);
    }
}

/// Result of [`Head::load_mmapped_chunks`] (Go `mmappedChunks`, `oooMmappedChunks`, `lastMmapRef`).
#[derive(Default)]
struct MmappedLoad {
    in_order: HashMap<u64, Vec<MmappedChunk>>,
    ooo: HashMap<u64, Vec<MmappedChunk>>,
    last_ref: ChunkDiskMapperRef,
}

/// Go `headMetrics`, registered on the per-tenant registry.
struct HeadMetrics {
    mmap_chunk_corruptions_total: prometheus::Counter,
    wal_corruptions_total: prometheus::Counter,
    snapshot_replay_error_total: prometheus::Counter,
    pending_commit_underflow: prometheus::Counter,
    ooo_samples_appended: CounterVec,
    ooo_histogram: SingleHistogram,
    active_appenders: prometheus::Gauge,
    appenders_created: prometheus::Counter,
    chunks: prometheus::Gauge,
    chunks_created: prometheus::Counter,
    chunks_removed: prometheus::Counter,
    series_not_found: prometheus::Counter,
    chunk_reads_of_removed_series: prometheus::Counter,
    gc_duration: crate::metrics::Summary,
    wal_truncate_duration: crate::metrics::Summary,
    data_replay_duration: prometheus::Gauge,
    samples_appended: CounterVec,
    duplicate_samples: CounterVec,
    out_of_bound_samples: CounterVec,
    out_of_order_samples: CounterVec,
    too_old_samples: CounterVec,
    head_truncate_fail: prometheus::Counter,
    head_truncate_total: prometheus::Counter,
    checkpoint_delete_fail: prometheus::Counter,
    checkpoint_delete_total: prometheus::Counter,
    checkpoint_creation_fail: prometheus::Counter,
    checkpoint_creation_total: prometheus::Counter,
    mmap_chunks_total: prometheus::Counter,
    head_chunks_max_mmapped: prometheus::Gauge,
    wal_replay_unknown_refs: CounterVec,
    wbl_replay_unknown_refs: CounterVec,
    wal: crate::metrics::WlogMetrics,
    wbl: Mutex<Option<crate::metrics::WlogMetrics>>,
    chunk_write_queue: Option<crate::metrics::ChunkWriteQueueMetrics>,
}

impl HeadMetrics {
    fn new_with_dirs(
        reg: Option<&Registry>,
        chunk_write_queue_size: usize,
        wal_dir: Option<&Path>,
    ) -> Self {
        use crate::metrics::{counter, counter_vec, gauge, summary};
        let ooo_samples_appended = CounterVec::new(
            Opts::new(
                "prometheus_tsdb_head_out_of_order_samples_appended_total",
                "Total number of appended out of order samples.",
            ),
            &["type"],
        )
        .expect("valid ooo samples appended counter vec");
        let _ = ooo_samples_appended.with_label_values(&["float"]);
        let _ = ooo_samples_appended.with_label_values(&["histogram"]);

        let ooo_histogram = native_histogram(
            reg,
            "prometheus_tsdb_sample_ooo_delta",
            "Delta in seconds by which a sample is considered out of order (reported regardless of OOO time window and whether sample is accepted or not).",
            Some(vec![
                600.0,
                1800.0,
                3600.0,
                7200.0,
                10800.0,
                21600.0,
                43200.0,
            ]),
        );
        if let Some(r) = reg {
            let _ = r.register(Box::new(ooo_samples_appended.clone()));
        }
        let pending_commit_underflow = counter(
            reg,
            "prometheus_tsdb_head_series_pending_commit_underflow_total",
            "Total number of attempts to release a pending-sample reservation when none was held.",
        );
        let snapshot_replay_error_total = counter(
            reg,
            "prometheus_tsdb_snapshot_replay_error_total",
            "Total number snapshot replays that failed.",
        );

        Self {
            active_appenders: gauge(
                reg,
                "prometheus_tsdb_head_active_appenders",
                "Number of currently active appender transactions",
            ),
            appenders_created: counter(
                reg,
                "prometheus_tsdb_head_appenders_created_total",
                "Total number of appender transactions created.",
            ),
            mmap_chunk_corruptions_total: counter(
                reg,
                "prometheus_tsdb_mmap_chunk_corruptions_total",
                "Total number of memory-mapped chunk corruptions.",
            ),
            wal_corruptions_total: counter(
                reg,
                "prometheus_tsdb_wal_corruptions_total",
                "Total number of WAL corruptions.",
            ),
            snapshot_replay_error_total,
            pending_commit_underflow,
            ooo_samples_appended,
            ooo_histogram,
            chunks: gauge(reg, "prometheus_tsdb_head_chunks", "Total number of chunks in the head block."),
            chunks_created: counter(
                reg,
                "prometheus_tsdb_head_chunks_created_total",
                "Total number of chunks created in the head",
            ),
            chunks_removed: counter(
                reg,
                "prometheus_tsdb_head_chunks_removed_total",
                "Total number of chunks removed in the head",
            ),
            series_not_found: counter(
                reg,
                "prometheus_tsdb_head_series_not_found_total",
                "Total number of requests for series that were not found.",
            ),
            chunk_reads_of_removed_series: counter(
                reg,
                "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
                "Total number of chunks that queries tried to read from the head after their series or chunks were evicted. The chunks are skipped because their samples are available from blocks.",
            ),
            
            gc_duration: summary(
                reg,
                "prometheus_tsdb_head_gc_duration_seconds",
                "Runtime of garbage collection in the head block.",
                &[],
            ),
            wal_truncate_duration: summary(
                reg,
                "prometheus_tsdb_wal_truncate_duration_seconds",
                "Duration of WAL truncation.",
                &[],
            ),
            data_replay_duration: gauge(
                reg,
                "prometheus_tsdb_data_replay_duration_seconds",
                "Time taken to replay the data on disk.",
            ),
            samples_appended: counter_vec(
                reg,
                "prometheus_tsdb_head_samples_appended_total",
                "Total number of appended samples.",
                "type",
            ),
            duplicate_samples: counter_vec(
                reg,
                "prometheus_tsdb_duplicate_samples_total",
                "Total number of samples rejected at Append or dropped at Commit because the series already had a sample at the same timestamp.",
                "type",
            ),
            out_of_bound_samples: counter_vec(
                reg,
                "prometheus_tsdb_out_of_bound_samples_total",
                "Total number of out of bound samples ingestion failed attempts with out of order support disabled.",
                "type",
            ),
            out_of_order_samples: counter_vec(
                reg,
                "prometheus_tsdb_out_of_order_samples_total",
                "Total number of out of order samples ingestion failed attempts due to out of order being disabled.",
                "type",
            ),
            too_old_samples: counter_vec(
                reg,
                "prometheus_tsdb_too_old_samples_total",
                "Total number of out of order samples ingestion failed attempts with out of support enabled, but sample outside of time window.",
                "type",
            ),
            head_truncate_fail: counter(
                reg,
                "prometheus_tsdb_head_truncations_failed_total",
                "Total number of head truncations that failed.",
            ),
            head_truncate_total: counter(
                reg,
                "prometheus_tsdb_head_truncations_total",
                "Total number of head truncations attempted.",
            ),
            checkpoint_delete_fail: counter(
                reg,
                "prometheus_tsdb_checkpoint_deletions_failed_total",
                "Total number of checkpoint deletions that failed.",
            ),
            checkpoint_delete_total: counter(
                reg,
                "prometheus_tsdb_checkpoint_deletions_total",
                "Total number of checkpoint deletions attempted.",
            ),
            checkpoint_creation_fail: counter(
                reg,
                "prometheus_tsdb_checkpoint_creations_failed_total",
                "Total number of checkpoint creations that failed.",
            ),
            checkpoint_creation_total: counter(
                reg,
                "prometheus_tsdb_checkpoint_creations_total",
                "Total number of checkpoint creations attempted.",
            ),
            mmap_chunks_total: counter(
                reg,
                "prometheus_tsdb_mmap_chunks_total",
                "Total number of chunks that were memory-mapped.",
            ),
            head_chunks_max_mmapped: gauge(
                reg,
                "prometheus_tsdb_head_chunks_max_mmapped",
                "Maximum number of head chunks memory-mapped for any individual series during the last memory-mapping pass.",
            ),
            
            wal_replay_unknown_refs: counter_vec(
                reg,
                "prometheus_tsdb_wal_replay_unknown_refs_total",
                "Total number of unknown series references encountered during WAL replay.",
                "type",
            ),
            wbl_replay_unknown_refs: counter_vec(
                reg,
                "prometheus_tsdb_wbl_replay_unknown_refs_total",
                "Total number of unknown series references encountered during WBL replay.",
                "type",
            ),
            wal: crate::metrics::WlogMetrics::new_with_dir(
                reg,
                crate::metrics::WAL_METRICS_PREFIX,
                wal_dir,
            ),
            wbl: Mutex::new(None),
            chunk_write_queue: (chunk_write_queue_size > 0)
                .then(|| crate::metrics::ChunkWriteQueueMetrics::new(reg)),
        }
    }

    fn ensure_wbl_metrics(&self, reg: Option<&Registry>, wbl_dir: &Path) -> crate::metrics::WlogMetrics {
        let mut g = self.wbl.lock();
        if let Some(ref m) = *g {
            return m.clone();
        }
        let m = crate::metrics::WlogMetrics::new_with_dir(
            reg,
            crate::metrics::WBL_METRICS_PREFIX,
            Some(wbl_dir),
        );
        *g = Some(m.clone());
        m
    }

    /// Go `onChunkCreated` / `appendChunkAndMmap`: one chunk created in the head.
    #[inline]
    fn chunks_created(&self, n: u64) {
        if n > 0 {
            self.chunks.add(n as f64);
            self.chunks_created.inc_by(n as f64);
        }
    }

    /// Returns `appended` from an in-order `(appended, chunk_created)` append result,
    /// counting the created chunk (Go `onChunkCreated` / `appendChunkAndMmap`).
    #[inline]
    fn track_append(&self, (appended, created): (bool, bool)) -> bool {
        if created {
            self.chunks_created(1);
        }
        appended
    }

    #[inline]
    fn record_append_err(&self, err: &HeadError, typ: &str) {
        match err {
            HeadError::OutOfBounds => self.out_of_bound_samples.with_label_values(&[typ]).inc(),
            HeadError::OutOfOrderSample => self.out_of_order_samples.with_label_values(&[typ]).inc(),
            HeadError::TooOldSample => self.too_old_samples.with_label_values(&[typ]).inc(),
            // Go `headAppenderV2.Append`: `errors.Is(appErr, errConflictingSample)`.
            HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_) => {
                self.duplicate_samples.with_label_values(&[typ]).inc()
            }
            _ => {}
        }
    }

    /// A sample identical (timestamp and value) to the last one of its series. Go queues it and
    /// drops it in `Commit` (`recordDroppedExactDup`); here it is dropped when it is appended.
    #[inline]
    fn exact_duplicate_dropped(&self, typ: &str) {
        self.duplicate_samples.with_label_values(&[typ]).inc();
    }

    /// Go `chunksRemoved.Add(n); chunks.Sub(n)`.
    #[inline]
    fn chunks_removed(&self, n: u64) {
        if n > 0 {
            self.chunks.sub(n as f64);
            self.chunks_removed.inc_by(n as f64);
        }
    }

    /// Go `counterAddNonZero`.
    fn add_unknown_refs(vec: &CounterVec, n: u64, typ: &str) {
        if n > 0 {
            vec.with_label_values(&[typ]).inc_by(n as f64);
        }
    }
}

/// Striped in-memory TSDB Head backed by WAL, WBL, and mmap `chunks_head`.
pub struct Head {
    dir: PathBuf,
    chunks_dir: PathBuf,
    read_only: bool,
    disable_wal: bool,
    enable_metadata_wal_records: AtomicBool,
    opts: HeadOptions,
    metrics: HeadMetrics,
    symbols: RwLock<SymbolTable>,
    series: StripeSeries,
    series_creation_mtx: Mutex<()>,
    postings: MemPostings,
    postings_cache: Arc<PostingsForMatchersCache>,
    postings_cache_source_id: u64,
    tombstones: MemTombstones,
    exemplars: CircularExemplarStorage,
    metadata: UserMetricsMetadata,

    wal: Mutex<Option<WlogWriter>>,
    wbl: Mutex<Option<WlogWriter>>,
    chunk_writer: Mutex<Option<HeadChunksWriter>>,
    chunk_reader: RwLock<Option<HeadChunksReader>>,
    mmap_mtx: Mutex<()>,
    chunk_snapshot_mtx: Mutex<()>,
    ooo_wbl_gate: RwLock<()>,
    ooo_compact_wbl_cut_seg: Mutex<Option<usize>>,

    min_time: Arc<AtomicI64>,
    max_time: Arc<AtomicI64>,
    min_valid_time: AtomicI64,
    min_ooo_time: AtomicI64,
    max_ooo_time: AtomicI64,
    ooo_time_window_ms: AtomicI64,
    chunk_end_time_variance_bits: AtomicU64,
    metadata_limits_max_metrics: AtomicUsize,
    metadata_limits_max_per_metric: AtomicUsize,
    float_chunk_encoding: AtomicU8,
    last_series_id: AtomicU64,
    num_series: AtomicU64,
    num_stale_series: Arc<AtomicU64>,
    num_native_histogram_series: Arc<AtomicU64>,
    num_native_histogram_buckets: Arc<AtomicU64>,
    last_wal_truncation_time: AtomicI64,
    last_memory_truncation_time: AtomicI64,
    mem_truncation_in_process: AtomicBool,
    has_evicting_series: AtomicBool,
    evicting_series: RwLock<Option<Arc<EvictedSeries>>>,
    closed: AtomicBool,
    has_any_metadata: AtomicBool,
    enable_st_as_zero_sample: AtomicBool,
    enable_memory_snapshot_on_shutdown: AtomicBool,
    deleted_series_since_symbol_gc: AtomicU64,
    symbol_arena_baseline_bytes: AtomicU64,
    symbol_gc_arena_growth_threshold: AtomicU64,
    wal_expiries: Mutex<HashMap<u64, i64>>,
    wbl_pinned_refs: Mutex<HashSet<u64>>,
    ooo_unflushed_series: Mutex<HashSet<u64>>,
    has_ooo_unflushed: AtomicBool,
    appender_pool: Mutex<Vec<AppenderScratch>>,
    lifecycle_callback: RwLock<Option<Arc<dyn SeriesLifecycleCallback>>>,
    after_waiting_for_readers_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    before_deleting_postings_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    after_series_lookup_hook: Mutex<Option<AfterSeriesLookupHook>>,
    has_after_series_lookup_hook: AtomicBool,
    test_commit_pause: Mutex<HashMap<u64, CommitPauseLatch>>,
    has_test_commit_pause: AtomicBool,
    iso: Isolation,
    series_txs: Vec<Mutex<FastU64Map<TxRing>>>,
    active_reads: Arc<HeadActiveReads>,
    series_gauge: Option<prometheus::Gauge>,
    series_created_total: Option<prometheus::Counter>,
    series_removed_total: Option<prometheus::Counter>,
}

#[derive(Debug, Default)]
struct HeadActiveReadsState {
    reads: Vec<(u64, i64, i64)>,
    next_id: u64,
}

#[derive(Debug, Default)]
struct HeadActiveReads {
    state: Mutex<HeadActiveReadsState>,
    cv: Condvar,
}

impl HeadActiveReads {
    fn start(&self, mint: i64, maxt: i64) -> u64 {
        let mut state = self.state.lock();
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.reads.push((id, mint, maxt));
        id
    }

    /// Registers another reader for an already registered read. It keeps the original `id`, so a
    /// waiter whose cutoff covers the original also waits for the clone.
    fn start_clone(&self, id: u64, mint: i64, maxt: i64) {
        self.state.lock().reads.push((id, mint, maxt));
    }

    fn finish(&self, id: u64) {
        let mut state = self.state.lock();
        if let Some(pos) = state.reads.iter().position(|(rid, _, _)| *rid == id) {
            state.reads.swap_remove(pos);
            self.cv.notify_all();
        }
    }

    /// Waits for the readers overlapping `[mint, maxt_exclusive)` that were registered before
    /// this call. Readers registered later are not waited for, so a steady stream of queries
    /// cannot starve the caller; the caller must make sure those later readers are safe (see
    /// the call sites).
    fn wait_for_range(&self, mint: i64, maxt_exclusive: i64) {
        if maxt_exclusive <= mint {
            return;
        }
        let maxt_inclusive = maxt_exclusive - 1;
        let mut state = self.state.lock();
        let cutoff = state.next_id;
        while state
            .reads
            .iter()
            .any(|&(id, r_mint, r_maxt)| id < cutoff && r_mint <= maxt_inclusive && mint <= r_maxt)
        {
            self.cv.wait(&mut state);
        }
    }
}

/// Active reader guard on [`Head`] preventing `truncate_memory` from unlinking Head
/// chunks for `[mint, maxt]` while a concurrent querier is reading them after releasing
/// `blocks_lock.read()` (matching Go `Head.WaitForPendingReadersInTimeRange`).
#[derive(Debug)]
pub struct HeadReadGuard {
    id: u64,
    mint: i64,
    maxt: i64,
    reads: Arc<HeadActiveReads>,
}

impl Clone for HeadReadGuard {
    fn clone(&self) -> Self {
        self.reads.start_clone(self.id, self.mint, self.maxt);
        Self {
            id: self.id,
            mint: self.mint,
            maxt: self.maxt,
            reads: Arc::clone(&self.reads),
        }
    }
}

impl Drop for HeadReadGuard {
    fn drop(&mut self) {
        self.reads.finish(self.id);
    }
}

impl Head {
    /// Opens (or initializes) a [`Head`] in `dir`, replaying `chunks_head`, `wal`, and `wbl`.
    pub fn open(
        dir: impl AsRef<Path>,
        opts: HeadOptions,
        min_valid_time: i64,
    ) -> Result<Self, HeadError> {
        let variance = opts.chunk_end_time_variance();
        Self::open_with_chunk_end_time_variance(dir, opts, min_valid_time, variance)
    }

    /// Opens a [`Head`] with Go's `ChunkEndTimeVariance` (0.0 disables jitter).
    pub fn open_with_chunk_end_time_variance(
        dir: impl AsRef<Path>,
        opts: HeadOptions,
        min_valid_time: i64,
        chunk_end_time_variance: f64,
    ) -> Result<Self, HeadError> {
        Self::open_with_seed(dir, opts, min_valid_time, chunk_end_time_variance, None)
    }

    /// Opens a [`Head`] with memory snapshot on shutdown enabled or disabled.
    pub fn open_with_snapshot(
        dir: impl AsRef<Path>,
        opts: HeadOptions,
        min_valid_time: i64,
        enable_memory_snapshot_on_shutdown: bool,
    ) -> Result<Self, HeadError> {
        let variance = opts.chunk_end_time_variance();
        Self::open_with_seed_and_snapshot(
            dir,
            opts,
            min_valid_time,
            variance,
            None,
            enable_memory_snapshot_on_shutdown,
        )
    }

    pub(crate) fn open_with_seed(
        dir: impl AsRef<Path>,
        opts: HeadOptions,
        min_valid_time: i64,
        chunk_end_time_variance: f64,
        in_order_blocks_max_time: Option<i64>,
    ) -> Result<Self, HeadError> {
        Self::open_with_seed_and_snapshot(
            dir,
            opts,
            min_valid_time,
            chunk_end_time_variance,
            in_order_blocks_max_time,
            false,
        )
    }

    /// Opens a Head with extra non-breaking options (disable_wal, enable_metadata_wal_records,
    /// enable_memory_snapshot_on_shutdown).
    pub fn open_with_extra(
        dir: impl AsRef<Path>,
        opts: HeadOptions,
        min_valid_time: i64,
        in_order_blocks_max_time: Option<i64>,
        disable_wal: bool,
        enable_metadata_wal_records: bool,
        enable_memory_snapshot_on_shutdown: bool,
    ) -> Result<Self, HeadError> {
        Self::open_with_extra_ext(
            dir,
            opts,
            min_valid_time,
            in_order_blocks_max_time,
            disable_wal,
            enable_metadata_wal_records,
            enable_memory_snapshot_on_shutdown,
            false,
        )
    }

    /// Opens a Head with full extra options including `isolation_disabled`.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_extra_ext(
        dir: impl AsRef<Path>,
        opts: HeadOptions,
        min_valid_time: i64,
        in_order_blocks_max_time: Option<i64>,
        disable_wal: bool,
        enable_metadata_wal_records: bool,
        enable_memory_snapshot_on_shutdown: bool,
        isolation_disabled: bool,
    ) -> Result<Self, HeadError> {
        let variance = opts.chunk_end_time_variance();
        Self::open_with_seed_and_snapshot_ext(
            dir,
            opts,
            min_valid_time,
            variance,
            in_order_blocks_max_time,
            enable_memory_snapshot_on_shutdown,
            disable_wal,
            enable_metadata_wal_records,
            isolation_disabled,
        )
    }

    fn open_with_seed_and_snapshot(
        dir: impl AsRef<Path>,
        opts: HeadOptions,
        min_valid_time: i64,
        chunk_end_time_variance: f64,
        in_order_blocks_max_time: Option<i64>,
        enable_memory_snapshot_on_shutdown: bool,
    ) -> Result<Self, HeadError> {
        Self::open_with_seed_and_snapshot_ext(
            dir,
            opts,
            min_valid_time,
            chunk_end_time_variance,
            in_order_blocks_max_time,
            enable_memory_snapshot_on_shutdown,
            false,
            true,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_with_seed_and_snapshot_ext(
        dir: impl AsRef<Path>,
        mut opts: HeadOptions,
        min_valid_time: i64,
        chunk_end_time_variance: f64,
        in_order_blocks_max_time: Option<i64>,
        enable_memory_snapshot_on_shutdown: bool,
        disable_wal: bool,
        enable_metadata_wal_records: bool,
        isolation_disabled: bool,
    ) -> Result<Self, HeadError> {
        if opts.chunk_range_ms < 1 {
            return Err(HeadError::InvalidOption(format!(
                "invalid chunk_range_ms {}",
                opts.chunk_range_ms
            )));
        }
        if opts.ooo_cap_max == 0 || opts.ooo_cap_max > 255 {
            return Err(HeadError::InvalidOption(format!(
                "invalid ooo_cap_max {} (must be 1..=255)",
                opts.ooo_cap_max
            )));
        }
        if opts.wal_replay_concurrency == 0 {
            opts.wal_replay_concurrency = std::thread::available_parallelism().map_or(4, |n| n.get());
        }

        let dir = dir.as_ref().to_path_buf();
        let wal_dir = dir.join("wal");
        let wbl_dir = dir.join(WBL_DIR_NAME);
        let chunks_dir = dir.join("chunks_head");

        if !disable_wal {
            fs::create_dir_all(&wal_dir)?;
            let _ = delete_temp_checkpoints(&wal_dir);
        }
        fs::create_dir_all(&chunks_dir)?;

        let mut min_valid_time = min_valid_time;
        if !disable_wal {
            if let Ok(Some(cp_mvt)) = read_min_valid_time(&wal_dir) {
                if cp_mvt > min_valid_time {
                    min_valid_time = cp_mvt;
                }
            }
        }

        let lifecycle_cb = match opts.series_lifecycle_callback.clone() {
            Some(cb) if cb.chunk_end_time_variance_override().is_some() => {
                cb.inner_lifecycle_callback().cloned()
            }
            other => other,
        };
        let (series_gauge, series_created_total, series_removed_total) =
            if let Some(ref reg) = opts.registry {
                let g = prometheus::Gauge::new(
                    "prometheus_tsdb_head_series",
                    "Total number of series in the head block.",
                )
                .expect("valid gauge");
                let c_created = prometheus::Counter::new(
                    "prometheus_tsdb_head_series_created_total",
                    "Total number of series created in the head",
                )
                .expect("valid counter");
                let c_removed = prometheus::Counter::new(
                    "prometheus_tsdb_head_series_removed_total",
                    "Total number of series removed in the head",
                )
                .expect("valid counter");
                let _ = reg.register(Box::new(g.clone()));
                let _ = reg.register(Box::new(c_created.clone()));
                let _ = reg.register(Box::new(c_removed.clone()));
                (Some(g), Some(c_created), Some(c_removed))
            } else {
                (None, None, None)
            };
        let float_enc = if opts.float_chunk_encoding == Encoding::Xor2 {
            Encoding::Xor2
        } else {
            Encoding::Xor
        };
        let metrics = HeadMetrics::new_with_dirs(
            opts.registry.as_deref(),
            opts.head_chunks_write_queue_size,
            (!disable_wal).then_some(wal_dir.as_path()),
        );
        let min_time = Arc::new(AtomicI64::new(i64::MAX));
        let max_time = Arc::new(AtomicI64::new(i64::MIN));
        let num_stale_series = Arc::new(AtomicU64::new(0));
        let num_native_histogram_series = Arc::new(AtomicU64::new(0));
        let num_native_histogram_buckets = Arc::new(AtomicU64::new(0));
        let iso = Isolation::new(isolation_disabled);
        {
            let reg = opts.registry.as_deref();
            let min_t = Arc::clone(&min_time);
            FnGaugeCollector::register(
                reg,
                "prometheus_tsdb_head_min_time",
                "Minimum time bound of the head block. The unit is decided by the library consumer.",
                move || min_t.load(Ordering::Acquire) as f64,
            );
            let max_t = Arc::clone(&max_time);
            FnGaugeCollector::register(
                reg,
                "prometheus_tsdb_head_max_time",
                "Maximum timestamp of the head block. The unit is decided by the library consumer.",
                move || max_t.load(Ordering::Acquire) as f64,
            );
            let stale_s = Arc::clone(&num_stale_series);
            FnGaugeCollector::register(
                reg,
                "prometheus_tsdb_head_stale_series",
                "Total number of stale series in the head block.",
                move || stale_s.load(Ordering::Acquire) as f64,
            );
            let nh_s = Arc::clone(&num_native_histogram_series);
            FnGaugeCollector::register(
                reg,
                "prometheus_tsdb_head_native_histogram_series",
                "Number of series whose most recent in-order sample is a native histogram.",
                move || nh_s.load(Ordering::Acquire) as f64,
            );
            let nh_b = Arc::clone(&num_native_histogram_buckets);
            FnGaugeCollector::register(
                reg,
                "prometheus_tsdb_head_native_histogram_buckets",
                "Number of positive and negative bucket entries in the most recent in-order native histogram sample of each series, including entries carried by staleness markers.",
                move || nh_b.load(Ordering::Acquire) as f64,
            );
            let iso_low = iso.clone();
            FnGaugeCollector::register(
                reg,
                "prometheus_tsdb_isolation_low_watermark",
                "The lowest TSDB append ID that is still referenced.",
                move || iso_low.low_watermark() as f64,
            );
            let iso_high = iso.clone();
            FnGaugeCollector::register(
                reg,
                "prometheus_tsdb_isolation_high_watermark",
                "The highest TSDB append ID that has been given out.",
                move || iso_high.last_append_id() as f64,
            );
        }
        let head = Self {
            dir: dir.clone(),
            chunks_dir: chunks_dir.clone(),
            read_only: false,
            disable_wal,
            enable_metadata_wal_records: AtomicBool::new(enable_metadata_wal_records),
            opts: opts.clone(),
            metrics,
            symbols: RwLock::new(SymbolTable::new()),
            series: StripeSeries::new(opts.stripe_size),
            series_creation_mtx: Mutex::new(()),
            postings: MemPostings::new_unordered(),
            postings_cache: opts
                .shared_postings_cache
                .clone()
                .unwrap_or_else(|| Arc::new(PostingsForMatchersCache::new(opts.postings_cache.clone()))),
            postings_cache_source_id: next_postings_cache_source_id(),
            tombstones: MemTombstones::new(),
            exemplars: CircularExemplarStorage::new_with_registry(
                if opts.enable_exemplar_storage {
                    opts.max_exemplars
                } else {
                    0
                },
                opts.ooo_time_window_ms,
                opts.registry.as_deref(),
            ),
            metadata: UserMetricsMetadata::new(),
            wal: Mutex::new(None),
            wbl: Mutex::new(None),
            chunk_writer: Mutex::new(None),
            chunk_reader: RwLock::new(None),
            mmap_mtx: Mutex::new(()),
            chunk_snapshot_mtx: Mutex::new(()),
            ooo_wbl_gate: RwLock::new(()),
            ooo_compact_wbl_cut_seg: Mutex::new(None),
            min_time,
            max_time,
            min_valid_time: AtomicI64::new(min_valid_time),
            min_ooo_time: AtomicI64::new(i64::MAX),
            max_ooo_time: AtomicI64::new(i64::MIN),
            ooo_time_window_ms: AtomicI64::new(opts.ooo_time_window_ms),
            chunk_end_time_variance_bits: AtomicU64::new(chunk_end_time_variance.to_bits()),
            metadata_limits_max_metrics: AtomicUsize::new(
                opts.metadata_limits.max_metrics_with_metadata_per_user,
            ),
            metadata_limits_max_per_metric: AtomicUsize::new(
                opts.metadata_limits.max_metadata_per_metric,
            ),
            float_chunk_encoding: AtomicU8::new(float_enc.as_u8()),
            last_series_id: AtomicU64::new(0),
            num_series: AtomicU64::new(0),
            num_stale_series,
            num_native_histogram_series,
            num_native_histogram_buckets,
            last_wal_truncation_time: AtomicI64::new(i64::MIN),
            last_memory_truncation_time: AtomicI64::new(i64::MIN),
            mem_truncation_in_process: AtomicBool::new(false),
            has_evicting_series: AtomicBool::new(false),
            evicting_series: RwLock::new(None),
            closed: AtomicBool::new(false),
            has_any_metadata: AtomicBool::new(false),
            enable_st_as_zero_sample: AtomicBool::new(false),
            enable_memory_snapshot_on_shutdown: AtomicBool::new(enable_memory_snapshot_on_shutdown),
            deleted_series_since_symbol_gc: AtomicU64::new(0),
            symbol_arena_baseline_bytes: AtomicU64::new(0),
            symbol_gc_arena_growth_threshold: AtomicU64::new(DEFAULT_SYMBOL_GC_ARENA_GROWTH_BYTES),
            wal_expiries: Mutex::new(HashMap::new()),
            wbl_pinned_refs: Mutex::new(HashSet::new()),
            ooo_unflushed_series: Mutex::new(HashSet::new()),
            has_ooo_unflushed: AtomicBool::new(false),
            appender_pool: Mutex::new(Vec::new()),
            lifecycle_callback: RwLock::new(lifecycle_cb),
            after_waiting_for_readers_hook: Mutex::new(None),
            before_deleting_postings_hook: Mutex::new(None),
            after_series_lookup_hook: Mutex::new(None),
            has_after_series_lookup_hook: AtomicBool::new(false),
            test_commit_pause: Mutex::new(HashMap::new()),
            has_test_commit_pause: AtomicBool::new(false),
            series_txs: (0..opts.stripe_size.clamp(64, 512))
                .map(|_| Mutex::new(FastU64Map::default()))
                .collect(),
            iso,
            active_reads: Arc::new(HeadActiveReads::default()),
            series_gauge,
            series_created_total,
            series_removed_total,
        };

        // Capture pre-open WAL/WBL segment bounds BEFORE `WlogWriter` cuts a new empty segment,
        // so replay never reads the freshly cut empty segment (matching Go `Head.Init`).
        let pre_open_wal_last = if disable_wal {
            None
        } else {
            segments_range(&wal_dir)?.map(|(_, last)| last)
        };

        // Like Go `DB.Open`, the WAL/WBL are opened (cutting a new segment) before the
        // head is initialised, so that a repair operates on the live write log.
        if !disable_wal {
            let mut wal_writer = WlogWriter::with_segment_size(
                &wal_dir,
                opts.wal_segment_size,
                opts.wal_compression,
            )?;
            wal_writer.set_metrics(head.metrics.wal.clone());
            *head.wal.lock() = Some(wal_writer);
        }

        let pre_open_wbl_range = if !disable_wal && (opts.ooo_time_window_ms > 0 || wbl_dir.exists()) {
            let range = segments_range(&wbl_dir)?;
            let mut wbl_writer = WlogWriter::with_segment_size(
                &wbl_dir,
                opts.wal_segment_size,
                opts.wal_compression,
            )?;
            let wbl_m = head
                .metrics
                .ensure_wbl_metrics(opts.registry.as_deref(), &wbl_dir);
            wbl_writer.set_metrics(wbl_m);
            *head.wbl.lock() = Some(wbl_writer);
            range
        } else {
            None
        };

        // Go `DB.reload()` -> `head.Truncate(inOrderBlocksMaxTime)` BEFORE `Head.Init` / WAL replay.
        if let Some(seed_maxt) = in_order_blocks_max_time {
            head.truncate_memory_inner(seed_maxt)?;
            head.min_valid_time.fetch_max(min_valid_time, Ordering::AcqRel);
        }

        let post_repair_chunk_seq = head.replay_on_open(
            &wal_dir,
            &wbl_dir,
            &chunks_dir,
            pre_open_wal_last,
            pre_open_wbl_range,
            disable_wal,
            false,
        )?;

        let mut chunk_writer = HeadChunksWriter::with_options(
            &chunks_dir,
            opts.max_head_chunk_file_size,
            opts.head_chunks_write_buffer_size,
        )?;
        chunk_writer.ensure_min_seq(post_repair_chunk_seq);
        if let Some(m) = head.metrics.chunk_write_queue.clone() {
            chunk_writer.set_queue_metrics(m);
        }
        *head.chunk_writer.lock() = Some(chunk_writer);
        if head.mmap_head_chunks_inner(false)? == 0 {
            head.refresh_chunk_reader()?;
        }

        Ok(head)
    }

    /// Opens a read-only Head backed by sandbox_dir (for hardlinked chunks_head)
    /// without creating, repairing, or modifying any files in db_dir.
    pub(crate) fn open_read_only(
        db_dir: &Path,
        sandbox_dir: &Path,
        min_valid_time: i64,
        replay_wal: bool,
    ) -> Result<Self, HeadError> {
        let opts = HeadOptions::default();
        let wal_dir = db_dir.join("wal");
        let wbl_dir = db_dir.join(WBL_DIR_NAME);
        let src_chunks = db_dir.join("chunks_head");
        let sandbox_chunks = sandbox_dir.join("chunks_head");
        fs::create_dir_all(&sandbox_chunks)?;
        if src_chunks.exists() {
            for entry in fs::read_dir(&src_chunks)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    let dst = sandbox_chunks.join(entry.file_name());
                    if !dst.exists() {
                        fs::hard_link(entry.path(), &dst)?;
                    }
                }
            }
        }

        let metrics = HeadMetrics::new_with_dirs(None, 0, None);
        let head = Self {
            dir: db_dir.to_path_buf(),
            chunks_dir: sandbox_chunks.clone(),
            read_only: true,
            disable_wal: false,
            enable_metadata_wal_records: AtomicBool::new(false),
            opts: opts.clone(),
            metrics,
            symbols: RwLock::new(SymbolTable::new()),
            series: StripeSeries::new(opts.stripe_size),
            series_creation_mtx: Mutex::new(()),
            postings: MemPostings::new_unordered(),
            postings_cache: Arc::new(PostingsForMatchersCache::new(opts.postings_cache.clone())),
            postings_cache_source_id: next_postings_cache_source_id(),
            tombstones: MemTombstones::new(),
            exemplars: CircularExemplarStorage::new_with_registry(0, 0, None),
            metadata: UserMetricsMetadata::new(),
            wal: Mutex::new(None),
            wbl: Mutex::new(None),
            chunk_writer: Mutex::new(None),
            chunk_reader: RwLock::new(None),
            mmap_mtx: Mutex::new(()),
            chunk_snapshot_mtx: Mutex::new(()),
            ooo_wbl_gate: RwLock::new(()),
            ooo_compact_wbl_cut_seg: Mutex::new(None),
            min_time: Arc::new(AtomicI64::new(i64::MAX)),
            max_time: Arc::new(AtomicI64::new(i64::MIN)),
            min_valid_time: AtomicI64::new(min_valid_time),
            min_ooo_time: AtomicI64::new(i64::MAX),
            max_ooo_time: AtomicI64::new(i64::MIN),
            ooo_time_window_ms: AtomicI64::new(0),
            chunk_end_time_variance_bits: AtomicU64::new(0.0f64.to_bits()),
            metadata_limits_max_metrics: AtomicUsize::new(0),
            metadata_limits_max_per_metric: AtomicUsize::new(0),
            float_chunk_encoding: AtomicU8::new(Encoding::Xor.as_u8()),
            last_series_id: AtomicU64::new(0),
            num_series: AtomicU64::new(0),
            num_stale_series: Arc::new(AtomicU64::new(0)),
            num_native_histogram_series: Arc::new(AtomicU64::new(0)),
            num_native_histogram_buckets: Arc::new(AtomicU64::new(0)),
            last_wal_truncation_time: AtomicI64::new(i64::MIN),
            last_memory_truncation_time: AtomicI64::new(i64::MIN),
            mem_truncation_in_process: AtomicBool::new(false),
            has_evicting_series: AtomicBool::new(false),
            evicting_series: RwLock::new(None),
            closed: AtomicBool::new(false),
            has_any_metadata: AtomicBool::new(false),
            enable_st_as_zero_sample: AtomicBool::new(false),
            enable_memory_snapshot_on_shutdown: AtomicBool::new(false),
            deleted_series_since_symbol_gc: AtomicU64::new(0),
            symbol_arena_baseline_bytes: AtomicU64::new(0),
            symbol_gc_arena_growth_threshold: AtomicU64::new(DEFAULT_SYMBOL_GC_ARENA_GROWTH_BYTES),
            wal_expiries: Mutex::new(HashMap::new()),
            wbl_pinned_refs: Mutex::new(HashSet::new()),
            ooo_unflushed_series: Mutex::new(HashSet::new()),
            has_ooo_unflushed: AtomicBool::new(false),
            appender_pool: Mutex::new(Vec::new()),
            lifecycle_callback: RwLock::new(None),
            after_waiting_for_readers_hook: Mutex::new(None),
            before_deleting_postings_hook: Mutex::new(None),
            after_series_lookup_hook: Mutex::new(None),
            has_after_series_lookup_hook: AtomicBool::new(false),
            test_commit_pause: Mutex::new(HashMap::new()),
            has_test_commit_pause: AtomicBool::new(false),
            series_txs: Vec::new(),
            iso: Isolation::new(true),
            active_reads: Arc::new(HeadActiveReads::default()),
            series_gauge: None,
            series_created_total: None,
            series_removed_total: None,
        };
        if min_valid_time > i64::MIN {
            head.truncate_memory_inner(min_valid_time)?;
            head.min_valid_time.fetch_max(min_valid_time, Ordering::AcqRel);
        }
        let pre_open_wal_last = if replay_wal && wal_dir.exists() {
            segments_range(&wal_dir)?.map(|(_, last)| last)
        } else {
            None
        };
        let pre_open_wbl_range = if replay_wal && wbl_dir.exists() {
            segments_range(&wbl_dir)?
        } else {
            None
        };
        head.replay_on_open(
            &wal_dir,
            &wbl_dir,
            &sandbox_chunks,
            pre_open_wal_last,
            pre_open_wbl_range,
            !replay_wal,
            true,
        )?;
        Ok(head)
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_on_open(
        &self,
        wal_dir: &Path,
        wbl_dir: &Path,
        chunks_dir: &Path,
        pre_open_wal_last: Option<usize>,
        pre_open_wbl_range: Option<(usize, usize)>,
        disable_wal: bool,
        read_only: bool,
    ) -> Result<usize, HeadError> {
        let replay_start = Instant::now();
        let mut snapshot_pos: Option<(usize, usize)> = None;
        if self.enable_memory_snapshot_on_shutdown.load(Ordering::Acquire) {
            if let Ok(Some((snap_dir, snap_idx, snap_offset))) = Self::last_chunk_snapshot(&self.dir) {
                let wal_behind = pre_open_wal_last.is_some_and(|last| last < snap_idx);
                if wal_behind {
                    let _ = Self::delete_chunk_snapshots(&self.dir, usize::MAX, usize::MAX);
                } else {
                    match self.load_chunk_snapshot(&snap_dir) {
                        Ok(()) => {
                            snapshot_pos = Some((snap_idx, snap_offset));
                        }
                        Err(e) => {
                            tracing::error!(err = %e, "Failed to load chunk snapshot");
                            self.metrics.snapshot_replay_error_total.inc();
                            self.reset_in_memory_state_after_snapshot_err();
                        }
                    }
                }
            }
        }
        // 1. Load mmapped chunks from chunks_head. Header/sequence errors are fatal (Go
        // `NewChunkDiskMapper` fails `NewHead`); iteration errors are repaired exactly like
        // Go `Head.Init` -> `removeCorruptedMmappedChunks` and the data is recovered from WAL.
        let mut reader = HeadChunksReader::open(chunks_dir)?;
        let MmappedLoad {
            in_order: mut mmapped_map,
            ooo: mut ooo_mmapped_map,
            last_ref: last_mmap_ref,
        } = match self.load_mmapped_chunks(&reader) {
            Ok(load) => load,
            Err((err, _)) => {
                tracing::error!(err = %err, "Loading on-disk chunks failed");
                if matches!(err, HeadChunksError::Corruption { .. }) {
                    self.metrics.mmap_chunk_corruptions_total.inc();
                }
                if snapshot_pos.take().is_some() {
                    self.reset_in_memory_state_after_snapshot_err();
                }
                self.remove_corrupted_mmapped_chunks(&mut reader, &err)
            }
        };
        let post_repair_chunk_seq = reader.max_seq();
        *self.chunk_reader.write() = Some(reader);

        // 2. Replay WAL checkpoint + segments. Like Go `Head.Init`, the first WAL error
        // aborts the replay (the WBL is then NOT replayed in this boot) and `DB.Open`
        // increments `prometheus_tsdb_wal_corruptions_total` and runs `wal.Repair(err)`,
        // which only handles `CorruptionErr`s carrying a segment index and always
        // repairs the WAL directory itself (even for errors read from a checkpoint).
        let mut multi_ref: HashMap<u64, u64> = HashMap::new();
        let wal_res = if disable_wal {
            Ok(())
        } else {
            self.replay_wal_dir(
                wal_dir,
                pre_open_wal_last,
                snapshot_pos,
                &mut multi_ref,
                &mut mmapped_map,
                &mut ooo_mmapped_map,
                last_mmap_ref,
            )
        };
        match wal_res {
            Err(err) if read_only => return Err(err),
            Err(err) => {
                self.metrics.wal_corruptions_total.inc();
                tracing::warn!(err = %err, "Encountered WAL read error, attempting repair");
                self.repair_write_log(&self.wal, err).map_err(|e| {
                    HeadError::Repair(format!("repair corrupted WAL: {e}"))
                })?;
                tracing::info!("Successfully repaired WAL");
            }
            Ok(()) => {
                // 3. Replay WBL segments one segment at a time (excluding the newly cut empty
                // writer segment), passing a chunk_writer so overflowing OOO head chunks spill to
                // `chunks_head` instead of dropping.
                if !disable_wal && wbl_dir.exists() {
                    if let Some((first_wbl, last_wbl)) = pre_open_wbl_range {
                        let mut replay_cw = HeadChunksWriter::with_max_file_size(
                            chunks_dir,
                            self.opts.max_head_chunk_file_size,
                        )?;
                        replay_cw.ensure_min_seq(post_repair_chunk_seq);
                        if let Some(m) = self.metrics.chunk_write_queue.clone() {
                            replay_cw.set_queue_metrics(m);
                        }
                        let mut wbl_res = Ok(());
                        for seg_idx in first_wbl..=last_wbl {
                            let mut r = match WlogReader::open_segment(wbl_dir, seg_idx) {
                                Ok(r) => r,
                                Err(e) => {
                                    wbl_res = Err(HeadError::from(e));
                                    break;
                                }
                            };
                            if let Err(e) = self.replay_wbl_reader(
                                &mut r,
                                &multi_ref,
                                last_mmap_ref,
                                Some(&mut replay_cw),
                            ) {
                                wbl_res = Err(e);
                                break;
                            }
                        }
                        let spilled = replay_cw.close().map_err(HeadError::from);
                        spilled?;
                        if let Err(err) = wbl_res {
                            if read_only {
                                return Err(err);
                            }
                            // Go `DB.Open`: `errLoadWbl` -> `wbl.Repair(e.err)`.
                            self.metrics.wal_corruptions_total.inc();
                            tracing::warn!(err = %err, "Encountered WBL read error, attempting repair");
                            self.repair_write_log(&self.wbl, err).map_err(|e| {
                                HeadError::Repair(format!("repair corrupted WBL: {e}"))
                            })?;
                            tracing::info!("Successfully repaired WBL");
                        }
                        self.refresh_chunk_reader()?;
                        self.promote_all_pending_ooo_mmapped();
                    }
                }
            }
        }

        self.postings.ensure_order_parallel();

        // Go `Head.Init` deferred order (LIFO): clamp `min_time` to `min_valid_time` BEFORE `gc`,
        // then run `gc(self.min_time())` so sample-less series replayed when uninitialized
        // (`min_time == i64::MAX`) are removed just like Go.
        let effective_mvt = self.min_valid_time.load(Ordering::Acquire);
        if self.initialized() && self.min_time() < effective_mvt {
            self.min_time.store(effective_mvt, Ordering::Release);
        }
        self.gc(self.min_time())?;
        self.metrics
            .data_replay_duration
            .set(replay_start.elapsed().as_secs_f64());
        self.symbol_arena_baseline_bytes
            .store(self.symbols.read().bytes_len() as u64, Ordering::Release);
        let final_chunk_seq = self
            .chunk_reader
            .read()
            .as_ref()
            .map_or(post_repair_chunk_seq, |r| post_repair_chunk_seq.max(r.max_seq()));
        Ok(final_chunk_seq)
    }

    /// WAL part of Go `Head.Init`: replays the last checkpoint (if any) and then every WAL
    /// segment `start_from..=end_at` one segment at a time, stopping at the first error.
    #[allow(clippy::too_many_arguments)]
    fn replay_wal_dir(
        &self,
        wal_dir: &Path,
        pre_open_wal_last: Option<usize>,
        snapshot_pos: Option<(usize, usize)>,
        multi_ref: &mut HashMap<u64, u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        last_mmap_ref: ChunkDiskMapperRef,
    ) -> Result<(), HeadError> {
        let mut start_from = snapshot_pos.map_or(0usize, |(idx, _)| idx);
        let mut global_missing_series_refs: HashSet<u64> = HashSet::new();
        if snapshot_pos.is_none() {
            if let Some((cp_dir, cp_idx)) = last_checkpoint(wal_dir)? {
                // A corrupted checkpoint is passed to the WAL repair like any other error
                // (Go: `backfill checkpoint: %w`).
                let mut r = WlogReader::open_dir(&cp_dir)?;
                self.replay_wal_reader(
                    &mut r,
                    multi_ref,
                    &mut global_missing_series_refs,
                    mmapped_map,
                    ooo_mmapped_map,
                    last_mmap_ref,
                )?;
                start_from = cp_idx + 1;
            }
        }
        if let Some(end_at) = pre_open_wal_last {
            for seg_idx in start_from..=end_at {
                let mut r = WlogReader::open_segment(wal_dir, seg_idx)?;
                if let Some((s_idx, s_off)) = snapshot_pos {
                    if seg_idx == s_idx && s_off > 0 {
                        while r.offset() < s_off {
                            if r.next_raw_record()?.is_none() {
                                break;
                            }
                        }
                    }
                }
                self.replay_wal_reader(
                    &mut r,
                    multi_ref,
                    &mut global_missing_series_refs,
                    mmapped_map,
                    ooo_mmapped_map,
                    last_mmap_ref,
                )?;
            }
        }
        Ok(())
    }

    /// Port of Go `wlog.WL.Repair(origErr)` as invoked by `DB.Open`: only a corruption
    /// error with a known segment index can be handled; the repair always operates on
    /// the given write log's own directory (the `CorruptionErr.Dir` is ignored, exactly like
    /// Go).
    fn repair_write_log(
        &self,
        log: &Mutex<Option<WlogWriter>>,
        err: HeadError,
    ) -> Result<(), String> {
        match err {
            HeadError::Wlog(WlogError::Corruption {
                segment: Some(seg),
                offset,
                ..
            }) => {
                tracing::warn!(segment = seg, offset = offset, "Starting corruption repair");
                let mut guard = log.lock();
                let w = guard.as_mut().ok_or_else(|| "write log not open".to_string())?;
                w.repair(seg, offset).map_err(|e| e.to_string())
            }
            HeadError::Wlog(WlogError::Corruption { segment: None, .. }) => {
                Err("corruption error does not specify position".to_string())
            }
            other => Err(format!("cannot handle error: {other}")),
        }
    }

    /// Port of Go `Head.loadMmappedChunks` (no chunk snapshot: every chunk is collected
    /// per series ref and attached during WAL replay). On error the partially collected
    /// maps are returned together with `secondLastRef` as `last_ref`, like Go.
    #[allow(clippy::result_large_err)]
    fn load_mmapped_chunks(
        &self,
        reader: &HeadChunksReader,
    ) -> Result<MmappedLoad, (HeadChunksError, MmappedLoad)> {
        let min_valid_time = self.min_valid_time.load(Ordering::Relaxed);
        let mut load = MmappedLoad::default();
        let mut second_last_ref = ChunkDiskMapperRef(0);
        let res = reader.iterate_all_chunks(|rec| {
            second_last_ref = load.last_ref;
            load.last_ref = rec.chunk_ref;
            if !rec.is_ooo && rec.maxt < min_valid_time {
                return Ok(());
            }
            // We ignore any chunk that doesn't have a valid encoding.
            if !rec.encoding.is_valid() {
                return Ok(());
            }
            let mc = MmappedChunk {
                ref_id: rec.chunk_ref,
                min_time: rec.mint,
                max_time: rec.maxt,
                num_samples: rec.num_samples,
                encoding: rec.encoding,
            };
            if rec.is_ooo {
                load.ooo.entry(rec.series_ref).or_default().push(mc);
                return Ok(());
            }
            let slice = load.in_order.entry(rec.series_ref).or_default();
            if let Some(last) = slice.last() {
                if last.max_time >= rec.mint {
                    self.metrics.mmap_chunk_corruptions_total.inc();
                    return Err(HeadChunksError::OutOfSequenceChunk {
                        series_ref: rec.series_ref,
                        last_mint: last.min_time,
                        last_maxt: last.max_time,
                        mint: rec.mint,
                        maxt: rec.maxt,
                    });
                }
            }
            slice.push(mc);
            Ok(())
        });
        match res {
            Ok(()) => Ok(load),
            Err(err) => {
                // secondLastRef because the lastRef caused an error.
                load.last_ref = second_last_ref;
                Err((err, load))
            }
        }
    }

    /// Port of Go `Head.removeCorruptedMmappedChunks`: deletes the corrupted m-map chunk
    /// files (or all of them if that is not possible) and re-attempts loading. Never fails:
    /// the data is recovered from the WAL.
    fn remove_corrupted_mmapped_chunks(
        &self,
        reader: &mut HeadChunksReader,
        err: &HeadChunksError,
    ) -> MmappedLoad {
        tracing::info!("Deleting mmapped chunk files");
        tracing::info!("Deleting mmapped chunk files");
        if let Err(del_err) = reader.delete_corrupted(err) {
            tracing::info!(
                err = %del_err,
                "Deletion of corrupted mmap chunk files failed, discarding chunk files completely"
            );
            if let Err(e) = reader.delete_all() {
                tracing::error!(err = %e, "Deletion of all mmap chunk files failed");
            }
            return MmappedLoad::default();
        }
        tracing::info!(
            "Deletion of mmap chunk files successful, reattempting m-mapping the on-disk chunks"
        );
        match self.load_mmapped_chunks(reader) {
            Ok(load) => load,
            Err((load_err, mut partial)) => {
                tracing::error!(
                    err = %load_err,
                    "Loading on-disk chunks failed, discarding chunk files completely"
                );
                if let Err(e) = reader.delete_all() {
                    tracing::error!(
                        err = %e,
                        "Deletion of all mmap chunk files failed after failed loading"
                    );
                }
                // Go only resets the in-order map here; OOO map and lastRef are kept.
                partial.in_order.clear();
                partial
            }
        }
    }

    fn refresh_chunk_reader(&self) -> Result<(), HeadChunksError> {
        let mut guard = self.chunk_reader.write();
        match *guard {
            Some(ref mut reader) => reader.refresh_in_place(),
            None => {
                let reader = HeadChunksReader::open_no_repair(&self.chunks_dir)?;
                *guard = Some(reader);
                Ok(())
            }
        }
    }

    /// Returns `(num_symbols, arena_bytes)` for the Head's interned symbol table.
    pub fn symbol_table_stats(&self) -> (usize, usize) {
        let syms = self.symbols.read();
        (syms.len(), syms.bytes_len())
    }

    #[doc(hidden)]
    pub fn set_symbol_gc_arena_growth_threshold_for_test(&self, bytes: u64) {
        self.symbol_gc_arena_growth_threshold
            .store(bytes.max(1), Ordering::Release);
    }

    #[doc(hidden)]
    pub fn set_wal_write_fault_hook_for_test(&self, hook: Option<crate::wlog::WriteFaultHook>) {
        if let Some(ref mut wal) = *self.wal.lock() {
            wal.set_write_fault_hook(hook);
        }
    }

    #[doc(hidden)]
    pub fn wal_last_segment_and_offset_for_test(&self) -> Result<(Option<usize>, usize), HeadError> {
        if let Some(ref wal) = *self.wal.lock() {
            Ok(wal.last_segment_and_offset()?)
        } else {
            Ok((None, 0))
        }
    }

    #[doc(hidden)]
    pub fn set_wbl_write_fault_hook_for_test(&self, hook: Option<crate::wlog::WriteFaultHook>) {
        if let Some(ref mut wbl) = *self.wbl.lock() {
            wbl.set_write_fault_hook(hook);
        }
    }

    #[doc(hidden)]
    pub fn set_chunk_writer_write_fault_hook_for_test(
        &self,
        hook: Option<crate::head_chunks::WriteFaultHook>,
    ) {
        if let Some(ref mut cw) = *self.chunk_writer.lock() {
            cw.set_write_fault_hook(hook);
        }
    }

    #[doc(hidden)]
    pub fn set_after_waiting_for_readers_hook_for_test(
        &self,
        hook: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        *self.after_waiting_for_readers_hook.lock() = hook;
    }

    #[doc(hidden)]
    pub fn set_before_deleting_postings_hook_for_test(
        &self,
        hook: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        *self.before_deleting_postings_hook.lock() = hook;
    }

    #[doc(hidden)]
    pub fn has_pending_tombstones_active_for_test(&self) -> bool {
        self.series.pending_tombstones.active.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn is_hash_pending_tombstone_for_test(&self, hash: u64) -> bool {
        self.series.pending_tombstones.is_pending(hash)
    }

    #[doc(hidden)]
    pub fn get_entry_by_id(&self, ref_id: u64) -> Option<Arc<SeriesEntry>> {
        self.series.get_by_id(ref_id)
    }

    #[doc(hidden)]
    pub fn mmapped_chunks_bounds_for_test(&self, ref_id: u64) -> Option<Vec<(u64, i64, i64)>> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        Some(
            st.mmapped()
                .iter()
                .map(|mc| (mc.ref_id.as_u64(), mc.min_time, mc.max_time))
                .collect(),
        )
    }

    #[doc(hidden)]
    pub fn head_chunks_bounds_for_test(&self, ref_id: u64) -> Option<Vec<(i64, i64)>> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        let mut out = Vec::with_capacity(st.head_chunks_len());
        st.for_each_head_chunk_bounds(|mn, mx| out.push((mn, mx)));
        Some(out)
    }

    #[doc(hidden)]
    pub fn merge_replay_mmapped_chunks_for_test(
        existing: Vec<(u64, i64, i64)>,
        incoming: Vec<(u64, i64, i64)>,
        last_mmap_ref: u64,
        ooo: bool,
    ) -> Vec<(u64, i64, i64)> {
        let to_mc = |v: Vec<(u64, i64, i64)>| -> Vec<MmappedChunk> {
            v.into_iter()
                .map(|(r, min_time, max_time)| MmappedChunk {
                    ref_id: ChunkDiskMapperRef(r),
                    min_time,
                    max_time,
                    num_samples: 0,
                    encoding: Encoding::Xor,
                })
                .collect()
        };
        Self::merge_replay_mmapped_chunks(
            to_mc(existing),
            to_mc(incoming),
            ChunkDiskMapperRef(last_mmap_ref),
            ooo,
        )
        .into_iter()
        .map(|mc| (mc.ref_id.as_u64(), mc.min_time, mc.max_time))
        .collect()
    }

    #[inline]
    fn symbol_arena_grew_past_gc_threshold(&self) -> bool {
        let sym_bytes = self.symbols.read().bytes_len() as u64;
        let baseline = self.symbol_arena_baseline_bytes.load(Ordering::Acquire);
        let threshold = self.symbol_gc_arena_growth_threshold.load(Ordering::Acquire);
        sym_bytes.saturating_sub(baseline) >= threshold
    }

    /// Reclaims dead interned strings from `Head.symbols` (`SymbolTable`) after series churn
    /// or whenever the symbol arena grew by at least 128 MiB since the last GC baseline.
    fn maybe_gc_symbols(&self) {
        let deleted = self.deleted_series_since_symbol_gc.load(Ordering::Acquire);
        let num_series = self.num_series();
        let (sym_len, sym_bytes) = {
            let syms = self.symbols.read();
            (syms.len(), syms.bytes_len() as u64)
        };
        let baseline_bytes = self.symbol_arena_baseline_bytes.load(Ordering::Acquire);
        let arena_threshold = self.symbol_gc_arena_growth_threshold.load(Ordering::Acquire);
        let arena_grew = sym_bytes.saturating_sub(baseline_bytes) >= arena_threshold;
        let should_check = arena_grew
            || if num_series == 0 {
                deleted > 0 && sym_len > 64
            } else {
                deleted >= (num_series / 2).max(1024)
            };
        if !should_check {
            return;
        }

        let _creation_guard = self.series_creation_mtx.lock();
        let mut syms = self.symbols.write();

        if self.num_series() == 0 && self.postings.is_all_empty() {
            self.postings.clear_and_shrink();
            *syms = SymbolTable::new();
            self.symbol_arena_baseline_bytes
                .store(syms.bytes_len() as u64, Ordering::Release);
            self.deleted_series_since_symbol_gc
                .store(0, Ordering::Release);
            return;
        }

        let old_len = syms.len();
        if old_len <= 1 {
            self.symbol_arena_baseline_bytes
                .store(syms.bytes_len() as u64, Ordering::Release);
            self.deleted_series_since_symbol_gc
                .store(0, Ordering::Release);
            return;
        }

        let mut used = vec![false; old_len];
        used[0] = true; // SymbolId::EMPTY
        self.postings.collect_live_symbols(&mut used);

        let has_meta = self.has_any_metadata.load(Ordering::Acquire);
        if has_meta {
            let mut entries = Vec::with_capacity(256);
            for stripe in &self.series.stripes {
                entries.clear();
                StripeSeries::collect_stripe_into(stripe, &mut entries);
                for e in entries.drain(..) {
                    let st = e.inner.lock();
                    if let Some(m) = st.metadata() {
                        if let Some(slot) = used.get_mut(m.unit.0 as usize) {
                            *slot = true;
                        }
                        if let Some(slot) = used.get_mut(m.help.0 as usize) {
                            *slot = true;
                        }
                    }
                }
            }
        }

        let old_bytes = syms.bytes_len();
        let mut live_count = 1usize;
        let mut live_bytes = 0usize;
        for (i, &is_used) in used.iter().enumerate().skip(1) {
            if is_used {
                live_count += 1;
                if let Some(s) = syms.try_resolve(SymbolId(i as u32)) {
                    live_bytes += s.len();
                }
            }
        }

        let min_reclaim_bytes = (arena_threshold as usize / 8).max(1024);
        let should_compact = live_count * 4 < old_len * 3
            || (old_len > 4096 && live_count + 1024 < old_len)
            || live_bytes * 4 < old_bytes * 3
            || old_bytes.saturating_sub(live_bytes) >= min_reclaim_bytes;
        if should_compact {
            let mut new_syms = SymbolTable::with_capacity(live_count, live_bytes);
            let mut remap = vec![SymbolId::EMPTY; old_len];
            for (i, &is_used) in used.iter().enumerate().skip(1) {
                if is_used {
                    if let Some(s) = syms.try_resolve(SymbolId(i as u32)) {
                        if let Ok(id) = new_syms.try_intern(s) {
                            remap[i] = id;
                        }
                    }
                }
            }

            self.postings.remap_symbols(&remap);
            if has_meta {
                let mut entries = Vec::with_capacity(256);
                for stripe in &self.series.stripes {
                    entries.clear();
                    StripeSeries::collect_stripe_into(stripe, &mut entries);
                    for e in entries.drain(..) {
                        let mut st = e.inner.lock();
                        st.remap_metadata(&remap);
                    }
                }
            }
            *syms = new_syms;
        }
        self.symbol_arena_baseline_bytes
            .store(syms.bytes_len() as u64, Ordering::Release);
        self.deleted_series_since_symbol_gc
            .store(0, Ordering::Release);
    }

    fn intern_metadata_symbols(
        &self,
        metric_type: MetricType,
        unit: &str,
        help: &str,
    ) -> Result<CompactSeriesMetadata, HeadError> {
        {
            let syms = self.symbols.read();
            if let (Some(u), Some(h)) = (syms.get(unit), syms.get(help)) {
                return Ok(CompactSeriesMetadata {
                    metric_type,
                    unit: u,
                    help: h,
                });
            }
        }
        let mut syms = self.symbols.write();
        Ok(CompactSeriesMetadata {
            metric_type,
            unit: syms.try_intern(unit)?,
            help: syms.try_intern(help)?,
        })
    }

    /// Port of Go `mergeReplayMmappedChunks` (`head_wal.go:680-700`): merges persisted
    /// chunks without modifying replay inventories. In-order chunks are sorted by time;
    /// OOO chunks by disk reference.
    fn merge_replay_mmapped_chunks(
        existing: Vec<MmappedChunk>,
        mut incoming: Vec<MmappedChunk>,
        last_mmap_ref: ChunkDiskMapperRef,
        ooo: bool,
    ) -> Vec<MmappedChunk> {
        if ooo && incoming.len() > 1 {
            incoming.sort_by_key(|c| c.ref_id);
        } else if !ooo && incoming.len() > 1 {
            incoming.sort_by_key(|c| (c.min_time, c.max_time));
        }
        if existing.is_empty() {
            return incoming;
        }
        let mut merged = Vec::with_capacity(existing.len() + incoming.len());
        let mut j = 0usize;
        for c in existing {
            if c.ref_id > last_mmap_ref {
                continue;
            }
            while j < incoming.len()
                && ((ooo && incoming[j].ref_id < c.ref_id)
                    || (!ooo && incoming[j].max_time < c.min_time))
            {
                merged.push(incoming[j]);
                j += 1;
            }
            if j < incoming.len()
                && (c.ref_id == incoming[j].ref_id
                    || (!ooo && c.max_time >= incoming[j].min_time))
            {
                // Deduplicate disk references; for overlapping in-order chunks, prefer the
                // later WAL definition.
                continue;
            }
            merged.push(c);
        }
        merged.extend_from_slice(&incoming[j..]);
        merged
    }

    #[allow(clippy::too_many_arguments)]
    fn attach_mmapped_chunks(
        &self,
        entry: &SeriesEntry,
        mmc: Vec<MmappedChunk>,
        ooo_mmc: Vec<MmappedChunk>,
        is_duplicate: bool,
        last_mmap_ref: ChunkDiskMapperRef,
        mint: &mut i64,
        maxt: &mut i64,
        ooo_mint: &mut i64,
        ooo_maxt: &mut i64,
    ) {
        if !is_duplicate && mmc.is_empty() && ooo_mmc.is_empty() {
            return;
        }
        let mut st = entry.inner.lock();
        let old_mmc = std::mem::take(&mut st.mmapped_chunks);
        let old_ooo_mmc = st
            .ooo_mut()
            .map(|o| std::mem::take(&mut o.ooo_mmapped_chunks))
            .unwrap_or_default();
        let old_mmc_len = old_mmc.len();
        let old_ooo_len = old_ooo_mmc.len();
        let old_head_len = if is_duplicate { st.head_chunks_len() } else { 0 };

        let merged_mmc =
            Self::merge_replay_mmapped_chunks(old_mmc, mmc, last_mmap_ref, false);
        let merged_ooo =
            Self::merge_replay_mmapped_chunks(old_ooo_mmc, ooo_mmc, last_mmap_ref, true);

        self.metrics
            .chunks_created((merged_mmc.len() + merged_ooo.len()) as u64);
        let removed_total = old_mmc_len + old_ooo_len + old_head_len;
        if removed_total > 0 {
            self.metrics.chunks_removed(removed_total as u64);
        }

        if !merged_mmc.is_empty() {
            *mint = (*mint).min(merged_mmc[0].min_time);
            *maxt = (*maxt).max(merged_mmc[merged_mmc.len() - 1].max_time);
            let mut merged_mmc = merged_mmc;
            shrink_mmapped_vec(&mut merged_mmc);
            st.mmapped_chunks = merged_mmc;
        }

        if merged_ooo.is_empty() {
            st.clear_ooo_if_empty();
        } else {
            for m in &merged_ooo {
                *ooo_mint = (*ooo_mint).min(m.min_time);
                *ooo_maxt = (*ooo_maxt).max(m.max_time);
            }
            let mut merged_ooo = merged_ooo;
            shrink_mmapped_vec(&mut merged_ooo);
            st.ooo_or_insert().ooo_mmapped_chunks = merged_ooo;
        }

        if is_duplicate {
            // Port of Go `resetSeriesWithMMappedChunks` (`head_wal.go:664-672`):
            // any un-mmapped samples replayed under an earlier WAL ref belong to the
            // obsolete generation prior to the duplicate Series record's mmappedChunks.
            let (was_stale, was_hist, old_buckets) = st.sample_state();
            if was_stale {
                self.update_stale_series_metric_on_append(true, false);
            }
            if was_hist {
                self.update_native_histogram_metrics_on_append(true, false, old_buckets, 0);
            }
            st.clear_head_chunks();
            st.set_next_at(0);
            st.invalidate_xor_state();
            st.invalidate_xor2_state();
            st.clear_hist_state();
            st.clear_last_sample();
        }

        entry
            .in_order_max_time
            .store(st.max_time(), Ordering::Release);
    }

    fn replay_one_sample(
        &self,
        entry: &SeriesEntry,
        mut s: RefSample,
        mint: &mut i64,
        maxt: &mut i64,
    ) -> Result<(), HeadError> {
        if s.v.to_bits() == QUIET_ZERO_NAN_BITS {
            s.v = 0.0;
        }
        // Pass s.st verbatim during WAL replay (matching Go head_wal.go:817-957;
        // Go only applies zeroOrST on live appenders, never during WAL/WBL replay).
        let st = s.st;
        let mut state = entry.inner.lock();
        if s.t <= state.mm_max_time() {
            return Ok(());
        }
        let (was_stale, was_hist, old_buckets) = state.sample_state();
        if is_stale_nan(s.v) {
            match state.latest_in_order_encoding() {
                Some(Encoding::Histogram | Encoding::HistogramSt) => {
                    let h = Histogram {
                        sum: s.v,
                        ..Histogram::default()
                    };
                    if self.metrics.track_append(state.append_histogram_in_order(
                            st,
                            s.t,
                            &h,
                            self.opts.chunk_range_ms,
                            self.opts.enable_histogram_st_encoding,
                        )?) {
                        *mint = (*mint).min(s.t);
                        *maxt = (*maxt).max(s.t);
                        self.update_stale_series_metric_on_append(was_stale, true);
                        self.update_native_histogram_metrics_on_append(was_hist, true, old_buckets, 0);
                    }
                    entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                    return Ok(());
                }
                Some(Encoding::FloatHistogram | Encoding::FloatHistogramSt) => {
                    let fh = FloatHistogram {
                        sum: s.v,
                        ..FloatHistogram::default()
                    };
                    if self.metrics.track_append(state.append_float_histogram_in_order(
                            st,
                            s.t,
                            &fh,
                            self.opts.chunk_range_ms,
                            self.opts.enable_histogram_st_encoding,
                        )?) {
                        *mint = (*mint).min(s.t);
                        *maxt = (*maxt).max(s.t);
                        self.update_stale_series_metric_on_append(was_stale, true);
                        self.update_native_histogram_metrics_on_append(was_hist, true, old_buckets, 0);
                    }
                    entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                    return Ok(());
                }
                _ => {}
            }
        }
        if self.metrics.track_append(state.append_float_in_order(
                &entry.labels,
                st,
                s.t,
                s.v,
                self.opts.chunk_range_ms,
                self.opts.samples_per_chunk,
                self.float_chunk_encoding(),
                self.opts.enable_st_storage,
                self.chunk_end_time_variance(),
            )?) {
            *mint = (*mint).min(s.t);
            *maxt = (*maxt).max(s.t);
            self.update_stale_series_metric_on_append(was_stale, is_stale_nan(s.v));
            if was_hist {
                self.update_native_histogram_metrics_on_append(true, false, old_buckets, 0);
            }
        }
        entry
            .in_order_max_time
            .store(state.max_time(), Ordering::Release);
        Ok(())
    }

    fn replay_one_histogram(
        &self,
        entry: &SeriesEntry,
        hs: RefHistogramSample,
        mint: &mut i64,
        maxt: &mut i64,
    ) -> Result<(), HeadError> {
        let st = hs.st;
        let mut state = entry.inner.lock();
        if hs.t <= state.mm_max_time() {
            return Ok(());
        }
        let (was_stale, was_hist, old_buckets) = state.sample_state();
        if self.metrics.track_append(state.append_histogram_in_order(
                st,
                hs.t,
                &hs.h,
                self.opts.chunk_range_ms,
                self.opts.enable_histogram_st_encoding,
            )?) {
            *mint = (*mint).min(hs.t);
            *maxt = (*maxt).max(hs.t);
            let new_buckets = hs.h.positive_buckets.len() + hs.h.negative_buckets.len();
            self.update_stale_series_metric_on_append(was_stale, is_stale_nan(hs.h.sum));
            self.update_native_histogram_metrics_on_append(
                was_hist,
                true,
                old_buckets,
                new_buckets,
            );
        }
        entry
            .in_order_max_time
            .store(state.max_time(), Ordering::Release);
        Ok(())
    }

    fn replay_one_float_histogram(
        &self,
        entry: &SeriesEntry,
        fhs: RefFloatHistogramSample,
        mint: &mut i64,
        maxt: &mut i64,
    ) -> Result<(), HeadError> {
        let st = fhs.st;
        let mut state = entry.inner.lock();
        if fhs.t <= state.mm_max_time() {
            return Ok(());
        }
        let (was_stale, was_hist, old_buckets) = state.sample_state();
        if self.metrics.track_append(state.append_float_histogram_in_order(
                st,
                fhs.t,
                &fhs.fh,
                self.opts.chunk_range_ms,
                self.opts.enable_histogram_st_encoding,
            )?) {
            *mint = (*mint).min(fhs.t);
            *maxt = (*maxt).max(fhs.t);
            let new_buckets = fhs.fh.positive_buckets.len() + fhs.fh.negative_buckets.len();
            self.update_stale_series_metric_on_append(was_stale, is_stale_nan(fhs.fh.sum));
            self.update_native_histogram_metrics_on_append(
                was_hist,
                true,
                old_buckets,
                new_buckets,
            );
        }
        entry
            .in_order_max_time
            .store(state.max_time(), Ordering::Release);
        Ok(())
    }

    fn replay_wal_reader<R: io::Read + SegmentTracker + Send>(
        &self,
        r: &mut WlogReader<R>,
        multi_ref: &mut HashMap<u64, u64>,
        global_missing_series_refs: &mut HashSet<u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        last_mmap_ref: ChunkDiskMapperRef,
    ) -> Result<(), HeadError> {
        let unknown = UnknownRefs::default();
        if self.opts.wal_replay_concurrency > 1 {
            self.replay_wal_reader_parallel(
                r,
                multi_ref,
                mmapped_map,
                ooo_mmapped_map,
                last_mmap_ref,
                &unknown,
            )?;
        } else {
            self.replay_wal_reader_sequential(
                r,
                multi_ref,
                mmapped_map,
                ooo_mmapped_map,
                last_mmap_ref,
                &unknown,
            )?;
        }
        self.report_wal_unknown_refs(&unknown, multi_ref, global_missing_series_refs);
        Ok(())
    }

    fn replay_wal_reader_sequential<R: io::Read + SegmentTracker + Send>(
        &self,
        r: &mut WlogReader<R>,
        multi_ref: &mut HashMap<u64, u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        last_mmap_ref: ChunkDiskMapperRef,
        unknown: &UnknownRefs,
    ) -> Result<(), HeadError> {

        const WAL_POSTINGS_BATCH: usize = 4096;

        let dec = RecordDecoder::new();
        let min_valid_time = self.min_valid_time.load(Ordering::Relaxed);
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;
        let mut ooo_mint = i64::MAX;
        let mut ooo_maxt = i64::MIN;
        let mut pending_postings: Vec<(u64, SymbolLabels)> = Vec::with_capacity(WAL_POSTINGS_BATCH);
        let mut postings_scratch = PostingsBatchScratch::new();
        let mut replay_err: Option<HeadError> = None;

        loop {
            let rec = match r.next_record() {
                Ok(Some(rec)) => rec,
                Ok(None) => break,
                Err(e) => {
                    replay_err = Some(HeadError::from(e));
                    break;
                }
            };
            let decoded = match dec.decode_record(rec) {
                Ok(d) => d,
                Err(e) => {
                    // Go `loadWAL`: decode errors are `wlog.CorruptionErr{Segment, Offset}`.
                    replay_err = Some(record_decode_corruption(r, &e));
                    break;
                }
            };
            match decoded {
                DecodedRecord::Series(series_list) => {
                    self.replay_wal_series_batch(
                        &series_list,
                        multi_ref,
                        mmapped_map,
                        ooo_mmapped_map,
                        &mut pending_postings,
                        |entry, mmc, ooo_mmc, is_duplicate| {
                            self.attach_mmapped_chunks(
                                &entry,
                                mmc,
                                ooo_mmc,
                                is_duplicate,
                                last_mmap_ref,
                                &mut mint,
                                &mut maxt,
                                &mut ooo_mint,
                                &mut ooo_maxt,
                            );
                            Ok(())
                        },
                    )?;
                    if pending_postings.len() >= WAL_POSTINGS_BATCH {
                        self.postings
                            .add_batch_unordered_with_scratch(&pending_postings, &mut postings_scratch);
                        pending_postings.clear();
                    }
                }
                DecodedRecord::Samples(samples) => {
                    for mut s in samples {
                        if s.t < min_valid_time {
                            continue;
                        }
                        if let Some(&mapped) = multi_ref.get(&s.ref_id) {
                            self.update_wal_expiry(s.ref_id, s.t);
                            s.ref_id = mapped;
                        }
                        let Some(entry) = self.series.get_by_id(s.ref_id) else {
                            unknown.miss(&unknown.samples, s.ref_id);
                            continue;
                        };
                        self.replay_one_sample(&entry, s, &mut mint, &mut maxt)?;
                    }
                }
                DecodedRecord::HistogramSamples(hists) => {
                    for mut hs in hists {
                        if hs.t < min_valid_time {
                            continue;
                        }
                        if let Some(&mapped) = multi_ref.get(&hs.ref_id) {
                            self.update_wal_expiry(hs.ref_id, hs.t);
                            hs.ref_id = mapped;
                        }
                        let Some(entry) = self.series.get_by_id(hs.ref_id) else {
                            unknown.miss(&unknown.histograms, hs.ref_id);
                            continue;
                        };
                        self.replay_one_histogram(&entry, hs, &mut mint, &mut maxt)?;
                    }
                }
                DecodedRecord::FloatHistogramSamples(fhists) => {
                    for mut fhs in fhists {
                        if fhs.t < min_valid_time {
                            continue;
                        }
                        if let Some(&mapped) = multi_ref.get(&fhs.ref_id) {
                            self.update_wal_expiry(fhs.ref_id, fhs.t);
                            fhs.ref_id = mapped;
                        }
                        let Some(entry) = self.series.get_by_id(fhs.ref_id) else {
                            unknown.miss(&unknown.histograms, fhs.ref_id);
                            continue;
                        };
                        self.replay_one_float_histogram(&entry, fhs, &mut mint, &mut maxt)?;
                    }
                }
                DecodedRecord::Tombstones(stones) => {
                    if !pending_postings.is_empty() {
                        self.postings
                            .add_batch_unordered_with_scratch(&pending_postings, &mut postings_scratch);
                        pending_postings.clear();
                    }
                    let mut to_delete = Vec::new();
                    let mut seen_delete_refs = HashSet::new();
                    for mut stone in stones {
                        self.bump_last_series_id(stone.ref_id);
                        if stone.mint == i64::MIN && stone.maxt == i64::MAX {
                            let mut ref_id = stone.ref_id;
                            if let Some(&mapped) = multi_ref.get(&ref_id) {
                                ref_id = mapped;
                            }
                            if seen_delete_refs.insert(ref_id) {
                                if let Some(entry) = self.series.get_by_id(ref_id) {
                                    self.series.unlink_hash(entry.hash, ref_id);
                                    to_delete.push(entry);
                                }
                            }
                            continue;
                        }
                        if seen_delete_refs.contains(&stone.ref_id) {
                            unknown.miss(&unknown.tombstones, stone.ref_id);
                            continue;
                        }
                        if stone.maxt < min_valid_time {
                            continue;
                        }
                        if let Some(&mapped) = multi_ref.get(&stone.ref_id) {
                            self.update_wal_expiry(stone.ref_id, stone.maxt);
                            stone.ref_id = mapped;
                        }
                        if self.series.get_by_id(stone.ref_id).is_some() {
                            self.tombstones
                                .add_interval(stone.ref_id, Interval::new(stone.mint, stone.maxt));
                        } else {
                            unknown.miss(&unknown.tombstones, stone.ref_id);
                        }
                    }
                    if !to_delete.is_empty() {
                        self.delete_series_entries(&to_delete);
                    }
                }
                DecodedRecord::Exemplars(exemplars) => {
                    for mut ex in exemplars {
                        if ex.t < min_valid_time {
                            continue;
                        }
                        if let Some(&mapped) = multi_ref.get(&ex.ref_id) {
                            self.update_wal_expiry(ex.ref_id, ex.t);
                            ex.ref_id = mapped;
                        }
                        let Some(entry) = self.series.get_by_id(ex.ref_id) else {
                            unknown.miss(&unknown.exemplars, ex.ref_id);
                            continue;
                        };
                        if self
                            .exemplars
                            .add_exemplar(&entry.labels, Exemplar::new(ex.labels, ex.v, ex.t))
                            .is_ok()
                        {
                            let mut st = entry.inner.lock();
                            st.update_last_exemplar_ts(ex.t);
                        }
                    }
                }
                DecodedRecord::Metadata(metas) => {
                    for mut m in metas {
                        if let Some(&mapped) = multi_ref.get(&m.ref_id) {
                            m.ref_id = mapped;
                        }
                        let Some(entry) = self.series.get_by_id(m.ref_id) else {
                            unknown.miss(&unknown.metadata, m.ref_id);
                            continue;
                        };
                        let metric_type = MetricType::from_u8(m.metric_type);
                        let compact_meta =
                            self.intern_metadata_symbols(metric_type, &m.unit, &m.help)?;
                        self.has_any_metadata.store(true, Ordering::Release);
                        let mut st = entry.inner.lock();
                        st.set_metadata(compact_meta);
                    }
                }
                // MinValidTime is read strictly from the latest checkpoint's first record
                // before replay; mid-WAL MinValidTime records are ignored (Go head_wal.go:251-254).
                DecodedRecord::MinValidTime(_)
                | DecodedRecord::MmapMarkers(_)
                | DecodedRecord::Unknown(_) => {}
            }
        }
        if !pending_postings.is_empty() {
            self.postings
                .add_batch_unordered_with_scratch(&pending_postings, &mut postings_scratch);
            pending_postings.clear();
        }
        if mint <= maxt {
            self.update_min_max_time(mint, maxt);
        }
        if ooo_mint <= ooo_maxt {
            self.update_min_ooo_max_ooo_time(ooo_mint, ooo_maxt);
        }
        match replay_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn replay_wal_reader_parallel<R: io::Read + SegmentTracker + Send>(
        &self,
        r: &mut WlogReader<R>,
        multi_ref: &mut HashMap<u64, u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        last_mmap_ref: ChunkDiskMapperRef,
        unknown: &UnknownRefs,
    ) -> Result<(), HeadError> {
        enum WalSampleJob {
            Float(RefSample),
            Histogram(Box<RefHistogramSample>),
            FloatHistogram(Box<RefFloatHistogramSample>),
        }

        struct WalBarrier {
            state: Mutex<(usize, bool)>,
            cv: Condvar,
        }
        impl WalBarrier {
            fn new(count: usize) -> Arc<Self> {
                Arc::new(Self { state: Mutex::new((count, false)), cv: Condvar::new() })
            }
            fn arrive(&self) {
                let mut g = self.state.lock();
                if g.0 > 0 { g.0 -= 1; }
                if g.0 == 0 { self.cv.notify_all(); }
            }
            fn abort(&self) {
                let mut g = self.state.lock();
                g.1 = true;
                self.cv.notify_all();
            }
            fn wait(&self) -> bool {
                let mut g = self.state.lock();
                while g.0 > 0 && !g.1 {
                    self.cv.wait(&mut g);
                }
                !g.1
            }
        }

        enum WalWorkerMsg {
            AttachMmapped {
                entry: Arc<SeriesEntry>,
                mmc: Vec<MmappedChunk>,
                ooo_mmc: Vec<MmappedChunk>,
                is_duplicate: bool,
            },
            SampleJobs(Vec<WalSampleJob>),
            Barrier(Arc<WalBarrier>),
            AddTombstone {
                ref_id: u64,
                interval: Interval,
            },
            DeleteSeries(Vec<u64>),
        }

        enum ParallelDecodedRecord {
            Series(Vec<RefSeries>, Vec<u64>),
            Other(DecodedRecord),
        }

        const WAL_SHARD_BATCH_MAX: usize = 2048;
        const WAL_DECODE_BATCH_RECORDS: usize = 64;
        const WAL_DECODE_BATCH_BYTES: usize = 512 * 1024;
        const WAL_SERIES_PARALLEL_BATCH_MAX: usize = 16_384;

        let max_threads = std::thread::available_parallelism()
            .map_or(8, |n| n.get())
            .clamp(2, 32);
        let workers = self.opts.wal_replay_concurrency.min(max_threads).max(2);
        let dec_workers = workers.min(8);
        let min_valid_time = self.min_valid_time.load(Ordering::Relaxed);
        let series_pool = mimir_common::thread_pool::ScopedWorkerPool::new("wal-series", workers);

        type RawEntry = (CompressionType, Vec<u8>, Option<PathBuf>, Option<usize>, usize);
        type RawBatch = (u64, Vec<RawEntry>);
        // A batch carries the records decoded before a decode/decompress error (if any),
        // so that the coordinator applies them in WAL order and stops exactly at the
        // corrupt record, like Go `loadWAL`.
        type DecodedBatch = (u64, Vec<ParallelDecodedRecord>, Option<HeadError>);

        // Channel capacity > dec_workers * dec_queue_cap ensures zero deadlock when reordering.
        let (decoded_tx, decoded_rx) =
            std::sync::mpsc::sync_channel::<Result<DecodedBatch, HeadError>>(dec_workers * 2 + 1);
        let mut dec_txs = Vec::with_capacity(dec_workers);
        let mut dec_rxs = Vec::with_capacity(dec_workers);
        for _ in 0..dec_workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<RawBatch>(1);
            dec_txs.push(tx);
            dec_rxs.push(rx);
        }

        let mut worker_txs = Vec::with_capacity(workers);
        let mut worker_rxs = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<WalWorkerMsg>(16);
            worker_txs.push(tx);
            worker_rxs.push(rx);
        }

        std::thread::scope(|scope| {
            // Stage 1: Sequential WAL framing + CRC32C reader feeding compressed batches round-robin.
            let reader_err_tx = decoded_tx.clone();
            let reader_handle = scope.spawn(move || {
                let mut batch_id = 0u64;
                let mut batch: Vec<RawEntry> = Vec::with_capacity(WAL_DECODE_BATCH_RECORDS);
                let mut batch_bytes = 0usize;
                loop {
                    let cur_dir = r.segment_dir().map(Path::to_path_buf);
                    match r.next_raw_record() {
                        Ok(Some((compr, raw))) => {
                            let raw_vec = raw.to_vec();
                            let post_dir = r.segment_dir().map(Path::to_path_buf).or(cur_dir);
                            let post_seg = r.segment();
                            let post_off = r.offset();
                            batch_bytes += raw_vec.len();
                            batch.push((compr, raw_vec, post_dir, post_seg, post_off));
                            if batch.len() >= WAL_DECODE_BATCH_RECORDS
                                || batch_bytes >= WAL_DECODE_BATCH_BYTES
                            {
                                let send_batch = std::mem::replace(
                                    &mut batch,
                                    Vec::with_capacity(WAL_DECODE_BATCH_RECORDS),
                                );
                                batch_bytes = 0;
                                let target = (batch_id as usize) % dec_workers;
                                if dec_txs[target].send((batch_id, send_batch)).is_err() {
                                    break;
                                }
                                batch_id += 1;
                            }
                        }
                        Ok(None) => {
                            if !batch.is_empty() {
                                let target = (batch_id as usize) % dec_workers;
                                let _ = dec_txs[target].send((batch_id, batch));
                            }
                            break;
                        }
                        Err(e) => {
                            if !batch.is_empty() {
                                let target = (batch_id as usize) % dec_workers;
                                let _ = dec_txs[target].send((batch_id, batch));
                            }
                            let _ = reader_err_tx.send(Err(HeadError::from(e)));
                            break;
                        }
                    }
                }
                drop(dec_txs);
                drop(reader_err_tx);
            });

            // Stage 1.5: Parallel Snappy/Zstd decompression + RecordDecoder workers.
            let mut dec_handles = Vec::with_capacity(dec_workers);
            for rx in dec_rxs {
                let d_tx = decoded_tx.clone();
                dec_handles.push(scope.spawn(move || {
                    let dec = RecordDecoder::new();
                    let mut decomp_buf = Vec::with_capacity(64 * 1024);
                    while let Ok((batch_id, raw_batch)) = rx.recv() {
                        let mut decoded_batch = Vec::with_capacity(raw_batch.len());
                        let mut err: Option<HeadError> = None;
                        for (compr, raw, dir, seg, off) in raw_batch {
                            if let Err(msg) = decompress_record_into(compr, &raw, &mut decomp_buf) {
                                err = Some(HeadError::Wlog(WlogError::Corruption {
                                    dir,
                                    segment: seg,
                                    offset: off,
                                    message: msg,
                                }));
                                break;
                            }
                            match dec.decode_record(&decomp_buf) {
                                Ok(DecodedRecord::Series(series_list)) => {
                                    let hashes: Vec<u64> =
                                        series_list.iter().map(|rs| rs.labels.hash()).collect();
                                    decoded_batch
                                        .push(ParallelDecodedRecord::Series(series_list, hashes));
                                }
                                Ok(rec) => decoded_batch.push(ParallelDecodedRecord::Other(rec)),
                                Err(e) => {
                                    // Go `loadWAL`: decode errors are `wlog.CorruptionErr`.
                                    err = Some(HeadError::Wlog(WlogError::Corruption {
                                        dir,
                                        segment: seg,
                                        offset: off,
                                        message: format!("decode record: {e}"),
                                    }));
                                    break;
                                }
                            }
                        }
                        let failed = err.is_some();
                        if d_tx.send(Ok((batch_id, decoded_batch, err))).is_err() || failed {
                            break;
                        }
                    }
                }));
            }
            drop(decoded_tx);

            // Stage 3: Sharded WAL replay worker threads (`resolved_ref_id % workers`).
            let mut worker_handles = Vec::with_capacity(workers);
            for rx in worker_rxs {
                let handle = scope.spawn(move || -> Result<(), HeadError> {
                    let mut mint = i64::MAX;
                    let mut maxt = i64::MIN;
                    let mut ooo_mint = i64::MAX;
                    let mut ooo_maxt = i64::MIN;

                    while let Ok(msg) = rx.recv() {
                        match msg {
                            WalWorkerMsg::AttachMmapped {
                                entry,
                                mmc,
                                ooo_mmc,
                                is_duplicate,
                            } => {
                                self.attach_mmapped_chunks(
                                    &entry,
                                    mmc,
                                    ooo_mmc,
                                    is_duplicate,
                                    last_mmap_ref,
                                    &mut mint,
                                    &mut maxt,
                                    &mut ooo_mint,
                                    &mut ooo_maxt,
                                );
                            }
                            WalWorkerMsg::SampleJobs(jobs) => {
                                for job in jobs {
                                    match job {
                                        WalSampleJob::Float(s) => {
                                            if let Some(entry) = self.series.get_by_id(s.ref_id) {
                                                self.replay_one_sample(
                                                    &entry, s, &mut mint, &mut maxt,
                                                )?;
                                            } else {
                                                unknown.miss(&unknown.samples, s.ref_id);
                                            }
                                        }
                                        WalSampleJob::Histogram(hs) => {
                                            if let Some(entry) = self.series.get_by_id(hs.ref_id) {
                                                self.replay_one_histogram(
                                                    &entry, *hs, &mut mint, &mut maxt,
                                                )?;
                                            } else {
                                                unknown.miss(&unknown.histograms, hs.ref_id);
                                            }
                                        }
                                        WalSampleJob::FloatHistogram(fhs) => {
                                            if let Some(entry) = self.series.get_by_id(fhs.ref_id) {
                                                self.replay_one_float_histogram(
                                                    &entry, *fhs, &mut mint, &mut maxt,
                                                )?;
                                            } else {
                                                unknown.miss(&unknown.histograms, fhs.ref_id);
                                            }
                                        }
                                    }
                                }
                            }
                            WalWorkerMsg::AddTombstone { ref_id, interval } => {
                                if self.series.get_by_id(ref_id).is_some() {
                                    self.tombstones.add_interval(ref_id, interval);
                                } else {
                                    unknown.miss(&unknown.tombstones, ref_id);
                                }
                            }
                            WalWorkerMsg::Barrier(b) => {
                                b.arrive();
                            }
                            WalWorkerMsg::DeleteSeries(refs) => {
                                let mut entries = Vec::with_capacity(refs.len());
                                for ref_id in refs {
                                    if let Some(entry) = self.series.get_by_id(ref_id) {
                                        entries.push(entry);
                                    }
                                }
                                if !entries.is_empty() {
                                    self.delete_series_entries(&entries);
                                }
                            }
                        }
                    }
                    // Drain any trailing Barrier messages if exiting early.
                    while let Ok(msg) = rx.try_recv() {
                        if let WalWorkerMsg::Barrier(b) = msg {
                            b.abort();
                        }
                    }

                    if mint <= maxt {
                        self.update_min_max_time(mint, maxt);
                    }
                    if ooo_mint <= ooo_maxt {
                        self.update_min_ooo_max_ooo_time(ooo_mint, ooo_maxt);
                    }
                    Ok(())
                });
                worker_handles.push(handle);
            }

            // Stage 2: Coordinator processes decoded batches in exact WAL order (`batch_id`),
            // resolves Series in parallel (sharded by hash % workers), and shards samples.
            let mut coord_res: Result<(), HeadError> = Ok(());
            let mut job_shards: Vec<Vec<WalSampleJob>> = (0..workers).map(|_| Vec::new()).collect();
            let mut samples_queued = false;
            let mut deleted_in_replay: HashSet<u64> = HashSet::new();
            let mut pending_series: Vec<(RefSeries, u64)> = Vec::new();
            let mut reorder: BTreeMap<u64, (Vec<ParallelDecodedRecord>, Option<HeadError>)> = BTreeMap::new();
            let mut next_batch_id = 0u64;

            let flush_series_batch = |pending: &mut Vec<(RefSeries, u64)>,
                                      multi_ref: &mut HashMap<u64, u64>,
                                      mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
                                      ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
                                      job_shards: &mut [Vec<WalSampleJob>],
                                      samples_queued: &mut bool,
                                      failed_outer: &mut bool|
             -> Result<(), HeadError> {
                if pending.is_empty() {
                    return Ok(());
                }
                if *samples_queued {
                    let barrier = WalBarrier::new(workers);
                    for idx in 0..workers {
                        if !job_shards[idx].is_empty() {
                            let batch = std::mem::take(&mut job_shards[idx]);
                            if worker_txs[idx].send(WalWorkerMsg::SampleJobs(batch)).is_err() {
                                *failed_outer = true;
                                return Err(HeadError::Closed);
                            }
                        }
                        if worker_txs[idx]
                            .send(WalWorkerMsg::Barrier(Arc::clone(&barrier)))
                            .is_err()
                        {
                            *failed_outer = true;
                            return Err(HeadError::Closed);
                        }
                    }
                    if !barrier.wait() {
                        *failed_outer = true;
                        return Err(HeadError::Closed);
                    }
                    *samples_queued = false;
                }
                let batch = std::mem::take(pending);
                let mut max_ref_id = 0u64;
                let has_mm = !mmapped_map.is_empty();
                let has_ooo_mm = !ooo_mmapped_map.is_empty();
                type SeriesShardItem = (RefSeries, u64, Vec<MmappedChunk>, Vec<MmappedChunk>);
                let mut shards: Vec<Vec<SeriesShardItem>> =
                    (0..workers).map(|_| Vec::new()).collect();
                for (rs, hash) in batch {
                    max_ref_id = max_ref_id.max(rs.ref_id);
                    let mmc = if has_mm {
                        mmapped_map.remove(&rs.ref_id).unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    let ooo_mmc = if has_ooo_mm {
                        ooo_mmapped_map.remove(&rs.ref_id).unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    let w = (hash as usize) % workers;
                    shards[w].push((rs, hash, mmc, ooo_mmc));
                }

                let mut out_multi_ref: Vec<Vec<(u64, u64)>> =
                    (0..workers).map(|_| Vec::new()).collect();
                let mut out_postings: Vec<Vec<(u64, SymbolLabels)>> =
                    (0..workers).map(|_| Vec::new()).collect();
                type AttachItem = (Arc<SeriesEntry>, Vec<MmappedChunk>, Vec<MmappedChunk>, bool);
                let mut out_attach: Vec<Vec<AttachItem>> =
                    (0..workers).map(|_| Vec::new()).collect();
                let mut out_created: Vec<u64> = vec![0u64; workers];

                struct MutPtr<T>(*mut T);
                impl<T> MutPtr<T> {
                    #[inline]
                    #[allow(clippy::mut_from_ref)]
                    unsafe fn get(&self, idx: usize) -> &mut T {
                        unsafe { &mut *self.0.add(idx) }
                    }
                }
                unsafe impl<T: Send> Send for MutPtr<T> {}
                unsafe impl<T: Send> Sync for MutPtr<T> {}

                let shards_ptr = MutPtr(shards.as_mut_ptr());
                let multi_ptr = MutPtr(out_multi_ref.as_mut_ptr());
                let postings_ptr = MutPtr(out_postings.as_mut_ptr());
                let attach_ptr = MutPtr(out_attach.as_mut_ptr());
                let created_ptr = MutPtr(out_created.as_mut_ptr());
                let mut out_err: Vec<Option<HeadError>> = (0..workers).map(|_| None).collect();
                let err_ptr = MutPtr(out_err.as_mut_ptr());
                let cb_opt = self.lifecycle_callback.read().clone();

                series_pool.scoped_execute(workers, |w| {
                    // SAFETY: each `w` in `0..workers` owns disjoint index `w`.
                    let shard = unsafe { std::mem::take(shards_ptr.get(w)) };
                    if shard.is_empty() {
                        return;
                    }
                    let w_multi = unsafe { multi_ptr.get(w) };
                    let w_postings = unsafe { postings_ptr.get(w) };
                    let w_attach = unsafe { attach_ptr.get(w) };
                    let w_created = unsafe { created_ptr.get(w) };
                    let w_err = unsafe { err_ptr.get(w) };

                    for (rs, hash, mmc, ooo_mmc) in shard {
                        if let Some(existing) =
                            self.series.get_by_hash_and_labels(hash, &rs.labels)
                        {
                            w_multi.push((rs.ref_id, existing.ref_id));
                            w_attach.push((existing, mmc, ooo_mmc, true));
                            continue;
                        }
                        let symbol_labels = match self.intern_labels_read_first(&rs.labels) {
                            Ok(sl) => sl,
                            Err(e) => {
                                *w_err = Some(e);
                                return;
                            }
                        };
                        let entry = Arc::new(SeriesEntry::new(rs.ref_id, hash, rs.labels, 0));
                        self.series.insert(Arc::clone(&entry));
                        w_postings.push((rs.ref_id, symbol_labels));
                        *w_created += 1;
                        if let Some(ref cb) = cb_opt {
                            cb.post_creation(rs.ref_id, &entry.labels);
                        }
                        if !mmc.is_empty() || !ooo_mmc.is_empty() {
                            w_attach.push((entry, mmc, ooo_mmc, false));
                        }
                    }
                });

                if let Some(err) = out_err.into_iter().flatten().next() {
                    return Err(err);
                }

                for mvec in out_multi_ref {
                    for (k, v) in mvec {
                        multi_ref.insert(k, v);
                    }
                }
                let created_count: u64 = out_created.into_iter().sum();
                if created_count > 0 {
                    self.num_series.fetch_add(created_count, Ordering::Relaxed);
                    if let Some(ref g) = self.series_gauge {
                        g.add(created_count as f64);
                    }
                    if let Some(ref c) = self.series_created_total {
                        c.inc_by(created_count as f64);
                    }
                }
                if max_ref_id > 0 {
                    self.bump_last_series_id(max_ref_id);
                }
                self.postings
                    .add_batches_unordered_parallel(&out_postings, &series_pool);

                for avec in out_attach {
                    for (entry, mmc, ooo_mmc, is_duplicate) in avec {
                        let idx = (entry.ref_id as usize) % workers;
                        if !job_shards[idx].is_empty() {
                            let batch = std::mem::take(&mut job_shards[idx]);
                            if worker_txs[idx]
                                .send(WalWorkerMsg::SampleJobs(batch))
                                .is_err()
                            {
                                *failed_outer = true;
                                return Err(HeadError::Closed);
                            }
                        }
                        if worker_txs[idx]
                            .send(WalWorkerMsg::AttachMmapped {
                                entry,
                                mmc,
                                ooo_mmc,
                                is_duplicate,
                            })
                            .is_err()
                        {
                            *failed_outer = true;
                            return Err(HeadError::Closed);
                        }
                    }
                }
                Ok(())
            };

            let mut failed_outer = false;
            let mut reader_err: Option<HeadError> = None;
            'outer: while let Ok(item) = decoded_rx.recv() {
                let (batch_id, decoded_batch, batch_err) = match item {
                    Ok(v) => v,
                    Err(e) => {
                        // A reader error is positioned after every batch sent before it:
                        // drain the remaining valid batches (an earlier decode error in
                        // one of them takes precedence).
                        if reader_err.is_none() {
                            reader_err = Some(e);
                        }
                        continue;
                    }
                };
                let mut cur_batch = if batch_id == next_batch_id {
                    next_batch_id += 1;
                    Some((decoded_batch, batch_err))
                } else {
                    reorder.insert(batch_id, (decoded_batch, batch_err));
                    None
                };

                while let Some((records, records_err)) = cur_batch.take().or_else(|| {
                    let b = reorder.remove(&next_batch_id);
                    if b.is_some() {
                        next_batch_id += 1;
                    }
                    b
                }) {
                    for prec in records {
                        match prec {
                            ParallelDecodedRecord::Series(series_list, hashes) => {
                                pending_series.reserve(series_list.len());
                                for (rs, h) in series_list.into_iter().zip(hashes) {
                                    pending_series.push((rs, h));
                                }
                                if pending_series.len() >= WAL_SERIES_PARALLEL_BATCH_MAX {
                                    if let Err(e) = flush_series_batch(
                                        &mut pending_series,
                                        multi_ref,
                                        mmapped_map,
                                        ooo_mmapped_map,
                                        &mut job_shards,
                                        &mut samples_queued,
                                        &mut failed_outer,
                                    ) {
                                        if !failed_outer && coord_res.is_ok() {
                                            coord_res = Err(e);
                                        }
                                        break 'outer;
                                    }
                                }
                            }
                            ParallelDecodedRecord::Other(decoded) => {
                                if !pending_series.is_empty() {
                                    if let Err(e) = flush_series_batch(
                                        &mut pending_series,
                                        multi_ref,
                                        mmapped_map,
                                        ooo_mmapped_map,
                                        &mut job_shards,
                                        &mut samples_queued,
                                        &mut failed_outer,
                                    ) {
                                        if !failed_outer && coord_res.is_ok() {
                                            coord_res = Err(e);
                                        }
                                        break 'outer;
                                    }
                                }
                                match decoded {
                                    DecodedRecord::Series(_) => unreachable!(),
                                    DecodedRecord::Samples(samples) => {
                                        for &(mut s) in &samples {
                                            if s.t < min_valid_time {
                                                continue;
                                            }
                                            if let Some(&mapped) = multi_ref.get(&s.ref_id) {
                                                self.update_wal_expiry(s.ref_id, s.t);
                                                s.ref_id = mapped;
                                            }
                                            if deleted_in_replay.contains(&s.ref_id) {
                                                unknown.miss(&unknown.samples, s.ref_id);
                                                continue;
                                            }
                                            let idx = (s.ref_id as usize) % workers;
                                            samples_queued = true;
                                            job_shards[idx].push(WalSampleJob::Float(s));
                                            if job_shards[idx].len() >= WAL_SHARD_BATCH_MAX {
                                                let batch = std::mem::take(&mut job_shards[idx]);
                                                if worker_txs[idx]
                                                    .send(WalWorkerMsg::SampleJobs(batch))
                                                    .is_err()
                                                {
                                                    failed_outer = true;
                                                    break 'outer;
                                                }
                                            }
                                        }
                                    }
                                    DecodedRecord::HistogramSamples(hists) => {
                                        for mut hs in hists {
                                            if hs.t < min_valid_time {
                                                continue;
                                            }
                                            if let Some(&mapped) = multi_ref.get(&hs.ref_id) {
                                                self.update_wal_expiry(hs.ref_id, hs.t);
                                                hs.ref_id = mapped;
                                            }
                                            let idx = (hs.ref_id as usize) % workers;
                                            samples_queued = true;
                                            job_shards[idx]
                                                .push(WalSampleJob::Histogram(Box::new(hs)));
                                            if job_shards[idx].len() >= WAL_SHARD_BATCH_MAX {
                                                let batch = std::mem::take(&mut job_shards[idx]);
                                                if worker_txs[idx]
                                                    .send(WalWorkerMsg::SampleJobs(batch))
                                                    .is_err()
                                                {
                                                    failed_outer = true;
                                                    break 'outer;
                                                }
                                            }
                                        }
                                    }
                                    DecodedRecord::FloatHistogramSamples(fhists) => {
                                        for mut fhs in fhists {
                                            if fhs.t < min_valid_time {
                                                continue;
                                            }
                                            if let Some(&mapped) = multi_ref.get(&fhs.ref_id) {
                                                self.update_wal_expiry(fhs.ref_id, fhs.t);
                                                fhs.ref_id = mapped;
                                            }
                                            let idx = (fhs.ref_id as usize) % workers;
                                            samples_queued = true;
                                            job_shards[idx].push(WalSampleJob::FloatHistogram(
                                                Box::new(fhs),
                                            ));
                                            if job_shards[idx].len() >= WAL_SHARD_BATCH_MAX {
                                                let batch = std::mem::take(&mut job_shards[idx]);
                                                if worker_txs[idx]
                                                    .send(WalWorkerMsg::SampleJobs(batch))
                                                    .is_err()
                                                {
                                                    failed_outer = true;
                                                    break 'outer;
                                                }
                                            }
                                        }
                                    }
                                    DecodedRecord::Tombstones(stones) => {
                                        let mut del_shards: Vec<Vec<u64>> =
                                            (0..workers).map(|_| Vec::new()).collect();
                                        for mut stone in stones {
                                            self.bump_last_series_id(stone.ref_id);
                                            if stone.mint == i64::MIN && stone.maxt == i64::MAX {
                                                let mut ref_id = stone.ref_id;
                                                if let Some(&mapped) = multi_ref.get(&ref_id) {
                                                    ref_id = mapped;
                                                }
                                                if deleted_in_replay.insert(ref_id) {
                                                    if let Some(entry) = self.series.get_by_id(ref_id) {
                                                        self.series.unlink_hash(entry.hash, ref_id);
                                                        let idx = (ref_id as usize) % workers;
                                                        del_shards[idx].push(ref_id);
                                                    }
                                                }
                                                continue;
                                            }
                                            if stone.maxt < min_valid_time {
                                                continue;
                                            }
                                            if let Some(&mapped) = multi_ref.get(&stone.ref_id) {
                                                self.update_wal_expiry(stone.ref_id, stone.maxt);
                                                stone.ref_id = mapped;
                                            }
                                            if deleted_in_replay.contains(&stone.ref_id) {
                                                unknown.miss(&unknown.tombstones, stone.ref_id);
                                                continue;
                                            }
                                            let idx = (stone.ref_id as usize) % workers;
                                            if worker_txs[idx]
                                                .send(WalWorkerMsg::AddTombstone {
                                                    ref_id: stone.ref_id,
                                                    interval: Interval::new(stone.mint, stone.maxt),
                                                })
                                                .is_err()
                                            {
                                                failed_outer = true;
                                                break 'outer;
                                            }
                                        }
                                        for (idx, del_refs) in del_shards.into_iter().enumerate() {
                                            if !del_refs.is_empty() {
                                                if !job_shards[idx].is_empty() {
                                                    let batch = std::mem::take(&mut job_shards[idx]);
                                                    if worker_txs[idx]
                                                        .send(WalWorkerMsg::SampleJobs(batch))
                                                        .is_err()
                                                    {
                                                        failed_outer = true;
                                                        break 'outer;
                                                    }
                                                }
                                                if worker_txs[idx]
                                                    .send(WalWorkerMsg::DeleteSeries(del_refs))
                                                    .is_err()
                                                {
                                                    failed_outer = true;
                                                    break 'outer;
                                                }
                                            }
                                        }
                                    }
                                    DecodedRecord::Exemplars(exemplars) => {
                                        for mut ex in exemplars {
                                            if ex.t < min_valid_time {
                                                continue;
                                            }
                                            if let Some(&mapped) = multi_ref.get(&ex.ref_id) {
                                                self.update_wal_expiry(ex.ref_id, ex.t);
                                                ex.ref_id = mapped;
                                            }
                                            if deleted_in_replay.contains(&ex.ref_id) {
                                                unknown.miss(&unknown.exemplars, ex.ref_id);
                                                continue;
                                            }
                                            let Some(entry) = self.series.get_by_id(ex.ref_id)
                                            else {
                                                unknown.miss(&unknown.exemplars, ex.ref_id);
                                                continue;
                                            };
                                            if self
                                                .exemplars
                                                .add_exemplar(
                                                    &entry.labels,
                                                    Exemplar::new(ex.labels, ex.v, ex.t),
                                                )
                                                .is_ok()
                                            {
                                                let mut st = entry.inner.lock();
                                                st.update_last_exemplar_ts(ex.t);
                                            }
                                        }
                                    }
                                    DecodedRecord::Metadata(metas) => {
                                        for mut m in metas {
                                            if let Some(&mapped) = multi_ref.get(&m.ref_id) {
                                                m.ref_id = mapped;
                                            }
                                            if deleted_in_replay.contains(&m.ref_id) {
                                                unknown.miss(&unknown.metadata, m.ref_id);
                                                continue;
                                            }
                                            let Some(entry) = self.series.get_by_id(m.ref_id)
                                            else {
                                                unknown.miss(&unknown.metadata, m.ref_id);
                                                continue;
                                            };
                                            let metric_type = MetricType::from_u8(m.metric_type);
                                            let compact_meta = match self.intern_metadata_symbols(
                                                metric_type,
                                                &m.unit,
                                                &m.help,
                                            ) {
                                                Ok(cm) => cm,
                                                Err(e) => {
                                                    if coord_res.is_ok() {
                                                        coord_res = Err(e);
                                                    }
                                                    break 'outer;
                                                }
                                            };
                                            self.has_any_metadata.store(true, Ordering::Release);
                                            let mut st = entry.inner.lock();
                                            st.set_metadata(compact_meta);
                                        }
                                    }
                                    // MinValidTime is read strictly from the latest checkpoint's first record
                                    // before replay; mid-WAL MinValidTime records are ignored (Go head_wal.go:251-254).
                                    DecodedRecord::MinValidTime(_)
                                    | DecodedRecord::MmapMarkers(_)
                                    | DecodedRecord::Unknown(_) => {}
                                }
                            }
                        }
                    }
                    if let Some(e) = records_err {
                        // Records before the corrupt one were applied above; stop here.
                        if coord_res.is_ok() {
                            coord_res = Err(e);
                        }
                        break 'outer;
                    }
                }
            }

            if !pending_series.is_empty() {
                if let Err(e) = flush_series_batch(
                    &mut pending_series,
                    multi_ref,
                    mmapped_map,
                    ooo_mmapped_map,
                    &mut job_shards,
                    &mut samples_queued,
                    &mut failed_outer,
                ) {
                    if !failed_outer && coord_res.is_ok() {
                        coord_res = Err(e);
                    }
                }
            }
            let _ = failed_outer;
            if coord_res.is_ok() {
                if let Some(e) = reader_err {
                    coord_res = Err(e);
                }
            }

            for (idx, shard) in job_shards.into_iter().enumerate() {
                if !shard.is_empty() {
                    let _ = worker_txs[idx].send(WalWorkerMsg::SampleJobs(shard));
                }
            }

            drop(worker_txs);
            drop(decoded_rx);
            let _ = reader_handle.join();
            for h in dec_handles {
                let _ = h.join();
            }
            for handle in worker_handles {
                if let Ok(Err(e)) = handle.join() {
                    if coord_res.is_ok() {
                        coord_res = Err(e);
                    }
                }
            }
            coord_res
        })
    }

    fn replay_wbl_reader<R: io::Read + SegmentTracker>(
        &self,
        r: &mut WlogReader<R>,
        multi_ref: &HashMap<u64, u64>,
        last_mmap_ref: ChunkDiskMapperRef,
        mut chunk_writer: Option<&mut HeadChunksWriter>,
    ) -> Result<(), HeadError> {
        let dec = RecordDecoder::new();
        let (last_seq, last_off) = last_mmap_ref.unpack();
        let mut pinned = HashSet::new();
        let unknown = UnknownRefs::default();
        self.replay_wbl_records(
            r,
            &dec,
            multi_ref,
            last_seq,
            last_off,
            &mut pinned,
            &mut chunk_writer,
            &unknown,
        )?;
        self.report_wbl_unknown_refs(&unknown);
        if !pinned.is_empty() {
            self.wbl_pinned_refs.lock().extend(pinned);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_wbl_records<R: io::Read + SegmentTracker>(
        &self,
        r: &mut WlogReader<R>,
        dec: &RecordDecoder,
        multi_ref: &HashMap<u64, u64>,
        last_seq: u32,
        last_off: u32,
        pinned: &mut HashSet<u64>,
        chunk_writer: &mut Option<&mut HeadChunksWriter>,
        unknown: &UnknownRefs,
    ) -> Result<(), HeadError> {

        let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
        let use_hist_st = self.opts.enable_histogram_st_encoding;

        while let Some(rec) = r.next_record()? {
            let decoded = match dec.decode_record(rec) {
                Ok(d) => d,
                // Go `loadWBL`: decode errors are `wlog.CorruptionErr{Segment, Offset}`.
                Err(e) => return Err(record_decode_corruption(r, &e)),
            };
            match decoded {
                DecodedRecord::MmapMarkers(markers) => {
                    for mut rm in markers {
                        let (seq, off) = ChunkDiskMapperRef(rm.mmap_ref).unpack();
                        if seq > last_seq || (seq == last_seq && off > last_off) {
                            continue;
                        }
                        if let Some(&mapped) = multi_ref.get(&rm.ref_id) {
                            pinned.insert(rm.ref_id);
                            rm.ref_id = mapped;
                        }
                        if let Some(entry) = self.series.get_by_id(rm.ref_id) {
                            let mut st = entry.inner.lock();
                            if let Some(ref mut ooo) = st.ooo_mut() {
                                ooo.ooo_head_chunk = None;
                            }
                        } else {
                            unknown.miss(&unknown.mmap_markers, rm.ref_id);
                        }
                    }
                }
                DecodedRecord::Samples(samples) => {
                    for mut s in samples {
                        if s.v.to_bits() == QUIET_ZERO_NAN_BITS {
                            continue;
                        }
                        if let Some(&mapped) = multi_ref.get(&s.ref_id) {
                            pinned.insert(s.ref_id);
                            s.ref_id = mapped;
                        }
                        if let Some(entry) = self.series.get_by_id(s.ref_id) {
                            let st_val = s.st;
                            let mut st = entry.inner.lock();
                            let (ok, created, _, _) = st.insert_ooo(
                                entry.ref_id,
                                st_val,
                                s.t,
                                OooSampleValue::Float(s.v),
                                self.opts.ooo_cap_max,
                                use_xor2,
                                use_hist_st,
                                chunk_writer.as_deref_mut(),
                            )?;
                            if created {
                                self.metrics.chunks_created(1);
                            }
                            if ok {
                                self.update_min_ooo_max_ooo_time(s.t, s.t);
                            }
                        } else {
                            unknown.miss(&unknown.samples, s.ref_id);
                        }
                    }
                }
                DecodedRecord::HistogramSamples(hists) => {
                    for mut hs in hists {
                        if let Some(&mapped) = multi_ref.get(&hs.ref_id) {
                            pinned.insert(hs.ref_id);
                            hs.ref_id = mapped;
                        }
                        if let Some(entry) = self.series.get_by_id(hs.ref_id) {
                            let st_val = hs.st;
                            let mut st = entry.inner.lock();
                            let (ok, created, _, _) = st.insert_ooo(
                                entry.ref_id,
                                st_val,
                                hs.t,
                                OooSampleValue::Histogram(hs.h),
                                self.opts.ooo_cap_max,
                                use_xor2,
                                use_hist_st,
                                chunk_writer.as_deref_mut(),
                            )?;
                            if created {
                                self.metrics.chunks_created(1);
                            }
                            if ok {
                                self.update_min_ooo_max_ooo_time(hs.t, hs.t);
                            }
                        } else {
                            unknown.miss(&unknown.histograms, hs.ref_id);
                        }
                    }
                }
                DecodedRecord::FloatHistogramSamples(fhists) => {
                    for mut fhs in fhists {
                        if let Some(&mapped) = multi_ref.get(&fhs.ref_id) {
                            pinned.insert(fhs.ref_id);
                            fhs.ref_id = mapped;
                        }
                        if let Some(entry) = self.series.get_by_id(fhs.ref_id) {
                            let st_val = fhs.st;
                            let mut st = entry.inner.lock();
                            let (ok, created, _, _) = st.insert_ooo(
                                entry.ref_id,
                                st_val,
                                fhs.t,
                                OooSampleValue::FloatHistogram(fhs.fh),
                                self.opts.ooo_cap_max,
                                use_xor2,
                                use_hist_st,
                                chunk_writer.as_deref_mut(),
                            )?;
                            if created {
                                self.metrics.chunks_created(1);
                            }
                            if ok {
                                self.update_min_ooo_max_ooo_time(fhs.t, fhs.t);
                            }
                        } else {
                            unknown.miss(&unknown.histograms, fhs.ref_id);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Go `loadWBL` unknown-reference reporting.
    fn report_wbl_unknown_refs(&self, u: &UnknownRefs) {
        let samples = u.samples.load(Ordering::Relaxed);
        let histograms = u.histograms.load(Ordering::Relaxed);
        let mmap_markers = u.mmap_markers.load(Ordering::Relaxed);
        if samples + histograms + mmap_markers == 0 {
            return;
        }
        let series = u.series.lock().len() as u64;
        tracing::warn!(
            series = series,
            samples = samples,
            histograms = histograms,
            mmap_markers = mmap_markers,
            "Unknown series references for ooo WAL replay"
        );
        let v = &self.metrics.wbl_replay_unknown_refs;
        HeadMetrics::add_unknown_refs(v, series, "series");
        HeadMetrics::add_unknown_refs(v, samples, "samples");
        HeadMetrics::add_unknown_refs(v, histograms, "histograms");
        HeadMetrics::add_unknown_refs(v, mmap_markers, "mmap_markers");
    }

    /// Go `loadWAL` unknown-reference reporting (`found_series_for_prior_segments`,
    /// `found_series`, `truly_missing_series` are the Mimir-specific diagnostics).
    fn report_wal_unknown_refs(
        &self,
        u: &UnknownRefs,
        multi_ref: &HashMap<u64, u64>,
        global_missing_series_refs: &mut HashSet<u64>,
    ) {
        let v = &self.metrics.wal_replay_unknown_refs;
        let mut found_for_prior_segments = 0u64;
        global_missing_series_refs.retain(|&wal_ref| {
            let head_ref = multi_ref.get(&wal_ref).copied().unwrap_or(wal_ref);
            if self.series.get_by_id(head_ref).is_some() {
                tracing::warn!(
                    wal_ref = wal_ref,
                    head_ref = head_ref,
                    "Series reported as missing in prior segment but was found in this WAL segment"
                );
                found_for_prior_segments += 1;
                false
            } else {
                true
            }
        });
        HeadMetrics::add_unknown_refs(
            v,
            found_for_prior_segments,
            "found_series_for_prior_segments",
        );

        let samples = u.samples.load(Ordering::Relaxed);
        let exemplars = u.exemplars.load(Ordering::Relaxed);
        let histograms = u.histograms.load(Ordering::Relaxed);
        let metadata = u.metadata.load(Ordering::Relaxed);
        let tombstones = u.tombstones.load(Ordering::Relaxed);
        if samples + exemplars + histograms + metadata + tombstones == 0 {
            return;
        }
        let missing = u.series.lock();
        tracing::warn!(
            series = missing.len(),
            samples = samples,
            exemplars = exemplars,
            histograms = histograms,
            metadata = metadata,
            tombstones = tombstones,
            "Unknown series references"
        );
        let mut found = 0u64;
        for &wal_ref in missing.iter() {
            let head_ref = multi_ref.get(&wal_ref).copied().unwrap_or(wal_ref);
            if self.series.get_by_id(head_ref).is_some() {
                found += 1;
            } else {
                global_missing_series_refs.insert(wal_ref);
            }
        }
        let truly_missing = missing.len() as u64 - found;
        HeadMetrics::add_unknown_refs(v, found, "found_series");
        HeadMetrics::add_unknown_refs(v, truly_missing, "truly_missing_series");
        HeadMetrics::add_unknown_refs(v, truly_missing + found, "series");
        HeadMetrics::add_unknown_refs(v, samples, "samples");
        HeadMetrics::add_unknown_refs(v, exemplars, "exemplars");
        HeadMetrics::add_unknown_refs(v, histograms, "histograms");
        HeadMetrics::add_unknown_refs(v, metadata, "metadata");
        HeadMetrics::add_unknown_refs(v, tombstones, "tombstones");
    }

    #[inline]
    pub fn initialized(&self) -> bool {
        self.min_time.load(Ordering::Acquire) != i64::MAX
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    #[inline]
    pub fn min_time(&self) -> i64 {
        self.min_time.load(Ordering::Acquire)
    }

    #[inline]
    pub fn max_time(&self) -> i64 {
        self.max_time.load(Ordering::Acquire)
    }

    #[inline]
    pub fn min_valid_time(&self) -> i64 {
        self.min_valid_time.load(Ordering::Acquire)
    }

    #[inline]
    pub fn min_ooo_time(&self) -> i64 {
        self.min_ooo_time.load(Ordering::Acquire)
    }

    #[inline]
    pub fn max_ooo_time(&self) -> i64 {
        self.max_ooo_time.load(Ordering::Acquire)
    }

    #[inline]
    pub fn num_series(&self) -> u64 {
        self.num_series.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn num_stale_series(&self) -> u64 {
        self.num_stale_series.load(Ordering::Acquire)
    }

    #[inline]
    pub fn num_native_histogram_series(&self) -> u64 {
        self.num_native_histogram_series.load(Ordering::Acquire)
    }

    #[inline]
    pub fn num_native_histogram_buckets(&self) -> u64 {
        self.num_native_histogram_buckets.load(Ordering::Acquire)
    }

    #[inline]
    fn update_stale_series_metric_on_append(&self, was_stale: bool, is_stale: bool) {
        match (was_stale, is_stale) {
            (false, true) => {
                self.num_stale_series.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.num_stale_series.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    #[inline]
    fn update_native_histogram_metrics_on_append(
        &self,
        was_histogram: bool,
        is_histogram: bool,
        old_buckets: usize,
        new_buckets: usize,
    ) {
        match (was_histogram, is_histogram) {
            (false, true) => {
                self.num_native_histogram_series
                    .fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                self.num_native_histogram_series
                    .fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
        if new_buckets > old_buckets {
            self.num_native_histogram_buckets
                .fetch_add((new_buckets - old_buckets) as u64, Ordering::Relaxed);
        } else if old_buckets > new_buckets {
            self.num_native_histogram_buckets
                .fetch_sub((old_buckets - new_buckets) as u64, Ordering::Relaxed);
        }
    }

    #[inline]
    fn update_metrics_on_series_delete(&self, st: &SeriesState) {
        let (is_stale, is_hist, buckets) = st.sample_state();
        if is_stale {
            self.num_stale_series.fetch_sub(1, Ordering::Relaxed);
        }
        if is_hist {
            self.num_native_histogram_series
                .fetch_sub(1, Ordering::Relaxed);
            if buckets > 0 {
                self.num_native_histogram_buckets
                    .fetch_sub(buckets as u64, Ordering::Relaxed);
            }
        }
    }

    #[inline]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    #[inline]
    pub fn options(&self) -> &HeadOptions {
        &self.opts
    }

    #[inline]
    pub fn chunk_end_time_variance(&self) -> f64 {
        f64::from_bits(self.chunk_end_time_variance_bits.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn set_chunk_end_time_variance(&self, variance: f64) {
        self.chunk_end_time_variance_bits
            .store(variance.to_bits(), Ordering::Relaxed);
    }

    #[inline]
    pub fn metadata_limits(&self) -> MetadataLimits {
        MetadataLimits {
            max_metrics_with_metadata_per_user: self
                .metadata_limits_max_metrics
                .load(Ordering::Relaxed),
            max_metadata_per_metric: self
                .metadata_limits_max_per_metric
                .load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub fn set_metadata_limits(&self, limits: MetadataLimits) {
        self.metadata_limits_max_metrics
            .store(limits.max_metrics_with_metadata_per_user, Ordering::Relaxed);
        self.metadata_limits_max_per_metric
            .store(limits.max_metadata_per_metric, Ordering::Relaxed);
    }

    #[inline]
    pub fn exemplars(&self) -> &CircularExemplarStorage {
        &self.exemplars
    }

    #[inline]
    pub fn metadata_store(&self) -> &UserMetricsMetadata {
        &self.metadata
    }

    #[inline]
    pub fn appendable_min_valid_time(&self) -> i64 {
        if !self.initialized() {
            return self.min_valid_time.load(Ordering::Acquire);
        }
        let cw_end = self.max_time().saturating_sub(self.opts.chunk_range_ms / 2);
        let min_valid = self.min_valid_time.load(Ordering::Acquire);
        cw_end.max(min_valid)
    }

    #[inline]
    pub fn min_in_order_append_time(&self) -> Option<i64> {
        if self.initialized() {
            Some(self.appendable_min_valid_time())
        } else {
            None
        }
    }

    #[inline]
    pub fn last_series_ref(&self) -> u64 {
        self.last_series_id.load(Ordering::Acquire)
    }

    #[inline]
    pub fn ooo_time_window_ms(&self) -> i64 {
        self.ooo_time_window_ms.load(Ordering::Acquire)
    }

    /// Dynamically updates the out-of-order time window (and opens WBL if needed).
    pub fn set_ooo_time_window(&self, ms: i64) -> Result<(), HeadError> {
        let clamped = ms.max(0);
        self.ooo_time_window_ms.store(clamped, Ordering::Release);
        self.exemplars.set_ooo_time_window(clamped);
        if clamped > 0 && !self.disable_wal && !self.read_only {
            let mut wbl_guard = self.wbl.lock();
            if wbl_guard.is_none() {
                let wbl_dir = self.dir.join(WBL_DIR_NAME);
                let mut wbl_writer = WlogWriter::with_segment_size(
                    &wbl_dir,
                    self.opts.wal_segment_size,
                    self.opts.wal_compression,
                )?;
                let wbl_m = self
                    .metrics
                    .ensure_wbl_metrics(self.opts.registry.as_deref(), &wbl_dir);
                wbl_writer.set_metrics(wbl_m);
                *wbl_guard = Some(wbl_writer);
            }
        }
        Ok(())
    }

    /// Dynamically resizes exemplar circular buffer capacity.
    pub fn set_max_exemplars(&self, max_exemplars: usize) {
        self.exemplars.resize(max_exemplars);
    }

    #[inline]
    pub fn float_chunk_encoding(&self) -> Encoding {
        Encoding::from_u8(self.float_chunk_encoding.load(Ordering::Acquire))
            .unwrap_or(Encoding::Xor)
    }

    /// Dynamically updates float chunk encoding (`Encoding::Xor` or `Encoding::Xor2`) for new chunks.
    pub fn set_float_chunk_encoding(&self, enc: Encoding) {
        let target = if enc == Encoding::Xor2 {
            Encoding::Xor2
        } else {
            Encoding::Xor
        };
        self.float_chunk_encoding
            .store(target.as_u8(), Ordering::Release);
    }

    /// Fsyncs WAL and WBL segments to disk, observing `prometheus_tsdb_wal_fsync_duration_seconds`.
    pub fn sync(&self) -> Result<(), HeadError> {
        {
            let mut wal_guard = self.wal.lock();
            if let Some(ref mut wal) = *wal_guard {
                wal.sync()?;
            }
        }
        {
            let mut wbl_guard = self.wbl.lock();
            if let Some(ref mut wbl) = *wbl_guard {
                wbl.sync()?;
            }
        }
        Ok(())
    }

    /// Fsyncs WAL and WBL segments to disk (Go `Head.FsyncWLSegments`).
    pub fn fsync_wl_segments(&self) -> Result<(), HeadError> {
        {
            let mut wal_guard = self.wal.lock();
            let wal = wal_guard
                .as_mut()
                .ok_or_else(|| HeadError::Repair("wal not initialized".to_string()))?;
            wal.fsync_segments_until_current().map_err(|e| {
                HeadError::Repair(format!("could not fsync wal segments: {e}"))
            })?;
        }
        {
            let mut wbl_guard = self.wbl.lock();
            if let Some(ref mut wbl) = *wbl_guard {
                wbl.fsync_segments_until_current().map_err(|e| {
                    HeadError::Repair(format!("could not fsync wbl segments: {e}"))
                })?;
            }
        }
        Ok(())
    }

    /// Computes postings cardinality stats (Go `Head.PostingsCardinalityStats`).
    pub fn postings_cardinality_stats(
        &self,
        stats_by_label_name: &str,
        limit: usize,
    ) -> crate::postings::PostingsStats {
        let syms = self.symbols.read();
        self.postings
            .stats(&syms, stats_by_label_name, limit, |_, _, _| 0)
    }

    #[doc(hidden)]
    pub fn get_wal_expiry_for_test(&self, ref_id: u64) -> Option<i64> {
        self.wal_expiries.lock().get(&ref_id).copied()
    }

    #[doc(hidden)]
    pub fn update_wal_expiry_for_test(&self, ref_id: u64, mint: i64) {
        self.update_wal_expiry(ref_id, mint);
    }

    #[doc(hidden)]
    pub fn pin_wbl_series_refs_for_test(&self, refs: &[u64]) {
        self.wbl_pinned_refs.lock().extend(refs.iter().copied());
    }

    #[doc(hidden)]
    pub fn release_wbl_pinned_series_refs_for_test(&self) {
        self.wbl_pinned_refs.lock().clear();
    }

    #[doc(hidden)]
    pub fn keep_series_in_wal_checkpoint_for_test(&self, mint: i64, ref_id: u64) -> bool {
        if self.series.get_by_id(ref_id).is_some() {
            return true;
        }
        if self.wbl_pinned_refs.lock().contains(&ref_id) {
            return true;
        }
        self.wal_expiries
            .lock()
            .get(&ref_id)
            .is_some_and(|&keep_until| keep_until >= mint)
    }

    #[doc(hidden)]
    pub fn reset_last_wal_truncation_time_for_test(&self) {
        self.last_wal_truncation_time.store(0, Ordering::Release);
    }

    #[doc(hidden)]
    pub fn set_min_time_for_test(&self, mint: i64) {
        self.min_time.store(mint, Ordering::Release);
    }

    #[doc(hidden)]
    pub fn set_max_time_for_test(&self, maxt: i64) {
        self.max_time.store(maxt, Ordering::Release);
    }

    pub fn set_enable_metadata_wal_records(&self, enabled: bool) {
        self.enable_metadata_wal_records.store(enabled, Ordering::Release);
    }

    #[doc(hidden)]
    pub fn set_min_valid_time_for_test(&self, mvt: i64) {
        self.min_valid_time.store(mvt, Ordering::Release);
    }

    #[doc(hidden)]
    pub fn unlink_hash_for_test(&self, hash: u64, ref_id: u64) {
        self.series.unlink_hash(hash, ref_id);
    }

    #[doc(hidden)]
    pub fn get_or_create_for_test(
        &self,
        pairs: &[(&str, &str)],
    ) -> Result<(Arc<SeriesEntry>, bool), HeadError> {
        let hash = hash_sorted_pairs(pairs);
        self.get_or_create_sorted_pairs(hash, pairs, 0)
    }

    #[doc(hidden)]
    pub fn compute_chunk_end_time_for_test(start: i64, cur: i64, max_t: i64, ratio: f64) -> i64 {
        compute_chunk_end_time(start, cur, max_t, ratio)
    }

    #[doc(hidden)]
    pub fn update_min_max_time_for_test(&self, mint: i64, maxt: i64) {
        self.update_min_max_time(mint, maxt);
    }

    /// Returns total on-disk size of WAL + WBL + chunks_head (Go Head.Size).
    pub fn size(&self) -> u64 {
        let wal = self.wal_size().unwrap_or(0);
        let wbl = self.wbl_size().unwrap_or(0);
        let cdm = self.chunks_head_size().unwrap_or(0);
        wal + wbl + cdm
    }

    pub fn wal_size(&self) -> io::Result<u64> {
        let guard = self.wal.lock();
        match guard.as_ref() {
            Some(w) => w.dir_size(),
            None => Ok(0),
        }
    }

    pub fn wbl_size(&self) -> io::Result<u64> {
        let guard = self.wbl.lock();
        match guard.as_ref() {
            Some(w) => w.dir_size(),
            None => Ok(0),
        }
    }

    pub fn chunks_head_size(&self) -> io::Result<u64> {
        if self.chunks_dir.exists() {
            crate::encoding::fileutil::dir_size(&self.chunks_dir)
        } else {
            Ok(0)
        }
    }

    #[inline]
    pub fn has_wal(&self) -> bool {
        self.wal.lock().is_some()
    }

    #[inline]
    pub fn has_wbl(&self) -> bool {
        self.wbl.lock().is_some()
    }

    #[inline]
    pub fn isolation(&self) -> &Isolation {
        &self.iso
    }

    #[inline]
    fn txs_stripe(&self, ref_id: u64) -> Option<&Mutex<FastU64Map<TxRing>>> {
        if self.series_txs.is_empty() {
            None
        } else {
            Some(&self.series_txs[(ref_id as usize) % self.series_txs.len()])
        }
    }

    #[inline]
    fn txs_add(&self, ref_id: u64, append_id: u64) {
        if append_id == 0 {
            return;
        }
        if let Some(stripe) = self.txs_stripe(ref_id) {
            let mut map = stripe.lock();
            map.entry(ref_id)
                .or_insert_with(|| TxRing::new(4))
                .add(append_id);
        }
    }

    #[inline]
    fn txs_cleanup_below(&self, ref_id: u64, bound: u64) {
        if bound == 0 {
            return;
        }
        if let Some(stripe) = self.txs_stripe(ref_id) {
            let mut map = stripe.lock();
            let empty = if let Some(ring) = map.get_mut(&ref_id) {
                ring.cleanup_append_ids_below(bound);
                ring.is_empty()
            } else {
                false
            };
            if empty {
                map.remove(&ref_id);
            }
        }
    }

    #[inline]
    fn txs_remove(&self, ref_id: u64) {
        if let Some(stripe) = self.txs_stripe(ref_id) {
            stripe.lock().remove(&ref_id);
        }
    }

    #[doc(hidden)]
    pub fn set_after_series_lookup_hook_for_test(
        &self,
        hook: Option<AfterSeriesLookupHook>,
    ) {
        self.has_after_series_lookup_hook.store(hook.is_some(), Ordering::Release);
        *self.after_series_lookup_hook.lock() = hook;
    }

    #[doc(hidden)]
    pub fn wal_log_raw_for_test(&self, rec: &[u8]) -> Result<(), HeadError> {
        let mut g = self.wal.lock();
        let w = g.as_mut().ok_or_else(|| HeadError::Repair("wal not initialized".into()))?;
        Ok(w.log(&[rec])?)
    }

    #[doc(hidden)]
    pub fn wbl_log_raw_for_test(&self, rec: &[u8]) -> Result<(), HeadError> {
        let mut g = self.wbl.lock();
        let w = g.as_mut().ok_or_else(|| HeadError::Repair("wbl not initialized".into()))?;
        Ok(w.log(&[rec])?)
    }

    #[doc(hidden)]
    pub fn series_ref_by_pairs_for_test(&self, pairs: &[(&str, &str)]) -> Option<u64> {
        let hash = hash_sorted_pairs(pairs);
        self.series
            .get_by_hash_and_labels(hash, &Labels::from_pairs(pairs.iter().copied()))
            .map(|e| e.ref_id)
    }

    #[doc(hidden)]
    pub fn series_ooo_mmapped_bounds_for_test(&self, ref_id: u64) -> Option<Vec<(u64, i64, i64)>> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        let ooo = st.ooo()?;
        Some(
            ooo.ooo_mmapped_chunks
                .iter()
                .map(|mc| (mc.ref_id.as_u64(), mc.min_time, mc.max_time))
                .collect(),
        )
    }

    #[doc(hidden)]
    pub fn ooo_head_chunk_samples_for_test(&self, ref_id: u64) -> Option<usize> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        let ooo = st.ooo()?;
        ooo.ooo_head_chunk.as_ref().map(|c| c.chunk.num_samples())
    }

    #[doc(hidden)]
    pub fn wal_next_segment_for_test(&self) -> Result<usize, HeadError> {
        let mut g = self.wal.lock();
        let w = g.as_mut().ok_or_else(|| HeadError::Repair("wal not initialized".into()))?;
        Ok(w.next_segment()?)
    }

    #[doc(hidden)]
    pub fn wbl_next_segment_for_test(&self) -> Result<usize, HeadError> {
        let mut g = self.wbl.lock();
        let w = g.as_mut().ok_or_else(|| HeadError::Repair("wbl not initialized".into()))?;
        Ok(w.next_segment()?)
    }

    #[doc(hidden)]
    pub fn wbl_sync_for_test(&self) -> Result<(), HeadError> {
        let mut g = self.wbl.lock();
        if let Some(ref mut w) = *g {
            w.sync()?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn wal_checkpoint_for_test(
        &self,
        from: usize,
        to: usize,
        mut keep: impl FnMut(u64) -> bool,
        mint: i64,
    ) -> Result<(), HeadError> {
        let g = self.wal.lock();
        let w = g.as_ref().ok_or_else(|| HeadError::Repair("wal not initialized".into()))?;
        crate::wlog::create_checkpoint(
            w.dir(),
            from,
            to,
            &mut keep,
            mint,
            self.opts.enable_st_storage,
            self.opts.wal_compression,
        )?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn truncate_chunk_disk_mapper_for_test(&self, min_mmap_seq: usize) -> Result<(), HeadError> {
        self.truncate_chunks_head(min_mmap_seq)
    }

    #[doc(hidden)]
    pub fn cut_chunk_disk_mapper_file_for_test(&self) -> Result<(), HeadError> {
        let mut w = self.chunk_writer.lock();
        if let Some(ref mut wr) = *w {
            wr.cut_new_file()?;
            self.refresh_chunk_reader()?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn series_ooo_chunk_count_for_test(&self, ref_id: u64) -> Option<(usize, bool)> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        Some(match st.ooo() {
            Some(ooo) => (ooo.ooo_mmapped_chunks.len(), ooo.ooo_head_chunk.is_some()),
            None => (0, false),
        })
    }

    #[doc(hidden)]
    pub fn series_head_chunk_count_for_test(&self, ref_id: u64) -> Option<(usize, usize)> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        Some((st.head_chunks_len(), st.mmapped().len()))
    }

    #[doc(hidden)]
    pub fn series_has_ooo_for_test(&self, ref_id: u64) -> Option<bool> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        Some(st.ooo().is_some())
    }

    #[doc(hidden)]
    pub fn series_max_time_for_test(&self, ref_id: u64) -> Option<i64> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        Some(st.max_time())
    }

    #[doc(hidden)]
    pub fn has_pending_commit_for_test(&self, ref_id: u64) -> Option<bool> {
        let entry = self.series.get_by_id(ref_id)?;
        Some(entry.pending_commits_count() > 0)
    }

    #[doc(hidden)]
    pub fn series_txs_empty_for_test(&self, ref_id: u64) -> Option<bool> {
        let entry = self.series.get_by_id(ref_id)?;
        let empty = self
            .txs_stripe(ref_id)
            .is_none_or(|s| s.lock().get(&ref_id).is_none_or(TxRing::is_empty));
        Some(entry.pending_commits_count() == 0 && empty)
    }

    #[doc(hidden)]
    pub fn series_txs_len_for_test(&self, ref_id: u64) -> Option<usize> {
        let _entry = self.series.get_by_id(ref_id)?;
        let len = self
            .txs_stripe(ref_id)
            .map_or(0, |s| s.lock().get(&ref_id).map_or(0, TxRing::len));
        Some(len)
    }

    #[doc(hidden)]
    pub fn wal_replay_unknown_refs_total_for_test(&self, kind: &str) -> u64 {
        self.metrics
            .wal_replay_unknown_refs
            .with_label_values(&[kind])
            .get() as u64
    }

    #[doc(hidden)]
    pub fn set_first_chunk_id_for_test(&self, ref_id: u64, id: u32) {
        if let Some(entry) = self.series.get_by_id(ref_id) {
            let mut st = entry.inner.lock();
            st.cold_mut().first_chunk_id = id;
        }
    }

    #[doc(hidden)]
    pub fn first_chunk_id_for_test(&self, ref_id: u64) -> Option<u32> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        Some(st.first_chunk_id())
    }

    #[doc(hidden)]
    pub fn newest_chunk_id_for_test(&self, ref_id: u64) -> Option<u32> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        let total = st.mmapped().len() + st.head_chunks_len();
        if total == 0 {
            return None;
        }
        let cid = ((st.first_chunk_id() as usize + total - 1) % (OOO_CHUNK_ID_MASK as usize)) as u32;
        Some(cid)
    }

    #[doc(hidden)]
    pub fn set_first_ooo_chunk_id_for_test(&self, ref_id: u64, id: u32) {
        if let Some(entry) = self.series.get_by_id(ref_id) {
            let mut st = entry.inner.lock();
            if let Some(ooo) = st.ooo_mut() {
                ooo.first_ooo_chunk_id = id;
            }
        }
    }

    #[doc(hidden)]
    pub fn pause_series_commit_for_test(&self, ref_id: u64) -> TestCommitPauseHandle<'_> {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        self.test_commit_pause
            .lock()
            .insert(ref_id, Arc::clone(&gate));
        self.has_test_commit_pause.store(true, Ordering::Release);
        TestCommitPauseHandle {
            head_map: &self.test_commit_pause,
            has_flag: &self.has_test_commit_pause,
            ref_id,
            gate,
        }
    }

    fn maybe_wait_commit_pause(&self, ref_id: u64) {
        if !self.has_test_commit_pause.load(Ordering::Acquire) {
            return;
        }
        let gate = self.test_commit_pause.lock().get(&ref_id).cloned();
        if let Some(g) = gate {
            let mut released = g.0.lock();
            while !*released {
                g.1.wait(&mut released);
            }
        }
    }

    #[doc(hidden)]
    pub fn snapshot_fingerprints_with_watermark_for_test(
        &self,
        refs: &[u64],
        append_id_watermark: u64,
    ) -> HashMap<u64, SeriesFingerprint> {
        self.snapshot_fingerprints_with_watermark(refs, append_id_watermark)
    }

    #[doc(hidden)]
    pub fn create_wal_checkpoint_for_test(
        dir: &Path,
        from: usize,
        to: usize,
        mint: i64,
    ) -> Result<(), HeadError> {
        crate::wlog::create_checkpoint(
            dir,
            from,
            to,
            |_| true,
            mint,
            false,
            CompressionType::None,
        )?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn corrupt_wal_checkpoint_first_byte_for_test(wal_dir: &Path) -> Result<(), HeadError> {
        let (cp_dir, _) = crate::wlog::last_checkpoint(wal_dir)?
            .ok_or_else(|| HeadError::Repair("no checkpoint found".into()))?;
        let seg0 = crate::wlog::segment_name(&cp_dir, 0);
        use std::io::{Seek, Write};
        let mut f = fs::OpenOptions::new().write(true).open(seg0)?;
        f.seek(io::SeekFrom::Start(1))?;
        f.write_all(&[42u8])?;
        f.sync_all()?;
        Ok(())
    }

    #[doc(hidden)]
    pub fn ooo_samples_appended_count_for_test(&self) -> u64 {
        let f = self.metrics.ooo_samples_appended.with_label_values(&["float"]).get() as u64;
        let h = self.metrics.ooo_samples_appended.with_label_values(&["histogram"]).get() as u64;
        f + h
    }

    #[doc(hidden)]
    pub fn series_metadata_for_test(&self, ref_id: u64) -> Option<SeriesMetadata> {
        let entry = self.series.get_by_id(ref_id)?;
        let st = entry.inner.lock();
        let cm = st.metadata()?;
        let syms = self.symbols.read();
        Some(SeriesMetadata {
            metric_type: cm.metric_type,
            unit: syms.resolve(cm.unit).to_owned(),
            help: syms.resolve(cm.help).to_owned(),
        })
    }

    /// Sets or replaces the series lifecycle callback.
    pub fn set_lifecycle_callback(&self, cb: Option<Arc<dyn SeriesLifecycleCallback>>) {
        *self.lifecycle_callback.write() = cb;
    }

    /// Clears the series lifecycle callback (breaks reference chains on tenant unload).
    pub fn clear_lifecycle_callback(&self) {
        *self.lifecycle_callback.write() = None;
    }

    /// Zero-allocation series lookup by sorted `(&str, &str)` pairs.
    pub fn get_ref_by_sorted_pairs(&self, pairs: &[(&str, &str)]) -> Option<u64> {
        let hash = hash_sorted_pairs(pairs);
        self.get_by_sorted_pairs(hash, pairs).map(|e| e.ref_id)
    }

    /// Zero-allocation `SeriesEntry` lookup by sorted `(&str, &str)` pairs.
    pub fn get_entry_by_sorted_pairs(&self, pairs: &[(&str, &str)]) -> Option<Arc<SeriesEntry>> {
        let hash = hash_sorted_pairs(pairs);
        self.get_by_sorted_pairs(hash, pairs)
    }

    /// Zero-allocation `SeriesEntry` lookup using a caller-supplied precomputed Prometheus `hash`.
    #[inline]
    pub fn get_entry_by_prehashed_sorted_pairs(
        &self,
        hash: u64,
        pairs: &[(&str, &str)],
    ) -> Option<Arc<SeriesEntry>> {
        self.get_by_sorted_pairs(hash, pairs)
    }

    /// Zero-allocation `SeriesEntry` lookup using precomputed `hash` and `stringlabels`-encoded bytes.
    #[inline]
    pub fn get_entry_by_prehashed_encoded_labels(
        &self,
        hash: u64,
        encoded_labels: &[u8],
    ) -> Option<Arc<SeriesEntry>> {
        self.series.get_by_encoded_labels(hash, encoded_labels)
    }

    /// Prefetches the `StripeSeries` stripe bucket for a precomputed `hash`.
    #[inline]
    pub fn prefetch_stripe_for_hash(&self, hash: u64) {
        let idx = self.series.stripe_idx(hash);
        prefetch_read_data(&self.series.stripes[idx]);
    }

    /// Pipelined, software-prefetched batch resolution of prehashed sorted pairs.
    pub fn resolve_prehashed_batch<T>(
        &self,
        items: &[T],
        get_hp: impl Fn(&T) -> Option<(u64, &[(&str, &str)])>,
        out: &mut Vec<Option<Arc<SeriesEntry>>>,
    ) {
        self.series.resolve_prehashed_batch(items, get_hp, out);
    }

    /// Pipelined, software-prefetched batch resolution of prehashed `stringlabels`-encoded bytes.
    pub fn resolve_prehashed_encoded_batch<T>(
        &self,
        items: &[T],
        get_he: impl Fn(&T) -> Option<(u64, &[u8])>,
        out: &mut Vec<Option<Arc<SeriesEntry>>>,
    ) {
        self.series
            .resolve_prehashed_encoded_batch(items, get_he, out);
    }

    /// Iterates every live series `(ref_id, &Labels)` in Head.
    pub fn for_each_series<F>(&self, mut f: F)
    where
        F: FnMut(u64, &Labels),
    {
        let mut entries = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            entries.clear();
            StripeSeries::collect_stripe_into(stripe, &mut entries);
            for entry in entries.drain(..) {
                f(entry.ref_id, &entry.labels);
            }
        }
    }

    /// Reads the packed `active_cache` word for `ref_id` without cloning `Arc<SeriesEntry>`.
    #[inline]
    pub fn active_cache_for_id(&self, ref_id: u64) -> u64 {
        self.series.active_cache_for_id(ref_id)
    }

    /// Returns postings list for a single `(name, value)` pair.
    pub fn postings_for_label_value(&self, name: &str, value: &str) -> Vec<u64> {
        let syms = self.symbols.read();
        let Some((nid, vid)) = syms.get(name).zip(syms.get(value)) else {
            return Vec::new();
        };
        self.postings.postings_for_id(nid, vid)
    }

    /// Visits postings slices for a single `(name, value)` pair without cloning.
    pub fn visit_postings_for_label_value<R: Default>(
        &self,
        name: &str,
        value: &str,
        f: impl FnOnce(&[u32], &[u64]) -> R,
    ) -> R {
        let syms = self.symbols.read();
        let Some((nid, vid)) = syms.get(name).zip(syms.get(value)) else {
            return R::default();
        };
        self.postings.visit_postings_for_id(nid, vid, f)
    }

    /// Visits all `(val, small_u32_refs, large_u64_refs)` postings for `name` under a single
    /// `symbols.read()` and single postings stripe lock.
    pub fn visit_all_values_postings_for_name(
        &self,
        name: &str,
        f: impl FnMut(&str, &[u32], &[u64]),
    ) {
        let syms = self.symbols.read();
        self.postings
            .visit_all_values_postings_for_name(name, &syms, f);
    }

    /// Returns number of postings for `(name, value)`.
    pub fn postings_length(&self, name: &str, value: &str) -> usize {
        let syms = self.symbols.read();
        let (Some(nid), Some(vid)) = (syms.get(name), syms.get(value)) else {
            return 0;
        };
        self.postings.postings_len(nid, vid)
    }

    /// Returns first/lowest series ref for `(name, value)`.
    pub fn first_posting(&self, name: &str, value: &str) -> Option<u64> {
        let syms = self.symbols.read();
        let (Some(nid), Some(vid)) = (syms.get(name), syms.get(value)) else {
            return None;
        };
        self.postings.first_posting(nid, vid)
    }

    /// Appends up to `limit` sample series refs for `(name, value)`.
    pub fn append_random_postings_refs(
        &self,
        name: &str,
        value: &str,
        limit: usize,
        out: &mut Vec<u64>,
    ) {
        let syms = self.symbols.read();
        let (Some(nid), Some(vid)) = (syms.get(name), syms.get(value)) else {
            return;
        };
        self.postings.append_postings_sample(nid, vid, limit, out);
    }

    /// Returns total bytes of distinct label values per label name.
    pub fn label_values_bytes(&self) -> HashMap<String, u64> {
        let syms = self.symbols.read();
        self.postings.label_values_bytes(&syms)
    }

    /// Go `Head.compactable`: with `TimelyCompaction` the head is compactable once the min
    /// block end is .5 chunk ranges in the past; otherwise once the head spans 1.5 chunk
    /// ranges (the Prometheus / Mimir default).
    pub fn compactable(&self) -> bool {
        if !self.initialized() {
            return false;
        }
        if self.opts.timely_compaction {
            let min_block_end = range_for_timestamp(self.min_time(), self.opts.chunk_range_ms);
            return min_block_end < self.appendable_min_valid_time();
        }
        (self.max_time() as i128) - (self.min_time() as i128)
            > ((self.opts.chunk_range_ms / 2) as i128) * 3
    }

    /// Alias for [`Head::compactable`] matching Go `Head.compactable` (`head.go:2418-2429`).
    #[inline]
    pub fn is_compactable(&self) -> bool {
        self.compactable()
    }

    pub fn init_time(&self, t: i64) {
        if self
            .max_time
            .compare_exchange(i64::MIN, t, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let _ = self.min_time.compare_exchange(
                i64::MAX,
                t,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        } else {
            let start = Instant::now();
            while self.min_time.load(Ordering::Acquire) == i64::MAX
                && self.max_time.load(Ordering::Acquire) != i64::MAX
            {
                if start.elapsed() >= std::time::Duration::from_millis(100) {
                    break;
                }
                std::hint::spin_loop();
            }
        }
    }

    fn update_min_max_time(&self, mint: i64, maxt: i64) {
        if mint <= maxt {
            self.init_time(mint);
        }
        if mint != i64::MAX {
            let mut cur = self.min_time.load(Ordering::Relaxed);
            while mint < cur {
                match self.min_time.compare_exchange_weak(
                    cur,
                    mint,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
        if maxt != i64::MIN {
            let mut cur = self.max_time.load(Ordering::Relaxed);
            while maxt > cur {
                match self.max_time.compare_exchange_weak(
                    cur,
                    maxt,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
    }

    fn update_min_ooo_max_ooo_time(&self, mint: i64, maxt: i64) {
        if mint != i64::MAX {
            let mut cur = self.min_ooo_time.load(Ordering::Relaxed);
            while mint < cur {
                match self.min_ooo_time.compare_exchange_weak(
                    cur,
                    mint,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
        if maxt != i64::MIN {
            let mut cur = self.max_ooo_time.load(Ordering::Relaxed);
            while maxt > cur {
                match self.max_ooo_time.compare_exchange_weak(
                    cur,
                    maxt,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
    }

    fn bump_last_series_id(&self, id: u64) {
        let mut cur = self.last_series_id.load(Ordering::Relaxed);
        while id > cur {
            match self.last_series_id.compare_exchange_weak(
                cur,
                id,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }

    fn update_wal_expiry(&self, id: u64, keep_until: i64) {
        let mut expiries = self.wal_expiries.lock();
        let entry = expiries.entry(id).or_insert(i64::MIN);
        if keep_until > *entry {
            *entry = keep_until;
        }
    }

    /// Looks up an existing series by sorted `(&str, &str)` pairs with ZERO heap allocations!
    fn get_by_sorted_pairs(&self, hash: u64, pairs: &[(&str, &str)]) -> Option<Arc<SeriesEntry>> {
        self.series.get_by_sorted_pairs(hash, pairs)
    }

    /// Gets or creates a series from already-sorted `(&str, &str)` pairs.
    fn get_or_create_sorted_pairs(
        &self,
        hash: u64,
        pairs: &[(&str, &str)],
        init_pending: u32,
    ) -> Result<(Arc<SeriesEntry>, bool), HeadError> {
        if let Some(existing) = self.get_by_sorted_pairs(hash, pairs) {
            return Ok((existing, false));
        }

        let cb_opt = self.lifecycle_callback.read().clone();
        if let Some(ref cb) = cb_opt {
            cb.pre_creation(pairs)?;
        }

        let labels = Labels::from_sorted_slice(pairs);
        let metric_name = pairs
            .iter()
            .find(|&&(k, _)| k == METRIC_NAME)
            .map_or("", |&(_, v)| v);

        loop {
            let creation_guard = self.series_creation_mtx.lock();
            let fast_pending = {
                let idx = self.series.stripe_idx(hash);
                let stripe = self.series.stripes[idx].read();
                if let Some((_, existing)) = stripe
                    .by_hash
                    .find(hash, |&(h, ref entry)| {
                        h == hash && entry.labels.eq_sorted_pairs(pairs)
                    })
                {
                    return Ok((Arc::clone(existing), false));
                }
                self.series.pending_tombstones.is_pending(hash)
            };
            if fast_pending {
                drop(creation_guard);
                self.series.pending_tombstones.wait_while_pending(hash);
                continue;
            }

            let symbol_labels = self.intern_sorted_pairs_read_first(pairs)?;
            let ref_id = self.last_series_id.fetch_add(1, Ordering::AcqRel) + 1;
            let entry = Arc::new(SeriesEntry::new(ref_id, hash, labels.clone(), init_pending));
            match self.series.insert_unless_pending(Arc::clone(&entry)) {
                Ok(()) => {
                    self.postings.add(ref_id, &symbol_labels);
                    self.postings_cache
                        .invalidate_metric_for_source(self.postings_cache_source_id, metric_name);
                    self.num_series.fetch_add(1, Ordering::Relaxed);
                    if let Some(ref g) = self.series_gauge {
                        g.inc();
                    }
                    if let Some(ref c) = self.series_created_total {
                        c.inc();
                    }
                    drop(creation_guard);
                    if let Some(ref cb) = cb_opt {
                        cb.post_creation(ref_id, &entry.labels);
                    }
                    return Ok((entry, true));
                }
                Err(Some(existing)) => return Ok((existing, false)),
                Err(None) => {
                    drop(creation_guard);
                    self.series.pending_tombstones.wait_while_pending(hash);
                    continue;
                }
            }
        }
    }

    /// Gets or creates a series directly from pre-encoded `stringlabels` bytes (ZERO re-encoding!).
    fn get_or_create_encoded_labels(
        &self,
        hash: u64,
        encoded_labels: &[u8],
        init_pending: u32,
    ) -> Result<(Arc<SeriesEntry>, bool), HeadError> {
        if let Some(existing) = self.series.get_by_encoded_labels(hash, encoded_labels) {
            return Ok((existing, false));
        }

        let cb_opt = self.lifecycle_callback.read().clone();
        if let Some(ref cb) = cb_opt {
            let pairs_tmp: SmallVec<[(&str, &str); 24]> =
                iter_stringlabels(encoded_labels).collect();
            cb.pre_creation(&pairs_tmp)?;
        }

        let labels = Labels::from_raw_stringlabels_bytes(Bytes::copy_from_slice(encoded_labels));

        loop {
            let creation_guard = self.series_creation_mtx.lock();
            let fast_pending = {
                let idx = self.series.stripe_idx(hash);
                let stripe = self.series.stripes[idx].read();
                if let Some((_, existing)) = stripe
                    .by_hash
                    .find(hash, |&(h, ref entry)| {
                        h == hash && entry.labels.as_bytes() == encoded_labels
                    })
                {
                    return Ok((Arc::clone(existing), false));
                }
                self.series.pending_tombstones.is_pending(hash)
            };
            if fast_pending {
                drop(creation_guard);
                self.series.pending_tombstones.wait_while_pending(hash);
                continue;
            }

            let symbol_labels = self.intern_encoded_labels_read_first(encoded_labels)?;
            let ref_id = self.last_series_id.fetch_add(1, Ordering::AcqRel) + 1;
            let entry = Arc::new(SeriesEntry::new(ref_id, hash, labels.clone(), init_pending));
            match self.series.insert_unless_pending(Arc::clone(&entry)) {
                Ok(()) => {
                    self.postings.add(ref_id, &symbol_labels);
                    self.postings_cache.invalidate_metric_for_source(
                        self.postings_cache_source_id,
                        entry.labels.get(METRIC_NAME),
                    );
                    self.num_series.fetch_add(1, Ordering::Relaxed);
                    if let Some(ref g) = self.series_gauge {
                        g.inc();
                    }
                    if let Some(ref c) = self.series_created_total {
                        c.inc();
                    }
                    drop(creation_guard);
                    if let Some(ref cb) = cb_opt {
                        cb.post_creation(ref_id, &entry.labels);
                    }
                    return Ok((entry, true));
                }
                Err(Some(existing)) => return Ok((existing, false)),
                Err(None) => {
                    drop(creation_guard);
                    self.series.pending_tombstones.wait_while_pending(hash);
                    continue;
                }
            }
        }
    }

    fn intern_encoded_labels_read_first(
        &self,
        encoded_labels: &[u8],
    ) -> Result<SymbolLabels, HeadError> {
        let mut missing = false;
        let mut resolved: SmallVec<[(Option<SymbolId>, Option<SymbolId>); 24]> = SmallVec::new();
        {
            let syms = self.symbols.read();
            for (k, v) in iter_stringlabels(encoded_labels) {
                let kid = syms.get(k);
                let vid = syms.get(v);
                if kid.is_none() || vid.is_none() {
                    missing = true;
                }
                resolved.push((kid, vid));
            }
        }
        let interned: SmallVec<[(SymbolId, SymbolId); 8]> = if !missing {
            resolved
                .into_iter()
                .filter_map(|(k, v)| Some((k?, v?)))
                .collect()
        } else {
            let mut syms = self.symbols.write();
            let mut out = SmallVec::with_capacity(resolved.len());
            for ((k, v), (kid, vid)) in iter_stringlabels(encoded_labels).zip(resolved) {
                let kf = match kid {
                    Some(id) => id,
                    None => syms.try_intern(k)?,
                };
                let vf = match vid {
                    Some(id) => id,
                    None => syms.try_intern(v)?,
                };
                out.push((kf, vf));
            }
            out
        };
        Ok(SymbolLabels::from_sorted_ids(interned))
    }

    fn intern_sorted_pairs_read_first(
        &self,
        pairs: &[(&str, &str)],
    ) -> Result<SymbolLabels, HeadError> {
        let mut missing = false;
        let mut resolved: SmallVec<[(Option<SymbolId>, Option<SymbolId>); 24]> =
            SmallVec::with_capacity(pairs.len());
        {
            let syms = self.symbols.read();
            for &(k, v) in pairs {
                let kid = syms.get(k);
                let vid = syms.get(v);
                if kid.is_none() || vid.is_none() {
                    missing = true;
                }
                resolved.push((kid, vid));
            }
        }
        let interned: SmallVec<[(SymbolId, SymbolId); 8]> = if !missing {
            resolved
                .into_iter()
                .filter_map(|(k, v)| Some((k?, v?)))
                .collect()
        } else {
            let mut syms = self.symbols.write();
            let mut out = SmallVec::with_capacity(pairs.len());
            for (i, &(k, v)) in pairs.iter().enumerate() {
                let (kid, vid) = resolved[i];
                let kf = match kid {
                    Some(id) => id,
                    None => syms.try_intern(k)?,
                };
                let vf = match vid {
                    Some(id) => id,
                    None => syms.try_intern(v)?,
                };
                out.push((kf, vf));
            }
            out
        };
        Ok(SymbolLabels::from_sorted_ids(interned))
    }

    fn intern_labels_read_first(&self, labels: &Labels) -> Result<SymbolLabels, HeadError> {
        let mut missing = false;
        let mut resolved: SmallVec<[(Option<SymbolId>, Option<SymbolId>); 24]> =
            SmallVec::new();
        {
            let syms = self.symbols.read();
            for (k, v) in labels.iter() {
                let kid = syms.get(k);
                let vid = syms.get(v);
                if kid.is_none() || vid.is_none() {
                    missing = true;
                }
                resolved.push((kid, vid));
            }
        }
        let interned: SmallVec<[(SymbolId, SymbolId); 8]> = if !missing {
            resolved
                .into_iter()
                .filter_map(|(k, v)| Some((k?, v?)))
                .collect()
        } else {
            let mut syms = self.symbols.write();
            let mut out = SmallVec::with_capacity(resolved.len());
            for ((k, v), (kid, vid)) in labels.iter().zip(resolved) {
                let kf = match kid {
                    Some(id) => id,
                    None => syms.try_intern(k)?,
                };
                let vf = match vid {
                    Some(id) => id,
                    None => syms.try_intern(v)?,
                };
                out.push((kf, vf));
            }
            out
        };
        Ok(SymbolLabels::from_sorted_ids(interned))
    }

    fn replay_wal_series_batch<F>(
        &self,
        series_list: &[RefSeries],
        multi_ref: &mut HashMap<u64, u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        pending_postings: &mut Vec<(u64, SymbolLabels)>,
        mut on_series: F,
    ) -> Result<(), HeadError>
    where
        F: FnMut(Arc<SeriesEntry>, Vec<MmappedChunk>, Vec<MmappedChunk>, bool) -> Result<(), HeadError>,
    {
        if series_list.is_empty() {
            return Ok(());
        }
        let mut max_ref_id = 0u64;
        let cb_opt = self.lifecycle_callback.read().clone();
        let mut syms = self.symbols.write();
        let mut created_count = 0u64;

        for rs in series_list {
            max_ref_id = max_ref_id.max(rs.ref_id);
            let hash = rs.labels.hash();
            if let Some(existing) = self.series.get_by_hash_and_labels(hash, &rs.labels) {
                multi_ref.insert(rs.ref_id, existing.ref_id);
                let mmc = mmapped_map.remove(&rs.ref_id).unwrap_or_default();
                let ooo_mmc = ooo_mmapped_map.remove(&rs.ref_id).unwrap_or_default();
                on_series(existing, mmc, ooo_mmc, true)?;
                continue;
            }

            let mut interned: SmallVec<[(SymbolId, SymbolId); 8]> = SmallVec::new();
            for (k, v) in rs.labels.iter() {
                interned.push((syms.try_intern(k)?, syms.try_intern(v)?));
            }
            let symbol_labels = SymbolLabels::from_sorted_ids(interned);
            let entry = Arc::new(SeriesEntry::new(
                rs.ref_id,
                hash,
                rs.labels.clone(),
                0,
            ));
            self.series.insert(Arc::clone(&entry));
            pending_postings.push((rs.ref_id, symbol_labels));
            created_count += 1;
            if let Some(ref cb) = cb_opt {
                cb.post_creation(rs.ref_id, &entry.labels);
            }
            let mmc = mmapped_map.remove(&rs.ref_id).unwrap_or_default();
            let ooo_mmc = ooo_mmapped_map.remove(&rs.ref_id).unwrap_or_default();
            on_series(entry, mmc, ooo_mmc, false)?;
        }
        drop(syms);

        if created_count > 0 {
            self.num_series.fetch_add(created_count, Ordering::Relaxed);
            if let Some(ref g) = self.series_gauge {
                g.add(created_count as f64);
            }
            if let Some(ref c) = self.series_created_total {
                c.inc_by(created_count as f64);
            }
        }
        if max_ref_id > 0 {
            self.bump_last_series_id(max_ref_id);
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn get_or_create_with_id_deferred_postings(
        &self,
        id: Option<u64>,
        hash: u64,
        labels: &Labels,
        pending_postings: &mut Vec<(u64, SymbolLabels)>,
    ) -> Result<(Arc<SeriesEntry>, bool), HeadError> {
        if let Some(existing) = self.series.get_by_hash_and_labels(hash, labels) {
            return Ok((existing, false));
        }

        let _creation_guard = self.series_creation_mtx.lock();
        if let Some(existing) = self.series.get_by_hash_and_labels(hash, labels) {
            return Ok((existing, false));
        }

        let ref_id = match id {
            Some(i) => i,
            None => self.last_series_id.fetch_add(1, Ordering::AcqRel) + 1,
        };
        let symbol_labels = self.intern_labels_read_first(labels)?;
        let entry = Arc::new(SeriesEntry::new(ref_id, hash, labels.clone(), 0));
        self.series.insert(Arc::clone(&entry));
        pending_postings.push((ref_id, symbol_labels));
        self.num_series.fetch_add(1, Ordering::Relaxed);
        if let Some(ref g) = self.series_gauge {
            g.inc();
        }
        if let Some(ref c) = self.series_created_total {
            c.inc();
        }
        if let Some(ref cb) = *self.lifecycle_callback.read() {
            cb.post_creation(ref_id, &entry.labels);
        }
        Ok((entry, true))
    }

    #[allow(dead_code)]
    fn get_or_create_with_id(
        &self,
        id: Option<u64>,
        hash: u64,
        labels: &Labels,
    ) -> Result<(Arc<SeriesEntry>, bool), HeadError> {
        if let Some(existing) = self.series.get_by_hash_and_labels(hash, labels) {
            return Ok((existing, false));
        }

        let _creation_guard = self.series_creation_mtx.lock();
        if let Some(existing) = self.series.get_by_hash_and_labels(hash, labels) {
            return Ok((existing, false));
        }

        let ref_id = match id {
            Some(i) => i,
            None => self.last_series_id.fetch_add(1, Ordering::AcqRel) + 1,
        };
        let symbol_labels = self.intern_labels_read_first(labels)?;
        let metric_name = labels.get(METRIC_NAME);
        let entry = Arc::new(SeriesEntry::new(ref_id, hash, labels.clone(), 0));
        self.series.insert(Arc::clone(&entry));
        self.postings.add(ref_id, &symbol_labels);
        self.postings_cache
            .invalidate_metric_silent_for_source(self.postings_cache_source_id, metric_name);
        self.num_series.fetch_add(1, Ordering::Relaxed);
        if let Some(ref g) = self.series_gauge {
            g.inc();
        }
        if let Some(ref c) = self.series_created_total {
            c.inc();
        }
        if let Some(ref cb) = *self.lifecycle_callback.read() {
            cb.post_creation(ref_id, &entry.labels);
        }
        Ok((entry, true))
    }

    fn delete_series_entries(&self, entries: &[Arc<SeriesEntry>]) {
        if entries.is_empty() {
            return;
        }
        self.delete_series_entries_owned(entries.to_vec());
    }

    fn delete_series_entries_owned(&self, entries: Vec<Arc<SeriesEntry>>) {
        if entries.is_empty() {
            return;
        }
        let entries_len = entries.len();
        let mut deleted = HashSet::with_capacity(entries_len);
        let has_cb = self.lifecycle_callback.read().is_some();
        let mut deleted_pairs = if has_cb {
            Vec::with_capacity(entries_len)
        } else {
            Vec::new()
        };
        let mut small_metric_names: Vec<String> = if entries_len <= 64 {
            Vec::with_capacity(entries_len)
        } else {
            Vec::new()
        };
        let mut affected_set: hashbrown::HashSet<(SymbolId, SymbolId)> =
            hashbrown::HashSet::with_capacity(entries_len.min(65536));
        // Keep `symbols` read-locked continuously from building `affected` until `postings.delete`
        // finishes so a concurrent `maybe_gc_symbols` cannot remap `SymbolId`s between lookup and deletion.
        let syms_guard = self.symbols.read();
        for e in &entries {
            for (k, v) in e.labels.iter() {
                if let (Some(nid), Some(vid)) = (syms_guard.get(k), syms_guard.get(v)) {
                    affected_set.insert((nid, vid));
                }
            }
        }
        let affected: Vec<(SymbolId, SymbolId)> = affected_set.into_iter().collect();

        let mut chunks_removed = 0u64;
        for e in entries {
            self.series.remove(&e);
            e.mark_gced();
            deleted.insert(e.ref_id);
            if has_cb {
                deleted_pairs.push((e.ref_id, e.labels.clone()));
            }
            if entries_len <= 64 {
                small_metric_names.push(e.labels.get(METRIC_NAME).to_owned());
            }
            {
                let mut st = e.inner.lock();
                if let Some(keep_until) = {
                    let mt = st.max_time();
                    (mt != i64::MIN).then_some(mt)
                } {
                    self.update_wal_expiry(e.ref_id, keep_until);
                }
                self.update_metrics_on_series_delete(&st);
                chunks_removed += (st.mmapped().len() + st.head_chunks_len()) as u64;
                st.mmapped_chunks = Vec::new();
                st.clear_head_chunks();
                st.set_gced(true);
            }
            // Drop `e: Arc<SeriesEntry>` immediately to release SeriesState + chunk buffers before
            // postings deletion and lifecycle callbacks.
            drop(e);
        }

        let removed_count = deleted.len();
        self.num_series.fetch_sub(removed_count as u64, Ordering::Relaxed);
        self.deleted_series_since_symbol_gc
            .fetch_add(removed_count as u64, Ordering::Release);
        if let Some(ref g) = self.series_gauge {
            g.sub(removed_count as f64);
        }
        if let Some(ref c) = self.series_removed_total {
            c.inc_by(removed_count as f64);
        }
        self.metrics.chunks_removed(chunks_removed);
        self.postings.delete(&deleted, &affected);
        drop(syms_guard);
        drop(affected);
        if entries_len > 64 {
            self.postings_cache
                .invalidate_source(self.postings_cache_source_id);
        } else {
            for m in &small_metric_names {
                self.postings_cache
                    .invalidate_metric_silent_for_source(self.postings_cache_source_id, m);
            }
        }
        self.tombstones.delete_tombstones(&deleted);
        drop(deleted);
        if let Some(ref cb) = *self.lifecycle_callback.read() {
            cb.post_deletion(&deleted_pairs);
        }
    }

    fn fire_post_deletion(&self, entries: &[(Arc<SeriesEntry>, i64)]) {
        if entries.is_empty() {
            return;
        }
        let cb_opt = self.lifecycle_callback.read().clone();
        if let Some(ref cb) = cb_opt {
            let deleted_pairs: Vec<(u64, Labels)> = entries
                .iter()
                .map(|(e, _)| (e.ref_id, e.labels.clone()))
                .collect();
            cb.post_deletion(&deleted_pairs);
        }
    }

    /// Cleans up postings, metrics, tombstones, WAL expiries, and lifecycle callbacks for series
    /// that were already atomically marked GCed and unlinked from `StripeSeries`.
    fn finish_deleted_series_cleanup(&self, entries: Vec<(Arc<SeriesEntry>, i64)>) {
        self.finish_deleted_series_cleanup_inner(entries, true);
    }

    fn finish_deleted_series_cleanup_inner(
        &self,
        entries: Vec<(Arc<SeriesEntry>, i64)>,
        call_post_deletion: bool,
    ) {
        if entries.is_empty() {
            return;
        }
        let entries_len = entries.len();
        let mut deleted = HashSet::with_capacity(entries_len);
        let has_cb = call_post_deletion && self.lifecycle_callback.read().is_some();
        let mut deleted_pairs = if has_cb {
            Vec::with_capacity(entries_len)
        } else {
            Vec::new()
        };
        let mut small_metric_names: Vec<String> = if entries_len <= 64 {
            Vec::with_capacity(entries_len)
        } else {
            Vec::new()
        };
        let mut affected_set: hashbrown::HashSet<(SymbolId, SymbolId)> =
            hashbrown::HashSet::with_capacity(entries_len.min(65536));
        let syms_guard = self.symbols.read();
        for (e, _) in &entries {
            for (k, v) in e.labels.iter() {
                if let (Some(nid), Some(vid)) = (syms_guard.get(k), syms_guard.get(v)) {
                    affected_set.insert((nid, vid));
                }
            }
        }
        let affected: Vec<(SymbolId, SymbolId)> = affected_set.into_iter().collect();

        for (e, keep_until) in entries {
            self.txs_remove(e.ref_id);
            deleted.insert(e.ref_id);
            if has_cb {
                deleted_pairs.push((e.ref_id, e.labels.clone()));
            }
            if entries_len <= 64 {
                small_metric_names.push(e.labels.get(METRIC_NAME).to_owned());
            }
            if keep_until != i64::MIN {
                self.update_wal_expiry(e.ref_id, keep_until);
            }
            drop(e);
        }

        let removed_count = deleted.len();
        self.num_series.fetch_sub(removed_count as u64, Ordering::Relaxed);
        self.deleted_series_since_symbol_gc
            .fetch_add(removed_count as u64, Ordering::Release);
        if let Some(ref g) = self.series_gauge {
            g.sub(removed_count as f64);
        }
        if let Some(ref c) = self.series_removed_total {
            c.inc_by(removed_count as f64);
        }
        self.postings.delete(&deleted, &affected);
        drop(syms_guard);
        drop(affected);
        if entries_len > 64 {
            self.postings_cache
                .invalidate_source(self.postings_cache_source_id);
        } else {
            for m in &small_metric_names {
                self.postings_cache
                    .invalidate_metric_silent_for_source(self.postings_cache_source_id, m);
            }
        }
        self.tombstones.delete_tombstones(&deleted);
        drop(deleted);
        if call_post_deletion {
            if let Some(ref cb) = *self.lifecycle_callback.read() {
                cb.post_deletion(&deleted_pairs);
            }
        }
    }

    #[inline]
    fn mark_ooo_unflushed(&self, ref_id: u64) {
        let mut g = self.ooo_unflushed_series.lock();
        g.insert(ref_id);
        self.has_ooo_unflushed.store(true, Ordering::Release);
    }

    #[inline]
    fn clear_ooo_unflushed(&self, ref_id: u64) {
        let mut g = self.ooo_unflushed_series.lock();
        if g.remove(&ref_id) {
            self.has_ooo_unflushed.store(!g.is_empty(), Ordering::Release);
        }
    }

    fn rewrite_discarded_ooo_pending_under_locks(
        entry_ref_id: u64,
        ooo: &mut SeriesOooState,
        writer: &mut HeadChunksWriter,
    ) -> Result<bool, HeadChunksError> {
        if ooo.ooo_pending_mmapped.is_empty() {
            return Ok(false);
        }
        let wm = writer.flushed_watermark();
        if !ooo
            .ooo_pending_mmapped
            .iter()
            .any(|p| wm.is_discarded(p.mc.ref_id, p.write_epoch))
        {
            return Ok(false);
        }
        let snap = writer.take_snapshot();
        let mut new_refs: SmallVec<[(usize, ChunkDiskMapperRef); 4]> = SmallVec::new();
        for (idx, p) in ooo.ooo_pending_mmapped.iter().enumerate() {
            if wm.is_discarded(p.mc.ref_id, p.write_epoch) {
                let r = match writer.write_chunk(
                    entry_ref_id,
                    p.mc.min_time,
                    p.mc.max_time,
                    &p.chunk,
                    true,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        writer.rollback_snapshot(snap);
                        return Err(e);
                    }
                };
                new_refs.push((idx, r));
            }
        }
        let cur_epoch = writer.discard_epoch();
        for (idx, r) in new_refs {
            let p = &mut ooo.ooo_pending_mmapped[idx];
            p.mc.ref_id = r;
            p.write_epoch = Some(cur_epoch);
        }
        Ok(true)
    }

    fn promote_all_pending_ooo_mmapped(&self) {
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            stripe_buf.clear();
            StripeSeries::collect_stripe_into(stripe, &mut stripe_buf);
            for entry in stripe_buf.drain(..) {
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo_mut() {
                    if !ooo.ooo_pending_mmapped.is_empty() {
                        for p in ooo.ooo_pending_mmapped.drain(..) {
                            push_mmapped_chunk(&mut ooo.ooo_mmapped_chunks, p.mc);
                        }
                        ooo.ooo_pending_mmapped = Vec::new();
                    }
                }
            }
        }
        let mut g = self.ooo_unflushed_series.lock();
        g.clear();
        self.has_ooo_unflushed.store(false, Ordering::Release);
    }

    /// Creates a new [`HeadAppender`] for transactional batched writes.
    pub fn appender(&self) -> HeadAppender<'_> {
        self.appender_with_capacity(0)
    }

    pub fn appender_with_capacity(&self, series_hint: usize) -> HeadAppender<'_> {
        self.metrics.active_appenders.inc();
        self.metrics.appenders_created.inc();
        let min_valid = self.appendable_min_valid_time();
        let head_maxt = self.max_time();
        let (append_id, cleanup_append_ids_below) = if self.iso.is_disabled() {
            (0, 0)
        } else {
            self.iso.new_append_id(min_valid)
        };
        let mut scratch = self.appender_pool.lock().pop().unwrap_or_default();
        if series_hint > 0 && scratch.floats.capacity() < series_hint {
            scratch.floats.reserve(series_hint - scratch.floats.len());
        }
        HeadAppender {
            head: self,
            min_valid_time: min_valid,
            head_maxt,
            ooo_time_window: self.ooo_time_window_ms.load(Ordering::Acquire),
            series_refs: scratch.series_refs,
            created_series: scratch.created_series,
            floats: scratch.floats,
            histograms: scratch.histograms,
            float_histograms: scratch.float_histograms,
            exemplars: scratch.exemplars,
            metadata: scratch.metadata,
            types_in_batch: scratch.types_in_batch,
            batch_boundaries: scratch.batch_boundaries,
            floats_committed: 0,
            hists_committed: 0,
            fhists_committed: 0,
            wal_buf: scratch.wal_buf,
            wal_comp_buf: scratch.wal_comp_buf,
            append_id,
            cleanup_append_ids_below,
            compact_notifier: None,
            closed: false,
        }
    }

    /// Flushes all sealed head chunks (`head_chunks[..len-1]`) to `HeadChunksWriter` (`chunks_head/`).
    pub fn mmap_head_chunks(&self) -> Result<usize, HeadChunksError> {
        self.mmap_head_chunks_inner(true)
    }

    fn mmap_head_chunks_inner(&self, update_mmap_metrics: bool) -> Result<usize, HeadChunksError> {
        let _mmap_guard = self.mmap_mtx.lock();
        let mut count = 0usize;
        let mut max_mmapped_chunks = 0usize;
        let mut pending: Vec<(Arc<SeriesEntry>, Vec<MmappedChunk>)> = Vec::new();
        let mut cw_start_epoch: Option<u64> = None;
        let mut entries = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            entries.clear();
            StripeSeries::collect_stripe_into(stripe, &mut entries);
            for entry in entries.drain(..) {
                // Lock order: entry.inner -> chunk_writer (matching commit()).
                let mut st = entry.inner.lock();
                if st.head_chunks_len() > 1 {
                    let mut writer_guard = self.chunk_writer.lock();
                    let Some(ref mut writer) = *writer_guard else {
                        return Ok(count);
                    };
                    cw_start_epoch.get_or_insert_with(|| writer.discard_epoch());
                    let mmapped = st.write_sealed_head_chunks(entry.ref_id, writer)?;
                    drop(writer_guard);
                    drop(st);
                    if !mmapped.is_empty() {
                        count += mmapped.len();
                        max_mmapped_chunks = max_mmapped_chunks.max(mmapped.len());
                        pending.push((entry, mmapped));
                    }
                }
            }
        }

        if count > 0 {
            {
                let mut writer_guard = self.chunk_writer.lock();
                if let Some(ref mut writer) = *writer_guard {
                    if cw_start_epoch.is_some_and(|e| writer.discard_epoch() != e) {
                        return Err(HeadChunksError::Io(io::Error::other(
                            "unflushed head chunks discarded by prior flush error",
                        )));
                    }
                    writer.flush()?;
                }
            }
            // Refresh mmap reader BEFORE exposing new mmapped_chunks on series state,
            // so concurrent readers never observe a ChunkDiskMapperRef not yet in chunk_reader.
            self.refresh_chunk_reader()?;
            for (entry, mmapped) in pending {
                let mut st = entry.inner.lock();
                st.promote_sealed_head_chunks(mmapped);
            }
        }
        // Go `mmapHeadChunks` only updates these on live runs, not during Head::open WAL replay.
        if update_mmap_metrics {
            self.metrics
                .head_chunks_max_mmapped
                .set(max_mmapped_chunks as f64);
            self.metrics.mmap_chunks_total.inc_by(count as f64);
        }
        Ok(count)
    }

    /// Collects lightweight `(ref_id, Labels)` candidates for streaming head compaction over `[mint, maxt_inclusive]`.
    pub fn collect_compaction_candidates(
        &self,
        mint: i64,
        maxt_inclusive: i64,
    ) -> Result<Vec<(u64, Labels)>, HeadError> {
        let has_tombs = self.tombstones.has_any();
        let mut tombs_buf = Vec::new();
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        let mut out = Vec::with_capacity(self.num_series() as usize);

        for stripe in &self.series.stripes {
            stripe_buf.clear();
            StripeSeries::collect_stripe_into(stripe, &mut stripe_buf);
            for entry in stripe_buf.drain(..) {
                let st = entry.inner.lock();
                if !has_tombs {
                    let has_overlap = st
                        .mmapped()
                        .iter()
                        .any(|mc| mc.overlaps_closed_interval(mint, maxt_inclusive))
                        || st.any_head_chunk_bounds(|hc_min, hc_max| {
                            hc_min <= maxt_inclusive && mint <= hc_max
                        });
                    if has_overlap {
                        out.push((entry.ref_id, entry.labels.clone()));
                    }
                } else {
                    self.tombstones.get_into(entry.ref_id, &mut tombs_buf);
                    let has_overlap = st.mmapped().iter().any(|mc| {
                        mc.overlaps_closed_interval(mint, maxt_inclusive)
                            && !is_completely_deleted(&tombs_buf, mc.min_time, mc.max_time)
                    }) || st.any_head_chunk_bounds(|hc_min, hc_max| {
                        hc_min <= maxt_inclusive
                            && mint <= hc_max
                            && !is_completely_deleted(&tombs_buf, hc_min, hc_max)
                    });
                    if has_overlap {
                        out.push((entry.ref_id, entry.labels.clone()));
                    }
                }
            }
        }
        Ok(out)
    }

    /// Loads in-order chunks overlapping `[mint, maxt_inclusive]` and tombstones for a single `ref_id`
    /// into caller-reused buffers (`out_chunks`, `out_tombs`).
    pub fn load_series_in_order_chunks_into(
        &self,
        ref_id: u64,
        mint: i64,
        maxt_inclusive: i64,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        self.load_series_in_order_chunks_into_inner(
            ref_id,
            mint,
            maxt_inclusive,
            None,
            out_chunks,
            out_tombs,
        )
    }

    /// Loads in-order chunks overlapping `[mint, maxt_inclusive]` and tombstones for `ref_id`,
    /// honoring `iso.evicting` (`skipEvictedSeries`).
    pub fn load_series_in_order_chunks_into_with_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt_inclusive: i64,
        iso: &HeadReadIsolation,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        self.load_series_in_order_chunks_into_inner(
            ref_id,
            mint,
            maxt_inclusive,
            Some(iso),
            out_chunks,
            out_tombs,
        )
    }

    pub fn load_series_in_order_chunks_into_with_tx_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt_inclusive: i64,
        tx_iso: Option<&IsolationStateInfo>,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        self.load_series_in_order_chunks_into_inner_tx(
            ref_id,
            mint,
            maxt_inclusive,
            None,
            tx_iso,
            out_chunks,
            out_tombs,
        )
    }

    fn load_series_in_order_chunks_into_inner(
        &self,
        ref_id: u64,
        mint: i64,
        maxt_inclusive: i64,
        iso: Option<&HeadReadIsolation>,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        self.load_series_in_order_chunks_into_inner_tx(
            ref_id,
            mint,
            maxt_inclusive,
            iso,
            None,
            out_chunks,
            out_tombs,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn load_series_in_order_chunks_into_inner_tx(
        &self,
        ref_id: u64,
        mint: i64,
        maxt_inclusive: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        let Some(entry) = self.series.get_by_id(ref_id) else {
            if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                self.metrics.chunk_reads_of_removed_series.inc();
            }
            return Ok(());
        };
        let effective_tx_iso = tx_iso.filter(|_| !self.iso.is_disabled());
        let (mmapped_refs, head_chks) = {
            let st = entry.inner.lock();
            if entry.is_gced() || iso.is_some_and(|i| i.should_skip_in_order(&entry, &st)) {
                if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                    self.metrics.chunk_reads_of_removed_series.inc();
                }
                return Ok(());
            }
            if effective_tx_iso.is_some() {
                if let Some(s) = self.txs_stripe(ref_id) {
                    let map = s.lock();
                    st.collect_in_order_chunks_for_tx_iso(
                        map.get(&ref_id),
                        mint,
                        maxt_inclusive,
                        effective_tx_iso,
                    )?
                } else {
                    st.collect_in_order_chunks_for_tx_iso(None, mint, maxt_inclusive, effective_tx_iso)?
                }
            } else {
                st.collect_in_order_chunks_for_tx_iso(None, mint, maxt_inclusive, None)?
            }
        };
        for mut hc in head_chks {
            hc.ref_id = 0;
            out_chunks.push(hc);
        }
        if !mmapped_refs.is_empty() {
            if self.is_closed() {
                return Err(HeadChunksError::Closed.into());
            }
            let chunk_reader_guard = self.chunk_reader.read();
            let Some(r) = chunk_reader_guard.as_ref() else {
                return Err(HeadChunksError::Closed.into());
            };
            for (_, mc, stop_after_opt) in mmapped_refs {
                let enc = r.chunk(mc.ref_id)?;
                if let Some(stop_after) = stop_after_opt {
                    if let Some((sliced, smint, smaxt)) = slice_chunk_prefix(&enc, stop_after)? {
                        if smint <= maxt_inclusive && mint <= smaxt {
                            out_chunks.push(ChunkMeta {
                                ref_id: mc.ref_id.as_u64(),
                                min_time: smint,
                                max_time: smaxt,
                                chunk: Some(sliced),
                            });
                        }
                    }
                } else {
                    out_chunks.push(ChunkMeta {
                        ref_id: mc.ref_id.as_u64(),
                        min_time: mc.min_time,
                        max_time: mc.max_time,
                        chunk: Some(enc),
                    });
                }
            }
        }
        if self.tombstones.has_any() {
            self.tombstones.get_into(ref_id, out_tombs);
        }
        Ok(())
    }

    /// Collects lightweight `(ref_id, Labels)` candidates for streaming OOO head compaction.
    /// Returns `(candidates, min_time, max_time_inclusive)`.
    pub fn collect_ooo_compaction_candidates(
        &self,
    ) -> Result<CompactionCandidatesRange, HeadError> {
        let snap = self.collect_ooo_compaction_candidates_with_watermark()?;
        *self.ooo_compact_wbl_cut_seg.lock() = snap.last_wbl_file;
        Ok((snap.candidates, snap.min_time, snap.max_time))
    }

    /// Port of Go `NewOOOCompactionHead` (`ooo_head_read.go:377-458`): cuts a new WBL
    /// segment BEFORE scanning series, mmaps every series' current `ooo_head_chunk`, records
    /// the maximum `ChunkDiskMapperRef` (`last_mmap_ref`), and returns only chunks with
    /// `ref_id <= last_mmap_ref` so concurrent OOO arrivals during compaction are excluded.
    pub fn collect_ooo_compaction_candidates_with_watermark(
        &self,
    ) -> Result<OooCompactionSnapshot, HeadError> {
        let ooo_active = self.ooo_time_window_ms.load(Ordering::Acquire) > 0
            || self.min_ooo_time.load(Ordering::Acquire) != i64::MAX;

        let mut mmapped_any = false;
        let mut any_pending = false;
        let mut last_mmap_ref = ChunkDiskMapperRef(0);
        let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
        let use_hist_st = self.opts.enable_histogram_st_encoding;
        let mut pending_entries: Vec<Arc<SeriesEntry>> = Vec::new();
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);

        // Hold ooo_wbl_gate exclusively across cutting the WBL segment first (matching Go
        // `NewOOOCompactionHead`: `head.wbl.NextSegmentSync()` without writing `MmapMarkers`)
        // and snapshotting `ooo_head_chunk`s so no in-flight `HeadAppender::commit` can log
        // Samples for an already-mmapped chunk into the new WBL segment.
        let (last_wbl_file, flushed_wm, discard_epoch_changed) = {
            let _wbl_gate = self.ooo_wbl_gate.write();
            let initial_discard_epoch = self
                .chunk_writer
                .lock()
                .as_ref()
                .map(|w| w.discard_epoch());

            let last_wbl_file = if ooo_active {
                let mut wbl_guard = self.wbl.lock();
                if let Some(ref mut wbl) = *wbl_guard {
                    Some(wbl.next_segment()?)
                } else {
                    None
                }
            } else {
                None
            };

            for stripe in &self.series.stripes {
                stripe_buf.clear();
                StripeSeries::collect_stripe_into(stripe, &mut stripe_buf);
                for entry in stripe_buf.drain(..) {
                    // Lock order: entry.inner -> chunk_writer (matching commit()).
                    let mut st = entry.inner.lock();
                    if let Some(ref mut ooo) = st.ooo_mut() {
                        let mut pushed_pending = false;
                        if !ooo.ooo_pending_mmapped.is_empty() {
                            let mut writer_guard = self.chunk_writer.lock();
                            if let Some(ref mut writer) = *writer_guard {
                                Self::rewrite_discarded_ooo_pending_under_locks(
                                    entry.ref_id,
                                    ooo,
                                    writer,
                                )?;
                            }
                            any_pending = true;
                            pending_entries.push(Arc::clone(&entry));
                            pushed_pending = true;
                        }
                        if let Some(ref ohc) = ooo.ooo_head_chunk {
                            let cut_chunks = ohc.chunk.to_encoded_chunks(
                                i64::MIN,
                                i64::MAX,
                                use_xor2,
                                use_hist_st,
                            )?;
                            if !cut_chunks.is_empty() {
                                let mut writer_guard = self.chunk_writer.lock();
                                if let Some(ref mut writer) = *writer_guard {
                                    let snap = writer.take_snapshot();
                                    let mut staged_pending: SmallVec<[PendingOooMmap; 4]> =
                                        SmallVec::new();
                                    for cc in cut_chunks {
                                        let r = match writer.write_chunk(
                                            entry.ref_id,
                                            cc.min_time,
                                            cc.max_time,
                                            &cc.chunk,
                                            true,
                                        ) {
                                            Ok(r) => r,
                                            Err(e) => {
                                                writer.rollback_snapshot(snap);
                                                return Err(e.into());
                                            }
                                        };
                                        let mc = MmappedChunk {
                                            ref_id: r,
                                            min_time: cc.min_time,
                                            max_time: cc.max_time,
                                            num_samples: cc.chunk.num_samples() as u16,
                                            encoding: cc.chunk.encoding(),
                                        };
                                        staged_pending.push(PendingOooMmap {
                                            mc,
                                            write_epoch: None,
                                            chunk: cc.chunk,
                                        });
                                    }
                                    let ep = writer.discard_epoch();
                                    for p in &mut staged_pending {
                                        p.write_epoch = Some(ep);
                                    }
                                    drop(writer_guard);
                                    ooo.ooo_head_chunk = None;
                                    ooo.ooo_pending_mmapped.extend(staged_pending);
                                    self.mark_ooo_unflushed(entry.ref_id);
                                    mmapped_any = true;
                                    if !pushed_pending {
                                        pending_entries.push(Arc::clone(&entry));
                                    }
                                }
                            } else {
                                ooo.ooo_head_chunk = None;
                            }
                        }
                        for mc in &ooo.ooo_mmapped_chunks {
                            if mc.ref_id > last_mmap_ref {
                                last_mmap_ref = mc.ref_id;
                            }
                        }
                        for p in &ooo.ooo_pending_mmapped {
                            any_pending = true;
                            if p.mc.ref_id.as_u64() != 0 && p.mc.ref_id > last_mmap_ref {
                                last_mmap_ref = p.mc.ref_id;
                            }
                        }
                    }
                }
            }

            let (flushed_wm, discard_epoch_changed) = if mmapped_any || any_pending {
                let mut writer_guard = self.chunk_writer.lock();
                if let Some(ref mut writer) = *writer_guard {
                    writer.flush()?;
                    let changed = initial_discard_epoch != Some(writer.discard_epoch());
                    (Some(writer.flushed_watermark()), changed)
                } else {
                    (None, true)
                }
            } else {
                (None, false)
            };

            (last_wbl_file, flushed_wm, discard_epoch_changed)
        };

        let mut any_unpromoted = false;
        if !pending_entries.is_empty() {
            if let Some(ref wm) = flushed_wm {
                self.refresh_chunk_reader()?;
                for entry in pending_entries {
                    let mut st = entry.inner.lock();
                    if let Some(ref mut ooo) = st.ooo_mut() {
                        if ooo.promote_pending_with_watermark(wm) {
                            self.clear_ooo_unflushed(entry.ref_id);
                        } else {
                            any_unpromoted = true;
                        }
                    }
                }
            } else {
                any_unpromoted = true;
            }
        }
        if any_unpromoted || discard_epoch_changed {
            return Err(HeadError::Io(io::Error::other(
                "pending OOO chunks discarded during OOO compaction scan",
            )));
        }

        let mut out = Vec::new();
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        if last_mmap_ref.as_u64() != 0 {
            for stripe in &self.series.stripes {
                stripe_buf.clear();
                StripeSeries::collect_stripe_into(stripe, &mut stripe_buf);
                for entry in stripe_buf.drain(..) {
                    let st = entry.inner.lock();
                    let Some(ooo) = st.ooo() else {
                        continue;
                    };
                    let mut has_any = false;
                    for mc in &ooo.ooo_mmapped_chunks {
                        if mc.ref_id <= last_mmap_ref {
                            mint = mint.min(mc.min_time);
                            maxt = maxt.max(mc.max_time);
                            has_any = true;
                        }
                    }
                    for p in &ooo.ooo_pending_mmapped {
                        if p.mc.ref_id.as_u64() != 0 && p.mc.ref_id <= last_mmap_ref {
                            mint = mint.min(p.mc.min_time);
                            maxt = maxt.max(p.mc.max_time);
                            has_any = true;
                        }
                    }
                    if has_any {
                        out.push((entry.ref_id, entry.labels.clone()));
                    }
                }
            }
        }

        Ok(OooCompactionSnapshot {
            candidates: out,
            min_time: mint,
            max_time: maxt,
            last_mmap_ref,
            last_wbl_file,
        })
    }

    /// Loads OOO chunks and tombstones for a single `ref_id` into caller-reused buffers.
    pub fn load_series_ooo_chunks_into(
        &self,
        ref_id: u64,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        self.load_series_ooo_chunks_into_up_to(
            ref_id,
            ChunkDiskMapperRef(0),
            out_chunks,
            out_tombs,
        )
    }

    /// Loads OOO chunks up to `max_mmap_ref` (when non-zero, matching Go
    /// `getOOOSeriesChunks`, `ooo_head_read.go:114, 132`: ignores `ooo_head_chunk` and any
    /// `ooo_mmapped_chunks` / `ooo_pending_mmapped` with `ref_id > max_mmap_ref`).
    pub fn load_series_ooo_chunks_into_up_to(
        &self,
        ref_id: u64,
        max_mmap_ref: ChunkDiskMapperRef,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        let Some(entry) = self.series.get_by_id(ref_id) else {
            return Ok(());
        };
        const OOO_HEAD_CHUNK_REF_BIT: u64 = 1u64 << 31;
        let mut mmapped_refs: SmallVec<[(u64, MmappedChunk); 8]> = SmallVec::new();
        {
            let st = entry.inner.lock();
            let Some(ooo) = st.ooo() else {
                return Ok(());
            };
            let mut ooo_pos = 0u64;
            for mc in &ooo.ooo_mmapped_chunks {
                if max_mmap_ref.as_u64() == 0 || mc.ref_id <= max_mmap_ref {
                    mmapped_refs.push((OOO_HEAD_CHUNK_REF_BIT | ooo_pos, *mc));
                }
                ooo_pos += 1;
            }
            for p in &ooo.ooo_pending_mmapped {
                if max_mmap_ref.as_u64() == 0
                    || (p.mc.ref_id.as_u64() != 0 && p.mc.ref_id <= max_mmap_ref)
                {
                    out_chunks.push(ChunkMeta {
                        ref_id: OOO_HEAD_CHUNK_REF_BIT | ooo_pos,
                        min_time: p.mc.min_time,
                        max_time: p.mc.max_time,
                        chunk: Some(p.chunk.clone()),
                    });
                }
                ooo_pos += 1;
            }
            if max_mmap_ref.as_u64() == 0 {
                if let Some(ref ohc) = ooo.ooo_head_chunk {
                    let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
                    let use_hist_st = self.opts.enable_histogram_st_encoding;
                    let head_ref = OOO_HEAD_CHUNK_REF_BIT | ooo_pos;
                    for cc in ohc
                        .chunk
                        .to_encoded_chunks(i64::MIN, i64::MAX, use_xor2, use_hist_st)?
                    {
                        out_chunks.push(ChunkMeta {
                            ref_id: head_ref,
                            min_time: cc.min_time,
                            max_time: cc.max_time,
                            chunk: Some(cc.chunk),
                        });
                    }
                }
            }
        }
        if !mmapped_refs.is_empty() {
            if self.is_closed() {
                return Err(HeadChunksError::Closed.into());
            }
            let chunk_reader_guard = self.chunk_reader.read();
            let Some(r) = chunk_reader_guard.as_ref() else {
                return Err(HeadChunksError::Closed.into());
            };
            for (sort_ref, mc) in mmapped_refs {
                let enc = r.chunk(mc.ref_id)?;
                out_chunks.push(ChunkMeta {
                    ref_id: sort_ref,
                    min_time: mc.min_time,
                    max_time: mc.max_time,
                    chunk: Some(enc),
                });
            }
        }
        if self.tombstones.has_any() {
            self.tombstones.get_into(ref_id, out_tombs);
        }
        Ok(())
    }

    /// Collects lightweight `(ref_id, Labels)` candidates for streaming selected-series compaction.
    pub fn collect_selected_compaction_candidates(
        &self,
        refs: &[u64],
    ) -> Result<CompactionCandidatesRange, HeadError> {
        let (range, _) = self.collect_selected_compaction_candidates_with_fingerprints(refs)?;
        Ok(range)
    }

    /// Collects candidates for selected-series compaction along with per-series chunk-shape
    /// fingerprints (Go `Head.snapshotFingerprints`) captured under each series lock.
    pub fn collect_selected_compaction_candidates_with_fingerprints(
        &self,
        refs: &[u64],
    ) -> Result<(CompactionCandidatesRange, HashMap<u64, SeriesFingerprint>), HeadError> {
        self.collect_selected_compaction_candidates_with_watermark_and_fingerprints(refs, u64::MAX)
    }

    pub fn collect_selected_compaction_candidates_with_watermark_and_fingerprints(
        &self,
        refs: &[u64],
        append_id_watermark: u64,
    ) -> Result<(CompactionCandidatesRange, HashMap<u64, SeriesFingerprint>), HeadError> {
        let mut out = Vec::with_capacity(refs.len());
        let mut fingerprints = HashMap::with_capacity(refs.len());
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        for &ref_id in refs {
            let Some(entry) = self.series.get_by_id(ref_id) else {
                continue;
            };
            let st = entry.inner.lock();
            if st.ooo().is_some_and(|o| {
                o.ooo_head_chunk.is_some()
                    || !o.ooo_mmapped_chunks.is_empty()
                    || !o.ooo_pending_mmapped.is_empty()
            }) {
                continue;
            }
            let fp = if append_id_watermark < u64::MAX {
                if let Some(s) = self.txs_stripe(entry.ref_id) {
                    let map = s.lock();
                    SeriesFingerprint::capture(
                        &entry,
                        &st,
                        map.get(&entry.ref_id),
                        append_id_watermark,
                    )
                } else {
                    SeriesFingerprint::capture(&entry, &st, None, append_id_watermark)
                }
            } else {
                SeriesFingerprint::capture(&entry, &st, None, append_id_watermark)
            };
            fingerprints.insert(entry.ref_id, fp);
            let mut has_any = false;
            for mc in st.mmapped() {
                mint = mint.min(mc.min_time);
                maxt = maxt.max(mc.max_time);
                has_any = true;
            }
            st.for_each_head_chunk_bounds(|hc_min, hc_max| {
                mint = mint.min(hc_min);
                maxt = maxt.max(hc_max);
                has_any = true;
            });
            if has_any {
                out.push((entry.ref_id, entry.labels.clone()));
            }
        }
        Ok(((out, mint, maxt), fingerprints))
    }

    /// Returns sorted, merged closed `[min_time, max_time]` intervals of all in-order chunks
    /// belonging to `candidates` so selected-series compaction can skip empty gaps in O(1).
    pub(crate) fn collect_selected_compaction_intervals(
        &self,
        candidates: &[(u64, Labels)],
        range_ms: i64,
    ) -> Vec<(i64, i64)> {
        let mut raw = Vec::new();
        let range128 = range_ms.max(1) as i128;
        let mut oversized_mmapped: Vec<MmappedChunk> = Vec::new();
        for &(ref_id, _) in candidates {
            let Some(entry) = self.series.get_by_id(ref_id) else {
                continue;
            };
            let st = entry.inner.lock();
            for mc in st.mmapped() {
                if (mc.max_time as i128 - mc.min_time as i128) > range128 {
                    oversized_mmapped.push(*mc);
                } else {
                    raw.push((mc.min_time, mc.max_time));
                }
            }
            st.for_each_head_chunk_cloned(|hc_chunk, hc_min, hc_max| {
                if (hc_max as i128 - hc_min as i128) > range128 {
                    if let Ok(samples) = decode_chunk_samples(&hc_chunk) {
                        for s in samples {
                            raw.push((s.t, s.t));
                        }
                        return;
                    }
                }
                raw.push((hc_min, hc_max));
            });
        }
        if !oversized_mmapped.is_empty() {
            // Lock order: drop all `entry.inner` and `series` stripe locks before taking
            // `chunk_reader.read()` as a leaf lock so we never invert against `entry.inner`
            // -> `chunk_reader.read()` and concurrent `chunk_reader.write()` callers.
            let cr_guard = self.chunk_reader.read();
            for mc in oversized_mmapped {
                if let Some(ref cr) = *cr_guard {
                    if let Ok(enc) = cr.chunk(mc.ref_id) {
                        if let Ok(samples) = decode_chunk_samples(&enc) {
                            for s in samples {
                                raw.push((s.t, s.t));
                            }
                            continue;
                        }
                    }
                }
                raw.push((mc.min_time, mc.max_time));
            }
        }
        merge_time_intervals(raw)
    }

    /// Returns sorted, merged closed `[min_time, max_time]` intervals of OOO chunks up to
    /// `max_mmap_ref` belonging to `candidates` so OOO compaction can skip empty gaps in O(1).
    pub(crate) fn collect_ooo_compaction_intervals(
        &self,
        candidates: &[(u64, Labels)],
        max_mmap_ref: ChunkDiskMapperRef,
        range_ms: i64,
    ) -> Vec<(i64, i64)> {
        let mut raw = Vec::new();
        if max_mmap_ref.as_u64() == 0 {
            return raw;
        }
        let range128 = range_ms.max(1) as i128;
        let mut oversized_mmapped: Vec<MmappedChunk> = Vec::new();
        for &(ref_id, _) in candidates {
            let Some(entry) = self.series.get_by_id(ref_id) else {
                continue;
            };
            let st = entry.inner.lock();
            let Some(ooo) = st.ooo() else {
                continue;
            };
            for mc in &ooo.ooo_mmapped_chunks {
                if mc.ref_id <= max_mmap_ref {
                    if (mc.max_time as i128 - mc.min_time as i128) > range128 {
                        oversized_mmapped.push(*mc);
                    } else {
                        raw.push((mc.min_time, mc.max_time));
                    }
                }
            }
            for p in &ooo.ooo_pending_mmapped {
                if p.mc.ref_id.as_u64() != 0 && p.mc.ref_id <= max_mmap_ref {
                    if (p.mc.max_time as i128 - p.mc.min_time as i128) > range128 {
                        if let Ok(samples) = decode_chunk_samples(&p.chunk) {
                            for s in samples {
                                raw.push((s.t, s.t));
                            }
                            continue;
                        }
                    }
                    raw.push((p.mc.min_time, p.mc.max_time));
                }
            }
        }
        if !oversized_mmapped.is_empty() {
            // Lock order: drop all `entry.inner` and `series` stripe locks before taking
            // `chunk_reader.read()` as a leaf lock.
            let cr_guard = self.chunk_reader.read();
            for mc in oversized_mmapped {
                if let Some(ref cr) = *cr_guard {
                    if let Ok(enc) = cr.chunk(mc.ref_id) {
                        if let Ok(samples) = decode_chunk_samples(&enc) {
                            for s in samples {
                                raw.push((s.t, s.t));
                            }
                            continue;
                        }
                    }
                }
                raw.push((mc.min_time, mc.max_time));
            }
        }
        merge_time_intervals(raw)
    }

    /// Collects all series with chunks overlapping `[mint, maxt_inclusive]` for block compaction.
    pub fn collect_compaction_series(
        &self,
        mint: i64,
        maxt_inclusive: i64,
    ) -> Result<Vec<CompactionSeries>, HeadError> {
        let entries = self.series.all_entries();

        struct ExtractedSeries {
            entry: Arc<SeriesEntry>,
            mmapped_refs: SmallVec<[MmappedChunk; 8]>,
            head_chks: Vec<ChunkMeta>,
        }
        let mut extracted = Vec::with_capacity(entries.len());
        let mut any_mmapped = false;

        for entry in entries {
            let st = entry.inner.lock();
            let mut mmapped_refs: SmallVec<[MmappedChunk; 8]> = SmallVec::new();
            for mc in st.mmapped() {
                if mc.overlaps_closed_interval(mint, maxt_inclusive) {
                    mmapped_refs.push(*mc);
                    any_mmapped = true;
                }
            }
            let mut head_chks = Vec::new();
            st.for_each_head_chunk_cloned(|hc_chunk, hc_min, hc_max| {
                if hc_min <= maxt_inclusive && mint <= hc_max {
                    head_chks.push(ChunkMeta {
                        ref_id: 0,
                        min_time: hc_min,
                        max_time: hc_max,
                        chunk: Some(hc_chunk),
                    });
                }
            });
            if !mmapped_refs.is_empty() || !head_chks.is_empty() {
                drop(st);
                extracted.push(ExtractedSeries {
                    entry,
                    mmapped_refs,
                    head_chks,
                });
            }
        }

        let mut out = Vec::with_capacity(extracted.len());
        let chunk_reader_guard = if any_mmapped {
            Some(self.chunk_reader.read())
        } else {
            None
        };
        let chunk_reader = chunk_reader_guard.as_ref().and_then(|g| g.as_ref());

        for ex in extracted {
            let mut chks = Vec::with_capacity(ex.mmapped_refs.len() + ex.head_chks.len());
            if !ex.mmapped_refs.is_empty() {
                if self.is_closed() {
                    return Err(HeadChunksError::Closed.into());
                }
                let Some(r) = chunk_reader else {
                    return Err(HeadChunksError::Closed.into());
                };
                for mc in ex.mmapped_refs {
                    let enc = r.chunk(mc.ref_id)?;
                    chks.push(ChunkMeta {
                        ref_id: mc.ref_id.as_u64(),
                        min_time: mc.min_time,
                        max_time: mc.max_time,
                        chunk: Some(enc),
                    });
                }
            }
            chks.extend(ex.head_chks);
            if !chks.is_empty() {
                let tombs = self.tombstones.get(ex.entry.ref_id);
                out.push(CompactionSeries {
                    labels: ex.entry.labels.clone(),
                    chunks: chks,
                    tombstones: tombs,
                });
            }
        }

        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        Ok(out)
    }

    /// Collects all out-of-order chunks across all series in Head for OOO block compaction.
    /// Returns `(series, min_time, max_time_inclusive)`.
    pub fn collect_ooo_compaction_series(
        &self,
    ) -> Result<(Vec<CompactionSeries>, i64, i64), HeadError> {
        let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
        let use_hist_st = self.opts.enable_histogram_st_encoding;
        let mut mmapped_any = false;
        let mut pending_entries: Vec<Arc<SeriesEntry>> = Vec::new();
        let entries = self.series.all_entries();

        let flushed_wm = {
            let _wbl_gate = self.ooo_wbl_gate.write();

            {
                let mut wbl_guard = self.wbl.lock();
                if let Some(ref mut wbl) = *wbl_guard {
                    let next_seg = wbl.next_segment()?;
                    *self.ooo_compact_wbl_cut_seg.lock() = Some(next_seg);
                }
            }

            for entry in &entries {
                // Lock order: entry.inner -> chunk_writer (matching commit()).
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo_mut() {
                    let mut pushed_pending = false;
                    if !ooo.ooo_pending_mmapped.is_empty() {
                        let mut writer_guard = self.chunk_writer.lock();
                        if let Some(ref mut writer) = *writer_guard {
                            Self::rewrite_discarded_ooo_pending_under_locks(
                                entry.ref_id,
                                ooo,
                                writer,
                            )?;
                        }
                        mmapped_any = true;
                        pending_entries.push(Arc::clone(entry));
                        pushed_pending = true;
                    }
                    if let Some(ref ohc) = ooo.ooo_head_chunk {
                        let cut_chunks = ohc.chunk.to_encoded_chunks(
                            i64::MIN,
                            i64::MAX,
                            use_xor2,
                            use_hist_st,
                        )?;
                        if !cut_chunks.is_empty() {
                            let mut writer_guard = self.chunk_writer.lock();
                            if let Some(ref mut writer) = *writer_guard {
                                let snap = writer.take_snapshot();
                                let mut staged_pending: SmallVec<[PendingOooMmap; 4]> =
                                    SmallVec::new();
                                for cc in cut_chunks {
                                    let r = match writer.write_chunk(
                                        entry.ref_id,
                                        cc.min_time,
                                        cc.max_time,
                                        &cc.chunk,
                                        true,
                                    ) {
                                        Ok(r) => r,
                                        Err(e) => {
                                            writer.rollback_snapshot(snap);
                                            return Err(e.into());
                                        }
                                    };
                                    let mc = MmappedChunk {
                                        ref_id: r,
                                        min_time: cc.min_time,
                                        max_time: cc.max_time,
                                        num_samples: cc.chunk.num_samples() as u16,
                                        encoding: cc.chunk.encoding(),
                                    };
                                    staged_pending.push(PendingOooMmap {
                                        mc,
                                        write_epoch: None,
                                        chunk: cc.chunk,
                                    });
                                }
                                let ep = writer.discard_epoch();
                                for p in &mut staged_pending {
                                    p.write_epoch = Some(ep);
                                }
                                drop(writer_guard);
                                ooo.ooo_head_chunk = None;
                                ooo.ooo_pending_mmapped.extend(staged_pending);
                                self.mark_ooo_unflushed(entry.ref_id);
                                mmapped_any = true;
                                if !pushed_pending {
                                    pending_entries.push(Arc::clone(entry));
                                }
                            }
                        } else {
                            ooo.ooo_head_chunk = None;
                        }
                    }
                }
            }

            if mmapped_any {
                let mut writer_guard = self.chunk_writer.lock();
                if let Some(ref mut writer) = *writer_guard {
                    writer.flush()?;
                    Some(writer.flushed_watermark())
                } else {
                    None
                }
            } else {
                None
            }
        };

        if !pending_entries.is_empty() {
            if let Some(ref wm) = flushed_wm {
                self.refresh_chunk_reader()?;
                for entry in pending_entries {
                    let mut st = entry.inner.lock();
                    if let Some(ref mut ooo) = st.ooo_mut() {
                        if ooo.promote_pending_with_watermark(wm) {
                            self.clear_ooo_unflushed(entry.ref_id);
                        }
                    }
                }
            }
        }

        struct ExtractedOoo {
            entry: Arc<SeriesEntry>,
            mmapped_refs: SmallVec<[MmappedChunk; 8]>,
            inline_chks: Vec<ChunkMeta>,
        }
        let mut extracted = Vec::new();
        let mut any_mmapped = false;
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        for entry in entries {
            let st = entry.inner.lock();
            let Some(ooo) = st.ooo() else {
                continue;
            };
            let mut mmapped_refs: SmallVec<[MmappedChunk; 8]> = SmallVec::new();
            for mc in &ooo.ooo_mmapped_chunks {
                mmapped_refs.push(*mc);
                any_mmapped = true;
            }
            let mut inline_chks = Vec::new();
            for p in &ooo.ooo_pending_mmapped {
                mint = mint.min(p.mc.min_time);
                maxt = maxt.max(p.mc.max_time);
                inline_chks.push(ChunkMeta {
                    ref_id: p.mc.ref_id.as_u64(),
                    min_time: p.mc.min_time,
                    max_time: p.mc.max_time,
                    chunk: Some(p.chunk.clone()),
                });
            }
            if let Some(ref ohc) = ooo.ooo_head_chunk {
                for cc in ohc
                    .chunk
                    .to_encoded_chunks(i64::MIN, i64::MAX, use_xor2, use_hist_st)?
                {
                    mint = mint.min(cc.min_time);
                    maxt = maxt.max(cc.max_time);
                    inline_chks.push(ChunkMeta {
                        ref_id: 0,
                        min_time: cc.min_time,
                        max_time: cc.max_time,
                        chunk: Some(cc.chunk),
                    });
                }
            }
            if !mmapped_refs.is_empty() || !inline_chks.is_empty() {
                drop(st);
                extracted.push(ExtractedOoo {
                    entry,
                    mmapped_refs,
                    inline_chks,
                });
            }
        }

        let chunk_reader_guard = if any_mmapped {
            Some(self.chunk_reader.read())
        } else {
            None
        };
        let chunk_reader = chunk_reader_guard.as_ref().and_then(|g| g.as_ref());

        let mut out = Vec::with_capacity(extracted.len());
        for ex in extracted {
            let mut chks = Vec::with_capacity(ex.mmapped_refs.len() + ex.inline_chks.len());
            if !ex.mmapped_refs.is_empty() {
                if self.is_closed() {
                    return Err(HeadChunksError::Closed.into());
                }
                let Some(r) = chunk_reader else {
                    return Err(HeadChunksError::Closed.into());
                };
                for mc in ex.mmapped_refs {
                    let enc = r.chunk(mc.ref_id)?;
                    mint = mint.min(mc.min_time);
                    maxt = maxt.max(mc.max_time);
                    chks.push(ChunkMeta {
                        ref_id: mc.ref_id.as_u64(),
                        min_time: mc.min_time,
                        max_time: mc.max_time,
                        chunk: Some(enc),
                    });
                }
            }
            chks.extend(ex.inline_chks);
            if !chks.is_empty() {
                let tombs = self.tombstones.get(ex.entry.ref_id);
                out.push(CompactionSeries {
                    labels: ex.entry.labels.clone(),
                    chunks: chks,
                    tombstones: tombs,
                });
            }
        }

        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        Ok((out, mint, maxt))
    }

    /// Clears all out-of-order chunks from Head memory after OOO compaction and cleans up series
    /// that now have neither in-order nor OOO chunks.
    pub fn clear_ooo_data(&self) -> Result<(), HeadError> {
        self.clear_ooo_data_before(i64::MAX)
    }

    /// Port of Go `Head.truncateOOO` (`head.go:1946-1969, 3572-3586`):
    /// drops only `ooo_mmapped_chunks` / `ooo_pending_mmapped` with `ref_id <= min_ooo_mmap_ref`,
    /// never touching `ooo_head_chunk` or newer OOO chunks, truncates WBL segments `< last_wbl_file`,
    /// deletes now-empty series, and truncates unused `chunks_head` files.
    pub fn truncate_ooo(
        &self,
        last_wbl_file: Option<usize>,
        min_ooo_mmap_ref: ChunkDiskMapperRef,
    ) -> Result<(), HeadError> {
        if last_wbl_file.is_none() && min_ooo_mmap_ref.as_u64() == 0 {
            return Ok(());
        }
        let _snap_guard = self.chunk_snapshot_mtx.lock();
        if min_ooo_mmap_ref.as_u64() > 0 {
            let old_min_ooo = self.min_ooo_time();
            let old_max_ooo = self.max_ooo_time();
            if old_min_ooo != i64::MAX && old_min_ooo <= old_max_ooo {
                let wait_maxt = old_max_ooo.min(i64::MAX - 1) + 1;
                self.active_reads.wait_for_range(old_min_ooo, wait_maxt);
            }
            let _mmap_guard = self.mmap_mtx.lock();
            // Go `truncateSeriesAndChunkDiskMapper("truncateOOO")` times `Head.gc()`.
            let gc_start = Instant::now();
            let min_valid_time = self.min_valid_time.load(Ordering::Acquire);
            let head_maxt = self.max_time();
            let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
            let mut to_delete: Vec<Arc<SeriesEntry>> = Vec::new();
            let mut min_mmap_seq = usize::MAX;
            let mut actual_mint = i64::MAX;
            let mut rem_ooo_mint = i64::MAX;
            let mut chunks_removed = 0u64;
            let wm = self
                .chunk_writer
                .lock()
                .as_ref()
                .map(|cw| cw.flushed_watermark());

            for stripe in &self.series.stripes {
                stripe_buf.clear();
                StripeSeries::collect_stripe_into(stripe, &mut stripe_buf);
                for entry in stripe_buf.drain(..) {
                    let mut st = entry.inner.lock();
                    if let Some(ref mut ooo) = st.ooo_mut() {
                        let before =
                            ooo.ooo_mmapped_chunks.len() + ooo.ooo_pending_mmapped.len();
                        ooo.ooo_mmapped_chunks
                            .retain(|mc| mc.ref_id > min_ooo_mmap_ref);
                        ooo.ooo_pending_mmapped.retain(|p| {
                            let is_flushed = wm
                                .as_ref()
                                .is_none_or(|w| w.is_flushed(p.mc.ref_id, p.write_epoch));
                            !(is_flushed && p.mc.ref_id <= min_ooo_mmap_ref)
                        });
                        if let Some(ref w) = wm {
                            for p in &mut ooo.ooo_pending_mmapped {
                                if w.is_discarded(p.mc.ref_id, p.write_epoch) {
                                    p.write_epoch = None;
                                    p.mc.ref_id = ChunkDiskMapperRef(0);
                                }
                            }
                        }
                        if ooo.ooo_pending_mmapped.is_empty() {
                            self.clear_ooo_unflushed(entry.ref_id);
                        }
                        chunks_removed += (before
                            - ooo.ooo_mmapped_chunks.len()
                            - ooo.ooo_pending_mmapped.len())
                            as u64;
                    }
                    st.clear_ooo_if_empty();
                    st.maybe_drop_cold();

                    let has_ooo = st.ooo().is_some();
                    let has_recent_exemplar =
                        st.last_exemplar_ts().is_some_and(|ts| ts >= min_valid_time);
                    if !has_ooo
                        && st.mmapped().is_empty()
                        && st.head_chunks_is_empty()
                        && !has_recent_exemplar
                        && self.series.remove_if_gced_under_entry_lock(&entry, &mut st)
                    {
                        self.update_metrics_on_series_delete(&st);
                        drop(st);
                        to_delete.push(entry);
                        continue;
                    }

                    let sm = st.min_time();
                    if sm < actual_mint {
                        actual_mint = sm;
                    }
                    for mc in st.mmapped() {
                        let (seq, _) = mc.ref_id.unpack();
                        min_mmap_seq = min_mmap_seq.min(seq as usize);
                    }
                    if let Some(ooo) = st.ooo() {
                        for mc in &ooo.ooo_mmapped_chunks {
                            let (seq, _) = mc.ref_id.unpack();
                            min_mmap_seq = min_mmap_seq.min(seq as usize);
                            rem_ooo_mint = rem_ooo_mint.min(mc.min_time);
                        }
                        for p in &ooo.ooo_pending_mmapped {
                            let is_discarded = wm
                                .as_ref()
                                .is_some_and(|w| w.is_discarded(p.mc.ref_id, p.write_epoch));
                            if !is_discarded && p.mc.ref_id.as_u64() != 0 {
                                let (seq, _) = p.mc.ref_id.unpack();
                                min_mmap_seq = min_mmap_seq.min(seq as usize);
                            }
                            rem_ooo_mint = rem_ooo_mint.min(p.mc.min_time);
                        }
                        if let Some(ref ohc) = ooo.ooo_head_chunk {
                            rem_ooo_mint = rem_ooo_mint.min(ohc.min_time);
                        }
                    }
                }
            }

            if chunks_removed > 0 {
                self.metrics.chunks_removed(chunks_removed);
            }

            let ooo_window = self.ooo_time_window_ms.load(Ordering::Acquire);
            let mut min_ooo_time = rem_ooo_mint;
            if head_maxt.wrapping_sub(ooo_window) < min_ooo_time {
                min_ooo_time = head_maxt.wrapping_sub(ooo_window);
            }
            self.min_ooo_time.store(min_ooo_time, Ordering::Release);

            if !to_delete.is_empty() {
                // Go `Head.gc()`: keep deleted series records in WAL checkpoints through
                // `actual_mint` (`min_time` if no series remain in Head).
                let keep_until = if actual_mint == i64::MAX {
                    self.min_time()
                } else {
                    actual_mint
                };
                let pairs: Vec<(Arc<SeriesEntry>, i64)> = to_delete
                    .into_iter()
                    .map(|e| (e, if keep_until == i64::MAX { i64::MIN } else { keep_until }))
                    .collect();
                self.finish_deleted_series_cleanup(pairs);
                self.series.shrink_oversized_stripes();
                self.maybe_gc_symbols();
            }
            self.metrics
                .gc_duration
                .observe(gc_start.elapsed().as_secs_f64());

            self.truncate_chunks_head(min_mmap_seq)?;
        }

        if let Some(cut_seg) = last_wbl_file {
            if let Some(ref mut wbl) = *self.wbl.lock() {
                wbl.truncate(cut_seg)?;
            }
            // Go `Head.truncateOOO`: truncated segments held every WBL record from before
            // the last replay; release pinned refs.
            self.wbl_pinned_refs.lock().clear();
        }
        Ok(())
    }

    /// Clears out-of-order chunks/samples `<= maxt_inclusive` from Head memory (preserving any OOO
    /// samples `> maxt_inclusive` that arrived during compaction), truncates WBL segments, and
    /// cleans up empty series and unused `chunks_head` files.
    pub fn clear_ooo_data_before(&self, maxt_inclusive: i64) -> Result<(), HeadError> {
        let _snap_guard = self.chunk_snapshot_mtx.lock();
        let old_min_ooo = self.min_ooo_time();
        let old_max_ooo = self.max_ooo_time().min(maxt_inclusive);
        if old_min_ooo != i64::MAX && old_min_ooo <= old_max_ooo {
            let wait_maxt = old_max_ooo.min(i64::MAX - 1) + 1;
            self.active_reads.wait_for_range(old_min_ooo, wait_maxt);
        }
        let _mmap_guard = self.mmap_mtx.lock();
        let min_valid_time = self.min_valid_time.load(Ordering::Acquire);
        let head_maxt = self.max_time();
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        let mut to_delete: Vec<(Arc<SeriesEntry>, i64)> = Vec::new();
        let mut min_mmap_seq = usize::MAX;
        let mut rem_ooo_mint = i64::MAX;

        let record_series_refs =
            |st: &SeriesState, min_mmap_seq: &mut usize, rem_mint: &mut i64| {
                for mc in st.mmapped() {
                    let (seq, _) = mc.ref_id.unpack();
                    *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                }
                if let Some(ooo) = st.ooo() {
                    for mc in &ooo.ooo_mmapped_chunks {
                        let (seq, _) = mc.ref_id.unpack();
                        *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                        *rem_mint = (*rem_mint).min(mc.min_time);
                    }
                    for p in &ooo.ooo_pending_mmapped {
                        if p.mc.ref_id.as_u64() != 0 && p.write_epoch.is_some() {
                            let (seq, _) = p.mc.ref_id.unpack();
                            *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                        }
                        *rem_mint = (*rem_mint).min(p.mc.min_time);
                    }
                    if let Some(ref ohc) = ooo.ooo_head_chunk {
                        *rem_mint = (*rem_mint).min(ohc.min_time);
                    }
                }
            };

        for stripe in &self.series.stripes {
            stripe_buf.clear();
            StripeSeries::collect_stripe_into(stripe, &mut stripe_buf);
            for entry in stripe_buf.drain(..) {
                let mut st = entry.inner.lock();
                if entry.pending_commits_count() > 0 {
                    // Series has an in-flight append: do not drop OOO data, and still count its
                    // chunks_head sequence numbers so chunk files are never deleted prematurely.
                    record_series_refs(&st, &mut min_mmap_seq, &mut rem_ooo_mint);
                    continue;
                }

                if let Some(ref mut ooo) = st.ooo_mut() {
                    let before_mm = ooo.ooo_mmapped_chunks.len();
                    let before = before_mm + ooo.ooo_pending_mmapped.len();
                    ooo.ooo_mmapped_chunks
                        .retain(|mc| mc.max_time > maxt_inclusive);
                    let removed_mm = before_mm - ooo.ooo_mmapped_chunks.len();
                    if removed_mm > 0 && ooo.first_ooo_chunk_id != 0 {
                        ooo.first_ooo_chunk_id = ((ooo.first_ooo_chunk_id as usize + removed_mm)
                            % (OOO_CHUNK_ID_MASK as usize)) as u32;
                    }
                    ooo.ooo_pending_mmapped
                        .retain(|p| p.mc.max_time > maxt_inclusive);
                    if ooo.ooo_pending_mmapped.is_empty() {
                        self.clear_ooo_unflushed(entry.ref_id);
                    }
                    // Go `truncateChunksBefore`: removed OOO m-mapped chunks.
                    self.metrics.chunks_removed(
                        (before - ooo.ooo_mmapped_chunks.len() - ooo.ooo_pending_mmapped.len())
                            as u64,
                    );
                    if let Some(ref mut ohc) = ooo.ooo_head_chunk {
                        if ohc.max_time <= maxt_inclusive {
                            ooo.ooo_head_chunk = None;
                        } else if ohc.min_time <= maxt_inclusive {
                            if let Some((nmin, nmax)) = ohc.chunk.retain_after(maxt_inclusive) {
                                ohc.min_time = nmin;
                                ohc.max_time = nmax;
                            } else {
                                ooo.ooo_head_chunk = None;
                            }
                        }
                    }
                }
                st.clear_ooo_if_empty();
                st.maybe_drop_cold();

                let has_ooo = st.ooo().is_some();
                let has_recent_exemplar =
                    st.last_exemplar_ts().is_some_and(|ts| ts >= min_valid_time);
                if !has_ooo
                    && st.mmapped().is_empty()
                    && st.head_chunks_is_empty()
                    && !has_recent_exemplar
                {
                    let keep_until = st.max_time();
                    if self.series.remove_if_gced_under_entry_lock(&entry, &mut st) {
                        self.update_metrics_on_series_delete(&st);
                        drop(st);
                        to_delete.push((entry, keep_until));
                        continue;
                    }
                }
                record_series_refs(&st, &mut min_mmap_seq, &mut rem_ooo_mint);
            }
        }

        let ooo_window = self.ooo_time_window_ms.load(Ordering::Acquire);
        let mut min_ooo_time = rem_ooo_mint;
        if head_maxt.wrapping_sub(ooo_window) < min_ooo_time {
            min_ooo_time = head_maxt.wrapping_sub(ooo_window);
        }
        self.min_ooo_time.store(min_ooo_time, Ordering::Release);

        let cut_seg_opt = self.ooo_compact_wbl_cut_seg.lock().take();
        if let Some(ref mut wbl) = *self.wbl.lock() {
            if let Some(cut_seg) = cut_seg_opt {
                wbl.truncate(cut_seg)?;
            } else if rem_ooo_mint == i64::MAX {
                let next_seg = wbl.next_segment()?;
                wbl.truncate(next_seg)?;
            }
        }
        if rem_ooo_mint == i64::MAX {
            self.wbl_pinned_refs.lock().clear();
        }

        if !to_delete.is_empty() {
            self.finish_deleted_series_cleanup(to_delete);
            self.series.shrink_oversized_stripes();
            self.maybe_gc_symbols();
        }

        self.truncate_chunks_head(min_mmap_seq)?;
        Ok(())
    }

    /// Go `ChunkDiskMapper.Truncate(uint32(minMmapFile))`: marks the current non-empty
    /// `chunks_head` file to cut a new file on the next write (`CutNewFile` ->
    /// `evtlPos.cutFileOnNextChunk`, `head_chunks.go:577-582, 971-1010`) and deletes every
    /// older file `< min_mmap_seq` excluding the current active sequence.
    fn truncate_chunks_head(&self, min_mmap_seq: usize) -> Result<(), HeadError> {
        // Hold `chunk_writer` across `chunk_reader.write()` + `truncate_in_place_except`
        // (lock order: `chunk_writer` -> `chunk_reader`) so a concurrent chunk write cannot
        // cut `active_seq + 1` and refresh `chunk_reader` before truncation runs.
        let mut w = self.chunk_writer.lock();
        let Some(wr) = w.as_mut() else {
            // Do not truncate `chunks_head` before `chunk_writer` is opened (e.g. during
            // `replay_on_open`), matching Go `Head.Init` deferring `h.gc()` without
            // calling `h.chunkDiskMapper.Truncate`.
            return Ok(());
        };
        let active_seq = wr.has_active_file().then(|| wr.current_seq());
        if wr.cur_file_has_chunks() {
            wr.cut_file_on_next_chunk();
        }
        if min_mmap_seq > 0 {
            let mut guard = self.chunk_reader.write();
            if let Some(ref mut reader) = *guard {
                reader.truncate_in_place_except(min_mmap_seq, active_seq)?;
            }
        }
        Ok(())
    }

    /// Collects in-order chunks for the specified `refs` and returns `(series, min_time, max_time_inclusive)`.
    pub fn collect_selected_compaction_series(
        &self,
        refs: &[u64],
    ) -> Result<(Vec<CompactionSeries>, i64, i64), HeadError> {

        struct ExtractedSelected {
            entry: Arc<SeriesEntry>,
            mmapped_refs: SmallVec<[MmappedChunk; 8]>,
            head_chks: Vec<ChunkMeta>,
        }
        let mut extracted = Vec::with_capacity(refs.len());
        let mut any_mmapped = false;
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        for &ref_id in refs {
            let Some(entry) = self.series.get_by_id(ref_id) else {
                continue;
            };
            let st = entry.inner.lock();
            // Skip series with active OOO state (caller should compact OOO head first).
            if st.ooo().is_some_and(|o| {
                o.ooo_head_chunk.is_some()
                    || !o.ooo_mmapped_chunks.is_empty()
                    || !o.ooo_pending_mmapped.is_empty()
            }) {
                continue;
            }
            let mut mmapped_refs: SmallVec<[MmappedChunk; 8]> = SmallVec::new();
            for mc in st.mmapped() {
                mmapped_refs.push(*mc);
                any_mmapped = true;
            }
            let mut head_chks = Vec::new();
            st.for_each_head_chunk_cloned(|hc_chunk, hc_min, hc_max| {
                mint = mint.min(hc_min);
                maxt = maxt.max(hc_max);
                head_chks.push(ChunkMeta {
                    ref_id: 0,
                    min_time: hc_min,
                    max_time: hc_max,
                    chunk: Some(hc_chunk),
                });
            });
            if !mmapped_refs.is_empty() || !head_chks.is_empty() {
                drop(st);
                extracted.push(ExtractedSelected {
                    entry,
                    mmapped_refs,
                    head_chks,
                });
            }
        }

        let chunk_reader_guard = if any_mmapped {
            Some(self.chunk_reader.read())
        } else {
            None
        };
        let chunk_reader = chunk_reader_guard.as_ref().and_then(|g| g.as_ref());

        let mut out = Vec::with_capacity(extracted.len());
        for ex in extracted {
            let mut chks = Vec::with_capacity(ex.mmapped_refs.len() + ex.head_chks.len());
            if !ex.mmapped_refs.is_empty() {
                if self.is_closed() {
                    return Err(HeadChunksError::Closed.into());
                }
                let Some(r) = chunk_reader else {
                    return Err(HeadChunksError::Closed.into());
                };
                for mc in ex.mmapped_refs {
                    let enc = r.chunk(mc.ref_id)?;
                    mint = mint.min(mc.min_time);
                    maxt = maxt.max(mc.max_time);
                    chks.push(ChunkMeta {
                        ref_id: mc.ref_id.as_u64(),
                        min_time: mc.min_time,
                        max_time: mc.max_time,
                        chunk: Some(enc),
                    });
                }
            }
            chks.extend(ex.head_chks);
            if !chks.is_empty() {
                let tombs = self.tombstones.get(ex.entry.ref_id);
                out.push(CompactionSeries {
                    labels: ex.entry.labels.clone(),
                    chunks: chks,
                    tombstones: tombs,
                });
            }
        }

        out.sort_by(|a, b| a.labels.cmp(&b.labels));
        Ok((out, mint, maxt))
    }

    /// Returns sorted series refs whose latest in-order sample is a stale marker and which have
    /// no out-of-order data (Go `Head.staleSeriesRefsNoOOOData`).
    pub fn stale_series_refs_no_ooo(&self) -> Vec<u64> {
        let mut refs = Vec::new();
        let mut buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            buf.clear();
            StripeSeries::collect_stripe_into(stripe, &mut buf);
            for entry in buf.drain(..) {
                let st = entry.inner.lock();
                let has_ooo = st.ooo().is_some_and(|o| {
                    o.ooo_head_chunk.is_some()
                        || !o.ooo_mmapped_chunks.is_empty()
                        || !o.ooo_pending_mmapped.is_empty()
                });
                if !has_ooo && st.sample_state().0 {
                    refs.push(entry.ref_id);
                }
            }
        }
        refs.sort_unstable();
        refs.dedup();
        refs
    }

    /// Captures per-series chunk-shape fingerprints for the given `refs` (Go `Head.snapshotFingerprints`).
    pub fn snapshot_fingerprints(&self, refs: &[u64]) -> HashMap<u64, SeriesFingerprint> {
        self.snapshot_fingerprints_with_watermark(refs, u64::MAX)
    }

    pub fn snapshot_fingerprints_with_watermark(
        &self,
        refs: &[u64],
        append_id_watermark: u64,
    ) -> HashMap<u64, SeriesFingerprint> {
        let mut fingerprints = HashMap::with_capacity(refs.len());
        for &ref_id in refs {
            let Some(entry) = self.series.get_by_id(ref_id) else {
                continue;
            };
            let st = entry.inner.lock();
            let fp = if append_id_watermark < u64::MAX {
                if let Some(s) = self.txs_stripe(entry.ref_id) {
                    let map = s.lock();
                    SeriesFingerprint::capture(
                        &entry,
                        &st,
                        map.get(&entry.ref_id),
                        append_id_watermark,
                    )
                } else {
                    SeriesFingerprint::capture(&entry, &st, None, append_id_watermark)
                }
            } else {
                SeriesFingerprint::capture(&entry, &st, None, append_id_watermark)
            };
            fingerprints.insert(entry.ref_id, fp);
        }
        fingerprints
    }

    /// Evicts stale series among `refs` with `max_time <= maxt_inclusive` that have no OOO data
    /// and have not mutated since `fingerprints` (Go `Head.truncateStaleSeries`).
    pub fn truncate_stale_series(
        &self,
        refs: &[u64],
        maxt_inclusive: i64,
        fingerprints: &HashMap<u64, SeriesFingerprint>,
    ) -> Result<usize, HeadError> {
        self.evict_series_refs_inner(refs, maxt_inclusive, Some(fingerprints), false, true)
    }

    /// Evicts the given `refs` from Head memory (used after `compact_selected_series`).
    pub fn evict_series_refs(&self, refs: &[u64]) -> Result<(), HeadError> {
        self.evict_series_refs_before(refs, i64::MAX)
    }

    /// Evicts `refs` from Head memory ONLY if `pending_commits == 0` and `state.max_time() <= maxt_inclusive`.
    pub fn evict_series_refs_before(
        &self,
        refs: &[u64],
        maxt_inclusive: i64,
    ) -> Result<(), HeadError> {
        self.evict_series_refs_inner(refs, maxt_inclusive, None, false, false)?;
        Ok(())
    }

    /// Evicts `refs` from Head memory ONLY if `pending_commits == 0`, `state.max_time() <= maxt_inclusive`,
    /// and the series has not mutated since `fingerprints` was captured (Go `truncateSelectedSeries`).
    pub fn evict_series_refs_with_fingerprints(
        &self,
        refs: &[u64],
        maxt_inclusive: i64,
        fingerprints: &HashMap<u64, SeriesFingerprint>,
    ) -> Result<(), HeadError> {
        self.evict_series_refs_inner(refs, maxt_inclusive, Some(fingerprints), false, false)?;
        Ok(())
    }

    /// Variant of `evict_series_refs_with_fingerprints` when `evicting_series` was already
    /// published atomically under `blocks.write()` (the caller clears it via RAII guard).
    pub fn evict_series_refs_with_fingerprints_prepublished(
        &self,
        refs: &[u64],
        maxt_inclusive: i64,
        fingerprints: &HashMap<u64, SeriesFingerprint>,
    ) -> Result<(), HeadError> {
        self.evict_series_refs_inner(refs, maxt_inclusive, Some(fingerprints), true, false)?;
        Ok(())
    }

    pub(crate) fn evict_stale_series_refs_with_fingerprints_prepublished(
        &self,
        refs: &[u64],
        maxt_inclusive: i64,
        fingerprints: &HashMap<u64, SeriesFingerprint>,
    ) -> Result<(), HeadError> {
        self.evict_series_refs_inner(refs, maxt_inclusive, Some(fingerprints), true, true)?;
        Ok(())
    }

    pub fn set_evicting_series(&self, evicting: Option<Arc<EvictedSeries>>) {
        match evicting {
            Some(ev) => {
                *self.evicting_series.write() = Some(ev);
                self.has_evicting_series.store(true, Ordering::Release);
            }
            None => {
                self.has_evicting_series.store(false, Ordering::Release);
                *self.evicting_series.write() = None;
            }
        }
    }

    #[inline]
    pub fn evicting_series(&self) -> Option<Arc<EvictedSeries>> {
        if !self.has_evicting_series.load(Ordering::Acquire) {
            return None;
        }
        self.evicting_series.read().clone()
    }

    fn evict_series_refs_inner(
        &self,
        refs: &[u64],
        maxt_inclusive: i64,
        fingerprints: Option<&HashMap<u64, SeriesFingerprint>>,
        prepublished_evicting: bool,
        require_stale: bool,
    ) -> Result<usize, HeadError> {
        let _snap_guard = self.chunk_snapshot_mtx.lock();
        if self.min_time() > maxt_inclusive {
            return Ok(0);
        }
        struct ClearEvictingGuard<'a>(&'a Head);
        impl Drop for ClearEvictingGuard<'_> {
            fn drop(&mut self) {
                self.0.set_evicting_series(None);
            }
        }
        let _evict_guard = if !prepublished_evicting && !refs.is_empty() {
            let ev = Arc::new(EvictedSeries::new(
                refs.to_vec(),
                maxt_inclusive,
                fingerprints.map(|fps| Arc::new(fps.clone())),
            ));
            self.set_evicting_series(Some(ev));
            Some(ClearEvictingGuard(self))
        } else {
            None
        };
        if self.initialized() {
            // Pre-reload readers registered before blocks.write() saw `evicting_series == None`
            // and must finish before Head unlinks these series; readers registered after see
            // `evicting_series` (Go `skipEvictedSeries`) and read them from the new ULID block,
            // so cutoff-bounded `wait_for_range` cannot be starved by continuous query traffic.
            let wait_maxt = maxt_inclusive.min(i64::MAX - 1) + 1;
            self.active_reads.wait_for_range(self.min_time(), wait_maxt);
        }
        if let Some(hook) = self.after_waiting_for_readers_hook.lock().take() {
            hook();
        }
        let min_valid_time = self.min_valid_time.load(Ordering::Acquire);
        let has_wal = self.wal.lock().is_some();
        let _pending_tomb_guard =
            has_wal.then(|| self.series.pending_tombstones.reserve_guard(refs.len()));
        let mut to_delete: Vec<(Arc<SeriesEntry>, i64)> = Vec::new();
        for &ref_id in refs {
            if let Some(entry) = self.series.get_by_id(ref_id) {
                let mut st = entry.inner.lock();
                if entry.pending_commits_count() > 0 || st.max_time() > maxt_inclusive {
                    continue;
                }
                if require_stale && !st.sample_state().0 {
                    continue;
                }
                if let Some(fps) = fingerprints {
                    if SeriesFingerprint::has_mutated(&entry, &st, fps) {
                        continue;
                    }
                }
                let has_ooo = st.ooo().is_some_and(|o| {
                    o.ooo_head_chunk.is_some()
                        || !o.ooo_mmapped_chunks.is_empty()
                        || !o.ooo_pending_mmapped.is_empty()
                });
                if !has_ooo {
                    let removed = (st.mmapped().len() + st.head_chunks_len()) as u64;
                    if st.last_exemplar_ts().is_some_and(|ts| ts >= min_valid_time) {
                        // Keep the series identity while its exemplar can still be replayed,
                        // matching Prometheus `gcSeries`.
                        if let Some(ev) = self.evicting_series() {
                            ev.mark_truncated_for_exemplar(entry.ref_id);
                        }
                        self.metrics.chunks_removed(removed);
                        st.mmapped_chunks = Vec::new();
                        st.clear_head_chunks();
                        entry
                            .in_order_max_time
                            .store(st.max_time(), Ordering::Release);
                    } else {
                        let keep_until = st.max_time();
                        let removed_entry = if has_wal {
                            self.series
                                .remove_if_gced_with_pending_tombstone(&entry, &mut st)
                        } else {
                            self.series.remove_if_gced_under_entry_lock(&entry, &mut st)
                        };
                        if removed_entry {
                            self.metrics.chunks_removed(removed);
                            self.update_metrics_on_series_delete(&st);
                            drop(st);
                            to_delete.push((entry, keep_until));
                        }
                    }
                }
            }
        }
        let evicted_count = to_delete.len();
        if !to_delete.is_empty() {
            // Fire post_deletion immediately after unlinking (before WAL tombstone write and
            // releasing pending_tombstones) so concurrent series recreations block until the
            // tombstone is logged (matching Go `stripeSeries.gcSeries` + `Head.gcSeries`).
            self.fire_post_deletion(&to_delete);
            let mut stones = Vec::with_capacity(to_delete.len());
            for (e, _) in &to_delete {
                stones.push(crate::record::Tombstone {
                    ref_id: e.ref_id,
                    mint: i64::MIN,
                    maxt: i64::MAX,
                });
            }
            let enc = RecordEncoder::new(self.opts.enable_st_storage);
            let rec = enc.tombstones(&stones, Vec::new());
            if let Some(ref mut wal) = *self.wal.lock() {
                wal.log(&[&rec])?;
            }
            drop(_pending_tomb_guard);
            if let Some(hook) = self.before_deleting_postings_hook.lock().take() {
                hook();
            }
            self.finish_deleted_series_cleanup_inner(to_delete, false);
            self.series.shrink_oversized_stripes();
            self.maybe_gc_symbols();
        }
        Ok(evicted_count)
    }

    /// Deletes samples in `[mint, maxt]` for series matching `matchers` by recording tombstones
    /// in memory and logging them to the WAL (`Head.Delete` / `DB.Delete`).
    pub fn delete(
        &self,
        mint: i64,
        maxt: i64,
        matchers: &[LabelMatcher],
    ) -> Result<(), HeadError> {
        let h_min = self.min_time();
        let h_max = self.max_time();
        let mint = mint.max(h_min);
        let maxt = maxt.min(h_max);
        if mint > maxt || h_min > h_max {
            return Ok(());
        }
        let refs = self.postings_for_matchers_direct(matchers)?;
        let mut stones = Vec::new();
        for r in refs {
            let Some(entry) = self.series.get_by_id(r) else {
                continue;
            };
            let st = entry.inner.lock();
            let mut sm = st.min_time();
            let mut sx = st.max_time();
            if let Some(ooo) = st.ooo() {
                for mc in &ooo.ooo_mmapped_chunks {
                    sm = sm.min(mc.min_time);
                    sx = sx.max(mc.max_time);
                }
                for p in &ooo.ooo_pending_mmapped {
                    sm = sm.min(p.mc.min_time);
                    sx = sx.max(p.mc.max_time);
                }
                if let Some(ref ohc) = ooo.ooo_head_chunk {
                    sm = sm.min(ohc.min_time);
                    sx = sx.max(ohc.max_time);
                }
            }
            drop(st);
            if sm == i64::MAX || sx == i64::MIN || sm > maxt || mint > sx {
                continue;
            }
            // Clamp tombstone to the series' current [sm, sx] so future samples appended
            // after Delete() are never retroactively covered (matching Go Head.Delete).
            let t0 = mint.max(sm);
            let t1 = maxt.min(sx);
            if t0 <= t1 {
                self.tombstones.add_interval(r, Interval { mint: t0, maxt: t1 });
                stones.push(crate::record::Tombstone { ref_id: r, mint: t0, maxt: t1 });
            }
        }
        if !stones.is_empty() {
            let enc = RecordEncoder::new(false);
            let rec = enc.tombstones(&stones, Vec::new());
            if let Some(ref mut wal) = *self.wal.lock() {
                wal.log(&[&rec])?;
            }
        }
        Ok(())
    }

    /// Truncates head chunks & series older than `mint`, then checkpoints and truncates the WAL.
    pub fn truncate(&self, mint: i64) -> Result<(), HeadError> {
        let initialized = self.initialized();
        self.truncate_memory(mint)?;
        if !initialized {
            return Ok(());
        }
        self.truncate_wal(mint)
    }

    /// Go `Head.truncateMemory` (counts `head_truncations_total` / `_failed_total`).
    pub fn truncate_memory(&self, mint: i64) -> Result<(), HeadError> {
        let res = self.truncate_memory_inner(mint);
        if res.is_err() {
            self.metrics.head_truncate_fail.inc();
        }
        res
    }

    /// Registers an active reader over `[mint, maxt]` on this `Head`.
    pub fn start_read(&self, mint: i64, maxt: i64) -> HeadReadGuard {
        let id = self.active_reads.start(mint, maxt);
        HeadReadGuard {
            id,
            mint,
            maxt,
            reads: Arc::clone(&self.active_reads),
        }
    }

    /// Publishes the pending in-order memory truncation target time before `wait_for_range`
    /// runs (Go `Head.truncateMemory`: `lastMemoryTruncationTime` stored before
    /// `memTruncationInProcess`).
    #[inline]
    pub fn begin_memory_truncation(&self, mint: i64) {
        self.last_memory_truncation_time
            .store(mint, Ordering::Release);
        self.mem_truncation_in_process
            .store(true, Ordering::Release);
    }

    /// Clears `mem_truncation_in_process` after memory truncation completes or aborts.
    #[inline]
    pub fn end_memory_truncation(&self) {
        self.mem_truncation_in_process
            .store(false, Ordering::Release);
    }

    /// Port of Go `Head.IsQuerierCollidingWithTruncation` (`head.go:1749-1778`):
    /// returns `(should_close, get_new, new_mint)`.
    #[inline]
    pub fn is_querier_colliding_with_truncation(
        &self,
        querier_mint: i64,
        querier_maxt: i64,
    ) -> (bool, bool, i64) {
        if !self.mem_truncation_in_process.load(Ordering::Acquire) {
            return (false, false, 0);
        }
        let mem_trunc_time = self.last_memory_truncation_time.load(Ordering::Acquire);
        if querier_maxt < mem_trunc_time {
            return (true, false, mem_trunc_time);
        }
        if querier_mint < mem_trunc_time {
            return (true, true, mem_trunc_time);
        }
        (false, false, 0)
    }

    fn truncate_memory_inner(&self, mint: i64) -> Result<(), HeadError> {
        let _snap_guard = self.chunk_snapshot_mtx.lock();
        struct MemTruncReset<'a>(&'a Head);
        impl Drop for MemTruncReset<'_> {
            fn drop(&mut self) {
                self.0.end_memory_truncation();
            }
        }
        let initialized = self.initialized();
        let old_min_time = self.min_time();
        if initialized && old_min_time >= mint {
            self.end_memory_truncation();
            if self.symbol_arena_grew_past_gc_threshold() {
                self.maybe_gc_symbols();
            }
            return Ok(());
        }
        if !self.mem_truncation_in_process.load(Ordering::Acquire) {
            self.begin_memory_truncation(mint);
        }
        let _mem_trunc_reset = MemTruncReset(self);

        if initialized {
            // Wait for any active queriers overlapping `[old_min_time, mint)` whose
            // snapshot of blocks predated `reload_after_write` before unlinking Head
            // chunks (matching Go `Head.WaitForPendingReadersInTimeRange`).
            self.active_reads.wait_for_range(old_min_time, mint);
        }

        self.min_time.store(mint, Ordering::Release);
        self.min_valid_time.store(mint, Ordering::Release);
        if self.max_time() < mint {
            self.max_time.store(mint, Ordering::Release);
        }
        if !initialized {
            return Ok(());
        }

        // Readers registered during the wait above still saw the old `min_time`, so they may
        // have counted chunks below `mint` for a lazy `QueryStream` and must materialize them
        // before the GC drops them. Readers registered from here on clamp their in-order range
        // to the `min_time` just published (Go clamps them through
        // `IsQuerierCollidingWithTruncation`) and are not waited for, which keeps this bounded.
        self.active_reads.wait_for_range(old_min_time, mint);

        self.metrics.head_truncate_total.inc();
        let gc_start = Instant::now();
        let head_maxt = self.max_time();
        let _mmap_guard = self.mmap_mtx.lock();
        let (to_delete, actual_mint, mut min_ooo_time, min_mmap_seq) =
            self.gc_collect_and_unlink(mint);

        if !to_delete.is_empty() {
            // Go `Head.truncateMemory` -> `Head.gc()` (`head.go:2256-2296`):
            // NEVER logs Tombstone(MIN, MAX) to the WAL; relies on `wal_expiries` (`actual_mint`)
            // and `min_valid_time` during WAL replay.
            self.finish_deleted_series_cleanup(to_delete);
        }
        self.metrics
            .gc_duration
            .observe(gc_start.elapsed().as_secs_f64());

        self.tombstones.truncate_before(mint);

        if actual_mint > self.min_time() {
            let clamped = actual_mint.min(self.appendable_min_valid_time());
            self.min_time.store(clamped, Ordering::Release);
            self.min_valid_time.store(clamped, Ordering::Release);
        }
        // Go `Head.truncateSeriesAndChunkDiskMapper` (`prometheus/tsdb/head.go:1994-2000`):
        // if the allowed OOO window is lower than the min OOO time seen during GC, clamp
        // `min_ooo_time` down to `head_maxt - ooo_window`.
        let ooo_window = self.ooo_time_window_ms.load(Ordering::Acquire);
        if head_maxt.wrapping_sub(ooo_window) < min_ooo_time {
            min_ooo_time = head_maxt.wrapping_sub(ooo_window);
        }
        self.min_ooo_time.store(min_ooo_time, Ordering::Release);

        self.truncate_chunks_head(min_mmap_seq)?;
        drop(_mmap_guard);
        self.series.shrink_oversized_stripes();
        self.maybe_gc_symbols();
        Ok(())
    }

    /// Garbage-collects chunks `< mint` and empty series (used on startup after WAL/WBL replay).
    pub fn gc(&self, mint: i64) -> Result<(), HeadError> {
        let _snap_guard = self.chunk_snapshot_mtx.lock();
        let _mmap_guard = self.mmap_mtx.lock();
        let (to_delete, _actual_mint, _min_ooo_time, _min_mmap_seq) =
            self.gc_collect_and_unlink(mint);
        if !to_delete.is_empty() {
            self.finish_deleted_series_cleanup(to_delete);
            self.series.shrink_oversized_stripes();
            self.maybe_gc_symbols();
        }
        self.tombstones.truncate_before(mint);
        Ok(())
    }

    fn gc_collect_and_unlink(&self, mint: i64) -> (Vec<(Arc<SeriesEntry>, i64)>, i64, i64, usize) {
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        let mut to_delete: Vec<(Arc<SeriesEntry>, i64)> = Vec::new();
        let mut actual_mint = i64::MAX;
        let mut min_ooo_time = i64::MAX;
        let mut min_mmap_seq = usize::MAX;
        let mut chunks_removed = 0u64;

        let record_refs = |st: &SeriesState,
                           actual_mint: &mut i64,
                           min_ooo_time: &mut i64,
                           min_mmap_seq: &mut usize| {
            let sm = st.min_time();
            if sm < *actual_mint {
                *actual_mint = sm;
            }
            for mc in st.mmapped() {
                let (seq, _) = mc.ref_id.unpack();
                *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
            }
            if let Some(ooo) = st.ooo() {
                for mc in &ooo.ooo_mmapped_chunks {
                    let (seq, _) = mc.ref_id.unpack();
                    *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                    *min_ooo_time = (*min_ooo_time).min(mc.min_time);
                }
                for p in &ooo.ooo_pending_mmapped {
                    if p.mc.ref_id.as_u64() != 0 && p.write_epoch.is_some() {
                        let (seq, _) = p.mc.ref_id.unpack();
                        *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                    }
                    *min_ooo_time = (*min_ooo_time).min(p.mc.min_time);
                }
                if let Some(ref ohc) = ooo.ooo_head_chunk {
                    *min_ooo_time = (*min_ooo_time).min(ohc.min_time);
                }
            }
        };

        for stripe in &self.series.stripes {
            stripe_buf.clear();
            StripeSeries::collect_stripe_into(stripe, &mut stripe_buf);
            for entry in stripe_buf.drain(..) {
                let mut st = entry.inner.lock();
                if entry.pending_commits_count() > 0 {
                    // Always include in-flight series' chunks_head sequence numbers and min_time
                    // so chunk files referenced by pending commits are NEVER deleted.
                    record_refs(&st, &mut actual_mint, &mut min_ooo_time, &mut min_mmap_seq);
                    continue;
                }

                let mut removed = 0usize;
                if !st.mmapped_chunks.is_empty() {
                    let before = st.mmapped_chunks.len();
                    st.mmapped_chunks.retain(|mc| mc.max_time >= mint);
                    removed += before - st.mmapped_chunks.len();
                    shrink_mmapped_vec(&mut st.mmapped_chunks);
                }
                removed += st.retain_head_chunks(mint);
                st.advance_first_chunk_id(removed);
                chunks_removed += removed as u64;
                entry
                    .in_order_max_time
                    .store(st.max_time(), Ordering::Release);
                if st.head_chunks_is_empty() {
                    st.invalidate_xor_state();
                    st.clear_xor2_state();
                    st.clear_hist_state();
                }

                st.clear_ooo_if_empty();
                let has_ooo = st.ooo().is_some();
                let has_recent_exemplar = st.last_exemplar_ts().is_some_and(|ts| ts >= mint);
                if !has_recent_exemplar {
                    st.flags &= !FLAG_HAS_LAST_EXEMPLAR;
                }
                st.maybe_drop_cold();

                if st.mmapped().is_empty()
                    && st.head_chunks_is_empty()
                    && !has_ooo
                    && !has_recent_exemplar
                    && self.series.remove_if_gced_under_entry_lock(&entry, &mut st)
                {
                    self.update_metrics_on_series_delete(&st);
                    st.clear_last_sample();
                    drop(st);
                    to_delete.push((entry, i64::MIN));
                    continue;
                }

                record_refs(&st, &mut actual_mint, &mut min_ooo_time, &mut min_mmap_seq);
            }
        }

        if actual_mint == i64::MAX {
            actual_mint = mint;
        }
        for (_, keep_until) in &mut to_delete {
            *keep_until = actual_mint;
        }

        self.metrics.chunks_removed(chunks_removed);
        (to_delete, actual_mint, min_ooo_time, min_mmap_seq)
    }

    /// Go `Head.truncateWAL`: checkpoints the lower two thirds of the WAL segments and
    /// drops them. A failed segment truncation or old-checkpoint deletion is only logged
    /// (and counted), like Go.
    pub fn truncate_wal(&self, mint: i64) -> Result<(), HeadError> {
        let _snap_guard = self.chunk_snapshot_mtx.lock();
        if mint <= self.last_wal_truncation_time.load(Ordering::Acquire) {
            return Ok(());
        }
        let start = Instant::now();
        self.last_wal_truncation_time.store(mint, Ordering::Release);

        let wal_dir = self.dir.join("wal");
        let (first, mut last) = {
            let mut wal_guard = self.wal.lock();
            let Some(ref mut wal) = *wal_guard else {
                return Ok(());
            };
            let Some((first, last)) = segments_range(&wal_dir)? else {
                return Ok(());
            };
            // Start a new segment, so low ingestion volume TSDB don't have more WAL than
            // needed.
            wal.next_segment()?;
            (first, last)
        };

        // Never consider last segment for checkpoint.
        if last == 0 {
            return Ok(());
        }
        last = last.saturating_sub(1);
        // The lower two thirds of segments should contain mostly obsolete samples.
        last = first + (last.saturating_sub(first)) * 2 / 3;
        if last <= first {
            return Ok(());
        }

        self.metrics.checkpoint_creation_total.inc();
        let pinned = self.wbl_pinned_refs.lock().clone();
        let expiries = self.wal_expiries.lock().clone();
        if let Err(e) = create_checkpoint(
            &wal_dir,
            first,
            last,
            |id| {
                self.series.get_by_id(id).is_some()
                    || pinned.contains(&id)
                    || expiries.get(&id).is_some_and(|&ku| ku >= mint)
            },
            mint,
            self.opts.enable_st_storage,
            self.opts.wal_compression,
        ) {
            self.metrics.checkpoint_creation_fail.inc();
            if matches!(e, WlogError::Corruption { .. }) {
                self.metrics.wal_corruptions_total.inc();
            }
            return Err(e.into());
        }

        if let Some(ref mut wal) = *self.wal.lock() {
            if let Err(e) = wal.truncate(last + 1) {
                // If truncating fails, we'll just try again at the next checkpoint.
                // Leftover segments will just be ignored in the future if there's a
                // checkpoint that supersedes them.
                tracing::error!(err = %e, "truncating segments failed");
            }
        }
        {
            // The checkpoint is written and data before mint is truncated, so stop
            // tracking expired series.
            let mut exp = self.wal_expiries.lock();
            exp.retain(|_, &mut ku| ku >= mint);
            let exp_len = exp.len();
            if exp.is_empty() {
                exp.shrink_to_fit();
            } else if exp.capacity() > (exp_len * 4).max(1024) {
                exp.shrink_to(exp_len * 2);
            }
        }
        self.metrics.checkpoint_delete_total.inc();
        if let Err(e) = delete_checkpoints(&wal_dir, last) {
            // Leftover old checkpoints do not cause problems down the line beyond
            // occupying disk space: they are ignored since a higher checkpoint exists.
            tracing::error!(err = %e, "delete old checkpoints");
            self.metrics.checkpoint_delete_fail.inc();
        }
        self.metrics
            .wal_truncate_duration
            .observe(start.elapsed().as_secs_f64());
        tracing::info!(
            first = first,
            last = last,
            duration = ?start.elapsed(),
            "WAL checkpoint complete"
        );
        Ok(())
    }

    /// Returns aggregate head statistics (`num_series`, `num_chunks`, `num_samples`, `min_time`, `max_time`).
    pub fn head_stats(&self) -> (u64, u64, u64, i64, i64) {
        let mut chunks = 0u64;
        let mut samples = 0u64;
        let mut entries = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            entries.clear();
            StripeSeries::collect_stripe_into(stripe, &mut entries);
            for e in entries.drain(..) {
                let st = e.inner.lock();
                chunks += (st.mmapped().len() + st.head_chunks_len()) as u64;
                for mc in st.mmapped() {
                    samples += mc.num_samples as u64;
                }
                samples += st.head_chunks_samples();
            }
        }
        (
            self.num_series(),
            chunks,
            samples,
            self.min_time(),
            self.max_time(),
        )
    }

    #[inline]
    pub fn postings_cache(&self) -> &PostingsForMatchersCache {
        &self.postings_cache
    }

    pub fn set_postings_cache_eligibility(&self, force: bool, shared: bool) {
        self.postings_cache.set_eligibility(force, shared);
    }

    /// Evaluates label matchers against `MemPostings` (backed by `PostingsForMatchersCache`),
    /// passing `concurrent` (Go `sharded` / `hints.ShardCount > 0`).
    pub fn postings_for_matchers_with_concurrent(
        &self,
        matchers: &[LabelMatcher],
        concurrent: bool,
    ) -> Result<Arc<[u64]>, HeadError> {
        self.postings_cache.get_or_compute_for_source(
            self.postings_cache_source_id,
            matchers,
            concurrent || self.postings_cache.force(),
            || {
                let syms = self.symbols.read();
                let view = self.postings.as_source(&syms);
                Ok(postings_for_matchers(&view, matchers)?)
            },
        )
    }

    /// Evaluates label matchers against `MemPostings` directly, bypassing `PostingsForMatchersCache`
    /// (matching Go's direct `tsdb.PostingsForMatchers` on non-`Select` RPCs).
    pub fn postings_for_matchers_direct(&self, matchers: &[LabelMatcher]) -> Result<Vec<u64>, HeadError> {
        let syms = self.symbols.read();
        let view = self.postings.as_source(&syms);
        Ok(postings_for_matchers(&view, matchers)?)
    }

    /// Direct (`PostingsForMatchersCache`-bypassing) evaluation returning `Arc<[u64]>`.
    pub fn postings_for_matchers_direct_arc(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<Arc<[u64]>, HeadError> {
        Ok(Arc::from(self.postings_for_matchers_direct(matchers)?))
    }

    /// Evaluates label matchers against `MemPostings` (backed by `PostingsForMatchersCache`),
    /// returning a shared `Arc<[u64]>` with zero slice copy on cache hits.
    pub fn postings_for_matchers_arc(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<Arc<[u64]>, HeadError> {
        self.postings_for_matchers_with_concurrent(matchers, false)
    }

    /// Evaluates label matchers against `MemPostings` (backed by `PostingsForMatchersCache`).
    pub fn postings_for_matchers(&self, matchers: &[LabelMatcher]) -> Result<Vec<u64>, HeadError> {
        Ok(self.postings_for_matchers_arc(matchers)?.to_vec())
    }

    /// Returns all series refs in `MemPostings`.
    pub fn all_postings(&self) -> Vec<u64> {
        self.postings.all_postings()
    }

    pub fn label_names(&self, matchers: &[LabelMatcher]) -> Result<Vec<String>, HeadError> {
        if matchers.is_empty() {
            let syms = self.symbols.read();
            return Ok(self.postings.label_names(&syms));
        }
        let refs = self.postings_for_matchers_direct(matchers)?;
        let mut names_set: hashbrown::HashSet<String> = hashbrown::HashSet::with_capacity(32);
        for &r in refs.iter() {
            if let Some(entry) = self.series.get_by_id(r) {
                for (k, _) in entry.labels.iter() {
                    if !names_set.contains(k) {
                        names_set.insert(k.to_owned());
                    }
                }
            }
        }
        let mut names: Vec<String> = names_set.into_iter().collect();
        names.sort_unstable();
        Ok(names)
    }

    pub fn label_values(
        &self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, HeadError> {
        self.label_values_with_limit(name, None, matchers)
    }

    pub fn label_values_with_limit(
        &self,
        name: &str,
        limit: Option<usize>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, HeadError> {
        if matchers.is_empty() {
            let syms = self.symbols.read();
            return Ok(self.postings.sorted_label_values_with_limit(name, &syms, limit));
        }
        if name.is_empty() {
            if self.postings.is_all_empty()
                || !matchers
                    .iter()
                    .filter(|m| m.name.is_empty())
                    .all(|m| m.matches(""))
            {
                return Ok(Vec::new());
            }
            if !matchers.iter().any(|m| !m.name.is_empty()) {
                return Ok(vec![String::new()]);
            }
            let refs = self.postings_for_matchers_direct(matchers)?;
            return Ok(if refs.is_empty() {
                Vec::new()
            } else {
                vec![String::new()]
            });
        }
        let refs = self.postings_for_matchers_direct(matchers)?;
        {
            let syms = self.symbols.read();
            if syms.get(name).is_none() {
                return Ok(Vec::new());
            }
        }
        let mut vals_set: hashbrown::HashSet<String> = hashbrown::HashSet::new();
        for &r in refs.iter() {
            if let Some(entry) = self.series.get_by_id(r) {
                let v = entry.labels.get(name);
                if !v.is_empty() && !vals_set.contains(v) {
                    vals_set.insert(v.to_owned());
                }
            }
        }
        let mut vals: Vec<String> = vals_set.into_iter().collect();
        if let Some(lim) = limit {
            if lim > 0 && vals.len() > lim {
                vals.select_nth_unstable(lim - 1);
                vals.truncate(lim);
            }
        }
        vals.sort_unstable();
        Ok(vals)
    }

    /// Port of Go `HeadAndOOOIndexReader.LabelValues` (`ooo_head_read.go:191-201`):
    /// returns empty if `[mint, maxt]` overlaps neither `[min_time, max_time]` nor
    /// `[min_ooo_time, max_ooo_time]`.
    pub fn ooo_label_values(
        &self,
        name: &str,
        mint: i64,
        maxt: i64,
        limit: Option<usize>,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, HeadError> {
        if (maxt < self.min_time() && maxt < self.min_ooo_time())
            || (mint > self.max_time() && mint > self.max_ooo_time())
        {
            return Ok(Vec::new());
        }
        self.label_values_with_limit(name, limit, matchers)
    }

    /// Returns `Some(entry.labels.clone())` (O(1) refcount bump, zero chunk clone) if `ref_id`
    /// has any non-tombstoned chunks overlapping `[mint, maxt]`.
    pub fn series_has_chunks_in_range(&self, ref_id: u64, mint: i64, maxt: i64) -> Option<Labels> {
        self.series_has_chunks_in_range_inner(ref_id, mint, maxt, None)
    }

    pub fn series_has_chunks_in_range_with_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: &HeadReadIsolation,
    ) -> Option<Labels> {
        self.series_has_chunks_in_range_inner(ref_id, mint, maxt, Some(iso))
    }

    fn series_has_chunks_in_range_inner(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
    ) -> Option<Labels> {
        let Some(entry) = self.series.get_by_id(ref_id) else {
            if !iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                self.metrics.series_not_found.inc();
            }
            return None;
        };
        let tombs = self.tombstones.get(ref_id);
        let ino_mint = iso.map_or_else(|| mint.max(self.min_time()), |i| i.ino_mint);
        let skip_ooo_ref = iso.map_or(ChunkDiskMapperRef(0), |i| i.max_ooo_mmap_ref_to_skip);
        let st = entry.inner.lock();
        if entry.is_gced() || iso.is_some_and(|i| i.should_skip_in_order(&entry, &st)) {
            return None;
        }
        let wm = if skip_ooo_ref.as_u64() > 0
            && st.ooo().is_some_and(|o| !o.ooo_pending_mmapped.is_empty())
        {
            self.chunk_writer.lock().as_ref().map(|cw| cw.flushed_watermark())
        } else {
            None
        };
        let has_chunk = st.any_head_chunk_bounds(|hc_min, hc_max| {
            hc_min <= maxt
                && ino_mint <= hc_max
                && !tombs.iter().any(|iv| iv.is_subrange(hc_min, hc_max))
        }) || st.mmapped().iter().any(|mc| {
            mc.overlaps_closed_interval(ino_mint, maxt)
                && !tombs.iter().any(|iv| iv.is_subrange(mc.min_time, mc.max_time))
        }) || st.ooo().is_some_and(|ooo| {
            ooo.ooo_mmapped_chunks.iter().any(|mc| {
                mc.overlaps_closed_interval(mint, maxt)
                    && !(skip_ooo_ref.as_u64() > 0 && mc.ref_id <= skip_ooo_ref)
                    && !tombs.iter().any(|iv| iv.is_subrange(mc.min_time, mc.max_time))
            }) || ooo.ooo_pending_mmapped.iter().any(|p| {
                let skipped_by_ooo_gc = skip_ooo_ref.as_u64() > 0
                    && p.mc.ref_id.as_u64() != 0
                    && p.mc.ref_id <= skip_ooo_ref
                    && wm
                        .as_ref()
                        .is_none_or(|w| w.is_flushed(p.mc.ref_id, p.write_epoch));
                !skipped_by_ooo_gc
                    && p.mc.overlaps_closed_interval(mint, maxt)
                    && !tombs
                        .iter()
                        .any(|iv| iv.is_subrange(p.mc.min_time, p.mc.max_time))
            }) || ooo.ooo_head_chunk.as_ref().is_some_and(|ohc| {
                if !ohc.overlaps_closed_interval(mint, maxt) {
                    return false;
                }
                let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
                let use_hist_st = self.opts.enable_histogram_st_encoding;
                match ohc.chunk.to_encoded_chunks(
                    ohc.min_time,
                    ohc.max_time,
                    use_xor2,
                    use_hist_st,
                ) {
                    Ok(chunks) => chunks.into_iter().any(|cc| {
                        cc.min_time <= maxt
                            && mint <= cc.max_time
                            && !tombs
                                .iter()
                                .any(|iv| iv.is_subrange(cc.min_time, cc.max_time))
                    }),
                    Err(_) => !tombs
                        .iter()
                        .any(|iv| iv.is_subrange(ohc.min_time, ohc.max_time)),
                }
            })
        });
        if has_chunk {
            Some(entry.labels.clone())
        } else {
            None
        }
    }

    /// Reads series labels and encoded chunks (in-order + OOO merged if overlapping) in `[mint, maxt]`.
    pub fn series_chunks(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
    ) -> Result<SeriesChunksResult, HeadError> {
        self.series_chunks_with_isolation_opt(ref_id, mint, maxt, None, None)
    }

    pub fn series_chunks_with_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: &HeadReadIsolation,
    ) -> Result<SeriesChunksResult, HeadError> {
        self.series_chunks_with_isolation_opt(ref_id, mint, maxt, Some(iso), None)
    }

    pub fn series_chunks_with_tx_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<SeriesChunksResult, HeadError> {
        self.series_chunks_with_isolation_opt(ref_id, mint, maxt, iso, tx_iso)
    }

    fn series_chunks_with_isolation_opt(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<SeriesChunksResult, HeadError> {
        let Some((labels_opt, chks, tombs)) =
            self.series_chunks_inner(ref_id, mint, maxt, true, iso, tx_iso)?
        else {
            return Ok(None);
        };
        let Some(labels) = labels_opt else {
            return Ok(None);
        };
        Ok(Some((labels, chks, tombs)))
    }

    #[allow(clippy::type_complexity)]
    fn series_chunks_inner(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        need_labels: bool,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Option<(Option<Labels>, Vec<ChunkMeta>, Vec<Interval>)>, HeadError> {
        let Some(entry) = self.series.get_by_id(ref_id) else {
            if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                self.metrics.chunk_reads_of_removed_series.inc();
            } else {
                // Go `headIndexReader.Series`: `seriesNotFound.Inc()`.
                self.metrics.series_not_found.inc();
            }
            return Ok(None);
        };
        self.series_chunks_for_entry_inner(&entry, mint, maxt, need_labels, iso, tx_iso)
    }

    #[allow(clippy::type_complexity)]
    fn series_chunks_for_entry_inner(
        &self,
        entry: &SeriesEntry,
        mint: i64,
        maxt: i64,
        need_labels: bool,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Option<(Option<Labels>, Vec<ChunkMeta>, Vec<Interval>)>, HeadError> {
        let ref_id = entry.ref_id;
        const OOO_HEAD_CHUNK_REF_BIT: u64 = 1u64 << 31;
        let skip_ooo_ref = iso.map_or(ChunkDiskMapperRef(0), |i| i.max_ooo_mmap_ref_to_skip);

        'retry: loop {
            if self.is_closed() {
                return Err(HeadChunksError::Closed.into());
            }
            let (mmapped_in_order, head_chks, mmapped_ooo, ooo_head_chks) = {
                let st = entry.inner.lock();
                if entry.is_gced() || iso.is_some_and(|i| i.should_skip_in_order(entry, &st)) {
                    if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                        self.metrics.chunk_reads_of_removed_series.inc();
                    }
                    return Ok(None);
                }
                let ino_mint = iso.map_or_else(|| mint.max(self.min_time()), |i| i.ino_mint);
                let effective_tx_iso = tx_iso.filter(|_| !self.iso.is_disabled());
                let (mmapped_in_order, head_chks) = if effective_tx_iso.is_some() {
                    if let Some(s) = self.txs_stripe(ref_id) {
                        let map = s.lock();
                        st.collect_in_order_chunks_for_tx_iso(
                            map.get(&ref_id),
                            ino_mint,
                            maxt,
                            effective_tx_iso,
                        )?
                    } else {
                        st.collect_in_order_chunks_for_tx_iso(None, ino_mint, maxt, effective_tx_iso)?
                    }
                } else {
                    st.collect_in_order_chunks_for_tx_iso(None, ino_mint, maxt, None)?
                };
                let mut mmapped_ooo = Vec::new();
                let mut ooo_head_chks = Vec::new();
                if let Some(ooo) = st.ooo() {
                    let wm = if skip_ooo_ref.as_u64() > 0 && !ooo.ooo_pending_mmapped.is_empty() {
                        self.chunk_writer.lock().as_ref().map(|cw| cw.flushed_watermark())
                    } else {
                        None
                    };
                    let mut ooo_pos = 0u64;
                    for mc in &ooo.ooo_mmapped_chunks {
                        if mc.overlaps_closed_interval(mint, maxt)
                            && !(skip_ooo_ref.as_u64() > 0 && mc.ref_id <= skip_ooo_ref)
                        {
                            mmapped_ooo.push((OOO_HEAD_CHUNK_REF_BIT | ooo_pos, *mc));
                        }
                        ooo_pos += 1;
                    }
                    for p in &ooo.ooo_pending_mmapped {
                        let skipped_by_ooo_gc = skip_ooo_ref.as_u64() > 0
                            && p.mc.ref_id.as_u64() != 0
                            && p.mc.ref_id <= skip_ooo_ref
                            && wm
                                .as_ref()
                                .is_none_or(|w| w.is_flushed(p.mc.ref_id, p.write_epoch));
                        if !skipped_by_ooo_gc && p.mc.overlaps_closed_interval(mint, maxt) {
                            ooo_head_chks.push(ChunkMeta {
                                ref_id: OOO_HEAD_CHUNK_REF_BIT | ooo_pos,
                                min_time: p.mc.min_time,
                                max_time: p.mc.max_time,
                                chunk: Some(p.chunk.clone()),
                            });
                        }
                        ooo_pos += 1;
                    }
                    if let Some(ref ohc) = ooo.ooo_head_chunk {
                        if ohc.overlaps_closed_interval(mint, maxt) {
                            let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
                            let use_hist_st = self.opts.enable_histogram_st_encoding;
                            let head_ref = OOO_HEAD_CHUNK_REF_BIT | ooo_pos;
                            for cc in ohc.chunk.to_encoded_chunks(
                                ohc.min_time,
                                ohc.max_time,
                                use_xor2,
                                use_hist_st,
                            )? {
                                if cc.min_time <= maxt && mint <= cc.max_time {
                                    ooo_head_chks.push(ChunkMeta {
                                        ref_id: head_ref,
                                        min_time: cc.min_time,
                                        max_time: cc.max_time,
                                        chunk: Some(cc.chunk),
                                    });
                                }
                            }
                        }
                    }
                }
                (mmapped_in_order, head_chks, mmapped_ooo, ooo_head_chks)
            };

            // Acquire chunk_reader only if there are mmapped chunks to load.
            if mmapped_in_order.is_empty() && mmapped_ooo.is_empty() {
                let mut chks = head_chks;
                chks.extend(ooo_head_chks);
                if chks.is_empty() {
                    return Ok(None);
                }
                let tombs = self.tombstones.get(ref_id);
                let labels = need_labels.then(|| entry.labels.clone());
                return Ok(Some((labels, chks, tombs)));
            }

            // Acquire chunk_reader AFTER releasing entry.inner (respecting entry.inner -> chunk_reader).
            // If truncate_memory/truncate_ooo concurrently removed a chunk and unlinked its chunks_head
            // file in the gap, drop chunk_reader first and re-check entry.inner before failing.
            let chunk_reader_guard = self.chunk_reader.read();
            let Some(r) = chunk_reader_guard.as_ref() else {
                return Err(HeadChunksError::Closed.into());
            };
            if self.is_closed() {
                return Err(HeadChunksError::Closed.into());
            }

            let ino_mint = iso.map_or_else(|| mint.max(self.min_time()), |i| i.ino_mint);
            let mut chks = Vec::with_capacity(
                mmapped_in_order.len() + head_chks.len() + mmapped_ooo.len() + ooo_head_chks.len(),
            );
            for (sort_ref, mc, stop_after_opt) in mmapped_in_order {
                match r.chunk(mc.ref_id) {
                    Ok(enc) => {
                        if let Some(stop_after) = stop_after_opt {
                            if let Some((sliced, smint, smaxt)) =
                                slice_chunk_prefix(&enc, stop_after)?
                            {
                                if smint <= maxt && ino_mint <= smaxt {
                                    chks.push(ChunkMeta {
                                        ref_id: sort_ref,
                                        min_time: smint,
                                        max_time: smaxt,
                                        chunk: Some(sliced),
                                    });
                                }
                            }
                        } else {
                            chks.push(ChunkMeta {
                                ref_id: sort_ref,
                                min_time: mc.min_time,
                                max_time: mc.max_time,
                                chunk: Some(enc),
                            });
                        }
                    }
                    Err(e) if !r.has_file(mc.ref_id.unpack().0 as usize) => {
                        drop(chunk_reader_guard);
                        if self.is_closed() {
                            return Err(HeadChunksError::Closed.into());
                        }
                        let st = entry.inner.lock();
                        if entry.is_gced() {
                            return Ok(None);
                        }
                        if st.mmapped().iter().any(|m| m.ref_id == mc.ref_id) {
                            return Err(e.into());
                        }
                        continue 'retry;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            chks.extend(head_chks);
            for (sort_ref, mc) in mmapped_ooo {
                match r.chunk(mc.ref_id) {
                    Ok(enc) => chks.push(ChunkMeta {
                        ref_id: sort_ref,
                        min_time: mc.min_time,
                        max_time: mc.max_time,
                        chunk: Some(enc),
                    }),
                    Err(e) if !r.has_file(mc.ref_id.unpack().0 as usize) => {
                        drop(chunk_reader_guard);
                        if self.is_closed() {
                            return Err(HeadChunksError::Closed.into());
                        }
                        let st = entry.inner.lock();
                        if entry.is_gced() {
                            return Ok(None);
                        }
                        let still_present = st.ooo().is_some_and(|o| {
                            o.ooo_mmapped_chunks.iter().any(|m| m.ref_id == mc.ref_id)
                        });
                        if still_present {
                            return Err(e.into());
                        }
                        continue 'retry;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            chks.extend(ooo_head_chks);

            if chks.is_empty() {
                return Ok(None);
            }
            let tombs = self.tombstones.get(ref_id);
            let labels = need_labels.then(|| entry.labels.clone());
            return Ok(Some((labels, chks, tombs)));
        }
    }

    /// Loads and overlap-merges (if tombstones or OOO overlaps exist) all chunks for `ref_id`
    /// overlapping `[mint, maxt]`. Used by lazy Phase 2 chunk streaming for a single series at a time.
    pub fn series_final_chunks(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
    ) -> Result<Option<(Labels, Vec<ChunkMeta>)>, HeadError> {
        self.series_final_chunks_inner(ref_id, mint, maxt, None, None)
    }

    pub fn series_final_chunks_with_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: &HeadReadIsolation,
    ) -> Result<Option<(Labels, Vec<ChunkMeta>)>, HeadError> {
        self.series_final_chunks_inner(ref_id, mint, maxt, Some(iso), None)
    }

    fn series_final_chunks_inner(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Option<(Labels, Vec<ChunkMeta>)>, HeadError> {
        let Some((labels, chks, tombs)) =
            self.series_chunks_with_isolation_opt(ref_id, mint, maxt, iso, tx_iso)?
        else {
            return Ok(None);
        };
        if chks.is_empty() {
            return Ok(None);
        }
        // Go `HeadAndOOOChunkReader` / `populateChunksFromIterable` (`querier.go:1271`)
        // ignores `floatChunkEncoding` on intra-head merges and uses `Xor` when `st == 0`.
        let final_chunks = merge_head_chunks_with_tombstones(chks, &tombs)?;
        if final_chunks.is_empty() {
            Ok(None)
        } else {
            Ok(Some((labels, final_chunks)))
        }
    }

    /// Loads and overlap-merges all chunks for `ref_id` overlapping `[mint, maxt]` without
    /// cloning `entry.labels` (for Phase 2 of lazy `QueryStream` when labels were already emitted).
    pub fn series_final_chunks_only(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        self.series_final_chunks_only_inner(ref_id, mint, maxt, None, None)
    }

    pub fn series_final_chunks_only_with_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: &HeadReadIsolation,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        self.series_final_chunks_only_inner(ref_id, mint, maxt, Some(iso), None)
    }

    pub fn series_final_chunks_only_with_tx_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        self.series_final_chunks_only_inner(ref_id, mint, maxt, iso, tx_iso)
    }

    fn series_final_chunks_only_inner(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        let Some((_, chks, tombs)) = self.series_chunks_inner(ref_id, mint, maxt, false, iso, tx_iso)?
        else {
            return Ok(Vec::new());
        };
        if chks.is_empty() {
            return Ok(Vec::new());
        }
        Ok(merge_head_chunks_with_tombstones(chks, &tombs)?)
    }

    /// Loads and overlap-merges all chunks for a pre-resolved `SeriesEntry` overlapping
    /// `[mint, maxt]` without a second `StripeSeries::get_by_id` lookup or `Labels` clone.
    pub fn series_final_chunks_for_entry(
        &self,
        entry: &SeriesEntry,
        mint: i64,
        maxt: i64,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        self.series_final_chunks_for_entry_inner(entry, mint, maxt, None, None)
    }

    pub fn series_final_chunks_for_entry_with_isolation(
        &self,
        entry: &SeriesEntry,
        mint: i64,
        maxt: i64,
        iso: &HeadReadIsolation,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        self.series_final_chunks_for_entry_inner(entry, mint, maxt, Some(iso), None)
    }

    pub fn series_final_chunks_for_entry_with_tx_isolation(
        &self,
        entry: &SeriesEntry,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        self.series_final_chunks_for_entry_inner(entry, mint, maxt, iso, tx_iso)
    }

    fn series_final_chunks_for_entry_inner(
        &self,
        entry: &SeriesEntry,
        mint: i64,
        maxt: i64,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        let Some((_, chks, tombs)) =
            self.series_chunks_for_entry_inner(entry, mint, maxt, false, iso, tx_iso)?
        else {
            return Ok(Vec::new());
        };
        if chks.is_empty() {
            return Ok(Vec::new());
        }
        Ok(merge_head_chunks_with_tombstones(chks, &tombs)?)
    }

    /// Materializes Phase 2 chunks from a Phase 1 [`HeadChunkSnapshot`], guaranteeing
    /// `chunks.len() == Phase 1 chunk_count` even under concurrent chunk cuts or OOO inserts.
    pub fn series_final_chunks_from_snapshot(
        &self,
        entry_opt: Option<&SeriesEntry>,
        ref_id: u64,
        snapshot: HeadChunkSnapshot,
    ) -> Result<Vec<ChunkMeta>, HeadError> {
        let min_times = match snapshot {
            HeadChunkSnapshot::Precomputed(chks) => return Ok(chks),
            HeadChunkSnapshot::InOrderByMinTime(min_times) => min_times,
        };
        if min_times.is_empty() {
            return Ok(Vec::new());
        }
        let looked_up;
        let entry = match entry_opt {
            Some(e) => e,
            None => {
                let Some(e) = self.series.get_by_id(ref_id) else {
                    self.metrics.chunk_reads_of_removed_series.inc();
                    return Ok(Vec::new());
                };
                looked_up = e;
                &looked_up
            }
        };

        'retry: loop {
            if self.is_closed() {
                return Err(HeadChunksError::Closed.into());
            }
            let (mmapped_in_order, head_chks) = {
                let st = entry.inner.lock();
                if entry.is_gced() {
                    self.metrics.chunk_reads_of_removed_series.inc();
                    return Ok(Vec::new());
                }
                let mut pos = 0u64;
                let mut mmapped_in_order = Vec::new();
                for mc in st.mmapped() {
                    if min_times.contains(&mc.min_time) {
                        mmapped_in_order.push((pos, *mc));
                        pos += 1;
                    }
                }
                let mut head_chks = Vec::new();
                st.for_each_head_chunk_cloned(|hc_chunk, hc_min, hc_max| {
                    if min_times.contains(&hc_min) {
                        head_chks.push(ChunkMeta {
                            ref_id: pos,
                            min_time: hc_min,
                            max_time: hc_max,
                            chunk: Some(hc_chunk),
                        });
                        pos += 1;
                    }
                });
                (mmapped_in_order, head_chks)
            };

            if mmapped_in_order.is_empty() {
                return Ok(head_chks);
            }

            let chunk_reader_guard = self.chunk_reader.read();
            let Some(r) = chunk_reader_guard.as_ref() else {
                return Err(HeadChunksError::Closed.into());
            };
            if self.is_closed() {
                return Err(HeadChunksError::Closed.into());
            }

            let mut chks = Vec::with_capacity(mmapped_in_order.len() + head_chks.len());
            for (sort_ref, mc) in mmapped_in_order {
                match r.chunk(mc.ref_id) {
                    Ok(enc) => chks.push(ChunkMeta {
                        ref_id: sort_ref,
                        min_time: mc.min_time,
                        max_time: mc.max_time,
                        chunk: Some(enc),
                    }),
                    Err(e) if !r.has_file(mc.ref_id.unpack().0 as usize) => {
                        drop(chunk_reader_guard);
                        if self.is_closed() {
                            return Err(HeadChunksError::Closed.into());
                        }
                        let st = entry.inner.lock();
                        if entry.is_gced() {
                            self.metrics.chunk_reads_of_removed_series.inc();
                            return Ok(Vec::new());
                        }
                        if st.mmapped().iter().any(|m| m.ref_id == mc.ref_id) {
                            return Err(e.into());
                        }
                        continue 'retry;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            chks.extend(head_chks);
            return Ok(chks);
        }
    }

    fn has_non_skipped_ooo_in_range(
        &self,
        st: &SeriesState,
        mint: i64,
        maxt: i64,
        skip_ooo_ref: ChunkDiskMapperRef,
    ) -> bool {
        let Some(ooo) = st.ooo() else {
            return false;
        };
        let wm = if skip_ooo_ref.as_u64() > 0 && !ooo.ooo_pending_mmapped.is_empty() {
            self.chunk_writer.lock().as_ref().map(|cw| cw.flushed_watermark())
        } else {
            None
        };
        ooo.ooo_mmapped_chunks.iter().any(|mc| {
            mc.overlaps_closed_interval(mint, maxt)
                && !(skip_ooo_ref.as_u64() > 0 && mc.ref_id <= skip_ooo_ref)
        }) || ooo.ooo_pending_mmapped.iter().any(|p| {
            let skipped_by_ooo_gc = skip_ooo_ref.as_u64() > 0
                && p.mc.ref_id.as_u64() != 0
                && p.mc.ref_id <= skip_ooo_ref
                && wm
                    .as_ref()
                    .is_none_or(|w| w.is_flushed(p.mc.ref_id, p.write_epoch));
            !skipped_by_ooo_gc && p.mc.overlaps_closed_interval(mint, maxt)
        }) || ooo
            .ooo_head_chunk
            .as_ref()
            .is_some_and(|ohc| ohc.overlaps_closed_interval(mint, maxt))
    }

    /// Computes `(entry, exact_chunk_count, snapshot)` for `ref_id` in `[mint, maxt]` without
    /// cloning `Labels`, chunk bytes, or reading mmap files on the normal in-order disjoint fast path.
    pub fn series_lazy_chunk_count_with_snapshot(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
    ) -> Result<Option<(Arc<SeriesEntry>, u32, HeadChunkSnapshot)>, HeadError> {
        self.series_lazy_chunk_count_with_snapshot_inner(ref_id, mint, maxt, filter, None, None)
    }

    pub fn series_lazy_chunk_count_with_snapshot_and_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
        iso: &HeadReadIsolation,
    ) -> Result<Option<(Arc<SeriesEntry>, u32, HeadChunkSnapshot)>, HeadError> {
        self.series_lazy_chunk_count_with_snapshot_inner(ref_id, mint, maxt, filter, Some(iso), None)
    }

    pub fn series_lazy_chunk_count_with_snapshot_and_tx_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Option<(Arc<SeriesEntry>, u32, HeadChunkSnapshot)>, HeadError> {
        self.series_lazy_chunk_count_with_snapshot_inner(ref_id, mint, maxt, filter, iso, tx_iso)
    }

    fn series_lazy_chunk_count_with_snapshot_inner(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<Option<(Arc<SeriesEntry>, u32, HeadChunkSnapshot)>, HeadError> {
        if self.is_closed() {
            return Err(HeadChunksError::Closed.into());
        }
        let Some(entry) = self.series.get_by_id(ref_id) else {
            if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                self.metrics.chunk_reads_of_removed_series.inc();
            } else {
                // Go `headIndexReader.Series`: `seriesNotFound.Inc()`.
                self.metrics.series_not_found.inc();
            }
            return Ok(None);
        };
        if let Some(pred) = filter {
            if !pred(&entry.labels) {
                return Ok(None);
            }
        }

        let has_any_tombs = self.tombstones.has_any();
        let ino_mint = iso.map_or_else(|| mint.max(self.min_time()), |i| i.ino_mint);
        let skip_ooo_ref = iso.map_or(ChunkDiskMapperRef(0), |i| i.max_ooo_mmap_ref_to_skip);
        let (min_times, needs_slow_resolve) = {
            let st = entry.inner.lock();
            if entry.is_gced() || iso.is_some_and(|i| i.should_skip_in_order(&entry, &st)) {
                if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                    self.metrics.chunk_reads_of_removed_series.inc();
                }
                return Ok(None);
            }
            let needs_tx_iso = tx_iso
                .filter(|_| !self.iso.is_disabled())
                .is_some_and(|info| {
                    self.txs_stripe(ref_id)
                        .is_some_and(|s| !tx_is_fast_path(s.lock().get(&ref_id), info))
                });
            let has_ooo = self.has_non_skipped_ooo_in_range(&st, mint, maxt, skip_ooo_ref);
            let mut min_times: SmallVec<[i64; 4]> = SmallVec::new();
            let mut prev_min: i64 = i64::MIN;
            let mut prev_max: i64 = i64::MIN;
            let mut has_prev = false;
            let mut sorted_disjoint = true;
            for mc in st.mmapped() {
                if mc.overlaps_closed_interval(ino_mint, maxt) {
                    if has_prev && (mc.min_time < prev_min || mc.min_time <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = mc.min_time;
                    prev_max = mc.max_time;
                    has_prev = true;
                    min_times.push(mc.min_time);
                }
            }
            st.for_each_head_chunk_bounds(|hc_min, hc_max| {
                if hc_min <= maxt && ino_mint <= hc_max {
                    if has_prev && (hc_min < prev_min || hc_min <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = hc_min;
                    prev_max = hc_max;
                    has_prev = true;
                    min_times.push(hc_min);
                }
            });
            (min_times, has_ooo || has_any_tombs || !sorted_disjoint || needs_tx_iso)
        };

        if !needs_slow_resolve {
            return Ok(if min_times.is_empty() {
                None
            } else {
                let cnt = min_times.len() as u32;
                Some((entry, cnt, HeadChunkSnapshot::InOrderByMinTime(min_times)))
            });
        }

        let chks = self.series_final_chunks_for_entry_inner(&entry, mint, maxt, iso, tx_iso)?;
        Ok(if chks.is_empty() {
            None
        } else {
            let cnt = chks.len() as u32;
            Some((entry, cnt, HeadChunkSnapshot::Precomputed(chks)))
        })
    }

    /// Computes `(entry, exact_chunk_count)` for `ref_id` in `[mint, maxt]` without cloning
    /// `Labels`, chunk bytes, or reading mmap files on the normal in-order disjoint fast path.
    pub fn series_lazy_chunk_count_with_entry(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
    ) -> Result<Option<(Arc<SeriesEntry>, u32)>, HeadError> {
        Ok(self
            .series_lazy_chunk_count_with_snapshot(ref_id, mint, maxt, filter)?
            .map(|(entry, cnt, _)| (entry, cnt)))
    }

    /// Computes `(labels, exact_chunk_count)` for `ref_id` in `[mint, maxt]` without cloning any
    /// chunk bytes or reading mmap files on the normal in-order disjoint fast path.
    pub fn series_lazy_chunk_count(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
    ) -> Result<Option<(Labels, u32)>, HeadError> {
        Ok(self
            .series_lazy_chunk_count_with_entry(ref_id, mint, maxt, filter)?
            .map(|(entry, cnt)| (entry.labels.clone(), cnt)))
    }

    /// Returns `(labels, chunk_intervals, snapshot)` for `ref_id` in `[mint, maxt]`
    /// without cloning chunk bytes on the fast path. Used when combining Head with on-disk Blocks.
    pub fn series_chunk_metas_no_bytes_with_snapshot(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
    ) -> Result<SeriesChunkMetasWithSnapshot, HeadError> {
        self.series_chunk_metas_no_bytes_with_snapshot_inner(ref_id, mint, maxt, filter, None, None)
    }

    pub fn series_chunk_metas_no_bytes_with_snapshot_and_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
        iso: &HeadReadIsolation,
    ) -> Result<SeriesChunkMetasWithSnapshot, HeadError> {
        self.series_chunk_metas_no_bytes_with_snapshot_inner(ref_id, mint, maxt, filter, Some(iso), None)
    }

    pub fn series_chunk_metas_no_bytes_with_snapshot_and_tx_isolation(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<SeriesChunkMetasWithSnapshot, HeadError> {
        self.series_chunk_metas_no_bytes_with_snapshot_inner(ref_id, mint, maxt, filter, iso, tx_iso)
    }

    fn series_chunk_metas_no_bytes_with_snapshot_inner(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
        iso: Option<&HeadReadIsolation>,
        tx_iso: Option<&IsolationStateInfo>,
    ) -> Result<SeriesChunkMetasWithSnapshot, HeadError> {
        if self.is_closed() {
            return Err(HeadChunksError::Closed.into());
        }
        let Some(entry) = self.series.get_by_id(ref_id) else {
            if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                self.metrics.chunk_reads_of_removed_series.inc();
            } else {
                // Go `headIndexReader.Series`: `seriesNotFound.Inc()`.
                self.metrics.series_not_found.inc();
            }
            return Ok(None);
        };
        if let Some(pred) = filter {
            if !pred(&entry.labels) {
                return Ok(None);
            }
        }
        let has_any_tombs = self.tombstones.has_any();
        let ino_mint = iso.map_or_else(|| mint.max(self.min_time()), |i| i.ino_mint);
        let skip_ooo_ref = iso.map_or(ChunkDiskMapperRef(0), |i| i.max_ooo_mmap_ref_to_skip);
        let (intervals, needs_slow) = {
            let st = entry.inner.lock();
            if entry.is_gced() || iso.is_some_and(|i| i.should_skip_in_order(&entry, &st)) {
                if iso.is_some_and(|i| i.evicting_contains(ref_id)) {
                    self.metrics.chunk_reads_of_removed_series.inc();
                }
                return Ok(None);
            }
            let needs_tx_iso = tx_iso
                .filter(|_| !self.iso.is_disabled())
                .is_some_and(|info| {
                    self.txs_stripe(ref_id)
                        .is_some_and(|s| !tx_is_fast_path(s.lock().get(&ref_id), info))
                });
            let has_ooo = self.has_non_skipped_ooo_in_range(&st, mint, maxt, skip_ooo_ref);
            let mut intervals: SmallVec<[(i64, i64); 4]> = SmallVec::new();
            let mut prev_min = i64::MIN;
            let mut prev_max = i64::MIN;
            let mut has_prev = false;
            let mut sorted_disjoint = true;
            for mc in st.mmapped() {
                if mc.overlaps_closed_interval(ino_mint, maxt) {
                    if has_prev && (mc.min_time < prev_min || mc.min_time <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = mc.min_time;
                    prev_max = mc.max_time;
                    has_prev = true;
                    intervals.push((mc.min_time, mc.max_time));
                }
            }
            st.for_each_head_chunk_bounds(|hc_min, hc_max| {
                if hc_min <= maxt && ino_mint <= hc_max {
                    if has_prev && (hc_min < prev_min || hc_min <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = hc_min;
                    prev_max = hc_max;
                    has_prev = true;
                    intervals.push((hc_min, hc_max));
                }
            });
            (intervals, has_ooo || has_any_tombs || !sorted_disjoint || needs_tx_iso)
        };
        if !needs_slow {
            if intervals.is_empty() {
                return Ok(None);
            }
            let min_times: SmallVec<[i64; 4]> = intervals.iter().map(|&(mn, _)| mn).collect();
            return Ok(Some((
                entry.labels.clone(),
                intervals,
                HeadChunkSnapshot::InOrderByMinTime(min_times),
            )));
        }
        let Some((labels, chks)) = self.series_final_chunks_inner(ref_id, mint, maxt, iso, tx_iso)? else {
            return Ok(None);
        };
        let ivs: SmallVec<[(i64, i64); 4]> =
            chks.iter().map(|c| (c.min_time, c.max_time)).collect();
        Ok(Some((labels, ivs, HeadChunkSnapshot::Precomputed(chks))))
    }

    /// Returns `(labels, chunk_intervals, needs_chunk_load_for_count)` for `ref_id` in `[mint, maxt]`
    /// without cloning chunk bytes on the fast path. Used when combining Head with on-disk Blocks.
    pub fn series_chunk_metas_no_bytes(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
    ) -> Result<SeriesChunkMetasNoBytes, HeadError> {
        Ok(self
            .series_chunk_metas_no_bytes_with_snapshot(ref_id, mint, maxt, filter)?
            .map(|(labels, ivs, snap)| {
                let is_slow = matches!(snap, HeadChunkSnapshot::Precomputed(_));
                (labels, ivs, is_slow)
            }))
    }

    /// Attempts a non-blocking `try_lock` on the series entry's state; returns `None` if absent,
    /// `Some(true)` if currently locked, `Some(false)` if unlocked.
    pub fn is_series_locked(&self, ref_id: u64) -> Option<bool> {
        self.series
            .get_by_id(ref_id)
            .map(|entry| entry.inner.try_lock().is_none())
    }

    pub fn series_labels(&self, ref_id: u64) -> Option<Labels> {
        self.series.get_by_id(ref_id).map(|e| e.labels.clone())
    }

    /// Returns a [`HeadIndexReader`] scoped to the full Head time range (Go `Head.Index`).
    pub fn index(&self) -> Result<HeadIndexReader<'_>, HeadError> {
        if self.is_closed() {
            return Err(HeadChunksError::Closed.into());
        }
        Ok(HeadIndexReader {
            head: self,
            mint: self.min_time(),
            maxt: i64::MAX,
        })
    }

    /// Returns a [`HeadIndexReader`] without returning an error (Go `Head.MustIndex`).
    pub fn must_index(&self) -> HeadIndexReader<'_> {
        HeadIndexReader {
            head: self,
            mint: self.min_time(),
            maxt: i64::MAX,
        }
    }

    pub fn set_enable_st_as_zero_sample(&self, enabled: bool) {
        self.enable_st_as_zero_sample
            .store(enabled, Ordering::Release);
    }

    #[inline]
    pub fn enable_st_as_zero_sample(&self) -> bool {
        self.enable_st_as_zero_sample.load(Ordering::Acquire)
    }

    pub fn set_enable_memory_snapshot_on_shutdown(&self, enabled: bool) {
        self.enable_memory_snapshot_on_shutdown
            .store(enabled, Ordering::Release);
    }

    #[inline]
    pub fn enable_memory_snapshot_on_shutdown(&self) -> bool {
        self.enable_memory_snapshot_on_shutdown.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn write_raw_chunk_for_test(
        &self,
        series_ref: u64,
        mint: i64,
        maxt: i64,
        encoding: u8,
        data: &[u8],
        is_ooo: bool,
    ) -> Result<ChunkDiskMapperRef, HeadError> {
        let mut cw_guard = self.chunk_writer.lock();
        let cw = cw_guard.as_mut().ok_or(HeadChunksError::Closed)?;
        let r = cw.write_raw_chunk_bytes(series_ref, mint, maxt, encoding, data, is_ooo)?;
        cw.flush()?;
        Ok(r)
    }

    #[doc(hidden)]
    pub fn force_close_wal_for_test(&self) {
        if let Some(ref mut wal) = *self.wal.lock() {
            wal.force_close_for_test();
        }
    }

    #[doc(hidden)]
    pub fn wait_for_pending_readers_in_time_range(&self, mint: i64, maxt_exclusive: i64) {
        self.active_reads.wait_for_range(mint, maxt_exclusive);
    }

    /// Reads `wal/series_state.json` (Go `Head.readSeriesStateFile`).
    pub fn read_series_state_file(&self) -> io::Result<SeriesLifecycleState> {
        let path = self.dir.join("wal").join(SERIES_STATE_FILENAME);
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Atomically writes `wal/series_state.json` (Go `Head.writeSeriesState`).
    pub fn write_series_state_file(&self, clean_shutdown: bool) -> io::Result<()> {
        let wal_dir = self.dir.join("wal");
        if !wal_dir.exists() {
            return Ok(());
        }
        let last = match *self.wal.lock() {
            Some(ref w) => w
                .last_segment_and_offset()
                .ok()
                .and_then(|(s, _)| s)
                .map_or(-1i64, |s| s as i64),
            None => segments_range(&wal_dir)
                .ok()
                .flatten()
                .map_or(-1i64, |(_, l)| l as i64),
        };
        let state = SeriesLifecycleState {
            last_series_id: self.last_series_id.load(Ordering::Acquire),
            last_wal_segment: last,
            clean_shutdown,
        };
        let path = wal_dir.join(SERIES_STATE_FILENAME);
        let tmp = wal_dir.join(format!("{SERIES_STATE_FILENAME}.tmp"));
        let data = serde_json::to_vec(&state)
            .map_err(io::Error::other)?;
        fs::write(&tmp, data)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    /// Performs a bounded reverse WAL scan from `end_segment` down to `state.last_wal_segment`
    /// to find the highest series ID (Go `Head.findLastSeriesID`).
    pub fn find_last_series_id(
        &self,
        state: &SeriesLifecycleState,
        end_segment: usize,
    ) -> Result<u64, HeadError> {
        let wal_dir = self.dir.join("wal");
        let start_seg = state.last_wal_segment.max(0) as usize;
        if end_segment < start_seg {
            return Ok(state.last_series_id);
        }
        let dec = RecordDecoder::new();
        for seg in (start_seg..=end_segment).rev() {
            let seg_path = crate::wlog::segment_name(&wal_dir, seg);
            if !seg_path.exists() {
                continue;
            }
            let mut r = WlogReader::open_segment(&wal_dir, seg)?;
            let mut highest: u64 = 0;
            let mut found = false;
            while let Some(raw) = r.next_record()? {
                if let Ok(DecodedRecord::Series(series)) = dec.decode_record(raw) {
                    for s in series {
                        highest = highest.max(s.ref_id);
                        found = true;
                    }
                }
            }
            if found {
                return Ok(highest);
            }
        }
        Ok(state.last_series_id)
    }

    /// Finds the most recent `chunk_snapshot.NNNNNN.OOOOOOOOOO` directory in `dir`.
    pub fn last_chunk_snapshot(dir: &Path) -> Result<Option<(PathBuf, usize, usize)>, HeadError> {
        let rd = match fs::read_dir(dir) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut best: Option<(PathBuf, usize, usize)> = None;
        for ent in rd {
            let ent = ent?;
            let name = ent.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            let Some(rest) = name_str.strip_prefix(CHUNK_SNAPSHOT_PREFIX) else {
                continue;
            };
            if !ent.file_type()?.is_dir() {
                continue;
            }
            let mut parts = rest.split('.');
            let (Some(idx_s), Some(off_s), None) = (parts.next(), parts.next(), parts.next()) else {
                continue;
            };
            let (Ok(idx), Ok(off)) = (idx_s.parse::<usize>(), off_s.parse::<usize>()) else {
                continue;
            };
            if best
                .as_ref()
                .is_none_or(|&(_, bi, bo)| idx > bi || (idx == bi && off > bo))
            {
                best = Some((ent.path(), idx, off));
            }
        }
        Ok(best)
    }

    pub fn delete_chunk_snapshots(dir: &Path, max_idx: usize, max_off: usize) -> io::Result<()> {
        let rd = match fs::read_dir(dir) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        for ent in rd {
            let ent = ent?;
            let name = ent.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            let Some(rest) = name_str.strip_prefix(CHUNK_SNAPSHOT_PREFIX) else {
                continue;
            };
            let mut parts = rest.split('.');
            let (Some(idx_s), Some(off_s), None) = (parts.next(), parts.next(), parts.next()) else {
                continue;
            };
            let (Ok(idx), Ok(off)) = (idx_s.parse::<usize>(), off_s.parse::<usize>()) else {
                continue;
            };
            if idx < max_idx || (idx == max_idx && off < max_off) {
                let _ = fs::remove_dir_all(ent.path());
            }
        }
        Ok(())
    }

    /// Writes a chunk snapshot directory `chunk_snapshot.NNNNNN.OOOOOOOOOO` (Go `Head.ChunkSnapshot`).
    pub fn chunk_snapshot(&self) -> Result<Option<PathBuf>, HeadError> {
        let (wlast, woffset) = {
            let wal_guard = self.wal.lock();
            let Some(ref wal) = *wal_guard else {
                return Ok(None);
            };
            let (seg_opt, off) = wal.last_segment_and_offset()?;
            (seg_opt.unwrap_or(0), off)
        };
        let _snap_guard = self.chunk_snapshot_mtx.lock();
        if let Some((existing_dir, cslast, csoffset)) = Self::last_chunk_snapshot(&self.dir)? {
            if wlast == cslast && woffset == csoffset {
                return Ok(Some(existing_dir));
            }
        }
        let snap_name = format!("{CHUNK_SNAPSHOT_PREFIX}{wlast:06}.{woffset:010}");
        let cpdir = self.dir.join(&snap_name);
        let cpdirtmp = self.dir.join(format!("{snap_name}.tmp"));
        if cpdirtmp.exists() {
            let _ = fs::remove_dir_all(&cpdirtmp);
        }
        fs::create_dir_all(&cpdirtmp)?;
        let mut cp = WlogWriter::with_segment_size(
            &cpdirtmp,
            self.opts.wal_segment_size,
            self.opts.wal_compression,
        )?;

        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut stripe_entries: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            stripe_entries.clear();
            StripeSeries::collect_stripe_into(stripe, &mut stripe_entries);
            for entry in stripe_entries.drain(..) {
                buf.clear();
                let mut enc_buf = Encbuf { buf: std::mem::take(&mut buf) };
                enc_buf.put_byte(CHUNK_SNAPSHOT_RECORD_TYPE_SERIES);
                enc_buf.put_be64(entry.ref_id);
                encode_labels(&mut enc_buf, &entry.labels);
                enc_buf.put_be64_i64(0);
                {
                    let st = entry.inner.lock();
                    let active_enc = st.active_encoding();
                    if active_enc == Encoding::None {
                        enc_buf.put_uvarint(0);
                    } else {
                        enc_buf.put_uvarint(1);
                        enc_buf.put_be64_i64(st.active_min_time);
                        enc_buf.put_be64_i64(st.active_max_time());
                        enc_buf.put_byte(active_enc.as_u8());
                        enc_buf.put_uvarint_bytes(st.active_bstream.bytes());
                        match active_enc {
                            Encoding::Xor | Encoding::Xor2 => {
                                for _ in 0..3 {
                                    enc_buf.put_be64_i64(0);
                                    enc_buf.put_be_f64(0.0);
                                }
                                enc_buf.put_be64_i64(0);
                                enc_buf.put_be_f64(st.last_sample_float().unwrap_or(0.0));
                            }
                            Encoding::Histogram | Encoding::HistogramSt => {
                                let h = match st.last_sample_clone() {
                                    LastSample::Histogram(h) => h,
                                    _ => Box::default(),
                                };
                                encode_histogram(&mut enc_buf, &h);
                            }
                            Encoding::FloatHistogram | Encoding::FloatHistogramSt => {
                                let fh = match st.last_sample_clone() {
                                    LastSample::FloatHistogram(fh) => fh,
                                    _ => Box::default(),
                                };
                                encode_float_histogram(&mut enc_buf, &fh);
                            }
                            Encoding::None => {}
                        }
                    }
                }
                buf = enc_buf.into_vec();
                cp.log(&[&buf])?;
            }
        }

        // Tombstones record.
        let tomb_payload = encode_tombstones(&self.tombstones);
        buf.clear();
        let mut tomb_enc = Encbuf { buf: std::mem::take(&mut buf) };
        tomb_enc.put_byte(CHUNK_SNAPSHOT_RECORD_TYPE_TOMBSTONES);
        tomb_enc.put_uvarint_bytes(&tomb_payload);
        buf = tomb_enc.into_vec();
        cp.log(&[&buf])?;

        // Exemplars records (in circular buffer order).
        let rec_enc = RecordEncoder::new(self.opts.enable_st_storage);
        let mut ex_batch: Vec<RefExemplar> = Vec::new();
        let mut ex_err: Option<HeadError> = None;
        self.exemplars.iterate_exemplars(|series_labels, ex| {
            if ex_err.is_some() {
                return;
            }
            if ex_batch.len() >= 10_000 {
                let mut raw = rec_enc.exemplars(&ex_batch, Vec::new());
                raw[0] = CHUNK_SNAPSHOT_RECORD_TYPE_EXEMPLARS;
                if let Err(e) = cp.log(&[&raw]) {
                    ex_err = Some(e.into());
                    return;
                }
                ex_batch.clear();
            }
            if let Some(entry) = self.series.get_by_hash_and_labels(series_labels.hash(), series_labels) {
                ex_batch.push(RefExemplar {
                    ref_id: entry.ref_id,
                    t: ex.timestamp_ms,
                    v: ex.value,
                    labels: ex.labels.clone(),
                });
            }
        });
        if let Some(e) = ex_err {
            return Err(e);
        }
        if !ex_batch.is_empty() {
            let mut raw = rec_enc.exemplars(&ex_batch, Vec::new());
            raw[0] = CHUNK_SNAPSHOT_RECORD_TYPE_EXEMPLARS;
            cp.log(&[&raw])?;
        }
        cp.close()?;
        if cpdir.exists() {
            let _ = fs::remove_dir_all(&cpdir);
        }
        fs::rename(&cpdirtmp, &cpdir)?;
        let _ = Self::delete_chunk_snapshots(&self.dir, wlast, woffset);
        Ok(Some(cpdir))
    }

    fn load_chunk_snapshot(&self, snap_dir: &Path) -> Result<(), HeadError> {
        let mut r = WlogReader::open_dir(snap_dir)?;
        let rec_dec = RecordDecoder::new();
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;
        let mut max_ref = 0u64;
        let mut series_by_ref: HashMap<u64, Arc<SeriesEntry>> = HashMap::new();
        while let Some(raw) = r.next_record()? {
            if raw.is_empty() {
                return Err(HeadError::Repair("empty snapshot record".to_string()));
            }
            match raw[0] {
                CHUNK_SNAPSHOT_RECORD_TYPE_SERIES => {
                    let mut dec = Decbuf::new(&raw[1..]);
                    let ref_id = dec.be64().map_err(|e| HeadError::Repair(e.to_string()))?;
                    let labels = decode_labels(&mut dec).map_err(|e| HeadError::Repair(e.to_string()))?;
                    let _unused_cr = dec.be64_i64().map_err(|e| HeadError::Repair(e.to_string()))?;
                    let has_chunk = dec.uvarint().map_err(|e| HeadError::Repair(e.to_string()))?;
                    let hash = labels.hash();
                    let symbol_labels = {
                        let pairs: SmallVec<[(&str, &str); 24]> = labels.iter().collect();
                        self.intern_sorted_pairs_read_first(&pairs)?
                    };
                    let entry = Arc::new(SeriesEntry::new(ref_id, hash, labels, 0));
                    self.series.insert(Arc::clone(&entry));
                    self.postings.add(ref_id, &symbol_labels);
                    self.num_series.fetch_add(1, Ordering::Relaxed);
                    if let Some(ref g) = self.series_gauge {
                        g.inc();
                    }
                    if let Some(ref c) = self.series_created_total {
                        c.inc();
                    }
                    if let Some(ref cb) = *self.lifecycle_callback.read() {
                        cb.post_creation(ref_id, &entry.labels);
                    }
                    max_ref = max_ref.max(ref_id);
                    series_by_ref.insert(ref_id, Arc::clone(&entry));
                    if has_chunk != 0 {
                        let c_min = dec.be64_i64().map_err(|e| HeadError::Repair(e.to_string()))?;
                        let c_max = dec.be64_i64().map_err(|e| HeadError::Repair(e.to_string()))?;
                        let enc_byte = dec.byte().map_err(|e| HeadError::Repair(e.to_string()))?;
                        let enc = Encoding::from_u8(enc_byte).map_err(HeadError::Chunk)?;
                        if !enc.is_valid() {
                            return Err(HeadError::Chunk(ChunkError::InvalidEncoding(enc_byte)));
                        }
                        let chunk_bytes = dec.uvarint_bytes().map_err(|e| HeadError::Repair(e.to_string()))?.to_vec();
                        let chunk = EncodedChunk::from_bytes(enc, chunk_bytes)?;
                        let mut st = entry.inner.lock();
                        st.set_active_chunk(chunk);
                        st.active_min_time = c_min;
                        st.set_active_max_time(c_max);
                        st.set_next_at(c_max);
                        match enc {
                            Encoding::Xor | Encoding::Xor2 => {
                                for _ in 0..3 {
                                    let _ = dec.be64_i64().map_err(|e| HeadError::Repair(e.to_string()))?;
                                    let _ = dec.be64_f64().map_err(|e| HeadError::Repair(e.to_string()))?;
                                }
                                let _ = dec.be64_i64().map_err(|e| HeadError::Repair(e.to_string()))?;
                                let last_v = dec.be64_f64().map_err(|e| HeadError::Repair(e.to_string()))?;
                                st.set_last_sample_float(last_v);
                            }
                            Encoding::Histogram | Encoding::HistogramSt => {
                                let h = decode_histogram(&mut dec).map_err(|e| HeadError::Repair(e.to_string()))?;
                                st.set_last_sample_histogram(h);
                            }
                            Encoding::FloatHistogram | Encoding::FloatHistogramSt => {
                                let fh = decode_float_histogram(&mut dec).map_err(|e| HeadError::Repair(e.to_string()))?;
                                st.set_last_sample_float_histogram(fh);
                            }
                            Encoding::None => {}
                        }
                        let (is_stale, is_hist, buckets) = st.sample_state();
                        if is_stale {
                            self.num_stale_series.fetch_add(1, Ordering::Relaxed);
                        }
                        if is_hist {
                            self.num_native_histogram_series.fetch_add(1, Ordering::Relaxed);
                            self.num_native_histogram_buckets.fetch_add(buckets as u64, Ordering::Relaxed);
                        }
                        self.metrics.chunks_created(1);
                        entry.in_order_max_time.store(c_max, Ordering::Release);
                        mint = mint.min(c_min);
                        maxt = maxt.max(c_max);
                    }
                }
                CHUNK_SNAPSHOT_RECORD_TYPE_TOMBSTONES => {
                    let mut dec = Decbuf::new(&raw[1..]);
                    let tomb_bytes = dec.uvarint_bytes().map_err(|e| HeadError::Repair(e.to_string()))?;
                    let tr = decode_tombstones(tomb_bytes).map_err(|e| {
                        HeadError::Repair(format!("decode snapshot tombstones: {e}"))
                    })?;
                    tr.iter(|ref_id, ivs| {
                        for &iv in ivs {
                            self.tombstones.add_interval(ref_id, iv);
                        }
                    });
                }
                CHUNK_SNAPSHOT_RECORD_TYPE_EXEMPLARS => {
                    if self.opts.enable_exemplar_storage && self.exemplars.capacity() > 0 {
                        let mut ex_buf = Vec::with_capacity(raw.len());
                        ex_buf.push(crate::record::RecordType::Exemplars as u8);
                        ex_buf.extend_from_slice(&raw[1..]);
                        let exs = rec_dec.exemplars(&ex_buf)?;
                        for ex in exs {
                            if let Some(entry) = series_by_ref.get(&ex.ref_id) {
                                if self
                                    .exemplars
                                    .add_exemplar(&entry.labels, Exemplar::new(ex.labels, ex.v, ex.t))
                                    .is_ok()
                                {
                                    let mut st = entry.inner.lock();
                                    st.update_last_exemplar_ts(ex.t);
                                }
                            }
                        }
                    }
                }
                other => {
                    return Err(HeadError::Repair(format!(
                        "unsupported snapshot record type {other}"
                    )));
                }
            }
        }
        let cur_last = self.last_series_id.load(Ordering::Acquire);
        if max_ref > cur_last {
            self.last_series_id.store(max_ref, Ordering::Release);
        }
        if mint <= maxt {
            self.update_min_max_time(mint, maxt);
        }
        Ok(())
    }

    fn reset_in_memory_state_after_snapshot_err(&self) {
        let mut all_entries = Vec::new();
        let mut buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            buf.clear();
            StripeSeries::collect_stripe_into(stripe, &mut buf);
            for entry in buf.drain(..) {
                let mut st = entry.inner.lock();
                if self.series.remove_if_gced_under_entry_lock(&entry, &mut st) {
                    let chks = (st.mmapped().len() + st.head_chunks_len()) as u64;
                    self.metrics.chunks_removed(chks);
                    self.update_metrics_on_series_delete(&st);
                    drop(st);
                    all_entries.push((entry, i64::MIN));
                }
            }
        }
        self.finish_deleted_series_cleanup(all_entries);
        self.tombstones.truncate_before(i64::MAX);
        self.min_time.store(i64::MAX, Ordering::Release);
        self.max_time.store(i64::MIN, Ordering::Release);
        self.last_series_id.store(0, Ordering::Release);
    }

    /// Flushes sealed chunks to `chunks_head`, syncs and closes WAL, WBL, and `HeadChunksWriter`.
    pub fn close(&self) -> Result<(), HeadError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if !self.read_only {
            self.mmap_head_chunks()?;
            if self.enable_memory_snapshot_on_shutdown.load(Ordering::Acquire) {
                self.chunk_snapshot()?;
            }
            let _ = self.write_series_state_file(true);
        }
        if let Some(cw) = self.chunk_writer.lock().take() {
            cw.close()?;
        }
        if let Some(wal) = self.wal.lock().take() {
            wal.close()?;
        }
        if let Some(wbl) = self.wbl.lock().take() {
            wbl.close()?;
        }
        *self.chunk_reader.write() = None;
        Ok(())
    }
}

impl Drop for Head {
    fn drop(&mut self) {
        let _ = self.close();
        self.postings_cache
            .remove_source(self.postings_cache_source_id);
    }
}

/// Index reader over a [`Head`] (Go `headIndexReader`).
#[derive(Clone, Copy)]
pub struct HeadIndexReader<'a> {
    head: &'a Head,
    mint: i64,
    maxt: i64,
}

impl std::fmt::Debug for HeadIndexReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeadIndexReader")
            .field("head_ptr", &(self.head as *const Head))
            .field("mint", &self.mint)
            .field("maxt", &self.maxt)
            .finish()
    }
}

impl PartialEq for HeadIndexReader<'_> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.head, other.head) && self.mint == other.mint && self.maxt == other.maxt
    }
}

impl<'a> HeadIndexReader<'a> {
    pub fn postings(&self, name: &str, value: &str) -> Vec<u64> {
        self.head.postings_for_label_value(name, value)
    }

    pub fn postings_for_label_matching(
        &self,
        name: &str,
        mut pred: impl FnMut(&str) -> bool,
    ) -> Vec<u64> {
        let syms = self.head.symbols.read();
        let Some(name_id) = syms.get(name) else {
            return Vec::new();
        };
        self.head
            .postings
            .postings_for_label_matching_id(name_id, &syms, &mut pred)
    }

    pub fn label_values_for(&self, postings: &[u64], name: &str) -> Vec<String> {
        let syms = self.head.symbols.read();
        let mut p = crate::postings::ListPostings::new(postings.to_vec());
        self.head
            .postings
            .label_values_for(&syms, &mut p, name)
            .unwrap_or_default()
    }

    pub fn label_values_excluding(&self, postings: &[u64], name: &str) -> Vec<String> {
        let syms = self.head.symbols.read();
        let mut p = crate::postings::ListPostings::new(postings.to_vec());
        self.head
            .postings
            .label_values_excluding(&syms, &mut p, name)
            .unwrap_or_default()
    }

    pub fn label_names(&self, matchers: &[LabelMatcher]) -> Result<Vec<String>, HeadError> {
        if self.maxt < self.head.min_time() || self.mint > self.head.max_time() {
            return Ok(Vec::new());
        }
        self.head.label_names(matchers)
    }

    pub fn label_values(
        &self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, HeadError> {
        if self.maxt < self.head.min_time() || self.mint > self.head.max_time() {
            return Ok(Vec::new());
        }
        self.head.label_values(name, matchers)
    }

    pub fn series_chunk_metas_no_bytes(
        &self,
        ref_id: u64,
        filter: Option<&dyn Fn(&Labels) -> bool>,
    ) -> Result<SeriesChunkMetasWithSnapshot, HeadError> {
        self.head
            .series_chunk_metas_no_bytes_with_snapshot(ref_id, self.mint, self.maxt, filter)
    }
}

#[derive(Clone)]
struct PendingFloat {
    entry: Arc<SeriesEntry>,
    st: i64,
    t: i64,
    v: f64,
    is_ooo: bool,
}

#[derive(Clone)]
struct PendingHist {
    entry: Arc<SeriesEntry>,
    st: i64,
    t: i64,
    h: Histogram,
}

#[derive(Clone)]
struct PendingFloatHist {
    entry: Arc<SeriesEntry>,
    st: i64,
    t: i64,
    fh: FloatHistogram,
}

#[derive(Clone)]
struct PendingExemplar {
    entry: Arc<SeriesEntry>,
    exemplar: Exemplar,
}

#[derive(Clone)]
struct PendingMetadata {
    entry: Arc<SeriesEntry>,
    metric_type: MetricType,
    unit: String,
    help: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum BatchSampleType {
    Histogram = 1,
    CustomBucketHistogram = 2,
    FloatHistogram = 3,
    CustomBucketFloatHistogram = 4,
}

impl BatchSampleType {
    #[inline]
    fn from_hist(h: &Histogram) -> Self {
        if h.uses_custom_buckets() {
            Self::CustomBucketHistogram
        } else {
            Self::Histogram
        }
    }

    #[inline]
    fn from_float_hist(fh: &FloatHistogram) -> Self {
        if fh.uses_custom_buckets() {
            Self::CustomBucketFloatHistogram
        } else {
            Self::FloatHistogram
        }
    }
}

#[derive(Clone, Copy, Default)]
struct BatchBoundary {
    floats_end: usize,
    hists_end: usize,
    fhists_end: usize,
}

#[derive(Default)]
struct AppenderScratch {
    series_refs: Vec<RefSeries>,
    created_series: Vec<Arc<SeriesEntry>>,
    floats: Vec<PendingFloat>,
    histograms: Vec<PendingHist>,
    float_histograms: Vec<PendingFloatHist>,
    exemplars: Vec<PendingExemplar>,
    metadata: Vec<PendingMetadata>,
    types_in_batch: FastU64Map<BatchSampleType>,
    batch_boundaries: Vec<BatchBoundary>,
    wal_buf: Vec<u8>,
    wal_comp_buf: Vec<u8>,
}

fn merge_time_intervals(mut raw: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    if raw.is_empty() {
        return raw;
    }
    raw.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::with_capacity(raw.len());
    for (s, e) in raw {
        if let Some(last) = merged.last_mut() {
            if s <= last.1 {
                if e > last.1 {
                    last.1 = e;
                }
                continue;
            }
        }
        merged.push((s, e));
    }
    merged
}

struct PreparedWalPayloads {
    series: Option<(Vec<u8>, bool, usize)>,
    records: Vec<(Vec<u8>, bool, usize)>,
}

#[derive(Default)]
struct DeferredStaleness {
    hists: VecDeque<PendingHist>,
    fhists: VecDeque<PendingFloatHist>,
}

impl Drop for DeferredStaleness {
    fn drop(&mut self) {
        for ph in self.hists.drain(..) {
            ph.entry.pending_commits.fetch_sub(1, Ordering::Release);
        }
        for pfh in self.fhists.drain(..) {
            pfh.entry.pending_commits.fetch_sub(1, Ordering::Release);
        }
    }
}

enum PendingOooWblRecord {
    Encoded(Vec<u8>),
    MmapMarkers(Vec<(RefMmapMarker, Option<u64>)>),
}

struct CommitContext<'a> {
    head: &'a Head,
    in_order_mint: i64,
    in_order_maxt: i64,
    ooo_mint: i64,
    ooo_maxt: i64,
    enc: RecordEncoder,
    wbl_samples: Vec<RefSample>,
    wbl_histograms: Vec<RefHistogramSample>,
    wbl_float_histograms: Vec<RefFloatHistogramSample>,
    ooo_mmap_markers: HashMap<u64, Vec<(ChunkDiskMapperRef, Option<u64>)>>,
    ooo_records: Vec<PendingOooWblRecord>,
    pending_ooo_promote: Vec<Arc<SeriesEntry>>,
    float_enc: Encoding,
    store_st: bool,
    use_xor2: bool,
    use_hist_st: bool,
    append_id: u64,
    cleanup_append_ids_below: u64,
    chunk_range_ms: i64,
    samples_per_chunk: usize,
    ooo_cap_max: usize,
    floats_appended: u64,
    histograms_appended: u64,
    float_dups: u64,
    hist_dups: u64,
    float_oob: u64,
    float_ooo: u64,
    float_too_old: u64,
    hist_oob: u64,
    hist_ooo: u64,
    hist_too_old: u64,
    ooo_wbl_guard: Option<parking_lot::RwLockReadGuard<'a, ()>>,
}

impl<'a> CommitContext<'a> {
    fn new(head: &'a Head, append_id: u64, cleanup_append_ids_below: u64) -> Self {
        let float_enc = head.float_chunk_encoding();
        let ooo_wbl_guard = Some(head.ooo_wbl_gate.read());
        Self {
            head,
            in_order_mint: i64::MAX,
            in_order_maxt: i64::MIN,
            ooo_mint: i64::MAX,
            ooo_maxt: i64::MIN,
            enc: RecordEncoder::new(head.opts.enable_st_storage),
            wbl_samples: Vec::new(),
            wbl_histograms: Vec::new(),
            wbl_float_histograms: Vec::new(),
            ooo_mmap_markers: HashMap::new(),
            ooo_records: Vec::new(),
            pending_ooo_promote: Vec::new(),
            float_enc,
            store_st: head.opts.enable_st_storage,
            use_xor2: float_enc == Encoding::Xor2,
            use_hist_st: head.opts.enable_histogram_st_encoding,
            append_id,
            cleanup_append_ids_below,
            chunk_range_ms: head.opts.chunk_range_ms,
            samples_per_chunk: head.opts.samples_per_chunk,
            ooo_cap_max: head.opts.ooo_cap_max,
            floats_appended: 0,
            histograms_appended: 0,
            float_dups: 0,
            hist_dups: 0,
            float_oob: 0,
            float_ooo: 0,
            float_too_old: 0,
            hist_oob: 0,
            hist_ooo: 0,
            hist_too_old: 0,
            ooo_wbl_guard,
        }
    }

    fn on_ooo_chunk_created(
        &mut self,
        entry: &Arc<SeriesEntry>,
        mmap_refs: Vec<ChunkDiskMapperRef>,
        write_epoch: Option<u64>,
    ) {
        self.head.metrics.chunks_created(1);
        let prev_markers = self.ooo_mmap_markers.get(&entry.ref_id);
        if prev_markers.is_none() || prev_markers.is_some_and(|r| !r.is_empty()) {
            self.flush_ooo_records();
        }
        if mmap_refs.is_empty() {
            self.ooo_mmap_markers
                .insert(entry.ref_id, vec![(ChunkDiskMapperRef(0), None)]);
        } else {
            self.head.mark_ooo_unflushed(entry.ref_id);
            self.pending_ooo_promote.push(Arc::clone(entry));
            self.ooo_mmap_markers.insert(
                entry.ref_id,
                mmap_refs.into_iter().map(|r| (r, write_epoch)).collect(),
            );
        }
    }

    fn flush_ooo_records(&mut self) {
        // Write previously queued MmapMarkers (if any) BEFORE samples that belong to those
        // newly opened chunks, and flush samples from prior chunks BEFORE new MmapMarkers.
        if !self.ooo_mmap_markers.is_empty() {
            let mut markers = Vec::new();
            for (&ref_id, refs) in &self.ooo_mmap_markers {
                for &(r, ep) in refs {
                    markers.push((
                        RefMmapMarker {
                            ref_id,
                            mmap_ref: r.as_u64(),
                        },
                        ep,
                    ));
                }
            }
            if !markers.is_empty() {
                self.ooo_records
                    .push(PendingOooWblRecord::MmapMarkers(markers));
            }
            self.ooo_mmap_markers.clear();
        }
        if !self.wbl_samples.is_empty() {
            self.ooo_records.push(PendingOooWblRecord::Encoded(
                self.enc.samples(&self.wbl_samples, Vec::new()),
            ));
            self.wbl_samples.clear();
        }
        if !self.wbl_histograms.is_empty() {
            let (r, cb) = self
                .enc
                .histogram_samples(&self.wbl_histograms, Vec::new());
            if !r.is_empty() {
                self.ooo_records.push(PendingOooWblRecord::Encoded(r));
            }
            if !cb.is_empty() {
                self.ooo_records.push(PendingOooWblRecord::Encoded(
                    self.enc.custom_buckets_histogram_samples(&cb, Vec::new()),
                ));
            }
            self.wbl_histograms.clear();
        }
        if !self.wbl_float_histograms.is_empty() {
            let (r, cb) = self
                .enc
                .float_histogram_samples(&self.wbl_float_histograms, Vec::new());
            if !r.is_empty() {
                self.ooo_records.push(PendingOooWblRecord::Encoded(r));
            }
            if !cb.is_empty() {
                self.ooo_records.push(PendingOooWblRecord::Encoded(
                    self.enc
                        .custom_buckets_float_histogram_samples(&cb, Vec::new()),
                ));
            }
            self.wbl_float_histograms.clear();
        }
    }

    fn commit_one_hist(
        &mut self,
        head_maxt: i64,
        min_valid_time: i64,
        ooo_time_window: i64,
        ph: &mut PendingHist,
    ) -> Result<(), HeadError> {
        let _pending_guard = PendingCommitGuard(&ph.entry, &self.head.metrics);
        self.head.maybe_wait_commit_pause(ph.entry.ref_id);
        let mut state = TxCleanupGuard {
            state: ph.entry.inner.lock(),
            head: self.head,
            ref_id: ph.entry.ref_id,
            bound: self.cleanup_append_ids_below,
        };
        let check = {
            let cr_guard = self.head.chunk_reader.read();
            state.appendable_histogram(
                ph.t,
                &ph.h,
                head_maxt,
                min_valid_time,
                ooo_time_window,
                cr_guard.as_ref(),
            )
        };
        let (is_ooo, is_dup) = match check {
            Ok(v) => v,
            Err(HeadError::OutOfBounds) => {
                self.hist_oob += 1;
                return Ok(());
            }
            Err(HeadError::OutOfOrderSample) => {
                self.hist_ooo += 1;
                return Ok(());
            }
            Err(HeadError::TooOldSample) => {
                self.hist_too_old += 1;
                return Ok(());
            }
            Err(
                HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_),
            ) => {
                self.hist_dups += 1;
                return Ok(());
            }
            Err(_) => return Ok(()),
        };
        if is_dup {
            self.hist_dups += 1;
            return Ok(());
        }
        if is_ooo {
            let mut cw_guard = self.head.chunk_writer.lock();
            let (ok, chunk_created, mmap_refs, write_epoch) = state.insert_ooo(
                ph.entry.ref_id,
                ph.st,
                ph.t,
                OooSampleValue::Histogram(ph.h.clone()),
                self.ooo_cap_max,
                self.use_xor2,
                self.use_hist_st,
                cw_guard.as_mut(),
            )?;
            drop(cw_guard);
            if chunk_created {
                self.on_ooo_chunk_created(&ph.entry, mmap_refs, write_epoch);
            }
            if ok {
                self.head
                    .metrics
                    .ooo_samples_appended
                    .with_label_values(&["histogram"])
                    .inc();
                self.wbl_histograms.push(RefHistogramSample {
                    ref_id: ph.entry.ref_id,
                    st: ph.st,
                    t: ph.t,
                    h: std::mem::take(&mut ph.h),
                });
                self.ooo_mint = self.ooo_mint.min(ph.t);
                self.ooo_maxt = self.ooo_maxt.max(ph.t);
                self.histograms_appended += 1;
            } else {
                self.hist_dups += 1;
            }
        } else {
            let (was_stale, was_hist, old_buckets) = state.sample_state();
            if self.head.metrics.track_append(state.append_histogram_in_order(
                ph.st,
                ph.t,
                &ph.h,
                self.chunk_range_ms,
                self.use_hist_st,
            )?) {
                self.head.txs_add(ph.entry.ref_id, self.append_id);
                self.in_order_mint = self.in_order_mint.min(ph.t);
                self.in_order_maxt = self.in_order_maxt.max(ph.t);
                self.histograms_appended += 1;
                let new_buckets = ph.h.positive_buckets.len() + ph.h.negative_buckets.len();
                self.head
                    .update_stale_series_metric_on_append(was_stale, is_stale_nan(ph.h.sum));
                self.head.update_native_histogram_metrics_on_append(
                    was_hist,
                    true,
                    old_buckets,
                    new_buckets,
                );
                ph.entry
                    .in_order_max_time
                    .store(state.max_time(), Ordering::Release);
            } else {
                self.hist_dups += 1;
            }
        }
        Ok(())
    }

    fn commit_one_float_hist(
        &mut self,
        head_maxt: i64,
        min_valid_time: i64,
        ooo_time_window: i64,
        pfh: &mut PendingFloatHist,
    ) -> Result<(), HeadError> {
        let _pending_guard = PendingCommitGuard(&pfh.entry, &self.head.metrics);
        self.head.maybe_wait_commit_pause(pfh.entry.ref_id);
        let mut state = TxCleanupGuard {
            state: pfh.entry.inner.lock(),
            head: self.head,
            ref_id: pfh.entry.ref_id,
            bound: self.cleanup_append_ids_below,
        };
        let check = {
            let cr_guard = self.head.chunk_reader.read();
            state.appendable_float_histogram(
                pfh.t,
                &pfh.fh,
                head_maxt,
                min_valid_time,
                ooo_time_window,
                cr_guard.as_ref(),
            )
        };
        let (is_ooo, is_dup) = match check {
            Ok(v) => v,
            Err(HeadError::OutOfBounds) => {
                self.hist_oob += 1;
                return Ok(());
            }
            Err(HeadError::OutOfOrderSample) => {
                self.hist_ooo += 1;
                return Ok(());
            }
            Err(HeadError::TooOldSample) => {
                self.hist_too_old += 1;
                return Ok(());
            }
            Err(
                HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_),
            ) => {
                self.hist_dups += 1;
                return Ok(());
            }
            Err(_) => return Ok(()),
        };
        if is_dup {
            self.hist_dups += 1;
            return Ok(());
        }
        if is_ooo {
            let mut cw_guard = self.head.chunk_writer.lock();
            let (ok, chunk_created, mmap_refs, write_epoch) = state.insert_ooo(
                pfh.entry.ref_id,
                pfh.st,
                pfh.t,
                OooSampleValue::FloatHistogram(pfh.fh.clone()),
                self.ooo_cap_max,
                self.use_xor2,
                self.use_hist_st,
                cw_guard.as_mut(),
            )?;
            drop(cw_guard);
            if chunk_created {
                self.on_ooo_chunk_created(&pfh.entry, mmap_refs, write_epoch);
            }
            if ok {
                self.head
                    .metrics
                    .ooo_samples_appended
                    .with_label_values(&["histogram"])
                    .inc();
                self.wbl_float_histograms.push(RefFloatHistogramSample {
                    ref_id: pfh.entry.ref_id,
                    st: pfh.st,
                    t: pfh.t,
                    fh: std::mem::take(&mut pfh.fh),
                });
                self.ooo_mint = self.ooo_mint.min(pfh.t);
                self.ooo_maxt = self.ooo_maxt.max(pfh.t);
                self.histograms_appended += 1;
            } else {
                self.hist_dups += 1;
            }
        } else {
            let (was_stale, was_hist, old_buckets) = state.sample_state();
            if self
                .head
                .metrics
                .track_append(state.append_float_histogram_in_order(
                    pfh.st,
                    pfh.t,
                    &pfh.fh,
                    self.chunk_range_ms,
                    self.use_hist_st,
                )?)
            {
                self.head.txs_add(pfh.entry.ref_id, self.append_id);
                self.in_order_mint = self.in_order_mint.min(pfh.t);
                self.in_order_maxt = self.in_order_maxt.max(pfh.t);
                self.histograms_appended += 1;
                let new_buckets = pfh.fh.positive_buckets.len() + pfh.fh.negative_buckets.len();
                self.head
                    .update_stale_series_metric_on_append(was_stale, is_stale_nan(pfh.fh.sum));
                self.head.update_native_histogram_metrics_on_append(
                    was_hist,
                    true,
                    old_buckets,
                    new_buckets,
                );
                pfh.entry
                    .in_order_max_time
                    .store(state.max_time(), Ordering::Release);
            } else {
                self.hist_dups += 1;
            }
        }
        Ok(())
    }

    fn finish(mut self, apply_err: Option<HeadError>) -> Result<(), HeadError> {
        let _ = (self.hist_oob, self.hist_too_old);
        let m = &self.head.metrics;
        m.out_of_order_samples
            .with_label_values(&["float"])
            .inc_by(self.float_ooo as f64);
        m.out_of_order_samples
            .with_label_values(&["histogram"])
            .inc_by(self.hist_ooo as f64);
        m.out_of_bound_samples
            .with_label_values(&["float"])
            .inc_by(self.float_oob as f64);
        m.too_old_samples
            .with_label_values(&["float"])
            .inc_by(self.float_too_old as f64);
        m.samples_appended
            .with_label_values(&["float"])
            .inc_by(self.floats_appended as f64);
        m.samples_appended
            .with_label_values(&["histogram"])
            .inc_by(self.histograms_appended as f64);
        m.duplicate_samples
            .with_label_values(&["float"])
            .inc_by(self.float_dups as f64);
        m.duplicate_samples
            .with_label_values(&["histogram"])
            .inc_by(self.hist_dups as f64);

        if self.in_order_mint <= self.in_order_maxt {
            self.head
                .update_min_max_time(self.in_order_mint, self.in_order_maxt);
        }

        self.flush_ooo_records();

        let need_ooo_flush = !self.pending_ooo_promote.is_empty()
            || self.head.has_ooo_unflushed.load(Ordering::Acquire);
        let mut entries_to_promote: Vec<Arc<SeriesEntry>> = Vec::new();
        let mut initial_last_flushed = ChunkDiskMapperRef(0);
        let mut rewrite_err: Option<HeadError> = None;

        if need_ooo_flush {
            let mut seen_promote_ids: HashSet<u64> =
                HashSet::with_capacity(self.pending_ooo_promote.len() + 4);
            for e in &self.pending_ooo_promote {
                if seen_promote_ids.insert(e.ref_id) {
                    entries_to_promote.push(Arc::clone(e));
                }
            }
            let dirty_ids: Vec<u64> = self
                .head
                .ooo_unflushed_series
                .lock()
                .iter()
                .copied()
                .collect();
            for id in dirty_ids {
                if let Some(entry) = self.head.series.get_by_id(id) {
                    if seen_promote_ids.insert(id) {
                        entries_to_promote.push(entry);
                    }
                } else {
                    self.head.clear_ooo_unflushed(id);
                }
            }

            if let Some(ref cw) = *self.head.chunk_writer.lock() {
                initial_last_flushed = cw.flushed_watermark().last_flushed_ref();
            }

            for entry in &entries_to_promote {
                let mut st = entry.inner.lock();
                let Some(ref mut ooo) = st.ooo_mut() else {
                    self.head.clear_ooo_unflushed(entry.ref_id);
                    continue;
                };
                if ooo.ooo_pending_mmapped.is_empty() {
                    self.head.clear_ooo_unflushed(entry.ref_id);
                    continue;
                }
                let mut cw_guard = self.head.chunk_writer.lock();
                let Some(ref mut cw) = *cw_guard else {
                    continue;
                };
                if let Err(e) = Head::rewrite_discarded_ooo_pending_under_locks(
                    entry.ref_id,
                    ooo,
                    cw,
                ) {
                    rewrite_err = Some(e.into());
                    break;
                }
            }
        }

        let mut post_wm: Option<FlushedRefWatermark> = None;
        let flush_res: Result<(), HeadError> = if let Some(err) = rewrite_err {
            if let Some(ref cw) = *self.head.chunk_writer.lock() {
                post_wm = Some(cw.flushed_watermark());
            }
            Err(err)
        } else if !entries_to_promote.is_empty() {
            if let Some(ref mut cw) = *self.head.chunk_writer.lock() {
                let res = cw.flush().map_err(HeadError::from);
                post_wm = Some(cw.flushed_watermark());
                res
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };

        if !self.ooo_records.is_empty() {
            let mut materialized: Vec<Vec<u8>> = Vec::with_capacity(self.ooo_records.len());
            for rec in self.ooo_records {
                match rec {
                    PendingOooWblRecord::Encoded(bytes) => materialized.push(bytes),
                    PendingOooWblRecord::MmapMarkers(markers) => {
                        let mut kept = Vec::with_capacity(markers.len());
                        for (m, ep_opt) in markers {
                            if m.mmap_ref == 0
                                || ep_opt.is_some_and(|ep| {
                                    post_wm.as_ref().is_some_and(|w| {
                                        w.is_flushed(ChunkDiskMapperRef(m.mmap_ref), Some(ep))
                                    })
                                })
                            {
                                kept.push(m);
                            }
                        }
                        if !kept.is_empty() {
                            materialized.push(self.enc.mmap_markers(&kept, Vec::new()));
                        }
                    }
                }
            }
            if !materialized.is_empty() {
                if let Some(ref mut wbl) = *self.head.wbl.lock() {
                    let slices: Vec<&[u8]> = materialized.iter().map(|v| v.as_slice()).collect();
                    if !slices.is_empty() {
                        if let Err(err) = wbl.log(&slices) {
                            tracing::error!(err = %err, "Failed to log out of order samples into the WAL");
                        }
                    }
                }
            }
        }
        drop(self.ooo_wbl_guard.take());

        let mut refresh_res: Result<(), HeadError> = Ok(());
        if !entries_to_promote.is_empty() {
            if let Some(ref wm) = post_wm {
                if flush_res.is_ok() || wm.last_flushed_ref() > initial_last_flushed {
                    match self.head.refresh_chunk_reader() {
                        Ok(()) => {
                            for entry in entries_to_promote {
                                let mut st = entry.inner.lock();
                                if let Some(ref mut ooo) = st.ooo_mut() {
                                    if ooo.promote_pending_with_watermark(wm) {
                                        self.head.clear_ooo_unflushed(entry.ref_id);
                                    }
                                } else {
                                    self.head.clear_ooo_unflushed(entry.ref_id);
                                }
                            }
                        }
                        Err(e) => refresh_res = Err(e.into()),
                    }
                }
            }
        }
        if self.ooo_mint <= self.ooo_maxt {
            self.head
                .update_min_ooo_max_ooo_time(self.ooo_mint, self.ooo_maxt);
        }

        if let Some(err) = apply_err {
            return Err(err);
        }
        flush_res?;
        refresh_res?;
        Ok(())
    }
}

/// Notification hook invoked when a DB-backed appender commits onto a compactable Head.
pub(crate) trait CompactNotifier: Send + Sync {
    fn notify_compact(&self);
}

/// Transactional write appender for [`Head`] matching `tsdb.headAppender`.
pub struct HeadAppender<'a> {
    head: &'a Head,
    min_valid_time: i64,
    head_maxt: i64,
    ooo_time_window: i64,
    series_refs: Vec<RefSeries>,
    created_series: Vec<Arc<SeriesEntry>>,
    floats: Vec<PendingFloat>,
    histograms: Vec<PendingHist>,
    float_histograms: Vec<PendingFloatHist>,
    exemplars: Vec<PendingExemplar>,
    metadata: Vec<PendingMetadata>,
    types_in_batch: FastU64Map<BatchSampleType>,
    batch_boundaries: Vec<BatchBoundary>,
    floats_committed: usize,
    hists_committed: usize,
    fhists_committed: usize,
    wal_buf: Vec<u8>,
    wal_comp_buf: Vec<u8>,
    append_id: u64,
    cleanup_append_ids_below: u64,
    compact_notifier: Option<&'a dyn CompactNotifier>,
    closed: bool,
}

impl<'a> HeadAppender<'a> {
    #[inline]
    pub(crate) fn with_compact_notifier(mut self, notifier: &'a dyn CompactNotifier) -> Self {
        self.compact_notifier = Some(notifier);
        self
    }

    /// Port of Go `headAppenderBase.zeroOrST` (`head_append.go:569-577`):
    /// forces `st = 0` when `!enable_st_storage` so in-memory chunks match WAL replay.
    #[inline]
    fn zero_or_st(&self, st: i64) -> i64 {
        if self.head.opts.enable_st_storage {
            st
        } else {
            0
        }
    }

    fn cut_batch(&mut self) {
        let f_end = self.floats.len();
        let h_end = self.histograms.len();
        let fh_end = self.float_histograms.len();
        let prev = self.batch_boundaries.last().copied().unwrap_or_default();
        if f_end > prev.floats_end || h_end > prev.hists_end || fh_end > prev.fhists_end {
            self.batch_boundaries.push(BatchBoundary {
                floats_end: f_end,
                hists_end: h_end,
                fhists_end: fh_end,
            });
        }
        self.types_in_batch.clear();
    }

    #[inline]
    fn record_hist_in_batch(&mut self, ref_id: u64, ht: BatchSampleType) {
        if let Some(&prev) = self.types_in_batch.get(&ref_id) {
            if prev != ht {
                self.cut_batch();
                self.types_in_batch.insert(ref_id, ht);
            }
        } else {
            self.types_in_batch.insert(ref_id, ht);
        }
    }


    fn release_remaining_pending_commits(&mut self) {
        if self.floats_committed < self.floats.len() {
            for pf in &self.floats[self.floats_committed..] {
                if !pf.entry.unmark_pending_commit() {
                    self.head.metrics.pending_commit_underflow.inc();
                }
            }
        }
        self.floats.clear();
        self.floats_committed = 0;
        if self.hists_committed < self.histograms.len() {
            for ph in &self.histograms[self.hists_committed..] {
                if !ph.entry.unmark_pending_commit() {
                    self.head.metrics.pending_commit_underflow.inc();
                }
            }
        }
        self.histograms.clear();
        self.hists_committed = 0;
        if self.fhists_committed < self.float_histograms.len() {
            for pfh in &self.float_histograms[self.fhists_committed..] {
                if !pfh.entry.unmark_pending_commit() {
                    self.head.metrics.pending_commit_underflow.inc();
                }
            }
        }
        self.float_histograms.clear();
        self.fhists_committed = 0;
        for entry in self.created_series.drain(..) {
            if !entry.unmark_pending_commit() {
                self.head.metrics.pending_commit_underflow.inc();
            }
        }
    }

    fn return_scratch(&mut self) {
        self.series_refs.clear();
        self.created_series.clear();
        self.floats.clear();
        self.histograms.clear();
        self.float_histograms.clear();
        self.exemplars.clear();
        self.metadata.clear();
        self.types_in_batch.clear();
        self.batch_boundaries.clear();
        self.wal_buf.clear();
        self.wal_comp_buf.clear();
        self.floats_committed = 0;
        self.hists_committed = 0;
        self.fhists_committed = 0;
        self.append_id = 0;
        self.cleanup_append_ids_below = 0;
        if self.series_refs.capacity() > 16_384 {
            self.series_refs = Vec::new();
        }
        if self.created_series.capacity() > 16_384 {
            self.created_series = Vec::new();
        }
        if self.floats.capacity() > 16_384 {
            self.floats = Vec::new();
        }
        if self.histograms.capacity() > 16_384 {
            self.histograms = Vec::new();
        }
        if self.float_histograms.capacity() > 16_384 {
            self.float_histograms = Vec::new();
        }
        if self.exemplars.capacity() > 16_384 {
            self.exemplars = Vec::new();
        }
        if self.metadata.capacity() > 16_384 {
            self.metadata = Vec::new();
        }
        if self.types_in_batch.capacity() > 16_384 {
            self.types_in_batch = FastU64Map::default();
        }
        if self.wal_buf.capacity() > 512 * 1024 {
            self.wal_buf = Vec::new();
        }
        if self.wal_comp_buf.capacity() > 512 * 1024 {
            self.wal_comp_buf = Vec::new();
        }
        let mut pool = self.head.appender_pool.lock();
        if pool.len() < 32 {
            pool.push(AppenderScratch {
                series_refs: std::mem::take(&mut self.series_refs),
                created_series: std::mem::take(&mut self.created_series),
                floats: std::mem::take(&mut self.floats),
                histograms: std::mem::take(&mut self.histograms),
                float_histograms: std::mem::take(&mut self.float_histograms),
                exemplars: std::mem::take(&mut self.exemplars),
                metadata: std::mem::take(&mut self.metadata),
                types_in_batch: std::mem::take(&mut self.types_in_batch),
                batch_boundaries: std::mem::take(&mut self.batch_boundaries),
                wal_buf: std::mem::take(&mut self.wal_buf),
                wal_comp_buf: std::mem::take(&mut self.wal_comp_buf),
            });
        }
    }


    fn resolve_sorted_pairs_prehashed(
        &mut self,
        series_ref: Option<u64>,
        prehashed: Option<u64>,
        pairs: &[(&str, &str)],
    ) -> Result<Arc<SeriesEntry>, HeadError> {
        if let Some(r) = series_ref {
            let looked = self.head.series.get_by_id(r);
            let hook = self.head.after_series_lookup_hook.lock().clone();
            if let Some(hook) = hook {
                hook(looked.as_deref());
            }
            if let Some(e) = looked {
                if !e.is_gced() {
                    return Ok(e);
                }
            }
        }

        if let Some(hash) = prehashed {
            if !pairs.is_empty() {
                let (entry, created) = self.head.get_or_create_sorted_pairs(hash, pairs, 1)?;
                if created {
                    self.series_refs.push(RefSeries {
                        ref_id: entry.ref_id,
                        labels: entry.labels.clone(),
                    });
                    self.created_series.push(Arc::clone(&entry));
                }
                return Ok(entry);
            }
        }

        // Fast check if pairs are already sorted, unique, and non-empty by label name.
        let needs_clean = pairs.iter().any(|(_, v)| v.is_empty())
            || !pairs.windows(2).all(|w| w[0].0 < w[1].0);
        let mut sorted: SmallVec<[(&str, &str); 16]> = SmallVec::new();
        let sorted_slice: &[(&str, &str)] = if !needs_clean {
            pairs
        } else {
            sorted.extend(pairs.iter().copied().filter(|(_, v)| !v.is_empty()));
            sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
            for w in sorted.windows(2) {
                if w[0].0 == w[1].0 {
                    return Err(HeadError::DuplicateLabelName(w[0].0.to_string()));
                }
            }
            &sorted
        };
        if sorted_slice.is_empty() {
            return Err(HeadError::EmptyLabelset);
        }

        let hash = hash_sorted_pairs(sorted_slice);
        let (entry, created) = self.head.get_or_create_sorted_pairs(hash, sorted_slice, 1)?;
        if created {
            self.series_refs.push(RefSeries {
                ref_id: entry.ref_id,
                labels: entry.labels.clone(),
            });
            self.created_series.push(Arc::clone(&entry));
        }
        Ok(entry)
    }

    fn resolve_encoded_labels_prehashed(
        &mut self,
        prehashed: u64,
        encoded_labels: &[u8],
    ) -> Result<Arc<SeriesEntry>, HeadError> {
        if encoded_labels.is_empty() {
            return Err(HeadError::EmptyLabelset);
        }
        let (entry, created) = self
            .head
            .get_or_create_encoded_labels(prehashed, encoded_labels, 1)?;
        if created {
            self.series_refs.push(RefSeries {
                ref_id: entry.ref_id,
                labels: entry.labels.clone(),
            });
            self.created_series.push(Arc::clone(&entry));
        }
        Ok(entry)
    }

    fn resolve_sorted_pairs(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
    ) -> Result<Arc<SeriesEntry>, HeadError> {
        self.resolve_sorted_pairs_prehashed(series_ref, None, pairs)
    }

    /// Appends a float sample using raw `(&str, &str)` pairs (zero heap allocation on existing series!).
    pub fn append_float(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        t: i64,
        v: f64,
    ) -> Result<u64, HeadError> {
        self.append_float_with_st(series_ref, pairs, 0, t, v)
    }

    /// Appends a float sample with an optional start timestamp `st`.
    pub fn append_float_with_st(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
    ) -> Result<u64, HeadError> {
        self.append_float_inner(series_ref, pairs, st, t, v, false)
    }

    /// Appends a float sample, rejecting out-of-order samples even when OOO is enabled.
    pub fn append_float_reject_ooo(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        t: i64,
        v: f64,
    ) -> Result<u64, HeadError> {
        self.append_float_inner(series_ref, pairs, 0, t, v, true)
    }

    /// Appends a float sample using a cached `Option<Arc<SeriesEntry>>`, resolving and caching on creation.
    #[inline]
    pub fn append_float_with_entry(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        self.append_float_with_entry_prehashed(series_entry, None, pairs, st, t, v, reject_ooo)
    }

    #[inline]
    fn check_entry_appendable_float(
        &self,
        state: &mut SeriesState,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<(bool, bool), HeadError> {
        let res = if state.needs_chunk_reader_for_t(t) {
            let cr_guard = self.head.chunk_reader.read();
            state.appendable_float(
                t,
                v,
                self.head_maxt,
                self.min_valid_time,
                self.ooo_time_window,
                cr_guard.as_ref(),
            )
        } else {
            state.appendable_float(
                t,
                v,
                self.head_maxt,
                self.min_valid_time,
                self.ooo_time_window,
                None,
            )
        };
        if reject_ooo && matches!(res, Ok((true, _)) | Err(HeadError::TooOldSample)) {
            self.head
                .metrics
                .record_append_err(&HeadError::OutOfOrderSample, "float");
            return Err(HeadError::OutOfOrderSample);
        }
        match res {
            Ok((is_ooo, is_dup)) => {
                if is_ooo && v.to_bits() != QUIET_ZERO_NAN_BITS {
                    let delta = self.head_maxt.saturating_sub(t);
                    if delta > 0 {
                        self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                    }
                }
                Ok((is_ooo, is_dup))
            }
            Err(e) => {
                // Go `memSeries.appendable` returns no delta with a duplicate sample error.
                if v.to_bits() != QUIET_ZERO_NAN_BITS && !e.is_duplicate_sample() {
                    let delta = self.head_maxt.saturating_sub(t);
                    if delta > 0 {
                        self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                    }
                }
                self.head.metrics.record_append_err(&e, "float");
                Err(e)
            }
        }
    }

    /// Returns a borrowed reference to the most recently appended float `SeriesEntry`
    /// without cloning its `Arc`.
    #[inline]
    pub fn last_float_entry(&self) -> Option<&SeriesEntry> {
        self.floats.last().map(|pf| &*pf.entry)
    }

    /// Appends a float sample using a cached `Option<Arc<SeriesEntry>>` and optional precomputed Prometheus hash
    /// (for already-sorted, unique, non-empty `pairs`).
    #[allow(clippy::too_many_arguments)]
    pub fn append_float_with_entry_prehashed(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        prehashed: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        self.append_float_with_entry_prehashed_inner(
            series_entry,
            false,
            prehashed,
            pairs,
            st,
            t,
            v,
            reject_ooo,
        )
    }

    /// Same as [`append_float_with_entry_prehashed`], but moves `series_entry` by value into `self.floats`
    /// on success to avoid a second `Arc::clone`/`Arc::drop` pair.
    #[allow(clippy::too_many_arguments)]
    pub fn append_float_with_entry_prehashed_take(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        prehashed: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        self.append_float_with_entry_prehashed_inner(
            series_entry,
            true,
            prehashed,
            pairs,
            st,
            t,
            v,
            reject_ooo,
        )
    }

    /// Appends a float sample using a cached `Option<Arc<SeriesEntry>>`, precomputed Prometheus hash,
    /// and pre-encoded `stringlabels` bytes (zero re-encoding on creation, zero label inspection on hit).
    #[allow(clippy::too_many_arguments)]
    pub fn append_float_with_entry_prehashed_encoded(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        prehashed: u64,
        encoded_labels: &[u8],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        self.append_float_with_entry_prehashed_encoded_inner(
            series_entry,
            false,
            prehashed,
            encoded_labels,
            st,
            t,
            v,
            reject_ooo,
        )
    }

    /// Same as [`append_float_with_entry_prehashed_encoded`], but moves `series_entry` by value into
    /// `self.floats` on success to avoid a second `Arc::clone`/`Arc::drop` pair.
    #[allow(clippy::too_many_arguments)]
    pub fn append_float_with_entry_prehashed_encoded_take(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        prehashed: u64,
        encoded_labels: &[u8],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        self.append_float_with_entry_prehashed_encoded_inner(
            series_entry,
            true,
            prehashed,
            encoded_labels,
            st,
            t,
            v,
            reject_ooo,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn append_float_with_entry_prehashed_encoded_inner(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        take_entry: bool,
        prehashed: u64,
        encoded_labels: &[u8],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        let st = self.zero_or_st(st);
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        if self.ooo_time_window == 0 && t < self.min_valid_time {
            self.head
                .metrics
                .out_of_bound_samples
                .with_label_values(&["float"])
                .inc();
            return Err(HeadError::OutOfBounds);
        }

        loop {
            let entry = match series_entry.take() {
                Some(e) => {
                    if !take_entry {
                        *series_entry = Some(Arc::clone(&e));
                    }
                    e
                }
                None => {
                    let e = self.resolve_encoded_labels_prehashed(prehashed, encoded_labels)?;
                    if !take_entry {
                        *series_entry = Some(Arc::clone(&e));
                    }
                    e
                }
            };
            if let Some(ref_id) = self.append_float_with_resolved_entry_inner(
                series_entry,
                take_entry,
                entry,
                st,
                t,
                v,
                reject_ooo,
            )? {
                return Ok(ref_id);
            }
            // Entry was GCed before acquiring pending_commits; clear cached entry and re-resolve.
            *series_entry = None;
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_float_with_entry_prehashed_inner(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        take_entry: bool,
        prehashed: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        let st = self.zero_or_st(st);
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        if self.ooo_time_window == 0 && t < self.min_valid_time {
            self.head
                .metrics
                .out_of_bound_samples
                .with_label_values(&["float"])
                .inc();
            return Err(HeadError::OutOfBounds);
        }

        loop {
            let entry = match series_entry.take() {
                Some(e) => {
                    if !take_entry {
                        *series_entry = Some(Arc::clone(&e));
                    }
                    e
                }
                None => {
                    let e = self.resolve_sorted_pairs_prehashed(None, prehashed, pairs)?;
                    if !take_entry {
                        *series_entry = Some(Arc::clone(&e));
                    }
                    e
                }
            };
            if let Some(ref_id) = self.append_float_with_resolved_entry_inner(
                series_entry,
                take_entry,
                entry,
                st,
                t,
                v,
                reject_ooo,
            )? {
                return Ok(ref_id);
            }
            // Entry was GCed before acquiring pending_commits; clear cached entry and re-resolve.
            *series_entry = None;
        }
    }

    /// Returns `Ok(Some(ref_id))` on success, `Ok(None)` if `entry` was GCed before reservation
    /// (caller must clear `*series_entry = None` and re-resolve).
    #[allow(clippy::too_many_arguments)]
    fn append_float_with_resolved_entry_inner(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        take_entry: bool,
        entry: Arc<SeriesEntry>,
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<Option<u64>, HeadError> {
        let ref_id = entry.ref_id;

        // Go `headAppender.Append` (`head_append.go:446-463`): if a histogram or float histogram
        // was appended earlier in the current batch on this same series, convert `StaleNaN`
        // directly to a stale histogram / float histogram so both WAL and memory stay ordered.
        if is_stale_nan(v) && !self.types_in_batch.is_empty() {
            if let Some(&prev_ht) = self.types_in_batch.get(&ref_id) {
                return match prev_ht {
                    BatchSampleType::Histogram | BatchSampleType::CustomBucketHistogram => {
                        let h = Histogram {
                            sum: v,
                            ..Histogram::default()
                        };
                        self.append_histogram_with_resolved_entry(
                            series_entry,
                            take_entry,
                            entry,
                            st,
                            t,
                            h,
                            self.ooo_time_window,
                            reject_ooo,
                        )
                    }
                    BatchSampleType::FloatHistogram
                    | BatchSampleType::CustomBucketFloatHistogram => {
                        let fh = FloatHistogram {
                            sum: v,
                            ..FloatHistogram::default()
                        };
                        self.append_float_histogram_with_resolved_entry(
                            series_entry,
                            take_entry,
                            entry,
                            st,
                            t,
                            fh,
                            self.ooo_time_window,
                            reject_ooo,
                        )
                    }
                };
            }
        }

        // Lock-free single-lock fast path: claim `pending_commits` 0 -> 1 and check `t > in_order_max_time`.
        // `commit()` updates `in_order_max_time` before decrementing `pending_commits` (`Release`).
        if !is_stale_nan(v)
            && t >= self.min_valid_time
            && entry
                .pending_commits
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            if entry.is_gced() {
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                return Ok(None);
            }
            let max_t = entry.in_order_max_time.load(Ordering::Acquire);
            if t > max_t {
                self.floats.push(PendingFloat {
                    entry,
                    st,
                    t,
                    v,
                    is_ooo: false,
                });
                return Ok(Some(ref_id));
            }
            let res = {
                let mut state = entry.inner.lock();
                if entry.is_gced() {
                    drop(state);
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    return Ok(None);
                }
                self.check_entry_appendable_float(&mut state, t, v, reject_ooo)
            };
            match res {
                Ok((is_ooo, is_dup)) => {
                    if is_dup {
                        self.head.metrics.exact_duplicate_dropped("float");
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
                        if self.cleanup_append_ids_below > 0 {
                            self.head.txs_cleanup_below(ref_id, self.cleanup_append_ids_below);
                        }
                        if take_entry {
                            *series_entry = Some(entry);
                        }
                        return Ok(Some(ref_id));
                    }
                    self.floats.push(PendingFloat {
                        entry,
                        st,
                        t,
                        v,
                        is_ooo,
                    });
                    return Ok(Some(ref_id));
                }
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    if take_entry {
                        *series_entry = Some(entry);
                    }
                    return Err(e);
                }
            }
        }

        if !entry.try_inc_pending_commit() {
            return Ok(None);
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }
        // Validate strictly against already-committed SeriesState (matching Go headAppenderV2);
        // intra-batch duplicate/OOO/conflicting samples are dropped silently during commit().
        let is_ooo = {
            let res = {
                let mut state = entry.inner.lock();
                if entry.is_gced() {
                    drop(state);
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    return Ok(None);
                }
                self.check_entry_appendable_float(&mut state, t, v, reject_ooo)
            };
            match res {
                Ok((is_ooo, is_dup)) => {
                    if is_dup {
                        self.head.metrics.exact_duplicate_dropped("float");
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
                        if self.cleanup_append_ids_below > 0 {
                            self.head.txs_cleanup_below(ref_id, self.cleanup_append_ids_below);
                        }
                        if take_entry {
                            *series_entry = Some(entry);
                        }
                        return Ok(Some(ref_id));
                    }
                    is_ooo
                }
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    if take_entry {
                        *series_entry = Some(entry);
                    }
                    return Err(e);
                }
            }
        };

        if !self.types_in_batch.is_empty() && self.types_in_batch.contains_key(&ref_id) {
            self.cut_batch();
        }
        self.floats.push(PendingFloat {
            entry,
            st,
            t,
            v,
            is_ooo,
        });
        Ok(Some(ref_id))
    }

    /// Appends a float sample with an optional start timestamp `st`, rejecting out-of-order samples.
    pub fn append_float_with_st_reject_ooo(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
    ) -> Result<u64, HeadError> {
        self.append_float_inner(series_ref, pairs, st, t, v, true)
    }

    fn has_pending_sample_at_or_after(&self, ref_id: u64, ts: i64) -> bool {
        self.floats.iter().any(|p| p.entry.ref_id == ref_id && p.t >= ts)
            || self.histograms.iter().any(|p| p.entry.ref_id == ref_id && p.t >= ts)
            || self.float_histograms.iter().any(|p| p.entry.ref_id == ref_id && p.t >= ts)
    }

    fn best_effort_append_st_zero_float(&mut self, entry: Arc<SeriesEntry>, orig_st: i64, t: i64) {
        if orig_st == 0 || orig_st >= t || orig_st < self.min_valid_time {
            return;
        }
        if self.has_pending_sample_at_or_after(entry.ref_id, orig_st) {
            return;
        }
        if !entry.try_inc_pending_commit() {
            return;
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return;
        }
        let ok = {
            let state = entry.inner.lock();
            let cr_guard = if state.needs_chunk_reader_for_t(orig_st) {
                Some(self.head.chunk_reader.read())
            } else {
                None
            };
            let res = state.appendable_float(
                orig_st,
                0.0,
                self.head_maxt,
                self.min_valid_time,
                self.ooo_time_window,
                cr_guard.as_ref().and_then(|g| g.as_ref()),
            );
            matches!(res, Ok((false, false)))
        };
        if !ok {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return;
        }
        let st = self.zero_or_st(orig_st);
        if !self.types_in_batch.is_empty() && self.types_in_batch.contains_key(&entry.ref_id) {
            self.cut_batch();
        }
        self.floats.push(PendingFloat {
            entry,
            st,
            t: orig_st,
            v: 0.0,
            is_ooo: false,
        });
    }

    fn best_effort_append_st_zero_hist(
        &mut self,
        entry: Arc<SeriesEntry>,
        orig_st: i64,
        t: i64,
        h: &Histogram,
    ) {
        if orig_st == 0 || orig_st >= t || orig_st < self.min_valid_time {
            return;
        }
        if self.has_pending_sample_at_or_after(entry.ref_id, orig_st) {
            return;
        }
        let zero_h = Histogram {
            counter_reset_hint: crate::chunkenc::CounterResetHint::CounterReset,
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            custom_values: h.custom_values.clone(),
            ..Histogram::default()
        };
        if !entry.try_inc_pending_commit() {
            return;
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return;
        }
        let ok = {
            let state = entry.inner.lock();
            let cr_guard = if state.needs_chunk_reader_for_t(orig_st) {
                Some(self.head.chunk_reader.read())
            } else {
                None
            };
            let res = state.appendable_histogram(
                orig_st,
                &zero_h,
                self.head_maxt,
                self.min_valid_time,
                self.ooo_time_window,
                cr_guard.as_ref().and_then(|g| g.as_ref()),
            );
            matches!(res, Ok((false, false)))
        };
        if !ok {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return;
        }
        let st = self.zero_or_st(orig_st);
        let ht = BatchSampleType::from_hist(&zero_h);
        let ref_id = entry.ref_id;
        self.record_hist_in_batch(ref_id, ht);
        self.histograms.push(PendingHist {
            entry,
            st,
            t: orig_st,
            h: zero_h,
        });
    }

    fn best_effort_append_st_zero_fhist(
        &mut self,
        entry: Arc<SeriesEntry>,
        orig_st: i64,
        t: i64,
        fh: &FloatHistogram,
    ) {
        if orig_st == 0 || orig_st >= t || orig_st < self.min_valid_time {
            return;
        }
        if self.has_pending_sample_at_or_after(entry.ref_id, orig_st) {
            return;
        }
        let zero_fh = FloatHistogram {
            counter_reset_hint: crate::chunkenc::CounterResetHint::CounterReset,
            schema: fh.schema,
            zero_threshold: fh.zero_threshold,
            custom_values: fh.custom_values.clone(),
            ..FloatHistogram::default()
        };
        if !entry.try_inc_pending_commit() {
            return;
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return;
        }
        let ok = {
            let state = entry.inner.lock();
            let cr_guard = if state.needs_chunk_reader_for_t(orig_st) {
                Some(self.head.chunk_reader.read())
            } else {
                None
            };
            let res = state.appendable_float_histogram(
                orig_st,
                &zero_fh,
                self.head_maxt,
                self.min_valid_time,
                self.ooo_time_window,
                cr_guard.as_ref().and_then(|g| g.as_ref()),
            );
            matches!(res, Ok((false, false)))
        };
        if !ok {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return;
        }
        let st = self.zero_or_st(orig_st);
        let ht = BatchSampleType::from_float_hist(&zero_fh);
        let ref_id = entry.ref_id;
        self.record_hist_in_batch(ref_id, ht);
        self.float_histograms.push(PendingFloatHist {
            entry,
            st,
            t: orig_st,
            fh: zero_fh,
        });
    }

    fn append_float_inner(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        let mut opt = series_ref.and_then(|r| self.head.series.get_by_id(r));
        if series_ref.is_some() && self.head.has_after_series_lookup_hook.load(Ordering::Acquire) {
            let hook = self.head.after_series_lookup_hook.lock().clone();
            if let Some(hook) = hook {
                hook(opt.as_deref());
            }
        }
        if self.head.enable_st_as_zero_sample() && st != 0 {
            if self.closed {
                return Err(HeadError::AppenderClosed);
            }
            if !self.head.initialized() {
                self.head.init_time(t);
                self.min_valid_time = self.head.appendable_min_valid_time();
                self.head_maxt = self.head.max_time();
            }
            if !(self.ooo_time_window == 0 && t < self.min_valid_time) {
                let entry = match opt {
                    Some(ref e) => Arc::clone(e),
                    None => {
                        let e = self.resolve_sorted_pairs(None, pairs)?;
                        opt = Some(Arc::clone(&e));
                        e
                    }
                };
                self.best_effort_append_st_zero_float(entry, st, t);
            }
        }
        self.append_float_with_entry(&mut opt, pairs, st, t, v, reject_ooo)
    }

    /// Appends a float sample directly by `series_ref` (zero label hashing or lookup overhead).
    pub fn append_float_by_ref(&mut self, series_ref: u64, t: i64, v: f64) -> Result<u64, HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        if self.ooo_time_window == 0 && t < self.min_valid_time {
            self.head
                .metrics
                .out_of_bound_samples
                .with_label_values(&["float"])
                .inc();
            return Err(HeadError::OutOfBounds);
        }
        let entry = self
            .head
            .series
            .get_by_id(series_ref)
            .ok_or(HeadError::UnknownSeriesRef(series_ref))?;
        if is_stale_nan(v) && !self.types_in_batch.is_empty() {
            if let Some(&prev_ht) = self.types_in_batch.get(&series_ref) {
                let mut opt = None;
                let res = match prev_ht {
                    BatchSampleType::Histogram | BatchSampleType::CustomBucketHistogram => {
                        let h = Histogram {
                            sum: v,
                            ..Histogram::default()
                        };
                        self.append_histogram_with_resolved_entry(
                            &mut opt,
                            false,
                            entry,
                            0,
                            t,
                            h,
                            self.ooo_time_window,
                            false,
                        )
                    }
                    BatchSampleType::FloatHistogram
                    | BatchSampleType::CustomBucketFloatHistogram => {
                        let fh = FloatHistogram {
                            sum: v,
                            ..FloatHistogram::default()
                        };
                        self.append_float_histogram_with_resolved_entry(
                            &mut opt,
                            false,
                            entry,
                            0,
                            t,
                            fh,
                            self.ooo_time_window,
                            false,
                        )
                    }
                }?;
                return res.ok_or(HeadError::UnknownSeriesRef(series_ref));
            }
        }
        if !entry.try_inc_pending_commit() || entry.is_gced() {
            return Err(HeadError::UnknownSeriesRef(series_ref));
        }
        let is_ooo = {
            // Lock order: entry.inner BEFORE chunk_reader.read() (C2).
            let state = entry.inner.lock();
            if entry.is_gced() {
                drop(state);
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                return Err(HeadError::UnknownSeriesRef(series_ref));
            }
            let res = if state.needs_chunk_reader_for_t(t) {
                let cr_guard = self.head.chunk_reader.read();
                state.appendable_float(
                    t,
                    v,
                    self.head_maxt,
                    self.min_valid_time,
                    self.ooo_time_window,
                    cr_guard.as_ref(),
                )
            } else {
                state.appendable_float(
                    t,
                    v,
                    self.head_maxt,
                    self.min_valid_time,
                    self.ooo_time_window,
                    None,
                )
            };
            drop(state);
            match res {
                Ok((is_ooo, is_dup)) => {
                    if is_dup {
                        self.head.metrics.exact_duplicate_dropped("float");
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
                        return Ok(series_ref);
                    }
                    is_ooo
                }
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    self.head.metrics.record_append_err(&e, "float");
                    return Err(e);
                }
            }
        };
        if !self.types_in_batch.is_empty() && self.types_in_batch.contains_key(&series_ref) {
            self.cut_batch();
        }
        self.floats.push(PendingFloat {
            entry,
            st: 0,
            t,
            v,
            is_ooo,
        });
        Ok(series_ref)
    }

    /// Appends an integer native histogram sample.
    pub fn append_histogram(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        h: Histogram,
    ) -> Result<u64, HeadError> {
        self.append_histogram_inner(series_ref, pairs, st, t, h, false)
    }

    /// Appends an integer native histogram sample, rejecting out-of-order samples even when OOO is enabled.
    pub fn append_histogram_reject_ooo(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        t: i64,
        h: Histogram,
    ) -> Result<u64, HeadError> {
        self.append_histogram_inner(series_ref, pairs, 0, t, h, true)
    }

    pub fn append_histogram_with_st_reject_ooo(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        h: Histogram,
    ) -> Result<u64, HeadError> {
        self.append_histogram_inner(series_ref, pairs, st, t, h, true)
    }

    fn append_histogram_inner(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        h: Histogram,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        let orig_st = st;
        let st = self.zero_or_st(st);
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        h.validate().map_err(HeadError::HistogramValidation)?;
        if self.ooo_time_window == 0 && t < self.min_valid_time {
            self.head
                .metrics
                .out_of_bound_samples
                .with_label_values(&["histogram"])
                .inc();
            return Err(HeadError::OutOfBounds);
        }

        let mut cur_ref = series_ref;
        let mut did_zero = false;
        loop {
            let e = self.resolve_sorted_pairs(cur_ref, pairs)?;
            if !did_zero && self.head.enable_st_as_zero_sample() && orig_st != 0 {
                did_zero = true;
                self.best_effort_append_st_zero_hist(Arc::clone(&e), orig_st, t, &h);
            }
            let mut opt = None;
            if let Some(ref_id) = self.append_histogram_with_resolved_entry(
                &mut opt,
                false,
                e,
                st,
                t,
                h.clone(),
                self.ooo_time_window,
                reject_ooo,
            )? {
                return Ok(ref_id);
            }
            cur_ref = None;
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_histogram_with_resolved_entry(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        take_entry: bool,
        entry: Arc<SeriesEntry>,
        st: i64,
        t: i64,
        h: Histogram,
        ooo_time_window: i64,
        reject_ooo: bool,
    ) -> Result<Option<u64>, HeadError> {
        if !entry.try_inc_pending_commit() {
            return Ok(None);
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }

        let ref_id = entry.ref_id;
        {
            // Lock order: entry.inner BEFORE chunk_reader.read() (C2).
            let state = entry.inner.lock();
            if entry.is_gced() {
                drop(state);
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                return Ok(None);
            }
            let res = if state.needs_chunk_reader_for_t(t) {
                let cr_guard = self.head.chunk_reader.read();
                state.appendable_histogram(
                    t,
                    &h,
                    self.head_maxt,
                    self.min_valid_time,
                    ooo_time_window,
                    cr_guard.as_ref(),
                )
            } else {
                state.appendable_histogram(
                    t,
                    &h,
                    self.head_maxt,
                    self.min_valid_time,
                    ooo_time_window,
                    None,
                )
            };
            drop(state);
            if reject_ooo && matches!(res, Ok((true, _)) | Err(HeadError::TooOldSample)) {
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                self.head
                    .metrics
                    .record_append_err(&HeadError::OutOfOrderSample, "histogram");
                return Err(HeadError::OutOfOrderSample);
            }
            // Go `appendableHistogram` / `appendableFloatHistogram` return no delta for an in-order
            // sample or with a duplicate sample error.
            if !matches!(res, Ok((false, _)))
                && !matches!(&res, Err(e) if e.is_duplicate_sample())
            {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            let (is_ooo, is_dup) = match res {
                Ok(v) => v,
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    if take_entry {
                        *series_entry = Some(entry);
                    }
                    self.head.metrics.record_append_err(&e, "histogram");
                    return Err(e);
                }
            };
            if is_dup {
                self.head.metrics.exact_duplicate_dropped("histogram");
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                return Ok(Some(ref_id));
            }
            let _ = is_ooo;
        }
        let ht = BatchSampleType::from_hist(&h);
        self.record_hist_in_batch(ref_id, ht);
        self.histograms.push(PendingHist {
            entry,
            st,
            t,
            h,
        });
        Ok(Some(ref_id))
    }

    /// Appends a float native histogram sample.
    pub fn append_float_histogram(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        fh: FloatHistogram,
    ) -> Result<u64, HeadError> {
        self.append_float_histogram_inner(series_ref, pairs, st, t, fh, false)
    }

    /// Appends a float native histogram sample, rejecting out-of-order samples even when OOO is enabled.
    pub fn append_float_histogram_reject_ooo(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        t: i64,
        fh: FloatHistogram,
    ) -> Result<u64, HeadError> {
        self.append_float_histogram_inner(series_ref, pairs, 0, t, fh, true)
    }

    pub fn append_float_histogram_with_st_reject_ooo(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        fh: FloatHistogram,
    ) -> Result<u64, HeadError> {
        self.append_float_histogram_inner(series_ref, pairs, st, t, fh, true)
    }

    fn append_float_histogram_inner(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        fh: FloatHistogram,
        reject_ooo: bool,
    ) -> Result<u64, HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        let orig_st = st;
        let st = self.zero_or_st(st);
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        fh.validate().map_err(HeadError::HistogramValidation)?;
        if self.ooo_time_window == 0 && t < self.min_valid_time {
            self.head
                .metrics
                .out_of_bound_samples
                .with_label_values(&["histogram"])
                .inc();
            return Err(HeadError::OutOfBounds);
        }

        let mut cur_ref = series_ref;
        let mut did_zero = false;
        loop {
            let e = self.resolve_sorted_pairs(cur_ref, pairs)?;
            if !did_zero && self.head.enable_st_as_zero_sample() && orig_st != 0 {
                did_zero = true;
                self.best_effort_append_st_zero_fhist(Arc::clone(&e), orig_st, t, &fh);
            }
            let mut opt = None;
            if let Some(ref_id) = self.append_float_histogram_with_resolved_entry(
                &mut opt,
                false,
                e,
                st,
                t,
                fh.clone(),
                self.ooo_time_window,
                reject_ooo,
            )? {
                return Ok(ref_id);
            }
            cur_ref = None;
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_float_histogram_with_resolved_entry(
        &mut self,
        series_entry: &mut Option<Arc<SeriesEntry>>,
        take_entry: bool,
        entry: Arc<SeriesEntry>,
        st: i64,
        t: i64,
        fh: FloatHistogram,
        ooo_time_window: i64,
        reject_ooo: bool,
    ) -> Result<Option<u64>, HeadError> {
        if !entry.try_inc_pending_commit() {
            return Ok(None);
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }

        let ref_id = entry.ref_id;
        {
            // Lock order: entry.inner BEFORE chunk_reader.read() (C2).
            let state = entry.inner.lock();
            if entry.is_gced() {
                drop(state);
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                return Ok(None);
            }
            let res = if state.needs_chunk_reader_for_t(t) {
                let cr_guard = self.head.chunk_reader.read();
                state.appendable_float_histogram(
                    t,
                    &fh,
                    self.head_maxt,
                    self.min_valid_time,
                    ooo_time_window,
                    cr_guard.as_ref(),
                )
            } else {
                state.appendable_float_histogram(
                    t,
                    &fh,
                    self.head_maxt,
                    self.min_valid_time,
                    ooo_time_window,
                    None,
                )
            };
            drop(state);
            if reject_ooo && matches!(res, Ok((true, _)) | Err(HeadError::TooOldSample)) {
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                self.head
                    .metrics
                    .record_append_err(&HeadError::OutOfOrderSample, "histogram");
                return Err(HeadError::OutOfOrderSample);
            }
            // Go `appendableHistogram` / `appendableFloatHistogram` return no delta for an in-order
            // sample or with a duplicate sample error.
            if !matches!(res, Ok((false, _)))
                && !matches!(&res, Err(e) if e.is_duplicate_sample())
            {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            let (is_ooo, is_dup) = match res {
                Ok(v) => v,
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    if take_entry {
                        *series_entry = Some(entry);
                    }
                    self.head.metrics.record_append_err(&e, "histogram");
                    return Err(e);
                }
            };
            if is_dup {
                self.head.metrics.exact_duplicate_dropped("histogram");
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                return Ok(Some(ref_id));
            }
            let _ = is_ooo;
        }
        let ht = BatchSampleType::from_float_hist(&fh);
        self.record_hist_in_batch(ref_id, ht);
        self.float_histograms.push(PendingFloatHist {
            entry,
            st,
            t,
            fh,
        });
        Ok(Some(ref_id))
    }

    /// Appends an exemplar for `series_ref` (or `pairs` if `series_ref` is `None`).
    pub fn append_exemplar(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        exemplar: Exemplar,
    ) -> Result<u64, HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        let lookup_by_pairs = || {
            let mut sorted: SmallVec<[(&str, &str); 16]> = pairs
                .iter()
                .copied()
                .filter(|(_, v)| !v.is_empty())
                .collect();
            sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
            let hash = hash_sorted_pairs(&sorted);
            self.head.get_by_sorted_pairs(hash, &sorted)
        };

        if !self.head.opts.enable_exemplar_storage || self.head.exemplars.capacity() == 0 {
            if let Some(r) = series_ref {
                return Ok(r);
            }
            return Ok(lookup_by_pairs().map_or(0, |e| e.ref_id));
        }

        let entry = if let Some(r) = series_ref {
            match self.head.series.get_by_id(r) {
                Some(e) => e,
                None => lookup_by_pairs().ok_or(HeadError::UnknownSeriesRef(r))?,
            }
        } else {
            lookup_by_pairs().ok_or(HeadError::UnknownSeriesRef(0))?
        };
        if entry.is_gced() {
            return Err(HeadError::UnknownSeriesRef(entry.ref_id));
        }

        let cleaned = Exemplar {
            labels: exemplar.labels.without_empty(),
            ..exemplar
        };
        match self.head.exemplars.validate_exemplar(&entry.labels, &cleaned) {
            Ok(()) => {}
            Err(ExemplarError::DuplicateExemplar | ExemplarError::Disabled) => {
                return Ok(entry.ref_id);
            }
            Err(e) => return Err(HeadError::Exemplar(e)),
        }

        let ref_id = entry.ref_id;
        {
            let mut st = entry.inner.lock();
            if entry.is_gced() {
                return Err(HeadError::UnknownSeriesRef(ref_id));
            }
            st.update_last_exemplar_ts(cleaned.timestamp_ms);
        }
        self.exemplars.push(PendingExemplar {
            entry,
            exemplar: cleaned,
        });
        Ok(ref_id)
    }

    /// Updates metric metadata for a series and optionally the tenant metadata store.
    pub fn update_metadata(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        metric_family_name: &str,
        metric_type: MetricType,
        unit: &str,
        help: &str,
    ) -> Result<u64, HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        let mut cur_ref = series_ref;
        let (entry, changed) = loop {
            let e = self.resolve_sorted_pairs(cur_ref, pairs)?;
            // Lock order: `symbols` -> `entry.inner` (matching `maybe_gc_symbols` and `commit_inner`),
            // preventing `SymbolId` ABA across a concurrent `maybe_gc_symbols`.
            let syms = self.head.symbols.read();
            let prev_meta = {
                let st = e.inner.lock();
                if e.is_gced() {
                    drop(st);
                    drop(syms);
                    cur_ref = None;
                    continue;
                }
                st.metadata()
            };
            let ch = match prev_meta {
                Some(m) => {
                    m.metric_type != metric_type
                        || syms.try_resolve(m.unit) != Some(unit)
                        || syms.try_resolve(m.help) != Some(help)
                }
                None => true,
            };
            break (e, ch);
        };
        let ref_id = entry.ref_id;

        if !metric_family_name.is_empty() {
            self.head.metadata.add_str(
                metric_family_name,
                metric_type,
                help,
                unit,
                self.head.metadata_limits(),
            )?;
        }

        if changed {
            self.metadata.push(PendingMetadata {
                entry,
                metric_type,
                unit: unit.to_string(),
                help: help.to_string(),
            });
        }
        Ok(ref_id)
    }

    /// Appends a float, histogram, or float histogram together with optional metadata and exemplars,
    /// matching Go headAppenderV2.Append (skipping metadata/exemplars when the sample is stale).
    #[allow(clippy::too_many_arguments)]
    pub fn append_v2(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
        st: i64,
        t: i64,
        v: f64,
        h: Option<Histogram>,
        fh: Option<FloatHistogram>,
        reject_ooo: bool,
        meta: Option<(MetricType, &str, &str)>,
        exemplars: &[Exemplar],
    ) -> Result<u64, HeadError> {
        let is_stale;
        let ref_id = if let Some(fh) = fh {
            is_stale = is_stale_nan(fh.sum);
            self.append_float_histogram_inner(series_ref, pairs, st, t, fh, reject_ooo)?
        } else if let Some(h) = h {
            is_stale = is_stale_nan(h.sum);
            self.append_histogram_inner(series_ref, pairs, st, t, h, reject_ooo)?
        } else {
            is_stale = is_stale_nan(v);
            if reject_ooo {
                self.append_float_with_st_reject_ooo(series_ref, pairs, st, t, v)?
            } else {
                self.append_float_with_st(series_ref, pairs, st, t, v)?
            }
        };
        if is_stale {
            return Ok(ref_id);
        }
        for ex in exemplars {
            let _ = self.append_exemplar(Some(ref_id), pairs, ex.clone());
        }
        if self.head.enable_metadata_wal_records.load(Ordering::Acquire) {
            if let Some((metric_type, unit, help)) = meta {
                if !unit.is_empty() || !help.is_empty() || metric_type != MetricType::Unknown {
                    self.update_metadata(Some(ref_id), pairs, "", metric_type, unit, help)?;
                }
            }
        }
        Ok(ref_id)
    }

    /// Commits all buffered series, samples, histograms, exemplars, and metadata to WAL/WBL and Head memory.
    pub fn commit(mut self) -> Result<(), HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        self.closed = true;
        self.head.metrics.active_appenders.dec();

        let compact_notifier = self.compact_notifier;
        let head = self.head;
        let res = self.commit_inner();
        if self.append_id > 0 {
            self.head.iso.close_append(self.append_id);
        }
        self.release_remaining_pending_commits();
        self.return_scratch();
        if let Some(notifier) = compact_notifier {
            if head.is_compactable() {
                notifier.notify_compact();
            }
        }
        res
    }

    /// Commits a batch of appenders atomically at the WAL boundary:
    /// encodes and writes ALL appenders' WAL records BEFORE mutating any in-memory Head chunks
    /// or exemplars (retrying `series_refs` persistence on WAL failure), and applies in-memory
    /// state only once all WAL writes succeed.
    pub fn commit_batch(mut apps: Vec<HeadAppender<'_>>) -> Result<(), HeadError> {
        if apps.is_empty() {
            return Ok(());
        }
        if apps.len() == 1 {
            return apps.into_iter().next().unwrap().commit();
        }
        if apps.iter().any(|a| a.closed) {
            return Err(HeadError::AppenderClosed);
        }
        for a in &mut apps {
            a.closed = true;
            a.head.metrics.active_appenders.dec();
        }

        let finish_all = |apps: &mut Vec<HeadAppender<'_>>| {
            for a in apps.iter_mut() {
                if a.append_id > 0 {
                    a.head.iso.close_append(a.append_id);
                }
                a.release_remaining_pending_commits();
                a.return_scratch();
            }
        };

        let same_head = apps.iter().all(|a| std::ptr::eq(a.head, apps[0].head));
        if same_head {
            let head = apps[0].head;
            let compact_notifier = apps.iter().find_map(|a| a.compact_notifier);
            let payload = match Self::prepare_batch_wal_payloads(&apps) {
                Ok(p) => p,
                Err(err) => {
                    for app in &mut apps {
                        app.rollback_series_refs_after_wal_err(&err);
                    }
                    finish_all(&mut apps);
                    if let Some(notifier) = compact_notifier {
                        if head.is_compactable() {
                            notifier.notify_compact();
                        }
                    }
                    return Err(err);
                }
            };

            let wal_res: Result<(), HeadError> = (|| {
                let mut wal_guard = head.wal.lock();
                if let Some(ref mut wal) = *wal_guard {
                    if let Some((ref b, c, ulen)) = payload.series {
                        wal.log_precompressed_with_lens(&[(b.as_slice(), c, ulen)])?;
                        for a in &mut apps {
                            a.series_refs.clear();
                        }
                    }
                    if !payload.records.is_empty() {
                        let slices: Vec<(&[u8], bool, usize)> = payload
                            .records
                            .iter()
                            .map(|(b, c, u)| (b.as_slice(), *c, *u))
                            .collect();
                        wal.log_precompressed_with_lens(&slices)?;
                    }
                }
                Ok(())
            })();
            drop(payload);
            if let Err(err) = wal_res {
                for a in &mut apps {
                    a.rollback_series_refs_after_wal_err(&err);
                }
                finish_all(&mut apps);
                if let Some(notifier) = compact_notifier {
                    if head.is_compactable() {
                        notifier.notify_compact();
                    }
                }
                return Err(err);
            }

            let mut ctx = CommitContext::new(head, 0, 0);
            let mut first_err: Option<HeadError> = None;
            for a in &mut apps {
                ctx.append_id = a.append_id;
                ctx.cleanup_append_ids_below = a.cleanup_append_ids_below;
                if let Err(err) = a.apply_in_memory_with_ctx(&mut ctx) {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
            let res = ctx.finish(first_err);
            finish_all(&mut apps);
            if let Some(notifier) = compact_notifier {
                if head.is_compactable() {
                    notifier.notify_compact();
                }
            }
            res
        } else {
            let mut payloads = Vec::with_capacity(apps.len());
            for a in &mut apps {
                match a.prepare_wal_payloads() {
                    Ok(p) => payloads.push(p),
                    Err(err) => {
                        for app in &mut apps {
                            app.rollback_series_refs_after_wal_err(&err);
                        }
                        let notifiers: SmallVec<[(&Head, &dyn CompactNotifier); 8]> = apps
                            .iter()
                            .filter_map(|a| a.compact_notifier.map(|n| (a.head, n)))
                            .collect();
                        finish_all(&mut apps);
                        for (h, n) in notifiers {
                            if h.is_compactable() {
                                n.notify_compact();
                            }
                        }
                        return Err(err);
                    }
                }
            }

            for (idx, p) in payloads.iter().enumerate() {
                let a = &mut apps[idx];
                let wal_res: Result<(), HeadError> = (|| {
                    let mut wal_guard = a.head.wal.lock();
                    if let Some(ref mut wal) = *wal_guard {
                        if let Some((ref b, c, u)) = p.series {
                            wal.log_precompressed_with_lens(&[(b.as_slice(), c, u)])?;
                            a.series_refs.clear();
                        }
                        if !p.records.is_empty() {
                            let slices: Vec<(&[u8], bool, usize)> = p
                                .records
                                .iter()
                                .map(|(b, c, u)| (b.as_slice(), *c, *u))
                                .collect();
                            wal.log_precompressed_with_lens(&slices)?;
                        }
                    }
                    Ok(())
                })();
                if let Err(err) = wal_res {
                    for app in &mut apps[idx..] {
                        app.rollback_series_refs_after_wal_err(&err);
                    }
                    for app in &mut apps[..idx] {
                        let _ = app.apply_in_memory_inner();
                    }
                    let notifiers: SmallVec<[(&Head, &dyn CompactNotifier); 8]> = apps
                        .iter()
                        .filter_map(|a| a.compact_notifier.map(|n| (a.head, n)))
                        .collect();
                    finish_all(&mut apps);
                    for (h, n) in notifiers {
                        if h.is_compactable() {
                            n.notify_compact();
                        }
                    }
                    return Err(err);
                }
            }
            drop(payloads);

            let mut first_err: Option<HeadError> = None;
            for a in &mut apps {
                if let Err(err) = a.apply_in_memory_inner() {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
            let notifiers: SmallVec<[(&Head, &dyn CompactNotifier); 8]> = apps
                .iter()
                .filter_map(|a| a.compact_notifier.map(|n| (a.head, n)))
                .collect();
            finish_all(&mut apps);
            for (h, n) in notifiers {
                if h.is_compactable() {
                    n.notify_compact();
                }
            }
            match first_err {
                Some(err) => Err(err),
                None => Ok(()),
            }
        }
    }

    fn rollback_series_refs_after_wal_err(&mut self, commit_err: &HeadError) {
        if self.series_refs.is_empty() {
            return;
        }
        if let Some(ref mut wal) = *self.head.wal.lock() {
            let enc = RecordEncoder::new(self.head.opts.enable_st_storage);
            let rec = enc.series(&self.series_refs, Vec::new());
            if let Err(rb_err) = wal.log(&[&rec]) {
                tracing::error!(
                    num_series = self.series_refs.len(),
                    commit_err = %commit_err,
                    rollback_err = %rb_err,
                    "failed to persist newly created series' declaration to the WAL on both commit and rollback retry"
                );
            }
        }
        self.series_refs.clear();
    }

    fn commit_inner(&mut self) -> Result<(), HeadError> {
        // 1. Log to WAL (falling back to persisting newly created series_refs on failure,
        // matching Prometheus headAppender.Commit -> Rollback).
        if let Err(err) = self.log_wal() {
            self.rollback_series_refs_after_wal_err(&err);
            return Err(err);
        }
        self.apply_in_memory_inner()
    }

    fn apply_in_memory_inner(&mut self) -> Result<(), HeadError> {
        let mut ctx = CommitContext::new(self.head, self.append_id, self.cleanup_append_ids_below);
        let apply_res = self.apply_in_memory_with_ctx(&mut ctx);
        ctx.finish(apply_res.err())
    }

    fn apply_in_memory_with_ctx<'c>(
        &mut self,
        ctx: &mut CommitContext<'c>,
    ) -> Result<(), HeadError> {
        // 2. Apply exemplars & metadata & samples to in-memory chunks (collecting OOO records for WBL).
        for ex in self.exemplars.drain(..) {
            if let Err(ExemplarError::OutOfOrderExemplar) =
                self.head.exemplars.add_exemplar(&ex.entry.labels, ex.exemplar)
            {
                continue;
            }
        }
        for pm in self.metadata.drain(..) {
            let mut applied = false;
            {
                let syms = self.head.symbols.read();
                if let (Some(u), Some(h)) = (syms.get(&pm.unit), syms.get(&pm.help)) {
                    self.head.has_any_metadata.store(true, Ordering::Release);
                    let mut st = pm.entry.inner.lock();
                    st.set_metadata(CompactSeriesMetadata {
                        metric_type: pm.metric_type,
                        unit: u,
                        help: h,
                    });
                    applied = true;
                }
            }
            if !applied {
                let mut syms = self.head.symbols.write();
                let compact_meta = CompactSeriesMetadata {
                    metric_type: pm.metric_type,
                    unit: syms.try_intern(&pm.unit)?,
                    help: syms.try_intern(&pm.help)?,
                };
                self.head.has_any_metadata.store(true, Ordering::Release);
                let mut st = pm.entry.inner.lock();
                st.set_metadata(compact_meta);
            }
        }

        let total_batches = self.batch_boundaries.len() + 1;
        let mut f_start = 0usize;
        let mut h_start = 0usize;
        let mut fh_start = 0usize;

        for batch_idx in 0..total_batches {
            let (f_end, h_end, fh_end) = if batch_idx < self.batch_boundaries.len() {
                let b = self.batch_boundaries[batch_idx];
                (b.floats_end, b.hists_end, b.fhists_end)
            } else {
                (
                    self.floats.len(),
                    self.histograms.len(),
                    self.float_histograms.len(),
                )
            };

            let mut deferred = DeferredStaleness::default();

            // Commit floats in this batch (software-prefetching SeriesEntry + SeriesState 4 iterations ahead)
            for i in f_start..f_end {
                if i + 4 < f_end {
                    let next_ptr = Arc::as_ptr(&self.floats[i + 4].entry);
                    prefetch_read_data(next_ptr);
                    prefetch_read_data((next_ptr as *const u8).wrapping_add(64));
                }
                self.floats_committed = i + 1;
                let pf = &self.floats[i];
                let mut v = pf.v;
                let _pending_guard = PendingCommitGuard(&pf.entry, &self.head.metrics);
                self.head.maybe_wait_commit_pause(pf.entry.ref_id);
                let mut state = TxCleanupGuard {
                    state: pf.entry.inner.lock(),
                    head: self.head,
                    ref_id: pf.entry.ref_id,
                    bound: ctx.cleanup_append_ids_below,
                };

                if !pf.is_ooo
                    && !is_stale_nan(v)
                    && pf.t >= self.min_valid_time
                    && pf.t > state.max_time()
                {
                    if v.to_bits() == QUIET_ZERO_NAN_BITS {
                        v = 0.0;
                    }
                    let (was_stale, was_hist, old_buckets) = state.sample_state();
                    if self.head.metrics.track_append(state.append_float_in_order(
                        &pf.entry.labels,
                        pf.st,
                        pf.t,
                        v,
                        ctx.chunk_range_ms,
                        ctx.samples_per_chunk,
                        ctx.float_enc,
                        ctx.store_st,
                        self.head.chunk_end_time_variance(),
                    )?) {
                        self.head.txs_add(pf.entry.ref_id, ctx.append_id);
                        ctx.in_order_mint = ctx.in_order_mint.min(pf.t);
                        ctx.in_order_maxt = ctx.in_order_maxt.max(pf.t);
                        ctx.floats_appended += 1;
                        self.head
                            .update_stale_series_metric_on_append(was_stale, is_stale_nan(v));
                        if was_hist {
                            self.head
                                .update_native_histogram_metrics_on_append(true, false, old_buckets, 0);
                        }
                    } else {
                        ctx.float_dups += 1;
                    }
                    pf.entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                    continue;
                }

                // Go `commitFloats` (`head_append.go:1503-1529`): defer float staleness markers
                // targeting histogram / float-histogram series into batch-local histogram tails
                // processed after `f_start..f_end` so WBL record ordering and OOO chunk cuts match Go.
                if is_stale_nan(v) {
                    match state.latest_in_order_encoding() {
                        Some(Encoding::Histogram | Encoding::HistogramSt) => {
                            pf.entry.pending_commits.fetch_add(1, Ordering::Release);
                            deferred.hists.push_back(PendingHist {
                                entry: Arc::clone(&pf.entry),
                                st: pf.st,
                                t: pf.t,
                                h: Histogram {
                                    sum: v,
                                    ..Histogram::default()
                                },
                            });
                            continue;
                        }
                        Some(Encoding::FloatHistogram | Encoding::FloatHistogramSt) => {
                            pf.entry.pending_commits.fetch_add(1, Ordering::Release);
                            deferred.fhists.push_back(PendingFloatHist {
                                entry: Arc::clone(&pf.entry),
                                st: pf.st,
                                t: pf.t,
                                fh: FloatHistogram {
                                    sum: v,
                                    ..FloatHistogram::default()
                                },
                            });
                            continue;
                        }
                        _ => {}
                    }
                }

                let check = if state.needs_chunk_reader_for_t(pf.t) {
                    let cr_guard = self.head.chunk_reader.read();
                    state.appendable_float(
                        pf.t,
                        v,
                        self.head_maxt,
                        self.min_valid_time,
                        self.ooo_time_window,
                        cr_guard.as_ref(),
                    )
                } else {
                    state.appendable_float(
                        pf.t,
                        v,
                        self.head_maxt,
                        self.min_valid_time,
                        self.ooo_time_window,
                        None,
                    )
                };
                let (is_ooo, is_dup) = match check {
                    Ok(v) => v,
                    Err(HeadError::OutOfBounds) => {
                        ctx.float_oob += 1;
                        continue;
                    }
                    Err(HeadError::OutOfOrderSample) => {
                        ctx.float_ooo += 1;
                        continue;
                    }
                    Err(HeadError::TooOldSample) => {
                        ctx.float_too_old += 1;
                        continue;
                    }
                    Err(
                        HeadError::DuplicateSampleForTimestamp
                        | HeadError::DuplicateSampleWithDetails(_),
                    ) => {
                        ctx.float_dups += 1;
                        continue;
                    }
                    Err(_) => continue,
                };
                if is_dup {
                    ctx.float_dups += 1;
                    continue;
                }
                if is_ooo {
                    if v.to_bits() == QUIET_ZERO_NAN_BITS {
                        // Go `commitFloats` (`head_append.go:1546-1547`): quiet zeros are not
                        // stored out-of-order, but still count in `samples_appended_total`.
                        ctx.floats_appended += 1;
                        continue;
                    }
                    let mut cw_guard = self.head.chunk_writer.lock();
                    let (ok, chunk_created, mmap_refs, write_epoch) = state.insert_ooo(
                        pf.entry.ref_id,
                        pf.st,
                        pf.t,
                        OooSampleValue::Float(v),
                        ctx.ooo_cap_max,
                        ctx.use_xor2,
                        ctx.use_hist_st,
                        cw_guard.as_mut(),
                    )?;
                    drop(cw_guard);
                    if chunk_created {
                        ctx.on_ooo_chunk_created(&pf.entry, mmap_refs, write_epoch);
                    }
                    if ok {
                        self.head
                            .metrics
                            .ooo_samples_appended
                            .with_label_values(&["float"])
                            .inc();
                        ctx.wbl_samples.push(RefSample {
                            ref_id: pf.entry.ref_id,
                            st: pf.st,
                            t: pf.t,
                            v,
                        });
                        ctx.ooo_mint = ctx.ooo_mint.min(pf.t);
                        ctx.ooo_maxt = ctx.ooo_maxt.max(pf.t);
                        ctx.floats_appended += 1;
                    } else {
                        ctx.float_dups += 1;
                    }
                } else {
                    if v.to_bits() == QUIET_ZERO_NAN_BITS {
                        v = 0.0;
                    }
                    let (was_stale, was_hist, old_buckets) = state.sample_state();
                    if self.head.metrics.track_append(state.append_float_in_order(
                        &pf.entry.labels,
                        pf.st,
                        pf.t,
                        v,
                        ctx.chunk_range_ms,
                        ctx.samples_per_chunk,
                        ctx.float_enc,
                        ctx.store_st,
                        self.head.chunk_end_time_variance(),
                    )?) {
                        self.head.txs_add(pf.entry.ref_id, ctx.append_id);
                        ctx.in_order_mint = ctx.in_order_mint.min(pf.t);
                        ctx.in_order_maxt = ctx.in_order_maxt.max(pf.t);
                        ctx.floats_appended += 1;
                        self.head
                            .update_stale_series_metric_on_append(was_stale, is_stale_nan(v));
                        if was_hist {
                            self.head
                                .update_native_histogram_metrics_on_append(true, false, old_buckets, 0);
                        }
                    } else {
                        ctx.float_dups += 1;
                    }
                    pf.entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                }
            }

            // Commit integer histograms in this batch (followed by any deferred float-staleness conversions)
            for i in h_start..h_end {
                self.hists_committed = i + 1;
                ctx.commit_one_hist(
                    self.head_maxt,
                    self.min_valid_time,
                    self.ooo_time_window,
                    &mut self.histograms[i],
                )?;
            }
            while let Some(mut ph) = deferred.hists.pop_front() {
                ctx.commit_one_hist(
                    self.head_maxt,
                    self.min_valid_time,
                    self.ooo_time_window,
                    &mut ph,
                )?;
            }

            // Commit float histograms in this batch (followed by any deferred float-staleness conversions)
            for i in fh_start..fh_end {
                self.fhists_committed = i + 1;
                ctx.commit_one_float_hist(
                    self.head_maxt,
                    self.min_valid_time,
                    self.ooo_time_window,
                    &mut self.float_histograms[i],
                )?;
            }
            while let Some(mut pfh) = deferred.fhists.pop_front() {
                ctx.commit_one_float_hist(
                    self.head_maxt,
                    self.min_valid_time,
                    self.ooo_time_window,
                    &mut pfh,
                )?;
            }

            f_start = f_end;
            h_start = h_end;
            fh_start = fh_end;
        }

        self.floats.clear();
        self.floats_committed = 0;
        self.histograms.clear();
        self.hists_committed = 0;
        self.float_histograms.clear();
        self.fhists_committed = 0;
        Ok(())
    }

    fn prepare_batch_wal_payloads(apps: &[HeadAppender<'_>]) -> Result<PreparedWalPayloads, HeadError> {
        let head = apps[0].head;
        let wal_compression = head.opts.wal_compression;
        let enc = RecordEncoder::new(head.opts.enable_st_storage);

        let all_only_floats = apps.iter().all(|a| {
            a.series_refs.is_empty()
                && a.metadata.is_empty()
                && a.histograms.is_empty()
                && a.float_histograms.is_empty()
                && a.exemplars.is_empty()
        });
        if all_only_floats {
            let total_floats: usize = apps.iter().map(|a| a.floats.len()).sum();
            if total_floats == 0 {
                return Ok(PreparedWalPayloads {
                    series: None,
                    records: Vec::new(),
                });
            }
            let rec = enc.samples_iter(
                apps.iter()
                    .flat_map(|a| a.floats.iter())
                    .map(|f| RefSample {
                        ref_id: f.entry.ref_id,
                        st: f.st,
                        t: f.t,
                        v: f.v,
                    }),
                Vec::with_capacity(total_floats * 14 + 17),
            );
            let ulen = rec.len();
            let mut comp_buf = Vec::new();
            let payload =
                if crate::wlog::compress_wal_record(&rec, wal_compression, &mut comp_buf)? {
                    (comp_buf, true, ulen)
                } else {
                    (rec, false, ulen)
                };
            return Ok(PreparedWalPayloads {
                series: None,
                records: vec![payload],
            });
        }

        let total_series: usize = apps.iter().map(|a| a.series_refs.len()).sum();
        let series_rec = if total_series > 0 {
            let mut all_series = Vec::with_capacity(total_series);
            for a in apps {
                all_series.extend(a.series_refs.iter().cloned());
            }
            Some(enc.series(&all_series, Vec::new()))
        } else {
            None
        };

        let mut recs: Vec<Vec<u8>> = Vec::new();
        let total_meta: usize = apps.iter().map(|a| a.metadata.len()).sum();
        if total_meta > 0 {
            let mut metas = Vec::with_capacity(total_meta);
            for a in apps {
                metas.extend(a.metadata.iter().map(|m| RefMetadata {
                    ref_id: m.entry.ref_id,
                    metric_type: m.metric_type as u8,
                    unit: m.unit.clone(),
                    help: m.help.clone(),
                }));
            }
            recs.push(enc.metadata(&metas, Vec::new()));
        }

        let max_batches = apps
            .iter()
            .map(|a| a.batch_boundaries.len() + 1)
            .max()
            .unwrap_or(1);
        let mut cursors: SmallVec<[(usize, usize, usize); 16]> =
            SmallVec::from_elem((0, 0, 0), apps.len());

        for batch_idx in 0..max_batches {
            let mut ends: SmallVec<[(usize, usize, usize); 16]> =
                SmallVec::with_capacity(apps.len());
            let mut batch_floats = 0usize;
            let mut batch_hists = 0usize;
            let mut batch_fhists = 0usize;

            for (idx, a) in apps.iter().enumerate() {
                let (f_end, h_end, fh_end) = if batch_idx < a.batch_boundaries.len() {
                    let b = a.batch_boundaries[batch_idx];
                    (b.floats_end, b.hists_end, b.fhists_end)
                } else {
                    (
                        a.floats.len(),
                        a.histograms.len(),
                        a.float_histograms.len(),
                    )
                };
                batch_floats += f_end - cursors[idx].0;
                batch_hists += h_end - cursors[idx].1;
                batch_fhists += fh_end - cursors[idx].2;
                ends.push((f_end, h_end, fh_end));
            }

            if batch_floats > 0 {
                recs.push(enc.samples_iter(
                    apps.iter().enumerate().flat_map(|(idx, a)| {
                        let f_start = cursors[idx].0;
                        let f_end = ends[idx].0;
                        a.floats[f_start..f_end].iter().map(|f| RefSample {
                            ref_id: f.entry.ref_id,
                            st: f.st,
                            t: f.t,
                            v: f.v,
                        })
                    }),
                    Vec::with_capacity(batch_floats * 14 + 17),
                ));
            }
            if batch_hists > 0 {
                let mut hists = Vec::with_capacity(batch_hists);
                for (idx, a) in apps.iter().enumerate() {
                    let h_start = cursors[idx].1;
                    let h_end = ends[idx].1;
                    hists.extend(a.histograms[h_start..h_end].iter().map(|h| {
                        RefHistogramSample {
                            ref_id: h.entry.ref_id,
                            st: h.st,
                            t: h.t,
                            h: h.h.clone(),
                        }
                    }));
                }
                let (r, cb) = enc.histogram_samples(&hists, Vec::new());
                if !r.is_empty() {
                    recs.push(r);
                }
                if !cb.is_empty() {
                    recs.push(enc.custom_buckets_histogram_samples(&cb, Vec::new()));
                }
            }
            if batch_fhists > 0 {
                let mut fhists = Vec::with_capacity(batch_fhists);
                for (idx, a) in apps.iter().enumerate() {
                    let fh_start = cursors[idx].2;
                    let fh_end = ends[idx].2;
                    fhists.extend(a.float_histograms[fh_start..fh_end].iter().map(|fh| {
                        RefFloatHistogramSample {
                            ref_id: fh.entry.ref_id,
                            st: fh.st,
                            t: fh.t,
                            fh: fh.fh.clone(),
                        }
                    }));
                }
                let (r, cb) = enc.float_histogram_samples(&fhists, Vec::new());
                if !r.is_empty() {
                    recs.push(r);
                }
                if !cb.is_empty() {
                    recs.push(enc.custom_buckets_float_histogram_samples(&cb, Vec::new()));
                }
            }

            cursors = ends;
        }

        let total_exemplars: usize = apps.iter().map(|a| a.exemplars.len()).sum();
        if total_exemplars > 0 {
            let mut exs = Vec::with_capacity(total_exemplars);
            for a in apps {
                exs.extend(a.exemplars.iter().map(|e| RefExemplar {
                    ref_id: e.entry.ref_id,
                    t: e.exemplar.timestamp_ms,
                    v: e.exemplar.value,
                    labels: e.exemplar.labels.clone(),
                }));
            }
            recs.push(enc.exemplars(&exs, Vec::new()));
        }

        let prepared_series = match series_rec {
            Some(r) => {
                let ulen = r.len();
                let mut comp_buf = Vec::new();
                if crate::wlog::compress_wal_record(&r, wal_compression, &mut comp_buf)? {
                    Some((comp_buf, true, ulen))
                } else {
                    Some((r, false, ulen))
                }
            }
            None => None,
        };

        let mut prepared: Vec<(Vec<u8>, bool, usize)> = Vec::with_capacity(recs.len());
        for r in recs {
            let ulen = r.len();
            let mut comp_buf = Vec::new();
            if crate::wlog::compress_wal_record(&r, wal_compression, &mut comp_buf)? {
                prepared.push((comp_buf, true, ulen));
            } else {
                prepared.push((r, false, ulen));
            }
        }

        Ok(PreparedWalPayloads {
            series: prepared_series,
            records: prepared,
        })
    }
    /// Rolls back buffered samples/exemplars while still logging newly created series declarations to WAL.
    pub fn rollback(mut self) -> Result<(), HeadError> {
        self.rollback_inner()
    }

    fn rollback_inner(&mut self) -> Result<(), HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        self.closed = true;
        self.head.metrics.active_appenders.dec();
        self.exemplars.clear();
        self.metadata.clear();

        let log_res = if !self.series_refs.is_empty() {
            if let Some(ref mut wal) = *self.head.wal.lock() {
                let enc = RecordEncoder::new(self.head.opts.enable_st_storage);
                let rec = enc.series(&self.series_refs, Vec::new());
                wal.log(&[&rec]).map_err(HeadError::from)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };
        if self.append_id > 0 {
            self.head.iso.close_append(self.append_id);
        }
        self.release_remaining_pending_commits();
        self.return_scratch();
        log_res
    }

    fn log_wal(&mut self) -> Result<(), HeadError> {
        let wal_compression = self.head.opts.wal_compression;
        let enc = RecordEncoder::new(self.head.opts.enable_st_storage);

        if self.series_refs.is_empty()
            && self.metadata.is_empty()
            && self.histograms.is_empty()
            && self.float_histograms.is_empty()
            && self.exemplars.is_empty()
        {
            if self.floats.is_empty() {
                return Ok(());
            }
            // Encode and compress OUTSIDE `head.wal.lock()` so concurrent appenders parallelize
            // varint/float serialization and Snappy/Zstd compression.
            self.wal_buf.clear();
            let need = self.floats.len() * 14 + 17;
            if self.wal_buf.capacity() < need {
                self.wal_buf.reserve(need - self.wal_buf.capacity());
            }
            let buf = std::mem::take(&mut self.wal_buf);
            let rec = enc.samples_iter(
                self.floats.iter().map(|f| RefSample {
                    ref_id: f.entry.ref_id,
                    st: f.st,
                    t: f.t,
                    v: f.v,
                }),
                buf,
            );
            self.wal_comp_buf.clear();
            let is_compressed = match crate::wlog::compress_wal_record(
                &rec,
                wal_compression,
                &mut self.wal_comp_buf,
            ) {
                Ok(c) => c,
                Err(e) => {
                    self.wal_buf = rec;
                    return Err(e.into());
                }
            };
            let res = {
                let mut wal_guard = self.head.wal.lock();
                if let Some(ref mut wal) = *wal_guard {
                    let payload = if is_compressed {
                        self.wal_comp_buf.as_slice()
                    } else {
                        rec.as_slice()
                    };
                    wal.log_precompressed_with_lens(&[(payload, is_compressed, rec.len())])
                } else {
                    Ok(())
                }
            };
            self.wal_buf = rec;
            res?;
            return Ok(());
        }

        let series_rec = if !self.series_refs.is_empty() {
            Some(enc.series(&self.series_refs, Vec::new()))
        } else {
            None
        };

        let mut recs: Vec<Vec<u8>> = Vec::new();
        if !self.metadata.is_empty() {
            let metas: Vec<RefMetadata> = self
                .metadata
                .iter()
                .map(|m| RefMetadata {
                    ref_id: m.entry.ref_id,
                    metric_type: m.metric_type as u8,
                    unit: m.unit.clone(),
                    help: m.help.clone(),
                })
                .collect();
            recs.push(enc.metadata(&metas, Vec::new()));
        }
        let total_batches = self.batch_boundaries.len() + 1;
        let mut f_start = 0usize;
        let mut h_start = 0usize;
        let mut fh_start = 0usize;

        for batch_idx in 0..total_batches {
            let (f_end, h_end, fh_end) = if batch_idx < self.batch_boundaries.len() {
                let b = self.batch_boundaries[batch_idx];
                (b.floats_end, b.hists_end, b.fhists_end)
            } else {
                (
                    self.floats.len(),
                    self.histograms.len(),
                    self.float_histograms.len(),
                )
            };

            if f_start < f_end {
                let slice = &self.floats[f_start..f_end];
                recs.push(enc.samples_iter(
                    slice.iter().map(|f| RefSample {
                        ref_id: f.entry.ref_id,
                        st: f.st,
                        t: f.t,
                        v: f.v,
                    }),
                    Vec::with_capacity(slice.len() * 14 + 17),
                ));
            }
            if h_start < h_end {
                let hists: Vec<RefHistogramSample> = self.histograms[h_start..h_end]
                    .iter()
                    .map(|h| RefHistogramSample {
                        ref_id: h.entry.ref_id,
                        st: h.st,
                        t: h.t,
                        h: h.h.clone(),
                    })
                    .collect();
                let (r, cb) = enc.histogram_samples(&hists, Vec::new());
                if !r.is_empty() {
                    recs.push(r);
                }
                if !cb.is_empty() {
                    recs.push(enc.custom_buckets_histogram_samples(&cb, Vec::new()));
                }
            }
            if fh_start < fh_end {
                let fhists: Vec<RefFloatHistogramSample> = self.float_histograms[fh_start..fh_end]
                    .iter()
                    .map(|fh| RefFloatHistogramSample {
                        ref_id: fh.entry.ref_id,
                        st: fh.st,
                        t: fh.t,
                        fh: fh.fh.clone(),
                    })
                    .collect();
                let (r, cb) = enc.float_histogram_samples(&fhists, Vec::new());
                if !r.is_empty() {
                    recs.push(r);
                }
                if !cb.is_empty() {
                    recs.push(enc.custom_buckets_float_histogram_samples(&cb, Vec::new()));
                }
            }

            f_start = f_end;
            h_start = h_end;
            fh_start = fh_end;
        }
        if !self.exemplars.is_empty() {
            let exs: Vec<RefExemplar> = self
                .exemplars
                .iter()
                .map(|e| RefExemplar {
                    ref_id: e.entry.ref_id,
                    t: e.exemplar.timestamp_ms,
                    v: e.exemplar.value,
                    labels: e.exemplar.labels.clone(),
                })
                .collect();
            recs.push(enc.exemplars(&exs, Vec::new()));
        }

        let prepared_series = match series_rec {
            Some(r) => {
                let ulen = r.len();
                let mut comp_buf = Vec::new();
                if crate::wlog::compress_wal_record(&r, wal_compression, &mut comp_buf)? {
                    Some((comp_buf, true, ulen))
                } else {
                    Some((r, false, ulen))
                }
            }
            None => None,
        };

        let mut prepared: Vec<(Vec<u8>, bool, usize)> = Vec::with_capacity(recs.len());
        for r in recs {
            let ulen = r.len();
            let mut comp_buf = Vec::new();
            if crate::wlog::compress_wal_record(&r, wal_compression, &mut comp_buf)? {
                prepared.push((comp_buf, true, ulen));
            } else {
                prepared.push((r, false, ulen));
            }
        }

        if prepared_series.is_some() || !prepared.is_empty() {
            let mut wal_guard = self.head.wal.lock();
            if let Some(ref mut wal) = *wal_guard {
                if let Some((ref b, c, ulen)) = prepared_series {
                    wal.log_precompressed_with_lens(&[(b.as_slice(), c, ulen)])?;
                    self.series_refs.clear();
                }
                if !prepared.is_empty() {
                    let slices: Vec<(&[u8], bool, usize)> = prepared
                        .iter()
                        .map(|(b, c, u)| (b.as_slice(), *c, *u))
                        .collect();
                    wal.log_precompressed_with_lens(&slices)?;
                }
            }
        }
        Ok(())
    }

    fn prepare_wal_payloads(&mut self) -> Result<PreparedWalPayloads, HeadError> {
        let wal_compression = self.head.opts.wal_compression;
        let enc = RecordEncoder::new(self.head.opts.enable_st_storage);

        if self.series_refs.is_empty()
            && self.metadata.is_empty()
            && self.histograms.is_empty()
            && self.float_histograms.is_empty()
            && self.exemplars.is_empty()
        {
            if self.floats.is_empty() {
                return Ok(PreparedWalPayloads {
                    series: None,
                    records: Vec::new(),
                });
            }
            let rec = enc.samples_iter(
                self.floats.iter().map(|f| RefSample {
                    ref_id: f.entry.ref_id,
                    st: f.st,
                    t: f.t,
                    v: f.v,
                }),
                Vec::with_capacity(self.floats.len() * 14 + 17),
            );
            let ulen = rec.len();
            let mut comp_buf = Vec::new();
            let payload = if crate::wlog::compress_wal_record(&rec, wal_compression, &mut comp_buf)?
            {
                (comp_buf, true, ulen)
            } else {
                (rec, false, ulen)
            };
            return Ok(PreparedWalPayloads {
                series: None,
                records: vec![payload],
            });
        }

        let series_rec = if !self.series_refs.is_empty() {
            Some(enc.series(&self.series_refs, Vec::new()))
        } else {
            None
        };

        let mut recs: Vec<Vec<u8>> = Vec::new();
        if !self.metadata.is_empty() {
            let metas: Vec<RefMetadata> = self
                .metadata
                .iter()
                .map(|m| RefMetadata {
                    ref_id: m.entry.ref_id,
                    metric_type: m.metric_type as u8,
                    unit: m.unit.clone(),
                    help: m.help.clone(),
                })
                .collect();
            recs.push(enc.metadata(&metas, Vec::new()));
        }
        let total_batches = self.batch_boundaries.len() + 1;
        let mut f_start = 0usize;
        let mut h_start = 0usize;
        let mut fh_start = 0usize;

        for batch_idx in 0..total_batches {
            let (f_end, h_end, fh_end) = if batch_idx < self.batch_boundaries.len() {
                let b = self.batch_boundaries[batch_idx];
                (b.floats_end, b.hists_end, b.fhists_end)
            } else {
                (
                    self.floats.len(),
                    self.histograms.len(),
                    self.float_histograms.len(),
                )
            };

            if f_start < f_end {
                let slice = &self.floats[f_start..f_end];
                recs.push(enc.samples_iter(
                    slice.iter().map(|f| RefSample {
                        ref_id: f.entry.ref_id,
                        st: f.st,
                        t: f.t,
                        v: f.v,
                    }),
                    Vec::with_capacity(slice.len() * 14 + 17),
                ));
            }
            if h_start < h_end {
                let hists: Vec<RefHistogramSample> = self.histograms[h_start..h_end]
                    .iter()
                    .map(|h| RefHistogramSample {
                        ref_id: h.entry.ref_id,
                        st: h.st,
                        t: h.t,
                        h: h.h.clone(),
                    })
                    .collect();
                let (r, cb) = enc.histogram_samples(&hists, Vec::new());
                if !r.is_empty() {
                    recs.push(r);
                }
                if !cb.is_empty() {
                    recs.push(enc.custom_buckets_histogram_samples(&cb, Vec::new()));
                }
            }
            if fh_start < fh_end {
                let fhists: Vec<RefFloatHistogramSample> = self.float_histograms[fh_start..fh_end]
                    .iter()
                    .map(|fh| RefFloatHistogramSample {
                        ref_id: fh.entry.ref_id,
                        st: fh.st,
                        t: fh.t,
                        fh: fh.fh.clone(),
                    })
                    .collect();
                let (r, cb) = enc.float_histogram_samples(&fhists, Vec::new());
                if !r.is_empty() {
                    recs.push(r);
                }
                if !cb.is_empty() {
                    recs.push(enc.custom_buckets_float_histogram_samples(&cb, Vec::new()));
                }
            }

            f_start = f_end;
            h_start = h_end;
            fh_start = fh_end;
        }
        if !self.exemplars.is_empty() {
            let exs: Vec<RefExemplar> = self
                .exemplars
                .iter()
                .map(|e| RefExemplar {
                    ref_id: e.entry.ref_id,
                    t: e.exemplar.timestamp_ms,
                    v: e.exemplar.value,
                    labels: e.exemplar.labels.clone(),
                })
                .collect();
            recs.push(enc.exemplars(&exs, Vec::new()));
        }

        let prepared_series = match series_rec {
            Some(r) => {
                let ulen = r.len();
                let mut comp_buf = Vec::new();
                if crate::wlog::compress_wal_record(&r, wal_compression, &mut comp_buf)? {
                    Some((comp_buf, true, ulen))
                } else {
                    Some((r, false, ulen))
                }
            }
            None => None,
        };

        let mut prepared: Vec<(Vec<u8>, bool, usize)> = Vec::with_capacity(recs.len());
        for r in recs {
            let ulen = r.len();
            let mut comp_buf = Vec::new();
            if crate::wlog::compress_wal_record(&r, wal_compression, &mut comp_buf)? {
                prepared.push((comp_buf, true, ulen));
            } else {
                prepared.push((r, false, ulen));
            }
        }

        Ok(PreparedWalPayloads {
            series: prepared_series,
            records: prepared,
        })
    }
}

impl Drop for HeadAppender<'_> {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(err) = self.rollback_inner() {
                tracing::warn!(
                    err = %err,
                    "failed to persist newly created series on HeadAppender drop"
                );
            }
        } else {
            self.release_remaining_pending_commits();
            self.return_scratch();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::stale_nan;
    use crate::matcher::MatchType;

    #[test]
    fn head_append_dup_stale_cut_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 10_000,
            samples_per_chunk: 60, // forces chunk cuts easily
            ..HeadOptions::default()
        };

        {
            let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            let pairs = [("__name__", "http_requests"), ("job", "api")];

            let mut r = None;
            {
                let mut app = head.appender();
                for i in 0..200i64 {
                    let id = app
                        .append_float(r, &pairs, 1_000_000 + i * 1000, i as f64)
                        .unwrap();
                    r = Some(id);
                }
                app.commit().unwrap();
            }

            {
                let mut app = head.appender();
                // Exact duplicate at last timestamp is a no-op
                app.append_float(r, &pairs, 1_000_000 + 199 * 1000, 199.0)
                    .unwrap();
                // Different value at last timestamp errors with DuplicateSampleForTimestamp / DuplicateSampleWithDetails
                assert!(matches!(
                    app.append_float(r, &pairs, 1_000_000 + 199 * 1000, 999.0),
                    Err(HeadError::DuplicateSampleForTimestamp
                        | HeadError::DuplicateSampleWithDetails(_))
                ));
                // StaleNaN at newer timestamp succeeds
                app.append_float(r, &pairs, 1_000_000 + 200 * 1000, stale_nan())
                    .unwrap();
                // OOO sample within window succeeds
                app.append_float(r, &pairs, 1_000_000 + 195 * 1000 + 500, 42.5)
                    .unwrap();

                app.commit().unwrap();
            }
            let (ns, nc, _, _, _) = head.head_stats();
            assert_eq!(ns, 1);
            assert!(nc >= 2);
            head.close().unwrap();
        }

        // Reopen and verify chunks_head + WAL + WBL recovery
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        assert_eq!(head.num_series(), 1);
        let m = LabelMatcher::new(MatchType::Equal, "__name__", "http_requests").unwrap();
        let refs = head.postings_for_matchers(&[m]).unwrap();
        assert_eq!(refs.len(), 1);
        let (_, chks, _) = head.series_chunks(refs[0], i64::MIN, i64::MAX).unwrap().unwrap();
        assert!(!chks.is_empty());
        head.close().unwrap();
    }

    #[test]
    fn series_entry_size_is_compact() {
        let entry_size = std::mem::size_of::<SeriesEntry>();
        let state_size = std::mem::size_of::<SeriesState>();
        let last_sample_size = std::mem::size_of::<LastSample>();
        let meta_size = std::mem::size_of::<Option<CompactSeriesMetadata>>();
        eprintln!(
            "SeriesEntry={entry_size}B, SeriesState={state_size}B, LastSample={last_sample_size}B, Option<CompactSeriesMetadata>={meta_size}B"
        );
        assert!(
            entry_size <= 176,
            "SeriesEntry size {entry_size}B exceeded 176B budget"
        );
        assert!(
            last_sample_size <= 16,
            "LastSample size {last_sample_size}B exceeded 16B budget"
        );
        assert!(meta_size <= 12, "Option<CompactSeriesMetadata> size {meta_size}B exceeded 12B");
    }

    #[test]
    fn postings_cache_push_then_query_and_per_metric_invalidation() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(Registry::new());
        let defaults = HeadOptions::default();
        let opts = HeadOptions {
            registry: Some(Arc::clone(&reg)),
            postings_cache: PostingsForMatchersCacheConfig {
                enable_invalidation: true,
                metrics: Some(crate::postings_cache::PostingsForMatchersCacheMetrics::new(
                    "prometheus_tsdb_",
                    Some(&reg),
                )),
                ..defaults.postings_cache.clone()
            },
            ..defaults
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        head.set_postings_cache_eligibility(true, true);

        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "metric_a"), ("instance", "1")], 1000, 1.0)
                .unwrap();
            app.commit().unwrap();
        }

        let m_a = [LabelMatcher::new(MatchType::Equal, "__name__", "metric_a").unwrap()];
        let m_inst = [LabelMatcher::new(MatchType::Equal, "instance", "1").unwrap()];

        // First query populates cache (miss)
        assert_eq!(head.postings_for_matchers(&m_a).unwrap().len(), 1);
        // Second query hits cache
        assert_eq!(head.postings_for_matchers(&m_a).unwrap().len(), 1);
        // Non-__name__ query populates cache (miss)
        assert_eq!(head.postings_for_matchers(&m_inst).unwrap().len(), 1);

        let hits_before = reg
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "prometheus_tsdb_postings_for_matchers_cache_hits_total")
            .unwrap()
            .get_metric()[0]
            .get_counter()
            .get_value();
        assert_eq!(hits_before, 1.0);

        // Creating a series in metric_b must NOT invalidate metric_a's cached postings,
        // but MUST invalidate m_inst (which has no __name__ equality matcher).
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "metric_b"), ("instance", "1")], 2000, 2.0)
                .unwrap();
            app.commit().unwrap();
        }

        assert_eq!(head.postings_for_matchers(&m_a).unwrap().len(), 1);
        let hits_after_b = reg
            .gather()
            .into_iter()
            .find(|mf| mf.get_name() == "prometheus_tsdb_postings_for_matchers_cache_hits_total")
            .unwrap()
            .get_metric()[0]
            .get_counter()
            .get_value();
        assert_eq!(hits_after_b, 2.0);

        // Strong consistency: m_inst sees the new metric_b series immediately
        assert_eq!(head.postings_for_matchers(&m_inst).unwrap().len(), 2);

        // Strong consistency: creating another metric_a series invalidates metric_a immediately
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "metric_a"), ("instance", "2")], 3000, 3.0)
                .unwrap();
            app.commit().unwrap();
        }
        assert_eq!(head.postings_for_matchers(&m_a).unwrap().len(), 2);

        head.close().unwrap();
    }

    #[test]
    fn parallel_wal_replay_concurrency_preserves_multi_ref_tombstones_and_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            wal_compression: CompressionType::Snappy,
            samples_per_chunk: 20,
            wal_replay_concurrency: 4,
            ..HeadOptions::default()
        };

        {
            let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            let mut app = head.appender();
            for s in 0..40usize {
                let name = format!("metric_{s}");
                let pairs = [("__name__", name.as_str()), ("job", "worker")];
                app.update_metadata(None, &pairs, &name, MetricType::Counter, "ops", "test help")
                    .unwrap();
                for t in 0..50i64 {
                    app.append_float(None, &pairs, 1_000_000 + t * 1000, (s + t as usize) as f64)
                        .unwrap();
                }
            }
            app.commit().unwrap();

            // Write an explicit duplicate RefSeries in WAL to exercise multi_ref remapping
            let first_entry = head
                .get_entry_by_sorted_pairs(&[("__name__", "metric_0"), ("job", "worker")])
                .unwrap();
            let dup_ref = head.last_series_ref() + 100;
            {
                let enc = RecordEncoder::new(false);
                let rs_rec = enc.series(
                    &[RefSeries {
                        ref_id: dup_ref,
                        labels: first_entry.labels.clone(),
                    }],
                    Vec::new(),
                );
                let samp_rec = enc.samples(
                    &[RefSample {
                        ref_id: dup_ref,
                        st: 0,
                        t: 1_000_000 + 60 * 1000,
                        v: 777.0,
                    }],
                    Vec::new(),
                );
                let del_entry = head
                    .get_entry_by_sorted_pairs(&[("__name__", "metric_1"), ("job", "worker")])
                    .unwrap();
                let stone_rec = enc.tombstones(
                    &[crate::record::Tombstone {
                        ref_id: del_entry.ref_id,
                        mint: i64::MIN,
                        maxt: i64::MAX,
                    }],
                    Vec::new(),
                );
                let mut wal = head.wal.lock();
                wal.as_mut()
                    .unwrap()
                    .log(&[&rs_rec, &samp_rec, &stone_rec])
                    .unwrap();
            }
            head.close().unwrap();
        }

        let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        assert_eq!(head.num_series(), 39);
        let e0 = head
            .get_entry_by_sorted_pairs(&[("__name__", "metric_0"), ("job", "worker")])
            .unwrap();
        let (_, chks, _) = head
            .series_chunks(e0.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let mut samples = Vec::new();
        for c in chks {
            samples.extend(decode_chunk_samples(&c.chunk.unwrap()).unwrap());
        }
        // TSDB-01 + Go resetSeriesWithMMappedChunks: the duplicate RefSeries record clears
        // pre-duplicate un-mmapped head_chunks (matching sequential replay & Go), leaving the
        // 34 mmapped samples + the 1 post-duplicate remapped sample (777.0) = 35 samples.
        assert_eq!(samples.len(), 35);
        assert_eq!(samples.last().unwrap().t, 1_000_000 + 60 * 1000);
        assert_eq!(samples.last().unwrap().value, SampleValue::Float(777.0));
        assert!(head
            .get_entry_by_sorted_pairs(&[("__name__", "metric_1"), ("job", "worker")])
            .is_none());
        head.close().unwrap();

        let seq_opts = HeadOptions {
            wal_replay_concurrency: 1,
            ..opts
        };
        let seq_head = Head::open(dir.path(), seq_opts, i64::MIN).unwrap();
        let e0_seq = seq_head
            .get_entry_by_sorted_pairs(&[("__name__", "metric_0"), ("job", "worker")])
            .unwrap();
        let (_, seq_chks, _) = seq_head
            .series_chunks(e0_seq.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let mut seq_samples = Vec::new();
        for c in seq_chks {
            seq_samples.extend(decode_chunk_samples(&c.chunk.unwrap()).unwrap());
        }
        assert_eq!(samples, seq_samples);
        seq_head.close().unwrap();
    }

    #[test]
    fn head_appender_pending_commits_in_batch_dup_and_ooo() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 10_000,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "up"), ("job", "api")];

        let mut app = head.appender();
        // First sample in batch (pending_commits == 0 fast path)
        let r = app.append_float(None, &pairs, 1_000_000, 1.0).unwrap();
        // Exact duplicate in same uncommitted batch -> Ok, dropped at commit
        app.append_float(Some(r), &pairs, 1_000_000, 1.0).unwrap();
        // Conflicting duplicate in same uncommitted batch -> Ok (Go headAppenderV2 parity),
        // silently dropped at commit because committed SeriesState already advances to 1_000_000 = 1.0.
        app.append_float(Some(r), &pairs, 1_000_000, 2.0).unwrap();
        // Newer in-order sample in same uncommitted batch -> succeeds
        app.append_float(Some(r), &pairs, 1_005_000, 5.0).unwrap();
        // OOO sample within window against committed head_maxt (1_000_000) -> accepted as OOO
        app.append_float(Some(r), &pairs, 1_002_000, 2.5).unwrap();
        // Conflicting duplicate in same uncommitted batch at 1_005_000 -> Ok, dropped at commit
        app.append_float(Some(r), &pairs, 1_005_000, 9.0).unwrap();
        app.commit().unwrap();

        let (_, chks, _) = head.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
        let mut got = Vec::new();
        for c in chks {
            for s in decode_chunk_samples(&c.chunk.unwrap()).unwrap() {
                got.push((s.t, s.value));
            }
        }
        got.sort_by_key(|(t, _)| *t);
        assert_eq!(
            got,
            vec![
                (1_000_000, SampleValue::Float(1.0)),
                (1_002_000, SampleValue::Float(2.5)),
                (1_005_000, SampleValue::Float(5.0)),
            ]
        );
        head.close().unwrap();
    }

    #[test]
    fn wal_replay_concurrency_zero_defaults_to_available_parallelism_and_one_stays_one() {
        let dir0 = tempfile::tempdir().unwrap();
        let head0 = Head::open(dir0.path(), HeadOptions::default(), i64::MIN).unwrap();
        let expected = std::thread::available_parallelism().map_or(4, |n| n.get());
        assert_eq!(head0.options().wal_replay_concurrency, expected);
        head0.close().unwrap();

        let dir1 = tempfile::tempdir().unwrap();
        let opts1 = HeadOptions {
            wal_replay_concurrency: 1,
            ..HeadOptions::default()
        };
        let head1 = Head::open(dir1.path(), opts1, i64::MIN).unwrap();
        assert_eq!(head1.options().wal_replay_concurrency, 1);
        head1.close().unwrap();
    }

    #[test]
    fn resolve_prehashed_batch_matches_single_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();

        let owned_pairs: Vec<(String, String)> = (0..100)
            .map(|i| (format!("metric_{i}"), format!("val_{i}")))
            .collect();
        {
            let mut app = head.appender();
            for (m, v) in &owned_pairs {
                app.append_float(
                    None,
                    &[("__name__", m.as_str()), ("job", v.as_str())],
                    1_000,
                    1.0,
                )
                .unwrap();
            }
            app.commit().unwrap();
        }

        type QueryPair<'a> = (u64, [(&'a str, &'a str); 2]);
        // Mix existing series, missing series, and skipped items.
        let mut queries: Vec<Option<QueryPair<'_>>> = Vec::new();
        for (i, (m, v)) in owned_pairs.iter().enumerate() {
            if i % 5 == 0 {
                queries.push(None);
            } else {
                let pairs = [("__name__", m.as_str()), ("job", v.as_str())];
                let h = hash_sorted_pairs(&pairs);
                queries.push(Some((h, pairs)));
            }
        }
        let missing_pairs = [("__name__", "does_not_exist"), ("job", "none")];
        queries.push(Some((hash_sorted_pairs(&missing_pairs), missing_pairs)));

        let mut batch_out = Vec::new();
        head.resolve_prehashed_batch(
            &queries,
            |q| q.as_ref().map(|(h, p)| (*h, &p[..])),
            &mut batch_out,
        );

        assert_eq!(batch_out.len(), queries.len());
        for (i, q) in queries.iter().enumerate() {
            let expected = q
                .as_ref()
                .and_then(|(h, p)| head.get_entry_by_prehashed_sorted_pairs(*h, p));
            assert_eq!(
                batch_out[i].as_ref().map(|e| e.ref_id),
                expected.as_ref().map(|e| e.ref_id),
                "mismatch at index {i}"
            );
        }
        head.close().unwrap();
    }

    #[test]
    fn resolve_prehashed_encoded_batch_matches_single_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();

        let encoded_items: Vec<(u64, Labels)> = (0..100)
            .map(|i| {
                let lbls = Labels::from_sorted_slice(&[
                    ("__name__", format!("metric_{i}").as_str()),
                    ("job", format!("val_{i}").as_str()),
                ]);
                (lbls.hash(), lbls)
            })
            .collect();
        {
            let mut app = head.appender();
            for (h, lbls) in &encoded_items {
                let mut entry_opt = None;
                app.append_float_with_entry_prehashed_encoded_take(
                    &mut entry_opt,
                    *h,
                    lbls.as_bytes(),
                    0,
                    1_000,
                    1.0,
                    false,
                )
                .unwrap();
            }
            app.commit().unwrap();
        }

        let mut queries: Vec<Option<(u64, Labels)>> = Vec::new();
        for (i, (h, lbls)) in encoded_items.iter().enumerate() {
            if i % 5 == 0 {
                queries.push(None);
            } else {
                queries.push(Some((*h, lbls.clone())));
            }
        }
        let missing = Labels::from_sorted_slice(&[("__name__", "does_not_exist"), ("job", "none")]);
        queries.push(Some((missing.hash(), missing)));

        let mut batch_out = Vec::new();
        head.resolve_prehashed_encoded_batch(
            &queries,
            |q| q.as_ref().map(|(h, l)| (*h, l.as_bytes())),
            &mut batch_out,
        );

        assert_eq!(batch_out.len(), queries.len());
        for (i, q) in queries.iter().enumerate() {
            let expected = q
                .as_ref()
                .and_then(|(h, l)| head.get_entry_by_prehashed_encoded_labels(*h, l.as_bytes()));
            assert_eq!(
                batch_out[i].as_ref().map(|e| e.ref_id),
                expected.as_ref().map(|e| e.ref_id),
                "mismatch at index {i}"
            );
        }
        head.close().unwrap();
    }

    #[test]
    fn parallel_wal_replay_interleaved_histograms_stalena_n_histograms() {
        use crate::chunkenc::{CounterResetHint, HistogramSpan};

        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            wal_replay_concurrency: 4,
            samples_per_chunk: 20,
            ..HeadOptions::default()
        };
        let make_hist = |sum: f64| Histogram {
            counter_reset_hint: CounterResetHint::NotCounterReset,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1,
            count: 2,
            sum,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![1],
            negative_spans: Vec::new(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        };
        let make_fhist = |sum: f64| FloatHistogram {
            counter_reset_hint: CounterResetHint::NotCounterReset,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1.0,
            count: 2.0,
            sum,
            positive_spans: vec![HistogramSpan {
                offset: 0,
                length: 1,
            }],
            positive_buckets: vec![1.0],
            negative_spans: Vec::new(),
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        };

        let num_series = 16usize;
        {
            let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            for s in 0..num_series {
                let h_name = format!("hist_interleave_{s}");
                let fh_name = format!("fhist_interleave_{s}");
                let hp = [("__name__", h_name.as_str())];
                let fhp = [("__name__", fh_name.as_str())];

                // Step 1: Histogram at t=1000
                let mut a1 = head.appender();
                let hr = a1.append_histogram(None, &hp, 0, 1_000, make_hist(10.0)).unwrap();
                let fhr = a1
                    .append_float_histogram(None, &fhp, 0, 1_000, make_fhist(10.0))
                    .unwrap();
                a1.commit().unwrap();

                // Step 2: Float StaleNaN at t=2000
                let mut a2 = head.appender();
                a2.append_float(Some(hr), &hp, 2_000, stale_nan()).unwrap();
                a2.append_float(Some(fhr), &fhp, 2_000, stale_nan()).unwrap();
                a2.commit().unwrap();

                // Step 3: Histogram at t=3000
                let mut a3 = head.appender();
                a3.append_histogram(Some(hr), &hp, 0, 3_000, make_hist(25.0))
                    .unwrap();
                a3.append_float_histogram(Some(fhr), &fhp, 0, 3_000, make_fhist(25.0))
                    .unwrap();
                a3.commit().unwrap();
            }
            head.close().unwrap();
        }

        // Reopen with wal_replay_concurrency=4: every series must have all 3 samples in exact WAL order.
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        for s in 0..num_series {
            let h_name = format!("hist_interleave_{s}");
            let fh_name = format!("fhist_interleave_{s}");

            let he = head
                .get_entry_by_sorted_pairs(&[("__name__", h_name.as_str())])
                .unwrap();
            let (_, h_chks, _) = head
                .series_chunks(he.ref_id, i64::MIN, i64::MAX)
                .unwrap()
                .unwrap();
            let mut h_samples = Vec::new();
            for c in h_chks {
                h_samples.extend(decode_chunk_samples(&c.chunk.unwrap()).unwrap());
            }
            assert_eq!(h_samples.len(), 3, "hist series {s} lost sample");
            assert_eq!(h_samples[0].t, 1_000);
            assert_eq!(h_samples[1].t, 2_000);
            assert!(matches!(
                h_samples[1].value,
                SampleValue::Histogram(ref hh) if is_stale_nan(hh.sum)
            ));
            assert_eq!(h_samples[2].t, 3_000);
            assert!(matches!(
                h_samples[2].value,
                SampleValue::Histogram(ref hh) if hh.sum == 25.0
            ));

            let fhe = head
                .get_entry_by_sorted_pairs(&[("__name__", fh_name.as_str())])
                .unwrap();
            let (_, fh_chks, _) = head
                .series_chunks(fhe.ref_id, i64::MIN, i64::MAX)
                .unwrap()
                .unwrap();
            let mut fh_samples = Vec::new();
            for c in fh_chks {
                fh_samples.extend(decode_chunk_samples(&c.chunk.unwrap()).unwrap());
            }
            assert_eq!(fh_samples.len(), 3, "fhist series {s} lost sample");
            assert_eq!(fh_samples[0].t, 1_000);
            assert_eq!(fh_samples[1].t, 2_000);
            assert!(matches!(
                fh_samples[1].value,
                SampleValue::FloatHistogram(ref hh) if is_stale_nan(hh.sum)
            ));
            assert_eq!(fh_samples[2].t, 3_000);
            assert!(matches!(
                fh_samples[2].value,
                SampleValue::FloatHistogram(ref hh) if hh.sum == 25.0
            ));
        }
        head.close().unwrap();
    }

    #[test]
    fn concurrent_truncate_memory_append_and_ooo_commit_no_deadlock_or_loss() {
        use std::sync::Barrier;

        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 5,
            ooo_time_window_ms: 500_000,
            ooo_cap_max: 4,
            max_head_chunk_file_size: 4096,
            ..HeadOptions::default()
        };
        let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());

        let base_t = 100_000i64;
        // Seed initial old samples on two series: one that stays old and gets GCed,
        // and one that continues receiving new samples concurrently with truncation.
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "ephemeral_old")], 1_000, 1.0)
                .unwrap();
            app.append_float(None, &[("__name__", "live_series")], base_t, 0.0)
                .unwrap();
            app.commit().unwrap();
        }

        let barrier = Arc::new(Barrier::new(5));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();

        // Thread 1: In-order + OOO writer on `live_series` (also exercises cached SeriesEntry GC re-resolve)
        {
            let h = Arc::clone(&head);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut cached_entry = None;
                let pairs = [("__name__", "live_series")];
                for i in 1..=120i64 {
                    let t = base_t + i * 1_000;
                    let mut app = h.appender();
                    app.append_float_with_entry(&mut cached_entry, &pairs, 0, t, i as f64, false)
                        .unwrap();
                    // Also append OOO samples to trigger OOO head chunk cuts -> chunk_writer -> chunk_reader
                    app.append_float_with_entry(
                        &mut cached_entry,
                        &pairs,
                        0,
                        t - 500,
                        (i as f64) + 0.5,
                        false,
                    )
                    .unwrap();
                    app.commit().unwrap();
                }
            }));
        }

        // Thread 2: Writer via `append_float_by_ref` + `append_histogram`
        {
            let h = Arc::clone(&head);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut app0 = h.appender();
                let r_byref = app0
                    .append_float(None, &[("__name__", "byref_series")], base_t, 0.0)
                    .unwrap();
                app0.commit().unwrap();

                for i in 1..=60i64 {
                    let mut app = h.appender();
                    let _ = app.append_float_by_ref(r_byref, base_t + i * 1_000, i as f64);
                    let _ = app.append_float_by_ref(r_byref, base_t + i * 1_000 - 250, i as f64);
                    app.commit().unwrap();
                }
            }));
        }

        // Thread 3: Concurrent mmap_head_chunks + truncate_memory + OOO compaction prep
        {
            let h = Arc::clone(&head);
            let b = Arc::clone(&barrier);
            let st = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut cut = 5_000i64;
                while !st.load(Ordering::Acquire) {
                    let _ = h.mmap_head_chunks();
                    let _ = h.truncate_memory(cut);
                    if cut < base_t + 30_000 {
                        cut += 2_000;
                    }
                    let _ = h.collect_ooo_compaction_candidates();
                    let _ = h.clear_ooo_data_before(base_t + 10_000);
                    std::thread::yield_now();
                }
            }));
        }

        // Thread 4: Concurrent reader (`series_chunks`)
        {
            let h = Arc::clone(&head);
            let b = Arc::clone(&barrier);
            let st = Arc::clone(&stop);
            handles.push(std::thread::spawn(move || {
                b.wait();
                while !st.load(Ordering::Acquire) {
                    if let Some(e) = h.get_entry_by_sorted_pairs(&[("__name__", "live_series")]) {
                        let _ = h.series_chunks(e.ref_id, i64::MIN, i64::MAX);
                    }
                    std::thread::yield_now();
                }
            }));
        }

        // Thread 5: Concurrent ephemeral churn series + cached SeriesEntry GC re-resolve test
        {
            let h = Arc::clone(&head);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut churn_cached = None;
                for i in 0..40 {
                    let name = format!("churn_{i}");
                    let mut app = h.appender();
                    app.append_float(None, &[("__name__", name.as_str())], 2_000, 1.0)
                        .unwrap();
                    let _ = app.append_float_with_entry(
                        &mut churn_cached,
                        &[("__name__", "ephemeral_old")],
                        0,
                        base_t + 50_000 + (i as i64) * 1000,
                        i as f64,
                        false,
                    );
                    app.commit().unwrap();
                }
            }));
        }

        // Join writer threads 1, 2, 5, then signal 3 & 4 to stop.
        let h5 = handles.pop().unwrap();
        let h4 = handles.pop().unwrap();
        let h3 = handles.pop().unwrap();
        let h2 = handles.pop().unwrap();
        let h1 = handles.pop().unwrap();
        h1.join().unwrap();
        h2.join().unwrap();
        h5.join().unwrap();
        stop.store(true, Ordering::Release);
        h3.join().unwrap();
        h4.join().unwrap();

        // Verify all post-truncation in-order samples on `live_series` are present (none lost!).
        let live = head
            .get_entry_by_sorted_pairs(&[("__name__", "live_series")])
            .expect("live_series must still exist");
        assert_eq!(live.pending_commits_count(), 0);
        let (_, chks, _) = head
            .series_chunks(live.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let mut ts_set = HashSet::new();
        for c in chks {
            for s in decode_chunk_samples(&c.chunk.unwrap()).unwrap() {
                ts_set.insert(s.t);
            }
        }
        // Truncate only went up to base_t + 30_000; every sample > base_t + 30_000 must exist!
        for i in 31..=120i64 {
            assert!(
                ts_set.contains(&(base_t + i * 1_000)),
                "missing in-order sample i={i}"
            );
        }
        head.close().unwrap();
    }

    #[test]
    fn commit_error_does_not_leak_pending_commits() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let pairs = [("__name__", "leak_check")];

        // Force WAL write error inside `commit()` (`log_wal` returns `WlogError::Closed`).
        {
            let mut wal = head.wal.lock();
            wal.as_mut().unwrap().force_close_for_test();
        }

        let mut app = head.appender();
        let ref_id = app.append_float(None, &pairs, 1_000, 1.0).unwrap();
        app.append_float(Some(ref_id), &pairs, 2_000, 2.0).unwrap();
        let entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
        assert_eq!(entry.pending_commits_count(), 3); // 1 created_series + 2 floats

        let err = app.commit().unwrap_err();
        assert!(matches!(err, HeadError::Wlog(WlogError::Closed)));
        // Every unconsumed pending_commits reservation must have been released on error!
        assert_eq!(entry.pending_commits_count(), 0);

        // Clear closed WAL writer so truncate_memory can GC the empty series cleanly.
        *head.wal.lock() = None;
        head.truncate_memory(5_000).unwrap();
        assert!(head.get_entry_by_sorted_pairs(&pairs).is_none());
        head.close().unwrap();
    }

    #[test]
    fn truncate_memory_updates_min_ooo_time_like_go() {
        // Without OOO window (`ooo_time_window_ms = 0`), `truncateMemory` sets `minOOOTime` to
        // `headMaxt - 0 == headMaxt` (`prometheus/tsdb/head.go:1994-2000`).
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        // Uninitialized head truncation (startup before WAL replay) leaves `min_ooo_time` untouched.
        head.truncate_memory(500).unwrap();
        assert_eq!(head.min_ooo_time(), i64::MAX);

        let pairs = [("__name__", "m")];
        let mut app = head.appender();
        app.append_float(None, &pairs, 1_000, 1.0).unwrap();
        app.commit().unwrap();
        assert_eq!(head.min_ooo_time(), i64::MAX);

        head.truncate_memory(1_001).unwrap();
        assert_eq!(head.max_time(), 1_001);
        assert_eq!(head.min_ooo_time(), 1_001);
        head.close().unwrap();

        // With `ooo_time_window_ms > 0`: `min_ooo_time = min(gc_min_ooo, head_maxt - ooo_window)`.
        let dir2 = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 600,
            ..HeadOptions::default()
        };
        let head2 = Head::open(dir2.path(), opts, i64::MIN).unwrap();
        let mut app = head2.appender();
        app.append_float(None, &pairs, 2_000, 1.0).unwrap();
        app.commit().unwrap();
        head2.truncate_memory(2_001).unwrap();
        assert_eq!(head2.min_ooo_time(), 2_001 - 600);

        // If an existing OOO chunk has `min_time < head_maxt - ooo_window`, keep its `min_time`.
        let mut app = head2.appender();
        app.append_float(None, &pairs, 2_500, 2.0).unwrap();
        app.commit().unwrap();
        let mut app = head2.appender();
        app.append_float(None, &pairs, 2_000, 3.0).unwrap(); // OOO within [2500-600, 2500)
        app.commit().unwrap();
        head2.set_ooo_time_window(100).unwrap();
        head2.truncate_memory(2_100).unwrap();
        // `gc` sees `oooHeadChunk.minTime = 2000` < `2500 - 100 = 2400`, so `2000` wins.
        assert_eq!(head2.min_ooo_time(), 2_000);
        head2.close().unwrap();
    }

    #[test]
    fn wave3_format_go_g_matches_strconv_format_float() {
        assert_eq!(format_go_g(0.0), "0");
        assert_eq!(format_go_g(-0.0), "-0");
        assert_eq!(format_go_g(f64::NAN), "NaN");
        assert_eq!(format_go_g(f64::from_bits(0xfff8_0000_0000_0001)), "NaN");
        assert_eq!(format_go_g(stale_nan()), "NaN");
        assert_eq!(format_go_g(f64::INFINITY), "+Inf");
        assert_eq!(format_go_g(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_go_g(1e6), "1e+06");
        assert_eq!(format_go_g(-1e6), "-1e+06");
        assert_eq!(format_go_g(1.5e-5), "1.5e-05");
        assert_eq!(format_go_g(-1.5e-5), "-1.5e-05");
        assert_eq!(format_go_g(999_999.0), "999999");
        assert_eq!(format_go_g(999_999.5), "999999.5");
        assert_eq!(
            format_go_g(f64::from_bits(0x412e_847f_ffff_ffff)),
            "999999.9999999999"
        );
        assert_eq!(format_go_g(0.0001), "0.0001");
        assert_eq!(
            format_go_g(f64::from_bits(0x3f1a_36e2_eb1c_432c)),
            "9.999999999999999e-05"
        );
        assert_eq!(format_go_g(1e100), "1e+100");
        assert_eq!(format_go_g(f64::from_bits(1)), "5e-324");
    }

    #[test]
    fn wave3_same_batch_hist_then_stale_nan_and_float_order_preserved() {
        use crate::chunkenc::HistogramSpan;

        let make_hist = |cnt: u64, sum: f64| Histogram {
            schema: 0,
            count: cnt,
            sum,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![cnt as i64],
            ..Histogram::default()
        };
        let make_fhist = |cnt: f64, sum: f64| FloatHistogram {
            schema: 0,
            count: cnt,
            sum,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![cnt],
            ..FloatHistogram::default()
        };

        // W3-01: out_of_order_time_window_ms = 0 — Histogram@1000 + Float(StaleNaN)@2000 in ONE
        // HeadAppender must commit Histogram@1000 first and convert StaleNaN@2000 into a stale
        // histogram in the same HistogramChunk, surviving WAL replay as well!
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 0,
            ..HeadOptions::default()
        };
        {
            let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            let hp = [("__name__", "m_ih_stale")];
            let fhp = [("__name__", "m_fh_stale")];
            let mixp = [("__name__", "m_ih_then_float")];

            let mut app = head.appender();
            let hr = app
                .append_histogram(None, &hp, 0, 1_000, make_hist(3, 4.5))
                .unwrap();
            app.append_float(Some(hr), &hp, 2_000, stale_nan()).unwrap();

            let fhr = app
                .append_float_histogram(None, &fhp, 0, 1_000, make_fhist(3.0, 4.5))
                .unwrap();
            app.append_float(Some(fhr), &fhp, 2_000, stale_nan())
                .unwrap();

            // Non-stale float after histogram in same appender cuts a new batch so both commit in order.
            let mr = app
                .append_histogram(None, &mixp, 0, 1_000, make_hist(5, 7.5))
                .unwrap();
            app.append_float(Some(mr), &mixp, 2_000, 42.0).unwrap();
            app.commit().unwrap();

            let (_, chks_ih, _) = head.series_chunks(hr, i64::MIN, i64::MAX).unwrap().unwrap();
            assert_eq!(chks_ih.len(), 1);
            assert_eq!(chks_ih[0].chunk.as_ref().unwrap().encoding(), Encoding::Histogram);
            let s_ih = decode_chunk_samples(chks_ih[0].chunk.as_ref().unwrap()).unwrap();
            assert_eq!(s_ih.len(), 2);
            assert_eq!(s_ih[0].t, 1_000);
            assert_eq!(s_ih[1].t, 2_000);
            match &s_ih[1].value {
                SampleValue::Histogram(h) => assert!(is_stale_nan(h.sum)),
                other => panic!("expected stale Histogram, got {other:?}"),
            }

            let (_, chks_fh, _) = head
                .series_chunks(fhr, i64::MIN, i64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(chks_fh.len(), 1);
            assert_eq!(
                chks_fh[0].chunk.as_ref().unwrap().encoding(),
                Encoding::FloatHistogram
            );
            let s_fh = decode_chunk_samples(chks_fh[0].chunk.as_ref().unwrap()).unwrap();
            assert_eq!(s_fh.len(), 2);

            let (_, chks_mix, _) = head.series_chunks(mr, i64::MIN, i64::MAX).unwrap().unwrap();
            assert_eq!(chks_mix.len(), 2);
            assert_eq!(chks_mix[0].min_time, 1_000);
            assert_eq!(chks_mix[1].min_time, 2_000);
            head.close().unwrap();
        }

        // Reopen from WAL and confirm identical chunks and samples.
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let he = head.get_entry_by_sorted_pairs(&[("__name__", "m_ih_stale")]).unwrap();
        let (_, chks_ih, _) = head
            .series_chunks(he.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(chks_ih.len(), 1);
        assert_eq!(
            decode_chunk_samples(chks_ih[0].chunk.as_ref().unwrap())
                .unwrap()
                .len(),
            2
        );
        let me = head
            .get_entry_by_sorted_pairs(&[("__name__", "m_ih_then_float")])
            .unwrap();
        let (_, chks_mix, _) = head
            .series_chunks(me.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(chks_mix.len(), 2);
        head.close().unwrap();
    }

    #[test]
    fn wave3_ooo_stale_nan_on_histograms_untrimmed_chunks_and_st_encodings() {
        use crate::chunkenc::HistogramSpan;

        let make_hist = |cnt: u64, sum: f64| Histogram {
            schema: 0,
            count: cnt,
            sum,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![cnt as i64],
            ..Histogram::default()
        };
        let make_fhist = |cnt: f64, sum: f64| FloatHistogram {
            schema: 0,
            count: cnt,
            sum,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![cnt],
            ..FloatHistogram::default()
        };

        // Enable Xor2 + HistogramSt + OOO window + small ooo_cap_max=2 to test W3-02, W3-03, W3-05
        // across both in-memory ooo_head_chunk AND mmapped chunks_head rollover + WBL replay!
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 60_000,
            ooo_cap_max: 2,
            float_chunk_encoding: Encoding::Xor2,
            enable_histogram_st_encoding: true,
            ..HeadOptions::default()
        };

        {
            let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
            let fp = [("__name__", "m_f_ooo")];
            let hp = [("__name__", "m_ih_ooo")];
            let fhp = [("__name__", "m_fh_ooo")];

            {
                let mut app = head.appender();
                app.append_float(None, &fp, 10_000, 10.0).unwrap();
                app.append_histogram(None, &hp, 0, 10_000, make_hist(5, 7.5))
                    .unwrap();
                app.append_float_histogram(None, &fhp, 0, 10_000, make_fhist(5.0, 7.5))
                    .unwrap();
                app.commit().unwrap();
            }

            // W3-02: OOO Float(StaleNaN) at t=5_000 on int-histogram and float-histogram series.
            {
                let mut app = head.appender();
                app.append_float(None, &fp, 4_000, 1.0).unwrap();
                app.append_float(None, &fp, 6_000, 2.0).unwrap();
                app.append_float(None, &hp, 5_000, stale_nan()).unwrap();
                app.append_float(None, &fhp, 5_000, stale_nan()).unwrap();
                app.commit().unwrap();
            }

            // W3-03 + W3-05: Querying a gap [4500, 5500] strictly inside ooo_head_chunk [4000, 6000]
            // MUST return the untrimmed Xor2 chunk [4000..6000] with both samples!
            let fe = head.get_entry_by_sorted_pairs(&fp).unwrap();
            let (_, f_chks, _) = head
                .series_chunks(fe.ref_id, 4_500, 5_500)
                .unwrap()
                .unwrap();
            assert_eq!(f_chks.len(), 1);
            assert_eq!(f_chks[0].min_time, 4_000);
            assert_eq!(f_chks[0].max_time, 6_000);
            assert_eq!(f_chks[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor2);

            let he = head.get_entry_by_sorted_pairs(&hp).unwrap();
            let (_, h_chks, _) = head
                .series_chunks(he.ref_id, 0, 20_000)
                .unwrap()
                .unwrap();
            assert_eq!(h_chks.len(), 2);
            for c in &h_chks {
                assert_eq!(c.chunk.as_ref().unwrap().encoding(), Encoding::HistogramSt);
            }

            let fhe = head.get_entry_by_sorted_pairs(&fhp).unwrap();
            let (_, fh_chks, _) = head
                .series_chunks(fhe.ref_id, 0, 20_000)
                .unwrap()
                .unwrap();
            assert_eq!(fh_chks.len(), 2);
            for c in &fh_chks {
                assert_eq!(
                    c.chunk.as_ref().unwrap().encoding(),
                    Encoding::FloatHistogramSt
                );
            }

            // Push a 3rd OOO sample on `fp` so ooo_cap_max=2 triggers chunks_head OOO rollover!
            {
                let mut app = head.appender();
                app.append_float(None, &fp, 7_000, 3.0).unwrap();
                app.commit().unwrap();
            }
            head.close().unwrap();
        }

        // Reopen from WAL + WBL + chunks_head and verify encodings & stale OOO histograms survived!
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let fe = head.get_entry_by_sorted_pairs(&[("__name__", "m_f_ooo")]).unwrap();
        let (_, f_chks, _) = head
            .series_chunks(fe.ref_id, 0, 20_000)
            .unwrap()
            .unwrap();
        assert!(f_chks
            .iter()
            .all(|c| c.chunk.as_ref().unwrap().encoding() == Encoding::Xor2));

        let he = head
            .get_entry_by_sorted_pairs(&[("__name__", "m_ih_ooo")])
            .unwrap();
        let (_, h_chks, _) = head
            .series_chunks(he.ref_id, 0, 20_000)
            .unwrap()
            .unwrap();
        assert_eq!(h_chks.len(), 2);
        assert!(h_chks
            .iter()
            .all(|c| c.chunk.as_ref().unwrap().encoding() == Encoding::HistogramSt));
        head.close().unwrap();
    }

    #[test]
    fn symbol_table_gc_reclaims_churn_and_preserves_metadata_and_postings() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();

        // Create 100 kept series with metadata, plus 2,000 old ephemeral series with metadata.
        {
            let mut app = head.appender();
            for i in 0..100 {
                let name = format!("kept_metric_{i}");
                let pairs = [("__name__", name.as_str()), ("env", "prod")];
                app.append_float(None, &pairs, 10_000, i as f64).unwrap();
                app.update_metadata(
                    None,
                    &pairs,
                    &name,
                    MetricType::Counter,
                    "seconds",
                    "kept help string",
                )
                .unwrap();
            }
            for i in 0..2_000 {
                let name = format!("old_churn_{i}");
                let pod = format!("pod-{i}");
                let pairs = [("__name__", name.as_str()), ("pod", pod.as_str())];
                app.append_float(None, &pairs, 1_000, i as f64).unwrap();
                app.update_metadata(
                    None,
                    &pairs,
                    &name,
                    MetricType::Gauge,
                    "bytes",
                    &format!("ephemeral help {i}"),
                )
                .unwrap();
            }
            app.commit().unwrap();
        }

        let (syms_before, bytes_before) = head.symbol_table_stats();
        assert!(syms_before > 4_000, "expected >4000 symbols, got {syms_before}");

        // Truncate before t=5_000: all 2,000 ephemeral series are GCed and symbols compacted.
        head.truncate_memory(5_000).unwrap();

        let (syms_after, bytes_after) = head.symbol_table_stats();
        assert!(
            syms_after < 500,
            "expected SymbolTable GC to drop symbols from {syms_before} to <500, got {syms_after}"
        );
        assert!(
            bytes_after < bytes_before / 4,
            "expected SymbolTable bytes to shrink: {bytes_before} -> {bytes_after}"
        );

        // Verify MemPostings lookups & SeriesState metadata survived symbol remapping.
        assert_eq!(head.num_series(), 100);
        let prod_refs = head.postings_for_label_value("env", "prod");
        assert_eq!(prod_refs.len(), 100);
        for i in 0..100 {
            let name = format!("kept_metric_{i}");
            let entry = head
                .get_entry_by_sorted_pairs(&[("__name__", name.as_str()), ("env", "prod")])
                .unwrap();
            let meta = {
                let syms = head.symbols.read();
                let st = entry.inner.lock();
                let m = st.metadata().unwrap();
                (
                    m.metric_type,
                    syms.resolve(m.unit).to_string(),
                    syms.resolve(m.help).to_string(),
                )
            };
            assert_eq!(meta.0, MetricType::Counter);
            assert_eq!(meta.1, "seconds");
            assert_eq!(meta.2, "kept help string");
        }
        assert!(head.postings_for_label_value("pod", "pod-0").is_empty());
        head.close().unwrap();
    }

    #[test]
    fn truncate_chunks_head_concurrent_with_ooo_writes_never_unlinks_active_seq() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 1_000_000,
            ooo_cap_max: 2,
            ..HeadOptions::default()
        };
        let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());

        // Seed in-order anchor at t=100_000 and an initial OOO chunk so chunks_head/000001 exists.
        {
            let mut app = head.appender();
            let pairs = [("__name__", "ooo_race_metric"), ("job", "race")];
            app.append_float(None, &pairs, 100_000, 1.0).unwrap();
            app.append_float(None, &pairs, 90_000, 2.0).unwrap();
            app.append_float(None, &pairs, 90_001, 3.0).unwrap();
            app.append_float(None, &pairs, 90_002, 4.0).unwrap();
            app.commit().unwrap();
        }

        // Phase 1: Drive concurrent `truncate_chunks_head(usize::MAX)` against `chunk_writer`
        // cuts on a fresh writer-race series while also checking `refresh_chunk_reader()` never
        // observes a sequence gap (`NonSequentialFiles`) or unlinked newly cut file.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_trunc = Arc::clone(&stop);
        let head_trunc = Arc::clone(&head);
        let trunc_handle = std::thread::spawn(move || {
            while !stop_trunc.load(Ordering::Relaxed) {
                head_trunc.truncate_chunks_head(usize::MAX).unwrap();
                std::thread::yield_now();
            }
        });

        let mut written_refs: Vec<(u64, crate::head_chunks::ChunkDiskMapperRef)> = Vec::new();
        for i in 0..160i64 {
            let name = format!("ooo_race_series_{i}");
            let pairs = [("__name__", name.as_str()), ("job", "race")];
            let mut app = head.appender();
            app.append_float(None, &pairs, 100_000, 1.0).unwrap();
            app.append_float(None, &pairs, 90_000, 2.0).unwrap();
            app.append_float(None, &pairs, 90_001, 3.0).unwrap();
            app.append_float(None, &pairs, 90_002, 4.0).unwrap();
            app.commit().unwrap();
            let e = head.get_entry_by_sorted_pairs(&pairs).unwrap();
            let st = e.inner.lock();
            if let Some(ooo) = st.ooo() {
                for mc in &ooo.ooo_mmapped_chunks {
                    written_refs.push((e.ref_id, mc.ref_id));
                }
            }
        }

        stop.store(true, Ordering::Release);
        trunc_handle.join().unwrap();

        // After stopping `truncate_chunks_head(usize::MAX)`, `refresh_chunk_reader` must succeed
        // (no `NonSequentialFiles` from unlinked newly-cut files) and the latest active file's
        // chunks must all exist in `chunk_reader`.
        head.refresh_chunk_reader().unwrap();
        let active_seq = head.chunk_writer.lock().as_ref().unwrap().current_seq();
        let reader_guard = head.chunk_reader.read();
        let reader = reader_guard.as_ref().unwrap();
        assert!(
            reader.has_file(active_seq),
            "active chunks_head file {active_seq} was unlinked!"
        );
        for (_, r) in written_refs
            .iter()
            .filter(|(_, r)| r.unpack().0 as usize >= active_seq)
        {
            reader.chunk(*r).unwrap();
        }
        drop(reader_guard);
        head.close().unwrap();

        // Phase 2: Full end-to-end concurrent `clear_ooo_data_before` (which computes
        // `min_mmap_seq == usize::MAX` when all older OOO chunks are cleared) + concurrent
        // `series_chunks` + concurrent OOO writes on a fresh Head.
        let dir2 = tempfile::tempdir().unwrap();
        let opts2 = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 1_000_000,
            ooo_cap_max: 2,
            ..HeadOptions::default()
        };
        let head2 = Arc::new(Head::open(dir2.path(), opts2, i64::MIN).unwrap());
        {
            let mut app = head2.appender();
            let pairs = [("__name__", "ooo_race_metric"), ("job", "race")];
            app.append_float(None, &pairs, 100_000, 1.0).unwrap();
            for t in 90_000..90_010 {
                app.append_float(None, &pairs, t, 2.0).unwrap();
            }
            app.commit().unwrap();
        }
        let stop2 = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2_t = Arc::clone(&stop2);
        let head2_t = Arc::clone(&head2);
        let trunc_ooo_handle = std::thread::spawn(move || {
            for step in 1..=30i64 {
                if stop2_t.load(Ordering::Relaxed) {
                    break;
                }
                head2_t.clear_ooo_data_before(90_500 + step * 10).unwrap();
                std::thread::yield_now();
            }
        });

        let entry_ref = head2
            .get_entry_by_sorted_pairs(&[("__name__", "ooo_race_metric"), ("job", "race")])
            .unwrap()
            .ref_id;
        let stop2_r = Arc::clone(&stop2);
        let head2_r = Arc::clone(&head2);
        let read_handle = std::thread::spawn(move || {
            while !stop2_r.load(Ordering::Relaxed) {
                let res = head2_r.series_chunks(entry_ref, i64::MIN, i64::MAX);
                assert!(
                    res.is_ok(),
                    "series_chunks failed during concurrent clear_ooo_data_before: {res:?}"
                );
                std::thread::yield_now();
            }
        });

        for i in 0..80i64 {
            let mut app = head2.appender();
            let pairs = [("__name__", "ooo_race_metric"), ("job", "race")];
            app.append_float(None, &pairs, 92_000 + i, i as f64).unwrap();
            app.commit().unwrap();
        }
        stop2.store(true, Ordering::Release);
        trunc_ooo_handle.join().unwrap();
        read_handle.join().unwrap();

        head2.refresh_chunk_reader().unwrap();
        head2.close().unwrap();
    }

    #[test]
    fn series_chunks_survives_concurrent_truncate_memory() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 1_000,
            samples_per_chunk: 2,
            ..HeadOptions::default()
        };
        let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());

        let pairs = [("__name__", "trunc_race"), ("id", "1")];
        for i in 0..40i64 {
            let mut app = head.appender();
            app.append_float(None, &pairs, 1_000 + i * 1_000, i as f64)
                .unwrap();
            app.commit().unwrap();
            if i % 2 == 1 {
                head.mmap_head_chunks().unwrap();
            }
        }
        let ref_id = head.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_r = Arc::clone(&stop);
        let head_r = Arc::clone(&head);
        let reader = std::thread::spawn(move || {
            while !stop_r.load(Ordering::Relaxed) {
                let res = head_r.series_chunks(ref_id, i64::MIN, i64::MAX);
                assert!(
                    res.is_ok(),
                    "series_chunks failed during concurrent truncate_memory: {res:?}"
                );
            }
        });

        for step in 1..=20i64 {
            head.truncate_memory(step * 2_000).unwrap();
        }
        stop.store(true, Ordering::Release);
        reader.join().unwrap();
        head.close().unwrap();
    }

    #[test]
    fn update_metadata_and_delete_series_entries_safe_under_symbol_gc() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();

        let mut old_entries = Vec::new();
        {
            let mut app = head.appender();
            for i in 0..200 {
                let name = format!("del_m_{i}");
                let pairs = [("__name__", name.as_str()), ("k", "v")];
                app.append_float(None, &pairs, 1_000, i as f64).unwrap();
            }
            let kept = [("__name__", "kept_meta_m"), ("k", "v")];
            app.append_float(None, &kept, 10_000, 1.0).unwrap();
            app.update_metadata(None, &kept, "kept_meta_m", MetricType::Counter, "bytes", "help1")
                .unwrap();
            app.commit().unwrap();
        }
        for i in 0..200 {
            let name = format!("del_m_{i}");
            if let Some(e) = head.get_entry_by_sorted_pairs(&[("__name__", name.as_str()), ("k", "v")]) {
                old_entries.push(e);
            }
        }
        head.delete_series_entries(&old_entries);
        head.maybe_gc_symbols();

        let mut app2 = head.appender();
        let kept = [("__name__", "kept_meta_m"), ("k", "v")];
        // Same metadata after symbol GC must not lose or corrupt unit/help resolution.
        app2.update_metadata(None, &kept, "kept_meta_m", MetricType::Counter, "bytes", "help1")
            .unwrap();
        // Updated metadata must be recorded.
        app2.update_metadata(None, &kept, "kept_meta_m", MetricType::Counter, "seconds", "help2")
            .unwrap();
        app2.commit().unwrap();

        let entry = head.get_entry_by_sorted_pairs(&kept).unwrap();
        let syms = head.symbols.read();
        let st = entry.inner.lock();
        let m = st.metadata().unwrap();
        assert_eq!(syms.resolve(m.unit), "seconds");
        assert_eq!(syms.resolve(m.help), "help2");
        drop(st);
        drop(syms);
        assert!(head.postings_for_label_value("__name__", "del_m_0").is_empty());
        head.close().unwrap();
    }

    #[test]
    fn paged_by_id_direct_lookup_overflow_and_shrink() {
        let stripes = StripeSeries::new(64);
        let ids = [
            1u64,
            BY_ID_PAGE_SIZE as u64,
            MAX_PAGED_SERIES_ID - 1,
            MAX_PAGED_SERIES_ID,
            MAX_PAGED_SERIES_ID + 7,
        ];
        let mut entries = Vec::new();
        for &id in &ids {
            let lbl = Labels::from_pairs([("__name__", "m"), ("id", &id.to_string())]);
            let h = lbl.hash();
            let e = Arc::new(SeriesEntry::new(id, h, lbl, 0));
            e.set_active_cache(id ^ 0xA5A5_5A5A);
            stripes.insert(Arc::clone(&e));
            entries.push(e);
        }

        // 3 paged pages (id=1, id=4096, id=MAX_PAGED_SERIES_ID-1) + 2 overflow entries.
        assert_eq!(stripes.allocated_page_count(), 3);
        for &id in &ids {
            let got = stripes.get_by_id(id).expect("series must exist");
            assert_eq!(got.ref_id, id);
            assert_eq!(stripes.active_cache_for_id(id), id ^ 0xA5A5_5A5A);
        }
        assert!(stripes.get_by_id(2).is_none());
        assert_eq!(stripes.active_cache_for_id(2), 0);

        // Remove the lone entry on page 1 (id = BY_ID_PAGE_SIZE) and shrink -> page 1 freed.
        stripes.remove(&entries[1]);
        assert!(stripes.get_by_id(ids[1]).is_none());
        stripes.shrink_oversized_stripes();
        assert_eq!(stripes.allocated_page_count(), 2);

        // Remove remaining paged entries + overflow entries and shrink -> 0 pages allocated.
        for e in entries.iter().filter(|e| e.ref_id != ids[1]) {
            stripes.remove(e);
        }
        stripes.shrink_oversized_stripes();
        assert_eq!(stripes.allocated_page_count(), 0);
        for &id in &ids {
            assert!(stripes.get_by_id(id).is_none());
        }
    }

    #[test]
    fn paged_by_id_overwrite_and_drop_non_empty() {
        let stripes = StripeSeries::new(16);
        for id in [1u64, 2, 3, BY_ID_PAGE_SIZE as u64 + 1] {
            let lbl = Labels::from_pairs([("__name__", "m"), ("id", &id.to_string())]);
            let e = Arc::new(SeriesEntry::new(id, lbl.hash(), lbl, 0));
            stripes.insert(e);
        }
        // Overwrite id=1 to exercise non-empty slot overwrite (live_counts must remain accurate).
        let lbl1_new = Labels::from_pairs([("__name__", "m_new"), ("id", "1")]);
        let e1_new = Arc::new(SeriesEntry::new(1, lbl1_new.hash(), lbl1_new, 0));
        e1_new.set_active_cache(0x1234);
        stripes.insert(e1_new);
        assert_eq!(stripes.active_cache_for_id(1), 0x1234);
        assert_eq!(stripes.allocated_page_count(), 2);
        // Drop stripes while pages and dirs are still populated.
    }

    #[test]
    fn hash_sorted_pairs_stack_and_heap_spill() {
        let small = [("__name__", "up"), ("job", "api")];
        assert_eq!(
            hash_sorted_pairs(&small),
            Labels::from_sorted_slice(&small).hash()
        );
        // Exceed 512-byte stack buffer to exercise MaybeUninit -> Vec spill path.
        let big_val = "x".repeat(600);
        let big = [("__name__", "big_metric"), ("payload", big_val.as_str())];
        assert_eq!(
            hash_sorted_pairs(&big),
            Labels::from_sorted_slice(&big).hash()
        );
    }

    #[test]
    fn series_final_chunks_only_and_active_cache_for_id() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let pairs = [("__name__", "fast_read_metric"), ("env", "prod")];
        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 1_000, 1.5).unwrap();
            app.append_float(None, &pairs, 2_000, 2.5).unwrap();
            app.commit().unwrap();
        }
        let entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
        let ref_id = entry.ref_id;
        entry.set_active_cache(0xDEAD_BEEF_1234_5678);
        assert_eq!(head.active_cache_for_id(ref_id), 0xDEAD_BEEF_1234_5678);
        assert_eq!(head.active_cache_for_id(ref_id + 100), 0);

        let expected = head
            .series_final_chunks(ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap()
            .1;
        let only = head
            .series_final_chunks_only(ref_id, i64::MIN, i64::MAX)
            .unwrap();
        assert_eq!(only.len(), expected.len());
        assert_eq!(only[0].min_time, expected[0].min_time);
        assert_eq!(only[0].max_time, expected[0].max_time);
        assert_eq!(only[0].chunk, expected[0].chunk);

        // Out-of-range / missing ref returns empty Vec without error.
        assert!(head
            .series_final_chunks_only(ref_id, 5_000, 6_000)
            .unwrap()
            .is_empty());
        assert!(head
            .series_final_chunks_only(ref_id + 100, i64::MIN, i64::MAX)
            .unwrap()
            .is_empty());
        head.close().unwrap();
    }

    #[test]
    fn next_at_and_max_time_large_span_cold_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let huge_range = (u32::MAX as i64) * 4;
        let opts = HeadOptions {
            chunk_range_ms: huge_range,
            samples_per_chunk: 120,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "huge_span_metric"), ("k", "1")];
        let t0 = 1_000i64;
        let t1 = t0 + (u32::MAX as i64) + 500_000;
        {
            let mut app = head.appender();
            app.append_float(None, &pairs, t0, 10.0).unwrap();
            app.append_float(None, &pairs, t1, 20.0).unwrap();
            app.commit().unwrap();
        }
        let entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
        {
            let st = entry.inner.lock();
            assert_eq!(st.min_time(), t0);
            assert_eq!(st.max_time(), t1);
            assert!(st.cold.is_some());
        }
        let (_, chks) = head
            .series_final_chunks(entry.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(chks.len(), 1);
        assert_eq!(chks[0].min_time, t0);
        assert_eq!(chks[0].max_time, t1);
        head.close().unwrap();
    }

    #[test]
    fn head_rollback_and_drop_persist_created_series_refs_across_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let s_rb = [("__name__", "rb_metric"), ("kind", "explicit_rollback")];
        let s_drop = [("__name__", "rb_metric"), ("kind", "implicit_drop")];

        {
            let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();

            // Batch 1a: creates s_rb then explicitly rolls back.
            let mut app1 = head.appender();
            app1.append_float(None, &s_rb, 1_000, 10.0).unwrap();
            app1.rollback().unwrap();

            // Batch 1b: creates s_drop then drops without commit/rollback.
            {
                let mut app2 = head.appender();
                app2.append_float(None, &s_drop, 1_000, 20.0).unwrap();
            }

            // Batch 2: appends valid samples to both already-created series and commits.
            let mut app3 = head.appender();
            app3.append_float(None, &s_rb, 2_000, 42.0).unwrap();
            app3.append_float(None, &s_drop, 2_000, 84.0).unwrap();
            app3.commit().unwrap();
            head.close().unwrap();
        }

        // Reopen from WAL and verify both series + Batch 2 samples survive 100% (Batch 1 discarded).
        let reopened = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let e_rb = reopened
            .get_entry_by_sorted_pairs(&s_rb)
            .expect("s_rb must survive WAL replay");
        let e_drop = reopened
            .get_entry_by_sorted_pairs(&s_drop)
            .expect("s_drop must survive WAL replay");

        let read_samples = |ref_id: u64| -> Vec<(i64, f64)> {
            let (_, chks) = reopened
                .series_final_chunks(ref_id, i64::MIN, i64::MAX)
                .unwrap()
                .unwrap();
            chks.into_iter()
                .flat_map(|c| crate::compact::decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
                .map(|s| {
                    let crate::compact::SampleValue::Float(v) = s.value else {
                        panic!("expected float");
                    };
                    (s.t, v)
                })
                .collect()
        };
        assert_eq!(read_samples(e_rb.ref_id), vec![(2_000, 42.0)]);
        assert_eq!(read_samples(e_drop.ref_id), vec![(2_000, 84.0)]);
        reopened.close().unwrap();
    }


    fn copy_dir_recursive(src: &Path, dst: &Path) {
        fs::create_dir_all(dst).unwrap();
        for entry in fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let ty = entry.file_type().unwrap();
            let to = dst.join(entry.file_name());
            if ty.is_dir() {
                copy_dir_recursive(&entry.path(), &to);
            } else {
                fs::copy(entry.path(), &to).unwrap();
            }
        }
    }

    fn read_head_float_samples(head: &Head, ref_id: u64) -> Vec<(i64, f64)> {
        let (_, chks) = head
            .series_final_chunks(ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let mut out: Vec<(i64, f64)> = chks
            .into_iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        out.sort_by_key(|x| x.0);
        out
    }

    #[test]
    fn w22_res_01_buffered_ooo_flush_enospc_recovery_and_cross_series_reuse() {
        let dir = tempfile::tempdir().unwrap();
        // Default 4 MiB head_chunks_write_buffer_size so cut OOO chunks buffer in RAM first.
        let opts = HeadOptions {
            ooo_time_window_ms: 500_000,
            ooo_cap_max: 4,
            samples_per_chunk: 4,
            ..HeadOptions::default()
        };
        assert_eq!(
            opts.head_chunks_write_buffer_size,
            crate::head_chunks::DEFAULT_WRITE_BUFFER_SIZE
        );
        let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let s_a = [("__name__", "w22_res01_a"), ("job", "api")];
        let s_b = [("__name__", "w22_res01_b"), ("job", "api")];
        let s_c = [("__name__", "w22_res01_c"), ("job", "api")];

        // Establish high-water mark for Series A and pre-populate Series B's sealed in-order
        // chunk in memory (not yet mmapped) so mmap_head_chunks() can flush it to (1, 8) later.
        {
            let mut app = head.appender();
            app.append_float(None, &s_a, 100_000, 999.0).unwrap();
            let mut r = None;
            for i in 1..=6i64 {
                let id = app
                    .append_float(r, &s_b, 200_000 + i * 1_000, 100.0 + i as f64)
                    .unwrap();
                r = Some(id);
            }
            app.commit().unwrap();
        }
        // Fill Series A's OOO head chunk to ooo_cap_max (4 samples).
        {
            let mut app = head.appender();
            for i in 1..=4i64 {
                app.append_float(None, &s_a, i * 1_000, i as f64).unwrap();
            }
            app.commit().unwrap();
        }
        let e_a = head.get_entry_by_sorted_pairs(&s_a).unwrap();

        // Inject ENOSPC on buffered flush (len > 8 allows cut_new_file's 8-byte header write so
        // insert_ooo buffers C1 into the 4 MiB write_buf at (seq=1, off=8), then cw.flush() fails).
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|len| {
                if len > 8 {
                    crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
                } else {
                    crate::head_chunks::WriteFaultAction::PassThrough
                }
            })));
        }
        {
            let mut app = head.appender();
            app.append_float(None, &s_a, 5_000, 5.0).unwrap();
            assert!(
                app.commit().is_err(),
                "commit must fail when buffered cw.flush() hits ENOSPC"
            );
        }
        {
            let st_a = e_a.inner.lock();
            assert_eq!(
                st_a.ooo().unwrap().ooo_pending_mmapped.len(),
                1,
                "C1 must have been cut into 4 MiB write_buf and retained in ooo_pending_mmapped"
            );
        }

        // Snapshot disk BEFORE recovery to verify crash recovery prior to disk recovery.
        let crash_before_dir = tempfile::tempdir().unwrap();
        copy_dir_recursive(dir.path(), crash_before_dir.path());
        {
            let reopened_before =
                Head::open(crash_before_dir.path(), opts.clone(), i64::MIN).unwrap();
            let ra = reopened_before.get_entry_by_sorted_pairs(&s_a).unwrap();
            assert_eq!(
                read_head_float_samples(&reopened_before, ra.ref_id),
                vec![
                    (1_000, 1.0),
                    (2_000, 2.0),
                    (3_000, 3.0),
                    (4_000, 4.0),
                    (5_000, 5.0),
                    (100_000, 999.0),
                ]
            );
            reopened_before.close().unwrap();
        }

        // Disk recovers.
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(None);
        }

        // Series B flushes its sealed in-order chunk via mmap_head_chunks(), reusing (seq=1, off=8)
        // and advancing last_flushed_ref past Series A's discarded ref BEFORE any OOO recovery!
        assert!(head.mmap_head_chunks().unwrap() >= 1);

        // Even if truncate_ooo runs with min_ooo_mmap_ref >= rolled-back ref BEFORE Series A's
        // pending chunk is re-written, Series A's pending chunk MUST NOT be dropped.
        head.truncate_ooo(None, crate::head_chunks::ChunkDiskMapperRef::new(1, 1024))
            .unwrap();
        {
            let st_a = e_a.inner.lock();
            assert_eq!(
                st_a.ooo().unwrap().ooo_pending_mmapped.len(),
                1,
                "truncate_ooo must NOT drop discarded pending OOO chunk"
            );
        }

        // Series C writes & cuts an OOO chunk, triggering CommitContext::finish() which must
        // re-write Series A's discarded pending chunk to a fresh (seq, off), flush, and promote
        // BOTH Series A and Series C cleanly without touching Series B's disk bytes.
        {
            let mut app = head.appender();
            app.append_float(None, &s_c, 100_000, 888.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            for i in 1..=5i64 {
                app.append_float(None, &s_c, i * 1_000, 50.0 + i as f64)
                    .unwrap();
            }
            app.commit().unwrap();
        }

        let e_b = head.get_entry_by_sorted_pairs(&s_b).unwrap();
        let e_c = head.get_entry_by_sorted_pairs(&s_c).unwrap();
        {
            let st_a = e_a.inner.lock();
            assert!(
                st_a.ooo().unwrap().ooo_pending_mmapped.is_empty(),
                "Series A discarded pending chunk must be re-written and promoted"
            );
        }
        assert_eq!(
            read_head_float_samples(&head, e_a.ref_id),
            vec![
                (1_000, 1.0),
                (2_000, 2.0),
                (3_000, 3.0),
                (4_000, 4.0),
                (5_000, 5.0),
                (100_000, 999.0),
            ]
        );
        assert_eq!(
            read_head_float_samples(&head, e_b.ref_id),
            vec![
                (201_000, 101.0),
                (202_000, 102.0),
                (203_000, 103.0),
                (204_000, 104.0),
                (205_000, 105.0),
                (206_000, 106.0),
            ]
        );
        assert_eq!(
            read_head_float_samples(&head, e_c.ref_id),
            vec![
                (1_000, 51.0),
                (2_000, 52.0),
                (3_000, 53.0),
                (4_000, 54.0),
                (5_000, 55.0),
                (100_000, 888.0),
            ]
        );

        // Snapshot disk AFTER recovery (without head.close()) to test crash recovery from WAL/WBL.
        let crash_after_dir = tempfile::tempdir().unwrap();
        copy_dir_recursive(dir.path(), crash_after_dir.path());
        head.close().unwrap();

        let reopened_after = Head::open(crash_after_dir.path(), opts, i64::MIN).unwrap();
        let ra = reopened_after.get_entry_by_sorted_pairs(&s_a).unwrap();
        let rb = reopened_after.get_entry_by_sorted_pairs(&s_b).unwrap();
        let rc = reopened_after.get_entry_by_sorted_pairs(&s_c).unwrap();
        assert_eq!(
            read_head_float_samples(&reopened_after, ra.ref_id),
            vec![
                (1_000, 1.0),
                (2_000, 2.0),
                (3_000, 3.0),
                (4_000, 4.0),
                (5_000, 5.0),
                (100_000, 999.0),
            ]
        );
        assert_eq!(
            read_head_float_samples(&reopened_after, rb.ref_id),
            vec![
                (201_000, 101.0),
                (202_000, 102.0),
                (203_000, 103.0),
                (204_000, 104.0),
                (205_000, 105.0),
                (206_000, 106.0),
            ]
        );
        assert_eq!(
            read_head_float_samples(&reopened_after, rc.ref_id),
            vec![
                (1_000, 51.0),
                (2_000, 52.0),
                (3_000, 53.0),
                (4_000, 54.0),
                (5_000, 55.0),
                (100_000, 888.0),
            ]
        );
        reopened_after.close().unwrap();
    }

    #[test]
    fn w22_deep_02_1_flushed_vs_discarded_pending_ooo_mmap_ref_skip() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 500_000,
            ooo_cap_max: 2,
            samples_per_chunk: 2,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let s_discarded = [("__name__", "w22_pending_discarded"), ("job", "a")];
        let s_flushed = [("__name__", "w22_pending_flushed"), ("job", "a")];

        {
            let mut app = head.appender();
            app.append_float(None, &s_discarded, 100_000, 100.0)
                .unwrap();
            app.append_float(None, &s_flushed, 100_000, 100.0).unwrap();
            app.commit().unwrap();
        }

        {
            let mut app = head.appender();
            app.append_float(None, &s_flushed, 1_000, 1.0).unwrap();
            app.append_float(None, &s_flushed, 2_000, 2.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float(None, &s_flushed, 3_000, 3.0).unwrap();
            app.commit().unwrap();
        }
        let e_flushed = head.get_entry_by_sorted_pairs(&s_flushed).unwrap();
        let flushed_ref = {
            let st = e_flushed.inner.lock();
            let ooo = st.ooo().unwrap();
            assert!(
                !ooo.ooo_mmapped_chunks.is_empty(),
                "s_flushed must have mmapped OOO chunk"
            );
            ooo.ooo_mmapped_chunks.last().unwrap().ref_id
        };

        {
            let mut app = head.appender();
            app.append_float(None, &s_discarded, 1_000, 11.0).unwrap();
            app.append_float(None, &s_discarded, 2_000, 12.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut()
                .unwrap()
                .set_write_fault_hook(Some(Arc::new(|len| {
                    if len > 8 {
                        crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
                    } else {
                        crate::head_chunks::WriteFaultAction::PassThrough
                    }
                })));
        }
        {
            let mut app = head.appender();
            app.append_float(None, &s_discarded, 3_000, 13.0).unwrap();
            assert!(app.commit().is_err());
        }
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(None);
        }

        let e_disc = head.get_entry_by_sorted_pairs(&s_discarded).unwrap();
        let iso = HeadReadIsolation {
            ino_mint: 0,
            max_ooo_mmap_ref_to_skip: ChunkDiskMapperRef::new(flushed_ref.unpack().0 + 10, 0),
            evicting: None,
        };

        let chks_flushed = head
            .series_final_chunks_only_with_isolation(e_flushed.ref_id, 0, 10_000, &iso)
            .unwrap();
        let ts_flushed: Vec<i64> = chks_flushed
            .iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| s.t)
            .collect();
        assert_eq!(ts_flushed, vec![3_000]);

        let chks_disc = head
            .series_final_chunks_only_with_isolation(e_disc.ref_id, 0, 10_000, &iso)
            .unwrap();
        let ts_disc: Vec<i64> = chks_disc
            .iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| s.t)
            .collect();
        assert_eq!(ts_disc, vec![1_000, 2_000, 3_000]);

        head.close().unwrap();
    }

    #[test]
    fn w22_res_01_compact_ooo_head_enospc_flush_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 100_000,
            ooo_cap_max: 8,
            samples_per_chunk: 4,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let s_a = [("__name__", "w22_res01_compact_a"), ("job", "api")];
        let s_b = [("__name__", "w22_res01_compact_b"), ("job", "api")];

        {
            let mut app = head.appender();
            app.append_float(None, &s_a, 100_000, 10.0).unwrap();
            app.append_float(None, &s_b, 100_000, 20.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float(None, &s_a, 1_000, 1.0).unwrap();
            app.append_float(None, &s_a, 2_000, 2.0).unwrap();
            app.commit().unwrap();
        }

        // Inject ENOSPC on flush during collect_ooo_compaction_candidates_with_watermark().
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|_len| {
                crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
            })));
        }
        assert!(head
            .collect_ooo_compaction_candidates_with_watermark()
            .is_err());

        // Recover disk, let Series B reuse (seq=1, off=8) via mmap_head_chunks.
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(None);
        }
        {
            let mut app = head.appender();
            let mut r = None;
            for i in 1..=5i64 {
                let id = app
                    .append_float(r, &s_b, 200_000 + i * 1_000, 200.0 + i as f64)
                    .unwrap();
                r = Some(id);
            }
            app.commit().unwrap();
        }
        assert!(head.mmap_head_chunks().unwrap() >= 1);

        // Retry OOO compaction candidate collection: must re-write Series A's discarded pending
        // chunk cleanly, promote it, and return Series A's exact samples.
        let snap = head
            .collect_ooo_compaction_candidates_with_watermark()
            .unwrap();
        assert_eq!(snap.candidates.len(), 1);
        let (ref_id, labels) = &snap.candidates[0];
        assert_eq!(labels.get("__name__"), "w22_res01_compact_a");
        let mut chks = Vec::new();
        let mut tombs = Vec::new();
        head.load_series_ooo_chunks_into_up_to(
            *ref_id,
            snap.last_mmap_ref,
            &mut chks,
            &mut tombs,
        )
        .unwrap();
        let mut got: Vec<(i64, f64)> = chks
            .into_iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        got.sort_by_key(|x| x.0);
        assert_eq!(got, vec![(1_000, 1.0), (2_000, 2.0)]);

        let e_b = head.get_entry_by_sorted_pairs(&s_b).unwrap();
        assert_eq!(
            read_head_float_samples(&head, e_b.ref_id),
            vec![
                (100_000, 20.0),
                (201_000, 201.0),
                (202_000, 202.0),
                (203_000, 203.0),
                (204_000, 204.0),
                (205_000, 205.0),
            ]
        );
        head.close().unwrap();
    }

    #[test]
    fn truncate_chunks_head_matches_go_cut_and_unlink_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 5,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let pairs = [("__name__", "cut_lifecycle_metric"), ("job", "test")];

        let mut r = None;
        for i in 0..12i64 {
            let mut app = head.appender();
            let id = app.append_float(r, &pairs, 1_000 + i * 100, i as f64).unwrap();
            r = Some(id);
            app.commit().unwrap();
        }
        assert!(head.mmap_head_chunks().unwrap() >= 2);
        let chunks_dir = dir.path().join("chunks_head");
        let seg1 = crate::head_chunks::head_chunk_file_path(&chunks_dir, 1);
        let seg2 = crate::head_chunks::head_chunk_file_path(&chunks_dir, 2);
        assert!(seg1.exists());

        // First truncation keeps active 000001 (no eager 000002 created, matching Go).
        head.truncate_memory(10_000).unwrap();
        assert!(seg1.exists());
        assert!(!seg2.exists());

        // Next mmap write cuts 000002; subsequent truncation unlinks 000001.
        let mut r2 = None;
        for i in 0..12i64 {
            let mut app = head.appender();
            let id = app.append_float(r2, &pairs, 20_000 + i * 100, i as f64).unwrap();
            r2 = Some(id);
            app.commit().unwrap();
        }
        assert!(head.mmap_head_chunks().unwrap() >= 2);
        assert!(seg2.exists());
        head.truncate_memory(30_000).unwrap();
        assert!(!seg1.exists(), "000001 should be unlinked once 000002 is active");
        assert!(seg2.exists());

        head.close().unwrap();
        let reopened = Head::open(dir.path(), opts, i64::MIN).unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn symbol_gc_triggers_on_arena_growth_from_metadata_churn_without_series_deletion() {
        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        head.set_symbol_gc_arena_growth_threshold_for_test(512 * 1024);

        let pairs = [("__name__", "meta_churn_metric"), ("job", "api")];
        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 1_000, 1.0).unwrap();
            app.commit().unwrap();
        }

        // Churn 1 MiB of metadata help strings on the single live series (0 deleted series!).
        for i in 0..256 {
            let help = format!("help-{i:04}-{}", "x".repeat(4096));
            let mut app = head.appender();
            app.update_metadata(
                None,
                &pairs,
                "meta_churn_metric",
                MetricType::Gauge,
                "bytes",
                &help,
            )
            .unwrap();
            app.commit().unwrap();
        }

        let (syms_before, bytes_before) = head.symbol_table_stats();
        assert!(bytes_before >= 1_000_000, "bytes_before={bytes_before}");
        assert_eq!(head.num_series(), 1);

        // Even though mint <= head.min_time() and deleted_series == 0, truncate_memory
        // must notice arena growth >= threshold and reclaim the 255 dead help strings.
        head.truncate_memory(500).unwrap();

        let (syms_after, bytes_after) = head.symbol_table_stats();
        assert!(
            syms_after < syms_before / 2,
            "expected symbol count shrink: {syms_before} -> {syms_after}"
        );
        assert!(
            bytes_after < 16_384,
            "expected symbol arena bytes shrink: {bytes_before} -> {bytes_after}"
        );
        head.close().unwrap();
    }

    #[test]
    fn p0_1_ooo_write_chunk_enospc_preserves_ooo_head_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 100_000,
            ooo_cap_max: 4,
            head_chunks_write_buffer_size: 16,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "ooo_enospc_metric"), ("job", "api")];

        // Establish in-order high-water mark so subsequent earlier timestamps go to OOO.
        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 100_000, 999.0).unwrap();
            app.commit().unwrap();
        }
        // Fill OOO head chunk to ooo_cap_max (4 samples).
        {
            let mut app = head.appender();
            for i in 1..=4i64 {
                app.append_float(None, &pairs, i * 1_000, i as f64).unwrap();
            }
            app.commit().unwrap();
        }

        let entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
        // Inject ENOSPC on HeadChunksWriter so cutting the 4-sample OOO head chunk fails.
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|_len| {
                crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
            })));
        }
        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 5_000, 5.0).unwrap();
            assert!(app.commit().is_err(), "commit must fail on OOO write_chunk ENOSPC");
        }
        // Clear fault hook and verify all 4 prior OOO samples are still preserved in ooo_head_chunk!
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(None);
        }
        {
            let st = entry.inner.lock();
            let ooo = st.ooo().expect("ooo state must still exist");
            assert_eq!(
                ooo.ooo_head_chunk.as_ref().map(|c| c.chunk.num_samples()),
                Some(4),
                "ooo_head_chunk must not be dropped when write_chunk fails with ENOSPC"
            );
        }
        // Retry 5th sample after disk space recovers: all 5 OOO samples + 1 in-order must read back.
        {
            let mut app = head.appender();
            app.append_float(None, &pairs, 5_000, 5.0).unwrap();
            app.commit().unwrap();
        }
        let (_, chks) = head
            .series_final_chunks(entry.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        let mut got: Vec<(i64, f64)> = chks
            .into_iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        got.sort_by_key(|x| x.0);
        assert_eq!(
            got,
            vec![
                (1_000, 1.0),
                (2_000, 2.0),
                (3_000, 3.0),
                (4_000, 4.0),
                (5_000, 5.0),
                (100_000, 999.0),
            ]
        );
        head.close().unwrap();
    }

    #[test]
    fn p0_2_closed_head_returns_closed_error_instead_of_skipping_mmapped_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 4,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "closed_read_metric"), ("job", "api")];
        let mut r = None;
        for i in 0..10i64 {
            let mut app = head.appender();
            let id = app.append_float(r, &pairs, 1_000 + i * 100, i as f64).unwrap();
            r = Some(id);
            app.commit().unwrap();
        }
        let ref_id = r.unwrap();
        assert!(head.mmap_head_chunks().unwrap() >= 2);

        head.close().unwrap();
        let err = head.series_chunks(ref_id, i64::MIN, i64::MAX).unwrap_err();
        assert!(matches!(err, HeadError::HeadChunks(HeadChunksError::Closed)), "got {err:?}");
        let err2 = head.series_lazy_chunk_count(ref_id, i64::MIN, i64::MAX, None).unwrap_err();
        assert!(matches!(err2, HeadError::HeadChunks(HeadChunksError::Closed)), "got {err2:?}");
    }

    #[test]
    fn p1_3_mmap_head_chunks_flush_enospc_leaves_extra_chunks_intact_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 4,
            head_chunks_write_buffer_size: 1024 * 1024,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "mmap_retry_metric"), ("job", "api")];
        let mut r = None;
        for i in 0..10i64 {
            let mut app = head.appender();
            let id = app.append_float(r, &pairs, 1_000 + i * 100, i as f64).unwrap();
            r = Some(id);
            app.commit().unwrap();
        }
        let ref_id = r.unwrap();

        // Allow cut_new_file header write (8 bytes), fail flush_write_buf (> 8 bytes) with ENOSPC.
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|len| {
                if len > crate::head_chunks::HEAD_CHUNK_FILE_HEADER_SIZE {
                    crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
                } else {
                    crate::head_chunks::WriteFaultAction::PassThrough
                }
            })));
        }
        assert!(head.mmap_head_chunks().is_err());

        // Sealed extra_head_chunks must remain intact in RAM, and write_buf must be discarded.
        {
            let entry = head.series.get_by_id(ref_id).unwrap();
            let st = entry.inner.lock();
            assert!(st.mmapped().is_empty());
            assert!(st.head_chunks_len() >= 2);
        }

        // Clear fault hook and retry mmap_head_chunks: must write each sealed chunk exactly once.
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(None);
        }
        let mmapped_count = head.mmap_head_chunks().unwrap();
        assert!(mmapped_count >= 2);

        let chunks_dir = dir.path().join("chunks_head");
        let reader = crate::head_chunks::HeadChunksReader::open(&chunks_dir).unwrap();
        let mut on_disk = 0usize;
        reader.iterate_all_chunks(|_| { on_disk += 1; Ok(()) }).unwrap();
        assert_eq!(on_disk, mmapped_count, "no duplicate chunks may be written on retry");
        head.close().unwrap();
    }

    #[test]
    fn w13_p1_2_and_p1_3_extreme_timestamps_jitter_and_cut_order() {
        assert_eq!(range_for_timestamp(i64::MAX, 7_200_000), i64::MAX);
        assert_eq!(range_for_timestamp(i64::MAX - 1, 7_200_000), i64::MAX);
        assert_eq!(
            compute_chunk_end_time(0, i64::MAX, i64::MAX, 4.0),
            i64::MAX
        );
        assert_eq!(
            compute_chunk_end_time(i64::MIN + 2, 0, i64::MAX, 4.0),
            i64::MAX
        );

        let dir = tempfile::tempdir().unwrap();
        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        {
            let mut app = head.appender();
            let p = [("__name__", "extreme_span")];
            app.append_float(None, &p, i64::MIN + 1000, 1.0).unwrap();
            app.append_float(None, &p, 1_700_000_000_000, 2.0).unwrap();
            app.commit().unwrap();
        }
        assert!(head.compactable());
        head.close().unwrap();

        let dir2 = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 100_000,
            samples_per_chunk: 8,
            ..HeadOptions::default()
        };
        let head2 = Head::open(dir2.path(), opts, i64::MIN).unwrap();
        let p = [("__name__", "cut_order")];
        {
            let mut app = head2.appender();
            app.append_float(None, &p, 0, 1.0).unwrap();
            app.append_float(None, &p, 5_000, 2.0).unwrap();
            app.append_float(None, &p, 26_000, 3.0).unwrap();
            app.commit().unwrap();
        }
        let entry = head2.get_entry_by_sorted_pairs(&p).unwrap();
        let (_, chks) = head2
            .series_final_chunks(entry.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(chks.len(), 2, "must cut chunk on sample #2 after recomputing next_at");
        assert_eq!(chks[0].max_time, 5_000);
        assert_eq!(chks[1].min_time, 26_000);
        head2.close().unwrap();

        let lbls = Labels::from_pairs([("__name__", "jitter_metric"), ("job", "api")]);
        let h = lbls.stable_hash();
        let j = add_jitter_to_chunk_end_time(h, 0, 20_000, 100_000, 0.2);
        assert!((18_000..=22_000).contains(&j));
        assert_eq!(add_jitter_to_chunk_end_time(h, 0, 20_000, 100_000, 0.0), 20_000);
        assert_eq!(add_jitter_to_chunk_end_time(h, 0, 100_000, 100_000, 0.2), 100_000);
    }

    #[test]
    fn mmapped_chunks_stepped_capacity_and_cold_state_drop_on_truncate() {
        let mut mv: Vec<MmappedChunk> = Vec::new();
        let mut caps = Vec::new();
        for i in 0..12u64 {
            push_mmapped_chunk(
                &mut mv,
                MmappedChunk {
                    ref_id: ChunkDiskMapperRef(i + 1),
                    min_time: (i as i64) * 100,
                    max_time: (i as i64) * 100 + 50,
                    num_samples: 10,
                    encoding: Encoding::Xor,
                },
            );
            if caps.last() != Some(&mv.capacity()) {
                caps.push(mv.capacity());
            }
        }
        assert_eq!(caps, vec![2, 4, 6, 8, 12]);

        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 100_000,
            ooo_cap_max: 4,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let p = [("__name__", "cold_drop_metric"), ("job", "api")];
        {
            let mut app = head.appender();
            app.append_float(None, &p, 10_000, 1.0).unwrap();
            app.append_float(None, &p, 20_000, 2.0).unwrap();
            // OOO sample creates SeriesColdState -> SeriesOooState.
            app.append_float(None, &p, 15_000, 1.5).unwrap();
            app.commit().unwrap();
        }
        let entry = head.get_entry_by_sorted_pairs(&p).unwrap();
        assert!(entry.inner.lock().cold.is_some());

        // Clear OOO data before 16_000: OOO state becomes empty -> SeriesColdState dropped to None.
        head.clear_ooo_data_before(16_000).unwrap();
        {
            let st = entry.inner.lock();
            assert!(st.ooo().is_none());
            assert!(st.cold.is_none());
        }

        // Report struct sizes for visibility.
        eprintln!(
            "sizes: SeriesEntry={} SeriesState={} SeriesColdState={} MmappedChunk={}",
            std::mem::size_of::<SeriesEntry>(),
            std::mem::size_of::<SeriesState>(),
            std::mem::size_of::<SeriesColdState>(),
            std::mem::size_of::<MmappedChunk>(),
        );
        head.close().unwrap();
    }

    #[test]
    fn active_chunk_num_samples_masks_histogram_st_and_checks_xor2_min_hdr() {
        use crate::chunkenc::{CounterResetHint, HistogramSpan};

        // Verify direct SeriesState header masking on HistogramSt / FloatHistogramSt
        // where top 2 bits store CounterResetHeader (0x40=NotCounterReset, 0x80=CounterReset, 0xC0=GaugeType).
        let mut st = SeriesState::new();
        for (enc, hdr0) in [
            (Encoding::HistogramSt, 0x40u8),
            (Encoding::HistogramSt, 0x80u8),
            (Encoding::HistogramSt, 0xC0u8),
            (Encoding::FloatHistogramSt, 0x40u8),
            (Encoding::FloatHistogramSt, 0x80u8),
            (Encoding::FloatHistogramSt, 0xC0u8),
        ] {
            // Raw u16 would be 16_385 / 32_769 / 49_153 without HISTOGRAM_ST_SAMPLE_COUNT_MASK.
            st.set_active_chunk(EncodedChunk::from_bstream(
                enc,
                BitStream::from_bytes(vec![hdr0, 0x01, 0x00], 0),
            ));
            assert_eq!(
                st.active_chunk_num_samples(),
                1,
                "failed for {enc:?} with header byte {hdr0:#x}"
            );
        }

        // Verify min_hdr = 3 is enforced for Xor2 (2-byte truncated header -> 0, 3-byte -> count).
        st.set_active_chunk(EncodedChunk::from_bstream(
            Encoding::Xor2,
            BitStream::from_bytes(vec![0x00, 0x01], 0),
        ));
        assert_eq!(st.active_chunk_num_samples(), 0);
        st.set_active_chunk(EncodedChunk::from_bstream(
            Encoding::Xor2,
            BitStream::from_bytes(vec![0x00, 0x01, 0x00], 0),
        ));
        assert_eq!(st.active_chunk_num_samples(), 1);

        // End-to-end Head test with enable_histogram_st_encoding = true.
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            enable_histogram_st_encoding: true,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let hp = [("__name__", "hist_st_active_samples")];
        let fhp = [("__name__", "fhist_st_active_samples")];

        let int_h = Histogram {
            count: 5,
            sum: 7.5,
            schema: 1,
            counter_reset_hint: CounterResetHint::NotCounterReset,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![5],
            ..Histogram::default()
        };

        let float_h = FloatHistogram {
            count: 5.0,
            sum: 7.5,
            schema: 1,
            counter_reset_hint: CounterResetHint::CounterReset,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![5.0],
            ..FloatHistogram::default()
        };

        {
            let mut app = head.appender();
            app.append_histogram(None, &hp, 0, 10_000, int_h).unwrap();
            app.append_float_histogram(None, &fhp, 0, 10_000, float_h)
                .unwrap();
            app.commit().unwrap();
        }

        let he = head.get_entry_by_sorted_pairs(&hp).unwrap();
        assert_eq!(he.inner.lock().active_chunk_num_samples(), 1);

        let fhe = head.get_entry_by_sorted_pairs(&fhp).unwrap();
        assert_eq!(fhe.inner.lock().active_chunk_num_samples(), 1);
        assert_eq!(head.head_stats().2, 2);

        head.close().unwrap();
    }

    #[test]
    fn evict_and_ooo_truncate_wait_for_active_head_read_guard() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 2 * 3_600_000,
            ..HeadOptions::default()
        };
        let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());
        let ref_id = {
            let mut app = head.appender();
            let r = app.append_float(None, &[("__name__", "p11_m")], 10_000, 1.0).unwrap();
            app.append_float(Some(r), &[("__name__", "p11_m")], 5_000, 0.5).unwrap();
            app.commit().unwrap();
            r
        };

        // 1. clear_ooo_data_before waits on active HeadReadGuard overlapping OOO range.
        let guard_ooo = head.start_read(4_000, 6_000);
        let h1 = Arc::clone(&head);
        let cleared = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let c1 = Arc::clone(&cleared);
        let t1 = std::thread::spawn(move || {
            h1.clear_ooo_data_before(6_000).unwrap();
            c1.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(!cleared.load(Ordering::SeqCst), "clear_ooo_data_before must wait on HeadReadGuard");
        drop(guard_ooo);
        t1.join().unwrap();
        assert!(cleared.load(Ordering::SeqCst));

        // 2. evict_series_refs_with_fingerprints waits on active HeadReadGuard overlapping [min_time, maxt_inclusive].
        let guard_in_order = head.start_read(9_000, 11_000);
        let h2 = Arc::clone(&head);
        let evicted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let c2 = Arc::clone(&evicted);
        let fps = HashMap::new();
        let t2 = std::thread::spawn(move || {
            h2.evict_series_refs_with_fingerprints(&[ref_id], 10_000, &fps).unwrap();
            c2.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(!evicted.load(Ordering::SeqCst), "evict_series_refs_inner must wait on HeadReadGuard");
        drop(guard_in_order);
        t2.join().unwrap();
        assert!(evicted.load(Ordering::SeqCst));

        head.close().unwrap();
    }

    #[test]
    fn tsdb_01_parallel_wal_replay_flushes_job_shards_before_duplicate_attach() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal_dir = dir.path().join("wal");
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let enc = RecordEncoder::new(false);
            let lbls = Labels::from_sorted_pairs([("__name__", "tsdb01_m"), ("job", "a")]);
            let r_s1 = enc.series(&[RefSeries { ref_id: 10, labels: lbls.clone() }], Vec::new());
            let samps1: Vec<RefSample> = (0..10i64)
                .map(|i| RefSample {
                    ref_id: 10,
                    st: 0,
                    t: 1_000 + i * 100,
                    v: i as f64,
                })
                .collect();
            let r_samp1 = enc.samples(&samps1, Vec::new());
            let r_s2 = enc.series(&[RefSeries { ref_id: 99, labels: lbls }], Vec::new());
            let samps2: Vec<RefSample> = (0..5i64)
                .map(|i| RefSample {
                    ref_id: 99,
                    st: 0,
                    t: 5_000 + i * 100,
                    v: (100 + i) as f64,
                })
                .collect();
            let r_samp2 = enc.samples(&samps2, Vec::new());
            w.log(&[&r_s1, &r_samp1, &r_s2, &r_samp2]).unwrap();
            w.close().unwrap();
        }

        let seq_head = Head::open(
            dir.path(),
            HeadOptions {
                wal_replay_concurrency: 1,
                ..HeadOptions::default()
            },
            i64::MIN,
        )
        .unwrap();
        let par_head = Head::open(
            dir.path(),
            HeadOptions {
                wal_replay_concurrency: 4,
                ..HeadOptions::default()
            },
            i64::MIN,
        )
        .unwrap();

        let extract = |h: &Head| -> Vec<(i64, f64)> {
            let e = h
                .get_entry_by_sorted_pairs(&[("__name__", "tsdb01_m"), ("job", "a")])
                .unwrap();
            let (_, chks, _) = h.series_chunks(e.ref_id, i64::MIN, i64::MAX).unwrap().unwrap();
            let mut out = Vec::new();
            for c in chks {
                for s in decode_chunk_samples(&c.chunk.unwrap()).unwrap() {
                    if let SampleValue::Float(v) = s.value {
                        out.push((s.t, v));
                    }
                }
            }
            out
        };
        let seq_s = extract(&seq_head);
        let par_s = extract(&par_head);
        assert_eq!(seq_s.len(), 5);
        assert_eq!(par_s, seq_s);
        // Go Head.Init calls h.gc() without raising h.minTime to actualInOrderMint;
        // both sequential and parallel replay match Go (min_time == 1_000).
        assert_eq!(par_head.min_time(), seq_head.min_time());
        assert_eq!(par_head.min_time(), 1_000);
        seq_head.close().unwrap();
        par_head.close().unwrap();
    }

    #[test]
    fn tsdb_02_head_appender_allows_intra_batch_conflicts_and_drops_at_commit() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 10_000,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pf = [("__name__", "tsdb02_f"), ("job", "api")];
        let ph = [("__name__", "tsdb02_h"), ("job", "api")];
        let pfh = [("__name__", "tsdb02_fh"), ("job", "api")];

        let make_hist = |sum: f64| Histogram {
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1,
            count: 1,
            sum,
            ..Histogram::default()
        };
        let make_fhist = |sum: f64| FloatHistogram {
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1.0,
            count: 1.0,
            sum,
            ..FloatHistogram::default()
        };

        let mut app = head.appender();
        let rf = app.append_float(None, &pf, 1_020_000, 5.0).unwrap();
        app.append_float(Some(rf), &pf, 1_020_000, 9.0).unwrap();
        app.append_float(Some(rf), &pf, 1_005_000, 2.5).unwrap();
        let rh = app.append_histogram(None, &ph, 0, 1_020_000, make_hist(1.0)).unwrap();
        app.append_histogram(Some(rh), &ph, 0, 1_020_000, make_hist(2.0)).unwrap();
        let rfh = app.append_float_histogram(None, &pfh, 0, 1_020_000, make_fhist(3.0)).unwrap();
        app.append_float_histogram(Some(rfh), &pfh, 0, 1_020_000, make_fhist(4.0)).unwrap();
        app.commit().unwrap();

        let (_, chks, _) = head.series_chunks(rf, i64::MIN, i64::MAX).unwrap().unwrap();
        let mut got = Vec::new();
        for c in chks {
            for s in decode_chunk_samples(&c.chunk.unwrap()).unwrap() {
                got.push((s.t, s.value));
            }
        }
        assert_eq!(got, vec![(1_020_000, SampleValue::Float(5.0))]);
        head.close().unwrap();
    }

    #[test]
    fn tsdb_03_postings_for_label_value_safe_under_concurrent_symbol_gc() {
        let dir = tempfile::tempdir().unwrap();
        let head = Arc::new(Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap());

        for batch in 0..5i64 {
            let mut app = head.appender();
            app.append_float(
                None,
                &[("__name__", "keep_metric"), ("env", "prod")],
                1_000 + batch * 10,
                batch as f64,
            )
            .unwrap();
            for i in 0..200usize {
                let k = format!("churn_{batch}_{i}");
                app.append_float(None, &[("__name__", k.as_str()), ("env", "dev")], 1_000, 1.0)
                    .unwrap();
            }
            app.commit().unwrap();
        }

        let stop = Arc::new(AtomicBool::new(false));
        let h1 = Arc::clone(&head);
        let s1 = Arc::clone(&stop);
        let reader = std::thread::spawn(move || {
            while !s1.load(Ordering::Relaxed) {
                let p = h1.postings_for_label_value("__name__", "keep_metric");
                assert_eq!(p.len(), 1);
                let cnt = h1.visit_postings_for_label_value("__name__", "keep_metric", |sm, lg| {
                    sm.len() + lg.len()
                });
                assert_eq!(cnt, 1);
            }
        });

        for _ in 0..20 {
            head.maybe_gc_symbols();
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().unwrap();
        head.close().unwrap();
    }

    #[test]
    fn tsdb_04_parallel_wal_replay_intern_metadata_error_does_not_deadlock() {
        let dir = tempfile::tempdir().unwrap();
        {
            let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
            let mut app = head.appender();
            let pairs = [("__name__", "tsdb04_meta"), ("job", "a")];
            app.append_float(None, &pairs, 1_000, 1.0).unwrap();
            app.update_metadata(None, &pairs, "tsdb04_meta", MetricType::Counter, "bytes", "help text")
                .unwrap();
            app.commit().unwrap();
            head.close().unwrap();
        }

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let dir_path = dir.path().to_path_buf();
        std::thread::spawn(move || {
            let mut opts = HeadOptions {
                wal_replay_concurrency: 2,
                ..HeadOptions::default()
            };
            let head = Head {
                dir: dir_path.clone(),
                chunks_dir: dir_path.join("chunks_head"),
                read_only: false,
                disable_wal: false,
                enable_metadata_wal_records: AtomicBool::new(true),
                opts: opts.clone(),
                metrics: HeadMetrics::new_with_dirs(None, 0, None),
                symbols: RwLock::new(SymbolTable::new()),
                series: StripeSeries::new(opts.stripe_size),
                series_creation_mtx: Mutex::new(()),
                postings: MemPostings::new_unordered(),
                postings_cache: opts
                    .shared_postings_cache
                    .clone()
                    .unwrap_or_else(|| Arc::new(PostingsForMatchersCache::new(opts.postings_cache.clone()))),
                postings_cache_source_id: next_postings_cache_source_id(),
                tombstones: MemTombstones::new(),
                exemplars: CircularExemplarStorage::new_with_registry(0, 0, None),
                metadata: UserMetricsMetadata::new(),
                wal: Mutex::new(None),
                wbl: Mutex::new(None),
                chunk_writer: Mutex::new(None),
                chunk_reader: RwLock::new(None),
                mmap_mtx: Mutex::new(()),
                chunk_snapshot_mtx: Mutex::new(()),
                ooo_wbl_gate: RwLock::new(()),
                ooo_compact_wbl_cut_seg: Mutex::new(None),
                min_time: Arc::new(AtomicI64::new(i64::MAX)),
                max_time: Arc::new(AtomicI64::new(i64::MIN)),
                min_valid_time: AtomicI64::new(i64::MIN),
                min_ooo_time: AtomicI64::new(i64::MAX),
                max_ooo_time: AtomicI64::new(i64::MIN),
                ooo_time_window_ms: AtomicI64::new(0),
                chunk_end_time_variance_bits: AtomicU64::new(0),
                metadata_limits_max_metrics: AtomicUsize::new(0),
                metadata_limits_max_per_metric: AtomicUsize::new(0),
                float_chunk_encoding: AtomicU8::new(opts.float_chunk_encoding.as_u8()),
                last_series_id: AtomicU64::new(0),
                num_series: AtomicU64::new(0),
                num_stale_series: Arc::new(AtomicU64::new(0)),
                num_native_histogram_series: Arc::new(AtomicU64::new(0)),
                num_native_histogram_buckets: Arc::new(AtomicU64::new(0)),
                last_wal_truncation_time: AtomicI64::new(i64::MIN),
                last_memory_truncation_time: AtomicI64::new(i64::MIN),
                mem_truncation_in_process: AtomicBool::new(false),
                has_evicting_series: AtomicBool::new(false),
                evicting_series: RwLock::new(None),
                closed: AtomicBool::new(false),
                has_any_metadata: AtomicBool::new(false),
                enable_st_as_zero_sample: AtomicBool::new(false),
                enable_memory_snapshot_on_shutdown: AtomicBool::new(false),
                deleted_series_since_symbol_gc: AtomicU64::new(0),
                symbol_arena_baseline_bytes: AtomicU64::new(0),
                symbol_gc_arena_growth_threshold: AtomicU64::new(DEFAULT_SYMBOL_GC_ARENA_GROWTH_BYTES),
                wal_expiries: Mutex::new(HashMap::new()),
                wbl_pinned_refs: Mutex::new(HashSet::new()),
                ooo_unflushed_series: Mutex::new(HashSet::new()),
                has_ooo_unflushed: AtomicBool::new(false),
                appender_pool: Mutex::new(Vec::new()),
                lifecycle_callback: RwLock::new(None),
                after_waiting_for_readers_hook: Mutex::new(None),
                before_deleting_postings_hook: Mutex::new(None),
                after_series_lookup_hook: Mutex::new(None),
                has_after_series_lookup_hook: AtomicBool::new(false),
                test_commit_pause: Mutex::new(HashMap::new()),
                has_test_commit_pause: AtomicBool::new(false),
                series_txs: (0..64).map(|_| Mutex::new(FastU64Map::default())).collect(),
                iso: Isolation::new(false),
                active_reads: Arc::new(HeadActiveReads::default()),
                series_gauge: None,
                series_created_total: None,
                series_removed_total: None,
            };
            {
                let mut syms = head.symbols.write();
                syms.intern("__name__");
                syms.intern("tsdb04_meta");
                syms.intern("job");
                syms.intern("a");
                syms.fail_next_intern_for_test();
            }
            opts.wal_replay_concurrency = 2;
            let mut r = WlogReader::open_wal_with_checkpoint(dir_path.join("wal")).unwrap();
            let mut multi_ref = HashMap::new();
            let mut mm = HashMap::new();
            let mut ooo_mm = HashMap::new();
            let unknown = UnknownRefs::default();
            let res = head.replay_wal_reader_parallel(
                &mut r,
                &mut multi_ref,
                &mut mm,
                &mut ooo_mm,
                ChunkDiskMapperRef(0),
                &unknown,
            );
            let _ = done_tx.send(res);
        });

        let res = done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("replay_wal_reader_parallel deadlocked on intern_metadata_symbols error");
        assert!(matches!(
            res,
            Err(HeadError::SymbolTable(SymbolTableError::TooManySymbols))
        ));
    }

    #[test]
    fn tsdb_07_commit_series_wal_fallback_and_commit_batch_atomic() {
        use std::sync::atomic::AtomicUsize;

        let dir1 = tempfile::tempdir().unwrap();
        {
            let head = Head::open(dir1.path(), HeadOptions::default(), i64::MIN).unwrap();
            let call_n = Arc::new(AtomicUsize::new(0));
            let c_clone = Arc::clone(&call_n);
            {
                let mut wal = head.wal.lock();
                wal.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(move |_| {
                    if c_clone.fetch_add(1, Ordering::SeqCst) == 0 {
                        crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::Other)
                    } else {
                        crate::head_chunks::WriteFaultAction::PassThrough
                    }
                })));
            }

            let mut app = head.appender();
            let pairs = [("__name__", "tsdb07_fallback"), ("job", "api")];
            let r = app.append_float(None, &pairs, 1_000, 42.0).unwrap();
            assert!(app.commit().is_err());
            assert!(head.series_chunks(r, i64::MIN, i64::MAX).unwrap().is_none());

            {
                let mut wal = head.wal.lock();
                wal.as_mut().unwrap().set_write_fault_hook(None);
            }
            let mut app2 = head.appender();
            let r2 = app2.append_float(None, &pairs, 2_000, 99.0).unwrap();
            assert_eq!(r2, r);
            app2.commit().unwrap();
            head.close().unwrap();
        }
        let reopened = Head::open(dir1.path(), HeadOptions::default(), i64::MIN).unwrap();
        let entry = reopened
            .get_entry_by_sorted_pairs(&[("__name__", "tsdb07_fallback"), ("job", "api")])
            .expect("series_refs fallback must have persisted series declaration");
        let (_, chks, _) = reopened
            .series_chunks(entry.ref_id, i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(chks.len(), 1);
        reopened.close().unwrap();

        let dir2 = tempfile::tempdir().unwrap();
        let head2 = Head::open(dir2.path(), HeadOptions::default(), i64::MIN).unwrap();
        {
            let mut wal = head2.wal.lock();
            wal.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|_| {
                crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::Other)
            })));
        }
        let mut apps = Vec::new();
        for s in 0..8usize {
            let mut app = head2.appender();
            let j = format!("s{s}");
            app.append_float(None, &[("__name__", "batch_m"), ("shard", j.as_str())], 1_000, s as f64)
                .unwrap();
            apps.push(app);
        }
        assert!(HeadAppender::commit_batch(apps).is_err());
        for s in 0..8usize {
            let j = format!("s{s}");
            let e = head2
                .get_entry_by_sorted_pairs(&[("__name__", "batch_m"), ("shard", j.as_str())])
                .unwrap();
            assert_eq!(e.pending_commits_count(), 0);
            assert!(head2.series_chunks(e.ref_id, i64::MIN, i64::MAX).unwrap().is_none());
        }

        {
            let mut wal = head2.wal.lock();
            wal.as_mut().unwrap().set_write_fault_hook(None);
        }
        let mut apps2 = Vec::new();
        for s in 0..8usize {
            let mut app = head2.appender();
            let j = format!("s{s}");
            app.append_float(None, &[("__name__", "batch_m"), ("shard", j.as_str())], 2_000, (s + 10) as f64)
                .unwrap();
            apps2.push(app);
        }
        HeadAppender::commit_batch(apps2).unwrap();
        for s in 0..8usize {
            let j = format!("s{s}");
            let e = head2
                .get_entry_by_sorted_pairs(&[("__name__", "batch_m"), ("shard", j.as_str())])
                .unwrap();
            assert_eq!(e.pending_commits_count(), 0);
            assert!(head2.series_chunks(e.ref_id, i64::MIN, i64::MAX).unwrap().is_some());
        }
        head2.close().unwrap();
    }

    #[test]
    fn sq_02_head_read_paths_clamp_ino_mint_and_preserve_ooo() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 2,
            ooo_time_window_ms: 100_000,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "sq02_m"), ("job", "api")];

        let mut app = head.appender();
        let r = app.append_float(None, &pairs, 1_000, 1.0).unwrap();
        app.append_float(Some(r), &pairs, 2_000, 2.0).unwrap();
        app.append_float(Some(r), &pairs, 10_000, 10.0).unwrap();
        app.append_float(Some(r), &pairs, 11_000, 11.0).unwrap();
        app.append_float(Some(r), &pairs, 500, 0.5).unwrap();
        app.commit().unwrap();
        head.mmap_head_chunks().unwrap();

        head.min_time.store(5_000, Ordering::Release);

        assert!(head.series_has_chunks_in_range(r, 0, 4_000).is_some());
        let (_, chks, _) = head.series_chunks(r, 0, 4_000).unwrap().unwrap();
        assert_eq!(chks.len(), 1);
        assert_eq!((chks[0].min_time, chks[0].max_time), (500, 500));
        let (_, cnt, _) = head.series_lazy_chunk_count_with_snapshot(r, 0, 4_000, None).unwrap().unwrap();
        assert_eq!(cnt, 1);
        let (_, ivs, _) = head.series_chunk_metas_no_bytes_with_snapshot(r, 0, 4_000, None).unwrap().unwrap();
        assert_eq!(ivs.as_slice(), &[(500, 500)]);

        assert!(head.series_has_chunks_in_range(r, 1_000, 4_000).is_none());
        assert!(head.series_chunks(r, 1_000, 4_000).unwrap().is_none());
        assert!(head.series_lazy_chunk_count_with_snapshot(r, 1_000, 4_000, None).unwrap().is_none());
        assert!(head.series_chunk_metas_no_bytes_with_snapshot(r, 1_000, 4_000, None).unwrap().is_none());

        head.close().unwrap();
    }

    #[test]
    fn w22_mid_scan_discard_epoch_bump_fails_ooo_compaction_and_preserves_wbl() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 100_000,
            ooo_cap_max: 8,
            samples_per_chunk: 4,
            ..HeadOptions::default()
        };
        let head = Arc::new(Head::open(dir.path(), opts.clone(), i64::MIN).unwrap());

        // Pick 3 series whose by_hash stripe indices (used by collect_stripe_into) are strictly ascending.
        let mut created: Vec<(String, Arc<SeriesEntry>, usize)> = Vec::new();
        for idx in 0..32usize {
            let name = format!("w22_mid_scan_s_{idx}");
            let pairs = [("__name__", name.as_str()), ("job", "api")];
            let mut app = head.appender();
            app.append_float(None, &pairs, 100_000, 10.0 + idx as f64)
                .unwrap();
            app.commit().unwrap();
            let entry = head.get_entry_by_sorted_pairs(&pairs).unwrap();
            let sidx = head.series.stripe_idx(entry.hash);
            created.push((name, entry, sidx));
        }
        created.sort_by_key(|x| x.2);
        let (name_a, e_a, sidx_a) = created[0].clone();
        let (_name_mid, e_mid, sidx_mid) = created
            .iter()
            .find(|x| x.2 > sidx_a)
            .unwrap()
            .clone();
        let (_name_b, e_b, sidx_b) = created
            .iter()
            .find(|x| x.2 > sidx_mid)
            .unwrap()
            .clone();
        assert!(sidx_a < sidx_mid && sidx_mid < sidx_b);
        let name_b = _name_b;

        {
            let mut app = head.appender();
            app.append_float(
                None,
                &[("__name__", name_a.as_str()), ("job", "api")],
                1_000,
                1.0,
            )
            .unwrap();
            app.append_float(
                None,
                &[("__name__", name_a.as_str()), ("job", "api")],
                2_000,
                2.0,
            )
            .unwrap();
            app.append_float(
                None,
                &[("__name__", name_b.as_str()), ("job", "api")],
                3_000,
                3.0,
            )
            .unwrap();
            app.append_float(
                None,
                &[("__name__", name_b.as_str()), ("job", "api")],
                4_000,
                4.0,
            )
            .unwrap();
            app.commit().unwrap();
        }

        let wbl_dir = dir.path().join("wbl");
        let (wbl_first_before, wbl_last_before) =
            crate::wlog::segments_range(&wbl_dir).unwrap().unwrap();

        let mid_guard = e_mid.inner.lock();
        let head_bg = Arc::clone(&head);
        let handle = std::thread::spawn(move || {
            head_bg.collect_ooo_compaction_candidates_with_watermark()
        });

        while !head.ooo_unflushed_series.lock().contains(&e_a.ref_id) {
            std::thread::yield_now();
        }

        {
            let mut cw_guard = head.chunk_writer.lock();
            let cw = cw_guard.as_mut().unwrap();
            cw.set_write_fault_hook(Some(Arc::new(|len| {
                if len > 8 {
                    crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
                } else {
                    crate::head_chunks::WriteFaultAction::PassThrough
                }
            })));
            assert!(cw.flush().is_err());
            cw.set_write_fault_hook(None);
        }

        drop(mid_guard);

        let res = handle.join().unwrap();
        assert!(
            res.is_err(),
            "collect_ooo_compaction_candidates_with_watermark must fail when discard_epoch bumps mid-scan"
        );

        let (wbl_first_after, wbl_last_after) =
            crate::wlog::segments_range(&wbl_dir).unwrap().unwrap();
        assert_eq!(wbl_first_after, wbl_first_before);
        assert!(wbl_last_after >= wbl_last_before);

        let snap = head
            .collect_ooo_compaction_candidates_with_watermark()
            .unwrap();
        assert_eq!(snap.candidates.len(), 2);

        let mut chks_a = Vec::new();
        let mut tombs_a = Vec::new();
        head.load_series_ooo_chunks_into_up_to(
            e_a.ref_id,
            snap.last_mmap_ref,
            &mut chks_a,
            &mut tombs_a,
        )
        .unwrap();
        let mut ts_a: Vec<(i64, f64)> = chks_a
            .into_iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        ts_a.sort_by_key(|x| x.0);
        assert_eq!(ts_a, vec![(1_000, 1.0), (2_000, 2.0)]);

        let mut chks_b = Vec::new();
        let mut tombs_b = Vec::new();
        head.load_series_ooo_chunks_into_up_to(
            e_b.ref_id,
            snap.last_mmap_ref,
            &mut chks_b,
            &mut tombs_b,
        )
        .unwrap();
        let mut ts_b: Vec<(i64, f64)> = chks_b
            .into_iter()
            .flat_map(|c| decode_chunk_samples(c.chunk.as_ref().unwrap()).unwrap())
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect();
        ts_b.sort_by_key(|x| x.0);
        assert_eq!(ts_b, vec![(3_000, 3.0), (4_000, 4.0)]);

        Arc::try_unwrap(head)
            .unwrap_or_else(|_| panic!("unique head arc"))
            .close()
            .unwrap();
    }

    #[test]
    fn w22_rewrite_discarded_ooo_pending_does_not_emit_retroactive_mmap_markers() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 500_000,
            ooo_cap_max: 4,
            samples_per_chunk: 4,
            ..HeadOptions::default()
        };
        let head = Arc::new(Head::open(dir.path(), opts.clone(), i64::MIN).unwrap());
        let s_a = [("__name__", "w22_no_retro_a"), ("job", "api")];
        let s_other = [("__name__", "w22_no_retro_other"), ("job", "api")];

        {
            let mut app = head.appender();
            app.append_float(None, &s_a, 100_000, 10.0).unwrap();
            app.append_float(None, &s_other, 100_000, 20.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float(None, &s_a, 1_000, 1.0).unwrap();
            app.append_float(None, &s_a, 2_000, 2.0).unwrap();
            app.commit().unwrap();
        }

        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|len| {
                if len > 8 {
                    crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
                } else {
                    crate::head_chunks::WriteFaultAction::PassThrough
                }
            })));
        }
        assert!(head
            .collect_ooo_compaction_candidates_with_watermark()
            .is_err());
        {
            let mut cw = head.chunk_writer.lock();
            cw.as_mut().unwrap().set_write_fault_hook(None);
        }

        let e_a = head.get_entry_by_sorted_pairs(&s_a).unwrap();
        {
            let st_a = e_a.inner.lock();
            assert!(st_a.ooo().unwrap().ooo_head_chunk.is_none());
            assert_eq!(st_a.ooo().unwrap().ooo_pending_mmapped.len(), 1);
        }

        {
            let mut app = head.appender();
            app.append_float(None, &s_other, 101_000, 21.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float(None, &s_a, 3_000, 3.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut wbl = head.wbl.lock();
            wbl.as_mut().unwrap().sync().unwrap();
        }

        let wbl_dir = dir.path().join("wbl");
        let (first_wbl, last_wbl) = crate::wlog::segments_range(&wbl_dir).unwrap().unwrap();
        let mut r = crate::wlog::WlogReader::open_ranges(&[crate::wlog::SegmentRange {
            dir: wbl_dir,
            first: Some(first_wbl),
            last: Some(last_wbl),
        }])
        .unwrap();
        let dec = RecordDecoder::new();
        while let Some(rec) = r.next_record().unwrap() {
            if let DecodedRecord::MmapMarkers(markers) = dec.decode_record(rec).unwrap() {
                assert!(
                    !markers.iter().any(|m| m.ref_id == e_a.ref_id && m.mmap_ref != 0),
                    "retroactive non-zero MmapMarkers must not be logged to WBL"
                );
            }
        }

        let mut conc_series = Vec::new();
        for idx in 0..8usize {
            let name = format!("w22_conc_retro_{idx}");
            let mut app = head.appender();
            app.append_float(None, &[("__name__", name.as_str()), ("job", "c")], 100_000, 99.0)
                .unwrap();
            app.commit().unwrap();
            conc_series.push(name);
        }

        for (idx, name) in conc_series.iter().enumerate() {
            let pairs = [("__name__", name.as_str()), ("job", "c")];
            {
                let mut app = head.appender();
                app.append_float(None, &pairs, 1_000, 10.0 + idx as f64)
                    .unwrap();
                app.append_float(None, &pairs, 2_000, 20.0 + idx as f64)
                    .unwrap();
                app.commit().unwrap();
            }
            {
                let mut cw = head.chunk_writer.lock();
                cw.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|len| {
                    if len > 8 {
                        crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
                    } else {
                        crate::head_chunks::WriteFaultAction::PassThrough
                    }
                })));
            }
            assert!(head
                .collect_ooo_compaction_candidates_with_watermark()
                .is_err());
            {
                let mut cw = head.chunk_writer.lock();
                cw.as_mut().unwrap().set_write_fault_hook(None);
            }

            let t1_ts = 102_000 + idx as i64 * 1_000;
            let s_new_val = 30.0 + idx as f64;
            std::thread::scope(|sc| {
                let h1 = &head;
                let h2 = &head;
                let t1 = sc.spawn(move || {
                    let mut app = h1.appender();
                    app.append_float(None, &s_other, t1_ts, 22.0).unwrap();
                    app.commit().unwrap();
                });
                let t2 = sc.spawn(move || {
                    let mut app = h2.appender();
                    app.append_float(None, &pairs, 3_000, s_new_val).unwrap();
                    app.commit().unwrap();
                });
                t1.join().unwrap();
                t2.join().unwrap();
            });
        }

        let crash_dir = tempfile::tempdir().unwrap();
        copy_dir_recursive(dir.path(), crash_dir.path());
        Arc::try_unwrap(head)
            .unwrap_or_else(|_| panic!("unique head arc"))
            .close()
            .unwrap();

        let reopened = Head::open(crash_dir.path(), opts, i64::MIN).unwrap();
        let ra = reopened.get_entry_by_sorted_pairs(&s_a).unwrap();
        assert_eq!(
            read_head_float_samples(&reopened, ra.ref_id),
            vec![(1_000, 1.0), (2_000, 2.0), (3_000, 3.0), (100_000, 10.0)]
        );
        for (idx, name) in conc_series.iter().enumerate() {
            let pairs = [("__name__", name.as_str()), ("job", "c")];
            let entry = reopened.get_entry_by_sorted_pairs(&pairs).unwrap();
            assert_eq!(
                read_head_float_samples(&reopened, entry.ref_id),
                vec![
                    (1_000, 10.0 + idx as f64),
                    (2_000, 20.0 + idx as f64),
                    (3_000, 30.0 + idx as f64),
                    (100_000, 99.0),
                ]
            );
        }
        reopened.close().unwrap();
    }

    #[test]
    fn head_open_and_wlog_checkpoint_replay_non_utf8_metadata_without_repair() {
        use crate::encoding::Encbuf;
        use crate::record::{MetricType, RecordEncoder, RecordType, RefSample, RefSeries};
        use crate::wlog::{create_checkpoint, last_checkpoint, segments_range, CompressionType, WlogReader, WlogWriter};

        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let enc = RecordEncoder::new(false);
        let mut w = WlogWriter::open(&wal_dir, CompressionType::Snappy).unwrap();

        // Segment 0: Series(ref=1), Metadata with raw non-UTF-8 unit/help bytes, Sample(ref=1).
        let s0 = enc.series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_pairs([("__name__", "seg0_metric"), ("job", "api")]),
            }],
            Vec::new(),
        );
        let mut meta_raw = Encbuf::new();
        meta_raw.put_byte(RecordType::Metadata as u8);
        meta_raw.put_uvarint64(1);
        meta_raw.put_byte(MetricType::Counter as u8);
        meta_raw.put_uvarint(2);
        meta_raw.put_uvarint_str("UNIT");
        meta_raw.put_uvarint_bytes(&[0xff, 0xfe]);
        meta_raw.put_uvarint_str("HELP");
        meta_raw.put_uvarint_bytes(b"bad\xffhelp");
        let samp0 = enc.samples(
            &[RefSample { ref_id: 1, st: 0, t: 1_000, v: 11.0 }],
            Vec::new(),
        );
        w.log(&[&s0, meta_raw.as_slice(), &samp0]).unwrap();
        w.next_segment().unwrap();

        // Segment 1: Series(ref=2) + Sample(ref=2) that would be wiped if repair_write_log ran.
        let s1 = enc.series(
            &[RefSeries {
                ref_id: 2,
                labels: Labels::from_pairs([("__name__", "seg1_metric"), ("job", "api")]),
            }],
            Vec::new(),
        );
        let samp1 = enc.samples(
            &[RefSample { ref_id: 2, st: 0, t: 2_000, v: 22.0 }],
            Vec::new(),
        );
        w.log(&[&s1, &samp1]).unwrap();
        w.close().unwrap();

        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        assert_eq!(head.num_series(), 2);
        let e1 = head
            .get_entry_by_sorted_pairs(&[("__name__", "seg0_metric"), ("job", "api")])
            .expect("seg0_metric must replay");
        let e2 = head
            .get_entry_by_sorted_pairs(&[("__name__", "seg1_metric"), ("job", "api")])
            .expect("seg1_metric in subsequent segment must not be truncated by repair_write_log");
        assert_eq!(read_head_float_samples(&head, e1.ref_id), vec![(1_000, 11.0)]);
        assert_eq!(read_head_float_samples(&head, e2.ref_id), vec![(2_000, 22.0)]);
        let meta = e1.inner.lock().metadata().expect("metadata replayed");
        let syms = head.symbols.read();
        assert_eq!(syms.resolve(meta.unit), "\u{FFFD}\u{FFFD}");
        assert_eq!(syms.resolve(meta.help), "bad\u{FFFD}help");
        drop(syms);
        head.close().unwrap();

        // Both original segments must still be intact, and checkpointing must succeed.
        assert_eq!(segments_range(&wal_dir).unwrap(), Some((0, 2)));
        let cp_stats = create_checkpoint(
            &wal_dir,
            0,
            0,
            |id| id == 1 || id == 2,
            500,
            false,
            CompressionType::Snappy,
        )
        .unwrap();
        assert_eq!(cp_stats.total_metadata, 1);
        let (cp_dir, _) = last_checkpoint(&wal_dir).unwrap().unwrap();
        let mut r = WlogReader::open_dir(&cp_dir).unwrap();
        let dec = RecordDecoder::new();
        let mut saw_meta = false;
        while let Some(rec) = r.next_record().unwrap() {
            if let DecodedRecord::Metadata(ms) = dec.decode_record(rec).unwrap() {
                assert_eq!(ms[0].unit, "\u{FFFD}\u{FFFD}");
                assert_eq!(ms[0].help, "bad\u{FFFD}help");
                saw_meta = true;
            }
        }
        assert!(saw_meta);
    }

    #[test]
    fn head_open_replay_full_tombstone_updates_wal_expiry_and_head_gauges() {
        use crate::chunkenc::histogram_model::{Histogram, HistogramSpan};
        use crate::record::{RecordEncoder, RefHistogramSample, RefSample, RefSeries, Tombstone};
        use crate::wlog::{CompressionType, WlogWriter};

        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let enc = RecordEncoder::new(false);
        let mut w = WlogWriter::open(&wal_dir, CompressionType::Snappy).unwrap();

        let s_stale = enc.series(
            &[
                RefSeries {
                    ref_id: 10,
                    labels: Labels::from_pairs([("__name__", "stale_s")]),
                },
                RefSeries {
                    ref_id: 11,
                    labels: Labels::from_pairs([("__name__", "hist_s")]),
                },
            ],
            Vec::new(),
        );
        let float_rec = enc.samples(
            &[
                RefSample { ref_id: 10, st: 0, t: 1_000, v: 1.0 },
                RefSample { ref_id: 10, st: 0, t: 2_000, v: stale_nan() },
            ],
            Vec::new(),
        );
        let hist = Histogram {
            schema: 0,
            count: 3,
            sum: 10.0,
            zero_threshold: 0.001,
            zero_count: 1,
            positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
            positive_buckets: vec![1, 1],
            ..Default::default()
        };
        let (hist_rec, _) = enc.histogram_samples(
            &[RefHistogramSample { ref_id: 11, st: 0, t: 3_000, h: hist }],
            Vec::new(),
        );
        let tomb_rec = enc.tombstones(
            &[
                Tombstone { ref_id: 10, mint: i64::MIN, maxt: i64::MAX },
                Tombstone { ref_id: 11, mint: i64::MIN, maxt: i64::MAX },
            ],
            Vec::new(),
        );
        w.log(&[&s_stale, &float_rec, &hist_rec, &tomb_rec]).unwrap();
        w.close().unwrap();

        let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        assert_eq!(head.num_series(), 0);
        assert_eq!(head.num_stale_series(), 0);
        assert_eq!(head.num_native_histogram_series(), 0);
        assert_eq!(head.num_native_histogram_buckets(), 0);
        {
            let exp = head.wal_expiries.lock();
            assert_eq!(exp.get(&10).copied(), Some(2_000));
            assert_eq!(exp.get(&11).copied(), Some(3_000));
        }
        head.close().unwrap();
    }

    #[test]
    fn commit_batch_multi_head_wal_partial_failure_applies_committed_heads_in_memory() {
        let dir0 = tempfile::tempdir().unwrap();
        let dir1 = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 10_000,
            ..HeadOptions::default()
        };
        let head0 = Head::open(dir0.path(), opts.clone(), i64::MIN).unwrap();
        let head1 = Head::open(dir1.path(), opts, i64::MIN).unwrap();

        // Seed an in-order anchor in head0 so we can test both in-order and OOO commit on head0.
        let p0_ooo = [("__name__", "h0_ooo"), ("job", "api")];
        {
            let mut app = head0.appender();
            app.append_float(None, &p0_ooo, 10_000, 1.0).unwrap();
            app.commit().unwrap();
        }

        // Inject WAL failure on head1.
        {
            let mut wal1 = head1.wal.lock();
            wal1.as_mut().unwrap().set_write_fault_hook(Some(Arc::new(|_| {
                crate::head_chunks::WriteFaultAction::Error(io::ErrorKind::StorageFull)
            })));
        }

        let p0_in = [("__name__", "h0_in"), ("job", "api")];
        let mut app0 = head0.appender();
        let r0_in = app0.append_float(None, &p0_in, 10_000, 42.0).unwrap();
        let r0_ooo = app0.append_float(None, &p0_ooo, 5_000, 99.0).unwrap();

        let p1 = [("__name__", "h1_fail"), ("job", "api")];
        let mut app1 = head1.appender();
        let r1 = app1.append_float(None, &p1, 10_000, 7.0).unwrap();

        let err = HeadAppender::commit_batch(vec![app0, app1]).unwrap_err();
        assert!(matches!(err, HeadError::Wlog(_)));

        // head0 already committed durably to its WAL, so both in-order and OOO samples must be visible.
        let e0_in = head0.get_entry_by_sorted_pairs(&p0_in).unwrap();
        assert_eq!(e0_in.pending_commits_count(), 0);
        assert_eq!(read_head_float_samples(&head0, r0_in), vec![(10_000, 42.0)]);
        assert_eq!(read_head_float_samples(&head0, r0_ooo), vec![(5_000, 99.0), (10_000, 1.0)]);

        // head1 failed WAL write, so its sample must NOT be visible in memory.
        let e1 = head1.get_entry_by_sorted_pairs(&p1).unwrap();
        assert_eq!(e1.pending_commits_count(), 0);
        assert!(head1.series_chunks(r1, i64::MIN, i64::MAX).unwrap().is_none());

        head0.close().unwrap();
        head1.close().unwrap();
    }

    #[test]
    fn shared_postings_cache_skips_source_epochs_on_fresh_head_open_and_prunes_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let shared_cache = Arc::new(PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            enable_invalidation: true,
            num_metric_versions: 64,
            ..Default::default()
        }));
        shared_cache.set_eligibility(true, true);

        let opts = HeadOptions {
            shared_postings_cache: Some(Arc::clone(&shared_cache)),
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        assert!(
            !shared_cache.has_source_epochs(),
            "fresh Head::open must not populate source_epochs on shared postings_cache"
        );

        let source_id = head.postings_cache_source_id;
        shared_cache.invalidate_source(source_id);
        assert!(shared_cache.has_source_epochs());

        drop(head);
        assert!(
            !shared_cache.has_source_epochs(),
            "Head::drop must prune source_id from shared postings_cache.source_epochs"
        );
    }

    #[test]
    fn quiet_zero_nan_reject_ooo_and_min_valid_time_exempt() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 0,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "qzn_m"), ("job", "t")];
        let qzn = f64::from_bits(QUIET_ZERO_NAN_BITS);

        // Seed in-order sample at t = 20_000 so min_valid_time = 20_000 - 5_000 = 15_000.
        let mut app = head.appender();
        let r = app.append_float(None, &pairs, 20_000, 1.0).unwrap();
        app.commit().unwrap();

        // 1) append_float_reject_ooo with QUIET_ZERO_NAN_BITS at t < maxt (within min_valid_time)
        // must return Err(HeadError::OutOfOrderSample) like Go head_append_v2.go:275-279.
        let mut app = head.appender();
        let err = app
            .append_float_reject_ooo(Some(r), &pairs, 18_000, qzn)
            .unwrap_err();
        assert!(
            matches!(err, HeadError::OutOfOrderSample),
            "expected OutOfOrderSample for reject_ooo + QUIET_ZERO_NAN_BITS at t < maxt, got {err:?}"
        );

        // Also verify prehashed_encoded reject_ooo path.
        let lbls = Labels::from_sorted_slice(&pairs);
        let h = lbls.hash();
        let mut cached = Some(head.get_entry_by_sorted_pairs(&pairs).unwrap());
        let err_enc = app
            .append_float_with_entry_prehashed_encoded(&mut cached, h, lbls.as_bytes(), 0, 18_000, qzn, true)
            .unwrap_err();
        assert!(
            matches!(err_enc, HeadError::OutOfOrderSample),
            "expected OutOfOrderSample for prehashed_encoded reject_ooo + QUIET_ZERO_NAN_BITS, got {err_enc:?}"
        );
        app.rollback().unwrap();

        // 2) With ooo_time_window == 0 and t < min_valid_time (10_000 < 15_000), Go head_append_v2.go:149
        // rejects unconditionally with ErrOutOfBounds before getOrCreate (no orphan series created).
        let mut app = head.appender();
        let new_pairs = [("__name__", "qzn_orphan"), ("job", "t")];
        assert!(matches!(
            app.append_float(None, &new_pairs, 10_000, qzn),
            Err(HeadError::OutOfBounds)
        ));
        assert!(head.get_entry_by_sorted_pairs(&new_pairs).is_none());

        // Within [min_valid_time, maxt), normal append (reject_ooo = false) accepts QUIET_ZERO_NAN_BITS
        // via memSeries.appendable() and drops it as a no-op in commit.
        app.append_float(Some(r), &pairs, 18_000, qzn).unwrap();
        app.append_float_by_ref(r, 18_000, qzn).unwrap();
        app.append_float_with_entry_prehashed_encoded(&mut cached, h, lbls.as_bytes(), 0, 18_000, qzn, false)
            .unwrap();
        app.commit().unwrap();

        // Only the original sample at 20_000 is stored.
        assert_eq!(read_head_float_samples(&head, r), vec![(20_000, 1.0)]);

        head.close().unwrap();
    }

    #[test]
    fn append_exemplar_stale_ref_fallback_filters_empty_labels() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            enable_exemplar_storage: true,
            max_exemplars: 100,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs_clean = [("__name__", "ex_m"), ("job", "api")];

        let mut app = head.appender();
        let r = app.append_float(None, &pairs_clean, 10_000, 1.0).unwrap();
        app.commit().unwrap();

        // Pass a stale series_ref (r + 9999) and pairs containing an empty label value.
        let pairs_with_empty = [("__name__", "ex_m"), ("empty", ""), ("job", "api")];
        let ex = Exemplar {
            labels: Labels::from_pairs([("trace_id", "abc")]),
            value: 1.0,
            timestamp_ms: 10_000,
            has_ts: true,
        };
        let mut app = head.appender();
        let resolved_ref = app
            .append_exemplar(Some(r + 9999), &pairs_with_empty, ex)
            .expect("stale ref fallback must strip empty labels to match Go ls.WithoutEmpty()");
        assert_eq!(resolved_ref, r);
        app.commit().unwrap();

        head.close().unwrap();
    }

    #[test]
    fn w27_a01_reject_ooo_with_ooo_window_enabled_returns_out_of_order_sample_not_oob() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 50_000,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        let make_hist = |v: u64| Histogram {
            schema: 0,
            count: v,
            sum: v as f64,
            positive_spans: vec![crate::chunkenc::HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![v as i64],
            ..Histogram::default()
        };
        let make_fhist = |v: f64| FloatHistogram {
            schema: 0,
            count: v,
            sum: v,
            positive_spans: vec![crate::chunkenc::HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![v],
            ..FloatHistogram::default()
        };

        let fp = [("__name__", "ct_float"), ("job", "t")];
        let hp = [("__name__", "ct_hist"), ("job", "t")];
        let fhp = [("__name__", "ct_fhist"), ("job", "t")];

        // Seed head maxt = 200_000 -> min_valid_time = 195_000, ooo cutoff = 150_000.
        let mut app = head.appender();
        let fr = app.append_float(None, &fp, 200_000, 1.0).unwrap();
        let hr = app.append_histogram(None, &hp, 0, 200_000, make_hist(1)).unwrap();
        let fhr = app.append_float_histogram(None, &fhp, 0, 200_000, make_fhist(1.0)).unwrap();
        app.commit().unwrap();

        let oob_before = head
            .metrics
            .out_of_bound_samples
            .with_label_values(&["float"])
            .get()
            + head
                .metrics
                .out_of_bound_samples
                .with_label_values(&["histogram"])
                .get();

        // Both older-than-OOO-window (100_000 < 150_000 < 195_000) and in-window OOO (170_000)
        // must return Err(HeadError::OutOfOrderSample) when reject_ooo == true.
        let mut app = head.appender();
        for &ct_ts in &[100_000i64, 170_000i64] {
            assert!(
                matches!(
                    app.append_float_reject_ooo(Some(fr), &fp, ct_ts, 0.0),
                    Err(HeadError::OutOfOrderSample)
                ),
                "float reject_ooo at {ct_ts} should be OutOfOrderSample"
            );

            let lbls = Labels::from_sorted_slice(&fp);
            let h = lbls.hash();
            let mut cached = Some(head.get_entry_by_sorted_pairs(&fp).unwrap());
            assert!(
                matches!(
                    app.append_float_with_entry_prehashed_encoded(
                        &mut cached,
                        h,
                        lbls.as_bytes(),
                        0,
                        ct_ts,
                        0.0,
                        true,
                    ),
                    Err(HeadError::OutOfOrderSample)
                ),
                "prehashed_encoded float reject_ooo at {ct_ts} should be OutOfOrderSample"
            );

            assert!(
                matches!(
                    app.append_histogram_reject_ooo(Some(hr), &hp, ct_ts, make_hist(0)),
                    Err(HeadError::OutOfOrderSample)
                ),
                "histogram reject_ooo at {ct_ts} should be OutOfOrderSample"
            );
            assert!(
                matches!(
                    app.append_float_histogram_reject_ooo(Some(fhr), &fhp, ct_ts, make_fhist(0.0)),
                    Err(HeadError::OutOfOrderSample)
                ),
                "float_histogram reject_ooo at {ct_ts} should be OutOfOrderSample"
            );
        }

        // Fresh series with created_ts < min_valid_time when ooo_time_window > 0.
        let fresh_fp = [("__name__", "ct_float_fresh"), ("job", "t")];
        let fresh_hp = [("__name__", "ct_hist_fresh"), ("job", "t")];
        let fresh_fhp = [("__name__", "ct_fhist_fresh"), ("job", "t")];
        assert!(matches!(
            app.append_float_reject_ooo(None, &fresh_fp, 100_000, 0.0),
            Err(HeadError::OutOfOrderSample)
        ));
        assert!(matches!(
            app.append_histogram_reject_ooo(None, &fresh_hp, 100_000, make_hist(0)),
            Err(HeadError::OutOfOrderSample)
        ));
        assert!(matches!(
            app.append_float_histogram_reject_ooo(None, &fresh_fhp, 100_000, make_fhist(0.0)),
            Err(HeadError::OutOfOrderSample)
        ));
        app.rollback().unwrap();

        let oob_after = head
            .metrics
            .out_of_bound_samples
            .with_label_values(&["float"])
            .get()
            + head
                .metrics
                .out_of_bound_samples
                .with_label_values(&["histogram"])
                .get();
        assert_eq!(oob_after, oob_before, "reject_ooo must not increment out_of_bound_samples");

        head.close().unwrap();
    }

    #[test]
    fn w27_a02_histogram_validation_runs_before_out_of_bounds_check() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 0,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let hp = [("__name__", "val_before_oob_h"), ("job", "t")];

        let mut app = head.appender();
        app.append_float(None, &[("__name__", "seed"), ("job", "t")], 20_000, 1.0)
            .unwrap();
        app.commit().unwrap();

        let invalid_h = Histogram {
            schema: 99, // invalid schema
            ..Histogram::default()
        };
        let invalid_fh = FloatHistogram {
            schema: 99, // invalid schema
            ..FloatHistogram::default()
        };

        let mut app = head.appender();
        // t = 10_000 < min_valid_time (15_000), ooo_time_window == 0:
        // must fail with HistogramValidation BEFORE OutOfBounds.
        assert!(matches!(
            app.append_histogram(None, &hp, 0, 10_000, invalid_h),
            Err(HeadError::HistogramValidation(_))
        ));
        assert!(matches!(
            app.append_float_histogram(None, &hp, 0, 10_000, invalid_fh),
            Err(HeadError::HistogramValidation(_))
        ));
        assert_eq!(
            head.metrics
                .out_of_bound_samples
                .with_label_values(&["histogram"])
                .get(),
            0.0
        );
        app.rollback().unwrap();
        head.close().unwrap();
    }

    #[test]
    fn gate_a1_t_eq_maxt_duplicate_and_quiet_zero_nan_reject_ooo_parity() {
        use prometheus::core::Collector as _;

        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 50_000,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let qzn = f64::from_bits(QUIET_ZERO_NAN_BITS);

        let make_hist = |v: u64| Histogram {
            schema: 0,
            count: v,
            sum: v as f64,
            positive_spans: vec![crate::chunkenc::HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![v as i64],
            ..Histogram::default()
        };
        let make_fhist = |v: f64| FloatHistogram {
            schema: 0,
            count: v,
            sum: v,
            positive_spans: vec![crate::chunkenc::HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![v],
            ..FloatHistogram::default()
        };

        let fp = [("__name__", "ga1_float"), ("job", "t")];
        let fp_zero = [("__name__", "ga1_float_zero"), ("job", "t")];
        let hp = [("__name__", "ga1_hist"), ("job", "t")];
        let fhp = [("__name__", "ga1_fhist"), ("job", "t")];

        // Seed in-order maxt = 200_000 and an OOO sample at t = 180_000 on each series.
        let mut app = head.appender();
        let fr = app.append_float(None, &fp, 200_000, 1.0).unwrap();
        let fzr = app.append_float(None, &fp_zero, 200_000, 0.0).unwrap();
        let hr = app.append_histogram(None, &hp, 0, 200_000, make_hist(1)).unwrap();
        let fhr = app.append_float_histogram(None, &fhp, 0, 200_000, make_fhist(1.0)).unwrap();
        app.commit().unwrap();

        let mut app = head.appender();
        app.append_float(Some(fr), &fp, 180_000, 7.0).unwrap();
        app.append_histogram(Some(hr), &hp, 0, 180_000, make_hist(7)).unwrap();
        app.append_float_histogram(Some(fhr), &fhp, 0, 180_000, make_fhist(7.0)).unwrap();
        app.commit().unwrap();

        let ooo_hist_count = || -> u64 {
            head.metrics
                .ooo_histogram
                .collect()
                .first()
                .and_then(|mf| mf.get_metric().first())
                .map(|m| m.get_histogram().get_sample_count())
                .unwrap_or(0)
        };
        let dup_f = || head.metrics.duplicate_samples.with_label_values(&["float"]).get();
        let dup_h = || head.metrics.duplicate_samples.with_label_values(&["histogram"]).get();
        let ooo_f = || head.metrics.out_of_order_samples.with_label_values(&["float"]).get();
        let ooo_h = || head.metrics.out_of_order_samples.with_label_values(&["histogram"]).get();

        let mut app = head.appender();

        // 1) Equal-value duplicate at t == st.max_time() (including QuietZeroNaN when prev_v == 0.0)
        // must succeed silently under reject_ooo == true without incrementing out_of_order_samples.
        let ooo_hist_0 = ooo_hist_count();
        let ooo_f_0 = ooo_f();
        let ooo_h_0 = ooo_h();
        let dup_f_0 = dup_f();
        let dup_h_0 = dup_h();

        assert_eq!(app.append_float_reject_ooo(Some(fr), &fp, 200_000, 1.0).unwrap(), fr);
        assert_eq!(app.append_float_reject_ooo(Some(fzr), &fp_zero, 200_000, qzn).unwrap(), fzr);
        let lbls = Labels::from_sorted_slice(&fp_zero);
        let h = lbls.hash();
        let mut cached_zero = Some(head.get_entry_by_sorted_pairs(&fp_zero).unwrap());
        assert_eq!(
            app.append_float_with_entry_prehashed_encoded(
                &mut cached_zero,
                h,
                lbls.as_bytes(),
                0,
                200_000,
                qzn,
                true,
            )
            .unwrap(),
            fzr
        );
        assert_eq!(app.append_histogram_reject_ooo(Some(hr), &hp, 200_000, make_hist(1)).unwrap(), hr);
        assert_eq!(
            app.append_float_histogram_reject_ooo(Some(fhr), &fhp, 200_000, make_fhist(1.0))
                .unwrap(),
            fhr
        );
        assert_eq!(ooo_f(), ooo_f_0);
        assert_eq!(ooo_h(), ooo_h_0);
        assert_eq!(ooo_hist_count(), ooo_hist_0);
        assert_eq!(dup_f(), dup_f_0 + 3.0);
        assert_eq!(dup_h(), dup_h_0 + 2.0);

        // 2) Override duplicate at t == st.max_time() under reject_ooo == true returns DuplicateSample*
        // and increments duplicate_samples, NOT out_of_order_samples or ooo_histogram.
        assert!(matches!(
            app.append_float_reject_ooo(Some(fr), &fp, 200_000, 2.0),
            Err(HeadError::DuplicateSampleWithDetails(_))
        ));
        assert!(matches!(
            app.append_histogram_reject_ooo(Some(hr), &hp, 200_000, make_hist(2)),
            Err(HeadError::DuplicateSampleForTimestamp)
        ));
        assert!(matches!(
            app.append_float_histogram_reject_ooo(Some(fhr), &fhp, 200_000, make_fhist(2.0)),
            Err(HeadError::DuplicateSampleForTimestamp)
        ));
        assert_eq!(ooo_f(), ooo_f_0);
        assert_eq!(ooo_h(), ooo_h_0);
        assert_eq!(ooo_hist_count(), ooo_hist_0);
        assert_eq!(dup_f(), dup_f_0 + 4.0);
        assert_eq!(dup_h(), dup_h_0 + 4.0);

        // 3) OOO duplicate at t < st.max_time() (180_000, already stored in OOO chunk) under
        // reject_ooo == true must return OutOfOrderSample (before OOO dup check), incrementing
        // out_of_order_samples and NOT duplicate_samples or ooo_histogram.
        assert!(matches!(
            app.append_float_reject_ooo(Some(fr), &fp, 180_000, 7.0),
            Err(HeadError::OutOfOrderSample)
        ));
        assert!(matches!(
            app.append_histogram_reject_ooo(Some(hr), &hp, 180_000, make_hist(7)),
            Err(HeadError::OutOfOrderSample)
        ));
        assert!(matches!(
            app.append_float_histogram_reject_ooo(Some(fhr), &fhp, 180_000, make_fhist(7.0)),
            Err(HeadError::OutOfOrderSample)
        ));
        assert_eq!(ooo_f(), ooo_f_0 + 1.0);
        assert_eq!(ooo_h(), ooo_h_0 + 2.0);
        assert_eq!(ooo_hist_count(), ooo_hist_0);
        assert_eq!(dup_f(), dup_f_0 + 4.0);
        assert_eq!(dup_h(), dup_h_0 + 4.0);

        // 4) QuietZeroNaN at t < st.max_time() under reject_ooo == true returns OutOfOrderSample.
        assert!(matches!(
            app.append_float_reject_ooo(Some(fzr), &fp_zero, 180_000, qzn),
            Err(HeadError::OutOfOrderSample)
        ));
        assert_eq!(ooo_f(), ooo_f_0 + 2.0);
        assert_eq!(ooo_hist_count(), ooo_hist_0);
        assert_eq!(dup_f(), dup_f_0 + 4.0);

        app.rollback().unwrap();
        head.close().unwrap();
    }

    #[test]
    fn compaction_interval_collection_never_deadlocks_with_appends_and_chunk_reader_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 10_000,
            samples_per_chunk: 4,
            ooo_time_window_ms: 200_000,
            ooo_cap_max: 2,
            ..HeadOptions::default()
        };
        let head = Arc::new(Head::open(dir.path(), opts, 0).unwrap());

        let mut candidates = Vec::new();
        for i in 0..8usize {
            let lbls = Labels::from_pairs([("__name__", "m"), ("s", &i.to_string())]);
            let mut app = head.appender();
            let mut ref_id = None;
            for step in 0..4i64 {
                let r = match ref_id {
                    Some(id) => app
                        .append_float_by_ref(id, 100_000 + step * 10_000, step as f64)
                        .unwrap(),
                    None => app
                        .append_float(
                            None,
                            &[("__name__", "m"), ("s", &i.to_string())],
                            100_000 + step * 10_000,
                            step as f64,
                        )
                        .unwrap(),
                };
                ref_id = Some(r);
            }
            app.commit().unwrap();
            candidates.push((ref_id.unwrap(), lbls));
        }
        head.mmap_head_chunks().unwrap();

        // Populate OOO mmapped chunks (> ooo_cap_max = 2) spanning > range_ms = 1.
        for &(ref_id, _) in &candidates {
            let mut app = head.appender();
            for t in [50_000i64, 51_000, 52_000, 53_000] {
                app.append_float_by_ref(ref_id, t, 1.0).unwrap();
            }
            app.commit().unwrap();
        }

        // Verify interval helpers return non-empty intervals for oversized mmapped chunks.
        let sel = head.collect_selected_compaction_intervals(&candidates, 1);
        assert!(!sel.is_empty());
        let ooo = head.collect_ooo_compaction_intervals(
            &candidates,
            ChunkDiskMapperRef::new(u32::MAX, u32::MAX),
            1,
        );
        assert!(!ooo.is_empty());

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Thread A: compaction interval collectors (previously inverted chunk_reader.read() -> entry.inner).
        let h_a = Arc::clone(&head);
        let cands_a = candidates.clone();
        let b_a = Arc::clone(&barrier);
        let stop_a = Arc::clone(&stop);
        let tx_a = done_tx.clone();
        std::thread::spawn(move || {
            b_a.wait();
            for _ in 0..400 {
                if stop_a.load(Ordering::Relaxed) {
                    break;
                }
                let _ = h_a.collect_selected_compaction_intervals(&cands_a, 1);
                let _ = h_a.collect_ooo_compaction_intervals(
                    &cands_a,
                    ChunkDiskMapperRef::new(u32::MAX, u32::MAX),
                    1,
                );
            }
            let _ = tx_a.send("a");
        });

        // Thread B: concurrent appends touching mmapped timestamps (entry.inner -> chunk_reader.read()).
        let h_b = Arc::clone(&head);
        let cands_b = candidates.clone();
        let b_b = Arc::clone(&barrier);
        let stop_b = Arc::clone(&stop);
        let tx_b = done_tx.clone();
        std::thread::spawn(move || {
            b_b.wait();
            for iter in 0..400i64 {
                if stop_b.load(Ordering::Relaxed) {
                    break;
                }
                let idx = (iter as usize) % cands_b.len();
                let ref_id = cands_b[idx].0;
                let mut app = h_b.appender();
                let _ = app.append_float_by_ref(ref_id, 105_000 + (iter % 200), iter as f64);
                let _ = app.commit();
            }
            let _ = tx_b.send("b");
        });

        // Thread C: concurrent chunk_reader.write() (refresh_chunk_reader / OOO flush).
        let h_c = Arc::clone(&head);
        let b_c = Arc::clone(&barrier);
        let stop_c = Arc::clone(&stop);
        let tx_c = done_tx;
        std::thread::spawn(move || {
            b_c.wait();
            for _ in 0..400 {
                if stop_c.load(Ordering::Relaxed) {
                    break;
                }
                h_c.refresh_chunk_reader().unwrap();
            }
            let _ = tx_c.send("c");
        });

        for _ in 0..3 {
            if done_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .is_err()
            {
                stop.store(true, Ordering::Relaxed);
                panic!("3-thread deadlock detected between compaction intervals, appends, and refresh_chunk_reader");
            }
        }
        head.close().unwrap();
    }
}
