//! Striped TSDB Head with zero-allocation hot-path appends, OOO support, mmap `chunks_head`,
//! WAL/WBL logging & crash recovery replay, and truncation/checkpointing.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/head.go,
//! github.com/prometheus/prometheus/tsdb/head_append.go,
//! github.com/prometheus/prometheus/tsdb/head_read.go,
//! and github.com/prometheus/prometheus/tsdb/head_wal.go.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use hashbrown::HashTable;
use parking_lot::{Mutex, RwLock};
use prometheus::{CounterVec, Histogram as PromHistogram, HistogramOpts, Opts, Registry};
use smallvec::SmallVec;
use thiserror::Error;

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
    decode_chunk_samples, encode_samples_into_chunks, CompactionSeries, SampleValue,
};
use crate::exemplar::{CircularExemplarStorage, Exemplar, ExemplarError};
use crate::head_chunks::{
    ChunkDiskMapperRef, HeadChunksError, HeadChunksReader, HeadChunksWriter,
    MAX_HEAD_CHUNK_FILE_SIZE,
};
use crate::labels::{encode_stringlabel_size, iter_stringlabels, Labels, SymbolLabels, METRIC_NAME};
use crate::metadata::{MetadataError, MetadataLimits, UserMetricsMetadata};
use crate::postings::{
    postings_for_matchers, MemPostings, PostingsBatchScratch, PostingsError,
};
use crate::postings_cache::{PostingsForMatchersCache, PostingsForMatchersCacheConfig};
use crate::matcher::LabelMatcher;
use crate::record::{
    DecodedRecord, MetricType, RecordDecoder, RecordEncoder, RecordError, RefExemplar,
    RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefMmapMarker, RefSample, RefSeries,
};
use crate::symbols::{SymbolId, SymbolTable};
use crate::tombstones::{is_completely_deleted, Interval, MemTombstones};
use crate::wlog::{
    create_checkpoint, decompress_record_into, delete_checkpoints,
    delete_temp_checkpoints, last_checkpoint, segments_range,
    CompressionType, SegmentRange, SegmentTracker, WlogError, WlogReader, WlogWriter,
    DEFAULT_SEGMENT_SIZE, WBL_DIR_NAME,
};

pub const DEFAULT_BLOCK_DURATION_MS: i64 = 2 * 60 * 60 * 1000; // 2h
pub const DEFAULT_SAMPLES_PER_CHUNK: usize = 120;
pub const DEFAULT_STRIPE_SIZE: usize = 16384;
pub const DEFAULT_MAX_EXEMPLARS: usize = 100_000;
pub const DEFAULT_OOO_CAP_MAX: usize = 32;

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
}

/// Lifecycle callback trait matching `prometheus/tsdb.SeriesLifecycleCallback`.
pub trait SeriesLifecycleCallback: Send + Sync {
    fn pre_creation(&self, pairs: &[(&str, &str)]) -> Result<(), HeadError>;
    fn post_creation(&self, ref_id: u64, labels: &Labels);
    fn post_deletion(&self, deleted: &[(u64, Labels)]);
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
            .field("metadata_limits", &self.metadata_limits)
            .field(
                "series_lifecycle_callback",
                &self.series_lifecycle_callback.as_ref().map(|_| "..."),
            )
            .field("registry", &self.registry.as_ref().map(|_| "..."))
            .finish()
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
            postings_cache: PostingsForMatchersCacheConfig {
                enable_invalidation: true,
                ..Default::default()
            },
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
    (t / width) * width + width
}

#[inline]
fn compute_chunk_end_time(start: i64, cur: i64, max_t: i64, ratio_to_full: f64) -> i64 {
    let n = ((max_t - start) as f64) / (((cur - start + 1) as f64) * ratio_to_full);
    if n <= 1.0 {
        return max_t;
    }
    start + (((max_t - start) as f64) / n.floor()) as i64
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

impl MemChunk {
    #[inline]
    fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time <= maxt && mint <= self.max_time
    }
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

#[derive(Debug, Default)]
struct SeriesOooState {
    ooo_head_chunk: Option<OooHeadChunk>,
    /// Encoded chunks from cut `ooo_head_chunk`s written to `HeadChunksWriter`, awaiting
    /// `writer.flush()` and `chunk_reader` refresh before promotion to `ooo_mmapped_chunks`.
    ooo_pending_mmapped: Vec<(MmappedChunk, EncodedChunk)>,
    ooo_mmapped_chunks: Vec<MmappedChunk>,
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

const FLAG_COMPUTED_HIST_END_TIME: u32 = 1 << 29;
const FLAG_GCED: u32 = 1 << 30;
const FLAG_HAS_LAST_EXEMPLAR: u32 = 1 << 31;

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

#[derive(Debug)]
pub struct SeriesEntry {
    pub ref_id: u64,
    pub hash: u64,
    pub labels: Labels,
    active_cache: AtomicU64,
    in_order_max_time: AtomicI64,
    pending_commits: AtomicU32,
    gced: AtomicBool,
    inner: Mutex<SeriesState>,
}

impl SeriesEntry {
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
        self.gced.load(Ordering::Acquire)
            || (self.pending_commits.load(Ordering::Acquire) & PENDING_COMMIT_GCED_BIT) != 0
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
}

struct PendingCommitGuard<'g>(&'g AtomicU32);

impl Drop for PendingCommitGuard<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
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

#[derive(Debug)]
struct SeriesState {
    head_chunks: SmallVec<[MemChunk; 1]>,
    #[allow(clippy::box_collection)]
    mmapped_chunks: Option<Box<Vec<MmappedChunk>>>,
    ooo: Option<Box<SeriesOooState>>,
    next_at: i64,
    created_timestamp: i64,
    last_exemplar_ts: i64,
    last_sample: LastSample,
    metadata: Option<CompactSeriesMetadata>,
    flags: u32,
    xor_state: XorAppendState,
    xor2_state: Option<Box<Xor2AppendState>>,
    hist_state: Option<Box<HistAppendCache>>,
}

impl SeriesState {
    fn new() -> Self {
        Self {
            head_chunks: SmallVec::new(),
            mmapped_chunks: None,
            ooo: None,
            next_at: i64::MIN,
            created_timestamp: 0,
            last_exemplar_ts: i64::MIN,
            last_sample: LastSample::None,
            metadata: None,
            flags: 0,
            xor_state: XorAppendState::default(),
            xor2_state: None,
            hist_state: None,
        }
    }

    #[inline]
    fn mmapped(&self) -> &[MmappedChunk] {
        self.mmapped_chunks.as_deref().map_or(&[], Vec::as_slice)
    }

    #[inline]
    fn mmapped_mut(&mut self) -> &mut Vec<MmappedChunk> {
        self.mmapped_chunks.get_or_insert_with(Default::default)
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
        ((self.flags & FLAG_HAS_LAST_EXEMPLAR) != 0).then_some(self.last_exemplar_ts)
    }

    #[inline]
    fn update_last_exemplar_ts(&mut self, ts: i64) {
        if (self.flags & FLAG_HAS_LAST_EXEMPLAR) != 0 {
            self.last_exemplar_ts = self.last_exemplar_ts.max(ts);
        } else {
            self.last_exemplar_ts = ts;
            self.flags |= FLAG_HAS_LAST_EXEMPLAR;
        }
    }

    #[inline]
    fn has_any_in_order_chunks(&self) -> bool {
        !self.head_chunks.is_empty() || !self.mmapped().is_empty()
    }

    #[inline]
    fn max_time(&self) -> i64 {
        if let Some(c) = self.head_chunks.last() {
            c.max_time
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
        } else if let Some(c) = self.head_chunks.first() {
            c.min_time
        } else {
            i64::MAX
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
        if let Some(c) = self.head_chunks.last() {
            Some(c.chunk.encoding())
        } else {
            self.mmapped().last().map(|c| c.encoding)
        }
    }

    fn last_in_order_sample(&self, chunks_reader: Option<&HeadChunksReader>) -> Option<LastSample> {
        if !matches!(self.last_sample, LastSample::None) {
            return Some(self.last_sample.clone());
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
        !self.mmapped().is_empty()
            && matches!(self.last_sample, LastSample::None)
            && t == self.max_time()
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
            return Ok((true, true));
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
                match self.xor2_state {
                    Some(ref mut s) => **s = Xor2AppendState::new_empty(),
                    None => self.xor2_state = Some(Box::new(Xor2AppendState::new_empty())),
                }
                EncodedChunk::Xor2(Xor2Chunk::new())
            }
            Encoding::Histogram => {
                self.hist_state = None;
                EncodedChunk::Histogram(HistogramChunk::new())
            }
            Encoding::HistogramSt => {
                self.hist_state = None;
                EncodedChunk::HistogramSt(HistogramStChunk::new())
            }
            Encoding::FloatHistogram => {
                self.hist_state = None;
                EncodedChunk::FloatHistogram(FloatHistogramChunk::new())
            }
            Encoding::FloatHistogramSt => {
                self.hist_state = None;
                EncodedChunk::FloatHistogramSt(FloatHistogramStChunk::new())
            }
            _ => {
                self.xor_state = XorAppendState::new_empty();
                EncodedChunk::Xor(XorChunk::new())
            }
        };
        self.head_chunks.push(MemChunk {
            chunk,
            min_time: mint,
            max_time: i64::MIN,
        });
        self.next_at = range_for_timestamp(mint, chunk_range_ms);
        self.set_computed_hist_end_time(false);
    }

    fn append_float_in_order(
        &mut self,
        st: i64,
        t: i64,
        v: f64,
        chunk_range_ms: i64,
        samples_per_chunk: usize,
        float_enc: Encoding,
    ) -> Result<(bool, bool), ChunkError> {
        let target_enc = if float_enc == Encoding::Xor2 {
            Encoding::Xor2
        } else {
            Encoding::Xor
        };
        // Returns (appended, chunk_created)
        let mut chunk_created = false;
        if self.head_chunks.is_empty() {
            if let Some(last_mm) = self.mmapped().last() {
                if last_mm.max_time >= t {
                    return Ok((false, false));
                }
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let need_cut = {
            let cur = self.head_chunks.last().unwrap();
            if cur.max_time >= t {
                return Ok((false, chunk_created));
            }
            let num_samples = cur.chunk.num_samples();
            (!chunk_created && cur.chunk.as_bytes().len() > MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND)
                || cur.chunk.encoding() != target_enc
                || (num_samples > 0 && (t >= self.next_at || num_samples >= samples_per_chunk * 2))
        };

        if need_cut {
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let prev_v = match self.last_sample {
            LastSample::Float(pv) => pv,
            _ => {
                self.xor_state.valid = false;
                0.0
            }
        };
        let cur = self.head_chunks.last_mut().unwrap();
        let num_samples = cur.chunk.num_samples();
        if num_samples == 0 {
            cur.min_time = t;
            self.next_at = range_for_timestamp(t, chunk_range_ms);
        } else if num_samples == samples_per_chunk / 4 {
            let max_next_at = self.next_at;
            self.next_at = compute_chunk_end_time(cur.min_time, cur.max_time, max_next_at, 4.0);
        }

        match cur.chunk {
            EncodedChunk::Xor(ref mut xc) => {
                xc.append_with_state(&mut self.xor_state, cur.max_time, prev_v, t, v)?;
            }
            EncodedChunk::Xor2(ref mut xc) => {
                let s = self
                    .xor2_state
                    .get_or_insert_with(|| Box::new(Xor2AppendState::default()));
                xc.append_with_state(s, st, t, v)?;
            }
            _ => unreachable!(),
        }
        cur.max_time = t;
        self.last_sample = LastSample::Float(v);
        if st != 0 && self.created_timestamp == 0 {
            self.created_timestamp = st;
        }
        Ok((true, chunk_created))
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
        if self.head_chunks.is_empty() {
            if let Some(last_mm) = self.mmapped().last() {
                if last_mm.max_time >= t {
                    return Ok((false, false));
                }
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        {
            let cur = self.head_chunks.last().unwrap();
            if cur.max_time >= t {
                return Ok((false, chunk_created));
            }
        }

        let cur_enc = self.head_chunks.last().unwrap().chunk.encoding();
        if cur_enc != target_enc {
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let (num_samples, num_bytes, c_min, c_max) = {
            let cur = self.head_chunks.last_mut().unwrap();
            let ns = cur.chunk.num_samples();
            if ns == 0 {
                cur.min_time = t;
                self.next_at = range_for_timestamp(t, chunk_range_ms);
            }
            (ns, cur.chunk.as_bytes().len(), cur.min_time, cur.max_time)
        };

        let next_chunk_range_start = if self.computed_hist_end_time() {
            range_for_timestamp(c_min, chunk_range_ms)
        } else {
            self.next_at
        };

        if !self.computed_hist_end_time() && num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK / 4 {
            let ratio = (TARGET_BYTES_PER_HISTOGRAM_CHUNK as f64) / (num_bytes as f64);
            self.next_at = compute_chunk_end_time(c_min, c_max, self.next_at, ratio);
            self.set_computed_hist_end_time(true);
        }

        if (t >= self.next_at || num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2)
            && (num_samples >= MIN_SAMPLES_PER_HISTOGRAM_CHUNK || t >= next_chunk_range_start)
        {
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let cur = self.head_chunks.last_mut().unwrap();
        match cur.chunk {
            EncodedChunk::Histogram(ref mut hc) => {
                let cache = self
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::Histogram(None)));
                if !matches!(**cache, HistAppendCache::Histogram(_)) {
                    **cache = HistAppendCache::Histogram(None);
                }
                let HistAppendCache::Histogram(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                match hc.append_with_state(st_opt, st, t, h)? {
                    AppendHistogramOutcome::Appended { .. } => {
                        cur.max_time = t;
                    }
                    AppendHistogramOutcome::NewChunk(new_chk) => {
                        self.head_chunks.push(MemChunk {
                            chunk: EncodedChunk::Histogram(new_chk),
                            min_time: t,
                            max_time: t,
                        });
                        self.next_at = range_for_timestamp(t, chunk_range_ms);
                        self.set_computed_hist_end_time(false);
                        chunk_created = true;
                    }
                }
            }
            EncodedChunk::HistogramSt(ref mut hc) => {
                let cache = self
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::HistogramSt(None)));
                if !matches!(**cache, HistAppendCache::HistogramSt(_)) {
                    **cache = HistAppendCache::HistogramSt(None);
                }
                let HistAppendCache::HistogramSt(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                match hc.append_with_state(st_opt, st, t, h)? {
                    AppendHistogramStOutcome::Appended { .. } => {
                        cur.max_time = t;
                    }
                    AppendHistogramStOutcome::NewChunk(new_chk) => {
                        self.head_chunks.push(MemChunk {
                            chunk: EncodedChunk::HistogramSt(new_chk),
                            min_time: t,
                            max_time: t,
                        });
                        self.next_at = range_for_timestamp(t, chunk_range_ms);
                        self.set_computed_hist_end_time(false);
                        chunk_created = true;
                    }
                }
            }
            _ => unreachable!(),
        }
        self.last_sample = LastSample::Histogram(Box::new(h.clone()));
        if st != 0 && self.created_timestamp == 0 {
            self.created_timestamp = st;
        }
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
        if self.head_chunks.is_empty() {
            if let Some(last_mm) = self.mmapped().last() {
                if last_mm.max_time >= t {
                    return Ok((false, false));
                }
            }
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        {
            let cur = self.head_chunks.last().unwrap();
            if cur.max_time >= t {
                return Ok((false, chunk_created));
            }
        }

        let cur_enc = self.head_chunks.last().unwrap().chunk.encoding();
        if cur_enc != target_enc {
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let (num_samples, num_bytes, c_min, c_max) = {
            let cur = self.head_chunks.last_mut().unwrap();
            let ns = cur.chunk.num_samples();
            if ns == 0 {
                cur.min_time = t;
                self.next_at = range_for_timestamp(t, chunk_range_ms);
            }
            (ns, cur.chunk.as_bytes().len(), cur.min_time, cur.max_time)
        };

        let next_chunk_range_start = if self.computed_hist_end_time() {
            range_for_timestamp(c_min, chunk_range_ms)
        } else {
            self.next_at
        };

        if !self.computed_hist_end_time() && num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK / 4 {
            let ratio = (TARGET_BYTES_PER_HISTOGRAM_CHUNK as f64) / (num_bytes as f64);
            self.next_at = compute_chunk_end_time(c_min, c_max, self.next_at, ratio);
            self.set_computed_hist_end_time(true);
        }

        if (t >= self.next_at || num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2)
            && (num_samples >= MIN_SAMPLES_PER_HISTOGRAM_CHUNK || t >= next_chunk_range_start)
        {
            self.cut_new_head_chunk(t, target_enc, chunk_range_ms);
            chunk_created = true;
        }

        let cur = self.head_chunks.last_mut().unwrap();
        match cur.chunk {
            EncodedChunk::FloatHistogram(ref mut fhc) => {
                let cache = self
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::FloatHistogram(None)));
                if !matches!(**cache, HistAppendCache::FloatHistogram(_)) {
                    **cache = HistAppendCache::FloatHistogram(None);
                }
                let HistAppendCache::FloatHistogram(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                match fhc.append_with_state(st_opt, st, t, fh)? {
                    AppendFloatHistogramOutcome::Appended { .. } => {
                        cur.max_time = t;
                    }
                    AppendFloatHistogramOutcome::NewChunk(new_chk) => {
                        self.head_chunks.push(MemChunk {
                            chunk: EncodedChunk::FloatHistogram(new_chk),
                            min_time: t,
                            max_time: t,
                        });
                        self.next_at = range_for_timestamp(t, chunk_range_ms);
                        self.set_computed_hist_end_time(false);
                        chunk_created = true;
                    }
                }
            }
            EncodedChunk::FloatHistogramSt(ref mut fhc) => {
                let cache = self
                    .hist_state
                    .get_or_insert_with(|| Box::new(HistAppendCache::FloatHistogramSt(None)));
                if !matches!(**cache, HistAppendCache::FloatHistogramSt(_)) {
                    **cache = HistAppendCache::FloatHistogramSt(None);
                }
                let HistAppendCache::FloatHistogramSt(ref mut st_opt) = **cache else {
                    unreachable!();
                };
                match fhc.append_with_state(st_opt, st, t, fh)? {
                    AppendFloatHistogramStOutcome::Appended { .. } => {
                        cur.max_time = t;
                    }
                    AppendFloatHistogramStOutcome::NewChunk(new_chk) => {
                        self.head_chunks.push(MemChunk {
                            chunk: EncodedChunk::FloatHistogramSt(new_chk),
                            min_time: t,
                            max_time: t,
                        });
                        self.next_at = range_for_timestamp(t, chunk_range_ms);
                        self.set_computed_hist_end_time(false);
                        chunk_created = true;
                    }
                }
            }
            _ => unreachable!(),
        }
        self.last_sample = LastSample::FloatHistogram(Box::new(fh.clone()));
        if st != 0 && self.created_timestamp == 0 {
            self.created_timestamp = st;
        }
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
    ) -> Result<(bool, bool, Vec<ChunkDiskMapperRef>), HeadChunksError> {
        let ooo = self.ooo.get_or_insert_with(|| Box::new(SeriesOooState::default()));
        let mut chunk_created = false;
        let mut mmap_refs = Vec::new();

        let need_cut = ooo
            .ooo_head_chunk
            .as_ref()
            .is_none_or(|c| c.chunk.num_samples() >= ooo_cap_max);

        if need_cut {
            if let Some(old) = ooo.ooo_head_chunk.take() {
                if let Some(writer) = chunk_writer {
                    let cut_chunks = old.chunk.to_encoded_chunks(
                        i64::MIN,
                        i64::MAX,
                        use_xor2,
                        use_histogram_st,
                    )?;
                    for cc in cut_chunks {
                        let r = writer.write_chunk(
                            ref_id,
                            cc.min_time,
                            cc.max_time,
                            &cc.chunk,
                            true,
                        )?;
                        mmap_refs.push(r);
                        let mc = MmappedChunk {
                            ref_id: r,
                            min_time: cc.min_time,
                            max_time: cc.max_time,
                            num_samples: cc.chunk.num_samples() as u16,
                            encoding: cc.chunk.encoding(),
                        };
                        ooo.ooo_pending_mmapped.push((mc, cc.chunk));
                    }
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
        let inserted = cur.chunk.insert(st, t, val);
        if inserted {
            if chunk_created || t < cur.min_time {
                cur.min_time = t;
            }
            if chunk_created || t > cur.max_time {
                cur.max_time = t;
            }
        }
        Ok((inserted, chunk_created, mmap_refs))
    }

    fn write_sealed_head_chunks(
        &mut self,
        ref_id: u64,
        writer: &mut HeadChunksWriter,
    ) -> Result<Vec<MmappedChunk>, HeadChunksError> {
        if self.head_chunks.len() <= 1 {
            return Ok(Vec::new());
        }
        let sealed_count = self.head_chunks.len() - 1;
        let mut out = Vec::with_capacity(sealed_count);
        for mc in &mut self.head_chunks[..sealed_count] {
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
}

struct SeriesStripe {
    by_hash: HashTable<(u64, Arc<SeriesEntry>)>,
    by_id: FastU64Map<Arc<SeriesEntry>>,
}

impl SeriesStripe {
    fn new() -> Self {
        Self {
            by_hash: HashTable::new(),
            by_id: FastU64Map::default(),
        }
    }
}

/// Power-of-two striped series map matching `tsdb.stripeSeries`.
struct StripeSeries {
    mask: usize,
    stripes: Vec<RwLock<SeriesStripe>>,
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
        }
    }

    #[inline]
    fn stripe_idx(&self, key: u64) -> usize {
        (key as usize) & self.mask
    }

    fn get_by_id(&self, ref_id: u64) -> Option<Arc<SeriesEntry>> {
        let idx = self.stripe_idx(ref_id);
        self.stripes[idx].read().by_id.get(&ref_id).cloned()
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

    fn insert(&self, entry: Arc<SeriesEntry>) {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);

        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            s.by_id.insert(entry.ref_id, Arc::clone(&entry));
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
            hs.by_hash
                .insert_unique(hash, (hash, Arc::clone(&entry)), |&(h, _)| h);
            rs.by_id.insert(entry.ref_id, entry);
        }
    }

    fn remove(&self, entry: &SeriesEntry) {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);

        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            s.by_id.remove(&entry.ref_id);
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
            rs.by_id.remove(&entry.ref_id);
        }
    }

    /// Atomically verifies `pending_commits == 0`, marks `entry` & `st` as GCed,
    /// and unlinks `entry` from `by_id` and `by_hash` while holding both `entry.inner`
    /// and the stripe write lock(s).
    fn remove_if_gced_under_entry_lock(&self, entry: &SeriesEntry, st: &mut SeriesState) -> bool {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);

        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            if !entry.try_mark_gced() {
                return false;
            }
            entry.gced.store(true, Ordering::Release);
            st.set_gced(true);
            s.by_id.remove(&entry.ref_id);
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
            entry.gced.store(true, Ordering::Release);
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
            rs.by_id.remove(&entry.ref_id);
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

    fn all_entries(&self) -> Vec<Arc<SeriesEntry>> {
        let mut out = Vec::new();
        for s in &self.stripes {
            let guard = s.read();
            out.extend(guard.by_id.values().cloned());
        }
        out
    }

    fn shrink_oversized_stripes(&self) {
        for stripe in &self.stripes {
            let mut s = stripe.write();
            if s.by_id.is_empty() {
                if s.by_id.capacity() > 0 {
                    s.by_id = FastU64Map::default();
                }
            } else {
                let id_len = s.by_id.len();
                if s.by_id.capacity() > (id_len * 4).max(64) {
                    s.by_id.shrink_to(id_len * 2);
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
    }
}

/// Series chunks + tombstones returned by [`Head::series_chunks`].
pub type SeriesChunksResult = Option<(Labels, Vec<ChunkMeta>, Vec<Interval>)>;

/// Lightweight series chunk intervals returned by [`Head::series_chunk_metas_no_bytes`].
pub type SeriesChunkMetasNoBytes = Option<(Labels, SmallVec<[(i64, i64); 4]>, bool)>;

/// Lightweight candidate list + `[min_time, max_time_inclusive]` range for streaming compaction.
pub type CompactionCandidatesRange = (Vec<(u64, Labels)>, i64, i64);

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

/// Go `headMetrics` (the subset Mimir aggregates in `TSDBMetrics`, plus the related
/// counters), registered on the per-tenant registry.
struct HeadMetrics {
    mmap_chunk_corruptions_total: prometheus::Counter,
    wal_corruptions_total: prometheus::Counter,
    ooo_samples_appended: CounterVec,
    ooo_histogram: PromHistogram,
    active_appenders: prometheus::Gauge,
    chunks: prometheus::Gauge,
    chunks_created: prometheus::Counter,
    chunks_removed: prometheus::Counter,
    series_not_found: prometheus::Counter,
    gc_duration: crate::metrics::Summary,
    wal_truncate_duration: crate::metrics::Summary,
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
    wbl: crate::metrics::WlogMetrics,
    chunk_write_queue: Option<crate::metrics::ChunkWriteQueueMetrics>,
}

impl HeadMetrics {
    fn new(reg: Option<&Registry>, chunk_write_queue_size: usize) -> Self {
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

        let ooo_histogram = PromHistogram::with_opts(
            HistogramOpts::new(
                "prometheus_tsdb_sample_ooo_delta",
                "Delta in seconds by which a sample is considered out of order (reported regardless of OOO time window and whether sample is accepted or not).",
            )
            .buckets(vec![
                600.0,
                1800.0,
                3600.0,
                7200.0,
                10800.0,
                21600.0,
                43200.0,
            ]),
        )
        .expect("valid ooo histogram");
        if let Some(r) = reg {
            let _ = r.register(Box::new(ooo_samples_appended.clone()));
            let _ = r.register(Box::new(ooo_histogram.clone()));
        }

        Self {
            active_appenders: gauge(
                reg,
                "prometheus_tsdb_head_active_appenders",
                "Number of currently active appender transactions.",
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
            wal: crate::metrics::WlogMetrics::new(reg, crate::metrics::WAL_METRICS_PREFIX),
            wbl: crate::metrics::WlogMetrics::new(reg, crate::metrics::WBL_METRICS_PREFIX),
            chunk_write_queue: (chunk_write_queue_size > 0)
                .then(|| crate::metrics::ChunkWriteQueueMetrics::new(reg)),
        }
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
    opts: HeadOptions,
    metrics: HeadMetrics,
    symbols: RwLock<SymbolTable>,
    series: StripeSeries,
    series_creation_mtx: Mutex<()>,
    postings: MemPostings,
    postings_cache: PostingsForMatchersCache,
    tombstones: MemTombstones,
    exemplars: CircularExemplarStorage,
    metadata: UserMetricsMetadata,

    wal: Mutex<Option<WlogWriter>>,
    wbl: Mutex<Option<WlogWriter>>,
    chunk_writer: Mutex<Option<HeadChunksWriter>>,
    chunk_reader: RwLock<Option<HeadChunksReader>>,
    mmap_mtx: Mutex<()>,
    ooo_compact_wbl_cut_seg: Mutex<Option<usize>>,

    min_time: AtomicI64,
    max_time: AtomicI64,
    min_valid_time: AtomicI64,
    min_ooo_time: AtomicI64,
    max_ooo_time: AtomicI64,
    ooo_time_window_ms: AtomicI64,
    float_chunk_encoding: AtomicU8,
    last_series_id: AtomicU64,
    num_series: AtomicU64,
    last_wal_truncation_time: AtomicI64,
    closed: AtomicBool,
    has_any_metadata: AtomicBool,
    deleted_series_since_symbol_gc: AtomicU64,
    wal_expiries: Mutex<HashMap<u64, i64>>,
    wbl_pinned_refs: Mutex<HashSet<u64>>,
    appender_pool: Mutex<Vec<AppenderScratch>>,
    lifecycle_callback: RwLock<Option<Arc<dyn SeriesLifecycleCallback>>>,
    series_gauge: Option<prometheus::Gauge>,
    series_created_total: Option<prometheus::Counter>,
    series_removed_total: Option<prometheus::Counter>,
}

impl Head {
    /// Opens (or initializes) a [`Head`] in `dir`, replaying `chunks_head`, `wal`, and `wbl`.
    pub fn open(
        dir: impl AsRef<Path>,
        mut opts: HeadOptions,
        min_valid_time: i64,
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

        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&chunks_dir)?;
        let _ = delete_temp_checkpoints(&wal_dir);

        let lifecycle_cb = opts.series_lifecycle_callback.clone();
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
        let metrics = HeadMetrics::new(opts.registry.as_deref(), opts.head_chunks_write_queue_size);
        let head = Self {
            dir: dir.clone(),
            opts: opts.clone(),
            metrics,
            symbols: RwLock::new(SymbolTable::new()),
            series: StripeSeries::new(opts.stripe_size),
            series_creation_mtx: Mutex::new(()),
            postings: MemPostings::new_unordered(),
            postings_cache: PostingsForMatchersCache::new(
                PostingsForMatchersCacheConfig {
                    // Rust caches every head PostingsForMatchers call (Go only caches
                    // concurrent / query-sharding calls unless `force`), so entries are
                    // always invalidated on postings changes to keep Go's uncached results.
                    enable_invalidation: true,
                    ..opts.postings_cache.clone()
                },
            ),
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
            ooo_compact_wbl_cut_seg: Mutex::new(None),
            min_time: AtomicI64::new(i64::MAX),
            max_time: AtomicI64::new(i64::MIN),
            min_valid_time: AtomicI64::new(min_valid_time),
            min_ooo_time: AtomicI64::new(i64::MAX),
            max_ooo_time: AtomicI64::new(i64::MIN),
            ooo_time_window_ms: AtomicI64::new(opts.ooo_time_window_ms),
            float_chunk_encoding: AtomicU8::new(float_enc.as_u8()),
            last_series_id: AtomicU64::new(0),
            num_series: AtomicU64::new(0),
            last_wal_truncation_time: AtomicI64::new(i64::MIN),
            closed: AtomicBool::new(false),
            has_any_metadata: AtomicBool::new(false),
            deleted_series_since_symbol_gc: AtomicU64::new(0),
            wal_expiries: Mutex::new(HashMap::new()),
            wbl_pinned_refs: Mutex::new(HashSet::new()),
            appender_pool: Mutex::new(Vec::new()),
            lifecycle_callback: RwLock::new(lifecycle_cb),
            series_gauge,
            series_created_total,
            series_removed_total,
        };

        // Like Go `DB.Open`, the WAL/WBL are opened (cutting a new segment) before the
        // head is initialised, so that a repair operates on the live write log.
        let mut wal_writer =
            WlogWriter::with_segment_size(&wal_dir, opts.wal_segment_size, opts.wal_compression)?;
        wal_writer.set_metrics(head.metrics.wal.clone());
        *head.wal.lock() = Some(wal_writer);

        if opts.ooo_time_window_ms > 0 || wbl_dir.exists() {
            let mut wbl_writer = WlogWriter::with_segment_size(
                &wbl_dir,
                opts.wal_segment_size,
                opts.wal_compression,
            )?;
            wbl_writer.set_metrics(head.metrics.wbl.clone());
            *head.wbl.lock() = Some(wbl_writer);
        }

        head.replay_on_open(&wal_dir, &wbl_dir, &chunks_dir)?;

        let mut chunk_writer = HeadChunksWriter::with_options(
            &chunks_dir,
            opts.max_head_chunk_file_size,
            opts.head_chunks_write_buffer_size,
        )?;
        if let Some(m) = head.metrics.chunk_write_queue.clone() {
            chunk_writer.set_queue_metrics(m);
        }
        *head.chunk_writer.lock() = Some(chunk_writer);
        head.refresh_chunk_reader()?;

        Ok(head)
    }

    fn replay_on_open(
        &self,
        wal_dir: &Path,
        wbl_dir: &Path,
        chunks_dir: &Path,
    ) -> Result<(), HeadError> {
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
                self.remove_corrupted_mmapped_chunks(&mut reader, &err)
            }
        };
        *self.chunk_reader.write() = Some(reader);

        // 2. Replay WAL checkpoint + segments. Like Go `Head.Init`, the first WAL error
        // aborts the replay (the WBL is then NOT replayed in this boot) and `DB.Open`
        // increments `prometheus_tsdb_wal_corruptions_total` and runs `wal.Repair(err)`,
        // which only handles `CorruptionErr`s carrying a segment index and always
        // repairs the WAL directory itself (even for errors read from a checkpoint).
        let mut multi_ref: HashMap<u64, u64> = HashMap::new();
        let wal_res = self.replay_wal_dir(
            wal_dir,
            &mut multi_ref,
            &mut mmapped_map,
            &mut ooo_mmapped_map,
        );
        match wal_res {
            Err(err) => {
                self.metrics.wal_corruptions_total.inc();
                tracing::warn!(err = %err, "Encountered WAL read error, attempting repair");
                self.repair_write_log(&self.wal, err).map_err(|e| {
                    HeadError::Repair(format!("repair corrupted WAL: {e}"))
                })?;
                tracing::info!("Successfully repaired WAL");
            }
            Ok(()) => {
                // 3. Replay WBL segments if present, passing a chunk_writer so overflowing OOO
                // head chunks spill to `chunks_head` instead of dropping.
                if wbl_dir.exists() {
                    if let Some((first_wbl, last_wbl)) = segments_range(wbl_dir)? {
                        let mut r = WlogReader::open_ranges(&[SegmentRange {
                            dir: wbl_dir.to_path_buf(),
                            first: Some(first_wbl),
                            last: Some(last_wbl),
                        }])?;
                        let mut replay_cw = HeadChunksWriter::with_max_file_size(
                            chunks_dir,
                            self.opts.max_head_chunk_file_size,
                        )?;
                        let wbl_res = self.replay_wbl_reader(
                            &mut r,
                            &multi_ref,
                            last_mmap_ref,
                            Some(&mut replay_cw),
                        );
                        let spilled = replay_cw.close().map_err(HeadError::from);
                        spilled?;
                        if let Err(err) = wbl_res {
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
        self.postings_cache.invalidate_all();

        let effective_mvt = self.min_valid_time.load(Ordering::Relaxed);
        self.gc(effective_mvt)?;

        // Clamp min_time to min_valid_time if initialized below it.
        if self.initialized() && self.min_time() < effective_mvt {
            self.min_time.store(effective_mvt, Ordering::Relaxed);
        }
        Ok(())
    }

    /// WAL part of Go `Head.Init`: replays the last checkpoint (if any) and then every WAL
    /// segment after it, stopping at the first error.
    fn replay_wal_dir(
        &self,
        wal_dir: &Path,
        multi_ref: &mut HashMap<u64, u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
    ) -> Result<(), HeadError> {
        let mut start_from = 0usize;
        if let Some((cp_dir, cp_idx)) = last_checkpoint(wal_dir)? {
            // A corrupted checkpoint is passed to the WAL repair like any other error
            // (Go: `backfill checkpoint: %w`).
            let mut r = WlogReader::open_dir(&cp_dir)?;
            self.replay_wal_reader(&mut r, multi_ref, mmapped_map, ooo_mmapped_map)?;
            start_from = cp_idx + 1;
        }
        if let Some((first_seg, last_seg)) = segments_range(wal_dir)? {
            let from = start_from.max(first_seg);
            if from <= last_seg {
                let mut r = WlogReader::open_ranges(&[SegmentRange {
                    dir: wal_dir.to_path_buf(),
                    first: Some(from),
                    last: Some(last_seg),
                }])?;
                self.replay_wal_reader(&mut r, multi_ref, mmapped_map, ooo_mmapped_map)?;
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
                let reader = HeadChunksReader::open_no_repair(self.dir.join("chunks_head"))?;
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

    /// Reclaims dead interned strings from `Head.symbols` (`SymbolTable`) after series churn.
    fn maybe_gc_symbols(&self) {
        let deleted = self.deleted_series_since_symbol_gc.load(Ordering::Acquire);
        let num_series = self.num_series();
        let should_check = if num_series == 0 {
            deleted > 0 && self.symbols.read().len() > 64
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
            self.deleted_series_since_symbol_gc
                .store(0, Ordering::Release);
            return;
        }

        let old_len = syms.len();
        if old_len <= 1 {
            self.deleted_series_since_symbol_gc
                .store(0, Ordering::Release);
            return;
        }

        let mut used = vec![false; old_len];
        used[0] = true; // SymbolId::EMPTY
        self.postings.collect_live_symbols(&mut used);

        let has_meta = self.has_any_metadata.load(Ordering::Acquire);
        if has_meta {
            for stripe in &self.series.stripes {
                let entries: Vec<Arc<SeriesEntry>> =
                    stripe.read().by_id.values().cloned().collect();
                for e in entries {
                    let st = e.inner.lock();
                    if let Some(m) = st.metadata {
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

        let live_count = used.iter().filter(|&&u| u).count();
        if live_count * 4 < old_len * 3 || (old_len > 4096 && live_count + 1024 < old_len) {
            let mut live_bytes = 0usize;
            for (i, &is_used) in used.iter().enumerate().skip(1) {
                if is_used {
                    live_bytes += syms.resolve(SymbolId(i as u32)).len();
                }
            }
            let mut new_syms = SymbolTable::with_capacity(live_count, live_bytes);
            let mut remap = vec![SymbolId::EMPTY; old_len];
            for (i, &is_used) in used.iter().enumerate().skip(1) {
                if is_used {
                    let s = syms.resolve(SymbolId(i as u32));
                    remap[i] = new_syms.intern(s);
                }
            }

            self.postings.remap_symbols(&remap);
            if has_meta {
                for stripe in &self.series.stripes {
                    let entries: Vec<Arc<SeriesEntry>> =
                        stripe.read().by_id.values().cloned().collect();
                    for e in entries {
                        let mut st = e.inner.lock();
                        if let Some(ref mut m) = st.metadata {
                            m.unit = remap[m.unit.0 as usize];
                            m.help = remap[m.help.0 as usize];
                        }
                    }
                }
            }
            *syms = new_syms;
        }
        self.deleted_series_since_symbol_gc
            .store(0, Ordering::Release);
    }

    fn intern_metadata_symbols(
        &self,
        metric_type: MetricType,
        unit: &str,
        help: &str,
    ) -> CompactSeriesMetadata {
        {
            let syms = self.symbols.read();
            if let (Some(u), Some(h)) = (syms.get(unit), syms.get(help)) {
                return CompactSeriesMetadata {
                    metric_type,
                    unit: u,
                    help: h,
                };
            }
        }
        let mut syms = self.symbols.write();
        CompactSeriesMetadata {
            metric_type,
            unit: syms.intern(unit),
            help: syms.intern(help),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn attach_mmapped_chunks(
        &self,
        entry: &SeriesEntry,
        mmc: Vec<MmappedChunk>,
        ooo_mmc: Vec<MmappedChunk>,
        mint: &mut i64,
        maxt: &mut i64,
        ooo_mint: &mut i64,
        ooo_maxt: &mut i64,
    ) {
        if mmc.is_empty() && ooo_mmc.is_empty() {
            return;
        }
        // Go `loadWAL` series record: `chunksCreated.Add(len(mmc) + len(oooMmc))`.
        self.metrics
            .chunks_created((mmc.len() + ooo_mmc.len()) as u64);
        let mut st = entry.inner.lock();
        if !mmc.is_empty() {
            for m in &mmc {
                *mint = (*mint).min(m.min_time);
                *maxt = (*maxt).max(m.max_time);
            }
            let mv = st.mmapped_mut();
            mv.extend(mmc);
            if mv.len() > 1 {
                mv.sort_by_key(|c| (c.min_time, c.max_time));
            }
            entry
                .in_order_max_time
                .store(st.max_time(), Ordering::Release);
        }
        if !ooo_mmc.is_empty() {
            for m in &ooo_mmc {
                *ooo_mint = (*ooo_mint).min(m.min_time);
                *ooo_maxt = (*ooo_maxt).max(m.max_time);
            }
            let ooo = st
                .ooo
                .get_or_insert_with(|| Box::new(SeriesOooState::default()));
            ooo.ooo_mmapped_chunks.extend(ooo_mmc);
        }
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
        let mut state = entry.inner.lock();
        if s.t <= state.mm_max_time() {
            return Ok(());
        }
        if is_stale_nan(s.v) {
            match state.latest_in_order_encoding() {
                Some(Encoding::Histogram | Encoding::HistogramSt) => {
                    let h = Histogram {
                        sum: s.v,
                        ..Histogram::default()
                    };
                    if self.metrics.track_append(state.append_histogram_in_order(
                            s.st,
                            s.t,
                            &h,
                            self.opts.chunk_range_ms,
                            self.opts.enable_histogram_st_encoding,
                        )?) {
                        *mint = (*mint).min(s.t);
                        *maxt = (*maxt).max(s.t);
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
                            s.st,
                            s.t,
                            &fh,
                            self.opts.chunk_range_ms,
                            self.opts.enable_histogram_st_encoding,
                        )?) {
                        *mint = (*mint).min(s.t);
                        *maxt = (*maxt).max(s.t);
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
                s.st,
                s.t,
                s.v,
                self.opts.chunk_range_ms,
                self.opts.samples_per_chunk,
                self.float_chunk_encoding(),
            )?) {
            *mint = (*mint).min(s.t);
            *maxt = (*maxt).max(s.t);
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
        let mut state = entry.inner.lock();
        if hs.t <= state.mm_max_time() {
            return Ok(());
        }
        if self.metrics.track_append(state.append_histogram_in_order(
                hs.st,
                hs.t,
                &hs.h,
                self.opts.chunk_range_ms,
                self.opts.enable_histogram_st_encoding,
            )?) {
            *mint = (*mint).min(hs.t);
            *maxt = (*maxt).max(hs.t);
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
        let mut state = entry.inner.lock();
        if fhs.t <= state.mm_max_time() {
            return Ok(());
        }
        if self.metrics.track_append(state.append_float_histogram_in_order(
                fhs.st,
                fhs.t,
                &fhs.fh,
                self.opts.chunk_range_ms,
                self.opts.enable_histogram_st_encoding,
            )?) {
            *mint = (*mint).min(fhs.t);
            *maxt = (*maxt).max(fhs.t);
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
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
    ) -> Result<(), HeadError> {
        let unknown = UnknownRefs::default();
        if self.opts.wal_replay_concurrency > 1 {
            self.replay_wal_reader_parallel(r, multi_ref, mmapped_map, ooo_mmapped_map, &unknown)?;
        } else {
            self.replay_wal_reader_sequential(r, multi_ref, mmapped_map, ooo_mmapped_map, &unknown)?;
        }
        self.report_wal_unknown_refs(&unknown, multi_ref);
        Ok(())
    }

    fn replay_wal_reader_sequential<R: io::Read + SegmentTracker + Send>(
        &self,
        r: &mut WlogReader<R>,
        multi_ref: &mut HashMap<u64, u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        unknown: &UnknownRefs,
    ) -> Result<(), HeadError> {

        const WAL_POSTINGS_BATCH: usize = 4096;

        let dec = RecordDecoder::new();
        let mut min_valid_time = self.min_valid_time.load(Ordering::Relaxed);
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
                        |entry, mmc, ooo_mmc| {
                            self.attach_mmapped_chunks(
                                &entry,
                                mmc,
                                ooo_mmc,
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
                    for mut stone in stones {
                        self.bump_last_series_id(stone.ref_id);
                        if stone.mint == i64::MIN && stone.maxt == i64::MAX {
                            let mut ref_id = stone.ref_id;
                            if let Some(&mapped) = multi_ref.get(&ref_id) {
                                ref_id = mapped;
                            }
                            if let Some(entry) = self.series.get_by_id(ref_id) {
                                self.series.unlink_hash(entry.hash, ref_id);
                                to_delete.push(entry);
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
                            self.intern_metadata_symbols(metric_type, &m.unit, &m.help);
                        self.has_any_metadata.store(true, Ordering::Release);
                        let mut st = entry.inner.lock();
                        st.metadata = Some(compact_meta);
                    }
                }
                DecodedRecord::MinValidTime(t) => {
                    if t > min_valid_time {
                        min_valid_time = t;
                        self.min_valid_time.fetch_max(t, Ordering::AcqRel);
                    }
                }
                DecodedRecord::MmapMarkers(_) | DecodedRecord::Unknown(_) => {}
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
        unknown: &UnknownRefs,
    ) -> Result<(), HeadError> {
        enum WalSampleJob {
            Float(RefSample),
            Histogram(RefHistogramSample),
            FloatHistogram(RefFloatHistogramSample),
        }

        enum WalWorkerMsg {
            AttachMmapped {
                entry: Arc<SeriesEntry>,
                mmc: Vec<MmappedChunk>,
                ooo_mmc: Vec<MmappedChunk>,
            },
            SampleJobs(Vec<WalSampleJob>),
            AddTombstone {
                ref_id: u64,
                interval: Interval,
            },
        }

        enum ParallelDecodedRecord {
            Series(Vec<RefSeries>, Vec<u64>),
            Other(DecodedRecord),
        }

        const WAL_SHARD_BATCH_MAX: usize = 5000;
        const WAL_DECODE_BATCH_RECORDS: usize = 64;
        const WAL_DECODE_BATCH_BYTES: usize = 1024 * 1024;
        const WAL_SERIES_PARALLEL_BATCH_MAX: usize = 32_768;

        let max_threads = std::thread::available_parallelism()
            .map_or(8, |n| n.get())
            .clamp(2, 32);
        let workers = self.opts.wal_replay_concurrency.min(max_threads).max(2);
        let dec_workers = workers.min(8);
        let mut min_valid_time = self.min_valid_time.load(Ordering::Relaxed);
        let series_pool = mimir_common::thread_pool::ScopedWorkerPool::new("wal-series", workers);

        type RawEntry = (CompressionType, Vec<u8>, Option<PathBuf>, Option<usize>, usize);
        type RawBatch = (u64, Vec<RawEntry>);
        // A batch carries the records decoded before a decode/decompress error (if any),
        // so that the coordinator applies them in WAL order and stops exactly at the
        // corrupt record, like Go `loadWAL`.
        type DecodedBatch = (u64, Vec<ParallelDecodedRecord>, Option<HeadError>);

        // Channel capacity > dec_workers * dec_queue_cap ensures zero deadlock when reordering.
        let (decoded_tx, decoded_rx) =
            std::sync::mpsc::sync_channel::<Result<DecodedBatch, HeadError>>(dec_workers * 6);
        let mut dec_txs = Vec::with_capacity(dec_workers);
        let mut dec_rxs = Vec::with_capacity(dec_workers);
        for _ in 0..dec_workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<RawBatch>(4);
            dec_txs.push(tx);
            dec_rxs.push(rx);
        }

        let mut worker_txs = Vec::with_capacity(workers);
        let mut worker_rxs = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = std::sync::mpsc::sync_channel::<WalWorkerMsg>(64);
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
                            } => {
                                self.attach_mmapped_chunks(
                                    &entry,
                                    mmc,
                                    ooo_mmc,
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
                                                    &entry, hs, &mut mint, &mut maxt,
                                                )?;
                                            } else {
                                                unknown.miss(&unknown.histograms, hs.ref_id);
                                            }
                                        }
                                        WalSampleJob::FloatHistogram(fhs) => {
                                            if let Some(entry) = self.series.get_by_id(fhs.ref_id) {
                                                self.replay_one_float_histogram(
                                                    &entry, fhs, &mut mint, &mut maxt,
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
            let mut pending_deletes: Vec<Arc<SeriesEntry>> = Vec::new();
            let mut pending_series: Vec<(RefSeries, u64)> = Vec::new();
            let mut reorder: BTreeMap<u64, (Vec<ParallelDecodedRecord>, Option<HeadError>)> = BTreeMap::new();
            let mut next_batch_id = 0u64;

            let flush_series_batch = |pending: &mut Vec<(RefSeries, u64)>,
                                      multi_ref: &mut HashMap<u64, u64>,
                                      mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
                                      ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
                                      failed_outer: &mut bool|
             -> Result<(), HeadError> {
                if pending.is_empty() {
                    return Ok(());
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
                type AttachItem = (Arc<SeriesEntry>, Vec<MmappedChunk>, Vec<MmappedChunk>);
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

                    for (rs, hash, mmc, ooo_mmc) in shard {
                        if let Some(existing) =
                            self.series.get_by_hash_and_labels(hash, &rs.labels)
                        {
                            w_multi.push((rs.ref_id, existing.ref_id));
                            if !mmc.is_empty() || !ooo_mmc.is_empty() {
                                w_attach.push((existing, mmc, ooo_mmc));
                            }
                            continue;
                        }
                        let symbol_labels = self.intern_labels_read_first(&rs.labels);
                        let entry = Arc::new(SeriesEntry {
                            ref_id: rs.ref_id,
                            hash,
                            labels: rs.labels,
                            active_cache: AtomicU64::new(0),
                            in_order_max_time: AtomicI64::new(i64::MIN),
                            pending_commits: AtomicU32::new(0),
                            gced: AtomicBool::new(false),
                            inner: Mutex::new(SeriesState::new()),
                        });
                        self.series.insert(Arc::clone(&entry));
                        w_postings.push((rs.ref_id, symbol_labels));
                        *w_created += 1;
                        if let Some(ref cb) = cb_opt {
                            cb.post_creation(rs.ref_id, &entry.labels);
                        }
                        if !mmc.is_empty() || !ooo_mmc.is_empty() {
                            w_attach.push((entry, mmc, ooo_mmc));
                        }
                    }
                });

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
                    for (entry, mmc, ooo_mmc) in avec {
                        let idx = (entry.ref_id as usize) % workers;
                        if worker_txs[idx]
                            .send(WalWorkerMsg::AttachMmapped {
                                entry,
                                mmc,
                                ooo_mmc,
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
                                            let idx = (s.ref_id as usize) % workers;
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
                                            job_shards[idx].push(WalSampleJob::Histogram(hs));
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
                                            job_shards[idx].push(WalSampleJob::FloatHistogram(fhs));
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
                                        for mut stone in stones {
                                            self.bump_last_series_id(stone.ref_id);
                                            if stone.mint == i64::MIN && stone.maxt == i64::MAX {
                                                let mut ref_id = stone.ref_id;
                                                if let Some(&mapped) = multi_ref.get(&ref_id) {
                                                    ref_id = mapped;
                                                }
                                                if let Some(entry) = self.series.get_by_id(ref_id) {
                                                    self.series.unlink_hash(entry.hash, ref_id);
                                                    pending_deletes.push(entry);
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
                                            let Some(entry) = self.series.get_by_id(m.ref_id)
                                            else {
                                                unknown.miss(&unknown.metadata, m.ref_id);
                                                continue;
                                            };
                                            let metric_type = MetricType::from_u8(m.metric_type);
                                            let compact_meta = self.intern_metadata_symbols(
                                                metric_type,
                                                &m.unit,
                                                &m.help,
                                            );
                                            self.has_any_metadata.store(true, Ordering::Release);
                                            let mut st = entry.inner.lock();
                                            st.metadata = Some(compact_meta);
                                        }
                                    }
                                    DecodedRecord::MinValidTime(t) => {
                                        if t > min_valid_time {
                                            min_valid_time = t;
                                            self.min_valid_time.fetch_max(t, Ordering::AcqRel);
                                        }
                                    }
                                    DecodedRecord::MmapMarkers(_) | DecodedRecord::Unknown(_) => {}
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
            if !pending_deletes.is_empty() {
                self.delete_series_entries(&pending_deletes);
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
                            if let Some(ref mut ooo) = st.ooo {
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
                            let mut st = entry.inner.lock();
                            let (ok, created, _) = st.insert_ooo(
                                entry.ref_id,
                                s.st,
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
                            let mut st = entry.inner.lock();
                            let (ok, created, _) = st.insert_ooo(
                                entry.ref_id,
                                hs.st,
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
                            let mut st = entry.inner.lock();
                            let (ok, created, _) = st.insert_ooo(
                                entry.ref_id,
                                fhs.st,
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

    /// Go `loadWAL` unknown-reference reporting (`found_series` / `truly_missing_series`
    /// are the Mimir-specific diagnostics).
    fn report_wal_unknown_refs(&self, u: &UnknownRefs, multi_ref: &HashMap<u64, u64>) {
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
            }
        }
        let truly_missing = missing.len() as u64 - found;
        let v = &self.metrics.wal_replay_unknown_refs;
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
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    #[inline]
    pub fn options(&self) -> &HeadOptions {
        &self.opts
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
        if clamped > 0 {
            let mut wbl_guard = self.wbl.lock();
            if wbl_guard.is_none() {
                let wbl_dir = self.dir.join(WBL_DIR_NAME);
                let mut wbl_writer = WlogWriter::with_segment_size(
                    &wbl_dir,
                    self.opts.wal_segment_size,
                    self.opts.wal_compression,
                )?;
                wbl_writer.set_metrics(self.metrics.wbl.clone());
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

    /// Sets or replaces the series lifecycle callback.
    pub fn set_lifecycle_callback(&self, cb: Option<Arc<dyn SeriesLifecycleCallback>>) {
        *self.lifecycle_callback.write() = cb;
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
        for stripe in &self.series.stripes {
            let entries: Vec<Arc<SeriesEntry>> = stripe.read().by_id.values().cloned().collect();
            for entry in entries {
                f(entry.ref_id, &entry.labels);
            }
        }
    }

    /// Returns postings list for a single `(name, value)` pair.
    pub fn postings_for_label_value(&self, name: &str, value: &str) -> Vec<u64> {
        let ids = {
            let syms = self.symbols.read();
            syms.get(name).zip(syms.get(value))
        };
        let Some((nid, vid)) = ids else {
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
        let ids = {
            let syms = self.symbols.read();
            syms.get(name).zip(syms.get(value))
        };
        let Some((nid, vid)) = ids else {
            return R::default();
        };
        self.postings.visit_postings_for_id(nid, vid, f)
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
        self.max_time() - self.min_time() > self.opts.chunk_range_ms / 2 * 3
    }

    fn init_time(&self, t: i64) {
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
            while self.min_time.load(Ordering::Acquire) == i64::MAX {
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

        let _creation_guard = self.series_creation_mtx.lock();
        // Double-check after acquiring creation lock.
        if let Some(existing) = self.get_by_sorted_pairs(hash, pairs) {
            return Ok((existing, false));
        }

        let cb_opt = self.lifecycle_callback.read().clone();
        if let Some(ref cb) = cb_opt {
            cb.pre_creation(pairs)?;
        }

        let symbol_labels = self.intern_sorted_pairs_read_first(pairs);
        let metric_name = pairs
            .iter()
            .find(|&&(k, _)| k == METRIC_NAME)
            .map_or("", |&(_, v)| v);
        let labels = Labels::from_sorted_slice(pairs);
        let ref_id = self.last_series_id.fetch_add(1, Ordering::AcqRel) + 1;
        let entry = Arc::new(SeriesEntry {
            ref_id,
            hash,
            labels,
            active_cache: AtomicU64::new(0),
            in_order_max_time: AtomicI64::new(i64::MIN),
            pending_commits: AtomicU32::new(init_pending),
            gced: AtomicBool::new(false),
            inner: Mutex::new(SeriesState::new()),
        });
        self.series.insert(Arc::clone(&entry));
        self.postings.add(ref_id, &symbol_labels);
        self.postings_cache.invalidate_metric(metric_name);
        self.num_series.fetch_add(1, Ordering::Relaxed);
        if let Some(ref g) = self.series_gauge {
            g.inc();
        }
        if let Some(ref c) = self.series_created_total {
            c.inc();
        }
        if let Some(ref cb) = cb_opt {
            cb.post_creation(ref_id, &entry.labels);
        }
        Ok((entry, true))
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

        let _creation_guard = self.series_creation_mtx.lock();
        if let Some(existing) = self.series.get_by_encoded_labels(hash, encoded_labels) {
            return Ok((existing, false));
        }

        let cb_opt = self.lifecycle_callback.read().clone();
        if let Some(ref cb) = cb_opt {
            let pairs_tmp: SmallVec<[(&str, &str); 24]> =
                iter_stringlabels(encoded_labels).collect();
            cb.pre_creation(&pairs_tmp)?;
        }

        let symbol_labels = self.intern_encoded_labels_read_first(encoded_labels);
        let labels = Labels::from_raw_stringlabels_bytes(Bytes::copy_from_slice(encoded_labels));
        let ref_id = self.last_series_id.fetch_add(1, Ordering::AcqRel) + 1;
        let entry = Arc::new(SeriesEntry {
            ref_id,
            hash,
            labels,
            active_cache: AtomicU64::new(0),
            in_order_max_time: AtomicI64::new(i64::MIN),
            pending_commits: AtomicU32::new(init_pending),
            gced: AtomicBool::new(false),
            inner: Mutex::new(SeriesState::new()),
        });
        self.series.insert(Arc::clone(&entry));
        self.postings.add(ref_id, &symbol_labels);
        self.postings_cache
            .invalidate_metric(entry.labels.get(METRIC_NAME));
        self.num_series.fetch_add(1, Ordering::Relaxed);
        if let Some(ref g) = self.series_gauge {
            g.inc();
        }
        if let Some(ref c) = self.series_created_total {
            c.inc();
        }
        if let Some(ref cb) = cb_opt {
            cb.post_creation(ref_id, &entry.labels);
        }
        Ok((entry, true))
    }

    fn intern_encoded_labels_read_first(&self, encoded_labels: &[u8]) -> SymbolLabels {
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
                .map(|(k, v)| (k.unwrap(), v.unwrap()))
                .collect()
        } else {
            let mut syms = self.symbols.write();
            let mut out = SmallVec::with_capacity(resolved.len());
            for ((k, v), (kid, vid)) in iter_stringlabels(encoded_labels).zip(resolved) {
                let kf = match kid {
                    Some(id) => id,
                    None => syms.intern(k),
                };
                let vf = match vid {
                    Some(id) => id,
                    None => syms.intern(v),
                };
                out.push((kf, vf));
            }
            out
        };
        SymbolLabels::from_sorted_ids(interned)
    }

    fn intern_sorted_pairs_read_first(&self, pairs: &[(&str, &str)]) -> SymbolLabels {
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
                .map(|(k, v)| (k.unwrap(), v.unwrap()))
                .collect()
        } else {
            let mut syms = self.symbols.write();
            let mut out = SmallVec::with_capacity(pairs.len());
            for (i, &(k, v)) in pairs.iter().enumerate() {
                let (kid, vid) = resolved[i];
                let kf = match kid {
                    Some(id) => id,
                    None => syms.intern(k),
                };
                let vf = match vid {
                    Some(id) => id,
                    None => syms.intern(v),
                };
                out.push((kf, vf));
            }
            out
        };
        SymbolLabels::from_sorted_ids(interned)
    }

    fn intern_labels_read_first(&self, labels: &Labels) -> SymbolLabels {
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
                .map(|(k, v)| (k.unwrap(), v.unwrap()))
                .collect()
        } else {
            let mut syms = self.symbols.write();
            let mut out = SmallVec::with_capacity(resolved.len());
            for ((k, v), (kid, vid)) in labels.iter().zip(resolved) {
                let kf = match kid {
                    Some(id) => id,
                    None => syms.intern(k),
                };
                let vf = match vid {
                    Some(id) => id,
                    None => syms.intern(v),
                };
                out.push((kf, vf));
            }
            out
        };
        SymbolLabels::from_sorted_ids(interned)
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
        F: FnMut(Arc<SeriesEntry>, Vec<MmappedChunk>, Vec<MmappedChunk>) -> Result<(), HeadError>,
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
                on_series(existing, mmc, ooo_mmc)?;
                continue;
            }

            let symbol_labels = SymbolLabels::from_labels(&mut syms, &rs.labels);
            let entry = Arc::new(SeriesEntry {
                ref_id: rs.ref_id,
                hash,
                labels: rs.labels.clone(),
                active_cache: AtomicU64::new(0),
                in_order_max_time: AtomicI64::new(i64::MIN),
                pending_commits: AtomicU32::new(0),
                gced: AtomicBool::new(false),
                inner: Mutex::new(SeriesState::new()),
            });
            self.series.insert(Arc::clone(&entry));
            pending_postings.push((rs.ref_id, symbol_labels));
            created_count += 1;
            if let Some(ref cb) = cb_opt {
                cb.post_creation(rs.ref_id, &entry.labels);
            }
            let mmc = mmapped_map.remove(&rs.ref_id).unwrap_or_default();
            let ooo_mmc = ooo_mmapped_map.remove(&rs.ref_id).unwrap_or_default();
            on_series(entry, mmc, ooo_mmc)?;
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
        let symbol_labels = self.intern_labels_read_first(labels);
        let entry = Arc::new(SeriesEntry {
            ref_id,
            hash,
            labels: labels.clone(),
            active_cache: AtomicU64::new(0),
            in_order_max_time: AtomicI64::new(i64::MIN),
            pending_commits: AtomicU32::new(0),
            gced: AtomicBool::new(false),
            inner: Mutex::new(SeriesState::new()),
        });
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
        let symbol_labels = self.intern_labels_read_first(labels);
        let metric_name = labels.get(METRIC_NAME);
        let entry = Arc::new(SeriesEntry {
            ref_id,
            hash,
            labels: labels.clone(),
            active_cache: AtomicU64::new(0),
            in_order_max_time: AtomicI64::new(i64::MIN),
            pending_commits: AtomicU32::new(0),
            gced: AtomicBool::new(false),
            inner: Mutex::new(SeriesState::new()),
        });
        self.series.insert(Arc::clone(&entry));
        self.postings.add(ref_id, &symbol_labels);
        self.postings_cache.invalidate_metric(metric_name);
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
        {
            let syms = self.symbols.read();
            for e in &entries {
                for (k, v) in e.labels.iter() {
                    if let (Some(nid), Some(vid)) = (syms.get(k), syms.get(v)) {
                        affected_set.insert((nid, vid));
                    }
                }
            }
        }
        let affected: Vec<(SymbolId, SymbolId)> = affected_set.into_iter().collect();

        let mut chunks_removed = 0u64;
        for e in entries {
            self.series.remove(&e);
            e.gced.store(true, Ordering::Release);
            deleted.insert(e.ref_id);
            if has_cb {
                deleted_pairs.push((e.ref_id, e.labels.clone()));
            }
            if entries_len <= 64 {
                small_metric_names.push(e.labels.get(METRIC_NAME).to_owned());
            }
            {
                let mut st = e.inner.lock();
                chunks_removed += (st.mmapped().len() + st.head_chunks.len()) as u64;
                st.mmapped_chunks = None;
                st.set_gced(true);
                if let Some(keep_until) = {
                    let mt = st.max_time();
                    (mt != i64::MIN).then_some(mt)
                } {
                    self.update_wal_expiry(e.ref_id, keep_until);
                }
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
        {
            // Keep `symbols` read-locked until `postings.delete` finishes so a concurrent
            // `maybe_gc_symbols` cannot remap `SymbolId`s between lookup and deletion.
            let syms = self.symbols.read();
            let _ = &*syms;
            self.postings.delete(&deleted, &affected);
        }
        drop(affected);
        if entries_len > 64 {
            self.postings_cache.invalidate_all();
        } else {
            for m in &small_metric_names {
                self.postings_cache.invalidate_metric(m);
            }
        }
        self.tombstones.delete_tombstones(&deleted);
        drop(deleted);
        if let Some(ref cb) = *self.lifecycle_callback.read() {
            cb.post_deletion(&deleted_pairs);
        }
    }

    /// Cleans up postings, metrics, tombstones, WAL expiries, and lifecycle callbacks for series
    /// that were already atomically marked GCed and unlinked from `StripeSeries`.
    fn finish_deleted_series_cleanup(&self, entries: Vec<(Arc<SeriesEntry>, i64)>) {
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
            self.postings_cache.invalidate_all();
        } else {
            for m in &small_metric_names {
                self.postings_cache.invalidate_metric(m);
            }
        }
        self.tombstones.delete_tombstones(&deleted);
        drop(deleted);
        if let Some(ref cb) = *self.lifecycle_callback.read() {
            cb.post_deletion(&deleted_pairs);
        }
    }

    fn promote_all_pending_ooo_mmapped(&self) {
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        for stripe in &self.series.stripes {
            stripe_buf.clear();
            {
                let guard = stripe.read();
                if guard.by_id.is_empty() {
                    continue;
                }
                stripe_buf.extend(guard.by_id.values().cloned());
            }
            for entry in stripe_buf.drain(..) {
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo {
                    if !ooo.ooo_pending_mmapped.is_empty() {
                        for (mc, _) in ooo.ooo_pending_mmapped.drain(..) {
                            ooo.ooo_mmapped_chunks.push(mc);
                        }
                    }
                }
            }
        }
    }

    /// Creates a new [`HeadAppender`] for transactional batched writes.
    pub fn appender(&self) -> HeadAppender<'_> {
        self.appender_with_capacity(0)
    }

    pub fn appender_with_capacity(&self, series_hint: usize) -> HeadAppender<'_> {
        self.metrics.active_appenders.inc();
        let min_valid = self.appendable_min_valid_time();
        let head_maxt = self.max_time();
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
            batch_last_sample: scratch.batch_last_sample,
            types_in_batch: scratch.types_in_batch,
            batch_boundaries: scratch.batch_boundaries,
            indexed_floats_len: 0,
            indexed_hists_len: 0,
            indexed_fhists_len: 0,
            floats_committed: 0,
            hists_committed: 0,
            fhists_committed: 0,
            wal_buf: scratch.wal_buf,
            closed: false,
        }
    }

    /// Flushes all sealed head chunks (`head_chunks[..len-1]`) to `HeadChunksWriter` (`chunks_head/`).
    pub fn mmap_head_chunks(&self) -> Result<usize, HeadChunksError> {
        let _mmap_guard = self.mmap_mtx.lock();
        let mut count = 0usize;
        let mut max_mmapped_chunks = 0usize;
        let mut pending: Vec<(Arc<SeriesEntry>, Vec<MmappedChunk>)> = Vec::new();
        for stripe in &self.series.stripes {
            let entries: Vec<Arc<SeriesEntry>> = stripe.read().by_id.values().cloned().collect();
            for entry in entries {
                // Lock order: entry.inner -> chunk_writer (matching commit()).
                let mut st = entry.inner.lock();
                if st.head_chunks.len() > 1 {
                    let mut writer_guard = self.chunk_writer.lock();
                    let Some(ref mut writer) = *writer_guard else {
                        return Ok(count);
                    };
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
                    writer.flush()?;
                }
            }
            // Refresh mmap reader BEFORE exposing new mmapped_chunks on series state,
            // so concurrent readers never observe a ChunkDiskMapperRef not yet in chunk_reader.
            self.refresh_chunk_reader()?;
            for (entry, mmapped) in pending {
                let mut st = entry.inner.lock();
                let mut matched = 0usize;
                for mc in mmapped {
                    if st.head_chunks.len() > 1
                        && st.head_chunks[0].min_time == mc.min_time
                        && st.head_chunks[0].max_time == mc.max_time
                    {
                        st.head_chunks.remove(0);
                        st.mmapped_mut().push(mc);
                        matched += 1;
                    }
                }
                if matched > 0 && st.mmapped().len() > 1 {
                    st.mmapped_mut().sort_by_key(|c| (c.min_time, c.max_time));
                }
            }
        }
        // Go `mmapHeadChunks`.
        self.metrics
            .head_chunks_max_mmapped
            .set(max_mmapped_chunks as f64);
        self.metrics.mmap_chunks_total.inc_by(count as f64);
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
            {
                let guard = stripe.read();
                if guard.by_id.is_empty() {
                    continue;
                }
                stripe_buf.extend(guard.by_id.values().cloned());
            }
            for entry in stripe_buf.drain(..) {
                let st = entry.inner.lock();
                if !has_tombs {
                    let has_overlap = st
                        .mmapped()
                        .iter()
                        .any(|mc| mc.overlaps_closed_interval(mint, maxt_inclusive))
                        || st
                            .head_chunks
                            .iter()
                            .any(|hc| hc.overlaps_closed_interval(mint, maxt_inclusive));
                    if has_overlap {
                        out.push((entry.ref_id, entry.labels.clone()));
                    }
                } else {
                    self.tombstones.get_into(entry.ref_id, &mut tombs_buf);
                    let has_overlap = st.mmapped().iter().any(|mc| {
                        mc.overlaps_closed_interval(mint, maxt_inclusive)
                            && !is_completely_deleted(&tombs_buf, mc.min_time, mc.max_time)
                    }) || st.head_chunks.iter().any(|hc| {
                        hc.overlaps_closed_interval(mint, maxt_inclusive)
                            && !is_completely_deleted(&tombs_buf, hc.min_time, hc.max_time)
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
        let Some(entry) = self.series.get_by_id(ref_id) else {
            return Ok(());
        };
        let mut mmapped_refs: SmallVec<[MmappedChunk; 8]> = SmallVec::new();
        {
            let st = entry.inner.lock();
            for mc in st.mmapped() {
                if mc.overlaps_closed_interval(mint, maxt_inclusive) {
                    mmapped_refs.push(*mc);
                }
            }
            for hc in &st.head_chunks {
                if hc.overlaps_closed_interval(mint, maxt_inclusive) {
                    out_chunks.push(ChunkMeta {
                        ref_id: 0,
                        min_time: hc.min_time,
                        max_time: hc.max_time,
                        chunk: Some(hc.chunk.clone()),
                    });
                }
            }
        }
        if !mmapped_refs.is_empty() {
            let chunk_reader_guard = self.chunk_reader.read();
            if let Some(r) = chunk_reader_guard.as_ref() {
                for mc in mmapped_refs {
                    let enc = r.chunk(mc.ref_id)?;
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
        let mut mmapped_any = false;
        let mut ooo_markers: Vec<RefMmapMarker> = Vec::new();
        let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
        let use_hist_st = self.opts.enable_histogram_st_encoding;
        let mut pending_entries: Vec<Arc<SeriesEntry>> = Vec::new();
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);

        for stripe in &self.series.stripes {
            stripe_buf.clear();
            {
                let guard = stripe.read();
                if guard.by_id.is_empty() {
                    continue;
                }
                stripe_buf.extend(guard.by_id.values().cloned());
            }
            for entry in stripe_buf.drain(..) {
                // Lock order: entry.inner -> chunk_writer (matching commit()).
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo {
                    if let Some(ohc) = ooo.ooo_head_chunk.take() {
                        let cut_chunks = ohc.chunk.to_encoded_chunks(
                            i64::MIN,
                            i64::MAX,
                            use_xor2,
                            use_hist_st,
                        )?;
                        if !cut_chunks.is_empty() {
                            let mut writer_guard = self.chunk_writer.lock();
                            if let Some(ref mut writer) = *writer_guard {
                                for cc in cut_chunks {
                                    let r = writer.write_chunk(
                                        entry.ref_id,
                                        cc.min_time,
                                        cc.max_time,
                                        &cc.chunk,
                                        true,
                                    )?;
                                    let mc = MmappedChunk {
                                        ref_id: r,
                                        min_time: cc.min_time,
                                        max_time: cc.max_time,
                                        num_samples: cc.chunk.num_samples() as u16,
                                        encoding: cc.chunk.encoding(),
                                    };
                                    ooo.ooo_pending_mmapped.push((mc, cc.chunk));
                                    ooo_markers.push(RefMmapMarker {
                                        ref_id: entry.ref_id,
                                        mmap_ref: r.as_u64(),
                                    });
                                    mmapped_any = true;
                                }
                                pending_entries.push(Arc::clone(&entry));
                            } else {
                                ooo.ooo_head_chunk = Some(ohc);
                            }
                        }
                    }
                }
            }
        }

        if mmapped_any {
            let mut writer_guard = self.chunk_writer.lock();
            if let Some(ref mut writer) = *writer_guard {
                writer.flush()?;
            }
        }

        {
            let mut wbl_guard = self.wbl.lock();
            if let Some(ref mut wbl) = *wbl_guard {
                if !ooo_markers.is_empty() {
                    let enc = RecordEncoder::new(self.opts.enable_st_storage);
                    let rec = enc.mmap_markers(&ooo_markers, Vec::new());
                    wbl.log(&[&rec])?;
                }
                let next_seg = wbl.next_segment()?;
                *self.ooo_compact_wbl_cut_seg.lock() = Some(next_seg);
            }
        }

        if mmapped_any {
            self.refresh_chunk_reader()?;
            for entry in pending_entries {
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo {
                    for (mc, _) in ooo.ooo_pending_mmapped.drain(..) {
                        ooo.ooo_mmapped_chunks.push(mc);
                    }
                }
            }
        }

        let mut out = Vec::new();
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        for stripe in &self.series.stripes {
            stripe_buf.clear();
            {
                let guard = stripe.read();
                if guard.by_id.is_empty() {
                    continue;
                }
                stripe_buf.extend(guard.by_id.values().cloned());
            }
            for entry in stripe_buf.drain(..) {
                let st = entry.inner.lock();
                let Some(ref ooo) = st.ooo else {
                    continue;
                };
                let mut has_any = false;
                for mc in &ooo.ooo_mmapped_chunks {
                    mint = mint.min(mc.min_time);
                    maxt = maxt.max(mc.max_time);
                    has_any = true;
                }
                for (mc, _) in &ooo.ooo_pending_mmapped {
                    mint = mint.min(mc.min_time);
                    maxt = maxt.max(mc.max_time);
                    has_any = true;
                }
                if let Some(ref ohc) = ooo.ooo_head_chunk {
                    mint = mint.min(ohc.min_time);
                    maxt = maxt.max(ohc.max_time);
                    has_any = true;
                }
                if has_any {
                    out.push((entry.ref_id, entry.labels.clone()));
                }
            }
        }

        Ok((out, mint, maxt))
    }

    /// Loads OOO chunks and tombstones for a single `ref_id` into caller-reused buffers.
    pub fn load_series_ooo_chunks_into(
        &self,
        ref_id: u64,
        out_chunks: &mut Vec<ChunkMeta>,
        out_tombs: &mut Vec<Interval>,
    ) -> Result<(), HeadError> {
        let Some(entry) = self.series.get_by_id(ref_id) else {
            return Ok(());
        };
        let mut mmapped_refs: SmallVec<[MmappedChunk; 8]> = SmallVec::new();
        {
            let st = entry.inner.lock();
            let Some(ref ooo) = st.ooo else {
                return Ok(());
            };
            for mc in &ooo.ooo_mmapped_chunks {
                mmapped_refs.push(*mc);
            }
            for (mc, chk) in &ooo.ooo_pending_mmapped {
                out_chunks.push(ChunkMeta {
                    ref_id: mc.ref_id.as_u64(),
                    min_time: mc.min_time,
                    max_time: mc.max_time,
                    chunk: Some(chk.clone()),
                });
            }
            if let Some(ref ohc) = ooo.ooo_head_chunk {
                let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
                let use_hist_st = self.opts.enable_histogram_st_encoding;
                for cc in ohc
                    .chunk
                    .to_encoded_chunks(i64::MIN, i64::MAX, use_xor2, use_hist_st)?
                {
                    out_chunks.push(ChunkMeta {
                        ref_id: 0,
                        min_time: cc.min_time,
                        max_time: cc.max_time,
                        chunk: Some(cc.chunk),
                    });
                }
            }
        }
        if !mmapped_refs.is_empty() {
            let chunk_reader_guard = self.chunk_reader.read();
            if let Some(r) = chunk_reader_guard.as_ref() {
                for mc in mmapped_refs {
                    let enc = r.chunk(mc.ref_id)?;
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

    /// Collects lightweight `(ref_id, Labels)` candidates for streaming selected-series compaction.
    pub fn collect_selected_compaction_candidates(
        &self,
        refs: &[u64],
    ) -> Result<CompactionCandidatesRange, HeadError> {
        let mut out = Vec::with_capacity(refs.len());
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        for &ref_id in refs {
            let Some(entry) = self.series.get_by_id(ref_id) else {
                continue;
            };
            let st = entry.inner.lock();
            if st.ooo.as_ref().is_some_and(|o| {
                o.ooo_head_chunk.is_some()
                    || !o.ooo_mmapped_chunks.is_empty()
                    || !o.ooo_pending_mmapped.is_empty()
            }) {
                continue;
            }
            let mut has_any = false;
            for mc in st.mmapped() {
                mint = mint.min(mc.min_time);
                maxt = maxt.max(mc.max_time);
                has_any = true;
            }
            for hc in &st.head_chunks {
                mint = mint.min(hc.min_time);
                maxt = maxt.max(hc.max_time);
                has_any = true;
            }
            if has_any {
                out.push((entry.ref_id, entry.labels.clone()));
            }
        }
        Ok((out, mint, maxt))
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
            for hc in &st.head_chunks {
                if hc.overlaps_closed_interval(mint, maxt_inclusive) {
                    head_chks.push(ChunkMeta {
                        ref_id: 0,
                        min_time: hc.min_time,
                        max_time: hc.max_time,
                        chunk: Some(hc.chunk.clone()),
                    });
                }
            }
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
            for mc in ex.mmapped_refs {
                if let Some(r) = chunk_reader {
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
        let mut ooo_markers: Vec<RefMmapMarker> = Vec::new();
        let mut pending_entries: Vec<Arc<SeriesEntry>> = Vec::new();
        let entries = self.series.all_entries();

        for entry in &entries {
            // Lock order: entry.inner -> chunk_writer (matching commit()).
            let mut st = entry.inner.lock();
            if let Some(ref mut ooo) = st.ooo {
                if let Some(ohc) = ooo.ooo_head_chunk.take() {
                    let cut_chunks = ohc.chunk.to_encoded_chunks(
                        i64::MIN,
                        i64::MAX,
                        use_xor2,
                        use_hist_st,
                    )?;
                    if !cut_chunks.is_empty() {
                        let mut writer_guard = self.chunk_writer.lock();
                        if let Some(ref mut writer) = *writer_guard {
                            for cc in cut_chunks {
                                let r = writer.write_chunk(
                                    entry.ref_id,
                                    cc.min_time,
                                    cc.max_time,
                                    &cc.chunk,
                                    true,
                                )?;
                                let mc = MmappedChunk {
                                    ref_id: r,
                                    min_time: cc.min_time,
                                    max_time: cc.max_time,
                                    num_samples: cc.chunk.num_samples() as u16,
                                    encoding: cc.chunk.encoding(),
                                };
                                ooo.ooo_pending_mmapped.push((mc, cc.chunk));
                                ooo_markers.push(RefMmapMarker {
                                    ref_id: entry.ref_id,
                                    mmap_ref: r.as_u64(),
                                });
                                mmapped_any = true;
                            }
                            pending_entries.push(Arc::clone(entry));
                        } else {
                            ooo.ooo_head_chunk = Some(ohc);
                        }
                    }
                }
            }
        }

        if mmapped_any {
            let mut writer_guard = self.chunk_writer.lock();
            if let Some(ref mut writer) = *writer_guard {
                writer.flush()?;
            }
        }

        {
            let mut wbl_guard = self.wbl.lock();
            if let Some(ref mut wbl) = *wbl_guard {
                if !ooo_markers.is_empty() {
                    let enc = RecordEncoder::new(self.opts.enable_st_storage);
                    let rec = enc.mmap_markers(&ooo_markers, Vec::new());
                    wbl.log(&[&rec])?;
                }
                let next_seg = wbl.next_segment()?;
                *self.ooo_compact_wbl_cut_seg.lock() = Some(next_seg);
            }
        }

        if mmapped_any {
            self.refresh_chunk_reader()?;
            for entry in pending_entries {
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo {
                    for (mc, _) in ooo.ooo_pending_mmapped.drain(..) {
                        ooo.ooo_mmapped_chunks.push(mc);
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
            let Some(ref ooo) = st.ooo else {
                continue;
            };
            let mut mmapped_refs: SmallVec<[MmappedChunk; 8]> = SmallVec::new();
            for mc in &ooo.ooo_mmapped_chunks {
                mmapped_refs.push(*mc);
                any_mmapped = true;
            }
            let mut inline_chks = Vec::new();
            for (mc, chk) in &ooo.ooo_pending_mmapped {
                mint = mint.min(mc.min_time);
                maxt = maxt.max(mc.max_time);
                inline_chks.push(ChunkMeta {
                    ref_id: mc.ref_id.as_u64(),
                    min_time: mc.min_time,
                    max_time: mc.max_time,
                    chunk: Some(chk.clone()),
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
            for mc in ex.mmapped_refs {
                if let Some(r) = chunk_reader {
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

    /// Clears out-of-order chunks/samples `<= maxt_inclusive` from Head memory (preserving any OOO
    /// samples `> maxt_inclusive` that arrived during compaction), truncates WBL segments, and
    /// cleans up empty series and unused `chunks_head` files.
    pub fn clear_ooo_data_before(&self, maxt_inclusive: i64) -> Result<(), HeadError> {
        let min_valid_time = self.min_valid_time.load(Ordering::Acquire);
        let head_maxt = self.max_time();
        let mut stripe_buf: Vec<Arc<SeriesEntry>> = Vec::with_capacity(256);
        let mut to_delete: Vec<(Arc<SeriesEntry>, i64)> = Vec::new();
        let mut min_mmap_seq = usize::MAX;
        let mut rem_ooo_mint = i64::MAX;
        let mut rem_ooo_maxt = i64::MIN;

        let record_series_refs =
            |st: &SeriesState, min_mmap_seq: &mut usize, rem_mint: &mut i64, rem_maxt: &mut i64| {
                for mc in st.mmapped() {
                    let (seq, _) = mc.ref_id.unpack();
                    *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                }
                if let Some(ref ooo) = st.ooo {
                    for mc in &ooo.ooo_mmapped_chunks {
                        let (seq, _) = mc.ref_id.unpack();
                        *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                        *rem_mint = (*rem_mint).min(mc.min_time);
                        *rem_maxt = (*rem_maxt).max(mc.max_time);
                    }
                    for (mc, _) in &ooo.ooo_pending_mmapped {
                        let (seq, _) = mc.ref_id.unpack();
                        *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                        *rem_mint = (*rem_mint).min(mc.min_time);
                        *rem_maxt = (*rem_maxt).max(mc.max_time);
                    }
                    if let Some(ref ohc) = ooo.ooo_head_chunk {
                        *rem_mint = (*rem_mint).min(ohc.min_time);
                        *rem_maxt = (*rem_maxt).max(ohc.max_time);
                    }
                }
            };

        for stripe in &self.series.stripes {
            stripe_buf.clear();
            {
                let guard = stripe.read();
                if guard.by_id.is_empty() {
                    continue;
                }
                stripe_buf.extend(guard.by_id.values().cloned());
            }
            for entry in stripe_buf.drain(..) {
                let mut st = entry.inner.lock();
                if entry.pending_commits_count() > 0 {
                    // Series has an in-flight append: do not drop OOO data, and still count its
                    // chunks_head sequence numbers so chunk files are never deleted prematurely.
                    record_series_refs(&st, &mut min_mmap_seq, &mut rem_ooo_mint, &mut rem_ooo_maxt);
                    continue;
                }

                if let Some(ref mut ooo) = st.ooo {
                    let before = ooo.ooo_mmapped_chunks.len() + ooo.ooo_pending_mmapped.len();
                    ooo.ooo_mmapped_chunks
                        .retain(|mc| mc.max_time > maxt_inclusive);
                    ooo.ooo_pending_mmapped
                        .retain(|(mc, _)| mc.max_time > maxt_inclusive);
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
                    if ooo.ooo_mmapped_chunks.is_empty()
                        && ooo.ooo_pending_mmapped.is_empty()
                        && ooo.ooo_head_chunk.is_none()
                    {
                        st.ooo = None;
                    }
                }

                let has_ooo = st.ooo.is_some();
                let has_recent_exemplar =
                    st.last_exemplar_ts().is_some_and(|ts| ts >= min_valid_time);
                if !has_ooo
                    && st.mmapped().is_empty()
                    && st.head_chunks.is_empty()
                    && !has_recent_exemplar
                {
                    let keep_until = st.max_time();
                    if self.series.remove_if_gced_under_entry_lock(&entry, &mut st) {
                        drop(st);
                        to_delete.push((entry, keep_until));
                        continue;
                    }
                }
                record_series_refs(&st, &mut min_mmap_seq, &mut rem_ooo_mint, &mut rem_ooo_maxt);
            }
        }

        let ooo_window = self.ooo_time_window_ms.load(Ordering::Acquire);
        let mut min_ooo_time = rem_ooo_mint;
        if head_maxt.wrapping_sub(ooo_window) < min_ooo_time {
            min_ooo_time = head_maxt.wrapping_sub(ooo_window);
        }
        self.min_ooo_time.store(min_ooo_time, Ordering::Release);
        self.max_ooo_time.store(rem_ooo_maxt, Ordering::Release);

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
    /// `chunks_head` file to cut a new file on the next write and deletes every older file
    /// `< min_mmap_seq` (`head_chunks.go:971-1010`).
    fn truncate_chunks_head(&self, min_mmap_seq: usize) -> Result<(), HeadError> {
        let active_seq = {
            let mut w = self.chunk_writer.lock();
            match w.as_mut() {
                Some(wr) => {
                    let cur = wr.has_active_file().then(|| wr.current_seq());
                    if wr.cur_file_has_chunks() {
                        wr.cut_file_on_next_chunk();
                    }
                    cur
                }
                None => None,
            }
        };
        if min_mmap_seq > 1 {
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
            if st.ooo.as_ref().is_some_and(|o| {
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
            for hc in &st.head_chunks {
                mint = mint.min(hc.min_time);
                maxt = maxt.max(hc.max_time);
                head_chks.push(ChunkMeta {
                    ref_id: 0,
                    min_time: hc.min_time,
                    max_time: hc.max_time,
                    chunk: Some(hc.chunk.clone()),
                });
            }
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
            for mc in ex.mmapped_refs {
                if let Some(r) = chunk_reader {
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
        let min_valid_time = self.min_valid_time.load(Ordering::Acquire);
        let mut to_delete: Vec<(Arc<SeriesEntry>, i64)> = Vec::new();
        for &ref_id in refs {
            if let Some(entry) = self.series.get_by_id(ref_id) {
                let mut st = entry.inner.lock();
                if entry.pending_commits_count() > 0 || st.max_time() > maxt_inclusive {
                    continue;
                }
                let has_ooo = st.ooo.as_ref().is_some_and(|o| {
                    o.ooo_head_chunk.is_some()
                        || !o.ooo_mmapped_chunks.is_empty()
                        || !o.ooo_pending_mmapped.is_empty()
                });
                if !has_ooo {
                    let removed = (st.mmapped().len() + st.head_chunks.len()) as u64;
                    if st.last_exemplar_ts().is_some_and(|ts| ts >= min_valid_time) {
                        // Keep the series identity while its exemplar can still be replayed,
                        // matching Prometheus `gcSeries`.
                        self.metrics.chunks_removed(removed);
                        st.mmapped_chunks = None;
                        st.head_chunks.clear();
                        entry
                            .in_order_max_time
                            .store(st.max_time(), Ordering::Release);
                    } else {
                        let keep_until = st.max_time();
                        if self.series.remove_if_gced_under_entry_lock(&entry, &mut st) {
                            self.metrics.chunks_removed(removed);
                            drop(st);
                            to_delete.push((entry, keep_until));
                        }
                    }
                }
            }
        }
        if !to_delete.is_empty() {
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
            self.finish_deleted_series_cleanup(to_delete);
            self.series.shrink_oversized_stripes();
            self.maybe_gc_symbols();
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

    fn truncate_memory_inner(&self, mint: i64) -> Result<(), HeadError> {
        let initialized = self.initialized();
        if initialized && self.min_time() >= mint {
            return Ok(());
        }

        self.min_time.store(mint, Ordering::Release);
        self.min_valid_time.store(mint, Ordering::Release);
        if self.max_time() < mint {
            self.max_time.store(mint, Ordering::Release);
        }
        if !initialized {
            return Ok(());
        }

        self.metrics.head_truncate_total.inc();
        let gc_start = Instant::now();
        let head_maxt = self.max_time();
        let (to_delete, actual_mint, mut min_ooo_time, min_mmap_seq) =
            self.gc_collect_and_unlink(mint);

        if !to_delete.is_empty() {
            // Log tombstones to WAL in bounded chunks after atomic unlink.
            const TOMBSTONE_BATCH: usize = 16_384;
            let enc = RecordEncoder::new(self.opts.enable_st_storage);
            let mut stones = Vec::with_capacity(to_delete.len().min(TOMBSTONE_BATCH));
            let mut rec_buf = Vec::new();
            for chunk in to_delete.chunks(TOMBSTONE_BATCH) {
                stones.clear();
                for (e, _) in chunk {
                    stones.push(crate::record::Tombstone {
                        ref_id: e.ref_id,
                        mint: i64::MIN,
                        maxt: i64::MAX,
                    });
                }
                rec_buf.clear();
                rec_buf = enc.tombstones(&stones, rec_buf);
                if let Some(ref mut wal) = *self.wal.lock() {
                    wal.log(&[&rec_buf])?;
                }
            }
            drop(stones);
            drop(rec_buf);
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
        self.series.shrink_oversized_stripes();
        self.maybe_gc_symbols();
        Ok(())
    }

    /// Garbage-collects chunks `< mint` and empty series (used on startup after WAL/WBL replay).
    pub fn gc(&self, mint: i64) -> Result<(), HeadError> {
        let (to_delete, actual_mint, _min_ooo_time, min_mmap_seq) =
            self.gc_collect_and_unlink(mint);
        if !to_delete.is_empty() {
            self.finish_deleted_series_cleanup(to_delete);
            self.series.shrink_oversized_stripes();
            self.maybe_gc_symbols();
        }
        self.tombstones.truncate_before(mint);
        if actual_mint != i64::MAX && self.initialized() && actual_mint > self.min_time() {
            self.min_time.store(actual_mint, Ordering::Release);
        }
        self.truncate_chunks_head(min_mmap_seq)?;
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
            if let Some(ref ooo) = st.ooo {
                for mc in &ooo.ooo_mmapped_chunks {
                    let (seq, _) = mc.ref_id.unpack();
                    *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                    *min_ooo_time = (*min_ooo_time).min(mc.min_time);
                }
                for (mc, _) in &ooo.ooo_pending_mmapped {
                    let (seq, _) = mc.ref_id.unpack();
                    *min_mmap_seq = (*min_mmap_seq).min(seq as usize);
                    *min_ooo_time = (*min_ooo_time).min(mc.min_time);
                }
                if let Some(ref ohc) = ooo.ooo_head_chunk {
                    *min_ooo_time = (*min_ooo_time).min(ohc.min_time);
                }
            }
        };

        for stripe in &self.series.stripes {
            stripe_buf.clear();
            {
                let guard = stripe.read();
                if guard.by_id.is_empty() {
                    continue;
                }
                stripe_buf.extend(guard.by_id.values().cloned());
            }
            for entry in stripe_buf.drain(..) {
                let mut st = entry.inner.lock();
                if entry.pending_commits_count() > 0 {
                    // Always include in-flight series' chunks_head sequence numbers and min_time
                    // so chunk files referenced by pending commits are NEVER deleted.
                    record_refs(&st, &mut actual_mint, &mut min_ooo_time, &mut min_mmap_seq);
                    continue;
                }

                let prev_max_time = st.max_time();
                let chunks_before = st.mmapped().len() + st.head_chunks.len();
                if let Some(ref mut mv) = st.mmapped_chunks {
                    mv.retain(|mc| mc.max_time >= mint);
                    if mv.is_empty() {
                        st.mmapped_chunks = None;
                    }
                }
                st.head_chunks.retain(|hc| hc.max_time >= mint);
                chunks_removed +=
                    (chunks_before - st.mmapped().len() - st.head_chunks.len()) as u64;
                entry
                    .in_order_max_time
                    .store(st.max_time(), Ordering::Release);
                if st.head_chunks.is_empty() {
                    st.xor_state.valid = false;
                    if let Some(ref mut s2) = st.xor2_state {
                        s2.valid = false;
                    }
                    st.hist_state = None;
                }

                let has_ooo = st.ooo.as_ref().is_some_and(|o| {
                    o.ooo_head_chunk.is_some()
                        || !o.ooo_mmapped_chunks.is_empty()
                        || !o.ooo_pending_mmapped.is_empty()
                });
                let has_recent_exemplar = st.last_exemplar_ts().is_some_and(|ts| ts >= mint);

                if st.mmapped().is_empty()
                    && st.head_chunks.is_empty()
                    && !has_ooo
                    && !has_recent_exemplar
                    && prev_max_time < mint
                    && self.series.remove_if_gced_under_entry_lock(&entry, &mut st)
                {
                    drop(st);
                    to_delete.push((entry, prev_max_time));
                    continue;
                }

                record_refs(&st, &mut actual_mint, &mut min_ooo_time, &mut min_mmap_seq);
            }
        }

        self.metrics.chunks_removed(chunks_removed);
        (to_delete, actual_mint, min_ooo_time, min_mmap_seq)
    }

    /// Go `Head.truncateWAL`: checkpoints the lower two thirds of the WAL segments and
    /// drops them. A failed segment truncation or old-checkpoint deletion is only logged
    /// (and counted), like Go.
    pub fn truncate_wal(&self, mint: i64) -> Result<(), HeadError> {
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
        for stripe in &self.series.stripes {
            let entries: Vec<Arc<SeriesEntry>> = stripe.read().by_id.values().cloned().collect();
            for e in entries {
                let st = e.inner.lock();
                chunks += (st.mmapped().len() + st.head_chunks.len()) as u64;
                for mc in st.mmapped() {
                    samples += mc.num_samples as u64;
                }
                for hc in &st.head_chunks {
                    samples += hc.chunk.num_samples() as u64;
                }
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

    /// Evaluates label matchers against `MemPostings` (backed by `PostingsForMatchersCache`),
    /// returning a shared `Arc<[u64]>` with zero slice copy on cache hits.
    pub fn postings_for_matchers_arc(
        &self,
        matchers: &[LabelMatcher],
    ) -> Result<Arc<[u64]>, HeadError> {
        self.postings_cache.get_or_compute(matchers, || {
            let syms = self.symbols.read();
            let view = self.postings.as_source(&syms);
            Ok(postings_for_matchers(&view, matchers)?)
        })
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
        let refs = self.postings_for_matchers_arc(matchers)?;
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
        let refs = self.postings_for_matchers_arc(matchers)?;
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

    /// Returns `Some(entry.labels.clone())` (O(1) refcount bump, zero chunk clone) if `ref_id`
    /// has any non-tombstoned chunks overlapping `[mint, maxt]`.
    pub fn series_has_chunks_in_range(&self, ref_id: u64, mint: i64, maxt: i64) -> Option<Labels> {
        let Some(entry) = self.series.get_by_id(ref_id) else {
            self.metrics.series_not_found.inc();
            return None;
        };
        let tombs = self.tombstones.get(ref_id);
        let st = entry.inner.lock();
        let has_chunk = st.head_chunks.iter().any(|hc| {
            hc.overlaps_closed_interval(mint, maxt)
                && !tombs.iter().any(|iv| iv.is_subrange(hc.min_time, hc.max_time))
        }) || st.mmapped().iter().any(|mc| {
            mc.overlaps_closed_interval(mint, maxt)
                && !tombs.iter().any(|iv| iv.is_subrange(mc.min_time, mc.max_time))
        }) || st.ooo.as_ref().is_some_and(|ooo| {
            ooo.ooo_mmapped_chunks
                .iter()
                .any(|mc| mc.overlaps_closed_interval(mint, maxt))
                || ooo
                    .ooo_pending_mmapped
                    .iter()
                    .any(|(mc, _)| mc.overlaps_closed_interval(mint, maxt))
                || ooo
                    .ooo_head_chunk
                    .as_ref()
                    .is_some_and(|ohc| ohc.overlaps_closed_interval(mint, maxt))
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
        let Some(entry) = self.series.get_by_id(ref_id) else {
            // Go `headIndexReader.Series`: `seriesNotFound.Inc()`.
            self.metrics.series_not_found.inc();
            return Ok(None);
        };

        let (mmapped_in_order, head_chks, mmapped_ooo, ooo_head_chks) = {
            let st = entry.inner.lock();
            let mmapped_count = st
                .mmapped()
                .iter()
                .filter(|mc| mc.overlaps_closed_interval(mint, maxt))
                .count();
            let mut mmapped_in_order = Vec::with_capacity(mmapped_count);
            if mmapped_count > 0 {
                for mc in st.mmapped() {
                    if mc.overlaps_closed_interval(mint, maxt) {
                        mmapped_in_order.push(*mc);
                    }
                }
            }
            let head_count = st
                .head_chunks
                .iter()
                .filter(|hc| hc.overlaps_closed_interval(mint, maxt))
                .count();
            let mut head_chks = Vec::with_capacity(head_count);
            if head_count > 0 {
                for hc in &st.head_chunks {
                    if hc.overlaps_closed_interval(mint, maxt) {
                        head_chks.push(ChunkMeta {
                            ref_id: 0,
                            min_time: hc.min_time,
                            max_time: hc.max_time,
                            chunk: Some(hc.chunk.clone()),
                        });
                    }
                }
            }
            let mut mmapped_ooo = Vec::new();
            let mut ooo_head_chks = Vec::new();
            if let Some(ref ooo) = st.ooo {
                for mc in &ooo.ooo_mmapped_chunks {
                    if mc.overlaps_closed_interval(mint, maxt) {
                        mmapped_ooo.push(*mc);
                    }
                }
                for (mc, chk) in &ooo.ooo_pending_mmapped {
                    if mc.overlaps_closed_interval(mint, maxt) {
                        ooo_head_chks.push(ChunkMeta {
                            ref_id: mc.ref_id.as_u64(),
                            min_time: mc.min_time,
                            max_time: mc.max_time,
                            chunk: Some(chk.clone()),
                        });
                    }
                }
                if let Some(ref ohc) = ooo.ooo_head_chunk {
                    if ohc.overlaps_closed_interval(mint, maxt) {
                        let use_xor2 = self.float_chunk_encoding() == Encoding::Xor2;
                        let use_hist_st = self.opts.enable_histogram_st_encoding;
                        for cc in ohc.chunk.to_encoded_chunks(
                            ohc.min_time,
                            ohc.max_time,
                            use_xor2,
                            use_hist_st,
                        )? {
                            ooo_head_chks.push(ChunkMeta {
                                ref_id: 0,
                                min_time: cc.min_time,
                                max_time: cc.max_time,
                                chunk: Some(cc.chunk),
                            });
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
            return Ok(Some((entry.labels.clone(), chks, tombs)));
        }

        // Acquire chunk_reader AFTER releasing entry.inner so any MmappedChunk observed
        // is guaranteed to be present in chunk_reader.
        let chunk_reader_guard = self.chunk_reader.read();
        let chunk_reader = chunk_reader_guard.as_ref();

        let mut chks = Vec::with_capacity(
            mmapped_in_order.len() + head_chks.len() + mmapped_ooo.len() + ooo_head_chks.len(),
        );
        for mc in mmapped_in_order {
            if let Some(r) = chunk_reader {
                chks.push(ChunkMeta {
                    ref_id: mc.ref_id.as_u64(),
                    min_time: mc.min_time,
                    max_time: mc.max_time,
                    chunk: Some(r.chunk(mc.ref_id)?),
                });
            }
        }
        chks.extend(head_chks);
        for mc in mmapped_ooo {
            if let Some(r) = chunk_reader {
                chks.push(ChunkMeta {
                    ref_id: mc.ref_id.as_u64(),
                    min_time: mc.min_time,
                    max_time: mc.max_time,
                    chunk: Some(r.chunk(mc.ref_id)?),
                });
            }
        }
        chks.extend(ooo_head_chks);

        if chks.is_empty() {
            return Ok(None);
        }
        let tombs = self.tombstones.get(ref_id);
        Ok(Some((entry.labels.clone(), chks, tombs)))
    }

    /// Loads and overlap-merges (if tombstones or OOO overlaps exist) all chunks for `ref_id`
    /// overlapping `[mint, maxt]`. Used by lazy Phase 2 chunk streaming for a single series at a time.
    pub fn series_final_chunks(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
    ) -> Result<Option<(Labels, Vec<ChunkMeta>)>, HeadError> {
        let Some((labels, chks, tombs)) = self.series_chunks(ref_id, mint, maxt)? else {
            return Ok(None);
        };
        if chks.is_empty() {
            return Ok(None);
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
                let has_overlap = chks.windows(2).any(|w| w[1].min_time <= w[0].max_time);
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
        if final_chunks.is_empty() {
            Ok(None)
        } else {
            Ok(Some((labels, final_chunks)))
        }
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
        let Some(entry) = self.series.get_by_id(ref_id) else {
            // Go `headIndexReader.Series`: `seriesNotFound.Inc()`.
            self.metrics.series_not_found.inc();
            return Ok(None);
        };
        if let Some(pred) = filter {
            if !pred(&entry.labels) {
                return Ok(None);
            }
        }

        let has_any_tombs = self.tombstones.has_any();
        let (fast_count, needs_slow_resolve) = {
            let st = entry.inner.lock();
            let has_ooo = st.ooo.as_ref().is_some_and(|ooo| {
                ooo.ooo_mmapped_chunks
                    .iter()
                    .any(|mc| mc.overlaps_closed_interval(mint, maxt))
                    || ooo
                        .ooo_pending_mmapped
                        .iter()
                        .any(|(mc, _)| mc.overlaps_closed_interval(mint, maxt))
                    || ooo
                        .ooo_head_chunk
                        .as_ref()
                        .is_some_and(|ohc| ohc.overlaps_closed_interval(mint, maxt))
            });
            let mut count: u32 = 0;
            let mut prev_min: i64 = i64::MIN;
            let mut prev_max: i64 = i64::MIN;
            let mut has_prev = false;
            let mut sorted_disjoint = true;
            for mc in st.mmapped() {
                if mc.overlaps_closed_interval(mint, maxt) {
                    if has_prev && (mc.min_time < prev_min || mc.min_time <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = mc.min_time;
                    prev_max = mc.max_time;
                    has_prev = true;
                    count += 1;
                }
            }
            for hc in &st.head_chunks {
                if hc.overlaps_closed_interval(mint, maxt) {
                    if has_prev && (hc.min_time < prev_min || hc.min_time <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = hc.min_time;
                    prev_max = hc.max_time;
                    has_prev = true;
                    count += 1;
                }
            }
            (count, has_ooo || has_any_tombs || !sorted_disjoint)
        };

        if !needs_slow_resolve {
            return Ok(if fast_count == 0 {
                None
            } else {
                Some((entry.labels.clone(), fast_count))
            });
        }

        Ok(self
            .series_final_chunks(ref_id, mint, maxt)?
            .map(|(labels, chks)| (labels, chks.len() as u32)))
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
        let Some(entry) = self.series.get_by_id(ref_id) else {
            // Go `headIndexReader.Series`: `seriesNotFound.Inc()`.
            self.metrics.series_not_found.inc();
            return Ok(None);
        };
        if let Some(pred) = filter {
            if !pred(&entry.labels) {
                return Ok(None);
            }
        }
        let has_any_tombs = self.tombstones.has_any();
        let (intervals, needs_slow) = {
            let st = entry.inner.lock();
            let has_ooo = st.ooo.as_ref().is_some_and(|ooo| {
                ooo.ooo_mmapped_chunks
                    .iter()
                    .any(|mc| mc.overlaps_closed_interval(mint, maxt))
                    || ooo
                        .ooo_pending_mmapped
                        .iter()
                        .any(|(mc, _)| mc.overlaps_closed_interval(mint, maxt))
                    || ooo
                        .ooo_head_chunk
                        .as_ref()
                        .is_some_and(|ohc| ohc.overlaps_closed_interval(mint, maxt))
            });
            let mut intervals: SmallVec<[(i64, i64); 4]> = SmallVec::new();
            let mut prev_min = i64::MIN;
            let mut prev_max = i64::MIN;
            let mut has_prev = false;
            let mut sorted_disjoint = true;
            for mc in st.mmapped() {
                if mc.overlaps_closed_interval(mint, maxt) {
                    if has_prev && (mc.min_time < prev_min || mc.min_time <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = mc.min_time;
                    prev_max = mc.max_time;
                    has_prev = true;
                    intervals.push((mc.min_time, mc.max_time));
                }
            }
            for hc in &st.head_chunks {
                if hc.overlaps_closed_interval(mint, maxt) {
                    if has_prev && (hc.min_time < prev_min || hc.min_time <= prev_max) {
                        sorted_disjoint = false;
                    }
                    prev_min = hc.min_time;
                    prev_max = hc.max_time;
                    has_prev = true;
                    intervals.push((hc.min_time, hc.max_time));
                }
            }
            (intervals, has_ooo || has_any_tombs || !sorted_disjoint)
        };
        if !needs_slow {
            if intervals.is_empty() {
                return Ok(None);
            }
            return Ok(Some((entry.labels.clone(), intervals, false)));
        }
        let Some((labels, chks)) = self.series_final_chunks(ref_id, mint, maxt)? else {
            return Ok(None);
        };
        let ivs: SmallVec<[(i64, i64); 4]> =
            chks.iter().map(|c| (c.min_time, c.max_time)).collect();
        Ok(Some((labels, ivs, true)))
    }

    pub fn series_labels(&self, ref_id: u64) -> Option<Labels> {
        self.series.get_by_id(ref_id).map(|e| e.labels.clone())
    }

    /// Flushes sealed chunks to `chunks_head`, syncs and closes WAL, WBL, and `HeadChunksWriter`.
    pub fn close(&self) -> Result<(), HeadError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.mmap_head_chunks()?;
        if let Some(cw) = self.chunk_writer.lock().take() {
            cw.close()?;
        }
        if let Some(wal) = self.wal.lock().take() {
            wal.close()?;
        }
        if let Some(wbl) = self.wbl.lock().take() {
            wbl.close()?;
        }
        Ok(())
    }
}

impl Drop for Head {
    fn drop(&mut self) {
        let _ = self.close();
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
    is_ooo: bool,
}

#[derive(Clone)]
struct PendingFloatHist {
    entry: Arc<SeriesEntry>,
    st: i64,
    t: i64,
    fh: FloatHistogram,
    is_ooo: bool,
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
    batch_last_sample: FastU64Map<(i64, LastSample)>,
    types_in_batch: FastU64Map<BatchSampleType>,
    batch_boundaries: Vec<BatchBoundary>,
    wal_buf: Vec<u8>,
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
    batch_last_sample: FastU64Map<(i64, LastSample)>,
    types_in_batch: FastU64Map<BatchSampleType>,
    batch_boundaries: Vec<BatchBoundary>,
    indexed_floats_len: usize,
    indexed_hists_len: usize,
    indexed_fhists_len: usize,
    floats_committed: usize,
    hists_committed: usize,
    fhists_committed: usize,
    wal_buf: Vec<u8>,
    closed: bool,
}

impl<'a> HeadAppender<'a> {
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
    #[inline]
    fn upsert_batch_last(
        map: &mut FastU64Map<(i64, LastSample)>,
        ref_id: u64,
        t: i64,
        ls: LastSample,
    ) {
        match map.entry(ref_id) {
            hashbrown::hash_map::Entry::Occupied(mut o) => {
                if t >= o.get().0 {
                    *o.get_mut() = (t, ls);
                }
            }
            hashbrown::hash_map::Entry::Vacant(v) => {
                v.insert((t, ls));
            }
        }
    }

    fn sync_batch_last_sample(&mut self) {
        while self.indexed_floats_len < self.floats.len() {
            let pf = &self.floats[self.indexed_floats_len];
            self.indexed_floats_len += 1;
            if !pf.is_ooo {
                Self::upsert_batch_last(
                    &mut self.batch_last_sample,
                    pf.entry.ref_id,
                    pf.t,
                    LastSample::Float(pf.v),
                );
            }
        }
        while self.indexed_hists_len < self.histograms.len() {
            let ph = &self.histograms[self.indexed_hists_len];
            self.indexed_hists_len += 1;
            if !ph.is_ooo {
                Self::upsert_batch_last(
                    &mut self.batch_last_sample,
                    ph.entry.ref_id,
                    ph.t,
                    LastSample::Histogram(Box::new(ph.h.clone())),
                );
            }
        }
        while self.indexed_fhists_len < self.float_histograms.len() {
            let pfh = &self.float_histograms[self.indexed_fhists_len];
            self.indexed_fhists_len += 1;
            if !pfh.is_ooo {
                Self::upsert_batch_last(
                    &mut self.batch_last_sample,
                    pfh.entry.ref_id,
                    pfh.t,
                    LastSample::FloatHistogram(Box::new(pfh.fh.clone())),
                );
            }
        }
    }

    fn release_remaining_pending_commits(&mut self) {
        if self.floats_committed < self.floats.len() {
            for pf in &self.floats[self.floats_committed..] {
                pf.entry.pending_commits.fetch_sub(1, Ordering::Release);
            }
        }
        self.floats.clear();
        self.floats_committed = 0;
        if self.hists_committed < self.histograms.len() {
            for ph in &self.histograms[self.hists_committed..] {
                ph.entry.pending_commits.fetch_sub(1, Ordering::Release);
            }
        }
        self.histograms.clear();
        self.hists_committed = 0;
        if self.fhists_committed < self.float_histograms.len() {
            for pfh in &self.float_histograms[self.fhists_committed..] {
                pfh.entry.pending_commits.fetch_sub(1, Ordering::Release);
            }
        }
        self.float_histograms.clear();
        self.fhists_committed = 0;
        for entry in self.created_series.drain(..) {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
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
        self.batch_last_sample.clear();
        self.types_in_batch.clear();
        self.batch_boundaries.clear();
        self.wal_buf.clear();
        self.indexed_floats_len = 0;
        self.indexed_hists_len = 0;
        self.indexed_fhists_len = 0;
        self.floats_committed = 0;
        self.hists_committed = 0;
        self.fhists_committed = 0;
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
        if self.batch_last_sample.capacity() > 16_384 {
            self.batch_last_sample = FastU64Map::default();
        }
        if self.types_in_batch.capacity() > 16_384 {
            self.types_in_batch = FastU64Map::default();
        }
        if self.wal_buf.capacity() > 512 * 1024 {
            self.wal_buf = Vec::new();
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
                batch_last_sample: std::mem::take(&mut self.batch_last_sample),
                types_in_batch: std::mem::take(&mut self.types_in_batch),
                batch_boundaries: std::mem::take(&mut self.batch_boundaries),
                wal_buf: std::mem::take(&mut self.wal_buf),
            });
        }
    }

    fn check_batch_last_sample(
        &self,
        ref_id: u64,
        t: i64,
        val: &LastSample,
        ooo_time_window: i64,
    ) -> Result<Option<(bool, bool)>, HeadError> {
        let Some((prev_t, prev_val)) = self.batch_last_sample.get(&ref_id) else {
            return Ok(None);
        };
        if t >= self.min_valid_time {
            if t > *prev_t {
                return Ok(Some((false, false)));
            }
            if t == *prev_t {
                let same = match (prev_val, val) {
                    (LastSample::Float(a), LastSample::Float(b)) => {
                        a.to_bits() == b.to_bits() || b.to_bits() == QUIET_ZERO_NAN_BITS
                    }
                    (LastSample::Histogram(a), LastSample::Histogram(b)) => a.prom_equals(b),
                    (LastSample::FloatHistogram(a), LastSample::FloatHistogram(b)) => {
                        a.prom_equals(b)
                    }
                    _ => false,
                };
                if same {
                    return Ok(Some((false, true)));
                }
                if let LastSample::Float(new_v) = val {
                    let msg = match prev_val {
                        LastSample::Float(prev_v) => format!(
                            "duplicate sample for timestamp {t}; overrides not allowed: existing {}, new value {}",
                            format_go_g(*prev_v),
                            format_go_g(*new_v)
                        ),
                        LastSample::Histogram(_) | LastSample::FloatHistogram(_) => format!(
                            "duplicate sample for timestamp {t}; overrides not allowed: existing is a histogram, new value {}",
                            format_go_g(*new_v)
                        ),
                        LastSample::None => "duplicate sample for timestamp".to_string(),
                    };
                    return Err(HeadError::DuplicateSampleWithDetails(msg));
                }
                return Err(HeadError::DuplicateSampleForTimestamp);
            }
        }
        if let LastSample::Float(v) = val {
            if v.to_bits() == QUIET_ZERO_NAN_BITS {
                return Ok(Some((true, true)));
            }
        }
        if ooo_time_window > 0 && t >= self.head_maxt.saturating_sub(ooo_time_window) {
            return Ok(Some((true, false)));
        }
        if ooo_time_window > 0 {
            return Err(HeadError::TooOldSample);
        }
        if t < self.min_valid_time {
            return Err(HeadError::OutOfBounds);
        }
        Err(HeadError::OutOfOrderSample)
    }
    fn resolve_sorted_pairs_prehashed(
        &mut self,
        series_ref: Option<u64>,
        prehashed: Option<u64>,
        pairs: &[(&str, &str)],
    ) -> Result<Arc<SeriesEntry>, HeadError> {
        if let Some(r) = series_ref {
            if let Some(e) = self.head.series.get_by_id(r) {
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
        effective_ooo: i64,
    ) -> Result<(bool, bool), HeadError> {
        let res = if state.needs_chunk_reader_for_t(t) {
            let cr_guard = self.head.chunk_reader.read();
            state.appendable_float(
                t,
                v,
                self.head_maxt,
                self.min_valid_time,
                effective_ooo,
                cr_guard.as_ref(),
            )
        } else {
            state.appendable_float(
                t,
                v,
                self.head_maxt,
                self.min_valid_time,
                effective_ooo,
                None,
            )
        };
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
                if v.to_bits() != QUIET_ZERO_NAN_BITS {
                    let delta = self.head_maxt.saturating_sub(t);
                    if delta > 0 {
                        self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                    }
                }
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
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        let effective_ooo = if reject_ooo { 0 } else { self.ooo_time_window };
        if effective_ooo == 0 && t < self.min_valid_time {
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
                effective_ooo,
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
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        let effective_ooo = if reject_ooo { 0 } else { self.ooo_time_window };
        if effective_ooo == 0 && t < self.min_valid_time {
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
                effective_ooo,
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
        effective_ooo: i64,
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
                            effective_ooo,
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
                            effective_ooo,
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
                self.check_entry_appendable_float(&mut state, t, v, effective_ooo)
            };
            match res {
                Ok((is_ooo, is_dup)) => {
                    if is_dup {
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
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
        let prev = entry.pending_commits_count().saturating_sub(1);
        let is_ooo = if prev > 0 {
            self.sync_batch_last_sample();
            match self.check_batch_last_sample(ref_id, t, &LastSample::Float(v), effective_ooo) {
                Ok(Some((is_ooo, is_dup))) => {
                    if is_ooo && v.to_bits() != QUIET_ZERO_NAN_BITS {
                        let delta = self.head_maxt.saturating_sub(t);
                        if delta > 0 {
                            self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                        }
                    }
                    if is_dup {
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
                        if take_entry {
                            *series_entry = Some(entry);
                        }
                        return Ok(Some(ref_id));
                    }
                    is_ooo
                }
                Ok(None) => {
                    let res = {
                        let mut state = entry.inner.lock();
                        if entry.is_gced() {
                            drop(state);
                            entry.pending_commits.fetch_sub(1, Ordering::Release);
                            return Ok(None);
                        }
                        self.check_entry_appendable_float(&mut state, t, v, effective_ooo)
                    };
                    match res {
                        Ok((is_ooo, is_dup)) => {
                            if is_dup {
                                entry.pending_commits.fetch_sub(1, Ordering::Release);
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
                }
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    if take_entry {
                        *series_entry = Some(entry);
                    }
                    return Err(e);
                }
            }
        } else {
            let res = {
                let mut state = entry.inner.lock();
                if entry.is_gced() {
                    drop(state);
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    return Ok(None);
                }
                self.check_entry_appendable_float(&mut state, t, v, effective_ooo)
            };
            match res {
                Ok((is_ooo, is_dup)) => {
                    if is_dup {
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
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
                        )
                    }
                }?;
                return res.ok_or(HeadError::UnknownSeriesRef(series_ref));
            }
        }
        if !entry.try_inc_pending_commit() || entry.is_gced() {
            return Err(HeadError::UnknownSeriesRef(series_ref));
        }
        let prev = entry.pending_commits_count().saturating_sub(1);
        let is_ooo = if prev > 0 {
            self.sync_batch_last_sample();
            match self.check_batch_last_sample(
                series_ref,
                t,
                &LastSample::Float(v),
                self.ooo_time_window,
            ) {
                Ok(Some((is_ooo, is_dup))) => {
                    if is_dup {
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
                        return Ok(series_ref);
                    }
                    is_ooo
                }
                Ok(None) => {
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
                                entry.pending_commits.fetch_sub(1, Ordering::Release);
                                return Ok(series_ref);
                            }
                            is_ooo
                        }
                        Err(e) => {
                            entry.pending_commits.fetch_sub(1, Ordering::Release);
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    return Err(e);
                }
            }
        } else {
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
                        entry.pending_commits.fetch_sub(1, Ordering::Release);
                        return Ok(series_ref);
                    }
                    is_ooo
                }
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
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
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        let effective_ooo = if reject_ooo { 0 } else { self.ooo_time_window };
        if effective_ooo == 0 && t < self.min_valid_time {
            return Err(HeadError::OutOfBounds);
        }
        h.validate().map_err(HeadError::HistogramValidation)?;

        let mut cur_ref = series_ref;
        loop {
            let e = self.resolve_sorted_pairs(cur_ref, pairs)?;
            let mut opt = None;
            if let Some(ref_id) =
                self.append_histogram_with_resolved_entry(&mut opt, false, e, st, t, h.clone(), effective_ooo)?
            {
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
        effective_ooo: i64,
    ) -> Result<Option<u64>, HeadError> {
        if !entry.try_inc_pending_commit() {
            return Ok(None);
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }

        let ref_id = entry.ref_id;
        let prev = entry.pending_commits_count().saturating_sub(1);
        let batch_check = if prev > 0 {
            let ls = LastSample::Histogram(Box::new(h.clone()));
            self.sync_batch_last_sample();
            match self.check_batch_last_sample(ref_id, t, &ls, effective_ooo) {
                Ok(v) => v,
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    if take_entry {
                        *series_entry = Some(entry);
                    }
                    return Err(e);
                }
            }
        } else {
            None
        };
        let is_ooo = if let Some((is_ooo, is_dup)) = batch_check {
            if is_ooo {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            if is_dup {
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                return Ok(Some(ref_id));
            }
            is_ooo
        } else {
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
                    effective_ooo,
                    cr_guard.as_ref(),
                )
            } else {
                state.appendable_histogram(
                    t,
                    &h,
                    self.head_maxt,
                    self.min_valid_time,
                    effective_ooo,
                    None,
                )
            };
            drop(state);
            if !matches!(res, Ok((false, _))) {
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
                    return Err(e);
                }
            };
            if is_dup {
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                return Ok(Some(ref_id));
            }
            is_ooo
        };
        let ht = BatchSampleType::from_hist(&h);
        self.record_hist_in_batch(ref_id, ht);
        self.histograms.push(PendingHist {
            entry,
            st,
            t,
            h,
            is_ooo,
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
        if !self.head.initialized() {
            self.head.init_time(t);
            self.min_valid_time = self.head.appendable_min_valid_time();
            self.head_maxt = self.head.max_time();
        }
        let effective_ooo = if reject_ooo { 0 } else { self.ooo_time_window };
        if effective_ooo == 0 && t < self.min_valid_time {
            return Err(HeadError::OutOfBounds);
        }
        fh.validate().map_err(HeadError::HistogramValidation)?;

        let mut cur_ref = series_ref;
        loop {
            let e = self.resolve_sorted_pairs(cur_ref, pairs)?;
            let mut opt = None;
            if let Some(ref_id) = self.append_float_histogram_with_resolved_entry(
                &mut opt,
                false,
                e,
                st,
                t,
                fh.clone(),
                effective_ooo,
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
        effective_ooo: i64,
    ) -> Result<Option<u64>, HeadError> {
        if !entry.try_inc_pending_commit() {
            return Ok(None);
        }
        if entry.is_gced() {
            entry.pending_commits.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }

        let ref_id = entry.ref_id;
        let prev = entry.pending_commits_count().saturating_sub(1);
        let batch_check = if prev > 0 {
            let ls = LastSample::FloatHistogram(Box::new(fh.clone()));
            self.sync_batch_last_sample();
            match self.check_batch_last_sample(ref_id, t, &ls, effective_ooo) {
                Ok(v) => v,
                Err(e) => {
                    entry.pending_commits.fetch_sub(1, Ordering::Release);
                    if take_entry {
                        *series_entry = Some(entry);
                    }
                    return Err(e);
                }
            }
        } else {
            None
        };
        let is_ooo = if let Some((is_ooo, is_dup)) = batch_check {
            if is_ooo {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            if is_dup {
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                return Ok(Some(ref_id));
            }
            is_ooo
        } else {
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
                    effective_ooo,
                    cr_guard.as_ref(),
                )
            } else {
                state.appendable_float_histogram(
                    t,
                    &fh,
                    self.head_maxt,
                    self.min_valid_time,
                    effective_ooo,
                    None,
                )
            };
            drop(state);
            if !matches!(res, Ok((false, _))) {
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
                    return Err(e);
                }
            };
            if is_dup {
                entry.pending_commits.fetch_sub(1, Ordering::Release);
                if take_entry {
                    *series_entry = Some(entry);
                }
                return Ok(Some(ref_id));
            }
            is_ooo
        };
        let ht = BatchSampleType::from_float_hist(&fh);
        self.record_hist_in_batch(ref_id, ht);
        self.float_histograms.push(PendingFloatHist {
            entry,
            st,
            t,
            fh,
            is_ooo,
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
        if !self.head.opts.enable_exemplar_storage || self.head.opts.max_exemplars == 0 {
            return Ok(0);
        }

        let entry = if let Some(r) = series_ref {
            self.head
                .series
                .get_by_id(r)
                .ok_or(HeadError::UnknownSeriesRef(r))?
        } else {
            let mut sorted: SmallVec<[(&str, &str); 16]> = pairs.iter().copied().collect();
            sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
            let hash = hash_sorted_pairs(&sorted);
            self.head
                .get_by_sorted_pairs(hash, &sorted)
                .ok_or(HeadError::UnknownSeriesRef(0))?
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
            Err(ExemplarError::DuplicateExemplar | ExemplarError::Disabled) => return Ok(0),
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
            let prev_meta = {
                let st = e.inner.lock();
                if e.is_gced() {
                    drop(st);
                    cur_ref = None;
                    continue;
                }
                st.metadata
            };
            let ch = match prev_meta {
                Some(m) => {
                    let syms = self.head.symbols.read();
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
                self.head.opts.metadata_limits,
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

    /// Commits all buffered series, samples, histograms, exemplars, and metadata to WAL/WBL and Head memory.
    pub fn commit(mut self) -> Result<(), HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        self.closed = true;
        self.head.metrics.active_appenders.dec();

        let res = self.commit_inner();
        self.release_remaining_pending_commits();
        self.return_scratch();
        res
    }

    fn commit_inner(&mut self) -> Result<(), HeadError> {
        // 1. Log to WAL.
        self.log_wal()?;

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
                    st.metadata = Some(CompactSeriesMetadata {
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
                    unit: syms.intern(&pm.unit),
                    help: syms.intern(&pm.help),
                };
                self.head.has_any_metadata.store(true, Ordering::Release);
                let mut st = pm.entry.inner.lock();
                st.metadata = Some(compact_meta);
            }
        }

        let mut in_order_mint = i64::MAX;
        let mut in_order_maxt = i64::MIN;
        let mut ooo_mint = i64::MAX;
        let mut ooo_maxt = i64::MIN;

        let enc = RecordEncoder::new(self.head.opts.enable_st_storage);
        let mut wbl_samples: Vec<RefSample> = Vec::new();
        let mut wbl_histograms: Vec<RefHistogramSample> = Vec::new();
        let mut wbl_float_histograms: Vec<RefFloatHistogramSample> = Vec::new();
        let mut ooo_mmap_markers: HashMap<u64, Vec<ChunkDiskMapperRef>> = HashMap::new();
        let mut ooo_records: Vec<Vec<u8>> = Vec::new();
        let mut pending_ooo_promote: Vec<Arc<SeriesEntry>> = Vec::new();

        let flush_ooo_records = |wbl_samples: &mut Vec<RefSample>,
                                 wbl_histograms: &mut Vec<RefHistogramSample>,
                                 wbl_float_histograms: &mut Vec<RefFloatHistogramSample>,
                                 ooo_mmap_markers: &mut HashMap<u64, Vec<ChunkDiskMapperRef>>,
                                 ooo_records: &mut Vec<Vec<u8>>| {
            // Write previously queued MmapMarkers (if any) BEFORE samples that belong to those
            // newly opened chunks, and flush samples from prior chunks BEFORE new MmapMarkers.
            if !ooo_mmap_markers.is_empty() {
                let mut markers = Vec::new();
                for (&ref_id, refs) in ooo_mmap_markers.iter() {
                    for &r in refs {
                        markers.push(RefMmapMarker {
                            ref_id,
                            mmap_ref: r.as_u64(),
                        });
                    }
                }
                if !markers.is_empty() {
                    ooo_records.push(enc.mmap_markers(&markers, Vec::new()));
                }
                ooo_mmap_markers.clear();
            }
            if !wbl_samples.is_empty() {
                ooo_records.push(enc.samples(wbl_samples, Vec::new()));
                wbl_samples.clear();
            }
            if !wbl_histograms.is_empty() {
                let (r, cb) = enc.histogram_samples(wbl_histograms, Vec::new());
                if !r.is_empty() {
                    ooo_records.push(r);
                }
                if !cb.is_empty() {
                    ooo_records.push(enc.custom_buckets_histogram_samples(&cb, Vec::new()));
                }
                wbl_histograms.clear();
            }
            if !wbl_float_histograms.is_empty() {
                let (r, cb) = enc.float_histogram_samples(wbl_float_histograms, Vec::new());
                if !r.is_empty() {
                    ooo_records.push(r);
                }
                if !cb.is_empty() {
                    ooo_records.push(enc.custom_buckets_float_histogram_samples(&cb, Vec::new()));
                }
                wbl_float_histograms.clear();
            }
        };

        let float_enc = self.head.float_chunk_encoding();
        let use_xor2 = float_enc == Encoding::Xor2;
        let use_hist_st = self.head.opts.enable_histogram_st_encoding;
        let chunk_range_ms = self.head.opts.chunk_range_ms;
        let samples_per_chunk = self.head.opts.samples_per_chunk;
        let ooo_cap_max = self.head.opts.ooo_cap_max;

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
                let _pending_guard = PendingCommitGuard(&pf.entry.pending_commits);
                let mut state = pf.entry.inner.lock();

                if !pf.is_ooo
                    && !is_stale_nan(v)
                    && pf.t >= self.min_valid_time
                    && pf.t > state.max_time()
                {
                    if v.to_bits() == QUIET_ZERO_NAN_BITS {
                        v = 0.0;
                    }
                    if self.head.metrics.track_append(state.append_float_in_order(
                        pf.st,
                        pf.t,
                        v,
                        chunk_range_ms,
                        samples_per_chunk,
                        float_enc,
                    )?) {
                        in_order_mint = in_order_mint.min(pf.t);
                        in_order_maxt = in_order_maxt.max(pf.t);
                    }
                    pf.entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                    continue;
                }

                if is_stale_nan(v) {
                    match state.latest_in_order_encoding() {
                        Some(Encoding::Histogram | Encoding::HistogramSt) => {
                            let h = Histogram {
                                sum: v,
                                ..Histogram::default()
                            };
                            let check = {
                                let cr_guard = self.head.chunk_reader.read();
                                state.appendable_histogram(
                                    pf.t,
                                    &h,
                                    self.head_maxt,
                                    self.min_valid_time,
                                    self.ooo_time_window,
                                    cr_guard.as_ref(),
                                )
                            };
                            if let Ok((is_ooo, is_dup)) = check {
                                if !is_dup {
                                    if is_ooo {
                                        let mut cw_guard = self.head.chunk_writer.lock();
                                        let (ok, chunk_created, mmap_refs) = state.insert_ooo(
                                            pf.entry.ref_id,
                                            pf.st,
                                            pf.t,
                                            OooSampleValue::Histogram(h.clone()),
                                            ooo_cap_max,
                                            use_xor2,
                                            use_hist_st,
                                            cw_guard.as_mut(),
                                        )?;
                                        drop(cw_guard);
                                        if chunk_created {
                                            self.head.metrics.chunks_created(1);
                                            let prev_markers =
                                                ooo_mmap_markers.get(&pf.entry.ref_id);
                                            if prev_markers.is_none()
                                                || prev_markers.is_some_and(|r| !r.is_empty())
                                            {
                                                flush_ooo_records(
                                                    &mut wbl_samples,
                                                    &mut wbl_histograms,
                                                    &mut wbl_float_histograms,
                                                    &mut ooo_mmap_markers,
                                                    &mut ooo_records,
                                                );
                                            }
                                            if mmap_refs.is_empty() {
                                                ooo_mmap_markers.insert(
                                                    pf.entry.ref_id,
                                                    vec![ChunkDiskMapperRef(0)],
                                                );
                                            } else {
                                                pending_ooo_promote.push(Arc::clone(&pf.entry));
                                                ooo_mmap_markers
                                                    .insert(pf.entry.ref_id, mmap_refs);
                                            }
                                        }
                                        if ok {
                                            self.head
                                                .metrics
                                                .ooo_samples_appended
                                                .with_label_values(&["histogram"])
                                                .inc();
                                            wbl_histograms.push(RefHistogramSample {
                                                ref_id: pf.entry.ref_id,
                                                st: pf.st,
                                                t: pf.t,
                                                h,
                                            });
                                            ooo_mint = ooo_mint.min(pf.t);
                                            ooo_maxt = ooo_maxt.max(pf.t);
                                        }
                                    } else if self.head.metrics.track_append(
                                        state.append_histogram_in_order(
                                            pf.st,
                                            pf.t,
                                            &h,
                                            chunk_range_ms,
                                            use_hist_st,
                                        )?,
                                    ) {
                                        in_order_mint = in_order_mint.min(pf.t);
                                        in_order_maxt = in_order_maxt.max(pf.t);
                                        pf.entry
                                            .in_order_max_time
                                            .store(state.max_time(), Ordering::Release);
                                    }
                                }
                            }
                            continue;
                        }
                        Some(Encoding::FloatHistogram | Encoding::FloatHistogramSt) => {
                            let fh = FloatHistogram {
                                sum: v,
                                ..FloatHistogram::default()
                            };
                            let check = {
                                let cr_guard = self.head.chunk_reader.read();
                                state.appendable_float_histogram(
                                    pf.t,
                                    &fh,
                                    self.head_maxt,
                                    self.min_valid_time,
                                    self.ooo_time_window,
                                    cr_guard.as_ref(),
                                )
                            };
                            if let Ok((is_ooo, is_dup)) = check {
                                if !is_dup {
                                    if is_ooo {
                                        let mut cw_guard = self.head.chunk_writer.lock();
                                        let (ok, chunk_created, mmap_refs) = state.insert_ooo(
                                            pf.entry.ref_id,
                                            pf.st,
                                            pf.t,
                                            OooSampleValue::FloatHistogram(fh.clone()),
                                            ooo_cap_max,
                                            use_xor2,
                                            use_hist_st,
                                            cw_guard.as_mut(),
                                        )?;
                                        drop(cw_guard);
                                        if chunk_created {
                                            self.head.metrics.chunks_created(1);
                                            let prev_markers =
                                                ooo_mmap_markers.get(&pf.entry.ref_id);
                                            if prev_markers.is_none()
                                                || prev_markers.is_some_and(|r| !r.is_empty())
                                            {
                                                flush_ooo_records(
                                                    &mut wbl_samples,
                                                    &mut wbl_histograms,
                                                    &mut wbl_float_histograms,
                                                    &mut ooo_mmap_markers,
                                                    &mut ooo_records,
                                                );
                                            }
                                            if mmap_refs.is_empty() {
                                                ooo_mmap_markers.insert(
                                                    pf.entry.ref_id,
                                                    vec![ChunkDiskMapperRef(0)],
                                                );
                                            } else {
                                                pending_ooo_promote.push(Arc::clone(&pf.entry));
                                                ooo_mmap_markers
                                                    .insert(pf.entry.ref_id, mmap_refs);
                                            }
                                        }
                                        if ok {
                                            self.head
                                                .metrics
                                                .ooo_samples_appended
                                                .with_label_values(&["histogram"])
                                                .inc();
                                            wbl_float_histograms.push(RefFloatHistogramSample {
                                                ref_id: pf.entry.ref_id,
                                                st: pf.st,
                                                t: pf.t,
                                                fh,
                                            });
                                            ooo_mint = ooo_mint.min(pf.t);
                                            ooo_maxt = ooo_maxt.max(pf.t);
                                        }
                                    } else if self.head.metrics.track_append(
                                        state.append_float_histogram_in_order(
                                            pf.st,
                                            pf.t,
                                            &fh,
                                            chunk_range_ms,
                                            use_hist_st,
                                        )?,
                                    ) {
                                        in_order_mint = in_order_mint.min(pf.t);
                                        in_order_maxt = in_order_maxt.max(pf.t);
                                        pf.entry
                                            .in_order_max_time
                                            .store(state.max_time(), Ordering::Release);
                                    }
                                }
                            }
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
                let Ok((is_ooo, is_dup)) = check else {
                    continue;
                };
                if is_dup {
                    continue;
                }
                if is_ooo {
                    let mut cw_guard = self.head.chunk_writer.lock();
                    let (ok, chunk_created, mmap_refs) = state.insert_ooo(
                        pf.entry.ref_id,
                        pf.st,
                        pf.t,
                        OooSampleValue::Float(v),
                        ooo_cap_max,
                        use_xor2,
                        use_hist_st,
                        cw_guard.as_mut(),
                    )?;
                    drop(cw_guard);
                    if chunk_created {
                        self.head.metrics.chunks_created(1);
                    }
                    if chunk_created {
                        let prev_markers = ooo_mmap_markers.get(&pf.entry.ref_id);
                        if prev_markers.is_none() || prev_markers.is_some_and(|r| !r.is_empty()) {
                            flush_ooo_records(
                                &mut wbl_samples,
                                &mut wbl_histograms,
                                &mut wbl_float_histograms,
                                &mut ooo_mmap_markers,
                                &mut ooo_records,
                            );
                        }
                        if mmap_refs.is_empty() {
                            ooo_mmap_markers
                                .insert(pf.entry.ref_id, vec![ChunkDiskMapperRef(0)]);
                        } else {
                            pending_ooo_promote.push(Arc::clone(&pf.entry));
                            ooo_mmap_markers.insert(pf.entry.ref_id, mmap_refs);
                        }
                    }
                    if ok {
                        self.head
                            .metrics
                            .ooo_samples_appended
                            .with_label_values(&["float"])
                            .inc();
                        wbl_samples.push(RefSample {
                            ref_id: pf.entry.ref_id,
                            st: pf.st,
                            t: pf.t,
                            v,
                        });
                        ooo_mint = ooo_mint.min(pf.t);
                        ooo_maxt = ooo_maxt.max(pf.t);
                    }
                } else {
                    if v.to_bits() == QUIET_ZERO_NAN_BITS {
                        v = 0.0;
                    }
                    if self.head.metrics.track_append(state.append_float_in_order(
                        pf.st,
                        pf.t,
                        v,
                        chunk_range_ms,
                        samples_per_chunk,
                        float_enc,
                    )?) {
                        in_order_mint = in_order_mint.min(pf.t);
                        in_order_maxt = in_order_maxt.max(pf.t);
                    }
                    pf.entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                }
            }

            // Commit integer histograms in this batch
            for i in h_start..h_end {
                self.hists_committed = i + 1;
                let ph = &mut self.histograms[i];
                let _pending_guard = PendingCommitGuard(&ph.entry.pending_commits);
                let mut state = ph.entry.inner.lock();
                let check = {
                    let cr_guard = self.head.chunk_reader.read();
                    state.appendable_histogram(
                        ph.t,
                        &ph.h,
                        self.head_maxt,
                        self.min_valid_time,
                        self.ooo_time_window,
                        cr_guard.as_ref(),
                    )
                };
                let Ok((is_ooo, is_dup)) = check else {
                    continue;
                };
                if is_dup {
                    continue;
                }
                if is_ooo {
                    let mut cw_guard = self.head.chunk_writer.lock();
                    let (ok, chunk_created, mmap_refs) = state.insert_ooo(
                        ph.entry.ref_id,
                        ph.st,
                        ph.t,
                        OooSampleValue::Histogram(ph.h.clone()),
                        ooo_cap_max,
                        use_xor2,
                        use_hist_st,
                        cw_guard.as_mut(),
                    )?;
                    drop(cw_guard);
                    if chunk_created {
                        self.head.metrics.chunks_created(1);
                    }
                    if chunk_created {
                        let prev_markers = ooo_mmap_markers.get(&ph.entry.ref_id);
                        if prev_markers.is_none() || prev_markers.is_some_and(|r| !r.is_empty()) {
                            flush_ooo_records(
                                &mut wbl_samples,
                                &mut wbl_histograms,
                                &mut wbl_float_histograms,
                                &mut ooo_mmap_markers,
                                &mut ooo_records,
                            );
                        }
                        if mmap_refs.is_empty() {
                            ooo_mmap_markers
                                .insert(ph.entry.ref_id, vec![ChunkDiskMapperRef(0)]);
                        } else {
                            pending_ooo_promote.push(Arc::clone(&ph.entry));
                            ooo_mmap_markers.insert(ph.entry.ref_id, mmap_refs);
                        }
                    }
                    if ok {
                        self.head
                            .metrics
                            .ooo_samples_appended
                            .with_label_values(&["histogram"])
                            .inc();
                        wbl_histograms.push(RefHistogramSample {
                            ref_id: ph.entry.ref_id,
                            st: ph.st,
                            t: ph.t,
                            h: std::mem::take(&mut ph.h),
                        });
                        ooo_mint = ooo_mint.min(ph.t);
                        ooo_maxt = ooo_maxt.max(ph.t);
                    }
                } else if self.head.metrics.track_append(state.append_histogram_in_order(
                    ph.st,
                    ph.t,
                    &ph.h,
                    chunk_range_ms,
                    use_hist_st,
                )?) {
                    in_order_mint = in_order_mint.min(ph.t);
                    in_order_maxt = in_order_maxt.max(ph.t);
                    ph.entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                }
            }

            // Commit float histograms in this batch
            for i in fh_start..fh_end {
                self.fhists_committed = i + 1;
                let pfh = &mut self.float_histograms[i];
                let _pending_guard = PendingCommitGuard(&pfh.entry.pending_commits);
                let mut state = pfh.entry.inner.lock();
                let check = {
                    let cr_guard = self.head.chunk_reader.read();
                    state.appendable_float_histogram(
                        pfh.t,
                        &pfh.fh,
                        self.head_maxt,
                        self.min_valid_time,
                        self.ooo_time_window,
                        cr_guard.as_ref(),
                    )
                };
                let Ok((is_ooo, is_dup)) = check else {
                    continue;
                };
                if is_dup {
                    continue;
                }
                if is_ooo {
                    let mut cw_guard = self.head.chunk_writer.lock();
                    let (ok, chunk_created, mmap_refs) = state.insert_ooo(
                        pfh.entry.ref_id,
                        pfh.st,
                        pfh.t,
                        OooSampleValue::FloatHistogram(pfh.fh.clone()),
                        ooo_cap_max,
                        use_xor2,
                        use_hist_st,
                        cw_guard.as_mut(),
                    )?;
                    drop(cw_guard);
                    if chunk_created {
                        self.head.metrics.chunks_created(1);
                    }
                    if chunk_created {
                        let prev_markers = ooo_mmap_markers.get(&pfh.entry.ref_id);
                        if prev_markers.is_none() || prev_markers.is_some_and(|r| !r.is_empty()) {
                            flush_ooo_records(
                                &mut wbl_samples,
                                &mut wbl_histograms,
                                &mut wbl_float_histograms,
                                &mut ooo_mmap_markers,
                                &mut ooo_records,
                            );
                        }
                        if mmap_refs.is_empty() {
                            ooo_mmap_markers
                                .insert(pfh.entry.ref_id, vec![ChunkDiskMapperRef(0)]);
                        } else {
                            pending_ooo_promote.push(Arc::clone(&pfh.entry));
                            ooo_mmap_markers.insert(pfh.entry.ref_id, mmap_refs);
                        }
                    }
                    if ok {
                        self.head
                            .metrics
                            .ooo_samples_appended
                            .with_label_values(&["histogram"])
                            .inc();
                        wbl_float_histograms.push(RefFloatHistogramSample {
                            ref_id: pfh.entry.ref_id,
                            st: pfh.st,
                            t: pfh.t,
                            fh: std::mem::take(&mut pfh.fh),
                        });
                        ooo_mint = ooo_mint.min(pfh.t);
                        ooo_maxt = ooo_maxt.max(pfh.t);
                    }
                } else if self
                    .head
                    .metrics
                    .track_append(state.append_float_histogram_in_order(
                        pfh.st,
                        pfh.t,
                        &pfh.fh,
                        chunk_range_ms,
                        use_hist_st,
                    )?)
                {
                    in_order_mint = in_order_mint.min(pfh.t);
                    in_order_maxt = in_order_maxt.max(pfh.t);
                    pfh.entry
                        .in_order_max_time
                        .store(state.max_time(), Ordering::Release);
                }
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

        // Flush chunk_writer and refresh chunk_reader BEFORE exposing any OOO MmappedChunk.
        if !pending_ooo_promote.is_empty() {
            if let Some(ref mut cw) = *self.head.chunk_writer.lock() {
                cw.flush()?;
            }
            self.head.refresh_chunk_reader()?;
            for entry in pending_ooo_promote {
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo {
                    for (mc, _) in ooo.ooo_pending_mmapped.drain(..) {
                        ooo.ooo_mmapped_chunks.push(mc);
                    }
                }
            }
        }

        if in_order_mint <= in_order_maxt {
            self.head.update_min_max_time(in_order_mint, in_order_maxt);
        }
        if ooo_mint <= ooo_maxt {
            self.head.update_min_ooo_max_ooo_time(ooo_mint, ooo_maxt);
        }

        // 3. Log OOO records to WBL if any (samples before their corresponding MmapMarkers).
        flush_ooo_records(
            &mut wbl_samples,
            &mut wbl_histograms,
            &mut wbl_float_histograms,
            &mut ooo_mmap_markers,
            &mut ooo_records,
        );
        if !ooo_records.is_empty() {
            if let Some(ref mut wbl) = *self.head.wbl.lock() {
                let slices: Vec<&[u8]> = ooo_records.iter().map(|v| v.as_slice()).collect();
                if !slices.is_empty() {
                    wbl.log(&slices)?;
                }
            }
        }

        Ok(())
    }

    /// Rolls back buffered samples/exemplars while still logging newly created series declarations to WAL.
    pub fn rollback(mut self) -> Result<(), HeadError> {
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
        self.release_remaining_pending_commits();
        self.return_scratch();
        log_res
    }

    fn log_wal(&mut self) -> Result<(), HeadError> {
        let mut wal_guard = self.head.wal.lock();
        let Some(ref mut wal) = *wal_guard else {
            return Ok(());
        };
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
            let res = wal.log(&[&rec]);
            self.wal_buf = rec;
            res?;
            return Ok(());
        }

        let mut recs: Vec<Vec<u8>> = Vec::new();

        if !self.series_refs.is_empty() {
            recs.push(enc.series(&self.series_refs, Vec::new()));
        }
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

        if !recs.is_empty() {
            let slices: Vec<&[u8]> = recs.iter().map(|v| v.as_slice()).collect();
            wal.log(&slices)?;
        }
        Ok(())
    }
}

impl Drop for HeadAppender<'_> {
    fn drop(&mut self) {
        if !self.closed {
            self.closed = true;
            self.head.metrics.active_appenders.dec();
        }
        self.release_remaining_pending_commits();
        self.return_scratch();
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
            entry_size <= 256,
            "SeriesEntry size {entry_size}B exceeded 256B budget"
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
                metrics: Some(crate::postings_cache::PostingsForMatchersCacheMetrics::new(
                    "prometheus_tsdb_",
                    Some(&reg),
                )),
                ..defaults.postings_cache.clone()
            },
            ..defaults
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

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

        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
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
        assert_eq!(samples.len(), 51);
        assert_eq!(samples.last().unwrap().t, 1_000_000 + 60 * 1000);
        assert_eq!(samples.last().unwrap().value, SampleValue::Float(777.0));
        assert!(head
            .get_entry_by_sorted_pairs(&[("__name__", "metric_1"), ("job", "worker")])
            .is_none());
        head.close().unwrap();
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
        // Exact duplicate in same uncommitted batch (pending_commits > 0 -> no-op)
        app.append_float(Some(r), &pairs, 1_000_000, 1.0).unwrap();
        // Conflicting duplicate in same uncommitted batch -> error
        assert!(matches!(
            app.append_float(Some(r), &pairs, 1_000_000, 2.0),
            Err(HeadError::DuplicateSampleWithDetails(_))
        ));
        // Newer in-order sample in same uncommitted batch -> succeeds
        app.append_float(Some(r), &pairs, 1_005_000, 5.0).unwrap();
        // OOO sample within window against batch_last_sample (1_005_000) -> accepted as OOO
        app.append_float(Some(r), &pairs, 1_002_000, 2.5).unwrap();
        // Duplicate against updated batch_last_sample (1_005_000) -> error
        assert!(matches!(
            app.append_float(Some(r), &pairs, 1_005_000, 9.0),
            Err(HeadError::DuplicateSampleWithDetails(_))
        ));
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
                let m = st.metadata.unwrap();
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
}
