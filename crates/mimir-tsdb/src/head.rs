//! Striped TSDB Head with zero-allocation hot-path appends, OOO support, mmap `chunks_head`,
//! WAL/WBL logging & crash recovery replay, and truncation/checkpointing.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/head.go,
//! github.com/prometheus/prometheus/tsdb/head_append.go,
//! github.com/prometheus/prometheus/tsdb/head_read.go,
//! and github.com/prometheus/prometheus/tsdb/head_wal.go.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use hashbrown::HashTable;
use parking_lot::{Mutex, RwLock};
use prometheus::core::{Collector, Desc};
use prometheus::proto::{
    Metric, MetricFamily, MetricType as PromMetricType, Quantile, Summary as ProtoSummary,
};
use prometheus::{CounterVec, Histogram as PromHistogram, HistogramOpts, Opts, Registry};
use smallvec::SmallVec;
use thiserror::Error;
use xxhash_rust::xxh64::Xxh64;

use crate::chunkenc::float_histogram::AppendFloatHistogramOutcome;
use crate::chunkenc::float_histogram_st::AppendFloatHistogramStOutcome;
use crate::chunkenc::histogram::AppendHistogramOutcome;
use crate::chunkenc::histogram_st::AppendHistogramStOutcome;
use crate::chunkenc::{
    is_stale_nan, ChunkError, EncodedChunk, Encoding, FloatHistogram, FloatHistogramChunk,
    FloatHistogramStChunk, Histogram, HistogramChunk, HistogramStChunk, OooChunk, OooSampleValue,
    Xor2Chunk, XorChunk, MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND, MIN_SAMPLES_PER_HISTOGRAM_CHUNK,
    QUIET_ZERO_NAN_BITS, TARGET_BYTES_PER_HISTOGRAM_CHUNK,
};
use crate::chunks_block::ChunkMeta;
use crate::compact::{decode_chunk_samples, CompactionSeries, SampleValue};
use crate::exemplar::{CircularExemplarStorage, Exemplar, ExemplarError};
use crate::head_chunks::{
    ChunkDiskMapperRef, HeadChunksError, HeadChunksReader, HeadChunksWriter,
    MAX_HEAD_CHUNK_FILE_SIZE,
};
use crate::labels::{encode_stringlabel_size, Labels, SymbolLabels};
use crate::metadata::{MetadataError, MetadataLimits, MetricMetadata, UserMetricsMetadata};
use crate::postings::{postings_for_matchers, MemPostings, PostingsError};
use crate::matcher::LabelMatcher;
use crate::record::{
    DecodedRecord, MetricType, RecordDecoder, RecordEncoder, RecordError, RefExemplar,
    RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefMmapMarker, RefSample, RefSeries,
};
use crate::symbols::SymbolTable;
use crate::tombstones::{Interval, MemTombstones};
use crate::wlog::{
    checkpoint_dir, create_checkpoint, delete_checkpoints, delete_temp_checkpoints,
    last_checkpoint, read_min_valid_time, segments_range, CompressionType, SegmentRange,
    SegmentTracker, WlogError, WlogReader, WlogWriter, DEFAULT_SEGMENT_SIZE, WBL_DIR_NAME,
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
    #[error("out of bounds")]
    OutOfBounds,
    #[error("out of order sample")]
    OutOfOrderSample,
    #[error("too old sample")]
    TooOldSample,
    #[error("duplicate sample for timestamp")]
    DuplicateSampleForTimestamp,
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
    #[error("missing metric name")]
    MissingMetricName,
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
            metadata_limits: MetadataLimits::default(),
            series_lifecycle_callback: None,
            registry: None,
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
fn hash_sorted_pairs(pairs: &[(&str, &str)]) -> u64 {
    let mut stack_buf = [0u8; 512];
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
            pos = write_stringlabel_to_slice(&mut stack_buf, pos, k.as_bytes());
            pos = write_stringlabel_to_slice(&mut stack_buf, pos, v.as_bytes());
        } else {
            let mut hb = Vec::with_capacity((pos + need) * 2);
            hb.extend_from_slice(&stack_buf[..pos]);
            encode_stringlabel_size(&mut hb, k.len());
            hb.extend_from_slice(k.as_bytes());
            encode_stringlabel_size(&mut hb, v.len());
            hb.extend_from_slice(v.as_bytes());
            heap_buf = Some(hb);
        }
    }

    match heap_buf {
        Some(ref hb) => xxh64_oneshot(hb),
        None => xxh64_oneshot(&stack_buf[..pos]),
    }
}

#[inline]
fn xxh64_oneshot(data: &[u8]) -> u64 {
    let mut h = Xxh64::new(0);
    h.update(data);
    h.digest()
}

#[inline]
fn write_stringlabel_to_slice(dst: &mut [u8], mut pos: usize, bytes: &[u8]) -> usize {
    let len = bytes.len();
    if len < 255 {
        dst[pos] = len as u8;
        pos += 1;
    } else {
        dst[pos] = 255;
        dst[pos + 1] = (len & 0xff) as u8;
        dst[pos + 2] = ((len >> 8) & 0xff) as u8;
        dst[pos + 3] = ((len >> 16) & 0xff) as u8;
        pos += 4;
    }
    dst[pos..pos + len].copy_from_slice(bytes);
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
    ooo_mmapped_chunks: Vec<MmappedChunk>,
}

#[derive(Clone, Debug)]
enum LastSample {
    None,
    Float(f64),
    Histogram(Histogram),
    FloatHistogram(FloatHistogram),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesMetadata {
    pub metric_type: MetricType,
    pub unit: String,
    pub help: String,
}

#[derive(Debug)]
pub struct SeriesEntry {
    pub ref_id: u64,
    pub hash: u64,
    pub symbol_labels: SymbolLabels,
    pub labels: Labels,
    inner: Mutex<SeriesState>,
}

#[derive(Debug)]
struct SeriesState {
    head_chunks: Vec<MemChunk>,
    mmapped_chunks: Vec<MmappedChunk>,
    ooo: Option<Box<SeriesOooState>>,
    next_at: i64,
    computed_hist_end_time: bool,
    last_sample: LastSample,
    created_timestamp: i64,
    metadata: Option<SeriesMetadata>,
    last_exemplar_ts: Option<i64>,
    gced: bool,
    pending_commits: u32,
}

impl SeriesState {
    fn new() -> Self {
        Self {
            head_chunks: Vec::with_capacity(1),
            mmapped_chunks: Vec::new(),
            ooo: None,
            next_at: i64::MIN,
            computed_hist_end_time: false,
            last_sample: LastSample::None,
            created_timestamp: 0,
            metadata: None,
            last_exemplar_ts: None,
            gced: false,
            pending_commits: 0,
        }
    }

    #[inline]
    fn has_any_in_order_chunks(&self) -> bool {
        !self.head_chunks.is_empty() || !self.mmapped_chunks.is_empty()
    }

    #[inline]
    fn max_time(&self) -> i64 {
        if let Some(c) = self.head_chunks.last() {
            c.max_time
        } else if let Some(c) = self.mmapped_chunks.last() {
            c.max_time
        } else {
            i64::MIN
        }
    }

    #[inline]
    fn min_time(&self) -> i64 {
        if let Some(c) = self.mmapped_chunks.first() {
            c.min_time
        } else if let Some(c) = self.head_chunks.first() {
            c.min_time
        } else {
            i64::MAX
        }
    }

    #[inline]
    fn mm_max_time(&self) -> i64 {
        self.mmapped_chunks
            .last()
            .map_or(i64::MIN, |c| c.max_time)
    }

    #[inline]
    fn latest_in_order_encoding(&self) -> Option<Encoding> {
        if let Some(c) = self.head_chunks.last() {
            Some(c.chunk.encoding())
        } else {
            self.mmapped_chunks.last().map(|c| c.encoding)
        }
    }

    fn last_in_order_sample(&self, chunks_reader: Option<&HeadChunksReader>) -> Option<LastSample> {
        if !matches!(self.last_sample, LastSample::None) {
            return Some(self.last_sample.clone());
        }
        let mmc = self.mmapped_chunks.last()?;
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
                            Err(HeadError::DuplicateSampleForTimestamp)
                        }
                        LastSample::Float(prev_v) => {
                            if prev_v.to_bits() != v.to_bits()
                                && v.to_bits() != QUIET_ZERO_NAN_BITS
                            {
                                Err(HeadError::DuplicateSampleForTimestamp)
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
            Encoding::Xor2 => EncodedChunk::Xor2(Xor2Chunk::new()),
            Encoding::Histogram => EncodedChunk::Histogram(HistogramChunk::new()),
            Encoding::HistogramSt => EncodedChunk::HistogramSt(HistogramStChunk::new()),
            Encoding::FloatHistogram => EncodedChunk::FloatHistogram(FloatHistogramChunk::new()),
            Encoding::FloatHistogramSt => {
                EncodedChunk::FloatHistogramSt(FloatHistogramStChunk::new())
            }
            _ => EncodedChunk::Xor(XorChunk::new()),
        };
        self.head_chunks.push(MemChunk {
            chunk,
            min_time: mint,
            max_time: i64::MIN,
        });
        self.next_at = range_for_timestamp(mint, chunk_range_ms);
        self.computed_hist_end_time = false;
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
            if let Some(last_mm) = self.mmapped_chunks.last() {
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
                let mut app = xc.appender()?;
                app.append(st, t, v)?;
            }
            EncodedChunk::Xor2(ref mut xc) => {
                let mut app = xc.appender()?;
                app.append(st, t, v)?;
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
            if let Some(last_mm) = self.mmapped_chunks.last() {
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

        let next_chunk_range_start = if self.computed_hist_end_time {
            range_for_timestamp(c_min, chunk_range_ms)
        } else {
            self.next_at
        };

        if !self.computed_hist_end_time && num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK / 4 {
            let ratio = (TARGET_BYTES_PER_HISTOGRAM_CHUNK as f64) / (num_bytes as f64);
            self.next_at = compute_chunk_end_time(c_min, c_max, self.next_at, ratio);
            self.computed_hist_end_time = true;
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
                let mut app = hc.appender()?;
                match app.append_histogram(None, st, t, h, false)? {
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
                        self.computed_hist_end_time = false;
                        chunk_created = true;
                    }
                }
            }
            EncodedChunk::HistogramSt(ref mut hc) => {
                let mut app = hc.appender()?;
                match app.append_histogram(None, st, t, h, false)? {
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
                        self.computed_hist_end_time = false;
                        chunk_created = true;
                    }
                }
            }
            _ => unreachable!(),
        }
        self.last_sample = LastSample::Histogram(h.clone());
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
            if let Some(last_mm) = self.mmapped_chunks.last() {
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

        let next_chunk_range_start = if self.computed_hist_end_time {
            range_for_timestamp(c_min, chunk_range_ms)
        } else {
            self.next_at
        };

        if !self.computed_hist_end_time && num_bytes >= TARGET_BYTES_PER_HISTOGRAM_CHUNK / 4 {
            let ratio = (TARGET_BYTES_PER_HISTOGRAM_CHUNK as f64) / (num_bytes as f64);
            self.next_at = compute_chunk_end_time(c_min, c_max, self.next_at, ratio);
            self.computed_hist_end_time = true;
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
                let mut app = fhc.appender()?;
                match app.append_float_histogram(None, st, t, fh, false)? {
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
                        self.computed_hist_end_time = false;
                        chunk_created = true;
                    }
                }
            }
            EncodedChunk::FloatHistogramSt(ref mut fhc) => {
                let mut app = fhc.appender()?;
                match app.append_float_histogram(None, st, t, fh, false)? {
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
                        self.computed_hist_end_time = false;
                        chunk_created = true;
                    }
                }
            }
            _ => unreachable!(),
        }
        self.last_sample = LastSample::FloatHistogram(fh.clone());
        if st != 0 && self.created_timestamp == 0 {
            self.created_timestamp = st;
        }
        Ok((true, chunk_created))
    }

    fn insert_ooo(
        &mut self,
        ref_id: u64,
        st: i64,
        t: i64,
        val: OooSampleValue,
        ooo_cap_max: usize,
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
                    let cut_chunks = old.chunk.to_encoded_chunks(i64::MIN, i64::MAX)?;
                    for cc in cut_chunks {
                        let r = writer.write_chunk(
                            ref_id,
                            cc.min_time,
                            cc.max_time,
                            &cc.chunk,
                            true,
                        )?;
                        mmap_refs.push(r);
                        ooo.ooo_mmapped_chunks.push(MmappedChunk {
                            ref_id: r,
                            min_time: cc.min_time,
                            max_time: cc.max_time,
                            num_samples: cc.chunk.num_samples() as u16,
                            encoding: cc.chunk.encoding(),
                        });
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
    by_hash: HashTable<Arc<SeriesEntry>>,
    by_id: HashMap<u64, Arc<SeriesEntry>>,
}

impl SeriesStripe {
    fn new() -> Self {
        Self {
            by_hash: HashTable::new(),
            by_id: HashMap::new(),
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

    fn get_by_sorted_pairs(
        &self,
        hash: u64,
        pairs: &[(&str, &str)],
        symbols: &SymbolTable,
    ) -> Option<Arc<SeriesEntry>> {
        let idx = self.stripe_idx(hash);
        let stripe = self.stripes[idx].read();
        stripe
            .by_hash
            .find(hash, |entry| {
                entry.hash == hash && symbol_labels_eq_pairs(&entry.symbol_labels, symbols, pairs)
            })
            .cloned()
    }

    fn get_by_hash_and_labels(&self, hash: u64, labels: &Labels) -> Option<Arc<SeriesEntry>> {
        let idx = self.stripe_idx(hash);
        let stripe = self.stripes[idx].read();
        stripe
            .by_hash
            .find(hash, |entry| entry.hash == hash && &entry.labels == labels)
            .cloned()
    }

    fn insert(&self, entry: Arc<SeriesEntry>) {
        let h_idx = self.stripe_idx(entry.hash);
        let r_idx = self.stripe_idx(entry.ref_id);

        if h_idx == r_idx {
            let mut s = self.stripes[h_idx].write();
            s.by_id.insert(entry.ref_id, Arc::clone(&entry));
            let hash = entry.hash;
            s.by_hash.insert_unique(hash, entry, |e| e.hash);
        } else {
            {
                let mut hs = self.stripes[h_idx].write();
                let hash = entry.hash;
                hs.by_hash.insert_unique(hash, Arc::clone(&entry), |e| e.hash);
            }
            {
                let mut rs = self.stripes[r_idx].write();
                rs.by_id.insert(entry.ref_id, entry);
            }
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
                .find_entry(entry.hash, |e| e.ref_id == entry.ref_id)
            {
                found.remove();
            }
        } else {
            {
                let mut hs = self.stripes[h_idx].write();
                if let Ok(found) = hs
                    .by_hash
                    .find_entry(entry.hash, |e| e.ref_id == entry.ref_id)
                {
                    found.remove();
                }
            }
            {
                let mut rs = self.stripes[r_idx].write();
                rs.by_id.remove(&entry.ref_id);
            }
        }
    }

    fn unlink_hash(&self, hash: u64, ref_id: u64) {
        let h_idx = self.stripe_idx(hash);
        let mut hs = self.stripes[h_idx].write();
        if let Ok(found) = hs.by_hash.find_entry(hash, |e| e.ref_id == ref_id) {
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
}

#[inline]
fn symbol_labels_eq_pairs(sl: &SymbolLabels, symbols: &SymbolTable, pairs: &[(&str, &str)]) -> bool {
    let slice = sl.as_slice();
    if slice.len() != pairs.len() {
        return false;
    }
    for (&(kid, vid), &(k, v)) in slice.iter().zip(pairs.iter()) {
        if symbols.resolve(kid) != k || symbols.resolve(vid) != v {
            return false;
        }
    }
    true
}

/// Series chunks + tombstones returned by [`Head::series_chunks`].
pub type SeriesChunksResult = Option<(Labels, Vec<ChunkMeta>, Vec<Interval>)>;

struct WalFsyncSummaryInner {
    desc: Desc,
    state: Mutex<(u64, f64)>,
}

#[derive(Clone)]
struct WalFsyncSummary(Arc<WalFsyncSummaryInner>);

impl WalFsyncSummary {
    fn new(reg: Option<&Registry>) -> Self {
        let desc = Desc::new(
            "prometheus_tsdb_wal_fsync_duration_seconds".to_string(),
            "Duration of write log fsync.".to_string(),
            vec![],
            HashMap::new(),
        )
        .expect("valid wal fsync summary desc");
        let s = Self(Arc::new(WalFsyncSummaryInner {
            desc,
            state: Mutex::new((0, 0.0)),
        }));
        if let Some(r) = reg {
            let _ = r.register(Box::new(s.clone()));
        }
        s
    }

    fn observe(&self, secs: f64) {
        let mut g = self.0.state.lock();
        g.0 += 1;
        g.1 += secs;
    }
}

impl Collector for WalFsyncSummary {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.0.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let (count, sum) = *self.0.state.lock();
        let mut s = ProtoSummary::default();
        s.set_sample_count(count);
        s.set_sample_sum(sum);
        let qs: Vec<Quantile> = [0.5, 0.9, 0.99]
            .into_iter()
            .map(|qv| {
                let mut q = Quantile::default();
                q.set_quantile(qv);
                q.set_value(f64::NAN);
                q
            })
            .collect();
        s.set_quantile(qs.into());

        let mut m = Metric::default();
        m.set_summary(s);
        let mut mf = MetricFamily::default();
        mf.set_name(self.0.desc.fq_name.clone());
        mf.set_help(self.0.desc.help.clone());
        mf.set_field_type(PromMetricType::SUMMARY);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

struct HeadMetrics {
    ooo_samples_appended: CounterVec,
    ooo_histogram: PromHistogram,
    wal_fsync_duration: WalFsyncSummary,
}

impl HeadMetrics {
    fn new(reg: Option<&Registry>) -> Self {
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
        let wal_fsync_duration = WalFsyncSummary::new(reg);
        Self {
            ooo_samples_appended,
            ooo_histogram,
            wal_fsync_duration,
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
    tombstones: MemTombstones,
    exemplars: CircularExemplarStorage,
    metadata: UserMetricsMetadata,

    wal: Mutex<Option<WlogWriter>>,
    wbl: Mutex<Option<WlogWriter>>,
    chunk_writer: Mutex<Option<HeadChunksWriter>>,
    chunk_reader: RwLock<Option<HeadChunksReader>>,

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
    wal_expiries: Mutex<HashMap<u64, i64>>,
    wbl_pinned_refs: Mutex<HashSet<u64>>,
    lifecycle_callback: RwLock<Option<Arc<dyn SeriesLifecycleCallback>>>,
    series_gauge: Option<prometheus::Gauge>,
    series_created_total: Option<prometheus::Counter>,
    series_removed_total: Option<prometheus::Counter>,
}

impl Head {
    /// Opens (or initializes) a [`Head`] in `dir`, replaying `chunks_head`, `wal`, and `wbl`.
    pub fn open(dir: impl AsRef<Path>, opts: HeadOptions, min_valid_time: i64) -> Result<Self, HeadError> {
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
        let metrics = HeadMetrics::new(opts.registry.as_deref());
        let head = Self {
            dir: dir.clone(),
            opts: opts.clone(),
            metrics,
            symbols: RwLock::new(SymbolTable::new()),
            series: StripeSeries::new(opts.stripe_size),
            series_creation_mtx: Mutex::new(()),
            postings: MemPostings::new_unordered(),
            tombstones: MemTombstones::new(),
            exemplars: CircularExemplarStorage::new(
                if opts.enable_exemplar_storage {
                    opts.max_exemplars
                } else {
                    0
                },
                opts.ooo_time_window_ms,
            ),
            metadata: UserMetricsMetadata::new(),
            wal: Mutex::new(None),
            wbl: Mutex::new(None),
            chunk_writer: Mutex::new(None),
            chunk_reader: RwLock::new(None),
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
            wal_expiries: Mutex::new(HashMap::new()),
            wbl_pinned_refs: Mutex::new(HashSet::new()),
            lifecycle_callback: RwLock::new(lifecycle_cb),
            series_gauge,
            series_created_total,
            series_removed_total,
        };

        head.replay_on_open(&wal_dir, &wbl_dir, &chunks_dir)?;

        // Open writers after replay completes.
        let wal_writer =
            WlogWriter::with_segment_size(&wal_dir, opts.wal_segment_size, opts.wal_compression)?;
        *head.wal.lock() = Some(wal_writer);

        if opts.ooo_time_window_ms > 0 || wbl_dir.exists() {
            let wbl_writer = WlogWriter::with_segment_size(
                &wbl_dir,
                opts.wal_segment_size,
                opts.wal_compression,
            )?;
            *head.wbl.lock() = Some(wbl_writer);
        }

        let chunk_writer =
            HeadChunksWriter::with_max_file_size(&chunks_dir, opts.max_head_chunk_file_size)?;
        *head.chunk_writer.lock() = Some(chunk_writer);
        let chunk_reader = HeadChunksReader::open(&chunks_dir)?;
        *head.chunk_reader.write() = Some(chunk_reader);

        Ok(head)
    }

    fn replay_on_open(
        &self,
        wal_dir: &Path,
        wbl_dir: &Path,
        chunks_dir: &Path,
    ) -> Result<(), HeadError> {
        let min_valid_time = self.min_valid_time.load(Ordering::Relaxed);

        // 1. Load mmapped chunks from chunks_head.
        let mut mmapped_map: HashMap<u64, Vec<MmappedChunk>> = HashMap::new();
        let mut ooo_mmapped_map: HashMap<u64, Vec<MmappedChunk>> = HashMap::new();
        let mut last_mmap_ref = ChunkDiskMapperRef(0);

        let reader = HeadChunksReader::open(chunks_dir)?;
        for rec in reader.collect_all_chunks()? {
            last_mmap_ref = rec.chunk_ref;
            if !rec.is_ooo && rec.maxt < min_valid_time {
                continue;
            }
            if !rec.encoding.is_valid() {
                continue;
            }
            let mc = MmappedChunk {
                ref_id: rec.chunk_ref,
                min_time: rec.mint,
                max_time: rec.maxt,
                num_samples: rec.num_samples,
                encoding: rec.encoding,
            };
            if rec.is_ooo {
                ooo_mmapped_map.entry(rec.series_ref).or_default().push(mc);
            } else {
                mmapped_map.entry(rec.series_ref).or_default().push(mc);
            }
        }
        *self.chunk_reader.write() = Some(reader);

        // 2. Replay WAL checkpoint + segments.
        let mut multi_ref: HashMap<u64, u64> = HashMap::new();
        let mut start_from = 0usize;
        if let Some((cp_dir, cp_idx)) = last_checkpoint(wal_dir)? {
            let mut r = WlogReader::open_dir(&cp_dir)?;
            self.replay_wal_reader(&mut r, &mut multi_ref, &mut mmapped_map, &mut ooo_mmapped_map)?;
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
                self.replay_wal_reader(
                    &mut r,
                    &mut multi_ref,
                    &mut mmapped_map,
                    &mut ooo_mmapped_map,
                )?;
            }
        }

        // 3. Replay WBL segments if present.
        if wbl_dir.exists() {
            if let Some((first_wbl, last_wbl)) = segments_range(wbl_dir)? {
                let mut r = WlogReader::open_ranges(&[SegmentRange {
                    dir: wbl_dir.to_path_buf(),
                    first: Some(first_wbl),
                    last: Some(last_wbl),
                }])?;
                self.replay_wbl_reader(&mut r, &multi_ref, last_mmap_ref)?;
            }
        }

        self.postings.ensure_order();

        // Clamp min_time to min_valid_time if initialized below it.
        if self.initialized() && self.min_time() < min_valid_time {
            self.min_time.store(min_valid_time, Ordering::Relaxed);
        }
        Ok(())
    }

    fn replay_wal_reader<R: io::Read + SegmentTracker>(
        &self,
        r: &mut WlogReader<R>,
        multi_ref: &mut HashMap<u64, u64>,
        mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
        ooo_mmapped_map: &mut HashMap<u64, Vec<MmappedChunk>>,
    ) -> Result<(), HeadError> {
        let dec = RecordDecoder::new();
        let min_valid_time = self.min_valid_time.load(Ordering::Relaxed);

        while let Some(rec) = r.next_record()? {
            match dec.decode_record(rec)? {
                DecodedRecord::Series(series_list) => {
                    for rs in series_list {
                        let hash = rs.labels.hash();
                        let (entry, created) =
                            self.get_or_create_with_id(Some(rs.ref_id), hash, &rs.labels)?;
                        self.bump_last_series_id(rs.ref_id);
                        if !created {
                            multi_ref.insert(rs.ref_id, entry.ref_id);
                        }

                        let mmc = mmapped_map.remove(&rs.ref_id).unwrap_or_default();
                        let ooo_mmc = ooo_mmapped_map.remove(&rs.ref_id).unwrap_or_default();
                        if !mmc.is_empty() || !ooo_mmc.is_empty() {
                            let mut st = entry.inner.lock();
                            if !mmc.is_empty() {
                                for m in &mmc {
                                    self.update_min_max_time(m.min_time, m.max_time);
                                }
                                st.mmapped_chunks.extend(mmc);
                            }
                            if !ooo_mmc.is_empty() {
                                for m in &ooo_mmc {
                                    self.update_min_ooo_max_ooo_time(m.min_time, m.max_time);
                                }
                                let ooo = st
                                    .ooo
                                    .get_or_insert_with(|| Box::new(SeriesOooState::default()));
                                ooo.ooo_mmapped_chunks.extend(ooo_mmc);
                            }
                        }
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
                            continue;
                        };
                        if s.v.to_bits() == QUIET_ZERO_NAN_BITS {
                            s.v = 0.0;
                        }
                        let mut state = entry.inner.lock();
                        if s.t <= state.mm_max_time() {
                            continue;
                        }
                        if is_stale_nan(s.v) {
                            match state.latest_in_order_encoding() {
                                Some(Encoding::Histogram | Encoding::HistogramSt) => {
                                    let h = Histogram {
                                        sum: s.v,
                                        ..Histogram::default()
                                    };
                                    if state
                                        .append_histogram_in_order(
                                            s.st,
                                            s.t,
                                            &h,
                                            self.opts.chunk_range_ms,
                                            self.opts.enable_histogram_st_encoding,
                                        )?
                                        .0
                                    {
                                        self.update_min_max_time(s.t, s.t);
                                    }
                                    continue;
                                }
                                Some(Encoding::FloatHistogram | Encoding::FloatHistogramSt) => {
                                    let fh = FloatHistogram {
                                        sum: s.v,
                                        ..FloatHistogram::default()
                                    };
                                    if state
                                        .append_float_histogram_in_order(
                                            s.st,
                                            s.t,
                                            &fh,
                                            self.opts.chunk_range_ms,
                                            self.opts.enable_histogram_st_encoding,
                                        )?
                                        .0
                                    {
                                        self.update_min_max_time(s.t, s.t);
                                    }
                                    continue;
                                }
                                _ => {}
                            }
                        }
                        if state
                            .append_float_in_order(
                                s.st,
                                s.t,
                                s.v,
                                self.opts.chunk_range_ms,
                                self.opts.samples_per_chunk,
                                self.float_chunk_encoding(),
                            )?
                            .0
                        {
                            self.update_min_max_time(s.t, s.t);
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
                        let Some(entry) = self.series.get_by_id(hs.ref_id) else {
                            continue;
                        };
                        let mut state = entry.inner.lock();
                        if hs.t <= state.mm_max_time() {
                            continue;
                        }
                        if state
                            .append_histogram_in_order(
                                hs.st,
                                hs.t,
                                &hs.h,
                                self.opts.chunk_range_ms,
                                self.opts.enable_histogram_st_encoding,
                            )?
                            .0
                        {
                            self.update_min_max_time(hs.t, hs.t);
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
                        let Some(entry) = self.series.get_by_id(fhs.ref_id) else {
                            continue;
                        };
                        let mut state = entry.inner.lock();
                        if fhs.t <= state.mm_max_time() {
                            continue;
                        }
                        if state
                            .append_float_histogram_in_order(
                                fhs.st,
                                fhs.t,
                                &fhs.fh,
                                self.opts.chunk_range_ms,
                                self.opts.enable_histogram_st_encoding,
                            )?
                            .0
                        {
                            self.update_min_max_time(fhs.t, fhs.t);
                        }
                    }
                }
                DecodedRecord::Tombstones(stones) => {
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
                            continue;
                        };
                        if self
                            .exemplars
                            .add_exemplar(&entry.labels, Exemplar::new(ex.labels, ex.v, ex.t))
                            .is_ok()
                        {
                            let mut st = entry.inner.lock();
                            st.last_exemplar_ts = Some(
                                st.last_exemplar_ts.map_or(ex.t, |prev| prev.max(ex.t)),
                            );
                        }
                    }
                }
                DecodedRecord::Metadata(metas) => {
                    for mut m in metas {
                        if let Some(&mapped) = multi_ref.get(&m.ref_id) {
                            m.ref_id = mapped;
                        }
                        let Some(entry) = self.series.get_by_id(m.ref_id) else {
                            continue;
                        };
                        let metric_type = MetricType::from_u8(m.metric_type);
                        let mut st = entry.inner.lock();
                        st.metadata = Some(SeriesMetadata {
                            metric_type,
                            unit: m.unit,
                            help: m.help,
                        });
                    }
                }
                DecodedRecord::MinValidTime(_)
                | DecodedRecord::MmapMarkers(_)
                | DecodedRecord::Unknown(_) => {}
            }
        }
        Ok(())
    }

    fn replay_wbl_reader<R: io::Read + SegmentTracker>(
        &self,
        r: &mut WlogReader<R>,
        multi_ref: &HashMap<u64, u64>,
        last_mmap_ref: ChunkDiskMapperRef,
    ) -> Result<(), HeadError> {
        let dec = RecordDecoder::new();
        let (last_seq, last_off) = last_mmap_ref.unpack();
        let mut pinned = HashSet::new();

        while let Some(rec) = r.next_record()? {
            match dec.decode_record(rec)? {
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
                            let (ok, _, _) = st.insert_ooo(
                                entry.ref_id,
                                s.st,
                                s.t,
                                OooSampleValue::Float(s.v),
                                self.opts.ooo_cap_max,
                                None,
                            )?;
                            if ok {
                                self.update_min_ooo_max_ooo_time(s.t, s.t);
                            }
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
                            let (ok, _, _) = st.insert_ooo(
                                entry.ref_id,
                                hs.st,
                                hs.t,
                                OooSampleValue::Histogram(hs.h),
                                self.opts.ooo_cap_max,
                                None,
                            )?;
                            if ok {
                                self.update_min_ooo_max_ooo_time(hs.t, hs.t);
                            }
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
                            let (ok, _, _) = st.insert_ooo(
                                entry.ref_id,
                                fhs.st,
                                fhs.t,
                                OooSampleValue::FloatHistogram(fhs.fh),
                                self.opts.ooo_cap_max,
                                None,
                            )?;
                            if ok {
                                self.update_min_ooo_max_ooo_time(fhs.t, fhs.t);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if !pinned.is_empty() {
            self.wbl_pinned_refs.lock().extend(pinned);
        }
        Ok(())
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
                let wbl_writer = WlogWriter::with_segment_size(
                    &wbl_dir,
                    self.opts.wal_segment_size,
                    self.opts.wal_compression,
                )?;
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
                let start = Instant::now();
                wal.sync()?;
                self.metrics
                    .wal_fsync_duration
                    .observe(start.elapsed().as_secs_f64());
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
        let syms = self.symbols.read();
        let (Some(nid), Some(vid)) = (syms.get(name), syms.get(value)) else {
            return Vec::new();
        };
        self.postings.postings_for_id(nid, vid)
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

    pub fn compactable(&self) -> bool {
        if !self.initialized() {
            return false;
        }
        let min_block_end = range_for_timestamp(self.min_time(), self.opts.chunk_range_ms);
        min_block_end < self.appendable_min_valid_time()
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
        let syms = self.symbols.read();
        self.series.get_by_sorted_pairs(hash, pairs, &syms)
    }

    /// Gets or creates a series from already-sorted `(&str, &str)` pairs.
    fn get_or_create_sorted_pairs(
        &self,
        hash: u64,
        pairs: &[(&str, &str)],
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

        let symbol_labels = {
            let mut syms = self.symbols.write();
            let mut interned = SmallVec::with_capacity(pairs.len());
            for &(k, v) in pairs {
                interned.push((syms.intern(k), syms.intern(v)));
            }
            SymbolLabels::from_sorted_ids(interned)
        };
        let labels = Labels::from_sorted_pairs(pairs.iter().copied());
        let ref_id = self.last_series_id.fetch_add(1, Ordering::AcqRel) + 1;
        let entry = Arc::new(SeriesEntry {
            ref_id,
            hash,
            symbol_labels: symbol_labels.clone(),
            labels,
            inner: Mutex::new(SeriesState::new()),
        });
        self.series.insert(Arc::clone(&entry));
        self.postings.add(ref_id, &symbol_labels);
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
        let symbol_labels = {
            let mut syms = self.symbols.write();
            SymbolLabels::from_labels(&mut syms, labels)
        };
        let entry = Arc::new(SeriesEntry {
            ref_id,
            hash,
            symbol_labels: symbol_labels.clone(),
            labels: labels.clone(),
            inner: Mutex::new(SeriesState::new()),
        });
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
        Ok((entry, true))
    }

    fn delete_series_entries(&self, entries: &[Arc<SeriesEntry>]) {
        if entries.is_empty() {
            return;
        }
        let mut deleted = HashSet::with_capacity(entries.len());
        let mut deleted_pairs = Vec::with_capacity(entries.len());
        let mut affected = Vec::new();
        for e in entries {
            self.series.remove(e);
            deleted.insert(e.ref_id);
            deleted_pairs.push((e.ref_id, e.labels.clone()));
            affected.extend_from_slice(e.symbol_labels.as_slice());
            let mut st = e.inner.lock();
            st.gced = true;
            if let Some(keep_until) = {
                let mt = st.max_time();
                (mt != i64::MIN).then_some(mt)
            } {
                self.update_wal_expiry(e.ref_id, keep_until);
            }
        }
        let removed_count = deleted.len();
        self.num_series.fetch_sub(removed_count as u64, Ordering::Relaxed);
        if let Some(ref g) = self.series_gauge {
            g.sub(removed_count as f64);
        }
        if let Some(ref c) = self.series_removed_total {
            c.inc_by(removed_count as f64);
        }
        self.postings.delete(&deleted, &affected);
        self.tombstones.delete_tombstones(&deleted);
        if let Some(ref cb) = *self.lifecycle_callback.read() {
            cb.post_deletion(&deleted_pairs);
        }
    }

    /// Creates a new [`HeadAppender`] for transactional batched writes.
    pub fn appender(&self) -> HeadAppender<'_> {
        let min_valid = self.appendable_min_valid_time();
        let head_maxt = self.max_time();
        HeadAppender {
            head: self,
            min_valid_time: min_valid,
            head_maxt,
            ooo_time_window: self.ooo_time_window_ms.load(Ordering::Acquire),
            series_refs: Vec::new(),
            floats: Vec::new(),
            histograms: Vec::new(),
            float_histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: Vec::new(),
            batch_last_sample: HashMap::new(),
            closed: false,
        }
    }

    /// Flushes all sealed head chunks (`head_chunks[..len-1]`) to `HeadChunksWriter` (`chunks_head/`).
    pub fn mmap_head_chunks(&self) -> Result<usize, HeadChunksError> {
        let mut writer_guard = self.chunk_writer.lock();
        let Some(ref mut writer) = *writer_guard else {
            return Ok(0);
        };
        let mut count = 0usize;
        let mut pending: Vec<(Arc<SeriesEntry>, Vec<MmappedChunk>)> = Vec::new();
        for stripe in &self.series.stripes {
            let entries: Vec<Arc<SeriesEntry>> = stripe.read().by_id.values().cloned().collect();
            for entry in entries {
                let mut st = entry.inner.lock();
                if st.head_chunks.len() > 1 {
                    let mmapped = st.write_sealed_head_chunks(entry.ref_id, writer)?;
                    if !mmapped.is_empty() {
                        count += mmapped.len();
                        drop(st);
                        pending.push((entry, mmapped));
                    }
                }
            }
        }
        writer.flush()?;

        // Refresh mmap reader BEFORE exposing new mmapped_chunks on series state,
        // so concurrent readers never observe a ChunkDiskMapperRef not yet in chunk_reader.
        if count > 0 {
            let reader = HeadChunksReader::open(self.dir.join("chunks_head"))?;
            *self.chunk_reader.write() = Some(reader);
            for (entry, mmapped) in pending {
                let mut st = entry.inner.lock();
                let n = mmapped.len().min(st.head_chunks.len().saturating_sub(1));
                st.head_chunks.drain(..n);
                st.mmapped_chunks.extend(mmapped.into_iter().take(n));
            }
        }
        Ok(count)
    }

    /// Collects all series with chunks overlapping `[mint, maxt_inclusive]` for block compaction.
    pub fn collect_compaction_series(
        &self,
        mint: i64,
        maxt_inclusive: i64,
    ) -> Result<Vec<CompactionSeries>, HeadError> {
        self.mmap_head_chunks()?;
        let chunk_reader_guard = self.chunk_reader.read();
        let chunk_reader = chunk_reader_guard.as_ref();

        let entries = self.series.all_entries();
        let mut out = Vec::with_capacity(entries.len());

        for entry in entries {
            let st = entry.inner.lock();
            let mut chks = Vec::new();
            for mc in &st.mmapped_chunks {
                if mc.overlaps_closed_interval(mint, maxt_inclusive) {
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
            }
            for hc in &st.head_chunks {
                if hc.overlaps_closed_interval(mint, maxt_inclusive) {
                    chks.push(ChunkMeta {
                        ref_id: 0,
                        min_time: hc.min_time,
                        max_time: hc.max_time,
                        chunk: Some(hc.chunk.clone()),
                    });
                }
            }
            if !chks.is_empty() {
                let tombs = self.tombstones.get(entry.ref_id);
                out.push(CompactionSeries {
                    labels: entry.labels.clone(),
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
        let mut writer_guard = self.chunk_writer.lock();
        let mut mmapped_any = false;
        let mut ooo_markers: Vec<RefMmapMarker> = Vec::new();
        let entries = self.series.all_entries();
        if let Some(ref mut writer) = *writer_guard {
            for entry in &entries {
                let mut st = entry.inner.lock();
                if let Some(ref mut ooo) = st.ooo {
                    if let Some(ohc) = ooo.ooo_head_chunk.take() {
                        let cut_chunks = ohc.chunk.to_encoded_chunks(i64::MIN, i64::MAX)?;
                        for cc in cut_chunks {
                            let r = writer.write_chunk(
                                entry.ref_id,
                                cc.min_time,
                                cc.max_time,
                                &cc.chunk,
                                true,
                            )?;
                            ooo.ooo_mmapped_chunks.push(MmappedChunk {
                                ref_id: r,
                                min_time: cc.min_time,
                                max_time: cc.max_time,
                                num_samples: cc.chunk.num_samples() as u16,
                                encoding: cc.chunk.encoding(),
                            });
                            ooo_markers.push(RefMmapMarker {
                                ref_id: entry.ref_id,
                                mmap_ref: r.as_u64(),
                            });
                            mmapped_any = true;
                        }
                    }
                }
            }
            if mmapped_any {
                writer.flush()?;
            }
        }
        drop(writer_guard);

        if !ooo_markers.is_empty() {
            if let Some(ref mut wbl) = *self.wbl.lock() {
                let enc = RecordEncoder::new(self.opts.enable_st_storage);
                let rec = enc.mmap_markers(&ooo_markers, Vec::new());
                wbl.log(&[&rec])?;
            }
        }

        if mmapped_any {
            let reader = HeadChunksReader::open(self.dir.join("chunks_head"))?;
            *self.chunk_reader.write() = Some(reader);
        }

        let chunk_reader_guard = self.chunk_reader.read();
        let chunk_reader = chunk_reader_guard.as_ref();

        let mut out = Vec::new();
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        for entry in entries {
            let st = entry.inner.lock();
            let Some(ref ooo) = st.ooo else {
                continue;
            };
            let mut chks = Vec::new();
            for mc in &ooo.ooo_mmapped_chunks {
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
            if let Some(ref ohc) = ooo.ooo_head_chunk {
                for cc in ohc.chunk.to_encoded_chunks(i64::MIN, i64::MAX)? {
                    mint = mint.min(cc.min_time);
                    maxt = maxt.max(cc.max_time);
                    chks.push(ChunkMeta {
                        ref_id: 0,
                        min_time: cc.min_time,
                        max_time: cc.max_time,
                        chunk: Some(cc.chunk),
                    });
                }
            }
            if !chks.is_empty() {
                let tombs = self.tombstones.get(entry.ref_id);
                out.push(CompactionSeries {
                    labels: entry.labels.clone(),
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
        let entries = self.series.all_entries();
        let mut to_delete = Vec::new();
        let mut min_mmap_seq = usize::MAX;
        for entry in entries {
            let mut st = entry.inner.lock();
            if st.pending_commits > 0 {
                continue;
            }
            st.ooo = None;
            if st.mmapped_chunks.is_empty() && st.head_chunks.is_empty() {
                to_delete.push(Arc::clone(&entry));
            } else {
                for mc in &st.mmapped_chunks {
                    let (seq, _) = mc.ref_id.unpack();
                    min_mmap_seq = min_mmap_seq.min(seq as usize);
                }
            }
        }
        self.min_ooo_time.store(i64::MAX, Ordering::Release);
        self.max_ooo_time.store(i64::MIN, Ordering::Release);
        self.wbl_pinned_refs.lock().clear();
        if !to_delete.is_empty() {
            self.delete_series_entries(&to_delete);
        }

        let (has_active, active_seq) = {
            let w = self.chunk_writer.lock();
            w.as_ref()
                .map_or((false, 0usize), |wr| (wr.has_active_file(), wr.current_seq()))
        };
        let cut_before = if has_active {
            min_mmap_seq.min(active_seq)
        } else {
            min_mmap_seq
        };
        if cut_before > 1 {
            let reader_opt = self.chunk_reader.write().take();
            if let Some(reader) = reader_opt {
                let truncated = reader.truncate(cut_before)?;
                *self.chunk_reader.write() = Some(truncated);
            }
        }
        Ok(())
    }

    /// Collects in-order chunks for the specified `refs` and returns `(series, min_time, max_time_inclusive)`.
    pub fn collect_selected_compaction_series(
        &self,
        refs: &[u64],
    ) -> Result<(Vec<CompactionSeries>, i64, i64), HeadError> {
        self.mmap_head_chunks()?;
        let chunk_reader_guard = self.chunk_reader.read();
        let chunk_reader = chunk_reader_guard.as_ref();

        let mut out = Vec::with_capacity(refs.len());
        let mut mint = i64::MAX;
        let mut maxt = i64::MIN;

        for &ref_id in refs {
            let Some(entry) = self.series.get_by_id(ref_id) else {
                continue;
            };
            let st = entry.inner.lock();
            // Skip series with active OOO state (caller should compact OOO head first).
            if st.ooo.as_ref().is_some_and(|o| {
                o.ooo_head_chunk.is_some() || !o.ooo_mmapped_chunks.is_empty()
            }) {
                continue;
            }
            let mut chks = Vec::new();
            for mc in &st.mmapped_chunks {
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
            for hc in &st.head_chunks {
                mint = mint.min(hc.min_time);
                maxt = maxt.max(hc.max_time);
                chks.push(ChunkMeta {
                    ref_id: 0,
                    min_time: hc.min_time,
                    max_time: hc.max_time,
                    chunk: Some(hc.chunk.clone()),
                });
            }
            if !chks.is_empty() {
                let tombs = self.tombstones.get(entry.ref_id);
                out.push(CompactionSeries {
                    labels: entry.labels.clone(),
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
        let min_valid_time = self.min_valid_time.load(Ordering::Acquire);
        let mut to_delete = Vec::new();
        for &ref_id in refs {
            if let Some(entry) = self.series.get_by_id(ref_id) {
                let mut st = entry.inner.lock();
                if st.pending_commits > 0 {
                    continue;
                }
                let has_ooo = st.ooo.as_ref().is_some_and(|o| {
                    o.ooo_head_chunk.is_some() || !o.ooo_mmapped_chunks.is_empty()
                });
                if !has_ooo {
                    if st.last_exemplar_ts.is_some_and(|ts| ts >= min_valid_time) {
                        // Keep the series identity while its exemplar can still be replayed,
                        // matching Prometheus `gcSeries`.
                        st.mmapped_chunks.clear();
                        st.head_chunks.clear();
                    } else {
                        to_delete.push(Arc::clone(&entry));
                    }
                }
            }
        }
        if !to_delete.is_empty() {
            let mut stones = Vec::with_capacity(to_delete.len());
            for e in &to_delete {
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
            self.delete_series_entries(&to_delete);
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

    pub fn truncate_memory(&self, mint: i64) -> Result<(), HeadError> {
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

        let entries = self.series.all_entries();
        let mut to_delete = Vec::new();
        let mut actual_mint = i64::MAX;
        let mut min_mmap_seq = usize::MAX;

        for entry in entries {
            let mut st = entry.inner.lock();
            if st.pending_commits > 0 {
                continue;
            }
            st.mmapped_chunks.retain(|mc| mc.max_time >= mint);
            st.head_chunks.retain(|hc| hc.max_time >= mint);

            let has_ooo = st.ooo.as_ref().is_some_and(|o| {
                o.ooo_head_chunk.is_some() || !o.ooo_mmapped_chunks.is_empty()
            });
            let has_recent_exemplar = st.last_exemplar_ts.is_some_and(|ts| ts >= mint);

            if st.mmapped_chunks.is_empty()
                && st.head_chunks.is_empty()
                && !has_ooo
                && !has_recent_exemplar
            {
                to_delete.push(Arc::clone(&entry));
                continue;
            }

            let sm = st.min_time();
            if sm < actual_mint {
                actual_mint = sm;
            }
            for mc in &st.mmapped_chunks {
                let (seq, _) = mc.ref_id.unpack();
                min_mmap_seq = min_mmap_seq.min(seq as usize);
            }
            if let Some(ref ooo) = st.ooo {
                for mc in &ooo.ooo_mmapped_chunks {
                    let (seq, _) = mc.ref_id.unpack();
                    min_mmap_seq = min_mmap_seq.min(seq as usize);
                }
            }
        }

        if !to_delete.is_empty() {
            // Log tombstones to WAL before unlinking.
            let mut stones = Vec::with_capacity(to_delete.len());
            for e in &to_delete {
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
            self.delete_series_entries(&to_delete);
        }

        if actual_mint != i64::MAX && actual_mint > self.min_time() {
            self.min_time.store(actual_mint, Ordering::Release);
            let clamped = actual_mint.min(self.appendable_min_valid_time());
            self.min_valid_time.store(clamped, Ordering::Release);
        }

        if min_mmap_seq != usize::MAX && min_mmap_seq > 1 {
            let reader_opt = self.chunk_reader.write().take();
            if let Some(reader) = reader_opt {
                let truncated = reader.truncate(min_mmap_seq)?;
                *self.chunk_reader.write() = Some(truncated);
            }
        }
        Ok(())
    }

    pub fn truncate_wal(&self, mint: i64) -> Result<(), HeadError> {
        if mint <= self.last_wal_truncation_time.load(Ordering::Acquire) {
            return Ok(());
        }
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
            wal.next_segment()?;
            (first, last)
        };

        if last == 0 {
            return Ok(());
        }
        last = last.saturating_sub(1);
        last = first + (last.saturating_sub(first)) * 2 / 3;
        if last <= first {
            return Ok(());
        }

        let pinned = self.wbl_pinned_refs.lock().clone();
        let expiries = self.wal_expiries.lock().clone();
        create_checkpoint(
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
        )?;

        if let Some(ref mut wal) = *self.wal.lock() {
            wal.truncate(last + 1)?;
        }
        {
            let mut exp = self.wal_expiries.lock();
            exp.retain(|_, &mut ku| ku >= mint);
        }
        let _ = delete_checkpoints(&wal_dir, last);
        let _ = checkpoint_dir(&wal_dir, last);
        let _ = read_min_valid_time(&wal_dir);
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
                chunks += (st.mmapped_chunks.len() + st.head_chunks.len()) as u64;
                for mc in &st.mmapped_chunks {
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

    /// Evaluates label matchers against `MemPostings`.
    pub fn postings_for_matchers(&self, matchers: &[LabelMatcher]) -> Result<Vec<u64>, HeadError> {
        let syms = self.symbols.read();
        let view = self.postings.as_source(&syms);
        Ok(postings_for_matchers(&view, matchers)?)
    }

    /// Returns all series refs in `MemPostings`.
    pub fn all_postings(&self) -> Vec<u64> {
        self.postings.all_postings()
    }

    pub fn label_names(&self, matchers: &[LabelMatcher]) -> Result<Vec<String>, HeadError> {
        let syms = self.symbols.read();
        if matchers.is_empty() {
            return Ok(self.postings.label_names(&syms));
        }
        let refs = postings_for_matchers(&self.postings.as_source(&syms), matchers)?;
        let mut names = HashSet::new();
        for r in refs {
            if let Some(entry) = self.series.get_by_id(r) {
                for &(nid, _) in entry.symbol_labels.as_slice() {
                    names.insert(syms.resolve(nid).to_string());
                }
            }
        }
        let mut out: Vec<String> = names.into_iter().collect();
        out.sort_unstable();
        Ok(out)
    }

    pub fn label_values(
        &self,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, HeadError> {
        let syms = self.symbols.read();
        if matchers.is_empty() {
            return Ok(self.postings.sorted_label_values(name, &syms));
        }
        let refs = postings_for_matchers(&self.postings.as_source(&syms), matchers)?;
        let Some(name_id) = syms.get(name) else {
            return Ok(Vec::new());
        };
        let mut vals = HashSet::new();
        for r in refs {
            if let Some(entry) = self.series.get_by_id(r) {
                for &(nid, vid) in entry.symbol_labels.as_slice() {
                    if nid == name_id {
                        vals.insert(syms.resolve(vid).to_string());
                        break;
                    }
                }
            }
        }
        let mut out: Vec<String> = vals.into_iter().collect();
        out.sort_unstable();
        Ok(out)
    }

    /// Reads series labels and encoded chunks (in-order + OOO merged if overlapping) in `[mint, maxt]`.
    pub fn series_chunks(
        &self,
        ref_id: u64,
        mint: i64,
        maxt: i64,
    ) -> Result<SeriesChunksResult, HeadError> {
        let Some(entry) = self.series.get_by_id(ref_id) else {
            return Ok(None);
        };

        let (mmapped_in_order, head_chks, mmapped_ooo, ooo_head_chks) = {
            let st = entry.inner.lock();
            let mmapped_in_order: Vec<MmappedChunk> = st
                .mmapped_chunks
                .iter()
                .filter(|mc| mc.overlaps_closed_interval(mint, maxt))
                .cloned()
                .collect();
            let head_chks: Vec<ChunkMeta> = st
                .head_chunks
                .iter()
                .filter(|hc| hc.overlaps_closed_interval(mint, maxt))
                .map(|hc| ChunkMeta {
                    ref_id: 0,
                    min_time: hc.min_time,
                    max_time: hc.max_time,
                    chunk: Some(hc.chunk.clone()),
                })
                .collect();
            let mut mmapped_ooo = Vec::new();
            let mut ooo_head_chks = Vec::new();
            if let Some(ref ooo) = st.ooo {
                for mc in &ooo.ooo_mmapped_chunks {
                    if mc.overlaps_closed_interval(mint, maxt) {
                        mmapped_ooo.push(*mc);
                    }
                }
                if let Some(ref ohc) = ooo.ooo_head_chunk {
                    if ohc.overlaps_closed_interval(mint, maxt) {
                        for cc in ohc.chunk.to_encoded_chunks(mint, maxt)? {
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
    meta: SeriesMetadata,
}

/// Transactional write appender for [`Head`] matching `tsdb.headAppender`.
pub struct HeadAppender<'a> {
    head: &'a Head,
    min_valid_time: i64,
    head_maxt: i64,
    ooo_time_window: i64,
    series_refs: Vec<RefSeries>,
    floats: Vec<PendingFloat>,
    histograms: Vec<PendingHist>,
    float_histograms: Vec<PendingFloatHist>,
    exemplars: Vec<PendingExemplar>,
    metadata: Vec<PendingMetadata>,
    batch_last_sample: HashMap<u64, (i64, LastSample)>,
    closed: bool,
}

impl<'a> HeadAppender<'a> {
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
    fn resolve_sorted_pairs(
        &mut self,
        series_ref: Option<u64>,
        pairs: &[(&str, &str)],
    ) -> Result<Arc<SeriesEntry>, HeadError> {
        if let Some(r) = series_ref {
            if let Some(e) = self.head.series.get_by_id(r) {
                return Ok(e);
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
            return Err(HeadError::MissingMetricName);
        }

        let hash = hash_sorted_pairs(sorted_slice);
        let (entry, created) = self.head.get_or_create_sorted_pairs(hash, sorted_slice)?;
        if created {
            self.series_refs.push(RefSeries {
                ref_id: entry.ref_id,
                labels: entry.labels.clone(),
            });
        }
        Ok(entry)
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

    fn append_float_inner(
        &mut self,
        series_ref: Option<u64>,
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

        let entry = self.resolve_sorted_pairs(series_ref, pairs)?;
        let ref_id = entry.ref_id;

        if let Some((is_ooo, is_dup)) =
            self.check_batch_last_sample(ref_id, t, &LastSample::Float(v), effective_ooo)?
        {
            if is_ooo && v.to_bits() != QUIET_ZERO_NAN_BITS {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            if is_dup {
                return Ok(ref_id);
            }
            let mut state = entry.inner.lock();
            state.pending_commits += 1;
            if !is_ooo {
                self.batch_last_sample
                    .insert(ref_id, (t, LastSample::Float(v)));
            }
        } else {
            let cr_guard = self.head.chunk_reader.read();
            let state = entry.inner.lock();
            let res = state.appendable_float(
                t,
                v,
                self.head_maxt,
                self.min_valid_time,
                effective_ooo,
                cr_guard.as_ref(),
            );
            drop(state);
            if v.to_bits() != QUIET_ZERO_NAN_BITS && !matches!(res, Ok((false, _))) {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            let (is_ooo, is_dup) = res?;
            if is_dup {
                return Ok(ref_id);
            }
            entry.inner.lock().pending_commits += 1;
            if !is_ooo {
                self.batch_last_sample
                    .insert(ref_id, (t, LastSample::Float(v)));
            }
        }

        self.floats.push(PendingFloat { entry, st, t, v });
        Ok(ref_id)
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
        {
            let cr_guard = self.head.chunk_reader.read();
            let mut state = entry.inner.lock();
            state.appendable_float(
                t,
                v,
                self.head_maxt,
                self.min_valid_time,
                self.ooo_time_window,
                cr_guard.as_ref(),
            )?;
            state.pending_commits += 1;
        }
        self.floats.push(PendingFloat {
            entry,
            st: 0,
            t,
            v,
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

        let entry = self.resolve_sorted_pairs(series_ref, pairs)?;
        let ref_id = entry.ref_id;
        let ls = LastSample::Histogram(h.clone());
        if let Some((is_ooo, is_dup)) =
            self.check_batch_last_sample(ref_id, t, &ls, effective_ooo)?
        {
            if is_ooo {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            if is_dup {
                return Ok(ref_id);
            }
            let mut state = entry.inner.lock();
            state.pending_commits += 1;
            if !is_ooo {
                self.batch_last_sample.insert(ref_id, (t, ls));
            }
        } else {
            let cr_guard = self.head.chunk_reader.read();
            let state = entry.inner.lock();
            let res = state.appendable_histogram(
                t,
                &h,
                self.head_maxt,
                self.min_valid_time,
                effective_ooo,
                cr_guard.as_ref(),
            );
            drop(state);
            if !matches!(res, Ok((false, _))) {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            let (is_ooo, is_dup) = res?;
            if is_dup {
                return Ok(ref_id);
            }
            entry.inner.lock().pending_commits += 1;
            if !is_ooo {
                self.batch_last_sample.insert(ref_id, (t, ls));
            }
        }
        self.histograms.push(PendingHist { entry, st, t, h });
        Ok(ref_id)
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

        let entry = self.resolve_sorted_pairs(series_ref, pairs)?;
        let ref_id = entry.ref_id;
        let ls = LastSample::FloatHistogram(fh.clone());
        if let Some((is_ooo, is_dup)) =
            self.check_batch_last_sample(ref_id, t, &ls, effective_ooo)?
        {
            if is_ooo {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            if is_dup {
                return Ok(ref_id);
            }
            let mut state = entry.inner.lock();
            state.pending_commits += 1;
            if !is_ooo {
                self.batch_last_sample.insert(ref_id, (t, ls));
            }
        } else {
            let cr_guard = self.head.chunk_reader.read();
            let state = entry.inner.lock();
            let res = state.appendable_float_histogram(
                t,
                &fh,
                self.head_maxt,
                self.min_valid_time,
                effective_ooo,
                cr_guard.as_ref(),
            );
            drop(state);
            if !matches!(res, Ok((false, _))) {
                let delta = self.head_maxt.saturating_sub(t);
                if delta > 0 {
                    self.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
                }
            }
            let (is_ooo, is_dup) = res?;
            if is_dup {
                return Ok(ref_id);
            }
            entry.inner.lock().pending_commits += 1;
            if !is_ooo {
                self.batch_last_sample.insert(ref_id, (t, ls));
            }
        }
        self.float_histograms
            .push(PendingFloatHist { entry, st, t, fh });
        Ok(ref_id)
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
            st.last_exemplar_ts = Some(
                st.last_exemplar_ts
                    .map_or(cleaned.timestamp_ms, |prev| prev.max(cleaned.timestamp_ms)),
            );
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
        let entry = self.resolve_sorted_pairs(series_ref, pairs)?;
        let ref_id = entry.ref_id;

        if !metric_family_name.is_empty() {
            self.head.metadata.add(
                metric_family_name,
                MetricMetadata {
                    metric_type,
                    metric_family_name: metric_family_name.to_string(),
                    help: help.to_string(),
                    unit: unit.to_string(),
                },
                self.head.opts.metadata_limits,
            )?;
        }

        let new_meta = SeriesMetadata {
            metric_type,
            unit: unit.to_string(),
            help: help.to_string(),
        };
        let changed = {
            let st = entry.inner.lock();
            st.metadata.as_ref() != Some(&new_meta)
        };
        if changed {
            self.metadata.push(PendingMetadata {
                entry,
                meta: new_meta,
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
            let mut st = pm.entry.inner.lock();
            st.metadata = Some(pm.meta);
        }

        let mut in_order_mint = i64::MAX;
        let mut in_order_maxt = i64::MIN;
        let mut ooo_mint = i64::MAX;
        let mut ooo_maxt = i64::MIN;

        let mut wbl_samples: Vec<RefSample> = Vec::new();
        let mut wbl_histograms: Vec<RefHistogramSample> = Vec::new();
        let mut wbl_float_histograms: Vec<RefFloatHistogramSample> = Vec::new();
        let mut wbl_markers: Vec<RefMmapMarker> = Vec::new();
        let mut wrote_ooo_mmap = false;

        // Commit floats
        for pf in self.floats.drain(..) {
            let mut v = pf.v;
            let mut state = pf.entry.inner.lock();
            state.pending_commits = state.pending_commits.saturating_sub(1);

            if is_stale_nan(v) {
                match &state.last_sample {
                    LastSample::Histogram(_) => {
                        let h = Histogram {
                            sum: v,
                            ..Histogram::default()
                        };
                        let cr_guard = self.head.chunk_reader.read();
                        if let Ok((is_ooo, is_dup)) = state.appendable_histogram(
                            pf.t,
                            &h,
                            self.head_maxt,
                            self.min_valid_time,
                            self.ooo_time_window,
                            cr_guard.as_ref(),
                        ) {
                            if !is_dup && !is_ooo
                                && state
                                    .append_histogram_in_order(
                                        pf.st,
                                        pf.t,
                                        &h,
                                        self.head.opts.chunk_range_ms,
                                        self.head.opts.enable_histogram_st_encoding,
                                    )?
                                    .0
                            {
                                in_order_mint = in_order_mint.min(pf.t);
                                in_order_maxt = in_order_maxt.max(pf.t);
                            }
                        }
                        continue;
                    }
                    LastSample::FloatHistogram(_) => {
                        let fh = FloatHistogram {
                            sum: v,
                            ..FloatHistogram::default()
                        };
                        let cr_guard = self.head.chunk_reader.read();
                        if let Ok((is_ooo, is_dup)) = state.appendable_float_histogram(
                            pf.t,
                            &fh,
                            self.head_maxt,
                            self.min_valid_time,
                            self.ooo_time_window,
                            cr_guard.as_ref(),
                        ) {
                            if !is_dup && !is_ooo
                                && state
                                    .append_float_histogram_in_order(
                                        pf.st,
                                        pf.t,
                                        &fh,
                                        self.head.opts.chunk_range_ms,
                                        self.head.opts.enable_histogram_st_encoding,
                                    )?
                                    .0
                            {
                                in_order_mint = in_order_mint.min(pf.t);
                                in_order_maxt = in_order_maxt.max(pf.t);
                            }
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            let check = {
                let cr_guard = self.head.chunk_reader.read();
                state.appendable_float(
                    pf.t,
                    v,
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
                    pf.entry.ref_id,
                    pf.st,
                    pf.t,
                    OooSampleValue::Float(v),
                    self.head.opts.ooo_cap_max,
                    cw_guard.as_mut(),
                )?;
                if chunk_created {
                    if mmap_refs.is_empty() {
                        wbl_markers.push(RefMmapMarker {
                            ref_id: pf.entry.ref_id,
                            mmap_ref: 0,
                        });
                    } else {
                        wrote_ooo_mmap = true;
                        for r in mmap_refs {
                            wbl_markers.push(RefMmapMarker {
                                ref_id: pf.entry.ref_id,
                                mmap_ref: r.as_u64(),
                            });
                        }
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
                if state
                    .append_float_in_order(
                        pf.st,
                        pf.t,
                        v,
                        self.head.opts.chunk_range_ms,
                        self.head.opts.samples_per_chunk,
                        self.head.float_chunk_encoding(),
                    )?
                    .0
                {
                    in_order_mint = in_order_mint.min(pf.t);
                    in_order_maxt = in_order_maxt.max(pf.t);
                }
            }
        }

        // Commit integer histograms
        for ph in self.histograms.drain(..) {
            let mut state = ph.entry.inner.lock();
            state.pending_commits = state.pending_commits.saturating_sub(1);
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
                    self.head.opts.ooo_cap_max,
                    cw_guard.as_mut(),
                )?;
                if chunk_created {
                    if mmap_refs.is_empty() {
                        wbl_markers.push(RefMmapMarker {
                            ref_id: ph.entry.ref_id,
                            mmap_ref: 0,
                        });
                    } else {
                        wrote_ooo_mmap = true;
                        for r in mmap_refs {
                            wbl_markers.push(RefMmapMarker {
                                ref_id: ph.entry.ref_id,
                                mmap_ref: r.as_u64(),
                            });
                        }
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
                        h: ph.h,
                    });
                    ooo_mint = ooo_mint.min(ph.t);
                    ooo_maxt = ooo_maxt.max(ph.t);
                }
            } else if state
                .append_histogram_in_order(
                    ph.st,
                    ph.t,
                    &ph.h,
                    self.head.opts.chunk_range_ms,
                    self.head.opts.enable_histogram_st_encoding,
                )?
                .0
            {
                in_order_mint = in_order_mint.min(ph.t);
                in_order_maxt = in_order_maxt.max(ph.t);
            }
        }

        // Commit float histograms
        for pfh in self.float_histograms.drain(..) {
            let mut state = pfh.entry.inner.lock();
            state.pending_commits = state.pending_commits.saturating_sub(1);
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
                    self.head.opts.ooo_cap_max,
                    cw_guard.as_mut(),
                )?;
                if chunk_created {
                    if mmap_refs.is_empty() {
                        wbl_markers.push(RefMmapMarker {
                            ref_id: pfh.entry.ref_id,
                            mmap_ref: 0,
                        });
                    } else {
                        wrote_ooo_mmap = true;
                        for r in mmap_refs {
                            wbl_markers.push(RefMmapMarker {
                                ref_id: pfh.entry.ref_id,
                                mmap_ref: r.as_u64(),
                            });
                        }
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
                        fh: pfh.fh,
                    });
                    ooo_mint = ooo_mint.min(pfh.t);
                    ooo_maxt = ooo_maxt.max(pfh.t);
                }
            } else if state
                .append_float_histogram_in_order(
                    pfh.st,
                    pfh.t,
                    &pfh.fh,
                    self.head.opts.chunk_range_ms,
                    self.head.opts.enable_histogram_st_encoding,
                )?
                .0
            {
                in_order_mint = in_order_mint.min(pfh.t);
                in_order_maxt = in_order_maxt.max(pfh.t);
            }
        }

        if wrote_ooo_mmap {
            if let Some(ref mut cw) = *self.head.chunk_writer.lock() {
                cw.flush()?;
            }
            let reader = HeadChunksReader::open(self.head.dir.join("chunks_head"))?;
            *self.head.chunk_reader.write() = Some(reader);
        }

        if in_order_mint <= in_order_maxt {
            self.head.update_min_max_time(in_order_mint, in_order_maxt);
        }
        if ooo_mint <= ooo_maxt {
            self.head.update_min_ooo_max_ooo_time(ooo_mint, ooo_maxt);
        }

        // 3. Log OOO records to WBL if any.
        if !wbl_markers.is_empty()
            || !wbl_samples.is_empty()
            || !wbl_histograms.is_empty()
            || !wbl_float_histograms.is_empty()
        {
            if let Some(ref mut wbl) = *self.head.wbl.lock() {
                let enc = RecordEncoder::new(self.head.opts.enable_st_storage);
                let mut recs: Vec<Vec<u8>> = Vec::new();
                if !wbl_markers.is_empty() {
                    recs.push(enc.mmap_markers(&wbl_markers, Vec::new()));
                }
                if !wbl_samples.is_empty() {
                    recs.push(enc.samples(&wbl_samples, Vec::new()));
                }
                if !wbl_histograms.is_empty() {
                    let (r, cb) = enc.histogram_samples(&wbl_histograms, Vec::new());
                    if !r.is_empty() {
                        recs.push(r);
                    }
                    if !cb.is_empty() {
                        recs.push(enc.custom_buckets_histogram_samples(&cb, Vec::new()));
                    }
                }
                if !wbl_float_histograms.is_empty() {
                    let (r, cb) = enc.float_histogram_samples(&wbl_float_histograms, Vec::new());
                    if !r.is_empty() {
                        recs.push(r);
                    }
                    if !cb.is_empty() {
                        recs.push(enc.custom_buckets_float_histogram_samples(&cb, Vec::new()));
                    }
                }
                let slices: Vec<&[u8]> = recs.iter().map(|v| v.as_slice()).collect();
                if !slices.is_empty() {
                    wbl.log(&slices)?;
                }
            }
        }

        if wbl_markers.iter().any(|m| m.mmap_ref != 0) {
            if let Some(ref mut cw) = *self.head.chunk_writer.lock() {
                cw.flush()?;
            }
            let reader = HeadChunksReader::open(self.head.dir.join("chunks_head"))?;
            *self.head.chunk_reader.write() = Some(reader);
        }

        Ok(())
    }

    /// Rolls back buffered samples/exemplars while still logging newly created series declarations to WAL.
    pub fn rollback(mut self) -> Result<(), HeadError> {
        if self.closed {
            return Err(HeadError::AppenderClosed);
        }
        self.closed = true;
        for pf in self.floats.drain(..) {
            let mut st = pf.entry.inner.lock();
            st.pending_commits = st.pending_commits.saturating_sub(1);
        }
        for ph in self.histograms.drain(..) {
            let mut st = ph.entry.inner.lock();
            st.pending_commits = st.pending_commits.saturating_sub(1);
        }
        for pfh in self.float_histograms.drain(..) {
            let mut st = pfh.entry.inner.lock();
            st.pending_commits = st.pending_commits.saturating_sub(1);
        }
        self.exemplars.clear();
        self.metadata.clear();

        if !self.series_refs.is_empty() {
            if let Some(ref mut wal) = *self.head.wal.lock() {
                let enc = RecordEncoder::new(self.head.opts.enable_st_storage);
                let rec = enc.series(&self.series_refs, Vec::new());
                wal.log(&[&rec])?;
            }
        }
        Ok(())
    }

    fn log_wal(&mut self) -> Result<(), HeadError> {
        let mut wal_guard = self.head.wal.lock();
        let Some(ref mut wal) = *wal_guard else {
            return Ok(());
        };
        let enc = RecordEncoder::new(self.head.opts.enable_st_storage);
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
                    metric_type: m.meta.metric_type as u8,
                    unit: m.meta.unit.clone(),
                    help: m.meta.help.clone(),
                })
                .collect();
            recs.push(enc.metadata(&metas, Vec::new()));
        }
        if !self.floats.is_empty() {
            let samples: Vec<RefSample> = self
                .floats
                .iter()
                .map(|f| RefSample {
                    ref_id: f.entry.ref_id,
                    st: f.st,
                    t: f.t,
                    v: f.v,
                })
                .collect();
            recs.push(enc.samples(&samples, Vec::new()));
        }
        if !self.histograms.is_empty() {
            let hists: Vec<RefHistogramSample> = self
                .histograms
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
        if !self.float_histograms.is_empty() {
            let fhists: Vec<RefFloatHistogramSample> = self
                .float_histograms
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
            for pf in self.floats.drain(..) {
                let mut st = pf.entry.inner.lock();
                st.pending_commits = st.pending_commits.saturating_sub(1);
            }
            for ph in self.histograms.drain(..) {
                let mut st = ph.entry.inner.lock();
                st.pending_commits = st.pending_commits.saturating_sub(1);
            }
            for pfh in self.float_histograms.drain(..) {
                let mut st = pfh.entry.inner.lock();
                st.pending_commits = st.pending_commits.saturating_sub(1);
            }
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
                // Different value at last timestamp errors with DuplicateSampleForTimestamp
                assert!(matches!(
                    app.append_float(r, &pairs, 1_000_000 + 199 * 1000, 999.0),
                    Err(HeadError::DuplicateSampleForTimestamp)
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
}
