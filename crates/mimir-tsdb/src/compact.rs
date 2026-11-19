//! Prometheus TSDB ULID block compaction from Head/RangeHead and on-disk Blocks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/compact.go
//! and github.com/prometheus/prometheus/tsdb/async_block_writer.go.

use std::fs;
use std::io;
use std::path::Path;

use thiserror::Error;
use ulid::Ulid;

use crate::block::{
    list_block_dirs, read_meta_file, sync_dir, write_meta_file, Block, BlockDesc, BlockError,
    BlockMeta, BlockMetaCompaction, BlockStats, META_VERSION_1,
};
use crate::chunkenc::{
    ChunkError, EncodedChunk, Encoding, FloatHistogram, FloatHistogramChunk,
    FloatHistogramStChunk, Histogram, HistogramChunk, HistogramStChunk, Xor2Chunk, XorChunk,
    MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND, MIN_SAMPLES_PER_HISTOGRAM_CHUNK,
    TARGET_BYTES_PER_HISTOGRAM_CHUNK,
};
use crate::chunkenc::float_histogram::{
    AppendFloatHistogramOutcome, FloatHistogramAppenderState,
};
use crate::chunkenc::float_histogram_st::AppendFloatHistogramStOutcome;
use crate::chunkenc::histogram::{AppendHistogramOutcome, HistogramAppenderState};
use crate::chunkenc::histogram_st::AppendHistogramStOutcome;
use crate::chunkenc::st::StEncoder;
use crate::chunkenc::xor::XorAppendState;
use crate::chunkenc::xor2::Xor2AppendState;
use crate::chunks_block::{BlockChunksError, BlockChunksWriter, ChunkMeta, DEFAULT_CHUNK_SEGMENT_SIZE};
use crate::head::DEFAULT_SAMPLES_PER_CHUNK;
use crate::index::{IndexError, IndexWriter, INDEX_FILENAME};
use crate::labels::Labels;
use crate::tombstones::{write_tombstones_file, Interval, MemTombstones, TombstoneError};

const TMP_BLOCK_DIR_SUFFIX: &str = ".tmp-for-creation";

#[derive(Debug, Error)]
pub enum CompactError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("block error: {0}")]
    Block(#[from] BlockError),
    #[error("chunks error: {0}")]
    Chunks(#[from] BlockChunksError),
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("tombstone error: {0}")]
    Tombstone(#[from] TombstoneError),
    #[error("chunk error: {0}")]
    Chunk(#[from] ChunkError),
}

/// Decoded sample value used during block compaction and sample-level querying.
#[derive(Clone, Debug, PartialEq)]
pub enum SampleValue {
    Float(f64),
    Histogram(Box<Histogram>),
    FloatHistogram(Box<FloatHistogram>),
}

const _: () = assert!(std::mem::size_of::<SampleValue>() <= 16);

/// A single decoded timestamped sample.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedSample {
    pub st: i64,
    pub t: i64,
    pub value: SampleValue,
}

/// Extracts all samples from an [`EncodedChunk`].
pub fn decode_chunk_samples(chunk: &EncodedChunk) -> Result<Vec<DecodedSample>, ChunkError> {
    match chunk {
        EncodedChunk::Xor(xc) => {
            let mut out = Vec::with_capacity(xc.num_samples());
            let mut it = xc.iter();
            while let Some((t, v)) = it.next_sample()? {
                out.push(DecodedSample {
                    st: 0,
                    t,
                    value: SampleValue::Float(v),
                });
            }
            Ok(out)
        }
        EncodedChunk::Xor2(xc) => {
            let mut out = Vec::with_capacity(xc.num_samples());
            let mut it = xc.iter();
            while let Some((t, v)) = it.next_sample()? {
                out.push(DecodedSample {
                    st: it.at_st(),
                    t,
                    value: SampleValue::Float(v),
                });
            }
            Ok(out)
        }
        EncodedChunk::Histogram(hc) => {
            let mut out = Vec::with_capacity(hc.num_samples());
            let mut it = hc.iter();
            while let Some((t, h)) = it.next_histogram()? {
                out.push(DecodedSample {
                    st: 0,
                    t,
                    value: SampleValue::Histogram(Box::new(h)),
                });
            }
            Ok(out)
        }
        EncodedChunk::HistogramSt(hc) => {
            let mut out = Vec::with_capacity(hc.num_samples());
            let mut it = hc.iter();
            while let Some((t, h)) = it.next_histogram()? {
                out.push(DecodedSample {
                    st: it.at_st(),
                    t,
                    value: SampleValue::Histogram(Box::new(h)),
                });
            }
            Ok(out)
        }
        EncodedChunk::FloatHistogram(fhc) => {
            let mut out = Vec::with_capacity(fhc.num_samples());
            let mut it = fhc.iter();
            while let Some((t, fh)) = it.next_float_histogram()? {
                out.push(DecodedSample {
                    st: 0,
                    t,
                    value: SampleValue::FloatHistogram(Box::new(fh)),
                });
            }
            Ok(out)
        }
        EncodedChunk::FloatHistogramSt(fhc) => {
            let mut out = Vec::with_capacity(fhc.num_samples());
            let mut it = fhc.iter();
            while let Some((t, fh)) = it.next_float_histogram()? {
                out.push(DecodedSample {
                    st: it.at_st(),
                    t,
                    value: SampleValue::FloatHistogram(Box::new(fh)),
                });
            }
            Ok(out)
        }
    }
}

/// Encodes a sorted slice of [`DecodedSample`]s into one or more compact [`ChunkMeta`]s,
/// cutting chunks on type changes, counter resets, or size/sample limits.
pub fn encode_samples_into_chunks(
    samples: &[DecodedSample],
    samples_per_chunk: usize,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    encode_samples_into_chunks_with_encodings(
        samples,
        samples_per_chunk,
        Encoding::Xor,
        Encoding::Histogram,
        Encoding::FloatHistogram,
    )
}

/// Encodes a sorted slice of [`DecodedSample`]s into [`ChunkMeta`]s using the specified target
/// encodings (`Xor`/`Xor2`, `Histogram`/`HistogramSt`, `FloatHistogram`/`FloatHistogramSt`).
pub fn encode_samples_into_chunks_with_encodings(
    samples: &[DecodedSample],
    samples_per_chunk: usize,
    float_enc: Encoding,
    hist_enc: Encoding,
    fhist_enc: Encoding,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let samples_per_chunk = samples_per_chunk.max(1);
    let use_xor2 = float_enc == Encoding::Xor2;
    let use_hist_st = hist_enc == Encoding::HistogramSt;
    let use_fhist_st = fhist_enc == Encoding::FloatHistogramSt;
    let mut out = Vec::new();

    enum Active {
        None,
        Xor {
            mint: i64,
            maxt: i64,
            chunk: XorChunk,
            state: XorAppendState,
            prev_t: i64,
            prev_v: f64,
        },
        Xor2 {
            mint: i64,
            maxt: i64,
            chunk: Xor2Chunk,
            state: Xor2AppendState,
        },
        Hist {
            mint: i64,
            maxt: i64,
            chunk: HistogramChunk,
            state: Option<HistogramAppenderState>,
            prev_state: Option<HistogramAppenderState>,
        },
        HistSt {
            mint: i64,
            maxt: i64,
            chunk: HistogramStChunk,
            state: Option<(HistogramAppenderState, StEncoder)>,
            prev_state: Option<HistogramAppenderState>,
        },
        FloatHist {
            mint: i64,
            maxt: i64,
            chunk: FloatHistogramChunk,
            state: Option<FloatHistogramAppenderState>,
            prev_state: Option<FloatHistogramAppenderState>,
        },
        FloatHistSt {
            mint: i64,
            maxt: i64,
            chunk: FloatHistogramStChunk,
            state: Option<(FloatHistogramAppenderState, StEncoder)>,
            prev_state: Option<FloatHistogramAppenderState>,
        },
    }

    let mut active = Active::None;

    let flush_active = |active: &mut Active, out: &mut Vec<ChunkMeta>| {
        match std::mem::replace(active, Active::None) {
            Active::None => {}
            Active::Xor {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Xor(chunk)),
                });
            }
            Active::Xor2 {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Xor2(chunk)),
                });
            }
            Active::Hist {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Histogram(chunk)),
                });
            }
            Active::HistSt {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::HistogramSt(chunk)),
                });
            }
            Active::FloatHist {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::FloatHistogram(chunk)),
                });
            }
            Active::FloatHistSt {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::FloatHistogramSt(chunk)),
                });
            }
        }
    };

    for s in samples {
        match &s.value {
            SampleValue::Float(v) => {
                if use_xor2 {
                    let need_cut = match &active {
                        Active::Xor2 { chunk, .. } => {
                            chunk.num_samples() >= samples_per_chunk
                                || chunk.as_bytes().len() > MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND
                        }
                        _ => true,
                    };
                    if need_cut {
                        flush_active(&mut active, &mut out);
                        active = Active::Xor2 {
                            mint: s.t,
                            maxt: s.t,
                            chunk: Xor2Chunk::new(),
                            state: Xor2AppendState::new_empty(),
                        };
                    }
                    if let Active::Xor2 { maxt, chunk, state, .. } = &mut active {
                        chunk.append_with_state(state, s.st, s.t, *v)?;
                        *maxt = s.t;
                    }
                } else {
                    let need_cut = match &active {
                        Active::Xor { chunk, .. } => {
                            chunk.num_samples() >= samples_per_chunk
                                || chunk.as_bytes().len() > MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND
                        }
                        _ => true,
                    };
                    if need_cut {
                        flush_active(&mut active, &mut out);
                        active = Active::Xor {
                            mint: s.t,
                            maxt: s.t,
                            chunk: XorChunk::new(),
                            state: XorAppendState::new_empty(),
                            prev_t: i64::MIN,
                            prev_v: 0.0,
                        };
                    }
                    if let Active::Xor {
                        maxt,
                        chunk,
                        state,
                        prev_t,
                        prev_v,
                        ..
                    } = &mut active
                    {
                        chunk.append_with_state(state, *prev_t, *prev_v, s.t, *v)?;
                        *prev_t = s.t;
                        *prev_v = *v;
                        *maxt = s.t;
                    }
                }
            }
            SampleValue::Histogram(h) => {
                if use_hist_st {
                    let need_cut = match &active {
                        Active::HistSt { chunk, .. } => {
                            chunk.as_bytes().len() >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2
                        }
                        _ => true,
                    };
                    if need_cut {
                        let prev_from_cut = if let Active::HistSt { ref mut state, .. } = active {
                            state.take().map(|(hs, _)| hs)
                        } else {
                            None
                        };
                        flush_active(&mut active, &mut out);
                        active = Active::HistSt {
                            mint: s.t,
                            maxt: s.t,
                            chunk: HistogramStChunk::new(),
                            state: None,
                            prev_state: prev_from_cut,
                        };
                    }
                    if let Active::HistSt {
                        mint,
                        maxt,
                        chunk,
                        state,
                        prev_state,
                    } = &mut active
                    {
                        let outcome = chunk.append_with_state_prev(
                            state,
                            prev_state.as_ref(),
                            s.st,
                            s.t,
                            h,
                        )?;
                        match outcome {
                            AppendHistogramStOutcome::Appended { .. } => {
                                *maxt = s.t;
                            }
                            AppendHistogramStOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::HistogramSt(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                                *prev_state = None;
                            }
                        }
                    }
                } else {
                    let need_cut = match &active {
                        Active::Hist { chunk, .. } => {
                            chunk.as_bytes().len() >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2
                        }
                        _ => true,
                    };
                    if need_cut {
                        let prev_from_cut = if let Active::Hist { ref mut state, .. } = active {
                            state.take()
                        } else {
                            None
                        };
                        flush_active(&mut active, &mut out);
                        active = Active::Hist {
                            mint: s.t,
                            maxt: s.t,
                            chunk: HistogramChunk::new(),
                            state: None,
                            prev_state: prev_from_cut,
                        };
                    }
                    if let Active::Hist {
                        mint,
                        maxt,
                        chunk,
                        state,
                        prev_state,
                    } = &mut active
                    {
                        let outcome = chunk.append_with_state_prev(
                            state,
                            prev_state.as_ref(),
                            s.st,
                            s.t,
                            h,
                        )?;
                        match outcome {
                            AppendHistogramOutcome::Appended { .. } => {
                                *maxt = s.t;
                            }
                            AppendHistogramOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::Histogram(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                                *prev_state = None;
                            }
                        }
                    }
                }
            }
            SampleValue::FloatHistogram(fh) => {
                if use_fhist_st {
                    let need_cut = match &active {
                        Active::FloatHistSt { chunk, .. } => {
                            chunk.as_bytes().len() >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2
                        }
                        _ => true,
                    };
                    if need_cut {
                        let prev_from_cut = if let Active::FloatHistSt { ref mut state, .. } = active {
                            state.take().map(|(fhs, _)| fhs)
                        } else {
                            None
                        };
                        flush_active(&mut active, &mut out);
                        active = Active::FloatHistSt {
                            mint: s.t,
                            maxt: s.t,
                            chunk: FloatHistogramStChunk::new(),
                            state: None,
                            prev_state: prev_from_cut,
                        };
                    }
                    if let Active::FloatHistSt {
                        mint,
                        maxt,
                        chunk,
                        state,
                        prev_state,
                    } = &mut active
                    {
                        let outcome = chunk.append_with_state_prev(
                            state,
                            prev_state.as_ref(),
                            s.st,
                            s.t,
                            fh,
                        )?;
                        match outcome {
                            AppendFloatHistogramStOutcome::Appended { .. } => {
                                *maxt = s.t;
                            }
                            AppendFloatHistogramStOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::FloatHistogramSt(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                                *prev_state = None;
                            }
                        }
                    }
                } else {
                    let need_cut = match &active {
                        Active::FloatHist { chunk, .. } => {
                            chunk.as_bytes().len() >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2
                        }
                        _ => true,
                    };
                    if need_cut {
                        let prev_from_cut = if let Active::FloatHist { ref mut state, .. } = active {
                            state.take()
                        } else {
                            None
                        };
                        flush_active(&mut active, &mut out);
                        active = Active::FloatHist {
                            mint: s.t,
                            maxt: s.t,
                            chunk: FloatHistogramChunk::new(),
                            state: None,
                            prev_state: prev_from_cut,
                        };
                    }
                    if let Active::FloatHist {
                        mint,
                        maxt,
                        chunk,
                        state,
                        prev_state,
                    } = &mut active
                    {
                        let outcome = chunk.append_with_state_prev(
                            state,
                            prev_state.as_ref(),
                            s.st,
                            s.t,
                            fh,
                        )?;
                        match outcome {
                            AppendFloatHistogramOutcome::Appended { .. } => {
                                *maxt = s.t;
                            }
                            AppendFloatHistogramOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::FloatHistogram(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                                *prev_state = None;
                            }
                        }
                    }
                }
            }
        }
    }

    flush_active(&mut active, &mut out);
    Ok(out)
}

/// Re-encodes a single chunk excluding samples matching `tombs` (`iv.in_bounds(s.t)`),
/// preserving the chunk's original encoding and without trimming samples to `[mint, maxt]`
/// (matching Prometheus `populateCurrForSingleChunk` with `DisableTrimming = true`).
///
/// Returns `Ok(None)` if all samples in the chunk are covered by `tombs`.
pub fn reencode_chunk_without_tombstones(
    cm: ChunkMeta,
    tombs: &[Interval],
) -> Result<Option<ChunkMeta>, ChunkError> {
    reencode_single_chunk_filtered(cm, tombs, i64::MIN, i64::MAX)
}

fn compact_chunk_in_place(cm: &mut ChunkMeta) {
    if let Some(chk) = cm.chunk.as_mut() {
        match chk {
            EncodedChunk::Xor(x) => x.compact(),
            EncodedChunk::Xor2(x) => x.compact(),
            EncodedChunk::Histogram(h) => h.compact(),
            EncodedChunk::HistogramSt(h) => h.compact(),
            EncodedChunk::FloatHistogram(fh) => fh.compact(),
            EncodedChunk::FloatHistogramSt(fh) => fh.compact(),
        }
    }
}

fn reencode_single_chunk_filtered(
    mut cm: ChunkMeta,
    tombs: &[Interval],
    mint: i64,
    maxt_inclusive: i64,
) -> Result<Option<ChunkMeta>, ChunkError> {
    let outside_range = cm.min_time < mint || cm.max_time > maxt_inclusive;
    let overlaps_tombs = !tombs.is_empty()
        && tombs
            .iter()
            .any(|iv| iv.overlaps(cm.min_time, cm.max_time));
    if !outside_range && !overlaps_tombs {
        compact_chunk_in_place(&mut cm);
        return Ok(Some(cm));
    }
    if tombs
        .iter()
        .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
    {
        return Ok(None);
    }
    let Some(ref enc) = cm.chunk else {
        return Ok(None);
    };
    let samples = decode_chunk_samples(enc)?;
    let surviving: Vec<DecodedSample> = samples
        .into_iter()
        .filter(|s| {
            s.t >= mint && s.t <= maxt_inclusive && !tombs.iter().any(|iv| iv.in_bounds(s.t))
        })
        .collect();
    if surviving.is_empty() {
        return Ok(None);
    }

    let mint = surviving[0].t;
    let maxt = surviving[surviving.len() - 1].t;
    let new_enc = match enc.encoding() {
        Encoding::Xor => {
            let mut c = XorChunk::new();
            {
                let mut app = c.appender()?;
                for s in &surviving {
                    if let SampleValue::Float(v) = s.value {
                        app.append(s.st, s.t, v)?;
                    }
                }
            }
            c.compact();
            EncodedChunk::Xor(c)
        }
        Encoding::Xor2 => {
            let mut c = Xor2Chunk::new();
            {
                let mut app = c.appender()?;
                for s in &surviving {
                    if let SampleValue::Float(v) = s.value {
                        app.append(s.st, s.t, v)?;
                    }
                }
            }
            c.compact();
            EncodedChunk::Xor2(c)
        }
        Encoding::Histogram => {
            let mut c = HistogramChunk::new();
            {
                let mut app = c.appender()?;
                for s in &surviving {
                    if let SampleValue::Histogram(ref h) = s.value {
                        app.append_histogram(None, s.st, s.t, h, true)?;
                    }
                }
            }
            c.compact();
            EncodedChunk::Histogram(c)
        }
        Encoding::HistogramSt => {
            let mut c = HistogramStChunk::new();
            {
                let mut app = c.appender()?;
                for s in &surviving {
                    if let SampleValue::Histogram(ref h) = s.value {
                        app.append_histogram(None, s.st, s.t, h, true)?;
                    }
                }
            }
            c.compact();
            EncodedChunk::HistogramSt(c)
        }
        Encoding::FloatHistogram => {
            let mut c = FloatHistogramChunk::new();
            {
                let mut app = c.appender()?;
                for s in &surviving {
                    if let SampleValue::FloatHistogram(ref fh) = s.value {
                        app.append_float_histogram(None, s.st, s.t, fh, true)?;
                    }
                }
            }
            c.compact();
            EncodedChunk::FloatHistogram(c)
        }
        Encoding::FloatHistogramSt => {
            let mut c = FloatHistogramStChunk::new();
            {
                let mut app = c.appender()?;
                for s in &surviving {
                    if let SampleValue::FloatHistogram(ref fh) = s.value {
                        app.append_float_histogram(None, s.st, s.t, fh, true)?;
                    }
                }
            }
            c.compact();
            EncodedChunk::FloatHistogramSt(c)
        }
        Encoding::None => return Ok(None),
    };

    Ok(Some(ChunkMeta {
        ref_id: cm.ref_id,
        min_time: mint,
        max_time: maxt,
        chunk: Some(new_enc),
    }))
}

/// Re-encodes a sorted, deduplicated stream of samples from overlapping chunks into new chunks,
/// matching Prometheus `populateChunksFromIterable` (`querier.go:1207-1330`):
/// - Switches to ST-capable encoding (`Xor2`, `HistogramSt`, `FloatHistogramSt`) whenever `st != 0`.
/// - Uses `MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND` for float chunks and
///   `TARGET_BYTES_PER_HISTOGRAM_CHUNK` + `MIN_SAMPLES_PER_HISTOGRAM_CHUNK` for histogram chunks.
pub fn encode_iterable_chunks(samples: &[DecodedSample]) -> Result<Vec<ChunkMeta>, ChunkError> {
    encode_iterable_chunks_with_float_encoding(samples, Encoding::Xor)
}

/// Like [`encode_iterable_chunks`], respecting `float_encoding` (`Xor` vs `Xor2`) when `st == 0`
/// (matching Prometheus `NewCompactingChunkSeriesMergerWithFloatEncoding`).
pub fn encode_iterable_chunks_with_float_encoding(
    samples: &[DecodedSample],
    float_encoding: Encoding,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let prefer_xor2 = float_encoding == Encoding::Xor2;

    enum Active {
        None,
        Xor {
            mint: i64,
            maxt: i64,
            chunk: XorChunk,
            state: XorAppendState,
            prev_t: i64,
            prev_v: f64,
        },
        Xor2 {
            mint: i64,
            maxt: i64,
            chunk: Xor2Chunk,
            state: Xor2AppendState,
        },
        Hist {
            mint: i64,
            maxt: i64,
            chunk: HistogramChunk,
            state: Option<HistogramAppenderState>,
        },
        HistSt {
            mint: i64,
            maxt: i64,
            chunk: HistogramStChunk,
            state: Option<(HistogramAppenderState, StEncoder)>,
        },
        FloatHist {
            mint: i64,
            maxt: i64,
            chunk: FloatHistogramChunk,
            state: Option<FloatHistogramAppenderState>,
        },
        FloatHistSt {
            mint: i64,
            maxt: i64,
            chunk: FloatHistogramStChunk,
            state: Option<(FloatHistogramAppenderState, StEncoder)>,
        },
    }

    let mut out = Vec::new();
    let mut active = Active::None;
    let mut has_ts = false;

    let flush_active = |active: &mut Active, out: &mut Vec<ChunkMeta>| {
        match std::mem::replace(active, Active::None) {
            Active::None => {}
            Active::Xor {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Xor(chunk)),
                });
            }
            Active::Xor2 {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Xor2(chunk)),
                });
            }
            Active::Hist {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Histogram(chunk)),
                });
            }
            Active::HistSt {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::HistogramSt(chunk)),
                });
            }
            Active::FloatHist {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::FloatHistogram(chunk)),
                });
            }
            Active::FloatHistSt {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::FloatHistogramSt(chunk)),
                });
            }
        }
    };

    for s in samples {
        let need_ts = s.st != 0;
        match &s.value {
            SampleValue::Float(v) => {
                let type_mismatch_or_st = !matches!(active, Active::Xor { .. } | Active::Xor2 { .. })
                    || (!has_ts && need_ts);
                let size_cut = !type_mismatch_or_st
                    && match &active {
                        Active::Xor { chunk, .. } => {
                            chunk.as_bytes().len() > MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND
                        }
                        Active::Xor2 { chunk, .. } => {
                            chunk.as_bytes().len() > MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND
                        }
                        _ => false,
                    };
                if type_mismatch_or_st || size_cut {
                    flush_active(&mut active, &mut out);
                    has_ts = need_ts;
                    active = if need_ts || prefer_xor2 {
                        Active::Xor2 {
                            mint: s.t,
                            maxt: s.t,
                            chunk: Xor2Chunk::new(),
                            state: Xor2AppendState::new_empty(),
                        }
                    } else {
                        Active::Xor {
                            mint: s.t,
                            maxt: s.t,
                            chunk: XorChunk::new(),
                            state: XorAppendState::new_empty(),
                            prev_t: i64::MIN,
                            prev_v: 0.0,
                        }
                    };
                }
                match &mut active {
                    Active::Xor {
                        maxt,
                        chunk,
                        state,
                        prev_t,
                        prev_v,
                        ..
                    } => {
                        chunk.append_with_state(state, *prev_t, *prev_v, s.t, *v)?;
                        *prev_t = s.t;
                        *prev_v = *v;
                        *maxt = s.t;
                    }
                    Active::Xor2 { maxt, chunk, state, .. } => {
                        chunk.append_with_state(state, s.st, s.t, *v)?;
                        *maxt = s.t;
                    }
                    _ => unreachable!(),
                }
            }
            SampleValue::Histogram(h) => {
                let type_mismatch_or_st =
                    !matches!(active, Active::Hist { .. } | Active::HistSt { .. })
                        || (!has_ts && need_ts);
                let size_cut = !type_mismatch_or_st
                    && match &active {
                        Active::Hist { chunk, .. } => {
                            chunk.as_bytes().len() > TARGET_BYTES_PER_HISTOGRAM_CHUNK
                                && chunk.num_samples() > MIN_SAMPLES_PER_HISTOGRAM_CHUNK
                        }
                        Active::HistSt { chunk, .. } => {
                            chunk.as_bytes().len() > TARGET_BYTES_PER_HISTOGRAM_CHUNK
                                && chunk.num_samples() > MIN_SAMPLES_PER_HISTOGRAM_CHUNK
                        }
                        _ => false,
                    };
                if type_mismatch_or_st || size_cut {
                    flush_active(&mut active, &mut out);
                    has_ts = need_ts;
                    active = if need_ts {
                        Active::HistSt {
                            mint: s.t,
                            maxt: s.t,
                            chunk: HistogramStChunk::new(),
                            state: None,
                        }
                    } else {
                        Active::Hist {
                            mint: s.t,
                            maxt: s.t,
                            chunk: HistogramChunk::new(),
                            state: None,
                        }
                    };
                }
                match &mut active {
                    Active::Hist {
                        mint,
                        maxt,
                        chunk,
                        state,
                    } => {
                        let outcome = chunk.append_with_state(state, s.st, s.t, h)?;
                        match outcome {
                            AppendHistogramOutcome::Appended { .. } => *maxt = s.t,
                            AppendHistogramOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::Histogram(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                            }
                        }
                    }
                    Active::HistSt {
                        mint,
                        maxt,
                        chunk,
                        state,
                    } => {
                        let outcome = chunk.append_with_state(state, s.st, s.t, h)?;
                        match outcome {
                            AppendHistogramStOutcome::Appended { .. } => *maxt = s.t,
                            AppendHistogramStOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::HistogramSt(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
            SampleValue::FloatHistogram(fh) => {
                let type_mismatch_or_st = !matches!(
                    active,
                    Active::FloatHist { .. } | Active::FloatHistSt { .. }
                ) || (!has_ts && need_ts);
                let size_cut = !type_mismatch_or_st
                    && match &active {
                        Active::FloatHist { chunk, .. } => {
                            chunk.as_bytes().len() > TARGET_BYTES_PER_HISTOGRAM_CHUNK
                                && chunk.num_samples() > MIN_SAMPLES_PER_HISTOGRAM_CHUNK
                        }
                        Active::FloatHistSt { chunk, .. } => {
                            chunk.as_bytes().len() > TARGET_BYTES_PER_HISTOGRAM_CHUNK
                                && chunk.num_samples() > MIN_SAMPLES_PER_HISTOGRAM_CHUNK
                        }
                        _ => false,
                    };
                if type_mismatch_or_st || size_cut {
                    flush_active(&mut active, &mut out);
                    has_ts = need_ts;
                    active = if need_ts {
                        Active::FloatHistSt {
                            mint: s.t,
                            maxt: s.t,
                            chunk: FloatHistogramStChunk::new(),
                            state: None,
                        }
                    } else {
                        Active::FloatHist {
                            mint: s.t,
                            maxt: s.t,
                            chunk: FloatHistogramChunk::new(),
                            state: None,
                        }
                    };
                }
                match &mut active {
                    Active::FloatHist {
                        mint,
                        maxt,
                        chunk,
                        state,
                    } => {
                        let outcome = chunk.append_with_state(state, s.st, s.t, fh)?;
                        match outcome {
                            AppendFloatHistogramOutcome::Appended { .. } => *maxt = s.t,
                            AppendFloatHistogramOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::FloatHistogram(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                            }
                        }
                    }
                    Active::FloatHistSt {
                        mint,
                        maxt,
                        chunk,
                        state,
                    } => {
                        let outcome = chunk.append_with_state(state, s.st, s.t, fh)?;
                        match outcome {
                            AppendFloatHistogramStOutcome::Appended { .. } => *maxt = s.t,
                            AppendFloatHistogramStOutcome::NewChunk(new_chk) => {
                                let cur_mint = *mint;
                                let cur_maxt = *maxt;
                                let mut old = std::mem::replace(chunk, new_chk);
                                old.compact();
                                out.push(ChunkMeta {
                                    ref_id: 0,
                                    min_time: cur_mint,
                                    max_time: cur_maxt,
                                    chunk: Some(EncodedChunk::FloatHistogramSt(old)),
                                });
                                *mint = s.t;
                                *maxt = s.t;
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    flush_active(&mut active, &mut out);
    Ok(out)
}

/// Merges multiple pre-sorted sample streams using Prometheus `storage.chainSampleIterator`
/// (`merge.go:615-675`) + Go `container/heap` min-heap tie-breaking semantics.
fn chain_sample_merge(streams: Vec<Vec<DecodedSample>>) -> Vec<DecodedSample> {
    let mut iters: Vec<(Vec<DecodedSample>, usize)> = streams
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(|s| (s, 0usize))
        .collect();
    if iters.is_empty() {
        return Vec::new();
    }
    if iters.len() == 1 {
        let Some((mut out, _)) = iters.pop() else {
            return Vec::new();
        };
        out.dedup_by_key(|s| s.t);
        return out;
    }

    #[inline]
    fn heap_up(h: &mut [(i64, usize)], mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2;
            if h[j].0 >= h[i].0 {
                break;
            }
            h.swap(i, j);
            j = i;
        }
    }

    #[inline]
    fn heap_down(h: &mut [(i64, usize)], mut i: usize, n: usize) {
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && h[j2].0 < h[j1].0 {
                j = j2;
            }
            if h[j].0 >= h[i].0 {
                break;
            }
            h.swap(i, j);
            i = j;
        }
    }

    let mut h: Vec<(i64, usize)> = Vec::with_capacity(iters.len() - 1);
    for (k, (stream, _)) in iters.iter().enumerate().skip(1) {
        h.push((stream[0].t, k));
        let len = h.len();
        heap_up(&mut h, len - 1);
    }

    let total_cap: usize = iters.iter().map(|(s, _)| s.len()).sum();
    let mut out = Vec::with_capacity(total_cap);
    let mut curr_id = 0usize;
    let mut last_t: Option<i64> = None;

    loop {
        if iters[curr_id].1 < iters[curr_id].0.len() {
            let idx = iters[curr_id].1;
            iters[curr_id].1 = idx + 1;
            let curr_t = iters[curr_id].0[idx].t;
            if last_t == Some(curr_t) {
                continue;
            }
            if h.is_empty() {
                last_t = Some(curr_t);
                out.push(iters[curr_id].0[idx].clone());
                continue;
            }
            let next_t = h[0].0;
            if curr_t < next_t {
                last_t = Some(curr_t);
                out.push(iters[curr_id].0[idx].clone());
                continue;
            }
            // Rewind cursor to `idx` (current AtT) before pushing onto heap.
            iters[curr_id].1 = idx;
            h.push((curr_t, curr_id));
            let len = h.len();
            heap_up(&mut h, len - 1);
        } else if h.is_empty() {
            break;
        }

        let n = h.len() - 1;
        h.swap(0, n);
        heap_down(&mut h, 0, n);
        let Some((pop_t, pop_id)) = h.pop() else {
            break;
        };
        curr_id = pop_id;
        let idx = iters[curr_id].1;
        iters[curr_id].1 = idx + 1;
        if last_t != Some(pop_t) {
            last_t = Some(pop_t);
            out.push(iters[curr_id].0[idx].clone());
        }
    }

    out
}

/// Applies per-chunk tombstones and (unless `allow_non_overlapping_raw` is set) merges only
/// overlapping chunk clusters, without trimming surviving samples to `[mint, maxt]`
/// (matching Prometheus `ooo_head_read.go:154-175` & `querier.go:1130-1330` under `DisableTrimming = true`).
pub fn merge_chunks_with_tombstones(
    items: Vec<(ChunkMeta, Vec<Interval>)>,
    allow_non_overlapping_raw: bool,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    merge_chunks_with_tombstones_and_float_encoding(items, allow_non_overlapping_raw, Encoding::Xor)
}

/// Maximum samples per chunk emitted by Prometheus `storage.seriesToChunkEncoder`
/// (`storage/series.go:338`).
pub const SERIES_TO_CHUNK_ENCODER_SPLIT: usize = 120;

/// Re-encodes a sorted, deduplicated sample stream matching Prometheus `storage.seriesToChunkEncoder`
/// (`storage/series.go:348-455`): splits chunks every `SERIES_TO_CHUNK_ENCODER_SPLIT` (120) samples,
/// honours `float_encoding` (`Xor` vs `Xor2`) for `st == 0` floats, and preserves compatible
/// already-open `Xor2` chunks when `st == 0`.
pub fn series_to_chunk_encoder_encode(
    samples: &[DecodedSample],
    float_encoding: Encoding,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let prefer_xor2 = float_encoding == Encoding::Xor2;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ValType {
        Float,
        Histogram,
        FloatHistogram,
    }

    enum Active {
        None,
        Xor {
            mint: i64,
            maxt: i64,
            chunk: XorChunk,
            state: XorAppendState,
            prev_t: i64,
            prev_v: f64,
        },
        Xor2 {
            mint: i64,
            maxt: i64,
            chunk: Xor2Chunk,
            state: Xor2AppendState,
        },
        Hist {
            mint: i64,
            maxt: i64,
            chunk: HistogramChunk,
            state: Option<HistogramAppenderState>,
        },
        HistSt {
            mint: i64,
            maxt: i64,
            chunk: HistogramStChunk,
            state: Option<(HistogramAppenderState, StEncoder)>,
        },
        FloatHist {
            mint: i64,
            maxt: i64,
            chunk: FloatHistogramChunk,
            state: Option<FloatHistogramAppenderState>,
        },
        FloatHistSt {
            mint: i64,
            maxt: i64,
            chunk: FloatHistogramStChunk,
            state: Option<(FloatHistogramAppenderState, StEncoder)>,
        },
    }

    impl Active {
        fn encoding(&self) -> Encoding {
            match self {
                Self::None => Encoding::None,
                Self::Xor { .. } => Encoding::Xor,
                Self::Xor2 { .. } => Encoding::Xor2,
                Self::Hist { .. } => Encoding::Histogram,
                Self::HistSt { .. } => Encoding::HistogramSt,
                Self::FloatHist { .. } => Encoding::FloatHistogram,
                Self::FloatHistSt { .. } => Encoding::FloatHistogramSt,
            }
        }
    }

    let flush_active = |active: &mut Active, out: &mut Vec<ChunkMeta>| {
        match std::mem::replace(active, Active::None) {
            Active::None => {}
            Active::Xor {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Xor(chunk)),
                });
            }
            Active::Xor2 {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Xor2(chunk)),
                });
            }
            Active::Hist {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Histogram(chunk)),
                });
            }
            Active::HistSt {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::HistogramSt(chunk)),
                });
            }
            Active::FloatHist {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::FloatHistogram(chunk)),
                });
            }
            Active::FloatHistSt {
                mint,
                maxt,
                mut chunk,
                ..
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::FloatHistogramSt(chunk)),
                });
            }
        }
    };

    let mut out = Vec::new();
    let mut active = Active::None;
    let mut last_type: Option<ValType> = None;
    let mut count_in_chunk = 0usize;

    for s in samples {
        let has_st = s.st != 0;
        let (typ, desired) = match &s.value {
            SampleValue::Float(_) => (
                ValType::Float,
                if has_st || prefer_xor2 {
                    Encoding::Xor2
                } else {
                    Encoding::Xor
                },
            ),
            SampleValue::Histogram(_) => (
                ValType::Histogram,
                if has_st {
                    Encoding::HistogramSt
                } else {
                    Encoding::Histogram
                },
            ),
            SampleValue::FloatHistogram(_) => (
                ValType::FloatHistogram,
                if has_st {
                    Encoding::FloatHistogramSt
                } else {
                    Encoding::FloatHistogram
                },
            ),
        };

        let mut cut = last_type != Some(typ) || count_in_chunk >= SERIES_TO_CHUNK_ENCODER_SPLIT;
        if !cut {
            let cur_enc = active.encoding();
            if cur_enc != desired {
                let compatible = matches!(
                    (cur_enc, desired),
                    (Encoding::Xor | Encoding::Xor2, Encoding::Xor | Encoding::Xor2)
                );
                cut = !compatible || (has_st && cur_enc == Encoding::Xor);
            }
        }

        if cut {
            flush_active(&mut active, &mut out);
            active = match desired {
                Encoding::Xor => Active::Xor {
                    mint: s.t,
                    maxt: s.t,
                    chunk: XorChunk::new(),
                    state: XorAppendState::new_empty(),
                    prev_t: i64::MIN,
                    prev_v: 0.0,
                },
                Encoding::Xor2 => Active::Xor2 {
                    mint: s.t,
                    maxt: s.t,
                    chunk: Xor2Chunk::new(),
                    state: Xor2AppendState::new_empty(),
                },
                Encoding::Histogram => Active::Hist {
                    mint: s.t,
                    maxt: s.t,
                    chunk: HistogramChunk::new(),
                    state: None,
                },
                Encoding::HistogramSt => Active::HistSt {
                    mint: s.t,
                    maxt: s.t,
                    chunk: HistogramStChunk::new(),
                    state: None,
                },
                Encoding::FloatHistogram => Active::FloatHist {
                    mint: s.t,
                    maxt: s.t,
                    chunk: FloatHistogramChunk::new(),
                    state: None,
                },
                Encoding::FloatHistogramSt => Active::FloatHistSt {
                    mint: s.t,
                    maxt: s.t,
                    chunk: FloatHistogramStChunk::new(),
                    state: None,
                },
                Encoding::None => unreachable!(),
            };
            count_in_chunk = 0;
        }
        last_type = Some(typ);

        match &s.value {
            SampleValue::Float(v) => match &mut active {
                Active::Xor {
                    maxt,
                    chunk,
                    state,
                    prev_t,
                    prev_v,
                    ..
                } => {
                    chunk.append_with_state(state, *prev_t, *prev_v, s.t, *v)?;
                    *prev_t = s.t;
                    *prev_v = *v;
                    *maxt = s.t;
                }
                Active::Xor2 {
                    maxt,
                    chunk,
                    state,
                    ..
                } => {
                    chunk.append_with_state(state, s.st, s.t, *v)?;
                    *maxt = s.t;
                }
                _ => unreachable!(),
            },
            SampleValue::Histogram(h) => match &mut active {
                Active::Hist {
                    mint,
                    maxt,
                    chunk,
                    state,
                } => match chunk.append_with_state(state, s.st, s.t, h)? {
                    AppendHistogramOutcome::Appended { .. } => *maxt = s.t,
                    AppendHistogramOutcome::NewChunk(new_chk) => {
                        let cur_mint = *mint;
                        let cur_maxt = *maxt;
                        let mut old = std::mem::replace(chunk, new_chk);
                        old.compact();
                        out.push(ChunkMeta {
                            ref_id: 0,
                            min_time: cur_mint,
                            max_time: cur_maxt,
                            chunk: Some(EncodedChunk::Histogram(old)),
                        });
                        *mint = s.t;
                        *maxt = s.t;
                        count_in_chunk = 0;
                    }
                },
                Active::HistSt {
                    mint,
                    maxt,
                    chunk,
                    state,
                } => match chunk.append_with_state(state, s.st, s.t, h)? {
                    AppendHistogramStOutcome::Appended { .. } => *maxt = s.t,
                    AppendHistogramStOutcome::NewChunk(new_chk) => {
                        let cur_mint = *mint;
                        let cur_maxt = *maxt;
                        let mut old = std::mem::replace(chunk, new_chk);
                        old.compact();
                        out.push(ChunkMeta {
                            ref_id: 0,
                            min_time: cur_mint,
                            max_time: cur_maxt,
                            chunk: Some(EncodedChunk::HistogramSt(old)),
                        });
                        *mint = s.t;
                        *maxt = s.t;
                        count_in_chunk = 0;
                    }
                },
                _ => unreachable!(),
            },
            SampleValue::FloatHistogram(fh) => match &mut active {
                Active::FloatHist {
                    mint,
                    maxt,
                    chunk,
                    state,
                } => match chunk.append_with_state(state, s.st, s.t, fh)? {
                    AppendFloatHistogramOutcome::Appended { .. } => *maxt = s.t,
                    AppendFloatHistogramOutcome::NewChunk(new_chk) => {
                        let cur_mint = *mint;
                        let cur_maxt = *maxt;
                        let mut old = std::mem::replace(chunk, new_chk);
                        old.compact();
                        out.push(ChunkMeta {
                            ref_id: 0,
                            min_time: cur_mint,
                            max_time: cur_maxt,
                            chunk: Some(EncodedChunk::FloatHistogram(old)),
                        });
                        *mint = s.t;
                        *maxt = s.t;
                        count_in_chunk = 0;
                    }
                },
                Active::FloatHistSt {
                    mint,
                    maxt,
                    chunk,
                    state,
                } => match chunk.append_with_state(state, s.st, s.t, fh)? {
                    AppendFloatHistogramStOutcome::Appended { .. } => *maxt = s.t,
                    AppendFloatHistogramStOutcome::NewChunk(new_chk) => {
                        let cur_mint = *mint;
                        let cur_maxt = *maxt;
                        let mut old = std::mem::replace(chunk, new_chk);
                        old.compact();
                        out.push(ChunkMeta {
                            ref_id: 0,
                            min_time: cur_mint,
                            max_time: cur_maxt,
                            chunk: Some(EncodedChunk::FloatHistogramSt(old)),
                        });
                        *mint = s.t;
                        *maxt = s.t;
                        count_in_chunk = 0;
                    }
                },
                _ => unreachable!(),
            },
        }
        count_in_chunk += 1;
    }

    flush_active(&mut active, &mut out);
    Ok(out)
}

/// Merges chunks across multiple blocks / sources matching Go
/// `storage.NewCompactingChunkSeriesMergerWithFloatEncoding(storage.ChainedSeriesMerge, ...)`
/// (`storage/merge.go:804-895` + `storage/series.go:348-455`):
/// 1. Orders chunks by `(min_time, max_time)`.
/// 2. Elides exact byte-identical duplicate chunks without decoding/re-encoding.
/// 3. Passes `[c1, c2, ..., c0]` (`curr` LAST after `overlapping`) to `chain_sample_merge` so `c0` wins duplicate-timestamp ties.
/// 4. Re-encodes overlapping runs via `series_to_chunk_encoder_encode` (`120`-sample split, `float_encoding`).
pub fn compact_chunk_series_merge(
    mut chunks: Vec<ChunkMeta>,
    float_encoding: Encoding,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }
    if chunks.len() == 1 {
        return Ok(chunks);
    }

    chunks.sort_by_key(|c| (c.min_time, c.max_time));
    let mut slots: Vec<Option<ChunkMeta>> = chunks.into_iter().map(Some).collect();
    let mut out = Vec::with_capacity(slots.len());
    let mut i = 0usize;

    while i < slots.len() {
        let Some(curr) = slots[i].take() else {
            i += 1;
            continue;
        };
        let mut o_max = curr.max_time;
        let mut overlapping: Vec<ChunkMeta> = Vec::new();
        let mut j = i + 1;

        while j < slots.len() {
            let Some(ref next) = slots[j] else {
                j += 1;
                continue;
            };
            if next.min_time > o_max {
                break;
            }
            let prev = overlapping.last().unwrap_or(&curr);
            let is_exact_dup = next.min_time == prev.min_time
                && next.max_time == prev.max_time
                && next.chunk.as_ref().map(|c| (c.encoding(), c.as_bytes()))
                    == prev.chunk.as_ref().map(|c| (c.encoding(), c.as_bytes()));
            let Some(next_taken) = slots[j].take() else {
                j += 1;
                continue;
            };
            j += 1;
            if !is_exact_dup {
                o_max = o_max.max(next_taken.max_time);
                overlapping.push(next_taken);
            }
        }
        i = j;

        if overlapping.is_empty() {
            out.push(curr);
            continue;
        }

        // Go `compactChunkIterator.Next()`: `c.curr` is appended LAST after `overlapping`
        // so `chainSampleIterator` gives `c.curr` precedence on duplicate timestamps.
        let mut streams = Vec::with_capacity(overlapping.len() + 1);
        for cm in overlapping.into_iter().chain(std::iter::once(curr)) {
            let Some(ref enc) = cm.chunk else {
                continue;
            };
            let stream = decode_chunk_samples(enc)?;
            if !stream.is_empty() {
                streams.push(stream);
            }
        }
        let merged = chain_sample_merge(streams);
        if !merged.is_empty() {
            out.extend(series_to_chunk_encoder_encode(&merged, float_encoding)?);
        }
    }

    Ok(out)
}

/// Like [`merge_chunks_with_tombstones`], respecting `float_encoding` (`Xor` vs `Xor2`) on
/// merged float clusters with `st == 0`.
pub fn merge_chunks_with_tombstones_and_float_encoding(
    mut items: Vec<(ChunkMeta, Vec<Interval>)>,
    allow_non_overlapping_raw: bool,
    float_encoding: Encoding,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    // Drop chunks completely covered by tombstones first so they don't force unnecessary cluster merges.
    items.retain(|(cm, tombs)| {
        !tombs
            .iter()
            .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
    });
    if items.is_empty() {
        return Ok(Vec::new());
    }

    if allow_non_overlapping_raw {
        let mut out = Vec::with_capacity(items.len());
        for (cm, tombs) in items {
            if let Some(c) = reencode_chunk_without_tombstones(cm, &tombs)? {
                out.push(c);
            }
        }
        return Ok(out);
    }

    items.sort_by_key(|(cm, _)| (cm.min_time, cm.ref_id));

    let mut out = Vec::with_capacity(items.len());
    let mut cluster: Vec<(ChunkMeta, Vec<Interval>)> = Vec::new();
    let mut cluster_max = i64::MIN;

    let flush_cluster = |cluster: &mut Vec<(ChunkMeta, Vec<Interval>)>,
                         out: &mut Vec<ChunkMeta>|
     -> Result<(), ChunkError> {
        if cluster.is_empty() {
            return Ok(());
        }
        if cluster.len() == 1 {
            let (cm, tombs) = cluster.pop().unwrap();
            if let Some(c) = reencode_chunk_without_tombstones(cm, &tombs)? {
                out.push(c);
            }
            return Ok(());
        }
        let mut streams = Vec::with_capacity(cluster.len());
        for (cm, tombs) in cluster.drain(..) {
            let Some(ref enc) = cm.chunk else {
                continue;
            };
            let mut stream = Vec::new();
            for s in decode_chunk_samples(enc)? {
                if tombs.iter().any(|iv| iv.in_bounds(s.t)) {
                    continue;
                }
                stream.push(s);
            }
            if !stream.is_empty() {
                streams.push(stream);
            }
        }
        let samples = chain_sample_merge(streams);
        if !samples.is_empty() {
            out.extend(encode_iterable_chunks_with_float_encoding(
                &samples,
                float_encoding,
            )?);
        }
        Ok(())
    };

    for (cm, tombs) in items {
        if cluster.is_empty() {
            cluster_max = cm.max_time;
            cluster.push((cm, tombs));
        } else if cm.min_time > cluster_max {
            flush_cluster(&mut cluster, &mut out)?;
            cluster_max = cm.max_time;
            cluster.push((cm, tombs));
        } else {
            cluster_max = cluster_max.max(cm.max_time);
            cluster.push((cm, tombs));
        }
    }
    flush_cluster(&mut cluster, &mut out)?;
    Ok(out)
}

/// Applies shared series tombstones and merges only overlapping chunk clusters for Head chunks,
/// avoiding cloning `tombs` per chunk.
pub fn merge_head_chunks_with_tombstones(
    chks: Vec<ChunkMeta>,
    tombs: &[Interval],
) -> Result<Vec<ChunkMeta>, ChunkError> {
    merge_head_chunks_with_tombstones_and_float_encoding(chks, tombs, Encoding::Xor)
}

/// Like [`merge_head_chunks_with_tombstones`], respecting `float_encoding` (`Xor` vs `Xor2`).
pub fn merge_head_chunks_with_tombstones_and_float_encoding(
    mut chks: Vec<ChunkMeta>,
    tombs: &[Interval],
    float_encoding: Encoding,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    if !tombs.is_empty() {
        chks.retain(|cm| {
            !tombs
                .iter()
                .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
        });
    }
    if chks.is_empty() {
        return Ok(Vec::new());
    }

    let already_sorted_disjoint = chks
        .windows(2)
        .all(|w| w[0].min_time <= w[1].min_time && w[0].max_time < w[1].min_time);
    if !already_sorted_disjoint {
        chks.sort_by_key(|c| (c.min_time, c.ref_id));
    }

    let mut out = Vec::with_capacity(chks.len());
    let mut cluster: Vec<ChunkMeta> = Vec::new();
    let mut cluster_max = i64::MIN;

    let flush_cluster =
        |cluster: &mut Vec<ChunkMeta>, out: &mut Vec<ChunkMeta>| -> Result<(), ChunkError> {
            if cluster.is_empty() {
                return Ok(());
            }
            if cluster.len() == 1 {
                let cm = cluster.pop().unwrap();
                if let Some(c) = reencode_chunk_without_tombstones(cm, tombs)? {
                    out.push(c);
                }
                return Ok(());
            }
            let mut streams = Vec::with_capacity(cluster.len());
            for cm in cluster.drain(..) {
                let Some(ref enc) = cm.chunk else {
                    continue;
                };
                let mut stream = Vec::new();
                for s in decode_chunk_samples(enc)? {
                    if !tombs.is_empty() && tombs.iter().any(|iv| iv.in_bounds(s.t)) {
                        continue;
                    }
                    stream.push(s);
                }
                if !stream.is_empty() {
                    streams.push(stream);
                }
            }
            let samples = chain_sample_merge(streams);
            if !samples.is_empty() {
                out.extend(encode_iterable_chunks_with_float_encoding(
                    &samples,
                    float_encoding,
                )?);
            }
            Ok(())
        };

    for cm in chks {
        if cluster.is_empty() {
            cluster_max = cm.max_time;
            cluster.push(cm);
        } else if cm.min_time > cluster_max {
            flush_cluster(&mut cluster, &mut out)?;
            cluster_max = cm.max_time;
            cluster.push(cm);
        } else {
            cluster_max = cluster_max.max(cm.max_time);
            cluster.push(cm);
        }
    }
    flush_cluster(&mut cluster, &mut out)?;
    Ok(out)
}

/// Input series fed into [`write_block_from_series`].
#[derive(Clone, Debug)]
pub struct CompactionSeries {
    pub labels: Labels,
    pub chunks: Vec<ChunkMeta>,
    pub tombstones: Vec<Interval>,
}

/// Trims/merges `chunks` to closed `[mint, maxt_inclusive]` excluding `tombstones`,
/// matching Prometheus `populateWithDelChunkSeriesIterator`:
/// - Non-overlapping single chunks are kept verbatim (compacted in-place) or trimmed individually
///   with `prev_state = None` while preserving their original encoding (`populateCurrForSingleChunk`).
/// - Only overlapping clusters are merged via `chain_sample_merge` and re-encoded via
///   `encode_iterable_chunks` (`populateChunksFromIterable`, where `st == 0` floats use `Xor`).
pub fn trim_and_merge_chunks(
    mut chunks: Vec<ChunkMeta>,
    tombstones: &[Interval],
    mint: i64,
    maxt_inclusive: i64,
    _samples_per_chunk: usize,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    if chunks.is_empty() || mint > maxt_inclusive {
        return Ok(Vec::new());
    }

    // Drop chunks completely outside [mint, maxt_inclusive] or completely covered by tombstones.
    chunks.retain(|c| {
        c.overlaps_closed_interval(mint, maxt_inclusive)
            && !tombstones
                .iter()
                .any(|iv| iv.is_subrange(c.min_time, c.max_time))
    });
    if chunks.is_empty() {
        return Ok(Vec::new());
    }

    chunks.sort_by_key(|c| (c.min_time, c.ref_id));

    let mut out = Vec::with_capacity(chunks.len());
    let mut cluster: Vec<ChunkMeta> = Vec::new();
    let mut cluster_max = i64::MIN;

    let flush_cluster =
        |cluster: &mut Vec<ChunkMeta>, out: &mut Vec<ChunkMeta>| -> Result<(), ChunkError> {
            if cluster.is_empty() {
                return Ok(());
            }
            if cluster.len() == 1 {
                let Some(cm) = cluster.pop() else {
                    return Ok(());
                };
                if let Some(c) =
                    reencode_single_chunk_filtered(cm, tombstones, mint, maxt_inclusive)?
                {
                    out.push(c);
                }
                return Ok(());
            }
            let mut streams = Vec::with_capacity(cluster.len());
            for cm in cluster.drain(..) {
                let Some(ref encoded) = cm.chunk else {
                    continue;
                };
                let mut stream = Vec::new();
                for s in decode_chunk_samples(encoded)? {
                    if s.t < mint || s.t > maxt_inclusive {
                        continue;
                    }
                    if !tombstones.is_empty() && tombstones.iter().any(|iv| iv.in_bounds(s.t)) {
                        continue;
                    }
                    stream.push(s);
                }
                if !stream.is_empty() {
                    streams.push(stream);
                }
            }
            let merged = chain_sample_merge(streams);
            if !merged.is_empty() {
                out.extend(encode_iterable_chunks(&merged)?);
            }
            Ok(())
        };

    for cm in chunks {
        if cluster.is_empty() {
            cluster_max = cm.max_time;
            cluster.push(cm);
        } else if cm.min_time > cluster_max {
            flush_cluster(&mut cluster, &mut out)?;
            cluster_max = cm.max_time;
            cluster.push(cm);
        } else {
            cluster_max = cluster_max.max(cm.max_time);
            cluster.push(cm);
        }
    }
    flush_cluster(&mut cluster, &mut out)?;
    Ok(out)
}

/// Streams Head compaction without materializing all `CompactionSeries` or chunk buffers in RAM.
///
/// `candidates` is a lightweight `Vec<(u64, Labels)>` (each `Labels` clone is an O(1) `Bytes`
/// refcount bump) already pre-filtered to series overlapping `[mint, maxt - 1]`.
#[allow(clippy::too_many_arguments)]
pub fn write_head_block_streaming<F>(
    dest: &Path,
    mint: i64,
    maxt: i64,
    mut candidates: Vec<(u64, Labels)>,
    compaction: BlockMetaCompaction,
    out_of_order: bool,
    segment_size: usize,
    mut load_series_chunks: F,
) -> Result<Option<BlockMeta>, CompactError>
where
    F: FnMut(u64, &mut Vec<ChunkMeta>, &mut Vec<Interval>) -> Result<(), CompactError>,
{
    if maxt <= mint || candidates.is_empty() {
        return Ok(None);
    }
    let maxt_inclusive = maxt - 1;

    candidates.sort_unstable_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    candidates.dedup_by_key(|a| a.0);

    let uid = Ulid::new();
    let final_dir = dest.join(uid.to_string());
    let tmp_dir = dest.join(format!("{uid}{TMP_BLOCK_DIR_SUFFIX}"));
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)?;
    }
    fs::create_dir_all(&tmp_dir)?;

    let write_res = (|| -> Result<Option<BlockMeta>, CompactError> {
        let seg_sz = if segment_size == 0 {
            DEFAULT_CHUNK_SEGMENT_SIZE
        } else {
            segment_size
        };
        let mut chunkw = BlockChunksWriter::with_segment_size(tmp_dir.join("chunks"), seg_sz)?;
        let mut indexw = IndexWriter::new(tmp_dir.join(INDEX_FILENAME))?;

        // Pass 1: collect unique &str symbols across lightweight candidate Labels (zero chunk allocs).
        {
            let mut sym_set: hashbrown::HashSet<&str> =
                hashbrown::HashSet::with_capacity(candidates.len().min(65536));
            sym_set.insert("");
            for (_, labels) in &candidates {
                for (k, v) in labels.iter() {
                    sym_set.insert(k);
                    sym_set.insert(v);
                }
            }
            let mut symbols: Vec<&str> = sym_set.into_iter().collect();
            symbols.sort_unstable();
            for sym in symbols {
                indexw.add_symbol(sym)?;
            }
        }

        // Pass 2: stream one series at a time using reused ChunkMeta/Interval scratch Vecs.
        let mut stats = BlockStats::default();
        let mut chks_buf: Vec<ChunkMeta> = Vec::with_capacity(4);
        let mut tombs_buf: Vec<Interval> = Vec::with_capacity(2);
        let mut series_idx = 0u64;

        for (ref_id, labels) in candidates {
            chks_buf.clear();
            tombs_buf.clear();
            load_series_chunks(ref_id, &mut chks_buf, &mut tombs_buf)?;
            if chks_buf.is_empty() {
                continue;
            }
            let mut trimmed = trim_and_merge_chunks(
                std::mem::take(&mut chks_buf),
                &tombs_buf,
                mint,
                maxt_inclusive,
                DEFAULT_SAMPLES_PER_CHUNK,
            )?;
            if trimmed.is_empty() {
                chks_buf = trimmed;
                continue;
            }

            chunkw.write_chunks(&mut trimmed)?;
            indexw.add_series(series_idx, &labels, &trimmed)?;
            series_idx += 1;

            stats.num_series += 1;
            stats.num_chunks += trimmed.len() as u64;
            for chk in &trimmed {
                if let Some(ref enc) = chk.chunk {
                    let n = enc.num_samples() as u64;
                    stats.num_samples += n;
                    match enc.encoding() {
                        Encoding::Xor | Encoding::Xor2 => stats.num_float_samples += n,
                        Encoding::Histogram
                        | Encoding::FloatHistogram
                        | Encoding::HistogramSt
                        | Encoding::FloatHistogramSt => stats.num_histogram_samples += n,
                        Encoding::None => {}
                    }
                }
            }
            trimmed.clear();
            chks_buf = trimmed;
        }

        chunkw.close()?;
        indexw.close()?;

        if stats.num_samples == 0 {
            return Ok(None);
        }

        let mut comp = compaction;
        if comp.sources.is_empty() {
            comp.sources = vec![uid];
        } else {
            comp.sources.sort();
            comp.sources.dedup();
        }
        if comp.level <= 0 {
            comp.level = 1;
        }
        if out_of_order {
            comp.set_out_of_order();
        }

        let mut meta = BlockMeta {
            ulid: uid,
            min_time: mint,
            max_time: maxt,
            stats,
            compaction: comp,
            version: META_VERSION_1,
            out_of_order,
            thanos: None,
        };
        write_meta_file(&tmp_dir, &mut meta)?;
        write_tombstones_file(&tmp_dir, &MemTombstones::new())?;

        if final_dir.exists() {
            fs::remove_dir_all(&final_dir)?;
        }
        sync_dir(&tmp_dir.join("chunks"))?;
        sync_dir(&tmp_dir)?;
        fs::rename(&tmp_dir, &final_dir)?;
        sync_dir(dest)?;
        Ok(Some(meta))
    })();

    if write_res.is_err() || matches!(write_res, Ok(None)) {
        let _ = fs::remove_dir_all(&tmp_dir);
    }
    write_res
}

fn trim_and_merge_chunks_per_chunk(
    chunks: Vec<(ChunkMeta, std::sync::Arc<[Interval]>)>,
    mint: i64,
    maxt_inclusive: i64,
    float_encoding: Encoding,
) -> Result<Vec<ChunkMeta>, ChunkError> {
    if chunks.is_empty() || mint > maxt_inclusive {
        return Ok(Vec::new());
    }

    let mut filtered = Vec::with_capacity(chunks.len());
    for (cm, tombs) in chunks {
        if !cm.overlaps_closed_interval(mint, maxt_inclusive)
            || tombs
                .iter()
                .any(|iv| iv.is_subrange(cm.min_time, cm.max_time))
        {
            continue;
        }
        if let Some(c) = reencode_single_chunk_filtered(cm, &tombs, mint, maxt_inclusive)? {
            filtered.push(c);
        }
    }
    compact_chunk_series_merge(filtered, float_encoding)
}

/// Writes a single Prometheus ULID block directory into `dest` from sorted [`CompactionSeries`]
/// over the half-open interval `[mint, maxt)`.
///
/// Returns `Ok(None)` without writing a block if no samples remain after trimming/tombstones.
pub fn write_block_from_series(
    dest: &Path,
    mint: i64,
    maxt: i64,
    series: Vec<CompactionSeries>,
    compaction: BlockMetaCompaction,
    out_of_order: bool,
    segment_size: usize,
) -> Result<Option<BlockMeta>, CompactError> {
    if maxt <= mint || series.is_empty() {
        return Ok(None);
    }
    let maxt_inclusive = maxt - 1;

    // Prepare trimmed series & collect sorted symbol set.
    let mut prepared: Vec<(Labels, Vec<ChunkMeta>)> = Vec::with_capacity(series.len());

    for mut s in series {
        let trimmed = trim_and_merge_chunks(
            s.chunks,
            &s.tombstones,
            mint,
            maxt_inclusive,
            DEFAULT_SAMPLES_PER_CHUNK,
        )?;
        if trimmed.is_empty() {
            continue;
        }
        prepared.push((std::mem::take(&mut s.labels), trimmed));
    }

    if prepared.is_empty() {
        return Ok(None);
    }

    prepared.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    write_prepared_block(dest, mint, maxt, prepared, compaction, out_of_order, segment_size)
}

fn write_prepared_block(
    dest: &Path,
    mint: i64,
    maxt: i64,
    prepared: Vec<(Labels, Vec<ChunkMeta>)>,
    compaction: BlockMetaCompaction,
    out_of_order: bool,
    segment_size: usize,
) -> Result<Option<BlockMeta>, CompactError> {

    let uid = Ulid::new();
    let final_dir = dest.join(uid.to_string());
    let tmp_dir = dest.join(format!("{uid}{TMP_BLOCK_DIR_SUFFIX}"));
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)?;
    }
    fs::create_dir_all(&tmp_dir)?;

    let write_res = (|| -> Result<Option<BlockMeta>, CompactError> {
        let seg_sz = if segment_size == 0 {
            DEFAULT_CHUNK_SEGMENT_SIZE
        } else {
            segment_size
        };
        let mut chunkw = BlockChunksWriter::with_segment_size(tmp_dir.join("chunks"), seg_sz)?;
        let mut indexw = IndexWriter::new(tmp_dir.join(INDEX_FILENAME))?;

        {
            let mut sym_set: hashbrown::HashSet<&str> =
                hashbrown::HashSet::with_capacity(prepared.len().min(65536));
            // Always include empty symbol matching Prometheus Head/Compactor convention.
            sym_set.insert("");
            for (labels, _) in &prepared {
                for (k, v) in labels.iter() {
                    sym_set.insert(k);
                    sym_set.insert(v);
                }
            }
            let mut symbols: Vec<&str> = sym_set.into_iter().collect();
            symbols.sort_unstable();
            for sym in symbols {
                indexw.add_symbol(sym)?;
            }
        }

        let mut stats = BlockStats::default();
        for (series_idx, (labels, mut chks)) in prepared.into_iter().enumerate() {
            chunkw.write_chunks(&mut chks)?;
            indexw.add_series(series_idx as u64, &labels, &chks)?;

            stats.num_series += 1;
            stats.num_chunks += chks.len() as u64;
            for chk in &chks {
                if let Some(ref enc) = chk.chunk {
                    let n = enc.num_samples() as u64;
                    stats.num_samples += n;
                    match enc.encoding() {
                        Encoding::Xor | Encoding::Xor2 => stats.num_float_samples += n,
                        Encoding::Histogram
                        | Encoding::FloatHistogram
                        | Encoding::HistogramSt
                        | Encoding::FloatHistogramSt => stats.num_histogram_samples += n,
                        Encoding::None => {}
                    }
                }
            }
        }

        chunkw.close()?;
        indexw.close()?;

        if stats.num_samples == 0 {
            return Ok(None);
        }

        let mut comp = compaction;
        if comp.sources.is_empty() {
            comp.sources = vec![uid];
        } else {
            comp.sources.sort();
            comp.sources.dedup();
        }
        if comp.level <= 0 {
            comp.level = 1;
        }
        if out_of_order {
            comp.set_out_of_order();
        }

        let mut meta = BlockMeta {
            ulid: uid,
            min_time: mint,
            max_time: maxt,
            stats,
            compaction: comp,
            version: META_VERSION_1,
            out_of_order,
            thanos: None,
        };
        write_meta_file(&tmp_dir, &mut meta)?;
        write_tombstones_file(&tmp_dir, &MemTombstones::new())?;

        if final_dir.exists() {
            fs::remove_dir_all(&final_dir)?;
        }
        sync_dir(&tmp_dir.join("chunks"))?;
        sync_dir(&tmp_dir)?;
        fs::rename(&tmp_dir, &final_dir)?;
        sync_dir(dest)?;
        Ok(Some(meta))
    })();

    if write_res.is_err() || matches!(write_res, Ok(None)) {
        let _ = fs::remove_dir_all(&tmp_dir);
    }
    write_res
}

/// Compacts multiple existing on-disk [`Block`]s into a single merged ULID block directory.
pub fn compact_blocks(dest: &Path, blocks: &[&Block]) -> Result<Option<BlockMeta>, CompactError> {
    compact_blocks_with_float_encoding(dest, blocks, Encoding::Xor)
}

/// Compacts multiple existing on-disk [`Block`]s into a single merged ULID block directory,
/// using `float_encoding` (`Xor` vs `Xor2`) when re-encoding overlapping float chunks.
pub fn compact_blocks_with_float_encoding(
    dest: &Path,
    blocks: &[&Block],
    float_encoding: Encoding,
) -> Result<Option<BlockMeta>, CompactError> {
    if blocks.is_empty() {
        return Ok(None);
    }

    let mut mint = i64::MAX;
    let mut maxt = i64::MIN;
    let mut max_level = 0i32;
    let mut sources_set = std::collections::BTreeSet::new();
    let mut parents = Vec::with_capacity(blocks.len());
    let mut all_ooo = true;
    let mut any_stale = false;
    let mut any_selected = false;

    for b in blocks {
        let m = b.meta();
        mint = mint.min(m.min_time);
        maxt = maxt.max(m.max_time);
        max_level = max_level.max(m.compaction.level);
        for &s in &m.compaction.sources {
            sources_set.insert(s);
        }
        if m.compaction.from_stale_series() {
            any_stale = true;
        }
        if m.compaction.from_selected_series() {
            any_selected = true;
        }
        if !m.compaction.from_out_of_order() {
            all_ooo = false;
        }
        parents.push(BlockDesc {
            ulid: m.ulid,
            min_time: m.min_time,
            max_time: m.max_time,
        });
    }
    let sources: Vec<Ulid> = sources_set.into_iter().collect();

    if maxt <= mint {
        return Ok(None);
    }
    let maxt_inclusive = maxt - 1;

    // Collect all series grouped by Labels across all input blocks, keeping each block's
    // tombstones scoped strictly to chunks from that block (matching Go LeveledCompactor).
    type SeriesChunkList = Vec<(ChunkMeta, std::sync::Arc<[Interval]>)>;
    let mut series_map: std::collections::BTreeMap<Labels, SeriesChunkList> =
        std::collections::BTreeMap::new();

    for b in blocks {
        let idx = b.index();
        let chk_r = b.chunks();
        let tomb_r = b.tombstones();
        let all_ids = idx
            .postings("", &[""])
            .map_err(CompactError::Index)?;
        for id in all_ids {
            let (labels, mut chks) = idx.series(id)?;
            let tombs: std::sync::Arc<[Interval]> = tomb_r.get(id).into();
            // Skip reading chunk bytes if the chunk is already completely covered by this block's tombstones.
            chks.retain(|c| !tombs.iter().any(|iv| iv.is_subrange(c.min_time, c.max_time)));
            if chks.is_empty() {
                continue;
            }
            for c in &mut chks {
                c.chunk = Some(chk_r.chunk(c.ref_id)?);
            }
            let entry = series_map.entry(labels).or_default();
            for c in chks {
                entry.push((c, std::sync::Arc::clone(&tombs)));
            }
        }
    }

    let mut prepared: Vec<(Labels, Vec<ChunkMeta>)> = Vec::with_capacity(series_map.len());
    for (labels, chks) in series_map {
        let trimmed = trim_and_merge_chunks_per_chunk(chks, mint, maxt_inclusive, float_encoding)?;
        if !trimmed.is_empty() {
            prepared.push((labels, trimmed));
        }
    }
    if prepared.is_empty() {
        return Ok(None);
    }
    // BTreeMap already yields sorted Labels.

    let mut comp = BlockMetaCompaction {
        level: max_level + 1,
        sources,
        deletable: false,
        parents,
        failed: false,
        hints: Vec::new(),
    };
    if any_stale {
        comp.set_stale_series();
    }
    if any_selected {
        comp.set_selected_series();
    }
    if all_ooo {
        comp.set_out_of_order();
    }

    write_prepared_block(dest, mint, maxt, prepared, comp, false, 0)
}

#[derive(Clone)]
struct DirMeta {
    dir: std::path::PathBuf,
    meta: BlockMeta,
}

/// Plans the next on-disk block compaction in `dir` matching Go `LeveledCompactor.Plan`.
pub fn plan_block_compactions(
    dir: &Path,
    ranges_ms: &[i64],
    enable_overlapping: bool,
) -> Result<Option<Vec<std::path::PathBuf>>, CompactError> {
    if ranges_ms.is_empty() {
        return Ok(None);
    }
    let dirs = list_block_dirs(dir)?;
    if dirs.is_empty() {
        return Ok(None);
    }
    let mut dms = Vec::with_capacity(dirs.len());
    for (_, path) in dirs {
        let meta = read_meta_file(&path)?;
        dms.push(DirMeta { dir: path, meta });
    }
    plan_from_metas(dms, ranges_ms, enable_overlapping)
}

fn plan_from_metas(
    dms: Vec<DirMeta>,
    ranges_ms: &[i64],
    enable_overlapping: bool,
) -> Result<Option<Vec<std::path::PathBuf>>, CompactError> {
    if dms.is_empty() {
        return Ok(None);
    }
    let mut stale = Vec::new();
    let mut selected = Vec::new();
    let mut non_hint = Vec::new();
    for dm in dms {
        if dm.meta.compaction.from_stale_series() {
            stale.push(dm);
        } else if dm.meta.compaction.from_selected_series() {
            selected.push(dm);
        } else {
            non_hint.push(dm);
        }
    }
    let classes =
        usize::from(!stale.is_empty()) + usize::from(!selected.is_empty()) + usize::from(!non_hint.is_empty());
    if classes > 1 {
        if let Some(res) = plan_class(non_hint, ranges_ms, enable_overlapping) {
            return Ok(Some(res));
        }
        if let Some(res) = plan_class(stale, ranges_ms, enable_overlapping) {
            return Ok(Some(res));
        }
        return Ok(plan_class(selected, ranges_ms, enable_overlapping));
    }
    let single = if !non_hint.is_empty() {
        non_hint
    } else if !stale.is_empty() {
        stale
    } else {
        selected
    };
    Ok(plan_class(single, ranges_ms, enable_overlapping))
}

fn plan_class(
    mut dms: Vec<DirMeta>,
    ranges_ms: &[i64],
    enable_overlapping: bool,
) -> Option<Vec<std::path::PathBuf>> {
    if dms.is_empty() {
        return None;
    }
    dms.sort_by_key(|dm| dm.meta.min_time);
    if enable_overlapping {
        let overlapping = select_overlapping_dirs(&dms);
        if !overlapping.is_empty() {
            return Some(overlapping);
        }
    }
    // Exclude the most recent block (Go compact.go:398).
    dms.pop();
    if dms.is_empty() {
        return None;
    }
    let selected = select_dirs(&dms, ranges_ms);
    if !selected.is_empty() {
        return Some(selected.into_iter().map(|dm| dm.dir.clone()).collect());
    }
    let mid_range = ranges_ms[ranges_ms.len() / 2];
    for v in dms.iter().rev() {
        let meta = &v.meta;
        if (meta.max_time as i128) - (meta.min_time as i128) < (mid_range as i128) {
            if meta.stats.num_tombstones > 0 && meta.stats.num_tombstones >= meta.stats.num_series {
                return Some(vec![v.dir.clone()]);
            }
            break;
        }
        if (meta.stats.num_tombstones as f64) / ((meta.stats.num_series + 1) as f64) > 0.05 {
            return Some(vec![v.dir.clone()]);
        }
    }
    None
}

fn select_overlapping_dirs(ds: &[DirMeta]) -> Vec<std::path::PathBuf> {
    if ds.len() < 2 {
        return Vec::new();
    }
    let mut overlapping = Vec::new();
    let mut global_maxt = ds[0].meta.max_time;
    for i in 0..ds.len() - 1 {
        let d = &ds[i + 1];
        if d.meta.min_time < global_maxt {
            if overlapping.is_empty() {
                overlapping.push(ds[i].dir.clone());
            }
            overlapping.push(d.dir.clone());
        } else if !overlapping.is_empty() {
            break;
        }
        if d.meta.max_time > global_maxt {
            global_maxt = d.meta.max_time;
        }
    }
    overlapping
}

fn select_dirs<'a>(ds: &'a [DirMeta], ranges_ms: &[i64]) -> Vec<&'a DirMeta> {
    if ranges_ms.len() < 2 || ds.is_empty() {
        return Vec::new();
    }
    let high_time = ds[ds.len() - 1].meta.min_time;
    for &iv in &ranges_ms[1..] {
        if iv <= 0 {
            continue;
        }
        let parts = split_by_range(ds, iv);
        for p in parts {
            if p.iter().any(|dm| dm.meta.compaction.failed) {
                continue;
            }
            let mint = p[0].meta.min_time;
            let maxt = p[p.len() - 1].meta.max_time;
            if ((maxt as i128) - (mint as i128) == (iv as i128) || maxt <= high_time)
                && p.len() > 1
            {
                return p;
            }
        }
    }
    Vec::new()
}

fn split_by_range(ds: &[DirMeta], tr: i64) -> Vec<Vec<&DirMeta>> {
    if tr <= 0 {
        return Vec::new();
    }
    let tr128 = tr as i128;
    let mut split_dirs = Vec::new();
    let mut i = 0usize;
    while i < ds.len() {
        let mut group = Vec::new();
        let m = &ds[i].meta;
        let t0: i128 = if m.min_time >= 0 {
            tr128 * ((m.min_time as i128) / tr128)
        } else {
            tr128 * (((m.min_time as i128) - tr128 + 1) / tr128)
        };
        if (m.max_time as i128) > t0 + tr128 {
            i += 1;
            continue;
        }
        while i < ds.len() {
            if (ds[i].meta.max_time as i128) > t0 + tr128 {
                break;
            }
            group.push(&ds[i]);
            i += 1;
        }
        if !group.is_empty() {
            split_dirs.push(group);
        }
    }
    split_dirs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::{FloatHistogram, FloatHistogramStChunk, Histogram, HistogramSpan, HistogramStChunk, Xor2Chunk};

    #[test]
    fn trim_and_merge_slow_path_preserves_st_encodings() {
        // Overlapping Xor2 chunks force slow path; output must remain Xor2 with ST preserved.
        let mut x1 = Xor2Chunk::new();
        {
            let mut a = x1.appender().unwrap();
            a.append(50, 100, 1.0).unwrap();
            a.append(50, 200, 2.0).unwrap();
        }
        let mut x2 = Xor2Chunk::new();
        {
            let mut a = x2.appender().unwrap();
            a.append(150, 200, 2.0).unwrap();
            a.append(150, 300, 3.0).unwrap();
        }
        let merged = trim_and_merge_chunks(
            vec![
                ChunkMeta {
                    ref_id: 1,
                    min_time: 100,
                    max_time: 200,
                    chunk: Some(EncodedChunk::Xor2(x1)),
                },
                ChunkMeta {
                    ref_id: 2,
                    min_time: 200,
                    max_time: 300,
                    chunk: Some(EncodedChunk::Xor2(x2)),
                },
            ],
            &[],
            100,
            300,
            120,
        )
        .unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor2);
        let decoded = decode_chunk_samples(merged[0].chunk.as_ref().unwrap()).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].st, 50);
        assert_eq!(decoded[2].st, 150);

        // Overlapping HistogramSt chunks preserve HistogramSt encoding.
        let h = Histogram {
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1,
            count: 2,
            sum: 3.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1],
            ..Histogram::default()
        };
        let mut hs1 = HistogramStChunk::new();
        {
            let mut a = hs1.appender().unwrap();
            a.append_histogram(None, 40, 100, &h, false).unwrap();
        }
        let mut hs2 = HistogramStChunk::new();
        {
            let mut a = hs2.appender().unwrap();
            a.append_histogram(None, 40, 100, &h, false).unwrap();
            a.append_histogram(None, 40, 200, &h, false).unwrap();
        }
        let h_merged = trim_and_merge_chunks(
            vec![
                ChunkMeta {
                    ref_id: 1,
                    min_time: 100,
                    max_time: 100,
                    chunk: Some(EncodedChunk::HistogramSt(hs1)),
                },
                ChunkMeta {
                    ref_id: 2,
                    min_time: 100,
                    max_time: 200,
                    chunk: Some(EncodedChunk::HistogramSt(hs2)),
                },
            ],
            &[],
            100,
            200,
            120,
        )
        .unwrap();
        assert_eq!(
            h_merged[0].chunk.as_ref().unwrap().encoding(),
            Encoding::HistogramSt
        );

        // Overlapping FloatHistogramSt chunks preserve FloatHistogramSt encoding.
        let fh = FloatHistogram {
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1.0,
            count: 2.0,
            sum: 3.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1.0],
            ..FloatHistogram::default()
        };
        let mut fhs1 = FloatHistogramStChunk::new();
        {
            let mut a = fhs1.appender().unwrap();
            a.append_float_histogram(None, 77, 100, &fh, false).unwrap();
        }
        let mut fhs2 = FloatHistogramStChunk::new();
        {
            let mut a = fhs2.appender().unwrap();
            a.append_float_histogram(None, 77, 100, &fh, false).unwrap();
            a.append_float_histogram(None, 77, 200, &fh, false).unwrap();
        }
        let fh_merged = trim_and_merge_chunks(
            vec![
                ChunkMeta {
                    ref_id: 1,
                    min_time: 100,
                    max_time: 100,
                    chunk: Some(EncodedChunk::FloatHistogramSt(fhs1)),
                },
                ChunkMeta {
                    ref_id: 2,
                    min_time: 100,
                    max_time: 200,
                    chunk: Some(EncodedChunk::FloatHistogramSt(fhs2)),
                },
            ],
            &[],
            100,
            200,
            120,
        )
        .unwrap();
        assert_eq!(
            fh_merged[0].chunk.as_ref().unwrap().encoding(),
            Encoding::FloatHistogramSt
        );
    }

    #[test]
    fn reencode_chunk_without_tombstones_matches_go_counter_reset_header_and_stale_safety() {
        use crate::chunkenc::{stale_nan, CounterResetHeader, CounterResetHint, HistogramSpan};
        use crate::tombstones::Interval;

        let make_hist = |sum: f64, cnt: u64, hint: CounterResetHint| Histogram {
            counter_reset_hint: hint,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1,
            count: cnt,
            sum,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1],
            ..Histogram::default()
        };
        let make_fhist = |sum: f64, cnt: f64, hint: CounterResetHint| FloatHistogram {
            counter_reset_hint: hint,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 1.0,
            count: cnt,
            sum,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1.0],
            ..FloatHistogram::default()
        };

        // Matches Go Prometheus `populateCurrForSingleChunk`: GaugeType preserves GaugeType,
        // while counter histograms emit UnknownCounterReset when re-encoded via DelIter.
        for orig_crh in [
            CounterResetHeader::NotCounterReset,
            CounterResetHeader::CounterReset,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHeader::GaugeType,
        ] {
            let hint = if orig_crh == CounterResetHeader::GaugeType {
                CounterResetHint::GaugeType
            } else {
                CounterResetHint::NotCounterReset
            };
            let h1 = make_hist(10.0, 2, hint);
            let h2 = make_hist(20.0, 4, hint);
            let fh1 = make_fhist(10.0, 2.0, hint);
            let fh2 = make_fhist(20.0, 4.0, hint);

            let mut hc = HistogramChunk::new();
            {
                let mut a = hc.appender().unwrap();
                a.append_histogram(None, 0, 100, &h1, false).unwrap();
                a.append_histogram(None, 0, 200, &h2, false).unwrap();
                a.set_counter_reset_header(orig_crh);
            }
            let mut hsc = HistogramStChunk::new();
            {
                let mut a = hsc.appender().unwrap();
                a.append_histogram(None, 50, 100, &h1, false).unwrap();
                a.append_histogram(None, 50, 200, &h2, false).unwrap();
                a.set_counter_reset_header(orig_crh);
            }
            let mut fhc = FloatHistogramChunk::new();
            {
                let mut a = fhc.appender().unwrap();
                a.append_float_histogram(None, 0, 100, &fh1, false).unwrap();
                a.append_float_histogram(None, 0, 200, &fh2, false).unwrap();
                a.set_counter_reset_header(orig_crh);
            }
            let mut fhsc = FloatHistogramStChunk::new();
            {
                let mut a = fhsc.appender().unwrap();
                a.append_float_histogram(None, 50, 100, &fh1, false).unwrap();
                a.append_float_histogram(None, 50, 200, &fh2, false).unwrap();
                a.set_counter_reset_header(orig_crh);
            }

            let expected = if orig_crh == CounterResetHeader::GaugeType {
                CounterResetHeader::GaugeType
            } else {
                CounterResetHeader::UnknownCounterReset
            };

            for (enc, label) in [
                (EncodedChunk::Histogram(hc), "Histogram"),
                (EncodedChunk::HistogramSt(hsc), "HistogramSt"),
                (EncodedChunk::FloatHistogram(fhc), "FloatHistogram"),
                (EncodedChunk::FloatHistogramSt(fhsc), "FloatHistogramSt"),
            ] {
                for del_t in [100i64, 200i64] {
                    let out = reencode_chunk_without_tombstones(
                        ChunkMeta {
                            ref_id: 1,
                            min_time: 100,
                            max_time: 200,
                            chunk: Some(enc.clone()),
                        },
                        &[Interval { mint: del_t, maxt: del_t }],
                    )
                    .unwrap()
                    .unwrap();
                    let got = match out.chunk.unwrap() {
                        EncodedChunk::Histogram(c) => c.counter_reset_header(),
                        EncodedChunk::HistogramSt(c) => c.counter_reset_header(),
                        EncodedChunk::FloatHistogram(c) => c.counter_reset_header(),
                        EncodedChunk::FloatHistogramSt(c) => c.counter_reset_header(),
                        _ => unreachable!(),
                    };
                    assert_eq!(got, expected, "{label} {orig_crh:?} del_t={del_t}");
                }
            }
        }

        // 2) Corrupt chunk with StaleNaN followed by a non-stale sample must return Err, not panic.
        let mut corrupt_hc = HistogramChunk::new();
        {
            let mut a = corrupt_hc.appender().unwrap();
            let stale_h = Histogram {
                sum: stale_nan(),
                ..Histogram::default()
            };
            a.append_histogram(None, 0, 100, &stale_h, false).unwrap();
            a.append_histogram(None, 0, 200, &stale_h, false).unwrap();
        }
        // Build a 3-sample chunk with a StaleNaN at t=200 after a normal sample at t=100,
        // and also test a corrupt BitStream chunk with StaleNaN at sample 0 + normal sample at sample 1.
        let mut bs = crate::chunkenc::bstream::BitStream::new();
        bs.write_byte(0);
        bs.write_byte(2); // num_samples = 2
        bs.write_byte(0); // flags = UnknownCounterReset
        bs.write_byte(0); // zero_threshold = 0
        crate::chunkenc::varbit::put_varbit_int(&mut bs, 0); // schema = 0
        crate::chunkenc::varbit::put_varbit_uint(&mut bs, 0); // 0 pos spans
        crate::chunkenc::varbit::put_varbit_uint(&mut bs, 0); // 0 neg spans
        crate::chunkenc::varbit::put_varbit_int(&mut bs, 100); // t0 = 100
        crate::chunkenc::varbit::put_varbit_uint(&mut bs, 0); // cnt = 0
        crate::chunkenc::varbit::put_varbit_uint(&mut bs, 0); // z_cnt = 0
        bs.write_bits(stale_nan().to_bits(), 64); // sample 0 sum = StaleNaN
        // sample 1: t_dod=100, cnt_dod=1, z_cnt_dod=0, xor sum -> 5.0
        crate::chunkenc::varbit::put_varbit_int(&mut bs, 100);
        crate::chunkenc::varbit::put_varbit_int(&mut bs, 1);
        crate::chunkenc::varbit::put_varbit_int(&mut bs, 0);
        bs.write_bit(true);
        bs.write_bit(true);
        bs.write_bits(0, 5);
        bs.write_bits(0, 6); // 64 bits
        bs.write_bits(stale_nan().to_bits() ^ 5.0f64.to_bits(), 64);
        let corrupt_stale_then_normal = HistogramChunk::from_bytes(bs.into_bytes());
        let res_err = reencode_chunk_without_tombstones(
            ChunkMeta {
                ref_id: 1,
                min_time: 100,
                max_time: 200,
                chunk: Some(EncodedChunk::Histogram(corrupt_stale_then_normal)),
            },
            &[Interval { mint: 150, maxt: 160 }],
        );
        assert!(matches!(res_err, Err(ChunkError::HistogramSchemaChange)), "got {res_err:?}");

        let res = reencode_chunk_without_tombstones(
            ChunkMeta {
                ref_id: 1,
                min_time: 100,
                max_time: 200,
                chunk: Some(EncodedChunk::Histogram(corrupt_hc)),
            },
            &[Interval { mint: 200, maxt: 200 }],
        );
        assert!(res.is_ok());
    }

    #[test]
    fn split_by_range_extreme_i64_no_overflow() {
        let make_dm = |min_time: i64, max_time: i64| DirMeta {
            dir: std::path::PathBuf::from("dummy"),
            meta: BlockMeta {
                ulid: Ulid::new(),
                min_time,
                max_time,
                stats: BlockStats::default(),
                compaction: BlockMetaCompaction::default(),
                version: META_VERSION_1,
                out_of_order: false,
                thanos: None,
            },
        };
        let ds = vec![
            make_dm(i64::MIN + 100, i64::MIN + 200),
            make_dm(i64::MAX - 200, i64::MAX - 100),
        ];
        let parts = split_by_range(&ds, 7_200_000);
        assert_eq!(parts.len(), 2);
        assert!(split_by_range(&ds, 0).is_empty());
    }
}
