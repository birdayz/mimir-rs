//! Prometheus TSDB ULID block compaction from Head/RangeHead and on-disk Blocks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/compact.go
//! and github.com/prometheus/prometheus/tsdb/async_block_writer.go.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;

use thiserror::Error;
use ulid::Ulid;

use crate::block::{
    write_meta_file, Block, BlockDesc, BlockError, BlockMeta, BlockMetaCompaction, BlockStats,
    META_VERSION_1,
};
use crate::chunkenc::{
    ChunkError, EncodedChunk, Encoding, FloatHistogram, FloatHistogramChunk, Histogram,
    HistogramChunk, XorChunk, MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND, TARGET_BYTES_PER_HISTOGRAM_CHUNK,
};
use crate::chunkenc::float_histogram::AppendFloatHistogramOutcome;
use crate::chunkenc::histogram::AppendHistogramOutcome;
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
    Histogram(Histogram),
    FloatHistogram(FloatHistogram),
}

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
                    value: SampleValue::Histogram(h),
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
                    value: SampleValue::Histogram(h),
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
                    value: SampleValue::FloatHistogram(fh),
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
                    value: SampleValue::FloatHistogram(fh),
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
    if samples.is_empty() {
        return Ok(Vec::new());
    }
    let samples_per_chunk = samples_per_chunk.max(1);
    let mut out = Vec::new();

    enum Active {
        None,
        Xor {
            mint: i64,
            maxt: i64,
            chunk: XorChunk,
        },
        Hist {
            mint: i64,
            maxt: i64,
            chunk: HistogramChunk,
            prev_state: Option<crate::chunkenc::histogram::HistogramAppenderState>,
        },
        FloatHist {
            mint: i64,
            maxt: i64,
            chunk: FloatHistogramChunk,
            prev_state: Option<crate::chunkenc::float_histogram::FloatHistogramAppenderState>,
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
            } => {
                chunk.compact();
                out.push(ChunkMeta {
                    ref_id: 0,
                    min_time: mint,
                    max_time: maxt,
                    chunk: Some(EncodedChunk::Xor(chunk)),
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
        }
    };

    for s in samples {
        match &s.value {
            SampleValue::Float(v) => {
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
                    };
                }
                if let Active::Xor { maxt, chunk, .. } = &mut active {
                    let mut app = chunk.appender()?;
                    app.append(s.st, s.t, *v)?;
                    *maxt = s.t;
                }
            }
            SampleValue::Histogram(h) => {
                let need_cut = match &active {
                    Active::Hist { chunk, .. } => {
                        chunk.as_bytes().len() >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2
                    }
                    _ => true,
                };
                if need_cut {
                    flush_active(&mut active, &mut out);
                    active = Active::Hist {
                        mint: s.t,
                        maxt: s.t,
                        chunk: HistogramChunk::new(),
                        prev_state: None,
                    };
                }
                if let Active::Hist {
                    mint,
                    maxt,
                    chunk,
                    prev_state,
                } = &mut active
                {
                    let mut app = chunk.appender()?;
                    let outcome =
                        app.append_histogram(prev_state.as_ref(), s.st, s.t, h, false)?;
                    let next_prev = app.state().clone();
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
                            *prev_state = Some(next_prev);
                        }
                    }
                }
            }
            SampleValue::FloatHistogram(fh) => {
                let need_cut = match &active {
                    Active::FloatHist { chunk, .. } => {
                        chunk.as_bytes().len() >= TARGET_BYTES_PER_HISTOGRAM_CHUNK * 2
                    }
                    _ => true,
                };
                if need_cut {
                    flush_active(&mut active, &mut out);
                    active = Active::FloatHist {
                        mint: s.t,
                        maxt: s.t,
                        chunk: FloatHistogramChunk::new(),
                        prev_state: None,
                    };
                }
                if let Active::FloatHist {
                    mint,
                    maxt,
                    chunk,
                    prev_state,
                } = &mut active
                {
                    let mut app = chunk.appender()?;
                    let outcome =
                        app.append_float_histogram(prev_state.as_ref(), s.st, s.t, fh, false)?;
                    let next_prev = app.state().clone();
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
                            *prev_state = Some(next_prev);
                        }
                    }
                }
            }
        }
    }

    flush_active(&mut active, &mut out);
    Ok(out)
}

/// Input series fed into [`write_block_from_series`].
#[derive(Clone, Debug)]
pub struct CompactionSeries {
    pub labels: Labels,
    pub chunks: Vec<ChunkMeta>,
    pub tombstones: Vec<Interval>,
}

/// Trims/merges `chunks` to closed `[mint, maxt_inclusive]` excluding `tombstones`.
pub fn trim_and_merge_chunks(
    mut chunks: Vec<ChunkMeta>,
    tombstones: &[Interval],
    mint: i64,
    maxt_inclusive: i64,
    samples_per_chunk: usize,
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

    chunks.sort_by_key(|c| (c.min_time, c.max_time));

    let any_overlap = chunks.windows(2).any(|w| w[1].min_time <= w[0].max_time);
    let any_partial = chunks.iter().any(|c| {
        c.min_time < mint
            || c.max_time > maxt_inclusive
            || tombstones.iter().any(|iv| iv.overlaps(c.min_time, c.max_time))
    });

    if !any_overlap && !any_partial {
        for c in &mut chunks {
            if let Some(chk) = c.chunk.as_mut() {
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
        return Ok(chunks);
    }

    // Slow path: decode samples, filter [mint, maxt_inclusive] & tombstones, sort & dedup by timestamp, re-encode.
    let mut samples = Vec::new();
    for c in &chunks {
        let Some(ref encoded) = c.chunk else {
            continue;
        };
        for s in decode_chunk_samples(encoded)? {
            if s.t < mint || s.t > maxt_inclusive {
                continue;
            }
            if tombstones.iter().any(|iv| iv.in_bounds(s.t)) {
                continue;
            }
            samples.push(s);
        }
    }
    if samples.is_empty() {
        return Ok(Vec::new());
    }

    samples.sort_by_key(|s| s.t);
    samples.dedup_by_key(|s| s.t);
    encode_samples_into_chunks(&samples, samples_per_chunk)
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
    let mut symbols: BTreeSet<String> = BTreeSet::new();
    // Always include empty symbol matching Prometheus Head/Compactor convention.
    symbols.insert(String::new());

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
        for (k, v) in s.labels.iter() {
            symbols.insert(k.to_string());
            symbols.insert(v.to_string());
        }
        prepared.push((std::mem::take(&mut s.labels), trimmed));
    }

    if prepared.is_empty() {
        return Ok(None);
    }

    prepared.sort_by(|a, b| a.0.cmp(&b.0));

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

        for sym in &symbols {
            indexw.add_symbol(sym)?;
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
        fs::rename(&tmp_dir, &final_dir)?;
        Ok(Some(meta))
    })();

    if write_res.is_err() || matches!(write_res, Ok(None)) {
        let _ = fs::remove_dir_all(&tmp_dir);
    }
    write_res
}

/// Compacts multiple existing on-disk [`Block`]s into a single merged ULID block directory.
pub fn compact_blocks(dest: &Path, blocks: &[&Block]) -> Result<Option<BlockMeta>, CompactError> {
    if blocks.is_empty() {
        return Ok(None);
    }

    let mut mint = i64::MAX;
    let mut maxt = i64::MIN;
    let mut max_level = 0i32;
    let mut sources = Vec::new();
    let mut parents = Vec::with_capacity(blocks.len());
    let mut all_ooo = true;

    for b in blocks {
        let m = b.meta();
        mint = mint.min(m.min_time);
        maxt = maxt.max(m.max_time);
        max_level = max_level.max(m.compaction.level);
        sources.extend_from_slice(&m.compaction.sources);
        if !m.compaction.from_out_of_order() {
            all_ooo = false;
        }
        parents.push(BlockDesc {
            ulid: m.ulid,
            min_time: m.min_time,
            max_time: m.max_time,
        });
    }

    // Collect all series grouped by Labels across all input blocks.
    let mut series_map: std::collections::BTreeMap<Labels, (Vec<ChunkMeta>, Vec<Interval>)> =
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
            for c in &mut chks {
                c.chunk = Some(chk_r.chunk(c.ref_id)?);
            }
            let tombs = tomb_r.get(id);
            let entry = series_map.entry(labels).or_default();
            entry.0.extend(chks);
            entry.1.extend(tombs);
        }
    }

    let comp_series: Vec<CompactionSeries> = series_map
        .into_iter()
        .map(|(labels, (chunks, tombstones))| CompactionSeries {
            labels,
            chunks,
            tombstones,
        })
        .collect();

    let mut comp = BlockMetaCompaction {
        level: max_level + 1,
        sources,
        deletable: false,
        parents,
        failed: false,
        hints: Vec::new(),
    };
    if all_ooo {
        comp.set_out_of_order();
    }

    write_block_from_series(dest, mint, maxt, comp_series, comp, all_ooo, 0)
}
