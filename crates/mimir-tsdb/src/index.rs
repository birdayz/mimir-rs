//! Bit-exact Prometheus TSDB Index v2 writer & memory-mapped v1/v2/v3 reader.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/index/index.go.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use std::ops::Deref;

use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use parking_lot::RwLock;
use thiserror::Error;

use crate::chunks_block::ChunkMeta;
use crate::encoding::{Decbuf, Encbuf, EncodingError};
use crate::labels::{encode_stringlabel_size, Labels};
use crate::matcher::{LabelMatcher, MatchType};
use crate::postings::{
    contains_all, intersect_any, merge_iters, merge_postings, BigEndianPostings, ContextCheck,
    EmptyPostingsIter, ErrPostingsIter, ListPostings, PostingsCloner, PostingsError, PostingsIter,
    PostingsSource, CHECK_CONTEXT_EVERY_N_ITERATIONS,
};

pub const MAGIC_INDEX: u32 = 0xBAAA_D700;
pub const INDEX_HEADER_LEN: usize = 5;
pub const INDEX_FORMAT_V1: u8 = 1;
pub const INDEX_FORMAT_V2: u8 = 2;
pub const INDEX_FORMAT_V3: u8 = 3;
pub const INDEX_FILENAME: &str = "index";

pub const SERIES_BYTE_ALIGN: usize = 16;
const SYMBOL_FACTOR: usize = 32;
const INDEX_TOC_LEN: usize = 6 * 8 + 4;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("encoding error: {0}")]
    Encoding(#[from] EncodingError),
    #[error("invalid index magic number {0:#x}")]
    InvalidMagic(u32),
    #[error("unsupported index version {0}")]
    UnsupportedVersion(u8),
    #[error("invalid index stage: {0}")]
    InvalidStage(&'static str),
    #[error("out-of-order symbol {0:?}")]
    OutOfOrderSymbol(String),
    #[error("unknown symbol {0:?}")]
    UnknownSymbol(String),
    #[error("unknown symbol offset {0}")]
    UnknownSymbolOffset(u32),
    #[error("out-of-order series {0}")]
    OutOfOrderSeries(String),
    #[error("unsorted chunk metadata: {0}")]
    InvalidChunkOrder(String),
    #[error("index checksum mismatch (expected {expected:#x}, actual {actual:#x})")]
    ChecksumMismatch { expected: u32, actual: u32 },
    #[error("series {0} not found in index")]
    SeriesNotFound(u64),
    #[error("corrupt index: {0}")]
    Corrupt(String),
    #[error("symbol table size {0} exceeds 4 GiB limit")]
    SymbolTableTooLarge(u64),
    #[error("index file size {0} exceeds max size of 64 GiB")]
    IndexExceeds64GiB(u64),
    #[error("postings offset table length {0} exceeds 4 bytes")]
    PostingsOffsetTableTooLarge(u64),
    #[error("posting size {0} exceeds 4 bytes")]
    PostingSizeTooLarge(usize),
    #[error("context canceled: {0}")]
    ContextCanceled(String),
}

/// 6-entry Table of Contents at the end of a Prometheus index file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexToc {
    pub symbols: u64,
    pub series: u64,
    pub label_indices: u64,
    pub label_indices_table: u64,
    pub postings: u64,
    pub postings_table: u64,
}

impl IndexToc {
    pub fn parse(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.len() < INDEX_TOC_LEN {
            return Err(IndexError::Encoding(EncodingError::InvalidSize));
        }
        let toc_slice = &bytes[bytes.len() - INDEX_TOC_LEN..];
        let payload = &toc_slice[..48];
        let exp_crc = u32::from_be_bytes(toc_slice[48..52].try_into().unwrap());
        let actual_crc = crc32c::crc32c(payload);
        if actual_crc != exp_crc {
            return Err(IndexError::ChecksumMismatch {
                expected: exp_crc,
                actual: actual_crc,
            });
        }
        let mut dec = Decbuf::new(payload);
        Ok(Self {
            symbols: dec.be64()?,
            series: dec.be64()?,
            label_indices: dec.be64()?,
            label_indices_table: dec.be64()?,
            postings: dec.be64()?,
            postings_table: dec.be64()?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum WriterStage {
    None,
    Symbols,
    Series,
    Done,
}

#[inline]
fn fast_hash_bytes(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

#[inline]
fn symbol_bytes<'a>(arena: &'a [u8], offsets: &[usize], idx: u32) -> &'a [u8] {
    let i = idx as usize;
    &arena[offsets[i]..offsets[i + 1]]
}

#[inline]
fn symbol_str<'a>(arena: &'a [u8], offsets: &[usize], idx: u32) -> &'a str {
    let raw = symbol_bytes(arena, offsets, idx);
    std::str::from_utf8(raw).unwrap_or("")
}

fn write_main_slice(
    f_opt: &mut Option<BufWriter<File>>,
    pos: &mut u64,
    bytes: &[u8],
) -> Result<(), IndexError> {
    let f = f_opt
        .as_mut()
        .ok_or(IndexError::InvalidStage("writer closed"))?;
    f.write_all(bytes)?;
    *pos += bytes.len() as u64;
    if *pos > 16 * u64::from(u32::MAX) {
        return Err(IndexError::IndexExceeds64GiB(*pos));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_posting_entry(
    fp_opt: &mut Option<BufWriter<File>>,
    fp_pos: &mut u64,
    fpo_opt: &mut Option<BufWriter<File>>,
    fpo_pos: &mut u64,
    cnt_po: &mut u32,
    buf1: &mut Encbuf,
    name: &str,
    value: &str,
    offs: &[u32],
) -> Result<(), IndexError> {
    let fp = fp_opt
        .as_mut()
        .ok_or(IndexError::InvalidStage("postings writer closed"))?;
    // Align postings entry to 4 bytes.
    let rem = (*fp_pos as usize) % 4;
    if rem != 0 {
        let pad = 4 - rem;
        let zeros = [0u8; 4];
        fp.write_all(&zeros[..pad])?;
        *fp_pos += pad as u64;
    }

    // Write offset table entry to temporary fpo.
    buf1.reset();
    buf1.put_uvarint(2);
    buf1.put_uvarint_str(name);
    buf1.put_uvarint_str(value);
    buf1.put_uvarint64(*fp_pos);
    {
        let fpo = fpo_opt
            .as_mut()
            .ok_or(IndexError::InvalidStage("postings offset writer closed"))?;
        fpo.write_all(buf1.as_slice())?;
        *fpo_pos += buf1.len() as u64;
    }
    *cnt_po += 1;

    // Encode raw postings: <4B BE len><4B BE count><count * 4B BE off><4B BE crc32c>
    let count_u32 =
        u32::try_from(offs.len()).map_err(|_| IndexError::PostingSizeTooLarge(offs.len()))?;
    buf1.reset();
    buf1.put_be32(count_u32);
    for &off in offs {
        buf1.put_be32(off);
    }
    let body_len =
        u32::try_from(buf1.len()).map_err(|_| IndexError::PostingSizeTooLarge(buf1.len()))?;
    buf1.put_crc32c();

    fp.write_all(&body_len.to_be_bytes())?;
    fp.write_all(buf1.as_slice())?;
    *fp_pos += 4 + buf1.len() as u64;
    Ok(())
}

/// Bit-exact Prometheus Index v2 writer (`index.Writer`).
pub struct IndexWriter {
    path: PathBuf,
    tmp_p_path: PathBuf,
    tmp_po_path: PathBuf,

    f: Option<BufWriter<File>>,
    pos: u64,

    fp: Option<BufWriter<File>>,
    fp_pos: u64,

    fpo: Option<BufWriter<File>>,
    fpo_pos: u64,
    cnt_po: u32,

    toc: IndexToc,
    stage: WriterStage,
    postings_start: u64,

    num_symbols: u32,
    symbols_body_crc: u32,
    symbols_body_len: usize,
    /// Flat contiguous UTF-8 byte arena for all symbols (zero per-symbol `String` allocs).
    symbols_arena: Vec<u8>,
    /// `symbol_offsets.len() == num_symbols + 1`; symbol `i` is `symbols_arena[off[i]..off[i+1]]`.
    symbol_offsets: Vec<usize>,
    /// Fast raw hash table mapping symbol bytes -> symbol `u32` ID with zero duplicate `String`s.
    symbol_table: hashbrown::HashTable<u32>,

    /// Counts of series per label name symbol ID: `(name_sid, count)` indexed by dense slot.
    label_name_counts: Vec<(u32, u64)>,
    /// Maps `name_sid -> slot` in `label_name_counts`.
    label_name_slots: hashbrown::HashMap<u32, usize>,
    /// Adjacent-series label cache referencing `last_series.as_bytes()` slices:
    /// `(name_start, name_end, name_idx, count_slot, val_start, val_end, val_idx)`.
    /// Exploits lexicographical series ordering to skip ~85% of symbol hash lookups in `add_series`.
    prev_label_cache: Vec<(usize, usize, u32, usize, usize, usize, u32)>,
    /// Reusable scratch staging buffer for `add_series` so a mid-loop `UnknownSymbol` or I/O
    /// error never leaves `prev_label_cache` or `label_name_counts` partially mutated.
    next_label_cache: Vec<(usize, usize, u32, usize, usize, usize, u32)>,
    /// Set if `close_inner` fails so `Drop` does not retry writing partial postings.
    close_failed: bool,
    /// All series posting IDs (`series_pos / 16`) collected during `add_series` (eliminates pass-0 mmap scan).
    all_series_ids: Vec<u32>,
    last_series: Option<Labels>,
    last_series_ref: u64,
    last_chunk_ref: u64,

    buf1: Encbuf,
    buf2: Encbuf,
}

impl IndexWriter {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, IndexError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let _ = fs::remove_file(&path);
        let tmp_p_path = PathBuf::from(format!("{}_tmp_p", path.display()));
        let tmp_po_path = PathBuf::from(format!("{}_tmp_po", path.display()));
        let _ = fs::remove_file(&tmp_p_path);
        let _ = fs::remove_file(&tmp_po_path);

        let f_raw = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let fp_raw = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&tmp_p_path)?;
        let fpo_raw = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&tmp_po_path)?;

        let mut w = Self {
            path,
            tmp_p_path,
            tmp_po_path,
            f: Some(BufWriter::with_capacity(1 << 20, f_raw)),
            pos: 0,
            fp: Some(BufWriter::with_capacity(1 << 20, fp_raw)),
            fp_pos: 0,
            fpo: Some(BufWriter::with_capacity(1 << 20, fpo_raw)),
            fpo_pos: 0,
            cnt_po: 0,
            toc: IndexToc::default(),
            stage: WriterStage::None,
            postings_start: 0,
            num_symbols: 0,
            symbols_body_crc: 0,
            symbols_body_len: 0,
            symbols_arena: Vec::with_capacity(4096),
            symbol_offsets: vec![0],
            symbol_table: hashbrown::HashTable::with_capacity(256),
            label_name_counts: Vec::with_capacity(32),
            label_name_slots: hashbrown::HashMap::with_capacity(32),
            prev_label_cache: Vec::with_capacity(16),
            next_label_cache: Vec::with_capacity(16),
            close_failed: false,
            all_series_ids: Vec::new(),
            last_series: None,
            last_series_ref: 0,
            last_chunk_ref: 0,
            buf1: Encbuf::with_capacity(4096),
            buf2: Encbuf::with_capacity(4096),
        };
        w.write_meta()?;
        Ok(w)
    }

    fn write_main(&mut self, bytes: &[u8]) -> Result<(), IndexError> {
        let f = self
            .f
            .as_mut()
            .ok_or(IndexError::InvalidStage("writer closed"))?;
        f.write_all(bytes)?;
        self.pos += bytes.len() as u64;
        if self.pos > 16 * u64::from(u32::MAX) {
            return Err(IndexError::IndexExceeds64GiB(self.pos));
        }
        Ok(())
    }

    fn write_main_at(&mut self, bytes: &[u8], pos: u64) -> Result<(), IndexError> {
        let f = self
            .f
            .as_mut()
            .ok_or(IndexError::InvalidStage("writer closed"))?;
        f.flush()?;
        let inner = f.get_mut();
        inner.seek(SeekFrom::Start(pos))?;
        inner.write_all(bytes)?;
        inner.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn add_padding(&mut self, align: usize) -> Result<(), IndexError> {
        let rem = (self.pos as usize) % align;
        if rem == 0 {
            return Ok(());
        }
        let pad = align - rem;
        let zeros = [0u8; 16];
        debug_assert!(pad <= zeros.len());
        self.write_main(&zeros[..pad])
    }

    fn write_meta(&mut self) -> Result<(), IndexError> {
        self.buf1.reset();
        self.buf1.put_be32(MAGIC_INDEX);
        self.buf1.put_byte(INDEX_FORMAT_V2);
        write_main_slice(&mut self.f, &mut self.pos, self.buf1.as_slice())
    }

    fn ensure_stage(&mut self, target: WriterStage) -> Result<(), IndexError> {
        if self.stage == target {
            return Ok(());
        }
        if self.stage > target {
            return Err(IndexError::InvalidStage("cannot move backwards in stages"));
        }
        if (target as u8) > (self.stage as u8) + 1 {
            let prev = match target {
                WriterStage::Series => WriterStage::Symbols,
                WriterStage::Done => WriterStage::Series,
                _ => WriterStage::None,
            };
            self.ensure_stage(prev)?;
        }

        match target {
            WriterStage::Symbols => {
                self.toc.symbols = self.pos;
                // Reserve 8 bytes: 4B BE symbol table size + 4B BE num_symbols.
                self.write_main(b"alenblen")?;
            }
            WriterStage::Series => {
                self.finish_symbols()?;
                self.toc.series = self.pos;
            }
            WriterStage::Done => {
                self.last_series = None;
                self.toc.label_indices = self.pos;
                self.write_postings_to_tmp_files()?;
                self.symbols_arena = Vec::new();
                self.symbol_offsets = Vec::new();
                self.symbol_table = hashbrown::HashTable::new();
                self.label_name_counts = Vec::new();
                self.label_name_slots = hashbrown::HashMap::new();
                self.prev_label_cache = Vec::new();
                self.next_label_cache = Vec::new();
                self.all_series_ids = Vec::new();
                self.toc.postings = self.pos;
                self.write_postings()?;
                self.toc.label_indices_table = self.pos;
                self.toc.postings_table = self.pos;
                self.write_postings_offset_table()?;
                self.write_toc()?;
            }
            WriterStage::None => {}
        }
        self.stage = target;
        Ok(())
    }

    /// Registers a symbol in strictly ascending lexicographical order.
    pub fn add_symbol(&mut self, sym: &str) -> Result<(), IndexError> {
        self.ensure_stage(WriterStage::Symbols)?;
        if self.num_symbols != 0 {
            let last_bytes = symbol_bytes(&self.symbols_arena, &self.symbol_offsets, self.num_symbols - 1);
            if sym.as_bytes() <= last_bytes {
                return Err(IndexError::OutOfOrderSymbol(sym.to_string()));
            }
        }
        let idx = self.num_symbols;
        let sym_bytes = sym.as_bytes();
        self.symbols_arena.extend_from_slice(sym_bytes);
        self.symbol_offsets.push(self.symbols_arena.len());
        let h = fast_hash_bytes(sym_bytes);
        let arena = &self.symbols_arena;
        let offsets = &self.symbol_offsets;
        self.symbol_table.insert_unique(h, idx, |&existing_id| {
            fast_hash_bytes(symbol_bytes(arena, offsets, existing_id))
        });
        self.num_symbols += 1;

        self.buf1.reset();
        self.buf1.put_uvarint_str(sym);
        let slice = self.buf1.as_slice();
        self.symbols_body_crc = crc32c::crc32c_append(self.symbols_body_crc, slice);
        self.symbols_body_len += slice.len();
        write_main_slice(&mut self.f, &mut self.pos, slice)
    }

    #[inline]
    fn lookup_symbol(&self, sym: &str) -> Option<u32> {
        let target = sym.as_bytes();
        let h = fast_hash_bytes(target);
        let arena = &self.symbols_arena;
        let offsets = &self.symbol_offsets;
        self.symbol_table
            .find(h, |&id| symbol_bytes(arena, offsets, id) == target)
            .copied()
    }

    fn finish_symbols(&mut self) -> Result<(), IndexError> {
        let sym_table_len = self.pos - self.toc.symbols - 4;
        let sym_table_size = u32::try_from(sym_table_len)
            .map_err(|_| IndexError::SymbolTableTooLarge(sym_table_len))?;
        let mut hdr = [0u8; 8];
        hdr[..4].copy_from_slice(&sym_table_size.to_be_bytes());
        hdr[4..8].copy_from_slice(&self.num_symbols.to_be_bytes());
        self.write_main_at(&hdr, self.toc.symbols)?;

        // Compute CRC32C over [toc.symbols + 4 .. self.pos] (4B numSymbols + all uvarintStr symbols)
        // by combining the 4-byte header CRC with the incrementally accumulated body CRC (zero disk read-back).
        let hdr_crc = crc32c::crc32c(&hdr[4..8]);
        let crc = crc32c::crc32c_combine(hdr_crc, self.symbols_body_crc, self.symbols_body_len);
        self.write_main(&crc.to_be_bytes())
    }

    /// Adds a series with its chunk metadata. Must be called in strictly ascending label order!
    pub fn add_series(
        &mut self,
        ref_id: u64,
        labels: &Labels,
        chunks: &[ChunkMeta],
    ) -> Result<(), IndexError> {
        self.ensure_stage(WriterStage::Series)?;
        if labels.is_empty() {
            return Err(IndexError::OutOfOrderSeries(labels.to_string()));
        }
        if let Some(ref prev) = self.last_series {
            if labels <= prev {
                return Err(IndexError::OutOfOrderSeries(labels.to_string()));
            }
            if ref_id < self.last_series_ref {
                return Err(IndexError::InvalidChunkOrder(format!(
                    "series with reference greater than {ref_id} already added (previous {})",
                    self.last_series_ref
                )));
            }
        }

        let mut last_chunk_ref = self.last_chunk_ref;
        let mut last_maxt = 0i64;
        for (ix, c) in chunks.iter().enumerate() {
            if c.ref_id < last_chunk_ref {
                return Err(IndexError::InvalidChunkOrder(format!(
                    "unsorted chunk ref {} < {}",
                    c.ref_id, last_chunk_ref
                )));
            }
            last_chunk_ref = c.ref_id;
            if ix > 0 && c.min_time <= last_maxt {
                return Err(IndexError::InvalidChunkOrder(format!(
                    "chunk min_time {} <= previous max_time {}",
                    c.min_time, last_maxt
                )));
            }
            if c.max_time < c.min_time {
                return Err(IndexError::InvalidChunkOrder(format!(
                    "chunk max_time {} < min_time {}",
                    c.max_time, c.min_time
                )));
            }
            last_maxt = c.max_time;
        }

        // Resolve all label symbol IDs into `self.next_label_cache` BEFORE mutating
        // `self.prev_label_cache`, `self.label_name_counts`, `self.all_series_ids`, or
        // writing any padding/bytes, so an `UnknownSymbol` error leaves writer state intact.
        self.buf2.reset();
        self.buf2.put_uvarint(labels.len());

        let cur_bytes = labels.as_bytes();
        let cur_base = cur_bytes.as_ptr() as usize;
        let prev_bytes: &[u8] = self
            .last_series
            .as_ref()
            .map_or(&[], |prev| prev.as_bytes());
        self.next_label_cache.clear();

        for (pair_idx, (name, value)) in labels.iter().enumerate() {
            let nb = name.as_bytes();
            let vb = value.as_bytes();
            let n_start = (nb.as_ptr() as usize) - cur_base;
            let n_end = n_start + nb.len();
            let v_start = (vb.as_ptr() as usize) - cur_base;
            let v_end = v_start + vb.len();

            let cached = self.prev_label_cache.get(pair_idx).copied();
            let (name_idx, count_slot) = match cached {
                Some((pn_s, pn_e, pn_idx, p_slot, _, _, _))
                    if prev_bytes.get(pn_s..pn_e) == Some(nb) =>
                {
                    (pn_idx, p_slot)
                }
                _ => {
                    let nid = self
                        .lookup_symbol(name)
                        .ok_or_else(|| IndexError::UnknownSymbol(name.to_string()))?;
                    let slot = self
                        .label_name_slots
                        .get(&nid)
                        .copied()
                        .unwrap_or(usize::MAX);
                    (nid, slot)
                }
            };
            let val_idx = match cached {
                Some((_, _, _, _, pv_s, pv_e, pv_idx))
                    if prev_bytes.get(pv_s..pv_e) == Some(vb) =>
                {
                    pv_idx
                }
                _ => self
                    .lookup_symbol(value)
                    .ok_or_else(|| IndexError::UnknownSymbol(value.to_string()))?,
            };

            self.next_label_cache
                .push((n_start, n_end, name_idx, count_slot, v_start, v_end, val_idx));
            self.buf2.put_uvarint32(name_idx);
            self.buf2.put_uvarint32(val_idx);
        }

        self.buf2.put_uvarint(chunks.len());
        if let Some(c0) = chunks.first() {
            self.buf2.put_varint64(c0.min_time);
            self.buf2
                .put_uvarint64(u64::from_ne_bytes(c0.max_time.wrapping_sub(c0.min_time).to_ne_bytes()));
            self.buf2.put_uvarint64(c0.ref_id);
            let mut t0 = c0.max_time;
            let mut ref0 = i64::from_ne_bytes(c0.ref_id.to_ne_bytes());
            for c in &chunks[1..] {
                self.buf2
                    .put_uvarint64(u64::from_ne_bytes(c.min_time.wrapping_sub(t0).to_ne_bytes()));
                self.buf2.put_uvarint64(u64::from_ne_bytes(
                    c.max_time.wrapping_sub(c.min_time).to_ne_bytes(),
                ));
                t0 = c.max_time;
                let cur_ref = i64::from_ne_bytes(c.ref_id.to_ne_bytes());
                self.buf2.put_varint64(cur_ref.wrapping_sub(ref0));
                ref0 = cur_ref;
            }
        }

        self.buf1.reset();
        self.buf1.put_uvarint(self.buf2.len());
        self.buf2.put_crc32c();

        self.add_padding(SERIES_BYTE_ALIGN)?;
        debug_assert_eq!(self.pos % (SERIES_BYTE_ALIGN as u64), 0);
        let series_id = u32::try_from(self.pos / (SERIES_BYTE_ALIGN as u64))
            .map_err(|_| IndexError::IndexExceeds64GiB(self.pos))?;

        write_main_slice(&mut self.f, &mut self.pos, self.buf1.as_slice())?;
        write_main_slice(&mut self.f, &mut self.pos, self.buf2.as_slice())?;

        // Commit label counts and swap adjacent-series cache only after all writes succeed.
        for entry in &mut self.next_label_cache {
            let slot = if entry.3 != usize::MAX {
                self.label_name_counts[entry.3].1 += 1;
                entry.3
            } else if let Some(&s) = self.label_name_slots.get(&entry.2) {
                self.label_name_counts[s].1 += 1;
                s
            } else {
                let s = self.label_name_counts.len();
                self.label_name_counts.push((entry.2, 1));
                self.label_name_slots.insert(entry.2, s);
                s
            };
            entry.3 = slot;
        }
        std::mem::swap(&mut self.prev_label_cache, &mut self.next_label_cache);
        self.all_series_ids.push(series_id);

        self.last_series = Some(labels.clone());
        self.last_series_ref = ref_id;
        self.last_chunk_ref = last_chunk_ref;
        Ok(())
    }

    fn write_postings_to_tmp_files(&mut self) -> Result<(), IndexError> {
        let f = self
            .f
            .as_mut()
            .ok_or(IndexError::InvalidStage("writer closed"))?;
        f.flush()?;

        // Release hash table before allocating posting sort buffers (symbols_arena + symbol_offsets remain).
        self.symbol_table = hashbrown::HashTable::new();

        // 1. Write all-postings ("", "") directly from series IDs recorded during `add_series`
        //    (eliminating a full pass-0 mmap scan of the series region).
        let all_offsets = std::mem::take(&mut self.all_series_ids);
        let max_postings = all_offsets.len() as u64;
        write_posting_entry(
            &mut self.fp,
            &mut self.fp_pos,
            &mut self.fpo,
            &mut self.fpo_pos,
            &mut self.cnt_po,
            &mut self.buf1,
            "",
            "",
            &all_offsets,
        )?;
        drop(all_offsets);

        if self.label_name_counts.is_empty() {
            return Ok(());
        }

        let file_for_mmap = File::open(&self.path)?;
        // SAFETY: only reading up to `self.toc.label_indices` which is already flushed.
        let mmap = unsafe { MmapOptions::new().map(&file_for_mmap)? };
        let series_region = &mmap[..self.toc.label_indices as usize];

        // Symbols were added in strictly ascending lexicographical order, so sorting label name
        // symbol IDs numerically produces exact ascending lexicographical label-name order.
        let mut sorted_names: Vec<(u32, u64)> = self.label_name_counts.clone();
        sorted_names.sort_unstable_by_key(|&(sid, _)| sid);

        // Batch up to 4x `max_postings` packed (val_sid, series_id) u64 entries per mmap scan
        // (8 bytes/entry => ~32 MiB at 1M series, cutting mmap scans ~4x vs 1-label batches while
        // eliminating BTreeMap & millions of tiny Vec<u32> allocations).
        let batch_limit = max_postings.saturating_mul(4).max(1);
        let mut offs_buf: Vec<u32> = Vec::new();
        let mut name_idx = 0usize;

        while name_idx < sorted_names.len() {
            let mut batch_count = 0u64;
            let batch_start = name_idx;
            while name_idx < sorted_names.len() {
                let (sid, cnt) = sorted_names[name_idx];
                if cnt > max_postings {
                    let name = symbol_str(&self.symbols_arena, &self.symbol_offsets, sid);
                    return Err(IndexError::Corrupt(format!(
                        "corruption detected when writing postings to index: label {name:?} has {cnt} uses, but maxPostings is {max_postings}"
                    )));
                }
                if cnt + batch_count > batch_limit && batch_count > 0 {
                    break;
                }
                batch_count += cnt;
                name_idx += 1;
            }

            let batch = &sorted_names[batch_start..name_idx];
            let mut batch_packed: Vec<Vec<u64>> = batch
                .iter()
                .map(|&(_, cnt)| Vec::with_capacity(cnt as usize))
                .collect();

            if batch.len() == 1 {
                let target_lno = batch[0].0;
                let dst = &mut batch_packed[0];
                let mut cur = self.toc.series as usize;
                while cur < series_region.len() {
                    while cur < series_region.len() && series_region[cur] == 0 {
                        cur += 1;
                    }
                    if cur >= series_region.len() {
                        break;
                    }
                    let series_id = u32::try_from(cur / SERIES_BYTE_ALIGN)
                        .map_err(|_| IndexError::IndexExceeds64GiB(cur as u64))?;
                    let mut dec = Decbuf::new(&series_region[cur..]);
                    let entry_len = dec.uvarint()?;
                    let header_consumed = (series_region.len() - cur) - dec.len();
                    let num_labels = dec.uvarint()?;
                    for _ in 0..num_labels {
                        let lno = dec.uvarint32()?;
                        let lvo = dec.uvarint32()?;
                        if lno == target_lno {
                            dst.push((u64::from(lvo) << 32) | u64::from(series_id));
                        }
                    }
                    cur += header_consumed + entry_len + 4;
                }
            } else if batch.len() <= 8 {
                let mut target_ids = [u32::MAX; 8];
                let blen = batch.len();
                for (slot, &(sid, _)) in batch.iter().enumerate() {
                    target_ids[slot] = sid;
                }
                let mut cur = self.toc.series as usize;
                while cur < series_region.len() {
                    while cur < series_region.len() && series_region[cur] == 0 {
                        cur += 1;
                    }
                    if cur >= series_region.len() {
                        break;
                    }
                    let series_id = u32::try_from(cur / SERIES_BYTE_ALIGN)
                        .map_err(|_| IndexError::IndexExceeds64GiB(cur as u64))?;
                    let mut dec = Decbuf::new(&series_region[cur..]);
                    let entry_len = dec.uvarint()?;
                    let header_consumed = (series_region.len() - cur) - dec.len();
                    let num_labels = dec.uvarint()?;
                    for _ in 0..num_labels {
                        let lno = dec.uvarint32()?;
                        let lvo = dec.uvarint32()?;
                        for (slot, &tid) in target_ids[..blen].iter().enumerate() {
                            if lno == tid {
                                batch_packed[slot]
                                    .push((u64::from(lvo) << 32) | u64::from(series_id));
                                break;
                            }
                        }
                    }
                    cur += header_consumed + entry_len + 4;
                }
            } else {
                let mut lno_to_slot: hashbrown::HashMap<u32, usize> =
                    hashbrown::HashMap::with_capacity(batch.len());
                for (slot, &(sid, _)) in batch.iter().enumerate() {
                    lno_to_slot.insert(sid, slot);
                }
                let mut cur = self.toc.series as usize;
                while cur < series_region.len() {
                    while cur < series_region.len() && series_region[cur] == 0 {
                        cur += 1;
                    }
                    if cur >= series_region.len() {
                        break;
                    }
                    let series_id = u32::try_from(cur / SERIES_BYTE_ALIGN)
                        .map_err(|_| IndexError::IndexExceeds64GiB(cur as u64))?;
                    let mut dec = Decbuf::new(&series_region[cur..]);
                    let entry_len = dec.uvarint()?;
                    let header_consumed = (series_region.len() - cur) - dec.len();
                    let num_labels = dec.uvarint()?;
                    for _ in 0..num_labels {
                        let lno = dec.uvarint32()?;
                        let lvo = dec.uvarint32()?;
                        if let Some(&slot) = lno_to_slot.get(&lno) {
                            batch_packed[slot]
                                .push((u64::from(lvo) << 32) | u64::from(series_id));
                        }
                    }
                    cur += header_consumed + entry_len + 4;
                }
            }

            // Sort each label's packed (val_sid << 32 | series_id) array and stream postings
            // in exact ascending (label_name, label_value, series_id) order with zero String clones.
            for (slot, &(name_sid, _)) in batch.iter().enumerate() {
                let packed = &mut batch_packed[slot];
                if packed.is_empty() {
                    continue;
                }
                packed.sort_unstable();
                let name_str = symbol_str(&self.symbols_arena, &self.symbol_offsets, name_sid);
                let mut i = 0usize;
                while i < packed.len() {
                    let val_sid = (packed[i] >> 32) as u32;
                    offs_buf.clear();
                    while i < packed.len() && ((packed[i] >> 32) as u32) == val_sid {
                        offs_buf.push(packed[i] as u32);
                        i += 1;
                    }
                    let val_str = symbol_str(&self.symbols_arena, &self.symbol_offsets, val_sid);
                    write_posting_entry(
                        &mut self.fp,
                        &mut self.fp_pos,
                        &mut self.fpo,
                        &mut self.fpo_pos,
                        &mut self.cnt_po,
                        &mut self.buf1,
                        name_str,
                        val_str,
                        &offs_buf,
                    )?;
                }
                // Free buffer immediately before moving to next label in batch.
                *packed = Vec::new();
            }
        }
        Ok(())
    }

    fn write_postings(&mut self) -> Result<(), IndexError> {
        self.add_padding(4)?;
        self.postings_start = self.pos;

        let mut fp = self.fp.take().unwrap();
        fp.flush()?;
        let mut fp_file = fp.into_inner().map_err(|e| e.into_error())?;
        fp_file.seek(SeekFrom::Start(0))?;

        let f = self.f.as_mut().unwrap();
        let copied = io::copy(&mut fp_file, f)?;
        if copied != self.fp_pos {
            return Err(IndexError::Corrupt(format!(
                "postings temp copy mismatch: wrote {}, read {copied}",
                self.fp_pos
            )));
        }
        self.pos += copied;
        drop(fp_file);
        let _ = fs::remove_file(&self.tmp_p_path);
        Ok(())
    }

    fn write_postings_offset_table(&mut self) -> Result<(), IndexError> {
        let mut fpo = self.fpo.take().unwrap();
        fpo.flush()?;
        let fpo_file = fpo.into_inner().map_err(|e| e.into_error())?;
        let fpo_mmap = unsafe { MmapOptions::new().map(&fpo_file)? };
        let fpo_bytes = &fpo_mmap[..self.fpo_pos as usize];

        let start_pos = self.pos;
        self.write_main(b"alen")?;

        let adjustment = self.postings_start;
        let cnt_bytes = self.cnt_po.to_be_bytes();
        let mut crc = crc32c::crc32c(&cnt_bytes);
        self.write_main(&cnt_bytes)?;

        let mut dec = Decbuf::new(fpo_bytes);
        let mut cnt = self.cnt_po;
        while cnt > 0 {
            let key_cnt = dec.uvarint()?;
            let name = dec.uvarint_str()?;
            let value = dec.uvarint_str()?;
            let off = dec.uvarint64()? + adjustment;

            self.buf1.reset();
            self.buf1.put_uvarint(key_cnt);
            self.buf1.put_uvarint_str(name);
            self.buf1.put_uvarint_str(value);
            self.buf1.put_uvarint64(off);
            let slice = self.buf1.as_slice();
            crc = crc32c::crc32c_append(crc, slice);
            write_main_slice(&mut self.f, &mut self.pos, slice)?;
            cnt -= 1;
        }

        drop(fpo_mmap);
        drop(fpo_file);
        let _ = fs::remove_file(&self.tmp_po_path);

        let table_len_u64 = self.pos - start_pos - 4;
        let table_len = u32::try_from(table_len_u64)
            .map_err(|_| IndexError::PostingsOffsetTableTooLarge(table_len_u64))?;
        self.write_main_at(&table_len.to_be_bytes(), start_pos)?;
        self.write_main(&crc.to_be_bytes())
    }

    fn write_toc(&mut self) -> Result<(), IndexError> {
        self.buf1.reset();
        self.buf1.put_be64(self.toc.symbols);
        self.buf1.put_be64(self.toc.series);
        self.buf1.put_be64(self.toc.label_indices);
        self.buf1.put_be64(self.toc.label_indices_table);
        self.buf1.put_be64(self.toc.postings);
        self.buf1.put_be64(self.toc.postings_table);
        self.buf1.put_crc32c();
        let bytes = self.buf1.as_slice().to_vec();
        self.write_main(&bytes)
    }

    pub fn close(mut self) -> Result<(), IndexError> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), IndexError> {
        if self.stage == WriterStage::Done {
            return Ok(());
        }
        let res = (|| -> Result<(), IndexError> {
            self.ensure_stage(WriterStage::Done)?;
            if let Some(mut f) = self.f.take() {
                f.flush()?;
                let file = f.into_inner().map_err(|e| e.into_error())?;
                file.sync_all()?;
            }
            Ok(())
        })();
        if res.is_err() {
            self.close_failed = true;
        }
        res
    }
}

impl Drop for IndexWriter {
    fn drop(&mut self) {
        if !self.close_failed && self.stage != WriterStage::Done && self.f.is_some() {
            let _ = self.close_inner();
        }
        let _ = fs::remove_file(&self.tmp_p_path);
        let _ = fs::remove_file(&self.tmp_po_path);
    }
}

#[derive(Clone, Copy, Debug)]
struct PostingOffsetEntry {
    name_off: u32,
    name_len: u32,
    val_off: u32,
    val_len: u32,
    table_rel_off: usize,
}

#[derive(Debug)]
enum IndexData {
    Mmap(Mmap),
    Bytes(Bytes),
}

impl Deref for IndexData {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Self::Mmap(m) => m,
            Self::Bytes(b) => b,
        }
    }
}

/// Optional per-block cache of `SeriesRef -> StableHash` for `IndexReader::sharded_postings`.
#[derive(Debug, Default)]
pub struct SeriesHashCache {
    hashes: RwLock<HashMap<u64, u64>>,
}

impl SeriesHashCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fetch(&self, id: u64) -> Option<u64> {
        self.hashes.read().get(&id).copied()
    }

    pub fn store(&self, id: u64, hash: u64) {
        self.hashes.write().insert(id, hash);
    }
}

/// Memory-mapped or byte-slice reader for Prometheus TSDB Index v1/v2/v3 files.
#[derive(Debug)]
pub struct IndexReader {
    mmap: IndexData,
    version: u8,
    toc: IndexToc,
    symbol_offsets: Vec<usize>,
    num_symbols: usize,
    name_cache: HashMap<u32, (u32, u32)>,
    postings_index: Vec<Box<[PostingOffsetEntry]>>,
    postings_v1: BTreeMap<String, BTreeMap<String, u64>>,
}

impl IndexReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IndexError> {
        let f = File::open(path.as_ref())?;
        // SAFETY: persisted block index files are immutable.
        let mmap = unsafe { MmapOptions::new().map(&f)? };
        drop(f);
        Self::from_data(IndexData::Mmap(mmap))
    }

    pub fn from_bytes(bytes: impl Into<Bytes>) -> Result<Self, IndexError> {
        Self::from_data(IndexData::Bytes(bytes.into()))
    }

    fn from_data(mmap: IndexData) -> Result<Self, IndexError> {
        if mmap.len() < INDEX_HEADER_LEN {
            return Err(IndexError::Encoding(EncodingError::InvalidSize));
        }
        let magic = u32::from_be_bytes([mmap[0], mmap[1], mmap[2], mmap[3]]);
        if magic != MAGIC_INDEX {
            return Err(IndexError::InvalidMagic(magic));
        }
        let version = mmap[4];
        if version != INDEX_FORMAT_V1 && version != INDEX_FORMAT_V2 && version != INDEX_FORMAT_V3 {
            return Err(IndexError::UnsupportedVersion(version));
        }

        let toc = IndexToc::parse(&mmap)?;
        let (symbol_offsets, num_symbols) = parse_symbols_index(&mmap, toc.symbols as usize)?;
        let postings_table_off = usize::try_from(toc.postings_table)
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let postings_payload_start = postings_table_off
            .checked_add(4)
            .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;

        let (postings_index, postings_v1, name_cache) = if version == INDEX_FORMAT_V1 {
            let (postings_v1, name_offsets) =
                parse_postings_offset_table_v1(&mmap, postings_table_off)?;
            let mut name_cache = HashMap::with_capacity(name_offsets.len());
            for (name, (name_off, name_len)) in name_offsets {
                let sid = reverse_lookup_symbol(&mmap, &symbol_offsets, num_symbols, &name, version)?;
                name_cache.insert(sid, (name_off, name_len));
            }
            (Vec::new(), postings_v1, name_cache)
        } else {
            let postings_index = parse_postings_offset_table(&mmap, postings_table_off)?;
            let mut name_cache = HashMap::with_capacity(postings_index.len());
            for entries in &postings_index {
                let Some(first) = entries.first() else {
                    continue;
                };
                if first.name_len == 0 {
                    continue;
                }
                let (start, end) = resolve_posting_str_range(
                    postings_payload_start,
                    first.name_off,
                    first.name_len,
                );
                // SAFETY: `parse_postings_offset_table` validated UTF-8 and slice bounds on `mmap`.
                let name = unsafe { std::str::from_utf8_unchecked(&mmap[start..end]) };
                let sid = reverse_lookup_symbol(&mmap, &symbol_offsets, num_symbols, name, version)?;
                name_cache.insert(sid, (first.name_off, first.name_len));
            }
            (postings_index, BTreeMap::new(), name_cache)
        };

        Ok(Self {
            mmap,
            version,
            toc,
            symbol_offsets,
            num_symbols,
            name_cache,
            postings_index,
            postings_v1,
        })
    }

    #[inline]
    fn mmap_str(&self, off: u32, len: u32) -> &str {
        let payload_start = (self.toc.postings_table as usize) + 4;
        let (start, end) = resolve_posting_str_range(payload_start, off, len);
        // SAFETY: offsets/lengths are derived from validated UTF-8 subslices of `self.mmap` at open.
        unsafe { std::str::from_utf8_unchecked(&self.mmap[start..end]) }
    }

    #[inline]
    fn postings_entries(&self, name: &str) -> Option<&[PostingOffsetEntry]> {
        let idx = self
            .postings_index
            .binary_search_by(|entries| {
                let Some(first) = entries.first() else {
                    return std::cmp::Ordering::Less;
                };
                self.mmap_str(first.name_off, first.name_len).cmp(name)
            })
            .ok()?;
        Some(&self.postings_index[idx])
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    /// Go `index.Reader.SymbolTableSize`: `len(symbols.offsets) * 8` (one offset kept per
    /// 32 symbols).
    pub fn symbol_table_size(&self) -> u64 {
        (self.symbol_offsets.len() * 8) as u64
    }

    pub fn size(&self) -> usize {
        self.mmap.len()
    }

    /// Looks up symbol string by index `o` (using `SYMBOL_FACTOR = 32` checkpoints and
    /// pre-validated UTF-8 from `IndexReader::open`, matching Go `tsdb/index`).
    #[inline]
    pub fn lookup_symbol(&self, o: u32) -> Result<&str, IndexError> {
        if self.version == INDEX_FORMAT_V1 {
            let slice = self
                .mmap
                .get(o as usize..)
                .ok_or(IndexError::UnknownSymbolOffset(o))?;
            let mut dec = Decbuf::new(slice);
            return Ok(dec.uvarint_str()?);
        }
        if (o as usize) >= self.num_symbols {
            return Err(IndexError::UnknownSymbolOffset(o));
        }
        let mut off = self.symbol_offsets[(o as usize) / SYMBOL_FACTOR];
        let skip = (o as usize) % SYMBOL_FACTOR;
        let mmap = &self.mmap;
        for _ in 0..skip {
            let b0 = mmap[off];
            if b0 < 0x80 {
                off += 1 + (b0 as usize);
            } else {
                let mut dec = Decbuf::new(&mmap[off..]);
                let len = dec.uvarint()?;
                let hdr = (mmap.len() - off) - dec.len();
                off += hdr + len;
            }
        }
        let b0 = mmap[off];
        let (len, str_off) = if b0 < 0x80 {
            (b0 as usize, off + 1)
        } else {
            let mut dec = Decbuf::new(&mmap[off..]);
            let len = dec.uvarint()?;
            let hdr = (mmap.len() - off) - dec.len();
            (len, off + hdr)
        };
        // SAFETY: `parse_symbols_index` validates bounds and UTF-8 for every symbol on open.
        Ok(unsafe { std::str::from_utf8_unchecked(&mmap[str_off..str_off + len]) })
    }

    /// Returns all symbols in ascending lexicographical order.
    pub fn symbols(&self) -> Result<Vec<String>, IndexError> {
        let slice = checked_decbuf_at(&self.mmap, self.toc.symbols as usize)?;
        let mut dec = Decbuf::new(slice);
        let cnt = dec.be32()? as usize;
        let mut out = Vec::with_capacity(cnt.min(dec.len()));
        for _ in 0..cnt {
            out.push(dec.uvarint_str()?.to_string());
        }
        Ok(out)
    }

    /// Returns all unique label names present in the index (sorted, excluding `""`).
    pub fn label_names(&self) -> Vec<String> {
        if self.version == INDEX_FORMAT_V1 {
            return self
                .postings_v1
                .keys()
                .filter(|k| !k.is_empty())
                .cloned()
                .collect();
        }
        self.postings_index
            .iter()
            .filter_map(|entries| {
                let first = entries.first()?;
                if first.name_len == 0 {
                    None
                } else {
                    Some(self.mmap_str(first.name_off, first.name_len).to_string())
                }
            })
            .collect()
    }

    /// Like `label_values`, supporting `limit` and `limit_smallest` (matching Go `index.Reader.LabelValues`).
    pub fn label_values_with_hints(
        &self,
        name: &str,
        limit: Option<usize>,
        limit_smallest: bool,
    ) -> Result<Vec<String>, IndexError> {
        let mut vals = self.label_values(name)?;
        if self.version == INDEX_FORMAT_V1 && limit_smallest {
            vals.sort_unstable();
        }
        if let Some(lim) = limit {
            if lim > 0 && vals.len() > lim {
                vals.truncate(lim);
            }
        }
        Ok(vals)
    }

    /// Returns all sorted label values for `name`.
    pub fn label_values(&self, name: &str) -> Result<Vec<String>, IndexError> {
        if self.version == INDEX_FORMAT_V1 {
            return Ok(self
                .postings_v1
                .get(name)
                .map_or_else(Vec::new, |m| m.keys().cloned().collect()));
        }
        let Some(entries) = self.postings_entries(name) else {
            return Ok(Vec::new());
        };
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(Vec::new());
        };
        let last_val = self.mmap_str(last.val_off, last.val_len);
        let mut out = Vec::with_capacity(entries.len() * SYMBOL_FACTOR);
        self.traverse_posting_offsets(first.table_rel_off, |val, _| {
            out.push(val.to_string());
            Ok(val != last_val)
        })?;
        Ok(out)
    }

    #[inline]
    fn decode_series_labels(&self, dec: &mut Decbuf<'_>) -> Result<Labels, IndexError> {
        let num_labels = dec.uvarint()?;
        if num_labels == 0 {
            return Ok(Labels::empty());
        }
        let mut stack_buf = [0u8; 512];
        let mut pos = 0usize;
        let mut heap_buf: Option<Vec<u8>> = None;

        for _ in 0..num_labels {
            let lno = dec.uvarint32()?;
            let lvo = dec.uvarint32()?;
            let ln = match self.name_cache.get(&lno) {
                Some(&(off, len)) => self.mmap_str(off, len),
                None => self.lookup_symbol(lno)?,
            };
            let lv = self.lookup_symbol(lvo)?;
            if ln.len() >= (1 << 24) || lv.len() >= (1 << 24) {
                return Err(IndexError::Corrupt(format!(
                    "label string length exceeds max stringlabels size ({})",
                    (1 << 24) - 1
                )));
            }
            let need = (if ln.len() < 255 { 1 } else { 4 })
                + ln.len()
                + (if lv.len() < 255 { 1 } else { 4 })
                + lv.len();

            if let Some(ref mut hb) = heap_buf {
                encode_stringlabel_size(hb, ln.len());
                hb.extend_from_slice(ln.as_bytes());
                encode_stringlabel_size(hb, lv.len());
                hb.extend_from_slice(lv.as_bytes());
            } else if pos + need <= stack_buf.len() {
                pos = write_stringlabel_slice(&mut stack_buf, pos, ln.as_bytes());
                pos = write_stringlabel_slice(&mut stack_buf, pos, lv.as_bytes());
            } else {
                let mut hb = Vec::with_capacity((pos + need) * 2);
                hb.extend_from_slice(&stack_buf[..pos]);
                encode_stringlabel_size(&mut hb, ln.len());
                hb.extend_from_slice(ln.as_bytes());
                encode_stringlabel_size(&mut hb, lv.len());
                hb.extend_from_slice(lv.as_bytes());
                heap_buf = Some(hb);
            }
        }

        let bytes = match heap_buf {
            Some(hb) => Bytes::from(hb),
            None => Bytes::copy_from_slice(&stack_buf[..pos]),
        };
        // SAFETY: `ln` and `lv` are `&str` (validated UTF-8 at `IndexReader::open`), encoded with
        // valid `stringlabels` framing immediately above.
        Ok(unsafe { Labels::from_valid_stringlabels_bytes_unchecked(bytes) })
    }

    #[inline]
    fn series_offset(&self, ref_id: u64) -> Result<usize, IndexError> {
        let r = usize::try_from(ref_id).map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        if self.version == INDEX_FORMAT_V1 {
            Ok(r)
        } else {
            r.checked_mul(SERIES_BYTE_ALIGN)
                .ok_or(IndexError::Encoding(EncodingError::InvalidSize))
        }
    }

    /// Decodes the series entry at `ref_id` (`series_id = file_offset` in v1,
    /// `file_offset / 16` in v2/v3).
    pub fn series(&self, ref_id: u64) -> Result<(Labels, Vec<ChunkMeta>), IndexError> {
        let offset = self.series_offset(ref_id)?;
        let payload = checked_uvarint_decbuf_at(&self.mmap, offset)?;
        let mut dec = Decbuf::new(payload);
        let labels = self.decode_series_labels(&mut dec)?;

        let num_chunks = dec.uvarint()?;
        let mut chks = Vec::with_capacity(num_chunks.min(dec.len()));
        if num_chunks > 0 {
            let t0 = dec.varint64()?;
            let dt0 = i64::from_ne_bytes(dec.uvarint64()?.to_ne_bytes());
            let maxt0 = t0.wrapping_add(dt0);
            let mut ref0 = i64::from_ne_bytes(dec.uvarint64()?.to_ne_bytes());
            chks.push(ChunkMeta {
                ref_id: u64::from_ne_bytes(ref0.to_ne_bytes()),
                min_time: t0,
                max_time: maxt0,
                chunk: None,
            });
            let mut prev_maxt = maxt0;
            for _ in 1..num_chunks {
                let d_mint = i64::from_ne_bytes(dec.uvarint64()?.to_ne_bytes());
                let mint = prev_maxt.wrapping_add(d_mint);
                let d_maxt = i64::from_ne_bytes(dec.uvarint64()?.to_ne_bytes());
                let maxt = mint.wrapping_add(d_maxt);
                ref0 = ref0.wrapping_add(dec.varint64()?);
                prev_maxt = maxt;
                chks.push(ChunkMeta {
                    ref_id: u64::from_ne_bytes(ref0.to_ne_bytes()),
                    min_time: mint,
                    max_time: maxt,
                    chunk: None,
                });
            }
        }
        Ok((labels, chks))
    }

    /// Decodes only the labels for `ref_id`.
    pub fn series_labels(&self, ref_id: u64) -> Result<Labels, IndexError> {
        let offset = self.series_offset(ref_id)?;
        let payload = checked_uvarint_decbuf_at(&self.mmap, offset)?;
        let mut dec = Decbuf::new(payload);
        self.decode_series_labels(&mut dec)
    }

    /// Decodes a raw postings list at `postings_off`.
    fn decode_postings_at(&self, postings_off: u64) -> Result<Vec<u64>, IndexError> {
        let off = usize::try_from(postings_off)
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let payload = checked_decbuf_at(&self.mmap, off)?;
        let mut dec = Decbuf::new(payload);
        let n = dec.be32()? as usize;
        let expected_len = 4usize
            .checked_mul(n)
            .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
        if dec.len() != expected_len {
            return Err(IndexError::Corrupt(format!(
                "unexpected postings length: expected {}, got {}",
                expected_len,
                dec.len()
            )));
        }
        let mut out = Vec::with_capacity(n.min(dec.len() / 4));
        for _ in 0..n {
            out.push(dec.be32()? as u64);
        }
        Ok(out)
    }

    fn traverse_posting_offsets<F>(&self, start_rel_off: usize, mut cb: F) -> Result<(), IndexError>
    where
        F: FnMut(&str, u64) -> Result<bool, IndexError>,
    {
        let table_start = usize::try_from(self.toc.postings_table)
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let payload_start = table_start
            .checked_add(4)
            .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
        if payload_start > self.mmap.len() {
            return Err(IndexError::Encoding(EncodingError::InvalidSize));
        }
        let payload_len =
            u32::from_be_bytes(self.mmap[table_start..payload_start].try_into().unwrap())
                as usize;
        let payload_end = payload_start
            .checked_add(payload_len)
            .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
        if payload_end > self.mmap.len() {
            return Err(IndexError::Encoding(EncodingError::InvalidSize));
        }
        // Skip 4B length header via relative offset already anchored at table payload start.
        let mut dec = Decbuf::new(&self.mmap[payload_start..payload_end]);
        dec.skip(start_rel_off)?;
        let mut prefix_skip: Option<usize> = None;
        while !dec.is_empty() {
            match prefix_skip {
                None => {
                    let before = dec.len();
                    let _ = dec.uvarint()?; // key_count = 2
                    let _ = dec.uvarint_bytes()?; // label name
                    prefix_skip = Some(before - dec.len());
                }
                Some(skip) => {
                    dec.skip(skip)?;
                }
            }
            let val = dec.uvarint_str()?;
            let postings_off = dec.uvarint64()?;
            if !cb(val, postings_off)? {
                break;
            }
        }
        Ok(())
    }

    pub fn postings(&self, name: &str, values: &[&str]) -> Result<Vec<u64>, IndexError> {
        if self.version == INDEX_FORMAT_V1 {
            let Some(vals) = self.postings_v1.get(name) else {
                return Ok(Vec::new());
            };
            if values.is_empty() {
                return Ok(Vec::new());
            }
            let mut sorted_vals: Vec<&str> = values.to_vec();
            sorted_vals.sort_unstable();
            sorted_vals.dedup();
            let mut lists = Vec::with_capacity(sorted_vals.len());
            for v in sorted_vals {
                if let Some(&off) = vals.get(v) {
                    lists.push(self.decode_postings_at(off)?);
                }
            }
            let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
            return Ok(merge_postings(&slices));
        }
        let Some(entries) = self.postings_entries(name) else {
            return Ok(Vec::new());
        };
        if entries.is_empty() || values.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted_vals: Vec<&str> = values.to_vec();
        sorted_vals.sort_unstable();
        sorted_vals.dedup();

        let first_val = self.mmap_str(entries[0].val_off, entries[0].val_len);
        let mut vi = 0usize;
        while vi < sorted_vals.len() && sorted_vals[vi] < first_val {
            vi += 1;
        }

        let mut lists: Vec<Vec<u64>> = Vec::new();
        while vi < sorted_vals.len() {
            let prev_vi = vi;
            let target = sorted_vals[vi];
            let mut idx = entries.partition_point(|e| self.mmap_str(e.val_off, e.val_len) < target);
            if idx == entries.len() {
                break;
            }
            if idx > 0 && self.mmap_str(entries[idx].val_off, entries[idx].val_len) != target {
                idx -= 1;
            }

            let next_anchor = entries
                .get(idx + 1)
                .map(|e| self.mmap_str(e.val_off, e.val_len));
            self.traverse_posting_offsets(entries[idx].table_rel_off, |val, postings_off| {
                while vi < sorted_vals.len() && val >= sorted_vals[vi] {
                    if val == sorted_vals[vi] {
                        lists.push(self.decode_postings_at(postings_off)?);
                    }
                    vi += 1;
                }
                if vi >= sorted_vals.len() {
                    return Ok(false);
                }
                match next_anchor {
                    None => return Ok(false),
                    Some(next_v) if sorted_vals[vi] >= next_v => return Ok(false),
                    Some(_) => {}
                }
                Ok(true)
            })?;
            if vi == prev_vi {
                return Err(IndexError::Corrupt(format!(
                    "postings offset table traversal made no progress for label {name:?}"
                )));
            }
        }

        let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
        Ok(merge_postings(&slices))
    }

    pub fn postings_for_all_values(&self, name: &str) -> Result<Vec<u64>, IndexError> {
        if self.version == INDEX_FORMAT_V1 {
            let Some(vals) = self.postings_v1.get(name) else {
                return Ok(Vec::new());
            };
            let mut lists = Vec::with_capacity(vals.len());
            for &off in vals.values() {
                lists.push(self.decode_postings_at(off)?);
            }
            let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
            return Ok(merge_postings(&slices));
        }
        let Some(entries) = self.postings_entries(name) else {
            return Ok(Vec::new());
        };
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(Vec::new());
        };
        let last_val = self.mmap_str(last.val_off, last.val_len);
        let mut lists = Vec::new();
        self.traverse_posting_offsets(first.table_rel_off, |val, postings_off| {
            lists.push(self.decode_postings_at(postings_off)?);
            Ok(val != last_val)
        })?;
        let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
        Ok(merge_postings(&slices))
    }

    pub fn postings_for_label_matching(
        &self,
        name: &str,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Result<Vec<u64>, IndexError> {
        if self.version == INDEX_FORMAT_V1 {
            let Some(vals) = self.postings_v1.get(name) else {
                return Ok(Vec::new());
            };
            let mut lists = Vec::new();
            for (val, &off) in vals {
                if pred(val) {
                    lists.push(self.decode_postings_at(off)?);
                }
            }
            let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
            return Ok(merge_postings(&slices));
        }
        let Some(entries) = self.postings_entries(name) else {
            return Ok(Vec::new());
        };
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(Vec::new());
        };
        let last_val = self.mmap_str(last.val_off, last.val_len);
        let mut lists = Vec::new();
        self.traverse_posting_offsets(first.table_rel_off, |val, postings_off| {
            if pred(val) {
                lists.push(self.decode_postings_at(postings_off)?);
            }
            Ok(val != last_val)
        })?;
        let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
        Ok(merge_postings(&slices))
    }

    /// Returns unique sorted label names across `postings` series IDs.
    pub fn label_names_for(&self, postings: &[u64]) -> Result<Vec<String>, IndexError> {
        let mut name_ids: HashSet<u32> = HashSet::new();
        for &id in postings {
            let offset = self.series_offset(id)?;
            let payload = checked_uvarint_decbuf_at(&self.mmap, offset)?;
            let mut dec = Decbuf::new(payload);
            let k = dec.uvarint()?;
            for _ in 0..k {
                let lno = dec.uvarint32()?;
                let _ = dec.uvarint32()?;
                name_ids.insert(lno);
            }
        }
        let mut names = Vec::with_capacity(name_ids.len());
        for lno in name_ids {
            let s = match self.name_cache.get(&lno) {
                Some(&(off, len)) => self.mmap_str(off, len),
                None => self.lookup_symbol(lno)?,
            };
            names.push(s.to_string());
        }
        names.sort_unstable();
        Ok(names)
    }

    /// Context-aware `label_names_for`, checking `ctx` every `CHECK_CONTEXT_EVERY_N_ITERATIONS`
    /// series (matching Go `index.Reader.LabelNamesFor`).
    pub fn label_names_for_ctx(
        &self,
        ctx: &dyn ContextCheck,
        postings: &mut dyn PostingsIter,
    ) -> Result<Vec<String>, IndexError> {
        let mut name_ids: HashSet<u32> = HashSet::new();
        let mut i = 0usize;
        while postings.next() {
            let id = postings.at();
            i += 1;
            if i.is_multiple_of(CHECK_CONTEXT_EVERY_N_ITERATIONS) {
                if let Some(msg) = ctx.err() {
                    return Err(IndexError::ContextCanceled(msg));
                }
            }
            let offset = self.series_offset(id)?;
            let payload = checked_uvarint_decbuf_at(&self.mmap, offset)?;
            let mut dec = Decbuf::new(payload);
            let k = dec.uvarint()?;
            for _ in 0..k {
                let lno = dec.uvarint32()?;
                let _ = dec.uvarint32()?;
                name_ids.insert(lno);
            }
        }
        if let Some(e) = postings.err() {
            return Err(IndexError::Corrupt(e.to_string()));
        }
        let mut names = Vec::with_capacity(name_ids.len());
        for lno in name_ids {
            let s = match self.name_cache.get(&lno) {
                Some(&(off, len)) => self.mmap_str(off, len),
                None => self.lookup_symbol(lno)?,
            };
            names.push(s.to_string());
        }
        names.sort_unstable();
        Ok(names)
    }

    fn decode_postings_iter_at(&self, postings_off: u64) -> Result<BigEndianPostings, IndexError> {
        let off = usize::try_from(postings_off)
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let payload = checked_decbuf_at(&self.mmap, off)?;
        let (_, bep) = decode_postings_raw(payload)?;
        Ok(bep)
    }

    /// Streaming postings iterator for `name` and `values`.
    pub fn postings_iter(
        &self,
        name: &str,
        values: &[&str],
    ) -> Result<Box<dyn PostingsIter>, IndexError> {
        if self.version == INDEX_FORMAT_V1 {
            let Some(vals) = self.postings_v1.get(name) else {
                return Ok(Box::new(EmptyPostingsIter));
            };
            let mut its: Vec<Box<dyn PostingsIter>> = Vec::with_capacity(values.len());
            for &v in values {
                if let Some(&off) = vals.get(v) {
                    its.push(Box::new(self.decode_postings_iter_at(off)?));
                }
            }
            return Ok(merge_iters(its));
        }
        let Some(entries) = self.postings_entries(name) else {
            return Ok(Box::new(EmptyPostingsIter));
        };
        if entries.is_empty() || values.is_empty() {
            return Ok(Box::new(EmptyPostingsIter));
        }
        let mut sorted_vals: Vec<&str> = values.to_vec();
        sorted_vals.sort_unstable();
        sorted_vals.dedup();
        let first_val = self.mmap_str(entries[0].val_off, entries[0].val_len);
        let mut vi = 0usize;
        while vi < sorted_vals.len() && sorted_vals[vi] < first_val {
            vi += 1;
        }
        let mut its: Vec<Box<dyn PostingsIter>> = Vec::with_capacity(sorted_vals.len());
        while vi < sorted_vals.len() {
            let prev_vi = vi;
            let target = sorted_vals[vi];
            let mut idx = entries.partition_point(|e| self.mmap_str(e.val_off, e.val_len) < target);
            if idx == entries.len() {
                break;
            }
            if idx > 0 && self.mmap_str(entries[idx].val_off, entries[idx].val_len) != target {
                idx -= 1;
            }
            let next_anchor = entries
                .get(idx + 1)
                .map(|e| self.mmap_str(e.val_off, e.val_len));
            self.traverse_posting_offsets(entries[idx].table_rel_off, |val, postings_off| {
                while vi < sorted_vals.len() && val >= sorted_vals[vi] {
                    if val == sorted_vals[vi] {
                        its.push(Box::new(self.decode_postings_iter_at(postings_off)?));
                    }
                    vi += 1;
                }
                if vi >= sorted_vals.len() {
                    return Ok(false);
                }
                match next_anchor {
                    None => return Ok(false),
                    Some(next_v) if sorted_vals[vi] >= next_v => return Ok(false),
                    Some(_) => {}
                }
                Ok(true)
            })?;
            if vi == prev_vi {
                return Err(IndexError::Corrupt(format!(
                    "postings offset table traversal made no progress for label {name:?}"
                )));
            }
        }
        Ok(merge_iters(its))
    }

    fn traverse_posting_offsets_ctx<F>(
        &self,
        ctx: &dyn ContextCheck,
        start_rel_off: usize,
        mut cb: F,
    ) -> Result<(), IndexError>
    where
        F: FnMut(&str, u64) -> Result<bool, IndexError>,
    {
        let table_start = usize::try_from(self.toc.postings_table)
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let payload_start = table_start
            .checked_add(4)
            .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
        if payload_start > self.mmap.len() {
            return Err(IndexError::Encoding(EncodingError::InvalidSize));
        }
        let payload_len =
            u32::from_be_bytes(self.mmap[table_start..payload_start].try_into().unwrap())
                as usize;
        let payload_end = payload_start
            .checked_add(payload_len)
            .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
        if payload_end > self.mmap.len() {
            return Err(IndexError::Encoding(EncodingError::InvalidSize));
        }
        let mut dec = Decbuf::new(&self.mmap[payload_start..payload_end]);
        dec.skip(start_rel_off)?;
        let mut prefix_skip: Option<usize> = None;
        let mut ctx_err = ctx.err();
        while !dec.is_empty() && ctx_err.is_none() {
            match prefix_skip {
                None => {
                    let before = dec.len();
                    let _ = dec.uvarint()?;
                    let _ = dec.uvarint_bytes()?;
                    prefix_skip = Some(before - dec.len());
                }
                Some(skip) => {
                    dec.skip(skip)?;
                }
            }
            let val = dec.uvarint_str()?;
            let postings_off = dec.uvarint64()?;
            if !cb(val, postings_off)? {
                break;
            }
            ctx_err = ctx.err();
        }
        if let Some(msg) = ctx_err {
            return Err(IndexError::ContextCanceled(msg));
        }
        Ok(())
    }

    /// Context-aware streaming `PostingsForLabelMatching`, matching Go `index.Reader.PostingsForLabelMatching`.
    pub fn postings_for_label_matching_iter_ctx(
        &self,
        ctx: &dyn ContextCheck,
        name: &str,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Box<dyn PostingsIter> {
        if self.version == INDEX_FORMAT_V1 {
            let Some(vals) = self.postings_v1.get(name) else {
                return Box::new(EmptyPostingsIter);
            };
            let mut its: Vec<Box<dyn PostingsIter>> = Vec::new();
            for (count, (val, &off)) in (1usize..).zip(vals.iter()) {
                if count.is_multiple_of(CHECK_CONTEXT_EVERY_N_ITERATIONS) {
                    if let Some(msg) = ctx.err() {
                        return Box::new(ErrPostingsIter::new(PostingsError::ContextCanceled(msg)));
                    }
                }
                if !pred(val) {
                    continue;
                }
                match self.decode_postings_iter_at(off) {
                    Ok(p) => its.push(Box::new(p)),
                    Err(e) => return Box::new(ErrPostingsIter::new(PostingsError::Index(e.to_string()))),
                }
            }
            return merge_iters(its);
        }

        let Some(entries) = self.postings_entries(name) else {
            return Box::new(EmptyPostingsIter);
        };
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Box::new(EmptyPostingsIter);
        };
        let last_val = self.mmap_str(last.val_off, last.val_len);
        let mut its: Vec<Box<dyn PostingsIter>> = Vec::new();
        let res = self.traverse_posting_offsets_ctx(ctx, first.table_rel_off, |val, postings_off| {
            if pred(val) {
                its.push(Box::new(self.decode_postings_iter_at(postings_off)?));
            }
            Ok(val != last_val)
        });
        match res {
            Ok(()) => merge_iters(its),
            Err(IndexError::ContextCanceled(msg)) => {
                Box::new(ErrPostingsIter::new(PostingsError::ContextCanceled(msg)))
            }
            Err(e) => Box::new(ErrPostingsIter::new(PostingsError::Index(e.to_string()))),
        }
    }

    /// Streaming `PostingsForAllLabelValues`, matching Go `index.Reader.PostingsForAllLabelValues`.
    pub fn postings_for_all_values_iter(&self, name: &str) -> Box<dyn PostingsIter> {
        self.postings_for_label_matching_iter_ctx(&crate::postings::NoopContext, name, &mut |_| true)
    }

    /// Filters `p` to series whose `Labels::stable_hash() % shard_count == shard_index`,
    /// matching Go `index.Reader.ShardedPostings`.
    pub fn sharded_postings(
        &self,
        p: &mut dyn PostingsIter,
        shard_index: u64,
        shard_count: u64,
        cache: Option<&SeriesHashCache>,
    ) -> Box<dyn PostingsIter> {
        let mut out = Vec::with_capacity(128);
        while p.next() {
            let id = p.at();
            let hash = if let Some(h) = cache.and_then(|c| c.fetch(id)) {
                h
            } else {
                let lbls = match self.series_labels(id) {
                    Ok(l) => l,
                    Err(_) => {
                        return Box::new(ErrPostingsIter::new(PostingsError::Index(format!(
                            "series {id} not found"
                        ))));
                    }
                };
                let h = lbls.stable_hash();
                if let Some(c) = cache {
                    c.store(id, h);
                }
                h
            };
            if hash % shard_count == shard_index {
                out.push(id);
            }
        }
        if let Some(e) = p.err() {
            return Box::new(ErrPostingsIter::new(e.clone_for_iter()));
        }
        Box::new(ListPostings::new(out))
    }

    /// Returns sorted label values for `name` present in series referred to by `postings`,
    /// matching Go `index.Reader.LabelValuesFor`.
    pub fn label_values_for(
        &self,
        postings: &mut dyn PostingsIter,
        name: &str,
    ) -> Result<Vec<String>, IndexError> {
        self.label_values_for_inner(postings, name, true)
    }

    /// Returns sorted label values for `name` present in series NOT covered by `postings`,
    /// matching Go `index.Reader.LabelValuesExcluding`.
    pub fn label_values_excluding(
        &self,
        postings: &mut dyn PostingsIter,
        name: &str,
    ) -> Result<Vec<String>, IndexError> {
        self.label_values_for_inner(postings, name, false)
    }

    fn label_values_for_inner(
        &self,
        postings: &mut dyn PostingsIter,
        name: &str,
        include_matches: bool,
    ) -> Result<Vec<String>, IndexError> {
        let cloner = PostingsCloner::new(postings);
        if let Some(err) = cloner.clone_iter().err() {
            return Err(IndexError::Corrupt(err.to_string()));
        }

        if self.version == INDEX_FORMAT_V1 {
            let Some(vals) = self.postings_v1.get(name) else {
                return Ok(Vec::new());
            };
            let mut out = Vec::new();
            for (val, &off) in vals {
                let mut cur_postings = self.decode_postings_iter_at(off)?;
                let mut cloned = cloner.clone_iter();
                let is_match = if include_matches {
                    intersect_any(&mut *cloned, &mut cur_postings)
                } else {
                    !contains_all(&mut *cloned, &mut cur_postings)
                };
                if is_match {
                    out.push(val.clone());
                }
            }
            return Ok(out);
        }

        let Some(entries) = self.postings_entries(name) else {
            return Ok(Vec::new());
        };
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Ok(Vec::new());
        };
        let last_val = self.mmap_str(last.val_off, last.val_len);
        let mut out = Vec::new();
        self.traverse_posting_offsets(first.table_rel_off, |val, postings_off| {
            let mut cur_postings = self.decode_postings_iter_at(postings_off)?;
            let mut cloned = cloner.clone_iter();
            let is_match = if include_matches {
                intersect_any(&mut *cloned, &mut cur_postings)
            } else {
                !contains_all(&mut *cloned, &mut cur_postings)
            };
            if is_match {
                out.push(val.to_string());
            }
            Ok(val != last_val)
        })?;
        Ok(out)
    }
}

impl PostingsSource for IndexReader {
    fn all_postings(&self) -> Result<Vec<u64>, PostingsError> {
        self.postings("", &[""])
            .map_err(|e| PostingsError::Index(e.to_string()))
    }

    fn postings_for(&self, name: &str, values: &[&str]) -> Result<Vec<u64>, PostingsError> {
        self.postings(name, values)
            .map_err(|e| PostingsError::Index(e.to_string()))
    }

    fn postings_for_all_values(&self, name: &str) -> Result<Vec<u64>, PostingsError> {
        self.postings_for_all_values(name)
            .map_err(|e| PostingsError::Index(e.to_string()))
    }

    fn postings_for_label_matching(
        &self,
        name: &str,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Result<Vec<u64>, PostingsError> {
        self.postings_for_label_matching(name, pred)
            .map_err(|e| PostingsError::Index(e.to_string()))
    }
}

fn checked_decbuf_at(buf: &[u8], off: usize) -> Result<&[u8], IndexError> {
    let payload_start = off
        .checked_add(4)
        .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
    if payload_start > buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let len = u32::from_be_bytes(buf[off..payload_start].try_into().unwrap()) as usize;
    let payload_end = payload_start
        .checked_add(len)
        .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
    let crc_end = payload_end
        .checked_add(4)
        .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
    if crc_end > buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let payload = &buf[payload_start..payload_end];
    let exp_crc = u32::from_be_bytes(buf[payload_end..crc_end].try_into().unwrap());
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != exp_crc {
        return Err(IndexError::ChecksumMismatch {
            expected: exp_crc,
            actual: actual_crc,
        });
    }
    Ok(payload)
}

/// Decodes a raw big-endian postings payload, matching Go `index.DecodePostingsRaw`.
pub fn decode_postings_raw(buf: &[u8]) -> Result<(usize, BigEndianPostings), IndexError> {
    let mut dec = Decbuf::new(buf);
    let n = dec.be32()? as usize;
    let expected_len = 4usize
        .checked_mul(n)
        .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
    if dec.len() != expected_len {
        return Err(IndexError::Corrupt(format!(
            "unexpected postings length: expected {expected_len}, got {}",
            dec.len()
        )));
    }
    Ok((n, BigEndianPostings::from_slice(dec.remaining())))
}

/// Prometheus `index.Symbols` view over an index symbol table section.
#[derive(Debug)]
pub struct Symbols {
    bytes: Bytes,
    version: u8,
    off: usize,
    offsets: Vec<usize>,
    seen: usize,
}

impl Symbols {
    pub fn new(bytes: Bytes, version: u8, off: usize) -> Result<Self, IndexError> {
        let (offsets, seen) = parse_symbols_index(&bytes, off)?;
        Ok(Self {
            bytes,
            version,
            off,
            offsets,
            seen,
        })
    }

    pub fn size(&self) -> usize {
        self.offsets.len() * 8
    }

    pub fn lookup(&self, o: u32) -> Result<&str, IndexError> {
        if self.version == INDEX_FORMAT_V1 {
            let slice = self
                .bytes
                .get(o as usize..)
                .ok_or(IndexError::UnknownSymbolOffset(o))?;
            let mut dec = Decbuf::new(slice);
            return Ok(dec.uvarint_str()?);
        }
        if (o as usize) >= self.seen {
            return Err(IndexError::UnknownSymbolOffset(o));
        }
        let block = (o as usize) / SYMBOL_FACTOR;
        let rem = (o as usize) % SYMBOL_FACTOR;
        let mut dec = Decbuf::new(&self.bytes[self.offsets[block]..]);
        for _ in 0..rem {
            let _ = dec.uvarint_bytes()?;
        }
        Ok(dec.uvarint_str()?)
    }

    pub fn reverse_lookup(&self, sym: &str) -> Result<u32, IndexError> {
        reverse_lookup_symbol(&self.bytes, &self.offsets, self.seen, sym, self.version)
    }

    pub fn iter(&self) -> Result<Vec<&str>, IndexError> {
        let payload = checked_decbuf_at(&self.bytes, self.off)?;
        let mut dec = Decbuf::new(payload);
        let cnt = dec.be32()? as usize;
        let mut out = Vec::with_capacity(cnt.min(dec.len()));
        for _ in 0..cnt {
            out.push(dec.uvarint_str()?);
        }
        Ok(out)
    }
}

/// Planning interface matching `index.LookupPlan` in `tsdb/index/lookup.go`.
pub trait LookupPlan {
    fn scan_matchers(&self) -> &[LabelMatcher];
    fn index_matchers(&self) -> &[LabelMatcher];
}

#[derive(Clone, Debug)]
pub struct IndexOnlyLookupPlan(pub Vec<LabelMatcher>);

impl LookupPlan for IndexOnlyLookupPlan {
    fn scan_matchers(&self) -> &[LabelMatcher] {
        &[]
    }

    fn index_matchers(&self) -> &[LabelMatcher] {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct ConcreteLookupPlan {
    pub index_matchers: Vec<LabelMatcher>,
    pub scan_matchers: Vec<LabelMatcher>,
}

impl LookupPlan for ConcreteLookupPlan {
    fn scan_matchers(&self) -> &[LabelMatcher] {
        &self.scan_matchers
    }

    fn index_matchers(&self) -> &[LabelMatcher] {
        &self.index_matchers
    }
}

/// Trait for index lookup planners (`index.LookupPlanner` in `tsdb/index/lookup.go`).
pub trait IndexLookupPlanner: Send + Sync {
    fn plan_index_lookup(&self, plan: &dyn LookupPlan) -> Result<ConcreteLookupPlan, IndexError>;
}

/// Defers empty/non-selective matchers (`l=""`, `l=~".+"`) to scan matchers,
/// matching `index.ScanEmptyMatchersLookupPlanner` in `tsdb/index/lookup.go`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ScanEmptyMatchersLookupPlanner;

impl IndexLookupPlanner for ScanEmptyMatchersLookupPlanner {
    fn plan_index_lookup(&self, plan: &dyn LookupPlan) -> Result<ConcreteLookupPlan, IndexError> {
        let planned = ScanEmptyMatchersLookupPlanner::plan_index_lookup(self, plan);
        Ok(ConcreteLookupPlan {
            index_matchers: planned.index_matchers().to_vec(),
            scan_matchers: planned.scan_matchers().to_vec(),
        })
    }
}

impl ScanEmptyMatchersLookupPlanner {
    fn should_drop_matcher(matcher: &LabelMatcher) -> bool {
        matcher.match_type == MatchType::RegexMatch && matcher.value == ".*"
    }

    fn should_scan_matcher(matcher: &LabelMatcher) -> bool {
        (matcher.match_type == MatchType::Equal && matcher.value.is_empty())
            || (matcher.match_type == MatchType::RegexMatch && matcher.value == ".+")
    }

    fn can_optimize_index_matchers(matchers: &[LabelMatcher]) -> bool {
        matchers
            .iter()
            .any(|m| Self::should_scan_matcher(m) || Self::should_drop_matcher(m))
    }

    pub fn plan_index_lookup(&self, plan: &dyn LookupPlan) -> Box<dyn LookupPlan> {
        let orig_index = plan.index_matchers();
        if orig_index.len() <= 1 || !Self::can_optimize_index_matchers(orig_index) {
            return Box::new(ConcreteLookupPlan {
                index_matchers: orig_index.to_vec(),
                scan_matchers: plan.scan_matchers().to_vec(),
            });
        }
        let mut index_matchers = Vec::with_capacity(orig_index.len());
        let mut scan_matchers = plan.scan_matchers().to_vec();
        for matcher in orig_index {
            if Self::should_drop_matcher(matcher) {
                continue;
            }
            if Self::should_scan_matcher(matcher) {
                scan_matchers.push(matcher.clone());
            } else {
                index_matchers.push(matcher.clone());
            }
        }
        if index_matchers.is_empty() && !scan_matchers.is_empty() {
            index_matchers.push(scan_matchers.remove(0));
        }
        if scan_matchers.is_empty() {
            Box::new(IndexOnlyLookupPlan(index_matchers))
        } else {
            Box::new(ConcreteLookupPlan {
                index_matchers,
                scan_matchers,
            })
        }
    }
}

pub fn checked_uvarint_decbuf_at(buf: &[u8], off: usize) -> Result<&[u8], IndexError> {
    if off >= buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let mut dec = Decbuf::new(&buf[off..]);
    let len = dec.uvarint()?;
    let header_len = (buf.len() - off) - dec.len();
    let payload_start = off
        .checked_add(header_len)
        .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
    let payload_end = payload_start
        .checked_add(len)
        .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
    let crc_end = payload_end
        .checked_add(4)
        .ok_or(IndexError::Encoding(EncodingError::InvalidSize))?;
    if crc_end > buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let payload = &buf[payload_start..payload_end];
    let exp_crc = u32::from_be_bytes(buf[payload_end..crc_end].try_into().unwrap());
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != exp_crc {
        return Err(IndexError::ChecksumMismatch {
            expected: exp_crc,
            actual: actual_crc,
        });
    }
    Ok(payload)
}

#[inline]
fn write_stringlabel_slice(dst: &mut [u8; 512], mut pos: usize, bytes: &[u8]) -> usize {
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

fn parse_symbols_index(buf: &[u8], off: usize) -> Result<(Vec<usize>, usize), IndexError> {
    let payload = checked_decbuf_at(buf, off)?;
    let mut dec = Decbuf::new(payload);
    let cnt = dec.be32()? as usize;
    let base_pos = off + 4;
    let orig_len = payload.len();

    let cap = (1 + cnt / SYMBOL_FACTOR).min(1 + dec.len() / SYMBOL_FACTOR);
    let mut offsets = Vec::with_capacity(cap);
    for seen in 0..cnt {
        if seen % SYMBOL_FACTOR == 0 {
            offsets.push(base_pos + orig_len - dec.len());
        }
        // Validate UTF-8 and bounds once at IndexReader::open so lookup_symbol can skip UTF-8 checks.
        let sym = dec.uvarint_str()?;
        if sym.len() >= (1 << 24) {
            return Err(IndexError::Corrupt(format!(
                "symbol length {} exceeds max label length {}",
                sym.len(),
                (1 << 24) - 1
            )));
        }
    }
    Ok((offsets, cnt))
}

fn reverse_lookup_symbol(
    buf: &[u8],
    offsets: &[usize],
    num_symbols: usize,
    sym: &str,
    version: u8,
) -> Result<u32, IndexError> {
    if offsets.is_empty() {
        return Err(IndexError::UnknownSymbol(sym.to_string()));
    }
    let mut idx = offsets.partition_point(|&off| {
        let mut dec = Decbuf::new(&buf[off..]);
        dec.uvarint_str().unwrap_or("") <= sym
    });
    idx = idx.saturating_sub(1);
    let mut dec = Decbuf::new(&buf[offsets[idx]..]);
    let mut res = idx * SYMBOL_FACTOR;
    while res < num_symbols {
        let sym_off = buf.len() - dec.len();
        let s = dec.uvarint_str()?;
        if s == sym {
            if version == INDEX_FORMAT_V1 {
                let off = u32::try_from(sym_off)
                    .map_err(|_| IndexError::SymbolTableTooLarge(sym_off as u64))?;
                return Ok(off);
            }
            return Ok(res as u32);
        }
        if s > sym {
            break;
        }
        res += 1;
    }
    Err(IndexError::UnknownSymbol(sym.to_string()))
}

#[inline]
fn resolve_posting_str_range(payload_start: usize, off: u32, len: u32) -> (usize, usize) {
    let start = payload_start + (off as usize);
    let end = start + (len as usize);
    (start, end)
}

type V1PostingsMap = BTreeMap<String, BTreeMap<String, u64>>;
type V1NameOffsets = Vec<(String, (u32, u32))>;

fn parse_postings_offset_table_v1(
    buf: &[u8],
    table_off: usize,
) -> Result<(V1PostingsMap, V1NameOffsets), IndexError> {
    let payload = checked_decbuf_at(buf, table_off)?;
    let mut dec = Decbuf::new(payload);
    let orig_len = dec.len();
    let cnt = dec.be32()? as usize;

    let mut map: V1PostingsMap = BTreeMap::new();
    let mut name_offsets: V1NameOffsets = Vec::new();

    for _ in 0..cnt {
        let key_cnt = dec.uvarint()?;
        if key_cnt != 2 {
            return Err(IndexError::Corrupt(format!(
                "unexpected postings offset table key_count {key_cnt}"
            )));
        }
        let name = dec.uvarint_str()?;
        let name_end = orig_len - dec.len();
        let name_off = u32::try_from(name_end - name.len())
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let name_len = u32::try_from(name.len())
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;

        let value = dec.uvarint_str()?;
        let postings_off = dec.uvarint64()?;

        if !name.is_empty() && !map.contains_key(name) {
            name_offsets.push((name.to_string(), (name_off, name_len)));
        }
        map.entry(name.to_string())
            .or_default()
            .insert(value.to_string(), postings_off);
    }

    Ok((map, name_offsets))
}

fn parse_postings_offset_table(
    buf: &[u8],
    table_off: usize,
) -> Result<Vec<Box<[PostingOffsetEntry]>>, IndexError> {
    let payload = checked_decbuf_at(buf, table_off)?;
    parse_postings_offset_table_payload(payload)
}

fn parse_postings_offset_table_payload(
    payload: &[u8],
) -> Result<Vec<Box<[PostingOffsetEntry]>>, IndexError> {
    let mut dec = Decbuf::new(payload);
    let orig_len = dec.len();
    let cnt = dec.be32()? as usize;

    let mut result: BTreeMap<&str, Vec<PostingOffsetEntry>> = BTreeMap::new();
    let mut current_group_name: Option<&str> = None;
    let mut last_val: Option<&str> = None;
    let mut last_name: Option<&str> = None;
    let mut last_entry: Option<PostingOffsetEntry> = None;
    let mut value_count = 0usize;

    for _ in 0..cnt {
        let rel_off = orig_len - dec.len();
        let key_cnt = dec.uvarint()?;
        if key_cnt != 2 {
            return Err(IndexError::Corrupt(format!(
                "unexpected postings offset table key_count {key_cnt}"
            )));
        }
        let name = dec.uvarint_str()?;
        let name_end = orig_len - dec.len();
        let name_off = u32::try_from(name_end - name.len())
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let name_len =
            u32::try_from(name.len()).map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;

        let value = dec.uvarint_str()?;
        let val_end = orig_len - dec.len();
        let val_off = u32::try_from(val_end - value.len())
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;
        let val_len = u32::try_from(value.len())
            .map_err(|_| IndexError::Encoding(EncodingError::InvalidSize))?;

        let _postings_off = dec.uvarint64()?;

        let entry = PostingOffsetEntry {
            name_off,
            name_len,
            val_off,
            val_len,
            table_rel_off: rel_off,
        };

        if current_group_name != Some(name) {
            if result.contains_key(name) {
                return Err(IndexError::Corrupt(format!(
                    "non-contiguous label name {name:?} in postings offset table"
                )));
            }
            result.insert(name, Vec::new());
            if let Some(prev_name) = last_name.take() {
                if let (Some(prev_vec), Some(pending)) =
                    (result.get_mut(prev_name), last_entry.take())
                {
                    prev_vec.push(pending);
                }
            }
            current_group_name = Some(name);
            last_val = Some(value);
            value_count = 0;
        } else if let Some(prev_val) = last_val {
            if value <= prev_val {
                return Err(IndexError::Corrupt(format!(
                    "non-increasing label value {value:?} after {prev_val:?} for label {name:?} in postings offset table"
                )));
            }
            last_val = Some(value);
        }
        if value_count.is_multiple_of(SYMBOL_FACTOR) {
            if let Some(vec) = result.get_mut(name) {
                vec.push(entry);
            }
            last_name = None;
            last_entry = None;
        } else {
            last_name = Some(name);
            last_entry = Some(entry);
        }
        value_count += 1;
    }

    if let Some(prev_name) = last_name {
        if let (Some(prev_vec), Some(pending)) = (result.get_mut(prev_name), last_entry.take()) {
            prev_vec.push(pending);
        }
    }
    Ok(result.into_values().map(Vec::into_boxed_slice).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matcher::{LabelMatcher, MatchType};
    use crate::postings::postings_for_matchers;

    #[test]
    fn index_v2_write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join(INDEX_FILENAME);

        let l1 = Labels::from_pairs([("__name__", "http_requests"), ("job", "api")]);
        let l2 = Labels::from_pairs([("__name__", "http_requests"), ("job", "db")]);

        let syms = ["", "__name__", "api", "db", "http_requests", "job"];
        {
            let mut w = IndexWriter::new(&idx_path).unwrap();
            for s in syms {
                w.add_symbol(s).unwrap();
            }
            w.add_series(
                0,
                &l1,
                &[ChunkMeta {
                    ref_id: 8,
                    min_time: 1000,
                    max_time: 2000,
                    chunk: None,
                }],
            )
            .unwrap();
            w.add_series(
                1,
                &l2,
                &[
                    ChunkMeta {
                        ref_id: 40,
                        min_time: 1000,
                        max_time: 2000,
                        chunk: None,
                    },
                    ChunkMeta {
                        ref_id: 80,
                        min_time: 2001,
                        max_time: 3000,
                        chunk: None,
                    },
                ],
            )
            .unwrap();
            w.close().unwrap();
        }

        assert!(std::mem::size_of::<PostingOffsetEntry>() <= 24);
        let r = IndexReader::open(&idx_path).unwrap();
        assert_eq!(r.version(), INDEX_FORMAT_V2);
        assert_eq!(r.label_names(), vec!["__name__", "job"]);
        assert_eq!(r.label_values("job").unwrap(), vec!["api", "db"]);

        let all = r.all_postings().unwrap();
        assert_eq!(all.len(), 2);

        let (dec_l1, chks1) = r.series(all[0]).unwrap();
        assert_eq!(dec_l1, l1);
        assert_eq!(chks1.len(), 1);
        assert_eq!(chks1[0].ref_id, 8);
        assert_eq!(chks1[0].min_time, 1000);
        assert_eq!(chks1[0].max_time, 2000);

        let (dec_l2, chks2) = r.series(all[1]).unwrap();
        assert_eq!(dec_l2, l2);
        assert_eq!(chks2.len(), 2);
        assert_eq!(chks2[1].ref_id, 80);
        assert_eq!(chks2[1].min_time, 2001);
        assert_eq!(chks2[1].max_time, 3000);

        let m = LabelMatcher::new(MatchType::Equal, "job", "db").unwrap();
        let matched = postings_for_matchers(&r, &[m]).unwrap();
        assert_eq!(matched, vec![all[1]]);
    }

    #[test]
    fn index_rejects_touching_and_overlapping_chunk_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join(INDEX_FILENAME);
        let l = Labels::from_pairs([("__name__", "m")]);
        let mut w = IndexWriter::new(&idx_path).unwrap();
        w.add_symbol("").unwrap();
        w.add_symbol("__name__").unwrap();
        w.add_symbol("m").unwrap();
        // Touching boundaries (c2.min_time == c1.max_time) must be rejected (Go index.go:463).
        let err_touch = w
            .add_series(
                0,
                &l,
                &[
                    ChunkMeta {
                        ref_id: 1,
                        min_time: 100,
                        max_time: 200,
                        chunk: None,
                    },
                    ChunkMeta {
                        ref_id: 2,
                        min_time: 200,
                        max_time: 300,
                        chunk: None,
                    },
                ],
            )
            .unwrap_err();
        assert!(matches!(err_touch, IndexError::InvalidChunkOrder(_)));

        // Empty labels on first series must be rejected.
        let err_empty = w.add_series(0, &Labels::empty(), &[]).unwrap_err();
        assert!(matches!(err_empty, IndexError::OutOfOrderSeries(_)));

        // Overlapping (c2.min_time < c1.max_time) must be rejected.
        let idx_path2 = dir.path().join("index2");
        let mut w2 = IndexWriter::new(&idx_path2).unwrap();
        w2.add_symbol("").unwrap();
        w2.add_symbol("__name__").unwrap();
        w2.add_symbol("m").unwrap();
        let err = w2
            .add_series(
                0,
                &l,
                &[
                    ChunkMeta {
                        ref_id: 1,
                        min_time: 100,
                        max_time: 200,
                        chunk: None,
                    },
                    ChunkMeta {
                        ref_id: 2,
                        min_time: 199,
                        max_time: 300,
                        chunk: None,
                    },
                ],
            )
            .unwrap_err();
        assert!(matches!(err, IndexError::InvalidChunkOrder(_)));
    }

    #[test]
    fn postings_stops_at_last_anchor_without_overrunning_into_next_label() {
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join(INDEX_FILENAME);
        let l1 = Labels::from_pairs([("__name__", "m_all"), ("i", "s0")]);
        let syms = ["", "__name__", "i", "m_all", "s0"];
        {
            let mut w = IndexWriter::new(&idx_path).unwrap();
            for s in syms {
                w.add_symbol(s).unwrap();
            }
            w.add_series(
                0,
                &l1,
                &[ChunkMeta {
                    ref_id: 8,
                    min_time: 1000,
                    max_time: 2000,
                    chunk: None,
                }],
            )
            .unwrap();
            w.close().unwrap();
        }
        let r = IndexReader::open(&idx_path).unwrap();
        // Querying an existing value plus lexicographically larger missing values must not overrun
        // from `__name__` into label `i` inside the postings offset table.
        let p = r
            .postings("__name__", &["m_all", "m_blk1_only", "m_head_only"])
            .unwrap();
        assert_eq!(p.len(), 1);
        let (decoded, _) = r.series(p[0]).unwrap();
        assert_eq!(decoded, l1);
    }

    #[test]
    fn corrupt_index_headers_do_not_oom_or_panic() {
        // 1) checked_decbuf_at and checked_uvarint_decbuf_at overflow lengths must return Err, not panic.
        let mut huge_len = vec![0u8; 16];
        huge_len[0..4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(checked_decbuf_at(&huge_len, 0).is_err());
        assert!(checked_decbuf_at(&huge_len, usize::MAX - 2).is_err());

        let mut huge_uvarint = vec![0xffu8; 12];
        huge_uvarint[9] = 0x01; // uvarint > usize::MAX - 20 or near u64::MAX
        assert!(checked_uvarint_decbuf_at(&huge_uvarint, 0).is_err());

        // 2) parse_symbols_index with 12-byte corrupt section (cnt = u32::MAX, valid CRC32C) must not OOM.
        let payload = u32::MAX.to_be_bytes();
        let crc = crc32c::crc32c(&payload);
        let mut sym_buf = Vec::new();
        sym_buf.extend_from_slice(&4u32.to_be_bytes());
        sym_buf.extend_from_slice(&payload);
        sym_buf.extend_from_slice(&crc.to_be_bytes());
        assert!(parse_symbols_index(&sym_buf, 0).is_err());

        // 3) Corrupt IndexReader file on disk with invalid TOC / huge symbol count fails cleanly.
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join(INDEX_FILENAME);
        let mut file_bytes = Vec::new();
        file_bytes.extend_from_slice(&MAGIC_INDEX.to_be_bytes());
        file_bytes.push(INDEX_FORMAT_V2);
        let sym_off = file_bytes.len() as u64;
        file_bytes.extend_from_slice(&sym_buf);
        let mut toc = Vec::new();
        toc.extend_from_slice(&sym_off.to_be_bytes());
        for _ in 0..5 {
            toc.extend_from_slice(&0u64.to_be_bytes());
        }
        let toc_crc = crc32c::crc32c(&toc);
        file_bytes.extend_from_slice(&toc);
        file_bytes.extend_from_slice(&toc_crc.to_be_bytes());
        fs::write(&idx_path, &file_bytes).unwrap();
        assert!(IndexReader::open(&idx_path).is_err());

        // 4) Valid IndexReader::series on a corrupt series record with huge num_chunks does not OOM.
        let idx_path2 = dir.path().join("index_corrupt_series");
        let l = Labels::from_pairs([("__name__", "m")]);
        {
            let mut w = IndexWriter::new(&idx_path2).unwrap();
            for s in ["", "__name__", "m"] {
                w.add_symbol(s).unwrap();
            }
            w.add_series(
                0,
                &l,
                &[ChunkMeta {
                    ref_id: 1,
                    min_time: 100,
                    max_time: 200,
                    chunk: None,
                }],
            )
            .unwrap();
            w.close().unwrap();
        }
        let reader = IndexReader::open(&idx_path2).unwrap();
        let p = reader.postings("__name__", &["m"]).unwrap();
        assert_eq!(p.len(), 1);
        // Corrupt the series record's num_chunks field and recompute its CRC in a fresh file.
        let mut raw = fs::read(&idx_path2).unwrap();
        let off = (p[0] as usize) * SERIES_BYTE_ALIGN;
        let len_uvarint_len = 1; // small series payload
        let payload_len = raw[off] as usize;
        let pstart = off + len_uvarint_len;
        let pend = pstart + payload_len;
        // Overwrite tail bytes inside payload with 0xff.. to make num_chunks huge (or invalid) and fix CRC.
        if pend - pstart >= 5 {
            for b in &mut raw[pend - 4..pend] {
                *b = 0xff;
            }
            let new_crc = crc32c::crc32c(&raw[pstart..pend]);
            raw[pend..pend + 4].copy_from_slice(&new_crc.to_be_bytes());
            let idx_path3 = dir.path().join("index_corrupt_series2");
            fs::write(&idx_path3, &raw).unwrap();
            let r3 = IndexReader::open(&idx_path3).unwrap();
            assert!(r3.series(p[0]).is_err());
        }
    }

    #[test]
    fn index_unsigned_chunk_time_delta_spanning_i64_min_to_positive() {
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join("index_wide_span");
        let l = Labels::from_pairs([("__name__", "wide_span")]);
        let chunks = vec![
            ChunkMeta {
                ref_id: 10,
                min_time: i64::MIN + 1000,
                max_time: 1_700_000_000_000,
                chunk: None,
            },
            ChunkMeta {
                ref_id: 20,
                min_time: i64::MAX - 100,
                max_time: i64::MAX,
                chunk: None,
            },
        ];
        {
            let mut w = IndexWriter::new(&idx_path).unwrap();
            for s in ["", "__name__", "wide_span"] {
                w.add_symbol(s).unwrap();
            }
            w.add_series(0, &l, &chunks).unwrap();
            w.close().unwrap();
        }
        let r = IndexReader::open(&idx_path).unwrap();
        let p = r.postings("__name__", &["wide_span"]).unwrap();
        assert_eq!(p.len(), 1);
        let (got_lbls, got_chunks) = r.series(p[0]).unwrap();
        assert_eq!(got_lbls, l);
        assert_eq!(got_chunks.len(), 2);
        assert_eq!(got_chunks[0].min_time, i64::MIN + 1000);
        assert_eq!(got_chunks[0].max_time, 1_700_000_000_000);
        assert_eq!(got_chunks[1].min_time, i64::MAX - 100);
        assert_eq!(got_chunks[1].max_time, i64::MAX);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn postings_offset_table_relative_offsets_above_4gib() {
        let mut payload = Encbuf::new();
        let records = [
            ("__name__", "http_requests_total", 100u64),
            ("job", "api", 200u64),
            ("job", "db", 300u64),
        ];
        payload.put_be32(records.len() as u32);
        for &(name, val, off) in &records {
            payload.put_uvarint(2);
            payload.put_uvarint_str(name);
            payload.put_uvarint_str(val);
            payload.put_uvarint64(off);
        }
        let payload_bytes = payload.as_slice();

        let entries = parse_postings_offset_table_payload(payload_bytes).unwrap();
        assert_eq!(entries.len(), 2);

        // Simulate a >= 4 GiB ULID index file where toc.postings_table + 4 > u32::MAX.
        let sim_payload_start = (u32::MAX as usize) + 64;

        let e_name = entries[0][0];
        let (ns, ne) = resolve_posting_str_range(sim_payload_start, e_name.name_off, e_name.name_len);
        let (vs, ve) = resolve_posting_str_range(sim_payload_start, e_name.val_off, e_name.val_len);
        assert!(ns > u32::MAX as usize);
        assert!(vs > u32::MAX as usize);
        assert_eq!(
            std::str::from_utf8(&payload_bytes[ns - sim_payload_start..ne - sim_payload_start]).unwrap(),
            "__name__"
        );
        assert_eq!(
            std::str::from_utf8(&payload_bytes[vs - sim_payload_start..ve - sim_payload_start]).unwrap(),
            "http_requests_total"
        );

        let e_job = entries[1][0];
        let (ns2, ne2) = resolve_posting_str_range(sim_payload_start, e_job.name_off, e_job.name_len);
        let (vs2, ve2) = resolve_posting_str_range(sim_payload_start, e_job.val_off, e_job.val_len);
        assert!(ns2 > u32::MAX as usize);
        assert!(vs2 > u32::MAX as usize);
        assert_eq!(
            std::str::from_utf8(&payload_bytes[ns2 - sim_payload_start..ne2 - sim_payload_start]).unwrap(),
            "job"
        );
        assert_eq!(
            std::str::from_utf8(&payload_bytes[vs2 - sim_payload_start..ve2 - sim_payload_start]).unwrap(),
            "api"
        );
    }

    struct FormatV1TestInput {
        symbols: Vec<String>,
        series: Vec<(Labels, Vec<ChunkMeta>)>,
        unsorted_postings: bool,
    }

    fn build_format_v1_index_bytes(input: &FormatV1TestInput) -> (Vec<u8>, Vec<u64>, HashMap<String, u32>) {
        let mut syms = input.symbols.clone();
        syms.sort();
        syms.dedup();

        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC_INDEX.to_be_bytes());
        buf.push(INDEX_FORMAT_V1);

        // Symbols section: 4B len + [4B cnt + uvarint_str...] + 4B CRC32C.
        let toc_symbols = buf.len() as u64;
        let mut sym_payload = Encbuf::new();
        sym_payload.put_be32(syms.len() as u32);
        let mut sym_rel_offsets = Vec::with_capacity(syms.len());
        for s in &syms {
            sym_rel_offsets.push(sym_payload.len());
            sym_payload.put_uvarint_str(s);
        }
        let sym_crc = crc32c::crc32c(sym_payload.as_slice());
        let sym_payload_start = buf.len() + 4;
        buf.extend_from_slice(&(sym_payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(sym_payload.as_slice());
        buf.extend_from_slice(&sym_crc.to_be_bytes());

        let mut sym_file_offsets = HashMap::with_capacity(syms.len());
        for (s, rel) in syms.iter().zip(sym_rel_offsets) {
            sym_file_offsets.insert(s.clone(), (sym_payload_start + rel) as u32);
        }

        // Deliberately write a 3-byte filler so series offsets are NOT 16-byte aligned.
        buf.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let toc_series = buf.len() as u64;

        let mut series_refs = Vec::with_capacity(input.series.len());
        let mut postings_map: Vec<((String, String), Vec<u32>)> = Vec::new();
        let mut postings_acc: BTreeMap<(String, String), Vec<u32>> = BTreeMap::new();

        for (lbls, chks) in &input.series {
            // In FormatV1, SeriesRef is the exact byte offset (unaligned).
            let series_off = buf.len() as u64;
            assert!(
                !series_off.is_multiple_of(SERIES_BYTE_ALIGN as u64),
                "expected non-16-aligned V1 series offset, got {series_off}"
            );
            series_refs.push(series_off);

            let mut sbuf = Encbuf::new();
            sbuf.put_uvarint(lbls.len());
            for (k, v) in lbls.iter() {
                let ko = *sym_file_offsets.get(k).expect("missing label name symbol");
                let vo = *sym_file_offsets.get(v).expect("missing label value symbol");
                sbuf.put_uvarint(ko as usize);
                sbuf.put_uvarint(vo as usize);
            }
            sbuf.put_uvarint(chks.len());
            if !chks.is_empty() {
                sbuf.put_varint64(chks[0].min_time);
                sbuf.put_uvarint64((chks[0].max_time - chks[0].min_time) as u64);
                sbuf.put_uvarint64(chks[0].ref_id);
                let mut prev_maxt = chks[0].max_time;
                let mut prev_ref = chks[0].ref_id as i64;
                for c in &chks[1..] {
                    sbuf.put_uvarint64((c.min_time - prev_maxt) as u64);
                    sbuf.put_uvarint64((c.max_time - c.min_time) as u64);
                    sbuf.put_varint64((c.ref_id as i64) - prev_ref);
                    prev_maxt = c.max_time;
                    prev_ref = c.ref_id as i64;
                }
            }
            let mut len_hdr = Encbuf::new();
            len_hdr.put_uvarint(sbuf.len());
            let scrc = crc32c::crc32c(sbuf.as_slice());
            buf.extend_from_slice(len_hdr.as_slice());
            buf.extend_from_slice(sbuf.as_slice());
            buf.extend_from_slice(&scrc.to_be_bytes());
            // Deliberately add 1 byte padding between series so subsequent series also stay unaligned if needed.
            buf.push(0x00);

            let sid = series_off as u32;
            postings_acc
                .entry((String::new(), String::new()))
                .or_default()
                .push(sid);
            for (k, v) in lbls.iter() {
                postings_acc
                    .entry((k.to_string(), v.to_string()))
                    .or_default()
                    .push(sid);
            }
        }

        for (kv, sids) in postings_acc {
            postings_map.push((kv, sids));
        }
        if input.unsorted_postings {
            postings_map.reverse();
        }

        let toc_label_indices = buf.len() as u64;
        let toc_postings = buf.len() as u64;
        let mut postings_offsets = Vec::with_capacity(postings_map.len());
        for ((k, v), sids) in postings_map {
            let p_off = buf.len() as u64;
            let mut pbuf = Encbuf::new();
            pbuf.put_be32(sids.len() as u32);
            for sid in sids {
                pbuf.put_be32(sid);
            }
            let pcrc = crc32c::crc32c(pbuf.as_slice());
            buf.extend_from_slice(&(pbuf.len() as u32).to_be_bytes());
            buf.extend_from_slice(pbuf.as_slice());
            buf.extend_from_slice(&pcrc.to_be_bytes());
            postings_offsets.push((k, v, p_off));
        }

        let toc_label_indices_table = buf.len() as u64;
        let toc_postings_table = buf.len() as u64;
        let mut pt_buf = Encbuf::new();
        pt_buf.put_be32(postings_offsets.len() as u32);
        for (k, v, off) in &postings_offsets {
            pt_buf.put_uvarint(2);
            pt_buf.put_uvarint_str(k);
            pt_buf.put_uvarint_str(v);
            pt_buf.put_uvarint64(*off);
        }
        let pt_crc = crc32c::crc32c(pt_buf.as_slice());
        buf.extend_from_slice(&(pt_buf.len() as u32).to_be_bytes());
        buf.extend_from_slice(pt_buf.as_slice());
        buf.extend_from_slice(&pt_crc.to_be_bytes());

        let mut toc = Encbuf::new();
        toc.put_be64(toc_symbols);
        toc.put_be64(toc_series);
        toc.put_be64(toc_label_indices);
        toc.put_be64(toc_label_indices_table);
        toc.put_be64(toc_postings);
        toc.put_be64(toc_postings_table);
        toc.put_crc32c();
        buf.extend_from_slice(toc.as_slice());

        (buf, series_refs, sym_file_offsets)
    }

    #[test]
    fn index_v1_read_symbols_unaligned_series_and_unsorted_postings() {
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join(INDEX_FILENAME);

        // > 32 symbols so checkpointing & ReverseLookup across multiple 32-symbol blocks are exercised.
        let mut syms: Vec<String> = (0..45).map(|i| format!("sym_{i:02}")).collect();
        syms.extend(
            [
                "",
                "__name__",
                "env",
                "http_requests",
                "job",
                "prod",
                "staging",
                "web_api",
                "worker",
                "z_metric",
            ]
            .into_iter()
            .map(String::from),
        );

        let l1 = Labels::from_pairs([
            ("__name__", "http_requests"),
            ("env", "prod"),
            ("job", "web_api"),
        ]);
        let l2 = Labels::from_pairs([
            ("__name__", "z_metric"),
            ("env", "staging"),
            ("job", "worker"),
        ]);
        let l3 = Labels::from_pairs([
            ("__name__", "http_requests"),
            ("env", "prod"),
            ("job", "worker"),
        ]);

        let (bytes, expected_refs, sym_offsets) = build_format_v1_index_bytes(&FormatV1TestInput {
            symbols: syms.clone(),
            series: vec![
                (
                    l1.clone(),
                    vec![ChunkMeta {
                        ref_id: 11,
                        min_time: 1_000,
                        max_time: 2_000,
                        chunk: None,
                    }],
                ),
                (
                    l2.clone(),
                    vec![
                        ChunkMeta {
                            ref_id: 22,
                            min_time: 1_000,
                            max_time: 2_000,
                            chunk: None,
                        },
                        ChunkMeta {
                            ref_id: 33,
                            min_time: 2_001,
                            max_time: 3_000,
                            chunk: None,
                        },
                    ],
                ),
                (
                    l3.clone(),
                    vec![ChunkMeta {
                        ref_id: 44,
                        min_time: 3_001,
                        max_time: 4_000,
                        chunk: None,
                    }],
                ),
            ],
            unsorted_postings: true,
        });
        fs::write(&idx_path, &bytes).unwrap();

        let r = IndexReader::open(&idx_path).unwrap();
        assert_eq!(r.version(), INDEX_FORMAT_V1);
        // 55 unique symbols -> ceil(55 / 32) = 2 checkpoints * 8 bytes = 16.
        assert_eq!(r.symbol_table_size(), 16);
        assert_eq!(r.symbols().unwrap().len(), 55);

        // V1 lookup_symbol takes direct byte offsets (including symbols in the second 32-symbol chunk).
        for s in ["", "__name__", "sym_00", "sym_35", "sym_44", "z_metric"] {
            let off = sym_offsets[s];
            assert_eq!(r.lookup_symbol(off).unwrap(), s);
        }

        // Unsorted postings table must still yield sorted label_names and label_values.
        assert_eq!(r.label_names(), vec!["__name__", "env", "job"]);
        assert_eq!(r.label_values("job").unwrap(), vec!["web_api", "worker"]);
        assert_eq!(r.label_values("env").unwrap(), vec!["prod", "staging"]);
        assert_eq!(r.label_values("").unwrap(), vec![""]);
        assert!(r.label_values("nonexistent").unwrap().is_empty());

        let all = r.all_postings().unwrap();
        assert_eq!(all, expected_refs);

        // Series lookup using direct unaligned byte-offset SeriesRefs.
        let (d1, c1) = r.series(all[0]).unwrap();
        assert_eq!(d1, l1);
        assert_eq!(c1.len(), 1);
        assert_eq!(c1[0].ref_id, 11);

        let (d2, c2) = r.series(all[1]).unwrap();
        assert_eq!(d2, l2);
        assert_eq!(c2.len(), 2);
        assert_eq!(c2[0].ref_id, 22);
        assert_eq!(c2[1].ref_id, 33);

        assert_eq!(r.series_labels(all[2]).unwrap(), l3);
        assert_eq!(
            r.label_names_for(&[all[0]]).unwrap(),
            vec!["__name__", "env", "job"]
        );

        assert_eq!(r.postings("job", &["worker"]).unwrap(), vec![all[1], all[2]]);
        assert_eq!(r.postings("job", &["worker", "web_api"]).unwrap(), all);
        assert_eq!(r.postings_for_all_values("env").unwrap(), all);
        assert_eq!(
            r.postings_for_label_matching("job", &mut |v| v.starts_with("web"))
                .unwrap(),
            vec![all[0]]
        );

        let m = LabelMatcher::new(MatchType::Equal, "env", "prod").unwrap();
        assert_eq!(postings_for_matchers(&r, &[m]).unwrap(), vec![all[0], all[2]]);
    }

    #[test]
    fn index_v1_invalid_offsets_return_err_without_panic() {
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join(INDEX_FILENAME);

        let l1 = Labels::from_pairs([("__name__", "m1")]);
        let (bytes, refs, _) = build_format_v1_index_bytes(&FormatV1TestInput {
            symbols: vec!["".into(), "__name__".into(), "m1".into()],
            series: vec![(
                l1,
                vec![ChunkMeta {
                    ref_id: 1,
                    min_time: 100,
                    max_time: 200,
                    chunk: None,
                }],
            )],
            unsorted_postings: false,
        });
        fs::write(&idx_path, &bytes).unwrap();

        let r = IndexReader::open(&idx_path).unwrap();
        assert!(matches!(
            r.lookup_symbol(u32::MAX),
            Err(IndexError::UnknownSymbolOffset(u32::MAX))
        ));
        assert!(r.series(refs[0] + 1).is_err());
        assert!(r.series(u64::MAX).is_err());
    }

    #[test]
    fn corrupt_interleaved_postings_offset_table_rejected_without_loop_c4() {
        // Exact 26-byte corrupt payload PoC from Scope C (C-4):
        // cnt = 3: ("aa", "a", 0), ("bb\t", "b", 0), ("aa", "z", 0).
        let payload: [u8; 26] = [
            0x00, 0x00, 0x00, 0x03, // cnt = 3
            0x02, 0x02, b'a', b'a', 0x01, b'a', 0x00, // ("aa", "a", 0)
            0x02, 0x03, b'b', b'b', 0x09, 0x01, b'b', 0x00, // ("bb\t", "b", 0)
            0x02, 0x02, b'a', b'a', 0x01, b'z', 0x00, // ("aa", "z", 0)
        ];
        assert!(matches!(
            parse_postings_offset_table_payload(&payload),
            Err(IndexError::Corrupt(_))
        ));

        // Non-strictly-increasing label values for the same label name must also be rejected.
        let mut non_mono = Encbuf::new();
        non_mono.put_be32(2);
        non_mono.put_uvarint(2);
        non_mono.put_uvarint_str("job");
        non_mono.put_uvarint_str("z");
        non_mono.put_uvarint64(0);
        non_mono.put_uvarint(2);
        non_mono.put_uvarint_str("job");
        non_mono.put_uvarint_str("a");
        non_mono.put_uvarint64(0);
        assert!(matches!(
            parse_postings_offset_table_payload(non_mono.as_slice()),
            Err(IndexError::Corrupt(_))
        ));

        let mut dup_val = Encbuf::new();
        dup_val.put_be32(2);
        dup_val.put_uvarint(2);
        dup_val.put_uvarint_str("job");
        dup_val.put_uvarint_str("a");
        dup_val.put_uvarint64(0);
        dup_val.put_uvarint(2);
        dup_val.put_uvarint_str("job");
        dup_val.put_uvarint_str("a");
        dup_val.put_uvarint64(0);
        assert!(matches!(
            parse_postings_offset_table_payload(dup_val.as_slice()),
            Err(IndexError::Corrupt(_))
        ));
    }

    #[test]
    fn add_series_unknown_symbol_error_does_not_corrupt_prev_label_cache_or_counts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(INDEX_FILENAME);
        let mut w = IndexWriter::new(&path).unwrap();
        for sym in ["", "a", "b", "bar", "foo", "goo", "qux"] {
            w.add_symbol(sym).unwrap();
        }

        let s1 = Labels::from_pairs([("a", "foo"), ("b", "bar")]);
        let s2_bad = Labels::from_pairs([("a", "goo"), ("c", "MISSING_SYM")]);
        let s3 = Labels::from_pairs([("a", "foo"), ("b", "qux")]);

        w.add_series(1, &s1, &[]).unwrap();
        assert!(matches!(
            w.add_series(2, &s2_bad, &[]),
            Err(IndexError::UnknownSymbol(_))
        ));
        w.add_series(3, &s3, &[]).unwrap();
        w.close().unwrap();

        let r = IndexReader::open(&path).unwrap();
        let sids = r.postings("", &[""]).unwrap();
        assert_eq!(sids.len(), 2);
        let (lbl1, _) = r.series(sids[0]).unwrap();
        let (lbl3, _) = r.series(sids[1]).unwrap();
        assert_eq!(lbl1, s1);
        assert_eq!(lbl3, s3, "prev_label_cache must not poison S3 with failed S2 symbol");
    }
}
