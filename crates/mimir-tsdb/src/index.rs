//! Bit-exact Prometheus TSDB Index v2 writer & memory-mapped reader.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/index/index.go.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use memmap2::{Mmap, MmapOptions};
use thiserror::Error;

use crate::chunks_block::ChunkMeta;
use crate::encoding::{Decbuf, Encbuf, EncodingError};
use crate::labels::Labels;
use crate::postings::{merge_postings, PostingsError, PostingsSource};

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
    last_symbol: String,
    symbols: Vec<String>,
    symbol_index: HashMap<String, u32>,

    label_names: BTreeMap<String, u64>,
    last_series: Option<Labels>,
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
            last_symbol: String::new(),
            symbols: Vec::new(),
            symbol_index: HashMap::with_capacity(256),
            label_names: BTreeMap::new(),
            last_series: None,
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
        let bytes = self.buf1.as_slice().to_vec();
        self.write_main(&bytes)
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
                self.toc.label_indices = self.pos;
                self.write_postings_to_tmp_files()?;
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
        if self.num_symbols != 0 && sym <= self.last_symbol.as_str() {
            return Err(IndexError::OutOfOrderSymbol(sym.to_string()));
        }
        let idx = self.num_symbols;
        self.last_symbol.clear();
        self.last_symbol.push_str(sym);
        self.symbols.push(sym.to_string());
        self.symbol_index.insert(sym.to_string(), idx);
        self.num_symbols += 1;

        self.buf1.reset();
        self.buf1.put_uvarint_str(sym);
        let bytes = self.buf1.as_slice().to_vec();
        self.write_main(&bytes)
    }

    fn finish_symbols(&mut self) -> Result<(), IndexError> {
        let sym_table_size = (self.pos - self.toc.symbols - 4) as u32;
        self.buf1.reset();
        self.buf1.put_be32(sym_table_size);
        self.buf1.put_be32(self.num_symbols);
        let hdr = self.buf1.as_slice().to_vec();
        self.write_main_at(&hdr, self.toc.symbols)?;

        // Compute CRC32C over [toc.symbols + 4 .. self.pos] (4B numSymbols + all uvarintStr symbols).
        let hash_pos = self.pos;
        let f = self
            .f
            .as_mut()
            .ok_or(IndexError::InvalidStage("writer closed"))?;
        f.flush()?;
        let raw_file = f.get_mut();
        raw_file.seek(SeekFrom::Start(self.toc.symbols + 4))?;
        let len = (hash_pos - (self.toc.symbols + 4)) as usize;
        let mut tmp = vec![0u8; len];
        raw_file.read_exact(&mut tmp)?;
        raw_file.seek(SeekFrom::End(0))?;

        let crc = crc32c::crc32c(&tmp);
        self.write_main(&crc.to_be_bytes())
    }

    /// Adds a series with its chunk metadata. Must be called in strictly ascending label order!
    pub fn add_series(
        &mut self,
        _ref_id: u64,
        labels: &Labels,
        chunks: &[ChunkMeta],
    ) -> Result<(), IndexError> {
        self.ensure_stage(WriterStage::Series)?;
        if let Some(ref prev) = self.last_series {
            if labels <= prev {
                return Err(IndexError::OutOfOrderSeries(labels.to_string()));
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

        self.add_padding(SERIES_BYTE_ALIGN)?;
        debug_assert_eq!(self.pos % (SERIES_BYTE_ALIGN as u64), 0);

        self.buf2.reset();
        self.buf2.put_uvarint(labels.len());

        for (name, value) in labels.iter() {
            let name_idx = *self
                .symbol_index
                .get(name)
                .ok_or_else(|| IndexError::UnknownSymbol(name.to_string()))?;
            let val_idx = *self
                .symbol_index
                .get(value)
                .ok_or_else(|| IndexError::UnknownSymbol(value.to_string()))?;
            *self.label_names.entry(name.to_string()).or_insert(0) += 1;
            self.buf2.put_uvarint32(name_idx);
            self.buf2.put_uvarint32(val_idx);
        }

        self.buf2.put_uvarint(chunks.len());
        if let Some(c0) = chunks.first() {
            self.buf2.put_varint64(c0.min_time);
            self.buf2.put_uvarint64((c0.max_time - c0.min_time) as u64);
            self.buf2.put_uvarint64(c0.ref_id);
            let mut t0 = c0.max_time;
            let mut ref0 = c0.ref_id as i64;
            for c in &chunks[1..] {
                self.buf2.put_uvarint64((c.min_time - t0) as u64);
                self.buf2.put_uvarint64((c.max_time - c.min_time) as u64);
                t0 = c.max_time;
                self.buf2.put_varint64((c.ref_id as i64) - ref0);
                ref0 = c.ref_id as i64;
            }
        }

        self.buf1.reset();
        self.buf1.put_uvarint(self.buf2.len());
        self.buf2.put_crc32c();

        let len_bytes = self.buf1.as_slice().to_vec();
        let body_bytes = self.buf2.as_slice().to_vec();
        self.write_main(&len_bytes)?;
        self.write_main(&body_bytes)?;

        self.last_series = Some(labels.clone());
        self.last_chunk_ref = last_chunk_ref;
        Ok(())
    }

    fn write_postings_to_tmp_files(&mut self) -> Result<(), IndexError> {
        let f = self
            .f
            .as_mut()
            .ok_or(IndexError::InvalidStage("writer closed"))?;
        f.flush()?;

        let file_for_mmap = File::open(&self.path)?;
        // SAFETY: only reading up to `self.toc.label_indices` which is already flushed.
        let mmap = unsafe { MmapOptions::new().map(&file_for_mmap)? };
        let series_region = &mmap[..self.toc.label_indices as usize];

        // 1. Collect all series offsets (series_id = start_pos / 16) for ("", "").
        let mut all_offsets = Vec::new();
        let mut cur = self.toc.series as usize;
        while cur < series_region.len() {
            while cur < series_region.len() && series_region[cur] == 0 {
                cur += 1;
            }
            if cur >= series_region.len() {
                break;
            }
            if !cur.is_multiple_of(SERIES_BYTE_ALIGN) {
                return Err(IndexError::Corrupt(format!(
                    "series not 16-byte aligned at {cur}"
                )));
            }
            all_offsets.push((cur / SERIES_BYTE_ALIGN) as u32);
            let mut dec = Decbuf::new(&series_region[cur..]);
            let entry_len = dec.uvarint()?;
            let consumed = (series_region.len() - cur) - dec.len();
            cur += consumed + entry_len + 4;
        }

        self.write_posting("", "", &all_offsets)?;
        let max_postings = all_offsets.len() as u64;

        // 2. Batch label names and scan series region to populate postings.
        let names: Vec<String> = self.label_names.keys().cloned().collect();
        let mut name_idx = 0usize;
        while name_idx < names.len() {
            let mut batch_count = 0u64;
            let batch_start = name_idx;
            while name_idx < names.len() {
                let cnt = self.label_names[&names[name_idx]];
                if cnt + batch_count > max_postings {
                    if batch_count > 0 {
                        break;
                    }
                    return Err(IndexError::Corrupt(format!(
                        "label {:?} has {cnt} uses > max_postings {max_postings}",
                        names[name_idx]
                    )));
                }
                batch_count += cnt;
                name_idx += 1;
            }

            let batch_names = &names[batch_start..name_idx];
            let mut wanted_name_ids: HashSet<u32> = HashSet::with_capacity(batch_names.len());
            for name in batch_names {
                let sid = *self
                    .symbol_index
                    .get(name)
                    .ok_or_else(|| IndexError::UnknownSymbol(name.clone()))?;
                wanted_name_ids.insert(sid);
            }

            let mut postings: HashMap<u32, BTreeMap<u32, Vec<u32>>> = HashMap::new();
            let mut cur = self.toc.series as usize;
            while cur < series_region.len() {
                while cur < series_region.len() && series_region[cur] == 0 {
                    cur += 1;
                }
                if cur >= series_region.len() {
                    break;
                }
                let series_id = (cur / SERIES_BYTE_ALIGN) as u32;
                let mut dec = Decbuf::new(&series_region[cur..]);
                let entry_len = dec.uvarint()?;
                let header_consumed = (series_region.len() - cur) - dec.len();
                let body_start_len = dec.len();
                let num_labels = dec.uvarint()?;
                for _ in 0..num_labels {
                    let lno = dec.uvarint32()?;
                    let lvo = dec.uvarint32()?;
                    if wanted_name_ids.contains(&lno) {
                        postings
                            .entry(lno)
                            .or_default()
                            .entry(lvo)
                            .or_default()
                            .push(series_id);
                    }
                }
                let _ = body_start_len;
                cur += header_consumed + entry_len + 4;
            }

            for name in batch_names {
                let sid = self.symbol_index[name];
                if let Some(val_map) = postings.get(&sid) {
                    for (&val_sid, offs) in val_map {
                        let val_str = self.symbols[val_sid as usize].clone();
                        self.write_posting(name, &val_str, offs)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn write_posting(&mut self, name: &str, value: &str, offs: &[u32]) -> Result<(), IndexError> {
        // Align postings entry to 4 bytes.
        let rem = (self.fp_pos as usize) % 4;
        if rem != 0 {
            let pad = 4 - rem;
            let zeros = [0u8; 4];
            let fp = self.fp.as_mut().unwrap();
            fp.write_all(&zeros[..pad])?;
            self.fp_pos += pad as u64;
        }

        // Write offset table entry to temporary fpo.
        self.buf1.reset();
        self.buf1.put_uvarint(2);
        self.buf1.put_uvarint_str(name);
        self.buf1.put_uvarint_str(value);
        self.buf1.put_uvarint64(self.fp_pos);
        {
            let fpo = self.fpo.as_mut().unwrap();
            fpo.write_all(self.buf1.as_slice())?;
            self.fpo_pos += self.buf1.len() as u64;
        }
        self.cnt_po += 1;

        // Encode raw postings: <4B BE len><4B BE count><count * 4B BE off><4B BE crc32c>
        self.buf1.reset();
        self.buf1.put_be32(offs.len() as u32);
        for &off in offs {
            self.buf1.put_be32(off);
        }
        let body_len = self.buf1.len() as u32;
        self.buf1.put_crc32c();

        let fp = self.fp.as_mut().unwrap();
        fp.write_all(&body_len.to_be_bytes())?;
        fp.write_all(self.buf1.as_slice())?;
        self.fp_pos += 4 + self.buf1.len() as u64;
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
            let slice = self.buf1.as_slice().to_vec();
            crc = crc32c::crc32c_append(crc, &slice);
            self.write_main(&slice)?;
            cnt -= 1;
        }

        drop(fpo_mmap);
        drop(fpo_file);
        let _ = fs::remove_file(&self.tmp_po_path);

        let table_len = (self.pos - start_pos - 4) as u32;
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
        self.ensure_stage(WriterStage::Done)?;
        if let Some(mut f) = self.f.take() {
            f.flush()?;
            let file = f.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
        }
        Ok(())
    }
}

impl Drop for IndexWriter {
    fn drop(&mut self) {
        if self.stage != WriterStage::Done && self.f.is_some() {
            let _ = self.close_inner();
        }
        let _ = fs::remove_file(&self.tmp_p_path);
        let _ = fs::remove_file(&self.tmp_po_path);
    }
}

#[derive(Clone, Debug)]
struct PostingOffsetEntry {
    value: String,
    table_rel_off: usize,
}

/// Memory-mapped reader for Prometheus TSDB Index v2/v3 files.
#[derive(Debug)]
pub struct IndexReader {
    mmap: Mmap,
    version: u8,
    toc: IndexToc,
    symbol_offsets: Vec<usize>,
    num_symbols: usize,
    name_cache: HashMap<u32, String>,
    postings_index: BTreeMap<String, Vec<PostingOffsetEntry>>,
}

impl IndexReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IndexError> {
        let f = File::open(path.as_ref())?;
        // SAFETY: persisted block index files are immutable.
        let mmap = unsafe { MmapOptions::new().map(&f)? };
        if mmap.len() < INDEX_HEADER_LEN {
            return Err(IndexError::Encoding(EncodingError::InvalidSize));
        }
        let magic = u32::from_be_bytes([mmap[0], mmap[1], mmap[2], mmap[3]]);
        if magic != MAGIC_INDEX {
            return Err(IndexError::InvalidMagic(magic));
        }
        let version = mmap[4];
        if version != INDEX_FORMAT_V2 && version != INDEX_FORMAT_V3 {
            return Err(IndexError::UnsupportedVersion(version));
        }

        let toc = IndexToc::parse(&mmap)?;
        let (symbol_offsets, num_symbols) = parse_symbols_index(&mmap, toc.symbols as usize)?;
        let postings_index = parse_postings_offset_table(&mmap, toc.postings_table as usize)?;

        let mut name_cache = HashMap::with_capacity(postings_index.len());
        for name in postings_index.keys() {
            if name.is_empty() {
                continue;
            }
            if let Ok(sid) =
                reverse_lookup_symbol(&mmap, &symbol_offsets, num_symbols, name.as_str())
            {
                name_cache.insert(sid, name.clone());
            }
        }

        Ok(Self {
            mmap,
            version,
            toc,
            symbol_offsets,
            num_symbols,
            name_cache,
            postings_index,
        })
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.mmap.len()
    }

    /// Looks up symbol string by index `o`.
    pub fn lookup_symbol(&self, o: u32) -> Result<&str, IndexError> {
        if (o as usize) >= self.num_symbols {
            return Err(IndexError::UnknownSymbolOffset(o));
        }
        let block_off = self.symbol_offsets[(o as usize) / SYMBOL_FACTOR];
        let mut dec = Decbuf::new(&self.mmap[block_off..]);
        let skip = (o as usize) % SYMBOL_FACTOR;
        for _ in 0..skip {
            let _ = dec.uvarint_bytes()?;
        }
        Ok(dec.uvarint_str()?)
    }

    /// Returns all symbols in ascending lexicographical order.
    pub fn symbols(&self) -> Result<Vec<String>, IndexError> {
        let slice = checked_decbuf_at(&self.mmap, self.toc.symbols as usize)?;
        let mut dec = Decbuf::new(slice);
        let cnt = dec.be32()? as usize;
        let mut out = Vec::with_capacity(cnt);
        for _ in 0..cnt {
            out.push(dec.uvarint_str()?.to_string());
        }
        Ok(out)
    }

    /// Returns all unique label names present in the index (sorted, excluding `""`).
    pub fn label_names(&self) -> Vec<String> {
        self.postings_index
            .keys()
            .filter(|k| !k.is_empty())
            .cloned()
            .collect()
    }

    /// Returns all sorted label values for `name`.
    pub fn label_values(&self, name: &str) -> Result<Vec<String>, IndexError> {
        let Some(entries) = self.postings_index.get(name) else {
            return Ok(Vec::new());
        };
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let last_val = &entries.last().unwrap().value;
        let mut out = Vec::with_capacity(entries.len() * SYMBOL_FACTOR);
        self.traverse_posting_offsets(entries[0].table_rel_off, |val, _| {
            out.push(val.to_string());
            Ok(val != last_val.as_str())
        })?;
        Ok(out)
    }

    /// Decodes the series entry at `ref_id` (`series_id = file_offset / 16`).
    pub fn series(&self, ref_id: u64) -> Result<(Labels, Vec<ChunkMeta>), IndexError> {
        let offset = (ref_id as usize) * SERIES_BYTE_ALIGN;
        let payload = checked_uvarint_decbuf_at(&self.mmap, offset)?;
        let mut dec = Decbuf::new(payload);

        let num_labels = dec.uvarint()?;
        let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            let lno = dec.uvarint32()?;
            let lvo = dec.uvarint32()?;
            let ln = match self.name_cache.get(&lno) {
                Some(s) => s.as_str(),
                None => self.lookup_symbol(lno)?,
            };
            let lv = self.lookup_symbol(lvo)?;
            pairs.push((ln, lv));
        }
        let labels = Labels::from_sorted_pairs(pairs);

        let num_chunks = dec.uvarint()?;
        let mut chks = Vec::with_capacity(num_chunks);
        if num_chunks > 0 {
            let t0 = dec.varint64()?;
            let maxt0 = t0 + dec.uvarint64()? as i64;
            let mut ref0 = dec.uvarint64()? as i64;
            chks.push(ChunkMeta {
                ref_id: ref0 as u64,
                min_time: t0,
                max_time: maxt0,
                chunk: None,
            });
            let mut prev_maxt = maxt0;
            for _ in 1..num_chunks {
                let mint = prev_maxt + dec.uvarint64()? as i64;
                let maxt = mint + dec.uvarint64()? as i64;
                ref0 += dec.varint64()?;
                prev_maxt = maxt;
                chks.push(ChunkMeta {
                    ref_id: ref0 as u64,
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
        let offset = (ref_id as usize) * SERIES_BYTE_ALIGN;
        let payload = checked_uvarint_decbuf_at(&self.mmap, offset)?;
        let mut dec = Decbuf::new(payload);
        let num_labels = dec.uvarint()?;
        let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            let lno = dec.uvarint32()?;
            let lvo = dec.uvarint32()?;
            let ln = match self.name_cache.get(&lno) {
                Some(s) => s.as_str(),
                None => self.lookup_symbol(lno)?,
            };
            let lv = self.lookup_symbol(lvo)?;
            pairs.push((ln, lv));
        }
        Ok(Labels::from_sorted_pairs(pairs))
    }

    /// Decodes a raw postings list at `postings_off`.
    fn decode_postings_at(&self, postings_off: u64) -> Result<Vec<u64>, IndexError> {
        let payload = checked_decbuf_at(&self.mmap, postings_off as usize)?;
        let mut dec = Decbuf::new(payload);
        let n = dec.be32()? as usize;
        if dec.len() != 4 * n {
            return Err(IndexError::Corrupt(format!(
                "unexpected postings length: expected {}, got {}",
                4 * n,
                dec.len()
            )));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(dec.be32()? as u64);
        }
        Ok(out)
    }

    fn traverse_posting_offsets<F>(&self, start_rel_off: usize, mut cb: F) -> Result<(), IndexError>
    where
        F: FnMut(&str, u64) -> Result<bool, IndexError>,
    {
        let table_start = self.toc.postings_table as usize;
        // Skip 4B length + 4B count header via relative offset already anchored at table payload start.
        let mut dec = Decbuf::new(&self.mmap[table_start + 4..]);
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
        let Some(entries) = self.postings_index.get(name) else {
            return Ok(Vec::new());
        };
        if entries.is_empty() || values.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted_vals: Vec<&str> = values.to_vec();
        sorted_vals.sort_unstable();
        sorted_vals.dedup();

        let mut vi = 0usize;
        while vi < sorted_vals.len() && sorted_vals[vi] < entries[0].value.as_str() {
            vi += 1;
        }

        let mut lists: Vec<Vec<u64>> = Vec::new();
        while vi < sorted_vals.len() {
            let target = sorted_vals[vi];
            let mut idx = entries.partition_point(|e| e.value.as_str() < target);
            if idx == entries.len() {
                break;
            }
            if idx > 0 && entries[idx].value.as_str() != target {
                idx -= 1;
            }

            let next_anchor = entries.get(idx + 1).map(|e| e.value.as_str());
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
                if let Some(next_v) = next_anchor {
                    if sorted_vals[vi] >= next_v {
                        return Ok(false);
                    }
                }
                Ok(true)
            })?;
        }

        let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
        Ok(merge_postings(&slices))
    }

    pub fn postings_for_all_values(&self, name: &str) -> Result<Vec<u64>, IndexError> {
        let Some(entries) = self.postings_index.get(name) else {
            return Ok(Vec::new());
        };
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let last_val = &entries.last().unwrap().value;
        let mut lists = Vec::new();
        self.traverse_posting_offsets(entries[0].table_rel_off, |val, postings_off| {
            lists.push(self.decode_postings_at(postings_off)?);
            Ok(val != last_val.as_str())
        })?;
        let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
        Ok(merge_postings(&slices))
    }

    pub fn postings_for_label_matching(
        &self,
        name: &str,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Result<Vec<u64>, IndexError> {
        let Some(entries) = self.postings_index.get(name) else {
            return Ok(Vec::new());
        };
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let last_val = &entries.last().unwrap().value;
        let mut lists = Vec::new();
        self.traverse_posting_offsets(entries[0].table_rel_off, |val, postings_off| {
            if pred(val) {
                lists.push(self.decode_postings_at(postings_off)?);
            }
            Ok(val != last_val.as_str())
        })?;
        let slices: Vec<&[u64]> = lists.iter().map(|v| v.as_slice()).collect();
        Ok(merge_postings(&slices))
    }

    /// Returns unique sorted label names across `postings` series IDs.
    pub fn label_names_for(&self, postings: &[u64]) -> Result<Vec<String>, IndexError> {
        let mut name_ids: HashSet<u32> = HashSet::new();
        for &id in postings {
            let offset = (id as usize) * SERIES_BYTE_ALIGN;
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
                Some(s) => s.clone(),
                None => self.lookup_symbol(lno)?.to_string(),
            };
            names.push(s);
        }
        names.sort_unstable();
        Ok(names)
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
    if off + 4 > buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let len = u32::from_be_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
    let payload_start = off + 4;
    let payload_end = payload_start + len;
    if payload_end + 4 > buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let payload = &buf[payload_start..payload_end];
    let exp_crc = u32::from_be_bytes(buf[payload_end..payload_end + 4].try_into().unwrap());
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != exp_crc {
        return Err(IndexError::ChecksumMismatch {
            expected: exp_crc,
            actual: actual_crc,
        });
    }
    Ok(payload)
}

fn checked_uvarint_decbuf_at(buf: &[u8], off: usize) -> Result<&[u8], IndexError> {
    if off >= buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let mut dec = Decbuf::new(&buf[off..]);
    let len = dec.uvarint()?;
    let header_len = (buf.len() - off) - dec.len();
    let payload_start = off + header_len;
    let payload_end = payload_start + len;
    if payload_end + 4 > buf.len() {
        return Err(IndexError::Encoding(EncodingError::InvalidSize));
    }
    let payload = &buf[payload_start..payload_end];
    let exp_crc = u32::from_be_bytes(buf[payload_end..payload_end + 4].try_into().unwrap());
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != exp_crc {
        return Err(IndexError::ChecksumMismatch {
            expected: exp_crc,
            actual: actual_crc,
        });
    }
    Ok(payload)
}

fn parse_symbols_index(buf: &[u8], off: usize) -> Result<(Vec<usize>, usize), IndexError> {
    let payload = checked_decbuf_at(buf, off)?;
    let mut dec = Decbuf::new(payload);
    let cnt = dec.be32()? as usize;
    let base_pos = off + 4;
    let orig_len = payload.len();

    let mut offsets = Vec::with_capacity(1 + cnt / SYMBOL_FACTOR);
    for seen in 0..cnt {
        if seen % SYMBOL_FACTOR == 0 {
            offsets.push(base_pos + orig_len - dec.len());
        }
        let _ = dec.uvarint_bytes()?;
    }
    Ok((offsets, cnt))
}

fn reverse_lookup_symbol(
    buf: &[u8],
    offsets: &[usize],
    num_symbols: usize,
    sym: &str,
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
        let s = dec.uvarint_str()?;
        if s == sym {
            return Ok(res as u32);
        }
        if s > sym {
            break;
        }
        res += 1;
    }
    Err(IndexError::UnknownSymbol(sym.to_string()))
}

fn parse_postings_offset_table(
    buf: &[u8],
    table_off: usize,
) -> Result<BTreeMap<String, Vec<PostingOffsetEntry>>, IndexError> {
    let payload = checked_decbuf_at(buf, table_off)?;
    let mut dec = Decbuf::new(payload);
    let orig_len = dec.len();
    let cnt = dec.be32()? as usize;

    let mut result: BTreeMap<String, Vec<PostingOffsetEntry>> = BTreeMap::new();
    let mut last_name: Option<String> = None;
    let mut last_value = String::new();
    let mut last_rel_off = 0usize;
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
        let value = dec.uvarint_str()?;
        let _postings_off = dec.uvarint64()?;

        if !result.contains_key(name) {
            result.insert(name.to_string(), Vec::new());
            if let Some(prev_name) = last_name.take() {
                result.get_mut(&prev_name).unwrap().push(PostingOffsetEntry {
                    value: std::mem::take(&mut last_value),
                    table_rel_off: last_rel_off,
                });
            }
            value_count = 0;
        }
        if value_count.is_multiple_of(SYMBOL_FACTOR) {
            result.get_mut(name).unwrap().push(PostingOffsetEntry {
                value: value.to_string(),
                table_rel_off: rel_off,
            });
            last_name = None;
        } else {
            last_name = Some(name.to_string());
            last_value = value.to_string();
            last_rel_off = rel_off;
        }
        value_count += 1;
    }

    if let Some(prev_name) = last_name {
        result.get_mut(&prev_name).unwrap().push(PostingOffsetEntry {
            value: last_value,
            table_rel_off: last_rel_off,
        });
    }
    Ok(result)
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
}
