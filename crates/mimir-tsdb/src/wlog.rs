//! Bit-exact Prometheus TSDB WAL / WBL / Checkpoint reader and writer.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/wlog/wlog.go,
//! github.com/prometheus/prometheus/tsdb/wlog/reader.go,
//! github.com/prometheus/prometheus/tsdb/wlog/live_reader.go,
//! and github.com/prometheus/prometheus/tsdb/wlog/checkpoint.go.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::record::{RecordDecoder, RecordEncoder, RecordError, RecordType, RefMetadata};

pub const DEFAULT_SEGMENT_SIZE: usize = 128 * 1024 * 1024;
pub const PAGE_SIZE: usize = 32 * 1024;
pub const RECORD_HEADER_SIZE: usize = 7;
pub const WBL_DIR_NAME: &str = "wbl";

pub const REC_PAGE_TERM: u8 = 0;
pub const REC_FULL: u8 = 1;
pub const REC_FIRST: u8 = 2;
pub const REC_MIDDLE: u8 = 3;
pub const REC_LAST: u8 = 4;

pub const SNAPPY_MASK: u8 = 1 << 3;
pub const ZSTD_MASK: u8 = 1 << 4;
pub const REC_TYPE_MASK: u8 = SNAPPY_MASK - 1;

pub const CHECKPOINT_PREFIX: &str = "checkpoint.";
pub const CHECKPOINT_TEMP_SUFFIX: &str = ".tmp";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CompressionType {
    #[default]
    None,
    Snappy,
    Zstd,
}

#[derive(Debug, Error)]
pub enum WlogError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("corruption in segment {segment:?} at offset {offset}: {message}")]
    Corruption {
        dir: Option<PathBuf>,
        segment: Option<usize>,
        offset: usize,
        message: String,
    },
    #[error("invalid segment size {0} (must be a multiple of {PAGE_SIZE})")]
    InvalidSegmentSize(usize),
    #[error("segments are not sequential")]
    NonSequentialSegments,
    #[error("wlog is closed")]
    Closed,
    #[error("record error: {0}")]
    Record(#[from] RecordError),
    #[error("compression error: {0}")]
    Compression(String),
    #[error("unexpected gap to last checkpoint (expected {expected}, requested {requested})")]
    CheckpointGap { expected: usize, requested: usize },
    #[error("checkpoint {0} is not a directory")]
    CheckpointNotDir(String),
}

#[inline]
pub fn segment_name(dir: &Path, index: usize) -> PathBuf {
    dir.join(format!("{index:08}"))
}

#[inline]
pub fn checkpoint_dir(dir: &Path, index: usize) -> PathBuf {
    dir.join(format!("{CHECKPOINT_PREFIX}{index:08}"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SegmentRef {
    name: String,
    index: usize,
}

fn list_segments(dir: &Path) -> Result<Vec<SegmentRef>, WlogError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut refs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(name_str) = fname.to_str() else {
            continue;
        };
        if let Ok(idx) = name_str.parse::<usize>() {
            refs.push(SegmentRef {
                name: name_str.to_string(),
                index: idx,
            });
        }
    }
    refs.sort_by_key(|r| r.index);
    for w in refs.windows(2) {
        if w[0].index + 1 != w[1].index {
            return Err(WlogError::NonSequentialSegments);
        }
    }
    Ok(refs)
}

/// Returns `(first, last)` segment indices in `dir`, or `None` if no segments exist.
pub fn segments_range(dir: &Path) -> Result<Option<(usize, usize)>, WlogError> {
    let refs = list_segments(dir)?;
    if refs.is_empty() {
        Ok(None)
    } else {
        Ok(Some((refs.first().unwrap().index, refs.last().unwrap().index)))
    }
}

#[derive(Clone, Debug)]
pub struct SegmentRange {
    pub dir: PathBuf,
    pub first: Option<usize>,
    pub last: Option<usize>,
}

struct Page {
    alloc: usize,
    flushed: usize,
    buf: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    fn new() -> Self {
        Self {
            alloc: 0,
            flushed: 0,
            buf: Box::new([0u8; PAGE_SIZE]),
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        PAGE_SIZE - self.alloc
    }

    #[inline]
    fn full(&self) -> bool {
        PAGE_SIZE - self.alloc < RECORD_HEADER_SIZE
    }

    fn reset(&mut self) {
        self.buf.fill(0);
        self.alloc = 0;
        self.flushed = 0;
    }
}

struct OpenSegment {
    file: File,
    index: usize,
}

/// Prometheus TSDB WAL / WBL writer with 32 KiB page framing, Snappy/Zstd compression,
/// Castagnoli CRC32C checksums, and segment rotation.
pub struct WlogWriter {
    dir: PathBuf,
    segment_size: usize,
    segment: Option<OpenSegment>,
    done_pages: usize,
    page: Page,
    compress: CompressionType,
    closed: bool,
    comp_buf: Vec<u8>,
}

impl WlogWriter {
    pub fn open(dir: impl AsRef<Path>, compress: CompressionType) -> Result<Self, WlogError> {
        Self::with_segment_size(dir, DEFAULT_SEGMENT_SIZE, compress)
    }

    pub fn with_segment_size(
        dir: impl AsRef<Path>,
        segment_size: usize,
        compress: CompressionType,
    ) -> Result<Self, WlogError> {
        if segment_size == 0 || !segment_size.is_multiple_of(PAGE_SIZE) {
            return Err(WlogError::InvalidSegmentSize(segment_size));
        }
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let write_segment_index = match segments_range(&dir)? {
            Some((_, last)) => last + 1,
            None => 0,
        };

        let seg = Self::create_segment(&dir, write_segment_index)?;
        let done_pages = (seg.file.metadata()?.len() as usize) / PAGE_SIZE;

        Ok(Self {
            dir,
            segment_size,
            segment: Some(seg),
            done_pages,
            page: Page::new(),
            compress,
            closed: false,
            comp_buf: Vec::new(),
        })
    }

    fn create_segment(dir: &Path, index: usize) -> Result<OpenSegment, WlogError> {
        let path = segment_name(dir, index);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(OpenSegment { file, index })
    }

    #[inline]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    #[inline]
    pub fn compression(&self) -> CompressionType {
        self.compress
    }

    #[inline]
    fn pages_per_segment(&self) -> usize {
        self.segment_size / PAGE_SIZE
    }

    fn flush_page(&mut self, force_clear: bool) -> Result<(), WlogError> {
        let seg = self.segment.as_mut().ok_or(WlogError::Closed)?;
        let should_clear = force_clear || self.page.full();
        if should_clear {
            self.page.alloc = PAGE_SIZE;
        }
        let slice = &self.page.buf[self.page.flushed..self.page.alloc];
        seg.file.write_all(slice)?;
        self.page.flushed += slice.len();
        if should_clear {
            self.page.reset();
            self.done_pages += 1;
        }
        Ok(())
    }

    /// Rotates to the next WAL segment file, flushing and fsyncing the previous segment.
    pub fn next_segment(&mut self) -> Result<usize, WlogError> {
        if self.closed {
            return Err(WlogError::Closed);
        }
        if self.page.alloc > 0 {
            self.flush_page(true)?;
        }
        let prev = self.segment.take().ok_or(WlogError::Closed)?;
        let next_idx = prev.index + 1;
        prev.file.sync_all()?;
        drop(prev);

        let next = Self::create_segment(&self.dir, next_idx)?;
        self.done_pages = (next.file.metadata()?.len() as usize) / PAGE_SIZE;
        self.segment = Some(next);
        Ok(next_idx)
    }

    /// Logs one or more records atomically into the WAL, flushing the partial page after the batch.
    pub fn log(&mut self, recs: &[&[u8]]) -> Result<(), WlogError> {
        if self.closed {
            return Err(WlogError::Closed);
        }
        for (i, &rec) in recs.iter().enumerate() {
            self.log_one(rec, i + 1 == recs.len())?;
        }
        Ok(())
    }

    fn log_one(&mut self, rec: &[u8], is_final: bool) -> Result<(), WlogError> {
        if self.page.full() {
            self.flush_page(true)?;
        }

        let (used_comp, use_comp_buf) =
            Self::encode_compressed(self.compress, rec, &mut self.comp_buf)?;
        let payload_len = if use_comp_buf {
            self.comp_buf.len()
        } else {
            rec.len()
        };

        let left = (self.page.remaining() - RECORD_HEADER_SIZE)
            + (PAGE_SIZE - RECORD_HEADER_SIZE)
                * self
                    .pages_per_segment()
                    .saturating_sub(self.done_pages + 1);

        if payload_len > left {
            self.next_segment()?;
        }

        let mut offset = 0usize;
        let mut part_idx = 0usize;
        while part_idx == 0 || offset < payload_len {
            let max_part = PAGE_SIZE - self.page.alloc - RECORD_HEADER_SIZE;
            let part_len = (payload_len - offset).min(max_part);

            let is_last = offset + part_len == payload_len;
            let mut typ = match (part_idx == 0, is_last) {
                (true, true) => REC_FULL,
                (false, true) => REC_LAST,
                (true, false) => REC_FIRST,
                (false, false) => REC_MIDDLE,
            };

            match used_comp {
                CompressionType::None => {}
                CompressionType::Snappy => typ |= SNAPPY_MASK,
                CompressionType::Zstd => typ |= ZSTD_MASK,
            }

            let part = if use_comp_buf {
                &self.comp_buf[offset..offset + part_len]
            } else {
                &rec[offset..offset + part_len]
            };
            let crc = crc32c::crc32c(part);
            let hdr_pos = self.page.alloc;
            self.page.buf[hdr_pos] = typ;
            self.page.buf[hdr_pos + 1..hdr_pos + 3]
                .copy_from_slice(&(part_len as u16).to_be_bytes());
            self.page.buf[hdr_pos + 3..hdr_pos + 7].copy_from_slice(&crc.to_be_bytes());
            self.page.buf[hdr_pos + RECORD_HEADER_SIZE..hdr_pos + RECORD_HEADER_SIZE + part_len]
                .copy_from_slice(part);
            self.page.alloc += RECORD_HEADER_SIZE + part_len;

            if self.page.full() {
                self.flush_page(true)?;
            }

            offset += part_len;
            part_idx += 1;
        }

        if is_final && self.page.alloc > 0 {
            self.flush_page(false)?;
        }

        Ok(())
    }

    fn encode_compressed(
        compress: CompressionType,
        rec: &[u8],
        comp_buf: &mut Vec<u8>,
    ) -> Result<(CompressionType, bool), WlogError> {
        if rec.is_empty() || compress == CompressionType::None {
            return Ok((CompressionType::None, false));
        }

        comp_buf.clear();
        match compress {
            CompressionType::None => unreachable!(),
            CompressionType::Snappy => {
                let max_len = snap::raw::max_compress_len(rec.len());
                comp_buf.resize(max_len, 0);
                let n = snap::raw::Encoder::new()
                    .compress(rec, comp_buf)
                    .map_err(|e| WlogError::Compression(e.to_string()))?;
                comp_buf.truncate(n);
            }
            CompressionType::Zstd => {
                let encoded = zstd::bulk::compress(rec, 3)
                    .map_err(|e| WlogError::Compression(e.to_string()))?;
                *comp_buf = encoded;
            }
        }

        if comp_buf.len() >= rec.len() {
            Ok((CompressionType::None, false))
        } else {
            Ok((compress, true))
        }
    }

    pub fn last_segment_and_offset(&self) -> Result<(Option<usize>, usize), WlogError> {
        let seg = segments_range(&self.dir)?.map(|(_, last)| last);
        let offset = (self.done_pages * PAGE_SIZE) + self.page.alloc;
        Ok((seg, offset))
    }

    pub fn truncate(&self, before_segment: usize) -> Result<(), WlogError> {
        for r in list_segments(&self.dir)? {
            if r.index >= before_segment {
                break;
            }
            fs::remove_file(self.dir.join(&r.name))?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), WlogError> {
        if let Some(seg) = self.segment.as_ref() {
            seg.file.sync_all()?;
        }
        Ok(())
    }

    pub fn close(mut self) -> Result<(), WlogError> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), WlogError> {
        if self.closed {
            return Ok(());
        }
        if self.segment.is_some() {
            if self.page.alloc > 0 {
                self.flush_page(true)?;
            }
            if let Some(seg) = self.segment.take() {
                seg.file.sync_all()?;
            }
        }
        self.closed = true;
        Ok(())
    }
}

impl Drop for WlogWriter {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

/// Repairs a corrupted WAL directory by removing segments after `corrupt_segment` and
/// rewriting `corrupt_segment` up to `corrupt_offset`.
pub fn repair_wal(
    dir: &Path,
    corrupt_segment: usize,
    corrupt_offset: usize,
    compress: CompressionType,
) -> Result<(), WlogError> {
    let refs = list_segments(dir)?;
    for r in &refs {
        if r.index > corrupt_segment {
            fs::remove_file(dir.join(&r.name))?;
        }
    }

    let seg_path = segment_name(dir, corrupt_segment);
    let tmp_path = dir.join(format!("{corrupt_segment:08}.repair"));
    fs::rename(&seg_path, &tmp_path)?;

    {
        let mut writer = WlogWriter::with_segment_size(dir, DEFAULT_SEGMENT_SIZE, compress)?;
        // Recreate exact corrupt_segment index if writer picked another index.
        if writer.segment.as_ref().map(|s| s.index) != Some(corrupt_segment) {
            writer.segment = Some(WlogWriter::create_segment(dir, corrupt_segment)?);
            writer.done_pages = 0;
            writer.page.reset();
        }

        let f = File::open(&tmp_path)?;
        let mut reader = WlogReader::from_reader(BufReader::new(f));
        while reader.next_record().ok().flatten().is_some() {
            if reader.offset() >= corrupt_offset {
                break;
            }
            writer.log(&[reader.record()])?;
        }
        if writer.page.alloc > 0 {
            writer.flush_page(true)?;
        }
        writer.close()?;
    }

    fs::remove_file(&tmp_path)?;
    // Create next clean segment
    let _next = WlogWriter::create_segment(dir, corrupt_segment + 1)?;
    Ok(())
}

/// Multi-segment buffered reader matching Prometheus `wlog.segmentBufReader`.
pub struct SegmentBufReader {
    segs: Vec<(PathBuf, usize, BufReader<File>)>,
    cur: usize,
    off: usize,
}

impl SegmentBufReader {
    pub fn open_dir(dir: &Path) -> Result<Self, WlogError> {
        Self::open_ranges(&[SegmentRange {
            dir: dir.to_path_buf(),
            first: None,
            last: None,
        }])
    }

    pub fn open_ranges(ranges: &[SegmentRange]) -> Result<Self, WlogError> {
        let mut segs = Vec::new();
        for range in ranges {
            let refs = list_segments(&range.dir)?;
            for r in refs {
                if let Some(first) = range.first {
                    if r.index < first {
                        continue;
                    }
                }
                if let Some(last) = range.last {
                    if r.index > last {
                        break;
                    }
                }
                let path = range.dir.join(&r.name);
                let file = File::open(&path)?;
                segs.push((range.dir.clone(), r.index, BufReader::with_capacity(16 * PAGE_SIZE, file)));
            }
        }
        Ok(Self {
            segs,
            cur: 0,
            off: 0,
        })
    }

    pub fn current_dir(&self) -> Option<&Path> {
        self.segs.get(self.cur).map(|(d, _, _)| d.as_path())
    }

    pub fn current_segment(&self) -> Option<usize> {
        self.segs.get(self.cur).map(|(_, idx, _)| *idx)
    }

    pub fn current_offset(&self) -> usize {
        self.off
    }
}

impl Read for SegmentBufReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.segs.is_empty() || self.cur >= self.segs.len() {
            return Ok(0);
        }

        let n = self.segs[self.cur].2.read(buf)?;
        self.off += n;
        if n > 0 {
            return Ok(n);
        }

        // Hit EOF on current segment. If short of a page boundary, fake zero-padding up to PAGE_SIZE.
        if !self.off.is_multiple_of(PAGE_SIZE) {
            let rem = PAGE_SIZE - (self.off % PAGE_SIZE);
            let pad = rem.min(buf.len());
            buf[..pad].fill(0);
            self.off += pad;
            return Ok(pad);
        }

        if self.cur + 1 >= self.segs.len() {
            return Ok(0);
        }

        self.cur += 1;
        self.off = 0;
        self.read(buf)
    }
}

/// Trait implemented by readers that can report segment directory/index/offset for corruption errors.
pub trait SegmentTracker {
    fn tracker_info(&self) -> (Option<&Path>, Option<usize>, Option<usize>);
}

impl SegmentTracker for SegmentBufReader {
    fn tracker_info(&self) -> (Option<&Path>, Option<usize>, Option<usize>) {
        (
            self.current_dir(),
            self.current_segment(),
            Some(self.current_offset()),
        )
    }
}

impl SegmentTracker for BufReader<File> {
    fn tracker_info(&self) -> (Option<&Path>, Option<usize>, Option<usize>) {
        (None, None, None)
    }
}

impl SegmentTracker for &[u8] {
    fn tracker_info(&self) -> (Option<&Path>, Option<usize>, Option<usize>) {
        (None, None, None)
    }
}

impl<R: SegmentTracker> SegmentTracker for io::Take<R> {
    fn tracker_info(&self) -> (Option<&Path>, Option<usize>, Option<usize>) {
        self.get_ref().tracker_info()
    }
}

/// Reader for Prometheus WAL/WBL/Checkpoint segments (`wlog.Reader`).
pub struct WlogReader<R> {
    rdr: R,
    rec: Vec<u8>,
    precompr_buf: Vec<u8>,
    page_buf: Box<[u8; PAGE_SIZE]>,
    total: usize,
    cur_rec_typ: u8,
}

impl WlogReader<SegmentBufReader> {
    /// Opens all WAL segments in `dir`.
    pub fn open_dir(dir: impl AsRef<Path>) -> Result<Self, WlogError> {
        let sbr = SegmentBufReader::open_dir(dir.as_ref())?;
        Ok(Self::from_reader(sbr))
    }

    /// Opens the latest checkpoint (if any) followed by all WAL segments newer than the checkpoint.
    pub fn open_wal_with_checkpoint(dir: impl AsRef<Path>) -> Result<Self, WlogError> {
        let dir = dir.as_ref();
        let mut ranges = Vec::new();
        let first_wal = match last_checkpoint(dir)? {
            Some((cp_dir, cp_idx)) => {
                ranges.push(SegmentRange {
                    dir: cp_dir,
                    first: None,
                    last: None,
                });
                Some(cp_idx + 1)
            }
            None => None,
        };
        ranges.push(SegmentRange {
            dir: dir.to_path_buf(),
            first: first_wal,
            last: None,
        });
        let sbr = SegmentBufReader::open_ranges(&ranges)?;
        Ok(Self::from_reader(sbr))
    }

    pub fn open_ranges(ranges: &[SegmentRange]) -> Result<Self, WlogError> {
        let sbr = SegmentBufReader::open_ranges(ranges)?;
        Ok(Self::from_reader(sbr))
    }
}

impl<R: Read + SegmentTracker> WlogReader<R> {
    pub fn from_reader(rdr: R) -> Self {
        Self {
            rdr,
            rec: Vec::new(),
            precompr_buf: Vec::new(),
            page_buf: Box::new([0u8; PAGE_SIZE]),
            total: 0,
            cur_rec_typ: REC_PAGE_TERM,
        }
    }

    #[inline]
    pub fn record(&self) -> &[u8] {
        &self.rec
    }

    #[inline]
    pub fn offset(&self) -> usize {
        let (_, _, off) = self.rdr.tracker_info();
        off.unwrap_or(self.total)
    }

    #[inline]
    pub fn segment(&self) -> Option<usize> {
        let (_, seg, _) = self.rdr.tracker_info();
        seg
    }

    fn make_corruption(&self, message: impl Into<String>) -> WlogError {
        let (dir, seg, off) = self.rdr.tracker_info();
        WlogError::Corruption {
            dir: dir.map(Path::to_path_buf),
            segment: seg,
            offset: off.unwrap_or(self.total),
            message: message.into(),
        }
    }

    /// Reads the next logical WAL record. Returns `Ok(None)` on clean EOF.
    pub fn next_record(&mut self) -> Result<Option<&[u8]>, WlogError> {
        self.precompr_buf.clear();
        let mut part_idx = 0usize;

        loop {
            let mut first_byte = [0u8; 1];
            if !read_exact_or_clean_eof(&mut self.rdr, &mut first_byte)? {
                if self.cur_rec_typ == REC_FIRST || self.cur_rec_typ == REC_MIDDLE {
                    return Err(self.make_corruption("last record is torn"));
                }
                return Ok(None);
            }
            self.total += 1;
            let hdr0 = first_byte[0];
            self.cur_rec_typ = hdr0 & REC_TYPE_MASK;

            let compr = if (hdr0 & SNAPPY_MASK) == SNAPPY_MASK {
                CompressionType::Snappy
            } else if (hdr0 & ZSTD_MASK) == ZSTD_MASK {
                CompressionType::Zstd
            } else {
                CompressionType::None
            };

            if self.cur_rec_typ == REC_PAGE_TERM {
                let k = PAGE_SIZE - (self.total % PAGE_SIZE);
                if k == PAGE_SIZE {
                    continue;
                }
                let pad = &mut self.page_buf[..k];
                if let Err(e) = self.rdr.read_exact(pad) {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        return Err(self.make_corruption("unexpected EOF while reading page padding"));
                    }
                    return Err(WlogError::Io(e));
                }
                self.total += k;
                if pad.iter().any(|&b| b != 0) {
                    return Err(self.make_corruption("unexpected non-zero byte in padded page"));
                }
                continue;
            }

            let mut hdr_rest = [0u8; RECORD_HEADER_SIZE - 1];
            if let Err(e) = self.rdr.read_exact(&mut hdr_rest) {
                return Err(self.make_corruption(format!("read remaining header: {e}")));
            }
            self.total += hdr_rest.len();

            let length = u16::from_be_bytes([hdr_rest[0], hdr_rest[1]]) as usize;
            let crc = u32::from_be_bytes([hdr_rest[2], hdr_rest[3], hdr_rest[4], hdr_rest[5]]);

            if length > PAGE_SIZE - RECORD_HEADER_SIZE {
                return Err(self.make_corruption(format!("invalid record size {length}")));
            }

            let payload = &mut self.page_buf[..length];
            if let Err(e) = self.rdr.read_exact(payload) {
                return Err(self.make_corruption(format!("read record payload: {e}")));
            }
            self.total += length;

            let actual_crc = crc32c::crc32c(payload);
            if actual_crc != crc {
                return Err(self.make_corruption(format!(
                    "unexpected checksum {actual_crc:#x}, expected {crc:#x}"
                )));
            }

            self.precompr_buf.extend_from_slice(payload);

            if let Err(msg) = validate_record(self.cur_rec_typ, part_idx) {
                return Err(self.make_corruption(msg));
            }

            if self.cur_rec_typ == REC_LAST || self.cur_rec_typ == REC_FULL {
                self.decompress_into_rec(compr)?;
                return Ok(Some(&self.rec));
            }

            part_idx += 1;
        }
    }

    fn decompress_into_rec(&mut self, compr: CompressionType) -> Result<(), WlogError> {
        match compr {
            CompressionType::None => {
                self.rec.clear();
                self.rec.extend_from_slice(&self.precompr_buf);
                Ok(())
            }
            CompressionType::Snappy => {
                if self.precompr_buf.is_empty() {
                    self.rec.clear();
                    return Ok(());
                }
                let dec_len = snap::raw::decompress_len(&self.precompr_buf)
                    .map_err(|e| self.make_corruption(format!("snappy decompress_len: {e}")))?;
                self.rec.resize(dec_len, 0);
                let n = snap::raw::Decoder::new()
                    .decompress(&self.precompr_buf, &mut self.rec)
                    .map_err(|e| self.make_corruption(format!("snappy decompress: {e}")))?;
                self.rec.truncate(n);
                Ok(())
            }
            CompressionType::Zstd => {
                if self.precompr_buf.is_empty() {
                    self.rec.clear();
                    return Ok(());
                }
                let out = zstd::stream::decode_all(self.precompr_buf.as_slice())
                    .map_err(|e| self.make_corruption(format!("zstd decompress: {e}")))?;
                self.rec = out;
                Ok(())
            }
        }
    }
}

fn read_exact_or_clean_eof<R: Read>(rdr: &mut R, buf: &mut [u8; 1]) -> io::Result<bool> {
    loop {
        match rdr.read(buf) {
            Ok(0) => return Ok(false),
            Ok(_) => return Ok(true),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

fn validate_record(typ: u8, part_idx: usize) -> Result<(), String> {
    match typ {
        REC_FULL => {
            if part_idx != 0 {
                return Err("unexpected full record".to_string());
            }
            Ok(())
        }
        REC_FIRST => {
            if part_idx != 0 {
                return Err("unexpected first record".to_string());
            }
            Ok(())
        }
        REC_MIDDLE => {
            if part_idx == 0 {
                return Err("unexpected middle record".to_string());
            }
            Ok(())
        }
        REC_LAST => {
            if part_idx == 0 {
                return Err("unexpected last record".to_string());
            }
            Ok(())
        }
        other => Err(format!("unexpected record type {other}")),
    }
}

/// Statistics returned by [`create_checkpoint`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckpointStats {
    pub dropped_series: usize,
    pub dropped_samples: usize,
    pub dropped_tombstones: usize,
    pub dropped_exemplars: usize,
    pub dropped_metadata: usize,
    pub total_series: usize,
    pub total_samples: usize,
    pub total_tombstones: usize,
    pub total_exemplars: usize,
    pub total_metadata: usize,
}

#[derive(Clone, Debug)]
struct CheckpointRef {
    name: String,
    index: usize,
}

fn list_checkpoints(dir: &Path) -> Result<Vec<CheckpointRef>, WlogError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut refs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(name_str) = fname.to_str() else {
            continue;
        };
        if !name_str.starts_with(CHECKPOINT_PREFIX) {
            continue;
        }
        let suffix = &name_str[CHECKPOINT_PREFIX.len()..];
        let Ok(index) = suffix.parse::<usize>() else {
            continue;
        };
        if !entry.file_type()?.is_dir() {
            return Err(WlogError::CheckpointNotDir(name_str.to_string()));
        }
        refs.push(CheckpointRef {
            name: name_str.to_string(),
            index,
        });
    }
    refs.sort_by_key(|c| c.index);
    Ok(refs)
}

/// Returns `(checkpoint_path, index)` of the most recent checkpoint in `dir`, if any.
pub fn last_checkpoint(dir: &Path) -> Result<Option<(PathBuf, usize)>, WlogError> {
    let cps = list_checkpoints(dir)?;
    Ok(cps.last().map(|c| (dir.join(&c.name), c.index)))
}

/// Reads the `MinValidTime` stored in the most recent checkpoint in `dir`, if present.
pub fn read_min_valid_time(dir: &Path) -> Result<Option<i64>, WlogError> {
    let Some((cp_dir, _)) = last_checkpoint(dir)? else {
        return Ok(None);
    };
    let mut reader = WlogReader::open_dir(&cp_dir)?;
    let Some(rec) = reader.next_record()? else {
        return Ok(None);
    };
    let dec = RecordDecoder::new();
    if dec.record_type(rec) != RecordType::MinValidTime {
        return Ok(None);
    }
    Ok(Some(dec.min_valid_time(rec)?))
}

/// Deletes all checkpoints in `dir` with index `< max_index`.
pub fn delete_checkpoints(dir: &Path, max_index: usize) -> Result<(), WlogError> {
    for cp in list_checkpoints(dir)? {
        if cp.index >= max_index {
            break;
        }
        fs::remove_dir_all(dir.join(&cp.name))?;
    }
    Ok(())
}

/// Removes any leftover `checkpoint.*.tmp` directories in `dir`.
pub fn delete_temp_checkpoints(dir: &Path) -> Result<(), WlogError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(name_str) = fname.to_str() else {
            continue;
        };
        if name_str.starts_with(CHECKPOINT_PREFIX)
            && name_str.ends_with(CHECKPOINT_TEMP_SUFFIX)
            && entry.file_type()?.is_dir()
        {
            fs::remove_dir_all(entry.path())?;
        }
    }
    Ok(())
}

const METADATA_BATCH_SIZE: usize = 1000;
const FLUSH_THRESHOLD: usize = 1024 * 1024;

/// Creates a compacted checkpoint `checkpoint.<to>` over WAL segments `[from, to]` (plus the
/// previous checkpoint if one exists), matching `wlog.Checkpoint`.
pub fn create_checkpoint<F>(
    dir: &Path,
    from: usize,
    to: usize,
    mut keep: F,
    mint: i64,
    enable_st_storage: bool,
    compress: CompressionType,
) -> Result<CheckpointStats, WlogError>
where
    F: FnMut(u64) -> bool,
{
    let mut stats = CheckpointStats::default();
    let mut ranges = Vec::new();
    let mut effective_from = from;

    if let Some((cp_dir, idx)) = last_checkpoint(dir)? {
        let expected = idx + 1;
        if effective_from > expected {
            return Err(WlogError::CheckpointGap {
                expected,
                requested: effective_from,
            });
        }
        effective_from = expected;
        ranges.push(SegmentRange {
            dir: cp_dir,
            first: None,
            last: Some(usize::MAX),
        });
    }
    ranges.push(SegmentRange {
        dir: dir.to_path_buf(),
        first: Some(effective_from),
        last: Some(to),
    });

    delete_temp_checkpoints(dir)?;

    let cp_final = checkpoint_dir(dir, to);
    let cp_tmp = PathBuf::from(format!("{}{CHECKPOINT_TEMP_SUFFIX}", cp_final.display()));
    fs::create_dir_all(&cp_tmp)?;

    let res = (|| -> Result<CheckpointStats, WlogError> {
        let mut cp_writer = WlogWriter::open(&cp_tmp, compress)?;
        let enc = RecordEncoder::new(enable_st_storage);
        let dec = RecordDecoder::new();

        let mut persisted_mint = mint;
        if let Some(prev_mint) = read_min_valid_time(dir)? {
            if prev_mint > persisted_mint {
                persisted_mint = prev_mint;
            }
        }
        let mvt_rec = enc.min_valid_time(persisted_mint, Vec::new());
        cp_writer.log(&[&mvt_rec])?;

        let mut reader = WlogReader::open_ranges(&ranges)?;
        let mut batch: Vec<Vec<u8>> = Vec::new();
        let mut batch_bytes = 0usize;
        let mut latest_metadata: HashMap<u64, RefMetadata> = HashMap::new();

        while let Some(rec) = reader.next_record()? {
            match dec.record_type(rec) {
                RecordType::Series => {
                    let series = dec.series(rec)?;
                    let n = series.len();
                    stats.total_series += n;
                    let kept: Vec<_> = series.into_iter().filter(|s| keep(s.ref_id)).collect();
                    stats.dropped_series += n - kept.len();
                    if !kept.is_empty() {
                        let encoded = enc.series(&kept, Vec::new());
                        batch_bytes += encoded.len();
                        batch.push(encoded);
                    }
                }
                RecordType::Samples | RecordType::SamplesV2 => {
                    let samples = dec.samples(rec)?;
                    let n = samples.len();
                    stats.total_samples += n;
                    let kept: Vec<_> = samples.into_iter().filter(|s| s.t >= mint).collect();
                    stats.dropped_samples += n - kept.len();
                    if !kept.is_empty() {
                        let encoded = enc.samples(&kept, Vec::new());
                        batch_bytes += encoded.len();
                        batch.push(encoded);
                    }
                }
                RecordType::HistogramSamples | RecordType::HistogramSamplesV2 => {
                    let hists = dec.histogram_samples(rec)?;
                    let n = hists.len();
                    stats.total_samples += n;
                    let kept: Vec<_> = hists.into_iter().filter(|h| h.t >= mint).collect();
                    stats.dropped_samples += n - kept.len();
                    if !kept.is_empty() {
                        let (encoded, leftover) = enc.histogram_samples(&kept, Vec::new());
                        if !encoded.is_empty() {
                            batch_bytes += encoded.len();
                            batch.push(encoded);
                        }
                        if !leftover.is_empty() {
                            let cb_enc = enc.custom_buckets_histogram_samples(&leftover, Vec::new());
                            batch_bytes += cb_enc.len();
                            batch.push(cb_enc);
                        }
                    }
                }
                RecordType::CustomBucketsHistogramSamples => {
                    let hists = dec.histogram_samples(rec)?;
                    let n = hists.len();
                    stats.total_samples += n;
                    let kept: Vec<_> = hists.into_iter().filter(|h| h.t >= mint).collect();
                    stats.dropped_samples += n - kept.len();
                    if !kept.is_empty() {
                        let refs: Vec<_> = kept.iter().collect();
                        let encoded = enc.custom_buckets_histogram_samples(&refs, Vec::new());
                        batch_bytes += encoded.len();
                        batch.push(encoded);
                    }
                }
                RecordType::FloatHistogramSamples | RecordType::FloatHistogramSamplesV2 => {
                    let hists = dec.float_histogram_samples(rec)?;
                    let n = hists.len();
                    stats.total_samples += n;
                    let kept: Vec<_> = hists.into_iter().filter(|h| h.t >= mint).collect();
                    stats.dropped_samples += n - kept.len();
                    if !kept.is_empty() {
                        let (encoded, leftover) = enc.float_histogram_samples(&kept, Vec::new());
                        if !encoded.is_empty() {
                            batch_bytes += encoded.len();
                            batch.push(encoded);
                        }
                        if !leftover.is_empty() {
                            let cb_enc =
                                enc.custom_buckets_float_histogram_samples(&leftover, Vec::new());
                            batch_bytes += cb_enc.len();
                            batch.push(cb_enc);
                        }
                    }
                }
                RecordType::CustomBucketsFloatHistogramSamples => {
                    let hists = dec.float_histogram_samples(rec)?;
                    let n = hists.len();
                    stats.total_samples += n;
                    let kept: Vec<_> = hists.into_iter().filter(|h| h.t >= mint).collect();
                    stats.dropped_samples += n - kept.len();
                    if !kept.is_empty() {
                        let refs: Vec<_> = kept.iter().collect();
                        let encoded =
                            enc.custom_buckets_float_histogram_samples(&refs, Vec::new());
                        batch_bytes += encoded.len();
                        batch.push(encoded);
                    }
                }
                RecordType::Tombstones => {
                    let stones = dec.tombstones(rec)?;
                    let n = stones.len();
                    stats.total_tombstones += n;
                    let kept: Vec<_> = stones
                        .into_iter()
                        .filter(|s| keep(s.ref_id) && s.maxt >= mint)
                        .collect();
                    stats.dropped_tombstones += n - kept.len();
                    if !kept.is_empty() {
                        let encoded = enc.tombstones(&kept, Vec::new());
                        batch_bytes += encoded.len();
                        batch.push(encoded);
                    }
                }
                RecordType::Exemplars => {
                    let exs = dec.exemplars(rec)?;
                    let n = exs.len();
                    stats.total_exemplars += n;
                    let kept: Vec<_> = exs.into_iter().filter(|e| e.t >= mint).collect();
                    stats.dropped_exemplars += n - kept.len();
                    if !kept.is_empty() {
                        let encoded = enc.exemplars(&kept, Vec::new());
                        batch_bytes += encoded.len();
                        batch.push(encoded);
                    }
                }
                RecordType::Metadata => {
                    let metas = dec.metadata(rec)?;
                    stats.total_metadata += metas.len();
                    for m in metas {
                        if keep(m.ref_id) {
                            if latest_metadata.insert(m.ref_id, m).is_some() {
                                stats.dropped_metadata += 1;
                            }
                        } else {
                            stats.dropped_metadata += 1;
                        }
                    }
                }
                RecordType::MinValidTime | RecordType::MmapMarkers | RecordType::Unknown => {}
            }

            if batch_bytes > FLUSH_THRESHOLD {
                let slices: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
                cp_writer.log(&slices)?;
                batch.clear();
                batch_bytes = 0;
            }
        }

        if !batch.is_empty() {
            let slices: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
            cp_writer.log(&slices)?;
            batch.clear();
        }

        if !latest_metadata.is_empty() {
            let mut meta_chunk = Vec::with_capacity(METADATA_BATCH_SIZE);
            for (_, m) in latest_metadata.drain() {
                meta_chunk.push(m);
                if meta_chunk.len() >= METADATA_BATCH_SIZE {
                    let encoded = enc.metadata(&meta_chunk, Vec::new());
                    cp_writer.log(&[&encoded])?;
                    meta_chunk.clear();
                }
            }
            if !meta_chunk.is_empty() {
                let encoded = enc.metadata(&meta_chunk, Vec::new());
                cp_writer.log(&[&encoded])?;
            }
        }

        cp_writer.close()?;
        if cp_final.exists() {
            fs::remove_dir_all(&cp_final)?;
        }
        fs::rename(&cp_tmp, &cp_final)?;
        Ok(stats)
    })();

    if res.is_err() {
        let _ = fs::remove_dir_all(&cp_tmp);
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::Labels;
    use crate::record::{RefSample, RefSeries};

    #[test]
    fn wal_write_read_multipage_and_snappy() {
        let dir = tempfile::tempdir().unwrap();
        let enc = RecordEncoder::new(false);
        let dec = RecordDecoder::new();

        // Write both a small record and a huge multi-page (>64KB) record
        let mut big_series = Vec::new();
        for i in 0..1500u64 {
            big_series.push(RefSeries {
                ref_id: i + 1,
                labels: Labels::from_pairs([
                    ("__name__", "http_requests_total"),
                    ("instance", "host-01.prod.internal:8080"),
                    ("index", &format!("{i:05}")),
                ]),
            });
        }
        let rec_series = enc.series(&big_series, Vec::new());
        assert!(rec_series.len() > PAGE_SIZE * 2);

        let rec_samples = enc.samples(
            &[RefSample {
                ref_id: 1,
                st: 0,
                t: 1_700_000_000_000,
                v: 42.5,
            }],
            Vec::new(),
        );

        for comp in [
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Zstd,
        ] {
            let sub = dir.path().join(format!("{comp:?}"));
            let mut w = WlogWriter::open(&sub, comp).unwrap();
            w.log(&[&rec_series]).unwrap();
            w.log(&[&rec_samples]).unwrap();
            w.next_segment().unwrap();
            w.log(&[&rec_samples]).unwrap();
            w.close().unwrap();

            let mut r = WlogReader::open_dir(&sub).unwrap();
            let r1 = r.next_record().unwrap().unwrap();
            assert_eq!(dec.series(r1).unwrap().len(), 1500);
            let r2 = r.next_record().unwrap().unwrap();
            assert_eq!(dec.samples(r2).unwrap()[0].v, 42.5);
            let r3 = r.next_record().unwrap().unwrap();
            assert_eq!(dec.samples(r3).unwrap()[0].v, 42.5);
            assert!(r.next_record().unwrap().is_none());
        }
    }

    #[test]
    fn checkpoint_create_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let enc = RecordEncoder::new(false);
        let dec = RecordDecoder::new();

        let mut w = WlogWriter::open(dir.path(), CompressionType::Snappy).unwrap();
        let s_rec = enc.series(
            &[
                RefSeries {
                    ref_id: 1,
                    labels: Labels::from_pairs([("__name__", "keep")]),
                },
                RefSeries {
                    ref_id: 2,
                    labels: Labels::from_pairs([("__name__", "drop")]),
                },
            ],
            Vec::new(),
        );
        let samp_rec = enc.samples(
            &[
                RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 100,
                    v: 1.0,
                },
                RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 500,
                    v: 2.0,
                },
            ],
            Vec::new(),
        );
        w.log(&[&s_rec, &samp_rec]).unwrap();
        w.next_segment().unwrap();
        w.close().unwrap();

        create_checkpoint(
            dir.path(),
            0,
            0,
            |id| id == 1,
            300,
            false,
            CompressionType::Snappy,
        )
        .unwrap();

        assert_eq!(read_min_valid_time(dir.path()).unwrap(), Some(300));

        let (cp_dir, cp_idx) = last_checkpoint(dir.path()).unwrap().unwrap();
        assert_eq!(cp_idx, 0);
        let mut r = WlogReader::open_dir(&cp_dir).unwrap();
        let mut got_series = Vec::new();
        let mut got_samples = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            match dec.record_type(rec) {
                RecordType::Series => got_series.extend(dec.series(rec).unwrap()),
                RecordType::Samples => got_samples.extend(dec.samples(rec).unwrap()),
                _ => {}
            }
        }
        assert_eq!(got_series.len(), 1);
        assert_eq!(got_series[0].ref_id, 1);
        assert_eq!(got_samples.len(), 1);
        assert_eq!(got_samples[0].t, 500);
    }

    #[test]
    fn wal_corruption_detection_and_repair() {
        let dir = tempfile::tempdir().unwrap();
        let enc = RecordEncoder::new(false);
        let dec = RecordDecoder::new();

        let rec1 = enc.samples(
            &[RefSample {
                ref_id: 1,
                st: 0,
                t: 100,
                v: 1.0,
            }],
            Vec::new(),
        );
        let rec2 = enc.samples(
            &[RefSample {
                ref_id: 1,
                st: 0,
                t: 200,
                v: 2.0,
            }],
            Vec::new(),
        );

        {
            let mut w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();
            w.log(&[&rec1]).unwrap();
            w.log(&[&rec2]).unwrap();
            w.close().unwrap();
        }

        // Corrupt a byte inside the second record payload (second record starts at offset 7 + rec1.len())
        let seg0 = segment_name(dir.path(), 0);
        let mut bytes = fs::read(&seg0).unwrap();
        let corrupt_pos = RECORD_HEADER_SIZE + rec1.len() + RECORD_HEADER_SIZE + 1;
        bytes[corrupt_pos] ^= 0xff;
        fs::write(&seg0, &bytes).unwrap();

        let mut r = WlogReader::open_dir(dir.path()).unwrap();
        let first = r.next_record().unwrap().unwrap();
        assert_eq!(dec.samples(first).unwrap()[0].t, 100);
        let err = r.next_record().unwrap_err();
        let (corrupt_seg, corrupt_off) = match err {
            WlogError::Corruption {
                segment: Some(s),
                offset,
                ..
            } => (s, offset),
            other => panic!("expected corruption error, got {other:?}"),
        };
        drop(r);

        repair_wal(dir.path(), corrupt_seg, corrupt_off, CompressionType::None).unwrap();

        let mut r2 = WlogReader::open_dir(dir.path()).unwrap();
        let recovered = r2.next_record().unwrap().unwrap();
        assert_eq!(dec.samples(recovered).unwrap()[0].t, 100);
        assert!(r2.next_record().unwrap().is_none());
    }
}
