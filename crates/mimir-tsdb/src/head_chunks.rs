//! Bit-exact Prometheus `chunks_head` (`ChunkDiskMapper`) mmap-backed head chunk files.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunks/head_chunks.go
//! and github.com/prometheus/prometheus/tsdb/chunks/chunks.go.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

use memmap2::{Mmap, MmapOptions};
use thiserror::Error;

use crate::chunkenc::{ChunkError, EncodedChunk, Encoding};
use crate::encoding::{decode_uvarint, put_uvarint};

pub const MAGIC_HEAD_CHUNKS: u32 = 0x0130_BC91;
pub const MAGIC_CHUNKS_SIZE: usize = 4;
pub const HEAD_CHUNKS_FORMAT_V1: u8 = 1;
pub const CHUNKS_FORMAT_VERSION_SIZE: usize = 1;
pub const SEGMENT_HEADER_PADDING_SIZE: usize = 3;
pub const HEAD_CHUNK_FILE_HEADER_SIZE: usize =
    MAGIC_CHUNKS_SIZE + CHUNKS_FORMAT_VERSION_SIZE + SEGMENT_HEADER_PADDING_SIZE;

pub const MAX_HEAD_CHUNK_FILE_SIZE: usize = 128 * 1024 * 1024;
/// Go `HeadChunkFilePreallocationSize` (`MinWriteBufferSize * 2` = 128 KiB on non-Windows).
pub const HEAD_CHUNK_FILE_PREALLOCATION_SIZE: usize = 131_072;
pub const SERIES_REF_SIZE: usize = 8;
pub const MINT_MAXT_SIZE: usize = 8;
pub const CHUNK_ENCODING_SIZE: usize = 1;
pub const MAX_CHUNK_LENGTH_FIELD_SIZE: usize = 5;
pub const CRC_SIZE: usize = 4;
pub const MAX_HEAD_CHUNK_META_SIZE: usize =
    SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE + CHUNK_ENCODING_SIZE + MAX_CHUNK_LENGTH_FIELD_SIZE + CRC_SIZE;

pub const OUT_OF_ORDER_MASK: u8 = 0b1000_0000;
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum HeadChunksError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("corruption in head chunk file {file_index} ({dir:?}): {message}")]
    Corruption {
        dir: PathBuf,
        file_index: i64,
        message: String,
    },
    /// Invalid head chunk file header (size / magic / version). Matches the plain
    /// (non-`CorruptionErr`) errors of Go `ChunkDiskMapper.openMMapFiles`.
    #[error("{path:?}: {message}")]
    InvalidFile { path: PathBuf, message: String },
    #[error("unsequential head chunk files: {prev} followed by {next}")]
    NonSequentialFiles { prev: usize, next: usize },
    #[error("chunk decode error: {0}")]
    Chunk(#[from] ChunkError),
    #[error("head chunks writer closed")]
    Closed,
    /// Go `Head.loadMmappedChunks` out-of-sequence in-order m-mapped chunk (plain error,
    /// not a `CorruptionErr`).
    #[error("out of sequence m-mapped chunk for series ref {series_ref}, last chunk: [{last_mint}, {last_maxt}], new: [{mint}, {maxt}]")]
    OutOfSequenceChunk {
        series_ref: u64,
        last_mint: i64,
        last_maxt: i64,
        mint: i64,
        maxt: i64,
    },
    /// Go `ChunkDiskMapper.DeleteCorrupted`: `cannot handle error: %w`.
    #[error("cannot handle error: {0}")]
    CannotHandle(String),
}

/// 64-bit packed reference to a head chunk on disk: `(seq << 32) | offset`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ChunkDiskMapperRef(pub u64);

impl ChunkDiskMapperRef {
    #[inline]
    pub const fn new(seq: u32, offset: u32) -> Self {
        Self(((seq as u64) << 32) | (offset as u64))
    }

    #[inline]
    pub const fn unpack(self) -> (u32, u32) {
        ((self.0 >> 32) as u32, (self.0 & 0xffff_ffff) as u32)
    }

    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Metadata yielded when iterating over all chunks in `chunks_head`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadChunkRecord {
    pub series_ref: u64,
    pub chunk_ref: ChunkDiskMapperRef,
    pub mint: i64,
    pub maxt: i64,
    pub num_samples: u16,
    pub encoding: Encoding,
    pub is_ooo: bool,
}

#[inline]
pub fn head_chunk_file_path(dir: &Path, seq: usize) -> PathBuf {
    dir.join(format!("{seq:06}"))
}

fn list_chunk_files(dir: &Path) -> Result<BTreeMap<usize, PathBuf>, HeadChunksError> {
    let mut res = BTreeMap::new();
    if !dir.exists() {
        return Ok(res);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(name_str) = fname.to_str() else {
            continue;
        };
        if let Ok(seq) = name_str.parse::<usize>() {
            res.insert(seq, entry.path());
        }
    }
    Ok(res)
}

fn repair_last_chunk_file(files: &mut BTreeMap<usize, PathBuf>) -> Result<(), HeadChunksError> {
    let Some((&last_seq, last_path)) = files.iter().next_back() else {
        return Ok(());
    };
    if last_seq == 0 {
        return Ok(());
    }
    let mut f = File::open(last_path)?;
    let mut buf = [0u8; MAGIC_CHUNKS_SIZE];
    let n = f.read(&mut buf)?;
    drop(f);

    if n < MAGIC_CHUNKS_SIZE || u32::from_be_bytes(buf) == 0 {
        match fs::remove_file(last_path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(HeadChunksError::Io(e)),
        }
        files.remove(&last_seq);
    }
    Ok(())
}

/// Fault-injection decision for testing `ENOSPC`/`EIO` write rollbacks.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub enum WriteFaultAction {
    PassThrough,
    PartialThenError {
        prefix_len: usize,
        err_kind: io::ErrorKind,
    },
    Error(io::ErrorKind),
}

/// Fault-injection hook invoked per write call with the slice length being written.
#[doc(hidden)]
pub type WriteFaultHook = Arc<dyn Fn(usize) -> WriteFaultAction + Send + Sync>;

/// Writer for Prometheus `chunks_head/000001`, `000002`, ... files.
pub struct HeadChunksWriter {
    dir: PathBuf,
    max_file_size: usize,
    write_buffer_size: usize,
    queue_metrics: Option<crate::metrics::ChunkWriteQueueMetrics>,
    seq: usize,
    offset: usize,
    file_offset: usize,
    cur_file: Option<File>,
    write_buf: Vec<u8>,
    record_buf: Vec<u8>,
    cut_on_next_chunk: bool,
    discard_epoch: u64,
    last_written_ref: ChunkDiskMapperRef,
    last_flushed_ref: ChunkDiskMapperRef,
    /// Maximum `ChunkDiskMapperRef` actually flushed to disk during each past `discard_epoch`.
    /// Indexed by `epoch as usize` for `epoch < self.discard_epoch`.
    flushed_up_to_by_epoch: smallvec::SmallVec<[ChunkDiskMapperRef; 4]>,
    closed: bool,
    fault_hook: Option<WriteFaultHook>,
}

#[derive(Clone, Debug)]
pub(crate) struct FlushedRefWatermark {
    last_flushed_ref: ChunkDiskMapperRef,
    discard_epoch: u64,
    flushed_up_to_by_epoch: smallvec::SmallVec<[ChunkDiskMapperRef; 4]>,
}

impl FlushedRefWatermark {
    #[inline]
    pub(crate) fn last_flushed_ref(&self) -> ChunkDiskMapperRef {
        self.last_flushed_ref
    }

    #[inline]
    pub(crate) fn is_flushed(&self, ref_id: ChunkDiskMapperRef, write_epoch: Option<u64>) -> bool {
        if ref_id.as_u64() == 0 {
            return false;
        }
        let Some(ep) = write_epoch else {
            return false;
        };
        if ep == self.discard_epoch {
            ref_id <= self.last_flushed_ref
        } else if let Some(&max_flushed) = self.flushed_up_to_by_epoch.get(ep as usize) {
            ref_id <= max_flushed
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn is_discarded(&self, ref_id: ChunkDiskMapperRef, write_epoch: Option<u64>) -> bool {
        if ref_id.as_u64() == 0 {
            return true;
        }
        let Some(ep) = write_epoch else {
            return true;
        };
        if ep == self.discard_epoch {
            false
        } else if let Some(&max_flushed) = self.flushed_up_to_by_epoch.get(ep as usize) {
            ref_id > max_flushed
        } else {
            true
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct HeadChunksSnapshot {
    seq: usize,
    offset: usize,
    file_offset: usize,
    write_buf_len: usize,
    cut_on_next_chunk: bool,
    discard_epoch: u64,
    last_written_ref: ChunkDiskMapperRef,
    last_flushed_ref: ChunkDiskMapperRef,
}

impl HeadChunksWriter {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, HeadChunksError> {
        Self::with_max_file_size(dir, MAX_HEAD_CHUNK_FILE_SIZE)
    }

    pub fn with_max_file_size(
        dir: impl AsRef<Path>,
        max_file_size: usize,
    ) -> Result<Self, HeadChunksError> {
        Self::with_options(dir, max_file_size, DEFAULT_WRITE_BUFFER_SIZE)
    }

    /// Opens the writer with Go `HeadChunksWriteBufferSize` (`write_buffer_size`).
    pub fn with_options(
        dir: impl AsRef<Path>,
        max_file_size: usize,
        write_buffer_size: usize,
    ) -> Result<Self, HeadChunksError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut files = list_chunk_files(&dir)?;
        repair_last_chunk_file(&mut files)?;
        let seq = files.keys().next_back().copied().unwrap_or(0);

        Ok(Self {
            dir,
            max_file_size,
            write_buffer_size: write_buffer_size.max(1),
            queue_metrics: None,
            seq,
            offset: 0,
            file_offset: 0,
            cur_file: None,
            write_buf: Vec::new(),
            record_buf: Vec::new(),
            cut_on_next_chunk: false,
            discard_epoch: 0,
            last_written_ref: ChunkDiskMapperRef(0),
            last_flushed_ref: ChunkDiskMapperRef(0),
            flushed_up_to_by_epoch: smallvec::SmallVec::new(),
            closed: false,
            fault_hook: None,
        })
    }

    #[inline]
    pub(crate) fn discard_epoch(&self) -> u64 {
        self.discard_epoch
    }

    #[inline]
    pub(crate) fn flushed_watermark(&self) -> FlushedRefWatermark {
        FlushedRefWatermark {
            last_flushed_ref: self.last_flushed_ref,
            discard_epoch: self.discard_epoch,
            flushed_up_to_by_epoch: self.flushed_up_to_by_epoch.clone(),
        }
    }

    #[inline]
    fn prealloc_len_u64(&self) -> u64 {
        #[cfg(unix)]
        {
            HEAD_CHUNK_FILE_PREALLOCATION_SIZE.min(self.max_file_size) as u64
        }
        #[cfg(not(unix))]
        {
            0
        }
    }

    pub(crate) fn take_snapshot(&self) -> HeadChunksSnapshot {
        HeadChunksSnapshot {
            seq: self.seq,
            offset: self.offset,
            file_offset: self.file_offset,
            write_buf_len: self.write_buf.len(),
            cut_on_next_chunk: self.cut_on_next_chunk,
            discard_epoch: self.discard_epoch,
            last_written_ref: self.last_written_ref,
            last_flushed_ref: self.last_flushed_ref,
        }
    }

    pub(crate) fn rollback_snapshot(&mut self, snap: HeadChunksSnapshot) {
        let snap_surviving_ref = if snap.seq == 0 || snap.write_buf_len == 0 {
            snap.last_flushed_ref
        } else {
            snap.last_written_ref
        };
        if self.seq > snap.seq {
            self.cur_file = None;
            for s in (snap.seq + 1)..=self.seq {
                let _ = fs::remove_file(head_chunk_file_path(&self.dir, s));
            }
            self.seq = snap.seq;
            self.file_offset = 0;
            self.offset = 0;
            self.write_buf.clear();
            self.cut_on_next_chunk = snap.cut_on_next_chunk;
            self.last_written_ref = snap_surviving_ref;
            if self.last_flushed_ref > snap_surviving_ref {
                self.last_flushed_ref = snap_surviving_ref;
            }
            for entry in self
                .flushed_up_to_by_epoch
                .iter_mut()
                .skip(snap.discard_epoch as usize)
            {
                if *entry > snap_surviving_ref {
                    *entry = snap_surviving_ref;
                }
            }
        } else if self.file_offset == snap.file_offset && self.discard_epoch == snap.discard_epoch {
            self.write_buf.truncate(snap.write_buf_len);
            self.offset = snap.offset;
            self.cut_on_next_chunk = snap.cut_on_next_chunk;
            self.last_written_ref = snap.last_written_ref;
        } else if self.file_offset >= snap.offset {
            let prealloc_len = self.prealloc_len_u64();
            if let Some(ref mut file) = self.cur_file {
                let saved_pos = snap.offset as u64;
                let rollback_len = if saved_pos >= HEAD_CHUNK_FILE_HEADER_SIZE as u64 {
                    saved_pos.max(prealloc_len)
                } else {
                    saved_pos
                };
                let _ = file.set_len(saved_pos);
                if rollback_len > saved_pos {
                    let _ = file.set_len(rollback_len);
                }
                let _ = file.seek(SeekFrom::Start(saved_pos));
            }
            self.file_offset = snap.offset;
            self.offset = snap.offset;
            self.write_buf.clear();
            self.cut_on_next_chunk = snap.cut_on_next_chunk;
            self.last_written_ref = snap_surviving_ref;
            if self.last_flushed_ref > snap_surviving_ref {
                self.last_flushed_ref = snap_surviving_ref;
            }
            for entry in self
                .flushed_up_to_by_epoch
                .iter_mut()
                .skip(snap.discard_epoch as usize)
            {
                if *entry > snap_surviving_ref {
                    *entry = snap_surviving_ref;
                }
            }
        } else {
            self.write_buf.clear();
            self.offset = self.file_offset;
            self.last_written_ref = self.last_flushed_ref;
        }
    }

    /// Installs or clears a write fault-injection hook (used by durability tests).
    #[doc(hidden)]
    pub fn set_write_fault_hook(&mut self, hook: Option<WriteFaultHook>) {
        self.fault_hook = hook;
    }

    fn write_all_with_hook(
        file: &mut File,
        hook: Option<&WriteFaultHook>,
        buf: &[u8],
    ) -> io::Result<()> {
        let mut written = 0usize;
        while written < buf.len() {
            let slice = &buf[written..];
            let res = match hook {
                None => file.write(slice),
                Some(h) => match h(slice.len()) {
                    WriteFaultAction::PassThrough => file.write(slice),
                    WriteFaultAction::PartialThenError {
                        prefix_len,
                        err_kind,
                    } => {
                        let p = prefix_len.min(slice.len());
                        if p > 0 {
                            file.write_all(&slice[..p])?;
                        }
                        Err(io::Error::new(err_kind, "injected head chunks write fault"))
                    }
                    WriteFaultAction::Error(err_kind) => {
                        Err(io::Error::new(err_kind, "injected head chunks write fault"))
                    }
                },
            };
            match res {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole head chunk buffer",
                    ));
                }
                Ok(n) => written += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn flush_write_buf(&mut self) -> Result<(), HeadChunksError> {
        if self.write_buf.is_empty() {
            return Ok(());
        }
        let prealloc_len = self.prealloc_len_u64();
        let Some(ref mut file) = self.cur_file else {
            return Ok(());
        };
        let saved_pos = self.file_offset as u64;
        if let Err(e) = Self::write_all_with_hook(file, self.fault_hook.as_ref(), &self.write_buf) {
            let rollback_len = if saved_pos >= HEAD_CHUNK_FILE_HEADER_SIZE as u64 {
                saved_pos.max(prealloc_len)
            } else {
                saved_pos
            };
            let _ = file.set_len(saved_pos);
            if rollback_len > saved_pos {
                let _ = file.set_len(rollback_len);
            }
            let _ = file.seek(SeekFrom::Start(saved_pos));
            self.offset = self.file_offset;
            self.write_buf.clear();
            self.last_written_ref = self.last_flushed_ref;
            self.flushed_up_to_by_epoch.push(self.last_flushed_ref);
            self.discard_epoch = self.discard_epoch.wrapping_add(1);
            return Err(HeadChunksError::Io(e));
        }
        self.file_offset += self.write_buf.len();
        self.write_buf.clear();
        self.last_flushed_ref = self.last_written_ref;
        Ok(())
    }

    pub fn cut_new_file(&mut self) -> Result<(), HeadChunksError> {
        if self.closed {
            return Err(HeadChunksError::Closed);
        }
        self.finalize_current()?;
        let next_seq = self.seq + 1;
        let path = head_chunk_file_path(&self.dir, next_seq);
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;

        let prealloc_len = self.prealloc_len_u64();
        if prealloc_len > 0 {
            if let Err(e) = f.set_len(prealloc_len).and_then(|()| f.seek(SeekFrom::Start(0))) {
                drop(f);
                let _ = fs::remove_file(&path);
                return Err(HeadChunksError::Io(e));
            }
        }

        let mut hdr = [0u8; HEAD_CHUNK_FILE_HEADER_SIZE];
        hdr[..MAGIC_CHUNKS_SIZE].copy_from_slice(&MAGIC_HEAD_CHUNKS.to_be_bytes());
        hdr[MAGIC_CHUNKS_SIZE] = HEAD_CHUNKS_FORMAT_V1;
        if let Err(e) = Self::write_all_with_hook(&mut f, self.fault_hook.as_ref(), &hdr)
            .and_then(|()| f.flush())
        {
            drop(f);
            let _ = fs::remove_file(&path);
            return Err(HeadChunksError::Io(e));
        }

        self.seq = next_seq;
        self.cur_file = Some(f);
        self.file_offset = HEAD_CHUNK_FILE_HEADER_SIZE;
        self.offset = HEAD_CHUNK_FILE_HEADER_SIZE;
        self.write_buf.clear();
        self.cut_on_next_chunk = false;
        Ok(())
    }

    fn finalize_current(&mut self) -> Result<(), HeadChunksError> {
        if self.cur_file.is_some() {
            self.flush_write_buf()?;
            if let Some(ref file) = self.cur_file {
                file.sync_all()?;
            }
            self.cur_file = None;
        }
        Ok(())
    }

    /// Writes a chunk to `chunks_head` and returns its packed [`ChunkDiskMapperRef`].
    pub fn write_chunk(
        &mut self,
        series_ref: u64,
        mint: i64,
        maxt: i64,
        chunk: &EncodedChunk,
        is_ooo: bool,
    ) -> Result<ChunkDiskMapperRef, HeadChunksError> {
        if self.closed {
            return Err(HeadChunksError::Closed);
        }

        let data = chunk.as_bytes();
        let mut meta_buf = [0u8; MAX_HEAD_CHUNK_META_SIZE];
        meta_buf[0..8].copy_from_slice(&series_ref.to_be_bytes());
        meta_buf[8..16].copy_from_slice(&mint.to_be_bytes());
        meta_buf[16..24].copy_from_slice(&maxt.to_be_bytes());
        let mut enc_byte = chunk.encoding().as_u8();
        if is_ooo {
            enc_byte |= OUT_OF_ORDER_MASK;
        }
        meta_buf[24] = enc_byte;
        let ulen = put_uvarint(&mut meta_buf[25..], data.len() as u64);
        let hdr_len = 25 + ulen;

        let crc = crc32c::crc32c(&meta_buf[..hdr_len]);
        let crc = crc32c::crc32c_append(crc, data);

        self.record_buf.clear();
        self.record_buf.reserve(hdr_len + data.len() + CRC_SIZE);
        self.record_buf.extend_from_slice(&meta_buf[..hdr_len]);
        self.record_buf.extend_from_slice(data);
        self.record_buf.extend_from_slice(&crc.to_be_bytes());
        let needed = self.record_buf.len();

        if self.cut_on_next_chunk
            || self.cur_file.is_none()
            || self.offset + needed > self.max_file_size
        {
            self.cut_new_file()?;
        }

        let chunk_offset = self.offset;
        let ref_id = ChunkDiskMapperRef::new(self.seq as u32, chunk_offset as u32);
        let mut flushed_now = false;
        if self.write_buf.len() + needed > self.write_buffer_size {
            self.flush_write_buf()?;
            if needed >= self.write_buffer_size {
                let prealloc_len = self.prealloc_len_u64();
                let file = self.cur_file.as_mut().ok_or(HeadChunksError::Closed)?;
                let saved_pos = self.offset as u64;
                if let Err(e) =
                    Self::write_all_with_hook(file, self.fault_hook.as_ref(), &self.record_buf)
                {
                    let rollback_len = if saved_pos >= HEAD_CHUNK_FILE_HEADER_SIZE as u64 {
                        saved_pos.max(prealloc_len)
                    } else {
                        saved_pos
                    };
                    let _ = file.set_len(saved_pos);
                    if rollback_len > saved_pos {
                        let _ = file.set_len(rollback_len);
                    }
                    let _ = file.seek(SeekFrom::Start(saved_pos));
                    return Err(HeadChunksError::Io(e));
                }
                self.file_offset += needed;
                self.offset += needed;
                flushed_now = true;
            } else {
                self.write_buf.extend_from_slice(&self.record_buf);
                self.offset += needed;
            }
        } else {
            self.write_buf.extend_from_slice(&self.record_buf);
            self.offset += needed;
        }

        self.last_written_ref = ref_id;
        if flushed_now {
            self.last_flushed_ref = ref_id;
        }
        if let Some(m) = self.queue_metrics.as_ref() {
            m.adds.inc();
            m.completed.inc();
        }
        Ok(ref_id)
    }

    /// Attaches the Go `chunk_write_queue` operation counters.
    pub fn set_queue_metrics(&mut self, m: crate::metrics::ChunkWriteQueueMetrics) {
        self.queue_metrics = Some(m);
    }

    pub fn flush(&mut self) -> Result<(), HeadChunksError> {
        self.flush_write_buf()
    }

    pub fn has_active_file(&self) -> bool {
        self.cur_file.is_some()
    }

    pub fn cur_file_has_chunks(&self) -> bool {
        self.cur_file.is_some() && self.offset > HEAD_CHUNK_FILE_HEADER_SIZE
    }

    pub fn cut_file_on_next_chunk(&mut self) {
        self.cut_on_next_chunk = true;
    }

    pub fn current_seq(&self) -> usize {
        self.seq
    }

    /// Ensures sequence numbering never regresses below `min_seq` across startup/truncation
    /// (matching Go `ChunkDiskMapper` monotonic `evtlPos.seq`).
    pub fn ensure_min_seq(&mut self, min_seq: usize) {
        if min_seq > self.seq {
            self.seq = min_seq;
        }
    }

    pub fn close(mut self) -> Result<(), HeadChunksError> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), HeadChunksError> {
        if self.closed {
            return Ok(());
        }
        self.finalize_current()?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for HeadChunksWriter {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

/// Memory-mapped reader for Prometheus `chunks_head/000001`, ... files.
pub struct HeadChunksReader {
    dir: PathBuf,
    files: BTreeMap<usize, Mmap>,
}

impl HeadChunksReader {
    /// Opens and memory-maps all head chunk files in `dir`, repairing any empty/zeroed
    /// trailing file left behind by a crash before header write (startup only).
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, HeadChunksError> {
        Self::open_inner(dir.as_ref(), true)
    }

    /// Opens and memory-maps all head chunk files in `dir` WITHOUT repairing or deleting
    /// files on disk (safe for runtime refreshes while [`HeadChunksWriter`] is active).
    pub fn open_no_repair(dir: impl AsRef<Path>) -> Result<Self, HeadChunksError> {
        Self::open_inner(dir.as_ref(), false)
    }

    fn open_inner(dir: &Path, repair: bool) -> Result<Self, HeadChunksError> {
        let dir = dir.to_path_buf();
        let mut paths = list_chunk_files(&dir)?;
        if repair {
            repair_last_chunk_file(&mut paths)?;
        }

        let mut files = BTreeMap::new();
        let mut prev_seq: Option<usize> = None;

        for (seq, path) in paths {
            if let Some(prev) = prev_seq {
                if seq != prev + 1 {
                    return Err(HeadChunksError::NonSequentialFiles { prev, next: seq });
                }
            }
            prev_seq = Some(seq);

            let mmap = Self::map_and_validate_file(&path)?;
            files.insert(seq, mmap);
        }

        Ok(Self { dir, files })
    }

    fn map_and_validate_file(path: &Path) -> Result<Mmap, HeadChunksError> {
        let file = File::open(path)?;
        // SAFETY: files are opened read-only for mmap inspection of persisted head chunks.
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        // Header validation errors are NOT `CorruptionErr` in Go
        // (`openMMapFiles` returns plain `fmt.Errorf`), so they are fatal for
        // `NewHead` and never trigger the m-map chunk repair path.
        if mmap.len() < HEAD_CHUNK_FILE_HEADER_SIZE {
            return Err(HeadChunksError::InvalidFile {
                path: path.to_path_buf(),
                message: "invalid head chunk file header: invalid size".to_string(),
            });
        }
        let magic = u32::from_be_bytes([mmap[0], mmap[1], mmap[2], mmap[3]]);
        if magic != MAGIC_HEAD_CHUNKS {
            return Err(HeadChunksError::InvalidFile {
                path: path.to_path_buf(),
                message: format!("invalid magic number {magic:x}"),
            });
        }
        let version = mmap[MAGIC_CHUNKS_SIZE];
        if version != HEAD_CHUNKS_FORMAT_V1 {
            return Err(HeadChunksError::InvalidFile {
                path: path.to_path_buf(),
                message: format!("invalid chunk format version {version}"),
            });
        }
        Ok(mmap)
    }

    /// Refreshes mapped segments in place without repairing/deleting files on disk.
    /// Immutable segments below the previous tail are retained without re-mapping;
    /// the previous tail is re-mapped if its file length grew, and new segments are mapped.
    pub fn refresh_in_place(&mut self) -> Result<(), HeadChunksError> {
        let paths = list_chunk_files(&self.dir)?;
        let prev_tail_seq = self.files.keys().next_back().copied();

        let stale_seqs: Vec<usize> = self
            .files
            .keys()
            .copied()
            .filter(|seq| !paths.contains_key(seq))
            .collect();
        for seq in stale_seqs {
            self.files.remove(&seq);
        }

        let mut prev_seq: Option<usize> = None;
        for (seq, path) in paths {
            if let Some(prev) = prev_seq {
                if seq != prev + 1 {
                    return Err(HeadChunksError::NonSequentialFiles { prev, next: seq });
                }
            }
            prev_seq = Some(seq);

            if let Some(existing) = self.files.get(&seq) {
                if prev_tail_seq == Some(seq) {
                    let disk_len = fs::metadata(&path)?.len() as usize;
                    if disk_len != existing.len() {
                        self.files.remove(&seq);
                        let mmap = Self::map_and_validate_file(&path)?;
                        self.files.insert(seq, mmap);
                    }
                }
                continue;
            }

            let mmap = Self::map_and_validate_file(&path)?;
            self.files.insert(seq, mmap);
        }

        Ok(())
    }

    #[inline]
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    #[inline]
    pub fn has_file(&self, seq: usize) -> bool {
        self.files.contains_key(&seq)
    }

    pub fn max_seq(&self) -> usize {
        self.files.keys().next_back().copied().unwrap_or(0)
    }

    /// Reads and CRC-validates the chunk referenced by `chunk_ref`.
    pub fn chunk(&self, chunk_ref: ChunkDiskMapperRef) -> Result<EncodedChunk, HeadChunksError> {
        let (seq_u32, offset_u32) = chunk_ref.unpack();
        let seq = seq_u32 as usize;
        let start_of_entry = offset_u32 as usize;

        let mmap = self.files.get(&seq).ok_or_else(|| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: format!("head chunk file index {seq} does not exist"),
        })?;

        let chk_start = start_of_entry
            .checked_add(SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE)
            .ok_or_else(|| HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "chunk offset overflow".to_string(),
            })?;
        if chk_start
            .checked_add(MAX_CHUNK_LENGTH_FIELD_SIZE)
            .is_none_or(|end| end > mmap.len())
        {
            return Err(HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "not enough bytes to read chunk length".to_string(),
            });
        }

        let raw_enc = mmap[chk_start] & !OUT_OF_ORDER_MASK;
        let encoding = Encoding::from_u8(raw_enc).map_err(|e| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: e.to_string(),
        })?;

        let len_start = chk_start + CHUNK_ENCODING_SIZE;
        let len_slice = &mmap[len_start..(len_start + MAX_CHUNK_LENGTH_FIELD_SIZE).min(mmap.len())];
        let (data_len, ulen) =
            decode_uvarint(len_slice).ok_or_else(|| HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "invalid chunk data length uvarint".to_string(),
            })?;
        let data_len = usize::try_from(data_len).map_err(|_| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: "chunk data length overflow".to_string(),
        })?;

        let data_start = len_start + ulen;
        let data_end = data_start.checked_add(data_len).ok_or_else(|| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: "chunk data length overflow".to_string(),
        })?;
        let chk_end = data_end.checked_add(CRC_SIZE).ok_or_else(|| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: "chunk data length overflow".to_string(),
        })?;
        if chk_end > mmap.len() {
            return Err(HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "not enough bytes to read chunk data and CRC".to_string(),
            });
        }

        let expected_crc = u32::from_be_bytes([
            mmap[data_end],
            mmap[data_end + 1],
            mmap[data_end + 2],
            mmap[data_end + 3],
        ]);
        let actual_crc = crc32c::crc32c(&mmap[start_of_entry..data_end]);
        if actual_crc != expected_crc {
            return Err(HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: format!("checksum mismatch expected:{expected_crc:#x}, actual:{actual_crc:#x}"),
            });
        }

        EncodedChunk::from_bytes(encoding, mmap[data_start..data_end].to_vec()).map_err(|e| {
            HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: e.to_string(),
            }
        })
    }

    /// Iterates over every chunk across all head chunk files in ascending sequence order,
    /// verifying CRC32C checksums for each entry.
    pub fn iterate_all_chunks<F>(&self, mut f: F) -> Result<(), HeadChunksError>
    where
        F: FnMut(HeadChunkRecord) -> Result<(), HeadChunksError>,
    {
        for (&seq, mmap) in &self.files {
            let file_end = mmap.len();
            let mut idx = HEAD_CHUNK_FILE_HEADER_SIZE;

            while idx < file_end {
                if file_end - idx < MAX_HEAD_CHUNK_META_SIZE {
                    if mmap[idx..file_end].iter().all(|&b| b == 0) {
                        break;
                    }
                    return Err(HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: format!(
                            "trailing non-zero bytes without complete chunk header at offset {idx}"
                        ),
                    });
                }

                let start_idx = idx;
                let chunk_ref = ChunkDiskMapperRef::new(seq as u32, start_idx as u32);

                let series_ref = u64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                idx += SERIES_REF_SIZE;
                let mint = i64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                idx += MINT_MAXT_SIZE;
                let maxt = i64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                idx += MINT_MAXT_SIZE;

                if series_ref == 0 && mint == 0 && maxt == 0 {
                    break;
                }

                let raw_enc = mmap[idx];
                idx += CHUNK_ENCODING_SIZE;
                let is_ooo = (raw_enc & OUT_OF_ORDER_MASK) != 0;
                // Go `IterateAllChunks` does not validate the encoding; `loadMmappedChunks`
                // skips chunks with an invalid encoding. Map unknown values to `None`
                // (not a valid chunk encoding) so the caller skips them the same way.
                let encoding =
                    Encoding::from_u8(raw_enc & !OUT_OF_ORDER_MASK).unwrap_or(Encoding::None);

                let (data_len, ulen) = decode_uvarint(
                    &mmap[idx..(idx + MAX_CHUNK_LENGTH_FIELD_SIZE).min(file_end)],
                )
                .ok_or_else(|| HeadChunksError::Corruption {
                    dir: self.dir.clone(),
                    file_index: seq as i64,
                    message: "invalid chunk length uvarint".to_string(),
                })?;
                idx += ulen;

                let data_len = usize::try_from(data_len).map_err(|_| HeadChunksError::Corruption {
                    dir: self.dir.clone(),
                    file_index: seq as i64,
                    message: "chunk data length overflow".to_string(),
                })?;
                let entry_end = idx
                    .checked_add(data_len)
                    .and_then(|end| end.checked_add(CRC_SIZE))
                    .ok_or_else(|| HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: "chunk data length overflow".to_string(),
                    })?;
                if data_len < 2 || entry_end > file_end {
                    return Err(HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: "chunk data extends past end of head chunk file".to_string(),
                    });
                }

                let raw_num_samples = u16::from_be_bytes([mmap[idx], mmap[idx + 1]]);
                let num_samples = match encoding {
                    Encoding::HistogramSt | Encoding::FloatHistogramSt => raw_num_samples & 0x3FFF,
                    _ => raw_num_samples,
                };
                idx += data_len;

                let expected_crc =
                    u32::from_be_bytes(mmap[idx..idx + CRC_SIZE].try_into().unwrap());
                let actual_crc = crc32c::crc32c(&mmap[start_idx..idx]);
                if actual_crc != expected_crc {
                    return Err(HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: format!(
                            "checksum mismatch expected:{expected_crc:#x}, actual:{actual_crc:#x}"
                        ),
                    });
                }
                idx += CRC_SIZE;

                f(HeadChunkRecord {
                    series_ref,
                    chunk_ref,
                    mint,
                    maxt,
                    num_samples,
                    encoding,
                    is_ooo,
                })?;
            }
        }
        Ok(())
    }

    /// Port of Go `ChunkDiskMapper.DeleteCorrupted`: deletes the corrupt head chunk file
    /// named by a [`HeadChunksError::Corruption`] and every file after it. Any other error
    /// cannot be handled and is returned as [`HeadChunksError::CannotHandle`].
    pub fn delete_corrupted(&mut self, original: &HeadChunksError) -> Result<(), HeadChunksError> {
        let HeadChunksError::Corruption { file_index, .. } = original else {
            return Err(HeadChunksError::CannotHandle(original.to_string()));
        };
        let segs: Vec<usize> = self
            .files
            .keys()
            .copied()
            .filter(|&seq| seq as i64 >= *file_index)
            .collect();
        self.delete_files(&segs)
    }

    /// Port of Go `ChunkDiskMapper.Truncate(math.MaxUint32)` at startup (no file is being
    /// written yet): deletes every head chunk file.
    pub fn delete_all(&mut self) -> Result<(), HeadChunksError> {
        let segs: Vec<usize> = self.files.keys().copied().collect();
        self.delete_files(&segs)
    }

    /// Port of Go `ChunkDiskMapper.deleteFiles`: unmaps and removes the given files,
    /// stopping at the first removal error.
    fn delete_files(&mut self, segs: &[usize]) -> Result<(), HeadChunksError> {
        for &seq in segs {
            self.files.remove(&seq);
        }
        for &seq in segs {
            fs::remove_file(head_chunk_file_path(&self.dir, seq))?;
        }
        Ok(())
    }

    /// Collects all [`HeadChunkRecord`]s into a vector.
    pub fn collect_all_chunks(&self) -> Result<Vec<HeadChunkRecord>, HeadChunksError> {
        let mut out = Vec::new();
        self.iterate_all_chunks(|rec| {
            out.push(rec);
            Ok(())
        })?;
        Ok(out)
    }

    /// Unmaps and deletes all head chunk files with sequence `< before_seq` in place.
    pub fn truncate_in_place(&mut self, before_seq: usize) -> Result<(), HeadChunksError> {
        self.truncate_in_place_except(before_seq, None)
    }

    /// Unmaps and deletes all head chunk files with sequence `< before_seq` except `active_seq`
    /// in place (`ChunkDiskMapper.Truncate` in Go `head_chunks.go`).
    pub fn truncate_in_place_except(
        &mut self,
        before_seq: usize,
        active_seq: Option<usize>,
    ) -> Result<(), HeadChunksError> {
        let to_remove: Vec<usize> = self
            .files
            .keys()
            .copied()
            .filter(|&seq| seq < before_seq && active_seq.is_none_or(|a| seq < a))
            .collect();
        for seq in to_remove {
            // Drop Mmap before unlinking so /proc/self/maps never retains `(deleted)` mappings.
            self.files.remove(&seq);
            let p = head_chunk_file_path(&self.dir, seq);
            match fs::remove_file(p) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(HeadChunksError::Io(e)),
            }
        }
        Ok(())
    }

    /// Deletes all head chunk files with sequence `< before_seq`.
    pub fn truncate(mut self, before_seq: usize) -> Result<Self, HeadChunksError> {
        self.truncate_in_place(before_seq)?;
        Ok(self)
    }

    /// Like [`Self::iterate_all_chunks`], also passing the raw masked-off encoding byte
    /// (`raw_enc & !OUT_OF_ORDER_MASK`) to `f`.
    pub fn iterate_all_chunks_raw<F>(&self, mut f: F) -> Result<(), HeadChunksError>
    where
        F: FnMut(HeadChunkRecord, u8) -> Result<(), HeadChunksError>,
    {
        for (&seq, mmap) in &self.files {
            let file_end = mmap.len();
            let mut idx = HEAD_CHUNK_FILE_HEADER_SIZE;

            while idx < file_end {
                if file_end - idx < MAX_HEAD_CHUNK_META_SIZE {
                    if mmap[idx..file_end].iter().all(|&b| b == 0) {
                        break;
                    }
                    return Err(HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: format!(
                            "trailing non-zero bytes without complete chunk header at offset {idx}"
                        ),
                    });
                }

                let start_idx = idx;
                let chunk_ref = ChunkDiskMapperRef::new(seq as u32, start_idx as u32);

                let series_ref = u64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                idx += SERIES_REF_SIZE;
                let mint = i64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                idx += MINT_MAXT_SIZE;
                let maxt = i64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                idx += MINT_MAXT_SIZE;

                if series_ref == 0 && mint == 0 && maxt == 0 {
                    break;
                }

                let raw_enc_byte = mmap[idx];
                idx += CHUNK_ENCODING_SIZE;
                let is_ooo = (raw_enc_byte & OUT_OF_ORDER_MASK) != 0;
                let stripped_enc = raw_enc_byte & !OUT_OF_ORDER_MASK;
                let encoding = Encoding::from_u8(stripped_enc).unwrap_or(Encoding::None);

                let (data_len, ulen) = decode_uvarint(
                    &mmap[idx..(idx + MAX_CHUNK_LENGTH_FIELD_SIZE).min(file_end)],
                )
                .ok_or_else(|| HeadChunksError::Corruption {
                    dir: self.dir.clone(),
                    file_index: seq as i64,
                    message: "invalid chunk length uvarint".to_string(),
                })?;
                idx += ulen;

                let data_len = usize::try_from(data_len).map_err(|_| HeadChunksError::Corruption {
                    dir: self.dir.clone(),
                    file_index: seq as i64,
                    message: "chunk data length overflow".to_string(),
                })?;
                let entry_end = idx
                    .checked_add(data_len)
                    .and_then(|end| end.checked_add(CRC_SIZE))
                    .ok_or_else(|| HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: "chunk data length overflow".to_string(),
                    })?;
                if data_len < 2 || entry_end > file_end {
                    return Err(HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: "chunk data extends past end of head chunk file".to_string(),
                    });
                }

                let raw_num_samples = u16::from_be_bytes([mmap[idx], mmap[idx + 1]]);
                let num_samples = match encoding {
                    Encoding::HistogramSt | Encoding::FloatHistogramSt => raw_num_samples & 0x3FFF,
                    _ => raw_num_samples,
                };
                idx += data_len;

                let expected_crc =
                    u32::from_be_bytes(mmap[idx..idx + CRC_SIZE].try_into().unwrap());
                let actual_crc = crc32c::crc32c(&mmap[start_idx..idx]);
                if actual_crc != expected_crc {
                    return Err(HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: format!(
                            "checksum mismatch expected:{expected_crc:#x}, actual:{actual_crc:#x}"
                        ),
                    });
                }
                idx += CRC_SIZE;

                f(
                    HeadChunkRecord {
                        series_ref,
                        chunk_ref,
                        mint,
                        maxt,
                        num_samples,
                        encoding,
                        is_ooo,
                    },
                    stripped_enc,
                )?;
            }
        }
        Ok(())
    }
}

/// Reads the header bytes from `rdr` into `buf` and drops/closes `rdr` (Go `readChunkHeaderAndClose`).
pub fn read_chunk_header_and_close<R: Read>(mut rdr: R, buf: &mut [u8]) -> io::Result<usize> {
    let res = rdr.read(buf);
    drop(rdr);
    res
}

/// Deletes `path` using `remove_fn`, retrying with exponential backoff when `is_retryable` returns true.
pub fn remove_chunk_file_with_retry<F, P>(
    path: &Path,
    mut remove_fn: F,
    mut is_retryable: P,
) -> io::Result<()>
where
    F: FnMut(&Path) -> io::Result<()>,
    P: FnMut(&io::Error) -> bool,
{
    const INITIAL_BACKOFF: Duration = Duration::from_millis(1);
    const MAX_BACKOFF: Duration = Duration::from_millis(250);
    const DEADLINE: Duration = Duration::from_secs(2);

    let mut backoff = INITIAL_BACKOFF;
    let start = Instant::now();
    loop {
        match remove_fn(path) {
            Ok(()) => return Ok(()),
            Err(e) if !is_retryable(&e) || start.elapsed() >= DEADLINE => return Err(e),
            Err(_) => {
                thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Repairs the last head chunk file in `files` using an injectable `remove_fn` and retry predicate.
pub fn repair_last_chunk_file_with_remover<F, P>(
    files: &mut BTreeMap<usize, PathBuf>,
    mut remove_fn: F,
    mut is_retryable: P,
) -> Result<(), HeadChunksError>
where
    F: FnMut(&Path) -> io::Result<()>,
    P: FnMut(&io::Error) -> bool,
{
    let Some((&last_seq, last_path)) = files.iter().next_back() else {
        return Ok(());
    };
    if last_seq == 0 {
        return Ok(());
    }
    let f = File::open(last_path)?;
    let mut buf = [0u8; MAGIC_CHUNKS_SIZE];
    let n = read_chunk_header_and_close(f, &mut buf)?;
    if n < MAGIC_CHUNKS_SIZE || u32::from_be_bytes(buf) == 0 {
        match remove_chunk_file_with_retry(last_path, &mut remove_fn, &mut is_retryable) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(HeadChunksError::Io(e)),
        }
        files.remove(&last_seq);
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub enum ChunkPayload {
    Encoded(EncodedChunk),
    Raw { encoding: u8, bytes: Vec<u8> },
}

impl ChunkPayload {
    #[inline]
    pub fn encoding_u8(&self) -> u8 {
        match self {
            Self::Encoded(c) => c.encoding().as_u8(),
            Self::Raw { encoding, .. } => *encoding,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Encoded(c) => c.as_bytes(),
            Self::Raw { bytes, .. } => bytes,
        }
    }

    #[inline]
    pub fn as_encoded(&self) -> Option<&EncodedChunk> {
        match self {
            Self::Encoded(c) => Some(c),
            Self::Raw { .. } => None,
        }
    }
}

pub type ChunkWriteCallback = Box<dyn FnOnce(Result<(), HeadChunksError>) + Send>;

pub struct ChunkWriteJob {
    pub series_ref: u64,
    pub mint: i64,
    pub maxt: i64,
    pub chk: Option<ChunkPayload>,
    pub r#ref: ChunkDiskMapperRef,
    pub cut_file: bool,
    pub is_ooo: bool,
    pub callback: Option<ChunkWriteCallback>,
}

impl Default for ChunkWriteJob {
    fn default() -> Self {
        Self {
            series_ref: 0,
            mint: 0,
            maxt: 0,
            chk: None,
            r#ref: ChunkDiskMapperRef(0),
            cut_file: false,
            is_ooo: false,
            callback: None,
        }
    }
}

struct QueueSegment {
    segment: Vec<Option<ChunkWriteJob>>,
    next_read: usize,
    next_write: usize,
}

struct WriteJobQueueInner {
    segments: VecDeque<QueueSegment>,
    size: usize,
    closed: bool,
}

/// Bounded segmented blocking FIFO queue matching Go `tsdb/chunks/queue.go:writeJobQueue`.
pub struct WriteJobQueue {
    max_size: usize,
    segment_size: usize,
    inner: Mutex<WriteJobQueueInner>,
    pushed: Condvar,
    popped: Condvar,
}

impl WriteJobQueue {
    pub fn new(max_size: usize, segment_size: usize) -> Self {
        assert!(max_size > 0 && segment_size > 0, "invalid queue");
        Self {
            max_size,
            segment_size,
            inner: Mutex::new(WriteJobQueueInner {
                segments: VecDeque::new(),
                size: 0,
                closed: false,
            }),
            pushed: Condvar::new(),
            popped: Condvar::new(),
        }
    }

    pub fn close(&self) {
        let mut g = self.inner.lock();
        g.closed = true;
        self.pushed.notify_all();
        self.popped.notify_all();
    }

    pub fn push(&self, job: ChunkWriteJob) -> bool {
        let mut g = self.inner.lock();
        while !g.closed && g.size >= self.max_size {
            self.popped.wait(&mut g);
        }
        if g.closed {
            return false;
        }
        let need_new = match g.segments.back() {
            None => true,
            Some(last) => last.next_write >= self.segment_size,
        };
        if need_new {
            let mut segment = Vec::with_capacity(self.segment_size);
            segment.resize_with(self.segment_size, || None);
            g.segments.push_back(QueueSegment {
                segment,
                next_read: 0,
                next_write: 0,
            });
        }
        let last = g.segments.back_mut().unwrap();
        let nw = last.next_write;
        last.segment[nw] = Some(job);
        last.next_write += 1;
        g.size += 1;
        self.pushed.notify_one();
        true
    }

    pub fn pop(&self) -> Option<ChunkWriteJob> {
        let mut g = self.inner.lock();
        while g.size == 0 {
            if g.closed {
                return None;
            }
            self.pushed.wait(&mut g);
        }
        let pop_front = {
            let first = g.segments.front_mut().unwrap();
            let nr = first.next_read;
            let job = first.segment[nr].take().expect("queue slot occupied");
            first.next_read += 1;
            (job, first.next_read >= self.segment_size)
        };
        g.size -= 1;
        if pop_front.1 {
            g.segments.pop_front();
        }
        let job = pop_front.0;
        self.popped.notify_one();
        Some(job)
    }

    pub fn length(&self) -> usize {
        self.inner.lock().size
    }

    #[doc(hidden)]
    pub fn assert_invariants(&self) {
        let g = self.inner.lock();
        let mut total_size = 0usize;
        let len = g.segments.len();
        for (idx, s) in g.segments.iter().enumerate() {
            assert_eq!(s.segment.len(), self.segment_size);
            assert!(s.next_read <= s.next_write);
            total_size += s.next_write - s.next_read;
            let is_first = idx == 0;
            let is_last = idx + 1 == len;
            if !is_first {
                assert_eq!(s.next_read, 0);
            }
            if is_first && s.next_read == s.next_write {
                assert!(s.next_write < s.segment.len());
            }
            if !is_first && !is_last {
                assert_eq!(s.next_write, s.segment.len());
            }
            assert!(s.next_write > 0);
        }
        assert_eq!(g.size, total_size);
    }

    #[doc(hidden)]
    pub fn first_segment_indices(&self) -> Option<(usize, usize)> {
        self.inner
            .lock()
            .segments
            .front()
            .map(|s| (s.next_read, s.next_write))
    }
}

pub type WriteChunkFn = Arc<
    dyn Fn(u64, i64, i64, Option<ChunkPayload>, ChunkDiskMapperRef, bool, bool) -> Result<(), HeadChunksError>
        + Send
        + Sync,
>;

const MAX_CHUNK_QUEUE_SEGMENT_SIZE: usize = 8192;

struct ChunkRefMapState {
    map: HashMap<ChunkDiskMapperRef, ChunkPayload>,
    peak_size: usize,
    last_shrink: Instant,
}

/// Async non-blocking chunk write queue matching Go `tsdb/chunks/chunk_write_queue.go`.
pub struct ChunkWriteQueue {
    jobs: Arc<WriteJobQueue>,
    ref_map: Arc<RwLock<ChunkRefMapState>>,
    is_running: Mutex<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
    write_chunk: Arc<RwLock<WriteChunkFn>>,
    metrics: Option<crate::metrics::ChunkWriteQueueMetrics>,
}

impl ChunkWriteQueue {
    pub fn new(
        metrics: Option<crate::metrics::ChunkWriteQueueMetrics>,
        size: usize,
        write_chunk: WriteChunkFn,
    ) -> Self {
        let seg_size = size.min(MAX_CHUNK_QUEUE_SEGMENT_SIZE);
        let jobs = Arc::new(WriteJobQueue::new(size, seg_size));
        let ref_map = Arc::new(RwLock::new(ChunkRefMapState {
            map: HashMap::new(),
            peak_size: 0,
            last_shrink: Instant::now(),
        }));
        let write_chunk = Arc::new(RwLock::new(write_chunk));

        let q = Self {
            jobs,
            ref_map,
            is_running: Mutex::new(false),
            worker: Mutex::new(None),
            write_chunk,
            metrics,
        };
        q.start();
        q
    }

    fn start(&self) {
        let jobs = Arc::clone(&self.jobs);
        let ref_map = Arc::clone(&self.ref_map);
        let write_chunk = Arc::clone(&self.write_chunk);
        let metrics = self.metrics.clone();
        let handle = thread::Builder::new()
            .name("chunk-write-queue".to_string())
            .spawn(move || {
                while let Some(mut job) = jobs.pop() {
                    let f = Arc::clone(&*write_chunk.read());
                    let payload = job.chk.take();
                    let res = f(
                        job.series_ref,
                        job.mint,
                        job.maxt,
                        payload,
                        job.r#ref,
                        job.is_ooo,
                        job.cut_file,
                    );
                    if let Some(cb) = job.callback.take() {
                        cb(res);
                    }
                    let mut g = ref_map.write();
                    g.map.remove(&job.r#ref);
                    if let Some(ref m) = metrics {
                        m.completed.inc();
                    }
                    if g.map.is_empty()
                        && g.peak_size >= 1000
                        && g.last_shrink.elapsed() >= Duration::from_secs(600)
                    {
                        let new_cap = g.peak_size / 2;
                        g.map = HashMap::with_capacity(new_cap);
                        g.peak_size = 0;
                        g.last_shrink = Instant::now();
                    }
                }
            })
            .expect("spawn chunk-write-queue worker");
        *self.worker.lock() = Some(handle);
        *self.is_running.lock() = true;
    }

    pub fn set_write_chunk_for_test(&self, f: WriteChunkFn) {
        *self.write_chunk.write() = f;
    }

    pub fn write_chunk_fn(&self) -> WriteChunkFn {
        Arc::clone(&*self.write_chunk.read())
    }

    pub fn add_job(&self, job: ChunkWriteJob) -> Result<(), HeadChunksError> {
        let mut running = self.is_running.lock();
        if !*running {
            return Err(HeadChunksError::Closed);
        }
        {
            let mut g = self.ref_map.write();
            let val = job.chk.clone().unwrap_or(ChunkPayload::Raw {
                encoding: 0,
                bytes: Vec::new(),
            });
            g.map.insert(job.r#ref, val);
            if g.map.len() > g.peak_size {
                g.peak_size = g.map.len();
            }
        }
        let ref_id = job.r#ref;
        if !self.jobs.push(job) {
            self.ref_map.write().map.remove(&ref_id);
            *running = false;
            return Err(HeadChunksError::Closed);
        }
        if let Some(ref m) = self.metrics {
            m.adds.inc();
        }
        Ok(())
    }

    pub fn get(&self, ref_id: ChunkDiskMapperRef) -> Option<ChunkPayload> {
        let g = self.ref_map.read();
        g.map.get(&ref_id).cloned()
    }

    pub fn queue_size(&self) -> usize {
        self.ref_map.read().map.len()
    }

    pub fn queue_is_empty(&self) -> bool {
        self.queue_size() == 0
    }

    pub fn queue_is_full(&self) -> bool {
        self.queue_size() == self.jobs.max_size + 1
    }

    pub fn stop(&self) {
        let mut running = self.is_running.lock();
        if !*running {
            return;
        }
        *running = false;
        drop(running);
        self.jobs.close();
        if let Some(handle) = self.worker.lock().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ChunkWriteQueue {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Default)]
struct EvtlPos {
    seq: u64,
    offset: u64,
    cut_file: bool,
}

impl EvtlPos {
    fn bytes_to_write_for_chunk(chk_len: usize) -> u64 {
        let mut len_buf = [0u8; MAX_CHUNK_LENGTH_FIELD_SIZE];
        let ulen = put_uvarint(&mut len_buf, chk_len as u64);
        (SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE + CHUNK_ENCODING_SIZE + ulen + chk_len + CRC_SIZE)
            as u64
    }

    fn should_cut_new_file(&self, bytes_to_write: u64) -> bool {
        if self.cut_file || self.seq == 0 {
            return true;
        }
        (self.offset + bytes_to_write) > MAX_HEAD_CHUNK_FILE_SIZE as u64
    }

    fn advance_to_new_file(&mut self) {
        self.seq += 1;
        self.offset = HEAD_CHUNK_FILE_HEADER_SIZE as u64;
    }

    fn get_next_chunk_ref(&mut self, chk_len: usize) -> (ChunkDiskMapperRef, bool) {
        let bytes_to_write = Self::bytes_to_write_for_chunk(chk_len);
        let mut cut_file = false;
        if self.should_cut_new_file(bytes_to_write) {
            self.advance_to_new_file();
            self.cut_file = false;
            cut_file = true;
        }
        let chk_offset = self.offset;
        self.offset += bytes_to_write;
        (ChunkDiskMapperRef::new(self.seq as u32, chk_offset as u32), cut_file)
    }
}

struct MapperWriteState {
    cur_file: Option<File>,
    cur_file_seq: usize,
    cur_file_offset: usize,
    file_maxt_set: bool,
    write_buf: Vec<u8>,
    write_buffer_size: usize,
    chunk_buffer: HashMap<ChunkDiskMapperRef, ChunkPayload>,
    closed: bool,
}

/// Full Go `chunks.ChunkDiskMapper` supporting buffered writes, optional async `ChunkWriteQueue`,
/// in-memory buffered chunk reads, mmap reads, `IterateAllChunks`, and `Truncate`.
pub struct ChunkDiskMapper {
    dir: PathBuf,
    evtl_pos: Mutex<EvtlPos>,
    read_path_mtx: RwLock<BTreeMap<usize, Mmap>>,
    write_path_mtx: Mutex<MapperWriteState>,
    write_queue: std::sync::OnceLock<Arc<ChunkWriteQueue>>,
}

impl ChunkDiskMapper {
    pub fn open(
        dir: impl AsRef<Path>,
        write_buffer_size: usize,
        write_queue_size: usize,
        queue_metrics: Option<crate::metrics::ChunkWriteQueueMetrics>,
    ) -> Result<Arc<Self>, HeadChunksError> {
        if !write_buffer_size.is_multiple_of(1024) {
            return Err(HeadChunksError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ChunkDiskMapper write buffer size should be a multiple of 1024 (actual: {write_buffer_size})"),
            )));
        }
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut files = list_chunk_files(&dir)?;
        repair_last_chunk_file(&mut files)?;

        let mut mmaps = BTreeMap::new();
        let mut prev_seq: Option<usize> = None;
        for (seq, path) in &files {
            if let Some(prev) = prev_seq {
                if *seq != prev + 1 {
                    return Err(HeadChunksError::NonSequentialFiles {
                        prev,
                        next: *seq,
                    });
                }
            }
            prev_seq = Some(*seq);
            let mmap = HeadChunksReader::map_and_validate_file(path)?;
            mmaps.insert(*seq, mmap);
        }
        let last_seq = prev_seq.unwrap_or(0);

        let cdm = Arc::new(Self {
            dir,
            evtl_pos: Mutex::new(EvtlPos {
                seq: last_seq as u64,
                offset: 0,
                cut_file: last_seq > 0,
            }),
            read_path_mtx: RwLock::new(mmaps),
            write_path_mtx: Mutex::new(MapperWriteState {
                cur_file: None,
                cur_file_seq: last_seq,
                cur_file_offset: 0,
                file_maxt_set: false,
                write_buf: Vec::with_capacity(write_buffer_size),
                write_buffer_size,
                chunk_buffer: HashMap::new(),
                closed: false,
            }),
            write_queue: std::sync::OnceLock::new(),
        });

        if write_queue_size > 0 {
            let weak = Arc::downgrade(&cdm);
            let write_fn: WriteChunkFn = Arc::new(move |series_ref, mint, maxt, chk, ref_id, is_ooo, cut_file| {
                let Some(cdm) = weak.upgrade() else {
                    return Err(HeadChunksError::Closed);
                };
                let payload = chk.expect("chunk payload must be present");
                cdm.write_chunk_internal(series_ref, mint, maxt, &payload, ref_id, is_ooo, cut_file)
            });
            let queue = Arc::new(ChunkWriteQueue::new(queue_metrics, write_queue_size, write_fn));
            let _ = cdm.write_queue.set(queue);
        }
        Ok(cdm)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn write_queue(&self) -> Option<&Arc<ChunkWriteQueue>> {
        self.write_queue.get()
    }

    pub fn is_queue_empty(&self) -> bool {
        self.write_queue.get().is_none_or(|q| q.queue_is_empty())
    }

    pub fn cur_file_sequence(&self) -> usize {
        self.write_path_mtx.lock().cur_file_seq
    }

    pub fn write_buf_buffered(&self) -> usize {
        self.write_path_mtx.lock().write_buf.len()
    }

    pub fn mmapped_file_count(&self) -> usize {
        self.read_path_mtx.read().len()
    }

    pub fn has_mmapped_file(&self, seq: usize) -> bool {
        self.read_path_mtx.read().contains_key(&seq)
    }

    pub fn file_maxt_set(&self) -> bool {
        self.write_path_mtx.lock().file_maxt_set
    }

    pub fn cut_new_file(&self) {
        self.evtl_pos.lock().cut_file = true;
    }

    #[doc(hidden)]
    pub fn cut_empty_file_for_test(&self) -> Result<(), HeadChunksError> {
        let seq = {
            let mut pos = self.evtl_pos.lock();
            pos.advance_to_new_file();
            pos.seq as usize
        };
        let mut st = self.write_path_mtx.lock();
        self.cut_seq_locked(&mut st, seq)
    }

    fn flush_buffer_locked(st: &mut MapperWriteState) -> Result<(), HeadChunksError> {
        if !st.write_buf.is_empty() {
            if let Some(ref mut f) = st.cur_file {
                f.write_all(&st.write_buf)?;
                st.cur_file_offset += st.write_buf.len();
            }
            st.write_buf.clear();
        }
        st.chunk_buffer.clear();
        Ok(())
    }

    fn finalize_cur_file_locked(st: &mut MapperWriteState) -> Result<(), HeadChunksError> {
        if st.cur_file.is_some() {
            Self::flush_buffer_locked(st)?;
            if let Some(ref f) = st.cur_file {
                f.sync_all()?;
            }
            st.cur_file = None;
        }
        Ok(())
    }

    fn cut_seq_locked(&self, st: &mut MapperWriteState, file_seq: usize) -> Result<(), HeadChunksError> {
        Self::finalize_cur_file_locked(st)?;
        let path = head_chunk_file_path(&self.dir, file_seq);
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        #[cfg(unix)]
        {
            let prealloc_len = HEAD_CHUNK_FILE_PREALLOCATION_SIZE as u64;
            f.set_len(prealloc_len)?;
            f.seek(SeekFrom::Start(0))?;
        }
        let mut hdr = [0u8; HEAD_CHUNK_FILE_HEADER_SIZE];
        hdr[..MAGIC_CHUNKS_SIZE].copy_from_slice(&MAGIC_HEAD_CHUNKS.to_be_bytes());
        hdr[MAGIC_CHUNKS_SIZE] = HEAD_CHUNKS_FORMAT_V1;
        f.write_all(&hdr)?;
        f.flush()?;
        let mmap = unsafe { MmapOptions::new().map(&f)? };
        self.read_path_mtx.write().insert(file_seq, mmap);
        st.cur_file = Some(f);
        st.cur_file_seq = file_seq;
        st.cur_file_offset = HEAD_CHUNK_FILE_HEADER_SIZE;
        Ok(())
    }

    pub fn write_chunk<F>(
        &self,
        series_ref: u64,
        mint: i64,
        maxt: i64,
        chunk: EncodedChunk,
        is_ooo: bool,
        callback: F,
    ) -> ChunkDiskMapperRef
    where
        F: FnOnce(Result<(), HeadChunksError>) + Send + 'static,
    {
        self.write_payload(series_ref, mint, maxt, ChunkPayload::Encoded(chunk), is_ooo, callback)
    }

    pub fn write_payload<F>(
        &self,
        series_ref: u64,
        mint: i64,
        maxt: i64,
        payload: ChunkPayload,
        is_ooo: bool,
        callback: F,
    ) -> ChunkDiskMapperRef
    where
        F: FnOnce(Result<(), HeadChunksError>) + Send + 'static,
    {
        let mut pos = self.evtl_pos.lock();
        let (ref_id, cut_file) = pos.get_next_chunk_ref(payload.as_bytes().len());
        if let Some(q) = self.write_queue.get() {
            let job = ChunkWriteJob {
                series_ref,
                mint,
                maxt,
                chk: Some(payload),
                r#ref: ref_id,
                cut_file,
                is_ooo,
                callback: Some(Box::new(callback)),
            };
            if let Err(e) = q.add_job(job) {
                // Queue stopped/closed.
                let _ = e;
            }
            return ref_id;
        }
        let res = self.write_chunk_internal(series_ref, mint, maxt, &payload, ref_id, is_ooo, cut_file);
        drop(pos);
        callback(res);
        ref_id
    }

    #[allow(clippy::too_many_arguments)]
    fn write_chunk_internal(
        &self,
        series_ref: u64,
        mint: i64,
        maxt: i64,
        payload: &ChunkPayload,
        ref_id: ChunkDiskMapperRef,
        is_ooo: bool,
        cut_file: bool,
    ) -> Result<(), HeadChunksError> {
        let mut st = self.write_path_mtx.lock();
        if st.closed {
            return Err(HeadChunksError::Closed);
        }
        if cut_file {
            let (exp_seq, exp_off) = ref_id.unpack();
            self.cut_seq_locked(&mut st, exp_seq as usize)?;
            if st.cur_file_seq != exp_seq as usize || st.cur_file_offset != exp_off as usize {
                return Err(HeadChunksError::Io(io::Error::other(format!(
                    "expected newly cut file {exp_seq}:{exp_off}, got {}:{}",
                    st.cur_file_seq, st.cur_file_offset
                ))));
            }
        }

        let data = payload.as_bytes();
        let mut meta_buf = [0u8; MAX_HEAD_CHUNK_META_SIZE];
        meta_buf[0..8].copy_from_slice(&series_ref.to_be_bytes());
        meta_buf[8..16].copy_from_slice(&mint.to_be_bytes());
        meta_buf[16..24].copy_from_slice(&maxt.to_be_bytes());
        let mut enc_byte = payload.encoding_u8();
        if is_ooo {
            enc_byte |= OUT_OF_ORDER_MASK;
        }
        meta_buf[24] = enc_byte;
        let ulen = put_uvarint(&mut meta_buf[25..], data.len() as u64);
        let hdr_len = 25 + ulen;
        let crc = crc32c::crc32c(&meta_buf[..hdr_len]);
        let crc = crc32c::crc32c_append(crc, data);
        let needed = hdr_len + data.len() + CRC_SIZE;

        if needed < st.write_buffer_size && st.write_buf.len() + needed > st.write_buffer_size {
            Self::flush_buffer_locked(&mut st)?;
        }

        if needed >= st.write_buffer_size {
            Self::flush_buffer_locked(&mut st)?;
            if let Some(ref mut f) = st.cur_file {
                f.write_all(&meta_buf[..hdr_len])?;
                f.write_all(data)?;
                f.write_all(&crc.to_be_bytes())?;
                st.cur_file_offset += needed;
            }
        } else {
            st.write_buf.extend_from_slice(&meta_buf[..hdr_len]);
            st.write_buf.extend_from_slice(data);
            st.write_buf.extend_from_slice(&crc.to_be_bytes());
            st.chunk_buffer.insert(ref_id, payload.clone());
        }
        Ok(())
    }

    pub fn chunk(&self, ref_id: ChunkDiskMapperRef) -> Result<EncodedChunk, HeadChunksError> {
        if let Some(q) = self.write_queue.get() {
            if let Some(ChunkPayload::Encoded(c)) = q.get(ref_id) {
                return Ok(c);
            }
        }
        let (seq_u32, off_u32) = ref_id.unpack();
        let seq = seq_u32 as usize;
        let off = off_u32 as usize;
        {
            let st = self.write_path_mtx.lock();
            if st.closed {
                return Err(HeadChunksError::Closed);
            }
            if seq == st.cur_file_seq {
                if let Some(ChunkPayload::Encoded(c)) = st.chunk_buffer.get(&ref_id) {
                    return Ok(c.clone());
                }
            }
        }
        let mmaps = self.read_path_mtx.read();
        let mmap = mmaps.get(&seq).ok_or_else(|| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: format!("head chunk file index {seq} does not exist on disk"),
        })?;
        let chk_start = off + SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE;
        if chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE > mmap.len() {
            return Err(HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "not enough bytes to read chunk length".to_string(),
            });
        }
        let raw_enc = mmap[chk_start] & !OUT_OF_ORDER_MASK;
        let encoding = Encoding::from_u8(raw_enc).map_err(|_| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: "invalid chunk encoding \"<unknown>\"".to_string(),
        })?;
        let len_start = chk_start + CHUNK_ENCODING_SIZE;
        let (data_len, ulen) = decode_uvarint(&mmap[len_start..(len_start + MAX_CHUNK_LENGTH_FIELD_SIZE).min(mmap.len())])
            .ok_or_else(|| HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "invalid chunk length uvarint".to_string(),
            })?;
        let data_start = len_start + ulen;
        let data_end = data_start + data_len as usize;
        let chk_end = data_end + CRC_SIZE;
        if chk_end > mmap.len() {
            return Err(HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "not enough bytes to read chunk data and CRC".to_string(),
            });
        }
        let expected_crc = u32::from_be_bytes(mmap[data_end..chk_end].try_into().unwrap());
        let actual_crc = crc32c::crc32c(&mmap[off..data_end]);
        if actual_crc != expected_crc {
            return Err(HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: format!("checksum mismatch expected:{expected_crc:#x}, actual:{actual_crc:#x}"),
            });
        }
        EncodedChunk::from_bytes(encoding, mmap[data_start..data_end].to_vec()).map_err(|e| {
            HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: e.to_string(),
            }
        })
    }

    pub fn iterate_all_chunks_raw<F>(&self, mut f: F) -> Result<(), HeadChunksError>
    where
        F: FnMut(HeadChunkRecord, u8) -> Result<(), HeadChunksError>,
    {
        let mut res = Ok(());
        {
            let mmaps = self.read_path_mtx.read();
            for (&seq, mmap) in mmaps.iter() {
                let file_end = mmap.len();
                let mut idx = HEAD_CHUNK_FILE_HEADER_SIZE;
                while idx < file_end {
                    if file_end - idx < MAX_HEAD_CHUNK_META_SIZE {
                        if mmap[idx..file_end].iter().all(|&b| b == 0) {
                            break;
                        }
                        res = Err(HeadChunksError::Corruption {
                            dir: self.dir.clone(),
                            file_index: seq as i64,
                            message: format!("trailing non-zero bytes at offset {idx}"),
                        });
                        break;
                    }
                    let start_idx = idx;
                    let chunk_ref = ChunkDiskMapperRef::new(seq as u32, start_idx as u32);
                    let series_ref = u64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                    idx += SERIES_REF_SIZE;
                    let mint = i64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                    idx += MINT_MAXT_SIZE;
                    let maxt = i64::from_be_bytes(mmap[idx..idx + 8].try_into().unwrap());
                    idx += MINT_MAXT_SIZE;
                    if series_ref == 0 && mint == 0 && maxt == 0 {
                        break;
                    }
                    let raw_enc_byte = mmap[idx];
                    idx += CHUNK_ENCODING_SIZE;
                    let is_ooo = (raw_enc_byte & OUT_OF_ORDER_MASK) != 0;
                    let stripped_enc = raw_enc_byte & !OUT_OF_ORDER_MASK;
                    let encoding = Encoding::from_u8(stripped_enc).unwrap_or(Encoding::None);
                    let Some((data_len, ulen)) = decode_uvarint(&mmap[idx..(idx + MAX_CHUNK_LENGTH_FIELD_SIZE).min(file_end)]) else {
                        res = Err(HeadChunksError::Corruption {
                            dir: self.dir.clone(),
                            file_index: seq as i64,
                            message: "invalid chunk length uvarint".to_string(),
                        });
                        break;
                    };
                    idx += ulen;
                    let data_len = data_len as usize;
                    if data_len < 2 || idx + data_len + CRC_SIZE > file_end {
                        res = Err(HeadChunksError::Corruption {
                            dir: self.dir.clone(),
                            file_index: seq as i64,
                            message: "chunk data extends past end of file".to_string(),
                        });
                        break;
                    }
                    let raw_num_samples = u16::from_be_bytes([mmap[idx], mmap[idx + 1]]);
                    let num_samples = match encoding {
                        Encoding::HistogramSt | Encoding::FloatHistogramSt => raw_num_samples & 0x3FFF,
                        _ => raw_num_samples,
                    };
                    idx += data_len;
                    let expected_crc = u32::from_be_bytes(mmap[idx..idx + CRC_SIZE].try_into().unwrap());
                    let actual_crc = crc32c::crc32c(&mmap[start_idx..idx]);
                    if actual_crc != expected_crc {
                        res = Err(HeadChunksError::Corruption {
                            dir: self.dir.clone(),
                            file_index: seq as i64,
                            message: format!("checksum mismatch expected:{expected_crc:#x}, actual:{actual_crc:#x}"),
                        });
                        break;
                    }
                    idx += CRC_SIZE;
                    if let Err(e) = f(
                        HeadChunkRecord {
                            series_ref,
                            chunk_ref,
                            mint,
                            maxt,
                            num_samples,
                            encoding,
                            is_ooo,
                        },
                        stripped_enc,
                    ) {
                        res = Err(e);
                        break;
                    }
                }
                if res.is_err() {
                    break;
                }
            }
        }
        self.write_path_mtx.lock().file_maxt_set = true;
        res
    }

    pub fn truncate(&self, file_no: usize) -> Result<(), HeadChunksError> {
        let cur_seq = self.write_path_mtx.lock().cur_file_seq;
        let removed: Vec<usize> = {
            let mmaps = self.read_path_mtx.read();
            mmaps
                .keys()
                .copied()
                .take_while(|&seq| seq != cur_seq && seq < file_no)
                .collect()
        };
        let cur_has_chunks = {
            let st = self.write_path_mtx.lock();
            st.cur_file.is_some() && (st.cur_file_offset + st.write_buf.len()) > HEAD_CHUNK_FILE_HEADER_SIZE
        };
        if cur_has_chunks {
            self.cut_new_file();
        }
        {
            let mut mmaps = self.read_path_mtx.write();
            for &seq in &removed {
                mmaps.remove(&seq);
            }
        }
        for seq in removed {
            match fs::remove_file(head_chunk_file_path(&self.dir, seq)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(HeadChunksError::Io(e)),
            }
        }
        Ok(())
    }

    pub fn close(&self) -> Result<(), HeadChunksError> {
        if let Some(q) = self.write_queue.get() {
            q.stop();
        }
        let mut st = self.write_path_mtx.lock();
        if st.closed {
            return Ok(());
        }
        Self::finalize_cur_file_locked(&mut st)?;
        st.closed = true;
        self.read_path_mtx.write().clear();
        Ok(())
    }
}

impl Drop for ChunkDiskMapper {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::XorChunk;

    #[test]
    fn write_and_mmap_read_head_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let mut xc = XorChunk::new();
        {
            let mut app = xc.appender().unwrap();
            app.append(0, 1000, 1.5).unwrap();
            app.append(0, 2000, 2.5).unwrap();
        }
        let encoded = EncodedChunk::Xor(xc.clone());

        let (r1, r2);
        {
            let mut w = HeadChunksWriter::with_max_file_size(dir.path(), 70).unwrap();
            r1 = w.write_chunk(10, 1000, 2000, &encoded, false).unwrap();
            r2 = w.write_chunk(10, 1000, 2000, &encoded, true).unwrap();
            w.close().unwrap();
        }
        assert_eq!(r1.unpack().0, 1);
        assert_eq!(r2.unpack().0, 2);

        let reader = HeadChunksReader::open(dir.path()).unwrap();
        let metas = reader.collect_all_chunks().unwrap();
        assert_eq!(metas.len(), 2);
        assert!(!metas[0].is_ooo);
        assert!(metas[1].is_ooo);
        assert_eq!(metas[0].series_ref, 10);
        assert_eq!(metas[0].num_samples, 2);

        assert_eq!(reader.chunk(r1).unwrap(), encoded);
        assert_eq!(reader.chunk(r2).unwrap(), encoded);
    }

    #[test]
    fn truncate_in_place_except_keeps_active_and_newer_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut xc = XorChunk::new();
        xc.appender().unwrap().append(0, 1000, 1.0).unwrap();
        let encoded = EncodedChunk::Xor(xc);

        {
            let mut w = HeadChunksWriter::with_max_file_size(dir.path(), 70).unwrap();
            for i in 0..4 {
                w.write_chunk(10, 1000 + i * 100, 1000 + i * 100, &encoded, false)
                    .unwrap();
            }
            w.close().unwrap();
        }
        let mut reader = HeadChunksReader::open(dir.path()).unwrap();
        assert_eq!(reader.file_count(), 4);

        reader.truncate_in_place_except(usize::MAX, Some(3)).unwrap();
        assert!(!reader.has_file(1));
        assert!(!reader.has_file(2));
        assert!(reader.has_file(3));
        assert!(reader.has_file(4));
        assert!(head_chunk_file_path(dir.path(), 3).exists());
        assert!(head_chunk_file_path(dir.path(), 4).exists());
    }

    #[test]
    fn iterate_all_chunks_corrupt_huge_uvarint_length_returns_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = head_chunk_file_path(dir.path(), 1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC_HEAD_CHUNKS.to_be_bytes());
        bytes.push(HEAD_CHUNKS_FORMAT_V1);
        bytes.extend_from_slice(&[0u8; SEGMENT_HEADER_PADDING_SIZE]);
        // series_ref = 1, mint = 100, maxt = 200, encoding = Xor(1)
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.extend_from_slice(&100i64.to_be_bytes());
        bytes.extend_from_slice(&200i64.to_be_bytes());
        bytes.push(1);
        // 10-byte uvarint = u64::MAX
        bytes.extend_from_slice(&[0xffu8; 9]);
        bytes.push(0x01);
        bytes.extend_from_slice(&[0u8; 32]);
        fs::write(&path, &bytes).unwrap();

        let reader = HeadChunksReader::open_no_repair(dir.path()).unwrap();
        let err = reader.collect_all_chunks().unwrap_err();
        assert!(matches!(err, HeadChunksError::Corruption { .. }), "got {err:?}");
    }

    #[test]
    fn synchronous_cut_new_file_refresh_and_truncate_without_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let mut xc = XorChunk::new();
        xc.appender().unwrap().append(0, 1000, 1.0).unwrap();
        let encoded = EncodedChunk::Xor(xc);

        let mut w = HeadChunksWriter::open(dir.path()).unwrap();
        let r1 = w.write_chunk(10, 1000, 1000, &encoded, false).unwrap();
        assert_eq!(r1.unpack().0, 1);
        assert!(w.cur_file_has_chunks());

        let mut reader = HeadChunksReader::open_no_repair(dir.path()).unwrap();
        reader.refresh_in_place().unwrap();
        assert!(reader.has_file(1));

        // Synchronously cut a new header-only file (seq 2), refresh reader, and truncate seq 1.
        w.cut_new_file().unwrap();
        assert_eq!(w.current_seq(), 2);
        assert!(!w.cur_file_has_chunks());
        reader.refresh_in_place().unwrap();
        reader
            .truncate_in_place_except(usize::MAX, Some(w.current_seq()))
            .unwrap();

        assert!(!reader.has_file(1));
        assert!(!head_chunk_file_path(dir.path(), 1).exists());
        assert!(reader.has_file(2));
        #[cfg(unix)]
        let expected_cut_len = HEAD_CHUNK_FILE_PREALLOCATION_SIZE as u64;
        #[cfg(not(unix))]
        let expected_cut_len = HEAD_CHUNK_FILE_HEADER_SIZE as u64;
        assert_eq!(
            fs::metadata(head_chunk_file_path(dir.path(), 2)).unwrap().len(),
            expected_cut_len
        );
        assert!(reader.collect_all_chunks().unwrap().is_empty());

        // Subsequent write lands in seq 2 (no extra file cut) and is readable after reopen.
        let r2 = w.write_chunk(10, 2000, 2000, &encoded, false).unwrap();
        assert_eq!(r2.unpack().0, 2);
        w.close().unwrap();

        let reopened = HeadChunksReader::open(dir.path()).unwrap();
        assert_eq!(reopened.collect_all_chunks().unwrap().len(), 1);
        assert_eq!(reopened.chunk(r2).unwrap(), encoded);
    }

    #[test]
    fn rollback_snapshot_branch1_clamps_last_flushed_and_epoch_watermarks_and_reuses_seq() {
        let mut xc = XorChunk::new();
        xc.appender().unwrap().append(0, 1000, 1.0).unwrap();
        let encoded = EncodedChunk::Xor(xc);

        // Subcase A: snap.seq == 0 -> cut_new_file & flush in seq 1..2 -> flush error -> rollback.
        let dir0 = tempfile::tempdir().unwrap();
        let mut wb = HeadChunksWriter::with_options(dir0.path(), 80, 128).unwrap();
        let sb = wb.take_snapshot();
        assert_eq!(sb.seq, 0);
        let rb1 = wb.write_chunk(10, 1000, 1000, &encoded, false).unwrap();
        let rb2 = wb.write_chunk(10, 2000, 2000, &encoded, false).unwrap();
        assert_eq!(rb1.unpack().0, 1);
        assert_eq!(rb2.unpack().0, 2);
        wb.flush().unwrap();
        assert_eq!(wb.flushed_watermark().last_flushed_ref(), rb2);
        let _rb3 = wb.write_chunk(10, 3000, 3000, &encoded, false).unwrap();
        wb.set_write_fault_hook(Some(Arc::new(|len| {
            if len > 8 {
                WriteFaultAction::Error(io::ErrorKind::StorageFull)
            } else {
                WriteFaultAction::PassThrough
            }
        })));
        assert!(wb.flush().is_err());
        wb.set_write_fault_hook(None);

        wb.rollback_snapshot(sb);
        assert_eq!(wb.current_seq(), 0);
        assert!(!head_chunk_file_path(dir0.path(), 1).exists());
        assert!(!head_chunk_file_path(dir0.path(), 2).exists());
        assert!(!head_chunk_file_path(dir0.path(), 3).exists());
        let wm0 = wb.flushed_watermark();
        assert_eq!(wm0.last_flushed_ref(), ChunkDiskMapperRef(0));
        assert!(!wm0.is_flushed(rb1, Some(0)));
        assert!(!wm0.is_flushed(rb2, Some(0)));
        assert!(!wm0.is_flushed(rb1, Some(1)));

        let r_reused = wb.write_chunk(10, 4000, 4000, &encoded, false).unwrap();
        assert_eq!(r_reused.unpack().0, 1);
        wb.close().unwrap();
        let reader0 = HeadChunksReader::open(dir0.path()).unwrap();
        let all0 = reader0.collect_all_chunks().unwrap();
        assert_eq!(all0.len(), 1);
        assert_eq!(all0[0].mint, 4000);

        // Subcase B: snap.seq == 1 with snap.write_buf_len > 0 -> cut seq 2 -> flush -> rollback.
        let dir1 = tempfile::tempdir().unwrap();
        let mut w1 = HeadChunksWriter::with_options(dir1.path(), 80, 4096).unwrap();
        let c1 = w1.write_chunk(10, 1000, 1000, &encoded, false).unwrap();
        assert_eq!(c1.unpack().0, 1);
        let snap1 = w1.take_snapshot();
        assert_eq!(snap1.seq, 1);
        assert!(snap1.write_buf_len > 0);

        let c2 = w1.write_chunk(10, 2000, 2000, &encoded, false).unwrap();
        assert_eq!(c2.unpack().0, 2);
        w1.flush().unwrap();
        assert_eq!(w1.flushed_watermark().last_flushed_ref(), c2);

        let _c3 = w1.write_chunk(10, 3000, 3000, &encoded, false).unwrap();
        w1.set_write_fault_hook(Some(Arc::new(|len| {
            if len > 8 {
                WriteFaultAction::Error(io::ErrorKind::StorageFull)
            } else {
                WriteFaultAction::PassThrough
            }
        })));
        assert!(w1.flush().is_err());
        w1.set_write_fault_hook(None);

        w1.rollback_snapshot(snap1);
        assert_eq!(w1.current_seq(), 1);
        assert!(head_chunk_file_path(dir1.path(), 1).exists());
        assert!(!head_chunk_file_path(dir1.path(), 2).exists());
        assert!(!head_chunk_file_path(dir1.path(), 3).exists());

        let wm1 = w1.flushed_watermark();
        assert_eq!(wm1.last_flushed_ref(), c1);
        assert!(wm1.is_flushed(c1, Some(0)));
        assert!(!wm1.is_flushed(c2, Some(0)));
        assert!(!wm1.is_flushed(c2, Some(1)));

        let c_next = w1.write_chunk(10, 5000, 5000, &encoded, false).unwrap();
        assert_eq!(c_next.unpack().0, 2);
        w1.close().unwrap();

        let reader1 = HeadChunksReader::open(dir1.path()).unwrap();
        let all1 = reader1.collect_all_chunks().unwrap();
        assert_eq!(all1.len(), 2);
        assert_eq!(reader1.chunk(c1).unwrap(), encoded);
        assert_eq!(reader1.chunk(c_next).unwrap(), encoded);
    }

    #[test]
    fn rollback_snapshot_branch3_with_buffered_snap_rolls_back_truncated_flushed_refs() {
        let dir = tempfile::tempdir().unwrap();
        let mut xc = XorChunk::new();
        xc.appender().unwrap().append(0, 1000, 1.0).unwrap();
        let encoded = EncodedChunk::Xor(xc);

        let mut w = HeadChunksWriter::with_options(dir.path(), 4096, 64).unwrap();
        let c1 = w.write_chunk(10, 1000, 1000, &encoded, false).unwrap();
        let snap = w.take_snapshot();
        assert_eq!(snap.seq, 1);
        assert!(snap.write_buf_len > 0);
        assert_eq!(snap.last_flushed_ref, ChunkDiskMapperRef(0));
        assert_eq!(snap.last_written_ref, c1);

        let c2 = w.write_chunk(10, 2000, 2000, &encoded, false).unwrap();
        w.flush().unwrap();
        assert_eq!(w.flushed_watermark().last_flushed_ref(), c2);

        let _c3 = w.write_chunk(10, 3000, 3000, &encoded, false).unwrap();
        w.set_write_fault_hook(Some(Arc::new(|len| {
            if len > 8 {
                WriteFaultAction::Error(io::ErrorKind::StorageFull)
            } else {
                WriteFaultAction::PassThrough
            }
        })));
        assert!(w.flush().is_err());
        w.set_write_fault_hook(None);

        w.rollback_snapshot(snap);
        let wm = w.flushed_watermark();
        assert_eq!(wm.last_flushed_ref(), c1);
        assert!(wm.is_flushed(c1, Some(0)));
        assert!(!wm.is_flushed(c2, Some(0)));
        assert!(!wm.is_flushed(c2, Some(1)));

        let c_after = w.write_chunk(10, 4000, 4000, &encoded, false).unwrap();
        assert_eq!(c_after.unpack(), (1, snap.offset as u32));
        w.close().unwrap();

        let reader = HeadChunksReader::open(dir.path()).unwrap();
        let all = reader.collect_all_chunks().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].mint, 1000);
        assert_eq!(all[1].mint, 4000);
        assert_eq!(reader.chunk(c1).unwrap(), encoded);
        assert_eq!(reader.chunk(c_after).unwrap(), encoded);
    }
}
