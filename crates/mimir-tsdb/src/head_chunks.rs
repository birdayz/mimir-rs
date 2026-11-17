//! Bit-exact Prometheus `chunks_head` (`ChunkDiskMapper`) mmap-backed head chunk files.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunks/head_chunks.go
//! and github.com/prometheus/prometheus/tsdb/chunks/chunks.go.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

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

/// Writer for Prometheus `chunks_head/000001`, `000002`, ... files.
pub struct HeadChunksWriter {
    dir: PathBuf,
    max_file_size: usize,
    write_buffer_size: usize,
    queue_metrics: Option<crate::metrics::ChunkWriteQueueMetrics>,
    seq: usize,
    offset: usize,
    cur_writer: Option<BufWriter<File>>,
    cut_on_next_chunk: bool,
    closed: bool,
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
            cur_writer: None,
            cut_on_next_chunk: false,
            closed: false,
        })
    }

    fn entry_len(data_len: usize) -> usize {
        let mut len_buf = [0u8; 10];
        let ulen = put_uvarint(&mut len_buf, data_len as u64);
        SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE + CHUNK_ENCODING_SIZE + ulen + data_len + CRC_SIZE
    }

    fn cut_new_file(&mut self) -> Result<(), HeadChunksError> {
        self.finalize_current()?;
        self.seq += 1;
        let path = head_chunk_file_path(&self.dir, self.seq);
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;

        let mut hdr = [0u8; HEAD_CHUNK_FILE_HEADER_SIZE];
        hdr[..MAGIC_CHUNKS_SIZE].copy_from_slice(&MAGIC_HEAD_CHUNKS.to_be_bytes());
        hdr[MAGIC_CHUNKS_SIZE] = HEAD_CHUNKS_FORMAT_V1;
        f.write_all(&hdr)?;
        f.flush()?;

        self.cur_writer = Some(BufWriter::with_capacity(self.write_buffer_size, f));
        self.offset = HEAD_CHUNK_FILE_HEADER_SIZE;
        Ok(())
    }

    fn finalize_current(&mut self) -> Result<(), HeadChunksError> {
        if let Some(mut w) = self.cur_writer.take() {
            w.flush()?;
            let file = w.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
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
        let needed = Self::entry_len(data.len());

        if self.cut_on_next_chunk
            || self.cur_writer.is_none()
            || self.offset + needed > self.max_file_size
        {
            self.cut_on_next_chunk = false;
            self.cut_new_file()?;
        }

        let chunk_offset = self.offset;
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

        let w = self.cur_writer.as_mut().unwrap();
        w.write_all(&meta_buf[..hdr_len])?;
        w.write_all(data)?;
        w.write_all(&crc.to_be_bytes())?;

        self.offset += needed;
        if let Some(m) = self.queue_metrics.as_ref() {
            m.adds.inc();
            m.completed.inc();
        }
        Ok(ChunkDiskMapperRef::new(self.seq as u32, chunk_offset as u32))
    }

    /// Attaches the Go `chunk_write_queue` operation counters.
    pub fn set_queue_metrics(&mut self, m: crate::metrics::ChunkWriteQueueMetrics) {
        self.queue_metrics = Some(m);
    }

    pub fn flush(&mut self) -> Result<(), HeadChunksError> {
        if let Some(w) = self.cur_writer.as_mut() {
            w.flush()?;
        }
        Ok(())
    }

    pub fn has_active_file(&self) -> bool {
        self.cur_writer.is_some()
    }

    pub fn cur_file_has_chunks(&self) -> bool {
        self.cur_writer.is_some() && self.offset > HEAD_CHUNK_FILE_HEADER_SIZE
    }

    pub fn cut_file_on_next_chunk(&mut self) {
        self.cut_on_next_chunk = true;
    }

    pub fn current_seq(&self) -> usize {
        self.seq
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

            let file = File::open(&path)?;
            // SAFETY: files are opened read-only for mmap inspection of persisted head chunks.
            let mmap = unsafe { MmapOptions::new().map(&file)? };

            // Header validation errors are NOT `CorruptionErr` in Go
            // (`openMMapFiles` returns plain `fmt.Errorf`), so they are fatal for
            // `NewHead` and never trigger the m-map chunk repair path.
            if mmap.len() < HEAD_CHUNK_FILE_HEADER_SIZE {
                return Err(HeadChunksError::InvalidFile {
                    path: path.clone(),
                    message: "invalid head chunk file header: invalid size".to_string(),
                });
            }
            let magic = u32::from_be_bytes([mmap[0], mmap[1], mmap[2], mmap[3]]);
            if magic != MAGIC_HEAD_CHUNKS {
                return Err(HeadChunksError::InvalidFile {
                    path: path.clone(),
                    message: format!("invalid magic number {magic:x}"),
                });
            }
            let version = mmap[MAGIC_CHUNKS_SIZE];
            if version != HEAD_CHUNKS_FORMAT_V1 {
                return Err(HeadChunksError::InvalidFile {
                    path: path.clone(),
                    message: format!("invalid chunk format version {version}"),
                });
            }

            files.insert(seq, mmap);
        }

        Ok(Self { dir, files })
    }

    #[inline]
    pub fn file_count(&self) -> usize {
        self.files.len()
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

        let chk_start = start_of_entry + SERIES_REF_SIZE + 2 * MINT_MAXT_SIZE;
        if chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE > mmap.len() {
            return Err(HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "not enough bytes to read chunk length".to_string(),
            });
        }

        let raw_enc = mmap[chk_start] & !OUT_OF_ORDER_MASK;
        let encoding = Encoding::from_u8(raw_enc)?;

        let len_start = chk_start + CHUNK_ENCODING_SIZE;
        let len_slice = &mmap[len_start..(len_start + MAX_CHUNK_LENGTH_FIELD_SIZE).min(mmap.len())];
        let (data_len, ulen) =
            decode_uvarint(len_slice).ok_or_else(|| HeadChunksError::Corruption {
                dir: self.dir.clone(),
                file_index: seq as i64,
                message: "invalid chunk data length uvarint".to_string(),
            })?;
        let data_len = data_len as usize;

        let data_start = len_start + ulen;
        let data_end = data_start.checked_add(data_len).ok_or_else(|| HeadChunksError::Corruption {
            dir: self.dir.clone(),
            file_index: seq as i64,
            message: "chunk data length overflow".to_string(),
        })?;
        if data_end + CRC_SIZE > mmap.len() {
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

        EncodedChunk::from_bytes(encoding, mmap[data_start..data_end].to_vec())
            .map_err(HeadChunksError::Chunk)
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

                let data_len = data_len as usize;
                if data_len < 2 || idx + data_len + CRC_SIZE > file_end {
                    return Err(HeadChunksError::Corruption {
                        dir: self.dir.clone(),
                        file_index: seq as i64,
                        message: "chunk data extends past end of head chunk file".to_string(),
                    });
                }

                let num_samples = u16::from_be_bytes([mmap[idx], mmap[idx + 1]]);
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

    /// Deletes all head chunk files with sequence `< before_seq`.
    pub fn truncate(self, before_seq: usize) -> Result<Self, HeadChunksError> {
        self.truncate_except(before_seq, None)
    }

    /// Deletes all head chunk files with sequence `< before_seq` except `active_seq`
    /// (`ChunkDiskMapper.Truncate` in Go `head_chunks.go`).
    pub fn truncate_except(
        mut self,
        before_seq: usize,
        active_seq: Option<usize>,
    ) -> Result<Self, HeadChunksError> {
        let to_remove: Vec<usize> = self
            .files
            .keys()
            .copied()
            .take_while(|&seq| seq < before_seq)
            .filter(|&seq| Some(seq) != active_seq)
            .collect();
        for seq in to_remove {
            self.files.remove(&seq);
            let p = head_chunk_file_path(&self.dir, seq);
            match fs::remove_file(p) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(HeadChunksError::Io(e)),
            }
        }
        Ok(self)
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
}
