//! Bit-exact Prometheus block chunks segment writer & memory-mapped reader (`chunks/000001`).
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunks/chunks.go.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use memmap2::{Mmap, MmapOptions};
use thiserror::Error;

use crate::chunkenc::{ChunkError, EncodedChunk, Encoding};
use crate::encoding::{decode_uvarint, put_uvarint};

pub const MAGIC_CHUNKS: u32 = 0x85BD_40DD;
pub const MAGIC_CHUNKS_SIZE: usize = 4;
pub const CHUNKS_FORMAT_V1: u8 = 1;
pub const CHUNKS_FORMAT_VERSION_SIZE: usize = 1;
pub const SEGMENT_HEADER_PADDING_SIZE: usize = 3;
pub const SEGMENT_HEADER_SIZE: usize =
    MAGIC_CHUNKS_SIZE + CHUNKS_FORMAT_VERSION_SIZE + SEGMENT_HEADER_PADDING_SIZE;

pub const DEFAULT_CHUNK_SEGMENT_SIZE: usize = 512 * 1024 * 1024;
pub const MAX_CHUNK_LENGTH_FIELD_SIZE: usize = 5;
pub const CHUNK_ENCODING_SIZE: usize = 1;
pub const CRC_SIZE: usize = 4;
const DEFAULT_BUFFER_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum BlockChunksError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid block chunk segment {index}: {message}")]
    Corruption { index: usize, message: String },
    #[error("segment index {index} out of range (count {count})")]
    SegmentOutOfRange { index: usize, count: usize },
    #[error("chunk decode error: {0}")]
    Chunk(#[from] ChunkError),
    #[error("block chunks writer closed")]
    Closed,
}

/// Packed 64-bit reference to a chunk inside a persisted block:
/// `(segment_index_0_based << 32) | byte_offset`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BlockChunkRef(pub u64);

impl BlockChunkRef {
    #[inline]
    pub const fn new(file_index: u32, file_offset: u32) -> Self {
        Self(((file_index as u64) << 32) | (file_offset as u64))
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

/// Chunk metadata used across Head, Block, and Compactor (`chunks.Meta`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkMeta {
    pub ref_id: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub chunk: Option<EncodedChunk>,
}

impl ChunkMeta {
    #[inline]
    pub const fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time <= maxt && mint <= self.max_time
    }
}

#[inline]
pub fn block_chunk_file_path(dir: &Path, seq: usize) -> PathBuf {
    dir.join(format!("{seq:06}"))
}

fn list_segment_files(dir: &Path) -> Result<Vec<(usize, PathBuf)>, BlockChunksError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(name_str) = fname.to_str() else {
            continue;
        };
        if let Ok(seq) = name_str.parse::<usize>() {
            files.push((seq, entry.path()));
        }
    }
    files.sort_by_key(|(seq, _)| *seq);
    Ok(files)
}

/// Writer for Prometheus block `chunks/000001`, `000002`, ... segment files.
pub struct BlockChunksWriter {
    dir: PathBuf,
    segment_size: usize,
    seq: usize,
    pos: usize,
    cur_writer: Option<BufWriter<File>>,
    closed: bool,
}

impl BlockChunksWriter {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, BlockChunksError> {
        Self::with_segment_size(dir, DEFAULT_CHUNK_SEGMENT_SIZE)
    }

    pub fn with_segment_size(
        dir: impl AsRef<Path>,
        segment_size: usize,
    ) -> Result<Self, BlockChunksError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let segment_size = if segment_size == 0 {
            DEFAULT_CHUNK_SEGMENT_SIZE
        } else {
            segment_size
        };
        let existing = list_segment_files(&dir)?;
        let seq = existing.last().map_or(0, |(s, _)| *s);
        Ok(Self {
            dir,
            segment_size,
            seq,
            pos: 0,
            cur_writer: None,
            closed: false,
        })
    }

    fn finalize_tail(&mut self) -> Result<(), BlockChunksError> {
        if let Some(mut w) = self.cur_writer.take() {
            w.flush()?;
            let file = w.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
        }
        Ok(())
    }

    fn cut(&mut self) -> Result<(), BlockChunksError> {
        self.finalize_tail()?;
        self.seq += 1;
        let path = block_chunk_file_path(&self.dir, self.seq);
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)?;

        let mut hdr = [0u8; SEGMENT_HEADER_SIZE];
        hdr[..MAGIC_CHUNKS_SIZE].copy_from_slice(&MAGIC_CHUNKS.to_be_bytes());
        hdr[MAGIC_CHUNKS_SIZE] = CHUNKS_FORMAT_V1;
        f.write_all(&hdr)?;

        self.cur_writer = Some(BufWriter::with_capacity(DEFAULT_BUFFER_SIZE, f));
        self.pos = SEGMENT_HEADER_SIZE;
        Ok(())
    }

    /// Writes `chunks` into the block chunk segments, updating each [`ChunkMeta::ref_id`]
    /// to its [`BlockChunkRef`].
    pub fn write_chunks(&mut self, chunks: &mut [ChunkMeta]) -> Result<(), BlockChunksError> {
        if self.closed {
            return Err(BlockChunksError::Closed);
        }
        if chunks.is_empty() {
            return Ok(());
        }

        if self.cur_writer.is_none() {
            self.cut()?;
        }

        let mut len_buf = [0u8; 10];
        for chk in chunks.iter_mut() {
            let encoded = chk
                .chunk
                .as_ref()
                .expect("ChunkMeta.chunk must be populated before write_chunks");
            let data = encoded.as_bytes();
            let max_chk_size = MAX_CHUNK_LENGTH_FIELD_SIZE + CHUNK_ENCODING_SIZE + data.len() + CRC_SIZE;

            if self.pos > SEGMENT_HEADER_SIZE && self.pos + max_chk_size > self.segment_size {
                self.cut()?;
            }

            // BlockChunkRef uses 0-based segment index (`self.seq - 1`).
            let seg_idx = (self.seq - 1) as u32;
            chk.ref_id = BlockChunkRef::new(seg_idx, self.pos as u32).as_u64();

            let ulen = put_uvarint(&mut len_buf, data.len() as u64);
            let enc_byte = [encoded.encoding().as_u8()];
            let crc = crc32c::crc32c(&enc_byte);
            let crc = crc32c::crc32c_append(crc, data);

            let w = self.cur_writer.as_mut().unwrap();
            w.write_all(&len_buf[..ulen])?;
            w.write_all(&enc_byte)?;
            w.write_all(data)?;
            w.write_all(&crc.to_be_bytes())?;

            self.pos += ulen + CHUNK_ENCODING_SIZE + data.len() + CRC_SIZE;
        }
        Ok(())
    }

    pub fn close(mut self) -> Result<(), BlockChunksError> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), BlockChunksError> {
        if self.closed {
            return Ok(());
        }
        self.finalize_tail()?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for BlockChunksWriter {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

/// Memory-mapped reader for Prometheus block `chunks/000001`, ... segment files.
#[derive(Debug)]
pub struct BlockChunksReader {
    segments: Vec<Mmap>,
    total_size: usize,
}

impl BlockChunksReader {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, BlockChunksError> {
        let files = list_segment_files(dir.as_ref())?;
        let mut segments = Vec::with_capacity(files.len());
        let mut total_size = 0usize;

        for (i, (_, path)) in files.into_iter().enumerate() {
            let f = File::open(&path)?;
            // SAFETY: persisted block chunk segments are immutable once written.
            let mmap = unsafe { MmapOptions::new().map(&f)? };
            if mmap.len() < SEGMENT_HEADER_SIZE {
                return Err(BlockChunksError::Corruption {
                    index: i,
                    message: "segment header too short".to_string(),
                });
            }
            let magic = u32::from_be_bytes([mmap[0], mmap[1], mmap[2], mmap[3]]);
            if magic != MAGIC_CHUNKS {
                return Err(BlockChunksError::Corruption {
                    index: i,
                    message: format!("invalid magic number {magic:#x}"),
                });
            }
            let version = mmap[MAGIC_CHUNKS_SIZE];
            if version != CHUNKS_FORMAT_V1 {
                return Err(BlockChunksError::Corruption {
                    index: i,
                    message: format!("invalid chunk format version {version}"),
                });
            }
            total_size += mmap.len();
            segments.push(mmap);
        }

        Ok(Self {
            segments,
            total_size,
        })
    }

    #[inline]
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// Reads and CRC-verifies the chunk referenced by `chunk_ref` (`BlockChunkRef`).
    pub fn chunk(&self, chunk_ref: u64) -> Result<EncodedChunk, BlockChunksError> {
        let (seg_idx_u32, offset_u32) = BlockChunkRef(chunk_ref).unpack();
        let seg_idx = seg_idx_u32 as usize;
        let chk_start = offset_u32 as usize;

        let mmap = self.segments.get(seg_idx).ok_or(BlockChunksError::SegmentOutOfRange {
            index: seg_idx,
            count: self.segments.len(),
        })?;

        if chk_start >= mmap.len() {
            return Err(BlockChunksError::Corruption {
                index: seg_idx,
                message: format!("chunk offset {chk_start} past segment end {}", mmap.len()),
            });
        }

        let len_end = (chk_start + MAX_CHUNK_LENGTH_FIELD_SIZE).min(mmap.len());
        let (data_len, ulen) = decode_uvarint(&mmap[chk_start..len_end]).ok_or_else(|| {
            BlockChunksError::Corruption {
                index: seg_idx,
                message: "invalid chunk length uvarint".to_string(),
            }
        })?;
        let data_len = usize::try_from(data_len).map_err(|_| BlockChunksError::Corruption {
            index: seg_idx,
            message: "chunk data length overflow".to_string(),
        })?;

        let enc_start = chk_start + ulen;
        let data_start = enc_start + CHUNK_ENCODING_SIZE;
        let data_end = data_start.checked_add(data_len).ok_or_else(|| {
            BlockChunksError::Corruption {
                index: seg_idx,
                message: "chunk data length overflow".to_string(),
            }
        })?;
        let chk_end = data_end.checked_add(CRC_SIZE).ok_or_else(|| {
            BlockChunksError::Corruption {
                index: seg_idx,
                message: "chunk data length overflow".to_string(),
            }
        })?;
        if chk_end > mmap.len() {
            return Err(BlockChunksError::Corruption {
                index: seg_idx,
                message: format!(
                    "chunk extends past segment end (need {chk_end}, have {})",
                    mmap.len()
                ),
            });
        }

        let expected_crc =
            u32::from_be_bytes(mmap[data_end..chk_end].try_into().unwrap());
        let actual_crc = crc32c::crc32c(&mmap[enc_start..data_end]);
        if actual_crc != expected_crc {
            return Err(BlockChunksError::Corruption {
                index: seg_idx,
                message: format!(
                    "checksum mismatch expected:{expected_crc:#x}, actual:{actual_crc:#x}"
                ),
            });
        }

        let encoding = Encoding::from_u8(mmap[enc_start])?;
        EncodedChunk::from_bytes(encoding, mmap[data_start..data_end].to_vec())
            .map_err(BlockChunksError::Chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkenc::XorChunk;

    #[test]
    fn write_and_read_block_chunks_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut c1 = XorChunk::new();
        {
            let mut app = c1.appender().unwrap();
            app.append(0, 1000, 1.0).unwrap();
            app.append(0, 2000, 2.0).unwrap();
        }
        c1.compact();
        let mut c2 = XorChunk::new();
        {
            let mut app = c2.appender().unwrap();
            app.append(0, 3000, 3.0).unwrap();
        }
        c2.compact();

        let mut metas = vec![
            ChunkMeta {
                ref_id: 0,
                min_time: 1000,
                max_time: 2000,
                chunk: Some(EncodedChunk::Xor(c1.clone())),
            },
            ChunkMeta {
                ref_id: 0,
                min_time: 3000,
                max_time: 3000,
                chunk: Some(EncodedChunk::Xor(c2.clone())),
            },
        ];

        {
            let mut w = BlockChunksWriter::open(dir.path()).unwrap();
            w.write_chunks(&mut metas).unwrap();
            w.close().unwrap();
        }

        assert!(block_chunk_file_path(dir.path(), 1).exists());
        let reader = BlockChunksReader::open(dir.path()).unwrap();
        assert_eq!(
            reader.chunk(metas[0].ref_id).unwrap(),
            EncodedChunk::Xor(c1)
        );
        assert_eq!(
            reader.chunk(metas[1].ref_id).unwrap(),
            EncodedChunk::Xor(c2)
        );
    }
}
