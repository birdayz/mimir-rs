//! Ported from:
//! - github.com/grafana/mimir/pkg/util/grpcencoding/s2/s2.go
//! - github.com/klauspost/compress/s2/*

use std::fmt;
use std::io;

pub mod block;
pub mod dict;
pub mod index;
pub mod lz4;
pub mod stream;

pub use block::{
    concat_blocks, decode, decode_into_slice, decoded_len, emit_copy, emit_copy_no_repeat,
    emit_literal, emit_repeat, encode, encode_best, encode_better, encode_block,
    encode_block_best, encode_block_better, encode_block_better_snappy, encode_block_snappy,
    encode_snappy, encode_snappy_best, encode_snappy_better, estimate_block_size, match_len,
    max_encoded_len, s2_decode, s2_decode_dict,
};
pub use dict::{Dict, MAX_DICT_SIZE, MIN_DICT_SIZE};
pub use index::{
    index_stream, remove_index_headers, restore_index_headers, Index, IndexEntry,
    S2_INDEX_HEADER, S2_INDEX_TRAILER,
};
pub use lz4::{LZ4Converter, LZ4sConverter};
pub use stream::{ReadSeeker, Reader, ReaderOptions, Writer, WriterLevel, WriterOptions};

/// Name registered for the S2 gRPC compressor (`grpc-encoding: s2`).
pub const S2_ENCODING_NAME: &str = "s2";

pub const TAG_LITERAL: u8 = 0x00;
pub const TAG_COPY1: u8 = 0x01;
pub const TAG_COPY2: u8 = 0x02;
pub const TAG_COPY4: u8 = 0x03;

pub const CHECKSUM_SIZE: usize = 4;
pub const CHUNK_HEADER_SIZE: usize = 4;
pub const OBUF_HEADER_LEN: usize = CHECKSUM_SIZE + CHUNK_HEADER_SIZE;
pub const SKIPPABLE_FRAME_HEADER: usize = 4;

pub const CHUNK_TYPE_COMPRESSED: u8 = 0x00;
pub const CHUNK_TYPE_UNCOMPRESSED: u8 = 0x01;
pub const CHUNK_TYPE_INDEX: u8 = 0x99;
pub const CHUNK_TYPE_PADDING: u8 = 0xfe;
pub const CHUNK_TYPE_STREAM_IDENTIFIER: u8 = 0xff;

pub const MAGIC_SNAPPY: &[u8; 6] = b"sNaPpY";
pub const MAGIC_S2: &[u8; 6] = b"S2sTwO";
pub const MAGIC_STREAM_S2: &[u8; 10] = b"\xff\x06\x00\x00S2sTwO";
pub const MAGIC_STREAM_SNAPPY: &[u8; 10] = b"\xff\x06\x00\x00sNaPpY";

pub const MAX_BLOCK_SIZE: usize = 4 << 20;
pub const MIN_BLOCK_SIZE: usize = 4 << 10;
pub const DEFAULT_BLOCK_SIZE: usize = 1 << 20;
pub const MAX_SNAPPY_BLOCK_SIZE: usize = 1 << 16;
pub const MAX_CHUNK_SIZE: usize = (1 << 24) - 1;
pub const MAX_DICT_SRC_OFFSET: usize = 65535;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S2Error {
    Corrupt,
    CRC,
    TooLarge,
    Unsupported,
    DstTooSmall,
    Io(io::ErrorKind),
}

impl fmt::Display for S2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corrupt => f.write_str("s2: corrupt input"),
            Self::CRC => f.write_str("s2: corrupt input, crc mismatch"),
            Self::TooLarge => f.write_str("s2: decoded block is too large"),
            Self::Unsupported => f.write_str("s2: unsupported input"),
            Self::DstTooSmall => f.write_str("s2: destination too small"),
            Self::Io(k) => write!(f, "s2: io error: {k:?}"),
        }
    }
}

impl std::error::Error for S2Error {}

#[inline]
pub fn masked_crc32c(data: &[u8]) -> u32 {
    let c = crc32c::crc32c(data);
    c.rotate_right(15).wrapping_add(0xa282_ead8)
}

/// Compresses `input` into S2 stream-framed bytes (`\xff\x06\x00\x00S2sTwO` header + chunks).
pub fn compress_framed(input: &[u8]) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() / 2 + 32);
    compress_framed_into(input, &mut out)?;
    Ok(out)
}

/// Compresses `input` into S2 stream-framed bytes, appending into `out`.
pub fn compress_framed_into(input: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
    out.extend_from_slice(MAGIC_STREAM_S2);
    if input.is_empty() {
        return Ok(());
    }
    let mut enc = snap::raw::Encoder::new();
    let mut scratch = Vec::new();
    for chunk in input.chunks(MAX_SNAPPY_BLOCK_SIZE) {
        let crc = masked_crc32c(chunk);
        let max_comp = snap::raw::max_compress_len(chunk.len());
        if scratch.len() < max_comp {
            scratch.resize(max_comp, 0);
        }
        let comp_len = enc
            .compress(chunk, &mut scratch)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if comp_len < chunk.len() - (chunk.len() / 8) {
            let payload_len = 4 + comp_len;
            out.push(CHUNK_TYPE_COMPRESSED);
            out.push((payload_len & 0xff) as u8);
            out.push(((payload_len >> 8) & 0xff) as u8);
            out.push(((payload_len >> 16) & 0xff) as u8);
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(&scratch[..comp_len]);
        } else {
            let payload_len = 4 + chunk.len();
            out.push(CHUNK_TYPE_UNCOMPRESSED);
            out.push((payload_len & 0xff) as u8);
            out.push(((payload_len >> 8) & 0xff) as u8);
            out.push(((payload_len >> 16) & 0xff) as u8);
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(chunk);
        }
    }
    Ok(())
}

/// S2 gRPC compressor matching `pkg/util/grpcencoding/s2/s2.go`.
#[derive(Debug, Default, Clone, Copy)]
pub struct S2Compressor;

impl S2Compressor {
    pub fn new() -> Self {
        Self
    }

    pub fn name(&self) -> &'static str {
        S2_ENCODING_NAME
    }

    pub fn compress(&self, input: &[u8]) -> io::Result<Vec<u8>> {
        compress_framed(input)
    }

    pub fn decompress(&self, input: &[u8], max_out_len: usize) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        decompress_framed_into(input, &mut out, max_out_len)?;
        Ok(out)
    }
}

/// Decompresses S2 (or Snappy) stream-framed bytes into `out`, enforcing `max_out_len` on the
/// number of decompressed bytes appended.
pub fn decompress_framed_into(
    input: &[u8],
    out: &mut Vec<u8>,
    max_out_len: usize,
) -> io::Result<()> {
    let start_len = out.len();
    let res = decompress_framed_into_inner(input, out, start_len, max_out_len);
    if res.is_err() {
        out.truncate(start_len);
    }
    res
}

fn decompress_framed_into_inner(
    input: &[u8],
    out: &mut Vec<u8>,
    start_len: usize,
    max_out_len: usize,
) -> io::Result<()> {
    let mut pos = 0usize;
    let mut read_header = false;
    let mut snappy_frame = false;

    while pos < input.len() {
        let hdr = input.get(pos..pos + 4).ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "s2: truncated chunk header")
        })?;
        pos += 4;
        let chunk_type = hdr[0];
        if !read_header {
            if chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "s2: missing stream identifier",
                ));
            }
            read_header = true;
        }
        let chunk_len = (hdr[1] as usize) | ((hdr[2] as usize) << 8) | ((hdr[3] as usize) << 16);
        let chunk = input.get(pos..pos + chunk_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "s2: truncated chunk payload")
        })?;
        pos += chunk_len;

        match chunk_type {
            CHUNK_TYPE_STREAM_IDENTIFIER => {
                if chunk.len() != 6 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: invalid stream identifier length",
                    ));
                }
                if chunk == MAGIC_S2 {
                    snappy_frame = false;
                } else if chunk == MAGIC_SNAPPY {
                    snappy_frame = true;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: invalid stream identifier magic",
                    ));
                }
            }
            CHUNK_TYPE_COMPRESSED => {
                if chunk.len() < 4 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: compressed chunk shorter than checksum",
                    ));
                }
                let checksum = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                let block = &chunk[4..];
                let (dec_len, var_len) = block::decode_block_uvarint(block)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let max_block = if snappy_frame {
                    MAX_SNAPPY_BLOCK_SIZE
                } else {
                    MAX_BLOCK_SIZE
                };
                if dec_len > max_block {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: decoded block exceeds max block size",
                    ));
                }
                let produced = out.len() - start_len;
                if produced.saturating_add(dec_len) > max_out_len {
                    out.truncate(start_len);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        crate::snappy::DecompressLimitExceeded { limit: max_out_len },
                    ));
                }
                let dst_start = out.len();
                out.resize(dst_start + dec_len, 0);
                s2_decode(&mut out[dst_start..], &block[var_len..])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if masked_crc32c(&out[dst_start..]) != checksum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: CRC mismatch",
                    ));
                }
            }
            CHUNK_TYPE_UNCOMPRESSED => {
                if chunk.len() < 4 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: uncompressed chunk shorter than checksum",
                    ));
                }
                let checksum = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                let payload = &chunk[4..];
                let max_block = if snappy_frame {
                    MAX_SNAPPY_BLOCK_SIZE
                } else {
                    MAX_BLOCK_SIZE
                };
                if payload.len() > max_block {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: uncompressed block exceeds max block size",
                    ));
                }
                let produced = out.len() - start_len;
                if produced.saturating_add(payload.len()) > max_out_len {
                    out.truncate(start_len);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        crate::snappy::DecompressLimitExceeded { limit: max_out_len },
                    ));
                }
                if masked_crc32c(payload) != checksum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: CRC mismatch",
                    ));
                }
                out.extend_from_slice(payload);
            }
            0x02..=0x7f => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("s2: unsupported unskippable chunk 0x{chunk_type:02x}"),
                ));
            }
            _ => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_snappy_stream_and_uncompressed_s2_stream() {
        let payload = b"hello s2 framing format! repeated repeated repeated 1234567890";
        let snappy_stream = crate::snappy::compress_framed(payload).unwrap();
        let mut out = Vec::new();
        decompress_framed_into(&snappy_stream, &mut out, 1024).unwrap();
        assert_eq!(&out, payload);

        let mut s2_stream = Vec::new();
        s2_stream.extend_from_slice(MAGIC_STREAM_S2);
        let chunk_len = 4 + payload.len();
        s2_stream.push(CHUNK_TYPE_UNCOMPRESSED);
        s2_stream.push((chunk_len & 0xff) as u8);
        s2_stream.push(((chunk_len >> 8) & 0xff) as u8);
        s2_stream.push(((chunk_len >> 16) & 0xff) as u8);
        s2_stream.extend_from_slice(&masked_crc32c(payload).to_le_bytes());
        s2_stream.extend_from_slice(payload);

        let mut out2 = Vec::new();
        decompress_framed_into(&s2_stream, &mut out2, 1024).unwrap();
        assert_eq!(&out2, payload);

        let mut too_small = Vec::new();
        let err = decompress_framed_into(&s2_stream, &mut too_small, 10).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decodes_s2_compressed_block_with_repeat_offset() {
        let expected = b"ABCDABCDABCDABCD";
        let block = [16u8, 0x0c, b'A', b'B', b'C', b'D', 0x01, 0x04, 0x11, 0x00];
        let mut s2_stream = Vec::new();
        s2_stream.extend_from_slice(MAGIC_STREAM_S2);
        let chunk_len = 4 + block.len();
        s2_stream.push(CHUNK_TYPE_COMPRESSED);
        s2_stream.push((chunk_len & 0xff) as u8);
        s2_stream.push(((chunk_len >> 8) & 0xff) as u8);
        s2_stream.push(((chunk_len >> 16) & 0xff) as u8);
        s2_stream.extend_from_slice(&masked_crc32c(expected).to_le_bytes());
        s2_stream.extend_from_slice(&block);

        let mut out = Vec::new();
        decompress_framed_into(&s2_stream, &mut out, 1024).unwrap();
        assert_eq!(&out, expected);
    }

    #[test]
    fn decompress_framed_into_truncates_out_to_start_len_on_every_error() {
        let prefix = b"keep-prefix-";

        let mut s2_bad_crc = Vec::new();
        s2_bad_crc.extend_from_slice(MAGIC_STREAM_S2);
        let chunk1 = b"first-chunk";
        let c1_len = 4 + chunk1.len();
        s2_bad_crc.push(CHUNK_TYPE_UNCOMPRESSED);
        s2_bad_crc.push((c1_len & 0xff) as u8);
        s2_bad_crc.push(((c1_len >> 8) & 0xff) as u8);
        s2_bad_crc.push(((c1_len >> 16) & 0xff) as u8);
        s2_bad_crc.extend_from_slice(&masked_crc32c(chunk1).to_le_bytes());
        s2_bad_crc.extend_from_slice(chunk1);

        let chunk2 = b"second-chunk";
        let c2_len = 4 + chunk2.len();
        s2_bad_crc.push(CHUNK_TYPE_UNCOMPRESSED);
        s2_bad_crc.push((c2_len & 0xff) as u8);
        s2_bad_crc.push(((c2_len >> 8) & 0xff) as u8);
        s2_bad_crc.push(((c2_len >> 16) & 0xff) as u8);
        s2_bad_crc.extend_from_slice(&(masked_crc32c(chunk2) ^ 0xDEAD_BEEF).to_le_bytes());
        s2_bad_crc.extend_from_slice(chunk2);

        let mut out = prefix.to_vec();
        let err = decompress_framed_into(&s2_bad_crc, &mut out, 1024).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(out, prefix);

        let mut s2_bad_comp = Vec::new();
        s2_bad_comp.extend_from_slice(MAGIC_STREAM_S2);
        let bad_block = [32u8, 0x01, 0xff];
        let bc_len = 4 + bad_block.len();
        s2_bad_comp.push(CHUNK_TYPE_COMPRESSED);
        s2_bad_comp.push((bc_len & 0xff) as u8);
        s2_bad_comp.push(((bc_len >> 8) & 0xff) as u8);
        s2_bad_comp.push(((bc_len >> 16) & 0xff) as u8);
        s2_bad_comp.extend_from_slice(&0u32.to_le_bytes());
        s2_bad_comp.extend_from_slice(&bad_block);

        let mut out2 = prefix.to_vec();
        assert!(decompress_framed_into(&s2_bad_comp, &mut out2, 1024).is_err());
        assert_eq!(out2, prefix);

        let mut s2_comp_crc = Vec::new();
        s2_comp_crc.extend_from_slice(MAGIC_STREAM_S2);
        let valid_block = [4u8, 0x0c, b'A', b'B', b'C', b'D'];
        let vc_len = 4 + valid_block.len();
        s2_comp_crc.push(CHUNK_TYPE_COMPRESSED);
        s2_comp_crc.push((vc_len & 0xff) as u8);
        s2_comp_crc.push(((vc_len >> 8) & 0xff) as u8);
        s2_comp_crc.push(((vc_len >> 16) & 0xff) as u8);
        s2_comp_crc.extend_from_slice(&(masked_crc32c(b"ABCD") ^ 1).to_le_bytes());
        s2_comp_crc.extend_from_slice(&valid_block);

        let mut out3 = prefix.to_vec();
        assert!(decompress_framed_into(&s2_comp_crc, &mut out3, 1024).is_err());
        assert_eq!(out3, prefix);

        for bad_input in [
            &b"\xff\x06"[..],
            &b"\x01\x04\x00\x00\x00\x00\x00\x00"[..],
            &b"\xff\x06\x00\x00S2sTwO\x02\x00\x00\x00"[..],
        ] {
            let mut out_err = prefix.to_vec();
            assert!(decompress_framed_into(bad_input, &mut out_err, 1024).is_err());
            assert_eq!(out_err, prefix);
        }
    }
}
