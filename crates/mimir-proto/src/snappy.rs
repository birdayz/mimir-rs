//! Ported from github.com/grafana/dskit/grpcencoding/snappy/snappy.go
//!
//! Uses Snappy **framing** (stream) format (`\xff\x06\x00\x00sNaPpY` magic + framed CRC32C chunks),
//! matching `github.com/golang/snappy` `NewBufferedWriter` / `NewReader`.

use std::cell::RefCell;
use std::io::Write;

use bytes::{BufMut, Bytes, BytesMut};

/// Name registered for the Snappy gRPC compressor (`grpc-encoding: snappy`).
pub const SNAPPY_ENCODING_NAME: &str = "snappy";

const CHUNK_TYPE_COMPRESSED: u8 = 0x00;
const CHUNK_TYPE_UNCOMPRESSED: u8 = 0x01;
const CHUNK_TYPE_PADDING: u8 = 0xfe;
const CHUNK_TYPE_STREAM_IDENTIFIER: u8 = 0xff;
const MAGIC_SNAPPY: &[u8; 6] = b"sNaPpY";
const MAX_SNAPPY_BLOCK_SIZE: usize = 1 << 16;

thread_local! {
    static SNAPPY_RAW_DECODER: RefCell<snap::raw::Decoder> =
        RefCell::new(snap::raw::Decoder::new());
}

/// Error payload attached to `std::io::ErrorKind::InvalidData` when a bounded framed
/// decompression exceeds its configured byte limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecompressLimitExceeded {
    pub limit: usize,
}

impl std::fmt::Display for DecompressLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "decompressed payload exceeds limit {}", self.limit)
    }
}

impl std::error::Error for DecompressLimitExceeded {}

/// Compresses `input` using Snappy framing format (stream format).
pub fn compress_framed(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() / 2 + 32);
    compress_framed_into(input, &mut out)?;
    Ok(out)
}

/// Compresses `input` using Snappy framing format into `out` (appending to `out`).
pub fn compress_framed_into(input: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
    let mut writer = snap::write::FrameEncoder::new(out);
    writer.write_all(input)?;
    writer.flush()?;
    Ok(())
}

/// Decompresses Snappy framed stream bytes into a `Vec<u8>`.
pub fn decompress_framed(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 2);
    decompress_framed_into(input, &mut out)?;
    Ok(out)
}

/// Decompresses Snappy framed stream bytes into `out` (appending to `out`).
pub fn decompress_framed_into(input: &[u8], out: &mut Vec<u8>) -> std::io::Result<()> {
    decompress_framed_bounded_into(input, out, usize::MAX).map(|_| ())
}

#[inline]
fn decompress_raw_block(src: &[u8], dst: &mut [u8]) -> std::io::Result<usize> {
    SNAPPY_RAW_DECODER.with(|cell| {
        if let Ok(mut dec) = cell.try_borrow_mut() {
            dec.decompress(src, dst)
        } else {
            snap::raw::Decoder::new().decompress(src, dst)
        }
    })
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Decompresses Snappy framed stream bytes into `dst`, stopping BEFORE growing `dst` if more than
/// `max_len` decompressed bytes would be produced.
///
/// Returns the number of decompressed bytes appended to `dst`.
pub fn decompress_framed_bounded_into(
    src: &[u8],
    dst: &mut Vec<u8>,
    max_len: usize,
) -> std::io::Result<usize> {
    let start = dst.len();
    let mut pos = 0usize;
    let mut read_header = false;

    while pos < src.len() {
        let hdr = src.get(pos..pos + 4).ok_or_else(|| {
            dst.truncate(start);
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "snappy: truncated chunk header",
            )
        })?;
        pos += 4;
        let chunk_type = hdr[0];
        if !read_header && chunk_type != CHUNK_TYPE_STREAM_IDENTIFIER {
            dst.truncate(start);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snappy: missing stream identifier",
            ));
        }

        let chunk_len = (hdr[1] as usize) | ((hdr[2] as usize) << 8) | ((hdr[3] as usize) << 16);
        let chunk_end = pos.checked_add(chunk_len).ok_or_else(|| {
            dst.truncate(start);
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "snappy: chunk length overflow",
            )
        })?;
        let chunk = src.get(pos..chunk_end).ok_or_else(|| {
            dst.truncate(start);
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "snappy: truncated chunk payload",
            )
        })?;
        pos = chunk_end;

        match chunk_type {
            CHUNK_TYPE_STREAM_IDENTIFIER => {
                if chunk != MAGIC_SNAPPY {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: invalid stream identifier",
                    ));
                }
                read_header = true;
            }
            CHUNK_TYPE_COMPRESSED => {
                if chunk.len() < 4 {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: compressed chunk shorter than checksum",
                    ));
                }
                let checksum = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                let block = &chunk[4..];
                let dec_len = snap::raw::decompress_len(block).map_err(|e| {
                    dst.truncate(start);
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })?;
                if dec_len > MAX_SNAPPY_BLOCK_SIZE {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: decoded block exceeds max block size",
                    ));
                }
                let produced = dst.len() - start;
                if dec_len > max_len || produced > max_len - dec_len {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        DecompressLimitExceeded { limit: max_len },
                    ));
                }
                let dst_start = dst.len();
                dst.resize(dst_start + dec_len, 0);
                let written = match decompress_raw_block(block, &mut dst[dst_start..]) {
                    Ok(n) => n,
                    Err(e) => {
                        dst.truncate(start);
                        return Err(e);
                    }
                };
                if written != dec_len {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: decoded length mismatch",
                    ));
                }
                if crate::s2::masked_crc32c(&dst[dst_start..]) != checksum {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: CRC mismatch",
                    ));
                }
            }
            CHUNK_TYPE_UNCOMPRESSED => {
                if chunk.len() < 4 {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: uncompressed chunk shorter than checksum",
                    ));
                }
                let checksum = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                let payload = &chunk[4..];
                if payload.len() > MAX_SNAPPY_BLOCK_SIZE {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: uncompressed block exceeds max block size",
                    ));
                }
                let produced = dst.len() - start;
                if payload.len() > max_len || produced > max_len - payload.len() {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        DecompressLimitExceeded { limit: max_len },
                    ));
                }
                if crate::s2::masked_crc32c(payload) != checksum {
                    dst.truncate(start);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "snappy: CRC mismatch",
                    ));
                }
                dst.extend_from_slice(payload);
            }
            CHUNK_TYPE_PADDING | 0x80..=0xfd => {
                // Padding and reserved skippable chunks: payload already consumed above.
            }
            0x02..=0x7f => {
                dst.truncate(start);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("snappy: unsupported unskippable chunk 0x{chunk_type:02x}"),
                ));
            }
        }
    }

    Ok(dst.len() - start)
}

/// Encodes a single gRPC length-prefixed message frame (1-byte compressed flag + 4-byte BE length + payload),
/// using Snappy framing compression when `compress == true`.
pub fn encode_grpc_frame(payload: &[u8], compress: bool) -> std::io::Result<Bytes> {
    if !compress {
        let mut buf = BytesMut::with_capacity(5 + payload.len());
        buf.put_u8(0);
        buf.put_u32(payload.len() as u32);
        buf.put_slice(payload);
        return Ok(buf.freeze());
    }
    let compressed = compress_framed(payload)?;
    let mut buf = BytesMut::with_capacity(5 + compressed.len());
    buf.put_u8(1);
    buf.put_u32(compressed.len() as u32);
    buf.put_slice(&compressed);
    Ok(buf.freeze())
}

/// Decodes a single gRPC length-prefixed message frame, decompressing Snappy framed payload if the
/// compressed flag byte is `1`.
pub fn decode_grpc_frame(frame: &[u8]) -> std::io::Result<Bytes> {
    if frame.len() < 5 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "gRPC frame shorter than 5-byte header",
        ));
    }
    let compressed_flag = frame[0];
    let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    let body = frame.get(5..5 + len).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "gRPC frame body shorter than declared length",
        )
    })?;
    match compressed_flag {
        0 => Ok(Bytes::copy_from_slice(body)),
        1 => Ok(Bytes::from(decompress_framed(body)?)),
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid gRPC compression flag: {other}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snappy_framed_roundtrip_and_magic() {
        let input = b"hello snappy framing format! repeated repeated repeated data 1234567890";
        let compressed = compress_framed(input).expect("compress");
        // Stream identifier chunk is 10 bytes: ff 06 00 00 sNaPpY
        assert!(compressed.starts_with(b"\xff\x06\x00\x00sNaPpY"));
        let decompressed = decompress_framed(&compressed).expect("decompress");
        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_snappy_framed_empty() {
        let compressed = compress_framed(b"").expect("compress empty");
        let decompressed = decompress_framed(&compressed).expect("decompress empty");
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_snappy_framed_large_multi_chunk() {
        // Larger than 64 KiB max block size to force multiple framing chunks
        let input: Vec<u8> = (0..150_000).map(|i| (i % 251) as u8).collect();
        let compressed = compress_framed(&input).expect("compress large");
        let decompressed = decompress_framed(&compressed).expect("decompress large");
        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_grpc_frame_roundtrip() {
        let payload = b"protobuf-payload-bytes";
        let uncomp = encode_grpc_frame(payload, false).unwrap();
        assert_eq!(uncomp[0], 0);
        assert_eq!(decode_grpc_frame(&uncomp).unwrap().as_ref(), payload);

        let comp = encode_grpc_frame(payload, true).unwrap();
        assert_eq!(comp[0], 1);
        assert_eq!(decode_grpc_frame(&comp).unwrap().as_ref(), payload);
    }

    #[test]
    fn test_snappy_framed_bounded_zip_bomb_rejected() {
        let big = vec![b'z'; 256 * 1024];
        let compressed = compress_framed(&big).expect("compress");
        // Snappy framed compresses 256 KiB of identical bytes down to a tiny payload.
        assert!(compressed.len() < 16 * 1024);

        let mut dst = b"prefix".to_vec();
        let err = decompress_framed_bounded_into(&compressed, &mut dst, 4096).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(
            err.get_ref().and_then(|e| e.downcast_ref::<DecompressLimitExceeded>()),
            Some(&DecompressLimitExceeded { limit: 4096 })
        );
        // dst is restored to its pre-call length and never grew past max_len + 1 + prefix.
        assert_eq!(dst, b"prefix");

        // Exact boundary succeeds.
        let exact = compress_framed(&vec![b'x'; 4096]).expect("compress exact");
        let mut ok_dst = Vec::new();
        let n = decompress_framed_bounded_into(&exact, &mut ok_dst, 4096).unwrap();
        assert_eq!(n, 4096);
        assert_eq!(ok_dst.len(), 4096);
    }

    #[test]
    fn test_snappy_framed_incompressible_padding_skippable_and_errors() {
        // High-entropy > 150 KiB payload forces FrameEncoder to emit uncompressed (0x01) + multiple chunks.
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut input = vec![0u8; 160_000];
        for b in &mut input {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = (state & 0xff) as u8;
        }
        let mut compressed = compress_framed(&input).expect("compress incompressible");

        // Append a padding chunk (0xfe) and a reserved skippable chunk (0x85).
        compressed.extend_from_slice(&[0xfe, 0x04, 0x00, 0x00, 1, 2, 3, 4]);
        compressed.extend_from_slice(&[0x85, 0x02, 0x00, 0x00, 9, 9]);

        // Verify byte-for-byte parity with snap::read::FrameDecoder AND our zero-alloc parser.
        let mut ref_out = Vec::new();
        std::io::Read::read_to_end(&mut snap::read::FrameDecoder::new(&compressed[..]), &mut ref_out)
            .unwrap();
        assert_eq!(ref_out, input);

        let our_out = decompress_framed(&compressed).unwrap();
        assert_eq!(our_out, ref_out);

        // Reserved unskippable chunk (0x02..=0x7f) must fail with InvalidData.
        let mut bad_unskippable = compressed.clone();
        bad_unskippable.extend_from_slice(&[0x42, 0x01, 0x00, 0x00, 0x00]);
        let mut dst = Vec::new();
        let err = decompress_framed_bounded_into(&bad_unskippable, &mut dst, 1_000_000).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(dst.is_empty());

        // Corrupted CRC on uncompressed chunk must fail.
        let mut bad_uncomp_crc = Vec::new();
        bad_uncomp_crc.extend_from_slice(b"\xff\x06\x00\x00sNaPpY");
        bad_uncomp_crc.extend_from_slice(&[0x01, 0x07, 0x00, 0x00]);
        bad_uncomp_crc.extend_from_slice(&0xdead_beefu32.to_le_bytes());
        bad_uncomp_crc.extend_from_slice(b"abc");
        let err = decompress_framed_bounded_into(&bad_uncomp_crc, &mut dst, 1024).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // Corrupted CRC on compressed chunk must fail.
        let mut bad_comp_crc = compress_framed(b"aaaaaaaaaaaaaaaaaaaa").unwrap();
        let last = bad_comp_crc.len() - 1;
        bad_comp_crc[last] ^= 0xff;
        let err = decompress_framed_bounded_into(&bad_comp_crc, &mut dst, 1024).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // Missing stream identifier must fail.
        let err = decompress_framed_bounded_into(&[0x01, 0x05, 0x00, 0x00, 0, 0, 0, 0, b'a'], &mut dst, 1024)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // Zip-bomb: dst with exact preallocated capacity 16 must NOT grow when chunk declares 64 KiB.
        let bomb = compress_framed(&vec![b'q'; 65_536]).unwrap();
        let mut bounded_dst = Vec::with_capacity(16);
        let err = decompress_framed_bounded_into(&bomb, &mut bounded_dst, 16).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(bounded_dst.capacity(), 16);
        assert!(bounded_dst.is_empty());
    }
}
