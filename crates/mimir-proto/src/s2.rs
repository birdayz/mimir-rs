//! Ported from:
//! - github.com/grafana/mimir/pkg/util/grpcencoding/s2/s2.go
//! - github.com/klauspost/compress/s2/{s2,decode,decode_other,reader}.go
//!
//! Decodes S2 stream-framed gRPC payloads (`grpc-encoding: s2`), which also supports
//! Snappy stream-framed chunks (`\xff\x06\x00\x00sNaPpY`) as a subset.

use std::io;

/// Name registered for the S2 gRPC compressor (`grpc-encoding: s2`).
pub const S2_ENCODING_NAME: &str = "s2";

const CHUNK_TYPE_COMPRESSED: u8 = 0x00;
const CHUNK_TYPE_UNCOMPRESSED: u8 = 0x01;
const CHUNK_TYPE_STREAM_IDENTIFIER: u8 = 0xff;
const MAGIC_SNAPPY: &[u8; 6] = b"sNaPpY";
const MAGIC_S2: &[u8; 6] = b"S2sTwO";

const MAX_BLOCK_SIZE: usize = 4 << 20;
const MAX_SNAPPY_BLOCK_SIZE: usize = 1 << 16;

#[inline]
pub fn masked_crc32c(data: &[u8]) -> u32 {
    let c = crc32c::crc32c(data);
    c.rotate_right(15).wrapping_add(0xa282_ead8)
}

fn decode_varint_u32(src: &[u8]) -> io::Result<(usize, usize)> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in src.iter().take(5).enumerate() {
        if i == 4 && b > 0x0f {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "s2: corrupt block length varint",
            ));
        }
        v |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return Ok((v as usize, i + 1));
        }
        shift += 7;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "s2: truncated block length varint",
    ))
}

fn s2_decode(dst: &mut [u8], src: &[u8]) -> io::Result<()> {
    let mut d = 0usize;
    let mut s = 0usize;
    let mut offset = 0usize;

    while s < src.len() {
        let tag_byte = src[s];
        match tag_byte & 0x03 {
            0x00 => {
                // tagLiteral
                let mut x = (tag_byte >> 2) as usize;
                match x {
                    0..=59 => {
                        s += 1;
                    }
                    60 => {
                        let b = *src.get(s + 1).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "s2: truncated literal")
                        })?;
                        x = b as usize;
                        s += 2;
                    }
                    61 => {
                        let bytes = src.get(s + 1..s + 3).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "s2: truncated literal")
                        })?;
                        x = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                        s += 3;
                    }
                    62 => {
                        let bytes = src.get(s + 1..s + 4).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "s2: truncated literal")
                        })?;
                        x = (bytes[0] as usize)
                            | ((bytes[1] as usize) << 8)
                            | ((bytes[2] as usize) << 16);
                        s += 4;
                    }
                    _ => {
                        // 63
                        let bytes = src.get(s + 1..s + 5).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "s2: truncated literal")
                        })?;
                        x = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
                        s += 5;
                    }
                }
                let length = x + 1;
                if length > dst.len() - d || length > src.len() - s {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: corrupt literal length",
                    ));
                }
                dst[d..d + length].copy_from_slice(&src[s..s + length]);
                d += length;
                s += length;
                continue;
            }
            0x01 => {
                // tagCopy1 (with S2 repeat-offset extension when toffset == 0)
                let b1 = *src.get(s + 1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "s2: truncated copy1")
                })?;
                s += 2;
                let mut length = ((tag_byte >> 2) & 0x07) as usize;
                let toffset = (((tag_byte as usize) & 0xe0) << 3) | (b1 as usize);
                if toffset == 0 {
                    match length {
                        5 => {
                            let b = *src.get(s).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "s2: truncated repeat copy",
                                )
                            })?;
                            length = (b as usize) + 4;
                            s += 1;
                        }
                        6 => {
                            let bytes = src.get(s..s + 2).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "s2: truncated repeat copy",
                                )
                            })?;
                            length = u16::from_le_bytes([bytes[0], bytes[1]]) as usize + (1 << 8);
                            s += 2;
                        }
                        7 => {
                            let bytes = src.get(s..s + 3).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "s2: truncated repeat copy",
                                )
                            })?;
                            length = ((bytes[0] as usize)
                                | ((bytes[1] as usize) << 8)
                                | ((bytes[2] as usize) << 16))
                                + (1 << 16);
                            s += 3;
                        }
                        _ => {}
                    }
                } else {
                    offset = toffset;
                }
                length += 4;
                if offset == 0 || d < offset || length > dst.len() - d {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: corrupt copy1 offset/length",
                    ));
                }
                if offset >= length {
                    dst.copy_within(d - offset..d - offset + length, d);
                } else {
                    for i in 0..length {
                        dst[d + i] = dst[d - offset + i];
                    }
                }
                d += length;
            }
            0x02 => {
                // tagCopy2
                let bytes = src.get(s + 1..s + 3).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "s2: truncated copy2")
                })?;
                let length = 1 + (tag_byte >> 2) as usize;
                offset = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                s += 3;
                if offset == 0 || d < offset || length > dst.len() - d {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: corrupt copy2 offset/length",
                    ));
                }
                if offset >= length {
                    dst.copy_within(d - offset..d - offset + length, d);
                } else {
                    for i in 0..length {
                        dst[d + i] = dst[d - offset + i];
                    }
                }
                d += length;
            }
            _ => {
                // 0x03: tagCopy4
                let bytes = src.get(s + 1..s + 5).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "s2: truncated copy4")
                })?;
                let length = 1 + (tag_byte >> 2) as usize;
                offset = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
                s += 5;
                if offset == 0 || d < offset || length > dst.len() - d {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "s2: corrupt copy4 offset/length",
                    ));
                }
                if offset >= length {
                    dst.copy_within(d - offset..d - offset + length, d);
                } else {
                    for i in 0..length {
                        dst[d + i] = dst[d - offset + i];
                    }
                }
                d += length;
            }
        }
    }

    if d != dst.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "s2: decoded length mismatch",
        ));
    }
    Ok(())
}

/// Decompresses S2 (or Snappy) stream-framed bytes into `out`, enforcing `max_out_len` on the
/// number of decompressed bytes appended.
pub fn decompress_framed_into(
    input: &[u8],
    out: &mut Vec<u8>,
    max_out_len: usize,
) -> io::Result<()> {
    let start_len = out.len();
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
                let (dec_len, var_len) = decode_varint_u32(block)?;
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
                s2_decode(&mut out[dst_start..], &block[var_len..])?;
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
            _ => {
                // 0x80..=0xfe: padding and reserved skippable chunks (already read above).
            }
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

        // S2 uncompressed stream: stream id ("S2sTwO") + uncompressed chunk (0x01).
        let mut s2_stream = Vec::new();
        s2_stream.extend_from_slice(b"\xff\x06\x00\x00S2sTwO");
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

        // Limit enforcement.
        let mut too_small = Vec::new();
        let err = decompress_framed_into(&s2_stream, &mut too_small, 10).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decodes_s2_compressed_block_with_repeat_offset() {
        // Construct an S2 compressed block using literal + copy1 + repeat-offset copy1:
        // "ABCDABCDABCDABCD" (16 bytes):
        // - literal 4 bytes "ABCD": tag=0 | ((4-1)<<2) = 0x0c, "ABCD"
        // - copy1 offset=4, len=4: ((4-4)<<2) | 0x01 | ((4 & 0x700)>>3) = 0x01, 0x04
        // - repeat copy1 len=8 (tag len=4 -> 4+4=8, toffset=0): (4<<2) | 0x01 = 0x11, 0x00
        let expected = b"ABCDABCDABCDABCD";
        let block = [
            16u8, // varint len = 16
            0x0c, b'A', b'B', b'C', b'D', // literal 4
            0x01, 0x04, // copy1 len=4, offset=4 -> "ABCD"
            0x11, 0x00, // repeat copy1 len=8, offset=4 -> "ABCDABCD"
        ];
        let mut s2_stream = Vec::new();
        s2_stream.extend_from_slice(b"\xff\x06\x00\x00S2sTwO");
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
}
