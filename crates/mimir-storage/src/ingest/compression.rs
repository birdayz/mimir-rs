//! Kafka RecordBatch compression & decompression codecs (None, Gzip, Snappy/Xerial, LZ4, Zstd).
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/compression.go
//! and Apache Kafka RecordBatch compression spec.

use std::cell::RefCell;
use std::io::{Read, Write};
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use kafka_protocol::records::Compression;
use lz4_flex::frame::{BlockMode, FrameDecoder, FrameEncoder, FrameInfo};
use thiserror::Error;

const XERIAL_MAGIC: &[u8; 8] = b"\x82SNAPPY\x00";

/// Maximum decompressed size permitted for a single Kafka RecordBatch (`256 MiB`),
/// protecting against Snappy varint bombs and Gzip/LZ4/Zstd zip bombs.
pub const MAX_DECOMPRESSED_KAFKA_BATCH_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CompressionError {
    #[error("decompressed Kafka batch exceeds limit of {limit} bytes")]
    DecompressedSizeExceeded { limit: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaCompression {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl KafkaCompression {
    pub fn to_protocol(self) -> Compression {
        match self {
            KafkaCompression::None => Compression::None,
            KafkaCompression::Gzip => Compression::Gzip,
            KafkaCompression::Snappy => Compression::Snappy,
            KafkaCompression::Lz4 => Compression::Lz4,
            KafkaCompression::Zstd => Compression::Zstd,
        }
    }

    pub fn from_protocol(c: Compression) -> Self {
        match c {
            Compression::None => KafkaCompression::None,
            Compression::Gzip => KafkaCompression::Gzip,
            Compression::Snappy => KafkaCompression::Snappy,
            Compression::Lz4 => KafkaCompression::Lz4,
            Compression::Zstd => KafkaCompression::Zstd,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            KafkaCompression::None => "none",
            KafkaCompression::Gzip => "gzip",
            KafkaCompression::Snappy => "snappy",
            KafkaCompression::Lz4 => "lz4",
            KafkaCompression::Zstd => "zstd",
        }
    }
}

impl FromStr for KafkaCompression {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "" | "none" => Ok(KafkaCompression::None),
            "gzip" => Ok(KafkaCompression::Gzip),
            "snappy" => Ok(KafkaCompression::Snappy),
            "lz4" => Ok(KafkaCompression::Lz4),
            "zstd" => Ok(KafkaCompression::Zstd),
            other => bail!("unknown Kafka compression codec: {other:?}"),
        }
    }
}

/// Custom compressor callback for `kafka_protocol::records::RecordBatchEncoder::encode_with_custom_compression`.
pub fn compress_record_batch(
    uncompressed: &mut BytesMut,
    out: &mut BytesMut,
    compression: Compression,
) -> Result<()> {
    match compression {
        Compression::None => {
            out.extend_from_slice(uncompressed);
            Ok(())
        }
        Compression::Gzip => {
            let mut encoder = GzEncoder::new(out.writer(), flate2::Compression::default());
            encoder
                .write_all(uncompressed)
                .context("gzip compress record batch")?;
            encoder.finish().context("gzip finish record batch")?;
            Ok(())
        }
        Compression::Snappy => {
            let start = out.len();
            let max_len = snap::raw::max_compress_len(uncompressed.len());
            out.resize(start + max_len, 0);
            let actual = SNAPPY_ENCODER
                .with(|enc| enc.borrow_mut().compress(uncompressed, &mut out[start..]))
                .context("snappy compress record batch")?;
            out.truncate(start + actual);
            Ok(())
        }
        Compression::Lz4 => {
            let mut frame_info = FrameInfo::new();
            frame_info.block_mode = BlockMode::Independent;
            let mut encoder = FrameEncoder::with_frame_info(frame_info, out.writer());
            encoder
                .write_all(uncompressed)
                .context("lz4 compress record batch")?;
            encoder.finish().context("lz4 finish record batch")?;
            Ok(())
        }
        Compression::Zstd => {
            zstd::stream::copy_encode(&uncompressed[..], out.writer(), 3)
                .context("zstd compress record batch")?;
            Ok(())
        }
    }
}

/// Custom decompressor callback for `kafka_protocol::records::RecordBatchDecoder::decode_with_custom_compression`.
pub fn decompress_record_batch(
    compressed: &mut Bytes,
    compression: Compression,
) -> Result<Bytes> {
    decompress_record_batch_with_limit(compressed, compression, MAX_DECOMPRESSED_KAFKA_BATCH_BYTES)
}

/// Decompresses a Kafka RecordBatch payload while enforcing `max_bytes` as a hard upper bound on
/// the decompressed output size before allocating large buffers.
pub fn decompress_record_batch_with_limit(
    compressed: &mut Bytes,
    compression: Compression,
    max_bytes: usize,
) -> Result<Bytes> {
    let raw = compressed.copy_to_bytes(compressed.remaining());
    match compression {
        Compression::None => {
            if raw.len() > max_bytes {
                return Err(CompressionError::DecompressedSizeExceeded { limit: max_bytes }.into());
            }
            Ok(raw)
        }
        Compression::Gzip => {
            let mut out = Vec::with_capacity(raw.len().saturating_mul(2).min(max_bytes));
            read_bounded(GzDecoder::new(&raw[..]), max_bytes, &mut out, "gzip")?;
            Ok(Bytes::from(out))
        }
        Compression::Snappy => decompress_snappy(&raw, max_bytes),
        Compression::Lz4 => {
            let mut out = Vec::with_capacity(raw.len().saturating_mul(2).min(max_bytes));
            read_bounded(FrameDecoder::new(&raw[..]), max_bytes, &mut out, "lz4")?;
            Ok(Bytes::from(out))
        }
        Compression::Zstd => decompress_zstd(&raw, max_bytes),
    }
}

fn read_bounded<R: Read>(
    reader: R,
    max_bytes: usize,
    out: &mut Vec<u8>,
    codec: &str,
) -> Result<()> {
    let start = out.len();
    let remaining = max_bytes.saturating_sub(start);
    let mut limited = reader.take((remaining as u64).saturating_add(1));
    limited
        .read_to_end(out)
        .with_context(|| format!("{codec} decompress record batch"))?;
    if out.len() - start > remaining {
        return Err(CompressionError::DecompressedSizeExceeded { limit: max_bytes }.into());
    }
    Ok(())
}

thread_local! {
    static ZSTD_DECOMPRESSOR: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new(zstd::bulk::Decompressor::default());
    static SNAPPY_DECODER: RefCell<snap::raw::Decoder> =
        RefCell::new(snap::raw::Decoder::new());
    static SNAPPY_ENCODER: RefCell<snap::raw::Encoder> =
        RefCell::new(snap::raw::Encoder::new());
}

fn decompress_zstd(raw: &[u8], max_bytes: usize) -> Result<Bytes> {
    if let Ok(Some(sz)) = zstd::zstd_safe::get_frame_content_size(raw) {
        if sz > max_bytes as u64 {
            return Err(CompressionError::DecompressedSizeExceeded { limit: max_bytes }.into());
        }
        if sz <= 64 * 1024 * 1024 {
            let mut out = Vec::with_capacity(sz as usize);
            let bulk_res = ZSTD_DECOMPRESSOR
                .with(|dec| dec.borrow_mut().decompress_to_buffer(raw, &mut out));
            if bulk_res.is_ok() {
                return Ok(Bytes::from(out));
            }
        }
    }
    let decoder = zstd::stream::read::Decoder::new(raw).context("zstd decompress record batch")?;
    let mut out = Vec::with_capacity(raw.len().saturating_mul(3).min(max_bytes));
    read_bounded(decoder, max_bytes, &mut out, "zstd")?;
    Ok(Bytes::from(out))
}

fn decompress_snappy(src: &[u8], max_bytes: usize) -> Result<Bytes> {
    if src.len() >= 16 && &src[..8] == XERIAL_MAGIC {
        let mut rem = &src[16..];
        let mut out = Vec::with_capacity(src.len().saturating_mul(2).min(max_bytes));
        SNAPPY_DECODER.with(|dec| {
            let mut dec = dec.borrow_mut();
            while !rem.is_empty() {
                if rem.len() < 4 {
                    bail!("malformed xerial snappy chunk header");
                }
                let size = u32::from_be_bytes([rem[0], rem[1], rem[2], rem[3]]) as usize;
                rem = &rem[4..];
                if rem.len() < size {
                    bail!(
                        "truncated xerial snappy chunk: expected {size}, got {}",
                        rem.len()
                    );
                }
                let chunk = &rem[..size];
                rem = &rem[size..];
                let dec_len = snap::raw::decompress_len(chunk).context("snappy xerial chunk len")?;
                let start = out.len();
                let new_len = start
                    .checked_add(dec_len)
                    .ok_or(CompressionError::DecompressedSizeExceeded { limit: max_bytes })?;
                if new_len > max_bytes {
                    return Err(CompressionError::DecompressedSizeExceeded { limit: max_bytes }.into());
                }
                out.resize(new_len, 0);
                let actual = dec
                    .decompress(chunk, &mut out[start..])
                    .context("snappy xerial chunk decompress")?;
                out.truncate(start + actual);
            }
            Ok(())
        })?;
        return Ok(Bytes::from(out));
    }

    let dec_len = snap::raw::decompress_len(src).context("snappy raw decompress_len")?;
    if dec_len > max_bytes {
        return Err(CompressionError::DecompressedSizeExceeded { limit: max_bytes }.into());
    }
    let mut out = vec![0u8; dec_len];
    let actual = SNAPPY_DECODER
        .with(|dec| dec.borrow_mut().decompress(src, &mut out))
        .context("snappy raw decompress")?;
    out.truncate(actual);
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_all_codecs() {
        let payload = b"hello mimir kafka ingest storage compression test payload repeated repeated";
        for codec in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let mut src = BytesMut::from(&payload[..]);
            let mut compressed = BytesMut::new();
            compress_record_batch(&mut src, &mut compressed, codec).unwrap();

            let mut comp_bytes = compressed.freeze();
            let decompressed = decompress_record_batch(&mut comp_bytes, codec).unwrap();
            assert_eq!(decompressed.as_ref(), &payload[..], "codec {codec:?}");
        }
    }

    #[test]
    fn test_xerial_snappy_decode() {
        let chunk1 = snap::raw::Encoder::new().compress_vec(b"hello ").unwrap();
        let chunk2 = snap::raw::Encoder::new().compress_vec(b"xerial").unwrap();
        let mut xerial = Vec::new();
        xerial.extend_from_slice(XERIAL_MAGIC);
        xerial.extend_from_slice(&1u32.to_be_bytes());
        xerial.extend_from_slice(&1u32.to_be_bytes());
        xerial.extend_from_slice(&(chunk1.len() as u32).to_be_bytes());
        xerial.extend_from_slice(&chunk1);
        xerial.extend_from_slice(&(chunk2.len() as u32).to_be_bytes());
        xerial.extend_from_slice(&chunk2);

        let mut buf = Bytes::from(xerial);
        let dec = decompress_record_batch(&mut buf, Compression::Snappy).unwrap();
        assert_eq!(dec.as_ref(), b"hello xerial");
    }

    #[test]
    fn test_snappy_4gib_varint_bomb_rejected() {
        // 5-byte Snappy header claiming u32::MAX (4 GiB) decompressed length.
        let bomb = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0x0f]);
        let mut buf = bomb.clone();
        let err = decompress_record_batch(&mut buf, Compression::Snappy).unwrap_err();
        assert_eq!(
            err.downcast_ref::<CompressionError>(),
            Some(&CompressionError::DecompressedSizeExceeded {
                limit: MAX_DECOMPRESSED_KAFKA_BATCH_BYTES
            })
        );

        // Same bomb wrapped inside a Xerial Snappy chunk.
        let mut xerial = Vec::new();
        xerial.extend_from_slice(XERIAL_MAGIC);
        xerial.extend_from_slice(&1u32.to_be_bytes());
        xerial.extend_from_slice(&1u32.to_be_bytes());
        xerial.extend_from_slice(&(bomb.len() as u32).to_be_bytes());
        xerial.extend_from_slice(&bomb);
        let mut xbuf = Bytes::from(xerial);
        let err = decompress_record_batch(&mut xbuf, Compression::Snappy).unwrap_err();
        assert_eq!(
            err.downcast_ref::<CompressionError>(),
            Some(&CompressionError::DecompressedSizeExceeded {
                limit: MAX_DECOMPRESSED_KAFKA_BATCH_BYTES
            })
        );
    }

    #[test]
    fn test_xerial_snappy_cumulative_size_limit() {
        let chunk1 = snap::raw::Encoder::new().compress_vec(&vec![b'a'; 600]).unwrap();
        let chunk2 = snap::raw::Encoder::new().compress_vec(&vec![b'b'; 600]).unwrap();
        let mut xerial = Vec::new();
        xerial.extend_from_slice(XERIAL_MAGIC);
        xerial.extend_from_slice(&1u32.to_be_bytes());
        xerial.extend_from_slice(&1u32.to_be_bytes());
        xerial.extend_from_slice(&(chunk1.len() as u32).to_be_bytes());
        xerial.extend_from_slice(&chunk1);
        xerial.extend_from_slice(&(chunk2.len() as u32).to_be_bytes());
        xerial.extend_from_slice(&chunk2);

        let mut buf = Bytes::from(xerial);
        let err = decompress_record_batch_with_limit(&mut buf, Compression::Snappy, 1024)
            .unwrap_err();
        assert_eq!(
            err.downcast_ref::<CompressionError>(),
            Some(&CompressionError::DecompressedSizeExceeded { limit: 1024 })
        );
    }

    #[test]
    fn test_gzip_lz4_zstd_zip_bombs_rejected() {
        let zeros = vec![0u8; 64 * 1024];
        for codec in [Compression::Gzip, Compression::Lz4, Compression::Zstd] {
            let mut src = BytesMut::from(&zeros[..]);
            let mut compressed = BytesMut::new();
            compress_record_batch(&mut src, &mut compressed, codec).unwrap();
            let comp = compressed.freeze();

            let mut buf = comp.clone();
            let err = decompress_record_batch_with_limit(&mut buf, codec, 1024).unwrap_err();
            assert_eq!(
                err.downcast_ref::<CompressionError>(),
                Some(&CompressionError::DecompressedSizeExceeded { limit: 1024 }),
                "codec {codec:?}"
            );
        }

        // Real Zstd frame claiming > 256 MiB content size is rejected by default limit before allocation.
        let mut frame = Vec::new();
        // Zstd magic (0xFD2FB528 LE), Frame_Header_Descriptor (0xE0: FCS_Flag=3 -> 8-byte FCS, single segment),
        // Frame_Content_Size = MAX_DECOMPRESSED_KAFKA_BATCH_BYTES + 1
        frame.extend_from_slice(&0xFD2F_B528u32.to_le_bytes());
        frame.push(0xE0);
        frame.extend_from_slice(&((MAX_DECOMPRESSED_KAFKA_BATCH_BYTES as u64) + 1).to_le_bytes());
        let mut zbuf = Bytes::from(frame);
        let err = decompress_record_batch(&mut zbuf, Compression::Zstd).unwrap_err();
        assert_eq!(
            err.downcast_ref::<CompressionError>(),
            Some(&CompressionError::DecompressedSizeExceeded {
                limit: MAX_DECOMPRESSED_KAFKA_BATCH_BYTES
            })
        );
    }
}
