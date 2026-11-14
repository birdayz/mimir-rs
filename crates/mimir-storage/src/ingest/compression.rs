//! Kafka RecordBatch compression & decompression codecs (None, Gzip, Snappy/Xerial, LZ4, Zstd).
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/compression.go
//! and Apache Kafka RecordBatch compression spec.

use std::io::{Read, Write};
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use kafka_protocol::records::Compression;
use lz4_flex::frame::{BlockMode, FrameDecoder, FrameEncoder, FrameInfo};

const XERIAL_MAGIC: &[u8; 8] = b"\x82SNAPPY\x00";

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
            let actual = snap::raw::Encoder::new()
                .compress(uncompressed, &mut out[start..])
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
    let raw = compressed.copy_to_bytes(compressed.remaining());
    match compression {
        Compression::None => Ok(raw),
        Compression::Gzip => {
            let mut decoder = GzDecoder::new(&raw[..]);
            let mut out = Vec::with_capacity(raw.len() * 2);
            decoder
                .read_to_end(&mut out)
                .context("gzip decompress record batch")?;
            Ok(Bytes::from(out))
        }
        Compression::Snappy => decompress_snappy(&raw),
        Compression::Lz4 => {
            let mut decoder = FrameDecoder::new(&raw[..]);
            let mut out = Vec::with_capacity(raw.len() * 2);
            decoder
                .read_to_end(&mut out)
                .context("lz4 decompress record batch")?;
            Ok(Bytes::from(out))
        }
        Compression::Zstd => {
            let mut out = Vec::with_capacity(raw.len() * 2);
            zstd::stream::copy_decode(&raw[..], &mut out)
                .context("zstd decompress record batch")?;
            Ok(Bytes::from(out))
        }
    }
}

fn decompress_snappy(src: &[u8]) -> Result<Bytes> {
    if src.len() >= 16 && &src[..8] == XERIAL_MAGIC {
        let mut rem = &src[16..];
        let mut out = Vec::with_capacity(src.len() * 2);
        let mut dec = snap::raw::Decoder::new();
        while !rem.is_empty() {
            if rem.len() < 4 {
                bail!("malformed xerial snappy chunk header");
            }
            let size = u32::from_be_bytes([rem[0], rem[1], rem[2], rem[3]]) as usize;
            rem = &rem[4..];
            if rem.len() < size {
                bail!("truncated xerial snappy chunk: expected {size}, got {}", rem.len());
            }
            let chunk = &rem[..size];
            rem = &rem[size..];
            let dec_len = snap::raw::decompress_len(chunk).context("snappy xerial chunk len")?;
            let start = out.len();
            out.resize(start + dec_len, 0);
            let actual = dec
                .decompress(chunk, &mut out[start..])
                .context("snappy xerial chunk decompress")?;
            out.truncate(start + actual);
        }
        return Ok(Bytes::from(out));
    }

    let dec_len = snap::raw::decompress_len(src).context("snappy raw decompress_len")?;
    let mut out = vec![0u8; dec_len];
    let actual = snap::raw::Decoder::new()
        .decompress(src, &mut out)
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
}
