//! Ported from github.com/grafana/dskit/grpcencoding/snappy/snappy.go
//!
//! Uses Snappy **framing** (stream) format (`\xff\x06\x00\x00sNaPpY` magic + framed CRC32C chunks),
//! matching `github.com/golang/snappy` `NewBufferedWriter` / `NewReader`.

use std::io::{Read, Write};

use bytes::{BufMut, Bytes, BytesMut};

/// Name registered for the Snappy gRPC compressor (`grpc-encoding: snappy`).
pub const SNAPPY_ENCODING_NAME: &str = "snappy";

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
    let mut reader = snap::read::FrameDecoder::new(input);
    reader.read_to_end(out)?;
    Ok(())
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
}
