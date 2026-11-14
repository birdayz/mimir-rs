//! Big-endian fixed integer and varint encoding/decoding buffers for TSDB WAL and index files.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/encoding/encoding.go.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodingError {
    #[error("invalid size / unexpected end of buffer")]
    InvalidSize,
    #[error("invalid checksum (expected {expected:#x}, actual {actual:#x})")]
    InvalidChecksum { expected: u32, actual: u32 },
    #[error("invalid varint encoding")]
    InvalidVarint,
    #[error("invalid UTF-8 string in buffer")]
    InvalidUtf8,
}

/// Reusable encoding buffer matching Prometheus `encoding.Encbuf`.
#[derive(Clone, Debug, Default)]
pub struct Encbuf {
    pub buf: Vec<u8>,
}

impl Encbuf {
    #[inline]
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.buf.clear();
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub fn put_byte(&mut self, b: u8) {
        self.buf.push(b);
    }

    #[inline]
    pub fn put_bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    #[inline]
    pub fn put_str(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
    }

    #[inline]
    pub fn put_be32(&mut self, x: u32) {
        self.buf.extend_from_slice(&x.to_be_bytes());
    }

    #[inline]
    pub fn put_be64(&mut self, x: u64) {
        self.buf.extend_from_slice(&x.to_be_bytes());
    }

    #[inline]
    pub fn put_be64_i64(&mut self, x: i64) {
        self.buf.extend_from_slice(&x.to_be_bytes());
    }

    #[inline]
    pub fn put_be_f64(&mut self, x: f64) {
        self.put_be64(x.to_bits());
    }

    #[inline]
    pub fn put_uvarint64(&mut self, mut x: u64) {
        while x >= 0x80 {
            self.buf.push((x as u8) | 0x80);
            x >>= 7;
        }
        self.buf.push(x as u8);
    }

    #[inline]
    pub fn put_uvarint32(&mut self, x: u32) {
        self.put_uvarint64(u64::from(x));
    }

    #[inline]
    pub fn put_uvarint(&mut self, x: usize) {
        self.put_uvarint64(x as u64);
    }

    #[inline]
    pub fn put_varint64(&mut self, x: i64) {
        let mut ux = (x as u64) << 1;
        if x < 0 {
            ux = !ux;
        }
        self.put_uvarint64(ux);
    }

    #[inline]
    pub fn put_uvarint_str(&mut self, s: &str) {
        self.put_uvarint(s.len());
        self.put_str(s);
    }

    #[inline]
    pub fn put_uvarint_bytes(&mut self, b: &[u8]) {
        self.put_uvarint(b.len());
        self.put_bytes(b);
    }

    /// Appends CRC32C (Castagnoli) over the current buffer contents.
    #[inline]
    pub fn put_crc32c(&mut self) {
        let crc = crc32c::crc32c(&self.buf);
        self.put_be32(crc);
    }
}

/// Safe bounds-checked decoding cursor matching Prometheus `encoding.Decbuf`.
#[derive(Clone, Debug)]
pub struct Decbuf<'a> {
    pub buf: &'a [u8],
}

impl<'a> Decbuf<'a> {
    #[inline]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub const fn remaining(&self) -> &'a [u8] {
        self.buf
    }

    #[inline]
    pub fn skip(&mut self, n: usize) -> Result<(), EncodingError> {
        if self.buf.len() < n {
            return Err(EncodingError::InvalidSize);
        }
        self.buf = &self.buf[n..];
        Ok(())
    }

    #[inline]
    pub fn byte(&mut self) -> Result<u8, EncodingError> {
        if self.buf.is_empty() {
            return Err(EncodingError::InvalidSize);
        }
        let b = self.buf[0];
        self.buf = &self.buf[1..];
        Ok(b)
    }

    #[inline]
    pub fn be32(&mut self) -> Result<u32, EncodingError> {
        if self.buf.len() < 4 {
            return Err(EncodingError::InvalidSize);
        }
        let v = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]);
        self.buf = &self.buf[4..];
        Ok(v)
    }

    #[inline]
    pub fn be64(&mut self) -> Result<u64, EncodingError> {
        if self.buf.len() < 8 {
            return Err(EncodingError::InvalidSize);
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.buf[..8]);
        self.buf = &self.buf[8..];
        Ok(u64::from_be_bytes(arr))
    }

    #[inline]
    pub fn be64_i64(&mut self) -> Result<i64, EncodingError> {
        self.be64().map(|v| v as i64)
    }

    #[inline]
    pub fn be64_f64(&mut self) -> Result<f64, EncodingError> {
        self.be64().map(f64::from_bits)
    }

    #[inline]
    pub fn uvarint64(&mut self) -> Result<u64, EncodingError> {
        let mut x = 0u64;
        let mut s = 0u32;
        for (i, &b) in self.buf.iter().enumerate() {
            if i == 10 {
                return Err(EncodingError::InvalidVarint);
            }
            if b < 0x80 {
                if i == 9 && b > 1 {
                    return Err(EncodingError::InvalidVarint);
                }
                self.buf = &self.buf[i + 1..];
                return Ok(x | (u64::from(b) << s));
            }
            x |= u64::from(b & 0x7f) << s;
            s += 7;
        }
        Err(EncodingError::InvalidSize)
    }

    #[inline]
    pub fn uvarint32(&mut self) -> Result<u32, EncodingError> {
        self.uvarint64().map(|v| v as u32)
    }

    #[inline]
    pub fn uvarint(&mut self) -> Result<usize, EncodingError> {
        self.uvarint64().map(|v| v as usize)
    }

    #[inline]
    pub fn varint64(&mut self) -> Result<i64, EncodingError> {
        let ux = self.uvarint64()?;
        let x = (ux >> 1) as i64;
        if (ux & 1) != 0 {
            Ok(!x)
        } else {
            Ok(x)
        }
    }

    #[inline]
    pub fn uvarint_bytes(&mut self) -> Result<&'a [u8], EncodingError> {
        let len = self.uvarint()?;
        if self.buf.len() < len {
            return Err(EncodingError::InvalidSize);
        }
        let s = &self.buf[..len];
        self.buf = &self.buf[len..];
        Ok(s)
    }

    #[inline]
    pub fn uvarint_str(&mut self) -> Result<&'a str, EncodingError> {
        let b = self.uvarint_bytes()?;
        std::str::from_utf8(b).map_err(|_| EncodingError::InvalidUtf8)
    }
}

/// Encodes `v` as unsigned LEB128 into `out` and returns bytes written.
#[inline]
pub fn put_uvarint(out: &mut [u8], mut v: u64) -> usize {
    let mut i = 0;
    while v >= 0x80 {
        out[i] = (v as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    out[i] = v as u8;
    i + 1
}

/// Decodes unsigned LEB128 from `buf`, returning `(value, bytes_read)` or `None` on truncation/overflow.
#[inline]
pub fn decode_uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if i == 10 {
            return None;
        }
        if b < 0x80 {
            if i == 9 && b > 1 {
                return None;
            }
            return Some((x | (u64::from(b) << s), i + 1));
        }
        x |= u64::from(b & 0x7f) << s;
        s += 7;
    }
    None
}
