//! Bit-level stream writer and reader matching Prometheus `bstream.go` bit-for-bit.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/bstream.go.

use super::ChunkError;

/// Bit-level writer matching `chunkenc.bstream` in Prometheus TSDB.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BitStream {
    pub(crate) stream: Vec<u8>,
    /// Number of unused right-most bits remaining in the last byte of `stream` (`0..=8`).
    pub(crate) count: u8,
}

impl BitStream {
    #[inline]
    pub fn new() -> Self {
        Self {
            stream: Vec::new(),
            count: 0,
        }
    }

    #[inline]
    pub fn with_header(header_len: usize, capacity: usize) -> Self {
        let mut stream = Vec::with_capacity(capacity.max(header_len));
        stream.resize(header_len, 0);
        Self { stream, count: 0 }
    }

    #[inline]
    pub fn from_bytes(stream: Vec<u8>, count: u8) -> Self {
        Self { stream, count }
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.stream
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.stream
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.stream
    }

    #[inline]
    pub fn compact(&mut self, threshold: usize) {
        if self.stream.capacity() > self.stream.len() + threshold {
            self.stream.shrink_to_fit();
        }
    }

    /// Appends a single bit to the stream.
    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if self.count == 0 {
            self.stream.push(0);
            self.count = 8;
        }
        if bit {
            let last = self.stream.len() - 1;
            self.stream[last] |= 1u8 << (self.count - 1);
        }
        self.count -= 1;
    }

    /// Appends a full byte to the stream (aligned or unaligned).
    #[inline]
    pub fn write_byte(&mut self, byt: u8) {
        if self.count == 0 {
            self.stream.push(byt);
            return;
        }
        let last = self.stream.len() - 1;
        self.stream[last] |= byt >> (8 - self.count);
        self.stream.push(byt << self.count);
    }

    /// Appends the `nbits` right-most bits of `u` in MSB-first order (`0 <= nbits <= 64`).
    #[inline]
    pub fn write_bits(&mut self, u: u64, nbits: usize) {
        if nbits == 0 {
            return;
        }
        debug_assert!(nbits <= 64);
        let mut u = u << (64 - nbits);
        let mut rem = nbits;

        if self.count > 0 {
            let free = self.count as usize;
            let last = self.stream.len() - 1;
            self.stream[last] |= (u >> (64 - free)) as u8;
            if rem < free {
                self.count = (free - rem) as u8;
                return;
            }
            u <<= free;
            rem -= free;
            self.count = 0;
        }

        while rem >= 8 {
            self.stream.push((u >> 56) as u8);
            u <<= 8;
            rem -= 8;
        }

        if rem > 0 {
            self.stream.push((u >> 56) as u8);
            self.count = (8 - rem) as u8;
        }
    }

    /// Writes a Go-compatible ZigZag-encoded signed 64-bit varint byte-by-byte.
    #[inline]
    pub fn write_varint(&mut self, x: i64) {
        let mut ux = (x as u64) << 1;
        if x < 0 {
            ux = !ux;
        }
        self.write_uvarint(ux);
    }

    /// Writes a Go-compatible unsigned LEB128 64-bit varint byte-by-byte.
    #[inline]
    pub fn write_uvarint(&mut self, mut x: u64) {
        while x >= 0x80 {
            self.write_byte((x as u8) | 0x80);
            x >>= 7;
        }
        self.write_byte(x as u8);
    }
}

impl Default for BitStream {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
const fn bitmask(nbits: u8) -> u64 {
    if nbits >= 64 {
        u64::MAX
    } else {
        (1u64 << nbits) - 1
    }
}

/// Bit-level reader matching `chunkenc.bstreamReader` in Prometheus TSDB.
#[derive(Clone, Debug)]
pub struct BitReader<'a> {
    stream: &'a [u8],
    stream_offset: usize,
    buffer: u64,
    pub(crate) valid: u8,
}

impl<'a> BitReader<'a> {
    #[inline]
    pub fn new(stream: &'a [u8]) -> Self {
        Self {
            stream,
            stream_offset: 0,
            buffer: 0,
            valid: 0,
        }
    }

    #[inline]
    pub fn valid_bits(&self) -> u8 {
        self.valid
    }

    #[inline]
    pub fn read_bit(&mut self) -> Result<bool, ChunkError> {
        if self.valid == 0 && !self.load_next_buffer(1) {
            return Err(ChunkError::UnexpectedEof);
        }
        self.valid -= 1;
        let mask = 1u64 << self.valid;
        Ok((self.buffer & mask) != 0)
    }

    #[inline]
    pub fn peek_bit(&self) -> Option<bool> {
        if self.valid >= 1 {
            Some(((self.buffer >> (self.valid - 1)) & 1) != 0)
        } else {
            None
        }
    }

    #[inline]
    pub fn consume_bit_unchecked(&mut self) {
        self.valid -= 1;
    }

    #[inline]
    pub fn peek_bits_fast(&self, nbits: u8) -> Option<u64> {
        if self.valid >= nbits {
            Some((self.buffer >> (self.valid - nbits)) & bitmask(nbits))
        } else {
            None
        }
    }

    #[inline]
    pub fn consume_bits_unchecked(&mut self, nbits: u8) {
        self.valid -= nbits;
    }

    /// Reads `nbits` bits (`0 <= nbits <= 64`) MSB-first into the lower bits of a `u64`.
    #[inline]
    pub fn read_bits(&mut self, nbits: u8) -> Result<u64, ChunkError> {
        if nbits == 0 {
            return Ok(0);
        }
        debug_assert!(nbits <= 64);
        if self.valid == 0 && !self.load_next_buffer(nbits) {
            return Err(ChunkError::UnexpectedEof);
        }

        if nbits <= self.valid {
            self.valid -= nbits;
            return Ok((self.buffer >> self.valid) & bitmask(nbits));
        }

        let mask1 = bitmask(self.valid);
        let rem = nbits - self.valid;
        let mut v = (self.buffer & mask1) << rem;
        self.valid = 0;

        if !self.load_next_buffer(rem) {
            return Err(ChunkError::UnexpectedEof);
        }

        v |= (self.buffer >> (self.valid - rem)) & bitmask(rem);
        self.valid -= rem;
        Ok(v)
    }

    #[inline]
    pub fn read_byte(&mut self) -> Result<u8, ChunkError> {
        self.read_bits(8).map(|v| v as u8)
    }

    /// Reads a Go-compatible unsigned LEB128 64-bit varint.
    #[inline]
    pub fn read_uvarint(&mut self) -> Result<u64, ChunkError> {
        let mut x = 0u64;
        let mut s = 0u32;
        for _ in 0..10 {
            let byt = self.read_byte()?;
            if byt < 0x80 {
                if s >= 64 || (s == 63 && byt > 1) {
                    return Err(ChunkError::UnexpectedEof);
                }
                return Ok(x | (u64::from(byt) << s));
            }
            x |= u64::from(byt & 0x7f) << s;
            s += 7;
        }
        Err(ChunkError::UnexpectedEof)
    }

    /// Reads a Go-compatible ZigZag-encoded signed 64-bit varint.
    #[inline]
    pub fn read_varint(&mut self) -> Result<i64, ChunkError> {
        let ux = self.read_uvarint()?;
        let x = (ux >> 1) as i64;
        if (ux & 1) != 0 {
            Ok(!x)
        } else {
            Ok(x)
        }
    }

    #[inline]
    fn load_next_buffer(&mut self, nbits: u8) -> bool {
        if self.stream_offset >= self.stream.len() {
            return false;
        }

        if self.stream_offset + 8 < self.stream.len() {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&self.stream[self.stream_offset..self.stream_offset + 8]);
            self.buffer = u64::from_be_bytes(arr);
            self.stream_offset += 8;
            self.valid = 64;
            return true;
        }

        let avail = self.stream.len() - self.stream_offset;
        let nbytes = ((nbits as usize / 8) + 1).min(avail);
        let mut buffer = 0u64;
        for i in 0..nbytes {
            buffer |= u64::from(self.stream[self.stream_offset + i]) << (8 * (nbytes - i - 1));
        }
        self.buffer = buffer;
        self.stream_offset += nbytes;
        self.valid = (nbytes * 8) as u8;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_write_read_roundtrip() {
        let mut bs = BitStream::new();
        bs.write_bit(true);
        bs.write_bit(false);
        bs.write_bits(0b10110, 5);
        bs.write_byte(0xAB);
        bs.write_bits(0xDEAD_BEEF_CAFE_BABE, 64);
        bs.write_varint(-123456789);
        bs.write_uvarint(9876543210);

        let mut br = BitReader::new(bs.bytes());
        assert!(br.read_bit().unwrap());
        assert!(!br.read_bit().unwrap());
        assert_eq!(br.read_bits(5).unwrap(), 0b10110);
        assert_eq!(br.read_byte().unwrap(), 0xAB);
        assert_eq!(br.read_bits(64).unwrap(), 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(br.read_varint().unwrap(), -123456789);
        assert_eq!(br.read_uvarint().unwrap(), 9876543210);
    }
}
