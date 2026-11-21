//! Bit-level stream writer and reader matching Prometheus `bstream.go` bit-for-bit.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/bstream.go.

use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

use super::ChunkError;

#[inline]
const fn next_bitstream_capacity(cur_cap: usize) -> usize {
    if cur_cap < 32 {
        32
    } else if cur_cap < 64 {
        64
    } else if cur_cap < 96 {
        96
    } else if cur_cap < 128 {
        128
    } else if cur_cap < 160 {
        160
    } else if cur_cap < 192 {
        192
    } else if cur_cap < 256 {
        256
    } else {
        // 1.5x growth above 256 bytes, rounded up to 32-byte alignment, capped at `u32::MAX`.
        let next = cur_cap.saturating_add(cur_cap >> 1);
        let aligned = next.saturating_add(31) & !31;
        if aligned > u32::MAX as usize {
            u32::MAX as usize
        } else {
            aligned
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum BitCount {
    C0 = 0,
    C1 = 1,
    C2 = 2,
    C3 = 3,
    C4 = 4,
    C5 = 5,
    C6 = 6,
    C7 = 7,
    C8 = 8,
}

impl BitCount {
    #[inline(always)]
    const fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::C0,
            1 => Self::C1,
            2 => Self::C2,
            3 => Self::C3,
            4 => Self::C4,
            5 => Self::C5,
            6 => Self::C6,
            7 => Self::C7,
            _ => Self::C8,
        }
    }

    #[inline(always)]
    const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Bit-level writer matching `chunkenc.bstream` in Prometheus TSDB.
#[derive(Debug)]
pub struct BitStream {
    ptr: NonNull<u8>,
    len: u32,
    cap: u32,
    /// Number of unused right-most bits remaining in the last byte of `stream` (`0..=8`).
    count: BitCount,
    _marker: PhantomData<Vec<u8>>,
}

// SAFETY: `BitStream` uniquely owns its heap buffer just like `Vec<u8>`.
unsafe impl Send for BitStream {}
// SAFETY: `BitStream` uniquely owns its heap buffer just like `Vec<u8>`.
unsafe impl Sync for BitStream {}

struct VecGuard<'a> {
    bs: &'a mut BitStream,
    vec: ManuallyDrop<Vec<u8>>,
}

impl Drop for VecGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `Vec::as_mut_ptr` is always non-null (dangling when capacity == 0).
        self.bs.ptr = unsafe { NonNull::new_unchecked(self.vec.as_mut_ptr()) };
        let Ok(len) = u32::try_from(self.vec.len()) else {
            std::process::abort();
        };
        let Ok(cap) = u32::try_from(self.vec.capacity()) else {
            std::process::abort();
        };
        self.bs.len = len;
        self.bs.cap = cap;
    }
}

impl Drop for BitStream {
    #[inline]
    fn drop(&mut self) {
        if self.cap > 0 {
            // SAFETY: `ptr`, `len`, `cap` were created from a `Vec<u8>` allocation.
            unsafe {
                drop(Vec::from_raw_parts(
                    self.ptr.as_ptr(),
                    self.len as usize,
                    self.cap as usize,
                ));
            }
        }
    }
}

impl Clone for BitStream {
    #[inline]
    fn clone(&self) -> Self {
        if self.len == 0 {
            Self {
                ptr: NonNull::dangling(),
                len: 0,
                cap: 0,
                count: self.count,
                _marker: PhantomData,
            }
        } else {
            Self::from_bytes(self.bytes().to_vec(), self.count.as_u8())
        }
    }
}

impl PartialEq for BitStream {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.bytes() == other.bytes()
    }
}

impl Eq for BitStream {}

impl BitStream {
    #[inline]
    pub const fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            cap: 0,
            count: BitCount::C0,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn with_header(header_len: usize, capacity: usize) -> Self {
        let mut stream = Vec::with_capacity(capacity.max(header_len));
        stream.resize(header_len, 0);
        Self::from_bytes(stream, 0)
    }

    #[inline]
    pub fn from_bytes(mut stream: Vec<u8>, count: u8) -> Self {
        if stream.capacity() > u32::MAX as usize && stream.len() <= u32::MAX as usize {
            stream.shrink_to_fit();
        }
        assert!(
            stream.len() <= u32::MAX as usize && stream.capacity() <= u32::MAX as usize,
            "BitStream length/capacity ({}/{}) exceeds u32::MAX",
            stream.len(),
            stream.capacity()
        );
        let mut v = ManuallyDrop::new(stream);
        let len = v.len() as u32;
        let cap = v.capacity() as u32;
        // SAFETY: `Vec::as_mut_ptr` is always non-null (dangling when capacity == 0).
        let ptr = unsafe { NonNull::new_unchecked(v.as_mut_ptr()) };
        Self {
            ptr,
            len,
            cap,
            count: BitCount::from_u8(count),
            _marker: PhantomData,
        }
    }

    /// Truncates the stream to `new_len` bytes and sets the trailing free bit count (`0..=7`),
    /// masking off any garbage bits in the unused low bits of the final byte.
    #[inline]
    pub(crate) fn truncate_and_set_count(&mut self, new_len: usize, free_bits: u8) {
        debug_assert!(free_bits < 8, "BitStream::truncate_and_set_count called with free_bits={free_bits}");
        let clamped_len = new_len.min(self.len as usize);
        self.len = clamped_len as u32;
        let free = free_bits.min(7);
        if free > 0 {
            if let Some(last) = self.bytes_mut().last_mut() {
                *last &= !((1u8 << free) - 1);
            }
        }
        self.count = BitCount::from_u8(free);
    }

    #[inline]
    fn with_vec<R>(&mut self, f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
        let vec = if self.cap == 0 {
            ManuallyDrop::new(Vec::new())
        } else {
            // SAFETY: `ptr`, `len`, `cap` belong to a valid `Vec<u8>` allocation.
            ManuallyDrop::new(unsafe {
                Vec::from_raw_parts(self.ptr.as_ptr(), self.len as usize, self.cap as usize)
            })
        };
        let mut guard = VecGuard { bs: self, vec };
        f(&mut guard.vec)
    }

    #[inline(always)]
    fn push_byte(&mut self, byte: u8) {
        if self.len < self.cap {
            // SAFETY: `self.len < self.cap` guarantees the write is within allocated capacity.
            unsafe {
                *self.ptr.as_ptr().add(self.len as usize) = byte;
            }
            self.len += 1;
        } else {
            self.grow_and_push(byte);
        }
    }

    #[cold]
    #[inline(never)]
    fn grow_and_push(&mut self, byte: u8) {
        assert!(self.len < u32::MAX, "BitStream length exceeds u32::MAX");
        let target_cap = next_bitstream_capacity(self.cap as usize);
        let additional = target_cap.saturating_sub(self.len as usize).max(1);
        self.with_vec(|v| {
            v.reserve_exact(additional);
            assert!(
                v.capacity() <= u32::MAX as usize,
                "BitStream capacity {} exceeds u32::MAX",
                v.capacity()
            );
            v.push(byte);
        });
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `len` initialized bytes (or dangling with `len == 0`).
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize) }
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is uniquely owned and valid for `len` initialized bytes.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len as usize) }
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        let me = ManuallyDrop::new(self);
        if me.cap == 0 {
            Vec::new()
        } else {
            // SAFETY: `ptr`, `len`, `cap` belong to a valid `Vec<u8>` allocation.
            unsafe { Vec::from_raw_parts(me.ptr.as_ptr(), me.len as usize, me.cap as usize) }
        }
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.cap as usize
    }

    #[inline]
    pub fn compact(&mut self, threshold: usize) {
        if (self.cap as usize) > (self.len as usize) + threshold {
            self.with_vec(|v| v.shrink_to_fit());
        }
    }

    /// Appends a single bit to the stream.
    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if self.count == BitCount::C0 {
            self.push_byte(0);
            self.count = BitCount::C8;
        }
        let c = self.count.as_u8();
        if bit {
            let mask = 1u8 << (c - 1);
            if let Some(last) = self.bytes_mut().last_mut() {
                *last |= mask;
            }
        }
        self.count = BitCount::from_u8(c - 1);
    }

    /// Appends a full byte to the stream (aligned or unaligned).
    #[inline]
    pub fn write_byte(&mut self, byt: u8) {
        let c = self.count.as_u8();
        if c == 0 {
            self.push_byte(byt);
            return;
        }
        if c >= 8 {
            debug_assert!(false, "BitStream::write_byte called with invalid count={c}");
            self.count = BitCount::C0;
            self.push_byte(byt);
            return;
        }
        let shift = 8 - c;
        if let Some(last) = self.bytes_mut().last_mut() {
            *last |= byt >> shift;
        }
        self.push_byte(byt << c);
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

        let c = self.count.as_u8();
        if c > 0 {
            let free = c as usize;
            let hi = (u >> (64 - free)) as u8;
            if let Some(last) = self.bytes_mut().last_mut() {
                *last |= hi;
            }
            if rem < free {
                self.count = BitCount::from_u8((free - rem) as u8);
                return;
            }
            u <<= free;
            rem -= free;
            self.count = BitCount::C0;
        }

        while rem >= 8 {
            self.push_byte((u >> 56) as u8);
            u <<= 8;
            rem -= 8;
        }

        if rem > 0 {
            self.push_byte((u >> 56) as u8);
            self.count = BitCount::from_u8((8 - rem) as u8);
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

    /// Returns `(consumed_bytes, free_bits)` where `consumed_bytes` is the exact byte prefix
    /// of `stream` containing all bits read so far and `free_bits` is in `0..=7`.
    #[inline]
    pub fn consumed_bytes_and_free_bits(&self) -> (usize, u8) {
        let unread_bytes = (self.valid as usize) / 8;
        let free_bits = self.valid % 8;
        (self.stream_offset.saturating_sub(unread_bytes), free_bits)
    }

    #[inline]
    pub fn remaining_bits(&self) -> usize {
        self.stream
            .len()
            .saturating_sub(self.stream_offset)
            .saturating_mul(8)
            .saturating_add(self.valid as usize)
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

        if !self.load_next_buffer(rem) || self.valid < rem {
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

    #[test]
    fn bitstream_size_and_compact_growth_steps() {
        assert_eq!(std::mem::size_of::<BitStream>(), 24);
        let mut bs = BitStream::with_header(2, 32);
        let mut caps = vec![bs.capacity()];
        for i in 0..400usize {
            bs.write_byte((i & 0xff) as u8);
            let c = bs.capacity();
            if *caps.last().unwrap() != c {
                caps.push(c);
            }
        }
        // Verify compact steps avoid jumping straight from 128 -> 256 -> 512.
        assert!(caps.contains(&32), "caps={caps:?}");
        assert!(caps.contains(&64), "caps={caps:?}");
        assert!(caps.contains(&96), "caps={caps:?}");
        assert!(caps.contains(&128), "caps={caps:?}");
        assert!(caps.contains(&160), "caps={caps:?}");
        assert!(caps.contains(&192), "caps={caps:?}");
        assert!(caps.contains(&256), "caps={caps:?}");
        assert!(caps.contains(&384), "caps={caps:?}");
        assert!(!caps.contains(&512), "caps={caps:?}");
    }

    #[test]
    fn next_bitstream_capacity_clamps_to_u32_max() {
        assert_eq!(next_bitstream_capacity(2_863_311_510), u32::MAX as usize);
        assert_eq!(next_bitstream_capacity(u32::MAX as usize), u32::MAX as usize);
        assert!(next_bitstream_capacity(2_000_000_000) <= u32::MAX as usize);
    }

    #[test]
    fn read_bits_partial_tail_buffer_returns_eof_without_underflow() {
        // 3 bytes total: read 16 bits leaving 8 valid bits, then request 64 bits with only
        // 1 byte (8 bits) remaining in stream -> must return UnexpectedEof without underflow.
        let mut br = BitReader::new(&[0xaa, 0xbb, 0xcc]);
        assert_eq!(br.read_bits(16).unwrap(), 0xaabb);
        // Only 8 bits remain in the stream while valid == 0 or valid > 0:
        let mut br2 = BitReader::new(&[0xaa, 0xbb, 0xcc]);
        assert_eq!(br2.read_bits(8).unwrap(), 0xaa);
        assert!(matches!(br2.read_bits(64), Err(ChunkError::UnexpectedEof)));
    }
}
