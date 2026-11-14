//! Variable-bit integer encoding used by XOR and Histogram chunks.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/varbit.go.

use super::bstream::{BitReader, BitStream};
use super::ChunkError;

/// Returns whether `x` fits into `nbits` using Prometheus's asymmetric signed range:
/// `[-((1 << (nbits - 1)) - 1), 1 << (nbits - 1)]`.
#[inline]
pub(crate) fn bit_range(x: i64, nbits: u8) -> bool {
    debug_assert!((2..=63).contains(&nbits));
    let half = 1i64 << (nbits - 1);
    -(half - 1) <= x && x <= half
}

#[inline]
fn bit_range_uint(x: u64, nbits: u32) -> bool {
    x.leading_zeros() >= 64 - nbits
}

/// Writes a signed `i64` using Prometheus varbit encoding.
pub fn put_varbit_int(b: &mut BitStream, val: i64) {
    let uval = val as u64;
    if val == 0 {
        b.write_bit(false);
    } else if bit_range(val, 3) {
        b.write_bits(0b10, 2);
        b.write_bits(uval, 3);
    } else if bit_range(val, 6) {
        b.write_bits(0b110, 3);
        b.write_bits(uval, 6);
    } else if bit_range(val, 9) {
        b.write_bits(0b1110, 4);
        b.write_bits(uval, 9);
    } else if bit_range(val, 12) {
        b.write_bits(0b11110, 5);
        b.write_bits(uval, 12);
    } else if bit_range(val, 18) {
        b.write_bits(0b111110, 6);
        b.write_bits(uval, 18);
    } else if bit_range(val, 25) {
        b.write_bits(0b1111110, 7);
        b.write_bits(uval, 25);
    } else if bit_range(val, 56) {
        b.write_bits(0b11111110, 8);
        b.write_bits(uval, 56);
    } else {
        b.write_bits(0b11111111, 8);
        b.write_bits(uval, 64);
    }
}

/// Reads a signed `i64` encoded with [`put_varbit_int`].
pub fn read_varbit_int(b: &mut BitReader<'_>) -> Result<i64, ChunkError> {
    let mut d: u8 = 0;
    for _ in 0..8 {
        d <<= 1;
        let bit = b.read_bit()?;
        if !bit {
            break;
        }
        d |= 1;
    }

    let sz: u8 = match d {
        0b0 => return Ok(0),
        0b10 => 3,
        0b110 => 6,
        0b1110 => 9,
        0b11110 => 12,
        0b111110 => 18,
        0b1111110 => 25,
        0b11111110 => 56,
        0b11111111 => {
            let bits = b.read_bits(64)?;
            return Ok(bits as i64);
        }
        other => return Err(ChunkError::InvalidVarbitPattern(other)),
    };

    let mut bits = b.read_bits(sz)?;
    if bits > (1u64 << (sz - 1)) {
        bits = bits.wrapping_sub(1u64 << sz);
    }
    Ok(bits as i64)
}

/// Writes an unsigned `u64` using Prometheus varbit encoding.
pub fn put_varbit_uint(b: &mut BitStream, val: u64) {
    if val == 0 {
        b.write_bit(false);
    } else if bit_range_uint(val, 3) {
        b.write_bits(0b10, 2);
        b.write_bits(val, 3);
    } else if bit_range_uint(val, 6) {
        b.write_bits(0b110, 3);
        b.write_bits(val, 6);
    } else if bit_range_uint(val, 9) {
        b.write_bits(0b1110, 4);
        b.write_bits(val, 9);
    } else if bit_range_uint(val, 12) {
        b.write_bits(0b11110, 5);
        b.write_bits(val, 12);
    } else if bit_range_uint(val, 18) {
        b.write_bits(0b111110, 6);
        b.write_bits(val, 18);
    } else if bit_range_uint(val, 25) {
        b.write_bits(0b1111110, 7);
        b.write_bits(val, 25);
    } else if bit_range_uint(val, 56) {
        b.write_bits(0b11111110, 8);
        b.write_bits(val, 56);
    } else {
        b.write_bits(0b11111111, 8);
        b.write_bits(val, 64);
    }
}

/// Reads an unsigned `u64` encoded with [`put_varbit_uint`].
pub fn read_varbit_uint(b: &mut BitReader<'_>) -> Result<u64, ChunkError> {
    let mut d: u8 = 0;
    for _ in 0..8 {
        d <<= 1;
        let bit = b.read_bit()?;
        if !bit {
            break;
        }
        d |= 1;
    }

    let sz: u8 = match d {
        0b0 => return Ok(0),
        0b10 => 3,
        0b110 => 6,
        0b1110 => 9,
        0b11110 => 12,
        0b111110 => 18,
        0b1111110 => 25,
        0b11111110 => 56,
        0b11111111 => return b.read_bits(64),
        other => return Err(ChunkError::InvalidVarbitPattern(other)),
    };

    b.read_bits(sz)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varbit_roundtrip_boundaries() {
        let signed_cases = [
            0,
            1,
            -1,
            4,
            -3,
            -4,
            32,
            -31,
            -32,
            256,
            -255,
            2048,
            -2047,
            131072,
            -131071,
            16777216,
            -16777215,
            36028797018963968,
            -36028797018963967,
            i64::MAX,
            i64::MIN,
        ];
        for &v in &signed_cases {
            let mut bs = BitStream::new();
            put_varbit_int(&mut bs, v);
            let mut br = BitReader::new(bs.bytes());
            assert_eq!(read_varbit_int(&mut br).unwrap(), v, "failed for {v}");
        }

        let unsigned_cases = [
            0,
            7,
            8,
            63,
            511,
            4095,
            262143,
            33554431,
            72057594037927935,
            u64::MAX,
        ];
        for &v in &unsigned_cases {
            let mut bs = BitStream::new();
            put_varbit_uint(&mut bs, v);
            let mut br = BitReader::new(bs.bytes());
            assert_eq!(read_varbit_uint(&mut br).unwrap(), v, "failed for {v}");
        }
    }
}
