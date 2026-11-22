//! Ported from github.com/klauspost/compress/s2/{s2,encode,encode_go,encode_all,encode_better,encode_best,decode,decode_other}.go.

use super::dict::Dict;
use super::{S2Error, MAX_DICT_SRC_OFFSET, TAG_COPY1, TAG_COPY2, TAG_COPY4, TAG_LITERAL};

pub const INPUT_MARGIN: usize = 8;
pub const MIN_NON_LITERAL_BLOCK_SIZE: usize = 32;

/// Maximum block length accepted by `max_encoded_len` on 64-bit platforms.
pub const MAX_BLOCK_SIZE_LIMIT: usize = (u32::MAX as usize) - 5 - 5;

#[inline]
pub(crate) fn load32(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
}

#[inline]
pub(crate) fn load64(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes([
        b[i],
        b[i + 1],
        b[i + 2],
        b[i + 3],
        b[i + 4],
        b[i + 5],
        b[i + 6],
        b[i + 7],
    ])
}

#[inline]
pub(crate) fn hash4(u: u64, h: u8) -> usize {
    const PRIME4: u32 = 2_654_435_761;
    ((u as u32).wrapping_mul(PRIME4) >> ((32 - h) & 31)) as usize
}

#[inline]
pub(crate) fn hash6(u: u64, h: u8) -> usize {
    const PRIME6: u64 = 227_718_039_650_203;
    (((u << 16).wrapping_mul(PRIME6)) >> ((64 - h) & 63)) as usize
}

#[inline]
pub(crate) fn hash7(u: u64, h: u8) -> usize {
    const PRIME7: u64 = 58_295_818_150_454_627;
    (((u << 8).wrapping_mul(PRIME7)) >> ((64 - h) & 63)) as usize
}

#[inline]
pub(crate) fn hash8(u: u64, h: u8) -> usize {
    const PRIME8: u64 = 0xcf1b_bcdc_b7a5_6463;
    ((u.wrapping_mul(PRIME8)) >> ((64 - h) & 63)) as usize
}

#[inline]
pub fn literal_extra_size(n: i64) -> i64 {
    if n == 0 {
        return 0;
    }
    if n < 60 {
        1
    } else if n < (1 << 8) {
        2
    } else if n < (1 << 16) {
        3
    } else if n < (1 << 24) {
        4
    } else {
        5
    }
}

/// Returns the maximum length of an S2/Snappy block given `src_len`, or `-1` if too large / negative.
pub fn max_encoded_len(src_len: i64) -> i64 {
    if src_len < 0 {
        return -1;
    }
    let n = src_len as u64;
    if cfg!(target_pointer_width = "32") {
        if n > i32::MAX as u64 {
            return -1;
        }
    } else if n > u32::MAX as u64 {
        return -1;
    }
    let varint_bytes = ((64 - n.leading_zeros() as u64) + 7) / 7;
    let total = n + varint_bytes + literal_extra_size(src_len) as u64;
    if cfg!(target_pointer_width = "32") {
        if total > i32::MAX as u64 {
            return -1;
        }
    } else if total > u32::MAX as u64 {
        return -1;
    }
    total as i64
}

#[inline]
pub fn put_uvarint(dst: &mut [u8], mut v: u64) -> usize {
    let mut i = 0;
    while v >= 0x80 {
        dst[i] = (v as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    dst[i] = v as u8;
    i + 1
}

#[inline]
pub fn decode_block_uvarint(src: &[u8]) -> Result<(usize, usize), S2Error> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in src.iter().take(5).enumerate() {
        if i == 4 && b > 0x0f {
            return Err(S2Error::Corrupt);
        }
        v |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            if v > u32::MAX as u64 {
                return Err(S2Error::Corrupt);
            }
            if cfg!(target_pointer_width = "32") && v > i32::MAX as u64 {
                return Err(S2Error::TooLarge);
            }
            return Ok((v as usize, i + 1));
        }
        shift += 7;
    }
    Err(S2Error::Corrupt)
}

/// Returns the decoded length of a block from its varint header.
pub fn decoded_len(src: &[u8]) -> Result<usize, S2Error> {
    decode_block_uvarint(src).map(|(len, _)| len)
}

/// Emits a literal chunk into `dst` and returns bytes written.
pub fn emit_literal(dst: &mut [u8], lit: &[u8]) -> usize {
    if lit.is_empty() {
        return 0;
    }
    let n = lit.len() - 1;
    let hdr_len = if n < 60 {
        dst[0] = ((n as u8) << 2) | TAG_LITERAL;
        1
    } else if n < (1 << 8) {
        dst[0] = (60 << 2) | TAG_LITERAL;
        dst[1] = n as u8;
        2
    } else if n < (1 << 16) {
        dst[0] = (61 << 2) | TAG_LITERAL;
        dst[1] = n as u8;
        dst[2] = (n >> 8) as u8;
        3
    } else if n < (1 << 24) {
        dst[0] = (62 << 2) | TAG_LITERAL;
        dst[1] = n as u8;
        dst[2] = (n >> 8) as u8;
        dst[3] = (n >> 16) as u8;
        4
    } else {
        dst[0] = (63 << 2) | TAG_LITERAL;
        dst[1] = n as u8;
        dst[2] = (n >> 8) as u8;
        dst[3] = (n >> 16) as u8;
        dst[4] = (n >> 24) as u8;
        5
    };
    dst[hdr_len..hdr_len + lit.len()].copy_from_slice(lit);
    hdr_len + lit.len()
}

pub fn emit_literal_size(lit: &[u8]) -> usize {
    if lit.is_empty() {
        return 0;
    }
    let len = lit.len();
    if len <= 60 {
        len + 1
    } else if len <= (1 << 8) {
        len + 2
    } else if len <= (1 << 16) {
        len + 3
    } else if len <= (1 << 24) {
        len + 4
    } else {
        len + 5
    }
}

/// Emits a repeat copy chunk into `dst`. `length` must be >= 4.
pub fn emit_repeat(dst: &mut [u8], offset: usize, mut length: usize) -> usize {
    length -= 4;
    if length <= 4 {
        dst[0] = ((length as u8) << 2) | TAG_COPY1;
        dst[1] = 0;
        return 2;
    }
    if length < 8 && offset < 2048 {
        dst[1] = offset as u8;
        dst[0] = (((offset >> 8) as u8) << 5) | ((length as u8) << 2) | TAG_COPY1;
        return 2;
    }
    if length < (1 << 8) + 4 {
        length -= 4;
        dst[2] = length as u8;
        dst[1] = 0;
        dst[0] = (5 << 2) | TAG_COPY1;
        return 3;
    }
    if length < (1 << 16) + (1 << 8) {
        length -= 1 << 8;
        dst[3] = (length >> 8) as u8;
        dst[2] = length as u8;
        dst[1] = 0;
        dst[0] = (6 << 2) | TAG_COPY1;
        return 4;
    }
    const MAX_REPEAT: usize = (1 << 24) - 1;
    length -= 1 << 16;
    let mut left = 0usize;
    if length > MAX_REPEAT {
        left = length - MAX_REPEAT + 4;
        length = MAX_REPEAT - 4;
    }
    dst[4] = (length >> 16) as u8;
    dst[3] = (length >> 8) as u8;
    dst[2] = length as u8;
    dst[1] = 0;
    dst[0] = (7 << 2) | TAG_COPY1;
    if left > 0 {
        5 + emit_repeat(&mut dst[5..], offset, left)
    } else {
        5
    }
}

/// Emits a copy chunk into `dst`. `1 <= offset`, `4 <= length`.
pub fn emit_copy(dst: &mut [u8], offset: usize, mut length: usize) -> usize {
    if offset >= 65536 {
        let mut i = 0;
        if length > 64 {
            dst[4] = (offset >> 24) as u8;
            dst[3] = (offset >> 16) as u8;
            dst[2] = (offset >> 8) as u8;
            dst[1] = offset as u8;
            dst[0] = (63 << 2) | TAG_COPY4;
            length -= 64;
            if length >= 4 {
                return 5 + emit_repeat(&mut dst[5..], offset, length);
            }
            i = 5;
        }
        if length == 0 {
            return i;
        }
        dst[i] = (((length - 1) as u8) << 2) | TAG_COPY4;
        dst[i + 1] = offset as u8;
        dst[i + 2] = (offset >> 8) as u8;
        dst[i + 3] = (offset >> 16) as u8;
        dst[i + 4] = (offset >> 24) as u8;
        return i + 5;
    }

    if length > 64 {
        let off = if offset < 2048 {
            dst[1] = offset as u8;
            dst[0] = (((offset >> 8) as u8) << 5) | (((8 - 4) as u8) << 2) | TAG_COPY1;
            length -= 8;
            2
        } else {
            dst[2] = (offset >> 8) as u8;
            dst[1] = offset as u8;
            dst[0] = (59 << 2) | TAG_COPY2;
            length -= 60;
            3
        };
        return off + emit_repeat(&mut dst[off..], offset, length);
    }
    if length >= 12 || offset >= 2048 {
        dst[2] = (offset >> 8) as u8;
        dst[1] = offset as u8;
        dst[0] = (((length - 1) as u8) << 2) | TAG_COPY2;
        return 3;
    }
    dst[1] = offset as u8;
    dst[0] = (((offset >> 8) as u8) << 5) | (((length - 4) as u8) << 2) | TAG_COPY1;
    2
}

/// Emits a Snappy-compatible copy chunk (no S2 repeats).
pub fn emit_copy_no_repeat(dst: &mut [u8], offset: usize, mut length: usize) -> usize {
    let mut written = 0;
    loop {
        if offset >= 65536 {
            if length > 64 {
                dst[written + 4] = (offset >> 24) as u8;
                dst[written + 3] = (offset >> 16) as u8;
                dst[written + 2] = (offset >> 8) as u8;
                dst[written + 1] = offset as u8;
                dst[written] = (63 << 2) | TAG_COPY4;
                length -= 64;
                written += 5;
                if length >= 4 {
                    continue;
                }
            }
            if length == 0 {
                return written;
            }
            dst[written] = (((length - 1) as u8) << 2) | TAG_COPY4;
            dst[written + 1] = offset as u8;
            dst[written + 2] = (offset >> 8) as u8;
            dst[written + 3] = (offset >> 16) as u8;
            dst[written + 4] = (offset >> 24) as u8;
            return written + 5;
        }
        if length > 64 {
            dst[written + 2] = (offset >> 8) as u8;
            dst[written + 1] = offset as u8;
            dst[written] = (59 << 2) | TAG_COPY2;
            length -= 60;
            written += 3;
            continue;
        }
        if length >= 12 || offset >= 2048 {
            dst[written + 2] = (offset >> 8) as u8;
            dst[written + 1] = offset as u8;
            dst[written] = (((length - 1) as u8) << 2) | TAG_COPY2;
            return written + 3;
        }
        dst[written + 1] = offset as u8;
        dst[written] = (((offset >> 8) as u8) << 5) | (((length - 4) as u8) << 2) | TAG_COPY1;
        return written + 2;
    }
}

pub fn emit_repeat_size(offset: usize, mut length: usize) -> usize {
    let mut total = 0;
    loop {
        length -= 4;
        if length <= 4 || (length < 8 && offset < 2048) {
            return total + 2;
        }
        if length < (1 << 8) + 4 {
            return total + 3;
        }
        if length < (1 << 16) + (1 << 8) {
            return total + 4;
        }
        const MAX_REPEAT: usize = (1 << 24) - 1;
        length -= 1 << 16;
        if length > MAX_REPEAT {
            length = length - MAX_REPEAT + 4;
            total += 5;
        } else {
            return total + 5;
        }
    }
}

pub fn emit_copy_size(offset: usize, mut length: usize) -> usize {
    if offset >= 65536 {
        let mut i = 0;
        if length > 64 {
            length -= 64;
            if length >= 4 {
                return 5 + emit_repeat_size(offset, length);
            }
            i = 5;
        }
        if length == 0 {
            return i;
        }
        return i + 5;
    }
    if length > 64 {
        let off = if offset < 2048 {
            length -= 8;
            2
        } else {
            length -= 60;
            3
        };
        return off + emit_repeat_size(offset, length);
    }
    if length >= 12 || offset >= 2048 {
        3
    } else {
        2
    }
}

pub fn emit_copy_no_repeat_size(offset: usize, mut length: usize) -> usize {
    let mut total = 0;
    loop {
        if offset >= 65536 {
            if length > 64 {
                length -= 64;
                total += 5;
                if length >= 4 {
                    continue;
                }
            }
            return if length == 0 { total } else { total + 5 };
        }
        if length > 64 {
            length -= 60;
            total += 3;
            continue;
        }
        if length >= 12 || offset >= 2048 {
            return total + 3;
        }
        return total + 2;
    }
}

/// Returns how many prefix bytes match between `a` and `b` (assumes `a.len() <= b.len()`).
pub fn match_len(mut a: &[u8], mut b: &[u8]) -> usize {
    b = &b[..a.len()];
    let mut checked = 0usize;
    if a.len() > 4 {
        let diff = load32(a, 0) ^ load32(b, 0);
        if diff != 0 {
            return (diff.trailing_zeros() >> 3) as usize;
        }
        checked = 4;
        a = &a[4..];
        b = &b[4..];
        while a.len() >= 8 {
            let diff = load64(a, 0) ^ load64(b, 0);
            if diff != 0 {
                return checked + ((diff.trailing_zeros() >> 3) as usize);
            }
            checked += 8;
            a = &a[8..];
            b = &b[8..];
        }
    }
    for (i, (&x, &y)) in a.iter().zip(b.iter()).enumerate() {
        if x != y {
            return checked + i;
        }
    }
    checked + a.len()
}

/// Decodes an S2/Snappy block body (without varint header) into `dst`.
pub fn s2_decode(dst: &mut [u8], src: &[u8]) -> Result<(), S2Error> {
    s2_decode_dict(dst, src, None)
}

/// Decodes an S2/Snappy block body (with optional `dict`) into `dst`.
pub fn s2_decode_dict(dst: &mut [u8], src: &[u8], dict: Option<&Dict>) -> Result<(), S2Error> {
    let mut d = 0usize;
    let mut s = 0usize;
    let mut offset = match dict {
        Some(dk) => dk.dict.len() - dk.repeat,
        None => 0,
    };

    while s < src.len() {
        let tag_byte = src[s];
        match tag_byte & 0x03 {
            0x00 => {
                let mut x = (tag_byte >> 2) as usize;
                match x {
                    0..=59 => {
                        s += 1;
                    }
                    60 => {
                        let b = *src.get(s + 1).ok_or(S2Error::Corrupt)?;
                        x = b as usize;
                        s += 2;
                    }
                    61 => {
                        let bytes = src.get(s + 1..s + 3).ok_or(S2Error::Corrupt)?;
                        x = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                        s += 3;
                    }
                    62 => {
                        let bytes = src.get(s + 1..s + 4).ok_or(S2Error::Corrupt)?;
                        x = (bytes[0] as usize)
                            | ((bytes[1] as usize) << 8)
                            | ((bytes[2] as usize) << 16);
                        s += 4;
                    }
                    _ => {
                        let bytes = src.get(s + 1..s + 5).ok_or(S2Error::Corrupt)?;
                        x = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
                        s += 5;
                    }
                }
                let length = x.checked_add(1).ok_or(S2Error::Corrupt)?;
                if length > dst.len() - d || length > src.len() - s {
                    return Err(S2Error::Corrupt);
                }
                dst[d..d + length].copy_from_slice(&src[s..s + length]);
                d += length;
                s += length;
                continue;
            }
            0x01 => {
                let b1 = *src.get(s + 1).ok_or(S2Error::Corrupt)?;
                s += 2;
                let mut length = ((tag_byte >> 2) & 0x07) as usize;
                let toffset = (((tag_byte as usize) & 0xe0) << 3) | (b1 as usize);
                if toffset == 0 {
                    match length {
                        5 => {
                            let b = *src.get(s).ok_or(S2Error::Corrupt)?;
                            length = (b as usize) + 4;
                            s += 1;
                        }
                        6 => {
                            let bytes = src.get(s..s + 2).ok_or(S2Error::Corrupt)?;
                            length = u16::from_le_bytes([bytes[0], bytes[1]]) as usize + (1 << 8);
                            s += 2;
                        }
                        7 => {
                            let bytes = src.get(s..s + 3).ok_or(S2Error::Corrupt)?;
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
                if offset == 0 || length > dst.len() - d {
                    return Err(S2Error::Corrupt);
                }
                if d < offset {
                    let dk = dict.ok_or(S2Error::Corrupt)?;
                    if d > MAX_DICT_SRC_OFFSET {
                        return Err(S2Error::Corrupt);
                    }
                    let back = offset - d;
                    if back > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    let start_off = dk.dict.len() - back;
                    if start_off + length > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    dst[d..d + length].copy_from_slice(&dk.dict[start_off..start_off + length]);
                    d += length;
                    continue;
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
                let bytes = src.get(s + 1..s + 3).ok_or(S2Error::Corrupt)?;
                let length = 1 + (tag_byte >> 2) as usize;
                offset = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                s += 3;
                if offset == 0 || length > dst.len() - d {
                    return Err(S2Error::Corrupt);
                }
                if d < offset {
                    let dk = dict.ok_or(S2Error::Corrupt)?;
                    if d > MAX_DICT_SRC_OFFSET {
                        return Err(S2Error::Corrupt);
                    }
                    let back = offset - d;
                    if back > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    let start_off = dk.dict.len() - back;
                    if start_off + length > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    dst[d..d + length].copy_from_slice(&dk.dict[start_off..start_off + length]);
                    d += length;
                    continue;
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
                let bytes = src.get(s + 1..s + 5).ok_or(S2Error::Corrupt)?;
                let length = 1 + (tag_byte >> 2) as usize;
                offset = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
                s += 5;
                if offset == 0 || length > dst.len() - d {
                    return Err(S2Error::Corrupt);
                }
                if d < offset {
                    let dk = dict.ok_or(S2Error::Corrupt)?;
                    if d > MAX_DICT_SRC_OFFSET {
                        return Err(S2Error::Corrupt);
                    }
                    let back = offset - d;
                    if back > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    let start_off = dk.dict.len() - back;
                    if start_off + length > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    dst[d..d + length].copy_from_slice(&dk.dict[start_off..start_off + length]);
                    d += length;
                    continue;
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
        return Err(S2Error::Corrupt);
    }
    Ok(())
}

/// Default maximum decoded S2 block length allowed by [`decode`] and [`Dict::decode`] (64 MiB).
pub const DEFAULT_MAX_BLOCK_DECODE_LEN: usize = 64 << 20;

/// Incrementally decodes an S2/Snappy block body (without varint header) into a `Vec<u8>`,
/// avoiding upfront `vec![0u8; d_len]` allocation on untrusted varint headers.
pub(crate) fn s2_decode_dict_vec(
    d_len: usize,
    src: &[u8],
    dict: Option<&Dict>,
    max_len: usize,
) -> Result<Vec<u8>, S2Error> {
    if d_len > max_len {
        return Err(S2Error::TooLarge);
    }
    if d_len > 0 && src.is_empty() {
        return Err(S2Error::Corrupt);
    }
    let initial_cap = d_len.min(src.len().saturating_mul(8)).min(64 * 1024);
    let mut dst = Vec::with_capacity(initial_cap);
    let mut s = 0usize;
    let mut offset = match dict {
        Some(dk) => dk.dict.len() - dk.repeat,
        None => 0,
    };

    while s < src.len() {
        let tag_byte = src[s];
        match tag_byte & 0x03 {
            0x00 => {
                let mut x = (tag_byte >> 2) as usize;
                match x {
                    0..=59 => {
                        s += 1;
                    }
                    60 => {
                        let b = *src.get(s + 1).ok_or(S2Error::Corrupt)?;
                        x = b as usize;
                        s += 2;
                    }
                    61 => {
                        let bytes = src.get(s + 1..s + 3).ok_or(S2Error::Corrupt)?;
                        x = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                        s += 3;
                    }
                    62 => {
                        let bytes = src.get(s + 1..s + 4).ok_or(S2Error::Corrupt)?;
                        x = (bytes[0] as usize)
                            | ((bytes[1] as usize) << 8)
                            | ((bytes[2] as usize) << 16);
                        s += 4;
                    }
                    _ => {
                        let bytes = src.get(s + 1..s + 5).ok_or(S2Error::Corrupt)?;
                        x = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
                        s += 5;
                    }
                }
                let length = x.checked_add(1).ok_or(S2Error::Corrupt)?;
                if length > d_len - dst.len() || length > src.len() - s {
                    return Err(S2Error::Corrupt);
                }
                dst.extend_from_slice(&src[s..s + length]);
                s += length;
                continue;
            }
            0x01 => {
                let b1 = *src.get(s + 1).ok_or(S2Error::Corrupt)?;
                s += 2;
                let mut length = ((tag_byte >> 2) & 0x07) as usize;
                let toffset = (((tag_byte as usize) & 0xe0) << 3) | (b1 as usize);
                if toffset == 0 {
                    match length {
                        5 => {
                            let b = *src.get(s).ok_or(S2Error::Corrupt)?;
                            length = (b as usize) + 4;
                            s += 1;
                        }
                        6 => {
                            let bytes = src.get(s..s + 2).ok_or(S2Error::Corrupt)?;
                            length = u16::from_le_bytes([bytes[0], bytes[1]]) as usize + (1 << 8);
                            s += 2;
                        }
                        7 => {
                            let bytes = src.get(s..s + 3).ok_or(S2Error::Corrupt)?;
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
                if offset == 0 || length > d_len - dst.len() {
                    return Err(S2Error::Corrupt);
                }
                let d = dst.len();
                if d < offset {
                    let dk = dict.ok_or(S2Error::Corrupt)?;
                    if d > MAX_DICT_SRC_OFFSET {
                        return Err(S2Error::Corrupt);
                    }
                    let back = offset - d;
                    if back > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    let start_off = dk.dict.len() - back;
                    if start_off + length > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    dst.extend_from_slice(&dk.dict[start_off..start_off + length]);
                    continue;
                }
                if offset >= length {
                    dst.extend_from_within(d - offset..d - offset + length);
                } else {
                    dst.reserve(length);
                    let base = d - offset;
                    for i in 0..length {
                        let b = dst[base + i];
                        dst.push(b);
                    }
                }
            }
            0x02 => {
                let bytes = src.get(s + 1..s + 3).ok_or(S2Error::Corrupt)?;
                let length = 1 + (tag_byte >> 2) as usize;
                offset = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                s += 3;
                if offset == 0 || length > d_len - dst.len() {
                    return Err(S2Error::Corrupt);
                }
                let d = dst.len();
                if d < offset {
                    let dk = dict.ok_or(S2Error::Corrupt)?;
                    if d > MAX_DICT_SRC_OFFSET {
                        return Err(S2Error::Corrupt);
                    }
                    let back = offset - d;
                    if back > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    let start_off = dk.dict.len() - back;
                    if start_off + length > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    dst.extend_from_slice(&dk.dict[start_off..start_off + length]);
                    continue;
                }
                if offset >= length {
                    dst.extend_from_within(d - offset..d - offset + length);
                } else {
                    dst.reserve(length);
                    let base = d - offset;
                    for i in 0..length {
                        let b = dst[base + i];
                        dst.push(b);
                    }
                }
            }
            _ => {
                let bytes = src.get(s + 1..s + 5).ok_or(S2Error::Corrupt)?;
                let length = 1 + (tag_byte >> 2) as usize;
                offset = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
                s += 5;
                if offset == 0 || length > d_len - dst.len() {
                    return Err(S2Error::Corrupt);
                }
                let d = dst.len();
                if d < offset {
                    let dk = dict.ok_or(S2Error::Corrupt)?;
                    if d > MAX_DICT_SRC_OFFSET {
                        return Err(S2Error::Corrupt);
                    }
                    let back = offset - d;
                    if back > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    let start_off = dk.dict.len() - back;
                    if start_off + length > dk.dict.len() {
                        return Err(S2Error::Corrupt);
                    }
                    dst.extend_from_slice(&dk.dict[start_off..start_off + length]);
                    continue;
                }
                if offset >= length {
                    dst.extend_from_within(d - offset..d - offset + length);
                } else {
                    dst.reserve(length);
                    let base = d - offset;
                    for i in 0..length {
                        let b = dst[base + i];
                        dst.push(b);
                    }
                }
            }
        }
    }

    if dst.len() != d_len {
        return Err(S2Error::Corrupt);
    }
    Ok(dst)
}

/// Decodes a complete S2/Snappy block (with varint header) into a newly allocated `Vec<u8>`,
/// bounded by [`DEFAULT_MAX_BLOCK_DECODE_LEN`].
pub fn decode(src: &[u8]) -> Result<Vec<u8>, S2Error> {
    decode_bounded(src, DEFAULT_MAX_BLOCK_DECODE_LEN)
}

/// Decodes a complete S2/Snappy block (with varint header) into a newly allocated `Vec<u8>`,
/// returning [`S2Error::TooLarge`] if the declared length exceeds `max_len`.
pub fn decode_bounded(src: &[u8], max_len: usize) -> Result<Vec<u8>, S2Error> {
    let (d_len, hdr) = decode_block_uvarint(src)?;
    s2_decode_dict_vec(d_len, &src[hdr..], None, max_len)
}

/// Decodes a complete S2/Snappy block into a caller-provided slice `buf`, returning the decoded length.
pub fn decode_into_slice(buf: &mut [u8], src: &[u8]) -> Result<usize, S2Error> {
    let (d_len, hdr) = decode_block_uvarint(src)?;
    if d_len > buf.len() {
        return Err(S2Error::DstTooSmall);
    }
    s2_decode(&mut buf[..d_len], &src[hdr..])?;
    Ok(d_len)
}

/// Encodes `src` as an S2 block (with varint length header).
pub fn encode(src: &[u8]) -> Vec<u8> {
    let mel = max_encoded_len(src.len() as i64);
    if mel < 0 {
        panic!("s2: input too large");
    }
    let mut dst = vec![0u8; mel as usize];
    let mut d = put_uvarint(&mut dst, src.len() as u64);
    if src.is_empty() {
        dst.truncate(d);
        return dst;
    }
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        d += emit_literal(&mut dst[d..], src);
        dst.truncate(d);
        return dst;
    }
    let n = encode_block(&mut dst[d..], src);
    if n > 0 {
        d += n;
    } else {
        d += emit_literal(&mut dst[d..], src);
    }
    dst.truncate(d);
    dst
}

/// Encodes `src` with better compression as an S2 block.
pub fn encode_better(src: &[u8]) -> Vec<u8> {
    let mel = max_encoded_len(src.len() as i64);
    if mel < 0 {
        panic!("s2: input too large");
    }
    let mut dst = vec![0u8; mel as usize];
    let mut d = put_uvarint(&mut dst, src.len() as u64);
    if src.is_empty() {
        dst.truncate(d);
        return dst;
    }
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        d += emit_literal(&mut dst[d..], src);
        dst.truncate(d);
        return dst;
    }
    let n = encode_block_better(&mut dst[d..], src);
    if n > 0 {
        d += n;
    } else {
        d += emit_literal(&mut dst[d..], src);
    }
    dst.truncate(d);
    dst
}

/// Encodes `src` with best compression as an S2 block.
pub fn encode_best(src: &[u8]) -> Vec<u8> {
    let mel = max_encoded_len(src.len() as i64);
    if mel < 0 {
        panic!("s2: input too large");
    }
    let mut dst = vec![0u8; mel as usize];
    let mut d = put_uvarint(&mut dst, src.len() as u64);
    if src.is_empty() {
        dst.truncate(d);
        return dst;
    }
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        d += emit_literal(&mut dst[d..], src);
        dst.truncate(d);
        return dst;
    }
    let n = encode_block_best(&mut dst[d..], src, None);
    if n > 0 {
        d += n;
    } else {
        d += emit_literal(&mut dst[d..], src);
    }
    dst.truncate(d);
    dst
}

/// Encodes `src` as a Snappy-compatible block (with varint length header).
pub fn encode_snappy(src: &[u8]) -> Vec<u8> {
    let mel = max_encoded_len(src.len() as i64);
    if mel < 0 {
        panic!("s2: input too large");
    }
    let mut dst = vec![0u8; mel as usize];
    let mut d = put_uvarint(&mut dst, src.len() as u64);
    if src.is_empty() {
        dst.truncate(d);
        return dst;
    }
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        d += emit_literal(&mut dst[d..], src);
        dst.truncate(d);
        return dst;
    }
    let n = encode_block_snappy(&mut dst[d..], src);
    if n > 0 {
        d += n;
    } else {
        d += emit_literal(&mut dst[d..], src);
    }
    dst.truncate(d);
    dst
}

/// Encodes `src` with better compression as a Snappy-compatible block.
pub fn encode_snappy_better(src: &[u8]) -> Vec<u8> {
    let mel = max_encoded_len(src.len() as i64);
    if mel < 0 {
        panic!("s2: input too large");
    }
    let mut dst = vec![0u8; mel as usize];
    let mut d = put_uvarint(&mut dst, src.len() as u64);
    if src.is_empty() {
        dst.truncate(d);
        return dst;
    }
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        d += emit_literal(&mut dst[d..], src);
        dst.truncate(d);
        return dst;
    }
    let n = encode_block_better_snappy(&mut dst[d..], src);
    if n > 0 {
        d += n;
    } else {
        d += emit_literal(&mut dst[d..], src);
    }
    dst.truncate(d);
    dst
}

/// Encodes `src` with best compression as a Snappy-compatible block.
pub fn encode_snappy_best(src: &[u8]) -> Vec<u8> {
    encode_snappy_better(src)
}

pub fn encode_block(dst: &mut [u8], src: &[u8]) -> usize {
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        return 0;
    }
    if src.len() <= (64 << 10) {
        encode_block_go_core::<true>(dst, src, false)
    } else {
        encode_block_go_core::<false>(dst, src, false)
    }
}

pub fn encode_block_snappy(dst: &mut [u8], src: &[u8]) -> usize {
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        return 0;
    }
    if src.len() <= (64 << 10) {
        encode_block_go_core::<true>(dst, src, true)
    } else {
        encode_block_go_core::<false>(dst, src, true)
    }
}

fn encode_block_go_core<const SMALL: bool>(dst: &mut [u8], src: &[u8], snappy: bool) -> usize {
    const TABLE_BITS: u8 = 14;
    const TABLE_SIZE: usize = 1 << TABLE_BITS;
    let mut table = vec![0u32; TABLE_SIZE];

    let s_limit = src.len() - INPUT_MARGIN;
    let dst_limit = src.len() - (src.len() >> 5) - 5;
    let mut d = 0usize;
    let mut next_emit = 0usize;
    let mut s = 1usize;
    let mut cv = load64(src, s);
    let mut repeat = 1usize;

    'outer: loop {
        let mut candidate: usize;
        loop {
            let skip_shift = if SMALL { 5 } else { 6 };
            let next_s = s + ((s - next_emit) >> skip_shift) + 4;
            if next_s > s_limit {
                break 'outer;
            }
            let h0 = hash6(cv, TABLE_BITS);
            let h1 = hash6(cv >> 8, TABLE_BITS);
            candidate = table[h0] as usize;
            let candidate2 = table[h1] as usize;
            table[h0] = s as u32;
            table[h1] = (s + 1) as u32;
            let h2 = hash6(cv >> 16, TABLE_BITS);

            const CHECK_REP: usize = 1;
            if !snappy
                && s >= repeat
                && (cv >> (CHECK_REP * 8)) as u32 == load32(src, s - repeat + CHECK_REP)
            {
                let mut base = s + CHECK_REP;
                let mut i = base - repeat;
                while base > next_emit && i > 0 && src[i - 1] == src[base - 1] {
                    i -= 1;
                    base -= 1;
                }
                if d + (base - next_emit) > dst_limit {
                    return 0;
                }
                d += emit_literal(&mut dst[d..], &src[next_emit..base]);

                let mut cand = s - repeat + 4 + CHECK_REP;
                s += 4 + CHECK_REP;
                while s <= s_limit {
                    let diff = load64(src, s) ^ load64(src, cand);
                    if diff != 0 {
                        s += (diff.trailing_zeros() >> 3) as usize;
                        break;
                    }
                    s += 8;
                    cand += 8;
                }
                if next_emit > 0 {
                    d += emit_repeat(&mut dst[d..], repeat, s - base);
                } else {
                    d += emit_copy(&mut dst[d..], repeat, s - base);
                }
                next_emit = s;
                if s >= s_limit {
                    break 'outer;
                }
                cv = load64(src, s);
                continue;
            }

            if (cv as u32) == load32(src, candidate) {
                break;
            }
            candidate = table[h2] as usize;
            if ((cv >> 8) as u32) == load32(src, candidate2) {
                table[h2] = (s + 2) as u32;
                candidate = candidate2;
                s += 1;
                break;
            }
            table[h2] = (s + 2) as u32;
            if ((cv >> 16) as u32) == load32(src, candidate) {
                s += 2;
                break;
            }
            cv = load64(src, next_s);
            s = next_s;
        }

        while candidate > 0 && s > next_emit && src[candidate - 1] == src[s - 1] {
            candidate -= 1;
            s -= 1;
        }
        if d + (s - next_emit) > dst_limit {
            return 0;
        }
        d += emit_literal(&mut dst[d..], &src[next_emit..s]);

        loop {
            let base = s;
            repeat = base - candidate;
            s += 4;
            candidate += 4;
            while s <= src.len() - 8 {
                let diff = load64(src, s) ^ load64(src, candidate);
                if diff != 0 {
                    s += (diff.trailing_zeros() >> 3) as usize;
                    break;
                }
                s += 8;
                candidate += 8;
            }
            if snappy {
                d += emit_copy_no_repeat(&mut dst[d..], repeat, s - base);
            } else {
                d += emit_copy(&mut dst[d..], repeat, s - base);
            }
            next_emit = s;
            if s >= s_limit {
                break 'outer;
            }
            if d > dst_limit {
                return 0;
            }
            let x = load64(src, s - 2);
            let m2_hash = hash6(x, TABLE_BITS);
            let curr_hash = hash6(x >> 16, TABLE_BITS);
            candidate = table[curr_hash] as usize;
            table[m2_hash] = (s - 2) as u32;
            table[curr_hash] = s as u32;
            if ((x >> 16) as u32) != load32(src, candidate) {
                cv = load64(src, s + 1);
                s += 1;
                break;
            }
        }
    }

    if next_emit < src.len() {
        if d + (src.len() - next_emit) > dst_limit {
            return 0;
        }
        d += emit_literal(&mut dst[d..], &src[next_emit..]);
    }
    d
}

pub fn encode_block_better(dst: &mut [u8], src: &[u8]) -> usize {
    encode_block_better_core(dst, src, false)
}

pub fn encode_block_better_snappy(dst: &mut [u8], src: &[u8]) -> usize {
    encode_block_better_core(dst, src, true)
}

fn encode_block_better_core(dst: &mut [u8], src: &[u8], snappy: bool) -> usize {
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        return 0;
    }
    const L_BITS: u8 = 17;
    const S_BITS: u8 = 14;
    let mut l_table = vec![0u32; 1 << L_BITS];
    let mut s_table = vec![0u32; 1 << S_BITS];

    let s_limit = src.len() - INPUT_MARGIN;
    let dst_limit = src.len() - (src.len() >> 5) - 6;
    let mut d = 0usize;
    let mut next_emit = 0usize;
    let mut s = 1usize;
    let mut cv = load64(src, s);
    let mut repeat = 0usize;
    const MAX_SKIP: usize = 100;

    'outer: loop {
        let mut candidate_l;
        let mut next_s;
        loop {
            let skip = ((s - next_emit) >> 7) + 1;
            next_s = s + skip.min(MAX_SKIP);
            if next_s > s_limit {
                break 'outer;
            }
            let hl = hash7(cv, L_BITS);
            let hs = hash4(cv, S_BITS);
            candidate_l = l_table[hl] as usize;
            let candidate_s = s_table[hs] as usize;
            l_table[hl] = s as u32;
            s_table[hs] = s as u32;

            let val_long = load64(src, candidate_l);
            let val_short = load64(src, candidate_s);
            if cv == val_long {
                break;
            }
            if cv == val_short {
                candidate_l = candidate_s;
                break;
            }
            if (cv as u32) == (val_long as u32) {
                break;
            }
            if (cv as u32) == (val_short as u32) {
                let hl2 = hash7(cv >> 8, L_BITS);
                let cand2 = l_table[hl2] as usize;
                l_table[hl2] = (s + 1) as u32;
                if ((cv >> 8) as u32) == load32(src, cand2) {
                    candidate_l = cand2;
                    s += 1;
                    break;
                }
                candidate_l = candidate_s;
                break;
            }
            cv = load64(src, next_s);
            s = next_s;
        }

        while candidate_l > 0 && s > next_emit && src[candidate_l - 1] == src[s - 1] {
            candidate_l -= 1;
            s -= 1;
        }
        if d + (s - next_emit) > dst_limit {
            return 0;
        }
        let base = s;
        let offset = base - candidate_l;
        s += 4;
        candidate_l += 4;
        while s < src.len() {
            if src.len() - s < 8 {
                if src[s] == src[candidate_l] {
                    s += 1;
                    candidate_l += 1;
                    continue;
                }
                break;
            }
            let diff = load64(src, s) ^ load64(src, candidate_l);
            if diff != 0 {
                s += (diff.trailing_zeros() >> 3) as usize;
                break;
            }
            s += 8;
            candidate_l += 8;
        }
        if !snappy && offset > 65535 && s - base <= 5 && repeat != offset {
            s = next_s + 1;
            if s >= s_limit {
                break 'outer;
            }
            cv = load64(src, s);
            continue;
        }

        d += emit_literal(&mut dst[d..], &src[next_emit..base]);
        if !snappy && repeat == offset {
            d += emit_repeat(&mut dst[d..], offset, s - base);
        } else if snappy {
            d += emit_copy_no_repeat(&mut dst[d..], offset, s - base);
        } else {
            d += emit_copy(&mut dst[d..], offset, s - base);
            repeat = offset;
        }
        next_emit = s;
        if s >= s_limit {
            break 'outer;
        }
        if d > dst_limit {
            return 0;
        }
        let mut idx0 = base + 1;
        let idx1 = s - 2;
        if idx0 < s - 1 && idx1 + 8 <= src.len() {
            let cv0 = load64(src, idx0);
            let cv1 = load64(src, idx1);
            l_table[hash7(cv0, L_BITS)] = idx0 as u32;
            s_table[hash4(cv0 >> 8, S_BITS)] = (idx0 + 1) as u32;
            l_table[hash7(cv1, L_BITS)] = idx1 as u32;
            s_table[hash4(cv1 >> 8, S_BITS)] = (idx1 + 1) as u32;
            idx0 += 1;
            let idx1m = idx1.saturating_sub(1);
            let mut idx2 = (idx0 + idx1m + 1) >> 1;
            while idx2 < idx1m && idx0 + 8 <= src.len() && idx2 + 8 <= src.len() {
                l_table[hash7(load64(src, idx0), L_BITS)] = idx0 as u32;
                l_table[hash7(load64(src, idx2), L_BITS)] = idx2 as u32;
                idx0 += 2;
                idx2 += 2;
            }
        }
        cv = load64(src, s);
    }

    if next_emit < src.len() {
        if d + (src.len() - next_emit) > dst_limit {
            return 0;
        }
        d += emit_literal(&mut dst[d..], &src[next_emit..]);
    }
    d
}

pub fn encode_block_best(dst: &mut [u8], src: &[u8], dict: Option<&Dict>) -> usize {
    if src.len() < MIN_NON_LITERAL_BLOCK_SIZE {
        return 0;
    }
    const L_BITS: u8 = 18;
    const S_BITS: u8 = 15;
    let mut l_table = vec![0u64; 1 << L_BITS];
    let mut s_table = vec![0u64; 1 << S_BITS];

    let s_limit = src.len() - 10;
    let s_limit_dict = (src.len() - 10).min(MAX_DICT_SRC_OFFSET - 10);
    let dst_limit = src.len() - 5;

    let mut d = 0usize;
    let mut next_emit = 0usize;
    let mut s = 1usize;
    let mut repeat: isize = 1;
    let mut active_dict = dict;
    if let Some(dk) = active_dict {
        s = 0;
        repeat = (dk.dict.len() - dk.repeat) as isize;
    }
    if s + 8 > src.len() {
        return 0;
    }
    let mut cv = load64(src, s);
    const MAX_SKIP: usize = 64;

    #[derive(Clone, Copy, Default)]
    struct Match {
        offset: isize,
        s: usize,
        length: usize,
        score: isize,
        rep: bool,
        dict: bool,
    }

    let score_fn = |m: &Match, next_emit: usize| -> isize {
        let mut sc = m.length as isize - m.s as isize;
        if next_emit == m.s {
            sc += 1;
        }
        let off = (m.s as isize - m.offset) as usize;
        if m.rep {
            sc - emit_repeat_size(off, m.length) as isize
        } else {
            sc - emit_copy_size(off, m.length) as isize
        }
    };

    let best_of = |a: Match, b: Match| -> Match {
        if b.length == 0 {
            return a;
        }
        if a.length == 0 {
            return b;
        }
        let as_ = a.score + b.s as isize;
        let bs = b.score + a.s as isize;
        if as_ >= bs {
            a
        } else {
            b
        }
    };

    'outer: loop {
        let mut best = Match::default();
        loop {
            let next_s = s + (((s - next_emit) >> 8) + 1).min(MAX_SKIP);
            if next_s > s_limit {
                break 'outer;
            }
            if active_dict.is_some() && s >= MAX_DICT_SRC_OFFSET {
                active_dict = None;
                if repeat > s as isize {
                    repeat = i32::MIN as isize;
                }
            }
            let hl = hash8(cv, L_BITS);
            let hs = hash4(cv, S_BITS);
            let cand_l = l_table[hl];
            let cand_s = s_table[hs];

            let match_at = |offset: usize, ms: usize, first: u32, rep: bool, cur_best: &Match| -> Match {
                if offset >= ms {
                    return Match::default();
                }
                if cur_best.length != 0 && cur_best.s as isize - cur_best.offset == (ms - offset) as isize {
                    return Match::default();
                }
                if offset + 4 > src.len() || load32(src, offset) != first {
                    return Match::default();
                }
                let mut m_end = 4 + offset;
                let mut ps = ms + 4;
                while ps < src.len() {
                    if src.len() - ps < 8 {
                        if src[ps] == src[m_end] {
                            m_end += 1;
                            ps += 1;
                            continue;
                        }
                        break;
                    }
                    let diff = load64(src, ps) ^ load64(src, m_end);
                    if diff != 0 {
                        m_end += (diff.trailing_zeros() >> 3) as usize;
                        break;
                    }
                    ps += 8;
                    m_end += 8;
                }
                let length = m_end - offset;
                let mut m = Match {
                    offset: offset as isize,
                    s: ms,
                    length,
                    score: 0,
                    rep,
                    dict: false,
                };
                m.score = score_fn(&m, next_emit);
                if m.score <= -(m.s as isize) {
                    m.length = 0;
                }
                m
            };

            let match_dict = |dk: &Dict, cand: usize, ms: usize, first: u32, rep: bool, cur_best: &Match| -> Match {
                if ms >= MAX_DICT_SRC_OFFSET || cand + 4 > dk.dict.len() {
                    return Match::default();
                }
                let off = -(dk.dict.len() as isize) + cand as isize;
                if cur_best.length != 0 && cur_best.s as isize - cur_best.offset == ms as isize - off && !rep {
                    return Match::default();
                }
                if load32(&dk.dict, cand) != first {
                    return Match::default();
                }
                let mut m_end = 4 + cand;
                let mut ps = ms + 4;
                let lim = if rep { src.len() } else { s_limit_dict };
                while ps < lim && m_end < dk.dict.len() {
                    if src.len() - ps < 8 || dk.dict.len() - m_end < 8 {
                        if src[ps] == dk.dict[m_end] {
                            m_end += 1;
                            ps += 1;
                            continue;
                        }
                        break;
                    }
                    let diff = load64(src, ps) ^ load64(&dk.dict, m_end);
                    if diff != 0 {
                        m_end += (diff.trailing_zeros() >> 3) as usize;
                        break;
                    }
                    ps += 8;
                    m_end += 8;
                }
                let length = m_end - cand;
                let mut m = Match {
                    offset: off,
                    s: ms,
                    length,
                    score: 0,
                    rep,
                    dict: true,
                };
                m.score = score_fn(&m, next_emit);
                if m.score <= -(m.s as isize) {
                    m.length = 0;
                }
                m
            };

            if s > 0 {
                let c0 = (cand_l & 0xffff_ffff) as usize;
                let c1 = (cand_l >> 32) as usize;
                let s0 = (cand_s & 0xffff_ffff) as usize;
                let s1 = (cand_s >> 32) as usize;
                best = best_of(
                    match_at(c0, s, cv as u32, false, &best),
                    match_at(c1, s, cv as u32, false, &best),
                );
                best = best_of(best, match_at(s0, s, cv as u32, false, &best));
                best = best_of(best, match_at(s1, s, cv as u32, false, &best));
            }
            if let Some(dk) = active_dict {
                let (dl, ds) = dk.best_candidates(cv);
                for c in dl.into_iter().chain(ds) {
                    best = best_of(best, match_dict(dk, c, s, cv as u32, false, &best));
                }
            }
            if (active_dict.is_none() || repeat <= s as isize) && repeat > 0 {
                let off = (s as isize - repeat + 1) as usize;
                best = best_of(best, match_at(off, s + 1, (cv >> 8) as u32, true, &best));
            } else if let Some(dk) = active_dict {
                if (s as isize - repeat) < -4 {
                    let cand = (dk.dict.len() as isize - (repeat - s as isize)) as usize;
                    best = best_of(best, match_dict(dk, cand, s, cv as u32, true, &best));
                    best = best_of(best, match_dict(dk, cand + 1, s + 1, (cv >> 8) as u32, true, &best));
                }
            }

            if best.length > 0 {
                let hs1 = hash4(cv >> 8, S_BITS);
                let next_short = s_table[hs1];
                let s1 = s + 1;
                let cv1 = load64(src, s1);
                let hl1 = hash8(cv1, L_BITS);
                let next_long = l_table[hl1];
                best = best_of(best, match_at((next_short & 0xffff_ffff) as usize, s1, cv1 as u32, false, &best));
                best = best_of(best, match_at((next_short >> 32) as usize, s1, cv1 as u32, false, &best));
                best = best_of(best, match_at((next_long & 0xffff_ffff) as usize, s1, cv1 as u32, false, &best));
                best = best_of(best, match_at((next_long >> 32) as usize, s1, cv1 as u32, false, &best));
                if let Some(dk) = active_dict {
                    let (dl1, ds1) = dk.best_candidates(cv1);
                    if let Some(&c) = dl1.first() {
                        best = best_of(best, match_dict(dk, c, s1, cv1 as u32, false, &best));
                    }
                    if let Some(&c) = ds1.first() {
                        best = best_of(best, match_dict(dk, c, s1, cv1 as u32, false, &best));
                    }
                }
                break;
            }

            l_table[hl] = (s as u64) | (cand_l << 32);
            s_table[hs] = (s as u64) | (cand_s << 32);
            s = next_s;
            cv = load64(src, s);
        }

        // Extend backwards (not needed for repeats/dict)
        if !best.rep && !best.dict {
            while best.offset > 0 && best.s > next_emit && src[best.offset as usize - 1] == src[best.s - 1] {
                best.offset -= 1;
                best.s -= 1;
                best.length += 1;
            }
        }

        if d + (best.s - next_emit) > dst_limit {
            return 0;
        }
        let off = (best.s as isize - best.offset) as usize;
        if off > 65535 && best.length <= 5 && !best.rep {
            s = best.s + 1;
            if s >= s_limit {
                break 'outer;
            }
            cv = load64(src, s);
            continue;
        }
        d += emit_literal(&mut dst[d..], &src[next_emit..best.s]);
        if best.rep && (next_emit > 0 || best.dict) {
            d += emit_repeat(&mut dst[d..], off, best.length);
        } else {
            d += emit_copy(&mut dst[d..], off, best.length);
        }
        repeat = off as isize;

        let end = best.s + best.length;
        // Index positions from s to end-1
        let mut idx = s.max(1);
        let max_idx = end.min(src.len().saturating_sub(8));
        while idx < max_idx {
            let cv_i = load64(src, idx);
            let hl = hash8(cv_i, L_BITS);
            let hs = hash4(cv_i, S_BITS);
            l_table[hl] = (idx as u64) | (l_table[hl] << 32);
            s_table[hs] = (idx as u64) | (s_table[hs] << 32);
            idx += 1;
        }

        next_emit = end;
        s = end;
        if s >= s_limit {
            break 'outer;
        }
        if d > dst_limit {
            return 0;
        }
        cv = load64(src, s);
    }

    if next_emit < src.len() {
        if d + (src.len() - next_emit) > dst_limit {
            return 0;
        }
        d += emit_literal(&mut dst[d..], &src[next_emit..]);
    }
    d
}

/// Estimates compressed block size without producing output; returns `-1` if incompressible.
pub fn estimate_block_size(src: &[u8]) -> isize {
    if src.len() <= INPUT_MARGIN || src.len() > u32::MAX as usize {
        return -1;
    }
    const TABLE_BITS: u8 = 13;
    let mut table = vec![0u32; 1 << TABLE_BITS];
    let s_limit = src.len() - INPUT_MARGIN;
    let dst_limit = src.len() - (src.len() >> 5) - 5;
    let mut d = 0usize;
    let mut next_emit = 0usize;
    let mut s = 1usize;
    let mut cv = load64(src, s);
    let mut repeat = 1usize;

    'outer: loop {
        let mut candidate: usize;
        loop {
            let next_s = s + ((s - next_emit) >> 6) + 4;
            if next_s > s_limit {
                break 'outer;
            }
            let h0 = hash6(cv, TABLE_BITS);
            let h1 = hash6(cv >> 8, TABLE_BITS);
            candidate = table[h0] as usize;
            let candidate2 = table[h1] as usize;
            table[h0] = s as u32;
            table[h1] = (s + 1) as u32;
            let h2 = hash6(cv >> 16, TABLE_BITS);

            const CHECK_REP: usize = 1;
            if s >= repeat
                && (cv >> (CHECK_REP * 8)) as u32 == load32(src, s - repeat + CHECK_REP)
            {
                let mut base = s + CHECK_REP;
                let mut i = base - repeat;
                while base > next_emit && i > 0 && src[i - 1] == src[base - 1] {
                    i -= 1;
                    base -= 1;
                }
                d += emit_literal_size(&src[next_emit..base]);
                let mut cand = s - repeat + 4 + CHECK_REP;
                s += 4 + CHECK_REP;
                while s <= s_limit {
                    let diff = load64(src, s) ^ load64(src, cand);
                    if diff != 0 {
                        s += (diff.trailing_zeros() >> 3) as usize;
                        break;
                    }
                    s += 8;
                    cand += 8;
                }
                d += emit_copy_no_repeat_size(repeat, s - base);
                next_emit = s;
                if s >= s_limit {
                    break 'outer;
                }
                cv = load64(src, s);
                continue;
            }

            if (cv as u32) == load32(src, candidate) {
                break;
            }
            candidate = table[h2] as usize;
            if ((cv >> 8) as u32) == load32(src, candidate2) {
                table[h2] = (s + 2) as u32;
                candidate = candidate2;
                s += 1;
                break;
            }
            table[h2] = (s + 2) as u32;
            if ((cv >> 16) as u32) == load32(src, candidate) {
                s += 2;
                break;
            }
            cv = load64(src, next_s);
            s = next_s;
        }

        while candidate > 0 && s > next_emit && src[candidate - 1] == src[s - 1] {
            candidate -= 1;
            s -= 1;
        }
        if d + (s - next_emit) > dst_limit {
            return -1;
        }
        d += emit_literal_size(&src[next_emit..s]);
        loop {
            let base = s;
            repeat = base - candidate;
            s += 4;
            candidate += 4;
            while s <= src.len() - 8 {
                let diff = load64(src, s) ^ load64(src, candidate);
                if diff != 0 {
                    s += (diff.trailing_zeros() >> 3) as usize;
                    break;
                }
                s += 8;
                candidate += 8;
            }
            d += emit_copy_no_repeat_size(repeat, s - base);
            next_emit = s;
            if s >= s_limit {
                break 'outer;
            }
            if d > dst_limit {
                return -1;
            }
            let x = load64(src, s - 2);
            let m2_hash = hash6(x, TABLE_BITS);
            let curr_hash = hash6(x >> 16, TABLE_BITS);
            candidate = table[curr_hash] as usize;
            table[m2_hash] = (s - 2) as u32;
            table[curr_hash] = s as u32;
            if ((x >> 16) as u32) != load32(src, candidate) {
                cv = load64(src, s + 1);
                s += 1;
                break;
            }
        }
    }

    if next_emit < src.len() {
        if d + (src.len() - next_emit) > dst_limit {
            return -1;
        }
        d += emit_literal_size(&src[next_emit..]);
    }
    let varint_bytes = ((64 - (src.len() as u64).leading_zeros() as usize) + 7) / 7;
    let total = d + varint_bytes;
    if total >= src.len() {
        -1
    } else {
        total as isize
    }
}

/// Concatenates S2 blocks into `dst` without recompressing.
pub fn concat_blocks(dst: &mut Vec<u8>, blocks: &[&[u8]]) -> Result<(), S2Error> {
    let mut total_size: u64 = 0;
    let mut comp_size: usize = 0;
    for b in blocks {
        let (l, hdr) = decode_block_uvarint(b)?;
        total_size += l as u64;
        comp_size += b.len() - hdr;
    }
    if total_size == 0 {
        dst.push(0);
        return Ok(());
    }
    if total_size > u32::MAX as u64 {
        return Err(S2Error::TooLarge);
    }
    let mut hdr_buf = [0u8; 5];
    let hdr_len = put_uvarint(&mut hdr_buf, total_size);
    dst.reserve(hdr_len + comp_size);
    dst.extend_from_slice(&hdr_buf[..hdr_len]);
    for b in blocks {
        let (_, hdr) = decode_block_uvarint(b)?;
        dst.extend_from_slice(&b[hdr..]);
    }
    Ok(())
}
