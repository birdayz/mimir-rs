//! Ported from github.com/klauspost/compress/s2/{lz4convert,lz4sconvert}.go.

use super::block::{emit_copy, emit_copy_no_repeat, emit_literal, emit_repeat};
use super::S2Error;

#[derive(Debug, Default, Clone, Copy)]
pub struct LZ4Converter;

#[derive(Debug, Default, Clone, Copy)]
pub struct LZ4sConverter;

impl LZ4Converter {
    pub fn convert_block_into(
        &self,
        dst: &mut [u8],
        dst_pos: usize,
        src: &[u8],
    ) -> Result<(usize, usize), S2Error> {
        convert_lz4_impl(dst, dst_pos, src, false, false)
    }

    pub fn convert_block_snappy_into(
        &self,
        dst: &mut [u8],
        dst_pos: usize,
        src: &[u8],
    ) -> Result<(usize, usize), S2Error> {
        convert_lz4_impl(dst, dst_pos, src, false, true)
    }
}

impl LZ4sConverter {
    pub fn convert_block_into(
        &self,
        dst: &mut [u8],
        dst_pos: usize,
        src: &[u8],
    ) -> Result<(usize, usize), S2Error> {
        convert_lz4_impl(dst, dst_pos, src, true, false)
    }

    pub fn convert_block_snappy_into(
        &self,
        dst: &mut [u8],
        dst_pos: usize,
        src: &[u8],
    ) -> Result<(usize, usize), S2Error> {
        convert_lz4_impl(dst, dst_pos, src, true, true)
    }
}

fn convert_lz4_impl(
    dst: &mut [u8],
    dst_pos: usize,
    src: &[u8],
    lz4s: bool,
    snappy: bool,
) -> Result<(usize, usize), S2Error> {
    if src.is_empty() {
        return Ok((dst_pos, 0));
    }
    if dst_pos >= dst.len() {
        if src.len() == 1 && src[0] == 0 {
            return Ok((dst_pos, 0));
        }
        return Err(S2Error::DstTooSmall);
    }

    let min_match: usize = if lz4s { 3 } else { 4 };
    let mut s = 0usize;
    let mut d = dst_pos;
    let d_limit = dst.len().saturating_sub(10);
    let mut last_offset = 0usize;
    let mut uncompressed: usize = 0;

    loop {
        if s >= src.len() {
            return Err(S2Error::Corrupt);
        }
        let token = src[s];
        let mut ll = (token >> 4) as usize;
        let mut ml = min_match + ((token & 0x0f) as usize);

        if token >= 0xf0 {
            loop {
                s += 1;
                if s >= src.len() {
                    return Err(S2Error::Corrupt);
                }
                let val = src[s];
                ll = ll.checked_add(val as usize).ok_or(S2Error::Corrupt)?;
                if ll >= src.len() - s {
                    return Err(S2Error::Corrupt);
                }
                if val != 255 {
                    break;
                }
            }
        }
        if s + ll >= src.len() {
            return Err(S2Error::Corrupt);
        }
        s += 1;
        if ll > 0 {
            if dst.len() < 13 || d + ll > dst.len() - 13 {
                return Err(S2Error::DstTooSmall);
            }
            d += emit_literal(&mut dst[d..], &src[s..s + ll]);
            s += ll;
            uncompressed = uncompressed
                .checked_add(ll)
                .filter(|&v| v <= i32::MAX as usize)
                .ok_or(S2Error::TooLarge)?;
        }

        if lz4s {
            if ml == min_match {
                if s == src.len() {
                    break;
                }
                continue;
            }
        } else if s == src.len() && ml == min_match {
            break;
        }

        if s + 2 > src.len() {
            return Err(S2Error::Corrupt);
        }
        if s >= src.len().saturating_sub(2) {
            return Err(S2Error::Corrupt);
        }
        let offset = u16::from_le_bytes([src[s], src[s + 1]]) as usize;
        s += 2;
        if offset == 0 || offset > uncompressed {
            return Err(S2Error::Corrupt);
        }

        if ml == min_match + 15 {
            loop {
                if s >= src.len() {
                    return Err(S2Error::Corrupt);
                }
                let val = src[s];
                s += 1;
                ml = ml
                    .checked_add(val as usize)
                    .filter(|&v| v <= i32::MAX as usize)
                    .ok_or(S2Error::TooLarge)?;
                if val != 255 {
                    if s >= src.len() {
                        return Err(S2Error::Corrupt);
                    }
                    break;
                }
            }
        }

        if !snappy && offset == last_offset {
            let needed = super::block::emit_repeat_size(offset, ml);
            if d + needed > dst.len() {
                return Err(S2Error::DstTooSmall);
            }
            d += emit_repeat(&mut dst[d..], offset, ml);
        } else if snappy {
            let needed = super::block::emit_copy_no_repeat_size(offset, ml);
            if d + needed > dst.len() {
                return Err(S2Error::DstTooSmall);
            }
            d += emit_copy_no_repeat(&mut dst[d..], offset, ml);
            last_offset = offset;
        } else {
            let needed = super::block::emit_copy_size(offset, ml);
            if d + needed > dst.len() {
                return Err(S2Error::DstTooSmall);
            }
            d += emit_copy(&mut dst[d..], offset, ml);
            last_offset = offset;
        }

        uncompressed = uncompressed
            .checked_add(ml)
            .filter(|&v| v <= i32::MAX as usize)
            .ok_or(S2Error::TooLarge)?;
        if d > d_limit {
            return Err(S2Error::DstTooSmall);
        }
    }

    Ok((d, uncompressed))
}
