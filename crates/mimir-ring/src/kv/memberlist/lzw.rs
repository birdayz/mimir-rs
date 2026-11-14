//! Go-compatible `compress/lzw` (LSB, litWidth=8) and IEEE CRC-32.
//!
//! Ported from:
//! - Go standard library `compress/lzw/writer.go` and `compress/lzw/reader.go`
//! - github.com/hashicorp/memberlist/lzw.go
//! - github.com/hashicorp/memberlist/net.go (`crc32.ChecksumIEEE`)

const MAX_CODE: u32 = (1 << 12) - 1;
const INVALID_CODE: u32 = u32::MAX;
const TABLE_SIZE: usize = 4 * (1 << 12);
const TABLE_MASK: usize = TABLE_SIZE - 1;
const INVALID_ENTRY: u32 = 0;

const LIT_WIDTH: u32 = 8;
const CLEAR_CODE: u32 = 1 << LIT_WIDTH; // 256
const EOF_CODE: u32 = CLEAR_CODE + 1; // 257

const MAX_WIDTH: u32 = 12;
const DECODER_INVALID_CODE: u16 = 0xffff;

/// Standard IEEE 802.3 CRC-32 (`0xedb88320` reflected polynomial), matching Go `crc32.ChecksumIEEE`.
pub fn crc32_ieee(data: &[u8]) -> u32 {
    const POLY: u32 = 0xedb8_8320;
    let mut crc = 0xffff_ffff_u32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
        }
    }
    !crc
}

/// Compresses `src` using Go `lzw.NewWriter(dst, lzw.LSB, 8)`.
pub fn lzw_compress(src: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(src.len() / 2 + 16);
    let mut table = vec![INVALID_ENTRY; TABLE_SIZE];
    let mut width = LIT_WIDTH + 1;
    let mut hi = CLEAR_CODE + 1;
    let mut overflow = 1u32 << width;
    let mut bits: u32 = 0;
    let mut n_bits: u32 = 0;

    let write_lsb = |c: u32,
                     width: u32,
                     bits: &mut u32,
                     n_bits: &mut u32,
                     out: &mut Vec<u8>| {
        *bits |= c << *n_bits;
        *n_bits += width;
        while *n_bits >= 8 {
            out.push((*bits & 0xff) as u8);
            *bits >>= 8;
            *n_bits -= 8;
        }
    };

    // Always write starting clear code.
    write_lsb(CLEAR_CODE, width, &mut bits, &mut n_bits, &mut out);

    if !src.is_empty() {
        let mut code = src[0] as u32;
        'outer: for &x in &src[1..] {
            let literal = x as u32;
            let key = (code << 8) | literal;
            let mut hash = ((key >> 12) ^ key) as usize & TABLE_MASK;
            loop {
                let t = table[hash];
                if t == INVALID_ENTRY {
                    break;
                }
                if key == (t >> 12) {
                    code = t & MAX_CODE;
                    continue 'outer;
                }
                hash = (hash + 1) & TABLE_MASK;
            }

            write_lsb(code, width, &mut bits, &mut n_bits, &mut out);
            code = literal;

            hi += 1;
            if hi == overflow {
                width += 1;
                overflow <<= 1;
            }
            if hi == MAX_CODE {
                write_lsb(CLEAR_CODE, width, &mut bits, &mut n_bits, &mut out);
                width = LIT_WIDTH + 1;
                hi = CLEAR_CODE + 1;
                overflow = CLEAR_CODE << 1;
                table.fill(INVALID_ENTRY);
                continue;
            }

            while table[hash] != INVALID_ENTRY {
                hash = (hash + 1) & TABLE_MASK;
            }
            table[hash] = (key << 12) | hi;
        }

        if code != INVALID_CODE {
            write_lsb(code, width, &mut bits, &mut n_bits, &mut out);
            hi += 1;
            if hi == overflow {
                width += 1;
            }
            if hi == MAX_CODE {
                write_lsb(CLEAR_CODE, width, &mut bits, &mut n_bits, &mut out);
                width = LIT_WIDTH + 1;
            }
        }
    }

    write_lsb(EOF_CODE, width, &mut bits, &mut n_bits, &mut out);
    if n_bits > 0 {
        out.push((bits & 0xff) as u8);
    }
    out
}

/// Decompresses `src` encoded by Go `lzw.NewWriter(..., lzw.LSB, 8)`, enforcing `max_bytes`.
pub fn lzw_decompress(src: &[u8], max_bytes: usize) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut prefix = [0u16; 1 << MAX_WIDTH];
    let mut suffix = [0u8; 1 << MAX_WIDTH];
    let mut stack = vec![0u8; 1 << MAX_WIDTH];

    let mut width = 1 + LIT_WIDTH;
    let clear = CLEAR_CODE as u16;
    let eof = EOF_CODE as u16;
    let mut hi = eof;
    let mut overflow = 1u16 << width;
    let mut last = DECODER_INVALID_CODE;

    let mut pos = 0usize;
    let mut bits: u32 = 0;
    let mut n_bits: u32 = 0;

    loop {
        while n_bits < width {
            if pos >= src.len() {
                return Err("lzw: unexpected EOF".to_string());
            }
            bits |= (src[pos] as u32) << n_bits;
            n_bits += 8;
            pos += 1;
        }
        let code = (bits & ((1u32 << width) - 1)) as u16;
        bits >>= width;
        n_bits -= width;

        if code < clear {
            if out.len() + 1 > max_bytes {
                return Err(format!(
                    "memberlist: LZW-decompressed payload exceeds {max_bytes} bytes"
                ));
            }
            out.push(code as u8);
            if last != DECODER_INVALID_CODE {
                suffix[hi as usize] = code as u8;
                prefix[hi as usize] = last;
            }
        } else if code == clear {
            width = 1 + LIT_WIDTH;
            hi = eof;
            overflow = 1u16 << width;
            last = DECODER_INVALID_CODE;
            continue;
        } else if code == eof {
            break;
        } else if code <= hi {
            let mut c = code;
            let mut i = stack.len();
            if code == hi && last != DECODER_INVALID_CODE {
                let mut head = last;
                while head >= clear {
                    head = prefix[head as usize];
                }
                i -= 1;
                stack[i] = head as u8;
                c = last;
            }
            while c >= clear {
                i -= 1;
                stack[i] = suffix[c as usize];
                c = prefix[c as usize];
            }
            i -= 1;
            stack[i] = c as u8;

            let decoded_slice = &stack[i..];
            if out.len() + decoded_slice.len() > max_bytes {
                return Err(format!(
                    "memberlist: LZW-decompressed payload exceeds {max_bytes} bytes"
                ));
            }
            out.extend_from_slice(decoded_slice);

            if last != DECODER_INVALID_CODE {
                suffix[hi as usize] = c as u8;
                prefix[hi as usize] = last;
            }
        } else {
            return Err("lzw: invalid code".to_string());
        }

        last = code;
        hi += 1;
        if hi >= overflow {
            if width == MAX_WIDTH {
                last = DECODER_INVALID_CODE;
                hi -= 1;
            } else {
                width += 1;
                overflow = 1u16 << width;
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_ieee() {
        assert_eq!(crc32_ieee(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32_ieee(b""), 0);
    }

    #[test]
    fn test_lzw_roundtrip_various() {
        let cases: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"TOBEORNOTTOBEORTOBEORNOT".to_vec(),
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec(),
            (0..20_000).map(|i| (i % 251) as u8).collect(),
        ];
        for data in cases {
            let comp = lzw_compress(&data);
            let decomp = lzw_decompress(&comp, 1024 * 1024).unwrap();
            assert_eq!(decomp, data);
        }
    }
}
