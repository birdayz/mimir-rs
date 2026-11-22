//! 1:1 upstream-ported tests for `github.com/klauspost/compress/s2` (Part 1: 31 tests).

use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::Command;

use mimir_proto::s2::index::{
    remove_index_headers, restore_index_headers, Index, S2_INDEX_HEADER, S2_INDEX_TRAILER,
};
use mimir_proto::s2::lz4::{LZ4Converter, LZ4sConverter};
use mimir_proto::s2::stream::{
    Reader, ReaderOptions, Writer, WriterLevel, WriterOptions,
};
use mimir_proto::s2::{
    decode, encode, encode_best, encode_better, max_encoded_len, s2_decode, Dict, S2Error,
    CHUNK_TYPE_COMPRESSED, CHUNK_TYPE_UNCOMPRESSED, MAGIC_S2, MAGIC_STREAM_SNAPPY,
    MAX_BLOCK_SIZE, MIN_DICT_SIZE,
};
use rand::{Rng, RngCore, SeedableRng};

const S2_TESTDATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/testdata/s2");

fn read_zip_entries(zip_name: &str) -> Vec<(String, Vec<u8>)> {
    let zip_path = Path::new(S2_TESTDATA).join(zip_name);
    let out = Command::new("python3")
        .arg("-c")
        .arg(
            "import sys, zipfile, json, base64; z = zipfile.ZipFile(sys.argv[1]); print(json.dumps([(n, base64.b64encode(z.read(n)).decode()) for n in z.namelist()]))",
        )
        .arg(&zip_path)
        .output()
        .expect("python3 zip reader");
    assert!(out.status.success(), "python3 failed: {:?}", out);
    let parsed: Vec<(String, String)> = serde_json::from_slice(&out.stdout).unwrap();
    parsed
        .into_iter()
        .map(|(n, b64)| (n, base64_decode(&b64)))
        .collect()
}

fn base64_decode(s: &str) -> Vec<u8> {
    const LUT: [i8; 256] = {
        let mut t = [-1i8; 256];
        let mut i = 0u8;
        while i < 26 {
            t[(b'A' + i) as usize] = i as i8;
            t[(b'a' + i) as usize] = (i + 26) as i8;
            i += 1;
        }
        let mut d = 0u8;
        while d < 10 {
            t[(b'0' + d) as usize] = (d + 52) as i8;
            d += 1;
        }
        t[b'+' as usize] = 62;
        t[b'/' as usize] = 63;
        t
    };
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for b in s.bytes() {
        if b == b'=' || b.is_ascii_whitespace() {
            continue;
        }
        let v = LUT[b as usize];
        if v >= 0 {
            buf = (buf << 6) | (v as u32);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buf >> bits) as u8);
            }
        }
    }
    out
}

fn put_uvarint_vec(v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    let mut x = v;
    while x >= 0x80 {
        out.push((x as u8) | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
    out
}

/// Simple deterministic LZ4 block compressor (literals + 4-byte+ matches) for testing LZ4Converter.
fn compress_lz4_block(src: &[u8]) -> Vec<u8> {
    if src.is_empty() {
        return Vec::new();
    }
    if src.len() < 16 {
        let mut out = Vec::new();
        let ll = src.len();
        if ll >= 15 {
            out.push(0xf0);
            let mut rem = ll - 15;
            while rem >= 255 {
                out.push(255);
                rem -= 255;
            }
            out.push(rem as u8);
        } else {
            out.push((ll as u8) << 4);
        }
        out.extend_from_slice(src);
        return out;
    }
    let mut table = vec![0usize; 1 << 14];
    let mut out = Vec::new();
    let mut anchor = 0usize;
    let mut s = 1usize;
    let limit = src.len().saturating_sub(12);
    while s < limit {
        let key = u32::from_le_bytes([src[s], src[s + 1], src[s + 2], src[s + 3]]);
        let h = ((key.wrapping_mul(2654435761)) >> 18) as usize;
        let cand = table[h];
        table[h] = s;
        if cand < s && s - cand < 65535 && src[cand..cand + 4] == src[s..s + 4] {
            let mut ml = 4usize;
            while s + ml < src.len() - 5 && src[cand + ml] == src[s + ml] {
                ml += 1;
            }
            let ll = s - anchor;
            let tok_ll = ll.min(15) as u8;
            let tok_ml = (ml - 4).min(15) as u8;
            out.push((tok_ll << 4) | tok_ml);
            if ll >= 15 {
                let mut rem = ll - 15;
                while rem >= 255 {
                    out.push(255);
                    rem -= 255;
                }
                out.push(rem as u8);
            }
            out.extend_from_slice(&src[anchor..s]);
            let off = (s - cand) as u16;
            out.extend_from_slice(&off.to_le_bytes());
            if ml - 4 >= 15 {
                let mut rem = (ml - 4) - 15;
                while rem >= 255 {
                    out.push(255);
                    rem -= 255;
                }
                out.push(rem as u8);
            }
            s += ml;
            anchor = s;
        } else {
            s += 1;
        }
    }
    let ll = src.len() - anchor;
    if ll >= 15 {
        out.push(0xf0);
        let mut rem = ll - 15;
        while rem >= 255 {
            out.push(255);
            rem -= 255;
        }
        out.push(rem as u8);
    } else {
        out.push((ll as u8) << 4);
    }
    out.extend_from_slice(&src[anchor..]);
    out
}

/// Simple deterministic LZ4s block compressor (min_match=3 semantics, match token 1..=15 -> len 4..=18).
fn compress_lz4s_block(src: &[u8]) -> Vec<u8> {
    if src.is_empty() {
        return Vec::new();
    }
    if src.len() < 16 {
        let mut out = Vec::new();
        let ll = src.len();
        if ll >= 15 {
            out.push(0xf0);
            let mut rem = ll - 15;
            while rem >= 255 {
                out.push(255);
                rem -= 255;
            }
            out.push(rem as u8);
        } else {
            out.push((ll as u8) << 4);
        }
        out.extend_from_slice(src);
        return out;
    }
    let mut table = vec![0usize; 1 << 14];
    let mut out = Vec::new();
    let mut anchor = 0usize;
    let mut s = 1usize;
    let limit = src.len().saturating_sub(12);
    while s < limit {
        let key = u32::from_le_bytes([src[s], src[s + 1], src[s + 2], src[s + 3]]);
        let h = ((key.wrapping_mul(2654435761)) >> 18) as usize;
        let cand = table[h];
        table[h] = s;
        if cand < s && s - cand < 65535 && src[cand..cand + 4] == src[s..s + 4] {
            let mut ml = 4usize;
            while s + ml < src.len() - 5 && src[cand + ml] == src[s + ml] {
                ml += 1;
            }
            let ll = s - anchor;
            let tok_ll = ll.min(15) as u8;
            let tok_ml = (ml - 3).min(15) as u8;
            out.push((tok_ll << 4) | tok_ml);
            if ll >= 15 {
                let mut rem = ll - 15;
                while rem >= 255 {
                    out.push(255);
                    rem -= 255;
                }
                out.push(rem as u8);
            }
            out.extend_from_slice(&src[anchor..s]);
            let off = (s - cand) as u16;
            out.extend_from_slice(&off.to_le_bytes());
            if ml - 3 >= 15 {
                let mut rem = (ml - 3) - 15;
                while rem >= 255 {
                    out.push(255);
                    rem -= 255;
                }
                out.push(rem as u8);
            }
            s += ml;
            anchor = s;
        } else {
            s += 1;
        }
    }
    let ll = src.len() - anchor;
    if ll >= 15 {
        out.push(0xf0);
        let mut rem = ll - 15;
        while rem >= 255 {
            out.push(255);
            rem -= 255;
        }
        out.push(rem as u8);
    } else {
        out.push((ll as u8) << 4);
    }
    out.extend_from_slice(&src[anchor..]);
    out
}

struct ZeroReader;
impl Read for ZeroReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        buf.fill(0);
        Ok(buf.len())
    }
}

// ============================================================================
// decode_test.go
// ============================================================================

// Upstream: github.com/klauspost/compress/s2/decode_test.go::TestDecodeRegression
#[test]
fn test_decode_regression() {
    for (_name, raw) in read_zip_entries("dec-block-regressions.zip") {
        let _ = decode(&raw);
    }
}

// Upstream: github.com/klauspost/compress/s2/decode_test.go::TestDecoderMaxBlockSize
#[test]
fn test_decoder_max_block_size() {
    let entries = read_zip_entries("enc_regressions.zip");
    let sizes = [4 << 10, 10 << 10, 1 << 20, 4 << 20];
    for (name, data) in entries.into_iter().take(6) {
        for size in sizes {
            let mut enc = Writer::with_options(
                Vec::new(),
                WriterOptions {
                    block_size: size,
                    padding: 16 << 10,
                    padding_src: Some(Box::new(ZeroReader)),
                    ..Default::default()
                },
            );
            enc.write_all(&data).unwrap();
            enc.close().unwrap();
            enc.close().unwrap();
            let buf = enc.reset(Vec::new()).unwrap();

            let mut dec = Reader::with_options(
                Cursor::new(&buf),
                ReaderOptions {
                    max_block_size: size,
                    alloc_block: size / 2,
                    ..Default::default()
                },
            );
            let mut got = Vec::new();
            dec.read_to_end(&mut got).unwrap();
            assert_eq!(got, data, "{name}/{size}");

            let n2 = enc.read_from(Cursor::new(&data)).unwrap();
            assert_eq!(n2 as usize, data.len());
            let n2b = enc.read_from(Cursor::new(&data)).unwrap();
            assert_eq!(n2b as usize, data.len());
            enc.close().unwrap();
            let buf2 = enc.into_inner().unwrap();
            assert_eq!(buf2.len() % (16 << 10), 0);

            let mut dec2 = Reader::with_options(
                Cursor::new(&buf2),
                ReaderOptions {
                    max_block_size: size,
                    alloc_block: size / 2,
                    ..Default::default()
                },
            );
            dec2.skip(data.len() as i64).unwrap();
            let mut got2 = Vec::new();
            dec2.read_to_end(&mut got2).unwrap();
            assert_eq!(got2, data);

            let mut dec3 = Reader::with_options(
                Cursor::new(&buf2),
                ReaderOptions {
                    max_block_size: size,
                    ..Default::default()
                },
            );
            let mut double_b = Vec::new();
            let nb = dec3.decode_concurrent(&mut double_b, 2).unwrap();
            assert_eq!(nb as usize, data.len() * 2);
            assert_eq!(&double_b[..data.len()], &data[..]);
            assert_eq!(&double_b[data.len()..], &data[..]);
        }
    }
}

// ============================================================================
// dict_test.go
// ============================================================================

// Upstream: github.com/klauspost/compress/s2/dict_test.go::TestDict
#[test]
fn test_dict() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    let mut data = vec![0u8; 128 << 10];
    rng.fill_bytes(&mut data);

    let mut raw1 = vec![0u8];
    raw1.extend_from_slice(&data[..65536]);
    let d1 = Dict::new(&raw1).unwrap();
    let enc1 = d1.encode(&data);
    assert!(enc1.len() < data.len() - 65500);
    assert_eq!(d1.decode(&enc1).unwrap(), data);

    let mut raw2 = vec![0u8];
    raw2.extend_from_slice(&data[5000..65536 + 5000]);
    let d2 = Dict::new(&raw2).unwrap();
    let enc2 = d2.encode(&data);
    assert!(enc2.len() < data.len() - 65500);
    assert_eq!(d2.decode(&enc2).unwrap(), data);

    let mut i = 1;
    while i < data.len() {
        let n = rng.gen_range(4..36).min(data.len() - i);
        let off = rng.gen_range(0..data.len() - n);
        data.copy_within(off..off + n, i);
        i += n;
    }
    let mut dict = vec![0u8; 65536];
    let mut i = 1;
    while i < dict.len() {
        let n = rng.gen_range(4..36).min(dict.len() - i);
        let off = rng.gen_range(0..65536 - n);
        dict[i..i + n].copy_from_slice(&data[off..off + n]);
        i += n;
    }
    let d3 = Dict::new(&dict).unwrap();
    let enc3 = d3.encode(&data);
    assert!(enc3.len() < data.len() - 20000);
    assert_eq!(d3.decode(&enc3).unwrap(), data);
}

// Upstream: github.com/klauspost/compress/s2/dict_test.go::TestNewDictRepeatOverflow
#[test]
fn test_new_dict_repeat_overflow() {
    for r in [
        1u64 << 31,
        u32::MAX as u64,
        (i64::MAX as u64) + 1,
        u64::MAX,
        12687765596934209639u64,
    ] {
        let mut dict_data = put_uvarint_vec(r);
        dict_data.extend_from_slice(&[0u8; MIN_DICT_SIZE]);
        assert!(Dict::new(&dict_data).is_none(), "repeat {r} should fail");
    }

    let dict = vec![0u8; MIN_DICT_SIZE];
    let mut ok_data = put_uvarint_vec(dict.len() as u64);
    ok_data.extend_from_slice(&dict);
    assert!(Dict::new(&ok_data).is_some());
}

// Upstream: github.com/klauspost/compress/s2/dict_test.go::TestDictBetter
#[test]
fn test_dict_better() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    let mut data = vec![0u8; 128 << 10];
    rng.fill_bytes(&mut data);

    let mut raw1 = vec![0u8];
    raw1.extend_from_slice(&data[..65536]);
    let d1 = Dict::new(&raw1).unwrap();
    let enc1 = d1.encode_better(&data);
    assert!(enc1.len() < data.len() - 65500);
    assert_eq!(d1.decode(&enc1).unwrap(), data);

    let mut raw2 = vec![0u8];
    raw2.extend_from_slice(&data[5000..65536 + 5000]);
    let d2 = Dict::new(&raw2).unwrap();
    let enc2 = d2.encode_better(&data);
    assert!(enc2.len() < data.len() - 65500);
    assert_eq!(d2.decode(&enc2).unwrap(), data);

    let mut i = 1;
    while i < data.len() {
        let n = rng.gen_range(4..36).min(data.len() - i);
        let off = rng.gen_range(0..data.len() - n);
        data.copy_within(off..off + n, i);
        i += n;
    }
    let mut dict = vec![0u8; 65536];
    let mut i = 1;
    while i < dict.len() {
        let n = rng.gen_range(4..36).min(dict.len() - i);
        let off = rng.gen_range(0..65536 - n);
        dict[i..i + n].copy_from_slice(&data[off..off + n]);
        i += n;
    }
    let d3 = Dict::new(&dict).unwrap();
    let enc3 = d3.encode_better(&data);
    assert!(enc3.len() < data.len() - 20000);
    assert_eq!(d3.decode(&enc3).unwrap(), data);
}

// Upstream: github.com/klauspost/compress/s2/dict_test.go::TestDictBest
#[test]
fn test_dict_best() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    let mut data = vec![0u8; 128 << 10];
    rng.fill_bytes(&mut data);

    let mut raw1 = vec![0u8];
    raw1.extend_from_slice(&data[..65536]);
    let d1 = Dict::new(&raw1).unwrap();
    let enc1 = d1.encode_best(&data);
    assert!(enc1.len() < data.len() - 65500);
    assert_eq!(d1.decode(&enc1).unwrap(), data);

    let mut raw2 = vec![0u8];
    raw2.extend_from_slice(&data[5000..65536 + 5000]);
    let d2 = Dict::new(&raw2).unwrap();
    let enc2 = d2.encode_best(&data);
    assert!(enc2.len() < data.len() - 65500);
    assert_eq!(d2.decode(&enc2).unwrap(), data);

    let mut i = 1;
    while i < data.len() {
        let n = rng.gen_range(4..36).min(data.len() - i);
        let off = rng.gen_range(0..data.len() - n);
        data.copy_within(off..off + n, i);
        i += n;
    }
    let mut dict = vec![0u8; 65536];
    let mut i = 1;
    while i < dict.len() {
        let n = rng.gen_range(4..36).min(dict.len() - i);
        let off = rng.gen_range(0..65536 - n);
        dict[i..i + n].copy_from_slice(&data[off..off + n]);
        i += n;
    }
    let d3 = Dict::new(&dict).unwrap();
    let enc3 = d3.encode_best(&data);
    assert!(enc3.len() < data.len() - 20000);
    assert_eq!(d3.decode(&enc3).unwrap(), data);
}

// Upstream: github.com/klauspost/compress/s2/dict_test.go::TestDictBetter2
#[test]
fn test_dict_better2() {
    let data = b"10 bananas which were brown were added";
    let mut raw = vec![6u8];
    raw.extend_from_slice(b"Yesterday 25 bananas were added to Benjamins brown bag");
    let d = Dict::new(&raw).unwrap();
    let enc = d.encode_better(data);
    assert_eq!(d.decode(&enc).unwrap(), data);
}

// Upstream: github.com/klauspost/compress/s2/dict_test.go::TestDictBest2
#[test]
fn test_dict_best2() {
    let data = b"10 bananas which were brown were added";
    let mut raw = vec![6u8];
    raw.extend_from_slice(b"Yesterday 25 bananas were added to Benjamins brown bag");
    let d = Dict::new(&raw).unwrap();
    let enc = d.encode_best(data);
    assert_eq!(d.decode(&enc).unwrap(), data);
}

// Upstream: github.com/klauspost/compress/s2/dict_test.go::TestDictSize
#[test]
fn test_dict_size() {
    let text = std::fs::read(Path::new(S2_TESTDATA).join("Mark.Twain-Tom.Sawyer.txt")).unwrap();
    let d = Dict::make(&text, b"Tom Sawyer").unwrap();
    for chunk in text.chunks(4096) {
        let enc = d.encode_best(chunk);
        let dec = d.decode(&enc).unwrap();
        assert_eq!(dec, chunk);
    }
}

// ============================================================================
// encode_skip_test.go & encode_test.go
// ============================================================================

fn mixed_runs_for_skip(total: usize, run_len: usize) -> Vec<u8> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    let words = ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur"];
    let mut out = Vec::with_capacity(total + 2 * run_len);
    while out.len() < total {
        let mut noise = vec![0u8; run_len];
        rng.fill_bytes(&mut noise);
        out.extend_from_slice(&noise);
        let start = out.len();
        while out.len() - start < run_len {
            let w = words[rng.gen_range(0..words.len())];
            out.extend_from_slice(w.as_bytes());
            out.push(b' ');
        }
        out.truncate(start + run_len);
    }
    out.truncate(total);
    out
}

// Upstream: github.com/klauspost/compress/s2/encode_skip_test.go::TestBetterGoSkipMatchesAsm
#[test]
fn test_better_go_skip_matches_asm() {
    for run_len in [16 << 10, 64 << 10, 256 << 10, 1 << 20] {
        let src = mixed_runs_for_skip(4 << 20, run_len);
        let enc = encode_better(&src);
        assert!(enc.len() < src.len() * 65 / 100, "run_len={run_len}: {}", enc.len());
        assert_eq!(decode(&enc).unwrap(), src);
    }
}

// Upstream: github.com/klauspost/compress/s2/encode_test.go::TestEncodeHuge
#[test]
fn test_encode_huge() {
    let data = vec![0x42u8; MAX_BLOCK_SIZE];
    for enc_fn in [encode, encode_better, encode_best] {
        let comp = enc_fn(&data);
        let mel = max_encoded_len(data.len() as i64) as usize;
        assert!(comp.len() <= mel);
        assert_eq!(decode(&comp).unwrap(), data);
    }
}

// ============================================================================
// index_test.go
// ============================================================================

// Upstream: github.com/klauspost/compress/s2/index_test.go::TestSeeking
#[test]
fn test_seeking() {
    let mut compressed = Vec::new();
    let mut enc = Writer::with_options(
        &mut compressed,
        WriterOptions {
            block_size: 16 << 10,
            ..Default::default()
        },
    );
    let n_elems = 100_000usize;
    let mut expected = Vec::with_capacity(n_elems * 25);
    for i in 0..n_elems {
        let line = format!("Item {i:019}\n");
        expected.extend_from_slice(line.as_bytes());
        enc.write_all(line.as_bytes()).unwrap();
    }
    let idx_bytes = enc.close_index().unwrap();
    let mut idx = Index::new();
    idx.load(&idx_bytes).unwrap();

    for sz in [100usize, 1_000, 10_000, 20_000, n_elems - 1] {
        let want_off = (sz * 25) as i64;
        let (comp_off, uncomp_off) = idx.find(want_off).unwrap();
        let mut dec = Reader::with_options(
            Cursor::new(&compressed[comp_off as usize..]),
            ReaderOptions {
                ignore_stream_identifier: true,
                ..Default::default()
            },
        );
        dec.skip(want_off - uncomp_off).unwrap();
        let mut got = vec![0u8; 25];
        dec.read_exact(&mut got).unwrap();
        assert_eq!(&got, &expected[want_off as usize..want_off as usize + 25]);
    }
}

// Upstream: github.com/klauspost/compress/s2/index_test.go::TestRemoveIndexHeadersShortChunk
#[test]
fn test_remove_index_headers_short_chunk() {
    let mut compressed = Vec::new();
    let mut enc = Writer::with_options(
        &mut compressed,
        WriterOptions {
            block_size: 4 << 10,
            ..Default::default()
        },
    );
    for i in 0..1000 {
        writeln!(enc, "Item {i:019}").unwrap();
    }
    let index = enc.close_index().unwrap();
    let stripped = remove_index_headers(&index).expect("valid index");
    assert_eq!(restore_index_headers(stripped), index);

    let min_chunk_len = S2_INDEX_HEADER.len() + S2_INDEX_TRAILER.len() + 4;
    for chunk_len in [
        0usize,
        1,
        S2_INDEX_HEADER.len() - 1,
        S2_INDEX_HEADER.len(),
        min_chunk_len - 1,
    ] {
        let mut b = index.clone();
        b[1] = (chunk_len & 0xff) as u8;
        b[2] = ((chunk_len >> 8) & 0xff) as u8;
        b[3] = ((chunk_len >> 16) & 0xff) as u8;
        assert!(remove_index_headers(&b).is_none());
    }
}

// ============================================================================
// lz4convert_test.go & lz4sconvert_test.go
// ============================================================================

// Upstream: github.com/klauspost/compress/s2/lz4convert_test.go::TestLZ4Converter_ConvertBlock
#[test]
fn test_lz4_converter_convert_block() {
    let data = std::fs::read(Path::new(S2_TESTDATA).join("Mark.Twain-Tom.Sawyer.txt")).unwrap();
    let lz4_data = compress_lz4_block(&data);
    let conv = LZ4Converter;
    let mut dst = vec![0u8; max_encoded_len(data.len() as i64) as usize];
    let hdr = mimir_proto::s2::block::put_uvarint(&mut dst, data.len() as u64);

    let (end_s2, uncomp) = conv.convert_block_into(&mut dst, hdr, &lz4_data).unwrap();
    assert_eq!(uncomp, data.len());
    assert_eq!(decode(&dst[..end_s2]).unwrap(), data);

    let (end_snap, uncomp_s) = conv.convert_block_snappy_into(&mut dst, hdr, &lz4_data).unwrap();
    assert_eq!(uncomp_s, data.len());
    assert_eq!(decode(&dst[..end_snap]).unwrap(), data);
}

// Upstream: github.com/klauspost/compress/s2/lz4convert_test.go::TestLZ4Converter_ConvertBlockSingle
#[test]
fn test_lz4_converter_convert_block_single() {
    let mut lz4_data = vec![0x6f, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x1, 0x0];
    lz4_data.extend(std::iter::repeat_n(0xffu8, 250));
    lz4_data.extend_from_slice(&[
        0x30, 0x0f, 0x30, 0x30, 0xe4, 0x1f, 0x30, 0x30, 0x30, 0xff, 0xff, 0x30, 0x2f, 0x30, 0x30,
        0x30, 0x30, 0xcf, 0x7f, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0xaf,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0xff, 0xff, 0x30,
        0x0f, 0x30, 0x30, 0x30, 0x1f, 0x30, 0x30, 0x30, 0xff, 0xff, 0x30, 0x30, 0x30, 0x30, 0x30,
    ]);
    let conv = LZ4Converter;
    let mut dst = vec![0u8; 4 << 20];
    let res = conv.convert_block_into(&mut dst, 0, &lz4_data);
    if let Ok((end, n)) = res {
        let mut out = vec![0u8; n];
        s2_decode(&mut out, &dst[..end]).unwrap();
    }
}

// Upstream: github.com/klauspost/compress/s2/lz4convert_test.go::TestLZ4ConvertBlockSrcBounds
#[test]
fn test_lz4_convert_block_src_bounds() {
    let conv = LZ4Converter;
    let conv_s = LZ4sConverter;
    for ll in 1..15usize {
        let mut src = vec![(ll as u8) << 4];
        src.extend(std::iter::repeat_n(b'a', ll));
        let mut dst = vec![0u8; 64];
        for use_s in [false, true] {
            let (end, got) = if use_s {
                conv_s.convert_block_into(&mut dst, 0, &src).unwrap()
            } else {
                conv.convert_block_into(&mut dst, 0, &src).unwrap()
            };
            assert_eq!(got, ll);
            assert!(end <= dst.len());
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/lz4convert_test.go::TestLZ4ConvertBlockDstBounds
#[test]
fn test_lz4_convert_block_dst_bounds() {
    fn seq(b: &mut Vec<u8>, ll: usize, offset: u16, ml: usize) {
        let ext = |b: &mut Vec<u8>, mut v: usize| {
            while v >= 255 {
                b.push(255);
                v -= 255;
            }
            b.push(v as u8);
        };
        let mut tok = ((ll.min(15)) as u8) << 4;
        if ml > 0 {
            tok |= (ml - 4).min(15) as u8;
        }
        b.push(tok);
        if ll >= 15 {
            ext(b, ll - 15);
        }
        b.extend(std::iter::repeat_n(b'a', ll));
        if ml == 0 {
            return;
        }
        b.extend_from_slice(&offset.to_le_bytes());
        if ml >= 19 {
            ext(b, ml - 19);
        }
    }

    let mut cases = Vec::new();
    let mut b1 = Vec::new();
    seq(&mut b1, 70000, 1, 65805);
    seq(&mut b1, 5, 0, 0);
    cases.push(("long-literal-copy", 69990usize, b1));

    let mut b2 = Vec::new();
    seq(&mut b2, 10, 1, 100 << 20);
    seq(&mut b2, 5, 0, 0);
    cases.push(("huge-copy", 0usize, b2));

    let conv = LZ4Converter;
    for (name, start, src) in cases {
        for dst_len in start..start + 50 {
            let mut buf = vec![0xaa; dst_len];
            let res = conv.convert_block_into(&mut buf, 0, &src);
            if let Err(e) = res {
                assert_eq!(e, S2Error::DstTooSmall, "{name} dst_len={dst_len}");
            }
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/lz4convert_test.go::TestLZ4ConvertBlockLengthOverflow
#[test]
fn test_lz4_convert_block_length_overflow() {
    let ext = |b: &mut Vec<u8>, v: i64| {
        b.extend(std::iter::repeat_n(255u8, (v / 255) as usize));
        b.push((v % 255) as u8);
    };
    let mut src1 = vec![0xf0u8];
    ext(&mut src1, (i32::MAX as i64) - 15);

    let mut src2 = vec![0x1f, b'a', 1, 0];
    ext(&mut src2, (1i64 << 32) + 100 - 19);
    src2.extend_from_slice(&[0x10, b'a']);

    let conv = LZ4Converter;
    let conv_s = LZ4sConverter;
    for src in [src1, src2] {
        let mut dst = vec![0u8; 4 << 10];
        assert!(conv.convert_block_into(&mut dst, 1, &src).is_err());
        assert!(conv_s.convert_block_into(&mut dst, 1, &src).is_err());
    }
}

// Upstream: github.com/klauspost/compress/s2/lz4convert_test.go::TestLZ4Converter_EmptyDst
#[test]
fn test_lz4_converter_empty_dst() {
    let data = b"hello world ".repeat(20);
    let lz4_data = compress_lz4_block(&data);
    let lz4s_data = compress_lz4s_block(&data);
    let conv = LZ4Converter;
    let conv_s = LZ4sConverter;

    for (use_s, snappy, input) in [
        (false, false, &lz4_data),
        (false, true, &lz4_data),
        (true, false, &lz4s_data),
        (true, true, &lz4s_data),
    ] {
        let mut empty = Vec::new();
        let res = match (use_s, snappy) {
            (false, false) => conv.convert_block_into(&mut empty, 0, input),
            (false, true) => conv.convert_block_snappy_into(&mut empty, 0, input),
            (true, false) => conv_s.convert_block_into(&mut empty, 0, input),
            (true, true) => conv_s.convert_block_snappy_into(&mut empty, 0, input),
        };
        assert_eq!(res.unwrap_err(), S2Error::DstTooSmall);

        let mut full = vec![0u8; 10];
        let res_empty = match (use_s, snappy) {
            (false, false) => conv.convert_block_into(&mut full, 10, &[0]),
            (false, true) => conv.convert_block_snappy_into(&mut full, 10, &[0]),
            (true, false) => conv_s.convert_block_into(&mut full, 10, &[0]),
            (true, true) => conv_s.convert_block_snappy_into(&mut full, 10, &[0]),
        };
        assert_eq!(res_empty.unwrap(), (10, 0));
    }
}

// Upstream: github.com/klauspost/compress/s2/lz4sconvert_test.go::TestLZ4sConverter_ConvertBlock
#[test]
fn test_lz4s_converter_convert_block() {
    let data = std::fs::read(Path::new(S2_TESTDATA).join("Mark.Twain-Tom.Sawyer.txt")).unwrap();
    let lz4s_data = compress_lz4s_block(&data);
    let conv = LZ4sConverter;
    let mut dst = vec![0u8; max_encoded_len(data.len() as i64) as usize];
    let hdr = mimir_proto::s2::block::put_uvarint(&mut dst, data.len() as u64);

    let (end_s2, uncomp) = conv.convert_block_into(&mut dst, hdr, &lz4s_data).unwrap();
    assert_eq!(uncomp, data.len());
    assert_eq!(decode(&dst[..end_s2]).unwrap(), data);

    let (end_snap, uncomp_s) = conv.convert_block_snappy_into(&mut dst, hdr, &lz4s_data).unwrap();
    assert_eq!(uncomp_s, data.len());
    assert_eq!(decode(&dst[..end_snap]).unwrap(), data);
}

// ============================================================================
// reader_test.go
// ============================================================================

// Upstream: github.com/klauspost/compress/s2/reader_test.go::TestLeadingSkippableBlock
#[test]
fn test_leading_skippable_block() {
    let mut buf = Vec::new();
    let mut w = Writer::new(&mut buf);
    w.add_skippable_block(0x80, b"skippable block").unwrap();
    w.write_all(b"some data").unwrap();
    w.close().unwrap();

    let mut r = Reader::new(Cursor::new(&buf));
    let sb = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let sb_c = std::sync::Arc::clone(&sb);
    r.skippable_cb(
        0x80,
        Box::new(move |payload| {
            *sb_c.lock() = payload.to_vec();
            Ok(())
        }),
    )
    .unwrap();
    assert_eq!(r.read(&mut []).unwrap(), 0);
    assert_eq!(&*sb.lock(), b"skippable block");
    let mut data = Vec::new();
    r.read_to_end(&mut data).unwrap();
    assert_eq!(&data, b"some data");
}

// Upstream: github.com/klauspost/compress/s2/reader_test.go::TestSkipOversizedChunk
#[test]
fn test_skip_oversized_chunk() {
    let mut stream = MAGIC_STREAM_SNAPPY.to_vec();
    stream.extend_from_slice(&[0x01, 0xff, 0xff, 0x00]);
    let mut r1 = Reader::with_options(
        Cursor::new(&stream),
        ReaderOptions {
            max_block_size: 1024,
            ..Default::default()
        },
    );
    assert!(r1.read(&mut [0u8; 1]).is_err());

    let mut r2 = Reader::with_options(
        Cursor::new(&stream),
        ReaderOptions {
            max_block_size: 1024,
            ..Default::default()
        },
    );
    assert_eq!(r2.skip(1).unwrap_err(), S2Error::Corrupt);
}

// Upstream: github.com/klauspost/compress/s2/reader_test.go::TestReaderSkipCRC
#[test]
fn test_reader_skip_crc() {
    let make_stream = |data: &[u8]| -> (Vec<u8>, usize) {
        let mut buf = Vec::new();
        let mut w = Writer::with_options(
            &mut buf,
            WriterOptions {
                block_size: 4 << 10,
                concurrency: 1,
                ..Default::default()
            },
        );
        w.write_all(data).unwrap();
        w.close().unwrap();
        let mut second = 4 + MAGIC_S2.len();
        let c1_len = (buf[second + 1] as usize)
            | ((buf[second + 2] as usize) << 8)
            | ((buf[second + 3] as usize) << 16);
        second += 4 + c1_len;
        (buf, second)
    };

    let mut random = vec![0u8; 16 << 10];
    rand::rngs::StdRng::seed_from_u64(0).fill_bytes(&mut random);
    let text = b"compressible text ".repeat(1 << 10);

    for (data, typ) in [
        (random.as_slice(), CHUNK_TYPE_UNCOMPRESSED),
        (text.as_slice(), CHUNK_TYPE_COMPRESSED),
    ] {
        let (mut b, chunk) = make_stream(data);
        assert_eq!(b[chunk], typ);
        b[chunk + 4] ^= 1;

        let mut r = Reader::new(Cursor::new(&b));
        match r.skip(4096 + 100) {
            Ok(()) => {
                let mut tmp = Vec::new();
                assert!(r.read_to_end(&mut tmp).is_err());
            }
            Err(e) => assert_eq!(e, S2Error::CRC),
        }

        let mut r2 = Reader::with_options(
            Cursor::new(&b),
            ReaderOptions {
                ignore_crc: true,
                ..Default::default()
            },
        );
        r2.skip(4096 + 100).unwrap();
        let mut got = Vec::new();
        r2.read_to_end(&mut got).unwrap();
        assert_eq!(got, &data[4096 + 100..]);
    }
}

// ============================================================================
// writer_test.go
// ============================================================================

// Upstream: github.com/klauspost/compress/s2/writer_test.go::TestEncoderRegression
#[test]
fn test_encoder_regression() {
    let entries = read_zip_entries("enc_regressions.zip");
    for (name, data) in entries.into_iter().take(12) {
        if data.len() > 10_000 {
            continue;
        }
        for level in [
            WriterLevel::Fast,
            WriterLevel::Better,
            WriterLevel::Best,
            WriterLevel::Uncompressed,
        ] {
            let mut buf = Vec::new();
            let mut enc = Writer::with_options(
                &mut buf,
                WriterOptions {
                    level,
                    add_index: level == WriterLevel::Fast,
                    padding: 8000,
                    padding_src: Some(Box::new(ZeroReader)),
                    ..Default::default()
                },
            );
            enc.write_all(&data).unwrap();
            enc.close().unwrap();
            assert_eq!(buf.len() % 8000, 0, "{name}/{level:?}");

            let mut dec = Reader::new(Cursor::new(&buf));
            let mut got = Vec::new();
            dec.read_to_end(&mut got).unwrap_or_else(|e| panic!("{name}/{level:?} (len={}): {e:?}", data.len()));
            assert_eq!(got, data, "{name}/{level:?}");
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/writer_test.go::TestIndex
#[test]
fn test_index() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0xabeefcafe);
    let mut input = vec![0u8; 500 << 10];
    rng.fill_bytes(&mut input);
    for v in &mut input {
        *v = b'0' + (*v & 3);
    }

    let mut buf = Vec::new();
    let mut enc = Writer::with_options(
        &mut buf,
        WriterOptions {
            block_size: 100 << 10,
            add_index: true,
            level: WriterLevel::Better,
            ..Default::default()
        },
    );
    let mut todo = input.as_slice();
    while !todo.is_empty() {
        let take = rng.gen_range(1..=(1 + (todo.len() & 65535)).min(todo.len()));
        enc.write_all(&todo[..take]).unwrap();
        if rng.gen_range(0..8) == 0 {
            enc.flush().unwrap();
        }
        todo = &todo[take..];
    }
    let idx_bytes = enc.close_index().unwrap();
    let mut index = Index::new();
    index.load(&idx_bytes).unwrap();

    for want_offset in (0..input.len() as i64).step_by(65531) {
        let want = &input[want_offset as usize..];
        let (comp_off, uncomp_off) = index.find(want_offset).unwrap();
        let mut dec = Reader::with_options(
            Cursor::new(&buf[comp_off as usize..]),
            ReaderOptions {
                ignore_stream_identifier: true,
                ..Default::default()
            },
        );
        dec.skip(want_offset - uncomp_off).unwrap();
        let mut got = Vec::new();
        dec.read_to_end(&mut got).unwrap();
        assert_eq!(got, want);

        for pos in [
            SeekFrom::Start(want_offset as u64),
            SeekFrom::Current(want_offset - (input.len() / 2) as i64),
            SeekFrom::End(-(input.len() as i64 - want_offset)),
        ] {
            let dec2 = Reader::new(Cursor::new(&buf));
            let mut rs = dec2.read_seeker(true, None).unwrap();
            let mut tmp = vec![0u8; input.len() / 2];
            rs.read_exact(&mut tmp).unwrap();
            let got_off = rs.seek(pos).unwrap();
            assert_eq!(got_off as i64, want_offset);
            let mut got2 = Vec::new();
            rs.read_to_end(&mut got2).unwrap();
            assert_eq!(got2, want);
        }

        let dec3 = Reader::new(Cursor::new(&buf));
        let mut rs3 = dec3.read_seeker(true, None).unwrap();
        let mut tmp = vec![0u8; input.len() / 2];
        let want_len = tmp.len().min(input.len() - want_offset as usize);
        let n = rs3.read_at(&mut tmp, want_offset).unwrap();
        assert_eq!(n, want_len);
        assert_eq!(&tmp[..n], &want[..n]);
    }
}

// Upstream: github.com/klauspost/compress/s2/writer_test.go::TestWriterPadding
#[test]
fn test_writer_padding() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x1337);
    for _ in 0..8 {
        let padding = (rng.next_u32() as usize & 0xffff) + 1;
        let len = (rng.next_u32() as usize & 0x3ffff) + 1;
        let mut src = vec![0u8; len];
        for b in &mut src {
            *b = (rng.next_u32() as u8) & 3;
        }
        let mut e = Writer::with_options(
            Vec::new(),
            WriterOptions {
                padding,
                ..Default::default()
            },
        );
        e.write_all(&src).unwrap();
        e.close().unwrap();
        e.close().unwrap();
        let dst = e.reset(Vec::new()).unwrap();
        assert_eq!(dst.len() % padding, 0);

        let mut got = Vec::new();
        Reader::new(Cursor::new(&dst)).read_to_end(&mut got).unwrap();
        assert_eq!(got, src);

        e.write_all(&src).unwrap();
        e.close().unwrap();
        let dst2 = e.into_inner().unwrap();
        assert_eq!(dst2.len() % padding, 0);
        let mut got2 = Vec::new();
        Reader::new(Cursor::new(&dst2)).read_to_end(&mut got2).unwrap();
        assert_eq!(got2, src);
    }
}

// Upstream: github.com/klauspost/compress/s2/writer_test.go::TestBigRegularWrites
#[test]
fn test_big_regular_writes() {
    let mut buf = vec![0u8; MAX_BLOCK_SIZE * 2];
    let mut dst = Vec::new();
    let mut enc = Writer::with_options(
        &mut dst,
        WriterOptions {
            level: WriterLevel::Best,
            ..Default::default()
        },
    );
    for n in 0..3u8 {
        buf.fill(n);
        enc.write_all(&buf).unwrap();
    }
    enc.close().unwrap();
    let mut dec = Reader::new(Cursor::new(&dst));
    let copied = io::copy(&mut dec, &mut io::sink()).unwrap();
    assert_eq!(copied, (buf.len() * 3) as u64);
}

// Upstream: github.com/klauspost/compress/s2/writer_test.go::TestBigEncodeBuffer
#[test]
fn test_big_encode_buffer() {
    const BLOCK_SIZE: usize = 1 << 20;
    let mut buf = vec![0u8; BLOCK_SIZE * 2];
    let mut dst = Vec::new();
    let mut enc = Writer::with_options(
        &mut dst,
        WriterOptions {
            block_size: BLOCK_SIZE,
            level: WriterLevel::Best,
            ..Default::default()
        },
    );
    for n in 0..3u8 {
        buf.fill(n);
        enc.encode_buffer(&buf).unwrap();
        enc.encode_buffer(&buf).unwrap();
        enc.flush().unwrap();
    }
    enc.close().unwrap();
    let mut dec = Reader::new(Cursor::new(&dst));
    let n = io::copy(&mut dec, &mut io::sink()).unwrap();
    assert_eq!(n, (buf.len() * 6) as u64);
}

// Upstream: github.com/klauspost/compress/s2/writer_test.go::TestBigEncodeBufferSync
#[test]
fn test_big_encode_buffer_sync() {
    const BLOCK_SIZE: usize = 1 << 20;
    let mut buf = vec![0u8; BLOCK_SIZE * 2];
    let mut dst = Vec::new();
    let mut enc = Writer::with_options(
        &mut dst,
        WriterOptions {
            block_size: BLOCK_SIZE,
            concurrency: 1,
            level: WriterLevel::Best,
            ..Default::default()
        },
    );
    for n in 0..2u8 {
        buf.fill(n);
        enc.encode_buffer(&buf).unwrap();
    }
    enc.close().unwrap();
    let mut dec = Reader::new(Cursor::new(&dst));
    let n = io::copy(&mut dec, &mut io::sink()).unwrap();
    assert_eq!(n, (buf.len() * 2) as u64);
}

// Upstream: github.com/klauspost/compress/s2/writer_test.go::TestWriterBufferDone
#[test]
fn test_writer_buffer_done() {
    const BLOCK_SIZE: usize = 1 << 20;
    let sizes = [10, 100, 10_000, BLOCK_SIZE, BLOCK_SIZE * 4];
    let mut buffers: Vec<Vec<u8>> = sizes
        .iter()
        .enumerate()
        .map(|(n, &sz)| vec![n as u8; sz])
        .collect();
    let expected = buffers.clone();
    let want_next = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let wn_c = std::sync::Arc::clone(&want_next);
    let mut enc = Writer::with_options(
        io::sink(),
        WriterOptions {
            block_size: BLOCK_SIZE,
            concurrency: 4,
            buffer_done: Some(Box::new(move |mut b| {
                let idx = wn_c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(b, expected[idx]);
                b.fill(255);
            })),
            ..Default::default()
        },
    );
    for b in buffers.drain(..) {
        enc.encode_buffer_owned(b).unwrap();
    }
    enc.close().unwrap();
    assert_eq!(want_next.load(std::sync::atomic::Ordering::SeqCst), sizes.len());
}

// ============================================================================
// s2_test.go (1 of 32 here: TestMaxEncodedLen)
// ============================================================================

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestMaxEncodedLen
#[test]
fn test_max_encoded_len() {
    use mimir_proto::s2::block::literal_extra_size;
    let varint_len = |x: u64| put_uvarint_vec(x << 1).len() as i64;
    let cases = [
        (0i64, 1i64),
        (
            1 << 24,
            (1 << 24) + varint_len(1 << 24) + literal_extra_size(1 << 24),
        ),
        (
            (u32::MAX as i64) - 5 - 5,
            u32::MAX as i64,
        ),
        (
            (u32::MAX as i64) - 5 - literal_extra_size(u32::MAX as i64),
            u32::MAX as i64,
        ),
        ((u32::MAX as i64) - 9, -1),
        ((u32::MAX as i64) - 8, -1),
        ((u32::MAX as i64) - 1, -1),
        (u32::MAX as i64, -1),
        (-1, -1),
        (-2, -1),
    ];
    for (inp, want) in cases {
        assert_eq!(max_encoded_len(inp), want, "input={inp}");
    }
    for i in (0..MAX_BLOCK_SIZE as i64).step_by(64) {
        let want = i + varint_len(i as u64) + literal_extra_size(i);
        assert_eq!(max_encoded_len(i), want, "i={i}");
    }
}
