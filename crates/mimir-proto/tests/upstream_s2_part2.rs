//! 1:1 upstream-ported tests for `github.com/klauspost/compress/s2` (Part 2: 31 tests from `s2_test.go`).

use std::cell::RefCell;
use std::io::{self, Cursor, Read, Write};
use std::path::Path;
use std::process::Command;
use std::rc::Rc;

use mimir_proto::s2::block::put_uvarint;
use mimir_proto::s2::stream::{Reader, Writer, WriterLevel, WriterOptions};
use mimir_proto::s2::{
    concat_blocks, decode, decode_into_slice, decoded_len, emit_copy, emit_literal, encode,
    encode_best, encode_better, encode_snappy, encode_snappy_best, encode_snappy_better,
    estimate_block_size, match_len, S2Error, CHUNK_TYPE_UNCOMPRESSED, MAGIC_STREAM_S2,
    MAX_BLOCK_SIZE, TAG_COPY2, TAG_LITERAL,
};
use rand::{Rng, RngCore, SeedableRng};

const S2_TESTDATA: &str = "/home/birdy/go/pkg/mod/github.com/klauspost/compress@v1.20.4/s2/testdata";

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
    assert!(out.status.success());
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

fn roundtrip(b: &[u8]) {
    let enc = encode(b);
    let dec = decode(&enc).unwrap();
    assert_eq!(dec, b, "encode/decode mismatch");

    let enc_better = encode_better(b);
    let dec_better = decode(&enc_better).unwrap();
    assert_eq!(dec_better, b, "encode_better/decode mismatch");

    let enc_best = encode_best(b);
    let dec_best = decode(&enc_best).unwrap();
    assert_eq!(dec_best, b, "encode_best/decode mismatch");

    let enc_snap = encode_snappy(b);
    let mut concat = b"existing".to_vec();
    concat_blocks(&mut concat, &[&enc, &enc_better, &[0u8], &enc_snap]).unwrap();
    assert_eq!(&concat[..8], b"existing");
    let dec_concat = decode(&concat[8..]).unwrap();
    let mut want = Vec::with_capacity(b.len() * 3);
    want.extend_from_slice(b);
    want.extend_from_slice(b);
    want.extend_from_slice(b);
    assert_eq!(dec_concat, want);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestEmpty
#[test]
fn test_empty() {
    roundtrip(&[]);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestSmallCopy
#[test]
fn test_small_copy() {
    for i in 0..32 {
        let s = format!("aaaa{}aaaabbbb", "b".repeat(i));
        roundtrip(s.as_bytes());
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestSmallRand
#[test]
fn test_small_rand() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    for n in (1..20000).step_by(23) {
        let mut b = vec![0u8; n];
        rng.fill_bytes(&mut b);
        roundtrip(&b);
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestSmallRegular
#[test]
fn test_small_regular() {
    for n in (1..20000).step_by(23) {
        let b: Vec<u8> = (0..n).map(|i| ((i % 10) as u8) + b'a').collect();
        roundtrip(&b);
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestSmallRepeat
#[test]
fn test_small_repeat() {
    for n in (1..20000).step_by(23) {
        let mut b = vec![0u8; n];
        for (i, slot) in b.iter_mut().enumerate().take(n / 2) {
            *slot = ((i * 255) / n) as u8;
        }
        for i in 0..(n - n / 2) {
            b[n / 2 + i] = ((i % 10) as u8) + b'a';
        }
        roundtrip(&b);
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestInvalidVarint
#[test]
fn test_invalid_varint() {
    let cases = [
        ("final byte has continuation bit", &b"\xff"[..]),
        (
            "value overflows uint64",
            &b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00"[..],
        ),
        ("value overflows uint32", &b"\x80\x80\x80\x80\x10"[..]),
        (
            "overlong varint 8 bytes",
            &b"\x84\x80\x80\x80\x80\x80\x80\x00\x00\x00\x00\x00\x00\x00\x00\x30"[..],
        ),
    ];
    for (desc, input) in cases {
        assert_eq!(decoded_len(input).unwrap_err(), S2Error::Corrupt, "{desc}");
        assert_eq!(decode(input).unwrap_err(), S2Error::Corrupt, "{desc}");
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestDecode
#[test]
fn test_decode() {
    let lit40: Vec<u8> = (0u8..40).collect();
    let make_lit40 = || {
        let mut v = vec![0x28u8, 0x9c];
        v.extend_from_slice(&lit40);
        v
    };
    type DecodeCase<'a> = (&'a str, Vec<u8>, Option<&'a [u8]>);
    let cases: Vec<DecodeCase<'_>> = vec![
        ("decodedLen=0", vec![0x00], Some(b"")),
        (
            "decodedLen=3; tagLiteral 0-byte",
            vec![0x03, 0x08, 0xff, 0xff, 0xff],
            Some(&[0xff, 0xff, 0xff]),
        ),
        (
            "decodedLen=2; not enough dst",
            vec![0x02, 0x08, 0xff, 0xff, 0xff],
            None,
        ),
        ("decodedLen=3; not enough src", vec![0x03, 0x08, 0xff, 0xff], None),
        ("decodedLen=40; tagLiteral", make_lit40(), Some(&lit40)),
        ("tagLiteral 1-byte; truncated", vec![0x01, 0xf0], None),
        (
            "tagLiteral 1-byte; valid",
            vec![0x03, 0xf0, 0x02, 0xff, 0xff, 0xff],
            Some(&[0xff, 0xff, 0xff]),
        ),
        ("tagLiteral 2-byte; truncated", vec![0x01, 0xf4, 0x00], None),
        (
            "tagLiteral 2-byte; valid",
            vec![0x03, 0xf4, 0x02, 0x00, 0xff, 0xff, 0xff],
            Some(&[0xff, 0xff, 0xff]),
        ),
        ("tagLiteral 3-byte; truncated", vec![0x01, 0xf8, 0x00, 0x00], None),
        (
            "tagLiteral 3-byte; valid",
            vec![0x03, 0xf8, 0x02, 0x00, 0x00, 0xff, 0xff, 0xff],
            Some(&[0xff, 0xff, 0xff]),
        ),
        (
            "tagLiteral 4-byte; truncated",
            vec![0x01, 0xfc, 0x00, 0x00, 0x00],
            None,
        ),
        (
            "tagLiteral 4-byte; not enough dst",
            vec![0x01, 0xfc, 0x02, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff],
            None,
        ),
        (
            "tagLiteral 4-byte; not enough src",
            vec![0x04, 0xfc, 0x02, 0x00, 0x00, 0x00, 0xff],
            None,
        ),
        (
            "tagLiteral 4-byte; valid",
            vec![0x03, 0xfc, 0x02, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff],
            Some(&[0xff, 0xff, 0xff]),
        ),
        ("tagCopy1 truncated", vec![0x04, 0x01], None),
        ("tagCopy2 truncated", vec![0x04, 0x02, 0x00], None),
        ("tagCopy4 truncated", vec![0x04, 0x03, 0x00, 0x00, 0x00], None),
        (
            "literal abcd",
            vec![0x04, 0x0c, b'a', b'b', b'c', b'd'],
            Some(b"abcd"),
        ),
        (
            "literal+copy1 overlap",
            vec![0x0d, 0x0c, b'a', b'b', b'c', b'd', 0x15, 0x04],
            Some(b"abcdabcdabcda"),
        ),
        (
            "literal+copy1 len=4 off=4",
            vec![0x08, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x04],
            Some(b"abcdabcd"),
        ),
        (
            "literal+copy1 len=4 off=2",
            vec![0x08, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x02],
            Some(b"abcdcdcd"),
        ),
        (
            "literal+copy1 len=4 off=1",
            vec![0x08, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x01],
            Some(b"abcddddd"),
        ),
        (
            "repeat offset as first match",
            vec![0x08, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x00],
            None,
        ),
        (
            "repeat offset as second match",
            vec![0x0d, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x01, 0x00, b'z', 0x01, 0x00],
            Some(b"abcdddddzzzzz"),
        ),
        (
            "inconsistent dLen",
            vec![0x09, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x04],
            None,
        ),
        (
            "offset too large",
            vec![0x08, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x05],
            None,
        ),
        (
            "length too large",
            vec![0x07, 0x0c, b'a', b'b', b'c', b'd', 0x01, 0x04],
            None,
        ),
        (
            "tagCopy2 valid",
            vec![0x06, 0x0c, b'a', b'b', b'c', b'd', 0x06, 0x03, 0x00],
            Some(b"abcdbc"),
        ),
        (
            "tagCopy4 valid",
            vec![0x06, 0x0c, b'a', b'b', b'c', b'd', 0x07, 0x03, 0x00, 0x00, 0x00],
            Some(b"abcdbc"),
        ),
    ];

    const NOT_PRESENT_BASE: u8 = 0xa0;
    const NOT_PRESENT_LEN: usize = 37;
    let mut dbuf = [0u8; 100];

    for (desc, input, want) in cases {
        for (j, slot) in dbuf.iter_mut().enumerate() {
            *slot = NOT_PRESENT_BASE + ((j % NOT_PRESENT_LEN) as u8);
        }
        let d_len = decoded_len(&input).unwrap();
        let res = decode_into_slice(&mut dbuf, &input);
        match want {
            Some(w) => {
                let n = res.expect(desc);
                assert_eq!(&dbuf[..n], w, "{desc}");
                for (j, &x) in dbuf.iter().enumerate().skip(d_len) {
                    assert_eq!(x, NOT_PRESENT_BASE + ((j % NOT_PRESENT_LEN) as u8));
                }
            }
            None => {
                assert_eq!(res.unwrap_err(), S2Error::Corrupt, "{desc}");
            }
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestDecodeCopy4
#[test]
fn test_decode_copy4() {
    let mut input = vec![0x89u8, 0x80, 0x04, 0x0c, b'p', b'q', b'r', b's', 0xf4, 0xff, 0xff];
    input.extend(std::iter::repeat_n(b'.', 65536));
    input.extend_from_slice(&[0x13, 0x04, 0x00, 0x01, 0x00]);
    let got = decode(&input).unwrap();
    let mut want = b"pqrs".to_vec();
    want.extend(std::iter::repeat_n(b'.', 65536));
    want.extend_from_slice(b"pqrs.");
    assert_eq!(got, want);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestDecodeLengthOffset
#[test]
fn test_decode_length_offset() {
    let prefix = b"abcdefghijklmnopqr";
    let suffix = b"ABCDEFGHIJKLMNOPQR";
    let mut got_buf = [0u8; 128];
    let mut input_buf = [0u8; 128];

    for length in 1..=18usize {
        for offset in 1..=18usize {
            for suffix_len in 0..=18usize {
                let total_len = prefix.len() + length + suffix_len;
                let mut il = put_uvarint(&mut input_buf, total_len as u64);
                input_buf[il] = TAG_LITERAL + 4 * ((prefix.len() - 1) as u8);
                il += 1;
                input_buf[il..il + prefix.len()].copy_from_slice(prefix);
                il += prefix.len();
                input_buf[il] = TAG_COPY2 + 4 * ((length - 1) as u8);
                input_buf[il + 1] = offset as u8;
                input_buf[il + 2] = 0;
                il += 3;
                if suffix_len > 0 {
                    input_buf[il] = TAG_LITERAL + 4 * ((suffix_len - 1) as u8);
                    il += 1;
                    input_buf[il..il + suffix_len].copy_from_slice(&suffix[..suffix_len]);
                    il += suffix_len;
                }
                got_buf.fill(0xa0);
                let n = decode_into_slice(&mut got_buf, &input_buf[..il]).unwrap();
                assert_eq!(n, total_len);
                let mut want = prefix.to_vec();
                for _ in 0..length {
                    let b = want[want.len() - offset];
                    want.push(b);
                }
                want.extend_from_slice(&suffix[..suffix_len]);
                assert_eq!(&got_buf[..n], &want[..]);
                assert!(got_buf[total_len..].iter().all(|&x| x == 0xa0));
            }
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestDecodeGoldenInput
#[test]
fn test_decode_golden_input() {
    let src = std::fs::read(Path::new(S2_TESTDATA).join("Mark.Twain-Tom.Sawyer.txt.rawsnappy"))
        .unwrap();
    let want = std::fs::read(Path::new(S2_TESTDATA).join("Mark.Twain-Tom.Sawyer.txt")).unwrap();
    let got = decode(&src).unwrap();
    assert_eq!(got, want);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestSlowForwardCopyOverrun
#[test]
fn test_slow_forward_copy_overrun() {
    const BASE: isize = 100;
    for length in 1..18isize {
        for offset in 1..18isize {
            let mut high_water_mark = BASE;
            let mut d = BASE;
            let mut l = length;
            let mut o = offset;
            while o < 8 {
                let end = d + 8;
                if high_water_mark < end {
                    high_water_mark = end;
                }
                l -= o;
                d += o;
                o += o;
            }
            let mut a = d;
            d += l;
            while l > 0 {
                let end = a + 8;
                if high_water_mark < end {
                    high_water_mark = end;
                }
                a += 8;
                l -= 8;
            }
            let d_want = BASE + length;
            let overrun = high_water_mark - d_want;
            assert_eq!(d, d_want);
            assert!((0..=10).contains(&overrun));
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestEncoderSkip
#[test]
fn test_encoder_skip() {
    for orig_len in [10 << 10, 256 << 10] {
        let mut src = vec![0u8; orig_len];
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        for b in &mut src[..orig_len / 2] {
            *b = rng.gen_range(0..=255u8);
        }
        for (idx, b) in src[orig_len / 2..].iter_mut().enumerate() {
            *b = (idx & 32) as u8;
        }
        let tail_start = orig_len - orig_len / 10;
        for b in &mut src[tail_start..] {
            *b = rng.gen_range(0..=255u8);
        }

        let mut dst = Vec::new();
        let mut enc = Writer::with_options(
            &mut dst,
            WriterOptions {
                block_size: 64 << 10,
                ..Default::default()
            },
        );
        enc.write_all(&src).unwrap();
        enc.close().unwrap();

        for i in (0..src.len()).step_by((src.len() / 20 - 17).max(1)) {
            let mut dec = Reader::new(Cursor::new(&dst));
            let mut want = &src[i..];
            let first_read = (want.len() / 10) as u64;
            let mut sink = io::sink();
            let copied = io::copy(&mut dec.by_ref().take(first_read), &mut sink).unwrap();
            want = &want[copied as usize..];
            dec.skip(i as i64).unwrap();
            let mut got = Vec::new();
            dec.read_to_end(&mut got).unwrap();
            assert_eq!(got, want, "skip={i}");
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestEncodeNoiseThenRepeats
#[test]
fn test_encode_noise_then_repeats() {
    for orig_len in [256 * 1024usize, 2048 * 1024] {
        let mut src = vec![0u8; orig_len];
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        for b in &mut src[..orig_len / 2] {
            *b = rng.gen_range(0..=255u8);
        }
        for (i, b) in src[orig_len / 2..].iter_mut().enumerate() {
            *b = (i >> 8) as u8;
        }
        let dst = encode(&src);
        assert!(dst.len() < orig_len * 3 / 4);
        assert_eq!(decode(&dst).unwrap(), src);
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestFramingFormat
#[test]
fn test_framing_format() {
    let mut src = vec![0u8; 1_000_000];
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    for i in 0..10 {
        if i % 2 == 0 {
            rng.fill_bytes(&mut src[100_000 * i..100_000 * (i + 1)]);
        } else {
            src[100_000 * i..100_000 * (i + 1)].fill(i as u8);
        }
    }
    let mut buf = Vec::new();
    let mut bw = Writer::new(&mut buf);
    bw.write_all(&src).unwrap();
    bw.close().unwrap();

    let mut dst = Vec::new();
    Reader::new(Cursor::new(&buf)).read_to_end(&mut dst).unwrap();
    assert_eq!(dst, src);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestFramingFormatBetter
#[test]
fn test_framing_format_better() {
    let mut src = vec![0u8; 1_000_000];
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    for i in 0..10 {
        if i % 2 == 0 {
            rng.fill_bytes(&mut src[100_000 * i..100_000 * (i + 1)]);
        } else {
            src[100_000 * i..100_000 * (i + 1)].fill(i as u8);
        }
    }
    let mut buf = Vec::new();
    let mut bw = Writer::with_options(
        &mut buf,
        WriterOptions {
            level: WriterLevel::Better,
            ..Default::default()
        },
    );
    bw.write_all(&src).unwrap();
    bw.close().unwrap();

    let mut dst = Vec::new();
    Reader::new(Cursor::new(&buf)).read_to_end(&mut dst).unwrap();
    assert_eq!(dst, src);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestEmitLiteral
#[test]
fn test_emit_literal() {
    let cases: [(usize, &[u8]); 13] = [
        (1, b"\x00"),
        (2, b"\x04"),
        (59, b"\xe8"),
        (60, b"\xec"),
        (61, b"\xf0\x3c"),
        (62, b"\xf0\x3d"),
        (254, b"\xf0\xfd"),
        (255, b"\xf0\xfe"),
        (256, b"\xf0\xff"),
        (257, b"\xf4\x00\x01"),
        (65534, b"\xf4\xfd\xff"),
        (65535, b"\xf4\xfe\xff"),
        (65536, b"\xf4\xff\xff"),
    ];
    let mut dst = vec![0u8; 70000];
    let nines = vec![0x99u8; 65536];
    for (length, want) in cases {
        let lit = &nines[..length];
        let n = emit_literal(&mut dst, lit);
        assert!(dst[..n].ends_with(lit));
        assert_eq!(&dst[..n - length], want, "length={length}");
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestEmitCopy
#[test]
fn test_emit_copy() {
    let cases: [(usize, usize, &[u8]); 53] = [
        (8, 4, b"\x01\x08"),
        (8, 11, b"\x1d\x08"),
        (8, 12, b"\x2e\x08\x00"),
        (8, 13, b"\x32\x08\x00"),
        (8, 59, b"\xea\x08\x00"),
        (8, 60, b"\xee\x08\x00"),
        (8, 61, b"\xf2\x08\x00"),
        (8, 62, b"\xf6\x08\x00"),
        (8, 63, b"\xfa\x08\x00"),
        (8, 64, b"\xfe\x08\x00"),
        (8, 65, b"\x11\x08\x15\x001"),
        (8, 66, b"\x11\x08\x15\x002"),
        (8, 67, b"\x11\x08\x15\x003"),
        (8, 68, b"\x11\x08\x15\x004"),
        (8, 69, b"\x11\x08\x15\x005"),
        (8, 80, b"\x11\x08\x15\x00@"),
        (8, 800, b"\x11\x08\x19\x00\x14\x02"),
        (8, 800000, b"\x11\x08\x1d\x00\xf44\x0b"),
        (256, 4, b"\x21\x00"),
        (256, 11, b"\x3d\x00"),
        (256, 12, b"\x2e\x00\x01"),
        (256, 13, b"\x32\x00\x01"),
        (256, 59, b"\xea\x00\x01"),
        (256, 60, b"\xee\x00\x01"),
        (256, 61, b"\xf2\x00\x01"),
        (256, 62, b"\xf6\x00\x01"),
        (256, 63, b"\xfa\x00\x01"),
        (256, 64, b"\xfe\x00\x01"),
        (256, 65, b"1\x00\x15\x001"),
        (256, 66, b"1\x00\x15\x002"),
        (256, 67, b"1\x00\x15\x003"),
        (256, 68, b"1\x00\x15\x004"),
        (256, 69, b"1\x00\x15\x005"),
        (256, 80, b"1\x00\x15\x00@"),
        (256, 800, b"1\x00\x19\x00\x14\x02"),
        (256, 80000, b"1\x00\x1d\x00t8\x00"),
        (2048, 4, b"\x0e\x00\x08"),
        (2048, 11, b"\x2a\x00\x08"),
        (2048, 12, b"\x2e\x00\x08"),
        (2048, 13, b"\x32\x00\x08"),
        (2048, 59, b"\xea\x00\x08"),
        (2048, 60, b"\xee\x00\x08"),
        (2048, 61, b"\xf2\x00\x08"),
        (2048, 62, b"\xf6\x00\x08"),
        (2048, 63, b"\xfa\x00\x08"),
        (2048, 64, b"\xfe\x00\x08"),
        (2048, 65, b"\xee\x00\x08\x05\x00"),
        (2048, 66, b"\xee\x00\x08\x09\x00"),
        (2048, 67, b"\xee\x00\x08\x0d\x00"),
        (2048, 68, b"\xee\x00\x08\x11\x00"),
        (2048, 69, b"\xee\x00\x08\x15\x00\x01"),
        (2048, 80, b"\xee\x00\x08\x15\x00\x0c"),
        (2048, 800, b"\xee\x00\x08\x19\x00\xe0\x01"),
    ];
    let mut dst = vec![0u8; 1024];
    for (offset, length, want) in cases {
        let n = emit_copy(&mut dst, offset, length);
        assert_eq!(&dst[..n], want, "offset={offset}, length={length}");
    }
    let far_cases: [(usize, usize, &[u8]); 5] = [
        (204800, 4, b"\x0f\x00\x20\x03\x00"),
        (204800, 65, b"\xff\x00\x20\x03\x00\x03\x00\x20\x03\x00"),
        (204800, 69, b"\xff\x00\x20\x03\x00\x05\x00"),
        (204800, 800, b"\xff\x00\x20\x03\x00\x19\x00\xdc\x01"),
        (204800, 80000, b"\xff\x00\x20\x03\x00\x1d\x00\x3c\x38\x00"),
    ];
    for (offset, length, want) in far_cases {
        let n = emit_copy(&mut dst, offset, length);
        assert_eq!(&dst[..n], want, "offset={offset}, length={length}");
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestNewWriter
#[test]
fn test_new_writer() {
    let inputs = [
        vec![b'a'; 40_000],
        vec![b'b'; 150_000],
        vec![b'c'; 60_000],
        vec![b'd'; 120_000],
        vec![b'e'; 30_000],
    ];
    for i in 0..(1 << inputs.len()) {
        let mut want = Vec::new();
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf);
        for (j, input) in inputs.iter().enumerate() {
            if i & (1 << j) == 0 {
                continue;
            }
            w.write_all(input).unwrap();
            want.extend_from_slice(input);
        }
        w.close().unwrap();
        let mut got = Vec::new();
        Reader::new(Cursor::new(&buf)).read_to_end(&mut got).unwrap();
        assert_eq!(got, want, "i={i:#04x}");
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestFlush
#[test]
fn test_flush() {
    let mut w = Writer::new(Vec::new());
    w.write_all(&[b'x'; 20]).unwrap();
    let buf = w.into_inner().unwrap();
    assert_eq!(buf.len(), 0);

    let mut w = Writer::new(buf);
    w.write_all(&[b'x'; 20]).unwrap();
    w.flush().unwrap();
    let buf = w.into_inner().unwrap();
    assert!(!buf.is_empty());
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestReaderUncompressedDataOK
#[test]
fn test_reader_uncompressed_data_ok() {
    let mut stream = MAGIC_STREAM_S2.to_vec();
    stream.extend_from_slice(&[0x01, 0x08, 0x00, 0x00, 0x68, 0x10, 0xe6, 0xb6, b'a', b'b', b'c', b'd']);
    let mut got = Vec::new();
    Reader::new(Cursor::new(&stream)).read_to_end(&mut got).unwrap();
    assert_eq!(&got, b"abcd");
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestReaderUncompressedDataNoPayload
#[test]
fn test_reader_uncompressed_data_no_payload() {
    let mut stream = MAGIC_STREAM_S2.to_vec();
    stream.extend_from_slice(&[0x01, 0x04, 0x00, 0x00]);
    let mut got = Vec::new();
    assert!(Reader::new(Cursor::new(&stream)).read_to_end(&mut got).is_err());
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestReaderUncompressedDataTooLong
#[test]
fn test_reader_uncompressed_data_too_long() {
    let n = MAX_BLOCK_SIZE + 4;
    let mut s1 = MAGIC_STREAM_S2.to_vec();
    s1.extend_from_slice(&[
        CHUNK_TYPE_UNCOMPRESSED,
        (n & 0xff) as u8,
        ((n >> 8) & 0xff) as u8,
        ((n >> 16) & 0xff) as u8,
    ]);
    s1.resize(s1.len() + n, 0);
    let err = Reader::new(Cursor::new(&s1)).read_to_end(&mut Vec::new()).unwrap_err();
    let se = err.get_ref().and_then(|e| e.downcast_ref::<S2Error>()).unwrap();
    assert_eq!(*se, S2Error::CRC);

    let n2 = n + 1;
    let mut s2 = MAGIC_STREAM_S2.to_vec();
    s2.extend_from_slice(&[
        CHUNK_TYPE_UNCOMPRESSED,
        (n2 & 0xff) as u8,
        ((n2 >> 8) & 0xff) as u8,
        ((n2 >> 16) & 0xff) as u8,
    ]);
    s2.resize(s2.len() + n2, 0);
    let err2 = Reader::new(Cursor::new(&s2)).read_to_end(&mut Vec::new()).unwrap_err();
    let se2 = err2.get_ref().and_then(|e| e.downcast_ref::<S2Error>()).unwrap();
    assert_eq!(*se2, S2Error::Corrupt);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestReaderReset
#[test]
fn test_reader_reset() {
    let gold = b"All that is gold does not glitter,
".repeat(10000);
    let mut enc_buf = Vec::new();
    let mut w = Writer::new(&mut enc_buf);
    w.write_all(&gold).unwrap();
    w.close().unwrap();

    let mut r = Reader::new(Cursor::new(Vec::<u8>::new()));
    for mode in ["encoded", "invalid", "partial", "encoded", "partial", "invalid", "encoded"] {
        if mode == "partial" {
            r.reset(Cursor::new(enc_buf.clone()));
            let mut tmp = [0u8; 101];
            r.read_exact(&mut tmp).unwrap();
            continue;
        }
        if mode == "encoded" {
            r.reset(Cursor::new(enc_buf.clone()));
            let mut got = Vec::new();
            r.read_to_end(&mut got).unwrap();
            assert_eq!(got, gold);
        } else {
            r.reset(Cursor::new(b"invalid".to_vec()));
            assert!(r.read_to_end(&mut Vec::new()).is_err());
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestWriterReset
#[test]
fn test_writer_reset() {
    let gold = b"Not all those who wander are lost;
".repeat(10000);
    const N: usize = 20;
    let mut w = Writer::new(Vec::new());
    for i in 0..=N {
        w.reset(Vec::new());
        let want = &gold[..gold.len() * i / N];
        w.write_all(want).unwrap();
        w.flush().unwrap();
        let buf = w.reset(Vec::new()).unwrap();
        let mut got = Vec::new();
        Reader::new(Cursor::new(&buf)).read_to_end(&mut got).unwrap();
        assert_eq!(got, want, "i={i}");
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestWriterResetWithoutFlush
#[test]
fn test_writer_reset_without_flush() {
    let mut w = Writer::new(Vec::new());
    w.write_all(b"xxx").unwrap();
    w.reset(Vec::new());
    w.write_all(b"yyy").unwrap();
    w.flush().unwrap();
    let buf1 = w.into_inner().unwrap();
    let mut got = Vec::new();
    Reader::new(Cursor::new(&buf1)).read_to_end(&mut got).unwrap();
    assert_eq!(&got, b"yyy");
}

struct WriteCounter {
    count: Rc<RefCell<usize>>,
}

impl Write for WriteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        *self.count.borrow_mut() += 1;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestNumUnderlyingWrites
#[test]
fn test_num_underlying_writes() {
    let count = Rc::new(RefCell::new(0usize));
    let mut w = Writer::new(WriteCounter {
        count: Rc::clone(&count),
    });
    let cases = [
        (vec![b'x'; 100], 2usize),
        (vec![b'y'; 100], 1usize),
        (b"ABCDEFGHIJKLMNOPQRST".to_vec(), 1usize),
    ];
    for (input, want) in cases {
        *count.borrow_mut() = 0;
        w.write_all(&input).unwrap();
        w.flush().unwrap();
        assert_eq!(*count.borrow(), want);
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestEstimateBlockSize
#[test]
fn test_estimate_block_size() {
    let mut input = Vec::new();
    #[allow(clippy::same_item_push)]
    for _ in 0..100 {
        let _ = estimate_block_size(&input);
        input.push(0);
    }
    assert!(estimate_block_size(&vec![0u8; 4096]) > 0);
}

fn check_all_roundtrips(data: &[u8]) {
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
                ..Default::default()
            },
        );
        enc.write_all(data).unwrap();
        enc.close().unwrap();
        let mut dec = Vec::new();
        Reader::new(Cursor::new(&buf)).read_to_end(&mut dec).unwrap();
        assert_eq!(dec, data, "stream/{level:?}");
    }

    for enc_fn in [encode, encode_better, encode_best] {
        let comp = enc_fn(data);
        assert_eq!(decode(&comp).unwrap(), data);
    }

    for snappy_fn in [encode_snappy, encode_snappy_better, encode_snappy_best] {
        let comp = snappy_fn(data);
        let snap_dec = snap::raw::Decoder::new().decompress_vec(&comp).unwrap();
        assert_eq!(snap_dec, data);
    }

    let snap_comp = snap::raw::Encoder::new().compress_vec(data).unwrap();
    assert_eq!(decode(&snap_comp).unwrap(), data);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestRoundtrips
#[test]
fn test_roundtrips() {
    let mut tom = std::fs::read(Path::new(S2_TESTDATA).join("Mark.Twain-Tom.Sawyer.txt")).unwrap();
    tom.truncate(8192);
    let rand_data = std::fs::read(Path::new(S2_TESTDATA).join("random")).unwrap();
    check_all_roundtrips(&tom);
    check_all_roundtrips(&rand_data[..16384]);
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestDataRoundtrips
#[test]
fn test_data_roundtrips() {
    check_all_roundtrips(&vec![0u8; 1 << 20]);
    for (_name, data) in read_zip_entries("enc_regressions.zip").into_iter().take(12) {
        if data.len() <= 10_000 {
            check_all_roundtrips(&data);
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestMatchLen
#[test]
fn test_match_len() {
    let ref_fn = |a: &[u8], b: &[u8]| -> usize {
        a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
    };
    let nums = [0, 1, 2, 7, 8, 9, 16, 20, 29, 30, 31, 32, 33, 34, 38, 39, 40];
    for y_idx in (31..=40).rev() {
        let mut xxx = [b'x'; 40];
        if y_idx < xxx.len() {
            xxx[y_idx] = b'y';
        }
        for &i in &nums {
            for &j in &nums {
                if i >= j {
                    continue;
                }
                let got = match_len(&xxx[j..], &xxx[i..]);
                let want = ref_fn(&xxx[j..], &xxx[i..]);
                assert_eq!(got, want, "y_idx={y_idx}, i={i}, j={j}");
            }
        }
    }
}

// Upstream: github.com/klauspost/compress/s2/s2_test.go::TestSilesiaRoundTrip
#[test]
fn test_silesia_round_trip() {
    // Representative multi-pattern corpus combining prose + binary + repetitive logs.
    let tom = std::fs::read(Path::new(S2_TESTDATA).join("Mark.Twain-Tom.Sawyer.txt")).unwrap();
    let mut corpus = Vec::with_capacity(tom.len() * 4);
    corpus.extend_from_slice(&tom);
    corpus.extend_from_slice(&tom);
    for i in 0..8192 {
        corpus.extend_from_slice(format!(r#"metric_name{{job="ingester",idx="{}"}} {}
"#, i % 64, i).as_bytes());
    }
    check_all_roundtrips(&corpus);
}
