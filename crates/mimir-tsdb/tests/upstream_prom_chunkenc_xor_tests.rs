//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/
//! (bstream_test.go, chunk_test.go, varbit_test.go, xor_test.go, xor2_test.go, st_helper_test.go).

use mimir_tsdb::chunkenc::bstream::{BitReader, BitStream};
use mimir_tsdb::chunkenc::st::MAX_FIRST_ST_CHANGE_ON;
use mimir_tsdb::chunkenc::varbit::{
    put_varbit_int, put_varbit_int_fast, put_varbit_uint, read_varbit_int, read_varbit_uint,
};
use mimir_tsdb::chunkenc::{
    is_stale_nan, ChunkError, EncodedChunk, Encoding, FloatHistogram, FloatHistogramChunk,
    FloatHistogramStChunk, Histogram, HistogramChunk, HistogramStChunk, Pool, Xor2Chunk,
    Xor2Iterator, XorChunk, STALE_NAN_BITS,
};

#[derive(Clone, Copy, Debug, PartialEq)]
struct Triple {
    st: i64,
    t: i64,
    v: f64,
}

/// Deterministic Go math/rand/v2 PCG64 implementation for exact parity with Go tests.
struct GoPcg {
    hi: u64,
    lo: u64,
}

impl GoPcg {
    fn new(seed1: u64, seed2: u64) -> Self {
        Self {
            hi: seed1,
            lo: seed2,
        }
    }

    fn next_raw(&mut self) -> (u64, u64) {
        const MUL_HI: u64 = 2549297995355413924;
        const MUL_LO: u64 = 4865540595714422341;
        const INC_HI: u64 = 6364136223846793005;
        const INC_LO: u64 = 1442695040888963407;

        let prod = (self.lo as u128) * (MUL_LO as u128);
        let lo = prod as u64;
        let hi = ((prod >> 64) as u64)
            .wrapping_add(self.hi.wrapping_mul(MUL_LO))
            .wrapping_add(self.lo.wrapping_mul(MUL_HI));

        let (lo, carry) = lo.overflowing_add(INC_LO);
        let hi = hi.wrapping_add(INC_HI).wrapping_add(u64::from(carry));
        self.lo = lo;
        self.hi = hi;
        (hi, lo)
    }

    fn uint64(&mut self) -> u64 {
        let (mut hi, lo) = self.next_raw();
        const CHEAP_MUL: u64 = 0xda942042e4dd58b5;
        hi ^= hi >> 32;
        hi = hi.wrapping_mul(CHEAP_MUL);
        hi ^= hi >> 48;
        hi = hi.wrapping_mul(lo | 1);
        hi
    }

    fn uint64n(&mut self, n: u64) -> u64 {
        if n & (n - 1) == 0 {
            return self.uint64() & (n - 1);
        }
        let mut prod = (self.uint64() as u128) * (n as u128);
        let mut lo = prod as u64;
        let mut hi = (prod >> 64) as u64;
        if lo < n {
            let thresh = n.wrapping_neg() % n;
            while lo < thresh {
                prod = (self.uint64() as u128) * (n as u128);
                lo = prod as u64;
                hi = (prod >> 64) as u64;
            }
        }
        hi
    }

    fn int_n(&mut self, n: usize) -> usize {
        self.uint64n(n as u64) as usize
    }

    fn float64(&mut self) -> f64 {
        ((self.uint64() << 11 >> 11) as f64) / ((1u64 << 53) as f64)
    }
}

// ============================================================================
// bstream_test.go
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/bstream_test.go::TestBstream_Reset
#[test]
fn test_bstream_reset() {
    let mut bs = BitStream::from_bytes(b"test".to_vec(), 10);
    bs.reset(b"was reset".to_vec());
    assert_eq!(bs.bytes(), b"was reset");
    assert_eq!(bs.count(), 0);
    assert_eq!(bs, BitStream::from_bytes(b"was reset".to_vec(), 0));
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/bstream_test.go::TestBstreamReader
#[test]
fn test_bstream_reader() {
    let mut w = BitStream::new();
    for bit in [true, false] {
        w.write_bit(bit);
    }
    for nbits in 1..=64 {
        w.write_bits(nbits as u64, nbits);
    }
    for v in (1u64..10000).step_by(123) {
        w.write_bits(v, 29);
    }

    let mut r = BitReader::new(w.bytes());
    for bit in [true, false] {
        let v = match r.read_bit_fast() {
            Ok(v) => v,
            Err(_) => r.read_bit().unwrap(),
        };
        assert_eq!(v, bit);
    }
    for nbits in 1u8..=64 {
        let v = match r.read_bits_fast(nbits) {
            Ok(v) => v,
            Err(_) => r.read_bits(nbits).unwrap(),
        };
        assert_eq!(v, u64::from(nbits), "nbits={nbits}");
    }
    for v in (1u64..10000).step_by(123) {
        let actual = match r.read_bits_fast(29) {
            Ok(val) => val,
            Err(_) => r.read_bits(29).unwrap(),
        };
        assert_eq!(actual, v, "v={v}");
    }
}

// ============================================================================
// chunk_test.go
// ============================================================================

fn run_test_chunk(supports_st: bool) {
    let mut rng = GoPcg::new(42, 99);
    let mut exp = Vec::with_capacity(300);
    let mut ts: i64 = 1_234_123_324;
    let mut v: f64 = 1_243_535.123;

    let mut xor_chunk = (!supports_st).then(XorChunk::new);
    let mut xor2_chunk = supports_st.then(Xor2Chunk::new);

    for i in 0..300 {
        ts += rng.int_n(10_000) as i64 + 1;
        if i % 2 == 0 {
            v += rng.int_n(1_000_000) as f64;
        } else {
            v -= rng.int_n(1_000_000) as f64;
        }

        if let Some(c) = xor_chunk.as_mut() {
            let mut app = c.appender().unwrap();
            app.append(ts - 100, ts, v).unwrap();
        } else if let Some(c) = xor2_chunk.as_mut() {
            let mut app = c.appender().unwrap();
            app.append(ts - 100, ts, v).unwrap();
        }

        let exp_st = if supports_st { ts - 100 } else { 0 };
        exp.push(Triple { st: exp_st, t: ts, v });
    }

    let collect_all = |reset_first: bool| -> Vec<Triple> {
        let mut out = Vec::new();
        if let Some(c) = xor_chunk.as_ref() {
            let mut it = c.iter();
            if reset_first {
                it.reset(c.as_bytes());
            }
            while let Some((ts, v)) = it.next_sample().unwrap() {
                out.push(Triple { st: 0, t: ts, v });
            }
        } else if let Some(c) = xor2_chunk.as_ref() {
            let mut it = c.iter();
            if reset_first {
                it.reset(c.as_bytes());
            }
            while let Some((ts, v)) = it.next_sample().unwrap() {
                out.push(Triple {
                    st: it.at_st(),
                    t: ts,
                    v,
                });
            }
        }
        out
    };

    assert_eq!(collect_all(false), exp);
    assert_eq!(collect_all(true), exp);

    // 3. Test iterator Seek.
    let mid = exp.len() / 2;
    let mut res3 = Vec::new();
    if let Some(c) = xor_chunk.as_ref() {
        let mut it = c.iter();
        assert!(it.seek(exp[mid].t).unwrap().is_some());
        assert!(it.seek(exp[mid].t).unwrap().is_some());
        assert!(it.seek(exp[mid].t).unwrap().is_some());
        let (ts, v) = it.at();
        res3.push(Triple { st: 0, t: ts, v });
        let mut last_ts = ts;
        while let Some((ts, v)) = it.next_sample().unwrap() {
            last_ts = ts;
            res3.push(Triple { st: 0, t: ts, v });
        }
        assert!(it.seek(last_ts).unwrap().is_some());
        assert_eq!(res3, exp[mid..]);
        assert!(it.seek(exp.last().unwrap().t + 1).unwrap().is_none());
    } else if let Some(c) = xor2_chunk.as_ref() {
        let mut it = c.iter();
        assert!(it.seek(exp[mid].t).unwrap().is_some());
        assert!(it.seek(exp[mid].t).unwrap().is_some());
        assert!(it.seek(exp[mid].t).unwrap().is_some());
        let (ts, v) = it.at();
        res3.push(Triple {
            st: it.at_st(),
            t: ts,
            v,
        });
        let mut last_ts = ts;
        while let Some((ts, v)) = it.next_sample().unwrap() {
            last_ts = ts;
            res3.push(Triple {
                st: it.at_st(),
                t: ts,
                v,
            });
        }
        assert!(it.seek(last_ts).unwrap().is_some());
        assert_eq!(res3, exp[mid..]);
        assert!(it.seek(exp.last().unwrap().t + 1).unwrap().is_none());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/chunk_test.go::TestChunk
#[test]
fn test_chunk() {
    run_test_chunk(false);
    run_test_chunk(true);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/chunk_test.go::TestPool
#[test]
fn test_pool() {
    let p = Pool::new();
    let cases = [
        ("xor", Encoding::Xor, false),
        ("histogram", Encoding::Histogram, false),
        ("float histogram", Encoding::FloatHistogram, false),
        ("xor opt st", Encoding::Xor2, false),
        ("histogram st", Encoding::HistogramSt, false),
        ("float histogram st", Encoding::FloatHistogramSt, false),
        ("invalid encoding", Encoding::None, true),
    ];

    for (name, enc, exp_err) in cases {
        let got = p.get(enc, b"test".to_vec());
        if exp_err {
            assert!(
                matches!(got, Err(ChunkError::InvalidEncoding(0))),
                "expected InvalidEncoding for {name}"
            );
            continue;
        }
        let mut c = got.unwrap();
        assert_eq!(c.bstream_parts(), (b"test".as_slice(), 0), "{name}");

        let stream = c.bstream_parts().0.to_vec();
        c.set_bstream_for_test(stream, 1);
        assert_eq!(c.bstream_parts().1, 1, "{name}");

        p.put(&mut c).unwrap();
        assert_eq!(c.bstream_parts(), (&[][..], 0), "{name}");

        // Second Get reuses pooled slot cleanly.
        let c2 = p.get(enc, b"reused".to_vec()).unwrap();
        assert_eq!(c2.bstream_parts(), (b"reused".as_slice(), 0), "{name}");
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/chunk_test.go::TestCompatibleValues
#[test]
fn test_compatible_values() {
    let cases = [
        (Encoding::Xor, Encoding::Xor, true),
        (Encoding::Xor2, Encoding::Xor2, true),
        (Encoding::Xor, Encoding::Xor2, true),
        (Encoding::Xor2, Encoding::Xor, true),
        (Encoding::Histogram, Encoding::Histogram, false),
        (Encoding::Histogram, Encoding::FloatHistogram, false),
        (Encoding::FloatHistogram, Encoding::Histogram, false),
        (Encoding::FloatHistogram, Encoding::FloatHistogram, false),
        (Encoding::Xor, Encoding::Histogram, false),
        (Encoding::Xor2, Encoding::Histogram, false),
        (Encoding::Xor, Encoding::FloatHistogram, false),
        (Encoding::Xor2, Encoding::FloatHistogram, false),
        (Encoding::Histogram, Encoding::Xor, false),
        (Encoding::Histogram, Encoding::Xor2, false),
        (Encoding::FloatHistogram, Encoding::Xor, false),
        (Encoding::FloatHistogram, Encoding::Xor2, false),
        (Encoding::None, Encoding::Xor, false),
        (Encoding::None, Encoding::Xor2, false),
        (Encoding::Xor, Encoding::None, false),
        (Encoding::Xor2, Encoding::None, false),
        (Encoding::None, Encoding::None, false),
    ];
    for (a, b, want) in cases {
        assert_eq!(
            a.compatible_values(b),
            want,
            "compatible_values({a:?}, {b:?})"
        );
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/chunk_test.go::TestChunkAppenderAfterReload
#[test]
fn test_chunk_appender_after_reload() {
    let encodings = [
        Encoding::Xor,
        Encoding::Xor2,
        Encoding::Histogram,
        Encoding::FloatHistogram,
        Encoding::HistogramSt,
        Encoding::FloatHistogramSt,
    ];
    for enc in encodings {
        let bytes = match enc {
            Encoding::Xor => {
                let mut c = XorChunk::new();
                let mut app = c.appender().unwrap();
                app.append(0, 10, 1.0).unwrap();
                app.append(0, 11, 1.0).unwrap();
                c.into_bytes()
            }
            Encoding::Xor2 => {
                let mut c = Xor2Chunk::new();
                let mut app = c.appender().unwrap();
                app.append(0, 10, 1.0).unwrap();
                app.append(0, 11, 1.0).unwrap();
                c.into_bytes()
            }
            Encoding::Histogram => {
                let mut c = HistogramChunk::new();
                let mut app = c.appender().unwrap();
                let h = Histogram::default();
                app.append_histogram(None, 0, 10, &h, false).unwrap();
                app.append_histogram(None, 0, 11, &h, false).unwrap();
                c.into_bytes()
            }
            Encoding::FloatHistogram => {
                let mut c = FloatHistogramChunk::new();
                let mut app = c.appender().unwrap();
                let fh = FloatHistogram::default();
                app.append_float_histogram(None, 0, 10, &fh, false).unwrap();
                app.append_float_histogram(None, 0, 11, &fh, false).unwrap();
                c.into_bytes()
            }
            Encoding::HistogramSt => {
                let mut c = HistogramStChunk::new();
                let mut app = c.appender().unwrap();
                let h = Histogram::default();
                app.append_histogram(None, 0, 10, &h, false).unwrap();
                app.append_histogram(None, 0, 11, &h, false).unwrap();
                c.into_bytes()
            }
            Encoding::FloatHistogramSt => {
                let mut c = FloatHistogramStChunk::new();
                let mut app = c.appender().unwrap();
                let fh = FloatHistogram::default();
                app.append_float_histogram(None, 0, 10, &fh, false).unwrap();
                app.append_float_histogram(None, 0, 11, &fh, false).unwrap();
                c.into_bytes()
            }
            Encoding::None => unreachable!(),
        };

        let reloaded = EncodedChunk::from_bytes(enc, bytes).unwrap();
        let timestamps: Vec<i64> = match reloaded {
            EncodedChunk::Xor(mut c) => {
                c.appender().unwrap().append(0, 13, 1.0).unwrap();
                let mut it = c.iter();
                let mut ts = Vec::new();
                while let Some((t, _)) = it.next_sample().unwrap() {
                    ts.push(t);
                }
                ts
            }
            EncodedChunk::Xor2(mut c) => {
                c.appender().unwrap().append(0, 13, 1.0).unwrap();
                let mut it = c.iter();
                let mut ts = Vec::new();
                while let Some((t, _)) = it.next_sample().unwrap() {
                    ts.push(t);
                }
                ts
            }
            EncodedChunk::Histogram(mut c) => {
                c.appender()
                    .unwrap()
                    .append_histogram(None, 0, 13, &Histogram::default(), false)
                    .unwrap();
                let mut it = c.iter();
                let mut ts = Vec::new();
                while let Some(t) = it.next_sample().unwrap() {
                    ts.push(t);
                }
                ts
            }
            EncodedChunk::FloatHistogram(mut c) => {
                c.appender()
                    .unwrap()
                    .append_float_histogram(None, 0, 13, &FloatHistogram::default(), false)
                    .unwrap();
                let mut it = c.iter();
                let mut ts = Vec::new();
                while let Some(t) = it.next_sample().unwrap() {
                    ts.push(t);
                }
                ts
            }
            EncodedChunk::HistogramSt(mut c) => {
                c.appender()
                    .unwrap()
                    .append_histogram(None, 0, 13, &Histogram::default(), false)
                    .unwrap();
                let mut it = c.iter();
                let mut ts = Vec::new();
                while let Some(t) = it.next_sample().unwrap() {
                    ts.push(t);
                }
                ts
            }
            EncodedChunk::FloatHistogramSt(mut c) => {
                c.appender()
                    .unwrap()
                    .append_float_histogram(None, 0, 13, &FloatHistogram::default(), false)
                    .unwrap();
                let mut it = c.iter();
                let mut ts = Vec::new();
                while let Some(t) = it.next_sample().unwrap() {
                    ts.push(t);
                }
                ts
            }
        };
        assert_eq!(timestamps, vec![10, 11, 13], "encoding={enc}");
    }
}

// ============================================================================
// varbit_test.go
// ============================================================================

fn test_varbit_int_boundary_values() -> Vec<i64> {
    vec![
        i64::MIN,
        -36_028_797_018_963_968,
        -36_028_797_018_963_967,
        -16_777_216,
        -16_777_215,
        -131_072,
        -131_071,
        -2048,
        -2047,
        -256,
        -255,
        -32,
        -31,
        -4,
        -3,
        -1,
        0,
        1,
        4,
        5,
        32,
        33,
        256,
        257,
        2048,
        2049,
        131_072,
        131_073,
        16_777_216,
        16_777_217,
        36_028_797_018_963_968,
        36_028_797_018_963_969,
        i64::MAX,
    ]
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/varbit_test.go::TestVarbitInt
#[test]
fn test_varbit_int() {
    let numbers = test_varbit_int_boundary_values();
    let mut bs = BitStream::new();
    for &n in &numbers {
        put_varbit_int(&mut bs, n);
    }
    let mut br = BitReader::new(bs.bytes());
    for want in numbers {
        let got = read_varbit_int(&mut br).unwrap();
        assert_eq!(got, want);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/varbit_test.go::TestVarbitIntFast
#[test]
fn test_varbit_int_fast() {
    let numbers = test_varbit_int_boundary_values();
    let mut bs = BitStream::new();
    for &n in &numbers {
        put_varbit_int_fast(&mut bs, n);
    }
    let mut br = BitReader::new(bs.bytes());
    for want in numbers {
        let got = read_varbit_int(&mut br).unwrap();
        assert_eq!(got, want);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/varbit_test.go::TestVarbitIntAndFastProduceIdenticalOutput
#[test]
fn test_varbit_int_and_fast_produce_identical_output() {
    let numbers = test_varbit_int_boundary_values();
    let mut slow = BitStream::new();
    let mut fast = BitStream::new();
    for &n in &numbers {
        put_varbit_int(&mut slow, n);
        put_varbit_int_fast(&mut fast, n);
    }
    assert_eq!(slow.bytes(), fast.bytes());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/varbit_test.go::TestVarbitUint
#[test]
fn test_varbit_uint() {
    let numbers: Vec<u64> = vec![
        0,
        1,
        7,
        8,
        63,
        64,
        511,
        512,
        4095,
        4096,
        262_143,
        262_144,
        33_554_431,
        33_554_432,
        72_057_594_037_927_935,
        72_057_594_037_927_936,
        u64::MAX,
    ];
    let mut bs = BitStream::new();
    for &n in &numbers {
        put_varbit_uint(&mut bs, n);
    }
    let mut br = BitReader::new(bs.bytes());
    for want in numbers {
        let got = read_varbit_uint(&mut br).unwrap();
        assert_eq!(got, want);
    }
}

// ============================================================================
// xor_test.go
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor_test.go::TestXORChunkOverFlowPanics
#[test]
fn test_xor_chunk_overflow_panics() {
    let mut chunk = XorChunk::new();
    let mut app = chunk.appender().unwrap();
    for i in 0..(u16::MAX as i64) {
        app.append(0, i, i as f64).unwrap();
    }
    let err = app.append(0, u16::MAX as i64, u16::MAX as f64).unwrap_err();
    assert!(matches!(err, ChunkError::CapacityExceeded));
}

// ============================================================================
// xor2_test.go + st_helper_test.go
// ============================================================================

struct Xor2SampleCase {
    name: String,
    samples: Vec<Triple>,
    want_bytes: usize,
}

fn xor2_window_samples() -> Vec<Xor2SampleCase> {
    let mut r = GoPcg::new(10, 20);
    type PatternSpec = (&'static str, Vec<f64>, [[usize; 2]; 2]);
    let patterns: Vec<PatternSpec> = vec![
        ("constant", (0..120).map(|_| 1e9).collect(), [[34, 217], [252, 435]]),
        (
            "counter",
            (0..120).map(|i| 1e9 + i as f64).collect(),
            [[214, 397], [419, 602]],
        ),
        (
            "alternating-bool",
            (0..120).map(|i| (i % 2) as f64).collect(),
            [[214, 397], [419, 602]],
        ),
        (
            "shifted-window-5",
            (0..120)
                .map(|i| {
                    let deltas = [0u64, 0xff << 27, (0xff << 27) ^ (0xff << 32), 0xff << 32];
                    f64::from_bits(1.0f64.to_bits() ^ deltas[i % 4])
                })
                .collect(),
            [[260, 443], [464, 647]],
        ),
        (
            "shifted-window-6",
            (0..120)
                .map(|i| {
                    let deltas = [0u64, 0xff << 27, (0xff << 27) ^ (0xff << 33), 0xff << 33];
                    f64::from_bits(1.0f64.to_bits() ^ deltas[i % 4])
                })
                .collect(),
            [[347, 530], [479, 662]],
        ),
        (
            "shifted-window-7",
            (0..120)
                .map(|i| {
                    let deltas = [0u64, 0xff << 27, (0xff << 27) ^ (0xff << 34), 0xff << 34];
                    f64::from_bits(1.0f64.to_bits() ^ deltas[i % 4])
                })
                .collect(),
            [[347, 530], [565, 748]],
        ),
        (
            "full-width-union",
            (0..120)
                .map(|i| {
                    let deltas = [
                        0u64,
                        (1u64 << 63) | (1u64 << 5),
                        (1u64 << 63) | (1u64 << 58) | (1u64 << 5) | 1,
                        (1u64 << 58) | 1,
                    ];
                    f64::from_bits(1.0f64.to_bits() ^ deltas[i % 4])
                })
                .collect(),
            [[1018, 1201], [1223, 1406]],
        ),
        (
            "sign-excursion",
            (0..120)
                .map(|i| {
                    let v = 1e9 + i as f64;
                    if i == 2 {
                        -v
                    } else {
                        v
                    }
                })
                .collect(),
            [[225, 408], [429, 612]],
        ),
        (
            "zero-excursion",
            (0..120)
                .map(|i| {
                    if i == 2 {
                        0.0
                    } else {
                        1000.0 + (i as f64) * 0.125
                    }
                })
                .collect(),
            [[175, 358], [380, 563]],
        ),
        (
            "random-integer",
            (0..120).map(|_| r.int_n(1001) as f64).collect(),
            [[260, 443], [464, 647]],
        ),
        (
            "random-float",
            (0..120).map(|_| r.float64() * 1000.0).collect(),
            [[900, 1083], [1104, 1287]],
        ),
        (
            "fractional",
            (0..120)
                .map(|i| i as f64 + (i as f64) / 10.0 + (i as f64) / 100.0 + (i as f64) / 1000.0)
                .collect(),
            [[883, 1066], [1088, 1271]],
        ),
        (
            "special-floats",
            (0..120)
                .map(|i| match i % 31 {
                    0 => f64::from_bits(STALE_NAN_BITS),
                    1 => f64::from_bits(0x7ff8_0000_0000_0012),
                    2 => f64::INFINITY,
                    3 => f64::NEG_INFINITY,
                    4 => -0.0f64,
                    5 => 0.0,
                    _ => 1e9 + i as f64,
                })
                .collect(),
            [[301, 484], [508, 691]],
        ),
    ];

    let mut cases = Vec::new();
    for (name, values, want_bytes_grid) in patterns {
        for (jitter_idx, jitter) in [false, true].into_iter().enumerate() {
            for (st_idx, with_st) in [false, true].into_iter().enumerate() {
                let mut timestamps = GoPcg::new(1, 2);
                let mut samples = Vec::with_capacity(values.len());
                for (i, &v) in values.iter().enumerate() {
                    let mut t = 1_700_000_000_000i64 + (i as i64) * 15_000;
                    if jitter {
                        t += timestamps.int_n(11) as i64 - 5;
                    }
                    let st = if with_st {
                        1_699_999_990_000i64 + ((i as i64) / 60) * 900_000
                    } else {
                        0
                    };
                    samples.push(Triple { st, t, v });
                }
                cases.push(Xor2SampleCase {
                    name: format!("{name}/jitter={jitter}/st={with_st}"),
                    samples,
                    want_bytes: want_bytes_grid[jitter_idx][st_idx],
                });
            }
        }
    }
    cases
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2WindowSelection
#[test]
fn test_xor2_window_selection() {
    for tc in xor2_window_samples() {
        let mut c = Xor2Chunk::new();
        {
            let mut app = c.appender().unwrap();
            for (i, s) in tc.samples.iter().enumerate() {
                if i == 5 {
                    let _ = app;
                    let cloned = c.as_bytes().to_vec();
                    c.reset(cloned);
                    app = c.appender().unwrap();
                }
                app.append(s.st, s.t, s.v).unwrap();
            }
        }
        assert_eq!(c.as_bytes().len(), tc.want_bytes, "{}", tc.name);
        let decoded = EncodedChunk::from_bytes(Encoding::Xor2, c.as_bytes().to_vec()).unwrap();
        let EncodedChunk::Xor2(decoded) = decoded else {
            unreachable!()
        };
        let mut it = decoded.iter();
        for s in &tc.samples {
            let (ts, v) = it.next_sample().unwrap().expect("expected sample");
            assert_eq!(ts, s.t, "{}", tc.name);
            assert_eq!(it.at_st(), s.st, "{}", tc.name);
            assert_eq!(v.to_bits(), s.v.to_bits(), "{}", tc.name);
        }
        assert!(it.next_sample().unwrap().is_none(), "{}", tc.name);
    }
}

fn require_xor2_samples(samples: &[Triple]) {
    let mut chunk = Xor2Chunk::new();
    {
        let mut app = chunk.appender().unwrap();
        for s in samples {
            app.append(s.st, s.t, s.v).unwrap();
        }
    }
    let mut it = chunk.iter();
    for want in samples {
        let (ts, v) = it.next_sample().unwrap().expect("expected sample");
        assert_eq!(it.at_st(), want.st);
        assert_eq!(ts, want.t);
        assert_eq!(v, want.v);
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2Basic
#[test]
fn test_xor2_basic() {
    let mut c = Xor2Chunk::new();
    let samples = [
        (1000i64, 1.0f64),
        (2000, 2.0),
        (3000, 3.0),
        (4000, 4.0),
        (5000, 5.0),
    ];
    {
        let mut app = c.appender().unwrap();
        for &(t, v) in &samples {
            app.append(0, t, v).unwrap();
        }
    }
    let mut it = c.iter();
    for &(exp_t, exp_v) in &samples {
        let (ts, v) = it.next_sample().unwrap().unwrap();
        assert_eq!(ts, exp_t);
        assert_eq!(v, exp_v);
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2WithStaleness
#[test]
fn test_xor2_with_staleness() {
    let mut c = Xor2Chunk::new();
    let samples = [
        (1000i64, 1.0f64, false),
        (2000, 2.0, false),
        (3000, f64::from_bits(STALE_NAN_BITS), true),
        (4000, 4.0, false),
        (5000, f64::from_bits(STALE_NAN_BITS), true),
        (6000, 6.0, false),
    ];
    {
        let mut app = c.appender().unwrap();
        for &(t, v, _) in &samples {
            app.append(0, t, v).unwrap();
        }
    }
    let mut it = c.iter();
    for &(exp_t, exp_v, stale) in &samples {
        let (ts, v) = it.next_sample().unwrap().unwrap();
        assert_eq!(ts, exp_t);
        if stale {
            assert!(is_stale_nan(v), "expected stale NaN at ts={ts}");
        } else {
            assert_eq!(v, exp_v);
        }
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2StaleWithDodNonZero
#[test]
fn test_xor2_stale_with_dod_non_zero() {
    let mut c = Xor2Chunk::new();
    let samples = [
        (1000i64, 1.0f64, false),
        (2000, 2.0, false),
        (3050, f64::from_bits(STALE_NAN_BITS), true),
        (4050, 4.0, false),
        (5050, 5.0, false),
    ];
    {
        let mut app = c.appender().unwrap();
        for &(t, v, _) in &samples {
            app.append(0, t, v).unwrap();
        }
    }
    let mut it = c.iter();
    for &(exp_t, exp_v, stale) in &samples {
        let (ts, v) = it.next_sample().unwrap().unwrap();
        assert_eq!(ts, exp_t);
        if stale {
            assert!(is_stale_nan(v), "expected stale NaN at ts={ts}");
        } else {
            assert_eq!(v, exp_v);
        }
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2IrregularTimestamps
#[test]
fn test_xor2_irregular_timestamps() {
    let mut c = Xor2Chunk::new();
    let timestamps = [
        1000i64,
        2000,
        3000,
        3050,
        4050,
        5050,
        5050 + 100_000,
        5050 + 200_000,
        5050 + 300_000,
        5050 + 301_000,
    ];
    {
        let mut app = c.appender().unwrap();
        for &ts in &timestamps {
            app.append(0, ts, 1.0).unwrap();
        }
    }
    let mut it = c.iter();
    for &expected in &timestamps {
        let (ts, _) = it.next_sample().unwrap().unwrap();
        assert_eq!(ts, expected);
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2LargeDod
#[test]
fn test_xor2_large_dod() {
    let mut c = Xor2Chunk::new();
    let timestamps = [0i64, 1000, 2000, 2000 + (1 << 20)];
    {
        let mut app = c.appender().unwrap();
        for &ts in &timestamps {
            app.append(0, ts, 1.0).unwrap();
        }
    }
    let mut it = c.iter();
    for &expected in &timestamps {
        let (ts, _) = it.next_sample().unwrap().unwrap();
        assert_eq!(ts, expected);
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2LargeDodWithActiveST
#[test]
fn test_xor2_large_dod_with_active_st() {
    require_xor2_samples(&[
        Triple { st: 0, t: 0, v: 1.0 },
        Triple { st: 900, t: 1000, v: 2.0 },
        Triple { st: 1000, t: 2000, v: 3.0 },
        Triple { st: 1_047_576, t: 1_050_576, v: 4.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ActiveSTFastPathBoundaries
#[test]
fn test_xor2_active_st_fast_path_boundaries() {
    require_xor2_samples(&[
        Triple { st: 0, t: 1000, v: 1.0 },
        Triple { st: 1990, t: 2000, v: 1.0 },
        Triple { st: 2986, t: 3000, v: 1.0 },
        Triple { st: 3954, t: 4000, v: 1.0 },
        Triple { st: 4698, t: 5000, v: 1.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ActiveST13BitDodValueUnchangedSTDeltaBranches
#[test]
fn test_xor2_active_st_13bit_dod_value_unchanged_st_delta_branches() {
    require_xor2_samples(&[
        Triple { st: 0, t: 1000, v: 1.0 },
        Triple { st: 0, t: 2000, v: 1.0 },
        Triple { st: 1500, t: 3000, v: 1.0 },
        Triple { st: 2500, t: 4001, v: 1.0 },
        Triple { st: 3497, t: 5003, v: 1.0 },
        Triple { st: 4467, t: 6004, v: 1.0 },
        Triple { st: 5212, t: 7007, v: 1.0 },
        Triple { st: 5915, t: 8008, v: 1.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2EncodeJointValueUnchangedThenChanged
#[test]
fn test_xor2_encode_joint_value_unchanged_then_changed() {
    require_xor2_samples(&[
        Triple { st: 0, t: 1000, v: 1.0 },
        Triple { st: 0, t: 2000, v: 2.0 },
        Triple { st: 0, t: 7096, v: 2.0 },
        Triple { st: 0, t: 12192, v: 3.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ConstantNonZeroSTFastPath
#[test]
fn test_xor2_constant_non_zero_st_fast_path() {
    require_xor2_samples(&[
        Triple { st: 500, t: 1000, v: 1.0 },
        Triple { st: 500, t: 2000, v: 2.0 },
        Triple { st: 500, t: 3000, v: 2.0 },
        Triple { st: 500, t: 4050, v: 2.0 },
        Triple { st: 500, t: 5100, v: 3.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ActiveSTDodZeroValueChange
#[test]
fn test_xor2_active_st_dod_zero_value_change() {
    require_xor2_samples(&[
        Triple { st: 0, t: 1000, v: 1.0 },
        Triple { st: 500, t: 2000, v: 2.0 },
        Triple { st: 500, t: 3000, v: 3.0 },
        Triple { st: 500, t: 4000, v: 4.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ActiveSTDodZeroValueUnchangedZeroSTDelta
#[test]
fn test_xor2_active_st_dod_zero_value_unchanged_zero_st_delta() {
    require_xor2_samples(&[
        Triple { st: 0, t: 1000, v: 1.0 },
        Triple { st: 0, t: 2000, v: 1.0 },
        Triple { st: 1500, t: 3000, v: 1.0 },
        Triple { st: 2500, t: 4000, v: 1.0 },
        Triple { st: 3500, t: 5000, v: 1.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ActiveSTValueChangeInlineSTDeltaBranches
#[test]
fn test_xor2_active_st_value_change_inline_st_delta_branches() {
    require_xor2_samples(&[
        Triple { st: 0, t: 1000, v: 1.0 },
        Triple { st: 0, t: 2000, v: 2.0 },
        Triple { st: 1500, t: 3000, v: 3.0 },
        Triple { st: 2500, t: 4000, v: 4.0 },
        Triple { st: 3497, t: 5000, v: 5.0 },
        Triple { st: 4466, t: 6000, v: 6.0 },
        Triple { st: 5211, t: 7000, v: 7.0 },
    ]);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ChunkST
#[test]
fn test_xor2_chunk_st() {
    let run_case = |samples: &[Triple]| {
        let mut chunk = Xor2Chunk::new();
        let mut clone_bytes = Vec::new();
        {
            let mut app = chunk.appender().unwrap();
            for (i, s) in samples.iter().enumerate() {
                if i + 1 == samples.len() {
                    let _ = app;
                    clone_bytes = chunk.as_bytes().to_vec();
                    app = chunk.appender().unwrap();
                }
                app.append(s.st, s.t, s.v).unwrap();
            }
        }
        let mut chunks_to_test = vec![chunk];
        if let Some(last) = samples.last() {
            let mut clone_chunk = Xor2Chunk::new();
            clone_chunk.reset(clone_bytes);
            clone_chunk
                .appender()
                .unwrap()
                .append(last.st, last.t, last.v)
                .unwrap();
            chunks_to_test.push(clone_chunk);
        }
        for (ci, chk) in chunks_to_test.iter().enumerate() {
            assert_eq!(chk.num_samples(), samples.len(), "chunk {ci}");
            let mut it = chk.iter();
            for (i, s) in samples.iter().enumerate() {
                let (ts, v) = it.next_sample().unwrap().unwrap();
                assert_eq!(ts, s.t, "chunk {ci}[{i}] ts");
                assert_eq!(it.at_st(), s.st, "chunk {ci}[{i}] st");
                assert!((v - s.v).abs() <= 1e-9, "chunk {ci}[{i}] v");
            }
            assert!(it.next_sample().unwrap().is_none());
        }
    };

    run_case(&[
        Triple { st: 0, t: 1000, v: 1.5 },
        Triple { st: 0, t: 2000, v: 2.5 },
        Triple { st: 0, t: 3000, v: 3.5 },
        Triple { st: 0, t: 4000, v: 4.5 },
    ]);

    let st_times = [0i64, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000];
    let ts_fn = |j: usize| -> i64 { 1000 * (j as i64 + 1) };

    for number_of_samples in 0..5 {
        let mut samples = vec![
            Triple {
                st: 0,
                t: 0,
                v: 0.0
            };
            number_of_samples
        ];
        let mut sample_st_idx = vec![0usize; number_of_samples];
        loop {
            for j in 0..number_of_samples {
                samples[j] = Triple {
                    st: st_times[sample_st_idx[j]],
                    t: ts_fn(j),
                    v: j as f64 + 0.5,
                };
            }
            run_case(&samples);

            let mut exhausted = true;
            for j in (0..number_of_samples).rev() {
                if st_times[sample_st_idx[j]] < ts_fn(j) {
                    sample_st_idx[j] += 1;
                    exhausted = false;
                    break;
                }
                sample_st_idx[j] = 0;
            }
            if exhausted {
                break;
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2Chunk_MoreThan127Samples
#[test]
fn test_xor2_chunk_more_than_127_samples() {
    let after_max = usize::from(MAX_FIRST_ST_CHANGE_ON) + 3;

    // zero ST
    {
        let mut chunk = Xor2Chunk::new();
        {
            let mut app = chunk.appender().unwrap();
            for i in 0..after_max {
                app.append(0, (i as i64) * 10 + 1, (i as f64) * 1.5).unwrap();
            }
        }
        let mut it = chunk.iter();
        for i in 0..after_max {
            let (ts, v) = it.next_sample().unwrap().unwrap();
            assert_eq!(it.at_st(), 0);
            assert_eq!(ts, (i as i64) * 10 + 1);
            assert_eq!(v, (i as f64) * 1.5);
        }
        assert!(it.next_sample().unwrap().is_none());
    }

    // non-zero ST after 127
    {
        let mut chunk = Xor2Chunk::new();
        {
            let mut app = chunk.appender().unwrap();
            for i in 0..after_max {
                let st = if i == after_max - 1 {
                    ((after_max - 1) as i64) * 10
                } else {
                    0
                };
                app.append(st, (i as i64) * 10 + 1, (i as f64) * 1.5)
                    .unwrap();
            }
        }
        let mut it = chunk.iter();
        for i in 0..after_max {
            let (ts, v) = it.next_sample().unwrap().unwrap();
            let expected_st = if i == after_max - 1 {
                ((after_max - 1) as i64) * 10
            } else {
                0
            };
            assert_eq!(it.at_st(), expected_st);
            assert_eq!(ts, (i as i64) * 10 + 1);
            assert_eq!(v, (i as f64) * 1.5);
        }
        assert!(it.next_sample().unwrap().is_none());
    }
}

fn xor2_delta_window(delta: u64) -> (u8, u8, u8) {
    let mut leading = delta.leading_zeros() as u8;
    let trailing = delta.trailing_zeros() as u8;
    if leading >= 32 {
        leading = 31;
    }
    (leading, trailing, 64 - leading - trailing)
}

fn write_xor2_new_window_payload(bs: &mut BitStream, delta: u64) {
    let (leading, trailing, sigbits) = xor2_delta_window(delta);
    let encoded_sigbits = if sigbits == 64 { 0 } else { sigbits };
    bs.write_bits(u64::from(leading), 5);
    bs.write_bits(u64::from(encoded_sigbits), 6);
    bs.write_bits(delta >> trailing, usize::from(sigbits));
}

fn build_padded_bytes(padding: u8, payload: impl FnOnce(&mut BitStream)) -> Vec<u8> {
    let mut bs = BitStream::new();
    if padding > 0 {
        bs.write_bits(0, usize::from(padding));
    }
    payload(&mut bs);
    bs.write_bits(0, 64);
    bs.into_bytes()
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2DecodeFunctionsAcrossPadding
#[test]
fn test_xor2_decode_functions_across_padding() {
    const BASELINE: f64 = 1234.5;

    // 1. decodeValue
    {
        let reuse_d = 0x0000_0ABC_DE00_0000_u64;
        let (r_l, r_t, r_s) = xor2_delta_window(reuse_d);
        let new_d_full = 0xFEDC_BA98_7654_3211u64;
        let (n_l_full, n_t_full, _) = xor2_delta_window(new_d_full);
        let new_d_small = 0x0000_0ABC_DE00_0000_u64;
        let (n_l_small, n_t_small, _) = xor2_delta_window(new_d_small);

        for padding in 0u8..64 {
            // unchanged
            {
                let bytes = build_padded_bytes(padding, |bs| bs.write_bit(false));
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.decode_value_step().unwrap();
                assert_eq!(it.val(), BASELINE);
                assert_eq!(it.baseline_v(), BASELINE);
            }
            // reuse_window
            {
                let bytes = build_padded_bytes(padding, |bs| {
                    bs.write_bits(0b10, 2);
                    bs.write_bits(reuse_d >> r_t, usize::from(r_s));
                });
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.set_window(r_l, r_t);
                it.decode_value_step().unwrap();
                let expected = f64::from_bits(BASELINE.to_bits() ^ reuse_d);
                assert_eq!(it.val(), expected);
                assert_eq!(it.baseline_v(), expected);
                assert_eq!(it.leading(), r_l);
                assert_eq!(it.trailing(), r_t);
            }
            // new_window_full_sigbits
            {
                let bytes = build_padded_bytes(padding, |bs| {
                    bs.write_bits(0b110, 3);
                    write_xor2_new_window_payload(bs, new_d_full);
                });
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.decode_value_step().unwrap();
                let expected = f64::from_bits(BASELINE.to_bits() ^ new_d_full);
                assert_eq!(it.val(), expected);
                assert_eq!(it.baseline_v(), expected);
                assert_eq!(it.leading(), n_l_full);
                assert_eq!(it.trailing(), n_t_full);
            }
            // new_window_small_sigbits
            {
                let bytes = build_padded_bytes(padding, |bs| {
                    bs.write_bits(0b110, 3);
                    write_xor2_new_window_payload(bs, new_d_small);
                });
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.decode_value_step().unwrap();
                let expected = f64::from_bits(BASELINE.to_bits() ^ new_d_small);
                assert_eq!(it.val(), expected);
                assert_eq!(it.baseline_v(), expected);
                assert_eq!(it.leading(), n_l_small);
                assert_eq!(it.trailing(), n_t_small);
            }
            // stale_nan
            {
                let bytes = build_padded_bytes(padding, |bs| bs.write_bits(0b111, 3));
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.decode_value_step().unwrap();
                assert!(is_stale_nan(it.val()));
                assert_eq!(it.baseline_v(), BASELINE);
            }
        }
    }

    // 2. decodeValueKnownNonZero
    {
        let delta = 0x0000_0ABC_DE00_0000_u64;
        let (d_l, d_t, d_s) = xor2_delta_window(delta);
        for padding in 0u8..64 {
            // reuse_window
            {
                let bytes = build_padded_bytes(padding, |bs| {
                    bs.write_bit(false);
                    bs.write_bits(delta >> d_t, usize::from(d_s));
                });
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.set_window(d_l, d_t);
                it.decode_value_known_non_zero_step().unwrap();
                let expected = f64::from_bits(BASELINE.to_bits() ^ delta);
                assert_eq!(it.val(), expected);
                assert_eq!(it.baseline_v(), expected);
            }
            // new_window
            {
                let bytes = build_padded_bytes(padding, |bs| {
                    bs.write_bit(true);
                    write_xor2_new_window_payload(bs, delta);
                });
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.set_window(d_l, d_t);
                it.decode_value_known_non_zero_step().unwrap();
                let expected = f64::from_bits(BASELINE.to_bits() ^ delta);
                assert_eq!(it.val(), expected);
                assert_eq!(it.baseline_v(), expected);
                assert_eq!(it.leading(), d_l);
                assert_eq!(it.trailing(), d_t);
            }
        }
    }

    // 3. decodeNewLeadingTrailing
    {
        let small_d = 0x0000_0ABC_DE00_0000_u64;
        let (s_l, s_t, _) = xor2_delta_window(small_d);
        let full_d = 0xFEDC_BA98_7654_3211u64;
        let (f_l, f_t, _) = xor2_delta_window(full_d);

        for padding in 0u8..64 {
            // small_sigbits
            {
                let bytes = build_padded_bytes(padding, |bs| {
                    write_xor2_new_window_payload(bs, small_d);
                });
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.decode_new_leading_trailing_step().unwrap();
                assert_eq!(it.leading(), s_l);
                assert_eq!(it.trailing(), s_t);
                let expected = f64::from_bits(BASELINE.to_bits() ^ small_d);
                assert_eq!(it.val(), expected);
                assert_eq!(it.baseline_v(), expected);
            }
            // full_width_sigbits
            {
                let bytes = build_padded_bytes(padding, |bs| {
                    write_xor2_new_window_payload(bs, full_d);
                });
                let mut br = BitReader::new(&bytes);
                if padding > 0 {
                    br.read_bits(padding).unwrap();
                }
                let mut it = Xor2Iterator::from_reader(br);
                it.set_baseline_v(BASELINE);
                it.decode_new_leading_trailing_step().unwrap();
                assert_eq!(it.leading(), f_l);
                assert_eq!(it.trailing(), f_t);
                let expected = f64::from_bits(BASELINE.to_bits() ^ full_d);
                assert_eq!(it.val(), expected);
                assert_eq!(it.baseline_v(), expected);
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/xor2_test.go::TestXOR2ChunkOverFlowPanics
#[test]
fn test_xor2_chunk_overflow_panics() {
    let mut chunk = Xor2Chunk::new();
    let mut app = chunk.appender().unwrap();
    for i in 0..(u16::MAX as i64) {
        app.append(0, i, i as f64).unwrap();
    }
    let err = app.append(0, u16::MAX as i64, u16::MAX as f64).unwrap_err();
    assert!(matches!(err, ChunkError::CapacityExceeded));
}
