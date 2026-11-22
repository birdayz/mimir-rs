//! Ported from github.com/prometheus/prometheus/tsdb/chunkenc/
//! (histogram_meta_test.go, histogram_st_test.go, histogram_test.go, st_helper_test.go).

use mimir_tsdb::chunkenc::bstream::{BitReader, BitStream};
use mimir_tsdb::chunkenc::float_histogram::XorValue;
use mimir_tsdb::chunkenc::histogram::{expand_int_spans_and_buckets, AppendHistogramOutcome};
use mimir_tsdb::chunkenc::histogram_meta::{
    adjust_for_inserts, count_spans, expand_spans_both_ways, insert_buckets,
    read_histogram_chunk_layout, write_histogram_chunk_layout, BucketIterator, Insert,
};
use mimir_tsdb::chunkenc::histogram_st::AppendHistogramStOutcome;
use mimir_tsdb::chunkenc::st::{HISTOGRAM_ST_SAMPLE_COUNT_MASK, MAX_FIRST_ST_CHANGE_ON};
use mimir_tsdb::chunkenc::{
    is_stale_nan, stale_nan, ChunkError, CounterResetHeader, CounterResetHint, FloatHistogram,
    Histogram, HistogramChunk, HistogramSpan, HistogramStChunk, CUSTOM_BUCKETS_SCHEMA,
    EXPONENTIAL_SCHEMA_MAX,
};

fn ldexp_0_5(exp: i32) -> f64 {
    0.5 * (exp as f64).exp2()
}

fn generate_test_histogram(i: i64) -> Histogram {
    Histogram {
        count: 12 + (i as u64) * 9,
        zero_count: 2 + (i as u64),
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        negative_buckets: vec![i + 1, 1, -1, 0],
        ..Default::default()
    }
}

fn generate_test_histograms(n: usize) -> Vec<Histogram> {
    (0..n)
        .map(|i| {
            let mut h = generate_test_histogram(i as i64);
            if i > 0 {
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
            }
            h
        })
        .collect()
}

fn generate_test_custom_buckets_histogram(i: i64) -> Histogram {
    Histogram {
        count: 5 + (i as u64) * 4,
        sum: 18.4 * (i + 1) as f64,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![i + 1, 1, -1, 0],
        custom_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
        ..Default::default()
    }
}

fn assert_first_int_histogram_sample_hint(chunk: &HistogramChunk, expected: CounterResetHint) {
    let mut it = chunk.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, v) = it.at_histogram();
    assert_eq!(v.counter_reset_hint, expected);
}

fn assert_hist_sample_count(chunk: &HistogramChunk, exp: usize) {
    let mut count = 0usize;
    let mut it = chunk.iter();
    while it.next_sample().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, exp);
}

// ============================================================================
// histogram_meta_test.go
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_meta_test.go::TestBucketIterator
#[test]
fn test_bucket_iterator() {
    let tests = [
        (vec![HistogramSpan { offset: 0, length: 1 }], vec![0i64]),
        (
            vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            vec![0, 1, 3],
        ),
        (
            vec![
                HistogramSpan { offset: 100, length: 4 },
                HistogramSpan { offset: 8, length: 7 },
                HistogramSpan { offset: 0, length: 1 },
            ],
            vec![100, 101, 102, 103, 112, 113, 114, 115, 116, 117, 118, 119],
        ),
        (
            vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 2, length: 1 },
                HistogramSpan { offset: 3, length: 2 },
                HistogramSpan { offset: 3, length: 1 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            vec![0, 1, 4, 8, 9, 13, 15],
        ),
        (
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 1, length: 1 },
                HistogramSpan { offset: 1, length: 4 },
                HistogramSpan { offset: 3, length: 3 },
            ],
            vec![0, 1, 2, 4, 6, 7, 8, 9, 13, 14, 15],
        ),
    ];

    for (spans, idxs) in tests {
        let mut b = BucketIterator::new(&spans);
        let mut got = Vec::new();
        while let Some(v) = b.next_idx() {
            got.push(v);
        }
        assert_eq!(got, idxs);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_meta_test.go::TestExpandSpansBothWaysAndInsert
#[test]
fn test_expand_spans_both_ways_and_insert() {
    struct Scenario {
        description: &'static str,
        spans_a: Vec<HistogramSpan>,
        spans_b: Vec<HistogramSpan>,
        f_inserts: Vec<Insert>,
        b_inserts: Vec<Insert>,
        buckets_in: Vec<i64>,
        buckets_out: Vec<i64>,
        merged_spans: Vec<HistogramSpan>,
    }

    let ins = |pos, num| Insert {
        pos,
        num,
        bucket_idx: 0,
    };

    let scenarios = [
        Scenario {
            description: "single prepend at the beginning",
            spans_a: vec![HistogramSpan { offset: -10, length: 3 }],
            spans_b: vec![HistogramSpan { offset: -11, length: 4 }],
            f_inserts: vec![ins(0, 1)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![0, 6, -3, 0],
            merged_spans: vec![HistogramSpan { offset: -11, length: 4 }],
        },
        Scenario {
            description: "single append at the end",
            spans_a: vec![HistogramSpan { offset: -10, length: 3 }],
            spans_b: vec![HistogramSpan { offset: -10, length: 4 }],
            f_inserts: vec![ins(3, 1)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![6, -3, 0, -3],
            merged_spans: vec![HistogramSpan { offset: -10, length: 4 }],
        },
        Scenario {
            description: "double prepend at the beginning",
            spans_a: vec![HistogramSpan { offset: -10, length: 3 }],
            spans_b: vec![HistogramSpan { offset: -12, length: 5 }],
            f_inserts: vec![ins(0, 2)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![0, 0, 6, -3, 0],
            merged_spans: vec![HistogramSpan { offset: -12, length: 5 }],
        },
        Scenario {
            description: "double append at the end",
            spans_a: vec![HistogramSpan { offset: -10, length: 3 }],
            spans_b: vec![HistogramSpan { offset: -10, length: 5 }],
            f_inserts: vec![ins(3, 2)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![6, -3, 0, -3, 0],
            merged_spans: vec![HistogramSpan { offset: -10, length: 5 }],
        },
        Scenario {
            description: "double prepend at the beginning and double append at the end",
            spans_a: vec![HistogramSpan { offset: -10, length: 3 }],
            spans_b: vec![HistogramSpan { offset: -12, length: 7 }],
            f_inserts: vec![ins(0, 2), ins(3, 2)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0],
            buckets_out: vec![0, 0, 6, -3, 0, -3, 0],
            merged_spans: vec![HistogramSpan { offset: -12, length: 7 }],
        },
        Scenario {
            description: "single removal of bucket at the start",
            spans_a: vec![HistogramSpan { offset: -10, length: 4 }],
            spans_b: vec![HistogramSpan { offset: -9, length: 3 }],
            f_inserts: vec![],
            b_inserts: vec![ins(0, 1)],
            buckets_in: vec![1, 2, -1, 2],
            buckets_out: vec![1, 2, -1, 2],
            merged_spans: vec![HistogramSpan { offset: -10, length: 4 }],
        },
        Scenario {
            description: "single removal of bucket in the middle",
            spans_a: vec![HistogramSpan { offset: -10, length: 4 }],
            spans_b: vec![
                HistogramSpan { offset: -10, length: 2 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            f_inserts: vec![],
            b_inserts: vec![ins(2, 1)],
            buckets_in: vec![1, 2, -1, 2],
            buckets_out: vec![1, 2, -1, 2],
            merged_spans: vec![HistogramSpan { offset: -10, length: 4 }],
        },
        Scenario {
            description: "single removal of bucket at the end",
            spans_a: vec![HistogramSpan { offset: -10, length: 4 }],
            spans_b: vec![HistogramSpan { offset: -10, length: 3 }],
            f_inserts: vec![],
            b_inserts: vec![ins(3, 1)],
            buckets_in: vec![1, 2, -1, 2],
            buckets_out: vec![1, 2, -1, 2],
            merged_spans: vec![HistogramSpan { offset: -10, length: 4 }],
        },
        Scenario {
            description: "as described in doc comment",
            spans_a: vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 2, length: 1 },
                HistogramSpan { offset: 3, length: 2 },
                HistogramSpan { offset: 3, length: 1 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            spans_b: vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 1, length: 1 },
                HistogramSpan { offset: 1, length: 4 },
                HistogramSpan { offset: 3, length: 3 },
            ],
            f_inserts: vec![ins(2, 1), ins(3, 2), ins(6, 1)],
            b_inserts: vec![],
            buckets_in: vec![6, -3, 0, -1, 2, 1, -4],
            buckets_out: vec![6, -3, -3, 3, -3, 0, 2, 2, 1, -5, 1],
            merged_spans: vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 1, length: 1 },
                HistogramSpan { offset: 1, length: 4 },
                HistogramSpan { offset: 3, length: 3 },
            ],
        },
        Scenario {
            description: "both forward and backward inserts, complex case",
            spans_a: vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 2, length: 1 },
                HistogramSpan { offset: 3, length: 2 },
                HistogramSpan { offset: 3, length: 1 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            spans_b: vec![
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 1 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 1 },
                HistogramSpan { offset: 4, length: 1 },
            ],
            f_inserts: vec![ins(2, 1), ins(3, 2), ins(6, 1)],
            b_inserts: vec![ins(0, 1), ins(5, 1), ins(6, 1), ins(7, 1)],
            buckets_in: vec![1, 2, -1, 2, 0, 3, 1],
            buckets_out: vec![1, 2, -3, 2, -2, 0, 4, 0, 3, -7, 8],
            merged_spans: vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 1, length: 1 },
                HistogramSpan { offset: 1, length: 4 },
                HistogramSpan { offset: 3, length: 3 },
            ],
        },
        Scenario {
            description: "inserts with gaps",
            spans_a: vec![
                HistogramSpan { offset: -19, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
            ],
            spans_b: vec![
                HistogramSpan { offset: -19, length: 1 },
                HistogramSpan { offset: 4, length: 1 },
                HistogramSpan { offset: 3, length: 1 },
            ],
            f_inserts: vec![ins(4, 2)],
            b_inserts: vec![ins(1, 3)],
            buckets_in: vec![1, 2, -1, 1],
            buckets_out: vec![1, 2, -1, 1, -3, 0],
            merged_spans: vec![
                HistogramSpan { offset: -19, length: 2 },
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 3, length: 1 },
            ],
        },
    ];

    for s in scenarios {
        let (f_inserts, b_inserts, m) = expand_spans_both_ways(&s.spans_a, &s.spans_b);
        assert_eq!(f_inserts, s.f_inserts, "{}", s.description);
        assert_eq!(b_inserts, s.b_inserts, "{}", s.description);
        assert_eq!(m, s.merged_spans, "{}", s.description);

        let got_buckets = insert_buckets(&s.buckets_in, s.buckets_out.len(), &f_inserts, true);
        assert_eq!(got_buckets, s.buckets_out, "{}", s.description);

        let deltas_to_abs = |src: &[i64]| -> Vec<f64> {
            let mut out = Vec::with_capacity(src.len());
            let mut acc = 0i64;
            for &v in src {
                acc += v;
                out.push(acc as f64);
            }
            out
        };
        let float_in = deltas_to_abs(&s.buckets_in);
        let float_out = deltas_to_abs(&s.buckets_out);
        let got_float = insert_buckets(&float_in, float_out.len(), &f_inserts, false);
        assert_eq!(got_float, float_out, "{}", s.description);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_meta_test.go::TestWriteReadHistogramChunkLayout
#[test]
fn test_write_read_histogram_chunk_layout() {
    struct LayoutCase {
        schema: i32,
        zero_threshold: f64,
        positive_spans: Vec<HistogramSpan>,
        negative_spans: Vec<HistogramSpan>,
        custom_values: Vec<f64>,
    }
    let layouts = [
        LayoutCase {
            schema: 3,
            zero_threshold: 0.0,
            positive_spans: vec![
                HistogramSpan { offset: -4, length: 3 },
                HistogramSpan { offset: 2, length: 42 },
            ],
            negative_spans: vec![],
            custom_values: vec![],
        },
        LayoutCase {
            schema: -2,
            zero_threshold: 2.938735877055719e-39,
            positive_spans: vec![],
            negative_spans: vec![
                HistogramSpan { offset: 2, length: 5 },
                HistogramSpan { offset: 1, length: 34 },
            ],
            custom_values: vec![],
        },
        LayoutCase {
            schema: 6,
            zero_threshold: 1024.0,
            positive_spans: vec![],
            negative_spans: vec![],
            custom_values: vec![],
        },
        LayoutCase {
            schema: 6,
            zero_threshold: 1025.0,
            positive_spans: vec![
                HistogramSpan { offset: 2, length: 5 },
                HistogramSpan { offset: 1, length: 34 },
                HistogramSpan { offset: 0, length: 0 },
            ],
            negative_spans: vec![
                HistogramSpan { offset: -345, length: 4545 },
                HistogramSpan { offset: 53_645_665, length: 345 },
                HistogramSpan { offset: 945_995, length: 85_848 },
            ],
            custom_values: vec![],
        },
        LayoutCase {
            schema: 6,
            zero_threshold: 2048.0,
            positive_spans: vec![],
            negative_spans: vec![],
            custom_values: vec![],
        },
        LayoutCase {
            schema: 0,
            zero_threshold: ldexp_0_5(-242),
            positive_spans: vec![HistogramSpan { offset: -4, length: 3 }],
            negative_spans: vec![
                HistogramSpan { offset: 2, length: 5 },
                HistogramSpan { offset: 1, length: 34 },
            ],
            custom_values: vec![],
        },
        LayoutCase {
            schema: 0,
            zero_threshold: ldexp_0_5(-243),
            positive_spans: vec![HistogramSpan { offset: -4, length: 3 }],
            negative_spans: vec![
                HistogramSpan { offset: 2, length: 5 },
                HistogramSpan { offset: 1, length: 34 },
            ],
            custom_values: vec![],
        },
        LayoutCase {
            schema: 4,
            zero_threshold: 42.0,
            positive_spans: vec![],
            negative_spans: vec![],
            custom_values: vec![],
        },
        LayoutCase {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![
                HistogramSpan { offset: -4, length: 3 },
                HistogramSpan { offset: 2, length: 42 },
            ],
            negative_spans: vec![],
            custom_values: vec![
                -5.0, -2.5, 0.0, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 255.0,
                500.0, 1000.0, 50000.0, 1e7,
            ],
        },
        LayoutCase {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![
                HistogramSpan { offset: -4, length: 3 },
                HistogramSpan { offset: 2, length: 42 },
            ],
            negative_spans: vec![],
            custom_values: vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0,
            ],
        },
        LayoutCase {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![
                HistogramSpan { offset: -4, length: 3 },
                HistogramSpan { offset: 2, length: 42 },
            ],
            negative_spans: vec![],
            custom_values: vec![
                0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024, 2.048,
                4.096, 8.192,
            ],
        },
        LayoutCase {
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            positive_spans: vec![
                HistogramSpan { offset: -4, length: 3 },
                HistogramSpan { offset: 2, length: 42 },
            ],
            negative_spans: vec![],
            custom_values: vec![1.001, 1.023, 2.01, 4.007, 4.095, 8.001, 8.19, 16.24],
        },
    ];

    let mut bs = BitStream::new();
    for l in &layouts {
        write_histogram_chunk_layout(
            &mut bs,
            l.schema,
            l.zero_threshold,
            &l.positive_spans,
            &l.negative_spans,
            &l.custom_values,
        );
    }

    let mut bsr = BitReader::new(bs.bytes());
    for want in &layouts {
        let got = read_histogram_chunk_layout(&mut bsr).unwrap();
        assert_eq!(got.schema, want.schema);
        assert_eq!(got.zero_threshold, want.zero_threshold);
        assert_eq!(got.positive_spans, want.positive_spans);
        assert_eq!(got.negative_spans, want.negative_spans);
        assert_eq!(got.custom_values, want.custom_values);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_meta_test.go::TestSpansFromBidirectionalCompareSpans
#[test]
fn test_spans_from_bidirectional_compare_spans() {
    let cases = [
        (vec![], vec![], vec![]),
        (
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 3, length: 3 },
                HistogramSpan { offset: 5, length: 3 },
            ],
            vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 2, length: 2 },
                HistogramSpan { offset: 2, length: 3 },
                HistogramSpan { offset: 3, length: 3 },
            ],
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 1, length: 7 },
                HistogramSpan { offset: 3, length: 3 },
            ],
        ),
        (
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 3, length: 5 },
                HistogramSpan { offset: 3, length: 3 },
            ],
            vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 5, length: 3 },
                HistogramSpan { offset: 4, length: 3 },
            ],
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 3, length: 5 },
                HistogramSpan { offset: 3, length: 3 },
            ],
        ),
        (
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 3, length: 3 },
                HistogramSpan { offset: 5, length: 3 },
            ],
            vec![
                HistogramSpan { offset: 3, length: 3 },
                HistogramSpan { offset: 4, length: 2 },
            ],
            vec![
                HistogramSpan { offset: 0, length: 9 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 2, length: 3 },
            ],
        ),
        (
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 3, length: 3 },
                HistogramSpan { offset: 5, length: 3 },
            ],
            vec![],
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 3, length: 3 },
                HistogramSpan { offset: 5, length: 3 },
            ],
        ),
        (
            vec![
                HistogramSpan { offset: -5, length: 0 },
                HistogramSpan { offset: 2, length: 0 },
                HistogramSpan { offset: 3, length: 3 },
                HistogramSpan { offset: 1, length: 0 },
                HistogramSpan { offset: 2, length: 3 },
                HistogramSpan { offset: 2, length: 0 },
                HistogramSpan { offset: 2, length: 0 },
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 4, length: 0 },
                HistogramSpan { offset: 5, length: 0 },
            ],
            vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 2, length: 2 },
                HistogramSpan { offset: 1, length: 0 },
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 3, length: 3 },
            ],
            vec![
                HistogramSpan { offset: 0, length: 3 },
                HistogramSpan { offset: 1, length: 7 },
                HistogramSpan { offset: 3, length: 3 },
            ],
        ),
    ];

    for (s1, s2, exp) in cases {
        let (_, _, act1) = expand_spans_both_ways(&s1, &s2);
        assert_eq!(act1, exp);
        let (_, _, act2) = expand_spans_both_ways(&s2, &s1);
        assert_eq!(act2, exp);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_meta_test.go::TestExpandIntOrFloatSpansAndBuckets
#[test]
fn test_expand_int_or_float_spans_and_buckets() {
    use mimir_tsdb::chunkenc::float_histogram::expand_float_spans_and_buckets;

    struct Case {
        name: &'static str,
        spans_a: Vec<HistogramSpan>,
        buckets_a: Vec<i64>,
        spans_b: Vec<HistogramSpan>,
        buckets_b: Vec<i64>,
        expect_reset: bool,
        expect_f_inserts: Vec<Insert>,
        expect_b_inserts: Vec<Insert>,
        expect_merged_spans: Vec<HistogramSpan>,
        expect_buckets_a: Vec<i64>,
        expect_buckets_b: Vec<i64>,
    }

    let i3 = |pos, num, bucket_idx| Insert {
        pos,
        num,
        bucket_idx,
    };

    let cases = [
        Case {
            name: "empty",
            spans_a: vec![],
            buckets_a: vec![],
            spans_b: vec![],
            buckets_b: vec![],
            expect_reset: false,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![],
            expect_buckets_a: vec![],
            expect_buckets_b: vec![],
        },
        Case {
            name: "single bucket reset to none",
            spans_a: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_a: vec![1],
            spans_b: vec![],
            buckets_b: vec![],
            expect_reset: true,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![],
            expect_buckets_a: vec![],
            expect_buckets_b: vec![],
        },
        Case {
            name: "single bucket reset to lower",
            spans_a: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_a: vec![2],
            spans_b: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_b: vec![1],
            expect_reset: true,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![],
            expect_buckets_a: vec![],
            expect_buckets_b: vec![],
        },
        Case {
            name: "single bucket increase",
            spans_a: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_a: vec![1],
            spans_b: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_b: vec![2],
            expect_reset: false,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![HistogramSpan { offset: 1, length: 1 }],
            expect_buckets_a: vec![1],
            expect_buckets_b: vec![2],
        },
        Case {
            name: "distinct new buckets and increase",
            spans_a: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_a: vec![1],
            spans_b: vec![
                HistogramSpan { offset: -2, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 3, length: 1 },
            ],
            buckets_b: vec![2, 0, 0, 0, 0],
            expect_reset: false,
            expect_f_inserts: vec![i3(0, 2, -2), i3(1, 1, 2), i3(1, 1, 6)],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![
                HistogramSpan { offset: -2, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 3, length: 1 },
            ],
            expect_buckets_a: vec![0, 0, 1, -1, 0],
            expect_buckets_b: vec![2, 0, 0, 0, 0],
        },
        Case {
            name: "distinct new buckets but reset",
            spans_a: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_a: vec![2],
            spans_b: vec![
                HistogramSpan { offset: -2, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 3, length: 1 },
            ],
            buckets_b: vec![1, 0, 0, 0, 0],
            expect_reset: true,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![],
            expect_buckets_a: vec![],
            expect_buckets_b: vec![],
        },
        Case {
            name: "distinct new buckets but missing",
            spans_a: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_a: vec![2],
            spans_b: vec![
                HistogramSpan { offset: -2, length: 2 },
                HistogramSpan { offset: 2, length: 1 },
                HistogramSpan { offset: 3, length: 1 },
            ],
            buckets_b: vec![1, 0, 0, 0],
            expect_reset: true,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![],
            expect_buckets_a: vec![],
            expect_buckets_b: vec![],
        },
        Case {
            name: "distinct new buckets and missing an empty bucket",
            spans_a: vec![HistogramSpan { offset: 1, length: 1 }],
            buckets_a: vec![0],
            spans_b: vec![HistogramSpan { offset: 3, length: 1 }],
            buckets_b: vec![1],
            expect_reset: false,
            expect_f_inserts: vec![i3(1, 1, 3)],
            expect_b_inserts: vec![i3(0, 1, 1)],
            expect_merged_spans: vec![
                HistogramSpan { offset: 1, length: 1 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            expect_buckets_a: vec![0, 0],
            expect_buckets_b: vec![0, 1],
        },
        Case {
            name: "distinct new buckets and missing multiple empty buckets",
            spans_a: vec![
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 2, length: 1 },
                HistogramSpan { offset: 2, length: 2 },
            ],
            buckets_a: vec![0, 0, 0, 0, 0, 0, 0, 0],
            spans_b: vec![HistogramSpan { offset: 8, length: 1 }],
            buckets_b: vec![1],
            expect_reset: false,
            expect_f_inserts: vec![i3(5, 1, 8)],
            expect_b_inserts: vec![i3(0, 3, 1), i3(0, 2, 5), i3(1, 1, 9), i3(1, 2, 12)],
            expect_merged_spans: vec![
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 2, length: 2 },
            ],
            expect_buckets_a: vec![0, 0, 0, 0, 0, 0, 0, 0, 0],
            expect_buckets_b: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
        },
        Case {
            name: "overlap new buckets and missing multiple empty buckets",
            spans_a: vec![
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 2, length: 2 },
            ],
            buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
            spans_b: vec![HistogramSpan { offset: 8, length: 1 }],
            buckets_b: vec![2],
            expect_reset: false,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![i3(0, 3, 1), i3(0, 2, 5), i3(1, 1, 9), i3(1, 2, 12)],
            expect_merged_spans: vec![
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 2, length: 2 },
            ],
            expect_buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
            expect_buckets_b: vec![0, 0, 0, 0, 0, 2, -2, 0, 0],
        },
        Case {
            name: "overlap new buckets and missing multiple empty buckets with 0 length/offset spans",
            spans_a: vec![
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 0 },
                HistogramSpan { offset: 1, length: 2 },
            ],
            buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
            spans_b: vec![
                HistogramSpan { offset: 1, length: 0 },
                HistogramSpan { offset: 7, length: 1 },
            ],
            buckets_b: vec![2],
            expect_reset: false,
            expect_f_inserts: vec![],
            expect_b_inserts: vec![i3(0, 3, 1), i3(0, 2, 5), i3(1, 1, 9), i3(1, 2, 12)],
            expect_merged_spans: vec![
                HistogramSpan { offset: 1, length: 3 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
                HistogramSpan { offset: 2, length: 2 },
            ],
            expect_buckets_a: vec![0, 0, 0, 0, 0, 1, -1, 0, 0],
            expect_buckets_b: vec![0, 0, 0, 0, 0, 2, -2, 0, 0],
        },
        Case {
            name: "new empty buckets between filled buckets",
            spans_a: vec![
                HistogramSpan { offset: -51, length: 8 },
                HistogramSpan { offset: 11, length: 1 },
                HistogramSpan { offset: 14, length: 1 },
            ],
            buckets_a: vec![1, 0, 1, -1, 1, 1, 0, -1, -1, 0],
            spans_b: vec![
                HistogramSpan { offset: -51, length: 9 },
                HistogramSpan { offset: 9, length: 2 },
                HistogramSpan { offset: 14, length: 1 },
            ],
            buckets_b: vec![1, 1, 0, 1, -1, 1, 0, -1, -1, 0, 0, 0],
            expect_reset: false,
            expect_f_inserts: vec![i3(8, 1, -43), i3(8, 1, -33)],
            expect_b_inserts: vec![],
            expect_merged_spans: vec![
                HistogramSpan { offset: -51, length: 9 },
                HistogramSpan { offset: 9, length: 2 },
                HistogramSpan { offset: 14, length: 1 },
            ],
            expect_buckets_a: vec![1, 0, 1, -1, 1, 1, 0, -1, -2, 0, 1, 0],
            expect_buckets_b: vec![1, 1, 0, 1, -1, 1, 0, -1, -1, 0, 0, 0],
        },
        Case {
            name: "real example 1",
            spans_a: vec![
                HistogramSpan { offset: -16, length: 1 },
                HistogramSpan { offset: 2, length: 1 },
                HistogramSpan { offset: 7, length: 1 },
            ],
            buckets_a: vec![0, 2, -2],
            spans_b: vec![
                HistogramSpan { offset: -15, length: 4 },
                HistogramSpan { offset: 5, length: 2 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            buckets_b: vec![0, 1, 2, -3, 0, 0, 0],
            expect_reset: false,
            expect_f_inserts: vec![
                i3(1, 2, -15),
                i3(2, 1, -12),
                i3(2, 1, -6),
                i3(3, 1, -3),
            ],
            expect_b_inserts: vec![i3(0, 1, -16)],
            expect_merged_spans: vec![
                HistogramSpan { offset: -16, length: 5 },
                HistogramSpan { offset: 5, length: 2 },
                HistogramSpan { offset: 1, length: 1 },
            ],
            expect_buckets_a: vec![0, 0, 0, 2, -2, 0, 0, 0],
            expect_buckets_b: vec![0, 0, 1, 2, -3, 0, 0, 0],
        },
    ];

    for tc in cases {
        assert_eq!(tc.buckets_a.len(), count_spans(&tc.spans_a), "{}", tc.name);
        assert_eq!(tc.buckets_b.len(), count_spans(&tc.spans_b), "{}", tc.name);
        if !tc.expect_reset {
            assert_eq!(
                tc.expect_buckets_a.len(),
                count_spans(&tc.expect_merged_spans),
                "{}",
                tc.name
            );
            assert_eq!(
                tc.expect_buckets_b.len(),
                count_spans(&tc.expect_merged_spans),
                "{}",
                tc.name
            );
        }

        // integers
        {
            let res = expand_int_spans_and_buckets(
                &tc.spans_a,
                &tc.spans_b,
                &tc.buckets_a,
                &tc.buckets_b,
            );
            if tc.expect_reset {
                assert!(res.is_none(), "{}", tc.name);
            } else {
                let (f_inserts, b_inserts) = res.expect(tc.name);
                assert_eq!(f_inserts, tc.expect_f_inserts, "{}", tc.name);
                assert_eq!(b_inserts, tc.expect_b_inserts, "{}", tc.name);
                assert_eq!(
                    adjust_for_inserts(&tc.spans_b, &b_inserts),
                    tc.expect_merged_spans,
                    "{}",
                    tc.name
                );

                let got_a = insert_buckets(
                    &tc.buckets_a,
                    tc.expect_buckets_a.len(),
                    &f_inserts,
                    true,
                );
                assert_eq!(got_a, tc.expect_buckets_a, "{}", tc.name);

                let got_b = insert_buckets(
                    &tc.buckets_b,
                    tc.expect_buckets_b.len(),
                    &b_inserts,
                    true,
                );
                assert_eq!(got_b, tc.expect_buckets_b, "{}", tc.name);
            }
        }

        // floats
        {
            let make_float_buckets = |src: &[i64]| -> Vec<f64> {
                let mut out = Vec::with_capacity(src.len());
                let mut abs_v = 0i64;
                for &v in src {
                    abs_v += v;
                    out.push(abs_v as f64);
                }
                out
            };
            let a_xors: Vec<XorValue> = make_float_buckets(&tc.buckets_a)
                .into_iter()
                .map(|value| XorValue {
                    value,
                    leading: 0,
                    trailing: 0,
                })
                .collect();
            let b_floats = make_float_buckets(&tc.buckets_b);
            let res = expand_float_spans_and_buckets(&tc.spans_a, &tc.spans_b, &a_xors, &b_floats);
            if tc.expect_reset {
                assert!(res.is_none(), "{}", tc.name);
            } else {
                let (f_inserts, b_inserts) = res.expect(tc.name);
                assert_eq!(f_inserts, tc.expect_f_inserts, "{}", tc.name);
                assert_eq!(b_inserts, tc.expect_b_inserts, "{}", tc.name);
                assert_eq!(
                    adjust_for_inserts(&tc.spans_b, &b_inserts),
                    tc.expect_merged_spans,
                    "{}",
                    tc.name
                );

                let exp_a_floats = make_float_buckets(&tc.expect_buckets_a);
                let got_a = insert_buckets(
                    &make_float_buckets(&tc.buckets_a),
                    exp_a_floats.len(),
                    &f_inserts,
                    false,
                );
                assert_eq!(got_a, exp_a_floats, "{}", tc.name);

                let exp_b_floats = make_float_buckets(&tc.expect_buckets_b);
                let got_b = insert_buckets(
                    &b_floats,
                    exp_b_floats.len(),
                    &b_inserts,
                    false,
                );
                assert_eq!(got_b, exp_b_floats, "{}", tc.name);
            }
        }
    }
}

// ============================================================================
// histogram_st_test.go
// ============================================================================

fn require_histogram_st_samples(samples: &[(i64, i64, Histogram)]) {
    let mut c = HistogramStChunk::new();
    {
        let mut app = c.appender().unwrap();
        for (st, t, h) in samples {
            match app.append_histogram(None, *st, *t, h, false).unwrap() {
                AppendHistogramStOutcome::Appended { .. } => {}
                AppendHistogramStOutcome::NewChunk(_) => panic!("unexpected new chunk"),
            }
        }
    }
    assert_eq!(c.num_samples(), samples.len());
    let mut it = c.iter();
    for (st, t, _) in samples {
        let got_t = it.next_sample().unwrap().expect("expected sample");
        assert_eq!(got_t, *t);
        assert_eq!(it.at_st(), *st);
    }
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkST
#[test]
fn test_histogram_st_chunk_st() {
    let run_case = |samples: &[(i64, i64, f64)]| {
        let mut chunk = HistogramStChunk::new();
        let mut clone_bytes = Vec::new();
        {
            let mut app = chunk.appender().unwrap();
            for (i, &(st, t, v)) in samples.iter().enumerate() {
                if i + 1 == samples.len() {
                    drop(app);
                    clone_bytes = chunk.as_bytes().to_vec();
                    app = chunk.appender().unwrap();
                }
                let h = Histogram {
                    sum: v,
                    count: (v * 10.0) as u64,
                    ..Default::default()
                };
                match app.append_histogram(None, st, t, &h, false).unwrap() {
                    AppendHistogramStOutcome::Appended { recoded } => assert!(!recoded),
                    AppendHistogramStOutcome::NewChunk(_) => panic!("unexpected new chunk"),
                }
            }
        }
        let mut chunks_to_test = vec![chunk];
        if let Some(&(st, t, v)) = samples.last() {
            let mut clone_chunk = HistogramStChunk::new();
            clone_chunk.reset(clone_bytes);
            let h = Histogram {
                sum: v,
                count: (v * 10.0) as u64,
                ..Default::default()
            };
            let _ = clone_chunk
                .appender()
                .unwrap()
                .append_histogram(None, st, t, &h, false)
                .unwrap();
            chunks_to_test.push(clone_chunk);
        }
        for chk in &chunks_to_test {
            assert_eq!(chk.num_samples(), samples.len());
            let mut it = chk.iter();
            for &(st, t, v) in samples {
                let got_t = it.next_sample().unwrap().unwrap();
                let (h_t, h) = it.at_histogram();
                assert_eq!(got_t, t);
                assert_eq!(h_t, t);
                assert_eq!(it.at_st(), st);
                assert!((h.sum - v).abs() <= 1e-9);
            }
            assert!(it.next_sample().unwrap().is_none());
        }
    };

    run_case(&[
        (0, 1000, 1.5),
        (0, 2000, 2.5),
        (0, 3000, 3.5),
        (0, 4000, 4.5),
    ]);

    let st_times = [0i64, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000];
    let ts_fn = |j: usize| -> i64 { 1000 * (j as i64 + 1) };
    for n in 0..5 {
        let mut samples = vec![(0i64, 0i64, 0.0f64); n];
        let mut idx = vec![0usize; n];
        loop {
            for j in 0..n {
                samples[j] = (st_times[idx[j]], ts_fn(j), j as f64 + 0.5);
            }
            run_case(&samples);
            let mut exhausted = true;
            for j in (0..n).rev() {
                if st_times[idx[j]] < ts_fn(j) {
                    idx[j] += 1;
                    exhausted = false;
                    break;
                }
                idx[j] = 0;
            }
            if exhausted {
                break;
            }
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTBasic
#[test]
fn test_histogram_st_basic() {
    let hs = generate_test_histograms(5);
    let samples: Vec<_> = hs
        .into_iter()
        .enumerate()
        .map(|(i, h)| (0, ((i + 1) * 1000) as i64, h))
        .collect();
    require_histogram_st_samples(&samples);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkAppendAndIterate
#[test]
fn test_histogram_st_chunk_append_and_iterate() {
    let hs = generate_test_histograms(5);
    let sts = [100, 100, 200, 200, 300];
    let samples: Vec<_> = hs
        .into_iter()
        .enumerate()
        .map(|(i, h)| (sts[i], ((i + 1) * 1000) as i64, h))
        .collect();
    require_histogram_st_samples(&samples);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkCounterReset
#[test]
fn test_histogram_st_chunk_counter_reset() {
    let mut c = HistogramStChunk::new();
    let h1 = Histogram {
        count: 10,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        positive_buckets: vec![6, -3],
        ..Default::default()
    };
    {
        let mut app = c.appender().unwrap();
        let out = app.append_histogram(None, 100, 1000, &h1, false).unwrap();
        assert!(matches!(out, AppendHistogramStOutcome::Appended { recoded: false }));
    }

    let mut h2 = h1.clone();
    h2.count = 3;
    h2.sum = 5.0;
    h2.positive_buckets = vec![2, -1];
    h2.counter_reset_hint = CounterResetHint::CounterReset;

    let mut st_chunk = match c
        .appender()
        .unwrap()
        .append_histogram(None, 200, 2000, &h2, false)
        .unwrap()
    {
        AppendHistogramStOutcome::NewChunk(nc) => nc,
        AppendHistogramStOutcome::Appended { .. } => panic!("expected new chunk"),
    };
    assert_eq!(st_chunk.counter_reset_header(), CounterResetHeader::CounterReset);
    assert_eq!(st_chunk.num_samples(), 1);

    {
        let mut it = st_chunk.iter();
        assert_eq!(it.next_sample().unwrap(), Some(2000));
        assert_eq!(it.at_st(), 200);
        assert!(it.next_sample().unwrap().is_none());
    }

    let mut h3 = h2.clone();
    h3.counter_reset_hint = CounterResetHint::UnknownCounterReset;
    h3.count = 8;
    h3.sum = 10.0;
    h3.positive_buckets = vec![4, -2];
    let _ = st_chunk
        .appender()
        .unwrap()
        .append_histogram(None, 300, 3000, &h3, false)
        .unwrap();
    assert_eq!(st_chunk.num_samples(), 2);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkRecode
#[test]
fn test_histogram_st_chunk_recode() {
    let mut c = HistogramStChunk::new();
    let h1 = Histogram {
        count: 27,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        negative_spans: vec![HistogramSpan { offset: 1, length: 1 }],
        negative_buckets: vec![1],
        ..Default::default()
    };
    let _ = c
        .appender()
        .unwrap()
        .append_histogram(None, 100, 1000, &h1, false)
        .unwrap();

    let mut h2 = h1.clone();
    h2.positive_spans = vec![
        HistogramSpan { offset: 0, length: 3 },
        HistogramSpan { offset: 1, length: 1 },
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 3, length: 3 },
    ];
    h2.negative_spans = vec![HistogramSpan { offset: 0, length: 2 }];
    h2.count = 35;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
    h2.negative_buckets = vec![2, -1];

    match c
        .appender()
        .unwrap()
        .append_histogram(None, 200, 2000, &h2, false)
        .unwrap()
    {
        AppendHistogramStOutcome::Appended { recoded } => assert!(recoded),
        AppendHistogramStOutcome::NewChunk(nc) => c = nc,
    }
    assert_eq!(c.num_samples(), 2);

    {
        let mut it = c.iter();
        assert_eq!(it.next_sample().unwrap(), Some(1000));
        assert_eq!(it.at_st(), 100);
        assert_eq!(it.next_sample().unwrap(), Some(2000));
        assert_eq!(it.at_st(), 200);
        assert!(it.next_sample().unwrap().is_none());
    }

    let mut h3 = h2.clone();
    h3.count = 40;
    h3.sum = 35.0;
    for b in &mut h3.positive_buckets {
        *b += 1;
    }
    let _ = c
        .appender()
        .unwrap()
        .append_histogram(None, 300, 3000, &h3, false)
        .unwrap();
    assert_eq!(c.num_samples(), 3);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramST_MoreThan127Samples
#[test]
fn test_histogram_st_more_than_127_samples() {
    let mut c = HistogramStChunk::new();
    let h = Histogram {
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        positive_buckets: vec![6, -3],
        ..Default::default()
    };

    let num_samples = usize::from(MAX_FIRST_ST_CHANGE_ON) + 3;
    {
        let mut app = c.appender().unwrap();
        for i in 0..num_samples {
            let mut hi = h.clone();
            hi.count = 5 + i as u64;
            hi.sum = (18 + i) as f64;
            let _ = app
                .append_histogram(None, 500, 1000 + (i as i64) * 1000, &hi, false)
                .unwrap();
        }
    }
    assert_eq!(c.num_samples(), num_samples);
    let mut it = c.iter();
    for i in 0..num_samples {
        assert_eq!(it.next_sample().unwrap(), Some(1000 + (i as i64) * 1000));
        assert_eq!(it.at_st(), 500);
    }
    assert!(it.next_sample().unwrap().is_none());

    let mut c2 = HistogramStChunk::new();
    let first_batch = usize::from(MAX_FIRST_ST_CHANGE_ON) + 1;
    {
        let mut app2 = c2.appender().unwrap();
        for i in 0..first_batch {
            let mut hi = h.clone();
            hi.count = 5 + i as u64;
            hi.sum = (18 + i) as f64;
            let _ = app2
                .append_histogram(None, 0, 1000 + (i as i64) * 1000, &hi, false)
                .unwrap();
        }
        for i in 0..3 {
            let mut hi = h.clone();
            hi.count = 200 + i as u64;
            hi.sum = (200 + i) as f64;
            let _ = app2
                .append_histogram(None, 100, 200_000 + (i as i64) * 1000, &hi, false)
                .unwrap();
        }
    }
    let mut it2 = c2.iter();
    for i in 0..first_batch {
        assert_eq!(it2.next_sample().unwrap(), Some(1000 + (i as i64) * 1000));
        assert_eq!(it2.at_st(), 0);
    }
    for i in 0..3 {
        assert_eq!(it2.next_sample().unwrap(), Some(200_000 + (i as i64) * 1000));
        assert_eq!(it2.at_st(), 100);
    }
    assert!(it2.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkMixedST
#[test]
fn test_histogram_st_chunk_mixed_st() {
    let hs = generate_test_histograms(5);
    let sts = [0, 0, 100, 0, 200];
    let samples: Vec<_> = hs
        .into_iter()
        .enumerate()
        .map(|(i, h)| (sts[i], ((i + 1) * 1000) as i64, h))
        .collect();
    require_histogram_st_samples(&samples);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkSeek
#[test]
fn test_histogram_st_chunk_seek() {
    let hs = generate_test_histograms(5);
    let sts = [0, 0, 100, 100, 200];
    let mut c = HistogramStChunk::new();
    {
        let mut app = c.appender().unwrap();
        for (i, h) in hs.iter().enumerate() {
            let _ = app
                .append_histogram(None, sts[i], ((i + 1) * 1000) as i64, h, false)
                .unwrap();
        }
    }

    let mut it = c.iter();
    assert_eq!(it.seek(2000).unwrap().unwrap().0, 2000);
    assert_eq!(it.at_st(), 0);

    assert_eq!(it.seek(3000).unwrap().unwrap().0, 3000);
    assert_eq!(it.at_st(), 100);

    assert_eq!(it.seek(3000).unwrap().unwrap().0, 3000);
    assert_eq!(it.at_st(), 100);

    assert_eq!(it.seek(4000).unwrap().unwrap().0, 4000);
    assert_eq!(it.at_st(), 100);

    assert_eq!(it.seek(5000).unwrap().unwrap().0, 5000);
    assert_eq!(it.at_st(), 200);

    assert!(it.seek(5001).unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkStaleSamples
#[test]
fn test_histogram_st_chunk_stale_samples() {
    let mut c = HistogramStChunk::new();
    let h1 = generate_test_histogram(0);
    let stale1 = Histogram {
        sum: stale_nan(),
        ..Default::default()
    };
    let stale2 = Histogram {
        sum: stale_nan(),
        ..Default::default()
    };

    {
        let mut app = c.appender().unwrap();
        let _ = app.append_histogram(None, 100, 1000, &h1, false).unwrap();
        let _ = app
            .append_histogram(None, 200, 2000, &stale1, false)
            .unwrap();
        let _ = app
            .append_histogram(None, 0, 3000, &stale2, false)
            .unwrap();
    }

    let mut it = c.iter();
    assert_eq!(it.next_sample().unwrap(), Some(1000));
    assert_eq!(it.at_st(), 100);
    assert!(!is_stale_nan(it.at_histogram().1.sum));

    assert_eq!(it.next_sample().unwrap(), Some(2000));
    assert_eq!(it.at_st(), 200);
    assert!(is_stale_nan(it.at_histogram().1.sum));

    assert_eq!(it.next_sample().unwrap(), Some(3000));
    assert_eq!(it.at_st(), 0);
    assert!(is_stale_nan(it.at_histogram().1.sum));

    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTAppendOnlyErrorsPreserveChunk
#[test]
fn test_histogram_st_append_only_errors_preserve_chunk() {
    // schema change
    {
        let mut c = HistogramStChunk::new();
        let h = generate_test_histogram(0);
        {
            let mut app = c.appender().unwrap();
            match app.append_histogram(None, 100, 1000, &h, true).unwrap() {
                AppendHistogramStOutcome::Appended { recoded } => assert!(!recoded),
                _ => panic!("unexpected new chunk"),
            }
        }
        let before = c.as_bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.clone();
        h2.schema += 1;
        let err = c
            .appender()
            .unwrap()
            .append_histogram(None, 200, 2000, &h2, true)
            .unwrap_err();
        assert!(matches!(err, ChunkError::HistogramSchemaChange));
        assert_eq!(c.num_samples(), before_samples);
        assert_eq!(c.as_bytes(), before.as_slice());
    }

    // counter reset
    {
        let mut c = HistogramStChunk::new();
        let h = generate_test_histogram(0);
        {
            let mut app = c.appender().unwrap();
            match app.append_histogram(None, 100, 1000, &h, true).unwrap() {
                AppendHistogramStOutcome::Appended { recoded } => assert!(!recoded),
                _ => panic!("unexpected new chunk"),
            }
        }
        let before = c.as_bytes().to_vec();
        let before_samples = c.num_samples();

        let mut h2 = h.clone();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = c
            .appender()
            .unwrap()
            .append_histogram(None, 200, 2000, &h2, true)
            .unwrap_err();
        assert!(matches!(err, ChunkError::HistogramCounterReset));
        assert_eq!(c.num_samples(), before_samples);
        assert_eq!(c.as_bytes(), before.as_slice());
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkGauge
#[test]
fn test_histogram_st_chunk_gauge() {
    let mut c = HistogramStChunk::new();
    let h1 = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        ..Default::default()
    };
    let _ = c
        .appender()
        .unwrap()
        .append_histogram(None, 100, 1000, &h1, false)
        .unwrap();
    assert_eq!(c.counter_reset_header(), CounterResetHeader::GaugeType);

    let mut h2 = h1.clone();
    h2.count += 9;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_spans = vec![
        HistogramSpan { offset: 0, length: 3 },
        HistogramSpan { offset: 1, length: 1 },
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 3, length: 3 },
    ];
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];

    match c
        .appender()
        .unwrap()
        .append_histogram(None, 200, 2000, &h2, false)
        .unwrap()
    {
        AppendHistogramStOutcome::Appended { recoded } => assert!(recoded),
        AppendHistogramStOutcome::NewChunk(nc) => c = nc,
    }
    assert_eq!(c.counter_reset_header(), CounterResetHeader::GaugeType);

    {
        let mut it = c.iter();
        assert_eq!(it.next_sample().unwrap(), Some(1000));
        assert_eq!(it.at_st(), 100);
        assert_eq!(it.next_sample().unwrap(), Some(2000));
        assert_eq!(it.at_st(), 200);
        assert!(it.next_sample().unwrap().is_none());
    }

    let mut h3 = h2.clone();
    h3.sum = 35.0;
    for b in &mut h3.positive_buckets {
        *b += 1;
    }
    let _ = c
        .appender()
        .unwrap()
        .append_histogram(None, 300, 3000, &h3, false)
        .unwrap();
    assert_eq!(c.num_samples(), 3);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunk_CounterResetHeader
#[test]
fn test_histogram_st_chunk_counter_reset_header() {
    let mut c = HistogramStChunk::new();
    assert_eq!(c.as_bytes().len(), 3);
    let h = generate_test_histogram(0);
    let _ = c
        .appender()
        .unwrap()
        .append_histogram(None, 100, 1000, &h, false)
        .unwrap();

    let byte2_before = c.as_bytes()[2];
    for cr in [
        CounterResetHeader::UnknownCounterReset,
        CounterResetHeader::CounterReset,
        CounterResetHeader::NotCounterReset,
        CounterResetHeader::GaugeType,
    ] {
        {
            let mut happ = c.appender().unwrap();
            happ.set_counter_reset_header(cr);
            assert_eq!(happ.num_samples(), 1);
        }
        assert_eq!(c.counter_reset_header(), cr);
        assert_eq!(c.num_samples(), 1);
        assert_eq!(c.as_bytes()[2], byte2_before);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_st_test.go::TestHistogramSTChunkOverFlowPanics
#[test]
fn test_histogram_st_chunk_overflow_panics() {
    let mut chunk = HistogramStChunk::new();
    let mut app = chunk.appender().unwrap();
    let mut h = Histogram {
        zero_threshold: 1e-128,
        ..Default::default()
    };
    for i in 0..HISTOGRAM_ST_SAMPLE_COUNT_MASK {
        h.count = u64::from(i);
        h.zero_count = u64::from(i);
        let _ = app
            .append_histogram(None, 0, i64::from(i), &h, true)
            .unwrap();
    }
    h.count = u64::from(HISTOGRAM_ST_SAMPLE_COUNT_MASK);
    h.zero_count = u64::from(HISTOGRAM_ST_SAMPLE_COUNT_MASK);
    let err = app
        .append_histogram(None, 0, i64::from(HISTOGRAM_ST_SAMPLE_COUNT_MASK), &h, true)
        .unwrap_err();
    assert!(matches!(err, ChunkError::CapacityExceeded));
}

// ============================================================================
// histogram_test.go
// ============================================================================

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestFirstHistogramExplicitCounterReset
#[test]
fn test_first_histogram_explicit_counter_reset() {
    let tests = [
        (
            CounterResetHint::CounterReset,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            CounterResetHint::NotCounterReset,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            CounterResetHint::UnknownCounterReset,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        ),
        (
            CounterResetHint::GaugeType,
            CounterResetHeader::GaugeType,
            CounterResetHint::GaugeType,
        ),
    ];

    for (hint, exp_header, exp_hint) in tests {
        let h = Histogram {
            counter_reset_hint: hint,
            ..Default::default()
        };
        let mut chk = HistogramChunk::new();
        let out = chk
            .appender()
            .unwrap()
            .append_histogram(None, 0, 0, &h, false)
            .unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { recoded: false }));
        assert_eq!(chk.counter_reset_header(), exp_header);
        assert_first_int_histogram_sample_hint(&chk, exp_hint);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramChunkSameBuckets
#[test]
fn test_histogram_chunk_same_buckets() {
    let mut c = HistogramChunk::new();
    let mut exp: Vec<(i64, Histogram, FloatHistogram)> = Vec::new();
    assert_eq!(c.num_samples(), 0);

    let mut ts = 1_234_567_890i64;
    let mut h = Histogram {
        count: 15,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-100,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1, 1, -1, 0],
        negative_spans: vec![
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 2, length: 3 },
        ],
        negative_buckets: vec![2, 1, -1, -1],
        ..Default::default()
    };
    {
        let mut app = c.appender().unwrap();
        let out = app.append_histogram(None, 0, ts, &h, false).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
        exp.push((ts, h.clone(), h.to_float()));

        ts += 16;
        h.count = 32;
        h.zero_count += 1;
        h.sum = 24.4;
        h.positive_buckets = vec![5, -2, 1, -2];
        h.negative_buckets = vec![4, -1, 1, -1];
        let out = app.append_histogram(None, 0, ts, &h, false).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
        let mut h_exp = h.clone();
        h_exp.counter_reset_hint = CounterResetHint::NotCounterReset;
        exp.push((ts, h_exp.clone(), h_exp.to_float()));
    }
    assert_eq!(c.num_samples(), 2);

    {
        let mut app = c.appender().unwrap();
        ts += 14;
        h.count = 54;
        h.zero_count += 2;
        h.sum = 24.4;
        h.positive_buckets = vec![6, 1, -3, 6];
        h.negative_buckets = vec![5, 1, -2, 3];
        let out = app.append_histogram(None, 0, ts, &h, false).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
        let mut h_exp = h.clone();
        h_exp.counter_reset_hint = CounterResetHint::NotCounterReset;
        exp.push((ts, h_exp.clone(), h_exp.to_float()));
    }
    assert_eq!(c.num_samples(), 3);

    // 1. Expand iterator in simple case.
    let mut it = c.iter();
    let mut act = Vec::new();
    while let Some(t) = it.next_sample().unwrap() {
        let (ts, h) = it.at_histogram();
        let (fts, fh) = it.at_float_histogram();
        assert_eq!(ts, t);
        assert_eq!(ts, fts);
        act.push((ts, h, fh));
    }
    assert_eq!(act, exp);

    // 2. Reuse iterator after reset.
    it.reset(c.as_bytes());
    let mut act2 = Vec::new();
    while it.next_sample().unwrap().is_some() {
        let (ts, h) = it.at_histogram();
        let (fts, fh) = it.at_float_histogram();
        assert_eq!(ts, fts);
        act2.push((ts, h, fh));
    }
    assert_eq!(act2, exp);

    // 3. Recycle an iterator that was never accessed.
    let mut it_x = c.iter();
    while it_x.next_sample().unwrap().is_some() {}
    it_x.reset(c.as_bytes());
    let mut act3 = Vec::new();
    while it_x.next_sample().unwrap().is_some() {
        let (ts, h) = it_x.at_histogram();
        let (fts, fh) = it_x.at_float_histogram();
        assert_eq!(ts, fts);
        act3.push((ts, h, fh));
    }
    assert_eq!(act3, exp);

    // 4. Seek.
    let mid = exp.len() / 2;
    let mut it4 = c.iter();
    let mut act4 = Vec::new();
    assert!(it4.seek(exp[mid].0).unwrap().is_some());
    assert!(it4.seek(exp[mid].0).unwrap().is_some());
    assert!(it4.seek(exp[mid].0).unwrap().is_some());
    let (ts, h) = it4.at_histogram();
    let (fts, fh) = it4.at_float_histogram();
    assert_eq!(ts, fts);
    act4.push((ts, h, fh));
    while it4.next_sample().unwrap().is_some() {
        let (ts, h) = it4.at_histogram();
        let (fts, fh) = it4.at_float_histogram();
        assert_eq!(ts, fts);
        act4.push((ts, h, fh));
    }
    assert_eq!(act4, exp[mid..]);
    assert!(it4.seek(exp.last().unwrap().0 + 1).unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramChunkBucketChanges
#[test]
fn test_histogram_chunk_bucket_changes() {
    let mut c = HistogramChunk::new();
    let ts1 = 1_234_567_890i64;
    let mut h1 = Histogram {
        count: 27,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        negative_spans: vec![HistogramSpan { offset: 1, length: 1 }],
        negative_buckets: vec![1],
        ..Default::default()
    };
    {
        let mut app = c.appender().unwrap();
        let out = app.append_histogram(None, 0, ts1, &h1, false).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
    }

    let ts2 = ts1 + 16;
    let mut h2 = h1.clone();
    h2.positive_spans = vec![
        HistogramSpan { offset: 0, length: 3 },
        HistogramSpan { offset: 1, length: 1 },
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 3, length: 3 },
    ];
    h2.negative_spans = vec![HistogramSpan { offset: 0, length: 2 }];
    h2.count = 35;
    h2.zero_count += 1;
    h2.sum = 30.0;
    h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
    h2.negative_buckets = vec![2, -1];

    {
        let mut h_app = c.appender().unwrap();
        let (p_fwd, n_fwd, p_bwd, n_bwd, ok, cr) = h_app.appendable(&h2);
        assert!(!p_fwd.is_empty());
        assert!(!n_fwd.is_empty());
        assert!(p_bwd.is_empty());
        assert!(n_bwd.is_empty());
        assert!(ok);
        assert_eq!(cr, CounterResetHeader::NotCounterReset);
        h_app.recode_chunk(&p_fwd, &n_fwd, &h2.positive_spans, &h2.negative_spans)
            .unwrap();
        let out = h_app.append_histogram(None, 0, ts2, &h2, false).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
    }
    assert_eq!(c.num_samples(), 2);

    h1.positive_spans = h2.positive_spans.clone();
    h1.positive_buckets = vec![6, -3, -3, 3, -3, 0, 2, 2, 1, -5, 1];
    h1.negative_spans = h2.negative_spans.clone();
    h1.negative_buckets = vec![0, 1];
    let mut h_exp = h2.clone();
    h_exp.counter_reset_hint = CounterResetHint::NotCounterReset;
    let exp = vec![
        (ts1, h1.clone(), h1.to_float()),
        (ts2, h_exp.clone(), h_exp.to_float()),
    ];
    let mut it = c.iter();
    let mut act = Vec::new();
    while it.next_sample().unwrap().is_some() {
        let (ts, h) = it.at_histogram();
        let (fts, fh) = it.at_float_histogram();
        assert_eq!(ts, fts);
        act.push((ts, h, fh));
    }
    assert_eq!(act, exp);
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramChunkAppendable
#[test]
fn test_histogram_chunk_appendable() {
    let eh = Histogram {
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        ..Default::default()
    };

    let cbh = Histogram {
        count: 24,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        custom_values: (1..=20).map(|i| i as f64).collect(),
        ..Default::default()
    };

    let setup = |h: &Histogram| -> (HistogramChunk, i64, Histogram) {
        let mut c = HistogramChunk::new();
        let ts = 1_234_567_890i64;
        let out = c
            .appender()
            .unwrap()
            .append_histogram(None, 0, ts, h, false)
            .unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
        assert_eq!(c.num_samples(), 1);
        assert_eq!(c.counter_reset_header(), CounterResetHeader::UnknownCounterReset);
        (c, ts, h.clone())
    };

    let assert_new_chunk = |old: &mut HistogramChunk,
                            ts: i64,
                            h: &Histogram,
                            exp_header: CounterResetHeader,
                            exp_hint: CounterResetHint| {
        let old_bytes = old.as_bytes().to_vec();
        let out = old
            .appender()
            .unwrap()
            .append_histogram(None, 0, ts, h, false)
            .unwrap();
        assert_eq!(old.as_bytes(), old_bytes.as_slice());
        let AppendHistogramOutcome::NewChunk(nc) = out else {
            panic!("expected new chunk");
        };
        assert_eq!(nc.counter_reset_header(), exp_header);
        assert_hist_sample_count(&nc, 1);
        assert_first_int_histogram_sample_hint(&nc, exp_hint);
    };

    let assert_no_new_chunk =
        |curr: &mut HistogramChunk, ts: i64, h: &Histogram, exp_header: CounterResetHeader| {
            let prev_len = curr.as_bytes().len();
            let out = curr
                .appender()
                .unwrap()
                .append_histogram(None, 0, ts, h, false)
                .unwrap();
            assert!(curr.as_bytes().len() > prev_len);
            assert!(matches!(
                out,
                AppendHistogramOutcome::Appended { recoded: false }
            ));
            assert_eq!(curr.counter_reset_header(), exp_header);
            assert_hist_sample_count(curr, 2);
        };

    let assert_recoded_chunk =
        |prev: &mut HistogramChunk, ts: i64, h: &Histogram, exp_header: CounterResetHeader| {
            let out = prev
                .appender()
                .unwrap()
                .append_histogram(None, 0, ts, h, false)
                .unwrap();
            match out {
                AppendHistogramOutcome::Appended { recoded } => {
                    assert!(recoded);
                    assert_eq!(prev.counter_reset_header(), exp_header);
                    assert_hist_sample_count(prev, 2);
                }
                AppendHistogramOutcome::NewChunk(nc) => {
                    assert_eq!(nc.counter_reset_header(), exp_header);
                    assert_hist_sample_count(&nc, 2);
                }
            }
        };

    // Schema change.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.schema += 1;
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Zero threshold change.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.zero_threshold += 0.1;
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // More buckets.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 9;
        h2.zero_count += 1;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
        {
            let h_app = c.appender().unwrap();
            let (p_i, n_i, p_bi, n_bi, ok, cr) = h_app.appendable(&h2);
            assert!(!p_i.is_empty());
            assert!(n_i.is_empty());
            assert!(p_bi.is_empty());
            assert!(n_bi.is_empty());
            assert!(ok);
            assert_eq!(cr, CounterResetHeader::NotCounterReset);
        }
        assert_recoded_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // Bucket missing.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 5, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 21.0;
        h2.positive_buckets = vec![6, -3, -1, 2, 1, -4];
        {
            let h_app = c.appender().unwrap();
            let (p_i, n_i, p_bi, n_bi, ok, cr) = h_app.appendable(&h2);
            assert!(p_i.is_empty() && n_i.is_empty() && p_bi.is_empty() && n_bi.is_empty());
            assert!(!ok);
            assert_eq!(cr, CounterResetHeader::CounterReset);
        }
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Buckets missing but empty.
    {
        let mut empty_h = eh.clone();
        empty_h.positive_buckets = vec![6, -6, 1, 1, -2, 1, 1];
        let (mut c, ts, h1) = setup(&empty_h);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 4, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.positive_buckets = vec![7, -5, 1, 0, 1];
        {
            let h_app = c.appender().unwrap();
            let (p_i, n_i, p_bi, n_bi, ok, cr) = h_app.appendable(&h2);
            assert!(p_i.is_empty() && n_i.is_empty() && !p_bi.is_empty() && n_bi.is_empty());
            assert!(ok);
            assert_eq!(cr, CounterResetHeader::NotCounterReset);
        }
        assert_no_new_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // New buckets AND empty buckets missing.
    {
        let mut empty_h = eh.clone();
        empty_h.positive_buckets = vec![6, -6, 1, 1, -2, 1, 1];
        let (mut c, ts, h1) = setup(&empty_h);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 4, length: 1 },
            HistogramSpan { offset: 1, length: 2 },
        ];
        h2.positive_buckets = vec![7, -5, 1, 0, 1, 1];
        {
            let h_app = c.appender().unwrap();
            let (p_i, n_i, p_bi, n_bi, ok, cr) = h_app.appendable(&h2);
            assert!(!p_i.is_empty() && n_i.is_empty() && !p_bi.is_empty() && n_bi.is_empty());
            assert!(ok);
            assert_eq!(cr, CounterResetHeader::NotCounterReset);
        }
        assert_recoded_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // Counter reset while buckets are same.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.sum = 23.0;
        h2.positive_buckets = vec![6, -4, 1, -1, 2, 1, -4];
        {
            let h_app = c.appender().unwrap();
            let (p_i, n_i, p_bi, n_bi, ok, cr) = h_app.appendable(&h2);
            assert!(p_i.is_empty() && n_i.is_empty() && p_bi.is_empty() && n_bi.is_empty());
            assert!(!ok);
            assert_eq!(cr, CounterResetHeader::CounterReset);
        }
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Counter reset while new buckets added.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.sum = 29.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 0];
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Counter reset while new buckets added before first bucket.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: -3, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 26.0;
        h2.positive_buckets = vec![1, 1, 3, -2, 0, -1, 2, 1, -4];
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Explicit counter reset.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }

    // Start new chunk explicitly with prev state (appendable).
    {
        let (mut c, ts, h1) = setup(&eh);
        let h_app_state = c.appender().unwrap().into_state();
        let h2 = h1.clone();
        let mut next_chunk = HistogramChunk::new();
        let out = next_chunk
            .appender()
            .unwrap()
            .append_histogram(Some(&h_app_state), 0, ts + 1, &h2, false)
            .unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { recoded: false }));
        assert_hist_sample_count(&next_chunk, 1);
        assert_eq!(next_chunk.counter_reset_header(), CounterResetHeader::NotCounterReset);
        assert_first_int_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    // Start new chunk explicitly with prev state (not appendable).
    {
        let (mut c, ts, h1) = setup(&eh);
        let h_app_state = c.appender().unwrap().into_state();
        let mut h2 = h1.clone();
        h2.count -= 1;
        let mut next_chunk = HistogramChunk::new();
        let out = next_chunk
            .appender()
            .unwrap()
            .append_histogram(Some(&h_app_state), 0, ts + 1, &h2, false)
            .unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { recoded: false }));
        assert_hist_sample_count(&next_chunk, 1);
        assert_eq!(next_chunk.counter_reset_header(), CounterResetHeader::CounterReset);
        assert_first_int_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    // Start new chunk explicitly with prev state (would need recode).
    {
        let (mut c, ts, h1) = setup(&eh);
        let h_app_state = c.appender().unwrap().into_state();
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 9;
        h2.zero_count += 1;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
        let mut next_chunk = HistogramChunk::new();
        let out = next_chunk
            .appender()
            .unwrap()
            .append_histogram(Some(&h_app_state), 0, ts + 1, &h2, false)
            .unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { recoded: false }));
        assert_hist_sample_count(&next_chunk, 1);
        assert_eq!(next_chunk.counter_reset_header(), CounterResetHeader::NotCounterReset);
        assert_first_int_histogram_sample_hint(&next_chunk, CounterResetHint::UnknownCounterReset);
    }

    // Start chunk with empty bucket, next has it missing.
    {
        let mut h1 = eh.clone();
        h1.positive_spans = Vec::with_capacity(eh.positive_spans.len() + 1);
        h1.positive_spans.push(HistogramSpan {
            offset: eh.positive_spans[0].offset - 10,
            length: 1,
        });
        h1.positive_spans.push(HistogramSpan {
            offset: eh.positive_spans[0].offset + 9,
            length: eh.positive_spans[0].length,
        });
        h1.positive_spans.extend_from_slice(&eh.positive_spans[1..]);
        h1.positive_buckets = vec![0];
        h1.positive_buckets.extend_from_slice(&eh.positive_buckets);
        let (mut c, ts, _) = setup(&h1);
        let h2 = eh.clone();
        {
            let h_app = c.appender().unwrap();
            let (p_i, n_i, p_bi, n_bi, ok, cr) = h_app.appendable(&h2);
            assert!(p_i.is_empty() && n_i.is_empty() && !p_bi.is_empty() && n_bi.is_empty());
            assert!(ok);
            assert_eq!(cr, CounterResetHeader::NotCounterReset);
        }
        assert_no_new_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // Custom buckets: no change, increase, decrease, bounds change, more buckets.
    {
        let (mut c, ts, h1) = setup(&cbh);
        assert!(c.appender().unwrap().appendable(&h1).4);
        assert_no_new_chunk(&mut c, ts + 1, &h1, CounterResetHeader::UnknownCounterReset);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count += 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -3];
        assert!(c.appender().unwrap().appendable(&h2).4);
        assert_no_new_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count -= 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -5];
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.custom_values = (1..=21).map(|i| i as f64).collect();
        assert!(!c.appender().unwrap().appendable(&h2).4);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::CounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 6;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
        assert!(c.appender().unwrap().appendable(&h2).4);
        assert_recoded_chunk(&mut c, ts + 1, &h2, CounterResetHeader::UnknownCounterReset);
    }

    // Schema 2 / zeroThreshold 1e-120 -> UnknownCounterReset.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.schema = 2;
        let (p_i, n_i, p_bi, n_bi, ok, cr) = c.appender().unwrap().appendable(&h2);
        assert!(p_i.is_empty() && n_i.is_empty() && p_bi.is_empty() && n_bi.is_empty());
        assert!(!ok);
        assert_eq!(cr, CounterResetHeader::UnknownCounterReset);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.zero_threshold = 1e-120;
        let (p_i, n_i, p_bi, n_bi, ok, cr) = c.appender().unwrap().appendable(&h2);
        assert!(p_i.is_empty() && n_i.is_empty() && p_bi.is_empty() && n_bi.is_empty());
        assert!(!ok);
        assert_eq!(cr, CounterResetHeader::UnknownCounterReset);
        assert_new_chunk(
            &mut c,
            ts + 1,
            &h2,
            CounterResetHeader::UnknownCounterReset,
            CounterResetHint::UnknownCounterReset,
        );
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramChunkAppendableWithEmptySpan
#[test]
fn test_histogram_chunk_appendable_with_empty_span() {
    let base_neg = vec![
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 2, length: 0 },
        HistogramSpan { offset: 2, length: 3 },
    ];
    let tests = [
        (
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 2, length: 4 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![
                    HistogramSpan { offset: 3, length: 4 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 3, length: 3 },
                ],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 2, length: 3 },
                ],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            Histogram {
                schema: 0,
                count: 21,
                sum: 1234.5,
                zero_threshold: 0.001,
                zero_count: 4,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 3, length: 0 },
                    HistogramSpan { offset: 4, length: 3 },
                ],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                negative_spans: base_neg.clone(),
                negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
                ..Default::default()
            },
            Histogram {
                schema: 0,
                count: 37,
                sum: 2345.6,
                zero_threshold: 0.001,
                zero_count: 5,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 3, length: 0 },
                    HistogramSpan { offset: 1, length: 0 },
                    HistogramSpan { offset: 4, length: 3 },
                ],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                negative_spans: base_neg,
                negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
                ..Default::default()
            },
        ),
        (
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 7,
                sum: 1234.5,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
                custom_values: (1..=10).map(|i| i as f64).collect(),
                ..Default::default()
            },
            Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                count: 10,
                sum: 2345.6,
                positive_spans: vec![
                    HistogramSpan { offset: 0, length: 4 },
                    HistogramSpan { offset: 0, length: 0 },
                    HistogramSpan { offset: 0, length: 3 },
                ],
                positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
                custom_values: (1..=10).map(|i| i as f64).collect(),
                ..Default::default()
            },
        ),
    ];

    for (h1, h2) in tests {
        let mut c = HistogramChunk::new();
        {
            let mut app = c.appender().unwrap();
            app.append_histogram(None, 1, 0, &h1, true).unwrap();
            let (p_i, n_i, bp_i, bn_i, ok, cr) = app.appendable(&h2);
            assert!(p_i.is_empty() && n_i.is_empty() && bp_i.is_empty() && bn_i.is_empty());
            assert!(ok);
            assert_eq!(cr, CounterResetHeader::NotCounterReset);
        }
        assert_eq!(c.num_samples(), 1);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestAtFloatHistogram
#[test]
fn test_at_float_histogram() {
    let p_spans = vec![
        HistogramSpan { offset: 0, length: 4 },
        HistogramSpan { offset: 0, length: 0 },
        HistogramSpan { offset: 0, length: 3 },
    ];
    let n_spans = vec![
        HistogramSpan { offset: 1, length: 4 },
        HistogramSpan { offset: 2, length: 0 },
        HistogramSpan { offset: 2, length: 3 },
    ];
    let input = [
        Histogram {
            schema: 0,
            count: 21,
            sum: 1234.5,
            zero_threshold: 0.001,
            zero_count: 4,
            positive_spans: p_spans.clone(),
            positive_buckets: vec![1, 1, -1, 0, 0, 0, 0],
            negative_spans: n_spans.clone(),
            negative_buckets: vec![1, 1, -1, 1, 0, 0, 0],
            ..Default::default()
        },
        Histogram {
            schema: 0,
            count: 36,
            sum: 2345.6,
            zero_threshold: 0.001,
            zero_count: 5,
            positive_spans: p_spans.clone(),
            positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
            negative_spans: n_spans.clone(),
            negative_buckets: vec![1, 3, -2, 5, -2, 0, -3],
            ..Default::default()
        },
        Histogram {
            schema: 0,
            count: 36,
            sum: 1111.1,
            zero_threshold: 0.001,
            zero_count: 5,
            positive_spans: p_spans.clone(),
            positive_buckets: vec![1, 2, -2, 2, -1, 0, 0],
            negative_spans: n_spans.clone(),
            negative_buckets: vec![1, 3, -2, 5, -1, 0, -3],
            ..Default::default()
        },
    ];

    let exp_output = [
        FloatHistogram {
            schema: 0,
            count: 21.0,
            sum: 1234.5,
            zero_threshold: 0.001,
            zero_count: 4.0,
            positive_spans: p_spans.clone(),
            positive_buckets: vec![1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            negative_spans: n_spans.clone(),
            negative_buckets: vec![1.0, 2.0, 1.0, 2.0, 2.0, 2.0, 2.0],
            ..Default::default()
        },
        FloatHistogram {
            counter_reset_hint: CounterResetHint::NotCounterReset,
            schema: 0,
            count: 36.0,
            sum: 2345.6,
            zero_threshold: 0.001,
            zero_count: 5.0,
            positive_spans: p_spans.clone(),
            positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
            negative_spans: n_spans.clone(),
            negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 5.0, 5.0, 2.0],
            ..Default::default()
        },
        FloatHistogram {
            counter_reset_hint: CounterResetHint::NotCounterReset,
            schema: 0,
            count: 36.0,
            sum: 1111.1,
            zero_threshold: 0.001,
            zero_count: 5.0,
            positive_spans: p_spans,
            positive_buckets: vec![1.0, 3.0, 1.0, 3.0, 2.0, 2.0, 2.0],
            negative_spans: n_spans,
            negative_buckets: vec![1.0, 4.0, 2.0, 7.0, 6.0, 6.0, 3.0],
            ..Default::default()
        },
    ];

    let mut chk = HistogramChunk::new();
    {
        let mut app = chk.appender().unwrap();
        for (i, h) in input.iter().enumerate() {
            let out = app.append_histogram(None, 0, i as i64, h, false).unwrap();
            assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
        }
    }
    let mut it = chk.iter();
    let mut i = 0;
    while let Some(t) = it.next_sample().unwrap() {
        let (ts, fh) = it.at_float_histogram();
        assert_eq!(ts, i as i64);
        assert_eq!(t, i as i64);
        assert_eq!(fh, exp_output[i], "histogram {i} unequal");
        i += 1;
    }
    assert_eq!(i, exp_output.len());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramChunkAppendableGauge
#[test]
fn test_histogram_chunk_appendable_gauge() {
    let eh = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 5,
        zero_count: 2,
        sum: 18.4,
        zero_threshold: 1e-125,
        schema: 1,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        ..Default::default()
    };

    let cbh = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        count: 24,
        sum: 18.4,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ],
        positive_buckets: vec![6, -3, 0, -1, 2, 1, -4],
        custom_values: (1..=20).map(|i| i as f64).collect(),
        ..Default::default()
    };

    let setup = |h: &Histogram| -> (HistogramChunk, i64, Histogram) {
        let mut c = HistogramChunk::new();
        let ts = 1_234_567_890i64;
        let out = c
            .appender()
            .unwrap()
            .append_histogram(None, 0, ts, h, false)
            .unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { .. }));
        assert_eq!(c.counter_reset_header(), CounterResetHeader::GaugeType);
        (c, ts, h.clone())
    };

    let assert_new_gauge = |old: &mut HistogramChunk, ts: i64, h: &Histogram| {
        let out = old
            .appender()
            .unwrap()
            .append_histogram(None, 0, ts, h, false)
            .unwrap();
        let AppendHistogramOutcome::NewChunk(nc) = out else {
            panic!("expected new chunk");
        };
        assert_eq!(nc.counter_reset_header(), CounterResetHeader::GaugeType);
        assert_hist_sample_count(&nc, 1);
        assert_first_int_histogram_sample_hint(&nc, CounterResetHint::GaugeType);
    };

    let assert_no_new_gauge = |curr: &mut HistogramChunk, ts: i64, h: &Histogram| {
        let out = curr
            .appender()
            .unwrap()
            .append_histogram(None, 0, ts, h, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendHistogramOutcome::Appended { recoded: false }
        ));
        assert_eq!(curr.counter_reset_header(), CounterResetHeader::GaugeType);
        assert_hist_sample_count(curr, 2);
    };

    let assert_recoded_gauge = |curr: &mut HistogramChunk, ts: i64, h: &Histogram| {
        let out = curr
            .appender()
            .unwrap()
            .append_histogram(None, 0, ts, h, false)
            .unwrap();
        match out {
            AppendHistogramOutcome::Appended { recoded } => {
                assert!(recoded);
                assert_eq!(curr.counter_reset_header(), CounterResetHeader::GaugeType);
                assert_hist_sample_count(curr, 2);
            }
            AppendHistogramOutcome::NewChunk(nc) => {
                assert_eq!(nc.counter_reset_header(), CounterResetHeader::GaugeType);
                assert_hist_sample_count(&nc, 2);
            }
        }
    };

    // Schema change.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.schema += 1;
        assert!(!c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_new_gauge(&mut c, ts + 1, &h2);
    }

    // Zero threshold change.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.zero_threshold += 0.1;
        assert!(!c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_new_gauge(&mut c, ts + 1, &h2);
    }

    // More buckets.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 9;
        h2.zero_count += 1;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
        {
            let g = c.appender().unwrap().appendable_gauge(&h2);
            assert!(!g.p_fwd.is_empty() && g.n_fwd.is_empty() && g.p_bwd.is_empty() && g.n_bwd.is_empty() && g.ok_to_append);
        }
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }

    // Buckets missing.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 4, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.count -= 4;
        h2.sum -= 1.0;
        h2.positive_buckets = vec![6, -3, 0, -1, 3, -4];
        {
            let g = c.appender().unwrap().appendable_gauge(&h2);
            assert!(g.p_fwd.is_empty() && g.n_fwd.is_empty() && !g.p_bwd.is_empty() && g.n_bwd.is_empty() && g.ok_to_append);
        }
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }

    // Buckets missing and new buckets.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 5, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 21.0;
        h2.positive_buckets = vec![6, -3, -1, 2, 1, -4];
        {
            let g = c.appender().unwrap().appendable_gauge(&h2);
            assert!(!g.p_fwd.is_empty() && !g.p_bwd.is_empty() && g.n_fwd.is_empty() && g.n_bwd.is_empty() && g.ok_to_append);
        }
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }

    // Counter reset while buckets same.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.sum = 23.0;
        h2.positive_buckets = vec![6, -4, 1, -1, 2, 1, -4];
        {
            let g = c.appender().unwrap().appendable_gauge(&h2);
            assert!(g.p_fwd.is_empty() && g.n_fwd.is_empty() && g.p_bwd.is_empty() && g.n_bwd.is_empty() && g.ok_to_append);
        }
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }

    // Counter reset while new buckets added.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.sum = 29.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 0];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }

    // Counter reset while new buckets added before first bucket.
    {
        let (mut c, ts, h1) = setup(&eh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: -3, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
            HistogramSpan { offset: 2, length: 1 },
            HistogramSpan { offset: 3, length: 2 },
            HistogramSpan { offset: 3, length: 1 },
            HistogramSpan { offset: 1, length: 1 },
        ];
        h2.sum = 26.0;
        h2.positive_buckets = vec![1, 1, 3, -2, 0, -1, 2, 1, -4];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }

    // Custom buckets gauge cases.
    {
        let (mut c, ts, h1) = setup(&cbh);
        assert!(c.appender().unwrap().appendable_gauge(&h1).ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h1);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count += 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -3];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.count -= 1;
        h2.positive_buckets = vec![6, -3, 0, -1, 2, 1, -5];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_no_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.custom_values = (1..=21).map(|i| i as f64).collect();
        assert!(!c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_new_gauge(&mut c, ts + 1, &h2);
    }
    {
        let (mut c, ts, h1) = setup(&cbh);
        let mut h2 = h1.clone();
        h2.positive_spans = vec![
            HistogramSpan { offset: 0, length: 3 },
            HistogramSpan { offset: 1, length: 1 },
            HistogramSpan { offset: 1, length: 4 },
            HistogramSpan { offset: 3, length: 3 },
        ];
        h2.count += 6;
        h2.sum = 30.0;
        h2.positive_buckets = vec![7, -2, -4, 2, -2, -1, 2, 3, 0, -5, 1];
        assert!(c.appender().unwrap().appendable_gauge(&h2).ok_to_append);
        assert_recoded_gauge(&mut c, ts + 1, &h2);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramAppendOnlyErrors
#[test]
fn test_histogram_append_only_errors() {
    // schema change error
    {
        let mut c = HistogramChunk::new();
        let h = generate_test_histogram(0);
        let mut app = c.appender().unwrap();
        let out = app.append_histogram(None, 0, 1, &h, true).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { recoded: false }));
        let mut h2 = h.clone();
        h2.schema += 1;
        let err = app.append_histogram(None, 0, 2, &h2, true).unwrap_err();
        assert!(matches!(err, ChunkError::HistogramSchemaChange));
    }
    // counter reset error
    {
        let mut c = HistogramChunk::new();
        let h = generate_test_histogram(0);
        let mut app = c.appender().unwrap();
        let out = app.append_histogram(None, 0, 1, &h, true).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { recoded: false }));
        let mut h2 = h.clone();
        h2.counter_reset_hint = CounterResetHint::CounterReset;
        let err = app.append_histogram(None, 0, 2, &h2, true).unwrap_err();
        assert!(matches!(err, ChunkError::HistogramCounterReset));
    }
    // counter reset error with custom buckets
    {
        let mut c = HistogramChunk::new();
        let h = generate_test_custom_buckets_histogram(0);
        let mut app = c.appender().unwrap();
        let out = app.append_histogram(None, 0, 1, &h, true).unwrap();
        assert!(matches!(out, AppendHistogramOutcome::Appended { recoded: false }));
        let mut h2 = h.clone();
        h2.custom_values = vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
        let err = app.append_histogram(None, 0, 2, &h2, true).unwrap_err();
        assert!(matches!(err, ChunkError::HistogramCounterReset));
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramUniqueSpansAfterNextWithAtHistogram
#[test]
fn test_histogram_unique_spans_after_next_with_at_histogram() {
    let h1 = Histogram {
        schema: 1,
        zero_threshold: 1e-100,
        count: 10,
        zero_count: 2,
        sum: 15.0,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1, 2, 3, 4],
        negative_spans: vec![HistogramSpan { offset: 1, length: 1 }],
        negative_buckets: vec![2],
        ..Default::default()
    };
    let h2 = h1.clone();

    let mut c = HistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_histogram(None, 0, 0, &h1, false).unwrap();
        app.append_histogram(None, 0, 1, &h2, false).unwrap();
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh1) = it.at_histogram();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh2) = it.at_histogram();

    assert_eq!(rh1.positive_spans, h1.positive_spans);
    assert_eq!(rh1.negative_spans, h1.negative_spans);
    assert_eq!(rh2.positive_spans, h1.positive_spans);
    assert_eq!(rh2.negative_spans, h1.negative_spans);
    assert_ne!(rh1.positive_spans.as_ptr(), rh2.positive_spans.as_ptr());
    assert_ne!(rh1.negative_spans.as_ptr(), rh2.negative_spans.as_ptr());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramUniqueSpansAfterNextWithAtFloatHistogram
#[test]
fn test_histogram_unique_spans_after_next_with_at_float_histogram() {
    let h1 = Histogram {
        schema: 1,
        zero_threshold: 1e-100,
        count: 10,
        zero_count: 2,
        sum: 15.0,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1, 2, 3, 4],
        negative_spans: vec![HistogramSpan { offset: 1, length: 1 }],
        negative_buckets: vec![2],
        ..Default::default()
    };
    let h2 = h1.clone();

    let mut c = HistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_histogram(None, 0, 0, &h1, false).unwrap();
        app.append_histogram(None, 0, 1, &h2, false).unwrap();
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh1) = it.at_float_histogram();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh2) = it.at_float_histogram();

    assert_eq!(rh1.positive_spans, h1.positive_spans);
    assert_eq!(rh1.negative_spans, h1.negative_spans);
    assert_eq!(rh2.positive_spans, h1.positive_spans);
    assert_eq!(rh2.negative_spans, h1.negative_spans);
    assert_ne!(rh1.positive_spans.as_ptr(), rh2.positive_spans.as_ptr());
    assert_ne!(rh1.negative_spans.as_ptr(), rh2.negative_spans.as_ptr());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramCustomValuesInternedAfterNextWithAtHistogram
#[test]
fn test_histogram_custom_values_interned_after_next_with_at_histogram() {
    let h1 = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 10,
        sum: 15.0,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1, 2, 3, 4],
        custom_values: vec![10.0, 11.0, 12.0, 13.0],
        ..Default::default()
    };
    let h2 = h1.clone();

    let mut c = HistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_histogram(None, 0, 0, &h1, false).unwrap();
        app.append_histogram(None, 0, 1, &h2, false).unwrap();
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh1) = it.at_histogram();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh2) = it.at_histogram();

    assert_eq!(rh1.positive_spans, h1.positive_spans);
    assert_eq!(rh1.custom_values, h1.custom_values);
    assert_eq!(rh2.positive_spans, h1.positive_spans);
    assert_eq!(rh2.custom_values, h1.custom_values);
    assert_ne!(rh1.positive_spans.as_ptr(), rh2.positive_spans.as_ptr());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramCustomValuesInternedAfterNextWithAtFloatHistogram
#[test]
fn test_histogram_custom_values_interned_after_next_with_at_float_histogram() {
    let h1 = Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 10,
        sum: 15.0,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![1, 2, 3, 4],
        custom_values: vec![10.0, 11.0, 12.0, 13.0],
        ..Default::default()
    };
    let h2 = h1.clone();

    let mut c = HistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_histogram(None, 0, 0, &h1, false).unwrap();
        app.append_histogram(None, 0, 1, &h2, false).unwrap();
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh1) = it.at_float_histogram();
    assert!(it.next_sample().unwrap().is_some());
    let (_, rh2) = it.at_float_histogram();

    assert_eq!(rh1.positive_spans, h1.positive_spans);
    assert_eq!(rh1.custom_values, h1.custom_values);
    assert_eq!(rh2.positive_spans, h1.positive_spans);
    assert_eq!(rh2.custom_values, h1.custom_values);
    assert_ne!(rh1.positive_spans.as_ptr(), rh2.positive_spans.as_ptr());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestIntHistogramEmptyBucketsWithGaps
#[test]
fn test_int_histogram_empty_buckets_with_gaps() {
    let h1 = Histogram {
        positive_spans: vec![
            HistogramSpan { offset: -19, length: 2 },
            HistogramSpan { offset: 1, length: 2 },
        ],
        positive_buckets: vec![0, 0, 0, 0],
        ..Default::default()
    };
    h1.validate().unwrap();

    let mut c = HistogramChunk::new();
    {
        let mut app = c.appender().unwrap();
        app.append_histogram(None, 0, 1, &h1, false).unwrap();
        let h2 = Histogram {
            positive_spans: vec![
                HistogramSpan { offset: -19, length: 1 },
                HistogramSpan { offset: 4, length: 1 },
                HistogramSpan { offset: 3, length: 1 },
            ],
            positive_buckets: vec![0, 0, 0],
            ..Default::default()
        };
        h2.validate().unwrap();

        match app.append_histogram(None, 0, 2, &h2, false).unwrap() {
            AppendHistogramOutcome::Appended { recoded } => assert!(recoded),
            AppendHistogramOutcome::NewChunk(nc) => c = nc,
        }
    }

    let mut it = c.iter();
    assert!(it.next_sample().unwrap().is_some());
    it.at_float_histogram().1.validate().unwrap();
    assert!(it.next_sample().unwrap().is_some());
    it.at_float_histogram().1.validate().unwrap();
    assert!(it.next_sample().unwrap().is_none());
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramIteratorFailIfSchemaInValid
#[test]
fn test_histogram_iterator_fail_if_schema_invalid() {
    for schema in [-101i32, 101] {
        let h = Histogram {
            schema,
            count: 10,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
            ],
            positive_buckets: vec![1, 2, 3, 4],
            ..Default::default()
        };
        let mut c = HistogramChunk::new();
        c.appender()
            .unwrap()
            .append_histogram(None, 0, 1, &h, false)
            .unwrap();
        let mut it = c.iter();
        let err = it.next_sample().unwrap_err();
        assert!(matches!(err, ChunkError::UnknownSchema(s) if s == schema));
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramIteratorReduceSchema
#[test]
fn test_histogram_iterator_reduce_schema() {
    for schema in [9i32, 52] {
        let h = Histogram {
            schema,
            count: 10,
            sum: 15.0,
            zero_threshold: 1e-100,
            positive_spans: vec![
                HistogramSpan { offset: 0, length: 2 },
                HistogramSpan { offset: 1, length: 2 },
            ],
            positive_buckets: vec![1, 2, 3, 4],
            ..Default::default()
        };
        let mut c = HistogramChunk::new();
        c.appender()
            .unwrap()
            .append_histogram(None, 0, 1, &h, false)
            .unwrap();
        let mut it = c.iter();
        assert!(it.next_sample().unwrap().is_some());
        let (_, rh) = it.at_histogram();
        assert_eq!(rh.schema, EXPONENTIAL_SCHEMA_MAX);
        let (_, rfh) = it.at_float_histogram();
        assert_eq!(rfh.schema, EXPONENTIAL_SCHEMA_MAX);
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/chunkenc/histogram_test.go::TestHistogramChunkOverFlowPanics
#[test]
fn test_histogram_chunk_overflow_panics() {
    let mut chunk = HistogramChunk::new();
    let mut app = chunk.appender().unwrap();
    let mut h = Histogram {
        zero_threshold: 1e-128,
        ..Default::default()
    };
    for i in 0..(u16::MAX as u64) {
        h.count = i;
        h.zero_count = i;
        let _ = app.append_histogram(None, 0, i as i64, &h, true).unwrap();
    }
    h.count = u16::MAX as u64;
    h.zero_count = u16::MAX as u64;
    let err = app
        .append_histogram(None, 0, u16::MAX as i64, &h, true)
        .unwrap_err();
    assert!(matches!(err, ChunkError::CapacityExceeded));
}
