use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, ChunkResponse, ChunkSampleJson, DecodeChunkRequest,
    EncodeChunkRequest, ExemplarJson, HashSeriesRequest, HashSeriesResponse, HistogramJson,
    LabelJson, SpanJson, TsdbSeriesFixtureJson, WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};
use mimir_tsdb::chunkenc::histogram::AppendHistogramOutcome;
use mimir_tsdb::chunkenc::float_histogram::AppendFloatHistogramOutcome;
use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHint, EncodedChunk, Encoding, FloatHistogram, FloatHistogramChunk,
    Histogram, HistogramChunk, HistogramSpan, OooChunk, XorChunk, CUSTOM_BUCKETS_SCHEMA,
    NORMAL_NAN_BITS, STALE_NAN_BITS,
};
use mimir_tsdb::head_chunks::HeadChunksReader;
use mimir_tsdb::labels::{Label, Labels, SymbolLabels};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::record::{DecodedRecord, RecordDecoder};
use mimir_tsdb::symbols::SymbolTable;
use mimir_tsdb::wlog::WlogReader;

fn encode_rust_xor(samples: &[ChunkSampleJson]) -> Vec<u8> {
    let mut chunk = XorChunk::new();
    {
        let mut app = chunk.appender().expect("xor appender");
        for s in samples {
            app.append(s.st, s.t, s.v).expect("xor append");
        }
    }
    chunk.compact();
    chunk.into_bytes()
}

fn decode_rust_xor(bytes: &[u8]) -> Vec<ChunkSampleJson> {
    let chunk = XorChunk::from_bytes(bytes.to_vec());
    let mut it = chunk.iter();
    let mut out = Vec::with_capacity(chunk.num_samples());
    while let Some((t, v)) = it.next_sample().expect("xor iter") {
        out.push(ChunkSampleJson { st: 0, t, v });
    }
    out
}

fn to_rust_hist(h: &HistogramJson) -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::from_u8(h.reset_hint as u8),
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        count: h.count,
        sum: h.sum,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|s| HistogramSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| HistogramSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_buckets: h.positive_deltas.clone(),
        negative_buckets: h.negative_deltas.clone(),
        custom_values: h.custom_values.clone(),
    }
}

fn to_rust_float_hist(h: &HistogramJson) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::from_u8(h.reset_hint as u8),
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count_float,
        count: h.count_float,
        sum: h.sum,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|s| HistogramSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| HistogramSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_buckets: h.positive_counts.clone(),
        negative_buckets: h.negative_counts.clone(),
        custom_values: h.custom_values.clone(),
    }
}

fn from_rust_hist(t: i64, h: &Histogram) -> HistogramJson {
    HistogramJson {
        timestamp_ms: t,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        zero_count_float: 0.0,
        count: h.count,
        count_float: 0.0,
        sum: h.sum,
        reset_hint: h.counter_reset_hint as u32,
        is_float: false,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_deltas: h.positive_buckets.clone(),
        positive_counts: Vec::new(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_deltas: h.negative_buckets.clone(),
        negative_counts: Vec::new(),
        custom_values: h.custom_values.clone(),
    }
}

fn from_rust_float_hist(t: i64, fh: &FloatHistogram) -> HistogramJson {
    HistogramJson {
        timestamp_ms: t,
        schema: fh.schema,
        zero_threshold: fh.zero_threshold,
        zero_count: 0,
        zero_count_float: fh.zero_count,
        count: 0,
        count_float: fh.count,
        sum: fh.sum,
        reset_hint: fh.counter_reset_hint as u32,
        is_float: true,
        positive_spans: fh
            .positive_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_deltas: Vec::new(),
        positive_counts: fh.positive_buckets.clone(),
        negative_spans: fh
            .negative_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_deltas: Vec::new(),
        negative_counts: fh.negative_buckets.clone(),
        custom_values: fh.custom_values.clone(),
    }
}

fn assert_xor_parity(name: &str, samples: &[ChunkSampleJson]) {
    let rust_bytes = encode_rust_xor(samples);

    // Rust -> Go decode
    let go_decoded: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: Encoding::Xor.as_u8(),
            bytes_b64: B64.encode(&rust_bytes),
        },
    )
    .unwrap_or_else(|e| panic!("{name}: go decode failed: {e}"));
    assert_eq!(go_decoded.num_samples, samples.len(), "{name}: sample count");
    assert_eq!(go_decoded.samples, samples, "{name}: go decoded samples");

    // Go -> Rust decode AND bit-for-bit byte parity
    let go_encoded: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: Encoding::Xor.as_u8(),
            samples: samples.to_vec(),
            histograms: Vec::new(),
        },
    )
    .unwrap_or_else(|e| panic!("{name}: go encode failed: {e}"));
    let go_bytes = B64.decode(&go_encoded.bytes_b64).expect("decode b64");
    assert_eq!(
        rust_bytes, go_bytes,
        "{name}: Rust XOR bytes must match Go XOR bytes bit-for-bit"
    );

    let rust_decoded = decode_rust_xor(&go_bytes);
    assert_eq!(rust_decoded, samples, "{name}: rust decoded samples");
}

#[test]
fn xor_chunk_conformance_bit_for_bit() {
    ensure_go_oracle_built().expect("go-oracle");

    // 1 sample (negative timestamp & 0.0)
    assert_xor_parity(
        "single_negative_ts_zero",
        &[ChunkSampleJson {
            st: 0,
            t: -5000,
            v: 0.0,
        }],
    );

    // 2 samples with -0.0 and negative value
    assert_xor_parity(
        "two_samples_neg_zero",
        &[
            ChunkSampleJson {
                st: 0,
                t: 1_000,
                v: -0.0,
            },
            ChunkSampleJson {
                st: 0,
                t: 2_500,
                v: -42.125,
            },
        ],
    );

    // Monotonic 120 samples
    let mono_120: Vec<_> = (0..120)
        .map(|i| ChunkSampleJson {
            st: 0,
            t: 1_700_000_000_000 + i * 15_000,
            v: 100.0 + (i as f64) * 0.5,
        })
        .collect();
    assert_xor_parity("monotonic_120", &mono_120);

    // Irregular timestamps covering 14-bit, 17-bit, 20-bit, and 64-bit DoD buckets + boundary values
    let mut t = 1_700_000_000_000i64;
    let mut mixed_220 = Vec::with_capacity(220);
    for i in 0..220usize {
        t += match i % 6 {
            0 => 15_000,
            1 => 15_100,
            2 => 30_000,
            3 => 250_000,
            4 => 10_000_000,
            _ => 15_000,
        };
        let v = match i % 4 {
            0 => 42.0,
            1 => 42.0,
            2 => (i as f64) * 0.375,
            _ => -123.5 + (i as f64),
        };
        mixed_220.push(ChunkSampleJson { st: 0, t, v });
    }
    assert_xor_parity("mixed_220", &mixed_220);

    // Explicitly verify XorChunk::append_with_state and Xor2Chunk::append_with_state match Go oracle bit-for-bit!
    let mut xor_fast = XorChunk::new();
    let mut xor_state = mimir_tsdb::chunkenc::XorAppendState::new_empty();
    let mut pt = i64::MIN;
    let mut pv = 0.0;
    for s in &mixed_220 {
        xor_fast
            .append_with_state(&mut xor_state, pt, pv, s.t, s.v)
            .unwrap();
        pt = s.t;
        pv = s.v;
    }
    xor_fast.compact();
    let go_xor: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: Encoding::Xor.as_u8(),
            samples: mixed_220.clone(),
            histograms: Vec::new(),
        },
    )
    .expect("go encode xor");
    assert_eq!(
        xor_fast.into_bytes(),
        B64.decode(&go_xor.bytes_b64).unwrap(),
        "XorAppendState must match Go encode-chunk bit-for-bit"
    );

    let xor2_samples: Vec<ChunkSampleJson> = mixed_220
        .iter()
        .enumerate()
        .map(|(i, s)| ChunkSampleJson {
            st: if i < 20 { 1_699_999_000_000 } else { 1_700_000_000_000 + (i as i64 / 30) * 500 },
            t: s.t,
            v: s.v,
        })
        .collect();
    let mut xor2_fast = mimir_tsdb::chunkenc::Xor2Chunk::new();
    let mut xor2_state = mimir_tsdb::chunkenc::Xor2AppendState::new_empty();
    for s in &xor2_samples {
        xor2_fast
            .append_with_state(&mut xor2_state, s.st, s.t, s.v)
            .unwrap();
    }
    xor2_fast.compact();
    let go_xor2: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: Encoding::Xor2.as_u8(),
            samples: xor2_samples,
            histograms: Vec::new(),
        },
    )
    .expect("go encode xor2");
    assert_eq!(
        xor2_fast.into_bytes(),
        B64.decode(&go_xor2.bytes_b64).unwrap(),
        "Xor2AppendState must match Go encode-chunk bit-for-bit"
    );
    let irregular: Vec<ChunkSampleJson> = vec![
        ChunkSampleJson { st: 0, t: 10_000, v: 1.0 },
        ChunkSampleJson { st: 0, t: 20_000, v: 1.0 },          // tDelta = 10000
        ChunkSampleJson { st: 0, t: 30_000, v: 2.0 },          // dod = 0
        ChunkSampleJson { st: 0, t: 48_192, v: -3.5 },         // dod = +8192 (max 14-bit)
        ChunkSampleJson { st: 0, t: 58_193, v: 1e-12 },        // dod = -8191 (min 14-bit)
        ChunkSampleJson { st: 0, t: 120_000, v: 1e100 },       // 17-bit dod
        ChunkSampleJson { st: 0, t: 450_000, v: -1e-100 },     // 20-bit dod
        ChunkSampleJson { st: 0, t: 5_000_000, v: 0.0 },       // 64-bit dod
        ChunkSampleJson { st: 0, t: 5_000_000, v: 0.0 },       // duplicate ts, dod != 0
    ];
    assert_xor_parity("irregular_dod_buckets", &irregular);

    // 1000 samples (varying signs, constant runs, oscillations)
    let samples_1000: Vec<_> = (0..1000)
        .map(|i| {
            let jitter = if i % 17 == 0 {
                313
            } else if i % 31 == 0 {
                -77
            } else {
                0
            };
            let v = if i % 10 < 3 {
                42.0
            } else if i % 7 == 0 {
                -((i as f64) * 1.375)
            } else {
                (i as f64) * 0.25 + ((i % 5) as f64) * 1e-6
            };
            ChunkSampleJson {
                st: 0,
                t: 1_700_000_000_000 + i * 1000 + jitter,
                v,
            }
        })
        .collect();
    assert_xor_parity("samples_1000", &samples_1000);
}

#[test]
fn xor_special_ieee754_values_roundtrip() {
    let values = [
        0.0f64,
        -0.0f64,
        f64::INFINITY,
        f64::NEG_INFINITY,
        stale_nan(),
        f64::from_bits(NORMAL_NAN_BITS),
        f64::MIN_POSITIVE,
        f64::MAX,
    ];
    let mut chunk = XorChunk::new();
    {
        let mut app = chunk.appender().unwrap();
        for (i, &v) in values.iter().enumerate() {
            app.append(0, 1000 + (i as i64) * 1000, v).unwrap();
        }
    }
    let mut it = chunk.iter();
    let mut idx = 0usize;
    while let Some((t, v)) = it.next_sample().unwrap() {
        assert_eq!(t, 1000 + (idx as i64) * 1000);
        assert_eq!(
            v.to_bits(),
            values[idx].to_bits(),
            "bit pattern mismatch at {idx}"
        );
        idx += 1;
    }
    assert_eq!(idx, values.len());
    assert_eq!(values[4].to_bits(), STALE_NAN_BITS);
}

fn assert_histogram_parity(name: &str, hists: &[HistogramJson], check_go_encode: bool) {
    let is_float = hists[0].is_float;
    let encoding = if is_float {
        Encoding::FloatHistogram
    } else {
        Encoding::Histogram
    };

    let rust_bytes = if is_float {
        let mut chunk = FloatHistogramChunk::new();
        {
            let mut app = chunk.appender().unwrap();
            for h in hists {
                let fh = to_rust_float_hist(h);
                match app
                    .append_float_histogram(None, 0, h.timestamp_ms, &fh, false)
                    .unwrap()
                {
                    AppendFloatHistogramOutcome::Appended { .. } => {}
                    AppendFloatHistogramOutcome::NewChunk(_) => {
                        panic!("{name}: unexpected new chunk")
                    }
                }
            }
        }
        chunk.compact();
        chunk.into_bytes()
    } else {
        let mut chunk = HistogramChunk::new();
        {
            let mut app = chunk.appender().unwrap();
            for h in hists {
                let ih = to_rust_hist(h);
                match app
                    .append_histogram(None, 0, h.timestamp_ms, &ih, false)
                    .unwrap()
                {
                    AppendHistogramOutcome::Appended { .. } => {}
                    AppendHistogramOutcome::NewChunk(_) => {
                        panic!("{name}: unexpected new chunk")
                    }
                }
            }
        }
        chunk.compact();
        chunk.into_bytes()
    };

    // Rust -> Go decode
    let go_decoded: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: encoding.as_u8(),
            bytes_b64: B64.encode(&rust_bytes),
        },
    )
    .unwrap_or_else(|e| panic!("{name}: go decode failed: {e}"));
    assert_eq!(go_decoded.num_samples, hists.len(), "{name}: num_samples");

    let decoded_rust = if is_float {
        let chunk = FloatHistogramChunk::from_bytes(rust_bytes.clone());
        let mut it = chunk.iter();
        let mut got = Vec::new();
        while let Some((t, fh)) = it.next_float_histogram().unwrap() {
            got.push(from_rust_float_hist(t, &fh));
        }
        got
    } else {
        let chunk = HistogramChunk::from_bytes(rust_bytes.clone());
        let mut it = chunk.iter();
        let mut got = Vec::new();
        while let Some((t, h)) = it.next_histogram().unwrap() {
            got.push(from_rust_hist(t, &h));
        }
        got
    };
    assert_eq!(decoded_rust.len(), go_decoded.histograms.len(), "{name}: decoded len");
    for (i, (r, g)) in decoded_rust.iter().zip(go_decoded.histograms.iter()).enumerate() {
        if is_float {
            assert!(
                to_rust_float_hist(r).prom_equals(&to_rust_float_hist(g)),
                "{name}: float hist #{i} mismatch:\nrust={r:?}\ngo={g:?}"
            );
        } else {
            assert!(
                to_rust_hist(r).prom_equals(&to_rust_hist(g)),
                "{name}: int hist #{i} mismatch:\nrust={r:?}\ngo={g:?}"
            );
        }
    }

    if !check_go_encode {
        return;
    }

    // Go -> Rust encode & bit-for-bit byte parity + decode parity
    let go_encoded: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: encoding.as_u8(),
            samples: Vec::new(),
            histograms: hists.to_vec(),
        },
    )
    .unwrap_or_else(|e| panic!("{name}: go encode failed: {e}"));
    let go_bytes = B64.decode(&go_encoded.bytes_b64).expect("decode b64");

    assert_eq!(
        rust_bytes, go_bytes,
        "{name}: Rust histogram chunk bytes must match Go bytes bit-for-bit"
    );
}

#[test]
fn histogram_chunks_conformance_bit_for_bit() {
    ensure_go_oracle_built().expect("go-oracle");

    // 1. Integer exponential histograms (matching spans -> bit-for-bit Go <-> Rust parity)
    let int_hists_no_recode = vec![
        HistogramJson {
            timestamp_ms: 1_700_000_000_000,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 2,
            count: 7,
            sum: 12.5,
            reset_hint: 2,
            is_float: false,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![3, -1],
            negative_spans: vec![SpanJson {
                offset: 1,
                length: 1,
            }],
            negative_deltas: vec![1],
            ..Default::default()
        },
        HistogramJson {
            timestamp_ms: 1_700_000_015_000,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 4,
            count: 14,
            sum: 28.75,
            reset_hint: 2,
            is_float: false,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![5, -2],
            negative_spans: vec![SpanJson {
                offset: 1,
                length: 1,
            }],
            negative_deltas: vec![2],
            ..Default::default()
        },
    ];
    assert_histogram_parity("int_histogram_exact", &int_hists_no_recode, true);

    // 2. Integer exponential histograms WITH span expansion / recoding (Rust encodes -> Go decodes)
    let int_hists_recode = vec![
        int_hists_no_recode[0].clone(),
        HistogramJson {
            timestamp_ms: 1_700_000_015_000,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 4,
            count: 14,
            sum: 28.75,
            reset_hint: 2,
            is_float: false,
            positive_spans: vec![
                SpanJson {
                    offset: 0,
                    length: 2,
                },
                SpanJson {
                    offset: 1,
                    length: 1,
                },
            ],
            positive_deltas: vec![5, -2, -1],
            negative_spans: vec![SpanJson {
                offset: 1,
                length: 1,
            }],
            negative_deltas: vec![2],
            ..Default::default()
        },
    ];
    assert_histogram_parity("int_histogram_with_recode", &int_hists_recode, false);

    // 3. Integer custom-bucket (NHCB) gauge histogram (bit-for-bit Go <-> Rust parity)
    let nhcb_hists = vec![
        HistogramJson {
            timestamp_ms: 1_000,
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            count: 5,
            sum: 10.0,
            reset_hint: 3, // GaugeType
            is_float: false,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![2, -1, 1],
            custom_values: vec![0.1, 0.5, 1.0, 5.0],
            ..Default::default()
        },
        HistogramJson {
            timestamp_ms: 2_000,
            schema: CUSTOM_BUCKETS_SCHEMA,
            zero_threshold: 0.0,
            count: 4,
            sum: 8.5,
            reset_hint: 3,
            is_float: false,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![1, 1, -1],
            custom_values: vec![0.1, 0.5, 1.0, 5.0],
            ..Default::default()
        },
    ];
    assert_histogram_parity("int_nhcb_gauge_histogram", &nhcb_hists, true);

    // 4. Float exponential histograms (matching spans -> bit-for-bit Go <-> Rust parity)
    let float_hists_exact = vec![
        HistogramJson {
            timestamp_ms: 1_700_000_000_000,
            schema: 1,
            zero_threshold: 2.0f64.powi(-128),
            zero_count_float: 1.5,
            count_float: 6.5,
            sum: 15.25,
            reset_hint: 2,
            is_float: true,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![2.5, 2.5],
            ..Default::default()
        },
        HistogramJson {
            timestamp_ms: 1_700_000_015_000,
            schema: 1,
            zero_threshold: 2.0f64.powi(-128),
            zero_count_float: 2.5,
            count_float: 10.0,
            sum: 30.5,
            reset_hint: 2,
            is_float: true,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![3.5, 4.0],
            ..Default::default()
        },
    ];
    assert_histogram_parity("float_histogram_exact", &float_hists_exact, true);

    // 5. Float exponential histograms WITH span expansion / recoding (Rust encodes -> Go decodes)
    let float_hists_recode = vec![
        float_hists_exact[0].clone(),
        HistogramJson {
            timestamp_ms: 1_700_000_015_000,
            schema: 1,
            zero_threshold: 2.0f64.powi(-128),
            zero_count_float: 2.5,
            count_float: 12.0,
            sum: 30.5,
            reset_hint: 2,
            is_float: true,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 3,
            }],
            positive_counts: vec![3.5, 4.0, 2.0],
            ..Default::default()
        },
    ];
    assert_histogram_parity("float_histogram_with_recode", &float_hists_recode, false);
}

#[test]
fn labels_hash_and_matcher_conformance() {
    ensure_go_oracle_built().expect("go-oracle");

    let long_val = "x".repeat(300);
    let pairs = [
        ("zone", "us-east-1a"),
        ("__name__", "http_request_duration_seconds_bucket"),
        ("job", "ingester"),
        ("le", "0.5"),
        ("long_val", long_val.as_str()),
    ];

    let go_hash: HashSeriesResponse = invoke_oracle(
        "hash-series",
        &HashSeriesRequest {
            user_id: "t1".to_string(),
            metric_name: "http_request_duration_seconds_bucket".to_string(),
            labels: pairs
                .iter()
                .map(|(k, v)| LabelJson {
                    name: (*k).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        },
    )
    .expect("hash-series");

    let labels = Labels::from_pairs(pairs.iter().copied());
    assert_eq!(labels.hash(), go_hash.prom_labels_hash);

    let mut st = SymbolTable::new();
    let sym_labels = SymbolLabels::from_labels(&mut st, &labels);
    assert_eq!(sym_labels.prom_hash(&st), go_hash.prom_labels_hash);

    // Verify matcher semantics
    let m_absent_eq = LabelMatcher::new(MatchType::Equal, "missing", "").unwrap();
    let m_present_re = LabelMatcher::new(MatchType::RegexMatch, "job", "ing.*").unwrap();
    assert!(sym_labels.matches(&st, &[m_absent_eq, m_present_re]));
}

#[test]
fn real_go_tsdb_wal_and_chunks_head_cross_read() {
    ensure_go_oracle_built().expect("go-oracle");

    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_string_lossy().to_string();

    // Generate >120 samples for `up` so Prometheus TSDB cuts an mmap chunk in chunks_head/000001
    // AND writes series + samples + histograms + exemplars into wal/00000000!
    let up_samples: Vec<ChunkSampleJson> = (0..150)
        .map(|i| ChunkSampleJson {
            st: 0,
            t: 1_700_000_000_000 + i * 15_000,
            v: (i as f64) * 1.5,
        })
        .collect();

    let hist_samples = vec![
        HistogramJson {
            timestamp_ms: 1_700_000_000_000,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 2,
            count: 5,
            sum: 12.5,
            reset_hint: 1,
            is_float: false,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![2, -1],
            ..Default::default()
        },
        HistogramJson {
            timestamp_ms: 1_700_000_015_000,
            schema: 0,
            zero_threshold: 0.001,
            zero_count: 4,
            count: 9,
            sum: 25.0,
            reset_hint: 2,
            is_float: false,
            positive_spans: vec![SpanJson {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![3, -1],
            ..Default::default()
        },
    ];

    let float_hist_samples = vec![HistogramJson {
        timestamp_ms: 1_700_000_000_000,
        schema: 1,
        zero_threshold: 0.001,
        zero_count_float: 1.5,
        count_float: 4.5,
        sum: 9.25,
        reset_hint: 1,
        is_float: true,
        positive_spans: vec![SpanJson {
            offset: 0,
            length: 2,
        }],
        positive_counts: vec![1.0, 2.0],
        ..Default::default()
    }];

    let up_exemplar = ExemplarJson {
        labels: vec![LabelJson {
            name: "trace_id".to_string(),
            value: "deadbeef01".to_string(),
        }],
        value: 42.0,
        timestamp_ms: up_samples.last().unwrap().t,
    };

    let resp: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir.clone(),
            compact: false,
            out_of_order_time_window_ms: 0,
            series: vec![
                TsdbSeriesFixtureJson {
                    labels: vec![
                        LabelJson {
                            name: "__name__".to_string(),
                            value: "up".to_string(),
                        },
                        LabelJson {
                            name: "job".to_string(),
                            value: "ingester".to_string(),
                        },
                    ],
                    samples: up_samples.clone(),
                    histograms: Vec::new(),
                    exemplars: vec![up_exemplar.clone()],
                },
                TsdbSeriesFixtureJson {
                    labels: vec![
                        LabelJson {
                            name: "__name__".to_string(),
                            value: "hist_int".to_string(),
                        },
                        LabelJson {
                            name: "env".to_string(),
                            value: "prod".to_string(),
                        },
                    ],
                    samples: Vec::new(),
                    histograms: hist_samples.clone(),
                    exemplars: Vec::new(),
                },
                TsdbSeriesFixtureJson {
                    labels: vec![
                        LabelJson {
                            name: "__name__".to_string(),
                            value: "hist_float".to_string(),
                        },
                        LabelJson {
                            name: "env".to_string(),
                            value: "prod".to_string(),
                        },
                    ],
                    samples: Vec::new(),
                    histograms: float_hist_samples.clone(),
                    exemplars: Vec::new(),
                },
            ],
        },
    )
    .expect("write-tsdb-fixture");
    assert_eq!(resp.status, "ok");

    // 1. Parse Go WAL directory directly with Rust WlogReader + RecordDecoder
    let wal_dir = tmp.path().join("wal");
    let mut wal_reader = WlogReader::open_dir(&wal_dir).expect("open Go WAL dir");
    let dec = RecordDecoder::new();

    let mut series_by_ref: BTreeMap<u64, Labels> = BTreeMap::new();
    let mut samples_by_ref: BTreeMap<u64, Vec<(i64, f64)>> = BTreeMap::new();
    let mut int_hists_by_ref: BTreeMap<u64, Vec<(i64, Histogram)>> = BTreeMap::new();
    let mut float_hists_by_ref: BTreeMap<u64, Vec<(i64, FloatHistogram)>> = BTreeMap::new();
    let mut exemplars_by_ref: BTreeMap<u64, Vec<ExemplarJson>> = BTreeMap::new();

    while let Some(rec) = wal_reader.next_record().expect("read Go WAL record") {
        match dec.decode_record(rec).expect("decode Go WAL record") {
            DecodedRecord::Series(rs) => {
                for s in rs {
                    series_by_ref.insert(s.ref_id, s.labels);
                }
            }
            DecodedRecord::Samples(ss) => {
                for s in ss {
                    samples_by_ref.entry(s.ref_id).or_default().push((s.t, s.v));
                }
            }
            DecodedRecord::HistogramSamples(hs) => {
                for h in hs {
                    int_hists_by_ref
                        .entry(h.ref_id)
                        .or_default()
                        .push((h.t, h.h));
                }
            }
            DecodedRecord::FloatHistogramSamples(fhs) => {
                for fh in fhs {
                    float_hists_by_ref
                        .entry(fh.ref_id)
                        .or_default()
                        .push((fh.t, fh.fh));
                }
            }
            DecodedRecord::Exemplars(exs) => {
                for ex in exs {
                    exemplars_by_ref
                        .entry(ex.ref_id)
                        .or_default()
                        .push(ExemplarJson {
                            labels: ex
                                .labels
                                .iter()
                                .map(|(k, v)| LabelJson {
                                    name: k.to_string(),
                                    value: v.to_string(),
                                })
                                .collect(),
                            value: ex.v,
                            timestamp_ms: ex.t,
                        });
                }
            }
            _ => {}
        }
    }

    assert_eq!(series_by_ref.len(), 3, "expected 3 series in Go WAL");
    let up_ref = *series_by_ref
        .iter()
        .find(|(_, ls)| ls.get("__name__") == "up")
        .unwrap()
        .0;
    let hi_ref = *series_by_ref
        .iter()
        .find(|(_, ls)| ls.get("__name__") == "hist_int")
        .unwrap()
        .0;
    let hf_ref = *series_by_ref
        .iter()
        .find(|(_, ls)| ls.get("__name__") == "hist_float")
        .unwrap()
        .0;

    let got_up: Vec<_> = samples_by_ref[&up_ref]
        .iter()
        .map(|&(t, v)| ChunkSampleJson { st: 0, t, v })
        .collect();
    assert_eq!(got_up, up_samples);
    assert_eq!(exemplars_by_ref[&up_ref], vec![up_exemplar]);

    assert_eq!(int_hists_by_ref[&hi_ref].len(), 2);
    assert!(int_hists_by_ref[&hi_ref][0]
        .1
        .prom_equals(&to_rust_hist(&hist_samples[0])));
    assert!(int_hists_by_ref[&hi_ref][1]
        .1
        .prom_equals(&to_rust_hist(&hist_samples[1])));

    assert_eq!(float_hists_by_ref[&hf_ref].len(), 1);
    assert!(float_hists_by_ref[&hf_ref][0]
        .1
        .prom_equals(&to_rust_float_hist(&float_hist_samples[0])));

    // 2. Parse Go chunks_head mmap chunks directly with Rust HeadChunksReader
    let chunks_head_dir = tmp.path().join("chunks_head");
    let hc_reader = HeadChunksReader::open(&chunks_head_dir).expect("open Go chunks_head");
    let metas = hc_reader
        .collect_all_chunks()
        .expect("iterate Go chunks_head");
    assert!(
        !metas.is_empty(),
        "expected >= 1 mmapped head chunk in Go chunks_head for 150 samples"
    );
    let mut mmap_samples = Vec::new();
    for meta in &metas {
        assert_eq!(meta.series_ref, up_ref);
        assert!(!meta.is_ooo);
        assert_eq!(meta.encoding, Encoding::Xor);

        let decoded_chunk = hc_reader
            .chunk(meta.chunk_ref)
            .expect("read Go mmap chunk");
        let EncodedChunk::Xor(xc) = decoded_chunk else {
            panic!("expected XOR chunk in chunks_head");
        };
        assert_eq!(xc.num_samples(), meta.num_samples as usize);
        let mut it = xc.iter();
        while let Some((t, v)) = it.next_sample().unwrap() {
            mmap_samples.push(ChunkSampleJson { st: 0, t, v });
        }
    }
    assert_eq!(mmap_samples, up_samples[..mmap_samples.len()]);
}

#[test]
fn real_go_ooo_wbl_and_ooo_chunk_conformance() {
    ensure_go_oracle_built().expect("go-oracle");

    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_string_lossy().to_string();

    // First write an in-order sample at t=2_000_000_000_000, then OOO samples earlier than that.
    let samples = vec![
        ChunkSampleJson {
            st: 0,
            t: 2_000_000_000_000,
            v: 10.0,
        },
        ChunkSampleJson {
            st: 0,
            t: 1_999_999_900_000,
            v: 5.0,
        },
        ChunkSampleJson {
            st: 0,
            t: 1_999_999_950_000,
            v: 7.5,
        },
    ];

    let resp: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir,
            compact: false,
            out_of_order_time_window_ms: 1_000_000,
            series: vec![TsdbSeriesFixtureJson {
                labels: vec![LabelJson {
                    name: "__name__".to_string(),
                    value: "ooo_metric".to_string(),
                }],
                samples,
                histograms: Vec::new(),
                exemplars: Vec::new(),
            }],
        },
    )
    .expect("write-tsdb-fixture ooo");
    assert_eq!(resp.status, "ok");

    // Verify Go wrote wbl/ containing the OOO samples
    let wbl_dir = tmp.path().join("wbl");
    let mut wbl_reader = WlogReader::open_dir(&wbl_dir).expect("open Go wbl");
    let dec = RecordDecoder::new();
    let mut ooo_samples = Vec::new();
    while let Some(rec) = wbl_reader.next_record().expect("read Go wbl record") {
        if let DecodedRecord::Samples(ss) = dec.decode_record(rec).expect("decode wbl record") {
            ooo_samples.extend(ss);
        }
    }
    assert_eq!(ooo_samples.len(), 2);

    // Feed those OOO samples into Rust OooChunk and confirm sorted cut XOR chunk
    let mut ooo = OooChunk::new();
    for s in ooo_samples {
        assert!(ooo.insert_float(s.st, s.t, s.v));
    }
    let cut = ooo.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
    assert_eq!(cut.len(), 1);
    let EncodedChunk::Xor(xc) = &cut[0].chunk else {
        panic!("expected XOR chunk from OooChunk");
    };
    let mut it = xc.iter();
    let mut got = Vec::new();
    while let Some(pair) = it.next_sample().unwrap() {
        got.push(pair);
    }
    assert_eq!(
        got,
        vec![(1_999_999_900_000, 5.0), (1_999_999_950_000, 7.5)]
    );

    // Also validate Label struct import works cleanly
    let _lbl = Label::new("__name__", "ooo_metric");
}

#[test]
fn rust_wal_read_by_real_go_tsdb() {
    use mimir_conformance::oracle::{VerifyTsdbDirRequest, VerifyTsdbDirResponse};
    use mimir_tsdb::record::{
        RecordEncoder, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefSample,
        RefSeries,
    };
    use mimir_tsdb::wlog::{CompressionType, WlogWriter};

    ensure_go_oracle_built().expect("go-oracle");

    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");

    let enc = RecordEncoder::new(false);
    let mut w = WlogWriter::open(&wal_dir, CompressionType::Snappy).expect("open rust wal");

    let s1 = Labels::from_pairs([("__name__", "hist_metric"), ("env", "prod")]);
    let s2 = Labels::from_pairs([("__name__", "up"), ("job", "ingester")]);
    let s3 = Labels::from_pairs([("__name__", "fhist_metric"), ("env", "staging")]);

    let rec_series = enc.series(
        &[
            RefSeries {
                ref_id: 1,
                labels: s1,
            },
            RefSeries {
                ref_id: 2,
                labels: s2,
            },
            RefSeries {
                ref_id: 3,
                labels: s3,
            },
        ],
        Vec::new(),
    );
    let rec_samples = enc.samples(
        &[
            RefSample {
                ref_id: 2,
                st: 0,
                t: 1_700_000_000_000,
                v: 1.0,
            },
            RefSample {
                ref_id: 2,
                st: 0,
                t: 1_700_000_015_000,
                v: 2.5,
            },
        ],
        Vec::new(),
    );
    let (rec_hists, _) = enc.histogram_samples(
        &[RefHistogramSample {
            ref_id: 1,
            st: 0,
            t: 1_700_000_000_000,
            h: Histogram {
                counter_reset_hint: CounterResetHint::CounterReset,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: 2,
                count: 5,
                sum: 12.5,
                positive_spans: vec![HistogramSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_buckets: vec![2, -1],
                ..Default::default()
            },
        }],
        Vec::new(),
    );
    let (rec_fhists, _) = enc.float_histogram_samples(
        &[RefFloatHistogramSample {
            ref_id: 3,
            st: 0,
            t: 1_700_000_000_000,
            fh: FloatHistogram {
                counter_reset_hint: CounterResetHint::CounterReset,
                schema: 1,
                zero_threshold: 0.001,
                zero_count: 1.5,
                count: 4.5,
                sum: 9.25,
                positive_spans: vec![HistogramSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_buckets: vec![1.0, 2.0],
                ..Default::default()
            },
        }],
        Vec::new(),
    );
    let rec_exemplars = enc.exemplars(
        &[RefExemplar {
            ref_id: 2,
            t: 1_700_000_015_000,
            v: 2.5,
            labels: Labels::from_pairs([("trace_id", "rust2go")]),
        }],
        Vec::new(),
    );

    w.log(&[
        &rec_series,
        &rec_samples,
        &rec_hists,
        &rec_fhists,
        &rec_exemplars,
    ])
    .expect("log rust wal");
    w.close().expect("close rust wal");

    let verify: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: tmp.path().to_string_lossy().to_string(),
            read_only: false,
        },
    )
    .expect("go tsdb verify-tsdb-dir on rust WAL");

    assert_eq!(verify.head_series, 3);
    assert_eq!(verify.series.len(), 3);
    assert_eq!(verify.series[0].histograms.len(), 1);
    assert_eq!(verify.series[1].histograms.len(), 1);
    assert_eq!(verify.series[2].samples.len(), 2);
    assert_eq!(verify.series[2].exemplars.len(), 1);
    assert_eq!(
        verify.series[2].exemplars[0].labels[0].value,
        "rust2go"
    );
}

#[derive(serde::Serialize)]
struct RegexMatcherErrorsRequest<'a> {
    patterns: &'a [String],
}

#[derive(serde::Deserialize)]
struct RegexMatcherErrorsResponse {
    errors: Vec<String>,
}

/// Every regex Prometheus `labels.NewMatcher(MatchRegexp, ...)` rejects (Go `regexp/syntax`)
/// is rejected by `LabelMatcher::new` with the byte-identical error text (it reaches clients as
/// the gRPC status message of read RPCs). Corpus: all strings of up to 3 characters over a
/// metacharacter-heavy alphabet plus deterministic pseudo-random longer ones and hand-picked
/// cases.
#[test]
fn regex_matcher_errors_match_go_regexp_syntax() {
    const ALPHABET: &[&str] = &[
        "(", ")", "[", "]", "{", "}", "*", "+", "?", "|", "\\", "a", "1", ",", "-", "^", ":",
        "P", "<", ">", "x", "p", "Q", "E", "0", "=", "}", "é",
    ];
    let mut patterns: Vec<String> = vec![String::new()];
    let mut frontier = vec![String::new()];
    for _ in 0..3 {
        let mut next = Vec::new();
        for p in &frontier {
            for a in ALPHABET {
                next.push(format!("{p}{a}"));
            }
        }
        patterns.extend(next.iter().cloned());
        frontier = next;
    }
    let tokens: &[&str] = &[
        "(", ")", "(?:", "(?i)", "(?P<n>", "(?<n>", "[", "]", "[^", "[:alpha:]", "[:foo:]", "{2}",
        "{1,}", "{2,1}", "{1001}", "{500}", "{3}", "*", "+?", "?", "|", "\\d", "\\pL", "\\p{Greek}",
        "\\p{Foo}", "\\x{41}", "\\x4", "\\Q", "\\E", "\\b", "\\z", "\\C", "a", "z-a", "a-z", "-",
        ".", "^", "$", "é",
    ];
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..20_000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let n = 2 + (seed >> 60) as usize % 6;
        let mut s = String::new();
        let mut x = seed;
        for _ in 0..n {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            s.push_str(tokens[(x >> 33) as usize % tokens.len()]);
        }
        patterns.push(s);
    }
    for p in [
        "(a{2}){500}", "(a{500}){3}", "((a{2}){2}){251}", "a{100000000}", "(?P=n)", "(?i-)",
        "(?--i)", "\\x{110000}", "[\\b]", "a{,2}", "x{1001", "[]a]", "[-a]", "[a-]", "\\p{^Greek}",
        "\\p{lower_case_letter}", "(?U)a+", "a\\Qb*\\E+", "\\Qab", "[[:^word:]\\d-z]",
        "\\p{Letter}", "\\p{Old_Italic}", "\\p{greek}", "\\pl", "\\p{LC}", "\\p{Lc}", "(?<n>a)",
    ] {
        patterns.push(p.to_string());
    }

    let resp: RegexMatcherErrorsResponse =
        invoke_oracle("regex-matcher-errors", &RegexMatcherErrorsRequest { patterns: &patterns })
            .expect("go-oracle regex-matcher-errors");
    assert_eq!(resp.errors.len(), patterns.len());

    let mut mismatches = Vec::new();
    let mut rust_only_rejects = Vec::new();
    let mut go_errors = 0usize;
    for (p, go) in patterns.iter().zip(&resp.errors) {
        let rust = match LabelMatcher::new(MatchType::RegexMatch, "x", p.as_str()) {
            Ok(_) => String::new(),
            Err(e) => e.to_string(),
        };
        if go.is_empty() {
            if !rust.is_empty() {
                rust_only_rejects.push(format!("{p:?}: {rust}"));
            }
        } else {
            go_errors += 1;
            if &rust != go {
                mismatches.push(format!("{p:?}:\n    go:   {go}\n    rust: {rust}"));
            }
        }
    }
    assert!(go_errors > 1000, "corpus must exercise Go errors ({go_errors})");
    assert!(
        mismatches.is_empty(),
        "{} of {} Go-rejected patterns differ:\n{}",
        mismatches.len(),
        go_errors,
        mismatches.iter().take(60).cloned().collect::<Vec<_>>().join("\n")
    );
    eprintln!(
        "{} Go-valid patterns rejected by the Rust regex engine:\n{}",
        rust_only_rejects.len(),
        rust_only_rejects.iter().take(40).cloned().collect::<Vec<_>>().join("\n")
    );
}

/// W20-SDW-04: `regexp/syntax` `checkLimits` (`ErrLarge` / `ErrNestingDepth`) and its gate
/// (`checkSize` only computes sizes once `numRegexp >= maxSize / repeats`). Every pattern goes
/// through the vendored `github.com/grafana/regexp` parser via `labels.NewMatcher`: the ones Go
/// accepts must pass `go_regexp_syntax::check` — a multi-megabyte literal or alternation is a
/// valid matcher — and the ones Go rejects must fail with the byte-identical error.
#[test]
fn regex_check_limits_gate_matches_go_regexp_syntax() {
    let a = |n: usize| "a".repeat(n);
    let big_alt = (0..100_000)
        .map(|i| format!("value_number_{i:08}_with_some_long_suffix_x"))
        .collect::<Vec<_>>()
        .join("|");
    let patterns: Vec<String> = vec![
        // Nesting depth.
        format!("{}a{}", "(".repeat(1000), ")".repeat(1000)),
        format!("{}a{}", "(".repeat(998), ")".repeat(998)),
        format!("{}{}", "(a.".repeat(499), ")".repeat(499)),
        format!("{}{}", "(a.".repeat(500), ")".repeat(500)),
        format!("{}{}", "(?:a.".repeat(1200), ")".repeat(1200)),
        format!("{}b{}", "(?:a|".repeat(1200), ")".repeat(1200)),
        format!("{}c{}", "(?:a|b.".repeat(600), ")".repeat(600)),
        format!("{}a{}", "(?:".repeat(1001), ")*".repeat(1001)),
        format!("{}a{}", "(?:".repeat(500), ")*".repeat(500)),
        // A lone repeat is pushed again as the only item of the concatenation and alternation.
        format!("({}){{1000}}", a(3353)),
        format!("({}){{1000}}", a(3354)),
        format!("({}){{999}}", a(3400)),
        format!("({}){{2}}", a(4000)),
        format!("(?:{}){{1000}}", a(3354)),
        format!("(?:{}){{1000}}", a(3400)),
        format!("(?:{}){{1000,}}", a(3400)),
        format!("(?:{}){{0,1000}}", a(4000)),
        "(a{1000}|b{1000}|c{1000}|d{1000}){1000}".to_string(),
        // Next to another item it is not: the gate stays closed.
        format!("x(?:{}){{1000}}", a(3400)),
        format!("x(?:{}){{1000,}}", a(3400)),
        format!(".*(?:{}){{1000}}", a(3400)),
        format!("x(?:{}){{1000}}y{{2}}", a(3400)),
        format!("x(?:{}){{1000}}y{{4}}", a(3400)),
        format!("x(?:{}){{1000}}(y*)(z+)(w?)", a(3400)),
        format!("x(?:{}){{1000}}y{{1000}}", a(3400)),
        format!("(?:{}){{1000}}(?:{}){{1000}}.*", "ab".repeat(900), "cd".repeat(900)),
        "a{1000}b{1000}c{1000}d{1000}.*".to_string(),
        "[a-z0-9]{1000}[a-z0-9]{1000}[a-z0-9]{1000}[a-z0-9]{1000}".to_string(),
        "(?:[0-9]{1,3}\\.){3}[0-9]{1,3}".to_string(),
        "(?:[a-f0-9]{8})-(?:[a-f0-9]{4})-(?:[a-f0-9]{4})-(?:[a-f0-9]{4})-(?:[a-f0-9]{12})"
            .to_string(),
        "(?:(?:a{20}))".to_string(),
        // No size limit on literals and alternations while repetition counts are small.
        format!("{}.*", a(3_400_000)),
        format!("(?:{})*", a(3_400_000)),
        big_alt.clone(),
        format!("foo.*({big_alt})"),
        // Sizes tracked: stale cached size of a literal run vs. a run pushed again on its own.
        format!("x{{1000}}y{{1000}}z{{1000}}{}", a(3_400_000)),
        format!("x{{1000}}y{{1000}}z{{1000}}{}.", a(3_400_000)),
        format!("(?:(?:a{{20}})){}", "b".repeat(3_400_000)),
        format!("x{{1000}}y{{1000}}z{{1000}}({})", a(3_400_000)),
        format!("x{{1000}}y{{1000}}z{{1000}}(?:{})w", a(3_400_000)),
        format!("(?:(?:a{{20}}))({})", "b".repeat(3_400_000)),
        format!("{}x{{1000}}y{{1000}}z{{1000}}", a(3_400_000)),
        // numRunes.
        format!("{}.*", "\\pL".repeat(27_000)),
        format!("{}.*", "\\pL".repeat(20_000)),
        format!("{}.*", "\\w".repeat(100_000)),
    ];

    let resp: RegexMatcherErrorsResponse =
        invoke_oracle("regex-matcher-errors", &RegexMatcherErrorsRequest { patterns: &patterns })
            .expect("go-oracle regex-matcher-errors");
    assert_eq!(resp.errors.len(), patterns.len());

    let head = |s: &str| s.chars().take(70).collect::<String>();
    let mut mismatches = Vec::new();
    let (mut go_ok, mut go_large, mut go_deep) = (0usize, 0usize, 0usize);
    for (p, go) in patterns.iter().zip(&resp.errors) {
        let rust = match mimir_tsdb::go_regexp_syntax::check(p) {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        };
        if go.is_empty() {
            go_ok += 1;
        } else if go.contains("expression too large") {
            go_large += 1;
        } else if go.contains("expression nests too deeply") {
            go_deep += 1;
        }
        if &rust != go {
            mismatches.push(format!(
                "{}... ({} bytes):\n    go:   {}\n    rust: {}",
                head(p),
                p.len(),
                head(go),
                head(&rust)
            ));
        }
    }
    assert!(
        go_ok >= 20 && go_large >= 12 && go_deep >= 4,
        "corpus must exercise both sides of every limit (ok={go_ok}, large={go_large}, deep={go_deep})"
    );
    assert!(
        mismatches.is_empty(),
        "{} of {} patterns differ:\n{}",
        mismatches.len(),
        patterns.len(),
        mismatches.join("\n")
    );
}
