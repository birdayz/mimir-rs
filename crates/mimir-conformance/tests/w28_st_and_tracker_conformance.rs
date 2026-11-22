//! Wave 28 Track 4: Cross-Engine ST Chunk/WAL/Block & Live PropagationDelayTracker Conformance Suite.
//!
//! Covers C-1..C-7 from the Wave 28 Scope C differential conformance audit:
//! 1. `w28_histogram_st_and_float_histogram_st_chunk_bit_for_bit_parity`:
//!    Bit-for-bit chunk bytes + decoded `(st, t, val/hist)` parity against `go-oracle` for
//!    `EncXOR2`, `EncHistogramST`, and `EncFloatHistogramST` across constant ST, changing ST
//!    (counter resets, >127 samples), `st == 0 -> st != 0`, negative timestamps, and NHCB custom buckets.
//! 2. `w28_bidirectional_st_wal_wbl_checkpoint_and_block_interop`:
//!    - Go writes ST TSDB (floats + int/float histograms + NHCB with ST, OOO WBL ST records,
//!      WAL checkpoint, `chunks_head`, compacted ULID block) -> Rust `Tsdb::open` replays and queries
//!      identical `(st, t, v/hist)`, including reopening with `enable_st_storage = false`,
//!      `float_chunk_encoding = Xor2`, `enable_histogram_st_encoding = true` (locks down C-1).
//!    - Rust writes ST TSDB (in-order + OOO + WAL checkpoint + `chunks_head` + compacted ULID
//!      block + OOO compaction) -> Go `go-oracle read-st-tsdb` opens, replays, and reads identical `(st, t, v/hist)`.
//! 3. `w28_propagation_delay_tracker_format_metrics_and_live_gossip_parity`:
//!    - Byte-for-byte parity of `PropagationDelayTrackerDesc::go_string()` and `to_go_json()`
//!      against `go-oracle format-propagation-tracker` (empty, single, multi-beacon with hex keys
//!      >= 0xa and tombstones; locks down C-5).
//!    - Live mixed Go + Rust `memberlist` cluster with `-memberlist.propagation-delay-tracker.enabled=true`:
//!      verifies bidirectional gossip, protobuf `/metrics` native + classic histogram buckets and
//!      published/received counters (C-4), `GET /memberlist?viewKey=memberlist-propagation-delay-tracker` (C-5),
//!      and a disabled node (`enabled=false`) returning 404 on `viewKey` and incrementing
//!      `memberlist_client_received_broadcasts_invalid_total` (C-3).

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use bytes::Buf;
use mimir_common::block_on_for_tests;
use mimir_conformance::free_test_port;
use mimir_conformance::oracle::{
    ensure_go_oracle_built, invoke_oracle, ChunkSampleJson, LabelJson, SpanJson,
};
use mimir_conformance::twin::{http_get, start_mimir, wait_ready, Impl};
use mimir_proto::memberlistpb::{BeaconDesc, PropagationDelayTrackerDesc};
use mimir_proto::prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
use mimir_ring::kv::memberlist::propagation_delay_tracker_codec;
use mimir_tsdb::chunkenc::float_histogram_st::AppendFloatHistogramStOutcome;
use mimir_tsdb::chunkenc::histogram_st::AppendHistogramStOutcome;
use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHint, Encoding, FloatHistogram, FloatHistogramStChunk, Histogram,
    HistogramSpan, HistogramStChunk, Xor2AppendState, Xor2Chunk, CUSTOM_BUCKETS_SCHEMA,
};
use mimir_tsdb::compact::SampleValue;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// -----------------------------------------------------------------------------
// Oracle JSON schemas (local to w28 conformance so shared oracle.rs stays untouched)
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
struct StHistogramJson {
    #[serde(default)]
    st: i64,
    timestamp_ms: i64,
    schema: i32,
    zero_threshold: f64,
    #[serde(default)]
    zero_count: u64,
    #[serde(default)]
    zero_count_float: f64,
    #[serde(default)]
    count: u64,
    #[serde(default)]
    count_float: f64,
    sum: f64,
    reset_hint: u32,
    #[serde(default)]
    is_float: bool,
    #[serde(default)]
    positive_spans: Vec<SpanJson>,
    #[serde(default)]
    positive_deltas: Vec<i64>,
    #[serde(default)]
    positive_counts: Vec<f64>,
    #[serde(default)]
    negative_spans: Vec<SpanJson>,
    #[serde(default)]
    negative_deltas: Vec<i64>,
    #[serde(default)]
    negative_counts: Vec<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    custom_values: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StEncodeChunkRequest {
    encoding: u8,
    #[serde(default)]
    samples: Vec<ChunkSampleJson>,
    #[serde(default)]
    histograms: Vec<StHistogramJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StDecodeChunkRequest {
    encoding: u8,
    bytes_b64: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StChunkResponse {
    encoding: u8,
    num_samples: usize,
    #[serde(default)]
    bytes_b64: String,
    #[serde(default)]
    samples: Vec<ChunkSampleJson>,
    #[serde(default)]
    histograms: Vec<StHistogramJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StSeriesFixtureJson {
    labels: Vec<LabelJson>,
    #[serde(default)]
    samples: Vec<ChunkSampleJson>,
    #[serde(default)]
    histograms: Vec<StHistogramJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StBatchFixtureJson {
    series: Vec<StSeriesFixtureJson>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WriteStTsdbFixtureRequest {
    dir: String,
    #[serde(default)]
    enable_st_storage: bool,
    #[serde(default)]
    float_chunk_encoding: String,
    #[serde(default)]
    enable_histogram_st_encoding: bool,
    #[serde(default)]
    out_of_order_time_window_ms: i64,
    #[serde(default)]
    wal_segment_size: usize,
    #[serde(default)]
    samples_per_chunk: usize,
    #[serde(default)]
    min_block_duration_ms: i64,
    #[serde(default)]
    max_block_duration_ms: i64,
    #[serde(default)]
    batches: Vec<StBatchFixtureJson>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compact_head_before_ms: Option<i64>,
    #[serde(default)]
    compact: bool,
    #[serde(default)]
    compact_ooo: bool,
    #[serde(default)]
    post_compact_batches: Vec<StBatchFixtureJson>,
}

#[derive(Debug, Clone, Deserialize)]
struct WriteStTsdbFixtureResponse {
    status: String,
    #[serde(default)]
    blocks: Vec<String>,
    #[serde(default)]
    files: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
struct ReadStTsdbRequest {
    dir: String,
    enable_st_storage: bool,
    float_chunk_encoding: String,
    enable_histogram_st_encoding: bool,
    out_of_order_time_window_ms: i64,
    read_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ReadStTsdbResponse {
    block_ulids: Vec<String>,
    #[allow(dead_code)]
    head_series: u64,
    #[serde(default)]
    files: BTreeMap<String, Vec<String>>,
    series: Vec<StSeriesFixtureJson>,
}

#[derive(Debug, Clone, Serialize)]
struct BeaconInputJson {
    published_by: String,
    published_at: i64,
    deleted_at: i64,
}

#[derive(Debug, Clone, Serialize)]
struct FormatPropTrackerRequest {
    value_b64: String,
    beacons: BTreeMap<String, BeaconInputJson>,
}

#[derive(Debug, Clone, Deserialize)]
struct FormatPropTrackerResponse {
    go_string: String,
    json: String,
    json_indent: String,
    json_encode: String,
    json_encode_indent: String,
}

// -----------------------------------------------------------------------------
// Histogram conversion helpers
// -----------------------------------------------------------------------------

fn to_rust_hist(h: &StHistogramJson) -> Histogram {
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

fn to_rust_fhist(h: &StHistogramJson) -> FloatHistogram {
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

fn from_rust_hist(st: i64, t: i64, h: &Histogram) -> StHistogramJson {
    StHistogramJson {
        st,
        timestamp_ms: t,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        count: h.count,
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
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_deltas: h.negative_buckets.clone(),
        custom_values: h.custom_values.clone(),
        ..Default::default()
    }
}

fn from_rust_fhist(st: i64, t: i64, h: &FloatHistogram) -> StHistogramJson {
    StHistogramJson {
        st,
        timestamp_ms: t,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count_float: h.zero_count,
        count_float: h.count,
        sum: h.sum,
        reset_hint: h.counter_reset_hint as u32,
        is_float: true,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_counts: h.positive_buckets.clone(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_counts: h.negative_buckets.clone(),
        custom_values: h.custom_values.clone(),
        ..Default::default()
    }
}

fn assert_st_hist_eq(ctx: &str, got: &StHistogramJson, want: &StHistogramJson) {
    assert_eq!(got.st, want.st, "{ctx}: st mismatch");
    assert_eq!(got.timestamp_ms, want.timestamp_ms, "{ctx}: t mismatch");
    assert_eq!(got.is_float, want.is_float, "{ctx}: is_float mismatch");
    assert_eq!(got.schema, want.schema, "{ctx}: schema mismatch");
    assert_eq!(
        got.zero_threshold.to_bits(),
        want.zero_threshold.to_bits(),
        "{ctx}: zero_threshold bits mismatch"
    );
    assert_eq!(
        got.sum.to_bits(),
        want.sum.to_bits(),
        "{ctx}: sum bits mismatch"
    );
    assert_eq!(got.positive_spans, want.positive_spans, "{ctx}: pos spans");
    assert_eq!(got.negative_spans, want.negative_spans, "{ctx}: neg spans");
    assert_eq!(
        got.custom_values
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>(),
        want.custom_values
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>(),
        "{ctx}: custom_values bits"
    );
    if want.is_float {
        assert_eq!(
            got.zero_count_float.to_bits(),
            want.zero_count_float.to_bits(),
            "{ctx}: zero_count_float bits"
        );
        assert_eq!(
            got.count_float.to_bits(),
            want.count_float.to_bits(),
            "{ctx}: count_float bits"
        );
        assert_eq!(
            got.positive_counts
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
            want.positive_counts
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
            "{ctx}: pos counts bits"
        );
        assert_eq!(
            got.negative_counts
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
            want.negative_counts
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
            "{ctx}: neg counts bits"
        );
    } else {
        assert_eq!(got.zero_count, want.zero_count, "{ctx}: zero_count");
        assert_eq!(got.count, want.count, "{ctx}: count");
        assert_eq!(got.positive_deltas, want.positive_deltas, "{ctx}: pos deltas");
        assert_eq!(got.negative_deltas, want.negative_deltas, "{ctx}: neg deltas");
    }
}

// -----------------------------------------------------------------------------
// Test 1: Bit-for-bit chunk codec parity for EncXOR2, EncHistogramST, EncFloatHistogramST
// -----------------------------------------------------------------------------

fn assert_xor2_bit_for_bit(name: &str, samples: &[ChunkSampleJson]) {
    let mut chunk_app = Xor2Chunk::new();
    {
        let mut app = chunk_app.appender().expect("xor2 appender");
        for s in samples {
            app.append(s.st, s.t, s.v).expect("xor2 append");
        }
    }
    chunk_app.compact();
    let rust_bytes = chunk_app.into_bytes();

    let mut chunk_fast = Xor2Chunk::new();
    let mut state = Xor2AppendState::new_empty();
    for s in samples {
        chunk_fast
            .append_with_state(&mut state, s.st, s.t, s.v)
            .expect("xor2 append_with_state");
    }
    chunk_fast.compact();
    assert_eq!(
        chunk_fast.into_bytes(),
        rust_bytes,
        "{name}: Xor2AppendState must match Xor2Appender bit-for-bit"
    );

    let go_enc: StChunkResponse = invoke_oracle(
        "encode-chunk",
        &StEncodeChunkRequest {
            encoding: Encoding::Xor2.as_u8(),
            samples: samples.to_vec(),
            histograms: Vec::new(),
        },
    )
    .expect("go encode-chunk xor2");
    let go_bytes = B64.decode(&go_enc.bytes_b64).expect("b64");
    assert_eq!(
        rust_bytes, go_bytes,
        "{name}: Rust Xor2Chunk bytes must match Go EncXOR2 bit-for-bit"
    );

    let go_dec: StChunkResponse = invoke_oracle(
        "decode-chunk",
        &StDecodeChunkRequest {
            encoding: Encoding::Xor2.as_u8(),
            bytes_b64: B64.encode(&rust_bytes),
        },
    )
    .expect("go decode-chunk xor2");
    assert_eq!(go_dec.samples.len(), samples.len(), "{name}: decoded len");
    for (i, (got, want)) in go_dec.samples.iter().zip(samples.iter()).enumerate() {
        assert_eq!(got.st, want.st, "{name}[{i}]: go decoded st");
        assert_eq!(got.t, want.t, "{name}[{i}]: go decoded t");
        assert_eq!(
            got.v.to_bits(),
            want.v.to_bits(),
            "{name}[{i}]: go decoded v bits"
        );
    }

    let rust_redec = Xor2Chunk::from_bytes(go_bytes);
    let mut it = rust_redec.iter();
    let mut i = 0usize;
    while let Some((t, v)) = it.next_sample().expect("rust xor2 iter") {
        let st = it.at_st();
        assert_eq!(st, samples[i].st, "{name}[{i}]: rust decoded st");
        assert_eq!(t, samples[i].t, "{name}[{i}]: rust decoded t");
        assert_eq!(
            v.to_bits(),
            samples[i].v.to_bits(),
            "{name}[{i}]: rust decoded v bits"
        );
        i += 1;
    }
    assert_eq!(i, samples.len(), "{name}: rust decoded sample count");
}

fn assert_int_hist_st_parity(name: &str, hists: &[StHistogramJson], bit_for_bit: bool) {
    let mut c_app = HistogramStChunk::new();
    {
        let mut app = c_app.appender().expect("hst appender");
        for h in hists {
            let rh = to_rust_hist(h);
            match app
                .append_histogram(None, h.st, h.timestamp_ms, &rh, false)
                .expect("hst append")
            {
                AppendHistogramStOutcome::Appended { .. } => {}
                AppendHistogramStOutcome::NewChunk(_) => panic!("{name}: unexpected NewChunk"),
            }
        }
    }
    c_app.compact();
    let rust_bytes = c_app.into_bytes();

    let mut c_fast = HistogramStChunk::new();
    let mut cached = None;
    for h in hists {
        let rh = to_rust_hist(h);
        match c_fast
            .append_with_state(&mut cached, h.st, h.timestamp_ms, &rh)
            .expect("hst append_with_state")
        {
            AppendHistogramStOutcome::Appended { .. } => {}
            AppendHistogramStOutcome::NewChunk(_) => panic!("{name}: unexpected NewChunk (fast)"),
        }
    }
    c_fast.compact();
    assert_eq!(
        c_fast.into_bytes(),
        rust_bytes,
        "{name}: HistogramSt append_with_state must match appender bit-for-bit"
    );

    if bit_for_bit {
        let go_enc: StChunkResponse = invoke_oracle(
            "encode-chunk",
            &StEncodeChunkRequest {
                encoding: Encoding::HistogramSt.as_u8(),
                samples: Vec::new(),
                histograms: hists.to_vec(),
            },
        )
        .expect("go encode-chunk histogramst");
        let go_bytes = B64.decode(&go_enc.bytes_b64).expect("b64");
        assert_eq!(
            rust_bytes, go_bytes,
            "{name}: Rust HistogramStChunk bytes must match Go EncHistogramST bit-for-bit"
        );

        let rust_redec = HistogramStChunk::from_bytes(go_bytes);
        let mut it = rust_redec.iter();
        let mut idx = 0usize;
        while let Some((t, h)) = it.next_histogram().expect("rust hst iter") {
            let decoded = from_rust_hist(it.at_st(), t, &h);
            assert_st_hist_eq(&format!("{name}:go->rust[{idx}]"), &decoded, &hists[idx]);
            idx += 1;
        }
        assert_eq!(idx, hists.len(), "{name}: rust decoded count");
    }

    let rust_self_dec = {
        let c = HistogramStChunk::from_bytes(rust_bytes.clone());
        let mut it = c.iter();
        let mut out = Vec::new();
        while let Some((t, h)) = it.next_histogram().expect("rust hst self iter") {
            out.push(from_rust_hist(it.at_st(), t, &h));
        }
        out
    };
    let go_dec: StChunkResponse = invoke_oracle(
        "decode-chunk",
        &StDecodeChunkRequest {
            encoding: Encoding::HistogramSt.as_u8(),
            bytes_b64: B64.encode(&rust_bytes),
        },
    )
    .expect("go decode-chunk histogramst");
    assert_eq!(go_dec.histograms.len(), hists.len(), "{name}: go dec len");
    assert_eq!(rust_self_dec.len(), hists.len(), "{name}: rust self dec len");
    for (idx, (got_go, got_rs)) in go_dec.histograms.iter().zip(rust_self_dec.iter()).enumerate() {
        assert_st_hist_eq(&format!("{name}:rust->go[{idx}]"), got_go, got_rs);
        if bit_for_bit {
            assert_st_hist_eq(&format!("{name}:exact[{idx}]"), got_go, &hists[idx]);
        } else {
            assert_eq!(got_go.st, hists[idx].st, "{name}[{idx}]: st");
            assert_eq!(got_go.timestamp_ms, hists[idx].timestamp_ms, "{name}[{idx}]: t");
            assert_eq!(got_go.sum.to_bits(), hists[idx].sum.to_bits(), "{name}[{idx}]: sum");
            assert_eq!(got_go.count, hists[idx].count, "{name}[{idx}]: count");
        }
    }
}

fn assert_float_hist_st_parity(name: &str, hists: &[StHistogramJson], bit_for_bit: bool) {
    let mut c_app = FloatHistogramStChunk::new();
    {
        let mut app = c_app.appender().expect("fhst appender");
        for h in hists {
            let rfh = to_rust_fhist(h);
            match app
                .append_float_histogram(None, h.st, h.timestamp_ms, &rfh, false)
                .expect("fhst append")
            {
                AppendFloatHistogramStOutcome::Appended { .. } => {}
                AppendFloatHistogramStOutcome::NewChunk(_) => {
                    panic!("{name}: unexpected NewChunk")
                }
            }
        }
    }
    c_app.compact();
    let rust_bytes = c_app.into_bytes();

    let mut c_fast = FloatHistogramStChunk::new();
    let mut cached = None;
    for h in hists {
        let rfh = to_rust_fhist(h);
        match c_fast
            .append_with_state(&mut cached, h.st, h.timestamp_ms, &rfh)
            .expect("fhst append_with_state")
        {
            AppendFloatHistogramStOutcome::Appended { .. } => {}
            AppendFloatHistogramStOutcome::NewChunk(_) => {
                panic!("{name}: unexpected NewChunk (fast)")
            }
        }
    }
    c_fast.compact();
    assert_eq!(
        c_fast.into_bytes(),
        rust_bytes,
        "{name}: FloatHistogramSt append_with_state must match appender bit-for-bit"
    );

    if bit_for_bit {
        let go_enc: StChunkResponse = invoke_oracle(
            "encode-chunk",
            &StEncodeChunkRequest {
                encoding: Encoding::FloatHistogramSt.as_u8(),
                samples: Vec::new(),
                histograms: hists.to_vec(),
            },
        )
        .expect("go encode-chunk floathistogramst");
        let go_bytes = B64.decode(&go_enc.bytes_b64).expect("b64");
        assert_eq!(
            rust_bytes, go_bytes,
            "{name}: Rust FloatHistogramStChunk bytes must match Go EncFloatHistogramST bit-for-bit"
        );

        let rust_redec = FloatHistogramStChunk::from_bytes(go_bytes);
        let mut it = rust_redec.iter();
        let mut idx = 0usize;
        while let Some((t, fh)) = it.next_float_histogram().expect("rust fhst iter") {
            let decoded = from_rust_fhist(it.at_st(), t, &fh);
            assert_st_hist_eq(&format!("{name}:go->rust[{idx}]"), &decoded, &hists[idx]);
            idx += 1;
        }
        assert_eq!(idx, hists.len(), "{name}: rust decoded count");
    }

    let rust_self_dec = {
        let c = FloatHistogramStChunk::from_bytes(rust_bytes.clone());
        let mut it = c.iter();
        let mut out = Vec::new();
        while let Some((t, fh)) = it.next_float_histogram().expect("rust fhst self iter") {
            out.push(from_rust_fhist(it.at_st(), t, &fh));
        }
        out
    };
    let go_dec: StChunkResponse = invoke_oracle(
        "decode-chunk",
        &StDecodeChunkRequest {
            encoding: Encoding::FloatHistogramSt.as_u8(),
            bytes_b64: B64.encode(&rust_bytes),
        },
    )
    .expect("go decode-chunk floathistogramst");
    assert_eq!(go_dec.histograms.len(), hists.len(), "{name}: go dec len");
    assert_eq!(rust_self_dec.len(), hists.len(), "{name}: rust self dec len");
    for (idx, (got_go, got_rs)) in go_dec.histograms.iter().zip(rust_self_dec.iter()).enumerate() {
        assert_st_hist_eq(&format!("{name}:rust->go[{idx}]"), got_go, got_rs);
        if bit_for_bit {
            assert_st_hist_eq(&format!("{name}:exact[{idx}]"), got_go, &hists[idx]);
        } else {
            assert_eq!(got_go.st, hists[idx].st, "{name}[{idx}]: st");
            assert_eq!(got_go.timestamp_ms, hists[idx].timestamp_ms, "{name}[{idx}]: t");
            assert_eq!(got_go.sum.to_bits(), hists[idx].sum.to_bits(), "{name}[{idx}]: sum");
            assert_eq!(got_go.count_float.to_bits(), hists[idx].count_float.to_bits(), "{name}[{idx}]: count_float");
        }
    }
}

fn make_int_hist(st: i64, t: i64, reset_hint: u32, c0: i64, c1: i64) -> StHistogramJson {
    StHistogramJson {
        st,
        timestamp_ms: t,
        schema: 0,
        zero_threshold: 0.001,
        zero_count: 2,
        count: (2 + c0 + (c0 + c1) + 1) as u64,
        sum: 12.5 + c0 as f64,
        reset_hint,
        is_float: false,
        positive_spans: vec![SpanJson { offset: 0, length: 2 }],
        positive_deltas: vec![c0, c1],
        negative_spans: vec![SpanJson { offset: 1, length: 1 }],
        negative_deltas: vec![1],
        custom_values: Vec::new(),
        ..Default::default()
    }
}

fn make_float_hist(st: i64, t: i64, reset_hint: u32, b0: f64, b1: f64) -> StHistogramJson {
    StHistogramJson {
        st,
        timestamp_ms: t,
        schema: 0,
        zero_threshold: 0.001,
        zero_count_float: 1.5,
        count_float: 1.5 + b0 + b1 + 0.5,
        sum: 15.25 + b0,
        reset_hint,
        is_float: true,
        positive_spans: vec![SpanJson { offset: 0, length: 2 }],
        positive_counts: vec![b0, b1],
        negative_spans: vec![SpanJson { offset: 1, length: 1 }],
        negative_counts: vec![0.5],
        custom_values: Vec::new(),
        ..Default::default()
    }
}

fn make_nhcb_int_hist(st: i64, t: i64, reset_hint: u32, d0: i64, d1: i64, d2: i64) -> StHistogramJson {
    let b0 = d0;
    let b1 = b0 + d1;
    let b2 = b1 + d2;
    StHistogramJson {
        st,
        timestamp_ms: t,
        schema: CUSTOM_BUCKETS_SCHEMA,
        zero_threshold: 0.0,
        count: (b0 + b1 + b2) as u64,
        sum: 14.25,
        reset_hint,
        is_float: false,
        positive_spans: vec![SpanJson { offset: 0, length: 3 }],
        positive_deltas: vec![d0, d1, d2],
        custom_values: vec![0.1, 0.5, 1.0, 5.0],
        ..Default::default()
    }
}

fn make_nhcb_float_hist(
    st: i64,
    t: i64,
    reset_hint: u32,
    c0: f64,
    c1: f64,
    c2: f64,
) -> StHistogramJson {
    StHistogramJson {
        st,
        timestamp_ms: t,
        schema: CUSTOM_BUCKETS_SCHEMA,
        zero_threshold: 0.0,
        count_float: c0 + c1 + c2,
        sum: 21.5,
        reset_hint,
        is_float: true,
        positive_spans: vec![SpanJson { offset: 0, length: 3 }],
        positive_counts: vec![c0, c1, c2],
        custom_values: vec![0.1, 0.5, 1.0, 5.0],
        ..Default::default()
    }
}

#[test]
fn w28_histogram_st_and_float_histogram_st_chunk_bit_for_bit_parity() {
    ensure_go_oracle_built().expect("go-oracle");

    // 1. EncXOR2: constant ST, changing ST (>127), st == 0 -> st != 0 (>127), negative timestamps
    assert_xor2_bit_for_bit(
        "xor2_constant_st_and_special_floats",
        &[
            ChunkSampleJson { st: 1_699_999_000_000, t: 1_700_000_000_000, v: 1.0 },
            ChunkSampleJson { st: 1_699_999_000_000, t: 1_700_000_015_000, v: 2.5 },
            ChunkSampleJson { st: 1_699_999_000_000, t: 1_700_000_030_000, v: -0.0 },
            ChunkSampleJson { st: 1_699_999_000_000, t: 1_700_000_045_000, v: 1e12 },
            ChunkSampleJson { st: 1_699_999_000_000, t: 1_700_000_060_000, v: -1e-12 },
            ChunkSampleJson { st: 1_699_999_000_000, t: 1_700_000_075_000, v: 3.75 },
        ],
    );
    // Verify StaleNaN and Inf roundtrip in Xor2Chunk with ST preserved.
    {
        let mut c = Xor2Chunk::new();
        let special = [
            (100_i64, 1_000_i64, 1.5_f64),
            (100, 2_000, f64::INFINITY),
            (100, 3_000, stale_nan()),
            (200, 4_000, 2.5),
        ];
        {
            let mut app = c.appender().expect("xor2 appender");
            for &(st, t, v) in &special {
                app.append(st, t, v).expect("xor2 append special");
            }
        }
        let mut it = c.iter();
        let mut idx = 0;
        while let Some((t, v)) = it.next_sample().expect("xor2 iter special") {
            assert_eq!(it.at_st(), special[idx].0);
            assert_eq!(t, special[idx].1);
            assert_eq!(v.to_bits(), special[idx].2.to_bits());
            idx += 1;
        }
        assert_eq!(idx, special.len());
    }
    let xor2_changing_gt_127: Vec<ChunkSampleJson> = (0..140)
        .map(|i| ChunkSampleJson {
            st: if i < 2 {
                1_699_999_000_000
            } else if i < 133 {
                1_700_000_000_000
            } else {
                1_700_001_000_000
            },
            t: 1_700_000_000_000 + i * 15_000,
            v: (i as f64) * 1.25,
        })
        .collect();
    assert_xor2_bit_for_bit("xor2_changing_st_gt_127", &xor2_changing_gt_127);

    let xor2_zero_to_nonzero: Vec<ChunkSampleJson> = (0..135)
        .map(|i| ChunkSampleJson {
            st: if i < 130 { 0 } else { 1_700_001_500_000 },
            t: 1_700_000_000_000 + i * 10_000,
            v: 100.0 + i as f64,
        })
        .collect();
    assert_xor2_bit_for_bit("xor2_zero_to_nonzero_gt_127", &xor2_zero_to_nonzero);
    assert_xor2_bit_for_bit(
        "xor2_zero_to_nonzero_short",
        &[
            ChunkSampleJson { st: 0, t: 10_000, v: 1.0 },
            ChunkSampleJson { st: 0, t: 20_000, v: 2.0 },
            ChunkSampleJson { st: 25_000, t: 30_000, v: 0.5 },
            ChunkSampleJson { st: 25_000, t: 40_000, v: 1.5 },
        ],
    );
    assert_xor2_bit_for_bit(
        "xor2_negative_timestamps",
        &[
            ChunkSampleJson { st: -50_000, t: -20_000, v: 1.0 },
            ChunkSampleJson { st: -50_000, t: -10_000, v: 2.0 },
            ChunkSampleJson { st: -5_000, t: 0, v: 0.25 },
            ChunkSampleJson { st: -5_000, t: 10_000, v: 1.25 },
        ],
    );

    // 2. EncHistogramST
    assert_int_hist_st_parity(
        "int_hist_st_constant",
        &[
            make_int_hist(1_699_999_000_000, 1_700_000_000_000, 2, 3, -1),
            make_int_hist(1_699_999_000_000, 1_700_000_015_000, 2, 5, -2),
            make_int_hist(1_699_999_000_000, 1_700_000_030_000, 2, 8, -3),
        ],
        true,
    );
    assert_int_hist_st_parity(
        "int_hist_st_changing_st_gauge",
        &[
            make_int_hist(1_699_999_000_000, 1_700_000_000_000, 3, 3, -1),
            make_int_hist(1_699_999_000_000, 1_700_000_015_000, 3, 6, -2),
            make_int_hist(1_700_000_020_000, 1_700_000_030_000, 3, 1, 0),
            make_int_hist(1_700_000_020_000, 1_700_000_045_000, 3, 4, -1),
            make_int_hist(1_700_000_050_000, 1_700_000_060_000, 3, 2, 1),
        ],
        true,
    );
    assert_int_hist_st_parity(
        "int_hist_st_changing_st_counter_chunk",
        &[
            make_int_hist(1_699_999_000_000, 1_700_000_000_000, 1, 2, 0),
            make_int_hist(1_699_999_000_000, 1_700_000_015_000, 2, 4, 0),
            make_int_hist(1_700_000_020_000, 1_700_000_030_000, 2, 6, 0),
            make_int_hist(1_700_000_035_000, 1_700_000_045_000, 2, 8, 0),
        ],
        true,
    );
    assert_int_hist_st_parity(
        "int_hist_st_zero_to_nonzero_short",
        &[
            make_int_hist(0, 10_000, 2, 2, 1),
            make_int_hist(0, 20_000, 2, 4, 0),
            make_int_hist(25_000, 30_000, 2, 5, 0),
            make_int_hist(25_000, 40_000, 2, 6, 1),
        ],
        true,
    );
    let int_hist_zero_gt_127: Vec<StHistogramJson> = (0..134)
        .map(|i| {
            let st = if i < 130 { 0 } else { 1_700_001_500_000 };
            make_int_hist(st, 1_700_000_000_000 + i * 10_000, 2, 2 + i, 1)
        })
        .collect();
    assert_int_hist_st_parity("int_hist_st_zero_to_nonzero_gt_127", &int_hist_zero_gt_127, true);
    assert_int_hist_st_parity(
        "int_hist_st_negative_timestamps",
        &[
            make_int_hist(-50_000, -20_000, 2, 3, -1),
            make_int_hist(-50_000, -10_000, 2, 5, -2),
            make_int_hist(-5_000, 0, 2, 7, -1),
            make_int_hist(-5_000, 10_000, 2, 9, 0),
        ],
        true,
    );
    assert_int_hist_st_parity(
        "int_hist_st_nhcb",
        &[
            make_nhcb_int_hist(0, -20_000, 3, 2, -1, 1),
            make_nhcb_int_hist(0, -10_000, 3, 3, 0, 2),
            make_nhcb_int_hist(-5_000, 0, 3, 1, 1, -1),
            make_nhcb_int_hist(5_000, 10_000, 3, 4, -2, 0),
        ],
        true,
    );
    let mut recode_h1 = make_int_hist(500, 10_000, 2, 3, -1);
    recode_h1.positive_spans = vec![
        SpanJson { offset: 0, length: 2 },
        SpanJson { offset: 1, length: 1 },
    ];
    recode_h1.positive_deltas = vec![5, -2, -1];
    assert_int_hist_st_parity(
        "int_hist_st_recode",
        &[make_int_hist(500, 5_000, 2, 3, -1), recode_h1],
        false,
    );

    // 3. EncFloatHistogramST
    assert_float_hist_st_parity(
        "float_hist_st_constant",
        &[
            make_float_hist(1_699_999_000_000, 1_700_000_000_000, 2, 1.5, 2.5),
            make_float_hist(1_699_999_000_000, 1_700_000_015_000, 2, 3.0, 4.5),
            make_float_hist(1_699_999_000_000, 1_700_000_030_000, 2, 5.5, 7.25),
        ],
        true,
    );
    assert_float_hist_st_parity(
        "float_hist_st_changing_st_gauge",
        &[
            make_float_hist(1_699_999_000_000, 1_700_000_000_000, 3, 2.0, 3.0),
            make_float_hist(1_699_999_000_000, 1_700_000_015_000, 3, 4.0, 5.5),
            make_float_hist(1_700_000_020_000, 1_700_000_030_000, 3, 0.5, 1.0),
            make_float_hist(1_700_000_020_000, 1_700_000_045_000, 3, 2.5, 3.5),
        ],
        true,
    );
    assert_float_hist_st_parity(
        "float_hist_st_changing_st_counter_chunk",
        &[
            make_float_hist(1_699_999_000_000, 1_700_000_000_000, 1, 1.0, 2.0),
            make_float_hist(1_699_999_000_000, 1_700_000_015_000, 2, 3.0, 4.0),
            make_float_hist(1_700_000_020_000, 1_700_000_030_000, 2, 5.0, 6.0),
            make_float_hist(1_700_000_035_000, 1_700_000_045_000, 2, 7.0, 8.0),
        ],
        true,
    );
    assert_float_hist_st_parity(
        "float_hist_st_zero_to_nonzero_short",
        &[
            make_float_hist(0, 10_000, 2, 1.0, 2.0),
            make_float_hist(0, 20_000, 2, 3.0, 4.0),
            make_float_hist(25_000, 30_000, 2, 4.5, 5.0),
            make_float_hist(25_000, 40_000, 2, 6.5, 7.5),
        ],
        true,
    );
    let float_hist_zero_gt_127: Vec<StHistogramJson> = (0..134)
        .map(|i| {
            let st = if i < 130 { 0 } else { 1_700_001_500_000 };
            make_float_hist(
                st,
                1_700_000_000_000 + i * 10_000,
                2,
                1.25 + i as f64,
                2.5 + i as f64,
            )
        })
        .collect();
    assert_float_hist_st_parity(
        "float_hist_st_zero_to_nonzero_gt_127",
        &float_hist_zero_gt_127,
        true,
    );
    assert_float_hist_st_parity(
        "float_hist_st_negative_timestamps",
        &[
            make_float_hist(-50_000, -20_000, 2, 1.5, 2.5),
            make_float_hist(-50_000, -10_000, 2, 3.0, 4.0),
            make_float_hist(-5_000, 0, 2, 4.5, 5.0),
            make_float_hist(-5_000, 10_000, 2, 6.0, 7.0),
        ],
        true,
    );
    assert_float_hist_st_parity(
        "float_hist_st_nhcb",
        &[
            make_nhcb_float_hist(0, -20_000, 3, 1.25, 2.5, 3.75),
            make_nhcb_float_hist(0, -10_000, 3, 2.0, 3.0, 4.0),
            make_nhcb_float_hist(-5_000, 0, 3, 0.5, 1.5, 2.5),
            make_nhcb_float_hist(5_000, 10_000, 3, 3.5, 4.5, 5.5),
        ],
        true,
    );
    let mut recode_fh1 = make_float_hist(500, 10_000, 2, 2.0, 3.0);
    recode_fh1.positive_spans = vec![
        SpanJson { offset: 0, length: 2 },
        SpanJson { offset: 1, length: 1 },
    ];
    recode_fh1.positive_counts = vec![2.0, 3.0, 1.5];
    assert_float_hist_st_parity(
        "float_hist_st_recode",
        &[make_float_hist(500, 5_000, 2, 2.0, 3.0), recode_fh1],
        false,
    );
}

// -----------------------------------------------------------------------------
// Test 2: Bidirectional ST WAL, WBL, WAL Checkpoint, chunks_head, and ULID Block Interop
// -----------------------------------------------------------------------------

fn copy_dir_recursive(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).expect("mkdir dst");
    for entry in fs::read_dir(src).expect("readdir src") {
        let entry = entry.expect("dir entry");
        let ty = entry.file_type().expect("file_type");
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str == "lock" {
            continue;
        }
        let s = entry.path();
        let d = dst.join(&name);
        if ty.is_dir() {
            copy_dir_recursive(&s, &d);
        } else if ty.is_file() {
            fs::copy(&s, &d).expect("copy file");
        }
    }
}

fn lbls(pairs: &[(&str, &str)]) -> Vec<LabelJson> {
    pairs
        .iter()
        .map(|(n, v)| LabelJson {
            name: (*n).to_string(),
            value: (*v).to_string(),
        })
        .collect()
}

fn filter_w28_series(series: Vec<StSeriesFixtureJson>) -> Vec<StSeriesFixtureJson> {
    series
        .into_iter()
        .filter(|s| s.labels.iter().any(|l| l.name == "job" && l.value == "w28"))
        .collect()
}

fn rust_query_w28_series(
    dir: &Path,
    enable_st_storage: bool,
    float_chunk_encoding: Encoding,
    enable_histogram_st_encoding: bool,
) -> Vec<StSeriesFixtureJson> {
    let mut opts = TsdbOptions {
        no_lockfile: true,
        ..Default::default()
    };
    opts.head.enable_st_storage = enable_st_storage;
    opts.head.float_chunk_encoding = float_chunk_encoding;
    opts.head.enable_histogram_st_encoding = enable_histogram_st_encoding;
    opts.head.ooo_time_window_ms = 3_600_000;

    let db = Tsdb::open_with_reload_interval(dir, opts, Duration::ZERO)
        .expect("Rust Tsdb::open");
    let m = [LabelMatcher::new(MatchType::Equal, "job", "w28").expect("matcher")];
    let q = db.querier(i64::MIN, i64::MAX);
    let queried = q.select(true, &m).expect("Rust select");
    let mut out = Vec::with_capacity(queried.len());
    for qs in queried {
        let mut entry = StSeriesFixtureJson {
            labels: qs
                .labels
                .iter()
                .map(|(name, value)| LabelJson {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            samples: Vec::new(),
            histograms: Vec::new(),
        };
        for ds in qs.samples {
            match ds.value {
                SampleValue::Float(v) => entry.samples.push(ChunkSampleJson {
                    st: ds.st,
                    t: ds.t,
                    v,
                }),
                SampleValue::Histogram(h) => {
                    entry.histograms.push(from_rust_hist(ds.st, ds.t, &h));
                }
                SampleValue::FloatHistogram(fh) => {
                    entry.histograms.push(from_rust_fhist(ds.st, ds.t, &fh));
                }
            }
        }
        out.push(entry);
    }
    drop(q);
    db.close().expect("Rust db.close");
    out
}

fn assert_series_fixtures_eq(ctx: &str, got: &[StSeriesFixtureJson], want: &[StSeriesFixtureJson]) {
    assert_eq!(got.len(), want.len(), "{ctx}: series count mismatch");
    for (g, w) in got.iter().zip(want.iter()) {
        assert_eq!(g.labels, w.labels, "{ctx}: labels mismatch");
        assert_eq!(
            g.samples.len(),
            w.samples.len(),
            "{ctx} {:?}: float samples len",
            w.labels
        );
        for (i, (gs, ws)) in g.samples.iter().zip(w.samples.iter()).enumerate() {
            assert_eq!(gs.st, ws.st, "{ctx} {:?}[{i}]: float st", w.labels);
            assert_eq!(gs.t, ws.t, "{ctx} {:?}[{i}]: float t", w.labels);
            assert_eq!(
                gs.v.to_bits(),
                ws.v.to_bits(),
                "{ctx} {:?}[{i}]: float v bits",
                w.labels
            );
        }
        assert_eq!(
            g.histograms.len(),
            w.histograms.len(),
            "{ctx} {:?}: histograms len",
            w.labels
        );
        for (i, (gh, wh)) in g.histograms.iter().zip(w.histograms.iter()).enumerate() {
            assert_st_hist_eq(&format!("{ctx} {:?}[{i}]", w.labels), gh, wh);
        }
    }
}

fn build_st_workload_batches() -> (Vec<StBatchFixtureJson>, Vec<StBatchFixtureJson>) {
    let b0 = vec![
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float"), ("job", "w28")]),
            samples: vec![
                ChunkSampleJson { st: 900_000, t: 1_000_000, v: 10.5 },
                ChunkSampleJson { st: 900_000, t: 1_100_000, v: 20.5 },
                ChunkSampleJson { st: 1_150_000, t: 1_200_000, v: 1.5 },
            ],
            histograms: Vec::new(),
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_int_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_int_hist(900_000, 1_000_000, 2, 3, -1),
                make_int_hist(900_000, 1_100_000, 2, 5, -2),
                make_int_hist(1_150_000, 1_200_000, 1, 1, 0),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_float_hist(900_000, 1_000_000, 2, 1.5, 2.5),
                make_float_hist(900_000, 1_100_000, 2, 3.0, 4.5),
                make_float_hist(1_150_000, 1_200_000, 1, 0.5, 1.0),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_nhcb_int_hist(950_000, 1_000_000, 3, 2, -1, 1),
                make_nhcb_int_hist(950_000, 1_100_000, 3, 4, -2, 0),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_fhist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_nhcb_float_hist(950_000, 1_000_000, 3, 1.25, 2.5, 3.75),
                make_nhcb_float_hist(950_000, 1_100_000, 3, 2.25, 3.5, 4.75),
            ],
        },
    ];
    let mut filler_batches = Vec::new();
    for batch_idx in 0..6 {
        let mut fb = Vec::with_capacity(400);
        for i in 0..400 {
            let idx = batch_idx * 400 + i;
            fb.push(StSeriesFixtureJson {
                labels: lbls(&[
                    ("__name__", "filler_wal_roll"),
                    ("job", "filler"),
                    ("instance", &format!("inst-{idx:05}-padding-label-to-roll-wal-segments")),
                ]),
                samples: vec![
                    ChunkSampleJson { st: 500_000 + idx as i64, t: 1_000_000 + (batch_idx as i64) * 1000, v: idx as f64 },
                    ChunkSampleJson { st: 500_000 + idx as i64, t: 1_100_000 + (batch_idx as i64) * 1000, v: (idx + 1) as f64 },
                ],
                histograms: Vec::new(),
            });
        }
        filler_batches.push(StBatchFixtureJson { series: fb });
    }

    let b1 = vec![
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float"), ("job", "w28")]),
            samples: vec![ChunkSampleJson { st: 7_900_000, t: 8_000_000, v: 42.0 }],
            histograms: Vec::new(),
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_int_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![make_int_hist(7_900_000, 8_000_000, 1, 2, 1)],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![make_float_hist(7_900_000, 8_000_000, 1, 2.0, 3.0)],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![make_nhcb_int_hist(7_950_000, 8_000_000, 3, 1, 1, -1)],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_fhist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![make_nhcb_float_hist(7_950_000, 8_000_000, 3, 0.5, 1.5, 2.5)],
        },
    ];

    let post_in_order = vec![
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float"), ("job", "w28")]),
            samples: vec![
                ChunkSampleJson { st: 7_900_000, t: 9_000_000, v: 43.0 },
                ChunkSampleJson { st: 7_900_000, t: 9_100_000, v: 44.0 },
                ChunkSampleJson { st: 9_150_000, t: 9_200_000, v: 1.0 },
            ],
            histograms: Vec::new(),
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_int_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_int_hist(7_900_000, 9_000_000, 2, 4, -1),
                make_int_hist(7_900_000, 9_100_000, 2, 6, -2),
                make_int_hist(9_150_000, 9_200_000, 1, 1, 0),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_float_hist(7_900_000, 9_000_000, 2, 4.0, 5.0),
                make_float_hist(7_900_000, 9_100_000, 2, 6.0, 7.0),
                make_float_hist(9_150_000, 9_200_000, 1, 0.25, 0.75),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_nhcb_int_hist(7_950_000, 9_000_000, 3, 2, 0, -1),
                make_nhcb_int_hist(9_050_000, 9_100_000, 3, 3, -1, 2),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_fhist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_nhcb_float_hist(7_950_000, 9_000_000, 3, 1.5, 2.5, 3.5),
                make_nhcb_float_hist(9_050_000, 9_100_000, 3, 2.5, 3.5, 4.5),
            ],
        },
    ];

    let post_ooo = vec![
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float"), ("job", "w28")]),
            samples: vec![
                ChunkSampleJson { st: 8_400_000, t: 8_500_000, v: 99.5 },
                ChunkSampleJson { st: 8_400_000, t: 8_600_000, v: 100.5 },
            ],
            histograms: Vec::new(),
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_int_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_int_hist(8_400_000, 8_500_000, 2, 7, -3),
                make_int_hist(8_400_000, 8_600_000, 2, 9, -4),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_float_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![
                make_float_hist(8_400_000, 8_500_000, 2, 7.5, 8.5),
                make_float_hist(8_400_000, 8_600_000, 2, 9.5, 10.5),
            ],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_hist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![make_nhcb_int_hist(8_450_000, 8_500_000, 3, 5, -2, 1)],
        },
        StSeriesFixtureJson {
            labels: lbls(&[("__name__", "st_nhcb_fhist"), ("job", "w28")]),
            samples: Vec::new(),
            histograms: vec![make_nhcb_float_hist(8_450_000, 8_500_000, 3, 4.5, 5.5, 6.5)],
        },
    ];

    let mut pre = vec![StBatchFixtureJson { series: b0 }];
    pre.extend(filler_batches);
    pre.push(StBatchFixtureJson { series: b1 });
    (
        pre,
        vec![
            StBatchFixtureJson { series: post_in_order },
            StBatchFixtureJson { series: post_ooo },
        ],
    )
}

fn list_dir_sorted(dir: &Path) -> Vec<String> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = rd
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn assert_all_st_artifacts_present(ctx: &str, dir: &Path, blocks: &[String]) {
    assert!(!blocks.is_empty(), "{ctx}: expected compacted ULID block(s)");
    let wal = list_dir_sorted(&dir.join("wal"));
    assert!(
        wal.iter().any(|n| n.starts_with("checkpoint.")),
        "{ctx}: expected WAL checkpoint in {wal:?}"
    );
    assert!(
        wal.iter().any(|n| n.chars().all(|c| c.is_ascii_digit())),
        "{ctx}: expected WAL segment in {wal:?}"
    );
    let chunks_head = list_dir_sorted(&dir.join("chunks_head"));
    assert!(!chunks_head.is_empty(), "{ctx}: expected chunks_head files");
    let wbl = list_dir_sorted(&dir.join("wbl"));
    assert!(!wbl.is_empty(), "{ctx}: expected OOO WBL files");
}

#[test]
fn w28_bidirectional_st_wal_wbl_checkpoint_and_block_interop() {
    ensure_go_oracle_built().expect("go-oracle");
    let (pre_batches, post_batches) = build_st_workload_batches();

    // --- Part A: Go writes ST TSDB -> Rust opens & queries (with ST on AND ST off / C-1) ---
    let tmp_go = tempfile::tempdir().expect("tempdir go");
    let go_dir = tmp_go.path().join("tsdb");
    fs::create_dir_all(&go_dir).expect("mkdir go_dir");

    let go_write: WriteStTsdbFixtureResponse = invoke_oracle(
        "write-st-tsdb-fixture",
        &WriteStTsdbFixtureRequest {
            dir: go_dir.display().to_string(),
            enable_st_storage: true,
            float_chunk_encoding: "xor2".to_string(),
            enable_histogram_st_encoding: true,
            out_of_order_time_window_ms: 3_600_000,
            wal_segment_size: 32_768,
            samples_per_chunk: 2,
            min_block_duration_ms: 3_600_000,
            max_block_duration_ms: 3_600_000,
            batches: pre_batches.clone(),
            compact_head_before_ms: Some(5_000_000),
            compact: false,
            compact_ooo: false,
            post_compact_batches: post_batches.clone(),
        },
    )
    .expect("go write-st-tsdb-fixture");
    assert_eq!(go_write.status, "ok");
    assert_all_st_artifacts_present("go-written-fixture", &go_dir, &go_write.blocks);
    let _ = &go_write.files;

    let go_copy_on = tmp_go.path().join("go_reopen_on");
    copy_dir_recursive(&go_dir, &go_copy_on);
    let go_read_on: ReadStTsdbResponse = invoke_oracle(
        "read-st-tsdb",
        &ReadStTsdbRequest {
            dir: go_copy_on.display().to_string(),
            enable_st_storage: true,
            float_chunk_encoding: "xor2".to_string(),
            enable_histogram_st_encoding: true,
            out_of_order_time_window_ms: 3_600_000,
            read_only: false,
        },
    )
    .expect("go read-st-tsdb (st=true)");
    let expected_w28 = filter_w28_series(go_read_on.series);
    assert_eq!(expected_w28.len(), 5, "expected 5 w28 series");
    assert!(!go_read_on.block_ulids.is_empty());
    let _ = &go_read_on.files;

    let go_copy_off = tmp_go.path().join("go_reopen_off");
    copy_dir_recursive(&go_dir, &go_copy_off);
    let go_read_off: ReadStTsdbResponse = invoke_oracle(
        "read-st-tsdb",
        &ReadStTsdbRequest {
            dir: go_copy_off.display().to_string(),
            enable_st_storage: false,
            float_chunk_encoding: "xor2".to_string(),
            enable_histogram_st_encoding: true,
            out_of_order_time_window_ms: 3_600_000,
            read_only: false,
        },
    )
    .expect("go read-st-tsdb (st=false)");
    let expected_w28_off = filter_w28_series(go_read_off.series);
    assert_series_fixtures_eq(
        "Go reopen with enable_st_storage=false preserves ST",
        &expected_w28_off,
        &expected_w28,
    );

    let rust_copy_on = tmp_go.path().join("rust_reopen_on");
    copy_dir_recursive(&go_dir, &rust_copy_on);
    let rust_got_on = rust_query_w28_series(&rust_copy_on, true, Encoding::Xor2, true);
    assert_series_fixtures_eq(
        "Go->Rust ST TSDB (enable_st_storage=true)",
        &rust_got_on,
        &expected_w28,
    );

    // --- Part B: Rust writes ST TSDB -> Go opens, replays, compacts, & reads back ---
    let tmp_rs = tempfile::tempdir().expect("tempdir rust");
    let rs_dir = tmp_rs.path().join("tsdb");
    fs::create_dir_all(&rs_dir).expect("mkdir rs_dir");

    {
        let mut opts = TsdbOptions {
            no_lockfile: true,
            ..Default::default()
        };
        opts.head.enable_st_storage = true;
        opts.head.float_chunk_encoding = Encoding::Xor2;
        opts.head.enable_histogram_st_encoding = true;
        opts.head.ooo_time_window_ms = 3_600_000;
        opts.head.wal_segment_size = 32_768;
        opts.head.samples_per_chunk = 2;
        opts.head.chunk_range_ms = 3_600_000;

        let db = Tsdb::open_with_reload_interval(&rs_dir, opts, Duration::ZERO)
            .expect("Rust Tsdb::open write");
        let append_batch = |batch: &StBatchFixtureJson| {
            let mut app = db.appender();
            for s in &batch.series {
                let pairs: Vec<(&str, &str)> = s
                    .labels
                    .iter()
                    .map(|l| (l.name.as_str(), l.value.as_str()))
                    .collect();
                let mut r: Option<u64> = None;
                for sample in &s.samples {
                    r = Some(
                        app.append_float_with_st(r, &pairs, sample.st, sample.t, sample.v)
                            .expect("rust append_float_with_st"),
                    );
                }
                for h in &s.histograms {
                    if h.is_float {
                        r = Some(
                            app.append_float_histogram(
                                r,
                                &pairs,
                                h.st,
                                h.timestamp_ms,
                                to_rust_fhist(h),
                            )
                            .expect("rust append_float_histogram"),
                        );
                    } else {
                        r = Some(
                            app.append_histogram(
                                r,
                                &pairs,
                                h.st,
                                h.timestamp_ms,
                                to_rust_hist(h),
                            )
                            .expect("rust append_histogram"),
                        );
                    }
                }
            }
            app.commit().expect("rust commit");
        };

        for b in &pre_batches {
            append_batch(b);
        }
        let min_t = db.head().min_time();
        let ulid = db
            .compact_head_range(min_t, 5_000_000)
            .expect("rust compact_head_range");
        assert!(ulid.is_some(), "expected compacted ULID block from Rust");

        for b in &post_batches {
            append_batch(b);
        }
        db.close().expect("rust close");
    }

    let rs_blocks: Vec<String> = fs::read_dir(&rs_dir)
        .expect("readdir rs_dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !matches!(n.as_str(), "wal" | "wbl" | "chunks_head"))
        .collect();
    assert_all_st_artifacts_present("rust-written-fixture", &rs_dir, &rs_blocks);

    let go_reads_rs_on = tmp_rs.path().join("go_reads_rs_on");
    copy_dir_recursive(&rs_dir, &go_reads_rs_on);
    let go_from_rs_on: ReadStTsdbResponse = invoke_oracle(
        "read-st-tsdb",
        &ReadStTsdbRequest {
            dir: go_reads_rs_on.display().to_string(),
            enable_st_storage: true,
            float_chunk_encoding: "xor2".to_string(),
            enable_histogram_st_encoding: true,
            out_of_order_time_window_ms: 3_600_000,
            read_only: false,
        },
    )
    .expect("go read-st-tsdb on rust dir (st=true)");
    assert_series_fixtures_eq(
        "Rust->Go ST TSDB (enable_st_storage=true)",
        &filter_w28_series(go_from_rs_on.series),
        &expected_w28,
    );

    let go_reads_rs_off = tmp_rs.path().join("go_reads_rs_off");
    copy_dir_recursive(&rs_dir, &go_reads_rs_off);
    let go_from_rs_off: ReadStTsdbResponse = invoke_oracle(
        "read-st-tsdb",
        &ReadStTsdbRequest {
            dir: go_reads_rs_off.display().to_string(),
            enable_st_storage: false,
            float_chunk_encoding: "xor2".to_string(),
            enable_histogram_st_encoding: true,
            out_of_order_time_window_ms: 3_600_000,
            read_only: false,
        },
    )
    .expect("go read-st-tsdb on rust dir (st=false)");
    assert_series_fixtures_eq(
        "Rust->Go ST TSDB (enable_st_storage=false)",
        &filter_w28_series(go_from_rs_off.series),
        &expected_w28,
    );

    let rs_compacted = tmp_rs.path().join("rs_compacted");
    copy_dir_recursive(&rs_dir, &rs_compacted);
    {
        let mut opts = TsdbOptions {
            no_lockfile: true,
            ..Default::default()
        };
        opts.head.enable_st_storage = true;
        opts.head.float_chunk_encoding = Encoding::Xor2;
        opts.head.enable_histogram_st_encoding = true;
        opts.head.ooo_time_window_ms = 3_600_000;
        opts.head.chunk_range_ms = 3_600_000;
        let db = Tsdb::open_with_reload_interval(&rs_compacted, opts, Duration::ZERO)
            .expect("reopen rs_compacted");
        let ooo_ulids = db.compact_ooo_head().expect("compact_ooo_head");
        assert!(!ooo_ulids.is_empty(), "expected OOO compacted block(s)");
        let _ = db.compact_head_all().expect("compact_head_all");
        db.close().expect("close rs_compacted");
    }
    let go_ro: ReadStTsdbResponse = invoke_oracle(
        "read-st-tsdb",
        &ReadStTsdbRequest {
            dir: rs_compacted.display().to_string(),
            enable_st_storage: true,
            float_chunk_encoding: "xor2".to_string(),
            enable_histogram_st_encoding: true,
            out_of_order_time_window_ms: 3_600_000,
            read_only: true,
        },
    )
    .expect("go read_only on compacted rust ST blocks");
    assert!(
        go_ro.block_ulids.len() >= 2,
        "expected >= 2 ULID blocks, got {:?}",
        go_ro.block_ulids
    );
    assert_series_fixtures_eq(
        "Rust->Go OpenDBReadOnly compacted ST blocks",
        &filter_w28_series(go_ro.series),
        &expected_w28,
    );

    // --- Part C: Locks down C-1 (reopening Go ST TSDB with enable_st_storage=false) ---
    let rust_copy_off = tmp_go.path().join("rust_reopen_off");
    copy_dir_recursive(&go_dir, &rust_copy_off);
    let rust_got_off = rust_query_w28_series(&rust_copy_off, false, Encoding::Xor2, true);
    assert_series_fixtures_eq(
        "Go->Rust ST TSDB (enable_st_storage=false, C-1 regression)",
        &rust_got_off,
        &expected_w28_off,
    );
}

// -----------------------------------------------------------------------------
// Test 3: PropagationDelayTracker Formatting, Protobuf Native Histogram, & Live Gossip
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct PromSpan {
    #[allow(dead_code)]
    offset: i32,
    #[allow(dead_code)]
    length: u32,
}

#[derive(Clone, Debug, Default)]
struct PromHist {
    sample_count: u64,
    #[allow(dead_code)]
    sample_sum: f64,
    classic_buckets: usize,
    schema: Option<i32>,
    zero_threshold: Option<f64>,
    zero_count: Option<u64>,
    zero_count_float: Option<f64>,
    negative_span: Vec<PromSpan>,
    positive_span: Vec<PromSpan>,
}

#[derive(Clone, Debug, Default)]
struct PromMetric {
    counter_value: Option<f64>,
    histogram: Option<PromHist>,
}

#[derive(Clone, Debug, Default)]
struct PromMetricFamily {
    name: String,
    metrics: Vec<PromMetric>,
}

fn unzigzag32(v: u32) -> i32 {
    ((v >> 1) as i32) ^ (-((v & 1) as i32))
}

fn read_len_slice<'a>(buf: &mut &'a [u8]) -> &'a [u8] {
    let len = decode_varint(buf).expect("varint len") as usize;
    let s = &buf[..len];
    *buf = &buf[len..];
    s
}

fn decode_span(mut buf: &[u8]) -> PromSpan {
    let mut s = PromSpan::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).expect("span key");
        match (tag, wt) {
            (1, WireType::Varint) => {
                s.offset = unzigzag32(decode_varint(&mut buf).expect("span offset") as u32);
            }
            (2, WireType::Varint) => {
                s.length = decode_varint(&mut buf).expect("span len") as u32;
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).expect("skip span"),
        }
    }
    s
}

fn decode_counter(mut buf: &[u8]) -> f64 {
    let mut v = 0.0;
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).expect("counter key");
        match (tag, wt) {
            (1, WireType::SixtyFourBit) => {
                v = f64::from_le_bytes(buf[..8].try_into().expect("8 bytes"));
                buf.advance(8);
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).expect("skip counter"),
        }
    }
    v
}

fn decode_histogram(mut buf: &[u8]) -> PromHist {
    let mut h = PromHist::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).expect("hist key");
        match (tag, wt) {
            (1, WireType::Varint) => h.sample_count = decode_varint(&mut buf).expect("sc"),
            (2, WireType::SixtyFourBit) => {
                h.sample_sum = f64::from_le_bytes(buf[..8].try_into().expect("8 bytes"));
                buf.advance(8);
            }
            (3, WireType::LengthDelimited) => {
                let _ = read_len_slice(&mut buf);
                h.classic_buckets += 1;
            }
            (5, WireType::Varint) => {
                h.schema = Some(unzigzag32(decode_varint(&mut buf).expect("schema") as u32));
            }
            (6, WireType::SixtyFourBit) => {
                h.zero_threshold = Some(f64::from_le_bytes(buf[..8].try_into().expect("8 bytes")));
                buf.advance(8);
            }
            (7, WireType::Varint) => h.zero_count = Some(decode_varint(&mut buf).expect("zc")),
            (8, WireType::SixtyFourBit) => {
                h.zero_count_float = Some(f64::from_le_bytes(buf[..8].try_into().expect("8 bytes")));
                buf.advance(8);
            }
            (9, WireType::LengthDelimited) => {
                h.negative_span.push(decode_span(read_len_slice(&mut buf)));
            }
            (12, WireType::LengthDelimited) => {
                h.positive_span.push(decode_span(read_len_slice(&mut buf)));
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).expect("skip hist"),
        }
    }
    h
}

fn decode_metric(mut buf: &[u8]) -> PromMetric {
    let mut m = PromMetric::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).expect("metric key");
        match (tag, wt) {
            (3, WireType::LengthDelimited) => {
                m.counter_value = Some(decode_counter(read_len_slice(&mut buf)));
            }
            (7, WireType::LengthDelimited) => {
                m.histogram = Some(decode_histogram(read_len_slice(&mut buf)));
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).expect("skip metric"),
        }
    }
    m
}

fn decode_metric_family(mut buf: &[u8]) -> PromMetricFamily {
    let mut mf = PromMetricFamily::default();
    while buf.has_remaining() {
        let (tag, wt) = decode_key(&mut buf).expect("mf key");
        match (tag, wt) {
            (1, WireType::LengthDelimited) => {
                mf.name = String::from_utf8_lossy(read_len_slice(&mut buf)).into_owned();
            }
            (4, WireType::LengthDelimited) => {
                mf.metrics.push(decode_metric(read_len_slice(&mut buf)));
            }
            _ => skip_field(wt, tag, &mut buf, DecodeContext::default()).expect("skip mf"),
        }
    }
    mf
}

fn decode_delimited_families(mut raw: &[u8]) -> Vec<PromMetricFamily> {
    let mut out = Vec::new();
    while raw.has_remaining() {
        let len = decode_varint(&mut raw).expect("delim len") as usize;
        let msg = &raw[..len];
        raw.advance(len);
        out.push(decode_metric_family(msg));
    }
    out
}

async fn scrape_proto_metrics(http: &reqwest::Client, port: u16) -> Vec<PromMetricFamily> {
    let resp = http
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .header(
            "Accept",
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited",
        )
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("scrape proto /metrics");
    assert_eq!(resp.status(), 200, "GET /metrics status");
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        ct.starts_with("application/vnd.google.protobuf"),
        "expected protobuf content-type, got {ct:?}"
    );
    let bytes = resp.bytes().await.expect("proto bytes");
    decode_delimited_families(&bytes)
}

fn proto_counter_sum(families: &[PromMetricFamily], name: &str) -> Option<f64> {
    let mf = families.iter().find(|f| f.name == name)?;
    Some(
        mf.metrics
            .iter()
            .filter_map(|m| m.counter_value)
            .sum::<f64>(),
    )
}

#[test]
fn w28_propagation_delay_tracker_format_metrics_and_live_gossip_parity() {
    ensure_go_oracle_built().expect("go-oracle");

    // --- Part A: Byte-for-byte go_string() and to_go_json() parity (locks down C-5) ---
    let codec = propagation_delay_tracker_codec();
    let empty = PropagationDelayTrackerDesc::default();

    let mut single = PropagationDelayTrackerDesc::default();
    single.beacons.insert(
        2,
        BeaconDesc {
            published_by: "node-a".into(),
            published_at: 1000,
            deleted_at: 0,
        },
    );

    let mut multi = single.clone();
    multi.beacons.insert(
        10,
        BeaconDesc {
            published_by: "node-b<&>\"ü".into(),
            published_at: 2000,
            deleted_at: 3000,
        },
    );
    multi.beacons.insert(
        0xdead_beef,
        BeaconDesc {
            published_by: "node-c".into(),
            published_at: 4000,
            deleted_at: 0,
        },
    );

    for (case_name, desc) in [("empty", &empty), ("single", &single), ("multi", &multi)] {
        let encoded = codec.encode_mergeable(desc).expect("encode_mergeable");
        let val = codec.decode_mergeable(&encoded).expect("decode_mergeable");

        let beacons_in: BTreeMap<String, BeaconInputJson> = desc
            .beacons
            .iter()
            .map(|(k, b)| {
                (
                    k.to_string(),
                    BeaconInputJson {
                        published_by: b.published_by.clone(),
                        published_at: b.published_at,
                        deleted_at: b.deleted_at,
                    },
                )
            })
            .collect();
        let go: FormatPropTrackerResponse = invoke_oracle(
            "format-propagation-tracker",
            &FormatPropTrackerRequest {
                value_b64: B64.encode(&encoded),
                beacons: beacons_in,
            },
        )
        .expect("go format-propagation-tracker");

        assert_eq!(val.go_string(), go.go_string, "{case_name}: go_string()");
        assert_eq!(
            val.to_go_json().encode().trim_end_matches('\n'),
            go.json,
            "{case_name}: json.Marshal(desc)"
        );
        assert_eq!(
            val.to_go_json().encode_indent("  ").trim_end_matches('\n'),
            go.json_indent,
            "{case_name}: json.MarshalIndent(desc, \"\", \"  \")"
        );
        assert_eq!(
            val.to_go_json().encode(),
            go.json_encode,
            "{case_name}: json.NewEncoder.Encode(desc)"
        );
        assert_eq!(
            val.to_go_json().encode_indent("    "),
            go.json_encode_indent,
            "{case_name}: json.NewEncoder.Encode(desc) 4-space indent"
        );
    }

    // --- Part B: Live Go + Rust memberlist cluster with PropagationDelayTracker (C-3, C-4, C-5) ---
    block_on_for_tests(async {
        let tmp = tempfile::tempdir().expect("tempdir");
        let http = reqwest::Client::new();

        let (go_http, go_grpc, go_ml) = (free_test_port(), free_test_port(), free_test_port());
        let (rs_http, rs_grpc, rs_ml) = (free_test_port(), free_test_port(), free_test_port());
        let (dis_http, dis_grpc, dis_ml) = (free_test_port(), free_test_port(), free_test_port());

        let make_yaml = |name: &str,
                         http_p: u16,
                         grpc_p: u16,
                         ml_p: u16,
                         joins: &[u16],
                         dir: &Path| {
            let join_yaml: String = joins
                .iter()
                .map(|p| format!("    - 127.0.0.1:{p}\n"))
                .collect();
            let bucket = dir.join("bucket");
            let tsdb = dir.join("tsdb");
            fs::create_dir_all(&bucket).unwrap();
            fs::create_dir_all(&tsdb).unwrap();
            format!(
                r#"target: ingester
server:
  http_listen_address: 127.0.0.1
  http_listen_port: {http_p}
  grpc_listen_address: 127.0.0.1
  grpc_listen_port: {grpc_p}
multitenancy_enabled: true
memberlist:
  node_name: {name}
  randomize_node_name: false
  bind_addr: [127.0.0.1]
  bind_port: {ml_p}
  advertise_addr: 127.0.0.1
  advertise_port: {ml_p}
  gossip_interval: 100ms
  rejoin_interval: 1s
  abort_if_cluster_join_fails: false
  join_members:
{join_yaml}ingester:
  ring:
    kvstore:
      store: memberlist
    instance_id: {name}
    instance_addr: 127.0.0.1
    num_tokens: 16
    min_ready_duration: 0s
    replication_factor: 1
blocks_storage:
  backend: filesystem
  filesystem:
    dir: {bucket}
  tsdb:
    dir: {tsdb}
    ship_interval: 0s
activity_tracker:
  filepath: ""
"#,
                bucket = bucket.display(),
                tsdb = tsdb.display(),
            )
        };

        let enabled_flags = vec![
            "-memberlist.propagation-delay-tracker.enabled=true".to_string(),
            "-memberlist.propagation-delay-tracker.beacon-interval=200ms".to_string(),
            "-memberlist.propagation-delay-tracker.beacon-lifetime=10s".to_string(),
            "-memberlist.propagation-delay-tracker.log-beacons-latency-longer-than=1ms".to_string(),
        ];
        let disabled_flags = vec![
            "-memberlist.propagation-delay-tracker.enabled=false".to_string(),
        ];

        // Start rust-tracker first (which also builds the release binary before Go starts) so
        // Go's initial fast-join connects immediately instead of entering exponential backoff.
        let go_dir = tmp.path().join("go-enabled");
        let rs_dir = tmp.path().join("rs-enabled");
        let dis_dir = tmp.path().join("rs-disabled");

        let mut rs_proc = start_mimir(
            Impl::Rust,
            "pt-rs",
            &rs_dir,
            &make_yaml("rust-tracker", rs_http, rs_grpc, rs_ml, &[go_ml], &rs_dir),
            &enabled_flags,
        );
        wait_ready(&http, &mut rs_proc, rs_http, Duration::from_secs(30)).await;

        let mut go_proc = start_mimir(
            Impl::Go,
            "pt-go",
            &go_dir,
            &make_yaml("go-tracker", go_http, go_grpc, go_ml, &[rs_ml], &go_dir),
            &enabled_flags,
        );
        let mut dis_proc = start_mimir(
            Impl::Rust,
            "pt-dis",
            &dis_dir,
            &make_yaml("rust-disabled", dis_http, dis_grpc, dis_ml, &[go_ml, rs_ml], &dis_dir),
            &disabled_flags,
        );

        wait_ready(&http, &mut go_proc, go_http, Duration::from_secs(30)).await;
        wait_ready(&http, &mut dis_proc, dis_http, Duration::from_secs(30)).await;

        let deadline = Instant::now() + Duration::from_secs(20);
        let mut converged = false;
        while Instant::now() < deadline {
            let (go_m, rs_m, dis_m) = tokio::join!(
                scrape_proto_metrics(&http, go_http),
                scrape_proto_metrics(&http, rs_http),
                scrape_proto_metrics(&http, dis_http),
            );
            let go_pub = proto_counter_sum(&go_m, "memberlist_propagation_tracker_beacons_published_total").unwrap_or(0.0);
            let go_rcv = proto_counter_sum(&go_m, "memberlist_propagation_tracker_beacons_received_total").unwrap_or(0.0);
            let rs_pub = proto_counter_sum(&rs_m, "memberlist_propagation_tracker_beacons_published_total").unwrap_or(0.0);
            let rs_rcv = proto_counter_sum(&rs_m, "memberlist_propagation_tracker_beacons_received_total").unwrap_or(0.0);
            let dis_inv = proto_counter_sum(&dis_m, "memberlist_client_received_broadcasts_invalid_total").unwrap_or(0.0);

            if go_pub > 0.0 && go_rcv > 0.0 && rs_pub > 0.0 && rs_rcv > 0.0 && dis_inv > 0.0 {
                let (_, _, go_json_body) = http_get(
                    &http,
                    go_http,
                    "/memberlist?viewKey=memberlist-propagation-delay-tracker&format=json",
                    false,
                )
                .await
                .unwrap_or_default();
                let (_, _, rs_json_body) = http_get(
                    &http,
                    rs_http,
                    "/memberlist?viewKey=memberlist-propagation-delay-tracker&format=json",
                    false,
                )
                .await
                .unwrap_or_default();
                if go_json_body.contains("go-tracker")
                    && go_json_body.contains("rust-tracker")
                    && rs_json_body.contains("go-tracker")
                    && rs_json_body.contains("rust-tracker")
                {
                    converged = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(
            converged,
            "propagation delay tracker did not converge across Go & Rust within 20s.\nGO LOG:\n{}\nRUST LOG:\n{}\nDISABLED LOG:\n{}",
            go_proc.log(),
            rs_proc.log(),
            dis_proc.log()
        );

        // Verify C-4: protobuf /metrics on both Go and Rust have native + classic histogram buckets
        for (side, port) in [("go", go_http), ("rust", rs_http)] {
            let mfs = scrape_proto_metrics(&http, port).await;
            let pub_cnt = proto_counter_sum(
                &mfs,
                "memberlist_propagation_tracker_beacons_published_total",
            )
            .unwrap_or(0.0);
            let rcv_cnt = proto_counter_sum(
                &mfs,
                "memberlist_propagation_tracker_beacons_received_total",
            )
            .unwrap_or(0.0);
            assert!(pub_cnt > 0.0, "{side}: beacons_published_total must be > 0");
            assert!(rcv_cnt > 0.0, "{side}: beacons_received_total must be > 0");

            let delay_mf = mfs
                .iter()
                .find(|f| f.name == "memberlist_propagation_tracker_delay_seconds")
                .unwrap_or_else(|| panic!("{side}: missing memberlist_propagation_tracker_delay_seconds"));
            let delay_h = delay_mf
                .metrics
                .first()
                .and_then(|m| m.histogram.as_ref())
                .unwrap_or_else(|| panic!("{side}: delay_seconds missing histogram payload"));
            assert!(delay_h.sample_count > 0, "{side}: delay_seconds sample_count > 0");
            assert!(
                delay_h.classic_buckets > 0,
                "{side}: delay_seconds missing classic buckets"
            );
            assert!(
                delay_h.schema.is_some() && delay_h.zero_threshold.is_some(),
                "{side}: delay_seconds missing native histogram fields (C-4): {delay_h:?}"
            );
            assert!(
                !delay_h.positive_span.is_empty()
                    || delay_h.zero_count.is_some()
                    || delay_h.zero_count_float.is_some(),
                "{side}: delay_seconds missing native histogram spans/zero_count (C-4): {delay_h:?}"
            );
        }

        // Verify C-5: GET /memberlist?viewKey=memberlist-propagation-delay-tracker on both
        for (side, port) in [("go", go_http), ("rust", rs_http)] {
            let (code_struct, _, body_struct) = http_get(
                &http,
                port,
                "/memberlist?viewKey=memberlist-propagation-delay-tracker",
                false,
            )
            .await
            .expect("GET viewKey struct");
            assert_eq!(code_struct, 200, "{side}: viewKey struct status");
            assert!(
                body_struct.starts_with("&memberlist.PropagationDelayTrackerDesc{Beacons: map[uint64]BeaconDesc{0x"),
                "{side}: unexpected struct viewKey format: {body_struct}"
            );
            assert!(
                body_struct.contains("memberlist.BeaconDesc{PublishedBy:"),
                "{side}: missing BeaconDesc field order: {body_struct}"
            );

            let (code_json, _, body_json) = http_get(
                &http,
                port,
                "/memberlist?viewKey=memberlist-propagation-delay-tracker&format=json",
                false,
            )
            .await
            .expect("GET viewKey json");
            assert_eq!(code_json, 200, "{side}: viewKey json status");
            let parsed: Value = serde_json::from_str(&body_json)
                .unwrap_or_else(|e| panic!("{side}: invalid JSON from viewKey: {e}: {body_json}"));
            assert!(
                parsed["beacons"].is_object(),
                "{side}: expected beacons object: {body_json}"
            );
        }

        // Verify C-3: disabled node returns 404 on viewKey and increments invalid broadcast counter
        let (dis_code, _, dis_body) = http_get(
            &http,
            dis_http,
            "/memberlist?viewKey=memberlist-propagation-delay-tracker",
            false,
        )
        .await
        .expect("GET disabled viewKey");
        assert_eq!(
            dis_code, 404,
            "disabled node must return 404 for propagation tracker viewKey (C-3), got {dis_code}: {dis_body}"
        );
        let dis_mfs = scrape_proto_metrics(&http, dis_http).await;
        let dis_invalid = proto_counter_sum(
            &dis_mfs,
            "memberlist_client_received_broadcasts_invalid_total",
        )
        .unwrap_or(0.0);
        assert!(
            dis_invalid > 0.0,
            "disabled node must increment memberlist_client_received_broadcasts_invalid_total (C-3), got {dis_invalid}"
        );
    });
}
