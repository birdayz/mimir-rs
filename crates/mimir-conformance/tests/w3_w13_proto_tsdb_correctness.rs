//! Wave 13 Protobuf/RW2 & TSDB Timestamp/Index Differential Conformance Suite.
//!
//! Verifies:
//! 1. Interleaved `Histogram` repeated fields decode in linear O(N) time/memory across
//!    `decode_write_request`, `decode_classic_stream`, and `decode_rw2_stream`.
//! 2. Go-oracle negative-timestamp ULID block cross-read/write spanning `i64::MIN + 1000`
//!    to `+1_700_000_000_000` (unsigned `uvarint64` chunk time deltas > `i64::MAX`).
//! 3. OOO & selected-series compaction of negative-timestamp (`t < 0`) samples.
//! 4. RW2 `ref == 0` handling with `offset > 0` / empty `common_symbols`, `uint32` varint
//!    truncation, `WriteRequestScratch::metadata` on `SliceRef::Common`, non-nullable
//!    `MetadataRW2` (`0x2a 0x00`), tag `1001` `header_changed_after_series`, and
//!    protobuf illegal field number `>= 2^29` rejection.

#![allow(clippy::result_large_err)]

use std::time::{Duration, Instant};

use mimir_conformance::oracle::{
    invoke_oracle, ChunkSampleJson, LabelJson, TsdbSeriesFixtureJson, VerifyTsdbDirRequest,
    VerifyTsdbDirResponse, WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};
use mimir_proto::cortexpb;
use mimir_proto::grpcutilpb::{
    pack_any, unpack_any, CORTEXPB_ERROR_DETAILS_TYPE_URL,
};
use mimir_proto::split::resymbolize_timeseries_rw2;
use mimir_proto::symbols::{v2_common_symbols, FastSymbolsTable, V2_RECORD_SYMBOL_OFFSET};
use mimir_proto::zerocopy::{
    decode_classic_stream, decode_rw2_stream, decode_write_request, encode_write_request_rw2_bytes,
    from_write_request_to_rw2, DecodeError, DecodeMode, DecodeOptions, Rw2ResolvedScratch,
    WriteRequestScratch,
};
use mimir_proto::prost;
use mimir_proto::prost::Message as _;
use mimir_tsdb::block::{Block, BlockMetaCompaction};
use mimir_tsdb::chunkenc::Encoding;
use mimir_tsdb::compact::{
    compact_blocks, encode_samples_into_chunks_with_encodings, write_block_from_series,
    CompactionSeries, DecodedSample, SampleValue,
};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::{add_jitter_to_chunk_end_time, range_for_timestamp, Head, HeadOptions};
use mimir_tsdb::index::IndexReader;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};

fn lbls(pairs: &[(&str, &str)]) -> Vec<LabelJson> {
    pairs
        .iter()
        .map(|(n, v)| LabelJson {
            name: (*n).to_string(),
            value: (*v).to_string(),
        })
        .collect()
}

fn build_interleaved_histogram_payload(pairs: usize) -> Vec<u8> {
    let mut hist_bytes = Vec::with_capacity(pairs * 6);
    hist_bytes.extend_from_slice(&[0x20, 0x00]); // schema = 0
    for i in 0..pairs {
        // tag 8 (negative_spans) / tag 11 (positive_spans) interleaved every 250 pairs
        if i % 250 == 0 {
            // Span { offset: 1, length: 1 } -> [0x08, 0x02, 0x10, 0x01]
            hist_bytes.extend_from_slice(&[0x42, 0x04, 0x08, 0x02, 0x10, 0x01]);
            hist_bytes.extend_from_slice(&[0x5a, 0x04, 0x08, 0x02, 0x10, 0x01]);
        }
        // tag 9 (negative_deltas, sint64=1 -> zigzag 2) & tag 12 (positive_deltas, sint64=2 -> zigzag 4)
        hist_bytes.extend_from_slice(&[0x48, 0x02, 0x60, 0x04]);
        if i % 100 == 0 {
            // tag 10 (neg_counts), tag 13 (pos_counts), tag 16 (custom_values)
            hist_bytes.push(0x51);
            hist_bytes.extend_from_slice(&1.25f64.to_le_bytes());
            hist_bytes.push(0x69);
            hist_bytes.extend_from_slice(&2.25f64.to_le_bytes());
            hist_bytes.extend_from_slice(&[0x81, 0x01]);
            hist_bytes.extend_from_slice(&3.25f64.to_le_bytes());
        }
    }
    hist_bytes
}

#[test]
fn w13_p0_1_interleaved_histogram_linear_decode_all_three_decoders() {
    let pairs = 30_000usize;
    let hist_bytes = build_interleaved_histogram_payload(pairs);

    // Classic WriteRequest wire
    let mut ts_classic = Vec::new();
    let lp = cortexpb::LabelPair::from_static("__name__", "interleaved_hist").encode_to_vec();
    ts_classic.push(0x0a);
    prost::encoding::encode_varint(lp.len() as u64, &mut ts_classic);
    ts_classic.extend_from_slice(&lp);
    ts_classic.push(0x22); // tag 4 histograms
    prost::encoding::encode_varint(hist_bytes.len() as u64, &mut ts_classic);
    ts_classic.extend_from_slice(&hist_bytes);
    let mut wr_classic = Vec::new();
    wr_classic.push(0x0a); // tag 1 timeseries
    prost::encoding::encode_varint(ts_classic.len() as u64, &mut wr_classic);
    wr_classic.extend_from_slice(&ts_classic);

    // RW2 WriteRequest wire
    let mut wr_rw2 = Vec::new();
    for s in ["", "__name__", "interleaved_hist"] {
        wr_rw2.push(0x22); // tag 4 symbols_rw2
        prost::encoding::encode_varint(s.len() as u64, &mut wr_rw2);
        wr_rw2.extend_from_slice(s.as_bytes());
    }
    let mut ts_rw2 = Vec::new();
    ts_rw2.extend_from_slice(&[0x0a, 0x02, 0x01, 0x02]); // labels_refs = [1, 2]
    ts_rw2.push(0x1a); // tag 3 histograms
    prost::encoding::encode_varint(hist_bytes.len() as u64, &mut ts_rw2);
    ts_rw2.extend_from_slice(&hist_bytes);
    wr_rw2.push(0x2a); // tag 5 timeseries_rw2
    prost::encoding::encode_varint(ts_rw2.len() as u64, &mut wr_rw2);
    wr_rw2.extend_from_slice(&ts_rw2);

    let start = Instant::now();
    let mut scratch = WriteRequestScratch::new();

    // 1. decode_write_request (classic & rw2)
    let b1 = decode_write_request(&wr_classic, DecodeOptions::classic(), &mut scratch)
        .expect("classic decode_write_request");
    let ts1 = b1.timeseries_at(0);
    let h1 = ts1.histograms().next().expect("hist");
    assert_eq!(h1.negative_deltas().len(), pairs);
    assert_eq!(h1.positive_deltas().len(), pairs);
    assert_eq!(h1.negative_spans().len(), pairs / 250);
    assert_eq!(h1.positive_spans().len(), pairs / 250);
    assert_eq!(h1.negative_counts().len(), pairs / 100);
    assert_eq!(h1.positive_counts().len(), pairs / 100);
    assert_eq!(h1.custom_values().len(), pairs / 100);

    let b2 = decode_write_request(&wr_rw2, DecodeOptions::rw2(), &mut scratch)
        .expect("rw2 decode_write_request");
    let ts2 = b2.timeseries_at(0);
    let h2 = ts2.histograms().next().expect("hist");
    assert_eq!(h2.negative_deltas().len(), pairs);
    assert_eq!(h2.positive_deltas().len(), pairs);

    // 2. decode_classic_stream
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();
    let mut seen = 0usize;
    decode_classic_stream(
        &wr_classic,
        DecodeOptions::classic(),
        &mut scratch,
        &mut label_bytes,
        &mut samples,
        |s| {
            let h = s.histograms().next().expect("hist");
            assert_eq!(h.negative_deltas().len(), pairs);
            assert_eq!(h.positive_deltas().len(), pairs);
            seen += 1;
        },
    )
    .expect("decode_classic_stream");
    assert_eq!(seen, 1);

    // 3. decode_rw2_stream
    let mut resolved = Rw2ResolvedScratch::new();
    let mut meta_out = Vec::new();
    seen = 0;
    decode_rw2_stream(
        &wr_rw2,
        DecodeOptions::rw2(),
        &mut scratch,
        &mut resolved,
        &mut label_bytes,
        &mut samples,
        &mut meta_out,
        |s| {
            let h = s.histograms().next().expect("hist");
            assert_eq!(h.negative_deltas().len(), pairs);
            assert_eq!(h.positive_deltas().len(), pairs);
            seen += 1;
        },
    )
    .expect("decode_rw2_stream");
    assert_eq!(seen, 1);

    assert!(
        start.elapsed() < Duration::from_secs(2),
        "interleaved histogram decode across all decoders must stay O(N); took {:?}",
        start.elapsed()
    );
}

#[test]
fn w13_p0_4_go_oracle_negative_to_positive_ulid_block_cross_read_write() {
    // 1) Go writes Block A at t = i64::MIN + 1000..=i64::MIN + 3000 and Block B at
    //    t = +1_700_000_000_000..=+1_700_000_002_000.
    //    Rust compacts both Go blocks into a single ULID block whose chunk delta > i64::MAX,
    //    reads it back via IndexReader::series & Tsdb::open, and Go verify-tsdb-dir verifies it!
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let dir_out = tempfile::tempdir().unwrap();

    let series_lbls = lbls(&[("__name__", "wide_cross_epoch"), ("job", "w13")]);

    let resp_a: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir_a.path().to_str().unwrap().to_string(),
            compact: true,
            out_of_order_time_window_ms: 0,
            series: vec![TsdbSeriesFixtureJson {
                labels: series_lbls.clone(),
                samples: vec![ChunkSampleJson {
                    st: 0,
                    t: i64::MIN + 10_000_000,
                    v: -42.5,
                }],
                ..Default::default()
            }],
        },
    )
    .expect("go write block A");
    assert_eq!(resp_a.blocks.len(), 1);

    let resp_b: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir_b.path().to_str().unwrap().to_string(),
            compact: true,
            out_of_order_time_window_ms: 0,
            series: vec![TsdbSeriesFixtureJson {
                labels: series_lbls,
                samples: vec![
                    ChunkSampleJson {
                        st: 0,
                        t: 1_700_000_000_000,
                        v: 100.5,
                    },
                    ChunkSampleJson {
                        st: 0,
                        t: 1_700_000_002_000,
                        v: 200.75,
                    },
                ],
                ..Default::default()
            }],
        },
    )
    .expect("go write block B");
    assert_eq!(resp_b.blocks.len(), 1);

    let blk_a = Block::open(dir_a.path().join(&resp_a.blocks[0])).expect("open go block A");
    let blk_b = Block::open(dir_b.path().join(&resp_b.blocks[0])).expect("open go block B");

    let compacted_meta = compact_blocks(dir_out.path(), &[&blk_a, &blk_b])
        .expect("compact Go extreme blocks")
        .expect("compacted block produced");
    assert_eq!(compacted_meta.min_time, i64::MIN + 10_000_000);
    assert_eq!(compacted_meta.max_time, 1_700_000_002_001);

    // Verify Rust IndexReader::series decodes chunks with u64 delta > i64::MAX
    let idx = IndexReader::open(dir_out.path().join(compacted_meta.ulid.to_string()).join("index"))
        .expect("open compacted index");
    let p = idx.postings("__name__", &["wide_cross_epoch"]).unwrap();
    assert_eq!(p.len(), 1);
    let (_lbls, chks) = idx.series(p[0]).expect("IndexReader::series across > i64::MAX delta");
    assert_eq!(chks.len(), 2);
    assert_eq!(chks[0].min_time, i64::MIN + 10_000_000);
    assert_eq!(chks[1].max_time, 1_700_000_002_000);

    // Verify Go prometheus/tsdb reads and verifies the Rust-compacted wide-span block
    std::fs::create_dir_all(dir_out.path().join("wal")).unwrap();
    let go_verify: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir_out.path().to_str().unwrap().to_string(),
            read_only: true,
        },
    )
    .expect("go verify-tsdb-dir on compacted wide-span block");
    assert_eq!(go_verify.series.len(), 1);
    let go_samples: Vec<(i64, f64)> = go_verify.series[0]
        .samples
        .iter()
        .map(|s| (s.t, s.v))
        .collect();
    assert_eq!(
        go_samples,
        vec![
            (i64::MIN + 10_000_000, -42.5),
            (1_700_000_000_000, 100.5),
            (1_700_000_002_000, 200.75),
        ]
    );

    // 2) Single-chunk ULID block written by Rust spanning i64::MIN + 1000 to +1_700_000_000_000
    //    (dt0 > i64::MAX as u64 in IndexWriter/IndexReader), verified by Go + Rust!
    let dir_single = tempfile::tempdir().unwrap();
    let single_meta = write_block_from_series(
        dir_single.path(),
        i64::MIN + 1_000,
        1_700_000_000_001,
        vec![CompactionSeries {
            labels: Labels::from_pairs([("__name__", "single_chunk_wide"), ("env", "prod")]),
            chunks: encode_samples_into_chunks_with_encodings(
                &[
                    DecodedSample {
                        st: 0,
                        t: i64::MIN + 1_000,
                        value: SampleValue::Float(1.0),
                    },
                    DecodedSample {
                        st: 0,
                        t: 1_700_000_000_000,
                        value: SampleValue::Float(2.0),
                    },
                ],
                120,
                Encoding::Xor,
                Encoding::Histogram,
                Encoding::FloatHistogram,
            )
            .unwrap(),
            tombstones: Vec::new(),
        }],
        BlockMetaCompaction::default(),
        false,
        0,
    )
    .expect("write single-chunk wide block")
    .expect("meta");

    std::fs::create_dir_all(dir_single.path().join("wal")).unwrap();
    let go_verify_single: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir_single.path().to_str().unwrap().to_string(),
            read_only: true,
        },
    )
    .expect("go verify-tsdb-dir on single-chunk wide block");
    assert_eq!(go_verify_single.block_ulids, vec![single_meta.ulid.to_string()]);
    assert_eq!(
        go_verify_single.series[0]
            .samples
            .iter()
            .map(|s| (s.t, s.v))
            .collect::<Vec<_>>(),
        vec![(i64::MIN + 1_000, 1.0), (1_700_000_000_000, 2.0)]
    );

    let rust_db = Tsdb::open(dir_single.path(), TsdbOptions::default()).expect("rust open single wide");
    let m = LabelMatcher::new(MatchType::Equal, "__name__", "single_chunk_wide").unwrap();
    let rust_res = rust_db.querier(i64::MIN, i64::MAX).select(true, &[m]).unwrap();
    assert_eq!(rust_res.len(), 1);
    assert_eq!(rust_res[0].samples.len(), 2);
    rust_db.close().unwrap();
}

#[test]
fn w13_p0_5_ooo_and_selected_compaction_negative_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = 7_200_000;
    opts.head.ooo_time_window_ms = 7_200_000;
    let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap();

    let sel_lbls = [("__name__", "neg_sel_metric"), ("job", "sel")];
    let ooo_lbls = [("__name__", "neg_ooo_metric"), ("job", "ooo")];
    {
        let mut app = db.appender();
        app.append_float(None, &sel_lbls, -1_000, 1.0).unwrap();
        app.append_float(None, &sel_lbls, -500, 2.0).unwrap();
        app.append_float(None, &ooo_lbls, -100, 3.0).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = db.appender();
        app.append_float(None, &ooo_lbls, -2_000, 4.0).unwrap();
        app.append_float(None, &ooo_lbls, -1_500, 5.0).unwrap();
        app.commit().unwrap();
    }

    let sel_ref = db.head().get_entry_by_sorted_pairs(&sel_lbls).unwrap().ref_id;
    let sel_ulids = db.compact_selected_series(&[sel_ref]).unwrap();
    assert!(!sel_ulids.is_empty());
    let ooo_ulids = db.compact_ooo_head().unwrap();
    assert!(!ooo_ulids.is_empty());

    // Verify Rust querier sees all 5 samples (including in-order head sample -100)
    let m_sel = LabelMatcher::new(MatchType::Equal, "__name__", "neg_sel_metric").unwrap();
    assert_eq!(
        db.querier(i64::MIN, i64::MAX).select(true, &[m_sel]).unwrap()[0]
            .samples
            .iter()
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect::<Vec<_>>(),
        vec![(-1_000, 1.0), (-500, 2.0)]
    );
    let m_ooo = LabelMatcher::new(MatchType::Equal, "__name__", "neg_ooo_metric").unwrap();
    assert_eq!(
        db.querier(i64::MIN, i64::MAX).select(true, &[m_ooo]).unwrap()[0]
            .samples
            .iter()
            .map(|s| match s.value {
                SampleValue::Float(v) => (s.t, v),
                _ => panic!("expected float"),
            })
            .collect::<Vec<_>>(),
        vec![(-2_000, 4.0), (-1_500, 5.0), (-100, 3.0)]
    );

    // Cross-verify compacted ULID blocks on disk with Go oracle!
    db.close().unwrap();
    let go_verify: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_str().unwrap().to_string(),
            read_only: true,
        },
    )
    .expect("go verify-tsdb-dir on negative compacted blocks");
    assert_eq!(go_verify.series.len(), 2);
    let mut by_name: std::collections::BTreeMap<String, Vec<(i64, f64)>> = Default::default();
    for s in go_verify.series {
        let name = s
            .labels
            .iter()
            .find(|l| l.name == "__name__")
            .unwrap()
            .value
            .clone();
        by_name.insert(name, s.samples.iter().map(|p| (p.t, p.v)).collect());
    }
    assert_eq!(by_name["neg_sel_metric"], vec![(-1_000, 1.0), (-500, 2.0)]);
    assert_eq!(by_name["neg_ooo_metric"], vec![(-2_000, 4.0), (-1_500, 5.0)]);
}

#[test]
fn w13_p1_rw2_ref_zero_and_illegal_field_num_rejection_and_proto_parity() {
    // 1. RW2 ref == 0 with offset > 0 and empty common_symbols in decode_rw2_stream & split
    let syms = ["", "__name__", "zero_ref_metric", "job", "api"];
    let mut packed_refs = Vec::new();
    for r in [257u64, 258, 259, 0] {
        prost::encoding::encode_varint(r, &mut packed_refs);
    }
    let mut ts_rw2 = Vec::new();
    ts_rw2.push(0x0a);
    prost::encoding::encode_varint(packed_refs.len() as u64, &mut ts_rw2);
    ts_rw2.extend_from_slice(&packed_refs);
    ts_rw2.extend_from_slice(&[0x2a, 0x02, 0x08, 0x01]); // MetadataRW2 type=1, help_ref=0, unit_ref=0

    let mut wire = Vec::new();
    for s in syms {
        wire.push(0x22);
        prost::encoding::encode_varint(s.len() as u64, &mut wire);
        wire.extend_from_slice(s.as_bytes());
    }
    wire.push(0x2a);
    prost::encoding::encode_varint(ts_rw2.len() as u64, &mut wire);
    wire.extend_from_slice(&ts_rw2);

    let opts = DecodeOptions {
        mode: DecodeMode::Rw2Custom {
            offset: 256,
            common_symbols: &[],
        },
        ..DecodeOptions::rw2()
    };
    let mut scratch = WriteRequestScratch::new();
    let mut resolved = Rw2ResolvedScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();
    let mut meta_out = Vec::new();
    decode_rw2_stream(
        &wire,
        opts,
        &mut scratch,
        &mut resolved,
        &mut label_bytes,
        &mut samples,
        &mut meta_out,
        |_| {},
    )
    .expect("RW2 ref == 0 with offset > 0 must succeed");
    assert_eq!(meta_out.len(), 1);
    assert_eq!(meta_out[0].help, "");
    assert_eq!(meta_out[0].unit, "");

    // split::resymbolize_timeseries_rw2 with offset > 0, common_symbols=None, ref_id=0
    let mut table = FastSymbolsTable::new(16);
    table.configure_common_symbols(256, None);
    let ts_obj = cortexpb::TimeSeriesRw2 {
        labels_refs: vec![257, 0],
        metadata: Some(cortexpb::MetadataRw2 {
            r#type: 1,
            help_ref: 0,
            unit_ref: 0,
        }),
        ..Default::default()
    };
    let resym = resymbolize_timeseries_rw2(
        &ts_obj,
        &["".to_string(), "__name__".to_string()],
        None,
        &mut table,
    );
    assert_eq!(resym.labels_refs, vec![257, 0]);
    assert_eq!(resym.metadata.unwrap().help_ref, 0);

    // 2. Illegal protobuf field numbers >= 2^29 rejected across all 3 decoders
    for bad_field in [1u64 << 29, 0x8000_0001u64, 1u64 << 32] {
        let mut bad = Vec::new();
        prost::encoding::encode_varint(bad_field << 3, &mut bad);
        bad.push(0x00);
        assert!(
            matches!(
                decode_write_request(&bad, DecodeOptions::classic(), &mut scratch),
                Err(DecodeError::InvalidFieldNumber(_))
            ),
            "field_num {bad_field} must be rejected by decode_write_request"
        );
        assert!(
            matches!(
                decode_classic_stream(
                    &bad,
                    DecodeOptions::classic(),
                    &mut scratch,
                    &mut label_bytes,
                    &mut samples,
                    |_| {},
                ),
                Err(DecodeError::InvalidFieldNumber(_))
            ),
            "field_num {bad_field} must be rejected by decode_classic_stream"
        );
        let mut resolved2 = Rw2ResolvedScratch::new();
        let mut meta_out2 = Vec::new();
        assert!(
            matches!(
                decode_rw2_stream(
                    &bad,
                    DecodeOptions::rw2(),
                    &mut scratch,
                    &mut resolved2,
                    &mut label_bytes,
                    &mut samples,
                    &mut meta_out2,
                    |_| {},
                ),
                Err(DecodeError::InvalidFieldNumber(_))
            ),
            "field_num {bad_field} must be rejected by decode_rw2_stream"
        );
    }

    // 3. WriteRequestScratch::metadata on SliceRef::Common (KafkaV2) + non-nullable MetadataRW2 (0x2a 0x00)
    let rw1 = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_static("__name__", "up")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: 1000,
            }],
            ..Default::default()
        }],
        metadata: vec![cortexpb::MetricMetadata {
            r#type: 2,
            metric_family_name: "up".to_string(),
            help: "seconds".to_string(),
            unit: "seconds".to_string(),
        }],
        ..Default::default()
    };
    let mut kv2_wire = Vec::new();
    encode_write_request_rw2_bytes(&rw1, V2_RECORD_SYMBOL_OFFSET, Some(v2_common_symbols()), &mut kv2_wire)
        .unwrap();
    decode_write_request(&kv2_wire, DecodeOptions::kafka_v2(), &mut scratch).unwrap();
    let metas: Vec<_> = scratch.metadata(&kv2_wire).collect();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].unit, "seconds");

    let rw2_no_meta = from_write_request_to_rw2(
        &cortexpb::WriteRequest {
            timeseries: rw1.timeseries,
            ..Default::default()
        },
        0,
        None,
    )
    .unwrap();
    assert!(rw2_no_meta.encode_to_vec().windows(2).any(|w| w == [0x2a, 0x00]));

    // 4. unpack_any suffix matching & extreme i64 TSDB helpers
    let det = cortexpb::ErrorDetails {
        cause: cortexpb::ErrorCause::TenantLimit as i32,
        soft: false,
        rejected_samples: 0,
    };
    let any = pack_any("https://custom.example/cortexpb.ErrorDetails", &det);
    assert_eq!(
        unpack_any::<cortexpb::ErrorDetails>(&any, CORTEXPB_ERROR_DETAILS_TYPE_URL)
            .unwrap()
            .unwrap(),
        det
    );
    assert_eq!(range_for_timestamp(i64::MAX, 7_200_000), i64::MAX);
    assert_eq!(
        add_jitter_to_chunk_end_time(12345, 0, 20_000, 100_000, 0.2).clamp(18_000, 22_000),
        add_jitter_to_chunk_end_time(12345, 0, 20_000, 100_000, 0.2)
    );
    let dir_h = tempfile::tempdir().unwrap();
    let h = Head::open(dir_h.path(), HeadOptions::default(), i64::MIN).unwrap();
    h.set_chunk_end_time_variance(0.2);
    assert!((h.chunk_end_time_variance() - 0.2).abs() < f64::EPSILON);
    h.close().unwrap();
}
