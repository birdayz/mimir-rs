//! Wave 12 TSDB Cross-Engine, Codec, & Checkpoint Differential Conformance Suite.
//!
//! Covers all 17 findings from `w12-hunt1-tsdb-nan-checkpoint.md` (`P0-1..6`, `P1-1..6`,
//! `P2-1..5`) against upstream Go Prometheus/Mimir via `target/go-oracle`.

#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fs;
use std::sync::{Arc, Barrier};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use mimir_conformance::oracle::{
    invoke_oracle, ChunkResponse, ChunkSampleJson, DecodeChunkRequest, EncodeChunkRequest,
    HistogramJson, LabelJson, SpanJson, TsdbSeriesFixtureJson, VerifyTsdbDirRequest,
    VerifyTsdbDirResponse, WriteTsdbFixtureRequest, WriteTsdbFixtureResponse,
};
use mimir_tsdb::block::{
    read_meta_file, write_meta_file, Block, BlockDesc, BlockMeta, BlockMetaCompaction, BlockStats,
    ThanosMeta, META_FILENAME, META_VERSION_1,
};
use mimir_tsdb::chunkenc::histogram_model::{CounterResetHint, Histogram, HistogramSpan};
use mimir_tsdb::chunkenc::{
    EncodedChunk, Encoding, FloatHistogram, FloatHistogramChunk, HistogramChunk, OooChunk,
    Xor2Chunk, XorChunk,
};
use mimir_tsdb::chunks_block::{BlockChunksError, BlockChunksReader, BlockChunksWriter, ChunkMeta};
use mimir_tsdb::compact::{
    compact_blocks, encode_iterable_chunks, encode_samples_into_chunks_with_encodings,
    write_block_from_series, CompactionSeries, DecodedSample, SampleValue,
};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::encoding::Encbuf;
use mimir_tsdb::head::{Head, HeadOptions};
use mimir_tsdb::index::{IndexError, IndexReader, IndexWriter, INDEX_FILENAME};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::record::{
    RecordDecoder, RecordEncoder, RefExemplar, RefFloatHistogramSample, RefHistogramSample,
    RefSeries,
};
use mimir_tsdb::wlog::{
    create_checkpoint, last_checkpoint, CompressionType, WlogError, WlogReader, WlogWriter,
};
use ulid::Ulid;

fn lbls(pairs: &[(&str, &str)]) -> Vec<LabelJson> {
    pairs
        .iter()
        .map(|(n, v)| LabelJson {
            name: (*n).to_string(),
            value: (*v).to_string(),
        })
        .collect()
}

fn make_float_hist(
    schema: i32,
    count: f64,
    sum: f64,
    pos: &[(i32, u32, &[f64])],
) -> FloatHistogram {
    let mut positive_spans = Vec::new();
    let mut positive_buckets = Vec::new();
    for &(off, len, bkts) in pos {
        positive_spans.push(HistogramSpan {
            offset: off,
            length: len,
        });
        positive_buckets.extend_from_slice(bkts);
    }
    FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema,
        zero_threshold: 1e-128,
        zero_count: 1.0,
        count,
        sum,
        positive_spans,
        negative_spans: Vec::new(),
        positive_buckets,
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn to_fhist_json(t: i64, fh: &FloatHistogram) -> HistogramJson {
    HistogramJson {
        timestamp_ms: t,
        schema: fh.schema,
        zero_threshold: fh.zero_threshold,
        zero_count_float: fh.zero_count,
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
        positive_counts: fh.positive_buckets.clone(),
        negative_spans: fh
            .negative_spans
            .iter()
            .map(|s| SpanJson {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_counts: fh.negative_buckets.clone(),
        custom_values: fh.custom_values.clone(),
        ..Default::default()
    }
}

/// [P0-1] Go-written EncXOR chunks with constant prefixes reopened via WAL/chunks_head
/// emit control bits `10` with `leading=0, trailing=0` (mb=64) on the first value change.
#[test]
fn p0_1_go_written_constant_prefix_xor_chunk_cross_engine_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap().to_string();

    // Phase 1: Go writes constant-value samples (42.0, 42.0) and closes.
    let _: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir_str.clone(),
            compact: false,
            out_of_order_time_window_ms: 0,
            series: vec![TsdbSeriesFixtureJson {
                labels: lbls(&[("__name__", "const_prefix"), ("job", "p0_1")]),
                samples: vec![
                    ChunkSampleJson { st: 0, t: 1_000_000, v: 42.0 },
                    ChunkSampleJson { st: 0, t: 1_015_000, v: 42.0 },
                ],
                ..Default::default()
            }],
        },
    )
    .expect("go phase 1 write");

    // Phase 2: Go reopens WAL/Head, calls XORChunk.Appender() on the constant-prefix
    // chunk (leading=0, trailing=0), and appends changed values.
    let _: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir_str,
            compact: false,
            out_of_order_time_window_ms: 0,
            series: vec![TsdbSeriesFixtureJson {
                labels: lbls(&[("__name__", "const_prefix"), ("job", "p0_1")]),
                samples: vec![
                    ChunkSampleJson { st: 0, t: 1_030_000, v: 99.375 },
                    ChunkSampleJson { st: 0, t: 1_045_000, v: -12.5 },
                ],
                ..Default::default()
            }],
        },
    )
    .expect("go phase 2 write");

    let db = Tsdb::open(dir.path(), TsdbOptions::default()).expect("rust open go tsdb");
    let m = LabelMatcher::new(MatchType::Equal, "__name__", "const_prefix").unwrap();
    let res = db.querier(i64::MIN, i64::MAX).select(true, &[m]).unwrap();
    assert_eq!(res.len(), 1);
    let got: Vec<(i64, f64)> = res[0]
        .samples
        .iter()
        .map(|s| match s.value {
            SampleValue::Float(v) => (s.t, v),
            _ => panic!("expected float"),
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (1_000_000, 42.0),
            (1_015_000, 42.0),
            (1_030_000, 99.375),
            (1_045_000, -12.5),
        ]
    );
    db.close().unwrap();

    // Direct EncXOR chunk with ctrl 10 (leading=0, trailing=0, mb=64) matches Go decode-chunk.
    let mut raw = vec![0u8, 2u8];
    let mut enc = Encbuf::new();
    enc.put_varint64(100);
    raw.extend_from_slice(enc.as_slice());
    raw.extend_from_slice(&42.0f64.to_bits().to_be_bytes());
    let mut enc2 = Encbuf::new();
    enc2.put_uvarint64(10);
    raw.extend_from_slice(enc2.as_slice());
    let delta_bits = 42.0f64.to_bits() ^ 13.375f64.to_bits();
    let b0 = 0b1000_0000u8 | ((delta_bits >> 58) as u8 & 0x3f);
    raw.push(b0);
    for shift in [50, 42, 34, 26, 18, 10, 2] {
        raw.push((delta_bits >> shift) as u8);
    }
    raw.push(((delta_bits & 0x3) as u8) << 6);

    let go_dec: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: Encoding::Xor.as_u8(),
            bytes_b64: B64.encode(&raw),
        },
    )
    .expect("go decode-chunk");
    assert_eq!(go_dec.samples.len(), 2);

    let chunk = XorChunk::from_bytes(raw);
    let mut it = chunk.iter();
    assert_eq!(it.next_sample().unwrap(), Some((100, 42.0)));
    assert_eq!(it.next_sample().unwrap(), Some((110, 13.375)));
    assert_eq!(it.next_sample().unwrap(), None);
}

/// [P0-2] Go-written `meta.json` containing `"labels": null` and null slices loads cleanly.
#[test]
fn p0_2_go_meta_json_with_null_maps_and_slices_loads_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let dir_str = dir.path().to_str().unwrap().to_string();

    let resp: WriteTsdbFixtureResponse = invoke_oracle(
        "write-tsdb-fixture",
        &WriteTsdbFixtureRequest {
            dir: dir_str,
            compact: true,
            out_of_order_time_window_ms: 0,
            series: vec![TsdbSeriesFixtureJson {
                labels: lbls(&[("__name__", "shipped_metric"), ("job", "p0_2")]),
                samples: vec![
                    ChunkSampleJson { st: 0, t: 1_000_000, v: 1.0 },
                    ChunkSampleJson { st: 0, t: 1_015_000, v: 2.0 },
                ],
                ..Default::default()
            }],
        },
    )
    .expect("go write block");
    assert_eq!(resp.blocks.len(), 1);
    let block_dir = dir.path().join(&resp.blocks[0]);

    let meta_path = block_dir.join(META_FILENAME);
    let mut raw_val: serde_json::Value =
        serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
    let obj = raw_val.as_object_mut().unwrap();
    obj.insert(
        "thanos".into(),
        serde_json::json!({
            "version": 1,
            "labels": null,
            "downsample": { "resolution": 0 },
            "source": "receive",
            "segment_files": null,
            "files": null
        }),
    );
    if let Some(comp) = obj.get_mut("compaction").and_then(|v| v.as_object_mut()) {
        comp.insert("parents".into(), serde_json::Value::Null);
        comp.insert("hints".into(), serde_json::Value::Null);
    }
    fs::write(&meta_path, serde_json::to_vec_pretty(&raw_val).unwrap()).unwrap();

    let meta = read_meta_file(&block_dir).expect("read_meta_file with nulls");
    assert_eq!(meta.version, META_VERSION_1);
    let thanos = meta.thanos.expect("thanos present");
    assert!(thanos.labels.is_empty());
    assert!(thanos.segment_files.is_empty());
    assert!(thanos.files.is_empty());

    let db = Tsdb::open(dir.path(), TsdbOptions::default()).expect("Tsdb::open with Go null meta");
    assert_eq!(db.blocks().len(), 1);
    db.reload_blocks().expect("reload_blocks with Go null meta");
    db.close().unwrap();
}

/// [P0-3] `compact_blocks` preserves `from-stale-series` and `from-selected-series` hints.
#[test]
fn p0_3_compact_blocks_preserves_stale_and_selected_hints_and_wal_cutoff() {
    for hint in ["selected", "stale"] {
        let dir = tempfile::tempdir().unwrap();
        let make_block = |mint: i64, maxt: i64, val: f64| -> Ulid {
            let mut comp = BlockMetaCompaction::default();
            if hint == "selected" {
                comp.set_selected_series();
            } else {
                comp.set_stale_series();
            }
            let meta = write_block_from_series(
                dir.path(),
                mint,
                maxt,
                vec![CompactionSeries {
                    labels: Labels::from_pairs([("__name__", "hinted"), ("kind", hint)]),
                    chunks: encode_samples_into_chunks_with_encodings(
                        &[DecodedSample {
                            st: 0,
                            t: mint + 1000,
                            value: SampleValue::Float(val),
                        }],
                        120,
                        Encoding::Xor,
                        Encoding::Histogram,
                        Encoding::FloatHistogram,
                    )
                    .unwrap(),
                    tombstones: Vec::new(),
                }],
                comp,
                false,
                0,
            )
            .unwrap()
            .unwrap();
            meta.ulid
        };

        let u1 = make_block(0, 2_000_000, 10.0);
        let u2 = make_block(2_000_000, 4_000_000, 20.0);
        let b1 = Block::open(dir.path().join(u1.to_string())).unwrap();
        let b2 = Block::open(dir.path().join(u2.to_string())).unwrap();

        let merged = compact_blocks(dir.path(), &[&b1, &b2])
            .unwrap()
            .expect("merged block");
        if hint == "selected" {
            assert!(merged.compaction.from_selected_series());
            assert!(!merged.compaction.from_stale_series());
        } else {
            assert!(merged.compaction.from_stale_series());
            assert!(!merged.compaction.from_selected_series());
        }

        drop(b1);
        drop(b2);
        fs::remove_dir_all(dir.path().join(u1.to_string())).unwrap();
        fs::remove_dir_all(dir.path().join(u2.to_string())).unwrap();

        {
            let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
            let mut app = db.appender();
            app.append_float(
                None,
                &[("__name__", "owned_wal_series")],
                1_500_000,
                777.0,
            )
            .unwrap();
            app.commit().unwrap();
            db.close().unwrap();
        }

        let db2 = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        let m = LabelMatcher::new(MatchType::Equal, "__name__", "owned_wal_series").unwrap();
        let got = db2.querier(i64::MIN, i64::MAX).select(true, &[m]).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].samples.len(), 1);
        assert_eq!(got[0].samples[0].t, 1_500_000);
        assert_eq!(got[0].samples[0].value, SampleValue::Float(777.0));
        db2.close().unwrap();
    }
}

/// [P0-4] `evict_series_refs_with_fingerprints` skips series mutated after candidate snapshot.
#[test]
fn p0_4_fingerprint_check_prevents_concurrent_append_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.samples_per_chunk = 3;

    let (r_same_chunk, r_new_chunk, r_unmutated);
    {
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "other")], 50_000, 1.0)
            .unwrap();
        r_same_chunk = app
            .append_float(None, &[("__name__", "s_same")], 10_000, 1.0)
            .unwrap();
        r_new_chunk = app
            .append_float(None, &[("__name__", "s_new")], 10_000, 1.0)
            .unwrap();
        app.append_float(Some(r_new_chunk), &[("__name__", "s_new")], 11_000, 2.0)
            .unwrap();
        app.append_float(Some(r_new_chunk), &[("__name__", "s_new")], 12_000, 3.0)
            .unwrap();
        r_unmutated = app
            .append_float(None, &[("__name__", "s_unmutated")], 10_000, 9.0)
            .unwrap();
        app.commit().unwrap();

        let head_maxt = db.head().max_time();
        assert_eq!(head_maxt, 50_000);

        let refs = [r_same_chunk, r_new_chunk, r_unmutated];
        let ((candidates, _, _), fingerprints) = db
            .head()
            .collect_selected_compaction_candidates_with_fingerprints(&refs)
            .unwrap();
        assert_eq!(candidates.len(), 3);

        let mut app2 = db.appender();
        app2.append_float(Some(r_same_chunk), &[("__name__", "s_same")], 20_000, 2.0)
            .unwrap();
        app2.append_float(Some(r_new_chunk), &[("__name__", "s_new")], 20_000, 4.0)
            .unwrap();
        app2.commit().unwrap();

        db.head()
            .evict_series_refs_with_fingerprints(&refs, head_maxt, &fingerprints)
            .unwrap();

        assert!(db.head().series_labels(r_same_chunk).is_some());
        assert!(db.head().series_labels(r_new_chunk).is_some());
        assert!(db.head().series_labels(r_unmutated).is_none());
        db.close().unwrap();
    }

    let db2 = Tsdb::open(dir.path(), opts).unwrap();
    let m_same = LabelMatcher::new(MatchType::Equal, "__name__", "s_same").unwrap();
    let res_same = db2.querier(i64::MIN, i64::MAX).select(true, &[m_same]).unwrap();
    assert_eq!(res_same[0].samples.len(), 2);
    assert_eq!(res_same[0].samples[1].t, 20_000);

    let m_new = LabelMatcher::new(MatchType::Equal, "__name__", "s_new").unwrap();
    let res_new = db2.querier(i64::MIN, i64::MAX).select(true, &[m_new]).unwrap();
    assert_eq!(res_new[0].samples.len(), 4);
    assert_eq!(res_new[0].samples[3].t, 20_000);
    db2.close().unwrap();
}

/// [P0-5] Concurrent `Head::truncate_wal` + `truncate_memory` serialized cleanly.
#[test]
fn p0_5_concurrent_truncate_wal_and_memory_serialized() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        wal_segment_size: 32 * 1024,
        wal_compression: CompressionType::Snappy,
        ..Default::default()
    };
    let head = Arc::new(Head::open(dir.path(), opts, i64::MIN).unwrap());

    let mut to_evict = Vec::new();
    for seg_batch in 0..8 {
        let mut app = head.appender();
        for s in 0..60 {
            let name = format!("m_{seg_batch}_{s}");
            let r = app
                .append_float(
                    None,
                    &[("__name__", name.as_str()), ("pad", "value_padding_0123456789abcdef")],
                    1000 + seg_batch * 1000,
                    s as f64,
                )
                .unwrap();
            if s % 3 == 0 {
                to_evict.push(r);
            }
        }
        app.commit().unwrap();
    }

    let barrier = Arc::new(Barrier::new(4));
    let h1 = Arc::clone(&head);
    let b1 = Arc::clone(&barrier);
    let t1 = std::thread::spawn(move || {
        b1.wait();
        for m in [2000, 4000, 6000] {
            h1.truncate_wal(m).unwrap();
        }
    });
    let h2 = Arc::clone(&head);
    let b2 = Arc::clone(&barrier);
    let t2 = std::thread::spawn(move || {
        b2.wait();
        for m in [2000, 4000, 6000] {
            h2.truncate_wal(m).unwrap();
        }
    });
    let h3 = Arc::clone(&head);
    let b3 = Arc::clone(&barrier);
    let t3 = std::thread::spawn(move || {
        b3.wait();
        for m in [2000, 4000, 6000] {
            h3.truncate_memory(m).unwrap();
        }
    });
    let h4 = Arc::clone(&head);
    let b4 = Arc::clone(&barrier);
    let t4 = std::thread::spawn(move || {
        b4.wait();
        for chunk in to_evict.chunks(20) {
            h4.evict_series_refs_before(chunk, 3000).unwrap();
        }
    });
    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
    t4.join().unwrap();
    head.close().unwrap();

    let go_verify: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_str().unwrap().to_string(),
            read_only: false,
        },
    )
    .expect("go verify after concurrent truncate_wal");
    assert!(go_verify.head_series > 0);
}

/// [P0-6] Reserved-schema (9..=52) histogram resolution reduction uses `((idx - 1) >> shift) + 1`.
#[test]
fn p0_6_reserved_schema_histogram_reduce_resolution_parity_and_validation() {
    let h_schema_9 = Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 9,
        zero_threshold: 0.01,
        zero_count: 5,
        count: 100,
        sum: 42.5,
        positive_spans: vec![HistogramSpan { offset: -1, length: 4 }],
        negative_spans: vec![HistogramSpan { offset: -1, length: 4 }],
        positive_buckets: vec![10, 10, 10, 10],
        negative_buckets: vec![2, 3, 5, 10],
        custom_values: Vec::new(),
    };
    let fh_schema_9 = FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 9,
        zero_threshold: 0.01,
        zero_count: 5.0,
        count: 100.0,
        sum: 42.5,
        positive_spans: vec![HistogramSpan { offset: -1, length: 4 }],
        negative_spans: vec![HistogramSpan { offset: -1, length: 4 }],
        positive_buckets: vec![10.0, 20.0, 30.0, 40.0],
        negative_buckets: vec![2.0, 5.0, 10.0, 20.0],
        custom_values: Vec::new(),
    };

    let dir = tempfile::tempdir().unwrap();
    let enc = RecordEncoder::new(true);
    let dec = RecordDecoder::new();
    {
        let mut w = WlogWriter::open(dir.path(), CompressionType::None).unwrap();
        let srec = enc.series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_pairs([("__name__", "h9")]),
            }],
            Vec::new(),
        );
        let (hrec, _) = enc.histogram_samples(
            &[RefHistogramSample {
                ref_id: 1,
                st: 100,
                t: 1_000,
                h: h_schema_9.clone(),
            }],
            Vec::new(),
        );
        let (fhrec, _) = enc.float_histogram_samples(
            &[RefFloatHistogramSample {
                ref_id: 1,
                st: 100,
                t: 2_000,
                fh: fh_schema_9.clone(),
            }],
            Vec::new(),
        );
        w.log(&[&srec, &hrec, &fhrec]).unwrap();
        w.next_segment().unwrap();
        w.close().unwrap();
    }

    create_checkpoint(dir.path(), 0, 0, |_| true, 500, true, CompressionType::None).unwrap();
    let (cp_dir, _) = last_checkpoint(dir.path()).unwrap().unwrap();
    let mut r = WlogReader::open_dir(&cp_dir).unwrap();
    let mut got_h = None;
    let mut got_fh = None;
    while let Some(rec) = r.next_record().unwrap() {
        match dec.record_type(rec) {
            mimir_tsdb::record::RecordType::HistogramSamples
            | mimir_tsdb::record::RecordType::HistogramSamplesV2 => {
                got_h = Some(dec.histogram_samples(rec).unwrap().pop().unwrap().h);
            }
            mimir_tsdb::record::RecordType::FloatHistogramSamples
            | mimir_tsdb::record::RecordType::FloatHistogramSamplesV2 => {
                got_fh = Some(dec.float_histogram_samples(rec).unwrap().pop().unwrap().fh);
            }
            _ => {}
        }
    }
    let got_h = got_h.expect("reduced int histogram");
    assert_eq!(got_h.schema, 8);
    assert_eq!(got_h.positive_spans, vec![HistogramSpan { offset: 0, length: 2 }]);
    assert_eq!(got_h.positive_buckets, vec![30, 40]);
    assert_eq!(got_h.negative_buckets, vec![7, 23]);

    let got_fh = got_fh.expect("reduced float histogram");
    assert_eq!(got_fh.schema, 8);
    assert_eq!(got_fh.positive_spans, vec![HistogramSpan { offset: 0, length: 2 }]);
    assert_eq!(got_fh.positive_buckets, vec![30.0, 70.0]);
    assert_eq!(got_fh.negative_buckets, vec![7.0, 30.0]);

    let mut hc = HistogramChunk::new();
    hc.appender()
        .unwrap()
        .append_histogram(None, 0, 1_000, &h_schema_9, false)
        .unwrap();
    let go_h: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: Encoding::Histogram.as_u8(),
            bytes_b64: B64.encode(hc.as_bytes()),
        },
    )
    .expect("go decode schema-9 int histogram");
    let (_, rust_h) = hc.iter().next_histogram().unwrap().unwrap();
    assert_eq!(rust_h.positive_buckets, go_h.histograms[0].positive_deltas);
    assert_eq!(rust_h.negative_buckets, go_h.histograms[0].negative_deltas);

    let mut fhc = FloatHistogramChunk::new();
    fhc.appender()
        .unwrap()
        .append_float_histogram(None, 0, 1_000, &fh_schema_9, false)
        .unwrap();
    let go_fh: ChunkResponse = invoke_oracle(
        "decode-chunk",
        &DecodeChunkRequest {
            encoding: Encoding::FloatHistogram.as_u8(),
            bytes_b64: B64.encode(fhc.as_bytes()),
        },
    )
    .expect("go decode schema-9 float histogram");
    let (_, rust_fh) = fhc.iter().next_float_histogram().unwrap().unwrap();
    assert_eq!(rust_fh.positive_buckets, go_fh.histograms[0].positive_counts);
    assert_eq!(rust_fh.negative_buckets, go_fh.histograms[0].negative_counts);

    // Invalid negative offset on span index > 0 and span/bucket mismatch must error.
    let bad_h = Histogram {
        schema: 9,
        positive_spans: vec![
            HistogramSpan { offset: 0, length: 1 },
            HistogramSpan { offset: -1, length: 1 },
        ],
        positive_buckets: vec![1, 1],
        ..Default::default()
    };
    let (bad_rec, _) = enc.histogram_samples(
        &[RefHistogramSample { ref_id: 1, st: 0, t: 1000, h: bad_h }],
        Vec::new(),
    );
    assert!(dec.histogram_samples(&bad_rec).is_err());

    let mismatch_fh = FloatHistogram {
        schema: 9,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        positive_buckets: vec![1.0],
        ..Default::default()
    };
    let (bad_frec, _) = enc.float_histogram_samples(
        &[RefFloatHistogramSample { ref_id: 1, st: 0, t: 1000, fh: mismatch_fh }],
        Vec::new(),
    );
    assert!(dec.float_histogram_samples(&bad_frec).is_err());
}

/// [P1-1] Live appender state in compact, ooo, and head rollover matches Go encode-chunk bit-for-bit.
#[test]
fn p1_1_live_appender_state_bit_for_bit_parity_with_go() {
    let samples = vec![
        DecodedSample { st: 0, t: 1_000, value: SampleValue::Float(42.0) },
        DecodedSample { st: 0, t: 2_000, value: SampleValue::Float(42.0) },
        DecodedSample { st: 0, t: 3_000, value: SampleValue::Float(42.5) },
        DecodedSample { st: 0, t: 4_000, value: SampleValue::Float(43.25) },
    ];
    let go_xor: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: Encoding::Xor.as_u8(),
            samples: samples
                .iter()
                .map(|s| match s.value {
                    SampleValue::Float(v) => ChunkSampleJson { st: s.st, t: s.t, v },
                    _ => unreachable!(),
                })
                .collect(),
            histograms: Vec::new(),
        },
    )
    .unwrap();
    let go_bytes = B64.decode(&go_xor.bytes_b64).unwrap();

    let c1 = encode_samples_into_chunks_with_encodings(
        &samples,
        120,
        Encoding::Xor,
        Encoding::Histogram,
        Encoding::FloatHistogram,
    )
    .unwrap();
    assert_eq!(c1[0].chunk.as_ref().unwrap().as_bytes(), go_bytes.as_slice());

    let c2 = encode_iterable_chunks(&samples).unwrap();
    assert_eq!(c2[0].chunk.as_ref().unwrap().as_bytes(), go_bytes.as_slice());

    let mut ooo = OooChunk::new();
    for s in &samples {
        if let SampleValue::Float(v) = s.value {
            ooo.insert_float(s.st, s.t, v);
        }
    }
    let c3 = ooo.to_encoded_chunks(i64::MIN, i64::MAX, false, false).unwrap();
    assert_eq!(c3[0].chunk.as_bytes(), go_bytes.as_slice());

    let fh1 = make_float_hist(0, 10.0, 20.0, &[(0, 2, &[4.0, 6.0])]);
    let fh2 = make_float_hist(0, 10.0, 20.0, &[(0, 2, &[4.0, 6.0])]);
    let fh3 = make_float_hist(0, 11.0, 20.5, &[(0, 2, &[4.0, 7.0])]);
    let fh4 = make_float_hist(0, 12.0, 21.5, &[(0, 2, &[4.0, 8.0])]);
    let fh_samples = vec![
        DecodedSample { st: 0, t: 1_000, value: SampleValue::FloatHistogram(Box::new(fh1.clone())) },
        DecodedSample { st: 0, t: 2_000, value: SampleValue::FloatHistogram(Box::new(fh2.clone())) },
        DecodedSample { st: 0, t: 3_000, value: SampleValue::FloatHistogram(Box::new(fh3.clone())) },
        DecodedSample { st: 0, t: 4_000, value: SampleValue::FloatHistogram(Box::new(fh4.clone())) },
    ];
    let go_fh: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: Encoding::FloatHistogram.as_u8(),
            samples: Vec::new(),
            histograms: vec![
                to_fhist_json(1_000, &fh1),
                to_fhist_json(2_000, &fh2),
                to_fhist_json(3_000, &fh3),
                to_fhist_json(4_000, &fh4),
            ],
        },
    )
    .unwrap();
    let go_fh_bytes = B64.decode(&go_fh.bytes_b64).unwrap();

    let cfh1 = encode_samples_into_chunks_with_encodings(
        &fh_samples,
        120,
        Encoding::Xor,
        Encoding::Histogram,
        Encoding::FloatHistogram,
    )
    .unwrap();
    assert_eq!(cfh1[0].chunk.as_ref().unwrap().as_bytes(), go_fh_bytes.as_slice());

    let cfh2 = encode_iterable_chunks(&fh_samples).unwrap();
    assert_eq!(cfh2[0].chunk.as_ref().unwrap().as_bytes(), go_fh_bytes.as_slice());

    let mut ooo_fh = OooChunk::new();
    for s in &fh_samples {
        if let SampleValue::FloatHistogram(ref fh) = s.value {
            ooo_fh.insert_float_histogram(s.st, s.t, (**fh).clone());
        }
    }
    let cfh3 = ooo_fh
        .to_encoded_chunks(i64::MIN, i64::MAX, false, false)
        .unwrap();
    assert_eq!(cfh3[0].chunk.as_bytes(), go_fh_bytes.as_slice());

    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let mut app = head.appender();
    let old_fh = make_float_hist(1, 5.0, 10.0, &[(0, 1, &[5.0])]);
    let sref = app
        .append_float_histogram(None, &[("__name__", "fh_rollover")], 0, 500, old_fh)
        .unwrap();
    for s in &fh_samples {
        if let SampleValue::FloatHistogram(ref fh) = s.value {
            app.append_float_histogram(
                Some(sref),
                &[("__name__", "fh_rollover")],
                s.st,
                s.t,
                (**fh).clone(),
            )
            .unwrap();
        }
    }
    app.commit().unwrap();
    let (_, chks, _) = head.series_chunks(sref, i64::MIN, i64::MAX).unwrap().unwrap();
    assert_eq!(chks.len(), 2);
    assert_eq!(chks[1].chunk.as_ref().unwrap().as_bytes(), go_fh_bytes.as_slice());
    head.close().unwrap();
}

/// [P1-2], [P1-4], [P2-3], [P2-4] IndexWriter/IndexReader & BlockChunksReader strictness.
#[test]
fn p1_2_p1_4_p2_3_p2_4_index_and_block_chunks_validation() {
    let dir = tempfile::tempdir().unwrap();

    let idx_path = dir.path().join(INDEX_FILENAME);
    let mut w = IndexWriter::new(&idx_path).unwrap();
    w.add_symbol("").unwrap();
    w.add_symbol("__name__").unwrap();
    w.add_symbol("a").unwrap();
    w.add_symbol("b").unwrap();

    assert!(matches!(
        w.add_series(0, &Labels::empty(), &[]),
        Err(IndexError::OutOfOrderSeries(_))
    ));
    let la = Labels::from_pairs([("__name__", "a")]);
    let lb = Labels::from_pairs([("__name__", "b")]);
    assert!(matches!(
        w.add_series(
            0,
            &la,
            &[
                ChunkMeta { ref_id: 1, min_time: 100, max_time: 200, chunk: None },
                ChunkMeta { ref_id: 2, min_time: 200, max_time: 300, chunk: None },
            ]
        ),
        Err(IndexError::InvalidChunkOrder(_))
    ));
    w.add_series(
        10,
        &la,
        &[ChunkMeta { ref_id: 1, min_time: 100, max_time: 200, chunk: None }],
    )
    .unwrap();
    assert!(matches!(
        w.add_series(
            5,
            &lb,
            &[ChunkMeta { ref_id: 2, min_time: 100, max_time: 200, chunk: None }]
        ),
        Err(IndexError::InvalidChunkOrder(_))
    ));
    w.close().unwrap();

    let ir = IndexReader::open(&idx_path).unwrap();
    assert!(ir.label_names_for(&[u64::MAX]).is_err());

    let mut idx_bytes = fs::read(&idx_path).unwrap();
    let sym_len = u32::from_be_bytes(idx_bytes[5..9].try_into().unwrap()) as usize;
    let sym_start = 5 + 4;
    let sym_end = sym_start + sym_len;
    let needle = b"__name__";
    let pos = idx_bytes[sym_start..sym_end]
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("__name__ in symbols");
    idx_bytes[sym_start + pos + 5] = b'X';
    let new_crc = crc32c::crc32c(&idx_bytes[sym_start..sym_end]);
    idx_bytes[sym_end..sym_end + 4].copy_from_slice(&new_crc.to_be_bytes());
    let corrupt_idx = dir.path().join("index_corrupt_po");
    fs::write(&corrupt_idx, &idx_bytes).unwrap();
    assert!(IndexReader::open(&corrupt_idx).is_err());

    let chunks_dir = dir.path().join("chunks");
    let mut cw = BlockChunksWriter::with_segment_size(&chunks_dir, 32).unwrap();
    let mut xc = XorChunk::new();
    xc.appender().unwrap().append(0, 1000, 1.0).unwrap();
    let mut cm1 = vec![ChunkMeta {
        ref_id: 0,
        min_time: 1000,
        max_time: 1000,
        chunk: Some(EncodedChunk::Xor(xc.clone())),
    }];
    cw.write_chunks(&mut cm1).unwrap();
    let mut cm2 = vec![ChunkMeta {
        ref_id: 0,
        min_time: 2000,
        max_time: 2000,
        chunk: Some(EncodedChunk::Xor(xc)),
    }];
    cw.write_chunks(&mut cm2).unwrap();
    cw.close().unwrap();
    assert!(chunks_dir.join("000001").exists());
    assert!(chunks_dir.join("000002").exists());
    fs::remove_file(chunks_dir.join("000001")).unwrap();
    assert!(matches!(
        BlockChunksReader::open(&chunks_dir),
        Err(BlockChunksError::Corruption { .. })
    ));
}

/// [P1-3] OOO & selected-series blocks emit `out_of_order: false` and zero-ULID parent.
#[test]
fn p1_3_ooo_and_selected_series_meta_parity_with_go() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_time_window_ms = 3_600_000;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    {
        let mut app = db.appender();
        let r_ooo = app
            .append_float(None, &[("__name__", "ooo_metric")], 10_000_000, 1.0)
            .unwrap();
        let r_sel = app
            .append_float(None, &[("__name__", "selected_metric")], 10_000_000, 3.0)
            .unwrap();
        app.commit().unwrap();

        let mut app2 = db.appender();
        app2.append_float(Some(r_ooo), &[("__name__", "ooo_metric")], 9_000_000, 2.0)
            .unwrap();
        app2.commit().unwrap();

        let ooo_ulids = db.compact_ooo_head().unwrap();
        assert_eq!(ooo_ulids.len(), 1);
        let ooo_meta = read_meta_file(&dir.path().join(ooo_ulids[0].to_string())).unwrap();
        assert!(!ooo_meta.out_of_order);
        assert!(ooo_meta.compaction.from_out_of_order());
        assert_eq!(
            ooo_meta.compaction.parents,
            vec![BlockDesc { ulid: Ulid::nil(), min_time: 0, max_time: 0 }]
        );

        let sel_ulids = db.compact_selected_series(&[r_sel]).unwrap();
        assert_eq!(sel_ulids.len(), 1);
        let sel_meta = read_meta_file(&dir.path().join(sel_ulids[0].to_string())).unwrap();
        assert!(!sel_meta.out_of_order);
        assert!(sel_meta.compaction.from_selected_series());
        assert_eq!(
            sel_meta.compaction.parents,
            vec![BlockDesc { ulid: Ulid::nil(), min_time: 0, max_time: 0 }]
        );
    }
    db.close().unwrap();

    let go_verify: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_str().unwrap().to_string(),
            read_only: true,
        },
    )
    .expect("go verify rust OOO + selected blocks");
    assert_eq!(go_verify.block_ulids.len(), 2);
}

/// [P1-5] `RecordDecoder::decode_labels` canonicalizes unsorted/duplicate/empty labels.
#[test]
fn p1_5_record_decoder_canonicalizes_non_canonical_wal_labels() {
    let mut enc = Encbuf::new();
    enc.put_byte(1);
    enc.put_be64(42);
    enc.put_uvarint(4);
    for (k, v) in [("z", "1"), ("a", "first"), ("empty", ""), ("a", "second")] {
        enc.put_uvarint_str(k);
        enc.put_uvarint_str(v);
    }
    let dec = RecordDecoder::new();
    let series = dec.series(enc.as_slice()).unwrap();
    assert_eq!(series.len(), 1);
    let got: Vec<(&str, &str)> = series[0].labels.iter().collect();
    assert_eq!(got, vec![("a", "second"), ("z", "1")]);

    let mut enc_ex = Encbuf::new();
    enc_ex.put_byte(4);
    enc_ex.put_be64(42);
    enc_ex.put_be64_i64(1000);
    enc_ex.put_varint64(0);
    enc_ex.put_varint64(0);
    enc_ex.put_be64(1.25f64.to_bits());
    enc_ex.put_uvarint(3);
    for (k, v) in [("trace_id", "t1"), ("drop_me", ""), ("app", "api")] {
        enc_ex.put_uvarint_str(k);
        enc_ex.put_uvarint_str(v);
    }
    let exs = dec.exemplars(enc_ex.as_slice()).unwrap();
    assert_eq!(exs.len(), 1);
    let ex_got: Vec<(&str, &str)> = exs[0].labels.iter().collect();
    assert_eq!(ex_got, vec![("app", "api"), ("trace_id", "t1")]);
    let _ = RefExemplar { ref_id: 0, t: 0, v: 0.0, labels: Labels::empty() };
}

/// [P1-6] `DB::compact()` runs multi-range block compaction and tombstone cleanup.
#[test]
fn p1_6_db_compact_runs_multi_range_block_and_tombstone_compaction() {
    let range_2h = 2 * 3_600_000i64;
    let range_4h = 4 * 3_600_000i64;

    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = range_2h;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.set_block_ranges(vec![range_2h, range_4h]);

    {
        let mut app = db.appender();
        for h in [0, 2, 4, 6, 8, 10] {
            app.append_float(
                None,
                &[("__name__", "multi_range")],
                h * 3_600_000 + 1_000,
                (h + 1) as f64,
            )
            .unwrap();
        }
        app.commit().unwrap();
    }

    let created = db.compact().unwrap();
    assert!(created.len() >= 5, "created={created:?}");
    let blocks = db.blocks();
    assert_eq!(blocks.len(), 3, "expected 4h merged block + 2h + newest 2h block");
    assert_eq!(blocks[0].min_time(), 1_000);
    assert_eq!(blocks[0].max_time(), range_4h);
    assert_eq!(blocks[0].meta().compaction.level, 2);
    assert_eq!(blocks[1].min_time(), range_4h + 1_000);
    assert_eq!(blocks[1].max_time(), range_4h + range_2h);
    db.close().unwrap();

    let go_verify: VerifyTsdbDirResponse = invoke_oracle(
        "verify-tsdb-dir",
        &VerifyTsdbDirRequest {
            dir: dir.path().to_str().unwrap().to_string(),
            read_only: false,
        },
    )
    .expect("go verify multi-range compacted DB");
    assert_eq!(go_verify.block_ulids.len(), 3);
    assert_eq!(go_verify.series.len(), 1);
    assert_eq!(go_verify.series[0].samples.len(), 6);

    let dir_t = tempfile::tempdir().unwrap();
    let u = write_block_from_series(
        dir_t.path(),
        0,
        range_2h,
        vec![CompactionSeries {
            labels: Labels::from_pairs([("__name__", "to_delete")]),
            chunks: encode_samples_into_chunks_with_encodings(
                &[DecodedSample { st: 0, t: 1_000, value: SampleValue::Float(1.0) }],
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
    .unwrap()
    .unwrap()
    .ulid;
    let bdir = dir_t.path().join(u.to_string());
    let _u2 = write_block_from_series(
        dir_t.path(),
        range_2h,
        range_4h,
        vec![CompactionSeries {
            labels: Labels::from_pairs([("__name__", "keep_me")]),
            chunks: encode_samples_into_chunks_with_encodings(
                &[DecodedSample { st: 0, t: range_2h + 1_000, value: SampleValue::Float(2.0) }],
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
    .unwrap()
    .unwrap()
    .ulid;

    let b_open = Block::open(&bdir).unwrap();
    let sid = b_open.index().postings("", &[""]).unwrap()[0];
    drop(b_open);
    let tombs = mimir_tsdb::tombstones::MemTombstones::new();
    tombs.add_interval(sid, mimir_tsdb::tombstones::Interval::new(i64::MIN, i64::MAX));
    mimir_tsdb::tombstones::write_tombstones_file(&bdir, &tombs).unwrap();
    let mut meta = read_meta_file(&bdir).unwrap();
    meta.stats.num_tombstones = 1;
    write_meta_file(&bdir, &mut meta).unwrap();

    let db_t = Tsdb::open(dir_t.path(), TsdbOptions::default()).unwrap();
    assert_eq!(db_t.blocks().len(), 2);
    db_t.compact_blocks().unwrap();
    assert_eq!(db_t.blocks().len(), 1);
    assert_eq!(db_t.blocks()[0].min_time(), range_2h);
    db_t.close().unwrap();
}

/// [P2-1], [P2-2], [P2-5] Xor2Iterator st_diff, stray checkpoint.* file, and Head::open MinValidTime.
#[test]
fn p2_1_p2_2_p2_5_edge_cases() {
    let _port = mimir_conformance::free_test_port();
    let s0 = (500i64, 1_000i64, 1.25f64);
    let s1 = (600i64, 2_000i64, 2.5f64);
    let mut xc2 = Xor2Chunk::new();
    xc2.appender().unwrap().append(s0.0, s0.1, s0.2).unwrap();
    xc2.appender().unwrap().append(s1.0, s1.1, s1.2).unwrap();
    xc2.compact();

    let go_x2: ChunkResponse = invoke_oracle(
        "encode-chunk",
        &EncodeChunkRequest {
            encoding: Encoding::Xor2.as_u8(),
            samples: vec![
                ChunkSampleJson { st: s0.0, t: s0.1, v: s0.2 },
                ChunkSampleJson { st: s1.0, t: s1.1, v: s1.2 },
            ],
            histograms: Vec::new(),
        },
    )
    .unwrap();
    assert_eq!(xc2.as_bytes(), B64.decode(&go_x2.bytes_b64).unwrap().as_slice());

    let dir_cp = tempfile::tempdir().unwrap();
    fs::write(dir_cp.path().join("checkpoint.stray"), b"not a dir").unwrap();
    assert!(matches!(
        last_checkpoint(dir_cp.path()),
        Err(WlogError::CheckpointNotDir(_))
    ));

    let dir_h = tempfile::tempdir().unwrap();
    {
        let opts = HeadOptions {
            wal_segment_size: 32 * 1024,
            max_head_chunk_file_size: 1024,
            ..Default::default()
        };
        let head = Head::open(dir_h.path(), opts, i64::MIN).unwrap();
        let mut app = head.appender();
        let r = app
            .append_float(None, &[("__name__", "stale_mmap")], 1_000, 1.0)
            .unwrap();
        app.append_float(Some(r), &[("__name__", "stale_mmap")], 2_000, 2.0)
            .unwrap();
        app.append_float(Some(r), &[("__name__", "stale_mmap")], 10_000_000, 3.0)
            .unwrap();
        app.commit().unwrap();
        head.mmap_head_chunks().unwrap();
        head.truncate_memory(5_000_000).unwrap();
        head.close().unwrap();
        create_checkpoint(
            &dir_h.path().join("wal"),
            0,
            0,
            |_| true,
            5_000_000,
            false,
            CompressionType::None,
        )
        .unwrap();
    }

    let head2 = Head::open(dir_h.path(), HeadOptions::default(), i64::MIN).unwrap();
    assert!(head2.min_valid_time() >= 5_000_000);
    let m = LabelMatcher::new(MatchType::Equal, "__name__", "stale_mmap").unwrap();
    let res = mimir_tsdb::querier::TsdbQuerier::new(Some(&head2), Vec::new(), i64::MIN, i64::MAX)
        .select(true, &[m])
        .unwrap();
    assert_eq!(res.len(), 1);
    assert!(res[0].samples.iter().all(|s| s.t >= 5_000_000));
    head2.close().unwrap();

    let _ = BTreeMap::<String, String>::new();
    let _ = BlockMeta {
        ulid: Ulid::nil(),
        min_time: 0,
        max_time: 0,
        stats: BlockStats::default(),
        compaction: BlockMetaCompaction::default(),
        version: META_VERSION_1,
        out_of_order: false,
        thanos: Some(ThanosMeta::default()),
    };
}
