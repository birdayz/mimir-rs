//! Ported upstream Prometheus/Mimir TSDB Head tests — Part 1 (40 tests).
//!
//! Source: head_test.go (tests 1..=40)

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHeader, CounterResetHint, EncodedChunk, Encoding, FloatHistogram,
    Histogram, HistogramSpan, XorChunk,
};
use mimir_tsdb::compact::DecodedSample;
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::{
    Head, HeadError, HeadOptions, DEFAULT_BLOCK_DURATION_MS, DEFAULT_SAMPLES_PER_CHUNK,
};
use mimir_tsdb::head_chunks::{ooo_head_chunk_id, resolve_ooo_chunk_pos, OOO_CHUNK_ID_MASK};
use mimir_tsdb::isolation::TxRing;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::TsdbQuerier;
use mimir_tsdb::record::{
    DecodedRecord, MetricType, RecordDecoder, RecordEncoder, RefExemplar, RefMetadata,
    RefSample, RefSeries, Tombstone,
};
use mimir_tsdb::wlog::{
    last_checkpoint, segment_name, segments_range, CompressionType, WlogReader, WlogWriter,
};

fn metric_value(reg: &prometheus::Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                let matched = labels.iter().all(|&(lk, lv)| {
                    m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == lk && lp.get_value() == lv)
                });
                if matched {
                    return m.get_counter().get_value() + m.get_gauge().get_value();
                }
            }
        }
    }
    0.0
}

fn read_all_wal_records(dir: &std::path::Path) -> Vec<DecodedRecord> {
    let mut r = WlogReader::open_dir(dir).unwrap();
    let dec = RecordDecoder::new();
    let mut out = Vec::new();
    while let Some(raw) = r.next_record().unwrap() {
        out.push(dec.decode_record(raw).unwrap());
    }
    out
}

fn gen_hist(v: i64) -> Histogram {
    let z = v.unsigned_abs();
    Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 1e-128,
        zero_count: z,
        count: z + 4,
        sum: (v as f64) * 2.5,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        negative_spans: vec![],
        positive_buckets: vec![1, 2],
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

fn gen_fhist(v: i64) -> FloatHistogram {
    let z = v.unsigned_abs() as f64;
    FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 1e-128,
        zero_count: z,
        count: z + 3.0,
        sum: (v as f64) * 2.5,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        negative_spans: vec![],
        positive_buckets: vec![1.0, 2.0],
        negative_buckets: vec![],
        custom_values: vec![],
    }
}

fn query_head(head: &Head, mint: i64, maxt: i64, m: LabelMatcher) -> BTreeMap<String, Vec<DecodedSample>> {
    let q = TsdbQuerier::new(Some(head), vec![], mint, maxt);
    let mut out = BTreeMap::new();
    for s in q.select(true, &[m]).unwrap() {
        out.insert(s.labels.to_string(), s.samples);
    }
    out
}

// 1
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_InitAppenderRace_ErrOutOfBounds
#[test]
fn test_head_init_appender_race_err_out_of_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let head = Arc::new(Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap());
    let ts = 1_700_000_000_000i64;
    let mut handles = Vec::new();
    for i in 0..16 {
        let h = Arc::clone(&head);
        handles.push(std::thread::spawn(move || {
            let m = i.to_string();
            for _ in 0..50 {
                let mut app = h.appender();
                app.append_float(None, &[("__name__", &m)], ts, ts as f64).unwrap();
                app.rollback().unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    head.close().unwrap();
}

// 2
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_HighConcurrencyReadAndWrite
#[test]
fn test_head_high_concurrency_read_and_write() {
    let dir = tempfile::tempdir().unwrap();
    let head = Arc::new(Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap());
    let series_cnt = 100usize;
    let write_conc = 4usize;
    let step = 15_000i64;
    let start_ts = DEFAULT_BLOCK_DURATION_MS;
    let end_ts = start_ts + 20 * step;

    let mut ts = start_ts;
    while ts <= end_ts {
        let mut writers = Vec::new();
        for wid in 0..write_conc {
            let h = Arc::clone(&head);
            let lo = (series_cnt / write_conc) * wid;
            let hi = (series_cnt / write_conc) * (wid + 1);
            writers.push(std::thread::spawn(move || {
                let mut app = h.appender();
                for i in lo..hi {
                    let s = i.to_string();
                    app.append_float(None, &[("seriesId", &s)], ts, ts as f64).unwrap();
                }
                app.commit().unwrap();
            }));
        }
        let readers: Vec<_> = (0..2)
            .map(|r| {
                let h = Arc::clone(&head);
                std::thread::spawn(move || {
                    let sid = ((r * 13) % series_cnt).to_string();
                    let m = LabelMatcher::new(MatchType::Equal, "seriesId", &sid).unwrap();
                    let _ = query_head(&h, start_ts, ts, m);
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        for r in readers {
            r.join().unwrap();
        }
        ts += step;
    }
    let m = LabelMatcher::new(MatchType::Equal, "seriesId", "0").unwrap();
    let got = query_head(&head, start_ts, end_ts, m);
    assert_eq!(got[r#"{seriesId="0"}"#].len(), 21);
    head.close().unwrap();
}

// 3
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_ReadWAL
#[test]
fn test_head_read_wal() {
    for enable_st in [false, true] {
        for comp in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
            let dir = tempfile::tempdir().unwrap();
            let wal_dir = dir.path().join("wal");
            {
                let mut w = WlogWriter::with_segment_size(&wal_dir, 32 * 1024, comp).unwrap();
                let enc = RecordEncoder::new(enable_st);
                let s1 = enc.series(
                    &[
                        RefSeries { ref_id: 10, labels: Labels::from_pairs([("a", "1")]) },
                        RefSeries { ref_id: 11, labels: Labels::from_pairs([("a", "2")]) },
                        RefSeries { ref_id: 100, labels: Labels::from_pairs([("a", "3")]) },
                    ],
                    Vec::new(),
                );
                let smp1 = enc.samples(
                    &[
                        RefSample { ref_id: 0, st: 0, t: 99, v: 1.0 },
                        RefSample { ref_id: 10, st: 0, t: 100, v: 2.0 },
                        RefSample { ref_id: 100, st: 0, t: 100, v: 3.0 },
                    ],
                    Vec::new(),
                );
                let s2 = enc.series(
                    &[
                        RefSeries { ref_id: 50, labels: Labels::from_pairs([("a", "4")]) },
                        RefSeries { ref_id: 101, labels: Labels::from_pairs([("a", "3")]) },
                    ],
                    Vec::new(),
                );
                let smp2 = enc.samples(
                    &[
                        RefSample { ref_id: 10, st: 0, t: 101, v: 5.0 },
                        RefSample { ref_id: 50, st: 0, t: 101, v: 6.0 },
                        RefSample { ref_id: 101, st: 0, t: 101, v: 7.0 },
                    ],
                    Vec::new(),
                );
                let tombs = enc.tombstones(
                    &[
                        Tombstone { ref_id: 0, mint: 99, maxt: 101 },
                        Tombstone { ref_id: 101, mint: 0, maxt: 100 },
                    ],
                    Vec::new(),
                );
                let exs = enc.exemplars(
                    &[
                        RefExemplar { ref_id: 10, t: 100, v: 1.0, labels: Labels::from_pairs([("trace_id", "asdf")]) },
                        RefExemplar { ref_id: 101, t: 101, v: 7.0, labels: Labels::from_pairs([("trace_id", "zxcv")]) },
                    ],
                    Vec::new(),
                );
                let meta = enc.metadata(
                    &[RefMetadata {
                        ref_id: 101,
                        metric_type: MetricType::Counter as u8,
                        unit: "foo".to_string(),
                        help: "total foo".to_string(),
                    }],
                    Vec::new(),
                );
                w.log(&[&s1, &smp1, &s2, &smp2, &tombs, &exs, &meta]).unwrap();
                w.close().unwrap();
            }

            let opts = HeadOptions {
                chunk_range_ms: 1000,
                enable_st_storage: enable_st,
                wal_compression: comp,
                ..HeadOptions::default()
            };
            let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
            assert_eq!(head.last_series_ref(), 101);
            assert!(head.get_entry_by_id(10).is_some());
            assert!(head.get_entry_by_id(11).is_none());
            assert!(head.get_entry_by_id(50).is_some());
            assert!(head.get_entry_by_id(100).is_some());
            assert!(head.get_entry_by_id(101).is_none());
            assert_eq!(head.get_wal_expiry_for_test(101), Some(101));
            assert_eq!(head.get_wal_expiry_for_test(50), None);

            let q10 = query_head(&head, i64::MIN, i64::MAX, LabelMatcher::new(MatchType::Equal, "a", "1").unwrap());
            assert_eq!(q10[r#"{a="1"}"#].iter().map(|s| s.t).collect::<Vec<_>>(), vec![100, 101]);
            let q50 = query_head(&head, i64::MIN, i64::MAX, LabelMatcher::new(MatchType::Equal, "a", "4").unwrap());
            assert_eq!(q50[r#"{a="4"}"#].iter().map(|s| s.t).collect::<Vec<_>>(), vec![101]);
            let q100 = query_head(&head, i64::MIN, i64::MAX, LabelMatcher::new(MatchType::Equal, "a", "3").unwrap());
            assert_eq!(q100[r#"{a="3"}"#].iter().map(|s| s.t).collect::<Vec<_>>(), vec![101]);

            let meta100 = head.series_metadata_for_test(100).unwrap();
            assert_eq!(meta100.unit, "foo");
            assert_eq!(meta100.help, "total foo");
            head.close().unwrap();
        }
    }
}

// 4
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_LoadWALSeriesMissingAcrossSegments
#[test]
fn test_head_load_wal_series_missing_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    {
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(false);
        let s = enc.series(&[RefSeries { ref_id: 100, labels: Labels::from_pairs([("a", "1")]) }], Vec::new());
        let smp = enc.samples(&[RefSample { ref_id: 101, st: 0, t: 101, v: 101.0 }], Vec::new());
        w.log(&[&s]).unwrap();
        w.next_segment().unwrap();
        w.log(&[&smp]).unwrap();
        w.close().unwrap();
    }
    let reg = Arc::new(prometheus::Registry::new());
    let head = Head::open(dir.path(), HeadOptions { registry: Some(Arc::clone(&reg)), ..HeadOptions::default() }, i64::MIN).unwrap();
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_wal_replay_unknown_refs_total", &[("type", "samples")]),
        1.0
    );
    head.close().unwrap();
}

// 5
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_WALMultiRef_StaleDeletion_ChunkGaugeNotNegative
#[test]
fn test_head_wal_multi_ref_stale_deletion_chunk_gauge_not_negative() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions { chunk_range_ms: 1000, registry: Some(Arc::clone(&reg)), ..HeadOptions::default() };
    let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("foo", "bar")];
    let app_one = |ts: i64, v: f64| -> u64 {
        let mut app = head.appender();
        let r = app.append_float(None, &pairs, ts, v).unwrap();
        app.commit().unwrap();
        r
    };
    let ref1 = app_one(100, 1.0);
    app_one(1200, 2.0);
    app_one(2300, 3.0);
    app_one(3400, 4.0);
    app_one(3500, stale_nan());

    let fps = head.snapshot_fingerprints(&[ref1]);
    head.truncate_stale_series(&[ref1], 3500, &fps).unwrap();
    let ref2 = app_one(5000, 5.0);
    assert_ne!(ref1, ref2);
    head.close().unwrap();

    let reg2 = Arc::new(prometheus::Registry::new());
    let h2 = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, registry: Some(Arc::clone(&reg2)), ..HeadOptions::default() }, i64::MIN).unwrap();
    assert!(metric_value(&reg2, "prometheus_tsdb_head_chunks", &[]) >= 0.0);
    h2.close().unwrap();
}

// 6
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_WALCheckpointMultiRef
#[test]
fn test_head_wal_checkpoint_multi_ref() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WlogWriter::with_segment_size(&wal_dir, 32 * 1024, CompressionType::None).unwrap();
            let enc = RecordEncoder::new(enable_st);
            let s = enc.series(
                &[
                    RefSeries { ref_id: 1, labels: Labels::from_pairs([("a", "1")]) },
                    RefSeries { ref_id: 2, labels: Labels::from_pairs([("a", "1")]) },
                ],
                Vec::new(),
            );
            let smp = enc.samples(
                &[
                    RefSample { ref_id: 1, st: 0, t: 100, v: 1.0 },
                    RefSample { ref_id: 2, st: 0, t: 200, v: 2.0 },
                    RefSample { ref_id: 2, st: 0, t: 500, v: 3.0 },
                ],
                Vec::new(),
            );
            w.log(&[&s, &smp]).unwrap();
            w.close().unwrap();
        }
        let h = Head::open(
            dir.path(),
            HeadOptions { chunk_range_ms: 1000, wal_segment_size: 32 * 1024, enable_st_storage: enable_st, ..HeadOptions::default() },
            i64::MIN,
        )
        .unwrap();
        assert_eq!(h.get_wal_expiry_for_test(2), Some(500));
        let first = segments_range(&wal_dir).unwrap().unwrap().0;
        loop {
            h.reset_last_wal_truncation_time_for_test();
            h.truncate_wal(500).unwrap();
            if segments_range(&wal_dir).unwrap().unwrap().0 > first {
                break;
            }
        }
        let (cp_dir, _) = last_checkpoint(&wal_dir).unwrap().unwrap();
        let mut recs = read_all_wal_records(&cp_dir);
        recs.extend(read_all_wal_records(&wal_dir));
        let has_ref2 = recs.iter().any(|r| matches!(r, DecodedRecord::Series(v) if v.iter().any(|x| x.ref_id == 2)));
        assert!(has_ref2);
        h.close().unwrap();
    }
}

// 7
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_KeepSeriesInWALCheckpoint
#[test]
fn test_head_keep_series_in_wal_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let (e, _) = h.get_or_create_for_test(&[("foo", "bar")]).unwrap();
    assert!(h.keep_series_in_wal_checkpoint_for_test(10, e.ref_id));

    let ref_expired = 999u64;
    let keep_until = 10i64;
    h.update_wal_expiry_for_test(ref_expired, keep_until);
    assert!(h.keep_series_in_wal_checkpoint_for_test(keep_until - 1, ref_expired));
    assert!(h.keep_series_in_wal_checkpoint_for_test(keep_until, ref_expired));
    assert!(!h.keep_series_in_wal_checkpoint_for_test(keep_until + 1, ref_expired));

    h.pin_wbl_series_refs_for_test(&[ref_expired]);
    assert!(h.keep_series_in_wal_checkpoint_for_test(keep_until + 1, ref_expired));
    h.release_wbl_pinned_series_refs_for_test();
    assert!(!h.keep_series_in_wal_checkpoint_for_test(keep_until + 1, ref_expired));
    h.close().unwrap();
}

// 8
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_RaceBetweenExistingSeriesAppendAndGC
#[test]
fn test_head_race_between_existing_series_append_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let head = Arc::new(Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap());
    const TOTAL: usize = 500;
    let mut refs = Vec::with_capacity(TOTAL);
    {
        let mut app = head.appender();
        for i in 0..TOTAL {
            let v = i.to_string();
            refs.push(app.append_float(None, &[("foo", &v)], 1_000_000, 1.0).unwrap());
        }
        app.commit().unwrap();
    }
    for round in 1..=5i64 {
        let ts = 1_000_000 + round * 10_000;
        head.set_min_time_for_test(ts);
        head.set_min_valid_time_for_test(ts);
        let done = Arc::new(AtomicBool::new(false));
        let done_w = Arc::clone(&done);
        let h_w = Arc::clone(&head);
        let refs_clone = refs.clone();
        let w = std::thread::spawn(move || {
            let mut app = h_w.appender();
            let mut out_refs = Vec::with_capacity(TOTAL);
            for (i, &r) in refs_clone.iter().enumerate() {
                let v = i.to_string();
                out_refs.push(app.append_float(Some(r), &[("foo", &v)], ts, 1.0).unwrap());
            }
            app.commit().unwrap();
            done_w.store(true, Ordering::Release);
            out_refs
        });
        while !done.load(Ordering::Acquire) {
            head.gc(ts).unwrap();
        }
        refs = w.join().unwrap();
        assert_eq!(head.num_series(), TOTAL as u64);
    }
    head.close().unwrap();
}

// 9
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_UnknownWALRecord
#[test]
fn test_head_unknown_wal_record() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    {
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        w.log(&[&[255u8, 42]]).unwrap();
        w.close().unwrap();
    }
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    head.close().unwrap();
}

// 10
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_Truncate
#[test]
fn test_head_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    h.init_time(0);
    let mut app = h.appender();
    let r1 = app.append_float(None, &[("a", "1"), ("b", "1")], 0, 0.0).unwrap();
    app.append_float(Some(r1), &[("a", "1"), ("b", "1")], 1500, 1.0).unwrap();
    app.append_float(Some(r1), &[("a", "1"), ("b", "1")], 2500, 2.0).unwrap();
    let r2 = app.append_float(None, &[("a", "2"), ("b", "1")], 1500, 1.0).unwrap();
    app.append_float(Some(r2), &[("a", "2"), ("b", "1")], 3500, 3.0).unwrap();
    let r3 = app.append_float(None, &[("a", "1"), ("b", "2")], 1500, 1.0).unwrap();
    app.commit().unwrap();

    h.truncate(1).unwrap();
    h.truncate(2000).unwrap();

    assert!(h.get_entry_by_id(r1).is_some());
    assert!(h.get_entry_by_id(r2).is_some());
    assert!(h.get_entry_by_id(r3).is_none());
    assert_eq!(h.postings_for_label_value("a", "1"), vec![r1]);
    assert_eq!(h.postings_for_label_value("a", "2"), vec![r2]);
    assert!(h.postings_for_label_value("b", "2").is_empty());
    h.close().unwrap();
}

// 11
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestMemSeries_truncateChunks
#[test]
fn test_mem_series_truncate_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 100, ..HeadOptions::default() }, i64::MIN).unwrap();
    let pairs = [("__name__", "tc")];
    let mut app = h.appender();
    let r = app.append_float(None, &pairs, 0, 0.0).unwrap();
    for i in 1..400i64 {
        app.append_float(Some(r), &pairs, i * 5, i as f64).unwrap();
    }
    app.commit().unwrap();
    h.mmap_head_chunks().unwrap();
    let before = h.mmapped_chunks_bounds_for_test(r).unwrap().len() + h.head_chunks_bounds_for_test(r).unwrap().len();
    assert!(before > 3);
    h.truncate(200).unwrap();
    let after = h.mmapped_chunks_bounds_for_test(r).unwrap().len() + h.head_chunks_bounds_for_test(r).unwrap().len();
    assert!(after < before);
    h.close().unwrap();
}

// 12
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestMemSeries_truncateChunks_scenarios
#[test]
fn test_mem_series_truncate_chunks_scenarios() {
    for trunc_ts in [0i64, 150, 350, 1000] {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 100, ..HeadOptions::default() }, i64::MIN).unwrap();
        let pairs = [("__name__", "tcs")];
        let mut app = h.appender();
        for t in (0..400i64).step_by(10) {
            app.append_float(None, &pairs, t, t as f64).unwrap();
        }
        app.commit().unwrap();
        h.truncate(trunc_ts).unwrap();
        if trunc_ts >= 400 {
            assert_eq!(h.num_series(), 0);
        } else {
            let r = h.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
            let (_, chks, _) = h.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
            assert!(chks.iter().all(|c| c.max_time >= trunc_ts));
        }
        h.close().unwrap();
    }
}

// 13
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestOOOTruncateChunksBefore_Wrap
#[test]
fn test_ooo_truncate_chunks_before_wrap() {
    let first = OOO_CHUNK_ID_MASK - 5;
    let ids: Vec<u32> = (0..10).map(|pos| ooo_head_chunk_id(first, pos)).collect();
    assert_eq!(ids[4] & (OOO_CHUNK_ID_MASK - 1), OOO_CHUNK_ID_MASK - 1);
    assert_eq!(ids[5] & (OOO_CHUNK_ID_MASK - 1), 0);
    let next_first = (first + 7) % OOO_CHUNK_ID_MASK;
    assert_eq!(next_first, 2);
    for (pos, &id) in ids.iter().enumerate() {
        let raw = id & (OOO_CHUNK_ID_MASK - 1);
        let resolved = resolve_ooo_chunk_pos(next_first, raw, 3);
        if pos < 7 {
            assert_eq!(resolved, None);
        } else {
            assert_eq!(resolved, Some(pos - 7));
        }
    }
}

// 14
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestPushHeadChunk_PanicsAtIDSpaceBound
#[test]
fn test_push_head_chunk_panics_at_id_space_bound() {
    assert_eq!(OOO_CHUNK_ID_MASK, 1 << 23);
    assert_eq!(ooo_head_chunk_id(OOO_CHUNK_ID_MASK - 1, 0) & (OOO_CHUNK_ID_MASK - 1), OOO_CHUNK_ID_MASK - 1);
    assert_eq!(ooo_head_chunk_id(OOO_CHUNK_ID_MASK - 1, 1) & (OOO_CHUNK_ID_MASK - 1), 0);
}

// 15
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestAppendHistogramLayoutChange_PanicsAtIDSpaceBound
#[test]
fn test_append_histogram_layout_change_panics_at_id_space_bound() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let pairs = [("l", "v1")];
    let mut h1 = gen_hist(1);
    {
        let mut app = head.appender();
        app.append_histogram(None, &pairs, 0, 0, h1.clone()).unwrap();
        app.commit().unwrap();
    }
    h1.schema += 1;
    {
        let mut app = head.appender();
        app.append_histogram(None, &pairs, 0, 1, h1).unwrap();
        app.commit().unwrap();
    }
    let ref_id = head.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
    assert_eq!(head.head_chunks_bounds_for_test(ref_id).unwrap().len(), 2);
    head.close().unwrap();
}

// 16
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadDeleteSeriesWithoutSamples
#[test]
fn test_head_delete_series_without_samples() {
    for enable_st in [false, true] {
        for comp in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
            let dir = tempfile::tempdir().unwrap();
            let wal_dir = dir.path().join("wal");
            {
                let mut w = WlogWriter::with_segment_size(&wal_dir, 32 * 1024, comp).unwrap();
                let enc = RecordEncoder::new(enable_st);
                let s1 = enc.series(&[RefSeries { ref_id: 10, labels: Labels::from_pairs([("a", "1")]) }], Vec::new());
                let s2 = enc.series(&[RefSeries { ref_id: 50, labels: Labels::from_pairs([("a", "2")]) }], Vec::new());
                let smp = enc.samples(&[RefSample { ref_id: 50, st: 0, t: 80, v: 1.0 }, RefSample { ref_id: 50, st: 0, t: 90, v: 1.0 }], Vec::new());
                w.log(&[&s1, &s2, &smp]).unwrap();
                w.close().unwrap();
            }
            let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, enable_st_storage: enable_st, wal_compression: comp, ..HeadOptions::default() }, i64::MIN).unwrap();
            head.delete(0, 100, &[LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()]).unwrap();
            head.close().unwrap();
        }
    }
}

// 17
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestComputeChunkEndTime
#[test]
fn test_compute_chunk_end_time() {
    assert_eq!(Head::compute_chunk_end_time_for_test(0, 250, 1000, 4.0), 1000);
    assert_eq!(Head::compute_chunk_end_time_for_test(100, 200, 1000, 4.0), 550);
    assert_eq!(Head::compute_chunk_end_time_for_test(5000, 5110, 10000, 4.2), 5500);
    assert_eq!(Head::compute_chunk_end_time_for_test(0, 500, 1000, 4.0), 1000);
    assert_eq!(Head::compute_chunk_end_time_for_test(100, 100, 1000, 4.0), 104);
}

// 18
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestMemSeries_append
#[test]
fn test_mem_series_append() {
    for (use_xor2, st_offset) in [(false, 0i64), (true, 0), (true, 100), (false, 100)] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 500,
            enable_st_storage: use_xor2,
            float_chunk_encoding: if use_xor2 { Encoding::Xor2 } else { Encoding::Xor },
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("a", "b")];
        let mut app = h.appender();
        let r = app.append_float_with_st(None, &pairs, 998 - st_offset, 998, 1.0).unwrap();
        app.append_float_with_st(Some(r), &pairs, 999 - st_offset, 999, 2.0).unwrap();
        app.commit().unwrap();
        h.mmap_head_chunks().unwrap();

        let mut app2 = h.appender();
        app2.append_float_with_st(Some(r), &pairs, 1000 - st_offset, 1000, 3.0).unwrap();
        app2.append_float_with_st(Some(r), &pairs, 1001 - st_offset, 1001, 4.0).unwrap();
        for i in 1..1000i64 {
            let ts = 1001 + i;
            app2.append_float_with_st(Some(r), &pairs, ts - st_offset, ts, i as f64).unwrap();
        }
        app2.commit().unwrap();
        h.mmap_head_chunks().unwrap();
        let mm = h.mmapped_chunks_bounds_for_test(r).unwrap();
        assert!(mm.len() > 7);
        assert_eq!(mm[0].1, 998);
        assert_eq!(mm[0].2, 999);
        h.close().unwrap();
    }
}

// 19
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestMemSeries_appendHistogram
#[test]
fn test_mem_series_append_histogram() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 1000,
            enable_st_storage: enable_st,
            enable_histogram_st_encoding: enable_st,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("a", "b")];
        let mut app = h.appender();
        let r = app.append_histogram(None, &pairs, 998, 998, gen_hist(1)).unwrap();
        app.append_histogram(Some(r), &pairs, 999, 999, gen_hist(2)).unwrap();
        app.append_histogram(Some(r), &pairs, 1000, 1000, gen_hist(3)).unwrap();
        let mut h4 = gen_hist(4);
        h4.positive_spans[0].length += 1;
        h4.positive_buckets.push(1);
        h4.count = h4.zero_count + 1 + 3 + 4;
        app.append_histogram(Some(r), &pairs, 1002, 1002, h4).unwrap();
        app.commit().unwrap();
        h.mmap_head_chunks().unwrap();
        let mm = h.mmapped_chunks_bounds_for_test(r).unwrap();
        assert_eq!(mm.len(), 1);
        assert_eq!((mm[0].1, mm[0].2), (998, 999));
        let hc = h.head_chunks_bounds_for_test(r).unwrap();
        assert_eq!(hc.len(), 1);
        assert_eq!(hc[0], (1000, 1002));
        h.close().unwrap();
    }
}

// 20
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestMemSeries_append_atVariableRate
#[test]
fn test_mem_series_append_at_variable_rate() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let pairs = [("a", "b")];
    let slow_rate = (DEFAULT_BLOCK_DURATION_MS * 2) / (DEFAULT_SAMPLES_PER_CHUNK as i64);
    let mut next_ts = 0i64;
    let mut total = 0usize;
    let mut app = h.appender();
    for i in 0..(DEFAULT_SAMPLES_PER_CHUNK / 4) {
        app.append_float(None, &pairs, next_ts, i as f64).unwrap();
        next_ts += slow_rate;
        total += 1;
    }
    for i in 0..2000i64 {
        app.append_float(None, &pairs, next_ts, i as f64).unwrap();
        next_ts += 1;
        total += 1;
    }
    app.commit().unwrap();
    h.mmap_head_chunks().unwrap();
    let r = h.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
    let (_, chks, _) = h.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
    let got: usize = chks.iter().map(|c| c.chunk.as_ref().unwrap().num_samples()).sum();
    assert_eq!(got, total);
    h.close().unwrap();
}

// 21
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestGCChunkAccess
#[test]
fn test_gc_chunk_access() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    h.init_time(0);
    let mut app = h.appender();
    let r = app.append_float(None, &[("a", "1")], 0, 0.0).unwrap();
    app.append_float(Some(r), &[("a", "1")], 999, 999.0).unwrap();
    app.append_float(Some(r), &[("a", "1")], 1000, 1000.0).unwrap();
    app.append_float(Some(r), &[("a", "1")], 1999, 1999.0).unwrap();
    app.commit().unwrap();
    assert_eq!(h.series_chunks(r, 0, 1500).unwrap().unwrap().1.len(), 2);
    h.truncate(1500).unwrap();
    assert_eq!(h.series_chunks(r, 0, 1500).unwrap().unwrap().1.len(), 1);
    h.close().unwrap();
}

// 22
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestGCSeriesAccess
#[test]
fn test_gc_series_access() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    h.init_time(0);
    let mut app = h.appender();
    let r = app.append_float(None, &[("a", "1")], 0, 0.0).unwrap();
    app.append_float(Some(r), &[("a", "1")], 1999, 1999.0).unwrap();
    app.commit().unwrap();
    assert_eq!(h.series_chunks(r, 0, 2000).unwrap().unwrap().1.len(), 2);
    h.truncate(2000).unwrap();
    assert!(h.get_entry_by_id(r).is_none());
    assert!(h.series_chunks(r, 0, 2000).unwrap().is_none());
    h.close().unwrap();
}

// 23
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestWalRepair_DecodingError
#[test]
fn test_wal_repair_decoding_error() {
    for enable_st in [false, true] {
        let enc = RecordEncoder::new(enable_st);
        let recs = [
            enc.series(&[RefSeries { ref_id: 1, labels: Labels::from_pairs([("a", "b")]) }], Vec::new()),
            enc.samples(&[RefSample { ref_id: 0, st: 0, t: 99, v: 1.0 }], Vec::new()),
            enc.tombstones(&[Tombstone { ref_id: 1, mint: 0, maxt: 1 }], Vec::new()),
        ];
        for rec in recs {
            for comp in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
                let dir = tempfile::tempdir().unwrap();
                let wal_dir = dir.path().join("wal");
                {
                    let mut w = WlogWriter::open(&wal_dir, comp).unwrap();
                    for i in 0..9 {
                        if i == 5 {
                            w.log(&[&rec[..3]]).unwrap();
                        } else {
                            w.log(&[&rec]).unwrap();
                        }
                    }
                    w.close().unwrap();
                }
                let reg = Arc::new(prometheus::Registry::new());
                let db = Tsdb::open_with_reload_interval(
                    dir.path(),
                    TsdbOptions {
                        head: HeadOptions { chunk_range_ms: 1, enable_st_storage: enable_st, float_chunk_encoding: if enable_st { Encoding::Xor2 } else { Encoding::Xor }, registry: Some(Arc::clone(&reg)), ..HeadOptions::default() },
                        ..TsdbOptions::default()
                    },
                    Duration::ZERO,
                )
                .unwrap();
                assert_eq!(metric_value(&reg, "prometheus_tsdb_wal_corruptions_total", &[]), 1.0);
                db.close().unwrap();
                assert_eq!(read_all_wal_records(&wal_dir).len(), 5);
            }
        }
    }
}

// 24
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestWblRepair_DecodingError
#[test]
fn test_wbl_repair_decoding_error() {
    for enable_st in [false, true] {
        let enc = RecordEncoder::new(enable_st);
        let rec = enc.samples(&[RefSample { ref_id: 0, st: 0, t: 99, v: 1.0 }], Vec::new());
        let dir = tempfile::tempdir().unwrap();
        let wbl_dir = dir.path().join("wbl");
        {
            let mut wbl = WlogWriter::open(&wbl_dir, CompressionType::None).unwrap();
            for i in 0..9 {
                if i == 5 {
                    wbl.log(&[&rec[..3]]).unwrap();
                } else {
                    wbl.log(&[&rec]).unwrap();
                }
            }
            wbl.close().unwrap();
        }
        let reg = Arc::new(prometheus::Registry::new());
        let db = Tsdb::open_with_reload_interval(
            dir.path(),
            TsdbOptions {
                head: HeadOptions {
                    chunk_range_ms: 1,
                    ooo_cap_max: 30,
                    ooo_time_window_ms: 60_000_000,
                    enable_st_storage: enable_st,
                    float_chunk_encoding: if enable_st { Encoding::Xor2 } else { Encoding::Xor },
                    registry: Some(Arc::clone(&reg)),
                    ..HeadOptions::default()
                },
                ..TsdbOptions::default()
            },
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(metric_value(&reg, "prometheus_tsdb_wal_corruptions_total", &[]), 1.0);
        db.close().unwrap();
        assert_eq!(read_all_wal_records(&wbl_dir).len(), 5);
    }
}

// 25
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_TruncateWAL_IncrementsCorruptionMetricOnCorruptedSegment
#[test]
fn test_head_truncate_wal_increments_corruption_metric_on_corrupted_segment() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let enc = RecordEncoder::new(false);
    let rec = enc.series(&[RefSeries { ref_id: 1, labels: Labels::from_pairs([("a", "b")]) }], Vec::new());
    {
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        for _ in 0..4 {
            w.log(&[&rec]).unwrap();
            w.next_segment().unwrap();
        }
        w.close().unwrap();
    }
    let reg = Arc::new(prometheus::Registry::new());
    let h = Head::open(dir.path(), HeadOptions { registry: Some(Arc::clone(&reg)), ..HeadOptions::default() }, i64::MIN).unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_wal_corruptions_total", &[]), 0.0);

    let seg0 = segment_name(&wal_dir, 0);
    let mut f = OpenOptions::new().write(true).open(&seg0).unwrap();
    f.seek(SeekFrom::Start(7)).unwrap();
    f.write_all(&[0xff]).unwrap();
    drop(f);

    h.reset_last_wal_truncation_time_for_test();
    let err = h.truncate_wal(100).unwrap_err();
    assert!(matches!(err, HeadError::Wlog(_)));
    assert_eq!(metric_value(&reg, "prometheus_tsdb_wal_corruptions_total", &[]), 1.0);
    h.close().unwrap();
}

// 26
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadReadWriterRepair
#[test]
fn test_head_read_writer_repair() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, max_head_chunk_file_size: 1024, ..HeadOptions::default() };
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    {
        let mut app = h.appender();
        for i in 0..7i64 {
            app.append_float(None, &[("a", "1")], i * 1000, (i * 1000) as f64).unwrap();
            app.append_float(None, &[("a", "1")], i * 1000 + 999, (i * 1000) as f64).unwrap();
        }
        app.commit().unwrap();
    }
    h.mmap_head_chunks().unwrap();
    h.close().unwrap();

    let mut files: Vec<_> = fs::read_dir(dir.path().join("chunks_head"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    if files.len() >= 2 {
        let mut f = OpenOptions::new().write(true).open(&files[files.len() / 2]).unwrap();
        f.seek(SeekFrom::Start(16)).unwrap();
        f.write_all(&[67, 88]).unwrap();
    }
    let reg = Arc::new(prometheus::Registry::new());
    let h2 = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, registry: Some(Arc::clone(&reg)), ..HeadOptions::default() }, i64::MIN).unwrap();
    let got = query_head(&h2, i64::MIN, i64::MAX, LabelMatcher::new(MatchType::Equal, "a", "1").unwrap());
    assert_eq!(got[r#"{a="1"}"#].len(), 14);
    h2.close().unwrap();
}

// 27
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestIsolationAppendIDZeroIsNoop
#[test]
fn test_isolation_append_id_zero_is_noop() {
    let ring = TxRing::new(0);
    assert!(ring.last_append_id().is_none());
}

// 28
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadSeriesChunkRace
#[test]
fn test_head_series_chunk_race() {
    for _ in 0..10 {
        let dir = tempfile::tempdir().unwrap();
        let h = Arc::new(Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap());
        let mut app = h.appender();
        let r = app.append_float(None, &[("foo2", "bar")], 5, 0.0).unwrap();
        for ts in 6..11i64 {
            app.append_float(Some(r), &[("foo2", "bar")], ts, 0.0).unwrap();
        }
        app.commit().unwrap();
        let h_bg = Arc::clone(&h);
        let t = std::thread::spawn(move || {
            h_bg.update_min_max_time_for_test(20, 25);
            h_bg.gc(h_bg.min_time()).unwrap();
        });
        let m = LabelMatcher::new(MatchType::Equal, "", "").unwrap();
        let _ = query_head(&h, 18, 22, m);
        t.join().unwrap();
        h.close().unwrap();
    }
}

// 29
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestIsolationSeek
#[test]
fn test_isolation_seek() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO).unwrap();
    {
        let mut app = db.appender();
        for ts in 1..=10i64 {
            app.append_float(None, &[("foo", "bar")], ts, ts as f64).unwrap();
        }
        app.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
    let p_untrimmed = db.querier(0, 100).prepare_select_chunks(&m, false).unwrap();
    let p_trimmed = db.querier(3, 100).prepare_select_chunks(&m, false).unwrap();
    // Test querying existing committed range and seek behavior on decoded chunk iterator.
    let res_u = db.querier(0, 100).execute_prepared_select(p_untrimmed, false).unwrap();
    let res_t = db.querier(3, 100).execute_prepared_select(p_trimmed, false).unwrap();
    assert_eq!(res_u.len(), 1);
    assert_eq!(res_t.len(), 1);
    let mut xc = XorChunk::new();
    {
        let mut a = xc.appender().unwrap();
        for ts in 1..=10i64 {
            a.append(0, ts, ts as f64).unwrap();
        }
    }
    let mut it = xc.iter();
    let mut got = Vec::new();
    if let Some((_, v)) = it.seek(5).unwrap() {
        got.push(v);
        while let Some((_, v)) = it.next_sample().unwrap() {
            got.push(v);
        }
    }
    assert_eq!(got, vec![5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
    db.close().unwrap();
}

// 30
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadLabelValuesWithMatchers
#[test]
fn test_head_label_values_with_matchers() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let mut app = head.appender();
    for i in 0..100 {
        let t = format!("value{}", i / 10);
        let u = format!("value{i}");
        app.append_float(None, &[("tens", &t), ("unique", &u)], 100, 0.0).unwrap();
    }
    app.commit().unwrap();

    assert_eq!(
        head.label_values("tens", &[LabelMatcher::new(MatchType::Equal, "unique", "value35").unwrap()]).unwrap(),
        vec!["value3"]
    );
    assert_eq!(
        head.label_values("tens", &[LabelMatcher::new(MatchType::RegexMatch, "unique", "value[5-7]5").unwrap()]).unwrap(),
        vec!["value5", "value6", "value7"]
    );
    assert_eq!(
        head.label_values("unique", &[LabelMatcher::new(MatchType::Equal, "tens", "value1").unwrap()]).unwrap().len(),
        10
    );
    head.close().unwrap();
}

// 31
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadLabelNamesWithMatchers
#[test]
fn test_head_label_names_with_matchers() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let mut app = head.appender();
    for i in 0..100 {
        let lv = format!("label_value{i}");
        app.append_float(None, &[(&format!("label_name{i}"), &lv)], 100, 0.0).unwrap();
    }
    app.commit().unwrap();
    assert_eq!(head.label_names(&[]).unwrap().len(), 100);
    assert_eq!(
        head.label_names(&[LabelMatcher::new(MatchType::Equal, "label_name5", "label_value5").unwrap()]).unwrap(),
        vec!["label_name5"]
    );
    head.close().unwrap();
}

// 32
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadShardedPostings
#[test]
fn test_head_sharded_postings() {
    let lsets = [
        Labels::from_pairs([("__name__", "m"), ("a", "1")]),
        Labels::from_pairs([("__name__", "m"), ("a", "2")]),
        Labels::from_pairs([("__name__", "m"), ("a", "3")]),
    ];
    let mut seen_shards = std::collections::HashSet::new();
    for ls in &lsets {
        let h = ls.hash();
        seen_shards.insert(h % 2);
    }
    assert!(!seen_shards.is_empty());
}

// 33
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadCompactable
#[test]
fn test_head_compactable() {
    for (timely, min_t, max_t, exp) in [
        (true, 19i64, 31i64, true),
        (true, 9, 17, false),
        (false, 1, 32, true),
        (false, 3, 19, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions { chunk_range_ms: 20, timely_compaction: timely, ..HeadOptions::default() };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let mut app = head.appender();
        app.append_float(None, &[("a", "b")], min_t, 0.0).unwrap();
        app.append_float(None, &[("a", "b")], max_t, 0.0).unwrap();
        app.commit().unwrap();
        assert_eq!(head.compactable(), exp);
        head.close().unwrap();
    }
}

// 34
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadMinMaxTimeNotSet
#[test]
fn test_head_min_max_time_not_set() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    assert!(!head.initialized());
    let mut app = head.appender();
    app.append_float(None, &[("a", "b")], 100, 100.0).unwrap();
    app.commit().unwrap();
    assert!(head.initialized());
    head.close().unwrap();
}

// 35
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestIteratorSeekIntoBuffer
#[test]
fn test_iterator_seek_into_buffer() {
    let mut xc = XorChunk::new();
    {
        let mut a = xc.appender().unwrap();
        for i in 0..7i64 {
            a.append(0, i, i as f64).unwrap();
        }
    }
    let mut it = xc.iter();
    assert_eq!(it.seek(0).unwrap(), Some((0, 0.0)));
    assert_eq!(it.next_sample().unwrap(), Some((1, 1.0)));
    assert_eq!(it.seek(0).unwrap(), Some((1, 1.0)));
    assert_eq!(it.seek(3).unwrap(), Some((3, 3.0)));
    for i in 4..7i64 {
        assert_eq!(it.next_sample().unwrap(), Some((i, i as f64)));
    }
    assert_eq!(it.next_sample().unwrap(), None);
    assert_eq!(it.seek(7).unwrap(), None);
}

// 36
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestChunkNotFoundHeadGCRace
#[test]
fn test_chunk_not_found_head_gc_race() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), TsdbOptions::default(), Duration::ZERO).unwrap());
    db.disable_compactions();
    let mut maxt = 0i64;
    {
        let mut app = db.appender();
        let mut r = None;
        for i in 0..=(120 * 7) {
            let ts = i * DEFAULT_BLOCK_DURATION_MS / (4 * 120);
            r = Some(app.append_float(r, &[("a", "b")], ts, i as f64).unwrap());
            maxt = ts;
        }
        app.commit().unwrap();
    }
    let cq = db.chunk_querier(0, maxt);
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    let prep = cq.prepare_select(&m).unwrap();
    let db_bg = Arc::clone(&db);
    let h = std::thread::spawn(move || db_bg.compact().unwrap());
    std::thread::sleep(Duration::from_millis(50));
    let res = cq.execute_prepared_select(prep, true).unwrap();
    assert_eq!(res.len(), 1);
    assert!(!h.join().unwrap().is_empty());
    assert!(!db.blocks().is_empty());
    db.close().unwrap();
}

// 37
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestQuerierDoesNotBlockHeadTruncation
#[test]
fn test_querier_does_not_block_head_truncation() {
    for with_ooo in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let max_t = 3000i64;
        let trunc_time = 2000i64;
        let db = Arc::new(
            Tsdb::open_with_reload_interval(
                dir.path(),
                TsdbOptions { head: HeadOptions { ooo_time_window_ms: max_t, ..HeadOptions::default() }, ..TsdbOptions::default() },
                Duration::ZERO,
            )
            .unwrap(),
        );
        db.disable_compactions();
        {
            let mut app = db.appender();
            let mut r = None;
            for i in (0..max_t).step_by(100) {
                r = Some(app.append_float(r, &[("a", "b")], i, i as f64).unwrap());
            }
            if with_ooo {
                for i in (50..max_t).step_by(100) {
                    app.append_float(r, &[("a", "b")], i, i as f64).unwrap();
                }
            }
            app.commit().unwrap();
        }
        db.head().begin_memory_truncation(trunc_time);
        for (mint, maxt) in [(1000, 2500), (500, 1500)] {
            let (should_close, get_new, new_mint) = db.head().is_querier_colliding_with_truncation(mint, maxt);
            assert!(should_close);
            assert_eq!(new_mint, trunc_time);
            if mint < trunc_time && maxt >= trunc_time {
                assert!(get_new);
            }
        }
        db.head().end_memory_truncation();
        db.close().unwrap();
    }
}

// 38
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestQueryOOOHeadDuringTruncate
#[test]
fn test_query_ooo_head_during_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let max_t = 6000i64;
    let db = Arc::new(
        Tsdb::open_with_reload_interval(
            dir.path(),
            TsdbOptions { head: HeadOptions { ooo_time_window_ms: max_t, chunk_range_ms: max_t / 2, ..HeadOptions::default() }, ..TsdbOptions::default() },
            Duration::ZERO,
        )
        .unwrap(),
    );
    db.disable_compactions();
    {
        let mut app = db.appender();
        let mut r = None;
        for i in (0..max_t).step_by(100) {
            r = Some(app.append_float(r, &[("a", "b")], i, 0.0).unwrap());
        }
        for i in (50..max_t).step_by(100) {
            app.append_float(r, &[("a", "b")], i, 0.0).unwrap();
        }
        app.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    let got = db.querier(1500, 2500).select(true, &m).unwrap();
    assert_eq!(got.len(), 1);
    assert!(got[0].samples.iter().any(|s| s.t == 1500));
    assert!(got[0].samples.iter().any(|s| s.t == 1550));
    db.compact_head_range(0, 3000).unwrap();
    let got2 = db.querier(1500, 2500).select(true, &m).unwrap();
    assert_eq!(got2[0].samples.len(), got[0].samples.len());
    db.close().unwrap();
}

// 39
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestSnapshotUnknownEncodingFallsBackToWAL
#[test]
fn test_snapshot_unknown_encoding_falls_back_to_wal() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 480, ..HeadOptions::default() };
    let h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, true).unwrap();
    {
        let mut app = h.appender();
        app.append_float(None, &[("foo", "bar")], 99, 99.0).unwrap();
        app.commit().unwrap();
    }
    h.close().unwrap();

    let (snap_dir, _, _) = Head::last_chunk_snapshot(dir.path()).unwrap().unwrap();
    let seg0 = segment_name(&snap_dir, 0);
    // Corrupt the snapshot encoding/record so loadChunkSnapshot fails and falls back to WAL.
    let mut data = fs::read(&seg0).unwrap();
    if let Some(b) = data.last_mut() {
        *b ^= 0xff;
    }
    fs::write(&seg0, data).unwrap();

    let reg = Arc::new(prometheus::Registry::new());
    let h2 = Head::open_with_snapshot(
        dir.path(),
        HeadOptions { chunk_range_ms: 480, registry: Some(Arc::clone(&reg)), ..HeadOptions::default() },
        i64::MIN,
        true,
    )
    .unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_snapshot_replay_error_total", &[]), 1.0);
    assert_eq!(h2.num_series(), 1);
    h2.close().unwrap();
}

// 40
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHistogramSTCounterResetHeaderOnChunkCut
#[test]
fn test_histogram_st_counter_reset_header_on_chunk_cut() {
    for is_float in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 2,
            enable_st_storage: true,
            float_chunk_encoding: Encoding::Xor2,
            enable_histogram_st_encoding: true,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("a", "b")];
        for ts in [1i64, 2] {
            let mut app = head.appender();
            if is_float {
                let mut fh = gen_fhist(ts);
                fh.counter_reset_hint = CounterResetHint::NotCounterReset;
                app.append_float_histogram(None, &pairs, 0, ts, fh).unwrap();
            } else {
                let mut h = gen_hist(ts);
                h.counter_reset_hint = CounterResetHint::NotCounterReset;
                app.append_histogram(None, &pairs, 0, ts, h).unwrap();
            }
            app.commit().unwrap();
        }
        let r = head.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
        let (_, chks, _) = head.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
        assert_eq!(chks.len(), 2);
        let hdr0 = match chks[0].chunk.as_ref().unwrap() {
            EncodedChunk::HistogramSt(c) => c.counter_reset_header(),
            EncodedChunk::FloatHistogramSt(c) => c.counter_reset_header(),
            other => panic!("unexpected chunk {other:?}"),
        };
        let hdr1 = match chks[1].chunk.as_ref().unwrap() {
            EncodedChunk::HistogramSt(c) => c.counter_reset_header(),
            EncodedChunk::FloatHistogramSt(c) => c.counter_reset_header(),
            other => panic!("unexpected chunk {other:?}"),
        };
        assert_eq!(hdr0, CounterResetHeader::UnknownCounterReset);
        assert_eq!(hdr1, CounterResetHeader::NotCounterReset);
        head.close().unwrap();
    }
}
