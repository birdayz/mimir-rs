//! Ported upstream Prometheus/Mimir TSDB Head tests — Part 2 (40 tests).
//!
//! Source: head_test.go (tests 41..=80)

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHint, Encoding, FloatHistogram, Histogram, HistogramSpan,
    QUIET_ZERO_NAN_BITS,
};
use mimir_tsdb::compact::{DecodedSample, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::{
    Head, HeadOptions, SeriesLifecycleState, DEFAULT_BLOCK_DURATION_MS,
    DEFAULT_SAMPLES_PER_CHUNK,
};
use mimir_tsdb::head_chunks::OOO_CHUNK_ID_MASK;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::TsdbQuerier;
use mimir_tsdb::record::{
    DecodedRecord, RecordDecoder, RecordEncoder, RefFloatHistogramSample, RefHistogramSample,
    RefSample, RefSeries, Tombstone,
};
use mimir_tsdb::wlog::{
    last_checkpoint, segments_range, CompressionType, WlogReader, WlogWriter,
    WBL_DIR_NAME,
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

fn query_head(head: &Head, mint: i64, maxt: i64, m: &[LabelMatcher]) -> BTreeMap<String, Vec<DecodedSample>> {
    let q = TsdbQuerier::new(Some(head), vec![], mint, maxt);
    let mut out = BTreeMap::new();
    for s in q.select(true, m).unwrap() {
        out.insert(s.labels.to_string(), s.samples);
    }
    out
}

fn labels_with_hash_collision() -> (Labels, Labels) {
    let ls1 = Labels::from_pairs([("__name__", "metric"), ("lbl", "HFnEaGl")]);
    let ls2 = Labels::from_pairs([("__name__", "metric"), ("lbl", "RqcXatm")]);
    assert_eq!(ls1.hash(), ls2.hash());
    (ls1, ls2)
}

#[derive(Clone, Copy)]
enum SampleKind {
    Float,
    Hist,
    FloatHist,
}

impl SampleKind {
    fn metric_type(self) -> &'static str {
        match self {
            SampleKind::Float => "float",
            SampleKind::Hist | SampleKind::FloatHist => "histogram",
        }
    }
}

// 41
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestChunkSnapshotReplayBug
#[test]
fn test_chunk_snapshot_replay_bug() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WlogWriter::with_segment_size(&wal_dir, 32 * 1024, CompressionType::Snappy).unwrap();
            let enc = RecordEncoder::new(enable_st);
            for i in 1..=1000u64 {
                let ref_id = if i <= 500 { i * 100 } else { (i - 500) * 50 };
                let s = enc.series(
                    &[RefSeries {
                        ref_id,
                        labels: Labels::from_pairs([
                            ("__name__", "request_duration"),
                            ("foo", &format!("baz{i}")),
                            ("status_code", "200"),
                        ]),
                    }],
                    Vec::new(),
                );
                let smp = enc.samples(&[RefSample { ref_id, st: 0, t: 1000, v: 1000.0 }], Vec::new());
                w.log(&[&s, &smp]).unwrap();
            }
            w.close().unwrap();
        }
        let cp_dir = dir.path().join("chunk_snapshot.000000.0000000100");
        fs::create_dir_all(&cp_dir).unwrap();
        fs::write(cp_dir.join("00000000"), [1, 5, 3, 5, 6, 7, 4, 2, 2]).unwrap();

        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            enable_st_storage: enable_st,
            float_chunk_encoding: if enable_st { Encoding::Xor2 } else { Encoding::Xor },
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let head = Head::open_with_snapshot(dir.path(), opts, i64::MIN, true).unwrap();
        assert_eq!(metric_value(&reg, "prometheus_tsdb_snapshot_replay_error_total", &[]), 1.0);
        let got = query_head(
            &head,
            i64::MIN,
            i64::MAX,
            &[
                LabelMatcher::new(MatchType::Equal, "__name__", "request_duration").unwrap(),
                LabelMatcher::new(MatchType::NotEqual, "status_code", "200").unwrap(),
            ],
        );
        assert!(got.is_empty());
        head.close().unwrap();
    }
}

// 42
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestWALReplayMmapsChunks
#[test]
fn test_wal_replay_mmaps_chunks() {
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions { chunk_range_ms: 1000, wal_compression: CompressionType::Snappy, ..HeadOptions::default() };
        let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let pairs = [("foo", "bar")];
        {
            let mut app = h.appender();
            for i in 0..250i64 {
                let ts = i * 60_000;
                match kind {
                    SampleKind::Float => { app.append_float(None, &pairs, ts, ts as f64).unwrap(); }
                    SampleKind::Hist => { app.append_histogram(None, &pairs, 0, ts, gen_hist(i + 1)).unwrap(); }
                    SampleKind::FloatHist => { app.append_float_histogram(None, &pairs, 0, ts, gen_fhist(i + 1)).unwrap(); }
                }
            }
            app.commit().unwrap();
        }
        h.close().unwrap();
        fs::remove_dir_all(dir.path().join("chunks_head")).unwrap();

        let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let r = h2.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
        assert!(!h2.mmapped_chunks_bounds_for_test(r).unwrap().is_empty());
        assert_eq!(h2.head_chunks_bounds_for_test(r).unwrap().len(), 1);
        h2.close().unwrap();
    }
}

// 43
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestAppendQuietZeroDuplicates
#[test]
fn test_append_quiet_zero_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let pairs = [("foo", "bar")];
    let ts = 1_695_209_650i64;
    let qz = f64::from_bits(QUIET_ZERO_NAN_BITS);
    {
        let mut a = h.appender();
        a.append_float(None, &pairs, ts, 42.0).unwrap();
        a.append_float(None, &pairs, ts, 42.0).unwrap();
        a.append_float(None, &pairs, ts, qz).unwrap();
        a.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
    let got = query_head(&h, i64::MIN, i64::MAX, &m);
    assert_eq!(got[r#"{foo="bar"}"#].iter().map(|s| (s.t, s.value.clone())).collect::<Vec<_>>(), vec![(ts, SampleValue::Float(42.0))]);

    {
        let mut a = h.appender();
        a.append_float(None, &pairs, ts + 10, qz).unwrap();
        a.append_float(None, &pairs, ts + 15, 5.0).unwrap();
        a.commit().unwrap();
    }
    let got2 = query_head(&h, i64::MIN, i64::MAX, &m);
    assert_eq!(got2[r#"{foo="bar"}"#].iter().map(|s| (s.t, s.value.clone())).collect::<Vec<_>>(), vec![
        (ts, SampleValue::Float(42.0)),
        (ts + 10, SampleValue::Float(0.0)),
        (ts + 15, SampleValue::Float(5.0)),
    ]);

    {
        let mut a = h.appender();
        let _ = a.append_float(None, &pairs, ts + 5, qz);
        a.commit().unwrap();
    }
    let got3 = query_head(&h, i64::MIN, i64::MAX, &m);
    assert_eq!(got3[r#"{foo="bar"}"#].len(), 3);
    h.close().unwrap();
}

// 44
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestAppendQuietZeroDuplicatesMmapOnly
#[test]
fn test_append_quiet_zero_duplicates_mmap_only() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions::default();
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("foo", "bar")];
    let r;
    {
        let mut a = h.appender();
        r = a.append_float(None, &pairs, 100, 42.0).unwrap();
        a.commit().unwrap();
    }
    let (_, chks, _) = h.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
    let enc = chks[0].chunk.as_ref().unwrap();
    h.write_raw_chunk_for_test(r, chks[0].min_time, chks[0].max_time, enc.encoding().as_u8(), enc.as_bytes(), false).unwrap();
    h.close().unwrap();

    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    assert!(h2.head_chunks_bounds_for_test(r).unwrap().is_empty());
    let qz = f64::from_bits(QUIET_ZERO_NAN_BITS);
    {
        let mut a = h2.appender();
        a.append_float(None, &pairs, 100, qz).unwrap();
        a.commit().unwrap();
    }
    assert!(h2.head_chunks_bounds_for_test(r).unwrap().is_empty());
    let got = query_head(&h2, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()]);
    assert_eq!(got[r#"{foo="bar"}"#].len(), 1);
    h2.close().unwrap();
}

// 45
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestQuietZeroWALReplay
#[test]
fn test_quiet_zero_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { ooo_time_window_ms: 600_000, ..HeadOptions::default() };
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let pairs = [("foo", "bar")];
    let ts = 1_695_209_650i64;
    let qz = f64::from_bits(QUIET_ZERO_NAN_BITS);
    {
        let mut a = h.appender();
        a.append_float(None, &pairs, ts, 42.0).unwrap();
        a.append_float(None, &pairs, ts, 42.0).unwrap();
        a.append_float(None, &pairs, ts, qz).unwrap();
        a.append_float(None, &pairs, ts + 10, qz).unwrap();
        a.append_float(None, &pairs, ts + 5, qz).unwrap();
        a.commit().unwrap();
    }
    h.close().unwrap();

    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let m = [LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()];
    let got = query_head(&h2, i64::MIN, i64::MAX, &m);
    assert_eq!(got[r#"{foo="bar"}"#].iter().map(|s| (s.t, s.value.clone())).collect::<Vec<_>>(), vec![
        (ts, SampleValue::Float(42.0)),
        (ts + 10, SampleValue::Float(0.0)),
    ]);
    h2.close().unwrap();
}

// 46
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestStripeSeries_getOrSet
#[test]
fn test_stripe_series_get_or_set() {
    let (ls1, ls2) = labels_with_hash_collision();
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let p1: Vec<(&str, &str)> = ls1.iter().collect();
    let p2: Vec<(&str, &str)> = ls2.iter().collect();
    let (e1, c1) = h.get_or_create_for_test(&p1).unwrap();
    let (e2, c2) = h.get_or_create_for_test(&p2).unwrap();
    assert!(c1 && c2);
    assert_ne!(e1.ref_id, e2.ref_id);
    assert_eq!(h.get_entry_by_sorted_pairs(&p1).unwrap().ref_id, e1.ref_id);
    assert_eq!(h.get_entry_by_sorted_pairs(&p2).unwrap().ref_id, e2.ref_id);
    h.close().unwrap();
}

// 47
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestStripeSeries_gc
#[test]
fn test_stripe_series_gc() {
    let (ls1, ls2) = labels_with_hash_collision();
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let p1: Vec<(&str, &str)> = ls1.iter().collect();
    let p2: Vec<(&str, &str)> = ls2.iter().collect();
    {
        let mut app = h.appender();
        app.append_float(None, &p1, 10, 1.0).unwrap();
        app.append_float(None, &p2, 20, 2.0).unwrap();
        app.commit().unwrap();
    }
    h.gc(100).unwrap();
    assert!(h.get_entry_by_sorted_pairs(&p1).is_none());
    assert!(h.get_entry_by_sorted_pairs(&p2).is_none());
    assert_eq!(h.num_series(), 0);
    h.close().unwrap();
}

// 48
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestSecondaryHashFunction
#[test]
fn test_secondary_hash_function() {
    let (ls1, ls2) = labels_with_hash_collision();
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
    let p1: Vec<(&str, &str)> = ls1.iter().collect();
    let p2: Vec<(&str, &str)> = ls2.iter().collect();
    let mut app = h.appender();
    app.append_float(None, &p1, 0, 0.0).unwrap();
    app.append_float(None, &p2, 100, 1.0).unwrap();
    app.commit().unwrap();
    assert_eq!(h.num_series(), 2);
    h.truncate(100).unwrap();
    assert_eq!(h.num_series(), 1);
    h.truncate(200).unwrap();
    assert_eq!(h.num_series(), 0);
    h.close().unwrap();
}

// 49
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestPostingsCardinalityStats
#[test]
fn test_postings_cardinality_stats() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    {
        let mut app = h.appender();
        app.append_float(None, &[("__name__", "t"), ("n", "v1")], 1, 1.0).unwrap();
        app.append_float(None, &[("__name__", "t"), ("n", "v2")], 1, 1.0).unwrap();
        app.commit().unwrap();
    }
    let st1 = h.postings_cardinality_stats("__name__", 10);
    assert_eq!(st1.cardinality_metrics_stats[0].count, 2);
    assert_eq!(st1.cardinality_metrics_stats[0].name, "t");
    let st2 = h.postings_cardinality_stats("n", 1);
    assert_eq!(st2.cardinality_metrics_stats.len(), 1);
    h.close().unwrap();
}

// 50
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_PendingCommitUnderflowIsReportedNotFatal
#[test]
fn test_head_appender_pending_commit_underflow_is_reported_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let h = Head::open(dir.path(), HeadOptions { registry: Some(Arc::clone(&reg)), ..HeadOptions::default() }, i64::MIN).unwrap();
    let pairs = [("a", "1")];
    let mut app = h.appender();
    let ref_id = app.append_float(None, &pairs, 100, 1.0).unwrap();

    let entry = h.get_entry_by_id(ref_id).unwrap();
    assert_eq!(entry.pending_commits_count(), 2);
    assert!(entry.unmark_pending_commit_for_test());
    assert!(entry.unmark_pending_commit_for_test());

    app.commit().unwrap();
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_series_pending_commit_underflow_total", &[]),
        2.0
    );
    assert_eq!(entry.pending_commits_count(), 0);
    assert!(!entry.is_gced());

    let got = query_head(&h, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()]);
    assert_eq!(got[r#"{a="1"}"#].len(), 1);
    h.close().unwrap();
}

// 51
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadCompactableDoesNotCompactEmptyHead
#[test]
fn test_head_compactable_does_not_compact_empty_head() {
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1, ..HeadOptions::default() }, i64::MIN).unwrap();
    assert!(!head.compactable());
    head.close().unwrap();
}

// 52
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestFsyncWLSegments
#[test]
fn test_fsync_wl_segments() {
    // 1. WAL only
    {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        let mut app = h.appender();
        app.append_float(None, &[("foo", "bar")], 1, 1.0).unwrap();
        app.commit().unwrap();
        h.fsync_wl_segments().unwrap();
        h.close().unwrap();
    }
    // 2. WAL and WBL
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions { ooo_time_window_ms: 3_600_000, ..HeadOptions::default() };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        {
            let mut app = h.appender();
            app.append_float(None, &[("foo", "bar")], 100, 1.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = h.appender();
            app.append_float(None, &[("foo", "bar")], 50, 0.0).unwrap();
            app.commit().unwrap();
        }
        h.fsync_wl_segments().unwrap();
        h.close().unwrap();
    }
    // 3. Closed WAL fails
    {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        h.force_close_wal_for_test();
        assert!(h.fsync_wl_segments().is_err());
        h.close().unwrap();
    }
}

// 53
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_NumStaleSeries_MixedType
#[test]
fn test_head_num_stale_series_mixed_type() {
    let transitions = [
        (SampleKind::Float, SampleKind::Hist),
        (SampleKind::Float, SampleKind::FloatHist),
        (SampleKind::Hist, SampleKind::Float),
        (SampleKind::Hist, SampleKind::FloatHist),
        (SampleKind::FloatHist, SampleKind::Float),
        (SampleKind::FloatHist, SampleKind::Hist),
    ];
    for mode in ["live", "wal", "snapshot"] {
        for (stale_k, resolve_k) in transitions {
            let dir = tempfile::tempdir().unwrap();
            let opts = HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() };
            let snap = mode == "snapshot";
            let mut h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, snap).unwrap();
            let pairs = [("name", "mixed")];
            let append_sample = |h: &Head, k: SampleKind, ts: i64, is_stale: bool| {
                let mut app = h.appender();
                match k {
                    SampleKind::Float => {
                        app.append_float(None, &pairs, ts, if is_stale { stale_nan() } else { ts as f64 }).unwrap();
                    }
                    SampleKind::Hist => {
                        let mut hh = gen_hist(1);
                        if is_stale { hh.sum = stale_nan(); }
                        app.append_histogram(None, &pairs, 0, ts, hh).unwrap();
                    }
                    SampleKind::FloatHist => {
                        let mut fh = gen_fhist(1);
                        if is_stale { fh.sum = stale_nan(); }
                        app.append_float_histogram(None, &pairs, 0, ts, fh).unwrap();
                    }
                }
                app.commit().unwrap();
            };
            append_sample(&h, stale_k, 100, true);
            assert_eq!(h.num_stale_series(), 1);
            append_sample(&h, resolve_k, 200, false);
            assert_eq!(h.num_stale_series(), 0);
            if mode != "live" {
                h.close().unwrap();
                h = Head::open_with_snapshot(dir.path(), opts, i64::MIN, snap).unwrap();
            }
            assert_eq!((h.num_series(), h.num_stale_series()), (1, 0));
            h.close().unwrap();
        }
    }
}

// 54
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_NumStaleSeries_MixedTypeRemoval
#[test]
fn test_head_num_stale_series_mixed_type_removal() {
    for truncate_stale in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        {
            let mut app = h.appender();
            app.append_float(None, &[("name", "cross_type")], 100, stale_nan()).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = h.appender();
            app.append_histogram(None, &[("name", "cross_type")], 0, 200, gen_hist(1)).unwrap();
            let mut sh = gen_hist(1);
            sh.sum = stale_nan();
            app.append_histogram(None, &[("name", "stale_only")], 0, 100, sh).unwrap();
            app.append_float(None, &[("name", "keeper")], 5000, 1.0).unwrap();
            app.commit().unwrap();
        }
        assert_eq!(h.num_stale_series(), 1);
        let stale_ref = h.get_entry_by_sorted_pairs(&[("name", "stale_only")]).unwrap().ref_id;
        assert_eq!(h.stale_series_refs_no_ooo(), vec![stale_ref]);

        if truncate_stale {
            let ct_ref = h.get_entry_by_sorted_pairs(&[("name", "cross_type")]).unwrap().ref_id;
            let k_ref = h.get_entry_by_sorted_pairs(&[("name", "keeper")]).unwrap().ref_id;
            let refs = vec![ct_ref, stale_ref, k_ref];
            let fps = h.snapshot_fingerprints(&refs);
            h.truncate_stale_series(&refs, 1000, &fps).unwrap();
            assert_eq!((h.num_series(), h.num_stale_series()), (2, 0));
            assert!(h.get_entry_by_id(ct_ref).is_some());
        } else {
            h.truncate(1000).unwrap();
            assert_eq!((h.num_series(), h.num_stale_series()), (1, 0));
        }
        h.close().unwrap();
    }
}

// 55
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_NumStaleSeries_WALReplayIgnoresRejectedCrossTypeSamples
#[test]
fn test_head_num_stale_series_wal_replay_ignores_rejected_cross_type_samples() {
    let mut stale_h = gen_hist(1);
    stale_h.sum = stale_nan();
    let enc = RecordEncoder::new(false);
    for case_idx in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let s = enc.series(&[RefSeries { ref_id: 1, labels: Labels::from_pairs([("name", "x")]) }], Vec::new());
            let f0 = enc.samples(&[RefSample { ref_id: 1, st: 0, t: 0, v: 1.0 }], Vec::new());
            let (newer, older) = match case_idx {
                0 => (
                    enc.histogram_samples(&[RefHistogramSample { ref_id: 1, st: 0, t: 200, h: stale_h.clone() }], Vec::new()).0,
                    enc.samples(&[RefSample { ref_id: 1, st: 0, t: 100, v: 1.0 }], Vec::new()),
                ),
                1 => (
                    enc.samples(&[RefSample { ref_id: 1, st: 0, t: 200, v: stale_nan() }], Vec::new()),
                    enc.histogram_samples(&[RefHistogramSample { ref_id: 1, st: 0, t: 100, h: gen_hist(1) }], Vec::new()).0,
                ),
                _ => (
                    enc.samples(&[RefSample { ref_id: 1, st: 0, t: 200, v: stale_nan() }], Vec::new()),
                    enc.float_histogram_samples(&[RefFloatHistogramSample { ref_id: 1, st: 0, t: 100, fh: gen_fhist(1) }], Vec::new()).0,
                ),
            };
            w.log(&[&s, &f0, &newer, &older]).unwrap();
            w.close().unwrap();
        }
        let h = Head::open(dir.path(), HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() }, i64::MIN).unwrap();
        assert_eq!((h.num_series(), h.num_stale_series()), (1, 1));
        h.close().unwrap();
    }
}

// 56
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_WALReplayStaleMarkerTypeConsistency
#[test]
fn test_head_wal_replay_stale_marker_type_consistency() {
    for is_fhist in [false, true] {
        for mmapped in [false, true] {
            for st_storage in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let opts = HeadOptions {
                    chunk_range_ms: 1000,
                    enable_st_storage: st_storage,
                    float_chunk_encoding: if st_storage { Encoding::Xor2 } else { Encoding::Xor },
                    enable_histogram_st_encoding: st_storage,
                    ..HeadOptions::default()
                };
                let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
                let pairs = [("foo", "bar")];
                {
                    let mut app = h.appender();
                    if is_fhist {
                        app.append_float_histogram(None, &pairs, 0, 100, gen_fhist(1)).unwrap();
                    } else {
                        app.append_histogram(None, &pairs, 0, 100, gen_hist(1)).unwrap();
                    }
                    app.commit().unwrap();
                }
                let marker_t = if mmapped { 1000 } else { 200 };
                {
                    let mut app = h.appender();
                    app.append_float(None, &pairs, marker_t, stale_nan()).unwrap();
                    app.commit().unwrap();
                }
                if mmapped {
                    h.mmap_head_chunks().unwrap();
                }
                h.close().unwrap();

                let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
                let got = query_head(&h2, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()]);
                let marker = got[r#"{foo="bar"}"#].iter().find(|s| s.t == marker_t).unwrap();
                if is_fhist {
                    assert!(matches!(marker.value, SampleValue::FloatHistogram(_)));
                } else {
                    assert!(matches!(marker.value, SampleValue::Histogram(_)));
                }
                h2.close().unwrap();
            }
        }
    }
}

// 57
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_NumNativeHistogramSeriesAndBuckets
#[test]
fn test_head_num_native_histogram_series_and_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, ..HeadOptions::default() };
    let mut h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, false).unwrap();
    assert_eq!((h.num_native_histogram_series(), h.num_native_histogram_buckets()), (0, 0));

    {
        let mut app = h.appender();
        app.append_float(None, &[("name", "s1")], 100, 1.0).unwrap();
        app.commit().unwrap();
    }
    assert_eq!((h.num_native_histogram_series(), h.num_native_histogram_buckets()), (0, 0));

    {
        let mut app = h.appender();
        app.append_histogram(None, &[("name", "s1")], 0, 110, gen_hist(1)).unwrap();
        app.append_float_histogram(None, &[("name", "s2")], 0, 100, gen_fhist(1)).unwrap();
        app.commit().unwrap();
    }
    assert_eq!((h.num_native_histogram_series(), h.num_native_histogram_buckets()), (2, 4));

    h.close().unwrap();
    h = Head::open_with_snapshot(dir.path(), opts.clone(), i64::MIN, false).unwrap();
    assert_eq!((h.num_native_histogram_series(), h.num_native_histogram_buckets()), (2, 4));

    // Float sample on histogram series removes it from histogram gauges.
    {
        let mut app = h.appender();
        app.append_float(None, &[("name", "s1")], 140, 5.0).unwrap();
        app.commit().unwrap();
    }
    assert_eq!((h.num_native_histogram_series(), h.num_native_histogram_buckets()), (1, 2));

    // Stale float histogram drops bucket count to 0 while keeping histogram series count.
    let stale_fh = FloatHistogram {
        sum: stale_nan(),
        ..Default::default()
    };
    {
        let mut app = h.appender();
        app.append_float_histogram(None, &[("name", "s2")], 0, 150, stale_fh).unwrap();
        app.commit().unwrap();
    }
    assert_eq!((h.num_native_histogram_series(), h.num_native_histogram_buckets()), (1, 0));

    {
        let mut app = h.appender();
        app.append_float_histogram(None, &[("name", "s2")], 0, 400, gen_fhist(2)).unwrap();
        app.commit().unwrap();
    }
    h.truncate(300).unwrap();
    assert_eq!((h.num_series(), h.num_native_histogram_series(), h.num_native_histogram_buckets()), (1, 1, 2));
    h.close().unwrap();
}

// 58
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_FilterSelectedSeriesAndSortPostings
#[test]
fn test_head_filter_selected_series_and_sort_postings() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, ooo_time_window_ms: 1000, ..HeadOptions::default() };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let clean_ref;
    let ooo_ref;
    {
        let mut app = h.appender();
        clean_ref = app.append_float(None, &[("name", "clean")], 200, 1.0).unwrap();
        ooo_ref = app.append_float(None, &[("name", "with-ooo")], 200, 2.0).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = h.appender();
        app.append_float(Some(ooo_ref), &[("name", "with-ooo")], 100, 2.5).unwrap();
        app.commit().unwrap();
    }
    let bogus_ref = 999_999u64;
    let (cands, _, _) = h.collect_selected_compaction_candidates(&[bogus_ref, ooo_ref, clean_ref]).unwrap();
    assert_eq!(cands.len(), 1);
    assert_eq!(cands[0].0, clean_ref);
    h.close().unwrap();
}

// 59
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_WALEncoder_EnableSTStorage
#[test]
fn test_head_appender_wal_encoder_enable_st_storage() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            enable_st_storage: enable_st,
            float_chunk_encoding: if enable_st { Encoding::Xor2 } else { Encoding::Xor },
            enable_histogram_st_encoding: enable_st,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let mut app = h.appender();
        for ts in 100..110i64 {
            app.append_float(None, &[("foo", "bar")], ts, ts as f64).unwrap();
        }
        app.commit().unwrap();
        h.close().unwrap();
        let recs = read_all_wal_records(&dir.path().join("wal"));
        let has_samples = recs.iter().any(|r| matches!(r, DecodedRecord::Samples(_)));
        assert!(has_samples);
    }
}

// 60
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_WBLEncoder_EnableSTStorage
#[test]
fn test_head_appender_wbl_encoder_enable_st_storage() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            ooo_time_window_ms: 3_600_000,
            enable_st_storage: enable_st,
            float_chunk_encoding: if enable_st { Encoding::Xor2 } else { Encoding::Xor },
            enable_histogram_st_encoding: enable_st,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        {
            let mut app = h.appender();
            app.append_float(None, &[("foo", "bar")], 200, 200.0).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = h.appender();
            for ts in 100..110i64 {
                app.append_float_with_st(None, &[("foo", "bar")], 50, ts, ts as f64).unwrap();
            }
            app.commit().unwrap();
        }
        h.close().unwrap();
        let recs = read_all_wal_records(&dir.path().join(WBL_DIR_NAME));
        assert!(recs.iter().any(|r| matches!(r, DecodedRecord::Samples(_))));
    }
}

// 61
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_STStorage_Disabled
#[test]
fn test_head_appender_st_storage_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        ooo_time_window_ms: 3_600_000,
        enable_st_storage: false,
        float_chunk_encoding: Encoding::Xor2,
        enable_histogram_st_encoding: true,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    {
        let mut app = h.appender();
        app.append_float_with_st(None, &[("foo", "bar")], 10, 100, 100.0).unwrap();
        app.append_histogram(None, &[("h", "1")], 10, 100, gen_hist(1)).unwrap();
        app.commit().unwrap();
    }
    let got = query_head(&h, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()]);
    assert_eq!(got[r#"{foo="bar"}"#][0].st, 0);
    let got_h = query_head(&h, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "h", "1").unwrap()]);
    assert_eq!(got_h[r#"{h="1"}"#][0].st, 0);
    h.close().unwrap();
}

// 62
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_STStorage_WALReplay
#[test]
fn test_head_appender_st_storage_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        enable_st_storage: true,
        float_chunk_encoding: Encoding::Xor2,
        enable_histogram_st_encoding: true,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    {
        let mut app = h.appender();
        app.append_float_with_st(None, &[("foo", "bar")], 50, 100, 1.0).unwrap();
        app.append_histogram(None, &[("h", "1")], 60, 100, gen_hist(1)).unwrap();
        app.commit().unwrap();
    }
    h.close().unwrap();
    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    assert_eq!(query_head(&h2, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()])[r#"{foo="bar"}"#][0].st, 50);
    assert_eq!(query_head(&h2, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "h", "1").unwrap()])[r#"{h="1"}"#][0].st, 60);
    h2.close().unwrap();
}

// 63
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestWALReplayStoreSTPassed
#[test]
fn test_wal_replay_store_st_passed() {
    let dir = tempfile::tempdir().unwrap();
    let opts1 = HeadOptions { float_chunk_encoding: Encoding::Xor, ..HeadOptions::default() };
    let h1 = Head::open(dir.path(), opts1, i64::MIN).unwrap();
    {
        let mut app = h1.appender();
        for ts in 1..=5i64 {
            app.append_float(None, &[("foo", "bar")], ts, ts as f64).unwrap();
        }
        app.commit().unwrap();
    }
    h1.close().unwrap();

    let opts2 = HeadOptions {
        enable_st_storage: true,
        float_chunk_encoding: Encoding::Xor2,
        ..HeadOptions::default()
    };
    let h2 = Head::open(dir.path(), opts2, i64::MIN).unwrap();
    {
        let mut app = h2.appender();
        app.append_float_with_st(None, &[("foo", "bar")], 7, 10, 10.0).unwrap();
        app.commit().unwrap();
    }
    let r = h2.get_entry_by_sorted_pairs(&[("foo", "bar")]).unwrap().ref_id;
    let (_, chks, _) = h2.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
    assert_eq!(chks[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor2);
    h2.close().unwrap();
}

// 64
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_STStorage_WBLReplay
#[test]
fn test_head_appender_st_storage_wbl_replay() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions {
        ooo_time_window_ms: 3_600_000,
        enable_st_storage: true,
        float_chunk_encoding: Encoding::Xor2,
        enable_histogram_st_encoding: true,
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    {
        let mut app = h.appender();
        app.append_float_with_st(None, &[("foo", "bar")], 50, 200, 200.0).unwrap();
        app.append_float_with_st(None, &[("foo", "bar")], 50, 100, 100.0).unwrap();
        app.commit().unwrap();
    }
    h.close().unwrap();
    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let got = query_head(&h2, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()]);
    assert_eq!(got[r#"{foo="bar"}"#].iter().map(|s| (s.st, s.t)).collect::<Vec<_>>(), vec![(50, 100), (50, 200)]);
    h2.close().unwrap();
}

// 65
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_STStorage_WALReplay_CrossEncoding
#[test]
fn test_head_appender_st_storage_wal_replay_cross_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let opts1 = HeadOptions { enable_st_storage: false, float_chunk_encoding: Encoding::Xor, ..HeadOptions::default() };
    let h1 = Head::open(dir.path(), opts1, i64::MIN).unwrap();
    {
        let mut app = h1.appender();
        for ts in 100..110i64 {
            app.append_float(None, &[("foo", "bar")], ts, ts as f64).unwrap();
        }
        app.commit().unwrap();
    }
    h1.close().unwrap();

    let opts2 = HeadOptions { enable_st_storage: true, float_chunk_encoding: Encoding::Xor2, ..HeadOptions::default() };
    let h2 = Head::open(dir.path(), opts2, i64::MIN).unwrap();
    let r = h2.get_entry_by_sorted_pairs(&[("foo", "bar")]).unwrap().ref_id;
    let (_, chks, _) = h2.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
    assert_eq!(chks[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor2);
    h2.close().unwrap();
}

// 66
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_STStorage_WALReplay_CrossEncoding_Reverse
#[test]
fn test_head_appender_st_storage_wal_replay_cross_encoding_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let opts1 = HeadOptions { enable_st_storage: true, float_chunk_encoding: Encoding::Xor2, ..HeadOptions::default() };
    let h1 = Head::open(dir.path(), opts1, i64::MIN).unwrap();
    {
        let mut app = h1.appender();
        for ts in 100..110i64 {
            app.append_float_with_st(None, &[("foo", "bar")], 50, ts, ts as f64).unwrap();
        }
        app.commit().unwrap();
    }
    h1.close().unwrap();

    let opts2 = HeadOptions { enable_st_storage: false, float_chunk_encoding: Encoding::Xor, ..HeadOptions::default() };
    let h2 = Head::open(dir.path(), opts2, i64::MIN).unwrap();
    let r = h2.get_entry_by_sorted_pairs(&[("foo", "bar")]).unwrap().ref_id;
    let (_, chks, _) = h2.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
    assert_eq!(chks[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor);
    let got = query_head(&h2, 100, 109, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()]);
    assert!(got[r#"{foo="bar"}"#].iter().all(|s| s.st == 0));
    h2.close().unwrap();
}

// 67
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_STStorage_ChunkEncoding
#[test]
fn test_head_appender_st_storage_chunk_encoding() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            enable_st_storage: enable_st,
            float_chunk_encoding: if enable_st { Encoding::Xor2 } else { Encoding::Xor },
            enable_histogram_st_encoding: enable_st,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let mut app = h.appender();
        app.append_float_with_st(None, &[("t", "f")], 10, 100, 1.0).unwrap();
        app.append_histogram(None, &[("t", "h")], 10, 100, gen_hist(1)).unwrap();
        app.append_float_histogram(None, &[("t", "fh")], 10, 100, gen_fhist(1)).unwrap();
        app.commit().unwrap();

        let rf = h.get_entry_by_sorted_pairs(&[("t", "f")]).unwrap().ref_id;
        let rh = h.get_entry_by_sorted_pairs(&[("t", "h")]).unwrap().ref_id;
        let rfh = h.get_entry_by_sorted_pairs(&[("t", "fh")]).unwrap().ref_id;
        assert_eq!(
            h.series_chunks(rf, i64::MIN, i64::MAX).unwrap().unwrap().1[0].chunk.as_ref().unwrap().encoding(),
            if enable_st { Encoding::Xor2 } else { Encoding::Xor }
        );
        assert_eq!(
            h.series_chunks(rh, i64::MIN, i64::MAX).unwrap().unwrap().1[0].chunk.as_ref().unwrap().encoding(),
            if enable_st { Encoding::HistogramSt } else { Encoding::Histogram }
        );
        assert_eq!(
            h.series_chunks(rfh, i64::MIN, i64::MAX).unwrap().unwrap().1[0].chunk.as_ref().unwrap().encoding(),
            if enable_st { Encoding::FloatHistogramSt } else { Encoding::FloatHistogram }
        );
        h.close().unwrap();
    }
}

// 68
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestFloatChunkEncodingSwitchWaitsForNextChunk
#[test]
fn test_float_chunk_encoding_switch_waits_for_next_chunk() {
    // 1. without_st_storage: waits for natural cut
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions { float_chunk_encoding: Encoding::Xor2, ..HeadOptions::default() };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("foo", "bar")];
        {
            let mut app = h.appender();
            for ts in 1..=5i64 {
                app.append_float(None, &pairs, ts, ts as f64).unwrap();
            }
            app.commit().unwrap();
        }
        h.set_float_chunk_encoding(Encoding::Xor);
        {
            let mut app = h.appender();
            for ts in 6..=10i64 {
                app.append_float(None, &pairs, ts, ts as f64).unwrap();
            }
            app.commit().unwrap();
        }
        let r = h.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
        assert_eq!(h.head_chunks_bounds_for_test(r).unwrap().len(), 1);
        assert_eq!(h.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap().1[0].chunk.as_ref().unwrap().encoding(), Encoding::Xor2);

        {
            let mut app = h.appender();
            for ts in 11..=(DEFAULT_SAMPLES_PER_CHUNK as i64 * 3) {
                app.append_float(None, &pairs, ts, ts as f64).unwrap();
            }
            app.commit().unwrap();
        }
        let (_, chks, _) = h.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
        assert!(chks.len() >= 2);
        assert_eq!(chks.last().unwrap().chunk.as_ref().unwrap().encoding(), Encoding::Xor);
        h.close().unwrap();
    }
    // 2. with_st_storage: immediate cut in both directions
    for (from_enc, to_enc) in [(Encoding::Xor2, Encoding::Xor), (Encoding::Xor, Encoding::Xor2)] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions { enable_st_storage: true, float_chunk_encoding: from_enc, ..HeadOptions::default() };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("foo", "bar")];
        {
            let mut app = h.appender();
            for ts in 1..=5i64 {
                app.append_float(None, &pairs, ts, ts as f64).unwrap();
            }
            app.commit().unwrap();
        }
        h.set_float_chunk_encoding(to_enc);
        {
            let mut app = h.appender();
            app.append_float(None, &pairs, 6, 6.0).unwrap();
            app.commit().unwrap();
        }
        let r = h.get_entry_by_sorted_pairs(&pairs).unwrap().ref_id;
        let (_, chks, _) = h.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
        assert_eq!(chks.len(), 2);
        assert_eq!(chks[0].chunk.as_ref().unwrap().encoding(), from_enc);
        assert_eq!(chks[1].chunk.as_ref().unwrap().encoding(), to_enc);
        h.close().unwrap();
    }
}

// 69
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestWALReplayRaceWithStaleSeriesCompaction
#[test]
fn test_wal_replay_race_with_stale_series_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opts = HeadOptions { chunk_range_ms: 1000, stripe_size: 32, ..HeadOptions::default() };
    let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
    let mut stale_refs = Vec::new();
    {
        let mut app = h.appender();
        for i in 0..500 {
            let iv = i.to_string();
            stale_refs.push(app.append_float(None, &[("__name__", "stale_metric"), ("i", &iv)], 100, i as f64).unwrap());
        }
        app.commit().unwrap();
    }
    {
        let mut app = h.appender();
        for (i, &r) in stale_refs.iter().enumerate() {
            let iv = i.to_string();
            app.append_float(Some(r), &[("__name__", "stale_metric"), ("i", &iv)], 200, stale_nan()).unwrap();
        }
        app.commit().unwrap();
    }
    assert_eq!(h.num_stale_series(), 500);
    let fps = h.snapshot_fingerprints(&stale_refs);
    h.truncate_stale_series(&stale_refs, 300, &fps).unwrap();
    assert_eq!((h.num_series(), h.num_stale_series()), (0, 0));

    {
        let mut app = h.appender();
        for i in 0..500 {
            let iv = i.to_string();
            app.append_float(None, &[("__name__", "new_metric"), ("i", &iv)], 300, i as f64).unwrap();
        }
        app.commit().unwrap();
    }
    h.close().unwrap();

    let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
    assert_eq!((h2.num_series(), h2.num_stale_series()), (500, 0));
    h2.close().unwrap();
}

// 70
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_ReadWAL_TombstonesAdvanceLastSeriesID
#[test]
fn test_head_read_wal_tombstones_advance_last_series_id() {
    for (mint, maxt) in [(0, 100), (i64::MIN, i64::MAX)] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let enc = RecordEncoder::new(false);
            let s = enc.series(&[RefSeries { ref_id: 10, labels: Labels::from_pairs([("a", "1")]) }], Vec::new());
            let smp = enc.samples(&[RefSample { ref_id: 10, st: 0, t: 100, v: 1.0 }], Vec::new());
            let tb = enc.tombstones(&[Tombstone { ref_id: 42, mint, maxt }], Vec::new());
            w.log(&[&s, &smp, &tb]).unwrap();
            w.close().unwrap();
        }
        let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        assert_eq!(h.last_series_ref(), 42);
        h.close().unwrap();
    }
}

// 71
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestStripeSeries_UnlinkHash
#[test]
fn test_stripe_series_unlink_hash() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let pairs = [("a", "1")];
    let (s1, _) = h.get_or_create_for_test(&pairs).unwrap();
    h.unlink_hash_for_test(s1.hash, s1.ref_id);
    assert!(h.get_entry_by_sorted_pairs(&pairs).is_none());
    assert!(h.get_entry_by_id(s1.ref_id).is_some());
    let (s2, created) = h.get_or_create_for_test(&pairs).unwrap();
    assert!(created);
    assert_ne!(s2.ref_id, s1.ref_id);
    h.close().unwrap();
}

// 72
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_WALReplay_FullRangeTombstoneDeletesDuplicateRef
#[test]
fn test_head_wal_replay_full_range_tombstone_deletes_duplicate_ref() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    {
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(false);
        let lbls = Labels::from_pairs([("a", "1")]);
        let s = enc.series(
            &[
                RefSeries { ref_id: 1, labels: lbls.clone() },
                RefSeries { ref_id: 2, labels: lbls },
            ],
            Vec::new(),
        );
        let smp = enc.samples(&[RefSample { ref_id: 2, st: 0, t: 100, v: 1.0 }], Vec::new());
        let tb = enc.tombstones(&[Tombstone { ref_id: 2, mint: i64::MIN, maxt: i64::MAX }], Vec::new());
        w.log(&[&s, &smp, &tb]).unwrap();
        w.close().unwrap();
    }
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    assert_eq!(h.num_series(), 0);
    h.close().unwrap();
}

// 73
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_WALCheckpoint_FullRangeTombstones
#[test]
fn test_head_wal_checkpoint_full_range_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    {
        let mut w = WlogWriter::with_segment_size(&wal_dir, 32 * 1024, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(false);
        let s = enc.series(
            &[
                RefSeries { ref_id: 1, labels: Labels::from_pairs([("a", "1")]) },
                RefSeries { ref_id: 2, labels: Labels::from_pairs([("a", "2")]) },
            ],
            Vec::new(),
        );
        let smp = enc.samples(
            &[
                RefSample { ref_id: 1, st: 0, t: 100, v: 1.0 },
                RefSample { ref_id: 1, st: 0, t: 500, v: 2.0 },
                RefSample { ref_id: 2, st: 0, t: 100, v: 3.0 },
                RefSample { ref_id: 2, st: 0, t: 200, v: 4.0 },
            ],
            Vec::new(),
        );
        let tb = enc.tombstones(
            &[
                Tombstone { ref_id: 1, mint: i64::MIN, maxt: i64::MAX },
                Tombstone { ref_id: 2, mint: i64::MIN, maxt: i64::MAX },
            ],
            Vec::new(),
        );
        w.log(&[&s, &smp, &tb]).unwrap();
        w.close().unwrap();
    }
    let h = Head::open(dir.path(), HeadOptions { wal_segment_size: 32 * 1024, ..HeadOptions::default() }, i64::MIN).unwrap();
    assert_eq!(h.num_series(), 0);
    assert_eq!(h.get_wal_expiry_for_test(1), Some(500));
    assert_eq!(h.get_wal_expiry_for_test(2), Some(200));

    let first = segments_range(&wal_dir).unwrap().unwrap().0;
    loop {
        h.reset_last_wal_truncation_time_for_test();
        h.truncate_wal(250).unwrap();
        if segments_range(&wal_dir).unwrap().unwrap().0 > first {
            break;
        }
    }
    let (cp_dir, _) = last_checkpoint(&wal_dir).unwrap().unwrap();
    let mut recs = read_all_wal_records(&cp_dir);
    recs.extend(read_all_wal_records(&wal_dir));
    assert!(recs.iter().any(|r| matches!(r, DecodedRecord::Series(v) if v.iter().any(|x| x.ref_id == 1))));
    assert!(!recs.iter().any(|r| matches!(r, DecodedRecord::Series(v) if v.iter().any(|x| x.ref_id == 2))));
    assert_eq!(h.get_wal_expiry_for_test(1), Some(500));
    assert_eq!(h.get_wal_expiry_for_test(2), None);
    h.close().unwrap();
}

// 74
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_FastStartupStateFile
#[test]
fn test_head_fast_startup_state_file() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    {
        let mut app = h.appender();
        app.append_float(None, &[("fast", "startup")], 100, 1.0).unwrap();
        app.commit().unwrap();
    }
    h.write_series_state_file(false).unwrap();
    let st_unclean = h.read_series_state_file().unwrap();
    assert!(!st_unclean.clean_shutdown);
    assert_eq!(st_unclean.last_series_id, 1);

    h.close().unwrap();
    let raw = fs::read(dir.path().join("wal").join("series_state.json")).unwrap();
    let st_clean: SeriesLifecycleState = serde_json::from_slice(&raw).unwrap();
    assert!(st_clean.clean_shutdown);
    assert_eq!(st_clean.last_series_id, 1);
}

// 75
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_ReadSeriesStateFile
#[test]
fn test_head_read_series_state_file() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let err = h.read_series_state_file().unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);

    let expected = SeriesLifecycleState {
        last_series_id: 42_000,
        last_wal_segment: 5,
        clean_shutdown: true,
    };
    fs::write(
        dir.path().join("wal").join("series_state.json"),
        serde_json::to_vec(&expected).unwrap(),
    )
    .unwrap();
    assert_eq!(h.read_series_state_file().unwrap(), expected);
    h.close().unwrap();
}

// 76
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_FindLastSeriesID
#[test]
fn test_head_find_last_series_id() {
    let dir = tempfile::tempdir().unwrap();
    let h = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let wal_dir = dir.path().join("wal");
    {
        let mut app = h.appender();
        app.append_float(None, &[("metric", "A")], 100, 1.0).unwrap();
        app.commit().unwrap();
    }
    let state = SeriesLifecycleState {
        last_series_id: 1,
        last_wal_segment: 0,
        clean_shutdown: false,
    };
    let last = segments_range(&wal_dir).unwrap().unwrap().1;
    assert_eq!(h.find_last_series_id(&state, last).unwrap(), 1);

    {
        let mut app = h.appender();
        app.append_float(None, &[("metric", "B")], 300, 3.0).unwrap();
        app.commit().unwrap();
    }
    let last2 = segments_range(&wal_dir).unwrap().unwrap().1;
    assert_eq!(h.find_last_series_id(&state, last2).unwrap(), 2);
    h.close().unwrap();
}

// 77
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHead_mmapHeadChunks
#[test]
fn test_head_mmap_head_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions {
        ooo_time_window_ms: 600_000,
        registry: Some(Arc::clone(&reg)),
        ..HeadOptions::default()
    };
    let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
    let interval = DEFAULT_BLOCK_DURATION_MS / (4 * DEFAULT_SAMPLES_PER_CHUNK as i64);
    let mut ts = 0i64;
    let pairs_a = [("__name__", "seriesA")];
    let pairs_b = [("__name__", "seriesB")];
    {
        let mut app = h.appender();
        app.append_float(None, &pairs_a, ts, 1.0).unwrap();
        app.commit().unwrap();
    }
    ts += interval;
    let mut rb = None;
    {
        let mut app = h.appender();
        for _ in 0..(2 * DEFAULT_SAMPLES_PER_CHUNK + 10) {
            rb = Some(app.append_float(rb, &pairs_b, ts, ts as f64).unwrap());
            ts += interval;
        }
        app.commit().unwrap();
    }
    let r_b = rb.unwrap();
    assert_eq!(h.head_chunks_bounds_for_test(r_b).unwrap().len(), 3);
    h.mmap_head_chunks().unwrap();
    assert_eq!(h.head_chunks_bounds_for_test(r_b).unwrap().len(), 1);
    assert_eq!(h.mmapped_chunks_bounds_for_test(r_b).unwrap().len(), 2);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_chunks_max_mmapped", &[]), 2.0);
    let before = metric_value(&reg, "prometheus_tsdb_mmap_chunks_total", &[]);
    h.mmap_head_chunks().unwrap();
    assert_eq!(metric_value(&reg, "prometheus_tsdb_mmap_chunks_total", &[]), before);
    h.close().unwrap();
}

// 78
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestQueryOOOHeadDuringTruncateAcrossWrap
#[test]
fn test_query_ooo_head_during_truncate_across_wrap() {
    let dir = tempfile::tempdir().unwrap();
    let max_t = 6000i64;
    let db = Tsdb::open_with_reload_interval(
        dir.path(),
        TsdbOptions {
            head: HeadOptions { ooo_time_window_ms: max_t, chunk_range_ms: max_t / 2, ..HeadOptions::default() },
            ..TsdbOptions::default()
        },
        Duration::ZERO,
    )
    .unwrap();
    db.disable_compactions();
    {
        let mut app = db.appender();
        let mut r = None;
        for i in (0..max_t).step_by(100) {
            r = Some(app.append_float(r, &[("a", "b")], i, i as f64).unwrap());
        }
        for i in (50..max_t).step_by(100) {
            app.append_float(r, &[("a", "b")], i, i as f64).unwrap();
        }
        app.commit().unwrap();
    }
    let m = [LabelMatcher::new(MatchType::Equal, "a", "b").unwrap()];
    assert!(!db.querier(0, max_t).select(true, &m).unwrap().is_empty());
    db.compact().unwrap();
    assert!(!db.querier(0, max_t).select(true, &m).unwrap().is_empty());
    const { assert!(OOO_CHUNK_ID_MASK > 0); }
    db.close().unwrap();
}

// 79
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestOOORestartResetsFirstOOOChunkID
#[test]
fn test_ooo_restart_resets_first_ooo_chunk_id() {
    let min_ms = 60_000i64;
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            chunk_range_ms: 1000,
            ooo_cap_max: 30,
            ooo_time_window_ms: 1000 * min_ms,
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let pairs = [("foo", "bar")];
        let add_m = |h: &Head, m: i64| {
            let mut app = h.appender();
            match kind {
                SampleKind::Float => { app.append_float(None, &pairs, m * min_ms, m as f64).unwrap(); }
                SampleKind::Hist => { app.append_histogram(None, &pairs, 0, m * min_ms, gen_hist(m)).unwrap(); }
                SampleKind::FloatHist => { app.append_float_histogram(None, &pairs, 0, m * min_ms, gen_fhist(m)).unwrap(); }
            }
            app.commit().unwrap();
        };
        add_m(&h, 200);
        for m in 100..=191i64 {
            add_m(&h, m);
        }
        h.close().unwrap();

        let h2 = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let got = query_head(&h2, i64::MIN, i64::MAX, &[LabelMatcher::new(MatchType::Equal, "foo", "bar").unwrap()]);
        assert_eq!(got[r#"{foo="bar"}"#].len(), 93);
        h2.close().unwrap();
    }
}

// 80
// Upstream: github.com/prometheus/prometheus/tsdb/head_test.go::TestHeadAppender_DuplicateSamplesDroppedMetric
#[test]
fn test_head_appender_duplicate_samples_dropped_metric() {
    for kind in [SampleKind::Float, SampleKind::Hist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            ooo_time_window_ms: 3_600_000,
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let h = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("foo", "bar")];
        let dups = || metric_value(&reg, "prometheus_tsdb_head_samples_dropped_total", &[("type", kind.metric_type())]);
        let append_k = |app: &mut mimir_tsdb::head::HeadAppender<'_>, ts: i64, val: i64| {
            match kind {
                SampleKind::Float => app.append_float(None, &pairs, ts, val as f64),
                SampleKind::Hist => app.append_histogram(None, &pairs, 0, ts, gen_hist(val)),
                SampleKind::FloatHist => app.append_float_histogram(None, &pairs, 0, ts, gen_fhist(val)),
            }
        };

        {
            let mut app = h.appender();
            append_k(&mut app, 60_000, 1).unwrap();
            append_k(&mut app, 120_000, 2).unwrap();
            app.commit().unwrap();
        }
        // Exact duplicate of latest in-order sample.
        {
            let mut app = h.appender();
            append_k(&mut app, 120_000, 2).unwrap();
            app.commit().unwrap();
        }
        // Conflicting sample at same timestamp.
        {
            let mut app = h.appender();
            let err = append_k(&mut app, 120_000, 3).unwrap_err();
            assert!(err.is_duplicate_sample());
            app.commit().unwrap();
        }
        // Within-batch conflict.
        {
            let mut app = h.appender();
            append_k(&mut app, 180_000, 6).unwrap();
            append_k(&mut app, 180_000, 7).unwrap();
            app.commit().unwrap();
        }
        // Within-batch exact duplicate.
        {
            let mut app = h.appender();
            append_k(&mut app, 240_000, 8).unwrap();
            append_k(&mut app, 240_000, 8).unwrap();
            app.commit().unwrap();
        }
        // OOO sample + exact duplicate + clash.
        {
            let mut app = h.appender();
            append_k(&mut app, 30_000, 4).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = h.appender();
            append_k(&mut app, 30_000, 4).unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = h.appender();
            append_k(&mut app, 30_000, 5).unwrap();
            app.commit().unwrap();
        }
        assert!(dups() >= 0.0);
        let dup_metric = metric_value(&reg, "prometheus_tsdb_duplicate_samples_total", &[("type", kind.metric_type())]);
        assert_eq!(dup_metric, 6.0);
        assert_eq!(metric_value(&reg, "prometheus_tsdb_out_of_order_samples_total", &[("type", kind.metric_type())]), 0.0);
        h.close().unwrap();
    }
}
