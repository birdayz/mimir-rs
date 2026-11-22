//! Ported upstream Prometheus TSDB db_append_v2_test.go (part 1 of 2, 37 tests).

mod db_test_helpers;

use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use db_test_helpers::{
    assert_sample_vals, compare_series, dir_hash, dir_size, match_eq, match_re, metric_val,
    open_db_range, opts_with_reg, query_chunks_expanded, query_db, read_all_wal_records,
    lcg_next as lcg, xor_block, DEFAULT_BLOCK_DURATION_MS,
};
use mimir_tsdb::chunkenc::{FloatHistogram, Histogram, HistogramSpan};
use mimir_tsdb::compact::{DecodedSample, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbExtraOptions, TsdbOptions, TsdbReadOnly};
use mimir_tsdb::head::HeadError;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::record::{DecodedRecord, RecordEncoder, RefSample, RefSeries};
use mimir_tsdb::wlog::{last_checkpoint, segments_range, CompressionType, WlogReader, WlogWriter, DEFAULT_SEGMENT_SIZE};

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDataAvailableOnlyAfterCommit_AppendV2
#[test]
fn test_data_available_only_after_commit_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("foo", "bar")], 0, 0.0).unwrap();
    assert!(query_db(&db, 0, 1, &[match_eq("foo", "bar")]).is_empty());
    app.commit().unwrap();
    assert_sample_vals(&query_db(&db, 0, 1, &[match_eq("foo", "bar")])[r#"{foo="bar"}"#], &[(0, 0.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestNoPanicAfterWALCorruption_AppendV2
#[test]
fn test_no_panic_after_wal_corruption_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.wal_segment_size = 32 * 1024;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    let mut exp = Vec::new();
    for maxt in 0..121i64 {
        let mut app = db.appender();
        app.append_float(None, &[("foo", "bar")], maxt, 0.0).unwrap();
        app.commit().unwrap();
        exp.push((maxt, 0.0));
    }
    db.close().unwrap();

    let wal_dir = dir.path().join("wal");
    let mut files: Vec<_> = fs::read_dir(&wal_dir).unwrap().filter_map(|e| e.ok()).filter(|e| !e.file_type().unwrap().is_dir()).collect();
    files.sort_by_key(|e| e.file_name());
    let first_wal = files[0].path();
    let mut r = WlogReader::open_segment(&wal_dir, 0).unwrap();
    assert!(r.next_record().unwrap().is_some());
    assert!(r.next_record().unwrap().is_some());
    let off = r.offset() as u64;
    drop(r);
    let mut f = fs::OpenOptions::new().read(true).write(true).open(&first_wal).unwrap();
    f.seek(SeekFrom::Start(off)).unwrap();
    f.write_all(&[99]).unwrap();
    drop(f);

    let reg = Arc::new(prometheus::Registry::new());
    let mut opts2 = TsdbOptions::default();
    opts2.head.registry = Some(reg.clone());
    let db2 = Tsdb::open(dir.path(), opts2).unwrap();
    assert_eq!(metric_val(&reg, "prometheus_tsdb_wal_corruptions_total", &[]), 1.0);
    let got = query_db(&db2, 0, 121, &[match_re("foo", ".*")]);
    assert_sample_vals(&got[r#"{foo="bar"}"#], &exp[..exp.len() - 1]);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDataNotAvailableAfterRollback_AppendV2
#[test]
fn test_data_not_available_after_rollback_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("type", "float")], 0, 0.0).unwrap();
    app.append_histogram(None, &[("type", "histogram")], 0, 0, Histogram { count: 42, sum: f64::NAN, ..Histogram::default() }).unwrap();
    app.append_float_histogram(None, &[("type", "floathistogram")], 0, 0, FloatHistogram { count: 42.0, sum: f64::NAN, ..FloatHistogram::default() }).unwrap();
    app.rollback().unwrap();

    for typ in ["float", "histogram", "floathistogram"] {
        assert!(query_db(&db, 0, 1, &[match_eq("type", typ)]).is_empty());
    }
    let mut counts = (0usize, 0usize, 0usize, 0usize, 0usize);
    for rec in read_all_wal_records(&dir.path().join("wal")) {
        match rec {
            DecodedRecord::Series(v) => counts.0 += v.len(),
            DecodedRecord::Samples(v) => counts.1 += v.len(),
            DecodedRecord::Exemplars(v) => counts.2 += v.len(),
            DecodedRecord::HistogramSamples(v) => counts.3 += v.len(),
            DecodedRecord::FloatHistogramSamples(v) => counts.4 += v.len(),
            _ => {}
        }
    }
    assert_eq!(counts, (3, 0, 0, 0, 0));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBAppenderV2_AddRef
#[test]
fn test_db_appender_v2_add_ref() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app1 = db.appender();
    let r1 = app1.append_float(None, &[("a", "b")], 123, 0.0).unwrap();
    let r2 = app1.append_float(Some(r1), &[], 124, 1.0).unwrap();
    assert_eq!(r1, r2);
    app1.commit().unwrap();

    let mut app2 = db.appender();
    assert_eq!(app2.append_float(Some(r1), &[], 125, 0.0).unwrap(), r1);
    assert_eq!(app2.append_float(Some(r1), &[("a", "b")], 133, 1.0).unwrap(), r1);
    assert_eq!(app2.append_float(Some(r2), &[], 143, 2.0).unwrap(), r1);
    assert!(app2.append_float(Some(9_999_999), &[], 1, 1.0).is_err());
    app2.commit().unwrap();

    assert_sample_vals(&query_db(&db, 0, 200, &[match_eq("a", "b")])[r#"{a="b"}"#], &[(123, 0.0), (124, 1.0), (125, 0.0), (133, 1.0), (143, 2.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBAppenderV2_EmptyLabelsIgnored
#[test]
fn test_db_appender_v2_empty_labels_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    let r1 = app.append_float(None, &[("a", "b")], 123, 0.0).unwrap();
    let r2 = app.append_float(None, &[("a", "b"), ("c", "")], 124, 0.0).unwrap();
    assert_eq!(r1, r2);
    app.commit().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBAppenderV2_EmptyLabelsetCausesError
#[test]
fn test_db_appender_v2_empty_labelset_causes_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    assert!(matches!(app.append_float(None, &[], 0, 0.0), Err(HeadError::EmptyLabelset)));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDeleteSimple_AppendV2
#[test]
fn test_delete_simple_append_v2() {
    type DelCase<'a> = (&'a [(i64, i64)], &'a [i64]);
    let cases: &[DelCase<'_>] = &[
        (&[(0, 3)], &[4, 5, 6, 7, 8, 9]),
        (&[(1, 3)], &[0, 4, 5, 6, 7, 8, 9]),
        (&[(1, 3), (4, 7)], &[0, 8, 9]),
        (&[(1, 3), (4, 700)], &[0]),
        (&[(0, 9)], &[]),
    ];
    for &(intervals, remaint) in cases {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        let mut app = db.appender();
        let mut smpls = Vec::new();
        let mut rng = 12345u64;
        for i in 0..10i64 {
            let v = (lcg(&mut rng) % 1000) as f64 / 7.0;
            smpls.push(v);
            app.append_float(None, &[("a", "b")], i, v).unwrap();
        }
        app.commit().unwrap();
        for &(mint, maxt) in intervals {
            db.delete(mint, maxt, &[match_eq("a", "b")]).unwrap();
        }
        let got = query_db(&db, 0, 10, &[match_eq("a", "b")]);
        if remaint.is_empty() {
            assert!(got.is_empty());
        } else {
            let want: Vec<(i64, f64)> = remaint.iter().map(|&t| (t, smpls[t as usize])).collect();
            assert_sample_vals(&got[r#"{a="b"}"#], &want);
        }
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestAmendHistogramDatapointCausesError_AppendV2
#[test]
fn test_amend_histogram_datapoint_causes_error_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 0, 0.0).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 0, 0.0).unwrap();
    assert!(matches!(app.append_float(None, &[("a", "b")], 0, 1.0), Err(HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_))));
    app.rollback().unwrap();

    let mut h = Histogram {
        schema: 3, count: 52, sum: 2.7, zero_threshold: 0.1, zero_count: 42,
        positive_spans: vec![HistogramSpan { offset: 0, length: 4 }, HistogramSpan { offset: 10, length: 3 }],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        ..Histogram::default()
    };
    let mut fh = h.to_float();

    let mut app = db.appender();
    app.append_histogram(None, &[("a", "c")], 0, 0, h.clone()).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_histogram(None, &[("a", "c")], 0, 0, h.clone()).unwrap();
    h.schema = 2;
    assert!(matches!(app.append_histogram(None, &[("a", "c")], 0, 0, h.clone()), Err(HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_))));
    app.rollback().unwrap();

    let mut app = db.appender();
    app.append_float_histogram(None, &[("a", "d")], 0, 0, fh.clone()).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_float_histogram(None, &[("a", "d")], 0, 0, fh.clone()).unwrap();
    fh.schema = 2;
    assert!(matches!(app.append_float_histogram(None, &[("a", "d")], 0, 0, fh.clone()), Err(HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_))));
    app.rollback().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDuplicateNaNDatapointNoAmendError_AppendV2
#[test]
fn test_duplicate_nan_datapoint_no_amend_error_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 0, f64::NAN).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 0, f64::NAN).unwrap();
    app.rollback().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestNonDuplicateNaNDatapointsCausesAmendError_AppendV2
#[test]
fn test_non_duplicate_nan_datapoints_causes_amend_error_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 0, f64::from_bits(0x7ff0_0000_0000_0001)).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    assert!(matches!(
        app.append_float(None, &[("a", "b")], 0, f64::from_bits(0x7ff0_0000_0000_0002)),
        Err(HeadError::DuplicateSampleForTimestamp | HeadError::DuplicateSampleWithDetails(_))
    ));
    app.rollback().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestSkippingInvalidValuesInSameTxn_AppendV2
#[test]
fn test_skipping_invalid_values_in_same_txn_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 0, 1.0).unwrap();
    app.append_float(None, &[("a", "b")], 0, 2.0).unwrap();
    app.commit().unwrap();
    assert_sample_vals(&query_db(&db, 0, 10, &[match_eq("a", "b")])[r#"{a="b"}"#], &[(0, 1.0)]);

    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 10, 3.0).unwrap();
    app.append_float(None, &[("a", "b")], 7, 5.0).unwrap();
    app.commit().unwrap();
    assert_sample_vals(&query_db(&db, 0, 10, &[match_eq("a", "b")])[r#"{a="b"}"#], &[(0, 1.0), (10, 3.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDB_Snapshot_AppendV2
#[test]
fn test_db_snapshot_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mint = 1_414_141_414_000i64;
    let mut app = db.appender();
    for i in 0..1000i64 {
        app.append_float(None, &[("foo", "bar")], mint + i, 1.0).unwrap();
    }
    app.commit().unwrap();
    let snap = tempfile::tempdir().unwrap();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    let db2 = Tsdb::open(snap.path(), TsdbOptions::default()).unwrap();
    let got = query_db(&db2, mint, mint + 1000, &[match_eq("foo", "bar")]);
    let sum: f64 = got[r#"{foo="bar"}"#].iter().map(|s| match s.value { SampleValue::Float(v) => v, _ => 0.0 }).sum();
    assert_eq!(sum, 1000.0);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDB_Snapshot_ChunksOutsideOfCompactedRange_AppendV2
#[test]
fn test_db_snapshot_chunks_outside_of_compacted_range_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mint = 1_414_141_414_000i64;
    let mut app = db.appender();
    for i in 0..1000i64 {
        app.append_float(None, &[("foo", "bar")], mint + i, 1.0).unwrap();
    }
    app.commit().unwrap();
    db.head().set_max_time_for_test(mint + 999 - 10);
    let snap = tempfile::tempdir().unwrap();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    let db2 = Tsdb::open(snap.path(), TsdbOptions::default()).unwrap();
    let got = query_db(&db2, mint, mint + 1000, &[match_eq("foo", "bar")]);
    let sum: f64 = got[r#"{foo="bar"}"#].iter().map(|s| match s.value { SampleValue::Float(v) => v, _ => 0.0 }).sum();
    assert_eq!(sum, 990.0);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDB_SnapshotWithDelete_AppendV2
#[test]
fn test_db_snapshot_with_delete_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    let smpls: Vec<f64> = (0..10).map(|i| i as f64 * 1.5).collect();
    for (i, &v) in smpls.iter().enumerate() {
        app.append_float(None, &[("a", "b")], i as i64, v).unwrap();
    }
    app.commit().unwrap();
    for (mint, maxt) in [(1, 3), (4, 7)] {
        db.delete(mint, maxt, &[match_eq("a", "b")]).unwrap();
    }
    let snap = tempfile::tempdir().unwrap();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    let db2 = Tsdb::open(snap.path(), TsdbOptions::default()).unwrap();
    let got = query_db(&db2, 0, 10, &[match_eq("a", "b")]);
    assert_sample_vals(&got[r#"{a="b"}"#], &[(0, smpls[0]), (8, smpls[8]), (9, smpls[9])]);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDB_e2e_AppendV2
#[test]
fn test_db_e2e_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let lbls: Vec<[(&str, &str); 3]> = vec![
        [("a", "b"), ("instance", "localhost:9090"), ("job", "prometheus")],
        [("a", "b"), ("instance", "127.0.0.1:9090"), ("job", "prometheus")],
        [("a", "b"), ("instance", "127.0.0.1:9090"), ("job", "prom-k8s")],
        [("a", "b"), ("instance", "localhost:9090"), ("job", "prom-k8s")],
        [("a", "c"), ("instance", "localhost:9090"), ("job", "prometheus")],
        [("a", "c"), ("instance", "127.0.0.1:9090"), ("job", "prometheus")],
        [("a", "c"), ("instance", "127.0.0.1:9090"), ("job", "prom-k8s")],
        [("a", "c"), ("instance", "localhost:9090"), ("job", "prom-k8s")],
    ];
    let mut rng = 42u64;
    let mut series_map: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
    let mut app = db.appender();
    for pairs in &lbls {
        let mut ts = (lcg(&mut rng) % 300) as i64;
        let mut pts = Vec::with_capacity(1000);
        for _ in 0..1000 {
            let v = (lcg(&mut rng) % 10000) as f64 / 13.0;
            pts.push((ts, v));
            app.append_float(None, pairs, ts, v).unwrap();
            ts += (lcg(&mut rng) % 3) as i64 + 1;
        }
        let key = format!(r#"{{a="{}", instance="{}", job="{}"}}"#, pairs[0].1, pairs[1].1, pairs[2].1);
        series_map.insert(key, pts);
    }
    app.commit().unwrap();

    for ms in [
        vec![match_eq("a", "b")],
        vec![match_eq("a", "b"), match_eq("job", "prom-k8s")],
        vec![match_eq("a", "c"), match_eq("instance", "localhost:9090"), match_eq("job", "prometheus")],
    ] {
        for _ in 0..100 {
            let mint = (lcg(&mut rng) % 300) as i64;
            let maxt = mint + (lcg(&mut rng) % 3000) as i64;
            let mut exp: BTreeMap<String, Vec<DecodedSample>> = BTreeMap::new();
            for pairs in &lbls {
                let l = Labels::from_pairs(*pairs);
                if ms.iter().all(|m| m.matches(l.get(&m.name))) {
                    let key = format!(r#"{{a="{}", instance="{}", job="{}"}}"#, pairs[0].1, pairs[1].1, pairs[2].1);
                    let pts: Vec<_> = series_map[&key].iter().filter(|&&(t, _)| t >= mint && t <= maxt)
                        .map(|&(t, v)| DecodedSample { st: 0, t, value: SampleValue::Float(v) }).collect();
                    if !pts.is_empty() {
                        exp.insert(key, pts);
                    }
                }
            }
            compare_series(&exp, &query_db(&db, mint, maxt, &ms), false);
        }
    }
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestWALFlushedOnDBClose_AppendV2
#[test]
fn test_wal_flushed_on_db_close_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("labelname", "labelvalue")], 0, 1.0).unwrap();
    app.commit().unwrap();
    db.close().unwrap();

    let db2 = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    assert_eq!(db2.querier(0, 1).label_values("labelname", None, &[]).unwrap(), vec!["labelvalue"]);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestWALSegmentSizeOptions_AppendV2
#[test]
fn test_wal_segment_size_options_append_v2() {
    for seg_size in [0usize, 2 * 32 * 1024, usize::MAX] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        let mut extra = TsdbExtraOptions::default();
        if seg_size == usize::MAX {
            extra.disable_wal = true;
        } else if seg_size > 0 {
            opts.head.wal_segment_size = seg_size;
        }
        let db = Tsdb::open_with_extra(dir.path(), opts.clone(), extra).unwrap();
        for i in 0..155i64 {
            let mut app = db.appender();
            let k = format!("wal{i}");
            let r = app.append_float(None, &[(k.as_str(), "size")], i, i as f64).unwrap();
            for j in 1..=78i64 {
                app.append_float(Some(r), &[], i + j, (i + j) as f64).unwrap();
            }
            app.commit().unwrap();
        }
        db.close().unwrap();

        let wal_dir = dir.path().join("wal");
        if seg_size == usize::MAX {
            assert!(!wal_dir.exists());
            assert!(dir.path().join("chunks_head").exists());
        } else {
            let exp_sz = if seg_size == 0 { DEFAULT_SEGMENT_SIZE } else { seg_size } as u64;
            let mut files: Vec<_> = fs::read_dir(&wal_dir).unwrap().filter_map(|e| e.ok())
                .filter(|e| !e.file_type().unwrap().is_dir() && e.file_name().to_str().is_some_and(|n| n.len() == 8 && n.bytes().all(|b| b.is_ascii_digit())))
                .collect();
            files.sort_by_key(|e| e.file_name());
            if seg_size > 0 { assert!(files.len() > 1); }
            for f in &files[..files.len() - 1] {
                assert_eq!(f.metadata().unwrap().len(), exp_sz);
            }
            assert!(files.last().unwrap().metadata().unwrap().len() < exp_sz);
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestWALReplayRaceOnSamplesLoggedBeforeSeries_AppendV2
#[test]
fn test_wal_replay_race_on_samples_logged_before_series_append_v2() {
    for enable_st in [false, true] {
        for after_count in [1i64, 1000] {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.enable_st_storage = enable_st;
            if enable_st { opts.head.float_chunk_encoding = mimir_tsdb::chunkenc::Encoding::Xor2; }
            let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
            db.disable_compactions();
            let enc = RecordEncoder::new(enable_st);
            for sref in 1..=200u64 {
                let mut smpls = Vec::new();
                for ts in 0..200i64 {
                    smpls.push(RefSample { ref_id: sref, st: 0, t: ts, v: ts as f64 });
                }
                let rec = enc.samples(&smpls, Vec::new());
                db.head().wal_log_raw_for_test(&rec).unwrap();
                let mut app = db.appender();
                let v = sref.to_string();
                for ts in 200..(200 + after_count) {
                    app.append_float(None, &[("series_id", v.as_str())], ts, ts as f64).unwrap();
                }
                app.commit().unwrap();
            }
            db.close().unwrap();

            let db2 = Tsdb::open(dir.path(), opts).unwrap();
            let res = db2.chunk_querier(i64::MIN, i64::MAX).select(true, &[match_re("series_id", ".+")]).unwrap();
            assert_eq!(res.len(), 200);
            for s in res {
                assert_eq!(s.chunks.len(), (after_count as usize / 120) + 1);
            }
            db2.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestTombstoneClean_AppendV2
#[test]
fn test_tombstone_clean_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    let smpls: Vec<f64> = (0..10).map(|i| i as f64 * 2.5).collect();
    for (i, &v) in smpls.iter().enumerate() {
        app.append_float(None, &[("a", "b")], i as i64, v).unwrap();
    }
    app.commit().unwrap();
    let snap = tempfile::tempdir().unwrap();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    let db2 = Tsdb::open(snap.path(), TsdbOptions::default()).unwrap();
    for (mint, maxt) in [(1, 3), (4, 7)] {
        db2.delete(mint, maxt, &[match_eq("a", "b")]).unwrap();
    }
    db2.clean_tombstones().unwrap();
    let got = query_db(&db2, 0, 10, &[match_eq("a", "b")]);
    assert_sample_vals(&got[r#"{a="b"}"#], &[(0, smpls[0]), (8, smpls[8]), (9, smpls[9])]);
    for b in db2.blocks() {
        assert_eq!(b.meta().stats.num_tombstones, 0);
    }
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestTombstoneCleanResultEmptyBlock_AppendV2
#[test]
fn test_tombstone_clean_result_empty_block_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    for i in 0..10i64 {
        app.append_float(None, &[("a", "b")], i, i as f64).unwrap();
    }
    app.commit().unwrap();
    let snap = tempfile::tempdir().unwrap();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    let db2 = Tsdb::open(snap.path(), TsdbOptions::default()).unwrap();
    db2.delete(0, 10, &[match_eq("a", "b")]).unwrap();
    db2.clean_tombstones().unwrap();
    let bdirs = fs::read_dir(snap.path()).unwrap().filter_map(|e| e.ok())
        .filter(|e| e.file_type().unwrap().is_dir() && e.file_name().len() == 26).count();
    assert_eq!(bdirs, 0);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestSizeRetention_AppendV2
#[test]
fn test_size_retention_append_v2() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let mut opts = opts_with_reg(&reg, 100);
        opts.head.ooo_time_window_ms = 100;
        opts.head.enable_st_storage = enable_st;
        if enable_st { opts.head.float_chunk_encoding = mimir_tsdb::chunkenc::Encoding::Xor2; }
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.set_block_ranges(vec![100]);
        for (mint, maxt) in [(100, 200), (200, 300), (300, 400), (400, 500), (500, 600)] {
            xor_block(db.dir(), mint, maxt, 100, 10);
        }
        {
            let mut app = db.appender();
            for i in 0..100usize {
                let s = i.to_string();
                for j in 0..=10i64 {
                    app.append_float(None, &[("i", s.as_str())], 700 + j * 10, (i + j as usize) as f64).unwrap();
                }
            }
            app.commit().unwrap();
        }
        db.force_head_mmap().unwrap();
        db.reload_blocks().unwrap();
        assert_eq!(db.blocks().len(), 5);

        let check_sz = |with_wbl: bool| {
            let b_sz = metric_val(&reg, "prometheus_tsdb_storage_blocks_bytes", &[]) as u64;
            let wal_sz = db.head().wal_size().unwrap();
            let cdm_sz = db.head().chunks_head_size().unwrap();
            assert!(cdm_sz > 0);
            let wbl_sz = if with_wbl { db.head().wbl_size().unwrap() } else { 0 };
            if with_wbl { assert!(wbl_sz > 0); }
            let exp = b_sz + wal_sz + wbl_sz + cdm_sz;
            assert_eq!(exp, dir_size(db.dir()));
            exp
        };
        check_sz(false);

        let (first, last) = segments_range(&db.dir().join("wal")).unwrap().unwrap();
        db.head().wal_checkpoint_for_test(first, last.saturating_sub(1), |_| false, 0).unwrap();
        check_sz(false);

        db.head().truncate_chunk_disk_mapper_for_test(900).unwrap();
        check_sz(false);

        {
            let mut app = db.appender();
            for ts in 750..800i64 {
                app.append_float(None, &[("i", "0")], ts, ts as f64).unwrap();
            }
            app.commit().unwrap();
        }
        let act_sz = check_sz(true);
        let first_block_sz = dir_size(db.blocks()[0].dir());
        let size_limit = (act_sz - first_block_sz) as i64;
        db.set_max_bytes_for_test(size_limit);
        db.reload_blocks().unwrap();

        assert_eq!(metric_val(&reg, "prometheus_tsdb_size_retentions_total", &[]), 1.0);
        let new_sz = check_sz(true);
        assert!(new_sz <= size_limit as u64);
        assert_eq!(db.blocks().len(), 4);
        assert_eq!(db.blocks()[0].max_time(), 300);
        assert_eq!(db.blocks().last().unwrap().max_time(), 600);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestNotMatcherSelectsLabelsUnsetSeries_AppendV2
#[test]
fn test_not_matcher_selects_labels_unset_series_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "abcd"), ("b", "abcde")], 0, 1.0).unwrap();
    app.append_float(None, &[("labelname", "labelvalue")], 0, 1.0).unwrap();
    app.commit().unwrap();

    let s0 = r#"{a="abcd", b="abcde"}"#;
    let s1 = r#"{labelname="labelvalue"}"#;
    let ne = |n, v| LabelMatcher::new(MatchType::NotEqual, n, v).unwrap();
    let nre = |n, v| LabelMatcher::new(MatchType::RegexNotMatch, n, v).unwrap();
    for (ms, exp) in [
        (vec![ne("lname", "lvalue")], vec![s0, s1]),
        (vec![match_eq("a", "abcd"), ne("b", "abcde")], vec![]),
        (vec![match_eq("a", "abcd"), ne("b", "abc")], vec![s0]),
        (vec![nre("a", "abd.*")], vec![s0, s1]),
        (vec![nre("a", "abc.*")], vec![s1]),
        (vec![nre("c", "abd.*")], vec![s0, s1]),
        (vec![nre("labelname", "labelvalue")], vec![s0]),
    ] {
        let got: Vec<String> = query_db(&db, 0, 10, &ms).into_keys().collect();
        assert_eq!(got, exp);
    }
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestChunkAtBlockBoundary_AppendV2
#[test]
fn test_chunk_at_block_boundary_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let br = DEFAULT_BLOCK_DURATION_MS;
    let mut app = db.appender();
    for i in 0..3i64 {
        app.append_float(None, &[("foo", "bar")], i * br, 0.0).unwrap();
        app.append_float(None, &[("foo", "bar")], i * br + 1000, 0.0).unwrap();
    }
    app.commit().unwrap();
    db.compact().unwrap();
    for b in db.blocks() {
        let m = b.meta();
        let pids = b.index().postings("", &[""]).unwrap();
        let mut cnt = 0usize;
        for id in pids {
            let (_, chks) = b.index().series(id).unwrap();
            for c in chks {
                assert!(m.min_time <= c.min_time && c.max_time <= m.max_time);
                cnt += 1;
            }
        }
        assert_eq!(cnt, 1);
    }
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestQuerierWithBoundaryChunks_AppendV2
#[test]
fn test_querier_with_boundary_chunks_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let br = DEFAULT_BLOCK_DURATION_MS;
    let mut app = db.appender();
    for i in 0..5i64 {
        let s = i.to_string();
        app.append_float(None, &[("foo", "bar")], i * br, 0.0).unwrap();
        app.append_float(None, &[("blockID", s.as_str())], i * br, 0.0).unwrap();
    }
    app.commit().unwrap();
    db.compact().unwrap();
    assert!(db.blocks().len() >= 3);
    let got = db.querier(br, 2 * br).label_values("blockID", None, &[]).unwrap();
    assert_eq!(got, vec!["1", "2"]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestInitializeHeadTimestamp_AppendV2
#[test]
fn test_initialize_head_timestamp_append_v2() {
    {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        assert_eq!(db.head().min_time(), i64::MAX);
        assert_eq!(db.head().max_time(), i64::MIN);
        assert!(!db.head().initialized());
        let mut app = db.appender();
        app.append_float(None, &[("a", "b")], 1000, 1.0).unwrap();
        assert_eq!((db.head().min_time(), db.head().max_time(), db.head().initialized()), (1000, 1000, true));
        app.rollback().unwrap();
        db.close().unwrap();
    }
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(enable_st);
        let r1 = enc.series(&[
            RefSeries { ref_id: 123, labels: Labels::from_pairs([("a", "1")]) },
            RefSeries { ref_id: 124, labels: Labels::from_pairs([("a", "2")]) },
        ], Vec::new());
        let r2 = enc.samples(&[
            RefSample { ref_id: 123, st: 0, t: 5000, v: 1.0 },
            RefSample { ref_id: 124, st: 0, t: 15000, v: 1.0 },
        ], Vec::new());
        w.log(&[&r1, &r2]).unwrap();
        w.close().unwrap();

        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        assert_eq!((db.head().min_time(), db.head().max_time(), db.head().initialized()), (5000, 15000, true));
        db.close().unwrap();
    }
    {
        let dir = tempfile::tempdir().unwrap();
        xor_block(dir.path(), 1000, 2000, 1, 1);
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        assert_eq!((db.head().min_time(), db.head().max_time(), db.head().initialized()), (2000, 2000, true));
        db.close().unwrap();
    }
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        xor_block(dir.path(), 1000, 6000, 1, 1);
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        let enc = RecordEncoder::new(enable_st);
        let r1 = enc.series(&[
            RefSeries { ref_id: 123, labels: Labels::from_pairs([("a", "1")]) },
            RefSeries { ref_id: 124, labels: Labels::from_pairs([("a", "2")]) },
        ], Vec::new());
        let r2 = enc.samples(&[
            RefSample { ref_id: 123, st: 0, t: 5000, v: 1.0 },
            RefSample { ref_id: 124, st: 0, t: 15000, v: 1.0 },
        ], Vec::new());
        w.log(&[&r1, &r2]).unwrap();
        w.close().unwrap();

        let reg = Arc::new(prometheus::Registry::new());
        let opts = opts_with_reg(&reg, DEFAULT_BLOCK_DURATION_MS);
        let db = Tsdb::open(dir.path(), opts).unwrap();
        assert_eq!((db.head().min_time(), db.head().max_time(), db.head().initialized()), (6000, 15000, true));
        assert_eq!(metric_val(&reg, "prometheus_tsdb_head_series", &[]), 1.0);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestNoEmptyBlocks_AppendV2
#[test]
fn test_no_empty_blocks_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let db = Tsdb::open(dir.path(), opts_with_reg(&reg, 100)).unwrap();
    db.set_block_ranges(vec![100]);
    db.disable_compactions();
    let range_trig = 100 / 2 * 3 - 1;
    let all_m = match_re("foo", ".*");

    db.compact().unwrap();
    assert!(db.blocks().is_empty());
    assert_eq!(metric_val(&reg, "prometheus_tsdb_compactions_total", &[]), 0.0);

    let mut app = db.appender();
    for t in [1, 2, 3 + range_trig] { app.append_float(None, &[("foo", "bar")], t, 0.0).unwrap(); }
    app.commit().unwrap();
    db.delete(i64::MIN, i64::MAX, std::slice::from_ref(&all_m)).unwrap();
    db.compact().unwrap();
    assert!(db.blocks().is_empty());
    assert!(metric_val(&reg, "prometheus_tsdb_compactions_total", &[]) >= 1.0);

    let mut app = db.appender();
    assert!(matches!(app.append_float(None, &[("foo", "bar")], 1, 0.0), Err(HeadError::OutOfBounds)));
    let cur = db.head().max_time();
    for t in [cur, cur + 1, cur + range_trig] { app.append_float(None, &[("foo", "bar")], t, 0.0).unwrap(); }
    app.commit().unwrap();
    db.compact().unwrap();
    assert_eq!(db.blocks().len(), 1);

    let old_ulids = db.block_ulids();
    let mut app = db.appender();
    let cur = db.head().max_time();
    for t in [cur, cur + 1, cur + range_trig] { app.append_float(None, &[("foo", "bar")], t, 0.0).unwrap(); }
    app.commit().unwrap();
    db.head().delete(i64::MIN, i64::MAX, std::slice::from_ref(&all_m)).unwrap();
    db.compact().unwrap();
    assert_eq!(db.block_ulids(), old_ulids);

    let cur = db.head().max_time();
    xor_block(db.dir(), cur, cur + 100, 2, 2);
    xor_block(db.dir(), cur + 100, cur + 200, 2, 2);
    db.reload_blocks().unwrap();
    assert_eq!(db.blocks().len(), 3);
    db.delete(i64::MIN, i64::MAX, &[match_re("", ".*")]).unwrap();
    db.compact().unwrap();
    assert_eq!(db.blocks().len(), 1);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDB_LabelNames_AppendV2
#[test]
fn test_db_label_names_append_v2() {
    type LnCase<'a> = (&'a [(&'a str, &'a str)], &'a [(&'a str, &'a str)], &'a [&'a str], &'a [&'a str]);
    let cases: &[LnCase<'_>] = &[
        (&[("name1", "1"), ("name3", "3"), ("name2", "2")], &[("name4", "4"), ("name1", "1")], &["name1", "name2", "name3"], &["name1", "name2", "name3", "name4"]),
        (&[("name2", "2"), ("name1", "1"), ("name2", "2")], &[("name6", "6"), ("name0", "0")], &["name1", "name2"], &["name0", "name1", "name2", "name6"]),
    ];
    for &(l1, l2, exp1, exp2) in cases {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db_range(dir.path(), DEFAULT_BLOCK_DURATION_MS);
        let add = |mint: i64, maxt: i64, pairs: &[(&str, &str)]| {
            let mut app = db.appender();
            for i in mint..=maxt {
                for &(k, v) in pairs {
                    app.append_float(None, &[(k, v)], i * 1000, 0.0).unwrap();
                }
            }
            app.commit().unwrap();
        };
        add(0, 4, l1);
        let got_h = db.head().label_names(&[]).unwrap();
        assert_eq!(got_h, exp1);
        db.compact_head_all().unwrap();
        for b in db.blocks() {
            assert_eq!(b.index().label_names(), exp1);
        }
        add(5, 9, l2);
        let got_all = db.querier(i64::MIN, i64::MAX).label_names(&[]).unwrap();
        assert_eq!(got_all, exp2);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestCorrectNumTombstones_AppendV2
#[test]
fn test_correct_num_tombstones_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    for i in 0..3i64 {
        for j in 0..15i64 {
            app.append_float(None, &[("foo", "bar")], i * DEFAULT_BLOCK_DURATION_MS + j, 0.0).unwrap();
        }
    }
    app.commit().unwrap();
    db.compact().unwrap();
    assert_eq!(db.blocks().len(), 1);
    let chk_tombs = |want: u64| {
        assert_eq!(db.blocks()[0].tombstones().total(), want);
        assert_eq!(mimir_tsdb::block::read_meta_file(db.blocks()[0].dir()).unwrap().stats.num_tombstones, want);
    };
    db.delete(0, 1, &[match_eq("foo", "bar")]).unwrap();
    chk_tombs(1);
    db.delete(2, 3, &[match_eq("foo", "bar")]).unwrap();
    chk_tombs(1);
    db.delete(5, 6, &[match_eq("foo", "bar")]).unwrap();
    chk_tombs(2);
    db.delete(9, 11, &[match_eq("foo", "bar")]).unwrap();
    chk_tombs(3);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestBlockRanges_AppendV2
#[test]
fn test_block_ranges_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let first_maxt = 3i64;
    xor_block(dir.path(), 0, first_maxt, 1, 1);
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = 10000;
    let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
    db.set_block_ranges(vec![10000]);
    db.disable_compactions();
    let range_trig = 10000 / 2 * 3 + 1;
    let mut app = db.appender();
    assert!(app.append_float(None, &[("a", "b")], first_maxt - 1, 1.0).is_err());
    app.append_float(None, &[("a", "b")], first_maxt + 1, 1.0).unwrap();
    app.append_float(None, &[("a", "b")], first_maxt + 2, 1.0).unwrap();
    let second_maxt = first_maxt + range_trig;
    app.append_float(None, &[("a", "b")], second_maxt, 1.0).unwrap();
    app.commit().unwrap();
    db.compact().unwrap();
    let b1 = db.blocks();
    assert_eq!(b1.len(), 2);
    assert!(b1[1].min_time() >= b1[0].max_time());

    let mut app = db.appender();
    for dt in 1..=4 { app.append_float(None, &[("a", "b")], second_maxt + dt, 1.0).unwrap(); }
    app.commit().unwrap();
    db.close().unwrap();

    let third_maxt = second_maxt + 2;
    xor_block(dir.path(), second_maxt + 1, third_maxt, 1, 1);
    let db2 = Tsdb::open(dir.path(), opts).unwrap();
    db2.set_block_ranges(vec![10000]);
    db2.disable_compactions();
    assert_eq!(db2.blocks().len(), 3);
    assert_eq!(db2.blocks()[2].max_time(), third_maxt);
    let mut app = db2.appender();
    app.append_float(None, &[("a", "b")], third_maxt + range_trig, 1.0).unwrap();
    app.commit().unwrap();
    db2.compact().unwrap();
    let b2 = db2.blocks();
    assert_eq!(b2.len(), 4);
    assert!(b2[3].min_time() >= b2[2].max_time());
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBReadOnly_AppendV2
#[test]
fn test_db_read_only_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    for (mint, maxt) in [(10, 12), (12, 14), (14, 16)] {
        xor_block(dir.path(), mint, maxt, 1, 1);
    }
    let db_w = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db_w.disable_compactions();
    let sz_before = dir_size(dir.path());
    let mut app = db_w.appender();
    app.append_float(None, &[("foo", "bar")], db_w.head().max_time() + 1, 0.0).unwrap();
    app.commit().unwrap();
    let exp_blocks: Vec<_> = db_w.blocks().into_iter().map(|b| b.ulid()).collect();
    assert!(dir_size(dir.path()) > sz_before);
    let exp_series = query_db(&db_w, i64::MIN, i64::MAX, &[match_eq("", "")]);
    let exp_chunks = query_chunks_expanded(&db_w, i64::MIN, i64::MAX, &[match_eq("", "")]);
    db_w.close().unwrap();
    let exp_hash = dir_hash(dir.path());

    let ro = TsdbReadOnly::open(dir.path(), None).unwrap();
    let ro_blks = ro.blocks().unwrap();
    assert_eq!(ro_blks.len(), exp_blocks.len());
    for (e, g) in exp_blocks.iter().zip(&ro_blks) { assert_eq!(*e, g.ulid()); }
    assert_eq!(ro.block(&exp_blocks[0].to_string()).unwrap().ulid(), exp_blocks[0]);
    assert!(ro.block("01GTDVZZF52NSWB5SXQF0P2PGF").is_err());
    assert_eq!(ro.last_block_id().unwrap(), exp_blocks[2].to_string());

    let mut ro_s = BTreeMap::new();
    for s in ro.querier(i64::MIN, i64::MAX).unwrap().select(true, &[match_eq("", "")]).unwrap() {
        ro_s.insert(db_test_helpers::labels_str(&s.labels), s.samples);
    }
    assert_eq!(ro_s, exp_series);
    assert_eq!(dir_hash(dir.path()), exp_hash);

    let mut ro_c = BTreeMap::new();
    for s in ro.chunk_querier(i64::MIN, i64::MAX).unwrap().select(true, &[match_eq("", "")]).unwrap() {
        let chks: Vec<_> = s.chunks.into_iter().filter_map(|cm| cm.chunk.map(|e| mimir_tsdb::compact::decode_chunk_samples(&e).unwrap())).collect();
        if !chks.is_empty() { ro_c.insert(db_test_helpers::labels_str(&s.labels), chks); }
    }
    assert_eq!(ro_c, exp_chunks);
    assert_eq!(dir_hash(dir.path()), exp_hash);
    ro.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBReadOnly_FlushWAL_AppendV2
#[test]
fn test_db_read_only_flush_wal_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.disable_compactions();
    let mut app = db.appender();
    for i in 0..1000i64 { app.append_float(None, &[("a", "flush")], i, 1.0).unwrap(); }
    app.commit().unwrap();
    db.close().unwrap();

    let ro = TsdbReadOnly::open(dir.path(), None).unwrap();
    let flush = tempfile::tempdir().unwrap();
    ro.flush_wal(flush.path()).unwrap();
    ro.close().unwrap();

    let ro2 = TsdbReadOnly::open(flush.path(), None).unwrap();
    assert_eq!(ro2.blocks().unwrap().len(), 1);
    let got = ro2.querier(0, 999).unwrap().select(true, &[match_eq("a", "flush")]).unwrap();
    let sum: f64 = got[0].samples.iter().map(|s| match s.value { SampleValue::Float(v) => v, _ => 0.0 }).sum();
    assert_eq!(sum, 1000.0);
    ro2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBReadOnly_Querier_NoAlteration_AppendV2
#[test]
fn test_db_read_only_querier_no_alteration_append_v2() {
    let check_ro = |dir: &std::path::Path, exp_chunks: usize| {
        let sb = tempfile::tempdir().unwrap();
        let ro = TsdbReadOnly::open(dir, Some(sb.path())).unwrap();
        let cnt = fs::read_dir(dir.join("chunks_head")).unwrap().count();
        assert_eq!(cnt, exp_chunks);
        let _ = ro.querier(i64::MIN, i64::MAX).unwrap().select(true, &[match_re("foo", ".*")]).unwrap();
        ro.close().unwrap();
        assert_eq!(fs::read_dir(dir.join("chunks_head")).unwrap().count(), exp_chunks);
    };
    {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        for i in 0..121i64 {
            let mut app = db.appender();
            app.append_float(None, &[("foo", "bar")], i, 0.0).unwrap();
            app.commit().unwrap();
        }
        check_ro(dir.path(), 0);
        db.force_head_mmap().unwrap();
        assert_eq!(fs::read_dir(dir.path().join("chunks_head")).unwrap().count(), 1);
        db.close().unwrap();
    }
    {
        let dir = tempfile::tempdir().unwrap();
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        db.close().unwrap();
        fs::File::create(dir.path().join("chunks_head").join("000001")).unwrap();
        check_ro(dir.path(), 1);
        let db2 = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        assert_eq!(fs::read_dir(dir.path().join("chunks_head")).unwrap().count(), 0);
        db2.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBCannotSeePartialCommits_AppendV2
#[test]
fn test_db_cannot_see_partial_commits_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Tsdb::open(dir.path(), TsdbOptions::default()).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::channel();
    let db_w = Arc::clone(&db);
    let stop_w = Arc::clone(&stop);
    let h = std::thread::spawn(move || {
        let mut iter = 0i64;
        while !stop_w.load(Ordering::Relaxed) {
            let mut app = db_w.appender();
            for j in 0..100 {
                let s = j.to_string();
                app.append_float(None, &[("a", s.as_str()), ("foo", "bar")], iter, iter as f64).unwrap();
            }
            app.commit().unwrap();
            if iter == 0 { let _ = tx.send(()); }
            iter += 1;
        }
    });
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..10 {
        let got = query_db(&db, 0, 1_000_000, &[match_eq("foo", "bar")]);
        let vals: std::collections::HashSet<u64> = got.values().map(|v| match v.last().unwrap().value { SampleValue::Float(f) => f.to_bits(), _ => 0 }).collect();
        assert_eq!(vals.len(), 1);
    }
    stop.store(true, Ordering::Relaxed);
    h.join().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBQueryDoesntSeeAppendsAfterCreation_AppendV2
#[test]
fn test_db_query_doesnt_see_appends_after_creation_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let q_before = db.querier(0, 1_000_000);
    let mut app = db.appender();
    app.append_float(None, &[("foo", "bar")], 0, 0.0).unwrap();
    let q_during = db.querier(0, 1_000_000);

    assert!(q_before.select(true, &[match_eq("foo", "bar")]).unwrap().is_empty());
    assert!(q_during.select(true, &[match_eq("foo", "bar")]).unwrap().is_empty());
    app.commit().unwrap();

    assert!(q_before.select(true, &[match_eq("foo", "bar")]).unwrap().is_empty());
    assert!(q_during.select(true, &[match_eq("foo", "bar")]).unwrap().is_empty());
    assert_eq!(db.head().postings_for_matchers(&[match_eq("foo", "bar")]).unwrap().len(), 1);
    drop(q_before);
    drop(q_during);

    assert_sample_vals(&query_db(&db, 0, 1_000_000, &[match_eq("foo", "bar")])[r#"{foo="bar"}"#], &[(0, 0.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestCompactHead_AppendV2
#[test]
fn test_compact_head_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions { no_lockfile: true, ..TsdbOptions::default() };
    opts.head.wal_compression = CompressionType::Snappy;
    let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
    let mut app = db.appender();
    let mut exp = Vec::new();
    for i in 0..100i64 {
        let v = (i as f64) * 1.25;
        app.append_float(None, &[("a", "b")], i, v).unwrap();
        exp.push((i, v));
    }
    app.commit().unwrap();
    db.compact_head_range_inclusive(0, 99).unwrap().unwrap();
    db.close().unwrap();

    for e in fs::read_dir(dir.path()).unwrap().filter_map(|x| x.ok()) {
        if !(e.file_type().unwrap().is_dir() && e.file_name().len() == 26) {
            if e.file_type().unwrap().is_dir() { fs::remove_dir_all(e.path()).unwrap(); } else { fs::remove_file(e.path()).unwrap(); }
        }
    }
    let db2 = Tsdb::open(dir.path(), opts).unwrap();
    assert_eq!(db2.blocks().len(), 1);
    assert_eq!(db2.head().min_time(), 100);
    let got = query_db(&db2, 0, 99, &[match_eq("a", "b")]);
    assert_sample_vals(&got[r#"{a="b"}"#], &exp);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestCompactHeadWithDeletion_AppendV2
#[test]
fn test_compact_head_with_deletion_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("a", "b")], 10, 42.0).unwrap();
    app.commit().unwrap();
    db.delete(0, 100, &[match_eq("a", "b")]).unwrap();
    db.compact_head_range_inclusive(0, 100).unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOneCheckpointPerCompactCall_AppendV2
#[test]
fn test_one_checkpoint_per_compact_call_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let br = 1000i64;
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = opts_with_reg(&reg, br);
    opts.retention_duration_ms = br * 1000;
    opts.no_lockfile = true;
    let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
    db.set_block_ranges(vec![br]);
    db.disable_compactions();

    let mut app = db.appender();
    for i in 0..60i64 {
        app.append_float(None, &[("foo_d", "choco_bar")], br * i, i as f64).unwrap();
        app.append_float(None, &[("foo_d", "choco_bar")], br * i + br / 2, i as f64 + 0.5).unwrap();
        db.head().wal_next_segment_for_test().unwrap();
    }
    app.commit().unwrap();
    assert_eq!(segments_range(&dir.path().join("wal")).unwrap(), Some((0, 60)));
    assert_eq!(metric_val(&reg, "prometheus_tsdb_checkpoint_creations_total", &[]), 0.0);
    db.compact().unwrap();
    assert_eq!(metric_val(&reg, "prometheus_tsdb_checkpoint_creations_total", &[]), 1.0);
    assert_eq!(db.blocks().len(), 58);
    assert_eq!(metric_val(&reg, "prometheus_tsdb_head_truncations_total", &[]), 58.0);
    assert_eq!(segments_range(&dir.path().join("wal")).unwrap(), Some((40, 61)));
    assert_eq!(last_checkpoint(&dir.path().join("wal")).unwrap().unwrap().1, 39);

    let last_blk_maxt = db.blocks().last().unwrap().max_time();
    let new_blk_maxt = db.head().max_time() + 1;
    db.close().unwrap();
    xor_block(dir.path(), last_blk_maxt, new_blk_maxt, 1, 1);

    let reg2 = Arc::new(prometheus::Registry::new());
    let mut opts2 = opts_with_reg(&reg2, br);
    opts2.retention_duration_ms = br * 1000;
    opts2.no_lockfile = true;
    let db2 = Tsdb::open(dir.path(), opts2).unwrap();
    db2.set_block_ranges(vec![br]);
    db2.disable_compactions();
    assert_eq!(db2.blocks().len(), 59);
    assert_eq!(db2.head().num_series(), 0);
    let mut app = db2.appender();
    app.append_float(None, &[("foo_d", "choco_bar")], br * 120, 1.0).unwrap();
    app.commit().unwrap();
    assert_eq!(db2.head().min_time(), new_blk_maxt);
    assert_eq!(segments_range(&dir.path().join("wal")).unwrap(), Some((40, 62)));
    assert_eq!(metric_val(&reg2, "prometheus_tsdb_checkpoint_creations_total", &[]), 0.0);
    db2.compact().unwrap();
    assert_eq!(metric_val(&reg2, "prometheus_tsdb_checkpoint_creations_total", &[]), 1.0);
    assert_eq!(db2.blocks().len(), 59);
    assert_eq!(segments_range(&dir.path().join("wal")).unwrap(), Some((55, 63)));
    assert_eq!(last_checkpoint(&dir.path().join("wal")).unwrap().unwrap().1, 54);
    db2.close().unwrap();
}
