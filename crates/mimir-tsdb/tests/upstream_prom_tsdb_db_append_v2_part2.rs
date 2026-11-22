//! Ported upstream Prometheus TSDB db_append_v2_test.go (part 2 of 2, 36 tests).

mod db_test_helpers;

use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;


use db_test_helpers::{
    append_kind_v2, compare_series, copy_dir_recursive, gen_test_fhist, gen_test_hist, lcg_next,
    make_sample_kind, match_eq, match_re, metric_val, opts_with_reg, query_chunks_expanded,
    query_db, read_all_wal_records, run_histogram_append_and_query_test, run_ooo_compaction_race_v2,
    run_ooo_querier_cases, run_ooo_runtime_config_test, set_crh, SampleKind, ALL_SAMPLE_KINDS,
    DEFAULT_BLOCK_DURATION_MS, HOUR_MS,
};
use mimir_tsdb::block::{Block, BlockMeta};
use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHint, Encoding, FloatHistogram, Histogram, HistogramSpan,
    CUSTOM_BUCKETS_SCHEMA,
};
use mimir_tsdb::compact::{DecodedSample, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbCompactorOverride, TsdbError, TsdbExtraOptions, TsdbOptions, TsdbRuntimeConfig};
use mimir_tsdb::record::{DecodedRecord, MetricType};
use mimir_tsdb::wlog::{segments_range, CompressionType};
use ulid::Ulid;

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestQuerierShouldNotFailIfOOOCompactionOccursAfterRetrievingQuerier_AppendV2
#[test]
fn test_querier_not_fail_if_ooo_compaction_after_retrieving_querier_append_v2() { run_ooo_compaction_race_v2(0); }

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestQuerierShouldNotFailIfOOOCompactionOccursAfterSelecting_AppendV2
#[test]
fn test_querier_not_fail_if_ooo_compaction_after_selecting_append_v2() { run_ooo_compaction_race_v2(1); }

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestQuerierShouldNotFailIfOOOCompactionOccursAfterRetrievingIterators_AppendV2
#[test]
fn test_querier_not_fail_if_ooo_compaction_after_retrieving_iterators_append_v2() { run_ooo_compaction_race_v2(2); }

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOWALWrite_AppendV2
#[test]
fn test_ooo_wal_write_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in [SampleKind::Float, SampleKind::IntHist, SampleKind::FloatHist, SampleKind::CustomBucketsIntHist, SampleKind::CustomBucketsFloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 2;
        opts.head.ooo_time_window_ms = 30 * 60_000;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let s1 = [("l", "v1")];
        let s2 = [("l", "v2")];

        let mut app = db.appender();
        append_kind_v2(&mut app, None, kind, &s1, min_ms(60), 60).unwrap();
        append_kind_v2(&mut app, None, kind, &s2, min_ms(60), 60).unwrap();
        app.commit().unwrap();
        let mut app = db.appender();
        append_kind_v2(&mut app, None, kind, &s1, min_ms(40), 40).unwrap();
        app.commit().unwrap();
        let mut app = db.appender();
        append_kind_v2(&mut app, None, kind, &s2, min_ms(42), 42).unwrap();
        app.commit().unwrap();
        let mut app = db.appender();
        append_kind_v2(&mut app, None, kind, &s2, min_ms(45), 45).unwrap();
        for m in [35, 36, 37] { append_kind_v2(&mut app, None, kind, &s1, min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        let mut app = db.appender();
        append_kind_v2(&mut app, None, kind, &s1, min_ms(50), 50).unwrap();
        append_kind_v2(&mut app, None, kind, &s2, min_ms(65), 65).unwrap();
        app.commit().unwrap();
        let mut app = db.appender();
        for m in [50, 51, 52, 53] { append_kind_v2(&mut app, None, kind, &s2, min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        db.head().wbl_sync_for_test().unwrap();

        let wal_recs = read_all_wal_records(&dir.path().join("wal"));
        assert!(wal_recs.iter().any(|r| matches!(r, DecodedRecord::Series(v) if v.len() == 2)));
        let wbl_recs = read_all_wal_records(&dir.path().join("wbl"));
        let mut mmap_refs = Vec::new();
        for r in &wbl_recs {
            if let DecodedRecord::MmapMarkers(ms) = r {
                for m in ms { mmap_refs.push((m.ref_id, m.mmap_ref)); }
            }
        }
        assert_eq!(mmap_refs.len(), 6);
        assert_eq!(mmap_refs[0], (1, 0));
        assert_eq!(mmap_refs[1], (2, 0));
        assert!(mmap_refs[2].1 > 0 && mmap_refs[3].1 > mmap_refs[2].1 && mmap_refs[4].1 > mmap_refs[3].1 && mmap_refs[5].1 > mmap_refs[4].1);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBPanicOnMmappingHeadChunk_AppendV2
#[test]
fn test_db_panic_on_mmapping_head_chunk_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db.disable_compactions();
    let itvl = 45_000i64;
    let mut last_ts = 0i64;
    let add_samples = |d: &Tsdb, n: usize, ts: &mut i64| {
        let mut app = d.appender();
        let mut r = None;
        for i in 0..n {
            r = Some(app.append_float(r, &[("__name__", "testing"), ("foo", "bar")], *ts, *ts as f64).unwrap());
            *ts += itvl;
            if i % 10 == 0 { app.commit().unwrap(); app = d.appender(); }
        }
        app.commit().unwrap();
    };
    add_samples(&db, (170 * 60_000 / itvl) as usize, &mut last_ts);
    db.compact().unwrap();
    assert!(db.blocks().is_empty());
    db.close().unwrap();

    let db2 = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    db2.disable_compactions();
    add_samples(&db2, (20 * 60_000 / itvl) as usize, &mut last_ts);
    db2.compact().unwrap();
    assert_eq!(db2.blocks().len(), 1);
    add_samples(&db2, (120 * 60_000 / itvl) as usize, &mut last_ts);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestMetadataInWAL_AppenderV2
#[test]
fn test_metadata_in_wal_appender_v2() {
    let dir = tempfile::tempdir().unwrap();
    let extra = TsdbExtraOptions { enable_metadata_wal_records: Some(true), ..Default::default() };
    let db = Tsdb::open_with_extra(dir.path(), TsdbOptions::default(), extra).unwrap();
    let (s1, s2, s3, s4) = ([("a", "b")], [("c", "d")], [("e", "f")], [("g", "h")]);
    let mut app = db.appender();
    for (p, t, u, h) in [(&s1, MetricType::Gauge, "unit_1", "help_1"), (&s2, MetricType::Gauge, "unit_2", "help_2"), (&s3, MetricType::Gauge, "unit_3", "help_3")] {
        app.append_float(None, p, 0, 0.0).unwrap();
        app.update_metadata(None, p, "", t, u, h).unwrap();
    }
    app.append_float(None, &s4, 0, 0.0).unwrap();
    app.commit().unwrap();

    let mut app = db.appender();
    app.append_float(None, &s1, 1, 0.0).unwrap();
    app.update_metadata(None, &s1, "", MetricType::Gauge, "unit_1", "help_1").unwrap();
    app.append_float(None, &s4, 1, 0.0).unwrap();
    app.update_metadata(None, &s4, "", MetricType::Counter, "unit_4", "help_4").unwrap();
    app.append_float(None, &s2, 1, 0.0).unwrap();
    app.update_metadata(None, &s2, "", MetricType::Counter, "unit_5", "help_5").unwrap();
    app.commit().unwrap();

    let meta_blocks: Vec<_> = read_all_wal_records(&dir.path().join("wal")).into_iter().filter_map(|r| match r {
        DecodedRecord::Metadata(v) => Some(v),
        _ => None,
    }).collect();
    assert_eq!(meta_blocks.len(), 2);
    assert_eq!(meta_blocks[0].len(), 3);
    assert_eq!(meta_blocks[1].len(), 2);
    assert_eq!(meta_blocks[1][0].ref_id, 4);
    assert_eq!(meta_blocks[1][1].ref_id, 2);
    db.close().unwrap();

    // Stale samples subtest
    for stale_case in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.max_exemplars = 10;
        let extra = TsdbExtraOptions { enable_metadata_wal_records: Some(true), ..Default::default() };
        let db = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
        let mut app = db.appender();
        let sn = stale_nan();
        match stale_case {
            0 => { app.append_float(None, &s1, 0, sn).unwrap(); }
            1 => { app.append_histogram(None, &s1, 0, 0, Histogram { sum: sn, ..Histogram::default() }).unwrap(); }
            2 => { app.append_float_histogram(None, &s1, 0, 0, FloatHistogram { sum: sn, ..FloatHistogram::default() }).unwrap(); }
            3 => {
                app.append_histogram(None, &s1, 0, 0, gen_test_hist(1)).unwrap();
                app.update_metadata(None, &s1, "", MetricType::Gauge, "unit_1", "help_1").unwrap();
                app.append_float(None, &s1, 1, sn).unwrap();
            }
            _ => {
                app.append_float_histogram(None, &s1, 0, 0, gen_test_fhist(1)).unwrap();
                app.update_metadata(None, &s1, "", MetricType::Gauge, "unit_1", "help_1").unwrap();
                app.append_float(None, &s1, 1, sn).unwrap();
            }
        }
        app.commit().unwrap();
        let mut got_meta = 0usize;
        let mut got_ex = 0usize;
        for rec in read_all_wal_records(&dir.path().join("wal")) {
            match rec {
                DecodedRecord::Metadata(v) => got_meta += v.len(),
                DecodedRecord::Exemplars(v) => got_ex += v.len(),
                _ => {}
            }
        }
        assert_eq!(got_meta, if stale_case >= 3 { 1 } else { 0 });
        assert_eq!(got_ex, 0);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestMetadataCheckpointingOnlyKeepsLatestEntry_AppendV2
#[test]
fn test_metadata_checkpointing_only_keeps_latest_entry_append_v2() {
    for enable_st in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.enable_st_storage = enable_st;
        if enable_st { opts.head.float_chunk_encoding = Encoding::Xor2; }
        let extra = TsdbExtraOptions { enable_metadata_wal_records: Some(true), ..Default::default() };
        let db = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
        let (s1, s2, s3, s4) = ([("a", "b")], [("c", "d")], [("e", "f")], [("g", "h")]);
        let mut app = db.appender();
        for (p, u) in [(&s1, "u1"), (&s2, "u2"), (&s3, "u3"), (&s4, "u4")] {
            app.append_float(None, p, 0, 0.0).unwrap();
            app.update_metadata(None, p, "", MetricType::Gauge, u, u).unwrap();
        }
        app.commit().unwrap();
        for (ts, (p, u)) in (1i64..).zip([(&s1, "u5"), (&s2, "u6"), (&s2, "u2"), (&s2, "u6"), (&s2, "u2"), (&s2, "u6")]) {
            let mut app = db.appender();
            app.append_float(None, p, ts, 0.0).unwrap();
            app.update_metadata(None, p, "", MetricType::Counter, u, u).unwrap();
            app.commit().unwrap();
        }
        db.head().wal_next_segment_for_test().unwrap();
        let (first, last) = segments_range(&dir.path().join("wal")).unwrap().unwrap();
        db.head().wal_checkpoint_for_test(first, last.saturating_sub(1), |id| id != 3, 0).unwrap();
        let (cdir, _) = mimir_tsdb::wlog::last_checkpoint(&dir.path().join("wal")).unwrap().unwrap();
        let mut meta_recs: Vec<_> = read_all_wal_records(&cdir).into_iter().filter_map(|r| match r {
            DecodedRecord::Metadata(v) => Some(v),
            _ => None,
        }).collect();
        assert_eq!(meta_recs.len(), 1);
        meta_recs[0].sort_by_key(|m| m.ref_id);
        let got: Vec<(u64, String)> = meta_recs[0].iter().map(|m| (m.ref_id, m.unit.clone())).collect();
        assert_eq!(got, vec![(1, "u5".into()), (2, "u6".into()), (4, "u4".into())]);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestMetadataAssertInMemoryData_AppendV2
#[test]
fn test_metadata_assert_in_memory_data_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let extra = TsdbExtraOptions { enable_metadata_wal_records: Some(true), ..Default::default() };
    let db = Tsdb::open_with_extra(dir.path(), TsdbOptions::default(), extra).unwrap();
    let (s1, s2, s3, s4) = ([("a", "b")], [("c", "d")], [("e", "f")], [("g", "h")]);
    let mut app = db.appender();
    let r1 = app.append_float(None, &s1, 0, 0.0).unwrap(); app.update_metadata(Some(r1), &s1, "", MetricType::Gauge, "u1", "h1").unwrap();
    let r2 = app.append_float(None, &s2, 0, 0.0).unwrap(); app.update_metadata(Some(r2), &s2, "", MetricType::Gauge, "u2", "h2").unwrap();
    let r3 = app.append_float(None, &s3, 0, 0.0).unwrap(); app.update_metadata(Some(r3), &s3, "", MetricType::Gauge, "u3", "h3").unwrap();
    let r4 = app.append_float(None, &s4, 0, 0.0).unwrap();
    app.commit().unwrap();
    for (r, u) in [(r1, "u1"), (r2, "u2"), (r3, "u3")] { assert_eq!(db.head().series_metadata_for_test(r).unwrap().unit, u); }
    assert!(db.head().series_metadata_for_test(r4).is_none());

    let mut app = db.appender();
    app.update_metadata(Some(r1), &s1, "", MetricType::Gauge, "u1", "h1").unwrap();
    app.update_metadata(Some(r4), &s4, "", MetricType::Counter, "u4", "h4").unwrap();
    app.update_metadata(Some(r2), &s2, "", MetricType::Counter, "u5", "h5").unwrap();
    app.commit().unwrap();
    assert_eq!(db.head().series_metadata_for_test(r2).unwrap().unit, "u5");
    assert_eq!(db.head().series_metadata_for_test(r4).unwrap().unit, "u4");
    db.close().unwrap();

    let db2 = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    for (p, u) in [(&s1, "u1"), (&s2, "u5"), (&s3, "u3"), (&s4, "u4")] {
        let r = db2.head().series_ref_by_pairs_for_test(p).unwrap();
        assert_eq!(db2.head().series_metadata_for_test(r).unwrap().unit, u);
    }
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestMultipleEncodingsCommitOrder_AppendV2
#[test]
fn test_multiple_encodings_commit_order_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_cap_max = 30;
    opts.head.ooo_time_window_ms = 24 * HOUR_MS;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.disable_compactions();

    let mut exp = Vec::new();
    let add_batch = |d: &Tsdb, ranges: &[(i64, i64, SampleKind)], e: &mut Vec<DecodedSample>| {
        let mut app = d.appender();
        for &(from, to, kind) in ranges {
            for i in from..to {
                e.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], i, i).unwrap().1);
            }
        }
        app.commit().unwrap();
    };
    add_batch(&db, &[(100, 105, SampleKind::Float), (110, 120, SampleKind::IntHist), (120, 130, SampleKind::FloatHist), (140, 150, SampleKind::FloatHist), (130, 135, SampleKind::Float)], &mut exp);
    exp.sort_by_key(|s| s.t);
    assert_eq!(db.head().ooo_samples_appended_count_for_test(), 5);

    let check = |min_t, max_t, e: &[DecodedSample]| {
        let mut m = BTreeMap::new();
        m.insert(r#"{foo="bar1"}"#.to_string(), e.to_vec());
        compare_series(&m, &query_db(&db, min_t, max_t, &[match_eq("foo", "bar1")]), true);
        let chks = query_chunks_expanded(&db, min_t, max_t, &[match_eq("foo", "bar1")]);
        let flat: Vec<_> = chks[r#"{foo="bar1"}"#].iter().flatten().cloned().collect();
        let mut fm = BTreeMap::new();
        fm.insert(r#"{foo="bar1"}"#.to_string(), flat);
        compare_series(&m, &fm, true);
    };
    check(100, 150, &exp);
    add_batch(&db, &[(150, 160, SampleKind::IntHist)], &mut exp);
    assert_eq!(db.head().ooo_samples_appended_count_for_test(), 5);
    check(100, 160, &exp);
    add_batch(&db, &[(50, 55, SampleKind::Float), (60, 70, SampleKind::IntHist), (70, 75, SampleKind::Float), (80, 90, SampleKind::FloatHist)], &mut exp);
    exp.sort_by_key(|s| s.t);
    assert_eq!(db.head().ooo_samples_appended_count_for_test(), 35);
    check(50, 160, &exp);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOCompaction_AppendV2
#[test]
fn test_ooo_compaction_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        for add_extra in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.ooo_cap_max = 30;
            opts.head.ooo_time_window_ms = 300 * 60_000;
            let db = Arc::new(Tsdb::open(dir.path(), opts).unwrap());
            db.disable_compactions();
            let add = |d: &Tsdb, from_m: i64, to_m: i64| {
                let mut app = d.appender();
                for m in from_m..=to_m {
                    append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap();
                    append_kind_v2(&mut app, None, kind, &[("foo", "bar2")], min_ms(m), 2 * m).unwrap();
                }
                app.commit().unwrap();
            };
            add(&db, 250, 300);
            add(&db, 90, 300);
            add(&db, 90, 300);
            if add_extra {
                let db_c = Arc::clone(&db);
                db.set_compact_ooo_head_hook_for_test(Some(Arc::new(move || {
                    let mut app = db_c.appender();
                    for m in (90..=120).chain(300..=330) {
                        append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap();
                        append_kind_v2(&mut app, None, kind, &[("foo", "bar2")], min_ms(m), 2 * m).unwrap();
                    }
                    app.commit().unwrap();
                })));
            }
            db.compact_ooo_head().unwrap();
            assert_eq!(db.blocks().len(), 3);
            let highest = if add_extra { 330 } else { 300 };
            let exp = BTreeMap::from([
                (r#"{foo="bar1"}"#.into(), (90..=highest).map(|m| make_sample_kind(kind, min_ms(m), m)).collect()),
                (r#"{foo="bar2"}"#.into(), (90..=highest).map(|m| make_sample_kind(kind, min_ms(m), 2 * m)).collect()),
            ]);
            compare_series(&exp, &query_db(&db, i64::MIN, i64::MAX, &[match_re("foo", "bar.*")]), true);
            db.compact_head_range(min_ms(250), min_ms(350)).unwrap();
            assert_eq!(db.blocks().len(), 4);
            db.compact().unwrap();
            assert_eq!(db.blocks().len(), 3);
            compare_series(&exp, &query_db(&db, i64::MIN, i64::MAX, &[match_re("foo", "bar.*")]), true);
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOCompactionWithNormalCompaction_AppendV2
#[test]
fn test_ooo_compaction_with_normal_compaction_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 30;
        opts.head.ooo_time_window_ms = 300 * 60_000;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let add = |from_m: i64, to_m: i64| {
            let mut app = db.appender();
            for m in from_m..=to_m {
                append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap();
                append_kind_v2(&mut app, None, kind, &[("foo", "bar2")], min_ms(m), 2 * m).unwrap();
            }
            app.commit().unwrap();
        };
        add(250, 350);
        add(90, 110);
        db.compact().unwrap();
        assert!(db.blocks().is_empty());
        add(400, 450);
        db.compact().unwrap();
        let blks = db.blocks();
        assert_eq!(blks.len(), 2);
        assert_eq!((blks[0].min_time(), blks[0].max_time()), (0, min_ms(120)));
        assert_eq!((blks[1].min_time(), blks[1].max_time()), (min_ms(250), min_ms(360)));
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOCompactionWithDisabledWriteLog_AppendV2
#[test]
fn test_ooo_compaction_with_disabled_write_log_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 30;
        opts.head.ooo_time_window_ms = 300 * 60_000;
        let extra = TsdbExtraOptions { disable_wal: true, ..Default::default() };
        let db = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
        db.disable_compactions();
        let add = |from_m: i64, to_m: i64| {
            let mut app = db.appender();
            for m in from_m..=to_m { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
            app.commit().unwrap();
        };
        add(250, 350);
        add(90, 110);
        db.compact().unwrap();
        assert!(db.blocks().is_empty());
        add(400, 450);
        db.compact().unwrap();
        assert_eq!(db.blocks().len(), 2);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOQueryAfterRestartWithSnapshotAndRemovedWBL_AppendV2
#[test]
fn test_ooo_query_after_restart_with_snapshot_and_removed_wbl_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 10;
        opts.head.ooo_time_window_ms = 300 * 60_000;
        let extra = TsdbExtraOptions { enable_memory_snapshot_on_shutdown: true, ..Default::default() };
        let db = Tsdb::open_with_extra(dir.path(), opts.clone(), extra).unwrap();
        db.disable_compactions();
        let mut app = db.appender();
        for m in 250..=350 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        for m in 90..=110 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        app.commit().unwrap();
        let r1 = db.head().series_ref_by_pairs_for_test(&[("foo", "bar1")]).unwrap();
        assert_eq!(db.head().series_ooo_chunk_count_for_test(r1), Some((2, true)));
        db.close().unwrap();

        fs::remove_dir_all(dir.path().join("wbl")).unwrap();
        let db2 = Tsdb::open(dir.path(), opts).unwrap();
        db2.disable_compactions();
        let r2 = db2.head().series_ref_by_pairs_for_test(&[("foo", "bar1")]).unwrap();
        assert_eq!(db2.head().series_ooo_chunk_count_for_test(r2), Some((2, false)));
        let mut exp = BTreeMap::new();
        exp.insert(r#"{foo="bar1"}"#.into(), (90..=109).map(|m| make_sample_kind(kind, min_ms(m), m)).collect());
        compare_series(&exp, &query_db(&db2, min_ms(90), min_ms(109), &[match_eq("foo", "bar1")]), true);
        db2.compact_ooo_head().unwrap();
        assert_eq!(db2.blocks().len(), 1);
        compare_series(&exp, &query_db(&db2, min_ms(90), min_ms(109), &[match_eq("foo", "bar1")]), true);
        db2.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestQuerierOOOQuery_AppendV2
#[test]
fn test_querier_ooo_query_append_v2() {
    for kind in [SampleKind::Float, SampleKind::IntHist, SampleKind::FloatHist] {
        run_ooo_querier_cases(kind, false, false);
    }
    run_ooo_querier_cases(SampleKind::IntHist, false, true);
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestChunkQuerierOOOQuery_AppendV2
#[test]
fn test_chunk_querier_ooo_query_append_v2() {
    for kind in [SampleKind::Float, SampleKind::IntHist, SampleKind::FloatHist] {
        run_ooo_querier_cases(kind, true, false);
    }
    run_ooo_querier_cases(SampleKind::IntHist, true, true);
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOONativeHistogramsWithCounterResets_AppendV2
#[test]
fn test_ooo_native_histograms_with_counter_resets_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in [SampleKind::IntHist, SampleKind::FloatHist] {
        let batches_list: &[&[(i64, i64, Option<i64>)]] = &[
            &[(40, 50, Some(45))],
            &[(40, 45, None), (50, 55, None), (45, 50, Some(45))],
            &[(40, 45, Some(44)), (50, 55, None), (45, 50, Some(49))],
        ];
        for batches in batches_list {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.ooo_cap_max = 30;
            opts.head.ooo_time_window_ms = 24 * HOUR_MS;
            let db = Tsdb::open(dir.path(), opts).unwrap();
            let mut app = db.appender();
            let mut exp = Vec::new();
            for &(from, until, reset_at) in *batches {
                let mut j = from;
                for i in from..until {
                    if reset_at == Some(i) { j = 0; }
                    let hint = if i == from || reset_at == Some(i) { CounterResetHint::UnknownCounterReset } else { CounterResetHint::NotCounterReset };
                    let (_, s) = append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(i), j).unwrap();
                    exp.push(set_crh(s, hint));
                    j += 1;
                }
            }
            app.commit().unwrap();
            exp.sort_by_key(|s| s.t);
            let mut exp_map = BTreeMap::new();
            exp_map.insert(r#"{foo="bar1"}"#.into(), exp);
            compare_series(&exp_map, &query_db(&db, min_ms(40), min_ms(55), &[match_eq("foo", "bar1")]), true);
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOInterleavedImplicitCounterResets_AppendV2
#[test]
fn test_ooo_interleaved_implicit_counter_resets_append_v2() {
    for kind in [SampleKind::IntHist, SampleKind::FloatHist, SampleKind::CustomBucketsIntHist, SampleKind::CustomBucketsFloatHist] {
        type CrCase<'a> = (&'a [(i64, i64)], usize, &'a [(i64, CounterResetHint)], &'a [usize]);
        let cases: &[CrCase<'_>] = &[
            (&[(1, 40), (4, 30), (2, 40), (3, 10)], 30, &[(40, CounterResetHint::UnknownCounterReset), (40, CounterResetHint::UnknownCounterReset), (10, CounterResetHint::UnknownCounterReset), (30, CounterResetHint::UnknownCounterReset)], &[1, 1, 1, 1]),
            (&[(8, 30), (1, 10), (2, 20), (3, 30), (5, 20), (6, 10), (7, 20), (4, 10)], 6, &[(10, CounterResetHint::UnknownCounterReset), (20, CounterResetHint::NotCounterReset), (30, CounterResetHint::NotCounterReset), (10, CounterResetHint::UnknownCounterReset), (20, CounterResetHint::UnknownCounterReset), (10, CounterResetHint::UnknownCounterReset), (20, CounterResetHint::NotCounterReset), (30, CounterResetHint::UnknownCounterReset)], &[3, 1, 1, 2, 1]),
            (&[(8, 100), (1, 50), (5, 40), (6, 50), (2, 10), (3, 20), (4, 30), (7, 60)], 3, &[(50, CounterResetHint::UnknownCounterReset), (10, CounterResetHint::UnknownCounterReset), (20, CounterResetHint::NotCounterReset), (30, CounterResetHint::NotCounterReset), (40, CounterResetHint::UnknownCounterReset), (50, CounterResetHint::NotCounterReset), (60, CounterResetHint::UnknownCounterReset), (100, CounterResetHint::UnknownCounterReset)], &[1, 3, 2, 1, 1]),
        ];
        for &(samples, ooo_cap, exp_samples, exp_chunk_sizes) in cases {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.ooo_cap_max = ooo_cap;
            opts.head.ooo_time_window_ms = 24 * HOUR_MS;
            let db = Tsdb::open(dir.path(), opts).unwrap();
            db.disable_compactions();
            let mut app = db.appender();
            for &(ts, v) in samples {
                match kind {
                    SampleKind::IntHist => {
                        let h = Histogram { count: v as u64, sum: v as f64, positive_spans: vec![HistogramSpan { offset: 0, length: 1 }], positive_buckets: vec![v], ..Histogram::default() };
                        app.append_histogram(None, &[("foo", "bar1")], 0, ts, h).unwrap();
                    }
                    SampleKind::FloatHist => {
                        let fh = FloatHistogram { count: v as f64, sum: v as f64, positive_spans: vec![HistogramSpan { offset: 0, length: 1 }], positive_buckets: vec![v as f64], ..FloatHistogram::default() };
                        app.append_float_histogram(None, &[("foo", "bar1")], 0, ts, fh).unwrap();
                    }
                    SampleKind::CustomBucketsIntHist => {
                        let h = Histogram { schema: CUSTOM_BUCKETS_SCHEMA, count: v as u64, sum: v as f64, positive_spans: vec![HistogramSpan { offset: 0, length: 1 }], positive_buckets: vec![v], custom_values: vec![1.0, 2.0, 3.0], ..Histogram::default() };
                        app.append_histogram(None, &[("foo", "bar1")], 0, ts, h).unwrap();
                    }
                    _ => {
                        let fh = FloatHistogram { schema: CUSTOM_BUCKETS_SCHEMA, count: v as f64, sum: v as f64, positive_spans: vec![HistogramSpan { offset: 0, length: 1 }], positive_buckets: vec![v as f64], custom_values: vec![1.0, 2.0, 3.0], ..FloatHistogram::default() };
                        app.append_float_histogram(None, &[("foo", "bar1")], 0, ts, fh).unwrap();
                    }
                }
            }
            app.commit().unwrap();
                    let got = &query_db(&db, 0, 10, &[match_eq("foo", "bar1")])[r#"{foo="bar1"}"#];
            assert_eq!(got.len(), exp_samples.len());
            for (s, &(ev, eh)) in got.iter().zip(exp_samples) {
                match &s.value {
                    SampleValue::Histogram(h) => { assert_eq!(h.counter_reset_hint, eh); assert_eq!(h.count, ev as u64); }
                    SampleValue::FloatHistogram(fh) => { assert_eq!(fh.counter_reset_hint, eh); assert_eq!(fh.count, ev as f64); }
                    _ => panic!("expected hist"),
                }
            }
            let chks = &query_chunks_expanded(&db, 0, 10, &[match_eq("foo", "bar1")])[r#"{foo="bar1"}"#];
            let got_sizes: Vec<usize> = chks.iter().map(|c| c.len()).collect();
            assert_eq!(got_sizes, exp_chunk_sizes);
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOAppendAndQuery_AppendV2
#[test]
fn test_ooo_append_and_query_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let mut opts = opts_with_reg(&reg, DEFAULT_BLOCK_DURATION_MS);
        opts.head.ooo_cap_max = 30;
        opts.head.ooo_time_window_ms = 4 * HOUR_MS;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let mut exp: BTreeMap<String, Vec<DecodedSample>> = BTreeMap::new();
        let mut rng = 77u64;
        let mut add = |key: &str, pairs: &[(&str, &str)], from_m: i64, to_m: i64, fail: bool| {
            let mut app = db.appender();
            for m in from_m..=to_m {
                let res = append_kind_v2(&mut app, None, kind, pairs, min_ms(m), (lcg_next(&mut rng) % 1000) as i64);
                if fail { assert!(res.is_err()); } else { exp.entry(key.into()).or_default().push(res.unwrap().1); }
            }
            if fail { app.rollback().unwrap(); } else { app.commit().unwrap(); }
        };
        add(r#"{foo="bar1"}"#, &[("foo", "bar1")], 300, 300, false);
        add(r#"{foo="bar2"}"#, &[("foo", "bar2")], 290, 290, false);
        assert_eq!(metric_val(&reg, "prometheus_tsdb_head_chunks_created_total", &[]), 2.0);
        add(r#"{foo="bar1"}"#, &[("foo", "bar1")], 250, 260, false);
        add(r#"{foo="bar2"}"#, &[("foo", "bar2")], 255, 265, false);
        assert_eq!((db.head().min_ooo_time(), db.head().max_ooo_time()), (min_ms(250), min_ms(265)));
        add(r#"{foo="bar1"}"#, &[("foo", "bar1")], 59, 59, true);
        add(r#"{foo="bar2"}"#, &[("foo", "bar2")], 49, 49, true);
        add(r#"{foo="bar1"}"#, &[("foo", "bar1")], 60, 65, false);
        assert_eq!((db.head().min_ooo_time(), db.head().max_ooo_time()), (min_ms(60), min_ms(265)));
        add(r#"{foo="bar2"}"#, &[("foo", "bar2")], 59, 59, true);
        add(r#"{foo="bar2"}"#, &[("foo", "bar2")], 60, 65, false);
        add(r#"{foo="bar1"}"#, &[("foo", "bar1")], 180, 249, false);
        for v in exp.values_mut() { v.sort_by_key(|s| s.t); }
        compare_series(&exp, &query_db(&db, i64::MIN, i64::MAX, &[match_re("foo", "bar.")]), true);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOODisabled_AppendV2
#[test]
fn test_ooo_disabled_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 0;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        assert!(!db.head().has_wbl());
        let mut app = db.appender();
        let (_, s0) = append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(300), 300).unwrap();
        app.commit().unwrap();
        let mut app = db.appender();
        assert!(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(250), 250).is_err());
        app.rollback().unwrap();
        let exp = BTreeMap::from([(r#"{foo="bar1"}"#.into(), vec![s0])]);
        compare_series(&exp, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestWBLAndMmapReplay_AppendV2
#[test]
fn test_wbl_and_mmap_replay_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 30;
        opts.head.ooo_time_window_ms = 4 * HOUR_MS;
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        db.disable_compactions();
        let mut app = db.appender();
        let mut exp = Vec::new();
        exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(300), 300).unwrap().1);
        for m in (250..=260).chain(195..=249) {
            exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap().1);
        }
        app.commit().unwrap();
        exp.sort_by_key(|s| s.t);
        db.close().unwrap();

        let orig_wbl = tempfile::tempdir().unwrap();
        let orig_mmap = tempfile::tempdir().unwrap();
        copy_dir_recursive(&dir.path().join("wbl"), orig_wbl.path());
        copy_dir_recursive(&dir.path().join("chunks_head"), orig_mmap.path());
        let reset = |wbl: bool, mmap: bool| {
            let _ = fs::remove_dir_all(dir.path().join("wbl"));
            let _ = fs::remove_dir_all(dir.path().join("chunks_head"));
            if wbl { copy_dir_recursive(orig_wbl.path(), &dir.path().join("wbl")); }
            if mmap { copy_dir_recursive(orig_mmap.path(), &dir.path().join("chunks_head")); }
        };

        for (wbl, mmap, cap) in [(true, true, 30), (true, false, 30), (false, true, 30), (true, true, 60), (true, true, 10)] {
            reset(wbl, mmap);
            let mut o2 = opts.clone();
            o2.head.ooo_cap_max = cap;
            let db2 = Tsdb::open(dir.path(), o2).unwrap();
            assert_eq!((db2.head().min_ooo_time(), db2.head().max_ooo_time()), (min_ms(195), min_ms(260)));
            let got = query_db(&db2, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]);
            if wbl {
                let mut m = BTreeMap::new();
                m.insert(r#"{foo="bar1"}"#.into(), exp.clone());
                compare_series(&m, &got, true);
            } else {
                assert!(!got[r#"{foo="bar1"}"#].is_empty());
            }
            db2.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOHistogramCompactionWithCounterResets_AppendV2
#[test]
fn test_ooo_histogram_compaction_with_counter_resets_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in [SampleKind::IntHist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 30;
        opts.head.ooo_time_window_ms = 500 * 60_000;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let mut exp = Vec::new();
        let mut app = db.appender();
        exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(520), 1_000_000).unwrap().1);
        for i in (100..200).step_by(10) { exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(i), 100_000 + i).unwrap().1); }
        exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(250), 100_250).unwrap().1);
        for i in (260..450).step_by(10) { exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(i), 100_000 + i).unwrap().1); }
        for i in (105..165).step_by(10) { exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(i), 100_000 + i).unwrap().1); }
        exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(165), 100_000).unwrap().1);
        for i in (175..405).step_by(10) { exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(i), 100_000 + i).unwrap().1); }
        for i in 480..510 { exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(i), if i == 490 { 100_000 } else { 100_000 + i }).unwrap().1); }
        app.commit().unwrap();
        exp.sort_by_key(|s| s.t);
        let exp_map = BTreeMap::from([(r#"{foo="bar1"}"#.into(), exp)]);
        compare_series(&exp_map, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
        db.compact_ooo_head().unwrap();
        compare_series(&exp_map, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestInterleavedInOrderAndOOOHistogramCompactionWithCounterResets_AppendV2
#[test]
fn test_interleaved_in_order_and_ooo_histogram_compaction_with_counter_resets_append_v2() {
    for kind in [SampleKind::IntHist, SampleKind::FloatHist] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 30;
        opts.head.ooo_time_window_ms = 500 * 60_000;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let mut app = db.appender();
        let mut exp = Vec::new();
        for (ts, v, h) in [
            (0, 0, CounterResetHint::UnknownCounterReset),
            (1, 10, CounterResetHint::NotCounterReset),
            (3, 3, CounterResetHint::UnknownCounterReset),
            (2, 0, CounterResetHint::UnknownCounterReset),
        ] {
            let s = append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], ts, v).unwrap().1;
            exp.push(set_crh(s, h));
        }
        app.commit().unwrap();
        exp.sort_by_key(|s| s.t);
        let mut exp_map = BTreeMap::new();
        exp_map.insert(r#"{foo="bar1"}"#.into(), exp);
        compare_series(&exp_map, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), false);
        db.compact_ooo_head().unwrap();
        assert_eq!(db.blocks().len(), 1);
        db.compact_head_range_inclusive(0, 3).unwrap();
        assert_eq!(db.blocks().len(), 2);
        db.compact().unwrap();
        assert_eq!(db.blocks().len(), 1);
        compare_series(&exp_map, &query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), false);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOCompactionFailure_AppendV2
#[test]
fn test_ooo_compaction_failure_append_v2() {
    struct FailWrite;
    impl TsdbCompactorOverride for FailWrite {
        fn write(&self, _d: &Path, _b: &Block, _mn: i64, _mx: i64, _base: Option<&BlockMeta>) -> Result<Vec<Ulid>, TsdbError> {
            Err(TsdbError::Other("injected failure".into()))
        }
    }
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 30;
        opts.head.ooo_time_window_ms = 300 * 60_000;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let mut app = db.appender();
        for m in (250..=350).chain(90..=310) { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
        app.commit().unwrap();

        db.set_compactor_override_for_test(Some(Arc::new(FailWrite)));
        for _ in 0..5 { assert!(db.compact_ooo_head().is_err()); }
        assert!(db.blocks().is_empty());
        assert_eq!(fs::read_dir(dir.path().join("wbl")).unwrap().count(), 6);

        db.set_compactor_override_for_test(None);
        db.compact_ooo_head().unwrap();
        assert_eq!(db.blocks().len(), 3);
        db.compact_ooo_head().unwrap();
        assert_eq!(db.blocks().len(), 3);
        assert_eq!(fs::read_dir(dir.path().join("wbl")).unwrap().count(), 1);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestWBLCorruption_AppendV2
#[test]
fn test_wbl_corruption_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_cap_max = 30;
    opts.head.ooo_time_window_ms = 300 * 60_000;
    let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
    let mut all = Vec::new();
    let mut exp_after = Vec::new();
    let add = |d: &Tsdb, from_m: i64, to_m: i64, keep: bool, a: &mut Vec<DecodedSample>, e: &mut Vec<DecodedSample>| {
        let mut app = d.appender();
        for m in from_m..=to_m {
            let s = append_kind_v2(&mut app, None, SampleKind::Float, &[("foo", "bar1")], min_ms(m), m).unwrap().1;
            a.push(s.clone());
            if keep { e.push(s); }
        }
        app.commit().unwrap();
    };
    add(&db, 340, 350, true, &mut all, &mut exp_after);
    for (a, b) in [(90, 99), (100, 119), (120, 130)] { add(&db, a, b, true, &mut all, &mut exp_after); }
    db.head().wbl_next_segment_for_test().unwrap();
    for (a, b) in [(200, 230), (240, 255)] { add(&db, a, b, true, &mut all, &mut exp_after); }
    db.head().wbl_sync_for_test().unwrap();
    let corrupt_path = dir.path().join("wbl").join("00000001");
    let corrupt_idx = fs::metadata(&corrupt_path).unwrap().len();
    db.head().wbl_log_raw_for_test(&[1, 99, 9, 99, 9, 99, 9, 99]).unwrap();
    for (a, b) in [(260, 280), (290, 300)] { add(&db, a, b, false, &mut all, &mut exp_after); }
    db.head().wbl_next_segment_for_test().unwrap();
    add(&db, 310, 320, false, &mut all, &mut exp_after);
    db.close().unwrap();

    fs::remove_dir_all(dir.path().join("chunks_head")).unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts2 = opts.clone();
    opts2.head.registry = Some(reg.clone());
    let db2 = Tsdb::open(dir.path(), opts2).unwrap();
    assert_eq!(metric_val(&reg, "prometheus_tsdb_wal_corruptions_total", &[]), 1.0);
    exp_after.sort_by_key(|s| s.t);
    let m = BTreeMap::from([(r#"{foo="bar1"}"#.into(), exp_after.clone())]);
    compare_series(&m, &query_db(&db2, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
    let bytes = fs::read(&corrupt_path).unwrap();
    assert!(bytes[corrupt_idx as usize..].iter().all(|&b| b == 0));
    db2.close().unwrap();

    let reg3 = Arc::new(prometheus::Registry::new());
    let mut opts3 = opts;
    opts3.head.registry = Some(reg3.clone());
    let db3 = Tsdb::open(dir.path(), opts3).unwrap();
    assert_eq!(metric_val(&reg3, "prometheus_tsdb_wal_corruptions_total", &[]), 0.0);
    compare_series(&m, &query_db(&db3, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
    db3.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOOMmapCorruption_AppendV2
#[test]
fn test_ooo_mmap_corruption_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 10;
        opts.head.ooo_time_window_ms = 300 * 60_000;
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        let mut all = Vec::new();
        let mut exp_mmap = Vec::new();
        let mut add = |from_m: i64, to_m: i64, in_first: bool| {
            let mut app = db.appender();
            for m in from_m..=to_m {
                let s = append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap().1;
                all.push(s.clone());
                if in_first { exp_mmap.push(s); }
            }
            app.commit().unwrap();
        };
        add(340, 350, true);
        add(90, 109, true);
        add(120, 120, false);
        db.head().cut_chunk_disk_mapper_file_for_test().unwrap();
        add(200, 230, false);
        add(240, 255, false);
        db.head().cut_chunk_disk_mapper_file_for_test().unwrap();
        add(260, 290, false);
        db.close().unwrap();

        let mmap_dir = dir.path().join("chunks_head");
        let mut mfiles: Vec<_> = fs::read_dir(&mmap_dir).unwrap().filter_map(|e| e.ok()).collect();
        mfiles.sort_by_key(|e| e.file_name());
        assert_eq!(mfiles.len(), 3);
        let mut f = fs::OpenOptions::new().read(true).write(true).open(mfiles[1].path()).unwrap();
        f.seek(SeekFrom::Start(20)).unwrap();
        f.write_all(&[99, 9, 99, 9, 99]).unwrap();
        drop(f);

        let wbl_tmp = tempfile::tempdir().unwrap();
        copy_dir_recursive(&dir.path().join("wbl"), wbl_tmp.path());
        fs::remove_dir_all(dir.path().join("wbl")).unwrap();

        let reg = Arc::new(prometheus::Registry::new());
        let mut o2 = opts.clone();
        o2.head.registry = Some(reg.clone());
        let db2 = Tsdb::open(dir.path(), o2).unwrap();
        assert_eq!(metric_val(&reg, "prometheus_tsdb_mmap_chunk_corruptions_total", &[]), 1.0);
        exp_mmap.sort_by_key(|s| s.t);
        let m_mmap = BTreeMap::from([(r#"{foo="bar1"}"#.into(), exp_mmap.clone())]);
        compare_series(&m_mmap, &query_db(&db2, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
        assert_eq!(fs::read_dir(&mmap_dir).unwrap().count(), 1);
        db2.close().unwrap();

        copy_dir_recursive(wbl_tmp.path(), &dir.path().join("wbl"));
        let db3 = Tsdb::open(dir.path(), opts).unwrap();
        all.sort_by_key(|s| s.t);
        let m_all = BTreeMap::from([(r#"{foo="bar1"}"#.into(), all)]);
        compare_series(&m_all, &query_db(&db3, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
        db3.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOutOfOrderRuntimeConfig_AppendV2
#[test]
fn test_out_of_order_runtime_config_append_v2() {
    for kind in ALL_SAMPLE_KINDS { run_ooo_runtime_config_test(kind); }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestNoGapAfterRestartWithOOO_AppendV2
#[test]
fn test_no_gap_after_restart_with_ooo_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        for (ooo_m, exp_blks) in [(489i64, [(300i64, 360i64), (480, 600)]), (479, [(300, 360), (360, 480)])] {
            let dir = tempfile::tempdir().unwrap();
            let mut opts = TsdbOptions::default();
            opts.head.ooo_time_window_ms = min_ms(30);
            let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
            db.disable_compactions();
            let mut app = db.appender();
            for m in 300..=490 { append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap(); }
            append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(ooo_m), ooo_m).unwrap();
            app.commit().unwrap();
            db.compact().unwrap();
            let blks = db.blocks();
            assert_eq!(blks.len(), 2);
            for (b, &(mn, mx)) in blks.iter().zip(&exp_blks) {
                assert_eq!((b.min_time(), b.max_time()), (min_ms(mn), min_ms(mx)));
            }
            assert_eq!((db.head().min_time(), db.head().max_time()), (min_ms(360), min_ms(490)));
            db.close().unwrap();

            let db2 = Tsdb::open(dir.path(), opts).unwrap();
            db2.disable_compactions();
            assert_eq!(db2.blocks().len(), 2);
            assert_eq!((db2.head().min_time(), db2.head().max_time()), (min_ms(360), min_ms(490)));
            db2.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestWblReplayAfterOOODisableAndRestart_AppendV2
#[test]
fn test_wbl_replay_after_ooo_disable_and_restart_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = min_ms(60);
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        let mut app = db.appender();
        let mut exp = Vec::new();
        for m in (290..=300).chain(250..=260) {
            exp.push(append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap().1);
        }
        app.commit().unwrap();
        db.close().unwrap();

        opts.head.ooo_time_window_ms = 0;
        let db2 = Tsdb::open(dir.path(), opts).unwrap();
        exp.sort_by_key(|s| s.t);
        let m = BTreeMap::from([(r#"{foo="bar1"}"#.into(), exp)]);
        compare_series(&m, &query_db(&db2, i64::MIN, i64::MAX, &[match_eq("foo", "bar1")]), true);
        db2.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestPanicOnApplyConfig_AppendV2
#[test]
fn test_panic_on_apply_config_append_v2() {
    let min_ms = |m: i64| m * 60_000;
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = min_ms(60);
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        let mut app = db.appender();
        for m in (290..=300).chain(250..=260) {
            append_kind_v2(&mut app, None, kind, &[("foo", "bar1")], min_ms(m), m).unwrap();
        }
        app.commit().unwrap();
        db.close().unwrap();

        opts.head.ooo_time_window_ms = 0;
        let db2 = Tsdb::open(dir.path(), opts).unwrap();
        db2.apply_config(&TsdbRuntimeConfig { ooo_time_window_ms: Some(min_ms(60)), ..Default::default() }).unwrap();
        db2.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestHistogramAppendAndQuery_AppendV2
#[test]
fn test_histogram_append_and_query_append_v2() {
    for float_hist in [false, true] { run_histogram_append_and_query_test(float_hist); }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestOOONativeHistogramsSettings_AppendV2
#[test]
fn test_ooo_native_histograms_settings_append_v2() {
    let h = Histogram { count: 9, zero_count: 4, zero_threshold: 0.001, sum: 35.5, schema: 1, positive_spans: vec![HistogramSpan { offset: 0, length: 2 }, HistogramSpan { offset: 2, length: 2 }], positive_buckets: vec![1, 1, -1, 0], ..Histogram::default() };
    for ooo_win in [0i64, 100] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = ooo_win;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        let mut app = db.appender();
        if ooo_win == 0 {
            app.append_histogram(None, &[("foo", "bar")], 0, 100, h.clone()).unwrap();
            app.append_histogram(None, &[("foo", "bar")], 0, 50, h.clone()).unwrap();
            app.commit().unwrap();
            assert_eq!(query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar")])[r#"{foo="bar"}"#].len(), 1);
        } else {
            app.append_histogram(None, &[("foo", "bar")], 0, 200, h.clone()).unwrap();
            app.append_histogram(None, &[("foo", "bar")], 0, 100, h.clone()).unwrap();
            app.append_histogram(None, &[("foo", "bar")], 0, 150, h.clone()).unwrap();
            app.commit().unwrap();
            let ts: Vec<i64> = query_db(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar")])[r#"{foo="bar"}"#].iter().map(|s| s.t).collect();
            assert_eq!(ts, vec![100, 150, 200]);
        }
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestChunkQuerierReadWriteRace_AppendV2
#[test]
fn test_chunk_querier_read_write_race_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Tsdb::open(dir.path(), TsdbOptions::default()).unwrap());
    let done = Arc::new(AtomicBool::new(false));
    let done_w = Arc::clone(&done);
    let db_w = Arc::clone(&db);
    let h = std::thread::spawn(move || {
        let mut ts = 0i64;
        for _ in 0..100 {
            let mut app = db_w.appender();
            for _ in 0..10 {
                ts += 1;
                app.append_float(None, &[("foo", "bar")], ts, (ts * 100) as f64).unwrap();
            }
            app.commit().unwrap();
        }
        done_w.store(true, Ordering::Release);
    });
    while !done.load(Ordering::Acquire) {
        let res = db.chunk_querier(i64::MIN, i64::MAX).select(true, &[match_eq("foo", "bar")]).unwrap();
        for s in res {
            for cm in s.chunks { let _ = cm.chunk.unwrap().as_bytes().to_vec(); }
        }
    }
    h.join().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestAbortBlockCompactions_AppendV2
#[test]
fn test_abort_block_compactions_append_v2() {
    struct MockPlan { cnt: Arc<AtomicUsize>, db: std::sync::Mutex<Option<Arc<Tsdb>>> }
    impl TsdbCompactorOverride for MockPlan {
        fn plan(&self, _dir: &Path) -> Result<Vec<PathBuf>, TsdbError> {
            let c = self.cnt.fetch_add(1, Ordering::Relaxed) + 1;
            if c > 3 {
                let d = self.db.lock().unwrap().as_ref().unwrap().clone();
                let cr = d.head().options().chunk_range_ms;
                d.head().set_min_time_for_test(0);
                d.head().set_max_time_for_test(cr * 2);
                assert!(d.head().compactable());
            }
            Ok(vec![PathBuf::from("1"), PathBuf::from("2"), PathBuf::from("3")])
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Tsdb::open(dir.path(), TsdbOptions::default()).unwrap());
    assert!(!db.head().compactable());
    let cnt = Arc::new(AtomicUsize::new(0));
    db.set_compactor_override_for_test(Some(Arc::new(MockPlan { cnt: cnt.clone(), db: std::sync::Mutex::new(Some(db.clone())) })));
    let _ = db.compact_blocks();
    db.set_compactor_override_for_test(None);
    assert!(db.head().compactable());
    assert_eq!(cnt.load(Ordering::Relaxed), 4);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestCompactHeadWithSTStorage_AppendV2
#[test]
fn test_compact_head_with_st_storage_append_v2() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions { no_lockfile: true, ..TsdbOptions::default() };
    opts.head.wal_compression = CompressionType::Snappy;
    opts.head.enable_st_storage = true;
    opts.head.enable_histogram_st_encoding = true;
    opts.head.float_chunk_encoding = Encoding::Xor2;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    let mut app = db.appender();
    for i in 100..200i64 {
        app.append_float_with_st(None, &[("a", "b"), ("type", "float")], i - 50, i, i as f64).unwrap();
        let mut h = gen_test_hist(i);
        h.counter_reset_hint = CounterResetHint::NotCounterReset;
        app.append_histogram(None, &[("a", "b"), ("type", "histogram")], i - 60, i, h).unwrap();
        let mut fh = gen_test_fhist(i);
        fh.counter_reset_hint = CounterResetHint::NotCounterReset;
        app.append_float_histogram(None, &[("a", "b"), ("type", "float_histogram")], i - 70, i, fh).unwrap();
    }
    app.commit().unwrap();
    db.compact_head_range_inclusive(100, 199).unwrap().unwrap();
    assert_eq!(db.blocks().len(), 1);
    let b = &db.blocks()[0];
    for (typ, exp_enc, st_off) in [("float", Encoding::Xor2, 50i64), ("histogram", Encoding::HistogramSt, 60), ("float_histogram", Encoding::FloatHistogramSt, 70)] {
        let pids = b.postings_for_matchers(&[match_eq("type", typ)]).unwrap();
        let (_, chks) = b.index().series(pids[0]).unwrap();
        for c in chks {
            let enc = b.chunks().chunk(c.ref_id).unwrap();
            assert_eq!(enc.encoding(), exp_enc);
            for s in mimir_tsdb::compact::decode_chunk_samples(&enc).unwrap() {
                assert_eq!(s.st, s.t - st_off);
            }
        }
    }
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestNewCompactorFunc_AppendV2
#[test]
fn test_new_compactor_func_append_v2() {
    struct MockFn(Ulid, Ulid);
    impl TsdbCompactorOverride for MockFn {
        fn plan(&self, _d: &Path) -> Result<Vec<PathBuf>, TsdbError> { Ok(vec![PathBuf::from(self.0.to_string()), PathBuf::from(self.1.to_string())]) }
        fn compact(&self, _d: &Path, _dirs: &[PathBuf], _b: &[&Block]) -> Result<Vec<Ulid>, TsdbError> { Ok(vec![self.0]) }
        fn write(&self, _d: &Path, _b: &Block, _mn: i64, _mx: i64, _base: Option<&BlockMeta>) -> Result<Vec<Ulid>, TsdbError> { Ok(vec![self.1]) }
    }
    let dir = tempfile::tempdir().unwrap();
    let (u1, u2) = (Ulid::from_parts(1, 0), Ulid::from_parts(2, 0));
    let extra = TsdbExtraOptions { compactor_override: Some(Arc::new(MockFn(u1, u2))), ..Default::default() };
    let db = Tsdb::open_with_extra(dir.path(), TsdbOptions::default(), extra).unwrap();
    let ov = db.compactor_override().unwrap();
    assert_eq!(ov.plan(dir.path()).unwrap(), vec![PathBuf::from(u1.to_string()), PathBuf::from(u2.to_string())]);
    assert_eq!(ov.compact(dir.path(), &[], &[]).unwrap(), vec![u1]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_append_v2_test.go::TestDBAppenderV2_STStorage_OutOfOrder
#[test]
fn test_db_appender_v2_st_storage_out_of_order() {
    let mut h = gen_test_hist(1);
    h.counter_reset_hint = CounterResetHint::NotCounterReset;
    for case in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_time_window_ms = 300 * 60_000;
        opts.head.enable_st_storage = true;
        opts.head.enable_histogram_st_encoding = true;
        opts.head.float_chunk_encoding = Encoding::Xor2;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let pts: &[(i64, i64, f64)] = match case {
            0 => &[(20, 200, 2.0), (10, 100, 1.0), (30, 300, 3.0), (25, 250, 2.5)],
            1 => &[(30, 300, 0.0), (10, 100, 0.0), (20, 200, 0.0)],
            _ => &[(10, 200, 2.0), (10, 100, 1.0), (10, 300, 3.0)],
        };
        for &(st, t, v) in pts {
            let mut app = db.appender();
            if case == 1 { app.append_histogram(None, &[("foo", "bar")], st, t, h.clone()).unwrap(); }
            else { app.append_float_with_st(None, &[("foo", "bar")], st, t, v).unwrap(); }
            app.commit().unwrap();
        }
        let chks = query_chunks_expanded(&db, i64::MIN, i64::MAX, &[match_eq("foo", "bar")]);
        let mut got: Vec<_> = chks[r#"{foo="bar"}"#].iter().flatten().cloned().collect();
        got.sort_by_key(|s| s.t);
        let mut exp_pts = pts.to_vec();
        exp_pts.sort_by_key(|x| x.1);
        assert_eq!(got.len(), exp_pts.len());
        for (g, &(st, t, _)) in got.iter().zip(&exp_pts) {
            assert_eq!((g.st, g.t), (st, t));
        }
        db.close().unwrap();
    }
}
