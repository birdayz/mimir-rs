//! Ported upstream Prometheus TSDB db_test.go (part 2 of 2).

mod db_test_helpers;

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use db_test_helpers::{
    append_kind_v2, assert_sample_vals, checkpoint_has_series_ref, compare_series,
    create_xor_block, force_checkpoint_at, gen_test_fhist, gen_test_hist, labels_str,
    make_hist_block_series, match_eq, match_re, metric_val, open_db_range, opts_range,
    opts_with_reg, query_db, query_db_raw_nans, query_head_range, run_selected_mutation_race,
    write_single_series_block, xor_chunk_pts, CountLifecycleCb, ALL_SAMPLE_KINDS, DAY_MS,
    DEFAULT_BLOCK_DURATION_MS, HOUR_MS,
};
use mimir_tsdb::block::{read_meta_file, Block, BlockMetaCompaction};
use mimir_tsdb::chunkenc::{stale_nan, CounterResetHint, FloatHistogram, Histogram};
use mimir_tsdb::compact::{
    write_block_from_block_with_base, write_block_from_series, CompactionSeries, DecodedSample,
    SampleValue,
};
use mimir_tsdb::db::{Tsdb, TsdbExtraOptions, TsdbOptions};
use mimir_tsdb::head::Head;
use mimir_tsdb::head_chunks::{ooo_head_chunk_id, resolve_ooo_chunk_pos, OOO_CHUNK_ID_MASK};
use mimir_tsdb::labels::Labels;
use ulid::Ulid;

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestQueryHistogramFromBlocksWithCompaction
#[test]
fn test_query_histogram_from_blocks_with_compaction() {
    for float_hist in [false, true] {
        let mk = |a, b, s, ih, ifl| make_hist_block_series(a, b, s, ih, ifl, float_hist);
        for case_series in [
            vec![mk(0, 119, 1, true, false), mk(120, 239, 1, true, false)],
            vec![mk(0, 119, 1, true, false), mk(120, 239, 1, false, true), mk(240, 359, 1, true, false)],
            vec![mk(0, 60, 1, true, true), mk(61, 120, 1, true, false)],
            vec![mk(0, 120, 3, true, false), mk(1, 120, 3, true, false), mk(2, 120, 3, true, false)],
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
            let mut exp: BTreeMap<String, Vec<DecodedSample>> = BTreeMap::new();
            for bs in &case_series {
                let mint = bs.iter().flat_map(|s| s.chunks.iter().map(|c| c.min_time)).min().unwrap();
                let maxt = bs.iter().flat_map(|s| s.chunks.iter().map(|c| c.max_time)).max().unwrap() + 1;
                write_block_from_series(db.dir(), mint, maxt, bs.clone(), BlockMetaCompaction::default(), false, 0).unwrap();
                for s in bs {
                    let e = exp.entry(labels_str(&s.labels)).or_default();
                    for cm in &s.chunks {
                        e.extend(mimir_tsdb::compact::decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap());
                    }
                    e.sort_by_key(|x| x.t);
                }
            }
            db.reload_blocks().unwrap();
            db.set_enable_overlapping_compaction(true);
            compare_series(&exp, &query_db(&db, i64::MIN, i64::MAX, &[match_re("i", ".*")]), true);
            db.compact_blocks().unwrap();
            compare_series(&exp, &query_db(&db, i64::MIN, i64::MAX, &[match_re("i", ".*")]), true);
            db.close().unwrap();
        }
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestBlockQuerierAndBlockChunkQuerier
#[test]
fn test_block_querier_and_block_chunk_querier() {
    let dir = tempfile::tempdir().unwrap();
    let extra = TsdbExtraOptions {
        block_querier_filter: Some(Arc::new(|b| !b.meta().compaction.hints.is_empty())),
        block_chunk_querier_filter: Some(Arc::new(|b| b.meta().compaction.level == 4)),
        ..Default::default()
    };
    let db = Tsdb::open_with_extra(dir.path(), TsdbOptions::default(), extra).unwrap();
    for (i, (hints, level)) in [(vec!["test-hint".to_string()], 1), (vec![], 4)].into_iter().enumerate() {
        let b_str = format!("block-{i}");
        let comp = BlockMetaCompaction { hints, level, ..Default::default() };
        write_block_from_series(
            db.dir(), 0, 10,
            vec![CompactionSeries {
                labels: Labels::from_pairs([("__name__", "test_metric"), ("block", b_str.as_str())]),
                chunks: vec![xor_chunk_pts(&[(0, 1.0)])],
                tombstones: Vec::new(),
            }],
            comp, false, 0,
        ).unwrap();
    }
    db.reload_blocks().unwrap();
    assert_eq!(db.blocks().len(), 2);

    let q_res = db.querier(0, 500).select(true, &[match_eq("__name__", "test_metric")]).unwrap();
    assert_eq!(q_res.len(), 1);
    assert_eq!(q_res[0].labels.get("block"), "block-0");

    let cq_res = db.chunk_querier(0, 500).select(true, &[match_eq("__name__", "test_metric")]).unwrap();
    assert_eq!(cq_res.len(), 1);
    assert_eq!(cq_res[0].labels.get("block"), "block-1");
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestGenerateCompactionDelay
#[test]
fn test_generate_compaction_delay() {
    for pct in [1, 10, 60, 100] {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.chunk_range_ms = 60_000;
        let extra = TsdbExtraOptions {
            enable_delayed_compaction: true,
            compaction_delay_max_percent: pct,
            ..Default::default()
        };
        let db = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
        let max_d = Duration::from_millis((60_000 * i64::from(pct) / 100) as u64);
        assert!(db.effective_compaction_delay() <= max_d);
        for _ in 0..200 {
            assert!(db.generate_compaction_delay() <= max_d);
        }
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestBlockClosingBlockedDuringRemoteRead
#[test]
fn test_block_closing_blocked_during_remote_read() {
    let dir = tempfile::tempdir().unwrap();
    create_xor_block(dir.path(), 2, 1, 0, 10);
    let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
    let blk = db.blocks()[0].clone();
    let guard = Block::start_read(&blk).unwrap();
    let (tx, rx) = mpsc::channel();
    let blk_c = Arc::clone(&blk);
    let h = std::thread::spawn(move || { blk_c.close_and_wait(); tx.send(()).unwrap(); });
    assert!(rx.recv_timeout(Duration::from_millis(80)).is_err());
    drop(guard);
    rx.recv_timeout(Duration::from_secs(2)).unwrap();
    h.join().unwrap();
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestBlockReloadInterval
#[test]
fn test_block_reload_interval() {
    for (inp, exp, verify) in [
        (Duration::ZERO, Duration::from_secs(60), false),
        (Duration::from_millis(1), Duration::from_secs(1), true),
        (Duration::from_secs(1), Duration::from_secs(1), true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = opts_with_reg(&reg, DEFAULT_BLOCK_DURATION_MS);
        let extra = TsdbExtraOptions { block_reload_interval: Some(inp), ..Default::default() };
        let db = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
        assert_eq!(db.block_reload_interval(), exp);
        if verify {
            assert_eq!(metric_val(&reg, "prometheus_tsdb_reloads_total", &[]), 1.0);
            let deadline = Instant::now() + Duration::from_secs(5);
            while metric_val(&reg, "prometheus_tsdb_reloads_total", &[]) < 3.0 && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            assert!(metric_val(&reg, "prometheus_tsdb_reloads_total", &[]) >= 3.0);
        }
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestFrequentCommitsDoNotStarveMaintenance
#[test]
fn test_frequent_commits_do_not_starve_maintenance() {
    for auto_compact in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let mut opts = opts_with_reg(&reg, 10);
        opts.compaction_delay = Duration::from_secs(3600);
        let extra = TsdbExtraOptions { block_reload_interval: Some(Duration::from_secs(1)), ..Default::default() };
        let db = Tsdb::open_with_extra(dir.path(), opts, extra).unwrap();
        if auto_compact { db.enable_compactions(); } else { db.disable_compactions(); }

        let mut app = db.appender();
        app.append_float(None, &[("series", "frequent-commit")], 0, 0.0).unwrap();
        app.append_float(None, &[("series", "frequent-commit")], 19, 0.0).unwrap();
        app.commit().unwrap();

        for ts in 20..40i64 {
            let mut app = db.appender();
            app.append_float(None, &[("series", "frequent-commit")], ts, 0.0).unwrap();
            let before = metric_val(&reg, "prometheus_tsdb_compactions_triggered_total", &[]);
            app.commit().unwrap();
            std::thread::sleep(Duration::from_millis(75));
            assert!(db.head().compactable());
            let deadline = Instant::now() + Duration::from_secs(2);
            while metric_val(&reg, "prometheus_tsdb_compactions_triggered_total", &[]) <= before && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(metric_val(&reg, "prometheus_tsdb_compactions_triggered_total", &[]) > before);
        }
        assert!(db.blocks().is_empty());
        assert!(metric_val(&reg, "prometheus_tsdb_reloads_total", &[]) > 1.0);
        assert!(metric_val(&reg, "prometheus_tsdb_head_chunks_created_total", &[]) > 0.0);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestBiggerBlocksForOldOOOData
#[test]
fn test_bigger_blocks_for_old_ooo_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_time_window_ms = 10 * DAY_MS;
    opts.enable_bigger_ooo_block_for_old_samples = true;
    let curr_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let curr_day_start = DAY_MS * (curr_ts / DAY_MS);
    let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
    let in_order_ts = curr_day_start + 6 * HOUR_MS;
    let mut app = db.appender();
    let r = app.append_float(None, &[("foo", "bar")], in_order_ts, in_order_ts as f64).unwrap();
    app.commit().unwrap();

    let mut exp = Vec::new();
    let mut ts = curr_day_start - 5 * DAY_MS;
    while ts < in_order_ts {
        let mut app = db.appender();
        app.append_float(Some(r), &[("foo", "bar")], ts, ts as f64).unwrap();
        app.commit().unwrap();
        exp.push(DecodedSample { st: 0, t: ts, value: SampleValue::Float(ts as f64) });
        ts += HOUR_MS;
    }
    db.compact_ooo_head().unwrap();
    assert_eq!(db.blocks().len(), 8);

    let new_dir = tempfile::tempdir().unwrap();
    let new_db = Tsdb::open(new_dir.path(), opts).unwrap();
    for b in db.blocks() {
        fs::rename(b.dir(), new_db.dir().join(b.ulid().to_string())).unwrap();
    }
    db.reload_blocks().unwrap();
    new_db.reload_blocks().unwrap();
    assert!(db.blocks().is_empty());
    assert_eq!(new_db.blocks().len(), 8);
    let got_old = query_db(&db, in_order_ts - 6 * DAY_MS, in_order_ts + 1, &[match_eq("foo", "bar")]);
    assert_eq!(got_old[r#"{foo="bar"}"#], vec![DecodedSample { st: 0, t: in_order_ts, value: SampleValue::Float(in_order_ts as f64) }]);
    let got_new = query_db(&new_db, in_order_ts - 6 * DAY_MS, in_order_ts + 1, &[match_eq("foo", "bar")]);
    assert_eq!(got_new[r#"{foo="bar"}"#], exp);
    db.close().unwrap();
    new_db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestStaleSeriesCompaction
#[test]
fn test_stale_series_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let v = 10.0;
    let stale_v = stale_nan();
    let h = gen_test_hist(0);
    let fh = gen_test_fhist(0);
    let stale_h = Histogram { sum: stale_v, ..Histogram::default() };
    let stale_fh = FloatHistogram { sum: stale_v, ..FloatHistogram::default() };

    let mut non_first_h = h.clone();
    non_first_h.counter_reset_hint = CounterResetHint::NotCounterReset;
    let mut non_first_fh = fh.clone();
    non_first_fh.counter_reset_hint = CounterResetHint::NotCounterReset;

    let add_normal = |d: &Tsdb, ts: i64, fs_name: &str, hs_name: &str, fhs_name: &str| {
        let mut app = d.appender();
        app.append_float(None, &[("name", fs_name)], ts, v).unwrap();
        app.append_histogram(None, &[("name", hs_name)], 0, ts, h.clone()).unwrap();
        app.append_float_histogram(None, &[("name", fhs_name)], 0, ts, fh.clone()).unwrap();
        app.commit().unwrap();
    };
    let add_stale = |d: &Tsdb, ts: i64, fs_name: &str, hs_name: &str, fhs_name: &str| {
        let mut app = d.appender();
        app.append_float(None, &[("name", fs_name)], ts, stale_v).unwrap();
        app.append_histogram(None, &[("name", hs_name)], 0, ts, stale_h.clone()).unwrap();
        app.append_float_histogram(None, &[("name", fhs_name)], 0, ts, stale_fh.clone()).unwrap();
        app.commit().unwrap();
    };

    add_normal(&db, 100, "series1000", "series2000", "series3000");
    add_normal(&db, 100, "series4000", "series5000", "series6000");
    add_normal(&db, 200, "series1000", "series2000", "series3000");
    add_stale(&db, 200, "series4000", "series5000", "series6000");
    add_normal(&db, 300, "series1000", "series2000", "series3000");
    assert_eq!(db.head().num_series(), 6);
    assert_eq!(db.head().num_stale_series(), 3);

    for ts in [300, 700, 1100] {
        add_normal(&db, ts, "series7000", "series8000", "series9000");
    }
    add_stale(&db, 1200, "series7000", "series8000", "series9000");

    db.compact_stale_head().unwrap();
    assert_eq!(db.head().num_series(), 3);
    assert_eq!(db.head().num_stale_series(), 0);
    assert_eq!(db.blocks().len(), 2);
    assert!(db.blocks()[0].meta().compaction.from_stale_series());
    assert!(db.blocks()[1].meta().compaction.from_stale_series());
    assert_eq!((db.blocks()[0].min_time(), db.blocks()[0].max_time()), (0, 1000));
    assert_eq!((db.blocks()[1].min_time(), db.blocks()[1].max_time()), (1000, 2000));
    db.reload_blocks().unwrap();

    let verify_head = |d: &Tsdb| {
        assert_eq!(d.head().num_series(), 3);
        assert_eq!(d.head().num_stale_series(), 0);
        let got = query_head_range(d, 0, 300, &[match_re("name", "series.*")], false);
        assert_eq!(got.len(), 3);
        assert_sample_vals(&got[r#"{name="series1000"}"#], &[(100, v), (200, v), (300, v)]);
        assert_eq!(got[r#"{name="series2000"}"#].len(), 3);
        assert_eq!(got[r#"{name="series3000"}"#].len(), 3);
    };
    verify_head(&db);

    let got_blk = query_db_raw_nans(&db, 0, 2000, &[match_re("name", "series.*")]);
    assert_sample_vals(&got_blk[r#"{name="series4000"}"#], &[(100, v), (200, stale_v)]);
    assert_sample_vals(&got_blk[r#"{name="series7000"}"#], &[(300, v), (700, v), (1100, v), (1200, stale_v)]);
    for k in [r#"{name="series5000"}"#, r#"{name="series6000"}"#] {
        assert_eq!(got_blk[k].len(), 2);
        assert_eq!(got_blk[k][1].t, 200);
    }
    for k in [r#"{name="series8000"}"#, r#"{name="series9000"}"#] {
        assert_eq!(got_blk[k].len(), 4);
        assert_eq!(got_blk[k][3].t, 1200);
    }

    db.close().unwrap();
    let db = open_db_range(dir.path(), 1000);
    verify_head(&db);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestLifecycleCallbackInvariantAfterWALReplayWithFullTombstones
#[test]
fn test_lifecycle_callback_invariant_after_wal_replay_with_full_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = opts_range(1000);
    opts.head.series_lifecycle_callback = Some(Arc::new(CountLifecycleCb::default()));
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.disable_compactions();
    {
        let mut app = db.appender();
        for i in 0..6 {
            let s = format!("series{i}");
            app.append_float(None, &[("name", s.as_str())], 100, 1.0).unwrap();
        }
        app.commit().unwrap();
    }
    {
        let mut app = db.appender();
        for i in 0..6 {
            let s = format!("series{i}");
            let v = if i < 3 { stale_nan() } else { i as f64 };
            app.append_float(None, &[("name", s.as_str())], 200, v).unwrap();
        }
        app.commit().unwrap();
    }
    assert_eq!(db.head().num_series(), 6);
    assert_eq!(db.head().num_stale_series(), 3);
    db.compact_stale_head().unwrap();
    assert_eq!(db.head().num_series(), 3);
    assert_eq!(db.head().num_stale_series(), 0);
    db.close().unwrap();

    let cb = Arc::new(CountLifecycleCb::default());
    let mut opts2 = opts_range(1000);
    opts2.head.series_lifecycle_callback = Some(cb.clone());
    let db2 = Tsdb::open(dir.path(), opts2).unwrap();
    assert_eq!(db2.head().num_series() as i64, cb.created.load(Ordering::Relaxed) - cb.deleted.load(Ordering::Relaxed));
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestStaleSeriesResurrectionOverlapNotMerged
#[test]
fn test_stale_series_resurrection_overlap_not_merged() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    app.append_float(None, &[("name", "resurrected")], 100, 1.0).unwrap();
    app.append_float(None, &[("name", "in_order_kept")], 100, 7.0).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("name", "resurrected")], 200, stale_nan()).unwrap();
    app.append_float(None, &[("name", "in_order_kept")], 700, 42.0).unwrap();
    app.commit().unwrap();
    db.compact_stale_head().unwrap();
    assert_eq!(db.head().num_series(), 1);
    let stale_ulid = db.blocks()[0].ulid();
    assert!(db.blocks()[0].meta().compaction.from_stale_series());

    let mut app = db.appender();
    app.append_float(None, &[("name", "resurrected")], 300, 9.0).unwrap();
    app.commit().unwrap();
    assert_eq!(db.head().num_series(), 2);

    let normal_ulid = db.compact_head_range(0, 500).unwrap().unwrap();
    db.reload_blocks().unwrap();
    db.compact_blocks().unwrap();
    db.reload_blocks().unwrap();

    let ulids: HashSet<Ulid> = db.blocks().iter().map(|b| b.ulid()).collect();
    assert!(ulids.contains(&stale_ulid));
    assert!(ulids.contains(&normal_ulid));
    assert_eq!(db.in_order_blocks_max_time(), Some(500));
    let got = query_head_range(&db, 501, 1000, &[match_eq("name", "in_order_kept")], false);
    assert_sample_vals(&got[r#"{name="in_order_kept"}"#], &[(700, 42.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactBlocksLoopDoesNotStarveStaleBlocks
#[test]
fn test_compact_blocks_loop_does_not_starve_stale_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    db.set_block_ranges(vec![1000, 3000]);

    let mut orig_ns = HashSet::new();
    let mut orig_st = HashSet::new();
    for (name, t) in [("ns_0", 0), ("ns_1", 1000), ("ns_2", 2000), ("ns_fresh", 7000)] {
        let b = write_single_series_block(db.dir(), name, &[(t, 1.0), (t + 999, 1.0)], None);
        orig_ns.insert(read_meta_file(&b).unwrap().ulid);
    }
    for (name, t) in [("st_0", 3000), ("st_1", 4000), ("st_2", 5000), ("st_fresh", 6000)] {
        let b = write_single_series_block(db.dir(), name, &[(t, 1.0), (t + 999, 1.0)], Some("stale"));
        orig_st.insert(read_meta_file(&b).unwrap().ulid);
    }
    db.reload_blocks().unwrap();
    assert_eq!(db.blocks().len(), 8);
    db.compact_blocks().unwrap();
    db.reload_blocks().unwrap();

    let mut merged_ns = None;
    let mut merged_st = None;
    let blks = db.blocks();
    for b in &blks {
        let m = b.meta();
        let has_ns = m.compaction.sources.iter().any(|s| orig_ns.contains(s));
        let has_st = m.compaction.sources.iter().any(|s| orig_st.contains(s));
        assert!(!(has_ns && has_st));
        if m.compaction.sources.len() > 1 {
            if has_st { merged_st = Some(m); } else if has_ns { merged_ns = Some(m); }
        }
    }
    let mns = merged_ns.expect("merged non-stale block");
    let mst = merged_st.expect("merged stale block");
    assert_eq!(mns.compaction.sources.len(), 3);
    assert!(!mns.compaction.from_stale_series());
    assert_eq!(mst.compaction.sources.len(), 3);
    assert!(mst.compaction.from_stale_series());
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestStaleSeriesCompactionExcludesOutOfOrderSeries
#[test]
fn test_stale_series_compaction_excludes_out_of_order_series() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = opts_range(1000);
    opts.head.ooo_time_window_ms = 1000;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.disable_compactions();
    let mut app = db.appender();
    app.append_float(None, &[("name", "in_order_stale")], 100, 10.0).unwrap();
    app.append_float(None, &[("name", "ooo_stale")], 100, 10.0).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("name", "in_order_stale")], 200, stale_nan()).unwrap();
    app.append_float(None, &[("name", "ooo_stale")], 200, stale_nan()).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_float(None, &[("name", "ooo_stale")], 50, 10.0).unwrap();
    app.commit().unwrap();

    assert_eq!(db.head().num_stale_series(), 2);
    db.compact_stale_head().unwrap();
    assert_eq!(db.blocks().len(), 1);
    let blks = db.blocks();
    let m = blks[0].meta();
    assert!(m.compaction.from_stale_series());
    assert!(!m.compaction.from_out_of_order());
    let got = query_db_raw_nans(&db, i64::MIN, i64::MAX, &[match_eq("name", "in_order_stale")]);
    assert!(got.contains_key(r#"{name="in_order_stale"}"#));
    let blk_series = db.blocks()[0].postings_for_matchers(&[match_eq("name", "ooo_stale")]).unwrap();
    assert!(blk_series.is_empty());
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactStaleHead_LateStaleAppendSurvives
#[test]
fn test_compact_stale_head_late_stale_append_survives() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_db_range(dir.path(), 1000));
    let mut app = db.appender();
    app.append_float(None, &[("name", "filler")], 100, 0.1).unwrap();
    app.append_float(None, &[("name", "filler")], 700, 0.7).unwrap();
    let sel_ref = app.append_float(None, &[("name", "stale-selected")], 100, 10.0).unwrap();
    app.append_float(Some(sel_ref), &[("name", "stale-selected")], 200, stale_nan()).unwrap();
    app.commit().unwrap();

    let db_c = Arc::clone(&db);
    db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
        let mut a = db_c.appender();
        a.append_float(Some(sel_ref), &[("name", "stale-selected")], 400, stale_nan()).unwrap();
        a.commit().unwrap();
    })));
    db.compact_stale_head().unwrap();
    assert_eq!(db.blocks().len(), 1);
    assert_eq!(db.head().num_series(), 2);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactStaleHead_EvictedSeriesRecordKeptInCheckpoint
#[test]
fn test_compact_stale_head_evicted_series_record_kept_in_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let sel_ref = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
    app.append_float(Some(sel_ref), &[("name", "selected")], 200, stale_nan()).unwrap();
    app.commit().unwrap();
    db.compact_stale_head().unwrap();
    assert!(db.head().get_wal_expiry_for_test(sel_ref).is_some());
    force_checkpoint_at(db.head(), 150);
    assert!(checkpoint_has_series_ref(db.head(), sel_ref));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactStaleHead_ChunkBoundarySampleNotLost
#[test]
fn test_compact_stale_head_chunk_boundary_sample_not_lost() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let r = app.append_float(None, &[("name", "selected")], 500, 1.0).unwrap();
    app.append_float(Some(r), &[("name", "selected")], 1000, stale_nan()).unwrap();
    app.commit().unwrap();

    db.compact_stale_head().unwrap();
    let got = query_db_raw_nans(&db, 0, 2000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got[r#"{name="selected"}"#], &[(500, 1.0), (1000, stale_nan())]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_SingleTimestampHeadIsEvicted
#[test]
fn test_compact_selected_series_single_timestamp_head_is_evicted() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let r = app.append_float(None, &[("name", "single")], 1000, 1.0).unwrap();
    app.commit().unwrap();
    db.compact_selected_series(&[r]).unwrap();
    assert_eq!((db.head().num_series(), db.blocks().len()), (0, 1));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries
#[test]
fn test_compact_selected_series() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let s1 = app.append_float(None, &[("name", "selected1")], 100, 1.0).unwrap();
    let s2 = app.append_float(None, &[("name", "selected2")], 200, 2.0).unwrap();
    app.append_float(None, &[("name", "kept")], 300, 3.0).unwrap();
    app.commit().unwrap();

    let min_before = db.head().min_time();
    db.compact_selected_series(&[s1, s2]).unwrap();
    assert_eq!(db.head().num_series(), 1);
    assert_eq!(db.head().num_stale_series(), 0);
    assert_eq!(db.head().min_time(), min_before);
    assert_eq!(db.blocks().len(), 1);
    assert!(db.blocks()[0].meta().compaction.from_selected_series());
    assert_eq!(db.selected_series_compaction_metrics_for_test(), (1, 0));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_UnsortedDuplicateRefs
#[test]
fn test_compact_selected_series_unsorted_duplicate_refs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let s1 = app.append_float(None, &[("name", "selected1")], 100, 1.0).unwrap();
    let s2 = app.append_float(None, &[("name", "selected2")], 200, 2.0).unwrap();
    let s3 = app.append_float(None, &[("name", "selected3")], 300, 3.0).unwrap();
    app.commit().unwrap();

    db.compact_selected_series(&[s3, s1, s2, s1, s3, s2]).unwrap();
    assert_eq!(db.head().num_series(), 0);
    assert_eq!(db.blocks().len(), 1);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_EmptyRefs
#[test]
fn test_compact_selected_series_empty_refs() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), DEFAULT_BLOCK_DURATION_MS);
    db.compact_selected_series(&[]).unwrap();
    assert!(db.blocks().is_empty());
    assert_eq!(db.selected_series_compaction_metrics_for_test(), (0, 0));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_MultipleChunkRanges
#[test]
fn test_compact_selected_series_multiple_chunk_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let mut r = None;
    for ts in (100..1500).step_by(100) {
        r = Some(app.append_float(r, &[("name", "spanning")], ts, ts as f64).unwrap());
    }
    app.commit().unwrap();
    db.compact_selected_series(&[r.unwrap()]).unwrap();
    assert_eq!(db.head().num_series(), 0);
    assert_eq!(db.blocks().len(), 2);
    assert!(db.blocks().iter().all(|b| b.meta().compaction.from_selected_series()));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_DecrementsNumStaleSeriesWhenStaleSeriesCompacted
#[test]
fn test_compact_selected_series_decrements_num_stale_series_when_stale_series_compacted() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let stale_r = app.append_float(None, &[("name", "stale")], 100, 1.0).unwrap();
    app.append_float(Some(stale_r), &[("name", "stale")], 200, stale_nan()).unwrap();
    let ns1 = app.append_float(None, &[("name", "non-stale-1")], 100, 2.0).unwrap();
    let ns2 = app.append_float(None, &[("name", "non-stale-2")], 100, 3.0).unwrap();
    app.commit().unwrap();

    assert_eq!(db.head().num_stale_series(), 1);
    db.compact_selected_series(&[ns1]).unwrap();
    assert_eq!(db.head().num_series(), 2);
    assert_eq!(db.head().num_stale_series(), 1);
    db.compact_selected_series(&[stale_r, ns2]).unwrap();
    assert_eq!(db.head().num_series(), 0);
    assert_eq!(db.head().num_stale_series(), 0);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_SkipsSeriesWithOOOData
#[test]
fn test_compact_selected_series_skips_series_with_ooo_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = opts_range(1000);
    opts.head.ooo_time_window_ms = 1000;
    let db = Tsdb::open(dir.path(), opts).unwrap();
    db.disable_compactions();
    let mut app = db.appender();
    let r_ooo = app.append_float(None, &[("name", "with-ooo")], 100, 1.0).unwrap();
    let r_ok = app.append_float(None, &[("name", "without-ooo")], 100, 2.0).unwrap();
    app.append_float(Some(r_ooo), &[("name", "with-ooo")], 200, 1.1).unwrap();
    app.append_float(Some(r_ok), &[("name", "without-ooo")], 200, 2.1).unwrap();
    app.commit().unwrap();
    let mut app = db.appender();
    app.append_float(Some(r_ooo), &[("name", "with-ooo")], 50, 1.5).unwrap();
    app.commit().unwrap();

    db.compact_selected_series(&[r_ooo, r_ok]).unwrap();
    assert_eq!(db.head().num_series(), 1);
    assert_eq!(db.selected_series_compaction_metrics_for_test(), (1, 0));
    assert_eq!(db.blocks().len(), 1);
    assert!(db.blocks()[0].meta().compaction.from_selected_series());
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_HintPropagation
#[test]
fn test_compact_selected_series_hint_propagation() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let r = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
    app.commit().unwrap();
    db.compact_selected_series(&[r]).unwrap();

    let src = &db.blocks()[0];
    let src_meta = src.meta();
    assert!(src_meta.compaction.from_selected_series());
    let dest = tempfile::tempdir().unwrap();
    let new_m = write_block_from_block_with_base(
        dest.path(), src, src_meta.min_time, src_meta.max_time + 1, Some(src_meta),
        mimir_tsdb::chunkenc::Encoding::Xor, 0, None,
    ).unwrap().unwrap();
    let new_meta = read_meta_file(&dest.path().join(new_m.ulid.to_string())).unwrap();
    assert!(new_meta.compaction.from_selected_series());
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestInOrderBlocksMaxTime_ExcludesSelectedSeriesBlocks
#[test]
fn test_in_order_blocks_max_time_excludes_selected_series_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let r = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
    app.commit().unwrap();
    db.compact_selected_series(&[r]).unwrap();
    assert!(db.blocks()[0].meta().compaction.from_selected_series() && db.in_order_blocks_max_time().is_none());
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestDBOpen_MinValidTime
#[test]
fn test_db_open_min_valid_time() {
    for case in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let bdir = create_xor_block(dir.path(), 1, 5, 0, 5000);
        let block_maxt = read_meta_file(&bdir).unwrap().max_time;
        let wal_dir = dir.path().join("wal");
        match case {
            0 => Head::create_wal_checkpoint_for_test(&wal_dir, 0, 1000, 999_999).unwrap(),
            1 => Head::create_wal_checkpoint_for_test(&wal_dir, 0, 1000, 1).unwrap(),
            2 => {}
            _ => {
                Head::create_wal_checkpoint_for_test(&wal_dir, 0, 1000, 999_999).unwrap();
                Head::corrupt_wal_checkpoint_first_byte_for_test(&wal_dir).unwrap();
            }
        }
        let db = Tsdb::open(dir.path(), TsdbOptions::default()).unwrap();
        let expected = if case == 0 { 999_999 } else { block_maxt };
        assert_eq!(db.head().min_valid_time(), expected, "case {case}");
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_LateAppendDuringCompactionSurvivesRestart
#[test]
fn test_compact_selected_series_late_append_during_compaction_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_db_range(dir.path(), 1000));
    let mut app = db.appender();
    app.append_float(None, &[("name", "filler")], 100, 0.1).unwrap();
    app.append_float(None, &[("name", "filler")], 700, 0.7).unwrap();
    let sel_ref = app.append_float(None, &[("name", "selected")], 100, 10.0).unwrap();
    app.append_float(Some(sel_ref), &[("name", "selected")], 200, 20.0).unwrap();
    app.commit().unwrap();

    let db_c = Arc::clone(&db);
    db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
        let mut a = db_c.appender();
        a.append_float(Some(sel_ref), &[("name", "selected")], 400, 40.0).unwrap();
        a.commit().unwrap();
    })));
    db.compact_selected_series(&[sel_ref]).unwrap();
    assert_eq!(db.blocks().len(), 1);
    assert_eq!(db.head().num_series(), 2);
    let got = query_db(&db, 0, 1000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got[r#"{name="selected"}"#], &[(100, 10.0), (200, 20.0), (400, 40.0)]);
    db.close().unwrap();

    let db2 = open_db_range(dir.path(), 1000);
    let got2 = query_db(&db2, 0, 1000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got2[r#"{name="selected"}"#], &[(100, 10.0), (200, 20.0), (400, 40.0)]);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_RestartDoesNotReplayOrphanedSampleAsUnknownSeriesRef
#[test]
fn test_compact_selected_series_restart_does_not_replay_orphaned_sample_as_unknown_series_ref() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let sel_ref = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
    app.commit().unwrap();
    for _ in 0..30 { db.head().wal_next_segment_for_test().unwrap(); }
    let mut app = db.appender();
    app.append_float(Some(sel_ref), &[("name", "selected")], 1200, 2.0).unwrap();
    app.commit().unwrap();
    db.compact_selected_series(&[sel_ref]).unwrap();
    assert_eq!((db.head().num_series(), db.blocks().len(), db.head().get_wal_expiry_for_test(sel_ref)), (0, 2, Some(1200)));
    force_checkpoint_at(db.head(), 150);
    assert!(checkpoint_has_series_ref(db.head(), sel_ref));
    force_checkpoint_at(db.head(), 1201);
    assert!(!checkpoint_has_series_ref(db.head(), sel_ref));
    db.close().unwrap();
    let reopened = open_db_range(dir.path(), 1000);
    assert_eq!(reopened.head().wal_replay_unknown_refs_total_for_test("samples"), 0);
    reopened.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_OOOAppendDuringCompactionSurvives
#[test]
fn test_compact_selected_series_ooo_append_during_compaction_survives() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = opts_range(1000);
    opts.head.ooo_time_window_ms = 1000;
    let db = Arc::new(Tsdb::open(dir.path(), opts).unwrap());
    db.disable_compactions();
    let mut app = db.appender();
    app.append_float(None, &[("name", "filler")], 100, 0.1).unwrap();
    app.append_float(None, &[("name", "filler")], 700, 0.7).unwrap();
    let sel_ref = app.append_float(None, &[("name", "selected")], 100, 10.0).unwrap();
    app.append_float(Some(sel_ref), &[("name", "selected")], 200, 20.0).unwrap();
    app.commit().unwrap();

    let db_c = Arc::clone(&db);
    db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
        let mut a = db_c.appender();
        a.append_float(Some(sel_ref), &[("name", "selected")], 150, 15.0).unwrap();
        a.commit().unwrap();
    })));
    db.compact_selected_series(&[sel_ref]).unwrap();
    assert_eq!(db.head().num_series(), 2);
    let got = query_db(&db, 0, 1000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got[r#"{name="selected"}"#], &[(100, 10.0), (150, 15.0), (200, 20.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_OpenAppenderCommittingDuringCompaction
#[test]
fn test_compact_selected_series_open_appender_committing_during_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(open_db_range(dir.path(), 1000));
    let mut app = db.appender();
    app.append_float(None, &[("name", "filler")], 100, 0.1).unwrap();
    app.append_float(None, &[("name", "filler")], 700, 0.7).unwrap();
    let sel_ref = app.append_float(None, &[("name", "selected")], 100, 10.0).unwrap();
    app.append_float(Some(sel_ref), &[("name", "selected")], 200, 20.0).unwrap();
    app.commit().unwrap();

    let mut late = db.appender();
    late.append_float(Some(sel_ref), &[("name", "selected")], 400, 40.0).unwrap();
    let (hook_tx, hook_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let done_rx = std::sync::Mutex::new(done_rx);
    db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
        hook_tx.send(()).unwrap();
        done_rx.lock().unwrap().recv().unwrap();
    })));
    std::thread::scope(|s| {
        let h = s.spawn(|| db.compact_selected_series(&[sel_ref]).unwrap());
        hook_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        late.commit().unwrap();
        done_tx.send(()).unwrap();
        h.join().unwrap();
    });
    assert_eq!(db.blocks().len(), 1);
    assert_eq!(db.head().num_series(), 2);
    let got = query_db(&db, 0, 1000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got[r#"{name="selected"}"#], &[(100, 10.0), (200, 20.0), (400, 40.0)]);
    db.close().unwrap();

    let db2 = open_db_range(dir.path(), 1000);
    let got2 = query_db(&db2, 0, 1000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got2[r#"{name="selected"}"#], &[(100, 10.0), (200, 20.0), (400, 40.0)]);
    db2.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_AppenderEvictionRaces
#[test]
fn test_compact_selected_series_appender_eviction_races() {
    // Subtest 1: retries after series evicted
    {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_db_range(dir.path(), 1000));
        let mut base = db.appender();
        let old_ref = base.append_float(None, &[("series", "resolved-before-gc")], 100, 1.0).unwrap();
        base.commit().unwrap();

        let (lk_tx, lk_rx) = mpsc::channel();
        let (res_tx, res_rx) = mpsc::channel::<()>();
        let res_rx = Arc::new(std::sync::Mutex::new(Some(res_rx)));
        db.head().set_after_series_lookup_hook_for_test(Some(Arc::new(move |entry| {
            if entry.is_some_and(|e| e.ref_id == old_ref) {
                let _ = lk_tx.send(());
                if let Some(rx) = res_rx.lock().unwrap().take() { let _ = rx.recv(); }
            }
        })));

        let db_a = Arc::clone(&db);
        let (done_tx, done_rx) = mpsc::channel();
        let h = std::thread::spawn(move || {
            let mut pend = db_a.appender();
            pend.append_float(Some(old_ref), &[("series", "resolved-before-gc")], 200, 2.0).unwrap();
            pend.commit().unwrap();
            done_tx.send(()).unwrap();
        });
        lk_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        db.head().set_after_series_lookup_hook_for_test(None);
        db.compact_selected_series(&[old_ref]).unwrap();
        let _ = res_tx.send(());
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        h.join().unwrap();

        assert_eq!(db.blocks().len(), 1);
        let got = query_db(&db, i64::MIN, i64::MAX, &[match_eq("series", "resolved-before-gc")]);
        assert_sample_vals(&got[r#"{series="resolved-before-gc"}"#], &[(100, 1.0), (200, 2.0)]);
        db.close().unwrap();

        let db2 = open_db_range(dir.path(), 1000);
        let got2 = query_db(&db2, i64::MIN, i64::MAX, &[match_eq("series", "resolved-before-gc")]);
        assert_sample_vals(&got2[r#"{series="resolved-before-gc"}"#], &[(100, 1.0), (200, 2.0)]);
        db2.close().unwrap();
    }
    // Subtest 2: overlapping appenders keep series
    for commit_interfering in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db_range(dir.path(), 1000);
        let mut base = db.appender();
        let r = base.append_float(None, &[("series", "overlapping-appenders")], 100, 1.0).unwrap();
        base.commit().unwrap();

        let mut pend = db.appender();
        pend.append_float(Some(r), &[("series", "overlapping-appenders")], 200, 2.0).unwrap();
        let mut interfering = db.appender();
        interfering.append_float(Some(r), &[("series", "overlapping-appenders")], 100, 1.0).unwrap();
        if commit_interfering { interfering.commit().unwrap(); } else { interfering.rollback().unwrap(); }

        db.compact_selected_series(&[r]).unwrap();
        pend.commit().unwrap();
        let got = query_db(&db, i64::MIN, i64::MAX, &[match_eq("series", "overlapping-appenders")]);
        assert_sample_vals(&got[r#"{series="overlapping-appenders"}"#], &[(100, 1.0), (200, 2.0)]);
        db.close().unwrap();

        let db2 = open_db_range(dir.path(), 1000);
        let got2 = query_db(&db2, i64::MIN, i64::MAX, &[match_eq("series", "overlapping-appenders")]);
        assert_sample_vals(&got2[r#"{series="overlapping-appenders"}"#], &[(100, 1.0), (200, 2.0)]);
        db2.close().unwrap();
    }
}

fn assert_unmerged_hinted_block(hint: &str) {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    app.append_float(None, &[("name", "wal_only")], 700, 42.0).unwrap();
    app.commit().unwrap();
    write_single_series_block(db.dir(), "plain", &[(0, 1.0), (499, 1.0)], None);
    let hdir = write_single_series_block(db.dir(), hint, &[(0, 1.0), (999, 1.0)], Some(hint));
    assert_eq!(read_meta_file(&hdir).unwrap().max_time, 1000);
    db.reload_blocks().unwrap();
    db.compact_blocks().unwrap();
    db.reload_blocks().unwrap();
    assert!(db.in_order_blocks_max_time().unwrap() < 700);
    let got = query_head_range(&db, 0, 1000, &[match_eq("name", "wal_only")], false);
    assert_sample_vals(&got[r#"{name="wal_only"}"#], &[(700, 42.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestSelectedBlockNotMergedWithNonSelectedBlock
#[test]
fn test_selected_block_not_merged_with_non_selected_block() { assert_unmerged_hinted_block("selected"); }

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestStaleBlockNotMergedWithNonStaleBlock
#[test]
fn test_stale_block_not_merged_with_non_stale_block() { assert_unmerged_hinted_block("stale"); }

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestOutOfOrderBlockMergePreservesHint
#[test]
fn test_out_of_order_block_merge_preserves_hint() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    app.append_float(None, &[("name", "wal_only")], 700, 42.0).unwrap();
    app.commit().unwrap();
    write_single_series_block(db.dir(), "ooo_a", &[(0, 1.0), (499, 1.0)], Some("ooo"));
    write_single_series_block(db.dir(), "ooo_b", &[(0, 1.0), (999, 1.0)], Some("ooo"));

    db.reload_blocks().unwrap();
    db.compact_blocks().unwrap(); db.reload_blocks().unwrap();
    assert_eq!(db.blocks().len(), 1);
    let blks = db.blocks();
    let m = blks[0].meta();
    assert!(m.compaction.from_out_of_order());
    assert_eq!(m.max_time, 1000);
    assert_eq!(db.in_order_blocks_max_time(), None);
    let got = query_head_range(&db, 0, 1000, &[match_eq("name", "wal_only")], false);
    assert_sample_vals(&got[r#"{name="wal_only"}"#], &[(700, 42.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_ChunkBoundarySampleNotLost
#[test]
fn test_compact_selected_series_chunk_boundary_sample_not_lost() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let r = app.append_float(None, &[("name", "selected")], 500, 1.0).unwrap();
    app.append_float(Some(r), &[("name", "selected")], 1000, 2.0).unwrap();
    app.commit().unwrap();
    db.compact_selected_series(&[r]).unwrap();
    let got = query_db(&db, 0, 2000, &[match_eq("name", "selected")]);
    assert_sample_vals(&got[r#"{name="selected"}"#], &[(500, 1.0), (1000, 2.0)]);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_SparseSelectedAcrossWideHead
#[test]
fn test_compact_selected_series_sparse_selected_across_wide_head() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let r = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
    app.append_float(Some(r), &[("name", "selected")], 200, 2.0).unwrap();
    app.append_float(None, &[("name", "filler")], 10500, 100.0).unwrap();
    app.commit().unwrap();

    db.compact_selected_series(&[r]).unwrap();
    assert_eq!(db.blocks().len(), 1);
    let blks = db.blocks();
    let m = blks[0].meta();
    assert!(m.compaction.from_selected_series());
    assert_eq!((m.min_time, m.max_time), (0, 1000));
    assert_eq!((m.stats.num_series, m.stats.num_samples), (1, 2));
    let ulid_dirs = fs::read_dir(db.dir()).unwrap().filter_map(|e| e.ok()).filter(|e| e.file_type().unwrap().is_dir() && e.file_name().len() == 26).count();
    assert_eq!(ulid_dirs, 1);
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_EvictedSeriesRecordKeptInCheckpoint
#[test]
fn test_compact_selected_series_evicted_series_record_kept_in_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_db_range(dir.path(), 1000);
    let mut app = db.appender();
    let sel_ref = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
    app.append_float(Some(sel_ref), &[("name", "selected")], 200, 2.0).unwrap();
    app.commit().unwrap();
    db.compact_selected_series(&[sel_ref]).unwrap();
    assert!(db.head().get_wal_expiry_for_test(sel_ref).is_some());
    force_checkpoint_at(db.head(), 150);
    assert!(checkpoint_has_series_ref(db.head(), sel_ref));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestBeyondSizeRetentionWithPercentage
#[test]
fn test_beyond_size_retention_with_percentage() {
    let dir = tempfile::tempdir().unwrap();
    let extra = TsdbExtraOptions { max_percentage: 10.0, fs_size_func: Some(Arc::new(|_| 100 * 1024)), ..Default::default() };
    let db = Tsdb::open_with_extra(dir.path(), TsdbOptions::default(), extra).unwrap();
    assert_eq!(db.head().size(), 0);
    let mut blocks: Vec<(Ulid, u64)> = (0..10).map(|_| (Ulid::new(), 1024)).collect();
    assert!(db.beyond_size_retention_for_test(&blocks).is_empty());
    let oldest = Ulid::new();
    blocks.push((oldest, 1024));
    let del = db.beyond_size_retention_for_test(&blocks);
    assert_eq!(del, HashSet::from([oldest]));
    db.close().unwrap();
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestOOOCompactionAcrossChunkIDWrap
#[test]
fn test_ooo_compaction_across_chunk_id_wrap() {
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let mut opts = TsdbOptions::default();
        opts.head.ooo_cap_max = 5;
        opts.head.ooo_time_window_ms = 4 * HOUR_MS;
        let db = Tsdb::open(dir.path(), opts).unwrap();
        db.disable_compactions();
        let pairs = [("l", "v1")];
        let min_ms = |m: i64| m * 60_000;
        let mut exp = Vec::new();
        let mut app = db.appender();
        let (r, s0) = append_kind_v2(&mut app, None, kind, &pairs, min_ms(200), 200).unwrap();
        exp.push(s0);
        for i in 1..=15i64 {
            exp.push(append_kind_v2(&mut app, Some(r), kind, &pairs, min_ms(i), i).unwrap().1);
        }
        app.commit().unwrap();

        let seed = OOO_CHUNK_ID_MASK - 1;
        db.head().set_first_ooo_chunk_id_for_test(r, seed);
        assert_eq!(ooo_head_chunk_id(seed, 0), (OOO_CHUNK_ID_MASK - 1) | OOO_CHUNK_ID_MASK);
        assert_eq!(resolve_ooo_chunk_pos(seed, 1, 4), Some(2));

        let mut app = db.appender();
        for i in 16..=30i64 {
            exp.push(append_kind_v2(&mut app, Some(r), kind, &pairs, min_ms(i), i).unwrap().1);
        }
        app.commit().unwrap();
        db.compact().unwrap();

        exp.sort_by_key(|s| s.t);
        let mut exp_map = BTreeMap::new();
        exp_map.insert(r#"{l="v1"}"#.to_string(), exp);
        compare_series(&exp_map, &query_db(&db, 0, min_ms(300), &[match_eq("l", "v1")]), true);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestInOrderCompactionAcrossChunkIDWrap
#[test]
fn test_in_order_compaction_across_chunk_id_wrap() {
    for kind in ALL_SAMPLE_KINDS {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db_range(dir.path(), 100);
        let pairs = [("l", "v1")];
        let mut app = db.appender();
        let (r, s0) = append_kind_v2(&mut app, None, kind, &pairs, 0, 0).unwrap();
        app.commit().unwrap();

        let seed = OOO_CHUNK_ID_MASK - 1;
        db.head().set_first_chunk_id_for_test(r, seed);
        let newest_before = db.head().newest_chunk_id_for_test(r).unwrap();

        let mut exp = vec![s0];
        let mut app = db.appender();
        for ts in (10..500).step_by(10) {
            exp.push(append_kind_v2(&mut app, Some(r), kind, &pairs, ts, ts).unwrap().1);
        }
        app.commit().unwrap();
        exp.sort_by_key(|s| s.t);
        assert!(db.head().newest_chunk_id_for_test(r).unwrap() < newest_before);

        let mut exp_map = BTreeMap::new();
        exp_map.insert(r#"{l="v1"}"#.to_string(), exp);
        compare_series(&exp_map, &query_db(&db, 0, 500, &[match_eq("l", "v1")]), true);

        db.compact().unwrap();
        assert!(db.head().first_chunk_id_for_test(r).unwrap() < seed);
        compare_series(&exp_map, &query_db(&db, 0, 500, &[match_eq("l", "v1")]), true);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactionSurvivesChunkRollAndMmap
#[test]
fn test_compaction_survives_chunk_roll_and_mmap() {
    for stale in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_db_range(dir.path(), 1000));
        let v = if stale { stale_nan() } else { 1.0 };
        let mut app = db.appender();
        app.append_float(None, &[("name", "filler")], 1400, 1.0).unwrap();
        let r = app.append_float(None, &[("name", "selected")], 900, v).unwrap();
        app.commit().unwrap();

        let before = db.head().snapshot_fingerprints_with_watermark_for_test(&[r], u64::MAX);
        let db_c = Arc::clone(&db);
        let before_c = before.clone();
        db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
            let mut a = db_c.appender();
            a.append_float(Some(r), &[("name", "selected")], 1000, v).unwrap();
            a.commit().unwrap();
            db_c.force_head_mmap().unwrap();
            let after = db_c.head().snapshot_fingerprints_with_watermark_for_test(&[r], u64::MAX);
            assert_ne!(before_c, after);
        })));
        if stale { db.compact_stale_head().unwrap(); } else { db.compact_selected_series(&[r]).unwrap(); }
        let got = query_db_raw_nans(&db, 0, 2000, &[match_eq("name", "selected")]);
        assert_eq!(got[r#"{name="selected"}"#].len(), 2);
        db.close().unwrap();
    }
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_SurvivesMutationFromStillOpenTransaction
#[test]
fn test_compact_selected_series_survives_mutation_from_still_open_transaction() {
    run_selected_mutation_race(false);
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactSelectedSeries_SurvivesEvidenceErasedByUnrelatedCleanup
#[test]
fn test_compact_selected_series_survives_evidence_erased_by_unrelated_cleanup() {
    run_selected_mutation_race(true);
}

// Upstream: github.com/prometheus/prometheus/tsdb/db_test.go::TestCompactionSurvivesLateAppendWithErasedWatermarkEvidence
#[test]
fn test_compaction_survives_late_append_with_erased_watermark_evidence() {
    for stale in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(open_db_range(dir.path(), 1000));
        let v = if stale { stale_nan() } else { 2.0 };
        let mut app = db.appender();
        let sel_ref = app.append_float(None, &[("name", "selected")], 100, 1.0).unwrap();
        app.append_float(Some(sel_ref), &[("name", "selected")], 200, v).unwrap();
        app.append_float(None, &[("name", "filler")], 700, 1.0).unwrap();
        app.commit().unwrap();

        let db_c = Arc::clone(&db);
        db.set_compact_head_view_before_evict_hook_for_test(Some(Arc::new(move || {
            for _ in 0..2 {
                let mut a = db_c.appender();
                a.append_float(Some(sel_ref), &[("name", "selected")], 400, v).unwrap();
                a.commit().unwrap();
            }
        })));
        if stale { db.compact_stale_head().unwrap(); } else { db.compact_selected_series(&[sel_ref]).unwrap(); }
        let got = query_db_raw_nans(&db, 0, 1000, &[match_eq("name", "selected")]);
        assert_eq!(got[r#"{name="selected"}"#].len(), 3);
        db.close().unwrap();

        let db2 = open_db_range(dir.path(), 1000);
        let got2 = query_db_raw_nans(&db2, 0, 1000, &[match_eq("name", "selected")]);
        assert_eq!(got2[r#"{name="selected"}"#].len(), 3);
        db2.close().unwrap();
    }
}
