use std::fs;
use std::sync::Arc;
use std::time::Instant;

use prometheus::core::Collector;

use mimir_tsdb::block::BlockMetaCompaction;
use mimir_tsdb::chunkenc::float_histogram::{
    AppendFloatHistogramOutcome, FloatHistogramChunk,
};
use mimir_tsdb::chunkenc::histogram::{AppendHistogramOutcome, HistogramChunk};
use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHint, FloatHistogram, Histogram, HistogramSpan,
};
use mimir_tsdb::compact::{write_head_block_streaming, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::go_regexp_syntax;
use mimir_tsdb::head::{Head, HeadError, HeadOptions};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{FastRegexMatcher, LabelMatcher, MatchType};
use mimir_tsdb::metrics::Summary;
use mimir_tsdb::record::{RecordEncoder, RefSeries};
use mimir_tsdb::wlog::{CompressionType, WlogWriter};

fn metric_family<'a>(
    families: &'a [prometheus::proto::MetricFamily],
    name: &str,
) -> Option<&'a prometheus::proto::MetricFamily> {
    families.iter().find(|mf| mf.get_name() == name)
}

#[test]
fn w20_cr_01_seed_head_bounds_from_in_order_block_before_wal_replay() {
    let dir = tempfile::tempdir().unwrap();
    let block_range = 7_200_000i64;
    let mut opts = TsdbOptions::default();
    opts.head.chunk_range_ms = block_range;

    // 1. Write samples in [0, block_range), compact into an in-order block, and close.
    {
        let db = Tsdb::open(dir.path(), opts.clone()).unwrap();
        let mut app = db.appender();
        let p = [("__name__", "cr01_m"), ("job", "a")];
        app.append_float(None, &p, 1_000, 1.0).unwrap();
        app.append_float(None, &p, 5_000, 2.0).unwrap();
        app.commit().unwrap();
        let uid = db.compact_head_range(0, block_range).unwrap().unwrap();
        let blks = db.blocks();
        assert_eq!(blks.len(), 1);
        assert_eq!(blks[0].ulid(), uid);
        assert_eq!(blks[0].max_time(), block_range);
        db.close().unwrap();
    }

    // 2a. Reopen with OOO window = 1000: Head must be seeded at B = 7_200_000,
    //     so a late write at B - window - 1 MUST fail with TooOldSample (not bypass OOO).
    {
        let mut ooo_opts = opts.clone();
        ooo_opts.head.ooo_time_window_ms = 1_000;
        let db = Tsdb::open(dir.path(), ooo_opts).unwrap();
        assert!(db.head().initialized());
        assert_eq!(db.head().min_time(), block_range);
        assert_eq!(db.head().max_time(), block_range);
        assert!(db.head().min_valid_time() >= block_range);

        let mut app = db.appender();
        let err = app
            .append_float(
                None,
                &[("__name__", "cr01_late"), ("job", "a")],
                block_range - 1_001,
                9.0,
            )
            .unwrap_err();
        assert!(
            matches!(err, HeadError::TooOldSample),
            "expected TooOldSample, got {err:?}"
        );
        app.rollback().unwrap();
        db.close().unwrap();
    }

    // 2b. Reopen with OOO window = 0: first write < B fails OutOfBounds and does NOT
    //     lower min_valid_time below B across compaction.
    {
        let db = Tsdb::open(dir.path(), opts).unwrap();
        assert!(db.head().initialized());
        assert_eq!(db.head().min_time(), block_range);
        assert_eq!(db.head().max_time(), block_range);

        let mut app = db.appender();
        let err = app
            .append_float(
                None,
                &[("__name__", "cr01_oob"), ("job", "a")],
                block_range - 100_000,
                1.0,
            )
            .unwrap_err();
        assert!(matches!(err, HeadError::OutOfBounds), "got {err:?}");
        app.append_float(
            None,
            &[("__name__", "cr01_ok"), ("job", "a")],
            block_range + 100,
            2.0,
        )
        .unwrap();
        app.commit().unwrap();
        assert!(db.head().min_valid_time() >= block_range);
        db.close().unwrap();
    }
}

#[test]
fn w20_cr_02_startup_gc_drops_sampleless_series_when_uninitialized() {
    let dir = tempfile::tempdir().unwrap();
    // Write a WAL containing only a Series record (no samples), simulating a series created
    // before its only sample was rejected as TooOldSample when no block exists yet.
    {
        let mut w = WlogWriter::open(dir.path().join("wal"), CompressionType::None).unwrap();
        let enc = RecordEncoder::new(false);
        let rec = enc.series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_sorted_pairs([("__name__", "cr02_orphan"), ("job", "a")]),
            }],
            Vec::new(),
        );
        w.log(&[&rec]).unwrap();
        w.close().unwrap();
    }

    let opts = TsdbOptions {
        head: HeadOptions {
            ooo_time_window_ms: 1_000,
            ..HeadOptions::default()
        },
        ..TsdbOptions::default()
    };
    let db = Tsdb::open(dir.path(), opts).unwrap();
    assert!(!db.head().initialized());
    assert_eq!(
        db.head().num_series(),
        0,
        "sample-less WAL series must be removed on startup GC when head is uninitialized"
    );
    db.close().unwrap();
}

#[test]
fn m20_03_summary_computes_sliding_window_quantiles() {
    let s = Summary::new("test_wal_fsync_duration_seconds", "help", &[0.5, 0.9, 0.99]);

    // Empty 10m window emits NaN quantiles.
    let mf_empty = s.collect();
    assert_eq!(mf_empty.len(), 1);
    let proto_empty = mf_empty[0].get_metric()[0].get_summary();
    assert_eq!(proto_empty.get_sample_count(), 0);
    for q in proto_empty.get_quantile() {
        assert!(q.get_value().is_nan(), "empty window must emit NaN");
    }

    // Observe finite samples including 0.0 -> finite quantiles.
    for v in [0.0, 0.002, 0.005, 0.010, 0.050, 0.100] {
        s.observe(v);
    }
    let mf = s.collect();
    let proto = mf[0].get_metric()[0].get_summary();
    assert_eq!(proto.get_sample_count(), 6);
    assert!((proto.get_sample_sum() - 0.167).abs() < 1e-12);
    let qs: Vec<f64> = proto.get_quantile().iter().map(|q| q.get_value()).collect();
    assert_eq!(qs.len(), 3);
    for &v in &qs {
        assert!(v.is_finite(), "observed window quantile must be finite, got {v}");
    }
    assert!(qs[0] <= qs[1] && qs[1] <= qs[2], "quantiles must be monotonic: {qs:?}");
}

#[test]
fn m20_04_and_m20_02_per_tenant_tsdb_metrics_and_native_histograms() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let mut opts = TsdbOptions::default();
    opts.head.registry = Some(Arc::clone(&reg));
    opts.head.ooo_time_window_ms = 60_000;
    opts.head.wal_compression = CompressionType::Snappy;
    opts.head.chunk_range_ms = 10_000;
    opts.retention_duration_ms = 3_600_000;

    let db = Tsdb::open(dir.path(), opts).unwrap();

    // Write float, stale float, native histogram, OOO float, OOO rejection, duplicate.
    let h = Histogram {
        schema: 0,
        zero_threshold: 0.001,
        zero_count: 1,
        count: 4,
        sum: 1.5,
        positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
        positive_buckets: vec![1, 1],
        ..Histogram::default()
    };
    let ref_sel;
    {
        let mut app = db.appender();
        let pf = [("__name__", "m20_f"), ("job", "a")];
        let ph = [("__name__", "m20_h"), ("job", "a")];
        let ps = [("__name__", "m20_s"), ("job", "a")];
        let r_f = app.append_float(None, &pf, 1_000, 1.0).unwrap();
        app.append_float(Some(r_f), &pf, 11_000, 2.0).unwrap();
        app.append_float(Some(r_f), &pf, 25_000, 3.0).unwrap();
        // OOO float within 60s window
        app.append_float(Some(r_f), &pf, 20_000, 2.5).unwrap();
        // Too old (below 25_000 - 60_000 is negative, so use -100_000)
        let _ = app.append_float(Some(r_f), &pf, -100_000, 9.0);
        // Duplicate at same timestamp
        app.append_float(Some(r_f), &pf, 25_000, 3.0).unwrap();

        let r_h = app.append_histogram(None, &ph, 0, 1_000, h.clone()).unwrap();
        app.append_histogram(Some(r_h), &ph, 0, 2_000, h.clone()).unwrap();

        ref_sel = app.append_float(None, &ps, 1_000, 1.0).unwrap();
        app.append_float(Some(ref_sel), &ps, 2_000, stale_nan()).unwrap();
        app.commit().unwrap();
    }

    // Exercise head compaction, OOO compaction, selected series compaction, and overlapping block compaction.
    db.compact_head_range(0, 10_000).unwrap();
    db.compact_ooo_head().unwrap();
    db.compact_selected_series(&[ref_sel]).unwrap();

    // Compact head again to create another block and run overlapping block compaction.
    db.set_enable_overlapping_compaction(true);
    db.compact_blocks().unwrap();

    // Fsync WAL to exercise fsync_duration summary quantiles.
    db.head().sync().unwrap();

    let families = reg.gather();
    let required_families = [
        // WAL (4 new + 7 existing)
        "prometheus_tsdb_wal_storage_size_bytes",
        "prometheus_tsdb_wal_record_part_writes_total",
        "prometheus_tsdb_wal_record_parts_bytes_written_total",
        "prometheus_tsdb_wal_record_bytes_saved_total",
        "prometheus_tsdb_wal_fsync_duration_seconds",
        // WBL (4 new when OOO enabled)
        "prometheus_tsdb_out_of_order_wbl_storage_size_bytes",
        "prometheus_tsdb_out_of_order_wbl_record_part_writes_total",
        "prometheus_tsdb_out_of_order_wbl_record_parts_bytes_written_total",
        "prometheus_tsdb_out_of_order_wbl_record_bytes_saved_total",
        "prometheus_tsdb_out_of_order_wbl_fsync_duration_seconds",
        // Head (17 new)
        "prometheus_tsdb_head_appenders_created_total",
        "prometheus_tsdb_head_stale_series",
        "prometheus_tsdb_head_native_histogram_series",
        "prometheus_tsdb_head_native_histogram_buckets",
        "prometheus_tsdb_head_chunk_reads_of_removed_series_total",
        "prometheus_tsdb_head_series_pending_commit_underflow_total",
        "prometheus_tsdb_data_replay_duration_seconds",
        "prometheus_tsdb_head_samples_appended_total",
        "prometheus_tsdb_duplicate_samples_total",
        "prometheus_tsdb_out_of_bound_samples_total",
        "prometheus_tsdb_out_of_order_samples_total",
        "prometheus_tsdb_too_old_samples_total",
        "prometheus_tsdb_snapshot_replay_error_total",
        "prometheus_tsdb_head_max_time",
        "prometheus_tsdb_head_min_time",
        "prometheus_tsdb_isolation_low_watermark",
        "prometheus_tsdb_isolation_high_watermark",
        // Compactor (5 new)
        "prometheus_tsdb_compaction_populating_block",
        "prometheus_tsdb_vertical_compactions_total",
        "prometheus_tsdb_compaction_chunk_size_bytes",
        "prometheus_tsdb_compaction_chunk_samples",
        "prometheus_tsdb_compaction_chunk_range_seconds",
        // DB (14 new)
        "prometheus_tsdb_compactions_triggered_total",
        "prometheus_tsdb_compactions_skipped_total",
        "prometheus_tsdb_lowest_timestamp",
        "prometheus_tsdb_tombstone_cleanup_seconds",
        "prometheus_tsdb_retention_limit_bytes",
        "prometheus_tsdb_retention_limit_percentage",
        "prometheus_tsdb_retention_limit_seconds",
        "prometheus_tsdb_size_retentions_total",
        "prometheus_tsdb_stale_series_compactions_triggered_total",
        "prometheus_tsdb_stale_series_compactions_failed_total",
        "prometheus_tsdb_stale_series_compaction_duration_seconds",
        "prometheus_tsdb_selected_series_compactions_triggered_total",
        "prometheus_tsdb_selected_series_compactions_failed_total",
        "prometheus_tsdb_selected_series_compaction_duration_seconds",
    ];
    for name in required_families {
        assert!(
            metric_family(&families, name).is_some(),
            "missing per-tenant metric family {name}"
        );
    }

    // Native histogram fields must be populated on compaction_duration_seconds and sample_ooo_delta!
    // (mimir_common::native_histogram stores proto3 native histogram fields in unknown_fields of proto::Histogram.)
    for nh_name in [
        "prometheus_tsdb_compaction_duration_seconds",
        "prometheus_tsdb_sample_ooo_delta",
    ] {
        let mf = metric_family(&families, nh_name).unwrap();
        let h = mf.get_metric()[0].get_histogram();
        assert!(h.get_sample_count() > 0, "{nh_name} must have samples");
        assert!(
            h.unknown_fields.fields.as_ref().is_some_and(|f| !f.is_empty()),
            "{nh_name} must carry native histogram fields in protobuf encoding"
        );
    }

    // Check WAL fsync summary quantiles are finite (M20-03).
    let wal_fsync = metric_family(&families, "prometheus_tsdb_wal_fsync_duration_seconds").unwrap();
    let wal_sum = wal_fsync.get_metric()[0].get_summary();
    assert!(wal_sum.get_sample_count() > 0);
    for q in wal_sum.get_quantile() {
        assert!(q.get_value().is_finite(), "WAL fsync quantile must be finite");
    }

    // Check active metrics reflect real counts.
    assert!(
        metric_family(&families, "prometheus_tsdb_head_native_histogram_series")
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value()
            >= 1.0
    );
    assert!(
        metric_family(&families, "prometheus_tsdb_head_native_histogram_buckets")
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value()
            >= 2.0
    );
    assert!(
        metric_family(&families, "prometheus_tsdb_selected_series_compactions_triggered_total")
            .unwrap()
            .get_metric()[0]
            .get_counter()
            .get_value()
            >= 1.0
    );
    assert_eq!(
        metric_family(&families, "prometheus_tsdb_retention_limit_seconds")
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value(),
        3600.0
    );
    assert_eq!(
        metric_family(&families, "prometheus_tsdb_compaction_populating_block")
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value(),
        0.0
    );

    // Ensure main default registry was NOT polluted.
    let global_fams = prometheus::default_registry().gather();
    for mf in &global_fams {
        assert!(
            !mf.get_name().starts_with("prometheus_tsdb_"),
            "main default registry polluted with {}",
            mf.get_name()
        );
    }

    db.close().unwrap();
}

#[test]
fn w20_sdw_04_regexp_check_limits_and_dfa_size() {
    // 1. Nesting depth limit: 1000 capturing groups -> height 1001 > 1000 -> ErrNestingDepth.
    let deep_fail = format!("{}a{}", "(".repeat(1000), ")".repeat(1000));
    let err_deep = go_regexp_syntax::check(&deep_fail).unwrap_err();
    assert_eq!(err_deep.code, "expression nests too deeply");
    assert!(err_deep
        .to_string()
        .starts_with("error parsing regexp: expression nests too deeply: "));

    // 999 capturing groups -> height 1000 <= 1000 -> Ok.
    let deep_ok = format!("{}a{}", "(".repeat(999), ")".repeat(999));
    go_regexp_syntax::check(&deep_ok).unwrap();

    // 2. Expression size limit (> 3_355_443): rejected in O(1) memory with ErrLarge.
    let huge_concat_repeat = format!("({}){{1000}}", "a".repeat(4000));
    let err_large = go_regexp_syntax::check(&huge_concat_repeat).unwrap_err();
    assert_eq!(err_large.code, "expression too large");
    assert!(err_large
        .to_string()
        .starts_with("error parsing regexp: expression too large: "));

    // Also test nested repeat checked for ErrLarge BEFORE ErrInvalidRepeatSize (Go parse.go:434).
    let huge_alt = "(a{1000}|b{1000}|c{1000}|d{1000}){1000}";
    let err_huge_alt = go_regexp_syntax::check(huge_alt).unwrap_err();
    assert_eq!(err_huge_alt.code, "expression too large");

    // And LabelMatcher::new propagates the exact Go error without compiling NFA.
    let m_err =
        LabelMatcher::new(MatchType::RegexMatch, "__name__", &huge_concat_repeat).unwrap_err();
    assert!(
        m_err.to_string().contains("expression too large"),
        "unexpected error: {m_err}"
    );

    // 3. Exponential-DFA pattern compiles and matches accurately with default dfa_size_limit.
    let m = FastRegexMatcher::new("[01]*1[01]{20}").unwrap();
    assert!(m.is_match("0000100000000000000000000"));
    assert!(!m.is_match("0000000000000000000000000"));
}

#[test]
fn w20_sdw_06_skip_empty_chunk_range_gaps_and_preserve_i64_max() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_time_window_ms = 1_000;
    let db = Tsdb::open(dir.path(), opts).unwrap();

    let r1;
    let r2;
    let r3;
    {
        let mut app = db.appender();
        r1 = app
            .append_float(None, &[("__name__", "sdw06_min")], i64::MIN + 1, 1.0)
            .unwrap();
        r2 = app
            .append_float(None, &[("__name__", "sdw06_now")], 1_780_000_000_000, 2.0)
            .unwrap();
        r3 = app
            .append_float(None, &[("__name__", "sdw06_max")], i64::MAX, 3.0)
            .unwrap();
        app.commit().unwrap();
    }

    let t0 = Instant::now();
    let ulids = db.compact_selected_series(&[r1, r2, r3]).unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed.as_secs() < 5,
        "compact_selected_series took too long across extreme gap: {elapsed:?}"
    );
    assert_eq!(ulids.len(), 3, "must produce 3 blocks for 3 sparse windows");

    // Verify no .tmp directories remain.
    for ent in fs::read_dir(dir.path()).unwrap() {
        let name = ent.unwrap().file_name().into_string().unwrap();
        assert!(!name.contains(".tmp"), "unexpected tmp dir left behind: {name}");
    }

    // Verify i64::MAX sample survived in the compacted block!
    let q = db.querier(i64::MIN, i64::MAX);
    let m = LabelMatcher::new(MatchType::Equal, "__name__", "sdw06_max").unwrap();
    let res = q.select(true, &[m]).unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].samples.len(), 1);
    assert_eq!(res[0].samples[0].t, i64::MAX);
    assert_eq!(res[0].samples[0].value, SampleValue::Float(3.0));

    // Also verify write_head_block_streaming does not create .tmp dirs when no series match [mint, maxt).
    let empty_dir = tempfile::tempdir().unwrap();
    let meta = write_head_block_streaming(
        empty_dir.path(),
        0,
        7_200_000,
        vec![(1, Labels::from_sorted_pairs([("__name__", "none")]))],
        BlockMetaCompaction::default(),
        false,
        0,
        |_ref_id, _chks, _tombs| Ok(()),
    )
    .unwrap();
    assert!(meta.is_none());
    assert_eq!(fs::read_dir(empty_dir.path()).unwrap().count(), 0);

    db.close().unwrap();

    let ooo_dir = tempfile::tempdir().unwrap();
    let mut ooo_opts = TsdbOptions::default();
    ooo_opts.head.ooo_time_window_ms = i64::MAX;
    let ooo_db = Tsdb::open(ooo_dir.path(), ooo_opts).unwrap();
    {
        let mut app = ooo_db.appender();
        let p = [("__name__", "sdw06_ooo")];
        // Initialize head at 1_000_000_000_000; OOO window i64::MAX allows any t >= 1e12 - i64::MAX.
        let r = app.append_float(None, &p, 1_000_000_000_000, 10.0).unwrap();
        app.append_float(Some(r), &p, -8_000_000_000_000_000_000, 1.0).unwrap();
        app.append_float(Some(r), &p, 0, 2.0).unwrap();
        app.commit().unwrap();
    }
    let t1 = Instant::now();
    let ooo_ulids = ooo_db.compact_ooo_head().unwrap();
    assert!(
        t1.elapsed().as_secs() < 5,
        "compact_ooo_head took too long across extreme gap"
    );
    assert_eq!(ooo_ulids.len(), 2);
    ooo_db.close().unwrap();
}

#[test]
fn w20_sdw_07_wide_histogram_span_indices_do_not_overflow_i32() {
    // Two valid 1-bucket gauge histograms with span gap = 3_000_000_000 > i32::MAX.
    let h1 = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 0,
        zero_threshold: 0.001,
        count: 1,
        sum: 1.0,
        positive_spans: vec![HistogramSpan {
            offset: -1_500_000_000,
            length: 1,
        }],
        positive_buckets: vec![1],
        ..Histogram::default()
    };
    let h2 = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 0,
        zero_threshold: 0.001,
        count: 2,
        sum: 2.0,
        positive_spans: vec![HistogramSpan {
            offset: 1_500_000_000,
            length: 1,
        }],
        positive_buckets: vec![2],
        ..Histogram::default()
    };

    // Integer histogram chunk: must not overflow/panic; cuts a new chunk when merged span offset > i32::MAX.
    let mut hc = HistogramChunk::new();
    {
        let mut app = hc.appender().unwrap();
        assert!(matches!(
            app.append_histogram(None, 0, 1_000, &h1, false).unwrap(),
            AppendHistogramOutcome::Appended { .. }
        ));
        let outcome = app.append_histogram(None, 0, 2_000, &h2, false).unwrap();
        assert!(matches!(outcome, AppendHistogramOutcome::NewChunk(_)));
    }

    // Float histogram chunk: same non-panicking behavior.
    let fh1 = FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 0,
        zero_threshold: 0.001,
        positive_spans: h1.positive_spans.clone(),
        positive_buckets: vec![1.0],
        ..FloatHistogram::default()
    };
    let fh2 = FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 0,
        zero_threshold: 0.001,
        positive_spans: h2.positive_spans.clone(),
        positive_buckets: vec![2.0],
        ..FloatHistogram::default()
    };
    let mut fhc = FloatHistogramChunk::new();
    {
        let mut app = fhc.appender().unwrap();
        assert!(matches!(
            app.append_float_histogram(None, 0, 1_000, &fh1, false)
                .unwrap(),
            AppendFloatHistogramOutcome::Appended { .. }
        ));
        let outcome = app
            .append_float_histogram(None, 0, 2_000, &fh2, false)
            .unwrap();
        assert!(matches!(outcome, AppendFloatHistogramOutcome::NewChunk(_)));
    }

    // Head end-to-end: append both gauge histograms and query them back cleanly.
    let dir = tempfile::tempdir().unwrap();
    let head = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
    let mut app = head.appender();
    let r = app
        .append_histogram(None, &[("__name__", "sdw07_h")], 0, 1_000, h1)
        .unwrap();
    app.append_histogram(Some(r), &[("__name__", "sdw07_h")], 0, 2_000, h2)
        .unwrap();
    app.commit().unwrap();
    let (_, chks, _) = head.series_chunks(r, i64::MIN, i64::MAX).unwrap().unwrap();
    assert_eq!(chks.len(), 2);
    let mut seen = Vec::new();
    for cm in chks {
        for s in mimir_tsdb::compact::decode_chunk_samples(&cm.chunk.unwrap()).unwrap() {
            if let SampleValue::Histogram(h) = s.value {
                seen.push((s.t, h.positive_spans[0].offset));
            }
        }
    }
    assert_eq!(seen, vec![(1_000, -1_500_000_000), (2_000, 1_500_000_000)]);
    head.close().unwrap();
}
