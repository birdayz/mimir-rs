//! Wave 21 regression tests for mimir-tsdb:
//! - W21-DF-02 (tsdb helper): Block::get_or_init_lookup_planner OnceLock slot
//! - W21-DF-04: DB-backed appender commit / commit_batch compact_pending wakeup
//! - W21-DF-05: 128 KiB segment preallocation in HeadChunksWriter::cut_new_file
//! - W21-DF-06: WBL MmapMarkers removal, commit_batch WAL/WBL merging, deferred float-staleness
//! - W21-DF-07: Native histogram BucketIterator::next_idx & int32(offset) recode parity
//! - W21-DF-10: PostingsForMatchersCache silent invalidation on WAL replay and Head GC

use std::any::Any;
use std::fs;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_tsdb::chunkenc::float_histogram::AppendFloatHistogramOutcome;
use mimir_tsdb::chunkenc::float_histogram_st::AppendFloatHistogramStOutcome;
use mimir_tsdb::chunkenc::histogram::AppendHistogramOutcome;
use mimir_tsdb::chunkenc::histogram_st::AppendHistogramStOutcome;
use mimir_tsdb::chunkenc::{
    stale_nan, CounterResetHint, EncodedChunk, FloatHistogram, FloatHistogramChunk,
    FloatHistogramStChunk, Histogram, HistogramChunk, HistogramSpan, HistogramStChunk, XorChunk,
};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::{Head, HeadAppender, HeadOptions};
#[cfg(not(unix))]
use mimir_tsdb::head_chunks::HEAD_CHUNK_FILE_HEADER_SIZE;
#[cfg(unix)]
use mimir_tsdb::head_chunks::HEAD_CHUNK_FILE_PREALLOCATION_SIZE;
use mimir_tsdb::head_chunks::{
    head_chunk_file_path, HeadChunksError, HeadChunksReader, HeadChunksWriter, WriteFaultAction,
};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::postings_cache::{PostingsForMatchersCacheConfig, PostingsForMatchersCacheMetrics};
use mimir_tsdb::record::{DecodedRecord, RecordDecoder};
use mimir_tsdb::wlog::{CompressionType, WlogReader};
use prometheus::Registry;

fn counter_value(reg: &Registry, name: &str) -> f64 {
    reg.gather()
        .into_iter()
        .find(|mf| mf.get_name() == name)
        .and_then(|mf| mf.get_metric().first().cloned())
        .map_or(0.0, |m| m.get_counter().get_value())
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

#[test]
fn w21_df_02_block_get_or_init_lookup_planner_caches_once() {
    #[derive(Debug, PartialEq)]
    struct DummyPlanner {
        id: usize,
    }

    let dir = tempfile::tempdir().unwrap();
    let db = Tsdb::open_with_reload_interval(
        dir.path(),
        TsdbOptions::default(),
        Duration::ZERO,
    )
    .unwrap();
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "up"), ("job", "api")], 1_000, 1.0)
            .unwrap();
        app.append_float(None, &[("__name__", "up"), ("job", "api")], 2_000, 2.0)
            .unwrap();
        app.commit().unwrap();
    }
    let _ulid = db.compact_head_all().unwrap().expect("block compacted");
    let blocks = db.blocks();
    assert_eq!(blocks.len(), 1);
    let blk = Arc::clone(&blocks[0]);

    let init_calls = Arc::new(AtomicUsize::new(0));
    let arcs: Vec<Arc<dyn Any + Send + Sync>> = std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..4 {
            let b = Arc::clone(&blk);
            let calls = Arc::clone(&init_calls);
            handles.push(s.spawn(move || {
                let slot = b.get_or_init_lookup_planner(|| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Arc::new(DummyPlanner { id: 42 })
                });
                let downcast = slot
                    .downcast_ref::<DummyPlanner>()
                    .expect("expected DummyPlanner");
                assert_eq!(downcast, &DummyPlanner { id: 42 });
                Arc::clone(slot)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    assert_eq!(init_calls.load(Ordering::SeqCst), 1);
    for a in &arcs[1..] {
        assert!(Arc::ptr_eq(a, &arcs[0]));
    }
    db.close().unwrap();
}

#[test]
fn w21_df_04_db_appender_commit_and_commit_batch_wake_compact_pending() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(Registry::new());
    let opts = TsdbOptions {
        head: HeadOptions {
            registry: Some(Arc::clone(&reg)),
            chunk_range_ms: 7_200_000, // 2h -> compactable once maxt - mint > 3h (10_800_000 ms)
            ..HeadOptions::default()
        },
        ..TsdbOptions::default()
    };
    // 60s reload interval ensures the 1m tick never fires during the test.
    let db = Tsdb::open_with_reload_interval(dir.path(), opts, Duration::from_secs(60)).unwrap();

    // Commit 1: single sample (not compactable) -> does not trigger compact_pending.
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "df04_m")], 0, 1.0)
            .unwrap();
        app.commit().unwrap();
    }
    assert!(!db.head().is_compactable());
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(
        counter_value(&reg, "prometheus_tsdb_compactions_triggered_total"),
        0.0
    );
    assert_eq!(
        counter_value(&reg, "prometheus_tsdb_compactions_skipped_total"),
        0.0
    );

    // Commit 2: span > 3h -> head.is_compactable() == true -> wakes tsdb-reload immediately.
    {
        let mut app = db.appender();
        app.append_float(None, &[("__name__", "df04_m")], 10_800_001, 2.0)
            .unwrap();
        app.commit().unwrap();
    }
    assert!(db.head().is_compactable());

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if counter_value(&reg, "prometheus_tsdb_compactions_triggered_total") >= 1.0
            && counter_value(&reg, "prometheus_tsdb_compactions_skipped_total") >= 1.0
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let triggered_after_single = counter_value(&reg, "prometheus_tsdb_compactions_triggered_total");
    let skipped_after_single = counter_value(&reg, "prometheus_tsdb_compactions_skipped_total");
    assert!(triggered_after_single >= 1.0);
    assert_eq!(skipped_after_single, triggered_after_single);

    // Commit 3: commit_batch with DB-backed appenders also wakes compact_pending.
    {
        let mut a1 = db.appender();
        let mut a2 = db.appender();
        a1.append_float(None, &[("__name__", "df04_m1")], 10_801_000, 3.0)
            .unwrap();
        a2.append_float(None, &[("__name__", "df04_m2")], 10_802_000, 4.0)
            .unwrap();
        HeadAppender::commit_batch(vec![a1, a2]).unwrap();
    }
    let deadline2 = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline2 {
        if counter_value(&reg, "prometheus_tsdb_compactions_triggered_total")
            > triggered_after_single
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let triggered_after_batch = counter_value(&reg, "prometheus_tsdb_compactions_triggered_total");
    let skipped_after_batch = counter_value(&reg, "prometheus_tsdb_compactions_skipped_total");
    assert!(triggered_after_batch > triggered_after_single);
    assert_eq!(skipped_after_batch, triggered_after_batch);

    db.close().unwrap();
}

#[test]
fn w21_df_05_head_chunks_writer_preallocates_128kib_on_unix() {
    let mut xc = XorChunk::new();
    {
        let mut app = xc.appender().unwrap();
        app.append(0, 1_000, 1.0).unwrap();
        app.append(0, 2_000, 2.0).unwrap();
    }
    let encoded = EncodedChunk::Xor(xc);

    // 1. Default max_file_size -> preallocates to 131_072 B on Unix.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = HeadChunksWriter::open(dir.path()).unwrap();
        let r = w.write_chunk(1, 1_000, 2_000, &encoded, false).unwrap();
        w.close().unwrap();

        #[cfg(unix)]
        assert_eq!(
            fs::metadata(head_chunk_file_path(dir.path(), 1))
                .unwrap()
                .len(),
            HEAD_CHUNK_FILE_PREALLOCATION_SIZE as u64
        );
        #[cfg(not(unix))]
        assert!(
            fs::metadata(head_chunk_file_path(dir.path(), 1))
                .unwrap()
                .len()
                >= HEAD_CHUNK_FILE_HEADER_SIZE as u64
        );

        let reader = HeadChunksReader::open(dir.path()).unwrap();
        let all = reader.collect_all_chunks().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(reader.chunk(r).unwrap(), encoded);
    }

    // 2. Clamped to max_file_size when custom small max_file_size < 131_072 is used.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = HeadChunksWriter::with_max_file_size(dir.path(), 4096).unwrap();
        w.cut_new_file().unwrap();
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(head_chunk_file_path(dir.path(), 1))
                .unwrap()
                .len(),
            4096
        );
        w.close().unwrap();
    }

    // 3. ENOSPC rollback preserves max(saved_pos, prealloc_len) AND zeroes torn bytes so
    // HeadChunksReader::open cleanly sees 0 chunks and 0 corruptions.
    {
        let dir = tempfile::tempdir().unwrap();
        let mut w = HeadChunksWriter::with_options(dir.path(), 1024 * 1024, 512).unwrap();
        w.write_chunk(1, 1_000, 2_000, &encoded, false).unwrap();
        w.set_write_fault_hook(Some(Arc::new(|_| WriteFaultAction::PartialThenError {
            prefix_len: 19,
            err_kind: ErrorKind::StorageFull,
        })));
        let err = w.flush().unwrap_err();
        assert!(matches!(err, HeadChunksError::Io(_)));
        w.set_write_fault_hook(None);
        w.close().unwrap();

        #[cfg(unix)]
        assert_eq!(
            fs::metadata(head_chunk_file_path(dir.path(), 1))
                .unwrap()
                .len(),
            HEAD_CHUNK_FILE_PREALLOCATION_SIZE as u64
        );
        #[cfg(not(unix))]
        assert_eq!(
            fs::metadata(head_chunk_file_path(dir.path(), 1))
                .unwrap()
                .len(),
            HEAD_CHUNK_FILE_HEADER_SIZE as u64
        );
        let reader = HeadChunksReader::open(dir.path()).unwrap();
        assert!(reader.collect_all_chunks().unwrap().is_empty());
    }
}

#[test]
fn w21_df_06_wbl_no_mmap_markers_on_compaction_merged_commit_batch_and_deferred_staleness() {
    // Sub-test A: snapshot_ooo_for_compaction & collect_ooo_compaction_series never write MmapMarkers
    for use_collect_ooo in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            wal_compression: CompressionType::None,
            ooo_time_window_ms: 3_600_000,
            ooo_cap_max: 32,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        {
            let mut app = head.appender();
            let r = app
                .append_float(None, &[("__name__", "df06_a")], 1_000_000, 1.0)
                .unwrap();
            app.append_float(Some(r), &[("__name__", "df06_a")], 500_000, 2.0)
                .unwrap();
            app.commit().unwrap();
        }
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "df06_a")], 501_000, 3.0)
                .unwrap();
            app.commit().unwrap();
        }

        let wbl_before = read_all_wal_records(&dir.path().join("wbl"));
        let markers_before = wbl_before
            .iter()
            .filter(|r| matches!(r, DecodedRecord::MmapMarkers(_)))
            .count();

        if use_collect_ooo {
            let (series, _, _) = head.collect_ooo_compaction_series().unwrap();
            assert_eq!(series.len(), 1);
        } else {
            let snap = head.collect_ooo_compaction_candidates_with_watermark().unwrap();
            assert_eq!(snap.candidates.len(), 1);
        }

        let wbl_after = read_all_wal_records(&dir.path().join("wbl"));
        let markers_after = wbl_after
            .iter()
            .filter(|r| matches!(r, DecodedRecord::MmapMarkers(_)))
            .count();
        assert_eq!(
            markers_after, markers_before,
            "snapshot_ooo_for_compaction / collect_ooo_compaction_series must not log MmapMarkers to WBL"
        );
        head.close().unwrap();
    }

    // Sub-test B: HeadAppender::commit_batch merges WAL Samples and WBL Samples when same_head
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            wal_compression: CompressionType::None,
            ooo_time_window_ms: 3_600_000,
            ooo_cap_max: 32,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        // Seed 4 series with in-order anchor and initial OOO sample (creating initial OOO chunks).
        let mut refs = Vec::new();
        {
            let mut app = head.appender();
            for i in 0..4usize {
                let s = format!("s{i}");
                let r = app
                    .append_float(None, &[("__name__", "df06_b"), ("s", &s)], 1_000_000, 1.0)
                    .unwrap();
                app.append_float(Some(r), &[("__name__", "df06_b"), ("s", &s)], 500_000, 2.0)
                    .unwrap();
                refs.push(r);
            }
            app.commit().unwrap();
        }

        let wal_before = read_all_wal_records(&dir.path().join("wal"));
        let wbl_before = read_all_wal_records(&dir.path().join("wbl"));

        // Now commit_batch across 4 separate HeadAppenders (1 per shard), each writing 1 in-order + 1 OOO sample.
        let mut apps = Vec::new();
        for (i, &r) in refs.iter().enumerate() {
            let mut app = head.appender();
            let s = format!("s{i}");
            app.append_float(Some(r), &[("__name__", "df06_b"), ("s", &s)], 1_001_000, 10.0 + i as f64)
                .unwrap();
            app.append_float(Some(r), &[("__name__", "df06_b"), ("s", &s)], 501_000, 20.0 + i as f64)
                .unwrap();
            apps.push(app);
        }
        HeadAppender::commit_batch(apps).unwrap();

        let wal_after = read_all_wal_records(&dir.path().join("wal"));
        let wbl_after = read_all_wal_records(&dir.path().join("wbl"));

        let new_wal_samples: Vec<_> = wal_after[wal_before.len()..]
            .iter()
            .filter_map(|r| match r {
                DecodedRecord::Samples(v) => Some(v.len()),
                _ => None,
            })
            .collect();
        assert_eq!(
            new_wal_samples,
            vec![8],
            "commit_batch must merge all shards into 1 WAL Samples record"
        );

        let new_wbl_samples: Vec<_> = wbl_after[wbl_before.len()..]
            .iter()
            .filter_map(|r| match r {
                DecodedRecord::Samples(v) => Some(v.len()),
                _ => None,
            })
            .collect();
        assert_eq!(
            new_wbl_samples,
            vec![4],
            "commit_batch must merge all shards into 1 WBL Samples record"
        );

        head.close().unwrap();
    }

    // Sub-test C: float staleness on histogram / float-histogram series is deferred after f_start..f_end
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            wal_compression: CompressionType::None,
            ooo_time_window_ms: 3_600_000,
            ooo_cap_max: 32,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

        let h = Histogram {
            schema: 0,
            zero_threshold: 0.001,
            count: 1,
            sum: 1.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1],
            ..Histogram::default()
        };
        let fh = FloatHistogram {
            schema: 0,
            zero_threshold: 0.001,
            count: 1.0,
            sum: 1.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![1.0],
            ..FloatHistogram::default()
        };

        let (r_h, r_fh, r_f);
        {
            let mut app = head.appender();
            r_h = app
                .append_histogram(None, &[("__name__", "df06_h")], 0, 1_000_000, h)
                .unwrap();
            r_fh = app
                .append_float_histogram(None, &[("__name__", "df06_fh")], 0, 1_000_000, fh)
                .unwrap();
            r_f = app
                .append_float(None, &[("__name__", "df06_f")], 1_000_000, 1.0)
                .unwrap();
            app.commit().unwrap();
        }

        let wbl_before = read_all_wal_records(&dir.path().join("wbl")).len();
        {
            // Push OOO float staleness onto r_h and r_fh BEFORE OOO regular float onto r_f.
            let mut app = head.appender();
            app.append_float(Some(r_h), &[("__name__", "df06_h")], 500_000, stale_nan())
                .unwrap();
            app.append_float(Some(r_fh), &[("__name__", "df06_fh")], 500_000, stale_nan())
                .unwrap();
            app.append_float(Some(r_f), &[("__name__", "df06_f")], 500_000, 42.0)
                .unwrap();
            app.commit().unwrap();
        }

        let wbl_new = &read_all_wal_records(&dir.path().join("wbl"))[wbl_before..];
        let pos_samples = wbl_new
            .iter()
            .position(|r| matches!(r, DecodedRecord::Samples(_)))
            .expect("expected WBL Samples record for r_f");
        let pos_hist = wbl_new
            .iter()
            .position(|r| matches!(r, DecodedRecord::HistogramSamples(_)))
            .expect("expected WBL HistogramSamples record for deferred stale r_h");
        let pos_fhist = wbl_new
            .iter()
            .position(|r| matches!(r, DecodedRecord::FloatHistogramSamples(_)))
            .expect("expected WBL FloatHistogramSamples record for deferred stale r_fh");

        assert!(
            pos_samples < pos_hist && pos_hist < pos_fhist,
            "deferred histogram staleness must be logged after WBL Samples: got {wbl_new:?}"
        );

        head.close().unwrap();
    }
}

#[test]
fn w21_df_07_histogram_bucket_iterator_wrapping_and_span_recode_parity() {
    // 1. Valid 2-span histogram with span[1].offset == i32::MAX (Go int(b.spans[b.span].Offset + 1) wrap).
    let h_max_span = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 0,
        zero_threshold: 0.001,
        count: 3,
        sum: 3.0,
        positive_spans: vec![
            HistogramSpan {
                offset: -2_147_483_647,
                length: 1,
            },
            HistogramSpan {
                offset: i32::MAX,
                length: 1,
            },
        ],
        positive_buckets: vec![1, 2],
        ..Histogram::default()
    };
    let h_extra = Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 0,
        zero_threshold: 0.001,
        count: 5,
        sum: 5.0,
        positive_spans: vec![
            HistogramSpan {
                offset: -2_147_483_647,
                length: 1,
            },
            HistogramSpan {
                offset: i32::MAX,
                length: 1,
            },
        ],
        positive_buckets: vec![2, 3],
        ..Histogram::default()
    };

    let mut hc_wrap = HistogramChunk::new();
    {
        let mut app = hc_wrap.appender().unwrap();
        assert!(matches!(
            app.append_histogram(None, 0, 1_000, &h_max_span, false)
                .unwrap(),
            AppendHistogramOutcome::Appended { .. }
        ));
        // Appending h_extra triggers expand_spans_both_ways -> BucketIterator::next_idx across span[1].offset == i32::MAX.
        // Because Go wraps int32(i32::MAX + 1) -> i32::MIN, merged offset stays exact and appends in-place cleanly!
        let outcome = app
            .append_histogram(None, 0, 2_000, &h_extra, false)
            .unwrap();
        assert!(matches!(
            outcome,
            AppendHistogramOutcome::Appended { recoded: false, .. }
        ));
    }

    // 2. Counter histograms with backward insert across span gap > i32::MAX exercise BOTH
    // expand_spans_both_ways AND adjust_for_inserts across all 4 histogram chunk types!
    let ch1 = Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 0.001,
        count: 0,
        sum: 0.0,
        positive_spans: vec![HistogramSpan {
            offset: 1_500_000_000,
            length: 1,
        }],
        positive_buckets: vec![0],
        ..Histogram::default()
    };
    let ch2 = Histogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 0.001,
        count: 2,
        sum: 2.0,
        positive_spans: vec![HistogramSpan {
            offset: -1_500_000_000,
            length: 1,
        }],
        positive_buckets: vec![2],
        ..Histogram::default()
    };
    let mut hc = HistogramChunk::new();
    {
        let mut app = hc.appender().unwrap();
        assert!(matches!(
            app.append_histogram(None, 0, 1_000, &ch1, false).unwrap(),
            AppendHistogramOutcome::Appended { .. }
        ));
        let out = app.append_histogram(None, 0, 2_000, &ch2, false).unwrap();
        assert!(matches!(
            out,
            AppendHistogramOutcome::Appended { recoded: true, .. }
        ));
    }
    assert_eq!(hc.num_samples(), 2);

    let mut hc_st = HistogramStChunk::new();
    {
        let mut app = hc_st.appender().unwrap();
        assert!(matches!(
            app.append_histogram(None, 0, 1_000, &ch1, false).unwrap(),
            AppendHistogramStOutcome::Appended { .. }
        ));
        let out = app.append_histogram(None, 0, 2_000, &ch2, false).unwrap();
        assert!(matches!(
            out,
            AppendHistogramStOutcome::Appended { recoded: true, .. }
        ));
    }
    assert_eq!(hc_st.num_samples(), 2);

    let fch1 = FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 0.001,
        count: 0.0,
        sum: 0.0,
        positive_spans: ch1.positive_spans.clone(),
        positive_buckets: vec![0.0],
        ..FloatHistogram::default()
    };
    let fch2 = FloatHistogram {
        counter_reset_hint: CounterResetHint::UnknownCounterReset,
        schema: 0,
        zero_threshold: 0.001,
        count: 2.0,
        sum: 2.0,
        positive_spans: ch2.positive_spans.clone(),
        positive_buckets: vec![2.0],
        ..FloatHistogram::default()
    };

    let mut fhc = FloatHistogramChunk::new();
    {
        let mut app = fhc.appender().unwrap();
        assert!(matches!(
            app.append_float_histogram(None, 0, 1_000, &fch1, false)
                .unwrap(),
            AppendFloatHistogramOutcome::Appended { .. }
        ));
        let out = app
            .append_float_histogram(None, 0, 2_000, &fch2, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramOutcome::Appended { recoded: true, .. }
        ));
    }
    assert_eq!(fhc.num_samples(), 2);

    let mut fhc_st = FloatHistogramStChunk::new();
    {
        let mut app = fhc_st.appender().unwrap();
        assert!(matches!(
            app.append_float_histogram(None, 0, 1_000, &fch1, false)
                .unwrap(),
            AppendFloatHistogramStOutcome::Appended { .. }
        ));
        let out = app
            .append_float_histogram(None, 0, 2_000, &fch2, false)
            .unwrap();
        assert!(matches!(
            out,
            AppendFloatHistogramStOutcome::Appended { recoded: true, .. }
        ));
    }
    assert_eq!(fhc_st.num_samples(), 2);

    let decoded = decode_chunk_samples(&EncodedChunk::Histogram(hc)).unwrap();
    assert_eq!(decoded.len(), 2);
    if let SampleValue::Histogram(ref h2_dec) = decoded[1].value {
        assert_eq!(h2_dec.counter_reset_hint, CounterResetHint::NotCounterReset);
    } else {
        panic!("expected histogram sample");
    }
}

#[test]
fn w21_df_10_postings_cache_silent_invalidation_on_wal_replay_and_head_gc() {
    let dir = tempfile::tempdir().unwrap();
    let make_opts = |reg: &Arc<Registry>| HeadOptions {
        registry: Some(Arc::clone(reg)),
        chunk_range_ms: 1_000,
        postings_cache: PostingsForMatchersCacheConfig {
            enable_invalidation: true,
            report_invalidations: true,
            num_metric_versions: 1024,
            metrics: Some(PostingsForMatchersCacheMetrics::new(
                "w21_df10_",
                Some(reg),
            )),
            ..Default::default()
        },
        ..HeadOptions::default()
    };

    let reg1 = Arc::new(Registry::new());
    let head = Head::open(dir.path(), make_opts(&reg1), i64::MIN).unwrap();
    head.set_postings_cache_eligibility(true, true);

    {
        let mut app = head.appender();
        for i in 0..5usize {
            let s = format!("old_{i}");
            app.append_float(None, &[("__name__", "m_old"), ("s", &s)], 100, 1.0)
                .unwrap();
        }
        for i in 0..5usize {
            let s = format!("new_{i}");
            app.append_float(None, &[("__name__", "m_new"), ("s", &s)], 5_000, 2.0)
                .unwrap();
        }
        app.commit().unwrap();
    }
    assert_eq!(
        counter_value(&reg1, "w21_df10_postings_for_matchers_cache_invalidations_total"),
        10.0
    );

    let m_old = [LabelMatcher::new(MatchType::Equal, "__name__", "m_old").unwrap()];
    assert_eq!(
        head.postings_for_matchers_with_concurrent(&m_old, true)
            .unwrap()
            .len(),
        5
    );

    // Head GC deleting <= 64 series (5 series in m_old): must preserve cache correctness
    // WITHOUT incrementing invalidations_total.
    head.truncate_memory(2_000).unwrap();
    assert_eq!(
        head.postings_for_matchers_with_concurrent(&m_old, true)
            .unwrap()
            .len(),
        0,
        "deleted series must not remain in cached postings"
    );
    assert_eq!(
        counter_value(&reg1, "w21_df10_postings_for_matchers_cache_invalidations_total"),
        10.0,
        "Head GC deletions must not increment invalidations_total"
    );
    head.close().unwrap();

    // WAL replay on reopen: must NOT increment invalidations_total.
    let reg2 = Arc::new(Registry::new());
    let reopened = Head::open(dir.path(), make_opts(&reg2), i64::MIN).unwrap();
    assert_eq!(
        counter_value(&reg2, "w21_df10_postings_for_matchers_cache_invalidations_total"),
        0.0,
        "WAL replay must not increment invalidations_total"
    );
    reopened.close().unwrap();
}
