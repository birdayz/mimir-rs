//! Wave 25 Track 1 regression tests for A2-F1 through A2-F6:
//! - A2-F1: Per-segment WlogReader replay for WAL & WBL + exclude newly cut empty writer segment
//! - A2-F2: Fail Head::open / Tsdb::open on missing WAL segment between checkpoint.N and first_seg
//! - A2-F3: Filter split ooo_head_chunk sub-chunks against [mint, maxt]
//! - A2-F4: Preserve metrics & last_sample when GC retains series due to OOO or recent exemplar
//! - A2-F5: Startup Head::gc does not truncate chunks_head and HeadChunksWriter keeps monotonic seq
//! - A2-F6: found_series_for_prior_segments, OOO QuietZeroNaN accounting, append_exemplar fallback

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Arc;

use mimir_tsdb::chunkenc::{stale_nan, Histogram, QUIET_ZERO_NAN_BITS};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::exemplar::Exemplar;
use mimir_tsdb::head::{Head, HeadError, HeadOptions};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::LazyStreamEvent;
use mimir_tsdb::record::{RecordEncoder, RefSample, RefSeries};
use mimir_tsdb::wlog::{
    checkpoint_dir, segment_name, segments_range, CompressionType, WlogWriter, PAGE_SIZE,
    RECORD_HEADER_SIZE, REC_FIRST,
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

fn build_rec_first_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
    out.push(REC_FIRST);
    out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    let crc = crc32c::crc32c(payload);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn make_hist(sum: f64) -> Histogram {
    Histogram {
        schema: 0,
        zero_threshold: 0.001,
        zero_count: 1,
        count: 4,
        sum,
        positive_spans: vec![mimir_tsdb::chunkenc::HistogramSpan {
            offset: 0,
            length: 2,
        }],
        positive_buckets: vec![1, 1],
        ..Histogram::default()
    }
}

fn collect_float_samples(head: &Head, pairs: &[(&str, &str)]) -> Vec<(i64, f64)> {
    let entry = head
        .get_entry_by_sorted_pairs(pairs)
        .expect("series must exist in head");
    let (_, chks, _) = head
        .series_chunks(entry.ref_id, i64::MIN, i64::MAX)
        .unwrap()
        .expect("series must have chunks");
    let mut out = Vec::new();
    for cm in chks {
        for s in decode_chunk_samples(&cm.chunk.unwrap()).unwrap() {
            if let SampleValue::Float(v) = s.value {
                out.push((s.t, v));
            }
        }
    }
    out.sort_by_key(|&(t, _)| t);
    out
}

#[test]
fn a2_f1_page_aligned_torn_rec_first_wal_and_wbl_restart_preserves_writes() {
    // (a) WAL: page-aligned torn REC_FIRST at end of segment 000000 followed by restart + repair
    // + new writes + second restart preserving all pre-crash and post-repair writes.
    for concurrency in [1usize, 4usize] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write valid series + sample in WAL 00000000, close cleanly (pads to PAGE_SIZE = 32 KiB).
        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let enc = RecordEncoder::new(false);
            let s_rec = enc.series(
                &[RefSeries {
                    ref_id: 1,
                    labels: Labels::from_pairs([("__name__", "pre_crash_m"), ("job", "wal")]),
                }],
                Vec::new(),
            );
            let samp_rec = enc.samples(
                &[RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 1_000,
                    v: 11.0,
                }],
                Vec::new(),
            );
            w.log(&[&s_rec, &samp_rec]).unwrap();
            w.close().unwrap();
        }

        // Append a full-page REC_FIRST fragment (no REC_LAST) so 00000000 ends at 2 * PAGE_SIZE.
        let seg0 = segment_name(&wal_dir, 0);
        assert_eq!(fs::metadata(&seg0).unwrap().len(), PAGE_SIZE as u64);
        {
            let payload = vec![0x5Au8; PAGE_SIZE - RECORD_HEADER_SIZE];
            let frame = build_rec_first_frame(&payload);
            assert_eq!(frame.len(), PAGE_SIZE);
            let mut f = OpenOptions::new().append(true).open(&seg0).unwrap();
            f.write_all(&frame).unwrap();
            f.sync_all().unwrap();
        }
        assert_eq!(fs::metadata(&seg0).unwrap().len(), (2 * PAGE_SIZE) as u64);

        // Restart 1: Head::open must detect torn REC_FIRST on segment 0 (NOT segment 1!),
        // repair segment 0, preserve pre_crash_m, and accept new writes in segment 1.
        let reg1 = Arc::new(prometheus::Registry::new());
        let opts1 = HeadOptions {
            wal_replay_concurrency: concurrency,
            registry: Some(Arc::clone(&reg1)),
            ..HeadOptions::default()
        };
        {
            let head = Head::open(dir.path(), opts1, i64::MIN).unwrap();
            assert_eq!(
                metric_value(&reg1, "prometheus_tsdb_wal_corruptions_total", &[]),
                1.0
            );
            assert_eq!(
                collect_float_samples(&head, &[("__name__", "pre_crash_m"), ("job", "wal")]),
                vec![(1_000, 11.0)]
            );

            let mut app = head.appender();
            app.append_float(
                None,
                &[("__name__", "post_repair_m"), ("job", "wal")],
                2_000,
                22.0,
            )
            .unwrap();
            app.commit().unwrap();
            head.close().unwrap();
        }

        // Restart 2: must replay cleanly (0 corruptions) and preserve BOTH pre_crash_m and post_repair_m.
        let reg2 = Arc::new(prometheus::Registry::new());
        let opts2 = HeadOptions {
            wal_replay_concurrency: concurrency,
            registry: Some(Arc::clone(&reg2)),
            ..HeadOptions::default()
        };
        let head2 = Head::open(dir.path(), opts2, i64::MIN).unwrap();
        assert_eq!(
            metric_value(&reg2, "prometheus_tsdb_wal_corruptions_total", &[]),
            0.0
        );
        assert_eq!(
            collect_float_samples(&head2, &[("__name__", "pre_crash_m"), ("job", "wal")]),
            vec![(1_000, 11.0)]
        );
        assert_eq!(
            collect_float_samples(&head2, &[("__name__", "post_repair_m"), ("job", "wal")]),
            vec![(2_000, 22.0)]
        );
        head2.close().unwrap();
    }

    // (a2) WBL: page-aligned torn REC_FIRST at end of WBL segment 000000 followed by restart + repair
    // + new OOO writes + second restart preserving all OOO writes.
    {
        let dir = tempfile::tempdir().unwrap();
        let ooo_opts = HeadOptions {
            ooo_time_window_ms: 100_000,
            ..HeadOptions::default()
        };
        let pairs = [("__name__", "wbl_torn_m"), ("job", "ooo")];
        {
            let head = Head::open(dir.path(), ooo_opts.clone(), i64::MIN).unwrap();
            let mut app = head.appender();
            app.append_float(None, &pairs, 50_000, 1.0).unwrap();
            app.append_float(None, &pairs, 10_000, 2.0).unwrap();
            app.commit().unwrap();
            head.close().unwrap();
        }

        let wbl_seg0 = segment_name(&dir.path().join("wbl"), 0);
        assert_eq!(fs::metadata(&wbl_seg0).unwrap().len(), PAGE_SIZE as u64);
        {
            let payload = vec![0x3Cu8; PAGE_SIZE - RECORD_HEADER_SIZE];
            let frame = build_rec_first_frame(&payload);
            let mut f = OpenOptions::new().append(true).open(&wbl_seg0).unwrap();
            f.write_all(&frame).unwrap();
            f.sync_all().unwrap();
        }

        let reg1 = Arc::new(prometheus::Registry::new());
        let mut opts1 = ooo_opts.clone();
        opts1.registry = Some(Arc::clone(&reg1));
        {
            let head = Head::open(dir.path(), opts1, i64::MIN).unwrap();
            assert_eq!(
                metric_value(&reg1, "prometheus_tsdb_wal_corruptions_total", &[]),
                1.0
            );
            assert_eq!(
                collect_float_samples(&head, &pairs),
                vec![(10_000, 2.0), (50_000, 1.0)]
            );
            let mut app = head.appender();
            app.append_float(None, &pairs, 20_000, 3.0).unwrap();
            app.commit().unwrap();
            head.close().unwrap();
        }

        let reg2 = Arc::new(prometheus::Registry::new());
        let mut opts2 = ooo_opts;
        opts2.registry = Some(Arc::clone(&reg2));
        let head2 = Head::open(dir.path(), opts2, i64::MIN).unwrap();
        assert_eq!(
            metric_value(&reg2, "prometheus_tsdb_wal_corruptions_total", &[]),
            0.0
        );
        assert_eq!(
            collect_float_samples(&head2, &pairs),
            vec![(10_000, 2.0), (20_000, 3.0), (50_000, 1.0)]
        );
        head2.close().unwrap();
    }
}

#[test]
fn a2_f1_mid_page_torn_rec_first_in_seg0_followed_by_seg1() {
    // Subcase A: complete REC_FIRST fragment ending mid-page in segment 000000 followed by
    // valid segment 000001. Per-segment reader finishes 000000 cleanly after zero-padding
    // (dropping the incomplete REC_FIRST) and reads 000001 cleanly without wiping 000001!
    for concurrency in [1usize, 4usize] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let enc = RecordEncoder::new(false);

        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let s0 = enc.series(
                &[RefSeries {
                    ref_id: 1,
                    labels: Labels::from_pairs([("__name__", "seg0_m")]),
                }],
                Vec::new(),
            );
            let d0 = enc.samples(
                &[RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 1_000,
                    v: 1.0,
                }],
                Vec::new(),
            );
            w.log(&[&s0, &d0]).unwrap();
            w.next_segment().unwrap();
            let s1 = enc.series(
                &[RefSeries {
                    ref_id: 2,
                    labels: Labels::from_pairs([("__name__", "seg1_m")]),
                }],
                Vec::new(),
            );
            let d1 = enc.samples(
                &[RefSample {
                    ref_id: 2,
                    st: 0,
                    t: 2_000,
                    v: 2.0,
                }],
                Vec::new(),
            );
            w.log(&[&s1, &d1]).unwrap();
            w.close().unwrap();
        }

        // Append a mid-page REC_FIRST fragment (100 bytes) to 00000000.
        {
            let seg0 = segment_name(&wal_dir, 0);
            let frame = build_rec_first_frame(&[0x77u8; 93]);
            let mut f = OpenOptions::new().append(true).open(&seg0).unwrap();
            f.write_all(&frame).unwrap();
            f.sync_all().unwrap();
        }

        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            wal_replay_concurrency: concurrency,
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        assert_eq!(
            metric_value(&reg, "prometheus_tsdb_wal_corruptions_total", &[]),
            0.0
        );
        assert_eq!(
            collect_float_samples(&head, &[("__name__", "seg0_m")]),
            vec![(1_000, 1.0)]
        );
        assert_eq!(
            collect_float_samples(&head, &[("__name__", "seg1_m")]),
            vec![(2_000, 2.0)]
        );
        head.close().unwrap();
    }

    // Subcase B: mid-page truncated REC_FIRST (truncated mid-payload, CRC mismatch) in segment
    // 000000 followed by segment 000001 -> must attribute corruption to segment 0 and repair
    // segment 0 (preserving seg0_m), NOT attribute corruption to segment 1.
    {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let enc = RecordEncoder::new(false);

        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            let s0 = enc.series(
                &[RefSeries {
                    ref_id: 1,
                    labels: Labels::from_pairs([("__name__", "seg0_m")]),
                }],
                Vec::new(),
            );
            let d0 = enc.samples(
                &[RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 1_000,
                    v: 1.0,
                }],
                Vec::new(),
            );
            w.log(&[&s0, &d0]).unwrap();
            w.next_segment().unwrap();
            let s1 = enc.series(
                &[RefSeries {
                    ref_id: 2,
                    labels: Labels::from_pairs([("__name__", "seg1_m")]),
                }],
                Vec::new(),
            );
            let d1 = enc.samples(
                &[RefSample {
                    ref_id: 2,
                    st: 0,
                    t: 2_000,
                    v: 2.0,
                }],
                Vec::new(),
            );
            w.log(&[&s1, &d1]).unwrap();
            w.close().unwrap();
        }

        // Append a mid-page truncated REC_FIRST (header declares 200 bytes, only 50 bytes written).
        {
            let seg0 = segment_name(&wal_dir, 0);
            let frame = build_rec_first_frame(&[0x88u8; 200]);
            let mut f = OpenOptions::new().append(true).open(&seg0).unwrap();
            f.write_all(&frame[..RECORD_HEADER_SIZE + 50]).unwrap();
            f.sync_all().unwrap();
        }

        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        assert_eq!(
            metric_value(&reg, "prometheus_tsdb_wal_corruptions_total", &[]),
            1.0
        );
        // Repaired segment 0 -> deleted segments > 0 and created clean active segment 1.
        assert_eq!(segments_range(&wal_dir).unwrap(), Some((0, 1)));
        assert_eq!(
            collect_float_samples(&head, &[("__name__", "seg0_m")]),
            vec![(1_000, 1.0)]
        );
        assert!(head
            .get_entry_by_sorted_pairs(&[("__name__", "seg1_m")])
            .is_none());

        // New writes after repair survive restart.
        let mut app = head.appender();
        app.append_float(None, &[("__name__", "after_seg0_repair")], 3_000, 3.0)
            .unwrap();
        app.commit().unwrap();
        head.close().unwrap();

        let head2 = Head::open(dir.path(), HeadOptions::default(), i64::MIN).unwrap();
        assert_eq!(
            collect_float_samples(&head2, &[("__name__", "seg0_m")]),
            vec![(1_000, 1.0)]
        );
        assert_eq!(
            collect_float_samples(&head2, &[("__name__", "after_seg0_repair")]),
            vec![(3_000, 3.0)]
        );
        head2.close().unwrap();
    }
}

#[test]
fn a2_f2_missing_wal_segment_between_checkpoint_and_first_seg_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let enc = RecordEncoder::new(false);

    // Create checkpoint.00000002 with series 1.
    let cp_dir = checkpoint_dir(&wal_dir, 2);
    {
        let mut cw = WlogWriter::open(&cp_dir, CompressionType::None).unwrap();
        let s_rec = enc.series(
            &[RefSeries {
                ref_id: 1,
                labels: Labels::from_pairs([("__name__", "cp_m")]),
            }],
            Vec::new(),
        );
        cw.log(&[&s_rec]).unwrap();
        cw.close().unwrap();
    }

    // Create WAL segments 00000000..=00000005, then delete 00000000..=00000003 so disk has
    // checkpoint.00000002 and WAL segments [00000004, 00000005] (missing 00000003!).
    {
        let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
        for i in 0..5usize {
            let samp = enc.samples(
                &[RefSample {
                    ref_id: 1,
                    st: 0,
                    t: 1_000 + i as i64 * 100,
                    v: i as f64,
                }],
                Vec::new(),
            );
            w.log(&[&samp]).unwrap();
            w.next_segment().unwrap();
        }
        w.close().unwrap();
        for i in 0..=3usize {
            fs::remove_file(segment_name(&wal_dir, i)).unwrap();
        }
    }

    // Both Head::open and Tsdb::open must fail hard with a non-corruption ENOENT error.
    let head_err = Head::open(dir.path(), HeadOptions::default(), i64::MIN)
        .err()
        .expect("Head::open must fail on missing WAL segment");
    assert!(
        matches!(head_err, HeadError::Repair(ref msg) if msg.contains("cannot handle error")),
        "expected hard non-repairable error on missing WAL segment 3, got {head_err:?}"
    );

    let tsdb_err = Tsdb::open(dir.path(), TsdbOptions::default())
        .err()
        .expect("Tsdb::open must fail on missing WAL segment");
    let msg = tsdb_err.to_string();
    assert!(
        msg.contains("cannot handle error"),
        "expected Tsdb::open to fail hard on missing WAL segment 3, got {msg}"
    );
}

#[test]
fn a2_f3_split_ooo_head_chunk_filters_subchunks_by_query_range() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = TsdbOptions::default();
    opts.head.ooo_time_window_ms = 100_000;
    opts.head.ooo_cap_max = 32;

    let db = Tsdb::open(dir.path(), opts).unwrap();
    let pairs = [("__name__", "mixed_ooo_split"), ("job", "ooo")];

    // In-order anchor at t=1_000, then OOO float at t=100 and OOO histogram at t=300
    // inside the same ooo_head_chunk [100, 300], which splits into [100, 100] and [300, 300].
    let ref_id = {
        let mut app = db.appender();
        let id = app.append_float(None, &pairs, 1_000, 10.0).unwrap();
        app.append_float(Some(id), &pairs, 100, 1.0).unwrap();
        app.append_histogram(Some(id), &pairs, 0, 300, make_hist(5.0))
            .unwrap();
        app.commit().unwrap();
        id
    };

    // 1. Head::series_chunks & series_has_chunks_in_range on [100, 150] vs gap [150, 250].
    let (_, chks_100_150, _) = db.head().series_chunks(ref_id, 100, 150).unwrap().unwrap();
    assert_eq!(chks_100_150.len(), 1, "only [100, 100] overlaps [100, 150]");
    assert_eq!(chks_100_150[0].min_time, 100);
    assert_eq!(chks_100_150[0].max_time, 100);

    assert!(
        db.head().series_chunks(ref_id, 150, 250).unwrap().is_none(),
        "gap [150, 250] must return no chunks"
    );
    assert!(db
        .head()
        .series_has_chunks_in_range(ref_id, 100, 150)
        .is_some());
    assert!(
        db.head()
            .series_has_chunks_in_range(ref_id, 150, 250)
            .is_none(),
        "gap [150, 250] must not claim series has chunks"
    );

    // 2. TsdbChunkQuerier::select & select_raw_labels & stream_prepared_chunks_lazy.
    let m = LabelMatcher::new(MatchType::Equal, "__name__", "mixed_ooo_split").unwrap();

    let cq_100_150 = db.chunk_querier(100, 150);
    let res_100_150 = cq_100_150.select(true, std::slice::from_ref(&m)).unwrap();
    assert_eq!(res_100_150.len(), 1);
    assert_eq!(res_100_150[0].chunks.len(), 1);
    assert_eq!(res_100_150[0].chunks[0].min_time, 100);
    assert_eq!(res_100_150[0].chunks[0].max_time, 100);

    let cq_gap = db.chunk_querier(150, 250);
    assert!(cq_gap.select(true, std::slice::from_ref(&m)).unwrap().is_empty());
    assert!(cq_gap
        .select_raw_labels(true, std::slice::from_ref(&m))
        .unwrap()
        .is_empty());

    let q_gap = db.querier(150, 250);
    assert!(q_gap
        .select_raw_labels(true, std::slice::from_ref(&m))
        .unwrap()
        .is_empty());

    let prepared_gap = cq_gap.prepare_select(std::slice::from_ref(&m)).unwrap();
    let mut lazy_series_count = 0usize;
    cq_gap
        .stream_prepared_chunks_lazy(
            prepared_gap,
            std::slice::from_ref(&m),
            true,
            None,
            |ev| {
                if matches!(ev, LazyStreamEvent::Series(_, _)) {
                    lazy_series_count += 1;
                }
                Ok(true)
            },
        )
        .unwrap();
    assert_eq!(lazy_series_count, 0);

    let prepared_100_150 = cq_100_150.prepare_select(std::slice::from_ref(&m)).unwrap();
    let mut streamed_chunks = Vec::new();
    cq_100_150
        .stream_prepared_chunks_lazy(prepared_100_150, &[m], true, None, |ev| {
            if let LazyStreamEvent::SeriesChunks(_, chks) = ev {
                streamed_chunks.extend(chks);
            }
            Ok(true)
        })
        .unwrap();
    assert_eq!(streamed_chunks.len(), 1);
    assert_eq!(streamed_chunks[0].min_time, 100);
    assert_eq!(streamed_chunks[0].max_time, 100);

    db.close().unwrap();
}

#[test]
fn a2_f4_gc_preserves_metrics_and_last_sample_when_retained_by_ooo_or_exemplar() {
    let dir = tempfile::tempdir().unwrap();
    let reg = Arc::new(prometheus::Registry::new());
    let opts = HeadOptions {
        ooo_time_window_ms: 100_000,
        enable_exemplar_storage: true,
        max_exemplars: 100,
        registry: Some(Arc::clone(&reg)),
        ..HeadOptions::default()
    };
    let head = Head::open(dir.path(), opts, i64::MIN).unwrap();

    let stale_ooo_pairs = [("__name__", "f4_stale_ooo")];
    let hist_ooo_pairs = [("__name__", "f4_hist_ooo")];
    let stale_ex_pairs = [("__name__", "f4_stale_ex")];

    {
        let mut app = head.appender();
        // Series 1: stale in-order at t=1_000 + OOO float at t=500.
        let r1 = app.append_float(None, &stale_ooo_pairs, 1_000, stale_nan()).unwrap();
        app.append_float(Some(r1), &stale_ooo_pairs, 500, 1.0).unwrap();
        // Series 2: native histogram (2 buckets) at t=1_000 + OOO float at t=500.
        let r2 = app
            .append_histogram(None, &hist_ooo_pairs, 0, 1_000, make_hist(4.0))
            .unwrap();
        app.append_float(Some(r2), &hist_ooo_pairs, 500, 2.0).unwrap();
        // Series 3: stale in-order at t=1_000 + recent exemplar at t=2_500.
        let r3 = app.append_float(None, &stale_ex_pairs, 1_000, stale_nan()).unwrap();
        app.append_exemplar(
            Some(r3),
            &stale_ex_pairs,
            Exemplar::new(Labels::from_pairs([("trace_id", "abc")]), 1.0, 2_500),
        )
        .unwrap();
        app.commit().unwrap();
    }

    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_stale_series", &[]), 2.0);
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_series", &[]),
        1.0
    );
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_buckets", &[]),
        2.0
    );

    // Truncate in-order chunks < 2_000: all 3 series have empty in-order chunks, but are
    // retained (Series 1 & 2 by OOO, Series 3 by exemplar at 2_500 >= 2_000).
    // Gauges MUST remain 2, 1, 2 (not dropped to 0!).
    head.truncate_memory(2_000).unwrap();
    assert_eq!(head.num_series(), 3);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_stale_series", &[]), 2.0);
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_series", &[]),
        1.0
    );
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_buckets", &[]),
        2.0
    );

    // Now truncate to 3_000: Series 3's exemplar expires (2_500 < 3_000) so Series 3 is deleted
    // (stale_series drops from 2 to 1), while Series 1 & 2 stay retained by OOO.
    head.truncate_memory(3_000).unwrap();
    assert_eq!(head.num_series(), 2);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_stale_series", &[]), 1.0);
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_series", &[]),
        1.0
    );
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_buckets", &[]),
        2.0
    );

    // Clear OOO data and truncate again -> Series 1 & 2 are now unlinked and gauges drop to 0.
    head.clear_ooo_data_before(i64::MAX).unwrap();
    head.truncate_memory(4_000).unwrap();
    assert_eq!(head.num_series(), 0);
    assert_eq!(metric_value(&reg, "prometheus_tsdb_head_stale_series", &[]), 0.0);
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_series", &[]),
        0.0
    );
    assert_eq!(
        metric_value(&reg, "prometheus_tsdb_head_native_histogram_buckets", &[]),
        0.0
    );

    head.close().unwrap();
}

#[test]
fn a2_f5_startup_gc_does_not_truncate_chunks_head_and_seq_stays_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let chunks_dir = dir.path().join("chunks_head");
    let opts = HeadOptions {
        chunk_range_ms: 10_000,
        samples_per_chunk: 5,
        ..HeadOptions::default()
    };
    let pairs = [("__name__", "f5_m"), ("job", "seq")];

    // 1. Write 2 chunks and mmap to chunks_head/000001.
    {
        let head = Head::open(dir.path(), opts.clone(), i64::MIN).unwrap();
        let mut r = None;
        for i in 0..12i64 {
            let mut app = head.appender();
            let id = app.append_float(r, &pairs, 1_000 + i * 100, i as f64).unwrap();
            r = Some(id);
            app.commit().unwrap();
        }
        assert!(head.mmap_head_chunks().unwrap() >= 2);
        assert!(chunks_dir.join("000001").exists());
        head.close().unwrap();
    }

    // 2. Reopen with min_valid_time = 10_000 (above all chunks in 000001, so load_mmapped_chunks
    // skips them and min_mmap_seq == usize::MAX). Startup Head::gc MUST NOT delete 000001!
    {
        let head = Head::open(dir.path(), opts.clone(), 10_000).unwrap();
        assert!(
            chunks_dir.join("000001").exists(),
            "startup Head::gc must not truncate chunks_head while chunk_writer is None"
        );

        // Append 1 sample at t=20_000 so head is initialized (while chunk_writer has no
        // active file yet), then runtime truncate_memory(50_000) deletes 000001.
        // HeadChunksWriter.seq must remain >= 1 so the next mmap cut writes 000002!
        {
            let mut app = head.appender();
            app.append_float(None, &[("__name__", "f5_init"), ("job", "seq")], 20_000, 1.0)
                .unwrap();
            app.commit().unwrap();
        }
        head.truncate_memory(50_000).unwrap();
        assert!(
            !chunks_dir.join("000001").exists(),
            "runtime truncate_memory should unlink obsolete 000001"
        );

        let mut r = None;
        for i in 0..12i64 {
            let mut app = head.appender();
            let id = app
                .append_float(r, &pairs, 60_000 + i * 100, i as f64)
                .unwrap();
            r = Some(id);
            app.commit().unwrap();
        }
        assert!(head.mmap_head_chunks().unwrap() >= 2);
        assert!(
            !chunks_dir.join("000001").exists(),
            "sequence must not reset back to 000001 after full truncation"
        );
        assert!(
            chunks_dir.join("000002").exists(),
            "next chunk file must use monotonic sequence 000002"
        );
        head.close().unwrap();
    }
}

#[test]
fn a2_f6_unknown_refs_prior_segments_quiet_zero_nan_ooo_and_exemplar_fallback() {
    // 1. Cross-segment and checkpoint->WAL found_series_for_prior_segments metric.
    for concurrency in [1usize, 4usize] {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let enc = RecordEncoder::new(false);

        // Checkpoint 00000000 references missing ref_id=99.
        {
            let mut cw =
                WlogWriter::open(checkpoint_dir(&wal_dir, 0), CompressionType::None).unwrap();
            let samp = enc.samples(
                &[RefSample {
                    ref_id: 99,
                    st: 0,
                    t: 1_000,
                    v: 1.0,
                }],
                Vec::new(),
            );
            cw.log(&[&samp]).unwrap();
            cw.close().unwrap();
        }

        // WAL 00000001 references missing ref_id=42 and ref_id=777 (never defined).
        // WAL 00000002 defines ref_id=99 and ref_id=42 (with no unknown refs of its own!).
        {
            let mut w = WlogWriter::open(&wal_dir, CompressionType::None).unwrap();
            w.next_segment().unwrap(); // 00000001
            let samp1 = enc.samples(
                &[
                    RefSample {
                        ref_id: 42,
                        st: 0,
                        t: 2_000,
                        v: 2.0,
                    },
                    RefSample {
                        ref_id: 777,
                        st: 0,
                        t: 2_500,
                        v: 3.0,
                    },
                ],
                Vec::new(),
            );
            w.log(&[&samp1]).unwrap();
            w.next_segment().unwrap(); // 00000002
            let s2 = enc.series(
                &[
                    RefSeries {
                        ref_id: 99,
                        labels: Labels::from_pairs([("__name__", "prior_cp_m")]),
                    },
                    RefSeries {
                        ref_id: 42,
                        labels: Labels::from_pairs([("__name__", "prior_wal_m")]),
                    },
                ],
                Vec::new(),
            );
            w.log(&[&s2]).unwrap();
            w.close().unwrap();
        }

        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            wal_replay_concurrency: concurrency,
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        assert_eq!(
            metric_value(
                &reg,
                "prometheus_tsdb_wal_replay_unknown_refs_total",
                &[("type", "found_series_for_prior_segments")],
            ),
            2.0
        );
        assert_eq!(
            metric_value(
                &reg,
                "prometheus_tsdb_wal_replay_unknown_refs_total",
                &[("type", "truly_missing_series")],
            ),
            3.0 // 99 in ckpt + 42, 777 in seg 1
        );
        head.close().unwrap();
    }

    // 2. OOO QuietZeroNaN float samples count in samples_appended_total, not duplicate_samples_total.
    {
        let dir = tempfile::tempdir().unwrap();
        let reg = Arc::new(prometheus::Registry::new());
        let opts = HeadOptions {
            ooo_time_window_ms: 100_000,
            registry: Some(Arc::clone(&reg)),
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "quiet_zero_ooo")];

        {
            let mut app = head.appender();
            let id = app.append_float(None, &pairs, 10_000, 1.0).unwrap();
            app.commit().unwrap();

            let mut app2 = head.appender();
            app2.append_float(Some(id), &pairs, 5_000, f64::from_bits(QUIET_ZERO_NAN_BITS))
                .unwrap();
            app2.commit().unwrap();
        }

        assert_eq!(
            metric_value(
                &reg,
                "prometheus_tsdb_head_samples_appended_total",
                &[("type", "float")],
            ),
            2.0
        );
        assert_eq!(
            metric_value(
                &reg,
                "prometheus_tsdb_duplicate_samples_total",
                &[("type", "float")],
            ),
            0.0
        );
        assert_eq!(
            metric_value(
                &reg,
                "prometheus_tsdb_head_out_of_order_samples_appended_total",
                &[("type", "float")],
            ),
            0.0
        );
        assert_eq!(collect_float_samples(&head, &pairs), vec![(10_000, 1.0)]);
        head.close().unwrap();
    }

    // 3. HeadAppender::append_exemplar falls back to pairs lookup when series_ref is stale.
    {
        let dir = tempfile::tempdir().unwrap();
        let opts = HeadOptions {
            enable_exemplar_storage: true,
            max_exemplars: 100,
            ..HeadOptions::default()
        };
        let head = Head::open(dir.path(), opts, i64::MIN).unwrap();
        let pairs = [("__name__", "ex_fallback_m"), ("job", "api")];

        let real_ref = {
            let mut app = head.appender();
            let id = app.append_float(None, &pairs, 1_000, 1.0).unwrap();
            app.commit().unwrap();
            id
        };

        {
            let mut app = head.appender();
            let ex = Exemplar::new(Labels::from_pairs([("trace_id", "t1")]), 2.0, 2_000);
            let resolved = app.append_exemplar(Some(999_999), &pairs, ex).unwrap();
            assert_eq!(resolved, real_ref);

            // Unknown ref + unknown pairs must still error with UnknownSeriesRef(999_999).
            let err = app
                .append_exemplar(
                    Some(999_999),
                    &[("__name__", "missing_series")],
                    Exemplar::new(Labels::from_pairs([("trace_id", "t2")]), 3.0, 2_100),
                )
                .unwrap_err();
            assert!(matches!(err, HeadError::UnknownSeriesRef(999_999)));
            app.commit().unwrap();
        }

        let m = LabelMatcher::new(MatchType::Equal, "__name__", "ex_fallback_m").unwrap();
        let qres = head.exemplars().select(0, 5_000, &[&[m]]);
        assert_eq!(qres.len(), 1);
        assert_eq!(qres[0].exemplars.len(), 1);
        assert_eq!(qres[0].exemplars[0].value, 2.0);
        head.close().unwrap();
    }
}
