//! Wave 22 regression tests for W22-DEEP-02 (TSDB compaction-vs-query isolation):
//! 1) `last_garbage_collected_mmap_ref` prevents duplicate OOO chunks while `truncate_ooo` waits.
//! 2) Early `last_memory_truncation_time` / `mem_truncation_in_process` prevents duplicate
//!    in-order chunks on `UnorderedChunkQuerier` (`ooo_window > 0`) while `truncate_memory` waits.
//! 3) Cutoff-bounded `wait_for_range` + `evicting_series` (`skipEvictedSeries`) prevents
//!    livelock and duplicate chunks during `compact_selected_series`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_tsdb::chunkenc::Encoding;
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::db::{Tsdb, TsdbOptions};
use mimir_tsdb::head::HeadOptions;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::querier::QueryChunkSeries;

fn wait_until(label: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn decode_raw_float_samples(series: &[QueryChunkSeries]) -> Vec<(i64, f64)> {
    let mut out = Vec::new();
    for s in series {
        for cm in &s.chunks {
            let enc = cm.chunk.as_ref().expect("chunk bytes present");
            for ds in decode_chunk_samples(enc).unwrap() {
                let SampleValue::Float(v) = ds.value else {
                    panic!("expected float sample");
                };
                out.push((ds.t, v));
            }
        }
    }
    out.sort_by_key(|&(t, _)| t);
    out
}

#[test]
fn w22_deep_02_1_ooo_compaction_last_garbage_collected_mmap_ref_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 2 * 3_600_000,
            ..HeadOptions::default()
        },
        ..TsdbOptions::default()
    };
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap());
    let pairs = [("__name__", "w22_ooo_iso"), ("job", "api")];
    let matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "w22_ooo_iso").unwrap()];

    {
        let mut app = db.appender();
        let r = app.append_float(None, &pairs, 20_000, 20.0).unwrap();
        app.append_float(Some(r), &pairs, 5_000, 5.0).unwrap();
        app.append_float(Some(r), &pairs, 6_000, 6.0).unwrap();
        app.commit().unwrap();
    }

    // Old reader snapshots BEFORE OOO compaction reloads the new OOO ULID block.
    // Its HeadReadGuard pins OOO chunks in Head while truncate_ooo waits.
    let old_cq = db.unordered_chunk_querier(4_000, 7_000);
    let old_prep = old_cq.prepare_select(&matchers).unwrap();
    assert!(db.blocks().is_empty());

    let db_bg = Arc::clone(&db);
    let ooo_done = Arc::new(AtomicBool::new(false));
    let ooo_done_bg = Arc::clone(&ooo_done);
    let handle = std::thread::spawn(move || {
        let ulids = db_bg.compact_ooo_head().unwrap();
        assert!(!ulids.is_empty());
        ooo_done_bg.store(true, Ordering::Release);
    });

    // Wait until reload_blocks makes the OOO ULID block visible.
    wait_until("OOO ULID block visible", || !db.blocks().is_empty());
    std::thread::sleep(Duration::from_millis(40));
    assert!(
        !ooo_done.load(Ordering::Acquire),
        "truncate_ooo must wait on old HeadReadGuard"
    );

    // While truncate_ooo is paused, a newly opened UnorderedChunkQuerier must read OOO chunks
    // ONLY from the new ULID block (skipping Head OOO chunks <= last_garbage_collected_mmap_ref).
    let new_res = db
        .unordered_chunk_querier(4_000, 7_000)
        .select(true, &matchers)
        .unwrap();
    let new_samples = decode_raw_float_samples(&new_res);
    assert_eq!(
        new_samples,
        vec![(5_000, 5.0), (6_000, 6.0)],
        "newly opened UnorderedChunkQuerier during paused truncate_ooo must return single-copy OOO samples (zero duplicates!)"
    );

    // Meanwhile, the old reader still reads its exact OOO chunks from Head.
    let old_res = old_cq.execute_prepared_select(old_prep, true).unwrap();
    let old_samples = decode_raw_float_samples(&old_res);
    assert_eq!(old_samples, vec![(5_000, 5.0), (6_000, 6.0)]);

    // Now old_prep was consumed & dropped, so truncate_ooo unblocks and completes.
    handle.join().unwrap();
    assert!(ooo_done.load(Ordering::Acquire));

    // After truncate_ooo finishes, queries still return exact single-copy OOO samples.
    let post_res = db
        .unordered_chunk_querier(4_000, 7_000)
        .select(true, &matchers)
        .unwrap();
    assert_eq!(
        decode_raw_float_samples(&post_res),
        vec![(5_000, 5.0), (6_000, 6.0)]
    );

    db.close().unwrap();
}

#[test]
fn w22_deep_02_2_memory_truncation_early_min_time_isolation_with_ooo() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: 10_000,
            ooo_time_window_ms: 3_600_000,
            ..HeadOptions::default()
        },
        ..TsdbOptions::default()
    };
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap());
    let pairs = [("__name__", "w22_mem_iso"), ("job", "api")];
    let matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "w22_mem_iso").unwrap()];

    // Window 1 [0, 10_000), Window 2 [10_000, 20_000), plus OOO sample at t=5_000.
    {
        let mut app = db.appender();
        let r = app.append_float(None, &pairs, 1_000, 1.0).unwrap();
        app.append_float(Some(r), &pairs, 2_000, 2.0).unwrap();
        app.append_float(Some(r), &pairs, 11_000, 11.0).unwrap();
        app.append_float(Some(r), &pairs, 12_000, 12.0).unwrap();
        app.commit().unwrap();
    }
    {
        let mut app = db.appender();
        app.append_float(None, &pairs, 5_000, 5.0).unwrap();
        app.commit().unwrap();
    }

    // Old reader snapshots BEFORE compact_head_range(0, 10_000) reloads the new block.
    let old_cq = db.unordered_chunk_querier(0, 15_000);
    let old_prep = old_cq.prepare_select(&matchers).unwrap();
    assert!(db.blocks().is_empty());

    let db_bg = Arc::clone(&db);
    let trunc_done = Arc::new(AtomicBool::new(false));
    let trunc_done_bg = Arc::clone(&trunc_done);
    let handle = std::thread::spawn(move || {
        let uid = db_bg.compact_head_range(0, 10_000).unwrap();
        assert!(uid.is_some());
        trunc_done_bg.store(true, Ordering::Release);
    });

    // Wait until the new [0, 10_000) block becomes visible in db.blocks().
    wait_until("Head compacted block visible", || !db.blocks().is_empty());
    std::thread::sleep(Duration::from_millis(40));
    assert!(
        !trunc_done.load(Ordering::Acquire),
        "truncate_memory must wait on old HeadReadGuard"
    );
    assert_eq!(
        db.head().min_time(),
        1_000,
        "head.min_time must still be old_min_time while waiting on pre-reload reader"
    );

    // While truncate_memory is paused, a newly opened UnorderedChunkQuerier (ooo_window > 0)
    // must clamp ino_mint = 10_000 and return exact single-copy samples:
    // [1_000, 2_000] from the new block, [5_000] OOO from Head, [11_000, 12_000] in-order from Head!
    let new_res = db
        .unordered_chunk_querier(0, 15_000)
        .select(true, &matchers)
        .unwrap();
    assert_eq!(
        decode_raw_float_samples(&new_res),
        vec![
            (1_000, 1.0),
            (2_000, 2.0),
            (5_000, 5.0),
            (11_000, 11.0),
            (12_000, 12.0),
        ],
        "new UnorderedChunkQuerier during paused truncate_memory must not duplicate [1000, 2000]"
    );

    // Also verify a query completely below trunc_time ([0, 9_000]) with OOO overlap (t=5_000)
    // does NOT read in-order [1_000, 2_000] from Head.
    let below_res = db
        .unordered_chunk_querier(0, 9_000)
        .select(true, &matchers)
        .unwrap();
    assert_eq!(
        decode_raw_float_samples(&below_res),
        vec![(1_000, 1.0), (2_000, 2.0), (5_000, 5.0)]
    );

    // Old reader still receives all 5 samples from Head before releasing its guard.
    let old_res = old_cq.execute_prepared_select(old_prep, true).unwrap();
    assert_eq!(
        decode_raw_float_samples(&old_res),
        vec![
            (1_000, 1.0),
            (2_000, 2.0),
            (5_000, 5.0),
            (11_000, 11.0),
            (12_000, 12.0),
        ]
    );

    handle.join().unwrap();
    assert!(trunc_done.load(Ordering::Acquire));

    db.close().unwrap();
}

#[test]
fn w22_deep_02_3_compact_selected_series_no_livelock_and_skip_evicted_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TsdbOptions {
        head: HeadOptions {
            chunk_range_ms: 10_000,
            ..HeadOptions::default()
        },
        ..TsdbOptions::default()
    };
    let db = Arc::new(Tsdb::open_with_reload_interval(dir.path(), opts, Duration::ZERO).unwrap());
    let sel_pairs = [("__name__", "w22_sel_evict"), ("job", "evict")];
    let keep_pairs = [("__name__", "w22_sel_keep"), ("job", "keep")];
    let sel_matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "w22_sel_evict").unwrap()];

    let sel_ref = {
        let mut app = db.appender();
        let r_sel = app.append_float(None, &sel_pairs, 1_000, 1.0).unwrap();
        app.append_float(Some(r_sel), &sel_pairs, 2_000, 2.0)
            .unwrap();
        let r_keep = app.append_float(None, &keep_pairs, 1_000, 10.0).unwrap();
        app.append_float(Some(r_keep), &keep_pairs, 2_000, 20.0)
            .unwrap();
        app.commit().unwrap();
        r_sel
    };

    // Pre-reload reader holds HeadReadGuard before compact_selected_series reloads blocks.
    let old_cq = db.unordered_chunk_querier(0, 10_000);
    let old_prep = old_cq.prepare_select(&sel_matchers).unwrap();
    assert!(db.blocks().is_empty());

    let db_bg = Arc::clone(&db);
    let evict_done = Arc::new(AtomicBool::new(false));
    let evict_done_bg = Arc::clone(&evict_done);
    let compact_handle = std::thread::spawn(move || {
        let ulids = db_bg.compact_selected_series(&[sel_ref]).unwrap();
        assert!(!ulids.is_empty());
        evict_done_bg.store(true, Ordering::Release);
    });

    // Wait until the selected-series block becomes visible.
    wait_until("selected-series block visible", || !db.blocks().is_empty());
    std::thread::sleep(Duration::from_millis(40));
    assert!(
        !evict_done.load(Ordering::Acquire),
        "evict_series_refs_inner must wait on pre-reload HeadReadGuard"
    );

    // Start continuous overlapping HeadReadGuards AFTER compact_selected_series began waiting:
    // at least one overlapping HeadReadGuard is ALWAYS held until `stop_stream = true`!
    // Under the old wait_for_range_all, this would livelock compact_selected_series forever.
    let stop_stream = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop_stream);
    let db_stream = Arc::clone(&db);
    let stream_started = Arc::new(AtomicBool::new(false));
    let stream_started_c = Arc::clone(&stream_started);
    let continuous_reader = std::thread::spawn(move || {
        let mut active = db_stream.head().start_read(0, 10_000);
        stream_started_c.store(true, Ordering::Release);
        while !stop_c.load(Ordering::Acquire) {
            let next = db_stream.head().start_read(0, 10_000);
            drop(active);
            active = next;
            std::thread::sleep(Duration::from_millis(2));
        }
        drop(active);
    });
    wait_until("continuous overlapping reader started", || {
        stream_started.load(Ordering::Acquire)
    });

    // While evict_series_refs_inner is paused, newly opened UnorderedChunkQuerier must skip
    // sel_ref in Head and read it ONLY from the new ULID block (zero duplicates!).
    for _ in 0..5 {
        let res = db
            .unordered_chunk_querier(0, 10_000)
            .select(true, &sel_matchers)
            .unwrap();
        assert_eq!(
            decode_raw_float_samples(&res),
            vec![(1_000, 1.0), (2_000, 2.0)],
            "UnorderedChunkQuerier during eviction must return single-copy chunks (zero duplicates!)"
        );
    }

    // Old reader still gets its exact Head samples before dropping its guard.
    let old_res = old_cq.execute_prepared_select(old_prep, true).unwrap();
    assert_eq!(
        decode_raw_float_samples(&old_res),
        vec![(1_000, 1.0), (2_000, 2.0)]
    );

    // Even while continuous overlapping HeadReadGuards keep rotating in `continuous_reader`,
    // `compact_selected_series` must NOT livelock and must finish promptly!
    wait_until("compact_selected_series finishes without livelock", || {
        evict_done.load(Ordering::Acquire)
    });

    stop_stream.store(true, Ordering::Release);
    continuous_reader.join().unwrap();
    compact_handle.join().unwrap();

    // Post-eviction query still sees sel_s from the block and keep_s from Head (float_enc sanity).
    let sel_post = db
        .unordered_chunk_querier(0, 10_000)
        .select(true, &sel_matchers)
        .unwrap();
    assert_eq!(
        decode_raw_float_samples(&sel_post),
        vec![(1_000, 1.0), (2_000, 2.0)]
    );
    let keep_matchers = [LabelMatcher::new(MatchType::Equal, "__name__", "w22_sel_keep").unwrap()];
    let keep_post = db
        .unordered_chunk_querier(0, 10_000)
        .select(true, &keep_matchers)
        .unwrap();
    assert_eq!(
        decode_raw_float_samples(&keep_post),
        vec![(1_000, 10.0), (2_000, 20.0)]
    );
    let _ = Encoding::Xor;

    db.close().unwrap();
}
