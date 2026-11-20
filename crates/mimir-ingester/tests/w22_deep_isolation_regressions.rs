//! Wave 22 regression tests for W22-DEEP-01 & W22-DEEP-02 on the Ingester QueryStream path:
//! - W22-DEEP-01: Atomic Block + Head + OOO source snapshotting in `select_chunk_series_for_query`
//!   when `index_lookup_planning_enabled = true` and `comparison_portion = 1.0`.
//! - W22-DEEP-02 (1): Planned `QueryStream` during paused `truncate_ooo` emits exact single-copy
//!   OOO chunks (zero duplicates!).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_config::ModelDuration;
use mimir_ingester::mirror_querier::{
    ChunkBlockMeta, ChunkQuerier, ChunkSeriesSet, MirroredChunkQuerier,
};
use mimir_ingester::query::{
    HeadAndOOOChunkQuerier, MirroredChunkQuerierAdapter, PlannedTsdbChunkQuerier,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_tsdb::chunkenc::{EncodedChunk, XorChunk};
use mimir_tsdb::compact::{decode_chunk_samples, SampleValue};
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use prometheus::Registry;
use tempfile::TempDir;
use tonic::metadata::MetadataMap;

use common::{
    default_ingester_test_config, default_limits_test_config,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

fn wait_until(label: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn counter_value(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                let all_match = labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|lp| lp.get_name() == *k && lp.get_value() == *v)
                });
                if all_match {
                    return m.get_counter().get_value();
                }
            }
        }
    }
    0.0
}

async fn push_float_series(
    ing: &mimir_ingester::ingester::Ingester,
    user: &str,
    name: &str,
    samples: &[(i64, f64)],
) {
    let req = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", name),
                cortexpb::LabelPair::from_strs("job", "w22"),
            ],
            samples: samples
                .iter()
                .map(|&(t, v)| cortexpb::Sample {
                    value: v,
                    timestamp_ms: t,
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    ing.push_grpc(user, &req).await.unwrap();
}

fn decode_query_stream_samples_raw(
    responses: &[ingesterpb::QueryStreamResponse],
) -> BTreeMap<String, Vec<(i64, f64)>> {
    let mut series_names: Vec<String> = Vec::new();
    let mut chunks_by_series: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();

    for resp in responses {
        for s in &resp.streaming_series {
            let name = s
                .labels
                .iter()
                .find(|lp| lp.name.as_ref() == b"__name__")
                .map(|lp| String::from_utf8_lossy(&lp.value).into_owned())
                .unwrap_or_default();
            series_names.push(name);
        }
        for sc in &resp.streaming_series_chunks {
            let name = series_names[sc.series_index as usize].clone();
            let entry = chunks_by_series.entry(name).or_default();
            for ch in &sc.chunks {
                let xc = XorChunk::from_bytes(ch.data.to_vec());
                for ds in decode_chunk_samples(&EncodedChunk::Xor(xc)).unwrap() {
                    let SampleValue::Float(v) = ds.value else {
                        panic!("expected float sample");
                    };
                    entry.push((ds.t, v));
                }
            }
        }
    }
    for vals in chunks_by_series.values_mut() {
        vals.sort_by_key(|&(t, _)| t);
    }
    chunks_by_series
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w22_deep_01_atomic_snapshot_across_concurrent_head_ooo_selected_and_block_compactions() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .index_lookup_planning_comparison_portion = 1.0;
    blocks_cfg.tsdb.block_ranges_period = vec![
        mimir_config::GoDuration(10 * 1_000_000_000),
        mimir_config::GoDuration(20 * 1_000_000_000),
    ];

    let mut limits = default_limits_test_config();
    limits.out_of_order_time_window = ModelDuration(3600 * 1_000_000_000);

    let reg = Registry::new();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let user = "user-w22-deep01";
    // Build Block B1 [0, 10_000) and Block B2 [10_000, 20_000) + Head [20_000, 30_000) + OOO [5_000, 6_000].
    push_float_series(&ing, user, "s_blk1", &[(1_000, 1.0), (2_000, 2.0)]).await;
    let db = ing.get_tsdb(user).unwrap();
    assert!(db.db.compact_head_range(0, 10_000).unwrap().is_some());

    push_float_series(&ing, user, "s_blk2", &[(11_000, 11.0), (12_000, 12.0)]).await;
    assert!(db.db.compact_head_range(10_000, 20_000).unwrap().is_some());
    assert_eq!(db.db.block_ulids().len(), 2);

    push_float_series(&ing, user, "s_head", &[(21_000, 21.0), (22_000, 22.0)]).await;
    push_float_series(&ing, user, "s_sel", &[(21_000, 31.0), (22_000, 32.0)]).await;
    push_float_series(&ing, user, "s_ooo", &[(25_000, 25.0)]).await;
    push_float_series(&ing, user, "s_ooo", &[(5_000, 5.0), (6_000, 6.0)]).await;

    let sel_ref = db
        .db
        .head()
        .get_entry_by_sorted_pairs(&[("__name__", "s_sel"), ("job", "w22")])
        .unwrap()
        .ref_id;

    // Part A: Deterministic guard pinning across simultaneous Head, OOO, Selected-Series, and Block compactions!
    let snapshot = db.db.snapshot_query_sources(0, 30_000);
    let block_guards = snapshot.block_guards;
    let blocks = snapshot.blocks;
    let head_snap = snapshot.head.expect("head snapshot must exist");
    assert_eq!(blocks.len(), 2);
    assert_eq!(block_guards.len(), 2);

    // Spawn concurrent compactions for ALL 4 sources while `snapshot` is held:
    // Because `compact_mtx` serializes compactions on Tsdb, run them sequentially in a
    // background thread — the VERY FIRST one that hits `HeadReadGuard` or `BlockReadGuard`
    // will block until we finish reading from `snapshot` and drop its guards!
    let db_c = Arc::clone(&db);
    let compactions_done = Arc::new(AtomicBool::new(false));
    let compactions_done_bg = Arc::clone(&compactions_done);
    let compact_thread = std::thread::spawn(move || {
        // 1. Block compaction (wants to close B1 & B2 -> blocked on block_guards!)
        let _ = db_c.db.compact_blocks().unwrap();
        // 2. Selected-series compaction (wants to evict s_sel -> blocked on head_snap.guard!)
        let _ = db_c.db.compact_selected_series(&[sel_ref]).unwrap();
        // 3. OOO compaction (wants to truncate OOO -> blocked on head_snap.guard!)
        let _ = db_c.db.compact_ooo_head().unwrap();
        // 4. Head compaction (wants to truncate Head -> blocked on head_snap.guard!)
        let _ = db_c.db.compact_head_range(20_000, 30_000).unwrap();
        compactions_done_bg.store(true, Ordering::Release);
    });

    // Wait briefly and verify compactions are blocked on our snapshot's guards.
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(
        !compactions_done.load(Ordering::Acquire),
        "compactions must block while atomic snapshot guards are held"
    );

    // Now execute the exact planned+mirrored+HeadAndOOO pipeline using `snapshot`:
    let matchers = [LabelMatcher::new(MatchType::Equal, "job", "w22").unwrap()];
    let mut all_samples: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
    let head_planner = db.head_lookup_planner();
    let pq_head = PlannedTsdbChunkQuerier::new_head_with_snapshot(
        Arc::clone(&db.db),
        0,
        30_000,
        Arc::clone(&head_planner),
        true,
        head_snap.clone(),
    );
    let mut head_cq: Box<dyn ChunkQuerier> = Box::new(MirroredChunkQuerierAdapter(
        MirroredChunkQuerier::new_with_meta(
        user,
        ing.metrics.index_lookup_comparison_outcomes.clone(),
        0,
        30_000,
        mimir_ingester::mirror_querier::ChunkBlockMeta {
            ulid: mimir_ingester::lookupplan::HEAD_ULID,
            min_time: head_snap.min_time,
            max_time: head_snap.max_time,
        },
        Box::new(pq_head),
    )));
    if head_snap.overlaps_ooo {
        head_cq = Box::new(HeadAndOOOChunkQuerier::new_with_snapshot(
            Arc::clone(&db.db),
            0,
            30_000,
            head_planner,
            Some(head_cq),
            head_snap,
        ));
    }
    let mut ss = head_cq.select(true, None, &matchers);
    while ss.next() {
        let s = ss.at().unwrap();
        let name = s.labels().get("__name__").to_string();
        let entry = all_samples.entry(name).or_default();
        for cm in s.chunks() {
            for ds in decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap() {
                let SampleValue::Float(v) = ds.value else {
                    panic!("expected float");
                };
                entry.push((ds.t, v));
            }
        }
    }
    assert!(ss.err().is_none());
    drop(ss);
    head_cq.close().unwrap();

    for (blk, guard) in blocks.into_iter().zip(block_guards) {
        let m = blk.meta();
        let block_meta = ChunkBlockMeta {
            ulid: m.ulid,
            min_time: m.min_time,
            max_time: m.max_time,
        };
        let planner = db.block_lookup_planner(&blk);
        let pq = PlannedTsdbChunkQuerier::new_block_with_guard(
            Arc::clone(&db.db),
            blk,
            guard,
            0,
            30_000,
            planner,
            true,
        );
        let mut mq = MirroredChunkQuerier::new_with_meta(
            user,
            ing.metrics.index_lookup_comparison_outcomes.clone(),
            0,
            30_000,
            block_meta,
            Box::new(pq),
        );
        let mut ss = mq.select(true, None, &matchers);
        while ss.next() {
            let s = ss.at().unwrap();
            let name = s.labels().get("__name__").to_string();
            let entry = all_samples.entry(name).or_default();
            for cm in s.chunks() {
                for ds in decode_chunk_samples(cm.chunk.as_ref().unwrap()).unwrap() {
                    let SampleValue::Float(v) = ds.value else {
                        panic!("expected float");
                    };
                    entry.push((ds.t, v));
                }
            }
        }
        assert!(ss.err().is_none());
        drop(ss);
        mq.close().unwrap();
    }

    for v in all_samples.values_mut() {
        v.sort_by_key(|&(t, _)| t);
    }
    assert_eq!(all_samples["s_blk1"], vec![(1_000, 1.0), (2_000, 2.0)]);
    assert_eq!(all_samples["s_blk2"], vec![(11_000, 11.0), (12_000, 12.0)]);
    assert_eq!(all_samples["s_head"], vec![(21_000, 21.0), (22_000, 22.0)]);
    assert_eq!(all_samples["s_sel"], vec![(21_000, 31.0), (22_000, 32.0)]);
    assert_eq!(
        all_samples["s_ooo"],
        vec![(5_000, 5.0), (6_000, 6.0), (25_000, 25.0)]
    );

    // All snapshot guards have now been dropped; compactions must complete cleanly!
    compact_thread.join().unwrap();
    assert!(compactions_done.load(Ordering::Acquire));

    // Part B: Concurrent `handle_query_stream` hammer test while Head, OOO, Selected-Series,
    // and Block compactions run repeatedly in the background.
    // Seed 3 new series across [30_000, 60_000) and hammer `handle_query_stream` across [0, 70_000].
    push_float_series(&ing, user, "s_hammer_1", &[(31_000, 1.0), (41_000, 2.0)]).await;
    push_float_series(&ing, user, "s_hammer_2", &[(51_000, 3.0)]).await;
    push_float_series(&ing, user, "s_hammer_2", &[(35_000, 4.0)]).await; // OOO

    let hammer_sel_ref = db
        .db
        .head()
        .get_entry_by_sorted_pairs(&[("__name__", "s_hammer_1"), ("job", "w22")])
        .unwrap()
        .ref_id;

    let db_h = Arc::clone(&db);
    let compact_done_h = Arc::new(AtomicBool::new(false));
    let compact_done_hc = Arc::clone(&compact_done_h);
    let compact_hammer = std::thread::spawn(move || {
        let _ = db_h.db.compact_selected_series(&[hammer_sel_ref]);
        let _ = db_h.db.compact_ooo_head();
        let _ = db_h.db.compact_head_range(30_000, 40_000);
        let _ = db_h.db.compact_head_range(40_000, 50_000);
        let _ = db_h.db.compact_head_range(50_000, 60_000);
        let _ = db_h.db.compact_blocks();
        compact_done_hc.store(true, Ordering::Release);
    });

    let mut queries_run = 0usize;
    while !compact_done_h.load(Ordering::Acquire) || queries_run < 5 {
        let res = ing
            .handle_query_stream(
                &MetadataMap::new(),
                user,
                ingesterpb::QueryRequest {
                    start_timestamp_ms: 0,
                    end_timestamp_ms: 70_000,
                    matchers: vec![ingesterpb::LabelMatcher {
                        r#type: ingesterpb::MatchType::Equal as i32,
                        name: "job".to_string(),
                        value: "w22".to_string(),
                    }],
                    streaming_chunks_batch_size: 0,
                },
            )
            .await
            .unwrap();
        let decoded = decode_query_stream_samples_raw(&res);
        assert_eq!(decoded["s_blk1"], vec![(1_000, 1.0), (2_000, 2.0)]);
        assert_eq!(decoded["s_blk2"], vec![(11_000, 11.0), (12_000, 12.0)]);
        assert_eq!(decoded["s_head"], vec![(21_000, 21.0), (22_000, 22.0)]);
        assert_eq!(decoded["s_sel"], vec![(21_000, 31.0), (22_000, 32.0)]);
        assert_eq!(
            decoded["s_ooo"],
            vec![(5_000, 5.0), (6_000, 6.0), (25_000, 25.0)]
        );
        assert_eq!(decoded["s_hammer_1"], vec![(31_000, 1.0), (41_000, 2.0)]);
        assert_eq!(decoded["s_hammer_2"], vec![(35_000, 4.0), (51_000, 3.0)]);
        queries_run += 1;
        if queries_run > 200 {
            break;
        }
    }
    compact_hammer.join().unwrap();

    assert_eq!(
        counter_value(
            &reg,
            "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
            &[("outcome", "missing_series"), ("user", user)]
        ),
        0.0
    );
    assert_eq!(
        counter_value(
            &reg,
            "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
            &[("outcome", "extra_series"), ("user", user)]
        ),
        0.0
    );

    let _ = ing.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w22_deep_02_1_planned_query_stream_during_paused_truncate_ooo_single_copy() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .index_lookup_planning_comparison_portion = 1.0;

    let mut limits = default_limits_test_config();
    limits.out_of_order_time_window = ModelDuration(3600 * 1_000_000_000);

    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, None);
    start_and_wait_healthy(&ing).await;

    let user = "user-w22-deep02-ooo";
    push_float_series(&ing, user, "w22_planned_ooo", &[(20_000, 20.0)]).await;
    push_float_series(&ing, user, "w22_planned_ooo", &[(5_000, 5.0), (6_000, 6.0)]).await;

    let db = ing.get_tsdb(user).unwrap();
    // Pin pre-compaction OOO chunks in Head via an old snapshot guard.
    let old_snapshot = db.db.snapshot_query_sources(4_000, 7_000);
    assert!(old_snapshot.blocks.is_empty());
    assert!(old_snapshot.head.is_some());

    let db_bg = Arc::clone(&db);
    let ooo_done = Arc::new(AtomicBool::new(false));
    let ooo_done_bg = Arc::clone(&ooo_done);
    let compact_thread = std::thread::spawn(move || {
        let ulids = db_bg.db.compact_ooo_head().unwrap();
        assert!(!ulids.is_empty());
        ooo_done_bg.store(true, Ordering::Release);
    });

    wait_until("OOO block visible in Tsdb", || !db.db.block_ulids().is_empty());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        !ooo_done.load(Ordering::Acquire),
        "truncate_ooo must wait on old_snapshot HeadReadGuard"
    );

    // Execute planned+mirrored QueryStream while truncate_ooo is paused:
    // Must return exact single-copy OOO samples [(5_000, 5.0), (6_000, 6.0)] with zero duplicates!
    let res = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 4_000,
                end_timestamp_ms: 7_000,
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".to_string(),
                    value: "w22_planned_ooo".to_string(),
                }],
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    let decoded = decode_query_stream_samples_raw(&res);
    let mut keys: BTreeSet<&String> = BTreeSet::new();
    keys.extend(decoded.keys());
    assert_eq!(keys.len(), 1);
    assert_eq!(
        decoded["w22_planned_ooo"],
        vec![(5_000, 5.0), (6_000, 6.0)],
        "planned QueryStream during paused truncate_ooo must return single-copy OOO chunks"
    );

    drop(old_snapshot);
    compact_thread.join().unwrap();
    assert!(ooo_done.load(Ordering::Acquire));

    let _ = ing.stop().await;
}
