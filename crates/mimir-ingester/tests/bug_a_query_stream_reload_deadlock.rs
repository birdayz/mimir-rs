//! Deterministic regression test for Hetzner Pass 3 Bug A:
//! 5-party deadlock across QueryStream (tx.blocking_send), TsdbInner::reload_blocks,
//! /metrics (registry.gather()), Ingester::weak_self, and new series pre_creation.

mod common;

use std::time::{Duration, Instant};

use futures::StreamExt;
use mimir_config::GoDuration;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use prometheus::Registry;
use tempfile::TempDir;
use tonic::metadata::MetadataMap;

use common::{
    default_ingester_test_config, default_limits_test_config, prepare_ingester_with_blocks_storage,
    start_and_wait_healthy,
};

const USER: &str = "bug-a-tenant";

fn make_write_req(prefix: &str, count: usize, start_idx: usize, ts_ms: i64) -> cortexpb::WriteRequest {
    let mut timeseries = Vec::with_capacity(count);
    for i in start_idx..start_idx + count {
        timeseries.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair {
                    name: "__name__".into(),
                    value: format!("{prefix}_metric").into(),
                },
                cortexpb::LabelPair {
                    name: "pod".into(),
                    value: format!("pod-{i:05}").into(),
                },
            ],
            samples: vec![cortexpb::Sample {
                value: i as f64,
                timestamp_ms: ts_ms,
            }],
            ..Default::default()
        });
    }
    cortexpb::WriteRequest {
        timeseries,
        source: cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    }
}

#[cfg(target_os = "linux")]
fn count_deleted_tsdb_mmaps(tsdb_dir: &std::path::Path) -> usize {
    let dir_str = tsdb_dir.to_string_lossy();
    let maps = std::fs::read_to_string("/proc/self/maps").expect("read /proc/self/maps");
    maps.lines()
        .filter(|line| line.contains("(deleted)") && line.contains(dir_str.as_ref()))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_query_stream_does_not_deadlock_reload_metrics_or_new_series() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    // Short retention so Part 2 can exercise live retention deletion while a QueryStream
    // holds a BlockReadGuard on the expiring block.
    blocks_cfg.tsdb.retention_period = GoDuration::from_millis(20);
    let reg = Registry::new();
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;

    // 1. Push 1,200 series (> 128 * 4 frames) and compact into ULID block #1.
    ing.push_to_storage(USER, &make_write_req("bug_a", 1200, 0, 1_000_000))
        .unwrap();
    let user_tsdb = ing.get_tsdb(USER).expect("user tsdb");
    let block1 = user_tsdb
        .db
        .compact_head_all()
        .unwrap()
        .expect("block1 compacted");
    assert_eq!(user_tsdb.db.block_ulids(), vec![block1]);

    // 2. Start a QueryStream whose consumer pauses after frame 1 (filling mpsc::channel(4)).
    let query_req = ingesterpb::QueryRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 20_000_000,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".into(),
            value: "bug_a_metric".into(),
        }],
        streaming_chunks_batch_size: 16,
    };
    let mut stream = ing
        .handle_query_stream_bimodal(&MetadataMap::new(), USER, query_req.clone())
        .await
        .expect("start QueryStream");
    let first_frame = stream
        .next()
        .await
        .expect("frame 1 present")
        .expect("frame 1 ok");
    assert!(
        !first_frame.streaming_series.is_empty(),
        "expected series labels in frame 1"
    );

    // Give the blocking worker a moment to fill the bounded channel (cap = 4) and block on
    // tx.blocking_send.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // 3. While QueryStream is paused with channel full, concurrently run:
    //    - reload_blocks() (no-op reload)
    //    - registry.gather() (/metrics)
    //    - push_to_storage() with brand-new series (pre_creation -> max_in_memory_series_fn)
    let op_start = Instant::now();
    let tsdb_c = user_tsdb.clone();
    let reload_fut = tokio::task::spawn_blocking(move || tsdb_c.db.reload_blocks());

    let reg_c = reg.clone();
    let metrics_fut = tokio::task::spawn_blocking(move || {
        for _ in 0..5 {
            let fams = reg_c.gather();
            assert!(!fams.is_empty());
        }
    });

    let ing_c = ing.clone();
    let push_fut = tokio::task::spawn_blocking(move || {
        ing_c.push_to_storage(USER, &make_write_req("bug_a_new", 32, 10_000, 10_000_000))
    });

    let (r_res, m_res, p_res) = tokio::time::timeout(
        Duration::from_secs(2),
        async { tokio::join!(reload_fut, metrics_fut, push_fut) },
    )
    .await
    .expect("concurrent reload_blocks + metrics + new series must complete in <2s");
    r_res.unwrap().unwrap();
    m_res.unwrap();
    p_res.unwrap().unwrap();
    assert!(
        op_start.elapsed() < Duration::from_secs(2),
        "elapsed {:?}",
        op_start.elapsed()
    );

    // 4. Resume draining QueryStream and verify all frames arrive.
    let mut total_frames = 1usize;
    while let Some(item) = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("stream frame within 2s")
    {
        item.expect("stream frame ok");
        total_frames += 1;
    }
    assert!(
        total_frames > 5,
        "expected >5 frames to guarantee mpsc::channel(4) filled, got {total_frames}"
    );

    // 5. Part B: Retention expiry while a QueryStream is paused holding a BlockReadGuard on block1.
    //    Push & compact block2 well past retention_period (20ms).
    tokio::time::sleep(Duration::from_millis(35)).await;
    ing.push_to_storage(USER, &make_write_req("bug_a", 1200, 2000, 10_000_000))
        .unwrap();
    let block2 = user_tsdb
        .db
        .compact_head_all()
        .unwrap()
        .expect("block2 compacted");
    assert_ne!(block1, block2);

    // Start a second QueryStream that acquires a BlockReadGuard on block1, pause after frame 1.
    let mut stream2 = ing
        .handle_query_stream_bimodal(&MetadataMap::new(), USER, query_req)
        .await
        .expect("start QueryStream 2");
    let _f1 = stream2
        .next()
        .await
        .expect("stream2 frame 1")
        .expect("stream2 frame 1 ok");
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Trigger reload_blocks() (which wants to delete expired block1): it must NOT hold
    // blocks.write() while waiting for stream2 BlockReadGuard, so registry.gather() and
    // new-series push MUST STILL finish in <2s!
    let tsdb_c2 = user_tsdb.clone();
    let reload_handle = tokio::task::spawn_blocking(move || tsdb_c2.db.reload_blocks());
    tokio::time::sleep(Duration::from_millis(30)).await;

    let reg_c2 = reg.clone();
    let m2 = tokio::task::spawn_blocking(move || reg_c2.gather());
    let ing_c2 = ing.clone();
    let p2 = tokio::task::spawn_blocking(move || {
        ing_c2.push_to_storage(USER, &make_write_req("bug_a_new2", 16, 20_000, 10_001_000))
    });
    let (m2_res, p2_res) = tokio::time::timeout(Duration::from_secs(2), async { tokio::join!(m2, p2) })
        .await
        .expect("metrics + push must not block behind retention reload_blocks");
    assert!(!m2_res.unwrap().is_empty());
    p2_res.unwrap().unwrap();

    // Now drain stream2 so its BlockReadGuard drops and reload_blocks finishes deleting block1.
    while let Some(item) = tokio::time::timeout(Duration::from_secs(2), stream2.next())
        .await
        .expect("stream2 drain")
    {
        item.expect("stream2 ok");
    }
    tokio::time::timeout(Duration::from_secs(2), reload_handle)
        .await
        .expect("reload_blocks finishes after stream2 drops BlockReadGuard")
        .unwrap()
        .unwrap();

    assert_eq!(
        user_tsdb.db.block_ulids(),
        vec![block2],
        "expired block1 should have been deleted"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(
        count_deleted_tsdb_mmaps(dir.path()),
        0,
        "0 (deleted) mmaps must remain after retention"
    );

    let _ = ing.stop().await;
}
