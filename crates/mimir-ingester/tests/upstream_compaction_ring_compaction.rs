//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_compaction_test.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_early_compaction_test.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_idle_tsdb_test.go

mod compaction_ring_helpers;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::{ManagedBasicService, Service};
use mimir_config::limits::LimitsConfig;
use mimir_config::{GoDuration, InstanceLimits, ModelDuration};
use mimir_ingester::compaction::{
    compaction_loop_extra_delay, filter_users_to_compact_to_reduce_in_memory_series,
    SeriesReductionEstimation,
};
use mimir_ingester::user_tsdb::TsdbCloseCheckResult;
use mimir_ingester::Ingester;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::sharding::shard_by_all_labels;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::KvClient;
use mimir_ring::ring::model::ring_desc_codec;
use mimir_ring::ring::Ring;
use parking_lot::Mutex;
use tonic::metadata::MetadataMap;

use compaction_ring_helpers::{
    default_ingester_test_config, default_limits_test_config, list_blocks_in_dir,
    prepare_ingester_with_blocks_storage, push_series_to_ingester,
    read_metric_samples_from_block_dir, run_test_query, start_and_wait_healthy,
    TestIngesterConfigs, USER_ID,
};

fn unix_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

fn parse_rfc3339_ms(s: &str) -> i64 {
    // Supported test timestamps: "2023-06-24T00:00:00Z" and "2026-05-05T00:00:00Z"
    let date_part = &s[0..10];
    let parts: Vec<i64> = date_part
        .split('-')
        .map(|p| p.parse::<i64>().unwrap())
        .collect();
    let (y, m, d) = (parts[0], parts[1], parts[2]);
    // Howard Hinnant civil-from-days inverse
    let y_adj = y - if m <= 2 { 1 } else { 0 };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    days * 86_400 * 1000
}

fn pick_owned_and_non_owned_series(user_id: &str) -> (&'static str, &'static str, u32) {
    let name_a = "metric_a";
    let name_b = "metric_b";
    let hash_a = shard_by_all_labels(user_id, [("__name__", name_a)]);
    let hash_b = shard_by_all_labels(user_id, [("__name__", name_b)]);
    assert_ne!(hash_a, hash_b);
    if hash_a < hash_b {
        (name_a, name_b, hash_a)
    } else {
        (name_b, name_a, hash_b)
    }
}

async fn setup_test_ingester_ring(
    root: &Path,
    zones: &[&str],
    ingesters_per_zone: usize,
    base_cfg: &TestIngesterConfigs,
    limits: LimitsConfig,
) -> Vec<(Arc<Ingester>, Arc<Ring>, Arc<ManagedBasicService>)> {
    let shared_kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let mut out = Vec::with_capacity(zones.len() * ingesters_per_zone);

    for (z_idx, &zone) in zones.iter().enumerate() {
        for i in 0..ingesters_per_zone {
            let sub = root.join(format!("ing_{z_idx}_{i}"));
            let tsdb_dir = sub.join("tsdb");
            let bucket_dir = sub.join("bucket");
            let _ = std::fs::create_dir_all(&tsdb_dir);
            let _ = std::fs::create_dir_all(&bucket_dir);

            let mut cfg = TestIngesterConfigs {
                ingester: base_cfg.ingester.clone(),
                blocks: base_cfg.blocks.clone(),
                ingest: base_cfg.ingest.clone(),
                ring_kv: Arc::clone(&shared_kv),
            };
            let id = format!("ingester-{zone}-{i}");
            cfg.ingester.ring.instance_id = id.clone();
            cfg.ingester.ring.instance_addr = id;
            cfg.ingester.ring.zone_awareness_enabled = true;
            cfg.ingester.ring.instance_availability_zone = zone.to_string();
            cfg.blocks.tsdb.dir = tsdb_dir.to_string_lossy().to_string();
            cfg.blocks.bucket.backend_cfg.filesystem.dir =
                bucket_dir.to_string_lossy().to_string();

            let (ing, ring, ring_svc) =
                prepare_ingester_with_blocks_storage(cfg, Some(limits.clone()), None).await;
            start_and_wait_healthy(&ing, &ring).await;
            // Pause background owned-series ticker so manual ownedTokenRanges edits aren't overwritten.
            ing.set_owned_series_paused(true);
            out.push((ing, ring, ring_svc));
        }
    }
    out
}

async fn stop_ring_ingesters(list: Vec<(Arc<Ingester>, Arc<Ring>, Arc<ManagedBasicService>)>) {
    for (ing, _, ring_svc) in list {
        let _ = ing.stop().await;
        ring_svc.stop_async();
        let _ = ring_svc.await_terminated().await;
    }
}

// Upstream: pkg/ingester/ingester_compaction_test.go::TestCompactionLoopExtraDelay
#[test]
fn test_compaction_loop_extra_delay() {
    let standard_interval = Duration::from_secs(60);
    let min_delay = Duration::from_secs(10);

    let cases = [
        (
            "fast iteration well below interval: no extra delay",
            Duration::from_secs(5),
            Duration::ZERO,
        ),
        (
            "iteration exactly at the boundary: no extra delay",
            standard_interval - min_delay,
            Duration::ZERO,
        ),
        (
            "iteration just past the boundary: extra delay equals minDelay",
            standard_interval - min_delay + Duration::from_secs(1),
            min_delay,
        ),
        (
            "iteration near the full interval: extra delay equals minDelay",
            standard_interval - Duration::from_secs(1),
            min_delay,
        ),
        (
            "iteration exactly at the interval: extra delay equals minDelay",
            standard_interval,
            min_delay,
        ),
        (
            "iteration overruns the interval: extra delay equals minDelay",
            standard_interval + Duration::from_secs(1),
            min_delay,
        ),
        (
            "iteration far overruns the interval: extra delay equals minDelay",
            10 * standard_interval,
            min_delay,
        ),
    ];

    for (name, iter_dur, expected) in cases {
        let got = compaction_loop_extra_delay(iter_dur, standard_interval, min_delay);
        assert_eq!(got, expected, "case {name}");
    }
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_filterUsersToCompactToReduceInMemorySeries
#[test]
fn test_ingester_filter_users_to_compact_to_reduce_in_memory_series() {
    // Case 1: below percentage -> empty
    let res1 = filter_users_to_compact_to_reduce_in_memory_series(
        100,
        80,
        20,
        vec![
            SeriesReductionEstimation { user_id: "1".into(), estimated_count: 1, estimated_percentage: 9 },
            SeriesReductionEstimation { user_id: "2".into(), estimated_count: 2, estimated_percentage: 10 },
            SeriesReductionEstimation { user_id: "3".into(), estimated_count: 3, estimated_percentage: 11 },
            SeriesReductionEstimation { user_id: "4".into(), estimated_count: 4, estimated_percentage: 50 },
        ],
    );
    assert!(res1.is_empty());

    // Case 2: >= configured percentage
    let mut res2 = filter_users_to_compact_to_reduce_in_memory_series(
        100,
        80,
        20,
        vec![
            SeriesReductionEstimation { user_id: "1".into(), estimated_count: 1, estimated_percentage: 19 },
            SeriesReductionEstimation { user_id: "2".into(), estimated_count: 2, estimated_percentage: 20 },
            SeriesReductionEstimation { user_id: "3".into(), estimated_count: 3, estimated_percentage: 21 },
            SeriesReductionEstimation { user_id: "4".into(), estimated_count: 50, estimated_percentage: 50 },
        ],
    );
    res2.sort();
    assert_eq!(res2, vec!["2", "3", "4"]);

    // Case 3: < configured percentage if needed to reach target reduction
    let mut res3 = filter_users_to_compact_to_reduce_in_memory_series(
        100,
        80,
        20,
        vec![
            SeriesReductionEstimation { user_id: "1".into(), estimated_count: 6, estimated_percentage: 1 },
            SeriesReductionEstimation { user_id: "2".into(), estimated_count: 7, estimated_percentage: 2 },
            SeriesReductionEstimation { user_id: "3".into(), estimated_count: 8, estimated_percentage: 3 },
            SeriesReductionEstimation { user_id: "4".into(), estimated_count: 15, estimated_percentage: 4 },
        ],
    );
    res3.sort();
    assert_eq!(res3, vec!["3", "4"]);
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceInMemorySeries_ShouldTriggerCompactionOnlyIfEstimatedSeriesReductionIsGreaterThanConfiguredPercentage
#[tokio::test]
async fn test_compact_blocks_to_reduce_in_memory_series_percentage_gate() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.blocks.tsdb.early_head_compaction_min_in_memory_series = 1;
    cfg.blocks
        .tsdb
        .early_head_compaction_min_estimated_series_reduction_percentage = 50;

    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let mut now = SystemTime::now();
    let sample_ts = unix_ms(now);
    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);

    for sid in 0..10 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(&ing, USER_ID, &[(&[("__name__", &name)], &[(sample_ts, 0.0)])])
            .await
            .unwrap();
    }

    // No inactive series yet -> no block
    ing.compact_blocks_to_reduce_in_memory_series(now).await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 0);

    // Mark first 10 series inactive
    let db = ing.get_tsdb(USER_ID).unwrap();
    db.active_series.purge(now + Duration::from_secs(30 * 60));
    assert_eq!(db.db.head().num_series(), 10);
    assert_eq!(db.active_series.active().0, 0);

    // Push 20 more series
    for sid in 10..30 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(&ing, USER_ID, &[(&[("__name__", &name)], &[(sample_ts, 0.0)])])
            .await
            .unwrap();
    }

    // Only 33% inactive < 50% -> should not compact
    ing.compact_blocks_to_reduce_in_memory_series(now).await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 0);
    assert_eq!(db.db.head().num_series(), 30);
    assert_eq!(db.active_series.active().0, 20);

    // Advance time until all are inactive -> triggers compaction
    now += Duration::from_secs(30 * 60);
    ing.compact_blocks_to_reduce_in_memory_series(now).await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);
    assert_eq!(db.db.head().num_series(), 0);
    assert_eq!(db.active_series.active().0, 0);

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceInMemorySeries_LimitPrecedence
#[tokio::test]
async fn test_compact_blocks_to_reduce_in_memory_series_limit_precedence() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.blocks.tsdb.early_head_compaction_min_in_memory_series = 1000;
    cfg.blocks
        .tsdb
        .early_head_compaction_min_estimated_series_reduction_percentage = 50;

    let instance_limits = Arc::new(Mutex::new(InstanceLimits::default()));
    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    let il_c = Arc::clone(&instance_limits);
    ing.set_instance_limits_fn(Arc::new(move || Some(il_c.lock().clone())));
    start_and_wait_healthy(&ing, &ring).await;

    let mut now = SystemTime::now();
    let sample_ts = unix_ms(now);
    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);

    for sid in 0..10 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(&ing, USER_ID, &[(&[("__name__", &name)], &[(sample_ts, 0.0)])])
            .await
            .unwrap();
    }

    now += Duration::from_secs(30 * 60);

    // Instance limit is unset (0) -> config threshold (1000) applies -> no compaction
    ing.compact_blocks_to_reduce_in_memory_series(now).await;
    assert!(list_blocks_in_dir(&user_blocks_dir).is_empty());

    // Instance limit set -> takes precedence over config threshold
    instance_limits.lock().early_head_compaction_min_in_memory_series = 5;
    ing.compact_blocks_to_reduce_in_memory_series(now).await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceInMemorySeries_ShouldCompactHeadUpUntilNowMinusActiveSeriesMetricsIdleTimeout
#[tokio::test]
async fn test_compact_blocks_to_reduce_in_memory_series_compacts_up_until_cutoff() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.blocks.tsdb.early_head_compaction_min_in_memory_series = 1;
    cfg.blocks
        .tsdb
        .early_head_compaction_min_estimated_series_reduction_percentage = 0;

    let mut limits = default_limits_test_config();
    limits.creation_grace_period = ModelDuration::from_hours(24);

    let (ing, ring, ring_svc) =
        prepare_ingester_with_blocks_storage(cfg, Some(limits), None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let metric_name = "metric_1";
    let expected_metric = BTreeMap::from([("__name__".to_string(), metric_name.to_string())]);
    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    let mut now = SystemTime::now();
    let mut sample_times = Vec::new();

    // Phase 1
    {
        let st = now;
        sample_times.push(unix_ms(st));
        push_series_to_ingester(
            &ing,
            USER_ID,
            &[(&[("__name__", metric_name)], &[(unix_ms(st), 1.0)])],
        )
        .await
        .unwrap();

        now += Duration::from_secs(10 * 60);
        ing.compact_blocks_to_reduce_in_memory_series(now).await;
        assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 0);

        now += Duration::from_secs(11 * 60);
        ing.compact_blocks_to_reduce_in_memory_series(now).await;

        let blocks = list_blocks_in_dir(&user_blocks_dir);
        assert_eq!(blocks.len(), 1);
        let bdir = user_blocks_dir.join(blocks[0].to_string());
        assert_eq!(
            read_metric_samples_from_block_dir(&bdir, metric_name),
            vec![(expected_metric.clone(), vec![(unix_ms(st), 1.0)])]
        );
        assert_eq!(ing.get_tsdb(USER_ID).unwrap().db.head().num_series(), 0);
    }

    // Phase 2
    {
        let st = now;
        sample_times.push(unix_ms(st));
        push_series_to_ingester(
            &ing,
            USER_ID,
            &[(&[("__name__", metric_name)], &[(unix_ms(st), 2.0)])],
        )
        .await
        .unwrap();

        now += Duration::from_secs(20 * 60);
        ing.compact_blocks_to_reduce_in_memory_series(now).await;

        let blocks = list_blocks_in_dir(&user_blocks_dir);
        assert_eq!(blocks.len(), 2);
        let bdir = user_blocks_dir.join(blocks[1].to_string());
        assert_eq!(
            read_metric_samples_from_block_dir(&bdir, metric_name),
            vec![(expected_metric.clone(), vec![(unix_ms(st), 2.0)])]
        );
        assert_eq!(ing.get_tsdb(USER_ID).unwrap().db.head().num_series(), 0);
    }

    // Phase 3: normal compaction across 2h boundary
    {
        let first_st = now;
        sample_times.push(unix_ms(first_st));
        push_series_to_ingester(
            &ing,
            USER_ID,
            &[(&[("__name__", metric_name)], &[(unix_ms(first_st), 3.0)])],
        )
        .await
        .unwrap();

        now += Duration::from_secs(4 * 3600);
        let second_st = now;
        sample_times.push(unix_ms(second_st));
        push_series_to_ingester(
            &ing,
            USER_ID,
            &[(&[("__name__", metric_name)], &[(unix_ms(second_st), 4.0)])],
        )
        .await
        .unwrap();

        ing.compact_blocks(false, 0, None).await;
        let blocks = list_blocks_in_dir(&user_blocks_dir);
        assert_eq!(blocks.len(), 3);
        let bdir = user_blocks_dir.join(blocks[2].to_string());
        assert_eq!(
            read_metric_samples_from_block_dir(&bdir, metric_name),
            vec![(expected_metric.clone(), vec![(unix_ms(first_st), 3.0)])]
        );
        assert_eq!(ing.get_tsdb(USER_ID).unwrap().db.head().num_series(), 1);
    }

    // Phase 4: QueryStream returns all 4 samples
    let md = MetadataMap::new();
    let res = run_test_query(
        &ing,
        &md,
        USER_ID,
        ingesterpb::MatchType::Equal,
        "__name__",
        metric_name,
    )
    .await
    .unwrap();
    assert_eq!(
        res,
        vec![(
            expected_metric,
            vec![
                (sample_times[0], 1.0),
                (sample_times[1], 2.0),
                (sample_times[2], 3.0),
                (sample_times[3], 4.0),
            ]
        )]
    );

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceInMemorySeries_ShouldCompactBlocksHonoringBlockRangePeriod
#[tokio::test]
async fn test_compact_blocks_to_reduce_in_memory_series_honors_block_range_period() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::ZERO;
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.blocks.tsdb.early_head_compaction_min_in_memory_series = 1;
    cfg.blocks
        .tsdb
        .early_head_compaction_min_estimated_series_reduction_percentage = 0;

    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let start_ms = parse_rfc3339_ms("2023-06-24T00:00:00Z");
    let hour_ms = 3600_i64 * 1000;
    let metric_name = "metric_1";
    let expected_metric = BTreeMap::from([("__name__".to_string(), metric_name.to_string())]);

    for i in 0..5i64 {
        push_series_to_ingester(
            &ing,
            USER_ID,
            &[(&[("__name__", metric_name)], &[(start_ms + i * hour_ms, i as f64)])],
        )
        .await
        .unwrap();
    }

    ing.compact_blocks_to_reduce_in_memory_series(SystemTime::now())
        .await;

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    let block_ids = list_blocks_in_dir(&user_blocks_dir);
    assert_eq!(block_ids.len(), 3);

    assert_eq!(
        read_metric_samples_from_block_dir(
            &user_blocks_dir.join(block_ids[0].to_string()),
            metric_name
        ),
        vec![(
            expected_metric.clone(),
            vec![(start_ms, 0.0), (start_ms + hour_ms, 1.0)]
        )]
    );
    assert_eq!(
        read_metric_samples_from_block_dir(
            &user_blocks_dir.join(block_ids[1].to_string()),
            metric_name
        ),
        vec![(
            expected_metric.clone(),
            vec![(start_ms + 2 * hour_ms, 2.0), (start_ms + 3 * hour_ms, 3.0)]
        )]
    );
    assert_eq!(
        read_metric_samples_from_block_dir(
            &user_blocks_dir.join(block_ids[2].to_string()),
            metric_name
        ),
        vec![(expected_metric, vec![(start_ms + 4 * hour_ms, 4.0)])]
    );

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceInMemorySeries_ShouldFailIngestingSamplesOlderThanActiveSeriesIdleTimeoutAfterEarlyCompaction
#[tokio::test]
async fn test_compact_blocks_to_reduce_in_memory_series_rejects_old_samples_after_early_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.blocks.tsdb.early_head_compaction_min_in_memory_series = 1;
    cfg.blocks
        .tsdb
        .early_head_compaction_min_estimated_series_reduction_percentage = 0;

    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let start_ms = parse_rfc3339_ms("2023-06-24T00:00:00Z");
    let min_ms = 60_i64 * 1000;

    push_series_to_ingester(
        &ing,
        USER_ID,
        &[(&[("__name__", "metric_1")], &[(start_ms, 1.0)])],
    )
    .await
    .unwrap();
    push_series_to_ingester(
        &ing,
        USER_ID,
        &[(&[("__name__", "metric_1")], &[(start_ms + 20 * min_ms, 2.0)])],
    )
    .await
    .unwrap();

    let comp_time = UNIX_EPOCH + Duration::from_millis((start_ms + 30 * min_ms) as u64);
    ing.compact_blocks_to_reduce_in_memory_series(comp_time).await;

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    let block_ids = list_blocks_in_dir(&user_blocks_dir);
    assert_eq!(block_ids.len(), 1);
    assert_eq!(
        read_metric_samples_from_block_dir(
            &user_blocks_dir.join(block_ids[0].to_string()),
            "metric_1"
        ),
        vec![(
            BTreeMap::from([("__name__".to_string(), "metric_1".to_string())]),
            vec![(start_ms, 1.0)]
        )]
    );

    let err = push_series_to_ingester(
        &ing,
        USER_ID,
        &[(&[("__name__", "metric_2")], &[(start_ms, 1.0)])],
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("the sample has been rejected because its timestamp is too old"),
        "unexpected error: {err}"
    );

    push_series_to_ingester(
        &ing,
        USER_ID,
        &[(&[("__name__", "metric_2")], &[(start_ms + 20 * min_ms, 2.0)])],
    )
    .await
    .unwrap();
    push_series_to_ingester(
        &ing,
        USER_ID,
        &[(&[("__name__", "metric_1")], &[(start_ms + 30 * min_ms, 3.0)])],
    )
    .await
    .unwrap();

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceInMemorySeries_Concurrency
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_compact_blocks_to_reduce_in_memory_series_concurrency() {
    const NUM_RUNS: usize = 3;
    const NUM_SERIES: usize = 50;
    const NUM_SAMPLES_PER_SERIES: usize = 120 * 10;
    const NUM_WRITERS: usize = 5;
    const NUM_READERS: usize = 5;

    for run in 0..NUM_RUNS {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = default_ingester_test_config(tmp.path());
        cfg.ingester.active_series.active_series_metrics_enabled = true;
        cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::ZERO;
        cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
        cfg.blocks.tsdb.early_head_compaction_min_in_memory_series = 1;
        cfg.blocks
            .tsdb
            .early_head_compaction_min_estimated_series_reduction_percentage = 0;

        let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
        ing.set_head_compaction_interval_jitter_enabled(false);
        start_and_wait_healthy(&ing, &ring).await;

        let start_ms = unix_ms(SystemTime::now());
        let writer_times = Arc::new(Mutex::new(vec![0i64; NUM_WRITERS]));
        let start_early = Arc::new(tokio::sync::Notify::new());
        let early_started = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut writer_handles = Vec::with_capacity(NUM_WRITERS);
        for wid in 0..NUM_WRITERS {
            let ing_c = Arc::clone(&ing);
            let wt_c = Arc::clone(&writer_times);
            let se_c = Arc::clone(&start_early);
            let es_c = Arc::clone(&early_started);
            writer_handles.push(tokio::spawn(async move {
                let from_id = wid * (NUM_SERIES / NUM_WRITERS);
                let to_id = from_id + (NUM_SERIES / NUM_WRITERS) - 1;
                for s_idx in 0..NUM_SAMPLES_PER_SERIES {
                    let ts = start_ms + s_idx as i64;
                    let names: Vec<String> = (from_id..=to_id)
                        .map(|id| format!("series_{id:05}"))
                        .collect();
                    let mut ts_list = Vec::with_capacity(names.len());
                    for n in &names {
                        ts_list.push(cortexpb::TimeSeries {
                            labels: vec![cortexpb::LabelPair::from_strs("__name__", n)],
                            samples: vec![cortexpb::Sample {
                                value: s_idx as f64,
                                timestamp_ms: ts,
                            }],
                            ..Default::default()
                        });
                    }
                    let req = cortexpb::WriteRequest {
                        timeseries: ts_list,
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    };
                    ing_c.push_grpc(USER_ID, &req).await.unwrap();
                    wt_c.lock()[wid] = ts;
                    if wid == 0 && s_idx == 200 {
                        es_c.store(true, Ordering::Release);
                        se_c.notify_waiters();
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }));
        }

        let mut reader_handles = Vec::with_capacity(NUM_READERS);
        for _ in 0..NUM_READERS {
            let ing_c = Arc::clone(&ing);
            let sf_c = Arc::clone(&stop_flag);
            reader_handles.push(tokio::spawn(async move {
                let md = MetadataMap::new();
                while !sf_c.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if sf_c.load(Ordering::Acquire) {
                        break;
                    }
                    let res = run_test_query(
                        &ing_c,
                        &md,
                        USER_ID,
                        ingesterpb::MatchType::RegexMatch,
                        "__name__",
                        "series_.*",
                    )
                    .await
                    .unwrap();
                    for (_, vals) in res {
                        for (s_idx, &(ts, val)) in vals.iter().enumerate() {
                            assert_eq!(ts, start_ms + s_idx as i64);
                            assert_eq!(val, s_idx as f64);
                        }
                    }
                }
            }));
        }

        let compactor_handle = {
            let ing_c = Arc::clone(&ing);
            let wt_c = Arc::clone(&writer_times);
            let se_c = Arc::clone(&start_early);
            let es_c = Arc::clone(&early_started);
            let sf_c = Arc::clone(&stop_flag);
            tokio::spawn(async move {
                while !es_c.load(Ordering::Acquire) && !sf_c.load(Ordering::Acquire) {
                    tokio::select! {
                        _ = se_c.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
                    }
                }
                while !sf_c.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if sf_c.load(Ordering::Acquire) {
                        break;
                    }
                    let lowest = *wt_c.lock().iter().min().unwrap();
                    if lowest == 0 {
                        continue;
                    }
                    let t = UNIX_EPOCH + Duration::from_millis(lowest as u64);
                    ing_c.compact_blocks_to_reduce_in_memory_series(t).await;
                }
            })
        };

        for h in writer_handles {
            h.await.unwrap();
        }
        stop_flag.store(true, Ordering::Release);
        for h in reader_handles {
            h.await.unwrap();
        }
        compactor_handle.await.unwrap();

        let blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
        assert!(
            list_blocks_in_dir(&blocks_dir).len() > 1,
            "run {run}: expected > 1 block produced by early compaction"
        );

        let md = MetadataMap::new();
        let res = run_test_query(
            &ing,
            &md,
            USER_ID,
            ingesterpb::MatchType::RegexMatch,
            "__name__",
            "series_.*",
        )
        .await
        .unwrap();
        assert_eq!(res.len(), NUM_SERIES);
        for (idx, (lbls, vals)) in res.iter().enumerate() {
            assert_eq!(
                lbls.get("__name__").map(String::as_str),
                Some(format!("series_{idx:05}").as_str())
            );
            assert_eq!(vals.len(), NUM_SAMPLES_PER_SERIES);
            for (s_idx, &(ts, val)) in vals.iter().enumerate() {
                assert_eq!(ts, start_ms + s_idx as i64);
                assert_eq!(val, s_idx as f64);
            }
        }

        let _ = ing.stop().await;
        ring_svc.stop_async();
        let _ = ring_svc.await_terminated().await;
    }
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceOwnedSeries_DisabledByDefault
#[tokio::test]
async fn test_compact_blocks_to_reduce_owned_series_disabled_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.use_ingester_owned_series_for_limits = true;

    let limits = default_limits_test_config();
    let (ing, ring, ring_svc) =
        prepare_ingester_with_blocks_storage(cfg, Some(limits), None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let now = SystemTime::now();
    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    for sid in 0..10 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(&ing, USER_ID, &[(&[("__name__", &name)], &[(unix_ms(now), 0.0)])])
            .await
            .unwrap();
    }

    ing.get_tsdb(USER_ID)
        .unwrap()
        .active_series
        .purge(now + Duration::from_secs(30 * 60));

    ing.compact_blocks_to_reduce_per_tenant_owned_series(now + Duration::from_secs(30 * 60))
        .await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 0);

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceOwnedSeries_TriggersWhenThresholdExceeded
#[tokio::test]
async fn test_compact_blocks_to_reduce_owned_series_triggers_when_threshold_exceeded() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.use_ingester_owned_series_for_limits = true;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 20;
    limits.early_head_compaction_min_estimated_series_reduction_percentage = 10;

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        2,
        &cfg,
        limits.clone(),
    )
    .await;
    let ing0 = &ingesters[0].0;
    let user_blocks_dir = Path::new(&ing0.blocks_cfg.tsdb.dir).join(USER_ID);
    let now = SystemTime::now();

    for sid in 0..12 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(ing0, USER_ID, &[(&[("__name__", &name)], &[(unix_ms(now), 0.0)])])
            .await
            .unwrap();
    }

    let db0 = ing0.get_tsdb(USER_ID).unwrap();
    db0.active_series.purge(now + Duration::from_secs(30 * 60));

    let owned_state = db0.owned_series_state();
    assert_eq!(owned_state.owned_series_count, 12);
    let local_threshold = ing0
        .limiter
        .convert_global_to_local_limit(USER_ID, limits.early_head_compaction_owned_series_threshold as usize);
    assert_eq!(local_threshold, 10);
    assert!(owned_state.owned_series_count < limits.early_head_compaction_owned_series_threshold as usize);
    assert!(owned_state.owned_series_count >= local_threshold);

    ing0.compact_blocks_to_reduce_per_tenant_owned_series(now + Duration::from_secs(30 * 60))
        .await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceOwnedSeries_RespectsCooldown
#[tokio::test]
async fn test_compact_blocks_to_reduce_owned_series_respects_cooldown() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.use_ingester_owned_series_for_limits = true;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 20;
    limits.early_head_compaction_min_estimated_series_reduction_percentage = 10;
    limits.creation_grace_period = ModelDuration::from_hours(24);

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        2,
        &cfg,
        limits,
    )
    .await;
    let ing0 = &ingesters[0].0;
    let user_blocks_dir = Path::new(&ing0.blocks_cfg.tsdb.dir).join(USER_ID);
    let now = SystemTime::now();

    for sid in 0..12 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(ing0, USER_ID, &[(&[("__name__", &name)], &[(unix_ms(now), 0.0)])])
            .await
            .unwrap();
    }

    let db0 = ing0.get_tsdb(USER_ID).unwrap();
    db0.active_series.purge(now + Duration::from_secs(30 * 60));

    // First compaction triggers
    ing0.compact_blocks_to_reduce_per_tenant_owned_series(now + Duration::from_secs(30 * 60))
        .await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);

    // Push more series
    for sid in 12..24 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(
            ing0,
            USER_ID,
            &[(&[("__name__", &name)], &[(unix_ms(now + Duration::from_secs(40 * 60)), 0.0)])],
        )
        .await
        .unwrap();
    }
    db0.active_series.purge(now + Duration::from_secs(70 * 60));

    // Within cooldown (40m - 30m = 10m < 20m) -> skipped
    ing0.compact_blocks_to_reduce_per_tenant_owned_series(now + Duration::from_secs(40 * 60))
        .await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);

    // Past cooldown (60m - 30m = 30m >= 20m) -> triggers
    ing0.compact_blocks_to_reduce_per_tenant_owned_series(now + Duration::from_secs(60 * 60))
        .await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 2);

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksToReduceOwnedSeries_RequiresOwnedSeriesForLimits
#[tokio::test]
async fn test_compact_blocks_to_reduce_owned_series_requires_owned_series_for_limits() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.use_ingester_owned_series_for_limits = false;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 5;

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        2,
        &cfg,
        limits,
    )
    .await;
    let ing0 = &ingesters[0].0;
    let user_blocks_dir = Path::new(&ing0.blocks_cfg.tsdb.dir).join(USER_ID);
    let now = SystemTime::now();

    for sid in 0..12 {
        let name = format!("metric_{sid}");
        push_series_to_ingester(ing0, USER_ID, &[(&[("__name__", &name)], &[(unix_ms(now), 0.0)])])
            .await
            .unwrap();
    }
    ing0.get_tsdb(USER_ID)
        .unwrap()
        .active_series
        .purge(now + Duration::from_secs(30 * 60));

    ing0.compact_blocks_to_reduce_per_tenant_owned_series(now + Duration::from_secs(30 * 60))
        .await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 0);

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldNotCompactWhenDisabled
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);

    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;
    let (owned_name, non_owned_name, min_hash) = pick_owned_and_non_owned_series(USER_ID);

    for name in [owned_name, non_owned_name] {
        push_series_to_ingester(&ing, USER_ID, &[(&[("__name__", name)], &[(t1, 1.0)])])
            .await
            .unwrap();
        push_series_to_ingester(&ing, USER_ID, &[(&[("__name__", name)], &[(t2, 2.0)])])
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    assert_eq!(db.db.head().num_series(), 2);

    db.set_owned_token_ranges(vec![0, min_hash]);
    assert!(db.recompute_owned_series(0, "test"));
    assert_eq!(db.owned_series_state().owned_series_count, 1);
    assert_eq!(db.pending_non_owned_refs_len(), 0);

    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    assert!(list_blocks_in_dir(&user_blocks_dir).is_empty());

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldNotCompactWhenNoPendingCompaction
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_no_pending() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.early_compaction_non_owned_series_enabled = true;
    cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::ZERO;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 1;

    let (ing, ring, ring_svc) =
        prepare_ingester_with_blocks_storage(cfg, Some(limits), None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    push_series_to_ingester(&ing, USER_ID, &[(&[("__name__", "metric_1")], &[(t1, 1.0)])])
        .await
        .unwrap();

    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    assert!(list_blocks_in_dir(&user_blocks_dir).is_empty());

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldFlushDataToBlock
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_flushes_data_to_block() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.early_compaction_non_owned_series_enabled = true;
    cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::ZERO;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 1;

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        1,
        &cfg,
        limits,
    )
    .await;
    let ing = &ingesters[0].0;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;
    let (owned_name, non_owned_name, min_hash) = pick_owned_and_non_owned_series(USER_ID);

    for name in [owned_name, non_owned_name] {
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t1, 1.0)])])
            .await
            .unwrap();
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t2, 2.0)])])
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    assert_eq!(db.db.head().num_series(), 2);

    db.set_owned_token_ranges(vec![0, min_hash]);
    assert!(db.recompute_owned_series(0, "test"));
    assert_eq!(db.owned_series_state().owned_series_count, 1);

    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    let block_ids = list_blocks_in_dir(&user_blocks_dir);
    assert_eq!(block_ids.len(), 1);

    assert_eq!(
        read_metric_samples_from_block_dir(
            &user_blocks_dir.join(block_ids[0].to_string()),
            non_owned_name
        ),
        vec![(
            BTreeMap::from([("__name__".to_string(), non_owned_name.to_string())]),
            vec![(t1, 1.0), (t2, 2.0)]
        )]
    );
    assert_eq!(db.db.head().num_series(), 1);

    // Second call creates no new block
    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);
    assert_eq!(db.last_early_compaction_ms(), 0);

    // Non-owned series still queryable from local block
    let md = MetadataMap::new();
    let res = run_test_query(
        ing,
        &md,
        USER_ID,
        ingesterpb::MatchType::Equal,
        "__name__",
        non_owned_name,
    )
    .await
    .unwrap();
    assert_eq!(
        res,
        vec![(
            BTreeMap::from([("__name__".to_string(), non_owned_name.to_string())]),
            vec![(t1, 1.0), (t2, 2.0)]
        )]
    );

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldFlushOnlyNonOwnedSeries
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_flushes_only_non_owned() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.early_compaction_non_owned_series_enabled = true;
    cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::ZERO;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 1;

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        1,
        &cfg,
        limits,
    )
    .await;
    let ing = &ingesters[0].0;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;
    let (owned_name, non_owned_name, min_hash) = pick_owned_and_non_owned_series(USER_ID);

    for name in [owned_name, non_owned_name] {
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t1, 1.0)])])
            .await
            .unwrap();
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t2, 2.0)])])
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    assert_eq!(db.db.head().num_series(), 2);

    db.set_owned_token_ranges(vec![0, min_hash]);
    assert!(db.recompute_owned_series(0, "test"));
    assert_eq!(db.owned_series_state().owned_series_count, 1);

    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    assert_eq!(db.db.head().num_series(), 1);

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    let block_ids = list_blocks_in_dir(&user_blocks_dir);
    assert_eq!(block_ids.len(), 1);
    let block_dir = user_blocks_dir.join(block_ids[0].to_string());

    assert_eq!(
        read_metric_samples_from_block_dir(&block_dir, non_owned_name),
        vec![(
            BTreeMap::from([("__name__".to_string(), non_owned_name.to_string())]),
            vec![(t1, 1.0), (t2, 2.0)]
        )]
    );
    assert!(read_metric_samples_from_block_dir(&block_dir, owned_name).is_empty());

    let md = MetadataMap::new();
    for name in [owned_name, non_owned_name] {
        let res = run_test_query(
            ing,
            &md,
            USER_ID,
            ingesterpb::MatchType::Equal,
            "__name__",
            name,
        )
        .await
        .unwrap();
        assert_eq!(
            res,
            vec![(
                BTreeMap::from([("__name__".to_string(), name.to_string())]),
                vec![(t1, 1.0), (t2, 2.0)]
            )]
        );
    }

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldHandleOOOSamples
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_handles_ooo_samples() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.early_compaction_non_owned_series_enabled = true;
    cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::ZERO;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 1;
    limits.out_of_order_time_window = ModelDuration::from_hours(1);

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        1,
        &cfg,
        limits,
    )
    .await;
    let ing = &ingesters[0].0;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;
    let t_ooo = t1 - 100;
    let (owned_name, non_owned_name, min_hash) = pick_owned_and_non_owned_series(USER_ID);

    for name in [owned_name, non_owned_name] {
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t1, 1.0)])])
            .await
            .unwrap();
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t2, 2.0)])])
            .await
            .unwrap();
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t_ooo, 0.5)])])
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    assert_eq!(db.db.head().num_series(), 2);

    db.set_owned_token_ranges(vec![0, min_hash]);
    assert!(db.recompute_owned_series(0, "test"));
    assert_eq!(db.owned_series_state().owned_series_count, 1);

    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    assert_eq!(db.db.head().num_series(), 1);

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    let block_ids = list_blocks_in_dir(&user_blocks_dir);
    assert_eq!(block_ids.len(), 2);

    let mut ooo_block_dir = None;
    let mut selected_block_dir = None;
    for bid in &block_ids {
        let bdir = user_blocks_dir.join(bid.to_string());
        let blk = mimir_tsdb::block::Block::open(&bdir).unwrap();
        let meta = blk.meta();
        if meta.compaction.from_out_of_order() {
            ooo_block_dir = Some(bdir);
        } else if meta.compaction.from_selected_series() {
            selected_block_dir = Some(bdir);
        } else {
            panic!("unexpected block hints: {:?}", meta.compaction.hints);
        }
    }
    let ooo_dir = ooo_block_dir.expect("expected OOO block");
    let sel_dir = selected_block_dir.expect("expected selected-series block");

    for name in [owned_name, non_owned_name] {
        assert_eq!(
            read_metric_samples_from_block_dir(&ooo_dir, name),
            vec![(
                BTreeMap::from([("__name__".to_string(), name.to_string())]),
                vec![(t_ooo, 0.5)]
            )]
        );
    }

    assert!(read_metric_samples_from_block_dir(&sel_dir, owned_name).is_empty());
    assert_eq!(
        read_metric_samples_from_block_dir(&sel_dir, non_owned_name),
        vec![(
            BTreeMap::from([("__name__".to_string(), non_owned_name.to_string())]),
            vec![(t1, 1.0), (t2, 2.0)]
        )]
    );

    let md = MetadataMap::new();
    for name in [owned_name, non_owned_name] {
        let res = run_test_query(
            ing,
            &md,
            USER_ID,
            ingesterpb::MatchType::Equal,
            "__name__",
            name,
        )
        .await
        .unwrap();
        assert_eq!(
            res,
            vec![(
                BTreeMap::from([("__name__".to_string(), name.to_string())]),
                vec![(t_ooo, 0.5), (t1, 1.0), (t2, 2.0)]
            )]
        );
    }

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldRespectGracePeriod
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_respects_grace_period() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.early_compaction_non_owned_series_enabled = true;
    cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::from_hours(1);
    cfg.ingester.early_compaction_non_owned_series_max_grace_period = GoDuration::ZERO;

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 1;

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        1,
        &cfg,
        limits,
    )
    .await;
    let ing = &ingesters[0].0;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;
    let (owned_name, non_owned_name, min_hash) = pick_owned_and_non_owned_series(USER_ID);

    for name in [owned_name, non_owned_name] {
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t1, 1.0)])])
            .await
            .unwrap();
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t2, 2.0)])])
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    db.set_owned_token_ranges(vec![0, min_hash]);
    assert!(db.recompute_owned_series(0, "test"));
    assert_eq!(db.owned_series_state().owned_series_count, 1);

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    assert!(list_blocks_in_dir(&user_blocks_dir).is_empty());
    assert_eq!(db.db.head().num_series(), 2);

    // Backdate pending refs by 2 hours
    db.backdate_pending_non_owned_refs(SystemTime::now() - Duration::from_secs(2 * 3600));
    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);
    assert_eq!(db.db.head().num_series(), 1);

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldHandleScaleUp
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_handles_scale_up() {
    const NUM_SERIES: usize = 10_000;
    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;

    async fn setup_scenario(
        root: &Path,
        ingesters_per_zone: usize,
        max_grace: GoDuration,
        t1: i64,
        t2: i64,
    ) -> (
        Vec<(Arc<Ingester>, Arc<Ring>, Arc<ManagedBasicService>)>,
        usize,
    ) {
        let mut cfg = default_ingester_test_config(root);
        cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
        cfg.ingester.track_ingester_owned_series = true;
        cfg.ingester.early_compaction_non_owned_series_enabled = true;
        cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::ZERO;
        cfg.ingester.early_compaction_non_owned_series_max_grace_period = max_grace;
        cfg.ingester.active_series.active_series_metrics_enabled = true;
        cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);
        cfg.ingester.use_ingester_owned_series_for_limits = true;

        let mut limits = default_limits_test_config();
        limits.early_head_compaction_owned_series_threshold = 30_000;

        let ingesters = setup_test_ingester_ring(
            root,
            &["zone-a", "zone-b", "zone-c"],
            ingesters_per_zone,
            &cfg,
            limits,
        )
        .await;
        let ing = &ingesters[0].0;

        // Batch push for fast setup
        let mut batch1 = Vec::with_capacity(NUM_SERIES);
        let mut batch2 = Vec::with_capacity(NUM_SERIES);
        for i in 0..NUM_SERIES {
            let name = format!("metric_{i}");
            batch1.push(cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_strs("__name__", &name)],
                samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: t1 }],
                ..Default::default()
            });
            batch2.push(cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_strs("__name__", &name)],
                samples: vec![cortexpb::Sample { value: 2.0, timestamp_ms: t2 }],
                ..Default::default()
            });
        }
        for chunk in batch1.chunks(1000) {
            ing.push_grpc(
                USER_ID,
                &cortexpb::WriteRequest {
                    timeseries: chunk.to_vec(),
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }
        for chunk in batch2.chunks(1000) {
            ing.push_grpc(
                USER_ID,
                &cortexpb::WriteRequest {
                    timeseries: chunk.to_vec(),
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }

        let db = ing.get_tsdb(USER_ID).unwrap();
        assert_eq!(db.db.head().num_series(), NUM_SERIES as u64);

        db.set_owned_token_ranges(vec![0, u32::MAX / (ingesters_per_zone as u32)]);
        assert!(db.recompute_owned_series(0, "test"));
        let owned = db.owned_series_state().owned_series_count;
        assert!(owned > 0 && owned < NUM_SERIES);
        (ingesters, owned)
    }

    // Subcase 1: pre-scale-up skips eviction
    {
        let tmp = tempfile::tempdir().unwrap();
        let (ingesters, _) = setup_scenario(tmp.path(), 2, GoDuration::ZERO, t1, t2).await;
        let ing = &ingesters[0].0;
        let db = ing.get_tsdb(USER_ID).unwrap();
        let blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
        ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
        assert!(list_blocks_in_dir(&blocks_dir).is_empty());
        assert_eq!(db.db.head().num_series(), NUM_SERIES as u64);
        stop_ring_ingesters(ingesters).await;
    }

    // Subcase 2: post-scale-up triggers eviction
    {
        let tmp = tempfile::tempdir().unwrap();
        let (ingesters, post_owned) = setup_scenario(tmp.path(), 4, GoDuration::ZERO, t1, t2).await;
        let ing = &ingesters[0].0;
        let db = ing.get_tsdb(USER_ID).unwrap();
        let blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);

        let local_thresh = ing.limiter.convert_global_to_local_limit(USER_ID, 30_000);
        assert!(post_owned < local_thresh);

        ing.compact_blocks_to_reduce_per_tenant_owned_series(SystemTime::now())
            .await;
        assert!(list_blocks_in_dir(&blocks_dir).is_empty());
        assert_eq!(db.db.head().num_series(), NUM_SERIES as u64);

        ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
        assert_eq!(list_blocks_in_dir(&blocks_dir).len(), 1);
        assert_eq!(db.db.head().num_series(), post_owned as u64);
        stop_ring_ingesters(ingesters).await;
    }

    // Subcase 3: max grace period triggers eviction when threshold gate is closed
    {
        let tmp = tempfile::tempdir().unwrap();
        let (ingesters, _) = setup_scenario(tmp.path(), 2, GoDuration::from_mins(1), t1, t2).await;
        let ing = &ingesters[0].0;
        let db = ing.get_tsdb(USER_ID).unwrap();
        let blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);

        ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
        assert!(list_blocks_in_dir(&blocks_dir).is_empty());
        assert_eq!(db.db.head().num_series(), NUM_SERIES as u64);

        db.backdate_pending_non_owned_refs(SystemTime::now() - Duration::from_secs(120));
        ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
        assert_eq!(list_blocks_in_dir(&blocks_dir).len(), 1);
        assert!(db.db.head().num_series() < NUM_SERIES as u64);
        stop_ring_ingesters(ingesters).await;
    }
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_StaleRefsAfterPriorEviction
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_stale_refs() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.early_compaction_non_owned_series_enabled = true;
    cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::ZERO;
    cfg.ingester.early_compaction_non_owned_series_max_grace_period = GoDuration::ZERO;
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.use_ingester_owned_series_for_limits = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 1;
    limits.early_head_compaction_min_estimated_series_reduction_percentage = 0;

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        1,
        &cfg,
        limits,
    )
    .await;
    let ing = &ingesters[0].0;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;
    let (owned_name, non_owned_name, min_hash) = pick_owned_and_non_owned_series(USER_ID);

    for name in [owned_name, non_owned_name] {
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t1, 1.0)])])
            .await
            .unwrap();
        push_series_to_ingester(ing, USER_ID, &[(&[("__name__", name)], &[(t2, 2.0)])])
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(USER_ID).unwrap();
    assert_eq!(db.db.head().num_series(), 2);
    db.set_owned_token_ranges(vec![0, min_hash]);
    assert!(db.recompute_owned_series(0, "test"));
    assert_eq!(db.owned_series_state().owned_series_count, 1);
    assert_eq!(db.pending_non_owned_refs_len(), 1);

    let t_recent = unix_ms(SystemTime::now());
    push_series_to_ingester(
        ing,
        USER_ID,
        &[(&[("__name__", owned_name)], &[(t_recent, 3.0)])],
    )
    .await
    .unwrap();

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    ing.compact_blocks_to_reduce_per_tenant_owned_series(SystemTime::now())
        .await;
    let blocks_after_old = list_blocks_in_dir(&user_blocks_dir);
    assert!(!blocks_after_old.is_empty());
    assert_eq!(db.db.head().num_series(), 1);
    assert_eq!(db.pending_non_owned_refs_len(), 1);

    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;
    assert_eq!(list_blocks_in_dir(&user_blocks_dir), blocks_after_old);
    assert_eq!(db.db.head().num_series(), 1);

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_early_compaction_test.go::TestIngester_compactBlocksDueToNonOwnedSeries_ShouldEvictAgedRefsDespiteFresherOnes
#[tokio::test]
async fn test_compact_blocks_due_to_non_owned_series_evicts_aged_refs_despite_fresher_ones() {
    const OLDER_COUNT: usize = 5;
    const NEWER_COUNT: usize = 5;
    const TOTAL: usize = OLDER_COUNT + NEWER_COUNT;

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
    cfg.ingester.track_ingester_owned_series = true;
    cfg.ingester.early_compaction_non_owned_series_enabled = true;
    cfg.ingester.early_compaction_non_owned_series_min_grace_period = GoDuration::from_hours(1);
    cfg.ingester.early_compaction_non_owned_series_max_grace_period = GoDuration::ZERO;
    cfg.ingester.active_series.active_series_metrics_enabled = true;
    cfg.ingester.use_ingester_owned_series_for_limits = true;
    cfg.ingester.active_series.active_series_metrics_idle_timeout = GoDuration::from_mins(20);

    let mut limits = default_limits_test_config();
    limits.early_head_compaction_owned_series_threshold = 1;

    let ingesters = setup_test_ingester_ring(
        tmp.path(),
        &["zone-a", "zone-b", "zone-c"],
        1,
        &cfg,
        limits,
    )
    .await;
    let ing = &ingesters[0].0;

    let t1 = parse_rfc3339_ms("2026-05-05T00:00:00Z");
    let t2 = t1 + 1;

    let push_batch = |start: usize, end: usize| {
        let ing_c = Arc::clone(ing);
        async move {
            for i in start..end {
                let name = format!("metric_{i}");
                push_series_to_ingester(&ing_c, USER_ID, &[(&[("__name__", &name)], &[(t1, 1.0)])])
                    .await
                    .unwrap();
                push_series_to_ingester(&ing_c, USER_ID, &[(&[("__name__", &name)], &[(t2, 2.0)])])
                    .await
                    .unwrap();
            }
        }
    };

    // Phase 1
    push_batch(0, OLDER_COUNT).await;
    let db = ing.get_tsdb(USER_ID).unwrap();
    assert_eq!(db.db.head().num_series(), OLDER_COUNT as u64);

    db.set_owned_token_ranges(vec![]);
    assert!(db.recompute_owned_series(0, "test"));
    assert_eq!(db.owned_series_state().owned_series_count, 0);
    assert_eq!(db.pending_non_owned_refs_len(), OLDER_COUNT);

    let backdated = SystemTime::now() - Duration::from_secs(2 * 3600);
    db.backdate_pending_non_owned_refs(backdated);

    // Phase 2
    push_batch(OLDER_COUNT, TOTAL).await;
    assert_eq!(db.db.head().num_series(), TOTAL as u64);
    assert!(db.recompute_owned_series(0, "test"));

    let snap = db.pending_non_owned_refs_snapshot();
    assert_eq!(snap.len(), TOTAL);
    let older_retained = snap.values().filter(|&&ts| ts == backdated).count();
    let newer_stamped = snap.values().filter(|&&ts| ts > backdated).count();
    assert_eq!(older_retained, OLDER_COUNT);
    assert_eq!(newer_stamped, NEWER_COUNT);

    let user_blocks_dir = Path::new(&ing.blocks_cfg.tsdb.dir).join(USER_ID);
    ing.compact_blocks_due_to_non_owned_series(Duration::ZERO).await;

    assert_eq!(list_blocks_in_dir(&user_blocks_dir).len(), 1);
    assert_eq!(db.db.head().num_series(), NEWER_COUNT as u64);

    let snap_after = db.pending_non_owned_refs_snapshot();
    assert_eq!(snap_after.len(), NEWER_COUNT);
    for ts in snap_after.values() {
        assert!(*ts > backdated);
    }

    stop_ring_ingesters(ingesters).await;
}

// Upstream: pkg/ingester/ingester_idle_tsdb_test.go::TestIngester_ReopenedTSDBWithoutInOrderSamplesIsNotIdle
#[tokio::test]
async fn test_ingester_reopened_tsdb_without_in_order_samples_is_not_idle() {
    let cases = [
        (
            "recent out-of-order sample",
            ModelDuration::from_hours(1),
            Duration::from_secs(40 * 60),
            Duration::from_secs(13 * 3600),
        ),
        (
            "out-of-order sample older than the close idle timeout",
            ModelDuration::from_hours(6),
            Duration::from_secs(3 * 3600),
            Duration::from_secs(2 * 3600),
        ),
    ];

    for (name, ooo_window, ooo_age, close_idle_timeout) in cases {
        const NUM_SERIES: usize = 3000;
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("tsdb_data");
        let bucket_dir = tmp.path().join("bucket");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&bucket_dir).unwrap();

        let make_cfg = |ring_kv: Arc<dyn KvClient>| {
            let mut c = default_ingester_test_config(tmp.path());
            c.ring_kv = ring_kv;
            c.blocks.tsdb.dir = data_dir.to_string_lossy().to_string();
            c.blocks.bucket.backend_cfg.filesystem.dir =
                bucket_dir.to_string_lossy().to_string();
            c.blocks.tsdb.ship_interval = GoDuration::ZERO;
            c.blocks.tsdb.close_idle_tsdb_when_shipping_disabled = true;
            c.blocks.tsdb.close_idle_tsdb_timeout =
                GoDuration::from_nanos(close_idle_timeout.as_nanos() as i64);
            c.blocks.tsdb.head_compaction_idle_timeout = GoDuration::from_hours(1);
            c.blocks.tsdb.head_compaction_interval = GoDuration::from_hours(1);
            c.blocks.tsdb.wal_segment_size_bytes = 32 * 1024;
            c
        };

        let mut limits = default_limits_test_config();
        limits.out_of_order_time_window = ooo_window;
        limits.max_global_exemplars_per_user = 1000;

        let now = SystemTime::now();
        let sample_ts = unix_ms(now - Duration::from_secs(30 * 60));
        let ooo_ts = unix_ms(now - ooo_age);

        // First ingester run
        {
            let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
            let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(
                make_cfg(kv),
                Some(limits.clone()),
                None,
            )
            .await;
            start_and_wait_healthy(&ing, &ring).await;

            // Series 0 with exemplar
            ing.push_grpc(
                USER_ID,
                &cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![
                            cortexpb::LabelPair::from_strs("__name__", "test"),
                            cortexpb::LabelPair::from_strs("series", "00000"),
                        ],
                        samples: vec![cortexpb::Sample {
                            value: 0.0,
                            timestamp_ms: sample_ts,
                        }],
                        exemplars: vec![cortexpb::Exemplar {
                            labels: vec![cortexpb::LabelPair::from_strs("traceID", "abc")],
                            value: 1.0,
                            timestamp_ms: sample_ts,
                        }],
                        ..Default::default()
                    }],
                    source: cortexpb::write_request::SourceEnum::Api as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            // Series 1..NUM_SERIES
            let mut rest = Vec::with_capacity(NUM_SERIES - 1);
            for n in 1..NUM_SERIES {
                rest.push(cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "test"),
                        cortexpb::LabelPair::from_strs("series", format!("{n:05}")),
                    ],
                    samples: vec![cortexpb::Sample {
                        value: n as f64,
                        timestamp_ms: sample_ts,
                    }],
                    ..Default::default()
                });
            }
            for chunk in rest.chunks(500) {
                ing.push_grpc(
                    USER_ID,
                    &cortexpb::WriteRequest {
                        timeseries: chunk.to_vec(),
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }

            let db = ing.get_tsdb(USER_ID).unwrap();
            let refs = db.db.head().postings_for_label_value("__name__", "test");
            db.db.compact_ooo_head().unwrap();
            db.db.compact_selected_series(&refs).unwrap();
            assert_eq!(
                db.db.head().num_series(),
                1,
                "case {name}: the series with an exemplar should stay in the head"
            );

            // Idle compaction checkpoints WAL and cleans up series 0
            db.set_last_update(now - Duration::from_secs(2 * 3600));
            ing.compact_blocks(false, 0, None).await;

            // Late out-of-order sample creates an OOO-only series
            push_series_to_ingester(
                &ing,
                USER_ID,
                &[(
                    &[("__name__", "test"), ("series", "late")],
                    &[(ooo_ts, 1.0)],
                )],
            )
            .await
            .unwrap();
            assert_eq!(db.db.head().max_ooo_time(), ooo_ts);

            ing.stop().await.unwrap();
            ring_svc.stop_async();
            let _ = ring_svc.await_terminated().await;
        }

        // Second ingester run on same data_dir
        {
            let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
            let (ing, ring, ring_svc) =
                prepare_ingester_with_blocks_storage(make_cfg(kv), Some(limits), None).await;
            start_and_wait_healthy(&ing, &ring).await;

            let db = ing.get_tsdb(USER_ID).unwrap();
            let dir = db.db.dir().to_path_buf();

            assert_eq!(
                db.db.head().num_series(),
                1,
                "case {name}: only the out-of-order-only series should be replayed"
            );
            assert_eq!(db.db.head().max_time(), i64::MIN);
            assert_eq!(db.db.head().max_ooo_time(), ooo_ts);
            assert!(
                !db.is_idle(SystemTime::now(), close_idle_timeout),
                "case {name}: lastUpdate must follow local blocks, not MinInt64 or old OOO"
            );

            ing.compact_blocks(false, 0, None).await;
            assert_ne!(
                ing.close_and_delete_user_tsdb_if_idle(USER_ID, close_idle_timeout)
                    .await,
                TsdbCloseCheckResult::IdleClosed
            );
            assert!(dir.exists(), "case {name}: local TSDB should not have been deleted");

            let _ = ing.stop().await;
            ring_svc.stop_async();
            let _ = ring_svc.await_terminated().await;
        }
    }
}
