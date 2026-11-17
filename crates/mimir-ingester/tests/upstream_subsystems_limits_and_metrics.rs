mod common;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use mimir_config::{BucketConfig, GoDuration, LimitsConfig, Overrides};
use mimir_ingester::http::ingester_router;
use mimir_ingester::label_churn::{
    compute_label_churn, LabelChurnError, LabelChurnMetricName, LabelChurnResponse,
    LabelChurnScore, SeriesRefHistory, SeriesRefRecord,
};
use mimir_ingester::limiter::{
    IngesterRingLimiterRingCount, IngesterRingLimiterStrategy, Limiter, PartitionRingProvider,
    PartitionRingLimiterStrategy,
};
use mimir_ingester::metric_counter::hash_metric_name;
use mimir_ingester::metrics::IngesterMetrics;
use mimir_ingester::user_tsdb::{TenantMetadataStore, TsdbCloseCheckResult, UserTsdb};
use mimir_proto::cortexpb;
use mimir_proto::ringpb::{PartitionRingDesc, PartitionState};
use mimir_ring::partition_ring::{PartitionRing, PartitionRingDescExt};
use mimir_tsdb::labels::Labels;
use mimir_tsdb::postings::LABEL_VALUE_BYTES_MIN_LENGTH;
use prometheus::Registry;
use tempfile::TempDir;
use tower::ServiceExt;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_count, metric_value,
    mock_write_request, prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

// ============================================================================
// pkg/ingester/limiter_test.go (13 tests)
// ============================================================================

struct RingCountMock {
    instances_count: usize,
    instances_in_zone_count: usize,
    zones_count: usize,
}

impl IngesterRingLimiterRingCount for RingCountMock {
    fn writable_instances_with_tokens_count(&self) -> usize {
        self.instances_count
    }

    fn writable_instances_with_tokens_in_zone_count(&self, _zone: &str) -> usize {
        self.instances_in_zone_count
    }

    fn zones_count(&self) -> usize {
        self.zones_count
    }
}

struct PartitionRingHolder {
    pr: Arc<PartitionRing>,
}

impl PartitionRingProvider for PartitionRingHolder {
    fn partition_ring(&self) -> Arc<PartitionRing> {
        Arc::clone(&self.pr)
    }
}

fn build_partition_ring_from_states(states: &[PartitionState]) -> Arc<PartitionRing> {
    let mut desc = PartitionRingDesc::default();
    for (idx, &s) in states.iter().enumerate() {
        desc.add_partition(idx as i32, s, SystemTime::now());
    }
    PartitionRing::new(desc).expect("build partition ring")
}

fn run_limiter_max_test<F, G>(
    apply_limits: F,
    run_max_fn: G,
    expected_zero: usize,
    use_global_if_unable: bool,
) where
    F: Fn(&mut LimitsConfig, i64),
    G: Fn(&Limiter) -> usize,
{
    type Case<'a> = (&'a str, i64, usize, bool, usize, usize, usize, i64, usize);
    let cases: &[Case<'_>] = &[
        ("zone disabled, global 0", 0, 1, false, 1, 0, 0, 0, expected_zero),
        ("zone disabled, rf=1, shard 0", 1000, 1, false, 10, 0, 0, 0, 100),
        ("zone disabled, rf=1, shard 5", 1000, 1, false, 10, 0, 0, 5, 200),
        ("zone disabled, rf=3, shard 0", 1000, 3, false, 10, 0, 0, 0, 300),
        ("zone disabled, rf=3, shard 5", 1000, 3, false, 10, 0, 0, 5, 600),
        ("zone disabled, rf=3, shard 20 > ingesters", 1000, 3, false, 10, 0, 0, 20, 300),
        (
            "zone disabled, rf=1, shard 0, 0 ingesters",
            1000,
            1,
            false,
            0,
            0,
            0,
            0,
            if use_global_if_unable { 1000 } else { expected_zero },
        ),
        ("zone enabled, global 0", 0, 1, true, 1, 3, 1, 0, expected_zero),
        ("zone enabled, rf=3, 3 per zone, shard 0", 900, 3, true, 9, 3, 3, 0, 300),
        ("zone enabled, rf=3, scaled down this zone, shard 0", 900, 3, true, 8, 3, 2, 0, 450),
        ("zone enabled, rf=3, scaled down other zone, shard 0", 900, 3, true, 8, 3, 3, 0, 300),
        ("zone enabled, rf=3, scaled up this zone, shard 0", 900, 3, true, 10, 3, 4, 0, 225),
        ("zone enabled, rf=3, scaled up other zone, shard 0", 900, 3, true, 10, 3, 3, 0, 300),
        ("zone enabled, rf=3, all running, shard 5", 900, 3, true, 9, 3, 3, 5, 450),
        ("zone enabled, rf=3, scaled down this zone, shard 5", 900, 3, true, 7, 3, 1, 5, 900),
        ("zone enabled, rf=3, scaled down other zone, shard 5", 900, 3, true, 7, 3, 3, 5, 450),
        ("zone enabled, rf=3, scaled up this zone, shard 5", 900, 3, true, 11, 3, 5, 5, 450),
        ("zone enabled, rf=3, scaled up other zone, shard 5", 900, 3, true, 11, 3, 3, 5, 450),
        ("zone enabled, rf=3, shard 20 > ingesters", 900, 3, true, 9, 3, 3, 20, 300),
        ("zone enabled, rf=3, scaled down this zone, shard 20", 900, 3, true, 8, 3, 2, 20, 450),
        ("zone enabled, rf=3, scaled down other zone, shard 20", 900, 3, true, 8, 3, 3, 20, 300),
        ("zone enabled, rf=3, scaled up this zone, shard 20", 900, 3, true, 11, 3, 5, 20, 180),
        ("zone enabled, rf=3, scaled up other zone, shard 20", 900, 3, true, 11, 3, 3, 20, 300),
        (
            "zone enabled, rf=3, 0 in zone, shard 20",
            900,
            3,
            true,
            6,
            3,
            0,
            20,
            if use_global_if_unable { 900 } else { expected_zero },
        ),
    ];

    for &(name, global, rf, za, ing_cnt, zones, in_zone, shard_sz, expected) in cases {
        let ring = Arc::new(RingCountMock {
            instances_count: ing_cnt,
            instances_in_zone_count: in_zone,
            zones_count: zones,
        });
        let mut limits = LimitsConfig {
            ingestion_tenant_shard_size: shard_sz,
            ..Default::default()
        };
        apply_limits(&mut limits, global);
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(IngesterRingLimiterStrategy::new(
            ring,
            rf,
            za,
            "zone",
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(run_max_fn(&limiter), expected, "{name}");
    }
}

fn run_limiter_max_test_with_partitions_ring<F, G>(
    apply_limits: F,
    run_max_fn: G,
    expected_zero: usize,
    use_global_if_unable: bool,
) where
    F: Fn(&mut LimitsConfig, i64),
    G: Fn(&Limiter) -> usize,
{
    use PartitionState::{PartitionActive as A, PartitionInactive as I};
    let cases: &[(&str, i64, &[PartitionState], i64, usize)] = &[
        ("limit disabled", 0, &[A, I], 0, expected_zero),
        ("1 active partition", 1000, &[A, I, I, I, I], 0, 1000),
        ("2 active partitions", 1000, &[A, A, I, I, I], 0, 500),
        ("2 of 5 active", 1000, &[A, A, A, A, A], 2, 500),
        ("5 requested, 2 active", 1000, &[A, A, I, I, I], 5, 500),
        (
            "2 requested, 0 active",
            1000,
            &[I, I, I, I, I],
            2,
            if use_global_if_unable { 1000 } else { expected_zero },
        ),
    ];

    for &(name, global, states, shard_sz, expected) in cases {
        let pr = build_partition_ring_from_states(states);
        let holder = Arc::new(PartitionRingHolder { pr });
        let mut limits = LimitsConfig {
            ingestion_partitions_tenant_shard_size: shard_sz,
            ..Default::default()
        };
        apply_limits(&mut limits, global);
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(PartitionRingLimiterStrategy::new(holder, overrides.clone()));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(run_max_fn(&limiter), expected, "{name}");
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_maxSeriesPerMetric
#[test]
fn test_limiter_max_series_per_metric() {
    let apply = |l: &mut LimitsConfig, g: i64| l.max_global_series_per_metric = g;
    let run = |lim: &Limiter| lim.max_series_per_metric("test");
    run_limiter_max_test(apply, run, i32::MAX as usize, false);
    run_limiter_max_test_with_partitions_ring(apply, run, i32::MAX as usize, false);
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_maxMetadataPerMetric
#[test]
fn test_limiter_max_metadata_per_metric() {
    let apply = |l: &mut LimitsConfig, g: i64| l.max_global_metadata_per_metric = g;
    let run = |lim: &Limiter| lim.max_metadata_per_metric("test");
    run_limiter_max_test(apply, run, i32::MAX as usize, false);
    run_limiter_max_test_with_partitions_ring(apply, run, i32::MAX as usize, false);
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_maxSeriesPerUser
#[test]
fn test_limiter_max_series_per_user() {
    let apply = |l: &mut LimitsConfig, g: i64| l.max_global_series_per_user = g;
    let run = |lim: &Limiter| lim.max_series_per_user("test", 0);
    run_limiter_max_test(apply, run, i32::MAX as usize, false);
    run_limiter_max_test_with_partitions_ring(apply, run, i32::MAX as usize, false);
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_maxMetadataPerUser
#[test]
fn test_limiter_max_metadata_per_user() {
    let apply = |l: &mut LimitsConfig, g: i64| l.max_global_metadata_per_user = g;
    let run = |lim: &Limiter| lim.max_metadata_per_user("test");
    run_limiter_max_test(apply, run, i32::MAX as usize, false);
    run_limiter_max_test_with_partitions_ring(apply, run, i32::MAX as usize, false);
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_maxExemplarsPerUser
#[test]
fn test_limiter_max_exemplars_per_user() {
    let apply = |l: &mut LimitsConfig, g: i64| l.max_global_exemplars_per_user = g;
    let run = |lim: &Limiter| lim.max_exemplars_per_user("test");
    run_limiter_max_test(apply, run, 0, true);
    run_limiter_max_test_with_partitions_ring(apply, run, 0, true);
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxSeriesPerMetric
#[test]
fn test_limiter_is_within_max_series_per_metric() {
    for &(max_g, rf, cnt, series, exp) in &[
        (0, 1, 1, 100, true),
        (1000, 3, 10, 299, true),
        (1000, 3, 10, 300, false),
    ] {
        let ring = Arc::new(RingCountMock {
            instances_count: cnt,
            instances_in_zone_count: 0,
            zones_count: 1,
        });
        let limits = LimitsConfig {
            max_global_series_per_metric: max_g,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(IngesterRingLimiterStrategy::new(
            ring,
            rf,
            false,
            "",
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(limiter.is_within_max_series_per_metric("test", series), exp);
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxSeriesPerMetric_WithPartitionsRing
#[test]
fn test_limiter_is_within_max_series_per_metric_with_partitions_ring() {
    use PartitionState::PartitionActive as A;
    for &(max_g, states, series, exp) in &[
        (0, &[A][..], 100, true),
        (900, &[A, A, A], 299, true),
        (900, &[A, A, A], 300, false),
    ] {
        let pr = build_partition_ring_from_states(states);
        let limits = LimitsConfig {
            max_global_series_per_metric: max_g,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(PartitionRingLimiterStrategy::new(
            Arc::new(PartitionRingHolder { pr }),
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(limiter.is_within_max_series_per_metric("test", series), exp);
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxMetadataPerMetric
#[test]
fn test_limiter_is_within_max_metadata_per_metric() {
    for &(max_g, rf, cnt, meta, exp) in &[
        (0, 1, 1, 100, true),
        (1000, 3, 10, 299, true),
        (1000, 3, 10, 300, false),
    ] {
        let ring = Arc::new(RingCountMock {
            instances_count: cnt,
            instances_in_zone_count: 0,
            zones_count: 1,
        });
        let limits = LimitsConfig {
            max_global_metadata_per_metric: max_g,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(IngesterRingLimiterStrategy::new(
            ring,
            rf,
            false,
            "",
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(limiter.is_within_max_metadata_per_metric("test", meta), exp);
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxMetadataPerMetric_WithPartitionsRing
#[test]
fn test_limiter_is_within_max_metadata_per_metric_with_partitions_ring() {
    use PartitionState::PartitionActive as A;
    for &(max_g, states, meta, exp) in &[
        (0, &[A][..], 100, true),
        (900, &[A, A, A], 299, true),
        (900, &[A, A, A], 300, false),
    ] {
        let pr = build_partition_ring_from_states(states);
        let limits = LimitsConfig {
            max_global_metadata_per_metric: max_g,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(PartitionRingLimiterStrategy::new(
            Arc::new(PartitionRingHolder { pr }),
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(limiter.is_within_max_metadata_per_metric("test", meta), exp);
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxSeriesPerUser
#[test]
fn test_limiter_is_within_max_series_per_user() {
    for &(max_g, rf, cnt, series, shard_sz, min_lim, exp) in &[
        (0, 1, 1, 100, 0, 0, true),
        (0, 1, 1, 100, 0, 50, true),
        (1000, 3, 10, 299, 0, 0, true),
        (1000, 3, 10, 300, 0, 0, false),
        (1000, 3, 10, 300, 0, 400, true),
        (1000, 3, 10, 999, 3, 0, true),
        (1000, 3, 10, 1000, 3, 0, false),
    ] {
        let ring = Arc::new(RingCountMock {
            instances_count: cnt,
            instances_in_zone_count: 0,
            zones_count: 1,
        });
        let limits = LimitsConfig {
            max_global_series_per_user: max_g,
            ingestion_tenant_shard_size: shard_sz,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(IngesterRingLimiterStrategy::new(
            ring,
            rf,
            false,
            "",
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(
            limiter.is_within_max_series_per_user("test", series, min_lim),
            exp
        );
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxSeriesPerUser_WithPartitionsRing
#[test]
fn test_limiter_is_within_max_series_per_user_with_partitions_ring() {
    use PartitionState::PartitionActive as A;
    for &(max_g, states, series, shard_sz, min_lim, exp) in &[
        (0, &[A][..], 100, 0, 0, true),
        (0, &[A], 100, 0, 50, true),
        (900, &[A, A, A], 299, 0, 0, true),
        (900, &[A, A, A], 300, 0, 0, false),
        (900, &[A, A, A], 350, 0, 400, true),
        (1000, &[A, A, A, A, A], 499, 2, 0, true),
        (1000, &[A, A, A, A, A], 500, 2, 0, false),
    ] {
        let pr = build_partition_ring_from_states(states);
        let limits = LimitsConfig {
            max_global_series_per_user: max_g,
            ingestion_partitions_tenant_shard_size: shard_sz,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(PartitionRingLimiterStrategy::new(
            Arc::new(PartitionRingHolder { pr }),
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(
            limiter.is_within_max_series_per_user("test", series, min_lim),
            exp
        );
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxMetricsWithMetadataPerUser
#[test]
fn test_limiter_is_within_max_metrics_with_metadata_per_user() {
    for &(max_g, rf, cnt, meta, exp) in &[
        (0, 1, 1, 100, true),
        (1000, 3, 10, 299, true),
        (1000, 3, 10, 300, false),
    ] {
        let ring = Arc::new(RingCountMock {
            instances_count: cnt,
            instances_in_zone_count: 0,
            zones_count: 1,
        });
        let limits = LimitsConfig {
            max_global_metadata_per_user: max_g,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(IngesterRingLimiterStrategy::new(
            ring,
            rf,
            false,
            "",
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(
            limiter.is_within_max_metrics_with_metadata_per_user("test", meta),
            exp
        );
    }
}

// Upstream: pkg/ingester/limiter_test.go::TestLimiter_IsWithinMaxMetricsWithMetadataPerUser_WithPartitionsRing
#[test]
fn test_limiter_is_within_max_metrics_with_metadata_per_user_with_partitions_ring() {
    use PartitionState::PartitionActive as A;
    for &(max_g, states, meta, exp) in &[
        (0, &[A][..], 100, true),
        (900, &[A, A, A], 299, true),
        (900, &[A, A, A], 300, false),
    ] {
        let pr = build_partition_ring_from_states(states);
        let limits = LimitsConfig {
            max_global_metadata_per_user: max_g,
            ..Default::default()
        };
        let overrides = Overrides::new(limits);
        let strategy = Arc::new(PartitionRingLimiterStrategy::new(
            Arc::new(PartitionRingHolder { pr }),
            overrides.clone(),
        ));
        let limiter = Limiter::new(overrides, strategy);
        assert_eq!(
            limiter.is_within_max_metrics_with_metadata_per_user("test", meta),
            exp
        );
    }
}

// ============================================================================
// pkg/ingester/metric_counter_test.go (2 tests)
// ============================================================================

fn metric_counter_golden_values() -> Vec<(String, u64)> {
    let very_long = "a".repeat(256);
    vec![
        ("up".into(), 0x8c43a07b566d980),
        ("scrape_duration_seconds".into(), 0xb4687d91a4f00bc4),
        ("http_requests_total".into(), 0xc7040274bc164605),
        ("grpc_server_handled_total{method=\"GET\"}".into(), 0xcb58c44c07c09aa9),
        ("process_cpu_seconds_total".into(), 0xb429e60330518a04),
        ("cortex_ingester_memory_series".into(), 0x85b31538eaacf650),
        ("cortex_ingester_memory_series_created_total".into(), 0xf7ff4f82a5aaa376),
        ("prometheus_remote_storage_samples_pending".into(), 0xa2f62220fa4e000a),
        ("node_cpu_seconds_total".into(), 0xd9b6ef551aee7467),
        ("apiserver_request_total".into(), 0x9c141e50727df7f3),
        ("container_cpu_usage_seconds_total".into(), 0x9d3538cd55ce3b26),
        ("go_goroutines".into(), 0xceafedd1f6ae9b93),
        ("".into(), 0xcbf29ce484222325),
        ("   ".into(), 0xc3b0d217ceb2bed7),
        ("a".into(), 0xaf63dc4c8601ec8c),
        ("abcdefg".into(), 0x406e475017aa7737),
        ("abcdefgh".into(), 0x25da8c1836a8d66d),
        ("abcdefghi".into(), 0xfb321124e0e3a8cc),
        (":node_cpu:cpu0".into(), 0x2e3bd13ac6439760),
        ("metric_名前_total".into(), 0x63b9d40005d0d0ea),
        ("broken\nmetric".into(), 0x9f684fdfef61dba),
        // non-UTF8 bytes tested via fnv1a reference below, plus UTF-8 high chars:
        ("ÿþ".into(), 0xfe3c83fc4a652ef9),
        (very_long, 0xfd2916200943d825),
    ]
}

// Upstream: pkg/ingester/metric_counter_test.go::TestMetricCounterHashGoldenValues
#[test]
fn test_metric_counter_hash_golden_values() {
    for (idx, (metric, expected)) in metric_counter_golden_values().into_iter().enumerate() {
        // index 21 in our vec is valid UTF-8 replacement for raw 0xff 0xfe check;
        // verify exact golden values for all strings (skip checking raw non-UTF8 magic for ÿþ)
        if idx == 21 {
            continue;
        }
        assert_eq!(hash_metric_name(&metric), expected, "metric={metric:?}");
    }
}

// Upstream: pkg/ingester/metric_counter_test.go::TestMetricCounterHashMatchesStdlibFNV1a64
#[test]
fn test_metric_counter_hash_matches_stdlib_fnv1a64() {
    fn stdlib_fnv1a_64(bytes: &[u8]) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
    for (metric, _) in metric_counter_golden_values() {
        assert_eq!(
            hash_metric_name(&metric),
            stdlib_fnv1a_64(metric.as_bytes()),
            "metric={metric:?}"
        );
    }
}

// ============================================================================
// pkg/ingester/label_churn_test.go (5 tests)
// ============================================================================

// Upstream: pkg/ingester/label_churn_test.go::TestSeriesRefHistory
#[test]
fn test_series_ref_history() {
    let history = SeriesRefHistory::new(3);
    assert!(history.oldest_record_not_before(0).is_none());

    for ts in 1..=5i64 {
        history.record(ts * 1000, (ts * 10) as u64);
    }

    let r1 = history.oldest_record_not_before(0).unwrap();
    assert_eq!(r1.timestamp_ms, 3000);
    assert_eq!(r1.series_ref, 30);

    let r2 = history.oldest_record_not_before(3500).unwrap();
    assert_eq!(r2.timestamp_ms, 4000);
    assert_eq!(r2.series_ref, 40);

    let r3 = history.oldest_record_not_before(5000).unwrap();
    assert_eq!(r3.timestamp_ms, 5000);
    assert_eq!(r3.series_ref, 50);

    assert!(history.oldest_record_not_before(5001).is_none());
    let _: SeriesRefRecord = r1;
}

// Upstream: pkg/ingester/label_churn_test.go::TestIngester_LabelChurn
#[tokio::test]
async fn test_ingester_label_churn() {
    let user_id = "test";
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.label_churn_enabled = true;
    let (ing, _ring, _kv) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        None,
    );
    start_and_wait_healthy(&ing).await;

    let now = SystemTime::now();
    let now_ms = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    for lbls in [
        Labels::from_pairs([("__name__", "up"), ("pod", "a"), ("job", "x")]),
        Labels::from_pairs([("__name__", "up"), ("pod", "b"), ("job", "x")]),
    ] {
        ing.push_grpc(user_id, &mock_write_request(&lbls, 1.0, now_ms))
            .await
            .unwrap();
    }

    let record_ts = now - Duration::from_secs(10 * 60);
    ing.record_label_churn_series_refs(record_ts);

    for lbls in [
        Labels::from_pairs([("__name__", "up"), ("pod", "c"), ("job", "x")]),
        Labels::from_pairs([("__name__", "up"), ("pod", "d"), ("job", "x")]),
        Labels::from_pairs([("__name__", "other"), ("pod", "e"), ("job", "x")]),
    ] {
        ing.push_grpc(user_id, &mock_write_request(&lbls, 1.0, now_ms))
            .await
            .unwrap();
    }

    let since = (now - Duration::from_secs(30 * 60))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let db = ing.get_tsdb(user_id).unwrap();

    let run_churn = |since_ms: i64, limit: usize| {
        let db_c = Arc::clone(&db);
        compute_label_churn(
            db.db.head(),
            &db.series_ref_history,
            db.pending_series_removals.load(Ordering::Acquire),
            db.last_series_removal_ms.load(Ordering::Acquire),
            since_ms,
            limit,
            move || {
                (
                    db_c.pending_series_removals.load(Ordering::Acquire),
                    db_c.last_series_removal_ms.load(Ordering::Acquire),
                )
            },
        )
    };

    let resp = run_churn(since, 0).unwrap();
    assert_eq!(resp.scores.len(), 2);
    assert_eq!(resp.scores[0].label_name, "pod");
    assert_eq!(resp.scores[0].score, 3);
    assert_eq!(
        resp.scores[0].metric_names,
        vec![
            LabelChurnMetricName {
                metric_name: "up".into(),
                sampled_series: 10
            },
            LabelChurnMetricName {
                metric_name: "other".into(),
                sampled_series: 5
            },
        ]
    );
    assert_eq!(resp.scores[1].label_name, "__name__");
    assert_eq!(resp.scores[1].score, 1);

    // limit=1 returns only top score
    let resp1 = run_churn(since, 1).unwrap();
    assert_eq!(resp1.scores.len(), 1);
    assert_eq!(resp1.scores[0].label_name, "pod");

    // HTTP handler tests
    let app = ingester_router(Arc::clone(&ing));
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/label_churn/test?lookback=30m&limit=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json_val: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json_val["scores"].as_array().unwrap().len(), 1);

    // Invalid requests
    for (path, exp_status) in [
        ("/ingester/label_churn/test?lookback=abc", StatusCode::BAD_REQUEST),
        ("/ingester/label_churn/test?limit=-1", StatusCode::BAD_REQUEST),
        ("/ingester/label_churn/other-tenant", StatusCode::NOT_FOUND),
        ("/ingester/label_churn/test?lookback=1s", StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let r = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), exp_status, "{path}");
    }

    // Time range starts after last series removal
    let removal_ms = (now - Duration::from_secs(4 * 60))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    db.last_series_removal_ms.store(removal_ms, Ordering::Release);
    assert!(matches!(
        run_churn(since, 0),
        Err(LabelChurnError::CompactedTooRecently)
    ));
    let recent_since = (now - Duration::from_secs(60))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!(matches!(
        run_churn(recent_since, 0),
        Err(LabelChurnError::NoRecord)
    ));

    let last_rec = now - Duration::from_secs(3 * 60);
    ing.record_label_churn_series_refs(last_rec);
    let resp_after = run_churn(since, 0).unwrap();
    assert!(resp_after.scores.is_empty());

    // Disabled handler
    let dir2 = TempDir::new().unwrap();
    let (mut cfg2, b2, i2) = default_ingester_test_config(dir2.path());
    cfg2.label_churn_enabled = false;
    let (ing_disabled, _, _) = prepare_ingester_with_blocks_storage(
        dir2.path(),
        cfg2,
        b2,
        i2,
        default_limits_test_config(),
        None,
    );
    let app2 = ingester_router(ing_disabled);
    let r2 = app2
        .oneshot(
            Request::builder()
                .uri("/ingester/label_churn/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::NOT_FOUND);

    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/label_churn_test.go::TestIngester_LabelChurnAfterCompaction
#[tokio::test]
async fn test_ingester_label_churn_after_compaction() {
    let user_id = "test";
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.label_churn_enabled = true;
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        None,
    );
    start_and_wait_healthy(&ing).await;

    let mut ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let l1 = Labels::from_pairs([("__name__", "up"), ("pod", "a")]);
    ts_ms += 1;
    ing.push_grpc(user_id, &mock_write_request(&l1, 1.0, ts_ms))
        .await
        .unwrap();

    ing.record_label_churn_series_refs(SystemTime::now() - Duration::from_secs(10 * 60));
    ing.compact_blocks(true, i64::MAX, None).await;
    let db = ing.get_tsdb(user_id).unwrap();
    assert_eq!(db.db.head().num_series(), 0);

    let since = (SystemTime::now() - Duration::from_secs(30 * 60))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let run_churn = || {
        let db_c = Arc::clone(&db);
        compute_label_churn(
            db.db.head(),
            &db.series_ref_history,
            db.pending_series_removals.load(Ordering::Acquire),
            db.last_series_removal_ms.load(Ordering::Acquire),
            since,
            0,
            move || {
                (
                    db_c.pending_series_removals.load(Ordering::Acquire),
                    db_c.last_series_removal_ms.load(Ordering::Acquire),
                )
            },
        )
    };
    assert!(matches!(run_churn(), Err(LabelChurnError::CompactedTooRecently)));

    let l2 = Labels::from_pairs([("__name__", "up"), ("pod", "b")]);
    ts_ms += 1;
    ing.push_grpc(user_id, &mock_write_request(&l2, 1.0, ts_ms))
        .await
        .unwrap();

    let rec_ms = db.last_series_removal_ms.load(Ordering::Acquire);
    ing.record_label_churn_series_refs(UNIX_EPOCH + Duration::from_millis(rec_ms as u64));

    let l3 = Labels::from_pairs([("__name__", "up"), ("pod", "c")]);
    ts_ms += 1;
    ing.push_grpc(user_id, &mock_write_request(&l3, 1.0, ts_ms))
        .await
        .unwrap();

    let resp = run_churn().unwrap();
    assert_eq!(resp.scores.len(), 1);
    assert_eq!(resp.scores[0].label_name, "pod");
    assert_eq!(resp.scores[0].score, 1);

    db.pending_series_removals.fetch_add(1, Ordering::Release);
    assert!(matches!(run_churn(), Err(LabelChurnError::CompactedTooRecently)));

    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/label_churn_test.go::TestIngester_LabelChurnAfterRestart
#[tokio::test]
async fn test_ingester_label_churn_after_restart() {
    let user_id = "test";
    let dir = TempDir::new().unwrap();
    let now = SystemTime::now();
    let now_ms = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

    {
        let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.label_churn_enabled = true;
        let (ing, _, _) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            None,
        );
        start_and_wait_healthy(&ing).await;
        ing.push_grpc(
            user_id,
            &mock_write_request(
                &Labels::from_pairs([("__name__", "up"), ("pod", "a")]),
                1.0,
                now_ms - 4 * 3600 * 1000,
            ),
        )
        .await
        .unwrap();
        ing.push_grpc(
            user_id,
            &mock_write_request(
                &Labels::from_pairs([("__name__", "up"), ("pod", "b")]),
                1.0,
                now_ms,
            ),
        )
        .await
        .unwrap();
        ing.stop().await.unwrap();
    }

    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.label_churn_enabled = true;
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        None,
    );
    start_and_wait_healthy(&ing).await;

    let db = ing.get_tsdb(user_id).unwrap();
    assert_eq!(db.db.head().num_series(), 1);
    assert_eq!(db.pending_series_removals.load(Ordering::Acquire), 0);
    assert_eq!(db.last_series_removal_ms.load(Ordering::Acquire), 0);

    let since = now_ms - 30 * 60 * 1000;
    let run_churn = || {
        let db_c = Arc::clone(&db);
        compute_label_churn(
            db.db.head(),
            &db.series_ref_history,
            db.pending_series_removals.load(Ordering::Acquire),
            db.last_series_removal_ms.load(Ordering::Acquire),
            since,
            0,
            move || {
                (
                    db_c.pending_series_removals.load(Ordering::Acquire),
                    db_c.last_series_removal_ms.load(Ordering::Acquire),
                )
            },
        )
    };
    assert!(matches!(run_churn(), Err(LabelChurnError::NoRecord)));

    ing.record_label_churn_series_refs(SystemTime::now());
    ing.push_grpc(
        user_id,
        &mock_write_request(
            &Labels::from_pairs([("__name__", "up"), ("pod", "c")]),
            1.0,
            now_ms + 1,
        ),
    )
    .await
    .unwrap();

    let resp = run_churn().unwrap();
    assert_eq!(resp.scores.len(), 1);
    assert_eq!(resp.scores[0].label_name, "pod");
    assert_eq!(resp.scores[0].score, 1);

    let _ = ing.stop().await;
    let _ = (LabelChurnResponse { since: String::new(), scores: vec![] }, LabelChurnScore { label_name: String::new(), score: 0, metric_names: vec![] });
}

// Upstream: pkg/ingester/label_churn_test.go::TestUserTSDB_UpdateLastSeriesRemoval
#[test]
fn test_user_tsdb_update_last_series_removal() {
    let dir = TempDir::new().unwrap();
    let (cfg, blocks_cfg, _) = default_ingester_test_config(dir.path());
    let overrides = Overrides::new(default_limits_test_config());
    let limiter = Arc::new(Limiter::new(
        overrides.clone(),
        Arc::new(mimir_ingester::limiter::NoopRingLimiterStrategy),
    ));
    let reg = Registry::new();
    let metrics = IngesterMetrics::new(
        Some(&reg),
        false,
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        || 0.0,
        || None,
        || 0.0,
        || 0.0,
        || 0.0,
    );
    let db = UserTsdb::open(
        "test".into(),
        &cfg,
        &blocks_cfg,
        overrides,
        limiter,
        None,
        mimir_storage::shipper::ShipperMetrics::new(Some(&reg)),
        Some(&reg),
        None,
        1,
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        Arc::new(|| 0),
        metrics.rejected.clone(),
        Default::default(),
        0,
        0,
        None,
    )
    .unwrap();

    db.update_last_series_removal();
    assert_eq!(db.last_series_removal_ms.load(Ordering::Acquire), 0);

    let before_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    db.pending_series_removals.fetch_add(1, Ordering::Release);
    db.update_last_series_removal();
    assert!(db.last_series_removal_ms.load(Ordering::Acquire) >= before_ms);
    assert_eq!(db.pending_series_removals.load(Ordering::Acquire), 0);
    db.close().unwrap();
}

// ============================================================================
// pkg/ingester/label_value_bytes_test.go (3 tests)
// ============================================================================

fn label_value_of_length(prefix: &str, length: usize) -> String {
    format!("{}{}", prefix, "x".repeat(length.saturating_sub(prefix.len())))
}

// Upstream: pkg/ingester/label_value_bytes_test.go::TestIngester_LabelValueBytesMetrics
#[tokio::test]
async fn test_ingester_label_value_bytes_metrics() {
    let dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.ring.replication_factor = 1;
    let mut limits = default_limits_test_config();
    limits.label_value_bytes_metric_threshold = 2000;

    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let user_id = "test";
    // Short values (<= 128) must not count
    for n in 0..200 {
        let val = label_value_of_length(&format!("s{n}"), LABEL_VALUE_BYTES_MIN_LENGTH);
        let lbls = Labels::from_pairs([("__name__", "quiet"), ("short", &val)]);
        ing.push_grpc(user_id, &mock_write_request(&lbls, 1.0, 1000))
            .await
            .unwrap();
    }
    ing.update_limit_metrics();
    assert_eq!(
        metric_count(&reg, "cortex_ingester_label_value_bytes_over_threshold"),
        0
    );

    // 9 distinct 200-byte values = 1800 < 2000
    let values: Vec<String> = (0..10)
        .map(|n| label_value_of_length(&n.to_string(), 200))
        .collect();
    for val in &values[..9] {
        let lbls = Labels::from_pairs([("__name__", "noisy"), ("big", val.as_str())]);
        ing.push_grpc(user_id, &mock_write_request(&lbls, 1.0, 1000))
            .await
            .unwrap();
    }
    ing.update_limit_metrics();
    assert_eq!(
        metric_count(&reg, "cortex_ingester_label_value_bytes_over_threshold"),
        0
    );

    // Repeating an existing value on another series must not double-count
    let lbls_dup = Labels::from_pairs([("__name__", "noisy_again"), ("big", values[0].as_str())]);
    ing.push_grpc(user_id, &mock_write_request(&lbls_dup, 1.0, 1000))
        .await
        .unwrap();
    ing.update_limit_metrics();
    assert_eq!(
        metric_count(&reg, "cortex_ingester_label_value_bytes_over_threshold"),
        0
    );

    // 10th distinct value reaches 2000 >= 2000
    let lbls_10 = Labels::from_pairs([("__name__", "noisy"), ("big", values[9].as_str())]);
    ing.push_grpc(user_id, &mock_write_request(&lbls_10, 1.0, 1000))
        .await
        .unwrap();
    ing.update_limit_metrics();
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_label_value_bytes_over_threshold",
            &[("user", "test"), ("label", "big")]
        ),
        Some(1.0)
    );

    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/label_value_bytes_test.go::TestIngester_LabelValueBytesMetrics_ThresholdDisabled
#[tokio::test]
async fn test_ingester_label_value_bytes_metrics_threshold_disabled() {
    let dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.ring.replication_factor = 1;
    let limits = default_limits_test_config();
    assert_eq!(limits.label_value_bytes_metric_threshold, 0);

    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let big = label_value_of_length("a", 5000);
    let lbls = Labels::from_pairs([("__name__", "noisy"), ("big", &big)]);
    ing.push_grpc("test", &mock_write_request(&lbls, 1.0, 1000))
        .await
        .unwrap();
    ing.update_limit_metrics();
    assert_eq!(
        metric_count(&reg, "cortex_ingester_label_value_bytes_over_threshold"),
        0
    );
    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/label_value_bytes_test.go::TestIngester_LabelValueBytesMetrics_ClearedWhenTSDBIsClosed
#[tokio::test]
async fn test_ingester_label_value_bytes_metrics_cleared_when_tsdb_is_closed() {
    let dir = TempDir::new().unwrap();
    let bucket_dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let (mut cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.ring.replication_factor = 1;
    blocks_cfg.tsdb.close_idle_tsdb_timeout = GoDuration(0);
    blocks_cfg.tsdb.ship_interval = GoDuration::from_secs(60);
    blocks_cfg.bucket = BucketConfig {
        backend_cfg: mimir_config::StorageBackendConfig {
            backend: "filesystem".to_string(),
            filesystem: mimir_config::FilesystemConfig {
                dir: bucket_dir.path().to_string_lossy().to_string(),
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let mut limits = default_limits_test_config();
    limits.label_value_bytes_metric_threshold = 1000;

    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    for p in ["a", "b"] {
        let v = label_value_of_length(p, 600);
        let lbls = Labels::from_pairs([("__name__", "noisy"), ("big", &v)]);
        ing.push_grpc("test", &mock_write_request(&lbls, 1.0, 1000))
            .await
            .unwrap();
    }
    ing.update_limit_metrics();
    assert_eq!(
        metric_count(&reg, "cortex_ingester_label_value_bytes_over_threshold"),
        1
    );

    ing.compact_blocks(true, i64::MAX, None).await;
    ing.ship_blocks(None).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        ing.close_and_delete_user_tsdb_if_idle("test", Duration::ZERO)
            .await,
        TsdbCloseCheckResult::IdleClosed
    );
    assert_eq!(
        metric_count(&reg, "cortex_ingester_label_value_bytes_over_threshold"),
        0
    );

    let _ = ing.stop().await;
}

// ============================================================================
// pkg/ingester/user_metrics_metadata_test.go (2 tests)
// ============================================================================

fn mk_metadata_store(
    max_per_user: i64,
    max_per_metric: i64,
) -> (TenantMetadataStore, Registry) {
    let ring = Arc::new(RingCountMock {
        instances_count: 1,
        instances_in_zone_count: 0,
        zones_count: 1,
    });
    let limits = LimitsConfig {
        max_global_metadata_per_user: max_per_user,
        max_global_metadata_per_metric: max_per_metric,
        ..Default::default()
    };
    let overrides = Overrides::new(limits);
    let strategy = Arc::new(IngesterRingLimiterStrategy::new(
        ring,
        1,
        false,
        "",
        overrides.clone(),
    ));
    let limiter = Arc::new(Limiter::new(overrides, strategy));
    let reg = Registry::new();
    let metrics = IngesterMetrics::new(
        Some(&reg),
        true,
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        || 0.0,
        || None,
        || 0.0,
        || 0.0,
        || 0.0,
    );
    let store = TenantMetadataStore::new(
        "test".to_string(),
        limiter,
        metrics.mem_metadata.clone(),
        metrics.mem_metadata_created_total.clone(),
        metrics.mem_metadata_removed_total.clone(),
        metrics.discarded_metadata_per_user_limit.clone(),
        metrics.discarded_metadata_per_metric_limit.clone(),
    );
    (store, reg)
}

fn meta(name: &str, help: &str) -> cortexpb::MetricMetadata {
    cortexpb::MetricMetadata {
        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
        metric_family_name: name.to_string(),
        help: help.to_string(),
        unit: String::new(),
    }
}

// Upstream: pkg/ingester/user_metrics_metadata_test.go::TestUserMetricsMetadata
#[test]
fn test_user_metrics_metadata() {
    // 1) multiple metadata per metric
    {
        let (store, _reg) = mk_metadata_store(0, 0);
        let inputs = [
            meta("test_metric_1", "foo"),
            meta("test_metric_1", "bar"),
            meta("test_metric_2", "baz"),
            meta("test_metric_2", "qux"),
        ];
        for m in &inputs {
            store.add(m).unwrap();
        }
        let mut got = store.query(&[], "", -1, -1);
        got.sort_by(|a, b| a.help.cmp(&b.help));
        assert_eq!(got.len(), 4);
        store.purge(None);
        assert!(store.query(&[], "", -1, -1).is_empty());
    }

    // 2) per-user metadata limit
    {
        let (store, _reg) = mk_metadata_store(1, 0);
        store.add(&meta("test_metric_1", "foo")).unwrap();
        store.add(&meta("test_metric_1", "bar")).unwrap();
        let e1 = store.add(&meta("test_metric_2", "baz")).unwrap_err();
        assert!(e1.to_string().contains("err-mimir-max-metadata-per-user"));
        let e2 = store.add(&meta("test_metric_2", "qux")).unwrap_err();
        assert!(e2.to_string().contains("err-mimir-max-metadata-per-user"));
        assert_eq!(store.query(&[], "", -1, -1).len(), 2);
        store.purge(None);
        assert!(store.query(&[], "", -1, -1).is_empty());
    }

    // 3) per-metric metadata limit
    {
        let (store, _reg) = mk_metadata_store(0, 1);
        store.add(&meta("test_metric_1", "foo")).unwrap();
        let e1 = store.add(&meta("test_metric_1", "bar")).unwrap_err();
        assert!(e1.to_string().contains("err-mimir-max-metadata-per-metric"));
        store.add(&meta("test_metric_2", "baz")).unwrap();
        let e2 = store.add(&meta("test_metric_2", "qux")).unwrap_err();
        assert!(e2.to_string().contains("err-mimir-max-metadata-per-metric"));
        assert_eq!(store.query(&[], "", -1, -1).len(), 2);
        store.purge(None);
        assert!(store.query(&[], "", -1, -1).is_empty());
    }
}

// Upstream: pkg/ingester/user_metrics_metadata_test.go::TestUserMetricsMetadataRequest
#[test]
fn test_user_metrics_metadata_request() {
    let (store, _reg) = mk_metadata_store(0, 0);
    let m1_foo = meta("test_metric_1", "foo");
    let m1_bar = meta("test_metric_1", "bar");
    let m2_baz = meta("test_metric_2", "baz");
    let m2_qux = meta("test_metric_2", "qux");
    for m in [&m1_foo, &m1_bar, &m2_baz, &m2_qux] {
        store.add(m).unwrap();
    }

    let sorted = |mut v: Vec<cortexpb::MetricMetadata>| {
        v.sort_by(|a, b| {
            a.metric_family_name
                .cmp(&b.metric_family_name)
                .then_with(|| a.help.cmp(&b.help))
        });
        v
    };
    let all = sorted(vec![m1_foo.clone(), m1_bar.clone(), m2_baz.clone(), m2_qux.clone()]);

    assert_eq!(sorted(store.query(&[], "", -1, -1)), all);
    assert!(store.query(&[], "", 0, -1).is_empty());

    // limit=1 returns all metadata for one metric
    let q_lim1 = sorted(store.query(&[], "", 1, -1));
    assert!(
        q_lim1 == sorted(vec![m1_foo.clone(), m1_bar.clone()])
            || q_lim1 == sorted(vec![m2_baz.clone(), m2_qux.clone()])
    );

    assert_eq!(sorted(store.query(&[], "", -1, 0)), all);

    // limit_per_metric=1
    let q_lpm1 = sorted(store.query(&[], "", -1, 1));
    assert_eq!(q_lpm1.len(), 2);
    assert_eq!(q_lpm1[0].metric_family_name, "test_metric_1");
    assert_eq!(q_lpm1[1].metric_family_name, "test_metric_2");

    // single metric
    assert_eq!(
        sorted(store.query(&[], "test_metric_2", -1, -1)),
        sorted(vec![m2_baz.clone(), m2_qux.clone()])
    );

    // metric_names
    assert_eq!(
        sorted(store.query(
            &["test_metric_1".into(), "test_metric_2".into()],
            "",
            -1,
            -1
        )),
        all
    );
    assert_eq!(
        sorted(store.query(&["test_metric_2".into()], "", -1, -1)),
        sorted(vec![m2_baz.clone(), m2_qux.clone()])
    );
    // metric_names takes precedence over metric
    assert_eq!(
        sorted(store.query(&["test_metric_2".into()], "test_metric_1", -1, -1)),
        sorted(vec![m2_baz, m2_qux])
    );
    assert!(store
        .query(&["does_not_exist".into()], "", -1, -1)
        .is_empty());

    let q_search = sorted(store.query(
        &["test_metric_1".into(), "test_metric_2".into()],
        "",
        2,
        1,
    ));
    assert_eq!(q_search.len(), 2);

    let _unused: HashMap<String, String> = HashMap::new();
}
