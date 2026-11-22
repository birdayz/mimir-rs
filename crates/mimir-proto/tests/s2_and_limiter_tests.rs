//! 1:1 upstream-ported tests for `mimir-proto`:
//! - `pkg/util/grpcencoding/s2/s2_test.go` (1)
//! - `pkg/util/limiter/query_limiter_test.go` (7)
//! - `pkg/util/limiter/memory_consumption_test.go` (10; tracing panic test in `mimir-common`)
//! - `pkg/util/limiter/series_labels_deduplicator_test.go` (4)

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use mimir_proto::client::streaming::{
    memory_consumption_tracker_from_context, with_memory_tracker,
    InflightMemoryConsumptionTracker, MemoryConsumptionSource, MemoryConsumptionTracker,
    QueryLimiter, QueryMetrics, SeriesDeduplicatorMetrics, SeriesLabelsDeduplicator,
    ERR_NO_MEMORY_CONSUMPTION_TRACKER_IN_CONTEXT, MEMORY_CONSUMPTION_SOURCE_COUNT,
};
use mimir_proto::compat::PromLabels;
use mimir_proto::s2::{S2Compressor, S2_ENCODING_NAME};
use prometheus::proto::MetricType;
use prometheus::{Counter, Opts, Registry};

// ============================================================================
// pkg/util/grpcencoding/s2/s2_test.go
// ============================================================================

// Upstream: pkg/util/grpcencoding/s2/s2_test.go::TestCompressor
#[test]
fn test_s2_compressor() {
    let c = S2Compressor::new();
    assert_eq!(c.name(), S2_ENCODING_NAME);
    assert_eq!(c.name(), "s2");

    let cases = [
        ("empty", String::new()),
        ("short", "hello world".to_string()),
        ("long", "123456789".repeat(1024)),
    ];
    for (name, input) in cases {
        let compressed = c.compress(input.as_bytes()).expect(name);
        let decompressed = c
            .decompress(&compressed, input.len().max(1024) + 1024)
            .expect(name);
        assert_eq!(String::from_utf8(decompressed).unwrap(), input, "{name}");
    }
}

// ============================================================================
// pkg/util/limiter/query_limiter_test.go
// ============================================================================

fn assert_rejected_queries_metric_value(
    reg: &Registry,
    expected_max_series: f64,
    expected_max_chunk_bytes: f64,
    expected_max_chunks: f64,
    expected_max_estimated_chunks: f64,
) {
    let mfs = reg.gather();
    let mf = mfs
        .iter()
        .find(|m| m.get_name() == "cortex_querier_queries_rejected_total")
        .expect("cortex_querier_queries_rejected_total must be registered");
    assert_eq!(mf.get_field_type(), MetricType::COUNTER);

    let mut actual: BTreeMap<String, f64> = BTreeMap::new();
    for m in mf.get_metric() {
        let reason = m
            .get_label()
            .iter()
            .find(|l| l.get_name() == "reason")
            .unwrap()
            .get_value()
            .to_string();
        actual.insert(reason, m.get_counter().get_value());
    }

    assert_eq!(
        actual.get("max-fetched-series-per-query").copied(),
        Some(expected_max_series)
    );
    assert_eq!(
        actual.get("max-fetched-chunk-bytes-per-query").copied(),
        Some(expected_max_chunk_bytes)
    );
    assert_eq!(
        actual.get("max-fetched-chunks-per-query").copied(),
        Some(expected_max_chunks)
    );
    assert_eq!(
        actual.get("max-estimated-fetched-chunks-per-query").copied(),
        Some(expected_max_estimated_chunks)
    );
    assert_eq!(
        actual
            .get("max-estimated-memory-consumption-per-query")
            .copied(),
        Some(0.0)
    );
}

// Upstream: pkg/util/limiter/query_limiter_test.go::TestQueryLimiter_AddSeries_ShouldReturnNoErrorOnLimitNotExceeded
#[test]
fn test_query_limiter_add_series_should_return_no_error_on_limit_not_exceeded() {
    let metric_name = "test_metric";
    let series1 = PromLabels::from_strings(&["__name__", &format!("{metric_name}_1"), "series1", "1"]);
    let series2 = PromLabels::from_strings(&["__name__", &format!("{metric_name}_2"), "series2", "1"]);
    let reg = Registry::new();
    let qm = Arc::new(QueryMetrics::new(Some(&reg)));
    let limiter = QueryLimiter::with_metrics(100, 0, 0, 0, Some(qm));

    assert!(limiter.add_series(&series1).is_ok());
    assert!(limiter.add_series(&series2).is_ok());
    assert_eq!(limiter.unique_series_count(), 2);
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);

    // Re-add previous series to make sure it's not double counted.
    assert!(limiter.add_series(&series1).is_ok());
    assert_eq!(limiter.unique_series_count(), 2);
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);
}

// Upstream: pkg/util/limiter/query_limiter_test.go::TestQueryLimiter_AddSeries_ShouldReturnErrorOnLimitExceeded
#[test]
fn test_query_limiter_add_series_should_return_error_on_limit_exceeded() {
    let metric_name = "test_metric";
    let series1 = PromLabels::from_strings(&["__name__", &format!("{metric_name}_1"), "series1", "1"]);
    let series2 = PromLabels::from_strings(&["__name__", &format!("{metric_name}_2"), "series2", "1"]);
    let series3 = PromLabels::from_strings(&["__name__", &format!("{metric_name}_3"), "series2", "1"]);
    let reg = Registry::new();
    let qm = Arc::new(QueryMetrics::new(Some(&reg)));
    let limiter = QueryLimiter::with_metrics(1, 0, 0, 0, Some(qm));

    assert!(limiter.add_series(&series1).is_ok());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);

    assert!(limiter.add_series(&series2).is_err());
    assert_rejected_queries_metric_value(&reg, 1.0, 0.0, 0.0, 0.0);

    // Add the same series again and ensure that we don't increment the failed queries metric again.
    assert!(limiter.add_series(&series2).is_err());
    assert_rejected_queries_metric_value(&reg, 1.0, 0.0, 0.0, 0.0);

    // Add another series and ensure that we don't increment the failed queries metric again.
    assert!(limiter.add_series(&series3).is_err());
    assert_rejected_queries_metric_value(&reg, 1.0, 0.0, 0.0, 0.0);
}

// Upstream: pkg/util/limiter/query_limiter_test.go::TestQueryLimiter_AddChunkBytes
#[test]
fn test_query_limiter_add_chunk_bytes() {
    let reg = Registry::new();
    let qm = Arc::new(QueryMetrics::new(Some(&reg)));
    let limiter = QueryLimiter::with_metrics(0, 100, 0, 0, Some(qm));

    assert!(limiter.add_chunk_bytes(100).is_ok());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);

    assert!(limiter.add_chunk_bytes(1).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 1.0, 0.0, 0.0);

    // Add more bytes and ensure that we don't increment the failed queries metric again.
    assert!(limiter.add_chunk_bytes(2).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 1.0, 0.0, 0.0);
}

// Upstream: pkg/util/limiter/query_limiter_test.go::TestQueryLimiter_AddChunks_EnabledLimit
#[test]
fn test_query_limiter_add_chunks_enabled_limit() {
    let reg = Registry::new();
    let qm = Arc::new(QueryMetrics::new(Some(&reg)));
    let limiter = QueryLimiter::with_metrics(0, 0, 100, 0, Some(qm));

    assert!(limiter.add_chunks(100).is_ok());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);

    assert!(limiter.add_chunks(1).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 1.0, 0.0);

    assert!(limiter.add_chunks(0).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 1.0, 0.0);

    assert!(limiter.add_chunks(2).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 1.0, 0.0);
}

// Upstream: pkg/util/limiter/query_limiter_test.go::TestQueryLimiter_AddChunks_IgnoresDisabledLimit
#[test]
fn test_query_limiter_add_chunks_ignores_disabled_limit() {
    let reg = Registry::new();
    let qm = Arc::new(QueryMetrics::new(Some(&reg)));
    let limiter = QueryLimiter::with_metrics(0, 0, 0, 0, Some(qm));

    assert!(limiter.add_chunks(100).is_ok());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);
}

// Upstream: pkg/util/limiter/query_limiter_test.go::TestQueryLimiter_AddEstimatedChunks_EnabledLimit
#[test]
fn test_query_limiter_add_estimated_chunks_enabled_limit() {
    let reg = Registry::new();
    let qm = Arc::new(QueryMetrics::new(Some(&reg)));
    let limiter = QueryLimiter::with_metrics(0, 0, 0, 100, Some(qm));

    assert!(limiter.add_estimated_chunks(100).is_ok());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);

    assert!(limiter.add_estimated_chunks(1).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 1.0);

    assert!(limiter.add_estimated_chunks(0).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 1.0);

    assert!(limiter.add_estimated_chunks(2).is_err());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 1.0);
}

// Upstream: pkg/util/limiter/query_limiter_test.go::TestQueryLimiter_AddEstimatedChunks_IgnoresDisabledLimit
#[test]
fn test_query_limiter_add_estimated_chunks_ignores_disabled_limit() {
    let reg = Registry::new();
    let qm = Arc::new(QueryMetrics::new(Some(&reg)));
    let limiter = QueryLimiter::with_metrics(0, 0, 0, 0, Some(qm));

    assert!(limiter.add_estimated_chunks(100).is_ok());
    assert_rejected_queries_metric_value(&reg, 0.0, 0.0, 0.0, 0.0);
}

// ============================================================================
// pkg/util/limiter/memory_consumption_test.go
// ============================================================================

const REJECTED_QUERIES_METRIC_NAME: &str = "rejected_queries";

fn create_rejected_metric() -> (Registry, Counter) {
    let reg = Registry::new();
    let metric = Counter::with_opts(Opts::new(REJECTED_QUERIES_METRIC_NAME, "rejected queries"))
        .expect("valid counter");
    reg.register(Box::new(metric.clone())).unwrap();
    (reg, metric)
}

fn assert_rejected_queries_count(reg: &Registry, expected: f64) {
    let mfs = reg.gather();
    let mf = mfs
        .iter()
        .find(|m| m.get_name() == REJECTED_QUERIES_METRIC_NAME)
        .expect("rejected_queries must be registered");
    assert_eq!(mf.get_metric()[0].get_counter().get_value(), expected);
}

fn assert_tracker_tracker_metrics(
    reg: &Registry,
    max_bytes: f64,
    current_bytes: f64,
    peak_bytes: f64,
    sampled: f64,
) {
    let mfs = reg.gather();
    let mut vals: BTreeMap<&str, f64> = BTreeMap::new();
    for mf in &mfs {
        vals.insert(mf.get_name(), mf.get_metric()[0].get_gauge().get_value());
    }
    assert_eq!(
        vals.get("cortex_querier_inflight_query_current_estimated_memory_consumption_bytes")
            .copied(),
        Some(current_bytes)
    );
    assert_eq!(
        vals.get("cortex_querier_inflight_query_max_estimated_memory_consumption_limit_bytes")
            .copied(),
        Some(max_bytes)
    );
    assert_eq!(
        vals.get("cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes")
            .copied(),
        Some(peak_bytes)
    );
    assert_eq!(
        vals.get("cortex_querier_inflight_query_sampled_count")
            .copied(),
        Some(sampled)
    );
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryConsumptionTrackerFromContext
#[tokio::test]
async fn test_memory_consumption_tracker_from_context() {
    // does not exist
    let err = memory_consumption_tracker_from_context().unwrap_err();
    assert_eq!(err, ERR_NO_MEMORY_CONSUMPTION_TRACKER_IN_CONTEXT);

    // exists
    let existing = Arc::new(MemoryConsumptionTracker::unlimited());
    existing
        .increase_memory_consumption_by_source(512, MemoryConsumptionSource::IngesterChunks)
        .unwrap();

    with_memory_tracker(Arc::clone(&existing), async {
        let stored = memory_consumption_tracker_from_context().unwrap();
        assert!(Arc::ptr_eq(&existing, &stored));
        assert_eq!(stored.current_estimated_memory_consumption_bytes(), 512);
        assert_eq!(
            stored.current_estimated_memory_consumption_bytes_by_source(
                MemoryConsumptionSource::IngesterChunks
            ),
            512
        );
    })
    .await;
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestAddToContext
#[tokio::test]
async fn test_add_to_context() {
    let existing = Arc::new(MemoryConsumptionTracker::unlimited());
    existing
        .increase_memory_consumption_by_source(512, MemoryConsumptionSource::IngesterChunks)
        .unwrap();

    with_memory_tracker(Arc::clone(&existing), async {
        let stored = memory_consumption_tracker_from_context().unwrap();
        assert!(Arc::ptr_eq(&existing, &stored));
        assert_eq!(stored.current_estimated_memory_consumption_bytes(), 512);
        assert_eq!(
            stored.current_estimated_memory_consumption_bytes_by_source(
                MemoryConsumptionSource::IngesterChunks
            ),
            512
        );
    })
    .await;
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryConsumptionTracker_Unlimited
#[test]
fn test_memory_consumption_tracker_unlimited() {
    let (reg, metric) = create_rejected_metric();
    let tracker = MemoryConsumptionTracker::new_with_details(0, Some(metric), "foo + bar", None);

    tracker
        .increase_memory_consumption_by_source(128, MemoryConsumptionSource::IngesterChunks)
        .unwrap();
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 128);
    assert_eq!(
        tracker.current_estimated_memory_consumption_bytes_by_source(
            MemoryConsumptionSource::IngesterChunks
        ),
        128
    );
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 128);

    // Add some more memory consumption. The current and peak stats should be updated.
    tracker
        .increase_memory_consumption_by_source(2, MemoryConsumptionSource::StoreGatewayChunks)
        .unwrap();
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 130);
    assert_eq!(
        tracker.current_estimated_memory_consumption_bytes_by_source(
            MemoryConsumptionSource::StoreGatewayChunks
        ),
        2
    );
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 130);

    // Reduce memory consumption. The current consumption should be updated, but the peak should be unchanged.
    tracker.decrease_memory_consumption_by_source(128, MemoryConsumptionSource::IngesterChunks);
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 2);
    assert_eq!(
        tracker.current_estimated_memory_consumption_bytes_by_source(
            MemoryConsumptionSource::IngesterChunks
        ),
        0
    );
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 130);

    // Add some more memory consumption that doesn't take us over the previous peak.
    tracker
        .increase_memory_consumption_by_source(8, MemoryConsumptionSource::FPointSlices)
        .unwrap();
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 10);
    assert_eq!(
        tracker.current_estimated_memory_consumption_bytes_by_source(
            MemoryConsumptionSource::FPointSlices
        ),
        8
    );
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 130);

    // Add some more memory consumption that takes us over the previous peak.
    tracker
        .increase_memory_consumption_by_source(121, MemoryConsumptionSource::HPointSlices)
        .unwrap();
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 131);
    assert_eq!(
        tracker.current_estimated_memory_consumption_bytes_by_source(
            MemoryConsumptionSource::HPointSlices
        ),
        121
    );
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 131);

    assert_rejected_queries_count(&reg, 0.0);

    // Test reducing memory consumption to a negative value panics with exact Go message.
    let res1 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tracker.decrease_memory_consumption_by_source(9, MemoryConsumptionSource::FPointSlices);
    }));
    let msg1 = res1
        .unwrap_err()
        .downcast::<String>()
        .map(|s| *s)
        .unwrap_or_default();
    assert_eq!(
        msg1,
        "Estimated memory consumption of all instances of []promql.FPoint in this query is 8 bytes when trying to return 9 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar"
    );

    let res2 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tracker.decrease_memory_consumption_by_source(130, MemoryConsumptionSource::HPointSlices);
    }));
    let msg2 = res2
        .unwrap_err()
        .downcast::<String>()
        .map(|s| *s)
        .unwrap_or_default();
    assert_eq!(
        msg2,
        "Estimated memory consumption of all instances of []promql.HPoint in this query is 121 bytes when trying to return 130 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar"
    );
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryConsumptionTracker_Limited
#[test]
fn test_memory_consumption_tracker_limited() {
    let (reg, metric) = create_rejected_metric();
    let tracker = MemoryConsumptionTracker::new_with_details(11, Some(metric), "foo + bar", None);

    tracker
        .increase_memory_consumption_by_source(8, MemoryConsumptionSource::IngesterChunks)
        .unwrap();
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 8);
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 8);
    assert_rejected_queries_count(&reg, 0.0);

    tracker
        .increase_memory_consumption_by_source(1, MemoryConsumptionSource::StoreGatewayChunks)
        .unwrap();
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 9);
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 9);
    assert_rejected_queries_count(&reg, 0.0);

    tracker.decrease_memory_consumption_by_source(1, MemoryConsumptionSource::StoreGatewayChunks);
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 8);
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 9);
    assert_rejected_queries_count(&reg, 0.0);

    let expected_err = "the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: 11 bytes) (err-mimir-max-estimated-memory-consumption-per-query)";
    let err = tracker
        .increase_memory_consumption_by_source(4, MemoryConsumptionSource::StoreGatewayChunks)
        .unwrap_err()
        .to_string();
    assert!(err.contains(expected_err), "got: {err}");
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 8);
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 9);
    assert_rejected_queries_count(&reg, 1.0);

    // Make sure we don't increment the rejection count a second time for the same query.
    let err2 = tracker
        .increase_memory_consumption_by_source(4, MemoryConsumptionSource::IngesterChunks)
        .unwrap_err()
        .to_string();
    assert!(err2.contains(expected_err), "got: {err2}");
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 8);
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 9);
    assert_rejected_queries_count(&reg, 1.0);

    // Keep adding more memory consumption up to the limit.
    for i in 0..3u64 {
        tracker
            .increase_memory_consumption_by_source(1, MemoryConsumptionSource::FPointSlices)
            .unwrap();
        assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 9 + i);
        assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 9 + i);
    }

    // Try to add when already at the limit.
    let err3 = tracker
        .increase_memory_consumption_by_source(1, MemoryConsumptionSource::FPointSlices)
        .unwrap_err()
        .to_string();
    assert!(err3.contains(expected_err), "got: {err3}");
    assert_eq!(tracker.current_estimated_memory_consumption_bytes(), 11);
    assert_eq!(tracker.peak_estimated_memory_consumption_bytes(), 11);
    assert_rejected_queries_count(&reg, 1.0);

    // Panics on returning more than held.
    let res1 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tracker.decrease_memory_consumption_by_source(150, MemoryConsumptionSource::FPointSlices);
    }));
    assert_eq!(
        *res1.unwrap_err().downcast::<String>().unwrap(),
        "Estimated memory consumption of all instances of []promql.FPoint in this query is 3 bytes when trying to return 150 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar"
    );

    let res2 = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tracker.decrease_memory_consumption_by_source(150, MemoryConsumptionSource::HPointSlices);
    }));
    assert_eq!(
        *res2.unwrap_err().downcast::<String>().unwrap(),
        "Estimated memory consumption of all instances of []promql.HPoint in this query is 0 bytes when trying to return 150 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar"
    );
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryConsumptionTrackerTracker_Aggregation
#[test]
fn test_memory_consumption_tracker_tracker_aggregation() {
    let reg = Registry::new();
    let tt = InflightMemoryConsumptionTracker::new(Some(&reg), None);

    let tracker1_limit = 100u64;
    let tracker2_limit = 200u64;
    let tracker1 = tt.new_memory_consumption_tracker(tracker1_limit, "query1", None);
    let tracker2 = tt.new_memory_consumption_tracker(tracker2_limit, "query2", None);

    let tracker1_ingester = 30u64;
    let tracker1_store_gateway = 20u64;
    let tracker1_decrease = 10u64;
    let tracker1_current = tracker1_ingester + tracker1_store_gateway - tracker1_decrease; // 40
    let tracker1_peak = tracker1_ingester + tracker1_store_gateway; // 50
    tracker1
        .increase_memory_consumption_by_source(
            tracker1_ingester,
            MemoryConsumptionSource::IngesterChunks,
        )
        .unwrap();
    tracker1
        .increase_memory_consumption_by_source(
            tracker1_store_gateway,
            MemoryConsumptionSource::StoreGatewayChunks,
        )
        .unwrap();
    tracker1.decrease_memory_consumption_by_source(
        tracker1_decrease,
        MemoryConsumptionSource::IngesterChunks,
    );

    let tracker2_ingester = 60u64;
    let tracker2_current = tracker2_ingester;
    let tracker2_peak = tracker2_ingester;
    tracker2
        .increase_memory_consumption_by_source(
            tracker2_ingester,
            MemoryConsumptionSource::IngesterChunks,
        )
        .unwrap();

    assert_tracker_tracker_metrics(
        &reg,
        (tracker1_limit + tracker2_limit) as f64,
        (tracker1_current + tracker2_current) as f64,
        (tracker1_peak + tracker2_peak) as f64,
        2.0,
    );

    tt.deregister(&tracker1);
    assert_tracker_tracker_metrics(
        &reg,
        tracker2_limit as f64,
        tracker2_current as f64,
        tracker2_peak as f64,
        1.0,
    );

    tt.deregister(&tracker2);
    assert_tracker_tracker_metrics(&reg, 0.0, 0.0, 0.0, 0.0);

    // idempotent
    tt.deregister(&tracker2);
    assert_tracker_tracker_metrics(&reg, 0.0, 0.0, 0.0, 0.0);
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryConsumptionTrackerTracker_DeregisterNonManagedTracker
#[test]
fn test_memory_consumption_tracker_tracker_deregister_non_managed_tracker() {
    let reg = Registry::new();
    let tt = InflightMemoryConsumptionTracker::new(Some(&reg), None);
    let non_managed = Arc::new(MemoryConsumptionTracker::new_with_details(
        100,
        None,
        "query3",
        None,
    ));
    assert!(!tt.is_tracking(&non_managed));
    tt.deregister(&non_managed);
    assert!(!tt.is_tracking(&non_managed));
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryConsumptionSourceNames
#[test]
fn test_memory_consumption_source_names() {
    assert_eq!(
        MemoryConsumptionSource::ALL.len(),
        MEMORY_CONSUMPTION_SOURCE_COUNT
    );
    for src in MemoryConsumptionSource::ALL {
        assert_ne!(src.as_str(), "unknown memory source");
        assert!(!src.to_string().is_empty());
    }
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestNewUnlimintedInflightMemoryConsumptionTracker
#[test]
fn test_new_unlimited_inflight_memory_consumption_tracker() {
    let reg = Registry::new();
    let factory = InflightMemoryConsumptionTracker::new_unlimited(Some(&reg));
    let tracker = factory.new_memory_consumption_tracker(1000, "foo + bar", None);
    tracker
        .increase_memory_consumption_by_source(5000, MemoryConsumptionSource::IngesterChunks)
        .unwrap();
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestMemoryTrackerNestedTrackers
#[tokio::test]
async fn test_memory_tracker_nested_trackers() {
    let reg = Registry::new();
    let reject_counter =
        Counter::with_opts(Opts::new(REJECTED_QUERIES_METRIC_NAME, "rejected")).unwrap();
    reg.register(Box::new(reject_counter.clone())).unwrap();
    let factory = InflightMemoryConsumptionTracker::new(Some(&reg), Some(reject_counter));

    // simple managed tracker is deregistered
    {
        let tracker = factory.new_memory_consumption_tracker(1000, "foo + bar", None);
        assert!(factory.is_tracking(&tracker));
        factory.deregister(&tracker);
        assert!(!factory.is_tracking(&tracker));
        factory.deregister(&tracker);
        assert!(!factory.is_tracking(&tracker));
    }

    // nested managed trackers - no memory limits exceeded
    {
        let parent = factory.new_memory_consumption_tracker(1000, "foo + bar", None);
        assert!(factory.is_tracking(&parent));

        let routines = 5usize;
        let mut handles = Vec::with_capacity(routines);
        for i in 0..routines {
            let parent_c = Arc::clone(&parent);
            let factory_c = Arc::clone(&factory);
            handles.push(tokio::spawn(async move {
                let child = parent_c.new_nested_memory_consumption_tracker(format!("child {i}"), None);
                assert_eq!(
                    parent_c.max_estimated_memory_consumption_bytes(),
                    child.max_estimated_memory_consumption_bytes()
                );
                assert!(factory_c.is_tracking(&parent_c));
                assert!(!factory_c.is_tracking(&child));

                child
                    .increase_memory_consumption_by_source(10, MemoryConsumptionSource::IngesterChunks)
                    .unwrap();
                child.decrease_memory_consumption_by_source(1, MemoryConsumptionSource::IngesterChunks);
                assert_eq!(child.current_estimated_memory_consumption_bytes(), 9);
                assert_eq!(child.query_description(), format!("child {i}"));
                assert!(parent_c.current_estimated_memory_consumption_bytes() >= 9);

                factory_c.deregister(&child);
                assert!(factory_c.is_tracking(&parent_c));
                assert!(!factory_c.is_tracking(&child));

                factory_c.deregister(&child);
                assert!(factory_c.is_tracking(&parent_c));
                assert!(!factory_c.is_tracking(&child));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            parent.current_estimated_memory_consumption_bytes(),
            (routines * 9) as u64
        );
        assert_eq!(parent.query_description(), "foo + bar");
        assert!(factory.is_tracking(&parent));

        factory.deregister(&parent);
        assert!(!factory.is_tracking(&parent));
        factory.deregister(&parent);
        assert!(!factory.is_tracking(&parent));
    }

    // nested managed trackers with label increases - no memory limits exceeded
    {
        let parent = factory.new_memory_consumption_tracker(1000, "foo + bar", None);
        assert!(factory.is_tracking(&parent));

        let size_all_labels = PromLabels::from_strings(&["key", "value", "key2", "value2"]).byte_size() as u64;
        let size_some_labels = PromLabels::from_strings(&["key", "value"]).byte_size() as u64;

        let routines = 5usize;
        let mut handles = Vec::with_capacity(routines);
        for i in 0..routines {
            let parent_c = Arc::clone(&parent);
            handles.push(tokio::spawn(async move {
                let child = parent_c.new_nested_memory_consumption_tracker(format!("child {i}"), None);
                child
                    .increase_memory_consumption_for_labels(&PromLabels::from_strings(&[
                        "key", "value", "key2", "value2",
                    ]))
                    .unwrap();
                assert_eq!(child.current_estimated_memory_consumption_bytes(), size_all_labels);
                assert!(parent_c.current_estimated_memory_consumption_bytes() >= 22);
                child
            }));
        }
        let mut children = Vec::with_capacity(routines);
        for h in handles {
            children.push(h.await.unwrap());
        }
        assert_eq!(
            parent.current_estimated_memory_consumption_bytes(),
            routines as u64 * size_all_labels
        );

        for child in &children {
            child.decrease_memory_consumption_for_labels(&PromLabels::from_strings(&["key", "value"]));
            assert_eq!(
                child.current_estimated_memory_consumption_bytes(),
                size_all_labels - size_some_labels
            );
            assert!(parent.current_estimated_memory_consumption_bytes() >= 22);
        }

        assert_eq!(
            parent.current_estimated_memory_consumption_bytes(),
            routines as u64 * (size_all_labels - size_some_labels)
        );
        assert_eq!(
            parent.peak_estimated_memory_consumption_bytes(),
            routines as u64 * size_all_labels
        );

        factory.deregister(&parent);
        assert!(!factory.is_tracking(&parent));
    }

    // nested managed trackers - with memory exceeded
    {
        let parent = factory.new_memory_consumption_tracker(5, "foo + bar", None);
        assert!(factory.is_tracking(&parent));

        let routines = 5usize;
        let mut handles = Vec::with_capacity(routines);
        for _ in 0..routines {
            let parent_c = Arc::clone(&parent);
            let factory_c = Arc::clone(&factory);
            handles.push(tokio::spawn(async move {
                let child = parent_c.new_nested_memory_consumption_tracker("child", None);
                assert!(child
                    .increase_memory_consumption_by_source(10, MemoryConsumptionSource::IngesterChunks)
                    .is_err());
                factory_c.deregister(&child);
                assert!(factory_c.is_tracking(&parent_c));
                assert!(!factory_c.is_tracking(&child));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(parent.current_estimated_memory_consumption_bytes(), 0);
        assert!(factory.is_tracking(&parent));
        factory.deregister(&parent);
        assert!(!factory.is_tracking(&parent));
    }

    // panic when attempting to wrap a non managed tracker
    {
        let tracker = Arc::new(MemoryConsumptionTracker::new_with_details(
            5,
            None,
            "foo + bar",
            None,
        ));
        assert!(!factory.is_tracking(&tracker));
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = tracker.new_nested_memory_consumption_tracker("child", None);
        }));
        let err = res.unwrap_err();
        let msg = err
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| err.downcast_ref::<&str>().copied())
            .unwrap();
        assert_eq!(
            msg,
            "cannot nest a tracker not created via a InflightMemoryConsumptionTracker"
        );
    }

    // panic when attempting to wrap a nested tracker
    {
        let tracker = factory.new_memory_consumption_tracker(5, "foo + bar", None);
        assert!(factory.is_tracking(&tracker));
        let child = tracker.new_nested_memory_consumption_tracker("child", None);
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = child.new_nested_memory_consumption_tracker("child", None);
        }));
        let err = res.unwrap_err();
        let msg = err
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| err.downcast_ref::<&str>().copied())
            .unwrap();
        assert_eq!(
            msg,
            "cannot nest a tracker not created via a InflightMemoryConsumptionTracker"
        );
    }
}

// Upstream: pkg/util/limiter/memory_consumption_test.go::TestInflightMemoryConsumptionTracker_MaxAge
#[test]
fn test_inflight_memory_consumption_tracker_max_age() {
    const MAX_AGE_METRIC: &str = "cortex_querier_inflight_query_max_age_seconds";

    let new_tracker = |clock_secs: Arc<AtomicU64>| -> (Registry, Arc<InflightMemoryConsumptionTracker>) {
        let reg = Registry::new();
        let tt = InflightMemoryConsumptionTracker::new(Some(&reg), None);
        tt.set_clock(Arc::new(move || {
            UNIX_EPOCH + Duration::from_secs(clock_secs.load(Ordering::SeqCst))
        }));
        (reg, tt)
    };

    let get_max_age = |reg: &Registry| -> f64 {
        reg.gather()
            .into_iter()
            .find(|m| m.get_name() == MAX_AGE_METRIC)
            .unwrap()
            .get_metric()[0]
            .get_gauge()
            .get_value()
    };

    // no in-flight queries emits zero
    {
        let clock = Arc::new(AtomicU64::new(1_000_000));
        let (reg, _tt) = new_tracker(clock);
        assert_eq!(get_max_age(&reg), 0.0);
    }

    // single in-flight query reports its age
    {
        let clock = Arc::new(AtomicU64::new(1_000_000));
        let (reg, tt) = new_tracker(Arc::clone(&clock));
        let _q1 = tt.new_memory_consumption_tracker(0, "query1", None);
        clock.fetch_add(30, Ordering::SeqCst);
        assert_eq!(get_max_age(&reg), 30.0);
    }

    // multiple in-flight queries report the oldest age
    {
        let clock = Arc::new(AtomicU64::new(1_000_000));
        let (reg, tt) = new_tracker(Arc::clone(&clock));
        let oldest = tt.new_memory_consumption_tracker(0, "oldest", None);
        clock.fetch_add(30, Ordering::SeqCst);
        let _middle = tt.new_memory_consumption_tracker(0, "middle", None);
        clock.fetch_add(30, Ordering::SeqCst);
        let _newest = tt.new_memory_consumption_tracker(0, "newest", None);
        clock.fetch_add(30, Ordering::SeqCst);
        assert_eq!(get_max_age(&reg), 90.0);

        tt.deregister(&oldest);
        assert_eq!(get_max_age(&reg), 60.0);
    }
}

// ============================================================================
// pkg/util/limiter/series_labels_deduplicator_test.go
// ============================================================================

// Upstream: pkg/util/limiter/series_labels_deduplicator_test.go::TestSeriesDeduplicator_Deduplicate_HashCollision
#[test]
fn test_series_deduplicator_deduplicate_hash_collision() {
    const COLLISION_HASH: u64 = 12345;
    let reg = Registry::new();
    let metrics = SeriesDeduplicatorMetrics::new(Some(&reg));

    let series_a = PromLabels::from_strings(&["__name__", "metric_a", "label", "a"]);
    let series_b = PromLabels::from_strings(&["__name__", "metric_b", "label", "b"]);
    let series_c = PromLabels::from_strings(&["__name__", "metric_c", "label", "c"]);

    let a_clone = series_a.clone();
    let b_clone = series_b.clone();
    let deduplicator = SeriesLabelsDeduplicator::with_hash_func(
        metrics,
        Box::new(move |l| {
            if l == &a_clone || l == &b_clone {
                COLLISION_HASH
            } else {
                l.hash()
            }
        }),
    );
    let memory_tracker = MemoryConsumptionTracker::unlimited();

    let ret_a1 = deduplicator.deduplicate(series_a.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_a1, series_a);
    assert_eq!(deduplicator.unique_series_len(), 1);
    assert!(!deduplicator.has_conflicts());
    assert_eq!(deduplicator.series_count(), 1);

    let ret_b1 = deduplicator.deduplicate(series_b.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_b1, series_b);
    assert_eq!(deduplicator.unique_series_len(), 1);
    assert!(deduplicator.has_conflicts());
    assert_eq!(deduplicator.conflict_bucket_len(COLLISION_HASH), 1);
    assert_eq!(deduplicator.series_count(), 2);

    let ret_a2 = deduplicator.deduplicate(series_a.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_a2, ret_a1);
    assert_eq!(deduplicator.series_count(), 2);

    let ret_b2 = deduplicator.deduplicate(series_b.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_b2, ret_b1);
    assert_eq!(deduplicator.series_count(), 2);

    let ret_c1 = deduplicator.deduplicate(series_c.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_c1, series_c);
    assert_eq!(deduplicator.unique_series_len(), 2);
    assert_eq!(deduplicator.series_count(), 3);
}

// Upstream: pkg/util/limiter/series_labels_deduplicator_test.go::TestSeriesDeduplicator_Deduplicate_HashCollisionWithThreeCollidingSeries
#[test]
fn test_series_deduplicator_deduplicate_hash_collision_with_three_colliding_series() {
    const COLLISION_HASH: u64 = 12345;
    let reg = Registry::new();
    let metrics = SeriesDeduplicatorMetrics::new(Some(&reg));

    let series_a = PromLabels::from_strings(&["__name__", "metric_a", "label", "a"]);
    let series_b = PromLabels::from_strings(&["__name__", "metric_b", "label", "b"]);
    let series_c = PromLabels::from_strings(&["__name__", "metric_c", "label", "c"]);

    let deduplicator =
        SeriesLabelsDeduplicator::with_hash_func(metrics, Box::new(move |_| COLLISION_HASH));
    let memory_tracker = MemoryConsumptionTracker::unlimited();

    let ret_a = deduplicator.deduplicate(series_a.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_a, series_a);
    assert_eq!(deduplicator.unique_series_len(), 1);
    assert!(!deduplicator.has_conflicts());

    let ret_b = deduplicator.deduplicate(series_b.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_b, series_b);
    assert_eq!(deduplicator.unique_series_len(), 1);
    assert_eq!(deduplicator.conflict_bucket_len(COLLISION_HASH), 1);

    let ret_c = deduplicator.deduplicate(series_c.clone(), &memory_tracker).unwrap();
    assert_eq!(ret_c, series_c);
    assert_eq!(deduplicator.unique_series_len(), 1);
    assert_eq!(deduplicator.conflict_bucket_len(COLLISION_HASH), 2);
    assert_eq!(deduplicator.series_count(), 3);

    assert_eq!(deduplicator.deduplicate(series_a, &memory_tracker).unwrap(), ret_a);
    assert_eq!(deduplicator.deduplicate(series_b, &memory_tracker).unwrap(), ret_b);
    assert_eq!(deduplicator.deduplicate(series_c, &memory_tracker).unwrap(), ret_c);
    assert_eq!(deduplicator.series_count(), 3);
}

// Upstream: pkg/util/limiter/series_labels_deduplicator_test.go::TestSeriesDeduplicator_Deduplicate_MemoryTrackingWithDuplicates
#[test]
fn test_series_deduplicator_deduplicate_memory_tracking_with_duplicates() {
    let reg = Registry::new();
    let metrics = SeriesDeduplicatorMetrics::new(Some(&reg));
    let deduplicator = SeriesLabelsDeduplicator::new(metrics);
    let memory_tracker = MemoryConsumptionTracker::new_with_details(1_000_000, None, "test", None);

    let series_a = PromLabels::from_strings(&["__name__", "metric_a", "label", "value_a"]);
    let series_b = PromLabels::from_strings(&["__name__", "metric_b", "label", "value_b"]);

    let initial_memory = memory_tracker.current_estimated_memory_consumption_bytes();

    deduplicator.deduplicate(series_a.clone(), &memory_tracker).unwrap();
    let after_first_add = memory_tracker.current_estimated_memory_consumption_bytes();
    assert!(after_first_add > initial_memory);

    deduplicator.deduplicate(series_a, &memory_tracker).unwrap();
    let after_duplicate_a = memory_tracker.current_estimated_memory_consumption_bytes();
    assert_eq!(after_first_add, after_duplicate_a);

    deduplicator.deduplicate(series_b.clone(), &memory_tracker).unwrap();
    let after_second_add = memory_tracker.current_estimated_memory_consumption_bytes();
    assert!(after_second_add > after_duplicate_a);

    deduplicator.deduplicate(series_b, &memory_tracker).unwrap();
    let after_duplicate_b = memory_tracker.current_estimated_memory_consumption_bytes();
    assert_eq!(after_second_add, after_duplicate_b);
}

// Upstream: pkg/util/limiter/series_labels_deduplicator_test.go::TestSeriesDeduplicator_Metrics
#[test]
fn test_series_deduplicator_metrics() {
    let reg = Registry::new();
    let metrics = SeriesDeduplicatorMetrics::new(Some(&reg));
    let deduplicator = SeriesLabelsDeduplicator::new(metrics);
    let memory_tracker = MemoryConsumptionTracker::unlimited();

    let series_a = PromLabels::from_strings(&["__name__", "metric_a", "label", "value_a"]);
    let series_b = PromLabels::from_strings(&["__name__", "metric_b", "label", "value_b"]);

    deduplicator.deduplicate(series_a.clone(), &memory_tracker).unwrap();
    deduplicator.deduplicate(series_a.clone(), &memory_tracker).unwrap();
    deduplicator.deduplicate(series_b.clone(), &memory_tracker).unwrap();
    deduplicator.deduplicate(series_b, &memory_tracker).unwrap();
    deduplicator.deduplicate(series_a, &memory_tracker).unwrap();

    let mfs = reg.gather();
    let processed = mfs
        .iter()
        .find(|m| m.get_name() == "cortex_querier_labels_deduplicator_processed_total")
        .unwrap()
        .get_metric()[0]
        .get_counter()
        .get_value();
    let deduplicated = mfs
        .iter()
        .find(|m| m.get_name() == "cortex_querier_labels_deduplicator_deduplicated_total")
        .unwrap()
        .get_metric()[0]
        .get_counter()
        .get_value();
    assert_eq!(processed, 5.0);
    assert_eq!(deduplicated, 3.0);
}
