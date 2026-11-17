//! Ingester wiring of the read-path utilization limiter (CL-17), the rejection prioritizer
//! (CL-18) and the `ingester-compute` worker pool used by `LabelValuesCardinality` (CL-19).
//!
//! Ported from / mirrors:
//! - github.com/grafana/mimir/pkg/ingester/ingester.go (`NewUtilizationBasedLimiter` wiring,
//!   `newIngesterReactiveLimiter`, `workerpool.New(..., "ingester-compute", ...)`)
//! - github.com/grafana/mimir/pkg/ingester/ingester_query.go (`checkReadOverloaded`)
//! - github.com/grafana/mimir/pkg/ingester/label_names_and_values.go (`labelValuesCardinality`)

mod common;

use std::time::{Duration, Instant};

use mimir_ingester::IngesterError;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_tsdb::labels::Labels;
use prometheus::Registry;
use tempfile::TempDir;
use tonic::metadata::MetadataMap;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_value, mock_write_request,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

const USER: &str = "test";

// Go: with read_path_memory_utilization_limit=1 the limiter switches to "memory" on its first
// tick and every read RPC fails with errTooBusy (ingesterTooBusyMsg, ERROR_CAUSE_TOO_BUSY,
// codes.ResourceExhausted), counted in cortex_ingester_utilization_limited_read_requests_total.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_path_memory_utilization_limit_rejects_reads() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.read_path_memory_utilization_limit = 1;
    let reg = Registry::new();
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        Some(&reg),
    );
    assert!(metric_value(&reg, "cortex_ingester_utilization_limiter_current_cpu_load", &[])
        .is_some());
    start_and_wait_healthy(&ing).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    let err = loop {
        match ing.check_read_overloaded() {
            Err(e) => break e,
            Ok(()) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(()) => panic!("utilization limiter never engaged"),
        }
    };
    assert!(matches!(err, IngesterError::TooBusy));
    assert_eq!(
        err.to_string(),
        "ingester is currently too busy to process queries, try again later"
    );

    let res = ing
        .handle_label_values_cardinality(
            &MetadataMap::new(),
            USER,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["a".into()],
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await;
    assert!(matches!(res, Err(IngesterError::TooBusy)));
    assert!(
        metric_value(
            &reg,
            "cortex_ingester_utilization_limited_read_requests_total",
            &[("reason", "memory")]
        )
        .unwrap()
            >= 2.0
    );
    let mem = metric_value(
        &reg,
        "cortex_ingester_utilization_limiter_current_memory_usage_bytes",
        &[],
    )
    .unwrap();
    assert!(mem > 0.0);

    let _ = ing.stop().await;
}

#[tokio::test]
async fn utilization_limiter_disabled_by_default() {
    let dir = TempDir::new().unwrap();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    let reg = Registry::new();
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        Some(&reg),
    );
    assert!(ing.utilization_based_limiter.read().is_none());
    assert!(metric_value(&reg, "cortex_ingester_utilization_limiter_current_cpu_load", &[])
        .is_none());
    // Prioritizer only when both reactive limiters are enabled.
    assert!(ing.reactive_limiter.prioritizer.is_none());
    assert!(metric_value(&reg, "cortex_ingester_rejection_rate", &[]).is_none());
}

#[tokio::test]
async fn prioritizer_wired_when_push_and_read_reactive_limiters_enabled() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_reactive_limiter.enabled = true;
    cfg.read_reactive_limiter.enabled = true;
    cfg.rejection_prioritizer.calibration_interval = mimir_config::GoDuration::from_millis(20);
    let reg = Registry::new();
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        Some(&reg),
    );
    let p = ing.reactive_limiter.prioritizer.clone().expect("prioritizer");
    assert_eq!(p.registered_policies(), 2);
    assert_eq!(
        ing.reactive_limiter.calibration_interval,
        Duration::from_millis(20)
    );
    assert_eq!(metric_value(&reg, "cortex_ingester_rejection_rate", &[]), Some(0.0));
    assert_eq!(metric_value(&reg, "cortex_ingester_rejection_threshold", &[]), Some(0.0));
    start_and_wait_healthy(&ing).await;

    // Reads and pushes are admitted while not overloaded.
    ing.push_grpc(
        USER,
        &mock_write_request(&Labels::from_pairs([("__name__", "m"), ("a", "1")]), 1.0, 1000),
    )
    .await
    .unwrap();
    let res = ing
        .handle_label_values_cardinality(
            &MetadataMap::new(),
            USER,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["a".into()],
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    let _ = ing.stop().await;
}

// LabelValuesCardinality runs on the "ingester-compute" pool in chunks of
// label_values_count.chunk_size, attributed to the tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_values_cardinality_uses_compute_pool_chunks() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.compute_workers = 3;
    cfg.label_values_count.chunk_size = 4;
    let reg = Registry::new();
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        Some(&reg),
    );
    assert_eq!(ing.compute_worker_pool.workers(), 3);
    start_and_wait_healthy(&ing).await;

    let mut ts = Vec::new();
    for i in 0..10 {
        let lbls = Labels::from_pairs([("__name__", "m"), ("a", &format!("v{i}"))]);
        ts.extend(mock_write_request(&lbls, 1.0, 1000).timeseries);
    }
    ing.push_grpc(
        USER,
        &cortexpb::WriteRequest {
            timeseries: ts,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let res = ing
        .handle_label_values_cardinality(
            &MetadataMap::new(),
            USER,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["a".into(), "missing".into()],
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].items.len(), 1);
    assert_eq!(res[0].items[0].label_name, "a");
    assert_eq!(res[0].items[0].label_value_series.len(), 10);
    assert!(res[0].items[0].label_value_series.values().all(|c| *c == 1));

    // 10 values / chunk_size 4 => 3 tasks.
    let mut tasks = 0u64;
    for mf in reg.gather() {
        if mf.get_name() == "mimir_workerpool_task_duration_seconds" {
            for m in mf.get_metric() {
                let labels: Vec<_> = m
                    .get_label()
                    .iter()
                    .map(|l| (l.get_name(), l.get_value()))
                    .collect();
                assert!(labels.contains(&("pool", "ingester-compute")));
                assert!(labels.contains(&("dimension", "label-values-series-count")));
                tasks += m.get_histogram().get_sample_count();
            }
        }
    }
    assert_eq!(tasks, 3);
    assert_eq!(
        metric_value(
            &reg,
            "mimir_workerpool_queue_length",
            &[("pool", "ingester-compute"), ("user", USER)]
        ),
        Some(0.0)
    );

    let _ = ing.stop().await;
}

#[test]
fn negative_compute_workers_is_rejected_by_pool() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.compute_workers = -1;
    let res = mimir_ingester::Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        mimir_config::Overrides::new(default_limits_test_config()),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let err = res.err().expect("negative pool size must fail");
    assert!(err
        .to_string()
        .contains("creating ingester compute worker pool: worker pool size must be >= 0"));
}
