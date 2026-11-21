//! Wave 15 Ingester Query & Lifecycle regression tests (SQ-03, SQ-04, WLL-01).

mod common;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use mimir_config::{LimitsConfig, ModelDuration};
use mimir_ingester::lookupplan::SelectHints;
use mimir_ingester::mirror_querier::ChunkQuerier;
use mimir_ingester::query::PlannedTsdbChunkQuerier;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use prometheus::Registry;
use tempfile::TempDir;
use tonic::metadata::MetadataMap;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_value,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

fn hist_sample_count(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> u64 {
    let expected: HashMap<&str, &str> = labels.iter().copied().collect();
    for mf in reg.gather() {
        if mf.get_name() != name {
            continue;
        }
        for m in mf.get_metric() {
            let mut actual = HashMap::new();
            for lp in m.get_label() {
                actual.insert(lp.get_name(), lp.get_value());
            }
            if actual == expected {
                return m.get_histogram().get_sample_count();
            }
        }
    }
    0
}

fn proto_matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: t as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

type SeriesInput<'a> = (&'a str, &'a str, Vec<(i64, f64)>);

fn write_req(series: Vec<SeriesInput<'_>>) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: series
            .into_iter()
            .map(|(name, job, samples)| cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", name),
                    cortexpb::LabelPair::from_strs("job", job),
                ],
                samples: samples
                    .into_iter()
                    .map(|(t, v)| cortexpb::Sample {
                        timestamp_ms: t,
                        value: v,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sq_03_overlaps_ooo_skips_head_single_block_and_lookup_plan_metrics() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    let limits = LimitsConfig {
        out_of_order_time_window: ModelDuration::from_millis(200_000),
        ..default_limits_test_config()
    };
    let reg = Registry::new();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let user = "user-sq03";
    ing.push_grpc(user, &write_req(vec![("sq03_m", "a", vec![(1_000, 1.0)])]))
        .await
        .unwrap();
    let db = ing.get_tsdb(user).unwrap();
    db.db.compact_head_range(1_000, 2_000).unwrap().unwrap();
    db.db.reload_blocks().unwrap();

    ing.push_grpc(
        user,
        &write_req(vec![("sq03_m", "a", vec![(50_000, 2.0), (40_000, 1.5)])]),
    )
    .await
    .unwrap();
    assert!(db.db.head().min_ooo_time() <= 40_000);
    assert_eq!(db.db.head().max_ooo_time(), 40_000);

    let matchers = vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "sq03_m")];

    // Query [45_000, 60_000]: Head does NOT overlap OOO -> observes 1 Head source.
    let res_no_ooo = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 45_000,
                end_timestamp_ms: 60_000,
                matchers: matchers.clone(),
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    assert!(!res_no_ooo.is_empty());
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(1.0)
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_index")]),
        1
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_filter")]),
        1
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_tsdb_index_lookup_planning_filtered_ratio", &[("user", user)]),
        1
    );

    // Query [35_000, 60_000]: Head overlaps OOO -> queried_blocks{block="0"} increments,
    // but single_block_index / single_block_filter / lookup_plan ratios MUST NOT.
    let _ = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 35_000,
                end_timestamp_ms: 60_000,
                matchers: matchers.clone(),
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(2.0)
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_index")]),
        1
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_filter")]),
        1
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_tsdb_index_lookup_planning_filtered_ratio", &[("user", user)]),
        1
    );

    // Query [0, 60_000]: overlaps both the compacted block (+1) and Head+OOO (skipped).
    let _ = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 60_000,
                matchers,
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(3.0)
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_index")]),
        2
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_filter")]),
        2
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_tsdb_index_lookup_planning_filtered_ratio", &[("user", user)]),
        2
    );

    let _ = ing.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sq_03_unplanned_overlaps_ooo_skips_head_single_block_metrics() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = false;
    let limits = LimitsConfig {
        out_of_order_time_window: ModelDuration::from_millis(200_000),
        ..default_limits_test_config()
    };
    let reg = Registry::new();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let user = "user-sq03-unplanned";
    ing.push_grpc(
        user,
        &write_req(vec![("sq03_u", "a", vec![(50_000, 2.0), (40_000, 1.5)])]),
    )
    .await
    .unwrap();

    let matchers = vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "sq03_u")];

    // Non-OOO range [45_000, 60_000]: Head single_block_* recorded.
    let _ = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 45_000,
                end_timestamp_ms: 60_000,
                matchers: matchers.clone(),
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(1.0)
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_index")]),
        1
    );

    // OOO-overlapping range [35_000, 60_000]: Head single_block_* skipped, queried_blocks incremented.
    let _ = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 35_000,
                end_timestamp_ms: 60_000,
                matchers,
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(2.0)
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_index")]),
        1
    );

    let _ = ing.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sq_04_planned_tsdb_chunk_querier_subtracting_fallback_and_per_block_sharding() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .index_lookup_planning_comparison_portion = 1.0;
    let limits = default_limits_test_config();
    let reg = Registry::new();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let user = "user-sq04";
    let req = cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: (0..24usize)
            .map(|i| {
                let job = if i < 18 { "keep" } else { "drop" };
                let metric = if i % 2 == 0 { "sq04_even" } else { "sq04_odd" };
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", metric),
                        cortexpb::LabelPair::from_strs("idx", format!("{i}")),
                        cortexpb::LabelPair::from_strs("job", job),
                    ],
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: 10_000,
                        value: i as f64,
                    }],
                    ..Default::default()
                }
            })
            .collect(),
        ..Default::default()
    };
    ing.push_grpc(user, &req).await.unwrap();

    // 1. Subtracting-only matcher {job!="drop"} must fall back and return all 18 keep series.
    let sub_only = vec![proto_matcher(ingesterpb::MatchType::NotEqual, "job", "drop")];
    let res = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 20_000,
                matchers: sub_only,
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    let total_series: usize = res.iter().map(|r| r.streaming_series.len()).sum();
    assert_eq!(total_series, 18);

    // 2. Direct PlannedTsdbChunkQuerier::select with SelectHints must shard inside select().
    let db = ing.get_tsdb(user).unwrap();
    let planner = db.head_lookup_planner();
    let tsdb_matchers = [LabelMatcher::new(MatchType::NotEqual, "job", "drop").unwrap()];
    let mut seen_across_shards = BTreeSet::new();
    for shard_index in 0..4u64 {
        let mut pq = PlannedTsdbChunkQuerier::new_head(
            Arc::clone(&db.db),
            0,
            20_000,
            Arc::clone(&planner),
            false,
        );
        let hints = SelectHints {
            shard_index,
            shard_count: 4,
        };
        let mut ss = pq.select(true, Some(&hints), &tsdb_matchers);
        while ss.next() {
            let s = ss.at().unwrap();
            let idx_val = s.labels().get("idx").to_string();
            assert!(seen_across_shards.insert(idx_val), "duplicate series across shards");
        }
        assert!(ss.err().is_none(), "unexpected error: {:?}", ss.err());
    }
    assert_eq!(seen_across_shards.len(), 18);

    let _ = ing.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wll_01_closed_user_tsdb_returns_empty_ok_on_all_read_rpcs() {
    let dir = TempDir::new().unwrap();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    let limits = default_limits_test_config();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, None);
    start_and_wait_healthy(&ing).await;

    let user = "user-wll01";
    ing.push_grpc(user, &write_req(vec![("wll01_m", "api", vec![(5_000, 1.0)])]))
        .await
        .unwrap();

    // Close UserTsdb while keeping it in ing.tsdbs (simulating idle eviction mid-RemoveTsdbGuard).
    let db = ing.get_tsdb(user).unwrap();
    db.close().unwrap();

    let md = MetadataMap::new();
    let m_eq = vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "wll01_m")];

    // Matches Go Prometheus TSDB (head_read.go:513 "can't read from a closed head" /
    // block.go:479 "block is closing"): TSDB querier-backed RPCs fail with an error while a
    // closing/closed UserTsdb is still in ing.tsdbs so Querier fails over to healthy replicas.
    assert!(ing
        .handle_query_stream(
            &md,
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers: m_eq.clone(),
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .is_err());

    assert!(ing
        .handle_label_values(
            &md,
            user,
            ingesterpb::LabelValuesRequest {
                label_name: "job".into(),
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers: None,
                limit: 0,
            },
        )
        .await
        .is_err());

    assert!(ing
        .handle_label_names(
            &md,
            user,
            ingesterpb::LabelNamesRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers: None,
                limit: 0,
            },
        )
        .await
        .is_err());

    let us = ing
        .handle_user_stats(&md, user, ingesterpb::UserStatsRequest::default())
        .await
        .unwrap();
    assert_eq!(us.num_series, 1);

    assert!(ing
        .handle_metrics_for_label_matchers(
            &md,
            user,
            ingesterpb::MetricsForLabelMatchersRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers_set: vec![ingesterpb::LabelMatchers {
                    matchers: m_eq.clone(),
                }],
                limit: 0,
            },
        )
        .await
        .is_err());

    let mm = ing
        .handle_metrics_metadata(&md, user, ingesterpb::MetricsMetadataRequest::default())
        .await
        .unwrap();
    assert!(mm.metadata.is_empty());

    let lnv = ing
        .handle_label_names_and_values(
            &md,
            user,
            ingesterpb::LabelNamesAndValuesRequest {
                matchers: m_eq.clone(),
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await
        .unwrap();
    let _ = lnv;

    let lvc = ing
        .handle_label_values_cardinality(
            &md,
            user,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["job".into()],
                matchers: m_eq.clone(),
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await
        .unwrap();
    let _ = lvc;

    let active = ing
        .handle_active_series(
            &md,
            user,
            ingesterpb::ActiveSeriesRequest {
                matchers: m_eq.clone(),
                r#type: ingesterpb::active_series_request::RequestType::Series as i32,
            },
        )
        .await
        .unwrap();
    let _ = active;

    assert!(ing
        .handle_search_label_names(
            &md,
            user,
            ingesterpb::SearchLabelNamesRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers: m_eq.clone(),
                ..Default::default()
            },
        )
        .await
        .is_err());

    assert!(ing
        .handle_search_label_values(
            &md,
            user,
            ingesterpb::SearchLabelValuesRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                name: "job".into(),
                matchers: m_eq,
                ..Default::default()
            },
        )
        .await
        .is_err());

    let _ = ing.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tsdb_06_default_user_tsdb_unsharded_query_stream_skips_postings_cache_and_sees_new_series() {
    let dir = TempDir::new().unwrap();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    assert!(!blocks_cfg.tsdb.head_postings_for_matchers_cache_force);
    assert!(!blocks_cfg.tsdb.block_postings_for_matchers_cache_force);
    assert!(!blocks_cfg.tsdb.shared_postings_for_matchers_cache);

    let limits = default_limits_test_config();
    let reg = Registry::new();
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, Some(&reg));
    start_and_wait_healthy(&ing).await;

    let user = "user-tsdb06-default";
    ing.push_grpc(
        user,
        &write_req(vec![("tsdb06_m", "job_a", vec![(1_000, 1.0)])]),
    )
    .await
    .unwrap();

    let db = ing.get_tsdb(user).unwrap();
    assert!(!db.db.head().postings_cache().force());
    assert!(!db.db.head().postings_cache().shared());

    let matchers = vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "tsdb06_m")];
    let q1 = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers: matchers.clone(),
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    assert_eq!(q1.iter().map(|r| r.streaming_series.len()).sum::<usize>(), 1);
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_tsdb_head_postings_for_matchers_cache_skips_total",
            &[("reason", "ineligible")],
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_tsdb_head_postings_for_matchers_cache_misses_total",
            &[],
        ),
        Some(0.0)
    );

    // Push a second series and immediately run the exact same unsharded QueryStream;
    // because ineligible queries bypass the cache, the newly pushed series is visible at once.
    let start = std::time::Instant::now();
    ing.push_grpc(
        user,
        &write_req(vec![("tsdb06_m", "job_b", vec![(2_000, 2.0)])]),
    )
    .await
    .unwrap();
    let q2 = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers,
                streaming_chunks_batch_size: 0,
            },
        )
        .await
        .unwrap();
    assert!(start.elapsed() < std::time::Duration::from_secs(10));
    assert_eq!(q2.iter().map(|r| r.streaming_series.len()).sum::<usize>(), 2);
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_tsdb_head_postings_for_matchers_cache_skips_total",
            &[("reason", "ineligible")],
        ),
        Some(2.0)
    );

    let _ = ing.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rkw_03_enforce_read_consistency_bounded_by_grpc_deadline_trips_read_breaker() {
    use std::future::Future;
    use std::pin::Pin;
    use mimir_config::{CircuitBreakerConfig, GoDuration, KafkaConfig, StringSliceCSV};
    use mimir_ingester::circuit_breaker::BreakerState;
    use mimir_ingester::grpc::{RpcDeadlineContext, RPC_DEADLINE_CONTEXT};
    use mimir_ingester::ingest_storage::IngestReaderHandle;
    use mimir_storage::ingest::{
        FetchedRecord, ReaderState, RecordBatchConsumer, SingleClusterPartitionReader,
    };

    struct NoOpConsumer;
    impl RecordBatchConsumer for NoOpConsumer {
        fn consume(
            &self,
            _records: Vec<FetchedRecord>,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, mut ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.read_circuit_breaker = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 50,
        failure_execution_threshold: 2,
        thresholding_period: GoDuration::from_secs(10),
        cooldown_period: GoDuration::from_secs(30),
        request_timeout: GoDuration::from_secs(60),
        ..Default::default()
    };
    ingest_cfg.kafka.wait_strong_read_consistency_timeout = GoDuration::from_secs(30);

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

    let reader_cfg = KafkaConfig {
        address: StringSliceCSV(vec!["127.0.0.1:9092".to_string()]),
        topic: "ingest-rkw03".to_string(),
        wait_strong_read_consistency_timeout: GoDuration::from_secs(30),
        ..Default::default()
    };
    let reader = SingleClusterPartitionReader::new(
        reader_cfg,
        0,
        "ing-0",
        None,
        Arc::new(NoOpConsumer),
        Some(&reg),
    )
    .unwrap();
    reader.set_state_for_test(ReaderState::Running);
    *ing.ingest_reader.write() = Some(Arc::new(IngestReaderHandle::Single(Arc::new(reader))));

    let mut md = MetadataMap::new();
    md.insert("__consistency_level__", "strong".parse().unwrap());
    md.insert("__consistency_offsets__", "v1=0:10".parse().unwrap());
    let req = ingesterpb::QueryRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 10_000,
        matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "m")],
        streaming_chunks_batch_size: 0,
    };

    for _ in 0..2 {
        let deadline_ctx = RpcDeadlineContext::new(Some(
            std::time::Instant::now() + std::time::Duration::from_millis(50),
        ));
        let started = std::time::Instant::now();
        let res = RPC_DEADLINE_CONTEXT
            .scope(
                deadline_ctx,
                ing.handle_query_stream(&md, "user-rkw03", req.clone()),
            )
            .await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "enforce_read_consistency must not wait for 30s config timeout when grpc-timeout is 50ms (took {elapsed:?})"
        );
        let err = res.expect_err("expected DeadlineExceeded");
        assert!(err.is_deadline_exceeded(), "unexpected error: {err:?}");
    }

    let read_cb = ing.circuit_breaker.read.as_ref().unwrap();
    assert_eq!(read_cb.state(), BreakerState::Open);
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_circuit_breaker_request_timeouts_total",
            &[("request_type", "read")],
        ),
        Some(2.0)
    );

    let _ = ing.stop().await;
}
