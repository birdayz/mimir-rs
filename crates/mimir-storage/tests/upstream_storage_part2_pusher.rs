#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use helpers::{
    create_kfake_record, create_test_kafka_config, get_counter_value, mock_timeseries,
    mock_timeseries_with_exemplar, mock_timeseries_with_sample,
};
use mimir_proto::cortexpb::{self, ErrorCause};
use mimir_proto::prost::Message;
use mimir_storage::ingest::{
    BatchingQueue, BatchingQueueMetrics, ErrorSampler, FetchedRecord, ParallelStoragePusher,
    ParallelStorageShards, PreCommitNotifier, PushErrorHandler, Pusher, PusherConsumer,
    PusherConsumerMetrics, PusherError, StoragePusherMetrics,
};
use parking_lot::Mutex as SyncMutex;
use prometheus::Registry;

struct FnPusher<F>(F);

impl<F, Fut> PreCommitNotifier for FnPusher<F>
where
    F: Fn(String, cortexpb::WriteRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), PusherError>> + Send + 'static,
{
    fn notify_pre_commit(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

impl<F, Fut> Pusher for FnPusher<F>
where
    F: Fn(String, cortexpb::WriteRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), PusherError>> + Send + 'static,
{
    fn push_to_storage_and_release_request(
        &self,
        tenant_id: &str,
        request: cortexpb::WriteRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PusherError>> + Send + '_>> {
        Box::pin((self.0)(tenant_id.to_string(), request))
    }
}

fn make_fetched_records(records: Vec<(u32, &str, Vec<u8>)>) -> Vec<FetchedRecord> {
    records
        .into_iter()
        .enumerate()
        .map(|(idx, (ver, tenant, content))| {
            let (_, _, val, headers) =
                create_kfake_record("test-topic", 1, Bytes::from(content), ver);
            FetchedRecord {
                offset: idx as i64,
                timestamp_ms: 1_700_000_000_000,
                key: Bytes::copy_from_slice(tenant.as_bytes()),
                value: val,
                headers,
            }
        })
        .collect()
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestPusherConsumer
#[tokio::test]
async fn test_pusher_consumer() {
    const TENANT_ID: &str = "t1";
    let write_reqs: Vec<cortexpb::WriteRequest> = ["series_1", "series_3", "series_4", "series_5", "series_6"]
        .iter()
        .map(|name| cortexpb::WriteRequest {
            timeseries: vec![mock_timeseries(name)],
            ..Default::default()
        })
        .collect();
    let wr_bytes: Vec<Vec<u8>> = write_reqs
        .iter()
        .map(|wr| {
            let mut b = Vec::new();
            wr.encode(&mut b).unwrap();
            b
        })
        .collect();

    struct Case {
        name: &'static str,
        records: Vec<(u32, &'static str, Vec<u8>)>,
        responses: Vec<Result<(), PusherError>>,
        expected_wrs: Vec<cortexpb::WriteRequest>,
        exp_err: Option<&'static str>,
        expected_log_substrings: Vec<&'static str>,
    }

    let cases = vec![
        Case {
            name: "single record",
            records: vec![(1, TENANT_ID, wr_bytes[0].clone())],
            responses: vec![Ok(())],
            expected_wrs: write_reqs[0..1].to_vec(),
            exp_err: None,
            expected_log_substrings: vec![],
        },
        Case {
            name: "multiple records",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
                (1, TENANT_ID, wr_bytes[2].clone()),
            ],
            responses: vec![Ok(()), Ok(()), Ok(())],
            expected_wrs: write_reqs[0..3].to_vec(),
            exp_err: None,
            expected_log_substrings: vec![],
        },
        Case {
            name: "unparsable record",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, vec![0]),
                (1, TENANT_ID, wr_bytes[1].clone()),
            ],
            responses: vec![Ok(()), Ok(())],
            expected_wrs: write_reqs[0..2].to_vec(),
            exp_err: None,
            expected_log_substrings: vec!["failed to parse write request; skipping"],
        },
        Case {
            name: "mixed record versions",
            records: vec![
                (0, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
                (1, TENANT_ID, wr_bytes[2].clone()),
            ],
            responses: vec![Ok(()), Ok(()), Ok(())],
            expected_wrs: write_reqs[0..3].to_vec(),
            exp_err: None,
            expected_log_substrings: vec![],
        },
        Case {
            name: "unsupported record version",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
                (101, TENANT_ID, wr_bytes[2].clone()),
            ],
            responses: vec![Ok(()), Ok(())],
            expected_wrs: write_reqs[0..2].to_vec(),
            exp_err: None,
            expected_log_substrings: vec!["unsupported version"],
        },
        Case {
            name: "failed processing of record",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
                (1, TENANT_ID, wr_bytes[2].clone()),
            ],
            responses: vec![Ok(()), Err(PusherError::server("mock error"))],
            expected_wrs: write_reqs[0..2].to_vec(),
            exp_err: Some("mock error"),
            expected_log_substrings: vec![],
        },
        Case {
            name: "failed processing of last record",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
            ],
            responses: vec![Ok(()), Err(PusherError::server("mock error"))],
            expected_wrs: write_reqs[0..2].to_vec(),
            exp_err: Some("mock error"),
            expected_log_substrings: vec![],
        },
        Case {
            name: "failed processing & failed unmarshalling",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
                (1, TENANT_ID, vec![0]),
            ],
            responses: vec![Ok(()), Err(PusherError::server("mock error"))],
            expected_wrs: write_reqs[0..2].to_vec(),
            exp_err: Some("mock error"),
            expected_log_substrings: vec![],
        },
        Case {
            name: "no records",
            records: vec![],
            responses: vec![],
            expected_wrs: vec![],
            exp_err: None,
            expected_log_substrings: vec![],
        },
        Case {
            name: "ingester client error",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
                (1, TENANT_ID, wr_bytes[2].clone()),
            ],
            responses: vec![
                Err(PusherError::ingester(ErrorCause::BadData, "InvalidArgument", "ingester test error").with_grpc_status_prefix()),
                Err(PusherError::ingester(ErrorCause::BadData, "Unknown", "ingester test error").with_grpc_status_prefix()),
                Ok(()),
            ],
            expected_wrs: write_reqs[0..3].to_vec(),
            exp_err: None,
            expected_log_substrings: vec![
                "code = InvalidArgument desc = ingester test error",
                "code = Unknown desc = ingester test error",
            ],
        },
        Case {
            name: "ingester server error",
            records: vec![
                (1, TENANT_ID, wr_bytes[0].clone()),
                (1, TENANT_ID, wr_bytes[1].clone()),
                (1, TENANT_ID, wr_bytes[2].clone()),
                (1, TENANT_ID, wr_bytes[3].clone()),
                (1, TENANT_ID, wr_bytes[4].clone()),
            ],
            responses: vec![
                Err(PusherError::ingester(ErrorCause::BadData, "InvalidArgument", "ingester test error").with_grpc_status_prefix()),
                Err(PusherError::ingester(ErrorCause::TsdbUnavailable, "Unavailable", "ingester internal error").with_grpc_status_prefix()),
            ],
            expected_wrs: write_reqs[0..2].to_vec(),
            exp_err: Some("ingester internal error"),
            expected_log_substrings: vec!["code = InvalidArgument desc = ingester test error"],
        },
    ];

    for tc in cases {
        let received = Arc::new(AtomicI64::new(0));
        let expected_wrs = Arc::new(tc.expected_wrs);
        let responses = Arc::new(tc.responses);

        let rec_c = Arc::clone(&received);
        let exp_c = Arc::clone(&expected_wrs);
        let resp_c = Arc::clone(&responses);
        let pusher = Arc::new(FnPusher(move |tenant_id, req: cortexpb::WriteRequest| {
            let idx = rec_c.fetch_add(1, Ordering::SeqCst) as usize;
            let exp_c = Arc::clone(&exp_c);
            let resp_c = Arc::clone(&resp_c);
            async move {
                assert_eq!(tenant_id, TENANT_ID, "case {}", tc.name);
                assert!(idx < exp_c.len(), "case {} received too many reqs", tc.name);
                assert_eq!(req.timeseries, exp_c[idx].timeseries, "case {}", tc.name);
                resp_c[idx].clone()
            }
        }));

        let reg = Registry::new();
        let mut cfg = create_test_kafka_config("", "test");
        cfg.ingestion_concurrency_max = 0;
        let log_sink = Arc::new(SyncMutex::new(Vec::<String>::new()));
        let consumer = PusherConsumer::new(pusher, cfg, PusherConsumerMetrics::new(Some(&reg)))
            .with_log_sink(Arc::clone(&log_sink));

        let res = consumer.consume(make_fetched_records(tc.records)).await;
        match tc.exp_err {
            None => assert!(res.is_ok(), "case {} failed: {:?}", tc.name, res),
            Some(sub) => {
                let err = res.unwrap_err();
                assert!(err.contains(sub), "case {} err {:?} missing {}", tc.name, err, sub);
            }
        }

        let logs = log_sink.lock().clone();
        assert_eq!(logs.len(), tc.expected_log_substrings.len(), "case {} logs: {:?}", tc.name, logs);
        for (line, sub) in logs.iter().zip(tc.expected_log_substrings.iter()) {
            assert!(line.contains(sub), "case {} line {line:?} missing {sub:?}", tc.name);
        }
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestPushErrorHandler_IsServerError
#[test]
fn test_push_error_handler_is_server_error() {
    let plain_error = PusherError::server("plain");
    let cases = vec![
        ("nil sampler, plain error", None, plain_error.clone(), true, ""),
        (
            "nil sampler, sampled error",
            None,
            ErrorSampler::new(20).wrap_error(plain_error.clone()),
            true,
            "sampled 1/20",
        ),
        (
            "fallback sampler, plain error",
            Some(Arc::new(ErrorSampler::new(5))),
            plain_error.clone(),
            true,
            "sampled 1/5",
        ),
        (
            "fallback sampler, sampled error",
            Some(Arc::new(ErrorSampler::new(5))),
            ErrorSampler::new(20).wrap_error(plain_error),
            true,
            "sampled 1/20",
        ),
    ];

    for (name, sampler, err, exp_sampled, exp_reason) in cases {
        let reg = Registry::new();
        let h = PushErrorHandler::new(StoragePusherMetrics::new(Some(&reg)), sampler, None);
        let (sampled, reason) = h.should_log_client_error(&err);
        assert_eq!(sampled, exp_sampled, "{name}");
        assert_eq!(reason, exp_reason, "{name}");
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestPusherConsumer_Consume_ShouldLogErrorsHonoringOptionalLogging
#[tokio::test]
async fn test_pusher_consumer_consume_should_log_errors_honoring_optional_logging() {
    let req = cortexpb::WriteRequest {
        timeseries: vec![mock_timeseries("series_1")],
        ..Default::default()
    };
    let mut req_bytes = Vec::new();
    req.encode(&mut req_bytes).unwrap();

    let run_case = |pusher_err: PusherError| {
        let req_bytes = req_bytes.clone();
        async move {
            let pusher = Arc::new(FnPusher(move |_, _| {
                let err = pusher_err.clone();
                async move { Err(err) }
            }));
            let reg = Registry::new();
            let log_sink = Arc::new(SyncMutex::new(Vec::<String>::new()));
            let mut cfg = create_test_kafka_config("", "test");
            cfg.ingestion_concurrency_max = 0;
            let consumer = PusherConsumer::new(pusher, cfg, PusherConsumerMetrics::new(Some(&reg)))
                .with_log_sink(Arc::clone(&log_sink));
            consumer
                .consume(make_fetched_records(vec![(1, "user-1", req_bytes)]))
                .await
                .unwrap();

            assert_eq!(
                get_counter_value(
                    &reg,
                    "cortex_ingest_storage_reader_requests_failed_total",
                    &[("cause", "client")]
                ),
                1.0
            );
            assert_eq!(
                get_counter_value(
                    &reg,
                    "cortex_ingest_storage_reader_requests_failed_total",
                    &[("cause", "server")]
                ),
                0.0
            );
            let out = log_sink.lock().join("\n");
            out
        }
    };

    // 1. no optional logging
    let err1 = PusherError::ingester(ErrorCause::BadData, "InvalidArgument", "mocked error");
    let logs1 = run_case(err1.clone()).await;
    assert!(logs1.contains(&err1.to_string()), "{logs1}");

    // 2. sampled 1/100 -> logs with "(sampled 1/100)"
    let err2 = ErrorSampler::new(100).wrap_error(err1.clone());
    let logs2 = run_case(err2).await;
    assert!(logs2.contains(&format!("{} (sampled 1/100)", err1)), "{logs2}");

    // 3. DoNotLog -> empty logs
    let err3 = err1.with_do_not_log();
    let logs3 = run_case(err3).await;
    assert!(logs3.is_empty(), "expected empty logs, got {logs3}");
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestPusherConsumer_Consume_ShouldNotLeakGoroutinesOnServerError
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pusher_consumer_consume_should_not_leak_goroutines_on_server_error() {
    const TENANT_ID: &str = "tenant-1";
    const NUM_WRITE_REQUESTS: usize = 200;

    let mut records = Vec::with_capacity(NUM_WRITE_REQUESTS);
    for i in 0..NUM_WRITE_REQUESTS {
        let req = cortexpb::WriteRequest {
            timeseries: vec![mock_timeseries(&format!("series_{i}"))],
            ..Default::default()
        };
        let mut b = Vec::new();
        req.encode(&mut b).unwrap();
        records.push((1u32, TENANT_ID, b));
    }
    let fetched = make_fetched_records(records);

    let mut cfgs = Vec::new();
    {
        let mut c = create_test_kafka_config("", "test");
        c.ingestion_concurrency_max = 0;
        cfgs.push(("disabled", c));
    }
    for (name, max_c, q_cap, b_sz) in [
        ("max=2,q=1,b=1", 2, 1, 1),
        ("max=2,q=5,b=1", 2, 5, 1),
        ("max=2,q=5,b=5", 2, 5, 5),
    ] {
        let mut c = create_test_kafka_config("", "test");
        c.ingestion_concurrency_max = max_c;
        c.ingestion_concurrency_estimated_bytes_per_sample = 1;
        c.ingestion_concurrency_target_flushes_per_shard = 1;
        c.ingestion_concurrency_batch_size = b_sz;
        c.ingestion_concurrency_queue_capacity = q_cap;
        cfgs.push((name, c));
    }

    for (sc_name, cfg) in cfgs {
        for fast in [true, false] {
            for mode in ["all_fail", "first_fail", "tenth_fail"] {
                let push_count = Arc::new(AtomicI64::new(0));
                let pc = Arc::clone(&push_count);
                let pusher = Arc::new(FnPusher(move |_, _| {
                    let pc = Arc::clone(&pc);
                    async move {
                        if !fast {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                        let n = pc.fetch_add(1, Ordering::SeqCst) + 1;
                        let fail = match mode {
                            "all_fail" => true,
                            "first_fail" => n == 1,
                            _ => n == 10,
                        };
                        if fail {
                            Err(PusherError::server("mocked server error"))
                        } else {
                            Ok(())
                        }
                    }
                }));

                let reg = Registry::new();
                let consumer =
                    PusherConsumer::new(pusher, cfg.clone(), PusherConsumerMetrics::new(Some(&reg)));
                let err = consumer.consume(fetched.clone()).await.unwrap_err();
                assert!(
                    err.contains("consuming record") && err.contains("mocked server error"),
                    "scenario {sc_name} fast={fast} mode={mode}: unexpected err {err}"
                );
            }
        }
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestParallelStorageShards_ShardWriteRequest
#[tokio::test]
async fn test_parallel_storage_shards_shard_write_request() {
    // Helper to run a test case against ParallelStorageShards
    struct Case {
        name: &'static str,
        shard_count: usize,
        batch_size: usize,
        requests: Vec<cortexpb::WriteRequest>,
        expected_errs: Option<Vec<bool>>,
        expected_errs_count: Option<usize>,
        expected_upstream_pushes: Vec<cortexpb::WriteRequest>,
        upstream_push_errs: Vec<Option<&'static str>>,
        expected_close_err: Option<&'static str>,
        verify_unordered_series_order: bool,
    }

    let cases = vec![
        Case {
            name: "push to a single shard and fill exactly capacity",
            shard_count: 1,
            batch_size: 2,
            requests: vec![
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_1_1")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_2_1")], ..Default::default() },
            ],
            expected_errs: Some(vec![false, false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![cortexpb::WriteRequest {
                timeseries: vec![mock_timeseries("series_1_1"), mock_timeseries("series_2_1")],
                ..Default::default()
            }],
            upstream_push_errs: vec![None],
            expected_close_err: None,
            verify_unordered_series_order: false,
        },
        Case {
            name: "push to multiple shards and fill exact capacity",
            shard_count: 2,
            batch_size: 2,
            requests: vec![
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_1_2")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_2_2")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_2")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_4_2")], ..Default::default() },
            ],
            expected_errs: Some(vec![false, false, false, false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries("series_1_2"), mock_timeseries("series_3_2")],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries("series_2_2"), mock_timeseries("series_4_2")],
                    ..Default::default()
                },
            ],
            upstream_push_errs: vec![None, None],
            expected_close_err: None,
            verify_unordered_series_order: false,
        },
        Case {
            name: "push to single shard and underfill capacity",
            shard_count: 1,
            batch_size: 2,
            requests: vec![
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_1_3")], ..Default::default() },
            ],
            expected_errs: Some(vec![false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![cortexpb::WriteRequest {
                timeseries: vec![mock_timeseries("series_1_3")],
                ..Default::default()
            }],
            upstream_push_errs: vec![None],
            expected_close_err: None,
            verify_unordered_series_order: false,
        },
        Case {
            name: "push to single shard and overfill capacity",
            shard_count: 1,
            batch_size: 2,
            requests: vec![
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_1_4")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_2_4")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_4")], ..Default::default() },
            ],
            expected_errs: Some(vec![false, false, false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries("series_1_4"), mock_timeseries("series_2_4")],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries("series_3_4")],
                    ..Default::default()
                },
            ],
            upstream_push_errs: vec![None, None],
            expected_close_err: None,
            verify_unordered_series_order: false,
        },
        Case {
            name: "push to single shard and overfill only with the series of a single request",
            shard_count: 1,
            batch_size: 2,
            requests: vec![cortexpb::WriteRequest {
                timeseries: vec![
                    mock_timeseries("series_1_5"),
                    mock_timeseries("series_2_5"),
                    mock_timeseries("series_3_5"),
                ],
                ..Default::default()
            }],
            expected_errs: Some(vec![false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries("series_1_5"), mock_timeseries("series_2_5")],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries("series_3_5")],
                    ..Default::default()
                },
            ],
            upstream_push_errs: vec![None, None],
            expected_close_err: None,
            verify_unordered_series_order: false,
        },
        Case {
            name: "push to single shard and get an error with an underfilled shard",
            shard_count: 1,
            batch_size: 2,
            requests: vec![
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_1_7")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_2_7")], ..Default::default() },
            ],
            expected_errs: Some(vec![false, false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![cortexpb::WriteRequest {
                timeseries: vec![mock_timeseries("series_1_7"), mock_timeseries("series_2_7")],
                ..Default::default()
            }],
            upstream_push_errs: vec![Some("assert error")],
            expected_close_err: Some("assert error"),
            verify_unordered_series_order: false,
        },
        Case {
            name: "push to single shard and get an error with an overfilled shard",
            shard_count: 1,
            batch_size: 2,
            requests: vec![
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_1_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_2_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8")], ..Default::default() },
            ],
            expected_errs: None,
            expected_errs_count: Some(1),
            expected_upstream_pushes: vec![
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_1_8"), mock_timeseries("series_2_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8"), mock_timeseries("series_3_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8"), mock_timeseries("series_3_8")], ..Default::default() },
                cortexpb::WriteRequest { timeseries: vec![mock_timeseries("series_3_8"), mock_timeseries("series_3_8")], ..Default::default() },
            ],
            upstream_push_errs: vec![Some("assert error"), None, None, None],
            expected_close_err: None,
            verify_unordered_series_order: false,
        },
        Case {
            name: "push with metadata and exemplars",
            shard_count: 1,
            batch_size: 3,
            requests: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries_with_exemplar("series_1_10")],
                    metadata: vec![cortexpb::MetricMetadata {
                        metric_family_name: "series_1_10".to_string(),
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                        help: "A test counter".to_string(),
                        unit: "bytes".to_string(),
                    }],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![mock_timeseries_with_exemplar("series_2_10")],
                    metadata: vec![cortexpb::MetricMetadata {
                        metric_family_name: "series_2_10".to_string(),
                        r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                        help: "A test gauge".to_string(),
                        unit: "seconds".to_string(),
                    }],
                    ..Default::default()
                },
            ],
            expected_errs: Some(vec![false, false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![
                        mock_timeseries_with_exemplar("series_1_10"),
                        mock_timeseries_with_exemplar("series_2_10"),
                    ],
                    metadata: vec![cortexpb::MetricMetadata {
                        metric_family_name: "series_1_10".to_string(),
                        r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                        help: "A test counter".to_string(),
                        unit: "bytes".to_string(),
                    }],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    metadata: vec![cortexpb::MetricMetadata {
                        metric_family_name: "series_2_10".to_string(),
                        r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                        help: "A test gauge".to_string(),
                        unit: "seconds".to_string(),
                    }],
                    ..Default::default()
                },
            ],
            upstream_push_errs: vec![None, None],
            expected_close_err: None,
            verify_unordered_series_order: false,
        },
        Case {
            name: "preserving sample order between requests",
            shard_count: 4,
            batch_size: 4,
            requests: vec![
                cortexpb::WriteRequest {
                    timeseries: vec![
                        mock_timeseries_with_sample("series_a", 1, 2.0),
                        mock_timeseries_with_sample("series_b", 1, 2.0),
                        mock_timeseries_with_sample("series_c", 1, 2.0),
                    ],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![
                        mock_timeseries_with_sample("series_b", 2, 3.0),
                        mock_timeseries_with_sample("series_a", 2, 3.0),
                        mock_timeseries_with_sample("series_c", 2, 3.0),
                    ],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![
                        mock_timeseries_with_sample("series_b", 3, 4.0),
                        mock_timeseries_with_sample("series_c", 3, 4.0),
                        mock_timeseries_with_sample("series_a", 3, 4.0),
                    ],
                    ..Default::default()
                },
                cortexpb::WriteRequest {
                    timeseries: vec![
                        mock_timeseries_with_sample("series_c", 4, 5.0),
                        mock_timeseries_with_sample("series_b", 4, 5.0),
                        mock_timeseries_with_sample("series_a", 4, 5.0),
                    ],
                    ..Default::default()
                },
            ],
            expected_errs: Some(vec![false, false, false, false]),
            expected_errs_count: None,
            expected_upstream_pushes: vec![],
            upstream_push_errs: vec![],
            expected_close_err: None,
            verify_unordered_series_order: true,
        },
    ];

    for tc in cases {
        let seen_pushes = Arc::new(SyncMutex::new(Vec::<cortexpb::WriteRequest>::new()));
        let expected_pushes = Arc::new(tc.expected_upstream_pushes);
        let push_errs = Arc::new(tc.upstream_push_errs);

        let seen_c = Arc::clone(&seen_pushes);
        let exp_c = Arc::clone(&expected_pushes);
        let err_c = Arc::clone(&push_errs);
        let pusher = Arc::new(FnPusher(move |_, req: cortexpb::WriteRequest| {
            let seen_c = Arc::clone(&seen_c);
            let exp_c = Arc::clone(&exp_c);
            let err_c = Arc::clone(&err_c);
            async move {
                let mut g = seen_c.lock();
                g.push(req.clone());
                if tc.verify_unordered_series_order {
                    return Ok(());
                }
                for (i, exp) in exp_c.iter().enumerate() {
                    if *exp == req {
                        if let Some(msg) = err_c[i] {
                            return Err(PusherError::server(msg));
                        }
                        return Ok(());
                    }
                }
                Ok(())
            }
        }));

        let reg = Registry::new();
        let metrics = StoragePusherMetrics::new(Some(&reg));
        let eh = Arc::new(PushErrorHandler::new(metrics.clone(), None, None));
        let mut shards =
            ParallelStorageShards::new(metrics, eh, tc.shard_count, tc.batch_size, 1, pusher);

        let mut actual_errs = Vec::new();
        for req in tc.requests {
            let res = shards.push_to_storage_and_release_request("t1", req).await;
            actual_errs.push(res.is_err());
            if tc.expected_errs_count.is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        if let Some(ref exp) = tc.expected_errs {
            assert_eq!(&actual_errs, exp, "case {}", tc.name);
        } else if let Some(cnt) = tc.expected_errs_count {
            let got = actual_errs.iter().filter(|&&b| b).count();
            assert!(got >= cnt, "case {} got {got} errs < {cnt}", tc.name);
        }

        let close_errs = shards.close().await;
        if let Some(exp_ce) = tc.expected_close_err {
            assert_eq!(close_errs.len(), 1, "case {}", tc.name);
            assert!(close_errs[0].to_string().contains(exp_ce));
        } else {
            assert!(close_errs.is_empty(), "case {} close_errs: {:?}", tc.name, close_errs);
        }

        if tc.verify_unordered_series_order {
            let mut by_series: HashMap<String, Vec<i64>> = HashMap::new();
            for wr in seen_pushes.lock().iter() {
                for ts in &wr.timeseries {
                    let name = String::from_utf8_lossy(&ts.labels[0].value).into_owned();
                    by_series
                        .entry(name)
                        .or_default()
                        .extend(ts.samples.iter().map(|s| s.timestamp_ms));
                }
            }
            for s in ["series_a", "series_b", "series_c"] {
                assert_eq!(by_series.get(s).unwrap(), &vec![1, 2, 3, 4]);
            }
        } else {
            assert_eq!(seen_pushes.lock().len(), expected_pushes.len(), "case {}", tc.name);
            let err_cnt = push_errs.iter().filter(|e| e.is_some()).count();
            assert_eq!(
                get_counter_value(&reg, "cortex_ingest_storage_reader_requests_total", &[]),
                expected_pushes.len() as f64
            );
            assert_eq!(
                get_counter_value(
                    &reg,
                    "cortex_ingest_storage_reader_requests_failed_total",
                    &[("cause", "server")]
                ),
                err_cnt as f64
            );
            assert_eq!(
                get_counter_value(
                    &reg,
                    "cortex_ingest_storage_reader_requests_failed_total",
                    &[("cause", "client")]
                ),
                0.0
            );
        }
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestParallelStoragePusher
#[tokio::test]
async fn test_parallel_storage_pusher() {
    let cases = vec![
        (
            "separate tenants and sources",
            vec![
                (
                    "tenant1",
                    cortexpb::WriteRequest {
                        timeseries: vec![mock_timeseries_with_exemplar("series_1")],
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    },
                ),
                (
                    "tenant1",
                    cortexpb::WriteRequest {
                        timeseries: vec![mock_timeseries_with_exemplar("series_2")],
                        source: cortexpb::write_request::SourceEnum::Rule as i32,
                        ..Default::default()
                    },
                ),
                (
                    "tenant2",
                    cortexpb::WriteRequest {
                        timeseries: vec![mock_timeseries_with_exemplar("series_3")],
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    },
                ),
                (
                    "tenant2",
                    cortexpb::WriteRequest {
                        timeseries: vec![mock_timeseries_with_exemplar("series_4")],
                        source: cortexpb::write_request::SourceEnum::Rule as i32,
                        ..Default::default()
                    },
                ),
            ],
            vec![
                ("tenant1", cortexpb::write_request::SourceEnum::Api as i32, 1),
                ("tenant1", cortexpb::write_request::SourceEnum::Rule as i32, 1),
                ("tenant2", cortexpb::write_request::SourceEnum::Api as i32, 1),
                ("tenant2", cortexpb::write_request::SourceEnum::Rule as i32, 1),
            ],
        ),
        (
            "metadata-only requests",
            vec![
                (
                    "tenant1",
                    cortexpb::WriteRequest {
                        metadata: vec![cortexpb::MetricMetadata {
                            metric_family_name: "metric1".to_string(),
                            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
                            help: "A test counter".to_string(),
                            unit: "bytes".to_string(),
                        }],
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    },
                ),
                (
                    "tenant1",
                    cortexpb::WriteRequest {
                        metadata: vec![cortexpb::MetricMetadata {
                            metric_family_name: "metric2".to_string(),
                            r#type: cortexpb::metric_metadata::MetricType::Gauge as i32,
                            help: "A test gauge".to_string(),
                            unit: "seconds".to_string(),
                        }],
                        source: cortexpb::write_request::SourceEnum::Rule as i32,
                        ..Default::default()
                    },
                ),
                (
                    "tenant2",
                    cortexpb::WriteRequest {
                        metadata: vec![cortexpb::MetricMetadata {
                            metric_family_name: "metric3".to_string(),
                            r#type: cortexpb::metric_metadata::MetricType::Histogram as i32,
                            help: "A test histogram".to_string(),
                            unit: "bytes".to_string(),
                        }],
                        source: cortexpb::write_request::SourceEnum::Api as i32,
                        ..Default::default()
                    },
                ),
            ],
            vec![
                ("tenant1", cortexpb::write_request::SourceEnum::Api as i32, 1),
                ("tenant1", cortexpb::write_request::SourceEnum::Rule as i32, 1),
                ("tenant2", cortexpb::write_request::SourceEnum::Api as i32, 1),
            ],
        ),
    ];

    for (name, reqs, expected) in cases {
        let received = Arc::new(SyncMutex::new(HashMap::<(String, i32), usize>::new()));
        let rec_c = Arc::clone(&received);
        let pusher = Arc::new(FnPusher(move |tenant_id, req: cortexpb::WriteRequest| {
            let rec_c = Arc::clone(&rec_c);
            async move {
                *rec_c.lock().entry((tenant_id, req.source)).or_insert(0) += 1;
                Ok(())
            }
        }));

        let mut samples_per_tenant = HashMap::new();
        for (t, r) in &reqs {
            *samples_per_tenant.entry((*t).to_string()).or_insert(0) += r.timeseries.len();
        }
        let reg = Registry::new();
        let metrics = StoragePusherMetrics::new(Some(&reg));
        let eh = Arc::new(PushErrorHandler::new(metrics.clone(), None, None));
        let mut psp = ParallelStoragePusher::new(
            metrics,
            pusher,
            samples_per_tenant,
            eh,
            1,
            1,
            5,
            500,
            80,
        );

        for (t, r) in reqs {
            psp.push_to_storage_and_release_request(t, r).await.unwrap();
        }
        let errs = psp.close().await;
        assert!(errs.is_empty(), "{name}");

        let got = received.lock();
        assert_eq!(got.len(), expected.len(), "{name}");
        for (t, s, cnt) in expected {
            assert_eq!(got.get(&(t.to_string(), s)).copied().unwrap_or(0), cnt, "{name}");
        }
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestParallelStoragePusher_idealShardsFor
#[test]
fn test_parallel_storage_pusher_ideal_shards_for() {
    let cases = [
        (1000, 100, 10, 1, 1, 1),
        (10000, 100, 10, 1, 15, 10),
        (100000, 100, 10, 1, 5, 5),
        (20000, 200, 5, 2, 15, 10),
        (500, 100, 10, 1, 10, 1),
    ];

    for (bytes_per_tenant, bytes_per_sample, batch_size, target_flushes, max_shards, expected) in
        cases
    {
        let reg = Registry::new();
        let metrics = StoragePusherMetrics::new(Some(&reg));
        let eh = Arc::new(PushErrorHandler::new(metrics.clone(), None, None));
        let pusher = Arc::new(FnPusher(|_, _| async { Ok(()) }));
        let mut bytes_map = HashMap::new();
        bytes_map.insert("tenant1".to_string(), bytes_per_tenant);
        let mut psp = ParallelStoragePusher::new(
            metrics,
            pusher,
            bytes_map,
            eh,
            max_shards,
            batch_size,
            5,
            bytes_per_sample,
            target_flushes,
        );
        assert_eq!(psp.ideal_shards_for("tenant1"), expected);
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestParallelStoragePusher_Fuzzy
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_parallel_storage_pusher_fuzzy() {
    const NUM_TENANTS: usize = 10;
    const NUM_WRITE_REQUESTS: usize = 50;
    const NUM_SERIES_PER_WRITE_REQUEST: usize = 50;

    let server_errs_count = Arc::new(AtomicI64::new(0));
    let pushed_per_tenant = Arc::new(SyncMutex::new(HashMap::<String, Vec<String>>::new()));

    let sec = Arc::clone(&server_errs_count);
    let ppt = Arc::clone(&pushed_per_tenant);
    let call_num = Arc::new(AtomicI64::new(0));
    let cn_c = Arc::clone(&call_num);

    let pusher = Arc::new(FnPusher(move |tenant_id, req: cortexpb::WriteRequest| {
        let sec = Arc::clone(&sec);
        let ppt = Arc::clone(&ppt);
        let cn = Arc::clone(&cn_c);
        async move {
            {
                let mut g = ppt.lock();
                let entry = g.entry(tenant_id).or_default();
                for ts in &req.timeseries {
                    entry.push(String::from_utf8_lossy(&ts.labels[0].value).into_owned());
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            if cn.fetch_add(1, Ordering::Relaxed) % 1000 == 500 {
                sec.fetch_add(1, Ordering::SeqCst);
                return Err(PusherError::server("mocked server error"));
            }
            Ok(())
        }
    }));

    let mut bytes_per_tenant = HashMap::new();
    for tid in 0..NUM_TENANTS {
        bytes_per_tenant.insert(format!("tenant-{tid}"), 5usize);
    }
    let reg = Registry::new();
    let metrics = StoragePusherMetrics::new(Some(&reg));
    let eh = Arc::new(PushErrorHandler::new(metrics.clone(), None, None));
    let mut psp = ParallelStoragePusher::new(metrics, pusher, bytes_per_tenant, eh, 10, 1, 5, 1, 1);

    let mut enqueued_per_tenant: HashMap<String, Vec<String>> = HashMap::new();
    for batch_id in 0..NUM_WRITE_REQUESTS {
        let tid = format!("tenant-{}", batch_id % NUM_TENANTS);
        assert!(psp.ideal_shards_for(&tid) > 1);
        let mut ts_list = Vec::with_capacity(NUM_SERIES_PER_WRITE_REQUEST);
        let mut names = Vec::with_capacity(NUM_SERIES_PER_WRITE_REQUEST);
        for i in 0..NUM_SERIES_PER_WRITE_REQUEST {
            let name = format!("series_{batch_id}_{i}");
            names.push(name.clone());
            ts_list.push(mock_timeseries(&name));
        }
        let req = cortexpb::WriteRequest {
            timeseries: ts_list,
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        if psp
            .push_to_storage_and_release_request(&tid, req)
            .await
            .is_ok()
        {
            enqueued_per_tenant.entry(tid).or_default().extend(names);
        } else {
            assert!(server_errs_count.load(Ordering::SeqCst) > 0);
            break;
        }
    }

    let errs = psp.close().await;
    if !errs.is_empty() {
        assert!(server_errs_count.load(Ordering::SeqCst) > 0);
    }

    let pushed = pushed_per_tenant.lock();
    for (tid, expected_names) in enqueued_per_tenant {
        let got = pushed.get(&tid).unwrap();
        for name in expected_names {
            assert!(got.contains(&name), "missing {name} for {tid}");
        }
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestBatchingQueue_NoDeadlock
#[tokio::test]
async fn test_batching_queue_no_deadlock() {
    let capacity = 2usize;
    let batch_size = 3usize;
    let reg = Registry::new();
    let m = BatchingQueueMetrics::new(Some(&reg));
    let (mut queue, mut rx) = BatchingQueue::new(capacity, batch_size, m);
    let done = queue.done_handle();
    let errs = queue.errors_handle();

    let h = tokio::spawn(async move {
        while let Some(_wr) = rx.recv().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
            errs.lock().push(PusherError::server("mock error"));
        }
        done.done();
    });

    let series = mock_timeseries("series_1");
    for _ in 0..batch_size * (capacity + 1) {
        queue
            .add_to_batch("t1", 0, series.clone())
            .await
            .unwrap();
    }

    let close_errs = queue.close().await;
    assert!(!close_errs.is_empty());
    assert!(close_errs.iter().any(|e| e.to_string().contains("mock error")));
    h.await.unwrap();

    assert_eq!(queue.pending_errors_len(), 0);
    assert_eq!(queue.current_batch_len(), 0);
    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_reader_batching_queue_flush_errors_total",
            &[]
        ),
        0.0
    );
    assert_eq!(
        get_counter_value(
            &reg,
            "cortex_ingest_storage_reader_batching_queue_flush_total",
            &[]
        ),
        3.0
    );
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestBatchingQueue
#[tokio::test]
async fn test_batching_queue() {
    let capacity = 5usize;
    let batch_size = 3usize;
    let s1 = mock_timeseries("series_1");
    let s2 = mock_timeseries("series_2");

    // "batch not flushed because batch size is 3 and we have 2 items in the queue"
    {
        let reg = Registry::new();
        let (mut q, mut rx) = BatchingQueue::new(capacity, batch_size, BatchingQueueMetrics::new(Some(&reg)));
        q.add_to_batch("t1", 0, s1.clone()).await.unwrap();
        q.add_to_batch("t1", 0, s2.clone()).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(50), rx.recv()).await.is_err());
    }

    // "batch flushed because batch size is 3 and we have 3 items in the queue"
    {
        let reg = Registry::new();
        let (mut q, mut rx) = BatchingQueue::new(capacity, batch_size, BatchingQueueMetrics::new(Some(&reg)));
        q.add_to_batch("t1", 0, s1.clone()).await.unwrap();
        q.add_to_batch("t1", 0, s2.clone()).await.unwrap();
        let s3 = mock_timeseries("series_3");
        q.add_to_batch("t1", 0, s3.clone()).await.unwrap();
        let batch = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.write_request.timeseries, vec![s1.clone(), s2.clone(), s3]);
        assert_eq!(q.current_batch_len(), 0);
    }

    // "if you close the queue with items in the queue, the queue should flush the items"
    {
        let reg = Registry::new();
        let (mut q, mut rx) = BatchingQueue::new(capacity, batch_size, BatchingQueueMetrics::new(Some(&reg)));
        q.add_to_batch("t1", 0, s1.clone()).await.unwrap();
        q.add_to_batch("t1", 0, s2.clone()).await.unwrap();
        let done = q.done_handle();
        let h = tokio::spawn(async move {
            let mut batches = Vec::new();
            while let Some(b) = rx.recv().await {
                batches.push(b);
            }
            done.done();
            batches
        });
        assert!(q.close().await.is_empty());
        let batches = h.await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].write_request.timeseries, vec![s1.clone(), s2.clone()]);
    }

    // "test queue capacity"
    {
        let reg = Registry::new();
        let (mut q, mut rx) = BatchingQueue::new(capacity, batch_size, BatchingQueueMetrics::new(Some(&reg)));
        for i in 0..capacity * batch_size {
            q.add_to_batch("t1", 0, mock_timeseries(&format!("series_{i}")))
                .await
                .unwrap();
        }
        assert_eq!(q.current_batch_len(), 0);
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.write_request.timeseries.len(), 3);
        for _ in 0..3 {
            q.add_to_batch("t1", 0, mock_timeseries("series_100"))
                .await
                .unwrap();
        }
        assert_eq!(q.current_batch_len(), 0);
    }

    // "metadata and exemplars are preserved in batches"
    {
        let reg = Registry::new();
        let (mut q, mut rx) = BatchingQueue::new(2, 2, BatchingQueueMetrics::new(Some(&reg)));
        let mut ts = mock_timeseries("series_1");
        ts.exemplars.push(cortexpb::Exemplar {
            value: 42.0,
            timestamp_ms: 1234567890,
            labels: vec![cortexpb::LabelPair::from_static("trace_id", "abc123")],
        });
        let md = cortexpb::MetricMetadata {
            r#type: cortexpb::metric_metadata::MetricType::Counter as i32,
            metric_family_name: "test_counter".to_string(),
            help: "A test counter".to_string(),
            unit: "bytes".to_string(),
        };
        q.add_to_batch("t1", 0, ts).await.unwrap();
        q.add_metadata_to_batch("t1", 0, md.clone()).await.unwrap();
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.write_request.timeseries.len(), 1);
        assert_eq!(batch.write_request.metadata, vec![md]);
        assert_eq!(batch.write_request.timeseries[0].exemplars[0].value, 42.0);
    }
}

// Upstream: pkg/storage/ingest/pusher_test.go::TestBatchingQueue_ErrorHandling
#[tokio::test]
async fn test_batching_queue_error_handling() {
    let s1 = mock_timeseries("series_1");
    let s2 = mock_timeseries("series_2");

    // "AddToBatch returns all errors and it pushes the batch when the batch is filled"
    {
        let reg = Registry::new();
        let (mut q, mut rx) = BatchingQueue::new(2, 2, BatchingQueueMetrics::new(Some(&reg)));
        q.add_to_batch("t1", 0, s2.clone()).await.unwrap();
        q.report_error(Some(PusherError::server("mock error 1")));
        q.report_error(Some(PusherError::server("mock error 2")));
        let err = q.add_to_batch("t1", 0, s2.clone()).await.unwrap_err();
        assert_eq!(err, "2 errors: mock error 1; mock error 2");
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.write_request.timeseries, vec![s2.clone(), s2.clone()]);
        q.add_to_batch("t1", 0, s2.clone()).await.unwrap();
        q.add_to_batch("t1", 0, s2.clone()).await.unwrap();
    }

    // "Any errors pushed after last AddToBatch call are received on Close"
    {
        let reg = Registry::new();
        let (mut q, mut rx) = BatchingQueue::new(2, 2, BatchingQueueMetrics::new(Some(&reg)));
        q.add_to_batch("t1", 0, s1.clone()).await.unwrap();
        assert!(rx.try_recv().is_err());
        q.report_error(Some(PusherError::server("mock error 1")));
        q.report_error(Some(PusherError::server("mock error 2")));
        q.done();
        let errs = q.close().await;
        assert_eq!(errs.len(), 2);
        assert_eq!(errs[0].to_string(), "mock error 1");
        assert_eq!(errs[1].to_string(), "mock error 2");
        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.write_request.timeseries, vec![s1]);
    }
}

#[tokio::test]
async fn w27_a_03_pusher_error_formats_bare_message_without_grpc_prefix() {
    let client_err = PusherError::ingester(
        ErrorCause::BadData,
        "InvalidArgument",
        "user=t1: sample out of bounds",
    );
    assert_eq!(client_err.to_string(), "user=t1: sample out of bounds");

    let server_err = PusherError::ingester(
        ErrorCause::TsdbUnavailable,
        "Unavailable",
        "tsdb closing",
    );
    assert_eq!(server_err.to_string(), "tsdb closing");

    let req = cortexpb::WriteRequest {
        timeseries: vec![mock_timeseries("s_w27_a_03")],
        ..Default::default()
    };
    let mut wr_bytes = Vec::new();
    req.encode(&mut wr_bytes).unwrap();

    // Client error log must contain bare message without "rpc error: code = ... desc = ".
    let log_sink = Arc::new(SyncMutex::new(Vec::<String>::new()));
    let c_err = client_err.clone();
    let pusher_client = Arc::new(FnPusher(move |_, _| {
        let e = c_err.clone();
        async move { Err(e) }
    }));
    let mut cfg = create_test_kafka_config("", "test");
    cfg.ingestion_concurrency_max = 0;
    let consumer = PusherConsumer::new(
        pusher_client,
        cfg.clone(),
        PusherConsumerMetrics::new(Some(&Registry::new())),
    )
    .with_log_sink(Arc::clone(&log_sink));
    consumer
        .consume(make_fetched_records(vec![(1, "t1", wr_bytes.clone())]))
        .await
        .unwrap();
    let logs = log_sink.lock().clone();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].contains("err=\"user=t1: sample out of bounds\""), "logs: {logs:?}");
    assert!(!logs[0].contains("rpc error: code ="), "logs: {logs:?}");

    // Server error return must wrap bare message without "rpc error: code = ... desc = ".
    let s_err = server_err.clone();
    let pusher_server = Arc::new(FnPusher(move |_, _| {
        let e = s_err.clone();
        async move { Err(e) }
    }));
    let consumer_srv = PusherConsumer::new(
        pusher_server,
        cfg,
        PusherConsumerMetrics::new(Some(&Registry::new())),
    );
    let err = consumer_srv
        .consume(make_fetched_records(vec![(1, "t1", wr_bytes)]))
        .await
        .unwrap_err();
    assert_eq!(err, "consuming record at offset 0 for tenant t1: tsdb closing");
}
