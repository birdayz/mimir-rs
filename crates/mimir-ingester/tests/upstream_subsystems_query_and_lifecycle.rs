mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use mimir_common::services::State;
use mimir_config::GoDuration;
use mimir_ingester::activity::{
    default_metrics_metadata_request, query_request_to_string, request_activity, ActivityRequest,
};
use mimir_ingester::http::ingester_router;
use mimir_ingester::mirror_querier::{
    ChunkBlockMeta, ChunkQuerier, ChunkSeries, ChunkSeriesSet, MirroredChunkQuerier,
    QuerierError, RetainingChunkSeriesSet,
};
use mimir_ingester::query::{
    compute_label_values_series_count, count_label_value_series,
    label_names_and_values_with_index, ComputeWorkerPool, LabelIndexReader,
    LabelNamesAndValuesError, ListPostings, PostingsCursor, PostingsForMatchersFn, Series,
    SeriesIndexReader, LABEL_VALUES_CARDINALITY_TARGET_SIZE_BYTES,
};
use mimir_proto::ingesterpb;
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::KvClient;
use mimir_ring::ring::ring_desc_codec;
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use prometheus::Registry;
use tempfile::TempDir;
use tonic::metadata::MetadataMap;
use tower::ServiceExt;
use ulid::Ulid;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_value, mock_write_request,
    prepare_ingester_with_blocks_storage, prepare_ingester_with_kv_and_overrides,
    start_and_wait_healthy, TEST_USER_ID,
};

// ============================================================================
// pkg/ingester/ingester_activity_test.go (2 tests)
// ============================================================================

// Upstream: pkg/ingester/ingester_activity_test.go::TestRequestActivity
#[test]
fn test_request_activity() {
    let default_meta = default_metrics_metadata_request();
    let card_req = ingesterpb::LabelValuesCardinalityRequest {
        label_names: vec!["hello".into(), "world".into()],
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "test".into(),
            value: "value".into(),
        }],
        count_method: ingesterpb::CountMethod::InMemory as i32,
    };

    assert_eq!(
        request_activity("", "", "test", ActivityRequest::None),
        "test: user=\"\" trace=\"\" request=<nil>"
    );
    assert_eq!(
        request_activity("", "", "test", ActivityRequest::MetricsMetadata(&default_meta)),
        "test: user=\"\" trace=\"\" request=&MetricsMetadataRequest{Limit:-1,LimitPerMetric:-1,Metric:,MetricNames:[],}"
    );
    assert_eq!(
        request_activity(
            "",
            "",
            "test",
            ActivityRequest::LabelValuesCardinality(&card_req)
        ),
        "test: user=\"\" trace=\"\" request=&LabelValuesCardinalityRequest{LabelNames:[hello world],Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:test,Value:value,},},CountMethod:IN_MEMORY,}"
    );
}

// Upstream: pkg/ingester/ingester_activity_test.go::TestQueryRequest_CustomStringer
#[test]
fn test_query_request_custom_stringer() {
    let mut s = String::new();
    query_request_to_string(&mut s, None);
    assert_eq!(s, "nil");

    let empty = ingesterpb::QueryRequest::default();
    s.clear();
    query_request_to_string(&mut s, Some(&empty));
    assert_eq!(
        s,
        "&QueryRequest{StartTimestampMs:0,EndTimestampMs:0,Matchers:[]*LabelMatcher{},StreamingChunksBatchSize:0,}"
    );

    let no_matchers = ingesterpb::QueryRequest {
        start_timestamp_ms: 123,
        end_timestamp_ms: 456,
        ..Default::default()
    };
    s.clear();
    query_request_to_string(&mut s, Some(&no_matchers));
    assert_eq!(
        s,
        "&QueryRequest{StartTimestampMs:123,EndTimestampMs:456,Matchers:[]*LabelMatcher{},StreamingChunksBatchSize:0,}"
    );

    let one_matcher = ingesterpb::QueryRequest {
        start_timestamp_ms: 10,
        end_timestamp_ms: 20,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "n_1".into(),
            value: "v_1".into(),
        }],
        ..Default::default()
    };
    s.clear();
    query_request_to_string(&mut s, Some(&one_matcher));
    assert_eq!(
        s,
        "&QueryRequest{StartTimestampMs:10,EndTimestampMs:20,Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:n_1,Value:v_1,},},StreamingChunksBatchSize:0,}"
    );

    let multi_matchers = ingesterpb::QueryRequest {
        start_timestamp_ms: 10,
        end_timestamp_ms: 20,
        matchers: vec![
            ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "n_1".into(),
                value: "v_1".into(),
            },
            ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "n_2".into(),
                value: "v_2".into(),
            },
            ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "n_3".into(),
                value: "v_3".into(),
            },
        ],
        ..Default::default()
    };
    s.clear();
    query_request_to_string(&mut s, Some(&multi_matchers));
    assert_eq!(
        s,
        "&QueryRequest{StartTimestampMs:10,EndTimestampMs:20,Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:n_1,Value:v_1,},&LabelMatcher{Type:EQUAL,Name:n_2,Value:v_2,},&LabelMatcher{Type:EQUAL,Name:n_3,Value:v_3,},},StreamingChunksBatchSize:0,}"
    );
}

// ============================================================================
// pkg/ingester/label_names_and_values_test.go (10 tests)
// ============================================================================

struct MockLabelIndex {
    existing: HashMap<String, Vec<String>>,
    op_delay: Duration,
}

impl LabelIndexReader for MockLabelIndex {
    fn label_names(
        &self,
        _matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, LabelNamesAndValuesError> {
        if !self.op_delay.is_zero() {
            std::thread::sleep(self.op_delay);
        }
        let mut names: Vec<String> = self.existing.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    fn label_values(
        &self,
        name: &str,
        _matchers: &[LabelMatcher],
    ) -> Result<Vec<String>, LabelNamesAndValuesError> {
        if !self.op_delay.is_zero() {
            std::thread::sleep(self.op_delay);
        }
        Ok(self.existing.get(name).cloned().unwrap_or_default())
    }
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestLabelNamesAndValuesAreSentInBatches
#[test]
fn test_label_names_and_values_are_sent_in_batches() {
    let mut existing = HashMap::new();
    existing.insert(
        "label-aa".into(),
        vec!["a0000000".into(), "a1111111".into(), "a2222222".into()],
    );
    existing.insert(
        "label-bb".into(),
        vec![
            "b0000000".into(),
            "b1111111".into(),
            "b2222222".into(),
            "b3333333".into(),
        ],
    );
    existing.insert("label-c".into(), vec!["c0000000".into()]);
    existing.insert("label-dd".into(), vec!["d0000000".into()]);
    let long_ee = "label-ee".repeat(10);
    existing.insert(long_ee.clone(), vec!["e0000000".into()]);
    existing.insert(
        "label-ff".into(),
        vec!["f0000000".into(), "f1111111".into(), "f2222222".into()],
    );
    existing.insert("label-gg".into(), vec!["g0000000".into()]);

    let idx = MockLabelIndex {
        existing,
        op_delay: Duration::ZERO,
    };
    let batches =
        label_names_and_values_with_index(&idx, &[], 32, || false, |_, _| Ok(true)).unwrap();
    assert_eq!(batches.len(), 7);
    assert_eq!(
        batches[0].items,
        vec![ingesterpb::LabelValues {
            label_name: "label-aa".into(),
            values: vec!["a0000000".into(), "a1111111".into(), "a2222222".into()],
        }]
    );
    assert_eq!(
        batches[1].items,
        vec![ingesterpb::LabelValues {
            label_name: "label-bb".into(),
            values: vec!["b0000000".into(), "b1111111".into(), "b2222222".into()],
        }]
    );
    assert_eq!(
        batches[2].items,
        vec![
            ingesterpb::LabelValues {
                label_name: "label-bb".into(),
                values: vec!["b3333333".into()],
            },
            ingesterpb::LabelValues {
                label_name: "label-c".into(),
                values: vec!["c0000000".into()],
            },
        ]
    );
    assert_eq!(
        batches[3].items,
        vec![ingesterpb::LabelValues {
            label_name: "label-dd".into(),
            values: vec!["d0000000".into()],
        }]
    );
    assert_eq!(
        batches[4].items,
        vec![ingesterpb::LabelValues {
            label_name: long_ee,
            values: vec!["e0000000".into()],
        }]
    );
    assert_eq!(
        batches[5].items,
        vec![ingesterpb::LabelValues {
            label_name: "label-ff".into(),
            values: vec!["f0000000".into(), "f1111111".into(), "f2222222".into()],
        }]
    );
    assert_eq!(
        batches[6].items,
        vec![ingesterpb::LabelValues {
            label_name: "label-gg".into(),
            values: vec!["g0000000".into()],
        }]
    );
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestLabelNamesAndValues_FilteredValues
#[test]
fn test_label_names_and_values_filtered_values() {
    let mut existing = HashMap::new();
    existing.insert(
        "label-aa".into(),
        vec!["a0000000".into(), "a1111111".into(), "a2222222".into()],
    );
    existing.insert(
        "label-bb".into(),
        vec![
            "b0000000".into(),
            "b1111111".into(),
            "b2222222".into(),
            "b3333333".into(),
        ],
    );
    existing.insert("label-cc".into(), vec!["c0000000".into()]);

    let idx = MockLabelIndex {
        existing,
        op_delay: Duration::ZERO,
    };
    let batches = label_names_and_values_with_index(
        &idx,
        &[],
        32,
        || false,
        |_, val| Ok(val.contains('0')),
    )
    .unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(
        batches[0].items,
        vec![
            ingesterpb::LabelValues {
                label_name: "label-aa".into(),
                values: vec!["a0000000".into()],
            },
            ingesterpb::LabelValues {
                label_name: "label-bb".into(),
                values: vec!["b0000000".into()],
            },
        ]
    );
    assert_eq!(
        batches[1].items,
        vec![ingesterpb::LabelValues {
            label_name: "label-cc".into(),
            values: vec!["c0000000".into()],
        }]
    );
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestIngester_LabelValuesCardinality_SentInBatches
#[tokio::test]
async fn test_ingester_label_values_cardinality_sent_in_batches() {
    let label_value_size = 1024usize;
    let max_batch_label_values = LABEL_VALUES_CARDINALITY_TARGET_SIZE_BYTES / label_value_size;

    let dir = TempDir::new().unwrap();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    let mut limits = default_limits_test_config();
    limits.max_global_series_per_user = 0;
    limits.max_global_series_per_metric = 0;
    let (ing, _, _) =
        prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, None);
    start_and_wait_healthy(&ing).await;

    let mut idx = 0usize;
    let mut next_val = || {
        idx += 1;
        format!("{idx:0label_value_size$}")
    };

    let mut ts = Vec::new();
    for _ in 0..max_batch_label_values {
        let lbls = Labels::from_pairs([("__name__", "metric"), ("label-a", &next_val())]);
        ts.extend(mock_write_request(&lbls, 1.0, 1000).timeseries);
    }
    let lbls_a_extra = Labels::from_pairs([("__name__", "metric"), ("label-a", &next_val())]);
    ts.extend(mock_write_request(&lbls_a_extra, 1.0, 1000).timeseries);
    for _ in 0..(max_batch_label_values - 1) {
        let lbls = Labels::from_pairs([("__name__", "metric"), ("label-b", &next_val())]);
        ts.extend(mock_write_request(&lbls, 1.0, 1000).timeseries);
    }
    let lbls_b_extra = Labels::from_pairs([("__name__", "metric"), ("label-b", &next_val())]);
    ts.extend(mock_write_request(&lbls_b_extra, 1.0, 1000).timeseries);

    let write_req = mimir_proto::cortexpb::WriteRequest {
        timeseries: ts,
        source: mimir_proto::cortexpb::write_request::SourceEnum::Api as i32,
        ..Default::default()
    };
    ing.push_grpc(TEST_USER_ID, &write_req).await.unwrap();

    let resp = ing
        .handle_label_values_cardinality(
            &MetadataMap::new(),
            TEST_USER_ID,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["label-a".into(), "label-b".into()],
                matchers: Vec::new(),
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await
        .unwrap();

    assert_eq!(resp.len(), 3);
    assert_eq!(resp[0].items.len(), 1);
    assert_eq!(resp[0].items[0].label_value_series.len(), max_batch_label_values);
    assert_eq!(resp[1].items.len(), 2);
    assert_eq!(resp[1].items[0].label_value_series.len(), 1);
    assert_eq!(resp[1].items[1].label_value_series.len(), max_batch_label_values - 1);
    assert_eq!(resp[2].items.len(), 1);
    assert_eq!(resp[2].items[0].label_value_series.len(), 1);

    let _ = ing.stop().await;
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestIngester_LabelValuesCardinality_AllValuesToBeReturnedInSingleMessage
#[tokio::test]
async fn test_ingester_label_values_cardinality_all_values_to_be_returned_in_single_message() {
    type CardCase<'a> = (
        &'a str,
        Vec<&'a str>,
        Vec<ingesterpb::LabelMatcher>,
        Vec<(&'a str, Vec<&'a str>)>,
        Vec<(&'a str, Vec<(&'a str, u64)>)>,
    );
    let cases: Vec<CardCase<'_>> = vec![
        ("empty", vec!["label-a", "label-b"], vec![], vec![], vec![]),
        (
            "single value",
            vec!["label-a", "label-b"],
            vec![],
            vec![("label-a", vec!["a-0"])],
            vec![("label-a", vec![("a-0", 1)])],
        ),
        (
            "multiple labels",
            vec!["label-a", "label-b"],
            vec![],
            vec![
                ("label-a", vec!["a-0", "a-1", "a-2"]),
                ("label-b", vec!["b-0", "b-1"]),
            ],
            vec![
                ("label-a", vec![("a-0", 1), ("a-1", 2), ("a-2", 3)]),
                ("label-b", vec![("b-0", 1), ("b-1", 2)]),
            ],
        ),
        (
            "with matcher",
            vec!["label-a", "label-b"],
            vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::RegexMatch as i32,
                name: "label-a".into(),
                value: "a-(1|2)".into(),
            }],
            vec![
                ("label-a", vec!["a-0", "a-1", "a-2"]),
                ("label-b", vec!["b-0", "b-1"]),
            ],
            vec![("label-a", vec![("a-1", 2), ("a-2", 3)])],
        ),
    ];

    for (name, req_labels, matchers, existing, expected) in cases {
        let dir = TempDir::new().unwrap();
        let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        let mut limits = default_limits_test_config();
        limits.max_global_series_per_user = 0;
        limits.max_global_series_per_metric = 0;
        let (ing, _, _) =
            prepare_ingester_with_blocks_storage(dir.path(), cfg, blocks_cfg, ingest_cfg, limits, None);
        start_and_wait_healthy(&ing).await;

        for (lbl_name, vals) in existing {
            for (idx, val) in vals.into_iter().enumerate() {
                for c in 0..=idx {
                    let lbls = Labels::from_pairs([
                        ("__name__", "foo"),
                        (lbl_name, val),
                        ("counter", &c.to_string()),
                    ]);
                    ing.push_grpc(TEST_USER_ID, &mock_write_request(&lbls, 1.0, 1000))
                        .await
                        .unwrap();
                }
            }
        }

        let resp = ing
            .handle_label_values_cardinality(
                &MetadataMap::new(),
                TEST_USER_ID,
                ingesterpb::LabelValuesCardinalityRequest {
                    label_names: req_labels.into_iter().map(String::from).collect(),
                    matchers,
                    count_method: ingesterpb::CountMethod::InMemory as i32,
                },
            )
            .await
            .unwrap();

        if expected.is_empty() {
            assert!(resp.is_empty(), "{name}");
        } else {
            assert_eq!(resp.len(), 1, "{name}");
            let exp_items: Vec<ingesterpb::LabelValueSeriesCount> = expected
                .into_iter()
                .map(|(ln, kvs)| ingesterpb::LabelValueSeriesCount {
                    label_name: ln.to_string(),
                    label_value_series: kvs
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                })
                .collect();
            assert_eq!(resp[0].items, exp_items, "{name}");
        }
        let _ = ing.stop().await;
    }
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestLabelNamesAndValues_ContextCancellation
#[tokio::test]
async fn test_label_names_and_values_context_cancellation() {
    let mut existing = HashMap::new();
    let vals: Vec<String> = (0..100).map(|j| format!("val-{j}")).collect();
    existing.insert("__name__".to_string(), vals);

    let idx = Arc::new(MockLabelIndex {
        existing,
        op_delay: Duration::from_millis(50),
    });
    let canceled = Arc::new(AtomicBool::new(false));
    let c_flag = Arc::clone(&canceled);

    let handle = tokio::task::spawn_blocking(move || {
        label_names_and_values_with_index(
            idx.as_ref(),
            &[],
            1024 * 1024,
            move || c_flag.load(Ordering::Acquire),
            |_, _| Ok(true),
        )
    });

    canceled.store(true, Ordering::Release);
    let res = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("completed promptly")
        .unwrap();
    assert_eq!(res, Err(LabelNamesAndValuesError::Canceled));
}

struct InfinitePostings;
impl PostingsCursor for InfinitePostings {
    fn next(&mut self) -> bool {
        true
    }
    fn at(&self) -> u64 {
        0
    }
    fn err(&self) -> Option<LabelNamesAndValuesError> {
        None
    }
}

struct EmptyPostings;
impl PostingsCursor for EmptyPostings {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> u64 {
        0
    }
    fn err(&self) -> Option<LabelNamesAndValuesError> {
        None
    }
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestCountLabelValueSeries_ContextCancellation
#[test]
fn test_count_label_value_series_context_cancellation() {
    let fn_postings: PostingsForMatchersFn = Arc::new(|_| Ok(Box::new(InfinitePostings)));
    let res = count_label_value_series(&|| true, "lblName", "lblVal", &[], &fn_postings);
    assert_eq!(res, Err(LabelNamesAndValuesError::Canceled));
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestComputeLabelValuesSeriesCount_AbortsOnCancelledContext
#[tokio::test]
async fn test_compute_label_values_series_count_aborts_on_cancelled_context() {
    let pool = ComputeWorkerPool::new(1);
    let lbl_values: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    let fn_postings: PostingsForMatchersFn = Arc::new(|_| Ok(Box::new(InfinitePostings)));

    let mut rx = compute_label_values_series_count(
        Arc::new(|| true),
        Arc::clone(&pool),
        1,
        "lblName".into(),
        lbl_values,
        Vec::new(),
        fn_postings,
    );

    let mut got = Vec::new();
    while let Some(r) = rx.recv().await {
        got.push(r);
    }
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].err, Some(LabelNamesAndValuesError::Canceled));
    pool.stop().await;
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestComputeLabelValuesSeriesCount_PoolStopped
#[tokio::test]
async fn test_compute_label_values_series_count_pool_stopped() {
    let pool = ComputeWorkerPool::new(1);
    pool.stop().await;
    let fn_postings: PostingsForMatchersFn = Arc::new(|_| Ok(Box::new(InfinitePostings)));

    let mut rx = compute_label_values_series_count(
        Arc::new(|| false),
        pool,
        2,
        "lblName".into(),
        vec!["a".into(), "b".into(), "c".into()],
        Vec::new(),
        fn_postings,
    );

    let mut got = Vec::new();
    while let Some(r) = rx.recv().await {
        got.push(r);
    }
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].err, Some(LabelNamesAndValuesError::PoolStopped));
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestComputeLabelValuesSeriesCount_MidStreamCancellationFreesPoolWorkers
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_compute_label_values_series_count_mid_stream_cancellation_frees_pool_workers() {
    let pool = ComputeWorkerPool::new(1);
    let canceled = Arc::new(AtomicBool::new(false));
    let first_started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(AtomicBool::new(false));

    let started_c = Arc::clone(&first_started);
    let release_c = Arc::clone(&release);
    let cancel_c = Arc::clone(&canceled);
    let started_once = Arc::new(AtomicBool::new(false));

    let fn_postings: PostingsForMatchersFn = Arc::new(move |_| {
        if !started_once.swap(true, Ordering::AcqRel) {
            started_c.notify_one();
        }
        while !release_c.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(2));
        }
        if cancel_c.load(Ordering::Acquire) {
            Err(LabelNamesAndValuesError::Canceled)
        } else {
            Ok(Box::new(EmptyPostings))
        }
    });

    let lbl_values: Vec<String> = (0..100).map(|i| i.to_string()).collect();
    let cancel_flag = Arc::clone(&canceled);
    let mut rx = compute_label_values_series_count(
        Arc::new(move || cancel_flag.load(Ordering::Acquire)),
        Arc::clone(&pool),
        1,
        "lblName".into(),
        lbl_values,
        Vec::new(),
        fn_postings,
    );

    first_started.notified().await;
    canceled.store(true, Ordering::Release);
    release.store(true, Ordering::Release);

    let first_res = rx.recv().await.unwrap();
    assert!(first_res.err.is_some());
    drop(rx);

    // Worker must still pick up another tenant's job
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    pool.submit(move || {
        let _ = done_tx.send(());
    })
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), done_rx)
        .await
        .expect("pool worker freed")
        .unwrap();
    pool.stop().await;
}

// Upstream: pkg/ingester/label_names_and_values_test.go::TestComputeLabelValuesSeriesCount_ChunkBoundaries
#[tokio::test]
async fn test_compute_label_values_series_count_chunk_boundaries() {
    let pool = ComputeWorkerPool::new(2);
    let chunk_size = 4usize;

    for num_values in [
        1,
        chunk_size - 1,
        chunk_size,
        chunk_size + 1,
        2 * chunk_size - 1,
        2 * chunk_size,
    ] {
        let lbl_values: Vec<String> = (0..num_values).map(|i| i.to_string()).collect();
        let fn_postings: PostingsForMatchersFn = Arc::new(|_| Ok(Box::new(EmptyPostings)));
        let mut rx = compute_label_values_series_count(
            Arc::new(|| false),
            Arc::clone(&pool),
            chunk_size,
            "lblName".into(),
            lbl_values.clone(),
            Vec::new(),
            fn_postings,
        );
        let mut seen: HashMap<String, usize> = HashMap::new();
        while let Some(r) = rx.recv().await {
            assert!(r.err.is_none());
            *seen.entry(r.val).or_insert(0) += 1;
        }
        assert_eq!(seen.len(), num_values);
        for v in &lbl_values {
            assert_eq!(seen.get(v).copied(), Some(1));
        }
    }
    pool.stop().await;
}

// ============================================================================
// pkg/ingester/list_series_test.go (1 test)
// ============================================================================

struct MockSeriesIndex {
    series_in_index: Vec<Labels>,
    return_err: bool,
    calls: usize,
}

impl SeriesIndexReader for MockSeriesIndex {
    fn series(&mut self, ref_id: u64) -> Result<Labels, String> {
        self.calls += 1;
        if self.return_err {
            return Err("error in Series()".to_string());
        }
        Ok(self.series_in_index[ref_id as usize].clone())
    }
}

// Upstream: pkg/ingester/list_series_test.go::TestSeries
#[test]
fn test_series() {
    let series_in_index = vec![
        Labels::from_pairs([("a", "0")]),
        Labels::from_pairs([("a", "1")]),
        Labels::from_pairs([("a", "2")]),
        Labels::from_pairs([("a", "3")]),
        Labels::from_pairs([("a", "4")]),
    ];

    // 1) fetches expected series
    {
        let idx = MockSeriesIndex {
            series_in_index: series_in_index.clone(),
            return_err: false,
            calls: 0,
        };
        let mut s = Series::new(ListPostings::new(vec![1, 2, 4]), idx);
        let mut out = Vec::new();
        while s.next() {
            out.push(s.at());
        }
        assert!(s.err().is_none());
        assert_eq!(
            out,
            vec![
                Labels::from_pairs([("a", "1")]),
                Labels::from_pairs([("a", "2")]),
                Labels::from_pairs([("a", "4")]),
            ]
        );
    }

    // 2) returns error if index fails
    {
        let idx = MockSeriesIndex {
            series_in_index,
            return_err: true,
            calls: 0,
        };
        let mut s = Series::new(ListPostings::new(vec![0, 3]), idx);
        while s.next() {
            let _ = s.at();
        }
        assert!(s.err().is_some());
    }
}

// ============================================================================
// pkg/ingester/mirror_querier_test.go (3 tests)
// ============================================================================

#[derive(Clone)]
struct MockChunkSeriesItem {
    labels: Labels,
    chunks: Vec<ChunkMeta>,
}

impl ChunkSeries for MockChunkSeriesItem {
    fn labels(&self) -> Labels {
        self.labels.clone()
    }

    fn chunks(&self) -> Vec<ChunkMeta> {
        self.chunks.clone()
    }
}

struct MockChunkSeriesSetImpl {
    series: Vec<Arc<dyn ChunkSeries>>,
    pos: usize,
    err: Option<QuerierError>,
}

impl ChunkSeriesSet for MockChunkSeriesSetImpl {
    fn next(&mut self) -> bool {
        if self.err.is_some() || self.pos >= self.series.len() {
            return false;
        }
        self.pos += 1;
        true
    }

    fn at(&mut self) -> Option<Arc<dyn ChunkSeries>> {
        if self.pos == 0 || self.pos > self.series.len() {
            return None;
        }
        Some(Arc::clone(&self.series[self.pos - 1]))
    }

    fn err(&self) -> Option<QuerierError> {
        self.err.clone()
    }
}

struct MockChunkQuerierImpl {
    series: Vec<Arc<dyn ChunkSeries>>,
    shared_err: Arc<parking_lot::Mutex<Option<QuerierError>>>,
}

impl ChunkQuerier for MockChunkQuerierImpl {
    fn select(
        &mut self,
        _sort_series: bool,
        _hints: Option<&mimir_ingester::lookupplan::SelectHints>,
        _matchers: &[LabelMatcher],
    ) -> Box<dyn ChunkSeriesSet> {
        let err = self.shared_err.lock().clone();
        Box::new(MockChunkSeriesSetImpl {
            series: self.series.clone(),
            pos: 0,
            err,
        })
    }

    fn label_values(
        &mut self,
        _name: &str,
        _matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        Err(QuerierError::Other("not implemented".into()))
    }

    fn label_names(
        &mut self,
        _matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        Err(QuerierError::Other("not implemented".into()))
    }

    fn close(&mut self) -> Result<(), QuerierError> {
        Ok(())
    }
}

// Upstream: pkg/ingester/mirror_querier_test.go::TestRetainingChunkSeriesSet
#[test]
fn test_retaining_chunk_series_set() {
    let s1: Arc<dyn ChunkSeries> = Arc::new(MockChunkSeriesItem {
        labels: Labels::from_pairs([("__name__", "metric1")]),
        chunks: vec![ChunkMeta {
            ref_id: 1,
            min_time: 1000,
            max_time: 2000,
            chunk: None,
        }],
    });
    let s2: Arc<dyn ChunkSeries> = Arc::new(MockChunkSeriesItem {
        labels: Labels::from_pairs([("__name__", "metric2")]),
        chunks: vec![ChunkMeta {
            ref_id: 2,
            min_time: 2000,
            max_time: 3000,
            chunk: None,
        }],
    });

    let cases = [
        ("two series", vec![Arc::clone(&s1), Arc::clone(&s2)], None, vec![s1.labels(), s2.labels()], false),
        ("empty", vec![], None, vec![], false),
        (
            "error propagation",
            vec![],
            Some(QuerierError::Other("test error".into())),
            vec![],
            true,
        ),
        ("single series", vec![Arc::clone(&s1)], None, vec![s1.labels()], false),
    ];

    for (name, series, err, exp_labels, exp_err) in cases {
        let delegate = Box::new(MockChunkSeriesSetImpl {
            series,
            pos: 0,
            err,
        });
        let mut r = RetainingChunkSeriesSet::new(delegate);
        let mut collected = 0usize;
        while r.next() {
            let first = r.at().unwrap();
            let second = r.at().unwrap();
            assert_eq!(first.labels(), second.labels());
            collected += 1;
        }
        assert_eq!(r.err().is_some(), exp_err, "{name}");
        assert_eq!(collected, exp_labels.len(), "{name}");
        assert_eq!(r.labels, exp_labels, "{name}");
    }
}

// Upstream: pkg/ingester/mirror_querier_test.go::TestMirroredChunkQuerier_CompareResults
#[test]
fn test_mirrored_chunk_querier_compare_results() {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let s1: Arc<dyn ChunkSeries> = Arc::new(MockChunkSeriesItem {
        labels: Labels::from_pairs([("__name__", "metric1")]),
        chunks: vec![ChunkMeta {
            ref_id: 1,
            min_time: now_ms - 3_600_000,
            max_time: now_ms - 60_000,
            chunk: None,
        }],
    });
    let s2: Arc<dyn ChunkSeries> = Arc::new(MockChunkSeriesItem {
        labels: Labels::from_pairs([("__name__", "metric2")]),
        chunks: vec![ChunkMeta {
            ref_id: 2,
            min_time: now_ms - 7_200_000,
            max_time: now_ms,
            chunk: None,
        }],
    });
    let s_future: Arc<dyn ChunkSeries> = Arc::new(MockChunkSeriesItem {
        labels: Labels::from_pairs([("__name__", "metric3")]),
        chunks: vec![ChunkMeta {
            ref_id: 3,
            min_time: now_ms + 5_000,
            max_time: now_ms + 10_000,
            chunk: None,
        }],
    });

    let cases = [
        (
            "matching",
            vec![s1.labels(), s2.labels()],
            vec![Arc::clone(&s1), Arc::clone(&s2)],
            None,
            "success",
        ),
        (
            "extra_series",
            vec![s1.labels(), s2.labels()],
            vec![Arc::clone(&s1)],
            None,
            "extra_series",
        ),
        (
            "missing_series",
            vec![s1.labels()],
            vec![Arc::clone(&s1), Arc::clone(&s2)],
            None,
            "missing_series",
        ),
        (
            "extra_and_missing",
            vec![s1.labels()],
            vec![Arc::clone(&s2)],
            None,
            "extra_and_missing_series",
        ),
        ("empty", vec![], vec![], None, "success"),
        (
            "secondary_error",
            vec![],
            vec![],
            Some(QuerierError::Other("secondary error".into())),
            "secondary_error",
        ),
        (
            "chunk trimming beyond finished_at ignored",
            vec![s1.labels()],
            vec![Arc::clone(&s1), Arc::clone(&s_future)],
            None,
            "success",
        ),
    ];

    for (name, primary_labels, sec_series, sec_err, exp_outcome) in cases {
        let reg = Registry::new();
        let outcomes = prometheus::CounterVec::new(
            prometheus::Opts::new("test_comparison_outcomes", "help"),
            &["outcome"],
        )
        .unwrap();
        reg.register(Box::new(outcomes.clone())).unwrap();

        let delegate = Box::new(MockChunkQuerierImpl {
            series: Vec::new(),
            shared_err: Arc::new(parking_lot::Mutex::new(None)),
        });
        let mut mq = MirroredChunkQuerier::new_with_meta(
            "test-user",
            outcomes.clone(),
            0,
            0,
            ChunkBlockMeta::default(),
            delegate,
        );
        mq.recorded_request.finished_at_ms = now_ms;
        mq.returned_series = Some(RetainingChunkSeriesSet::from_retained_labels(
            primary_labels,
            Box::new(MockChunkSeriesSetImpl {
                series: Vec::new(),
                pos: 0,
                err: None,
            }),
        ));
        let mut secondary = MockChunkSeriesSetImpl {
            series: sec_series,
            pos: 0,
            err: sec_err,
        };
        mq.compare_results(&mut secondary);
        assert_eq!(
            outcomes.with_label_values(&[exp_outcome]).get(),
            1.0,
            "{name}"
        );
    }
}

// Upstream: pkg/ingester/mirror_querier_test.go::TestMirroredChunkQuerier
#[test]
fn test_mirrored_chunk_querier() {
    let s1: Arc<dyn ChunkSeries> = Arc::new(MockChunkSeriesItem {
        labels: Labels::from_pairs([("__name__", "metric1")]),
        chunks: vec![ChunkMeta {
            ref_id: 1,
            min_time: 1000,
            max_time: 2000,
            chunk: None,
        }],
    });
    let m = LabelMatcher::new(MatchType::Equal, "__name__", "metric1").unwrap();

    // 1) context_cancelled_before_select
    {
        let outcomes = prometheus::CounterVec::new(
            prometheus::Opts::new("test_outcomes_1", "help"),
            &["outcome", "user"],
        )
        .unwrap();
        let delegate = Box::new(MockChunkQuerierImpl {
            series: vec![Arc::clone(&s1)],
            shared_err: Arc::new(parking_lot::Mutex::new(Some(QuerierError::Canceled))),
        });
        let mut mq = MirroredChunkQuerier::new_with_meta(
            "test-user",
            outcomes.clone(),
            1000,
            2000,
            ChunkBlockMeta {
                ulid: Ulid::default(),
                min_time: 0,
                max_time: 0,
            },
            delegate,
        );
        let mut ss = mq.select(true, None, std::slice::from_ref(&m));
        while ss.next() {
            let _ = ss.at();
        }
        assert_eq!(ss.err(), Some(QuerierError::Canceled));
        drop(ss);
        mq.close().unwrap();
        assert_eq!(
            outcomes
                .with_label_values(&["context_cancelled", "test-user"])
                .get(),
            1.0
        );
    }

    // 2) context_cancelled_before_close
    {
        let outcomes = prometheus::CounterVec::new(
            prometheus::Opts::new("test_outcomes_2", "help"),
            &["outcome", "user"],
        )
        .unwrap();
        let shared_err = Arc::new(parking_lot::Mutex::new(None));
        let delegate = Box::new(MockChunkQuerierImpl {
            series: vec![Arc::clone(&s1)],
            shared_err: Arc::clone(&shared_err),
        });
        let mut mq = MirroredChunkQuerier::new_with_meta(
            "test-user",
            outcomes.clone(),
            1000,
            2000,
            ChunkBlockMeta::default(),
            delegate,
        );
        let mut ss = mq.select(true, None, std::slice::from_ref(&m));
        while ss.next() {
            let _ = ss.at();
        }
        assert!(ss.err().is_none());
        *shared_err.lock() = Some(QuerierError::Canceled);
        drop(ss);
        mq.close().unwrap();
        assert_eq!(
            outcomes
                .with_label_values(&["context_cancelled", "test-user"])
                .get(),
            1.0
        );
    }

    // 3) select_not_called
    {
        let outcomes = prometheus::CounterVec::new(
            prometheus::Opts::new("test_outcomes_3", "help"),
            &["outcome", "user"],
        )
        .unwrap();
        let delegate = Box::new(MockChunkQuerierImpl {
            series: vec![s1],
            shared_err: Arc::new(parking_lot::Mutex::new(None)),
        });
        let mut mq = MirroredChunkQuerier::new_with_meta(
            "test-user",
            outcomes.clone(),
            1000,
            2000,
            ChunkBlockMeta::default(),
            delegate,
        );
        mq.close().unwrap();
        assert_eq!(
            outcomes
                .with_label_values(&["no_select", "test-user"])
                .get(),
            1.0
        );
    }
}

// ============================================================================
// pkg/ingester/downscale_test.go (1 test)
// ============================================================================

// Upstream: pkg/ingester/downscale_test.go::TestIngester_PrepareInstanceRingDownscaleHandler
#[tokio::test]
async fn test_ingester_prepare_instance_ring_downscale_handler() {
    let target = "/ingester/prepare-instance-ring-downscale";

    // 1) POST switches to read-only, second POST preserves timestamp; DELETE clears read-only
    {
        let dir = TempDir::new().unwrap();
        let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        let (ing, ring, _) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            None,
        );
        start_and_wait_healthy(&ing).await;
        let app = ingester_router(Arc::clone(&ing));

        assert!(!ing.lifecycler.as_ref().unwrap().get_read_only_state().0);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let json1: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let ts1 = json1["timestamp"].as_i64().unwrap();
        assert!(ts1 > 0);
        assert!(ing.lifecycler.as_ref().unwrap().get_read_only_state().0);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if let Ok(desc) = ring.get_instance("localhost") {
                if desc.read_only {
                    break;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for ring instance read_only=true"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Second POST keeps timestamp unchanged
        let res2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res2.status(), StatusCode::OK);
        let body2 = axum::body::to_bytes(res2.into_body(), usize::MAX)
            .await
            .unwrap();
        let json2: serde_json::Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2["timestamp"].as_i64().unwrap(), ts1);

        // DELETE clears read-only
        let res_del = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_del.status(), StatusCode::OK);
        let body_del = axum::body::to_bytes(res_del.into_body(), usize::MAX)
            .await
            .unwrap();
        let json_del: serde_json::Value = serde_json::from_slice(&body_del).unwrap();
        assert_eq!(json_del["timestamp"].as_i64().unwrap(), 0);
        assert!(!ing.lifecycler.as_ref().unwrap().get_read_only_state().0);

        // Conflict when read-only and compaction in progress
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        ing.num_compactions_in_progress.fetch_add(1, Ordering::AcqRel);
        let res_conflict = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_conflict.status(), StatusCode::CONFLICT);
        assert!(ing.lifecycler.as_ref().unwrap().get_read_only_state().0);
        ing.num_compactions_in_progress.fetch_sub(1, Ordering::AcqRel);

        // OK when NOT read-only and compaction in progress
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        ing.num_compactions_in_progress.fetch_add(1, Ordering::AcqRel);
        let res_ok = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res_ok.status(), StatusCode::OK);
        ing.num_compactions_in_progress.fetch_sub(1, Ordering::AcqRel);

        let _ = ing.stop().await;
    }

    // 2) ServiceUnavailable when not running
    {
        let dir = TempDir::new().unwrap();
        let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        let (ing, _, _) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            None,
        );
        let app = ingester_router(ing);
        let res = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // 3) MethodNotAllowed when ingest storage is enabled
    {
        let dir = TempDir::new().unwrap();
        let (mut cfg, blocks_cfg, mut ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.ring.instance_id = "ingester-0".to_string();
        ingest_cfg.enabled = true;
        let (ing, _, _) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            None,
        );
        ing.set_state_for_test(State::Running);
        let app = ingester_router(ing);
        let res = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(target)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}

// ============================================================================
// pkg/ingester/lifecycle_test.go (2 tests)
// ============================================================================

async fn num_tokens_in_kv(kv: &Arc<dyn KvClient>, id: &str, ring_key: &str) -> usize {
    let Ok(Some(val)) = kv.get(ring_key).await else {
        return 0;
    };
    let Some(desc) = val.downcast_ref::<mimir_proto::ringpb::Desc>() else {
        return 0;
    };
    desc.ingesters
        .get(id)
        .map_or(0, |inst| inst.tokens.len())
}

// Upstream: pkg/ingester/lifecycle_test.go::TestIngesterRestart
#[tokio::test]
async fn test_ingester_restart() {
    let dir = TempDir::new().unwrap();
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    for _ in 0..2 {
        let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.ring.unregister_on_shutdown = false;
        let overrides = mimir_config::Overrides::new(default_limits_test_config());
        let (ing, _) = prepare_ingester_with_kv_and_overrides(
            cfg,
            blocks_cfg,
            ingest_cfg,
            overrides,
            Arc::clone(&kv),
            None,
        );
        start_and_wait_healthy(&ing).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        ing.stop().await.unwrap();
        assert_eq!(num_tokens_in_kv(&kv, "localhost", "collectors/ring").await, 1);
    }
}

// Upstream: pkg/ingester/lifecycle_test.go::TestIngester_ShutdownHandler
#[tokio::test]
async fn test_ingester_shutdown_handler() {
    for unregister in [false, true] {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.ring.unregister_on_shutdown = unregister;
        let overrides = mimir_config::Overrides::new(default_limits_test_config());
        let (ing, _) = prepare_ingester_with_kv_and_overrides(
            cfg,
            blocks_cfg,
            ingest_cfg,
            overrides,
            Arc::clone(&kv),
            None,
        );
        start_and_wait_healthy(&ing).await;

        let app = ingester_router(Arc::clone(&ing));
        let res = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/ingester/shutdown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(num_tokens_in_kv(&kv, "localhost", "collectors/ring").await, 0);
    }
}

// ============================================================================
// pkg/ingester/tenants_http_test.go (1 test)
// ============================================================================

// Upstream: pkg/ingester/tenants_http_test.go::TestIngester_TenantsHandlers
#[tokio::test]
async fn test_ingester_tenants_handlers() {
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

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    ing.push_grpc(
        TEST_USER_ID,
        &mock_write_request(&Labels::from_pairs([("__name__", "up")]), 1.0, now_ms),
    )
    .await
    .unwrap();

    let tenant_more = "tenant-with-more-series";
    for metric in ["first", "second"] {
        ing.push_grpc(
            tenant_more,
            &mock_write_request(&Labels::from_pairs([("__name__", metric)]), 0.0, now_ms),
        )
        .await
        .unwrap();
    }

    let app = ingester_router(Arc::clone(&ing));

    // 1) tenants list
    {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ingester/tenants")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = String::from_utf8(
            axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains(&format!("<a href=\"tsdb/{TEST_USER_ID}\">{TEST_USER_ID}</a>")));
        assert!(body.contains("<td>(-0s, +10m0s)</td>"));
        let pos_more = body.find(&format!("href=\"tsdb/{tenant_more}\"")).unwrap();
        let pos_one = body.find(&format!("href=\"tsdb/{TEST_USER_ID}\"")).unwrap();
        assert!(pos_more < pos_one);
    }

    // 2) tenant TSDB for valid tenant
    {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/ingester/tsdb/{TEST_USER_ID}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = String::from_utf8(
            axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains("<li>Number of series: 1</li>"));
        assert!(body.contains(&format!("<a href=\"../label_churn/{TEST_USER_ID}\">Label churn</a>")));
    }

    // 3) tenant TSDB for unknown tenant
    {
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/ingester/tsdb/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = String::from_utf8(
            axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains("TSDB not found for tenant unknown"));
    }

    let _ = ing.stop().await;
    let _unused: (GoDuration, AtomicUsize) = (GoDuration(0), AtomicUsize::new(0));
    let _ = (metric_value,);
}
