//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_search_test.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_streaming_memory_test.go

mod compaction_ring_helpers;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::Service;
use mimir_ingester::grpc::IngesterService;
use mimir_ingester::query::{
    build_search_hints, proto_to_params, stream_search_results, QueryStreamSink, SearchResultSet,
    VecSearchResultSet, QUERY_STREAM_BATCH_SIZE, SEARCH_BATCH_SIZE,
};
use mimir_ingester::IngesterError;
use mimir_proto::ingesterpb;
use mimir_proto::ingesterpb::ingester_server::Ingester as IngesterRpc;
use mimir_ring::kv::BoxFuture;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use mimir_tsdb::search::{FuzzAlg, SearchResult};
use prometheus::Registry;
use tokio::sync::Notify;
use tonic::metadata::MetadataMap;
use tonic::{Code, Request};

use compaction_ring_helpers::{
    default_ingester_test_config, prepare_ingester_with_blocks_storage, push_series_to_ingester,
    start_and_wait_healthy,
};

struct FakeSearchResultSet {
    results: Vec<SearchResult>,
    idx: usize,
    err: Option<String>,
    warnings: Vec<String>,
}

impl SearchResultSet for FakeSearchResultSet {
    fn next(&mut self) -> bool {
        if self.idx >= self.results.len() {
            return false;
        }
        self.idx += 1;
        true
    }

    fn at(&self) -> SearchResult {
        self.results[self.idx - 1].clone()
    }

    fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn err(&self) -> Option<String> {
        self.err.clone()
    }
}

fn read_rss_bytes() -> usize {
    if let Ok(contents) = std::fs::read_to_string("/proc/self/statm") {
        if let Some(rss_pages_str) = contents.split_whitespace().nth(1) {
            if let Ok(pages) = rss_pages_str.parse::<usize>() {
                return pages * 4096;
            }
        }
    }
    0
}

// Upstream: pkg/ingester/ingester_search_test.go::TestIngesterSearchLabelNames
#[tokio::test]
async fn test_ingester_search_label_names() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let (ing, ring, ring_svc) =
        prepare_ingester_with_blocks_storage(cfg, None, Some(&reg)).await;
    start_and_wait_healthy(&ing, &ring).await;

    let user = "test";
    push_series_to_ingester(
        &ing,
        user,
        &[
            (&[("__name__", "metric_a"), ("status", "200")], &[(100_000, 1.0)]),
            (&[("__name__", "metric_b"), ("env", "prod")], &[(110_000, 1.0)]),
        ],
    )
    .await
    .unwrap();

    let cases = [
        (
            "no filter returns all",
            Vec::<String>::new(),
            false,
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            0i64,
            vec!["__name__", "env", "status"],
        ),
        (
            "substring 'env' case-sensitive",
            vec!["env".to_string()],
            false,
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            0,
            vec!["env"],
        ),
        (
            "case-insensitive 'ENV' matches",
            vec!["ENV".to_string()],
            true,
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            0,
            vec!["env"],
        ),
        (
            "limit 1",
            vec![],
            false,
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            1,
            vec!["__name__"],
        ),
        (
            "value desc",
            vec![],
            false,
            ingesterpb::SearchOrdering::OrderByValueDesc as i32,
            0,
            vec!["status", "env", "__name__"],
        ),
    ];

    let md = MetadataMap::new();
    for (name, filter_terms, case_insens, ordering, limit, want) in cases {
        let req = ingesterpb::SearchLabelNamesRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 200_000,
            filter: Some(ingesterpb::SearchFilter {
                terms: filter_terms,
                case_insensitive: case_insens,
                ..Default::default()
            }),
            ordering,
            limit,
            ..Default::default()
        };
        let batches = ing.handle_search_label_names(&md, user, req).await.unwrap();
        let got: Vec<String> = batches
            .into_iter()
            .flat_map(|b| b.results.into_iter().map(|r| r.value))
            .collect();
        assert_eq!(got, want, "case {name}");
    }

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_search_test.go::TestIngesterSearchLabelValues
#[tokio::test]
async fn test_ingester_search_label_values() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = default_ingester_test_config(tmp.path());
    let reg = Registry::new();
    let (ing, ring, ring_svc) =
        prepare_ingester_with_blocks_storage(cfg, None, Some(&reg)).await;
    start_and_wait_healthy(&ing, &ring).await;

    let user = "test";
    push_series_to_ingester(
        &ing,
        user,
        &[
            (&[("__name__", "metric"), ("status", "200")], &[(100_000, 1.0)]),
            (&[("__name__", "metric"), ("status", "300")], &[(110_000, 1.0)]),
            (&[("__name__", "metric"), ("status", "500")], &[(120_000, 1.0)]),
        ],
    )
    .await
    .unwrap();

    let cases = [
        (
            "all status values",
            "status",
            Vec::<String>::new(),
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            0i64,
            vec!["200", "300", "500"],
        ),
        (
            "substring '0'",
            "status",
            vec!["0".to_string()],
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            0,
            vec!["200", "300", "500"],
        ),
        (
            "substring '20'",
            "status",
            vec!["20".to_string()],
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            0,
            vec!["200"],
        ),
        (
            "limit 2",
            "status",
            vec![],
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            2,
            vec!["200", "300"],
        ),
        (
            "value desc",
            "status",
            vec![],
            ingesterpb::SearchOrdering::OrderByValueDesc as i32,
            0,
            vec!["500", "300", "200"],
        ),
        (
            "missing label returns empty",
            "missing",
            vec![],
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            0,
            vec![],
        ),
    ];

    let md = MetadataMap::new();
    for (name, label_name, filter_terms, ordering, limit, want) in cases {
        let req = ingesterpb::SearchLabelValuesRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 200_000,
            name: label_name.to_string(),
            filter: Some(ingesterpb::SearchFilter {
                terms: filter_terms,
                ..Default::default()
            }),
            ordering,
            limit,
            ..Default::default()
        };
        let batches = ing.handle_search_label_values(&md, user, req).await.unwrap();
        let got: Vec<String> = batches
            .into_iter()
            .flat_map(|b| b.results.into_iter().map(|r| r.value))
            .collect();
        assert_eq!(got, want, "case {name}");
    }

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_search_test.go::TestStreamSearchResultsPropagatesWarnings
#[test]
fn test_stream_search_results_propagates_warnings() {
    let cases = [
        (
            "results with warnings: warnings ride on the final batch",
            vec![
                SearchResult { value: "a".into(), score: 1.0 },
                SearchResult { value: "b".into(), score: 0.5 },
            ],
            vec!["limit reached".to_string()],
            vec![ingesterpb::SearchResultBatch {
                results: vec![
                    ingesterpb::search_result_batch::Result { value: "a".into(), score: 1.0 },
                    ingesterpb::search_result_batch::Result { value: "b".into(), score: 0.5 },
                ],
                warnings: vec!["limit reached".into()],
            }],
        ),
        (
            "warnings only, no results: still sends a batch",
            vec![],
            vec!["no series matched".to_string()],
            vec![ingesterpb::SearchResultBatch {
                results: vec![],
                warnings: vec!["no series matched".into()],
            }],
        ),
        ("no results, no warnings: sends nothing", vec![], vec![], vec![]),
        (
            "results, no warnings: warnings field stays empty",
            vec![SearchResult { value: "x".into(), score: 1.0 }],
            vec![],
            vec![ingesterpb::SearchResultBatch {
                results: vec![ingesterpb::search_result_batch::Result {
                    value: "x".into(),
                    score: 1.0,
                }],
                warnings: vec![],
            }],
        ),
    ];

    for (name, results, warnings, want_sent) in cases {
        let mut rs = FakeSearchResultSet {
            results,
            idx: 0,
            err: None,
            warnings,
        };
        let mut sent = Vec::new();
        stream_search_results(
            &|| false,
            &mut rs,
            &mut |b| {
                sent.push(b);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(sent, want_sent, "case {name}");
    }
}

// Upstream: pkg/ingester/ingester_search_test.go::TestStreamSearchResultsPropagatesErrInsteadOfWarnings
#[test]
fn test_stream_search_results_propagates_err_instead_of_warnings() {
    let total = SEARCH_BATCH_SIZE;
    let results: Vec<SearchResult> = (0..total)
        .map(|i| SearchResult {
            value: format!("v{i:04}"),
            score: 1.0,
        })
        .collect();
    let mut rs = FakeSearchResultSet {
        results,
        idx: 0,
        err: Some("boom".to_string()),
        warnings: vec!["should not appear".to_string()],
    };
    let mut sent = Vec::new();
    let err = stream_search_results(
        &|| false,
        &mut rs,
        &mut |b| {
            sent.push(b);
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(err, "boom");
    assert_eq!(
        sent.len(),
        1,
        "trailer batch carrying warnings must be suppressed when rs.err() is Some"
    );
    assert_eq!(sent[0].results.len(), SEARCH_BATCH_SIZE);
    assert!(
        sent[0].warnings.is_empty(),
        "warnings must never ride on mid-iteration batches"
    );
}

// Upstream: pkg/ingester/ingester_search_test.go::TestStreamSearchResultsBatching
#[test]
fn test_stream_search_results_batching() {
    let total = SEARCH_BATCH_SIZE + 1;
    let results: Vec<SearchResult> = (0..total)
        .map(|i| SearchResult {
            value: format!("v{i:04}"),
            score: 1.0,
        })
        .collect();
    let mut rs = FakeSearchResultSet {
        results,
        idx: 0,
        err: None,
        warnings: vec![],
    };
    let mut sent = Vec::new();
    stream_search_results(
        &|| false,
        &mut rs,
        &mut |b| {
            sent.push(b);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].results.len(), SEARCH_BATCH_SIZE);
    assert_eq!(sent[1].results.len(), 1);
    assert_eq!(sent[0].results[0].value, "v0000");
    assert_eq!(
        sent[0].results[SEARCH_BATCH_SIZE - 1].value,
        format!("v{:04}", SEARCH_BATCH_SIZE - 1)
    );
    assert_eq!(sent[1].results[0].value, format!("v{SEARCH_BATCH_SIZE:04}"));
}

// Upstream: pkg/ingester/ingester_search_test.go::TestIngesterSearchLabelValuesRejectsInvalidFuzzThresholdAsInvalidArgument
#[tokio::test]
async fn test_ingester_search_label_values_rejects_invalid_fuzz_threshold_as_invalid_argument() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = default_ingester_test_config(tmp.path());
    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    start_and_wait_healthy(&ing, &ring).await;

    push_series_to_ingester(
        &ing,
        "test",
        &[(&[("__name__", "metric")], &[(100_000, 1.0)])],
    )
    .await
    .unwrap();

    let svc = IngesterService::new(Arc::clone(&ing));
    let mut req = Request::new(ingesterpb::SearchLabelValuesRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 200_000,
        name: "status".to_string(),
        filter: Some(ingesterpb::SearchFilter {
            terms: vec!["x".to_string()],
            fuzz_threshold: 200,
            ..Default::default()
        }),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-scope-orgid", "test".parse().unwrap());
    let status = match svc.search_label_values(req).await {
        Ok(_) => panic!("expected InvalidArgument"),
        Err(st) => st,
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_search_test.go::TestIngesterSearchLabelValuesRejectsUnknownFuzzAlgAsInvalidArgument
#[tokio::test]
async fn test_ingester_search_label_values_rejects_unknown_fuzz_alg_as_invalid_argument() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = default_ingester_test_config(tmp.path());
    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    start_and_wait_healthy(&ing, &ring).await;

    push_series_to_ingester(
        &ing,
        "test",
        &[(&[("__name__", "metric")], &[(100_000, 1.0)])],
    )
    .await
    .unwrap();

    let svc = IngesterService::new(Arc::clone(&ing));
    let mut req = Request::new(ingesterpb::SearchLabelValuesRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 200_000,
        name: "status".to_string(),
        filter: Some(ingesterpb::SearchFilter {
            terms: vec!["x".to_string()],
            fuzz_alg: 99,
            ..Default::default()
        }),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-scope-orgid", "test".parse().unwrap());
    let status = match svc.search_label_values(req).await {
        Ok(_) => panic!("expected InvalidArgument"),
        Err(st) => st,
    };
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "unknown fuzz algorithm 99");

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

// Upstream: pkg/ingester/ingester_search_test.go::TestProtoToParamsFuzzAlg
#[test]
fn test_proto_to_params_fuzz_alg() {
    let cases = [
        (
            ingesterpb::search_filter::FuzzAlg::Subsequence,
            FuzzAlg::Subsequence,
        ),
        (
            ingesterpb::search_filter::FuzzAlg::JaroWinkler,
            FuzzAlg::JaroWinkler,
        ),
        (
            ingesterpb::search_filter::FuzzAlg::SubstringLeft,
            FuzzAlg::SubstringLeft,
        ),
        (
            ingesterpb::search_filter::FuzzAlg::Substring,
            FuzzAlg::Substring,
        ),
    ];

    for (wire, want) in cases {
        let filter = ingesterpb::SearchFilter {
            terms: vec!["foo".to_string()],
            fuzz_alg: wire as i32,
            ..Default::default()
        };
        let params = proto_to_params(Some(&filter)).unwrap().unwrap();
        assert_eq!(params.fuzz_alg, want);
    }

    let bad_filter = ingesterpb::SearchFilter {
        terms: vec!["foo".to_string()],
        fuzz_alg: 99,
        ..Default::default()
    };
    let err = proto_to_params(Some(&bad_filter)).unwrap_err();
    assert_eq!(err.to_string(), "unknown fuzz algorithm 99");
}

// Upstream: pkg/ingester/ingester_search_test.go::TestStreamSearchResults
#[test]
fn test_stream_search_results() {
    // 1. splits into fixed-size wire batches
    {
        let total = SEARCH_BATCH_SIZE + 1;
        let results: Vec<SearchResult> = (0..total)
            .map(|i| SearchResult {
                value: format!("metric_{i:04}"),
                score: 1.0,
            })
            .collect();
        let mut rs = VecSearchResultSet::new(results, vec![], None);
        let mut sent = Vec::new();
        stream_search_results(
            &|| false,
            &mut rs,
            &mut |b| {
                sent.push(b);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].results.len(), SEARCH_BATCH_SIZE);
        assert_eq!(sent[1].results.len(), 1);
    }

    // 2. warnings-only batch is sent
    {
        let mut rs = VecSearchResultSet::new(vec![], vec!["all clamped".to_string()], None);
        let mut sent = Vec::new();
        stream_search_results(
            &|| false,
            &mut rs,
            &mut |b| {
                sent.push(b);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].results.is_empty());
        assert_eq!(sent[0].warnings, vec!["all clamped".to_string()]);
    }
}

// Upstream: pkg/ingester/ingester_search_test.go::TestStreamSearchResultsHonoursCtxCancellation
#[test]
fn test_stream_search_results_honours_ctx_cancellation() {
    let results: Vec<SearchResult> = (0..10)
        .map(|i| SearchResult {
            value: format!("v{i}"),
            score: 1.0,
        })
        .collect();
    let canceled = AtomicBool::new(true);
    let mut rs = VecSearchResultSet::new(results, vec![], None);
    let err = stream_search_results(&|| canceled.load(Ordering::Acquire), &mut rs, &mut |_| Ok(()))
        .unwrap_err();
    assert!(err.contains("canceled"));
}

// Upstream: pkg/ingester/ingester_search_test.go::TestBuildSearchHintsLimitGuard
#[test]
fn test_build_search_hints_limit_guard() {
    let cases = [
        ("zero is no-limit", 0i64, false, 0usize),
        ("positive passes through", 1000, false, 1000),
        ("negative is rejected", -1, true, 0),
        ("min int64 is rejected", i64::MIN, true, 0),
        ("max int64 clamps to max int", i64::MAX, false, isize::MAX as usize),
    ];
    for (name, limit, want_err, want) in cases {
        let res = build_search_hints(
            None,
            ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            limit,
            &[],
        );
        if want_err {
            assert!(res.is_err(), "case {name} expected error");
        } else {
            let (hints, _) = res.expect("expected ok");
            assert_eq!(hints.limit, want, "case {name}");
        }
    }
}

struct BlockingCoordinator {
    total_tasks: i64,
    blocked_count: AtomicI64,
    released: AtomicBool,
    all_blocked_notify: Notify,
    release_notify: Notify,
}

struct BlockingQueryStreamSink {
    coordinator: Arc<BlockingCoordinator>,
}

impl QueryStreamSink for BlockingQueryStreamSink {
    fn send<'a>(
        &'a self,
        resp: ingesterpb::QueryStreamResponse,
    ) -> BoxFuture<'a, Result<(), IngesterError>> {
        Box::pin(async move {
            if resp.is_end_of_series_stream {
                let blocked = self.coordinator.blocked_count.fetch_add(1, Ordering::AcqRel) + 1;
                if blocked == self.coordinator.total_tasks {
                    self.coordinator.all_blocked_notify.notify_waiters();
                }
                while !self.coordinator.released.load(Ordering::Acquire) {
                    self.coordinator.release_notify.notified().await;
                }
            }
            Ok(())
        })
    }
}

// Upstream: pkg/ingester/ingester_streaming_memory_test.go::TestIngester_SendStreamingQuerySeries_ConcurrentMemoryUsage
#[tokio::test]
async fn test_ingester_send_streaming_query_series_concurrent_memory_usage() {
    const NUM_SERIES: usize = 1000;
    const NUM_LABELS: usize = 20;
    const LABEL_VAL_LEN: usize = 1024;
    const NUM_TASKS: usize = 100;

    let tmp = tempfile::tempdir().unwrap();
    let cfg = default_ingester_test_config(tmp.path());
    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, None).await;
    start_and_wait_healthy(&ing, &ring).await;

    let user = "test-user";
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let large_val = "x".repeat(LABEL_VAL_LEN);

    for s in 0..NUM_SERIES {
        let mut owned_pairs: Vec<(String, String)> = Vec::with_capacity(NUM_LABELS + 1);
        owned_pairs.push(("__name__".to_string(), format!("series_{s:04}")));
        for j in 0..NUM_LABELS {
            owned_pairs.push((format!("label_{j:02}"), large_val.clone()));
        }
        let borrowed: Vec<(&str, &str)> = owned_pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        push_series_to_ingester(&ing, user, &[(&borrowed, &[(now_ms, s as f64)])])
            .await
            .unwrap();
    }

    let db = ing.get_tsdb(user).expect("db");
    ing.compact_blocks(true, now_ms + 60_000, None).await;
    assert_eq!(
        db.db.head().num_series(),
        0,
        "all series should be compacted out of head"
    );

    let matchers = vec![LabelMatcher::new(MatchType::RegexMatch, "__name__", ".*").unwrap()];
    let coordinator = Arc::new(BlockingCoordinator {
        total_tasks: NUM_TASKS as i64,
        blocked_count: AtomicI64::new(0),
        released: AtomicBool::new(false),
        all_blocked_notify: Notify::new(),
        release_notify: Notify::new(),
    });

    let baseline_rss = read_rss_bytes();

    let mut handles = Vec::with_capacity(NUM_TASKS);
    for _ in 0..NUM_TASKS {
        let ing_c = Arc::clone(&ing);
        let db_c = Arc::clone(&db);
        let matchers_c = matchers.clone();
        let coord_c = Arc::clone(&coordinator);
        handles.push(tokio::spawn(async move {
            let sink = BlockingQueryStreamSink {
                coordinator: coord_c,
            };
            ing_c
                .send_streaming_query_series(
                    &db_c,
                    now_ms - 1000,
                    now_ms + 1000,
                    &matchers_c,
                    &sink,
                )
                .await
                .map(|_| ())
        }));
    }

    tokio::time::timeout(Duration::from_secs(30), async {
        while coordinator.blocked_count.load(Ordering::Acquire) < NUM_TASKS as i64 {
            coordinator.all_blocked_notify.notified().await;
        }
    })
    .await
    .expect("timeout waiting for all tasks to block");

    let peak_rss = read_rss_bytes();
    coordinator.released.store(true, Ordering::Release);
    coordinator.release_notify.notify_waiters();

    for h in handles {
        h.await.unwrap().expect("streaming query succeeded");
    }

    let label_bytes_per_series = NUM_LABELS * LABEL_VAL_LEN;
    let batch_label_bytes = label_bytes_per_series * QUERY_STREAM_BATCH_SIZE;
    let max_allowed_memory = (4 * batch_label_bytes * NUM_TASKS) as isize;
    let memory_growth = peak_rss as isize - baseline_rss as isize;

    assert!(
        memory_growth < max_allowed_memory,
        "memory growth ({}MB) exceeded maximum allowed ({}MB)",
        memory_growth / (1024 * 1024),
        max_allowed_memory / (1024 * 1024)
    );

    let _ = ing.stop().await;
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}
