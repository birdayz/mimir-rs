//! Wave 19 Ingester Read RPC, ActivityTracker & UsageStats regression tests:
//! - W19-RPC-01: Reversed time range (end < start) on QueryStream & MetricsForLabelMatchers
//! - W19-RPC-02: QueryStream past head.max_time() & on empty Head records queried_blocks / queried_series
//! - W19-RPC-03: QueryStream with streaming_chunks_batch_size >= 1 << 63 (u64::MAX) emits no spurious empty frame
//! - W19-RPC-04: LabelValuesCardinality & LabelValues/SearchLabelValues unexpected all postings parity
//! - W19-RPC-05: __query_shard__ rejects leading '+', MirroredChunkQuerier calls mq.close() on Select error
//!   with "selecting series from ChunkQuerier: ", SearchLabel* invalid threshold msg
//! - W19-MOD-02: ActivityTracker formats all 13 cortex.Ingester read RPC requests
//! - W19-MOD-04: update_usage_stats tracks min_ooo_ns in nanoseconds before dividing to seconds

mod common;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http;
use bytes::Bytes;
use mimir_common::activitytracker::{load_unfinished_entries, ActivityTracker, ActivityTrackerConfig};
use mimir_config::{ModelDuration, Overrides};
use mimir_ingester::activity::{request_activity, ActivityRequest};
use mimir_ingester::query::remove_shard_from_matchers;
use mimir_ingester::IngesterService;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_tsdb::matcher::{LabelMatcher, MatchType};
use prometheus::Registry;
use tempfile::TempDir;
use tonic::metadata::MetadataMap;
use tower::ServiceExt;

use common::{
    default_ingester_test_config, default_limits_test_config, metric_value,
    prepare_ingester_with_blocks_storage, start_and_wait_healthy,
};

struct SingleChunkBody(Option<Bytes>);

impl hyper::body::Body for SingleChunkBody {
    type Data = Bytes;
    type Error = tonic::Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.0.take() {
            Some(b) => Poll::Ready(Some(Ok(hyper::body::Frame::data(b)))),
            None => Poll::Ready(None),
        }
    }
}

fn grpc_frame<M: Message>(msg: &M) -> Bytes {
    let payload = msg.encode_to_vec();
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Bytes::from(out)
}

fn decode_grpc_frames<M: Message + Default>(data: &[u8]) -> Vec<M> {
    let mut frames = Vec::new();
    let mut off = 0usize;
    while off + 5 <= data.len() {
        let len = u32::from_be_bytes([data[off + 1], data[off + 2], data[off + 3], data[off + 4]])
            as usize;
        off += 5;
        let msg = M::decode(&data[off..off + len]).expect("decode grpc frame");
        frames.push(msg);
        off += len;
    }
    assert_eq!(off, data.len(), "trailing bytes in gRPC frame stream");
    frames
}

async fn collect_grpc_http_resp(
    mut resp: http::Response<tonic::body::BoxBody>,
) -> (http::HeaderMap, Option<tonic::Status>, Vec<u8>) {
    let headers = resp.headers().clone();
    let mut trailers = http::HeaderMap::new();
    let mut data = Vec::new();
    let body = resp.body_mut();
    while let Some(frame_res) =
        futures::future::poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut *body), cx)).await
    {
        let frame = frame_res.expect("body frame");
        if frame.is_data() {
            if let Ok(chunk) = frame.into_data() {
                data.extend_from_slice(&chunk);
            }
        } else if let Ok(tr) = frame.into_trailers() {
            trailers.extend(tr);
        }
    }
    let status = tonic::Status::from_header_map(&trailers)
        .or_else(|| tonic::Status::from_header_map(&headers));
    (headers, status, data)
}

#[allow(clippy::result_large_err)]
async fn grpc_unary_call<Req: Message, Resp: Message + Default>(
    svc: &mut mimir_ingester::grpc::IngesterZeroCopyServer,
    path: &str,
    tenant: &str,
    req: &Req,
) -> Result<Resp, tonic::Status> {
    let http_req = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("x-scope-orgid", tenant)
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(grpc_frame(req)))))
        .unwrap();
    let resp = svc.oneshot(http_req).await.unwrap();
    let (_, status, data) = collect_grpc_http_resp(resp).await;
    if let Some(st) = status {
        if st.code() != tonic::Code::Ok {
            return Err(st);
        }
    }
    let mut frames = decode_grpc_frames::<Resp>(&data);
    assert_eq!(frames.len(), 1, "expected single unary response frame");
    Ok(frames.remove(0))
}

#[allow(clippy::result_large_err)]
async fn grpc_stream_call<Req: Message, Resp: Message + Default>(
    svc: &mut mimir_ingester::grpc::IngesterZeroCopyServer,
    path: &str,
    tenant: &str,
    req: &Req,
) -> Result<Vec<Resp>, tonic::Status> {
    let http_req = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("x-scope-orgid", tenant)
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(grpc_frame(req)))))
        .unwrap();
    let resp = svc.oneshot(http_req).await.unwrap();
    let (_, status, data) = collect_grpc_http_resp(resp).await;
    if let Some(st) = status {
        if st.code() != tonic::Code::Ok {
            return Err(st);
        }
    }
    Ok(decode_grpc_frames::<Resp>(&data))
}

fn proto_matcher(t: ingesterpb::MatchType, name: &str, value: &str) -> ingesterpb::LabelMatcher {
    ingesterpb::LabelMatcher {
        r#type: t as i32,
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn write_req(name: &str, job: &str, samples: &[(i64, f64)]) -> cortexpb::WriteRequest {
    cortexpb::WriteRequest {
        source: cortexpb::write_request::SourceEnum::Api as i32,
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", name),
                cortexpb::LabelPair::from_strs("job", job),
            ],
            samples: samples
                .iter()
                .map(|&(t, v)| cortexpb::Sample {
                    timestamp_ms: t,
                    value: v,
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w19_rpc_01_reversed_time_range_query_stream_and_metrics_for_label_matchers() {
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
    start_and_wait_healthy(&ing).await;

    let user = "user-rpc01";
    ing.push_grpc(user, &write_req("up", "api", &[(1_000, 1.0), (2_000, 2.0)]))
        .await
        .unwrap();

    let mut svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // QueryStream with reversed range start=1800 > end=1200 overlapping [1000, 2000].
    let qs_frames: Vec<ingesterpb::QueryStreamResponse> = grpc_stream_call(
        &mut svc,
        "/cortex.Ingester/QueryStream",
        user,
        &ingesterpb::QueryRequest {
            start_timestamp_ms: 1_800,
            end_timestamp_ms: 1_200,
            matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "up")],
            streaming_chunks_batch_size: 16,
        },
    )
    .await
    .unwrap();
    let series_count: usize = qs_frames.iter().map(|f| f.streaming_series.len()).sum();
    let chunk_series_count: usize = qs_frames
        .iter()
        .map(|f| f.streaming_series_chunks.len())
        .sum();
    assert_eq!(series_count, 1, "QueryStream must return in-order Head series on overlapping reversed range");
    assert_eq!(chunk_series_count, 1);

    // MetricsForLabelMatchers with reversed range start=1800 > end=1200.
    let mflm_resp: ingesterpb::MetricsForLabelMatchersResponse = grpc_unary_call(
        &mut svc,
        "/cortex.Ingester/MetricsForLabelMatchers",
        user,
        &ingesterpb::MetricsForLabelMatchersRequest {
            start_timestamp_ms: 1_800,
            end_timestamp_ms: 1_200,
            matchers_set: vec![ingesterpb::LabelMatchers {
                matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "up")],
            }],
            limit: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        mflm_resp.metric.len(),
        1,
        "MetricsForLabelMatchers must return matching Head series on overlapping reversed range"
    );

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w19_rpc_02_query_stream_past_head_max_time_and_empty_head_records_metrics() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.head_postings_for_matchers_cache_force = true;
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

    let mut svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // Trigger B: open an empty tenant TSDB and QueryStream with end_timestamp_ms = i64::MAX.
    let empty_user = "user-empty";
    ing.get_or_create_tsdb(empty_user).unwrap();
    let frames_empty: Vec<ingesterpb::QueryStreamResponse> = grpc_stream_call(
        &mut svc,
        "/cortex.Ingester/QueryStream",
        empty_user,
        &ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: i64::MAX,
            matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "up")],
            streaming_chunks_batch_size: 16,
        },
    )
    .await
    .unwrap();
    assert!(frames_empty.iter().all(|f| f.streaming_series.is_empty()));
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(1.0),
        "empty Head with end=i64::MAX must still query Head and increment queried_blocks_total"
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_index")]),
        1
    );

    // Trigger A: push sample at t=1000, then QueryStream [2000, 5000] (> head.max_time()).
    let user = "user-future";
    ing.push_grpc(user, &write_req("up", "api", &[(1_000, 1.0)]))
        .await
        .unwrap();
    let frames_future: Vec<ingesterpb::QueryStreamResponse> = grpc_stream_call(
        &mut svc,
        "/cortex.Ingester/QueryStream",
        user,
        &ingesterpb::QueryRequest {
            start_timestamp_ms: 2_000,
            end_timestamp_ms: 5_000,
            matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "up")],
            streaming_chunks_batch_size: 16,
        },
    )
    .await
    .unwrap();
    assert!(frames_future.iter().all(|f| f.streaming_series.is_empty()));
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(2.0),
        "start > head.max_time() must still query Head and increment queried_blocks_total"
    );
    assert_eq!(
        hist_sample_count(&reg, "cortex_ingester_queried_series", &[("stage", "single_block_filter")]),
        2
    );

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w19_rpc_03_query_stream_huge_batch_size_no_spurious_empty_frame() {
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
    start_and_wait_healthy(&ing).await;

    let user = "user-rpc03";
    ing.push_grpc(user, &write_req("up", "api", &[(1_000, 1.0)]))
        .await
        .unwrap();

    let mut svc = IngesterService::new(Arc::clone(&ing)).into_server();
    for batch_size in [u64::MAX, 1u64 << 63] {
        let frames: Vec<ingesterpb::QueryStreamResponse> = grpc_stream_call(
            &mut svc,
            "/cortex.Ingester/QueryStream",
            user,
            &ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10_000,
                matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "up")],
                streaming_chunks_batch_size: batch_size,
            },
        )
        .await
        .unwrap();

        for (idx, f) in frames.iter().enumerate() {
            let is_completely_empty = f.streaming_series.is_empty()
                && !f.is_end_of_series_stream
                && f.streaming_series_chunks.is_empty();
            assert!(
                !is_completely_empty,
                "frame {idx} for batch_size={batch_size} must not be a spurious empty frame: {f:?}"
            );
        }
        let chunk_series: usize = frames.iter().map(|f| f.streaming_series_chunks.len()).sum();
        assert_eq!(chunk_series, 1);
    }

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w19_rpc_04_unexpected_all_postings_on_cardinality_and_empty_label_values() {
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
    start_and_wait_healthy(&ing).await;

    let user = "user-rpc04";
    ing.push_grpc(user, &write_req("up", "api", &[(1_000, 1.0)]))
        .await
        .unwrap();

    let mut svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // 1. LabelValuesCardinality with [{}] must fail with Internal("unexpected all postings")
    // for both IN_MEMORY and ACTIVE.
    for count_method in [
        ingesterpb::CountMethod::InMemory as i32,
        ingesterpb::CountMethod::Active as i32,
    ] {
        let err = grpc_stream_call::<_, ingesterpb::LabelValuesCardinalityResponse>(
            &mut svc,
            "/cortex.Ingester/LabelValuesCardinality",
            user,
            &ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["job".to_string()],
                matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "", "")],
                count_method,
            },
        )
        .await
        .expect_err("LabelValuesCardinality with [{}] must fail");
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(
            err.message().contains("unexpected all postings"),
            "unexpected error message: {}",
            err.message()
        );
    }

    // Also test unary handle_label_values_cardinality directly.
    let err_unary = ing
        .handle_label_values_cardinality(
            &MetadataMap::new(),
            user,
            ingesterpb::LabelValuesCardinalityRequest {
                label_names: vec!["job".to_string()],
                matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "", "")],
                count_method: ingesterpb::CountMethod::InMemory as i32,
            },
        )
        .await
        .expect_err("unary handle_label_values_cardinality with [{}] must fail");
    assert!(err_unary.to_string().contains("unexpected all postings"));

    // 2. LabelValues with label_name="" and [{Equal, "", ""}, {Equal, "job", "api"}].
    let err_lv = grpc_unary_call::<_, ingesterpb::LabelValuesResponse>(
        &mut svc,
        "/cortex.Ingester/LabelValues",
        user,
        &ingesterpb::LabelValuesRequest {
            label_name: String::new(),
            start_timestamp_ms: 0,
            end_timestamp_ms: i64::MAX,
            matchers: Some(ingesterpb::LabelMatchers {
                matchers: vec![
                    proto_matcher(ingesterpb::MatchType::Equal, "", ""),
                    proto_matcher(ingesterpb::MatchType::Equal, "job", "api"),
                ],
            }),
            limit: 0,
        },
    )
    .await
    .expect_err("LabelValues(name=\"\", [{}, job=\"api\"]) must fail");
    assert_eq!(err_lv.code(), tonic::Code::Internal);

    // 3. SearchLabelValues with name="" and [{Equal, "", ""}, {Equal, "job", "api"}].
    let err_slv = grpc_stream_call::<_, ingesterpb::SearchResultBatch>(
        &mut svc,
        "/cortex.Ingester/SearchLabelValues",
        user,
        &ingesterpb::SearchLabelValuesRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: i64::MAX,
            name: String::new(),
            matchers: vec![
                proto_matcher(ingesterpb::MatchType::Equal, "", ""),
                proto_matcher(ingesterpb::MatchType::Equal, "job", "api"),
            ],
            filter: None,
            ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            limit: 0,
        },
    )
    .await
    .expect_err("SearchLabelValues(name=\"\", [{}, job=\"api\"]) must fail");
    assert_eq!(err_slv.code(), tonic::Code::Internal);

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w19_rpc_05_shard_leading_plus_mirrored_close_and_threshold_msg() {
    // 1. __query_shard__ rejects leading '+'.
    for bad in ["+1_of_2", "1_of_+2"] {
        let m = vec![LabelMatcher::new(MatchType::Equal, "__query_shard__", bad).unwrap()];
        assert!(
            remove_shard_from_matchers(m).is_err(),
            "__query_shard__={bad:?} must be rejected"
        );
    }

    // 2. MirroredChunkQuerier error path closes mq (recording outcome metric) and uses
    // "selecting series from ChunkQuerier: " prefix; SearchLabelNames invalid threshold msg.
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_comparison_portion = 1.0;
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

    let user = "user-rpc05";
    ing.push_grpc(user, &write_req("up", "api", &[(1_000, 1.0)]))
        .await
        .unwrap();

    let mut svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // Passing [{Equal, "", ""}, {Equal, "job", "api"}] bypasses from_proto_matchers and fails inside
    // MirroredChunkQuerier::select with UnexpectedAllPostings.
    let err_qs = grpc_stream_call::<_, ingesterpb::QueryStreamResponse>(
        &mut svc,
        "/cortex.Ingester/QueryStream",
        user,
        &ingesterpb::QueryRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 10_000,
            matchers: vec![
                proto_matcher(ingesterpb::MatchType::Equal, "", ""),
                proto_matcher(ingesterpb::MatchType::Equal, "job", "api"),
            ],
            streaming_chunks_batch_size: 16,
        },
    )
    .await
    .expect_err("QueryStream with [{}, job=\"api\"] must fail");
    assert_eq!(err_qs.code(), tonic::Code::Internal);
    assert!(
        err_qs.message().starts_with("selecting series from ChunkQuerier: "),
        "unexpected prefix: {}",
        err_qs.message()
    );
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
            &[("outcome", "returned_series_error"), ("user", user)],
        ),
        Some(1.0),
        "mq.close() must record returned_series_error when MirroredChunkQuerier::select fails"
    );

    // 3. SearchLabelNames invalid fuzz_threshold message matches Go "[0,100]" (no space).
    let err_sln = grpc_stream_call::<_, ingesterpb::SearchResultBatch>(
        &mut svc,
        "/cortex.Ingester/SearchLabelNames",
        user,
        &ingesterpb::SearchLabelNamesRequest {
            start_timestamp_ms: 0,
            end_timestamp_ms: 10_000,
            matchers: vec![],
            filter: Some(ingesterpb::SearchFilter {
                terms: vec!["up".to_string()],
                case_insensitive: false,
                fuzz_alg: ingesterpb::search_filter::FuzzAlg::Subsequence as i32,
                fuzz_threshold: -1,
            }),
            ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            limit: 0,
        },
    )
    .await
    .expect_err("SearchLabelNames with fuzz_threshold=-1 must fail");
    assert_eq!(err_sln.code(), tonic::Code::InvalidArgument);
    assert_eq!(err_sln.message(), "fuzz threshold -1 out of [0,100]");

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w19_mod_02_activity_tracker_formats_all_13_read_rpcs() {
    let m_eq = ingesterpb::LabelMatcher {
        r#type: ingesterpb::MatchType::Equal as i32,
        name: "__name__".to_string(),
        value: "up".to_string(),
    };
    let lms = ingesterpb::LabelMatchers {
        matchers: vec![m_eq.clone()],
    };

    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/QueryExemplars",
            ActivityRequest::ExemplarQuery(&ingesterpb::ExemplarQueryRequest {
                start_timestamp_ms: 10,
                end_timestamp_ms: 20,
                matchers: vec![lms.clone()],
            }),
        ),
        "Ingester/QueryExemplars: user=\"u1\" trace=\"t1\" request=&ExemplarQueryRequest{StartTimestampMs:10,EndTimestampMs:20,Matchers:[]*LabelMatchers{&LabelMatchers{Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:__name__,Value:up,},},},},}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/LabelValues",
            ActivityRequest::LabelValues(&ingesterpb::LabelValuesRequest {
                label_name: "job".to_string(),
                start_timestamp_ms: 10,
                end_timestamp_ms: 20,
                matchers: Some(lms.clone()),
                limit: 5,
            }),
        ),
        "Ingester/LabelValues: user=\"u1\" trace=\"t1\" request=&LabelValuesRequest{LabelName:job,StartTimestampMs:10,EndTimestampMs:20,Matchers:&LabelMatchers{Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:__name__,Value:up,},},},Limit:5,}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/LabelNames",
            ActivityRequest::LabelNames(&ingesterpb::LabelNamesRequest {
                start_timestamp_ms: 10,
                end_timestamp_ms: 20,
                matchers: None,
                limit: 0,
            }),
        ),
        "Ingester/LabelNames: user=\"u1\" trace=\"t1\" request=&LabelNamesRequest{StartTimestampMs:10,EndTimestampMs:20,Matchers:nil,Limit:0,}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/UserStats",
            ActivityRequest::UserStats(&ingesterpb::UserStatsRequest {
                count_method: ingesterpb::CountMethod::Active as i32,
            }),
        ),
        "Ingester/UserStats: user=\"u1\" trace=\"t1\" request=&UserStatsRequest{CountMethod:ACTIVE,}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/MetricsForLabelMatchers",
            ActivityRequest::MetricsForLabelMatchers(&ingesterpb::MetricsForLabelMatchersRequest {
                start_timestamp_ms: 1,
                end_timestamp_ms: 2,
                matchers_set: vec![lms.clone()],
                limit: 10,
            }),
        ),
        "Ingester/MetricsForLabelMatchers: user=\"u1\" trace=\"t1\" request=&MetricsForLabelMatchersRequest{StartTimestampMs:1,EndTimestampMs:2,MatchersSet:[]*LabelMatchers{&LabelMatchers{Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:__name__,Value:up,},},},},Limit:10,}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/LabelNamesAndValues",
            ActivityRequest::LabelNamesAndValues(&ingesterpb::LabelNamesAndValuesRequest {
                matchers: vec![m_eq.clone()],
                count_method: ingesterpb::CountMethod::InMemory as i32,
            }),
        ),
        "Ingester/LabelNamesAndValues: user=\"u1\" trace=\"t1\" request=&LabelNamesAndValuesRequest{Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:__name__,Value:up,},},CountMethod:IN_MEMORY,}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/ActiveSeries",
            ActivityRequest::ActiveSeries(&ingesterpb::ActiveSeriesRequest {
                matchers: vec![m_eq.clone()],
                r#type: ingesterpb::active_series_request::RequestType::NativeHistogramSeries as i32,
            }),
        ),
        "Ingester/ActiveSeries: user=\"u1\" trace=\"t1\" request=&ActiveSeriesRequest{Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:__name__,Value:up,},},Type:NATIVE_HISTOGRAM_SERIES,}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/SearchLabelNames",
            ActivityRequest::SearchLabelNames(&ingesterpb::SearchLabelNamesRequest {
                start_timestamp_ms: 1,
                end_timestamp_ms: 2,
                matchers: vec![m_eq.clone()],
                filter: Some(ingesterpb::SearchFilter {
                    terms: vec!["foo".to_string(), "bar".to_string()],
                    case_insensitive: true,
                    fuzz_alg: ingesterpb::search_filter::FuzzAlg::Substring as i32,
                    fuzz_threshold: 50,
                }),
                ordering: ingesterpb::SearchOrdering::OrderByScoreDesc as i32,
                limit: 25,
            }),
        ),
        "Ingester/SearchLabelNames: user=\"u1\" trace=\"t1\" request=&SearchLabelNamesRequest{StartTimestampMs:1,EndTimestampMs:2,Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:__name__,Value:up,},},Filter:&SearchFilter{Terms:[foo bar],CaseInsensitive:true,FuzzAlg:FUZZ_ALG_SUBSTRING,FuzzThreshold:50,},Ordering:ORDER_BY_SCORE_DESC,Limit:25,}"
    );
    assert_eq!(
        request_activity(
            "u1",
            "t1",
            "Ingester/SearchLabelValues",
            ActivityRequest::SearchLabelValues(&ingesterpb::SearchLabelValuesRequest {
                start_timestamp_ms: 1,
                end_timestamp_ms: 2,
                name: "job".to_string(),
                matchers: vec![m_eq],
                filter: None,
                ordering: ingesterpb::SearchOrdering::OrderByValueDesc as i32,
                limit: 10,
            }),
        ),
        "Ingester/SearchLabelValues: user=\"u1\" trace=\"t1\" request=&SearchLabelValuesRequest{StartTimestampMs:1,EndTimestampMs:2,Name:job,Matchers:[]*LabelMatcher{&LabelMatcher{Type:EQUAL,Name:__name__,Value:up,},},Filter:nil,Ordering:ORDER_BY_VALUE_DESC,Limit:10,}"
    );

    // End-to-end gRPC dispatch verification: streaming handlers hold ActivityStream guard until
    // response body is dropped, so the formatted request (never "<nil>") is present on disk.
    let dir = TempDir::new().unwrap();
    let act_path = dir.path().join("metrics-activity.log");
    let tracker = Arc::new(
        ActivityTracker::new(
            ActivityTrackerConfig {
                filepath: act_path.clone(),
                max_entries: 16,
            },
            None,
        )
        .unwrap()
        .unwrap(),
    );
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    let (ing, _, _) = prepare_ingester_with_blocks_storage(
        dir.path(),
        cfg,
        blocks_cfg,
        ingest_cfg,
        default_limits_test_config(),
        None,
    );
    ing.set_activity_tracker(Some(Arc::clone(&tracker)));
    start_and_wait_healthy(&ing).await;

    for (uri, body_bytes, expected_prefix) in [
        (
            "/cortex.Ingester/LabelNamesAndValues",
            grpc_frame(&ingesterpb::LabelNamesAndValuesRequest::default()),
            "Ingester/LabelNamesAndValues: user=\"user-act\" trace=\"\" request=&LabelNamesAndValuesRequest{",
        ),
        (
            "/cortex.Ingester/ActiveSeries",
            grpc_frame(&ingesterpb::ActiveSeriesRequest::default()),
            "Ingester/ActiveSeries: user=\"user-act\" trace=\"\" request=&ActiveSeriesRequest{",
        ),
        (
            "/cortex.Ingester/SearchLabelNames",
            grpc_frame(&ingesterpb::SearchLabelNamesRequest::default()),
            "Ingester/SearchLabelNames: user=\"user-act\" trace=\"\" request=&SearchLabelNamesRequest{",
        ),
        (
            "/cortex.Ingester/SearchLabelValues",
            grpc_frame(&ingesterpb::SearchLabelValuesRequest {
                name: "job".to_string(),
                ..Default::default()
            }),
            "Ingester/SearchLabelValues: user=\"user-act\" trace=\"\" request=&SearchLabelValuesRequest{",
        ),
    ] {
        let http_req = http::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-scope-orgid", "user-act")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(body_bytes))))
            .unwrap();
        let svc = IngesterService::new(Arc::clone(&ing)).into_server();
        let resp = svc.oneshot(http_req).await.unwrap();
        let entries = load_unfinished_entries(&act_path).unwrap();
        assert!(
            entries.iter().any(|e| e.activity.starts_with(expected_prefix)),
            "expected activity starting with {expected_prefix:?}, got {entries:?}"
        );
        drop(resp);
        assert!(
            load_unfinished_entries(&act_path).unwrap().is_empty(),
            "activity slot must be freed after dropping response stream"
        );
    }

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w19_mod_04_update_usage_stats_subsecond_ooo_window() {
    let dir = TempDir::new().unwrap();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    let mut default_limits = default_limits_test_config();
    default_limits.out_of_order_time_window = ModelDuration::from_millis(600_000);
    let mut tenant_a_limits = default_limits.clone();
    tenant_a_limits.out_of_order_time_window = ModelDuration::from_millis(500);

    let overrides = Overrides::with_static_map(
        default_limits,
        HashMap::from([("tenant-a".to_string(), tenant_a_limits)]),
    );

    let kv: Arc<dyn mimir_ring::kv::KvClient> =
        Arc::new(mimir_ring::kv::inmemory::InMemoryKv::new(mimir_ring::ring::ring_desc_codec()));
    let (ing, _) = common::prepare_ingester_with_kv_and_overrides(
        cfg,
        blocks_cfg,
        ingest_cfg,
        overrides,
        kv,
        None,
    );
    start_and_wait_healthy(&ing).await;

    ing.push_grpc("tenant-a", &write_req("up", "a", &[(10_000, 1.0)]))
        .await
        .unwrap();
    ing.push_grpc("tenant-b", &write_req("up", "b", &[(10_000, 2.0)]))
        .await
        .unwrap();

    let snap = ing.update_usage_stats();
    assert_eq!(snap.tenants_with_out_of_order_enabled, 2);
    assert_eq!(
        snap.min_out_of_order_time_window_seconds, 0,
        "sub-second 500ms OOO window must yield min=0s, not be overwritten by tenant-b's 600s"
    );
    assert_eq!(snap.max_out_of_order_time_window_seconds, 600);
    ing.stop().await.unwrap();
}
