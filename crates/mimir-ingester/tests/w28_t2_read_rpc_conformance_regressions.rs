//! Wave 28 Track 2 Read RPC, LookupPlanner, MirroredChunkQuerier & Activity Tracker regressions:
//! - A-01: Inverted time range (start_timestamp_ms > end_timestamp_ms) Head overlap across all 5 RPCs
//! - A-02: LookupPlanner wired into MetricsForLabelMatchers (standard & raw fast-path)
//! - A-03: MirroredChunkQuerier secondary un-planned Select metrics (2 observations per mirrored block, 1 queried_blocks)
//! - A-04: exact_metric_name_from_matchers checked on post-plan index_matchers in observe_block_select_stats_with_estimates
//! - A-05: SharedRetainingChunkSeriesSet::at() O(1) per series & idempotent repeated at()
//! - A-06: ActivityTracker AllUserStats tenant ID & unknown CountMethod/MatchType numeric formatting

mod common;

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http;
use bytes::Bytes;
use mimir_common::activitytracker::{load_unfinished_entries, ActivityTracker, ActivityTrackerConfig};
use mimir_ingester::activity::{label_matcher_to_string, request_activity, ActivityRequest};
use mimir_ingester::mirror_querier::{
    ChunkBlockMeta, ChunkQuerier, ChunkSeries, ChunkSeriesSet, MirroredChunkQuerier, QuerierError,
};
use mimir_ingester::IngesterService;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_tsdb::chunks_block::ChunkMeta;
use mimir_tsdb::labels::Labels;
use mimir_tsdb::matcher::LabelMatcher;
use prometheus::{CounterVec, Opts, Registry};
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

fn hist_stats(reg: &Registry, name: &str, labels: &[(&str, &str)]) -> (u64, f64) {
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
                let h = m.get_histogram();
                return (h.get_sample_count(), h.get_sample_sum());
            }
        }
    }
    (0, 0.0)
}

fn hist_total_count(reg: &Registry, name: &str) -> u64 {
    let mut total = 0u64;
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                total += m.get_histogram().get_sample_count();
            }
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a01_inverted_time_range_head_overlap_all_five_rpcs() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .index_lookup_planning_comparison_portion = 1.0;

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

    let user = "user-a01";
    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "a01_metric"),
                        cortexpb::LabelPair::from_strs("env", "prod"),
                    ],
                    samples: vec![
                        cortexpb::Sample {
                            timestamp_ms: 1_000,
                            value: 1.0,
                        },
                        cortexpb::Sample {
                            timestamp_ms: 5_000,
                            value: 2.0,
                        },
                    ],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair::from_strs("__name__", "a01_metric"),
                        cortexpb::LabelPair::from_strs("env", "staging"),
                    ],
                    samples: vec![
                        cortexpb::Sample {
                            timestamp_ms: 1_000,
                            value: 3.0,
                        },
                        cortexpb::Sample {
                            timestamp_ms: 5_000,
                            value: 4.0,
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // 1. LabelNames with inverted range [3000, 2000] overlapping Head [1000, 5000].
    let ln_resp: ingesterpb::LabelNamesResponse = grpc_unary_call(
        &mut svc,
        "/cortex.Ingester/LabelNames",
        user,
        &ingesterpb::LabelNamesRequest {
            start_timestamp_ms: 3_000,
            end_timestamp_ms: 2_000,
            matchers: None,
            limit: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(ln_resp.label_names, vec!["__name__", "env"]);

    // 2. LabelValues with inverted range [3000, 2000].
    let lv_resp: ingesterpb::LabelValuesResponse = grpc_unary_call(
        &mut svc,
        "/cortex.Ingester/LabelValues",
        user,
        &ingesterpb::LabelValuesRequest {
            label_name: "env".to_string(),
            start_timestamp_ms: 3_000,
            end_timestamp_ms: 2_000,
            matchers: None,
            limit: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(lv_resp.label_values, vec!["prod", "staging"]);

    // 3. SearchLabelNames with inverted range [3000, 2000].
    let sln_frames: Vec<ingesterpb::SearchResultBatch> = grpc_stream_call(
        &mut svc,
        "/cortex.Ingester/SearchLabelNames",
        user,
        &ingesterpb::SearchLabelNamesRequest {
            start_timestamp_ms: 3_000,
            end_timestamp_ms: 2_000,
            matchers: Vec::new(),
            filter: None,
            ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            limit: 10,
        },
    )
    .await
    .unwrap();
    let sln_vals: Vec<String> = sln_frames
        .into_iter()
        .flat_map(|f| f.results.into_iter().map(|r| r.value))
        .collect();
    assert_eq!(sln_vals, vec!["__name__", "env"]);

    // 4. SearchLabelValues with inverted range [3000, 2000].
    let slv_frames: Vec<ingesterpb::SearchResultBatch> = grpc_stream_call(
        &mut svc,
        "/cortex.Ingester/SearchLabelValues",
        user,
        &ingesterpb::SearchLabelValuesRequest {
            start_timestamp_ms: 3_000,
            end_timestamp_ms: 2_000,
            name: "env".to_string(),
            matchers: Vec::new(),
            filter: None,
            ordering: ingesterpb::SearchOrdering::OrderByValueAsc as i32,
            limit: 10,
        },
    )
    .await
    .unwrap();
    let slv_vals: Vec<String> = slv_frames
        .into_iter()
        .flat_map(|f| f.results.into_iter().map(|r| r.value))
        .collect();
    assert_eq!(slv_vals, vec!["prod", "staging"]);

    // 5. Planned + Mirrored QueryStream with inverted range [3000, 2000].
    let qs_frames: Vec<ingesterpb::QueryStreamResponse> = grpc_stream_call(
        &mut svc,
        "/cortex.Ingester/QueryStream",
        user,
        &ingesterpb::QueryRequest {
            start_timestamp_ms: 3_000,
            end_timestamp_ms: 2_000,
            matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "a01_metric")],
            streaming_chunks_batch_size: 16,
        },
    )
    .await
    .unwrap();
    let qs_series: usize = qs_frames.iter().map(|f| f.streaming_series.len()).sum();
    let qs_chunks: usize = qs_frames
        .iter()
        .flat_map(|f| f.streaming_series_chunks.iter())
        .map(|sc| sc.chunks.len())
        .sum();
    assert_eq!(qs_series, 2);
    assert_eq!(qs_chunks, 2);
    assert_eq!(
        metric_value(
            &reg,
            "cortex_ingester_index_lookup_planning_comparison_outcomes_total",
            &[("outcome", "success"), ("user", user)]
        ),
        Some(1.0)
    );

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a02_metrics_for_label_matchers_invokes_lookup_planner_standard_and_raw() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .min_series_per_block_for_query_planning = 1;

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

    let user = "user-a02";
    // Compact one block with 20 series so both Block and Head planners are exercised.
    let mut blk_series = Vec::new();
    for i in 0..20 {
        blk_series.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", "a02_blk"),
                cortexpb::LabelPair::from_strs("idx", i.to_string()),
                cortexpb::LabelPair::from_strs("job", if i == 0 { "rare_blk" } else { "common" }),
            ],
            samples: vec![cortexpb::Sample {
                timestamp_ms: 1_000,
                value: i as f64,
            }],
            ..Default::default()
        });
    }
    blk_series.push(cortexpb::TimeSeries {
        labels: vec![
            cortexpb::LabelPair::from_strs("__name__", "a02_other_blk"),
            cortexpb::LabelPair::from_strs("idx", "99"),
            cortexpb::LabelPair::from_strs("job", "rare_blk"),
        ],
        samples: vec![cortexpb::Sample {
            timestamp_ms: 1_000,
            value: 99.0,
        }],
        ..Default::default()
    });
    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: blk_series,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let db = ing.get_tsdb(user).unwrap();
    db.db.compact_head_range(1_000, 2_000).unwrap().unwrap();
    db.db.reload_blocks().unwrap();

    // Push 20 series into Head as well.
    let mut head_series = Vec::new();
    for i in 0..20 {
        head_series.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", "a02_head"),
                cortexpb::LabelPair::from_strs("idx", i.to_string()),
                cortexpb::LabelPair::from_strs("job", if i == 0 { "rare_head" } else { "common" }),
            ],
            samples: vec![cortexpb::Sample {
                timestamp_ms: 50_000,
                value: i as f64,
            }],
            ..Default::default()
        });
    }
    head_series.push(cortexpb::TimeSeries {
        labels: vec![
            cortexpb::LabelPair::from_strs("__name__", "a02_other_head"),
            cortexpb::LabelPair::from_strs("idx", "99"),
            cortexpb::LabelPair::from_strs("job", "rare_head"),
        ],
        samples: vec![cortexpb::Sample {
            timestamp_ms: 50_000,
            value: 99.0,
        }],
        ..Default::default()
    });
    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: head_series,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db.generate_head_statistics();

    assert_eq!(
        hist_stats(
            &reg,
            "cortex_tsdb_index_lookup_planning_duration_seconds",
            &[("outcome", "success"), ("user", user)]
        )
        .0,
        0
    );

    let req = ingesterpb::MetricsForLabelMatchersRequest {
        start_timestamp_ms: 0,
        end_timestamp_ms: 60_000,
        matchers_set: vec![
            ingesterpb::LabelMatchers {
                matchers: vec![
                    proto_matcher(ingesterpb::MatchType::Equal, "__name__", "a02_blk"),
                    proto_matcher(ingesterpb::MatchType::Equal, "job", "rare_blk"),
                ],
            },
            ingesterpb::LabelMatchers {
                matchers: vec![
                    proto_matcher(ingesterpb::MatchType::Equal, "__name__", "a02_head"),
                    proto_matcher(ingesterpb::MatchType::Equal, "job", "rare_head"),
                ],
            },
        ],
        limit: 100,
    };

    // Standard path: 2 matcher sets * 2 overlapping sources (Head + Block) = 4 planning observations.
    let resp = ing
        .handle_metrics_for_label_matchers(&MetadataMap::new(), user, req.clone())
        .await
        .unwrap();
    assert_eq!(resp.metric.len(), 2);
    assert_eq!(
        hist_stats(
            &reg,
            "cortex_tsdb_index_lookup_planning_duration_seconds",
            &[("outcome", "success"), ("user", user)]
        )
        .0,
        4
    );

    // Raw fast-path via gRPC service: adds another 4 planning observations and returns identical metrics.
    let mut svc = IngesterService::new(Arc::clone(&ing)).into_server();
    let raw_resp: ingesterpb::MetricsForLabelMatchersResponse = grpc_unary_call(
        &mut svc,
        "/cortex.Ingester/MetricsForLabelMatchers",
        user,
        &req,
    )
    .await
    .unwrap();
    assert_eq!(raw_resp.metric, resp.metric);
    assert_eq!(
        hist_stats(
            &reg,
            "cortex_tsdb_index_lookup_planning_duration_seconds",
            &[("outcome", "success"), ("user", user)]
        )
        .0,
        8
    );

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a03_mirrored_chunk_querier_records_secondary_unplanned_stats_twice() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .index_lookup_planning_comparison_portion = 1.0;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .min_series_per_block_for_query_planning = 1;
    blocks_cfg.tsdb.head_postings_for_matchers_cache_force = true;
    blocks_cfg.tsdb.block_postings_for_matchers_cache_force = true;

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

    let user = "user-a03";
    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", "a03_m"),
                    cortexpb::LabelPair::from_strs("job", "api"),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 1_000,
                    value: 1.0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let db = ing.get_tsdb(user).unwrap();
    db.db.compact_head_range(1_000, 2_000).unwrap().unwrap();
    db.db.reload_blocks().unwrap();

    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_strs("__name__", "a03_m"),
                    cortexpb::LabelPair::from_strs("job", "web"),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 50_000,
                    value: 2.0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    db.generate_head_statistics();

    let res = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 60_000,
                matchers: vec![proto_matcher(ingesterpb::MatchType::Equal, "__name__", "a03_m")],
                streaming_chunks_batch_size: 16,
            },
        )
        .await
        .unwrap();
    let total_series: usize = res.iter().map(|r| r.streaming_series.len()).sum();
    assert_eq!(total_series, 2);

    // 2 sources (Head + 1 Block) -> queried_blocks incremented ONCE per source (not twice!).
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "0")]),
        Some(1.0)
    );
    assert_eq!(
        metric_value(&reg, "cortex_ingester_queried_blocks_total", &[("generation", "1")]),
        Some(1.0)
    );
    // Secondary un-planned Select runs through statsTrackingChunkQuerier -> 2 observations per mirrored source = 4!
    assert_eq!(
        hist_stats(
            &reg,
            "cortex_ingester_queried_series",
            &[("stage", "single_block_index")]
        )
        .0,
        4
    );
    assert_eq!(
        hist_stats(
            &reg,
            "cortex_ingester_queried_series",
            &[("stage", "single_block_filter")]
        )
        .0,
        4
    );
    assert_eq!(
        hist_stats(
            &reg,
            "cortex_tsdb_index_lookup_planning_filtered_ratio",
            &[("user", user)]
        )
        .0,
        4
    );
    assert_eq!(
        hist_stats(
            &reg,
            "cortex_tsdb_index_lookup_planning_block_cardinality_accuracy_ratio",
            &[("user", user)]
        )
        .0,
        4
    );
    assert_eq!(
        hist_stats(
            &reg,
            "cortex_tsdb_index_lookup_planning_index_selection_accuracy_ratio",
            &[("user", user)]
        )
        .0,
        4
    );
    // Only the 2 primary selects invoke PlanIndexLookup (secondary disables planning).
    assert_eq!(
        hist_total_count(&reg, "cortex_tsdb_index_lookup_planning_duration_seconds"),
        2
    );

    ing.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a04_shared_cache_eligibility_uses_post_plan_index_matchers() {
    let dir = TempDir::new().unwrap();
    let (cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    blocks_cfg.tsdb.index_lookup_planning.index_lookup_planning_enabled = true;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .index_lookup_planning_comparison_portion = 0.0;
    blocks_cfg
        .tsdb
        .index_lookup_planning
        .min_series_per_block_for_query_planning = 1;
    blocks_cfg.tsdb.shared_postings_for_matchers_cache = true;
    blocks_cfg.tsdb.head_postings_for_matchers_cache_invalidation = true;
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

    let user = "user-a04";
    // Make __name__="a04_m" high-cardinality (2000 series) and job="rare" low-cardinality (2 series)
    // so CostBasedPlanner keeps job="rare" in index_matchers and defers __name__="a04_m" to scan_matchers.
    let mut series: Vec<cortexpb::TimeSeries> = (0..2000)
        .map(|i| cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", "a04_m"),
                cortexpb::LabelPair::from_strs("idx", i.to_string()),
                cortexpb::LabelPair::from_strs("job", if i == 0 { "rare" } else { "common" }),
            ],
            samples: vec![cortexpb::Sample {
                timestamp_ms: 10_000,
                value: i as f64,
            }],
            ..Default::default()
        })
        .collect();
    series.push(cortexpb::TimeSeries {
        labels: vec![
            cortexpb::LabelPair::from_strs("__name__", "a04_other"),
            cortexpb::LabelPair::from_strs("idx", "9999"),
            cortexpb::LabelPair::from_strs("job", "rare"),
        ],
        samples: vec![cortexpb::Sample {
            timestamp_ms: 10_000,
            value: 99.0,
        }],
        ..Default::default()
    });
    ing.push_grpc(
        user,
        &cortexpb::WriteRequest {
            source: cortexpb::write_request::SourceEnum::Api as i32,
            timeseries: series,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    ing.get_tsdb(user).unwrap().generate_head_statistics();

    let res = ing
        .handle_query_stream(
            &MetadataMap::new(),
            user,
            ingesterpb::QueryRequest {
                start_timestamp_ms: 0,
                end_timestamp_ms: 20_000,
                matchers: vec![
                    proto_matcher(ingesterpb::MatchType::Equal, "__name__", "a04_m"),
                    proto_matcher(ingesterpb::MatchType::Equal, "job", "rare"),
                ],
                streaming_chunks_batch_size: 16,
            },
        )
        .await
        .unwrap();
    let total_series: usize = res.iter().map(|r| r.streaming_series.len()).sum();
    assert_eq!(total_series, 1);

    // Because post-plan index_matchers = [job="rare"] lacks exact __name__, Head shared cache
    // was skipped -> single_block_index=0, filtered_ratio=0, intersection_size_ratio not observed!
    let (idx_cnt, idx_sum) = hist_stats(
        &reg,
        "cortex_ingester_queried_series",
        &[("stage", "single_block_index")],
    );
    assert_eq!(idx_cnt, 1);
    assert_eq!(idx_sum, 0.0);

    let (flt_cnt, flt_sum) = hist_stats(
        &reg,
        "cortex_ingester_queried_series",
        &[("stage", "single_block_filter")],
    );
    assert_eq!(flt_cnt, 1);
    assert_eq!(flt_sum, 1.0);

    let (fr_cnt, fr_sum) = hist_stats(
        &reg,
        "cortex_tsdb_index_lookup_planning_filtered_ratio",
        &[("user", user)],
    );
    assert_eq!(fr_cnt, 1);
    assert_eq!(fr_sum, 0.0);

    assert_eq!(
        hist_stats(
            &reg,
            "cortex_tsdb_index_lookup_planning_index_selection_accuracy_ratio",
            &[("user", user)]
        )
        .0,
        0
    );

    ing.stop().await.unwrap();
}

struct MockSeries(Labels);

impl ChunkSeries for MockSeries {
    fn labels(&self) -> Labels {
        self.0.clone()
    }
    fn chunks(&self) -> Vec<ChunkMeta> {
        Vec::new()
    }
}

struct MockSeriesSet {
    items: Vec<Labels>,
    idx: Option<usize>,
}

impl ChunkSeriesSet for MockSeriesSet {
    fn next(&mut self) -> bool {
        let next = match self.idx {
            None => 0,
            Some(i) => i + 1,
        };
        if next < self.items.len() {
            self.idx = Some(next);
            true
        } else {
            self.idx = Some(self.items.len());
            false
        }
    }
    fn at(&mut self) -> Option<Arc<dyn ChunkSeries>> {
        self.idx
            .and_then(|i| self.items.get(i))
            .map(|l| Arc::new(MockSeries(l.clone())) as Arc<dyn ChunkSeries>)
    }
    fn err(&self) -> Option<QuerierError> {
        None
    }
}

struct MockChunkQuerier {
    items: Vec<Labels>,
}

impl ChunkQuerier for MockChunkQuerier {
    fn select(
        &mut self,
        _sort_series: bool,
        _hints: Option<&mimir_ingester::lookupplan::SelectHints>,
        _matchers: &[LabelMatcher],
    ) -> Box<dyn ChunkSeriesSet> {
        Box::new(MockSeriesSet {
            items: self.items.clone(),
            idx: None,
        })
    }
    fn label_values(
        &mut self,
        _name: &str,
        _matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        Ok((Vec::new(), Vec::new()))
    }
    fn label_names(
        &mut self,
        _matchers: &[LabelMatcher],
    ) -> Result<(Vec<String>, Vec<String>), QuerierError> {
        Ok((Vec::new(), Vec::new()))
    }
    fn close(&mut self) -> Result<(), QuerierError> {
        Ok(())
    }
}

#[test]
fn a05_shared_retaining_series_set_incremental_push_and_idempotent_at() {
    let items: Vec<Labels> = (0..500)
        .map(|i| Labels::from_pairs([("__name__", "m_a05"), ("id", &format!("{i:04}"))]))
        .collect();
    let outcomes = CounterVec::new(
        Opts::new("test_outcomes", "test"),
        &["outcome", "user"],
    )
    .unwrap();
    let mut mq = MirroredChunkQuerier::new_with_meta(
        "user-a05",
        outcomes.clone(),
        0,
        10_000,
        ChunkBlockMeta::default(),
        Box::new(MockChunkQuerier {
            items: items.clone(),
        }),
    );

    let mut ss = mq.select(true, None, &[]);
    let mut seen = BTreeSet::new();
    while ss.next() {
        // Call at() twice on the same series to verify no duplicate push into labels_sink.
        let s1 = ss.at().unwrap();
        let s2 = ss.at().unwrap();
        assert_eq!(s1.labels(), s2.labels());
        seen.insert(s1.labels().get("id").to_string());
    }
    assert!(ss.err().is_none());
    assert_eq!(ss.labels().len(), 500);
    assert_eq!(seen.len(), 500);
    drop(ss);

    mq.close().unwrap();
    assert_eq!(outcomes.with_label_values(&["success", "user-a05"]).get() as u64, 1);
    assert_eq!(
        outcomes
            .with_label_values(&["extra_series", "user-a05"])
            .get() as u64,
        0
    );
    assert_eq!(
        outcomes
            .with_label_values(&["missing_series", "user-a05"])
            .get() as u64,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a06_activity_tracker_all_user_stats_and_numeric_unknown_enums() {
    // 1. Unknown CountMethod and MatchType format as decimal integers (not "UNKNOWN").
    let lvc = request_activity(
        "u1",
        "t1",
        "Ingester/LabelValuesCardinality",
        ActivityRequest::LabelValuesCardinality(&ingesterpb::LabelValuesCardinalityRequest {
            label_names: vec!["job".to_string()],
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: 77,
                name: "env".to_string(),
                value: "prod".to_string(),
            }],
            count_method: 99,
        }),
    );
    assert_eq!(
        lvc,
        r#"Ingester/LabelValuesCardinality: user="u1" trace="t1" request=&LabelValuesCardinalityRequest{LabelNames:[job],Matchers:[]*LabelMatcher{&LabelMatcher{Type:77,Name:env,Value:prod,},},CountMethod:99,}"#
    );

    let mut lm_buf = String::new();
    label_matcher_to_string(
        &mut lm_buf,
        Some(&ingesterpb::LabelMatcher {
            r#type: -5,
            name: "a".to_string(),
            value: "b".to_string(),
        }),
    );
    assert_eq!(lm_buf, "&LabelMatcher{Type:-5,Name:a,Value:b,}");

    // 2. AllUserStats records tenant ID from x-scope-orgid (multitenancy=true) and
    //    no_auth_tenant (multitenancy=false) instead of hardcoded "".
    for (multitenancy, header_tenant, expected_user) in [
        (true, Some("tenant-a42"), "tenant-a42"),
        (false, None, "anonymous"),
    ] {
        let dir = TempDir::new().unwrap();
        let act_path = dir.path().join("activity.log");
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
        let (mut cfg, mut blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
        cfg.ring.instance_id = format!("act-{multitenancy}");
        blocks_cfg.tsdb.dir = dir.path().to_string_lossy().to_string();
        let (ing, _, _) = prepare_ingester_with_blocks_storage(
            dir.path(),
            cfg,
            blocks_cfg,
            ingest_cfg,
            default_limits_test_config(),
            None,
        );
        ing.set_multitenancy(multitenancy, "anonymous");
        ing.set_activity_tracker(Some(Arc::clone(&tracker)));
        start_and_wait_healthy(&ing).await;

        let mut builder = http::Request::builder()
            .method("POST")
            .uri("/cortex.Ingester/AllUserStats")
            .header("content-type", "application/grpc")
            .header("te", "trailers");
        if let Some(t) = header_tenant {
            builder = builder.header("x-scope-orgid", t);
        }
        let http_req = builder
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(grpc_frame(
                &ingesterpb::UserStatsRequest::default(),
            )))))
            .unwrap();
        let svc = IngesterService::new(Arc::clone(&ing)).into_server();
        let resp = svc.oneshot(http_req).await.unwrap();
        let (_, status, _) = collect_grpc_http_resp(resp).await;
        assert!(status.is_none_or(|s| s.code() == tonic::Code::Ok));
        assert!(load_unfinished_entries(&act_path).unwrap().is_empty());
        let formatted = request_activity(
            expected_user,
            "",
            "Ingester/AllUserStats",
            ActivityRequest::UserStats(&ingesterpb::UserStatsRequest::default()),
        );
        assert_eq!(
            formatted,
            format!(
                r#"Ingester/AllUserStats: user={expected_user:?} trace="" request=&UserStatsRequest{{CountMethod:IN_MEMORY,}}"#
            )
        );

        ing.stop().await.unwrap();
    }
}
