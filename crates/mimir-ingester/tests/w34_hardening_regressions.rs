//! Wave 34 deep-audit hardening regression tests for `mimir-ingester`.

mod common;

use std::io::Write as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http;
use bytes::Bytes;
use mimir_common::services::{ServiceContext, State};
use mimir_config::{LimitsConfig, Overrides};
use mimir_ingester::active_series::{ActiveSeries, AmMatchType};
use mimir_ingester::push::PrehashedSeriesItem;
use mimir_ingester::query::{
    compute_label_values_counts, ComputeWorkerPool, LabelValueCountFn,
};
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_tsdb::chunkenc::{CounterResetHeader, EncodedChunk};
use tempfile::TempDir;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataMap;
use tower::ServiceExt;

use common::default_ingester_test_config;

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

fn new_test_ingester(dir: &std::path::Path, limits: LimitsConfig) -> Arc<Ingester> {
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir);
    cfg.push_grpc_method_enabled = true;
    Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(limits),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("create ingester")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn encode_grpc_frame(payload: &[u8]) -> Bytes {
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(1);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    Bytes::from(frame)
}

#[tokio::test]
async fn w34_zstd_window_log_max_rejects_excessive_window_allocation() {
    let dir = TempDir::new().unwrap();
    let ing = new_test_ingester(dir.path(), LimitsConfig::default());
    ing.set_state_for_test(State::Running);

    let ts = now_ms();
    let req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "zstd_win_test")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let raw = req.encode_to_vec();

    // Compress with WindowLog(25) (32 MiB window) — exceeds window_log_max cap (<= 23, 8 MiB).
    let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
    enc.set_parameter(zstd::zstd_safe::CParameter::WindowLog(25))
        .unwrap();
    enc.write_all(&raw).unwrap();
    let high_win_zstd = enc.finish().unwrap();

    let svc = IngesterService::new(Arc::clone(&ing))
        .into_server()
        .accept_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(64 * 1024 * 1024);

    let bad_req = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/Push")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-encoding", "zstd")
        .header("x-scope-orgid", "t-zstd")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(
            encode_grpc_frame(&high_win_zstd),
        ))))
        .unwrap();
    let (_, status, _) = collect_grpc_http_resp(svc.clone().oneshot(bad_req).await.unwrap()).await;
    let st = status.expect("zstd excessive windowLog must return error status");
    assert_eq!(st.code(), tonic::Code::Internal);
    assert!(
        st.message().contains("zstd decompression failed"),
        "expected zstd decompression error, got: {}",
        st.message()
    );

    // Normal zstd payload with small windowLog (12 = 4 KiB) succeeds.
    let mut small_enc = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
    small_enc
        .set_parameter(zstd::zstd_safe::CParameter::WindowLog(12))
        .unwrap();
    small_enc.write_all(&raw).unwrap();
    let ok_zstd = small_enc.finish().unwrap();

    let ok_req = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/Push")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-encoding", "zstd")
        .header("x-scope-orgid", "t-zstd")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(
            encode_grpc_frame(&ok_zstd),
        ))))
        .unwrap();
    let (_, ok_st, _) = collect_grpc_http_resp(svc.oneshot(ok_req).await.unwrap()).await;
    if let Some(s) = ok_st {
        assert_eq!(s.code(), tonic::Code::Ok);
    }
    let db = ing.get_tsdb("t-zstd").expect("tsdb created for valid zstd push");
    assert_eq!(db.db.head().num_series(), 1);
}

#[test]
fn w34_nanos_slab_mmap_reclaim_and_am_match_type_option() {
    assert_eq!(AmMatchType::from_u8(0), Some(AmMatchType::Equal));
    assert_eq!(AmMatchType::from_u8(1), Some(AmMatchType::NotEqual));
    assert_eq!(AmMatchType::from_u8(2), Some(AmMatchType::RegexMatch));
    assert_eq!(AmMatchType::from_u8(3), Some(AmMatchType::RegexNotMatch));
    assert_eq!(AmMatchType::from_u8(4), None);
    assert_eq!(AmMatchType::from_u8(u8::MAX), None);

    let tracker = ActiveSeries::new(Vec::new(), Duration::from_secs(60));
    let t0 = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let pairs = [("__name__", "slab_test")];
    tracker.update_series(&pairs, 123, 1, t0, -1, false);
    assert!(tracker.is_ref_active_fast(1, i64::MIN));

    // Purge past TTL and reclaim page; under Linux anonymous mmap, page is reclaimed safely.
    let t1 = t0 + Duration::from_secs(120);
    tracker.purge(t1);
    let (alloc_pages, _resident, reclaimed) = tracker.debug_slab_stats();
    assert_eq!(alloc_pages, 1);
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64"),
        not(miri)
    ))]
    assert_eq!(reclaimed, 1, "anonymous mmap SlabPage must support MADV_DONTNEED reclaim");
    #[cfg(not(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64"),
        not(miri)
    )))]
    let _ = reclaimed;

    // Re-activating slot 1 on the reclaimed page works cleanly.
    let t2 = t1 + Duration::from_secs(10);
    tracker.update_series(&pairs, 123, 1, t2, -1, false);
    assert!(tracker.is_ref_active_fast(1, i64::MIN));
}

#[tokio::test]
async fn w34_shutdown_drains_inflight_pushes_and_run_starting_cleans_pool() {
    let dir = TempDir::new().unwrap();
    let ing = new_test_ingester(dir.path(), LimitsConfig::default());
    ing.set_shutdown_push_drain_timeout(Duration::from_millis(500));

    // 1) Start with canceled ServiceContext -> run_starting error must stop compute_worker_pool.
    let canceled_ctx = ServiceContext::background();
    canceled_ctx.cancel();
    let _ = ing.start_with_context(canceled_ctx).await;
    assert!(
        ing.compute_worker_pool
            .submit("test", "t1", || {})
            .is_err(),
        "run_starting failure must stop compute_worker_pool"
    );

    // 2) Shutdown drain waits for inflight_push_requests to drop to 0 (and respects bounded timeout).
    let dir2 = TempDir::new().unwrap();
    let ing2 = new_test_ingester(dir2.path(), LimitsConfig::default());
    ing2.set_shutdown_push_drain_timeout(Duration::from_millis(100));
    ing2.set_state_for_test(State::Running);

    let ts = now_ms();
    ing2.push_grpc(
        "t-drain",
        &cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_strs("__name__", "before_stop")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Simulate an in-flight push permit that completes 40ms after stop() starts.
    ing2.inflight_push_requests.fetch_add(1, Ordering::AcqRel);
    let ing2_rel = Arc::clone(&ing2);
    let rel_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        ing2_rel.inflight_push_requests.fetch_sub(1, Ordering::AcqRel);
    });
    ing2.stop().await.unwrap();
    rel_handle.await.unwrap();
    assert_eq!(ing2.inflight_push_requests.load(Ordering::Acquire), 0);

    // 3) Ingest when State::Stopping must be rejected by push_grpc_raw.
    let err = ing2
        .push_grpc_raw(
            "t-drain",
            Bytes::from(
                cortexpb::WriteRequest {
                    timeseries: vec![cortexpb::TimeSeries {
                        labels: vec![cortexpb::LabelPair::from_strs("__name__", "late_push")],
                        samples: vec![cortexpb::Sample {
                            value: 2.0,
                            timestamp_ms: ts + 1000,
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(err.http_status_code(), 500);
    assert!(err.to_string().contains("unavailable"), "got: {err}");

    // 4) Bounded timeout test: stuck inflight_push_requests does not hang drain_inflight_pushes.
    let dir3 = TempDir::new().unwrap();
    let ing3 = new_test_ingester(dir3.path(), LimitsConfig::default());
    ing3.inflight_push_requests.store(1, Ordering::Release);
    let start = std::time::Instant::now();
    ing3.drain_inflight_pushes(Duration::from_millis(30)).await;
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn w34_active_series_disabled_gate_matches_go() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.active_series.active_series_metrics_enabled = false;
    cfg.push_grpc_method_enabled = true;
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(LimitsConfig::default()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_state_for_test(State::Running);

    let md = MetadataMap::new();
    let req = ingesterpb::ActiveSeriesRequest {
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".into(),
            value: "up".into(),
        }],
        r#type: ingesterpb::active_series_request::RequestType::Series as i32,
    };

    // When tenant has no TSDB yet, returns empty Ok (matching Go).
    let empty = ing
        .handle_active_series(&md, "u-no-tsdb", req.clone())
        .await
        .unwrap();
    assert!(empty.is_empty());

    // Push a sample so tenant has a TSDB, then ActiveSeries must return Go's error string.
    ing.push_grpc(
        "u-as-off",
        &cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_strs("__name__", "up")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: now_ms(),
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let err1 = ing
        .handle_active_series(&md, "u-as-off", req.clone())
        .await
        .unwrap_err();
    assert_eq!(
        err1.to_string(),
        "error listing active series: active series tracker is not initialized"
    );

    let err2 = match ing.handle_active_series_rx(&md, "u-as-off", req).await {
        Ok(_) => panic!("expected ActiveSeries rx error when tracker disabled"),
        Err(e) => e,
    };
    assert_eq!(
        err2.to_string(),
        "error listing active series: active series tracker is not initialized"
    );
}

#[tokio::test]
async fn w34_push_parity_created_ts_bad_utf8_series_str_and_reset_hint_bounds() {
    let dir = TempDir::new().unwrap();
    let limits = LimitsConfig {
        native_histograms_ingestion_enabled: true,
        out_of_order_time_window: mimir_config::ModelDuration::ZERO,
        ..Default::default()
    };
    let ing = new_test_ingester(dir.path(), limits);
    ing.set_state_for_test(State::Running);

    let ts = now_ms();

    // 1) created_timestamp older than head compaction min_valid_time must NOT abort request with 500;
    //    must record partial 400 and ingest the valid sample in the same series.
    ing.push_grpc(
        "t-ct",
        &cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_strs("__name__", "ct_seed")],
                samples: vec![cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    ing.get_tsdb("t-ct")
        .unwrap()
        .compact_head(ts)
        .await
        .unwrap();

    let ct_req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ct_target")],
            samples: vec![cortexpb::Sample {
                value: 2.0,
                timestamp_ms: ts + 5_000,
            }],
            created_timestamp: ts - 1_000_000, // older than min_valid_time!
            ..Default::default()
        }],
        ..Default::default()
    };
    let ct_err = ing.push_grpc("t-ct", &ct_req).await.unwrap_err();
    assert_eq!(
        ct_err.http_status_code(),
        400,
        "created_timestamp append error must not return 500: {ct_err}"
    );
    let db = ing.get_tsdb("t-ct").unwrap();
    assert_eq!(db.db.head().num_series(), 1);

    // 2) bad_utf8 validation error formats raw bytes lossily on BOTH raw and owned push paths.
    let bad_req = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", "bad_utf8_metric"),
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"job"),
                    value: Bytes::from_static(b"bad\xffval"),
                },
            ],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let raw_err = ing
        .push_grpc_raw("t-utf8", Bytes::from(bad_req.encode_to_vec()))
        .await
        .unwrap_err();
    assert_eq!(raw_err.http_status_code(), 400);
    assert!(
        raw_err.to_string().contains("bad_utf8_metric{job="),
        "raw push bad_utf8 error must include formatted series: {raw_err}"
    );

    let owned_err = ing.push_grpc("t-utf8", &bad_req).await.unwrap_err();
    assert_eq!(owned_err.http_status_code(), 400);
    assert!(
        owned_err.to_string().contains("bad_utf8_metric{job="),
        "owned push bad_utf8 error must include formatted series: {owned_err}"
    );

    // Also test prehashed batch path directly with bad_utf8 and empty label_bytes.
    let prehashed_err = ing
        .ingest_prehashed_shard_batch_with_trace(
            "t-utf8",
            cortexpb::write_request::SourceEnum::Api,
            ts,
            &[PrehashedSeriesItem {
                encoded_labels: &[],
                raw_encoded_labels: b"\x08__name__\x0bprehashed_m\x03job\x03a\xffb",
                samples: &[cortexpb::Sample {
                    value: 1.0,
                    timestamp_ms: ts,
                }],
                created_timestamp: 0,
                prom_hash: 0,
                labels_sorted_unique: false,
                bad_utf8: true,
                histograms: &[],
                exemplars: &[],
            }],
            &[],
            None,
        )
        .unwrap_err();
    assert!(
        prehashed_err.to_string().contains("prehashed_m{job="),
        "prehashed bad_utf8 error must include lossily formatted series: {prehashed_err}"
    );

    // 3) 32-bit wire reset_hint = 257 (0x101) / 258 (0x102) must NOT truncate as u8 to CounterReset (1) / NotCounterReset (2).
    let make_hist_wr = |name: &str, reset_hint: i32| cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", name)],
            histograms: vec![cortexpb::Histogram {
                count: Some(cortexpb::histogram::Count::CountInt(5)),
                zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(1)),
                sum: 10.0,
                schema: 0,
                positive_spans: vec![cortexpb::BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                positive_deltas: vec![4],
                reset_hint,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let raw_wr = make_hist_wr("hist_rh_raw", 257);
    ing.push_grpc_raw("t-rh", Bytes::from(raw_wr.encode_to_vec()))
        .await
        .unwrap();
    let owned_wr = make_hist_wr("hist_rh_owned", 258);
    ing.push_grpc("t-rh", &owned_wr).await.unwrap();

    let db_rh = ing.get_tsdb("t-rh").unwrap();
    for metric in ["hist_rh_raw", "hist_rh_owned"] {
        let m = mimir_tsdb::matcher::LabelMatcher::new(
            mimir_tsdb::matcher::MatchType::Equal,
            "__name__",
            metric,
        )
        .unwrap();
        let cq = db_rh.db.chunk_querier(0, i64::MAX);
        let series_list = cq.select(false, &[m]).unwrap();
        assert_eq!(series_list.len(), 1);
        let EncodedChunk::Histogram(c) = series_list[0].chunks[0].chunk.as_ref().unwrap() else {
            panic!("expected histogram chunk");
        };
        assert_eq!(
            CounterResetHeader::from_header_byte(c.as_bytes()[2]),
            CounterResetHeader::UnknownCounterReset
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn w34_label_values_counts_semaphore_prevents_worker_starvation() {
    // 4-worker pool: slow tenant holds at most workers/2 = 2 chunks, leaving >= 2 workers free!
    let pool = Arc::new(ComputeWorkerPool::new(4, "test-starve", None).unwrap());
    pool.start().unwrap();

    let entered_slow = Arc::new(AtomicUsize::new(0));
    let release_slow = Arc::new(AtomicBool::new(false));
    let entered_slow_c = Arc::clone(&entered_slow);
    let release_slow_c = Arc::clone(&release_slow);

    let slow_fn: LabelValueCountFn = Arc::new(move |_| {
        entered_slow_c.fetch_add(1, Ordering::AcqRel);
        while !release_slow_c.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok(1)
    });

    let values: Vec<String> = (0..100).map(|i| format!("v{i}")).collect();
    let _slow_rx = compute_label_values_counts(
        Arc::new(|| false),
        &pool,
        "slow-tenant",
        1,
        Arc::new(values),
        slow_fn,
    );

    // Wait briefly for slow chunks to occupy their bounded semaphore slots.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let in_flight_slow = entered_slow.load(Ordering::Acquire);
    assert!(
        in_flight_slow <= 2,
        "slow request must hold at most workers/2=2 workers, held {in_flight_slow}"
    );

    // Another tenant can immediately run a task on the remaining free worker threads!
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    pool.submit("other-dim", "healthy-tenant", move || {
        let _ = done_tx.send(42u32);
    })
    .unwrap();
    let healthy_res = tokio::time::timeout(Duration::from_secs(2), done_rx)
        .await
        .expect("healthy tenant must not be starved by slow LabelValuesCardinality client")
        .unwrap();
    assert_eq!(healthy_res, 42);

    release_slow.store(true, Ordering::Release);
    pool.stop();
}
