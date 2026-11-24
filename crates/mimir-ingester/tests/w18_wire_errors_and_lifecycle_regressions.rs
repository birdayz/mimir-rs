mod common;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http;
use bytes::Bytes;
use mimir_common::grpc_log::GrpcErrorLog;
use mimir_common::services::State;
use mimir_common::shutdownmarker;
use mimir_config::{CircuitBreakerConfig, GoDuration, LimitsConfig, Overrides};
use mimir_ingester::circuit_breaker::{BreakerState, CircuitBreaker, IngesterCircuitBreaker};
use mimir_ingester::errors::{format_labels_string, IngesterError};
use mimir_ingester::ingest_storage::ingester_err_to_pusher_err;
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::cortexpb::{self, ErrorCause};
use mimir_proto::grpcutilpb::extract_cortex_error_details;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_proto::zerocopy::{
    decode_rw2_stream, DecodeOptions, Rw2ResolvedScratch, WriteRequestScratch,
};
use prometheus::Registry;
use tempfile::TempDir;
use tonic::codec::CompressionEncoding;
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

fn new_test_ingester(dir: &std::path::Path, limits: LimitsConfig, push_enabled: bool) -> Arc<Ingester> {
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir);
    cfg.push_grpc_method_enabled = push_enabled;
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

fn unframed(payload: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Bytes::from(out)
}

// ============================================================================
// Item 1 (W18-SR-01): Ingester::stop() on State::New transitions directly to
// State::Terminated without running stop_inner() or removing shutdown marker.
// ============================================================================
#[tokio::test]
async fn w18_sr_01_stop_on_new_ingester_preserves_shutdown_marker() {
    let dir = TempDir::new().unwrap();
    let (cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    let marker_path = shutdownmarker::get_path(&blocks_cfg.tsdb.dir);
    shutdownmarker::create(&marker_path).expect("create shutdown marker");
    assert!(shutdownmarker::exists(&marker_path).unwrap());

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
    assert_eq!(ing.state(), State::New);

    ing.stop().await.expect("stop on State::New must succeed");
    assert_eq!(ing.state(), State::Terminated);
    assert!(
        shutdownmarker::exists(&marker_path).unwrap(),
        "stopping a never-started New ingester must preserve shutdown-requested.txt"
    );

    // Subsequent stop() on Terminated is a clean no-op and start() cannot revive it.
    ing.stop().await.expect("idempotent stop");
    assert!(ing.start().await.is_err());
    assert!(shutdownmarker::exists(&marker_path).unwrap());
}

// ============================================================================
// Item 2 (W18-SR-03 & W18-SR-07): CircuitBreaker edge configs & HalfOpen parity
// ============================================================================
#[test]
fn w18_sr_03_circuit_breaker_edge_configs_match_failsafe_go() {
    // 1) failure_threshold_percentage > 100 rejected at construction
    let bad_cfg = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 150,
        failure_execution_threshold: 5,
        thresholding_period: GoDuration::from_secs(60),
        cooldown_period: GoDuration::from_secs(10),
        request_timeout: GoDuration::from_secs(10),
        ..Default::default()
    };
    assert!(
        CircuitBreaker::new(&bad_cfg, None, "push", false).is_err(),
        "failure_threshold_percentage=150 must fail construction"
    );
    assert!(
        IngesterCircuitBreaker::new(&bad_cfg, &CircuitBreakerConfig::default(), None).is_err()
    );

    // 2) failure_threshold_percentage == 0: must NOT open on 0 failures; opens only when
    // execution_count >= exec_threshold && failure_count >= 1
    let zero_pct_cfg = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 0,
        failure_execution_threshold: 5,
        thresholding_period: GoDuration::from_secs(60),
        cooldown_period: GoDuration::from_secs(10),
        request_timeout: GoDuration::from_secs(10),
        ..Default::default()
    };
    let cb0 = CircuitBreaker::new(&zero_pct_cfg, None, "push", false)
        .unwrap()
        .unwrap();
    cb0.activate();
    for _ in 0..20 {
        cb0.try_acquire_permit()
            .unwrap()
            .unwrap()
            .finish(Duration::from_millis(1), false);
    }
    assert_eq!(
        cb0.state(),
        BreakerState::Closed,
        "0% threshold with 0 failures must stay Closed"
    );
    // 1 failure with execution_count >= 5 -> Open
    cb0.try_acquire_permit()
        .unwrap()
        .unwrap()
        .finish(Duration::from_secs(1), true);
    assert_eq!(cb0.state(), BreakerState::Open);

    // 3) thresholding_period == 0: uses ClosedStats::Counting(exec_threshold.max(1))
    let zero_period_cfg = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 50,
        failure_execution_threshold: 4,
        thresholding_period: GoDuration(0),
        cooldown_period: GoDuration::from_secs(10),
        request_timeout: GoDuration::from_secs(10),
        ..Default::default()
    };
    let cb_cnt = CircuitBreaker::new(&zero_period_cfg, None, "push", false)
        .unwrap()
        .unwrap();
    cb_cnt.activate();
    // Spread executions across >1ms so a 1ns timed window would have expired them
    for i in 0..4 {
        std::thread::sleep(Duration::from_millis(2));
        cb_cnt
            .try_acquire_permit()
            .unwrap()
            .unwrap()
            .finish(Duration::from_secs(1), i < 2);
    }
    assert_eq!(
        cb_cnt.state(),
        BreakerState::Open,
        "thresholding_period=0 must use counting stats over exec_threshold"
    );
}

#[test]
fn w18_sr_07_state_does_not_transition_and_closed_completions_replenish_half_open() {
    let cfg = CircuitBreakerConfig {
        enabled: true,
        failure_threshold_percentage: 50,
        failure_execution_threshold: 2,
        thresholding_period: GoDuration::from_secs(60),
        cooldown_period: GoDuration::from_millis(15),
        request_timeout: GoDuration::from_secs(10),
        ..Default::default()
    };
    let reg = Registry::new();
    let cb = CircuitBreaker::new(&cfg, Some(&reg), "push", false)
        .unwrap()
        .unwrap();
    cb.activate();

    // Hold 2 permits acquired while Closed
    let closed_p1 = cb.try_acquire_permit().unwrap().unwrap();
    let closed_p2 = cb.try_acquire_permit().unwrap().unwrap();

    // Trip breaker to Open via 2 failures
    cb.try_acquire_permit()
        .unwrap()
        .unwrap()
        .finish(Duration::from_secs(1), true);
    cb.try_acquire_permit()
        .unwrap()
        .unwrap()
        .finish(Duration::from_secs(1), true);
    assert_eq!(cb.state(), BreakerState::Open);

    // Wait past cooldown: state() & is_open() MUST NOT flip Open -> HalfOpen
    std::thread::sleep(Duration::from_millis(25));
    assert_eq!(cb.state(), BreakerState::Open);
    assert!(cb.is_open());

    // Push + Read pair: try_acquire_read_permit calls push.is_open() and must still reject
    let pr = IngesterCircuitBreaker {
        push: Some(Arc::clone(&cb)),
        read: CircuitBreaker::new(&cfg, None, "read", false).unwrap(),
    };
    pr.read.as_ref().unwrap().activate();
    assert!(matches!(
        pr.try_acquire_read_permit(),
        Err(IngesterError::CircuitBreakerOpen { .. })
    ));
    assert_eq!(cb.state(), BreakerState::Open);

    // Now try_acquire_permit() transitions Open -> HalfOpen (capacity = 2, 1 taken -> 1 left)
    let ho_p1 = cb.try_acquire_permit().unwrap().unwrap();
    assert_eq!(cb.state(), BreakerState::HalfOpen);
    let ho_p2 = cb.try_acquire_permit().unwrap().unwrap();
    assert!(cb.try_acquire_permit().is_err());

    // A permit acquired while Closed finishing while HalfOpen replenishes +1 permitted_executions.
    closed_p1.finish(Duration::from_secs(1), true);
    assert_eq!(cb.state(), BreakerState::HalfOpen);
    let ho_p3 = cb.try_acquire_permit().unwrap().unwrap();
    assert!(cb.try_acquire_permit().is_err());

    // Dropping an aborted HalfOpen permit also replenishes +1
    drop(ho_p3);
    let ho_p4 = cb.try_acquire_permit().unwrap().unwrap();
    assert!(cb.try_acquire_permit().is_err());

    drop(ho_p1);
    drop(ho_p2);
    drop(ho_p4);
    closed_p2.finish(Duration::from_millis(1), false);
}

// ============================================================================
// Item 3 (W18-SR-06 ingester side): notify_pre_commit fsyncs ALL tenants
// ============================================================================
#[test]
fn w18_sr_06_notify_pre_commit_fsyncs_all_tenants_and_aggregates_errors() {
    let dir = TempDir::new().unwrap();
    for concurrency in [1i64, 4i64] {
        let (cfg, blocks_cfg, mut ingest_cfg) = default_ingester_test_config(dir.path());
        ingest_cfg.write_logs_fsync_before_kafka_commit_concurrency = concurrency;
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

        let ts = now_ms();
        for uid in ["t-a", "t-b", "t-c"] {
            let wr = cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair::from_strs("__name__", "up")],
                    samples: vec![cortexpb::Sample {
                        value: 1.0,
                        timestamp_ms: ts,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            ing.push_write_request_sync(uid, &wr).unwrap();
        }

        let synced = parking_lot::Mutex::new(Vec::<String>::new());
        let err = ing
            .notify_pre_commit_with(|uid, _db| {
                synced.lock().push(uid.to_string());
                if uid == "t-a" || uid == "t-c" {
                    Err(format!("io error on {uid}"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        let mut got_synced = synced.into_inner();
        got_synced.sort();
        assert_eq!(
            got_synced,
            vec!["t-a", "t-b", "t-c"],
            "all tenants must be fsynced even when earlier ones fail (concurrency={concurrency})"
        );
        let msg = err.to_string();
        assert!(
            msg.starts_with("2 errors: "),
            "expected multierror prefix, got: {msg}"
        );
        assert!(msg.contains("t-a") && msg.contains("t-c"), "{msg}");
    }
}

// ============================================================================
// Item 4 (W18-WPE-01): http_status_code table matches Go errorCauseToHTTPStatusCode
// ============================================================================
#[test]
fn w18_wpe_01_http_status_code_table_matches_go_1_to_1() {
    assert_eq!(
        IngesterError::BadRequest {
            message: "x".into()
        }
        .http_status_code(),
        400
    );
    assert_eq!(
        IngesterError::per_user_series_limit_reached(1).http_status_code(),
        400
    );
    assert_eq!(
        IngesterError::ReactiveLimiterExceeded {
            message: "x".into()
        }
        .http_status_code(),
        429
    );
    assert_eq!(IngesterError::PushGrpcDisabled.http_status_code(), 501);
    assert_eq!(
        IngesterError::TsdbUnavailable {
            message: "closing".into()
        }
        .http_status_code(),
        503
    );
    assert_eq!(
        IngesterError::CircuitBreakerOpen {
            request_type: "push".into(),
            remaining_delay: Duration::from_secs(1)
        }
        .http_status_code(),
        503
    );
    assert_eq!(
        IngesterError::max_inflight_requests_reached().http_status_code(),
        503
    );
    assert_eq!(
        IngesterError::Internal {
            message: "x".into()
        }
        .http_status_code(),
        500
    );
    assert_eq!(
        IngesterError::Unavailable {
            state: State::Stopping
        }
        .http_status_code(),
        500
    );
    assert_eq!(IngesterError::TooBusy.http_status_code(), 500);
    assert!(
        !cortexpb::is_client_error(&IngesterError::TooBusy.to_httpgrpc_status()),
        "TooBusy must NOT be classified as a 4xx client error"
    );
}

// ============================================================================
// Item 5 (W18-WPE-02): Exemplar invalid UTF-8 preserves ErrorDetails(Soft=true),
// series invalid UTF-8 has no dangling " This is for series ".
// ============================================================================
#[tokio::test]
async fn w18_wpe_02_invalid_utf8_exemplar_preserves_details_and_series_no_dangling_suffix() {
    let dir = TempDir::new().unwrap();
    let limits = LimitsConfig {
        max_global_exemplars_per_user: 10,
        ..Default::default()
    };
    let ing = new_test_ingester(dir.path(), limits, true);
    ing.set_state_for_test(State::Running);

    let ts = now_ms();
    // 1) Exemplar invalid UTF-8 on push_grpc_raw AND push_write_request_sync
    let req_ex = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_utf8")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![cortexpb::LabelPair {
                    name: Bytes::from_static(b"trace_id"),
                    value: Bytes::from_static(&[0xff, 0xfe]),
                }],
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let err_raw = ing
        .push_grpc_raw("u-ex", Bytes::from(req_ex.encode_to_vec()))
        .await
        .unwrap_err();
    let status = err_raw.map_push_error_to_grpc_server_status();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let details = extract_cortex_error_details(&status)
        .expect("exemplar invalid UTF-8 must include cortexpb.ErrorDetails");
    assert_eq!(details.cause, ErrorCause::BadData as i32);
    assert!(details.soft, "exemplar invalid UTF-8 must be Soft=true");

    let err_owned = ing.push_write_request_sync("u-ex-2", &req_ex).unwrap_err();
    let status_owned = err_owned.map_push_error_to_grpc_server_status();
    let details_owned = extract_cortex_error_details(&status_owned)
        .expect("owned path exemplar invalid UTF-8 must include cortexpb.ErrorDetails");
    assert_eq!(details_owned.cause, ErrorCause::BadData as i32);
    assert!(details_owned.soft);

    // 2) Series invalid UTF-8 must NOT have dangling " This is for series "
    let req_series = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair {
                name: Bytes::from_static(b"__name__"),
                value: Bytes::from_static(b"\xffbad"),
            }],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let err_s = ing
        .push_grpc_raw("u-ser", Bytes::from(req_series.encode_to_vec()))
        .await
        .unwrap_err();
    let s_msg = err_s.to_string();
    assert!(
        !s_msg.ends_with(" This is for series "),
        "dangling suffix in series invalid UTF-8 error: {s_msg:?}"
    );
}

// ============================================================================
// Item 6 (W18-WPE-03): format_labels_string "{}" + Go strconv.Quote escaping
// ============================================================================
#[test]
fn w18_wpe_03_format_labels_string_empty_and_go_strconv_quote() {
    assert_eq!(format_labels_string(std::iter::empty()), "{}");
    assert_eq!(format_labels_string([("__name__", "")]), "{}");
    assert_eq!(
        format_labels_string([
            ("__name__", "m"),
            ("k", "a\x07b\x08c\x0cd\ne\rf\tg\x0bh\x00i\x7fj\u{0085}k\u{00ad}l"),
        ]),
        r#"m{k="a\ab\bc\fd\ne\rf\tg\vh\x00i\x7fj\u0085k\u00adl"}"#
    );
}

// ============================================================================
// Item 7 (W18-WPE-04): Empty-value labels preserved in RW2 unsorted re-emit
// and soft push error strings across classic, owned, and RW2 paths.
// ============================================================================
#[tokio::test]
async fn w18_wpe_04_empty_value_labels_preserved_in_soft_push_errors_and_rw2() {
    let dir = TempDir::new().unwrap();
    let ing = new_test_ingester(dir.path(), LimitsConfig::default(), true);
    ing.set_state_for_test(State::Running);

    let future_ts = now_ms() + 3_600_000;
    let wr_sorted_empty = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", "up"),
                cortexpb::LabelPair::from_strs("empty", ""),
                cortexpb::LabelPair::from_strs("job", "api"),
            ],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: future_ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Classic raw push
    let err_raw = ing
        .push_grpc_raw("u-e1", Bytes::from(wr_sorted_empty.encode_to_vec()))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err_raw.contains(r#"up{empty="", job="api"}"#),
        "classic raw push must keep empty=\"\" in error string: {err_raw}"
    );

    // Owned / streaming push_write_request_sync
    let err_owned = ing
        .push_write_request_sync("u-e2", &wr_sorted_empty)
        .unwrap_err()
        .to_string();
    assert!(
        err_owned.contains(r#"up{empty="", job="api"}"#),
        "owned push must keep empty=\"\" in error string: {err_owned}"
    );

    // RW2 unsorted with empty value via decode_rw2_stream -> PrehashedSeriesItem -> ingest_prehashed_shard_batch_with_trace
    let wr_rw2_unsorted = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_strs("__name__", "up"),
                cortexpb::LabelPair::from_strs("b", "1"),
                cortexpb::LabelPair::from_strs("a", ""),
            ],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: now_ms(),
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let rw2_wire = mimir_proto::zerocopy::from_write_request_to_rw2(&wr_rw2_unsorted, 0, None)
        .unwrap()
        .encode_to_vec();
    let mut scratch = WriteRequestScratch::new();
    let mut resolved = Rw2ResolvedScratch::new();
    let mut label_bytes = Vec::new();
    let mut samples = Vec::new();
    let mut meta_out = Vec::new();
    let mut ranges = Vec::new();
    decode_rw2_stream(
        &rw2_wire,
        DecodeOptions::rw2(),
        &mut scratch,
        &mut resolved,
        &mut label_bytes,
        &mut samples,
        &mut meta_out,
        |s| {
            ranges.push((
                s.label_bytes_range.clone(),
                s.raw_label_bytes_range.clone(),
                s.samples_range.clone(),
                s.labels_sorted_unique,
                s.bad_utf8,
                s.min_sample_ts,
                s.created_timestamp,
            ));
        },
    )
    .unwrap();
    let (lr, rlr, sr, sorted, bad, min_ts, ct) = &ranges[0];
    let enc = &label_bytes[lr.clone()];
    let raw_enc = rlr.as_ref().map_or(enc, |r| &label_bytes[r.clone()]);
    let item = mimir_ingester::push::PrehashedSeriesItem {
        encoded_labels: enc,
        raw_encoded_labels: raw_enc,
        samples: &samples[sr.clone()],
        created_timestamp: *ct,
        prom_hash: 0,
        labels_sorted_unique: *sorted,
        bad_utf8: *bad,
        histograms: &mut [],
        exemplars: &[],
    };
    let err_rw2 = ing
        .ingest_prehashed_shard_batch_with_trace(
            "u-e3",
            cortexpb::write_request::SourceEnum::Api,
            *min_ts,
            &mut [item],
            &[],
            None,
        )
        .unwrap_err()
        .to_string();
    assert!(
        err_rw2.contains(r#"up{b="1", a=""}"#),
        "RW2 unsorted error must keep a=\"\" in wire order: {err_rw2}"
    );
}

// ============================================================================
// Item 8 (W18-WPE-05): /cortex.Ingester/Push validation order
// ============================================================================
#[tokio::test]
async fn w18_wpe_05_push_validation_order_matches_go() {
    let dir = TempDir::new().unwrap();
    let ing = new_test_ingester(dir.path(), LimitsConfig::default(), false);
    ing.set_state_for_test(State::Running);

    let svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // 1) push_grpc_method_enabled=false + valid tenant + malformed wire [0xff] => Code::Internal (dec(in) wins)
    let req1 = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/Push")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("x-scope-orgid", "user-1")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(&[0xff])))))
        .unwrap();
    let (_, st1, _) = collect_grpc_http_resp(svc.clone().oneshot(req1).await.unwrap()).await;
    let st1 = st1.unwrap();
    assert_eq!(st1.code(), tonic::Code::Internal);
    assert!(
        st1.message().starts_with("grpc: failed to unmarshal the received message:"),
        "{}",
        st1.message()
    );

    // 2) push_grpc_method_enabled=false + x-scope-orgid: a|b + valid empty WriteRequest => Unimplemented + MethodNotAllowed
    let empty_wr = cortexpb::WriteRequest::default().encode_to_vec();
    let req2 = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/Push")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("x-scope-orgid", "a|b")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(&empty_wr)))))
        .unwrap();
    let (_, st2, _) = collect_grpc_http_resp(svc.clone().oneshot(req2).await.unwrap()).await;
    let st2 = st2.unwrap();
    assert_eq!(st2.code(), tonic::Code::Unimplemented);
    let d2 = extract_cortex_error_details(&st2).expect("MethodNotAllowed details");
    assert_eq!(d2.cause, ErrorCause::MethodNotAllowed as i32);

    // 3) push_grpc_method_enabled=false + missing x-scope-orgid + valid empty WriteRequest => Unknown ("no org id")
    let req3 = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/Push")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(&empty_wr)))))
        .unwrap();
    let (_, st3, _) = collect_grpc_http_resp(svc.oneshot(req3).await.unwrap()).await;
    let st3 = st3.unwrap();
    assert_eq!(st3.code(), tonic::Code::Unknown);
}

// ============================================================================
// Items 9 & 12 (W18-WPE-06 & W18-WPE-09): Echo grpc-encoding across all 14 RPCs
// and do NOT compress 0-byte frames in SnappyCompressBody.
// ============================================================================
#[tokio::test]
async fn w18_wpe_06_and_09_echo_grpc_encoding_all_rpcs_and_skip_zero_byte_frames() {
    let dir = TempDir::new().unwrap();
    let ing = new_test_ingester(dir.path(), LimitsConfig::default(), true);
    ing.set_state_for_test(State::Running);
    let ts = now_ms();
    let wr = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "echo_metric")],
            samples: vec![cortexpb::Sample {
                value: 7.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    ing.push_grpc_raw("u-enc", Bytes::from(wr.encode_to_vec()))
        .await
        .unwrap();

    let svc = IngesterService::new(Arc::clone(&ing))
        .into_server()
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
        .max_encoding_message_size(4);

    // 1) W18-WPE-06: ActiveSeries + LabelNames with snappy & s2 echo grpc-encoding and compress non-empty frames
    let as_req = ingesterpb::ActiveSeriesRequest {
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".into(),
            value: "echo_metric".into(),
        }],
        r#type: 0,
    }
    .encode_to_vec();
    let as_framed = mimir_proto::snappy::encode_grpc_frame(&as_req, false).unwrap();
    let http_as = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/ActiveSeries")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-encoding", "snappy")
        .header("grpc-accept-encoding", "snappy,s2,gzip,identity")
        .header("x-scope-orgid", "u-enc")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(as_framed))))
        .unwrap();
    let svc_unbounded = IngesterService::new(Arc::clone(&ing))
        .into_server()
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd);
    let (h_as, st_as, body_as) =
        collect_grpc_http_resp(svc_unbounded.clone().oneshot(http_as).await.unwrap()).await;
    assert_eq!(st_as.unwrap().code(), tonic::Code::Ok);
    assert_eq!(
        h_as.get("grpc-encoding").and_then(|v| v.to_str().ok()),
        Some("snappy")
    );
    assert!(!body_as.is_empty());
    assert_eq!(body_as[0], 1, "non-empty response frame must be compressed");

    let ln_req = ingesterpb::LabelNamesRequest {
        start_timestamp_ms: ts - 1000,
        end_timestamp_ms: ts + 1000,
        matchers: None,
        limit: 0,
    }
    .encode_to_vec();
    let ln_framed = mimir_proto::snappy::encode_grpc_frame(&ln_req, false).unwrap();
    let http_ln = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/LabelNames")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-encoding", "s2")
        .header("grpc-accept-encoding", "snappy,s2,gzip,identity")
        .header("x-scope-orgid", "u-enc")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(ln_framed))))
        .unwrap();
    let (h_ln, st_ln, body_ln) =
        collect_grpc_http_resp(svc_unbounded.oneshot(http_ln).await.unwrap()).await;
    assert_eq!(st_ln.unwrap().code(), tonic::Code::Ok);
    assert_eq!(
        h_ln.get("grpc-encoding").and_then(|v| v.to_str().ok()),
        Some("s2")
    );
    assert_eq!(body_ln[0], 1);

    // 2) W18-WPE-09: 0-byte response (MetricsForLabelMatchers on empty tenant) with snappy and max_encoding_message_size(4)
    let mflm_req = ingesterpb::MetricsForLabelMatchersRequest {
        start_timestamp_ms: ts - 1000,
        end_timestamp_ms: ts + 1000,
        matchers_set: Vec::new(),
        limit: 0,
    }
    .encode_to_vec();
    let mflm_framed = mimir_proto::snappy::encode_grpc_frame(&mflm_req, false).unwrap();
    let http_mflm = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/MetricsForLabelMatchers")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-encoding", "snappy")
        .header("grpc-accept-encoding", "snappy,s2,gzip,identity")
        .header("x-scope-orgid", "u-empty")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(mflm_framed))))
        .unwrap();
    let (h_mflm, st_mflm, body_mflm) =
        collect_grpc_http_resp(svc.oneshot(http_mflm).await.unwrap()).await;
    assert_eq!(st_mflm.unwrap().code(), tonic::Code::Ok);
    assert_eq!(
        h_mflm.get("grpc-encoding").and_then(|v| v.to_str().ok()),
        Some("snappy")
    );
    assert_eq!(
        body_mflm,
        vec![0, 0, 0, 0, 0],
        "0-byte protobuf frame must remain uncompressed flag=0, len=0"
    );
}

// ============================================================================
// Item 10 (W18-WPE-07): RequestRateLimited -> Code::Internal in push/read/pusher
// ============================================================================
#[test]
fn w18_wpe_07_request_rate_limited_maps_to_internal() {
    let err = IngesterError::ReactiveLimiterExceeded {
        message: "limit exceeded".to_string(),
    };
    let push_status = err.map_push_error_to_status();
    assert_eq!(push_status.code(), tonic::Code::Internal);
    let push_details = extract_cortex_error_details(&push_status).unwrap();
    assert_eq!(push_details.cause, ErrorCause::RequestRateLimited as i32);
    assert!(!push_details.soft);

    let read_status = err.map_read_error_to_status();
    assert_eq!(read_status.code(), tonic::Code::Internal);
    let read_details = extract_cortex_error_details(&read_status).unwrap();
    assert_eq!(read_details.cause, ErrorCause::RequestRateLimited as i32);

    let pusher_err = ingester_err_to_pusher_err(err);
    assert_eq!(pusher_err.grpc_code.as_deref(), Some("Internal"));
}

// ============================================================================
// Item 11 (W18-WPE-08): Raw wire-order ex_str in all TsdbIngestExemplar error sites
// ============================================================================
#[tokio::test]
async fn w18_wpe_08_tsdb_ingest_exemplar_uses_raw_wire_order_labels() {
    let dir = TempDir::new().unwrap();
    let limits = LimitsConfig {
        max_global_exemplars_per_user: 10,
        ..Default::default()
    };
    let ing = new_test_ingester(dir.path(), limits, true);
    ing.set_state_for_test(State::Running);

    let ts = now_ms();
    let long_val = "x".repeat(130);
    let wr = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "ex_order_metric")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            exemplars: vec![cortexpb::Exemplar {
                labels: vec![
                    cortexpb::LabelPair::from_strs("z", "1"),
                    cortexpb::LabelPair::from_strs("a", &long_val),
                ],
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let err_raw = ing
        .push_grpc_raw("u-ord1", Bytes::from(wr.encode_to_vec()))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err_raw.contains(&format!(r#"exemplar={{z="1", a="{long_val}"}}"#)),
        "push_grpc_raw must keep raw wire order in TsdbIngestExemplar: {err_raw}"
    );

    let err_owned = ing
        .push_write_request_sync("u-ord2", &wr)
        .unwrap_err()
        .to_string();
    assert!(
        err_owned.contains(&format!(r#"exemplar={{z="1", a="{long_val}"}}"#)),
        "push_write_request_sync must keep raw wire order in TsdbIngestExemplar: {err_owned}"
    );
}

// ============================================================================
// Item 13 (W18-WPE-10): LabelValues validation order, custom read decoder prefix,
// and mid-stream GrpcErrorLog recording.
// ============================================================================
#[tokio::test]
async fn w18_wpe_10_label_values_order_decoder_prefix_and_midstream_grpc_error_log() {
    let dir = TempDir::new().unwrap();
    let ing = new_test_ingester(dir.path(), LimitsConfig::default(), true);
    ing.set_state_for_test(State::Running);
    let svc = IngesterService::new(Arc::clone(&ing)).into_server();

    // 1) LabelValues with x-scope-orgid: a|b AND invalid matcher type => invalid matcher error beats multiple org IDs
    let lv_bad = ingesterpb::LabelValuesRequest {
        label_name: "__name__".into(),
        start_timestamp_ms: 0,
        end_timestamp_ms: 1000,
        matchers: Some(ingesterpb::LabelMatchers {
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: 99,
                name: "__name__".into(),
                value: "up".into(),
            }],
        }),
        limit: 0,
    }
    .encode_to_vec();
    let http_lv = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/LabelValues")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("x-scope-orgid", "a|b")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(&lv_bad)))))
        .unwrap();
    let (_, st_lv, _) = collect_grpc_http_resp(svc.clone().oneshot(http_lv).await.unwrap()).await;
    let st_lv = st_lv.unwrap();
    assert_eq!(st_lv.code(), tonic::Code::Internal);
    assert!(
        st_lv.message().contains("invalid matcher type"),
        "expected matcher error before tenant error, got: {}",
        st_lv.message()
    );

    // 2) Custom read decoders (QueryStream & MetricsForLabelMatchers) include grpc-go unmarshal prefix
    for uri in [
        "/cortex.Ingester/QueryStream",
        "/cortex.Ingester/MetricsForLabelMatchers",
    ] {
        let http_bad = http::Request::builder()
            .method(http::Method::POST)
            .uri(uri)
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("x-scope-orgid", "u1")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(&[0xff])))))
            .unwrap();
        let (_, st_bad, _) =
            collect_grpc_http_resp(svc.clone().oneshot(http_bad).await.unwrap()).await;
        let st_bad = st_bad.unwrap();
        assert_eq!(st_bad.code(), tonic::Code::Internal);
        assert!(
            st_bad
                .message()
                .starts_with("grpc: failed to unmarshal the received message: "),
            "{uri} missing prefix: {}",
            st_bad.message()
        );
    }

    // 3) End-to-end mid-stream error in LabelValuesCardinality (stopped compute_worker_pool -> mid-stream PoolStopped)
    ing.compute_worker_pool.stop();
    let ts = now_ms();
    let wr = cortexpb::WriteRequest {
        timeseries: vec![cortexpb::TimeSeries {
            labels: vec![cortexpb::LabelPair::from_strs("__name__", "midstream_m")],
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    ing.push_grpc_raw("u-log", Bytes::from(wr.encode_to_vec()))
        .await
        .unwrap();

    let log = GrpcErrorLog::default();
    let lvc_req = ingesterpb::LabelValuesCardinalityRequest {
        label_names: vec!["__name__".into()],
        matchers: Vec::new(),
        count_method: ingesterpb::CountMethod::InMemory as i32,
    }
    .encode_to_vec();
    let mut http_lvc = http::Request::builder()
        .method(http::Method::POST)
        .uri("/cortex.Ingester/LabelValuesCardinality")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("x-scope-orgid", "u-log")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(&lvc_req)))))
        .unwrap();
    http_lvc.extensions_mut().insert(log.clone());
    let (_, st_lvc, _) = collect_grpc_http_resp(svc.oneshot(http_lvc).await.unwrap()).await;
    let st_lvc = st_lvc.unwrap();
    assert_eq!(st_lvc.code(), tonic::Code::Internal);
    let entry = log
        .take()
        .expect("mid-stream streaming read error must record GrpcErrorLog");
    assert!(entry.keep);
    assert_eq!(entry.err, st_lvc.message());
}
