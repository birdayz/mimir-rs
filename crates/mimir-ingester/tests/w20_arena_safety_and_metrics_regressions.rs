//! Wave 20 Arena Safety, DoS Bounds, Compaction & Metrics Regression Tests:
//! - W20-SDW-01: Bound RW2 expanded label arena + eliminate u32 offset truncation + shrink WorkerArena
//! - W20-SDW-05: Eliminate assert!(len < (1 << 24)) panic on oversized exemplar & Kafka v0/v1 / owned Push labels
//! - W20-SDW-02: Cap cumulative decompressed bytes & unary frame count in Snappy/S2 gRPC decompression
//! - W20-CR-03: Do not exit UserTsdb::compact_head loop early when head.num_series() == 0
//! - M20-02: workerpool enqueue_duration & task_duration are native-only histograms
//! - M20-05: IngesterShardedBatchConsumer StoragePusherMetrics parity (estimated_timeseries, shards/pushers, sequential empty)
//! - M20-09: Multi-shard Push observes appender_add_duration & appender_commit_duration only on success
//! - W20-SDW-09: ActiveSeries active_native_histogram_buckets uses wrapping_add / wrapping_sub
//! - M20-07 & W20-SDW-11 (items 2 & 3): RefLeaks validation + Ingester::set_ref_leaks_tracker on Push,
//!   client::compat negative limit guard, and proleptic Gregorian UTC fallback for extreme i64 timestamps.

mod common;

use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http;
use bytes::Bytes;
use mimir_common::services::State;
use mimir_config::{LimitsConfig, Overrides};
use mimir_ingester::active_series::ActiveSeries;
use mimir_ingester::errors::format_timestamp_ms_go_time_string;
use mimir_ingester::http::ingester_router;
use mimir_ingester::ingest_storage::IngesterShardedBatchConsumer;
use mimir_ingester::workerpool::Pool;
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::client::compat::{
    from_label_names_request, from_metrics_for_label_matchers_request,
};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use mimir_proto::ref_leaks::InstrumentRefLeaksConfig;
use mimir_proto::zerocopy::{
    decode_rw2_stream, decode_rw2_stream_with_limit, DecodeError, DecodeOptions,
    Rw2ResolvedScratch, WriteRequestScratch,
};
use mimir_proto::ringpb::{Desc, InstanceState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{CasOutcome, KvClient};
use mimir_ring::ring::model::DescExt;
use mimir_ring::ring::ring_desc_codec;
use mimir_storage::ingest::pusher::PusherConsumerMetrics;
use mimir_storage::ingest::version::{RecordSerializer, RecordVersion};
use mimir_storage::ingest::FetchedRecord;
use prometheus::{Encoder as _, ProtobufEncoder, Registry};
use tempfile::TempDir;
use tower::ServiceExt;

use common::{
    default_ingester_test_config, default_limits_test_config,
    prepare_ingester_with_kv_and_overrides, start_and_wait_healthy,
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

fn new_running_ingester(
    dir: &std::path::Path,
    limits: LimitsConfig,
    reg: Option<&Registry>,
) -> Arc<Ingester> {
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir);
    cfg.push_grpc_method_enabled = true;
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(limits),
        None,
        None,
        None,
        None,
        None,
        reg,
    )
    .expect("create ingester");
    ing.set_state_for_test(State::Running);
    ing
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

fn simple_series(name: &str, ts_ms: i64, val: f64) -> cortexpb::TimeSeries {
    cortexpb::TimeSeries {
        labels: vec![cortexpb::LabelPair {
            name: Bytes::from_static(b"__name__"),
            value: Bytes::copy_from_slice(name.as_bytes()),
        }],
        samples: vec![cortexpb::Sample {
            timestamp_ms: ts_ms,
            value: val,
        }],
        ..Default::default()
    }
}

fn make_kafka_record(
    version: RecordVersion,
    key: &str,
    offset: i64,
    req: &cortexpb::WriteRequest,
) -> FetchedRecord {
    let (recs, _) = RecordSerializer::new(version)
        .to_records("t", 0, key, req, 64 << 20)
        .expect("serialize record");
    let r = recs.into_iter().next().expect("one record");
    FetchedRecord {
        key: r.key,
        value: r.value,
        headers: r.headers,
        offset,
        timestamp_ms: 0,
    }
}

fn counter_val(reg: &Registry, name: &str) -> f64 {
    reg.gather()
        .iter()
        .find(|mf| mf.get_name() == name)
        .and_then(|mf| mf.get_metric().first().map(|m| m.get_counter().get_value()))
        .unwrap_or(0.0)
}

fn hist_stats(reg: &Registry, name: &str) -> (u64, f64) {
    reg.gather()
        .iter()
        .find(|mf| mf.get_name() == name)
        .and_then(|mf| {
            mf.get_metric().first().map(|m| {
                let h = m.get_histogram();
                (h.get_sample_count(), h.get_sample_sum())
            })
        })
        .unwrap_or((0, 0.0))
}

// ---------------------------------------------------------------------------
// 1. [W20-SDW-01] RW2 expanded label arena cap + WorkerArena shrink
// ---------------------------------------------------------------------------
#[test]
fn w20_sdw_01_rw2_stream_bounds_expanded_label_arena_and_handles_large_batches() {
    // Build an RW2 payload with a 64 KiB symbol referenced 10 times (~640 KiB expanded).
    let big_val = "x".repeat(64 * 1024);
    let syms = ["", "__name__", "m", "big", big_val.as_str()];
    let mut packed = Vec::new();
    for idx in [1u64, 2, 3, 4] {
        mimir_proto::prost::encoding::encode_varint(idx, &mut packed);
    }
    let mut ts_rw2 = Vec::new();
    ts_rw2.push(0x0a);
    mimir_proto::prost::encoding::encode_varint(packed.len() as u64, &mut ts_rw2);
    ts_rw2.extend_from_slice(&packed);

    let mut wire = Vec::new();
    for s in syms {
        wire.push(0x22);
        mimir_proto::prost::encoding::encode_varint(s.len() as u64, &mut wire);
        wire.extend_from_slice(s.as_bytes());
    }
    for _ in 0..10 {
        wire.push(0x2a);
        mimir_proto::prost::encoding::encode_varint(ts_rw2.len() as u64, &mut wire);
        wire.extend_from_slice(&ts_rw2);
    }

    let mut scratch = WriteRequestScratch::new();
    let mut label_bytes = b"sentinel".to_vec();
    let mut samples = Vec::new();

    // Tight expansion cap must reject before amplifying and truncate cleanly back to sentinel.
    {
        let mut resolved = Rw2ResolvedScratch::new();
        let mut metas = Vec::new();
        let err = decode_rw2_stream_with_limit(
            &wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut metas,
            256 * 1024,
            |_| {},
        )
        .expect_err("must reject RW2 symbol amplification past cap");
        assert!(matches!(
            err,
            DecodeError::LabelArenaLimitExceeded { limit: 262_144 }
        ));
    }
    assert_eq!(label_bytes.as_slice(), b"sentinel");

    // Within default cap it decodes all 10 series accurately.
    let mut count = 0usize;
    {
        let mut resolved = Rw2ResolvedScratch::new();
        let mut metas = Vec::new();
        decode_rw2_stream(
            &wire,
            DecodeOptions::rw2(),
            &mut scratch,
            &mut resolved,
            &mut label_bytes,
            &mut samples,
            &mut metas,
            |_| {
                count += 1;
            },
        )
        .expect("default cap allows 640 KiB expansion");
    }
    assert_eq!(count, 10);

    // Multi-MiB Kafka v2 batch followed by small batch exercises WorkerArena shrink & ingestion.
    let dir = TempDir::new().unwrap();
    let ing = new_running_ingester(dir.path(), default_limits_test_config(), None);
    let reg = Registry::new();
    let consumer = IngesterShardedBatchConsumer::new(
        Arc::clone(&ing),
        mimir_config::IngestStorageConfig::default().kafka,
        PusherConsumerMetrics::new(Some(&reg)),
    );
    let ts = now_ms();
    let big_series: Vec<_> = (0..100)
        .map(|i| cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair::from_static("__name__", "big_rw2"),
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"idx"),
                    value: Bytes::from(i.to_string()),
                },
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"pad"),
                    value: Bytes::from(vec![b'a'; 50 * 1024]),
                },
            ],
            samples: vec![cortexpb::Sample {
                timestamp_ms: ts,
                value: i as f64,
            }],
            ..Default::default()
        })
        .collect();
    let wr_big = cortexpb::WriteRequest {
        timeseries: big_series,
        ..Default::default()
    };
    consumer
        .consume_records_sync(&[make_kafka_record(RecordVersion::V2, "t-big", 1, &wr_big)])
        .unwrap();
    let wr_small = cortexpb::WriteRequest {
        timeseries: vec![simple_series("small_rw2", ts, 1.0)],
        ..Default::default()
    };
    consumer
        .consume_records_sync(&[make_kafka_record(RecordVersion::V2, "t-big", 2, &wr_small)])
        .unwrap();
    assert_eq!(ing.get_tsdb("t-big").unwrap().db.head().num_series(), 101);
}

// ---------------------------------------------------------------------------
// 2. [W20-SDW-05] Oversized (>= 16 MiB) exemplar & series labels never panic
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w20_sdw_05_oversized_16mib_exemplar_and_series_labels_never_panic() {
    let huge = Bytes::from(vec![b'x'; 1 << 24]);
    let ts = now_ms();

    // Case A: max_exemplars == 0 (disabled) on both RawPushSvc and owned push_grpc.
    {
        let dir = TempDir::new().unwrap();
        let mut limits = default_limits_test_config();
        limits.max_global_exemplars_per_user = 0;
        let ing = new_running_ingester(dir.path(), limits, None);
        let svc = IngesterService::new(Arc::clone(&ing)).into_server();

        let req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "ex_disabled")],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: ts,
                    value: 1.0,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: Bytes::from_static(b"trace_id"),
                        value: huge.clone(),
                    }],
                    value: 1.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let http_req = http::Request::builder()
            .method("POST")
            .uri("/cortex.Ingester/Push")
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("x-scope-orgid", "tenant-ex0")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(
                &req.encode_to_vec(),
            )))))
            .unwrap();
        let resp = svc.oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);

        // Owned push_grpc must also succeed without panicking when exemplars are disabled.
        ing.push_grpc("tenant-ex0-owned", &req).await.unwrap();
    }

    // Case B: max_exemplars > 0 with 16 MiB exemplar label -> soft error, never panic.
    {
        let dir = TempDir::new().unwrap();
        let mut limits = default_limits_test_config();
        limits.max_global_exemplars_per_user = 10;
        let ing = new_running_ingester(dir.path(), limits, None);

        let req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "ex_enabled")],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: ts,
                    value: 2.0,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: Bytes::from_static(b"trace_id"),
                        value: huge.clone(),
                    }],
                    value: 2.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = ing
            .push_grpc("tenant-ex-on", &req)
            .await
            .expect_err("oversized exemplar returns soft partial error, not panic");
        assert!(err.is_soft(), "expected soft error, got: {err}");
        let err2 = ing
            .push_grpc_raw("tenant-ex-on-raw", Bytes::from(req.encode_to_vec()))
            .await
            .expect_err("raw push oversized exemplar returns soft error");
        assert!(err2.is_soft(), "expected soft error, got: {err2}");
    }

    // Case C: Owned push_grpc with 16 MiB series label -> soft validation error, never panic.
    {
        let dir = TempDir::new().unwrap();
        let ing = new_running_ingester(dir.path(), default_limits_test_config(), None);
        let req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "big_series"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"big"),
                        value: huge.clone(),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: ts,
                    value: 1.0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = ing
            .push_grpc("tenant-big-lbl", &req)
            .await
            .expect_err("oversized series label must return validation error");
        assert!(err.is_soft(), "expected soft validation error, got: {err}");
    }

    // Case D: Kafka v1 with 16 MiB series label + 16 MiB exemplar label -> never panics.
    {
        let dir = TempDir::new().unwrap();
        let mut limits = default_limits_test_config();
        limits.max_global_exemplars_per_user = 10;
        let ing = new_running_ingester(dir.path(), limits, None);
        let reg = Registry::new();
        let consumer = IngesterShardedBatchConsumer::new(
            Arc::clone(&ing),
            mimir_config::IngestStorageConfig::default().kafka,
            PusherConsumerMetrics::new(Some(&reg)),
        );
        let req_big_series = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair::from_static("__name__", "v1_big"),
                    cortexpb::LabelPair {
                        name: Bytes::from_static(b"big"),
                        value: huge.clone(),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: ts,
                    value: 1.0,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_big_ex = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "v1_ex")],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: ts,
                    value: 3.0,
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: Bytes::from_static(b"trace_id"),
                        value: huge,
                    }],
                    value: 3.0,
                    timestamp_ms: ts,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        consumer
            .consume_records_sync(&[
                make_kafka_record(RecordVersion::V1, "t-v1", 1, &req_big_series),
                make_kafka_record(RecordVersion::V1, "t-v1", 2, &req_big_ex),
            ])
            .expect("oversized v1 series/exemplar must not panic or fail batch");
        assert_eq!(ing.get_tsdb("t-v1").unwrap().db.head().num_series(), 1);
    }
}

// ---------------------------------------------------------------------------
// 3. [W20-SDW-02] Multi-frame S2/Snappy cumulative decompression & unary frame cap
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w20_sdw_02_multi_frame_s2_and_snappy_cumulative_decompression_capped() {
    let dir = TempDir::new().unwrap();
    let ing = new_running_ingester(dir.path(), default_limits_test_config(), None);
    let max_decode = 1024usize;
    let svc = IngesterService::new(Arc::clone(&ing))
        .into_server()
        .max_decoding_message_size(max_decode);

    // 2 frames of 700 bytes each (<= 1024 individually, 1400 > 1024 cumulatively).
    // Snappy stream framing is valid for both grpc-encoding: snappy and grpc-encoding: s2.
    let payload = vec![0u8; 700];
    let frame = mimir_proto::snappy::encode_grpc_frame(&payload, true).unwrap();
    assert!(frame.len() * 2 <= max_decode + 5);
    let mut two_frames = Vec::with_capacity(frame.len() * 2);
    two_frames.extend_from_slice(&frame);
    two_frames.extend_from_slice(&frame);
    let two_frames = Bytes::from(two_frames);

    for enc in ["s2", "snappy"] {
        let http_req = http::Request::builder()
            .method("POST")
            .uri("/cortex.Ingester/Push")
            .header("content-type", "application/grpc")
            .header("grpc-encoding", enc)
            .header("te", "trailers")
            .header("x-scope-orgid", "tenant-a")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(
                two_frames.clone(),
            ))))
            .unwrap();
        let resp = svc.clone().oneshot(http_req).await.unwrap();
        let status = tonic::Status::from_header_map(resp.headers()).expect("grpc-status header");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted, "enc={enc}");
        assert!(
            status
                .message()
                .contains("received message after decompression larger than max 1024"),
            "unexpected status for {enc}: {status:?}"
        );
    }

    // 2 small compressed frames within max_decode must still be rejected as multiple unary frames.
    let small_frame = mimir_proto::snappy::encode_grpc_frame(&[1u8; 32], true).unwrap();
    let mut two_small = Vec::with_capacity(small_frame.len() * 2);
    two_small.extend_from_slice(&small_frame);
    two_small.extend_from_slice(&small_frame);
    let http_req3 = http::Request::builder()
        .method("POST")
        .uri("/cortex.Ingester/Push")
        .header("content-type", "application/grpc")
        .header("grpc-encoding", "s2")
        .header("te", "trailers")
        .header("x-scope-orgid", "tenant-a")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
            two_small,
        )))))
        .unwrap();
    let resp3 = svc.oneshot(http_req3).await.unwrap();
    let status3 = tonic::Status::from_header_map(resp3.headers()).expect("grpc-status header");
    assert_eq!(status3.code(), tonic::Code::Internal);
}

// ---------------------------------------------------------------------------
// 4. [W20-CR-03] UserTsdb::compact_head continues past num_series() == 0
// ---------------------------------------------------------------------------
#[tokio::test]
async fn w20_cr_03_compact_head_continues_past_zero_series_to_max_time() {
    let dir = TempDir::new().unwrap();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    let block_dur = blocks_cfg
        .tsdb
        .block_ranges_period
        .first()
        .map(|d| d.as_millis().max(1))
        .unwrap_or(2 * 3600 * 1000);
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(default_limits_test_config()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    ing.set_state_for_test(State::Running);

    // Push series A in range 0 [0, block_dur) and series B in range 2 [2*block_dur, 3*block_dur).
    let t_a = 1_000i64;
    let t_b = block_dur * 2 + 1_000;
    let req = cortexpb::WriteRequest {
        timeseries: vec![
            simple_series("series_a", t_a, 1.0),
            simple_series("series_b", t_b, 2.0),
        ],
        ..Default::default()
    };
    ing.push_grpc("tenant-cr03", &req).await.unwrap();

    let utsdb = ing.get_tsdb("tenant-cr03").unwrap();
    let head = utsdb.db.head();
    let ref_b = head
        .get_entry_by_sorted_pairs(&[("__name__", "series_b")])
        .expect("series_b entry")
        .ref_id;

    // Early compaction of series B removes all series from range 2 without changing head.max_time().
    utsdb.db.compact_selected_series(&[ref_b]).unwrap();
    assert_eq!(head.num_series(), 1);
    assert_eq!(head.max_time(), t_b);

    // Forced compact_head must iterate through range 0, range 1, and range 2 even after
    // num_series() drops to 0 at the end of range 0.
    utsdb.compact_head(t_b).await.unwrap();
    assert_eq!(head.min_time(), t_b + 1);
    assert_eq!(head.appendable_min_valid_time(), t_b + 1);
}

// ---------------------------------------------------------------------------
// 5. [M20-02] workerpool histograms emit native histogram fields in protobuf
// ---------------------------------------------------------------------------
#[test]
fn m20_02_workerpool_histograms_encode_native_buckets_in_protobuf() {
    let reg = Registry::new();
    let pool = Pool::new(1, "w20-pool", Some(&reg)).unwrap();
    pool.start().unwrap();
    let (tx, rx) = mpsc::channel();
    pool.submit("dim1", "tenant-a", move || {
        std::thread::sleep(Duration::from_millis(2));
        tx.send(()).unwrap();
    })
    .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    pool.stop();

    let gathered = reg.gather();
    for name in [
        "mimir_workerpool_enqueue_duration_seconds",
        "mimir_workerpool_task_duration_seconds",
    ] {
        let mf = gathered
            .iter()
            .find(|mf| mf.get_name() == name)
            .cloned()
            .unwrap_or_else(|| panic!("missing {name}"));
        let metric = &mf.get_metric()[0];
        let h = metric.get_histogram();
        assert!(h.get_bucket().is_empty(), "{name} must be native-only");

        // Encode into delimited protobuf (same format Prometheus scrapes) and verify the
        // native histogram fields (schema=5 -> tag 0x28, zero_threshold=6 -> tag 0x31,
        // positive_span=12 -> tag 0x62, positive_delta=13 -> tag 0x68/0x6a) are encoded.
        let mut pb = Vec::new();
        ProtobufEncoder::new().encode(&[mf], &mut pb).unwrap();
        assert!(!pb.is_empty());
        // Native-only histogram with positive sample has positive_span (field 12, wire type 2 => 0x62)
        // and schema (field 5, wire type 0 => 0x28) inside the Histogram submessage.
        assert!(
            pb.windows(2).any(|w| w == [0x28, 0x06]),
            "{name} protobuf exposition missing native schema zigzag varint (factor 1.1 -> schema 3 -> zigzag 6)"
        );
        assert!(
            pb.contains(&0x62),
            "{name} protobuf exposition missing native positive_span field 12"
        );
    }
}

// ---------------------------------------------------------------------------
// 6. [M20-05] IngesterShardedBatchConsumer StoragePusherMetrics parity
// ---------------------------------------------------------------------------
#[test]
fn m20_05_ingest_storage_consumer_pusher_metrics_parity() {
    let ts = now_ms();

    // Parallel mode (ingestion_concurrency_max = 4):
    {
        let dir = TempDir::new().unwrap();
        let ing = new_running_ingester(dir.path(), default_limits_test_config(), None);
        let reg = Registry::new();
        let mut kcfg = mimir_config::IngestStorageConfig::default().kafka;
        kcfg.ingestion_concurrency_max = 4;
        kcfg.ingestion_concurrency_estimated_bytes_per_sample = 10;
        kcfg.ingestion_concurrency_batch_size = 10;
        kcfg.ingestion_concurrency_target_flushes_per_shard = 1;
        let consumer = IngesterShardedBatchConsumer::new(
            Arc::clone(&ing),
            kcfg,
            PusherConsumerMetrics::new(Some(&reg)),
        );

        // 1) Corrupt record only -> skipped, estimated_timeseries == 0, shards/pushers observed (0).
        let corrupt = FetchedRecord {
            key: Bytes::from_static(b"t1"),
            value: Bytes::from_static(b"\xff\xff\xff\xff"),
            headers: vec![],
            offset: 1,
            timestamp_ms: 0,
        };
        consumer.consume_records_sync(&[corrupt]).unwrap();
        assert_eq!(
            counter_val(&reg, "cortex_ingest_storage_reader_pusher_estimated_timeseries_total"),
            0.0
        );
        assert_eq!(
            hist_stats(&reg, "cortex_ingest_storage_reader_pushers_per_push"),
            (1, 0.0)
        );
        assert_eq!(
            hist_stats(&reg, "cortex_ingest_storage_reader_shards_per_push"),
            (1, 0.0)
        );

        // 2) Same tenant t1 with API + RULE -> 2 pushers, estimated_timeseries counted per pusher.
        let wr_api = cortexpb::WriteRequest {
            timeseries: vec![simple_series("m_api", ts, 1.0)],
            source: cortexpb::write_request::SourceEnum::Api as i32,
            ..Default::default()
        };
        let wr_rule = cortexpb::WriteRequest {
            timeseries: vec![simple_series("m_rule", ts, 2.0)],
            source: cortexpb::write_request::SourceEnum::Rule as i32,
            ..Default::default()
        };
        let r_api = make_kafka_record(RecordVersion::V1, "t1", 2, &wr_api);
        let r_rule = make_kafka_record(RecordVersion::V1, "t1", 3, &wr_rule);
        let expected_per_pusher = (r_api.value.len() + r_rule.value.len()) / 10;
        consumer.consume_records_sync(&[r_api, r_rule]).unwrap();
        assert_eq!(
            counter_val(&reg, "cortex_ingest_storage_reader_pusher_estimated_timeseries_total"),
            (expected_per_pusher * 2) as f64
        );
        assert_eq!(
            hist_stats(&reg, "cortex_ingest_storage_reader_pushers_per_push"),
            (2, 2.0)
        );
        let (sc, ss) = hist_stats(&reg, "cortex_ingest_storage_reader_shards_per_push");
        assert_eq!(sc, 2);
        assert!(ss >= 2.0);

        // 3) Invalid tenant key still observes shards_per_push & pushers_per_push on error exit.
        let bad = make_kafka_record(RecordVersion::V1, "x|y", 4, &wr_api);
        assert!(consumer.consume_records_sync(&[bad]).is_err());
        assert_eq!(
            hist_stats(&reg, "cortex_ingest_storage_reader_pushers_per_push").0,
            3
        );
        assert_eq!(
            hist_stats(&reg, "cortex_ingest_storage_reader_shards_per_push").0,
            3
        );
    }

    // Sequential mode (ingestion_concurrency_max = 0) with empty WriteRequest:
    {
        let dir = TempDir::new().unwrap();
        let ing = new_running_ingester(dir.path(), default_limits_test_config(), None);
        let reg = Registry::new();
        let mut kcfg = mimir_config::IngestStorageConfig::default().kafka;
        kcfg.ingestion_concurrency_max = 0;
        let consumer = IngesterShardedBatchConsumer::new(
            Arc::clone(&ing),
            kcfg,
            PusherConsumerMetrics::new(Some(&reg)),
        );
        let empty_rec_v0 = FetchedRecord {
            key: Bytes::from_static(b"t-seq"),
            value: Bytes::new(),
            headers: vec![],
            offset: 1,
            timestamp_ms: 0,
        };
        let empty_rec_v2 = FetchedRecord {
            key: Bytes::from_static(b"t-seq"),
            value: Bytes::new(),
            headers: vec![(
                mimir_proto::symbols::RECORD_VERSION_HEADER_KEY.to_string(),
                Bytes::from_static(&[0, 0, 0, 2]),
            )],
            offset: 2,
            timestamp_ms: 0,
        };
        consumer
            .consume_records_sync(&[empty_rec_v0, empty_rec_v2])
            .unwrap();
        assert_eq!(
            counter_val(&reg, "cortex_ingest_storage_reader_requests_total"),
            2.0
        );
        assert_eq!(
            hist_stats(&reg, "cortex_ingest_storage_reader_pusher_timeseries_per_flush"),
            (2, 0.0)
        );
        let empty_proc_count = reg
            .gather()
            .iter()
            .find(|mf| mf.get_name() == "cortex_ingest_storage_reader_pusher_processing_time_seconds")
            .and_then(|mf| {
                mf.get_metric()
                    .iter()
                    .find(|mt| mt.get_label().iter().any(|l| l.get_value() == "empty"))
                    .map(|mt| mt.get_histogram().get_sample_count())
            })
            .unwrap_or(0);
        assert_eq!(empty_proc_count, 2);
    }
}

// ---------------------------------------------------------------------------
// 7. [M20-09] Multi-shard Push observes appender_add_duration & commit_duration only on success
// ---------------------------------------------------------------------------
#[tokio::test]
async fn m20_09_multi_shard_push_does_not_observe_appender_durations_on_error() {
    let dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    cfg.ring.kvstore.store = "inmemory".to_string();
    cfg.ring.replication_factor = 1;
    cfg.ring.num_tokens = 1;

    // Populate KV ring with a remote instance owning 128 evenly-spaced tokens so
    // verify_ring_ownership fails across a 600-series multi-shard push.
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let remote_tokens: Vec<u32> = (0..128u32).map(|i| i.wrapping_mul(u32::MAX / 128)).collect();
    kv.cas(
        "collectors/ring",
        Box::new(move |in_val| {
            let mut desc = in_val
                .and_then(|a| a.downcast_ref::<Desc>())
                .cloned()
                .unwrap_or_default();
            desc.add_ingester(
                "other-instance",
                "127.0.0.1:9095",
                "",
                remote_tokens.clone(),
                InstanceState::Active,
                Some(SystemTime::now()),
                false,
                None,
                std::collections::HashMap::new(),
            );
            Ok(CasOutcome::Update(Box::new(desc)))
        }),
    )
    .await
    .unwrap();

    let (ing, _ring) = prepare_ingester_with_kv_and_overrides(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(default_limits_test_config()),
        kv,
        Some(&reg),
    );
    start_and_wait_healthy(&ing).await;
    ing.verify_ring_ownership.store(true, Ordering::Relaxed);

    let ts = now_ms();
    // 600 series (>= 512 multi-shard threshold, < 4096 classic sharded fast-path).
    let series: Vec<_> = (0..600)
        .map(|i| simple_series(&format!("m_{i}"), ts, i as f64))
        .collect();
    let req = cortexpb::WriteRequest {
        timeseries: series,
        ..Default::default()
    };
    let err = ing
        .push_grpc_raw("tenant-m20-09", Bytes::from(req.encode_to_vec()))
        .await
        .expect_err("ownership failure must abort multi-shard push");
    assert!(!err.is_soft());

    assert_eq!(
        hist_stats(&reg, "cortex_ingester_tsdb_appender_add_duration_seconds").0,
        0,
        "appender_add_duration must not be observed on error"
    );
    assert_eq!(
        hist_stats(&reg, "cortex_ingester_tsdb_appender_commit_duration_seconds").0,
        0,
        "appender_commit_duration must not be observed on error"
    );
    ing.stop().await.unwrap();
}

// ---------------------------------------------------------------------------
// 8. [W20-SDW-09] ActiveSeries active_native_histogram_buckets wrapping arithmetic
// ---------------------------------------------------------------------------
#[test]
fn w20_sdw_09_active_native_histogram_buckets_wrapping_arithmetic() {
    let tracker = ActiveSeries::new(Vec::new(), Duration::from_secs(600));
    let now = SystemTime::now();

    // Place 3 series in the same stripe (NUM_STRIPES = 4096 -> ref_ids 1, 4097, 8193).
    // 3 * 2_000_000_000 = 6_000_000_000 wraps u32 without panicking in debug mode.
    let p1 = [("__name__", "h"), ("id", "1")];
    let p2 = [("__name__", "h"), ("id", "2")];
    let p3 = [("__name__", "h"), ("id", "3")];
    tracker.update_series(&p1, 1, 1, now, 2_000_000_000, false);
    tracker.update_series(&p2, 2, 4097, now, 2_000_000_000, false);
    tracker.update_series(&p3, 3, 8193, now, 2_000_000_000, false);

    let (_, _, _, buckets_after_add) = tracker.active();
    assert_eq!(buckets_after_add, (6_000_000_000u64 as u32) as usize);

    // Transition all 3 back to float (-1 buckets): modular subtraction must unwind cleanly to 0.
    tracker.update_series(&p1, 1, 1, now, -1, false);
    tracker.update_series(&p2, 2, 4097, now, -1, false);
    tracker.update_series(&p3, 3, 8193, now, -1, false);

    let (_, _, _, buckets_after_sub) = tracker.active();
    assert_eq!(buckets_after_sub, 0);
}

// ---------------------------------------------------------------------------
// 9. [M20-07 & W20-SDW-11 items 2 & 3] RefLeaks wiring, negative limit, extreme timestamps
// ---------------------------------------------------------------------------
#[tokio::test]
async fn m20_07_and_w20_sdw_11_ref_leaks_compat_limit_and_extreme_timestamps() {
    // M20-07: before_reuse_period < 0 validation + Ingester::set_ref_leaks_tracker on gRPC Push.
    assert!(InstrumentRefLeaksConfig::validate_before_reuse_period_ns(-1)
        .unwrap_err()
        .contains("before-reuse-period must be positive"));
    assert!(InstrumentRefLeaksConfig::validate_raw(100.0, -1).is_err());

    let dir = TempDir::new().unwrap();
    let reg = Registry::new();
    let (mut cfg, blocks_cfg, ingest_cfg) = default_ingester_test_config(dir.path());
    cfg.push_grpc_method_enabled = true;
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let (ing, _ring) = prepare_ingester_with_kv_and_overrides(
        cfg,
        blocks_cfg,
        ingest_cfg,
        Overrides::new(default_limits_test_config()),
        kv,
        Some(&reg),
    );
    let rl_cfg = InstrumentRefLeaksConfig {
        percentage: 100.0,
        before_reuse_period: Duration::ZERO,
        max_inflight_instrumented_bytes: 0,
    };
    let tracker = rl_cfg.tracker(Some(&reg));
    ing.set_ref_leaks_tracker(tracker.clone());

    start_and_wait_healthy(&ing).await;
    let svc = IngesterService::new(Arc::clone(&ing)).into_server();
    let req = cortexpb::WriteRequest {
        timeseries: vec![simple_series("rl_metric", now_ms(), 1.0)],
        ..Default::default()
    };
    let http_req = http::Request::builder()
        .method("POST")
        .uri("/cortex.Ingester/Push")
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("x-scope-orgid", "tenant-rl")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(unframed(
            &req.encode_to_vec(),
        )))))
        .unwrap();
    let resp = svc.oneshot(http_req).await.unwrap();
    assert_eq!(resp.status(), http::StatusCode::OK);
    assert_eq!(tracker.instrumented_buffers_total(), 1.0);
    assert_eq!(tracker.inflight_instrumented_bytes(), 0.0);

    // W20-SDW-11 item 3: client::compat negative limit guard.
    let (hints, _) = from_metrics_for_label_matchers_request(
        &ingesterpb::MetricsForLabelMatchersRequest {
            limit: -1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(hints.limit, 0);
    let (_, _, ln_hints, _) = from_label_names_request(&ingesterpb::LabelNamesRequest {
        limit: -1,
        ..Default::default()
    })
    .unwrap();
    assert!(ln_hints.is_none());

    // W20-SDW-11 item 2: proleptic Gregorian UTC fallback on extreme i64 timestamps.
    assert_eq!(
        format_timestamp_ms_go_time_string(i64::MIN + 1),
        "-292275055-05-16 16:47:04.193 +0000 UTC"
    );
    assert_eq!(
        format_timestamp_ms_go_time_string(8_334_632_851_200_000),
        "266084-01-01 00:00:00 +0000 UTC"
    );

    // Verify /ingester/tenants renders proleptic Gregorian UTC timestamp for i64::MIN + 1.
    let extreme_req = cortexpb::WriteRequest {
        timeseries: vec![simple_series("extreme_ts", i64::MIN + 1, 1.0)],
        ..Default::default()
    };
    ing.push_grpc("tenant-extreme", &extreme_req).await.unwrap();
    let app = ingester_router(Arc::clone(&ing));
    let tenants_resp = app
        .oneshot(
            http::Request::builder()
                .uri("/ingester/tenants")
                .header("accept", "text/html")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tenants_resp.status(), http::StatusCode::OK);
    let body = axum::body::to_bytes(tenants_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("-292275055-05-16T16:47:04Z (-9223372036854775807)"),
        "expected proleptic Gregorian UTC formatting in /ingester/tenants, got: {body_str}"
    );
    ing.stop().await.unwrap();
}
