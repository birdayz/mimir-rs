//! Regression tests for Wave 15 Track 4 gRPC wire/compression/deadline fixes (RKW-01..RKW-03).

use std::io::{Read as _, Write as _};
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use http::{HeaderMap, Method, StatusCode};
use http_body_util::BodyExt;
use mimir_cmd::grpc::{
    GrpcCompressionEchoService, HealthService, HttpGrpcBridge, HttpGrpcStatusFixService,
};
use mimir_cmd::modules::{ModuleStates, StateCell};
use mimir_cmd::server::body::TrackedBody;
use mimir_common::services::State;
use mimir_config::{
    BlocksStorageConfig, IngestStorageConfig, IngesterConfig, LimitsConfig, Overrides,
};
use mimir_ingester::grpc::{RpcDeadlineContext, SnappyDecompressService};
use mimir_ingester::{Ingester, IngesterService};
use mimir_proto::httpgrpcpb::http_server::HttpServer;
use mimir_proto::prost::Message as _;
use mimir_proto::{cortexpb, httpgrpcpb, ingesterpb};
use tonic::codec::CompressionEncoding;
use tower::ServiceExt;

struct SingleChunkBody(Option<Bytes>);

impl hyper::body::Body for SingleChunkBody {
    type Data = Bytes;
    type Error = tonic::Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(
            self.0
                .take()
                .map(|b| Ok(hyper::body::Frame::data(b))),
        )
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
}

fn frame_grpc(flag: u8, payload: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flag);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Bytes::from(out)
}

fn compress_payload(enc: &str, payload: &[u8]) -> Vec<u8> {
    match enc {
        "gzip" => {
            let mut gz = GzEncoder::new(Vec::new(), Compression::default());
            gz.write_all(payload).unwrap();
            gz.finish().unwrap()
        }
        "zstd" => zstd::stream::encode_all(payload, 3).unwrap(),
        "snappy" => mimir_proto::snappy::compress_framed(payload).unwrap(),
        "s2" => {
            let mut out = mimir_proto::snappy::compress_framed(payload).unwrap();
            if out.len() >= 10 {
                out[4..10].copy_from_slice(b"S2sTwO");
            }
            out
        }
        other => panic!("unsupported test encoding: {other}"),
    }
}

fn decompress_payload(enc: &str, payload: &[u8]) -> Vec<u8> {
    match enc {
        "gzip" => {
            let mut dec = GzDecoder::new(payload);
            let mut out = Vec::new();
            dec.read_to_end(&mut out).unwrap();
            out
        }
        "zstd" => zstd::stream::decode_all(payload).unwrap(),
        "snappy" => mimir_proto::snappy::decompress_framed(payload).unwrap(),
        "s2" => {
            let mut out = Vec::new();
            mimir_proto::s2::decompress_framed_into(payload, &mut out, 8 << 20).unwrap();
            out
        }
        other => panic!("unsupported test encoding: {other}"),
    }
}

async fn extract_grpc_status(resp: http::Response<tonic::body::BoxBody>) -> tonic::Status {
    if let Some(st) = tonic::Status::from_header_map(resp.headers()) {
        return st;
    }
    let collected = resp.into_body().collect().await.unwrap();
    if let Some(trailers) = collected.trailers() {
        if let Some(st) = tonic::Status::from_header_map(trailers) {
            return st;
        }
    }
    panic!("expected grpc-status in headers or trailers");
}

fn make_health_svc(
    max_recv: usize,
    max_send: usize,
) -> GrpcCompressionEchoService<SnappyDecompressService<HealthService>> {
    let modules = ModuleStates::new();
    let cell = StateCell::new(State::Running);
    modules.add("server", cell.state_fn());
    let shutdown = Arc::new(AtomicBool::new(false));
    GrpcCompressionEchoService::new_with_limits(
        SnappyDecompressService::new(
            HealthService::new(shutdown, modules)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            max_recv,
        ),
        max_recv,
        max_send,
    )
}

fn make_httpgrpc_svc(
    router: Router,
    max_recv: usize,
    max_send: usize,
) -> GrpcCompressionEchoService<
    HttpGrpcStatusFixService<SnappyDecompressService<HttpServer<HttpGrpcBridge>>>,
> {
    GrpcCompressionEchoService::new_with_limits(
        HttpGrpcStatusFixService::new(SnappyDecompressService::new(
            HttpServer::new(HttpGrpcBridge::new(router))
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            max_recv,
        )),
        max_recv,
        max_send,
    )
}

fn make_test_ingester(dir: &std::path::Path) -> Arc<Ingester> {
    let mut cfg = IngesterConfig::default();
    cfg.ring.instance_id = "rkw-ing-test".to_string();
    cfg.push_grpc_method_enabled = true;
    let mut blocks_cfg = BlocksStorageConfig::default();
    blocks_cfg.tsdb.dir = dir.to_string_lossy().into_owned();
    let ing = Ingester::new(
        cfg,
        blocks_cfg,
        IngestStorageConfig::default(),
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
    ing
}

fn make_ingester_svc(
    ing: Arc<Ingester>,
    max_recv: usize,
    max_send: usize,
) -> GrpcCompressionEchoService<mimir_ingester::grpc::IngesterZeroCopyServer> {
    GrpcCompressionEchoService::new_with_limits(
        IngesterService::new(ing)
            .into_server()
            .max_decoding_message_size(max_recv)
            .max_encoding_message_size(usize::MAX)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd),
        max_recv,
        max_send,
    )
}

#[tokio::test]
async fn rkw01_gzip_and_zstd_zip_bomb_returns_resource_exhausted_across_all_services() {
    let max_recv = 4096usize;
    let max_send = 4 << 20;

    // 512 KiB of repeated bytes compresses to a few hundred bytes (under max_recv=4096 wire),
    // but decompresses far past max_recv=4096.
    let huge = vec![b'A'; 512 * 1024];

    let tmp = tempfile::tempdir().unwrap();
    let ing = make_test_ingester(tmp.path());
    let ing_svc = make_ingester_svc(Arc::clone(&ing), max_recv, max_send);
    let health_svc = make_health_svc(max_recv, max_send);
    let http_svc = make_httpgrpc_svc(Router::new().route("/ok", get(|| async { "ok" })), max_recv, max_send);

    for enc in ["gzip", "zstd"] {
        let comp = compress_payload(enc, &huge);
        assert!(comp.len() < max_recv, "{enc} compressed size {} must be < max_recv", comp.len());
        let framed = frame_grpc(1, &comp);

        // 1. Health
        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/grpc.health.v1.Health/Check")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", enc)
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed.clone()))))
            .unwrap();
        let st = extract_grpc_status(health_svc.clone().oneshot(req).await.unwrap()).await;
        assert_eq!(st.code(), tonic::Code::ResourceExhausted, "health {enc}: {st:?}");
        assert!(
            st.message().contains("received message after decompression larger than max 4096"),
            "health {enc} msg: {}",
            st.message()
        );

        // 2. httpgrpc.HTTP
        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/httpgrpc.HTTP/Handle")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", enc)
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed.clone()))))
            .unwrap();
        let st = extract_grpc_status(http_svc.clone().oneshot(req).await.unwrap()).await;
        assert_eq!(st.code(), tonic::Code::ResourceExhausted, "httpgrpc {enc}: {st:?}");
        assert!(
            st.message().contains("received message after decompression larger than max 4096"),
            "httpgrpc {enc} msg: {}",
            st.message()
        );

        // 3. Ingester Push & QueryStream
        for uri in ["/cortex.Ingester/Push", "/cortex.Ingester/QueryStream"] {
            let req = http::Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .header("grpc-encoding", enc)
                .header("x-scope-orgid", "t1")
                .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed.clone()))))
                .unwrap();
            let st = extract_grpc_status(ing_svc.clone().oneshot(req).await.unwrap()).await;
            assert_eq!(st.code(), tonic::Code::ResourceExhausted, "{uri} {enc}: {st:?}");
            assert!(
                st.message().contains("received message after decompression larger than max 4096"),
                "{uri} {enc} msg: {}",
                st.message()
            );
        }
    }
}

#[tokio::test]
async fn rkw01_oversized_identity_recv_and_send_return_resource_exhausted_not_out_of_range() {
    let max_recv = 256usize;
    let max_send = 256usize;

    let health_svc = make_health_svc(max_recv, max_send);
    let oversized_req = frame_grpc(0, &vec![b'x'; 500]);
    let req = http::Request::builder()
        .method(Method::POST)
        .uri("/grpc.health.v1.Health/Check")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(oversized_req.clone()))))
        .unwrap();
    let st = extract_grpc_status(health_svc.oneshot(req).await.unwrap()).await;
    assert_eq!(st.code(), tonic::Code::ResourceExhausted, "got {st:?}");
    assert!(st.message().contains("received message larger than max (500 vs. 256)"));

    let tmp = tempfile::tempdir().unwrap();
    let ing = make_test_ingester(tmp.path());
    let ing_svc = make_ingester_svc(Arc::clone(&ing), max_recv, max_send);
    let req = http::Request::builder()
        .method(Method::POST)
        .uri("/cortex.Ingester/Push")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("x-scope-orgid", "t1")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(oversized_req))))
        .unwrap();
    let st = extract_grpc_status(ing_svc.oneshot(req).await.unwrap()).await;
    assert_eq!(st.code(), tonic::Code::ResourceExhausted, "got {st:?}");
    assert!(st.message().contains("received message larger than max (500 vs. 256)"));

    // Oversized send response on httpgrpc.HTTP/Handle with incompressible data -> ResourceExhausted.
    let mut pseudo_random = Vec::with_capacity(2048);
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..2048 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        pseudo_random.push((x >> 56) as u8);
    }
    let payload_bytes = Bytes::from(pseudo_random);
    let router = Router::new().route(
        "/big",
        get(move || {
            let p = payload_bytes.clone();
            async move { p }
        }),
    );
    let http_svc = make_httpgrpc_svc(router, 4 << 20, max_send);
    let rpc_req = httpgrpcpb::HttpRequest {
        method: "GET".to_string(),
        url: "/big".to_string(),
        body: Bytes::new(),
        headers: Vec::new(),
    }
    .encode_to_vec();

    for enc in [None, Some("gzip"), Some("s2")] {
        let mut b = http::Request::builder()
            .method(Method::POST)
            .uri("/httpgrpc.HTTP/Handle")
            .header(http::header::CONTENT_TYPE, "application/grpc");
        let body = match enc {
            None => frame_grpc(0, &rpc_req),
            Some(e) => {
                b = b.header("grpc-encoding", e);
                frame_grpc(1, &compress_payload(e, &rpc_req))
            }
        };
        let req = b
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(body))))
            .unwrap();
        let st = extract_grpc_status(http_svc.clone().oneshot(req).await.unwrap()).await;
        assert_eq!(st.code(), tonic::Code::ResourceExhausted, "send {enc:?}: {st:?}");
        assert!(
            st.message().contains("trying to send message larger than max"),
            "send {enc:?} msg: {}",
            st.message()
        );
    }
}

#[tokio::test]
async fn rkw02_post_compression_max_send_s2_ratio_over_5x_and_unconditional_echo() {
    let max_send = 1024usize;
    let big_repetitive = "mimir_series_value_payload_0123456789abcdef_".repeat(400); // >16 KiB
    let router = Router::new().route(
        "/rep",
        get({
            let body = big_repetitive.clone();
            move || async move { body }
        }),
    );
    let http_svc = make_httpgrpc_svc(router, 4 << 20, max_send);
    let rpc_req = httpgrpcpb::HttpRequest {
        method: "GET".to_string(),
        url: "/rep".to_string(),
        body: Bytes::new(),
        headers: Vec::new(),
    }
    .encode_to_vec();

    // Uncompressed exceeds max_send=1024 -> ResourceExhausted.
    let req_id = http::Request::builder()
        .method(Method::POST)
        .uri("/httpgrpc.HTTP/Handle")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(frame_grpc(
            0, &rpc_req,
        )))))
        .unwrap();
    let st_id = extract_grpc_status(http_svc.clone().oneshot(req_id).await.unwrap()).await;
    assert_eq!(st_id.code(), tonic::Code::ResourceExhausted);

    // Compressed under s2, snappy, gzip, zstd shrinks far below max_send=1024 and succeeds,
    // even when grpc-accept-encoding omits the request encoding (unconditional echo per grpc-go).
    for enc in ["s2", "snappy", "gzip", "zstd"] {
        let comp_req = compress_payload(enc, &rpc_req);
        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/httpgrpc.HTTP/Handle")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", enc)
            // Deliberately advertise a different encoding to test unconditional echo!
            .header("grpc-accept-encoding", if enc == "gzip" { "zstd" } else { "gzip" })
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(frame_grpc(
                1, &comp_req,
            )))))
            .unwrap();
        let resp = http_svc.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "enc={enc}");
        assert_eq!(
            resp.headers()
                .get("grpc-encoding")
                .and_then(|v| v.to_str().ok()),
            Some(enc),
            "grpc-go unconditionally echoes grpc-encoding={enc}"
        );
        let collected = resp.into_body().collect().await.unwrap();
        let trailers = collected.trailers().cloned().unwrap_or_else(HeaderMap::new);
        assert!(
            trailers
                .get("grpc-status")
                .is_none_or(|v| v == "0"),
            "enc={enc} trailers={trailers:?}"
        );
        let wire = collected.to_bytes();
        assert!(wire.len() >= 5 && wire[0] == 1);
        let frame_len = u32::from_be_bytes([wire[1], wire[2], wire[3], wire[4]]) as usize;
        assert!(
            frame_len <= max_send,
            "enc={enc} compressed frame_len={frame_len} must be <= max_send={max_send}"
        );
        let comp_payload = &wire[5..5 + frame_len];
        if enc == "s2" {
            assert_eq!(&comp_payload[..10], b"\xff\x06\x00\x00S2sTwO");
            assert_eq!(
                comp_payload[10], 0x00,
                "s2 must emit 0x00 compressed block, not 0x01 uncompressed"
            );
            assert!(
                comp_payload.len() * 5 < big_repetitive.len(),
                "s2 compression ratio must exceed 5x (compressed={} vs raw={})",
                comp_payload.len(),
                big_repetitive.len()
            );
        }
        let dec = decompress_payload(enc, comp_payload);
        let http_resp = httpgrpcpb::HttpResponse::decode(dec.as_slice()).unwrap();
        assert_eq!(http_resp.code, 200);
        assert_eq!(http_resp.body.len(), big_repetitive.len());
    }

    // Also verify QueryStream with s2 produces real compressed S2sTwO blocks (>5x ratio)
    // and honors unconditional echo when grpc-accept-encoding omits s2.
    let tmp = tempfile::tempdir().unwrap();
    let ing = make_test_ingester(tmp.path());
    let now_ms = 1_700_000_000_000i64;
    let mut series = Vec::new();
    for idx in 0..64 {
        series.push(cortexpb::TimeSeries {
            labels: vec![
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"__name__"),
                    value: Bytes::from_static(b"s2_ratio_metric"),
                },
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"cluster"),
                    value: Bytes::from_static(b"prod-us-east-1-repeated-cluster-label-value"),
                },
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"namespace"),
                    value: Bytes::from_static(b"observability-mimir-ingester-production-ns"),
                },
                cortexpb::LabelPair {
                    name: Bytes::from_static(b"pod"),
                    value: Bytes::from(format!("mimir-ingester-zone-a-pod-{idx:04}")),
                },
            ],
            samples: vec![cortexpb::Sample {
                value: 100.0 + idx as f64,
                timestamp_ms: now_ms,
            }],
            ..Default::default()
        });
    }
    let wr = cortexpb::WriteRequest {
        timeseries: series,
        ..Default::default()
    };
    ing.push_grpc("tenant-s2", &wr).await.unwrap();

    let ing_svc = make_ingester_svc(ing, 4 << 20, 4 << 20);
    let q_req = ingesterpb::QueryRequest {
        start_timestamp_ms: now_ms - 10_000,
        end_timestamp_ms: now_ms + 10_000,
        matchers: vec![ingesterpb::LabelMatcher {
            r#type: ingesterpb::MatchType::Equal as i32,
            name: "__name__".into(),
            value: "s2_ratio_metric".into(),
        }],
        streaming_chunks_batch_size: 64,
    }
    .encode_to_vec();
    let req = http::Request::builder()
        .method(Method::POST)
        .uri("/cortex.Ingester/QueryStream")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .header("grpc-encoding", "s2")
        .header("grpc-accept-encoding", "gzip")
        .header("x-scope-orgid", "tenant-s2")
        .body(tonic::body::BoxBody::new(SingleChunkBody(Some(frame_grpc(
            1,
            &compress_payload("s2", &q_req),
        )))))
        .unwrap();
    let resp = ing_svc.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("grpc-encoding")
            .and_then(|v| v.to_str().ok()),
        Some("s2")
    );
    let wire = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(wire[0], 1);
    let frame_len = u32::from_be_bytes([wire[1], wire[2], wire[3], wire[4]]) as usize;
    let comp_payload = &wire[5..5 + frame_len];
    assert_eq!(&comp_payload[..10], b"\xff\x06\x00\x00S2sTwO");
    assert_eq!(comp_payload[10], 0x00, "QueryStream s2 must emit 0x00 compressed block");
    let raw = decompress_payload("s2", comp_payload);
    assert!(
        comp_payload.len() * 5 < raw.len(),
        "QueryStream s2 ratio must exceed 5x (comp={} vs raw={})",
        comp_payload.len(),
        raw.len()
    );
    let qs = ingesterpb::QueryStreamResponse::decode(raw.as_slice()).unwrap();
    assert_eq!(qs.streaming_series.len(), 64);
}

#[tokio::test]
async fn rkw03_tracked_body_ready_loop_marks_rpc_deadline_context_expired() {
    struct InfiniteReadyBody;
    impl hyper::body::Body for InfiniteReadyBody {
        type Data = Bytes;
        type Error = tonic::Status;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(b"data")))))
        }
    }

    let deadline = Instant::now() + Duration::from_millis(25);
    let ctx = RpcDeadlineContext::new(Some(deadline));
    let cb_ctx = Arc::clone(&ctx);
    let mut body = TrackedBody::new(InfiniteReadyBody).deadline_with_cb(Some(deadline), move || {
        cb_ctx.mark_expired();
        "grpc deadline exceeded".into()
    });

    let mut saw_err = false;
    for _ in 0..100 {
        match std::future::poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut body), cx)).await {
            Some(Ok(_)) => tokio::time::sleep(Duration::from_millis(5)).await,
            Some(Err(_)) => {
                saw_err = true;
                break;
            }
            None => break,
        }
    }
    assert!(saw_err, "TrackedBody must abort ready-loop stream at deadline");
    assert!(ctx.is_expired(), "RpcDeadlineContext must be marked expired");
}
