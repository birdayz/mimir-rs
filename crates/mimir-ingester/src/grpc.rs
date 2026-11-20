//! Tonic `cortex.Ingester` gRPC server implementation.
//!
//! Ported from `pkg/ingester/ingester_push.go`, `ingester_query.go`, `ingester_search.go`.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{Future, Stream};
use mimir_common::grpc_log::{GrpcErrorLog, GrpcErrorLogEntry};
use mimir_common::tenant::{extract_org_id_from_grpc_metadata, parse_single_tenant_id};
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use tonic::codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::{Request, Response, Status};

use crate::activity::{request_activity, ActivityRequest};
use crate::errors::IngesterError;
use crate::ingester::{ActivityGuard, Ingester};
use crate::query::{build_search_hints, RawQueryStreamBox, ReadTapPermit, READ_TAP_ADMITTED};

pub struct IngesterService {
    ingester: Arc<Ingester>,
}

impl IngesterService {
    pub fn new(ingester: Arc<Ingester>) -> Self {
        Self { ingester }
    }

    pub fn ingester(&self) -> &Arc<Ingester> {
        &self.ingester
    }

    pub fn into_server(self) -> IngesterZeroCopyServer {
        let ingester = Arc::clone(&self.ingester);
        IngesterZeroCopyServer {
            inner: ingesterpb::ingester_server::IngesterServer::new(self)
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
            ingester,
            accept_compression: [None; 4],
            accept_compression_len: 0,
            send_compression: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }
}

#[derive(Clone, Default)]
struct RawPushDecoder;

impl Decoder for RawPushDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let rem = src.remaining();
        Ok(Some(src.copy_to_bytes(rem)))
    }
}

#[derive(Clone, Default)]
struct WriteResponseEncoder;

impl Encoder for WriteResponseEncoder {
    type Item = cortexpb::WriteResponse;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        item.encode(dst)
            .map_err(|e| Status::internal(e.to_string()))
    }
}

#[derive(Clone, Default)]
struct RawPushCodec;

impl Codec for RawPushCodec {
    type Encode = cortexpb::WriteResponse;
    type Decode = Bytes;
    type Encoder = WriteResponseEncoder;
    type Decoder = RawPushDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        WriteResponseEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawPushDecoder
    }
}

#[derive(Clone, Default)]
struct QueryRequestDecoder;

impl Decoder for QueryRequestDecoder {
    type Item = ingesterpb::QueryRequest;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        ingesterpb::QueryRequest::decode(src).map(Some).map_err(|e| {
            Status::internal(format!(
                "grpc: failed to unmarshal the received message: {e}"
            ))
        })
    }
}

#[derive(Clone, Default)]
struct RawBytesEncoder;

impl Encoder for RawBytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put_slice(&item);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct RawQueryStreamCodec;

impl Codec for RawQueryStreamCodec {
    type Encode = Bytes;
    type Decode = ingesterpb::QueryRequest;
    type Encoder = RawBytesEncoder;
    type Decoder = QueryRequestDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        QueryRequestDecoder
    }
}

#[derive(Clone, Default)]
struct MetricsForLabelMatchersRequestDecoder;

impl Decoder for MetricsForLabelMatchersRequestDecoder {
    type Item = ingesterpb::MetricsForLabelMatchersRequest;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        ingesterpb::MetricsForLabelMatchersRequest::decode(src)
            .map(Some)
            .map_err(|e| {
                Status::internal(format!(
                    "grpc: failed to unmarshal the received message: {e}"
                ))
            })
    }
}

#[derive(Clone, Default)]
struct RawMetricsForLabelMatchersCodec;

impl Codec for RawMetricsForLabelMatchersCodec {
    type Encode = Bytes;
    type Decode = ingesterpb::MetricsForLabelMatchersRequest;
    type Encoder = RawBytesEncoder;
    type Decoder = MetricsForLabelMatchersRequestDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        MetricsForLabelMatchersRequestDecoder
    }
}

/// `cortex.Ingester` Tonic server wrapper that decodes `/cortex.Ingester/Push`
/// directly from raw gRPC frame bytes using the zero-copy `WriteRequestScratch` decoder.
#[derive(Clone)]
pub struct IngesterZeroCopyServer {
    inner: ingesterpb::ingester_server::IngesterServer<IngesterService>,
    ingester: Arc<Ingester>,
    accept_compression: [Option<CompressionEncoding>; 4],
    accept_compression_len: u8,
    send_compression: Option<CompressionEncoding>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl IngesterZeroCopyServer {
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        let idx = self.accept_compression_len as usize;
        if idx < self.accept_compression.len() {
            self.accept_compression[idx] = Some(encoding);
            self.accept_compression_len += 1;
        }
        self.inner = self.inner.accept_compressed(encoding);
        self
    }

    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.send_compression = Some(encoding);
        self.inner = self.inner.send_compressed(encoding);
        self
    }

    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.max_decoding_message_size = Some(limit);
        // Keep Tonic's internal limit unbounded so pre-Tonic / post-compression enforcement emits
        // grpc-go's `Code::ResourceExhausted` instead of Tonic's `Code::OutOfRange`.
        self.inner = self.inner.max_decoding_message_size(usize::MAX);
        self
    }

    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.max_encoding_message_size = Some(limit);
        self.inner = self.inner.max_encoding_message_size(usize::MAX);
        self
    }
}

impl tonic::server::NamedService for IngesterZeroCopyServer {
    const NAME: &'static str = "cortex.Ingester";
}

/// Per-RPC deadline context attached by `grpc_server.rs` into `Request::extensions()`
/// and propagated via `RPC_DEADLINE_CONTEXT` so circuit-breaker/reactive-limiter drop guards
/// and `enforce_read_consistency` can distinguish `grpc-timeout` expiration from client
/// cancellation (`RST_STREAM`).
#[derive(Debug)]
pub struct RpcDeadlineContext {
    pub deadline: Option<std::time::Instant>,
    pub expired: AtomicBool,
}

impl RpcDeadlineContext {
    pub fn new(deadline: Option<std::time::Instant>) -> Arc<Self> {
        Arc::new(Self {
            deadline,
            expired: AtomicBool::new(false),
        })
    }

    #[inline]
    pub fn mark_expired(&self) {
        self.expired.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        self.expired.load(Ordering::Acquire)
    }

    #[inline]
    pub fn remaining(&self) -> Option<std::time::Duration> {
        self.deadline
            .map(|d| d.saturating_duration_since(std::time::Instant::now()))
    }

    #[inline]
    pub fn has_timed_out(&self) -> bool {
        self.is_expired()
            || self
                .deadline
                .is_some_and(|d| std::time::Instant::now() >= d)
    }
}

tokio::task_local! {
    pub static RPC_DEADLINE_CONTEXT: Arc<RpcDeadlineContext>;
}

/// Returns the current task's `RpcDeadlineContext`, if any.
pub fn current_rpc_deadline_context() -> Option<Arc<RpcDeadlineContext>> {
    RPC_DEADLINE_CONTEXT.try_with(Arc::clone).ok()
}

/// Intersects `configured_timeout` with any remaining `grpc-timeout` deadline on the current RPC.
pub fn effective_read_consistency_timeout(
    configured_timeout: std::time::Duration,
) -> std::time::Duration {
    match current_rpc_deadline_context().and_then(|ctx| ctx.remaining()) {
        Some(rem) if configured_timeout.is_zero() => rem,
        Some(rem) => configured_timeout.min(rem),
        None => configured_timeout,
    }
}

async fn scope_deadline_ctx<F, T>(ctx: Option<Arc<RpcDeadlineContext>>, fut: F) -> T
where
    F: Future<Output = T>,
{
    match ctx {
        Some(c) => RPC_DEADLINE_CONTEXT.scope(c, fut).await,
        None => fut.await,
    }
}

/// Response extension inserted ONLY when `TapHandle` (`RPCCallStarting` in
/// `dskit/server/limits.go`) rejects an RPC before execution, so `grpc_server::RpcGuard`
/// suppresses metrics/logs/traces for genuine tap rejections without dropping
/// post-TapHandle `Unavailable` responses (instance limits, reactive limiter timeouts, etc.).
#[derive(Clone, Copy, Debug, Default)]
pub struct TapRejected;

/// Pre-acquired `TapHandle` permit forwarded from `grpc_server.rs` into
/// `IngesterZeroCopyServer` via `http::Request::extensions_mut()` so `TapHandle` runs
/// before `auth` / `cluster_validation` / `RpcGuard` without double-acquiring permits.
#[derive(Clone)]
pub enum PreAcquiredTap {
    Push(Arc<parking_lot::Mutex<Option<crate::push::PushTapPermit>>>),
    Read(Arc<parking_lot::Mutex<Option<ReadTapPermit>>>),
}

#[derive(Clone)]
struct ReadTapSlotContext {
    slot: Option<Arc<parking_lot::Mutex<Option<ReadTapGuard>>>>,
    deadline_ctx: Option<Arc<RpcDeadlineContext>>,
}

static ONLY_REPLICA_HEADERS: std::sync::LazyLock<http::HeaderMap> =
    std::sync::LazyLock::new(|| {
        let mut h = http::HeaderMap::new();
        h.insert(
            crate::query::ONLY_REPLICA_GRPC_MD_KEY,
            http::HeaderValue::from_static("true"),
        );
        h
    });
static EMPTY_HEADERS: std::sync::LazyLock<http::HeaderMap> =
    std::sync::LazyLock::new(http::HeaderMap::new);

#[allow(clippy::result_large_err)]
async fn prepare_read_reactive_for_slot(
    ingester: &Ingester,
    md: &tonic::metadata::MetadataMap,
    deadline_ctx: Option<&RpcDeadlineContext>,
    read_tap_slot: Option<&parking_lot::Mutex<Option<ReadTapGuard>>>,
    ext: &mut http::Extensions,
) -> Result<(), Status> {
    let headers: &http::HeaderMap = if crate::query::is_only_replica_metadata(md) {
        &ONLY_REPLICA_HEADERS
    } else {
        &EMPTY_HEADERS
    };
    match ingester.prepare_read_reactive(headers, deadline_ctx).await {
        Ok(Some(rp)) => {
            if let Some(slot) = read_tap_slot {
                if let Some(ref mut guard) = *slot.lock() {
                    if let Some(ref mut tap) = guard.tap {
                        tap.set_reactive_permit(rp);
                    }
                }
            }
        }
        Ok(None) => {}
        Err(e) => return Err(Status::unavailable(e.to_string())),
    }
    if let Some(slot) = read_tap_slot {
        if let Some(ref mut guard) = *slot.lock() {
            if let Some(ref mut tap) = guard.tap {
                ext.insert(TapDeadlineFlag(tap.deadline_exceeded_flag()));
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct TapDeadlineFlag(pub(crate) Arc<AtomicBool>);

struct ReadTapGuard {
    tap: Option<ReadTapPermit>,
    deadline_ctx: Option<Arc<RpcDeadlineContext>>,
}

impl ReadTapGuard {
    fn finish(&mut self, canceled: bool) {
        if let Some(tap) = self.tap.take() {
            tap.finish(canceled);
        }
    }

    fn take_for_body(&mut self) -> Option<ReadTapPermit> {
        self.tap.take()
    }
}

impl Drop for ReadTapGuard {
    fn drop(&mut self) {
        if let Some(tap) = self.tap.take() {
            let timed_out = self
                .deadline_ctx
                .as_ref()
                .is_some_and(|c| c.is_expired());
            if timed_out {
                tap.deadline_exceeded_flag().store(true, Ordering::Release);
                tap.finish(false);
            } else {
                tap.finish(true);
            }
        }
    }
}

struct ReadTapDropBody {
    inner: tonic::body::BoxBody,
    permit: Option<ReadTapPermit>,
    deadline_ctx: Option<Arc<RpcDeadlineContext>>,
}

impl hyper::body::Body for ReadTapDropBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let poll = Pin::new(&mut self.inner).poll_frame(cx);
        let (done, deadline_exceeded) = match &poll {
            Poll::Ready(None) => (true, false),
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    let is_deadline = trailers
                        .get("grpc-status")
                        .and_then(|v| v.to_str().ok())
                        == Some("4");
                    (true, is_deadline)
                } else {
                    (self.inner.is_end_stream(), false)
                }
            }
            Poll::Ready(Some(Err(status))) => {
                (true, status.code() == tonic::Code::DeadlineExceeded)
            }
            Poll::Pending => (false, false),
        };
        if done {
            if let Some(permit) = self.permit.take() {
                if deadline_exceeded {
                    permit.deadline_exceeded_flag().store(true, Ordering::Release);
                }
                permit.finish(false);
            }
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for ReadTapDropBody {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            let timed_out = self
                .deadline_ctx
                .as_ref()
                .is_some_and(|c| c.is_expired());
            if timed_out {
                permit.deadline_exceeded_flag().store(true, Ordering::Release);
                permit.finish(false);
            } else {
                permit.finish(true);
            }
        }
    }
}

struct ActivityStream<S> {
    inner: S,
    _guard: Option<ActivityGuard>,
}

impl<S: Stream + Unpin> Stream for ActivityStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn parse_message_size_header(headers: &http::HeaderMap) -> i64 {
    let mut iter = headers.get_all("message-size").iter();
    let Some(first) = iter.next() else {
        return 0;
    };
    if iter.next().is_some() {
        return 0;
    }
    first
        .to_str()
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

fn unavailable_tap_response(e: IngesterError) -> http::Response<tonic::body::BoxBody> {
    let mut resp = Status::unavailable(e.to_string()).into_http();
    resp.headers_mut().insert(
        "grpc-accept-encoding",
        http::HeaderValue::from_static(GRPC_ACCEPT_ENCODING_VALUE),
    );
    resp.extensions_mut().insert(TapRejected);
    resp
}

struct SingleChunkBody(Option<Bytes>);

impl hyper::body::Body for SingleChunkBody {
    type Data = Bytes;
    type Error = Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.0.take().map(|b| Ok(hyper::body::Frame::data(b))))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FrameDecompressAlgo {
    Identity,
    Gzip,
    Zstd,
}

/// Pre-Tonic non-client-streaming request body wrapper enforcing:
/// - per-frame wire size `msg_len <= max_recv`,
/// - bounded decompression for `gzip` / `zstd`, and
/// - grpc-go `serverStream.RecvMsg` request cardinality (0 frames -> Internal,
///   2+ frames -> validate/decompress frame 2 and fail BEFORE invoking handler).
struct NonClientStreamingFrameBody {
    inner: tonic::body::BoxBody,
    algo: FrameDecompressAlgo,
    max_recv: usize,
    in_buf: BytesMut,
    frames_seen: usize,
    buffered_frame1: Option<Bytes>,
    pending_trailers: Option<http::HeaderMap>,
    eof_seen: bool,
    done: bool,
}

impl NonClientStreamingFrameBody {
    fn new(inner: tonic::body::BoxBody, algo: FrameDecompressAlgo, max_recv: usize) -> Self {
        Self {
            inner,
            algo,
            max_recv,
            in_buf: BytesMut::new(),
            frames_seen: 0,
            buffered_frame1: None,
            pending_trailers: None,
            eof_seen: false,
            done: false,
        }
    }

    #[allow(clippy::result_large_err)]
    fn check_incoming_header(&self) -> Result<(), Status> {
        if self.in_buf.len() >= 5 {
            let msg_len = u32::from_be_bytes([
                self.in_buf[1],
                self.in_buf[2],
                self.in_buf[3],
                self.in_buf[4],
            ]) as usize;
            if msg_len > self.max_recv {
                return Err(Status::resource_exhausted(format!(
                    "grpc: received message larger than max ({msg_len} vs. {})",
                    self.max_recv
                )));
            }
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn drain_buffered_frames(&mut self) -> Result<(), Status> {
        use std::io::Read as _;
        while self.in_buf.len() >= 5 {
            let flag = self.in_buf[0];
            let msg_len = u32::from_be_bytes([
                self.in_buf[1],
                self.in_buf[2],
                self.in_buf[3],
                self.in_buf[4],
            ]) as usize;
            if msg_len > self.max_recv {
                return Err(Status::resource_exhausted(format!(
                    "grpc: received message larger than max ({msg_len} vs. {})",
                    self.max_recv
                )));
            }
            let Some(frame_end) = 5usize.checked_add(msg_len) else {
                return Err(Status::internal("gRPC frame length overflow"));
            };
            if self.in_buf.len() < frame_end {
                break;
            }
            let decoded_frame: Bytes = match self.algo {
                FrameDecompressAlgo::Identity => match flag {
                    0 => self.in_buf.split_to(frame_end).freeze(),
                    1 => {
                        return Err(Status::internal(
                            "grpc: compressed flag set with identity or empty encoding",
                        ));
                    }
                    other => {
                        return Err(Status::internal(format!(
                            "protocol error: received message with invalid compression flag: {other} (valid flags are 0 and 1), while sending request"
                        )));
                    }
                },
                FrameDecompressAlgo::Gzip | FrameDecompressAlgo::Zstd => match flag {
                    0 => self.in_buf.split_to(frame_end).freeze(),
                    1 => {
                        let compressed = &self.in_buf[5..frame_end];
                        let limit = (self.max_recv as u64).saturating_add(1);
                        let mut decompressed =
                            Vec::with_capacity(compressed.len().min(self.max_recv).min(64 * 1024));
                        match self.algo {
                            FrameDecompressAlgo::Gzip => {
                                let mut dec = flate2::read::MultiGzDecoder::new(compressed).take(limit);
                                dec.read_to_end(&mut decompressed).map_err(|e| {
                                    Status::internal(format!("gzip decompression failed: {e}"))
                                })?;
                            }
                            FrameDecompressAlgo::Zstd => {
                                let dec = zstd::stream::read::Decoder::new(compressed).map_err(
                                    |e| Status::internal(format!("zstd decompression failed: {e}")),
                                )?;
                                dec.take(limit).read_to_end(&mut decompressed).map_err(|e| {
                                    Status::internal(format!("zstd decompression failed: {e}"))
                                })?;
                            }
                            FrameDecompressAlgo::Identity => unreachable!(),
                        }
                        self.in_buf.advance(frame_end);
                        if decompressed.len() > self.max_recv {
                            return Err(Status::resource_exhausted(format!(
                                "grpc: received message after decompression larger than max {}",
                                self.max_recv
                            )));
                        }
                        let dec_len_u32 = u32::try_from(decompressed.len()).map_err(|_| {
                            Status::resource_exhausted(format!(
                                "grpc: received message after decompression larger than max {}",
                                self.max_recv
                            ))
                        })?;
                        let mut out = BytesMut::with_capacity(5 + decompressed.len());
                        out.put_u8(0);
                        out.extend_from_slice(&dec_len_u32.to_be_bytes());
                        out.extend_from_slice(&decompressed);
                        out.freeze()
                    }
                    other => {
                        return Err(Status::internal(format!(
                            "protocol error: received message with invalid compression flag: {other} (valid flags are 0 and 1), while sending request"
                        )));
                    }
                },
            };
            self.frames_seen += 1;
            if self.frames_seen == 1 {
                self.buffered_frame1 = Some(decoded_frame);
            } else {
                self.buffered_frame1 = None;
                return Err(Status::internal(
                    "cardinality violation: received multiple request messages for non-client-streaming RPC",
                ));
            }
        }
        Ok(())
    }
}

impl hyper::body::Body for NonClientStreamingFrameBody {
    type Data = Bytes;
    type Error = Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if self.eof_seen {
            if let Some(frame1) = self.buffered_frame1.take() {
                return Poll::Ready(Some(Ok(hyper::body::Frame::data(frame1))));
            }
            if let Some(trailers) = self.pending_trailers.take() {
                self.done = true;
                return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
            }
            self.done = true;
            return Poll::Ready(None);
        }
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(frame))) => {
                    if frame.is_data() {
                        if let Ok(data) = frame.into_data() {
                            if data.is_empty() {
                                continue;
                            }
                            if self.frames_seen == 0
                                && self.algo == FrameDecompressAlgo::Identity
                                && self.in_buf.is_empty()
                                && data.len() >= 5
                                && data[0] == 0
                            {
                                let msg_len = u32::from_be_bytes([
                                    data[1], data[2], data[3], data[4],
                                ]) as usize;
                                if msg_len > self.max_recv {
                                    self.done = true;
                                    return Poll::Ready(Some(Err(Status::resource_exhausted(
                                        format!(
                                            "grpc: received message larger than max ({msg_len} vs. {})",
                                            self.max_recv
                                        ),
                                    ))));
                                }
                                if 5usize.checked_add(msg_len) == Some(data.len()) {
                                    self.frames_seen = 1;
                                    self.buffered_frame1 = Some(data);
                                    continue;
                                }
                            }
                            self.in_buf.extend_from_slice(&data);
                            if let Err(st) = self.check_incoming_header() {
                                self.done = true;
                                return Poll::Ready(Some(Err(st)));
                            }
                            if let Err(st) = self.drain_buffered_frames() {
                                self.done = true;
                                return Poll::Ready(Some(Err(st)));
                            }
                        }
                    } else if let Ok(trailers) = frame.into_trailers() {
                        if !self.in_buf.is_empty() {
                            self.done = true;
                            return Poll::Ready(Some(Err(Status::internal(
                                "Unexpected EOF decoding stream.",
                            ))));
                        }
                        if self.frames_seen == 0 {
                            self.done = true;
                            return Poll::Ready(Some(Err(Status::internal(
                                "cardinality violation: received no request message from non-client-streaming RPC",
                            ))));
                        }
                        self.eof_seen = true;
                        self.pending_trailers = Some(trailers);
                        if let Some(frame1) = self.buffered_frame1.take() {
                            return Poll::Ready(Some(Ok(hyper::body::Frame::data(frame1))));
                        }
                        self.done = true;
                        if let Some(t) = self.pending_trailers.take() {
                            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(t))));
                        }
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(None) => {
                    if !self.in_buf.is_empty() {
                        self.done = true;
                        return Poll::Ready(Some(Err(Status::internal(
                            "Unexpected EOF decoding stream.",
                        ))));
                    }
                    if self.frames_seen == 0 {
                        self.done = true;
                        return Poll::Ready(Some(Err(Status::internal(
                            "cardinality violation: received no request message from non-client-streaming RPC",
                        ))));
                    }
                    self.eof_seen = true;
                    if let Some(frame1) = self.buffered_frame1.take() {
                        return Poll::Ready(Some(Ok(hyper::body::Frame::data(frame1))));
                    }
                    self.done = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

fn status_to_trailers(status: Status) -> http::HeaderMap {
    let mut headers = status.into_http().into_parts().0.headers;
    headers.remove(http::header::CONTENT_TYPE);
    headers
}

struct MaxSendFrameBody {
    inner: tonic::body::BoxBody,
    max_send: usize,
    in_buf: BytesMut,
    pending_trailers: Option<http::HeaderMap>,
    done: bool,
}

impl MaxSendFrameBody {
    fn new(inner: tonic::body::BoxBody, max_send: usize) -> Self {
        Self {
            inner,
            max_send,
            in_buf: BytesMut::new(),
            pending_trailers: None,
            done: false,
        }
    }

    #[allow(clippy::result_large_err)]
    fn drain_checked(&mut self) -> Result<Option<Bytes>, Status> {
        let mut out = BytesMut::new();
        while self.in_buf.len() >= 5 {
            let len = u32::from_be_bytes([
                self.in_buf[1],
                self.in_buf[2],
                self.in_buf[3],
                self.in_buf[4],
            ]) as usize;
            let Some(frame_end) = 5usize.checked_add(len) else {
                break;
            };
            if self.in_buf.len() < frame_end {
                break;
            }
            if len > self.max_send {
                self.in_buf.clear();
                return Err(Status::resource_exhausted(format!(
                    "trying to send message larger than max ({len} vs. {})",
                    self.max_send
                )));
            }
            out.extend_from_slice(&self.in_buf[..frame_end]);
            self.in_buf.advance(frame_end);
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out.freeze()))
        }
    }
}

impl hyper::body::Body for MaxSendFrameBody {
    type Data = Bytes;
    type Error = Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if let Some(trailers) = self.pending_trailers.take() {
            self.done = true;
            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
        }
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            match self.drain_checked() {
                Ok(Some(chunk)) => return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk)))),
                Ok(None) => {}
                Err(status) => {
                    self.done = true;
                    return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(
                        status_to_trailers(status),
                    ))));
                }
            }
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    if !self.in_buf.is_empty() {
                        return Poll::Ready(Some(Ok(hyper::body::Frame::data(
                            self.in_buf.split().freeze(),
                        ))));
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => {
                        if !data.is_empty() {
                            self.in_buf.extend_from_slice(&data);
                        }
                    }
                    Err(other) => {
                        if let Ok(trailers) = other.into_trailers() {
                            if !self.in_buf.is_empty() {
                                self.pending_trailers = Some(trailers);
                                return Poll::Ready(Some(Ok(hyper::body::Frame::data(
                                    self.in_buf.split().freeze(),
                                ))));
                            }
                            self.done = true;
                            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
                        }
                    }
                },
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

struct SnappyCompressBody {
    inner: tonic::body::BoxBody,
    encoding: CustomGrpcEncoding,
    max_send: usize,
    in_buf: BytesMut,
    pending_trailers: Option<http::HeaderMap>,
    done: bool,
}

impl SnappyCompressBody {
    fn new(inner: tonic::body::BoxBody, encoding: CustomGrpcEncoding, max_send: usize) -> Self {
        Self {
            inner,
            encoding,
            max_send,
            in_buf: BytesMut::new(),
            pending_trailers: None,
            done: false,
        }
    }

    #[allow(clippy::result_large_err)]
    fn drain_complete_frames(&mut self) -> Result<Option<Bytes>, Status> {
        let mut out = BytesMut::new();
        while self.in_buf.len() >= 5 {
            let flag = self.in_buf[0];
            let len = u32::from_be_bytes([
                self.in_buf[1],
                self.in_buf[2],
                self.in_buf[3],
                self.in_buf[4],
            ]) as usize;
            let Some(frame_end) = 5usize.checked_add(len) else {
                break;
            };
            if self.in_buf.len() < frame_end {
                break;
            }
            if flag == 0 && len > 0 {
                let hdr_pos = out.len();
                out.extend_from_slice(&[1, 0, 0, 0, 0]);
                if mimir_proto::snappy::compress_framed_buf(&self.in_buf[5..frame_end], &mut out)
                    .is_ok()
                {
                    if matches!(self.encoding, CustomGrpcEncoding::S2)
                        && out.len() >= hdr_pos + 15
                    {
                        out[hdr_pos + 9..hdr_pos + 15].copy_from_slice(b"S2sTwO");
                    }
                    let comp_len = out.len() - (hdr_pos + 5);
                    if comp_len > self.max_send {
                        out.truncate(hdr_pos);
                        self.in_buf.clear();
                        return Err(Status::resource_exhausted(format!(
                            "trying to send message larger than max ({comp_len} vs. {})",
                            self.max_send
                        )));
                    }
                    if let Ok(comp_len_u32) = u32::try_from(comp_len) {
                        out[hdr_pos + 1..hdr_pos + 5]
                            .copy_from_slice(&comp_len_u32.to_be_bytes());
                        self.in_buf.advance(frame_end);
                        continue;
                    }
                }
                out.truncate(hdr_pos);
            }
            if len > self.max_send {
                self.in_buf.clear();
                return Err(Status::resource_exhausted(format!(
                    "trying to send message larger than max ({len} vs. {})",
                    self.max_send
                )));
            }
            out.extend_from_slice(&self.in_buf[..frame_end]);
            self.in_buf.advance(frame_end);
        }
        if out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(out.freeze()))
        }
    }
}

impl hyper::body::Body for SnappyCompressBody {
    type Data = Bytes;
    type Error = Status;

    #[allow(clippy::result_large_err)]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if let Some(trailers) = self.pending_trailers.take() {
            self.done = true;
            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
        }
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            match self.drain_complete_frames() {
                Ok(Some(chunk)) => return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk)))),
                Ok(None) => {}
                Err(status) => {
                    self.done = true;
                    return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(
                        status_to_trailers(status),
                    ))));
                }
            }
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    if !self.in_buf.is_empty() {
                        return Poll::Ready(Some(Ok(hyper::body::Frame::data(
                            self.in_buf.split().freeze(),
                        ))));
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => {
                        if !data.is_empty() {
                            self.in_buf.extend_from_slice(&data);
                        }
                    }
                    Err(other) => {
                        if let Ok(trailers) = other.into_trailers() {
                            if !self.in_buf.is_empty() {
                                self.pending_trailers = Some(trailers);
                                return Poll::Ready(Some(Ok(hyper::body::Frame::data(
                                    self.in_buf.split().freeze(),
                                ))));
                            }
                            self.done = true;
                            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
                        }
                    }
                },
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

#[derive(Clone, Copy)]
enum CustomGrpcEncoding {
    Snappy,
    S2,
}

fn custom_grpc_encoding(headers: &http::HeaderMap) -> Option<CustomGrpcEncoding> {
    let val = headers.get("grpc-encoding")?.to_str().ok()?;
    if val.eq_ignore_ascii_case(mimir_proto::snappy::SNAPPY_ENCODING_NAME) {
        Some(CustomGrpcEncoding::Snappy)
    } else if val.eq_ignore_ascii_case(mimir_proto::s2::S2_ENCODING_NAME) {
        Some(CustomGrpcEncoding::S2)
    } else {
        None
    }
}

const MAX_TLS_SCRATCH_CAP: usize = 2 * 1024 * 1024;
const GRPC_ACCEPT_ENCODING_VALUE: &str = "snappy,s2,gzip,identity";

thread_local! {
    static RAW_BODY_SCRATCH: std::cell::RefCell<Option<BytesMut>> =
        const { std::cell::RefCell::new(None) };
    static DECOMP_SCRATCH: std::cell::RefCell<Vec<u8>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

struct PooledBytesMut(Option<BytesMut>);

impl PooledBytesMut {
    fn take_or_new(cap: usize) -> Self {
        let buf = RAW_BODY_SCRATCH
            .with(|cell| cell.borrow_mut().take())
            .unwrap_or_else(|| BytesMut::with_capacity(cap));
        Self(Some(buf))
    }
}

impl Drop for PooledBytesMut {
    fn drop(&mut self) {
        if let Some(mut buf) = self.0.take() {
            if buf.capacity() <= MAX_TLS_SCRATCH_CAP {
                buf.clear();
                let _ = RAW_BODY_SCRATCH.try_with(|cell| {
                    if let Ok(mut slot) = cell.try_borrow_mut() {
                        *slot = Some(buf);
                    }
                });
            }
        }
    }
}

struct VecResetGuard<'a>(&'a mut Vec<u8>);

impl Drop for VecResetGuard<'_> {
    fn drop(&mut self) {
        self.0.clear();
        if self.0.capacity() > MAX_TLS_SCRATCH_CAP {
            self.0.shrink_to(256 * 1024);
        }
    }
}

#[allow(clippy::result_large_err)]
fn decode_single_custom_grpc_frame(
    frame_slice: &[u8],
    encoding: CustomGrpcEncoding,
    max_decode_len: usize,
) -> Result<Bytes, Status> {
    let flag = frame_slice[0];
    let len = frame_slice.len() - 5;
    let payload = &frame_slice[5..];
    match flag {
        0 => {
            if len > max_decode_len {
                return Err(Status::resource_exhausted(format!(
                    "grpc: received message larger than max ({len} vs. {max_decode_len})"
                )));
            }
            Ok(Bytes::copy_from_slice(frame_slice))
        }
        1 => DECOMP_SCRATCH.with(|cell| {
            let mut scratch_ref = cell.borrow_mut();
            let guard = VecResetGuard(&mut scratch_ref);
            let out = &mut *guard.0;
            out.extend_from_slice(&[0u8; 5]);
            let map_decomp_err = |codec: &str, e: std::io::Error| -> Status {
                if e.get_ref()
                    .is_some_and(|r| r.is::<mimir_proto::snappy::DecompressLimitExceeded>())
                {
                    Status::resource_exhausted(format!(
                        "grpc: received message after decompression larger than max {max_decode_len}"
                    ))
                } else {
                    Status::internal(format!("{codec} decompression failed: {e}"))
                }
            };
            match encoding {
                CustomGrpcEncoding::Snappy => {
                    mimir_proto::snappy::decompress_framed_bounded_into(
                        payload,
                        out,
                        max_decode_len,
                    )
                    .map_err(|e| map_decomp_err("snappy", e))?;
                }
                CustomGrpcEncoding::S2 => {
                    mimir_proto::s2::decompress_framed_into(payload, out, max_decode_len)
                        .map_err(|e| map_decomp_err("s2", e))?;
                }
            }
            let dec_len = out.len() - 5;
            if dec_len > max_decode_len {
                return Err(Status::resource_exhausted(format!(
                    "grpc: received message after decompression larger than max {max_decode_len}"
                )));
            }
            out[1..5].copy_from_slice(&(dec_len as u32).to_be_bytes());
            Ok(Bytes::copy_from_slice(out))
        }),
        other => Err(Status::internal(format!(
            "invalid gRPC compression flag {other}"
        ))),
    }
}

#[allow(clippy::result_large_err)]
async fn decompress_custom_grpc_request(
    req: http::Request<tonic::body::BoxBody>,
    encoding: CustomGrpcEncoding,
    max_decode_len: usize,
) -> Result<http::Request<tonic::body::BoxBody>, Status> {
    let (mut parts, mut body) = req.into_parts();
    let mut pooled = PooledBytesMut::take_or_new(4096);
    let mut frame1_out: Option<Bytes> = None;
    let mut frame_count = 0usize;

    while let Some(frame_res) =
        futures::future::poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut body), cx)).await
    {
        let frame = frame_res.map_err(|s| Status::internal(format!("reading gRPC body: {s}")))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.is_empty() {
            continue;
        }
        let buf = pooled.0.get_or_insert_with(|| BytesMut::with_capacity(4096));
        if frame_count == 0 && buf.is_empty() && data.len() >= 5 && data[0] == 0 {
            let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if len > max_decode_len {
                return Err(Status::resource_exhausted(format!(
                    "grpc: received message larger than max ({len} vs. {max_decode_len})"
                )));
            }
            if 5usize.checked_add(len) == Some(data.len()) {
                frame1_out = Some(data);
                frame_count = 1;
                continue;
            }
        }
        buf.extend_from_slice(&data);
        while buf.len() >= 5 {
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if len > max_decode_len {
                return Err(Status::resource_exhausted(format!(
                    "grpc: received message larger than max ({len} vs. {max_decode_len})"
                )));
            }
            let Some(frame_end) = 5usize.checked_add(len) else {
                return Err(Status::internal("gRPC compressed frame length overflow"));
            };
            if buf.len() < frame_end {
                break;
            }
            let decoded =
                decode_single_custom_grpc_frame(&buf[..frame_end], encoding, max_decode_len)?;
            buf.advance(frame_end);
            frame_count += 1;
            if frame_count == 1 {
                frame1_out = Some(decoded);
            } else {
                // grpc-go `stream.go` `serverStream.RecvMsg`: validate/decompress frame 2
                // against `maxReceiveMessageSize`, then fail BEFORE invoking handler.
                return Err(Status::internal(
                    "cardinality violation: received multiple request messages for non-client-streaming RPC",
                ));
            }
        }
    }

    if let Some(buf) = pooled.0.as_ref() {
        if !buf.is_empty() {
            if buf.len() < 5 {
                return Err(Status::internal("truncated gRPC compressed frame header"));
            }
            return Err(Status::internal("truncated gRPC compressed frame payload"));
        }
    }
    if frame_count == 0 {
        return Err(Status::internal(
            "cardinality violation: received no request message from non-client-streaming RPC",
        ));
    }

    parts.headers.remove("grpc-encoding");
    parts.headers.remove(http::header::CONTENT_LENGTH);
    Ok(http::Request::from_parts(
        parts,
        tonic::body::BoxBody::new(SingleChunkBody(Some(frame1_out.unwrap_or_default()))),
    ))
}

/// Accepts `grpc-encoding: snappy` and `grpc-encoding: s2` requests for any wrapped gRPC service:
/// Go registers dskit's `grpcencoding/snappy` and Mimir's `grpcencoding/s2` compressors
/// process-wide, so every gRPC service of a Mimir server (e.g. `grpc.health.v1.Health`,
/// `httpgrpc.HTTP`, `cortex.Ingester`) decodes Snappy- and S2-compressed messages.
#[derive(Clone)]
pub struct SnappyDecompressService<S> {
    inner: S,
    max_decode_len: usize,
}

impl<S> SnappyDecompressService<S> {
    pub fn new(inner: S, max_decode_len: usize) -> Self {
        Self {
            inner,
            max_decode_len,
        }
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for SnappyDecompressService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> tower::Service<http::Request<tonic::body::BoxBody>> for SnappyDecompressService<S>
where
    S: tower::Service<
            http::Request<tonic::body::BoxBody>,
            Response = http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let Some(encoding) = custom_grpc_encoding(req.headers()) else {
            return Box::pin(self.inner.call(req));
        };
        // Use the service that was polled ready; leave a fresh clone behind.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let max_decode_len = self.max_decode_len;
        Box::pin(async move {
            match decompress_custom_grpc_request(req, encoding, max_decode_len).await {
                Ok(req) => inner.call(req).await,
                Err(status) => Ok(status.into_http()),
            }
        })
    }
}

impl tower::Service<http::Request<tonic::body::BoxBody>> for IngesterZeroCopyServer {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <ingesterpb::ingester_server::IngesterServer<IngesterService> as tower::Service<
            http::Request<tonic::body::BoxBody>,
        >>::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, mut req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let pre_tap = req.extensions_mut().remove::<PreAcquiredTap>();
        let custom_enc = custom_grpc_encoding(req.headers());
        let max_decode_len = self.max_decoding_message_size.unwrap_or(4 * 1024 * 1024);
        let max_send_len = self.max_encoding_message_size.unwrap_or(usize::MAX);
        let deadline_ctx = req.extensions().get::<Arc<RpcDeadlineContext>>().cloned();
        let path = req.uri().path();
        let is_push = path == "/cortex.Ingester/Push";
        let is_query_stream = path == "/cortex.Ingester/QueryStream";
        let is_metrics_for_label_matchers = path == "/cortex.Ingester/MetricsForLabelMatchers";
        let is_ingester_rpc = path.starts_with("/cortex.Ingester/");
        let echo_encoding = if is_ingester_rpc && self.send_compression.is_none() {
            req.headers()
                .get("grpc-encoding")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .and_then(|enc| {
                    if enc.eq_ignore_ascii_case("gzip") {
                        Some("gzip")
                    } else if enc.eq_ignore_ascii_case("zstd") {
                        Some("zstd")
                    } else if enc.eq_ignore_ascii_case(mimir_proto::snappy::SNAPPY_ENCODING_NAME) {
                        Some(mimir_proto::snappy::SNAPPY_ENCODING_NAME)
                    } else if enc.eq_ignore_ascii_case(mimir_proto::s2::S2_ENCODING_NAME) {
                        Some(mimir_proto::s2::S2_ENCODING_NAME)
                    } else {
                        None
                    }
                })
        } else {
            None
        };

        let ingester = Arc::clone(&self.ingester);
        let accept_compression = self.accept_compression;
        let accept_compression_len = self.accept_compression_len as usize;
        let send_compression = self.send_compression;
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            // Pre-decompression gRPC TapHandle (`dskit/server/limits.go` + `pkg/mimir/grpc_call_check.go`).
            // If `grpc_server.rs` already ran TapHandle before `auth` / `check_cluster`, reuse
            // that permit instead of acquiring a second one.
            let mut pre_tap = pre_tap;
            let push_tap_slot: Option<
                Arc<parking_lot::Mutex<Option<crate::push::PushTapPermit>>>,
            > = if is_push {
                if let Some(PreAcquiredTap::Push(slot)) = pre_tap.take() {
                    Some(slot)
                } else {
                    let req_size = parse_message_size_header(req.headers());
                    match ingester.try_start_push_tap(req_size) {
                        Ok(p) => Some(Arc::new(parking_lot::Mutex::new(Some(p)))),
                        Err(e) => return Ok(unavailable_tap_response(e)),
                    }
                }
            } else {
                None
            };
            let read_tap_slot: Option<Arc<parking_lot::Mutex<Option<ReadTapGuard>>>> =
                if !is_push && is_ingester_rpc {
                    if let Some(PreAcquiredTap::Read(slot)) = pre_tap.take() {
                        let permit = slot.lock().take();
                        permit.map(|p| {
                            Arc::new(parking_lot::Mutex::new(Some(ReadTapGuard {
                                tap: Some(p),
                                deadline_ctx: deadline_ctx.clone(),
                            })))
                        })
                    } else {
                        match ingester.try_start_read_tap(req.headers()) {
                            Ok(p) => Some(Arc::new(parking_lot::Mutex::new(Some(ReadTapGuard {
                                tap: Some(p),
                                deadline_ctx: deadline_ctx.clone(),
                            })))),
                            Err(e) => return Ok(unavailable_tap_response(e)),
                        }
                    }
                } else {
                    None
                };

            let mut req = if let Some(encoding) = custom_enc {
                match decompress_custom_grpc_request(req, encoding, max_decode_len).await {
                    Ok(r) => r,
                    Err(status) => {
                        if let Some(ref slot) = read_tap_slot {
                            if let Some(mut guard) = slot.lock().take() {
                                guard.finish(false);
                            }
                        }
                        if let Some(ref slot) = push_tap_slot {
                            drop(slot.lock().take());
                        }
                        let mut resp = status.into_http();
                        resp.headers_mut().insert(
                            "grpc-accept-encoding",
                            http::HeaderValue::from_static(GRPC_ACCEPT_ENCODING_VALUE),
                        );
                        return Ok(resp);
                    }
                }
            } else {
                let has_gzip = accept_compression[..accept_compression_len]
                    .iter()
                    .flatten()
                    .any(|e| *e == CompressionEncoding::Gzip);
                let has_zstd = accept_compression[..accept_compression_len]
                    .iter()
                    .flatten()
                    .any(|e| *e == CompressionEncoding::Zstd);
                let req_enc = req
                    .headers()
                    .get("grpc-encoding")
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .map(str::to_ascii_lowercase);
                match req_enc.as_deref() {
                    Some("gzip") if has_gzip => {
                        let mut req = req;
                        req.headers_mut().remove("grpc-encoding");
                        req.headers_mut().remove(http::header::CONTENT_LENGTH);
                        req.map(|b| {
                            tonic::body::BoxBody::new(NonClientStreamingFrameBody::new(
                                b,
                                FrameDecompressAlgo::Gzip,
                                max_decode_len,
                            ))
                        })
                    }
                    Some("zstd") if has_zstd => {
                        let mut req = req;
                        req.headers_mut().remove("grpc-encoding");
                        req.headers_mut().remove(http::header::CONTENT_LENGTH);
                        req.map(|b| {
                            tonic::body::BoxBody::new(NonClientStreamingFrameBody::new(
                                b,
                                FrameDecompressAlgo::Zstd,
                                max_decode_len,
                            ))
                        })
                    }
                    None | Some("identity") => req.map(|b| {
                        tonic::body::BoxBody::new(NonClientStreamingFrameBody::new(
                            b,
                            FrameDecompressAlgo::Identity,
                            max_decode_len,
                        ))
                    }),
                    // Unsupported encoding: let Tonic return Unimplemented before reading body.
                    Some(_) => req,
                }
            };

            // RPCCallProcessing (`prepare_push_reactive` / `prepare_read_reactive`) runs INSIDE
            // each service entrypoint below — AFTER frame decode/decompression (`dec(in)`),
            // `ServerUserHeaderInterceptor` (`x-scope-orgid`), and `ClusterUnaryServerInterceptor`
            // — and returns a normal `Status::unavailable` (WITHOUT `TapRejected`) so
            // `RpcGuard` records `cortex_request_duration_seconds{status_code="Unavailable"}`,
            // `GRPCServerLog`, and OTel spans.
            let mut resp = if is_push {
                struct RawPushSvc {
                    ingester: Arc<Ingester>,
                    tap_slot: Option<Arc<parking_lot::Mutex<Option<crate::push::PushTapPermit>>>>,
                    deadline_ctx: Option<Arc<RpcDeadlineContext>>,
                }
                impl tonic::server::UnaryService<Bytes> for RawPushSvc {
                    type Response = cortexpb::WriteResponse;
                    type Future = Pin<
                        Box<
                            dyn Future<Output = Result<Response<Self::Response>, Status>>
                                + Send
                                + 'static,
                        >,
                    >;

                    fn call(&mut self, request: Request<Bytes>) -> Self::Future {
                        struct TrackedBufferGuard(mimir_proto::ref_leaks::TrackedBuffer);
                        impl Drop for TrackedBufferGuard {
                            fn drop(&mut self) {
                                self.0.free();
                            }
                        }

                        let ingester = Arc::clone(&self.ingester);
                        let tap_slot = self.tap_slot.clone();
                        let deadline_ctx = self.deadline_ctx.clone();
                        Box::pin(async move {
                            let mut tap = tap_slot.as_ref().and_then(|s| s.lock().take());
                            let org_id_res = org_id_from_req(&ingester, &request);
                            let (_, extensions, raw) = request.into_parts();
                            let _tracked_buf = ingester
                                .ref_leaks_tracker()
                                .and_then(|t| t.maybe_instrument_ref_leaks(&raw))
                                .map(TrackedBufferGuard);
                            if let Some(ref g) = _tracked_buf {
                                let _ = g.0.read_only_data();
                            }
                            // Go order: dec(in) -> ServerUserHeaderInterceptor -> RPCCallProcessing -> PushGrpcMethodEnabled -> tenant.TenantID.
                            // On the happy path we avoid double-decoding by validating wire bytes early only
                            // when org_id / reactive / push_grpc_method_enabled / tenant_id fails.
                            if org_id_res.is_err() {
                                if let Err(e) = ingester.validate_grpc_push_wire(&raw) {
                                    drop(tap);
                                    record_push_error(
                                        &ingester,
                                        extensions.get::<GrpcErrorLog>(),
                                        &e,
                                    );
                                    return Err(e.map_push_error_to_grpc_server_status());
                                }
                            }
                            let org_id = match org_id_res {
                                Ok(id) => id,
                                Err(status) => {
                                    drop(tap);
                                    return Err(status);
                                }
                            };
                            match ingester.prepare_push_reactive(deadline_ctx.as_deref()).await {
                                Ok(Some(rp)) => {
                                    if let Some(ref mut t) = tap {
                                        t.set_reactive_permit(rp);
                                    }
                                }
                                Ok(None) => {}
                                Err(reactive_err) => {
                                    if let Err(e) = ingester.validate_grpc_push_wire(&raw) {
                                        drop(tap);
                                        record_push_error(
                                            &ingester,
                                            extensions.get::<GrpcErrorLog>(),
                                            &e,
                                        );
                                        return Err(e.map_push_error_to_grpc_server_status());
                                    }
                                    drop(tap);
                                    return Err(Status::unavailable(reactive_err.to_string()));
                                }
                            }
                            if !ingester.cfg.push_grpc_method_enabled {
                                if let Err(e) = ingester.validate_grpc_push_wire(&raw) {
                                    drop(tap);
                                    record_push_error(
                                        &ingester,
                                        extensions.get::<GrpcErrorLog>(),
                                        &e,
                                    );
                                    return Err(e.map_push_error_to_grpc_server_status());
                                }
                                drop(tap);
                                let e = IngesterError::PushGrpcDisabled;
                                record_push_error(
                                    &ingester,
                                    extensions.get::<GrpcErrorLog>(),
                                    &e,
                                );
                                return Err(e.map_push_error_to_grpc_server_status());
                            }
                            let user_id = match tenant_from_org_id(&org_id, &extensions) {
                                Ok(uid) => uid,
                                Err(tenant_status) => {
                                    let wire_err = ingester.validate_grpc_push_wire(&raw).err();
                                    drop(tap);
                                    if let Some(e) = wire_err {
                                        record_push_error(
                                            &ingester,
                                            extensions.get::<GrpcErrorLog>(),
                                            &e,
                                        );
                                        return Err(e.map_push_error_to_grpc_server_status());
                                    }
                                    return Err(tenant_status);
                                }
                            };
                            let res = match tap {
                                Some(permit) => {
                                    ingester
                                        .push_grpc_raw_admitted(&user_id, raw, permit)
                                        .await
                                }
                                None => ingester.push_grpc_raw(&user_id, raw).await,
                            };
                            if let Err(e) = res {
                                record_push_error(
                                    &ingester,
                                    extensions.get::<GrpcErrorLog>(),
                                    &e,
                                );
                                return Err(e.map_push_error_to_grpc_server_status());
                            }
                            Ok(Response::new(cortexpb::WriteResponse {}))
                        })
                    }
                }

                let mut push_grpc = tonic::server::Grpc::new(RawPushCodec);
                for enc in accept_compression[..accept_compression_len]
                    .iter()
                    .flatten()
                {
                    push_grpc = push_grpc.accept_compressed(*enc);
                }
                let effective_tonic_send = send_compression.or_else(|| match echo_encoding {
                    Some("gzip")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Gzip) =>
                    {
                        Some(CompressionEncoding::Gzip)
                    }
                    Some("zstd")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Zstd) =>
                    {
                        Some(CompressionEncoding::Zstd)
                    }
                    _ => None,
                });
                if let Some(enc) = effective_tonic_send {
                    push_grpc = push_grpc.send_compressed(enc);
                }
                push_grpc = push_grpc
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX);
                let resp = scope_deadline_ctx(
                    deadline_ctx.clone(),
                    push_grpc.unary(
                        RawPushSvc {
                            ingester: Arc::clone(&ingester),
                            tap_slot: push_tap_slot.clone(),
                            deadline_ctx: deadline_ctx.clone(),
                        },
                        req,
                    ),
                )
                .await;
                let resp = if let Some(custom_resp_enc @ ("snappy" | "s2")) = echo_encoding {
                    if resp.status() == http::StatusCode::OK
                        && !resp.headers().contains_key("grpc-status")
                        && resp.headers().get("grpc-encoding").is_none()
                    {
                        let (mut parts, body) = resp.into_parts();
                        parts.headers.insert(
                            "grpc-encoding",
                            http::HeaderValue::from_static(custom_resp_enc),
                        );
                        let enc = if custom_resp_enc == "s2" {
                            CustomGrpcEncoding::S2
                        } else {
                            CustomGrpcEncoding::Snappy
                        };
                        http::Response::from_parts(
                            parts,
                            tonic::body::BoxBody::new(SnappyCompressBody::new(
                                body,
                                enc,
                                max_send_len,
                            )),
                        )
                    } else {
                        resp
                    }
                } else {
                    resp
                };
                if let Some(slot) = push_tap_slot {
                    drop(slot.lock().take());
                }
                resp
            } else if is_query_stream {
                struct RawQueryStreamSvc {
                    ingester: Arc<Ingester>,
                    read_tap_slot: Option<Arc<parking_lot::Mutex<Option<ReadTapGuard>>>>,
                    deadline_ctx: Option<Arc<RpcDeadlineContext>>,
                }
                impl tonic::server::ServerStreamingService<ingesterpb::QueryRequest> for RawQueryStreamSvc {
                    type Response = Bytes;
                    type ResponseStream = RawQueryStreamBox;
                    type Future = Pin<
                        Box<
                            dyn Future<Output = Result<Response<Self::ResponseStream>, Status>>
                                + Send
                                + 'static,
                        >,
                    >;

                    fn call(&mut self, request: Request<ingesterpb::QueryRequest>) -> Self::Future {
                        let ingester = Arc::clone(&self.ingester);
                        let read_tap_slot = self.read_tap_slot.clone();
                        let deadline_ctx = self.deadline_ctx.clone();
                        Box::pin(async move {
                            let org_id = org_id_from_req(&ingester, &request)?;
                            let (md, mut ext, inner) = request.into_parts();
                            prepare_read_reactive_for_slot(
                                &ingester,
                                &md,
                                deadline_ctx.as_deref(),
                                read_tap_slot.as_deref(),
                                &mut ext,
                            )
                            .await?;
                            let user_id = tenant_from_org_id(&org_id, &ext)?;
                            let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
                            let act = ingester.insert_activity(|| {
                                request_activity(
                                    &user_id,
                                    &trace_id,
                                    "Ingester/QueryStream",
                                    ActivityRequest::Query(&inner),
                                )
                            });
                            let err_sink = crate::query::ReadErrorSink::from_extensions(&ext);
                            let stream = ingester
                                .handle_query_stream_raw_with_sink(&md, &user_id, inner, err_sink)
                                .await
                                .map_err(|e| read_error_status(&ext, e))?;
                            let wrapped: RawQueryStreamBox = Box::pin(ActivityStream {
                                inner: stream,
                                _guard: act,
                            });
                            Ok(Response::new(wrapped))
                        })
                    }
                }

                let mut qs_grpc = tonic::server::Grpc::new(RawQueryStreamCodec);
                for enc in accept_compression[..accept_compression_len]
                    .iter()
                    .flatten()
                {
                    qs_grpc = qs_grpc.accept_compressed(*enc);
                }
                let effective_tonic_send = send_compression.or_else(|| match echo_encoding {
                    Some("gzip")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Gzip) =>
                    {
                        Some(CompressionEncoding::Gzip)
                    }
                    Some("zstd")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Zstd) =>
                    {
                        Some(CompressionEncoding::Zstd)
                    }
                    _ => None,
                });
                if let Some(enc) = effective_tonic_send {
                    qs_grpc = qs_grpc.send_compressed(enc);
                }
                qs_grpc = qs_grpc
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX);
                let qs_resp = READ_TAP_ADMITTED
                    .scope(
                        true,
                        scope_deadline_ctx(
                            deadline_ctx.clone(),
                            qs_grpc.server_streaming(
                                RawQueryStreamSvc {
                                    ingester,
                                    read_tap_slot: read_tap_slot.clone(),
                                    deadline_ctx: deadline_ctx.clone(),
                                },
                                req,
                            ),
                        ),
                    )
                    .await;
                let qs_resp = if let Some(custom_resp_enc @ ("snappy" | "s2")) = echo_encoding {
                    if qs_resp.status() == http::StatusCode::OK
                        && !qs_resp.headers().contains_key("grpc-status")
                        && qs_resp.headers().get("grpc-encoding").is_none()
                    {
                        let (mut parts, body) = qs_resp.into_parts();
                        parts.headers.insert(
                            "grpc-encoding",
                            http::HeaderValue::from_static(custom_resp_enc),
                        );
                        let enc = if custom_resp_enc == "s2" {
                            CustomGrpcEncoding::S2
                        } else {
                            CustomGrpcEncoding::Snappy
                        };
                        http::Response::from_parts(
                            parts,
                            tonic::body::BoxBody::new(SnappyCompressBody::new(
                                body,
                                enc,
                                max_send_len,
                            )),
                        )
                    } else {
                        qs_resp
                    }
                } else if max_send_len < usize::MAX
                    && !qs_resp.headers().contains_key("grpc-status")
                {
                    qs_resp.map(|b| tonic::body::BoxBody::new(MaxSendFrameBody::new(b, max_send_len)))
                } else {
                    qs_resp
                };
                if let Some(mut guard) = read_tap_slot.as_ref().and_then(|s| s.lock().take()) {
                    if qs_resp.headers().contains_key("grpc-status") {
                        guard.finish(false);
                        qs_resp
                    } else {
                        let permit = guard.take_for_body();
                        qs_resp.map(|body| {
                            tonic::body::BoxBody::new(ReadTapDropBody {
                                inner: body,
                                permit,
                                deadline_ctx: deadline_ctx.clone(),
                            })
                        })
                    }
                } else {
                    qs_resp
                }
            } else if is_metrics_for_label_matchers {
                struct RawMetricsForLabelMatchersSvc {
                    ingester: Arc<Ingester>,
                    read_tap_slot: Option<Arc<parking_lot::Mutex<Option<ReadTapGuard>>>>,
                    deadline_ctx: Option<Arc<RpcDeadlineContext>>,
                }
                impl tonic::server::UnaryService<ingesterpb::MetricsForLabelMatchersRequest>
                    for RawMetricsForLabelMatchersSvc
                {
                    type Response = Bytes;
                    type Future = Pin<
                        Box<dyn Future<Output = Result<Response<Self::Response>, Status>> + Send + 'static>,
                    >;

                    fn call(
                        &mut self,
                        request: Request<ingesterpb::MetricsForLabelMatchersRequest>,
                    ) -> Self::Future {
                        let ingester = Arc::clone(&self.ingester);
                        let read_tap_slot = self.read_tap_slot.clone();
                        let deadline_ctx = self.deadline_ctx.clone();
                        Box::pin(async move {
                            let org_id = org_id_from_req(&ingester, &request)?;
                            let (md, mut ext, inner) = request.into_parts();
                            prepare_read_reactive_for_slot(
                                &ingester,
                                &md,
                                deadline_ctx.as_deref(),
                                read_tap_slot.as_deref(),
                                &mut ext,
                            )
                            .await?;
                            let user_id = tenant_from_org_id(&org_id, &ext)?;
                            let trace_id =
                                mimir_common::tracing::current_trace_id().unwrap_or_default();
                            let _act = ingester.insert_activity(|| {
                                request_activity(
                                    &user_id,
                                    &trace_id,
                                    "Ingester/MetricsForLabelMatchers",
                                    ActivityRequest::MetricsForLabelMatchers(&inner),
                                )
                            });
                            let bytes = ingester
                                .handle_metrics_for_label_matchers_raw(&md, &user_id, inner)
                                .await
                                .map_err(|e| read_error_status(&ext, e))?;
                            Ok(Response::new(bytes))
                        })
                    }
                }

                let mut mflm_grpc = tonic::server::Grpc::new(RawMetricsForLabelMatchersCodec);
                for enc in accept_compression[..accept_compression_len]
                    .iter()
                    .flatten()
                {
                    mflm_grpc = mflm_grpc.accept_compressed(*enc);
                }
                let effective_tonic_send = send_compression.or_else(|| match echo_encoding {
                    Some("gzip")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Gzip) =>
                    {
                        Some(CompressionEncoding::Gzip)
                    }
                    Some("zstd")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Zstd) =>
                    {
                        Some(CompressionEncoding::Zstd)
                    }
                    _ => None,
                });
                if let Some(enc) = effective_tonic_send {
                    mflm_grpc = mflm_grpc.send_compressed(enc);
                }
                mflm_grpc = mflm_grpc
                    .max_decoding_message_size(usize::MAX)
                    .max_encoding_message_size(usize::MAX);
                let mflm_resp = READ_TAP_ADMITTED
                    .scope(
                        true,
                        scope_deadline_ctx(
                            deadline_ctx.clone(),
                            mflm_grpc.unary(
                                RawMetricsForLabelMatchersSvc {
                                    ingester,
                                    read_tap_slot: read_tap_slot.clone(),
                                    deadline_ctx: deadline_ctx.clone(),
                                },
                                req,
                            ),
                        ),
                    )
                    .await;
                let mflm_resp = if let Some(custom_resp_enc @ ("snappy" | "s2")) = echo_encoding
                {
                    if mflm_resp.status() == http::StatusCode::OK
                        && !mflm_resp.headers().contains_key("grpc-status")
                        && mflm_resp.headers().get("grpc-encoding").is_none()
                    {
                        let (mut parts, body) = mflm_resp.into_parts();
                        parts.headers.insert(
                            "grpc-encoding",
                            http::HeaderValue::from_static(custom_resp_enc),
                        );
                        let enc = if custom_resp_enc == "s2" {
                            CustomGrpcEncoding::S2
                        } else {
                            CustomGrpcEncoding::Snappy
                        };
                        http::Response::from_parts(
                            parts,
                            tonic::body::BoxBody::new(SnappyCompressBody::new(
                                body,
                                enc,
                                max_send_len,
                            )),
                        )
                    } else {
                        mflm_resp
                    }
                } else if max_send_len < usize::MAX
                    && !mflm_resp.headers().contains_key("grpc-status")
                {
                    mflm_resp
                        .map(|b| tonic::body::BoxBody::new(MaxSendFrameBody::new(b, max_send_len)))
                } else {
                    mflm_resp
                };
                if let Some(mut guard) = read_tap_slot.as_ref().and_then(|s| s.lock().take()) {
                    if mflm_resp.headers().contains_key("grpc-status") {
                        guard.finish(false);
                        mflm_resp
                    } else {
                        let permit = guard.take_for_body();
                        mflm_resp.map(|body| {
                            tonic::body::BoxBody::new(ReadTapDropBody {
                                inner: body,
                                permit,
                                deadline_ctx: deadline_ctx.clone(),
                            })
                        })
                    }
                } else {
                    mflm_resp
                }
            } else if is_ingester_rpc {
                let effective_tonic_send = send_compression.or_else(|| match echo_encoding {
                    Some("gzip")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Gzip) =>
                    {
                        Some(CompressionEncoding::Gzip)
                    }
                    Some("zstd")
                        if accept_compression[..accept_compression_len]
                            .iter()
                            .flatten()
                            .any(|e| *e == CompressionEncoding::Zstd) =>
                    {
                        Some(CompressionEncoding::Zstd)
                    }
                    _ => None,
                });
                if let Some(enc) = effective_tonic_send {
                    if send_compression.is_none() {
                        inner = inner.send_compressed(enc);
                    }
                }
                req.extensions_mut().insert(ReadTapSlotContext {
                    slot: read_tap_slot.clone(),
                    deadline_ctx: deadline_ctx.clone(),
                });
                let resp = READ_TAP_ADMITTED
                    .scope(
                        true,
                        scope_deadline_ctx(deadline_ctx.clone(), async { inner.call(req).await }),
                    )
                    .await?;
                let resp = if let Some(custom_resp_enc @ ("snappy" | "s2")) = echo_encoding {
                    if resp.status() == http::StatusCode::OK
                        && !resp.headers().contains_key("grpc-status")
                        && resp.headers().get("grpc-encoding").is_none()
                    {
                        let (mut parts, body) = resp.into_parts();
                        parts.headers.insert(
                            "grpc-encoding",
                            http::HeaderValue::from_static(custom_resp_enc),
                        );
                        let enc = if custom_resp_enc == "s2" {
                            CustomGrpcEncoding::S2
                        } else {
                            CustomGrpcEncoding::Snappy
                        };
                        http::Response::from_parts(
                            parts,
                            tonic::body::BoxBody::new(SnappyCompressBody::new(
                                body,
                                enc,
                                max_send_len,
                            )),
                        )
                    } else {
                        resp
                    }
                } else if max_send_len < usize::MAX
                    && !resp.headers().contains_key("grpc-status")
                {
                    resp.map(|b| tonic::body::BoxBody::new(MaxSendFrameBody::new(b, max_send_len)))
                } else {
                    resp
                };
                if let Some(mut guard) = read_tap_slot.as_ref().and_then(|s| s.lock().take()) {
                    if resp.headers().contains_key("grpc-status") {
                        guard.finish(false);
                        resp
                    } else {
                        let permit = guard.take_for_body();
                        resp.map(|body| {
                            tonic::body::BoxBody::new(ReadTapDropBody {
                                inner: body,
                                permit,
                                deadline_ctx: deadline_ctx.clone(),
                            })
                        })
                    }
                } else {
                    resp
                }
            } else {
                inner.call(req).await?
            };

            resp.headers_mut().insert(
                "grpc-accept-encoding",
                http::HeaderValue::from_static(GRPC_ACCEPT_ENCODING_VALUE),
            );
            Ok(resp)
        })
    }
}

/// Hands a failed Push's logging decision to the server's log interceptor (dskit
/// `GRPCServerLog`: `ErrorWithStatus` logs its message; sampled errors are logged once per
/// `-ingester.error-sample-rate` occurrences, `DoNotLogError` instance-limit errors never).
fn record_push_error(ingester: &Ingester, log: Option<&GrpcErrorLog>, e: &IngesterError) {
    let rate = ingester.cfg.error_sample_rate;
    let entry = GrpcErrorLogEntry {
        err: e.to_string(),
        keep: e.should_log_error(),
        reason: (e.is_sampled() && rate != 0).then(|| format!("sampled 1/{rate}")),
    };
    match log {
        Some(log) => log.set(entry),
        // Served without the dskit server's log interceptor (embedded/tests): log here.
        None if entry.keep => {
            tracing::warn!(method = "/cortex.Ingester/Push", err = %e, "gRPC call failed");
        }
        None => {}
    }
}

/// `mapReadErrorToErrorWithStatus` plus what dskit's log / instrumentation interceptors make
/// of the Go error: `ErrorWithStatus` is logged as its message; an error wrapping
/// `context.DeadlineExceeded` is returned as-is (sent as `DeadlineExceeded`, labelled `Unknown`).
fn read_error_status(ext: &http::Extensions, e: IngesterError) -> Status {
    if let Some(log) = ext.get::<GrpcErrorLog>() {
        log.set(GrpcErrorLogEntry {
            err: e.to_string(),
            keep: true,
            reason: None,
        });
        if e.is_deadline_exceeded() {
            log.set_instrumentation_code(tonic::Code::Unknown as i32);
        }
    }
    if e.is_deadline_exceeded() {
        if let Some(flag) = ext.get::<TapDeadlineFlag>() {
            flag.0.store(true, Ordering::Relaxed);
        }
    }
    e.map_read_error_to_status()
}

/// The org ID the gRPC auth middleware puts in the request context (Mimir
/// `noauth.SetupAuthMiddleware`): with multitenancy enabled dskit
/// `ServerUserHeaderInterceptor` requires exactly one `x-scope-orgid` value (else `codes.Unknown`
/// `no org id`); with multitenancy disabled the header is ignored and `no_auth_tenant` injected.
#[allow(clippy::result_large_err)]
fn org_id_from_req<T>(ingester: &Ingester, req: &Request<T>) -> Result<String, Status> {
    if !ingester.is_multitenancy_enabled() {
        return Ok(ingester.no_auth_tenant());
    }
    extract_org_id_from_grpc_metadata(req.metadata()).map_err(|e| {
        if let Some(log) = req.extensions().get::<GrpcErrorLog>() {
            log.set(GrpcErrorLogEntry {
                err: e.to_string(),
                keep: true,
                reason: None,
            });
        }
        Status::unknown(e.to_string())
    })
}

/// dskit `tenant.TenantID` in the handler: invalid or multiple (`a|b`) tenant IDs are plain
/// errors that Mimir's `mapReadErrorToErrorWithStatus` (read RPCs) and the Push path both return
/// as `codes.Internal` with the error text (and the log interceptor logs as that text).
#[allow(clippy::result_large_err)]
fn tenant_from_org_id(org_id: &str, ext: &http::Extensions) -> Result<String, Status> {
    parse_single_tenant_id(org_id).map_err(|e| {
        if let Some(log) = ext.get::<GrpcErrorLog>() {
            log.set(GrpcErrorLogEntry {
                err: e.to_string(),
                keep: true,
                reason: None,
            });
        }
        Status::internal(e.to_string())
    })
}

/// [`org_id_from_req`] + [`tenant_from_org_id`].
#[allow(clippy::result_large_err)]
#[cfg_attr(not(test), allow(dead_code))]
fn tenant_from_req<T>(ingester: &Ingester, req: &Request<T>) -> Result<String, Status> {
    let org_id = org_id_from_req(ingester, req)?;
    tenant_from_org_id(&org_id, req.extensions())
}

#[allow(clippy::result_large_err)]
async fn prepare_read_reactive_for_request<T>(
    ingester: &Ingester,
    req: &mut Request<T>,
) -> Result<(), Status> {
    let Some(ctx) = req.extensions_mut().remove::<ReadTapSlotContext>() else {
        return Ok(());
    };
    let md = req.metadata().clone();
    prepare_read_reactive_for_slot(
        ingester,
        &md,
        ctx.deadline_ctx.as_deref(),
        ctx.slot.as_deref(),
        req.extensions_mut(),
    )
    .await
}



#[tonic::async_trait]
impl ingesterpb::ingester_server::Ingester for IngesterService {
    async fn push(
        &self,
        request: Request<cortexpb::WriteRequest>,
    ) -> Result<Response<cortexpb::WriteResponse>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        if !self.ingester.cfg.push_grpc_method_enabled {
            let e = IngesterError::PushGrpcDisabled;
            record_push_error(&self.ingester, request.extensions().get::<GrpcErrorLog>(), &e);
            return Err(e.map_push_error_to_grpc_server_status());
        }
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        if let Err(e) = self.ingester.push_grpc(&user_id, request.get_ref()).await {
            record_push_error(&self.ingester, request.extensions().get::<GrpcErrorLog>(), &e);
            return Err(e.map_push_error_to_grpc_server_status());
        }
        Ok(Response::new(cortexpb::WriteResponse {}))
    }

    type QueryStreamStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::QueryStreamResponse, Status>> + Send + 'static>>;

    async fn query_stream(
        &self,
        mut request: Request<ingesterpb::QueryRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/QueryStream",
                ActivityRequest::Query(&inner),
            )
        });
        let err_sink = crate::query::ReadErrorSink::from_extensions(&ext);
        let stream = self
            .ingester
            .handle_query_stream_bimodal_with_sink(&md, &user_id, inner, err_sink)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(Box::pin(ActivityStream {
            inner: stream,
            _guard: act,
        })))
    }

    async fn query_exemplars(
        &self,
        mut request: Request<ingesterpb::ExemplarQueryRequest>,
    ) -> Result<Response<ingesterpb::ExemplarQueryResponse>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let _act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/QueryExemplars",
                ActivityRequest::ExemplarQuery(&inner),
            )
        });
        let resp = self
            .ingester
            .handle_query_exemplars(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn label_values(
        &self,
        mut request: Request<ingesterpb::LabelValuesRequest>,
    ) -> Result<Response<ingesterpb::LabelValuesResponse>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let matchers = crate::query::validate_label_values_matchers(request.get_ref())
            .map_err(|e| read_error_status(request.extensions(), e))?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let _act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/LabelValues",
                ActivityRequest::LabelValues(&inner),
            )
        });
        let resp = self
            .ingester
            .handle_label_values_with_matchers(&md, &user_id, inner, matchers)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn label_names(
        &self,
        mut request: Request<ingesterpb::LabelNamesRequest>,
    ) -> Result<Response<ingesterpb::LabelNamesResponse>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let _act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/LabelNames",
                ActivityRequest::LabelNames(&inner),
            )
        });
        let resp = self
            .ingester
            .handle_label_names(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn user_stats(
        &self,
        mut request: Request<ingesterpb::UserStatsRequest>,
    ) -> Result<Response<ingesterpb::UserStatsResponse>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let _act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/UserStats",
                ActivityRequest::UserStats(&inner),
            )
        });
        let resp = self
            .ingester
            .handle_user_stats(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn all_user_stats(
        &self,
        mut request: Request<ingesterpb::UserStatsRequest>,
    ) -> Result<Response<ingesterpb::UsersStatsResponse>, Status> {
        // Not per-tenant, but still behind the gRPC auth middleware.
        org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let _act = self.ingester.insert_activity(|| {
            request_activity(
                "",
                &trace_id,
                "Ingester/AllUserStats",
                ActivityRequest::UserStats(&inner),
            )
        });
        let resp = self
            .ingester
            .handle_all_user_stats(&md, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn metrics_for_label_matchers(
        &self,
        mut request: Request<ingesterpb::MetricsForLabelMatchersRequest>,
    ) -> Result<Response<ingesterpb::MetricsForLabelMatchersResponse>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let _act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/MetricsForLabelMatchers",
                ActivityRequest::MetricsForLabelMatchers(&inner),
            )
        });
        let resp = self
            .ingester
            .handle_metrics_for_label_matchers(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn metrics_metadata(
        &self,
        mut request: Request<ingesterpb::MetricsMetadataRequest>,
    ) -> Result<Response<ingesterpb::MetricsMetadataResponse>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let _act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/MetricsMetadata",
                ActivityRequest::MetricsMetadata(&inner),
            )
        });
        let resp = self
            .ingester
            .handle_metrics_metadata(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    type LabelNamesAndValuesStream = Pin<
        Box<dyn Stream<Item = Result<ingesterpb::LabelNamesAndValuesResponse, Status>> + Send + 'static>,
    >;

    async fn label_names_and_values(
        &self,
        mut request: Request<ingesterpb::LabelNamesAndValuesRequest>,
    ) -> Result<Response<Self::LabelNamesAndValuesStream>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/LabelNamesAndValues",
                ActivityRequest::LabelNamesAndValues(&inner),
            )
        });
        let err_sink = crate::query::ReadErrorSink::from_extensions(&ext);
        let stream = self
            .ingester
            .handle_label_names_and_values_rx_with_sink(&md, &user_id, inner, err_sink)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(Box::pin(ActivityStream {
            inner: stream,
            _guard: act,
        })))
    }

    type LabelValuesCardinalityStream = Pin<
        Box<
            dyn Stream<Item = Result<ingesterpb::LabelValuesCardinalityResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn label_values_cardinality(
        &self,
        mut request: Request<ingesterpb::LabelValuesCardinalityRequest>,
    ) -> Result<Response<Self::LabelValuesCardinalityStream>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/LabelValuesCardinality",
                ActivityRequest::LabelValuesCardinality(&inner),
            )
        });
        let err_sink = crate::query::ReadErrorSink::from_extensions(&ext);
        let stream = self
            .ingester
            .handle_label_values_cardinality_rx_with_sink(&md, &user_id, inner, err_sink)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(Box::pin(ActivityStream {
            inner: stream,
            _guard: act,
        })))
    }

    type ActiveSeriesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::ActiveSeriesResponse, Status>> + Send + 'static>>;

    async fn active_series(
        &self,
        mut request: Request<ingesterpb::ActiveSeriesRequest>,
    ) -> Result<Response<Self::ActiveSeriesStream>, Status> {
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/ActiveSeries",
                ActivityRequest::ActiveSeries(&inner),
            )
        });
        let err_sink = crate::query::ReadErrorSink::from_extensions(&ext);
        let stream = self
            .ingester
            .handle_active_series_rx_with_sink(&md, &user_id, inner, err_sink)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(Box::pin(ActivityStream {
            inner: stream,
            _guard: act,
        })))
    }

    type SearchLabelNamesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::SearchResultBatch, Status>> + Send + 'static>>;

    async fn search_label_names(
        &self,
        mut request: Request<ingesterpb::SearchLabelNamesRequest>,
    ) -> Result<Response<Self::SearchLabelNamesStream>, Status> {
        // dskit's auth interceptor runs first, then RPCCallProcessing; then Go validates the
        // input ahead of the tenant and the deferred read error mapping (InvalidArgument).
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let r = request.get_ref();
        let (hints, matchers) =
            build_search_hints(r.filter.as_ref(), r.ordering, r.limit, &r.matchers)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/SearchLabelNames",
                ActivityRequest::SearchLabelNames(&inner),
            )
        });
        let err_sink = crate::query::ReadErrorSink::from_extensions(&ext);
        let stream = self
            .ingester
            .handle_search_label_names_with_hints_rx_with_sink(
                &md, &user_id, inner, hints, matchers, err_sink,
            )
            .await
            .map_err(|e: IngesterError| read_error_status(&ext, e))?;
        Ok(Response::new(Box::pin(ActivityStream {
            inner: stream,
            _guard: act,
        })))
    }

    type SearchLabelValuesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::SearchResultBatch, Status>> + Send + 'static>>;

    async fn search_label_values(
        &self,
        mut request: Request<ingesterpb::SearchLabelValuesRequest>,
    ) -> Result<Response<Self::SearchLabelValuesStream>, Status> {
        // dskit's auth interceptor runs first, then RPCCallProcessing; then Go validates the
        // input ahead of the tenant and the deferred read error mapping (InvalidArgument).
        let org_id = org_id_from_req(&self.ingester, &request)?;
        prepare_read_reactive_for_request(&self.ingester, &mut request).await?;
        let r = request.get_ref();
        let (hints, matchers) =
            build_search_hints(r.filter.as_ref(), r.ordering, r.limit, &r.matchers)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
        let act = self.ingester.insert_activity(|| {
            request_activity(
                &user_id,
                &trace_id,
                "Ingester/SearchLabelValues",
                ActivityRequest::SearchLabelValues(&inner),
            )
        });
        let err_sink = crate::query::ReadErrorSink::from_extensions(&ext);
        let stream = self
            .ingester
            .handle_search_label_values_with_hints_rx_with_sink(
                &md, &user_id, inner, hints, matchers, err_sink,
            )
            .await
            .map_err(|e: IngesterError| read_error_status(&ext, e))?;
        Ok(Response::new(Box::pin(ActivityStream {
            inner: stream,
            _guard: act,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_tenant_extraction_handles_multitenancy_disabled_and_unknown_code() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Multitenancy enabled + missing header => Code::Unknown
        let req = Request::new(());
        let err = tenant_from_req(&ing, &req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unknown);

        // Multitenancy disabled + missing header => no_auth_tenant ("fake" default)
        ing.set_multitenancy(false, "fake");
        assert_eq!(tenant_from_req(&ing, &req).unwrap(), "fake");

        // Custom no_auth_tenant
        ing.set_multitenancy(false, "anonymous");
        assert_eq!(tenant_from_req(&ing, &req).unwrap(), "anonymous");

        // Multitenancy disabled: Mimir's noauth middleware injects no_auth_tenant and ignores
        // the header entirely.
        let mut with_header = Request::new(());
        with_header
            .metadata_mut()
            .insert("x-scope-orgid", "a|b".parse().unwrap());
        assert_eq!(tenant_from_req(&ing, &with_header).unwrap(), "anonymous");

        // Multitenancy enabled: `a|b` passes the interceptor and fails tenant.TenantID in the
        // handler (plain error -> codes.Internal); repeated headers fail the interceptor.
        ing.set_multitenancy(true, "anonymous");
        let err = tenant_from_req(&ing, &with_header).unwrap_err();
        assert_eq!((err.code(), err.message()), (tonic::Code::Internal, "multiple org IDs present"));
        assert_eq!(org_id_from_req(&ing, &with_header).unwrap(), "a|b");
        with_header
            .metadata_mut()
            .append("x-scope-orgid", "a|b".parse().unwrap());
        let err = org_id_from_req(&ing, &with_header).unwrap_err();
        assert_eq!((err.code(), err.message()), (tonic::Code::Unknown, "no org id"));
    }

    #[tokio::test]
    async fn grpc_push_accepts_snappy_encoding() {
        use tower::ServiceExt;

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-snappy".to_string();
        cfg.push_grpc_method_enabled = true;
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let write_req = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![
                    cortexpb::LabelPair {
                        name: "__name__".into(),
                        value: "snappy_push_metric".into(),
                    },
                    cortexpb::LabelPair {
                        name: "job".into(),
                        value: "test".into(),
                    },
                ],
                samples: vec![cortexpb::Sample {
                    value: 42.5,
                    timestamp_ms: 1_700_000_000_000,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let encoded = write_req.encode_to_vec();
        let framed = mimir_proto::snappy::encode_grpc_frame(&encoded, true).unwrap();

        let http_req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/cortex.Ingester/Push")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", "snappy")
            .header("x-scope-orgid", "tenant-snappy")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed))))
            .unwrap();

        let svc = IngesterService::new(Arc::clone(&ing))
            .into_server()
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);
        let resp = svc.oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        if let Some(gs) = resp.headers().get("grpc-status") {
            assert_eq!(gs, "0", "unexpected grpc-status: {gs:?}");
        }

        let db = ing.get_tsdb("tenant-snappy").expect("tenant tsdb created");
        assert_eq!(db.db.head().num_series(), 1);
        assert_eq!(
            resp.headers()
                .get("grpc-accept-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("snappy,s2,gzip,identity")
        );

        // Also verify QueryStream & LabelValues with grpc-encoding: snappy (and multi-chunk body)
        struct MultiChunkBody(std::collections::VecDeque<Bytes>);
        impl hyper::body::Body for MultiChunkBody {
            type Data = Bytes;
            type Error = Status;
            #[allow(clippy::result_large_err)]
            fn poll_frame(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(
                    self.0
                        .pop_front()
                        .map(|b| Ok(hyper::body::Frame::data(b))),
                )
            }
        }

        let q_req = ingesterpb::QueryRequest {
            start_timestamp_ms: 1_699_999_000_000,
            end_timestamp_ms: 1_700_001_000_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".into(),
                value: "snappy_push_metric".into(),
            }],
            streaming_chunks_batch_size: 0,
        };
        let q_framed = mimir_proto::snappy::encode_grpc_frame(&q_req.encode_to_vec(), true).unwrap();
        let mid = q_framed.len() / 2;
        let q_chunks = MultiChunkBody(std::collections::VecDeque::from([
            q_framed.slice(..mid),
            q_framed.slice(mid..),
        ]));
        let q_http = http::Request::builder()
            .method(http::Method::POST)
            .uri("/cortex.Ingester/QueryStream")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", "snappy")
            .header("x-scope-orgid", "tenant-snappy")
            .body(tonic::body::BoxBody::new(q_chunks))
            .unwrap();
        let svc2 = IngesterService::new(Arc::clone(&ing))
            .into_server()
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);
        let q_resp = svc2.oneshot(q_http).await.unwrap();
        assert_eq!(q_resp.status(), http::StatusCode::OK);
        assert_eq!(
            q_resp
                .headers()
                .get("grpc-accept-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("snappy,s2,gzip,identity")
        );
        if let Some(gs) = q_resp.headers().get("grpc-status") {
            assert_eq!(gs, "0", "unexpected QueryStream grpc-status: {gs:?}");
        }

        let lv_req = ingesterpb::LabelValuesRequest {
            label_name: "__name__".into(),
            start_timestamp_ms: 1_699_999_000_000,
            end_timestamp_ms: 1_700_001_000_000,
            matchers: None,
            limit: 0,
        };
        let lv_framed =
            mimir_proto::snappy::encode_grpc_frame(&lv_req.encode_to_vec(), false).unwrap();
        let lv_http = http::Request::builder()
            .method(http::Method::POST)
            .uri("/cortex.Ingester/LabelValues")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", "s2")
            .header("x-scope-orgid", "tenant-snappy")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(lv_framed))))
            .unwrap();
        let svc3 = IngesterService::new(Arc::clone(&ing))
            .into_server()
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);
        let lv_resp = svc3.oneshot(lv_http).await.unwrap();
        assert_eq!(lv_resp.status(), http::StatusCode::OK);
        assert_eq!(
            lv_resp
                .headers()
                .get("grpc-accept-encoding")
                .and_then(|v| v.to_str().ok()),
            Some("snappy,s2,gzip,identity")
        );
    }

    fn test_ingester(tmp: &std::path::Path, multitenancy: bool) -> Arc<Ingester> {
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-w2".to_string();
        cfg.push_grpc_method_enabled = true;
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.to_string_lossy().into_owned();
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_multitenancy(multitenancy, "anonymous");
        ing
    }

    fn series(labels: &[(&str, &str)], ts: i64) -> cortexpb::TimeSeries {
        cortexpb::TimeSeries {
            labels: labels
                .iter()
                .map(|(n, v)| cortexpb::LabelPair {
                    name: Bytes::copy_from_slice(n.as_bytes()),
                    value: Bytes::copy_from_slice(v.as_bytes()),
                })
                .collect(),
            samples: vec![cortexpb::Sample {
                value: 1.0,
                timestamp_ms: ts,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn grpc_tenant_parse_errors_are_internal_and_no_auth_tenant_overrides_header() {
        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(tmp.path(), true);
        let mut req = Request::new(());
        req.metadata_mut().insert("x-scope-orgid", "a|b".parse().unwrap());
        let err = tenant_from_req(&ing, &req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(err.message(), "multiple org IDs present");

        // noauth: the no-auth tenant is injected regardless of any x-scope-orgid.
        ing.set_multitenancy(false, "anonymous");
        assert_eq!(tenant_from_req(&ing, &req).unwrap(), "anonymous");
        assert_eq!(org_id_from_req(&ing, &Request::new(())).unwrap(), "anonymous");

        // Plain tenant parse errors are handed to the server's log interceptor as their text.
        ing.set_multitenancy(true, "anonymous");
        let log = GrpcErrorLog::default();
        req.extensions_mut().insert(log.clone());
        tenant_from_req(&ing, &req).unwrap_err();
        assert_eq!(log.take().unwrap().err, "multiple org IDs present");
    }

    #[tokio::test]
    async fn raw_push_hands_error_log_decision_to_server() {
        use tower::ServiceExt;

        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(tmp.path(), true);
        // Before Running, TapHandle rejects pre-decompression with Unavailable without reaching RawPushSvc.
        let body = cortexpb::WriteRequest {
            timeseries: vec![series(&[("job", "only_job")], 1_700_000_000_000)],
            ..Default::default()
        }
        .encode_to_vec();
        let framed = mimir_proto::snappy::encode_grpc_frame(&body, false).unwrap();
        let log = GrpcErrorLog::default();
        let mut tap_req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/cortex.Ingester/Push")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("x-scope-orgid", "t1")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed.clone()))))
            .unwrap();
        tap_req.extensions_mut().insert(log.clone());
        let tap_resp = IngesterService::new(Arc::clone(&ing))
            .into_server()
            .oneshot(tap_req)
            .await
            .unwrap();
        let tap_status = tonic::Status::from_header_map(tap_resp.headers()).expect("trailers-only error");
        assert_eq!(tap_status.code(), tonic::Code::Unavailable);
        assert!(log.take().is_none(), "TapHandle rejects before middleware/RawPushSvc");

        // Once Running, RawPushSvc executes and hands the error log decision to the server.
        ing.set_state_for_test(mimir_common::services::State::Running);
        let mut http_req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/cortex.Ingester/Push")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("x-scope-orgid", "t1")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed))))
            .unwrap();
        http_req.extensions_mut().insert(log.clone());
        let resp = IngesterService::new(Arc::clone(&ing))
            .into_server()
            .oneshot(http_req)
            .await
            .unwrap();
        let status = tonic::Status::from_header_map(resp.headers()).expect("trailers-only error");
        assert_eq!(status.code(), tonic::Code::Internal);
        let entry = log.take().expect("RawPushSvc records the error log entry");
        assert!(entry.keep);
        assert_eq!(entry.reason, None);
        assert!(entry.err.contains("no metric name label"), "{}", entry.err);
    }

    #[tokio::test]
    async fn hard_push_errors_roll_back_the_whole_request() {
        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(tmp.path(), true);
        ing.set_state_for_test(mimir_common::services::State::Running);
        let ts = 1_700_000_000_000;
        for (bad, msg) in [
            (vec![("job", "only_job")], "user=t-hard: no metric name label"),
            (vec![("a", "")], "user=t-hard: empty labelset: invalid sample"),
        ] {
            let req = cortexpb::WriteRequest {
                timeseries: vec![series(&[("__name__", "valid_s")], ts), series(&bad, ts)],
                ..Default::default()
            };
            let err = ing.push_grpc("t-hard", &req).await.unwrap_err();
            let status = err.map_push_error_to_grpc_server_status();
            assert_eq!(status.code(), tonic::Code::Internal, "{bad:?}");
            assert_eq!(status.message(), msg);
            assert!(mimir_proto::grpcutilpb::extract_cortex_error_details(&status).is_none());
        }
        let db = ing.get_tsdb("t-hard").expect("tenant tsdb");
        let m = mimir_tsdb::matcher::LabelMatcher::new(
            mimir_tsdb::matcher::MatchType::Equal,
            "__name__",
            "valid_s",
        )
        .unwrap();
        let committed = db.db.chunk_querier(0, i64::MAX).select(false, &[m]).unwrap();
        assert!(committed.is_empty(), "earlier series of the request must be rolled back");
    }

    #[tokio::test]
    async fn snappy_and_s2_grpc_zip_bombs_fail_with_resource_exhausted() {
        use tower::ServiceExt;

        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(tmp.path(), true);
        ing.set_state_for_test(mimir_common::services::State::Running);
        let max_decode_len = 4096usize;

        // 256 KiB of zeros compresses into a small Snappy framed payload (< 16 KiB, and small
        // enough to fit inside a 4 KiB-max compressed gRPC frame if we use ~64 KiB zeros -> ~3 KiB).
        let uncompressed_bomb = vec![0u8; 64 * 1024];
        let framed = mimir_proto::snappy::encode_grpc_frame(&uncompressed_bomb, true).unwrap();
        assert!(framed.len() <= max_decode_len);

        for enc in ["snappy", "s2"] {
            let http_req = http::Request::builder()
                .method(http::Method::POST)
                .uri("/cortex.Ingester/Push")
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .header("grpc-encoding", enc)
                .header("x-scope-orgid", "t1")
                .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed.clone()))))
                .unwrap();
            let resp = IngesterService::new(Arc::clone(&ing))
                .into_server()
                .max_decoding_message_size(max_decode_len)
                .oneshot(http_req)
                .await
                .unwrap();
            let status =
                tonic::Status::from_header_map(resp.headers()).expect("trailers-only error");
            assert_eq!(status.code(), tonic::Code::ResourceExhausted, "encoding={enc}");
            assert_eq!(
                status.message(),
                format!("grpc: received message after decompression larger than max {max_decode_len}"),
                "encoding={enc}"
            );
            assert_eq!(
                resp.headers()
                    .get("grpc-accept-encoding")
                    .and_then(|v| v.to_str().ok()),
                Some("snappy,s2,gzip,identity")
            );
        }
    }

    #[tokio::test]
    async fn read_tap_drop_body_records_normal_completion_on_trailers_and_cancels_on_early_drop() {
        use hyper::body::{Body as _, Frame};
        use mimir_common::limiter::{ReactiveLimiter, ReactiveLimiterConfig};
        use std::collections::VecDeque;
        use std::task::{Context, Poll};

        struct VecFramesBody(VecDeque<Result<Frame<Bytes>, Status>>);

        impl hyper::body::Body for VecFramesBody {
            type Data = Bytes;
            type Error = Status;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
                Poll::Ready(self.0.pop_front())
            }

            fn is_end_stream(&self) -> bool {
                self.0.is_empty()
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(tmp.path(), true);
        ing.set_state_for_test(mimir_common::services::State::Running);

        let rl = ReactiveLimiter::new(ReactiveLimiterConfig {
            min_limit: 1,
            initial_limit: 2,
            max_limit: 10,
            max_limit_factor: 1.0,
            min_limit_factor: 1.0,
            recent_window_min_duration: std::time::Duration::from_millis(1),
            recent_window_max_duration: std::time::Duration::from_millis(1),
            recent_window_min_samples: 1,
            initial_rejection_factor: 1.0,
            max_rejection_factor: 1.0,
            ..Default::default()
        })
        .unwrap();

        // 1) Early drop before trailers must cancel (permit.finish(true) -> drop_permit),
        // leaving reactive limiter max_inflight() at 0.
        let rp_early = rl.acquire_permit().await.unwrap();
        let mut tap_early = ing.try_start_read_tap(&http::HeaderMap::new()).unwrap();
        tap_early.set_reactive_permit(rp_early);
        let inner_early = tonic::body::BoxBody::new(VecFramesBody(VecDeque::from([
            Ok(Frame::data(Bytes::from_static(b"partial"))),
            Ok(Frame::data(Bytes::from_static(b"more"))),
        ])));
        let mut body_early = ReadTapDropBody {
            inner: inner_early,
            permit: Some(tap_early),
            deadline_ctx: None,
        };
        let _ = std::future::poll_fn(|cx| Pin::new(&mut body_early).poll_frame(cx)).await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(body_early.permit.is_some());
        drop(body_early);
        assert_eq!(rl.inflight(), 0);
        assert_eq!(
            rl.max_inflight(),
            0,
            "early drop must call drop_permit() and NOT record RTT sample"
        );

        // 2) Normal gRPC read completion: hyper stops polling after Frame::trailers and drops body.
        let rp_ok = rl.acquire_permit().await.unwrap();
        let mut tap_ok = ing.try_start_read_tap(&http::HeaderMap::new()).unwrap();
        tap_ok.set_reactive_permit(rp_ok);
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        let inner_ok = tonic::body::BoxBody::new(VecFramesBody(VecDeque::from([
            Ok(Frame::data(Bytes::from_static(b"payload"))),
            Ok(Frame::trailers(trailers)),
        ])));
        let mut body_ok = ReadTapDropBody {
            inner: inner_ok,
            permit: Some(tap_ok),
            deadline_ctx: None,
        };
        let f1 = std::future::poll_fn(|cx| Pin::new(&mut body_ok).poll_frame(cx)).await;
        assert!(f1.unwrap().unwrap().is_data());
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let f2 = std::future::poll_fn(|cx| Pin::new(&mut body_ok).poll_frame(cx)).await;
        assert!(f2.unwrap().unwrap().is_trailers());
        // Drop immediately without polling for Poll::Ready(None), mirroring hyper HTTP/2.
        assert!(body_ok.permit.is_none(), "permit must finish on Frame::trailers");
        drop(body_ok);
        assert_eq!(rl.inflight(), 0);
        assert_eq!(
            rl.max_inflight(),
            1,
            "normal completion on Frame::trailers must call rp.record()"
        );
    }

    #[tokio::test]
    async fn raw_metrics_for_label_matchers_svc_wire_parity_and_snappy() {
        use mimir_proto::prost::Message;
        use tower::ServiceExt;

        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(tmp.path(), true);
        ing.set_state_for_test(mimir_common::services::State::Running);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let write_req = cortexpb::WriteRequest {
            timeseries: vec![
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair { name: Bytes::from_static(b"__name__"), value: Bytes::from_static(b"mflm_raw_metric") },
                        cortexpb::LabelPair { name: Bytes::from_static(b"pod"), value: Bytes::from_static(b"pod-2") },
                    ],
                    samples: vec![cortexpb::Sample { value: 1.0, timestamp_ms: now }],
                    ..Default::default()
                },
                cortexpb::TimeSeries {
                    labels: vec![
                        cortexpb::LabelPair { name: Bytes::from_static(b"__name__"), value: Bytes::from_static(b"mflm_raw_metric") },
                        cortexpb::LabelPair { name: Bytes::from_static(b"pod"), value: Bytes::from_static(b"pod-1") },
                    ],
                    samples: vec![cortexpb::Sample { value: 2.0, timestamp_ms: now }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        ing.push_write_request("t-mflm", &write_req).await.unwrap();

        let mflm_req = ingesterpb::MetricsForLabelMatchersRequest {
            start_timestamp_ms: now - 60_000,
            end_timestamp_ms: now + 60_000,
            matchers_set: vec![ingesterpb::LabelMatchers {
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".into(),
                    value: "mflm_raw_metric".into(),
                }],
            }],
            limit: 0,
        };
        let req_bytes = mflm_req.encode_to_vec();
        let comp_req = mimir_proto::snappy::encode_grpc_frame(&req_bytes, true).unwrap();

        let svc = IngesterService::new(Arc::clone(&ing)).into_server();
        let http_req = http::Request::builder()
            .method(http::Method::POST)
            .uri("/cortex.Ingester/MetricsForLabelMatchers")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .header("grpc-encoding", "snappy")
            .header("grpc-accept-encoding", "snappy,identity")
            .header("x-scope-orgid", "t-mflm")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(comp_req))))
            .unwrap();

        let resp = svc.oneshot(http_req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.headers().get("grpc-encoding").unwrap(), "snappy");
        let mut resp_body = resp.into_body();
        let mut body = BytesMut::new();
        while let Some(frame_res) =
            futures::future::poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut resp_body), cx)).await
        {
            if let Ok(data) = frame_res.unwrap().into_data() {
                body.extend_from_slice(&data);
            }
        }
        let payload = mimir_proto::snappy::decode_grpc_frame(&body).unwrap();
        let decoded = ingesterpb::MetricsForLabelMatchersResponse::decode(&payload[..]).unwrap();
        assert_eq!(decoded.metric.len(), 2);
        assert_eq!(decoded.metric[0].labels[1].value.as_ref(), b"pod-1");
        assert_eq!(decoded.metric[1].labels[1].value.as_ref(), b"pod-2");
    }

    #[test]
    fn rpc_deadline_context_intersects_consistency_timeout() {
        assert_eq!(
            effective_read_consistency_timeout(std::time::Duration::from_secs(20)),
            std::time::Duration::from_secs(20)
        );
        let ctx = RpcDeadlineContext::new(Some(
            std::time::Instant::now() + std::time::Duration::from_millis(250),
        ));
        let eff = RPC_DEADLINE_CONTEXT.sync_scope(Arc::clone(&ctx), || {
            effective_read_consistency_timeout(std::time::Duration::from_secs(20))
        });
        assert!(eff <= std::time::Duration::from_millis(250));
        assert!(eff > std::time::Duration::ZERO);
        let eff_zero = RPC_DEADLINE_CONTEXT.sync_scope(ctx, || {
            effective_read_consistency_timeout(std::time::Duration::ZERO)
        });
        assert!(eff_zero <= std::time::Duration::from_millis(250));
    }

    #[tokio::test]
    async fn push_tap_pre_handler_drop_and_read_tap_guard_behavior() {
        use crate::circuit_breaker::CircuitBreaker;
        use mimir_common::limiter::{ReactiveLimiter, ReactiveLimiterConfig};
        use mimir_config::CircuitBreakerConfig;

        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(tmp.path(), true);
        ing.set_state_for_test(mimir_common::services::State::Running);

        let cb_cfg = CircuitBreakerConfig {
            enabled: true,
            failure_threshold_percentage: 2,
            failure_execution_threshold: 5,
            cooldown_period: mimir_config::GoDuration::from_secs(30),
            request_timeout: mimir_config::GoDuration::from_secs(60),
            ..Default::default()
        };
        let push_reg = prometheus::Registry::new();
        let push_cb = CircuitBreaker::new(&cb_cfg, Some(&push_reg), "push", true).unwrap().unwrap();
        push_cb.activate();
        let read_reg = prometheus::Registry::new();
        let read_cb = CircuitBreaker::new(&cb_cfg, Some(&read_reg), "read", true).unwrap().unwrap();
        read_cb.activate();

        let rl_cfg = ReactiveLimiterConfig {
            min_limit: 1,
            initial_limit: 2,
            max_limit: 10,
            max_limit_factor: 1.0,
            min_limit_factor: 1.0,
            recent_window_min_duration: std::time::Duration::from_millis(1),
            recent_window_max_duration: std::time::Duration::from_millis(1),
            recent_window_min_samples: 1,
            initial_rejection_factor: 1.0,
            max_rejection_factor: 1.0,
            ..Default::default()
        };
        let push_rl = ReactiveLimiter::new(rl_cfg.clone()).unwrap();
        let read_rl = ReactiveLimiter::new(rl_cfg).unwrap();

        let timeout_count = |reg: &prometheus::Registry| -> f64 {
            reg.gather()
                .into_iter()
                .find(|f| f.get_name() == "cortex_ingester_circuit_breaker_request_timeouts_total")
                .map(|f| f.get_metric()[0].get_counter().get_value())
                .unwrap_or(0.0)
        };

        // 1. Pre-handler drop of PushTapPermit decrements inflight counters, drops reactive permit
        // without recording an RTT sample, and records a zero-duration success on the breaker.
        {
            let push_tap = crate::push::PushTapPermit {
                started_at: std::time::Instant::now(),
                breaker_permit: push_cb.try_acquire_permit().unwrap(),
                reactive_permit: Some(push_rl.acquire_permit().await.unwrap()),
                ingester: Arc::clone(&ing),
                req_size: 10,
                finished: false,
            };
            ing.inflight_push_requests.fetch_add(1, Ordering::AcqRel);
            ing.inflight_push_requests_bytes.fetch_add(10, Ordering::AcqRel);
            drop(push_tap);
        }
        assert_eq!(ing.inflight_push_requests.load(Ordering::Acquire), 0);
        assert_eq!(ing.inflight_push_requests_bytes.load(Ordering::Acquire), 0);
        assert_eq!(timeout_count(&push_reg), 0.0);
        assert_eq!(push_cb.recorded_successes(), 1);
        assert_eq!(push_rl.max_inflight(), 0);

        // 1b. Transport-level RST_STREAM(CANCEL) at grpc-timeout (deadline in the past, but
        // not marked expired with DeadlineExceeded): ReadTapGuard MUST treat the dropped future
        // as canceled (0 breaker timeouts, drop reactive permit) to match Go's
        // closeStream(RST_STREAM CANCEL) -> context.Canceled path.
        {
            let ctx = RpcDeadlineContext::new(Some(
                std::time::Instant::now() - std::time::Duration::from_millis(10),
            ));
            let mut rtap = ing.try_start_read_tap(&http::HeaderMap::new()).unwrap();
            rtap.set_reactive_permit(read_rl.acquire_permit().await.unwrap());
            let _read_guard = ReadTapGuard {
                tap: Some(rtap),
                deadline_ctx: Some(ctx),
            };
        }
        assert_eq!(timeout_count(&read_reg), 0.0);
        assert_eq!(read_rl.max_inflight(), 0);

        // 3. Expired grpc-timeout on ReadTapGuard & ReadTapDropBody MUST record DeadlineExceeded & rp.record().
        let ctx_r1 = RpcDeadlineContext::new(Some(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
        ));
        ctx_r1.mark_expired();
        let mut rtap1 = ing.try_start_read_tap(&http::HeaderMap::new()).unwrap();
        rtap1.set_reactive_permit(read_rl.acquire_permit().await.unwrap());
        // Simulate active read breaker via ReadTapGuard with deadline_ctx expired:
        drop(rtap1);
        let bp1 = read_cb.try_acquire_permit().unwrap().unwrap();
        // Also verify ReadTapDropBody records deadline when deadline_ctx is expired:
        {
            let ctx_r2 = RpcDeadlineContext::new(Some(
                std::time::Instant::now() + std::time::Duration::from_secs(60),
            ));
            ctx_r2.mark_expired();
            let mut rtap2 = ing.try_start_read_tap(&http::HeaderMap::new()).unwrap();
            rtap2.set_reactive_permit(read_rl.acquire_permit().await.unwrap());
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            let _body = ReadTapDropBody {
                inner: tonic::body::empty_body(),
                permit: Some(rtap2),
                deadline_ctx: Some(ctx_r2),
            };
        }
        assert!(read_rl.max_inflight() >= 1);
        bp1.finish(std::time::Duration::from_millis(1), true);
        assert_eq!(timeout_count(&read_reg), 1.0);
    }

    #[tokio::test]
    async fn w22_deep_03_non_client_streaming_frame_body_decodes_multi_member_gzip() {
        use std::io::Write as _;

        #[allow(clippy::result_large_err)]
        async fn collect_body(mut b: NonClientStreamingFrameBody) -> Result<BytesMut, Status> {
            let mut out = BytesMut::new();
            while let Some(frame_res) = futures::future::poll_fn(|cx| {
                hyper::body::Body::poll_frame(Pin::new(&mut b), cx)
            })
            .await
            {
                if let Ok(data) = frame_res?.into_data() {
                    out.extend_from_slice(&data);
                }
            }
            Ok(out)
        }

        let mut e1 = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e1.write_all(b"first-member-").unwrap();
        let mut combined = e1.finish().unwrap();

        let mut e2 = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e2.write_all(b"second-member").unwrap();
        combined.extend_from_slice(&e2.finish().unwrap());

        let mut frame = Vec::with_capacity(5 + combined.len());
        frame.push(1);
        frame.extend_from_slice(&(combined.len() as u32).to_be_bytes());
        frame.extend_from_slice(&combined);

        let body = tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(frame.clone()))));
        let dec_body = NonClientStreamingFrameBody::new(body, FrameDecompressAlgo::Gzip, 1024);
        let out = collect_body(dec_body).await.unwrap();
        assert_eq!(out[0], 0);
        let len = u32::from_be_bytes([out[1], out[2], out[3], out[4]]) as usize;
        assert_eq!(&out[5..5 + len], b"first-member-second-member");

        // Bomb across multiple members must still be rejected with ResourceExhausted.
        let bomb_body = tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(frame))));
        let bomb_dec = NonClientStreamingFrameBody::new(bomb_body, FrameDecompressAlgo::Gzip, 16);
        let err = collect_body(bomb_dec).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn only_replica_bypasses_saturated_read_reactive_limiter() {
        use mimir_proto::prost::Message;
        use tower::ServiceExt;

        fn encode_uncompressed_grpc<M: Message>(msg: &M) -> Bytes {
            let payload = msg.encode_to_vec();
            let mut frame = Vec::with_capacity(5 + payload.len());
            frame.push(0);
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(&payload);
            Bytes::from(frame)
        }

        async fn drain_grpc_status(resp: http::Response<tonic::body::BoxBody>) -> tonic::Code {
            if let Some(status) = tonic::Status::from_header_map(resp.headers()) {
                return status.code();
            }
            let mut body = resp.into_body();
            let mut trailer_status = tonic::Code::Ok;
            while let Some(frame_res) = futures::future::poll_fn(|cx| {
                hyper::body::Body::poll_frame(Pin::new(&mut body), cx)
            })
            .await
            {
                let frame = frame_res.unwrap();
                if let Some(trailers) = frame.trailers_ref() {
                    if let Some(st) = tonic::Status::from_header_map(trailers) {
                        trailer_status = st.code();
                    }
                }
            }
            trailer_status
        }

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-only-replica".to_string();
        cfg.read_reactive_limiter = mimir_config::ReactiveLimiterConfig {
            enabled: true,
            min_limit: 1,
            max_limit: 1,
            initial_limit: 1,
            // Enable a small queue so requests enter `prepare_read_reactive_for_slot`
            // (after `try_start_read_tap`) and time out on the saturated limiter unless
            // `__only_replica__` bypasses `prepare_read_reactive`.
            initial_rejection_factor: 1.0,
            max_rejection_factor: 2.0,
            ..Default::default()
        };
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
            cfg,
            blocks_cfg,
            mimir_config::IngestStorageConfig::default(),
            mimir_config::Overrides::new(mimir_config::LimitsConfig::default()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        ing.set_state_for_test(mimir_common::services::State::Running);

        let _held = ing
            .reactive_limiter
            .read
            .as_ref()
            .expect("read reactive limiter enabled")
            .acquire_permit()
            .await
            .unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let label_names_frame = encode_uncompressed_grpc(&ingesterpb::LabelNamesRequest {
            start_timestamp_ms: now - 60_000,
            end_timestamp_ms: now + 60_000,
            ..Default::default()
        });
        let query_stream_frame = encode_uncompressed_grpc(&ingesterpb::QueryRequest {
            start_timestamp_ms: now - 60_000,
            end_timestamp_ms: now + 60_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".into(),
                value: "up".into(),
            }],
            ..Default::default()
        });
        let mflm_frame = encode_uncompressed_grpc(&ingesterpb::MetricsForLabelMatchersRequest {
            start_timestamp_ms: now - 60_000,
            end_timestamp_ms: now + 60_000,
            matchers_set: vec![ingesterpb::LabelMatchers {
                matchers: vec![ingesterpb::LabelMatcher {
                    r#type: ingesterpb::MatchType::Equal as i32,
                    name: "__name__".into(),
                    value: "up".into(),
                }],
            }],
            limit: 0,
        });

        let cases = [
            ("/cortex.Ingester/LabelNames", label_names_frame),
            ("/cortex.Ingester/QueryStream", query_stream_frame),
            ("/cortex.Ingester/MetricsForLabelMatchers", mflm_frame),
        ];

        for (uri, payload) in &cases {
            let svc = IngesterService::new(Arc::clone(&ing)).into_server();
            let mut req_rejected = http::Request::builder()
                .method(http::Method::POST)
                .uri(*uri)
                .header(http::header::CONTENT_TYPE, "application/grpc")
                .header("x-scope-orgid", "t-only-replica")
                .body(tonic::body::BoxBody::new(SingleChunkBody(Some(
                    payload.clone(),
                ))))
                .unwrap();
            req_rejected.extensions_mut().insert(RpcDeadlineContext::new(Some(
                std::time::Instant::now() + std::time::Duration::from_millis(20),
            )));
            let resp_rejected = svc.oneshot(req_rejected).await.unwrap();
            assert_eq!(
                drain_grpc_status(resp_rejected).await,
                tonic::Code::Unavailable,
                "expected Unavailable without only-replica header on {uri}"
            );

            for hdr_name in ["__only_replica__", "x-only-replica"] {
                let svc = IngesterService::new(Arc::clone(&ing)).into_server();
                let mut req_ok = http::Request::builder()
                    .method(http::Method::POST)
                    .uri(*uri)
                    .header(http::header::CONTENT_TYPE, "application/grpc")
                    .header("x-scope-orgid", "t-only-replica")
                    .header(hdr_name, "true")
                    .body(tonic::body::BoxBody::new(SingleChunkBody(Some(
                        payload.clone(),
                    ))))
                    .unwrap();
                req_ok.extensions_mut().insert(RpcDeadlineContext::new(Some(
                    std::time::Instant::now() + std::time::Duration::from_millis(20),
                )));
                let resp_ok = svc.oneshot(req_ok).await.unwrap();
                assert_eq!(
                    resp_ok.status(),
                    http::StatusCode::OK,
                    "expected 200 OK with {hdr_name}: true on {uri}"
                );
                assert_eq!(
                    drain_grpc_status(resp_ok).await,
                    tonic::Code::Ok,
                    "expected gRPC Ok with {hdr_name}: true on {uri}"
                );
            }
        }
    }
}
