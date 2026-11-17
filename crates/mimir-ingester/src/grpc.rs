//! Tonic `cortex.Ingester` gRPC server implementation.
//!
//! Ported from `pkg/ingester/ingester_push.go`, `ingester_query.go`, `ingester_search.go`.

use std::pin::Pin;
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

use crate::errors::IngesterError;
use crate::ingester::Ingester;
use crate::query::{build_search_hints, RawQueryStreamBox};

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
            inner: ingesterpb::ingester_server::IngesterServer::new(self),
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
        ingesterpb::QueryRequest::decode(src)
            .map(Some)
            .map_err(|e| Status::internal(e.to_string()))
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
        self.inner = self.inner.max_decoding_message_size(limit);
        self
    }

    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.max_encoding_message_size = Some(limit);
        self.inner = self.inner.max_encoding_message_size(limit);
        self
    }
}

impl tonic::server::NamedService for IngesterZeroCopyServer {
    const NAME: &'static str = "cortex.Ingester";
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

struct SnappyCompressBody {
    inner: tonic::body::BoxBody,
    in_buf: BytesMut,
    comp_buf: Vec<u8>,
    pending_trailers: Option<http::HeaderMap>,
    done: bool,
}

impl SnappyCompressBody {
    fn new(inner: tonic::body::BoxBody) -> Self {
        Self {
            inner,
            in_buf: BytesMut::new(),
            comp_buf: Vec::new(),
            pending_trailers: None,
            done: false,
        }
    }

    fn drain_complete_frames(&mut self) -> Option<Bytes> {
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
            if flag == 0 {
                self.comp_buf.clear();
                if mimir_proto::snappy::compress_framed_into(
                    &self.in_buf[5..frame_end],
                    &mut self.comp_buf,
                )
                .is_ok()
                {
                    if let Ok(comp_len) = u32::try_from(self.comp_buf.len()) {
                        out.reserve(5 + self.comp_buf.len());
                        out.put_u8(1);
                        out.put_u32(comp_len);
                        out.extend_from_slice(&self.comp_buf);
                        self.in_buf.advance(frame_end);
                        continue;
                    }
                }
            }
            out.extend_from_slice(&self.in_buf[..frame_end]);
            self.in_buf.advance(frame_end);
        }
        if out.is_empty() {
            None
        } else {
            Some(out.freeze())
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
            if let Some(chunk) = self.drain_complete_frames() {
                return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))));
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
async fn decompress_custom_grpc_request(
    req: http::Request<tonic::body::BoxBody>,
    encoding: CustomGrpcEncoding,
    max_decode_len: usize,
) -> Result<http::Request<tonic::body::BoxBody>, Status> {
    let (mut parts, mut body) = req.into_parts();
    let max_wire_len = max_decode_len.saturating_add(5);
    let mut single_chunk: Option<Bytes> = None;
    let mut multi_buf: Option<PooledBytesMut> = None;

    while let Some(frame_res) =
        futures::future::poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut body), cx)).await
    {
        let frame = frame_res.map_err(|s| Status::internal(format!("reading gRPC body: {s}")))?;
        if let Ok(data) = frame.into_data() {
            if data.is_empty() {
                continue;
            }
            if let Some(ref mut pooled) = multi_buf {
                if let Some(buf) = pooled.0.as_mut() {
                    if buf.len().saturating_add(data.len()) > max_wire_len {
                        return Err(Status::resource_exhausted(format!(
                            "grpc: received message larger than max ({} vs. {max_decode_len})",
                            buf.len().saturating_add(data.len()).saturating_sub(5)
                        )));
                    }
                    buf.extend_from_slice(&data);
                }
            } else if let Some(first) = single_chunk.take() {
                let total = first.len().saturating_add(data.len());
                if total > max_wire_len {
                    return Err(Status::resource_exhausted(format!(
                        "grpc: received message larger than max ({} vs. {max_decode_len})",
                        total.saturating_sub(5)
                    )));
                }
                let mut pooled = PooledBytesMut::take_or_new(total.max(4096));
                if let Some(buf) = pooled.0.as_mut() {
                    buf.extend_from_slice(&first);
                    buf.extend_from_slice(&data);
                }
                multi_buf = Some(pooled);
            } else {
                if data.len() > max_wire_len {
                    return Err(Status::resource_exhausted(format!(
                        "grpc: received message larger than max ({} vs. {max_decode_len})",
                        data.len().saturating_sub(5)
                    )));
                }
                single_chunk = Some(data);
            }
        }
    }

    let out_bytes: Bytes = if let Some(single) = single_chunk {
        if single.len() >= 5 && single[0] == 0 {
            let len = u32::from_be_bytes([single[1], single[2], single[3], single[4]]) as usize;
            if 5usize.checked_add(len) == Some(single.len()) && len <= max_decode_len {
                parts.headers.remove("grpc-encoding");
                parts.headers.remove(http::header::CONTENT_LENGTH);
                return Ok(http::Request::from_parts(
                    parts,
                    tonic::body::BoxBody::new(SingleChunkBody(Some(single))),
                ));
            }
        }
        decompress_frames_slice(&single, encoding, max_decode_len)?
    } else if let Some(ref pooled) = multi_buf {
        let slice = pooled.0.as_deref().unwrap_or(&[]);
        decompress_frames_slice(slice, encoding, max_decode_len)?
    } else {
        Bytes::new()
    };

    parts.headers.remove("grpc-encoding");
    parts.headers.remove(http::header::CONTENT_LENGTH);
    Ok(http::Request::from_parts(
        parts,
        tonic::body::BoxBody::new(SingleChunkBody(Some(out_bytes))),
    ))
}

#[allow(clippy::result_large_err)]
fn decompress_frames_slice(
    slice: &[u8],
    encoding: CustomGrpcEncoding,
    max_decode_len: usize,
) -> Result<Bytes, Status> {
    if slice.is_empty() {
        return Ok(Bytes::new());
    }
    DECOMP_SCRATCH.with(|cell| {
        let mut scratch_ref = cell.borrow_mut();
        let guard = VecResetGuard(&mut scratch_ref);
        let out = &mut *guard.0;

        let mut pos = 0usize;
        while pos < slice.len() {
            if slice.len() - pos < 5 {
                return Err(Status::internal("truncated gRPC compressed frame header"));
            }
            let flag = slice[pos];
            let len = u32::from_be_bytes([
                slice[pos + 1],
                slice[pos + 2],
                slice[pos + 3],
                slice[pos + 4],
            ]) as usize;
            pos += 5;
            let end = pos
                .checked_add(len)
                .ok_or_else(|| Status::internal("gRPC compressed frame length overflow"))?;
            let payload = slice
                .get(pos..end)
                .ok_or_else(|| Status::internal("truncated gRPC compressed frame payload"))?;
            pos = end;

            match flag {
                0 => {
                    if len > max_decode_len {
                        return Err(Status::resource_exhausted(format!(
                            "grpc: received message larger than max ({len} vs. {max_decode_len})"
                        )));
                    }
                    out.push(0);
                    out.extend_from_slice(&(len as u32).to_be_bytes());
                    out.extend_from_slice(payload);
                }
                1 => {
                    let hdr_pos = out.len();
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
                            mimir_proto::s2::decompress_framed_into(
                                payload,
                                out,
                                max_decode_len,
                            )
                            .map_err(|e| map_decomp_err("s2", e))?;
                        }
                    }
                    let dec_len = out.len() - (hdr_pos + 5);
                    if dec_len > max_decode_len {
                        return Err(Status::resource_exhausted(format!(
                            "grpc: received message after decompression larger than max {max_decode_len}"
                        )));
                    }
                    out[hdr_pos + 1..hdr_pos + 5].copy_from_slice(&(dec_len as u32).to_be_bytes());
                }
                other => {
                    return Err(Status::internal(format!(
                        "invalid gRPC compression flag {other}"
                    )));
                }
            }
        }

        Ok(Bytes::copy_from_slice(out))
    })
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

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let custom_enc = custom_grpc_encoding(req.headers());
        let max_decode_len = self.max_decoding_message_size.unwrap_or(4 * 1024 * 1024);
        let path = req.uri().path();
        let is_push = path == "/cortex.Ingester/Push";
        let is_query_stream = path == "/cortex.Ingester/QueryStream";
        let qs_echo_encoding = if is_query_stream && self.send_compression.is_none() {
            let req_enc = req
                .headers()
                .get("grpc-encoding")
                .and_then(|v| v.to_str().ok());
            let accepts = req
                .headers()
                .get("grpc-accept-encoding")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let client_accepts = |name: &str| {
                accepts
                    .split(',')
                    .any(|s| s.trim().eq_ignore_ascii_case(name))
            };
            req_enc.and_then(|enc| {
                if enc.eq_ignore_ascii_case("gzip") && client_accepts("gzip") {
                    Some("gzip")
                } else if enc.eq_ignore_ascii_case("zstd") && client_accepts("zstd") {
                    Some("zstd")
                } else if enc.eq_ignore_ascii_case(mimir_proto::snappy::SNAPPY_ENCODING_NAME)
                    && client_accepts(mimir_proto::snappy::SNAPPY_ENCODING_NAME)
                {
                    Some(mimir_proto::snappy::SNAPPY_ENCODING_NAME)
                } else if enc.eq_ignore_ascii_case(mimir_proto::s2::S2_ENCODING_NAME)
                    && client_accepts(mimir_proto::s2::S2_ENCODING_NAME)
                {
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
        let max_decoding_message_size = self.max_decoding_message_size;
        let max_encoding_message_size = self.max_encoding_message_size;
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let req = if let Some(encoding) = custom_enc {
                match decompress_custom_grpc_request(req, encoding, max_decode_len).await {
                    Ok(r) => r,
                    Err(status) => {
                        let mut resp = status.into_http();
                        resp.headers_mut().insert(
                            "grpc-accept-encoding",
                            http::HeaderValue::from_static(GRPC_ACCEPT_ENCODING_VALUE),
                        );
                        return Ok(resp);
                    }
                }
            } else {
                req
            };

            let mut resp = if is_push {
                struct RawPushSvc(Arc<Ingester>);
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
                        let ingester = Arc::clone(&self.0);
                        Box::pin(async move {
                            let user_id = tenant_from_req(&ingester, &request)?;
                            let (_, extensions, raw) = request.into_parts();
                            if let Err(e) = ingester.push_grpc_raw(&user_id, raw).await {
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
                if let Some(enc) = send_compression {
                    push_grpc = push_grpc.send_compressed(enc);
                }
                if let Some(lim) = max_decoding_message_size {
                    push_grpc = push_grpc.max_decoding_message_size(lim);
                }
                if let Some(lim) = max_encoding_message_size {
                    push_grpc = push_grpc.max_encoding_message_size(lim);
                }
                push_grpc.unary(RawPushSvc(ingester), req).await
            } else if is_query_stream {
                struct RawQueryStreamSvc(Arc<Ingester>);
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
                        let ingester = Arc::clone(&self.0);
                        Box::pin(async move {
                            let user_id = tenant_from_req(&ingester, &request)?;
                            let (md, ext, inner) = request.into_parts();
                            let stream = ingester
                                .handle_query_stream_raw(&md, &user_id, inner)
                                .await
                                .map_err(|e| read_error_status(&ext, e))?;
                            Ok(Response::new(stream))
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
                let effective_tonic_send = send_compression.or_else(|| match qs_echo_encoding {
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
                if let Some(lim) = max_decoding_message_size {
                    qs_grpc = qs_grpc.max_decoding_message_size(lim);
                }
                if let Some(lim) = max_encoding_message_size {
                    qs_grpc = qs_grpc.max_encoding_message_size(lim);
                }
                let qs_resp = qs_grpc
                    .server_streaming(RawQueryStreamSvc(ingester), req)
                    .await;
                if let Some(custom_resp_enc @ ("snappy" | "s2")) = qs_echo_encoding {
                    if qs_resp.status() == http::StatusCode::OK
                        && !qs_resp.headers().contains_key("grpc-status")
                        && qs_resp.headers().get("grpc-encoding").is_none()
                    {
                        let (mut parts, body) = qs_resp.into_parts();
                        parts.headers.insert(
                            "grpc-encoding",
                            http::HeaderValue::from_static(custom_resp_enc),
                        );
                        http::Response::from_parts(
                            parts,
                            tonic::body::BoxBody::new(SnappyCompressBody::new(body)),
                        )
                    } else {
                        qs_resp
                    }
                } else {
                    qs_resp
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
fn tenant_from_req<T>(ingester: &Ingester, req: &Request<T>) -> Result<String, Status> {
    let org_id = org_id_from_req(ingester, req)?;
    tenant_from_org_id(&org_id, req.extensions())
}

fn vec_to_stream<T: Send + 'static>(
    items: Vec<T>,
) -> Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>> {
    Box::pin(tokio_stream::iter(items.into_iter().map(Ok)))
}

#[tonic::async_trait]
impl ingesterpb::ingester_server::Ingester for IngesterService {
    async fn push(
        &self,
        request: Request<cortexpb::WriteRequest>,
    ) -> Result<Response<cortexpb::WriteResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
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
        request: Request<ingesterpb::QueryRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let stream = self
            .ingester
            .handle_query_stream_bimodal(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(stream))
    }

    async fn query_exemplars(
        &self,
        request: Request<ingesterpb::ExemplarQueryRequest>,
    ) -> Result<Response<ingesterpb::ExemplarQueryResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_query_exemplars(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn label_values(
        &self,
        request: Request<ingesterpb::LabelValuesRequest>,
    ) -> Result<Response<ingesterpb::LabelValuesResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_label_values(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn label_names(
        &self,
        request: Request<ingesterpb::LabelNamesRequest>,
    ) -> Result<Response<ingesterpb::LabelNamesResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_label_names(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn user_stats(
        &self,
        request: Request<ingesterpb::UserStatsRequest>,
    ) -> Result<Response<ingesterpb::UserStatsResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_user_stats(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn all_user_stats(
        &self,
        request: Request<ingesterpb::UserStatsRequest>,
    ) -> Result<Response<ingesterpb::UsersStatsResponse>, Status> {
        // Not per-tenant, but still behind the gRPC auth middleware.
        org_id_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_all_user_stats(&md, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn metrics_for_label_matchers(
        &self,
        request: Request<ingesterpb::MetricsForLabelMatchersRequest>,
    ) -> Result<Response<ingesterpb::MetricsForLabelMatchersResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_metrics_for_label_matchers(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(resp))
    }

    async fn metrics_metadata(
        &self,
        request: Request<ingesterpb::MetricsMetadataRequest>,
    ) -> Result<Response<ingesterpb::MetricsMetadataResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
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
        request: Request<ingesterpb::LabelNamesAndValuesRequest>,
    ) -> Result<Response<Self::LabelNamesAndValuesStream>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_label_names_and_values(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(vec_to_stream(items)))
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
        request: Request<ingesterpb::LabelValuesCardinalityRequest>,
    ) -> Result<Response<Self::LabelValuesCardinalityStream>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_label_values_cardinality(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(vec_to_stream(items)))
    }

    type ActiveSeriesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::ActiveSeriesResponse, Status>> + Send + 'static>>;

    async fn active_series(
        &self,
        request: Request<ingesterpb::ActiveSeriesRequest>,
    ) -> Result<Response<Self::ActiveSeriesStream>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, ext, inner) = request.into_parts();
        let stream = self
            .ingester
            .handle_active_series_rx(&md, &user_id, inner)
            .await
            .map_err(|e| read_error_status(&ext, e))?;
        Ok(Response::new(Box::pin(stream)))
    }

    type SearchLabelNamesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::SearchResultBatch, Status>> + Send + 'static>>;

    async fn search_label_names(
        &self,
        request: Request<ingesterpb::SearchLabelNamesRequest>,
    ) -> Result<Response<Self::SearchLabelNamesStream>, Status> {
        // dskit's auth interceptor runs first; then Go validates the input ahead of the tenant and
        // the deferred read error mapping (InvalidArgument).
        let org_id = org_id_from_req(&self.ingester, &request)?;
        let r = request.get_ref();
        let (hints, matchers) =
            build_search_hints(r.filter.as_ref(), r.ordering, r.limit, &r.matchers)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_search_label_names_with_hints(&md, &user_id, inner, hints, matchers)
            .await
            .map_err(|e: IngesterError| read_error_status(&ext, e))?;
        Ok(Response::new(vec_to_stream(items)))
    }

    type SearchLabelValuesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::SearchResultBatch, Status>> + Send + 'static>>;

    async fn search_label_values(
        &self,
        request: Request<ingesterpb::SearchLabelValuesRequest>,
    ) -> Result<Response<Self::SearchLabelValuesStream>, Status> {
        // dskit's auth interceptor runs first; then Go validates the input ahead of the tenant and
        // the deferred read error mapping (InvalidArgument).
        let org_id = org_id_from_req(&self.ingester, &request)?;
        let r = request.get_ref();
        let (hints, matchers) =
            build_search_hints(r.filter.as_ref(), r.ordering, r.limit, &r.matchers)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let user_id = tenant_from_org_id(&org_id, request.extensions())?;
        let (md, ext, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_search_label_values_with_hints(&md, &user_id, inner, hints, matchers)
            .await
            .map_err(|e: IngesterError| read_error_status(&ext, e))?;
        Ok(Response::new(vec_to_stream(items)))
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
        // Not running yet: Push fails with an (always logged) unavailable error.
        let body = cortexpb::WriteRequest {
            timeseries: vec![series(&[("__name__", "m")], 1_700_000_000_000)],
            ..Default::default()
        }
        .encode_to_vec();
        let framed = mimir_proto::snappy::encode_grpc_frame(&body, false).unwrap();
        let log = GrpcErrorLog::default();
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
        assert_ne!(status.code(), tonic::Code::Ok);
        let entry = log.take().expect("RawPushSvc records the error log entry");
        assert!(entry.keep);
        assert_eq!(entry.reason, None);
        assert!(entry.err.contains("unavailable"), "{}", entry.err);
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
}
