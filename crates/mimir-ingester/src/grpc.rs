//! Tonic `cortex.Ingester` gRPC server implementation.
//!
//! Ported from `pkg/ingester/ingester_push.go`, `ingester_query.go`, `ingester_search.go`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http;
use bytes::{Buf, Bytes, BytesMut};
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
use crate::query::build_search_hints;

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
            accept_compression: Vec::new(),
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

/// `cortex.Ingester` Tonic server wrapper that decodes `/cortex.Ingester/Push`
/// directly from raw gRPC frame bytes using the zero-copy `WriteRequestScratch` decoder.
#[derive(Clone)]
pub struct IngesterZeroCopyServer {
    inner: ingesterpb::ingester_server::IngesterServer<IngesterService>,
    ingester: Arc<Ingester>,
    accept_compression: Vec<CompressionEncoding>,
    send_compression: Option<CompressionEncoding>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl IngesterZeroCopyServer {
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.accept_compression.push(encoding);
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

fn is_snappy_grpc_encoding(headers: &http::HeaderMap) -> bool {
    headers
        .get("grpc-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case(mimir_proto::snappy::SNAPPY_ENCODING_NAME))
}

#[allow(clippy::result_large_err)]
async fn decompress_snappy_grpc_request(
    req: http::Request<tonic::body::BoxBody>,
    max_decode_len: usize,
) -> Result<http::Request<tonic::body::BoxBody>, Status> {
    let (mut parts, mut body) = req.into_parts();
    let mut raw_body = BytesMut::new();
    while let Some(frame_res) =
        futures::future::poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut body), cx)).await
    {
        let frame = frame_res.map_err(|s| Status::internal(format!("reading gRPC body: {s}")))?;
        if let Ok(data) = frame.into_data() {
            if raw_body.len().saturating_add(data.len()) > max_decode_len.saturating_add(5) {
                return Err(Status::resource_exhausted(format!(
                    "compressed gRPC message exceeds max decoding size {max_decode_len}"
                )));
            }
            raw_body.extend_from_slice(&data);
        }
    }

    let mut out = Vec::with_capacity(raw_body.len().saturating_mul(2));
    let mut pos = 0usize;
    let slice = &raw_body[..];
    while pos < slice.len() {
        if slice.len() - pos < 5 {
            return Err(Status::internal("truncated gRPC snappy frame header"));
        }
        let flag = slice[pos];
        let len = u32::from_be_bytes([
            slice[pos + 1],
            slice[pos + 2],
            slice[pos + 3],
            slice[pos + 4],
        ]) as usize;
        pos += 5;
        let payload = slice.get(pos..pos + len).ok_or_else(|| {
            Status::internal("truncated gRPC snappy frame payload")
        })?;
        pos += len;

        match flag {
            0 => {
                if len > max_decode_len {
                    return Err(Status::resource_exhausted(format!(
                        "gRPC message size {len} exceeds max decoding size {max_decode_len}"
                    )));
                }
                out.push(0);
                out.extend_from_slice(&(len as u32).to_be_bytes());
                out.extend_from_slice(payload);
            }
            1 => {
                let hdr_pos = out.len();
                out.extend_from_slice(&[0u8; 5]);
                mimir_proto::snappy::decompress_framed_into(payload, &mut out).map_err(|e| {
                    Status::internal(format!("snappy decompression failed: {e}"))
                })?;
                let dec_len = out.len() - (hdr_pos + 5);
                if dec_len > max_decode_len {
                    return Err(Status::resource_exhausted(format!(
                        "decompressed gRPC message size {dec_len} exceeds max decoding size {max_decode_len}"
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

    parts.headers.remove("grpc-encoding");
    parts.headers.remove(http::header::CONTENT_LENGTH);
    Ok(http::Request::from_parts(
        parts,
        tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(out)))),
    ))
}

/// Accepts `grpc-encoding: snappy` requests for any wrapped gRPC service: Go registers dskit's
/// `grpcencoding/snappy` compressor process-wide, so every gRPC service of a Mimir server (e.g.
/// `httpgrpc.HTTP`) decodes Snappy-compressed messages, not just `cortex.Ingester`.
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
        if !is_snappy_grpc_encoding(req.headers()) {
            return Box::pin(self.inner.call(req));
        }
        // Use the service that was polled ready; leave a fresh clone behind.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let max_decode_len = self.max_decode_len;
        Box::pin(async move {
            match decompress_snappy_grpc_request(req, max_decode_len).await {
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
        let snappy_req = is_snappy_grpc_encoding(req.headers());
        let max_decode_len = self.max_decoding_message_size.unwrap_or(4 * 1024 * 1024);

        if req.uri().path() == "/cortex.Ingester/Push" {
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
                            record_push_error(&ingester, extensions.get::<GrpcErrorLog>(), &e);
                            return Err(e.map_push_error_to_grpc_server_status());
                        }
                        Ok(Response::new(cortexpb::WriteResponse {}))
                    })
                }
            }

            let mut grpc = tonic::server::Grpc::new(RawPushCodec);
            for enc in &self.accept_compression {
                grpc = grpc.accept_compressed(*enc);
            }
            if let Some(enc) = self.send_compression {
                grpc = grpc.send_compressed(enc);
            }
            if let Some(lim) = self.max_decoding_message_size {
                grpc = grpc.max_decoding_message_size(lim);
            }
            if let Some(lim) = self.max_encoding_message_size {
                grpc = grpc.max_encoding_message_size(lim);
            }
            let ingester = Arc::clone(&self.ingester);
            Box::pin(async move {
                let req = if snappy_req {
                    match decompress_snappy_grpc_request(req, max_decode_len).await {
                        Ok(r) => r,
                        Err(status) => return Ok(status.into_http()),
                    }
                } else {
                    req
                };
                Ok(grpc.unary(RawPushSvc(ingester), req).await)
            })
        } else if snappy_req {
            let mut inner = self.inner.clone();
            Box::pin(async move {
                let req = match decompress_snappy_grpc_request(req, max_decode_len).await {
                    Ok(r) => r,
                    Err(status) => return Ok(status.into_http()),
                };
                inner.call(req).await
            })
        } else {
            Box::pin(self.inner.call(req))
        }
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
}
