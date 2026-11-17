//! Tonic `cortex.Ingester` gRPC server implementation.
//!
//! Ported from `pkg/ingester/ingester_push.go`, `ingester_query.go`, `ingester_search.go`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http;
use bytes::{Buf, Bytes, BytesMut};
use futures::{Future, Stream};
use mimir_common::tenant::extract_tenant_id_from_grpc_metadata;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
use mimir_proto::prost::Message;
use tonic::codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::{Request, Response, Status};

use crate::errors::IngesterError;
use crate::ingester::Ingester;

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
                        let raw = request.into_inner();
                        if let Err(e) = ingester.push_grpc_raw(&user_id, raw).await {
                            if e.should_log_error() {
                                tracing::warn!(
                                    method = "/cortex.Ingester/Push",
                                    err = %e,
                                    "gRPC call failed"
                                );
                            }
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

#[allow(clippy::result_large_err)]
fn tenant_from_req<T>(ingester: &Ingester, req: &Request<T>) -> Result<String, Status> {
    match extract_tenant_id_from_grpc_metadata(req.metadata()) {
        Ok(id) => Ok(id),
        Err(mimir_common::tenant::TenantError::NoOrgID) if !ingester.is_multitenancy_enabled() => {
            Ok(ingester.no_auth_tenant())
        }
        Err(mimir_common::tenant::TenantError::NoOrgID) => {
            Err(Status::unknown(mimir_common::tenant::TenantError::NoOrgID.to_string()))
        }
        Err(e) => Err(Status::invalid_argument(e.to_string())),
    }
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
            if e.should_log_error() {
                tracing::warn!(method = "/cortex.Ingester/Push", err = %e, "gRPC call failed");
            }
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
        let (md, _, inner) = request.into_parts();
        let stream = self
            .ingester
            .handle_query_stream_bimodal(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(stream))
    }

    async fn query_exemplars(
        &self,
        request: Request<ingesterpb::ExemplarQueryRequest>,
    ) -> Result<Response<ingesterpb::ExemplarQueryResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_query_exemplars(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(resp))
    }

    async fn label_values(
        &self,
        request: Request<ingesterpb::LabelValuesRequest>,
    ) -> Result<Response<ingesterpb::LabelValuesResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_label_values(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(resp))
    }

    async fn label_names(
        &self,
        request: Request<ingesterpb::LabelNamesRequest>,
    ) -> Result<Response<ingesterpb::LabelNamesResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_label_names(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(resp))
    }

    async fn user_stats(
        &self,
        request: Request<ingesterpb::UserStatsRequest>,
    ) -> Result<Response<ingesterpb::UserStatsResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_user_stats(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(resp))
    }

    async fn all_user_stats(
        &self,
        request: Request<ingesterpb::UserStatsRequest>,
    ) -> Result<Response<ingesterpb::UsersStatsResponse>, Status> {
        let (md, _, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_all_user_stats(&md, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(resp))
    }

    async fn metrics_for_label_matchers(
        &self,
        request: Request<ingesterpb::MetricsForLabelMatchersRequest>,
    ) -> Result<Response<ingesterpb::MetricsForLabelMatchersResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_metrics_for_label_matchers(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(resp))
    }

    async fn metrics_metadata(
        &self,
        request: Request<ingesterpb::MetricsMetadataRequest>,
    ) -> Result<Response<ingesterpb::MetricsMetadataResponse>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let resp = self
            .ingester
            .handle_metrics_metadata(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
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
        let (md, _, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_label_names_and_values(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
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
        let (md, _, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_label_values_cardinality(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(vec_to_stream(items)))
    }

    type ActiveSeriesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::ActiveSeriesResponse, Status>> + Send + 'static>>;

    async fn active_series(
        &self,
        request: Request<ingesterpb::ActiveSeriesRequest>,
    ) -> Result<Response<Self::ActiveSeriesStream>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let stream = self
            .ingester
            .handle_active_series_rx(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(Box::pin(stream)))
    }

    type SearchLabelNamesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::SearchResultBatch, Status>> + Send + 'static>>;

    async fn search_label_names(
        &self,
        request: Request<ingesterpb::SearchLabelNamesRequest>,
    ) -> Result<Response<Self::SearchLabelNamesStream>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_search_label_names(&md, &user_id, inner)
            .await
            .map_err(|e: IngesterError| e.map_read_error_to_status())?;
        Ok(Response::new(vec_to_stream(items)))
    }

    type SearchLabelValuesStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::SearchResultBatch, Status>> + Send + 'static>>;

    async fn search_label_values(
        &self,
        request: Request<ingesterpb::SearchLabelValuesRequest>,
    ) -> Result<Response<Self::SearchLabelValuesStream>, Status> {
        let user_id = tenant_from_req(&self.ingester, &request)?;
        let (md, _, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_search_label_values(&md, &user_id, inner)
            .await
            .map_err(|e: IngesterError| e.map_read_error_to_status())?;
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
}
