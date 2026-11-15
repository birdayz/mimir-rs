//! Tonic `cortex.Ingester` gRPC server implementation.
//!
//! Ported from `pkg/ingester/ingester_push.go`, `ingester_query.go`, `ingester_search.go`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::http;
use bytes::{Buf, Bytes};
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
                        let user_id = tenant_from_req(&request)?;
                        let raw = request.into_inner();
                        if let Err(e) = ingester.push_grpc_raw(&user_id, &raw).await {
                            if e.should_log_error() {
                                tracing::warn!(
                                    method = "/cortex.Ingester/Push",
                                    err = %e,
                                    "gRPC call failed"
                                );
                            }
                            return Err(e.map_push_error_to_status());
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
            Box::pin(async move { Ok(grpc.unary(RawPushSvc(ingester), req).await) })
        } else {
            Box::pin(self.inner.call(req))
        }
    }
}

#[allow(clippy::result_large_err)]
fn tenant_from_req<T>(req: &Request<T>) -> Result<String, Status> {
    extract_tenant_id_from_grpc_metadata(req.metadata())
        .map_err(|e| Status::invalid_argument(e.to_string()))
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
        let user_id = tenant_from_req(&request)?;
        if let Err(e) = self.ingester.push_grpc(&user_id, request.get_ref()).await {
            if e.should_log_error() {
                tracing::warn!(method = "/cortex.Ingester/Push", err = %e, "gRPC call failed");
            }
            return Err(e.map_push_error_to_status());
        }
        Ok(Response::new(cortexpb::WriteResponse {}))
    }

    type QueryStreamStream =
        Pin<Box<dyn Stream<Item = Result<ingesterpb::QueryStreamResponse, Status>> + Send + 'static>>;

    async fn query_stream(
        &self,
        request: Request<ingesterpb::QueryRequest>,
    ) -> Result<Response<Self::QueryStreamStream>, Status> {
        let user_id = tenant_from_req(&request)?;
        let (md, _, inner) = request.into_parts();
        let stream = self
            .ingester
            .handle_query_stream_rx(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(Box::pin(stream)))
    }

    async fn query_exemplars(
        &self,
        request: Request<ingesterpb::ExemplarQueryRequest>,
    ) -> Result<Response<ingesterpb::ExemplarQueryResponse>, Status> {
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
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
        let user_id = tenant_from_req(&request)?;
        let (md, _, inner) = request.into_parts();
        let items = self
            .ingester
            .handle_search_label_values(&md, &user_id, inner)
            .await
            .map_err(|e: IngesterError| e.map_read_error_to_status())?;
        Ok(Response::new(vec_to_stream(items)))
    }
}
