//! Tonic `cortex.Ingester` gRPC server implementation.
//!
//! Ported from `pkg/ingester/ingester_push.go`, `ingester_query.go`, `ingester_search.go`.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use mimir_common::tenant::extract_tenant_id_from_grpc_metadata;
use mimir_proto::cortexpb;
use mimir_proto::ingesterpb;
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

    pub fn into_server(self) -> ingesterpb::ingester_server::IngesterServer<Self> {
        ingesterpb::ingester_server::IngesterServer::new(self)
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
        self.ingester
            .push_grpc(&user_id, request.get_ref())
            .await
            .map_err(|e| e.map_push_error_to_status())?;
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
        let items = self
            .ingester
            .handle_query_stream(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(vec_to_stream(items)))
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
        let items = self
            .ingester
            .handle_active_series(&md, &user_id, inner)
            .await
            .map_err(|e| e.map_read_error_to_status())?;
        Ok(Response::new(vec_to_stream(items)))
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
