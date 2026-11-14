//! Ported from github.com/grafana/mimir/pkg/ingester/client/ingester.proto

use std::collections::HashMap;

use bytes::Bytes;

use crate::cortexpb;

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelNamesAndValuesRequest {
    #[prost(message, repeated, tag = "1")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(enumeration = "CountMethod", tag = "2")]
    pub count_method: i32,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelNamesAndValuesResponse {
    #[prost(message, repeated, tag = "1")]
    pub items: Vec<LabelValues>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelValues {
    #[prost(string, tag = "1")]
    pub label_name: String,
    #[prost(string, repeated, tag = "2")]
    pub values: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum CountMethod {
    InMemory = 0,
    Active = 1,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelValuesCardinalityRequest {
    #[prost(string, repeated, tag = "1")]
    pub label_names: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(enumeration = "CountMethod", tag = "3")]
    pub count_method: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct LabelValuesCardinalityResponse {
    #[prost(message, repeated, tag = "1")]
    pub items: Vec<LabelValueSeriesCount>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct LabelValueSeriesCount {
    #[prost(string, tag = "1")]
    pub label_name: String,
    #[prost(map = "string, uint64", tag = "2")]
    pub label_value_series: HashMap<String, u64>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct QueryRequest {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(uint64, tag = "100")]
    pub streaming_chunks_batch_size: u64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ExemplarQueryRequest {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatchers>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ActiveSeriesRequest {
    #[prost(message, repeated, tag = "1")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(enumeration = "active_series_request::RequestType", tag = "2")]
    pub r#type: i32,
}

pub mod active_series_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum RequestType {
        Series = 0,
        NativeHistogramSeries = 1,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum SearchOrdering {
    OrderByValueAsc = 0,
    OrderByValueDesc = 1,
    OrderByScoreDesc = 2,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SearchFilter {
    #[prost(string, repeated, tag = "1")]
    pub terms: Vec<String>,
    #[prost(bool, tag = "2")]
    pub case_insensitive: bool,
    #[prost(enumeration = "search_filter::FuzzAlg", tag = "3")]
    pub fuzz_alg: i32,
    #[prost(int32, tag = "4")]
    pub fuzz_threshold: i32,
}

pub mod search_filter {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum FuzzAlg {
        Subsequence = 0,
        JaroWinkler = 1,
        SubstringLeft = 2,
        Substring = 3,
    }
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SearchLabelNamesRequest {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(message, optional, tag = "4")]
    pub filter: Option<SearchFilter>,
    #[prost(enumeration = "SearchOrdering", tag = "5")]
    pub ordering: i32,
    #[prost(int64, tag = "6")]
    pub limit: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct SearchLabelValuesRequest {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(string, tag = "3")]
    pub name: String,
    #[prost(message, repeated, tag = "4")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(message, optional, tag = "5")]
    pub filter: Option<SearchFilter>,
    #[prost(enumeration = "SearchOrdering", tag = "6")]
    pub ordering: i32,
    #[prost(int64, tag = "7")]
    pub limit: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SearchResultBatch {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<search_result_batch::Result>,
    #[prost(string, repeated, tag = "2")]
    pub warnings: Vec<String>,
}

pub mod search_result_batch {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Result {
        #[prost(string, tag = "1")]
        pub value: String,
        #[prost(double, tag = "2")]
        pub score: f64,
    }
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct QueryStreamResponse {
    #[prost(message, repeated, tag = "3")]
    pub streaming_series: Vec<QueryStreamSeries>,
    #[prost(bool, tag = "4")]
    pub is_end_of_series_stream: bool,
    #[prost(message, repeated, tag = "5")]
    pub streaming_series_chunks: Vec<QueryStreamSeriesChunks>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct QueryStreamSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<cortexpb::LabelPair>,
    #[prost(int64, tag = "2")]
    pub chunk_count: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct QueryStreamSeriesChunks {
    #[prost(uint64, tag = "1")]
    pub series_index: u64,
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<Chunk>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ExemplarQueryResponse {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<cortexpb::TimeSeries>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelValuesRequest {
    #[prost(string, tag = "1")]
    pub label_name: String,
    #[prost(int64, tag = "2")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "3")]
    pub end_timestamp_ms: i64,
    #[prost(message, optional, tag = "4")]
    pub matchers: Option<LabelMatchers>,
    #[prost(int64, tag = "5")]
    pub limit: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelValuesResponse {
    #[prost(string, repeated, tag = "1")]
    pub label_values: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelNamesRequest {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, optional, tag = "3")]
    pub matchers: Option<LabelMatchers>,
    #[prost(int64, tag = "4")]
    pub limit: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelNamesResponse {
    #[prost(string, repeated, tag = "1")]
    pub label_names: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, prost::Message)]
pub struct UserStatsRequest {
    #[prost(enumeration = "CountMethod", tag = "1")]
    pub count_method: i32,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct UserStatsResponse {
    #[prost(double, tag = "1")]
    pub ingestion_rate: f64,
    #[prost(uint64, tag = "2")]
    pub num_series: u64,
    #[prost(double, tag = "3")]
    pub api_ingestion_rate: f64,
    #[prost(double, tag = "4")]
    pub rule_ingestion_rate: f64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct UserIdStatsResponse {
    #[prost(string, tag = "1")]
    pub user_id: String,
    #[prost(message, optional, tag = "2")]
    pub data: Option<UserStatsResponse>,
}

pub type UserIDStatsResponse = UserIdStatsResponse;

#[derive(Clone, PartialEq, prost::Message)]
pub struct UsersStatsResponse {
    #[prost(message, repeated, tag = "1")]
    pub stats: Vec<UserIdStatsResponse>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct MetricsForLabelMatchersRequest {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers_set: Vec<LabelMatchers>,
    #[prost(int64, tag = "4")]
    pub limit: i64,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct MetricsForLabelMatchersResponse {
    #[prost(message, repeated, tag = "1")]
    pub metric: Vec<cortexpb::Metric>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct MetricsMetadataRequest {
    #[prost(sint32, tag = "1")]
    pub limit: i32,
    #[prost(sint32, tag = "2")]
    pub limit_per_metric: i32,
    #[prost(string, tag = "3")]
    pub metric: String,
    #[prost(string, repeated, tag = "4")]
    pub metric_names: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct MetricsMetadataResponse {
    #[prost(message, repeated, tag = "1")]
    pub metadata: Vec<cortexpb::MetricMetadata>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct ActiveSeriesResponse {
    #[prost(message, repeated, tag = "1")]
    pub metric: Vec<cortexpb::Metric>,
    #[prost(uint64, repeated, tag = "2")]
    pub bucket_count: Vec<u64>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Chunk {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(int32, tag = "3")]
    pub encoding: i32,
    #[prost(bytes = "bytes", tag = "4")]
    pub data: Bytes,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelMatchers {
    #[prost(message, repeated, tag = "1")]
    pub matchers: Vec<LabelMatcher>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum MatchType {
    Equal = 0,
    NotEqual = 1,
    RegexMatch = 2,
    RegexNoMatch = 3,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct LabelMatcher {
    #[prost(enumeration = "MatchType", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

/// Generated-equivalent Tonic gRPC client for `cortex.Ingester`.
pub mod ingester_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::let_unit_value,
        clippy::result_large_err
    )]
    use tonic::codegen::http::Uri;
    use tonic::codegen::*;

    #[derive(Debug, Clone)]
    pub struct IngesterClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl IngesterClient<tonic::transport::Channel> {
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }

    impl<T> IngesterClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }

        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }

        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> IngesterClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            IngesterClient::new(InterceptedService::new(inner, interceptor))
        }

        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }

        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }

        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }

        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }

        pub async fn push(
            &mut self,
            request: impl tonic::IntoRequest<super::cortexpb::WriteRequest>,
        ) -> std::result::Result<tonic::Response<super::cortexpb::WriteResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/Push");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "Push"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn query_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::QueryRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::QueryStreamResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/QueryStream");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "QueryStream"));
            self.inner.server_streaming(req, path, codec).await
        }

        pub async fn query_exemplars(
            &mut self,
            request: impl tonic::IntoRequest<super::ExemplarQueryRequest>,
        ) -> std::result::Result<tonic::Response<super::ExemplarQueryResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/QueryExemplars");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "QueryExemplars"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn label_values(
            &mut self,
            request: impl tonic::IntoRequest<super::LabelValuesRequest>,
        ) -> std::result::Result<tonic::Response<super::LabelValuesResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/LabelValues");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "LabelValues"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn label_names(
            &mut self,
            request: impl tonic::IntoRequest<super::LabelNamesRequest>,
        ) -> std::result::Result<tonic::Response<super::LabelNamesResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/LabelNames");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "LabelNames"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn user_stats(
            &mut self,
            request: impl tonic::IntoRequest<super::UserStatsRequest>,
        ) -> std::result::Result<tonic::Response<super::UserStatsResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/UserStats");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "UserStats"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn all_user_stats(
            &mut self,
            request: impl tonic::IntoRequest<super::UserStatsRequest>,
        ) -> std::result::Result<tonic::Response<super::UsersStatsResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/AllUserStats");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "AllUserStats"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn metrics_for_label_matchers(
            &mut self,
            request: impl tonic::IntoRequest<super::MetricsForLabelMatchersRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MetricsForLabelMatchersResponse>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/cortex.Ingester/MetricsForLabelMatchers");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "MetricsForLabelMatchers"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn metrics_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::MetricsMetadataRequest>,
        ) -> std::result::Result<tonic::Response<super::MetricsMetadataResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/MetricsMetadata");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "MetricsMetadata"));
            self.inner.unary(req, path, codec).await
        }

        pub async fn label_names_and_values(
            &mut self,
            request: impl tonic::IntoRequest<super::LabelNamesAndValuesRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::LabelNamesAndValuesResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/cortex.Ingester/LabelNamesAndValues");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "LabelNamesAndValues"));
            self.inner.server_streaming(req, path, codec).await
        }

        pub async fn label_values_cardinality(
            &mut self,
            request: impl tonic::IntoRequest<super::LabelValuesCardinalityRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::LabelValuesCardinalityResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/cortex.Ingester/LabelValuesCardinality");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "LabelValuesCardinality"));
            self.inner.server_streaming(req, path, codec).await
        }

        pub async fn active_series(
            &mut self,
            request: impl tonic::IntoRequest<super::ActiveSeriesRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::ActiveSeriesResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/ActiveSeries");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "ActiveSeries"));
            self.inner.server_streaming(req, path, codec).await
        }

        pub async fn search_label_names(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchLabelNamesRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::SearchResultBatch>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/SearchLabelNames");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "SearchLabelNames"));
            self.inner.server_streaming(req, path, codec).await
        }

        pub async fn search_label_values(
            &mut self,
            request: impl tonic::IntoRequest<super::SearchLabelValuesRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::SearchResultBatch>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unknown(format!("Service was not ready: {}", e.into()))
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/cortex.Ingester/SearchLabelValues");
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("cortex.Ingester", "SearchLabelValues"));
            self.inner.server_streaming(req, path, codec).await
        }
    }
}

/// Generated-equivalent Tonic gRPC server for `cortex.Ingester`.
pub mod ingester_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;

    #[async_trait]
    pub trait Ingester: std::marker::Send + std::marker::Sync + 'static {
        async fn push(
            &self,
            request: tonic::Request<super::cortexpb::WriteRequest>,
        ) -> std::result::Result<tonic::Response<super::cortexpb::WriteResponse>, tonic::Status>;

        type QueryStreamStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::QueryStreamResponse, tonic::Status>,
            > + std::marker::Send
            + 'static;

        async fn query_stream(
            &self,
            request: tonic::Request<super::QueryRequest>,
        ) -> std::result::Result<tonic::Response<Self::QueryStreamStream>, tonic::Status>;

        async fn query_exemplars(
            &self,
            request: tonic::Request<super::ExemplarQueryRequest>,
        ) -> std::result::Result<tonic::Response<super::ExemplarQueryResponse>, tonic::Status>;

        async fn label_values(
            &self,
            request: tonic::Request<super::LabelValuesRequest>,
        ) -> std::result::Result<tonic::Response<super::LabelValuesResponse>, tonic::Status>;

        async fn label_names(
            &self,
            request: tonic::Request<super::LabelNamesRequest>,
        ) -> std::result::Result<tonic::Response<super::LabelNamesResponse>, tonic::Status>;

        async fn user_stats(
            &self,
            request: tonic::Request<super::UserStatsRequest>,
        ) -> std::result::Result<tonic::Response<super::UserStatsResponse>, tonic::Status>;

        async fn all_user_stats(
            &self,
            request: tonic::Request<super::UserStatsRequest>,
        ) -> std::result::Result<tonic::Response<super::UsersStatsResponse>, tonic::Status>;

        async fn metrics_for_label_matchers(
            &self,
            request: tonic::Request<super::MetricsForLabelMatchersRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MetricsForLabelMatchersResponse>,
            tonic::Status,
        >;

        async fn metrics_metadata(
            &self,
            request: tonic::Request<super::MetricsMetadataRequest>,
        ) -> std::result::Result<tonic::Response<super::MetricsMetadataResponse>, tonic::Status>;

        type LabelNamesAndValuesStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::LabelNamesAndValuesResponse, tonic::Status>,
            > + std::marker::Send
            + 'static;

        async fn label_names_and_values(
            &self,
            request: tonic::Request<super::LabelNamesAndValuesRequest>,
        ) -> std::result::Result<tonic::Response<Self::LabelNamesAndValuesStream>, tonic::Status>;

        type LabelValuesCardinalityStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::LabelValuesCardinalityResponse, tonic::Status>,
            > + std::marker::Send
            + 'static;

        async fn label_values_cardinality(
            &self,
            request: tonic::Request<super::LabelValuesCardinalityRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::LabelValuesCardinalityStream>,
            tonic::Status,
        >;

        type ActiveSeriesStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::ActiveSeriesResponse, tonic::Status>,
            > + std::marker::Send
            + 'static;

        async fn active_series(
            &self,
            request: tonic::Request<super::ActiveSeriesRequest>,
        ) -> std::result::Result<tonic::Response<Self::ActiveSeriesStream>, tonic::Status>;

        type SearchLabelNamesStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::SearchResultBatch, tonic::Status>,
            > + std::marker::Send
            + 'static;

        async fn search_label_names(
            &self,
            request: tonic::Request<super::SearchLabelNamesRequest>,
        ) -> std::result::Result<tonic::Response<Self::SearchLabelNamesStream>, tonic::Status>;

        type SearchLabelValuesStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::SearchResultBatch, tonic::Status>,
            > + std::marker::Send
            + 'static;

        async fn search_label_values(
            &self,
            request: tonic::Request<super::SearchLabelValuesRequest>,
        ) -> std::result::Result<tonic::Response<Self::SearchLabelValuesStream>, tonic::Status>;
    }

    #[derive(Debug)]
    pub struct IngesterServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }

    impl<T> IngesterServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }

        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }

        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }

        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }

        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }

        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }

        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }

    macro_rules! dispatch_unary {
        ($self:expr, $req:expr, $svc_struct:ident, $req_ty:ty, $resp_ty:ty, $method:ident) => {{
            struct $svc_struct<T: Ingester>(pub Arc<T>);
            impl<T: Ingester> tonic::server::UnaryService<$req_ty> for $svc_struct<T> {
                type Response = $resp_ty;
                type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                fn call(&mut self, request: tonic::Request<$req_ty>) -> Self::Future {
                    let inner = Arc::clone(&self.0);
                    let fut = async move { <T as Ingester>::$method(&inner, request).await };
                    Box::pin(fut)
                }
            }
            let accept_compression_encodings = $self.accept_compression_encodings;
            let send_compression_encodings = $self.send_compression_encodings;
            let max_decoding_message_size = $self.max_decoding_message_size;
            let max_encoding_message_size = $self.max_encoding_message_size;
            let inner = Arc::clone(&$self.inner);
            let fut = async move {
                let method = $svc_struct(inner);
                let codec = tonic::codec::ProstCodec::default();
                let mut grpc = tonic::server::Grpc::new(codec)
                    .apply_compression_config(
                        accept_compression_encodings,
                        send_compression_encodings,
                    )
                    .apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                let res = grpc.unary(method, $req).await;
                Ok(res)
            };
            Box::pin(fut)
        }};
    }

    macro_rules! dispatch_server_streaming {
        ($self:expr, $req:expr, $svc_struct:ident, $req_ty:ty, $resp_ty:ty, $stream_ty:ident, $method:ident) => {{
            struct $svc_struct<T: Ingester>(pub Arc<T>);
            impl<T: Ingester> tonic::server::ServerStreamingService<$req_ty> for $svc_struct<T> {
                type Response = $resp_ty;
                type ResponseStream = T::$stream_ty;
                type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                fn call(&mut self, request: tonic::Request<$req_ty>) -> Self::Future {
                    let inner = Arc::clone(&self.0);
                    let fut = async move { <T as Ingester>::$method(&inner, request).await };
                    Box::pin(fut)
                }
            }
            let accept_compression_encodings = $self.accept_compression_encodings;
            let send_compression_encodings = $self.send_compression_encodings;
            let max_decoding_message_size = $self.max_decoding_message_size;
            let max_encoding_message_size = $self.max_encoding_message_size;
            let inner = Arc::clone(&$self.inner);
            let fut = async move {
                let method = $svc_struct(inner);
                let codec = tonic::codec::ProstCodec::default();
                let mut grpc = tonic::server::Grpc::new(codec)
                    .apply_compression_config(
                        accept_compression_encodings,
                        send_compression_encodings,
                    )
                    .apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                let res = grpc.server_streaming(method, $req).await;
                Ok(res)
            };
            Box::pin(fut)
        }};
    }

    impl<T, B> tonic::codegen::Service<http::Request<B>> for IngesterServer<T>
    where
        T: Ingester,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/cortex.Ingester/Push" => dispatch_unary!(
                    self,
                    req,
                    PushSvc,
                    super::cortexpb::WriteRequest,
                    super::cortexpb::WriteResponse,
                    push
                ),
                "/cortex.Ingester/QueryStream" => dispatch_server_streaming!(
                    self,
                    req,
                    QueryStreamSvc,
                    super::QueryRequest,
                    super::QueryStreamResponse,
                    QueryStreamStream,
                    query_stream
                ),
                "/cortex.Ingester/QueryExemplars" => dispatch_unary!(
                    self,
                    req,
                    QueryExemplarsSvc,
                    super::ExemplarQueryRequest,
                    super::ExemplarQueryResponse,
                    query_exemplars
                ),
                "/cortex.Ingester/LabelValues" => dispatch_unary!(
                    self,
                    req,
                    LabelValuesSvc,
                    super::LabelValuesRequest,
                    super::LabelValuesResponse,
                    label_values
                ),
                "/cortex.Ingester/LabelNames" => dispatch_unary!(
                    self,
                    req,
                    LabelNamesSvc,
                    super::LabelNamesRequest,
                    super::LabelNamesResponse,
                    label_names
                ),
                "/cortex.Ingester/UserStats" => dispatch_unary!(
                    self,
                    req,
                    UserStatsSvc,
                    super::UserStatsRequest,
                    super::UserStatsResponse,
                    user_stats
                ),
                "/cortex.Ingester/AllUserStats" => dispatch_unary!(
                    self,
                    req,
                    AllUserStatsSvc,
                    super::UserStatsRequest,
                    super::UsersStatsResponse,
                    all_user_stats
                ),
                "/cortex.Ingester/MetricsForLabelMatchers" => dispatch_unary!(
                    self,
                    req,
                    MetricsForLabelMatchersSvc,
                    super::MetricsForLabelMatchersRequest,
                    super::MetricsForLabelMatchersResponse,
                    metrics_for_label_matchers
                ),
                "/cortex.Ingester/MetricsMetadata" => dispatch_unary!(
                    self,
                    req,
                    MetricsMetadataSvc,
                    super::MetricsMetadataRequest,
                    super::MetricsMetadataResponse,
                    metrics_metadata
                ),
                "/cortex.Ingester/LabelNamesAndValues" => dispatch_server_streaming!(
                    self,
                    req,
                    LabelNamesAndValuesSvc,
                    super::LabelNamesAndValuesRequest,
                    super::LabelNamesAndValuesResponse,
                    LabelNamesAndValuesStream,
                    label_names_and_values
                ),
                "/cortex.Ingester/LabelValuesCardinality" => dispatch_server_streaming!(
                    self,
                    req,
                    LabelValuesCardinalitySvc,
                    super::LabelValuesCardinalityRequest,
                    super::LabelValuesCardinalityResponse,
                    LabelValuesCardinalityStream,
                    label_values_cardinality
                ),
                "/cortex.Ingester/ActiveSeries" => dispatch_server_streaming!(
                    self,
                    req,
                    ActiveSeriesSvc,
                    super::ActiveSeriesRequest,
                    super::ActiveSeriesResponse,
                    ActiveSeriesStream,
                    active_series
                ),
                "/cortex.Ingester/SearchLabelNames" => dispatch_server_streaming!(
                    self,
                    req,
                    SearchLabelNamesSvc,
                    super::SearchLabelNamesRequest,
                    super::SearchResultBatch,
                    SearchLabelNamesStream,
                    search_label_names
                ),
                "/cortex.Ingester/SearchLabelValues" => dispatch_server_streaming!(
                    self,
                    req,
                    SearchLabelValuesSvc,
                    super::SearchLabelValuesRequest,
                    super::SearchResultBatch,
                    SearchLabelValuesStream,
                    search_label_values
                ),
                _ => Box::pin(async move {
                    let mut response = http::Response::new(empty_body());
                    let headers = response.headers_mut();
                    headers.insert(
                        tonic::Status::GRPC_STATUS,
                        (tonic::Code::Unimplemented as i32).into(),
                    );
                    headers.insert(
                        http::header::CONTENT_TYPE,
                        tonic::metadata::GRPC_CONTENT_TYPE,
                    );
                    Ok(response)
                }),
            }
        }
    }

    impl<T> Clone for IngesterServer<T> {
        fn clone(&self) -> Self {
            let inner = Arc::clone(&self.inner);
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }

    impl<T> tonic::server::NamedService for IngesterServer<T> {
        const NAME: &'static str = "cortex.Ingester";
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn test_ingester_proto_wire_tags() {
        let qreq = QueryRequest {
            start_timestamp_ms: 100,
            end_timestamp_ms: 200,
            matchers: vec![LabelMatcher {
                r#type: MatchType::RegexMatch as i32,
                name: "__name__".to_string(),
                value: "up.*".to_string(),
            }],
            streaming_chunks_batch_size: 1000,
        };
        let wire = qreq.encode_to_vec();
        let dec = QueryRequest::decode(wire.as_slice()).unwrap();
        assert_eq!(dec, qreq);

        let qresp = QueryStreamResponse {
            streaming_series: vec![QueryStreamSeries {
                labels: vec![cortexpb::LabelPair::from_static("__name__", "up")],
                chunk_count: 2,
            }],
            is_end_of_series_stream: true,
            streaming_series_chunks: vec![QueryStreamSeriesChunks {
                series_index: 0,
                chunks: vec![Chunk {
                    start_timestamp_ms: 100,
                    end_timestamp_ms: 200,
                    encoding: 1,
                    data: Bytes::from_static(&[1, 2, 3, 4]),
                }],
            }],
        };
        let wire_resp = qresp.encode_to_vec();
        assert_eq!(
            QueryStreamResponse::decode(wire_resp.as_slice()).unwrap(),
            qresp
        );
    }
}
