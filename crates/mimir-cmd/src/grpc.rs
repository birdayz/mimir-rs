//! gRPC server services: `httpgrpc.HTTP` bridge and `grpc.health.v1.Health`.
//!
//! Ported from:
//! - github.com/grafana/dskit/httpgrpc/server/server.go
//! - github.com/grafana/dskit/grpcutil/health_check.go

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::Router;
use futures::Future;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request as HttpRequestMsg, Uri};
use mimir_common::services::State;
use mimir_ingester::Ingester;
use mimir_proto::grpcutilpb::status_with_http_response;
use mimir_proto::httpgrpcpb;
use mimir_proto::httpgrpcpb::http_server::Http;
use tonic::{Request, Response, Status};
use tower::ServiceExt;

/// Health check callback determining whether the node is serving gRPC traffic.
pub type HealthCheckFn = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone, PartialEq, prost::Message)]
pub struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    pub service: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HealthCheckResponse {
    /// 0=UNKNOWN, 1=SERVING, 2=NOT_SERVING, 3=SERVICE_UNKNOWN
    #[prost(int32, tag = "1")]
    pub status: i32,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HealthListRequest {}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HealthListResponse {
    #[prost(map = "string, message", tag = "1")]
    pub statuses: HashMap<String, HealthCheckResponse>,
}

const SERVING_STATUS: i32 = 1;
const NOT_SERVING_STATUS: i32 = 2;

/// Tonic service implementing `grpc.health.v1.Health` (`/grpc.health.v1.Health/Check`, `Watch`, `List`).
#[derive(Clone)]
pub struct HealthService {
    shutdown_requested: Arc<AtomicBool>,
    ingester: Option<Arc<Ingester>>,
}

impl HealthService {
    pub fn new(shutdown_requested: Arc<AtomicBool>, ingester: Option<Arc<Ingester>>) -> Self {
        Self {
            shutdown_requested,
            ingester,
        }
    }

    pub fn is_serving(&self) -> bool {
        if self.shutdown_requested.load(Ordering::Acquire) {
            return false;
        }
        if let Some(ref ing) = self.ingester {
            let st = ing.state();
            return matches!(st, State::Running | State::Stopping);
        }
        true
    }

    fn check_response(&self) -> HealthCheckResponse {
        HealthCheckResponse {
            status: if self.is_serving() {
                SERVING_STATUS
            } else {
                NOT_SERVING_STATUS
            },
        }
    }
}

impl tonic::server::NamedService for HealthService {
    const NAME: &'static str = "grpc.health.v1.Health";
}

impl tower::Service<http::Request<tonic::body::BoxBody>> for HealthService {
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let this = self.clone();
        match req.uri().path() {
            "/grpc.health.v1.Health/Check" => {
                struct CheckSvc(HealthService);
                impl tonic::server::UnaryService<HealthCheckRequest> for CheckSvc {
                    type Response = HealthCheckResponse;
                    type Future = Pin<
                        Box<
                            dyn Future<Output = Result<Response<Self::Response>, Status>>
                                + Send
                                + 'static,
                        >,
                    >;

                    fn call(&mut self, _request: Request<HealthCheckRequest>) -> Self::Future {
                        let resp = self.0.check_response();
                        Box::pin(async move { Ok(Response::new(resp)) })
                    }
                }
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Ok(grpc.unary(CheckSvc(this), req).await)
                })
            }
            "/grpc.health.v1.Health/Watch" => {
                struct WatchSvc;
                impl tonic::server::ServerStreamingService<HealthCheckRequest> for WatchSvc {
                    type Response = HealthCheckResponse;
                    type ResponseStream = Pin<
                        Box<
                            dyn tokio_stream::Stream<Item = Result<Self::Response, Status>>
                                + Send
                                + 'static,
                        >,
                    >;
                    type Future = Pin<
                        Box<
                            dyn Future<Output = Result<Response<Self::ResponseStream>, Status>>
                                + Send
                                + 'static,
                        >,
                    >;

                    fn call(&mut self, _request: Request<HealthCheckRequest>) -> Self::Future {
                        Box::pin(async {
                            Err(Status::unimplemented("Watching is not supported"))
                        })
                    }
                }
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Ok(grpc.server_streaming(WatchSvc, req).await)
                })
            }
            "/grpc.health.v1.Health/List" => {
                struct ListSvc(HealthService);
                impl tonic::server::UnaryService<HealthListRequest> for ListSvc {
                    type Response = HealthListResponse;
                    type Future = Pin<
                        Box<
                            dyn Future<Output = Result<Response<Self::Response>, Status>>
                                + Send
                                + 'static,
                        >,
                    >;

                    fn call(&mut self, _request: Request<HealthListRequest>) -> Self::Future {
                        let check_resp = self.0.check_response();
                        let mut statuses = HashMap::new();
                        statuses.insert("server".to_string(), check_resp);
                        Box::pin(async move {
                            Ok(Response::new(HealthListResponse { statuses }))
                        })
                    }
                }
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic::codec::ProstCodec::default());
                    Ok(grpc.unary(ListSvc(this), req).await)
                })
            }
            _ => Box::pin(async move {
                let mut response = http::Response::new(tonic::body::empty_body());
                let headers = response.headers_mut();
                headers.insert(
                    Status::GRPC_STATUS,
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

/// Bridges `/httpgrpc.HTTP/Handle` calls directly onto the Axum HTTP `Router`,
/// matching `dskit/httpgrpc/server/server.go` (`WithReturn4XXErrors`).
#[derive(Clone)]
pub struct HttpGrpcBridge {
    router: Router,
    return_4xx_errors: bool,
}

impl HttpGrpcBridge {
    pub fn new(router: Router) -> Self {
        Self {
            router,
            return_4xx_errors: true,
        }
    }
}

#[tonic::async_trait]
impl Http for HttpGrpcBridge {
    async fn handle(
        &self,
        request: Request<httpgrpcpb::HttpRequest>,
    ) -> Result<Response<httpgrpcpb::HttpResponse>, Status> {
        let req = request.into_inner();

        let method = Method::from_bytes(req.method.as_bytes())
            .map_err(|e| Status::invalid_argument(format!("invalid HTTP method: {e}")))?;

        let uri = if req.url.is_empty() {
            Uri::from_static("/")
        } else {
            req.url
                .parse::<Uri>()
                .map_err(|e| Status::invalid_argument(format!("invalid URL: {e}")))?
        };

        let mut builder = HttpRequestMsg::builder().method(method).uri(uri);
        for h in req.headers {
            let Ok(name) = HeaderName::from_bytes(h.key.as_bytes()) else {
                continue;
            };
            for v in h.values {
                if let Ok(val) = HeaderValue::from_str(&v) {
                    builder = builder.header(name.clone(), val);
                }
            }
        }

        let http_req = builder
            .body(Body::from(req.body))
            .map_err(|e| Status::internal(format!("failed to construct HTTP request: {e}")))?;

        let resp = self
            .router
            .clone()
            .oneshot(http_req)
            .await
            .map_err(|e| Status::internal(format!("router error: {e}")))?;

        let code = resp.status().as_u16() as i32;
        let mut resp_headers = resp.headers().clone();
        resp_headers.remove("x-donotlogerror");
        let error_msg_override = resp_headers
            .remove("x-errormessage")
            .and_then(|v| v.to_str().ok().map(str::to_string));

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .map_err(|e| Status::internal(format!("failed to read HTTP response body: {e}")))?;

        let headers = header_map_to_httpgrpc(&resp_headers);
        let http_resp = httpgrpcpb::HttpResponse {
            code,
            headers,
            body,
        };

        let class = code / 100;
        if class == 5 || (self.return_4xx_errors && class == 4) {
            let msg = error_msg_override.unwrap_or_else(|| {
                String::from_utf8_lossy(&http_resp.body).into_owned()
            });
            return Err(status_with_http_response(&http_resp, msg));
        }

        Ok(Response::new(http_resp))
    }
}

fn header_map_to_httpgrpc(headers: &HeaderMap) -> Vec<httpgrpcpb::Header> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for (name, val) in headers {
        if let Ok(v) = val.to_str() {
            grouped
                .entry(name.as_str().to_string())
                .or_default()
                .push(v.to_string());
        }
    }
    let mut out: Vec<httpgrpcpb::Header> = grouped
        .into_iter()
        .map(|(key, values)| httpgrpcpb::Header { key, values })
        .collect();
    out.sort_by(|a, b| a.key.cmp(&b.key));
    out
}
