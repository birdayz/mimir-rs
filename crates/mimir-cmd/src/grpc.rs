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
use bytes::Bytes;
use futures::Future;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request as HttpRequestMsg, Uri};
use crate::modules::ModuleStates;
use mimir_common::grpc_log::{GrpcErrorLog, GrpcErrorLogEntry};
use mimir_proto::grpcutilpb::{extract_http_response, status_with_http_response};
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
    modules: Arc<ModuleStates>,
}

impl HealthService {
    pub fn new(shutdown_requested: Arc<AtomicBool>, modules: Arc<ModuleStates>) -> Self {
        Self {
            shutdown_requested,
            modules,
        }
    }

    /// `grpcutil.NewHealthCheckFrom(WithShutdownRequested(..), WithManager(sm))`.
    pub fn is_serving(&self) -> bool {
        if self.shutdown_requested.load(Ordering::Acquire) {
            return false;
        }
        self.modules.is_serving()
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
        let error_log = request.extensions().get::<GrpcErrorLog>().cloned();
        let req = request.into_inner();

        // `http.NewRequest("", ...)`: an empty method (the proto3 default) means GET.
        let method = if req.method.is_empty() {
            Method::GET
        } else {
            Method::from_bytes(req.method.as_bytes())
                .map_err(|e| Status::invalid_argument(format!("invalid HTTP method: {e}")))?
        };

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
        let do_not_log_error = resp_headers.remove("x-donotlogerror").is_some();
        let error_msg_override = resp_headers
            .remove("x-errormessage")
            .and_then(|v| v.to_str().ok().map(str::to_string))
            .filter(|s| !s.is_empty());

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
            if do_not_log_error {
                // `middleware.DoNotLogError{Err: err}`: the gRPC log interceptor drops it.
                if let Some(log) = &error_log {
                    log.set(GrpcErrorLogEntry {
                        err: format!("rpc error: code = Code({code}) desc = {msg}"),
                        keep: false,
                        reason: None,
                    });
                }
            }
            return Err(status_with_http_response(&http_resp, msg));
        }

        Ok(Response::new(http_resp))
    }
}

/// Canonicalizes an HTTP header key matching Go's `http.CanonicalHeaderKey`
/// (e.g. `content-type` -> `Content-Type`, `x-scope-orgid` -> `X-Scope-Orgid`).
pub fn http_canonical_header_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    let mut upper = true;
    for b in key.bytes() {
        let ch = if upper {
            b.to_ascii_uppercase()
        } else {
            b.to_ascii_lowercase()
        };
        out.push(ch as char);
        upper = b == b'-';
    }
    out
}

fn header_map_to_httpgrpc(headers: &HeaderMap) -> Vec<httpgrpcpb::Header> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for (name, val) in headers {
        if let Ok(v) = val.to_str() {
            grouped
                .entry(http_canonical_header_key(name.as_str()))
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

fn rewrite_httpgrpc_status_header(headers: &mut HeaderMap) {
    if !headers.contains_key("grpc-status-details-bin") {
        return;
    }
    if let Some(status) = tonic::Status::from_header_map(headers) {
        if let Some(http_resp) = extract_http_response(&status) {
            if let Ok(val) = HeaderValue::from_str(&http_resp.code.to_string()) {
                headers.insert(Status::GRPC_STATUS, val);
            }
        }
    }
}

struct RewriteGrpcStatusBody {
    inner: tonic::body::BoxBody,
}

impl hyper::body::Body for RewriteGrpcStatusBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if frame.is_trailers() {
                    let mut trailers = frame
                        .into_trailers()
                        .expect("is_trailers implies into_trailers succeeds");
                    rewrite_httpgrpc_status_header(&mut trailers);
                    Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))))
                } else {
                    Poll::Ready(Some(Ok(frame)))
                }
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

/// Tower service wrapper around `HttpServer` rewriting the HTTP/2 `grpc-status`
/// header/trailer to `httpgrpc.HTTPResponse.Code` (matching `google.rpc.Status.code`)
/// so Go `grpc-go` clients' `status.FromProto` never hits `"grpc-status-details-bin mismatch"`.
#[derive(Clone)]
pub struct HttpGrpcStatusFixService<S> {
    inner: S,
}

impl<S> HttpGrpcStatusFixService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for HttpGrpcStatusFixService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> tower::Service<http::Request<tonic::body::BoxBody>> for HttpGrpcStatusFixService<S>
where
    S: tower::Service<
            http::Request<tonic::body::BoxBody>,
            Response = http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + Send
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
        let fut = self.inner.call(req);
        Box::pin(async move {
            let mut resp = fut.await?;
            rewrite_httpgrpc_status_header(resp.headers_mut());
            let resp = resp.map(|body| {
                tonic::body::BoxBody::new(RewriteGrpcStatusBody { inner: body })
            });
            Ok(resp)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use http::StatusCode;
    use mimir_proto::grpcutilpb::decode_status_details;
    use mimir_proto::httpgrpcpb::http_server::HttpServer;
    use prost::Message as _;

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

    #[test]
    fn test_http_canonical_header_key() {
        assert_eq!(http_canonical_header_key("content-type"), "Content-Type");
        assert_eq!(http_canonical_header_key("x-scope-orgid"), "X-Scope-Orgid");
        assert_eq!(http_canonical_header_key("etag"), "Etag");
    }

    #[tokio::test]
    async fn test_httpgrpc_status_fix_rewrites_grpc_status_and_headers() {
        let router = Router::new().route(
            "/err",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    [
                        ("content-type", "text/plain"),
                        ("x-donotlogerror", "1"),
                        ("x-errormessage", "custom error msg"),
                    ],
                    "raw body",
                )
            }),
        );

        let svc = HttpGrpcStatusFixService::new(HttpServer::new(HttpGrpcBridge::new(router)));

        let rpc_req = httpgrpcpb::HttpRequest {
            method: "GET".to_string(),
            url: "/err".to_string(),
            body: Bytes::new(),
            headers: Vec::new(),
        };
        let encoded = rpc_req.encode_to_vec();
        let mut framed = Vec::with_capacity(5 + encoded.len());
        framed.push(0);
        framed.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        framed.extend_from_slice(&encoded);

        let http_req = http::Request::builder()
            .method(Method::POST)
            .uri("/httpgrpc.HTTP/Handle")
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
                framed,
            )))))
            .unwrap();

        let resp = svc.oneshot(http_req).await.unwrap();
        let grpc_status = resp
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .expect("grpc-status header present");
        assert_eq!(grpc_status, "404");

        let parsed = tonic::Status::from_header_map(resp.headers()).expect("parse status");
        assert_eq!(parsed.message(), "custom error msg");
        let rpc_status = decode_status_details(&parsed).expect("decode google.rpc.Status");
        assert_eq!(rpc_status.code, 404);
        let http_resp = extract_http_response(&parsed).expect("extract http response");
        assert_eq!(http_resp.code, 404);
        assert!(
            http_resp
                .headers
                .iter()
                .any(|h| h.key == "Content-Type" && h.values == vec!["text/plain"]),
            "headers: {:?}",
            http_resp.headers
        );
        assert!(
            !http_resp
                .headers
                .iter()
                .any(|h| h.key.eq_ignore_ascii_case("x-donotlogerror")
                    || h.key.eq_ignore_ascii_case("x-errormessage")),
            "expected internal headers stripped: {:?}",
            http_resp.headers
        );
    }

    async fn call_bridge(
        router: Router,
        rpc_req: httpgrpcpb::HttpRequest,
        snappy: bool,
        error_log: Option<GrpcErrorLog>,
    ) -> http::Response<tonic::body::BoxBody> {
        use mimir_ingester::grpc::SnappyDecompressService;
        let svc = HttpGrpcStatusFixService::new(SnappyDecompressService::new(
            HttpServer::new(HttpGrpcBridge::new(router)),
            4 << 20,
        ));
        let framed = mimir_proto::snappy::encode_grpc_frame(&rpc_req.encode_to_vec(), snappy).unwrap();
        let mut b = http::Request::builder()
            .method(Method::POST)
            .uri("/httpgrpc.HTTP/Handle")
            .header(http::header::CONTENT_TYPE, "application/grpc");
        if snappy {
            b = b.header("grpc-encoding", "snappy");
        }
        let mut req = b
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed))))
            .unwrap();
        if let Some(log) = error_log {
            req.extensions_mut().insert(log);
        }
        svc.oneshot(req).await.unwrap()
    }

    async fn decode_http_response(resp: http::Response<tonic::body::BoxBody>) -> httpgrpcpb::HttpResponse {
        use http_body_util::BodyExt;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(body.len() >= 5, "no response message");
        httpgrpcpb::HttpResponse::decode(&body[5..]).unwrap()
    }

    #[tokio::test]
    async fn httpgrpc_empty_method_defaults_to_get_and_accepts_snappy() {
        let router = Router::new().route("/ready", get(|| async { "ready" }));
        for snappy in [false, true] {
            let resp = call_bridge(
                router.clone(),
                httpgrpcpb::HttpRequest {
                    method: String::new(),
                    url: "/ready".to_string(),
                    body: Bytes::new(),
                    headers: Vec::new(),
                },
                snappy,
                None,
            )
            .await;
            assert!(resp.headers().get("grpc-status").is_none_or(|v| v == "0"));
            let http_resp = decode_http_response(resp).await;
            assert_eq!(http_resp.code, 200, "snappy={snappy}");
            assert_eq!(&http_resp.body[..], b"ready");
        }
    }

    #[tokio::test]
    async fn httpgrpc_do_not_log_error_header_suppresses_server_log() {
        let router = Router::new()
            .route(
                "/quiet",
                get(|| async { (StatusCode::SERVICE_UNAVAILABLE, [("x-donotlogerror", "true")], "busy") }),
            )
            .route("/loud", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "busy") }));
        let get_req = |url: &str| httpgrpcpb::HttpRequest {
            method: "GET".to_string(),
            url: url.to_string(),
            body: Bytes::new(),
            headers: Vec::new(),
        };

        let log = GrpcErrorLog::default();
        let resp = call_bridge(router.clone(), get_req("/quiet"), false, Some(log.clone())).await;
        assert_eq!(resp.headers().get("grpc-status").unwrap(), "503");
        let entry = log.take().expect("DoNotLogError recorded");
        assert!(!entry.keep);

        let log = GrpcErrorLog::default();
        let _ = call_bridge(router, get_req("/loud"), false, Some(log.clone())).await;
        assert!(log.take().is_none(), "plain errors are logged by the interceptor");
    }
}
