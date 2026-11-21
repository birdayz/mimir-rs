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
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::Future;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request as HttpRequestMsg, Uri};
use crate::modules::ModuleStates;
use mimir_common::grpc_log::{GrpcErrorLog, GrpcErrorLogEntry};
use mimir_proto::grpcutilpb::{extract_http_response, status_with_http_response};
use mimir_proto::httpgrpcpb;
use mimir_proto::httpgrpcpb::http_server::Http;
use tonic::codec::CompressionEncoding;
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
    accept_compression: Vec<CompressionEncoding>,
    send_compression: Option<CompressionEncoding>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl HealthService {
    pub fn new(shutdown_requested: Arc<AtomicBool>, modules: Arc<ModuleStates>) -> Self {
        Self {
            shutdown_requested,
            modules,
            accept_compression: Vec::new(),
            send_compression: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }

    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.accept_compression.push(encoding);
        self
    }

    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.send_compression = Some(encoding);
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

    fn configure_grpc<C: tonic::codec::Codec>(&self, codec: C) -> tonic::server::Grpc<C> {
        let mut grpc = tonic::server::Grpc::new(codec);
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
        grpc
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
                    let mut grpc = this.configure_grpc(tonic::codec::ProstCodec::default());
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
                    let mut grpc = this.configure_grpc(tonic::codec::ProstCodec::default());
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
                    let mut grpc = this.configure_grpc(tonic::codec::ProstCodec::default());
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
struct CapturedMatchedRoute(Arc<parking_lot::Mutex<Option<String>>>);

#[derive(Clone)]
pub struct HttpGrpcBridge {
    router: Router,
    return_4xx_errors: bool,
}

impl HttpGrpcBridge {
    pub fn new(router: Router) -> Self {
        let router = router.route_layer(axum::middleware::from_fn(
            |req: axum::extract::Request, next: axum::middleware::Next| async move {
                if let (Some(cell), Some(mp)) = (
                    req.extensions().get::<CapturedMatchedRoute>(),
                    req.extensions().get::<axum::extract::MatchedPath>(),
                ) {
                    let mut p = mp.as_str();
                    if let Some(i) = p.rfind("/*") {
                        p = &p[..i];
                    }
                    const PPROF: &str = "/debug/pprof";
                    if let Some(i) = p.find(PPROF) {
                        if &p[i + PPROF.len()..] != "/cmdline" {
                            p = &p[..i + PPROF.len()];
                        }
                    }
                    *cell.0.lock() = Some(crate::server::metrics::make_label_value(
                        &p.replace(':', ""),
                    ));
                }
                next.run(req).await
            },
        ));
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
        let parent_span_attrs = request
            .extensions()
            .get::<crate::server::grpc_server::GrpcSpanAttributes>()
            .cloned();
        let outer_trace = request
            .extensions()
            .get::<mimir_common::tracing::TraceContext>()
            .cloned()
            .or_else(mimir_common::tracing::current_trace_context);
        let req = request.into_inner();

        // `http.NewRequest("", ...)`: an empty method (the proto3 default) means GET.
        let method = if req.method.is_empty() {
            Method::GET
        } else {
            Method::from_bytes(req.method.as_bytes())
                .map_err(|e| Status::unknown(format!("net/http: invalid method: {e}")))?
        };

        validate_go_url(&req.url)
            .map_err(|e| Status::unknown(format!("parse {:?}: {e}", req.url)))?;

        let GoUrlCleanPath {
            redirect: pending_redirect,
            path: raw_url_path,
            route_url,
        } = split_go_url_clean_redirect(&req.url);
        let uri = if pending_redirect.is_some() {
            Uri::from_static("/__clean_path_redirect__")
        } else {
            parse_go_url_to_http_uri(route_url.as_deref().unwrap_or(&req.url))
                .map_err(|e| Status::unknown(format!("parse {:?}: {e}", req.url)))?
        };

        let mut req_headers = HeaderMap::new();
        let mut raw_org_id: Option<mimir_ingester::http::RawScopeOrgId> = None;
        for h in req.headers {
            let Ok(name) = HeaderName::from_bytes(h.key.as_bytes()) else {
                continue;
            };
            let is_org_id = name.as_str().eq_ignore_ascii_case("x-scope-orgid");
            if is_org_id {
                raw_org_id = None;
            }
            // Go `httpgrpc.ToHeader`: `header[http.CanonicalHeaderKey(h.Key)] = h.Values` overwrites
            // any prior entry for the same canonical header name.
            req_headers.remove(&name);
            for (idx, v) in h.values.into_iter().enumerate() {
                match HeaderValue::from_bytes(v.as_bytes()) {
                    Ok(val) => {
                        req_headers.append(name.clone(), val);
                    }
                    Err(_) if is_org_id && idx == 0 => {
                        raw_org_id = Some(mimir_ingester::http::RawScopeOrgId(v.into_bytes()));
                    }
                    Err(_) => {}
                }
            }
        }

        let (effective_trace, parent_span_id) = match outer_trace {
            Some(ref t) if t.is_valid() => {
                mimir_common::tracing::start_server_span_context(t, false)
            }
            _ => {
                let from_inner = mimir_common::tracing::extract_from_http_headers(&req_headers);
                mimir_common::tracing::start_server_span_context(&from_inner, false)
            }
        };
        let url_path = if pending_redirect.is_some() {
            raw_url_path
        } else {
            uri.path().to_string()
        };
        let user_agent = req_headers
            .get(http::header::USER_AGENT)
            .and_then(|v| std::str::from_utf8(v.as_bytes()).ok())
            .unwrap_or("")
            .to_string();
        let matched_route_cell = Arc::new(parking_lot::Mutex::new(None));
        let mut http_req = HttpRequestMsg::builder()
            .method(method.clone())
            .uri(uri)
            .body(Body::from(req.body))
            .map_err(|e| Status::internal(format!("failed to construct HTTP request: {e}")))?;
        *http_req.headers_mut() = req_headers;
        http_req
            .extensions_mut()
            .insert(CapturedMatchedRoute(Arc::clone(&matched_route_cell)));
        if let Some(ref loc) = pending_redirect {
            http_req
                .extensions_mut()
                .insert(crate::http::PendingCleanPathRedirect(loc.clone()));
        }
        if let Some(raw_org) = raw_org_id {
            http_req.extensions_mut().insert(raw_org);
        }
        if effective_trace.is_valid() {
            http_req.extensions_mut().insert(effective_trace.clone());
        }

        let span_start = mimir_common::tracing::now_unix_nanos();
        let mut resp = mimir_common::tracing::with_trace_context(
            effective_trace.clone(),
            self.router.clone().oneshot(http_req),
        )
        .await
        .map_err(|e| Status::internal(format!("router error: {e}")))?;
        if let Some(ref loc) = pending_redirect {
            if resp.extensions().get::<crate::http::CleanPathHandled>().is_none() {
                resp = crate::http::clean_path_redirect_response(loc);
            }
        }
        let route_name = if pending_redirect.is_some() {
            String::new()
        } else {
            matched_route_cell
                .lock()
                .take()
                .unwrap_or_else(|| {
                    if resp.status() == http::StatusCode::NOT_FOUND {
                        "notfound".to_string()
                    } else {
                        String::new()
                    }
                })
        };
        let http_span_attrs = vec![
            mimir_common::tracing::SpanAttribute {
                key: "http.request.method".to_string(),
                value: mimir_common::tracing::SpanAttrValue::String(method.to_string()),
            },
            mimir_common::tracing::SpanAttribute {
                key: "http.route".to_string(),
                value: mimir_common::tracing::SpanAttrValue::String(route_name.clone()),
            },
            mimir_common::tracing::SpanAttribute {
                key: "user_agent.original".to_string(),
                value: mimir_common::tracing::SpanAttrValue::String(user_agent),
            },
            mimir_common::tracing::SpanAttribute {
                key: "url.path".to_string(),
                value: mimir_common::tracing::SpanAttrValue::String(url_path),
            },
        ];
        if let Some(ref parent_attrs) = parent_span_attrs {
            parent_attrs.0.lock().extend(http_span_attrs.clone());
        }
        if effective_trace.is_sampled() {
            if let Some(rt) = mimir_common::tracing::global_tracing() {
                if rt.exporter.is_some() {
                    let span_name = if route_name.is_empty() {
                        format!("HTTP {method}")
                    } else {
                        format!("HTTP {method} - {route_name}")
                    };
                    let mut attributes = http_span_attrs;
                    if let Some(ref dbg) = effective_trace.jaeger_debug_id {
                        attributes.push(mimir_common::tracing::SpanAttribute {
                            key: "jaeger-debug-id".to_string(),
                            value: mimir_common::tracing::SpanAttrValue::String(dbg.clone()),
                        });
                    }
                    mimir_common::tracing::export_span(mimir_common::tracing::ExportedSpan {
                        service_name: rt.service_name.clone(),
                        trace_id: effective_trace.trace_id,
                        span_id: effective_trace.span_id,
                        parent_span_id,
                        name: span_name,
                        kind: 2,
                        start_time_unix_nano: span_start,
                        end_time_unix_nano: mimir_common::tracing::now_unix_nanos(),
                        attributes,
                        status_error: resp.status().as_u16() >= 500,
                    });
                }
            }
        }

        let code = resp.status().as_u16() as i32;
        let explicit_content_length = resp
            .extensions()
            .get::<crate::http::ExplicitContentLength>()
            .is_some();
        let mut resp_headers = resp.headers().clone();
        // dskit httpgrpc runs against httptest.ResponseRecorder, which never carries server-added
        // Content-Length (unless explicitly set by the handler, e.g. /static/ files) or Connection.
        if !explicit_content_length {
            resp_headers.remove(http::header::CONTENT_LENGTH);
        }
        resp_headers.remove(http::header::CONNECTION);
        if resp
            .extensions()
            .get::<crate::server::http_middleware::MethodMismatch>()
            .is_some()
        {
            resp_headers.remove(http::header::ALLOW);
        }
        let do_not_log_error = resp_headers.remove("x-donotlogerror").is_some();
        let error_msg_override = resp_headers
            .remove("x-errormessage")
            .and_then(|v| std::str::from_utf8(v.as_bytes()).ok().map(str::to_string))
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
            let (msg, non_utf8_body) = match error_msg_override {
                Some(override_msg) => (override_msg, false),
                None => match std::str::from_utf8(&http_resp.body) {
                    Ok(s) => (s.to_string(), false),
                    Err(_) => (String::from_utf8_lossy(&http_resp.body).into_owned(), true),
                },
            };
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
            if non_utf8_body {
                // Go grpc-go: proto.Marshal(google.rpc.Status) fails with errInvalidUTF8 when
                // Status.Message is invalid UTF-8, omitting grpc-status-details-bin while still
                // sending grpc-status = http_resp.code and grpc-message = encodeGrpcMessage(body)
                // (google.golang.org/grpc/internal/transport/http_util.go:227-265).
                let encoded_msg = encode_grpc_message_like_go(&msg);
                let mut st = Status::new(
                    mimir_proto::grpcutilpb::http_code_to_grpc_code(http_resp.code),
                    msg,
                );
                if let Ok(val) = tonic::metadata::MetadataValue::try_from(http_resp.code.to_string())
                {
                    st.metadata_mut().insert("x-mimir-httpgrpc-raw-code", val);
                }
                if let Ok(val) = tonic::metadata::MetadataValue::try_from(encoded_msg) {
                    st.metadata_mut().insert("x-mimir-httpgrpc-raw-msg", val);
                }
                return Err(st);
            }
            return Err(status_with_http_response(&http_resp, msg));
        }

        Ok(Response::new(http_resp))
    }
}

/// `req.url` split the way Go `net/url.Parse` splits it, with the outcome of `gorilla/mux`
/// `cleanPath` on its path.
struct GoUrlCleanPath {
    /// `Location` of the 301 redirect `gorilla/mux` answers with (`url.String()` with the
    /// cleaned path), when the path is not canonical.
    redirect: Option<String>,
    /// `url.Path` as found in `req.url`.
    path: String,
    /// `path[?query]` to route instead of `req.url` when Go parsed a scheme-less
    /// `//authority` out of it (`//host/ready` routes `/ready`).
    route_url: Option<String>,
}

/// `net/url` `getScheme`: `(scheme, rest)`, with an empty scheme when there is none.
fn go_url_scheme(raw: &str) -> (&str, &str) {
    for (i, &c) in raw.as_bytes().iter().enumerate() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' if i > 0 => {}
            b':' if i > 0 => return (&raw[..i], &raw[i + 1..]),
            _ => return ("", raw),
        }
    }
    ("", raw)
}

/// Splits `raw` per Go `net/url.Parse` (`parse(rawURL, viaRequest = false)`) and checks whether
/// `gorilla/mux` `cleanPath` requires a 301 redirect, building its `Location` like
/// `URL.String()` (scheme, `//authority`, cleaned path, `?query` and `#fragment`).
fn split_go_url_clean_redirect(raw: &str) -> GoUrlCleanPath {
    let (without_frag, frag) = raw.split_once('#').unwrap_or((raw, ""));
    let (scheme, rest) = go_url_scheme(without_frag);
    let (rest, query, force_query) = if rest.ends_with('?') && rest.matches('?').count() == 1 {
        (&rest[..rest.len() - 1], "", true)
    } else {
        let (r, q) = rest.split_once('?').unwrap_or((rest, ""));
        (r, q, false)
    };
    let push_query_and_fragment = |loc: &mut String| {
        if force_query || !query.is_empty() {
            loc.push('?');
            loc.push_str(query);
        }
        if !frag.is_empty() {
            loc.push('#');
            loc.push_str(frag);
        }
    };

    if !scheme.is_empty() && !rest.starts_with('/') {
        // Rootless path with a scheme: `url.Opaque`. `url.Path` is empty, so `cleanPath` always
        // redirects, and `URL.String()` prints the opaque part instead of the path.
        let mut loc = format!("{}:{rest}", scheme.to_ascii_lowercase());
        push_query_and_fragment(&mut loc);
        return GoUrlCleanPath {
            redirect: Some(loc),
            path: String::new(),
            route_url: None,
        };
    }

    let mut authority: Option<&str> = None;
    let mut omit_host = false;
    let mut path = rest;
    if (!scheme.is_empty() || !rest.starts_with("///")) && rest.starts_with("//") {
        let after = &rest[2..];
        match after.find('/') {
            Some(i) => {
                authority = Some(&after[..i]);
                path = &after[i..];
            }
            None => {
                authority = Some(after);
                path = "";
            }
        }
    } else if !scheme.is_empty() && rest.starts_with('/') {
        omit_host = true;
    }

    let Some(cleaned) = crate::http::gorilla_mux_clean_path_redirect(path) else {
        let route_url = ((scheme.is_empty() && authority.is_some())
            || (!scheme.is_empty() && (omit_host || authority == Some(""))))
        .then(|| {
            let mut u = path.to_string();
            push_query_and_fragment(&mut u);
            u
        });
        return GoUrlCleanPath {
            redirect: None,
            path: path.to_string(),
            route_url,
        };
    };

    let mut loc = String::new();
    if !scheme.is_empty() {
        loc.push_str(&scheme.to_ascii_lowercase());
        loc.push(':');
    }
    // `cleaned` is never empty, so `URL.String()` writes `//` whenever it writes an authority.
    let authority = authority.unwrap_or("");
    if (!scheme.is_empty() || !authority.is_empty()) && !(omit_host && authority.is_empty()) {
        loc.push_str("//");
        loc.push_str(authority);
    }
    loc.push_str(&cleaned);
    push_query_and_fragment(&mut loc);
    GoUrlCleanPath {
        redirect: Some(loc),
        path: path.to_string(),
        route_url: None,
    }
}

fn validate_go_url(raw: &str) -> Result<(), String> {
    if raw.is_empty() {
        return Ok(());
    }
    let bytes = raw.as_bytes();
    for &b in bytes {
        if b < 0x20 || b == 0x7f {
            return Err("net/url: invalid control character in URL".to_string());
        }
    }
    // Match Go net/url.Parse: split off '#fragment' first (url.setFragment unescapes and rejects
    // invalid '%' in fragments), while leaving '?query' (RawQuery) unescaped.
    let (without_frag, frag_opt) = match raw.split_once('#') {
        Some((before, frag)) => (before, Some(frag)),
        None => (raw, None),
    };
    let check_escapes = |s: &str| -> Result<(), String> {
        let bytes = s.as_bytes();
        let mut idx = 0;
        while idx < bytes.len() {
            if bytes[idx] == b'%' {
                if idx + 2 >= bytes.len()
                    || !bytes[idx + 1].is_ascii_hexdigit()
                    || !bytes[idx + 2].is_ascii_hexdigit()
                {
                    return Err("invalid URL escape".to_string());
                }
                idx += 3;
            } else {
                idx += 1;
            }
        }
        Ok(())
    };
    let before_query = without_frag.split('?').next().unwrap_or("");
    check_escapes(before_query)?;
    if let Some(frag) = frag_opt {
        check_escapes(frag)?;
    }
    let before_pq = without_frag.split(['/', '?']).next().unwrap_or("");
    if let Some((scheme, _)) = before_pq.split_once(':') {
        if scheme.is_empty() {
            return Err("missing protocol scheme".to_string());
        }
        let sb = scheme.as_bytes();
        if !sb[0].is_ascii_alphabetic()
            || !sb
                .iter()
                .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
        {
            return Err("first path segment in URL cannot contain colon".to_string());
        }
    }
    Ok(())
}

/// Parses `req.url` with Go `net/url.Parse` tolerance (accepting relative paths without leading
/// `/` and unescaped `{}`/`[]`/`"`/`|`/`^`/spaces in query strings) and encodes disallowed
/// RFC 3986 characters so `http::Uri` can pass it into Axum's `Router`.
fn parse_go_url_to_http_uri(raw: &str) -> Result<Uri, String> {
    if raw.is_empty() {
        return Ok(Uri::from_static("/"));
    }
    validate_go_url(raw)?;
    let without_frag = raw.split('#').next().unwrap_or(raw);
    let (path_part, query_part) = match without_frag.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (without_frag, None),
    };

    let encode_ascii_unsafe = |s: &str, out: &mut String| {
        for &b in s.as_bytes() {
            let ok = b.is_ascii_graphic()
                && !matches!(b, b'{' | b'}' | b'[' | b']' | b'"' | b'|' | b'^' | b'`' | b'<' | b'>' | b'\\');
            if ok {
                out.push(b as char);
            } else {
                use std::fmt::Write as _;
                let _ = write!(out, "%{b:02X}");
            }
        }
    };

    let mut encoded = String::with_capacity(without_frag.len());
    if let Some((scheme, rest)) = path_part.split_once("://") {
        if let Some(after_empty_auth) = rest.strip_prefix('/') {
            // Valid Go URL with scheme + empty authority (e.g. `http:///ready`): route `/ready`.
            encoded.push('/');
            encode_ascii_unsafe(after_empty_auth, &mut encoded);
        } else {
            encoded.push_str(scheme);
            encoded.push_str("://");
            if let Some((authority, path)) = rest.split_once('/') {
                encoded.push_str(authority);
                encoded.push('/');
                encode_ascii_unsafe(path, &mut encoded);
            } else {
                encoded.push_str(rest);
            }
        }
    } else {
        let (scheme, rest) = go_url_scheme(path_part);
        if !scheme.is_empty() && rest.starts_with('/') {
            // Valid Go URL with scheme + OmitHost (e.g. `http:/ready`): route `/ready`.
            encode_ascii_unsafe(rest, &mut encoded);
        } else {
            encode_ascii_unsafe(path_part, &mut encoded);
        }
    }
    if let Some(q) = query_part {
        encoded.push('?');
        encode_ascii_unsafe(q, &mut encoded);
    }
    if encoded.is_empty() {
        encoded.push('/');
    }
    encoded.parse::<Uri>().map_err(|e| e.to_string())
}

const GRPC_ACCEPT_ENCODING_ALL: &str = "identity,gzip,zstd,snappy,s2";

fn s2_compress_framed_into(input: &[u8], out: &mut BytesMut) -> Result<(), std::io::Error> {
    let start = out.len();
    mimir_proto::snappy::compress_framed_buf(input, out)?;
    // Both S2 and Snappy framing share chunk types 0x00/0x01, masked CRC32C checksums,
    // and raw Snappy blocks <= 64 KiB (a strict subset of S2 raw blocks).
    if out.len() >= start + 10 {
        out[start + 4..start + 10].copy_from_slice(b"S2sTwO");
    }
    Ok(())
}

fn compress_grpc_payload_into(
    enc: &str,
    payload: &[u8],
    out: &mut BytesMut,
) -> Result<(), std::io::Error> {
    use std::io::Write as _;
    match enc {
        "gzip" => {
            let mut encoder =
                flate2::write::GzEncoder::new(out.writer(), flate2::Compression::default());
            encoder.write_all(payload)?;
            encoder.finish()?;
            Ok(())
        }
        "zstd" => zstd::stream::copy_encode(payload, out.writer(), 0),
        "snappy" => mimir_proto::snappy::compress_framed_buf(payload, out),
        "s2" => s2_compress_framed_into(payload, out),
        _ => {
            out.put_slice(payload);
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
enum BoundedDecompressAlgo {
    Identity,
    Gzip,
    Zstd,
    Snappy,
    S2,
}

/// Pre-Tonic streaming request body wrapper enforcing `max_recv` on wire frame lengths,
/// bounding `gzip`/`zstd`/`snappy`/`s2` decompression to `max_recv`, and enforcing
/// non-client-streaming gRPC cardinality (0 frames or 2+ frames -> `Code::Internal`) before
/// emitting frame 1 so inner handlers never execute on frame 1 when frame 2 is present.
struct BoundedGrpcDecompressBody {
    inner: tonic::body::BoxBody,
    algo: BoundedDecompressAlgo,
    max_recv: usize,
    in_buf: BytesMut,
    out_buf: BytesMut,
    buffered_frame1: Option<Bytes>,
    frames_seen: usize,
    pending_trailers: Option<http::HeaderMap>,
    done: bool,
}

impl BoundedGrpcDecompressBody {
    fn new(inner: tonic::body::BoxBody, algo: BoundedDecompressAlgo, max_recv: usize) -> Self {
        Self {
            inner,
            algo,
            max_recv,
            in_buf: BytesMut::new(),
            out_buf: BytesMut::new(),
            buffered_frame1: None,
            frames_seen: 0,
            pending_trailers: None,
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

    /// Decompresses at most one complete gRPC frame from `self.in_buf`, returning its
    /// uncompressed 5-byte-framed `Bytes` if a complete frame was available.
    #[allow(clippy::result_large_err)]
    fn drain_one_frame(&mut self) -> Result<Option<Bytes>, Status> {
        use std::io::Read as _;
        if self.in_buf.len() < 5 {
            return Ok(None);
        }
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
            return Ok(None);
        }
        match self.algo {
            BoundedDecompressAlgo::Identity => match flag {
                0 => {
                    return Ok(Some(self.in_buf.split_to(frame_end).freeze()));
                }
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
            BoundedDecompressAlgo::Gzip
            | BoundedDecompressAlgo::Zstd
            | BoundedDecompressAlgo::Snappy
            | BoundedDecompressAlgo::S2 => match flag {
                0 => {
                    self.out_buf.extend_from_slice(&self.in_buf[..frame_end]);
                }
                1 => {
                    let compressed = &self.in_buf[5..frame_end];
                    let limit = (self.max_recv as u64).saturating_add(1);
                    let mut decompressed =
                        Vec::with_capacity(compressed.len().min(self.max_recv).min(64 * 1024));
                    let map_custom_err = |codec: &str, e: std::io::Error| -> Status {
                        if e.get_ref()
                            .is_some_and(|r| r.is::<mimir_proto::snappy::DecompressLimitExceeded>())
                        {
                            Status::resource_exhausted(format!(
                                "grpc: received message after decompression larger than max {}",
                                self.max_recv
                            ))
                        } else {
                            Status::internal(format!("{codec} decompression failed: {e}"))
                        }
                    };
                    match self.algo {
                        BoundedDecompressAlgo::Gzip => {
                            let mut dec = flate2::read::MultiGzDecoder::new(compressed).take(limit);
                            dec.read_to_end(&mut decompressed).map_err(|e| {
                                Status::internal(format!("gzip decompression failed: {e}"))
                            })?;
                        }
                        BoundedDecompressAlgo::Zstd => {
                            let dec = zstd::stream::read::Decoder::new(compressed).map_err(|e| {
                                Status::internal(format!("zstd decompression failed: {e}"))
                            })?;
                            dec.take(limit).read_to_end(&mut decompressed).map_err(|e| {
                                Status::internal(format!("zstd decompression failed: {e}"))
                            })?;
                        }
                        BoundedDecompressAlgo::Snappy => {
                            mimir_proto::snappy::decompress_framed_bounded_into(
                                compressed,
                                &mut decompressed,
                                self.max_recv,
                            )
                            .map_err(|e| map_custom_err("snappy", e))?;
                        }
                        BoundedDecompressAlgo::S2 => {
                            mimir_proto::s2::decompress_framed_into(
                                compressed,
                                &mut decompressed,
                                self.max_recv,
                            )
                            .map_err(|e| map_custom_err("s2", e))?;
                        }
                        BoundedDecompressAlgo::Identity => unreachable!(),
                    }
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
                    self.out_buf.put_u8(0);
                    self.out_buf.extend_from_slice(&dec_len_u32.to_be_bytes());
                    self.out_buf.extend_from_slice(&decompressed);
                }
                other => {
                    return Err(Status::internal(format!(
                        "protocol error: received message with invalid compression flag: {other} (valid flags are 0 and 1), while sending request"
                    )));
                }
            },
        }
        self.in_buf.advance(frame_end);
        Ok(Some(self.out_buf.split().freeze()))
    }

    #[allow(clippy::result_large_err)]
    fn process_buffered_frames(&mut self) -> Result<(), Status> {
        while self.in_buf.len() >= 5 {
            self.check_incoming_header()?;
            let Some(frame) = self.drain_one_frame()? else {
                break;
            };
            if self.frames_seen == 0 {
                self.buffered_frame1 = Some(frame);
                self.frames_seen = 1;
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

impl hyper::body::Body for BoundedGrpcDecompressBody {
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
        if let Err(st) = self.process_buffered_frames() {
            self.buffered_frame1 = None;
            self.done = true;
            return Poll::Ready(Some(Err(st)));
        }
        loop {
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if frame.is_data() {
                        if let Ok(data) = frame.into_data() {
                            if !data.is_empty() {
                                if self.frames_seen == 0
                                    && matches!(self.algo, BoundedDecompressAlgo::Identity)
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
                                        self.buffered_frame1 = Some(data);
                                        self.frames_seen = 1;
                                        continue;
                                    }
                                }
                                if self.in_buf.len() < 5 && self.in_buf.len() + data.len() >= 5 {
                                    let need = 5 - self.in_buf.len();
                                    self.in_buf.extend_from_slice(&data[..need]);
                                    if let Err(st) = self.check_incoming_header() {
                                        self.buffered_frame1 = None;
                                        self.done = true;
                                        return Poll::Ready(Some(Err(st)));
                                    }
                                    self.in_buf.extend_from_slice(&data[need..]);
                                } else {
                                    self.in_buf.extend_from_slice(&data);
                                    if let Err(st) = self.check_incoming_header() {
                                        self.buffered_frame1 = None;
                                        self.done = true;
                                        return Poll::Ready(Some(Err(st)));
                                    }
                                }
                                if let Err(st) = self.process_buffered_frames() {
                                    self.buffered_frame1 = None;
                                    self.done = true;
                                    return Poll::Ready(Some(Err(st)));
                                }
                            }
                        }
                    } else {
                        match frame.into_trailers() {
                            Ok(trailers) => {
                                if !self.in_buf.is_empty() {
                                    self.buffered_frame1 = None;
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
                                if let Some(f1) = self.buffered_frame1.take() {
                                    self.pending_trailers = Some(trailers);
                                    return Poll::Ready(Some(Ok(hyper::body::Frame::data(f1))));
                                }
                                self.done = true;
                                return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(
                                    trailers,
                                ))));
                            }
                            Err(other_frame) => {
                                return Poll::Ready(Some(Ok(other_frame)));
                            }
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    self.buffered_frame1 = None;
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    if !self.in_buf.is_empty() {
                        self.buffered_frame1 = None;
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
                    self.done = true;
                    if let Some(f1) = self.buffered_frame1.take() {
                        return Poll::Ready(Some(Ok(hyper::body::Frame::data(f1))));
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

fn status_to_trailers(status: Status) -> http::HeaderMap {
    let mut headers = status.into_http().into_parts().0.headers;
    headers.remove(http::header::CONTENT_TYPE);
    headers
}

struct CompressGrpcBody {
    inner: tonic::body::BoxBody,
    encoding: Option<&'static str>,
    max_send: usize,
    buf: BytesMut,
    out_buf: BytesMut,
    out_queue: std::collections::VecDeque<Bytes>,
    pending_trailers: Option<http::HeaderMap>,
    done: bool,
}

impl CompressGrpcBody {
    fn new(inner: tonic::body::BoxBody, encoding: Option<&'static str>, max_send: usize) -> Self {
        Self {
            inner,
            encoding,
            max_send,
            buf: BytesMut::new(),
            out_buf: BytesMut::new(),
            out_queue: std::collections::VecDeque::new(),
            pending_trailers: None,
            done: false,
        }
    }

    fn drain_frames(&mut self) -> Result<(), Box<Status>> {
        while self.buf.len() >= 5 {
            let flag = self.buf[0];
            let msg_len =
                u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
            let Some(frame_end) = 5usize.checked_add(msg_len) else {
                break;
            };
            if self.buf.len() < frame_end {
                break;
            }
            if flag == 0 && msg_len > 0 {
                if let Some(enc) = self.encoding {
                    let hdr_pos = self.out_buf.len();
                    self.out_buf.extend_from_slice(&[1, 0, 0, 0, 0]);
                    if let Err(e) = compress_grpc_payload_into(
                        enc,
                        &self.buf[5..frame_end],
                        &mut self.out_buf,
                    ) {
                        self.out_buf.truncate(hdr_pos);
                        return Err(Box::new(Status::internal(format!(
                            "compress gRPC response: {e}"
                        ))));
                    }
                    let comp_len = self.out_buf.len() - (hdr_pos + 5);
                    if comp_len > self.max_send {
                        self.out_buf.truncate(hdr_pos);
                        self.buf.clear();
                        return Err(Box::new(Status::resource_exhausted(format!(
                            "trying to send message larger than max ({comp_len} vs. {})",
                            self.max_send
                        ))));
                    }
                    let comp_len_u32 = u32::try_from(comp_len).map_err(|_| {
                        Box::new(Status::internal(
                            "compressed gRPC response frame exceeds u32::MAX",
                        ))
                    })?;
                    self.out_buf[hdr_pos + 1..hdr_pos + 5]
                        .copy_from_slice(&comp_len_u32.to_be_bytes());
                } else {
                    if msg_len > self.max_send {
                        self.buf.clear();
                        return Err(Box::new(Status::resource_exhausted(format!(
                            "trying to send message larger than max ({msg_len} vs. {})",
                            self.max_send
                        ))));
                    }
                    self.out_buf.extend_from_slice(&self.buf[..frame_end]);
                }
            } else {
                if msg_len > self.max_send {
                    self.buf.clear();
                    return Err(Box::new(Status::resource_exhausted(format!(
                        "trying to send message larger than max ({msg_len} vs. {})",
                        self.max_send
                    ))));
                }
                self.out_buf.extend_from_slice(&self.buf[..frame_end]);
            }
            self.buf.advance(frame_end);
        }
        if !self.out_buf.is_empty() {
            self.out_queue.push_back(self.out_buf.split().freeze());
        }
        Ok(())
    }
}

impl hyper::body::Body for CompressGrpcBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if let Some(chunk) = self.out_queue.pop_front() {
            return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))));
        }
        if let Some(trailers) = self.pending_trailers.take() {
            self.done = true;
            return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))));
        }
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if frame.is_data() {
                        if let Ok(data) = frame.into_data() {
                            self.buf.extend_from_slice(&data);
                            if let Err(status) = self.drain_frames() {
                                if !self.out_buf.is_empty() {
                                    let chunk = self.out_buf.split().freeze();
                                    self.out_queue.push_back(chunk);
                                }
                                let err_trailers = status_to_trailers(*status);
                                if let Some(chunk) = self.out_queue.pop_front() {
                                    self.pending_trailers = Some(err_trailers);
                                    return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))));
                                }
                                self.done = true;
                                return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(
                                    err_trailers,
                                ))));
                            }
                            if let Some(chunk) = self.out_queue.pop_front() {
                                return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))));
                            }
                        }
                    } else {
                        match frame.into_trailers() {
                            Ok(trailers) => {
                                if !self.buf.is_empty() {
                                    self.pending_trailers = Some(trailers);
                                    let rem = self.buf.split().freeze();
                                    return Poll::Ready(Some(Ok(hyper::body::Frame::data(rem))));
                                }
                                self.done = true;
                                return Poll::Ready(Some(Ok(hyper::body::Frame::trailers(
                                    trailers,
                                ))));
                            }
                            Err(other_frame) => {
                                if !self.buf.is_empty() {
                                    let rem = self.buf.split().freeze();
                                    return Poll::Ready(Some(Ok(hyper::body::Frame::data(rem))));
                                }
                                return Poll::Ready(Some(Ok(other_frame)));
                            }
                        }
                    }
                }
                Poll::Ready(None) => {
                    self.done = true;
                    if !self.buf.is_empty() {
                        let rem = self.buf.split().freeze();
                        return Poll::Ready(Some(Ok(hyper::body::Frame::data(rem))));
                    }
                    return Poll::Ready(None);
                }
                other => return other,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
            || (self.out_queue.is_empty()
                && self.buf.is_empty()
                && self.pending_trailers.is_none()
                && self.inner.is_end_stream())
    }
}

/// Echoes the client's `grpc-encoding` (`gzip`, `zstd`, `snappy`, `s2`) unconditionally on gRPC
/// responses like Go `grpc.NewServer`, enforces bounded pre-Tonic decompression on `gzip`/`zstd`
/// and `max_recv`/`max_send` with `Code::ResourceExhausted`, and advertises supported
/// encodings in `grpc-accept-encoding` on unsupported-compressor errors.
#[derive(Clone)]
pub struct GrpcCompressionEchoService<S> {
    inner: S,
    max_recv: usize,
    max_send: usize,
}

impl<S> GrpcCompressionEchoService<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            max_recv: usize::MAX,
            max_send: usize::MAX,
        }
    }

    pub fn new_with_limits(inner: S, max_recv: usize, max_send: usize) -> Self {
        Self {
            inner,
            max_recv,
            max_send,
        }
    }

    pub fn max_recv_msg_size(mut self, max_recv: usize) -> Self {
        self.max_recv = max_recv;
        self
    }

    pub fn max_send_msg_size(mut self, max_send: usize) -> Self {
        self.max_send = max_send;
        self
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for GrpcCompressionEchoService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> tower::Service<http::Request<tonic::body::BoxBody>> for GrpcCompressionEchoService<S>
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

    fn call(&mut self, mut req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let req_enc = req
            .headers()
            .get("grpc-encoding")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase);
        if let Some(ref enc) = req_enc {
            if !matches!(enc.as_str(), "identity" | "gzip" | "zstd" | "snappy" | "s2") {
                let mut resp = Status::unimplemented(format!(
                    "grpc: Decompressor is not installed for grpc-encoding {enc:?}"
                ))
                .into_http();
                resp.headers_mut().insert(
                    "grpc-accept-encoding",
                    HeaderValue::from_static(GRPC_ACCEPT_ENCODING_ALL),
                );
                return Box::pin(async move { Ok(resp) });
            }
        }
        // grpc-go server.go:1415-1427 unconditionally echoes incoming grpc-encoding when it
        // names a registered compressor (regardless of grpc-accept-encoding).
        let echo_enc: Option<&'static str> = req_enc.as_deref().and_then(|enc| match enc {
            "gzip" => Some("gzip"),
            "zstd" => Some("zstd"),
            "snappy" => Some("snappy"),
            "s2" => Some("s2"),
            _ => None,
        });
        let max_recv = self.max_recv;
        let max_send = self.max_send;
        match req_enc.as_deref() {
            Some("gzip") => {
                req.headers_mut().remove("grpc-encoding");
                req.headers_mut().remove(http::header::CONTENT_LENGTH);
                req = req.map(|b| {
                    tonic::body::BoxBody::new(BoundedGrpcDecompressBody::new(
                        b,
                        BoundedDecompressAlgo::Gzip,
                        max_recv,
                    ))
                });
            }
            Some("zstd") => {
                req.headers_mut().remove("grpc-encoding");
                req.headers_mut().remove(http::header::CONTENT_LENGTH);
                req = req.map(|b| {
                    tonic::body::BoxBody::new(BoundedGrpcDecompressBody::new(
                        b,
                        BoundedDecompressAlgo::Zstd,
                        max_recv,
                    ))
                });
            }
            Some("snappy") => {
                req.headers_mut().remove("grpc-encoding");
                req.headers_mut().remove(http::header::CONTENT_LENGTH);
                req = req.map(|b| {
                    tonic::body::BoxBody::new(BoundedGrpcDecompressBody::new(
                        b,
                        BoundedDecompressAlgo::Snappy,
                        max_recv,
                    ))
                });
            }
            Some("s2") => {
                req.headers_mut().remove("grpc-encoding");
                req.headers_mut().remove(http::header::CONTENT_LENGTH);
                req = req.map(|b| {
                    tonic::body::BoxBody::new(BoundedGrpcDecompressBody::new(
                        b,
                        BoundedDecompressAlgo::S2,
                        max_recv,
                    ))
                });
            }
            _ => {
                req = req.map(|b| {
                    tonic::body::BoxBody::new(BoundedGrpcDecompressBody::new(
                        b,
                        BoundedDecompressAlgo::Identity,
                        max_recv,
                    ))
                });
            }
        }
        let fut = self.inner.call(req);
        Box::pin(async move {
            let mut resp = fut.await?;
            if !resp.headers().contains_key("grpc-status") {
                if let Some(enc) = echo_enc {
                    if !resp.headers().contains_key("grpc-encoding") {
                        resp.headers_mut()
                            .insert("grpc-encoding", HeaderValue::from_static(enc));
                        resp = resp.map(|body| {
                            tonic::body::BoxBody::new(CompressGrpcBody::new(
                                body,
                                Some(enc),
                                max_send,
                            ))
                        });
                    } else if max_send < usize::MAX {
                        resp = resp.map(|body| {
                            tonic::body::BoxBody::new(CompressGrpcBody::new(body, None, max_send))
                        });
                    }
                } else if max_send < usize::MAX {
                    resp = resp.map(|body| {
                        tonic::body::BoxBody::new(CompressGrpcBody::new(body, None, max_send))
                    });
                }
            }
            Ok(resp)
        })
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
        if let Ok(v) = std::str::from_utf8(val.as_bytes()) {
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

/// Go `google.golang.org/grpc/internal/transport/http_util.go:227-265` (`encodeGrpcMessage` /
/// `encodeGrpcMessageUnchecked`): leaves printable ASCII `' '..='~'` except `'%'` unencoded and
/// percent-encodes all other UTF-8 bytes (including `U+FFFD` replacement bytes `%EF%BF%BD`
/// produced by `utf8.DecodeRuneInString` on invalid UTF-8) as uppercase `%XX`.
fn encode_grpc_message_like_go(msg_lossy: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(msg_lossy.len());
    for &b in msg_lossy.as_bytes() {
        if (b' '..=b'~').contains(&b) && b != b'%' {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn rewrite_httpgrpc_status_header(headers: &mut HeaderMap) {
    let raw_msg = headers.remove("x-mimir-httpgrpc-raw-msg");
    if let Some(raw_code) = headers.remove("x-mimir-httpgrpc-raw-code") {
        headers.insert(Status::GRPC_STATUS, raw_code);
        if let Some(msg) = raw_msg {
            headers.insert(Status::GRPC_MESSAGE, msg);
        }
        return;
    }
    if let Some(msg) = raw_msg {
        headers.insert(Status::GRPC_MESSAGE, msg);
    }
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

    /// Expected values produced by Go: `url.Parse(in)`, `gorilla/mux` `cleanPath(u.EscapedPath())`
    /// (`UseEncodedPath`) and, when it differs, `u.Path = p; u.String()` (`None`: no redirect).
    #[test]
    fn split_go_url_clean_redirect_matches_go_url_parse_and_string() {
        let cases: &[(&str, Option<&str>)] = &[
            ("ready", Some("/ready")),
            ("ready?x=1", Some("/ready?x=1")),
            ("ready?x=1#frag", Some("/ready?x=1#frag")),
            ("ready#", Some("/ready")),
            ("ready?", Some("/ready?")),
            ("/ready", None),
            ("/ready?", None),
            ("/ready/", None),
            ("//ready", Some("//ready/")),
            ("//ready#frag", Some("//ready/#frag")),
            ("//ready/x", None),
            ("//ready//x?a=b", Some("//ready/x?a=b")),
            ("///ready", Some("/ready")),
            ("////ready", Some("/ready")),
            ("/./ready", Some("/ready")),
            ("/ready/.", Some("/ready")),
            ("/ready/..", Some("/")),
            ("/ingester/../ready?x=1", Some("/ready?x=1")),
            ("/ingester//ring?tokens=true", Some("/ingester/ring?tokens=true")),
            ("/a//b%20c", Some("/a/b%2520c")),
            ("/a//b c", Some("/a/b%2520c")),
            ("//foo(bar)?q=(1)", Some("//foo(bar)/?q=(1)")),
            ("/a//b%2Fc?x=%2F", Some("/a/b%252Fc?x=%2F")),
            ("http://example.com//a%20b", Some("http://example.com/a%2520b")),
            ("//host//a%41", Some("//host/a%2541")),
            ("/a//b{c}", Some("/a/b%257Bc%257D")),
            ("x//y%2e", Some("/x/y%252e")),
            ("/%2e%2e//ready", Some("/%252e%252e/ready")),
            ("", Some("/")),
            ("http://example.com//ready?q=1", Some("http://example.com/ready?q=1")),
            ("http://example.com", Some("http://example.com/")),
            ("http://example.com/", None),
            ("HTTP://Example.com/a/../b#f", Some("http://Example.com/b#f")),
            ("http://user:pw@example.com//x", Some("http://user:pw@example.com/x")),
            ("http:/x//y", Some("http:/x/y")),
            ("http:///x//y", Some("http:///x/y")),
            ("mailto:foo@bar", Some("mailto:foo@bar")),
            ("http:foo/bar?x=1", Some("http:foo/bar?x=1")),
            ("*", Some("/%2A")),
            ("/*", None),
            ("//", Some("/")),
            ("/", None),
            ("/..", Some("/")),
            ("/a/b/../../c/", Some("/c/")),
            ("//host", Some("//host/")),
            ("//host?q=1", Some("//host/?q=1")),
            ("//host/ready", None),
            ("//host/ready?x=1", None),
            ("//user@host/a//b", Some("//user@host/a/b")),
            ("/ready?a=b?c", None),
            ("/ready??", None),
            (r#"/config?match[]={job="foo"}&mode=diff"#, None),
            (r#"//config?match[]={job="foo"}"#, Some(r#"//config/?match[]={job="foo"}"#)),
        ];
        for (input, want) in cases {
            let got = split_go_url_clean_redirect(input);
            assert_eq!(got.redirect.as_deref(), *want, "url {input:?}");
        }

        // A scheme-less `//authority` is not part of `url.Path`: Go routes the path after it.
        let routed = split_go_url_clean_redirect("//host/ready?x=1");
        assert_eq!(routed.path, "/ready");
        assert_eq!(routed.route_url.as_deref(), Some("/ready?x=1"));
        assert_eq!(split_go_url_clean_redirect("/ready?x=1").route_url, None);
        assert_eq!(split_go_url_clean_redirect("http://h/ready").route_url, None);
    }

    #[test]
    fn d2_09_validate_go_url_allows_raw_percent_in_query_and_validates_fragment() {
        assert!(validate_go_url("/ready?q=100%").is_ok());
        assert!(validate_go_url("/ready?q=100%#sec").is_ok());
        assert!(validate_go_url("/ready#100%25").is_ok());
        assert!(parse_go_url_to_http_uri("/ready?q=100%").is_ok());

        let err_frag = validate_go_url("/ready#100%").unwrap_err();
        assert!(err_frag.contains("invalid URL escape"), "got: {err_frag}");
        let err_qfrag = validate_go_url("/ready?q=100%#sec%").unwrap_err();
        assert!(err_qfrag.contains("invalid URL escape"), "got: {err_qfrag}");
        assert!(parse_go_url_to_http_uri("/ready#100%").is_err());
        assert!(validate_go_url("/ready%zz").is_err());
        assert!(validate_go_url("/ready%?q=100%").is_err());
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
                    || h.key.eq_ignore_ascii_case("x-errormessage")
                    || h.key.eq_ignore_ascii_case("content-length")
                    || h.key.eq_ignore_ascii_case("connection")),
            "expected internal and server-added headers stripped: {:?}",
            http_resp.headers
        );
        assert_eq!(http_resp.headers.len(), 1, "expected exact Content-Type only: {:?}", http_resp.headers);
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

    async fn call_health(
        svc: mimir_ingester::grpc::SnappyDecompressService<HealthService>,
        path: &str,
        body: Vec<u8>,
        encoding: Option<&str>,
    ) -> http::Response<tonic::body::BoxBody> {
        let mut b = http::Request::builder()
            .method(Method::POST)
            .uri(path)
            .header(http::header::CONTENT_TYPE, "application/grpc");
        let framed = match encoding {
            None => mimir_proto::snappy::encode_grpc_frame(&body, false).unwrap(),
            Some("snappy") => {
                b = b.header("grpc-encoding", "snappy");
                mimir_proto::snappy::encode_grpc_frame(&body, true).unwrap()
            }
            Some("gzip") => {
                use flate2::{write::GzEncoder, Compression};
                use std::io::Write;
                b = b.header("grpc-encoding", "gzip");
                let mut enc = GzEncoder::new(Vec::new(), Compression::default());
                enc.write_all(&body).unwrap();
                let gz = enc.finish().unwrap();
                let mut out = Vec::with_capacity(5 + gz.len());
                out.push(1);
                out.extend_from_slice(&(gz.len() as u32).to_be_bytes());
                out.extend_from_slice(&gz);
                Bytes::from(out)
            }
            Some("s2") => {
                b = b.header("grpc-encoding", "s2");
                // S2 uncompressed chunk inside S2 stream framing.
                let crc = mimir_proto::s2::masked_crc32c(&body);
                let mut s2 = Vec::with_capacity(18 + body.len());
                s2.extend_from_slice(b"\xff\x06\x00\x00S2sTwO");
                let chunk_len = 4 + body.len();
                s2.push(0x01);
                s2.push((chunk_len & 0xff) as u8);
                s2.push(((chunk_len >> 8) & 0xff) as u8);
                s2.push(((chunk_len >> 16) & 0xff) as u8);
                s2.extend_from_slice(&crc.to_le_bytes());
                s2.extend_from_slice(&body);
                let mut out = Vec::with_capacity(5 + s2.len());
                out.push(1);
                out.extend_from_slice(&(s2.len() as u32).to_be_bytes());
                out.extend_from_slice(&s2);
                Bytes::from(out)
            }
            Some(other) => panic!("unsupported test encoding {other}"),
        };
        let req = b
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(framed))))
            .unwrap();
        svc.oneshot(req).await.unwrap()
    }

    #[tokio::test]
    async fn health_service_accepts_identity_gzip_snappy_and_s2() {
        use crate::modules::StateCell;
        use http_body_util::BodyExt;
        use mimir_common::services::State;
        use tonic::codec::CompressionEncoding;

        let modules = ModuleStates::new();
        let server_state = StateCell::new(State::Running);
        modules.add("server", server_state.state_fn());
        let shutdown = Arc::new(AtomicBool::new(false));
        let svc = mimir_ingester::grpc::SnappyDecompressService::new(
            HealthService::new(Arc::clone(&shutdown), Arc::clone(&modules))
                .max_decoding_message_size(4 << 20)
                .max_encoding_message_size(4 << 20)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd),
            4 << 20,
        );

        for enc in [None, Some("gzip"), Some("snappy"), Some("s2")] {
            // Check
            let req_bytes = HealthCheckRequest {
                service: String::new(),
            }
            .encode_to_vec();
            let resp = call_health(
                svc.clone(),
                "/grpc.health.v1.Health/Check",
                req_bytes.clone(),
                enc,
            )
            .await;
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert!(body.len() >= 5, "enc={enc:?}");
            let check = HealthCheckResponse::decode(&body[5..]).unwrap();
            assert_eq!(check.status, SERVING_STATUS, "enc={enc:?}");

            // List
            let resp = call_health(
                svc.clone(),
                "/grpc.health.v1.Health/List",
                HealthListRequest {}.encode_to_vec(),
                enc,
            )
            .await;
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            let list = HealthListResponse::decode(&body[5..]).unwrap();
            assert_eq!(
                list.statuses.get("server").map(|r| r.status),
                Some(SERVING_STATUS),
                "enc={enc:?}"
            );

            // Watch -> Unimplemented("Watching is not supported"), NOT unsupported compression.
            let resp = call_health(svc.clone(), "/grpc.health.v1.Health/Watch", req_bytes, enc).await;
            let st = tonic::Status::from_header_map(resp.headers()).expect("watch status");
            assert_eq!(st.code(), tonic::Code::Unimplemented, "enc={enc:?}");
            assert_eq!(st.message(), "Watching is not supported", "enc={enc:?}");
        }

        // Shutdown flips Check to NOT_SERVING.
        shutdown.store(true, Ordering::Release);
        let resp = call_health(
            svc,
            "/grpc.health.v1.Health/Check",
            HealthCheckRequest::default().encode_to_vec(),
            Some("gzip"),
        )
        .await;
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let check = HealthCheckResponse::decode(&body[5..]).unwrap();
        assert_eq!(check.status, NOT_SERVING_STATUS);
    }

    #[tokio::test]
    async fn query_stream_compression_echo_never_double_compresses() {
        use flate2::read::GzDecoder;
        use http_body_util::BodyExt;
        use mimir_proto::{cortexpb, ingesterpb};
        use std::io::Read as _;

        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-compress".to_string();
        cfg.push_grpc_method_enabled = true;
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Arc::new(
            mimir_ingester::Ingester::new(
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
            .unwrap(),
        );
        ing.set_state_for_test(mimir_common::services::State::Running);

        let now_ms = 1_700_000_000_000i64;
        let wr = cortexpb::WriteRequest {
            timeseries: vec![cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair {
                    name: Bytes::from_static(b"__name__"),
                    value: Bytes::from_static(b"echo_compress_metric"),
                }],
                samples: vec![cortexpb::Sample {
                    value: 42.0,
                    timestamp_ms: now_ms,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        ing.push_grpc("tenant-comp", &wr).await.unwrap();

        let svc = GrpcCompressionEchoService::new(
            mimir_ingester::IngesterService::new(Arc::clone(&ing))
                .into_server()
                .max_decoding_message_size(4 << 20)
                .max_encoding_message_size(4 << 20)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd),
        );

        let q_req = ingesterpb::QueryRequest {
            start_timestamp_ms: now_ms - 10_000,
            end_timestamp_ms: now_ms + 10_000,
            matchers: vec![ingesterpb::LabelMatcher {
                r#type: ingesterpb::MatchType::Equal as i32,
                name: "__name__".into(),
                value: "echo_compress_metric".into(),
            }],
            streaming_chunks_batch_size: 16,
        };
        let raw_req = q_req.encode_to_vec();

        for enc in ["snappy", "s2", "gzip", "zstd"] {
            for accept_hdr in [Some(enc), Some("identity,gzip,zstd,snappy,s2"), None] {
                let mut comp = BytesMut::new();
                compress_grpc_payload_into(enc, &raw_req, &mut comp).unwrap();
                let mut framed = Vec::with_capacity(5 + comp.len());
                framed.push(1);
                framed.extend_from_slice(&(comp.len() as u32).to_be_bytes());
                framed.extend_from_slice(&comp);

                let mut builder = http::Request::builder()
                    .method(Method::POST)
                    .uri("/cortex.Ingester/QueryStream")
                    .header(http::header::CONTENT_TYPE, "application/grpc")
                    .header("grpc-encoding", enc)
                    .header("x-scope-orgid", "tenant-comp");
                if let Some(acc) = accept_hdr {
                    builder = builder.header("grpc-accept-encoding", acc);
                }
                let http_req = builder
                    .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(
                        framed,
                    )))))
                    .unwrap();

                let resp = svc.clone().oneshot(http_req).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK, "enc={enc} accept={accept_hdr:?}");
                let resp_enc = resp
                    .headers()
                    .get("grpc-encoding")
                    .and_then(|v| v.to_str().ok())
                    .expect("grpc-encoding response header")
                    .to_string();
                if accept_hdr.is_none() || accept_hdr == Some(enc) {
                    assert_eq!(resp_enc, enc, "enc={enc} accept={accept_hdr:?}");
                }

                let bytes = resp.into_body().collect().await.unwrap().to_bytes();
                assert!(bytes.len() >= 5, "enc={enc} accept={accept_hdr:?}: empty body");
                let mut pos = 0usize;
                let mut total_series = 0usize;
                while pos < bytes.len() {
                    assert_eq!(
                        bytes[pos], 1,
                        "enc={enc} accept={accept_hdr:?}: expected compressed frame at {pos}"
                    );
                    let frame_len = u32::from_be_bytes([
                        bytes[pos + 1],
                        bytes[pos + 2],
                        bytes[pos + 3],
                        bytes[pos + 4],
                    ]) as usize;
                    let compressed_payload = &bytes[pos + 5..pos + 5 + frame_len];
                    pos += 5 + frame_len;
                    let decompressed = match resp_enc.as_str() {
                        "snappy" => {
                            mimir_proto::snappy::decompress_framed(compressed_payload).unwrap()
                        }
                        "s2" => {
                            let mut out = Vec::new();
                            mimir_proto::s2::decompress_framed_into(
                                compressed_payload,
                                &mut out,
                                4 << 20,
                            )
                            .unwrap();
                            out
                        }
                        "gzip" => {
                            let mut dec = GzDecoder::new(compressed_payload);
                            let mut out = Vec::new();
                            dec.read_to_end(&mut out).unwrap();
                            out
                        }
                        "zstd" => zstd::stream::decode_all(compressed_payload).unwrap(),
                        other => panic!("unexpected resp_enc {other}"),
                    };
                    let qs = ingesterpb::QueryStreamResponse::decode(decompressed.as_slice())
                        .expect("single decompression must yield valid QueryStreamResponse");
                    total_series += qs.streaming_series.len();
                }
                assert_eq!(total_series, 1, "enc={enc} accept={accept_hdr:?}");
            }
        }
    }

    #[tokio::test]
    async fn compress_grpc_body_leaves_zero_length_frames_uncompressed() {
        use http_body_util::BodyExt;

        let frames = vec![
            0, 0, 0, 0, 0, // 0-byte message
            0, 0, 0, 0, 3, b'a', b'b', b'c', // 3-byte message
        ];
        let body = tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(frames))));
        let compressed_body = CompressGrpcBody::new(body, Some("gzip"), 4 << 20);
        let out = compressed_body.collect().await.unwrap().to_bytes();

        // First frame must remain flag=0, len=0 (matching grpc-go rpc_util.go:831).
        assert!(out.len() > 10, "out too short: {out:?}");
        assert_eq!(&out[..5], &[0, 0, 0, 0, 0]);
        // Second frame must be compressed (flag=1).
        assert_eq!(out[5], 1);
        let comp_len = u32::from_be_bytes([out[6], out[7], out[8], out[9]]) as usize;
        assert_eq!(out.len(), 10 + comp_len);
        use std::io::Read as _;
        let mut dec = flate2::read::GzDecoder::new(&out[10..]);
        let mut plain = Vec::new();
        dec.read_to_end(&mut plain).unwrap();
        assert_eq!(plain, b"abc");
    }

    #[tokio::test]
    async fn w20_sdw_02_bounded_decompress_yields_at_most_one_frame_per_poll() {
        use hyper::body::Body as _;

        // Build two zstd-compressed gRPC frames packed in a single DATA chunk:
        // frame 1: small valid payload (16 bytes), frame 2: payload decompressing to 256 bytes (> max_recv=64).
        let p1 = vec![b'a'; 16];
        let p2 = vec![b'b'; 256];
        let mut c1 = Vec::new();
        let mut c2 = Vec::new();
        zstd::stream::copy_encode(p1.as_slice(), &mut c1, 0).unwrap();
        zstd::stream::copy_encode(p2.as_slice(), &mut c2, 0).unwrap();

        let mut wire = Vec::new();
        wire.push(1);
        wire.extend_from_slice(&(c1.len() as u32).to_be_bytes());
        wire.extend_from_slice(&c1);
        wire.push(1);
        wire.extend_from_slice(&(c2.len() as u32).to_be_bytes());
        wire.extend_from_slice(&c2);

        let body = tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(wire))));
        let mut dec = BoundedGrpcDecompressBody::new(body, BoundedDecompressAlgo::Zstd, 64);

        // Frame 1 is buffered until EOF/trailers or frame 2; frame 2 exceeds max_recv=64
        // so poll_frame immediately rejects with ResourceExhausted without ever emitting frame 1.
        let err = std::future::poll_fn(|cx| Pin::new(&mut dec).poll_frame(cx))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn w22_deep_03_bounded_decompress_body_decodes_multi_member_gzip() {
        use http_body_util::BodyExt as _;
        use std::io::Write as _;

        let mut e1 = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e1.write_all(b"member-1:").unwrap();
        let mut combined = e1.finish().unwrap();

        let mut e2 = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e2.write_all(b"member-2").unwrap();
        combined.extend_from_slice(&e2.finish().unwrap());

        let mut frame = Vec::with_capacity(5 + combined.len());
        frame.push(1);
        frame.extend_from_slice(&(combined.len() as u32).to_be_bytes());
        frame.extend_from_slice(&combined);

        let body = tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(frame.clone()))));
        let dec_body = BoundedGrpcDecompressBody::new(body, BoundedDecompressAlgo::Gzip, 1024);
        let out = dec_body.collect().await.unwrap().to_bytes();
        assert_eq!(out[0], 0);
        let len = u32::from_be_bytes([out[1], out[2], out[3], out[4]]) as usize;
        assert_eq!(&out[5..5 + len], b"member-1:member-2");

        // Combined size across members exceeding max_recv must still fail with ResourceExhausted.
        let bomb_body = tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(frame))));
        let bomb_dec =
            BoundedGrpcDecompressBody::new(bomb_body, BoundedDecompressAlgo::Gzip, 10);
        let err = bomb_dec.collect().await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn d_httpgrpc_01_routes_scheme_urls_with_empty_or_omitted_host() {
        use mimir_proto::httpgrpcpb::http_server::Http;

        let router = Router::new().route("/ready", get(|| async { (StatusCode::OK, "ready") }));
        let bridge = HttpGrpcBridge::new(router);

        for url in [
            "http:///ready",
            "http:/ready",
            "https:///ready",
            "https:/ready",
            "http:///ready?x=1",
            "http:/ready?x=1#frag",
        ] {
            let req = tonic::Request::new(httpgrpcpb::HttpRequest {
                method: "GET".to_string(),
                url: url.to_string(),
                headers: vec![],
                body: Bytes::new(),
            });
            let resp = bridge
                .handle(req)
                .await
                .unwrap_or_else(|e| panic!("url {url:?} failed: {e}"))
                .into_inner();
            assert_eq!(resp.code, 200, "url {url:?}");
            assert_eq!(resp.body.as_ref(), b"ready", "url {url:?}");
        }
    }

    #[tokio::test]
    async fn d_httpgrpc_02_non_utf8_error_body_omits_details_bin_and_preserves_raw_status() {
        use http_body_util::BodyExt as _;
        use mimir_proto::httpgrpcpb::http_server::Http;
        use tower::ServiceExt as _;

        let raw_non_utf8: &[u8] = &[0x1f, 0x8b, 0x08, b'b', b'a', b'd'];
        let router = Router::new()
            .route(
                "/bin-err",
                get(|| async {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Bytes::from_static(&[0x1f, 0x8b, 0x08, b'b', b'a', b'd']),
                    )
                }),
            )
            .route(
                "/bin-err-pct",
                get(|| async {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Bytes::from_static(b"100% err:\xff\x80"),
                    )
                }),
            )
            .route(
                "/bin-err-override",
                get(|| async {
                    (
                        StatusCode::BAD_REQUEST,
                        [("x-errormessage", "utf8 override")],
                        Bytes::from_static(&[0x1f, 0x8b, 0x08, b'b', b'a', b'd']),
                    )
                }),
            );

        let bridge = HttpGrpcBridge::new(router.clone());
        let status = bridge
            .handle(tonic::Request::new(httpgrpcpb::HttpRequest {
                method: "GET".to_string(),
                url: "/bin-err".to_string(),
                headers: vec![],
                body: Bytes::new(),
            }))
            .await
            .unwrap_err();
        assert!(
            status.details().is_empty(),
            "non-UTF-8 body without X-ErrorMessage must omit grpc-status-details-bin"
        );
        assert!(extract_http_response(&status).is_none());

        // Through HttpGrpcStatusFixService (wire trailers/headers):
        // grpc-status == "500", grpc-status-details-bin absent, x-mimir-httpgrpc-raw-code absent,
        // grpc-message == "%1F%EF%BF%BD%08bad" (matching Go grpc-go encodeGrpcMessage).
        let svc = HttpGrpcStatusFixService::new(HttpServer::new(HttpGrpcBridge::new(router)));
        let req_msg = httpgrpcpb::HttpRequest {
            method: "GET".to_string(),
            url: "/bin-err".to_string(),
            headers: vec![],
            body: Bytes::new(),
        };
        let payload = req_msg.encode_to_vec();
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);

        let http_req = http::Request::builder()
            .method("POST")
            .uri("/httpgrpc.HTTP/Handle")
            .header("content-type", "application/grpc")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(frame)))))
            .unwrap();
        let resp = svc.clone().oneshot(http_req).await.unwrap();
        let mut merged_headers = resp.headers().clone();
        let collected = resp.into_body().collect().await.unwrap();
        if let Some(trailers) = collected.trailers() {
            merged_headers.extend(trailers.clone());
        }
        assert_eq!(merged_headers.get("grpc-status").unwrap(), "500");
        assert!(
            !merged_headers.contains_key("grpc-status-details-bin"),
            "grpc-status-details-bin must be omitted on non-UTF-8 Status.Message"
        );
        assert!(!merged_headers.contains_key("x-mimir-httpgrpc-raw-code"));
        assert!(!merged_headers.contains_key("x-mimir-httpgrpc-raw-msg"));
        assert_eq!(
            merged_headers.get("grpc-message").unwrap(),
            "%1F%EF%BF%BD%08bad"
        );

        // Verify Go grpc-go encodeGrpcMessage parity on '%', ' ', and invalid UTF-8 bytes:
        // b"100% err:\xff\x80" -> "100%25 err:%EF%BF%BD%EF%BF%BD" (grpc-go v1.85.0 http_util.go:241-265).
        let pct_req = httpgrpcpb::HttpRequest {
            method: "GET".to_string(),
            url: "/bin-err-pct".to_string(),
            headers: vec![],
            body: Bytes::new(),
        };
        let pct_payload = pct_req.encode_to_vec();
        let mut pct_frame = Vec::with_capacity(5 + pct_payload.len());
        pct_frame.push(0);
        pct_frame.extend_from_slice(&(pct_payload.len() as u32).to_be_bytes());
        pct_frame.extend_from_slice(&pct_payload);
        let pct_http_req = http::Request::builder()
            .method("POST")
            .uri("/httpgrpc.HTTP/Handle")
            .header("content-type", "application/grpc")
            .body(tonic::body::BoxBody::new(SingleChunkBody(Some(Bytes::from(pct_frame)))))
            .unwrap();
        let pct_resp = svc.clone().oneshot(pct_http_req).await.unwrap();
        let mut pct_headers = pct_resp.headers().clone();
        let pct_collected = pct_resp.into_body().collect().await.unwrap();
        if let Some(trailers) = pct_collected.trailers() {
            pct_headers.extend(trailers.clone());
        }
        assert_eq!(pct_headers.get("grpc-status").unwrap(), "500");
        assert!(!pct_headers.contains_key("grpc-status-details-bin"));
        assert!(!pct_headers.contains_key("x-mimir-httpgrpc-raw-code"));
        assert!(!pct_headers.contains_key("x-mimir-httpgrpc-raw-msg"));
        assert_eq!(
            pct_headers.get("grpc-message").unwrap(),
            "100%25 err:%EF%BF%BD%EF%BF%BD"
        );

        // When X-ErrorMessage IS set (valid UTF-8), grpc-status-details-bin MUST still be emitted.
        let status_override = bridge
            .handle(tonic::Request::new(httpgrpcpb::HttpRequest {
                method: "GET".to_string(),
                url: "/bin-err-override".to_string(),
                headers: vec![],
                body: Bytes::new(),
            }))
            .await
            .unwrap_err();
        let extracted = extract_http_response(&status_override).unwrap();
        assert_eq!(extracted.code, 400);
        assert_eq!(extracted.body.as_ref(), raw_non_utf8);
    }
}
