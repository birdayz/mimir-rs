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
                .map_err(|e| Status::unknown(format!("net/http: invalid method: {e}")))?
        };

        let uri = parse_go_url_to_http_uri(&req.url)
            .map_err(|e| Status::unknown(format!("parse {:?}: {e}", req.url)))?;

        // `gorilla/mux` `ServeHTTP` (`cleanPath`): a non-empty relative URL without a leading `/`
        // (e.g. `"ready"` or `"ready?x=1"`, which `http::Uri` parses as authority) redirects 301
        // to `/<path>[?<query>]`.
        let without_frag = req.url.split('#').next().unwrap_or(&req.url);
        let (raw_path, raw_query) = match without_frag.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (without_frag, None),
        };
        if !raw_path.is_empty() && !raw_path.contains("://") && !raw_path.starts_with('/') {
            let location = match raw_query {
                Some(q) => format!("/{raw_path}?{q}"),
                None => format!("/{raw_path}"),
            };
            return Ok(Response::new(httpgrpcpb::HttpResponse {
                code: 301,
                headers: vec![httpgrpcpb::Header {
                    key: "Location".to_string(),
                    values: vec![location],
                }],
                body: Bytes::new(),
            }));
        }

        let mut req_headers = HeaderMap::new();
        for h in req.headers {
            let Ok(name) = HeaderName::from_bytes(h.key.as_bytes()) else {
                continue;
            };
            // Go `httpgrpc.ToHeader`: `header[http.CanonicalHeaderKey(h.Key)] = h.Values` overwrites
            // any prior entry for the same canonical header name.
            req_headers.remove(&name);
            for v in h.values {
                if let Ok(val) = HeaderValue::from_str(&v) {
                    req_headers.append(name.clone(), val);
                }
            }
        }

        let mut http_req = HttpRequestMsg::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(req.body))
            .map_err(|e| Status::internal(format!("failed to construct HTTP request: {e}")))?;
        *http_req.headers_mut() = req_headers;



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

/// Parses `req.url` with Go `net/url.Parse` tolerance (accepting relative paths without leading
/// `/` and unescaped `{}`/`[]`/`"`/`|`/`^`/spaces in query strings) and encodes disallowed
/// RFC 3986 characters so `http::Uri` can pass it into Axum's `Router`.
fn parse_go_url_to_http_uri(raw: &str) -> Result<Uri, String> {
    if raw.is_empty() {
        return Ok(Uri::from_static("/"));
    }
    let bytes = raw.as_bytes();
    for &b in bytes {
        if b < 0x20 || b == 0x7f {
            return Err("net/url: invalid control character in URL".to_string());
        }
    }
    // Validate percent-escapes like Go `url.parse`.
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

    // Strip #fragment (Go `http.NewRequest` parses it into URL.Fragment, not Path/RawQuery).
    let without_frag = raw.split('#').next().unwrap_or(raw);
    // Check scheme validity if `:` precedes `/` / `?`.
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
    if path_part.contains("://") {
        encoded.push_str(path_part);
    } else {
        encode_ascii_unsafe(path_part, &mut encoded);
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

fn client_accepts_grpc_encoding(headers: &HeaderMap, enc: &str) -> bool {
    let Some(val) = headers.get("grpc-accept-encoding").and_then(|v| v.to_str().ok()) else {
        return true;
    };
    val.split(',').any(|s| s.trim().eq_ignore_ascii_case(enc))
}

fn s2_compress_framed(input: &[u8]) -> Vec<u8> {
    const MAX_BLOCK: usize = 4 << 20;
    let mut out = Vec::with_capacity(10 + 8 + input.len());
    out.extend_from_slice(b"\xff\x06\x00\x00S2sTwO");
    if input.is_empty() {
        let chunk_len = 4usize;
        out.push(0x01);
        out.push((chunk_len & 0xff) as u8);
        out.push(((chunk_len >> 8) & 0xff) as u8);
        out.push(((chunk_len >> 16) & 0xff) as u8);
        out.extend_from_slice(&mimir_proto::s2::masked_crc32c(&[]).to_le_bytes());
        return out;
    }
    for chunk in input.chunks(MAX_BLOCK) {
        let chunk_len = 4 + chunk.len();
        out.push(0x01);
        out.push((chunk_len & 0xff) as u8);
        out.push(((chunk_len >> 8) & 0xff) as u8);
        out.push(((chunk_len >> 16) & 0xff) as u8);
        out.extend_from_slice(&mimir_proto::s2::masked_crc32c(chunk).to_le_bytes());
        out.extend_from_slice(chunk);
    }
    out
}

fn compress_grpc_payload(enc: &str, payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    use std::io::Write as _;
    match enc {
        "gzip" => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(payload)?;
            encoder.finish()
        }
        "zstd" => zstd::stream::encode_all(payload, 0),
        "snappy" => mimir_proto::snappy::compress_framed(payload),
        "s2" => Ok(s2_compress_framed(payload)),
        _ => Ok(payload.to_vec()),
    }
}

struct CompressGrpcBody {
    inner: tonic::body::BoxBody,
    encoding: &'static str,
    buf: Vec<u8>,
    out_queue: std::collections::VecDeque<Bytes>,
}

impl CompressGrpcBody {
    fn new(inner: tonic::body::BoxBody, encoding: &'static str) -> Self {
        Self {
            inner,
            encoding,
            buf: Vec::new(),
            out_queue: std::collections::VecDeque::new(),
        }
    }

    fn drain_frames(&mut self) -> Result<(), Box<Status>> {
        while self.buf.len() >= 5 {
            let flag = self.buf[0];
            let msg_len =
                u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
            if self.buf.len() < 5 + msg_len {
                break;
            }
            let frame: Vec<u8> = self.buf.drain(..5 + msg_len).collect();
            let payload = &frame[5..];
            if flag == 0 {
                let compressed = compress_grpc_payload(self.encoding, payload)
                    .map_err(|e| Box::new(Status::internal(format!("compress gRPC response: {e}"))))?;
                let mut out = Vec::with_capacity(5 + compressed.len());
                out.push(1);
                out.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
                out.extend_from_slice(&compressed);
                self.out_queue.push_back(Bytes::from(out));
            } else {
                self.out_queue.push_back(Bytes::from(frame));
            }
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
        loop {
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if frame.is_data() {
                        if let Ok(data) = frame.into_data() {
                            self.buf.extend_from_slice(&data);
                            self.drain_frames().map_err(|e| *e)?;
                            if let Some(chunk) = self.out_queue.pop_front() {
                                return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))));
                            }
                        }
                    } else {
                        if !self.buf.is_empty() {
                            let rem = std::mem::take(&mut self.buf);
                            return Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from(rem)))));
                        }
                        return Poll::Ready(Some(Ok(frame)));
                    }
                }
                Poll::Ready(None) => {
                    if !self.buf.is_empty() {
                        let rem = std::mem::take(&mut self.buf);
                        return Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from(rem)))));
                    }
                    return Poll::Ready(None);
                }
                other => return other,
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.out_queue.is_empty() && self.buf.is_empty() && self.inner.is_end_stream()
    }
}

/// Echoes the client's negotiated `grpc-encoding` (`gzip`, `zstd`, `snappy`, `s2`) on gRPC
/// responses like Go `grpc.NewServer`, and advertises `snappy,s2` in `grpc-accept-encoding` on
/// unsupported-compressor errors.
#[derive(Clone)]
pub struct GrpcCompressionEchoService<S> {
    inner: S,
}

impl<S> GrpcCompressionEchoService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
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

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
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
        let echo_enc: Option<&'static str> = req_enc.as_deref().and_then(|enc| {
            let canon = match enc {
                "gzip" => "gzip",
                "zstd" => "zstd",
                "snappy" => "snappy",
                "s2" => "s2",
                _ => return None,
            };
            client_accepts_grpc_encoding(req.headers(), canon).then_some(canon)
        });
        let fut = self.inner.call(req);
        Box::pin(async move {
            let mut resp = fut.await?;
            if let Some(enc) = echo_enc {
                if !resp.headers().contains_key("grpc-encoding")
                    && !resp.headers().contains_key("grpc-status")
                {
                    resp.headers_mut()
                        .insert("grpc-encoding", HeaderValue::from_static(enc));
                    resp = resp.map(|body| {
                        tonic::body::BoxBody::new(CompressGrpcBody::new(body, enc))
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
                let comp = compress_grpc_payload(enc, &raw_req).unwrap();
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
}
