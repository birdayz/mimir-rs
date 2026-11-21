//! The S3 protocol client (request construction, signing, retries, bucket location cache and
//! error decoding) used by the thanos objstore S3 provider, ported from minio-go.
//!
//! Ported from:
//! - github.com/minio/minio-go/v7/api.go (`privateNew`, `New`, `executeMethod`, `newRequest`,
//!   `makeTargetURL`, `isVirtualHostStyleRequest`, `setUserAgent`, `do`)
//! - github.com/minio/minio-go/v7/retry.go (`newRetryTimer`, retryable codes/statuses/errors)
//! - github.com/minio/minio-go/v7/bucket-cache.go (`getBucketLocation`,
//!   `getBucketLocationRequest`, `processBucketLocationResponse`)
//! - github.com/minio/minio-go/v7/api-error-response.go (`ErrorResponse`,
//!   `httpRespToErrorResponse`)
//! - github.com/minio/minio-go/v7/s3-error.go (`s3ErrorResponseMap` subset)

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use mimir_config::S3BucketLookupType;
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderValue};

use super::credentials::{Credentials, SignerType};
use super::signer::{self, SignableRequest};
use super::transport::HttpTransport;
use crate::bucket::s3utils::{self, EndpointUrl};

/// minio-go `MaxRetry`, `DefaultRetryUnit`, `DefaultRetryCap`, `MaxJitter`.
const MAX_RETRY: usize = 10;
const DEFAULT_RETRY_UNIT: Duration = Duration::from_millis(200);
const DEFAULT_RETRY_CAP: Duration = Duration::from_secs(1);

const LIBRARY_VERSION: &str = "v7.0.98";

static APP_VERSION: OnceLock<String> = OnceLock::new();

/// Sets the `<version> (<runtime>)` part of the S3 User-Agent (thanos `SetAppInfo("thanos-<component>",
/// fmt.Sprintf("%s (%s)", version.Version, runtime.Version()))`). Called once by the binary.
pub fn set_app_version(version: &str, runtime: &str) {
    let _ = APP_VERSION.set(format!("{version} ({runtime})"));
}

fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "powerpc64" => "ppc64le",
        other => other,
    }
}

/// minio-go `libraryUserAgent` + app info.
pub(crate) fn user_agent(component: &str) -> String {
    let app = APP_VERSION
        .get()
        .cloned()
        .unwrap_or_else(|| format!("{} (rust)", env!("CARGO_PKG_VERSION")));
    format!(
        "MinIO ({}; {}) minio-go/{LIBRARY_VERSION} thanos-{component}/{app}",
        std::env::consts::OS,
        go_arch()
    )
}

/// minio-go `ErrorResponse`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub bucket_name: String,
    pub key: String,
    pub request_id: String,
    pub host_id: String,
    pub region: String,
    pub server: String,
    pub status_code: u16,
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "Error response code {}.", self.code)
        } else {
            f.write_str(&self.message)
        }
    }
}

impl std::error::Error for ErrorResponse {}

/// Errors returned by the S3 client.
#[derive(Debug)]
pub(crate) enum S3Error {
    /// An S3 error response (minio-go `ErrorResponse`).
    Response(Box<ErrorResponse>),
    /// A transport-level error (Go `*url.Error`).
    Request(String),
    /// Any other client-side error (invalid argument, decode error, ...).
    Other(String),
}

impl S3Error {
    pub fn code(&self) -> &str {
        match self {
            S3Error::Response(r) => &r.code,
            _ => "",
        }
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3Error::Response(r) => r.fmt(f),
            S3Error::Request(m) | S3Error::Other(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for S3Error {}

pub(crate) fn err_invalid_argument(message: impl Into<String>) -> S3Error {
    S3Error::Response(Box::new(ErrorResponse {
        status_code: 400,
        code: "InvalidArgument".into(),
        message: message.into(),
        request_id: "minio".into(),
        ..Default::default()
    }))
}

/// A request body that can be replayed for retries (minio-go retries only seekable bodies; every
/// body produced by the bucket client is seekable).
#[derive(Clone, Debug)]
pub(crate) enum Body {
    Empty,
    Bytes(Bytes),
    File { path: PathBuf, offset: u64, len: u64 },
}

impl Body {
    pub fn len(&self) -> u64 {
        match self {
            Body::Empty => 0,
            Body::Bytes(b) => b.len() as u64,
            Body::File { len, .. } => *len,
        }
    }

    pub async fn read_all(&self) -> std::io::Result<Bytes> {
        match self {
            Body::Empty => Ok(Bytes::new()),
            Body::Bytes(b) => Ok(b.clone()),
            Body::File { path, offset, len } => {
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                let mut f = tokio::fs::File::open(path).await?;
                f.seek(std::io::SeekFrom::Start(*offset)).await?;
                let mut buf = vec![0u8; *len as usize];
                f.read_exact(&mut buf).await?;
                Ok(Bytes::from(buf))
            }
        }
    }

    /// Streams the body in `chunk` sized pieces.
    fn stream(
        &self,
        chunk: usize,
    ) -> futures::stream::BoxStream<'static, std::io::Result<Bytes>> {
        match self.clone() {
            Body::Empty => futures::stream::empty().boxed(),
            Body::Bytes(b) => {
                let mut pieces = Vec::new();
                let mut off = 0;
                while off < b.len() {
                    let end = (off + chunk).min(b.len());
                    pieces.push(Ok(b.slice(off..end)));
                    off = end;
                }
                futures::stream::iter(pieces).boxed()
            }
            Body::File { path, offset, len } => {
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                futures::stream::unfold(
                    (None::<tokio::fs::File>, path, offset, len),
                    move |(file, path, offset, remaining)| async move {
                        if remaining == 0 {
                            return None;
                        }
                        let mut f = match file {
                            Some(f) => f,
                            None => {
                                let opened = async {
                                    let mut f = tokio::fs::File::open(&path).await?;
                                    f.seek(std::io::SeekFrom::Start(offset)).await?;
                                    Ok::<_, std::io::Error>(f)
                                }
                                .await;
                                match opened {
                                    Ok(f) => f,
                                    Err(e) => return Some((Err(e), (None, path, offset, 0))),
                                }
                            }
                        };
                        let n = (remaining as usize).min(chunk);
                        let mut buf = vec![0u8; n];
                        match f.read_exact(&mut buf).await {
                            Ok(_) => Some((
                                Ok(Bytes::from(buf)),
                                (Some(f), path, offset, remaining - n as u64),
                            )),
                            Err(e) => Some((Err(e), (None, path, offset, 0))),
                        }
                    },
                )
                .boxed()
            }
        }
    }
}

/// minio-go `requestMetadata`.
#[derive(Clone, Debug)]
pub(crate) struct RequestMetadata {
    pub bucket_name: String,
    pub object_name: String,
    pub query: Vec<(String, String)>,
    pub custom_header: HeaderMap,
    pub body: Body,
    pub content_md5_base64: String,
    pub content_sha256_hex: String,
    pub stream_sha256: bool,
    pub expect_200_ok_with_error: bool,
    pub bucket_location: String,
}

impl RequestMetadata {
    pub fn new(bucket_name: &str, object_name: &str) -> Self {
        Self {
            bucket_name: bucket_name.to_string(),
            object_name: object_name.to_string(),
            query: Vec::new(),
            custom_header: HeaderMap::new(),
            body: Body::Empty,
            content_md5_base64: String::new(),
            content_sha256_hex: String::new(),
            stream_sha256: false,
            expect_200_ok_with_error: false,
            bucket_location: String::new(),
        }
    }
}

/// A fully read HTTP response.
#[derive(Clone, Debug)]
pub(crate) struct Response {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Response {
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    }
}

/// Settings of [`Client::new`] (minio-go `Options` as filled by thanos).
pub(crate) struct ClientOptions {
    pub endpoint: EndpointUrl,
    pub secure: bool,
    pub region: String,
    pub lookup: S3BucketLookupType,
    pub dualstack_enabled: bool,
    pub credentials: Credentials,
    pub http: HttpTransport,
    pub component: String,
    pub trace: bool,
}

/// minio-go `Client`.
pub(crate) struct Client {
    pub endpoint: EndpointUrl,
    secure: bool,
    /// minio-go `Client.region` (configured or derived from the endpoint hostname).
    pub region: String,
    lookup: S3BucketLookupType,
    dualstack: bool,
    credentials: Credentials,
    http: HttpTransport,
    bucket_loc_cache: parking_lot::Mutex<HashMap<String, String>>,
    max_retries: usize,
    user_agent: String,
    trace: bool,
}

fn split_authority_host_port(authority: &str) -> (&str, Option<&str>) {
    if let Some(rest) = authority.strip_prefix('[') {
        if let Some((host, after)) = rest.split_once(']') {
            let port = after.strip_prefix(':');
            return (host, port);
        }
    }
    if let Some((host, port)) = authority.rsplit_once(':') {
        if !host.contains(':') {
            return (host, Some(port));
        }
    }
    (authority, None)
}

fn header_value(v: &str) -> HeaderValue {
    HeaderValue::from_str(v).unwrap_or_else(|_| HeaderValue::from_static(""))
}

/// minio-go `xmlDecoder` for flat S3 documents: returns the root element name and the text of
/// its direct children (repeated children are all returned in order).
pub(crate) fn xml_root_children(body: &[u8]) -> Option<(String, Vec<(String, String)>)> {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut root: Option<String> = None;
    let mut root_text = String::new();
    let mut depth = 0usize;
    let mut fields: Vec<(String, String)> = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                depth += 1;
                if depth == 1 {
                    root = Some(name);
                } else if depth == 2 {
                    fields.push((name, String::new()));
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if depth == 0 {
                    return Some((name, Vec::new()));
                }
                if depth == 1 {
                    fields.push((name, String::new()));
                }
            }
            Ok(Event::Text(t)) => {
                let text = t.unescape().ok()?.into_owned();
                if depth == 1 {
                    root_text.push_str(&text);
                } else if depth == 2 {
                    if let Some(last) = fields.last_mut() {
                        last.1.push_str(&text);
                    }
                }
            }
            Ok(Event::CData(t)) => {
                let text = String::from_utf8_lossy(&t).into_owned();
                if depth == 1 {
                    root_text.push_str(&text);
                } else if depth == 2 {
                    if let Some(last) = fields.last_mut() {
                        last.1.push_str(&text);
                    }
                }
            }
            Ok(Event::End(_)) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let root = root?;
                    if fields.is_empty() {
                        return Some((root, vec![(String::new(), root_text)]));
                    }
                    return Some((root, fields));
                }
            }
            Ok(Event::Eof) => return None,
            Ok(_) => {}
            Err(_) => return None,
        }
        buf.clear();
    }
}

/// minio-go `httpRespToErrorResponse`. Returns `None` for a successful response whose body is not
/// an `<Error>` document.
pub(crate) fn http_resp_to_error_response(
    status: u16,
    headers: &HeaderMap,
    body: &[u8],
    bucket_name: &str,
    object_name: &str,
) -> Option<ErrorResponse> {
    let header = |k: &str| {
        headers
            .get(k)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    };
    let success = matches!(status, 200 | 204 | 206);
    let mut resp = ErrorResponse {
        status_code: status,
        server: header("Server"),
        ..Default::default()
    };
    let trimmed = trim_ascii(body);
    match xml_root_children(body) {
        Some((root, fields)) if root == "Error" => {
            for (k, v) in fields {
                match k.as_str() {
                    "Code" => resp.code = v,
                    "Message" => resp.message = v,
                    "BucketName" => resp.bucket_name = v,
                    "Key" => resp.key = v,
                    "RequestId" => resp.request_id = v,
                    "HostId" => resp.host_id = v,
                    "Region" => resp.region = v,
                    _ => {}
                }
            }
        }
        parsed => {
            // A well-formed document of another type is an xml.UnmarshalError; on success
            // it is not an error response.
            if success && parsed.is_some() {
                return None;
            }
            let reason = reqwest::StatusCode::from_u16(status)
                .ok()
                .and_then(|s| s.canonical_reason())
                .unwrap_or("");
            let status_text = format!("{status} {reason}");
            let base = |code: &str, message: &str, key: bool| ErrorResponse {
                status_code: status,
                code: code.into(),
                message: message.into(),
                bucket_name: bucket_name.into(),
                key: if key { object_name.into() } else { String::new() },
                ..Default::default()
            };
            resp = match status {
                404 if object_name.is_empty() => {
                    base("NoSuchBucket", "The specified bucket does not exist.", false)
                }
                404 => base("NoSuchKey", "The specified key does not exist.", true),
                403 => base("AccessDenied", "Access Denied.", true),
                409 => base("Conflict", "Bucket not empty.", false),
                412 => base("PreconditionFailed", "Pre condition failed.", true),
                _ => {
                    let mut msg = status_text.clone();
                    if !trimmed.is_empty() {
                        msg = String::from_utf8_lossy(trimmed).into_owned();
                        if msg.len() > 1024 {
                            let mut cut = 1024;
                            while !msg.is_char_boundary(cut) {
                                cut -= 1;
                            }
                            msg = format!("{}...", &msg[..cut]);
                        }
                    }
                    ErrorResponse {
                        status_code: status,
                        code: status_text,
                        message: msg,
                        bucket_name: bucket_name.into(),
                        ..Default::default()
                    }
                }
            };
        }
    }
    let code = header("x-minio-error-code");
    if !code.is_empty() {
        resp.code = code;
    }
    let desc = header("x-minio-error-desc");
    if !desc.is_empty() {
        resp.message = desc.trim_matches('"').to_string();
    }
    if resp.request_id.is_empty() {
        resp.request_id = header("x-amz-request-id");
    }
    if resp.host_id.is_empty() {
        resp.host_id = header("x-amz-id-2");
    }
    if resp.region.is_empty() {
        resp.region = header("x-amz-bucket-region");
    }
    if resp.code == "InvalidRegion" && !resp.region.is_empty() {
        resp.message = format!(
            "Region does not match, expecting region \u{2018}{}\u{2019}.",
            resp.region
        );
    }
    Some(resp)
}

fn trim_ascii(b: &[u8]) -> &[u8] {
    let start = b.iter().position(|c| !c.is_ascii_whitespace()).unwrap_or(b.len());
    let end = b.iter().rposition(|c| !c.is_ascii_whitespace()).map_or(start, |e| e + 1);
    &b[start..end]
}

/// minio-go `processBucketLocationResponse`.
pub(crate) fn process_bucket_location_response(
    resp: &Response,
    bucket_name: &str,
) -> Result<String, S3Error> {
    if resp.status != 200 {
        let err = http_resp_to_error_response(resp.status, &resp.headers, &resp.body, bucket_name, "")
            .unwrap_or_default();
        match err.code.as_str() {
            "NotImplemented" => match err.server.as_str() {
                "AmazonSnowball" => return Ok("snowball".to_string()),
                "cloudflare" => return Ok("us-east-1".to_string()),
                _ => {}
            },
            "AuthorizationHeaderMalformed" | "InvalidRegion" | "AccessDenied" => {
                if err.region.is_empty() {
                    return Ok("us-east-1".to_string());
                }
                return Ok(err.region);
            }
            _ => {}
        }
        return Err(S3Error::Response(Box::new(err)));
    }
    let location = match xml_root_children(&resp.body) {
        Some((_, fields)) => fields.into_iter().map(|(_, v)| v).collect::<String>(),
        None => return Err(S3Error::Other("EOF".to_string())),
    };
    Ok(match location.as_str() {
        "" => "us-east-1".to_string(),
        "EU" => "eu-west-1".to_string(),
        _ => location,
    })
}

const RETRYABLE_S3_CODES: &[&str] = &[
    "RequestError",
    "RequestTimeout",
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "RequestThrottled",
    "InternalError",
    "ExpiredToken",
    "ExpiredTokenException",
    "TokenRefreshRequired",
    "SlowDown",
    "SlowDownWrite",
    "SlowDownRead",
];

/// `isS3CodeRetryable`.
pub(crate) fn is_s3_code_retryable(code: &str) -> bool {
    RETRYABLE_S3_CODES.contains(&code)
}

/// `isHTTPStatusRetryable`.
pub(crate) fn is_http_status_retryable(status: u16) -> bool {
    matches!(status, 408 | 429 | 499 | 500 | 502 | 503 | 504 | 520)
}

/// `isRequestErrorRetryable`: every transport error is retried except an unknown certificate
/// authority and plain-HTTP answers to HTTPS requests.
fn is_request_error_retryable(err: &reqwest::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = source {
        if let Some(
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)
            | rustls::Error::InvalidMessage(_),
        ) = e.downcast_ref::<rustls::Error>()
        {
            return false;
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if let Some(inner) = io.get_ref() {
                if let Some(tls) = inner.downcast_ref::<rustls::Error>() {
                    if matches!(
                        tls,
                        rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)
                            | rustls::Error::InvalidMessage(_)
                    ) {
                        return false;
                    }
                }
            }
        }
        source = e.source();
    }
    true
}

fn request_error_text(err: &reqwest::Error) -> String {
    let mut msg = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(e) = source {
        msg.push_str(": ");
        msg.push_str(&e.to_string());
        source = e.source();
    }
    msg
}

enum Attempt {
    Done(Result<Response, S3Error>),
    Retry(S3Error),
}

impl Client {
    pub fn new(opts: ClientOptions) -> Self {
        let region = if opts.region.is_empty() {
            s3utils::get_region_from_url(&opts.endpoint.hostname)
        } else {
            opts.region
        };
        // minio.New: Amazon endpoints are dual-stack by default; thanos switches it off with
        // SetS3EnableDualstack(false) (only effective for Amazon endpoints).
        let dualstack = s3utils::is_amazon_endpoint(&opts.endpoint.hostname) && opts.dualstack_enabled;
        Self {
            endpoint: opts.endpoint,
            secure: opts.secure,
            region,
            lookup: opts.lookup,
            dualstack,
            credentials: opts.credentials,
            http: opts.http,
            bucket_loc_cache: parking_lot::Mutex::new(HashMap::new()),
            max_retries: MAX_RETRY,
            user_agent: user_agent(&opts.component),
            trace: opts.trace,
        }
    }

    pub fn cached_location(&self, bucket: &str) -> Option<String> {
        self.bucket_loc_cache.lock().get(bucket).cloned()
    }

    /// `isVirtualHostStyleRequest`.
    pub fn is_virtual_host_style_request(&self, bucket_name: &str) -> bool {
        if bucket_name.is_empty() {
            return false;
        }
        match self.lookup {
            S3BucketLookupType::VirtualHost => true,
            S3BucketLookupType::Path => false,
            S3BucketLookupType::Auto => s3utils::is_virtual_host_supported(&self.endpoint, bucket_name),
        }
    }

    /// `makeTargetURL`: returns (host, decoded path).
    pub fn make_target_url(
        &self,
        bucket_name: &str,
        object_name: &str,
        bucket_location: &str,
        virtual_host: bool,
    ) -> (String, String) {
        let host = s3utils::target_host(&self.endpoint, bucket_location, self.dualstack);
        if bucket_name.is_empty() {
            return (host, "/".to_string());
        }
        if virtual_host {
            (format!("{bucket_name}.{host}"), format!("/{object_name}"))
        } else {
            (host, format!("/{bucket_name}/{object_name}"))
        }
    }

    fn service_type(&self) -> &'static str {
        if s3utils::is_amazon_outposts_endpoint(&self.endpoint.hostname) {
            signer::SERVICE_TYPE_S3_OUTPOSTS
        } else {
            signer::SERVICE_TYPE_S3
        }
    }

    /// `getBucketLocation`.
    pub async fn get_bucket_location(&self, bucket_name: &str) -> Result<String, S3Error> {
        if !self.region.is_empty() {
            return Ok(self.region.clone());
        }
        if let Some(loc) = self.cached_location(bucket_name) {
            return Ok(loc);
        }
        let (host, path, virtual_style) = {
            let host = s3utils::strip_default_port(self.endpoint.scheme, &self.endpoint.host);
            let virtual_style = self.is_virtual_host_style_request(bucket_name);
            if virtual_style {
                (format!("{bucket_name}.{host}"), "/".to_string(), true)
            } else {
                (host, format!("/{bucket_name}/"), false)
            }
        };
        let mut req = SignableRequest {
            method: "GET",
            host,
            path,
            query: vec![("location".to_string(), String::new())],
            headers: HeaderMap::new(),
        };
        req.headers.insert("user-agent", header_value(&self.user_agent));
        let value = self.credentials.get().await.map_err(S3Error::Other)?;
        let mut raw_query = if virtual_style {
            // `/?location` literally; signing rewrites it to `location=`.
            Some("location".to_string())
        } else {
            None
        };
        if value.signer_type != SignerType::Anonymous {
            let sha = if self.secure {
                signer::UNSIGNED_PAYLOAD
            } else {
                signer::EMPTY_SHA256_HEX
            };
            req.set_header("X-Amz-Content-Sha256", sha).map_err(S3Error::Other)?;
            let region = if s3utils::is_amazon_outposts_endpoint(&self.endpoint.hostname) {
                s3utils::get_default_location(&self.endpoint.hostname, &self.region)
            } else {
                "us-east-1".to_string()
            };
            signer::sign_v4(
                &mut req,
                &value.access_key_id,
                &value.secret_access_key,
                &value.session_token,
                &region,
                self.service_type(),
                Utc::now(),
            )
            .map_err(S3Error::Other)?;
            if !value.access_key_id.is_empty() && !value.secret_access_key.is_empty() {
                raw_query = None;
            }
        }
        let resp = self
            .send(&req, raw_query, None)
            .await
            .map_err(|e| S3Error::Request(e.message))?;
        let location = process_bucket_location_response(&resp, bucket_name)?;
        self.bucket_loc_cache
            .lock()
            .insert(bucket_name.to_string(), location.clone());
        Ok(location)
    }

    /// `newRequest`: returns the signed request and its wire body.
    async fn new_request(
        &self,
        method: &'static str,
        meta: &RequestMetadata,
    ) -> Result<(SignableRequest, Option<String>, WireBody), S3Error> {
        let mut location = meta.bucket_location.clone();
        if location.is_empty() {
            if !meta.bucket_name.is_empty() {
                location = self.get_bucket_location(&meta.bucket_name).await?;
            }
            if location.is_empty() {
                location = s3utils::get_default_location(&self.endpoint.hostname, &self.region);
            }
        }
        let is_make_bucket = meta.object_name.is_empty() && method == "PUT" && meta.query.is_empty();
        let virtual_host = self.is_virtual_host_style_request(&meta.bucket_name) && !is_make_bucket;
        let (host, path) =
            self.make_target_url(&meta.bucket_name, &meta.object_name, &location, virtual_host);

        let value = self.credentials.get().await.map_err(S3Error::Other)?;
        let mut req = SignableRequest {
            method,
            host,
            path,
            query: meta.query.clone(),
            headers: HeaderMap::new(),
        };
        req.headers.insert("user-agent", header_value(&self.user_agent));
        for (k, v) in meta.custom_header.iter() {
            req.headers.insert(k.clone(), v.clone());
        }
        let content_length = meta.body.len();
        if !meta.content_md5_base64.is_empty() {
            req.set_header("Content-Md5", &meta.content_md5_base64)
                .map_err(S3Error::Other)?;
        }
        // Anonymous: unsigned, query sent in s3utils.QueryEncode form.
        if value.signer_type == SignerType::Anonymous
            || value.access_key_id.is_empty()
            || value.secret_access_key.is_empty()
        {
            let raw = (!meta.query.is_empty()).then(|| query_encode(&meta.query));
            return Ok((req, raw, WireBody::plain(meta.body.clone())));
        }
        let service = self.service_type();
        if meta.stream_sha256 && !self.secure {
            let (chunk_signer, wire_len) = signer::streaming_sign_v4(
                &mut req,
                &value.access_key_id,
                &value.secret_access_key,
                &value.session_token,
                &location,
                content_length,
                Utc::now(),
                service,
            )
            .map_err(S3Error::Other)?;
            return Ok((
                req,
                None,
                WireBody::Streaming {
                    body: meta.body.clone(),
                    signer: chunk_signer,
                    wire_len,
                },
            ));
        }
        let sha = if meta.content_sha256_hex.is_empty() {
            signer::UNSIGNED_PAYLOAD.to_string()
        } else {
            meta.content_sha256_hex.clone()
        };
        req.set_header("X-Amz-Content-Sha256", &sha).map_err(S3Error::Other)?;
        signer::sign_v4(
            &mut req,
            &value.access_key_id,
            &value.secret_access_key,
            &value.session_token,
            &location,
            service,
            Utc::now(),
        )
        .map_err(S3Error::Other)?;
        Ok((req, None, WireBody::plain(meta.body.clone())))
    }

    /// minio-go `do` (one HTTP exchange; the whole response body is read).
    async fn send(
        &self,
        req: &SignableRequest,
        raw_query: Option<String>,
        body: Option<WireBody>,
    ) -> Result<Response, SendError> {
        let query = match raw_query {
            Some(q) => q,
            None => signer::canonical_query(&req.query),
        };
        let canonical_path = signer::canonical_uri_path(&req.path);
        let mut url = format!("{}://{}{}", self.endpoint.scheme, req.host, canonical_path);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&query);
        }
        let (client, dial_url) = if self.endpoint.scheme == "https" {
            if let Some(sni) = &self.http.sni {
                let (target_host, target_port) = split_authority_host_port(&req.host);
                let client = sni
                    .client_for_host(target_host)
                    .map_err(|message| SendError {
                        message,
                        retryable: false,
                    })?;
                let dial_url = if !sni.sni_host.eq_ignore_ascii_case(target_host) {
                    let dial_host = match target_port {
                        Some(p) => format!("{}:{p}", sni.sni_host),
                        None => sni.sni_host.clone(),
                    };
                    let mut u = format!("{}://{}{}", self.endpoint.scheme, dial_host, canonical_path);
                    if !query.is_empty() {
                        u.push('?');
                        u.push_str(&query);
                    }
                    u
                } else {
                    url.clone()
                };
                (client, dial_url)
            } else {
                (self.http.client.clone(), url.clone())
            }
        } else {
            (self.http.client.clone(), url.clone())
        };
        let method = reqwest::Method::from_bytes(req.method.as_bytes()).map_err(|e| SendError {
            message: format!("invalid HTTP method {}: {e}", req.method),
            retryable: false,
        })?;
        let mut builder = client.request(method, &dial_url);
        let mut headers = req.headers.clone();
        if !req.host.is_empty() && !headers.contains_key(reqwest::header::HOST) {
            let host_val = HeaderValue::from_str(&req.host).map_err(|e| SendError {
                message: format!("invalid host header {:?}: {e}", req.host),
                retryable: false,
            })?;
            headers.insert(reqwest::header::HOST, host_val);
        }
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let mut done_tx = Some(done_tx);
        match body {
            None => {
                if matches!(req.method, "PUT" | "POST") {
                    headers.insert("content-length", HeaderValue::from_static("0"));
                }
                if let Some(tx) = done_tx.take() {
                    let _ = tx.send(());
                }
            }
            Some(wire) => {
                let (stream, len) = wire.into_stream(done_tx.take());
                match len {
                    Some(len) => {
                        let val = HeaderValue::from_str(&len.to_string()).map_err(|e| SendError {
                            message: format!("invalid content-length header: {e}"),
                            retryable: false,
                        })?;
                        headers.insert("content-length", val);
                    }
                    None => {
                        headers.remove("content-length");
                    }
                }
                builder = builder.body(reqwest::Body::wrap_stream(stream));
            }
        }
        builder = builder.headers(headers.clone());
        if self.trace {
            tracing::debug!(s3TraceMsg = %dump_request(req.method, &url, &headers), "s3 trace");
        }

        let _permit = match &self.http.conn_limit {
            Some(sem) => Some(sem.clone().acquire_owned().await.map_err(|e| SendError {
                message: format!("HTTP connection limit semaphore closed: {e}"),
                retryable: false,
            })?),
            None => None,
        };
        let send = builder.send();
        let resp = match self.http.response_header_timeout {
            None => send.await?,
            Some(timeout) => {
                tokio::pin!(send);
                let deadline = async {
                    let _ = done_rx.await;
                    tokio::time::sleep(timeout).await;
                };
                tokio::select! {
                    r = &mut send => r?,
                    _ = deadline => {
                        return Err(SendError {
                            message: format!("{} \"{url}\": net/http: timeout awaiting response headers", go_method_name(req.method)),
                            retryable: true,
                        });
                    }
                }
            }
        };
        let status = resp.status().as_u16();
        let resp_headers = resp.headers().clone();
        let body = resp.bytes().await?;
        if self.trace {
            tracing::debug!(s3TraceMsg = %dump_response(status, &resp_headers), "s3 trace");
        }
        Ok(Response {
            status,
            headers: resp_headers,
            body,
        })
    }

    /// `executeMethod`.
    pub async fn execute_method(
        &self,
        method: &'static str,
        mut meta: RequestMetadata,
    ) -> Result<Response, S3Error> {
        let mut last_err: Option<S3Error> = None;
        for attempt in 0..self.max_retries {
            match self.attempt(method, &mut meta).await {
                Attempt::Done(r) => return r,
                Attempt::Retry(e) => last_err = Some(e),
            }
            tokio::time::sleep(self.backoff(attempt)).await;
        }
        Err(last_err.unwrap_or_else(|| S3Error::Other("request was not attempted".into())))
    }

    /// `newRetryTimer` backoff with full jitter.
    fn backoff(&self, attempt: usize) -> Duration {
        let mut sleep = DEFAULT_RETRY_UNIT * (1u32 << attempt.min(30));
        if sleep > DEFAULT_RETRY_CAP {
            sleep = DEFAULT_RETRY_CAP;
        }
        let jitter: f64 = rand::thread_rng().gen();
        sleep - sleep.mul_f64(jitter)
    }

    async fn attempt(&self, method: &'static str, meta: &mut RequestMetadata) -> Attempt {
        let (req, raw_query, body) = match self.new_request(method, meta).await {
            Ok(r) => r,
            Err(e) => {
                if matches!(
                    e.code(),
                    "ExpiredToken" | "ExpiredTokenException" | "TokenRefreshRequired"
                ) {
                    self.credentials.expire().await;
                }
                if is_s3_code_retryable(e.code()) {
                    return Attempt::Retry(e);
                }
                return Attempt::Done(Err(e));
            }
        };
        let body = if body.is_empty() { None } else { Some(body) };
        let res = match self.send(&req, raw_query, body).await {
            Ok(r) => r,
            Err(e) => {
                let retryable = e.retryable;
                let err = S3Error::Request(e.message);
                if retryable {
                    return Attempt::Retry(err);
                }
                return Attempt::Done(Err(err));
            }
        };
        let success = matches!(res.status, 200 | 204 | 206);
        if success && !meta.expect_200_ok_with_error {
            return Attempt::Done(Ok(res));
        }
        let api_err = match http_resp_to_error_response(
            res.status,
            &res.headers,
            &res.body,
            &meta.bucket_name,
            &meta.object_name,
        ) {
            None => return Attempt::Done(Ok(res)),
            Some(e) => e,
        };
        if self.region.is_empty()
            && matches!(
                api_err.code.as_str(),
                "AuthorizationHeaderMalformed" | "InvalidRegion" | "AccessDenied"
            )
        {
            if api_err.region.is_empty() {
                return Attempt::Done(Err(S3Error::Response(Box::new(api_err))));
            }
            if !meta.bucket_name.is_empty() {
                let mut cache = self.bucket_loc_cache.lock();
                if let Some(loc) = cache.get(&meta.bucket_name) {
                    if *loc != api_err.region {
                        cache.insert(meta.bucket_name.clone(), api_err.region.clone());
                        return Attempt::Retry(S3Error::Response(Box::new(api_err)));
                    }
                }
            } else if api_err.region != meta.bucket_location {
                meta.bucket_location = api_err.region.clone();
                return Attempt::Retry(S3Error::Response(Box::new(api_err)));
            }
        }
        if matches!(
            api_err.code.as_str(),
            "ExpiredToken" | "ExpiredTokenException" | "TokenRefreshRequired"
        ) {
            self.credentials.expire().await;
        }
        if (meta.expect_200_ok_with_error && res.status == 200 && api_err.code == "200 OK")
            || is_s3_code_retryable(&api_err.code)
            || is_http_status_retryable(res.status)
        {
            return Attempt::Retry(S3Error::Response(Box::new(api_err)));
        }
        Attempt::Done(Err(S3Error::Response(Box::new(api_err))))
    }
}

/// A transport error of one HTTP exchange (Go `*url.Error`).
#[derive(Debug)]
pub(crate) struct SendError {
    pub message: String,
    pub retryable: bool,
}

impl From<reqwest::Error> for SendError {
    fn from(e: reqwest::Error) -> Self {
        SendError {
            message: request_error_text(&e),
            retryable: is_request_error_retryable(&e),
        }
    }
}

/// Go `url.Error.Op` formatting of the HTTP method (`"Put"`, `"Get"`, ...).
fn go_method_name(method: &str) -> String {
    let mut c = method.chars();
    match c.next() {
        Some(f) => f.to_string() + &c.as_str().to_ascii_lowercase(),
        None => String::new(),
    }
}

fn dump_request(method: &str, url: &str, headers: &HeaderMap) -> String {
    let mut out = format!("---------START-HTTP---------\n{method} {url}\n");
    for (k, v) in headers {
        let v = v.to_str().unwrap_or("");
        let v = if k.as_str() == "authorization" {
            match v.find("Signature=") {
                Some(i) => format!("{}Signature=**REDACTED**", &v[..i]),
                None => "**REDACTED**".to_string(),
            }
        } else {
            v.to_string()
        };
        out.push_str(&format!("{k}: {v}\n"));
    }
    out
}

fn dump_response(status: u16, headers: &HeaderMap) -> String {
    let mut out = format!("HTTP/1.1 {status}\n");
    for (k, v) in headers {
        out.push_str(&format!("{k}: {}\n", v.to_str().unwrap_or("")));
    }
    out.push_str("---------END-HTTP---------");
    out
}

/// `s3utils.QueryEncode` (sorted keys, `EncodePath` + `/` percent-encoded).
fn query_encode(query: &[(String, String)]) -> String {
    let mut sorted: Vec<&(String, String)> = query.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    sorted
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                signer::encode_path(k).replace('/', "%2F"),
                signer::encode_path(v).replace('/', "%2F")
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// The body actually written on the wire.
pub(crate) enum WireBody {
    Plain(Body),
    Streaming {
        body: Body,
        signer: signer::ChunkSigner,
        wire_len: u64,
    },
}

impl WireBody {
    fn plain(body: Body) -> Self {
        WireBody::Plain(body)
    }

    fn is_empty(&self) -> bool {
        match self {
            WireBody::Plain(b) => b.len() == 0,
            // A zero-length streaming upload still carries the final signed chunk; Go sends it
            // with `Transfer-Encoding: chunked` (Content-Length 0 + non-nil body).
            WireBody::Streaming { .. } => false,
        }
    }

    /// Returns the byte stream and the Content-Length (`None` = chunked transfer encoding).
    fn into_stream(
        self,
        done: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> (
        futures::stream::BoxStream<'static, std::io::Result<Bytes>>,
        Option<u64>,
    ) {
        let (stream, len) = match self {
            WireBody::Plain(body) => {
                let len = body.len();
                (body.stream(256 * 1024), Some(len))
            }
            WireBody::Streaming {
                body,
                signer,
                wire_len,
            } => {
                let data_len = body.len();
                let inner = body.stream(signer::PAYLOAD_CHUNK_SIZE as usize);
                // minio-go StreamingReader: 64KiB chunks, each signature chained to the
                // previous one, terminated by a signed zero-length chunk.
                let signed = futures::stream::unfold(
                    Some((inner, signer)),
                    |state| async move {
                        let (mut inner, mut signer) = state?;
                        match inner.next().await {
                            Some(Ok(data)) => {
                                let mut out = Vec::with_capacity(data.len() + 100);
                                signer.sign_chunk(&data, &mut out);
                                Some((Ok(Bytes::from(out)), Some((inner, signer))))
                            }
                            Some(Err(e)) => Some((Err(e), None)),
                            None => {
                                let mut out = Vec::new();
                                signer.sign_chunk(&[], &mut out);
                                Some((Ok(Bytes::from(out)), None))
                            }
                        }
                    },
                )
                .boxed();
                let len = if data_len == 0 { None } else { Some(wire_len) };
                (signed, len)
            }
        };
        let mut done = done;
        let stream = stream
            .chain(futures::stream::poll_fn(move |_| {
                if let Some(tx) = done.take() {
                    let _ = tx.send(());
                }
                std::task::Poll::Ready(None)
            }))
            .boxed();
        (stream, len)
    }
}

#[cfg(test)]
mod d_tls_03_tests {
    use super::*;
    use std::sync::Arc;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::LazyConfigAcceptor;

    fn fixture(name: &str) -> String {
        format!(
            "{}/tests/fixtures/kafka_tls/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    #[tokio::test]
    async fn d_tls_03_s3_sni_isolates_pools_across_distinct_target_hosts() {
        async fn spawn_labeled_sni_server(
            bind_ip: &str,
            expected_sni: &'static str,
            body: &'static str,
        ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
            let listener = TcpListener::bind(format!("{bind_ip}:0")).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let endpoint = format!("{bind_ip}:{}", addr.port());
            let hits = Arc::new(AtomicUsize::new(0));
            let hits_srv = Arc::clone(&hits);

            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let certs: Vec<CertificateDer<'static>> =
                CertificateDer::pem_file_iter(fixture("sni.crt"))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
            let key = PrivateKeyDer::from_pem_file(fixture("sni.key")).unwrap();
            let sni_cfg = Arc::new(
                rustls::ServerConfig::builder_with_provider(provider)
                    .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
                    .unwrap()
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .unwrap(),
            );

            let handle = tokio::spawn(async move {
                loop {
                    let Ok((sock, _)) = listener.accept().await else {
                        break;
                    };
                    let hits_conn = Arc::clone(&hits_srv);
                    let sni_cfg = Arc::clone(&sni_cfg);
                    tokio::spawn(async move {
                        let acceptor =
                            LazyConfigAcceptor::new(rustls::server::Acceptor::default(), sock);
                        let Ok(start) = acceptor.await else {
                            return;
                        };
                        if start.client_hello().server_name() != Some(expected_sni) {
                            return;
                        }
                        let Ok(mut tls_stream) = start.into_stream(sni_cfg).await else {
                            return;
                        };
                        let mut buf = Vec::new();
                        loop {
                            buf.clear();
                            let mut tmp = [0u8; 4096];
                            loop {
                                match tls_stream.read(&mut tmp).await {
                                    Ok(0) | Err(_) => return,
                                    Ok(n) => buf.extend_from_slice(&tmp[..n]),
                                }
                                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                            hits_conn.fetch_add(1, Ordering::SeqCst);
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            if tls_stream.write_all(resp.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });
            (endpoint, hits, handle)
        }

        let (ep_a, hits_a, srv_a) =
            spawn_labeled_sni_server("127.0.0.1", "kafka.mimir-rs.test", "from-a").await;
        let (ep_b, hits_b, srv_b) =
            spawn_labeled_sni_server("127.0.0.2", "kafka.mimir-rs.test", "from-b").await;

        let http_cfg = mimir_config::BucketHTTPConfig {
            tls_ca_path: fixture("ca.crt"),
            tls_server_name: "kafka.mimir-rs.test".to_string(),
            max_idle_connections_per_host: 2,
            ..Default::default()
        };
        let transport = HttpTransport::new(&http_cfg).unwrap();
        let endpoint = crate::bucket::s3utils::get_endpoint_url(&ep_a, true).unwrap();
        let creds = crate::bucket::s3::credentials::new_credentials(
            &crate::bucket::s3::credentials::CredentialsConfig {
                aws_sdk_auth: false,
                access_key: "AKIA".into(),
                secret_key: "SECRET".into(),
                session_token: String::new(),
                region: "us-east-1".into(),
                sts_endpoint: String::new(),
            },
            Arc::new(|_| String::new()),
        );
        let client = Client::new(ClientOptions {
            endpoint,
            secure: true,
            region: "us-east-1".to_string(),
            lookup: S3BucketLookupType::Path,
            dualstack_enabled: false,
            credentials: creds,
            http: transport,
            component: "test".to_string(),
            trace: false,
        });

        let req_a = SignableRequest {
            method: "GET",
            host: ep_a.clone(),
            path: "/bkt/obj".to_string(),
            query: Vec::new(),
            headers: HeaderMap::new(),
        };
        let req_b = SignableRequest {
            method: "GET",
            host: ep_b.clone(),
            path: "/bkt/obj".to_string(),
            query: Vec::new(),
            headers: HeaderMap::new(),
        };

        // Request 1 hits host A and leaves an idle keep-alive connection open in A's pool.
        let r1 = tokio::time::timeout(std::time::Duration::from_secs(3), client.send(&req_a, None, None))
            .await
            .expect("r1 timed out")
            .unwrap();
        assert_eq!(r1.body.as_ref(), b"from-a");
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        // Request 2 targets host B through the SAME S3 client with pooling enabled: must NOT
        // reuse host A's idle connection.
        let r2 = tokio::time::timeout(std::time::Duration::from_secs(3), client.send(&req_b, None, None))
            .await
            .expect("r2 timed out")
            .unwrap();
        assert_eq!(r2.body.as_ref(), b"from-b");
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 1);

        // Concurrent requests to host A and host B must never race or cross-route.
        for _ in 0..8 {
            let (ra, rb) = tokio::join!(
                client.send(&req_a, None, None),
                client.send(&req_b, None, None),
            );
            assert_eq!(ra.unwrap().body.as_ref(), b"from-a");
            assert_eq!(rb.unwrap().body.as_ref(), b"from-b");
        }
        assert_eq!(hits_a.load(Ordering::SeqCst), 9);
        assert_eq!(hits_b.load(Ordering::SeqCst), 9);

        srv_a.abort();
        srv_b.abort();
    }
}

