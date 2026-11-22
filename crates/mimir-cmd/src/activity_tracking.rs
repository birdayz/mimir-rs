//! HTTP activity tracking, max body size, and tenant validation middlewares.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/api/activity_tracking.go
//! - github.com/grafana/mimir/pkg/api/tenant.go
//! - github.com/grafana/mimir/pkg/api/api.go (`newMaxBodySizeHandler`)

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http::{header, HeaderMap, Method, StatusCode};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::BodyExt as _;
use mimir_common::activitytracker::ActivityTracker;
use mimir_common::logging::{LogLevel, Logger};
use mimir_common::tenant::{
    extract_org_id_bytes_from_http_headers, extract_org_id_from_http_headers,
    parse_tenant_ids_bytes,
};

/// Error produced by [`MaxBytesBody`] when a request body exceeds `limit` bytes
/// (matching Go `http.MaxBytesError`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxBytesError {
    pub limit: usize,
}

impl fmt::Display for MaxBytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("http: request body too large")
    }
}

impl StdError for MaxBytesError {}

/// Checks whether `err` or any error in its `source()` chain is a [`MaxBytesError`].
pub fn is_request_body_too_large(mut err: &(dyn StdError + 'static)) -> bool {
    loop {
        if err.is::<MaxBytesError>() {
            return true;
        }
        let Some(next) = err.source() else {
            return false;
        };
        err = next;
    }
}

/// Streaming [`http_body::Body`] wrapper mirroring Go `http.MaxBytesReader`.
pub struct MaxBytesBody {
    inner: Body,
    remaining: usize,
    limit: usize,
    exceeded: bool,
}

impl MaxBytesBody {
    pub fn new(inner: Body, limit: usize) -> Self {
        Self {
            inner,
            remaining: limit,
            limit,
            exceeded: false,
        }
    }
}

impl HttpBody for MaxBytesBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.exceeded {
            return Poll::Ready(Some(Err(axum::Error::new(MaxBytesError {
                limit: self.limit,
            }))));
        }
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    if data.len() > self.remaining {
                        self.exceeded = true;
                        return Poll::Ready(Some(Err(axum::Error::new(MaxBytesError {
                            limit: self.limit,
                        }))));
                    }
                    self.remaining -= data.len();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Middleware equivalent to `newMaxBodySizeHandler` in `pkg/api/api.go`.
pub async fn max_body_size_middleware(
    State(limit): State<usize>,
    mut req: Request,
    next: Next,
) -> Response {
    if limit > 0 {
        let body = std::mem::replace(req.body_mut(), Body::empty());
        *req.body_mut() = Body::new(MaxBytesBody::new(body, limit));
    }
    next.run(req).await
}

#[derive(Clone)]
pub struct ActivityTrackingState {
    pub tracker: Arc<ActivityTracker>,
    pub logger: Arc<dyn Logger>,
}

struct ActivityDeleteGuard {
    tracker: Arc<ActivityTracker>,
    idx: isize,
}

impl Drop for ActivityDeleteGuard {
    fn drop(&mut self) {
        self.tracker.delete(self.idx);
    }
}

/// Middleware equivalent to `NewActivityTrackingMiddleware` in `pkg/api/activity_tracking.go`.
pub async fn activity_tracking_middleware(
    State(st): State<ActivityTrackingState>,
    mut req: Request,
    next: Next,
) -> Response {
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let is_chunked = req
        .headers()
        .get(header::TRANSFER_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    let known_content_len = !is_chunked
        && (req.headers().contains_key(header::CONTENT_LENGTH)
            || req.body().size_hint().exact().is_some());

    let params = if content_type == "application/x-www-form-urlencoded" && known_content_len {
        let body = std::mem::replace(req.body_mut(), Body::empty());
        let collected = match body.collect().await {
            Ok(c) => c.to_bytes(),
            Err(err) => {
                if is_request_body_too_large(&err) {
                    return (StatusCode::PAYLOAD_TOO_LARGE, "http: request body too large\n")
                        .into_response();
                }
                let err_s = err.to_string();
                st.logger.log(
                    LogLevel::Error,
                    &[
                        ("msg", "failed to parse request params for activity tracking"),
                        ("path", req.uri().path()),
                        ("contentType", &content_type),
                        ("err", &err_s),
                    ],
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to parse request params for activity tracking\n",
                )
                    .into_response();
            }
        };
        *req.body_mut() = Body::from(collected.clone());
        match parse_go_form_values(req.uri().query(), Some(&collected)) {
            Ok(v) => v,
            Err(err) => {
                st.logger.log(
                    LogLevel::Error,
                    &[
                        ("msg", "failed to parse request params for activity tracking"),
                        ("path", req.uri().path()),
                        ("contentType", &content_type),
                        ("err", &err),
                    ],
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to parse request params for activity tracking\n",
                )
                    .into_response();
            }
        }
    } else {
        parse_go_query_values(req.uri().query().unwrap_or("")).unwrap_or_default()
    };

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();
    let idx = st.tracker.insert(|| {
        to_activity_tracker_string(&method, &path, &headers, &params)
    });
    if idx < 0 {
        st.logger.log(
            LogLevel::Error,
            &[("msg", "failed to insert request for activity tracking")],
        );
    }
    let _guard = ActivityDeleteGuard {
        tracker: Arc::clone(&st.tracker),
        idx,
    };
    next.run(req).await
}

pub fn to_activity_tracker_string(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    request_params: &BTreeMap<String, Vec<String>>,
) -> String {
    let tenant_id = extract_org_id_from_http_headers(headers)
        .unwrap_or_else(|_| "(unknown)".to_string());
    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let mut params = encode_go_url_values(request_params);
    if params.is_empty() {
        params = "(no params)".to_string();
    }
    format!("user:{tenant_id} UA:{user_agent} req:{method} {path} {params}")
}

fn parse_go_form_values(
    raw_query: Option<&str>,
    body: Option<&[u8]>,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let mut map = BTreeMap::new();
    if let Some(q) = raw_query {
        merge_query_into(&mut map, q, false)?;
    }
    if let Some(b) = body {
        let s = std::str::from_utf8(b).map_err(|e| format!("invalid utf-8 form body: {e}"))?;
        merge_query_into(&mut map, s, true)?;
    }
    Ok(map)
}

pub fn parse_go_query_values(raw: &str) -> Result<BTreeMap<String, Vec<String>>, String> {
    let mut map = BTreeMap::new();
    merge_query_into(&mut map, raw, false)?;
    Ok(map)
}

fn merge_query_into(
    map: &mut BTreeMap<String, Vec<String>>,
    raw: &str,
    strict_semicolon: bool,
) -> Result<(), String> {
    if raw.is_empty() {
        return Ok(());
    }
    for part in raw.split('&') {
        if part.is_empty() {
            continue;
        }
        if part.contains(';') {
            if strict_semicolon {
                return Err("invalid semicolon separator in query".to_string());
            }
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let k = form_unescape(k)?;
        let v = form_unescape(v)?;
        map.entry(k).or_default().push(v);
    }
    Ok(())
}

fn form_unescape(s: &str) -> Result<String, String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err("invalid URL escape".to_string());
                }
                let hi = (bytes[i + 1] as char).to_digit(16).ok_or("invalid URL escape")?;
                let lo = (bytes[i + 2] as char).to_digit(16).ok_or("invalid URL escape")?;
                out.push(((hi << 4) | lo) as u8);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

fn encode_go_url_values(values: &BTreeMap<String, Vec<String>>) -> String {
    let mut parts = Vec::new();
    for (k, vs) in values {
        let ek = form_escape(k);
        for v in vs {
            parts.push(format!("{ek}={}", form_escape(v)));
        }
    }
    parts.join("&")
}

fn form_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
            b' ' => out.push('+'),
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                use std::fmt::Write as _;
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// `pkg/api/tenant.go`: `newTenantValidationMiddleware` + `middleware.AuthenticateUser`.
#[derive(Debug, Clone, Copy)]
pub struct TenantValidationConfig {
    pub federation: bool,
    pub max_tenants: usize,
}

pub async fn tenant_auth_and_validation_middleware(
    State(cfg): State<TenantValidationConfig>,
    req: Request,
    next: Next,
) -> Response {
    let org_bytes = match extract_org_id_bytes_from_http_headers(req.headers()) {
        Ok(b) => b,
        Err(err) => {
            return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
        }
    };
    let ids = match parse_tenant_ids_bytes(org_bytes) {
        Ok(ids) => ids,
        Err(err) => {
            return (StatusCode::UNAUTHORIZED, format!("{err}\n")).into_response();
        }
    };
    let num_ids = ids.len();
    if !cfg.federation && num_ids > 1 {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("too many tenant IDs present in the request. max: 1 actual: {num_ids}\n"),
        )
            .into_response();
    }
    if cfg.federation && cfg.max_tenants > 0 && num_ids > cfg.max_tenants {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "too many tenant IDs present in the request. max: {} actual: {num_ids}\n",
                cfg.max_tenants
            ),
        )
            .into_response();
    }
    next.run(req).await
}
