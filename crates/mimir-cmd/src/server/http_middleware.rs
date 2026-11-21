//! HTTP middleware chain of the dskit server.
//!
//! Ported from:
//! - github.com/grafana/dskit/server/server.go (`BuildHTTPMiddleware`: `RouteInjector`, `Tracer`,
//!   `Log`, `Instrument`, then `ClusterValidationMiddleware` when enabled)
//! - github.com/grafana/dskit/middleware/route_injector.go (`getRouteName`, `MakeLabelValue`;
//!   unmatched requests are reported as `other`)
//! - github.com/grafana/dskit/middleware/instrument.go (`cortex_request_duration_seconds`,
//!   `cortex_request_message_bytes`, `cortex_response_message_bytes`, `cortex_inflight_requests`,
//!   `IsWSHandshakeRequest`)
//! - github.com/grafana/dskit/middleware/logging.go (`Log` with Mimir's
//!   `DisableRequestSuccessLog = true`)
//! - github.com/grafana/dskit/middleware/source_ips.go (`SourceIPExtractor`)
//! - github.com/grafana/dskit/middleware/http_cluster.go (`ClusterValidationMiddleware`)
//!
//! The dskit HTTP middleware only wraps requests served by the HTTP server
//! ([`super::http_server::ServedOverHttp`]); requests arriving over `httpgrpc.HTTP/Handle` hit the
//! router directly, as in dskit.

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http::{header, HeaderMap, StatusCode};
use http_body::Frame;
use mimir_common::logging::{LogLevel, LogValue, Logger};
use mimir_common::tracing::{
    export_span, extract_from_http_headers, global_tracing, now_unix_nanos,
    start_server_span_context, with_trace_context, ExportedSpan, SpanAttrValue, SpanAttribute,
    TraceContext,
};
use mimir_config::GoDuration;
use parking_lot::Mutex;
use regex::Regex;

use super::body::TrackedBody;
use super::http_server::ServedOverHttp;
use super::metrics::{make_label_value, ServerMetrics};

/// Marker extension on 405 responses produced by the router's method-mismatch fallback (as
/// opposed to handlers answering 405 themselves): gorilla/mux reports such requests as unmatched.
#[derive(Clone, Copy, Debug)]
pub struct MethodMismatch;

/// Router fallback for a known path with a method it does not serve (gorilla/mux
/// `methodNotAllowedHandler`: empty 405).
pub async fn method_not_allowed() -> Response {
    let mut resp = StatusCode::METHOD_NOT_ALLOWED.into_response();
    resp.extensions_mut().insert(MethodMismatch);
    resp
}

/// Header names logged requests never include (`middleware.AlwaysExcludedHeaders`).
const ALWAYS_EXCLUDED_HEADERS: &[&str] = &[
    "Cookie",
    "X-Csrf-Token",
    "Authorization",
    "X-Grafana-Id",
    "X-Access-Token",
];

/// dskit `SourceIPExtractor`.
#[derive(Clone)]
pub struct SourceIps {
    /// Custom header and the regex extracting addresses from it.
    custom: Option<(String, Regex)>,
    extract_all_hosts: bool,
}

impl SourceIps {
    /// `middleware.NewSourceIPs`.
    pub fn new(header: &str, regex: &str, extract_all_hosts: bool) -> anyhow::Result<Self> {
        if header.is_empty() != regex.is_empty() {
            anyhow::bail!("either both a header field and a regex have to be given or neither");
        }
        let custom = if regex.is_empty() {
            None
        } else {
            let re = Regex::new(regex).map_err(|_| anyhow::anyhow!("invalid regex given"))?;
            Some((header.to_string(), re))
        };
        Ok(Self {
            custom,
            extract_all_hosts,
        })
    }

    fn extract_header(&self, h: &str, re: &Regex) -> Vec<String> {
        let mut out = Vec::new();
        for caps in re.captures_iter(h) {
            if let Some(m) = caps.get(1) {
                out.push(m.as_str().to_string());
            }
            if !self.extract_all_hosts {
                break;
            }
        }
        out
    }

    fn get_ip(&self, headers: &HeaderMap) -> Vec<String> {
        let get = |n: &str| headers.get(n).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        if let Some((header, re)) = &self.custom {
            let h = get(header);
            if h.is_empty() {
                return Vec::new();
            }
            return self.extract_header(&h, re);
        }
        static FORWARDED: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
        static XFF: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
        let fwd = get("Forwarded");
        if !fwd.is_empty() {
            let re = FORWARDED.get_or_init(|| Regex::new(r#"(?i)(?:for=)(?: *"?([^;,]+)"? *)"#).expect("valid"));
            return self.extract_header(&fwd, re);
        }
        let real = get("X-Real-IP");
        if !real.is_empty() {
            return vec![real];
        }
        let xff = get("X-Forwarded-For").replace(' ', "");
        if !xff.is_empty() {
            let re = XFF.get_or_init(|| Regex::new(r#"(?: *"?([^,]+)"? *)"#).expect("valid"));
            return self.extract_header(&xff, re);
        }
        Vec::new()
    }

    /// `SourceIPExtractor.Get`.
    pub fn get(&self, headers: &HeaderMap, remote: &str) -> String {
        let hosts: Vec<String> = self.get_ip(headers).iter().map(|a| extract_host(a)).collect();
        let fwd = hosts.join(", ");
        if fwd.is_empty() {
            if remote.is_empty() {
                return String::new();
            }
            return extract_host(remote);
        }
        if remote.is_empty() {
            return fwd;
        }
        let remote_ip = extract_host(remote);
        if fwd == remote_ip {
            return remote_ip;
        }
        format!("{fwd}, {remote_ip}")
    }
}

fn extract_host(address: &str) -> String {
    if let Ok(ip) = address.parse::<std::net::IpAddr>() {
        return ip.to_string();
    }
    if let Ok(sa) = address.parse::<std::net::SocketAddr>() {
        return sa.ip().to_string();
    }
    match address.rsplit_once(':') {
        Some((h, _)) if !h.is_empty() => h.trim_start_matches('[').trim_end_matches(']').to_string(),
        _ => address.to_string(),
    }
}

/// dskit `ClusterValidationMiddleware` configuration.
#[derive(Clone)]
pub struct HttpClusterValidation {
    pub allowed: Vec<String>,
    pub soft: bool,
    pub excluded_path: Regex,
    pub excluded_user_agent: Option<Regex>,
}

impl HttpClusterValidation {
    pub fn new(allowed: Vec<String>, soft: bool, excluded_paths: &[String], excluded_uas: &[String]) -> anyhow::Result<Self> {
        if allowed.is_empty() {
            anyhow::bail!("no cluster labels provided");
        }
        let mut re = String::from(".*/(metrics|debug/pprof.*|ready");
        for p in excluded_paths {
            re.push('|');
            re.push_str(&regex::escape(p));
        }
        re.push(')');
        let excluded_user_agent = if excluded_uas.is_empty() {
            None
        } else {
            Some(Regex::new(&format!("({})", excluded_uas.join("|")))?)
        };
        Ok(Self {
            allowed,
            soft,
            excluded_path: Regex::new(&re)?,
            excluded_user_agent,
        })
    }
}

#[derive(Clone)]
pub struct HttpMiddlewareConfig {
    pub metrics: ServerMetrics,
    pub logger: Arc<dyn Logger>,
    pub log_request_headers: bool,
    pub log_request_at_info_level: bool,
    pub log_source_ips: Option<SourceIps>,
    pub trace_source_ips: Option<SourceIps>,
    pub exclude_headers: Vec<String>,
    pub trace_request_headers: bool,
    pub trace_exclude_headers: Vec<String>,
    pub create_new_traces: bool,
    pub cluster_validation: Option<HttpClusterValidation>,
    pub http_path_prefix: String,
    pub cost_attribution_path: Option<String>,
}

/// Checks BEFORE `inflight`, `check_cluster`, and OTel span creation whether the matched Axum
/// route rejects `req.method()` with a `gorilla/mux` `ErrMethodMismatch`, matching dskit's
/// `RouteInjector` (`routeName = ""`, `other` / `<unknown-route>`).
fn is_method_mismatch_before_run(req: &Request, cfg: &HttpMiddlewareConfig) -> bool {
    let Some(matched_raw) = req.extensions().get::<MatchedPath>().map(|m| m.as_str()) else {
        return false;
    };
    let path = if !cfg.http_path_prefix.is_empty() {
        matched_raw
            .strip_prefix(&cfg.http_path_prefix)
            .unwrap_or(matched_raw)
    } else {
        matched_raw
    };
    let m = req.method();
    match path {
        "/ready"
        | "/metrics"
        | "/debug/pprof/cmdline"
        | "/debug/pprof"
        | "/debug/pprof/"
        | "/debug/pprof/*name"
        | "/debug/pprof/profile"
        | "/debug/pprof/symbol" => false,
        "/ingester/flush" | "/ingester/ring" | "/ingester/partition-ring" => {
            !matches!(*m, http::Method::GET | http::Method::POST)
        }
        p if p.starts_with("/ingester/partition-ring/compartment-") => {
            !matches!(*m, http::Method::GET | http::Method::POST)
        }
        "/ingester/prepare-shutdown"
        | "/ingester/prepare-partition-downscale"
        | "/ingester/prepare-instance-ring-downscale" => {
            !matches!(*m, http::Method::GET | http::Method::POST | http::Method::DELETE)
        }
        "/ingester/unregister-on-shutdown" => {
            !matches!(*m, http::Method::GET | http::Method::PUT | http::Method::DELETE)
        }
        "/ingester/shutdown" => *m != http::Method::POST,
        "/services"
        | "/config"
        | "/api/v1/status/buildinfo"
        | "/api/v1/status/config"
        | "/api/v1/status/flags"
        | "/debug/fgprof"
        | "/static/"
        | "/static/*path"
        | "/runtime_config"
        | "/api/v1/user_limits"
        | "/"
        | "/memberlist"
        | "/ingester/tsdb_metrics"
        | "/ingester/tenants"
        | "/ingester/tsdb/:tenant"
        | "/ingester/label_churn/:tenant" => *m != http::Method::GET,
        p if cfg.cost_attribution_path.as_deref() == Some(p) => *m != http::Method::GET,
        _ => false,
    }
}

/// Route name for metrics: the matched route template through `MakeLabelValue`; prefix routes
/// (`/*rest` in axum) are named after their prefix, as gorilla/mux `PathPrefix` templates are.
fn route_name(req: &Request) -> Option<String> {
    let p = req.extensions().get::<MatchedPath>()?.as_str();
    let mut p = match p.rfind("/*") {
        Some(i) => &p[..i],
        None => p,
    };
    // dskit registers `/debug/pprof/cmdline` as its own route and everything else under the
    // `/debug/pprof` PathPrefix route.
    const PPROF: &str = "/debug/pprof";
    if let Some(i) = p.find(PPROF) {
        if &p[i + PPROF.len()..] != "/cmdline" {
            p = &p[..i + PPROF.len()];
        }
    }
    Some(make_label_value(&p.replace(':', "")))
}

fn is_ws_handshake(headers: &HeaderMap) -> bool {
    let get = |n: header::HeaderName| headers.get(n).and_then(|v| v.to_str().ok()).unwrap_or("").to_ascii_lowercase();
    get(header::UPGRADE) == "websocket" && get(header::CONNECTION).split(',').any(|p| p.trim() == "upgrade")
}

fn dump_headers(headers: &HeaderMap, exclude: &[String]) -> String {
    let mut lines: Vec<String> = Vec::new();
    let mut keys: Vec<String> = headers
        .keys()
        .map(|k| crate::grpc::http_canonical_header_key(k.as_str()))
        .collect();
    keys.sort();
    keys.dedup();
    for k in keys {
        if ALWAYS_EXCLUDED_HEADERS.contains(&k.as_str()) || exclude.contains(&k) {
            continue;
        }
        for v in headers.get_all(k.as_str()) {
            lines.push(format!("{k}: {}", String::from_utf8_lossy(v.as_bytes())));
        }
    }
    lines.iter().map(|l| format!("{l}; ")).collect()
}

fn build_http_span_attributes(
    req: &Request,
    route: Option<&str>,
    trace_source_ips: Option<&SourceIps>,
    remote: &str,
    trace_headers: bool,
    trace_exclude_headers: &[String],
) -> Vec<SpanAttribute> {
    let mut attrs = Vec::new();
    if let Some(r) = route.filter(|r| !r.is_empty()) {
        attrs.push(SpanAttribute {
            key: "http.route".to_string(),
            value: SpanAttrValue::String(r.to_string()),
        });
    }
    if let Some(sips) = trace_source_ips {
        attrs.push(SpanAttribute {
            key: "source_ips".to_string(),
            value: SpanAttrValue::String(sips.get(req.headers(), remote)),
        });
    }
    if trace_headers {
        const MAX_HEADERS: usize = 100;
        for name in req.headers().keys().take(MAX_HEADERS) {
            let canon = crate::grpc::http_canonical_header_key(name.as_str());
            if ALWAYS_EXCLUDED_HEADERS.contains(&canon.as_str())
                || trace_exclude_headers.contains(&canon)
            {
                attrs.push(SpanAttribute {
                    key: format!("http.header.{canon}.present"),
                    value: SpanAttrValue::String("true".to_string()),
                });
            } else {
                let vals: Vec<String> = req
                    .headers()
                    .get_all(name)
                    .iter()
                    .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
                    .collect();
                attrs.push(SpanAttribute {
                    key: format!("http.header.{canon}"),
                    value: SpanAttrValue::StringArray(vals),
                });
            }
        }
    }
    attrs
}

/// The dskit HTTP middleware chain (`Tracer` + `Log` + `Instrument` + cluster validation).
pub async fn dskit_http_middleware(State(cfg): State<Arc<HttpMiddlewareConfig>>, mut req: Request, next: Next) -> Response {
    let Some(served) = req.extensions().get::<ServedOverHttp>().copied() else {
        return next.run(req).await;
    };
    let begin = Instant::now();
    let start_unix_nano = now_unix_nanos();
    let method = req.method().as_str().to_string();
    let method_mismatch = is_method_mismatch_before_run(&req, &cfg);
    let matched_route = if method_mismatch {
        None
    } else {
        route_name(&req)
    };
    let route = matched_route.clone().unwrap_or_else(|| "other".to_string());
    let uri = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| req.uri().to_string());
    let ws = is_ws_handshake(req.headers());
    let remote = served.remote_addr.to_string();
    let source_ips = cfg.log_source_ips.as_ref().map(|s| s.get(req.headers(), &remote));
    let headers_dump = cfg.log_request_headers.then(|| dump_headers(req.headers(), &cfg.exclude_headers));

    let extracted_trace = extract_from_http_headers(req.headers());
    let (span_ctx, parent_span_id) = start_server_span_context(&extracted_trace, cfg.create_new_traces);
    let should_export = span_ctx.is_sampled() && global_tracing().is_some_and(|t| t.exporter.is_some());
    let span_attrs = if should_export {
        build_http_span_attributes(
            &req,
            matched_route.as_deref(),
            cfg.trace_source_ips.as_ref(),
            &remote,
            cfg.trace_request_headers,
            &cfg.trace_exclude_headers,
        )
    } else {
        Vec::new()
    };
    if span_ctx.is_valid() {
        req.extensions_mut().insert(span_ctx.clone());
    }

    let inflight = cfg.metrics.inflight_requests.with_label_values(&[&method, &route]);
    inflight.inc();

    let read = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (parts, body) = req.into_parts();
    let read_c = Arc::clone(&read);
    let body = Body::new(TrackedBody::new(body).on_frame(Box::new(move |f: &Frame<Bytes>| {
        if let Some(d) = f.data_ref() {
            read_c.fetch_add(d.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
    })));
    let req = Request::from_parts(parts, body);

    let resp = with_trace_context(span_ctx.clone(), async {
        match &cfg.cluster_validation {
            Some(cv) => match check_cluster(&req, cv, matched_route.as_deref(), &cfg, &span_ctx) {
                Some(rejection) => rejection,
                None => next.run(req).await,
            },
            None => next.run(req).await,
        }
    })
    .await;

    let status = resp.status();
    // gorilla/mux reports a method mismatch as an unmatched route.
    let route = if resp.extensions().get::<MethodMismatch>().is_some() {
        "other".to_string()
    } else {
        route
    };
    let code = status.as_u16();
    let capture_body = !(100..500).contains(&code) && code != 502 && code != 503 && !(200..300).contains(&code);
    let written = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let (w, cap) = (Arc::clone(&written), Arc::clone(&captured));
    let on_frame = Box::new(move |f: &Frame<Bytes>| {
        if let Some(d) = f.data_ref() {
            w.fetch_add(d.len() as u64, std::sync::atomic::Ordering::Relaxed);
            if capture_body {
                let mut c = cap.lock();
                let room = 4096usize.saturating_sub(c.len());
                c.extend_from_slice(&d[..d.len().min(room)]);
            }
        }
    });
    let m = cfg.metrics.clone();
    let cfg2 = Arc::clone(&cfg);
    let span_name = match matched_route.as_deref() {
        Some(r) if !r.is_empty() => format!("HTTP {method} - {r}"),
        _ => format!("HTTP {method}"),
    };
    let on_done = Box::new(move |_completed: bool| {
        let elapsed = begin.elapsed();
        if should_export {
            if let Some(rt) = global_tracing() {
                export_span(ExportedSpan {
                    service_name: rt.service_name.clone(),
                    trace_id: span_ctx.trace_id,
                    span_id: span_ctx.span_id,
                    parent_span_id,
                    name: span_name.clone(),
                    kind: 2,
                    start_time_unix_nano: start_unix_nano,
                    end_time_unix_nano: start_unix_nano.saturating_add(elapsed.as_nanos() as u64),
                    attributes: span_attrs.clone(),
                    status_error: code >= 500,
                });
            }
        }
        m.received_message_size
            .with_label_values(&[&method, &route])
            .observe(read.load(std::sync::atomic::Ordering::Relaxed) as f64);
        m.sent_message_size
            .with_label_values(&[&method, &route])
            .observe(written.load(std::sync::atomic::Ordering::Relaxed) as f64);
        m.request_duration
            .with_label_values(&[&method, &route, &code.to_string(), &ws.to_string()])
            .observe(elapsed.as_secs_f64());
        inflight.dec();
        log_request(&cfg2, &method, &uri, code, elapsed, ws, &captured.lock(), source_ips.as_deref(), headers_dump.as_deref(), &span_ctx);
    });
    let (parts, body) = resp.into_parts();
    Response::from_parts(parts, Body::new(TrackedBody::new(body).on_frame(on_frame).on_done(on_done)))
}

#[allow(clippy::too_many_arguments)]
fn log_request(
    cfg: &HttpMiddlewareConfig,
    method: &str,
    uri: &str,
    code: u16,
    elapsed: std::time::Duration,
    ws: bool,
    body: &[u8],
    source_ips: Option<&str>,
    headers: Option<&str>,
    span_ctx: &TraceContext,
) {
    // Mimir sets DisableRequestSuccessLog.
    if (200..300).contains(&code) {
        return;
    }
    let dur = GoDuration(elapsed.as_nanos() as i64).to_string();
    let (level, msg) = if (100..500).contains(&code) || code == 502 || code == 503 {
        let lvl = if cfg.log_request_at_info_level { LogLevel::Info } else { LogLevel::Debug };
        let msg = match headers {
            Some(h) => format!("{method} {uri} ({code}) {dur} ws: {ws}; {h}"),
            None => format!("{method} {uri} ({code}) {dur}"),
        };
        (lvl, msg)
    } else {
        let b = String::from_utf8_lossy(body);
        let msg = match headers {
            Some(h) => format!("{method} {uri} ({code}) {dur} Response: {b} ws: {ws}; {h}"),
            None => format!("{method} {uri} ({code}) {dur} Response: {b}"),
        };
        (LogLevel::Warn, msg)
    };
    let (trace_id, sampled) = span_ctx.extract_sampled_trace_id();
    let mut kvs: Vec<(&str, &str)> = Vec::new();
    if sampled {
        kvs.push(("trace_id", &trace_id));
    } else if !trace_id.is_empty() {
        kvs.push(("trace_id_unsampled", &trace_id));
    }
    if let Some(ips) = source_ips.filter(|s| !s.is_empty()) {
        kvs.push(("sourceIPs", ips));
    }
    kvs.push(("msg", &msg));
    cfg.logger.log(level, &kvs);
}

fn check_cluster(
    req: &Request,
    cv: &HttpClusterValidation,
    route: Option<&str>,
    cfg: &HttpMiddlewareConfig,
    span_ctx: &TraceContext,
) -> Option<Response> {
    let path = req.uri().path();
    if cv.excluded_path.is_match(path) {
        return None;
    }
    let get = |n: &str| req.headers().get(n).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    if let Some(re) = &cv.excluded_user_agent {
        if re.is_match(&get("User-Agent")) {
            return None;
        }
    }
    let route = route.unwrap_or("<unknown-route>").to_string();
    let values: Vec<String> = req
        .headers()
        .get_all("X-Cluster")
        .iter()
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .collect();
    let expected = format!("[{}]", cv.allowed.join(" "));
    let first = cv.allowed.first().cloned().unwrap_or_default();
    let tenant = get("X-Scope-OrgID");
    let ua = get("User-Agent");
    let host = req.uri().host().map(|h| h.to_string()).unwrap_or_else(|| get("Host"));
    let client_addr = req.extensions().get::<ServedOverHttp>().map(|s| s.remote_addr.to_string()).unwrap_or_default();
    let (trace_id, trace_sampled) = span_ctx.extract_sampled_trace_id();
    let log = |extra: Vec<(&str, String)>| {
        let mut kvs: Vec<(&str, LogValue<'_>)> = vec![
            ("path", LogValue::from(path)),
            ("method", LogValue::from(req.method().as_str())),
            ("cluster_validation_labels", LogValue::from(expected.as_str())),
            ("soft_validation", LogValue::Bool(cv.soft)),
            ("tenant", LogValue::from(tenant.as_str())),
            ("user_agent", LogValue::from(ua.as_str())),
            ("host", LogValue::from(host.as_str())),
            ("client_address", LogValue::from(client_addr.as_str())),
        ];
        if trace_sampled {
            kvs.push(("trace_id", LogValue::from(trace_id.as_str())));
        }
        for (k, v) in &extra {
            kvs.push((k, LogValue::from(v.as_str())));
        }
        cfg.logger.log_values(LogLevel::Warn, &kvs);
    };
    let err = if values.len() > 1 {
        cfg.metrics.invalid_cluster_requests.with_label_values(&["http", &route, &first, ""]).inc();
        let e = format!(
            "request header should contain exactly 1 value for key \"X-Cluster\", but it contains [{}]",
            values.join(" ")
        );
        log(vec![("msg", "detected error during cluster validation label extraction".into()), ("err", e.clone())]);
        format!("rejected request: {e}")
    } else {
        match values.first().filter(|v| !v.is_empty()) {
            Some(c) if cv.allowed.iter().any(|a| a == c) => return None,
            Some(c) => {
                cfg.metrics.invalid_cluster_requests.with_label_values(&["http", &route, &first, c]).inc();
                log(vec![
                    ("msg", "request with wrong cluster validation label".into()),
                    ("request_cluster_validation_label", c.clone()),
                ]);
                format!("rejected request with wrong cluster validation label {c:?} - it should be one of {expected}")
            }
            None => {
                cfg.metrics.invalid_cluster_requests.with_label_values(&["http", &route, &first, ""]).inc();
                log(vec![("msg", "request with no cluster validation label".into())]);
                format!("rejected request with empty cluster validation label - it should be one of {expected}")
            }
        }
    };
    if cv.soft {
        return None;
    }
    let body = serde_json::json!({"cluster_validation_error_message": err, "route": route}).to_string();
    Some((StatusCode::NETWORK_AUTHENTICATION_REQUIRED, [(header::CONTENT_TYPE, "application/json")], body).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_ips_like_dskit() {
        let s = SourceIps::new("", "", false).unwrap();
        let mut h = HeaderMap::new();
        assert_eq!(s.get(&h, "1.2.3.4:5678"), "1.2.3.4");
        h.insert("X-Forwarded-For", "10.0.0.1, 10.0.0.2".parse().unwrap());
        assert_eq!(s.get(&h, "1.2.3.4:5678"), "10.0.0.1, 1.2.3.4");
        let all = SourceIps::new("", "", true).unwrap();
        assert_eq!(all.get(&h, "1.2.3.4:5678"), "10.0.0.1, 10.0.0.2, 1.2.3.4");
        assert!(SourceIps::new("X", "", false).is_err());
    }

    #[tokio::test]
    async fn w23_csc_11_method_mismatch_resolves_other_before_inflight_cluster_and_otel() {
        use axum::routing::{on, MethodFilter};
        use axum::Router;
        use http_body_util::BodyExt as _;
        use prometheus::Registry;
        use tower::ServiceExt as _;

        #[derive(Default)]
        struct NoopLogger;
        impl Logger for NoopLogger {
            fn log(&self, _level: LogLevel, _fields: &[(&str, &str)]) {}
        }

        static SPANS: std::sync::OnceLock<Arc<Mutex<Vec<ExportedSpan>>>> = std::sync::OnceLock::new();
        let spans = SPANS.get_or_init(|| Arc::new(Mutex::new(Vec::new())));
        let prev_exp = std::env::var("OTEL_TRACES_EXPORTER").ok();
        std::env::set_var("OTEL_TRACES_EXPORTER", "otlp");
        let sink = Arc::clone(spans);
        let _ = mimir_common::tracing::init_otel_or_jaeger_from_env(
            "mimir-test",
            &NoopLogger,
            Some(Arc::new(move |s: ExportedSpan| {
                sink.lock().push(s);
            })),
        );
        match prev_exp {
            Some(v) => std::env::set_var("OTEL_TRACES_EXPORTER", v),
            None => std::env::remove_var("OTEL_TRACES_EXPORTER"),
        }
        spans.lock().clear();

        let reg = Registry::new();
        let metrics = ServerMetrics::new(&reg, false, false).unwrap();
        let cv = HttpClusterValidation::new(vec!["prod".to_string()], false, &[], &[]).unwrap();
        let cfg = Arc::new(HttpMiddlewareConfig {
            metrics,
            logger: Arc::new(NoopLogger),
            log_request_headers: false,
            log_request_at_info_level: false,
            log_source_ips: None,
            trace_source_ips: None,
            exclude_headers: Vec::new(),
            trace_request_headers: false,
            trace_exclude_headers: Vec::new(),
            create_new_traces: true,
            cluster_validation: Some(cv),
            http_path_prefix: String::new(),
            cost_attribution_path: None,
        });

        let inflight_seen = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let inflight_seen_cb = Arc::clone(&inflight_seen);
        let reg_cb = reg.clone();

        let app = Router::new()
            .route("/config", on(MethodFilter::GET, || async { "ok" }))
            .route("/ingester/tenants", {
                let reg_cb = reg_cb.clone();
                let inflight_seen_cb = Arc::clone(&inflight_seen_cb);
                on(MethodFilter::GET, || async { "ok" }).head(move || {
                    let reg_cb = reg_cb.clone();
                    let inflight_seen_cb = Arc::clone(&inflight_seen_cb);
                    async move {
                        for mf in reg_cb.gather() {
                            if mf.get_name() == "cortex_inflight_requests" {
                                for m in mf.get_metric() {
                                    if m.get_gauge().get_value() > 0.0 {
                                        let method = m.get_label().iter().find(|l| l.get_name() == "method").map(|l| l.get_value().to_string()).unwrap_or_default();
                                        let route = m.get_label().iter().find(|l| l.get_name() == "route").map(|l| l.get_value().to_string()).unwrap_or_default();
                                        inflight_seen_cb.lock().push((method, route));
                                    }
                                }
                            }
                        }
                        method_not_allowed().await
                    }
                })
            })
            .method_not_allowed_fallback(method_not_allowed)
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&cfg),
                dskit_http_middleware,
            ));

        let mut req1 = Request::builder()
            .method(http::Method::HEAD)
            .uri("/ingester/tenants")
            .header("X-Cluster", "prod")
            .header("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
            .body(Body::empty())
            .unwrap();
        req1.extensions_mut().insert(ServedOverHttp {
            remote_addr: "127.0.0.1:12345".parse().unwrap(),
        });
        let resp1 = app.clone().oneshot(req1).await.unwrap();
        assert_eq!(resp1.status(), StatusCode::METHOD_NOT_ALLOWED);
        let _ = resp1.into_body().collect().await.unwrap();
        assert_eq!(*inflight_seen.lock(), vec![("HEAD".to_string(), "other".to_string())]);

        let mut req2 = Request::builder()
            .method(http::Method::DELETE)
            .uri("/config")
            .header("traceparent", "00-33333333333333333333333333333333-4444444444444444-01")
            .body(Body::empty())
            .unwrap();
        req2.extensions_mut().insert(ServedOverHttp {
            remote_addr: "127.0.0.1:12345".parse().unwrap(),
        });
        let resp2 = app.clone().oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::NETWORK_AUTHENTICATION_REQUIRED);
        let body2 = String::from_utf8(resp2.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        let json2: serde_json::Value = serde_json::from_str(&body2).unwrap();
        assert_eq!(json2["route"], "<unknown-route>");

        for mf in reg.gather() {
            for m in mf.get_metric() {
                for lp in m.get_label() {
                    if lp.get_name() == "route" {
                        assert!(
                            lp.get_value() == "other" || lp.get_value() == "<unknown-route>",
                            "unexpected route label {} in {}",
                            lp.get_value(),
                            mf.get_name()
                        );
                    }
                }
            }
        }


    }
}
