//! HTTP admin & lifecycle router matching `pkg/api/api.go`, `pkg/mimir/mimir.go`,
//! `pkg/mimir/status.go`, `pkg/mimir/runtime_config.go`, and `pkg/util/version/info_handler.go`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Path as AxumPath, RawQuery, State as AxumState};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{any, on, MethodFilter, MethodRouter};
use axum::{Json, Router};
use mimir_common::services::State;
use mimir_common::tenant::{
    extract_org_id_bytes_from_http_headers, parse_single_tenant_id_bytes, parse_tenant_ids_bytes,
};
use mimir_config::{yaml_v3_emit, LimitsConfig, MimirConfig, Overrides, RuntimeConfigManager};
use mimir_ingester::{ingester_router, Ingester};
use mimir_ring::kv::memberlist::gofmt::{format_go_time_string, format_rfc3339_nano};
use mimir_ring::kv::memberlist::{http_status, MemberlistKv};
use prometheus::Registry;
use serde::Serialize;
use serde_yaml::{Mapping, Value};

use crate::cli::{diff_yaml_values, effective_config_yaml, marshal_config_yaml};
use crate::memberlist_status::render_memberlist_status_page;
use crate::metrics::{branch_str, go_version_str, revision_str, version_str};
use crate::modules::ModuleStates;

#[derive(Clone)]
pub struct AppHttpState {
    pub config: Arc<MimirConfig>,
    pub default_config: Arc<MimirConfig>,
    pub registry: Registry,
    pub shutdown_requested: Arc<AtomicBool>,
    pub ingester: Option<Arc<Ingester>>,
    pub runtime_config: Option<Arc<RuntimeConfigManager>>,
    pub overrides: Overrides,
    pub memberlist_kv: Option<Arc<MemberlistKv>>,
    /// Module services of the configured targets (Go `Mimir.ServiceMap`) and their states.
    pub modules: Arc<ModuleStates>,
    /// `(cost_attribution_registry_path, registry)` when cost attribution is enabled
    /// (Go: `API.RegisterCostAttribution`).
    pub cost_attribution_registry: Option<(String, Registry)>,
}

/// Exact `GET`-only route matching Gorilla `mux` `Methods("GET")` (rejecting `HEAD` with 405).
fn get_only<H, T, S>(handler: H) -> MethodRouter<S>
where
    H: axum::handler::Handler<T, S>,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    on(MethodFilter::GET, handler).head(crate::server::http_middleware::method_not_allowed)
}

async fn reject_head_on_gorilla_mux_routes(
    AxumState(prefix): AxumState<String>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if req.method() == axum::http::Method::HEAD {
        let matched = req
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .map(|p| {
                let s = p.as_str();
                if prefix.is_empty() {
                    s
                } else {
                    s.strip_prefix(&prefix).unwrap_or(s)
                }
            });
        let allow_head = matches!(
            matched,
            Some("/ready" | "/metrics" | "/debug/pprof" | "/debug/pprof/")
        ) || matched.is_some_and(|p| p.starts_with("/debug/pprof/"));
        if !allow_head {
            return crate::server::http_middleware::method_not_allowed().await;
        }
    }
    next.run(req).await
}

/// Builds the full Axum HTTP router (with `server.http_path_prefix` honored).
pub fn build_http_router(state: AppHttpState) -> Router {
    let register_instrumentation = state.config.server.register_instrumentation;
    // Go registers /memberlist whenever the memberlist-kv module is initialised (a dependency of
    // the ingester ring); it answers "This instance doesn't use memberlist." without a KV.
    let has_memberlist = state.memberlist_kv.is_some() || state.ingester.is_some();
    let ingester_opt = state.ingester.clone();
    let raw_prefix = state.config.server.http_path_prefix.as_str();
    let shutdown_requested = Arc::clone(&state.shutdown_requested);

    // gorilla/mux `PathPrefix` errors when a non-empty prefix lacks a leading '/' ("mux: path
    // must start with a slash"), leaving `subrouter.routeConf.regexp.path == nil`; dskit mounts
    // `subrouter` directly as the HTTP handler, so routes end up mounted unprefixed at '/'
    // (and `/<prefix>/ready` is 404).
    let path_prefix = if !raw_prefix.starts_with('/') {
        String::new()
    } else {
        raw_prefix.trim_end_matches('/').to_string()
    };
    let has_prefix = !path_prefix.is_empty();

    // `/ready` and `/metrics` are registered without a method restriction (Go
    // `Server.HTTP.Path("/ready")`, dskit `router.Handle("/metrics", ...)`).
    let mut router = Router::new()
        .route("/ready", any(ready_handler))
        .route("/services", get_only(services_handler))
        .route("/config", get_only(config_handler))
        .route("/api/v1/status/buildinfo", get_only(buildinfo_handler))
        .route("/api/v1/status/config", get_only(status_config_handler))
        .route("/api/v1/status/flags", get_only(status_flags_handler))
        .route("/debug/fgprof", get_only(crate::pprof::fgprof_handler))
        .route("/static/", get_only(static_dir_handler))
        .route("/static/*path", get_only(static_file_handler));

    // Go `initRuntimeConfig` only registers /runtime_config and /api/v1/user_limits when
    // -runtime-config.file is configured (`pkg/mimir/modules.go:460-475`).
    if state.runtime_config.is_some() {
        router = router
            .route("/runtime_config", get_only(runtime_config_handler))
            .route("/api/v1/user_limits", get_only(user_limits_handler));
    }

    if !has_prefix {
        router = router.route("/", get_only(index_handler));
    }

    if register_instrumentation {
        // dskit `registerInstrumentation` registers `/metrics`, `/debug/pprof/cmdline`
        // (NotFound) and `PathPrefix("/debug/pprof").Handler(http.DefaultServeMux)`. Because
        // `DefaultServeMux` only knows `/debug/pprof*` (without `-server.path-prefix`), under
        // a non-empty prefix Go matches the prefixed route (`route="<prefix>_debug_pprof"`)
        // and returns 404.
        if has_prefix {
            router = router
                .route("/debug/pprof/cmdline", any(not_found_handler))
                .route("/debug/pprof", any(not_found_handler))
                .route("/debug/pprof/", any(not_found_handler))
                .route("/debug/pprof/*name", any(not_found_handler));
        } else {
            router = router
                .route("/debug/pprof/cmdline", any(not_found_handler))
                .route("/debug/pprof/profile", any(crate::pprof::profile_handler))
                .route("/debug/pprof/symbol", any(crate::pprof::symbol_handler))
                .route("/debug/pprof", any(crate::pprof::index_redirect_handler))
                .route("/debug/pprof/", any(crate::pprof::index_handler))
                .route("/debug/pprof/*name", any(crate::pprof::unknown_profile_handler));
        }

        let cache = Arc::new(MetricsCache::new(
            state
                .config
                .server
                .metrics_cache_duration
                .as_std()
                .unwrap_or_default(),
        ));
        router = router.route(
            "/metrics",
            any(move |st: AxumState<AppHttpState>, RawQuery(raw_query): RawQuery, headers: HeaderMap| {
                metrics_handler(st, RawQuery(raw_query), headers, Arc::clone(&cache))
            }),
        );
    }
    if has_memberlist {
        router = router.route("/memberlist", get_only(memberlist_handler));
    }
    let mut cost_attribution_path: Option<String> = None;
    if let Some((path, reg)) = state.cost_attribution_registry.clone() {
        let path = if path.starts_with('/') { path } else { format!("/{path}") };
        cost_attribution_path = Some(path.clone());
        router = router.route(
            &path,
            get_only(move |RawQuery(raw_query): RawQuery, headers: HeaderMap| {
                let reg = reg.clone();
                async move {
                    let filter = parse_name_filter(raw_query.as_deref());
                    encode_families(&reg.gather(), &headers, false, None, filter.as_ref())
                }
            }),
        );
    }

    let state_for_root = state.clone();
    let mut app = router.with_state(state);

    if let Some(ing) = ingester_opt {
        app = app.merge(ingester_router(ing));
    }

    // pkg/api/api.go newRoute: `gziphandler.GzipHandler` on every gzip-enabled route.
    let gzip = crate::server::gzip::GzipConfig {
        level: state_for_root.config.api_response_compression_level().unwrap_or(-1),
        cost_attribution_path,
    };
    app = app
        .route_layer(axum::middleware::from_fn_with_state(
            path_prefix.clone(),
            reject_head_on_gorilla_mux_routes,
        ))
        .route_layer(axum::middleware::from_fn_with_state(gzip.clone(), crate::server::gzip::gzip_handler))
        // gorilla/mux answers a known path with an unsupported method with an empty 405 and
        // reports it as an unmatched route.
        .method_not_allowed_fallback(crate::server::http_middleware::method_not_allowed)
        .fallback(not_found_handler);

    let router = if has_prefix {
        Router::new()
            .route(
                &format!("{path_prefix}/"),
                get_only(index_handler)
                    .with_state(state_for_root)
                    .route_layer(axum::middleware::from_fn_with_state(
                        path_prefix.clone(),
                        reject_head_on_gorilla_mux_routes,
                    ))
                    .route_layer(axum::middleware::from_fn_with_state(
                        gzip,
                        crate::server::gzip::gzip_handler,
                    )),
            )
            .nest(&path_prefix, app)
            .method_not_allowed_fallback(prefixed_not_found_method_mismatch)
            .fallback(not_found_handler)
    } else {
        app
    };

    // Go `gorilla/mux` `ServeHTTP` (`cleanPath`) redirects non-canonical paths with 301
    // before routing. (`Connection: close` on shutdown belongs to the HTTP connection writer
    // only, not the shared router used by `HttpGrpcBridge`, and Axum's top-level `Allow` on
    // `MethodMismatch` 405s is stripped after `RouteFuture` in `http_server.rs` and `grpc.rs`.)
    let _ = shutdown_requested;
    router.layer(axum::middleware::from_fn(clean_path_middleware))
}

/// `http.NotFoundHandler()` (`http.Error(w, "404 page not found", 404)`).
pub async fn not_found_handler() -> Response {
    (
        StatusCode::NOT_FOUND,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        "404 page not found\n",
    )
        .into_response()
}

/// Under a prefixed gorilla/mux subrouter, a method mismatch on the subrouter's `/` route
/// (e.g. `POST /mimir/`) gets cleared by subsequent routes' `PathPrefix` matcher and falls
/// through to 404 (`route="other"`).
async fn prefixed_not_found_method_mismatch() -> Response {
    let mut resp = not_found_handler().await;
    resp.extensions_mut()
        .insert(crate::server::http_middleware::MethodMismatch);
    resp
}

async fn ready_handler(AxumState(st): AxumState<AppHttpState>) -> Response {
    if st.shutdown_requested.load(Ordering::Acquire) {
        return go_http_error(StatusCode::SERVICE_UNAVAILABLE, "Application is stopping");
    }

    // Go: `if !sm.IsHealthy()` lists every module service that is not Running, one
    // "<module>: <state>" per line (pkg/mimir/mimir.go readyHandler).
    let not_running = st.modules.not_running();
    if !not_running.is_empty() {
        return go_http_error(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!("Some services are not Running:\n{}", not_running.join("\n")),
        );
    }

    if let Some(ref ing) = st.ingester {
        if let Err(err) = ing.check_ready().await {
            return go_http_error(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("Ingester not ready: {err}"),
            );
        }
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain")],
        "ready",
    )
        .into_response()
}

/// dskit `registerInstrumentation`: `promhttp.HandlerFor(gatherer, HandlerOpts{EnableOpenMetrics:
/// true})` (wrapped by `metrics.CachedHandlerFor` when `-server.metrics-cache-duration` > 0):
/// content negotiation (OpenMetrics 1.0.0/0.0.1, delimited protobuf, text 0.0.4), `Vary` headers
/// and gzip response compression.
#[derive(Clone, Debug)]
pub(crate) struct PendingCleanPathRedirect(pub String);

#[derive(Clone, Copy, Debug)]
pub(crate) struct CleanPathHandled;

/// Marker extension on responses whose handler explicitly sets `Content-Length`
/// (e.g. `http.FileServer` for `/static/` files), so `HttpGrpcBridge` preserves it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExplicitContentLength;

/// Matches Go `gorilla/mux` `cleanPath` (POSIX `path.Clean` preserving trailing `/`).
fn gorilla_mux_clean_path(p: &[u8]) -> Vec<u8> {
    if p.is_empty() {
        return b"/".to_vec();
    }
    let prefixed: std::borrow::Cow<'_, [u8]> = if p[0] == b'/' {
        std::borrow::Cow::Borrowed(p)
    } else {
        let mut v = Vec::with_capacity(p.len() + 1);
        v.push(b'/');
        v.extend_from_slice(p);
        std::borrow::Cow::Owned(v)
    };
    let mut stack: Vec<&[u8]> = Vec::new();
    for seg in prefixed.split(|&b| b == b'/') {
        match seg {
            b"" | b"." => {}
            b".." => {
                stack.pop();
            }
            other => stack.push(other),
        }
    }
    let mut np = Vec::with_capacity(prefixed.len());
    for seg in &stack {
        np.push(b'/');
        np.extend_from_slice(seg);
    }
    // `path.Clean` drops the trailing slash except for the root: put it back.
    if np.is_empty() || prefixed.ends_with(b"/") {
        np.push(b'/');
    }
    np
}

/// Go `net/url` `unescape(s, encodePath)`: decodes `%XX`, leaving anything else (including `+`
/// and malformed escapes, which Go rejects before routing) untouched.
fn go_path_unescape(raw: &str) -> Vec<u8> {
    let hex = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    };
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// Go `net/url` `shouldEscape(c, encodePath)`: everything but unreserved characters and
/// `$&+,/:;=@` is escaped.
fn go_should_escape_path(b: u8) -> bool {
    !(b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'_' | b'.' | b'~' | b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@'
        ))
}

/// Go `net/url` `escape(s, encodePath)` (upper-case hex digits).
fn go_path_escape(path: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(path.len());
    for &b in path {
        if go_should_escape_path(b) {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        } else {
            out.push(b as char);
        }
    }
    out
}

/// Go `net/url` `validEncoded(s, encodePath)`.
fn go_valid_encoded_path(s: &str) -> bool {
    s.bytes().all(|b| {
        matches!(
            b,
            b'!' | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
                | b'['
                | b']'
                | b'%'
        ) || !go_should_escape_path(b)
    })
}

/// Go `URL.EscapedPath()` for a URL whose `Path` is `path` and whose `RawPath` is `raw_path`.
fn go_escaped_path(path: &[u8], raw_path: &str) -> String {
    if !raw_path.is_empty() && go_valid_encoded_path(raw_path) && go_path_unescape(raw_path) == path
    {
        return raw_path.to_string();
    }
    if path == b"*" {
        return "*".to_string();
    }
    go_path_escape(path)
}

/// `gorilla/mux` `Router.ServeHTTP` with `UseEncodedPath()` (pkg/api/api.go): `cleanPath` runs on
/// `req.URL.EscapedPath()`, and a non-canonical path is answered with a 301 whose `Location` is
/// `url.Path = p; url.String()`. Because `p` is the cleaned *escaped* path, Go escapes it once
/// more (`//a%20b` redirects to `/a%2520b`). Returns that `Location` path for `p` (the path as
/// found in the URL, `URL.setPath(p)`), or `None` when the path is canonical.
pub(crate) fn gorilla_mux_clean_path_redirect(p: &str) -> Option<String> {
    let path = go_path_unescape(p);
    let raw_path = if go_path_escape(&path) == p { "" } else { p };
    let escaped = go_escaped_path(&path, raw_path);
    let cleaned = gorilla_mux_clean_path(escaped.as_bytes());
    (cleaned != escaped.as_bytes()).then(|| go_escaped_path(&cleaned, raw_path))
}

pub(crate) fn clean_path_redirect_response(loc: &str) -> Response {
    let mut resp = Response::new(axum::body::Body::empty());
    *resp.status_mut() = StatusCode::MOVED_PERMANENTLY;
    if let Ok(val) = HeaderValue::from_bytes(loc.as_bytes()) {
        resp.headers_mut().insert(header::LOCATION, val);
    }
    resp.extensions_mut().insert(CleanPathHandled);
    resp
}

async fn clean_path_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if let Some(PendingCleanPathRedirect(loc)) = req.extensions().get::<PendingCleanPathRedirect>() {
        return clean_path_redirect_response(loc);
    }
    let uri = req.uri();
    if let Some(cleaned) = gorilla_mux_clean_path_redirect(uri.path()) {
        let mut loc = String::new();
        if let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) {
            loc.push_str(scheme);
            loc.push_str("://");
            loc.push_str(authority.as_str());
        }
        loc.push_str(&cleaned);
        if let Some(q) = uri.query() {
            loc.push('?');
            loc.push_str(q);
        }
        return clean_path_redirect_response(&loc);
    }
    next.run(req).await
}

fn percent_decode_form(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = |b: u8| match b {
                    b'0'..=b'9' => Some(b - b'0'),
                    b'a'..=b'f' => Some(b - b'a' + 10),
                    b'A'..=b'F' => Some(b - b'A' + 10),
                    _ => None,
                };
                if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    out.push((hi << 4) | lo);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_name_filter(raw_query: Option<&str>) -> Option<std::collections::HashSet<String>> {
    let q = raw_query?;
    let mut names = std::collections::HashSet::new();
    for part in q.split('&') {
        if part.is_empty() {
            continue;
        }
        let (raw_k, raw_v) = match part.split_once('=') {
            Some((k, v)) => (k, v),
            None => (part, ""),
        };
        if percent_decode_form(raw_k) == "name[]" {
            names.insert(percent_decode_form(raw_v));
        }
    }
    (!names.is_empty()).then_some(names)
}

fn join_header_values(headers: &HeaderMap, name: &header::HeaderName) -> Option<String> {
    let mut joined = String::new();
    for v in headers.get_all(name) {
        if let Ok(s) = std::str::from_utf8(v.as_bytes()) {
            if !joined.is_empty() {
                joined.push(',');
            }
            joined.push_str(s);
        }
    }
    (!joined.is_empty()).then_some(joined)
}

async fn metrics_handler(
    AxumState(st): AxumState<AppHttpState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    cache: Arc<MetricsCache>,
) -> Response {
    tokio::task::spawn_blocking(move || {
        let families = cache.gather(&st.registry);
        let filter = parse_name_filter(raw_query.as_deref());
        let created = st
            .config
            .server
            .enable_open_metrics_text_created_samples
            .then_some(cache.as_ref());
        encode_families(&families, &headers, true, created, filter.as_ref())
    })
    .await
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn encode_families(
    families: &[prometheus::proto::MetricFamily],
    headers: &HeaderMap,
    enable_open_metrics: bool,
    created_tracker: Option<&MetricsCache>,
    name_filter: Option<&std::collections::HashSet<String>>,
) -> Response {
    let filtered: std::borrow::Cow<'_, [prometheus::proto::MetricFamily]> = match name_filter {
        Some(filter) => std::borrow::Cow::Owned(
            families
                .iter()
                .filter(|mf| filter.contains(mf.get_name()))
                .cloned()
                .collect(),
        ),
        None => std::borrow::Cow::Borrowed(families),
    };
    let accept = join_header_values(headers, &header::ACCEPT);
    let format = crate::expfmt::negotiate_with_options(accept.as_deref(), enable_open_metrics);
    let encoded = if format.kind == crate::expfmt::FormatType::OpenMetrics {
        if let Some(cache) = created_tracker {
            Ok(encode_openmetrics_with_created(&filtered, cache, format.escaping_scheme()))
        } else {
            crate::expfmt::encode(&filtered, &format)
        }
    } else {
        crate::expfmt::encode(&filtered, &format)
    };
    let body = match encoded {
        Ok(b) => b,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                format!("An error has occurred while serving metrics:\n\n{err}\n"),
            )
                .into_response();
        }
    };
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::VARY, "Accept")
        .header(header::VARY, "Accept-Encoding")
        .header(header::CONTENT_TYPE, format.content_type);
    let accept_encoding = join_header_values(headers, &header::ACCEPT_ENCODING);
    let body = match crate::expfmt::negotiate_content_encoding(accept_encoding.as_deref()) {
        Some("gzip") => {
            builder = builder.header(header::CONTENT_ENCODING, "gzip");
            gzip_bytes(&body)
        }
        // Go fails to build a writer for an unacceptable encoding and falls back to identity.
        _ => body,
    };
    builder
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

pub(crate) fn gzip_bytes(data: &[u8]) -> Vec<u8> {
    use std::io::Write as _;
    let mut enc = flate2::write::GzEncoder::new(
        Vec::with_capacity(data.len() / 4 + 64),
        flate2::Compression::default(),
    );
    let _ = enc.write_all(data);
    enc.finish().unwrap_or_default()
}

/// dskit `metrics.CachedHandlerFor`: reuses the gathered metric families for
/// `-server.metrics-cache-duration` (0 disables caching).
pub struct MetricsCache {
    duration: std::time::Duration,
    cached: parking_lot::Mutex<Option<(std::time::Instant, Arc<Vec<prometheus::proto::MetricFamily>>)>>,
    created_timestamps: parking_lot::Mutex<std::collections::HashMap<String, f64>>,
}

impl MetricsCache {
    pub fn new(duration: std::time::Duration) -> Self {
        Self {
            duration,
            cached: parking_lot::Mutex::new(None),
            created_timestamps: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn created_ts(&self, key: &str, now_secs: f64) -> f64 {
        let mut map = self.created_timestamps.lock();
        *map.entry(key.to_string()).or_insert(now_secs)
    }

    fn gather(&self, registry: &Registry) -> Arc<Vec<prometheus::proto::MetricFamily>> {
        if self.duration.is_zero() {
            return Arc::new(registry.gather());
        }
        let mut cached = self.cached.lock();
        if let Some((at, ref fams)) = *cached {
            if at.elapsed() < self.duration {
                return Arc::clone(fams);
            }
        }
        let fams = Arc::new(registry.gather());
        *cached = Some((std::time::Instant::now(), Arc::clone(&fams)));
        fams
    }
}

#[derive(Serialize)]
struct RenderService {
    name: String,
    status: String,
}

#[derive(Serialize)]
struct StatusPageContents {
    now: String,
    services: Vec<RenderService>,
}

async fn services_handler(AxumState(st): AxumState<AppHttpState>, headers: HeaderMap) -> Response {
    // `servicesHandler`: every module service of `Mimir.ServiceMap` with its current state. The
    // service manager starts every module at once (each waits for its dependencies while
    // Starting), so a module not started yet is reported as Starting.
    let svcs: Vec<RenderService> = st
        .modules
        .snapshot()
        .into_iter()
        .map(|(name, state)| RenderService {
            name,
            status: if state == State::New { State::Starting } else { state }.to_string(),
        })
        .collect();

    let now_local = chrono::Local::now().fixed_offset();
    let accepts_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("application/json"));
    if accepts_json {
        let now = format_rfc3339_nano(&now_local);
        return Json(StatusPageContents { now, services: svcs }).into_response();
    }

    let now_html = html_escape(&format_go_time_string(&now_local));
    let mut html = format!(
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>Services' status</title>\n</head>\n<body>\n<h1>Services' status</h1>\n<p>Current time: {now_html}</p>\n<table border=\"1\">\n    <thead>\n    <tr>\n        <th>Service</th>\n        <th>Status</th>\n    </tr>\n    </thead>\n    <tbody>\n"
    );
    for s in svcs {
        html.push_str(&format!(
            "    \n        <tr>\n            <td>{}</td>\n            <td>{}</td>\n        </tr>\n    \n",
            html_escape(&s.name),
            html_escape(&s.status)
        ));
    }
    html.push_str("    </tbody>\n</table>\n</body>\n</html>");
    Html(html).into_response()
}

fn decode_query_component(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// First `mode` query parameter (`r.URL.Query().Get("mode")`), tolerating duplicates.
fn extract_mode_query(raw: Option<&str>) -> Option<String> {
    raw?
        .split(['&', ';'])
        .filter(|p| !p.is_empty())
        .filter_map(|kv| kv.split_once('=').or(Some((kv, ""))))
        .find(|(k, _)| decode_query_component(k) == "mode")
        .map(|(_, v)| decode_query_component(v))
}

async fn config_handler(
    AxumState(st): AxumState<AppHttpState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let mode = extract_mode_query(raw_query.as_deref());
    // Go `ConfigHandler` + `util.WriteYAMLResponse` (yaml.v3 `Marshal`): the config structs keep
    // their field order, the diff is a `map[string]interface{}` and therefore sorted.
    let yaml = match mode.as_deref() {
        Some("diff") => {
            let mut def = effective_config_yaml(&st.default_config);
            let mut act = effective_config_yaml(&st.config);
            mimir_config::yaml_v3::order_like_go_config(&mut def)
                .and_then(|()| mimir_config::yaml_v3::order_like_go_config(&mut act))
                .and_then(|()| {
                    let diff = diff_yaml_values(&def, &act)
                        .unwrap_or_else(|| Value::Mapping(Mapping::new()));
                    yaml_v3_emit::marshal_generic(&diff)
                })
        }
        Some("defaults") => marshal_config_yaml(&st.default_config),
        _ => marshal_config_yaml(&st.config),
    };

    match yaml {
        Ok(yaml) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            yaml,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn runtime_config_handler(
    AxumState(st): AxumState<AppHttpState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let mode = extract_mode_query(raw_query.as_deref());
    let Some(ref mgr) = st.runtime_config else {
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain")],
            "runtime config file doesn't exist",
        )
            .into_response();
    };

    let cfg = mgr.get_config();
    let actual_val = runtime_config_to_yaml(&cfg);

    // Go `runtimeConfigHandler`: the `runtimeConfigValues` struct, or the
    // `map[string]interface{}` returned by `util.DiffConfig` (sorted) in diff mode.
    let yaml = match mode.as_deref() {
        Some("diff") => {
            let mut default_overrides = Mapping::new();
            let mut default_limit_val =
                serde_yaml::to_value(&st.config.limits).unwrap_or(Value::Mapping(Mapping::new()));
            let _ = mimir_config::yaml_v3::order_like_go_type(
                "validation.Limits",
                &mut default_limit_val,
            );
            // Go only seeds defaults for non-nil tenant limits, so `t: null` shows in the diff.
            for (tenant, limits) in &cfg.overrides {
                if limits.is_some() {
                    default_overrides
                        .insert(Value::String(tenant.clone()), default_limit_val.clone());
                }
            }
            let mut default_root = Mapping::new();
            default_root.insert(
                Value::String("overrides".to_string()),
                Value::Mapping(default_overrides),
            );
            default_root.insert(
                Value::String("multi_kv_config".to_string()),
                serde_yaml::to_value(mimir_config::MultiRuntimeConfig::default())
                    .unwrap_or(Value::Mapping(Mapping::new())),
            );
            // Go's default `runtimeConfigValues` has nil instance limits pointers.
            default_root.insert(Value::String("ingester_limits".to_string()), Value::Null);
            default_root.insert(Value::String("distributor_limits".to_string()), Value::Null);
            let diff = diff_yaml_values(&Value::Mapping(default_root), &actual_val)
                .unwrap_or_else(|| Value::Mapping(Mapping::new()));
            yaml_v3_emit::marshal_generic(&diff)
        }
        _ => yaml_v3_emit::marshal_struct(&actual_val),
    };

    match yaml {
        Ok(yaml) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            yaml,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

/// Go `runtimeConfigValues` as a `Value` in field order: `overrides` is a Go map (emitted sorted
/// by yaml.v3), nil `ingester_limits` / `distributor_limits` pointers are `null`.
fn runtime_config_to_yaml(cfg: &mimir_config::RuntimeConfigValues) -> Value {
    let mut root = Mapping::new();
    let mut overrides_map = Mapping::new();
    for (k, v) in &cfg.overrides {
        // A nil `*validation.Limits` marshals as `null`.
        let val = match v {
            Some(limits) => serde_yaml::to_value(limits.as_ref()).map(|mut val| {
                // Only fails for a broken embedded type graph; the value is then kept as is.
                let _ = mimir_config::yaml_v3::order_like_go_type("validation.Limits", &mut val);
                val
            }),
            None => Ok(Value::Null),
        };
        if let Ok(val) = val {
            overrides_map.insert(Value::String(k.clone()), val);
        }
    }
    // Sorting can only fail for sequence / mapping keys, which tenant IDs never are.
    let _ = yaml_v3_emit::sort_mapping_like_go(&mut overrides_map);
    root.insert(
        Value::String("overrides".to_string()),
        Value::Mapping(overrides_map),
    );
    if let Ok(multi_val) = serde_yaml::to_value(&cfg.multi_kv_config) {
        root.insert(Value::String("multi_kv_config".to_string()), multi_val);
    }
    // Nil `*ingester.InstanceLimits` / `*distributor.InstanceLimits` (no `omitempty`) marshal
    // as `null`.
    let ingester_limits = cfg
        .ingester_limits
        .as_ref()
        .and_then(|ing| serde_yaml::to_value(ing).ok())
        .unwrap_or(Value::Null);
    root.insert(Value::String("ingester_limits".to_string()), ingester_limits);
    let distributor_limits = cfg
        .distributor_limits
        .as_ref()
        .and_then(|dist| serde_yaml::to_value(dist).ok())
        .unwrap_or(Value::Null);
    root.insert(Value::String("distributor_limits".to_string()), distributor_limits);
    Value::Mapping(root)
}

#[derive(Serialize)]
struct UserLimitsResponse {
    compactor_blocks_retention_period_seconds: i64,
    ingestion_rate: f64,
    ingestion_burst_size: i64,
    ingestion_burst_factor: f64,
    max_global_series_per_user: i64,
    max_global_series_per_metric: i64,
    max_global_exemplars_per_user: i64,
    max_fetched_chunks_per_query: i64,
    max_fetched_series_per_query: i64,
    max_fetched_chunk_bytes_per_query: i64,
    ruler_max_rules_per_rule_group: i64,
    ruler_max_rule_groups_per_tenant: i64,
    alertmanager_notification_rate_limit: f64,
    alertmanager_max_dispatcher_aggregation_groups: i64,
    alertmanager_max_templates_count: i64,
    alertmanager_max_alerts_count: i64,
}

impl From<&LimitsConfig> for UserLimitsResponse {
    fn from(l: &LimitsConfig) -> Self {
        Self {
            compactor_blocks_retention_period_seconds: l.compactor_blocks_retention_period.0
                / 1_000_000_000,
            ingestion_rate: l.ingestion_rate,
            ingestion_burst_size: l.ingestion_burst_size,
            ingestion_burst_factor: l.ingestion_burst_factor,
            max_global_series_per_user: l.max_global_series_per_user,
            max_global_series_per_metric: l.max_global_series_per_metric,
            max_global_exemplars_per_user: l.max_global_exemplars_per_user,
            max_fetched_chunks_per_query: l.max_fetched_chunks_per_query,
            max_fetched_series_per_query: l.max_fetched_series_per_query,
            max_fetched_chunk_bytes_per_query: l.max_fetched_chunk_bytes_per_query,
            ruler_max_rules_per_rule_group: l.ruler_max_rules_per_rule_group,
            ruler_max_rule_groups_per_tenant: l.ruler_max_rule_groups_per_tenant,
            alertmanager_notification_rate_limit: l.alertmanager_notification_rate_limit,
            alertmanager_max_dispatcher_aggregation_groups: l
                .alertmanager_max_dispatcher_aggregation_groups,
            alertmanager_max_templates_count: l.alertmanager_max_templates_count,
            alertmanager_max_alerts_count: l.alertmanager_max_alerts_count,
        }
    }
}

fn go_http_error(status: StatusCode, msg: &str) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        format!("{msg}\n"),
    )
        .into_response()
}

async fn user_limits_handler(
    AxumState(st): AxumState<AppHttpState>,
    raw_org_id: Option<axum::Extension<mimir_ingester::http::RawScopeOrgId>>,
    headers: HeaderMap,
) -> Response {
    let org_bytes: &[u8] = if st.config.multitenancy_enabled {
        match raw_org_id.as_ref().map(|e| &e.0).filter(|r| !r.0.is_empty()) {
            Some(raw) => raw.0.as_slice(),
            None => match extract_org_id_bytes_from_http_headers(&headers) {
                Ok(b) => b,
                Err(err) => return go_http_error(StatusCode::UNAUTHORIZED, &err.to_string()),
            },
        }
    } else {
        st.config.no_auth_tenant.as_bytes()
    };

    let ids = match parse_tenant_ids_bytes(org_bytes) {
        Ok(ids) => ids,
        Err(err) => return go_http_error(StatusCode::UNAUTHORIZED, &err.to_string()),
    };
    let (federation, max_tenants) = st.config.tenant_federation_config();
    if !federation && ids.len() > 1 {
        return go_http_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!(
                "too many tenant IDs present in the request. max: 1 actual: {}",
                ids.len()
            ),
        );
    }
    if federation && max_tenants > 0 && ids.len() > max_tenants as usize {
        return go_http_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            &format!(
                "too many tenant IDs present in the request. max: {max_tenants} actual: {}",
                ids.len()
            ),
        );
    }

    let user_id = match parse_single_tenant_id_bytes(org_bytes) {
        Ok(id) => id,
        Err(err) => return go_http_error(StatusCode::UNAUTHORIZED, &err.to_string()),
    };

    let limits = st.overrides.for_tenant(&user_id);
    Json(UserLimitsResponse::from(limits.as_ref())).into_response()
}

#[derive(Serialize)]
struct BuildInfoResponse {
    status: &'static str,
    data: BuildInfoData,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BuildInfoData {
    application: &'static str,
    version: &'static str,
    revision: &'static str,
    branch: &'static str,
    go_version: String,
    features: BuildInfoFeatures,
}

/// Go `version.BuildInfoFeatures` (field order is the JSON order).
#[derive(Serialize)]
struct BuildInfoFeatures {
    ruler_config_api: String,
    alertmanager_config_api: String,
    query_sharding: String,
    federated_rules: String,
    ingest_storage: String,
}

/// A boolean option of a block mimir-rs keeps as an extra section: the CLI flag wins over the
/// YAML value, which wins over the flag default (Go flag registration + YAML + flag parsing).
fn extra_section_bool(cfg: &MimirConfig, path: &[&str], flag: &str, default: bool) -> bool {
    if let Some(v) = cfg.extra_cli_flag_value(flag) {
        // strconv.ParseBool (the value was validated when the flag was parsed).
        return matches!(v, "1" | "t" | "T" | "TRUE" | "true" | "True");
    }
    let Some((section, rest)) = path.split_first() else {
        return default;
    };
    let mut v = cfg.extra_sections.get(*section);
    for key in rest {
        v = v.and_then(|v| v.get(*key));
    }
    v.and_then(Value::as_bool).unwrap_or(default)
}

/// pkg/mimir/modules.go `initAPI` (`version.BuildInfoHandler("Grafana Mimir", ...)`).
async fn buildinfo_handler(AxumState(st): AxumState<AppHttpState>) -> Response {
    let cfg = &st.config;
    let features = BuildInfoFeatures {
        ruler_config_api: extra_section_bool(cfg, &["ruler", "enable_api"], "ruler.enable-api", true).to_string(),
        alertmanager_config_api: extra_section_bool(cfg, &["alertmanager", "enable_api"], "alertmanager.enable-api", true)
            .to_string(),
        query_sharding: extra_section_bool(
            cfg,
            &["frontend", "parallelize_shardable_queries"],
            "query-frontend.parallelize-shardable-queries",
            true,
        )
        .to_string(),
        federated_rules: extra_section_bool(
            cfg,
            &["ruler", "tenant_federation", "enabled"],
            "ruler.tenant-federation.enabled",
            false,
        )
        .to_string(),
        ingest_storage: cfg.ingest_storage.enabled.to_string(),
    };
    Json(BuildInfoResponse {
        status: "success",
        data: BuildInfoData {
            application: "Grafana Mimir",
            version: version_str(),
            revision: revision_str(),
            branch: branch_str(),
            go_version: go_version_str(),
            features,
        },
    })
    .into_response()
}

#[derive(Serialize)]
struct StatusDataResponse {
    status: &'static str,
    data: std::collections::BTreeMap<String, String>,
}

async fn status_config_handler() -> Response {
    Json(StatusDataResponse {
        status: "success",
        data: std::collections::BTreeMap::new(),
    })
    .into_response()
}

async fn status_flags_handler() -> Response {
    Json(StatusDataResponse {
        status: "success",
        data: std::collections::BTreeMap::new(),
    })
    .into_response()
}

/// Mimir `memberlistStatusHandler(kvs)` = dskit `HTTPStatusHandler` with Mimir's page template.
async fn memberlist_handler(
    AxumState(st): AxumState<AppHttpState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let res = http_status::serve_http(
        st.memberlist_kv.as_deref(),
        query.as_deref(),
        accept,
        render_memberlist_status_page,
    );
    let mut resp = Response::new(axum::body::Body::from(res.body));
    *resp.status_mut() = StatusCode::from_u16(res.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    for (name, value) in res.headers {
        if let Ok(v) = header::HeaderValue::from_str(&value) {
            resp.headers_mut().insert(name, v);
        }
    }
    resp
}

struct IndexLink {
    desc: &'static str,
    path: &'static str,
    dangerous: bool,
}

struct IndexGroup {
    weight: u8,
    desc: &'static str,
    links: Vec<IndexLink>,
}

async fn index_handler(AxumState(st): AxumState<AppHttpState>) -> Response {
    // pkg/api/handlers.go & pkg/api/api.go: relative paths so `-server.path-prefix` works cleanly.
    let mut groups: Vec<IndexGroup> = vec![
        IndexGroup {
            weight: 0,
            desc: "Overview",
            links: vec![IndexLink {
                desc: "Services' status",
                path: "services",
                dangerous: false,
            }],
        },
        IndexGroup {
            weight: 1,
            desc: "Current config",
            links: vec![
                IndexLink {
                    desc: "Including the default values",
                    path: "config",
                    dangerous: false,
                },
                IndexLink {
                    desc: "Only values that differ from the defaults",
                    path: "config?mode=diff",
                    dangerous: false,
                },
            ],
        },
    ];

    if st.runtime_config.is_some() {
        groups.push(IndexGroup {
            weight: 2,
            desc: "Current runtime config",
            links: vec![
                IndexLink {
                    desc: "Entire runtime config (including overrides)",
                    path: "runtime_config",
                    dangerous: false,
                },
                IndexLink {
                    desc: "Only values that differ from the defaults",
                    path: "runtime_config?mode=diff",
                    dangerous: false,
                },
            ],
        });
    }

    if let Some(ref ing) = st.ingester {
        let mut links = Vec::new();
        if ing.partition_ring_watcher.is_some() {
            links.push(IndexLink {
                desc: "Partition ring status",
                path: "ingester/partition-ring",
                dangerous: false,
            });
        }
        links.push(IndexLink {
            desc: "Ingester Tenants",
            path: "ingester/tenants",
            dangerous: true,
        });
        if ing.instance_ring.is_some() {
            links.push(IndexLink {
                desc: "Ring status",
                path: "ingester/ring",
                dangerous: false,
            });
        }
        groups.push(IndexGroup {
            weight: 3,
            desc: "Ingester",
            links,
        });
    }

    if st.memberlist_kv.is_some() || st.ingester.is_some() {
        groups.push(IndexGroup {
            weight: 4,
            desc: "Memberlist",
            links: vec![IndexLink {
                desc: "Status",
                path: "memberlist",
                dangerous: false,
            }],
        });
    }

    if st.ingester.is_some() {
        groups.push(IndexGroup {
            weight: 5,
            desc: "Dangerous",
            links: vec![
                IndexLink {
                    desc: "Trigger a flush of data from ingester to storage",
                    path: "ingester/flush",
                    dangerous: true,
                },
                IndexLink {
                    desc: "Trigger ingester shutdown",
                    path: "ingester/shutdown",
                    dangerous: true,
                },
            ],
        });
    }

    groups.sort_by(|a, b| a.weight.cmp(&b.weight).then_with(|| a.desc.cmp(b.desc)));

    let mut html = String::from(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <title>Grafana Mimir</title>

    <link rel="stylesheet" href="static/bootstrap-5.1.3.min.css">
    <link rel="stylesheet" href="static/mimir-styles.css">
    <script src="static/bootstrap-5.1.3.bundle.min.js"></script>
</head>
<body>
<div class="d-flex flex-column container py-3">
    <div class="header row border-bottom py-3 flex-column-reverse flex-sm-row">
        <div class="col-12 col-sm-9 text-center text-sm-start">
            <h1>Grafana Mimir Admin</h1>
        </div>
        <div class="col-12 col-sm-3 text-center text-sm-end mb-3 mb-sm-0">
            <img alt="Mimir logo" class="mimir-brand" src="static/mimir-icon.png">
        </div>
    </div>
"#,
    );

    for g in &groups {
        html.push_str(&format!(
            "    <div class=\"row service-row border-bottom py-3\">\n        <div class=\"col-sm-3 text-start text-sm-end\"><h2>{}</h2></div>\n        <div class=\"col-sm-9\">\n            <ul class=\"my-0 list-unstyled\">\n",
            html_escape(g.desc)
        ));
        for l in &g.links {
            html.push_str(&format!(
                "                <li><a href=\"{}\">{}</a>",
                html_escape(l.path),
                html_escape(l.desc)
            ));
            if l.dangerous {
                html.push_str(" <span class=\"badge bg-danger rounded-pill align-middle\">Dangerous</span>");
            }
            html.push_str("</li>\n");
        }
        html.push_str("            </ul>\n        </div>\n    </div>\n");
    }

    html.push_str("</div>\n</body>\n</html>");
    Html(html).into_response()
}

const STATIC_BOOTSTRAP_JS: &[u8] = include_bytes!("../static/bootstrap-5.1.3.bundle.min.js");
const STATIC_BOOTSTRAP_CSS: &[u8] = include_bytes!("../static/bootstrap-5.1.3.min.css");
const STATIC_BOOTSTRAP_ICONS_CSS: &[u8] = include_bytes!("../static/bootstrap-icons-1.8.1.css");
const STATIC_BOOTSTRAP_ICONS_WOFF: &[u8] = include_bytes!("../static/fonts/bootstrap-icons.woff");
const STATIC_BOOTSTRAP_ICONS_WOFF2: &[u8] = include_bytes!("../static/fonts/bootstrap-icons.woff2");
const STATIC_MIMIR_ICON: &[u8] = include_bytes!("../static/mimir-icon.png");
const STATIC_MIMIR_STYLES: &[u8] = include_bytes!("../static/mimir-styles.css");

async fn static_dir_handler() -> Response {
    let body = "<pre>\n\
        <a href=\"bootstrap-5.1.3.bundle.min.js\">bootstrap-5.1.3.bundle.min.js</a>\n\
        <a href=\"bootstrap-5.1.3.min.css\">bootstrap-5.1.3.min.css</a>\n\
        <a href=\"bootstrap-icons-1.8.1.css\">bootstrap-icons-1.8.1.css</a>\n\
        <a href=\"fonts/\">fonts/</a>\n\
        <a href=\"mimir-icon.png\">mimir-icon.png</a>\n\
        <a href=\"mimir-styles.css\">mimir-styles.css</a>\n\
        </pre>\n";
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn static_file_handler(AxumPath(path): AxumPath<String>) -> Response {
    let rel = path.trim_start_matches('/');
    let (bytes, ct): (&'static [u8], &'static str) = match rel {
        "bootstrap-5.1.3.bundle.min.js" => (STATIC_BOOTSTRAP_JS, "text/javascript; charset=utf-8"),
        "bootstrap-5.1.3.min.css" => (STATIC_BOOTSTRAP_CSS, "text/css; charset=utf-8"),
        "bootstrap-icons-1.8.1.css" => (STATIC_BOOTSTRAP_ICONS_CSS, "text/css; charset=utf-8"),
        "fonts/bootstrap-icons.woff" => (STATIC_BOOTSTRAP_ICONS_WOFF, "font/woff"),
        "fonts/bootstrap-icons.woff2" => (STATIC_BOOTSTRAP_ICONS_WOFF2, "font/woff2"),
        "mimir-icon.png" => (STATIC_MIMIR_ICON, "image/png"),
        "mimir-styles.css" => (STATIC_MIMIR_STYLES, "text/css; charset=utf-8"),
        "fonts/" | "fonts" => {
            let body = "<pre>\n\
                <a href=\"bootstrap-icons.woff\">bootstrap-icons.woff</a>\n\
                <a href=\"bootstrap-icons.woff2\">bootstrap-icons.woff2</a>\n\
                </pre>\n";
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                body,
            )
                .into_response();
        }
        _ => return not_found_handler().await,
    };

    let mut resp = Response::new(axum::body::Body::from(bytes));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(ct));
    resp.extensions_mut().insert(ExplicitContentLength);
    resp
}

fn encode_openmetrics_with_created(
    families: &[prometheus::proto::MetricFamily],
    cache: &MetricsCache,
    scheme: crate::expfmt::EscapingScheme,
) -> Vec<u8> {
    use prometheus::proto::MetricType;

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let om_float = |f: f64| -> String {
        if f == 1.0 {
            "1.0".to_string()
        } else if f == 0.0 {
            "0.0".to_string()
        } else if f == -1.0 {
            "-1.0".to_string()
        } else if f.is_nan() || f.is_infinite() {
            crate::expfmt::format_go_float_g(f)
        } else {
            let mut s = crate::expfmt::format_go_float_g(f);
            if !s.contains('e') && !s.contains('.') {
                s.push_str(".0");
            }
            s
        }
    };
    let escape = |s: &str| -> String {
        let mut out = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '"' => out.push_str("\\\""),
                _ => out.push(c),
            }
        }
        out
    };
    let write_name_labels = crate::expfmt::write_name_and_label_pairs;

    let mut out = Vec::with_capacity(64 * 1024);
    for mf in families {
        // Go `expfmt.NewEncoder`: `model.EscapeMetricFamily(v, scheme)` before encoding.
        let name = crate::expfmt::escape_family_name(mf.get_name(), scheme);
        let name: &str = &name;
        if mf.get_metric().is_empty() {
            continue;
        }
        let typ = mf.get_field_type();
        let mut compliant = name.to_string();
        if typ == MetricType::COUNTER && name.ends_with("_total") {
            compliant.truncate(name.len() - 6);
        }
        out.extend_from_slice(b"# HELP ");
        crate::expfmt::write_metric_name(&mut out, &compliant);
        out.push(b' ');
        out.extend_from_slice(escape(mf.get_help()).as_bytes());
        out.push(b'\n');
        let t = match typ {
            MetricType::COUNTER if name.ends_with("_total") => "counter",
            MetricType::COUNTER | MetricType::UNTYPED => "unknown",
            MetricType::GAUGE => "gauge",
            MetricType::SUMMARY => "summary",
            MetricType::HISTOGRAM => "histogram",
        };
        out.extend_from_slice(b"# TYPE ");
        crate::expfmt::write_metric_name(&mut out, &compliant);
        out.push(b' ');
        out.extend_from_slice(t.as_bytes());
        out.push(b'\n');
        let sample_name = if typ == MetricType::COUNTER && name.ends_with("_total") {
            format!("{compliant}_total")
        } else {
            compliant.clone()
        };
        for m in mf.get_metric() {
            let labels = crate::expfmt::escaped_sorted_labels(m, scheme);
            let ts = m.has_timestamp_ms().then(|| m.get_timestamp_ms());
            let mut key_buf = Vec::new();
            write_name_labels(&mut key_buf, &compliant, &labels, None);
            let series_key = String::from_utf8_lossy(&key_buf).into_owned();
            let emit_sample = |out: &mut Vec<u8>, sname: &str, extra: Option<(&str, &str)>, val: &str| {
                write_name_labels(out, sname, &labels, extra);
                out.push(b' ');
                out.extend_from_slice(val.as_bytes());
                if let Some(ts) = ts {
                    out.push(b' ');
                    out.extend_from_slice(om_float(ts as f64 / 1000.0).as_bytes());
                }
                out.push(b'\n');
            };
            let emit_created = |out: &mut Vec<u8>| {
                let created = cache.created_ts(&series_key, now_secs);
                write_name_labels(out, &format!("{compliant}_created"), &labels, None);
                out.push(b' ');
                out.extend_from_slice(om_float(created).as_bytes());
                out.push(b'\n');
            };
            match typ {
                MetricType::COUNTER => {
                    emit_sample(&mut out, &sample_name, None, &om_float(m.get_counter().get_value()));
                    emit_created(&mut out);
                }
                MetricType::GAUGE => {
                    emit_sample(&mut out, &sample_name, None, &om_float(m.get_gauge().get_value()));
                }
                MetricType::UNTYPED => {
                    emit_sample(&mut out, &sample_name, None, &om_float(m.get_untyped().get_value()));
                }
                MetricType::SUMMARY => {
                    let s = m.get_summary();
                    for q in s.get_quantile() {
                        let qv = om_float(q.get_quantile());
                        emit_sample(&mut out, &sample_name, Some(("quantile", &qv)), &om_float(q.get_value()));
                    }
                    emit_sample(&mut out, &format!("{sample_name}_sum"), None, &om_float(s.get_sample_sum()));
                    emit_sample(&mut out, &format!("{sample_name}_count"), None, &s.get_sample_count().to_string());
                    emit_created(&mut out);
                }
                MetricType::HISTOGRAM => {
                    let h = m.get_histogram();
                    let bucket_name = format!("{sample_name}_bucket");
                    let mut inf_seen = false;
                    for b in h.get_bucket() {
                        let le = om_float(b.get_upper_bound());
                        emit_sample(&mut out, &bucket_name, Some(("le", &le)), &b.get_cumulative_count().to_string());
                        if b.get_upper_bound().is_infinite() && b.get_upper_bound() > 0.0 {
                            inf_seen = true;
                        }
                    }
                    if !inf_seen {
                        emit_sample(&mut out, &bucket_name, Some(("le", "+Inf")), &h.get_sample_count().to_string());
                    }
                    emit_sample(&mut out, &format!("{sample_name}_sum"), None, &om_float(h.get_sample_sum()));
                    emit_sample(&mut out, &format!("{sample_name}_count"), None, &h.get_sample_count().to_string());
                    emit_created(&mut out);
                }
            }
        }
    }
    out.extend_from_slice(b"# EOF\n");
    out
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('+', "&#43;")
}

#[cfg(test)]
mod tests {
    /// Expected values produced by Go: `url.ParseRequestURI(in)`, `gorilla/mux`
    /// `cleanPath(u.EscapedPath())` (`UseEncodedPath`) and, when it differs,
    /// `u.Path = p; u.String()` (`None`: no redirect).
    #[test]
    fn gorilla_mux_clean_path_redirect_matches_go() {
        let cases: &[(&str, Option<&str>)] = &[
            ("/ready", None),
            ("/ready/", None),
            ("//ready", Some("/ready")),
            ("/./ready", Some("/ready")),
            ("/ready/..", Some("/")),
            ("/ready/../", Some("/")),
            ("/foo%2F%2Fbar", None),
            ("/%2e%2e/ready", None),
            ("/ingester/%2E/ring", None),
            ("/ready%2F", None),
            ("//foo(bar)", Some("/foo%28bar%29")),
            ("//foo%20bar", Some("/foo%2520bar")),
            ("/foo//b%41r", Some("/foo/b%2541r")),
            ("//a%2Fb", Some("/a%252Fb")),
            ("/%72eady", None),
            ("//x*y", Some("/x%2Ay")),
            ("/a//b!c'd", Some("/a/b%21c%27d")),
            ("//a%ffb", Some("/a%25ffb")),
            ("/a/./b/", Some("/a/b/")),
            ("///", Some("/")),
            ("//a+b", Some("/a+b")),
            ("//a%2Bb", Some("/a%252Bb")),
            ("//a%3Fb", Some("/a%253Fb")),
            ("//a;b,c:d@e&f=g$h", Some("/a;b,c:d@e&f=g$h")),
            ("//a~b_c-d.e", Some("/a~b_c-d.e")),
            ("//a%25b", Some("/a%2525b")),
            ("//a[b]{c}|d^e", Some("/a%255Bb%255D%257Bc%257D%257Cd%255Ee")),
            ("/", None),
            ("//", Some("/")),
        ];
        for (input, want) in cases {
            assert_eq!(
                super::gorilla_mux_clean_path_redirect(input).as_deref(),
                *want,
                "path {input:?}"
            );
        }
    }

    use super::*;
    use axum::body::Body;
    use http::{Method, Request};
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    fn test_state(http_path_prefix: &str, with_activity_tracker: bool) -> AppHttpState {
        let mut cfg = MimirConfig::default();
        cfg.server.http_path_prefix = http_path_prefix.to_string();
        let modules = ModuleStates::new();
        let server_cell = crate::modules::StateCell::new(State::Running);
        modules.add("server", server_cell.state_fn());
        if with_activity_tracker {
            let at_cell = crate::modules::StateCell::new(State::Running);
            modules.add("activity-tracker", at_cell.state_fn());
        }
        let limits = cfg.limits.clone();
        AppHttpState {
            config: Arc::new(cfg),
            default_config: Arc::new(MimirConfig::default()),
            registry: Registry::new(),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            ingester: None,
            runtime_config: None,
            overrides: Overrides::new(limits),
            memberlist_kv: None,
            modules,
            cost_attribution_registry: None,
        }
    }

    #[tokio::test]
    async fn prefixed_router_matches_go_static_pprof_and_root_routes() {
        let router = build_http_router(test_state("/mimir", false));

        // 1. Static assets served under prefix with gzip Vary header.
        let req = Request::builder()
            .method(Method::GET)
            .uri("/mimir/static/mimir-styles.css")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get(header::VARY).unwrap(), "Accept-Encoding");
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(bytes.as_ref(), STATIC_MIMIR_STYLES);

        // 2. /mimir (without trailing slash) -> 404.
        let req = Request::builder()
            .method(Method::GET)
            .uri("/mimir")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // 3. POST /mimir/ -> 404 (not 405).
        let req = Request::builder()
            .method(Method::POST)
            .uri("/mimir/")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert!(resp
            .extensions()
            .get::<crate::server::http_middleware::MethodMismatch>()
            .is_some());

        // 4. GET /mimir/ with gzip -> 200, gzipped, Vary: Accept-Encoding, relative hrefs.
        let req = Request::builder()
            .method(Method::GET)
            .uri("/mimir/")
            .header(header::ACCEPT_ENCODING, "gzip")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get(header::CONTENT_ENCODING).unwrap(), "gzip");
        assert_eq!(resp.headers().get(header::VARY).unwrap(), "Accept-Encoding");
        let gz_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let mut dec = flate2::read::GzDecoder::new(&gz_bytes[..]);
        let mut html = String::new();
        std::io::Read::read_to_string(&mut dec, &mut html).unwrap();
        assert!(html.contains(r#"href="services""#));
        assert!(html.contains(r#"href="config""#));
        assert!(!html.contains(r#"href="/mimir/"#));

        // 5. /mimir/debug/pprof and /mimir/debug/pprof/ both return 404.
        for p in ["/mimir/debug/pprof", "/mimir/debug/pprof/", "/mimir/debug/pprof/cmdline"] {
            let req = Request::builder()
                .method(Method::GET)
                .uri(p)
                .body(Body::empty())
                .unwrap();
            let resp = router.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "path {p}");
        }
    }

    #[tokio::test]
    async fn slashless_prefix_falls_back_to_unprefixed_like_gorilla_mux() {
        let router = build_http_router(test_state("mimir", false));
        let req = Request::builder()
            .method(Method::GET)
            .uri("/mimir/ready")
            .body(Body::empty())
            .unwrap();
        assert_eq!(router.clone().oneshot(req).await.unwrap().status(), StatusCode::NOT_FOUND);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/ready")
            .body(Body::empty())
            .unwrap();
        assert_eq!(router.oneshot(req).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unprefixed_static_and_services_format() {
        let router = build_http_router(test_state("", true));

        let req = Request::builder()
            .method(Method::GET)
            .uri("/static/mimir-styles.css")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/services")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        assert!(body.contains("<td>activity-tracker</td>"));
        assert!(body.contains("Current time: "));
        assert!(!body.contains("T00:") && !body.ends_with("Z</p>"));
    }

    #[test]
    fn apc02_openmetrics_with_created_quotes_non_legacy_names() {
        use prometheus::proto::{Counter, LabelPair, Metric, MetricFamily, MetricType};

        let mut mf = MetricFamily::default();
        mf.set_name("http.server.requests_total".to_string());
        mf.set_help("Requests".to_string());
        mf.set_field_type(MetricType::COUNTER);

        let mut lp1 = LabelPair::default();
        lp1.set_name("k8s.namespace.name".to_string());
        lp1.set_value("prod".to_string());
        let mut lp2 = LabelPair::default();
        lp2.set_name("method".to_string());
        lp2.set_value("GET".to_string());

        let mut m = Metric::default();
        m.set_label(vec![lp1, lp2].into());
        let mut c = Counter::default();
        c.set_value(7.0);
        m.set_counter(c);
        mf.mut_metric().push(m);

        let cache = MetricsCache::new(std::time::Duration::ZERO);
        let out = String::from_utf8(encode_openmetrics_with_created(
            std::slice::from_ref(&mf),
            &cache,
            crate::expfmt::EscapingScheme::NoEscaping,
        ))
        .unwrap();
        assert!(out.contains("# HELP \"http.server.requests\" Requests\n"), "{out}");
        assert!(out.contains("# TYPE \"http.server.requests\" counter\n"), "{out}");
        assert!(
            out.contains("{\"http.server.requests_total\",\"k8s.namespace.name\"=\"prod\",method=\"GET\"} 7.0\n"),
            "{out}"
        );
        assert!(
            out.contains("{\"http.server.requests_created\",\"k8s.namespace.name\"=\"prod\",method=\"GET\"} "),
            "{out}"
        );
        assert!(out.ends_with("# EOF\n"), "{out}");
    }

    #[tokio::test]
    async fn apc06_and_apc07_tsdb_metrics_through_cmd_router_has_vary_and_federation() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = MimirConfig::default();
        cfg.ingester.ring.instance_id = "test-0".to_string();
        cfg.blocks_storage.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        cfg.extra_sections.insert(
            "tenant_federation".to_string(),
            serde_yaml::from_str("enabled: true\nmax_tenants: 2\n").unwrap(),
        );

        let ing = mimir_ingester::Ingester::new(
            cfg.ingester.clone(),
            cfg.blocks_storage.clone(),
            cfg.ingest_storage.clone(),
            Overrides::new(cfg.limits.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (tf_enabled, tf_max) = cfg.tenant_federation_config();
        ing.set_tenant_federation(tf_enabled, tf_max);
        let _ = ing.get_or_create_tsdb("tenant-a").unwrap();

        let mut st = test_state("", false);
        st.config = Arc::new(cfg);
        st.ingester = Some(ing);
        let router = build_http_router(st);

        // 1. Gzip + Vary: Accept only on /ingester/tsdb_metrics (verified against the Go binary:
        // promhttp's TimeoutHandler replaces the gzip handler's Vary: Accept-Encoding).
        let req = Request::builder()
            .method(Method::GET)
            .uri("/ingester/tsdb_metrics")
            .header("X-Scope-OrgID", "tenant-a")
            .header(header::ACCEPT_ENCODING, "gzip")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get(header::CONTENT_ENCODING).unwrap(), "gzip");
        let vary_vals: Vec<&str> = resp
            .headers()
            .get_all(header::VARY)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(vary_vals, vec!["Accept"]);

        // 2. Tenant federation: 2 tenants -> 401 multiple org IDs present, 3 tenants -> 422.
        let req = Request::builder()
            .method(Method::GET)
            .uri("/ingester/tsdb_metrics")
            .header("X-Scope-OrgID", "tenant-a|tenant-b")
            .body(Body::empty())
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        assert_eq!(body, "multiple org IDs present\n");

        let req = Request::builder()
            .method(Method::GET)
            .uri("/ingester/tsdb_metrics")
            .header("X-Scope-OrgID", "tenant-a|tenant-b|tenant-c")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
        assert_eq!(body, "too many tenant IDs present in the request. max: 2 actual: 3\n");
    }

    #[tokio::test]
    async fn w24_b08_ready_503_nosniff_and_prepare_shutdown_content_type() {
        // 1. /ready 503 when module not running -> X-Content-Type-Options: nosniff + text/plain; charset=utf-8.
        let mut st = test_state("", false);
        let mods = ModuleStates::new();
        let cell = crate::modules::StateCell::new(State::Starting);
        mods.add("ingester", cell.state_fn());
        st.modules = mods;
        let router = build_http_router(st.clone());
        let resp = router
            .oneshot(Request::builder().method(Method::GET).uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
        assert_eq!(resp.headers().get(header::CONTENT_TYPE).unwrap(), "text/plain; charset=utf-8");

        // 2. /ready 503 when shutdown_requested -> X-Content-Type-Options: nosniff.
        let st_shut = test_state("", false);
        st_shut.shutdown_requested.store(true, Ordering::Release);
        let router_shut = build_http_router(st_shut);
        let resp = router_shut
            .oneshot(Request::builder().method(Method::GET).uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
        assert_eq!(resp.headers().get(header::CONTENT_TYPE).unwrap(), "text/plain; charset=utf-8");

        // 3. /ready 503 when ingester not ready + GET /ingester/prepare-shutdown Content-Type: text/plain.
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = MimirConfig::default();
        cfg.ingester.ring.instance_id = "test-0".to_string();
        cfg.blocks_storage.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = mimir_ingester::Ingester::new(
            cfg.ingester.clone(),
            cfg.blocks_storage.clone(),
            cfg.ingest_storage.clone(),
            Overrides::new(cfg.limits.clone()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let mut st_ing = test_state("", false);
        st_ing.ingester = Some(Arc::clone(&ing));
        let router_ing = build_http_router(st_ing);

        // Ingester in State::New -> check_ready() fails -> 503 with nosniff.
        let resp = router_ing
            .clone()
            .oneshot(Request::builder().method(Method::GET).uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
        assert_eq!(resp.headers().get(header::CONTENT_TYPE).unwrap(), "text/plain; charset=utf-8");

        // Ingester Running -> GET /ingester/prepare-shutdown emits Content-Type: text/plain (matching Go util.WriteTextResponse).
        ing.set_state_for_test(State::Running);
        let resp = router_ing
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/ingester/prepare-shutdown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get(header::CONTENT_TYPE).unwrap(), "text/plain");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"unset\n");
    }
}
