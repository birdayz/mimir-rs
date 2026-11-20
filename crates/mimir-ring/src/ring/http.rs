//! HTTP status & admin handler for classic hash rings (`/ingester/ring`).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/ring_http.go
//! - github.com/grafana/dskit/ring/ring_status.gohtml
//! - Go `net/http` `Request.ParseForm` / `Request.FormValue` / `http.Error`, `net/url.ParseQuery`
//!   and `mime.ParseMediaType` (shared by every admin handler that reads Go form values)

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::extract::{RawQuery, Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{on, MethodFilter};
use axum::Router;
use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_proto::ringpb::{Desc, InstanceDesc};
use serde::{Deserialize, Serialize};

use crate::kv::memberlist::gofmt::{
    format_go_time_string, format_rfc3339_nano, format_time_millis, go_zero_time, GoJson,
};

use super::model::{DescExt, InstanceDescExt};
use super::replication_strategy::REPORTING;
use super::Ring;
use crate::kv::{CasCallbackError, CasOutcome, KvClient, KvError};

pub type InstanceLinkFn = Arc<dyn Fn(&str, &InstanceDesc) -> String + Send + Sync>;

#[derive(Clone, Default)]
pub struct StatusPageConfig {
    pub hide_tokens_ui_elements: bool,
    pub show_versions: bool,
    pub component_names: HashMap<u64, String>,
    pub instance_link: Option<InstanceLinkFn>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RingHttpResponse {
    pub shards: Vec<IngesterHttpDesc>,
    pub now: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngesterHttpDesc {
    pub id: String,
    pub state: String,
    pub address: String,
    pub timestamp: String,
    pub registered_timestamp: String,
    pub read_only: bool,
    pub read_only_updated_timestamp: String,
    pub zone: String,
    pub tokens: Vec<u32>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub versions: BTreeMap<u64, u64>,
}

#[derive(Deserialize)]
pub struct RingQuery {
    #[serde(default)]
    pub tokens: Option<String>,
}

pub trait RingAccess: Send + Sync {
    fn get_ring_desc(&self) -> Desc;
    fn heartbeat_timeout(&self) -> Duration;
    fn kv_client(&self) -> Option<&Arc<dyn KvClient>>;
    fn ring_key(&self) -> &str;
}

impl RingAccess for Ring {
    fn get_ring_desc(&self) -> Desc {
        self.desc_snapshot()
    }

    fn heartbeat_timeout(&self) -> Duration {
        self.heartbeat_timeout()
    }

    fn kv_client(&self) -> Option<&Arc<dyn KvClient>> {
        self.kv_client()
    }

    fn ring_key(&self) -> &str {
        self.key()
    }
}

/// Actually we don't have async_trait, so no attribute needed above; clean up trait declaration.
#[derive(Clone)]
pub struct RingPageState {
    pub ring: Arc<Ring>,
    pub config: StatusPageConfig,
}

pub fn ring_router(ring: Arc<Ring>, config: StatusPageConfig) -> Router {
    let state = RingPageState { ring, config };
    Router::new()
        .route(
            "/ingester/ring",
            on(MethodFilter::GET, handle_ring_get)
                .head(|| async { StatusCode::METHOD_NOT_ALLOWED })
                .post(handle_ring_post),
        )
        .with_state(state)
}

pub async fn handle_ring_get(
    State(state): State<RingPageState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let desc = state.ring.desc_snapshot();
    let hb_timeout = state.ring.heartbeat_timeout();
    let now = SystemTime::now();
    let now_local = chrono::DateTime::<Utc>::from(now)
        .with_timezone(&chrono::Local)
        .fixed_offset();
    let owned_tokens = desc.count_tokens();

    let mut ids: Vec<String> = desc.ingesters.keys().cloned().collect();
    ids.sort();

    let mut shards = Vec::with_capacity(ids.len());
    for id in &ids {
        let ing = &desc.ingesters[id];
        let mut st_str = ing.instance_state().as_str_name().to_string();
        if !ing.is_healthy(REPORTING, hb_timeout, now) {
            st_str = "UNHEALTHY".to_string();
        }
        let (ro, ro_ts) = ing.get_read_only_state();
        shards.push(IngesterHttpDesc {
            id: id.clone(),
            state: st_str,
            address: ing.addr.clone(),
            timestamp: format_unix_secs_rfc3339_utc(ing.timestamp),
            registered_timestamp: ing
                .get_registered_at()
                .map(format_rfc3339_nano_utc)
                .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_string()),
            read_only: ro,
            read_only_updated_timestamp: ro_ts
                .map(format_rfc3339_nano_utc)
                .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_string()),
            zone: ing.zone.clone(),
            tokens: ing.tokens.clone(),
            versions: ing.versions.iter().map(|(&k, &v)| (k, v)).collect(),
        });
    }

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("application/json") {
        let json_shards = if shards.is_empty() {
            GoJson::Null
        } else {
            GoJson::Array(shards.iter().map(ingester_desc_to_go_json).collect())
        };
        let body = GoJson::Object(vec![
            ("shards".to_string(), json_shards),
            ("now".to_string(), GoJson::time(&now_local)),
        ])
        .encode();
        return ([(header::CONTENT_TYPE, "application/json")], body).into_response();
    }

    let mut query_values = HashMap::new();
    let _ = go_parse_query(&mut query_values, raw_query.as_deref().unwrap_or(""));
    let show_tokens = query_values
        .get("tokens")
        .and_then(|v| v.first())
        .map(String::as_str)
        == Some("true");
    let html = render_ring_html(&shards, &owned_tokens, now, now_local, show_tokens, &state.config, &desc);
    ([(header::CONTENT_TYPE, "text/html")], html).into_response()
}

/// Go `ringPageHandler.handle` for `POST`: `req.FormValue("forget")` (URL query merged with an
/// optional `application/x-www-form-urlencoded` body; no `Content-Type` is required).
pub async fn handle_ring_post(State(state): State<RingPageState>, req: Request) -> Response {
    let form = parse_go_form(req).await;
    let forget = form.value("forget").to_string();
    let Some(kv) = state.ring.kv_client() else {
        return go_http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("error forgetting instance '{forget}': ring has no KV client configured"),
        );
    };
    let forget_id = forget.clone();
    let res = kv
        .cas(
            state.ring.key(),
            Box::new(move |cur| {
                let Some(any) = cur else {
                    return Err(CasCallbackError::fatal(KvError::Other(
                        "found empty ring when trying to unregister".to_string(),
                    )));
                };
                let mut desc = any
                    .downcast_ref::<Desc>()
                    .ok_or_else(|| {
                        CasCallbackError::fatal(KvError::InvalidType("expected Desc".to_string()))
                    })?
                    .clone();
                desc.remove_ingester(&forget_id);
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await;

    match res {
        Ok(()) => (
            StatusCode::FOUND,
            [(header::LOCATION, "#")],
            "",
        )
            .into_response(),
        Err(err) => go_http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("error forgetting instance '{forget}': {err}"),
        ),
    }
}

fn format_unix_secs_rfc3339_utc(secs: i64) -> String {
    match DateTime::<Utc>::from_timestamp(secs, 0) {
        Some(dt) => format_rfc3339_nano(&dt.fixed_offset()),
        None => "0001-01-01T00:00:00Z".to_string(),
    }
}

fn ingester_desc_to_go_json(d: &IngesterHttpDesc) -> GoJson {
    let tokens = if d.tokens.is_empty() {
        GoJson::Null
    } else {
        GoJson::Array(d.tokens.iter().map(|&t| GoJson::int(i128::from(t))).collect())
    };
    let mut fields = vec![
        ("id".to_string(), GoJson::str(&d.id)),
        ("state".to_string(), GoJson::str(&d.state)),
        ("address".to_string(), GoJson::str(&d.address)),
        ("timestamp".to_string(), GoJson::str(&d.timestamp)),
        ("registered_timestamp".to_string(), GoJson::str(&d.registered_timestamp)),
        ("read_only".to_string(), GoJson::Bool(d.read_only)),
        (
            "read_only_updated_timestamp".to_string(),
            GoJson::str(&d.read_only_updated_timestamp),
        ),
        ("zone".to_string(), GoJson::str(&d.zone)),
        ("tokens".to_string(), tokens),
    ];
    if !d.versions.is_empty() {
        let entries = d
            .versions
            .iter()
            .map(|(k, v)| (k.to_string(), GoJson::int(i128::from(*v))))
            .collect();
        fields.push(("versions".to_string(), GoJson::sorted_map(entries)));
    }
    GoJson::Object(fields)
}

/// Go `fmt.Sprintf("%.3g", f)` for finite non-negative percentages.
pub(crate) fn go_format_g3(f: f64) -> String {
    if !f.is_finite() {
        return if f.is_nan() { "NaN" } else { "+Inf" }.to_string();
    }
    if f == 0.0 {
        return "0".to_string();
    }
    let abs = f.abs();
    let exp = abs.log10().floor() as i32;
    let factor = 10f64.powi(2 - exp);
    let rounded = (abs * factor).round() / factor;
    let rounded_exp = rounded.log10().floor() as i32;
    let sign = if f.is_sign_negative() { "-" } else { "" };
    if !(-4..3).contains(&rounded_exp) {
        let mant = rounded / 10f64.powi(rounded_exp);
        let s = format!("{mant:.2}");
        let s = s.trim_end_matches('0').trim_end_matches('.');
        let esign = if rounded_exp < 0 { '-' } else { '+' };
        format!("{sign}{s}e{esign}{:02}", rounded_exp.unsigned_abs())
    } else {
        let dec = (2 - rounded_exp).max(0) as usize;
        let s = format!("{rounded:.dec$}");
        let s = if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.')
        } else {
            s.as_str()
        };
        format!("{sign}{s}")
    }
}

/// Go `time.Since(t).Truncate(time.Second).String()`.
fn go_duration_since(now: SystemTime, t: DateTime<FixedOffset>) -> String {
    let now_dt = DateTime::<Utc>::from(now);
    let diff = now_dt.signed_duration_since(t.with_timezone(&Utc));
    let total_secs = diff.num_seconds();
    if total_secs == 0 {
        return "0s".to_string();
    }
    let sign = if total_secs < 0 { "-" } else { "" };
    let mut rem = total_secs.unsigned_abs();
    let h = rem / 3600;
    rem %= 3600;
    let m = rem / 60;
    let s = rem % 60;
    let mut out = String::from(sign);
    if h > 0 {
        let _ = write!(out, "{h}h{m}m{s}s");
    } else if m > 0 {
        let _ = write!(out, "{m}m{s}s");
    } else {
        let _ = write!(out, "{s}s");
    }
    out
}

fn time_or_empty_string_unix(secs: i64) -> String {
    if secs <= 0 {
        return String::new();
    }
    let Some(dt) = DateTime::<Utc>::from_timestamp(secs, 0) else {
        return String::new();
    };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

fn utc_from_unix_or_zero(secs: i64) -> DateTime<FixedOffset> {
    if secs == 0 {
        return go_zero_time();
    }
    DateTime::<Utc>::from_timestamp(secs, 0)
        .map(|d| d.fixed_offset())
        .unwrap_or_else(go_zero_time)
}

fn render_ring_html(
    shards: &[IngesterHttpDesc],
    owned_tokens: &HashMap<String, i64>,
    now: SystemTime,
    now_local: DateTime<FixedOffset>,
    show_tokens: bool,
    config: &StatusPageConfig,
    desc: &Desc,
) -> String {
    let mut out = String::from(
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>Ring Status</title>\n</head>\n<body>\n<h1>Ring Status</h1>\n",
    );
    out.push_str(&format!(
        "<p>Current time: {}</p>\n",
        html_escape(&format_go_time_string(&now_local))
    ));
    out.push_str("<form action=\"\" method=\"POST\">\n");
    out.push_str("    <input type=\"hidden\" name=\"csrf_token\" value=\"$__CSRF_TOKEN_PLACEHOLDER__\">\n");
    out.push_str("    <table width=\"100%\" border=\"1\">\n        <thead>\n        <tr>\n");
    out.push_str("            <th>Instance ID</th>\n            <th>Availability Zone</th>\n            <th>State</th>\n            <th>Address</th>\n            <th>Registered At</th>\n            <th>Read-Only</th>\n            <th>Read-Only Updated</th>\n            <th>Last Heartbeat</th>\n");
    if !config.hide_tokens_ui_elements {
        out.push_str("            <th>Tokens</th>\n            <th>Ownership</th>\n");
    }
    if config.show_versions {
        out.push_str("            <th>Versions</th>\n");
    }
    out.push_str("            <th>Actions</th>\n        </tr>\n        </thead>\n        <tbody>\n");

    for (i, s) in shards.iter().enumerate() {
        let ing_raw = desc.ingesters.get(&s.id);
        let reg_ts = ing_raw.map_or(0, |d| d.registered_timestamp);
        let hb_ts = ing_raw.map_or(0, |d| d.timestamp);
        let ro_ts = ing_raw.map_or(0, |d| d.read_only_updated_timestamp);

        let row_attr = if i % 2 == 0 { "" } else { " bgcolor=\"#BEBEBE\"" };
        out.push_str(&format!("            <tr{row_attr}>\n"));
        let link_html = config
            .instance_link
            .as_ref()
            .and_then(|f| ing_raw.map(|d| f(&s.id, d)))
            .filter(|l| !l.is_empty());
        match link_html {
            Some(l) => out.push_str(&format!(
                "            <td><a href=\"{}\">{}</a></td>\n",
                html_url_attr_escape(&l),
                html_escape(&s.id)
            )),
            None => out.push_str(&format!("            <td>{}</td>\n", html_escape(&s.id))),
        }
        out.push_str(&format!("            <td>{}</td>\n", html_escape(&s.zone)));
        out.push_str(&format!("            <td>{}</td>\n", html_escape(&s.state)));
        out.push_str(&format!("            <td>{}</td>\n", html_escape(&s.address)));
        out.push_str(&format!(
            "            <td>{}</td>\n",
            html_escape(&time_or_empty_string_unix(reg_ts))
        ));
        out.push_str(&format!("            <td>{}</td>\n", s.read_only));
        if s.read_only {
            let dt = if ro_ts > 0 { utc_from_unix_or_zero(ro_ts) } else { go_zero_time() };
            out.push_str(&format!(
                "            <td>{} ago ({})</td>\n",
                go_duration_since(now, dt),
                format_time_millis(&dt)
            ));
        } else {
            let empty_or_ts = if ro_ts > 0 { time_or_empty_string_unix(ro_ts) } else { String::new() };
            out.push_str(&format!("            <td>{}</td>\n", html_escape(&empty_or_ts)));
        }
        let hb_dt = utc_from_unix_or_zero(hb_ts);
        out.push_str(&format!(
            "            <td>{} ago ({})</td>\n",
            go_duration_since(now, hb_dt),
            format_time_millis(&hb_dt)
        ));
        if !config.hide_tokens_ui_elements {
            let owned = owned_tokens.get(&s.id).copied().unwrap_or(0);
            let pct = (owned as f64 / u32::MAX as f64) * 100.0;
            out.push_str(&format!(
                "            <td>{}</td>\n            <td>{}%</td>\n",
                s.tokens.len(),
                go_format_g3(pct)
            ));
        }
        if config.show_versions {
            out.push_str("            <td>\n");
            for (k, v) in &s.versions {
                if let Some(cname) = config.component_names.get(k) {
                    out.push_str(&format!("              {k} ({}): v{v}<br/>\n", html_escape(cname)));
                } else {
                    out.push_str(&format!("              {k}: v{v}<br/>\n"));
                }
            }
            out.push_str("            </td>\n");
        }
        out.push_str(&format!(
            "            <td>\n                <button name=\"forget\" value=\"{}\" type=\"submit\">Forget</button>\n            </td>\n            </tr>\n",
            html_escape(&s.id)
        ));
    }
    out.push_str("        </tbody>\n    </table>\n    <br>\n");
    if !config.hide_tokens_ui_elements {
        if show_tokens {
            out.push_str("        <input type=\"button\" value=\"Hide Tokens\" onclick=\"window.location.href = '?tokens=false' \"/>\n");
            for s in shards {
                out.push_str(&format!("        <h2>Instance: {}</h2>\n        <p>\n            Tokens:<br/>\n", html_escape(&s.id)));
                for t in &s.tokens {
                    out.push_str(&format!("                {t}\n"));
                }
                out.push_str("        </p>\n");
            }
        } else {
            out.push_str("        <input type=\"button\" value=\"Show Tokens\" onclick=\"window.location.href = '?tokens=true'\"/>\n");
        }
    }
    out.push_str("</form>\n</body>\n</html>\n");
    out
}

/// Go `html/template` `htmlEscaper` / `attrEscaper` (`htmlReplacementTable`).
pub(crate) fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\0' => out.push('\u{FFFD}'),
            '"' => out.push_str("&#34;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '+' => out.push_str("&#43;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            c => out.push(c),
        }
    }
    out
}

/// Go `html/template` escaping of a string in a quoted URL attribute (`href="{{ .Link }}"`):
/// `urlFilter` (schemes other than http, https and mailto become `#ZgotmplZ`), `urlNormalizer`
/// (`processURLOnto(s, true)`: lower-case `%xx` for bytes outside RFC 3986 unreserved/reserved
/// characters, valid escapes kept) and `attrEscaper`.
fn html_url_attr_escape(s: &str) -> String {
    if let Some((protocol, _)) = s.split_once(':') {
        if !protocol.contains('/')
            && !protocol.eq_ignore_ascii_case("http")
            && !protocol.eq_ignore_ascii_case("https")
            && !protocol.eq_ignore_ascii_case("mailto")
        {
            return "#ZgotmplZ".to_string();
        }
    }
    let b = s.as_bytes();
    let mut normalized = String::with_capacity(s.len() + 16);
    for (i, &c) in b.iter().enumerate() {
        let keep = match c {
            b'!' | b'#' | b'$' | b'&' | b'*' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?'
            | b'@' | b'[' | b']' | b'-' | b'.' | b'_' | b'~' => true,
            b'%' => i + 2 < b.len() && b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit(),
            c => c.is_ascii_alphanumeric(),
        };
        if keep {
            normalized.push(char::from(c));
        } else {
            normalized.push_str(&format!("%{c:02x}"));
        }
    }
    html_escape(&normalized)
}

// ---------------------------------------------------------------------------------------------
// Go `net/http` form handling.
// ---------------------------------------------------------------------------------------------

/// Go `http.Error(w, msg, code)`: `text/plain; charset=utf-8`, `X-Content-Type-Options: nosniff`
/// and the message followed by a newline.
pub fn go_http_error(status: StatusCode, msg: &str) -> Response {
    let mut body = String::with_capacity(msg.len() + 1);
    body.push_str(msg);
    body.push('\n');
    (
        status,
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}

/// Go's `http.Request.Form` (`url.Values`) after `Request.ParseForm`, plus the first error
/// `ParseForm` reported. Like Go, values that parsed successfully are kept even on error.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GoForm {
    values: HashMap<String, Vec<String>>,
    err: Option<String>,
}

impl GoForm {
    /// Go `r.Form[key]` (`None` when the key is absent; an empty value is `Some([""])`).
    pub fn get(&self, key: &str) -> Option<&[String]> {
        self.values.get(key).map(Vec::as_slice)
    }

    /// Go `r.FormValue(key)`: the first value, or `""`.
    pub fn value(&self, key: &str) -> &str {
        self.values
            .get(key)
            .and_then(|v| v.first())
            .map_or("", String::as_str)
    }

    /// The error `r.ParseForm()` returned, if any.
    pub fn error(&self) -> Option<&str> {
        self.err.as_deref()
    }

    fn set_err(&mut self, err: String) {
        if self.err.is_none() {
            self.err = Some(err);
        }
    }
}

/// `parsePostForm`'s limit for bodies not already wrapped in a `MaxBytesReader` (10 MiB).
const MAX_FORM_SIZE: usize = 10 << 20;

/// Go `r.ParseForm()`: for `POST`/`PUT`/`PATCH` with `Content-Type:
/// application/x-www-form-urlencoded` the body values come first, followed by the URL query
/// values. Values are strictly percent/`+` decoded (`url.QueryUnescape`); invalid escapes,
/// `;` separators and oversized bodies are reported through [`GoForm::error`].
///
/// `multipart/form-data` bodies are not decoded (only `Request.ParseMultipartForm` does that in
/// Go, and none of the ingester admin handlers rely on it); such requests see the URL values.
pub async fn parse_go_form(req: Request) -> GoForm {
    let (parts, body) = req.into_parts();
    let mut form = GoForm::default();

    if matches!(parts.method, Method::POST | Method::PUT | Method::PATCH) {
        let ct = parts
            .headers
            .get(header::CONTENT_TYPE)
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .unwrap_or_default();
        let ct = if ct.is_empty() {
            "application/octet-stream"
        } else {
            ct.as_str()
        };
        let (media_type, ct_err) = go_parse_media_type(ct);
        if let Some(e) = ct_err {
            form.set_err(e.to_string());
        }
        if media_type == "application/x-www-form-urlencoded" {
            match read_limited(body, MAX_FORM_SIZE + 1).await {
                Ok(b) if b.len() > MAX_FORM_SIZE => form.set_err("http: POST too large".to_string()),
                Ok(b) => {
                    let mut post = HashMap::new();
                    let err = go_parse_query(&mut post, &String::from_utf8_lossy(&b));
                    form.values = post;
                    if let Some(e) = err {
                        form.set_err(e);
                    }
                }
                Err(e) => form.set_err(e.to_string()),
            }
        }
    }

    let mut query_values = HashMap::new();
    let err = go_parse_query(&mut query_values, parts.uri.query().unwrap_or(""));
    if let Some(e) = err {
        form.set_err(e);
    }
    if form.values.is_empty() {
        form.values = query_values;
    } else {
        for (k, v) in query_values {
            form.values.entry(k).or_default().extend(v);
        }
    }
    form
}

/// `io.ReadAll(io.LimitReader(body, limit))`.
async fn read_limited(body: axum::body::Body, limit: usize) -> Result<Vec<u8>, axum::Error> {
    use futures::StreamExt as _;
    let mut stream = body.into_data_stream();
    let mut out = Vec::new();
    while out.len() < limit {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk?;
        let take = chunk.len().min(limit - out.len());
        out.extend_from_slice(&chunk[..take]);
    }
    Ok(out)
}

/// Go `url.ParseQuery` into `m` (`parseQuery`): returns the first error, keeping every value
/// that parsed.
pub fn go_parse_query(m: &mut HashMap<String, Vec<String>>, query: &str) -> Option<String> {
    // `urlParamsWithinMax` (default `urlmaxqueryparams` = 10000).
    if query.bytes().filter(|&b| b == b'&').count() + 1 > 10_000 {
        return Some("number of URL query parameters exceeded limit".to_string());
    }
    let mut err: Option<String> = None;
    for part in query.split('&') {
        if part.contains(';') {
            err = Some("invalid semicolon separator in query".to_string());
            continue;
        }
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let key = match go_query_unescape(k) {
            Ok(k) => k,
            Err(e) => {
                err.get_or_insert(e);
                continue;
            }
        };
        let value = match go_query_unescape(v) {
            Ok(v) => v,
            Err(e) => {
                err.get_or_insert(e);
                continue;
            }
        };
        m.entry(key).or_default().push(value);
    }
    err
}

/// Go `url.QueryUnescape` (`unescape(s, encodeQueryComponent)`), including its
/// `invalid URL escape "%ZZ"` error.
pub fn go_query_unescape(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    if !b.iter().any(|&c| c == b'%' || c == b'+') {
        return Ok(s.to_string());
    }
    let hex = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => match (b.get(i + 1).copied().and_then(hex), b.get(i + 2).copied().and_then(hex)) {
                (Some(hi), Some(lo)) => {
                    out.push(hi << 4 | lo);
                    i += 3;
                }
                _ => {
                    let end = (i + 3).min(b.len());
                    let bad = String::from_utf8_lossy(&b[i..end]);
                    return Err(format!(
                        "invalid URL escape {}",
                        crate::dns::go_quote(&bad)
                    ));
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// Go `mime.ParseMediaType`, returning only the (lower-cased) media type and the error. Like Go,
/// an invalid parameter keeps the media type, while an invalid type or a conflicting duplicate
/// parameter clears it.
pub fn go_parse_media_type(v: &str) -> (String, Option<&'static str>) {
    let base = v.split(';').next().unwrap_or("");
    let media_type = base.to_lowercase().trim().to_string();
    if let Err(e) = check_media_type_disposition(&media_type) {
        return (String::new(), Some(e));
    }
    let mut params: HashMap<String, String> = HashMap::new();
    let mut rest = &v[base.len()..];
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        let Some((key, value, next)) = consume_media_param(rest) else {
            if rest.trim() == ";" {
                break;
            }
            return (media_type, Some("mime: invalid media parameter"));
        };
        if params.get(&key).is_some_and(|prev| *prev != value) {
            return (String::new(), Some("mime: duplicate parameter name"));
        }
        params.insert(key, value);
        rest = next;
    }
    (media_type, None)
}

fn is_tspecial(c: u8) -> bool {
    b"()<>@,;:\\\"/[]?=".contains(&c)
}

fn is_token_char(c: u8) -> bool {
    c > 0x20 && c < 0x7f && !is_tspecial(c)
}

fn consume_token(v: &str) -> (&str, &str) {
    let end = v.bytes().position(|c| !is_token_char(c)).unwrap_or(v.len());
    v.split_at(end)
}

fn check_media_type_disposition(s: &str) -> Result<(), &'static str> {
    let (typ, rest) = consume_token(s);
    if typ.is_empty() {
        return Err("mime: no media type");
    }
    if rest.is_empty() {
        return Ok(());
    }
    let Some(rest) = rest.strip_prefix('/') else {
        return Err("mime: expected slash after first token");
    };
    let (subtype, rest) = consume_token(rest);
    if subtype.is_empty() {
        return Err("mime: expected token after slash");
    }
    if !rest.is_empty() {
        return Err("mime: unexpected content after media subtype");
    }
    Ok(())
}

/// Go `consumeValue`: a token or a quoted string; `None` when nothing could be consumed.
fn consume_value(v: &str) -> Option<(String, &str)> {
    let b = v.as_bytes();
    if b.first() != Some(&b'"') {
        let (tok, rest) = consume_token(v);
        return if tok.is_empty() { None } else { Some((tok.to_string(), rest)) };
    }
    let mut buf = Vec::new();
    let mut i = 1;
    while i < b.len() {
        let r = b[i];
        if r == b'"' {
            return Some((String::from_utf8_lossy(&buf).into_owned(), &v[i + 1..]));
        }
        if r == b'\\' && i + 1 < b.len() && is_tspecial(b[i + 1]) {
            buf.push(b[i + 1]);
            i += 2;
            continue;
        }
        if r == b'\r' || r == b'\n' {
            return None;
        }
        buf.push(r);
        i += 1;
    }
    None
}

/// Go `consumeMediaParam`.
fn consume_media_param(v: &str) -> Option<(String, String, &str)> {
    let rest = v.trim_start().strip_prefix(';')?.trim_start();
    let (param, rest) = consume_token(rest);
    if param.is_empty() {
        return None;
    }
    let rest = rest.trim_start().strip_prefix('=')?.trim_start();
    let (value, rest) = consume_value(rest)?;
    Some((param.to_lowercase(), value, rest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    fn req(method: Method, uri: &str, ct: Option<&str>, body: &str) -> Request {
        let mut b = axum::http::Request::builder().method(method).uri(uri);
        if let Some(ct) = ct {
            b = b.header(header::CONTENT_TYPE, ct);
        }
        b.body(Body::from(body.to_string())).unwrap()
    }

    #[tokio::test]
    async fn parse_form_merges_post_body_before_query() {
        let f = parse_go_form(req(
            Method::POST,
            "/x?tenant=q1&wait=false",
            Some("application/x-www-form-urlencoded"),
            "tenant=b1&wait=true",
        ))
        .await;
        assert_eq!(f.error(), None);
        assert_eq!(f.get("tenant").unwrap(), ["b1", "q1"]);
        assert_eq!(f.value("wait"), "true");
    }

    #[tokio::test]
    async fn parse_form_ignores_body_without_form_content_type() {
        // No Content-Type => application/octet-stream: body ignored, query still parsed (no 415).
        let f = parse_go_form(req(Method::POST, "/x?forget=ghost", None, "forget=other")).await;
        assert_eq!(f.error(), None);
        assert_eq!(f.value("forget"), "ghost");
        // GET never reads the body.
        let f = parse_go_form(req(
            Method::GET,
            "/x",
            Some("application/x-www-form-urlencoded"),
            "a=b",
        ))
        .await;
        assert_eq!(f.get("a"), None);
    }

    #[tokio::test]
    async fn parse_form_decodes_and_keeps_empty_values() {
        let f = parse_go_form(req(Method::GET, "/x?tenant=&t=t%281%29+x&flag", None, "")).await;
        assert_eq!(f.error(), None);
        assert_eq!(f.get("tenant").unwrap(), [""]);
        assert_eq!(f.value("t"), "t(1) x");
        assert_eq!(f.get("flag").unwrap(), [""]);
        assert_eq!(f.get("missing"), None);
    }

    #[tokio::test]
    async fn parse_form_reports_errors_but_keeps_valid_values() {
        let f = parse_go_form(req(Method::GET, "/x?tenant=%ZZ&wait=true", None, "")).await;
        assert_eq!(f.error(), Some("invalid URL escape \"%ZZ\""));
        assert_eq!(f.get("tenant"), None);
        assert_eq!(f.value("wait"), "true");

        let f = parse_go_form(req(Method::GET, "/x?a=1;b=2&c=3", None, "")).await;
        assert_eq!(f.error(), Some("invalid semicolon separator in query"));
        assert_eq!(f.value("c"), "3");

        let f = parse_go_form(req(Method::GET, "/x?a=%4", None, "")).await;
        assert_eq!(f.error(), Some("invalid URL escape \"%4\""));

        let f = parse_go_form(req(
            Method::POST,
            "/x",
            Some("application/x-www-form-urlencoded"),
            "tenant=%G1",
        ))
        .await;
        assert_eq!(f.error(), Some("invalid URL escape \"%G1\""));
    }

    #[tokio::test]
    async fn parse_form_rejects_oversized_body() {
        let body = "a".repeat(MAX_FORM_SIZE + 1);
        let f = parse_go_form(req(
            Method::POST,
            "/x?q=1",
            Some("application/x-www-form-urlencoded"),
            &body,
        ))
        .await;
        assert_eq!(f.error(), Some("http: POST too large"));
        assert_eq!(f.value("q"), "1");
    }

    #[test]
    fn media_type_matches_go_parse_media_type() {
        assert_eq!(
            go_parse_media_type("Application/X-WWW-Form-Urlencoded; charset=UTF-8"),
            ("application/x-www-form-urlencoded".to_string(), None)
        );
        assert_eq!(
            go_parse_media_type("application/x-www-form-urlencoded;"),
            ("application/x-www-form-urlencoded".to_string(), None)
        );
        assert_eq!(
            go_parse_media_type("application/x-www-form-urlencoded; charset"),
            (
                "application/x-www-form-urlencoded".to_string(),
                Some("mime: invalid media parameter")
            )
        );
        assert_eq!(
            go_parse_media_type("text/plain; a=1; a=2"),
            (String::new(), Some("mime: duplicate parameter name"))
        );
        assert_eq!(
            go_parse_media_type("text/plain; a=\"x\\\"y\"; a=\"x\\\"y\""),
            ("text/plain".to_string(), None)
        );
        assert_eq!(go_parse_media_type(""), (String::new(), Some("mime: no media type")));
        assert_eq!(
            go_parse_media_type("text/"),
            (String::new(), Some("mime: expected token after slash"))
        );
        assert_eq!(
            go_parse_media_type("text plain"),
            (String::new(), Some("mime: expected slash after first token"))
        );
    }

    #[test]
    fn go_http_error_appends_newline() {
        let resp = go_http_error(StatusCode::BAD_REQUEST, "boom");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
    }

    /// Expected values produced by Go `html/template` with dskit's
    /// `<a href="{{ .Link }}">{{ .ID }}</a>` (Go 1.27).
    #[test]
    fn url_attr_and_html_escaping_match_go_html_template() {
        for (link, want) in [
            ("http://10.0.0.1:8080/ingester/tenants", "http://10.0.0.1:8080/ingester/tenants"),
            (
                "http://[::1]:8080/x?a=b&c=d e+f'\"<>%41%zz%4",
                "http://[::1]:8080/x?a=b&amp;c=d%20e&#43;f%27%22%3c%3e%41%25zz%254",
            ),
            ("javascript:alert(1)", "#ZgotmplZ"),
            ("ftp://x/y", "#ZgotmplZ"),
            ("/relative/{ingester}é", "/relative/%7bingester%7d%c3%a9"),
            ("HTTPS://x", "HTTPS://x"),
            ("mailto:a@b", "mailto:a@b"),
            ("a/b:c", "a/b:c"),
        ] {
            assert_eq!(html_url_attr_escape(link), want, "{link:?}");
        }
        assert_eq!(html_escape("ing-1<&\"'+>"), "ing-1&lt;&amp;&#34;&#39;&#43;&gt;");
    }

    #[test]
    fn ring_page_links_instances_with_the_configured_template() {
        let mut desc = Desc::default();
        desc.ingesters.insert(
            "ingester-1".to_string(),
            InstanceDesc {
                addr: "[2001:db8::1]:9095".to_string(),
                ..Default::default()
            },
        );
        desc.ingesters.insert("ingester-2".to_string(), InstanceDesc::default());
        let shard = |id: &str| IngesterHttpDesc {
            id: id.to_string(),
            state: "ACTIVE".to_string(),
            address: String::new(),
            timestamp: String::new(),
            registered_timestamp: String::new(),
            read_only: false,
            read_only_updated_timestamp: String::new(),
            zone: String::new(),
            tokens: Vec::new(),
            versions: BTreeMap::new(),
        };
        let shards = [shard("ingester-1"), shard("ingester-2")];
        let mut cfg = mimir_config::ingester::RingConfig {
            status_page_ingester_link_template: "http://{host}:8080/{ingester}/x?a=1&b=2".to_string(),
            ..Default::default()
        };
        let now = SystemTime::now();
        let now_local = chrono::DateTime::<Utc>::from(now).with_timezone(&chrono::Local).fixed_offset();
        let page = render_ring_html(
            &shards,
            &HashMap::new(),
            now,
            now_local,
            false,
            &super::super::to_status_page_config(&cfg),
            &desc,
        );
        assert!(
            page.contains(
                "<td><a href=\"http://[2001:db8::1]:8080/ingester-1/x?a=1&amp;b=2\">ingester-1</a></td>"
            ),
            "{page}"
        );
        // An address without port is used as the host as is.
        assert!(page.contains("<a href=\"http://:8080/ingester-2/x?a=1&amp;b=2\">ingester-2</a>"), "{page}");

        // Empty template: no links.
        cfg.status_page_ingester_link_template.clear();
        let page = render_ring_html(
            &shards,
            &HashMap::new(),
            now,
            now_local,
            false,
            &super::super::to_status_page_config(&cfg),
            &desc,
        );
        assert!(page.contains("<td>ingester-1</td>") && !page.contains("<a href"), "{page}");
    }

    #[tokio::test]
    async fn w21_aps_01_and_07_ring_router_rejects_head_405_and_empty_ring_forget_no_key_not_found() {
        let kv: Arc<dyn crate::kv::KvClient> =
            Arc::new(crate::kv::inmemory::InMemoryKv::new(crate::ring::ring_desc_codec()));
        let mut ring_cfg = mimir_config::ingester::RingConfig::default();
        ring_cfg.kvstore.store = "inmemory".to_string();
        ring_cfg.replication_factor = 1;
        let ring = Ring::with_store_and_strategy(
            crate::ring::RingSettings::from(&ring_cfg),
            "ingester",
            "ring",
            kv,
            Arc::new(crate::ring::DefaultReplicationStrategy),
            None,
        )
        .unwrap();
        let app = ring_router(ring, crate::ring::to_status_page_config(&ring_cfg));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        // W21-APS-01: HEAD /ingester/ring must return 405 Method Not Allowed.
        let head_resp = client
            .head(format!("http://{addr}/ingester/ring"))
            .send()
            .await
            .unwrap();
        assert_eq!(head_resp.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

        // W21-APS-07: POST /ingester/ring on empty ring omits "key not found: ".
        let post_resp = client
            .post(format!("http://{addr}/ingester/ring?forget=ing-0"))
            .send()
            .await
            .unwrap();
        assert_eq!(post_resp.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
        let body = post_resp.text().await.unwrap();
        assert_eq!(
            body,
            "error forgetting instance 'ing-0': found empty ring when trying to unregister\n"
        );
        srv.abort();
    }
}
