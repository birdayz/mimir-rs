//! HTTP status & admin handler for classic hash rings (`/ingester/ring`).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/ring_http.go
//! - github.com/grafana/dskit/ring/ring_status.gohtml

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Form, Json, Router};
use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_proto::ringpb::{Desc, InstanceDesc};
use serde::{Deserialize, Serialize};

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
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub versions: HashMap<u64, u64>,
}

#[derive(Deserialize)]
pub struct RingQuery {
    #[serde(default)]
    pub tokens: Option<String>,
}

#[derive(Deserialize)]
pub struct ForgetForm {
    #[serde(default)]
    pub forget: String,
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
        .route("/ingester/ring", get(handle_ring_get).post(handle_ring_post))
        .route("/ring", get(handle_ring_get).post(handle_ring_post))
        .with_state(state)
}

pub async fn handle_ring_get(
    State(state): State<RingPageState>,
    headers: HeaderMap,
    axum::extract::Query(query): axum::extract::Query<RingQuery>,
) -> Response {
    let desc = state.ring.desc_snapshot();
    let hb_timeout = state.ring.heartbeat_timeout();
    let now = SystemTime::now();
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
            timestamp: format_unix_secs_rfc3339(ing.timestamp),
            registered_timestamp: ing
                .get_registered_at()
                .map(format_rfc3339_nano_utc)
                .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string()),
            read_only: ro,
            read_only_updated_timestamp: ro_ts
                .map(format_rfc3339_nano_utc)
                .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_string()),
            zone: ing.zone.clone(),
            tokens: ing.tokens.clone(),
            versions: ing.versions.clone(),
        });
    }

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("application/json") {
        let resp = RingHttpResponse {
            shards,
            now: format_rfc3339_nano_utc(now),
        };
        return Json(resp).into_response();
    }

    let show_tokens = query.tokens.as_deref() == Some("true");
    let html = render_ring_html(&shards, &owned_tokens, now, show_tokens, &state.config, &desc);
    Html(html).into_response()
}

pub async fn handle_ring_post(
    State(state): State<RingPageState>,
    Form(form): Form<ForgetForm>,
) -> Response {
    let Some(kv) = state.ring.kv_client() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "ring has no KV client configured",
        )
            .into_response();
    };
    let forget_id = form.forget.clone();
    let res = kv
        .cas(
            state.ring.key(),
            Box::new(move |cur| {
                let Some(any) = cur else {
                    return Err(CasCallbackError::fatal(KvError::NotFound(
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
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("error forgetting instance '{}': {err}", form.forget),
        )
            .into_response(),
    }
}

fn format_unix_secs_rfc3339(secs: i64) -> String {
    if secs <= 0 {
        return "1970-01-01T00:00:00Z".to_string();
    }
    format_rfc3339_nano_utc(std::time::UNIX_EPOCH + Duration::from_secs(secs as u64))
}

fn render_ring_html(
    shards: &[IngesterHttpDesc],
    owned_tokens: &HashMap<String, i64>,
    now: SystemTime,
    show_tokens: bool,
    config: &StatusPageConfig,
    desc: &Desc,
) -> String {
    let mut out = String::from(
        "<!DOCTYPE html>\n<html>\n<head><meta charset=\"UTF-8\"><title>Ring Status</title></head>\n<body>\n<h1>Ring Status</h1>\n",
    );
    out.push_str(&format!("<p>Current time: {}</p>\n", format_rfc3339_nano_utc(now)));
    out.push_str("<form action=\"\" method=\"POST\">\n");
    out.push_str("<table width=\"100%\" border=\"1\">\n<thead><tr>\n");
    out.push_str("<th>Instance ID</th><th>Availability Zone</th><th>State</th><th>Address</th><th>Registered At</th><th>Read-Only</th><th>Read-Only Updated</th><th>Last Heartbeat</th>");
    if !config.hide_tokens_ui_elements {
        out.push_str("<th>Tokens</th><th>Ownership</th>");
    }
    if config.show_versions {
        out.push_str("<th>Versions</th>");
    }
    out.push_str("<th>Actions</th></tr></thead>\n<tbody>\n");

    for (i, s) in shards.iter().enumerate() {
        let row_attr = if i % 2 == 0 { "" } else { " bgcolor=\"#BEBEBE\"" };
        out.push_str(&format!("<tr{row_attr}>"));
        let link_html = config
            .instance_link
            .as_ref()
            .and_then(|f| desc.ingesters.get(&s.id).map(|d| f(&s.id, d)))
            .filter(|l| !l.is_empty());
        match link_html {
            Some(l) => out.push_str(&format!(
                "<td><a href=\"{}\">{}</a></td>",
                html_url_attr_escape(&l),
                html_escape(&s.id)
            )),
            None => out.push_str(&format!("<td>{}</td>", html_escape(&s.id))),
        }
        out.push_str(&format!("<td>{}</td>", html_escape(&s.zone)));
        out.push_str(&format!("<td>{}</td>", html_escape(&s.state)));
        out.push_str(&format!("<td>{}</td>", html_escape(&s.address)));
        out.push_str(&format!("<td>{}</td>", html_escape(&s.registered_timestamp)));
        out.push_str(&format!("<td>{}</td>", s.read_only));
        out.push_str(&format!("<td>{}</td>", html_escape(&s.read_only_updated_timestamp)));
        out.push_str(&format!("<td>{}</td>", html_escape(&s.timestamp)));
        if !config.hide_tokens_ui_elements {
            let owned = owned_tokens.get(&s.id).copied().unwrap_or(0);
            let pct = (owned as f64 / u32::MAX as f64) * 100.0;
            out.push_str(&format!("<td>{}</td><td>{:.3}%</td>", s.tokens.len(), pct));
        }
        if config.show_versions {
            out.push_str("<td>");
            for (k, v) in &s.versions {
                if let Some(cname) = config.component_names.get(k) {
                    out.push_str(&format!("{k} ({}): v{v}<br/>", html_escape(cname)));
                } else {
                    out.push_str(&format!("{k}: v{v}<br/>"));
                }
            }
            out.push_str("</td>");
        }
        out.push_str(&format!(
            "<td><button name=\"forget\" value=\"{}\" type=\"submit\">Forget</button></td></tr>\n",
            html_escape(&s.id)
        ));
    }
    out.push_str("</tbody></table><br>\n");
    if !config.hide_tokens_ui_elements {
        if show_tokens {
            out.push_str("<input type=\"button\" value=\"Hide Tokens\" onclick=\"window.location.href = '?tokens=false'\"/>\n");
            for s in shards {
                out.push_str(&format!("<h2>Instance: {}</h2><p>Tokens:<br/>", html_escape(&s.id)));
                for t in &s.tokens {
                    out.push_str(&format!("{t} "));
                }
                out.push_str("</p>\n");
            }
        } else {
            out.push_str("<input type=\"button\" value=\"Show Tokens\" onclick=\"window.location.href = '?tokens=true'\"/>\n");
        }
    }
    out.push_str("</form></body></html>\n");
    out
}

/// Go `html/template` `htmlEscaper` / `attrEscaper` (`htmlReplacementTable`).
fn html_escape(s: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

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
            versions: HashMap::new(),
        };
        let shards = [shard("ingester-1"), shard("ingester-2")];
        let mut cfg = mimir_config::ingester::RingConfig {
            status_page_ingester_link_template: "http://{host}:8080/{ingester}/x?a=1&b=2".to_string(),
            ..Default::default()
        };
        let page = render_ring_html(
            &shards,
            &HashMap::new(),
            SystemTime::now(),
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
            SystemTime::now(),
            false,
            &super::super::to_status_page_config(&cfg),
            &desc,
        );
        assert!(page.contains("<td>ingester-1</td>") && !page.contains("<a href"), "{page}");
    }
}
