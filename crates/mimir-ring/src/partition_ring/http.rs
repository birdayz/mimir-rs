//! HTTP status and admin handler for partition rings (`/ingester/partition-ring`).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/partition_ring_http.go
//! - github.com/grafana/dskit/ring/partition_ring_status.gohtml
//! - Go `strconv.Atoi` / `strconv.ParseBool` error messages used by the POST handler

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{RawQuery, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use chrono::{Datelike, Timelike};
use mimir_proto::ringpb::{PartitionState, PartitionTokenScheme};
use serde::{Deserialize, Deserializer, Serialize};

use super::instance_ring::PartitionRingReader;
use super::lifecycler::PartitionRingEditor;
use super::model::{clean_partition_state_name, clean_token_scheme_name};
use crate::dns::go_quote;
use crate::kv::memberlist::gofmt::GoJson;
use crate::ring::http::{go_format_g3, go_http_error, go_parse_query, html_escape, parse_go_form};

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionRingHttpResponse {
    pub partitions: Vec<PartitionPageData>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionPageData {
    pub id: i32,
    pub corrupted: bool,
    pub state: i32,
    pub state_timestamp: String,
    pub state_change_locked: bool,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub owner_ids: Vec<String>,
    pub token_scheme: i32,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub tokens: Vec<u32>,
}

#[derive(Deserialize)]
pub struct PartitionRingQuery {
    #[serde(default)]
    pub tokens: Option<String>,
}

#[derive(Clone)]
pub struct PartitionRingPageState {
    pub reader: Arc<dyn PartitionRingReader>,
    pub editor: Arc<PartitionRingEditor>,
}

pub fn partition_ring_router(
    reader: Arc<dyn PartitionRingReader>,
    editor: Arc<PartitionRingEditor>,
) -> Router {
    let state = PartitionRingPageState { reader, editor };
    Router::new()
        .route(
            "/ingester/partition-ring",
            get(handle_partition_ring_get).post(handle_partition_ring_post),
        )
        .with_state(state)
}

/// Serves the partition ring status page and editor at `path` only (Mimir registers one per read
/// compartment at `/ingester/partition-ring/compartment-<id>`, `pkg/api/api.go`).
pub fn partition_ring_router_at(
    path: &str,
    reader: Arc<dyn PartitionRingReader>,
    editor: Arc<PartitionRingEditor>,
) -> Router {
    let state = PartitionRingPageState { reader, editor };
    Router::new()
        .route(
            path,
            get(handle_partition_ring_get).post(handle_partition_ring_post),
        )
        .with_state(state)
}

pub async fn handle_partition_ring_get(
    State(state): State<PartitionRingPageState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let ring = state.reader.partition_ring();
    let desc = ring.desc();
    let owned_tokens = ring.count_tokens();

    let mut by_id: HashMap<i32, (PartitionPageData, i64)> =
        HashMap::with_capacity(desc.partitions.len());
    for (&id, part) in &desc.partitions {
        let tokens = match ring.partition_tokens(id) {
            Ok(t) => t.to_vec(),
            Err(err) => {
                return go_http_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string());
            }
        };
        let mut owners = ring.partition_owner_ids_copy(id);
        owners.sort();
        let ts = format_unix_secs_local_rfc3339(part.state_timestamp);
        by_id.insert(
            id,
            (
                PartitionPageData {
                    id,
                    corrupted: false,
                    state: part.state,
                    state_timestamp: ts,
                    state_change_locked: part.state_change_locked,
                    owner_ids: owners,
                    token_scheme: part.token_scheme,
                    tokens,
                },
                part.state_timestamp,
            ),
        );
    }

    for (owner_id, owner) in &desc.owners {
        let entry = by_id
            .entry(owner.owned_partition)
            .or_insert_with(|| {
                (
                    PartitionPageData {
                        id: owner.owned_partition,
                        corrupted: true,
                        state: PartitionState::PartitionUnknown as i32,
                        state_timestamp: "0001-01-01T00:00:00Z".to_string(),
                        state_change_locked: false,
                        owner_ids: vec![owner_id.clone()],
                        token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
                        tokens: Vec::new(),
                    },
                    0,
                )
            });
        if !entry.0.owner_ids.contains(owner_id) {
            entry.0.owner_ids.push(owner_id.clone());
        }
    }

    let mut partitions_with_ts: Vec<(PartitionPageData, i64)> = by_id.into_values().collect();
    partitions_with_ts.sort_by_key(|(p, _)| p.id);

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("application/json") {
        let items: Vec<GoJson> = partitions_with_ts
            .iter()
            .map(|(p, _)| partition_page_data_to_go_json(p))
            .collect();
        let body = GoJson::Object(vec![("partitions".to_string(), GoJson::Array(items))]).encode();
        return ([(header::CONTENT_TYPE, "application/json")], body).into_response();
    }

    let mut query_values = HashMap::new();
    let _ = go_parse_query(&mut query_values, raw_query.as_deref().unwrap_or(""));
    let show_tokens = query_values
        .get("tokens")
        .and_then(|v| v.first())
        .map(String::as_str)
        == Some("true");
    let html = render_partition_ring_html(&partitions_with_ts, &owned_tokens, show_tokens);
    ([(header::CONTENT_TYPE, "text/html")], html).into_response()
}

fn partition_page_data_to_go_json(p: &PartitionPageData) -> GoJson {
    let tokens = if p.corrupted || p.tokens.is_empty() {
        GoJson::Null
    } else {
        GoJson::Array(p.tokens.iter().map(|&t| GoJson::int(i128::from(t))).collect())
    };
    GoJson::Object(vec![
        ("id".to_string(), GoJson::int(i128::from(p.id))),
        ("corrupted".to_string(), GoJson::Bool(p.corrupted)),
        ("state".to_string(), GoJson::int(i128::from(p.state))),
        ("state_timestamp".to_string(), GoJson::str(&p.state_timestamp)),
        ("state_change_locked".to_string(), GoJson::Bool(p.state_change_locked)),
        ("owner_ids".to_string(), GoJson::strings(&p.owner_ids)),
        ("token_scheme".to_string(), GoJson::int(i128::from(p.token_scheme))),
        ("tokens".to_string(), tokens),
    ])
}

/// Go `PartitionRingPageHandler.handlePostRequest`: every parameter is read with
/// `req.FormValue` (URL query merged with an optional `application/x-www-form-urlencoded` body;
/// no `Content-Type` is required).
pub async fn handle_partition_ring_post(
    State(state): State<PartitionRingPageState>,
    req: Request,
) -> Response {
    let form = parse_go_form(req).await;
    let action = form.value("action");
    match action {
        "change_state" | "change_state_and_lock" => {
            let pid = match go_atoi(form.value("partition_id")) {
                Ok(id) => id as i32,
                Err(e) => {
                    return go_http_error(StatusCode::BAD_REQUEST, &format!("invalid partition ID: {e}"));
                }
            };
            let Some(to_state) = PartitionState::from_str_name(form.value("partition_state")) else {
                return go_http_error(StatusCode::BAD_REQUEST, "invalid partition state");
            };
            if let Err(err) = state.editor.change_partition_state(pid, to_state).await {
                return go_http_error(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to change partition state: {err}"),
                );
            }
            if action == "change_state_and_lock" {
                if let Err(err) = state.editor.set_partition_state_change_lock(pid, true).await {
                    return go_http_error(
                        StatusCode::BAD_REQUEST,
                        &format!("failed to lock partition state change: {err}"),
                    );
                }
            }
        }
        "state_change_lock" => {
            let pid = match go_atoi(form.value("partition_id")) {
                Ok(id) => id as i32,
                Err(e) => {
                    return go_http_error(StatusCode::BAD_REQUEST, &format!("invalid partition ID: {e}"));
                }
            };
            let locked = match go_parse_bool(form.value("locked")) {
                Ok(b) => b,
                Err(e) => {
                    return go_http_error(StatusCode::BAD_REQUEST, &format!("invalid locked value: {e}"));
                }
            };
            if let Err(err) = state
                .editor
                .set_partition_state_change_lock(pid, locked)
                .await
            {
                return go_http_error(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to lock partition state change: {err}"),
                );
            }
        }
        _ => {}
    }

    (StatusCode::FOUND, [(header::LOCATION, "#")], "").into_response()
}

/// Go `PartitionDesc.GetStateTime()` (`time.Unix(stateTimestamp, 0)` in `time.Local`) marshaled
/// via `time.Time.MarshalJSON` (`RFC3339Nano`: local offset, or `Z` when local offset is 0).
fn format_unix_secs_local_rfc3339(secs: i64) -> String {
    match chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0) {
        Some(utc) => {
            let local = utc.with_timezone(&chrono::Local).fixed_offset();
            crate::kv::memberlist::gofmt::format_rfc3339_nano(&local)
        }
        None => "1970-01-01T00:00:00Z".to_string(),
    }
}

/// Go `strconv.Atoi` (64-bit `int`), including its error messages. The caller truncates to
/// `int32` exactly like Go's `int32(partitionID)` conversion.
fn go_atoi(s: &str) -> Result<i64, String> {
    use std::num::IntErrorKind;
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.Atoi: parsing {}: invalid syntax", go_quote(s)));
    }
    s.parse::<i64>().map_err(|e| match e.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => {
            format!("strconv.Atoi: parsing {}: value out of range", go_quote(s))
        }
        _ => format!("strconv.Atoi: parsing {}: invalid syntax", go_quote(s)),
    })
}

/// Go `strconv.ParseBool`, including its error message.
fn go_parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {}: invalid syntax", go_quote(s))),
    }
}

#[cfg(unix)]
fn local_tz_abbr(unix_secs: i64) -> Option<String> {
    let t = unix_secs as libc::time_t;
    let mut tm = unsafe { std::mem::zeroed::<libc::tm>() };
    if unsafe { libc::localtime_r(&t, &mut tm) }.is_null() || tm.tm_zone.is_null() {
        return None;
    }
    let cstr = unsafe { std::ffi::CStr::from_ptr(tm.tm_zone) };
    cstr.to_str().ok().filter(|s| !s.is_empty()).map(str::to_string)
}

#[cfg(not(unix))]
fn local_tz_abbr(_unix_secs: i64) -> Option<String> {
    None
}

/// Go `ts.Format("2006-01-02 15:04:05 MST")`.
fn format_partition_timestamp(unix_secs: i64) -> String {
    match chrono::DateTime::<chrono::Utc>::from_timestamp(unix_secs, 0) {
        Some(utc) => {
            let local = utc.with_timezone(&chrono::Local);
            let off = local.offset().local_minus_utc();
            let tz_str = local_tz_abbr(unix_secs).unwrap_or_else(|| {
                if off == 0 {
                    "UTC".to_string()
                } else {
                    let sign = if off < 0 { '-' } else { '+' };
                    let a = off.unsigned_abs();
                    format!("{sign}{:02}{:02}", a / 3600, (a / 60) % 60)
                }
            });
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02} {tz_str}",
                local.year(),
                local.month(),
                local.day(),
                local.hour(),
                local.minute(),
                local.second()
            )
        }
        None => "N/A".to_string(),
    }
}

fn render_partition_ring_html(
    partitions: &[(PartitionPageData, i64)],
    owned_tokens: &HashMap<i32, i64>,
    show_tokens: bool,
) -> String {
    let mut out = String::from(
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>Partitions Ring Status</title>\n</head>\n<body>\n    <h1>Partitions Ring Status</h1>\n",
    );
    out.push_str("    <table width=\"100%\" border=\"1\">\n        <thead>\n            <tr>\n");
    out.push_str("                <th>Partition ID</th>\n                <th>State</th>\n                <th>State updated at</th>\n                <th>Owners</th>\n                <th>Token scheme</th>\n                <th>Tokens</th>\n                <th>Ownership</th>\n                <th>Actions</th>\n            </tr>\n        </thead>\n        <tbody>\n");

    for (p, state_ts_secs) in partitions {
        let row_attr = if p.corrupted {
            " bgcolor=\"#FFDEDE\""
        } else if p.id % 2 == 0 {
            " bgcolor=\"#BEBEBE\""
        } else {
            ""
        };
        out.push_str(&format!("            <tr {row_attr}>\n"));
        out.push_str(&format!("                <td>{}</td>\n", p.id));
        let pstate = PartitionState::try_from(p.state).unwrap_or(PartitionState::PartitionUnknown);
        if p.corrupted {
            out.push_str("                <td>Corrupt</td>\n");
        } else {
            out.push_str(&format!(
                "                <td>{}</td>\n",
                html_escape(clean_partition_state_name(pstate))
            ));
        }
        if !p.corrupted {
            out.push_str(&format!(
                "                <td>{}</td>\n",
                html_escape(&format_partition_timestamp(*state_ts_secs))
            ));
        } else {
            out.push_str("                <td>N/A</td>\n");
        }
        out.push_str("                <td>\n");
        for o in &p.owner_ids {
            out.push_str(&format!("                    {} <br />\n", html_escape(o)));
        }
        out.push_str("                </td>\n");
        let scheme = PartitionTokenScheme::try_from(p.token_scheme)
            .unwrap_or(PartitionTokenScheme::PartitionTokensStored);
        if p.corrupted {
            out.push_str("                <td></td>\n");
        } else {
            out.push_str(&format!(
                "                <td>{}</td>\n",
                html_escape(clean_token_scheme_name(scheme))
            ));
        }
        let owned = owned_tokens.get(&p.id).copied().unwrap_or(0);
        let pct = (owned as f64 / u32::MAX as f64) * 100.0;
        out.push_str(&format!(
            "                <td>{}</td>\n                <td>{}%</td>\n",
            p.tokens.len(),
            go_format_g3(pct)
        ));
        out.push_str("                <td>\n");
        if !p.corrupted {
            let next_state = match pstate {
                PartitionState::PartitionPending => Some(PartitionState::PartitionActive),
                PartitionState::PartitionActive => Some(PartitionState::PartitionInactive),
                PartitionState::PartitionInactive => Some(PartitionState::PartitionActive),
                _ => None,
            };
            if let Some(ns) = next_state {
                let to_name = clean_partition_state_name(ns);
                let disabled = if p.state_change_locked {
                    " disabled title=\"State change is locked\""
                } else {
                    ""
                };
                out.push_str(&format!(
                    "                    <form action=\"\" method=\"POST\">\n                        <input type=\"hidden\" name=\"csrf_token\" value=\"$__CSRF_TOKEN_PLACEHOLDER__\">\n                        <input type=\"hidden\" name=\"partition_id\" value=\"{}\">\n                        <input type=\"hidden\" name=\"partition_state\" value=\"{}\">\n                        <button name=\"action\" value=\"change_state\" type=\"submit\"{disabled} onclick=\"return confirm('Do you confirm you want to change the state to {to_name}?');\">Change state to {to_name}</button>\n                        <button name=\"action\" value=\"change_state_and_lock\" type=\"submit\"{disabled} onclick=\"return confirm('Do you confirm you want to change the state to {to_name} and lock it?');\">Change state to {to_name} & Lock</button>\n                    </form>\n",
                    p.id,
                    ns.as_str_name(),
                ));
            }
            let lock_next = !p.state_change_locked;
            let lock_verb = if p.state_change_locked { "unlock" } else { "lock" };
            let lock_label = if p.state_change_locked {
                "Unlock State Change"
            } else {
                "Lock State Change"
            };
            out.push_str(&format!(
                "                    <form action=\"\" method=\"POST\" style=\"margin-top: 5px;\" onsubmit=\"return confirm('Do you confirm you want to {lock_verb} the state change?');\">\n                        <input type=\"hidden\" name=\"csrf_token\" value=\"$__CSRF_TOKEN_PLACEHOLDER__\">\n                        <input type=\"hidden\" name=\"partition_id\" value=\"{}\">\n                        <input type=\"hidden\" name=\"locked\" value=\"{lock_next}\">\n                        <button name=\"action\" value=\"state_change_lock\" type=\"submit\">{lock_label}</button>\n                    </form>\n",
                p.id
            ));
        }
        out.push_str("                </td>\n            </tr>\n");
    }

    out.push_str("        </tbody>\n    </table>\n    <br>\n");
    if show_tokens {
        out.push_str("    <input type=\"button\" value=\"Hide Tokens\" onclick=\"window.location.href = '?tokens=false'\"/>\n");
        for (p, _) in partitions {
            out.push_str(&format!("    <h2>Partition: {}</h2>\n    <p>\n        Tokens:<br/>\n", p.id));
            for t in &p.tokens {
                out.push_str(&format!("            {t}\n"));
            }
            out.push_str("    </p>\n");
        }
    } else {
        out.push_str("    <input type=\"button\" value=\"Show Tokens\" onclick=\"window.location.href = '?tokens=true'\"/>\n");
    }
    out.push_str("</body>\n</html>\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use crate::kv::inmemory::InMemoryKv;
    use crate::kv::KvClient;
    use crate::partition_ring::model::{partition_ring_desc_codec, PartitionRingDescExt};
    use crate::partition_ring::ring::PartitionRing;
    use axum::body::Body;
    use mimir_proto::ringpb::PartitionRingDesc;

    struct Static(Arc<PartitionRing>);
    impl PartitionRingReader for Static {
        fn partition_ring(&self) -> Arc<PartitionRing> {
            Arc::clone(&self.0)
        }
    }

    async fn setup() -> (PartitionRingPageState, Arc<InMemoryKv>) {
        let kv = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let mut desc = PartitionRingDesc::default();
        desc.add_partition(0, PartitionState::PartitionActive, SystemTime::now());
        kv.put_any("ingester-partitions", &desc).await.unwrap();
        let reader = Arc::new(Static(PartitionRing::new(desc).unwrap()));
        let editor = Arc::new(PartitionRingEditor::new(
            "ingester-partitions",
            Arc::clone(&kv) as Arc<dyn KvClient>,
        ));
        (PartitionRingPageState { reader, editor }, kv)
    }

    async fn stored(kv: &InMemoryKv) -> PartitionRingDesc {
        kv.get("ingester-partitions")
            .await
            .unwrap()
            .unwrap()
            .downcast_ref::<PartitionRingDesc>()
            .unwrap()
            .clone()
    }

    async fn post(
        app: &PartitionRingPageState,
        uri: &str,
        ct: Option<&str>,
        body: &str,
    ) -> (StatusCode, String) {
        let mut b = axum::http::Request::builder().method("POST").uri(uri);
        if let Some(ct) = ct {
            b = b.header(header::CONTENT_TYPE, ct);
        }
        let resp = handle_partition_ring_post(
            State(app.clone()),
            b.body(Body::from(body.to_string())).unwrap(),
        )
        .await;
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn post_accepts_query_params_without_content_type() {
        let (app, kv) = setup().await;
        let (code, _) = post(
            &app,
            "/ingester/partition-ring?action=change_state&partition_id=0&partition_state=PartitionInactive",
            None,
            "",
        )
        .await;
        assert_eq!(code, StatusCode::FOUND);
        assert_eq!(
            stored(&kv).await.partitions[&0].state,
            PartitionState::PartitionInactive as i32
        );

        // Body values win over query values (Go `r.Form` order).
        let (code, _) = post(
            &app,
            "/ingester/partition-ring?locked=false",
            Some("application/x-www-form-urlencoded"),
            "action=state_change_lock&partition_id=0&locked=T",
        )
        .await;
        assert_eq!(code, StatusCode::FOUND);
        assert!(stored(&kv).await.partitions[&0].state_change_locked);
    }

    #[tokio::test]
    async fn post_errors_match_go() {
        let (app, _kv) = setup().await;
        let (code, body) = post(&app, "/ingester/partition-ring?action=change_state&partition_id=x", None, "").await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(body, "invalid partition ID: strconv.Atoi: parsing \"x\": invalid syntax\n");

        let (code, body) = post(
            &app,
            "/ingester/partition-ring?action=change_state&partition_id=0&partition_state=Bogus",
            None,
            "",
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(body, "invalid partition state\n");

        let (code, body) = post(
            &app,
            "/ingester/partition-ring?action=state_change_lock&partition_id=0&locked=yes",
            None,
            "",
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(body, "invalid locked value: strconv.ParseBool: parsing \"yes\": invalid syntax\n");

        let (code, body) = post(
            &app,
            "/ingester/partition-ring?action=state_change_lock&partition_id=7&locked=true",
            None,
            "",
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "failed to lock partition state change: the partition does not exist\n"
        );

        let (code, body) = post(
            &app,
            "/ingester/partition-ring?action=change_state&partition_id=7&partition_state=PartitionInactive",
            None,
            "",
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            "failed to change partition state: the partition does not exist\n"
        );

        // Unknown / missing action: plain redirect.
        let (code, _) = post(&app, "/ingester/partition-ring", None, "").await;
        assert_eq!(code, StatusCode::FOUND);
    }

    #[test]
    fn go_atoi_and_parse_bool() {
        assert_eq!(go_atoi("+7"), Ok(7));
        assert_eq!(go_atoi("-3"), Ok(-3));
        assert_eq!(
            go_atoi("99999999999999999999"),
            Err("strconv.Atoi: parsing \"99999999999999999999\": value out of range".to_string())
        );
        assert_eq!(go_atoi(""), Err("strconv.Atoi: parsing \"\": invalid syntax".to_string()));
        // int32(partitionID) truncation.
        assert_eq!(go_atoi("4294967296").map(|v| v as i32), Ok(0));
        assert_eq!(go_parse_bool("1"), Ok(true));
        assert_eq!(go_parse_bool("False"), Ok(false));
        assert!(go_parse_bool("tRuE").is_err());
    }

    #[tokio::test]
    async fn get_json_formats_state_timestamp_in_local_timezone() {
        let kv = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let mut desc = PartitionRingDesc::default();
        let fixed_secs = 1_794_898_593i64;
        desc.add_partition(
            0,
            PartitionState::PartitionActive,
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(fixed_secs as u64),
        );
        // Add an owner pointing to non-existent partition 9 -> corrupted entry (`time.Time{}`).
        desc.owners.insert(
            "orphan-owner".to_string(),
            mimir_proto::ringpb::OwnerDesc {
                owned_partition: 9,
                updated_timestamp: fixed_secs,
                ..Default::default()
            },
        );
        kv.put_any("ingester-partitions", &desc).await.unwrap();
        let reader = Arc::new(Static(PartitionRing::new(desc).unwrap()));
        let editor = Arc::new(PartitionRingEditor::new(
            "ingester-partitions",
            Arc::clone(&kv) as Arc<dyn KvClient>,
        ));
        let app = PartitionRingPageState { reader, editor };

        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, "application/json".parse().unwrap());
        let resp = handle_partition_ring_get(
            State(app),
            headers,
            RawQuery(None),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let parsed: PartitionRingHttpResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.partitions.len(), 2);

        let expected_local = crate::kv::memberlist::gofmt::format_rfc3339_nano(
            &chrono::DateTime::<chrono::Utc>::from_timestamp(fixed_secs, 0)
                .unwrap()
                .with_timezone(&chrono::Local)
                .fixed_offset(),
        );
        assert_eq!(parsed.partitions[0].id, 0);
        assert_eq!(parsed.partitions[0].state_timestamp, expected_local);
        assert_eq!(parsed.partitions[1].id, 9);
        assert!(parsed.partitions[1].corrupted);
        assert_eq!(parsed.partitions[1].state_timestamp, "0001-01-01T00:00:00Z");
    }
}
