//! HTTP status and admin handler for partition rings (`/ingester/partition-ring`).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/partition_ring_http.go
//! - github.com/grafana/dskit/ring/partition_ring_status.gohtml
//! - Go `strconv.Atoi` / `strconv.ParseBool` error messages used by the POST handler

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_proto::ringpb::{PartitionState, PartitionTokenScheme};
use serde::{Deserialize, Serialize};

use super::instance_ring::PartitionRingReader;
use super::lifecycler::PartitionRingEditor;
use super::model::{clean_partition_state_name, clean_token_scheme_name};
use crate::dns::go_quote;
use crate::ring::http::{go_http_error, parse_go_form};

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
    pub owner_ids: Vec<String>,
    pub token_scheme: i32,
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
        .route(
            "/partition-ring",
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
    axum::extract::Query(query): axum::extract::Query<PartitionRingQuery>,
) -> Response {
    let ring = state.reader.partition_ring();
    let desc = ring.desc();
    let owned_tokens = ring.count_tokens();

    let mut by_id: HashMap<i32, PartitionPageData> = HashMap::with_capacity(desc.partitions.len());
    for (&id, part) in &desc.partitions {
        let tokens = match ring.partition_tokens(id) {
            Ok(t) => t.to_vec(),
            Err(err) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
            }
        };
        let mut owners = ring.partition_owner_ids_copy(id);
        owners.sort();
        let ts = if part.state_timestamp > 0 {
            format_rfc3339_nano_utc(UNIX_EPOCH + Duration::from_secs(part.state_timestamp as u64))
        } else {
            "1970-01-01T00:00:00Z".to_string()
        };
        by_id.insert(
            id,
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
        );
    }

    for (owner_id, owner) in &desc.owners {
        let entry = by_id
            .entry(owner.owned_partition)
            .or_insert_with(|| PartitionPageData {
                id: owner.owned_partition,
                corrupted: true,
                state: PartitionState::PartitionUnknown as i32,
                state_timestamp: "0001-01-01T00:00:00Z".to_string(),
                state_change_locked: false,
                owner_ids: vec![owner_id.clone()],
                token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
                tokens: Vec::new(),
            });
        if !entry.owner_ids.contains(owner_id) {
            entry.owner_ids.push(owner_id.clone());
        }
    }

    let mut partitions: Vec<PartitionPageData> = by_id.into_values().collect();
    partitions.sort_by_key(|p| p.id);

    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("application/json") {
        return Json(PartitionRingHttpResponse { partitions }).into_response();
    }

    let show_tokens = query.tokens.as_deref() == Some("true");
    let html = render_partition_ring_html(&partitions, &owned_tokens, show_tokens);
    Html(html).into_response()
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

fn render_partition_ring_html(
    partitions: &[PartitionPageData],
    owned_tokens: &HashMap<i32, i64>,
    show_tokens: bool,
) -> String {
    let mut out = String::from(
        "<!DOCTYPE html>\n<html>\n<head><meta charset=\"UTF-8\"><title>Partitions Ring Status</title></head>\n<body>\n<h1>Partitions Ring Status</h1>\n",
    );
    out.push_str("<table width=\"100%\" border=\"1\">\n<thead><tr>\n");
    out.push_str("<th>Partition ID</th><th>State</th><th>State updated at</th><th>Owners</th><th>Token scheme</th><th>Tokens</th><th>Ownership</th><th>Actions</th></tr></thead>\n<tbody>\n");

    for p in partitions {
        let row_attr = if p.corrupted {
            " bgcolor=\"#FFDEDE\""
        } else if p.id % 2 != 0 {
            " bgcolor=\"#BEBEBE\""
        } else {
            ""
        };
        out.push_str(&format!("<tr{row_attr}>"));
        out.push_str(&format!("<td>{}</td>", p.id));
        let pstate = PartitionState::try_from(p.state).unwrap_or(PartitionState::PartitionUnknown);
        if p.corrupted {
            out.push_str("<td>Corrupt</td>");
        } else {
            out.push_str(&format!("<td>{}</td>", clean_partition_state_name(pstate)));
        }
        out.push_str(&format!("<td>{}</td>", p.state_timestamp));
        out.push_str("<td>");
        for o in &p.owner_ids {
            out.push_str(&format!("{o}<br/>"));
        }
        out.push_str("</td>");
        let scheme = PartitionTokenScheme::try_from(p.token_scheme)
            .unwrap_or(PartitionTokenScheme::PartitionTokensStored);
        if p.corrupted {
            out.push_str("<td></td>");
        } else {
            out.push_str(&format!("<td>{}</td>", clean_token_scheme_name(scheme)));
        }
        let owned = owned_tokens.get(&p.id).copied().unwrap_or(0);
        let pct = (owned as f64 / u32::MAX as f64) * 100.0;
        out.push_str(&format!("<td>{}</td><td>{pct:.3}%</td>", p.tokens.len()));
        out.push_str("<td>");
        if !p.corrupted {
            let next_state = match pstate {
                PartitionState::PartitionPending => Some(PartitionState::PartitionActive),
                PartitionState::PartitionActive => Some(PartitionState::PartitionInactive),
                PartitionState::PartitionInactive => Some(PartitionState::PartitionActive),
                _ => None,
            };
            if let Some(ns) = next_state {
                let disabled = if p.state_change_locked { " disabled" } else { "" };
                out.push_str(&format!(
                    "<form action=\"\" method=\"POST\"><input type=\"hidden\" name=\"partition_id\" value=\"{}\"><input type=\"hidden\" name=\"partition_state\" value=\"{}\"><button name=\"action\" value=\"change_state\" type=\"submit\"{disabled}>Change state to {}</button></form>",
                    p.id,
                    ns.as_str_name(),
                    clean_partition_state_name(ns)
                ));
            }
            let lock_next = !p.state_change_locked;
            let lock_label = if p.state_change_locked {
                "Unlock State Change"
            } else {
                "Lock State Change"
            };
            out.push_str(&format!(
                "<form action=\"\" method=\"POST\"><input type=\"hidden\" name=\"partition_id\" value=\"{}\"><input type=\"hidden\" name=\"locked\" value=\"{lock_next}\"><button name=\"action\" value=\"state_change_lock\" type=\"submit\">{lock_label}</button></form>",
                p.id
            ));
        }
        out.push_str("</td></tr>\n");
    }

    out.push_str("</tbody></table><br>\n");
    if show_tokens {
        out.push_str("<input type=\"button\" value=\"Hide Tokens\" onclick=\"window.location.href = '?tokens=false'\"/>\n");
        for p in partitions {
            out.push_str(&format!("<h2>Partition: {}</h2><p>Tokens:<br/>", p.id));
            for t in &p.tokens {
                out.push_str(&format!("{t} "));
            }
            out.push_str("</p>\n");
        }
    } else {
        out.push_str("<input type=\"button\" value=\"Show Tokens\" onclick=\"window.location.href = '?tokens=true'\"/>\n");
    }
    out.push_str("</body></html>\n");
    let _ = SystemTime::now();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
