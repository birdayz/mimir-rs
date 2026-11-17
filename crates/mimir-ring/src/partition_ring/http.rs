//! HTTP status and admin handler for partition rings (`/ingester/partition-ring`).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/partition_ring_http.go
//! - github.com/grafana/dskit/ring/partition_ring_status.gohtml

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Form, Json, Router};
use mimir_common::logging::format_rfc3339_nano_utc;
use mimir_proto::ringpb::{PartitionState, PartitionTokenScheme};
use serde::{Deserialize, Serialize};

use super::instance_ring::PartitionRingReader;
use super::lifecycler::PartitionRingEditor;
use super::model::{clean_partition_state_name, clean_token_scheme_name};

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

#[derive(Deserialize)]
pub struct PartitionActionForm {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub partition_id: String,
    #[serde(default)]
    pub partition_state: String,
    #[serde(default)]
    pub locked: String,
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

pub async fn handle_partition_ring_post(
    State(state): State<PartitionRingPageState>,
    Form(form): Form<PartitionActionForm>,
) -> Response {
    match form.action.as_str() {
        "change_state" | "change_state_and_lock" => {
            let Ok(pid) = form.partition_id.parse::<i32>() else {
                return (StatusCode::BAD_REQUEST, "invalid partition ID").into_response();
            };
            let Some(to_state) = PartitionState::from_str_name(&form.partition_state) else {
                return (StatusCode::BAD_REQUEST, "invalid partition state").into_response();
            };
            if let Err(err) = state.editor.change_partition_state(pid, to_state).await {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("failed to change partition state: {err}"),
                )
                    .into_response();
            }
            if form.action == "change_state_and_lock" {
                if let Err(err) = state.editor.set_partition_state_change_lock(pid, true).await {
                    return (
                        StatusCode::BAD_REQUEST,
                        format!("failed to lock partition state change: {err}"),
                    )
                        .into_response();
                }
            }
        }
        "state_change_lock" => {
            let Ok(pid) = form.partition_id.parse::<i32>() else {
                return (StatusCode::BAD_REQUEST, "invalid partition ID").into_response();
            };
            let Ok(locked) = form.locked.parse::<bool>() else {
                return (StatusCode::BAD_REQUEST, "invalid locked value").into_response();
            };
            if let Err(err) = state
                .editor
                .set_partition_state_change_lock(pid, locked)
                .await
            {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("failed to lock partition state change: {err}"),
                )
                    .into_response();
            }
        }
        _ => {}
    }

    (StatusCode::FOUND, [(header::LOCATION, "#")], "").into_response()
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
