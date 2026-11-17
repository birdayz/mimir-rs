//! Axum HTTP admin & lifecycle router matching `pkg/api/api.go:RegisterIngester`.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/api/api.go (`RegisterIngester`), pkg/api/tenant.go
//!   (`newTenantValidationMiddleware`), dskit/middleware/http_auth.go (`AuthenticateUser`)
//! - github.com/grafana/mimir/pkg/ingester/ingester_http.go (`FlushHandler`)
//! - github.com/grafana/mimir/pkg/ingester/tenants_http.go, tenants.gohtml, tenant_tsdb.gohtml
//!
//! Endpoints:
//! - `/ingester/flush` (GET, POST)
//! - `/ingester/prepare-shutdown` (GET, POST, DELETE)
//! - `/ingester/prepare-partition-downscale` (GET, POST, DELETE)
//! - `/ingester/prepare-instance-ring-downscale` (GET, POST, DELETE)
//! - `/ingester/unregister-on-shutdown` (GET, PUT, DELETE)
//! - `/ingester/shutdown` (POST)
//! - `/ingester/tsdb_metrics` (GET)
//! - `/ingester/tenants` (GET)
//! - `/ingester/tsdb/:tenant` (GET)
//! - `/ingester/label_churn/:tenant` (GET)
//! - `/ingester/ring` (GET, POST - when classic Ring is configured)
//! - `/ingester/partition-ring` (GET, POST - when PartitionRingWatcher is configured)

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State as AxumState};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use mimir_common::services::State;
use mimir_common::shutdownmarker;
use mimir_common::tenant::{parse_single_tenant_id, parse_tenant_ids, ORG_ID_HEADER_NAME};
use mimir_config::{parse_go_duration, GoDuration};
use mimir_proto::ringpb::PartitionState;
use mimir_ring::partition_ring::http::partition_ring_router;
use mimir_ring::partition_ring::{PartitionRingEditor, PartitionRingError, PartitionRingReader};
use mimir_ring::kv::memberlist::gofmt::{format_go_time_string, GoJson};
use mimir_ring::kv::memberlist::http_status::html_escape;
use mimir_ring::ring::http::{go_http_error, parse_go_form, ring_router, StatusPageConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::ingester::{Ingester, TriggerRequest};
use crate::label_churn::{
    compute_label_churn, LabelChurnError, DEFAULT_LABEL_CHURN_LIMIT,
    DEFAULT_LABEL_CHURN_LOOKBACK,
};

#[derive(Clone)]
pub struct HttpState {
    pub ingester: Arc<Ingester>,
}

pub fn ingester_router(ingester: Arc<Ingester>) -> Router {
    let state = HttpState {
        ingester: Arc::clone(&ingester),
    };

    let mut router = Router::new()
        .route("/ingester/flush", get(flush_handler).post(flush_handler))
        .route(
            "/ingester/prepare-shutdown",
            get(prepare_shutdown_handler)
                .post(prepare_shutdown_handler)
                .delete(prepare_shutdown_handler),
        )
        .route(
            "/ingester/prepare-partition-downscale",
            get(prepare_partition_downscale_handler)
                .post(prepare_partition_downscale_handler)
                .delete(prepare_partition_downscale_handler),
        )
        .route(
            "/ingester/prepare-instance-ring-downscale",
            get(prepare_instance_ring_downscale_handler)
                .post(prepare_instance_ring_downscale_handler)
                .delete(prepare_instance_ring_downscale_handler),
        )
        .route(
            "/ingester/unregister-on-shutdown",
            get(prepare_unregister_handler)
                .put(prepare_unregister_handler)
                .delete(prepare_unregister_handler),
        )
        .route("/ingester/shutdown", post(shutdown_handler))
        .route("/ingester/tsdb_metrics", get(user_registry_handler))
        .route("/ingester/tenants", get(tenants_handler))
        .route("/ingester/tsdb/:tenant", get(tenant_tsdb_handler))
        .route("/ingester/label_churn/:tenant", get(label_churn_handler))
        .with_state(state);

    if let Some(ref ring) = ingester.instance_ring {
        router = router.merge(ring_router(Arc::clone(ring), StatusPageConfig::default()));
    }

    if let Some(ref watcher) = ingester.partition_ring_watcher {
        // Go: `RegisterIngesterPartitionRings(..., PartitionRingKey, kvClient)` with the
        // partition ring watcher's KV client (partition_ring.kvstore, partition ring codec).
        let editor = Arc::new(PartitionRingEditor::new(
            watcher.key(),
            Arc::clone(watcher.kv_client()),
        ));
        let reader: Arc<dyn PartitionRingReader> = Arc::clone(watcher) as Arc<dyn PartitionRingReader>;
        router = router.merge(partition_ring_router(reader, editor));
    }

    router
}

/// Go `Ingester.FlushHandler`: `r.ParseForm()` (URL query plus an
/// `application/x-www-form-urlencoded` body; `400` on a parse error), the tenant allow-list is
/// `r.Form["tenant"]` (an empty `tenant=` allows only the empty tenant, an absent one allows all)
/// and the flush is synchronous only when the first `wait` value is `"true"`.
async fn flush_handler(AxumState(st): AxumState<HttpState>, req: Request) -> Response {
    let ing_state = st.ingester.state();
    if ing_state != State::Running && ing_state != State::Starting {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("ingester is unavailable (current state: {ing_state})"),
        )
            .into_response();
    }

    let Some(fc_tx) = st.ingester.force_compact_tx.read().clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "compaction service is not running",
        )
            .into_response();
    };
    let form = parse_go_form(req).await;
    if let Some(err) = form.error() {
        tracing::warn!(err = %err, "failed to parse HTTP request in flush handler");
        return StatusCode::BAD_REQUEST.into_response();
    }
    // `util.NewAllowList(r.Form["tenant"], nil)`: an empty list allows every tenant.
    let users: Option<HashSet<String>> = form
        .get("tenant")
        .filter(|t| !t.is_empty())
        .map(|t| t.iter().cloned().collect());
    let wait = form.get("wait").and_then(|w| w.first()).is_some_and(|w| w == "true");
    let ship_tx_opt = st.ingester.ship_tx.read().clone();
    let ing = Arc::clone(&st.ingester);

    let run = async move {
        ing.num_compactions_in_progress
            .fetch_add(1, Ordering::AcqRel);
        let (cb_tx, cb_rx) = oneshot::channel();
        if fc_tx
            .send(TriggerRequest {
                users: users.clone(),
                callback: cb_tx,
            })
            .await
            .is_ok()
        {
            let _ = cb_rx.await;
        }
        if let Some(ship_tx) = ship_tx_opt {
            let (scb_tx, scb_rx) = oneshot::channel();
            if ship_tx
                .send(TriggerRequest {
                    users,
                    callback: scb_tx,
                })
                .await
                .is_ok()
            {
                let _ = scb_rx.await;
            }
        }
        ing.num_compactions_in_progress
            .fetch_sub(1, Ordering::AcqRel);
    };

    if wait {
        run.await;
    } else {
        tokio::spawn(run);
    }

    StatusCode::NO_CONTENT.into_response()
}

async fn prepare_shutdown_handler(
    AxumState(st): AxumState<HttpState>,
    method: Method,
) -> Response {
    if st.ingester.state() != State::Running {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let marker_path = shutdownmarker::get_path(&st.ingester.blocks_cfg.tsdb.dir);
    match method {
        Method::GET => match shutdownmarker::exists(&marker_path) {
            Ok(true) => (StatusCode::OK, "set\n").into_response(),
            Ok(false) => (StatusCode::OK, "unset\n").into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        Method::POST => {
            if shutdownmarker::create(&marker_path).is_err() {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            st.ingester.set_prepare_shutdown();
            StatusCode::NO_CONTENT.into_response()
        }
        Method::DELETE => {
            if st.ingester.ingest_cfg.enabled {
                return StatusCode::METHOD_NOT_ALLOWED.into_response();
            }
            if shutdownmarker::remove(&marker_path).is_err() {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            st.ingester.unset_prepare_shutdown();
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

#[derive(Serialize, Deserialize)]
struct PrepareUnregisterBody {
    unregister: Option<bool>,
}

async fn prepare_unregister_handler(
    AxumState(st): AxumState<HttpState>,
    method: Method,
    body: Bytes,
) -> Response {
    if st.ingester.state() != State::Running {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(ref lc) = st.ingester.lifecycler else {
        return Json(PrepareUnregisterBody {
            unregister: Some(st.ingester.cfg.ring.unregister_on_shutdown),
        })
        .into_response();
    };
    match method {
        Method::PUT => {
            let Ok(input) = serde_json::from_slice::<PrepareUnregisterBody>(&body) else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            let Some(val) = input.unregister else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            lc.set_unregister_on_shutdown(val);
        }
        Method::DELETE => {
            lc.set_unregister_on_shutdown(st.ingester.cfg.ring.unregister_on_shutdown);
        }
        _ => {}
    }
    Json(PrepareUnregisterBody {
        unregister: Some(lc.should_unregister_on_shutdown()),
    })
    .into_response()
}

#[derive(Serialize)]
struct TimestampBody {
    timestamp: i64,
}

async fn prepare_partition_downscale_handler(
    AxumState(st): AxumState<HttpState>,
    method: Method,
) -> Response {
    if st.ingester.state() != State::Running {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if !st.ingester.ingest_cfg.enabled {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Some(ref plc) = st.ingester.partition_lifecycler else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    match method {
        Method::POST => {
            let Ok((state, _)) = plc.get_partition_state().await else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            if state == PartitionState::PartitionPending {
                return StatusCode::CONFLICT.into_response();
            }
            if let Err(e) = plc
                .change_partition_state(PartitionState::PartitionInactive)
                .await
            {
                // Only `ring.ErrPartitionStateChangeLocked` maps to 409 Conflict.
                let status = if matches!(e, PartitionRingError::PartitionStateChangeLocked) {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                return (status, e.to_string()).into_response();
            }
        }
        Method::DELETE => {
            let Ok((state, _)) = plc.get_partition_state().await else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            if state == PartitionState::PartitionInactive {
                if let Err(e) = plc
                    .change_partition_state(PartitionState::PartitionActive)
                    .await
                {
                    // Only `ring.ErrPartitionStateChangeLocked` maps to 409 Conflict.
                    let status = if matches!(e, PartitionRingError::PartitionStateChangeLocked) {
                        StatusCode::CONFLICT
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    };
                    return (status, e.to_string()).into_response();
                }
            }
        }
        _ => {}
    }

    match plc.get_partition_state().await {
        Ok((PartitionState::PartitionInactive, ts)) => {
            let unix = ts
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            Json(TimestampBody { timestamp: unix }).into_response()
        }
        Ok(_) => Json(TimestampBody { timestamp: 0 }).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn prepare_instance_ring_downscale_handler(
    AxumState(st): AxumState<HttpState>,
    method: Method,
) -> Response {
    if st.ingester.state() != State::Running {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if st.ingester.ingest_cfg.enabled {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Some(ref lc) = st.ingester.lifecycler else {
        return Json(TimestampBody { timestamp: 0 }).into_response();
    };

    match method {
        Method::POST => {
            if let Err(e) = lc.change_read_only_state(true).await {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
            if let Some(ref pcb) = st.ingester.circuit_breaker.push {
                pcb.deactivate();
            }
        }
        Method::DELETE => {
            let (ro, _) = lc.get_read_only_state();
            if ro
                && st
                    .ingester
                    .num_compactions_in_progress
                    .load(Ordering::Acquire)
                    != 0
            {
                return (
                    StatusCode::CONFLICT,
                    "cannot clear read-only mode while compaction is pending or in progress",
                )
                    .into_response();
            }
            if let Err(e) = lc.change_read_only_state(false).await {
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
            if let Some(ref pcb) = st.ingester.circuit_breaker.push {
                pcb.activate();
            }
        }
        _ => {}
    }

    let (ro, rots) = lc.get_read_only_state();
    if ro {
        let unix = rots
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_secs() as i64);
        Json(TimestampBody { timestamp: unix }).into_response()
    } else {
        Json(TimestampBody { timestamp: 0 }).into_response()
    }
}

async fn shutdown_handler(AxumState(st): AxumState<HttpState>) -> Response {
    st.ingester
        .flush_on_shutdown
        .store(true, Ordering::Release);
    if let Some(ref lc) = st.ingester.lifecycler {
        lc.set_flush_on_shutdown(true);
        lc.set_unregister_on_shutdown(true);
    }
    let _ = st.ingester.stop().await;
    StatusCode::NO_CONTENT.into_response()
}

/// The tenant of an `auth=true` route: dskit `AuthenticateUser` (`401 no org id` without
/// `X-Scope-OrgID`) followed by Mimir's `newTenantValidationMiddleware` (`401` on an invalid
/// tenant ID, `422` on more than one tenant since federation is disabled on ingesters) and the
/// handler's own `tenant.TenantID`. Without multi-tenancy `noauth` injects the no-auth tenant and
/// ignores the header.
fn authenticated_tenant(st: &HttpState, headers: &HeaderMap) -> Result<String, (StatusCode, String)> {
    if !st.ingester.is_multitenancy_enabled() {
        return Ok(st.ingester.no_auth_tenant());
    }
    // `r.Header.Get("X-Scope-OrgID")`: the first value only.
    let org_id = headers
        .get(ORG_ID_HEADER_NAME)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    if org_id.is_empty() {
        return Err((StatusCode::UNAUTHORIZED, "no org id".to_string()));
    }
    let ids = parse_tenant_ids(&org_id).map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;
    if ids.len() > 1 {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "too many tenant IDs present in the request. max: 1 actual: {}",
                ids.len()
            ),
        ));
    }
    parse_single_tenant_id(&org_id).map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))
}

async fn user_registry_handler(
    AxumState(st): AxumState<HttpState>,
    headers: HeaderMap,
) -> Response {
    let user_id = match authenticated_tenant(&st, &headers) {
        Ok(id) => id,
        Err((status, msg)) => return go_http_error(status, &msg),
    };
    let Some(reg) = st.ingester.tsdb_metrics.registry_for_tenant(&user_id) else {
        return go_http_error(StatusCode::NOT_FOUND, "user registry not found");
    };
    let families = reg.gather();
    let encoder = prometheus::TextEncoder::new();
    match encoder.encode_to_string(&families) {
        Ok(body) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

// ---------------------------------------------------------------------------------------------
// /ingester/tenants and /ingester/tsdb/{tenant} (pkg/ingester/tenants_http.go)
// ---------------------------------------------------------------------------------------------

/// Go `tenantStats`.
struct TenantStats {
    tenant: String,
    series: u64,
    blocks: usize,
    min_time: String,
    max_time: String,
    past_grace_period_ns: i64,
    future_grace_period_ns: i64,
    warning: String,
}

/// Go `util.RenderHTTPResponse`'s content negotiation (`r.Header.Get("Accept")`).
fn accepts_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .is_some_and(|v| String::from_utf8_lossy(v.as_bytes()).contains("application/json"))
}

/// Go `util.WriteJSONResponse` (`json.Marshal`: no trailing newline).
fn go_json_response(v: &GoJson) -> Response {
    let mut body = v.encode();
    body.pop();
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// Go `time.Now()` in the local zone.
fn local_now() -> DateTime<FixedOffset> {
    chrono::Local::now().fixed_offset()
}

/// Go `formatTime`: `t.UTC().Format(time.RFC3339)`.
fn format_time(t: DateTime<Utc>) -> String {
    let mut out = String::with_capacity(20);
    let year = t.year();
    // Go `appendInt(b, year, 4)`.
    if year < 0 {
        let _ = write!(out, "-{:04}", year.unsigned_abs());
    } else {
        let _ = write!(out, "{year:04}");
    }
    let _ = write!(
        out,
        "-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    out
}

/// Go `formatMillisTime`.
fn format_millis_time(t: i64) -> String {
    match t {
        0 => "0".to_string(),
        i64::MIN => "math.MinInt64".to_string(),
        i64::MAX => "math.MaxInt64".to_string(),
        _ => match DateTime::<Utc>::from_timestamp_millis(t) {
            Some(dt) => format!("{} ({t})", format_time(dt)),
            None => format!("({t})"),
        },
    }
}

/// `html/template`'s `urlNormalizer` (`urlProcessor(true, ...)`), used for values inside a URL
/// attribute after a literal prefix such as `href="tsdb/{{ .Tenant }}"`.
fn html_url_normalize(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    for (i, &c) in b.iter().enumerate() {
        let keep = match c {
            b'!' | b'#' | b'$' | b'&' | b'*' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?'
            | b'@' | b'[' | b']' | b'-' | b'.' | b'_' | b'~' => true,
            b'%' => i + 2 < b.len() && b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit(),
            c => c.is_ascii_alphanumeric(),
        };
        if keep {
            out.push(c as char);
        } else {
            let _ = write!(out, "%{c:02x}");
        }
    }
    out
}

/// `{{ . }}` of a string inside a quoted URL attribute: `urlNormalizer | attrEscaper`.
fn html_url_attr(s: &str) -> String {
    html_escape(&html_url_normalize(s))
}

/// Go `Ingester.TenantsHandler`.
async fn tenants_handler(
    AxumState(st): AxumState<HttpState>,
    headers: HeaderMap,
) -> Response {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);

    let mut tss = Vec::new();
    for t in st.ingester.get_tsdb_users() {
        let Some(db) = st.ingester.get_tsdb(&t) else {
            continue;
        };
        let lim = st.ingester.overrides.for_tenant(&t);
        let past_grace = lim.past_grace_period.0;
        let past_grace_period_ns = past_grace.wrapping_add(lim.out_of_order_time_window.0);
        let future_grace_period_ns = lim.creation_grace_period.0;

        let head = db.db.head();
        let min_millis = head.min_time();
        let max_millis = head.max_time();

        // Go int64 arithmetic wraps (e.g. an empty head's math.MinInt64 max time).
        let mut warnings: Vec<String> = Vec::new();
        let delta = max_millis.wrapping_sub(now_ms);
        if delta > future_grace_period_ns / 1_000_000 {
            warnings.push(format!(
                "TSDB Head max timestamp too far in the future: {}",
                GoDuration(delta.wrapping_mul(1_000_000))
            ));
        }
        if past_grace > 0 && now_ms.wrapping_sub(min_millis) > past_grace_period_ns / 1_000_000 {
            warnings.push("TSDB Head min timestamp too far in the past".to_string());
        }

        tss.push(TenantStats {
            series: head.num_series(),
            blocks: db.db.blocks().len(),
            min_time: format_millis_time(min_millis),
            max_time: format_millis_time(max_millis),
            past_grace_period_ns,
            future_grace_period_ns,
            warning: warnings.join(", "),
            tenant: t,
        });
    }
    tss.sort_by(|a, b| b.series.cmp(&a.series).then_with(|| a.tenant.cmp(&b.tenant)));

    let now = local_now();
    if accepts_json(&headers) {
        let tenants = if tss.is_empty() {
            GoJson::Null
        } else {
            GoJson::Array(
                tss.iter()
                    .map(|s| {
                        GoJson::Object(vec![
                            ("Tenant".into(), GoJson::str(s.tenant.as_str())),
                            ("Series".into(), GoJson::int(s.series)),
                            ("Blocks".into(), GoJson::int(s.blocks as u64)),
                            ("MinTime".into(), GoJson::str(s.min_time.as_str())),
                            ("MaxTime".into(), GoJson::str(s.max_time.as_str())),
                            ("PastGracePeriod".into(), GoJson::int(s.past_grace_period_ns)),
                            ("FutureGracePeriod".into(), GoJson::int(s.future_grace_period_ns)),
                            ("Warning".into(), GoJson::str(s.warning.as_str())),
                        ])
                    })
                    .collect(),
            )
        };
        return go_json_response(&GoJson::Object(vec![
            ("Now".into(), GoJson::time(&now)),
            ("Tenants".into(), tenants),
        ]));
    }

    Html(render_tenants_page(&now, &tss)).into_response()
}

/// `tenants.gohtml`.
fn render_tenants_page(now: &DateTime<FixedOffset>, tss: &[TenantStats]) -> String {
    let mut out = String::with_capacity(1024 + tss.len() * 512);
    out.push_str(
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>Ingester tenants</title>\n</head>\n<body>\n<h1>Ingester tenants</h1>\n<p>Current time: ",
    );
    out.push_str(&html_escape(&format_go_time_string(now)));
    out.push_str(
        "</p>\n<table border=\"1\" cellpadding=\"5\" style=\"border-collapse: collapse\">\n    <thead>\n    <tr>\n        <th>Tenant</th>\n        <th>Head series</th>\n        <th>Blocks</th>\n        <th>Head MinT</th>\n        <th>Head MaxT</th>\n        <th>Grace interval</th>\n        <th>Warning</th>\n    </tr>\n    </thead>\n    <tbody style=\"font-family: monospace;\">\n    ",
    );
    for s in tss {
        let _ = write!(
            out,
            "\n        <tr>\n            <td><a href=\"tsdb/{}\">{}</a></td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>(-{}, +{})</td>\n            <td>{}</td>\n        </tr>\n    ",
            html_url_attr(&s.tenant),
            html_escape(&s.tenant),
            s.series,
            s.blocks,
            html_escape(&s.min_time),
            html_escape(&s.max_time),
            html_escape(&GoDuration(s.past_grace_period_ns).to_string()),
            html_escape(&GoDuration(s.future_grace_period_ns).to_string()),
            html_escape(&s.warning),
        );
    }
    out.push_str("\n    </tbody>\n</table>\n</body>\n</html>");
    out
}

/// Go `tenantTSDBBlockPageContent`.
struct TenantTsdbBlock {
    id: String,
    min_time: String,
    max_time: String,
    out_of_order: bool,
    compaction: mimir_tsdb::block::BlockMetaCompaction,
    stats: mimir_tsdb::block::BlockStats,
    uploaded_on: String,
}

impl TenantTsdbBlock {
    /// Go `json.Marshal` of the block, including `tsdb.BlockMetaCompaction` / `tsdb.BlockStats`
    /// with their `omitempty` tags in declaration order.
    fn to_go_json(&self) -> GoJson {
        let c = &self.compaction;
        let mut compaction = vec![("level".to_string(), GoJson::int(c.level))];
        if !c.sources.is_empty() {
            compaction.push((
                "sources".into(),
                GoJson::Array(c.sources.iter().map(|u| GoJson::str(u.to_string())).collect()),
            ));
        }
        if c.deletable {
            compaction.push(("deletable".into(), GoJson::Bool(true)));
        }
        if !c.parents.is_empty() {
            compaction.push((
                "parents".into(),
                GoJson::Array(
                    c.parents
                        .iter()
                        .map(|p| {
                            GoJson::Object(vec![
                                ("ulid".into(), GoJson::str(p.ulid.to_string())),
                                ("minTime".into(), GoJson::int(p.min_time)),
                                ("maxTime".into(), GoJson::int(p.max_time)),
                            ])
                        })
                        .collect(),
                ),
            ));
        }
        if c.failed {
            compaction.push(("failed".into(), GoJson::Bool(true)));
        }
        if !c.hints.is_empty() {
            compaction.push(("hints".into(), GoJson::strings(&c.hints)));
        }

        let st = &self.stats;
        let stats: Vec<(String, GoJson)> = [
            ("numSamples", st.num_samples),
            ("numFloatSamples", st.num_float_samples),
            ("numHistogramSamples", st.num_histogram_samples),
            ("numSeries", st.num_series),
            ("numChunks", st.num_chunks),
            ("numTombstones", st.num_tombstones),
        ]
        .into_iter()
        .filter(|(_, v)| *v != 0)
        .map(|(k, v)| (k.to_string(), GoJson::int(v)))
        .collect();

        GoJson::Object(vec![
            ("ID".into(), GoJson::str(self.id.as_str())),
            ("MinTime".into(), GoJson::str(self.min_time.as_str())),
            ("MaxTime".into(), GoJson::str(self.max_time.as_str())),
            ("OutOfOrder".into(), GoJson::Bool(self.out_of_order)),
            ("Compaction".into(), GoJson::Object(compaction)),
            ("Stats".into(), GoJson::Object(stats)),
            ("UploadedOn".into(), GoJson::str(self.uploaded_on.as_str())),
        ])
    }
}

/// Go `Ingester.TenantTSDBHandler`. The route is registered on a router with
/// `UseEncodedPath()`, so `{tenant}` is the still percent-encoded path segment.
async fn tenant_tsdb_handler(
    AxumState(st): AxumState<HttpState>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let tenant = uri
        .path()
        .strip_prefix("/ingester/tsdb/")
        .unwrap_or_default()
        .to_string();
    if tenant.is_empty() {
        return ([(header::CONTENT_TYPE, "text/plain")], "Tenant ID can't be empty").into_response();
    }
    let Some(db) = st.ingester.get_tsdb(&tenant) else {
        // `w.WriteHeader(404)` precedes `WriteTextResponse`'s header, so net/http sniffs the type.
        return (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            format!("TSDB not found for tenant {tenant}"),
        )
            .into_response();
    };

    let head = db.db.head();
    let appendable_min_valid_time = head
        .min_in_order_append_time()
        .map(format_millis_time)
        .unwrap_or_default();
    let shipped = db.cached_shipped_blocks();
    let blocks: Vec<TenantTsdbBlock> = db
        .db
        .blocks()
        .iter()
        .map(|b| {
            let m = b.meta();
            TenantTsdbBlock {
                id: m.ulid.to_string(),
                min_time: format_millis_time(m.min_time),
                max_time: format_millis_time(m.max_time),
                out_of_order: m.out_of_order,
                compaction: m.compaction.clone(),
                stats: m.stats.clone(),
                uploaded_on: shipped
                    .get(&m.ulid)
                    .map(|t| format_time(DateTime::<Utc>::from(*t)))
                    .unwrap_or_default(),
            }
        })
        .collect();
    let num_series = head.num_series();
    let min_time = format_millis_time(head.min_time());
    let max_time = format_millis_time(head.max_time());
    let min_ooo_time = format_millis_time(head.min_ooo_time());
    let max_ooo_time = format_millis_time(head.max_ooo_time());
    let label_churn_enabled = st.ingester.cfg.label_churn_enabled;
    let now = local_now();

    if accepts_json(&headers) {
        let blocks_json = if blocks.is_empty() {
            GoJson::Null
        } else {
            GoJson::Array(blocks.iter().map(TenantTsdbBlock::to_go_json).collect())
        };
        return go_json_response(&GoJson::Object(vec![
            ("Now".into(), GoJson::time(&now)),
            ("Tenant".into(), GoJson::str(tenant.as_str())),
            (
                "Head".into(),
                GoJson::Object(vec![
                    ("MinTime".into(), GoJson::str(min_time.as_str())),
                    ("MaxTime".into(), GoJson::str(max_time.as_str())),
                    ("NumSeries".into(), GoJson::int(num_series)),
                    (
                        "AppendableMinValidTime".into(),
                        GoJson::str(appendable_min_valid_time.as_str()),
                    ),
                    ("MinOOOTime".into(), GoJson::str(min_ooo_time.as_str())),
                    ("MaxOOOTime".into(), GoJson::str(max_ooo_time.as_str())),
                ]),
            ),
            ("Blocks".into(), blocks_json),
            ("LabelChurnEnabled".into(), GoJson::Bool(label_churn_enabled)),
        ]));
    }

    // tenant_tsdb.gohtml
    let tenant_html = html_escape(&tenant);
    let mut out = String::with_capacity(2048 + blocks.len() * 512);
    let _ = write!(
        out,
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>Ingester: TSDB for tenant {tenant_html}</title>\n</head>\n<body>\n<h1>Ingester: TSDB for tenant {tenant_html}</h1>\n<p>Current time: {}</p>\n\n<h2>TSDB Head</h2>\n\n<ul>\n    <li>Number of series: {num_series}</li>\n    <li>Min Time: {}</li>\n    <li>Max Time: {}</li>\n    <li>Appendable Min Valid Time: {}</li>\n    <li>Min OOO Time: {}</li>\n    <li>Max OOO Time: {}</li>\n</ul>\n",
        html_escape(&format_go_time_string(&now)),
        html_escape(&min_time),
        html_escape(&max_time),
        if appendable_min_valid_time.is_empty() {
            "N/A".to_string()
        } else {
            html_escape(&appendable_min_valid_time)
        },
        html_escape(&min_ooo_time),
        html_escape(&max_ooo_time),
    );
    if label_churn_enabled {
        let _ = write!(
            out,
            "\n<p><a href=\"../label_churn/{}\">Label churn</a></p>\n",
            html_url_attr(&tenant)
        );
    }
    out.push_str(
        "\n\n<h2>Blocks</h2>\n\n\n<table border=\"1\" cellpadding=\"5\" style=\"border-collapse: collapse\">\n    <thead>\n    <tr>\n        <th>ID</th>\n        <th>Min Time</th>\n        <th>Max Time</th>\n        <th>Number of Series</th>\n        <th>Number of Chunks</th>\n        <th>Number of Samples</th>\n        <th>Compaction Hints</th>\n        <th>Uploaded</th>\n    </tr>\n    </thead>\n    <tbody style=\"font-family: monospace;\">\n    ",
    );
    for b in &blocks {
        let _ = write!(
            out,
            "\n        <tr>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n            <td>{}</td>\n        </tr>\n    ",
            html_escape(&b.id),
            html_escape(&b.min_time),
            html_escape(&b.max_time),
            b.stats.num_series,
            b.stats.num_chunks,
            b.stats.num_samples,
            // fmt of a []string: `[a b]`.
            html_escape(&format!("[{}]", b.compaction.hints.join(" "))),
            html_escape(&b.uploaded_on),
        );
    }
    out.push_str("\n    </tbody>\n</table>\n</body>\n</html>");
    Html(out).into_response()
}

#[derive(Deserialize)]
struct LabelChurnQuery {
    lookback: Option<String>,
    limit: Option<i64>,
}

async fn label_churn_handler(
    AxumState(st): AxumState<HttpState>,
    Path(tenant): Path<String>,
    Query(q): Query<LabelChurnQuery>,
) -> Response {
    if !st.ingester.cfg.label_churn_enabled {
        return (
            StatusCode::NOT_FOUND,
            "label churn detection is disabled, enable it with -ingester.label-churn-enabled",
        )
            .into_response();
    }
    let lookback = if let Some(ref lb) = q.lookback {
        match parse_go_duration(lb) {
            Ok(d_ns) if d_ns > 0 => Duration::from_nanos(d_ns as u64),
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    "the lookback parameter must be a positive duration",
                )
                    .into_response();
            }
        }
    } else {
        DEFAULT_LABEL_CHURN_LOOKBACK
    };

    let limit = if let Some(l) = q.limit {
        if l < 0 {
            return (
                StatusCode::BAD_REQUEST,
                "the limit parameter must be a non-negative integer",
            )
                .into_response();
        }
        l as usize
    } else {
        DEFAULT_LABEL_CHURN_LIMIT
    };

    let Some(db) = st.ingester.get_tsdb(&tenant) else {
        return (
            StatusCode::NOT_FOUND,
            format!("the ingester has no TSDB for tenant {tenant}"),
        )
            .into_response();
    };

    let since_ms = SystemTime::now()
        .checked_sub(lookback)
        .unwrap_or(UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let pending = db.pending_series_removals.load(Ordering::Acquire);
    let last_removal = db.last_series_removal_ms.load(Ordering::Acquire);
    let db_ref = Arc::clone(&db);
    match compute_label_churn(
        db.db.head(),
        &db.series_ref_history,
        pending,
        last_removal,
        since_ms,
        limit,
        move || {
            (
                db_ref.pending_series_removals.load(Ordering::Acquire),
                db_ref.last_series_removal_ms.load(Ordering::Acquire),
            )
        },
    ) {
        Ok(resp) => Json(resp).into_response(),
        Err(LabelChurnError::CompactedTooRecently | LabelChurnError::NoRecord) => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use tower::ServiceExt;

    fn test_ingester(tmp: &tempfile::TempDir) -> Arc<Ingester> {
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        Ingester::new(
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
        .unwrap()
    }

    async fn get(
        app: &Router,
        uri: &str,
        headers: &[(&str, &str)],
    ) -> (StatusCode, HeaderMap, String) {
        let mut b = axum::http::Request::builder().uri(uri);
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        let resp = app
            .clone()
            .oneshot(b.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, headers, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn tsdb_metrics_auth_matches_mimir_middlewares() {
        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(&tmp);
        let _ = ing.get_or_create_tsdb("a").unwrap();
        let app = ingester_router(Arc::clone(&ing));

        let (code, h, body) = get(&app, "/ingester/tsdb_metrics", &[]).await;
        assert_eq!((code, body.as_str()), (StatusCode::UNAUTHORIZED, "no org id\n"));
        assert_eq!(h.get("content-type").unwrap(), "text/plain; charset=utf-8");

        let (code, _, body) = get(&app, "/ingester/tsdb_metrics", &[("X-Scope-OrgID", "a|b")]).await;
        assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body, "too many tenant IDs present in the request. max: 1 actual: 2\n");

        let (code, _, body) = get(&app, "/ingester/tsdb_metrics", &[("X-Scope-OrgID", "a/b")]).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        assert_eq!(body, "tenant ID 'a/b' contains unsupported character '/'\n");

        // Normalized duplicates and metadata are a single tenant.
        let (code, _, _) = get(&app, "/ingester/tsdb_metrics", &[("X-Scope-OrgID", "a|a:k=v")]).await;
        assert_eq!(code, StatusCode::OK);

        let (code, _, body) = get(&app, "/ingester/tsdb_metrics", &[("X-Scope-OrgID", "zzz")]).await;
        assert_eq!((code, body.as_str()), (StatusCode::NOT_FOUND, "user registry not found\n"));

        // Without multi-tenancy the header is ignored and the no-auth tenant is used.
        ing.set_multitenancy(false, "a".to_string());
        let (code, _, _) = get(&app, "/ingester/tsdb_metrics", &[("X-Scope-OrgID", "x|y")]).await;
        assert_eq!(code, StatusCode::OK);
    }

    #[tokio::test]
    async fn tenants_pages_use_go_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(&tmp);
        let app = ingester_router(Arc::clone(&ing));

        let (code, h, body) = get(&app, "/ingester/tenants", &[("Accept", "application/json")]).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(h.get("content-type").unwrap(), "application/json");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v["Now"].is_string());
        assert!(v["Tenants"].is_null());

        let _ = ing.get_or_create_tsdb("t(1)").unwrap();
        let (_, _, body) = get(&app, "/ingester/tenants", &[("Accept", "application/json")]).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let t = &v["Tenants"][0];
        let keys: Vec<&str> = t.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys.len(), 8);
        assert_eq!(t["Tenant"], "t(1)");
        assert_eq!(t["Series"], 0);
        assert_eq!(t["Blocks"], 0);
        assert_eq!(t["MinTime"], "math.MaxInt64");
        assert_eq!(t["MaxTime"], "math.MinInt64");
        assert_eq!(t["PastGracePeriod"], 0);
        assert_eq!(t["FutureGracePeriod"], 600_000_000_000i64);
        // Go's wrapping `math.MinInt64 - now` makes an empty head look far in the future.
        assert!(t["Warning"]
            .as_str()
            .unwrap()
            .starts_with("TSDB Head max timestamp too far in the future: "));
        // Field order is Go's struct declaration order.
        assert!(body.find("\"Now\"").unwrap() < body.find("\"Tenants\"").unwrap());

        let (_, _, html) = get(&app, "/ingester/tenants", &[]).await;
        assert!(html.contains("<td><a href=\"tsdb/t%281%29\">t(1)</a></td>"), "{html}");
        assert!(html.contains("<td>(-0s, +10m0s)</td>"));
        assert!(html.ends_with("</html>"));

        let (code, _, body) = get(&app, "/ingester/tsdb/t(1)", &[("Accept", "application/json")]).await;
        assert_eq!(code, StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["Tenant"], "t(1)");
        assert_eq!(v["Head"]["NumSeries"], 0);
        assert_eq!(v["Head"]["MinTime"], "math.MaxInt64");
        assert_eq!(v["Head"]["AppendableMinValidTime"], "");
        assert_eq!(v["Head"]["MinOOOTime"], "math.MaxInt64");
        assert_eq!(v["Head"]["MaxOOOTime"], "math.MinInt64");
        assert!(v["Blocks"].is_null());
        assert_eq!(v["LabelChurnEnabled"], false);

        let (_, _, html) = get(&app, "/ingester/tsdb/t(1)", &[]).await;
        assert!(html.contains("<li>Appendable Min Valid Time: N/A</li>"));

        // `UseEncodedPath`: the tenant segment is not percent-decoded.
        let (code, _, body) = get(&app, "/ingester/tsdb/t%281%29", &[]).await;
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert_eq!(body, "TSDB not found for tenant t%281%29");
    }

    #[test]
    fn format_helpers_match_go() {
        assert_eq!(format_millis_time(0), "0");
        assert_eq!(format_millis_time(i64::MIN), "math.MinInt64");
        assert_eq!(format_millis_time(i64::MAX), "math.MaxInt64");
        assert_eq!(
            format_millis_time(1_794_884_474_707),
            "2026-11-17T03:01:14Z (1794884474707)"
        );
        assert_eq!(format_millis_time(-1), "1969-12-31T23:59:59Z (-1)");
        assert_eq!(html_url_normalize("a!*'()-_.~%41%zz b"), "a!*%27%28%29-_.~%41%25zz%20b");
    }

    #[tokio::test]
    async fn tsdb_metrics_endpoint_encodes_tenant_prometheus_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        let mut blocks_cfg = mimir_config::BlocksStorageConfig::default();
        blocks_cfg.tsdb.dir = tmp.path().to_string_lossy().into_owned();
        let ing = Ingester::new(
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
        .unwrap();

        let _ = ing.get_or_create_tsdb("tenant-1").unwrap();
        let app = ingester_router(ing);
        let req = axum::http::Request::builder()
            .uri("/ingester/tsdb_metrics")
            .header("X-Scope-OrgID", "tenant-1")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("prometheus_tsdb_head_series"));
    }
}
