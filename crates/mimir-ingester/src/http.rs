//! Axum HTTP admin & lifecycle router matching `pkg/api/api.go:RegisterIngester`.
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
//! - `/ingester/partition-ring` (GET, POST - when PartitionRingWatcher is configured; with
//!   compartments enabled a GET-only index page plus `/ingester/partition-ring/compartment-<id>`)
//!
//! Also ported from github.com/grafana/mimir/pkg/compartments/index_page.go (+ `.gohtml`).

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, Query, RawQuery, State as AxumState};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mimir_common::logging::format_rfc3339_utc;
use mimir_common::services::State;
use mimir_common::shutdownmarker;
use mimir_common::tenant::extract_tenant_id_from_http_headers;
use mimir_config::{parse_go_duration, GoDuration};
use mimir_proto::ringpb::PartitionState;
use mimir_ring::partition_ring::http::{partition_ring_router, partition_ring_router_at};
use mimir_ring::partition_ring::{PartitionRingEditor, PartitionRingError, PartitionRingReader};
use mimir_ring::ring::http::ring_router;
use mimir_ring::ring::to_status_page_config;
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
        // Go `RingConfig.ToRingConfig`: `StatusPageConfig = toStatusPageConfig()` (links from
        // `-ingester.ring.status-page-ingester-link-template`).
        router = router.merge(ring_router(Arc::clone(ring), to_status_page_config(&ingester.cfg.ring)));
    }

    let compartment_watchers = ingester
        .partition_ring_watchers()
        .filter(|_| ingester.compartments_config().enabled);
    if let Some(watchers) = compartment_watchers {
        // Go `RegisterIngesterPartitionRings` with compartments enabled: an index page linking
        // one page per read compartment, each editing that compartment's `-rc-<id>` ring key.
        let index = compartments_index_page_html(
            "Partitions Ring Status",
            "partition-ring/compartment-<read-compartment-id>",
            watchers.count(),
        );
        router = router.route(
            "/ingester/partition-ring",
            get(move || {
                let body = index.clone();
                async move {
                    (
                        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                        body,
                    )
                }
            }),
        );
        for (compartment_id, watcher) in watchers.all().iter().enumerate() {
            let editor = Arc::new(PartitionRingEditor::new(
                watcher.key(),
                Arc::clone(watcher.kv_client()),
            ));
            let reader: Arc<dyn PartitionRingReader> =
                Arc::clone(watcher) as Arc<dyn PartitionRingReader>;
            router = router.merge(partition_ring_router_at(
                &format!("/ingester/partition-ring/compartment-{compartment_id}"),
                reader,
                editor,
            ));
        }
    } else if let Some(ref watcher) = ingester.partition_ring_watcher {
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

/// Go `html/template` escaping of text and attribute values.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// `compartments.NewIndexPageHandler`: one link per compartment, with the read compartment ID
/// placeholder of `link_path_format` replaced by the compartment ID (`index_page.gohtml`).
fn compartments_index_page_html(
    page_title: &str,
    link_path_format: &str,
    num_compartments: usize,
) -> String {
    let title = escape_html(page_title);
    let mut out = format!(
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>{title}</title>\n</head>\n<body>\n    <h1>{title}</h1>\n    <ul>\n    "
    );
    for id in 0..num_compartments {
        let path = link_path_format.replace("<read-compartment-id>", &id.to_string());
        out.push_str(&format!(
            "\n        <li><a href=\"{}\">Compartment {id}</a></li>\n    ",
            escape_html(&path)
        ));
    }
    out.push_str("\n    </ul>\n</body>\n</html>\n");
    out
}

fn parse_query_params(raw_query: Option<&str>) -> (Option<HashSet<String>>, bool) {
    let Some(q) = raw_query else {
        return (None, false);
    };
    let mut tenants = HashSet::new();
    let mut wait = false;
    for pair in q.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == "tenant" && !v.is_empty() {
                tenants.insert(v.to_string());
            } else if k == "wait" && v == "true" {
                wait = true;
            }
        }
    }
    let users = if tenants.is_empty() {
        None
    } else {
        Some(tenants)
    };
    (users, wait)
}

async fn flush_handler(
    AxumState(st): AxumState<HttpState>,
    RawQuery(raw_query): RawQuery,
) -> Response {
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
    let ship_tx_opt = st.ingester.ship_tx.read().clone();
    let (users, wait) = parse_query_params(raw_query.as_deref());
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

async fn user_registry_handler(
    AxumState(st): AxumState<HttpState>,
    headers: HeaderMap,
) -> Response {
    let user_id = match extract_tenant_id_from_http_headers(&headers) {
        Ok(id) => id,
        Err(mimir_common::tenant::TenantError::NoOrgID)
            if !st.ingester.is_multitenancy_enabled() =>
        {
            st.ingester.no_auth_tenant()
        }
        Err(_) => return (StatusCode::UNAUTHORIZED, "no org id").into_response(),
    };
    let Some(reg) = st.ingester.tsdb_metrics.registry_for_tenant(&user_id) else {
        return (StatusCode::NOT_FOUND, "user registry not found").into_response();
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

#[derive(Serialize)]
struct TenantsJsonItem {
    tenant: String,
    series: u64,
    blocks: usize,
    min_time: i64,
    max_time: i64,
}

async fn tenants_handler(
    AxumState(st): AxumState<HttpState>,
    headers: HeaderMap,
) -> Response {
    let mut rows = Vec::new();
    for user_id in st.ingester.get_tsdb_users() {
        let Some(db) = st.ingester.get_tsdb(&user_id) else {
            continue;
        };
        let head = db.db.head();
        rows.push(TenantsJsonItem {
            tenant: user_id,
            series: head.num_series(),
            blocks: db.db.blocks().len(),
            min_time: head.min_time(),
            max_time: head.max_time(),
        });
    }
    rows.sort_by(|a, b| b.series.cmp(&a.series).then_with(|| a.tenant.cmp(&b.tenant)));

    let accepts_json = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("application/json"));
    if accepts_json {
        return Json(rows).into_response();
    }

    let now_str = format_rfc3339_utc(SystemTime::now());
    let mut html = format!(
        "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"UTF-8\">\n<title>Ingester tenants</title>\n</head>\n<body>\n\
         <h1>Ingester tenants</h1>\n<p>Current time: {now_str}</p>\n\
         <table border=\"1\" cellpadding=\"5\" style=\"border-collapse: collapse\">\n\
         <thead><tr><th>Tenant</th><th>Head series</th><th>Blocks</th><th>Head MinT</th><th>Head MaxT</th><th>Grace interval</th><th>Warning</th></tr></thead>\n\
         <tbody style=\"font-family: monospace;\">"
    );
    for r in rows {
        let lim = st.ingester.overrides.for_tenant(&r.tenant);
        let past_grace = GoDuration(lim.past_grace_period.0 + lim.out_of_order_time_window.0);
        let future_grace = GoDuration(lim.creation_grace_period.0);
        html.push_str(&format!(
            "<tr><td><a href=\"tsdb/{0}\">{0}</a></td><td>{1}</td><td>{2}</td><td>{3}</td><td>{4}</td><td>(-{5}, +{6})</td><td></td></tr>",
            r.tenant, r.series, r.blocks, r.min_time, r.max_time, past_grace, future_grace
        ));
    }
    html.push_str("</tbody></table></body></html>");
    Html(html).into_response()
}

#[derive(Serialize)]
struct TenantTsdbJson {
    tenant: String,
    num_series: u64,
    min_time: i64,
    max_time: i64,
    min_ooo_time: i64,
    max_ooo_time: i64,
    blocks: Vec<String>,
}

async fn tenant_tsdb_handler(
    AxumState(st): AxumState<HttpState>,
    Path(tenant): Path<String>,
    headers: HeaderMap,
) -> Response {
    if tenant.is_empty() {
        return (StatusCode::BAD_REQUEST, "Tenant ID can't be empty").into_response();
    }
    let Some(db) = st.ingester.get_tsdb(&tenant) else {
        return (
            StatusCode::NOT_FOUND,
            format!("TSDB not found for tenant {tenant}"),
        )
            .into_response();
    };
    let head = db.db.head();
    let block_ids: Vec<String> = db.db.block_ulids().into_iter().map(|u| u.to_string()).collect();
    let accepts_json = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("application/json"));
    if accepts_json {
        return Json(TenantTsdbJson {
            tenant,
            num_series: head.num_series(),
            min_time: head.min_time(),
            max_time: head.max_time(),
            min_ooo_time: head.min_ooo_time(),
            max_ooo_time: head.max_ooo_time(),
            blocks: block_ids,
        })
        .into_response();
    }

    let now_str = format_rfc3339_utc(SystemTime::now());
    let label_churn_link = if st.ingester.cfg.label_churn_enabled {
        format!("<p><a href=\"../label_churn/{tenant}\">Label churn</a></p>")
    } else {
        String::new()
    };
    let html = format!(
        "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"UTF-8\">\n<title>Ingester: TSDB for tenant {tenant}</title>\n</head>\n<body>\n\
         <h1>Ingester: TSDB for tenant {tenant}</h1>\n<p>Current time: {now_str}</p>\n\
         <h2>TSDB Head</h2>\n<ul>\n\
         <li>Number of series: {}</li>\n\
         <li>Min Time: {}</li>\n\
         <li>Max Time: {}</li>\n\
         <li>Min OOO Time: {}</li>\n\
         <li>Max OOO Time: {}</li>\n\
         </ul>\n{}\n\
         <h2>Blocks</h2>\n<p>Blocks: {:?}</p></body></html>",
        head.num_series(),
        head.min_time(),
        head.max_time(),
        head.min_ooo_time(),
        head.max_ooo_time(),
        label_churn_link,
        block_ids
    );
    Html(html).into_response()
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
