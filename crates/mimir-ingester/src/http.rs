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
//! - `/ingester/partition-ring` (GET, POST - when PartitionRingWatcher is configured)

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, Query, RawQuery, State as AxumState};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use mimir_common::logging::format_rfc3339_utc;
use mimir_common::services::State;
use mimir_common::shutdownmarker;
use mimir_common::tenant::extract_tenant_id_from_http_headers;
use mimir_config::{parse_go_duration, GoDuration};
use mimir_proto::ringpb::PartitionState;
use mimir_ring::partition_ring::http::partition_ring_router;
use mimir_ring::partition_ring::{PartitionRingEditor, PartitionRingError, PartitionRingReader};
use mimir_ring::ring::http::{ring_router, StatusPageConfig};
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
        router = router.nest(
            "/ingester/ring",
            ring_router(Arc::clone(ring), StatusPageConfig::default()),
        );
    }

    if let Some(ref watcher) = ingester.partition_ring_watcher {
        if let Some(kv) = ingester
            .instance_ring
            .as_ref()
            .and_then(|r| r.kv_client().cloned())
        {
            let editor = Arc::new(PartitionRingEditor::new("ingester-partitions", kv));
            let reader: Arc<dyn PartitionRingReader> = Arc::clone(watcher) as Arc<dyn PartitionRingReader>;
            router = router.nest(
                "/ingester/partition-ring",
                partition_ring_router(reader, editor),
            );
        }
    }

    router
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
                let status = if matches!(
                    e,
                    PartitionRingError::PartitionStateChangeLocked
                        | PartitionRingError::PartitionStateChangeNotAllowed
                ) {
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
                    let status = if matches!(
                        e,
                        PartitionRingError::PartitionStateChangeLocked
                            | PartitionRingError::PartitionStateChangeNotAllowed
                    ) {
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
    let Ok(user_id) = extract_tenant_id_from_http_headers(&headers) else {
        return (StatusCode::UNAUTHORIZED, "no org id").into_response();
    };
    let Some(db) = st.ingester.get_tsdb(&user_id) else {
        return (StatusCode::NOT_FOUND, "user registry not found").into_response();
    };
    let head = db.db.head();
    let (series, chunks, samples, mint, maxt) = head.head_stats();
    let body = format!(
        "# TYPE prometheus_tsdb_head_series gauge\nprometheus_tsdb_head_series {series}\n\
         # TYPE prometheus_tsdb_head_chunks gauge\nprometheus_tsdb_head_chunks {chunks}\n\
         # TYPE prometheus_tsdb_head_samples_appended_total counter\nprometheus_tsdb_head_samples_appended_total {samples}\n\
         # TYPE prometheus_tsdb_head_min_time gauge\nprometheus_tsdb_head_min_time {mint}\n\
         # TYPE prometheus_tsdb_head_max_time gauge\nprometheus_tsdb_head_max_time {maxt}\n"
    );
    (StatusCode::OK, body).into_response()
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
