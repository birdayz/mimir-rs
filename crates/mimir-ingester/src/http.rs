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
//! - `/ingester/partition-ring` (GET, POST - when PartitionRingWatcher is configured; with
//!   compartments enabled a GET-only index page plus `/ingester/partition-ring/compartment-<id>`)
//!
//! Also ported from github.com/grafana/mimir/pkg/compartments/index_page.go (+ `.gohtml`).

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{RawQuery, Request, State as AxumState};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Datelike, FixedOffset, Timelike, Utc};
use mimir_common::services::State;
use mimir_common::shutdownmarker;
use mimir_common::tenant::{
    extract_org_id_bytes_from_http_headers, parse_single_tenant_id_bytes, parse_tenant_ids_bytes,
};
use mimir_config::{parse_model_duration, GoDuration};
use mimir_proto::ringpb::PartitionState;
use mimir_ring::partition_ring::http::{partition_ring_router, partition_ring_router_at};
use mimir_ring::partition_ring::{PartitionRingEditor, PartitionRingError, PartitionRingReader};
use mimir_ring::kv::memberlist::gofmt::{format_go_time_string, GoJson};
use mimir_ring::kv::memberlist::http_status::html_escape;
use mimir_ring::ring::http::{go_http_error, parse_go_form, ring_router};
use mimir_ring::ring::to_status_page_config;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::ingester::{Ingester, TriggerRequest};
use crate::label_churn::{
    compute_label_churn, LabelChurnError, DEFAULT_LABEL_CHURN_LIMIT,
    DEFAULT_LABEL_CHURN_LOOKBACK,
};

/// Raw `X-Scope-OrgID` header bytes preserved when `http::HeaderValue` rejects
/// control/DEL characters (matching Go `http.Header` over `httpgrpc`).
#[derive(Clone, Debug)]
pub struct RawScopeOrgId(pub Vec<u8>);

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
        // `-ingester.ring.status-page-ingester-link-template`, and
        // `HideTokensInStatusPage = cfg.IngestStorage.Enabled` in `pkg/mimir/modules.go`).
        let mut status_cfg = to_status_page_config(&ingester.cfg.ring);
        status_cfg.hide_tokens_ui_elements = ingester.ingest_cfg.enabled;
        router = router.merge(ring_router(Arc::clone(ring), status_cfg));
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

/// `compartments.NewIndexPageHandler`: one link per compartment, with the read compartment ID
/// placeholder of `link_path_format` replaced by the compartment ID (`index_page.gohtml`).
fn compartments_index_page_html(
    page_title: &str,
    link_path_format: &str,
    num_compartments: usize,
) -> String {
    let title = html_escape(page_title);
    let mut out = format!(
        "<!DOCTYPE html>\n<html>\n<head>\n    <meta charset=\"UTF-8\">\n    <title>{title}</title>\n</head>\n<body>\n    <h1>{title}</h1>\n    <ul>\n    "
    );
    for id in 0..num_compartments {
        let path = link_path_format.replace("<read-compartment-id>", &id.to_string());
        out.push_str(&format!(
            "\n        <li><a href=\"{}\">Compartment {id}</a></li>\n    ",
            html_escape(&path)
        ));
    }
    out.push_str("\n    </ul>\n</body>\n</html>\n");
    out
}

/// Go `Ingester.FlushHandler`: `r.ParseForm()` (URL query plus an
/// `application/x-www-form-urlencoded` body; `400` on a parse error), the tenant allow-list is
/// `r.Form["tenant"]` (an empty `tenant=` allows only the empty tenant, an absent one allows all)
/// and the flush is synchronous only when the first `wait` value is `"true"`.
struct CompactionInProgressGuard<'a>(&'a Ingester);
impl Drop for CompactionInProgressGuard<'_> {
    fn drop(&mut self) {
        self.0
            .num_compactions_in_progress
            .fetch_sub(1, Ordering::AcqRel);
    }
}

async fn flush_handler(AxumState(st): AxumState<HttpState>, req: Request) -> Response {
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            "",
            &trace_id,
            "Ingester/FlushHandler",
            crate::activity::ActivityRequest::None,
        )
    });
    let ing_state = st.ingester.state();
    if ing_state != State::Running && ing_state != State::Starting {
        let err_msg = format!("ingester is unavailable (current state: {ing_state})");
        tracing::warn!(err = %err_msg, "flushing TSDB blocks is not allowed");
        return (StatusCode::SERVICE_UNAVAILABLE, err_msg).into_response();
    }

    if st.ingester.compaction_service_state() != State::Running {
        tracing::warn!(
            "flushing TSDB blocks is not allowed because compaction service is not running yet"
        );
        return go_http_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "compaction service is not running",
        );
    }
    let Some(fc_tx) = st.ingester.force_compact_tx.read().clone() else {
        tracing::warn!(
            "flushing TSDB blocks is not allowed because compaction service is not running yet"
        );
        return go_http_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "compaction service is not running",
        );
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
    let ing = Arc::clone(&st.ingester);

    let run = async move {
        let cur_state = ing.state();
        if cur_state != State::Running && cur_state != State::Starting {
            tracing::info!("flushing TSDB blocks: ingester not running, ignoring flush request");
            return;
        }

        ing.num_compactions_in_progress
            .fetch_add(1, Ordering::AcqRel);
        let _guard = CompactionInProgressGuard(&ing);

        tracing::info!("flushing TSDB blocks: triggering compaction");
        let (cb_tx, cb_rx) = oneshot::channel();
        if fc_tx
            .send(TriggerRequest {
                users: users.clone(),
                callback: cb_tx,
            })
            .await
            .is_err()
        {
            tracing::warn!("failed to compact TSDB blocks, ingester not running anymore");
            return;
        }
        if cb_rx.await.is_err() {
            tracing::warn!("failed to compact TSDB blocks, ingester not running anymore");
            return;
        }
        tracing::info!("finished compacting TSDB blocks");

        let ship_tx_opt = ing.ship_tx.read().clone();
        if let Some(ship_tx) = ship_tx_opt {
            tracing::info!("flushing TSDB blocks: triggering shipping");
            let (scb_tx, scb_rx) = oneshot::channel();
            if ship_tx
                .send(TriggerRequest {
                    users,
                    callback: scb_tx,
                })
                .await
                .is_err()
            {
                tracing::warn!("failed to ship TSDB blocks, ingester not running anymore");
                return;
            }
            if scb_rx.await.is_err() {
                tracing::warn!("failed to ship TSDB blocks, ingester not running anymore");
                return;
            }
            tracing::info!("shipping of TSDB blocks finished");
        }
        tracing::info!("flushing TSDB blocks: finished");
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
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            "",
            &trace_id,
            "Ingester/PrepareShutdownHandler",
            crate::activity::ActivityRequest::None,
        )
    });
    if st.ingester.state() != State::Running {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let marker_path = shutdownmarker::get_path(&st.ingester.blocks_cfg.tsdb.dir);
    match method {
        Method::GET => match shutdownmarker::exists(&marker_path) {
            Ok(true) => (StatusCode::OK, "set\n").into_response(),
            Ok(false) => (StatusCode::OK, "unset\n").into_response(),
            Err(err) => {
                tracing::error!(
                    path = %marker_path.display(),
                    err = %err,
                    "unable to check for prepare-shutdown marker file"
                );
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
        Method::POST => {
            if let Err(err) = shutdownmarker::create(&marker_path) {
                tracing::error!(
                    path = %marker_path.display(),
                    err = %err,
                    "unable to create prepare-shutdown marker file"
                );
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            st.ingester.set_prepare_shutdown();
            tracing::info!(
                path = %marker_path.display(),
                "created prepare-shutdown marker file"
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Method::DELETE => {
            if st.ingester.ingest_cfg.enabled {
                tracing::error!(
                    "the ingest storage doesn't support reverting the prepared shutdown"
                );
                return StatusCode::METHOD_NOT_ALLOWED.into_response();
            }
            if let Err(err) = shutdownmarker::remove(&marker_path) {
                tracing::error!(
                    path = %marker_path.display(),
                    err = %err,
                    "unable to remove prepare-shutdown marker file"
                );
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            st.ingester.unset_prepare_shutdown();
            tracing::info!(
                path = %marker_path.display(),
                "removed prepare-shutdown marker file"
            );
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

#[derive(Serialize, Deserialize)]
struct PrepareUnregisterBody {
    unregister: Option<bool>,
}

/// Decodes the first JSON value from `body` matching `unregister` case-insensitively
/// like Go's `json.NewDecoder(r.Body).Decode(&input)`.
fn decode_prepare_unregister_body(body: &[u8]) -> Option<bool> {
    let mut de = serde_json::Deserializer::from_slice(body).into_iter::<serde_json::Value>();
    let first = de.next()?.ok()?;
    let obj = first.as_object()?;
    if let Some(exact) = obj.get("unregister") {
        return exact.as_bool();
    }
    let (_, val) = obj
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("unregister"))?;
    val.as_bool()
}

async fn prepare_unregister_handler(
    AxumState(st): AxumState<HttpState>,
    method: Method,
    body: Bytes,
) -> Response {
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            "",
            &trace_id,
            "Ingester/PrepareUnregisterHandler",
            crate::activity::ActivityRequest::None,
        )
    });
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
            let Some(val) = decode_prepare_unregister_body(&body) else {
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
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            "",
            &trace_id,
            "Ingester/PreparePartitionDownscaleHandler",
            crate::activity::ActivityRequest::None,
        )
    });
    if st.ingester.state() != State::Running {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if !st.ingester.ingest_cfg.enabled {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let Some(ref plc) = st.ingester.partition_lifecycler else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let partition_id = plc.partition_id();

    match method {
        Method::POST => {
            let state = match plc.get_partition_state().await {
                Ok((s, _)) => s,
                Err(e) => {
                    tracing::error!(
                        partition = partition_id,
                        err = %e,
                        "failed to check partition state in the ring"
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };
            if state == PartitionState::PartitionPending {
                tracing::warn!(
                    partition = partition_id,
                    "received a request to prepare partition for shutdown, but the request can't be satisfied because the partition is in PENDING state"
                );
                return StatusCode::CONFLICT.into_response();
            }
            if let Err(e) = plc
                .change_partition_state(PartitionState::PartitionInactive)
                .await
            {
                tracing::error!(
                    partition = partition_id,
                    err = %e,
                    "failed to change partition state to inactive"
                );
                // Only `ring.ErrPartitionStateChangeLocked` maps to 409 Conflict.
                let status = if matches!(e, PartitionRingError::PartitionStateChangeLocked) {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                return go_http_error(status, &e.to_string());
            }
        }
        Method::DELETE => {
            let state = match plc.get_partition_state().await {
                Ok((s, _)) => s,
                Err(e) => {
                    tracing::error!(
                        partition = partition_id,
                        err = %e,
                        "failed to check partition state in the ring"
                    );
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };
            if state == PartitionState::PartitionInactive {
                if let Err(e) = plc
                    .change_partition_state(PartitionState::PartitionActive)
                    .await
                {
                    tracing::error!(
                        partition = partition_id,
                        err = %e,
                        "failed to change partition state to active"
                    );
                    // Only `ring.ErrPartitionStateChangeLocked` maps to 409 Conflict.
                    let status = if matches!(e, PartitionRingError::PartitionStateChangeLocked) {
                        StatusCode::CONFLICT
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    };
                    return go_http_error(status, &e.to_string());
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
        Err(e) => {
            tracing::error!(
                partition = partition_id,
                err = %e,
                "failed to check partition state in the ring"
            );
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn prepare_instance_ring_downscale_handler(
    AxumState(st): AxumState<HttpState>,
    method: Method,
) -> Response {
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            "",
            &trace_id,
            "Ingester/PrepareInstanceRingDownscaleHandler",
            crate::activity::ActivityRequest::None,
        )
    });
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
                tracing::error!(
                    err = %e,
                    "failed to set ingester to read-only mode in the ring"
                );
                return go_http_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
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
                let msg = "cannot clear read-only mode while compaction is pending or in progress";
                tracing::warn!("{msg}");
                return go_http_error(StatusCode::CONFLICT, msg);
            }
            if let Err(e) = lc.change_read_only_state(false).await {
                tracing::error!(err = %e, "failed to clear ingester's read-only mode");
                return go_http_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
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
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            "",
            &trace_id,
            "Ingester/ShutdownHandler",
            crate::activity::ActivityRequest::None,
        )
    });
    // Run shutdown + flag restoration inside a detached task so an HTTP client
    // disconnecting mid-request cannot cancel `stop()` or leave mutated shutdown
    // flags behind.
    let ing = Arc::clone(&st.ingester);
    let handle = tokio::spawn(async move {
        let _shutdown_guard = ing.shutdown_mtx.lock().await;
        struct RestoreFlagsGuard {
            ing: Arc<Ingester>,
            orig_ing_flush: bool,
            orig_lc: Option<(bool, bool)>,
        }
        impl Drop for RestoreFlagsGuard {
            fn drop(&mut self) {
                self.ing
                    .flush_on_shutdown
                    .store(self.orig_ing_flush, Ordering::Release);
                if let (Some(ref lc), Some((orig_flush, orig_unreg))) =
                    (&self.ing.lifecycler, self.orig_lc)
                {
                    lc.set_flush_on_shutdown(orig_flush);
                    lc.set_unregister_on_shutdown(orig_unreg);
                }
            }
        }
        let orig_ing_flush = ing.flush_on_shutdown.swap(true, Ordering::AcqRel);
        let orig_lc = ing.lifecycler.as_ref().map(|lc| {
            let orig_flush = lc.flush_on_shutdown();
            let orig_unreg = lc.should_unregister_on_shutdown();
            lc.set_flush_on_shutdown(true);
            lc.set_unregister_on_shutdown(true);
            (orig_flush, orig_unreg)
        });
        let _restore_guard = RestoreFlagsGuard {
            ing: Arc::clone(&ing),
            orig_ing_flush,
            orig_lc,
        };
        let _ = ing.stop().await;
    });
    let _ = handle.await;
    StatusCode::NO_CONTENT.into_response()
}

/// The tenant of an `auth=true` route: dskit `AuthenticateUser` (`401 no org id` without
/// `X-Scope-OrgID`) followed by Mimir's `newTenantValidationMiddleware` (`401` on an invalid
/// tenant ID, `422` when `!federation && n > 1` or `federation && max_tenants > 0 && n > max_tenants`)
/// and the handler's own `tenant.TenantID` (`401 multiple org IDs present` when federation allows
/// multiple tenants through the middleware). Without multi-tenancy `noauth` injects the no-auth
/// tenant and ignores the header.
fn authenticated_tenant(
    st: &HttpState,
    headers: &HeaderMap,
    raw_org_id: Option<&RawScopeOrgId>,
) -> Result<String, (StatusCode, String)> {
    if !st.ingester.is_multitenancy_enabled() {
        return Ok(st.ingester.no_auth_tenant());
    }
    let org_bytes = match raw_org_id.filter(|r| !r.0.is_empty()) {
        Some(raw) => raw.0.as_slice(),
        None => extract_org_id_bytes_from_http_headers(headers)
            .map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?,
    };
    let ids =
        parse_tenant_ids_bytes(org_bytes).map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;
    let (federation, max_tenants) = st.ingester.tenant_federation();
    if !federation && ids.len() > 1 {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "too many tenant IDs present in the request. max: 1 actual: {}",
                ids.len()
            ),
        ));
    }
    if federation && max_tenants > 0 && ids.len() > max_tenants as usize {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "too many tenant IDs present in the request. max: {max_tenants} actual: {}",
                ids.len()
            ),
        ));
    }
    parse_single_tenant_id_bytes(org_bytes).map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))
}

fn parse_name_filter(raw_query: Option<&str>) -> Option<HashSet<String>> {
    let q = raw_query?;
    let mut names = HashSet::new();
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

fn join_accept_headers(headers: &HeaderMap) -> Option<String> {
    let mut joined = String::new();
    for v in headers.get_all(header::ACCEPT) {
        if let Ok(s) = std::str::from_utf8(v.as_bytes()) {
            if !joined.is_empty() {
                joined.push(',');
            }
            joined.push_str(s);
        }
    }
    (!joined.is_empty()).then_some(joined)
}

async fn user_registry_handler(
    AxumState(st): AxumState<HttpState>,
    RawQuery(raw_query): RawQuery,
    raw_org_id: Option<axum::Extension<RawScopeOrgId>>,
    headers: HeaderMap,
) -> Response {
    let user_id = match authenticated_tenant(&st, &headers, raw_org_id.as_ref().map(|e| &e.0)) {
        Ok(id) => id,
        Err((status, msg)) => return go_http_error(status, &msg),
    };
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            &user_id,
            &trace_id,
            "Ingester/UserRegistryHandler",
            crate::activity::ActivityRequest::None,
        )
    });
    let Some(reg) = st.ingester.tsdb_metrics.registry_for_tenant(&user_id) else {
        return go_http_error(StatusCode::NOT_FOUND, "user registry not found");
    };
    let mut families = reg.gather();
    if let Some(filter) = parse_name_filter(raw_query.as_deref()) {
        families.retain(|mf| filter.contains(mf.get_name()));
    }
    let accept = join_accept_headers(&headers);
    let format = crate::expfmt::negotiate_with_options(accept.as_deref(), false);
    match crate::expfmt::encode(&families, &format) {
        Ok(body) => {
            let mut resp = Response::new(axum::body::Body::from(body));
            *resp.status_mut() = StatusCode::OK;
            resp.headers_mut()
                .insert(header::VARY, axum::http::HeaderValue::from_static("Accept"));
            if let Ok(ct) = axum::http::HeaderValue::from_str(&format.content_type) {
                resp.headers_mut().insert(header::CONTENT_TYPE, ct);
            }
            resp
        }
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
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let _act = st.ingester.insert_activity(|| {
            crate::activity::request_activity(
                "",
                &trace_id,
                "Ingester/TenantsHandler",
                crate::activity::ActivityRequest::None,
            )
        });
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
    })
    .await
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
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
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    tokio::task::spawn_blocking(move || {
        let _act = st.ingester.insert_activity(|| {
            crate::activity::request_activity(
                "",
                &trace_id,
                "Ingester/TenantTSDBHandler",
                crate::activity::ActivityRequest::None,
            )
        });
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
    })
    .await
    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
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

fn query_first_value(query: &str, target: &str) -> Option<String> {
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (raw_k, raw_v) = match part.split_once('=') {
            Some((k, v)) => (k, v),
            None => (part, ""),
        };
        if percent_decode_form(raw_k) == target {
            return Some(percent_decode_form(raw_v));
        }
    }
    None
}

async fn label_churn_handler(
    AxumState(st): AxumState<HttpState>,
    uri: Uri,
) -> Response {
    let trace_id = mimir_common::tracing::current_trace_id().unwrap_or_default();
    let _act = st.ingester.insert_activity(|| {
        crate::activity::request_activity(
            "",
            &trace_id,
            "Ingester/LabelChurnHandler",
            crate::activity::ActivityRequest::None,
        )
    });
    if !st.ingester.cfg.label_churn_enabled {
        return go_http_error(
            StatusCode::NOT_FOUND,
            "label churn detection is disabled, enable it with -ingester.label-churn-enabled",
        );
    }
    let query_str = uri.query().unwrap_or_default();
    let lookback = match query_first_value(query_str, "lookback") {
        Some(ref lb) if !lb.is_empty() => match parse_model_duration(lb) {
            Ok(d_ns) if d_ns > 0 => Duration::from_nanos(d_ns as u64),
            _ => {
                return go_http_error(
                    StatusCode::BAD_REQUEST,
                    "the lookback parameter must be a positive duration",
                );
            }
        },
        _ => DEFAULT_LABEL_CHURN_LOOKBACK,
    };

    let limit = match query_first_value(query_str, "limit") {
        Some(ref lim_str) if !lim_str.is_empty() => match lim_str.parse::<i64>() {
            Ok(l) if l >= 0 => l as usize,
            _ => {
                return go_http_error(
                    StatusCode::BAD_REQUEST,
                    "the limit parameter must be a non-negative integer",
                );
            }
        },
        _ => DEFAULT_LABEL_CHURN_LIMIT,
    };

    let tenant = uri
        .path()
        .strip_prefix("/ingester/label_churn/")
        .unwrap_or_default()
        .to_string();
    let Some(db) = st.ingester.get_tsdb(&tenant) else {
        return go_http_error(
            StatusCode::NOT_FOUND,
            &format!("the ingester has no TSDB for tenant {tenant}"),
        );
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
        Err(e @ (LabelChurnError::CompactedTooRecently | LabelChurnError::NoRecord)) => {
            go_http_error(StatusCode::SERVICE_UNAVAILABLE, &e.to_string())
        }
        Err(e) => go_http_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
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

    #[tokio::test]
    async fn wave3_label_churn_503_bodies_match_go_http_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cfg = mimir_config::IngesterConfig::default();
        cfg.ring.instance_id = "test-0".to_string();
        cfg.label_churn_enabled = true;
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

        let db = ing.get_or_create_tsdb("t1").unwrap();
        let app = ingester_router(Arc::clone(&ing));

        // NoRecord -> 503 with Go's exact explanation + trailing newline.
        let (code, h, body) = get(&app, "/ingester/label_churn/t1?lookback=1ms", &[]).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(h.get("content-type").unwrap(), "text/plain; charset=utf-8");
        assert_eq!(h.get("x-content-type-options").unwrap(), "nosniff");
        assert_eq!(
            body,
            "the ingester has no record in the lookback window, the ingester records once per minute after it starts\n"
        );

        // CompactedTooRecently -> 503 with Go's exact explanation + trailing newline.
        db.pending_series_removals.store(1, Ordering::Release);
        let (code, _, body) = get(&app, "/ingester/label_churn/t1?lookback=30m", &[]).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body,
            "a compaction removed series after the start of the lookback window, retry later or use a shorter lookback\n"
        );
    }

    #[test]
    fn decode_prepare_unregister_body_is_case_insensitive() {
        assert_eq!(decode_prepare_unregister_body(br#"{"unregister":true}"#), Some(true));
        assert_eq!(decode_prepare_unregister_body(br#"{"Unregister":true}"#), Some(true));
        assert_eq!(decode_prepare_unregister_body(br#"{"UNREGISTER":false}"#), Some(false));
        assert_eq!(decode_prepare_unregister_body(br#"{"unregister":null}"#), None);
        assert_eq!(decode_prepare_unregister_body(br#"{}"#), None);
    }

    #[tokio::test]
    async fn shutdown_handler_and_stop_survive_client_disconnect_and_restore_flags() {
        use mimir_common::services::State;

        let tmp = tempfile::tempdir().unwrap();
        let ing = test_ingester(&tmp);
        ing.flush_on_shutdown.store(false, Ordering::Release);
        ing.start().await.unwrap();
        let _ = ing.get_or_create_tsdb("t-disc").unwrap();
        let app = ingester_router(Arc::clone(&ing));

        // Simulate a client disconnecting immediately after sending POST /ingester/shutdown.
        let req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/ingester/shutdown")
            .body(axum::body::Body::empty())
            .unwrap();
        let fut = app.oneshot(req);
        tokio::pin!(fut);
        tokio::select! {
            _ = &mut fut => {}
            _ = tokio::task::yield_now() => {
                // Dropped fut mid-flight to simulate client disconnect.
            }
        }

        // Subsequent SIGTERM (ing.stop()) must cleanly await the detached stop task.
        ing.stop().await.unwrap();
        assert_eq!(ing.state(), State::Terminated);
        assert!(ing.tsdbs.is_empty());

        // Wait for the detached shutdown_handler task to release shutdown_mtx after restoring flags.
        let _guard = ing.shutdown_mtx.lock().await;
        assert!(
            !ing.flush_on_shutdown.load(Ordering::Acquire),
            "flush_on_shutdown must be restored even after HTTP client disconnect"
        );
    }
}
