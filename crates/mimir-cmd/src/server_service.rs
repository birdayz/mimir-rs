//! dskit `services.Service` wrapper for `Server`.
//!
//! Ported from github.com/grafana/mimir/pkg/mimir/server_service.go

use std::sync::Arc;

use axum::Router;
use mimir_common::services::{
    ManagedBasicService, RunningFn, Service, ServiceError, StartingFn, StoppingFn,
};
use parking_lot::Mutex;

use crate::server::{Server, ServerShutdownHandle};

pub type ServicesToWaitForFn = Arc<dyn Fn() -> Vec<Arc<dyn Service>> + Send + Sync>;

/// Constructs a `ManagedBasicService` managing `server` lifecycle, matching `NewServerService`.
///
/// Returns `(service, server_control_handle)` so callers (and tests) can invoke `shutdown()` or
/// `stop()` directly on the underlying `Server` while `ServerService` is running.
pub fn new_server_service(
    server: Server,
    router: Router,
    grpc_routes: tonic::service::Routes,
    services_to_wait_for: ServicesToWaitForFn,
) -> (Arc<ManagedBasicService>, Arc<Mutex<Option<ServerShutdownHandle>>>) {
    let server_slot = Arc::new(Mutex::new(Some((server, router, grpc_routes))));
    let handle_slot: Arc<Mutex<Option<ServerShutdownHandle>>> = Arc::new(Mutex::new(None));

    let server_slot_start = Arc::clone(&server_slot);
    let handle_slot_start = Arc::clone(&handle_slot);
    let starting_fn: StartingFn = Box::new(move |_ctx| {
        let server_slot = Arc::clone(&server_slot_start);
        let handle_slot = Arc::clone(&handle_slot_start);
        Box::pin(async move {
            let Some((srv, router, grpc_routes)) = server_slot.lock().take() else {
                return Err(ServiceError::failure("server already started"));
            };
            let running = srv
                .start(router, grpc_routes)
                .map_err(|e| ServiceError::failure(e.to_string()))?;
            *handle_slot.lock() = Some(running.shutdown_handle());
            Ok(())
        })
    });

    let handle_slot_run = Arc::clone(&handle_slot);
    let running_fn: RunningFn = Box::new(move |ctx| {
        let handle_slot = Arc::clone(&handle_slot_run);
        Box::pin(async move {
            let handle = handle_slot.lock().clone();
            let Some(mut handle) = handle else {
                ctx.canceled().await;
                return Ok(());
            };
            tokio::select! {
                _ = ctx.canceled() => Ok(()),
                _ = handle.wait_stopped() => {
                    if ctx.is_canceled() {
                        Ok(())
                    } else {
                        Err(ServiceError::failure("server stopped unexpectedly"))
                    }
                }
            }
        })
    });

    let handle_slot_stop = Arc::clone(&handle_slot);
    let stopping_fn: StoppingFn = Box::new(move |_err| {
        let services_to_wait_for = Arc::clone(&services_to_wait_for);
        let handle_slot = Arc::clone(&handle_slot_stop);
        Box::pin(async move {
            for s in services_to_wait_for() {
                let _ = s.await_terminated().await;
            }
            let handle = handle_slot.lock().clone();
            if let Some(mut h) = handle {
                h.shutdown().await;
            }
            Ok(())
        })
    });

    let svc = ManagedBasicService::new(Some(starting_fn), Some(running_fn), Some(stopping_fn));
    (svc, handle_slot)
}
