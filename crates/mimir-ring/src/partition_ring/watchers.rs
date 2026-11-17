//! Ported from github.com/grafana/dskit/ring/partition_ring_watchers.go
//!
//! Groups one or more [`PartitionRingWatcher`]s (one per read compartment in Mimir) and exposes
//! them through a single service that starts, stops and fails them together.

use std::sync::Arc;

use mimir_common::services::{
    basic_service, start_manager_and_await_healthy, stop_manager_and_await_stopped,
    ManagedBasicService, ManagerListener, Service, ServiceContext, ServiceError, ServiceManager,
};
use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::ring::PartitionRing;
use super::watcher::PartitionRingWatcher;

/// dskit `ring.PartitionRingWatchers`.
pub struct PartitionRingWatchers {
    /// Indexed by position in the group; always at least one element.
    watchers: Vec<Arc<PartitionRingWatcher>>,
}

impl PartitionRingWatchers {
    /// `NewPartitionRingWatchers`: the watchers must not be started by the caller.
    pub fn new(watchers: Vec<Arc<PartitionRingWatcher>>) -> Result<Arc<Self>, String> {
        if watchers.is_empty() {
            return Err("at least one partition ring watcher must be provided".to_string());
        }
        Ok(Arc::new(Self { watchers }))
    }

    /// Number of watchers in the group.
    pub fn count(&self) -> usize {
        self.watchers.len()
    }

    /// The watcher at the given index.
    pub fn watcher(&self, idx: usize) -> Option<&Arc<PartitionRingWatcher>> {
        self.watchers.get(idx)
    }

    /// The partition ring of the watcher at the given index.
    pub fn partition_ring(&self, idx: usize) -> Option<Arc<PartitionRing>> {
        self.watchers.get(idx).map(|w| w.partition_ring())
    }

    /// The watchers indexed by position in the group.
    pub fn all(&self) -> &[Arc<PartitionRingWatcher>] {
        &self.watchers
    }

    /// The group service running every watcher as a subservice.
    pub fn into_service(self: Arc<Self>) -> Result<Arc<ManagedBasicService>, String> {
        let subservices: Vec<Arc<dyn Service>> = self
            .watchers
            .iter()
            .map(|w| Arc::clone(w).into_service() as Arc<dyn Service>)
            .collect();
        let manager = ServiceManager::new(subservices).map_err(|e| {
            format!("creating partition ring watchers service manager: {e}")
        })?;

        // `services.FailureWatcher` on the subservices manager.
        struct FailureForwarder(mpsc::UnboundedSender<String>);
        impl ManagerListener for FailureForwarder {
            fn failure(&self, service: &Arc<dyn Service>) {
                let err = service
                    .failure_case()
                    .map(|e| e.to_string())
                    .unwrap_or_default();
                let _ = self.0.send(err);
            }
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let listener = manager.add_listener(Arc::new(FailureForwarder(tx)));
        let failures = Arc::new(Mutex::new(Some(rx)));

        let m_start = Arc::clone(&manager);
        let m_stop = Arc::clone(&manager);
        let svc = basic_service(
            Some(Box::new(move |ctx: ServiceContext| {
                let m = Arc::clone(&m_start);
                Box::pin(async move { start_manager_and_await_healthy(ctx, &m).await })
            })),
            Some(Box::new(move |ctx: ServiceContext| {
                let failures = Arc::clone(&failures);
                Box::pin(async move {
                    let Some(mut rx) = failures.lock().take() else {
                        ctx.canceled().await;
                        return Ok(());
                    };
                    tokio::select! {
                        _ = ctx.canceled() => Ok(()),
                        Some(err) = rx.recv() => Err(ServiceError::failure(format!(
                            "partition ring watcher subservice failed: {err}"
                        ))),
                    }
                })
            })),
            Some(Box::new(move |_| {
                let m = Arc::clone(&m_stop);
                let _keep_listener = &listener;
                Box::pin(async move {
                    stop_manager_and_await_stopped(ServiceContext::background(), &m).await
                })
            })),
        )
        .with_name("partition-ring-watchers");
        Ok(svc)
    }
}
