//! Partition ring watcher service.
//!
//! Ported from github.com/grafana/dskit/ring/partition_ring_watcher.go

use std::collections::HashMap;
use std::sync::Arc;

use mimir_common::services::{basic_service, ManagedBasicService, ServiceContext, ServiceError};
use mimir_proto::ringpb::{PartitionRingDesc, PartitionTokenScheme};
use parking_lot::Mutex;
use prometheus::{Gauge, GaugeVec, Opts, Registry};
use tracing::{info, warn};

use super::model::{clean_partition_state_name, PartitionRingDescExt, PartitionRingError};
use super::ring::{PartitionRing, PartitionRingOptions};
use crate::kv::KvClient;

pub trait PartitionRingWatcherDelegate: Send + Sync {
    fn on_partition_ring_changed(
        &self,
        old_ring: &PartitionRingDesc,
        new_ring: &PartitionRingDesc,
    );
}

struct WatcherMetrics {
    num_partitions: GaugeVec,
    max_partition_id: Gauge,
    max_derived_partition_id: Gauge,
}

impl WatcherMetrics {
    fn new(name: &str, reg: Option<&Registry>) -> Self {
        let mut const_labels = HashMap::new();
        const_labels.insert("name".to_string(), name.to_string());

        let num_partitions = GaugeVec::new(
            Opts::new(
                "partition_ring_partitions",
                "Number of partitions by state in the partitions ring.",
            )
            .const_labels(const_labels.clone()),
            &["state"],
        )
        .expect("valid metric");
        let max_partition_id = Gauge::with_opts(
            Opts::new(
                "partition_ring_max_partition_id",
                "Highest partition ID in the ring, or -1 when empty.",
            )
            .const_labels(const_labels.clone()),
        )
        .expect("valid metric");
        let max_derived_partition_id = Gauge::with_opts(
            Opts::new(
                "partition_ring_max_derived_partition_id",
                "Highest partition ID using derived tokens in the ring, or -1 when none.",
            )
            .const_labels(const_labels),
        )
        .expect("valid metric");

        max_partition_id.set(-1.0);
        max_derived_partition_id.set(-1.0);

        if let Some(r) = reg {
            let _ = r.register(Box::new(num_partitions.clone()));
            let _ = r.register(Box::new(max_partition_id.clone()));
            let _ = r.register(Box::new(max_derived_partition_id.clone()));
        }

        Self {
            num_partitions,
            max_partition_id,
            max_derived_partition_id,
        }
    }
}

pub struct PartitionRingWatcher {
    key: String,
    kv: Arc<dyn KvClient>,
    delegate: Mutex<Option<Arc<dyn PartitionRingWatcherDelegate>>>,
    ring: Mutex<Arc<PartitionRing>>,
    opts: PartitionRingOptions,
    metrics: WatcherMetrics,
}

impl PartitionRingWatcher {
    pub fn new(
        name: &str,
        key: &str,
        kv: Arc<dyn KvClient>,
        opts: PartitionRingOptions,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        let empty = PartitionRing::with_options(PartitionRingDesc::default(), opts.clone())
            .expect("empty partition ring is always valid");
        Arc::new(Self {
            key: key.to_string(),
            kv,
            delegate: Mutex::new(None),
            ring: Mutex::new(empty),
            opts,
            metrics: WatcherMetrics::new(name, reg),
        })
    }

    pub fn with_delegate(self: &Arc<Self>, delegate: Arc<dyn PartitionRingWatcherDelegate>) {
        *self.delegate.lock() = Some(delegate);
    }

    /// The partition ring KV client this watcher reads from (Mimir also hands it to the
    /// `/ingester/partition-ring` page to edit partition states).
    pub fn kv_client(&self) -> &Arc<dyn KvClient> {
        &self.kv
    }

    /// The KV key of the partition ring.
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn partition_ring(&self) -> Arc<PartitionRing> {
        self.ring.lock().clone()
    }

    pub fn update_partition_ring(
        &self,
        desc: PartitionRingDesc,
    ) -> Result<(), PartitionRingError> {
        let new_ring = PartitionRing::with_options(desc.clone(), self.opts.clone())?;
        let old_ring = {
            let mut guard = self.ring.lock();
            let old = guard.clone();
            *guard = new_ring.clone();
            old
        };

        if let Some(del) = self.delegate.lock().clone() {
            del.on_partition_ring_changed(old_ring.desc(), &desc);
        }

        let mut max_derived_id = -1i32;
        for (&id, part) in &desc.partitions {
            if part.token_scheme() == PartitionTokenScheme::PartitionTokensSmt512
                && id > max_derived_id
            {
                max_derived_id = id;
            }
        }
        self.metrics
            .max_partition_id
            .set(new_ring.max_partition_id() as f64);
        self.metrics
            .max_derived_partition_id
            .set(max_derived_id as f64);

        for (state, count) in desc.count_partitions_by_state() {
            self.metrics
                .num_partitions
                .with_label_values(&[clean_partition_state_name(state)])
                .set(count as f64);
        }

        for (&pid, part) in &desc.partitions {
            let prev = old_ring.desc().partitions.get(&pid);
            let prev_locked = prev.map(|p| p.state_change_locked).unwrap_or(false);
            if prev.is_none() || part.state_change_locked != prev_locked {
                if part.state_change_locked {
                    warn!(partition_id = pid, "partition state change is locked");
                } else if prev.is_some() {
                    info!(partition_id = pid, "partition state change is unlocked");
                }
            }
        }

        Ok(())
    }

    pub fn into_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        let w_start = self.clone();
        let w_run = self.clone();

        basic_service(
            Some(Box::new(move |_ctx| {
                let w = w_start.clone();
                Box::pin(async move {
                    let val = w.kv.get(&w.key).await.map_err(|e| {
                        ServiceError::failure(format!("unable to initialise ring state: {e}"))
                    })?;
                    let desc = match val {
                        Some(any) => any
                            .downcast::<PartitionRingDesc>()
                            .map(|b| *b)
                            .unwrap_or_default(),
                        None => {
                            info!(key = %w.key, "partition ring doesn't exist in KV store yet");
                            PartitionRingDesc::default()
                        }
                    };
                    w.update_partition_ring(desc)
                        .map_err(|e| ServiceError::failure(e.to_string()))?;
                    Ok(())
                })
            })),
            Some(Box::new(move |ctx: ServiceContext| {
                let w = w_run.clone();
                Box::pin(async move {
                    let watch_err: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
                    let w_cb = w.clone();
                    let err_cb = watch_err.clone();
                    w.kv
                        .watch_key(
                            &w.key,
                            ctx,
                            Box::new(move |any| {
                                if let Ok(desc) = any.downcast::<PartitionRingDesc>() {
                                    if let Err(e) = w_cb.update_partition_ring(*desc) {
                                        *err_cb.lock() = Some(e.to_string());
                                        return false;
                                    }
                                }
                                true
                            }),
                        )
                        .await;
                    if let Some(err) = watch_err.lock().take() {
                        return Err(ServiceError::failure(err));
                    }
                    Ok(())
                })
            })),
            None,
        )
        .with_name("partitions-ring-watcher")
    }
}
