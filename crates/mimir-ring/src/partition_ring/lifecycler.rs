//! Partition instance lifecycler and partition ring editor.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/partition_instance_lifecycler.go
//! - github.com/grafana/dskit/ring/partition_ring_editor.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mimir_common::services::{basic_service, ManagedBasicService, ServiceContext, ServiceError};
use mimir_config::ingester::PartitionRingConfig;
use mimir_proto::ringpb::{OwnerState, PartitionRingDesc, PartitionState};
use parking_lot::Mutex;
use prometheus::{CounterVec, Opts};
use tracing::{info, warn};

use super::model::{
    clean_partition_state_name, PartitionDescExt, PartitionRingDescExt, PartitionRingError,
};
use crate::kv::{AsRegisterer, CasCallbackError, CasOutcome, KvClient, KvError, Registerer};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionInstanceLifecyclerConfig {
    pub partition_id: i32,
    pub create_partitions_with_derived_tokens: bool,
    pub max_derived_token_partitions: i32,
    pub instance_id: String,
    pub multi_partition_ownership: bool,
    pub wait_owners_count_on_pending: usize,
    pub wait_owners_duration_on_pending: Duration,
    pub delete_inactive_partition_after: Duration,
    pub polling_interval: Duration,
}

impl PartitionInstanceLifecyclerConfig {
    pub fn from_partition_ring_config(
        cfg: &PartitionRingConfig,
        partition_id: i32,
        instance_id: impl Into<String>,
        polling_interval: Duration,
    ) -> Self {
        Self {
            partition_id,
            create_partitions_with_derived_tokens: cfg.create_partitions_with_derived_tokens,
            max_derived_token_partitions: cfg.max_derived_token_partitions as i32,
            instance_id: instance_id.into(),
            multi_partition_ownership: false,
            wait_owners_count_on_pending: cfg.min_partition_owners_count.max(0) as usize,
            wait_owners_duration_on_pending: cfg
                .min_partition_owners_duration
                .as_std()
                .unwrap_or(Duration::ZERO),
            delete_inactive_partition_after: cfg
                .delete_inactive_partition_after
                .as_std()
                .unwrap_or(Duration::ZERO),
            polling_interval,
        }
    }
}

impl Default for PartitionInstanceLifecyclerConfig {
    fn default() -> Self {
        Self {
            partition_id: 0,
            create_partitions_with_derived_tokens: false,
            max_derived_token_partitions: 0,
            instance_id: String::new(),
            multi_partition_ownership: false,
            wait_owners_count_on_pending: 1,
            wait_owners_duration_on_pending: Duration::from_secs(10),
            delete_inactive_partition_after: Duration::from_secs(13 * 3600),
            polling_interval: Duration::from_secs(5),
        }
    }
}

pub fn multi_partition_owner_instance_id(instance_id: &str, partition_id: i32) -> String {
    format!("{instance_id}/{partition_id}")
}

pub fn is_partition_state_change_allowed(from: PartitionState, to: PartitionState) -> bool {
    matches!(
        (from, to),
        (PartitionState::PartitionPending, PartitionState::PartitionActive)
            | (PartitionState::PartitionPending, PartitionState::PartitionInactive)
            | (PartitionState::PartitionActive, PartitionState::PartitionInactive)
            | (PartitionState::PartitionInactive, PartitionState::PartitionActive)
    )
}

pub fn change_partition_state_in_desc(
    ring: &mut PartitionRingDesc,
    partition_id: i32,
    to_state: PartitionState,
) -> Result<bool, PartitionRingError> {
    let Some(part) = ring.partitions.get(&partition_id) else {
        return Err(PartitionRingError::PartitionDoesNotExist);
    };
    let from_state = part.partition_state();
    if from_state == to_state {
        return Ok(false);
    }
    if !is_partition_state_change_allowed(from_state, to_state) {
        return Err(PartitionRingError::PartitionStateChangeNotAllowed {
            from: from_state,
            to: to_state,
        });
    }
    ring.update_partition_state(partition_id, to_state, SystemTime::now())
}

struct LifecyclerMetrics {
    reconciles_total: CounterVec,
    reconciles_failed_total: CounterVec,
}

impl LifecyclerMetrics {
    fn new(ring_name: &str, reg: Option<&Registerer>) -> Self {
        let mut labels = HashMap::new();
        labels.insert("name".to_string(), ring_name.to_string());
        let reconciles_total = CounterVec::new(
            Opts::new(
                "partition_ring_lifecycler_reconciles_total",
                "Total number of reconciliations started.",
            )
            .const_labels(labels.clone()),
            &["type"],
        )
        .expect("valid metric");
        let reconciles_failed_total = CounterVec::new(
            Opts::new(
                "partition_ring_lifecycler_reconciles_failed_total",
                "Total number of reconciliations failed.",
            )
            .const_labels(labels),
            &["type"],
        )
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(reconciles_total.clone()));
            let _ = r.register(Box::new(reconciles_failed_total.clone()));
        }
        Self {
            reconciles_total,
            reconciles_failed_total,
        }
    }
}

pub struct PartitionInstanceLifecycler {
    cfg: PartitionInstanceLifecyclerConfig,
    ring_name: String,
    ring_key: String,
    store: Arc<dyn KvClient>,
    create_partition_on_startup: AtomicBool,
    remove_owner_on_shutdown: AtomicBool,
    started: AtomicBool,
    running: AtomicBool,
    /// Magnitude (in nanoseconds) of a negative min_partition_owners_duration, preserving Go signed
    /// now.Add(-WaitOwnersDurationOnPending) semantics (before = now + neg).
    wait_owners_negative_duration_nanos: AtomicU64,
    op_lock: tokio::sync::Mutex<()>,
    metrics: LifecyclerMetrics,
}

impl PartitionInstanceLifecycler {
    pub fn new(
        mut cfg: PartitionInstanceLifecyclerConfig,
        ring_name: &str,
        ring_key: &str,
        store: Arc<dyn KvClient>,
        reg: Option<&dyn AsRegisterer>,
    ) -> Result<Arc<Self>, PartitionRingError> {
        let reg = reg.map(|r| r.as_registerer());
        if cfg.partition_id < 0 {
            return Err(PartitionRingError::Other(format!(
                "invalid partition ID {}: must not be negative",
                cfg.partition_id
            )));
        }
        if cfg.create_partitions_with_derived_tokens
            && cfg.partition_id >= cfg.max_derived_token_partitions
        {
            return Err(PartitionRingError::Other(format!(
                "invalid partition ID {}: must be below the maximum derived token partitions ({}) when creating partitions with derived tokens",
                cfg.partition_id, cfg.max_derived_token_partitions
            )));
        }
        if cfg.polling_interval.is_zero() {
            cfg.polling_interval = Duration::from_secs(5);
        }

        Ok(Arc::new(Self {
            cfg,
            ring_name: ring_name.to_string(),
            ring_key: ring_key.to_string(),
            store,
            create_partition_on_startup: AtomicBool::new(true),
            remove_owner_on_shutdown: AtomicBool::new(false),
            started: AtomicBool::new(false),
            running: AtomicBool::new(false),
            wait_owners_negative_duration_nanos: AtomicU64::new(0),
            op_lock: tokio::sync::Mutex::new(()),
            metrics: LifecyclerMetrics::new(ring_name, reg.as_ref()),
        }))
    }

    /// Sets the negative-duration offset for WaitOwnersDurationOnPending when configured with a
    /// negative Go duration (e.g. -1s => before = now + 1s).
    pub fn set_wait_owners_negative_duration(&self, neg: Duration) {
        let nanos = neg.as_nanos().min(u64::MAX as u128) as u64;
        self.wait_owners_negative_duration_nanos
            .store(nanos, Ordering::Release);
    }

    /// Applies the signed min_partition_owners_duration from PartitionRingConfig.
    pub fn apply_min_partition_owners_duration(&self, d: mimir_config::GoDuration) {
        if d.0 < 0 {
            self.set_wait_owners_negative_duration(Duration::from_nanos(d.0.unsigned_abs()));
        } else {
            self.wait_owners_negative_duration_nanos
                .store(0, Ordering::Release);
        }
    }

    pub fn partition_id(&self) -> i32 {
        self.cfg.partition_id
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    pub fn create_partition_on_startup(&self) -> bool {
        self.create_partition_on_startup.load(Ordering::Acquire)
    }

    pub fn set_create_partition_on_startup(&self, create: bool) {
        self.create_partition_on_startup
            .store(create, Ordering::Release);
    }

    pub fn remove_owner_on_shutdown(&self) -> bool {
        self.remove_owner_on_shutdown.load(Ordering::Acquire)
    }

    pub fn set_remove_owner_on_shutdown(&self, remove: bool) {
        self.remove_owner_on_shutdown
            .store(remove, Ordering::Release);
    }

    pub fn partition_owner_id(&self) -> String {
        if self.cfg.multi_partition_ownership {
            multi_partition_owner_instance_id(&self.cfg.instance_id, self.cfg.partition_id)
        } else {
            self.cfg.instance_id.clone()
        }
    }

    pub async fn get_partition_state(
        &self,
    ) -> Result<(PartitionState, SystemTime), PartitionRingError> {
        let desc = self.get_ring().await?;
        let part = desc
            .partitions
            .get(&self.cfg.partition_id)
            .ok_or(PartitionRingError::PartitionDoesNotExist)?;
        Ok((part.partition_state(), part.state_time()))
    }

    pub async fn change_partition_state(
        &self,
        to_state: PartitionState,
    ) -> Result<(), PartitionRingError> {
        if !self.running.load(Ordering::Acquire) {
            return Err(PartitionRingError::Other("lifecycler not running".to_string()));
        }
        let op = self.op_lock.lock().await;
        if !self.running.load(Ordering::Acquire) {
            return Err(PartitionRingError::Other("lifecycler not running".to_string()));
        }
        let pid = self.cfg.partition_id;
        self.update_ring_locked(&op, move |ring| {
            change_partition_state_in_desc(ring, pid, to_state)
        })
        .await
    }

    async fn get_ring(&self) -> Result<PartitionRingDesc, PartitionRingError> {
        let val = self
            .store
            .get(&self.ring_key)
            .await
            .map_err(|e| PartitionRingError::Other(e.to_string()))?;
        Ok(val
            .and_then(|a| a.downcast::<PartitionRingDesc>().ok())
            .map(|b| *b)
            .unwrap_or_default())
    }

    async fn update_ring<F>(&self, update: F) -> Result<(), PartitionRingError>
    where
        F: Fn(&mut PartitionRingDesc) -> Result<bool, PartitionRingError> + Send + Sync + 'static,
    {
        let op = self.op_lock.lock().await;
        self.update_ring_locked(&op, update).await
    }

    async fn update_ring_locked<F>(
        &self,
        _op: &tokio::sync::MutexGuard<'_, ()>,
        update: F,
    ) -> Result<(), PartitionRingError>
    where
        F: Fn(&mut PartitionRingDesc) -> Result<bool, PartitionRingError> + Send + Sync + 'static,
    {
        let typed_err: Arc<Mutex<Option<PartitionRingError>>> = Arc::new(Mutex::new(None));
        let typed_err_cb = Arc::clone(&typed_err);
        let res = self
            .store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let mut next = in_val
                        .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                        .cloned()
                        .unwrap_or_default();
                    let changed = match update(&mut next) {
                        Ok(c) => c,
                        Err(e) => {
                            let msg = e.to_string();
                            *typed_err_cb.lock() = Some(e);
                            return Err(CasCallbackError::fatal(KvError::Other(msg)));
                        }
                    };
                    if !changed {
                        Ok(CasOutcome::NoChange)
                    } else {
                        Ok(CasOutcome::Update(Box::new(next)))
                    }
                }),
            )
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                if let Some(pe) = typed_err.lock().take() {
                    Err(pe)
                } else {
                    Err(PartitionRingError::Other(e.to_string()))
                }
            }
        }
    }

    pub async fn create_partition_and_register_owner(&self) -> Result<(), PartitionRingError> {
        let pid = self.cfg.partition_id;
        let derived = self.cfg.create_partitions_with_derived_tokens;
        let owner_id = self.partition_owner_id();

        self.update_ring(move |ring| {
            let now = SystemTime::now();
            let mut changed = false;
            if !ring.has_partition(pid) {
                if derived {
                    ring.add_partition_with_derived_tokens(
                        pid,
                        PartitionState::PartitionPending,
                        now,
                    );
                } else {
                    ring.add_partition(pid, PartitionState::PartitionPending, now);
                }
                changed = true;
            }
            if ring.add_or_update_owner(&owner_id, OwnerState::OwnerActive, pid, now) {
                changed = true;
            }
            Ok(changed)
        })
        .await
    }

    async fn wait_partition_and_register_owner(
        &self,
        ctx: &ServiceContext,
    ) -> Result<(), PartitionRingError> {
        let pid = self.cfg.partition_id;
        loop {
            let ring = self.get_ring().await?;
            if ring.has_partition(pid) {
                break;
            }
            tokio::select! {
                _ = ctx.canceled() => return Err(PartitionRingError::Other("canceled".to_string())),
                _ = tokio::time::sleep(self.cfg.polling_interval) => {}
            }
        }
        let owner_id = self.partition_owner_id();
        self.update_ring(move |ring| {
            Ok(ring.add_or_update_owner(&owner_id, OwnerState::OwnerActive, pid, SystemTime::now()))
        })
        .await
    }

    pub async fn reconcile_owned_partition(&self, now: SystemTime) {
        self.metrics
            .reconciles_total
            .with_label_values(&["owned-partition"])
            .inc();
        let pid = self.cfg.partition_id;
        let wait_count = self.cfg.wait_owners_count_on_pending;
        let wait_dur = self.cfg.wait_owners_duration_on_pending;
        let wait_neg_nanos = self
            .wait_owners_negative_duration_nanos
            .load(Ordering::Acquire);

        let res = self
            .update_ring(move |ring| {
                let part = ring
                    .partitions
                    .get(&pid)
                    .ok_or(PartitionRingError::PartitionDoesNotExist)?;
                let before = if wait_neg_nanos > 0 {
                    now.checked_add(Duration::from_nanos(wait_neg_nanos))
                        .unwrap_or(now)
                } else {
                    now.checked_sub(wait_dur).unwrap_or(SystemTime::UNIX_EPOCH)
                };
                if part.is_pending()
                    && ring.partition_owners_count_updated_before(pid, before) >= wait_count
                {
                    info!(
                        partition = pid,
                        "switching partition state from Pending to Active"
                    );
                    return ring.update_partition_state(pid, PartitionState::PartitionActive, now);
                }
                Ok(false)
            })
            .await;

        if let Err(err) = res {
            self.metrics
                .reconciles_failed_total
                .with_label_values(&["owned-partition"])
                .inc();
            warn!(partition = pid, %err, "failed to reconcile owned partition");
        }
    }

    pub async fn reconcile_other_partitions(&self, now: SystemTime) {
        self.metrics
            .reconciles_total
            .with_label_values(&["other-partitions"])
            .inc();
        let owned_pid = self.cfg.partition_id;
        let delete_after = self.cfg.delete_inactive_partition_after;

        let res = self
            .update_ring(move |ring| {
                if delete_after.is_zero() {
                    return Ok(false);
                }
                let delete_before = now
                    .checked_sub(delete_after)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let to_remove: Vec<i32> = ring
                    .partitions
                    .iter()
                    .filter_map(|(&pid, part)| {
                        if pid != owned_pid
                            && part.is_inactive_since(delete_before)
                            && ring.partition_owners_count(pid) == 0
                        {
                            Some(pid)
                        } else {
                            None
                        }
                    })
                    .collect();
                let changed = !to_remove.is_empty();
                for pid in to_remove {
                    if let Some(p) = ring.partitions.get(&pid) {
                        info!(
                            partition = pid,
                            state = clean_partition_state_name(p.partition_state()),
                            "removing inactive partition with no owners from ring"
                        );
                    }
                    ring.remove_partition(pid);
                }
                Ok(changed)
            })
            .await;

        if let Err(err) = res {
            self.metrics
                .reconciles_failed_total
                .with_label_values(&["other-partitions"])
                .inc();
            warn!(%err, "failed to reconcile other partitions");
        }
    }

    pub fn into_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        self.started.store(true, Ordering::Release);
        let l_start = self.clone();
        let l_run = self.clone();
        let l_stop = self.clone();

        basic_service(
            Some(Box::new(move |ctx| {
                let l = l_start.clone();
                Box::pin(async move {
                    if l.create_partition_on_startup() {
                        tokio::select! {
                            res = l.create_partition_and_register_owner() => {
                                res.map_err(|e| ServiceError::failure(e.to_string()))?;
                            }
                            _ = ctx.canceled() => return Err(ServiceError::Canceled),
                        }
                    } else {
                        tokio::select! {
                            res = l.wait_partition_and_register_owner(&ctx) => {
                                res.map_err(|e| ServiceError::failure(e.to_string()))?;
                            }
                            _ = ctx.canceled() => return Err(ServiceError::Canceled),
                        }
                    }
                    if ctx.is_canceled() {
                        return Err(ServiceError::Canceled);
                    }
                    l.running.store(true, Ordering::Release);
                    Ok(())
                })
            })),
            Some(Box::new(move |ctx: ServiceContext| {
                let l = l_run.clone();
                Box::pin(async move {
                    tokio::select! {
                        biased;
                        _ = ctx.canceled() => {
                            l.running.store(false, Ordering::Release);
                            return Ok(());
                        }
                        _ = async {
                            l.reconcile_owned_partition(SystemTime::now()).await;
                            l.reconcile_other_partitions(SystemTime::now()).await;
                        } => {}
                    }

                    let mut ticker = tokio::time::interval(l.cfg.polling_interval);
                    ticker.tick().await;
                    loop {
                        tokio::select! {
                            biased;
                            _ = ctx.canceled() => break,
                            _ = ticker.tick() => {
                                let now = SystemTime::now();
                                tokio::select! {
                                    biased;
                                    _ = ctx.canceled() => break,
                                    _ = async {
                                        l.reconcile_owned_partition(now).await;
                                        l.reconcile_other_partitions(now).await;
                                    } => {}
                                }
                            }
                        }
                    }
                    l.running.store(false, Ordering::Release);
                    Ok(())
                })
            })),
            Some(Box::new(move |_| {
                let l = l_stop.clone();
                Box::pin(async move {
                    l.running.store(false, Ordering::Release);
                    info!(ring = %l.ring_name, "partition ring lifecycler shutting down");
                    if l.remove_owner_on_shutdown() {
                        let owner_id = l.partition_owner_id();
                        if let Err(err) = l
                            .update_ring(move |ring| Ok(ring.remove_owner(&owner_id)))
                            .await
                        {
                            warn!(ring = %l.ring_name, %err, "failed to remove owner on shutdown");
                        }
                    }
                    Ok(())
                })
            })),
        )
        .with_name("partition-instance-lifecycler")
    }
}

/// Editor for mutating partition state & locks in the KV store.
pub struct PartitionRingEditor {
    key: String,
    kv: Arc<dyn KvClient>,
}

impl PartitionRingEditor {
    pub fn new(key: impl Into<String>, kv: Arc<dyn KvClient>) -> Self {
        Self {
            key: key.into(),
            kv,
        }
    }

    pub async fn change_partition_state(
        &self,
        partition_id: i32,
        to_state: PartitionState,
    ) -> Result<(), PartitionRingError> {
        self.update_ring(move |desc| change_partition_state_in_desc(desc, partition_id, to_state))
            .await
    }

    pub async fn remove_multi_partition_owner(
        &self,
        instance_id: &str,
        partition_id: i32,
    ) -> Result<(), PartitionRingError> {
        let owner_id = multi_partition_owner_instance_id(instance_id, partition_id);
        self.update_ring(move |desc| Ok(desc.remove_owner(&owner_id)))
            .await
    }

    pub async fn set_partition_state_change_lock(
        &self,
        partition_id: i32,
        locked: bool,
    ) -> Result<(), PartitionRingError> {
        self.update_ring(move |desc| {
            if !desc.has_partition(partition_id) {
                return Err(PartitionRingError::PartitionDoesNotExist);
            }
            Ok(desc.update_partition_state_change_lock(partition_id, locked, SystemTime::now()))
        })
        .await
    }

    async fn update_ring<F>(&self, update: F) -> Result<(), PartitionRingError>
    where
        F: Fn(&mut PartitionRingDesc) -> Result<bool, PartitionRingError> + Send + Sync + 'static,
    {
        let typed_err: Arc<Mutex<Option<PartitionRingError>>> = Arc::new(Mutex::new(None));
        let typed_err_cb = Arc::clone(&typed_err);
        let res = self
            .kv
            .cas(
                &self.key,
                Box::new(move |in_val| {
                    let mut desc = in_val
                        .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                        .cloned()
                        .unwrap_or_default();
                    let changed = match update(&mut desc) {
                        Ok(c) => c,
                        Err(e) => {
                            let msg = e.to_string();
                            *typed_err_cb.lock() = Some(e);
                            return Err(CasCallbackError::fatal(KvError::Other(msg)));
                        }
                    };
                    if !changed {
                        Ok(CasOutcome::NoChange)
                    } else {
                        Ok(CasOutcome::Update(Box::new(desc)))
                    }
                }),
            )
            .await;
        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                if let Some(pe) = typed_err.lock().take() {
                    Err(pe)
                } else {
                    Err(PartitionRingError::Other(e.to_string()))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::inmemory::InMemoryKv;
    use crate::partition_ring::model::partition_ring_desc_codec;

    #[test]
    fn negative_durations_default_to_zero() {
        let cfg = PartitionRingConfig {
            min_partition_owners_duration: mimir_config::GoDuration(-1),
            delete_inactive_partition_after: mimir_config::GoDuration(-1),
            ..Default::default()
        };
        let plc_cfg = PartitionInstanceLifecyclerConfig::from_partition_ring_config(
            &cfg,
            0,
            "ing-0",
            Duration::from_secs(5),
        );
        assert_eq!(plc_cfg.wait_owners_duration_on_pending, Duration::ZERO);
        assert_eq!(plc_cfg.delete_inactive_partition_after, Duration::ZERO);
    }

    #[tokio::test]
    async fn change_partition_state_requires_running_service() {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let plc = PartitionInstanceLifecycler::new(
            PartitionInstanceLifecyclerConfig {
                partition_id: 0,
                instance_id: "ing-0".to_string(),
                ..Default::default()
            },
            "ingester-partitions",
            "ingester-partitions",
            kv,
            None,
        )
        .unwrap();
        let err = plc
            .change_partition_state(PartitionState::PartitionActive)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("lifecycler not running"));
    }

    #[tokio::test]
    async fn w23_csc_04_negative_partition_ring_durations_match_go_signed_semantics() {
        let cfg = PartitionRingConfig {
            min_partition_owners_count: 1,
            min_partition_owners_duration: mimir_config::GoDuration::from_secs(-1),
            delete_inactive_partition_after: mimir_config::GoDuration::from_secs(-1),
            ..Default::default()
        };
        let plc_cfg = PartitionInstanceLifecyclerConfig::from_partition_ring_config(
            &cfg,
            0,
            "ing-0",
            Duration::from_secs(5),
        );
        assert_eq!(plc_cfg.wait_owners_duration_on_pending, Duration::ZERO);
        assert_eq!(plc_cfg.delete_inactive_partition_after, Duration::ZERO);

        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let plc = PartitionInstanceLifecycler::new(
            plc_cfg,
            "ingester-partitions",
            "ingester-partitions",
            kv.clone(),
            None,
        )
        .unwrap();
        plc.apply_min_partition_owners_duration(cfg.min_partition_owners_duration);

        let now = SystemTime::now();
        kv.cas(
            "ingester-partitions",
            Box::new(move |in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_partition(0, PartitionState::PartitionPending, now);
                desc.add_or_update_owner("ing-0", OwnerState::OwnerActive, 0, now);
                let old_ts = now - Duration::from_secs(24 * 3600);
                desc.add_partition(1, PartitionState::PartitionInactive, old_ts);
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

        plc.reconcile_owned_partition(now).await;
        plc.reconcile_other_partitions(now).await;

        let desc = plc.get_ring().await.unwrap();
        assert_eq!(
            desc.partitions.get(&0).unwrap().partition_state(),
            PartitionState::PartitionActive
        );
        assert!(
            desc.partitions.contains_key(&1),
            "negative delete_inactive_partition_after must disable deletion"
        );
    }

    #[tokio::test]
    async fn c01_partition_lifecycler_cancels_promptly_on_hanging_reconcile() {
        use crate::kv::BoxFuture;
        use mimir_common::services::Service;
        use std::sync::atomic::AtomicUsize;

        struct HangOnReconcileKv {
            cas_calls: AtomicUsize,
        }

        impl KvClient for HangOnReconcileKv {
            fn list<'a>(&'a self, _p: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                Box::pin(async { Ok(Vec::new()) })
            }
            fn get<'a>(
                &'a self,
                _k: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                Box::pin(async { Ok(None) })
            }
            fn delete<'a>(&'a self, _k: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async { Ok(()) })
            }
            fn cas<'a>(
                &'a self,
                _k: &'a str,
                mut f: crate::kv::CasFn,
            ) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    let n = self.cas_calls.fetch_add(1, Ordering::SeqCst);
                    if n >= 1 {
                        std::future::pending::<()>().await;
                    }
                    let _ = f(None);
                    Ok(())
                })
            }
            fn watch_key<'a>(
                &'a self,
                _k: &'a str,
                ctx: ServiceContext,
                _f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async move {
                    ctx.canceled().await;
                })
            }
            fn watch_prefix<'a>(
                &'a self,
                _p: &'a str,
                ctx: ServiceContext,
                _f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async move {
                    ctx.canceled().await;
                })
            }
        }

        let cfg = PartitionInstanceLifecyclerConfig {
            partition_id: 0,
            instance_id: "ing-0".into(),
            polling_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let plc = PartitionInstanceLifecycler::new(
            cfg,
            "ingester-partitions",
            "ingester-partitions",
            Arc::new(HangOnReconcileKv {
                cas_calls: AtomicUsize::new(0),
            }),
            None,
        )
        .unwrap();
        plc.set_create_partition_on_startup(true);
        plc.set_remove_owner_on_shutdown(false);
        let svc = plc.into_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        svc.stop_async();
        tokio::time::timeout(Duration::from_millis(500), svc.await_terminated())
            .await
            .expect("PartitionInstanceLifecycler must abort hanging reconcile on ctx cancellation")
            .unwrap();
    }
}
