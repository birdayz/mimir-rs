//! Owned-series tracking & ring strategies matching `pkg/ingester/owned_series.go`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_common::services::{
    basic_service, start_and_await_running, stop_and_await_terminated, ManagedBasicService,
    Service, ServiceContext, ServiceError, State,
};
use mimir_config::Overrides;
use mimir_proto::ringpb::InstanceState;
use mimir_proto::sharding::shard_by_all_labels;
use mimir_ring::partition_ring::PartitionRingWatcher;
use mimir_ring::ring::replication_strategy::{Operation, ReplicationSet};
use mimir_ring::ring::{Ring, RingError};
use mimir_tsdb::labels::Labels;
use parking_lot::Mutex;
use prometheus::{Histogram, HistogramOpts, Registry};
use tracing::{debug, info, warn};

use crate::user_tsdb::UserTsdb;

pub const RECOMPUTE_OWNED_SERIES_REASON_EARLY_COMPACTION: &str = "early compaction";
pub const RECOMPUTE_OWNED_SERIES_REASON_COMPACTION: &str = "compaction";
pub const RECOMPUTE_OWNED_SERIES_REASON_NEW_USER: &str = "new user";
pub const RECOMPUTE_OWNED_SERIES_REASON_GET_TOKEN_RANGES_FAILED: &str =
    "token ranges check failed";
pub const RECOMPUTE_OWNED_SERIES_REASON_RING_CHANGED: &str = "ring changed";
pub const RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED: &str = "shard size changed";
pub const RECOMPUTE_OWNED_SERIES_REASON_LOCAL_LIMIT_CHANGED: &str = "local series limit changed";

/// Returns `true` if `key` falls inside any inclusive `[start, end]` pair in `ranges`.
#[inline]
pub fn token_ranges_include_key(ranges: &[u32], key: u32) -> bool {
    if ranges.is_empty() {
        return false;
    }
    let idx = ranges.partition_point(|&v| v < key);
    if idx < ranges.len() {
        if ranges[idx] == key {
            return true;
        }
        return idx % 2 == 1;
    }
    false
}

/// Computes the stable FNV-1 secondary hash for `(user_id, labels)` matching `ShardByAllLabels`.
#[inline]
pub fn secondary_tsdb_hash_for_user(user_id: &str, labels: &Labels) -> u32 {
    shard_by_all_labels(user_id, labels.iter())
}

pub trait OwnedSeriesRingStrategy: Send + Sync {
    fn check_ring_for_changes(&self) -> Result<bool, String>;
    fn shard_size_for_user(&self, tenant: &str) -> usize;
    fn token_ranges_for_user(&self, user_id: &str, shard_size: usize) -> Result<Vec<u32>, String>;
    fn owner_key_and_value(&self) -> (&'static str, String);
}

fn owned_series_ring_op() -> Operation {
    Operation::new(
        &[
            InstanceState::Pending,
            InstanceState::Joining,
            InstanceState::Active,
            InstanceState::Leaving,
        ],
        &[],
    )
}

pub struct OwnedSeriesIngesterRingStrategy {
    instance_id: String,
    ring: Arc<Ring>,
    overrides: Overrides,
    previous_ring: Mutex<Option<ReplicationSet>>,
}

impl OwnedSeriesIngesterRingStrategy {
    pub fn new(instance_id: impl Into<String>, ring: Arc<Ring>, overrides: Overrides) -> Self {
        Self {
            instance_id: instance_id.into(),
            ring,
            overrides,
            previous_ring: Mutex::new(None),
        }
    }
}

fn replication_set_changed_without_state_or_addr(
    prev: Option<&ReplicationSet>,
    curr: &ReplicationSet,
) -> bool {
    let Some(p) = prev else {
        return true;
    };
    if p.instances.len() != curr.instances.len() {
        return true;
    }
    let mut p_ids: Vec<(&str, &[u32], &str)> = p
        .instances
        .iter()
        .map(|i| (i.id.as_str(), i.tokens.as_slice(), i.zone.as_str()))
        .collect();
    let mut c_ids: Vec<(&str, &[u32], &str)> = curr
        .instances
        .iter()
        .map(|i| (i.id.as_str(), i.tokens.as_slice(), i.zone.as_str()))
        .collect();
    p_ids.sort_unstable();
    c_ids.sort_unstable();
    p_ids != c_ids
}

impl OwnedSeriesRingStrategy for OwnedSeriesIngesterRingStrategy {
    fn check_ring_for_changes(&self) -> Result<bool, String> {
        let rs = self
            .ring
            .get_all_healthy(owned_series_ring_op())
            .map_err(|e| e.to_string())?;
        let mut prev = self.previous_ring.lock();
        let changed = replication_set_changed_without_state_or_addr(prev.as_ref(), &rs);
        *prev = Some(rs);
        Ok(changed)
    }

    fn shard_size_for_user(&self, tenant: &str) -> usize {
        self.overrides
            .for_tenant(tenant)
            .ingestion_tenant_shard_size
            .max(0) as usize
    }

    fn token_ranges_for_user(&self, user_id: &str, shard_size: usize) -> Result<Vec<u32>, String> {
        let subring = self.ring.shuffle_shard(user_id, shard_size);
        match subring.get_token_ranges_for_instance(&self.instance_id) {
            Ok(ranges) => Ok(ranges),
            Err(RingError::InstanceNotFound) => Ok(Vec::new()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn owner_key_and_value(&self) -> (&'static str, String) {
        ("ingester", self.instance_id.clone())
    }
}

pub struct OwnedSeriesPartitionRingStrategy {
    partition_id: i32,
    watcher: Arc<PartitionRingWatcher>,
    overrides: Overrides,
    previous_active_partitions: Mutex<Vec<i32>>,
}

impl OwnedSeriesPartitionRingStrategy {
    pub fn new(
        partition_id: i32,
        watcher: Arc<PartitionRingWatcher>,
        overrides: Overrides,
    ) -> Self {
        Self {
            partition_id,
            watcher,
            overrides,
            previous_active_partitions: Mutex::new(Vec::new()),
        }
    }
}

impl OwnedSeriesRingStrategy for OwnedSeriesPartitionRingStrategy {
    fn check_ring_for_changes(&self) -> Result<bool, String> {
        let r = self.watcher.partition_ring();
        if r.partitions_count() == 0 {
            return Err("empty ring".to_string());
        }
        let active = r.active_partition_ids();
        let mut prev = self.previous_active_partitions.lock();
        let changed = *prev != active;
        if changed {
            *prev = active;
        }
        Ok(changed)
    }

    fn shard_size_for_user(&self, tenant: &str) -> usize {
        let l = self.overrides.for_tenant(tenant);
        l.effective_ingestion_partitions_tenant_write_shard_size().max(0) as usize
    }

    fn token_ranges_for_user(&self, user_id: &str, shard_size: usize) -> Result<Vec<u32>, String> {
        let r = self.watcher.partition_ring();
        let sr = r.shuffle_shard(user_id, shard_size).map_err(|e| e.to_string())?;
        match sr.get_token_ranges_for_partition(self.partition_id) {
            Ok(ranges) => Ok(ranges),
            Err(mimir_ring::partition_ring::PartitionRingError::PartitionDoesNotExist) => {
                Ok(Vec::new())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn owner_key_and_value(&self) -> (&'static str, String) {
        ("partition", self.partition_id.to_string())
    }
}

pub struct OwnedSeriesMetrics {
    pub check_duration: Histogram,
}

impl OwnedSeriesMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let check_duration = Histogram::with_opts(HistogramOpts::new(
            "cortex_ingester_owned_series_check_duration_seconds",
            "How long does it take to check for owned series for all users.",
        ))
        .expect("valid owned_series histogram");
        if let Some(r) = reg {
            let _ = r.register(Box::new(check_duration.clone()));
        }
        Self { check_duration }
    }
}

pub const DEFAULT_OWNED_SERIES_INITIAL_RETRY_INTERVAL: Duration = Duration::from_millis(100);

type GetUsersFn = Arc<dyn Fn() -> Vec<String> + Send + Sync>;
type GetTsdbFn = Arc<dyn Fn(&str) -> Option<Arc<UserTsdb>> + Send + Sync>;

/// Service periodically recomputing owned series per tenant (`pkg/ingester/owned_series.go:ownedSeriesService`).
pub struct OwnedSeriesService {
    ring_strategy: Arc<dyn OwnedSeriesRingStrategy>,
    get_tsdb_users: GetUsersFn,
    get_tsdb: GetTsdbFn,
    metrics: OwnedSeriesMetrics,
    interval: Duration,
    initial_ring_check_succeeded: AtomicBool,
    last_reasons: Mutex<Vec<String>>,
    service: Mutex<Option<Arc<ManagedBasicService>>>,
}

impl OwnedSeriesService {
    pub fn new(
        interval: Duration,
        ring_strategy: Arc<dyn OwnedSeriesRingStrategy>,
        reg: Option<&Registry>,
        get_tsdb_users: GetUsersFn,
        get_tsdb: GetTsdbFn,
    ) -> Arc<Self> {
        let this = Arc::new(Self {
            ring_strategy,
            get_tsdb_users,
            get_tsdb,
            metrics: OwnedSeriesMetrics::new(reg),
            interval,
            initial_ring_check_succeeded: AtomicBool::new(false),
            last_reasons: Mutex::new(Vec::new()),
            service: Mutex::new(None),
        });

        let s_start = Arc::clone(&this);
        let s_run = Arc::clone(&this);
        let basic = basic_service(
            Some(Box::new(move |_ctx: ServiceContext| {
                let s = Arc::clone(&s_start);
                Box::pin(async move {
                    match s.ring_strategy.check_ring_for_changes() {
                        Ok(_) => {
                            s.initial_ring_check_succeeded.store(true, Ordering::Release);
                            let _ = s.update_all_tenants(true);
                        }
                        Err(e) => {
                            if e.contains("empty") {
                                warn!("skipped initial owned series computation, ring is empty");
                                s.initial_ring_check_succeeded
                                    .store(false, Ordering::Release);
                            } else {
                                return Err(ServiceError::failure(format!("can't read ring: {e}")));
                            }
                        }
                    }
                    Ok(())
                })
            })),
            Some(Box::new(move |ctx: ServiceContext| {
                let s = Arc::clone(&s_run);
                Box::pin(async move {
                    let mut ticker_interval = if s.initial_ring_check_succeeded.load(Ordering::Acquire) {
                        s.interval
                    } else {
                        DEFAULT_OWNED_SERIES_INITIAL_RETRY_INTERVAL
                    };
                    loop {
                        tokio::select! {
                            _ = ctx.canceled() => return Ok(()),
                            _ = tokio::time::sleep(ticker_interval) => {
                                match s.ring_strategy.check_ring_for_changes() {
                                    Ok(ring_changed) => {
                                        if ticker_interval != s.interval {
                                            ticker_interval = s.interval;
                                        }
                                        s.update_all_tenants(ring_changed);
                                    }
                                    Err(e) => {
                                        debug!(err = %e, "can't check ring for updates");
                                    }
                                }
                            }
                        }
                    }
                })
            })),
            None,
        );
        *this.service.lock() = Some(basic);
        this
    }

    pub async fn start_and_await_running(&self) -> Result<(), String> {
        let svc = self.service.lock().clone().expect("service initialized");
        let ctx = ServiceContext::background();
        start_and_await_running(ctx, svc.as_ref())
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn stop_and_await_terminated(&self) -> Result<(), String> {
        let svc = self.service.lock().clone().expect("service initialized");
        let ctx = ServiceContext::background();
        stop_and_await_terminated(ctx, svc.as_ref())
            .await
            .map_err(|e| e.to_string())
    }

    pub fn state(&self) -> State {
        self.service
            .lock()
            .as_ref()
            .map_or(State::New, |s| s.state())
    }

    pub fn update_all_tenants(&self, ring_changed: bool) -> usize {
        let mut updated = 0usize;
        let mut reasons = Vec::new();
        let start = Instant::now();
        for user_id in (self.get_tsdb_users)() {
            let Some(db) = (self.get_tsdb)(&user_id) else {
                continue;
            };
            let shard_size = self.ring_strategy.shard_size_for_user(&user_id);
            let local_limit = db.limiter.max_series_per_user(&user_id, 0);
            let reason_preview = db.peek_reason_for_recompute_owned_series().or_else(|| {
                let os = db.owned_series_state();
                if shard_size != os.shard_size {
                    Some(RECOMPUTE_OWNED_SERIES_REASON_SHARD_SIZE_CHANGED.to_string())
                } else if local_limit != os.local_series_limit {
                    Some(RECOMPUTE_OWNED_SERIES_REASON_LOCAL_LIMIT_CHANGED.to_string())
                } else {
                    None
                }
            });
            if db.update_owned_series_if_needed(self.ring_strategy.as_ref(), ring_changed) {
                updated += 1;
                if let Some(r) = reason_preview {
                    reasons.push(r);
                } else {
                    reasons.push(RECOMPUTE_OWNED_SERIES_REASON_RING_CHANGED.to_string());
                }
            } else if let Some(r) = db.peek_reason_for_recompute_owned_series() {
                if r == RECOMPUTE_OWNED_SERIES_REASON_GET_TOKEN_RANGES_FAILED {
                    reasons.push(r);
                }
            }
        }
        *self.last_reasons.lock() = reasons;
        let elapsed = start.elapsed();
        self.metrics.check_duration.observe(elapsed.as_secs_f64());
        if updated > 0 {
            info!(
                updated_users = updated,
                ring_changed,
                duration_ms = elapsed.as_millis(),
                "updated owned series for users"
            );
        }
        updated
    }

    pub fn last_update_reasons(&self) -> Vec<String> {
        self.last_reasons.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_ranges_include_key_exact() {
        let ranges = vec![10, 20, 30, 40];
        assert!(!token_ranges_include_key(&ranges, 5));
        assert!(token_ranges_include_key(&ranges, 10));
        assert!(token_ranges_include_key(&ranges, 15));
        assert!(token_ranges_include_key(&ranges, 20));
        assert!(!token_ranges_include_key(&ranges, 25));
        assert!(token_ranges_include_key(&ranges, 30));
        assert!(token_ranges_include_key(&ranges, 40));
        assert!(!token_ranges_include_key(&ranges, 45));
        assert!(!token_ranges_include_key(&[], 10));
    }
}
