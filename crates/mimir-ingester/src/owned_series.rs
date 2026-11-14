//! Owned-series tracking & ring strategies matching `pkg/ingester/owned_series.go`.

use std::sync::Arc;
use std::time::Duration;

use mimir_config::Overrides;
use mimir_proto::ringpb::InstanceState;
use mimir_proto::sharding::shard_by_all_labels;
use mimir_ring::partition_ring::PartitionRingWatcher;
use mimir_ring::ring::replication_strategy::{Operation, ReplicationSet};
use mimir_ring::ring::{Ring, RingError};
use mimir_tsdb::labels::Labels;
use parking_lot::Mutex;
use prometheus::{Histogram, HistogramOpts, Registry};

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
        let write_ss = l.ingestion_partitions_tenant_write_shard_size;
        if write_ss > 0 {
            write_ss as usize
        } else {
            l.ingestion_partitions_tenant_shard_size.max(0) as usize
        }
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
