//! Per-tenant and ring-scaled limiters matching `pkg/ingester/limiter.go`.

use std::sync::Arc;

use mimir_config::Overrides;
use mimir_ring::partition_ring::{PartitionRing, PartitionRingWatcher};
use mimir_ring::prng::shuffle_shard_expected_instances_per_zone;
use mimir_ring::ring::Ring;

pub trait LimiterRingStrategy: Send + Sync {
    fn convert_global_to_local_limit(&self, user_id: &str, global_limit: usize) -> usize;
    fn get_shard_size(&self, user_id: &str) -> usize;
}

/// Interface exposed by a ring implementation allowing instance/zone counting (`ingesterRingLimiterRingCount`).
pub trait IngesterRingLimiterRingCount: Send + Sync {
    fn writable_instances_with_tokens_count(&self) -> usize;
    fn writable_instances_with_tokens_in_zone_count(&self, zone: &str) -> usize;
    fn zones_count(&self) -> usize;
}

impl IngesterRingLimiterRingCount for Ring {
    fn writable_instances_with_tokens_count(&self) -> usize {
        Ring::writable_instances_with_tokens_count(self)
    }

    fn writable_instances_with_tokens_in_zone_count(&self, zone: &str) -> usize {
        Ring::writable_instances_with_tokens_in_zone_count(self, zone)
    }

    fn zones_count(&self) -> usize {
        Ring::zones_count(self)
    }
}

/// Interface for accessing a `PartitionRing` (`partitionRingWatcher` in `limiter.go`).
pub trait PartitionRingProvider: Send + Sync {
    fn partition_ring(&self) -> Arc<PartitionRing>;
}

impl PartitionRingProvider for PartitionRingWatcher {
    fn partition_ring(&self) -> Arc<PartitionRing> {
        PartitionRingWatcher::partition_ring(self)
    }
}

/// Classic ingester ring strategy scaling global limits across active instances/zones.
pub struct IngesterRingLimiterStrategy {
    ring: Option<Arc<dyn IngesterRingLimiterRingCount>>,
    replication_factor: usize,
    zone_awareness_enabled: bool,
    ingester_zone: String,
    overrides: Overrides,
}

impl IngesterRingLimiterStrategy {
    pub fn new<R: IngesterRingLimiterRingCount + 'static>(
        ring: Arc<R>,
        replication_factor: usize,
        zone_awareness_enabled: bool,
        ingester_zone: impl Into<String>,
        overrides: Overrides,
    ) -> Self {
        Self::new_opt(
            Some(ring),
            replication_factor,
            zone_awareness_enabled,
            ingester_zone,
            overrides,
        )
    }

    pub fn new_opt(
        ring: Option<Arc<dyn IngesterRingLimiterRingCount>>,
        replication_factor: usize,
        zone_awareness_enabled: bool,
        ingester_zone: impl Into<String>,
        overrides: Overrides,
    ) -> Self {
        Self {
            ring,
            replication_factor,
            zone_awareness_enabled,
            ingester_zone: ingester_zone.into(),
            overrides,
        }
    }

    fn zones_count(&self) -> usize {
        if self.zone_awareness_enabled {
            self.ring.as_ref().map_or(1, |r| r.zones_count().max(1))
        } else {
            1
        }
    }
}

impl LimiterRingStrategy for IngesterRingLimiterStrategy {
    fn convert_global_to_local_limit(&self, user_id: &str, global_limit: usize) -> usize {
        if global_limit == 0 {
            return 0;
        }
        let Some(ref ring) = self.ring else {
            return 0;
        };
        let zones_count = self.zones_count();
        let user_shard_size = self.get_shard_size(user_id);

        let mut ingesters_in_zone_count = if zones_count > 1 {
            ring.writable_instances_with_tokens_in_zone_count(&self.ingester_zone)
        } else {
            ring.writable_instances_with_tokens_count()
        };

        if user_shard_size > 0 {
            let expected = shuffle_shard_expected_instances_per_zone(user_shard_size, zones_count);
            ingesters_in_zone_count = ingesters_in_zone_count.min(expected);
        }

        if ingesters_in_zone_count == 0 {
            return 0;
        }

        (((global_limit * self.replication_factor) as f64) / (zones_count as f64)
            / (ingesters_in_zone_count as f64)) as usize
    }

    fn get_shard_size(&self, user_id: &str) -> usize {
        let l = self.overrides.for_tenant(user_id);
        l.ingestion_tenant_shard_size.max(0) as usize
    }
}

/// Partition ring strategy scaling global limits across active partitions.
pub struct PartitionRingLimiterStrategy {
    watcher: Arc<dyn PartitionRingProvider>,
    overrides: Overrides,
}

impl PartitionRingLimiterStrategy {
    pub fn new<P: PartitionRingProvider + 'static>(
        watcher: Arc<P>,
        overrides: Overrides,
    ) -> Self {
        Self { watcher, overrides }
    }
}

impl LimiterRingStrategy for PartitionRingLimiterStrategy {
    fn convert_global_to_local_limit(&self, user_id: &str, global_limit: usize) -> usize {
        if global_limit == 0 {
            return 0;
        }
        let user_shard_size = self.get_shard_size(user_id);
        let pr = self.watcher.partition_ring();
        let active_partitions_count = pr.shuffle_shard_size(user_shard_size);
        if active_partitions_count == 0 {
            return 0;
        }
        ((global_limit as f64) / (active_partitions_count as f64)) as usize
    }

    fn get_shard_size(&self, user_id: &str) -> usize {
        let l = self.overrides.for_tenant(user_id);
        l.effective_ingestion_partitions_tenant_write_shard_size().max(0) as usize
    }
}

/// No-op single-node ring strategy (useful in standalone unit tests without a ring).
pub struct NoopRingLimiterStrategy;

impl LimiterRingStrategy for NoopRingLimiterStrategy {
    fn convert_global_to_local_limit(&self, _user_id: &str, global_limit: usize) -> usize {
        global_limit
    }

    fn get_shard_size(&self, _user_id: &str) -> usize {
        0
    }
}

/// Per-tenant series/metadata/exemplar limiter (`pkg/ingester/limiter.go`).
pub struct Limiter {
    overrides: Overrides,
    ring_strategy: Arc<dyn LimiterRingStrategy>,
}

impl Limiter {
    pub fn new(overrides: Overrides, ring_strategy: Arc<dyn LimiterRingStrategy>) -> Self {
        Self {
            overrides,
            ring_strategy,
        }
    }

    pub fn overrides(&self) -> &Overrides {
        &self.overrides
    }

    pub fn ring_strategy(&self) -> &Arc<dyn LimiterRingStrategy> {
        &self.ring_strategy
    }

    pub fn is_within_max_series_per_metric(&self, user_id: &str, series: usize) -> bool {
        series < self.max_series_per_metric(user_id)
    }

    pub fn is_within_max_metadata_per_metric(&self, user_id: &str, metadata: usize) -> bool {
        metadata < self.max_metadata_per_metric(user_id)
    }

    pub fn is_within_max_series_per_user(
        &self,
        user_id: &str,
        series: usize,
        min_local_limit: usize,
    ) -> bool {
        series < self.max_series_per_user(user_id, min_local_limit)
    }

    pub fn is_within_max_metrics_with_metadata_per_user(
        &self,
        user_id: &str,
        metrics: usize,
    ) -> bool {
        metrics < self.max_metadata_per_user(user_id)
    }

    pub fn is_over_label_value_bytes_metric_threshold(&self, user_id: &str, bytes: u64) -> bool {
        bytes >= self.label_value_bytes_metric_threshold(user_id) as u64
    }

    pub fn label_value_bytes_metric_threshold(&self, user_id: &str) -> usize {
        let global = self
            .overrides
            .for_tenant(user_id)
            .label_value_bytes_metric_threshold
            .max(0) as usize;
        self.convert_global_to_local_or_unlimited(user_id, global, 0)
    }

    pub fn max_series_per_metric(&self, user_id: &str) -> usize {
        let global = self
            .overrides
            .for_tenant(user_id)
            .max_global_series_per_metric
            .max(0) as usize;
        self.convert_global_to_local_or_unlimited(user_id, global, 0)
    }

    pub fn max_metadata_per_metric(&self, user_id: &str) -> usize {
        let global = self
            .overrides
            .for_tenant(user_id)
            .max_global_metadata_per_metric
            .max(0) as usize;
        self.convert_global_to_local_or_unlimited(user_id, global, 0)
    }

    pub fn max_series_per_user(&self, user_id: &str, min_local_limit: usize) -> usize {
        let global = self
            .overrides
            .for_tenant(user_id)
            .max_global_series_per_user
            .max(0) as usize;
        self.convert_global_to_local_or_unlimited(user_id, global, min_local_limit)
    }

    pub fn max_metadata_per_user(&self, user_id: &str) -> usize {
        let global = self
            .overrides
            .for_tenant(user_id)
            .max_global_metadata_per_user
            .max(0) as usize;
        self.convert_global_to_local_or_unlimited(user_id, global, 0)
    }

    pub fn max_global_metadata_per_metric(&self, user_id: &str) -> i64 {
        self.overrides
            .for_tenant(user_id)
            .max_global_metadata_per_metric
    }

    pub fn max_global_metrics_with_metadata_per_user(&self, user_id: &str) -> i64 {
        self.overrides
            .for_tenant(user_id)
            .max_global_metadata_per_user
    }

    pub fn max_exemplars_per_user(&self, user_id: &str) -> usize {
        let global = self
            .overrides
            .for_tenant(user_id)
            .max_global_exemplars_per_user
            .max(0) as usize;
        let local = self
            .ring_strategy
            .convert_global_to_local_limit(user_id, global);
        if local > 0 {
            local
        } else {
            global
        }
    }

    pub fn convert_global_to_local_limit(&self, user_id: &str, global_limit: usize) -> usize {
        self.ring_strategy
            .convert_global_to_local_limit(user_id, global_limit)
    }

    fn convert_global_to_local_or_unlimited(
        &self,
        user_id: &str,
        global_limit: usize,
        min_local_limit: usize,
    ) -> usize {
        let mut local = self
            .ring_strategy
            .convert_global_to_local_limit(user_id, global_limit);
        if local == 0 {
            local = i32::MAX as usize;
        }
        local.max(min_local_limit)
    }
}
