//! Partition instance ring (intersection of partition ring and instance ring).
//!
//! Ported from github.com/grafana/dskit/ring/partition_instance_ring.go

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mimir_proto::ringpb::InstanceDesc;

use super::model::PartitionRingError;
use super::ring::PartitionRing;
use super::watcher::PartitionRingWatcher;
use crate::ring::model::InstanceDescExt;
use crate::ring::replication_strategy::{Operation, ReplicationSet};
use crate::ring::{Ring, RingError};

pub trait PartitionRingReader: Send + Sync {
    fn partition_ring(&self) -> Arc<PartitionRing>;
}

pub trait InstanceRingReader: Send + Sync {
    fn get_instance(&self, id: &str) -> Result<InstanceDesc, RingError>;
    fn instances_count(&self) -> usize;
}

impl PartitionRingReader for PartitionRingWatcher {
    fn partition_ring(&self) -> Arc<PartitionRing> {
        self.partition_ring()
    }
}

impl InstanceRingReader for Ring {
    fn get_instance(&self, id: &str) -> Result<InstanceDesc, RingError> {
        self.get_instance(id)
    }

    fn instances_count(&self) -> usize {
        self.instances_count()
    }
}

pub struct StaticPartitionRingReader {
    ring: Arc<PartitionRing>,
}

impl StaticPartitionRingReader {
    pub fn new(ring: Arc<PartitionRing>) -> Self {
        Self { ring }
    }
}

impl PartitionRingReader for StaticPartitionRingReader {
    fn partition_ring(&self) -> Arc<PartitionRing> {
        self.ring.clone()
    }
}

pub struct PartitionInstanceRing {
    partitions_ring_reader: Arc<dyn PartitionRingReader>,
    instances_ring: Arc<dyn InstanceRingReader>,
    heartbeat_timeout: Duration,
}

impl PartitionInstanceRing {
    pub fn new(
        partitions_ring_reader: Arc<dyn PartitionRingReader>,
        instances_ring: Arc<dyn InstanceRingReader>,
        heartbeat_timeout: Duration,
    ) -> Self {
        Self {
            partitions_ring_reader,
            instances_ring,
            heartbeat_timeout,
        }
    }

    pub fn partition_ring(&self) -> Arc<PartitionRing> {
        self.partitions_ring_reader.partition_ring()
    }

    pub fn instance_ring(&self) -> &Arc<dyn InstanceRingReader> {
        &self.instances_ring
    }

    pub fn get_replication_sets_for_operation(
        &self,
        op: Operation,
    ) -> Result<Vec<ReplicationSet>, PartitionRingError> {
        self.get_replication_sets_for_operation_at(op, SystemTime::now())
    }

    pub fn get_replication_sets_for_operation_at(
        &self,
        op: Operation,
        now: SystemTime,
    ) -> Result<Vec<ReplicationSet>, PartitionRingError> {
        let partitions_ring = self.partition_ring();
        let desc = partitions_ring.desc();
        if desc.partitions.is_empty() {
            return Err(PartitionRingError::EmptyRing);
        }

        let mut result = Vec::with_capacity(desc.partitions.len());
        let mut zones_buf: Vec<String> = Vec::with_capacity(3);

        for &partition_id in desc.partitions.keys() {
            let owner_ids = partitions_ring.partition_owner_ids(partition_id);
            let mut instances = Vec::with_capacity(owner_ids.len());

            for instance_id in owner_ids {
                let Ok(instance) = self.instances_ring.get_instance(instance_id) else {
                    continue;
                };
                if !instance.is_healthy(op, self.heartbeat_timeout, now) {
                    continue;
                }
                instances.push(instance);
            }

            if instances.is_empty() {
                return Err(PartitionRingError::PartitionTooManyUnhealthyInstances(
                    partition_id,
                ));
            }

            zones_buf.clear();
            for inst in &instances {
                if !zones_buf.contains(&inst.zone) {
                    zones_buf.push(inst.zone.clone());
                }
            }
            let unique_zones = zones_buf.len();

            result.push(ReplicationSet {
                instances,
                max_errors: 0,
                max_unavailable_zones: unique_zones.saturating_sub(1),
                zone_awareness_enabled: true,
                partition_id,
            });
        }

        Ok(result)
    }

    pub fn shuffle_shard(
        &self,
        identifier: &str,
        size: usize,
    ) -> Result<Self, PartitionRingError> {
        let sub = self.partition_ring().shuffle_shard(identifier, size)?;
        Ok(Self::new(
            Arc::new(StaticPartitionRingReader { ring: sub }),
            self.instances_ring.clone(),
            self.heartbeat_timeout,
        ))
    }

    pub fn shuffle_shard_with_lookback(
        &self,
        identifier: &str,
        size: usize,
        lookback_period: Duration,
        now: SystemTime,
    ) -> Result<Self, PartitionRingError> {
        let sub = self
            .partition_ring()
            .shuffle_shard_with_lookback(identifier, size, lookback_period, now)?;
        Ok(Self::new(
            Arc::new(StaticPartitionRingReader { ring: sub }),
            self.instances_ring.clone(),
            self.heartbeat_timeout,
        ))
    }
}

/// Holds one PartitionInstanceRing per watcher in a PartitionRingWatchers group.
///
/// Ported from github.com/grafana/dskit/ring/partition_instance_rings.go.
pub struct PartitionInstanceRings {
    rings: Vec<PartitionInstanceRing>,
}

impl PartitionInstanceRings {
    pub fn new(
        watchers: &super::watchers::PartitionRingWatchers,
        instance_ring: Arc<dyn InstanceRingReader>,
        heartbeat_timeout: Duration,
    ) -> Self {
        let rings = watchers
            .all()
            .iter()
            .map(|w| {
                PartitionInstanceRing::new(
                    Arc::clone(w) as Arc<dyn PartitionRingReader>,
                    Arc::clone(&instance_ring),
                    heartbeat_timeout,
                )
            })
            .collect();
        Self { rings }
    }

    pub fn count(&self) -> usize {
        self.rings.len()
    }

    pub fn get(&self, idx: usize) -> &PartitionInstanceRing {
        &self.rings[idx]
    }
}

/// Partition instance ring supporting multi-partition ownership.
///
/// Ported from github.com/grafana/dskit/ring/multi_partition_instance_ring.go.
pub struct MultiPartitionInstanceRing {
    partitions_ring_reader: Arc<dyn PartitionRingReader>,
    instances_ring: Arc<dyn InstanceRingReader>,
    heartbeat_timeout: Duration,
}

impl MultiPartitionInstanceRing {
    pub fn new(
        partitions_ring_reader: Arc<dyn PartitionRingReader>,
        instances_ring: Arc<dyn InstanceRingReader>,
        heartbeat_timeout: Duration,
    ) -> Self {
        Self {
            partitions_ring_reader,
            instances_ring,
            heartbeat_timeout,
        }
    }

    pub fn partition_ring(&self) -> Arc<PartitionRing> {
        self.partitions_ring_reader.partition_ring()
    }

    pub fn get_replication_set_for_partition_and_operation(
        &self,
        partition_id: i32,
        op: Operation,
    ) -> Result<ReplicationSet, PartitionRingError> {
        self.get_replication_set_for_partition_and_operation_at(partition_id, op, SystemTime::now())
    }

    pub fn get_replication_set_for_partition_and_operation_at(
        &self,
        partition_id: i32,
        op: Operation,
        now: SystemTime,
    ) -> Result<ReplicationSet, PartitionRingError> {
        let owner_ids = self.partition_ring().multi_partition_owner_ids(partition_id);
        if owner_ids.is_empty() {
            return Err(PartitionRingError::EmptyRing);
        }

        let mut instances = Vec::with_capacity(owner_ids.len());
        let mut healthy_owner_ids = Vec::with_capacity(owner_ids.len());
        for instance_id in owner_ids {
            let Ok(instance) = self.instances_ring.get_instance(&instance_id) else {
                continue;
            };
            if !instance.is_healthy(op, self.heartbeat_timeout, now) {
                continue;
            }
            instances.push(instance);
            healthy_owner_ids.push(instance_id);
        }

        if instances.is_empty() {
            return Err(PartitionRingError::PartitionTooManyUnhealthyInstances(
                partition_id,
            ));
        }

        let mut zones_buf: Vec<String> = Vec::with_capacity(8);
        for inst in &instances {
            if !zones_buf.contains(&inst.zone) {
                zones_buf.push(inst.zone.clone());
            }
        }
        let unique_zones = zones_buf.len();
        let picked =
            highest_preferably_non_read_only_from_each_zone(&instances, &healthy_owner_ids, &zones_buf);

        Ok(ReplicationSet {
            instances: picked,
            max_errors: 0,
            max_unavailable_zones: unique_zones.saturating_sub(1),
            zone_awareness_enabled: true,
            partition_id,
        })
    }
}

fn highest_preferably_non_read_only_from_each_zone(
    instances: &[InstanceDesc],
    instance_ids: &[String],
    instance_zones: &[String],
) -> Vec<InstanceDesc> {
    let mut out = Vec::with_capacity(instance_zones.len());
    for zone in instance_zones {
        let mut highest: Option<usize> = None;
        for (i, instance) in instances.iter().enumerate() {
            if &instance.zone != zone {
                continue;
            }
            let Some(h) = highest else {
                highest = Some(i);
                continue;
            };
            if instances[h].read_only && !instance.read_only {
                highest = Some(i);
                continue;
            }
            if instance.read_only && !instances[h].read_only {
                continue;
            }
            if index_from_instance_suffix(&instance_ids[i])
                < index_from_instance_suffix(&instance_ids[h])
            {
                continue;
            }
            highest = Some(i);
        }
        if let Some(h) = highest {
            out.push(instances[h].clone());
        }
    }
    out
}

fn index_from_instance_suffix(instance: &str) -> usize {
    let bytes = instance.as_bytes();
    let mut digits_len = 0usize;
    while digits_len < bytes.len() {
        let b = bytes[bytes.len() - digits_len - 1];
        if b.is_ascii_digit() {
            digits_len += 1;
        } else {
            break;
        }
    }
    if digits_len == 0 {
        return usize::MAX;
    }
    instance[instance.len() - digits_len..]
        .parse::<usize>()
        .unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_ring::ring::PartitionRingOptions;
    use mimir_proto::ringpb::{OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc, PartitionState};

    #[test]
    fn unhealthy_error_includes_partition_id() {
        struct EmptyInstRing;
        impl InstanceRingReader for EmptyInstRing {
            fn get_instance(&self, _id: &str) -> Result<InstanceDesc, RingError> {
                Err(RingError::InstanceNotFound)
            }
            fn instances_count(&self) -> usize {
                0
            }
        }

        let mut desc = PartitionRingDesc::default();
        desc.partitions.insert(
            42,
            PartitionDesc {
                state: PartitionState::PartitionActive as i32,
                tokens: vec![100],
                ..Default::default()
            },
        );
        desc.owners.insert(
            "owner-42".to_string(),
            OwnerDesc {
                state: OwnerState::OwnerActive as i32,
                owned_partition: 42,
                ..Default::default()
            },
        );
        let ring = PartitionRing::with_options(desc, PartitionRingOptions::default()).unwrap();
        let pir = PartitionInstanceRing::new(
            Arc::new(StaticPartitionRingReader { ring }),
            Arc::new(EmptyInstRing),
            Duration::from_secs(60),
        );
        let err = pir
            .get_replication_sets_for_operation(crate::ring::replication_strategy::WRITE)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "partition 42: too many unhealthy instances in the ring"
        );
    }
}
