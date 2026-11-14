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

struct StaticPartitionRingReader {
    ring: Arc<PartitionRing>,
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
                return Err(PartitionRingError::TooManyUnhealthyInstances);
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
