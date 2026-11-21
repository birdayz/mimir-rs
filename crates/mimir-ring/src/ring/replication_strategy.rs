//! Ring operations, replication set, and replication filtering strategies.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/ring.go (Operation)
//! - github.com/grafana/dskit/ring/replication_set.go
//! - github.com/grafana/dskit/ring/replication_strategy.go

use std::time::{Duration, SystemTime};

use mimir_proto::ringpb::{InstanceDesc, InstanceState};

use super::model::InstanceDescExt;
use super::RingError;

/// Operation describes which instance states are healthy and which extend the replica set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Operation(u32);

impl Operation {
    pub const fn new(healthy_states: &[InstanceState], extend_states: &[InstanceState]) -> Self {
        let mut op = 0u32;
        let mut i = 0;
        while i < healthy_states.len() {
            op |= 1u32 << (healthy_states[i] as u32);
            i += 1;
        }
        let mut j = 0;
        while j < extend_states.len() {
            op |= 0x10000u32 << (extend_states[j] as u32);
            j += 1;
        }
        Self(op)
    }

    #[inline]
    pub fn is_instance_in_state_healthy(self, s: InstanceState) -> bool {
        self.is_raw_state_healthy(s as i32)
    }

    #[inline]
    pub fn is_raw_state_healthy(self, s: i32) -> bool {
        if !(0..16).contains(&s) {
            return false;
        }
        (self.0 & (1u32 << (s as u32))) != 0
    }

    #[inline]
    pub fn should_extend_replica_set_on_state(self, s: InstanceState) -> bool {
        self.should_extend_raw_state(s as i32)
    }

    #[inline]
    pub fn should_extend_raw_state(self, s: i32) -> bool {
        if !(0..16).contains(&s) {
            return false;
        }
        (self.0 & (0x10000u32 << (s as u32))) != 0
    }
}

pub const WRITE: Operation = Operation::new(
    &[InstanceState::Active],
    &[
        InstanceState::Leaving,
        InstanceState::Pending,
        InstanceState::Joining,
        InstanceState::Left,
    ],
);

pub const WRITE_NO_EXTEND: Operation = Operation::new(&[InstanceState::Active], &[]);

pub const READ: Operation = Operation::new(
    &[
        InstanceState::Active,
        InstanceState::Pending,
        InstanceState::Leaving,
    ],
    &[
        InstanceState::Pending,
        InstanceState::Joining,
        InstanceState::Left,
    ],
);

pub const REPORTING: Operation = Operation(0x0000_ffff);

#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationSet {
    pub instances: Vec<InstanceDesc>,
    pub max_errors: usize,
    pub max_unavailable_zones: usize,
    pub zone_awareness_enabled: bool,
    pub partition_id: i32,
}

impl Default for ReplicationSet {
    fn default() -> Self {
        Self {
            instances: Vec::new(),
            max_errors: 0,
            max_unavailable_zones: 0,
            zone_awareness_enabled: false,
            partition_id: -1,
        }
    }
}

pub trait ReplicationStrategy: Send + Sync {
    fn filter(
        &self,
        instances: Vec<InstanceDesc>,
        op: Operation,
        replication_factor: usize,
        heartbeat_timeout: Duration,
        zone_awareness_enabled: bool,
        now: SystemTime,
    ) -> Result<(Vec<InstanceDesc>, usize), RingError>;

    fn supports_expanded_replication(&self) -> bool;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultReplicationStrategy;

impl ReplicationStrategy for DefaultReplicationStrategy {
    fn filter(
        &self,
        instances: Vec<InstanceDesc>,
        op: Operation,
        mut replication_factor: usize,
        heartbeat_timeout: Duration,
        zone_awareness_enabled: bool,
        now: SystemTime,
    ) -> Result<(Vec<InstanceDesc>, usize), RingError> {
        if instances.len() > replication_factor {
            replication_factor = instances.len();
        }
        let min_success = (replication_factor / 2) + 1;

        let mut healthy = Vec::with_capacity(instances.len());
        let mut unhealthy = Vec::new();
        for inst in instances {
            if inst.is_healthy(op, heartbeat_timeout, now) {
                healthy.push(inst);
            } else {
                unhealthy.push(inst.addr);
            }
        }

        if healthy.len() < min_success {
            let unhealthy_str = if unhealthy.is_empty() {
                String::new()
            } else {
                format!(" - unhealthy instances: {}", unhealthy.join(","))
            };
            let msg = if zone_awareness_enabled {
                format!(
                    "at least {} live replicas required across different availability zones, could only find {}{}",
                    min_success,
                    healthy.len(),
                    unhealthy_str
                )
            } else {
                format!(
                    "at least {} live replicas required, could only find {}{}",
                    min_success,
                    healthy.len(),
                    unhealthy_str
                )
            };
            return Err(RingError::TooFewReplicas(msg));
        }

        let max_failures = healthy.len() - min_success;
        Ok((healthy, max_failures))
    }

    fn supports_expanded_replication(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IgnoreUnhealthyInstancesReplicationStrategy;

impl ReplicationStrategy for IgnoreUnhealthyInstancesReplicationStrategy {
    fn filter(
        &self,
        instances: Vec<InstanceDesc>,
        op: Operation,
        _replication_factor: usize,
        heartbeat_timeout: Duration,
        _zone_awareness_enabled: bool,
        now: SystemTime,
    ) -> Result<(Vec<InstanceDesc>, usize), RingError> {
        let mut healthy = Vec::with_capacity(instances.len());
        let mut unhealthy = Vec::new();
        for inst in instances {
            if inst.is_healthy(op, heartbeat_timeout, now) {
                healthy.push(inst);
            } else {
                unhealthy.push(inst.addr);
            }
        }

        if healthy.is_empty() {
            let unhealthy_str = if unhealthy.is_empty() {
                String::new()
            } else {
                format!(" - unhealthy instances: {}", unhealthy.join(","))
            };
            return Err(RingError::TooFewReplicas(format!(
                "at least 1 healthy replica required, could only find 0{unhealthy_str}"
            )));
        }

        let max_failures = healthy.len() - 1;
        Ok((healthy, max_failures))
    }

    fn supports_expanded_replication(&self) -> bool {
        true
    }
}
