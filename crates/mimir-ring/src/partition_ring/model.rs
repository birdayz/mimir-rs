//! Partition ring model and Mergeable CAS/gossip merge.
//!
//! Ported from github.com/grafana/dskit/ring/partition_ring_model.go

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_proto::ringpb::{
    OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc, PartitionState, PartitionTokenScheme,
};
use thiserror::Error;

use crate::kv::memberlist::Mergeable;
use crate::kv::{Codec, ProtoSnappyCodec};
use crate::ring::model::{future_timestamp_limit, local_merge_timestamp, system_time_to_unix_secs};
use crate::token_gen::{
    SpreadMinimizingTokenGenerator, TokenGenerator, OPTIMAL_TOKENS_PER_INSTANCE,
};

#[derive(Debug, Error)]
pub enum PartitionRingError {
    #[error("the partition does not exist")]
    PartitionDoesNotExist,
    #[error("the partition state does not match the expected one")]
    PartitionStateMismatch,
    #[error("partition state change not allowed")]
    PartitionStateChangeNotAllowed,
    #[error("partition state change is locked")]
    PartitionStateChangeLocked,
    #[error("no active partition found")]
    NoActivePartitionFound,
    #[error("inconsistent partition ring tokens information")]
    InconsistentTokensInfo,
    #[error("too many unhealthy instances in the ring")]
    TooManyUnhealthyInstances,
    #[error("empty ring")]
    EmptyRing,
    #[error("{0}")]
    Other(String),
}

pub fn partition_ring_desc_codec() -> Arc<dyn Codec> {
    Arc::new(
        ProtoSnappyCodec::<PartitionRingDesc>::new("partitionRingDesc").with_mergeable(
            |d| Box::new(d),
            |m| m.as_any().downcast_ref::<PartitionRingDesc>(),
        ),
    )
}

pub trait PartitionDescExt {
    fn partition_state(&self) -> PartitionState;
    fn token_scheme(&self) -> PartitionTokenScheme;
    fn is_pending(&self) -> bool;
    fn is_active(&self) -> bool;
    fn is_inactive(&self) -> bool;
    fn is_deleted(&self) -> bool;
    fn is_inactive_since(&self, since: SystemTime) -> bool;
    fn state_time(&self) -> SystemTime;
}

impl PartitionDescExt for PartitionDesc {
    fn partition_state(&self) -> PartitionState {
        PartitionState::try_from(self.state).unwrap_or(PartitionState::PartitionUnknown)
    }

    fn token_scheme(&self) -> PartitionTokenScheme {
        PartitionTokenScheme::try_from(self.token_scheme)
            .unwrap_or(PartitionTokenScheme::PartitionTokensStored)
    }

    fn is_pending(&self) -> bool {
        self.partition_state() == PartitionState::PartitionPending
    }

    fn is_active(&self) -> bool {
        self.partition_state() == PartitionState::PartitionActive
    }

    fn is_inactive(&self) -> bool {
        self.partition_state() == PartitionState::PartitionInactive
    }

    fn is_deleted(&self) -> bool {
        self.partition_state() == PartitionState::PartitionDeleted
    }

    fn is_inactive_since(&self, since: SystemTime) -> bool {
        self.is_inactive() && self.state_timestamp < system_time_to_unix_secs(since)
    }

    fn state_time(&self) -> SystemTime {
        if self.state_timestamp <= 0 {
            UNIX_EPOCH
        } else {
            UNIX_EPOCH + Duration::from_secs(self.state_timestamp as u64)
        }
    }
}

pub fn clean_partition_state_name(s: PartitionState) -> &'static str {
    match s {
        PartitionState::PartitionUnknown => "Unknown",
        PartitionState::PartitionPending => "Pending",
        PartitionState::PartitionActive => "Active",
        PartitionState::PartitionInactive => "Inactive",
        PartitionState::PartitionDeleted => "Deleted",
    }
}

pub fn clean_token_scheme_name(s: PartitionTokenScheme) -> &'static str {
    match s {
        PartitionTokenScheme::PartitionTokensStored => "Stored",
        PartitionTokenScheme::PartitionTokensSmt512 => "Smt512",
    }
}

pub trait PartitionRingDescExt {
    fn owners_by_partition(&self) -> HashMap<i32, Vec<String>>;
    fn count_partitions_by_state(&self) -> HashMap<PartitionState, usize>;
    fn active_partitions_count(&self) -> usize;
    fn non_pending_partitions_count(&self) -> usize;
    fn max_partition_id(&self) -> i32;
    fn with_partitions(&self, partitions: &HashSet<i32>) -> PartitionRingDesc;
    fn add_partition(&mut self, id: i32, state: PartitionState, now: SystemTime);
    fn add_partition_with_derived_tokens(&mut self, id: i32, state: PartitionState, now: SystemTime);
    fn update_partition_state(
        &mut self,
        id: i32,
        state: PartitionState,
        now: SystemTime,
    ) -> Result<bool, PartitionRingError>;
    fn update_partition_state_change_lock(&mut self, id: i32, locked: bool, now: SystemTime)
        -> bool;
    fn remove_partition(&mut self, id: i32);
    fn has_partition(&self, id: i32) -> bool;
    fn add_or_update_owner(
        &mut self,
        id: &str,
        state: OwnerState,
        owned_partition: i32,
        now: SystemTime,
    ) -> bool;
    fn remove_owner(&mut self, id: &str) -> bool;
    fn has_owner(&self, id: &str) -> bool;
    fn partition_owners_count(&self, partition_id: i32) -> usize;
    fn partition_owners_count_updated_before(&self, partition_id: i32, before: SystemTime) -> usize;
    fn merge_with_time(
        &mut self,
        other: &PartitionRingDesc,
        local_cas: bool,
        now: SystemTime,
    ) -> Option<PartitionRingDesc>;
}

impl PartitionRingDescExt for PartitionRingDesc {
    fn owners_by_partition(&self) -> HashMap<i32, Vec<String>> {
        let mut out: HashMap<i32, Vec<String>> = HashMap::with_capacity(self.partitions.len());
        for (id, o) in &self.owners {
            out.entry(o.owned_partition).or_default().push(id.clone());
        }
        for ids in out.values_mut() {
            ids.sort();
        }
        out
    }

    fn count_partitions_by_state(&self) -> HashMap<PartitionState, usize> {
        let mut out = HashMap::new();
        out.insert(PartitionState::PartitionPending, 0);
        out.insert(PartitionState::PartitionActive, 0);
        out.insert(PartitionState::PartitionInactive, 0);
        for p in self.partitions.values() {
            *out.entry(p.partition_state()).or_insert(0) += 1;
        }
        out
    }

    fn active_partitions_count(&self) -> usize {
        self.partitions.values().filter(|p| p.is_active()).count()
    }

    fn non_pending_partitions_count(&self) -> usize {
        self.partitions.values().filter(|p| !p.is_pending()).count()
    }

    fn max_partition_id(&self) -> i32 {
        self.partitions.keys().copied().max().unwrap_or(-1)
    }

    fn with_partitions(&self, partitions: &HashSet<i32>) -> PartitionRingDesc {
        let mut new_partitions = HashMap::with_capacity(partitions.len());
        let mut new_owners = HashMap::with_capacity(partitions.len() * 2);

        for (&pid, p) in &self.partitions {
            if partitions.contains(&pid) {
                new_partitions.insert(pid, p.clone());
            }
        }
        for (oid, o) in &self.owners {
            if partitions.contains(&o.owned_partition) {
                new_owners.insert(oid.clone(), *o);
            }
        }
        PartitionRingDesc {
            partitions: new_partitions,
            owners: new_owners,
        }
    }

    fn add_partition(&mut self, id: i32, state: PartitionState, now: SystemTime) {
        let smt = SpreadMinimizingTokenGenerator::for_instance_and_zone_id(
            "",
            id.max(0) as usize,
            0,
            false,
        );
        self.partitions.insert(
            id,
            PartitionDesc {
                id,
                token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
                tokens: smt.generate_tokens(OPTIMAL_TOKENS_PER_INSTANCE, &[]),
                state: state as i32,
                state_timestamp: system_time_to_unix_secs(now),
                state_change_locked: false,
                state_change_locked_timestamp: 0,
            },
        );
    }

    fn add_partition_with_derived_tokens(
        &mut self,
        id: i32,
        state: PartitionState,
        now: SystemTime,
    ) {
        self.partitions.insert(
            id,
            PartitionDesc {
                id,
                token_scheme: PartitionTokenScheme::PartitionTokensSmt512 as i32,
                tokens: Vec::new(),
                state: state as i32,
                state_timestamp: system_time_to_unix_secs(now),
                state_change_locked: false,
                state_change_locked_timestamp: 0,
            },
        );
    }

    fn update_partition_state(
        &mut self,
        id: i32,
        state: PartitionState,
        now: SystemTime,
    ) -> Result<bool, PartitionRingError> {
        let Some(d) = self.partitions.get_mut(&id) else {
            return Ok(false);
        };
        if d.partition_state() == state {
            return Ok(false);
        }
        if d.state_change_locked {
            return Err(PartitionRingError::PartitionStateChangeLocked);
        }
        d.state = state as i32;
        d.state_timestamp = system_time_to_unix_secs(now);
        Ok(true)
    }

    fn update_partition_state_change_lock(
        &mut self,
        id: i32,
        locked: bool,
        now: SystemTime,
    ) -> bool {
        let Some(d) = self.partitions.get_mut(&id) else {
            return false;
        };
        if d.state_change_locked == locked {
            return false;
        }
        d.state_change_locked = locked;
        d.state_change_locked_timestamp = system_time_to_unix_secs(now);
        true
    }

    fn remove_partition(&mut self, id: i32) {
        self.partitions.remove(&id);
    }

    fn has_partition(&self, id: i32) -> bool {
        self.partitions.contains_key(&id)
    }

    fn add_or_update_owner(
        &mut self,
        id: &str,
        state: OwnerState,
        owned_partition: i32,
        now: SystemTime,
    ) -> bool {
        let prev = self.owners.get(id).copied();
        let prev_ts = prev.map(|p| p.updated_timestamp).unwrap_or(0);
        let mut updated = OwnerDesc {
            state: state as i32,
            owned_partition,
            updated_timestamp: prev_ts,
        };
        if prev == Some(updated) {
            return false;
        }
        updated.updated_timestamp = system_time_to_unix_secs(now);
        self.owners.insert(id.to_string(), updated);
        true
    }

    fn remove_owner(&mut self, id: &str) -> bool {
        self.owners.remove(id).is_some()
    }

    fn has_owner(&self, id: &str) -> bool {
        self.owners.contains_key(id)
    }

    fn partition_owners_count(&self, partition_id: i32) -> usize {
        self.owners
            .values()
            .filter(|o| o.owned_partition == partition_id)
            .count()
    }

    fn partition_owners_count_updated_before(&self, partition_id: i32, before: SystemTime) -> usize {
        let before_secs = system_time_to_unix_secs(before);
        self.owners
            .values()
            .filter(|o| o.owned_partition == partition_id && o.updated_timestamp < before_secs)
            .count()
    }

    fn merge_with_time(
        &mut self,
        other: &PartitionRingDesc,
        local_cas: bool,
        now: SystemTime,
    ) -> Option<PartitionRingDesc> {
        let mut change = PartitionRingDesc::default();
        let timestamp_limit = future_timestamp_limit(now);
        let now_unix = system_time_to_unix_secs(now);

        for (&id, other_part) in &other.partitions {
            let mut changed = false;
            let this_part = match self.partitions.get(&id).cloned() {
                None => {
                    if other_part.state_timestamp > timestamp_limit {
                        continue;
                    }
                    changed = true;
                    other_part.clone()
                }
                Some(mut tp) => {
                    let this_state_ts = local_merge_timestamp(tp.state_timestamp, timestamp_limit);
                    let this_lock_ts = local_merge_timestamp(
                        tp.state_change_locked_timestamp,
                        timestamp_limit,
                    );

                    if other_part.state_timestamp <= timestamp_limit
                        && (other_part.state_timestamp > this_state_ts
                            || (other_part.state_timestamp == this_state_ts
                                && other_part.is_deleted()
                                && !tp.is_deleted()))
                    {
                        changed = true;
                        tp.state = other_part.state;
                        tp.state_timestamp = other_part.state_timestamp;
                    }

                    if other_part.state_change_locked_timestamp <= timestamp_limit
                        && other_part.state_change_locked_timestamp > this_lock_ts
                    {
                        changed = true;
                        tp.state_change_locked = other_part.state_change_locked;
                        tp.state_change_locked_timestamp = other_part.state_change_locked_timestamp;
                    }
                    tp
                }
            };

            if changed {
                self.partitions.insert(id, this_part.clone());
                change.partitions.insert(id, this_part);
            }
        }

        if local_cas {
            for (&pid, this_part) in &mut self.partitions {
                if !other.partitions.contains_key(&pid) && !this_part.is_deleted() {
                    this_part.state = PartitionState::PartitionDeleted as i32;
                    this_part.state_timestamp = now_unix;
                    change.partitions.insert(pid, this_part.clone());
                }
            }
        }

        for (id, other_owner) in &other.owners {
            if other_owner.updated_timestamp > timestamp_limit {
                continue;
            }
            let this_owner = self.owners.get(id).copied().unwrap_or_default();
            let this_ts = local_merge_timestamp(this_owner.updated_timestamp, timestamp_limit);
            let other_deleted = other_owner.state == OwnerState::OwnerDeleted as i32;
            let this_deleted = this_owner.state == OwnerState::OwnerDeleted as i32;

            if other_owner.updated_timestamp > this_ts
                || (other_owner.updated_timestamp == this_ts && other_deleted && !this_deleted)
            {
                self.owners.insert(id.clone(), *other_owner);
                change.owners.insert(id.clone(), *other_owner);
            }
        }

        if local_cas {
            for (id, this_owner) in &mut self.owners {
                if !other.owners.contains_key(id)
                    && this_owner.state != OwnerState::OwnerDeleted as i32
                {
                    this_owner.state = OwnerState::OwnerDeleted as i32;
                    this_owner.updated_timestamp = now_unix;
                    change.owners.insert(id.clone(), *this_owner);
                }
            }
        }

        if change.partitions.is_empty() && change.owners.is_empty() {
            None
        } else {
            Some(change)
        }
    }
}

impl Mergeable for PartitionRingDesc {
    fn merge(
        &mut self,
        other: &mut dyn Mergeable,
        local_cas: bool,
    ) -> Result<Option<Box<dyn Mergeable>>, String> {
        let other_desc = other
            .as_any()
            .downcast_ref::<PartitionRingDesc>()
            .ok_or_else(|| "expected PartitionRingDesc".to_string())?;
        Ok(self
            .merge_with_time(other_desc, local_cas, SystemTime::now())
            .map(|c| Box::new(c) as Box<dyn Mergeable>))
    }

    fn merge_content(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(self.partitions.len() + self.owners.len());
        for pid in self.partitions.keys() {
            out.push(pid.to_string());
        }
        for id in self.owners.keys() {
            out.push(id.clone());
        }
        out
    }

    fn remove_tombstones(&mut self, limit: Option<SystemTime>) -> (usize, usize) {
        let mut total = 0usize;
        let mut removed = 0usize;
        let limit_secs = limit.map(system_time_to_unix_secs);

        self.partitions.retain(|_, part| {
            if part.is_deleted() {
                let expired = match limit_secs {
                    None => true,
                    Some(lim) => part.state_timestamp < lim,
                };
                if expired {
                    removed += 1;
                    return false;
                } else {
                    total += 1;
                }
            }
            true
        });

        self.owners.retain(|_, owner| {
            if owner.state == OwnerState::OwnerDeleted as i32 {
                let expired = match limit_secs {
                    None => true,
                    Some(lim) => owner.updated_timestamp < lim,
                };
                if expired {
                    removed += 1;
                    return false;
                } else {
                    total += 1;
                }
            }
            true
        });

        (total, removed)
    }

    fn clone_mergeable(&self) -> Box<dyn Mergeable> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }

    fn as_any_mut(&mut self) -> &mut (dyn Any + Send + Sync) {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}
