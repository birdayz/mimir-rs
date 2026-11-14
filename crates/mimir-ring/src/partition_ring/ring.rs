//! Partition ring lookup, shuffle sharding, and token ranges.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/partition_ring.go
//! - github.com/grafana/dskit/ring/partitions_ring_shuffle_shard_cache.go

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_proto::ringpb::{
    InstanceDesc, InstanceState, PartitionDesc, PartitionRingDesc, PartitionTokenScheme,
};
use parking_lot::RwLock;

use super::model::{PartitionDescExt, PartitionRingDescExt, PartitionRingError};
use crate::prng::{shuffle_shard_seed, GoRand};
use crate::ring::model::system_time_to_unix_secs;
use crate::ring::replication_strategy::ReplicationSet;
use crate::token_gen::{search_token, token_distance, PartitionTokenGenerator, OPTIMAL_TOKENS_PER_INSTANCE};

#[derive(Clone, Default)]
pub struct PartitionRingOptions {
    pub token_generator: Option<Arc<dyn PartitionTokenGenerator>>,
    pub shuffle_shard_cache_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CacheKey {
    identifier: String,
    shard_size: usize,
    lookback_period: Duration,
}

#[derive(Clone)]
struct CachedLookback {
    subring: Arc<PartitionRing>,
    valid_after: i64,
    valid_before: i64,
}

struct ShuffleShardCache {
    without_lookback: RwLock<HashMap<CacheKey, Arc<PartitionRing>>>,
    with_lookback: RwLock<HashMap<CacheKey, CachedLookback>>,
}

impl ShuffleShardCache {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            without_lookback: RwLock::new(HashMap::new()),
            with_lookback: RwLock::new(HashMap::new()),
        })
    }
}

pub fn resolve_partition_tokens<'a>(
    desc: &'a PartitionRingDesc,
    id: i32,
    opts: &'a PartitionRingOptions,
) -> Result<&'a [u32], PartitionRingError> {
    let partition = desc
        .partitions
        .get(&id)
        .ok_or(PartitionRingError::PartitionDoesNotExist)?;

    match partition.token_scheme() {
        PartitionTokenScheme::PartitionTokensStored => Ok(&partition.tokens),
        PartitionTokenScheme::PartitionTokensSmt512 => {
            let gen = opts.token_generator.as_ref().ok_or_else(|| {
                PartitionRingError::Other(format!(
                    "cannot resolve tokens for partition {id}: ring has no token generator"
                ))
            })?;
            gen.tokens_for(id).map_err(|e| {
                PartitionRingError::Other(format!("cannot resolve tokens for partition {id}: {e}"))
            })
        }
    }
}

fn resolve_ring_tokens(
    desc: &PartitionRingDesc,
    opts: &PartitionRingOptions,
) -> Result<(Vec<u32>, HashMap<u32, i32>), PartitionRingError> {
    let cap = desc.partitions.len() * OPTIMAL_TOKENS_PER_INSTANCE;
    let mut ring_tokens = Vec::with_capacity(cap);
    let mut partition_by_token = HashMap::with_capacity(cap);

    for &id in desc.partitions.keys() {
        let tokens = resolve_partition_tokens(desc, id, opts)?;
        ring_tokens.extend_from_slice(tokens);
        for &tok in tokens {
            partition_by_token.insert(tok, id);
        }
    }
    ring_tokens.sort_unstable();
    Ok((ring_tokens, partition_by_token))
}

/// Immutable snapshot of the partitions ring.
pub struct PartitionRing {
    pub(crate) desc: PartitionRingDesc,
    ring_tokens: Vec<u32>,
    partition_by_token: HashMap<u32, i32>,
    ring_partition_ids: Vec<i32>,
    ring_partition_active: Vec<bool>,
    owners_by_partition: HashMap<i32, Vec<String>>,
    shuffle_shard_cache: Arc<ShuffleShardCache>,
    all_active: OnceLock<Result<Arc<PartitionRing>, String>>,
    active_partitions_count: usize,
    non_pending_partitions_count: usize,
    max_partition_id: i32,
    opts: PartitionRingOptions,
}

impl PartitionRing {
    pub fn new(desc: PartitionRingDesc) -> Result<Arc<Self>, PartitionRingError> {
        Self::with_options(desc, PartitionRingOptions::default())
    }

    pub fn with_options(
        desc: PartitionRingDesc,
        opts: PartitionRingOptions,
    ) -> Result<Arc<Self>, PartitionRingError> {
        let (ring_tokens, partition_by_token) = resolve_ring_tokens(&desc, &opts)?;

        let mut ring_partition_ids = Vec::with_capacity(ring_tokens.len());
        let mut ring_partition_active = Vec::with_capacity(ring_tokens.len());
        for &tok in &ring_tokens {
            let pid = partition_by_token
                .get(&tok)
                .copied()
                .ok_or(PartitionRingError::InconsistentTokensInfo)?;
            let part = desc
                .partitions
                .get(&pid)
                .ok_or(PartitionRingError::InconsistentTokensInfo)?;
            ring_partition_ids.push(pid);
            ring_partition_active.push(part.is_active());
        }

        let owners_by_partition = desc.owners_by_partition();
        let active_partitions_count = desc.active_partitions_count();
        let non_pending_partitions_count = desc.non_pending_partitions_count();
        let max_partition_id = desc.max_partition_id();

        Ok(Arc::new(Self {
            desc,
            ring_tokens,
            partition_by_token,
            ring_partition_ids,
            ring_partition_active,
            owners_by_partition,
            shuffle_shard_cache: ShuffleShardCache::new(),
            all_active: OnceLock::new(),
            active_partitions_count,
            non_pending_partitions_count,
            max_partition_id,
            opts,
        }))
    }

    pub fn desc(&self) -> &PartitionRingDesc {
        &self.desc
    }

    pub fn partition_tokens(&self, id: i32) -> Result<&[u32], PartitionRingError> {
        resolve_partition_tokens(&self.desc, id, &self.opts)
    }

    pub fn count_tokens(&self) -> HashMap<i32, i64> {
        let mut owned: HashMap<i32, i64> = HashMap::with_capacity(self.desc.partitions.len());
        for (i, &token) in self.ring_tokens.iter().enumerate() {
            let pid = self.ring_partition_ids[i];
            let prev_token = if i == 0 {
                self.ring_tokens[self.ring_tokens.len() - 1]
            } else {
                self.ring_tokens[i - 1]
            };
            let diff = token_distance(prev_token, token);
            *owned.entry(pid).or_insert(0) += diff;
        }
        for &id in self.desc.partitions.keys() {
            owned.entry(id).or_insert(0);
        }
        owned
    }

    /// Returns the active partition ID responsible for `key`.
    pub fn active_partition_for_key(&self, key: u32) -> Result<i32, PartitionRingError> {
        let tokens_count = self.ring_tokens.len();
        if tokens_count == 0 {
            return Err(PartitionRingError::NoActivePartitionFound);
        }
        let start = search_token(&self.ring_tokens, key);
        for offset in 0..tokens_count {
            let idx = (start + offset) % tokens_count;
            if self.ring_partition_active[idx] {
                return Ok(self.ring_partition_ids[idx]);
            }
        }
        Err(PartitionRingError::NoActivePartitionFound)
    }

    pub fn shuffle_shard_size(&self, size: usize) -> usize {
        if size == 0 || size > self.active_partitions_count {
            self.active_partitions_count
        } else {
            size
        }
    }

    fn normalize_shuffle_shard_size(&self, size: usize) -> usize {
        if size == 0 || size >= self.non_pending_partitions_count {
            self.non_pending_partitions_count
        } else {
            size
        }
    }

    pub fn shuffle_shard(
        &self,
        identifier: &str,
        size: usize,
    ) -> Result<Arc<PartitionRing>, PartitionRingError> {
        let size = self.normalize_shuffle_shard_size(size);
        let key = CacheKey {
            identifier: identifier.to_string(),
            shard_size: size,
            lookback_period: Duration::ZERO,
        };
        if let Some(cached) = self.shuffle_shard_cache.without_lookback.read().get(&key) {
            return Ok(cached.clone());
        }
        let sub = self.compute_shuffle_shard(identifier, size, Duration::ZERO, UNIX_EPOCH)?;
        self.shuffle_shard_cache
            .without_lookback
            .write()
            .insert(key, sub.clone());
        Ok(sub)
    }

    pub fn shuffle_shard_with_lookback(
        &self,
        identifier: &str,
        size: usize,
        lookback_period: Duration,
        now: SystemTime,
    ) -> Result<Arc<PartitionRing>, PartitionRingError> {
        let size = self.normalize_shuffle_shard_size(size);
        let key = CacheKey {
            identifier: identifier.to_string(),
            shard_size: size,
            lookback_period,
        };
        let lookback_start =
            system_time_to_unix_secs(now.checked_sub(lookback_period).unwrap_or(UNIX_EPOCH));
        if let Some(cached) = self.shuffle_shard_cache.with_lookback.read().get(&key) {
            if lookback_start >= cached.valid_after && lookback_start <= cached.valid_before {
                return Ok(cached.subring.clone());
            }
        }

        let sub = self.compute_shuffle_shard(identifier, size, lookback_period, now)?;

        let mut valid_before = i64::MAX;
        for part in sub.desc.partitions.values() {
            if part.state_timestamp >= lookback_start && part.state_timestamp < valid_before {
                valid_before = part.state_timestamp;
            }
        }
        let mut wl = self.shuffle_shard_cache.with_lookback.write();
        let replace = wl
            .get(&key)
            .map(|e| e.valid_after < lookback_start)
            .unwrap_or(true);
        if replace {
            wl.insert(
                key,
                CachedLookback {
                    subring: sub.clone(),
                    valid_after: lookback_start,
                    valid_before,
                },
            );
        }
        Ok(sub)
    }

    fn compute_shuffle_shard(
        &self,
        identifier: &str,
        mut size: usize,
        lookback_period: Duration,
        now: SystemTime,
    ) -> Result<Arc<PartitionRing>, PartitionRingError> {
        size = self.normalize_shuffle_shard_size(size);

        if lookback_period.is_zero() && size >= self.active_partitions_count {
            let res = self.all_active.get_or_init(|| {
                self.all_active_partitions().map_err(|e| e.to_string())
            });
            return res
                .clone()
                .map_err(PartitionRingError::Other);
        }

        let lookback_until = if !lookback_period.is_zero() {
            system_time_to_unix_secs(now.checked_sub(lookback_period).unwrap_or(UNIX_EPOCH))
        } else {
            0
        };

        let mut rng = GoRand::new(shuffle_shard_seed(identifier, ""));
        let tokens_count = self.ring_tokens.len();
        let mut result: HashSet<i32> = HashSet::with_capacity(size);
        let mut exclude: HashSet<i32> = HashSet::new();

        while result.len() < size {
            let start = search_token(&self.ring_tokens, rng.uint32());
            let mut found = false;

            for offset in 0..tokens_count {
                let idx = (start + offset) % tokens_count;
                let pid = self.ring_partition_ids[idx];

                if result.contains(&pid) || exclude.contains(&pid) {
                    continue;
                }

                let p = self
                    .desc
                    .partitions
                    .get(&pid)
                    .ok_or(PartitionRingError::InconsistentTokensInfo)?;

                if p.is_pending() {
                    exclude.insert(pid);
                    continue;
                }

                let within_lookback =
                    !lookback_period.is_zero() && p.state_timestamp >= lookback_until;
                let should_extend = within_lookback;
                let should_include = p.is_active() || within_lookback;

                if should_include {
                    result.insert(pid);
                } else {
                    exclude.insert(pid);
                }
                if should_extend {
                    size += 1;
                }
                if should_include && !should_extend {
                    found = true;
                    break;
                }
            }

            if !found {
                break;
            }
        }

        self.with_partitions(&result)
    }

    fn all_active_partitions(&self) -> Result<Arc<PartitionRing>, PartitionRingError> {
        let mut partitions = HashSet::with_capacity(self.active_partitions_count);
        for (&id, part) in &self.desc.partitions {
            if !part.is_active() {
                continue;
            }
            let tokens = self.partition_tokens(id)?;
            if !tokens.is_empty() {
                partitions.insert(id);
            }
        }
        self.with_partitions(&partitions)
    }

    pub fn with_partitions(
        &self,
        partitions: &HashSet<i32>,
    ) -> Result<Arc<PartitionRing>, PartitionRingError> {
        if partitions.len() < self.desc.partitions.len() / 4 {
            return Self::with_options(self.desc.with_partitions(partitions), self.opts.clone());
        }

        let sub_desc = self.desc.with_partitions(partitions);
        let owners_by_partition = sub_desc.owners_by_partition();
        let active_partitions_count = sub_desc.active_partitions_count();
        let non_pending_partitions_count = sub_desc.non_pending_partitions_count();
        let max_partition_id = sub_desc.max_partition_id();

        if partitions.len() == self.desc.partitions.len() {
            return Ok(Arc::new(Self {
                desc: sub_desc,
                ring_tokens: self.ring_tokens.clone(),
                partition_by_token: self.partition_by_token.clone(),
                ring_partition_ids: self.ring_partition_ids.clone(),
                ring_partition_active: self.ring_partition_active.clone(),
                owners_by_partition,
                shuffle_shard_cache: ShuffleShardCache::new(),
                all_active: OnceLock::new(),
                active_partitions_count,
                non_pending_partitions_count,
                max_partition_id,
                opts: self.opts.clone(),
            }));
        }

        let mut token_count = 0usize;
        for &id in sub_desc.partitions.keys() {
            token_count += self.partition_tokens(id)?.len();
        }
        let mut ring_tokens = Vec::with_capacity(token_count);
        let mut ring_partition_ids = Vec::with_capacity(token_count);
        let mut ring_partition_active = Vec::with_capacity(token_count);
        let mut partition_by_token = HashMap::with_capacity(token_count);

        for (i, &tok) in self.ring_tokens.iter().enumerate() {
            let id = self.ring_partition_ids[i];
            if !partitions.contains(&id) {
                continue;
            }
            ring_tokens.push(tok);
            ring_partition_ids.push(id);
            ring_partition_active.push(self.ring_partition_active[i]);
            partition_by_token.insert(tok, id);
        }

        Ok(Arc::new(Self {
            desc: sub_desc,
            ring_tokens,
            partition_by_token,
            ring_partition_ids,
            ring_partition_active,
            owners_by_partition,
            shuffle_shard_cache: ShuffleShardCache::new(),
            all_active: OnceLock::new(),
            active_partitions_count,
            non_pending_partitions_count,
            max_partition_id,
            opts: self.opts.clone(),
        }))
    }

    pub fn partitions_count(&self) -> usize {
        self.desc.partitions.len()
    }

    pub fn max_partition_id(&self) -> i32 {
        self.max_partition_id
    }

    pub fn active_partitions_count(&self) -> usize {
        self.active_partitions_count
    }

    pub fn partitions(&self) -> Vec<PartitionDesc> {
        self.desc.partitions.values().cloned().collect()
    }

    pub fn partition_ids(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self.desc.partitions.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn pending_partition_ids(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self
            .desc
            .partitions
            .iter()
            .filter(|(_, p)| p.is_pending())
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
    }

    pub fn active_partition_ids(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self
            .desc
            .partitions
            .iter()
            .filter(|(_, p)| p.is_active())
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
    }

    pub fn inactive_partition_ids(&self) -> Vec<i32> {
        let mut ids: Vec<i32> = self
            .desc
            .partitions
            .iter()
            .filter(|(_, p)| p.is_inactive())
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        ids
    }

    pub fn partition_owner_ids(&self, partition_id: i32) -> &[String] {
        self.owners_by_partition
            .get(&partition_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn partition_owner_ids_copy(&self, partition_id: i32) -> Vec<String> {
        self.owners_by_partition
            .get(&partition_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn multi_partition_owner_ids(&self, partition_id: i32) -> Vec<String> {
        self.partition_owner_ids(partition_id)
            .iter()
            .map(|id| match id.rfind('/') {
                Some(idx) => id[..idx].to_string(),
                None => id.clone(),
            })
            .collect()
    }

    pub fn get_token_ranges_for_partition(
        &self,
        partition_id: i32,
    ) -> Result<Vec<u32>, PartitionRingError> {
        let tokens = self.partition_tokens(partition_id)?;
        let mut ranges = Vec::with_capacity(2 * (tokens.len() + 1));

        let add_range = |ranges: &mut Vec<u32>, start: u32, end: u32| {
            if !ranges.is_empty() && ranges[ranges.len() - 1] == start.wrapping_sub(1) && start > 0
            {
                let last = ranges.len() - 1;
                ranges[last] = end;
            } else {
                ranges.push(start);
                ranges.push(end);
            }
        };

        let mut owns_last_range = false;
        let mut start_of_last_range = 0u32;
        let mut ring_tokens: &[u32] = &self.ring_tokens;

        for (iter, &t) in tokens.iter().enumerate() {
            let last_owned_token = t.wrapping_sub(1);
            let ix = search_token(ring_tokens, last_owned_token);
            if ix == 0 {
                if iter > 0 {
                    return Err(PartitionRingError::InconsistentTokensInfo);
                }
                let prev_ix = ring_tokens.len() - 1;
                owns_last_range = true;
                start_of_last_range = ring_tokens[prev_ix];
                if t > 0 {
                    add_range(&mut ranges, 0, last_owned_token);
                }
            } else {
                let prev_ix = ix - 1;
                add_range(&mut ranges, ring_tokens[prev_ix], last_owned_token);
            }
            ring_tokens = &ring_tokens[ix..];
        }

        if owns_last_range {
            add_range(&mut ranges, start_of_last_range, u32::MAX);
        }

        Ok(ranges)
    }
}

pub struct ActivePartitionBatchRing {
    ring: Arc<PartitionRing>,
}

impl ActivePartitionBatchRing {
    pub fn new(ring: Arc<PartitionRing>) -> Self {
        Self { ring }
    }

    pub fn instances_count(&self) -> usize {
        self.ring.active_partitions_count()
    }

    pub fn replication_factor(&self) -> usize {
        1
    }

    pub fn get(&self, key: u32) -> Result<ReplicationSet, PartitionRingError> {
        let pid = self.ring.active_partition_for_key(key)?;
        let pid_str = pid.to_string();
        Ok(ReplicationSet {
            instances: vec![InstanceDesc {
                addr: pid_str.clone(),
                timestamp: 0,
                state: InstanceState::Active as i32,
                id: pid_str,
                ..Default::default()
            }],
            max_errors: 0,
            max_unavailable_zones: 0,
            zone_awareness_enabled: false,
            partition_id: pid,
        })
    }
}
