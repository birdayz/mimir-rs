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
use parking_lot::Mutex;

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

struct LruNode<V> {
    key: CacheKey,
    val: V,
    prev: Option<usize>,
    next: Option<usize>,
}

struct BoundedLru<V> {
    cap: usize,
    index: HashMap<CacheKey, usize>,
    nodes: Vec<Option<LruNode<V>>>,
    free: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
}

impl<V: Clone> BoundedLru<V> {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            index: HashMap::with_capacity(cap),
            nodes: Vec::with_capacity(cap),
            free: Vec::new(),
            head: None,
            tail: None,
        }
    }

    fn detach(&mut self, idx: usize) {
        let Some((prev, next)) = self.nodes.get(idx).and_then(|o| o.as_ref()).map(|n| (n.prev, n.next)) else {
            return;
        };
        if let Some(p) = prev {
            if let Some(pn) = self.nodes[p].as_mut() {
                pn.next = next;
            }
        } else {
            self.head = next;
        }
        if let Some(nx) = next {
            if let Some(nn) = self.nodes[nx].as_mut() {
                nn.prev = prev;
            }
        } else {
            self.tail = prev;
        }
        if let Some(n) = self.nodes[idx].as_mut() {
            n.prev = None;
            n.next = None;
        }
    }

    fn push_front(&mut self, idx: usize) {
        let old_head = self.head;
        if let Some(n) = self.nodes[idx].as_mut() {
            n.prev = None;
            n.next = old_head;
        }
        if let Some(h) = old_head {
            if let Some(hn) = self.nodes[h].as_mut() {
                hn.prev = Some(idx);
            }
        } else {
            self.tail = Some(idx);
        }
        self.head = Some(idx);
    }

    fn get(&mut self, key: &CacheKey) -> Option<V> {
        let idx = *self.index.get(key)?;
        if self.head != Some(idx) {
            self.detach(idx);
            self.push_front(idx);
        }
        self.nodes[idx].as_ref().map(|n| n.val.clone())
    }

    fn insert(&mut self, key: CacheKey, val: V) {
        if let Some(&idx) = self.index.get(&key) {
            if let Some(n) = self.nodes[idx].as_mut() {
                n.val = val;
            }
            if self.head != Some(idx) {
                self.detach(idx);
                self.push_front(idx);
            }
            return;
        }
        if self.index.len() >= self.cap {
            if let Some(lru_idx) = self.tail {
                self.detach(lru_idx);
                if let Some(old) = self.nodes[lru_idx].take() {
                    self.index.remove(&old.key);
                }
                self.free.push(lru_idx);
            }
        }
        let idx = if let Some(slot) = self.free.pop() {
            self.nodes[slot] = Some(LruNode {
                key: key.clone(),
                val,
                prev: None,
                next: None,
            });
            slot
        } else {
            let slot = self.nodes.len();
            self.nodes.push(Some(LruNode {
                key: key.clone(),
                val,
                prev: None,
                next: None,
            }));
            slot
        };
        self.index.insert(key, idx);
        self.push_front(idx);
    }
}

enum CacheStorage<V> {
    Unbounded(HashMap<CacheKey, V>),
    Lru(BoundedLru<V>),
}

impl<V: Clone> CacheStorage<V> {
    fn new(size: usize) -> Self {
        if size > 0 {
            Self::Lru(BoundedLru::new(size))
        } else {
            Self::Unbounded(HashMap::new())
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<V> {
        match self {
            Self::Unbounded(m) => m.get(key).cloned(),
            Self::Lru(lru) => lru.get(key),
        }
    }

    fn insert(&mut self, key: CacheKey, val: V) {
        match self {
            Self::Unbounded(m) => {
                m.insert(key, val);
            }
            Self::Lru(lru) => lru.insert(key, val),
        }
    }
}

struct ShuffleShardCache {
    without_lookback: Mutex<CacheStorage<Arc<PartitionRing>>>,
    with_lookback: Mutex<CacheStorage<CachedLookback>>,
}

impl ShuffleShardCache {
    fn new(size: usize) -> Arc<Self> {
        Arc::new(Self {
            without_lookback: Mutex::new(CacheStorage::new(size)),
            with_lookback: Mutex::new(CacheStorage::new(size)),
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

    match partition.try_token_scheme() {
        Ok(PartitionTokenScheme::PartitionTokensStored) => Ok(&partition.tokens),
        Ok(PartitionTokenScheme::PartitionTokensSmt512) => {
            let gen = opts.token_generator.as_ref().ok_or_else(|| {
                PartitionRingError::Other(format!(
                    "cannot resolve tokens for partition {id}: ring has no token generator"
                ))
            })?;
            gen.tokens_for(id).map_err(|e| {
                PartitionRingError::Other(format!("cannot resolve tokens for partition {id}: {e}"))
            })
        }
        Err(raw) => Err(PartitionRingError::Other(format!(
            "cannot resolve tokens for partition {id}: unknown token scheme {raw}"
        ))),
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
    all_active: Arc<OnceLock<Result<Arc<PartitionRing>, String>>>,
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

        let cache_size = opts.shuffle_shard_cache_size;
        Ok(Arc::new(Self {
            desc,
            ring_tokens,
            partition_by_token,
            ring_partition_ids,
            ring_partition_active,
            owners_by_partition,
            shuffle_shard_cache: ShuffleShardCache::new(cache_size),
            all_active: Arc::new(OnceLock::new()),
            active_partitions_count,
            non_pending_partitions_count,
            max_partition_id,
            opts,
        }))
    }

    pub fn desc(&self) -> &PartitionRingDesc {
        &self.desc
    }

    /// Reuses `prev`'s shuffle-shard cache and all-Active subring memo when the two rings
    /// differ only in `PENDING` partitions and their owners (Go `PartitionRing.inheritShards`).
    pub(crate) fn inherit_shards(&mut self, prev: &PartitionRing) {
        if !same_except_pending(&prev.desc, &self.desc) {
            return;
        }
        self.shuffle_shard_cache = Arc::clone(&prev.shuffle_shard_cache);
        self.all_active = Arc::clone(&prev.all_active);
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
        if let Some(cached) = self.shuffle_shard_cache.without_lookback.lock().get(&key) {
            return Ok(cached);
        }
        let sub = self.compute_shuffle_shard(identifier, size, Duration::ZERO, UNIX_EPOCH)?;
        self.shuffle_shard_cache
            .without_lookback
            .lock()
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
        if let Some(cached) = self.shuffle_shard_cache.with_lookback.lock().get(&key) {
            if lookback_start >= cached.valid_after && lookback_start <= cached.valid_before {
                return Ok(cached.subring);
            }
        }

        let sub = self.compute_shuffle_shard(identifier, size, lookback_period, now)?;

        let mut valid_before = i64::MAX;
        for part in sub.desc.partitions.values() {
            if part.state_timestamp >= lookback_start && part.state_timestamp < valid_before {
                valid_before = part.state_timestamp;
            }
        }
        let mut wl = self.shuffle_shard_cache.with_lookback.lock();
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
                shuffle_shard_cache: ShuffleShardCache::new(self.opts.shuffle_shard_cache_size),
                all_active: Arc::new(OnceLock::new()),
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
            shuffle_shard_cache: ShuffleShardCache::new(self.opts.shuffle_shard_cache_size),
            all_active: Arc::new(OnceLock::new()),
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

/// Reports whether `a` and `b` hold the same non-Pending partitions and the same owners of them
/// (Go `sameExceptPending`).
fn same_except_pending(a: &PartitionRingDesc, b: &PartitionRingDesc) -> bool {
    let mut count = 0isize;
    for (id, pa) in &a.partitions {
        if pa.is_pending() {
            continue;
        }
        count += 1;
        match b.partitions.get(id) {
            Some(pb) if pa == pb => {}
            _ => return false,
        }
    }
    for pb in b.partitions.values() {
        if !pb.is_pending() {
            count -= 1;
        }
    }
    if count != 0 {
        return false;
    }

    let owns_non_pending = |o: &mimir_proto::ringpb::OwnerDesc| -> bool {
        a.partitions
            .get(&o.owned_partition)
            .is_some_and(|p| !p.is_pending())
    };
    count = 0;
    for (id, oa) in &a.owners {
        if !owns_non_pending(oa) {
            continue;
        }
        count += 1;
        match b.owners.get(id) {
            Some(ob) if oa == ob => {}
            _ => return false,
        }
    }
    for ob in b.owners.values() {
        if owns_non_pending(ob) {
            count -= 1;
        }
    }
    count == 0
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

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_proto::ringpb::{PartitionDesc, PartitionState};

    #[test]
    fn test_shuffle_shard_cache_size_evicts_lru() {
        let mut desc = PartitionRingDesc::default();
        for pid in 0..4 {
            desc.partitions.insert(
                pid,
                PartitionDesc {
                    state: PartitionState::PartitionActive as i32,
                    tokens: vec![(pid as u32 + 1) * 1000],
                    ..Default::default()
                },
            );
        }
        let ring = PartitionRing::with_options(
            desc,
            PartitionRingOptions {
                token_generator: None,
                shuffle_shard_cache_size: 2,
            },
        )
        .unwrap();

        let _ = ring.shuffle_shard("t1", 1).unwrap();
        let _ = ring.shuffle_shard("t2", 1).unwrap();
        // Touch t1 so t2 becomes LRU.
        let _ = ring.shuffle_shard("t1", 1).unwrap();
        // Insert t3 -> must evict t2, keeping capacity at 2.
        let _ = ring.shuffle_shard("t3", 1).unwrap();

        let guard = ring.shuffle_shard_cache.without_lookback.lock();
        match &*guard {
            CacheStorage::Lru(lru) => {
                assert_eq!(lru.index.len(), 2);
                let keys: Vec<&str> = lru.index.keys().map(|k| k.identifier.as_str()).collect();
                assert!(keys.contains(&"t1"));
                assert!(keys.contains(&"t3"));
                assert!(!keys.contains(&"t2"));
            }
            CacheStorage::Unbounded(_) => panic!("expected bounded LRU"),
        }
    }
}
