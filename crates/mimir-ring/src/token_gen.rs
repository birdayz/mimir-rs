//! Token generators and partition token table.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/token_generator.go
//! - github.com/grafana/dskit/ring/ownership_priority_queue.go
//! - github.com/grafana/dskit/ring/spread_minimizing_token_generator.go
//! - github.com/grafana/dskit/ring/partition_tokens.go
//! - github.com/grafana/dskit/ring/tokens.go

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::SystemTime;

use mimir_proto::ringpb::InstanceDesc;
use parking_lot::Mutex;
use crate::kv::{AsRegisterer, Registerer};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::prng::GoRand;

pub const TOTAL_TOKENS_COUNT: u64 = (u32::MAX as u64) + 1;
pub const OPTIMAL_TOKENS_PER_INSTANCE: usize = 512;
pub const MAX_ZONES_COUNT: usize = 8;

#[derive(Debug, Error)]
pub enum TokenGenError {
    #[error("unable to extract instance id from {0:?}")]
    BadInstanceIdFormat(String),
    #[error("the instance {0:?} has not been registered to the ring or has no tokens yet")]
    MissingPreviousInstance(String),
    #[error("number of zones {0} is not correct: it must be greater than 0 and less or equal than {MAX_ZONES_COUNT}")]
    ZoneCountOutOfBound(usize),
    #[error("zone {0} is not valid")]
    ZoneNotValid(String),
    #[error("derived token partition count must be non-negative, got {0}")]
    NegativePartitionCount(i32),
    #[error("partition ID must be non-negative, got {0}")]
    NegativePartitionId(i32),
    #[error("partition ID {id} must be between 0 and {len} (exclusive)")]
    PartitionIdOutOfRange { id: i32, len: usize },
    #[error("spread minimizing token calculation failed: {0}")]
    SpreadMinimizing(String),
    #[error("tokens file error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tokens json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Returns the distance between `from` and `to` on the 32-bit token ring.
/// The distance between a token and itself is the whole ring (`u32::MAX + 1`).
#[inline]
pub fn token_distance(from: u32, to: u32) -> i64 {
    if from < to {
        (to - from) as i64
    } else {
        (u32::MAX as i64) - (from as i64) + (to as i64) + 1
    }
}

/// Returns the index of the first token strictly greater than `key`, wrapping to `0`.
#[inline]
pub fn search_token(tokens: &[u32], key: u32) -> usize {
    if tokens.is_empty() {
        return 0;
    }
    let idx = match tokens.binary_search(&key) {
        Ok(i) => i + 1,
        Err(i) => i,
    };
    if idx >= tokens.len() {
        0
    } else {
        idx
    }
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Serialize, Deserialize)]
struct TokensJson {
    #[serde(default, alias = "Tokens", deserialize_with = "deserialize_null_default")]
    tokens: Vec<u32>,
}

/// Encodes tokens into JSON (`{"tokens":[...]}`), matching `dskit/ring.Tokens.Marshal`.
pub fn marshal_tokens(tokens: &[u32]) -> Result<Vec<u8>, TokenGenError> {
    Ok(serde_json::to_vec(&TokensJson {
        tokens: tokens.to_vec(),
    })?)
}

/// Decodes tokens from a JSON byte slice, matching `dskit/ring.Tokens.Unmarshal`.
pub fn unmarshal_tokens(bytes: &[u8]) -> Result<Vec<u32>, TokenGenError> {
    let parsed: TokensJson = serde_json::from_slice(bytes)?;
    Ok(parsed.tokens)
}

/// Returns `true` if both token lists contain the same multiset of tokens (order-independent),
/// matching `dskit/ring.Tokens.Equals`.
pub fn tokens_equals(a: &[u32], b: &[u32]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut sa = a.to_vec();
    let mut sb = b.to_vec();
    sa.sort_unstable();
    sb.sort_unstable();
    sa == sb
}

/// Stores sorted tokens to a JSON file atomically via `.tmp` + rename.
pub fn store_tokens_to_file(tokens: &[u32], path: &Path) -> Result<(), TokenGenError> {
    if path.as_os_str().is_empty() {
        return Err(TokenGenError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path is empty",
        )));
    }
    let tmp_path = {
        let mut os = path.as_os_str().to_os_string();
        os.push(".tmp");
        std::path::PathBuf::from(os)
    };
    let payload = serde_json::to_vec(&TokensJson {
        tokens: tokens.to_vec(),
    })?;
    std::fs::write(&tmp_path, payload)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Loads sorted tokens from a JSON file.
pub fn load_tokens_from_file(path: &Path) -> Result<Vec<u32>, TokenGenError> {
    let bytes = std::fs::read(path)?;
    let mut parsed: TokensJson = serde_json::from_slice(&bytes)?;
    if !parsed.tokens.windows(2).all(|w| w[0] <= w[1]) {
        parsed.tokens.sort_unstable();
    }
    Ok(parsed.tokens)
}

/// Trait matching `dskit/ring.TokenGenerator`.
pub trait TokenGenerator: Send + Sync {
    /// Generates at most `requested_tokens_count` unique sorted tokens that do not collide with `all_taken_tokens`.
    fn generate_tokens(&self, requested_tokens_count: usize, all_taken_tokens: &[u32]) -> Vec<u32>;

    /// Checks whether the instance owning this generator can join given current instances.
    fn can_join(&self, instances: &HashMap<String, InstanceDesc>) -> Result<(), TokenGenError>;

    /// Returns `true` if `can_join` check should be performed before joining.
    fn can_join_enabled(&self) -> bool;
}

/// Random token generator using Go's `math/rand` PRNG.
pub struct RandomTokenGenerator {
    rng: Mutex<GoRand>,
}

impl RandomTokenGenerator {
    pub fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(1);
        Self {
            rng: Mutex::new(GoRand::new(nanos)),
        }
    }

    pub fn with_seed(seed: i64) -> Self {
        Self {
            rng: Mutex::new(GoRand::new(seed)),
        }
    }
}

impl Default for RandomTokenGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TokenGenerator for RandomTokenGenerator {
    fn generate_tokens(&self, requested_tokens_count: usize, all_taken_tokens: &[u32]) -> Vec<u32> {
        if requested_tokens_count == 0 {
            return Vec::new();
        }
        let mut used: HashSet<u32> = HashSet::with_capacity(all_taken_tokens.len() + requested_tokens_count);
        for &t in all_taken_tokens {
            used.insert(t);
        }

        let mut tokens = Vec::with_capacity(requested_tokens_count);
        let mut rng = self.rng.lock();
        while tokens.len() < requested_tokens_count {
            let candidate = rng.uint32();
            if used.insert(candidate) {
                tokens.push(candidate);
            }
        }
        drop(rng);

        tokens.sort_unstable();
        tokens
    }

    fn can_join(&self, _instances: &HashMap<String, InstanceDesc>) -> Result<(), TokenGenError> {
        Ok(())
    }

    fn can_join_enabled(&self) -> bool {
        false
    }
}

// --- Ownership Priority Queue (exact Go container/heap port) ---

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingToken {
    pub token: u32,
    pub prev_token: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OwnershipInfo<T> {
    pub item: T,
    pub ownership: f64,
}

pub trait RingItem {
    fn key(&self) -> usize;
}

impl RingItem for usize {
    #[inline]
    fn key(&self) -> usize {
        *self
    }
}

impl RingItem for RingToken {
    #[inline]
    fn key(&self) -> usize {
        self.token as usize
    }
}

pub fn new_ring_token_ownership_info(token: u32, prev_token: u32) -> OwnershipInfo<RingToken> {
    OwnershipInfo {
        ownership: token_distance(prev_token, token) as f64,
        item: RingToken { token, prev_token },
    }
}

pub fn new_ring_instance_ownership_info(instance_id: usize, ownership: f64) -> OwnershipInfo<usize> {
    OwnershipInfo {
        ownership,
        item: instance_id,
    }
}

#[derive(Clone, Debug)]
pub struct OwnershipPriorityQueue<T> {
    items: Vec<OwnershipInfo<T>>,
}

impl<T: RingItem> OwnershipPriorityQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn less(&self, i: usize, j: usize) -> bool {
        let a = &self.items[i];
        let b = &self.items[j];
        if a.ownership == b.ownership {
            return a.item.key() > b.item.key();
        }
        a.ownership > b.ownership || (b.ownership.is_nan() && !a.ownership.is_nan())
    }

    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2;
            if i == j || !self.less(j, i) {
                break;
            }
            self.items.swap(i, j);
            j = i;
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2;
            }
            if !self.less(j, i) {
                break;
            }
            self.items.swap(i, j);
            i = j;
        }
        i > i0
    }

    pub fn push(&mut self, item: OwnershipInfo<T>) {
        self.items.push(item);
        let idx = self.items.len() - 1;
        self.up(idx);
    }

    pub fn pop(&mut self) -> Option<OwnershipInfo<T>> {
        if self.items.is_empty() {
            return None;
        }
        let n = self.items.len() - 1;
        self.items.swap(0, n);
        self.down(0, n);
        self.items.pop()
    }

    pub fn peek(&self) -> Option<&OwnershipInfo<T>> {
        self.items.first()
    }

    pub fn peek_mut(&mut self) -> Option<&mut OwnershipInfo<T>> {
        self.items.first_mut()
    }

    pub fn fix(&mut self, i: usize) {
        let n = self.items.len();
        if !self.down(i, n) {
            self.up(i);
        }
    }
}

/// Spread-minimizing deterministic token generator.
#[derive(Clone, Debug)]
pub struct SpreadMinimizingTokenGenerator {
    instance_id: usize,
    instance_prefix: String,
    zone_id: usize,
    pub can_join_enabled: bool,
}

pub fn find_zone_id(zone: &str, sorted_zones: &[String]) -> Result<usize, TokenGenError> {
    sorted_zones
        .iter()
        .position(|z| z == zone)
        .ok_or_else(|| TokenGenError::ZoneNotValid(zone.to_string()))
}

impl SpreadMinimizingTokenGenerator {
    pub fn new(
        instance: &str,
        zone: &str,
        spread_minimizing_zones: &[String],
        can_join_enabled: bool,
    ) -> Result<Self, TokenGenError> {
        if spread_minimizing_zones.is_empty() || spread_minimizing_zones.len() > MAX_ZONES_COUNT {
            return Err(TokenGenError::ZoneCountOutOfBound(
                spread_minimizing_zones.len(),
            ));
        }
        let mut sorted_zones = spread_minimizing_zones.to_vec();
        sorted_zones.sort();

        let zone_id = find_zone_id(zone, &sorted_zones)?;

        let (prefix, instance_id) = parse_instance_id(instance)?;
        Ok(Self::for_instance_and_zone_id(
            prefix,
            instance_id,
            zone_id,
            can_join_enabled,
        ))
    }

    pub fn for_instance_and_zone_id(
        instance_prefix: impl Into<String>,
        instance_id: usize,
        zone_id: usize,
        can_join_enabled: bool,
    ) -> Self {
        Self {
            instance_id,
            instance_prefix: instance_prefix.into(),
            zone_id,
            can_join_enabled,
        }
    }

    pub fn generate_first_instance_tokens(&self) -> Vec<u32> {
        let token_distance =
            ((TOTAL_TOKENS_COUNT / OPTIMAL_TOKENS_PER_INSTANCE as u64 / MAX_ZONES_COUNT as u64)
                * MAX_ZONES_COUNT as u64) as u32;
        let mut tokens = Vec::with_capacity(OPTIMAL_TOKENS_PER_INSTANCE);
        for i in 0..OPTIMAL_TOKENS_PER_INSTANCE {
            let token = (i as u32).wrapping_mul(token_distance).wrapping_add(self.zone_id as u32);
            tokens.push(token);
        }
        tokens
    }

    pub fn calculate_new_token(
        &self,
        token: RingToken,
        optimal_token_ownership: u32,
    ) -> Result<u32, TokenGenError> {
        let max_zones = MAX_ZONES_COUNT as u32;
        if optimal_token_ownership < max_zones || !optimal_token_ownership.is_multiple_of(max_zones) {
            return Err(TokenGenError::SpreadMinimizing(format!(
                "calculation of a new token between {} and {} with optimal token ownership {optimal_token_ownership} was impossible: optimal token ownership must be a positive multiple of maximal allowed number of zones {max_zones}",
                token.prev_token, token.token
            )));
        }
        if token.prev_token % max_zones != token.token % max_zones {
            return Err(TokenGenError::SpreadMinimizing(format!(
                "calculation of a new token between {} and {} with optimal token ownership {optimal_token_ownership} was impossible: lower and upper bounds must be congruent modulo maximal allowed number of zones {max_zones}",
                token.prev_token, token.token
            )));
        }
        let ownership = token_distance(token.prev_token, token.token);
        if ownership <= optimal_token_ownership as i64 {
            return Err(TokenGenError::SpreadMinimizing(format!(
                "calculation of a new token between {} and {} with optimal token ownership {optimal_token_ownership} was impossible: distance between lower and upper bound {ownership} is not big enough",
                token.prev_token, token.token
            )));
        }

        let max_token_value =
            (((TOTAL_TOKENS_COUNT / MAX_ZONES_COUNT as u64) - 1) * MAX_ZONES_COUNT as u64) as u32;
        let offset = max_token_value.wrapping_sub(token.prev_token);
        if offset < optimal_token_ownership {
            Ok(optimal_token_ownership.wrapping_sub(offset))
        } else {
            Ok(token.prev_token.wrapping_add(optimal_token_ownership))
        }
    }

    pub fn optimal_token_ownership(
        &self,
        optimal_instance_ownership: f64,
        curr_instance_ownership: f64,
        remaining_tokens_count: u32,
    ) -> u32 {
        let optimal_token_ownership =
            ((optimal_instance_ownership - curr_instance_ownership) as u32) / remaining_tokens_count;
        (optimal_token_ownership / MAX_ZONES_COUNT as u32) * MAX_ZONES_COUNT as u32
    }

    pub fn generate_all_tokens(&self) -> Result<Vec<u32>, TokenGenError> {
        let mut by_id = self.generate_tokens_by_instance_id()?;
        let mut all_tokens = by_id.remove(self.instance_id);
        all_tokens.sort_unstable();
        Ok(all_tokens)
    }

    pub fn generate_tokens_by_instance_id(&self) -> Result<Vec<Vec<u32>>, TokenGenError> {
        const MAX_SPREAD_MINIMIZING_INSTANCES: usize =
            (TOTAL_TOKENS_COUNT as usize) / OPTIMAL_TOKENS_PER_INSTANCE;
        let instance_count = self.instance_id.checked_add(1).ok_or_else(|| {
            TokenGenError::SpreadMinimizing(format!("instance_id {} overflow", self.instance_id))
        })?;
        if instance_count > MAX_SPREAD_MINIMIZING_INSTANCES {
            return Err(TokenGenError::SpreadMinimizing(format!(
                "instance_id {} exceeds max {}",
                self.instance_id,
                MAX_SPREAD_MINIMIZING_INSTANCES - 1
            )));
        }
        let first_instance_tokens = self.generate_first_instance_tokens();
        let mut tokens_by_instance_id: Vec<Vec<u32>> = vec![Vec::new(); instance_count];
        tokens_by_instance_id[0] = first_instance_tokens.clone();

        if self.instance_id == 0 {
            return Ok(tokens_by_instance_id);
        }

        let mut tokens_queues: Vec<OwnershipPriorityQueue<RingToken>> =
            Vec::with_capacity(self.instance_id);

        let mut first_queue = OwnershipPriorityQueue::with_capacity(OPTIMAL_TOKENS_PER_INSTANCE);
        let mut prev = first_instance_tokens.len() - 1;
        let mut first_instance_ownership = 0.0_f64;
        for (tk, &token) in first_instance_tokens.iter().enumerate() {
            let token_ownership = token_distance(first_instance_tokens[prev], token) as f64;
            first_instance_ownership += token_ownership;
            first_queue.push(new_ring_token_ownership_info(
                token,
                first_instance_tokens[prev],
            ));
            prev = tk;
        }
        tokens_queues.push(first_queue);

        let mut instance_queue = OwnershipPriorityQueue::with_capacity(self.instance_id);
        instance_queue.push(new_ring_instance_ownership_info(0, first_instance_ownership));

        let mut ignored_instances: Vec<OwnershipInfo<usize>> = Vec::with_capacity(self.instance_id);

        for i in 1..=self.instance_id {
            let optimal_instance_ownership = (TOTAL_TOKENS_COUNT as f64) / ((i + 1) as f64);
            let mut curr_instance_ownership = 0.0_f64;
            let mut added_tokens = 0_usize;
            ignored_instances.clear();
            let mut tokens = Vec::with_capacity(OPTIMAL_TOKENS_PER_INSTANCE);
            let mut curr_instance_token_queue =
                OwnershipPriorityQueue::with_capacity(OPTIMAL_TOKENS_PER_INSTANCE);

            while added_tokens < OPTIMAL_TOKENS_PER_INSTANCE {
                let optimal_token_ownership = self.optimal_token_ownership(
                    optimal_instance_ownership,
                    curr_instance_ownership,
                    (OPTIMAL_TOKENS_PER_INSTANCE - added_tokens) as u32,
                );
                let highest_instance_id = match instance_queue.peek() {
                    Some(inst) if inst.ownership > optimal_token_ownership as f64 => inst.item,
                    _ => {
                        return Err(TokenGenError::SpreadMinimizing(format!(
                            "impossible to add {}th token for instance {} in zone {} for ownership {optimal_token_ownership}",
                            added_tokens + 1,
                            i,
                            self.zone_id
                        )));
                    }
                };

                let highest_token_ownership = tokens_queues[highest_instance_id]
                    .peek()
                    .map(|t| t.ownership)
                    .unwrap_or(0.0);
                if highest_token_ownership <= optimal_token_ownership as f64 {
                    if let Some(popped) = instance_queue.pop() {
                        ignored_instances.push(popped);
                    }
                    continue;
                }

                let Some(token) = tokens_queues[highest_instance_id].peek().map(|t| t.item) else {
                    return Err(TokenGenError::SpreadMinimizing(format!(
                        "token queue for instance {highest_instance_id} unexpectedly empty"
                    )));
                };
                let new_token = self.calculate_new_token(token, optimal_token_ownership)?;
                tokens.push(new_token);
                curr_instance_token_queue
                    .push(new_ring_token_ownership_info(new_token, token.prev_token));

                let old_token_ownership = highest_token_ownership;
                let new_token_ownership = token_distance(new_token, token.token) as f64;
                curr_instance_ownership += old_token_ownership - new_token_ownership;

                if let Some(top_tok) = tokens_queues[highest_instance_id].peek_mut() {
                    top_tok.item.prev_token = new_token;
                    top_tok.ownership = new_token_ownership;
                }
                tokens_queues[highest_instance_id].fix(0);

                if let Some(top_inst) = instance_queue.peek_mut() {
                    top_inst.ownership =
                        top_inst.ownership - old_token_ownership + new_token_ownership;
                }
                instance_queue.fix(0);

                added_tokens += 1;
            }

            tokens_by_instance_id[i] = tokens;
            if i == self.instance_id {
                return Ok(tokens_by_instance_id);
            }

            for ignored in ignored_instances.drain(..) {
                instance_queue.push(ignored);
            }

            tokens_queues.push(curr_instance_token_queue);
            instance_queue.push(new_ring_instance_ownership_info(i, curr_instance_ownership));
        }

        Ok(tokens_by_instance_id)
    }
}

pub fn parse_instance_id(instance_id: &str) -> Result<(String, usize), TokenGenError> {
    let Some((prefix_no_dash, num_str)) = instance_id.rsplit_once('-') else {
        return Err(TokenGenError::BadInstanceIdFormat(instance_id.to_string()));
    };
    if num_str.is_empty() || !num_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(TokenGenError::BadInstanceIdFormat(instance_id.to_string()));
    }
    let val = num_str
        .parse::<usize>()
        .map_err(|_| TokenGenError::BadInstanceIdFormat(instance_id.to_string()))?;
    Ok((format!("{prefix_no_dash}-"), val))
}

impl TokenGenerator for SpreadMinimizingTokenGenerator {
    fn generate_tokens(&self, requested_tokens_count: usize, all_taken_tokens: &[u32]) -> Vec<u32> {
        let used: HashSet<u32> = all_taken_tokens.iter().copied().collect();
        let all_tokens = match self.generate_all_tokens() {
            Ok(tokens) => tokens,
            Err(err) => {
                tracing::error!(
                    %err,
                    instance_id = self.instance_id,
                    zone_id = self.zone_id,
                    "spread minimizing token generation failed"
                );
                return Vec::new();
            }
        };
        let mut unique_tokens = Vec::with_capacity(requested_tokens_count);
        for token in all_tokens {
            if unique_tokens.len() >= requested_tokens_count {
                break;
            }
            if !used.contains(&token) {
                unique_tokens.push(token);
            }
        }
        unique_tokens
    }

    fn can_join(&self, instances: &HashMap<String, InstanceDesc>) -> Result<(), TokenGenError> {
        if !self.can_join_enabled || self.instance_id == 0 {
            return Ok(());
        }
        let prev_instance = format!("{}{}", self.instance_prefix, self.instance_id - 1);
        match instances.get(&prev_instance) {
            Some(desc) if !desc.tokens.is_empty() => Ok(()),
            _ => Err(TokenGenError::MissingPreviousInstance(prev_instance)),
        }
    }

    fn can_join_enabled(&self) -> bool {
        self.can_join_enabled
    }
}

/// Partition token generator trait matching `dskit/ring.PartitionTokenGenerator`.
pub trait PartitionTokenGenerator: Send + Sync {
    fn tokens_for(&self, id: i32) -> Result<&[u32], TokenGenError>;
}

/// Pre-generated sorted tokens for partition IDs `0..partitions`.
#[derive(Debug, Clone)]
pub struct PartitionTokenTable {
    tokens: Vec<Vec<u32>>,
}

impl PartitionTokenTable {
    pub fn new(partitions: i32, reg: Option<&dyn AsRegisterer>) -> Result<Self, TokenGenError> {
        let reg = reg.map(|r| r.as_registerer());
        let reg = reg.as_ref();
        if partitions < 0 {
            return Err(TokenGenError::NegativePartitionCount(partitions));
        }
        if partitions == 0 {
            register_partition_token_table_metrics(partitions, reg);
            return Ok(Self { tokens: Vec::new() });
        }
        let max_id = partitions - 1;
        let generator = SpreadMinimizingTokenGenerator::for_instance_and_zone_id(
            "",
            max_id as usize,
            0,
            false,
        );
        let mut tokens = generator.generate_tokens_by_instance_id()?;
        for t in &mut tokens {
            t.sort_unstable();
        }
        register_partition_token_table_metrics(partitions, reg);
        Ok(Self { tokens })
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

impl PartitionTokenGenerator for PartitionTokenTable {
    fn tokens_for(&self, id: i32) -> Result<&[u32], TokenGenError> {
        if id < 0 || (id as usize) >= self.tokens.len() {
            return Err(TokenGenError::PartitionIdOutOfRange {
                id,
                len: self.tokens.len(),
            });
        }
        Ok(&self.tokens[id as usize])
    }
}

fn register_partition_token_table_metrics(partitions: i32, reg: Option<&Registerer>) {
    let Some(reg) = reg else {
        return;
    };
    if let Ok(g) = prometheus::Gauge::new(
        "partition_ring_max_derived_token_partitions",
        "Number of partition IDs, from 0, that can use derived tokens.",
    ) {
        g.set(partitions as f64);
        let _ = reg.register(Box::new(g));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_token_generator_unique_sorted() {
        let gen = RandomTokenGenerator::with_seed(123);
        let taken = vec![10, 20, 30];
        let toks = gen.generate_tokens(128, &taken);
        assert_eq!(toks.len(), 128);
        assert!(toks.windows(2).all(|w| w[0] < w[1]));
        for t in &taken {
            assert!(!toks.contains(t));
        }
    }

    #[test]
    fn test_spread_minimizing_basic() {
        let zones = vec!["zone-a".to_string(), "zone-b".to_string(), "zone-c".to_string()];
        let g0 = SpreadMinimizingTokenGenerator::new("ingester-0", "zone-a", &zones, true).unwrap();
        let g1 = SpreadMinimizingTokenGenerator::new("ingester-1", "zone-a", &zones, true).unwrap();
        let t0 = g0.generate_tokens(512, &[]);
        let t1 = g1.generate_tokens(512, &t0);
        assert_eq!(t0.len(), 512);
        assert_eq!(t1.len(), 512);
        assert!(t0.windows(2).all(|w| w[0] < w[1]));
        assert!(t1.windows(2).all(|w| w[0] < w[1]));
        let set0: HashSet<u32> = t0.iter().copied().collect();
        assert!(t1.iter().all(|t| !set0.contains(t)));
    }

    #[test]
    fn test_partition_token_table() {
        let table = PartitionTokenTable::new(4, None).unwrap();
        assert_eq!(table.len(), 4);
        for id in 0..4 {
            let toks = table.tokens_for(id).unwrap();
            assert_eq!(toks.len(), 512);
            assert!(toks.windows(2).all(|w| w[0] < w[1]));
        }
        assert!(table.tokens_for(4).is_err());
        assert!(table.tokens_for(-1).is_err());
    }
}
