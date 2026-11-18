//! Sharded per-metric series counter matching `pkg/ingester/metric_counter.go`.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::limiter::Limiter;

const NUM_METRIC_COUNTER_SHARDS: usize = 128;

#[derive(Clone, Copy, Default)]
struct FastU64Hasher(u64);

impl std::hash::Hasher for FastU64Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self
                .0
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(b as u64);
        }
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        let mut x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 32;
        self.0 = x;
    }
}

#[derive(Clone, Copy, Default)]
struct FastU64BuildHasher;

impl std::hash::BuildHasher for FastU64BuildHasher {
    type Hasher = FastU64Hasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        FastU64Hasher(0)
    }
}

type MetricCountBucket = smallvec::SmallVec<[(Box<str>, usize); 1]>;

#[derive(Default)]
struct MetricCounterShard {
    counts: hashbrown::HashMap<u64, MetricCountBucket, FastU64BuildHasher>,
}

/// Thread-safe 128-shard counter enforcing `max_series_per_metric`.
pub struct MetricCounter {
    limiter: Arc<Limiter>,
    shards: Vec<Mutex<MetricCounterShard>>,
    ignored_metrics: HashSet<String>,
}

impl MetricCounter {
    pub fn new(limiter: Arc<Limiter>, ignored_metrics: HashSet<String>) -> Self {
        let mut shards = Vec::with_capacity(NUM_METRIC_COUNTER_SHARDS);
        for _ in 0..NUM_METRIC_COUNTER_SHARDS {
            shards.push(Mutex::new(MetricCounterShard {
                counts: hashbrown::HashMap::with_hasher(FastU64BuildHasher),
            }));
        }
        Self {
            limiter,
            shards,
            ignored_metrics,
        }
    }

    #[inline]
    fn shard_and_fp(&self, metric_name: &str) -> (&Mutex<MetricCounterShard>, u64) {
        let fp = hash_metric_name(metric_name);
        let h = hash_fp(fp) as usize;
        (&self.shards[h % NUM_METRIC_COUNTER_SHARDS], fp)
    }

    pub fn can_add_series_for(&self, user_id: &str, metric: &str) -> bool {
        if self.ignored_metrics.contains(metric) {
            return true;
        }
        let (shard, fp) = self.shard_and_fp(metric);
        let guard = shard.lock();
        let current = guard
            .counts
            .get(&fp)
            .and_then(|bucket| bucket.iter().find(|(k, _)| k.as_ref() == metric).map(|(_, v)| *v))
            .unwrap_or(0);
        self.limiter.is_within_max_series_per_metric(user_id, current)
    }

    pub fn increase_series_for_metric(&self, metric: &str) {
        let (shard, fp) = self.shard_and_fp(metric);
        let mut guard = shard.lock();
        let bucket = guard.counts.entry(fp).or_default();
        if let Some((_, c)) = bucket.iter_mut().find(|(k, _)| k.as_ref() == metric) {
            *c += 1;
        } else {
            bucket.push((metric.into(), 1));
        }
    }

    pub fn decrease_series_for_metric(&self, metric: &str) {
        let (shard, fp) = self.shard_and_fp(metric);
        let mut guard = shard.lock();
        let remove_bucket = if let Some(bucket) = guard.counts.get_mut(&fp) {
            if let Some(pos) = bucket.iter().position(|(k, _)| k.as_ref() == metric) {
                if bucket[pos].1 <= 1 {
                    bucket.swap_remove(pos);
                } else {
                    bucket[pos].1 -= 1;
                }
            }
            bucket.is_empty()
        } else {
            false
        };
        if remove_bucket {
            guard.counts.remove(&fp);
        }
    }
}

/// FNV-1a 64-bit hash matching `pkg/ingester/metric_counter.go:hashMetricName`.
#[inline]
pub fn hash_metric_name(metric: &str) -> u64 {
    fnv64a(metric.as_bytes())
}

#[inline]
fn fnv64a(data: &[u8]) -> u64 {
    let mut h: u64 = 14_695_981_039_346_656_037;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(1_099_511_628_211);
    }
    h
}

#[inline]
fn hash_fp(fp: u64) -> u32 {
    (fp ^ (fp >> 32) ^ (fp >> 16)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use mimir_config::{LimitsConfig, Overrides};
    use crate::limiter::{Limiter, NoopRingLimiterStrategy};

    #[test]
    fn metric_counter_handles_hash_collisions_without_conflation() {
        let limits = LimitsConfig {
            max_global_series_per_metric: 1,
            ..Default::default()
        };
        let limiter = Arc::new(Limiter::new(
            Overrides::new(limits),
            Arc::new(NoopRingLimiterStrategy),
        ));
        let mc = MetricCounter::new(limiter, HashSet::new());
        // Simulate a 64-bit FNV-1a collision inside the same shard bucket.
        let fp = hash_metric_name("metric_a");
        let shard = &mc.shards[hash_fp(fp) as usize % NUM_METRIC_COUNTER_SHARDS];
        shard.lock().counts.insert(
            fp,
            smallvec::smallvec![("metric_b".into(), 1)],
        );
        assert!(mc.can_add_series_for("u1", "metric_a"));
        mc.increase_series_for_metric("metric_a");
        assert!(!mc.can_add_series_for("u1", "metric_a"));
        mc.decrease_series_for_metric("metric_a");
        assert!(mc.can_add_series_for("u1", "metric_a"));
        assert_eq!(
            shard.lock().counts.get(&fp).map(|b| b.as_slice()),
            Some([("metric_b".into(), 1)].as_slice())
        );
    }
}
