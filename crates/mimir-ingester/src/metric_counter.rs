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

#[derive(Default)]
struct MetricCounterShard {
    counts: hashbrown::HashMap<u64, usize, FastU64BuildHasher>,
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
        let current = guard.counts.get(&fp).copied().unwrap_or(0);
        self.limiter.is_within_max_series_per_metric(user_id, current)
    }

    pub fn increase_series_for_metric(&self, metric: &str) {
        let (shard, fp) = self.shard_and_fp(metric);
        let mut guard = shard.lock();
        *guard.counts.entry(fp).or_insert(0) += 1;
    }

    pub fn decrease_series_for_metric(&self, metric: &str) {
        let (shard, fp) = self.shard_and_fp(metric);
        let mut guard = shard.lock();
        if let Some(c) = guard.counts.get_mut(&fp) {
            if *c <= 1 {
                guard.counts.remove(&fp);
            } else {
                *c -= 1;
            }
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
