//! Sharded per-metric series counter matching `pkg/ingester/metric_counter.go`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::limiter::Limiter;

const NUM_METRIC_COUNTER_SHARDS: usize = 128;

#[derive(Default)]
struct MetricCounterShard {
    counts: HashMap<String, usize>,
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
            shards.push(Mutex::new(MetricCounterShard::default()));
        }
        Self {
            limiter,
            shards,
            ignored_metrics,
        }
    }

    fn shard(&self, metric_name: &str) -> &Mutex<MetricCounterShard> {
        let fp = hash_metric_name(metric_name);
        let h = hash_fp(fp) as usize;
        &self.shards[h % NUM_METRIC_COUNTER_SHARDS]
    }

    pub fn can_add_series_for(&self, user_id: &str, metric: &str) -> bool {
        if self.ignored_metrics.contains(metric) {
            return true;
        }
        let guard = self.shard(metric).lock();
        let current = guard.counts.get(metric).copied().unwrap_or(0);
        self.limiter.is_within_max_series_per_metric(user_id, current)
    }

    pub fn increase_series_for_metric(&self, metric: &str) {
        let mut guard = self.shard(metric).lock();
        *guard.counts.entry(metric.to_string()).or_insert(0) += 1;
    }

    pub fn decrease_series_for_metric(&self, metric: &str) {
        let mut guard = self.shard(metric).lock();
        if let Some(c) = guard.counts.get_mut(metric) {
            if *c <= 1 {
                guard.counts.remove(metric);
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
