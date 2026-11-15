//! Concurrent TTL + byte-bounded `PostingsForMatchersCache` with per-metric invalidation.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache.go.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hashbrown::{HashMap, HashTable};
use parking_lot::{Condvar, Mutex, RwLock};
use prometheus::{Counter, CounterVec, Gauge, Opts, Registry};
use smallvec::SmallVec;
use xxhash_rust::xxh3::xxh3_64;

use crate::labels::METRIC_NAME;
use crate::matcher::{LabelMatcher, MatchType};

pub const DEFAULT_POSTINGS_CACHE_TTL: Duration = Duration::from_secs(10);
pub const DEFAULT_POSTINGS_CACHE_MAX_ITEMS: usize = 512;
pub const DEFAULT_POSTINGS_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_METRIC_VERSIONS_SLOTS: usize = 8192;

#[derive(Clone, Debug)]
pub struct PostingsForMatchersCacheConfig {
    pub ttl: Duration,
    pub max_items: usize,
    pub max_bytes: usize,
    pub enable_invalidation: bool,
    pub num_metric_versions: usize,
}

impl Default for PostingsForMatchersCacheConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_POSTINGS_CACHE_TTL,
            max_items: DEFAULT_POSTINGS_CACHE_MAX_ITEMS,
            max_bytes: DEFAULT_POSTINGS_CACHE_MAX_BYTES,
            enable_invalidation: false,
            num_metric_versions: DEFAULT_METRIC_VERSIONS_SLOTS,
        }
    }
}

struct PromMetrics {
    entries: Gauge,
    bytes: Gauge,
    requests: Counter,
    hits: Counter,
    misses: Counter,
    invalidations: Counter,
    evictions_ttl: Counter,
    evictions_max_bytes: Counter,
    evictions_max_items: Counter,
}

impl PromMetrics {
    fn register(reg: &Registry) -> Self {
        let entries = Gauge::new(
            "prometheus_tsdb_postings_for_matchers_cache_entries_total",
            "Total number of entries in the PostingsForMatchers cache.",
        )
        .expect("valid gauge");
        let bytes = Gauge::new(
            "prometheus_tsdb_postings_for_matchers_cache_bytes_total",
            "Total number of bytes in the PostingsForMatchers cache.",
        )
        .expect("valid gauge");
        let requests = Counter::new(
            "prometheus_tsdb_postings_for_matchers_cache_requests_total",
            "Total number of requests to the PostingsForMatchers cache.",
        )
        .expect("valid counter");
        let hits = Counter::new(
            "prometheus_tsdb_postings_for_matchers_cache_hits_total",
            "Total number of postings lists returned from the PostingsForMatchers cache.",
        )
        .expect("valid counter");
        let misses = Counter::new(
            "prometheus_tsdb_postings_for_matchers_cache_misses_total",
            "Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.",
        )
        .expect("valid counter");
        let invalidations = Counter::new(
            "prometheus_tsdb_postings_for_matchers_cache_invalidations_total",
            "Total number of cache entries that were invalidated.",
        )
        .expect("valid counter");
        let evictions = CounterVec::new(
            Opts::new(
                "prometheus_tsdb_postings_for_matchers_cache_evictions_total",
                "Total number of evictions from the PostingsForMatchers cache.",
            ),
            &["reason"],
        )
        .expect("valid counter vec");
        let evictions_ttl = evictions.with_label_values(&["ttl-expired"]);
        let evictions_max_bytes = evictions.with_label_values(&["max-bytes-reached"]);
        let evictions_max_items = evictions.with_label_values(&["max-items-reached"]);

        let _ = reg.register(Box::new(entries.clone()));
        let _ = reg.register(Box::new(bytes.clone()));
        let _ = reg.register(Box::new(requests.clone()));
        let _ = reg.register(Box::new(hits.clone()));
        let _ = reg.register(Box::new(misses.clone()));
        let _ = reg.register(Box::new(invalidations.clone()));
        let _ = reg.register(Box::new(evictions));

        Self {
            entries,
            bytes,
            requests,
            hits,
            misses,
            invalidations,
            evictions_ttl,
            evictions_max_bytes,
            evictions_max_items,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct InFlightKey {
    version: u64,
    is_per_metric: bool,
    key_bytes: Box<[u8]>,
}

struct InFlightState {
    done: bool,
    result: Option<Arc<[u64]>>,
}

struct InFlightCall {
    state: Mutex<InFlightState>,
    cvar: Condvar,
}

impl InFlightCall {
    fn new() -> Self {
        Self {
            state: Mutex::new(InFlightState {
                done: false,
                result: None,
            }),
            cvar: Condvar::new(),
        }
    }

    fn complete(&self, res: Option<Arc<[u64]>>) {
        let mut g = self.state.lock();
        g.result = res;
        g.done = true;
        self.cvar.notify_all();
    }

    fn wait(&self) -> Option<Arc<[u64]>> {
        let mut g = self.state.lock();
        while !g.done {
            self.cvar.wait(&mut g);
        }
        g.result.clone()
    }
}

struct InFlightGuard<'a> {
    map: &'a Mutex<HashMap<InFlightKey, Arc<InFlightCall>>>,
    key: InFlightKey,
    call: Arc<InFlightCall>,
    completed: bool,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.map.lock().remove(&self.key);
            self.call.complete(None);
        }
    }
}

struct CacheEntry {
    id: u64,
    hash: u64,
    version: u64,
    is_per_metric: bool,
    key_bytes: Box<[u8]>,
    postings: Arc<[u64]>,
    size_bytes: usize,
    inserted_at: Instant,
}

struct FifoMeta {
    id: u64,
    hash: u64,
    inserted_at: Instant,
}

#[derive(Default)]
struct CacheInner {
    table: HashTable<CacheEntry>,
    fifo: VecDeque<FifoMeta>,
    total_bytes: usize,
    next_id: u64,
}

/// Concurrent TTL + byte-bounded cache for `postings_for_matchers` evaluations.
pub struct PostingsForMatchersCache {
    config: PostingsForMatchersCacheConfig,
    global_epoch: AtomicU64,
    postings_generation: AtomicU64,
    metric_versions: Option<Box<[AtomicU64]>>,
    inner: RwLock<CacheInner>,
    in_flight: Mutex<HashMap<InFlightKey, Arc<InFlightCall>>>,
    metrics: Option<PromMetrics>,
}

impl std::fmt::Debug for PostingsForMatchersCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostingsForMatchersCache")
            .field("ttl", &self.config.ttl)
            .field("max_items", &self.config.max_items)
            .field("max_bytes", &self.config.max_bytes)
            .field("enable_invalidation", &self.config.enable_invalidation)
            .finish()
    }
}

impl PostingsForMatchersCache {
    pub fn new(config: PostingsForMatchersCacheConfig, registry: Option<&Registry>) -> Self {
        let metric_versions = if config.enable_invalidation {
            let slots = config.num_metric_versions.max(64).next_power_of_two();
            let mut v = Vec::with_capacity(slots);
            for _ in 0..slots {
                v.push(AtomicU64::new(0));
            }
            Some(v.into_boxed_slice())
        } else {
            None
        };
        let metrics = registry.map(PromMetrics::register);
        Self {
            config,
            global_epoch: AtomicU64::new(0),
            postings_generation: AtomicU64::new(0),
            metric_versions,
            inner: RwLock::new(CacheInner::default()),
            in_flight: Mutex::new(HashMap::new()),
            metrics,
        }
    }

    #[inline]
    fn metric_slot(&self, metric_name: &str, len: usize) -> usize {
        // FNV-1a matching Prometheus metricVersions.getIndex.
        let mut h: u64 = 14695981039346656037;
        for &b in metric_name.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        (h as usize) & (len - 1)
    }

    /// Invalidates cached Head postings affected by creating or deleting a series with `metric_name`.
    #[inline]
    pub fn invalidate_metric(&self, metric_name: &str) {
        self.postings_generation.fetch_add(1, Ordering::Release);
        if let Some(ref mv) = self.metric_versions {
            if !metric_name.is_empty() {
                let idx = self.metric_slot(metric_name, mv.len());
                mv[idx].fetch_add(1, Ordering::Release);
            }
        }
        if let Some(ref m) = self.metrics {
            m.invalidations.inc();
        }
    }

    /// Invalidates all cached entries (e.g., after bulk postings rebuild/truncation).
    pub fn invalidate_all(&self) {
        self.global_epoch.fetch_add(1, Ordering::Release);
        self.postings_generation.fetch_add(1, Ordering::Release);
        let mut inner = self.inner.write();
        inner.table.clear();
        inner.fifo.clear();
        inner.total_bytes = 0;
        if let Some(ref m) = self.metrics {
            m.entries.set(0.0);
            m.bytes.set(0.0);
            m.invalidations.inc();
        }
    }

    #[inline]
    fn version_for_matchers(&self, matchers: &[LabelMatcher]) -> (u64, bool) {
        let Some(ref mv) = self.metric_versions else {
            return (0, false);
        };
        let epoch = self.global_epoch.load(Ordering::Acquire);
        if let Some(metric_name) = exact_metric_name_from_matchers(matchers) {
            let idx = self.metric_slot(metric_name, mv.len());
            let ver = mv[idx].load(Ordering::Acquire);
            ((epoch << 32) ^ ver, true)
        } else {
            let gen = self.postings_generation.load(Ordering::Acquire);
            ((epoch << 32) ^ gen, false)
        }
    }

    /// Evaluates `matchers` using the cache, returning an `Arc<[u64]>` of matching series refs.
    pub fn get_or_compute<F, E>(
        &self,
        matchers: &[LabelMatcher],
        compute: F,
    ) -> Result<Arc<[u64]>, E>
    where
        F: FnOnce() -> Result<Vec<u64>, E>,
    {
        if matchers.is_empty()
            || self.config.ttl.is_zero()
            || self.config.max_items == 0
            || self.config.max_bytes == 0
        {
            return compute().map(Arc::from);
        }

        if let Some(ref m) = self.metrics {
            m.requests.inc();
        }

        let mut key_buf = SmallVec::<[u8; 256]>::new();
        encode_canonical_matchers_key(matchers, &mut key_buf);

        let (ver_before, is_per_metric) = self.version_for_matchers(matchers);
        let hash = cache_entry_hash(ver_before, is_per_metric, &key_buf);
        let now = Instant::now();

        {
            let inner = self.inner.read();
            if let Some(entry) = inner.table.find(hash, |e| {
                e.version == ver_before
                    && e.is_per_metric == is_per_metric
                    && e.key_bytes.as_ref() == key_buf.as_slice()
            }) {
                if now.duration_since(entry.inserted_at) < self.config.ttl {
                    if let Some(ref m) = self.metrics {
                        m.hits.inc();
                    }
                    return Ok(Arc::clone(&entry.postings));
                }
            }
        }

        // Coalesce concurrent identical cache misses.
        let flight_key = InFlightKey {
            version: ver_before,
            is_per_metric,
            key_bytes: key_buf.as_slice().into(),
        };

        let existing_call = {
            let mut flights = self.in_flight.lock();
            if let Some(call) = flights.get(&flight_key) {
                Some(Arc::clone(call))
            } else {
                let call = Arc::new(InFlightCall::new());
                flights.insert(flight_key.clone(), Arc::clone(&call));
                drop(flights);
                let mut guard = InFlightGuard {
                    map: &self.in_flight,
                    key: flight_key,
                    call,
                    completed: false,
                };

                if let Some(ref m) = self.metrics {
                    m.misses.inc();
                }

                let vec = compute()?;
                let arc: Arc<[u64]> = Arc::from(vec);
                let (ver_after, _) = self.version_for_matchers(matchers);

                if ver_after == ver_before {
                    self.insert_entry(
                        hash,
                        ver_before,
                        is_per_metric,
                        guard.key.key_bytes.clone(),
                        Arc::clone(&arc),
                    );
                    guard.map.lock().remove(&guard.key);
                    guard.call.complete(Some(Arc::clone(&arc)));
                    guard.completed = true;
                } else {
                    guard.map.lock().remove(&guard.key);
                    guard.call.complete(None);
                    guard.completed = true;
                }
                return Ok(arc);
            }
        };

        if let Some(call) = existing_call {
            if let Some(arc) = call.wait() {
                if let Some(ref m) = self.metrics {
                    m.hits.inc();
                }
                return Ok(arc);
            }
        }

        // Leader failed or invalidated concurrently: recompute directly.
        compute().map(Arc::from)
    }

    fn insert_entry(
        &self,
        hash: u64,
        version: u64,
        is_per_metric: bool,
        key_bytes: Box<[u8]>,
        postings: Arc<[u64]>,
    ) {
        let size_bytes =
            key_bytes.len() + postings.len() * std::mem::size_of::<u64>() + std::mem::size_of::<CacheEntry>();
        if size_bytes > self.config.max_bytes {
            return;
        }

        let now = Instant::now();
        let mut inner = self.inner.write();
        self.evict_locked(&mut inner, now);

        // If an expired entry with the exact same key is still in table, replace/remove it first.
        if let Ok(occ) = inner.table.find_entry(hash, |e| {
            e.version == version
                && e.is_per_metric == is_per_metric
                && e.key_bytes.as_ref() == key_bytes.as_ref()
        }) {
            let (removed, _) = occ.remove();
            inner.total_bytes = inner.total_bytes.saturating_sub(removed.size_bytes);
            if let Some(ref m) = self.metrics {
                m.entries.dec();
                m.bytes.sub(removed.size_bytes as f64);
            }
        }

        inner.next_id = inner.next_id.wrapping_add(1);
        let id = inner.next_id;
        inner.total_bytes += size_bytes;
        inner.fifo.push_back(FifoMeta {
            id,
            hash,
            inserted_at: now,
        });
        inner.table.insert_unique(
            hash,
            CacheEntry {
                id,
                hash,
                version,
                is_per_metric,
                key_bytes,
                postings,
                size_bytes,
                inserted_at: now,
            },
            |e| e.hash,
        );

        if let Some(ref m) = self.metrics {
            m.entries.inc();
            m.bytes.add(size_bytes as f64);
        }

        self.evict_locked(&mut inner, now);
    }

    fn evict_locked(&self, inner: &mut CacheInner, now: Instant) {
        while let Some(front) = inner.fifo.front() {
            let expired = now.duration_since(front.inserted_at) >= self.config.ttl;
            let over_bytes = inner.total_bytes > self.config.max_bytes;
            let over_items = inner.table.len() > self.config.max_items;
            if !expired && !over_bytes && !over_items {
                break;
            }
            let meta = inner.fifo.pop_front().unwrap();
            if let Ok(occ) = inner.table.find_entry(meta.hash, |e| e.id == meta.id) {
                let (removed, _) = occ.remove();
                inner.total_bytes = inner.total_bytes.saturating_sub(removed.size_bytes);
                if let Some(ref m) = self.metrics {
                    m.entries.dec();
                    m.bytes.sub(removed.size_bytes as f64);
                    if expired {
                        m.evictions_ttl.inc();
                    } else if over_bytes {
                        m.evictions_max_bytes.inc();
                    } else {
                        m.evictions_max_items.inc();
                    }
                }
            }
        }
    }
}

#[inline]
fn exact_metric_name_from_matchers(matchers: &[LabelMatcher]) -> Option<&str> {
    let mut found: Option<&str> = None;
    for m in matchers {
        if m.name == METRIC_NAME && m.match_type == MatchType::Equal && !m.value.is_empty() {
            match found {
                None => found = Some(m.value.as_str()),
                Some(prev) if prev != m.value => return None,
                Some(_) => {}
            }
        }
    }
    found
}

#[inline]
fn match_type_code(mt: MatchType) -> u8 {
    match mt {
        MatchType::Equal => 0,
        MatchType::NotEqual => 1,
        MatchType::RegexMatch => 2,
        MatchType::RegexNotMatch => 3,
    }
}

fn encode_canonical_matchers_key(matchers: &[LabelMatcher], buf: &mut SmallVec<[u8; 256]>) {
    if matchers.len() == 1 {
        let m = &matchers[0];
        buf.push(match_type_code(m.match_type));
        buf.extend_from_slice(&(m.name.len() as u32).to_le_bytes());
        buf.extend_from_slice(m.name.as_bytes());
        buf.extend_from_slice(&(m.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(m.value.as_bytes());
        return;
    }

    let mut indices: SmallVec<[usize; 16]> = (0..matchers.len()).collect();
    indices.sort_unstable_by(|&a, &b| {
        let ma = &matchers[a];
        let mb = &matchers[b];
        match_type_code(ma.match_type)
            .cmp(&match_type_code(mb.match_type))
            .then_with(|| ma.name.cmp(&mb.name))
            .then_with(|| ma.value.cmp(&mb.value))
    });

    for idx in indices {
        let m = &matchers[idx];
        buf.push(match_type_code(m.match_type));
        buf.extend_from_slice(&(m.name.len() as u32).to_le_bytes());
        buf.extend_from_slice(m.name.as_bytes());
        buf.extend_from_slice(&(m.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(m.value.as_bytes());
    }
}

#[inline]
fn cache_entry_hash(version: u64, is_per_metric: bool, key_bytes: &[u8]) -> u64 {
    let h = xxh3_64(key_bytes);
    h ^ version.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (is_per_metric as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn postings_cache_hit_and_per_metric_invalidation() {
        let reg = Registry::new();
        let cache = PostingsForMatchersCache::new(
            PostingsForMatchersCacheConfig {
                enable_invalidation: true,
                ..Default::default()
            },
            Some(&reg),
        );

        let m_a = vec![
            LabelMatcher::new(MatchType::Equal, "__name__", "metric_a").unwrap(),
            LabelMatcher::new(MatchType::Equal, "job", "api").unwrap(),
        ];
        // Reordered matchers hit the same canonical entry
        let m_a_reordered = vec![
            LabelMatcher::new(MatchType::Equal, "job", "api").unwrap(),
            LabelMatcher::new(MatchType::Equal, "__name__", "metric_a").unwrap(),
        ];
        let m_no_name = vec![LabelMatcher::new(MatchType::Equal, "job", "api").unwrap()];

        let calls = AtomicUsize::new(0);
        let r1 = cache
            .get_or_compute::<_, ()>(&m_a, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3])
            })
            .unwrap();
        assert_eq!(&*r1, &[1, 2, 3]);
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        // Cache hit (reordered)
        let r2 = cache
            .get_or_compute::<_, ()>(&m_a_reordered, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![999])
            })
            .unwrap();
        assert_eq!(&*r2, &[1, 2, 3]);
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        // Query without exact __name__
        let r_no_name = cache
            .get_or_compute::<_, ()>(&m_no_name, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3, 4])
            })
            .unwrap();
        assert_eq!(&*r_no_name, &[1, 2, 3, 4]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        // Creating a series in metric_b does NOT invalidate metric_a!
        cache.invalidate_metric("metric_b");

        let r3 = cache
            .get_or_compute::<_, ()>(&m_a, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![999])
            })
            .unwrap();
        assert_eq!(&*r3, &[1, 2, 3]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        // However, query without __name__ IS invalidated by metric_b creation
        let r_no_name_2 = cache
            .get_or_compute::<_, ()>(&m_no_name, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3, 4, 5])
            })
            .unwrap();
        assert_eq!(&*r_no_name_2, &[1, 2, 3, 4, 5]);
        assert_eq!(calls.load(Ordering::Relaxed), 3);

        // Creating a series in metric_a invalidates metric_a
        cache.invalidate_metric("metric_a");
        let r4 = cache
            .get_or_compute::<_, ()>(&m_a, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3, 6])
            })
            .unwrap();
        assert_eq!(&*r4, &[1, 2, 3, 6]);
        assert_eq!(calls.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn postings_cache_evicts_by_max_items_and_ttl() {
        let cache = PostingsForMatchersCache::new(
            PostingsForMatchersCacheConfig {
                ttl: Duration::from_millis(40),
                max_items: 2,
                max_bytes: 1024 * 1024,
                enable_invalidation: false,
                num_metric_versions: 64,
            },
            None,
        );

        let m1 = [LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()];
        let m2 = [LabelMatcher::new(MatchType::Equal, "a", "2").unwrap()];
        let m3 = [LabelMatcher::new(MatchType::Equal, "a", "3").unwrap()];

        let calls = AtomicUsize::new(0);
        for m in [&m1, &m2, &m3] {
            cache
                .get_or_compute::<_, ()>(m, || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(vec![1])
                })
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 3);

        // m1 should have been evicted when m3 was inserted (max_items = 2)
        cache
            .get_or_compute::<_, ()>(&m1, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1])
            })
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 4);

        // Wait for TTL expiry
        std::thread::sleep(Duration::from_millis(50));
        cache
            .get_or_compute::<_, ()>(&m1, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![2])
            })
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 5);
    }
}
