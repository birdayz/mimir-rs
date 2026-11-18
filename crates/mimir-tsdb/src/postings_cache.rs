//! Concurrent TTL + byte-bounded `PostingsForMatchersCache` with per-metric invalidation.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache.go.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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
    /// Shared cache metrics (Go `PostingsForMatchersCacheConfig.Metrics`); `None` = not exported.
    pub metrics: Option<PostingsForMatchersCacheMetrics>,
    /// Whether `invalidate_metric` counts `invalidations_total`. Go only tracks metric versions
    /// (and counts invalidations) for a shared cache with invalidation enabled; this cache always
    /// invalidates internally, so the counter follows the Go configuration instead.
    pub report_invalidations: bool,
}

impl Default for PostingsForMatchersCacheConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_POSTINGS_CACHE_TTL,
            max_items: DEFAULT_POSTINGS_CACHE_MAX_ITEMS,
            max_bytes: DEFAULT_POSTINGS_CACHE_MAX_BYTES,
            enable_invalidation: false,
            num_metric_versions: DEFAULT_METRIC_VERSIONS_SLOTS,
            metrics: None,
            report_invalidations: false,
        }
    }
}

/// Go `PostingsForMatchersCacheMetrics`. One instance is shared by every cache built from the
/// same Mimir cache factory (all tenants' Head caches, resp. all tenants' block caches), so the
/// series are aggregated across tenants exactly like Go's single registration.
#[derive(Clone)]
pub struct PostingsForMatchersCacheMetrics {
    entries: Gauge,
    bytes: Gauge,
    requests: Counter,
    hits: Counter,
    misses: Counter,
    skips_stale: Counter,
    invalidations: Counter,
    evictions_ttl: Counter,
    evictions_max_bytes: Counter,
    evictions_max_items: Counter,
    evictions_unknown: Counter,
}

impl std::fmt::Debug for PostingsForMatchersCacheMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostingsForMatchersCacheMetrics").finish_non_exhaustive()
    }
}

impl PostingsForMatchersCacheMetrics {
    /// Go `NewPostingsForMatchersCacheMetrics(prometheus.WrapRegistererWithPrefix(prefix, reg))`
    /// (Mimir: `cortex_ingester_tsdb_head_` / `cortex_ingester_tsdb_block_`). Every series,
    /// including each `reason` of `skips_total` and `evictions_total`, is exposed from the start.
    /// `reg == None` (Go `NewPostingsForMatchersCacheMetrics(nil)`) creates unregistered metrics.
    pub fn new(prefix: &str, reg: Option<&Registry>) -> Self {
        const SKIPS_HELP: &str = "Total number of requests to the PostingsForMatchers cache that have been skipped the cache. The subsequent result is not cached.";
        const EVICTIONS_HELP: &str = "Total number of evictions from the PostingsForMatchers cache.";
        let name = |suffix: &str| format!("{prefix}postings_for_matchers_cache_{suffix}");
        let gauge = |suffix: &str, help: &str| {
            let g = Gauge::new(name(suffix), help).expect("valid postings cache gauge");
            if let Some(reg) = reg {
                let _ = reg.register(Box::new(g.clone()));
            }
            g
        };
        let counter = |suffix: &str, help: &str| {
            let c = Counter::new(name(suffix), help).expect("valid postings cache counter");
            if let Some(reg) = reg {
                let _ = reg.register(Box::new(c.clone()));
            }
            c
        };
        let by_reason = |suffix: &str, help: &str, reasons: &[&str]| {
            let v = CounterVec::new(Opts::new(name(suffix), help), &["reason"])
                .expect("valid postings cache counter vec");
            let out: Vec<Counter> = reasons.iter().map(|r| v.with_label_values(&[r])).collect();
            if let Some(reg) = reg {
                let _ = reg.register(Box::new(v));
            }
            out
        };
        let entries = gauge("entries_total", "Total number of entries in the PostingsForMatchers cache.");
        let bytes = gauge("bytes_total", "Total number of bytes in the PostingsForMatchers cache.");
        let requests = counter("requests_total", "Total number of requests to the PostingsForMatchers cache.");
        let hits = counter(
            "hits_total",
            "Total number of postings lists returned from the PostingsForMatchers cache.",
        );
        let misses = counter(
            "misses_total",
            "Total number of requests to the PostingsForMatchers cache for which there is no valid cached entry. The subsequent result is cached.",
        );
        let skips = by_reason(
            "skips_total",
            SKIPS_HELP,
            &["ineligible", "stale-cached-entry", "canceled-cached-entry"],
        );
        let invalidations = counter(
            "invalidations_total",
            "Total number of cache entries that were invalidated.",
        );
        let evictions = by_reason(
            "evictions_total",
            EVICTIONS_HELP,
            &["ttl-expired", "max-bytes-reached", "max-items-reached", "unknown"],
        );
        Self {
            entries,
            bytes,
            requests,
            hits,
            misses,
            // `ineligible` (Go: non-concurrent calls without `force`, or no shared cache key) stays
            // 0: this cache serves every call (see PORT_STATUS.md).
            // `canceled-cached-entry` (Go: the caller's context was canceled while waiting for an
            // in-flight call) is exposed but never incremented: calls are not cancellable here.
            skips_stale: skips[1].clone(),
            invalidations,
            evictions_ttl: evictions[0].clone(),
            evictions_max_bytes: evictions[1].clone(),
            evictions_max_items: evictions[2].clone(),
            evictions_unknown: evictions[3].clone(),
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

const METRIC_VERSION_PAGE_BITS: usize = 13;
const METRIC_VERSION_PAGE_SLOTS: usize = 1 << METRIC_VERSION_PAGE_BITS; // 8192 * 8 B = 64 KiB

struct PagedMetricVersions {
    slots_mask: usize,
    page_len: usize,
    pages: Box<[AtomicPtr<AtomicU64>]>,
}

// SAFETY: Every non-null pointer in `pages` points to an owned `Box<[AtomicU64]>` of length
// `page_len`, which is `Sync` and reclaimed only in `Drop`.
unsafe impl Send for PagedMetricVersions {}
unsafe impl Sync for PagedMetricVersions {}

impl Drop for PagedMetricVersions {
    fn drop(&mut self) {
        for page in self.pages.iter_mut() {
            let ptr = *page.get_mut();
            if !ptr.is_null() {
                // SAFETY: `ptr` came from `Box::into_raw` on a `Box<[AtomicU64]>` of length
                // `self.page_len`.
                unsafe {
                    drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                        ptr,
                        self.page_len,
                    )));
                }
            }
        }
    }
}

impl PagedMetricVersions {
    fn new(num_metric_versions: usize) -> Self {
        let slots = num_metric_versions.max(64).next_power_of_two();
        let page_len = slots.min(METRIC_VERSION_PAGE_SLOTS);
        let num_pages = slots.div_ceil(page_len);
        let mut pages = Vec::with_capacity(num_pages);
        for _ in 0..num_pages {
            pages.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        Self {
            slots_mask: slots - 1,
            page_len,
            pages: pages.into_boxed_slice(),
        }
    }

    #[inline]
    fn slot_coords(&self, metric_name: &str) -> (usize, usize) {
        // FNV-1a matching Prometheus metricVersions.getIndex.
        let mut h: u64 = 14695981039346656037;
        for &b in metric_name.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        let idx = (h as usize) & self.slots_mask;
        (
            idx >> METRIC_VERSION_PAGE_BITS,
            idx & (METRIC_VERSION_PAGE_SLOTS - 1),
        )
    }

    #[inline]
    fn load(&self, metric_name: &str) -> u64 {
        let (page_idx, off) = self.slot_coords(metric_name);
        let ptr = self.pages[page_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return 0;
        }
        // SAFETY: `ptr` is non-null, published with `Release`, and points to `page_len`
        // `AtomicU64` elements where `off < page_len`.
        unsafe { (&*ptr.add(off)).load(Ordering::Acquire) }
    }

    #[inline]
    fn fetch_add(&self, metric_name: &str) {
        let (page_idx, off) = self.slot_coords(metric_name);
        let ptr = self.get_or_alloc_page(page_idx);
        // SAFETY: `ptr` is non-null and points to `page_len` `AtomicU64` elements where
        // `off < page_len`.
        unsafe {
            (&*ptr.add(off)).fetch_add(1, Ordering::Release);
        }
    }

    #[cold]
    fn get_or_alloc_page(&self, page_idx: usize) -> *mut AtomicU64 {
        let slot = &self.pages[page_idx];
        let ptr = slot.load(Ordering::Acquire);
        if !ptr.is_null() {
            return ptr;
        }
        let mut page = Vec::with_capacity(self.page_len);
        page.resize_with(self.page_len, || AtomicU64::new(0));
        let raw = Box::into_raw(page.into_boxed_slice()) as *mut AtomicU64;
        match slot.compare_exchange(
            std::ptr::null_mut(),
            raw,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => raw,
            Err(existing) => {
                // SAFETY: `raw` was just allocated by this thread and never published.
                unsafe {
                    drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                        raw,
                        self.page_len,
                    )));
                }
                existing
            }
        }
    }

    #[cfg(test)]
    fn allocated_pages(&self) -> usize {
        self.pages
            .iter()
            .filter(|p| !p.load(Ordering::Relaxed).is_null())
            .count()
    }
}

/// Concurrent TTL + byte-bounded cache for `postings_for_matchers` evaluations.
pub struct PostingsForMatchersCache {
    config: PostingsForMatchersCacheConfig,
    global_epoch: AtomicU64,
    postings_generation: AtomicU64,
    metric_versions: OnceLock<PagedMetricVersions>,
    inner: RwLock<CacheInner>,
    in_flight: Mutex<HashMap<InFlightKey, Arc<InFlightCall>>>,
    metrics: Option<PostingsForMatchersCacheMetrics>,
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

impl Drop for PostingsForMatchersCache {
    fn drop(&mut self) {
        if let Some(ref m) = self.metrics {
            let inner = self.inner.get_mut();
            if !inner.table.is_empty() {
                m.entries.sub(inner.table.len() as f64);
            }
            if inner.total_bytes > 0 {
                m.bytes.sub(inner.total_bytes as f64);
            }
        }
    }
}

impl PostingsForMatchersCache {
    pub fn new(config: PostingsForMatchersCacheConfig) -> Self {
        let metrics = config.metrics.clone();
        Self {
            config,
            global_epoch: AtomicU64::new(0),
            postings_generation: AtomicU64::new(0),
            metric_versions: OnceLock::new(),
            inner: RwLock::new(CacheInner::default()),
            in_flight: Mutex::new(HashMap::new()),
            metrics,
        }
    }

    /// Invalidates cached Head postings affected by creating or deleting a series with `metric_name`.
    #[inline]
    pub fn invalidate_metric(&self, metric_name: &str) {
        self.postings_generation.fetch_add(1, Ordering::Release);
        if self.config.enable_invalidation && !metric_name.is_empty() {
            if let Some(mv) = self.metric_versions.get() {
                mv.fetch_add(metric_name);
            }
        }
        if self.config.report_invalidations {
            if let Some(ref m) = self.metrics {
                m.invalidations.inc();
            }
        }
    }

    /// Invalidates all cached entries (e.g., after bulk postings rebuild/truncation).
    pub fn invalidate_all(&self) {
        self.global_epoch.fetch_add(1, Ordering::Release);
        self.postings_generation.fetch_add(1, Ordering::Release);
        let mut inner = self.inner.write();
        // The gauges are shared with other tenants' caches: subtract only this cache's share.
        if let Some(ref m) = self.metrics {
            m.entries.sub(inner.table.len() as f64);
            m.bytes.sub(inner.total_bytes as f64);
        }
        inner.table.clear();
        inner.fifo.clear();
        inner.total_bytes = 0;
    }

    #[inline]
    fn version_for_matchers(&self, matchers: &[LabelMatcher]) -> (u64, bool) {
        if !self.config.enable_invalidation {
            return (0, false);
        }
        let mv = self
            .metric_versions
            .get_or_init(|| PagedMetricVersions::new(self.config.num_metric_versions));
        let epoch = self.global_epoch.load(Ordering::Acquire);
        if let Some(metric_name) = exact_metric_name_from_matchers(matchers) {
            let ver = mv.load(metric_name);
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
        if let Some(ref m) = self.metrics {
            m.requests.inc();
        }

        if matchers.is_empty()
            || self.config.ttl.is_zero()
            || self.config.max_items == 0
            || self.config.max_bytes == 0
        {
            // Go still runs these through the cache (a miss whose result is not kept).
            if let Some(ref m) = self.metrics {
                m.misses.inc();
            }
            return compute().map(Arc::from);
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

        // Leader failed or invalidated concurrently: recompute directly (Go: a stale in-flight
        // entry is skipped and computed without the cache).
        if let Some(ref m) = self.metrics {
            m.skips_stale.inc();
        }
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
                if now.duration_since(removed.inserted_at) >= self.config.ttl {
                    m.evictions_ttl.inc();
                } else {
                    m.evictions_unknown.inc();
                }
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
            let Some(meta) = inner.fifo.pop_front() else {
                break;
            };
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
        let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            enable_invalidation: true,
            metrics: Some(PostingsForMatchersCacheMetrics::new("test_", Some(&reg))),
            ..Default::default()
        });

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
                ..Default::default()
            },
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

    fn sample(reg: &Registry, name: &str, reason: Option<&str>) -> f64 {
        for mf in reg.gather() {
            if mf.get_name() != name {
                continue;
            }
            for m in mf.get_metric() {
                let r = m.get_label().iter().find(|l| l.get_name() == "reason").map(|l| l.get_value());
                if r == reason {
                    return if m.has_gauge() {
                        m.get_gauge().get_value()
                    } else {
                        m.get_counter().get_value()
                    };
                }
            }
        }
        panic!("{name} {reason:?} not exposed");
    }

    /// Go registers one `PostingsForMatchersCacheMetrics` per Mimir cache factory and every
    /// tenant's cache updates it: the series are aggregated across caches, all `reason`s are
    /// exposed from the start, and gauges only move by each cache's own share.
    #[test]
    fn shared_metrics_are_aggregated_across_caches() {
        let reg = Registry::new();
        let metrics = PostingsForMatchersCacheMetrics::new("cortex_ingester_tsdb_head_", Some(&reg));
        let p = "cortex_ingester_tsdb_head_postings_for_matchers_cache_";
        for (suffix, reason) in [
            ("entries_total", None),
            ("bytes_total", None),
            ("requests_total", None),
            ("hits_total", None),
            ("misses_total", None),
            ("invalidations_total", None),
            ("skips_total", Some("ineligible")),
            ("skips_total", Some("stale-cached-entry")),
            ("skips_total", Some("canceled-cached-entry")),
            ("evictions_total", Some("ttl-expired")),
            ("evictions_total", Some("max-bytes-reached")),
            ("evictions_total", Some("max-items-reached")),
            ("evictions_total", Some("unknown")),
        ] {
            assert_eq!(sample(&reg, &format!("{p}{suffix}"), reason), 0.0, "{suffix} {reason:?}");
        }

        let cfg = PostingsForMatchersCacheConfig {
            enable_invalidation: true,
            max_items: 1,
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let a = PostingsForMatchersCache::new(cfg.clone());
        let b = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            report_invalidations: true,
            ..cfg
        });
        let m1 = [LabelMatcher::new(MatchType::Equal, "a", "1").unwrap()];
        let m2 = [LabelMatcher::new(MatchType::Equal, "a", "2").unwrap()];
        let ok = |v: Vec<u64>| move || Ok::<_, ()>(v);
        a.get_or_compute(&m1, ok(vec![1])).unwrap();
        a.get_or_compute(&m1, ok(vec![1])).unwrap();
        b.get_or_compute(&m1, ok(vec![1])).unwrap();
        assert_eq!(sample(&reg, &format!("{p}requests_total"), None), 3.0);
        assert_eq!(sample(&reg, &format!("{p}hits_total"), None), 1.0);
        assert_eq!(sample(&reg, &format!("{p}misses_total"), None), 2.0);
        assert_eq!(sample(&reg, &format!("{p}entries_total"), None), 2.0);
        let bytes = sample(&reg, &format!("{p}bytes_total"), None);
        assert!(bytes > 0.0);

        // max_items 1: the second entry of `a` evicts its first one.
        a.get_or_compute(&m2, ok(vec![2])).unwrap();
        assert_eq!(sample(&reg, &format!("{p}evictions_total"), Some("max-items-reached")), 1.0);
        assert_eq!(sample(&reg, &format!("{p}entries_total"), None), 2.0);

        // Invalidations are only counted when Go would track metric versions.
        a.invalidate_metric("x");
        b.invalidate_metric("x");
        assert_eq!(sample(&reg, &format!("{p}invalidations_total"), None), 1.0);

        // Dropping all of `a`'s entries leaves `b`'s share in the gauges.
        a.invalidate_all();
        assert_eq!(sample(&reg, &format!("{p}entries_total"), None), 1.0);
        assert_eq!(sample(&reg, &format!("{p}bytes_total"), None), bytes / 2.0);

        // Calls that bypass the cache are still requests (and misses, as in Go).
        a.get_or_compute(&[], ok(vec![])).unwrap();
        assert_eq!(sample(&reg, &format!("{p}requests_total"), None), 5.0);
        assert_eq!(sample(&reg, &format!("{p}misses_total"), None), 4.0);

        // Dropping `b` subtracts its remaining entries & bytes from shared gauges.
        drop(b);
        assert_eq!(sample(&reg, &format!("{p}entries_total"), None), 0.0);
        assert_eq!(sample(&reg, &format!("{p}bytes_total"), None), 0.0);
    }

    #[test]
    fn lazy_paged_metric_versions_avoids_eager_allocation() {
        let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            enable_invalidation: true,
            num_metric_versions: 2_097_152,
            ..Default::default()
        });

        // Write-only / unqueried tenant: 0 bytes & 0 pages allocated even across many invalidations.
        for i in 0..1000 {
            cache.invalidate_metric(&format!("metric_{i}"));
        }
        assert!(cache.metric_versions.get().is_none());

        // First query initializes directory, but reads allocate 0 64 KiB pages.
        let m = [LabelMatcher::new(MatchType::Equal, "__name__", "hot_metric").unwrap()];
        let r1 = cache.get_or_compute::<_, ()>(&m, || Ok(vec![10, 20])).unwrap();
        assert_eq!(&*r1, &[10, 20]);
        let mv = cache.metric_versions.get().unwrap();
        assert_eq!(mv.pages.len(), 256);
        assert_eq!(mv.allocated_pages(), 0);

        // Invalidating a single metric faults only 1 64 KiB page (instead of 16 MiB!).
        cache.invalidate_metric("hot_metric");
        assert_eq!(mv.allocated_pages(), 1);
        let r2 = cache.get_or_compute::<_, ()>(&m, || Ok(vec![10, 20, 30])).unwrap();
        assert_eq!(&*r2, &[10, 20, 30]);
    }
}
