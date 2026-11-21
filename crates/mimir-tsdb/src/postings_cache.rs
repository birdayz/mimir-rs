//! Concurrent TTL + byte-bounded `PostingsForMatchersCache` with per-metric invalidation.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/postings_for_matchers_cache.go.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
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
    skips_ineligible: Counter,
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
            skips_ineligible: skips[0].clone(),
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

    /// Returns the shared `invalidations_total` counter registered for this cache factory.
    #[inline]
    pub fn invalidations(&self) -> &Counter {
        &self.invalidations
    }
}

static NEXT_POSTINGS_CACHE_SOURCE_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates a globally unique monotonic source ID for a `Head` or `Block` instance so
/// entries in a shared `PostingsForMatchersCache` never collide across tenants or blocks.
#[inline]
pub fn next_postings_cache_source_id() -> u64 {
    NEXT_POSTINGS_CACHE_SOURCE_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct InFlightKey {
    source_id: u64,
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
    source_id: u64,
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
    fn slot_coords(&self, source_id: u64, metric_name: &str) -> (usize, usize) {
        // FNV-1a matching Prometheus metricVersions.getIndex(key, metricName).
        let mut h: u64 = 14695981039346656037;
        if source_id != 0 {
            for b in source_id.to_le_bytes() {
                h ^= b as u64;
                h = h.wrapping_mul(1099511628211);
            }
        }
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
    fn load(&self, source_id: u64, metric_name: &str) -> u64 {
        let (page_idx, off) = self.slot_coords(source_id, metric_name);
        let ptr = self.pages[page_idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return 0;
        }
        // SAFETY: `ptr` is non-null, published with `Release`, and points to `page_len`
        // `AtomicU64` elements where `off < page_len`.
        unsafe { (&*ptr.add(off)).load(Ordering::Acquire) }
    }

    #[inline]
    fn fetch_add(&self, source_id: u64, metric_name: &str) {
        let (page_idx, off) = self.slot_coords(source_id, metric_name);
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
    force: AtomicBool,
    shared: AtomicBool,
    global_epoch: AtomicU64,
    has_source_epochs: AtomicBool,
    source_epochs: RwLock<HashMap<u64, u64>>,
    metric_versions: OnceLock<PagedMetricVersions>,
    inner: RwLock<CacheInner>,
    in_flight: Mutex<HashMap<InFlightKey, Arc<InFlightCall>>>,
    expire_in_progress: AtomicBool,
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
            force: AtomicBool::new(false),
            shared: AtomicBool::new(false),
            global_epoch: AtomicU64::new(0),
            has_source_epochs: AtomicBool::new(false),
            source_epochs: RwLock::new(HashMap::new()),
            metric_versions: OnceLock::new(),
            inner: RwLock::new(CacheInner::default()),
            in_flight: Mutex::new(HashMap::new()),
            expire_in_progress: AtomicBool::new(false),
            metrics,
        }
    }

    /// Sets Mimir cache eligibility flags (`force` and `shared`, default `false`) matching Go
    /// `PostingsForMatchersCacheConfig.{Force,Shared}`.
    pub fn set_eligibility(&self, force: bool, shared: bool) {
        self.force.store(force, Ordering::Release);
        self.shared.store(shared, Ordering::Release);
    }

    #[inline]
    pub fn force(&self) -> bool {
        self.force.load(Ordering::Acquire)
    }

    #[inline]
    pub fn shared(&self) -> bool {
        self.shared.load(Ordering::Acquire)
    }

    /// Returns true only when both `shared` and `enable_invalidation` are enabled, matching
    /// Go `postings_for_matchers_cache.go:146-151` (`config.Shared && config.Invalidation`).
    #[inline]
    pub fn invalidation_active(&self) -> bool {
        self.shared() && self.config.enable_invalidation
    }

    /// Invalidates cached Head postings for `source_id` and `metric_name` without incrementing
    /// `invalidations_total` (used internally during WAL replay and Head GC).
    #[inline]
    pub fn invalidate_metric_silent_for_source(&self, source_id: u64, metric_name: &str) {
        if !self.invalidation_active() {
            return;
        }
        if let Some(mv) = self.metric_versions.get() {
            mv.fetch_add(source_id, metric_name);
        }
    }

    /// Invalidates cached Head postings affected by creating or deleting a series with `metric_name`
    /// without incrementing `invalidations_total`.
    #[inline]
    pub fn invalidate_metric_silent(&self, metric_name: &str) {
        self.invalidate_metric_silent_for_source(0, metric_name);
    }

    /// Invalidates cached Head postings for `source_id` and `metric_name`.
    #[inline]
    pub fn invalidate_metric_for_source(&self, source_id: u64, metric_name: &str) {
        if !self.invalidation_active() {
            return;
        }
        self.invalidate_metric_silent_for_source(source_id, metric_name);
        if self.config.report_invalidations && !metric_name.is_empty() {
            if let Some(ref m) = self.metrics {
                m.invalidations.inc();
            }
        }
    }

    /// Invalidates cached Head postings affected by creating or deleting a series with `metric_name`.
    #[inline]
    pub fn invalidate_metric(&self, metric_name: &str) {
        self.invalidate_metric_for_source(0, metric_name);
    }

    /// Returns `true` if `source_epochs` is non-empty (requiring a read lock on the query path).
    #[inline]
    pub fn has_source_epochs(&self) -> bool {
        self.has_source_epochs.load(Ordering::Acquire)
    }

    /// Removes `source_id` from `source_epochs` and purges any cached entries belonging to it
    /// (called when a `Head` or `Block` is dropped). Resets `has_source_epochs` to `false` once
    /// no per-source epochs remain so the query hot path avoids acquiring `source_epochs.read()`.
    pub fn remove_source(&self, source_id: u64) {
        if self.has_source_epochs.load(Ordering::Acquire) {
            let mut epochs = self.source_epochs.write();
            if epochs.remove(&source_id).is_some() && epochs.is_empty() {
                self.has_source_epochs.store(false, Ordering::Release);
            }
        }
        if self.shared() {
            let mut inner = self.inner.write();
            let mut removed_entries = 0usize;
            let mut removed_bytes = 0usize;
            inner.table.retain(|e| {
                if e.source_id == source_id {
                    removed_entries += 1;
                    removed_bytes += e.size_bytes;
                    false
                } else {
                    true
                }
            });
            inner.total_bytes = inner.total_bytes.saturating_sub(removed_bytes);
            if removed_entries > 0 {
                if let Some(ref m) = self.metrics {
                    m.entries.sub(removed_entries as f64);
                    m.bytes.sub(removed_bytes as f64);
                }
            }
        }
    }

    /// Invalidates all cached entries belonging to `source_id` (without disturbing other
    /// sources in a shared cache).
    pub fn invalidate_source(&self, source_id: u64) {
        if !self.shared() {
            self.invalidate_all();
            return;
        }
        if !self.config.enable_invalidation {
            return;
        }
        {
            let mut epochs = self.source_epochs.write();
            let entry = epochs.entry(source_id).or_insert(0);
            *entry = entry.wrapping_add(1);
            self.has_source_epochs.store(true, Ordering::Release);
        }
        let mut inner = self.inner.write();
        let mut removed_entries = 0usize;
        let mut removed_bytes = 0usize;
        inner.table.retain(|e| {
            if e.source_id == source_id {
                removed_entries += 1;
                removed_bytes += e.size_bytes;
                false
            } else {
                true
            }
        });
        inner.total_bytes = inner.total_bytes.saturating_sub(removed_bytes);
        if removed_entries > 0 {
            if let Some(ref m) = self.metrics {
                m.entries.sub(removed_entries as f64);
                m.bytes.sub(removed_bytes as f64);
            }
        }
    }

    /// Invalidates all cached entries (e.g., after bulk postings rebuild/truncation).
    pub fn invalidate_all(&self) {
        self.global_epoch.fetch_add(1, Ordering::Release);
        let mut inner = self.inner.write();
        // The gauges may be shared across caches: subtract only this cache's entries.
        if let Some(ref m) = self.metrics {
            m.entries.sub(inner.table.len() as f64);
            m.bytes.sub(inner.total_bytes as f64);
        }
        inner.table.clear();
        inner.fifo.clear();
        inner.total_bytes = 0;
    }

    #[inline]
    fn version_for_matchers(&self, source_id: u64, matchers: &[LabelMatcher]) -> (u64, bool) {
        if !self.invalidation_active() {
            return (0, false);
        }
        let mv = self
            .metric_versions
            .get_or_init(|| PagedMetricVersions::new(self.config.num_metric_versions));
        let source_epoch = if self.has_source_epochs.load(Ordering::Acquire) {
            self.source_epochs
                .read()
                .get(&source_id)
                .copied()
                .unwrap_or(0)
        } else {
            0
        };
        let epoch = self.global_epoch.load(Ordering::Acquire) ^ source_epoch;
        if let Some(metric_name) = exact_metric_name_from_matchers(matchers) {
            let ver = mv.load(source_id, metric_name);
            ((epoch << 32) ^ (ver & 0xFFFF_FFFF), true)
        } else {
            (epoch << 32, false)
        }
    }

    /// Evaluates `matchers` using the cache (with default `source_id = 0`).
    pub fn get_or_compute<F, E>(
        &self,
        matchers: &[LabelMatcher],
        concurrent: bool,
        compute: F,
    ) -> Result<Arc<[u64]>, E>
    where
        F: FnOnce() -> Result<Vec<u64>, E>,
    {
        self.get_or_compute_for_source(0, matchers, concurrent, compute)
    }

    /// Evaluates `matchers` for `source_id` using the cache, returning an `Arc<[u64]>` of
    /// matching series refs.
    ///
    /// Matches Go `PostingsForMatchersCache.PostingsForMatchers` (`postings_for_matchers_cache.go:239-280`):
    /// skips the cache (incrementing `skips_total{reason="ineligible"}`) when
    /// `(!concurrent && !self.force()) || (self.invalidation_active() && exact_metric_name_from_matchers(matchers).is_none())`.
    pub fn get_or_compute_for_source<F, E>(
        &self,
        source_id: u64,
        matchers: &[LabelMatcher],
        concurrent: bool,
        compute: F,
    ) -> Result<Arc<[u64]>, E>
    where
        F: FnOnce() -> Result<Vec<u64>, E>,
    {
        if let Some(ref m) = self.metrics {
            m.requests.inc();
        }

        let skip_ineligible = (!concurrent && !self.force())
            || (self.invalidation_active()
                && exact_metric_name_from_matchers(matchers).is_none());
        if skip_ineligible {
            if let Some(ref m) = self.metrics {
                m.skips_ineligible.inc();
            }
            return compute().map(Arc::from);
        }

        self.expire();

        let mut key_buf = SmallVec::<[u8; 256]>::new();
        encode_canonical_matchers_key(matchers, &mut key_buf);

        let (ver_before, is_per_metric) = self.version_for_matchers(source_id, matchers);
        let hash = cache_entry_hash(source_id, ver_before, is_per_metric, &key_buf);
        let now = Instant::now();

        if !self.config.ttl.is_zero() {
            let inner = self.inner.read();
            if let Some(entry) = inner.table.find(hash, |e| {
                e.source_id == source_id
                    && e.version == ver_before
                    && e.is_per_metric == is_per_metric
                    && e.key_bytes.as_ref() == key_buf.as_slice()
            }) {
                if now.duration_since(entry.inserted_at) < self.config.ttl {
                    if let Some(ref m) = self.metrics {
                        m.hits.inc();
                    }
                    return Ok(Arc::clone(&entry.postings));
                }
                drop(inner);
                if let Some(ref m) = self.metrics {
                    m.skips_stale.inc();
                }
                return compute().map(Arc::from);
            }
        }

        // Coalesce concurrent identical cache misses (including ttl == 0 and empty matchers).
        let flight_key = InFlightKey {
            source_id,
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
                let (ver_after, _) = self.version_for_matchers(source_id, matchers);

                if ver_after == ver_before {
                    if !self.config.ttl.is_zero() {
                        self.insert_entry(
                            source_id,
                            hash,
                            ver_before,
                            is_per_metric,
                            guard.key.key_bytes.clone(),
                            Arc::clone(&arc),
                        );
                    }
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

    /// Bounded read-side TTL & size expiration matching Go `PostingsForMatchersCache.expire()`.
    pub fn expire(&self) {
        if self.config.ttl.is_zero() {
            return;
        }
        if self
            .expire_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        struct ExpireReset<'a>(&'a AtomicBool);
        impl Drop for ExpireReset<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _reset = ExpireReset(&self.expire_in_progress);
        let now = Instant::now();
        {
            let inner = self.inner.read();
            if !self.should_evict_head(&inner, now) {
                return;
            }
        }
        let mut inner = self.inner.write();
        self.evict_locked(&mut inner, now);
    }

    #[inline]
    fn should_evict_head(&self, inner: &CacheInner, now: Instant) -> bool {
        if inner.table.len() > self.config.max_items || inner.total_bytes > self.config.max_bytes {
            return true;
        }
        inner
            .fifo
            .front()
            .is_some_and(|front| now.duration_since(front.inserted_at) >= self.config.ttl)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_entry(
        &self,
        source_id: u64,
        hash: u64,
        version: u64,
        is_per_metric: bool,
        key_bytes: Box<[u8]>,
        postings: Arc<[u64]>,
    ) {
        let size_bytes =
            key_bytes.len() + postings.len() * std::mem::size_of::<u64>() + std::mem::size_of::<CacheEntry>();
        if self.config.max_items == 0 || size_bytes > self.config.max_bytes {
            return;
        }

        let now = Instant::now();
        let mut inner = self.inner.write();
        self.evict_locked(&mut inner, now);

        // If an expired entry with the exact same key is still in table, replace/remove it first.
        if let Ok(occ) = inner.table.find_entry(hash, |e| {
            e.source_id == source_id
                && e.version == version
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
                source_id,
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

/// Returns the value of the first `__name__=...` equality matcher (even if empty or followed by
/// another `__name__=` matcher), matching Go `metricNameFromMatchers`.
#[inline]
pub fn exact_metric_name_from_matchers(matchers: &[LabelMatcher]) -> Option<&str> {
    matchers
        .iter()
        .find(|m| m.name == METRIC_NAME && m.match_type == MatchType::Equal)
        .map(|m| m.value.as_str())
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
fn cache_entry_hash(source_id: u64, version: u64, is_per_metric: bool, key_bytes: &[u8]) -> u64 {
    let h = xxh3_64(key_bytes);
    h ^ source_id.wrapping_mul(0xBF58_476D_1CE4_E5B9)
        ^ version.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (is_per_metric as u64)
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Barrier;

    #[test]
    fn postings_cache_hit_and_per_metric_invalidation() {
        let reg = Registry::new();
        let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            enable_invalidation: true,
            metrics: Some(PostingsForMatchersCacheMetrics::new("test_", Some(&reg))),
            ..Default::default()
        });
        cache.set_eligibility(true, true);

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
            .get_or_compute::<_, ()>(&m_a, true, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3])
            })
            .unwrap();
        assert_eq!(&*r1, &[1, 2, 3]);
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        // Cache hit (reordered)
        let r2 = cache
            .get_or_compute::<_, ()>(&m_a_reordered, true, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![999])
            })
            .unwrap();
        assert_eq!(&*r2, &[1, 2, 3]);
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        // With shared + invalidation active, query without exact __name__ skips cache as ineligible
        let r_no_name = cache
            .get_or_compute::<_, ()>(&m_no_name, true, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3, 4])
            })
            .unwrap();
        assert_eq!(&*r_no_name, &[1, 2, 3, 4]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(sample(&reg, "test_postings_for_matchers_cache_skips_total", Some("ineligible")), 1.0);

        // Creating a series in metric_b does NOT invalidate metric_a!
        cache.invalidate_metric("metric_b");

        let r3 = cache
            .get_or_compute::<_, ()>(&m_a, true, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![999])
            })
            .unwrap();
        assert_eq!(&*r3, &[1, 2, 3]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        // Creating a series in metric_a invalidates metric_a
        cache.invalidate_metric("metric_a");
        let r4 = cache
            .get_or_compute::<_, ()>(&m_a, true, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1, 2, 3, 6])
            })
            .unwrap();
        assert_eq!(&*r4, &[1, 2, 3, 6]);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
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
                .get_or_compute::<_, ()>(m, true, || {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(vec![1])
                })
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 3);

        // m1 should have been evicted when m3 was inserted (max_items = 2)
        cache
            .get_or_compute::<_, ()>(&m1, true, || {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(vec![1])
            })
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 4);

        // Wait for TTL expiry
        std::thread::sleep(Duration::from_millis(50));
        cache
            .get_or_compute::<_, ()>(&m1, true, || {
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
        a.set_eligibility(true, true);
        b.set_eligibility(true, true);
        let m1 = [LabelMatcher::new(MatchType::Equal, "__name__", "x").unwrap()];
        let m2 = [LabelMatcher::new(MatchType::Equal, "__name__", "y").unwrap()];
        let ok = |v: Vec<u64>| move || Ok::<_, ()>(v);
        a.get_or_compute(&m1, true, ok(vec![1])).unwrap();
        a.get_or_compute(&m1, true, ok(vec![1])).unwrap();
        b.get_or_compute(&m1, true, ok(vec![1])).unwrap();
        assert_eq!(sample(&reg, &format!("{p}requests_total"), None), 3.0);
        assert_eq!(sample(&reg, &format!("{p}hits_total"), None), 1.0);
        assert_eq!(sample(&reg, &format!("{p}misses_total"), None), 2.0);
        assert_eq!(sample(&reg, &format!("{p}entries_total"), None), 2.0);
        let bytes = sample(&reg, &format!("{p}bytes_total"), None);
        assert!(bytes > 0.0);

        // max_items 1: the second entry of `a` evicts its first one.
        a.get_or_compute(&m2, true, ok(vec![2])).unwrap();
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

        // Calls that skip the cache are still counted as requests.
        a.get_or_compute(&[], true, ok(vec![])).unwrap();
        assert_eq!(sample(&reg, &format!("{p}requests_total"), None), 5.0);
        assert_eq!(sample(&reg, &format!("{p}skips_total"), Some("ineligible")), 1.0);

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
        cache.set_eligibility(true, true);

        // Write-only / unqueried tenant: 0 bytes & 0 pages allocated even across many invalidations.
        for i in 0..1000 {
            cache.invalidate_metric(&format!("metric_{i}"));
        }
        assert!(cache.metric_versions.get().is_none());

        // First query initializes directory, but reads allocate 0 64 KiB pages.
        let m = [LabelMatcher::new(MatchType::Equal, "__name__", "hot_metric").unwrap()];
        let r1 = cache.get_or_compute::<_, ()>(&m, true, || Ok(vec![10, 20])).unwrap();
        assert_eq!(&*r1, &[10, 20]);
        let mv = cache.metric_versions.get().unwrap();
        assert_eq!(mv.pages.len(), 256);
        assert_eq!(mv.allocated_pages(), 0);

        // Invalidating a single metric faults only 1 64 KiB page (instead of 16 MiB!).
        cache.invalidate_metric("hot_metric");
        assert_eq!(mv.allocated_pages(), 1);
        let r2 = cache.get_or_compute::<_, ()>(&m, true, || Ok(vec![10, 20, 30])).unwrap();
        assert_eq!(&*r2, &[10, 20, 30]);
    }

    #[test]
    fn sq_01_eligibility_skips_non_concurrent_without_force_and_shared_without_metric_name() {
        let reg = Registry::new();
        let metrics = PostingsForMatchersCacheMetrics::new("sq01_", Some(&reg));
        let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            enable_invalidation: true,
            metrics: Some(metrics),
            ..Default::default()
        });
        let m_named = [LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()];
        let m_job = [LabelMatcher::new(MatchType::Equal, "job", "api").unwrap()];
        let calls = AtomicUsize::new(0);

        // Default force=false, shared=false: non-concurrent calls bypass cache and count skips_ineligible.
        let r1 = cache
            .get_or_compute::<_, ()>(&m_named, false, || {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(vec![n as u64])
            })
            .unwrap();
        let r2 = cache
            .get_or_compute::<_, ()>(&m_named, false, || {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(vec![n as u64])
            })
            .unwrap();
        assert_eq!(&*r1, &[1]);
        assert_eq!(&*r2, &[2]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(
            sample(&reg, "sq01_postings_for_matchers_cache_skips_total", Some("ineligible")),
            2.0
        );
        assert_eq!(sample(&reg, "sq01_postings_for_matchers_cache_misses_total", None), 0.0);

        // With force=true, shared=false: non-concurrent calls DO use the cache.
        cache.set_eligibility(true, false);
        let r3 = cache
            .get_or_compute::<_, ()>(&m_named, false, || {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(vec![n as u64])
            })
            .unwrap();
        let r4 = cache
            .get_or_compute::<_, ()>(&m_named, false, || {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(vec![n as u64])
            })
            .unwrap();
        assert_eq!(&*r3, &[3]);
        assert_eq!(&*r4, &[3]);
        assert_eq!(sample(&reg, "sq01_postings_for_matchers_cache_hits_total", None), 1.0);

        // With shared=true + enable_invalidation=true: queries without exact __name__ skip cache
        // even when concurrent=true or force=true.
        cache.set_eligibility(true, true);
        let r5 = cache
            .get_or_compute::<_, ()>(&m_job, true, || {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(vec![n as u64])
            })
            .unwrap();
        let r6 = cache
            .get_or_compute::<_, ()>(&m_job, true, || {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(vec![n as u64])
            })
            .unwrap();
        assert_eq!(&*r5, &[4]);
        assert_eq!(&*r6, &[5]);
        assert_eq!(
            sample(&reg, "sq01_postings_for_matchers_cache_skips_total", Some("ineligible")),
            4.0
        );
    }

    #[test]
    fn f2_shared_cache_disambiguates_sources_and_isolates_invalidations() {
        let reg = Registry::new();
        let metrics = PostingsForMatchersCacheMetrics::new("f2_", Some(&reg));
        // Budget allows ~1 entry (CacheEntry + 8B postings + key ~= 120B).
        let cache = Arc::new(PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            ttl: Duration::from_secs(60),
            max_items: usize::MAX,
            max_bytes: 180,
            enable_invalidation: true,
            num_metric_versions: 1024,
            metrics: Some(metrics),
            report_invalidations: true,
        }));
        cache.set_eligibility(true, true);

        let s1 = next_postings_cache_source_id();
        let s2 = next_postings_cache_source_id();
        let m_up = [LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()];

        let r1 = cache.get_or_compute_for_source::<_, ()>(s1, &m_up, true, || Ok(vec![101])).unwrap();
        let r2 = cache.get_or_compute_for_source::<_, ()>(s2, &m_up, true, || Ok(vec![202])).unwrap();
        assert_eq!(&*r1, &[101]);
        assert_eq!(&*r2, &[202]);
        // Shared max_bytes (180B) only fits one entry, so inserting s2 evicted s1.
        assert_eq!(sample(&reg, "f2_postings_for_matchers_cache_entries_total", None), 1.0);
        assert_eq!(sample(&reg, "f2_postings_for_matchers_cache_evictions_total", Some("max-bytes-reached")), 1.0);

        // With larger budget, both coexist without collision and invalidate independently.
        let cache2 = Arc::new(PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            ttl: Duration::from_secs(60),
            max_items: usize::MAX,
            max_bytes: 1024 * 1024,
            enable_invalidation: true,
            num_metric_versions: 1024,
            metrics: None,
            report_invalidations: false,
        }));
        cache2.set_eligibility(true, true);
        let c1 = AtomicUsize::new(0);
        let c2 = AtomicUsize::new(0);
        let a1 = cache2.get_or_compute_for_source::<_, ()>(s1, &m_up, true, || {
            c1.fetch_add(1, Ordering::Relaxed);
            Ok(vec![1])
        }).unwrap();
        let b1 = cache2.get_or_compute_for_source::<_, ()>(s2, &m_up, true, || {
            c2.fetch_add(1, Ordering::Relaxed);
            Ok(vec![2])
        }).unwrap();
        assert_eq!(&*a1, &[1]);
        assert_eq!(&*b1, &[2]);

        // Invalidating s1 "up" does NOT invalidate s2 "up".
        cache2.invalidate_metric_for_source(s1, "up");
        let a2 = cache2.get_or_compute_for_source::<_, ()>(s1, &m_up, true, || {
            c1.fetch_add(1, Ordering::Relaxed);
            Ok(vec![1, 11])
        }).unwrap();
        let b2 = cache2.get_or_compute_for_source::<_, ()>(s2, &m_up, true, || {
            c2.fetch_add(1, Ordering::Relaxed);
            Ok(vec![999])
        }).unwrap();
        assert_eq!(&*a2, &[1, 11]);
        assert_eq!(&*b2, &[2]);
        assert_eq!(c1.load(Ordering::Relaxed), 2);
        assert_eq!(c2.load(Ordering::Relaxed), 1);

        // invalidate_source(s1) leaves s2 cached.
        cache2.invalidate_source(s1);
        let b3 = cache2.get_or_compute_for_source::<_, ()>(s2, &m_up, true, || {
            c2.fetch_add(1, Ordering::Relaxed);
            Ok(vec![999])
        }).unwrap();
        assert_eq!(&*b3, &[2]);
        assert_eq!(c2.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn f3a_non_shared_cache_is_purely_ttl_based() {
        let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            ttl: Duration::from_secs(60),
            enable_invalidation: true,
            num_metric_versions: 64,
            ..Default::default()
        });
        cache.set_eligibility(true, false);
        assert!(!cache.invalidation_active());
        let m_up = [LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()];
        let m_job = [LabelMatcher::new(MatchType::Equal, "job", "api").unwrap()];
        let calls = AtomicUsize::new(0);
        cache.get_or_compute::<_, ()>(&m_up, true, || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec![1])
        }).unwrap();
        cache.get_or_compute::<_, ()>(&m_job, true, || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec![2])
        }).unwrap();
        cache.invalidate_metric("up");
        cache.invalidate_metric("other");
        assert_eq!(&*cache.get_or_compute::<_, ()>(&m_up, true, || { calls.fetch_add(1, Ordering::Relaxed); Ok(vec![9]) }).unwrap(), &[1]);
        assert_eq!(&*cache.get_or_compute::<_, ()>(&m_job, true, || { calls.fetch_add(1, Ordering::Relaxed); Ok(vec![9]) }).unwrap(), &[2]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn f3b_ttl_zero_and_empty_matchers_coalesce_in_flight() {
        let reg = Registry::new();
        let metrics = PostingsForMatchersCacheMetrics::new("f3b_", Some(&reg));
        let cache = Arc::new(PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            ttl: Duration::ZERO,
            max_items: usize::MAX,
            max_bytes: 1024 * 1024,
            enable_invalidation: false,
            metrics: Some(metrics),
            ..Default::default()
        }));
        cache.set_eligibility(true, false);

        let leader_entered = Arc::new(Barrier::new(2));
        let release_leader = Arc::new(Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));

        let c1 = Arc::clone(&cache);
        let le1 = Arc::clone(&leader_entered);
        let rl1 = Arc::clone(&release_leader);
        let calls1 = Arc::clone(&calls);
        let h1 = std::thread::spawn(move || {
            c1.get_or_compute::<_, ()>(&[], true, || {
                calls1.fetch_add(1, Ordering::Relaxed);
                le1.wait();
                rl1.wait();
                Ok(vec![7, 8])
            }).unwrap()
        });

        leader_entered.wait();
        let c2 = Arc::clone(&cache);
        let calls2 = Arc::clone(&calls);
        let h2 = std::thread::spawn(move || {
            c2.get_or_compute::<_, ()>(&[], true, || {
                calls2.fetch_add(1, Ordering::Relaxed);
                Ok(vec![999])
            }).unwrap()
        });
        // Give waiter a moment to register on in_flight before releasing leader.
        std::thread::sleep(Duration::from_millis(25));
        release_leader.wait();

        assert_eq!(&*h1.join().unwrap(), &[7, 8]);
        assert_eq!(&*h2.join().unwrap(), &[7, 8]);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(sample(&reg, "f3b_postings_for_matchers_cache_misses_total", None), 1.0);
        assert_eq!(sample(&reg, "f3b_postings_for_matchers_cache_hits_total", None), 1.0);
        assert_eq!(sample(&reg, "f3b_postings_for_matchers_cache_entries_total", None), 0.0);

        // Because ttl == 0, completed entry was deleted from in_flight, so next call misses again.
        let r3 = cache.get_or_compute::<_, ()>(&[], true, || {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(vec![9])
        }).unwrap();
        assert_eq!(&*r3, &[9]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(sample(&reg, "f3b_postings_for_matchers_cache_misses_total", None), 2.0);
    }

    #[test]
    fn f3c_exact_metric_name_matches_go_first_equal_matcher() {
        let empty_name = [LabelMatcher::new(MatchType::Equal, "__name__", "").unwrap()];
        assert_eq!(exact_metric_name_from_matchers(&empty_name), Some(""));

        let dup_names = [
            LabelMatcher::new(MatchType::Equal, "__name__", "first").unwrap(),
            LabelMatcher::new(MatchType::Equal, "__name__", "second").unwrap(),
        ];
        assert_eq!(exact_metric_name_from_matchers(&dup_names), Some("first"));

        let empty_then_named = [
            LabelMatcher::new(MatchType::Equal, "__name__", "").unwrap(),
            LabelMatcher::new(MatchType::Equal, "__name__", "second").unwrap(),
        ];
        assert_eq!(exact_metric_name_from_matchers(&empty_then_named), Some(""));

        let regex_only = [LabelMatcher::new(MatchType::RegexMatch, "__name__", "up.*").unwrap()];
        assert_eq!(exact_metric_name_from_matchers(&regex_only), None);
    }

    #[test]
    fn f3d_read_side_expire_and_stale_cached_entry_skip() {
        let reg = Registry::new();
        let metrics = PostingsForMatchersCacheMetrics::new("f3d_", Some(&reg));
        let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            ttl: Duration::from_millis(30),
            max_items: 100,
            max_bytes: 1024 * 1024,
            enable_invalidation: false,
            metrics: Some(metrics),
            ..Default::default()
        });
        cache.set_eligibility(true, false);

        let m1 = [LabelMatcher::new(MatchType::Equal, "__name__", "m1").unwrap()];
        let m2 = [LabelMatcher::new(MatchType::Equal, "__name__", "m2").unwrap()];
        cache.get_or_compute::<_, ()>(&m1, true, || Ok(vec![1])).unwrap();
        assert_eq!(sample(&reg, "f3d_postings_for_matchers_cache_entries_total", None), 1.0);

        std::thread::sleep(Duration::from_millis(45));
        // Even an ineligible read does not expire, whereas an eligible read for m2 runs expire()
        // and evicts expired m1 first, or if expire_in_progress is contended, hitting expired m1
        // increments skips_total{reason="stale-cached-entry"}.
        cache.expire_in_progress.store(true, Ordering::Release);
        let res = cache.get_or_compute::<_, ()>(&m1, true, || Ok(vec![2])).unwrap();
        assert_eq!(&*res, &[2]);
        assert_eq!(sample(&reg, "f3d_postings_for_matchers_cache_skips_total", Some("stale-cached-entry")), 1.0);
        cache.expire_in_progress.store(false, Ordering::Release);

        // Now expire() runs on eligible read and evicts m1.
        cache.get_or_compute::<_, ()>(&m2, false, || Ok(vec![3])).unwrap(); // skipped ineligible -> no expire
        assert_eq!(sample(&reg, "f3d_postings_for_matchers_cache_entries_total", None), 1.0);
        cache.get_or_compute::<_, ()>(&m2, true, || Ok(vec![3])).unwrap(); // runs expire(), evicts m1, inserts m2
        assert_eq!(sample(&reg, "f3d_postings_for_matchers_cache_evictions_total", Some("ttl-expired")), 1.0);
        assert_eq!(sample(&reg, "f3d_postings_for_matchers_cache_entries_total", None), 1.0);
    }

    #[test]
    fn remove_source_prunes_epoch_and_clears_has_source_epochs_when_empty() {
        let cache = PostingsForMatchersCache::new(PostingsForMatchersCacheConfig {
            ttl: Duration::from_secs(60),
            enable_invalidation: true,
            num_metric_versions: 64,
            ..Default::default()
        });
        cache.set_eligibility(true, true);
        assert!(!cache.has_source_epochs());

        let s1 = next_postings_cache_source_id();
        let s2 = next_postings_cache_source_id();
        let m_up = [LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap()];
        cache.get_or_compute_for_source::<_, ()>(s1, &m_up, true, || Ok(vec![1])).unwrap();
        cache.get_or_compute_for_source::<_, ()>(s2, &m_up, true, || Ok(vec![2])).unwrap();

        cache.invalidate_source(s1);
        assert!(cache.has_source_epochs());
        assert_eq!(cache.source_epochs.read().len(), 1);

        // Removing an unrelated source leaves s1's epoch intact.
        cache.remove_source(s2);
        assert!(cache.has_source_epochs());
        assert_eq!(cache.source_epochs.read().len(), 1);

        // Removing s1 clears source_epochs and resets has_source_epochs to false.
        cache.remove_source(s1);
        assert!(!cache.has_source_epochs());
        assert!(cache.source_epochs.read().is_empty());
    }
}