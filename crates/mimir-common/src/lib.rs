//! Shared primitives: multi-tenancy, label hashing, service lifecycle, logging, and limiters.
//!
//! Ported from:
//! - github.com/grafana/dskit/services
//! - github.com/grafana/dskit/tenant
//! - github.com/grafana/dskit/user
//! - github.com/grafana/dskit/backoff
//! - github.com/grafana/dskit/limiter
//! - github.com/grafana/dskit/crypto/tls
//! - github.com/grafana/dskit/netutil
//! - github.com/hashicorp/go-sockaddr (GetPrivateIP)
//! - Go stdlib sort (sort.Sort / pdqsort)
//! - github.com/grafana/mimir/pkg/util/log
//! - github.com/grafana/mimir/pkg/util/globalerror
//! - github.com/grafana/mimir/pkg/util/activitytracker
//! - github.com/grafana/mimir/pkg/util/shutdownmarker
//! - github.com/grafana/mimir/pkg/util/reactivelimiter
//! - github.com/grafana/mimir/pkg/distributor/validate.go
//! - github.com/prometheus/prometheus/model/labels
//! - github.com/prometheus/client_golang/prometheus/histogram.go (native histograms)

pub mod activitytracker;
pub mod atomicfs;
pub mod backoff;
pub mod compartments;
pub mod concurrency;
pub mod errors;
pub mod instrument;
pub mod multierror;
pub mod gosort;
pub mod grpc_deadline;
pub mod grpc_log;
pub mod labels_hash;
pub mod limiter;
pub mod netutil;
pub mod logging;
pub mod native_histogram;
pub mod services;
pub mod shutdownmarker;
pub mod sockaddr;
pub mod tenant;
pub mod thread_pool;
pub mod timeutil;
pub mod tls;
pub mod tracing;
pub mod usagestats;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Snapshot of jemalloc allocator statistics read via `mallctl` on scrape.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JemallocStats {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub mapped: usize,
    pub retained: usize,
    pub metadata: usize,
}

/// Snapshot of process-wide off-heap `NanosSlab` anonymous mmap usage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NanosSlabStats {
    pub mapped_bytes: usize,
    pub pages: usize,
    pub reclaimed_pages_total: u64,
}

/// Snapshot of process-wide `ScopedWorkerPool` OS threads and fork-join executions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScopedWorkerPoolStats {
    pub threads: usize,
    pub executions_total: u64,
}

static ALLOCATOR_PURGE_HOOK: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();
static JEMALLOC_STATS_HOOK: std::sync::OnceLock<fn() -> Option<JemallocStats>> =
    std::sync::OnceLock::new();
static ALLOCATOR_PURGES_TOTAL: AtomicU64 = AtomicU64::new(0);
static NANOS_SLAB_MAPPED_BYTES: AtomicUsize = AtomicUsize::new(0);
static NANOS_SLAB_PAGES: AtomicUsize = AtomicUsize::new(0);
static NANOS_SLAB_RECLAIMED_PAGES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SCOPED_WORKER_POOL_THREADS: AtomicUsize = AtomicUsize::new(0);
static SCOPED_WORKER_POOL_EXECUTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Registers a process-global allocator arena purge callback (e.g. jemalloc `arena.4096.purge`).
pub fn register_allocator_purge_hook(hook: fn()) {
    let _ = ALLOCATOR_PURGE_HOOK.set(hook);
}

/// Returns unused dirty/muzzy pages across allocator arenas back to the OS when a hook is registered.
pub fn purge_allocator_arenas() {
    if let Some(hook) = ALLOCATOR_PURGE_HOOK.get() {
        hook();
        ALLOCATOR_PURGES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

/// Total number of explicit allocator arena purges performed via [`purge_allocator_arenas`].
pub fn allocator_arena_purges_total() -> u64 {
    ALLOCATOR_PURGES_TOTAL.load(Ordering::Relaxed)
}

/// Registers a process-global jemalloc stats reader callback (invoked on `/metrics` scrape).
pub fn register_jemalloc_stats_hook(hook: fn() -> Option<JemallocStats>) {
    let _ = JEMALLOC_STATS_HOOK.set(hook);
}

/// Reads current jemalloc allocator stats if a reader hook is registered and succeeds.
pub fn read_jemalloc_stats() -> Option<JemallocStats> {
    JEMALLOC_STATS_HOOK.get().and_then(|hook| hook())
}

/// Records allocation of one off-heap `NanosSlab` page of `bytes` size.
pub fn record_nanos_slab_page_alloc(bytes: usize) {
    NANOS_SLAB_MAPPED_BYTES.fetch_add(bytes, Ordering::Relaxed);
    NANOS_SLAB_PAGES.fetch_add(1, Ordering::Relaxed);
}

/// Records deallocation of one off-heap `NanosSlab` page of `bytes` size.
pub fn record_nanos_slab_page_free(bytes: usize) {
    NANOS_SLAB_MAPPED_BYTES.fetch_sub(bytes, Ordering::Relaxed);
    NANOS_SLAB_PAGES.fetch_sub(1, Ordering::Relaxed);
}

/// Records physical reclamation of one `NanosSlab` page via `madvise(MADV_DONTNEED)`.
pub fn record_nanos_slab_page_reclaimed() {
    NANOS_SLAB_RECLAIMED_PAGES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Reads process-wide off-heap `NanosSlab` statistics.
pub fn read_nanos_slab_stats() -> NanosSlabStats {
    NanosSlabStats {
        mapped_bytes: NANOS_SLAB_MAPPED_BYTES.load(Ordering::Relaxed),
        pages: NANOS_SLAB_PAGES.load(Ordering::Relaxed),
        reclaimed_pages_total: NANOS_SLAB_RECLAIMED_PAGES_TOTAL.load(Ordering::Relaxed),
    }
}

/// Records spawned persistent worker threads in a `ScopedWorkerPool`.
pub fn record_scoped_worker_pool_threads_added(threads: usize) {
    SCOPED_WORKER_POOL_THREADS.fetch_add(threads, Ordering::Relaxed);
}

/// Records stopped persistent worker threads in a `ScopedWorkerPool`.
pub fn record_scoped_worker_pool_threads_removed(threads: usize) {
    SCOPED_WORKER_POOL_THREADS.fetch_sub(threads, Ordering::Relaxed);
}

/// Records one fork-join batch dispatched on a `ScopedWorkerPool`.
pub fn record_scoped_worker_pool_execution() {
    SCOPED_WORKER_POOL_EXECUTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Reads process-wide `ScopedWorkerPool` statistics.
pub fn read_scoped_worker_pool_stats() -> ScopedWorkerPoolStats {
    ScopedWorkerPoolStats {
        threads: SCOPED_WORKER_POOL_THREADS.load(Ordering::Relaxed),
        executions_total: SCOPED_WORKER_POOL_EXECUTIONS_TOTAL.load(Ordering::Relaxed),
    }
}

#[doc(hidden)]
pub fn block_on_for_tests<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime for tests")
        .block_on(future)
}
