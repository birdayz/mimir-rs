//! Tenant-fair compute worker pool.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/util/workerpool/pool.go
//! - github.com/grafana/mimir/pkg/util/workerpool/pool_test.go (tests below)
//! - github.com/grafana/mimir/pkg/queue/max_queue_length.go (`MaxQueueLengthGauge`)
//! - dequeue order of github.com/grafana/mimir/pkg/queue (tree queue: root level selects the
//!   queue dimension by `workerID % len(dimensions)`, second level round-robins tenants starting
//!   after the worker's last served tenant index).
//!
//! Tasks run on dedicated OS threads (Go: worker goroutines), so CPU-bound work does not occupy
//! async runtime workers. Tasks are pulled from per-tenant, per-dimension FIFO queues so one tenant
//! flooding the pool cannot starve another tenant's tasks. The Go queue is uncapped
//! (`math.MaxInt32` per tenant), so `mimir_workerpool_discarded_requests_total` never increments.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{CounterVec, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts, Registry};
use tracing::error;

/// Go: `workerpool.ErrPoolStopped`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrPoolStopped;

impl std::fmt::Display for ErrPoolStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker pool is stopped")
    }
}

impl std::error::Error for ErrPoolStopped {}

type TaskFn = Box<dyn FnOnce() + Send + 'static>;

struct Task {
    dimension: String,
    tenant: String,
    f: TaskFn,
}

#[derive(Default)]
struct QueueState {
    stopped: bool,
    /// Dimension order (Go: `ConsumerWorkerQueuePriorityAlgo.nodeOrder`).
    dim_order: Vec<String>,
    /// Per dimension: per tenant FIFO.
    dims: HashMap<String, HashMap<String, VecDeque<Task>>>,
    /// Tenant slots (Go: `tenantIDOrder`); `None` marks a free slot.
    tenant_order: Vec<Option<String>>,
    tenant_slot: HashMap<String, usize>,
    tenant_items: HashMap<String, usize>,
    total: usize,
}

impl QueueState {
    fn enqueue(&mut self, task: Task) {
        if !self.tenant_slot.contains_key(&task.tenant) {
            let slot = match self.tenant_order.iter().position(Option::is_none) {
                Some(i) => {
                    self.tenant_order[i] = Some(task.tenant.clone());
                    i
                }
                None => {
                    self.tenant_order.push(Some(task.tenant.clone()));
                    self.tenant_order.len() - 1
                }
            };
            self.tenant_slot.insert(task.tenant.clone(), slot);
        }
        *self.tenant_items.entry(task.tenant.clone()).or_default() += 1;
        if !self.dims.contains_key(&task.dimension) {
            self.dim_order.push(task.dimension.clone());
        }
        self.dims
            .entry(task.dimension.clone())
            .or_default()
            .entry(task.tenant.clone())
            .or_default()
            .push_back(task);
        self.total += 1;
    }

    /// Dequeues the next task for `worker_id`, given the worker's last served tenant slot.
    fn dequeue(&mut self, worker_id: usize, last_tenant_idx: &mut Option<usize>) -> Option<Task> {
        if self.total == 0 || self.dim_order.is_empty() {
            return None;
        }
        let dim_idx = worker_id % self.dim_order.len();
        let dim_name = self.dim_order[dim_idx].clone();
        let dim = self.dims.get_mut(&dim_name)?;

        let n = self.tenant_order.len();
        let start = last_tenant_idx.map_or(0, |i| i + 1);
        let mut chosen = None;
        for k in 0..n {
            let idx = (start + k) % n;
            if let Some(ref t) = self.tenant_order[idx] {
                if dim.get(t).is_some_and(|q| !q.is_empty()) {
                    chosen = Some((idx, t.clone()));
                    break;
                }
            }
        }
        let (idx, tenant) = chosen?;
        *last_tenant_idx = Some(idx);

        let q = dim.get_mut(&tenant)?;
        let task = q.pop_front()?;
        if q.is_empty() {
            dim.remove(&tenant);
        }
        if dim.is_empty() {
            self.dims.remove(&dim_name);
            self.dim_order.remove(dim_idx);
        }
        self.total -= 1;
        let remaining = self.tenant_items.get_mut(&tenant).map(|c| {
            *c -= 1;
            *c
        });
        if remaining == Some(0) {
            self.tenant_items.remove(&tenant);
            if let Some(slot) = self.tenant_slot.remove(&tenant) {
                self.tenant_order[slot] = None;
            }
            while matches!(self.tenant_order.last(), Some(None)) {
                self.tenant_order.pop();
            }
        }
        Some(task)
    }
}

struct Shared {
    state: Mutex<QueueState>,
    cv: Condvar,
}

#[derive(Default)]
struct TenantDepth {
    current: usize,
    max: usize,
}

/// Go: `queue.MaxQueueLengthGauge`.
pub struct MaxQueueLengthGauge {
    desc: Desc,
    tenants: Mutex<HashMap<String, TenantDepth>>,
}

impl MaxQueueLengthGauge {
    pub fn new(name: &str, help: &str, const_labels: HashMap<String, String>) -> Self {
        Self {
            desc: Desc::new(
                name.to_string(),
                help.to_string(),
                vec!["user".to_string()],
                const_labels,
            )
            .expect("valid max queue length desc"),
            tenants: Mutex::new(HashMap::new()),
        }
    }

    pub fn inc(&self, tenant: &str) {
        let mut t = self.tenants.lock();
        let d = t.entry(tenant.to_string()).or_default();
        d.current += 1;
        if d.current > d.max {
            d.max = d.current;
        }
    }

    pub fn dec(&self, tenant: &str) {
        let mut t = self.tenants.lock();
        if let Some(d) = t.get_mut(tenant) {
            if d.current > 0 {
                d.current -= 1;
            }
        }
    }

    pub fn delete_tenant(&self, tenant: &str) {
        self.tenants.lock().remove(tenant);
    }
}

impl Collector for MaxQueueLengthGauge {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let peaks: Vec<(String, usize)> = {
            let mut t = self.tenants.lock();
            t.iter_mut()
                .map(|(k, d)| {
                    let p = (k.clone(), d.max);
                    d.max = d.current;
                    p
                })
                .collect()
        };
        if peaks.is_empty() {
            return Vec::new();
        }
        let mut metrics = Vec::with_capacity(peaks.len());
        for (tenant, max) in peaks {
            let mut g = prometheus::proto::Gauge::default();
            g.set_value(max as f64);
            let mut m = prometheus::proto::Metric::default();
            m.set_gauge(g);
            let mut labels = self.desc.const_label_pairs.to_vec();
            let mut lp = prometheus::proto::LabelPair::default();
            lp.set_name("user".to_string());
            lp.set_value(tenant);
            labels.push(lp);
            labels.sort_by(|a, b| a.get_name().cmp(b.get_name()));
            m.set_label(labels.into());
            metrics.push(m);
        }
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(prometheus::proto::MetricType::GAUGE);
        mf.set_metric(metrics.into());
        vec![mf]
    }
}

struct PoolMetrics {
    queue_length: GaugeVec,
    max_queue_length: Arc<MaxQueueLengthGauge>,
    discarded: CounterVec,
    enqueue_duration: Histogram,
    task_duration: HistogramVec,
}

/// Go: `workerpool.Pool`.
pub struct Pool {
    name: String,
    workers: usize,
    shared: Arc<Shared>,
    metrics: Arc<PoolMetrics>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

/// Go: `runtime.GOMAXPROCS(0)` default (cgroup-aware CPU count), clamped to `[1, 8]`
/// so unconstrained 24+ core hosts do not spawn excessive idle worker threads.
fn default_workers() -> usize {
    std::thread::available_parallelism().map_or(4, |n| n.get().clamp(1, 8))
}

impl Pool {
    /// Go: `workerpool.New(Config{Size: size}, name, reg, logger)`. `name` becomes the `pool`
    /// const label. Workers start with [`Pool::start`].
    pub fn new(size: i64, name: &str, reg: Option<&Registry>) -> Result<Arc<Self>, String> {
        if size < 0 {
            return Err("worker pool size must be >= 0".to_string());
        }
        let workers = if size == 0 { default_workers() } else { size as usize };
        let const_labels: HashMap<String, String> =
            [("pool".to_string(), name.to_string())].into_iter().collect();

        let queue_length = GaugeVec::new(
            Opts::new(
                "mimir_workerpool_queue_length",
                "Number of work items queued in the worker pool, per tenant.",
            )
            .const_labels(const_labels.clone()),
            &["user"],
        )
        .map_err(|e| e.to_string())?;
        let max_queue_length = Arc::new(MaxQueueLengthGauge::new(
            "mimir_workerpool_max_queue_length",
            "Maximum number of work items observed in a tenant's queue since the last metric collection (reset on each scrape). Captures the true peak queue depth between scrapes.",
            const_labels.clone(),
        ));
        let discarded = CounterVec::new(
            Opts::new(
                "mimir_workerpool_discarded_requests_total",
                "Total number of work items discarded by the worker pool because the tenant's queue was full.",
            )
            .const_labels(const_labels.clone()),
            &["user"],
        )
        .map_err(|e| e.to_string())?;
        // Go declares these as native-only histograms (no classic buckets): the classic exposition
        // only carries the +Inf bucket, sum and count.
        let enqueue_duration = Histogram::with_opts(
            HistogramOpts::new(
                "mimir_workerpool_enqueue_duration_seconds",
                "Time spent enqueueing work items into the worker pool.",
            )
            .const_labels(const_labels.clone())
            .buckets(vec![f64::INFINITY]),
        )
        .map_err(|e| e.to_string())?;
        let task_duration = HistogramVec::new(
            HistogramOpts::new(
                "mimir_workerpool_task_duration_seconds",
                "Time taken to execute work items in the worker pool, per task dimension.",
            )
            .const_labels(const_labels)
            .buckets(vec![f64::INFINITY]),
            &["dimension"],
        )
        .map_err(|e| e.to_string())?;

        if let Some(r) = reg {
            r.register(Box::new(queue_length.clone()))
                .map_err(|e| e.to_string())?;
            r.register(Box::new(ArcCollector(Arc::clone(&max_queue_length))))
                .map_err(|e| e.to_string())?;
            r.register(Box::new(discarded.clone()))
                .map_err(|e| e.to_string())?;
            r.register(Box::new(enqueue_duration.clone()))
                .map_err(|e| e.to_string())?;
            r.register(Box::new(task_duration.clone()))
                .map_err(|e| e.to_string())?;
        }

        Ok(Arc::new(Self {
            name: name.to_string(),
            workers,
            shared: Arc::new(Shared {
                state: Mutex::new(QueueState::default()),
                cv: Condvar::new(),
            }),
            metrics: Arc::new(PoolMetrics {
                queue_length,
                max_queue_length,
                discarded,
                enqueue_duration,
                task_duration,
            }),
            handles: Mutex::new(Vec::new()),
        }))
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Go: `starting()`: launches every worker. Idempotent.
    pub fn start(&self) -> Result<(), String> {
        let mut handles = self.handles.lock();
        if !handles.is_empty() {
            return Ok(());
        }
        if self.shared.state.lock().stopped {
            return Err(ErrPoolStopped.to_string());
        }
        for worker_id in 0..self.workers {
            let shared = Arc::clone(&self.shared);
            let metrics = Arc::clone(&self.metrics);
            let h = std::thread::Builder::new()
                .name(format!("{}-{worker_id}", self.name))
                .spawn(move || worker_loop(worker_id, &shared, &metrics))
                .map_err(|e| format!("registering worker pool worker: {e}"))?;
            handles.push(h);
        }
        Ok(())
    }

    /// Go: `stopping()`: stops the queue (queued tasks are still drained) and waits for workers.
    /// Blocking; call from a blocking context.
    pub fn stop(&self) {
        {
            let mut st = self.shared.state.lock();
            st.stopped = true;
        }
        self.shared.cv.notify_all();
        let handles = std::mem::take(&mut *self.handles.lock());
        for h in handles {
            if h.join().is_err() {
                error!(pool = %self.name, "worker pool worker panicked");
            }
        }
    }

    /// Go: `Submit(dimension, tenantID, fn)`.
    pub fn submit(
        &self,
        dimension: &str,
        tenant_id: &str,
        f: impl FnOnce() + Send + 'static,
    ) -> Result<(), ErrPoolStopped> {
        let start = Instant::now();
        let res = {
            let mut st = self.shared.state.lock();
            if st.stopped {
                Err(ErrPoolStopped)
            } else {
                st.enqueue(Task {
                    dimension: dimension.to_string(),
                    tenant: tenant_id.to_string(),
                    f: Box::new(f),
                });
                self.metrics
                    .queue_length
                    .with_label_values(&[tenant_id])
                    .inc();
                self.metrics.max_queue_length.inc(tenant_id);
                Ok(())
            }
        };
        if res.is_ok() {
            self.shared.cv.notify_one();
        }
        self.metrics
            .enqueue_duration
            .observe(start.elapsed().as_secs_f64());
        res
    }

    /// Go: `RemoveTenant(tenantID)`: deletes the per-tenant metric series.
    pub fn remove_tenant(&self, tenant_id: &str) {
        let _ = self.metrics.queue_length.remove_label_values(&[tenant_id]);
        self.metrics.max_queue_length.delete_tenant(tenant_id);
        let _ = self.metrics.discarded.remove_label_values(&[tenant_id]);
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shared.state.lock().stopped = true;
        self.shared.cv.notify_all();
    }
}

struct ArcCollector(Arc<MaxQueueLengthGauge>);

impl Collector for ArcCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.0.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.0.collect()
    }
}

fn worker_loop(worker_id: usize, shared: &Shared, metrics: &PoolMetrics) {
    let mut last_tenant_idx: Option<usize> = None;
    loop {
        let task = {
            let mut st = shared.state.lock();
            loop {
                if let Some(t) = st.dequeue(worker_id, &mut last_tenant_idx) {
                    break t;
                }
                if st.stopped {
                    return;
                }
                shared.cv.wait(&mut st);
            }
        };
        metrics
            .queue_length
            .with_label_values(&[&task.tenant])
            .dec();
        metrics.max_queue_length.dec(&task.tenant);
        let start = Instant::now();
        // A panicking task is not recovered in Go: it crashes the process.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(task.f)).is_err() {
            std::process::abort();
        }
        metrics
            .task_duration
            .with_label_values(&[&task.dimension])
            .observe(start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    fn start_pool(size: i64) -> Arc<Pool> {
        let p = Pool::new(size, "test", None).unwrap();
        p.start().unwrap();
        p
    }

    fn eventually(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if f() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        f()
    }

    // Upstream: pool_test.go::TestPool_RunsTasksConcurrently
    #[test]
    fn runs_tasks_concurrently() {
        const WORKERS: usize = 4;
        let p = start_pool(WORKERS as i64);
        let inflight = Arc::new(AtomicI64::new(0));
        let max_inflight = Arc::new(AtomicI64::new(0));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (done_tx, done_rx) = mpsc::channel();
        for _ in 0..WORKERS {
            let (inflight, max_inflight, release, done_tx) = (
                Arc::clone(&inflight),
                Arc::clone(&max_inflight),
                Arc::clone(&release),
                done_tx.clone(),
            );
            p.submit("test", "tenant-1", move || {
                let cur = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max_inflight.fetch_max(cur, Ordering::SeqCst);
                let (m, cv) = &*release;
                let mut r = m.lock();
                while !*r {
                    cv.wait(&mut r);
                }
                drop(r);
                inflight.fetch_sub(1, Ordering::SeqCst);
                done_tx.send(()).unwrap();
            })
            .unwrap();
        }
        assert!(eventually(
            || inflight.load(Ordering::SeqCst) == WORKERS as i64,
            Duration::from_secs(1)
        ));
        assert_eq!(max_inflight.load(Ordering::SeqCst), WORKERS as i64);
        *release.0.lock() = true;
        release.1.notify_all();
        for _ in 0..WORKERS {
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        p.stop();
    }

    // Upstream: pool_test.go::TestPool_TenantFairness
    #[test]
    fn tenant_fairness() {
        let p = start_pool(1);
        let (hog_tx, hog_rx) = mpsc::channel::<()>();
        let hog_rx = Arc::new(std::sync::Mutex::new(hog_rx));
        let hog_starts = Arc::new(AtomicI64::new(0));
        for _ in 0..8 {
            let (hog_rx, hog_starts) = (Arc::clone(&hog_rx), Arc::clone(&hog_starts));
            p.submit("test", "hog", move || {
                hog_starts.fetch_add(1, Ordering::SeqCst);
                let _ = hog_rx.lock().unwrap().recv();
            })
            .unwrap();
        }
        assert!(eventually(
            || hog_starts.load(Ordering::SeqCst) >= 1,
            Duration::from_secs(1)
        ));
        let (light_tx, light_rx) = mpsc::channel::<()>();
        p.submit("test", "light", move || {
            light_tx.send(()).unwrap();
        })
        .unwrap();
        hog_tx.send(()).unwrap();
        assert!(
            light_rx.recv_timeout(Duration::from_secs(1)).is_ok(),
            "light tenant's task was starved (hogStarts={})",
            hog_starts.load(Ordering::SeqCst)
        );
        // Close the hog channel so the remaining hog tasks complete.
        drop(hog_tx);
        p.stop();
    }

    // Upstream: pool_test.go::TestPool_StopDrainsInflightWork
    #[test]
    fn stop_drains_inflight_work() {
        let p = Pool::new(2, "drain", None).unwrap();
        p.start().unwrap();
        let ran = Arc::new(AtomicI64::new(0));
        for _ in 0..4 {
            let ran = Arc::clone(&ran);
            p.submit("test", "tenant", move || {
                std::thread::sleep(Duration::from_millis(50));
                ran.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        }
        p.stop();
        assert_eq!(ran.load(Ordering::SeqCst), 4);
    }

    // Upstream: pool_test.go::TestPool_SubmitAfterStopReturnsErrPoolStopped
    #[test]
    fn submit_after_stop_returns_err_pool_stopped() {
        let p = start_pool(1);
        p.stop();
        assert_eq!(p.submit("test", "tenant", || {}), Err(ErrPoolStopped));
        assert_eq!(ErrPoolStopped.to_string(), "worker pool is stopped");
    }

    // Upstream: pool_test.go::TestPool_StartingBlocksUntilWorkersAreRegistered
    #[test]
    fn starting_spawns_all_workers() {
        let p = start_pool(4);
        assert_eq!(p.handles.lock().len(), 4);
        p.stop();
    }

    // Upstream: pool_test.go::TestPool_RecordsTaskDuration
    #[test]
    fn records_task_duration() {
        let reg = Registry::new();
        let p = Pool::new(2, "test", Some(&reg)).unwrap();
        p.start().unwrap();
        let (tx, rx) = mpsc::channel();
        for _ in 0..3 {
            let tx = tx.clone();
            p.submit("mydim", "tenant", move || tx.send(()).unwrap())
                .unwrap();
        }
        for _ in 0..3 {
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        let count = || {
            reg.gather()
                .into_iter()
                .filter(|mf| mf.get_name() == "mimir_workerpool_task_duration_seconds")
                .flat_map(|mf| mf.get_metric().to_vec())
                .find(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.get_name() == "dimension" && l.get_value() == "mydim")
                })
                .map_or(0, |m| m.get_histogram().get_sample_count())
        };
        assert!(eventually(|| count() == 3, Duration::from_secs(1)));
        p.stop();
    }

    // Upstream: pool_test.go::TestPool_RemoveTenant
    #[test]
    fn remove_tenant() {
        let p = Pool::new(1, "test", Some(&Registry::new())).unwrap();
        p.metrics.queue_length.with_label_values(&["tenant-a"]).set(2.0);
        p.metrics.discarded.with_label_values(&["tenant-a"]).inc();
        p.metrics.queue_length.with_label_values(&["tenant-b"]).set(1.0);
        p.metrics.discarded.with_label_values(&["tenant-b"]).inc();
        let count = |c: &dyn Collector| c.collect().iter().map(|mf| mf.get_metric().len()).sum::<usize>();
        assert_eq!(count(&p.metrics.queue_length), 2);
        assert_eq!(count(&p.metrics.discarded), 2);
        p.remove_tenant("tenant-a");
        assert_eq!(count(&p.metrics.queue_length), 1);
        assert_eq!(count(&p.metrics.discarded), 1);
    }

    // Upstream: pool_test.go::TestConfig_Validate (+ New's GOMAXPROCS default)
    #[test]
    fn config_validate() {
        assert!(Pool::new(0, "test", None).is_ok());
        assert!(Pool::new(4, "test", None).is_ok());
        assert_eq!(
            Pool::new(-1, "test", None).err().unwrap(),
            "worker pool size must be >= 0"
        );
        assert_eq!(Pool::new(0, "test", None).unwrap().workers(), default_workers());
    }

    // Exposition: metric names, the `pool` const label and the `user` label, plus the
    // max-queue-length reset-on-scrape behaviour of queue.MaxQueueLengthGauge.
    #[test]
    fn metrics_exposition() {
        let reg = Registry::new();
        let p = Pool::new(1, "ingester-compute", Some(&reg)).unwrap();
        let (block_tx, block_rx) = mpsc::channel::<()>();
        let (started_tx, started_rx) = mpsc::channel::<()>();
        p.submit("label-values-series-count", "user-1", move || {
            started_tx.send(()).unwrap();
            let _ = block_rx.recv();
        })
        .unwrap();
        p.submit("label-values-series-count", "user-1", || {}).unwrap();
        p.submit("label-values-series-count", "user-1", || {}).unwrap();
        p.start().unwrap();
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let find = |name: &str| reg.gather().into_iter().find(|mf| mf.get_name() == name);
        // A single scrape: gathering resets the max-queue-length peaks.
        let first = reg.gather();
        let in_first = |name: &str| first.iter().find(|mf| mf.get_name() == name).unwrap();
        let ql = in_first("mimir_workerpool_queue_length");
        let m = &ql.get_metric()[0];
        let labels: Vec<(String, String)> = m
            .get_label()
            .iter()
            .map(|l| (l.get_name().to_string(), l.get_value().to_string()))
            .collect();
        assert_eq!(
            labels,
            vec![
                ("pool".to_string(), "ingester-compute".to_string()),
                ("user".to_string(), "user-1".to_string())
            ]
        );
        assert_eq!(m.get_gauge().get_value(), 2.0);
        let mql = in_first("mimir_workerpool_max_queue_length");
        assert_eq!(mql.get_metric()[0].get_gauge().get_value(), 3.0);
        let mql = find("mimir_workerpool_max_queue_length").unwrap();
        assert_eq!(mql.get_metric()[0].get_gauge().get_value(), 2.0);

        block_tx.send(()).unwrap();
        p.stop();

        let td = find("mimir_workerpool_task_duration_seconds").unwrap();
        assert_eq!(td.get_metric()[0].get_histogram().get_sample_count(), 3);
        assert!(td.get_metric()[0].get_histogram().get_bucket().is_empty());
        let ed = find("mimir_workerpool_enqueue_duration_seconds").unwrap();
        assert_eq!(ed.get_metric()[0].get_histogram().get_sample_count(), 3);
        assert!(find("mimir_workerpool_discarded_requests_total").is_none());

        p.remove_tenant("user-1");
        assert!(find("mimir_workerpool_queue_length").is_none());
        assert!(find("mimir_workerpool_max_queue_length").is_none());
    }

    #[test]
    fn dimensions_spread_across_workers() {
        let mut st = QueueState::default();
        for d in ["d1", "d2"] {
            for _ in 0..2 {
                st.enqueue(Task {
                    dimension: d.to_string(),
                    tenant: "t".to_string(),
                    f: Box::new(|| {}),
                });
            }
        }
        let (mut l0, mut l1) = (None, None);
        assert_eq!(st.dequeue(0, &mut l0).unwrap().dimension, "d1");
        assert_eq!(st.dequeue(1, &mut l1).unwrap().dimension, "d2");
    }

    #[test]
    fn default_workers_is_bounded() {
        let w = default_workers();
        assert!((1..=8).contains(&w), "default_workers={w} out of [1, 8]");
    }
}
