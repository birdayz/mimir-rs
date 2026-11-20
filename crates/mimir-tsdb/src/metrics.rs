//! Per-tenant Prometheus TSDB metric helpers shared by the Head, the write logs and the DB.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/wlog/wlog.go (`newWLMetrics`),
//! github.com/prometheus/prometheus/tsdb/chunks/chunk_write_queue.go (`newChunkWriteQueue`
//! metrics) and the summary metrics of github.com/prometheus/prometheus/tsdb/head.go.
//! Mimir aggregates them from the per-tenant registry in
//! github.com/grafana/mimir/pkg/storage/tsdb/tsdb_metrics.go.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_common::native_histogram::{HistogramOpts as NativeHistogramOpts, SingleHistogram};
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{
    Gauge as ProtoGauge, Metric, MetricFamily, MetricType as PromMetricType, Quantile,
    Summary as ProtoSummary,
};
use prometheus::{Counter, CounterVec, Gauge, Opts, Registry};

/// Go `prometheus.DefMaxAge` (10 minutes).
const SUMMARY_MAX_AGE: Duration = Duration::from_secs(600);
/// Cap buffered samples per summary in the 10-minute window so heavy workloads stay bounded.
const SUMMARY_MAX_WINDOW_SAMPLES: usize = 8192;

struct SummaryState {
    count: u64,
    sum: f64,
    samples: VecDeque<(Instant, f64)>,
}

impl SummaryState {
    fn prune_expired(&mut self, now: Instant) {
        while let Some(&(ts, _)) = self.samples.front() {
            if now.saturating_duration_since(ts) > SUMMARY_MAX_AGE {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }
}

struct SummaryInner {
    desc: Desc,
    quantiles: Vec<f64>,
    state: Mutex<SummaryState>,
}

/// A Prometheus summary exposing `_count` / `_sum` (aggregated by Mimir's `SendSumOfSummaries`)
/// plus sliding-window quantiles over Go's `DefMaxAge` (10m) when objectives are configured.
#[derive(Clone)]
pub struct Summary(Arc<SummaryInner>);

impl Summary {
    pub fn new(name: &str, help: &str, quantiles: &[f64]) -> Self {
        let desc = Desc::new(name.to_string(), help.to_string(), vec![], HashMap::new())
            .expect("valid summary desc");
        Self(Arc::new(SummaryInner {
            desc,
            quantiles: quantiles.to_vec(),
            state: Mutex::new(SummaryState {
                count: 0,
                sum: 0.0,
                samples: VecDeque::new(),
            }),
        }))
    }

    pub fn observe(&self, v: f64) {
        let now = if self.0.quantiles.is_empty() {
            None
        } else {
            Some(Instant::now())
        };
        let mut g = self.0.state.lock();
        g.count += 1;
        g.sum += v;
        if let Some(now) = now {
            g.prune_expired(now);
            if g.samples.len() >= SUMMARY_MAX_WINDOW_SAMPLES {
                g.samples.pop_front();
            }
            g.samples.push_back((now, v));
        }
    }

    pub fn sample_count(&self) -> u64 {
        self.0.state.lock().count
    }
}

impl Collector for Summary {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.0.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let (count, sum, mut vals) = {
            let mut g = self.0.state.lock();
            if !self.0.quantiles.is_empty() {
                g.prune_expired(Instant::now());
            }
            let vals: Vec<f64> = if self.0.quantiles.is_empty() {
                Vec::new()
            } else {
                g.samples.iter().map(|&(_, v)| v).collect()
            };
            (g.count, g.sum, vals)
        };
        if !vals.is_empty() {
            vals.sort_unstable_by(f64::total_cmp);
        }

        let mut s = ProtoSummary::default();
        s.set_sample_count(count);
        s.set_sample_sum(sum);
        let qs: Vec<Quantile> = self
            .0
            .quantiles
            .iter()
            .map(|&qv| {
                let mut q = Quantile::default();
                q.set_quantile(qv);
                let val = if vals.is_empty() {
                    f64::NAN
                } else {
                    let idx = ((qv * (vals.len() - 1) as f64).round() as usize)
                        .min(vals.len() - 1);
                    vals[idx]
                };
                q.set_value(val);
                q
            })
            .collect();
        s.set_quantile(qs.into());
        let mut m = Metric::default();
        m.set_summary(s);
        let mut mf = MetricFamily::default();
        mf.set_name(self.0.desc.fq_name.clone());
        mf.set_help(self.0.desc.help.clone());
        mf.set_field_type(PromMetricType::SUMMARY);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

pub(crate) fn counter(reg: Option<&Registry>, name: &str, help: &str) -> Counter {
    let c = Counter::new(name, help).expect("valid counter");
    if let Some(r) = reg {
        let _ = r.register(Box::new(c.clone()));
    }
    c
}

pub(crate) fn gauge(reg: Option<&Registry>, name: &str, help: &str) -> Gauge {
    let g = Gauge::new(name, help).expect("valid gauge");
    if let Some(r) = reg {
        let _ = r.register(Box::new(g.clone()));
    }
    g
}

pub(crate) fn counter_vec(
    reg: Option<&Registry>,
    name: &str,
    help: &str,
    label: &str,
) -> CounterVec {
    let c = CounterVec::new(Opts::new(name, help), &[label]).expect("valid counter vec");
    if let Some(r) = reg {
        let _ = r.register(Box::new(c.clone()));
    }
    c
}

pub(crate) fn summary(
    reg: Option<&Registry>,
    name: &str,
    help: &str,
    quantiles: &[f64],
) -> Summary {
    let s = Summary::new(name, help, quantiles);
    if let Some(r) = reg {
        let _ = r.register(Box::new(s.clone()));
    }
    s
}

pub(crate) fn native_histogram(
    reg: Option<&Registry>,
    name: &str,
    help: &str,
    buckets: Option<Vec<f64>>,
) -> SingleHistogram {
    let mut opts = NativeHistogramOpts::new(name, help).native(
        1.1,
        100,
        Duration::from_secs(3600),
    );
    if let Some(b) = buckets {
        opts = opts.buckets(b);
    }
    let h = SingleHistogram::new(opts).expect("valid native histogram");
    if let Some(r) = reg {
        let _ = r.register(Box::new(h.clone()));
    }
    h
}

pub(crate) struct FnGaugeCollector {
    desc: Desc,
    f: Box<dyn Fn() -> f64 + Send + Sync>,
}

impl FnGaugeCollector {
    pub(crate) fn register(
        reg: Option<&Registry>,
        name: &str,
        help: &str,
        f: impl Fn() -> f64 + Send + Sync + 'static,
    ) {
        let Some(r) = reg else {
            return;
        };
        let Ok(desc) = Desc::new(name.to_string(), help.to_string(), Vec::new(), HashMap::new())
        else {
            return;
        };
        let _ = r.register(Box::new(Self {
            desc,
            f: Box::new(f),
        }));
    }
}

impl Collector for FnGaugeCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut g = ProtoGauge::default();
        g.set_value((self.f)());
        let mut m = Metric::default();
        m.set_gauge(g);
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(PromMetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

fn dir_size_bytes(root: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total = total.saturating_add(meta.len());
            }
        }
    }
    total
}

/// Dynamic gauge collector for `prometheus_tsdb_{wal,out_of_order_wbl}_storage_size_bytes`
/// (Go `wlog.go:266` -> `w.Size()`).
struct WlogSizeCollector {
    desc: Desc,
    dir: PathBuf,
}

impl Collector for WlogSizeCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut g = ProtoGauge::default();
        g.set_value(dir_size_bytes(&self.dir) as f64);
        let mut m = Metric::default();
        m.set_gauge(g);
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(PromMetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

/// Go `wlog.wlMetrics` for the WAL (`prometheus_tsdb_wal_` prefix). The WBL registers the
/// same set under `prometheus_tsdb_out_of_order_wbl_`.
#[derive(Clone)]
pub struct WlogMetrics {
    pub fsync_duration: Summary,
    pub page_flushes: Counter,
    pub page_completions: Counter,
    pub truncate_fail: Counter,
    pub truncate_total: Counter,
    pub current_segment: Gauge,
    pub writes_failed: Counter,
    pub record_part_writes: Counter,
    pub record_parts_bytes_written: Counter,
    pub record_bytes_saved: CounterVec,
}

impl WlogMetrics {
    pub fn new(reg: Option<&Registry>, prefix: &str) -> Self {
        Self::new_with_dir(reg, prefix, None)
    }

    pub fn new_with_dir(reg: Option<&Registry>, prefix: &str, dir: Option<&Path>) -> Self {
        let n = |s: &str| format!("{prefix}{s}");
        if let (Some(r), Some(d)) = (reg, dir) {
            if let Ok(desc) = Desc::new(
                n("storage_size_bytes"),
                "Size of the write log directory.".to_string(),
                Vec::new(),
                HashMap::new(),
            ) {
                let _ = r.register(Box::new(WlogSizeCollector {
                    desc,
                    dir: d.to_path_buf(),
                }));
            }
        }
        Self {
            fsync_duration: summary(
                reg,
                &n("fsync_duration_seconds"),
                "Duration of write log fsync.",
                &[0.5, 0.9, 0.99],
            ),
            page_flushes: counter(
                reg,
                &n("page_flushes_total"),
                "Total number of page flushes.",
            ),
            page_completions: counter(
                reg,
                &n("completed_pages_total"),
                "Total number of completed pages.",
            ),
            truncate_fail: counter(
                reg,
                &n("truncations_failed_total"),
                "Total number of write log truncations that failed.",
            ),
            truncate_total: counter(
                reg,
                &n("truncations_total"),
                "Total number of write log truncations attempted.",
            ),
            current_segment: gauge(
                reg,
                &n("segment_current"),
                "Write log segment index that TSDB is currently writing to.",
            ),
            writes_failed: counter(
                reg,
                &n("writes_failed_total"),
                "Total number of write log writes that failed.",
            ),
            record_part_writes: counter(
                reg,
                &n("record_part_writes_total"),
                "Total number of record parts written before flushing.",
            ),
            record_parts_bytes_written: counter(
                reg,
                &n("record_parts_bytes_written_total"),
                "Total number of record part bytes written before flushing, including CRC and compression headers.",
            ),
            record_bytes_saved: counter_vec(
                reg,
                &n("record_bytes_saved_total"),
                "Total number of bytes saved by the optional record compression. Use this metric to learn about the effectiveness compression.",
                "compression",
            ),
        }
    }
}

pub const WAL_METRICS_PREFIX: &str = "prometheus_tsdb_wal_";
pub const WBL_METRICS_PREFIX: &str = "prometheus_tsdb_out_of_order_wbl_";

/// Go `chunkWriteQueue` operation counters (`prometheus_tsdb_chunk_write_queue_operations_total`),
/// registered only when `head_chunks_write_queue_size > 0` like Go. Rust writes m-mapped
/// chunks synchronously, i.e. the queue is always drained immediately: every chunk write is
/// one `add` and one `complete`; a lookup can never find a chunk still queued (`get`), and
/// the ref map never grows (`shrink`).
#[derive(Clone)]
pub struct ChunkWriteQueueMetrics {
    pub adds: Counter,
    pub completed: Counter,
}

impl ChunkWriteQueueMetrics {
    pub fn new(reg: Option<&Registry>) -> Self {
        let v = counter_vec(
            reg,
            "prometheus_tsdb_chunk_write_queue_operations_total",
            "Number of operations on the chunk_write_queue.",
            "operation",
        );
        let adds = v.with_label_values(&["add"]);
        let _ = v.with_label_values(&["get"]);
        let completed = v.with_label_values(&["complete"]);
        let _ = v.with_label_values(&["shrink"]);
        Self { adds, completed }
    }
}
