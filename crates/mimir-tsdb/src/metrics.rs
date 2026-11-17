//! Per-tenant Prometheus TSDB metric helpers shared by the Head, the write logs and the DB.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/wlog/wlog.go (`newWLMetrics`),
//! github.com/prometheus/prometheus/tsdb/chunks/chunk_write_queue.go (`newChunkWriteQueue`
//! metrics) and the summary metrics of github.com/prometheus/prometheus/tsdb/head.go.
//! Mimir aggregates them from the per-tenant registry in
//! github.com/grafana/mimir/pkg/storage/tsdb/tsdb_metrics.go.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{
    Metric, MetricFamily, MetricType as PromMetricType, Quantile, Summary as ProtoSummary,
};
use prometheus::{Counter, CounterVec, Gauge, Opts, Registry};

struct SummaryInner {
    desc: Desc,
    quantiles: Vec<f64>,
    state: Mutex<(u64, f64)>,
}

/// A Prometheus summary exposing `_count` / `_sum` (the values Mimir's `SendSumOfSummaries`
/// aggregates). Configured objectives are exposed as quantile entries with value `NaN`, the
/// value client_golang reports for an objective when no observation is in the current window
/// (the Rust `prometheus` crate has no summary type).
#[derive(Clone)]
pub struct Summary(Arc<SummaryInner>);

impl Summary {
    pub fn new(name: &str, help: &str, quantiles: &[f64]) -> Self {
        let desc = Desc::new(name.to_string(), help.to_string(), vec![], HashMap::new())
            .expect("valid summary desc");
        Self(Arc::new(SummaryInner {
            desc,
            quantiles: quantiles.to_vec(),
            state: Mutex::new((0, 0.0)),
        }))
    }

    pub fn observe(&self, v: f64) {
        let mut g = self.0.state.lock();
        g.0 += 1;
        g.1 += v;
    }

    pub fn sample_count(&self) -> u64 {
        self.0.state.lock().0
    }
}

impl Collector for Summary {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.0.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let (count, sum) = *self.0.state.lock();
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
                q.set_value(f64::NAN);
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
}

impl WlogMetrics {
    pub fn new(reg: Option<&Registry>, prefix: &str) -> Self {
        let n = |s: &str| format!("{prefix}{s}");
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
