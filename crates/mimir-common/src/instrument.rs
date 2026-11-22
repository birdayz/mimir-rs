//! Ported from github.com/grafana/dskit/instrument/instrument.go.

use std::error::Error as StdError;
use std::time::Instant;

use prometheus::{GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry};

/// Default histogram buckets for request latency (seconds).
pub const DEF_BUCKETS: [f64; 14] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0,
];

/// Label names used by [`HistogramCollector`].
pub const HISTOGRAM_COLLECTOR_BUCKETS: [&str; 2] = ["operation", "status_code"];

/// Collector describes something that collects metrics before and/or after a task.
pub trait Collector: Send + Sync {
    fn register(&self, registry: &Registry);
    fn before(&self, method: &str, start: Instant);
    fn after(&self, method: &str, status_code: &str, start: Instant);
}

/// Collects request duration into a Prometheus `HistogramVec`.
pub struct HistogramCollector {
    metric: Option<HistogramVec>,
}

impl HistogramCollector {
    pub fn from_opts(opts: HistogramOpts) -> prometheus::Result<Self> {
        let metric = HistogramVec::new(opts, &HISTOGRAM_COLLECTOR_BUCKETS)?;
        Ok(Self {
            metric: Some(metric),
        })
    }

    pub fn new(metric: HistogramVec) -> Self {
        Self {
            metric: Some(metric),
        }
    }

    pub fn metric(&self) -> Option<&HistogramVec> {
        self.metric.as_ref()
    }
}

impl Collector for HistogramCollector {
    fn register(&self, registry: &Registry) {
        if let Some(m) = &self.metric {
            let _ = registry.register(Box::new(m.clone()));
        }
    }

    fn before(&self, _method: &str, _start: Instant) {}

    fn after(&self, method: &str, status_code: &str, start: Instant) {
        if let Some(m) = &self.metric {
            m.with_label_values(&[method, status_code])
                .observe(start.elapsed().as_secs_f64());
        }
    }
}

/// Collects metrics for periodic/batch jobs.
pub struct JobCollector {
    start: GaugeVec,
    end: GaugeVec,
    duration: GaugeVec,
    started: IntCounterVec,
    completed: IntCounterVec,
}

impl JobCollector {
    pub fn new(namespace: &str) -> prometheus::Result<Self> {
        Ok(Self {
            start: GaugeVec::new(
                Opts::new(
                    "latest_start_timestamp",
                    "Unix UTC timestamp of most recent job start time",
                )
                .namespace(namespace)
                .subsystem("job"),
                &["operation"],
            )?,
            end: GaugeVec::new(
                Opts::new(
                    "latest_end_timestamp",
                    "Unix UTC timestamp of most recent job end time",
                )
                .namespace(namespace)
                .subsystem("job"),
                &["operation", "status_code"],
            )?,
            duration: GaugeVec::new(
                Opts::new("latest_duration_seconds", "duration of most recent job")
                    .namespace(namespace)
                    .subsystem("job"),
                &["operation", "status_code"],
            )?,
            started: IntCounterVec::new(
                Opts::new("started_total", "Number of jobs started")
                    .namespace(namespace)
                    .subsystem("job"),
                &["operation"],
            )?,
            completed: IntCounterVec::new(
                Opts::new("completed_total", "Number of jobs completed")
                    .namespace(namespace)
                    .subsystem("job"),
                &["operation", "status_code"],
            )?,
        })
    }
}

impl Collector for JobCollector {
    fn register(&self, registry: &Registry) {
        let _ = registry.register(Box::new(self.start.clone()));
        let _ = registry.register(Box::new(self.end.clone()));
        let _ = registry.register(Box::new(self.duration.clone()));
        let _ = registry.register(Box::new(self.started.clone()));
        let _ = registry.register(Box::new(self.completed.clone()));
    }

    fn before(&self, method: &str, _start: Instant) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as f64)
            .unwrap_or(0.0);
        self.start.with_label_values(&[method]).set(now_secs);
        self.started.with_label_values(&[method]).inc();
    }

    fn after(&self, method: &str, status_code: &str, start: Instant) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as f64)
            .unwrap_or(0.0);
        self.end
            .with_label_values(&[method, status_code])
            .set(now_secs);
        self.duration
            .with_label_values(&[method, status_code])
            .set(start.elapsed().as_secs_f64());
        self.completed.with_label_values(&[method, status_code]).inc();
    }
}

/// Converts an error outcome into an HTTP status code string (`"200"` or `"500"`).
pub fn error_code<E>(res: Option<&E>) -> String {
    if res.is_none() {
        "200".to_string()
    } else {
        "500".to_string()
    }
}

/// Runs a tracked request using `col` to monitor before/after timestamps and status codes.
pub fn collected_request<F, E>(
    method: &str,
    col: &dyn Collector,
    to_status_code: Option<fn(Option<&E>) -> String>,
    f: F,
) -> Result<(), E>
where
    F: FnOnce() -> Result<(), E>,
    E: StdError,
{
    let to_code = to_status_code.unwrap_or(error_code::<E>);
    let start = Instant::now();
    col.before(method, start);
    let res = f();
    let code = to_code(res.as_ref().err());
    col.after(method, &code, start);
    res
}
