//! Object storage operation metrics (`thanos_objstore_bucket_*{component,bucket,operation}`).
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/storage/bucket/client.go (`bucketWithMetrics`: registerer
//!   wrapped with prefix `thanos_` and const label `component`)
//! - github.com/thanos-io/objstore/objstore.go (`BucketMetrics`, `wrapWithMetrics`,
//!   `metricBucket`, `timingReader`)
//!
//! Native-histogram settings of the Go histograms are not representable with the `prometheus`
//! crate; the classic buckets are identical.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use prometheus::{CounterVec, Gauge, HistogramOpts, HistogramVec, Opts, Registry};

pub const OP_ITER: &str = "iter";
pub const OP_GET: &str = "get";
pub const OP_GET_RANGE: &str = "get_range";
pub const OP_EXISTS: &str = "exists";
pub const OP_UPLOAD: &str = "upload";
pub const OP_DELETE: &str = "delete";
pub const OP_ATTRIBUTES: &str = "attributes";

/// objstore `Metrics`.
#[derive(Clone, Debug)]
pub struct BucketMetrics {
    ops: CounterVec,
    ops_failures: CounterVec,
    ops_fetched_bytes: CounterVec,
    ops_transferred_bytes: HistogramVec,
    ops_duration: HistogramVec,
    last_successful_upload_time: Gauge,
}

/// `prometheus.ExponentialBuckets(2<<14, 2, 16)`.
fn transferred_bytes_buckets() -> Vec<f64> {
    (0..16).map(|i| (32768u64 << i) as f64).collect()
}

const DURATION_BUCKETS: &[f64] = &[
    0.001, 0.01, 0.1, 0.3, 0.6, 1.0, 3.0, 6.0, 9.0, 20.0, 30.0, 60.0, 90.0, 120.0,
];

impl BucketMetrics {
    /// Mimir `bucketWithMetrics(bucket, component, reg)` + objstore `WrapWithMetrics(bkt, reg,
    /// bkt.Name())`.
    pub fn register(registry: &Registry, component: &str, bucket: &str) -> prometheus::Result<Self> {
        let const_labels = || {
            let mut m = std::collections::HashMap::new();
            m.insert("component".to_string(), component.to_string());
            m.insert("bucket".to_string(), bucket.to_string());
            m
        };
        let ops = CounterVec::new(
            Opts::new(
                "thanos_objstore_bucket_operations_total",
                "Total number of all attempted operations against a bucket.",
            )
            .const_labels(const_labels()),
            &["operation"],
        )?;
        let ops_failures = CounterVec::new(
            Opts::new(
                "thanos_objstore_bucket_operation_failures_total",
                "Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated.",
            )
            .const_labels(const_labels()),
            &["operation"],
        )?;
        let ops_fetched_bytes = CounterVec::new(
            Opts::new(
                "thanos_objstore_bucket_operation_fetched_bytes_total",
                "Total number of bytes fetched from bucket, per operation.",
            )
            .const_labels(const_labels()),
            &["operation"],
        )?;
        let ops_transferred_bytes = HistogramVec::new(
            HistogramOpts::new(
                "thanos_objstore_bucket_operation_transferred_bytes",
                "Number of bytes transferred from/to bucket per operation.",
            )
            .const_labels(const_labels())
            .buckets(transferred_bytes_buckets()),
            &["operation"],
        )?;
        let ops_duration = HistogramVec::new(
            HistogramOpts::new(
                "thanos_objstore_bucket_operation_duration_seconds",
                "Duration of successful operations against the bucket per operation - iter operations include time spent on each callback.",
            )
            .const_labels(const_labels())
            .buckets(DURATION_BUCKETS.to_vec()),
            &["operation"],
        )?;
        let last_successful_upload_time = Gauge::with_opts(
            Opts::new(
                "thanos_objstore_bucket_last_successful_upload_time",
                "Second timestamp of the last successful upload to the bucket.",
            )
            .const_labels(const_labels()),
        )?;
        registry.register(Box::new(ops.clone()))?;
        registry.register(Box::new(ops_failures.clone()))?;
        registry.register(Box::new(ops_fetched_bytes.clone()))?;
        registry.register(Box::new(ops_transferred_bytes.clone()))?;
        registry.register(Box::new(ops_duration.clone()))?;
        registry.register(Box::new(last_successful_upload_time.clone()))?;

        // wrapWithMetrics: initialise every operation series with 0.
        for op in [OP_ITER, OP_GET, OP_GET_RANGE, OP_EXISTS, OP_UPLOAD, OP_DELETE, OP_ATTRIBUTES] {
            ops.with_label_values(&[op]);
            ops_failures.with_label_values(&[op]);
            ops_duration.with_label_values(&[op]);
        }
        for op in [OP_GET, OP_GET_RANGE, OP_UPLOAD] {
            ops_fetched_bytes.with_label_values(&[op]);
            ops_transferred_bytes.with_label_values(&[op]);
        }
        Ok(Self {
            ops,
            ops_failures,
            ops_fetched_bytes,
            ops_transferred_bytes,
            ops_duration,
            last_successful_upload_time,
        })
    }

    pub(crate) fn start(&self, op: &'static str) -> OpTimer<'_> {
        self.ops.with_label_values(&[op]).inc();
        OpTimer {
            metrics: self,
            op,
            start: Instant::now(),
        }
    }
}

/// Tracks one bucket operation.
pub(crate) struct OpTimer<'a> {
    metrics: &'a BucketMetrics,
    op: &'static str,
    start: Instant,
}

impl OpTimer<'_> {
    fn observe_duration(&self) {
        self.metrics
            .ops_duration
            .with_label_values(&[self.op])
            .observe(self.start.elapsed().as_secs_f64());
    }

    pub fn failed(&self) {
        self.metrics.ops_failures.with_label_values(&[self.op]).inc();
    }

    /// `Exists` / `Attributes` / `Delete`: duration only for successful operations.
    pub fn finish_simple(&self, ok: bool) {
        if ok {
            self.observe_duration();
        } else {
            self.failed();
        }
    }

    /// `Iter`: the duration is always observed (deferred timer).
    pub fn finish_iter(&self, ok: bool) {
        if !ok {
            self.failed();
        }
        self.observe_duration();
    }

    /// `Get` / `GetRange`: on error the failure (unless expected) and duration are recorded; on
    /// success the `timingReader` records fetched/transferred bytes and the duration when the body
    /// has been read and closed.
    pub fn finish_read(&self, result: Result<u64, ()>, expected_failure: bool) {
        match result {
            Err(()) => {
                if !expected_failure {
                    self.failed();
                }
                self.observe_duration();
            }
            Ok(n) => {
                self.metrics
                    .ops_fetched_bytes
                    .with_label_values(&[self.op])
                    .inc_by(n as f64);
                self.observe_duration();
                self.metrics
                    .ops_transferred_bytes
                    .with_label_values(&[self.op])
                    .observe(n as f64);
            }
        }
    }

    /// `Upload`: `transferred` is the number of bytes read through the `timingReader`'s `Read`
    /// method (0 when the backend reads through `ReaderAt`, as minio-go does). The timing reader
    /// is closed (duration + transferred bytes observed) whatever the outcome.
    pub fn finish_upload(&self, ok: bool, transferred: u64) {
        self.observe_duration();
        self.metrics
            .ops_transferred_bytes
            .with_label_values(&[self.op])
            .observe(transferred as f64);
        if ok {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            self.metrics.last_successful_upload_time.set(now);
        } else {
            self.failed();
        }
    }
}
