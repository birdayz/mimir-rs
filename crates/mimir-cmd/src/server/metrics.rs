//! dskit server metrics as configured by Mimir (`MetricsNamespace = "cortex"`,
//! `MetricsNativeHistogramFactor = 1.1`, message-size native histograms disabled).
//!
//! Ported from:
//! - github.com/grafana/dskit/server/metrics.go (`NewServerMetrics`)
//! - github.com/grafana/dskit/middleware/grpc_stats.go (`StreamTracker`,
//!   `grpc_concurrent_streams_by_conn_max`)
//! - github.com/grafana/dskit/middleware/http_cluster.go / grpc_cluster.go
//!   (`NewInvalidClusterRequests`)
//! - github.com/grafana/dskit/instrument/instrument.go (`DefBuckets`)
//! - github.com/grafana/dskit/middleware/instrument.go (`BodySizeBuckets`)

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_common::native_histogram::{exponential_buckets, HistogramOpts, HistogramVec};
use parking_lot::RwLock;
use prometheus::core::{Collector, Desc};
use prometheus::{proto, GaugeVec, IntCounterVec, Opts, Registry};

/// dskit `instrument.DefBuckets`.
pub const INSTRUMENT_DEF_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0,
];

/// Mimir sets `server.Config.MetricsNativeHistogramFactor = 1.1` (pkg/mimir/mimir.go).
pub const METRICS_NATIVE_HISTOGRAM_FACTOR: f64 = 1.1;

#[derive(Clone)]
pub struct ServerMetrics {
    pub tcp_connections: GaugeVec,
    pub tcp_connections_limit: GaugeVec,
    pub grpc_concurrent_streams_limit: GaugeVec,
    pub request_duration: HistogramVec,
    pub received_message_size: HistogramVec,
    pub sent_message_size: HistogramVec,
    pub inflight_requests: GaugeVec,
    pub request_throughput: HistogramVec,
    pub throughput_latency_cutoff: Duration,
    pub throughput_unit: String,
    pub invalid_cluster_requests: IntCounterVec,
    pub stream_tracker: Option<Arc<StreamTracker>>,
}

impl ServerMetrics {
    /// Creates and registers the metrics. `stats_tracking` / `collect_max_streams_by_conn` mirror
    /// `-server.grpc.stats-tracking-enabled` / `-server.grpc.collect-max-streams-by-conn`.
    pub fn new(
        reg: &Registry,
        stats_tracking: bool,
        collect_max_streams_by_conn: bool,
    ) -> prometheus::Result<Self> {
        Self::new_with_throughput(
            reg,
            stats_tracking,
            collect_max_streams_by_conn,
            "samples_processed",
            0,
        )
    }

    /// Creates and registers server metrics with configured throughput unit and latency cutoff
    /// (Go `dskit/server/metrics.go:NewServerMetrics`).
    pub fn new_with_throughput(
        reg: &Registry,
        stats_tracking: bool,
        collect_max_streams_by_conn: bool,
        throughput_unit: &str,
        throughput_latency_cutoff_ms: i64,
    ) -> prometheus::Result<Self> {
        let ns = "cortex";
        let tcp_connections = GaugeVec::new(
            Opts::new("tcp_connections", "Current number of accepted TCP connections.").namespace(ns),
            &["protocol"],
        )?;
        let tcp_connections_limit = GaugeVec::new(
            Opts::new(
                "tcp_connections_limit",
                "The max number of TCP connections that can be accepted (0 means no limit).",
            )
            .namespace(ns),
            &["protocol"],
        )?;
        let grpc_concurrent_streams_limit = GaugeVec::new(
            Opts::new(
                "grpc_concurrent_streams_limit",
                "The max number of concurrent streams that can be accepted (0 means no limit).",
            )
            .namespace(ns),
            &[],
        )?;
        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "cortex_request_duration_seconds",
                "Time (in seconds) spent serving HTTP requests.",
            )
            .buckets(INSTRUMENT_DEF_BUCKETS.to_vec())
            .native(METRICS_NATIVE_HISTOGRAM_FACTOR, 100, Duration::from_secs(3600)),
            &["method", "route", "status_code", "ws"],
        )?;
        let body_buckets = exponential_buckets(4.0, 4.0, 15);
        let received_message_size = HistogramVec::new(
            HistogramOpts::new(
                "cortex_request_message_bytes",
                "Size (in bytes) of messages received in the request.",
            )
            .buckets(body_buckets.clone()),
            &["method", "route"],
        )?;
        let sent_message_size = HistogramVec::new(
            HistogramOpts::new(
                "cortex_response_message_bytes",
                "Size (in bytes) of messages sent in response.",
            )
            .buckets(body_buckets),
            &["method", "route"],
        )?;
        let inflight_requests = GaugeVec::new(
            Opts::new("inflight_requests", "Current number of inflight requests.").namespace(ns),
            &["method", "route"],
        )?;
        let request_throughput = HistogramVec::new(
            HistogramOpts::new(
                format!("{ns}_request_throughput_{throughput_unit}"),
                "Server throughput of running requests.",
            )
            .const_label("cutoff_ms", throughput_latency_cutoff_ms.to_string())
            .buckets(INSTRUMENT_DEF_BUCKETS.to_vec())
            .native(METRICS_NATIVE_HISTOGRAM_FACTOR, 100, Duration::from_secs(3600)),
            &["method", "route"],
        )?;
        let invalid_cluster_requests = IntCounterVec::new(
            Opts::new(
                "server_invalid_cluster_validation_label_requests_total",
                "Number of requests received by server with invalid cluster validation label.",
            )
            .namespace(ns),
            &["protocol", "method", "cluster_validation_label", "request_cluster_validation_label"],
        )?;

        reg.register(Box::new(tcp_connections.clone()))?;
        reg.register(Box::new(tcp_connections_limit.clone()))?;
        reg.register(Box::new(grpc_concurrent_streams_limit.clone()))?;
        reg.register(Box::new(request_duration.clone()))?;
        reg.register(Box::new(received_message_size.clone()))?;
        reg.register(Box::new(sent_message_size.clone()))?;
        reg.register(Box::new(inflight_requests.clone()))?;
        reg.register(Box::new(request_throughput.clone()))?;
        reg.register(Box::new(invalid_cluster_requests.clone()))?;

        let stream_tracker = if stats_tracking && collect_max_streams_by_conn {
            let t = Arc::new(StreamTracker::new()?);
            reg.register(Box::new(StreamTrackerCollector(Arc::clone(&t))))?;
            Some(t)
        } else {
            None
        };

        Ok(Self {
            tcp_connections,
            tcp_connections_limit,
            grpc_concurrent_streams_limit,
            request_duration,
            received_message_size,
            sent_message_size,
            inflight_requests,
            request_throughput,
            throughput_latency_cutoff: Duration::from_millis(throughput_latency_cutoff_ms.max(0) as u64),
            throughput_unit: throughput_unit.to_string(),
            invalid_cluster_requests,
            stream_tracker,
        })
    }
}

/// dskit `StreamTracker`: concurrent streams per connection; exposes the maximum.
pub struct StreamTracker {
    desc: Desc,
    conns: RwLock<HashMap<u64, Arc<AtomicI64>>>,
}

impl StreamTracker {
    pub fn new() -> prometheus::Result<Self> {
        Ok(Self {
            desc: Desc::new(
                "grpc_concurrent_streams_by_conn_max".to_string(),
                "The current number of concurrent streams in the connection with the most concurrent streams."
                    .to_string(),
                vec![],
                HashMap::new(),
            )?,
            conns: RwLock::new(HashMap::new()),
        })
    }

    pub fn open_stream(&self, conn_id: u64) {
        if let Some(c) = self.conns.read().get(&conn_id) {
            c.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.conns
            .write()
            .entry(conn_id)
            .or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn close_stream(&self, conn_id: u64) {
        let Some(c) = self.conns.read().get(&conn_id).cloned() else {
            return;
        };
        if c.fetch_sub(1, Ordering::Relaxed) == 1 {
            let mut conns = self.conns.write();
            if conns.get(&conn_id).is_some_and(|c| c.load(Ordering::Relaxed) == 0) {
                conns.remove(&conn_id);
            }
        }
    }

    pub fn max_streams(&self) -> i64 {
        self.conns
            .read()
            .values()
            .map(|c| c.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
            .max(0)
    }
}

struct StreamTrackerCollector(Arc<StreamTracker>);

impl Collector for StreamTrackerCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.0.desc]
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut g = proto::Gauge::default();
        g.set_value(self.0.max_streams() as f64);
        let mut m = proto::Metric::default();
        m.set_gauge(g);
        let mut mf = proto::MetricFamily::default();
        mf.set_name(self.0.desc.fq_name.clone());
        mf.set_help(self.0.desc.help.clone());
        mf.set_field_type(proto::MetricType::GAUGE);
        mf.set_metric(vec![m].into());
        vec![mf]
    }
}

/// dskit `middleware.MakeLabelValue`: converts a mux path template into a label value.
pub fn make_label_value(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut last_us = false;
    for c in path.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            last_us = false;
        } else if !last_us {
            out.push('_');
            last_us = true;
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "root".to_string()
    } else {
        trimmed.to_string()
    }
}

/// gRPC status code name (`codes.Code.String()`), or the HTTP status for httpgrpc errors (dskit
/// `instrumentationLabel.statusCodeToString` with `ReportGRPCStatusOption`, as Mimir enables
/// `-server.report-grpc-codes-in-instrumentation-label-enabled` by default). With the option
/// disabled, dskit reports `success` / `cancel` / `error`.
pub fn grpc_status_label(code: i32, report_grpc_status: bool) -> String {
    instrumentation_status_code_to_string(code, report_grpc_status, false)
}

/// Ported from `dskit/middleware/grpc_instrumentation.go` (`instrumentationLabel`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InstrumentationLabelConfig {
    pub report_grpc_status: bool,
    pub mask_http_status: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstrumentationOption {
    ReportGrpcStatus,
}

pub fn apply_instrumentation_options(
    mask_http_statuses: bool,
    options: &[InstrumentationOption],
) -> InstrumentationLabelConfig {
    let mut cfg = InstrumentationLabelConfig {
        report_grpc_status: false,
        mask_http_status: mask_http_statuses,
    };
    for opt in options {
        match opt {
            InstrumentationOption::ReportGrpcStatus => cfg.report_grpc_status = true,
        }
    }
    cfg
}

/// Ported from `dskit/middleware/grpc_instrumentation.go` (`statusCodeToString`).
pub fn instrumentation_status_code_to_string(
    code: i32,
    report_grpc_status: bool,
    mask_http_status: bool,
) -> String {
    if (100..600).contains(&code) {
        if mask_http_status {
            return format!("{}xx", code / 100);
        }
        return code.to_string();
    }
    if report_grpc_status {
        return match code {
            0 => "OK",
            1 => "Canceled",
            2 => "Unknown",
            3 => "InvalidArgument",
            4 => "DeadlineExceeded",
            5 => "NotFound",
            6 => "AlreadyExists",
            7 => "PermissionDenied",
            8 => "ResourceExhausted",
            9 => "FailedPrecondition",
            10 => "Aborted",
            11 => "OutOfRange",
            12 => "Unimplemented",
            13 => "Internal",
            14 => "Unavailable",
            15 => "DataLoss",
            16 => "Unauthenticated",
            other => return format!("Code({other})"),
        }
        .to_string();
    }
    match code {
        0 => {
            if mask_http_status {
                "2xx".to_string()
            } else {
                "success".to_string()
            }
        }
        1 => "cancel".to_string(),
        _ => "error".to_string(),
    }
}

/// Ported from `dskit/middleware/grpc_instrumentation.go` (`errorToStatusCode`).
pub fn error_to_grpc_status_code(err: Option<&(dyn std::error::Error + 'static)>) -> i32 {
    let Some(e) = err else {
        return 0;
    };
    // Check direct or wrapped tonic::Status or context cancellation.
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(node) = cur {
        if let Some(st) = node.downcast_ref::<tonic::Status>() {
            if let Some(http_resp) = mimir_proto::grpcutilpb::extract_http_response(st) {
                return http_resp.code;
            }
            if let Ok(rpc_st) = mimir_proto::grpcutilpb::decode_status_details(st) {
                if (100..600).contains(&rpc_st.code) {
                    return rpc_st.code;
                }
            }
            return st.code() as i32;
        }
        if node.to_string() == "context canceled" {
            return tonic::Code::Cancelled as i32;
        }
        cur = node.source();
    }
    tonic::Code::Unknown as i32
}

/// Trait abstracting a gRPC client stream for `StreamClientInstrumentInterceptor`.
pub trait MockableClientStream: Send + Sync {
    fn send_msg(&mut self) -> Result<(), StreamOpError>;
    fn recv_msg(&mut self) -> Result<(), StreamOpError>;
    fn header(&mut self) -> Result<(), StreamOpError>;
    fn close_send(&mut self) -> Result<(), StreamOpError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamOpError {
    Eof,
    Cancelled,
    Other(String),
}

impl std::fmt::Display for StreamOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eof => f.write_str("EOF"),
            Self::Cancelled => f.write_str("context canceled"),
            Self::Other(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for StreamOpError {}

/// Ported from `dskit/middleware/grpc_instrumentation.go` (`instrumentedClientStream`).
pub struct InstrumentedClientStream<S> {
    inner: S,
    metric: HistogramVec,
    method: String,
    server_streams: bool,
    start: std::time::Instant,
    label_cfg: InstrumentationLabelConfig,
    finished: std::sync::atomic::AtomicBool,
}

impl<S> InstrumentedClientStream<S> {
    pub fn new(
        inner: S,
        metric: HistogramVec,
        method: impl Into<String>,
        server_streams: bool,
        options: &[InstrumentationOption],
    ) -> Self {
        Self {
            inner,
            metric,
            method: method.into(),
            server_streams,
            start: std::time::Instant::now(),
            label_cfg: apply_instrumentation_options(true, options),
            finished: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn finish_with_error(&self, err: Option<&StreamOpError>) {
        if self
            .finished
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            return;
        }
        let code = match err {
            None => 0,
            Some(StreamOpError::Cancelled) => tonic::Code::Cancelled as i32,
            Some(StreamOpError::Eof) => 0,
            Some(StreamOpError::Other(_)) => tonic::Code::Unknown as i32,
        };
        let label = instrumentation_status_code_to_string(
            code,
            self.label_cfg.report_grpc_status,
            self.label_cfg.mask_http_status,
        );
        self.metric
            .with_label_values(&[&self.method, &label])
            .observe(self.start.elapsed().as_secs_f64());
    }

    pub fn cancel(&self) {
        self.finish_with_error(Some(&StreamOpError::Cancelled));
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S: MockableClientStream> MockableClientStream for InstrumentedClientStream<S> {
    fn send_msg(&mut self) -> Result<(), StreamOpError> {
        let res = self.inner.send_msg();
        match &res {
            Ok(()) | Err(StreamOpError::Eof) => {}
            Err(e) => self.finish_with_error(Some(e)),
        }
        res
    }

    fn recv_msg(&mut self) -> Result<(), StreamOpError> {
        let res = self.inner.recv_msg();
        if !self.server_streams {
            self.finish_with_error(res.as_ref().err());
            return res;
        }
        match &res {
            Ok(()) => {}
            Err(StreamOpError::Eof) => self.finish_with_error(None),
            Err(e) => self.finish_with_error(Some(e)),
        }
        res
    }

    fn header(&mut self) -> Result<(), StreamOpError> {
        let res = self.inner.header();
        if let Err(ref e) = res {
            self.finish_with_error(Some(e));
        }
        res
    }

    fn close_send(&mut self) -> Result<(), StreamOpError> {
        let res = self.inner.close_send();
        if let Err(ref e) = res {
            self.finish_with_error(Some(e));
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_label_value_matches_dskit() {
        assert_eq!(make_label_value("/"), "root");
        assert_eq!(make_label_value("/ingester/tsdb/{tenant}"), "ingester_tsdb_tenant");
        assert_eq!(make_label_value("/api/v1/status/buildinfo"), "api_v1_status_buildinfo");
        assert_eq!(make_label_value("/ingester/prepare-shutdown"), "ingester_prepare_shutdown");
        assert_eq!(make_label_value("/Debug//pprof"), "debug_pprof");
    }

    #[test]
    fn grpc_status_labels() {
        assert_eq!(grpc_status_label(0, true), "OK");
        assert_eq!(grpc_status_label(13, true), "Internal");
        assert_eq!(grpc_status_label(429, true), "429");
        assert_eq!(grpc_status_label(0, false), "success");
        assert_eq!(grpc_status_label(1, false), "cancel");
        assert_eq!(grpc_status_label(2, false), "error");
        assert_eq!(grpc_status_label(42, true), "Code(42)");
    }

    #[test]
    fn stream_tracker_max() {
        let t = StreamTracker::new().unwrap();
        t.open_stream(1);
        t.open_stream(1);
        t.open_stream(2);
        assert_eq!(t.max_streams(), 2);
        t.close_stream(1);
        t.close_stream(1);
        assert_eq!(t.max_streams(), 1);
        assert_eq!(t.conns.read().len(), 1);
    }
}
