//! KIP-714 client telemetry metrics and OTLP protobuf serializer.
//!
//! Ported from github.com/twmb/franz-go/pkg/kgo/metrics_714.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_PUSH_INTERVAL_MILLIS: i32 = 5 * 60 * 1000;

/// Ported from `kgo.validatePushIntervalMillis`.
pub fn validate_push_interval_millis(advertised: i32) -> i32 {
    if advertised <= 0 {
        DEFAULT_PUSH_INTERVAL_MILLIS
    } else {
        advertised
    }
}

/// Ported from `kgo.metricRate`.
#[derive(Debug, Default)]
pub struct MetricRate {
    count: AtomicI64,
    tot: AtomicI64,
    last_tot: i64,
}

impl MetricRate {
    pub fn observe(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.tot.fetch_add(1, Ordering::Relaxed);
    }

    pub fn roll_nums(&mut self, dur: Duration) -> (f64, i64, i64) {
        let count = self.count.swap(0, Ordering::Relaxed);
        let tot = self.tot.load(Ordering::Relaxed);
        let last_tot = self.last_tot;
        self.last_tot = tot;
        let secs = dur.as_secs_f64();
        let rate = if secs > 0.0 {
            (count as f64) / secs
        } else {
            0.0
        };
        (rate, tot, last_tot)
    }
}

/// Ported from `kgo.metricTime`.
#[derive(Debug, Default)]
pub struct MetricTime {
    sum: AtomicI64,
    count: AtomicI64,
    max: AtomicI64,
}

impl MetricTime {
    pub fn observe(&self, v: i64) {
        self.sum.fetch_add(v, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.max.fetch_max(v, Ordering::Relaxed);
    }

    pub fn roll_nums(&self) -> (f64, i64) {
        let count = self.count.swap(0, Ordering::Relaxed);
        let sum = self.sum.swap(0, Ordering::Relaxed);
        let max = self.max.swap(0, Ordering::Relaxed);
        let avg = if count > 0 {
            (sum as f64) / (count as f64)
        } else {
            0.0
        };
        (avg, max)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OtelAttrValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Unsupported,
}

const PROTO_TYPE_VARINT: u64 = 0;
const PROTO_TYPE_64BIT: u64 = 1;
const PROTO_TYPE_LENGTH: u64 = 2;

fn append_uvarint(b: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        b.push((v as u8) | 0x80);
        v >>= 7;
    }
    b.push(v as u8);
}

fn append_proto_tag(b: &mut Vec<u8>, field_num: u64, wire_type: u64) {
    append_uvarint(b, (field_num << 3) | wire_type);
}

fn append_proto_string(b: &mut Vec<u8>, s: &str) {
    append_uvarint(b, s.len() as u64);
    b.extend_from_slice(s.as_bytes());
}

/// Ported from `kgo.appendOtelAttributesTo`: unsupported attribute types are completely
/// skipped without writing a dangling field tag.
pub fn append_otel_attributes_to(
    b: &mut Vec<u8>,
    field_number: u64,
    attrs: &HashMap<String, OtelAttrValue>,
) {
    for (key, value) in attrs {
        let mut any_val = Vec::new();
        match value {
            OtelAttrValue::String(s) => {
                append_proto_tag(&mut any_val, 1, PROTO_TYPE_LENGTH);
                append_proto_string(&mut any_val, s);
            }
            OtelAttrValue::Bool(v) => {
                append_proto_tag(&mut any_val, 2, PROTO_TYPE_VARINT);
                append_uvarint(&mut any_val, u64::from(*v));
            }
            OtelAttrValue::Int(v) => {
                append_proto_tag(&mut any_val, 3, PROTO_TYPE_VARINT);
                append_uvarint(&mut any_val, *v as u64);
            }
            OtelAttrValue::Double(v) => {
                append_proto_tag(&mut any_val, 4, PROTO_TYPE_64BIT);
                any_val.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            OtelAttrValue::Bytes(bytes) => {
                append_proto_tag(&mut any_val, 7, PROTO_TYPE_LENGTH);
                append_uvarint(&mut any_val, bytes.len() as u64);
                any_val.extend_from_slice(bytes);
            }
            OtelAttrValue::Unsupported => continue,
        }

        append_proto_tag(b, field_number, PROTO_TYPE_LENGTH);
        let mut kv = Vec::new();
        append_proto_tag(&mut kv, 1, PROTO_TYPE_LENGTH);
        append_proto_string(&mut kv, key);
        append_proto_tag(&mut kv, 2, PROTO_TYPE_LENGTH);
        append_uvarint(&mut kv, any_val.len() as u64);
        kv.extend_from_slice(&any_val);

        append_uvarint(b, kv.len() as u64);
        b.extend_from_slice(&kv);
    }
}

#[derive(Debug, Default)]
pub struct KgoTelemetryMetrics {
    pub p_conn_creation: MetricRate,
    pub p_throttle: MetricTime,
    pub c_conn_creation: MetricRate,
    pub c_commit_latency: MetricTime,
    pub init_nano: i64,
    pub last_push_nano: i64,
}

impl KgoTelemetryMetrics {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Self {
            init_nano: now,
            last_push_nano: now,
            ..Default::default()
        }
    }

    /// Serializes OTLP `MetricsData` protobuf, returning `(bytes, metric_count)`.
    pub fn append_to(&mut self, delta: bool) -> (Vec<u8>, usize) {
        let now_nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .max(1) as i64;
        let last_push = self.last_push_nano;
        self.last_push_nano = now_nano;
        let agg_dur = Duration::from_nanos((now_nano - last_push).max(1) as u64);

        let mut metrics_bytes = Vec::new();
        let mut count = 0usize;

        for (name, rate_m) in [
            ("org.apache.kafka.producer.connection.creation", &mut self.p_conn_creation),
            ("org.apache.kafka.consumer.connection.creation", &mut self.c_conn_creation),
        ] {
            let (rate, tot, last_tot) = rate_m.roll_nums(agg_dur);
            if rate != 0.0 {
                let mut dp = Vec::new();
                append_proto_tag(&mut dp, 2, PROTO_TYPE_64BIT);
                dp.extend_from_slice(&(last_push as u64).to_le_bytes());
                append_proto_tag(&mut dp, 3, PROTO_TYPE_64BIT);
                dp.extend_from_slice(&(now_nano as u64).to_le_bytes());
                append_proto_tag(&mut dp, 4, PROTO_TYPE_64BIT);
                dp.extend_from_slice(&rate.to_bits().to_le_bytes());

                let mut gauge = Vec::new();
                append_proto_tag(&mut gauge, 1, PROTO_TYPE_LENGTH);
                append_uvarint(&mut gauge, dp.len() as u64);
                gauge.extend_from_slice(&dp);

                let mut metric = Vec::new();
                append_proto_tag(&mut metric, 1, PROTO_TYPE_LENGTH);
                append_proto_string(&mut metric, &format!("{name}.rate"));
                append_proto_tag(&mut metric, 5, PROTO_TYPE_LENGTH);
                append_uvarint(&mut metric, gauge.len() as u64);
                metric.extend_from_slice(&gauge);

                append_proto_tag(&mut metrics_bytes, 2, PROTO_TYPE_LENGTH);
                append_uvarint(&mut metrics_bytes, metric.len() as u64);
                metrics_bytes.extend_from_slice(&metric);
                count += 1;
            }
            if tot - last_tot != 0 {
                let (start_nano, val, temporality) = if delta {
                    (last_push, tot - last_tot, 1u64)
                } else {
                    (self.init_nano, tot, 2u64)
                };
                let mut dp = Vec::new();
                append_proto_tag(&mut dp, 2, PROTO_TYPE_64BIT);
                dp.extend_from_slice(&(start_nano as u64).to_le_bytes());
                append_proto_tag(&mut dp, 3, PROTO_TYPE_64BIT);
                dp.extend_from_slice(&(now_nano as u64).to_le_bytes());
                append_proto_tag(&mut dp, 6, PROTO_TYPE_64BIT);
                dp.extend_from_slice(&(val as u64).to_le_bytes());

                let mut sum = Vec::new();
                append_proto_tag(&mut sum, 1, PROTO_TYPE_LENGTH);
                append_uvarint(&mut sum, dp.len() as u64);
                sum.extend_from_slice(&dp);
                // Field 2: aggregation_temporality
                append_proto_tag(&mut sum, 2, PROTO_TYPE_VARINT);
                append_uvarint(&mut sum, temporality);
                // Field 3: is_monotonic = true
                append_proto_tag(&mut sum, 3, PROTO_TYPE_VARINT);
                append_uvarint(&mut sum, 1);

                let mut metric = Vec::new();
                append_proto_tag(&mut metric, 1, PROTO_TYPE_LENGTH);
                append_proto_string(&mut metric, &format!("{name}.total"));
                append_proto_tag(&mut metric, 7, PROTO_TYPE_LENGTH);
                append_uvarint(&mut metric, sum.len() as u64);
                metric.extend_from_slice(&sum);

                append_proto_tag(&mut metrics_bytes, 2, PROTO_TYPE_LENGTH);
                append_uvarint(&mut metrics_bytes, metric.len() as u64);
                metrics_bytes.extend_from_slice(&metric);
                count += 1;
            }
        }

        // Wrap in ScopeMetrics -> ResourceMetrics -> MetricsData
        let mut scope = Vec::new();
        append_proto_tag(&mut scope, 1, PROTO_TYPE_LENGTH);
        append_proto_string(&mut scope, "franz-go");
        let mut scope_metrics = Vec::new();
        append_proto_tag(&mut scope_metrics, 1, PROTO_TYPE_LENGTH);
        append_uvarint(&mut scope_metrics, scope.len() as u64);
        scope_metrics.extend_from_slice(&scope);
        scope_metrics.extend_from_slice(&metrics_bytes);

        let mut resource_metrics = Vec::new();
        append_proto_tag(&mut resource_metrics, 2, PROTO_TYPE_LENGTH);
        append_uvarint(&mut resource_metrics, scope_metrics.len() as u64);
        resource_metrics.extend_from_slice(&scope_metrics);

        let mut out = Vec::new();
        append_proto_tag(&mut out, 1, PROTO_TYPE_LENGTH);
        append_uvarint(&mut out, resource_metrics.len() as u64);
        out.extend_from_slice(&resource_metrics);

        (out, count)
    }
}
