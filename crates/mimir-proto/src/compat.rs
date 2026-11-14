//! Ported from github.com/grafana/mimir/pkg/mimirpb/compat.go,
//! compat_slice.go, custom.go, errors.go, query_response_extra.go, and validate.go.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use prost::Message as _;
use xxhash_rust::xxh64::{xxh64, Xxh64};

use crate::cortexpb;

/// Matches `nonStableHashFastPathCap` in `pkg/mimirpb/compat.go`.
pub const NON_STABLE_HASH_FAST_PATH_CAP: usize = 1024;

static IS_TESTING_JSON: AtomicBool = AtomicBool::new(false);

pub fn set_json_testing_mode(enabled: bool) {
    IS_TESTING_JSON.store(enabled, Ordering::Relaxed);
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelAdapter {
    pub name: String,
    pub value: String,
}

pub type UnsafeMutableLabel = LabelAdapter;
pub type UnsafeMutableString = String;

impl LabelAdapter {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn encoded_len(&self) -> usize {
        let mut n = 0usize;
        if !self.name.is_empty() {
            let l = self.name.len();
            n += 1 + l + prost::encoding::encoded_len_varint(l as u64);
        }
        if !self.value.is_empty() {
            let l = self.value.len();
            n += 1 + l + prost::encoding::encoded_len_varint(l as u64);
        }
        n
    }

    pub fn marshal(&self) -> Result<Vec<u8>, prost::EncodeError> {
        let pair = cortexpb::LabelPair {
            name: Bytes::copy_from_slice(self.name.as_bytes()),
            value: Bytes::copy_from_slice(self.value.as_bytes()),
        };
        Ok(pair.encode_to_vec())
    }

    pub fn unmarshal(&mut self, data: &[u8]) -> Result<(), prost::DecodeError> {
        let pair = cortexpb::LabelPair::decode(data)?;
        self.name = String::from_utf8_lossy(&pair.name).into_owned();
        self.value = String::from_utf8_lossy(&pair.value).into_owned();
        Ok(())
    }

    pub fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.name
            .cmp(&other.name)
            .then_with(|| self.value.cmp(&other.value))
    }
}

impl From<&cortexpb::LabelPair> for LabelAdapter {
    fn from(lp: &cortexpb::LabelPair) -> Self {
        Self {
            name: String::from_utf8_lossy(&lp.name).into_owned(),
            value: String::from_utf8_lossy(&lp.value).into_owned(),
        }
    }
}

impl From<&LabelAdapter> for cortexpb::LabelPair {
    fn from(la: &LabelAdapter) -> Self {
        Self {
            name: Bytes::copy_from_slice(la.name.as_bytes()),
            value: Bytes::copy_from_slice(la.value.as_bytes()),
        }
    }
}

/// Prometheus `labels.Label` representation.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PromLabel {
    pub name: String,
    pub value: String,
}

/// Prometheus `labels.Labels` representation with optional contiguous backing buffer (`CopyLabels`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromLabels {
    pub labels: Vec<PromLabel>,
}

impl PromLabels {
    pub fn from_strings(pairs: &[&str]) -> Self {
        assert!(pairs.len().is_multiple_of(2), "pairs must have even length");
        let mut labels = Vec::with_capacity(pairs.len() / 2);
        for [name, value] in pairs.as_chunks::<2>().0 {
            labels.push(PromLabel {
                name: (*name).to_string(),
                value: (*value).to_string(),
            });
        }
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        Self { labels }
    }

    pub fn from_map(map: &BTreeMap<&str, &str>) -> Self {
        let mut labels: Vec<PromLabel> = map
            .iter()
            .map(|(k, v)| PromLabel {
                name: (*k).to_string(),
                value: (*v).to_string(),
            })
            .collect();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        Self { labels }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|l| l.name == name)
            .map(|l| l.value.as_str())
    }
}

/// Compact contiguous-buffer copy of labels (ported from `CopyLabels` in `pkg/mimirpb/compat_slice.go`).
/// Stores all label names and values in one contiguous `String` allocation with byte offsets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContiguousLabels {
    backing: String,
    offsets: Vec<(usize, usize, usize, usize)>,
}

impl ContiguousLabels {
    pub fn from_labels(input: &[PromLabel]) -> Self {
        let total: usize = input.iter().map(|l| l.name.len() + l.value.len()).sum();
        let mut backing = String::with_capacity(total);
        let mut offsets = Vec::with_capacity(input.len());
        for l in input {
            let n_start = backing.len();
            backing.push_str(&l.name);
            let n_end = backing.len();
            let v_start = backing.len();
            backing.push_str(&l.value);
            let v_end = backing.len();
            offsets.push((n_start, n_end, v_start, v_end));
        }
        Self { backing, offsets }
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.offsets.iter().map(|&(ns, ne, vs, ve)| {
            (&self.backing[ns..ne], &self.backing[vs..ve])
        })
    }

    pub fn to_vec(&self) -> Vec<PromLabel> {
        self.iter()
            .map(|(n, v)| PromLabel {
                name: n.to_string(),
                value: v.to_string(),
            })
            .collect()
    }

    pub fn backing_ptr(&self) -> *const u8 {
        self.backing.as_ptr()
    }

    pub fn slices_with_ptrs(&self) -> Vec<(&str, &str)> {
        self.iter().collect()
    }
}

pub fn copy_labels(input: &[PromLabel]) -> ContiguousLabels {
    ContiguousLabels::from_labels(input)
}

pub fn from_label_adapters_to_labels(ls: &[LabelAdapter]) -> PromLabels {
    PromLabels {
        labels: ls
            .iter()
            .map(|l| PromLabel {
                name: l.name.clone(),
                value: l.value.clone(),
            })
            .collect(),
    }
}

pub fn from_label_adapters_to_labels_with_copy(ls: &[LabelAdapter]) -> PromLabels {
    let copied = ContiguousLabels::from_labels(
        &ls.iter()
            .map(|l| PromLabel {
                name: l.name.clone(),
                value: l.value.clone(),
            })
            .collect::<Vec<_>>(),
    );
    PromLabels {
        labels: copied.to_vec(),
    }
}

pub fn from_labels_to_label_adapters(ls: &PromLabels) -> Vec<LabelAdapter> {
    ls.labels
        .iter()
        .map(|l| LabelAdapter {
            name: l.name.clone(),
            value: l.value.clone(),
        })
        .collect()
}

/// Compares two slices of `LabelAdapter`, matching Prometheus `labels.Compare` semantics.
/// Ported from `CompareLabelAdapters` in `pkg/mimirpb/compat.go`.
pub fn compare_label_adapters(a: &[LabelAdapter], b: &[LabelAdapter]) -> i32 {
    let min_len = a.len().min(b.len());
    for i in 0..min_len {
        match a[i].name.cmp(&b[i].name) {
            std::cmp::Ordering::Less => return -1,
            std::cmp::Ordering::Greater => return 1,
            std::cmp::Ordering::Equal => {}
        }
        match a[i].value.cmp(&b[i].value) {
            std::cmp::Ordering::Less => return -1,
            std::cmp::Ordering::Greater => return 1,
            std::cmp::Ordering::Equal => {}
        }
    }
    match a.len().cmp(&b.len()) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Equal => 0,
    }
}

/// Returns whether labels are sorted by their names in lexicographic ascending order,
/// with each name appearing once. Ported from `AreLabelNamesSortedAndUnique` in `pkg/mimirpb/validate.go`.
pub fn are_label_names_sorted_and_unique(labels: &[LabelAdapter]) -> bool {
    for i in 1..labels.len() {
        if labels[i].name <= labels[i - 1].name {
            return false;
        }
    }
    true
}

/// Ported from `NonStableHash` in `pkg/mimirpb/compat.go`.
pub fn non_stable_hash(ls: &[LabelAdapter]) -> u64 {
    let mut buf = [0u8; NON_STABLE_HASH_FAST_PATH_CAP];
    let mut len = 0usize;
    for (i, v) in ls.iter().enumerate() {
        let needed = v.name.len() + v.value.len() + 2;
        if len + needed >= NON_STABLE_HASH_FAST_PATH_CAP {
            let mut hasher = Xxh64::new(0);
            hasher.update(&buf[..len]);
            for rem in &ls[i..] {
                hasher.update(rem.name.as_bytes());
                hasher.update(&[0xff]);
                hasher.update(rem.value.as_bytes());
                hasher.update(&[0xff]);
            }
            return hasher.digest();
        }
        buf[len..len + v.name.len()].copy_from_slice(v.name.as_bytes());
        len += v.name.len();
        buf[len] = 0xff;
        len += 1;
        buf[len..len + v.value.len()].copy_from_slice(v.value.as_bytes());
        len += v.value.len();
        buf[len] = 0xff;
        len += 1;
    }
    xxh64(&buf[..len], 0)
}

pub fn from_label_adapters_to_key_string(ls: &[LabelAdapter]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1024);
    for l in ls {
        buf.push(0xff);
        buf.extend_from_slice(l.name.as_bytes());
        buf.push(0xff);
        buf.extend_from_slice(l.value.as_bytes());
    }
    buf
}

pub fn from_label_adapters_to_string(ls: &[LabelAdapter]) -> String {
    let mut out = String::new();
    let mut metric_name_idx = None;
    for (i, l) in ls.iter().enumerate() {
        if l.name == "__name__" {
            out.push_str(&l.value);
            metric_name_idx = Some(i);
            break;
        }
    }
    let mut count = 0usize;
    for (i, l) in ls.iter().enumerate() {
        if Some(i) == metric_name_idx {
            continue;
        }
        if count == 0 {
            out.push('{');
        } else {
            out.push_str(", ");
        }
        out.push_str(&l.name);
        out.push('=');
        if let Ok(q) = serde_json::to_string(&l.value) {
            out.push_str(&q);
        }
        count += 1;
    }
    if count > 0 {
        out.push('}');
    }
    if out.is_empty() {
        return "{}".to_string();
    }
    out
}

/// Prometheus `model.MetricType` equivalent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelMetricType {
    Counter,
    Gauge,
    Histogram,
    GaugeHistogram,
    Summary,
    Info,
    Stateset,
    Unknown,
}

impl ModelMetricType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ModelMetricType::Counter => "counter",
            ModelMetricType::Gauge => "gauge",
            ModelMetricType::Histogram => "histogram",
            ModelMetricType::GaugeHistogram => "gaugehistogram",
            ModelMetricType::Summary => "summary",
            ModelMetricType::Info => "info",
            ModelMetricType::Stateset => "stateset",
            ModelMetricType::Unknown => "unknown",
        }
    }
}

/// Ported from `MetricMetadataMetricTypeToMetricType` in `pkg/mimirpb/compat.go`.
pub fn metric_metadata_metric_type_to_metric_type(mt: i32) -> ModelMetricType {
    use cortexpb::metric_metadata::MetricType;
    match MetricType::try_from(mt) {
        Ok(MetricType::Unknown) => ModelMetricType::Unknown,
        Ok(MetricType::Counter) => ModelMetricType::Counter,
        Ok(MetricType::Gauge) => ModelMetricType::Gauge,
        Ok(MetricType::Histogram) => ModelMetricType::Histogram,
        Ok(MetricType::Gaugehistogram) => ModelMetricType::GaugeHistogram,
        Ok(MetricType::Summary) => ModelMetricType::Summary,
        Ok(MetricType::Info) => ModelMetricType::Info,
        Ok(MetricType::Stateset) => ModelMetricType::Stateset,
        Err(_) => ModelMetricType::Unknown,
    }
}

/// Prometheus `promql.FPoint` equivalent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PromFPoint {
    pub t: i64,
    pub f: f64,
}

/// Prometheus `promql.HPoint` equivalent.
#[derive(Clone, Debug, PartialEq)]
pub struct PromHPoint {
    pub t: i64,
    pub h: PromFloatHistogram,
}

/// Prometheus `histogram.Span` equivalent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PromSpan {
    pub offset: i32,
    pub length: u32,
}

/// Prometheus `histogram.CounterResetHint` equivalent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum CounterResetHint {
    UnknownCounterReset = 0,
    CounterReset = 1,
    NotCounterReset = 2,
    GaugeType = 3,
}

/// Prometheus `histogram.Histogram` equivalent.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PromHistogram {
    pub counter_reset_hint: i32,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub count: u64,
    pub sum: f64,
    pub positive_spans: Vec<PromSpan>,
    pub negative_spans: Vec<PromSpan>,
    pub positive_buckets: Vec<i64>,
    pub negative_buckets: Vec<i64>,
    pub custom_values: Vec<f64>,
}

/// Prometheus `histogram.FloatHistogram` equivalent.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PromFloatHistogram {
    pub counter_reset_hint: i32,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<PromSpan>,
    pub negative_spans: Vec<PromSpan>,
    pub positive_buckets: Vec<f64>,
    pub negative_buckets: Vec<f64>,
    pub custom_values: Vec<f64>,
}

/// Prometheus `model.HistogramBucket` equivalent.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PromHistogramBucket {
    pub boundaries: i32,
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
}

/// Prometheus `model.SampleHistogram` equivalent.
#[derive(Clone, Debug, PartialEq)]
pub struct PromSampleHistogram {
    pub count: f64,
    pub sum: f64,
    pub buckets: Vec<PromHistogramBucket>,
}

/// Prometheus `model.SampleHistogramPair` equivalent.
#[derive(Clone, Debug, PartialEq)]
pub struct PromSampleHistogramPair {
    pub timestamp: i64,
    pub histogram: Option<PromSampleHistogram>,
}

pub fn from_fpoints_to_float_samples(points: &[PromFPoint]) -> Vec<cortexpb::FloatSample> {
    points
        .iter()
        .map(|p| cortexpb::FloatSample {
            timestamp_ms: p.t,
            value: p.f,
        })
        .collect()
}

pub fn from_float_samples_to_fpoints(samples: &[cortexpb::FloatSample]) -> Vec<PromFPoint> {
    samples
        .iter()
        .map(|s| PromFPoint {
            t: s.timestamp_ms,
            f: s.value,
        })
        .collect()
}

pub fn from_hpoints_to_histograms(points: &[PromHPoint]) -> Vec<cortexpb::FloatHistogramPair> {
    points
        .iter()
        .map(|p| cortexpb::FloatHistogramPair {
            timestamp_ms: p.t,
            histogram: Some(float_histogram_from_prometheus_model(&p.h)),
        })
        .collect()
}

pub fn from_histograms_to_hpoints(histograms: &[cortexpb::FloatHistogramPair]) -> Vec<PromHPoint> {
    histograms
        .iter()
        .map(|p| PromHPoint {
            t: p.timestamp_ms,
            h: p.histogram
                .as_ref()
                .map(float_histogram_to_prometheus_model)
                .unwrap_or_default(),
        })
        .collect()
}

pub fn float_histogram_to_prometheus_model(h: &cortexpb::FloatHistogram) -> PromFloatHistogram {
    PromFloatHistogram {
        counter_reset_hint: h.counter_reset_hint as i32,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        count: h.count,
        sum: h.sum,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|s| PromSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| PromSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_buckets: h.positive_buckets.clone(),
        negative_buckets: h.negative_buckets.clone(),
        custom_values: h.custom_values.clone(),
    }
}

pub fn float_histogram_from_prometheus_model(h: &PromFloatHistogram) -> cortexpb::FloatHistogram {
    cortexpb::FloatHistogram {
        counter_reset_hint: h.counter_reset_hint as u32,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count,
        count: h.count,
        sum: h.sum,
        positive_spans: h
            .positive_spans
            .iter()
            .map(|s| cortexpb::BucketSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        negative_spans: h
            .negative_spans
            .iter()
            .map(|s| cortexpb::BucketSpan {
                offset: s.offset,
                length: s.length,
            })
            .collect(),
        positive_buckets: h.positive_buckets.clone(),
        negative_buckets: h.negative_buckets.clone(),
        custom_values: h.custom_values.clone(),
    }
}

fn from_spans_proto_to_spans(spans: &[cortexpb::BucketSpan]) -> Vec<PromSpan> {
    spans
        .iter()
        .map(|s| PromSpan {
            offset: s.offset,
            length: s.length,
        })
        .collect()
}

fn from_spans_to_spans_proto(spans: &[PromSpan]) -> Vec<cortexpb::BucketSpan> {
    spans
        .iter()
        .map(|s| cortexpb::BucketSpan {
            offset: s.offset,
            length: s.length,
        })
        .collect()
}

pub fn deltas_to_counts(deltas: &[i64]) -> Vec<f64> {
    let mut counts = Vec::with_capacity(deltas.len());
    let mut cur = 0.0f64;
    for &d in deltas {
        cur += d as f64;
        counts.push(cur);
    }
    counts
}

pub fn from_histogram_proto_to_histogram(hp: &cortexpb::Histogram) -> PromHistogram {
    assert!(
        !hp.is_float_histogram(),
        "FromHistogramProtoToHistogram called on float histogram"
    );
    let count = match hp.count {
        Some(cortexpb::histogram::Count::CountInt(c)) => c,
        _ => 0,
    };
    let zero_count = match hp.zero_count {
        Some(cortexpb::histogram::ZeroCount::ZeroCountInt(zc)) => zc,
        _ => 0,
    };
    PromHistogram {
        counter_reset_hint: hp.reset_hint,
        schema: hp.schema,
        zero_threshold: hp.zero_threshold,
        zero_count,
        count,
        sum: hp.sum,
        positive_spans: from_spans_proto_to_spans(&hp.positive_spans),
        negative_spans: from_spans_proto_to_spans(&hp.negative_spans),
        positive_buckets: hp.positive_deltas.clone(),
        negative_buckets: hp.negative_deltas.clone(),
        custom_values: hp.custom_values.clone(),
    }
}

pub fn from_histogram_proto_to_float_histogram(hp: &cortexpb::Histogram) -> PromFloatHistogram {
    assert!(
        !hp.is_float_histogram(),
        "FromHistogramProtoToFloatHistogram called on float histogram"
    );
    let count = match hp.count {
        Some(cortexpb::histogram::Count::CountInt(c)) => c as f64,
        _ => 0.0,
    };
    let zero_count = match hp.zero_count {
        Some(cortexpb::histogram::ZeroCount::ZeroCountInt(zc)) => zc as f64,
        _ => 0.0,
    };
    PromFloatHistogram {
        counter_reset_hint: hp.reset_hint,
        schema: hp.schema,
        zero_threshold: hp.zero_threshold,
        zero_count,
        count,
        sum: hp.sum,
        positive_spans: from_spans_proto_to_spans(&hp.positive_spans),
        negative_spans: from_spans_proto_to_spans(&hp.negative_spans),
        positive_buckets: deltas_to_counts(&hp.positive_deltas),
        negative_buckets: deltas_to_counts(&hp.negative_deltas),
        custom_values: hp.custom_values.clone(),
    }
}

pub fn from_float_histogram_proto_to_float_histogram(
    hp: &cortexpb::Histogram,
) -> PromFloatHistogram {
    assert!(
        hp.is_float_histogram(),
        "FromFloatHistogramProtoToFloatHistogram called on integer histogram"
    );
    let count = match hp.count {
        Some(cortexpb::histogram::Count::CountFloat(c)) => c,
        _ => 0.0,
    };
    let zero_count = match hp.zero_count {
        Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(zc)) => zc,
        _ => 0.0,
    };
    PromFloatHistogram {
        counter_reset_hint: hp.reset_hint,
        schema: hp.schema,
        zero_threshold: hp.zero_threshold,
        zero_count,
        count,
        sum: hp.sum,
        positive_spans: from_spans_proto_to_spans(&hp.positive_spans),
        negative_spans: from_spans_proto_to_spans(&hp.negative_spans),
        positive_buckets: hp.positive_counts.clone(),
        negative_buckets: hp.negative_counts.clone(),
        custom_values: hp.custom_values.clone(),
    }
}

pub fn from_histogram_to_histogram_proto(timestamp: i64, h: &PromHistogram) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountInt(h.count)),
        sum: h.sum,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(h.zero_count)),
        negative_spans: from_spans_to_spans_proto(&h.negative_spans),
        negative_deltas: h.negative_buckets.clone(),
        negative_counts: Vec::new(),
        positive_spans: from_spans_to_spans_proto(&h.positive_spans),
        positive_deltas: h.positive_buckets.clone(),
        positive_counts: Vec::new(),
        reset_hint: h.counter_reset_hint,
        timestamp,
        custom_values: h.custom_values.clone(),
    }
}

pub fn from_float_histogram_to_histogram_proto(
    timestamp: i64,
    fh: &PromFloatHistogram,
) -> cortexpb::Histogram {
    cortexpb::Histogram {
        count: Some(cortexpb::histogram::Count::CountFloat(fh.count)),
        sum: fh.sum,
        schema: fh.schema,
        zero_threshold: fh.zero_threshold,
        zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(fh.zero_count)),
        negative_spans: from_spans_to_spans_proto(&fh.negative_spans),
        negative_deltas: Vec::new(),
        negative_counts: fh.negative_buckets.clone(),
        positive_spans: from_spans_to_spans_proto(&fh.positive_spans),
        positive_deltas: Vec::new(),
        positive_counts: fh.positive_buckets.clone(),
        reset_hint: fh.counter_reset_hint,
        timestamp,
        custom_values: fh.custom_values.clone(),
    }
}

pub fn from_histogram_proto_to_prom_histogram(hp: &cortexpb::Histogram) -> PromSampleHistogram {
    if hp.is_float_histogram() {
        from_float_histogram_to_prom_histogram(&from_float_histogram_proto_to_float_histogram(hp))
    } else {
        from_histogram_to_prom_histogram(&from_histogram_proto_to_histogram(hp))
    }
}

pub fn from_histogram_to_prom_histogram(h: &PromHistogram) -> PromSampleHistogram {
    let fh = PromFloatHistogram {
        counter_reset_hint: h.counter_reset_hint,
        schema: h.schema,
        zero_threshold: h.zero_threshold,
        zero_count: h.zero_count as f64,
        count: h.count as f64,
        sum: h.sum,
        positive_spans: h.positive_spans.clone(),
        negative_spans: h.negative_spans.clone(),
        positive_buckets: deltas_to_counts(&h.positive_buckets),
        negative_buckets: deltas_to_counts(&h.negative_buckets),
        custom_values: h.custom_values.clone(),
    };
    from_float_histogram_to_prom_histogram(&fh)
}

#[inline]
fn ldexp(frac: f64, exp: i32) -> f64 {
    frac * 2.0_f64.powi(exp)
}

fn get_bound_exponential(idx: i32, schema: i32) -> f64 {
    if schema < 0 {
        let exp = (idx as i64) << (-schema);
        if exp == 1024 {
            return f64::MAX;
        }
        return ldexp(1.0, exp as i32);
    }

    let frac_idx = (idx & ((1 << schema) - 1)) as usize;
    let frac = match schema {
        0 => 0.5_f64,
        1 => [0.5_f64, std::f64::consts::FRAC_1_SQRT_2][frac_idx],
        2 => [
            0.5_f64,
            0.5946035575013605,
            std::f64::consts::FRAC_1_SQRT_2,
            0.8408964152537144,
        ][frac_idx],
        _ => {
            // Exact formula matching Prometheus precomputed bounds table: 2^((frac_idx / 2^schema) - 1)
            2.0_f64.powf((frac_idx as f64) / ((1u32 << schema) as f64) - 1.0)
        }
    };
    let exp = (idx >> schema) + 1;
    if frac == 0.5 && exp == 1025 {
        return f64::MAX;
    }
    ldexp(frac, exp)
}

fn get_bound(idx: i32, schema: i32, custom_values: &[f64]) -> f64 {
    if schema == cortexpb::NATIVE_HISTOGRAMS_WITH_CUSTOM_BUCKETS_SCHEMA {
        let len = custom_values.len() as i32;
        if idx == len {
            f64::INFINITY
        } else if idx == -1 {
            f64::NEG_INFINITY
        } else {
            custom_values[idx as usize]
        }
    } else {
        get_bound_exponential(idx, schema)
    }
}

fn collect_buckets(
    spans: &[PromSpan],
    buckets: &[f64],
    schema: i32,
    positive: bool,
    custom_values: &[f64],
) -> Vec<PromHistogramBucket> {
    let mut out = Vec::with_capacity(buckets.len());
    let mut idx = 0i32;
    let mut bucket_i = 0usize;
    for span in spans {
        idx += span.offset;
        for _ in 0..span.length {
            let count = buckets[bucket_i];
            bucket_i += 1;
            let (lower, upper) = if positive {
                (
                    get_bound(idx - 1, schema, custom_values),
                    get_bound(idx, schema, custom_values),
                )
            } else {
                (
                    -get_bound(idx, schema, custom_values),
                    -get_bound(idx - 1, schema, custom_values),
                )
            };
            let (lower_inc, upper_inc) = if schema == cortexpb::NATIVE_HISTOGRAMS_WITH_CUSTOM_BUCKETS_SCHEMA {
                (idx == 0, true)
            } else {
                (lower < 0.0, upper > 0.0)
            };
            let boundaries = match (lower_inc, upper_inc) {
                (true, true) => 3,
                (true, false) => 1,
                (false, true) => 0,
                (false, false) => 2,
            };
            out.push(PromHistogramBucket {
                boundaries,
                lower,
                upper,
                count,
            });
            idx += 1;
        }
    }
    out
}

/// Converts `PromFloatHistogram` to `PromSampleHistogram` in exact Prometheus bucket order
/// (negative buckets ascending, zero bucket, positive buckets ascending), omitting empty buckets.
/// Ported from `FromFloatHistogramToPromHistogram` in `pkg/mimirpb/compat.go`.
pub fn from_float_histogram_to_prom_histogram(h: &PromFloatHistogram) -> PromSampleHistogram {
    let mut neg = collect_buckets(
        &h.negative_spans,
        &h.negative_buckets,
        h.schema,
        false,
        &h.custom_values,
    );
    neg.reverse(); // negative buckets iterate from lowest (-inf) towards 0

    let mut buckets = Vec::with_capacity(neg.len() + 1 + h.positive_buckets.len());
    for b in neg {
        if b.count != 0.0 {
            buckets.push(b);
        }
    }
    if h.zero_count != 0.0
        && h.schema != cortexpb::NATIVE_HISTOGRAMS_WITH_CUSTOM_BUCKETS_SCHEMA
    {
        buckets.push(PromHistogramBucket {
            boundaries: 3,
            lower: -h.zero_threshold,
            upper: h.zero_threshold,
            count: h.zero_count,
        });
    }
    let pos = collect_buckets(
        &h.positive_spans,
        &h.positive_buckets,
        h.schema,
        true,
        &h.custom_values,
    );
    for b in pos {
        if b.count != 0.0 {
            buckets.push(b);
        }
    }

    PromSampleHistogram {
        count: h.count,
        sum: h.sum,
        buckets,
    }
}

pub fn from_float_histogram_to_sample_histogram(
    h: &PromFloatHistogram,
) -> cortexpb::SampleHistogram {
    let sh = from_float_histogram_to_prom_histogram(h);
    cortexpb::SampleHistogram {
        count: sh.count,
        sum: sh.sum,
        buckets: sh
            .buckets
            .into_iter()
            .map(|b| cortexpb::HistogramBucket {
                boundaries: b.boundaries,
                lower: b.lower,
                upper: b.upper,
                count: b.count,
            })
            .collect(),
    }
}

fn format_prom_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v == f64::INFINITY {
        "+Inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else if v == 0.0 {
        "0".to_string()
    } else {
        v.to_string()
    }
}

fn parse_prom_float(s: &str) -> Result<f64, String> {
    match s {
        "NaN" => Ok(f64::NAN),
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => s
            .parse::<f64>()
            .map_err(|e| format!("failed to parse float '{s}': {e}")),
    }
}

/// Ported from `FloatSample.MarshalJSON` in `pkg/mimirpb/compat.go`.
pub fn marshal_float_sample_json(sample: cortexpb::FloatSample) -> Result<String, String> {
    if IS_TESTING_JSON.load(Ordering::Relaxed) && sample.value.is_nan() {
        return Err("test sample".to_string());
    }
    let t = (sample.timestamp_ms as f64) / 1000.0;
    let t_str = if t == 0.0 {
        "0".to_string()
    } else {
        t.to_string()
    };
    let v_str = serde_json::to_string(&format_prom_float(sample.value)).map_err(|e| e.to_string())?;
    Ok(format!("[{t_str},{v_str}]"))
}

/// Ported from `FloatSample.UnmarshalJSON` in `pkg/mimirpb/compat.go`.
pub fn unmarshal_float_sample_json(data: &[u8]) -> Result<cortexpb::FloatSample, String> {
    let val: serde_json::Value = serde_json::from_slice(data).map_err(|e| e.to_string())?;
    let arr = val
        .as_array()
        .ok_or_else(|| "expected JSON array".to_string())?;
    if arr.len() != 2 {
        return Err("expected 2-element array".to_string());
    }
    let t_sec = arr[0]
        .as_f64()
        .ok_or_else(|| "expected timestamp number".to_string())?;
    let v_str = arr[1]
        .as_str()
        .ok_or_else(|| "expected string value".to_string())?;
    let value = parse_prom_float(v_str)?;
    if IS_TESTING_JSON.load(Ordering::Relaxed) && value.is_nan() {
        return Err("test sample".to_string());
    }
    let timestamp_ms = (t_sec * 1000.0) as i64;
    Ok(cortexpb::FloatSample {
        value,
        timestamp_ms,
    })
}

/// Ported from `SampleHistogramPair.MarshalJSON` in `pkg/mimirpb/compat.go`.
pub fn marshal_sample_histogram_pair_json(
    pair: &cortexpb::SampleHistogramPair,
) -> Result<String, String> {
    let t = (pair.timestamp as f64) / 1000.0;
    let t_str = if t == 0.0 {
        "0".to_string()
    } else {
        t.to_string()
    };
    let hist = pair
        .histogram
        .as_ref()
        .ok_or_else(|| "missing histogram".to_string())?;
    let mut buckets_json = String::from("[");
    for (i, b) in hist.buckets.iter().enumerate() {
        if i > 0 {
            buckets_json.push(',');
        }
        buckets_json.push_str(&format!(
            "[{},{},{},{}]",
            b.boundaries,
            serde_json::to_string(&format_prom_float(b.lower)).unwrap(),
            serde_json::to_string(&format_prom_float(b.upper)).unwrap(),
            serde_json::to_string(&format_prom_float(b.count)).unwrap(),
        ));
    }
    buckets_json.push(']');
    let count_str = serde_json::to_string(&format_prom_float(hist.count)).unwrap();
    let sum_str = serde_json::to_string(&format_prom_float(hist.sum)).unwrap();
    Ok(format!(
        "[{t_str},{{\"count\":{count_str},\"sum\":{sum_str},\"buckets\":{buckets_json}}}]"
    ))
}

/// Ported from `SampleHistogramPair.UnmarshalJSON` in `pkg/mimirpb/compat.go`.
pub fn unmarshal_sample_histogram_pair_json(
    data: &[u8],
) -> Result<cortexpb::SampleHistogramPair, String> {
    let val: serde_json::Value = serde_json::from_slice(data).map_err(|e| e.to_string())?;
    let arr = val
        .as_array()
        .ok_or_else(|| "expected JSON array".to_string())?;
    if arr.len() != 2 {
        return Err("expected 2-element array".to_string());
    }
    let t_sec = arr[0]
        .as_f64()
        .ok_or_else(|| "expected timestamp number".to_string())?;
    let obj = arr[1]
        .as_object()
        .ok_or_else(|| "expected histogram object".to_string())?;
    let count = parse_prom_float(
        obj.get("count")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "missing count".to_string())?,
    )?;
    let sum = parse_prom_float(
        obj.get("sum")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "missing sum".to_string())?,
    )?;
    let buckets_arr = obj
        .get("buckets")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing buckets".to_string())?;
    let mut buckets = Vec::with_capacity(buckets_arr.len());
    for b in buckets_arr {
        let ba = b
            .as_array()
            .ok_or_else(|| "expected bucket array".to_string())?;
        if ba.len() != 4 {
            return Err("expected 4-element bucket".to_string());
        }
        let boundaries = ba[0]
            .as_i64()
            .ok_or_else(|| "expected boundaries int".to_string())? as i32;
        let lower = parse_prom_float(
            ba[1].as_str()
                .ok_or_else(|| "expected lower string".to_string())?,
        )?;
        let upper = parse_prom_float(
            ba[2].as_str()
                .ok_or_else(|| "expected upper string".to_string())?,
        )?;
        let bcount = parse_prom_float(
            ba[3].as_str()
                .ok_or_else(|| "expected count string".to_string())?,
        )?;
        buckets.push(cortexpb::HistogramBucket {
            boundaries,
            lower,
            upper,
            count: bcount,
        });
    }
    Ok(cortexpb::SampleHistogramPair {
        timestamp: (t_sec * 1000.0) as i64,
        histogram: Some(cortexpb::SampleHistogram {
            count,
            sum,
            buckets,
        }),
    })
}

/// Ported from `PreallocatingMetric` in `pkg/mimirpb/compat.go`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PreallocatingMetric {
    pub metric: cortexpb::Metric,
}

impl PreallocatingMetric {
    pub fn unmarshal(&mut self, data: &[u8]) -> Result<(), prost::DecodeError> {
        if let Some(num_labels) = Self::labels_count(data) {
            if num_labels > 0 {
                self.metric.labels = Vec::with_capacity(num_labels);
            }
        }
        self.metric.merge(data)
    }

    pub fn marshal(&self) -> Vec<u8> {
        self.metric.encode_to_vec()
    }

    /// Pre-scans protobuf bytes to count `Metric.labels` (tag 1, wire type 2) without allocating.
    pub fn labels_count(data: &[u8]) -> Option<usize> {
        let mut idx = 0usize;
        let len = data.len();
        let mut num_labels = 0usize;
        while idx < len {
            let mut wire = 0u64;
            let mut shift = 0u32;
            loop {
                if shift >= 64 || idx >= len {
                    return None;
                }
                let b = data[idx];
                idx += 1;
                wire |= ((b & 0x7f) as u64) << shift;
                if b < 0x80 {
                    break;
                }
                shift += 7;
            }
            let field_num = (wire >> 3) as i32;
            let wire_type = (wire & 0x7) as u8;
            if wire_type == 4 || field_num <= 0 {
                return None;
            }
            if field_num == 1 {
                if wire_type != 2 {
                    return None;
                }
                let mut msglen = 0usize;
                let mut shift = 0u32;
                loop {
                    if shift >= 64 || idx >= len {
                        return None;
                    }
                    let b = data[idx];
                    idx += 1;
                    msglen |= ((b & 0x7f) as usize) << shift;
                    if b < 0x80 {
                        break;
                    }
                    shift += 7;
                }
                let post_idx = idx.checked_add(msglen)?;
                if post_idx > len {
                    return None;
                }
                num_labels += 1;
                idx = post_idx;
            } else {
                break;
            }
        }
        Some(num_labels)
    }
}
