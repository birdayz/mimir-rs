//! Prometheus histograms with native (sparse, exponential) buckets, as exposed by Go
//! `client_golang` when `HistogramOpts.NativeHistogramBucketFactor > 1`.
//!
//! Ported from github.com/prometheus/client_golang/prometheus/histogram.go
//! (`newHistogram`, `histogramCounts.observe`, `limitBuckets`, `maybeReset`, `doubleBucketWidth`,
//! `Write`, `pickSchema`, `makeBuckets`, `nativeHistogramBounds`).
//!
//! The `prometheus` crate (0.13) only models classic histograms. [`HistogramVec`] implements
//! [`prometheus::core::Collector`] and emits a regular classic `proto::Histogram` (so the text and
//! OpenMetrics encoders render exactly what Go renders: classic buckets only, or just the `+Inf`
//! bucket for native-only histograms), while the native fields of `io.prometheus.client.Histogram`
//! (`schema`, `zero_threshold`, `zero_count`, `negative_span`, `negative_delta`, `positive_span`,
//! `positive_delta`, `created_timestamp`) are attached as protobuf unknown fields with their
//! upstream field numbers. The protobuf exposition format therefore carries the native histogram
//! bit-compatibly with Go.
//!
//! Concurrency differs from Go's lock-free hot/cold double buffering (a mutex per child is used),
//! which is not externally observable.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use prometheus::core::{Collector, Desc};
use prometheus::proto;

/// `DefNativeHistogramZeroThreshold` (2^-128).
pub const DEF_NATIVE_HISTOGRAM_ZERO_THRESHOLD: f64 = 2.938_735_877_055_719e-39;

const NATIVE_HISTOGRAM_SCHEMA_MAXIMUM: i32 = 8;
const NATIVE_HISTOGRAM_SCHEMA_MINIMUM: i32 = -4;

// io.prometheus.client.Histogram field numbers (client_model metrics.proto).
const FIELD_SCHEMA: u32 = 5;
const FIELD_ZERO_THRESHOLD: u32 = 6;
const FIELD_ZERO_COUNT: u32 = 7;
const FIELD_NEGATIVE_SPAN: u32 = 9;
const FIELD_NEGATIVE_DELTA: u32 = 10;
const FIELD_POSITIVE_SPAN: u32 = 12;
const FIELD_POSITIVE_DELTA: u32 = 13;
const FIELD_CREATED_TIMESTAMP: u32 = 15;

/// Options mirroring Go `prometheus.HistogramOpts` (classic + native parts).
#[derive(Clone, Debug)]
pub struct HistogramOpts {
    pub name: String,
    pub help: String,
    pub const_labels: HashMap<String, String>,
    /// Classic bucket upper bounds. Empty with `native_histogram_bucket_factor <= 1` means
    /// `prometheus.DefBuckets`; empty with a native factor means "no classic buckets".
    pub buckets: Vec<f64>,
    pub native_histogram_bucket_factor: f64,
    /// 0 means [`DEF_NATIVE_HISTOGRAM_ZERO_THRESHOLD`]; negative means an actual 0 threshold.
    pub native_histogram_zero_threshold: f64,
    pub native_histogram_max_bucket_number: u32,
    pub native_histogram_min_reset_duration: Duration,
}

impl HistogramOpts {
    pub fn new(name: impl Into<String>, help: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            help: help.into(),
            const_labels: HashMap::new(),
            buckets: Vec::new(),
            native_histogram_bucket_factor: 0.0,
            native_histogram_zero_threshold: 0.0,
            native_histogram_max_bucket_number: 0,
            native_histogram_min_reset_duration: Duration::ZERO,
        }
    }

    pub fn buckets(mut self, buckets: Vec<f64>) -> Self {
        self.buckets = buckets;
        self
    }

    pub fn native(mut self, factor: f64, max_buckets: u32, min_reset: Duration) -> Self {
        self.native_histogram_bucket_factor = factor;
        self.native_histogram_max_bucket_number = max_buckets;
        self.native_histogram_min_reset_duration = min_reset;
        self
    }

    pub fn const_label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.const_labels.insert(name.into(), value.into());
        self
    }
}

/// Go `prometheus.DefBuckets`.
pub const DEF_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Go `prometheus.ExponentialBuckets(start, factor, count)`.
pub fn exponential_buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    let mut out = Vec::with_capacity(count);
    let mut next = start;
    for _ in 0..count {
        out.push(next);
        next *= factor;
    }
    out
}

/// Go `pickSchema`.
pub fn pick_schema(bucket_factor: f64) -> i32 {
    assert!(bucket_factor > 1.0, "bucketFactor {bucket_factor} is <=1");
    let floor = bucket_factor.log2().log2().floor();
    if floor <= -8.0 {
        NATIVE_HISTOGRAM_SCHEMA_MAXIMUM
    } else if floor >= 4.0 {
        NATIVE_HISTOGRAM_SCHEMA_MINIMUM
    } else {
        -(floor as i32)
    }
}

/// Go `math.Frexp`: `f == frac * 2^exp` with `|frac|` in `[0.5, 1)`.
fn frexp(f: f64) -> (f64, i32) {
    if f == 0.0 || f.is_nan() || f.is_infinite() {
        return (f, 0);
    }
    let (mut f, mut exp) = (f, 0i32);
    // Normalize subnormals.
    if f.abs() < f64::MIN_POSITIVE {
        f *= (1u64 << 52) as f64;
        exp = -52;
    }
    let bits = f.to_bits();
    exp += ((bits >> 52) & 0x7ff) as i32 - 1022;
    let frac = f64::from_bits((bits & !(0x7ffu64 << 52)) | (1022u64 << 52));
    (frac, exp)
}

/// Native bucket key of `v` (assumed not NaN) for `schema`, mirroring `histogramCounts.observe`.
pub fn native_bucket_key(v: f64, schema: i32) -> i32 {
    let (mut v, mut is_inf) = (v, false);
    if v.is_infinite() {
        v = if v > 0.0 { f64::MAX } else { -f64::MAX };
        is_inf = true;
    }
    let (frac, exp) = frexp(v.abs());
    let mut key = if schema > 0 {
        let bounds = NATIVE_HISTOGRAM_BOUNDS[schema as usize];
        let idx = bounds.partition_point(|b| *b < frac) as i32;
        idx + (exp - 1) * bounds.len() as i32
    } else {
        let mut key = exp;
        if frac == 0.5 {
            key -= 1;
        }
        let offset = (1i32 << -schema) - 1;
        (key + offset) >> -schema
    };
    if is_inf {
        key += 1;
    }
    key
}

#[derive(Clone)]
struct Counts {
    /// Non-cumulative classic bucket counts; the last slot is the implicit `+Inf` bucket.
    buckets: Vec<u64>,
    count: u64,
    sum: f64,
    schema: i32,
    zero_threshold: f64,
    zero_count: u64,
    positive: BTreeMap<i32, i64>,
    negative: BTreeMap<i32, i64>,
}

impl Counts {
    fn new(num_classic: usize, schema: i32, zero_threshold: f64) -> Self {
        Self {
            buckets: vec![0; num_classic + 1],
            count: 0,
            sum: 0.0,
            schema,
            zero_threshold,
            zero_count: 0,
            positive: BTreeMap::new(),
            negative: BTreeMap::new(),
        }
    }

    fn num_native_buckets(&self) -> usize {
        self.positive.len() + self.negative.len()
    }
}

struct Inner {
    counts: Counts,
    last_reset: Instant,
    last_reset_wall: SystemTime,
    reset_scheduled: bool,
}

struct Shared {
    upper_bounds: Vec<f64>,
    /// `i32::MIN` marks "no native buckets" (Go uses `math.MinInt32` the same way).
    schema: i32,
    zero_threshold: f64,
    max_buckets: u32,
    min_reset: Duration,
}

/// A single histogram child (Go `prometheus.Histogram`).
pub struct Histogram {
    shared: Arc<Shared>,
    inner: Mutex<Inner>,
    label_pairs: Vec<proto::LabelPair>,
}

impl Histogram {
    fn new(shared: Arc<Shared>, label_pairs: Vec<proto::LabelPair>) -> Self {
        let counts = Counts::new(shared.upper_bounds.len(), shared.schema, shared.zero_threshold);
        Self {
            inner: Mutex::new(Inner {
                counts,
                last_reset: Instant::now(),
                last_reset_wall: SystemTime::now(),
                reset_scheduled: false,
            }),
            shared,
            label_pairs,
        }
    }

    fn find_bucket(&self, v: f64) -> usize {
        let ub = &self.shared.upper_bounds;
        // Matches Go's findBucket: index of the first bound >= v, or len for +Inf.
        ub.partition_point(|b| *b < v)
    }

    /// Go `Histogram.Observe`.
    pub fn observe(&self, v: f64) {
        let bucket = self.find_bucket(v);
        let mut inner = self.inner.lock();
        self.maybe_run_scheduled_reset(&mut inner);
        let do_sparse = self.shared.schema != i32::MIN && !v.is_nan();
        let created = Self::observe_counts(&mut inner.counts, v, bucket, do_sparse);
        if do_sparse && created {
            self.limit_buckets(&mut inner, v, bucket);
        }
    }

    /// Returns whether a new native bucket was created.
    fn observe_counts(c: &mut Counts, v: f64, bucket: usize, do_sparse: bool) -> bool {
        if bucket < c.buckets.len() {
            c.buckets[bucket] += 1;
        }
        c.sum += v;
        let mut created = false;
        if do_sparse {
            let key = native_bucket_key(v, c.schema);
            if v > c.zero_threshold {
                created = add_to_bucket(&mut c.positive, key, 1);
            } else if v < -c.zero_threshold {
                created = add_to_bucket(&mut c.negative, key, 1);
            } else {
                c.zero_count += 1;
            }
        }
        c.count += 1;
        created
    }

    fn reset_counts(&self, inner: &mut Inner) {
        inner.counts = Counts::new(
            self.shared.upper_bounds.len(),
            self.shared.schema,
            self.shared.zero_threshold,
        );
    }

    /// Go schedules `h.reset` via `afterFunc` at `lastResetTime + minResetDuration`; applying it
    /// lazily on the next observation or collection is observably identical.
    fn maybe_run_scheduled_reset(&self, inner: &mut Inner) {
        if inner.reset_scheduled
            && inner.last_reset.elapsed() >= self.shared.min_reset
        {
            self.reset_counts(inner);
            inner.reset_scheduled = false;
            inner.last_reset = Instant::now();
            inner.last_reset_wall = SystemTime::now();
        }
    }

    fn limit_buckets(&self, inner: &mut Inner, value: f64, bucket: usize) {
        let max = self.shared.max_buckets as usize;
        if max == 0 || inner.counts.num_native_buckets() <= max {
            return;
        }
        // maybeReset.
        let min_reset = self.shared.min_reset;
        if !min_reset.is_zero()
            && !inner.reset_scheduled
            && inner.last_reset.elapsed() >= min_reset
        {
            self.reset_counts(inner);
            Self::observe_counts(&mut inner.counts, value, bucket, true);
            inner.last_reset = Instant::now();
            inner.last_reset_wall = SystemTime::now();
            return;
        }
        if !min_reset.is_zero() && !inner.reset_scheduled {
            inner.reset_scheduled = true;
        }
        // maybeWidenZeroBucket: NativeHistogramMaxZeroThreshold is never set by Mimir/dskit,
        // so widening never applies.
        // doubleBucketWidth.
        let c = &mut inner.counts;
        if c.schema == NATIVE_HISTOGRAM_SCHEMA_MINIMUM {
            return;
        }
        c.schema -= 1;
        let merge = |m: &BTreeMap<i32, i64>| -> BTreeMap<i32, i64> {
            let mut out = BTreeMap::new();
            for (&k, &v) in m {
                let mut key = k;
                if key > 0 {
                    key += 1;
                }
                key /= 2;
                *out.entry(key).or_insert(0) += v;
            }
            out
        };
        c.positive = merge(&c.positive);
        c.negative = merge(&c.negative);
    }

    fn write(&self) -> proto::Metric {
        let mut inner = self.inner.lock();
        self.maybe_run_scheduled_reset(&mut inner);
        let c = &inner.counts;
        let mut h = proto::Histogram::default();
        h.set_sample_count(c.count);
        h.set_sample_sum(c.sum);
        let mut cum = 0u64;
        let mut buckets = Vec::with_capacity(self.shared.upper_bounds.len());
        for (i, ub) in self.shared.upper_bounds.iter().enumerate() {
            cum += c.buckets[i];
            let mut b = proto::Bucket::default();
            b.set_cumulative_count(cum);
            b.set_upper_bound(*ub);
            buckets.push(b);
        }
        h.set_bucket(buckets.into());

        let uf = &mut h.unknown_fields;
        // created_timestamp (google.protobuf.Timestamp).
        let created = inner
            .last_reset_wall
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let mut ts = Vec::new();
        put_tag(&mut ts, 1, 0);
        put_varint(&mut ts, created.as_secs());
        put_tag(&mut ts, 2, 0);
        put_varint(&mut ts, created.subsec_nanos() as u64);
        if self.shared.schema != i32::MIN {
            uf.add_fixed64(FIELD_ZERO_THRESHOLD, c.zero_threshold.to_bits());
            uf.add_varint(FIELD_SCHEMA, zigzag32(c.schema) as u64);
            uf.add_varint(FIELD_ZERO_COUNT, c.zero_count);
            let (neg_spans, neg_deltas) = make_buckets(&c.negative);
            let (mut pos_spans, pos_deltas) = make_buckets(&c.positive);
            // Add a no-op span to a histogram without observations and with a zero threshold of
            // zero. Otherwise, a native histogram would look like a classic histogram to scrapers.
            if c.zero_threshold == 0.0
                && c.zero_count == 0
                && pos_spans.is_empty()
                && neg_spans.is_empty()
            {
                pos_spans.push((0, 0));
            }
            for (offset, length) in neg_spans {
                uf.add_length_delimited(FIELD_NEGATIVE_SPAN, encode_span(offset, length));
            }
            for d in neg_deltas {
                uf.add_varint(FIELD_NEGATIVE_DELTA, zigzag64(d));
            }
            for (offset, length) in pos_spans {
                uf.add_length_delimited(FIELD_POSITIVE_SPAN, encode_span(offset, length));
            }
            for d in pos_deltas {
                uf.add_varint(FIELD_POSITIVE_DELTA, zigzag64(d));
            }
        }
        uf.add_length_delimited(FIELD_CREATED_TIMESTAMP, ts);

        let mut m = proto::Metric::default();
        m.set_label(self.label_pairs.clone().into());
        m.set_histogram(h);
        m
    }
}

fn add_to_bucket(m: &mut BTreeMap<i32, i64>, key: i32, inc: i64) -> bool {
    match m.get_mut(&key) {
        Some(v) => {
            *v += inc;
            false
        }
        None => {
            m.insert(key, inc);
            true
        }
    }
}

/// Go `makeBuckets`: spans `(offset, length)` and count deltas. Gaps of one or two buckets are
/// filled with empty buckets instead of starting a new span.
pub fn make_buckets(m: &BTreeMap<i32, i64>) -> (Vec<(i32, u32)>, Vec<i64>) {
    let mut spans: Vec<(i32, u32)> = Vec::new();
    let mut deltas = Vec::new();
    let mut prev_count = 0i64;
    let mut next_i = 0i32;
    for (n, (&i, &count)) in m.iter().enumerate() {
        let i_delta = i - next_i;
        if n == 0 || i_delta > 2 {
            spans.push((i_delta, 0));
        } else {
            for _ in 0..i_delta {
                spans.last_mut().expect("span exists").1 += 1;
                deltas.push(-prev_count);
                prev_count = 0;
            }
        }
        spans.last_mut().expect("span exists").1 += 1;
        deltas.push(count - prev_count);
        prev_count = count;
        next_i = i + 1;
    }
    (spans, deltas)
}

fn zigzag32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

fn zigzag64(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn put_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn put_tag(buf: &mut Vec<u8>, field: u32, wire_type: u32) {
    put_varint(buf, ((field << 3) | wire_type) as u64);
}

fn encode_span(offset: i32, length: u32) -> Vec<u8> {
    let mut b = Vec::with_capacity(8);
    put_tag(&mut b, 1, 0);
    put_varint(&mut b, zigzag32(offset) as u64);
    put_tag(&mut b, 2, 0);
    put_varint(&mut b, length as u64);
    b
}

/// Go `prometheus.HistogramVec` with native histogram support.
#[derive(Clone)]
pub struct HistogramVec {
    desc: Arc<Desc>,
    label_names: Arc<Vec<String>>,
    shared: Arc<Shared>,
    const_pairs: Arc<Vec<proto::LabelPair>>,
    /// Children keyed by the xxhash64 of their label values; each bucket holds the (rarely more
    /// than one) children with that hash. Lookups on the hot path do not allocate.
    children: Arc<RwLock<ChildMap>>,
}

/// Label values → child histogram, bucketed by [`label_values_hash`].
type ChildMap = HashMap<u64, Vec<(Vec<String>, Arc<Histogram>)>>;

/// Hash of a label value tuple (values separated by 0xff, which never occurs in UTF-8).
fn label_values_hash<S: AsRef<str>>(values: &[S]) -> u64 {
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    for v in values {
        h.update(v.as_ref().as_bytes());
        h.update(&[0xff]);
    }
    h.digest()
}

fn same_values(a: &[String], b: &[&str]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}

impl HistogramVec {
    pub fn new(opts: HistogramOpts, label_names: &[&str]) -> prometheus::Result<Self> {
        let desc = Desc::new(
            opts.name.clone(),
            opts.help.clone(),
            label_names.iter().map(|s| s.to_string()).collect(),
            opts.const_labels.clone(),
        )?;
        let mut upper = opts.buckets.clone();
        if upper.is_empty() && opts.native_histogram_bucket_factor <= 1.0 {
            upper = DEF_BUCKETS.to_vec();
        }
        for i in 0..upper.len() {
            if i + 1 < upper.len() {
                if upper[i] >= upper[i + 1] {
                    return Err(prometheus::Error::Msg(format!(
                        "histogram buckets must be in increasing order: {} >= {}",
                        upper[i],
                        upper[i + 1]
                    )));
                }
            } else if upper[i].is_infinite() && upper[i] > 0.0 {
                upper.pop();
                break;
            }
        }
        let (schema, zero_threshold) = if opts.native_histogram_bucket_factor <= 1.0 {
            (i32::MIN, 0.0)
        } else {
            let zt = if opts.native_histogram_zero_threshold > 0.0 {
                opts.native_histogram_zero_threshold
            } else if opts.native_histogram_zero_threshold == 0.0 {
                DEF_NATIVE_HISTOGRAM_ZERO_THRESHOLD
            } else {
                0.0
            };
            (pick_schema(opts.native_histogram_bucket_factor), zt)
        };
        let mut const_pairs: Vec<proto::LabelPair> = opts
            .const_labels
            .iter()
            .map(|(k, v)| {
                let mut lp = proto::LabelPair::default();
                lp.set_name(k.clone());
                lp.set_value(v.clone());
                lp
            })
            .collect();
        const_pairs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
        Ok(Self {
            desc: Arc::new(desc),
            label_names: Arc::new(label_names.iter().map(|s| s.to_string()).collect()),
            shared: Arc::new(Shared {
                upper_bounds: upper,
                schema,
                zero_threshold,
                max_buckets: opts.native_histogram_max_bucket_number,
                min_reset: opts.native_histogram_min_reset_duration,
            }),
            const_pairs: Arc::new(const_pairs),
            children: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Go `HistogramVec.WithLabelValues`. Panics on label cardinality mismatch, like Go.
    pub fn with_label_values(&self, values: &[&str]) -> Arc<Histogram> {
        assert_eq!(
            values.len(),
            self.label_names.len(),
            "inconsistent label cardinality: expected {} label values but got {}",
            self.label_names.len(),
            values.len()
        );
        let hash = label_values_hash(values);
        if let Some(h) = self
            .children
            .read()
            .get(&hash)
            .and_then(|b| b.iter().find(|(k, _)| same_values(k, values)))
        {
            return Arc::clone(&h.1);
        }
        let mut children = self.children.write();
        let bucket = children.entry(hash).or_default();
        if let Some((_, h)) = bucket.iter().find(|(k, _)| same_values(k, values)) {
            return Arc::clone(h);
        }
        let key: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        let mut pairs = (*self.const_pairs).clone();
        for (name, value) in self.label_names.iter().zip(key.iter()) {
            let mut lp = proto::LabelPair::default();
            lp.set_name(name.clone());
            lp.set_value(value.clone());
            pairs.push(lp);
        }
        pairs.sort_by(|a, b| a.get_name().cmp(b.get_name()));
        let h = Arc::new(Histogram::new(Arc::clone(&self.shared), pairs));
        bucket.push((key, Arc::clone(&h)));
        h
    }

    /// Go `HistogramVec.DeleteLabelValues`.
    pub fn remove_label_values(&self, values: &[&str]) -> bool {
        let hash = label_values_hash(values);
        let mut children = self.children.write();
        let Some(bucket) = children.get_mut(&hash) else {
            return false;
        };
        let before = bucket.len();
        bucket.retain(|(k, _)| !same_values(k, values));
        let removed = bucket.len() != before;
        if bucket.is_empty() {
            children.remove(&hash);
        }
        removed
    }
}

impl Collector for HistogramVec {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let children: Vec<Arc<Histogram>> = self
            .children
            .read()
            .values()
            .flat_map(|b| b.iter().map(|(_, h)| Arc::clone(h)))
            .collect();
        if children.is_empty() && !self.label_names.is_empty() {
            return Vec::new();
        }
        let mut mf = proto::MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(proto::MetricType::HISTOGRAM);
        let metrics: Vec<proto::Metric> = children.iter().map(|h| h.write()).collect();
        mf.set_metric(metrics.into());
        vec![mf]
    }
}

/// A label-less histogram (Go `prometheus.NewHistogram`).
#[derive(Clone)]
pub struct SingleHistogram {
    vec: HistogramVec,
    child: Arc<Histogram>,
}

impl SingleHistogram {
    pub fn new(opts: HistogramOpts) -> prometheus::Result<Self> {
        let vec = HistogramVec::new(opts, &[])?;
        let child = vec.with_label_values(&[]);
        Ok(Self { vec, child })
    }

    pub fn observe(&self, v: f64) {
        self.child.observe(v);
    }
}

impl Collector for SingleHistogram {
    fn desc(&self) -> Vec<&Desc> {
        self.vec.desc()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.vec.collect()
    }
}

/// Copied verbatim from client_golang prometheus/histogram.go `nativeHistogramBounds`.
static NATIVE_HISTOGRAM_BOUNDS: [&[f64]; 9] = include!("native_histogram_bounds.in");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_schema_matches_go() {
        assert_eq!(pick_schema(1.1), 3);
        assert_eq!(pick_schema(1.00271), 8);
        assert_eq!(pick_schema(2.0), 0);
        assert_eq!(pick_schema(4.0), -1);
        assert_eq!(pick_schema(65536.0), -4);
        assert_eq!(pick_schema(1.0001), 8);
    }

    #[test]
    fn frexp_matches_go() {
        assert_eq!(frexp(1.0), (0.5, 1));
        assert_eq!(frexp(0.75), (0.75, 0));
        assert_eq!(frexp(8.0), (0.5, 4));
        assert_eq!(frexp(0.0), (0.0, 0));
        let (f, e) = frexp(5e-324);
        assert_eq!((f, e), (0.5, -1073));
    }

    #[test]
    fn bucket_keys_match_client_golang() {
        // Values from client_golang TestSparseHistogram "factor 1.1 results in schema 3".
        let keys: Vec<i32> = [0.0, 1.0, 2.0, 3.0]
            .iter()
            .filter(|v| **v > 0.0)
            .map(|v| native_bucket_key(*v, 3))
            .collect();
        assert_eq!(keys, vec![0, 8, 13]);
        // schema 0: 1 -> 0, 2 -> 1, 3 -> 2.
        assert_eq!(native_bucket_key(1.0, 0), 0);
        assert_eq!(native_bucket_key(2.0, 0), 1);
        assert_eq!(native_bucket_key(3.0, 0), 2);
        // schema -1: 1 -> 0, 2 -> 1, 3 -> 1, 5 -> 2.
        assert_eq!(native_bucket_key(1.0, -1), 0);
        assert_eq!(native_bucket_key(2.0, -1), 1);
        assert_eq!(native_bucket_key(3.0, -1), 1);
        assert_eq!(native_bucket_key(5.0, -1), 2);
    }

    #[test]
    fn make_buckets_matches_go() {
        // client_golang TestSparseHistogram "factor 1.1 results in schema 3":
        // observations {0, 1, 2, 3} -> PositiveSpan {Offset 0, Length 1}, {Offset 7, Length 1},
        // {Offset 4, Length 1}; PositiveDelta {1, 0, 0}.
        let mut m = BTreeMap::new();
        for k in [0, 8, 13] {
            m.insert(k, 1);
        }
        let (spans, deltas) = make_buckets(&m);
        assert_eq!(spans, vec![(0, 1), (7, 1), (4, 1)]);
        assert_eq!(deltas, vec![1, 0, 0]);
        // Small gaps are filled: keys 0,2 -> one span of length 3 with delta {1,-1,1}.
        let mut m = BTreeMap::new();
        m.insert(0, 1);
        m.insert(2, 1);
        assert_eq!(make_buckets(&m), (vec![(0, 3)], vec![1, -1, 1]));
    }

    #[test]
    fn double_bucket_width_on_limit() {
        // client_golang "buckets limited by halving resolution": factor 1.2 (schema 2), max 4,
        // observations {0, 1, 1.1, 1.2, 1.4, 1.8, 2, 3} -> schema 1, spans {0,5}, deltas
        // {1, 2, -1, -2, 1}; zero count 1.
        let vec = HistogramVec::new(
            HistogramOpts::new("x", "h").native(1.2, 4, Duration::ZERO),
            &[],
        )
        .unwrap();
        let h = vec.with_label_values(&[]);
        for v in [0.0, 1.0, 1.1, 1.2, 1.4, 1.8, 2.0, 3.0] {
            h.observe(v);
        }
        let inner = h.inner.lock();
        assert_eq!(inner.counts.schema, 1);
        assert_eq!(inner.counts.zero_count, 1);
        let (spans, deltas) = make_buckets(&inner.counts.positive);
        assert_eq!(spans, vec![(0, 5)]);
        assert_eq!(deltas, vec![1, 2, -1, -2, 1]);
    }

    #[test]
    fn native_only_has_no_classic_buckets() {
        let vec = HistogramVec::new(HistogramOpts::new("x", "h").native(1.1, 0, Duration::ZERO), &[])
            .unwrap();
        vec.with_label_values(&[]).observe(0.5);
        let mfs = vec.collect();
        let h = mfs[0].get_metric()[0].get_histogram();
        assert!(h.get_bucket().is_empty());
        assert_eq!(h.get_sample_count(), 1);
    }

    #[test]
    fn classic_defaults_to_def_buckets() {
        let vec = HistogramVec::new(HistogramOpts::new("x", "h"), &["a"]).unwrap();
        vec.with_label_values(&["v"]).observe(0.5);
        let mfs = vec.collect();
        assert_eq!(mfs[0].get_metric()[0].get_histogram().get_bucket().len(), DEF_BUCKETS.len());
    }
}
