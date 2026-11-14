//! Prometheus native histogram data model (`Histogram`, `FloatHistogram`, `Span`, `CounterResetHint`).
//!
//! Ported from github.com/prometheus/prometheus/model/histogram/histogram.go,
//! float_histogram.go, and generic.go.

pub const EXPONENTIAL_SCHEMA_MAX: i32 = 8;
pub const EXPONENTIAL_SCHEMA_MAX_RESERVED: i32 = 52;
pub const EXPONENTIAL_SCHEMA_MIN: i32 = -4;
pub const EXPONENTIAL_SCHEMA_MIN_RESERVED: i32 = -9;
pub const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

#[inline]
pub const fn is_custom_buckets_schema(s: i32) -> bool {
    s == CUSTOM_BUCKETS_SCHEMA
}

#[inline]
pub const fn is_exponential_schema(s: i32) -> bool {
    s >= EXPONENTIAL_SCHEMA_MIN && s <= EXPONENTIAL_SCHEMA_MAX
}

#[inline]
pub const fn is_exponential_schema_reserved(s: i32) -> bool {
    s >= EXPONENTIAL_SCHEMA_MIN_RESERVED && s <= EXPONENTIAL_SCHEMA_MAX_RESERVED
}

#[inline]
pub const fn is_valid_schema(s: i32) -> bool {
    is_custom_buckets_schema(s) || is_exponential_schema(s)
}

#[inline]
pub const fn is_known_schema(s: i32) -> bool {
    is_custom_buckets_schema(s) || is_exponential_schema_reserved(s)
}

#[inline]
pub fn custom_bucket_bounds_match(c1: &[f64], c2: &[f64]) -> bool {
    if c1.len() != c2.len() {
        return false;
    }
    c1.iter().zip(c2.iter()).all(|(a, b)| a == b)
}

/// Counter reset information per histogram sample.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CounterResetHint {
    #[default]
    UnknownCounterReset = 0,
    CounterReset = 1,
    NotCounterReset = 2,
    GaugeType = 3,
}

impl CounterResetHint {
    #[inline]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::CounterReset,
            2 => Self::NotCounterReset,
            3 => Self::GaugeType,
            _ => Self::UnknownCounterReset,
        }
    }
}

/// Two-bit counter reset header stored at byte 2 (`bits 7..6`) of histogram chunks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CounterResetHeader {
    #[default]
    UnknownCounterReset = 0b0000_0000,
    NotCounterReset = 0b0100_0000,
    CounterReset = 0b1000_0000,
    GaugeType = 0b1100_0000,
}

impl CounterResetHeader {
    pub const MASK: u8 = 0b1100_0000;

    #[inline]
    pub const fn from_header_byte(b: u8) -> Self {
        match b & Self::MASK {
            0b0100_0000 => Self::NotCounterReset,
            0b1000_0000 => Self::CounterReset,
            0b1100_0000 => Self::GaugeType,
            _ => Self::UnknownCounterReset,
        }
    }

    #[inline]
    pub const fn from_hint(hint: CounterResetHint) -> Self {
        match hint {
            CounterResetHint::CounterReset => Self::CounterReset,
            CounterResetHint::NotCounterReset => Self::NotCounterReset,
            CounterResetHint::GaugeType => Self::GaugeType,
            CounterResetHint::UnknownCounterReset => Self::UnknownCounterReset,
        }
    }
}

/// A continuous sequence of populated histogram buckets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct HistogramSpan {
    pub offset: i32,
    pub length: u32,
}

/// Native histogram with integer bucket counts (`PositiveBuckets`/`NegativeBuckets` are delta-encoded).
#[derive(Clone, Debug, Default)]
pub struct Histogram {
    pub counter_reset_hint: CounterResetHint,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub count: u64,
    pub sum: f64,
    pub positive_spans: Vec<HistogramSpan>,
    pub negative_spans: Vec<HistogramSpan>,
    pub positive_buckets: Vec<i64>,
    pub negative_buckets: Vec<i64>,
    pub custom_values: Vec<f64>,
}

impl Histogram {
    #[inline]
    pub fn uses_custom_buckets(&self) -> bool {
        is_custom_buckets_schema(self.schema)
    }

    /// Compares two histograms matching Prometheus `Histogram.Equals`.
    pub fn prom_equals(&self, other: &Self) -> bool {
        if self.schema != other.schema
            || self.count != other.count
            || self.sum.to_bits() != other.sum.to_bits()
        {
            return false;
        }
        if self.uses_custom_buckets()
            && !custom_bucket_bounds_match(&self.custom_values, &other.custom_values)
        {
            return false;
        }
        if self.zero_threshold != other.zero_threshold || self.zero_count != other.zero_count {
            return false;
        }
        spans_match(&self.negative_spans, &other.negative_spans)
            && self.negative_buckets == other.negative_buckets
            && spans_match(&self.positive_spans, &other.positive_spans)
            && self.positive_buckets == other.positive_buckets
    }

    /// Converts this integer histogram into a [`FloatHistogram`].
    pub fn to_float(&self) -> FloatHistogram {
        let mut p_buckets = Vec::with_capacity(self.positive_buckets.len());
        let mut cur = 0i64;
        for &d in &self.positive_buckets {
            cur += d;
            p_buckets.push(cur as f64);
        }
        let mut n_buckets = Vec::with_capacity(self.negative_buckets.len());
        cur = 0;
        for &d in &self.negative_buckets {
            cur += d;
            n_buckets.push(cur as f64);
        }
        FloatHistogram {
            counter_reset_hint: self.counter_reset_hint,
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count as f64,
            count: self.count as f64,
            sum: self.sum,
            positive_spans: self.positive_spans.clone(),
            negative_spans: self.negative_spans.clone(),
            positive_buckets: p_buckets,
            negative_buckets: n_buckets,
            custom_values: self.custom_values.clone(),
        }
    }
}

impl PartialEq for Histogram {
    fn eq(&self, other: &Self) -> bool {
        self.counter_reset_hint == other.counter_reset_hint && self.prom_equals(other)
    }
}

/// Native histogram with floating-point absolute bucket counts.
#[derive(Clone, Debug, Default)]
pub struct FloatHistogram {
    pub counter_reset_hint: CounterResetHint,
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub positive_spans: Vec<HistogramSpan>,
    pub negative_spans: Vec<HistogramSpan>,
    pub positive_buckets: Vec<f64>,
    pub negative_buckets: Vec<f64>,
    pub custom_values: Vec<f64>,
}

impl FloatHistogram {
    #[inline]
    pub fn uses_custom_buckets(&self) -> bool {
        is_custom_buckets_schema(self.schema)
    }

    /// Compares two float histograms matching Prometheus `FloatHistogram.Equals`.
    pub fn prom_equals(&self, other: &Self) -> bool {
        if self.schema != other.schema
            || self.count.to_bits() != other.count.to_bits()
            || self.sum.to_bits() != other.sum.to_bits()
        {
            return false;
        }
        if self.uses_custom_buckets()
            && !custom_bucket_bounds_match(&self.custom_values, &other.custom_values)
        {
            return false;
        }
        if self.zero_threshold.to_bits() != other.zero_threshold.to_bits()
            || self.zero_count.to_bits() != other.zero_count.to_bits()
        {
            return false;
        }
        spans_match(&self.negative_spans, &other.negative_spans)
            && buckets_float_bits_eq(&self.negative_buckets, &other.negative_buckets)
            && spans_match(&self.positive_spans, &other.positive_spans)
            && buckets_float_bits_eq(&self.positive_buckets, &other.positive_buckets)
    }
}

impl PartialEq for FloatHistogram {
    fn eq(&self, other: &Self) -> bool {
        self.counter_reset_hint == other.counter_reset_hint && self.prom_equals(other)
    }
}

fn buckets_float_bits_eq(a: &[f64], b: &[f64]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(x, y)| x.to_bits() == y.to_bits())
}

fn all_empty_spans(spans: &[HistogramSpan]) -> bool {
    spans.iter().all(|s| s.length == 0)
}

fn spans_match(s1: &[HistogramSpan], s2: &[HistogramSpan]) -> bool {
    if s1.is_empty() && s2.is_empty() {
        return true;
    }
    let (mut i1, mut i2) = (0usize, 0usize);
    loop {
        if i1 >= s1.len() {
            return all_empty_spans(&s2[i2..]);
        }
        if i2 >= s2.len() {
            return all_empty_spans(&s1[i1..]);
        }
        let mut c1 = s1[i1];
        let mut c2 = s2[i2];
        i1 += 1;
        i2 += 1;
        if c1.length == 0 {
            while i1 < s1.len() && s1[i1].length == 0 {
                c1.offset += s1[i1].offset;
                i1 += 1;
            }
            if i1 < s1.len() {
                c1.offset += s1[i1].offset;
                c1.length = s1[i1].length;
                i1 += 1;
            }
        }
        if c2.length == 0 {
            while i2 < s2.len() && s2[i2].length == 0 {
                c2.offset += s2[i2].offset;
                i2 += 1;
            }
            if i2 < s2.len() {
                c2.offset += s2[i2].offset;
                c2.length = s2[i2].length;
                i2 += 1;
            }
        }
        if c1.length == 0 && c2.length == 0 {
            return true;
        }
        if c1 != c2 {
            return false;
        }
    }
}
