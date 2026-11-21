//! Prometheus native histogram data model (`Histogram`, `FloatHistogram`, `Span`, `CounterResetHint`).
//!
//! Ported from github.com/prometheus/prometheus/model/histogram/histogram.go,
//! float_histogram.go, and generic.go.

pub const EXPONENTIAL_SCHEMA_MAX: i32 = 8;
pub const EXPONENTIAL_SCHEMA_MAX_RESERVED: i32 = 52;
pub const EXPONENTIAL_SCHEMA_MIN: i32 = -4;
pub const EXPONENTIAL_SCHEMA_MIN_RESERVED: i32 = -9;
pub const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

/// The `histogram.Err*` sentinel (or `InvalidSchemaError`) a validation error wraps
/// (`model/histogram/generic.go`), so callers can match like Go's `errors.Is`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramErrorKind {
    CountNotBigEnough,
    CountMismatch,
    NegativeCount,
    NegativeBucketCount,
    SpanNegativeOffset,
    SpansBucketsMismatch,
    SpanIndexOutOfRange,
    CustomBucketsMismatch,
    CustomBucketsInvalid,
    CustomBucketsInfinite,
    CustomBucketsNaN,
    CustomBucketsZeroCount,
    CustomBucketsZeroThresh,
    CustomBucketsNegSpans,
    CustomBucketsNegBuckets,
    ExpSchemaCustomBounds,
    /// `InvalidSchemaError` (wraps `ErrHistogramsInvalidSchema`).
    InvalidSchema,
}

impl HistogramErrorKind {
    /// The sentinel's error text.
    pub fn sentinel_message(self) -> &'static str {
        match self {
            Self::CountNotBigEnough => "histogram's observation count should be at least the number of observations found in the buckets",
            Self::CountMismatch => "histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)",
            Self::NegativeCount => "histogram's observation count is negative",
            Self::NegativeBucketCount => "histogram has a bucket whose observation count is negative",
            Self::SpanNegativeOffset => "histogram has a span whose offset is negative",
            Self::SpansBucketsMismatch => "histogram spans specify different number of buckets than provided",
            Self::SpanIndexOutOfRange => "histogram has a span whose bucket index is out of range",
            Self::CustomBucketsMismatch => "histogram custom bounds are too few",
            Self::CustomBucketsInvalid => "histogram custom bounds must be in strictly increasing order",
            Self::CustomBucketsInfinite => "histogram custom bounds must be finite",
            Self::CustomBucketsNaN => "histogram custom bounds must not be NaN",
            Self::CustomBucketsZeroCount => "custom buckets: must have zero count of 0",
            Self::CustomBucketsZeroThresh => "custom buckets: must have zero threshold of 0",
            Self::CustomBucketsNegSpans => "custom buckets: must not have negative spans",
            Self::CustomBucketsNegBuckets => "custom buckets: must not have negative buckets",
            Self::ExpSchemaCustomBounds => "histogram with exponential schema must not have custom bounds",
            Self::InvalidSchema => "histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets",
        }
    }
}

/// A native histogram `Validate()` error: the wrapped sentinel [`HistogramErrorKind`] plus the
/// full Go error text (`fmt.Errorf("...: %w", ErrHistogram...)` chains).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramValidationError {
    pub kind: HistogramErrorKind,
    message: String,
}

impl HistogramValidationError {
    /// `fmt.Errorf("{context}: %w", sentinel)`.
    pub fn new(kind: HistogramErrorKind, context: String) -> Self {
        let message = format!("{context}: {}", kind.sentinel_message());
        Self { kind, message }
    }

    /// The bare sentinel.
    pub fn sentinel(kind: HistogramErrorKind) -> Self {
        Self {
            kind,
            message: kind.sentinel_message().to_string(),
        }
    }

    /// `InvalidSchemaError(schema)`.
    pub fn invalid_schema(schema: i32) -> Self {
        Self {
            kind: HistogramErrorKind::InvalidSchema,
            message: format!("{}, got schema {schema}", HistogramErrorKind::InvalidSchema.sentinel_message()),
        }
    }

    /// `fmt.Errorf("{prefix}: %w", err)`.
    pub fn wrap(self, prefix: &str) -> Self {
        Self {
            kind: self.kind,
            message: format!("{prefix}: {}", self.message),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for HistogramValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HistogramValidationError {}

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

    /// Validates this integer histogram matching Prometheus `Histogram.Validate()`.
    pub fn validate(&self) -> Result<(), HistogramValidationError> {
        let mut n_count = 0u64;
        let mut p_count = 0u64;
        if is_custom_buckets_schema(self.schema) {
            check_histogram_custom_bounds(
                &self.custom_values,
                &self.positive_spans,
                self.positive_buckets.len(),
            )
            .map_err(|e| e.wrap("custom buckets"))?;
            if self.zero_count != 0 {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsZeroCount));
            }
            if self.zero_threshold != 0.0 {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsZeroThresh));
            }
            if !self.negative_spans.is_empty() {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsNegSpans));
            }
            if !self.negative_buckets.is_empty() {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsNegBuckets));
            }
        } else if is_exponential_schema(self.schema) {
            check_histogram_spans(&self.positive_spans, self.positive_buckets.len())
                .map_err(|e| e.wrap("positive side"))?;
            check_histogram_spans(&self.negative_spans, self.negative_buckets.len())
                .map_err(|e| e.wrap("negative side"))?;
            check_histogram_int_buckets(&self.negative_buckets, &mut n_count)
                .map_err(|e| e.wrap("negative side"))?;
            if !self.custom_values.is_empty() {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::ExpSchemaCustomBounds));
            }
        } else {
            return Err(HistogramValidationError::invalid_schema(self.schema));
        }

        check_histogram_int_buckets(&self.positive_buckets, &mut p_count)
            .map_err(|e| e.wrap("positive side"))?;

        let sum_of_buckets = n_count
            .wrapping_add(p_count)
            .wrapping_add(self.zero_count);
        if self.sum.is_nan() {
            if sum_of_buckets > self.count {
                return Err(HistogramValidationError::new(
                    HistogramErrorKind::CountNotBigEnough,
                    format!("{sum_of_buckets} observations found in buckets, but the Count field is {}", self.count),
                ));
            }
        } else if sum_of_buckets != self.count {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::CountMismatch,
                format!("{sum_of_buckets} observations found in buckets, but the Count field is {}", self.count),
            ));
        }

        Ok(())
    }

    /// Converts this integer histogram into a [`FloatHistogram`].
    pub fn to_float(&self) -> FloatHistogram {
        let mut p_buckets = Vec::with_capacity(self.positive_buckets.len());
        let mut cur = 0i64;
        for &d in &self.positive_buckets {
            cur = cur.wrapping_add(d);
            p_buckets.push(cur as f64);
        }
        let mut n_buckets = Vec::with_capacity(self.negative_buckets.len());
        cur = 0;
        for &d in &self.negative_buckets {
            cur = cur.wrapping_add(d);
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
        if self.zero_threshold != other.zero_threshold
            || self.zero_count.to_bits() != other.zero_count.to_bits()
        {
            return false;
        }
        spans_match(&self.negative_spans, &other.negative_spans)
            && buckets_float_bits_eq(&self.negative_buckets, &other.negative_buckets)
            && spans_match(&self.positive_spans, &other.positive_spans)
            && buckets_float_bits_eq(&self.positive_buckets, &other.positive_buckets)
    }

    /// Validates this float histogram matching Prometheus `FloatHistogram.Validate()`.
    pub fn validate(&self) -> Result<(), HistogramValidationError> {
        if is_custom_buckets_schema(self.schema) {
            check_histogram_custom_bounds(
                &self.custom_values,
                &self.positive_spans,
                self.positive_buckets.len(),
            )
            .map_err(|e| e.wrap("custom buckets"))?;
            if self.zero_count != 0.0 {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsZeroCount));
            }
            if self.zero_threshold != 0.0 {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsZeroThresh));
            }
            if !self.negative_spans.is_empty() {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsNegSpans));
            }
            if !self.negative_buckets.is_empty() {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsNegBuckets));
            }
        } else if is_exponential_schema(self.schema) {
            check_histogram_spans(&self.positive_spans, self.positive_buckets.len())
                .map_err(|e| e.wrap("positive side"))?;
            check_histogram_spans(&self.negative_spans, self.negative_buckets.len())
                .map_err(|e| e.wrap("negative side"))?;
            check_histogram_float_buckets(&self.negative_buckets)
                .map_err(|e| e.wrap("negative side"))?;
            if self.zero_count < 0.0 {
                return Err(HistogramValidationError::new(
                    HistogramErrorKind::NegativeBucketCount,
                    format!(
                        "zero bucket has observation count of {}",
                        format_go_g(self.zero_count)
                    ),
                ));
            }
            if !self.custom_values.is_empty() {
                return Err(HistogramValidationError::sentinel(HistogramErrorKind::ExpSchemaCustomBounds));
            }
        } else {
            return Err(HistogramValidationError::invalid_schema(self.schema));
        }

        if self.count < 0.0 {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::NegativeCount,
                format!("observation count is  {}", format_go_g(self.count)),
            ));
        }
        check_histogram_float_buckets(&self.positive_buckets)
            .map_err(|e| e.wrap("positive side"))?;
        Ok(())
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
                c1.offset = c1.offset.wrapping_add(s1[i1].offset);
                i1 += 1;
            }
            if i1 < s1.len() {
                c1.offset = c1.offset.wrapping_add(s1[i1].offset);
                c1.length = s1[i1].length;
                i1 += 1;
            }
        }
        if c2.length == 0 {
            while i2 < s2.len() && s2[i2].length == 0 {
                c2.offset = c2.offset.wrapping_add(s2[i2].offset);
                i2 += 1;
            }
            if i2 < s2.len() {
                c2.offset = c2.offset.wrapping_add(s2[i2].offset);
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

fn check_histogram_spans(spans: &[HistogramSpan], num_buckets: usize) -> Result<(), HistogramValidationError> {
    let mut span_buckets: usize = 0;
    let mut end_idx: i64 = 0;
    for (n, span) in spans.iter().enumerate() {
        if n > 0 && span.offset < 0 {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::SpanNegativeOffset,
                format!("span number {} with offset {}", n + 1, span.offset),
            ));
        } else if n == 0 && span.offset == i32::MIN {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::SpanIndexOutOfRange,
                format!("span number {} starts at bucket index {}", n + 1, span.offset),
            ));
        }
        span_buckets = span_buckets.saturating_add(span.length as usize);
        end_idx = end_idx
            .saturating_add(span.offset as i64)
            .saturating_add(span.length as i64);
        if end_idx.saturating_sub(1) > i32::MAX as i64 {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::SpanIndexOutOfRange,
                format!(
                    "span number {} ends at bucket index {}",
                    n + 1,
                    end_idx.saturating_sub(1)
                ),
            ));
        }
    }
    if span_buckets != num_buckets {
        return Err(HistogramValidationError::new(
            HistogramErrorKind::SpansBucketsMismatch,
            format!("spans need {span_buckets} buckets, have {num_buckets} buckets"),
        ));
    }
    Ok(())
}

fn format_go_f(v: f64) -> String {
    if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else if v.is_nan() {
        "NaN".to_string()
    } else {
        format!("{v:.6}")
    }
}

pub(crate) fn format_go_g(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() { "+Inf" } else { "-Inf" }.to_string();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    let sci = format!("{v:e}");
    let Some((mant, exp_str)) = sci.split_once('e') else {
        return sci;
    };
    let Ok(exp) = exp_str.parse::<i32>() else {
        return sci;
    };
    if !(-4..6).contains(&exp) {
        return if exp >= 0 {
            format!("{mant}e+{exp:02}")
        } else {
            format!("{mant}e-{:02}", exp.unsigned_abs())
        };
    }
    let (sign, unsigned_mant) = match mant.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", mant),
    };
    let digits: String = unsigned_mant.chars().filter(|&c| c != '.').collect();
    let dp = exp + 1;
    if dp <= 0 {
        format!("{sign}0.{}{digits}", "0".repeat((-dp) as usize))
    } else {
        let dp_usize = dp as usize;
        if dp_usize >= digits.len() {
            format!("{sign}{digits}{}", "0".repeat(dp_usize - digits.len()))
        } else {
            format!("{sign}{}.{}", &digits[..dp_usize], &digits[dp_usize..])
        }
    }
}

fn check_histogram_int_buckets(buckets: &[i64], count: &mut u64) -> Result<(), HistogramValidationError> {
    let mut last = 0i64;
    for (i, &delta) in buckets.iter().enumerate() {
        let c = last.wrapping_add(delta);
        if c < 0 {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::NegativeBucketCount,
                format!("bucket number {} has observation count of {c}", i + 1),
            ));
        }
        last = c;
        *count = count.wrapping_add(c as u64);
    }
    Ok(())
}

fn check_histogram_float_buckets(buckets: &[f64]) -> Result<(), HistogramValidationError> {
    for (i, &c) in buckets.iter().enumerate() {
        if c < 0.0 {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::NegativeBucketCount,
                format!(
                    "bucket number {} has observation count of {}",
                    i + 1,
                    format_go_g(c)
                ),
            ));
        }
    }
    Ok(())
}

fn check_histogram_custom_bounds(
    bounds: &[f64],
    spans: &[HistogramSpan],
    num_buckets: usize,
) -> Result<(), HistogramValidationError> {
    let mut prev = f64::NEG_INFINITY;
    for (i, &curr) in bounds.iter().enumerate() {
        if curr.is_nan() {
            return Err(HistogramValidationError::sentinel(HistogramErrorKind::CustomBucketsNaN));
        }
        if i > 0 && curr <= prev {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::CustomBucketsInvalid,
                format!(
                    "previous bound is {} and current is {}",
                    format_go_f(prev),
                    format_go_f(curr)
                ),
            ));
        }
        prev = curr;
    }
    if prev == f64::INFINITY {
        return Err(HistogramValidationError::new(
            HistogramErrorKind::CustomBucketsInfinite,
            "last +Inf bound must not be explicitly defined".to_string(),
        ));
    }

    let mut span_buckets: usize = 0;
    let mut total_span_length: i64 = 0;
    for (n, span) in spans.iter().enumerate() {
        if span.offset < 0 {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::SpanNegativeOffset,
                format!("span number {} with offset {}", n + 1, span.offset),
            ));
        }
        span_buckets = span_buckets.saturating_add(span.length as usize);
        total_span_length = total_span_length
            .saturating_add(span.length as i64)
            .saturating_add(span.offset as i64);
    }
    if span_buckets != num_buckets {
        return Err(HistogramValidationError::new(
            HistogramErrorKind::SpansBucketsMismatch,
            format!("spans need {span_buckets} buckets, have {num_buckets} buckets"),
        ));
    }
    if ((bounds.len() + 1) as i64) < total_span_length {
        return Err(HistogramValidationError::new(
            HistogramErrorKind::CustomBucketsMismatch,
            format!(
                "only {} custom bounds defined which is insufficient to cover total span length of {total_span_length}",
                bounds.len()
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tsdb_08_int_buckets_overflow_wrapping_matches_go() {
        // positive_deltas = [i64::MAX, 1] must wrap in two's complement (no panic) and report
        // NegativeBucketCount, matching Go c = last + buckets[i].
        let h_signed_wrap = Histogram {
            schema: 0,
            count: 1,
            sum: 1.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 2 }],
            positive_buckets: vec![i64::MAX, 1],
            ..Histogram::default()
        };
        let err = h_signed_wrap.validate().unwrap_err();
        assert_eq!(err.kind, HistogramErrorKind::NegativeBucketCount);

        // Unsigned bucket-sum wrap: zero_count = u64::MAX, positive_deltas = [10], count = u64::MAX.
        // Go computes wrapping uint64 sumOfBuckets = 9 != u64::MAX -> CountMismatch.
        let h_u64_wrap = Histogram {
            schema: 0,
            zero_count: u64::MAX,
            count: u64::MAX,
            sum: 1.0,
            positive_spans: vec![HistogramSpan { offset: 0, length: 1 }],
            positive_buckets: vec![10],
            ..Histogram::default()
        };
        let err = h_u64_wrap.validate().unwrap_err();
        assert_eq!(err.kind, HistogramErrorKind::CountMismatch);
        assert!(err.to_string().contains("9 observations found in buckets"));
    }

    #[test]
    fn tsdb_08_spans_match_wrapping_and_float_g_formatting() {
        let s1 = [
            HistogramSpan {
                offset: i32::MAX,
                length: 0,
            },
            HistogramSpan {
                offset: 1,
                length: 1,
            },
        ];
        let s2 = [HistogramSpan {
            offset: i32::MIN,
            length: 1,
        }];
        assert!(spans_match(&s1, &s2));

        let fh_neg = FloatHistogram {
            schema: 0,
            count: -1_000_000.0,
            ..FloatHistogram::default()
        };
        let err = fh_neg.validate().unwrap_err();
        assert_eq!(err.kind, HistogramErrorKind::NegativeCount);
        assert!(err.to_string().contains("observation count is  -1e+06:"), "got: {err}");

        let fh_neg_bucket = FloatHistogram {
            schema: 0,
            zero_count: -1_000_000.0,
            ..FloatHistogram::default()
        };
        let err2 = fh_neg_bucket.validate().unwrap_err();
        assert_eq!(err2.kind, HistogramErrorKind::NegativeBucketCount);
        assert!(
            err2.to_string().contains("zero bucket has observation count of -1e+06:"),
            "got: {err2}"
        );
    }

    #[test]
    fn custom_buckets_invalid_formats_pos_and_neg_inf_like_go() {
        let fh_pos_inf = FloatHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            custom_values: vec![f64::INFINITY, 1.0],
            ..FloatHistogram::default()
        };
        let err1 = fh_pos_inf.validate().unwrap_err();
        assert_eq!(err1.kind, HistogramErrorKind::CustomBucketsInvalid);
        let msg1 = err1.to_string();
        assert!(msg1.contains("previous bound is +Inf"), "got: {msg1}");

        let fh_neg_inf = FloatHistogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            custom_values: vec![f64::NEG_INFINITY, f64::NEG_INFINITY],
            ..FloatHistogram::default()
        };
        let err2 = fh_neg_inf.validate().unwrap_err();
        assert_eq!(err2.kind, HistogramErrorKind::CustomBucketsInvalid);
        let msg2 = err2.to_string();
        assert!(msg2.contains("previous bound is -Inf and current is -Inf"), "got: {msg2}");
    }
}
