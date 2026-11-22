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
    IncompatibleSchema,
    /// `InvalidSchemaError` (wraps `ErrHistogramsInvalidSchema`).
    InvalidSchema,
    /// `UnknownSchemaError` (wraps `ErrHistogramsUnknownSchema`).
    UnknownSchema,
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
            Self::IncompatibleSchema => "cannot apply this operation on histograms with a mix of exponential and custom bucket schemas",
            Self::InvalidSchema => "histogram has an invalid schema, which must be between -4 and 8 for exponential buckets, or -53 for custom buckets",
            Self::UnknownSchema => "histogram has an unknown schema, which must be between -9 and 52 for exponential buckets, or -53 for custom buckets",
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

    /// Converts this integer histogram into a [`FloatHistogram`] matching Go `Histogram.ToFloat`.
    pub fn to_float(&self) -> FloatHistogram {
        self.to_float_into(None)
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


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReduceResolutionError {
    Validation(HistogramValidationError),
    InvalidArg(String),
}

impl std::fmt::Display for ReduceResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(e) => write!(f, "{e}"),
            Self::InvalidArg(s) => f.write_str(s),
        }
    }
}

impl std::error::Error for ReduceResolutionError {}

impl From<HistogramValidationError> for ReduceResolutionError {
    fn from(e: HistogramValidationError) -> Self {
        Self::Validation(e)
    }
}

#[inline]
pub fn ldexp(mut x: f64, mut exp: i32) -> f64 {
    if x == 0.0 || !x.is_finite() {
        return x;
    }
    let mut bits = x.to_bits();
    let mut e = ((bits >> 52) & 0x7ff) as i32;
    if e == 0 {
        x *= f64::from_bits((1023u64 + 54) << 52);
        bits = x.to_bits();
        e = ((bits >> 52) & 0x7ff) as i32 - 54;
    }
    let new_e = e.saturating_add(exp);
    if new_e >= 0x7ff {
        return if (bits >> 63) != 0 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    if new_e > 0 {
        let out_bits = (bits & !(0x7ffu64 << 52)) | ((new_e as u64) << 52);
        return f64::from_bits(out_bits);
    }
    if new_e <= -54 {
        return 0.0_f64.copysign(x);
    }
    exp = new_e + 54;
    let out_bits = (bits & !(0x7ffu64 << 52)) | ((exp as u64) << 52);
    f64::from_bits(out_bits) * f64::from_bits((1023u64 - 54) << 52)
}

#[inline(never)]
pub fn kahan_inc(inc: f64, sum: f64, mut c: f64) -> (f64, f64) {
    let t = sum + inc;
    if t > f64::MAX || t < -f64::MAX {
        c = 0.0;
    } else if sum.abs() >= inc.abs() {
        c += (sum - t) + inc;
    } else {
        c += (inc - t) + sum;
    }
    (t, c)
}

#[inline(never)]
pub fn kahan_dec(dec: f64, sum: f64, c: f64) -> (f64, f64) {
    kahan_inc(-dec, sum, c)
}

pub trait BucketCountFmt: Copy {
    fn fmt_count(self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;
}

impl BucketCountFmt for u64 {
    fn fmt_count(self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl BucketCountFmt for f64 {
    fn fmt_count(self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format_go_g(self))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bucket<BC> {
    pub lower: f64,
    pub upper: f64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub count: BC,
    pub index: i32,
}

impl<BC: BucketCountFmt> std::fmt::Display for Bucket<BC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.lower_inclusive { "[" } else { "(" })?;
        f.write_str(&format_go_g(self.lower))?;
        f.write_str(",")?;
        f.write_str(&format_go_g(self.upper))?;
        f.write_str(if self.upper_inclusive { "]" } else { ")" })?;
        f.write_str(":")?;
        self.count.fmt_count(f)
    }
}

impl<BC> Bucket<BC> {
    pub fn fraction_below(&self, v: f64, linear: bool) -> f64 {
        if linear {
            return (v - self.lower) / (self.upper - self.lower);
        }
        let log_lower = self.lower.abs().log2();
        let log_upper = self.upper.abs().log2();
        let log_v = v.abs().log2();
        if v > 0.0 {
            (log_v - log_lower) / (log_upper - log_lower)
        } else {
            1.0 - ((log_v - log_upper) / (log_lower - log_upper))
        }
    }
}

static EXPONENTIAL_BOUNDS: [&[f64]; 9] = [
	// Schema 0:
    &[0.5],
	// Schema 1:
    &[0.5, 0.7071067811865475],
	// Schema 2:
    &[0.5, 0.5946035575013605, 0.7071067811865475, 0.8408964152537144],
	// Schema 3:
    &[
		0.5, 0.5452538663326288, 0.5946035575013605, 0.6484197773255048,
		0.7071067811865475, 0.7711054127039704, 0.8408964152537144, 0.9170040432046711,
	],
	// Schema 4:
    &[
		0.5, 0.5221368912137069, 0.5452538663326288, 0.5693943173783458,
		0.5946035575013605, 0.620928906036742, 0.6484197773255048, 0.6771277734684463,
		0.7071067811865475, 0.7384130729697496, 0.7711054127039704, 0.805245165974627,
		0.8408964152537144, 0.8781260801866495, 0.9170040432046711, 0.9576032806985735,
	],
	// Schema 5:
    &[
		0.5, 0.5109485743270583, 0.5221368912137069, 0.5335702003384117,
		0.5452538663326288, 0.5571933712979462, 0.5693943173783458, 0.5818624293887887,
		0.5946035575013605, 0.6076236799902344, 0.620928906036742, 0.6345254785958666,
		0.6484197773255048, 0.6626183215798706, 0.6771277734684463, 0.6919549409819159,
		0.7071067811865475, 0.7225904034885232, 0.7384130729697496, 0.7545822137967112,
		0.7711054127039704, 0.7879904225539431, 0.805245165974627, 0.8228777390769823,
		0.8408964152537144, 0.8593096490612387, 0.8781260801866495, 0.8973545375015533,
		0.9170040432046711, 0.9370838170551498, 0.9576032806985735, 0.9785720620876999,
	],
	// Schema 6:
    &[
		0.5, 0.5054446430258502, 0.5109485743270583, 0.5165124395106142,
		0.5221368912137069, 0.5278225891802786, 0.5335702003384117, 0.5393803988785598,
		0.5452538663326288, 0.5511912916539204, 0.5571933712979462, 0.5632608093041209,
		0.5693943173783458, 0.5755946149764913, 0.5818624293887887, 0.5881984958251406,
		0.5946035575013605, 0.6010783657263515, 0.6076236799902344, 0.6142402680534349,
		0.620928906036742, 0.6276903785123455, 0.6345254785958666, 0.6414350080393891,
		0.6484197773255048, 0.6554806057623822, 0.6626183215798706, 0.6698337620266515,
		0.6771277734684463, 0.6845012114872953, 0.6919549409819159, 0.6994898362691555,
		0.7071067811865475, 0.7148066691959849, 0.7225904034885232, 0.7304588970903234,
		0.7384130729697496, 0.7464538641456323, 0.7545822137967112, 0.762799075372269,
		0.7711054127039704, 0.7795022001189185, 0.7879904225539431, 0.7965710756711334,
		0.805245165974627, 0.8140137109286738, 0.8228777390769823, 0.8318382901633681,
		0.8408964152537144, 0.8500531768592616, 0.8593096490612387, 0.8686669176368529,
		0.8781260801866495, 0.8876882462632604, 0.8973545375015533, 0.9071260877501991,
		0.9170040432046711, 0.9269895625416926, 0.9370838170551498, 0.9472879907934827,
		0.9576032806985735, 0.9680308967461471, 0.9785720620876999, 0.9892280131939752,
	],
	// Schema 7:
    &[
		0.5, 0.5027149505564014, 0.5054446430258502, 0.5081891574554764,
		0.5109485743270583, 0.5137229745593818, 0.5165124395106142, 0.5193170509806894,
		0.5221368912137069, 0.5249720429003435, 0.5278225891802786, 0.5306886136446309,
		0.5335702003384117, 0.5364674337629877, 0.5393803988785598, 0.5423091811066545,
		0.5452538663326288, 0.5482145409081883, 0.5511912916539204, 0.5541842058618393,
		0.5571933712979462, 0.5602188762048033, 0.5632608093041209, 0.5663192597993595,
		0.5693943173783458, 0.572486072215902, 0.5755946149764913, 0.5787200368168754,
		0.5818624293887887, 0.585021884841625, 0.5881984958251406, 0.5913923554921704,
		0.5946035575013605, 0.5978321960199137, 0.6010783657263515, 0.6043421618132907,
		0.6076236799902344, 0.6109230164863786, 0.6142402680534349, 0.6175755319684665,
		0.620928906036742, 0.6243004885946023, 0.6276903785123455, 0.6310986751971253,
		0.6345254785958666, 0.637970889198196, 0.6414350080393891, 0.6449179367033329,
		0.6484197773255048, 0.6519406325959679, 0.6554806057623822, 0.659039800633032,
		0.6626183215798706, 0.6662162735415805, 0.6698337620266515, 0.6734708931164728,
		0.6771277734684463, 0.6808045103191123, 0.6845012114872953, 0.688217985377265,
		0.6919549409819159, 0.6957121878859629, 0.6994898362691555, 0.7032879969095076,
		0.7071067811865475, 0.7109463010845827, 0.7148066691959849, 0.718687998724491,
		0.7225904034885232, 0.7265139979245261, 0.7304588970903234, 0.7344252166684908,
		0.7384130729697496, 0.7424225829363761, 0.7464538641456323, 0.7505070348132126,
		0.7545822137967112, 0.7586795205991071, 0.762799075372269, 0.7669409989204777,
		0.7711054127039704, 0.7752924388424999, 0.7795022001189185, 0.7837348199827764,
		0.7879904225539431, 0.7922691326262467, 0.7965710756711334, 0.8008963778413465,
		0.805245165974627, 0.8096175675974316, 0.8140137109286738, 0.8184337248834821,
		0.8228777390769823, 0.8273458838280969, 0.8318382901633681, 0.8363550898207981,
		0.8408964152537144, 0.8454623996346523, 0.8500531768592616, 0.8546688815502312,
		0.8593096490612387, 0.8639756154809185, 0.8686669176368529, 0.8733836930995842,
		0.8781260801866495, 0.8828942179666361, 0.8876882462632604, 0.8925083056594671,
		0.8973545375015533, 0.9022270839033115, 0.9071260877501991, 0.9120516927035263,
		0.9170040432046711, 0.9219832844793128, 0.9269895625416926, 0.9320230241988943,
		0.9370838170551498, 0.9421720895161669, 0.9472879907934827, 0.9524316709088368,
		0.9576032806985735, 0.9628029718180622, 0.9680308967461471, 0.9732872087896164,
		0.9785720620876999, 0.9838856116165875, 0.9892280131939752, 0.9945994234836328,
	],
	// Schema 8:
    &[
		0.5, 0.5013556375251013, 0.5027149505564014, 0.5040779490592088,
		0.5054446430258502, 0.5068150424757447, 0.5081891574554764, 0.509566998038869,
		0.5109485743270583, 0.5123338964485679, 0.5137229745593818, 0.5151158188430205,
		0.5165124395106142, 0.5179128468009786, 0.5193170509806894, 0.520725062344158,
		0.5221368912137069, 0.5235525479396449, 0.5249720429003435, 0.526395386502313,
		0.5278225891802786, 0.5292536613972564, 0.5306886136446309, 0.5321274564422321,
		0.5335702003384117, 0.5350168559101208, 0.5364674337629877, 0.5379219445313954,
		0.5393803988785598, 0.5408428074966075, 0.5423091811066545, 0.5437795304588847,
		0.5452538663326288, 0.5467321995364429, 0.5482145409081883, 0.549700901315111,
		0.5511912916539204, 0.5526857228508706, 0.5541842058618393, 0.5556867516724088,
		0.5571933712979462, 0.5587040757836845, 0.5602188762048033, 0.5617377836665098,
		0.5632608093041209, 0.564787964283144, 0.5663192597993595, 0.5678547070789026,
		0.5693943173783458, 0.5709381019847808, 0.572486072215902, 0.5740382394200894,
		0.5755946149764913, 0.5771552102951081, 0.5787200368168754, 0.5802891060137493,
		0.5818624293887887, 0.5834400184762408, 0.585021884841625, 0.5866080400818185,
		0.5881984958251406, 0.5897932637314379, 0.5913923554921704, 0.5929957828304968,
		0.5946035575013605, 0.5962156912915756, 0.5978321960199137, 0.5994530835371903,
		0.6010783657263515, 0.6027080545025619, 0.6043421618132907, 0.6059806996384005,
		0.6076236799902344, 0.6092711149137041, 0.6109230164863786, 0.6125793968185725,
		0.6142402680534349, 0.6159056423670379, 0.6175755319684665, 0.6192499490999082,
		0.620928906036742, 0.622612415087629, 0.6243004885946023, 0.6259931389331581,
		0.6276903785123455, 0.6293922197748583, 0.6310986751971253, 0.6328097572894031,
		0.6345254785958666, 0.6362458516947014, 0.637970889198196, 0.6397006037528346,
		0.6414350080393891, 0.6431741147730128, 0.6449179367033329, 0.6466664866145447,
		0.6484197773255048, 0.6501778216898253, 0.6519406325959679, 0.6537082229673385,
		0.6554806057623822, 0.6572577939746774, 0.659039800633032, 0.6608266388015788,
		0.6626183215798706, 0.6644148621029772, 0.6662162735415805, 0.6680225691020727,
		0.6698337620266515, 0.6716498655934177, 0.6734708931164728, 0.6752968579460171,
		0.6771277734684463, 0.6789636531064505, 0.6808045103191123, 0.6826503586020058,
		0.6845012114872953, 0.6863570825438342, 0.688217985377265, 0.690083933630119,
		0.6919549409819159, 0.6938310211492645, 0.6957121878859629, 0.6975984549830999,
		0.6994898362691555, 0.7013863456101023, 0.7032879969095076, 0.7051948041086352,
		0.7071067811865475, 0.7090239421602076, 0.7109463010845827, 0.7128738720527471,
		0.7148066691959849, 0.7167447066838943, 0.718687998724491, 0.7206365595643126,
		0.7225904034885232, 0.7245495448210174, 0.7265139979245261, 0.7284837772007218,
		0.7304588970903234, 0.7324393720732029, 0.7344252166684908, 0.7364164454346837,
		0.7384130729697496, 0.7404151139112358, 0.7424225829363761, 0.7444354947621984,
		0.7464538641456323, 0.7484777058836176, 0.7505070348132126, 0.7525418658117031,
		0.7545822137967112, 0.7566280937263048, 0.7586795205991071, 0.7607365094544071,
		0.762799075372269, 0.7648672334736434, 0.7669409989204777, 0.7690203869158282,
		0.7711054127039704, 0.7731960915705107, 0.7752924388424999, 0.7773944698885442,

		0.7795022001189185, 0.7816156449856788, 0.7837348199827764, 0.7858597406461707,
		0.7879904225539431, 0.7901268813264122, 0.7922691326262467, 0.7944171921585818,
		0.7965710756711334, 0.7987307989543135, 0.8008963778413465, 0.8030678282083853,
		0.805245165974627, 0.8074284071024302, 0.8096175675974316, 0.8118126635086642,
		0.8140137109286738, 0.8162207259936375, 0.8184337248834821, 0.820652723822003,
		0.8228777390769823, 0.8251087869603088, 0.8273458838280969, 0.8295890460808079,
		0.8318382901633681, 0.8340936325652911, 0.8363550898207981, 0.8386226785089391,
		0.8408964152537144, 0.8431763167241966, 0.8454623996346523, 0.8477546807446661,
		0.8500531768592616, 0.8523579048290255, 0.8546688815502312, 0.8569861239649629,
		0.8593096490612387, 0.8616394738731368, 0.8639756154809185, 0.8663180910111553,
		0.8686669176368529, 0.871022112577578, 0.8733836930995842, 0.8757516765159389,
		0.8781260801866495, 0.8805069215187917, 0.8828942179666361, 0.8852879870317771,
		0.8876882462632604, 0.890095013257712, 0.8925083056594671, 0.8949281411607002,
		0.8973545375015533, 0.8997875124702672, 0.9022270839033115, 0.9046732696855155,
		0.9071260877501991, 0.909585556079304, 0.9120516927035263, 0.9145245157024483,
		0.9170040432046711, 0.9194902933879467, 0.9219832844793128, 0.9244830347552253,
		0.9269895625416926, 0.92950288621441, 0.9320230241988943, 0.9345499949706191,
		0.9370838170551498, 0.93962450902828, 0.9421720895161669, 0.9447265771954693,
		0.9472879907934827, 0.9498563490882775, 0.9524316709088368, 0.9550139751351947,
		0.9576032806985735, 0.9601996065815236, 0.9628029718180622, 0.9654133954938133,
		0.9680308967461471, 0.9706554947643201, 0.9732872087896164, 0.9759260581154889,
		0.9785720620876999, 0.9812252401044634, 0.9838856116165875, 0.9865531961276168,
		0.9892280131939752, 0.9919100824251095, 0.9945994234836328, 0.9972960560854698,
	],
];

#[inline]
pub fn get_bound_exponential(idx: i32, schema: i32) -> f64 {
    if schema < 0 {
        let exp = (idx as i64) << (-schema);
        if exp == 1024 {
            return f64::MAX;
        }
        return ldexp(1.0, exp.clamp(i32::MIN as i64, i32::MAX as i64) as i32);
    }
    let frac_idx = (idx & ((1 << schema) - 1)) as usize;
    let frac = EXPONENTIAL_BOUNDS[schema as usize][frac_idx];
    let exp = (idx >> schema) + 1;
    if frac == 0.5 && exp == 1025 {
        return f64::MAX;
    }
    ldexp(frac, exp)
}

#[inline]
pub fn get_bound(idx: i32, schema: i32, custom_values: &[f64]) -> f64 {
    if is_custom_buckets_schema(schema) {
        let length = custom_values.len() as i32;
        assert!(
            idx <= length && idx >= -1,
            "index {idx} out of bounds for custom bounds of length {length}"
        );
        if idx == length {
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

#[inline]
pub fn target_idx(idx: i32, origin_schema: i32, target_schema: i32) -> i32 {
    ((idx - 1) >> (origin_schema - target_schema)) + 1
}

fn bucket_from_idx<BC>(
    schema: i32,
    positive: bool,
    curr_idx: i32,
    count: BC,
    custom_values: &[f64],
) -> Bucket<BC> {
    let (lower, upper) = if positive {
        (
            get_bound(curr_idx - 1, schema, custom_values),
            get_bound(curr_idx, schema, custom_values),
        )
    } else {
        (
            -get_bound(curr_idx, schema, custom_values),
            -get_bound(curr_idx - 1, schema, custom_values),
        )
    };
    let (lower_inclusive, upper_inclusive) = if is_custom_buckets_schema(schema) {
        (curr_idx == 0, true)
    } else {
        (lower < 0.0, upper > 0.0)
    };
    Bucket {
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
        count,
        index: curr_idx,
    }
}


pub trait CompactBucketNum: Copy + PartialEq {
    fn is_zero(self) -> bool;
    fn zero_val() -> Self;
    fn wrapping_add(self, other: Self) -> Self;
    fn neg_val(self) -> Self;
}

impl CompactBucketNum for i64 {
    #[inline]
    fn is_zero(self) -> bool { self == 0 }
    #[inline]
    fn zero_val() -> Self { 0 }
    #[inline]
    fn wrapping_add(self, other: Self) -> Self { self.wrapping_add(other) }
    #[inline]
    fn neg_val(self) -> Self { self.wrapping_neg() }
}

impl CompactBucketNum for f64 {
    #[inline]
    fn is_zero(self) -> bool { self == 0.0 }
    #[inline]
    fn zero_val() -> Self { 0.0 }
    #[inline]
    fn wrapping_add(self, other: Self) -> Self { self + other }
    #[inline]
    fn neg_val(self) -> Self { -self }
}

pub fn compact_buckets_generic<T: CompactBucketNum>(
    primary_buckets: &mut Vec<T>,
    mut compensation_buckets: Option<&mut Vec<f64>>,
    spans: &mut Vec<HistogramSpan>,
    max_empty_buckets: usize,
    delta_buckets: bool,
) {
    if delta_buckets && compensation_buckets.is_some() {
        panic!("histogram type mismatch: deltaBuckets cannot be true when compensationBuckets is provided");
    }
    if let Some(ref cb) = compensation_buckets {
        assert_eq!(
            primary_buckets.len(),
            cb.len(),
            "primary buckets layout mismatch against compensation buckets layout"
        );
    }

    let mut nothing_to_do = true;
    let mut current_abs = T::zero_val();
    for &b in primary_buckets.iter() {
        current_abs = if delta_buckets {
            current_abs.wrapping_add(b)
        } else {
            b
        };
        if current_abs.is_zero() {
            nothing_to_do = false;
            break;
        }
    }
    if nothing_to_do {
        for s in spans.iter() {
            if (s.offset as isize) <= (max_empty_buckets as isize) || s.length == 0 {
                nothing_to_do = false;
                break;
            }
        }
        if nothing_to_do {
            return;
        }
    }

    if spans.len() > 1 {
        let mut i_span = 0usize;
        for i in 1..spans.len() {
            let s = spans[i];
            if s.offset == 0 {
                spans[i_span].length = spans[i_span].length.wrapping_add(s.length);
                continue;
            }
            i_span += 1;
            if i != i_span {
                spans[i_span] = s;
            }
        }
        spans.truncate(i_span + 1);
    }

    let mut i_span = 0usize;
    for i in 0..spans.len() {
        let s = spans[i];
        if s.length == 0 {
            if i + 1 < spans.len() {
                spans[i + 1].offset = spans[i + 1].offset.wrapping_add(s.offset);
            }
            continue;
        }
        if i != i_span {
            spans[i_span] = s;
        }
        i_span += 1;
    }
    spans.truncate(i_span);

    if spans.is_empty() {
        primary_buckets.clear();
        if let Some(cb) = compensation_buckets.as_deref_mut() {
            cb.clear();
        }
        return;
    }

    let mut i_bucket = 0usize;
    let mut i_span = 0usize;
    let mut pos_in_span = 0u32;
    current_abs = T::zero_val();

    while i_bucket < primary_buckets.len() && i_span < spans.len() {
        current_abs = if delta_buckets {
            current_abs.wrapping_add(primary_buckets[i_bucket])
        } else {
            primary_buckets[i_bucket]
        };

        let n_empty = {
            let mut cnt = 0usize;
            let mut abs = current_abs;
            let mut comp = compensation_buckets
                .as_deref()
                .map_or(0.0, |cb| cb[i_bucket]);
            while (cnt as u32) + pos_in_span < spans[i_span].length && abs.is_zero() && comp == 0.0 {
                cnt += 1;
                if cnt + i_bucket >= primary_buckets.len() {
                    break;
                }
                abs = primary_buckets[cnt + i_bucket];
                comp = compensation_buckets
                    .as_deref()
                    .map_or(0.0, |cb| cb[cnt + i_bucket]);
            }
            cnt
        };

        if n_empty > 0 {
            if pos_in_span > 0
                && n_empty < (spans[i_span].length - pos_in_span) as usize
                && n_empty <= max_empty_buckets
            {
                i_bucket += n_empty;
                if delta_buckets {
                    current_abs = T::zero_val();
                }
                pos_in_span += n_empty as u32;
                continue;
            }
            if delta_buckets && i_bucket + n_empty < primary_buckets.len() {
                current_abs = primary_buckets[i_bucket].neg_val();
                let cur_val = primary_buckets[i_bucket];
                primary_buckets[i_bucket + n_empty] =
                    primary_buckets[i_bucket + n_empty].wrapping_add(cur_val);
            }
            primary_buckets.drain(i_bucket..i_bucket + n_empty);
            if let Some(cb) = compensation_buckets.as_deref_mut() {
                cb.drain(i_bucket..i_bucket + n_empty);
            }
            if pos_in_span == 0 {
                if n_empty == spans[i_span].length as usize {
                    let offset = spans[i_span].offset;
                    spans.remove(i_span);
                    if spans.len() > i_span {
                        spans[i_span].offset = spans[i_span]
                            .offset
                            .wrapping_add(offset)
                            .wrapping_add(n_empty as i32);
                    }
                    continue;
                }
                spans[i_span].length -= n_empty as u32;
                spans[i_span].offset = spans[i_span].offset.wrapping_add(n_empty as i32);
                continue;
            }
            let new_span = HistogramSpan {
                offset: n_empty as i32,
                length: spans[i_span].length - pos_in_span - (n_empty as u32),
            };
            spans[i_span].length = pos_in_span;
            i_span += 1;
            pos_in_span = 0;
            if new_span.length == 0 {
                if i_span < spans.len() {
                    spans[i_span].offset = spans[i_span].offset.wrapping_add(n_empty as i32);
                }
                continue;
            }
            spans.insert(i_span, new_span);
            continue;
        }

        i_bucket += 1;
        pos_in_span += 1;
        if pos_in_span >= spans[i_span].length {
            pos_in_span = 0;
            i_span += 1;
        }
    }

    if max_empty_buckets == 0 || primary_buckets.is_empty() {
        return;
    }

    let mut i_bucket = spans[0].length as usize;
    if delta_buckets {
        current_abs = T::zero_val();
        for &b in &primary_buckets[..i_bucket] {
            current_abs = current_abs.wrapping_add(b);
        }
    }
    let mut i_span = 1usize;
    while i_span < spans.len() {
        if (spans[i_span].offset as isize) > (max_empty_buckets as isize) {
            let l = spans[i_span].length as usize;
            if delta_buckets {
                for &b in &primary_buckets[i_bucket..i_bucket + l] {
                    current_abs = current_abs.wrapping_add(b);
                }
            }
            i_bucket += l;
            i_span += 1;
            continue;
        }
        let offset = spans[i_span].offset as usize;
        let l = spans[i_span].length as usize;
        spans[i_span - 1].length += (offset as u32) + spans[i_span].length;
        spans.remove(i_span);

        primary_buckets.splice(i_bucket..i_bucket, std::iter::repeat_n(T::zero_val(), offset));
        if delta_buckets {
            primary_buckets[i_bucket] = current_abs.neg_val();
            primary_buckets[i_bucket + offset] =
                primary_buckets[i_bucket + offset].wrapping_add(current_abs);
        }
        if let Some(cb) = compensation_buckets.as_deref_mut() {
            cb.splice(i_bucket..i_bucket, std::iter::repeat_n(0.0, offset));
        }
        i_bucket += offset;
        if delta_buckets {
            current_abs = T::zero_val();
            for &b in &primary_buckets[i_bucket..i_bucket + l] {
                current_abs = current_abs.wrapping_add(b);
            }
        }
        i_bucket += l;
    }
}

pub fn reduce_resolution_generic<T: CompactBucketNum>(
    origin_spans: &[HistogramSpan],
    origin_buckets: &[T],
    origin_schema: i32,
    target_schema: i32,
    delta_buckets: bool,
) -> Result<(Vec<HistogramSpan>, Vec<T>), HistogramValidationError> {
    let mut target_spans: Vec<HistogramSpan> = Vec::new();
    let mut target_buckets: Vec<T> = Vec::new();
    let mut bucket_idx = 0i32;
    let mut bucket_count_idx = 0usize;
    let mut last_bucket_count = T::zero_val();
    let mut last_target_bucket_idx = 0i32;
    let mut last_target_bucket_count = T::zero_val();

    for (n, span) in origin_spans.iter().enumerate() {
        if n > 0 && span.offset < 0 {
            return Err(HistogramValidationError::new(
                HistogramErrorKind::SpanNegativeOffset,
                format!("span number {} with offset {}", n + 1, span.offset),
            ));
        }
        bucket_idx = bucket_idx.wrapping_add(span.offset);
        for _ in 0..span.length {
            if bucket_count_idx >= origin_buckets.len() {
                return Err(HistogramValidationError::new(
                    HistogramErrorKind::SpansBucketsMismatch,
                    format!("have {} buckets but spans need more", origin_buckets.len()),
                ));
            }
            let target_bucket_idx = target_idx(bucket_idx, origin_schema, target_schema);
            let orig_val = origin_buckets[bucket_count_idx];
            if target_spans.is_empty() {
                target_spans.push(HistogramSpan {
                    offset: target_bucket_idx,
                    length: 1,
                });
                target_buckets.push(orig_val);
                last_target_bucket_idx = target_bucket_idx;
                last_bucket_count = orig_val;
                last_target_bucket_count = orig_val;
            } else if last_target_bucket_idx == target_bucket_idx {
                if delta_buckets {
                    last_bucket_count = last_bucket_count.wrapping_add(orig_val);
                    let last = target_buckets.last_mut().unwrap();
                    *last = last.wrapping_add(last_bucket_count);
                    last_target_bucket_count = last_target_bucket_count.wrapping_add(last_bucket_count);
                } else {
                    let last = target_buckets.last_mut().unwrap();
                    *last = last.wrapping_add(orig_val);
                }
            } else if last_target_bucket_idx + 1 == target_bucket_idx {
                target_spans.last_mut().unwrap().length += 1;
                last_target_bucket_idx += 1;
                if delta_buckets {
                    last_bucket_count = last_bucket_count.wrapping_add(orig_val);
                    target_buckets.push(last_bucket_count.wrapping_add(last_target_bucket_count.neg_val()));
                    last_target_bucket_count = last_bucket_count;
                } else {
                    target_buckets.push(orig_val);
                }
            } else {
                target_spans.push(HistogramSpan {
                    offset: target_bucket_idx - last_target_bucket_idx - 1,
                    length: 1,
                });
                last_target_bucket_idx = target_bucket_idx;
                if delta_buckets {
                    last_bucket_count = last_bucket_count.wrapping_add(orig_val);
                    target_buckets.push(last_bucket_count.wrapping_add(last_target_bucket_count.neg_val()));
                    last_target_bucket_count = last_bucket_count;
                } else {
                    target_buckets.push(orig_val);
                }
            }
            bucket_idx = bucket_idx.wrapping_add(1);
            bucket_count_idx += 1;
        }
    }
    if bucket_count_idx != origin_buckets.len() {
        return Err(HistogramValidationError::new(
            HistogramErrorKind::SpansBucketsMismatch,
            format!(
                "spans need {bucket_count_idx} buckets, have {} buckets",
                origin_buckets.len()
            ),
        ));
    }
    Ok((target_spans, target_buckets))
}

#[derive(Clone, Debug)]
pub struct RegularIntBucketIter<'a> {
    spans: &'a [HistogramSpan],
    buckets: &'a [i64],
    schema: i32,
    positive: bool,
    custom_values: &'a [f64],
    spans_idx: usize,
    idx_in_span: u32,
    buckets_idx: usize,
    curr_count: i64,
    curr_idx: i32,
}

impl<'a> RegularIntBucketIter<'a> {
    fn new(
        spans: &'a [HistogramSpan],
        buckets: &'a [i64],
        schema: i32,
        positive: bool,
        custom_values: &'a [f64],
    ) -> Self {
        Self {
            spans,
            buckets,
            schema,
            positive,
            custom_values,
            spans_idx: 0,
            idx_in_span: 0,
            buckets_idx: 0,
            curr_count: 0,
            curr_idx: 0,
        }
    }

    pub fn next_bucket(&mut self) -> bool {
        if self.spans_idx >= self.spans.len() {
            return false;
        }
        let mut span = self.spans[self.spans_idx];
        if self.buckets_idx == 0 {
            self.curr_idx = span.offset;
        } else {
            self.curr_idx += 1;
        }
        while self.idx_in_span >= span.length {
            self.idx_in_span = 0;
            self.spans_idx += 1;
            if self.spans_idx >= self.spans.len() {
                return false;
            }
            span = self.spans[self.spans_idx];
            self.curr_idx += span.offset;
        }
        if self.buckets_idx >= self.buckets.len() {
            return false;
        }
        self.curr_count = self.curr_count.wrapping_add(self.buckets[self.buckets_idx]);
        self.idx_in_span += 1;
        self.buckets_idx += 1;
        true
    }

    pub fn at(&self) -> Bucket<u64> {
        bucket_from_idx(
            self.schema,
            self.positive,
            self.curr_idx,
            self.curr_count as u64,
            self.custom_values,
        )
    }
}

impl Iterator for RegularIntBucketIter<'_> {
    type Item = Bucket<u64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_bucket() {
            Some(self.at())
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct CumulativeIntBucketIter<'a> {
    h: &'a Histogram,
    pos_spans_idx: isize,
    pos_buckets_idx: usize,
    idx_in_span: u32,
    curr_idx: i32,
    curr_upper: f64,
    curr_count: i64,
    curr_cumulative_count: u64,
    empty_bucket_count: i32,
    initialized: bool,
}

impl<'a> CumulativeIntBucketIter<'a> {
    fn new(h: &'a Histogram) -> Self {
        Self {
            h,
            pos_spans_idx: -1,
            pos_buckets_idx: 0,
            idx_in_span: 0,
            curr_idx: 0,
            curr_upper: 0.0,
            curr_count: 0,
            curr_cumulative_count: 0,
            empty_bucket_count: 0,
            initialized: false,
        }
    }

    pub fn next_bucket(&mut self) -> bool {
        if self.pos_spans_idx == -1 {
            self.pos_spans_idx = 0;
            if self.h.zero_count == 0 {
                return self.next_bucket();
            }
            self.curr_upper = self.h.zero_threshold;
            self.curr_count = self.h.zero_count as i64;
            self.curr_cumulative_count = self.h.zero_count;
            return true;
        }
        let s_idx = self.pos_spans_idx as usize;
        if s_idx >= self.h.positive_spans.len() {
            return false;
        }
        if self.empty_bucket_count > 0 {
            self.curr_upper = get_bound(self.curr_idx, self.h.schema, &self.h.custom_values);
            self.curr_idx += 1;
            self.empty_bucket_count -= 1;
            return true;
        }
        let span = self.h.positive_spans[s_idx];
        if s_idx == 0 && !self.initialized {
            self.curr_idx = span.offset;
            self.curr_count = 0;
            self.initialized = true;
        }
        if self.pos_buckets_idx >= self.h.positive_buckets.len() {
            return false;
        }
        self.curr_count = self.curr_count.wrapping_add(self.h.positive_buckets[self.pos_buckets_idx]);
        self.curr_cumulative_count = self.curr_cumulative_count.wrapping_add(self.curr_count as u64);
        self.curr_upper = get_bound(self.curr_idx, self.h.schema, &self.h.custom_values);
        self.pos_buckets_idx += 1;
        self.idx_in_span += 1;
        self.curr_idx += 1;
        if self.idx_in_span >= span.length {
            self.pos_spans_idx += 1;
            self.idx_in_span = 0;
            let ns = self.pos_spans_idx as usize;
            if ns < self.h.positive_spans.len() {
                self.empty_bucket_count = self.h.positive_spans[ns].offset;
            }
        }
        true
    }

    pub fn at(&self) -> Bucket<u64> {
        Bucket {
            lower: f64::NEG_INFINITY,
            upper: self.curr_upper,
            lower_inclusive: true,
            upper_inclusive: true,
            count: self.curr_cumulative_count,
            index: self.curr_idx - 1,
        }
    }
}

impl Iterator for CumulativeIntBucketIter<'_> {
    type Item = Bucket<u64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_bucket() {
            Some(self.at())
        } else {
            None
        }
    }
}

impl Histogram {
    pub fn copy(&self) -> Self {
        let mut c = Self {
            counter_reset_hint: self.counter_reset_hint,
            schema: self.schema,
            count: self.count,
            sum: self.sum,
            ..Self::default()
        };
        if self.uses_custom_buckets() {
            c.custom_values = self.custom_values.clone();
        } else {
            c.zero_threshold = self.zero_threshold;
            c.zero_count = self.zero_count;
            c.negative_spans = self.negative_spans.clone();
            c.negative_buckets = self.negative_buckets.clone();
        }
        c.positive_spans = self.positive_spans.clone();
        c.positive_buckets = self.positive_buckets.clone();
        c
    }

    pub fn copy_to(&self, to: &mut Self) {
        to.counter_reset_hint = self.counter_reset_hint;
        to.schema = self.schema;
        to.count = self.count;
        to.sum = self.sum;
        if self.uses_custom_buckets() {
            to.zero_threshold = 0.0;
            to.zero_count = 0;
            to.negative_spans.clear();
            to.negative_buckets.clear();
            to.custom_values = self.custom_values.clone();
        } else {
            to.zero_threshold = self.zero_threshold;
            to.zero_count = self.zero_count;
            to.negative_spans.clear();
            to.negative_spans.extend_from_slice(&self.negative_spans);
            to.negative_buckets.clear();
            to.negative_buckets.extend_from_slice(&self.negative_buckets);
            to.custom_values.clear();
        }
        to.positive_spans.clear();
        to.positive_spans.extend_from_slice(&self.positive_spans);
        to.positive_buckets.clear();
        to.positive_buckets.extend_from_slice(&self.positive_buckets);
    }

    pub fn zero_bucket(&self) -> Bucket<u64> {
        assert!(
            !self.uses_custom_buckets(),
            "histograms with custom buckets have no zero bucket"
        );
        Bucket {
            lower: -self.zero_threshold,
            upper: self.zero_threshold,
            lower_inclusive: true,
            upper_inclusive: true,
            count: self.zero_count,
            index: 0,
        }
    }

    pub fn positive_bucket_iter(&self) -> RegularIntBucketIter<'_> {
        RegularIntBucketIter::new(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            true,
            &self.custom_values,
        )
    }

    pub fn negative_bucket_iter(&self) -> RegularIntBucketIter<'_> {
        RegularIntBucketIter::new(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            false,
            &[],
        )
    }

    pub fn cumulative_bucket_iter(&self) -> CumulativeIntBucketIter<'_> {
        assert!(
            self.negative_buckets.is_empty(),
            "CumulativeBucketIterator called on Histogram with negative buckets"
        );
        CumulativeIntBucketIter::new(self)
    }

    pub fn compact(&mut self, max_empty_buckets: usize) -> &mut Self {
        compact_buckets_generic(
            &mut self.positive_buckets,
            None,
            &mut self.positive_spans,
            max_empty_buckets,
            true,
        );
        compact_buckets_generic(
            &mut self.negative_buckets,
            None,
            &mut self.negative_spans,
            max_empty_buckets,
            true,
        );
        self
    }

    pub fn to_float_into(&self, dst: Option<&mut FloatHistogram>) -> FloatHistogram {
        let mut owned;
        let fh = match dst {
            Some(d) => d,
            None => {
                owned = FloatHistogram::default();
                &mut owned
            }
        };
        fh.counter_reset_hint = self.counter_reset_hint;
        fh.schema = self.schema;
        fh.count = self.count as f64;
        fh.sum = self.sum;
        if self.uses_custom_buckets() {
            fh.zero_threshold = 0.0;
            fh.zero_count = 0.0;
            fh.negative_spans.clear();
            fh.negative_buckets.clear();
            fh.custom_values = self.custom_values.clone();
        } else {
            fh.zero_threshold = self.zero_threshold;
            fh.zero_count = self.zero_count as f64;
            fh.negative_spans.clear();
            fh.negative_spans.extend_from_slice(&self.negative_spans);
            fh.negative_buckets.clear();
            let mut cur = 0i64;
            for &b in &self.negative_buckets {
                cur = cur.wrapping_add(b);
                fh.negative_buckets.push(cur as f64);
            }
            fh.custom_values.clear();
        }
        fh.positive_spans.clear();
        fh.positive_spans.extend_from_slice(&self.positive_spans);
        fh.positive_buckets.clear();
        let mut cur = 0i64;
        for &b in &self.positive_buckets {
            cur = cur.wrapping_add(b);
            fh.positive_buckets.push(cur as f64);
        }
        fh.clone()
    }

    pub fn reduce_resolution(&mut self, target_schema: i32) -> Result<(), ReduceResolutionError> {
        if self.uses_custom_buckets() {
            return Err(ReduceResolutionError::InvalidArg(
                "cannot reduce resolution when there are custom buckets".to_string(),
            ));
        }
        if is_custom_buckets_schema(target_schema) {
            return Err(ReduceResolutionError::InvalidArg(
                "cannot reduce resolution to custom buckets schema".to_string(),
            ));
        }
        if target_schema >= self.schema {
            return Err(ReduceResolutionError::InvalidArg(format!(
                "cannot reduce resolution from schema {} to {}",
                self.schema, target_schema
            )));
        }
        let (ps, pb) = reduce_resolution_generic(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            target_schema,
            true,
        )?;
        let (ns, nb) = reduce_resolution_generic(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            target_schema,
            true,
        )?;
        self.positive_spans = ps;
        self.positive_buckets = pb;
        self.negative_spans = ns;
        self.negative_buckets = nb;
        self.schema = target_schema;
        Ok(())
    }
}

impl std::fmt::Display for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{count:{}, sum:{}", self.count, format_go_g(self.sum))?;
        let n_buckets: Vec<Bucket<u64>> = self
            .negative_bucket_iter()
            .filter(|b| b.count != 0)
            .collect();
        for b in n_buckets.iter().rev() {
            write!(f, ", {b}")?;
        }
        if self.zero_count != 0 {
            write!(f, ", {}", self.zero_bucket())?;
        }
        for b in self.positive_bucket_iter() {
            if b.count != 0 {
                write!(f, ", {b}")?;
            }
        }
        f.write_str("}")
    }
}


#[derive(Clone, Debug)]
pub struct FloatBucketIter<'a> {
    spans: &'a [HistogramSpan],
    buckets: &'a [f64],
    schema: i32,
    target_schema: i32,
    positive: bool,
    custom_values: &'a [f64],
    spans_idx: usize,
    idx_in_span: u32,
    buckets_idx: usize,
    curr_count: f64,
    curr_idx: i32,
    orig_idx: i32,
    absolute_start_value: f64,
    bound_reached_start_value: bool,
}

impl<'a> FloatBucketIter<'a> {
    pub fn next_bucket(&mut self) -> bool {
        if self.spans_idx >= self.spans.len() {
            return false;
        }
        let mut span = self.spans[self.spans_idx];
        if self.schema == self.target_schema {
            if self.buckets_idx == 0 {
                self.curr_idx = span.offset;
            } else {
                self.curr_idx += 1;
            }
            if self.buckets_idx >= self.buckets.len() {
                return false;
            }
            while self.idx_in_span >= span.length {
                self.idx_in_span = 0;
                self.spans_idx += 1;
                if self.spans_idx >= self.spans.len() {
                    return false;
                }
                span = self.spans[self.spans_idx];
                self.curr_idx += span.offset;
            }
            self.curr_count = self.buckets[self.buckets_idx];
            self.idx_in_span += 1;
            self.buckets_idx += 1;
        } else {
            let mut orig_idx = self.orig_idx;
            let mut spans_idx = self.spans_idx;
            let mut idx_in_span = self.idx_in_span;
            let mut first_pass = true;
            self.curr_count = 0.0;
            loop {
                if self.buckets_idx == 0 {
                    orig_idx = span.offset;
                } else {
                    orig_idx += 1;
                }
                if self.buckets_idx >= self.buckets.len() {
                    if first_pass {
                        return false;
                    }
                    break;
                }
                while idx_in_span >= span.length {
                    idx_in_span = 0;
                    spans_idx += 1;
                    if spans_idx >= self.spans.len() {
                        if first_pass {
                            return false;
                        }
                        break;
                    }
                    span = self.spans[spans_idx];
                    orig_idx += span.offset;
                }
                if spans_idx >= self.spans.len() {
                    break;
                }
                let curr_idx = target_idx(orig_idx, self.schema, self.target_schema);
                if first_pass {
                    self.curr_idx = curr_idx;
                    first_pass = false;
                } else if curr_idx != self.curr_idx {
                    break;
                }
                self.curr_count += self.buckets[self.buckets_idx];
                idx_in_span += 1;
                self.buckets_idx += 1;
                self.orig_idx = orig_idx;
                self.spans_idx = spans_idx;
                self.idx_in_span = idx_in_span;
            }
        }
        if !self.bound_reached_start_value
            && is_exponential_schema(self.target_schema)
            && get_bound_exponential(self.curr_idx, self.target_schema) <= self.absolute_start_value
        {
            return self.next_bucket();
        }
        self.bound_reached_start_value = true;
        true
    }

    pub fn at(&self) -> Bucket<f64> {
        bucket_from_idx(
            self.target_schema,
            self.positive,
            self.curr_idx,
            self.curr_count,
            self.custom_values,
        )
    }

    fn stripped_at(&self) -> (f64, i32) {
        (self.curr_count, self.curr_idx)
    }
}

impl Iterator for FloatBucketIter<'_> {
    type Item = Bucket<f64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_bucket() {
            Some(self.at())
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReverseFloatBucketIter<'a> {
    spans: &'a [HistogramSpan],
    buckets: &'a [f64],
    schema: i32,
    positive: bool,
    custom_values: &'a [f64],
    spans_idx: isize,
    idx_in_span: i32,
    buckets_idx: isize,
    curr_count: f64,
    curr_idx: i32,
}

impl<'a> ReverseFloatBucketIter<'a> {
    fn new(
        spans: &'a [HistogramSpan],
        buckets: &'a [f64],
        schema: i32,
        positive: bool,
        custom_values: &'a [f64],
    ) -> Self {
        let spans_idx = (spans.len() as isize) - 1;
        let buckets_idx = (buckets.len() as isize) - 1;
        let idx_in_span = if spans_idx >= 0 {
            (spans[spans_idx as usize].length as i32) - 1
        } else {
            0
        };
        let mut curr_idx = 0i32;
        for s in spans {
            curr_idx = curr_idx.wrapping_add(s.offset).wrapping_add(s.length as i32);
        }
        Self {
            spans,
            buckets,
            schema,
            positive,
            custom_values,
            spans_idx,
            idx_in_span,
            buckets_idx,
            curr_count: 0.0,
            curr_idx,
        }
    }

    pub fn next_bucket(&mut self) -> bool {
        self.curr_idx -= 1;
        if self.buckets_idx < 0 {
            return false;
        }
        while self.idx_in_span < 0 {
            self.spans_idx -= 1;
            if self.spans_idx < 0 {
                return false;
            }
            let s_idx = self.spans_idx as usize;
            self.idx_in_span = (self.spans[s_idx].length as i32) - 1;
            self.curr_idx -= self.spans[s_idx + 1].offset;
        }
        self.curr_count = self.buckets[self.buckets_idx as usize];
        self.buckets_idx -= 1;
        self.idx_in_span -= 1;
        true
    }

    pub fn at(&self) -> Bucket<f64> {
        bucket_from_idx(
            self.schema,
            self.positive,
            self.curr_idx,
            self.curr_count,
            self.custom_values,
        )
    }
}

impl Iterator for ReverseFloatBucketIter<'_> {
    type Item = Bucket<f64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_bucket() {
            Some(self.at())
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct AllFloatBucketIter<'a> {
    h: &'a FloatHistogram,
    left_iter: ReverseFloatBucketIter<'a>,
    right_iter: FloatBucketIter<'a>,
    state: i8,
    curr_bucket: Option<Bucket<f64>>,
}

impl<'a> AllFloatBucketIter<'a> {
    pub fn next_bucket(&mut self) -> bool {
        match self.state {
            -1 => {
                if self.left_iter.next_bucket() {
                    let mut b = self.left_iter.at();
                    if b.upper < 0.0 && b.upper > -self.h.zero_threshold {
                        b.upper = -self.h.zero_threshold;
                    } else if b.lower > 0.0 && b.lower < self.h.zero_threshold {
                        b.lower = self.h.zero_threshold;
                    }
                    self.curr_bucket = Some(b);
                    return true;
                }
                self.state = 0;
                self.next_bucket()
            }
            0 => {
                self.state = 1;
                if self.h.zero_count > 0.0 {
                    self.curr_bucket = Some(self.h.zero_bucket());
                    return true;
                }
                self.next_bucket()
            }
            1 => {
                if self.right_iter.next_bucket() {
                    let mut b = self.right_iter.at();
                    if b.lower > 0.0 && b.lower < self.h.zero_threshold {
                        b.lower = self.h.zero_threshold;
                    } else if b.upper < 0.0 && b.upper > -self.h.zero_threshold {
                        b.upper = -self.h.zero_threshold;
                    }
                    self.curr_bucket = Some(b);
                    return true;
                }
                self.state = 42;
                false
            }
            _ => false,
        }
    }

    pub fn at(&self) -> Bucket<f64> {
        self.curr_bucket.expect("at() called before next()")
    }
}

impl Iterator for AllFloatBucketIter<'_> {
    type Item = Bucket<f64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_bucket() {
            Some(self.at())
        } else {
            None
        }
    }
}

impl FloatHistogram {
    pub fn copy(&self) -> Self {
        let mut c = Self {
            counter_reset_hint: self.counter_reset_hint,
            schema: self.schema,
            count: self.count,
            sum: self.sum,
            ..Self::default()
        };
        if self.uses_custom_buckets() {
            c.custom_values = self.custom_values.clone();
        } else {
            c.zero_threshold = self.zero_threshold;
            c.zero_count = self.zero_count;
            c.negative_spans = self.negative_spans.clone();
            c.negative_buckets = self.negative_buckets.clone();
        }
        c.positive_spans = self.positive_spans.clone();
        c.positive_buckets = self.positive_buckets.clone();
        c
    }

    pub fn copy_to(&self, to: &mut Self) {
        to.counter_reset_hint = self.counter_reset_hint;
        to.schema = self.schema;
        to.count = self.count;
        to.sum = self.sum;
        if self.uses_custom_buckets() {
            to.zero_threshold = 0.0;
            to.zero_count = 0.0;
            to.negative_spans.clear();
            to.negative_buckets.clear();
            to.custom_values = self.custom_values.clone();
        } else {
            to.zero_threshold = self.zero_threshold;
            to.zero_count = self.zero_count;
            to.negative_spans.clear();
            to.negative_spans.extend_from_slice(&self.negative_spans);
            to.negative_buckets.clear();
            to.negative_buckets.extend_from_slice(&self.negative_buckets);
            to.custom_values.clear();
        }
        to.positive_spans.clear();
        to.positive_spans.extend_from_slice(&self.positive_spans);
        to.positive_buckets.clear();
        to.positive_buckets.extend_from_slice(&self.positive_buckets);
    }

    pub fn copy_to_schema(&self, target_schema: i32) -> Result<Self, HistogramValidationError> {
        if target_schema == self.schema {
            return Ok(self.copy());
        }
        if self.uses_custom_buckets() {
            panic!("cannot reduce resolution to {target_schema} when there are custom buckets");
        }
        if is_custom_buckets_schema(target_schema) {
            panic!("cannot reduce resolution to custom buckets schema");
        }
        if target_schema > self.schema {
            panic!("cannot copy from schema {} to {target_schema}", self.schema);
        }
        let (ps, pb) = reduce_resolution_generic(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            target_schema,
            false,
        )?;
        let (ns, nb) = reduce_resolution_generic(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            target_schema,
            false,
        )?;
        Ok(Self {
            counter_reset_hint: self.counter_reset_hint,
            schema: target_schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            positive_spans: ps,
            negative_spans: ns,
            positive_buckets: pb,
            negative_buckets: nb,
            custom_values: Vec::new(),
        })
    }

    pub fn zero_bucket(&self) -> Bucket<f64> {
        assert!(
            !self.uses_custom_buckets(),
            "histograms with custom buckets have no zero bucket"
        );
        Bucket {
            lower: -self.zero_threshold,
            upper: self.zero_threshold,
            lower_inclusive: true,
            upper_inclusive: true,
            count: self.zero_count,
            index: 0,
        }
    }

    pub fn size(&self) -> usize {
        168 + (self.positive_spans.len()
            + self.negative_spans.len()
            + self.positive_buckets.len()
            + self.negative_buckets.len()
            + self.custom_values.len())
            * 8
    }

    pub fn mul(&mut self, factor: f64) -> &mut Self {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for b in &mut self.positive_buckets {
            *b *= factor;
        }
        for b in &mut self.negative_buckets {
            *b *= factor;
        }
        if factor < 0.0 {
            self.counter_reset_hint = CounterResetHint::GaugeType;
        }
        self
    }

    pub fn div(&mut self, scalar: f64) -> &mut Self {
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        if scalar == 0.0 {
            self.positive_buckets.clear();
            self.negative_buckets.clear();
            self.positive_spans.clear();
            self.negative_spans.clear();
            return self;
        }
        for b in &mut self.positive_buckets {
            *b /= scalar;
        }
        for b in &mut self.negative_buckets {
            *b /= scalar;
        }
        if scalar < 0.0 {
            self.counter_reset_hint = CounterResetHint::GaugeType;
        }
        self
    }

    pub fn compact(&mut self, max_empty_buckets: usize) -> &mut Self {
        compact_buckets_generic(
            &mut self.positive_buckets,
            None,
            &mut self.positive_spans,
            max_empty_buckets,
            false,
        );
        compact_buckets_generic(
            &mut self.negative_buckets,
            None,
            &mut self.negative_spans,
            max_empty_buckets,
            false,
        );
        self
    }

    pub fn kahan_compact(&mut self, max_empty_buckets: usize, c: &mut Self) {
        compact_buckets_generic(
            &mut self.positive_buckets,
            Some(&mut c.positive_buckets),
            &mut self.positive_spans,
            max_empty_buckets,
            false,
        );
        compact_buckets_generic(
            &mut self.negative_buckets,
            Some(&mut c.negative_buckets),
            &mut self.negative_spans,
            max_empty_buckets,
            false,
        );
    }

    pub fn float_bucket_iter(
        &self,
        positive: bool,
        absolute_start_value: f64,
        target_schema: i32,
    ) -> FloatBucketIter<'_> {
        if self.uses_custom_buckets() && target_schema != self.schema {
            panic!("cannot merge from custom buckets schema to exponential schema");
        }
        if !self.uses_custom_buckets() && is_custom_buckets_schema(target_schema) {
            panic!("cannot merge from exponential buckets schema to custom schema");
        }
        if target_schema > self.schema {
            panic!("cannot merge from schema {} to {target_schema}", self.schema);
        }
        let (spans, buckets, custom_values): (&[HistogramSpan], &[f64], &[f64]) = if positive {
            (&self.positive_spans, &self.positive_buckets, &self.custom_values)
        } else {
            (&self.negative_spans, &self.negative_buckets, &[])
        };
        FloatBucketIter {
            spans,
            buckets,
            schema: self.schema,
            target_schema,
            positive,
            custom_values,
            spans_idx: 0,
            idx_in_span: 0,
            buckets_idx: 0,
            curr_count: 0.0,
            curr_idx: 0,
            orig_idx: 0,
            absolute_start_value,
            bound_reached_start_value: absolute_start_value == 0.0,
        }
    }

    pub fn positive_bucket_iter(&self) -> FloatBucketIter<'_> {
        self.float_bucket_iter(true, 0.0, self.schema)
    }

    pub fn negative_bucket_iter(&self) -> FloatBucketIter<'_> {
        self.float_bucket_iter(false, 0.0, self.schema)
    }

    pub fn positive_reverse_bucket_iter(&self) -> ReverseFloatBucketIter<'_> {
        ReverseFloatBucketIter::new(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            true,
            &self.custom_values,
        )
    }

    pub fn negative_reverse_bucket_iter(&self) -> ReverseFloatBucketIter<'_> {
        ReverseFloatBucketIter::new(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            false,
            &[],
        )
    }

    pub fn all_bucket_iter(&self) -> AllFloatBucketIter<'_> {
        AllFloatBucketIter {
            h: self,
            left_iter: self.negative_reverse_bucket_iter(),
            right_iter: self.positive_bucket_iter(),
            state: -1,
            curr_bucket: None,
        }
    }

    pub fn all_reverse_bucket_iter(&self) -> AllFloatBucketIter<'_> {
        AllFloatBucketIter {
            h: self,
            left_iter: self.positive_reverse_bucket_iter(),
            right_iter: self.negative_bucket_iter(),
            state: -1,
            curr_bucket: None,
        }
    }

    pub fn reduce_resolution(&mut self, target_schema: i32) -> Result<(), ReduceResolutionError> {
        if self.uses_custom_buckets() {
            return Err(ReduceResolutionError::InvalidArg(
                "cannot reduce resolution when there are custom buckets".to_string(),
            ));
        }
        if is_custom_buckets_schema(target_schema) {
            return Err(ReduceResolutionError::InvalidArg(
                "cannot reduce resolution to custom buckets schema".to_string(),
            ));
        }
        if target_schema >= self.schema {
            return Err(ReduceResolutionError::InvalidArg(format!(
                "cannot reduce resolution from schema {} to {}",
                self.schema, target_schema
            )));
        }
        let (ps, pb) = reduce_resolution_generic(
            &self.positive_spans,
            &self.positive_buckets,
            self.schema,
            target_schema,
            false,
        )?;
        let (ns, nb) = reduce_resolution_generic(
            &self.negative_spans,
            &self.negative_buckets,
            self.schema,
            target_schema,
            false,
        )?;
        self.positive_spans = ps;
        self.positive_buckets = pb;
        self.negative_spans = ns;
        self.negative_buckets = nb;
        self.schema = target_schema;
        Ok(())
    }

    pub fn has_overflow(&self) -> bool {
        self.zero_count.is_infinite()
            || self.count.is_infinite()
            || self.sum.is_infinite()
            || self.positive_buckets.iter().any(|v| v.is_infinite())
            || self.negative_buckets.iter().any(|v| v.is_infinite())
            || self.custom_values.iter().any(|v| v.is_infinite())
    }


    fn zero_count_for_larger_threshold(
        &self,
        mut larger_threshold: f64,
        c: Option<&Self>,
    ) -> (f64, f64, f64) {
        let mut c_zero_count = c.map_or(0.0, |ch| ch.zero_count);
        if larger_threshold == self.zero_threshold {
            return (self.zero_count, larger_threshold, c_zero_count);
        }
        assert!(
            larger_threshold >= self.zero_threshold,
            "new threshold {larger_threshold} is less than old threshold {}",
            self.zero_threshold
        );
        'outer: loop {
            let mut h_zero_count = self.zero_count;
            let mut it = self.positive_bucket_iter();
            let mut buckets_idx = 0usize;
            while it.next_bucket() {
                let b = it.at();
                if b.lower >= larger_threshold {
                    break;
                }
                (h_zero_count, c_zero_count) = kahan_inc(b.count, h_zero_count, c_zero_count);
                if let Some(ch) = c {
                    (h_zero_count, c_zero_count) =
                        kahan_inc(ch.positive_buckets[buckets_idx], h_zero_count, c_zero_count);
                }
                if b.upper > larger_threshold {
                    if b.count != 0.0 {
                        larger_threshold = b.upper;
                    }
                    break;
                }
                buckets_idx += 1;
            }
            let mut it_neg = self.negative_bucket_iter();
            let mut neg_idx = 0usize;
            while it_neg.next_bucket() {
                let b = it_neg.at();
                if b.upper <= -larger_threshold {
                    break;
                }
                (h_zero_count, c_zero_count) = kahan_inc(b.count, h_zero_count, c_zero_count);
                if let Some(ch) = c {
                    (h_zero_count, c_zero_count) =
                        kahan_inc(ch.negative_buckets[neg_idx], h_zero_count, c_zero_count);
                }
                if b.lower < -larger_threshold {
                    if b.count != 0.0 {
                        larger_threshold = -b.lower;
                        continue 'outer;
                    }
                    break;
                }
                neg_idx += 1;
            }
            return (h_zero_count, larger_threshold, c_zero_count);
        }
    }

    fn trim_buckets_in_zero_bucket(&mut self, mut c: Option<&mut Self>) {
        let mut pos_count = 0usize;
        {
            let mut it = self.positive_bucket_iter();
            while it.next_bucket() {
                let b = it.at();
                if b.lower >= self.zero_threshold {
                    break;
                }
                pos_count += 1;
            }
        }
        for idx in 0..pos_count {
            self.positive_buckets[idx] = 0.0;
            if let Some(ch) = c.as_deref_mut() {
                ch.positive_buckets[idx] = 0.0;
            }
        }
        let mut neg_count = 0usize;
        {
            let mut it = self.negative_bucket_iter();
            while it.next_bucket() {
                let b = it.at();
                if b.upper <= -self.zero_threshold {
                    break;
                }
                neg_count += 1;
            }
        }
        for idx in 0..neg_count {
            self.negative_buckets[idx] = 0.0;
            if let Some(ch) = c.as_deref_mut() {
                ch.negative_buckets[idx] = 0.0;
            }
        }
        if let Some(ch) = c {
            self.kahan_compact(0, ch);
        } else {
            self.compact(0);
        }
    }

    fn reconcile_zero_buckets(
        &mut self,
        other: &Self,
        mut c: Option<&mut Self>,
    ) -> (f64, f64) {
        let mut other_zero_count = other.zero_count;
        let mut other_zero_threshold = other.zero_threshold;
        let mut other_c_zero_count = 0.0;
        while other_zero_threshold != self.zero_threshold {
            if self.zero_threshold > other_zero_threshold {
                (other_zero_count, other_zero_threshold, other_c_zero_count) =
                    other.zero_count_for_larger_threshold(self.zero_threshold, None);
            }
            if other_zero_threshold > self.zero_threshold {
                let c_zero_count;
                (self.zero_count, self.zero_threshold, c_zero_count) =
                    self.zero_count_for_larger_threshold(other_zero_threshold, c.as_deref());
                if let Some(ch) = c.as_deref_mut() {
                    ch.zero_count = c_zero_count;
                }
                self.trim_buckets_in_zero_bucket(c.as_deref_mut());
            }
        }
        (other_zero_count, other_c_zero_count)
    }

    fn check_schema_and_bounds(&self, other: &Self) -> Result<(), HistogramValidationError> {
        if self.uses_custom_buckets() != other.uses_custom_buckets() {
            return Err(HistogramValidationError::sentinel(
                HistogramErrorKind::IncompatibleSchema,
            ));
        }
        Ok(())
    }

    fn check_spans_for_resolution_reduction(&self, other: &Self) -> Result<(), HistogramValidationError> {
        let to_reduce = if other.schema < self.schema {
            self
        } else if other.schema > self.schema {
            other
        } else {
            return Ok(());
        };
        check_histogram_spans(&to_reduce.positive_spans, to_reduce.positive_buckets.len())?;
        check_histogram_spans(&to_reduce.negative_spans, to_reduce.negative_buckets.len())
    }

    fn adjust_counter_reset(&mut self, other: &Self) -> bool {
        if other.counter_reset_hint == self.counter_reset_hint
            || self.counter_reset_hint == CounterResetHint::GaugeType
            || self.counter_reset_hint == CounterResetHint::UnknownCounterReset
        {
            false
        } else if other.counter_reset_hint == CounterResetHint::GaugeType {
            self.counter_reset_hint = CounterResetHint::GaugeType;
            false
        } else if other.counter_reset_hint == CounterResetHint::UnknownCounterReset {
            self.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            false
        } else {
            self.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            true
        }
    }

    pub fn add(&mut self, other: &Self) -> Result<(bool, bool), HistogramValidationError> {
        self.check_schema_and_bounds(other)?;
        self.check_spans_for_resolution_reduction(other)?;
        let counter_reset_collision = self.adjust_counter_reset(other);
        if !self.uses_custom_buckets() {
            let (other_zero, _) = self.reconcile_zero_buckets(other, None);
            self.zero_count += other_zero;
        }
        self.count += other.count;
        self.sum += other.sum;

        if self.uses_custom_buckets() {
            if custom_bucket_bounds_match(&self.custom_values, &other.custom_values) {
                add_buckets(
                    self.schema,
                    self.zero_threshold,
                    false,
                    &mut self.positive_spans,
                    &mut self.positive_buckets,
                    &other.positive_spans,
                    &other.positive_buckets,
                );
                return Ok((counter_reset_collision, false));
            }
            let intersected = intersect_custom_bucket_bounds(&self.custom_values, &other.custom_values);
            let (new_spans, new_buckets, _) = add_custom_buckets_with_mismatches(
                false,
                &self.positive_spans,
                &self.positive_buckets,
                &self.custom_values,
                &other.positive_spans,
                &other.positive_buckets,
                &other.custom_values,
                None,
                &intersected,
            );
            self.positive_spans = new_spans;
            self.positive_buckets = new_buckets;
            self.custom_values = intersected;
            return Ok((counter_reset_collision, true));
        }

        let other_pos_spans;
        let other_pos_buckets;
        let other_neg_spans;
        let other_neg_buckets;
        let (ops, opb, ons, onb): (&[HistogramSpan], &[f64], &[HistogramSpan], &[f64]) =
            if other.schema < self.schema {
                (self.positive_spans, self.positive_buckets) = reduce_resolution_generic(
                    &self.positive_spans,
                    &self.positive_buckets,
                    self.schema,
                    other.schema,
                    false,
                )?;
                (self.negative_spans, self.negative_buckets) = reduce_resolution_generic(
                    &self.negative_spans,
                    &self.negative_buckets,
                    self.schema,
                    other.schema,
                    false,
                )?;
                self.schema = other.schema;
                (
                    &other.positive_spans,
                    &other.positive_buckets,
                    &other.negative_spans,
                    &other.negative_buckets,
                )
            } else if other.schema > self.schema {
                (other_pos_spans, other_pos_buckets) = reduce_resolution_generic(
                    &other.positive_spans,
                    &other.positive_buckets,
                    other.schema,
                    self.schema,
                    false,
                )?;
                (other_neg_spans, other_neg_buckets) = reduce_resolution_generic(
                    &other.negative_spans,
                    &other.negative_buckets,
                    other.schema,
                    self.schema,
                    false,
                )?;
                (
                    &other_pos_spans,
                    &other_pos_buckets,
                    &other_neg_spans,
                    &other_neg_buckets,
                )
            } else {
                (
                    &other.positive_spans,
                    &other.positive_buckets,
                    &other.negative_spans,
                    &other.negative_buckets,
                )
            };

        add_buckets(
            self.schema,
            self.zero_threshold,
            false,
            &mut self.positive_spans,
            &mut self.positive_buckets,
            ops,
            opb,
        );
        add_buckets(
            self.schema,
            self.zero_threshold,
            false,
            &mut self.negative_spans,
            &mut self.negative_buckets,
            ons,
            onb,
        );
        Ok((counter_reset_collision, false))
    }

    fn new_compensation_histogram(&self) -> Self {
        Self {
            counter_reset_hint: self.counter_reset_hint,
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            zero_count: 0.0,
            count: 0.0,
            sum: 0.0,
            positive_spans: self.positive_spans.clone(),
            negative_spans: self.negative_spans.clone(),
            positive_buckets: vec![0.0; self.positive_buckets.len()],
            negative_buckets: if self.uses_custom_buckets() {
                Vec::new()
            } else {
                vec![0.0; self.negative_buckets.len()]
            },
            custom_values: self.custom_values.clone(),
        }
    }

    pub fn kahan_add(
        &mut self,
        other: &Self,
        c: Option<Self>,
    ) -> Result<(Self, bool, bool), HistogramValidationError> {
        self.check_schema_and_bounds(other)?;
        let counter_reset_collision = self.adjust_counter_reset(other);
        let mut c = c.unwrap_or_else(|| self.new_compensation_histogram());

        if !self.uses_custom_buckets() {
            let (other_zero, other_c_zero) = self.reconcile_zero_buckets(other, Some(&mut c));
            (self.zero_count, c.zero_count) = kahan_inc(other_zero, self.zero_count, c.zero_count);
            (self.zero_count, c.zero_count) =
                kahan_inc(other_c_zero, self.zero_count, c.zero_count);
        }
        (self.count, c.count) = kahan_inc(other.count, self.count, c.count);
        (self.sum, c.sum) = kahan_inc(other.sum, self.sum, c.sum);

        if self.uses_custom_buckets() {
            if custom_bucket_bounds_match(&self.custom_values, &other.custom_values) {
                kahan_add_buckets(
                    self.schema,
                    self.zero_threshold,
                    false,
                    &mut self.positive_spans,
                    &mut self.positive_buckets,
                    &other.positive_spans,
                    &other.positive_buckets,
                    &mut c.positive_buckets,
                    None,
                );
                c.positive_spans = self.positive_spans.clone();
                return Ok((c, counter_reset_collision, false));
            }
            let intersected = intersect_custom_bucket_bounds(&self.custom_values, &other.custom_values);
            let (new_spans, new_buckets, new_cbuckets) = add_custom_buckets_with_mismatches(
                false,
                &self.positive_spans,
                &self.positive_buckets,
                &self.custom_values,
                &other.positive_spans,
                &other.positive_buckets,
                &other.custom_values,
                Some(&c.positive_buckets),
                &intersected,
            );
            self.positive_spans = new_spans;
            self.positive_buckets = new_buckets;
            c.positive_buckets = new_cbuckets;
            self.custom_values = intersected.clone();
            c.custom_values = intersected;
            c.positive_spans = self.positive_spans.clone();
            return Ok((c, counter_reset_collision, true));
        }

        let ops_buf;
        let opb_buf;
        let ons_buf;
        let onb_buf;
        let mut ocpos_buf = Vec::new();
        let mut ocneg_buf = Vec::new();
        let mut has_ocpos = false;
        let mut has_ocneg = false;

        let (ops, opb, ons, onb): (&[HistogramSpan], &[f64], &[HistogramSpan], &[f64]) =
            if other.schema < self.schema {
                (self.positive_spans, self.positive_buckets, c.positive_buckets) =
                    kahan_reduce_resolution(
                        &self.positive_spans,
                        &self.positive_buckets,
                        &c.positive_buckets,
                        self.schema,
                        other.schema,
                    );
                (self.negative_spans, self.negative_buckets, c.negative_buckets) =
                    kahan_reduce_resolution(
                        &self.negative_spans,
                        &self.negative_buckets,
                        &c.negative_buckets,
                        self.schema,
                        other.schema,
                    );
                self.schema = other.schema;
                (
                    &other.positive_spans,
                    &other.positive_buckets,
                    &other.negative_spans,
                    &other.negative_buckets,
                )
            } else if other.schema > self.schema {
                if !other.positive_buckets.is_empty() {
                    let oc = vec![0.0; other.positive_buckets.len()];
                    (ops_buf, opb_buf, ocpos_buf) = kahan_reduce_resolution(
                        &other.positive_spans,
                        &other.positive_buckets,
                        &oc,
                        other.schema,
                        self.schema,
                    );
                    has_ocpos = true;
                } else {
                    ops_buf = other.positive_spans.clone();
                    opb_buf = other.positive_buckets.clone();
                }
                if !other.negative_buckets.is_empty() {
                    let oc = vec![0.0; other.negative_buckets.len()];
                    (ons_buf, onb_buf, ocneg_buf) = kahan_reduce_resolution(
                        &other.negative_spans,
                        &other.negative_buckets,
                        &oc,
                        other.schema,
                        self.schema,
                    );
                    has_ocneg = true;
                } else {
                    ons_buf = other.negative_spans.clone();
                    onb_buf = other.negative_buckets.clone();
                }
                (&ops_buf, &opb_buf, &ons_buf, &onb_buf)
            } else {
                (
                    &other.positive_spans,
                    &other.positive_buckets,
                    &other.negative_spans,
                    &other.negative_buckets,
                )
            };

        kahan_add_buckets(
            self.schema,
            self.zero_threshold,
            false,
            &mut self.positive_spans,
            &mut self.positive_buckets,
            ops,
            opb,
            &mut c.positive_buckets,
            if has_ocpos { Some(&ocpos_buf) } else { None },
        );
        kahan_add_buckets(
            self.schema,
            self.zero_threshold,
            false,
            &mut self.negative_spans,
            &mut self.negative_buckets,
            ons,
            onb,
            &mut c.negative_buckets,
            if has_ocneg { Some(&ocneg_buf) } else { None },
        );

        c.schema = self.schema;
        c.zero_threshold = self.zero_threshold;
        c.positive_spans = self.positive_spans.clone();
        c.negative_spans = self.negative_spans.clone();

        Ok((c, counter_reset_collision, false))
    }

    pub fn sub(&mut self, other: &Self) -> Result<(bool, bool), HistogramValidationError> {
        self.check_schema_and_bounds(other)?;
        self.check_spans_for_resolution_reduction(other)?;
        let counter_reset_collision = self.adjust_counter_reset(other);
        if !self.uses_custom_buckets() {
            let (other_zero, _) = self.reconcile_zero_buckets(other, None);
            self.zero_count -= other_zero;
        }
        self.count -= other.count;
        self.sum -= other.sum;

        if self.uses_custom_buckets() {
            if custom_bucket_bounds_match(&self.custom_values, &other.custom_values) {
                add_buckets(
                    self.schema,
                    self.zero_threshold,
                    true,
                    &mut self.positive_spans,
                    &mut self.positive_buckets,
                    &other.positive_spans,
                    &other.positive_buckets,
                );
                return Ok((counter_reset_collision, false));
            }
            let intersected = intersect_custom_bucket_bounds(&self.custom_values, &other.custom_values);
            let (new_spans, new_buckets, _) = add_custom_buckets_with_mismatches(
                true,
                &self.positive_spans,
                &self.positive_buckets,
                &self.custom_values,
                &other.positive_spans,
                &other.positive_buckets,
                &other.custom_values,
                None,
                &intersected,
            );
            self.positive_spans = new_spans;
            self.positive_buckets = new_buckets;
            self.custom_values = intersected;
            return Ok((counter_reset_collision, true));
        }

        let other_pos_spans;
        let other_pos_buckets;
        let other_neg_spans;
        let other_neg_buckets;
        let (ops, opb, ons, onb): (&[HistogramSpan], &[f64], &[HistogramSpan], &[f64]) =
            if other.schema < self.schema {
                (self.positive_spans, self.positive_buckets) = reduce_resolution_generic(
                    &self.positive_spans,
                    &self.positive_buckets,
                    self.schema,
                    other.schema,
                    false,
                )?;
                (self.negative_spans, self.negative_buckets) = reduce_resolution_generic(
                    &self.negative_spans,
                    &self.negative_buckets,
                    self.schema,
                    other.schema,
                    false,
                )?;
                self.schema = other.schema;
                (
                    &other.positive_spans,
                    &other.positive_buckets,
                    &other.negative_spans,
                    &other.negative_buckets,
                )
            } else if other.schema > self.schema {
                (other_pos_spans, other_pos_buckets) = reduce_resolution_generic(
                    &other.positive_spans,
                    &other.positive_buckets,
                    other.schema,
                    self.schema,
                    false,
                )?;
                (other_neg_spans, other_neg_buckets) = reduce_resolution_generic(
                    &other.negative_spans,
                    &other.negative_buckets,
                    other.schema,
                    self.schema,
                    false,
                )?;
                (
                    &other_pos_spans,
                    &other_pos_buckets,
                    &other_neg_spans,
                    &other_neg_buckets,
                )
            } else {
                (
                    &other.positive_spans,
                    &other.positive_buckets,
                    &other.negative_spans,
                    &other.negative_buckets,
                )
            };

        add_buckets(
            self.schema,
            self.zero_threshold,
            true,
            &mut self.positive_spans,
            &mut self.positive_buckets,
            ops,
            opb,
        );
        add_buckets(
            self.schema,
            self.zero_threshold,
            true,
            &mut self.negative_spans,
            &mut self.negative_buckets,
            ons,
            onb,
        );
        Ok((counter_reset_collision, false))
    }

    pub fn detect_reset(&self, previous: &Self) -> bool {
        if self.counter_reset_hint == CounterResetHint::CounterReset {
            return true;
        }
        if self.counter_reset_hint == CounterResetHint::NotCounterReset {
            return false;
        }
        if self.count < previous.count {
            return true;
        }
        if self.uses_custom_buckets() {
            if !previous.uses_custom_buckets() {
                return true;
            }
            if !custom_bucket_bounds_match(&self.custom_values, &previous.custom_values) {
                return self.detect_reset_with_mismatched_custom_bounds(
                    previous,
                    &self.custom_values,
                    &previous.custom_values,
                );
            }
        }
        if self.schema > previous.schema {
            return true;
        }
        if self.zero_threshold < previous.zero_threshold {
            return true;
        }
        let (prev_zero_count, new_threshold, _) =
            previous.zero_count_for_larger_threshold(self.zero_threshold, None);
        if new_threshold != self.zero_threshold {
            return true;
        }
        if self.zero_count < prev_zero_count {
            return true;
        }
        let mut curr_it = self.float_bucket_iter(true, self.zero_threshold, self.schema);
        let mut prev_it = previous.float_bucket_iter(true, self.zero_threshold, self.schema);
        if detect_reset_iter(&mut curr_it, &mut prev_it) {
            return true;
        }
        let mut curr_it = self.float_bucket_iter(false, self.zero_threshold, self.schema);
        let mut prev_it = previous.float_bucket_iter(false, self.zero_threshold, self.schema);
        detect_reset_iter(&mut curr_it, &mut prev_it)
    }

    fn detect_reset_with_mismatched_custom_bounds(
        &self,
        previous: &Self,
        curr_bounds: &[f64],
        prev_bounds: &[f64],
    ) -> bool {
        assert!(
            self.schema == CUSTOM_BUCKETS_SCHEMA && previous.schema == CUSTOM_BUCKETS_SCHEMA,
            "detectResetWithMismatchedCustomBounds called with non-NHCB schema"
        );
        let mut curr_it = self.float_bucket_iter(true, 0.0, CUSTOM_BUCKETS_SCHEMA);
        let mut prev_it = previous.float_bucket_iter(true, 0.0, CUSTOM_BUCKETS_SCHEMA);

        fn rollup_sum(
            iter: &mut FloatBucketIter<'_>,
            iter_started: bool,
            mut iter_bucket: Option<Bucket<f64>>,
            bound: f64,
        ) -> (f64, Option<Bucket<f64>>, bool) {
            if !iter_started {
                if !iter.next_bucket() {
                    return (0.0, None, false);
                }
                iter_bucket = Some(iter.at());
            }
            let mut sum = 0.0;
            while let Some(b) = iter_bucket {
                if b.upper > bound {
                    return (sum, Some(b), true);
                }
                sum += b.count;
                if !iter.next_bucket() {
                    return (sum, None, false);
                }
                iter_bucket = Some(iter.at());
            }
            (sum, None, false)
        }

        let mut curr_bound_idx = 0usize;
        let mut prev_bound_idx = 0usize;
        let mut curr_bucket: Option<Bucket<f64>> = None;
        let mut prev_bucket: Option<Bucket<f64>> = None;
        let mut curr_started = false;
        let mut curr_has_more = false;
        let mut prev_started = false;
        let mut prev_has_more = false;

        while curr_bound_idx <= curr_bounds.len() && prev_bound_idx <= prev_bounds.len() {
            let curr_bound = if curr_bound_idx < curr_bounds.len() {
                curr_bounds[curr_bound_idx]
            } else {
                f64::INFINITY
            };
            let prev_bound = if prev_bound_idx < prev_bounds.len() {
                prev_bounds[prev_bound_idx]
            } else {
                f64::INFINITY
            };
            if curr_bound == prev_bound {
                let curr_sum = if !curr_started || curr_has_more {
                    let s;
                    (s, curr_bucket, curr_has_more) =
                        rollup_sum(&mut curr_it, curr_started, curr_bucket, curr_bound);
                    curr_started = true;
                    s
                } else {
                    0.0
                };
                let prev_sum = if !prev_started || prev_has_more {
                    let s;
                    (s, prev_bucket, prev_has_more) =
                        rollup_sum(&mut prev_it, prev_started, prev_bucket, curr_bound);
                    prev_started = true;
                    s
                } else {
                    0.0
                };
                if curr_sum < prev_sum {
                    return true;
                }
                curr_bound_idx += 1;
                prev_bound_idx += 1;
            } else if curr_bound < prev_bound {
                curr_bound_idx += 1;
            } else {
                prev_bound_idx += 1;
            }
        }
        false
    }

    pub fn trim_buckets(&mut self, rhs: f64, is_upper_trim: bool) -> &mut Self {
        let mut updated_count = 0.0;
        let mut updated_sum = 0.0;
        let mut trimmed_buckets = false;
        let is_custom = self.uses_custom_buckets();
        let mut has_positive = false;
        let mut has_negative = false;

        let mut pos_updates: Vec<(usize, f64)> = Vec::new();
        {
            let mut it = self.positive_bucket_iter();
            let mut i = 0usize;
            while it.next_bucket() {
                let b = it.at();
                if b.count != 0.0 {
                    has_positive = true;
                    let (keep, midpoint) = trim_bucket(b, rhs, is_upper_trim, true, is_custom);
                    updated_count += keep;
                    updated_sum += midpoint * keep;
                    if self.positive_buckets[i] != keep {
                        pos_updates.push((i, keep));
                        trimmed_buckets = true;
                    }
                }
                i += 1;
            }
        }
        for (i, keep) in pos_updates {
            self.positive_buckets[i] = keep;
        }

        let mut neg_updates: Vec<(usize, f64)> = Vec::new();
        {
            let mut it = self.negative_bucket_iter();
            let mut i = 0usize;
            while it.next_bucket() {
                let b = it.at();
                if b.count != 0.0 {
                    has_negative = true;
                    let (keep, midpoint) = trim_bucket(b, rhs, is_upper_trim, false, is_custom);
                    updated_count += keep;
                    updated_sum += midpoint * keep;
                    if self.negative_buckets[i] != keep {
                        neg_updates.push((i, keep));
                        trimmed_buckets = true;
                    }
                }
                i += 1;
            }
        }
        for (i, keep) in neg_updates {
            self.negative_buckets[i] = keep;
        }

        if self.zero_count > 0.0 {
            let (keep, midpoint) = compute_zero_bucket_trim(
                self.zero_bucket(),
                rhs,
                has_negative,
                has_positive,
                is_upper_trim,
            );
            if self.zero_count != keep {
                self.zero_count = keep;
                trimmed_buckets = true;
            }
            updated_sum += midpoint * keep;
            updated_count += keep;
        }

        if trimmed_buckets {
            self.count = updated_count;
            self.sum = updated_sum;
            self.compact(0);
        }
        self
    }
}

impl std::fmt::Display for FloatHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{count:{}, sum:{}",
            format_go_g(self.count),
            format_go_g(self.sum)
        )?;
        let n_buckets: Vec<Bucket<f64>> = self
            .negative_bucket_iter()
            .filter(|b| b.count != 0.0)
            .collect();
        for b in n_buckets.iter().rev() {
            write!(f, ", {b}")?;
        }
        if self.zero_count != 0.0 {
            write!(f, ", {}", self.zero_bucket())?;
        }
        for b in self.positive_bucket_iter() {
            if b.count != 0.0 {
                write!(f, ", {b}")?;
            }
        }
        f.write_str("}")
    }
}

fn detect_reset_iter(curr_it: &mut FloatBucketIter<'_>, prev_it: &mut FloatBucketIter<'_>) -> bool {
    if !prev_it.next_bucket() {
        return false;
    }
    let mut prev_b = prev_it.stripped_at();
    if !curr_it.next_bucket() {
        loop {
            if prev_b.0 != 0.0 {
                return true;
            }
            if !prev_it.next_bucket() {
                return false;
            }
            prev_b = prev_it.stripped_at();
        }
    }
    let mut curr_b = curr_it.stripped_at();
    loop {
        while curr_b.1 < prev_b.1 {
            if !curr_it.next_bucket() {
                loop {
                    if prev_b.0 != 0.0 {
                        return true;
                    }
                    if !prev_it.next_bucket() {
                        return false;
                    }
                    prev_b = prev_it.stripped_at();
                }
            }
            curr_b = curr_it.stripped_at();
        }
        if curr_b.1 > prev_b.1 {
            if prev_b.0 != 0.0 {
                return true;
            }
        } else if curr_b.0 < prev_b.0 {
            return true;
        }
        if !prev_it.next_bucket() {
            return false;
        }
        prev_b = prev_it.stripped_at();
    }
}

fn add_buckets(
    schema: i32,
    threshold: f64,
    negative: bool,
    spans_a: &mut Vec<HistogramSpan>,
    buckets_a: &mut Vec<f64>,
    spans_b: &[HistogramSpan],
    buckets_b: &[f64],
) {
    let mut i_span: isize = -1;
    let mut i_bucket: isize = -1;
    let mut i_in_span = 0i32;
    let mut index_a = 0i32;
    let mut index_b = 0i32;
    let mut b_idx_b = 0usize;
    let mut lower_than_threshold = true;

    for span_b in spans_b {
        index_b += span_b.offset;
        for _ in 0..span_b.length {
            let skip = lower_than_threshold
                && is_exponential_schema(schema)
                && get_bound_exponential(index_b, schema) <= threshold;
            if !skip {
                lower_than_threshold = false;
                let mut bucket_b = buckets_b[b_idx_b];
                if negative {
                    bucket_b = -bucket_b;
                }
                if i_span == -1 {
                    if spans_a.is_empty() || spans_a[0].offset > index_b {
                        buckets_a.insert(0, bucket_b);
                        if !spans_a.is_empty() && spans_a[0].offset == index_b + 1 {
                            spans_a[0].length += 1;
                            spans_a[0].offset -= 1;
                        } else {
                            spans_a.insert(
                                0,
                                HistogramSpan {
                                    offset: index_b,
                                    length: 1,
                                },
                            );
                            if spans_a.len() > 1 {
                                spans_a[1].offset -= index_b + 1;
                            }
                        }
                        index_a = index_b;
                        index_b += 1;
                        b_idx_b += 1;
                        continue;
                    } else if spans_a[0].offset == index_b {
                        buckets_a[0] += bucket_b;
                        index_a = index_b;
                        index_b += 1;
                        b_idx_b += 1;
                        continue;
                    }
                    i_span = 0;
                    i_bucket = 0;
                    i_in_span = 0;
                    index_a = spans_a[0].offset;
                }
                let mut delta_index = index_b - index_a;
                loop {
                    let s_idx = i_span as usize;
                    let remaining = (spans_a[s_idx].length as i32) - i_in_span;
                    if delta_index < remaining {
                        i_bucket += delta_index as isize;
                        i_in_span += delta_index;
                        buckets_a[i_bucket as usize] += bucket_b;
                        break;
                    }
                    delta_index -= remaining;
                    i_bucket += remaining as isize;
                    i_span += 1;
                    let ns_idx = i_span as usize;
                    if ns_idx == spans_a.len() || delta_index < spans_a[ns_idx].offset {
                        buckets_a.insert(i_bucket as usize, bucket_b);
                        if delta_index == 0 {
                            if ns_idx < spans_a.len() {
                                spans_a[ns_idx].offset -= 1;
                            }
                            i_span -= 1;
                            let prev_idx = i_span as usize;
                            i_in_span = spans_a[prev_idx].length as i32;
                            spans_a[prev_idx].length += 1;
                        } else if ns_idx < spans_a.len() && delta_index == spans_a[ns_idx].offset - 1
                        {
                            i_in_span = 0;
                            spans_a[ns_idx].offset -= 1;
                            spans_a[ns_idx].length += 1;
                        } else {
                            i_in_span = 0;
                            if ns_idx < spans_a.len() {
                                spans_a[ns_idx].offset -= delta_index + 1;
                            }
                            spans_a.insert(
                                ns_idx,
                                HistogramSpan {
                                    offset: delta_index,
                                    length: 1,
                                },
                            );
                        }
                        break;
                    } else {
                        delta_index -= spans_a[ns_idx].offset;
                        i_in_span = 0;
                    }
                }
            }
            index_a = index_b;
            index_b += 1;
            b_idx_b += 1;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn kahan_add_buckets(
    schema: i32,
    threshold: f64,
    negative: bool,
    spans_a: &mut Vec<HistogramSpan>,
    buckets_a: &mut Vec<f64>,
    spans_b: &[HistogramSpan],
    buckets_b: &[f64],
    comp_a: &mut Vec<f64>,
    comp_b: Option<&[f64]>,
) {
    let mut i_span: isize = -1;
    let mut i_bucket: isize = -1;
    let mut i_in_span = 0i32;
    let mut index_a = 0i32;
    let mut index_b = 0i32;
    let mut b_idx_b = 0usize;
    let mut lower_than_threshold = true;

    for span_b in spans_b {
        index_b += span_b.offset;
        for _ in 0..span_b.length {
            let skip = lower_than_threshold
                && is_exponential_schema(schema)
                && get_bound_exponential(index_b, schema) <= threshold;
            if !skip {
                lower_than_threshold = false;
                let mut bucket_b = buckets_b[b_idx_b];
                let mut comp_val_b = comp_b.map_or(0.0, |cb| cb[b_idx_b]);
                if negative {
                    bucket_b = -bucket_b;
                    comp_val_b = -comp_val_b;
                }
                if i_span == -1 {
                    if spans_a.is_empty() || spans_a[0].offset > index_b {
                        buckets_a.insert(0, bucket_b);
                        comp_a.insert(0, comp_val_b);
                        if !spans_a.is_empty() && spans_a[0].offset == index_b + 1 {
                            spans_a[0].length += 1;
                            spans_a[0].offset -= 1;
                        } else {
                            spans_a.insert(
                                0,
                                HistogramSpan {
                                    offset: index_b,
                                    length: 1,
                                },
                            );
                            if spans_a.len() > 1 {
                                spans_a[1].offset -= index_b + 1;
                            }
                        }
                        index_a = index_b;
                        index_b += 1;
                        b_idx_b += 1;
                        continue;
                    } else if spans_a[0].offset == index_b {
                        (buckets_a[0], comp_a[0]) = kahan_inc(bucket_b, buckets_a[0], comp_a[0]);
                        if comp_val_b != 0.0 {
                            (buckets_a[0], comp_a[0]) =
                                kahan_inc(comp_val_b, buckets_a[0], comp_a[0]);
                        }
                        index_a = index_b;
                        index_b += 1;
                        b_idx_b += 1;
                        continue;
                    }
                    i_span = 0;
                    i_bucket = 0;
                    i_in_span = 0;
                    index_a = spans_a[0].offset;
                }
                let mut delta_index = index_b - index_a;
                loop {
                    let s_idx = i_span as usize;
                    let remaining = (spans_a[s_idx].length as i32) - i_in_span;
                    if delta_index < remaining {
                        i_bucket += delta_index as isize;
                        i_in_span += delta_index;
                        let bi = i_bucket as usize;
                        (buckets_a[bi], comp_a[bi]) =
                            kahan_inc(bucket_b, buckets_a[bi], comp_a[bi]);
                        if comp_val_b != 0.0 {
                            (buckets_a[bi], comp_a[bi]) =
                                kahan_inc(comp_val_b, buckets_a[bi], comp_a[bi]);
                        }
                        break;
                    }
                    delta_index -= remaining;
                    i_bucket += remaining as isize;
                    i_span += 1;
                    let ns_idx = i_span as usize;
                    if ns_idx == spans_a.len() || delta_index < spans_a[ns_idx].offset {
                        buckets_a.insert(i_bucket as usize, bucket_b);
                        comp_a.insert(i_bucket as usize, comp_val_b);
                        if delta_index == 0 {
                            if ns_idx < spans_a.len() {
                                spans_a[ns_idx].offset -= 1;
                            }
                            i_span -= 1;
                            let prev_idx = i_span as usize;
                            i_in_span = spans_a[prev_idx].length as i32;
                            spans_a[prev_idx].length += 1;
                        } else if ns_idx < spans_a.len() && delta_index == spans_a[ns_idx].offset - 1
                        {
                            i_in_span = 0;
                            spans_a[ns_idx].offset -= 1;
                            spans_a[ns_idx].length += 1;
                        } else {
                            i_in_span = 0;
                            if ns_idx < spans_a.len() {
                                spans_a[ns_idx].offset -= delta_index + 1;
                            }
                            spans_a.insert(
                                ns_idx,
                                HistogramSpan {
                                    offset: delta_index,
                                    length: 1,
                                },
                            );
                        }
                        break;
                    } else {
                        delta_index -= spans_a[ns_idx].offset;
                        i_in_span = 0;
                    }
                }
            }
            index_a = index_b;
            index_b += 1;
            b_idx_b += 1;
        }
    }
}

fn kahan_reduce_resolution(
    origin_spans: &[HistogramSpan],
    origin_receiving_buckets: &[f64],
    origin_comp_buckets: &[f64],
    origin_schema: i32,
    target_schema: i32,
) -> (Vec<HistogramSpan>, Vec<f64>, Vec<f64>) {
    let mut target_spans: Vec<HistogramSpan> = Vec::new();
    let mut target_receiving: Vec<f64> = Vec::new();
    let mut target_comp: Vec<f64> = Vec::new();
    let mut bucket_idx = 0i32;
    let mut bucket_count_idx = 0usize;
    let mut last_target_bucket_idx = 0i32;

    for span in origin_spans {
        bucket_idx += span.offset;
        for _ in 0..span.length {
            let t_idx = target_idx(bucket_idx, origin_schema, target_schema);
            let recv_v = origin_receiving_buckets[bucket_count_idx];
            let comp_v = origin_comp_buckets[bucket_count_idx];
            if target_spans.is_empty() {
                target_spans.push(HistogramSpan {
                    offset: t_idx,
                    length: 1,
                });
                target_receiving.push(recv_v);
                target_comp.push(comp_v);
                last_target_bucket_idx = t_idx;
            } else if last_target_bucket_idx == t_idx {
                let last = target_receiving.len() - 1;
                (target_receiving[last], target_comp[last]) =
                    kahan_inc(recv_v, target_receiving[last], target_comp[last]);
                (target_receiving[last], target_comp[last]) =
                    kahan_inc(comp_v, target_receiving[last], target_comp[last]);
            } else if last_target_bucket_idx + 1 == t_idx {
                target_spans.last_mut().unwrap().length += 1;
                last_target_bucket_idx += 1;
                target_receiving.push(recv_v);
                target_comp.push(comp_v);
            } else {
                target_spans.push(HistogramSpan {
                    offset: t_idx - last_target_bucket_idx - 1,
                    length: 1,
                });
                last_target_bucket_idx = t_idx;
                target_receiving.push(recv_v);
                target_comp.push(comp_v);
            }
            bucket_idx += 1;
            bucket_count_idx += 1;
        }
    }
    (target_spans, target_receiving, target_comp)
}

fn intersect_custom_bucket_bounds(bounds_a: &[f64], bounds_b: &[f64]) -> Vec<f64> {
    if bounds_a.is_empty() || bounds_b.is_empty() {
        return Vec::new();
    }
    let mut result = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < bounds_a.len() && j < bounds_b.len() {
        if bounds_a[i] == bounds_b[j] {
            result.push(bounds_a[i]);
            i += 1;
            j += 1;
        } else if bounds_a[i] < bounds_b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn add_custom_buckets_with_mismatches(
    negative: bool,
    spans_a: &[HistogramSpan],
    buckets_a: &[f64],
    bounds_a: &[f64],
    spans_b: &[HistogramSpan],
    buckets_b: &[f64],
    bounds_b: &[f64],
    buckets_c: Option<&[f64]>,
    intersected_bounds: &[f64],
) -> (Vec<HistogramSpan>, Vec<f64>, Vec<f64>) {
    let mut target_buckets = vec![0.0; intersected_bounds.len() + 1];
    let mut c_target_buckets = vec![0.0; intersected_bounds.len() + 1];

    let mut map_buckets = |spans: &[HistogramSpan],
                           buckets: &[f64],
                           bounds: &[f64],
                           neg: bool,
                           with_comp: bool| {
        let mut src_idx = 0usize;
        let mut bucket_idx = 0usize;
        let mut intersect_idx = 0usize;
        for span in spans {
            src_idx += span.offset as usize;
            for _ in 0..span.length {
                if bucket_idx < buckets.len() {
                    let value = buckets[bucket_idx];
                    let mut target_i = target_buckets.len() - 1;
                    if src_idx < bounds.len() {
                        let src_bound = bounds[src_idx];
                        while intersect_idx < intersected_bounds.len() {
                            if intersected_bounds[intersect_idx] >= src_bound {
                                target_i = intersect_idx;
                                break;
                            }
                            intersect_idx += 1;
                        }
                    }
                    if neg {
                        (target_buckets[target_i], c_target_buckets[target_i]) =
                            kahan_dec(value, target_buckets[target_i], c_target_buckets[target_i]);
                    } else {
                        (target_buckets[target_i], c_target_buckets[target_i]) =
                            kahan_inc(value, target_buckets[target_i], c_target_buckets[target_i]);
                        if with_comp {
                            if let Some(bc) = buckets_c {
                                (target_buckets[target_i], c_target_buckets[target_i]) = kahan_inc(
                                    bc[bucket_idx],
                                    target_buckets[target_i],
                                    c_target_buckets[target_i],
                                );
                            }
                        }
                    }
                }
                src_idx += 1;
                bucket_idx += 1;
            }
        }
    };

    map_buckets(spans_a, buckets_a, bounds_a, false, true);
    map_buckets(spans_b, buckets_b, bounds_b, negative, false);

    let mut dest_spans: Vec<HistogramSpan> = Vec::new();
    let mut dest_buckets: Vec<f64> = Vec::new();
    let mut c_dest_buckets: Vec<f64> = Vec::new();
    let mut last_idx = -1i32;

    for i in 0..target_buckets.len() {
        if target_buckets[i] == 0.0 && c_target_buckets[i] == 0.0 {
            continue;
        }
        dest_buckets.push(target_buckets[i]);
        c_dest_buckets.push(c_target_buckets[i]);
        let idx = i as i32;
        if !dest_spans.is_empty() && idx == last_idx + 1 {
            dest_spans.last_mut().unwrap().length += 1;
        } else {
            let offset = if dest_spans.is_empty() {
                idx
            } else {
                idx - last_idx - 1
            };
            dest_spans.push(HistogramSpan { offset, length: 1 });
        }
        last_idx = idx;
    }

    (dest_spans, dest_buckets, c_dest_buckets)
}

fn trim_bucket(
    b: Bucket<f64>,
    rhs: f64,
    is_upper_trim: bool,
    is_positive: bool,
    is_custom_bucket: bool,
) -> (f64, f64) {
    let (keep_all, contains_trim_point) = if is_upper_trim {
        (b.upper <= rhs, b.lower < rhs)
    } else {
        (b.lower >= rhs, b.upper > rhs)
    };
    if keep_all {
        (
            b.count,
            compute_midpoint(b.lower, b.upper, is_positive, is_custom_bucket),
        )
    } else if contains_trim_point {
        compute_bucket_trim(b, rhs, is_upper_trim, is_positive, is_custom_bucket)
    } else {
        (0.0, 0.0)
    }
}

fn handle_infinity_buckets(is_upper_trim: bool, b: Bucket<f64>, rhs: f64) -> (f64, f64) {
    let zero_if_inf = |x: f64| if x.is_infinite() { 0.0 } else { x };
    if b.lower.is_infinite() && b.lower.is_sign_negative() {
        if is_upper_trim {
            if rhs >= b.upper {
                return (b.count, 0.0);
            }
            if rhs > 0.0 && b.upper > 0.0 && !b.upper.is_infinite() {
                return (b.count * rhs / b.upper, rhs / 2.0);
            }
            if b.upper <= 0.0 {
                return (b.count, rhs);
            }
            return (0.0, zero_if_inf(b.upper));
        }
        if rhs <= b.lower {
            return (b.count, 0.0);
        }
        if rhs >= 0.0 && b.upper > rhs && !b.upper.is_infinite() {
            return (b.count * (1.0 - rhs / b.upper), (rhs + b.upper) / 2.0);
        }
        return (0.0, zero_if_inf(b.upper));
    }
    if b.upper.is_infinite() && b.upper.is_sign_positive() {
        if is_upper_trim {
            return (0.0, zero_if_inf(b.lower));
        }
        if rhs >= b.lower {
            return (b.count, rhs);
        }
        return (0.0, zero_if_inf(b.lower));
    }
    panic!("one of the bounds must be infinite for handle_infinity_buckets");
}

fn compute_split(b: Bucket<f64>, rhs: f64, is_linear: bool) -> f64 {
    if rhs <= b.lower {
        return 0.0;
    }
    if rhs >= b.upper {
        return b.count;
    }
    b.count * b.fraction_below(rhs, is_linear)
}

fn compute_zero_bucket_trim(
    zero_bucket: Bucket<f64>,
    rhs: f64,
    has_negative: bool,
    has_positive: bool,
    is_upper_trim: bool,
) -> (f64, f64) {
    let mut lower = zero_bucket.lower;
    let mut upper = zero_bucket.upper;
    if has_negative && !has_positive {
        upper = 0.0;
    }
    if has_positive && !has_negative {
        lower = 0.0;
    }
    let (fraction, midpoint) = if is_upper_trim {
        if rhs <= lower {
            return (0.0, 0.0);
        }
        if rhs >= upper {
            return (zero_bucket.count, (lower + upper) / 2.0);
        }
        ((rhs - lower) / (upper - lower), (lower + rhs) / 2.0)
    } else {
        if rhs <= lower {
            return (zero_bucket.count, (lower + upper) / 2.0);
        }
        if rhs >= upper {
            return (0.0, 0.0);
        }
        ((upper - rhs) / (upper - lower), (rhs + upper) / 2.0)
    };
    (zero_bucket.count * fraction, midpoint)
}

fn compute_bucket_trim(
    b: Bucket<f64>,
    rhs: f64,
    is_upper_trim: bool,
    is_positive: bool,
    is_custom_bucket: bool,
) -> (f64, f64) {
    if (b.lower.is_infinite() && b.lower.is_sign_negative())
        || (b.upper.is_infinite() && b.upper.is_sign_positive())
    {
        return handle_infinity_buckets(is_upper_trim, b, rhs);
    }
    let under_count = compute_split(b, rhs, is_custom_bucket);
    if is_upper_trim {
        (
            under_count,
            compute_midpoint(b.lower, rhs, is_positive, is_custom_bucket),
        )
    } else {
        (
            b.count - under_count,
            compute_midpoint(rhs, b.upper, is_positive, is_custom_bucket),
        )
    }
}

fn compute_midpoint(
    surv_lower: f64,
    surv_upper: f64,
    is_positive: bool,
    is_linear: bool,
) -> f64 {
    if surv_lower.is_infinite() {
        if surv_upper.is_infinite() {
            return 0.0;
        }
        if surv_upper > 0.0 {
            return surv_upper / 2.0;
        }
        return surv_upper;
    } else if surv_upper.is_infinite() {
        return surv_lower;
    }
    if is_linear {
        return (surv_lower + surv_upper) / 2.0;
    }
    let geo_mean = (surv_lower * surv_upper).abs().sqrt();
    if is_positive {
        geo_mean
    } else {
        -geo_mean
    }
}

pub const CLASSIC_SUFFIX_BUCKET: &str = "_bucket";
pub const CLASSIC_SUFFIX_COUNT: &str = "_count";
pub const CLASSIC_SUFFIX_SUM: &str = "_sum";

#[derive(Clone, Copy, Debug)]
pub enum NhcbRef<'a> {
    Histogram(&'a Histogram),
    FloatHistogram(&'a FloatHistogram),
}

#[derive(Clone, Debug, Default)]
pub struct ClassicSeriesCache {
    lset: crate::labels::Labels,
    base_name: String,
    bucket_name: String,
    count_name: String,
    sum_name: String,
    custom_values: Vec<f64>,
    le_strings: Vec<String>,
    bucket_labels: Vec<crate::labels::Labels>,
    have_buckets: bool,
    count_labels: crate::labels::Labels,
    have_count: bool,
    sum_labels: crate::labels::Labels,
    have_sum: bool,
    positive_buckets: Vec<f64>,
}

impl ClassicSeriesCache {
    fn prepare(&mut self, lset: &crate::labels::Labels, base_name: &str) {
        if self.lset != *lset {
            self.lset = lset.clone();
            self.have_buckets = false;
            self.have_count = false;
            self.have_sum = false;
        }
        if self.base_name != base_name {
            self.base_name = base_name.to_string();
            self.bucket_name = format!("{base_name}{CLASSIC_SUFFIX_BUCKET}");
            self.count_name = format!("{base_name}{CLASSIC_SUFFIX_COUNT}");
            self.sum_name = format!("{base_name}{CLASSIC_SUFFIX_SUM}");
        }
    }

    fn custom_values_match(&self, custom_values: &[f64]) -> bool {
        self.custom_values.len() == custom_values.len()
            && self.le_strings.len() == custom_values.len()
            && self
                .custom_values
                .iter()
                .zip(custom_values.iter())
                .all(|(a, b)| a == b)
    }

    fn buckets_match(&self, custom_values: &[f64]) -> bool {
        self.have_buckets && self.custom_values_match(custom_values)
    }
}

pub fn classic_bucket_counts(
    nhcb: Option<NhcbRef<'_>>,
    buf: &mut Vec<f64>,
) -> Result<(Vec<f64>, f64, f64), String> {
    let mut positive_buckets = std::mem::take(buf);
    let (count, sum) = match nhcb {
        Some(NhcbRef::Histogram(h)) => {
            if !is_custom_buckets_schema(h.schema) {
                return Err("unsupported histogram schema, not a NHCB".to_string());
            }
            if let Err(e) = h.validate() {
                return Err(e.to_string());
            }
            positive_buckets.clear();
            positive_buckets.resize(h.custom_values.len() + 1, 0.0);
            let mut idx = 0usize;
            let mut curr_idx = 0usize;
            let mut acc = 0i64;
            for s in &h.positive_spans {
                idx += s.offset as usize;
                for _ in 0..s.length {
                    acc = acc.wrapping_add(h.positive_buckets[curr_idx]);
                    positive_buckets[idx] = acc as f64;
                    idx += 1;
                    curr_idx += 1;
                }
            }
            (h.count as f64, h.sum)
        }
        Some(NhcbRef::FloatHistogram(fh)) => {
            if !is_custom_buckets_schema(fh.schema) {
                return Err("unsupported histogram schema, not a NHCB".to_string());
            }
            if let Err(e) = fh.validate() {
                return Err(e.to_string());
            }
            positive_buckets.clear();
            positive_buckets.resize(fh.custom_values.len() + 1, 0.0);
            let mut idx = 0usize;
            let mut curr_idx = 0usize;
            for span in &fh.positive_spans {
                idx += span.offset as usize;
                for _ in 0..span.length {
                    positive_buckets[idx] = fh.positive_buckets[curr_idx];
                    idx += 1;
                    curr_idx += 1;
                }
            }
            (fh.count, fh.sum)
        }
        None => return Err("unsupported histogram type: <nil>".to_string()),
    };

    let is_stale = crate::chunkenc::is_stale_nan(sum);
    let mut curr_count = 0.0;
    for b in &mut positive_buckets {
        curr_count += *b;
        *b = if is_stale { sum } else { curr_count };
    }
    let out_count = if is_stale { sum } else { count };
    Ok((positive_buckets, out_count, sum))
}

pub fn convert_nhcb_to_classic(
    nhcb: Option<NhcbRef<'_>>,
    lset: &crate::labels::Labels,
    lset_builder: &mut crate::labels::LabelsBuilder,
    only_suffix: &str,
    mut cache: Option<&mut ClassicSeriesCache>,
    mut emit: impl FnMut(crate::labels::Labels, f64) -> Result<(), String>,
) -> Result<(), String> {
    let base_name = lset.get(crate::labels::METRIC_NAME).to_string();
    if base_name.is_empty() {
        return Err("metric name label '__name__' is missing".to_string());
    }
    if let Some(c) = cache.as_deref_mut() {
        c.prepare(lset, &base_name);
    }

    let old_labels = lset_builder.labels();
    struct ResetGuard<'a>(&'a mut crate::labels::LabelsBuilder, crate::labels::Labels);
    impl Drop for ResetGuard<'_> {
        fn drop(&mut self) {
            self.0.reset(self.1.clone());
        }
    }
    let guard = ResetGuard(lset_builder, old_labels);
    let lset_builder = &mut *guard.0;

    let want_buckets = only_suffix.is_empty() || only_suffix == CLASSIC_SUFFIX_BUCKET;
    let want_count = only_suffix.is_empty() || only_suffix == CLASSIC_SUFFIX_COUNT;
    let want_sum = only_suffix.is_empty() || only_suffix == CLASSIC_SUFFIX_SUM;

    let mut scratch = cache
        .as_deref_mut()
        .map_or_else(Vec::new, |c| std::mem::take(&mut c.positive_buckets));
    let (buckets, count, sum) = classic_bucket_counts(nhcb, &mut scratch)?;
    if let Some(c) = cache.as_deref_mut() {
        c.positive_buckets = buckets.clone();
    }

    let custom_values: &[f64] = match nhcb {
        Some(NhcbRef::Histogram(h)) => &h.custom_values,
        Some(NhcbRef::FloatHistogram(fh)) => &fh.custom_values,
        None => &[],
    };

    if want_buckets {
        if let Some(c) = cache.as_deref_mut() {
            if !c.buckets_match(custom_values) {
                if !c.custom_values_match(custom_values) {
                    c.custom_values.clear();
                    c.custom_values.extend_from_slice(custom_values);
                    c.le_strings.clear();
                    for &val in custom_values {
                        c.le_strings.push(crate::labels::format_openmetrics_float(val));
                    }
                }
                c.bucket_labels.clear();
                lset_builder.reset(lset.clone());
                lset_builder.set(crate::labels::METRIC_NAME, &c.bucket_name);
                for le_str in &c.le_strings {
                    lset_builder.set("le", le_str);
                    c.bucket_labels.push(lset_builder.labels());
                }
                lset_builder.set("le", "+Inf");
                c.bucket_labels.push(lset_builder.labels());
                c.have_buckets = true;
            }
            for (lbl, &val) in c.bucket_labels.iter().zip(buckets.iter()) {
                emit(lbl.clone(), val)?;
            }
        } else {
            for (i, &val) in custom_values.iter().enumerate() {
                lset_builder.reset(lset.clone());
                lset_builder.set(
                    crate::labels::METRIC_NAME,
                    &format!("{base_name}{CLASSIC_SUFFIX_BUCKET}"),
                );
                lset_builder.set("le", &crate::labels::format_openmetrics_float(val));
                emit(lset_builder.labels(), buckets[i])?;
            }
            lset_builder.reset(lset.clone());
            lset_builder.set(
                crate::labels::METRIC_NAME,
                &format!("{base_name}{CLASSIC_SUFFIX_BUCKET}"),
            );
            lset_builder.set("le", "+Inf");
            emit(lset_builder.labels(), buckets[custom_values.len()])?;
        }
    }

    if want_count {
        if let Some(c) = cache.as_deref_mut() {
            if !c.have_count {
                lset_builder.reset(lset.clone());
                lset_builder.set(crate::labels::METRIC_NAME, &c.count_name);
                c.count_labels = lset_builder.labels();
                c.have_count = true;
            }
            emit(c.count_labels.clone(), count)?;
        } else {
            lset_builder.reset(lset.clone());
            lset_builder.set(
                crate::labels::METRIC_NAME,
                &format!("{base_name}{CLASSIC_SUFFIX_COUNT}"),
            );
            emit(lset_builder.labels(), count)?;
        }
    }

    if want_sum {
        if let Some(c) = cache {
            if !c.have_sum {
                lset_builder.reset(lset.clone());
                lset_builder.set(crate::labels::METRIC_NAME, &c.sum_name);
                c.sum_labels = lset_builder.labels();
                c.have_sum = true;
            }
            emit(c.sum_labels.clone(), sum)?;
        } else {
            lset_builder.reset(lset.clone());
            lset_builder.set(
                crate::labels::METRIC_NAME,
                &format!("{base_name}{CLASSIC_SUFFIX_SUM}"),
            );
            emit(lset_builder.labels(), sum)?;
        }
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
