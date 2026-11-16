//! Prometheus stringlabels flat byte encoding, xxhash64 label hashing,
//! label/metric name validation, and StaleNaN / exemplar validation helpers.
//!
//! Ported from:
//! - github.com/prometheus/prometheus/model/labels/labels_stringlabels.go
//! - github.com/prometheus/prometheus/model/labels/labels_common.go
//! - github.com/prometheus/prometheus/model/value/value.go
//! - github.com/prometheus/common/model/labels.go
//! - github.com/prometheus/common/model/metric.go
//! - github.com/grafana/mimir/pkg/distributor/validate.go
//! - github.com/grafana/mimir/pkg/mimirpb/validate.go
//! - github.com/grafana/mimir/pkg/util/validation/label_value_length_over_limit_strategy.go

use std::fmt;

use xxhash_rust::xxh64::{xxh64, Xxh64};

/// Special label name representing a metric name.
pub const METRIC_NAME_LABEL: &str = "__name__";

/// Bucket upper-bound label name.
pub const BUCKET_LABEL: &str = "le";

/// Alert name label name.
pub const ALERT_NAME_LABEL: &str = "alertname";

/// Separator byte used in `HashForLabels` and `HashWithoutLabels`.
pub const HASH_SEP: u8 = 0xff;

/// Maximum encodable label name/value length in Prometheus stringlabels (2^24).
pub const MAX_STRINGLABELS_STRING_LEN: usize = 1 << 24;

/// Normal quiet NaN bit pattern in Prometheus.
pub const NORMAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0001;

/// StaleNaN signaling NaN bit pattern in Prometheus (`0x7ff0000000000002`).
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// QuietZeroNaN bit pattern in Prometheus (`0x7ff0000000000003`).
pub const QUIET_ZERO_NAN_BITS: u64 = 0x7ff0_0000_0000_0003;

/// Maximum combined UTF-8 character length of an exemplar's label set (OpenMetrics spec).
pub const EXEMPLAR_MAX_LABEL_SET_LENGTH: usize = 128;

/// Length of `(hash:<64-hex-chars>)` used when truncating/dropping oversized label values.
pub const LABEL_VALUE_HASH_LEN: usize = 7 + 32 * 2;

/// Returns a Prometheus StaleNaN `f64`.
#[inline]
pub fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN_BITS)
}

/// Returns `true` when `v` has the exact Prometheus StaleNaN bit pattern.
#[inline]
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN_BITS
}

#[inline]
fn encode_size_into(out: &mut Vec<u8>, len: usize) {
    if len < 255 {
        out.push(len as u8);
    } else if len <= MAX_STRINGLABELS_STRING_LEN {
        out.push(255);
        out.push((len & 0xff) as u8);
        out.push(((len >> 8) & 0xff) as u8);
        out.push(((len >> 16) & 0xff) as u8);
    } else {
        panic!("String too long to encode as label.");
    }
}

#[inline]
fn decode_size_checked(data: &[u8], mut index: usize) -> Result<(usize, usize), &'static str> {
    if index >= data.len() {
        return Err("unexpected end of stringlabels data");
    }
    let b = data[index];
    index += 1;
    if b == 255 {
        if index + 3 > data.len() {
            return Err("truncated 24-bit length in stringlabels data");
        }
        let len = (data[index] as usize)
            | ((data[index + 1] as usize) << 8)
            | ((data[index + 2] as usize) << 16);
        Ok((len, index + 3))
    } else {
        Ok((b as usize, index))
    }
}

#[inline]
fn decode_size(data: &[u8], mut index: usize) -> (usize, usize) {
    let b = data[index];
    index += 1;
    if b == 255 {
        let len = (data[index] as usize)
            | ((data[index + 1] as usize) << 8)
            | ((data[index + 2] as usize) << 16);
        (len, index + 3)
    } else {
        (b as usize, index)
    }
}

#[inline]
fn decode_str(data: &[u8], index: usize) -> (&str, usize) {
    let (size, start) = decode_size(data, index);
    let end = start + size;
    // All LabelsBytes are constructed from valid UTF-8 `&str` inputs.
    let s = unsafe { std::str::from_utf8_unchecked(&data[start..end]) };
    (s, end)
}

#[inline]
fn hash_len_prefixed_str(hasher: &mut Xxh64, s: &str) {
    let len = s.len();
    if len < 255 {
        hasher.update(&[len as u8]);
    } else if len <= MAX_STRINGLABELS_STRING_LEN {
        let hdr = [
            255u8,
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
        ];
        hasher.update(&hdr);
    } else {
        panic!("String too long to encode as label.");
    }
    hasher.update(s.as_bytes());
}

#[inline]
fn size_when_encoded(len: usize) -> usize {
    if len < 255 {
        1
    } else if len <= MAX_STRINGLABELS_STRING_LEN {
        4
    } else {
        panic!("String too long to encode as label.");
    }
}

/// Prometheus `stringlabels.Labels` flat byte encoding.
///
/// Each sorted `(name, value)` pair is encoded as:
/// `encode_size(name.len()) || name || encode_size(value.len()) || value`
/// where lengths `< 255` take 1 byte and lengths `>= 255` take `0xff` followed by
/// 3-byte little-endian length.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelsBytes {
    data: Vec<u8>,
}

impl LabelsBytes {
    /// Returns an empty `LabelsBytes`.
    pub fn empty() -> Self {
        Self { data: Vec::new() }
    }

    /// Encodes an unsorted slice of `(name, value)` pairs into sorted `LabelsBytes`.
    pub fn from_labels(labels: &[(&str, &str)]) -> Self {
        if labels.is_empty() {
            return Self::empty();
        }
        let mut sorted: Vec<(&str, &str)> = labels.to_vec();
        if !sorted.windows(2).all(|w| w[0].0 <= w[1].0) {
            sorted.sort_by(|a, b| a.0.cmp(b.0));
        }
        Self::from_sorted_labels(&sorted)
    }

    /// Encodes a slice of `(name, value)` pairs already sorted by `name` ascending.
    pub fn from_sorted_labels(labels: &[(&str, &str)]) -> Self {
        let cap: usize = labels
            .iter()
            .map(|(n, v)| {
                size_when_encoded(n.len()) + n.len() + size_when_encoded(v.len()) + v.len()
            })
            .sum();
        let mut data = Vec::with_capacity(cap);
        for &(name, value) in labels {
            encode_size_into(&mut data, name.len());
            data.extend_from_slice(name.as_bytes());
            encode_size_into(&mut data, value.len());
            data.extend_from_slice(value.as_bytes());
        }
        Self { data }
    }

    /// Encodes into a caller-provided reusable buffer.
    pub fn encode_sorted_into(labels: &[(&str, &str)], buf: &mut Vec<u8>) {
        buf.clear();
        for &(name, value) in labels {
            encode_size_into(buf, name.len());
            buf.extend_from_slice(name.as_bytes());
            encode_size_into(buf, value.len());
            buf.extend_from_slice(value.as_bytes());
        }
    }

    /// Validates framing and UTF-8, then wraps raw encoded bytes directly.
    pub fn from_raw(data: Vec<u8>) -> Result<Self, &'static str> {
        let mut i = 0;
        while i < data.len() {
            let (name_len, name_start) = decode_size_checked(&data, i)?;
            let name_end = name_start
                .checked_add(name_len)
                .filter(|&end| end <= data.len())
                .ok_or("truncated label name in raw stringlabels")?;
            std::str::from_utf8(&data[name_start..name_end])
                .map_err(|_| "invalid UTF-8 in label name")?;
            let (val_len, val_start) = decode_size_checked(&data, name_end)?;
            let val_end = val_start
                .checked_add(val_len)
                .filter(|&end| end <= data.len())
                .ok_or("truncated label value in raw stringlabels")?;
            std::str::from_utf8(&data[val_start..val_end])
                .map_err(|_| "invalid UTF-8 in label value")?;
            i = val_end;
        }
        Ok(Self { data })
    }

    /// Returns the raw flat byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consumes `self` and returns the underlying byte vector.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data
    }

    /// Returns `true` if there are no labels.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the byte length of the encoded buffer.
    #[inline]
    pub fn byte_size(&self) -> usize {
        self.data.len()
    }

    /// Returns the number of labels.
    pub fn len(&self) -> usize {
        let mut count = 0;
        let mut i = 0;
        while i < self.data.len() {
            let (sz, next) = decode_size(&self.data, i);
            i = next + sz;
            let (sz, next) = decode_size(&self.data, i);
            i = next + sz;
            count += 1;
        }
        count
    }

    /// Returns a `LabelsBytes` with all empty-value labels removed (`Labels.WithoutEmpty`).
    pub fn without_empty(&self) -> Self {
        let mut out = Vec::with_capacity(self.data.len());
        let mut i = 0;
        while i < self.data.len() {
            let old_pos = i;
            let (_, next) = decode_str(&self.data, i);
            let (value, next) = decode_str(&self.data, next);
            i = next;
            if !value.is_empty() {
                out.extend_from_slice(&self.data[old_pos..i]);
            }
        }
        Self { data: out }
    }

    /// Computes Prometheus `labels.Labels.Hash()` (`xxhash64` seed 0 over the flat buffer).
    #[inline]
    pub fn hash(&self) -> u64 {
        xxh64(&self.data, 0)
    }

    /// Computes Prometheus `HashForLabels` for sorted `names`.
    pub fn hash_for_labels(&self, buf: &mut Vec<u8>, names: &[&str]) -> u64 {
        buf.clear();
        let mut j = 0;
        let mut i = 0;
        while i < self.data.len() {
            let (name, next) = decode_str(&self.data, i);
            let (value, next) = decode_str(&self.data, next);
            i = next;
            while j < names.len() && names[j] < name {
                j += 1;
            }
            if j == names.len() {
                break;
            }
            if name == names[j] {
                buf.extend_from_slice(name.as_bytes());
                buf.push(HASH_SEP);
                buf.extend_from_slice(value.as_bytes());
                buf.push(HASH_SEP);
            }
        }
        xxh64(buf, 0)
    }

    /// Computes Prometheus `HashWithoutLabels` for sorted `names` (always excludes `__name__`).
    pub fn hash_without_labels(&self, buf: &mut Vec<u8>, names: &[&str]) -> u64 {
        buf.clear();
        let mut j = 0;
        let mut i = 0;
        while i < self.data.len() {
            let (name, next) = decode_str(&self.data, i);
            let (value, next) = decode_str(&self.data, next);
            i = next;
            while j < names.len() && names[j] < name {
                j += 1;
            }
            if name == METRIC_NAME_LABEL || (j < names.len() && name == names[j]) {
                continue;
            }
            buf.extend_from_slice(name.as_bytes());
            buf.push(HASH_SEP);
            buf.extend_from_slice(value.as_bytes());
            buf.push(HASH_SEP);
        }
        xxh64(buf, 0)
    }

    /// Copies encoded bytes for only labels whose name is in sorted `names`.
    pub fn bytes_with_labels(&self, buf: &mut Vec<u8>, names: &[&str]) {
        buf.clear();
        let mut j = 0;
        let mut pos = 0;
        while pos < self.data.len() {
            let (l_name, next) = decode_str(&self.data, pos);
            let (_, new_pos) = decode_str(&self.data, next);
            while j < names.len() && names[j] < l_name {
                j += 1;
            }
            if j == names.len() {
                break;
            }
            if l_name == names[j] {
                buf.extend_from_slice(&self.data[pos..new_pos]);
            }
            pos = new_pos;
        }
    }

    /// Copies encoded bytes for labels whose name is NOT in sorted `names`.
    pub fn bytes_without_labels(&self, buf: &mut Vec<u8>, names: &[&str]) {
        buf.clear();
        let mut j = 0;
        let mut pos = 0;
        while pos < self.data.len() {
            let (l_name, next) = decode_str(&self.data, pos);
            let (_, new_pos) = decode_str(&self.data, next);
            while j < names.len() && names[j] < l_name {
                j += 1;
            }
            if j == names.len() || l_name != names[j] {
                buf.extend_from_slice(&self.data[pos..new_pos]);
            }
            pos = new_pos;
        }
    }

    /// Returns the value for `name`, or `None` if not present.
    pub fn get(&self, name: &str) -> Option<&str> {
        if name.is_empty() {
            return None;
        }
        let name_b0 = name.as_bytes()[0];
        let mut i = 0;
        while i < self.data.len() {
            let (size, next) = decode_size(&self.data, i);
            i = next;
            if size > 0 && self.data[i] == name_b0 {
                let l_name = unsafe { std::str::from_utf8_unchecked(&self.data[i..i + size]) };
                i += size;
                if l_name == name {
                    let (val, _) = decode_str(&self.data, i);
                    return Some(val);
                }
            } else {
                if size > 0 && self.data[i] > name_b0 {
                    break;
                }
                i += size;
            }
            let (vsize, vnext) = decode_size(&self.data, i);
            i = vnext + vsize;
        }
        None
    }

    /// Returns `true` if the label `name` is present.
    pub fn has(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Checks whether the sorted label set contains duplicate label names.
    pub fn has_duplicate_label_names(&self) -> Option<&str> {
        let mut prev: Option<&str> = None;
        let mut i = 0;
        while i < self.data.len() {
            let (l_name, next) = decode_str(&self.data, i);
            let (_, next) = decode_str(&self.data, next);
            i = next;
            if prev == Some(l_name) {
                return Some(l_name);
            }
            prev = Some(l_name);
        }
        None
    }

    /// Iterates over `(name, value)` pairs in sorted order.
    pub fn iter(&self) -> LabelsBytesIter<'_> {
        LabelsBytesIter {
            data: &self.data,
            pos: 0,
        }
    }
}

pub struct LabelsBytesIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for LabelsBytesIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        let (name, next) = decode_str(self.data, self.pos);
        let (value, next) = decode_str(self.data, next);
        self.pos = next;
        Some((name, value))
    }
}

/// Computes `labels.Labels.Hash()` (`xxhash64` seed 0 over Prometheus `stringlabels` encoding)
/// for an arbitrary slice of `(name, value)` pairs without allocating when already sorted.
pub fn prom_labels_hash(labels: &[(&str, &str)]) -> u64 {
    if labels.is_empty() {
        return xxh64(&[], 0);
    }
    let is_sorted = labels.windows(2).all(|w| w[0].0 <= w[1].0);
    if is_sorted {
        return prom_labels_hash_sorted(labels.iter().copied());
    }
    let mut sorted = labels.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    prom_labels_hash_sorted(sorted)
}

/// Zero-allocation `labels.Labels.Hash()` for an iterator of `(name, value)` pairs
/// already sorted by `name` ascending.
pub fn prom_labels_hash_sorted<'a, I>(labels: I) -> u64
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut hasher = Xxh64::new(0);
    for (name, value) in labels {
        hash_len_prefixed_str(&mut hasher, name);
        hash_len_prefixed_str(&mut hasher, value);
    }
    hasher.digest()
}

/// Computes Prometheus `HashForLabels` directly from sorted `(name, value)` pairs
/// and sorted `names`, using `0xff` separator.
pub fn hash_for_labels<'a>(labels: &[(&'a str, &'a str)], names: &[&str]) -> u64 {
    let mut hasher = Xxh64::new(0);
    let mut j = 0;
    for &(name, value) in labels {
        while j < names.len() && names[j] < name {
            j += 1;
        }
        if j == names.len() {
            break;
        }
        if name == names[j] {
            hasher.update(name.as_bytes());
            hasher.update(&[HASH_SEP]);
            hasher.update(value.as_bytes());
            hasher.update(&[HASH_SEP]);
        }
    }
    hasher.digest()
}

/// Computes Prometheus `HashWithoutLabels` directly from sorted `(name, value)` pairs
/// and sorted `names`, using `0xff` separator and always excluding `__name__`.
pub fn hash_without_labels<'a>(labels: &[(&'a str, &'a str)], names: &[&str]) -> u64 {
    let mut hasher = Xxh64::new(0);
    let mut j = 0;
    for &(name, value) in labels {
        while j < names.len() && names[j] < name {
            j += 1;
        }
        if name == METRIC_NAME_LABEL || (j < names.len() && name == names[j]) {
            continue;
        }
        hasher.update(name.as_bytes());
        hasher.update(&[HASH_SEP]);
        hasher.update(value.as_bytes());
        hasher.update(&[HASH_SEP]);
    }
    hasher.digest()
}

/// Prometheus metric & label name validation scheme (`model.ValidationScheme`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidationScheme {
    Unset,
    Legacy,
    #[default]
    Utf8,
}

impl ValidationScheme {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "legacy" => Ok(Self::Legacy),
            "utf8" => Ok(Self::Utf8),
            other => Err(format!("unrecognized ValidationScheme: {other:?}")),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Legacy => "legacy",
            Self::Utf8 => "utf8",
        }
    }

    /// Matches Prometheus `ValidationScheme.IsValidLabelName`.
    pub fn is_valid_label_name(&self, label_name: &str) -> bool {
        match self {
            Self::Legacy => is_valid_legacy_label_name(label_name),
            Self::Utf8 => !label_name.is_empty(),
            Self::Unset => panic!("Invalid name validation scheme requested: unset"),
        }
    }

    /// Matches Prometheus `ValidationScheme.IsValidMetricName`.
    pub fn is_valid_metric_name(&self, metric_name: &str) -> bool {
        match self {
            Self::Legacy => is_valid_legacy_metric_name(metric_name),
            Self::Utf8 => !metric_name.is_empty(),
            Self::Unset => panic!("Invalid name validation scheme requested: unset"),
        }
    }
}

impl fmt::Display for ValidationScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Matches Prometheus `LegacyValidation.IsValidLabelName` (`^[a-zA-Z_][a-zA-Z0-9_]*$`).
#[inline]
pub fn is_valid_legacy_label_name(label_name: &str) -> bool {
    let bytes = label_name.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        let valid = matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'_') || (i > 0 && b.is_ascii_digit());
        if !valid {
            return false;
        }
    }
    true
}

/// Matches Prometheus `LegacyValidation.IsValidMetricName` (`^[a-zA-Z_:][a-zA-Z0-9_:]*$`).
#[inline]
pub fn is_valid_legacy_metric_name(metric_name: &str) -> bool {
    let bytes = metric_name.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        let valid =
            matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'_' | b':') || (i > 0 && b.is_ascii_digit());
        if !valid {
            return false;
        }
    }
    true
}

/// Matches Prometheus `UTF8Validation.IsValidLabelName` on raw bytes.
#[inline]
pub fn is_valid_utf8_label_name_bytes(bytes: &[u8]) -> bool {
    !bytes.is_empty() && std::str::from_utf8(bytes).is_ok()
}

/// Matches Prometheus `model.LabelValue.IsValid()` on raw bytes (`utf8.ValidString`).
#[inline]
pub fn is_valid_label_value_bytes(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok()
}

/// Matches `mimirpb.AreLabelNamesSortedAndUnique`.
pub fn are_label_names_sorted_and_unique(labels: &[(&str, &str)]) -> bool {
    for i in 1..labels.len() {
        if labels[i].0 <= labels[i - 1].0 {
            return false;
        }
    }
    true
}

/// Strategy when a label value exceeds `MaxLabelValueLength`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LabelValueLengthOverLimitStrategy {
    #[default]
    Error,
    Truncate,
    Drop,
}

impl LabelValueLengthOverLimitStrategy {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "" | "error" => Ok(Self::Error),
            "truncate" => Ok(Self::Truncate),
            "drop" => Ok(Self::Drop),
            other => Err(format!(
                "unrecognized LabelValueLengthOverLimitStrategy: {other:?}"
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Truncate => "truncate",
            Self::Drop => "drop",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stale_nan() {
        let s = stale_nan();
        assert!(s.is_nan());
        assert!(is_stale_nan(s));
        assert!(!is_stale_nan(f64::NAN));
        assert!(!is_stale_nan(f64::from_bits(NORMAL_NAN_BITS)));
        assert!(!is_stale_nan(f64::from_bits(QUIET_ZERO_NAN_BITS)));
        assert!(!is_stale_nan(0.0));
    }

    #[test]
    fn test_validation_schemes() {
        assert!(ValidationScheme::Legacy.is_valid_label_name("abc_123"));
        assert!(!ValidationScheme::Legacy.is_valid_label_name("123_abc"));
        assert!(!ValidationScheme::Legacy.is_valid_label_name("a:b"));
        assert!(!ValidationScheme::Legacy.is_valid_label_name(""));
        assert!(ValidationScheme::Legacy.is_valid_metric_name("a:b_1"));
        assert!(!ValidationScheme::Legacy.is_valid_metric_name("1:b"));

        assert!(ValidationScheme::Utf8.is_valid_label_name("a.b/c:d 🚀"));
        assert!(!ValidationScheme::Utf8.is_valid_label_name(""));
        assert!(ValidationScheme::Utf8.is_valid_metric_name("http.server.duration"));
        assert!(!ValidationScheme::Utf8.is_valid_metric_name(""));

        assert!(is_valid_label_value_bytes(b"hello\xf0\x9f\x9a\x80"));
        assert!(!is_valid_label_value_bytes(b"\xff\xfe"));
    }

    #[test]
    fn test_labels_bytes_and_hash_for_and_without() {
        let lb = LabelsBytes::from_labels(&[
            ("job", "api"),
            ("__name__", "http_requests_total"),
            ("instance", "localhost:8080"),
        ]);
        assert_eq!(lb.len(), 3);
        assert_eq!(lb.get("__name__"), Some("http_requests_total"));
        assert_eq!(lb.get("job"), Some("api"));
        assert_eq!(lb.get("missing"), None);
        assert!(lb.has("instance"));
        assert!(!lb.has(""));
        assert_eq!(lb.has_duplicate_label_names(), None);

        assert_eq!(
            lb.hash(),
            prom_labels_hash(&[
                ("job", "api"),
                ("__name__", "http_requests_total"),
                ("instance", "localhost:8080"),
            ])
        );

        let mut buf = Vec::new();
        let h_for = lb.hash_for_labels(&mut buf, &["instance", "job"]);
        let h_for_direct = hash_for_labels(
            &[
                ("__name__", "http_requests_total"),
                ("instance", "localhost:8080"),
                ("job", "api"),
            ],
            &["instance", "job"],
        );
        assert_eq!(h_for, h_for_direct);

        // HashWithoutLabels always excludes __name__
        let h_without = lb.hash_without_labels(&mut buf, &["instance"]);
        let h_without_direct = hash_without_labels(
            &[
                ("__name__", "http_requests_total"),
                ("instance", "localhost:8080"),
                ("job", "api"),
            ],
            &["instance"],
        );
        assert_eq!(h_without, h_without_direct);
        assert_eq!(
            h_without,
            hash_for_labels(&[("job", "api")], &["job"])
        );
    }

    #[test]
    fn test_long_label_255_plus() {
        let long_name = "n".repeat(255);
        let long_val = "v".repeat(300);
        let lb = LabelsBytes::from_labels(&[("a", "1"), (&long_name, &long_val)]);
        assert_eq!(lb.get(&long_name), Some(long_val.as_str()));
        assert_eq!(lb.get("a"), Some("1"));
        assert_eq!(
            lb.hash(),
            prom_labels_hash(&[(&long_name, &long_val), ("a", "1")])
        );
        // Verify 0xff + 3-byte LE prefix for >= 255
        let raw = lb.as_bytes();
        // first label "a"="1" -> [1, b'a', 1, b'1'] (4 bytes)
        assert_eq!(&raw[0..4], &[1, b'a', 1, b'1']);
        // second label name len 255 -> [255, 255, 0, 0]
        assert_eq!(&raw[4..8], &[255, 255, 0, 0]);
        let val_offset = 8 + 255;
        // second label value len 300 = 0x12c -> [255, 0x2c, 0x01, 0x00]
        assert_eq!(&raw[val_offset..val_offset + 4], &[255, 0x2c, 0x01, 0x00]);
    }

    #[test]
    fn test_from_raw_validates_utf8_and_bounds() {
        let valid = LabelsBytes::from_labels(&[("a", "1"), ("b", "2")]).into_bytes();
        let parsed = LabelsBytes::from_raw(valid).expect("valid raw labels");
        assert_eq!(parsed.get("a"), Some("1"));
        assert_eq!(parsed.get("b"), Some("2"));

        // Invalid UTF-8 in label value
        assert!(LabelsBytes::from_raw(vec![1, b'a', 1, 0xff]).is_err());
        // Invalid UTF-8 in label name
        assert!(LabelsBytes::from_raw(vec![1, 0xff, 1, b'1']).is_err());
        // Truncated payload
        assert!(LabelsBytes::from_raw(vec![1, b'a']).is_err());
        assert!(LabelsBytes::from_raw(vec![5, b'a', b'b']).is_err());
    }
}
