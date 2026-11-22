//! Prometheus label sets, interned `SymbolLabels`, and bit-exact `stringlabels` hashing.
//!
//! Ported from github.com/prometheus/prometheus/model/labels/labels_stringlabels.go,
//! labels_common.go, float.go, cost.go, regexp.go, and sharding_stringlabels.go.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use bytes::Bytes;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;
use thiserror::Error;
use xxhash_rust::xxh64::xxh64;

/// Error returned when parsing raw `stringlabels` bytes.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StringLabelsError {
    #[error("truncated stringlabels buffer")]
    Truncated,
    #[error("invalid UTF-8 in stringlabels buffer: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

use crate::matcher::LabelMatcher;
use crate::symbols::{SymbolId, SymbolTable};

/// Special label name representing a metric name.
pub const METRIC_NAME: &str = "__name__";

/// Separator byte used in `HashForLabels`, `HashWithoutLabels`, and `StableHash`.
const LABEL_SEP: u8 = 0xff;

/// Prometheus validation scheme (`github.com/prometheus/common/model.ValidationScheme`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValidationScheme {
    Legacy,
    Utf8,
}

impl ValidationScheme {
    #[inline]
    pub fn is_valid_label_name(self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }
        match self {
            Self::Legacy => {
                let b = name.as_bytes();
                let first_ok = b[0] == b'_' || b[0].is_ascii_alphabetic();
                first_ok && b[1..].iter().all(|&c| c == b'_' || c.is_ascii_alphanumeric())
            }
            Self::Utf8 => true,
        }
    }

    #[inline]
    pub fn is_valid_metric_name(self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }
        match self {
            Self::Legacy => {
                let b = name.as_bytes();
                let first_ok = b[0] == b'_' || b[0] == b':' || b[0].is_ascii_alphabetic();
                first_ok && b[1..].iter().all(|&c| c == b'_' || c == b':' || c.is_ascii_alphanumeric())
            }
            Self::Utf8 => true,
        }
    }

    #[inline]
    pub fn is_valid_label_name_bytes(self, name: &[u8]) -> bool {
        match std::str::from_utf8(name) {
            Ok(s) => self.is_valid_label_name(s),
            Err(_) => false,
        }
    }

    #[inline]
    pub fn is_valid_metric_name_bytes(self, name: &[u8]) -> bool {
        match std::str::from_utf8(name) {
            Ok(s) => self.is_valid_metric_name(s),
            Err(_) => false,
        }
    }
}

/// Writes `s` quoted identically to Go `strconv.AppendQuote`.
fn write_go_quote(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    f.write_str("\"")?;
    for ch in s.chars() {
        match ch {
            '"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\x07' => f.write_str("\\a")?,
            '\x08' => f.write_str("\\b")?,
            '\x0c' => f.write_str("\\f")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            '\x0b' => f.write_str("\\v")?,
            c if !c.is_control() => write!(f, "{c}")?,
            c if (c as u32) < 0x100 => write!(f, "\\x{:02x}", c as u32)?,
            c if (c as u32) < 0x10000 => write!(f, "\\u{:04x}", c as u32)?,
            c => write!(f, "\\U{:08x}", c as u32)?,
        }
    }
    f.write_str("\"")
}

/// An owned label name/value pair.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    #[inline]
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Encodes a label string length using Prometheus `stringlabels` varint format:
/// - `len < 255`: 1 byte (`len as u8`)
/// - `255 <= len < 2^24`: `[255, le0, le1, le2]`
#[inline]
pub fn encode_stringlabel_size(buf: &mut Vec<u8>, len: usize) {
    if len < 255 {
        buf.push(len as u8);
    } else {
        assert!(len < (1 << 24), "string too long to encode as label");
        buf.push(255);
        buf.push((len & 0xff) as u8);
        buf.push(((len >> 8) & 0xff) as u8);
        buf.push(((len >> 16) & 0xff) as u8);
    }
}

/// Appends a single `(name, value)` pair in Prometheus `stringlabels` flat byte encoding.
#[inline(always)]
pub fn append_stringlabel_pair(buf: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    let klen = name.len();
    let vlen = value.len();
    if klen < 255 && vlen < 255 {
        let need = 2 + klen + vlen;
        buf.reserve(need);
        let start = buf.len();
        // SAFETY: `reserve(need)` ensures capacity >= start + need, and `klen < 255 && vlen < 255`.
        unsafe {
            let ptr = buf.as_mut_ptr().add(start);
            *ptr = klen as u8;
            std::ptr::copy_nonoverlapping(name.as_ptr(), ptr.add(1), klen);
            *ptr.add(1 + klen) = vlen as u8;
            std::ptr::copy_nonoverlapping(value.as_ptr(), ptr.add(2 + klen), vlen);
            buf.set_len(start + need);
        }
    } else {
        append_stringlabel_pair_slow(buf, name, value);
    }
}

#[cold]
#[inline(never)]
fn append_stringlabel_pair_slow(buf: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    encode_stringlabel_size(buf, name.len());
    buf.extend_from_slice(name);
    encode_stringlabel_size(buf, value.len());
    buf.extend_from_slice(value);
}

/// Safely iterates over `(name, value)` pairs inside a raw Prometheus `stringlabels` byte slice.
///
/// Validates framing bounds and UTF-8 on each pair, returning `None` on malformed or non-UTF-8 bytes.
#[inline]
pub fn iter_stringlabels(data: &[u8]) -> StringLabelsIter<'_> {
    StringLabelsIter { data, pos: 0 }
}

/// Iterates over `(name, value)` pairs inside a pre-validated `stringlabels` byte slice.
///
/// # Safety
/// Caller must guarantee `data` is validly framed Prometheus `stringlabels` containing valid UTF-8.
#[inline]
unsafe fn iter_stringlabels_unchecked(data: &[u8]) -> LabelsIter<'_> {
    LabelsIter { data, pos: 0 }
}

#[inline]
fn try_decode_stringlabel_size(data: &[u8], pos: usize) -> Result<(usize, usize), StringLabelsError> {
    let b = *data.get(pos).ok_or(StringLabelsError::Truncated)?;
    if b == 255 {
        if pos + 4 > data.len() {
            return Err(StringLabelsError::Truncated);
        }
        let len = (data[pos + 1] as usize)
            | ((data[pos + 2] as usize) << 8)
            | ((data[pos + 3] as usize) << 16);
        Ok((len, pos + 4))
    } else {
        Ok((b as usize, pos + 1))
    }
}

#[cold]
#[inline(never)]
fn decode_stringlabel_size_slow(data: &[u8], pos: usize) -> (usize, usize) {
    let len = (data[pos + 1] as usize)
        | ((data[pos + 2] as usize) << 8)
        | ((data[pos + 3] as usize) << 16);
    (len, pos + 4)
}

#[inline(always)]
pub fn decode_stringlabel_size(data: &[u8], pos: usize) -> (usize, usize) {
    let b = data[pos];
    if b < 255 {
        (b as usize, pos + 1)
    } else {
        decode_stringlabel_size_slow(data, pos)
    }
}

/// # Safety
/// Caller must guarantee `data[pos..]` starts with a validly framed `stringlabels` UTF-8 string.
#[inline]
unsafe fn decode_stringlabel_str(data: &[u8], pos: usize) -> (&str, usize) {
    let (len, start) = decode_stringlabel_size(data, pos);
    let end = start + len;
    // SAFETY: Caller guarantees `data` contains validly framed valid UTF-8 `stringlabels`.
    let s = unsafe { std::str::from_utf8_unchecked(&data[start..end]) };
    (s, end)
}

/// Computes the exact Prometheus `stringlabels` `Labels.Hash()` (`xxhash64`) over
/// an already-sorted sequence of `(name, value)` pairs.
pub fn prom_labels_hash_sorted<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> u64 {
    let mut buf = Vec::with_capacity(128);
    for (name, value) in pairs {
        encode_stringlabel_size(&mut buf, name.len());
        buf.extend_from_slice(name.as_bytes());
        encode_stringlabel_size(&mut buf, value.len());
        buf.extend_from_slice(value.as_bytes());
    }
    xxh64(&buf, 0)
}

/// Computes the exact Prometheus `stringlabels` `Labels.Hash()` (`xxhash64`) for
/// arbitrary unsorted `(name, value)` pairs.
pub fn prom_labels_hash<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> u64 {
    let mut vec: SmallVec<[(&str, &str); 16]> = pairs.into_iter().collect();
    vec.sort_unstable_by(|a, b| a.0.cmp(b.0));
    prom_labels_hash_sorted(vec)
}

/// Flat packed label set matching Prometheus `stringlabels.Labels` wire/hash layout.
///
/// Labels are stored sorted by label name in a single contiguous buffer where each
/// name and value is prefixed by its `stringlabels` length.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Labels {
    data: Bytes,
}

impl Labels {
    /// Returns an empty `Labels` set.
    #[inline]
    pub const fn empty() -> Self {
        Self { data: Bytes::new() }
    }

    /// Wraps pre-encoded `stringlabels` bytes after validating framing and UTF-8.
    #[inline]
    pub fn from_raw_stringlabels_bytes(data: Bytes) -> Self {
        Self::try_from_raw_stringlabels_bytes(data).expect("invalid stringlabels bytes")
    }

    /// Validates `stringlabels` framing and UTF-8, wrapping `data` without copying.
    pub fn try_from_raw_stringlabels_bytes(data: Bytes) -> Result<Self, StringLabelsError> {
        let slice = data.as_ref();
        let mut pos = 0usize;
        while pos < slice.len() {
            let (nlen, nstart) = try_decode_stringlabel_size(slice, pos)?;
            let nend = nstart
                .checked_add(nlen)
                .ok_or(StringLabelsError::Truncated)?;
            if nend > slice.len() {
                return Err(StringLabelsError::Truncated);
            }
            std::str::from_utf8(&slice[nstart..nend])?;

            let (vlen, vstart) = try_decode_stringlabel_size(slice, nend)?;
            let vend = vstart
                .checked_add(vlen)
                .ok_or(StringLabelsError::Truncated)?;
            if vend > slice.len() {
                return Err(StringLabelsError::Truncated);
            }
            std::str::from_utf8(&slice[vstart..vend])?;
            pos = vend;
        }
        Ok(Self { data })
    }

    /// Wraps pre-encoded `stringlabels` bytes directly without validating UTF-8 or framing.
    ///
    /// # Safety
    /// Caller must guarantee `data` is validly framed Prometheus `stringlabels` containing valid UTF-8.
    #[inline]
    pub unsafe fn from_valid_stringlabels_bytes_unchecked(data: Bytes) -> Self {
        Self { data }
    }

    /// Constructs sorted `Labels` from `(name, value)` pairs.
    pub fn from_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let mut items: SmallVec<[(&str, &str); 16]> = pairs.into_iter().collect();
        items.sort_by(|a, b| a.0.cmp(b.0));
        Self::from_sorted_slice(&items)
    }

    /// Constructs `Labels` from an already-sorted slice of `(name, value)` pairs with exact capacity.
    pub fn from_sorted_slice(pairs: &[(&str, &str)]) -> Self {
        if pairs.is_empty() {
            return Self::empty();
        }
        let cap: usize = pairs
            .iter()
            .map(|(name, value)| {
                (if name.len() < 255 { 1 } else { 4 })
                    + name.len()
                    + (if value.len() < 255 { 1 } else { 4 })
                    + value.len()
            })
            .sum();
        let mut data = Vec::with_capacity(cap);
        for &(name, value) in pairs {
            encode_stringlabel_size(&mut data, name.len());
            data.extend_from_slice(name.as_bytes());
            encode_stringlabel_size(&mut data, value.len());
            data.extend_from_slice(value.as_bytes());
        }
        Self {
            data: Bytes::from(data),
        }
    }

    /// Constructs `Labels` from already-sorted `(name, value)` pairs.
    pub fn from_sorted_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let mut data = Vec::new();
        for (name, value) in pairs {
            encode_stringlabel_size(&mut data, name.len());
            data.extend_from_slice(name.as_bytes());
            encode_stringlabel_size(&mut data, value.len());
            data.extend_from_slice(value.as_bytes());
        }
        Self {
            data: Bytes::from(data),
        }
    }

    /// Constructs sorted `Labels` from a slice of [`Label`]s.
    pub fn from_labels(mut labels: Vec<Label>) -> Self {
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        Self::from_sorted_pairs(labels.iter().map(|l| (l.name.as_str(), l.value.as_str())))
    }

    /// Returns `true` if there are no labels.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Number of label pairs.
    pub fn len(&self) -> usize {
        let mut count = 0;
        let mut pos = 0;
        while pos < self.data.len() {
            let (nlen, p1) = decode_stringlabel_size(&self.data, pos);
            let (vlen, p2) = decode_stringlabel_size(&self.data, p1 + nlen);
            pos = p2 + vlen;
            count += 1;
        }
        count
    }

    /// Raw packed `stringlabels` bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Clones the underlying `Bytes` buffer with an atomic refcount increment (zero-copy).
    #[inline]
    pub fn clone_bytes(&self) -> Bytes {
        self.data.clone()
    }

    /// Exact Prometheus `stringlabels` `Labels.Hash()`.
    #[inline]
    pub fn hash(&self) -> u64 {
        xxh64(&self.data, 0)
    }

    /// Exact Prometheus `stringlabels.StableHash(ls)`.
    pub fn stable_hash(&self) -> u64 {
        let mut stack_buf = [0u8; 1024];
        let mut pos = 0usize;
        let mut hasher: Option<xxhash_rust::xxh64::Xxh64> = None;
        for (name, value) in self.iter() {
            let nb = name.as_bytes();
            let vb = value.as_bytes();
            if hasher.is_none() && pos + nb.len() + vb.len() + 2 >= stack_buf.len() {
                let mut h = xxhash_rust::xxh64::Xxh64::new(0);
                h.update(&stack_buf[..pos]);
                hasher = Some(h);
            }
            if let Some(ref mut h) = hasher {
                h.update(nb);
                h.update(&[0xff]);
                h.update(vb);
                h.update(&[0xff]);
            } else {
                stack_buf[pos..pos + nb.len()].copy_from_slice(nb);
                pos += nb.len();
                stack_buf[pos] = 0xff;
                pos += 1;
                stack_buf[pos..pos + vb.len()].copy_from_slice(vb);
                pos += vb.len();
                stack_buf[pos] = 0xff;
                pos += 1;
            }
        }
        match hasher {
            Some(h) => h.digest(),
            None => xxh64(&stack_buf[..pos], 0),
        }
    }

    /// Returns the value for `name`, or `""` if the label does not exist.
    pub fn get(&self, name: &str) -> &str {
        if name.is_empty() {
            return "";
        }
        let first = name.as_bytes()[0];
        let mut pos = 0;
        while pos < self.data.len() {
            let (nlen, nstart) = decode_stringlabel_size(&self.data, pos);
            let nbytes = &self.data[nstart..nstart + nlen];
            if !nbytes.is_empty() {
                if nbytes[0] == first {
                    if nbytes == name.as_bytes() {
                        // SAFETY: `Labels` guarantees valid `stringlabels` framing and UTF-8.
                        let (val, _) = unsafe { decode_stringlabel_str(&self.data, nstart + nlen) };
                        return val;
                    }
                } else if nbytes[0] > first {
                    break;
                }
            }
            let (vlen, vstart) = decode_stringlabel_size(&self.data, nstart + nlen);
            pos = vstart + vlen;
        }
        ""
    }

    /// Returns `true` if `self` has the exact same sorted `(name, value)` pairs.
    #[inline]
    pub fn eq_sorted_pairs(&self, pairs: &[(&str, &str)]) -> bool {
        let data = &self.data;
        let len = data.len();
        let mut pos = 0usize;
        for &(k, v) in pairs {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            if kb.len() < 255 && vb.len() < 255 {
                let need = 2 + kb.len() + vb.len();
                if pos + need > len {
                    return false;
                }
                let kstart = pos + 1;
                let kend = kstart + kb.len();
                if data[pos] != kb.len() as u8 || data[kend] != vb.len() as u8 {
                    return false;
                }
                if &data[kstart..kend] != kb {
                    return false;
                }
                let vstart = kend + 1;
                let vend = vstart + vb.len();
                if &data[vstart..vend] != vb {
                    return false;
                }
                pos = vend;
            } else {
                if pos >= len {
                    return false;
                }
                let (nlen, nstart) = decode_stringlabel_size(data, pos);
                if nstart + nlen > len || &data[nstart..nstart + nlen] != kb {
                    return false;
                }
                let (vlen, vstart) = decode_stringlabel_size(data, nstart + nlen);
                if vstart + vlen > len || &data[vstart..vstart + vlen] != vb {
                    return false;
                }
                pos = vstart + vlen;
            }
        }
        pos == len
    }

    /// Returns `true` if `name` is present in the label set.
    pub fn has(&self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }
        let first = name.as_bytes()[0];
        let mut pos = 0;
        while pos < self.data.len() {
            let (nlen, nstart) = decode_stringlabel_size(&self.data, pos);
            let nbytes = &self.data[nstart..nstart + nlen];
            if !nbytes.is_empty() {
                if nbytes[0] == first {
                    if nbytes == name.as_bytes() {
                        return true;
                    }
                } else if nbytes[0] > first {
                    break;
                }
            }
            let (vlen, vstart) = decode_stringlabel_size(&self.data, nstart + nlen);
            pos = vstart + vlen;
        }
        false
    }

    /// Iterates over `(name, value)` pairs in sorted order.
    pub fn iter(&self) -> LabelsIter<'_> {
        // SAFETY: `Labels` guarantees `self.data` is validly framed `stringlabels` UTF-8.
        unsafe { iter_stringlabels_unchecked(&self.data) }
    }

    /// Iterates over `(name, value)` pairs in sorted order as zero-copy [`Bytes`] slices
    /// sharing `self`'s backing buffer.
    #[inline]
    pub fn iter_bytes_slices(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        LabelsBytesIter {
            data: &self.data,
            pos: 0,
        }
    }

    /// Collects into a `Vec<Label>`.
    pub fn to_vec(&self) -> Vec<Label> {
        self.iter().map(|(n, v)| Label::new(n, v)).collect()
    }

    /// Returns a copy without any labels whose value is `""`.
    pub fn without_empty(&self) -> Self {
        if self.iter().all(|(_, v)| !v.is_empty()) {
            return self.clone();
        }
        Self::from_sorted_pairs(self.iter().filter(|(_, v)| !v.is_empty()))
    }

    /// Returns `true` if all `matchers` match this label set (with missing labels treated as `""`).
    pub fn matches(&self, matchers: &[LabelMatcher]) -> bool {
        matchers.iter().all(|m| m.matches(self.get(&m.name)))
    }

    /// Constructs sorted `Labels` from flat alternating `(name, value)` string pairs.
    ///
    /// Panics if `ss.len()` is odd (matching Go `labels.FromStrings`).
    pub fn from_strings(ss: &[&str]) -> Self {
        assert!(ss.len().is_multiple_of(2), "invalid number of strings");
        let mut items: SmallVec<[(&str, &str); 16]> = SmallVec::with_capacity(ss.len() / 2);
        for pair in ss.as_chunks::<2>().0 {
            items.push((pair[0], pair[1]));
        }
        items.sort_by(|a, b| a.0.cmp(b.0));
        Self::from_sorted_slice(&items)
    }

    /// Constructs sorted `Labels` from a map/iterator of `(name, value)` pairs.
    pub fn from_map<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self::from_pairs(pairs)
    }

    /// Returns a sorted `BTreeMap<String, String>` of the labels.
    pub fn map(&self) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        for (k, v) in self.iter() {
            m.insert(k.to_string(), v.to_string());
        }
        m
    }

    /// Returns an independent deep copy of the labels.
    pub fn copy(&self) -> Self {
        Self {
            data: Bytes::copy_from_slice(&self.data),
        }
    }

    /// Copies labels from `other` into `self`.
    #[inline]
    pub fn copy_from(&mut self, other: &Self) {
        self.data = other.data.clone();
    }

    /// Returns the encoded byte length of this label set (`Labels.ByteSize()`).
    #[inline]
    pub fn byte_size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Returns whether two label sets are equal (`labels.Equal`).
    #[inline]
    pub fn equal(a: &Self, b: &Self) -> bool {
        a.data == b.data
    }

    /// Compares two label sets (`labels.Compare`): `<0` if `a < b`, `0` if equal, `>0` if `a > b`.
    #[inline]
    pub fn compare(a: &Self, b: &Self) -> i32 {
        match a.cmp(b) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    }

    /// Checks whether the sorted label set has duplicate label names (`HasDuplicateLabelNames`).
    pub fn has_duplicate_label_names(&self) -> Option<&str> {
        let mut prev: Option<&str> = None;
        for (name, _) in self.iter() {
            if prev == Some(name) {
                return Some(name);
            }
            prev = Some(name);
        }
        None
    }

    /// Appends raw `stringlabels` bytes into `buf`, clearing `buf` first (`Labels.Bytes`).
    #[inline]
    pub fn bytes(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(&self.data);
    }

    /// Writes raw `stringlabels` bytes only for labels matching `names` (must be sorted ascending).
    pub fn bytes_with_labels(&self, buf: &mut Vec<u8>, names: &[&str]) {
        buf.clear();
        let data = &self.data;
        let mut j = 0usize;
        let mut pos = 0usize;
        while pos < data.len() {
            let (nlen, nstart) = decode_stringlabel_size(data, pos);
            // SAFETY: `Labels` guarantees valid `stringlabels` UTF-8.
            let name = unsafe { std::str::from_utf8_unchecked(&data[nstart..nstart + nlen]) };
            let (vlen, vstart) = decode_stringlabel_size(data, nstart + nlen);
            let new_pos = vstart + vlen;
            while j < names.len() && names[j] < name {
                j += 1;
            }
            if j == names.len() {
                break;
            }
            if name == names[j] {
                buf.extend_from_slice(&data[pos..new_pos]);
            }
            pos = new_pos;
        }
    }

    /// Writes raw `stringlabels` bytes for labels NOT matching `names` (must be sorted ascending).
    pub fn bytes_without_labels(&self, buf: &mut Vec<u8>, names: &[&str]) {
        buf.clear();
        let data = &self.data;
        let mut j = 0usize;
        let mut pos = 0usize;
        while pos < data.len() {
            let (nlen, nstart) = decode_stringlabel_size(data, pos);
            // SAFETY: `Labels` guarantees valid `stringlabels` UTF-8.
            let name = unsafe { std::str::from_utf8_unchecked(&data[nstart..nstart + nlen]) };
            let (vlen, vstart) = decode_stringlabel_size(data, nstart + nlen);
            let new_pos = vstart + vlen;
            while j < names.len() && names[j] < name {
                j += 1;
            }
            if j == names.len() || name != names[j] {
                buf.extend_from_slice(&data[pos..new_pos]);
            }
            pos = new_pos;
        }
    }

    /// Computes hash for labels matching `names` (must be sorted ascending), reusing `buf`.
    pub fn hash_for_labels(&self, buf: &mut Vec<u8>, names: &[&str]) -> u64 {
        buf.clear();
        let mut j = 0usize;
        for (name, value) in self.iter() {
            while j < names.len() && names[j] < name {
                j += 1;
            }
            if j == names.len() {
                break;
            }
            if name == names[j] {
                buf.extend_from_slice(name.as_bytes());
                buf.push(LABEL_SEP);
                buf.extend_from_slice(value.as_bytes());
                buf.push(LABEL_SEP);
            }
        }
        xxh64(buf, 0)
    }

    /// Computes hash for all labels except `__name__` and `names` (must be sorted ascending), reusing `buf`.
    pub fn hash_without_labels(&self, buf: &mut Vec<u8>, names: &[&str]) -> u64 {
        buf.clear();
        let mut j = 0usize;
        for (name, value) in self.iter() {
            while j < names.len() && names[j] < name {
                j += 1;
            }
            if name == METRIC_NAME || (j < names.len() && name == names[j]) {
                continue;
            }
            buf.extend_from_slice(name.as_bytes());
            buf.push(LABEL_SEP);
            buf.extend_from_slice(value.as_bytes());
            buf.push(LABEL_SEP);
        }
        xxh64(buf, 0)
    }

    /// Returns a subset of `Labels` matching or excluding `names` (`Labels.MatchLabels`).
    pub fn match_labels(&self, on: bool, names: &[&str]) -> Self {
        let mut b = LabelsBuilder::new(self.clone());
        if on {
            b.keep(names);
        } else {
            b.del(&[METRIC_NAME]);
            b.del(names);
        }
        b.labels()
    }

    /// Returns `Labels` with `__name__` removed (`Labels.DropMetricName`).
    pub fn drop_metric_name(&self) -> Self {
        self.drop_reserved(|n| n == METRIC_NAME)
    }

    /// Returns `Labels` without reserved labels matching `should_drop` (`Labels.DropReserved`).
    pub fn drop_reserved(&self, mut should_drop: impl FnMut(&str) -> bool) -> Self {
        let mut data = self.data.as_ref();
        let mut owned: Option<Vec<u8>> = None;
        let mut i = 0usize;
        loop {
            let slice = owned.as_deref().unwrap_or(data);
            if i >= slice.len() {
                break;
            }
            let (nlen, nstart) = decode_stringlabel_size(slice, i);
            // SAFETY: `Labels` guarantees valid `stringlabels` UTF-8.
            let name = unsafe { std::str::from_utf8_unchecked(&slice[nstart..nstart + nlen]) };
            let (vlen, vstart) = decode_stringlabel_size(slice, nstart + nlen);
            let i2 = vstart + vlen;
            if name.as_bytes().first().copied().is_some_and(|b| b > b'_') {
                break;
            }
            if should_drop(name) {
                if i == 0 && owned.is_none() {
                    data = &data[i2..];
                } else {
                    let vec = owned.get_or_insert_with(|| data.to_vec());
                    vec.drain(i..i2);
                }
                continue;
            }
            i = i2;
        }
        match owned {
            Some(v) => Self {
                data: Bytes::from(v),
            },
            None if data.len() == self.data.len() => self.clone(),
            None => Self {
                data: self.data.slice(self.data.len() - data.len()..),
            },
        }
    }

    /// Calls `f` for each `(name, value)` pair (`Labels.Range`).
    #[inline]
    pub fn range(&self, mut f: impl FnMut(&str, &str)) {
        for (k, v) in self.iter() {
            f(k, v);
        }
    }

    /// Calls `f` on each label, stopping on the first error (`Labels.Validate`).
    #[inline]
    pub fn validate<E>(&self, mut f: impl FnMut(&str, &str) -> Result<(), E>) -> Result<(), E> {
        for (k, v) in self.iter() {
            f(k, v)?;
        }
        Ok(())
    }

    /// Checks whether all label names (and metric name if present) are valid under `scheme` (`Labels.IsValid`).
    pub fn is_valid(&self, scheme: ValidationScheme) -> bool {
        for (k, v) in self.iter() {
            if k == METRIC_NAME && !scheme.is_valid_metric_name(v) {
                return false;
            }
            if !scheme.is_valid_label_name(k) {
                return false;
            }
        }
        true
    }

    /// Returns the string representation without spaces after commas (`Labels.StringNoSpace`).
    pub fn string_no_space(&self) -> String {
        LabelsNoSpaceDisplay(self).to_string()
    }

    /// Compares `self` and `other` assuming both share an identical pair-aligned byte prefix
    /// `0..pair_offset` (so all label pairs before `pair_offset` are identical).
    #[inline]
    pub fn cmp_from_pair_offset(&self, other: &Self, pair_offset: usize) -> Ordering {
        let a = &self.data;
        let b = &other.data;
        let mut pa = pair_offset;
        let mut pb = pair_offset;
        while pa < a.len() && pb < b.len() {
            let ba = a[pa];
            let bb = b[pb];
            let (an_len, an_start) = if ba < 128 {
                (ba as usize, pa + 1)
            } else {
                decode_stringlabel_size(a, pa)
            };
            let (bn_len, bn_start) = if bb < 128 {
                (bb as usize, pb + 1)
            } else {
                decode_stringlabel_size(b, pb)
            };
            let an_end = an_start + an_len;
            let bn_end = bn_start + bn_len;
            match a[an_start..an_end].cmp(&b[bn_start..bn_end]) {
                Ordering::Equal => {}
                ord => return ord,
            }
            let vba = a[an_end];
            let vbb = b[bn_end];
            let (av_len, av_start) = if vba < 128 {
                (vba as usize, an_end + 1)
            } else {
                decode_stringlabel_size(a, an_end)
            };
            let (bv_len, bv_start) = if vbb < 128 {
                (vbb as usize, bn_end + 1)
            } else {
                decode_stringlabel_size(b, bn_end)
            };
            let av_end = av_start + av_len;
            let bv_end = bv_start + bv_len;
            match a[av_start..av_end].cmp(&b[bv_start..bv_end]) {
                Ordering::Equal => {}
                ord => return ord,
            }
            pa = av_end;
            pb = bv_end;
        }
        (pa < a.len()).cmp(&(pb < b.len()))
    }
}

impl Ord for Labels {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut a = self.iter();
        let mut b = other.iter();
        loop {
            match (a.next(), b.next()) {
                (None, None) => return Ordering::Equal,
                (None, Some(_)) => return Ordering::Less,
                (Some(_), None) => return Ordering::Greater,
                (Some((an, av)), Some((bn, bv))) => {
                    match an.cmp(bn).then_with(|| av.cmp(bv)) {
                        Ordering::Equal => continue,
                        ord => return ord,
                    }
                }
            }
        }
    }
}

impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Labels({self})")
    }
}

fn fmt_labels_impl(ls: &Labels, f: &mut fmt::Formatter<'_>, add_space: bool) -> fmt::Result {
    f.write_str("{")?;
    for (i, (k, v)) in ls.iter().enumerate() {
        if i > 0 {
            f.write_str(if add_space { ", " } else { "," })?;
        }
        if !ValidationScheme::Legacy.is_valid_label_name(k) {
            write_go_quote(f, k)?;
        } else {
            f.write_str(k)?;
        }
        f.write_str("=")?;
        write_go_quote(f, v)?;
    }
    f.write_str("}")
}

struct LabelsNoSpaceDisplay<'a>(&'a Labels);

impl fmt::Display for LabelsNoSpaceDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_labels_impl(self.0, f, false)
    }
}

impl fmt::Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_labels_impl(self, f, true)
    }
}

impl Serialize for Labels {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (k, v) in self.iter() {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Labels {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LabelsVisitor;
        impl<'de> Visitor<'de> for LabelsVisitor {
            type Value = Labels;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a label map")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut access: M) -> Result<Labels, M::Error> {
                let mut pairs: Vec<(String, String)> = Vec::new();
                while let Some((k, v)) = access.next_entry()? {
                    pairs.push((k, v));
                }
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(Labels::from_sorted_pairs(
                    pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())),
                ))
            }
        }
        deserializer.deserialize_map(LabelsVisitor)
    }
}

/// Iterator over `(name, value)` pairs inside [`Labels`].
#[derive(Clone)]
pub struct LabelsIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for LabelsIter<'a> {
    type Item = (&'a str, &'a str);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos;
        let data = self.data;
        if pos >= data.len() {
            return None;
        }
        let b0 = data[pos];
        if b0 < 128 {
            let nstart = pos + 1;
            let nend = nstart + b0 as usize;
            let b1 = data[nend];
            if b1 < 128 {
                let vstart = nend + 1;
                let vend = vstart + b1 as usize;
                self.pos = vend;
                // SAFETY: `Labels` only ever encodes valid UTF-8 strings.
                let name = unsafe { std::str::from_utf8_unchecked(&data[nstart..nend]) };
                let value = unsafe { std::str::from_utf8_unchecked(&data[vstart..vend]) };
                return Some((name, value));
            }
        }
        // SAFETY: `LabelsIter` is only constructed via `iter_stringlabels_unchecked` on validated `Labels`.
        let (name, p1) = unsafe { decode_stringlabel_str(data, pos) };
        let (value, p2) = unsafe { decode_stringlabel_str(data, p1) };
        self.pos = p2;
        Some((name, value))
    }
}

/// Safe iterator over `(name, value)` pairs inside a raw Prometheus `stringlabels` byte slice.
///
/// Returns `None` when the end of the buffer is reached or if truncated framing, length overflow,
/// or invalid UTF-8 is encountered.
#[derive(Clone)]
pub struct StringLabelsIter<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for StringLabelsIter<'a> {
    type Item = (&'a str, &'a str);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let data = self.data;
        let pos = self.pos;
        let b0 = *data.get(pos)?;
        let (nlen, nstart) = if b0 < 255 {
            (b0 as usize, pos + 1)
        } else {
            try_decode_stringlabel_size(data, pos).ok()?
        };
        let nend = nstart.checked_add(nlen)?;
        let name_bytes = data.get(nstart..nend)?;
        let name = std::str::from_utf8(name_bytes).ok()?;

        let b1 = *data.get(nend)?;
        let (vlen, vstart) = if b1 < 255 {
            (b1 as usize, nend + 1)
        } else {
            try_decode_stringlabel_size(data, nend).ok()?
        };
        let vend = vstart.checked_add(vlen)?;
        let val_bytes = data.get(vstart..vend)?;
        let value = std::str::from_utf8(val_bytes).ok()?;

        self.pos = vend;
        Some((name, value))
    }
}

struct LabelsBytesIter<'a> {
    data: &'a Bytes,
    pos: usize,
}

impl Iterator for LabelsBytesIter<'_> {
    type Item = (Bytes, Bytes);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        let (nlen, nstart) = decode_stringlabel_size(self.data, self.pos);
        let nend = nstart + nlen;
        let (vlen, vstart) = decode_stringlabel_size(self.data, nend);
        let vend = vstart + vlen;
        self.pos = vend;
        Some((self.data.slice(nstart..nend), self.data.slice(vstart..vend)))
    }
}

/// Compact interned label set stored as sorted `(SymbolId, SymbolId)` pairs.
///
/// Pairs are sorted lexicographically by label **name string** (not by `SymbolId`),
/// preserving exact Prometheus label ordering and hashing semantics.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct SymbolLabels {
    pairs: SmallVec<[(SymbolId, SymbolId); 8]>,
}

impl SymbolLabels {
    #[inline]
    pub fn empty() -> Self {
        Self {
            pairs: SmallVec::new(),
        }
    }

    /// Interns `(name, value)` pairs into `symbols`, sorting by label name.
    pub fn from_pairs<'a>(
        symbols: &mut SymbolTable,
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        let mut raw: SmallVec<[(&str, &str); 16]> = pairs.into_iter().collect();
        raw.sort_by(|a, b| a.0.cmp(b.0));
        let mut interned = SmallVec::with_capacity(raw.len());
        for (name, value) in raw {
            interned.push((symbols.intern(name), symbols.intern(value)));
        }
        Self { pairs: interned }
    }

    /// Interns `(name, value)` pairs, dropping any pair with an empty value (Prometheus convention).
    pub fn from_pairs_non_empty<'a>(
        symbols: &mut SymbolTable,
        pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        let mut raw: SmallVec<[(&str, &str); 16]> =
            pairs.into_iter().filter(|(_, v)| !v.is_empty()).collect();
        raw.sort_by(|a, b| a.0.cmp(b.0));
        let mut interned = SmallVec::with_capacity(raw.len());
        for (name, value) in raw {
            interned.push((symbols.intern(name), symbols.intern(value)));
        }
        Self { pairs: interned }
    }

    /// Interns an existing [`Labels`] set into `symbols`.
    pub fn from_labels(symbols: &mut SymbolTable, labels: &Labels) -> Self {
        let mut pairs = SmallVec::with_capacity(labels.len());
        for (k, v) in labels.iter() {
            pairs.push((symbols.intern(k), symbols.intern(v)));
        }
        Self { pairs }
    }

    /// Constructs from pre-sorted `(SymbolId, SymbolId)` pairs (must already be sorted by name string).
    #[inline]
    pub fn from_sorted_ids(pairs: SmallVec<[(SymbolId, SymbolId); 8]>) -> Self {
        Self { pairs }
    }

    /// Materializes a [`Labels`] instance from the symbol table.
    pub fn to_labels(&self, symbols: &SymbolTable) -> Labels {
        Labels::from_sorted_pairs(
            self.pairs
                .iter()
                .map(|&(k, v)| (symbols.resolve(k), symbols.resolve(v))),
        )
    }

    /// Materializes owned `Vec<Label>` pairs.
    pub fn to_vec(&self, symbols: &SymbolTable) -> Vec<Label> {
        self.pairs
            .iter()
            .map(|&(k, v)| Label::new(symbols.resolve(k), symbols.resolve(v)))
            .collect()
    }

    #[inline]
    pub fn as_slice(&self) -> &[(SymbolId, SymbolId)] {
        &self.pairs
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Resolves the value for label `name`, or `""` if absent.
    pub fn get<'a>(&self, symbols: &'a SymbolTable, name: &str) -> &'a str {
        if let Some(name_id) = symbols.get(name) {
            for &(k, v) in &self.pairs {
                if k == name_id {
                    return symbols.resolve(v);
                }
            }
            return "";
        }
        ""
    }

    /// Returns `true` if label `name` is present.
    pub fn has(&self, symbols: &SymbolTable, name: &str) -> bool {
        if let Some(name_id) = symbols.get(name) {
            return self.pairs.iter().any(|&(k, _)| k == name_id);
        }
        false
    }

    /// Iterates over resolved `(&str, &str)` pairs.
    #[inline]
    pub fn iter<'a>(
        &'a self,
        symbols: &'a SymbolTable,
    ) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        self.pairs
            .iter()
            .map(|&(k, v)| (symbols.resolve(k), symbols.resolve(v)))
    }

    /// Computes the exact Prometheus `stringlabels` `Labels.Hash()` (`xxhash64`).
    pub fn prom_hash(&self, symbols: &SymbolTable) -> u64 {
        prom_labels_hash_sorted(self.iter(symbols))
    }

    /// Evaluates a single [`LabelMatcher`] against this series (missing label == `""`).
    #[inline]
    pub fn matches_one(&self, symbols: &SymbolTable, matcher: &LabelMatcher) -> bool {
        matcher.matches(self.get(symbols, &matcher.name))
    }

    /// Evaluates all `matchers` against this series (missing label == `""`).
    #[inline]
    pub fn matches(&self, symbols: &SymbolTable, matchers: &[LabelMatcher]) -> bool {
        matchers.iter().all(|m| self.matches_one(symbols, m))
    }
}

#[inline]
fn stringlabel_size_when_encoded(len: usize) -> usize {
    if len < 255 {
        1
    } else {
        assert!(len < (1 << 24), "string too long to encode as label");
        4
    }
}

/// Approximate encoded space required for `n` copies of `(name, value)` (`labels.SizeOfLabels`).
#[inline]
pub fn size_of_labels(name: &str, value: &str, n: u64) -> u64 {
    let per = (name.len() + stringlabel_size_when_encoded(name.len())
        + value.len() + stringlabel_size_when_encoded(value.len())) as u64;
    per * n
}

/// Builder allowing incremental modification of a `Labels` set (`labels.Builder`).
#[derive(Clone, Debug, Default)]
pub struct LabelsBuilder {
    base: Labels,
    del: Vec<String>,
    add: Vec<Label>,
}

impl LabelsBuilder {
    pub fn new(base: Labels) -> Self {
        let mut b = Self {
            base: Labels::empty(),
            del: Vec::with_capacity(5),
            add: Vec::with_capacity(5),
        };
        b.reset(base);
        b
    }

    pub fn reset(&mut self, base: Labels) {
        self.base = base;
        self.del.clear();
        self.add.clear();
        for (k, v) in self.base.iter() {
            if v.is_empty() {
                self.del.push(k.to_string());
            }
        }
    }

    pub fn del(&mut self, names: &[&str]) -> &mut Self {
        for &n in names {
            self.add.retain(|a| a.name != n);
            self.del.push(n.to_string());
        }
        self
    }

    pub fn keep(&mut self, names: &[&str]) -> &mut Self {
        for (k, _) in self.base.iter() {
            if !names.contains(&k) {
                self.del.push(k.to_string());
            }
        }
        self
    }

    pub fn set(&mut self, name: &str, value: &str) -> &mut Self {
        if value.is_empty() {
            return self.del(&[name]);
        }
        for a in &mut self.add {
            if a.name == name {
                a.value.clear();
                a.value.push_str(value);
                return self;
            }
        }
        self.add.push(Label::new(name, value));
        self
    }

    pub fn get(&self, name: &str) -> &str {
        for a in &self.add {
            if a.name == name {
                return &a.value;
            }
        }
        if self.del.iter().any(|d| d == name) {
            return "";
        }
        self.base.get(name)
    }

    pub fn range(&mut self, mut f: impl FnMut(&str, &str)) {
        let orig_add = self.add.clone();
        let orig_del = self.del.clone();
        for (k, v) in self.base.iter() {
            if !orig_del.iter().any(|d| d == k) && !orig_add.iter().any(|a| a.name == k) {
                f(k, v);
            }
        }
        for a in &orig_add {
            f(&a.name, &a.value);
        }
    }

    pub fn labels(&mut self) -> Labels {
        if self.del.is_empty() && self.add.is_empty() {
            return self.base.clone();
        }
        self.add.sort_by(|a, b| a.name.cmp(&b.name));
        self.del.sort();
        let mut a_idx = 0usize;
        let mut d_idx = 0usize;
        let mut buf = Vec::with_capacity(self.base.data.len() + self.add.len() * 16);
        let data = self.base.as_bytes();
        let mut pos = 0usize;
        while pos < data.len() {
            let old_pos = pos;
            let (nlen, nstart) = decode_stringlabel_size(data, pos);
            // SAFETY: `Labels` guarantees valid `stringlabels` UTF-8.
            let l_name = unsafe { std::str::from_utf8_unchecked(&data[nstart..nstart + nlen]) };
            let (vlen, vstart) = decode_stringlabel_size(data, nstart + nlen);
            pos = vstart + vlen;
            while d_idx < self.del.len() && self.del[d_idx].as_str() < l_name {
                d_idx += 1;
            }
            if d_idx < self.del.len() && self.del[d_idx].as_str() == l_name {
                continue;
            }
            while a_idx < self.add.len() && self.add[a_idx].name.as_str() < l_name {
                append_stringlabel_pair(
                    &mut buf,
                    self.add[a_idx].name.as_bytes(),
                    self.add[a_idx].value.as_bytes(),
                );
                a_idx += 1;
            }
            if a_idx < self.add.len() && self.add[a_idx].name.as_str() == l_name {
                append_stringlabel_pair(
                    &mut buf,
                    self.add[a_idx].name.as_bytes(),
                    self.add[a_idx].value.as_bytes(),
                );
                a_idx += 1;
                continue;
            }
            buf.extend_from_slice(&data[old_pos..pos]);
        }
        while a_idx < self.add.len() {
            append_stringlabel_pair(
                &mut buf,
                self.add[a_idx].name.as_bytes(),
                self.add[a_idx].value.as_bytes(),
            );
            a_idx += 1;
        }
        Labels {
            data: Bytes::from(buf),
        }
    }
}

/// Efficient scratch builder for constructing `Labels` from scratch (`labels.ScratchBuilder`).
#[derive(Clone, Debug, Default)]
pub struct ScratchBuilder {
    add: Vec<Label>,
    output: Labels,
    overwrite_buf: Vec<u8>,
}

impl ScratchBuilder {
    pub fn new(cap: usize) -> Self {
        Self {
            add: Vec::with_capacity(cap),
            output: Labels::empty(),
            overwrite_buf: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.add.clear();
        self.output = Labels::empty();
    }

    pub fn add(&mut self, name: &str, value: &str) {
        self.add.push(Label::new(name, value));
    }

    pub fn sort(&mut self) {
        self.add.sort_by(|a, b| a.name.cmp(&b.name));
    }

    pub fn assign(&mut self, l: Labels) {
        self.output = l;
    }

    pub fn set_unsafe_add(&mut self, _unsafe: bool) {}

    pub fn labels(&mut self) -> Labels {
        if self.output.is_empty() && !self.add.is_empty() {
            let cap: usize = self
                .add
                .iter()
                .map(|l| {
                    l.name.len()
                        + stringlabel_size_when_encoded(l.name.len())
                        + l.value.len()
                        + stringlabel_size_when_encoded(l.value.len())
                })
                .sum();
            let mut buf = Vec::with_capacity(cap);
            for l in &self.add {
                append_stringlabel_pair(&mut buf, l.name.as_bytes(), l.value.as_bytes());
            }
            self.output = Labels {
                data: Bytes::from(buf),
            };
        }
        self.output.clone()
    }

    pub fn overwrite(&mut self, ls: &mut Labels) {
        self.overwrite_buf.clear();
        for l in &self.add {
            append_stringlabel_pair(&mut self.overwrite_buf, l.name.as_bytes(), l.value.as_bytes());
        }
        *ls = Labels {
            data: Bytes::copy_from_slice(&self.overwrite_buf),
        };
    }
}

/// Formats `f` like Go `FormatOpenMetricsFloat`, appending `.0` if needed.
pub fn format_openmetrics_float(f: f64) -> String {
    if f == 1.0 {
        return "1.0".to_string();
    }
    if f == 0.0 {
        return "0.0".to_string();
    }
    if f == -1.0 {
        return "-1.0".to_string();
    }
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() { "+Inf" } else { "-Inf" }.to_string();
    }
    let mut buf = Vec::with_capacity(24);
    append_openmetrics_float(&mut buf, f);
    String::from_utf8(buf).expect("ASCII float formatting")
}

/// Appends the OpenMetrics float representation of `f` to `dst` (`labels.AppendOpenMetricsFloat`).
pub fn append_openmetrics_float(dst: &mut Vec<u8>, f: f64) {
    if f == 1.0 {
        dst.extend_from_slice(b"1.0");
        return;
    }
    if f == 0.0 {
        dst.extend_from_slice(b"0.0");
        return;
    }
    if f == -1.0 {
        dst.extend_from_slice(b"-1.0");
        return;
    }
    if f.is_nan() {
        dst.extend_from_slice(b"NaN");
        return;
    }
    if f.is_infinite() {
        dst.extend_from_slice(if f.is_sign_positive() { b"+Inf" } else { b"-Inf" });
        return;
    }
    let start = dst.len();
    let formatted = crate::chunkenc::histogram_model::format_go_g(f);
    dst.extend_from_slice(formatted.as_bytes());
    if !dst[start..].iter().any(|&b| b == b'.' || b == b'e') {
        dst.extend_from_slice(b".0");
    }
}

/// Ported from github.com/prometheus/prometheus/model/labels/regexp.go
/// and github.com/grafana/regexp/syntax.
pub mod prom_regexp {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::OnceLock;

    use parking_lot::Mutex;

    use hashbrown::HashSet;
    use regex::{Regex, RegexBuilder};

    use crate::go_regexp_syntax;
    use crate::matcher::MatcherError;

    pub const MAX_SET_MATCHES: usize = 256;
    pub const MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD: usize = 16;
    pub const FAST_REGEX_MATCHER_CACHE_MAX_SIZE_BYTES: usize = 1024 * 1024 * 1024;

    pub const FLAG_FOLD_CASE: u16 = 1 << 0;
    pub const FLAG_NON_GREEDY: u16 = 1 << 1;
    pub const FLAG_DOT_NL: u16 = 1 << 2;
    pub const FLAG_MULTILINE: u16 = 1 << 3;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum SyntaxOp {
        EmptyMatch,
        Literal,
        CharClass,
        AnyCharNotNL,
        AnyChar,
        BeginLine,
        EndLine,
        BeginText,
        EndText,
        WordBoundary,
        NoWordBoundary,
        Capture,
        Star,
        Plus,
        Quest,
        Repeat,
        Concat,
        Alternate,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SyntaxRegexp {
        pub op: SyntaxOp,
        pub flags: u16,
        pub sub: Vec<SyntaxRegexp>,
        pub runes: Vec<char>,
        pub min: i32,
        pub max: i32,
        pub cap: usize,
        pub negated: bool,
    }

    impl SyntaxRegexp {
        fn new(op: SyntaxOp, flags: u16) -> Self {
            Self {
                op,
                flags,
                sub: Vec::new(),
                runes: Vec::new(),
                min: 0,
                max: 0,
                cap: 0,
                negated: false,
            }
        }

        #[inline]
        pub fn is_case_insensitive(&self) -> bool {
            (self.flags & FLAG_FOLD_CASE) != 0
        }

        #[inline]
        pub fn is_case_sensitive(&self) -> bool {
            !self.is_case_insensitive()
        }

        pub fn to_pattern_string(&self) -> String {
            let mut out = String::new();
            self.write_pattern(&mut out);
            out
        }

        fn write_pattern(&self, out: &mut String) {
            match self.op {
                SyntaxOp::EmptyMatch => out.push_str("(?:)"),
                SyntaxOp::Literal => {
                    if self.is_case_insensitive() {
                        out.push_str("(?i:");
                    }
                    for &c in &self.runes {
                        push_escaped_char(out, c);
                    }
                    if self.is_case_insensitive() {
                        out.push(')');
                    }
                }
                SyntaxOp::CharClass => {
                    if self.is_case_insensitive() {
                        out.push_str("(?i:");
                    }
                    out.push('[');
                    if self.negated {
                        out.push('^');
                    }
                    for pair in self.runes.as_chunks::<2>().0 {
                        let (lo, hi) = (pair[0], pair[1]);
                        push_class_char(out, lo);
                        if lo != hi {
                            out.push('-');
                            push_class_char(out, hi);
                        }
                    }
                    out.push(']');
                    if self.is_case_insensitive() {
                        out.push(')');
                    }
                }
                SyntaxOp::AnyCharNotNL => out.push_str("(?-s:.)"),
                SyntaxOp::AnyChar => out.push_str("(?s:.)"),
                SyntaxOp::BeginLine => out.push_str("(?m:^)"),
                SyntaxOp::EndLine => out.push_str("(?m:$)"),
                SyntaxOp::BeginText => out.push_str(r"\A"),
                SyntaxOp::EndText => out.push_str(r"\z"),
                SyntaxOp::WordBoundary => out.push_str(r"\b"),
                SyntaxOp::NoWordBoundary => out.push_str(r"\B"),
                SyntaxOp::Capture => {
                    out.push('(');
                    for s in &self.sub {
                        s.write_pattern(out);
                    }
                    out.push(')');
                }
                SyntaxOp::Star | SyntaxOp::Plus | SyntaxOp::Quest | SyntaxOp::Repeat => {
                    if let Some(sub) = self.sub.first() {
                        let need_paren = matches!(
                            sub.op,
                            SyntaxOp::Concat | SyntaxOp::Alternate | SyntaxOp::Star | SyntaxOp::Plus | SyntaxOp::Quest | SyntaxOp::Repeat
                        ) || (sub.op == SyntaxOp::Literal && sub.runes.len() > 1);
                        if need_paren {
                            out.push_str("(?:");
                            sub.write_pattern(out);
                            out.push(')');
                        } else {
                            sub.write_pattern(out);
                        }
                    }
                    match self.op {
                        SyntaxOp::Star => out.push('*'),
                        SyntaxOp::Plus => out.push('+'),
                        SyntaxOp::Quest => out.push('?'),
                        SyntaxOp::Repeat => {
                            if self.max < 0 {
                                out.push_str(&format!("{{{},}}", self.min));
                            } else if self.min == self.max {
                                out.push_str(&format!("{{{}}}", self.min));
                            } else {
                                out.push_str(&format!("{{{},{}}}", self.min, self.max));
                            }
                        }
                        _ => {}
                    }
                    if (self.flags & FLAG_NON_GREEDY) != 0 {
                        out.push('?');
                    }
                }
                SyntaxOp::Concat => {
                    for s in &self.sub {
                        if s.op == SyntaxOp::Alternate {
                            out.push_str("(?:");
                            s.write_pattern(out);
                            out.push(')');
                        } else {
                            s.write_pattern(out);
                        }
                    }
                }
                SyntaxOp::Alternate => {
                    for (i, s) in self.sub.iter().enumerate() {
                        if i > 0 {
                            out.push('|');
                        }
                        s.write_pattern(out);
                    }
                }
            }
        }
    }

    fn push_escaped_char(out: &mut String, c: char) {
        if matches!(
            c,
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$'
        ) {
            out.push('\\');
        }
        out.push(c);
    }

    fn push_class_char(out: &mut String, c: char) {
        if matches!(c, '\\' | ']' | '-' | '^') {
            out.push('\\');
        }
        out.push(c);
    }

    #[inline]
    pub fn simple_fold_next(r: char) -> char {
        match r {
            'k' => '\u{212a}',
            'K' => 'k',
            '\u{212a}' => 'K',
            's' => '\u{017f}',
            'S' => 's',
            '\u{017f}' => 'S',
            'a'..='z' => ((r as u8) - b'a' + b'A') as char,
            'A'..='Z' => ((r as u8) - b'A' + b'a') as char,
            _ => r,
        }
    }

    #[inline]
    fn min_fold_rune(r: char) -> char {
        let mut min_r = r;
        let mut cur = simple_fold_next(r);
        while cur != r {
            if cur < min_r {
                min_r = cur;
            }
            cur = simple_fold_next(cur);
        }
        min_r
    }

    fn simple_fold_all(r: char) -> Vec<char> {
        let mut out = vec![r];
        let mut cur = simple_fold_next(r);
        while cur != r {
            out.push(cur);
            cur = simple_fold_next(cur);
        }
        out.sort();
        out.dedup();
        out
    }

    #[derive(Clone, Default)]
    struct ParserFrame {
        flags: u16,
        is_capturing: bool,
        cap_idx: usize,
        alts: Vec<Vec<SyntaxRegexp>>,
        cur: Vec<SyntaxRegexp>,
    }

    impl ParserFrame {
        fn push_node(&mut self, node: SyntaxRegexp) {
            if node.op == SyntaxOp::Literal && !node.runes.is_empty() {
                if let Some(last) = self.cur.last_mut() {
                    if last.op == SyntaxOp::Literal
                        && (last.flags & FLAG_FOLD_CASE) == (node.flags & FLAG_FOLD_CASE)
                    {
                        last.runes.extend_from_slice(&node.runes);
                        return;
                    }
                }
            }
            self.cur.push(node);
        }

        fn push_alt(&mut self) {
            let cur = std::mem::take(&mut self.cur);
            self.alts.push(cur);
        }
    }

    fn collapse_concat(mut items: Vec<SyntaxRegexp>, flags: u16) -> SyntaxRegexp {
        items.retain(|r| r.op != SyntaxOp::EmptyMatch);
        let mut merged: Vec<SyntaxRegexp> = Vec::with_capacity(items.len());
        for item in items {
            if item.op == SyntaxOp::Concat {
                for sub in item.sub {
                    if sub.op == SyntaxOp::Literal {
                        if let Some(last) = merged.last_mut() {
                            if last.op == SyntaxOp::Literal
                                && (last.flags & FLAG_FOLD_CASE) == (sub.flags & FLAG_FOLD_CASE)
                            {
                                last.runes.extend_from_slice(&sub.runes);
                                continue;
                            }
                        }
                    }
                    if sub.op != SyntaxOp::EmptyMatch {
                        merged.push(sub);
                    }
                }
            } else if item.op == SyntaxOp::Literal {
                if let Some(last) = merged.last_mut() {
                    if last.op == SyntaxOp::Literal
                        && (last.flags & FLAG_FOLD_CASE) == (item.flags & FLAG_FOLD_CASE)
                    {
                        last.runes.extend_from_slice(&item.runes);
                        continue;
                    }
                }
                merged.push(item);
            } else {
                merged.push(item);
            }
        }
        match merged.len() {
            0 => SyntaxRegexp::new(SyntaxOp::EmptyMatch, flags),
            1 => merged.into_iter().next().unwrap(),
            _ => {
                let mut re = SyntaxRegexp::new(SyntaxOp::Concat, flags);
                re.sub = merged;
                re
            }
        }
    }

    fn leading_string(re: &SyntaxRegexp) -> (Option<&[char]>, u16) {
        let mut r = re;
        if r.op == SyntaxOp::Concat && !r.sub.is_empty() {
            r = &r.sub[0];
        }
        if r.op == SyntaxOp::Literal && !r.runes.is_empty() {
            (Some(&r.runes), r.flags & FLAG_FOLD_CASE)
        } else {
            (None, 0)
        }
    }

    fn remove_leading_string(re: SyntaxRegexp, n: usize) -> SyntaxRegexp {
        if re.op == SyntaxOp::Concat && !re.sub.is_empty() {
            let mut sub = re.sub;
            let first = remove_leading_string(sub.remove(0), n);
            if first.op != SyntaxOp::EmptyMatch {
                sub.insert(0, first);
            }
            return collapse_concat(sub, re.flags);
        }
        if re.op == SyntaxOp::Literal {
            if n >= re.runes.len() {
                return SyntaxRegexp::new(SyntaxOp::EmptyMatch, re.flags);
            }
            let mut out = re;
            out.runes.drain(..n);
            return out;
        }
        re
    }

    fn leading_regexp(re: &SyntaxRegexp) -> Option<&SyntaxRegexp> {
        if re.op == SyntaxOp::EmptyMatch {
            return None;
        }
        if re.op == SyntaxOp::Concat && !re.sub.is_empty() {
            let first = &re.sub[0];
            if first.op == SyntaxOp::EmptyMatch {
                return None;
            }
            return Some(first);
        }
        Some(re)
    }

    fn remove_leading_regexp(re: SyntaxRegexp) -> SyntaxRegexp {
        if re.op == SyntaxOp::Concat && !re.sub.is_empty() {
            let mut sub = re.sub;
            sub.remove(0);
            return collapse_concat(sub, re.flags);
        }
        SyntaxRegexp::new(SyntaxOp::EmptyMatch, re.flags)
    }

    fn factor_alternates(sub_in: Vec<SyntaxRegexp>, flags: u16) -> SyntaxRegexp {
        let mut sub: Vec<SyntaxRegexp> = Vec::with_capacity(sub_in.len());
        for s in sub_in {
            if s.op == SyntaxOp::Alternate {
                sub.extend(s.sub);
            } else {
                sub.push(s);
            }
        }
        if sub.len() < 2 {
            return sub
                .into_iter()
                .next()
                .unwrap_or_else(|| SyntaxRegexp::new(SyntaxOp::EmptyMatch, flags));
        }

        if sub.len() > 2 {
            let (s0, f0) = leading_string(&sub[0]);
            let ok_to_sort = s0.is_some()
                && sub.iter().all(|item| {
                    let (si, fi) = leading_string(item);
                    si.is_some() && fi == f0
                });
            if ok_to_sort {
                sub.sort_by(|a, b| {
                    let (Some(la), _) = leading_string(a) else {
                        return std::cmp::Ordering::Equal;
                    };
                    let (Some(lb), _) = leading_string(b) else {
                        return std::cmp::Ordering::Equal;
                    };
                    let pref = la.len().min(lb.len());
                    la[..pref].cmp(&lb[..pref])
                });
            }
        }

        // Round 1: factor out common literal prefixes.
        let mut out: Vec<SyntaxRegexp> = Vec::with_capacity(sub.len());
        let mut start = 0usize;
        let mut prefix_runes: Vec<char> = Vec::new();
        let mut prefix_fold = 0u16;
        for i in 0..=sub.len() {
            let mut continued = false;
            if i < sub.len() {
                if let (Some(istr), ifold) = leading_string(&sub[i]) {
                    if i == start {
                        prefix_runes = istr.to_vec();
                        prefix_fold = ifold;
                        continued = true;
                    } else if ifold == prefix_fold {
                        let same = prefix_runes
                            .iter()
                            .zip(istr.iter())
                            .take_while(|(a, b)| a == b)
                            .count();
                        if same > 0 {
                            prefix_runes.truncate(same);
                            continued = true;
                        }
                    }
                }
            }
            if continued {
                continue;
            }
            if i == start + 1 {
                out.push(sub[start].clone());
            } else if i > start + 1 {
                let mut pref = SyntaxRegexp::new(SyntaxOp::Literal, prefix_fold);
                pref.runes = prefix_runes.clone();
                let rem: Vec<SyntaxRegexp> = sub[start..i]
                    .iter()
                    .cloned()
                    .map(|s| remove_leading_string(s, prefix_runes.len()))
                    .collect();
                let suffix = factor_alternates(rem, flags);
                out.push(collapse_concat(vec![pref, suffix], flags));
            }
            if i < sub.len() {
                start = i;
                if let (Some(istr), ifold) = leading_string(&sub[i]) {
                    prefix_runes = istr.to_vec();
                    prefix_fold = ifold;
                } else {
                    prefix_runes.clear();
                    prefix_fold = 0;
                    out.push(sub[i].clone());
                    start = i + 1;
                }
            }
        }
        sub = out;

        // Round 2: factor common leading charclass / AnyChar (or fixed repeat thereof).
        let is_char_class = |r: &SyntaxRegexp| -> bool {
            (r.op == SyntaxOp::Literal && r.runes.len() == 1)
                || (r.op == SyntaxOp::CharClass && !(r.negated && r.runes.is_empty()))
                || matches!(r.op, SyntaxOp::AnyCharNotNL | SyntaxOp::AnyChar)
        };
        let mut out2: Vec<SyntaxRegexp> = Vec::with_capacity(sub.len());
        let mut start = 0usize;
        let mut first: Option<SyntaxRegexp> = None;
        for i in 0..=sub.len() {
            let mut continued = false;
            if i < sub.len() {
                if let Some(ifirst) = leading_regexp(&sub[i]) {
                    let is_factorable = is_char_class(ifirst)
                        || (ifirst.op == SyntaxOp::Repeat
                            && ifirst.min == ifirst.max
                            && ifirst.sub.first().is_some_and(is_char_class));
                    if is_factorable {
                        if first.is_none() {
                            first = Some(ifirst.clone());
                            start = i;
                            continued = true;
                        } else if first.as_ref() == Some(ifirst) {
                            continued = true;
                        }
                    }
                }
            }
            if continued {
                continue;
            }
            if let Some(fnode) = first.take() {
                if i == start + 1 {
                    out2.push(sub[start].clone());
                } else if i > start + 1 {
                    let rem: Vec<SyntaxRegexp> = sub[start..i]
                        .iter()
                        .cloned()
                        .map(remove_leading_regexp)
                        .collect();
                    let suffix = factor_alternates(rem, flags);
                    out2.push(collapse_concat(vec![fnode, suffix], flags));
                }
            }
            if i < sub.len() {
                if let Some(ifirst) = leading_regexp(&sub[i]) {
                    let is_factorable = is_char_class(ifirst)
                        || (ifirst.op == SyntaxOp::Repeat
                            && ifirst.min == ifirst.max
                            && ifirst.sub.first().is_some_and(is_char_class));
                    if is_factorable {
                        first = Some(ifirst.clone());
                        start = i;
                        continue;
                    }
                }
                out2.push(sub[i].clone());
                start = i + 1;
            }
        }
        sub = out2;

        // Round 3: merge adjacent single-rune literals and charclasses.
        let mut out3: Vec<SyntaxRegexp> = Vec::with_capacity(sub.len());
        for item in sub {
            let is_single_rune = (item.op == SyntaxOp::Literal && item.runes.len() == 1)
                || item.op == SyntaxOp::CharClass;
            if is_single_rune {
                if let Some(prev) = out3.last_mut() {
                    let prev_is_single = (prev.op == SyntaxOp::Literal && prev.runes.len() == 1)
                        || prev.op == SyntaxOp::CharClass;
                    if prev_is_single && !prev.negated && !is_negated(&item) {
                        let mut ranges = syntax_to_ranges(prev);
                        ranges.extend(syntax_to_ranges(&item));
                        *prev = clean_ranges_to_syntax(ranges, prev.flags & item.flags & FLAG_FOLD_CASE);
                        continue;
                    }
                }
            }
            out3.push(item);
        }

        if out3.len() == 1 {
            out3.into_iter()
                .next()
                .unwrap_or_else(|| SyntaxRegexp::new(SyntaxOp::EmptyMatch, flags))
        } else {
            let mut alt = SyntaxRegexp::new(SyntaxOp::Alternate, flags);
            alt.sub = out3;
            alt
        }
    }

    fn is_negated(re: &SyntaxRegexp) -> bool {
        re.op == SyntaxOp::CharClass && re.negated
    }

    fn syntax_to_ranges(re: &SyntaxRegexp) -> Vec<(char, char)> {
        match re.op {
            SyntaxOp::Literal if re.runes.len() == 1 => {
                let c = re.runes[0];
                if re.is_case_insensitive() {
                    simple_fold_all(c).into_iter().map(|ch| (ch, ch)).collect()
                } else {
                    vec![(c, c)]
                }
            }
            SyntaxOp::CharClass => re.runes.as_chunks::<2>().0.iter().map(|p| (p[0], p[1])).collect(),
            _ => Vec::new(),
        }
    }

    fn clean_ranges_to_syntax(mut ranges: Vec<(char, char)>, flags: u16) -> SyntaxRegexp {
        if ranges.is_empty() {
            let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
            cc.negated = true;
            return cc;
        }
        ranges.sort();
        let mut merged: Vec<(char, char)> = Vec::with_capacity(ranges.len());
        for (lo, hi) in ranges {
            if let Some(last) = merged.last_mut() {
                if (lo as u32) <= (last.1 as u32).saturating_add(1) {
                    if hi > last.1 {
                        last.1 = hi;
                    }
                    continue;
                }
            }
            merged.push((lo, hi));
        }
        if merged.len() == 1 && merged[0].0 == merged[0].1 {
            let mut lit = SyntaxRegexp::new(SyntaxOp::Literal, flags & !FLAG_FOLD_CASE);
            lit.runes.push(merged[0].0);
            return lit;
        }
        if merged.len() == 2 && merged[0].0 == merged[0].1 && merged[1].0 == merged[1].1 {
            let fold = simple_fold_all(merged[0].0);
            if fold.len() == 2 && fold[0] == merged[0].0 && fold[1] == merged[1].0 {
                let mut lit = SyntaxRegexp::new(SyntaxOp::Literal, flags | FLAG_FOLD_CASE);
                lit.runes.push(fold[0]);
                return lit;
            }
        }
        let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
        for (lo, hi) in merged {
            cc.runes.push(lo);
            cc.runes.push(hi);
        }
        cc
    }

    fn syntax_err(pattern: &str, reason: &str) -> MatcherError {
        MatcherError::InvalidRegex {
            pattern: pattern.to_string(),
            reason: reason.to_string(),
        }
    }

    fn collapse_alternates(mut frame: ParserFrame) -> SyntaxRegexp {
        let cur = std::mem::take(&mut frame.cur);
        frame.alts.push(cur);
        let mut branch_nodes: Vec<SyntaxRegexp> = Vec::new();
        for b in frame.alts {
            let node = collapse_concat(b, frame.flags);
            if node.op == SyntaxOp::Alternate {
                branch_nodes.extend(node.sub);
            } else {
                branch_nodes.push(node);
            }
        }
        if branch_nodes.len() == 1 {
            branch_nodes
                .into_iter()
                .next()
                .unwrap_or_else(|| SyntaxRegexp::new(SyntaxOp::EmptyMatch, frame.flags))
        } else {
            factor_alternates(branch_nodes, frame.flags)
        }
    }

    pub fn parse_go_syntax(pattern: &str) -> Result<SyntaxRegexp, MatcherError> {
        go_regexp_syntax::check(pattern)?;
        let b = pattern.as_bytes();
        let len = b.len();
        let mut frames = vec![ParserFrame {
            flags: FLAG_DOT_NL,
            ..Default::default()
        }];
        let mut next_cap = 1usize;
        let mut pos = 0usize;

        while pos < len {
            match b[pos] {
                b'(' => {
                    if pos + 1 < len && b[pos + 1] == b'?' {
                        if pos + 2 < len && b[pos + 2] == b'P' && pos + 3 < len && b[pos + 3] == b'<' {
                            let end = pattern[pos + 4..]
                                .find('>')
                                .map(|i| pos + 4 + i + 1)
                                .ok_or_else(|| syntax_err(pattern, "invalid named capture"))?;
                            let flags = frames
                                .last()
                                .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?
                                .flags;
                            let cap_idx = next_cap;
                            next_cap += 1;
                            frames.push(ParserFrame {
                                flags,
                                is_capturing: true,
                                cap_idx,
                                ..Default::default()
                            });
                            pos = end;
                        } else if pos + 2 < len && b[pos + 2] == b'<' {
                            let end = pattern[pos + 3..]
                                .find('>')
                                .map(|i| pos + 3 + i + 1)
                                .ok_or_else(|| syntax_err(pattern, "invalid named capture"))?;
                            let flags = frames
                                .last()
                                .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?
                                .flags;
                            let cap_idx = next_cap;
                            next_cap += 1;
                            frames.push(ParserFrame {
                                flags,
                                is_capturing: true,
                                cap_idx,
                                ..Default::default()
                            });
                            pos = end;
                        } else {
                            let mut p = pos + 2;
                            let mut flags = frames
                                .last()
                                .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?
                                .flags;
                            let mut neg = false;
                            while p < len && !matches!(b[p], b':' | b')') {
                                match b[p] {
                                    b'-' => neg = true,
                                    b'i' => {
                                        if neg { flags &= !FLAG_FOLD_CASE; } else { flags |= FLAG_FOLD_CASE; }
                                    }
                                    b's' => {
                                        if neg { flags &= !FLAG_DOT_NL; } else { flags |= FLAG_DOT_NL; }
                                    }
                                    b'm' => {
                                        if neg { flags &= !FLAG_MULTILINE; } else { flags |= FLAG_MULTILINE; }
                                    }
                                    b'U' => {
                                        if neg { flags &= !FLAG_NON_GREEDY; } else { flags |= FLAG_NON_GREEDY; }
                                    }
                                    _ => {}
                                }
                                p += 1;
                            }
                            if p < len && b[p] == b':' {
                                frames.push(ParserFrame {
                                    flags,
                                    is_capturing: false,
                                    ..Default::default()
                                });
                                pos = p + 1;
                            } else {
                                frames
                                    .last_mut()
                                    .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?
                                    .flags = flags;
                                pos = (p + 1).min(len);
                            }
                        }
                    } else {
                        let flags = frames
                            .last()
                            .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?
                            .flags;
                        let cap_idx = next_cap;
                        next_cap += 1;
                        frames.push(ParserFrame {
                            flags,
                            is_capturing: true,
                            cap_idx,
                            ..Default::default()
                        });
                        pos += 1;
                    }
                }
                b'|' => {
                    frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?
                        .push_alt();
                    pos += 1;
                }
                b')' => {
                    if frames.len() <= 1 {
                        return Err(syntax_err(pattern, "unexpected )"));
                    }
                    let popped = frames
                        .pop()
                        .ok_or_else(|| syntax_err(pattern, "unexpected )"))?;
                    let cap_idx = popped.cap_idx;
                    let is_cap = popped.is_capturing;
                    let flags = popped.flags;
                    let inner = collapse_alternates(popped);
                    let node = if is_cap {
                        let mut cap = SyntaxRegexp::new(SyntaxOp::Capture, flags);
                        cap.cap = cap_idx;
                        cap.sub.push(inner);
                        cap
                    } else {
                        inner
                    };
                    frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?
                        .push_node(node);
                    pos += 1;
                }
                b'^' => {
                    let frame = frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                    let flags = frame.flags;
                    let op = if (flags & FLAG_MULTILINE) != 0 {
                        SyntaxOp::BeginLine
                    } else {
                        SyntaxOp::BeginText
                    };
                    frame.push_node(SyntaxRegexp::new(op, flags));
                    pos += 1;
                }
                b'$' => {
                    let frame = frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                    let flags = frame.flags;
                    let op = if (flags & FLAG_MULTILINE) != 0 {
                        SyntaxOp::EndLine
                    } else {
                        SyntaxOp::EndText
                    };
                    frame.push_node(SyntaxRegexp::new(op, flags));
                    pos += 1;
                }
                b'.' => {
                    let frame = frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                    let flags = frame.flags;
                    let op = if (flags & FLAG_DOT_NL) != 0 {
                        SyntaxOp::AnyChar
                    } else {
                        SyntaxOp::AnyCharNotNL
                    };
                    frame.push_node(SyntaxRegexp::new(op, flags));
                    pos += 1;
                }
                b'*' | b'+' | b'?' => {
                    let frame = frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                    let mut flags = frame.flags;
                    let op = match b[pos] {
                        b'*' => SyntaxOp::Star,
                        b'+' => SyntaxOp::Plus,
                        _ => SyntaxOp::Quest,
                    };
                    pos += 1;
                    if pos < len && b[pos] == b'?' {
                        flags ^= FLAG_NON_GREEDY;
                        pos += 1;
                    }
                    let sub = frame
                        .cur
                        .pop()
                        .ok_or_else(|| syntax_err(pattern, "missing argument to repetition operator"))?;
                    let sub = if sub.op == SyntaxOp::Literal && sub.runes.len() > 1 {
                        let mut head = sub.clone();
                        let last_ch = head
                            .runes
                            .pop()
                            .ok_or_else(|| syntax_err(pattern, "empty literal repetition"))?;
                        frame.push_node(head);
                        let mut tail = SyntaxRegexp::new(SyntaxOp::Literal, sub.flags);
                        tail.runes.push(last_ch);
                        tail
                    } else {
                        sub
                    };
                    let mut rep = SyntaxRegexp::new(op, flags);
                    rep.sub.push(sub);
                    frame.push_node(rep);
                }
                b'{' => {
                    if let Some((min, max, after)) = parse_repeat_bounds(b, pos) {
                        let frame = frames
                            .last_mut()
                            .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                        let mut flags = frame.flags;
                        pos = after;
                        if pos < len && b[pos] == b'?' {
                            flags ^= FLAG_NON_GREEDY;
                            pos += 1;
                        }
                        let sub = frame.cur.pop().ok_or_else(|| {
                            syntax_err(pattern, "missing argument to repetition operator")
                        })?;
                        let sub = if sub.op == SyntaxOp::Literal && sub.runes.len() > 1 {
                            let mut head = sub.clone();
                            let last_ch = head
                                .runes
                                .pop()
                                .ok_or_else(|| syntax_err(pattern, "empty literal repetition"))?;
                            frame.push_node(head);
                            let mut tail = SyntaxRegexp::new(SyntaxOp::Literal, sub.flags);
                            tail.runes.push(last_ch);
                            tail
                        } else {
                            sub
                        };
                        let mut rep = SyntaxRegexp::new(SyntaxOp::Repeat, flags);
                        rep.min = min;
                        rep.max = max;
                        rep.sub.push(sub);
                        frame.push_node(rep);
                    } else {
                        let frame = frames
                            .last_mut()
                            .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                        let flags = frame.flags;
                        let mut lit = SyntaxRegexp::new(SyntaxOp::Literal, flags);
                        lit.runes.push('{');
                        frame.push_node(lit);
                        pos += 1;
                    }
                }
                b'[' => {
                    let frame = frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                    let flags = frame.flags;
                    let (node, next) = parse_char_class(pattern, pos, flags)?;
                    frame.push_node(node);
                    pos = next;
                }
                b'\\' => {
                    let frame = frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                    let flags = frame.flags;
                    if pos + 1 < len && b[pos + 1] == b'Q' {
                        let start = pos + 2;
                        let end = pattern[start..].find(r"\E").map(|i| start + i);
                        let (body, next) = match end {
                            Some(e) => (&pattern[start..e], e + 2),
                            None => (&pattern[start..], len),
                        };
                        if body.is_empty() {
                            frame.push_node(SyntaxRegexp::new(SyntaxOp::EmptyMatch, flags));
                        } else {
                            let mut lit = SyntaxRegexp::new(SyntaxOp::Literal, flags);
                            for ch in body.chars() {
                                lit.runes.push(if (flags & FLAG_FOLD_CASE) != 0 {
                                    min_fold_rune(ch)
                                } else {
                                    ch
                                });
                            }
                            frame.push_node(lit);
                        }
                        pos = next;
                    } else {
                        let (node, next) = parse_escape_node(pattern, pos, flags)?;
                        frame.push_node(node);
                        pos = next;
                    }
                }
                _ => {
                    let ch = pattern[pos..]
                        .chars()
                        .next()
                        .ok_or_else(|| syntax_err(pattern, "unexpected end of pattern"))?;
                    let clen = ch.len_utf8();
                    let frame = frames
                        .last_mut()
                        .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
                    let flags = frame.flags;
                    let stored = if (flags & FLAG_FOLD_CASE) != 0 {
                        min_fold_rune(ch)
                    } else {
                        ch
                    };
                    let mut lit = SyntaxRegexp::new(SyntaxOp::Literal, flags);
                    lit.runes.push(stored);
                    frame.push_node(lit);
                    pos += clen;
                }
            }
        }

        if frames.len() != 1 {
            return Err(syntax_err(pattern, "missing closing )"));
        }
        let root = frames
            .pop()
            .ok_or_else(|| syntax_err(pattern, "unexpected parser state"))?;
        Ok(collapse_alternates(root))
    }

    // Mirrors Go syntax.parseRepeat / parseInt: rejects leading zeros so {01},
    // {00}, {1,02} stay literal '{', and enforces 0..=1000 and max == -1 || min <= max.
    fn parse_repeat_bounds(b: &[u8], pos: usize) -> Option<(i32, i32, usize)> {
        let mut p = pos + 1;
        let min_start = p;
        while p < b.len() && b[p].is_ascii_digit() {
            p += 1;
        }
        if p == min_start || p >= b.len() {
            return None;
        }
        if p - min_start >= 2 && b[min_start] == b'0' {
            return None;
        }
        let min: i32 = std::str::from_utf8(&b[min_start..p]).ok()?.parse().ok()?;
        if !(0..=1000).contains(&min) {
            return None;
        }
        if b[p] == b'}' {
            return Some((min, min, p + 1));
        }
        if b[p] != b',' {
            return None;
        }
        p += 1;
        if p < b.len() && b[p] == b'}' {
            return Some((min, -1, p + 1));
        }
        let max_start = p;
        while p < b.len() && b[p].is_ascii_digit() {
            p += 1;
        }
        if p == max_start || p >= b.len() || b[p] != b'}' {
            return None;
        }
        if p - max_start >= 2 && b[max_start] == b'0' {
            return None;
        }
        let max: i32 = std::str::from_utf8(&b[max_start..p]).ok()?.parse().ok()?;
        if !(0..=1000).contains(&max) || min > max {
            return None;
        }
        Some((min, max, p + 1))
    }

    fn opaque_char_class(flags: u16) -> SyntaxRegexp {
        let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
        cc.negated = true;
        cc
    }

    fn parse_escape_node(s: &str, pos: usize, flags: u16) -> Result<(SyntaxRegexp, usize), MatcherError> {
        let b = s.as_bytes();
        let next_ch = s
            .get(pos + 1..)
            .and_then(|rest| rest.chars().next())
            .ok_or_else(|| syntax_err(s, "trailing backslash"))?;
        let next_pos = pos + 1 + next_ch.len_utf8();
        Ok(match next_ch {
            'A' => (SyntaxRegexp::new(SyntaxOp::BeginText, flags), next_pos),
            'z' => (SyntaxRegexp::new(SyntaxOp::EndText, flags), next_pos),
            'b' => (SyntaxRegexp::new(SyntaxOp::WordBoundary, flags), next_pos),
            'B' => (SyntaxRegexp::new(SyntaxOp::NoWordBoundary, flags), next_pos),
            'd' => {
                let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
                cc.runes.extend_from_slice(&['0', '9']);
                (cc, next_pos)
            }
            'D' => {
                let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
                cc.negated = true;
                cc.runes.extend_from_slice(&['0', '9']);
                (cc, next_pos)
            }
            's' => {
                let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
                cc.runes.extend_from_slice(&['\t', '\n', '\x0c', '\r', ' ', ' ']);
                (cc, next_pos)
            }
            'S' => {
                let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
                cc.negated = true;
                cc.runes.extend_from_slice(&['\t', '\n', '\x0c', '\r', ' ', ' ']);
                (cc, next_pos)
            }
            'w' => {
                let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
                cc.runes.extend_from_slice(&['0', '9', 'A', 'Z', '_', '_', 'a', 'z']);
                (cc, next_pos)
            }
            'W' => {
                let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
                cc.negated = true;
                cc.runes.extend_from_slice(&['0', '9', 'A', 'Z', '_', '_', 'a', 'z']);
                (cc, next_pos)
            }
            'n' => lit_char('\n', flags, next_pos),
            'r' => lit_char('\r', flags, next_pos),
            't' => lit_char('\t', flags, next_pos),
            'f' => lit_char('\x0c', flags, next_pos),
            'v' => lit_char('\x0b', flags, next_pos),
            'a' => lit_char('\x07', flags, next_pos),
            'x' => {
                if next_pos < b.len() && b[next_pos] == b'{' {
                    let end = s[next_pos + 1..]
                        .find('}')
                        .ok_or_else(|| syntax_err(s, "invalid hex escape"))?;
                    let hex = &s[next_pos + 1..next_pos + 1 + end];
                    let val = u32::from_str_radix(hex, 16)
                        .map_err(|_| syntax_err(s, "invalid hex escape"))?;
                    let Some(ch) = char::from_u32(val) else {
                        return Ok((opaque_char_class(flags), next_pos + 1 + end + 1));
                    };
                    return Ok(lit_char(ch, flags, next_pos + 1 + end + 1));
                }
                let hex = s
                    .get(next_pos..next_pos + 2)
                    .ok_or_else(|| syntax_err(s, "invalid hex escape"))?;
                let val = u32::from_str_radix(hex, 16)
                    .map_err(|_| syntax_err(s, "invalid hex escape"))?;
                let ch = char::from_u32(val).unwrap_or('\u{FFFD}');
                lit_char(ch, flags, next_pos + 2)
            }
            '0'..='7' => {
                let mut p = pos + 1;
                let mut v = 0u32;
                for _ in 0..3 {
                    if p < b.len() && (b'0'..=b'7').contains(&b[p]) {
                        v = (v << 3) | ((b[p] - b'0') as u32);
                        p += 1;
                    } else {
                        break;
                    }
                }
                let ch = char::from_u32(v).unwrap_or('\u{FFFD}');
                lit_char(ch, flags, p)
            }
            'p' | 'P' => {
                let name_end = if next_pos < b.len() && b[next_pos] == b'{' {
                    let end = s[next_pos + 1..]
                        .find('}')
                        .ok_or_else(|| syntax_err(s, "invalid unicode class"))?;
                    next_pos + 1 + end + 1
                } else {
                    let ch = s
                        .get(next_pos..)
                        .and_then(|rest| rest.chars().next())
                        .ok_or_else(|| syntax_err(s, "invalid unicode class"))?;
                    next_pos + ch.len_utf8()
                };
                // Mark Unicode classes opaque so FastRegexMatcher always delegates to the compiled Regex.
                (opaque_char_class(flags), name_end)
            }
            c => lit_char(c, flags, next_pos),
        })
    }

    fn lit_char(ch: char, flags: u16, next: usize) -> (SyntaxRegexp, usize) {
        let mut lit = SyntaxRegexp::new(SyntaxOp::Literal, flags);
        lit.runes.push(if (flags & FLAG_FOLD_CASE) != 0 {
            min_fold_rune(ch)
        } else {
            ch
        });
        (lit, next)
    }

    fn push_scalar_range_no_surrogates(out: &mut Vec<(char, char)>, lo: u32, hi: u32) {
        if lo > hi {
            return;
        }
        if lo <= 0xD7FF {
            let r_hi = hi.min(0xD7FF);
            if let (Some(a), Some(b)) = (char::from_u32(lo), char::from_u32(r_hi)) {
                out.push((a, b));
            }
        }
        if hi >= 0xE000 {
            let r_lo = lo.max(0xE000);
            if let (Some(a), Some(b)) = (char::from_u32(r_lo), char::from_u32(hi.min(0x10FFFF))) {
                out.push((a, b));
            }
        }
    }

    fn complement_ranges(input: &[(char, char)]) -> Vec<(char, char)> {
        let mut sorted = input.to_vec();
        sorted.sort();
        let mut out = Vec::new();
        let mut cur: u32 = 0;
        for (lo, hi) in sorted {
            let lo_u = lo as u32;
            let hi_u = hi as u32;
            if lo_u > cur {
                push_scalar_range_no_surrogates(&mut out, cur, lo_u - 1);
            }
            if hi_u >= cur {
                cur = hi_u.saturating_add(1);
                if (0xD800..=0xDFFF).contains(&cur) {
                    cur = 0xE000;
                }
            }
        }
        if cur <= 0x10FFFF {
            push_scalar_range_no_surrogates(&mut out, cur, 0x10FFFF);
        }
        out
    }

    fn posix_class_ranges(cname: &str) -> Option<Vec<(char, char)>> {
        let (negated, base) = match cname.strip_prefix('^') {
            Some(rest) => (true, rest),
            None => (false, cname),
        };
        let pos: Vec<(char, char)> = match base {
            "alnum" => vec![('0', '9'), ('A', 'Z'), ('a', 'z')],
            "alpha" => vec![('A', 'Z'), ('a', 'z')],
            "ascii" => vec![('\0', '\x7f')],
            "blank" => vec![('\t', '\t'), (' ', ' ')],
            "cntrl" => vec![('\0', '\x1f'), ('\x7f', '\x7f')],
            "digit" => vec![('0', '9')],
            "graph" => vec![('!', '~')],
            "lower" => vec![('a', 'z')],
            "print" => vec![(' ', '~')],
            "punct" => vec![('!', '/'), (':', '@'), ('[', '`'), ('{', '~')],
            "space" => vec![('\t', '\r'), (' ', ' ')],
            "upper" => vec![('A', 'Z')],
            "word" => vec![('0', '9'), ('A', 'Z'), ('_', '_'), ('a', 'z')],
            "xdigit" => vec![('0', '9'), ('A', 'F'), ('a', 'f')],
            _ => return None,
        };
        if negated {
            Some(complement_ranges(&pos))
        } else {
            Some(pos)
        }
    }

    enum ClassItem {
        Char {
            ch: char,
            is_surrogate_or_invalid: bool,
            next: usize,
        },
        Ranges {
            ranges: Vec<(char, char)>,
            next: usize,
        },
        Opaque {
            next: usize,
        },
    }

    fn parse_char_class(s: &str, start: usize, flags: u16) -> Result<(SyntaxRegexp, usize), MatcherError> {
        let b = s.as_bytes();
        let mut p = start + 1;
        let mut negated = false;
        if p < b.len() && b[p] == b'^' {
            negated = true;
            p += 1;
        }
        let mut ranges: Vec<(char, char)> = Vec::new();
        let mut opaque = false;
        let mut first = true;
        let mut closed = false;
        while p < b.len() {
            if b[p] == b']' && !first {
                p += 1;
                closed = true;
                break;
            }
            first = false;
            if b[p] == b'[' && p + 1 < b.len() && b[p + 1] == b':' {
                if let Some(end) = s[p + 2..].find(":]") {
                    let cname = &s[p + 2..p + 2 + end];
                    if let Some(posix_ranges) = posix_class_ranges(cname) {
                        ranges.extend(posix_ranges);
                    } else {
                        opaque = true;
                    }
                    p = p + 2 + end + 2;
                    continue;
                }
            }
            match parse_class_item(s, p)? {
                ClassItem::Opaque { next } => {
                    opaque = true;
                    p = next;
                }
                ClassItem::Ranges { ranges: cr, next } => {
                    ranges.extend(cr);
                    p = next;
                }
                ClassItem::Char {
                    ch: lo,
                    is_surrogate_or_invalid: lo_inv,
                    next: next1,
                } => {
                    p = next1;
                    if p + 1 < b.len() && b[p] == b'-' && b[p + 1] != b']' {
                        match parse_class_item(s, p + 1)? {
                            ClassItem::Opaque { next } => {
                                opaque = true;
                                p = next;
                            }
                            ClassItem::Ranges { next, .. } => {
                                opaque = true;
                                p = next;
                            }
                            ClassItem::Char {
                                ch: hi,
                                is_surrogate_or_invalid: hi_inv,
                                next: next2,
                            } => {
                                p = next2;
                                if lo_inv || hi_inv || lo > hi {
                                    opaque = true;
                                } else {
                                    ranges.push((lo, hi));
                                }
                            }
                        }
                    } else if lo_inv {
                        opaque = true;
                    } else {
                        ranges.push((lo, lo));
                    }
                }
            }
        }
        if !closed {
            return Err(syntax_err(s, "missing closing ]"));
        }
        if opaque {
            return Ok((opaque_char_class(flags), p));
        }
        if (flags & FLAG_FOLD_CASE) != 0 {
            let mut folded = Vec::with_capacity(ranges.len() * 2);
            for (lo, hi) in ranges {
                folded.push((lo, hi));
                if lo <= hi && (hi as u32 - lo as u32) <= 256 {
                    for c in lo..=hi {
                        for fc in simple_fold_all(c) {
                            folded.push((fc, fc));
                        }
                    }
                } else {
                    return Ok((opaque_char_class(flags), p));
                }
            }
            ranges = folded;
        }
        if negated {
            let mut cc = SyntaxRegexp::new(SyntaxOp::CharClass, flags);
            cc.negated = true;
            for (lo, hi) in ranges {
                cc.runes.push(lo);
                cc.runes.push(hi);
            }
            return Ok((cc, p));
        }
        Ok((clean_ranges_to_syntax(ranges, flags), p))
    }

    fn parse_class_item(s: &str, pos: usize) -> Result<ClassItem, MatcherError> {
        let b = s.as_bytes();
        if b[pos] == b'\\' {
            let next_ch = s
                .get(pos + 1..)
                .and_then(|rest| rest.chars().next())
                .ok_or_else(|| syntax_err(s, "trailing backslash"))?;
            let next_pos = pos + 1 + next_ch.len_utf8();
            return Ok(match next_ch {
                'd' => ClassItem::Ranges {
                    ranges: vec![('0', '9')],
                    next: next_pos,
                },
                's' => ClassItem::Ranges {
                    ranges: vec![('\t', '\n'), ('\x0c', '\r'), (' ', ' ')],
                    next: next_pos,
                },
                'w' => ClassItem::Ranges {
                    ranges: vec![('0', '9'), ('A', 'Z'), ('_', '_'), ('a', 'z')],
                    next: next_pos,
                },
                'D' => ClassItem::Ranges {
                    ranges: complement_ranges(&[('0', '9')]),
                    next: next_pos,
                },
                'S' => ClassItem::Ranges {
                    ranges: complement_ranges(&[('\t', '\n'), ('\x0c', '\r'), (' ', ' ')]),
                    next: next_pos,
                },
                'W' => ClassItem::Ranges {
                    ranges: complement_ranges(&[('0', '9'), ('A', 'Z'), ('_', '_'), ('a', 'z')]),
                    next: next_pos,
                },
                'p' | 'P' => {
                    let name_end = if next_pos < b.len() && b[next_pos] == b'{' {
                        let end = s[next_pos + 1..]
                            .find('}')
                            .ok_or_else(|| syntax_err(s, "invalid unicode class"))?;
                        next_pos + 1 + end + 1
                    } else {
                        let ch = s
                            .get(next_pos..)
                            .and_then(|rest| rest.chars().next())
                            .ok_or_else(|| syntax_err(s, "invalid unicode class"))?;
                        next_pos + ch.len_utf8()
                    };
                    ClassItem::Opaque { next: name_end }
                }
                'n' => ClassItem::Char {
                    ch: '\n',
                    is_surrogate_or_invalid: false,
                    next: next_pos,
                },
                'r' => ClassItem::Char {
                    ch: '\r',
                    is_surrogate_or_invalid: false,
                    next: next_pos,
                },
                't' => ClassItem::Char {
                    ch: '\t',
                    is_surrogate_or_invalid: false,
                    next: next_pos,
                },
                'f' => ClassItem::Char {
                    ch: '\x0c',
                    is_surrogate_or_invalid: false,
                    next: next_pos,
                },
                'v' => ClassItem::Char {
                    ch: '\x0b',
                    is_surrogate_or_invalid: false,
                    next: next_pos,
                },
                'a' => ClassItem::Char {
                    ch: '\x07',
                    is_surrogate_or_invalid: false,
                    next: next_pos,
                },
                'x' => {
                    if next_pos < b.len() && b[next_pos] == b'{' {
                        let end = s[next_pos + 1..]
                            .find('}')
                            .ok_or_else(|| syntax_err(s, "invalid hex escape"))?;
                        let hex = &s[next_pos + 1..next_pos + 1 + end];
                        let val = u32::from_str_radix(hex, 16)
                            .map_err(|_| syntax_err(s, "invalid hex escape"))?;
                        let ch = char::from_u32(val);
                        return Ok(ClassItem::Char {
                            ch: ch.unwrap_or('\u{FFFD}'),
                            is_surrogate_or_invalid: ch.is_none(),
                            next: next_pos + 1 + end + 1,
                        });
                    }
                    let hex = s
                        .get(next_pos..next_pos + 2)
                        .ok_or_else(|| syntax_err(s, "invalid hex escape"))?;
                    let val = u32::from_str_radix(hex, 16)
                        .map_err(|_| syntax_err(s, "invalid hex escape"))?;
                    let ch = char::from_u32(val);
                    ClassItem::Char {
                        ch: ch.unwrap_or('\u{FFFD}'),
                        is_surrogate_or_invalid: ch.is_none(),
                        next: next_pos + 2,
                    }
                }
                '0'..='7' => {
                    let mut p = pos + 1;
                    let mut v = 0u32;
                    for _ in 0..3 {
                        if p < b.len() && (b'0'..=b'7').contains(&b[p]) {
                            v = (v << 3) | ((b[p] - b'0') as u32);
                            p += 1;
                        } else {
                            break;
                        }
                    }
                    let ch = char::from_u32(v);
                    ClassItem::Char {
                        ch: ch.unwrap_or('\u{FFFD}'),
                        is_surrogate_or_invalid: ch.is_none(),
                        next: p,
                    }
                }
                c => ClassItem::Char {
                    ch: c,
                    is_surrogate_or_invalid: false,
                    next: next_pos,
                },
            });
        }
        let ch = s
            .get(pos..)
            .and_then(|rest| rest.chars().next())
            .ok_or_else(|| syntax_err(s, "unexpected end of character class"))?;
        Ok(ClassItem::Char {
            ch,
            is_surrogate_or_invalid: false,
            next: pos + ch.len_utf8(),
        })
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum StringMatcher {
        Empty,
        True,
        AnyWithoutNewline,
        AnyNonEmpty {
            match_nl: bool,
        },
        ZeroOrOneChar {
            match_nl: bool,
        },
        Equal {
            s: String,
            case_sensitive: bool,
        },
        Or(Vec<StringMatcher>),
        EqualMultiSlice {
            values: Vec<String>,
            lengths_mask: u64,
            case_sensitive: bool,
        },
        EqualMultiMap {
            values: HashSet<String>,
            lengths_mask: u64,
            prefixes: BTreeMap<String, Vec<StringMatcher>>,
            min_prefix_len: usize,
            case_sensitive: bool,
        },
        LiteralPrefixSensitive {
            prefix: String,
            right: Box<StringMatcher>,
        },
        LiteralPrefixInsensitive {
            prefix: String,
            right: Box<StringMatcher>,
        },
        LiteralSuffix {
            left: Box<StringMatcher>,
            suffix: String,
            suffix_case_sensitive: bool,
        },
        Contains {
            left: Option<Box<StringMatcher>>,
            substrings: Vec<String>,
            right: Option<Box<StringMatcher>>,
        },
    }

    impl StringMatcher {
        pub fn matches(&self, s: &str) -> bool {
            self.matches_bytes(s.as_bytes())
        }

        pub fn matches_bytes(&self, b: &[u8]) -> bool {
            match self {
                Self::Empty => b.is_empty(),
                Self::True => true,
                Self::AnyWithoutNewline => !b.contains(&b'\n'),
                Self::AnyNonEmpty { match_nl } => {
                    if *match_nl {
                        !b.is_empty()
                    } else {
                        !b.is_empty() && !b.contains(&b'\n')
                    }
                }
                Self::ZeroOrOneChar { match_nl } => {
                    if b.is_empty() {
                        return true;
                    }
                    let (_, size) = decode_go_rune(b);
                    if size < b.len() {
                        return false;
                    }
                    *match_nl || b[0] != b'\n'
                }
                Self::Equal { s, case_sensitive } => {
                    if *case_sensitive {
                        b == s.as_bytes()
                    } else {
                        equal_fold_bytes(s.as_bytes(), b)
                    }
                }
                Self::Or(list) => list.iter().any(|m| m.matches_bytes(b)),
                Self::EqualMultiSlice {
                    values,
                    lengths_mask,
                    case_sensitive,
                } => {
                    if *case_sensitive {
                        (lengths_mask & length_mask_bytes(b)) != 0
                            && values.iter().any(|v| v.as_bytes() == b)
                    } else {
                        values.iter().any(|v| equal_fold_bytes(b, v.as_bytes()))
                    }
                }
                Self::EqualMultiMap {
                    values,
                    lengths_mask,
                    prefixes,
                    min_prefix_len,
                    case_sensitive,
                } => {
                    if !values.is_empty() {
                        if *min_prefix_len == 0
                            && *case_sensitive
                            && (lengths_mask & length_mask_bytes(b)) == 0
                        {
                            return false;
                        }
                        if let Ok(s) = std::str::from_utf8(b) {
                            if *case_sensitive {
                                if values.contains(s) {
                                    return true;
                                }
                            } else {
                                let norm = to_normalised_lower(s);
                                if values.contains(&norm) {
                                    return true;
                                }
                            }
                        }
                    }
                    if *min_prefix_len > 0 && b.len() >= *min_prefix_len {
                        if let Ok(s) = std::str::from_utf8(b) {
                            if s.is_char_boundary(*min_prefix_len) {
                                let pref = if *case_sensitive {
                                    s[..*min_prefix_len].to_string()
                                } else {
                                    to_normalised_lower(&s[..*min_prefix_len])
                                };
                                if let Some(list) = prefixes.get(&pref) {
                                    if list.iter().any(|m| m.matches_bytes(b)) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    false
                }
                Self::LiteralPrefixSensitive { prefix, right } => {
                    b.strip_prefix(prefix.as_bytes())
                        .is_some_and(|rest| right.matches_bytes(rest))
                }
                Self::LiteralPrefixInsensitive { prefix, right } => {
                    prefix_case_insensitive_match_len_bytes(b, prefix.as_bytes())
                        .is_some_and(|n| right.matches_bytes(&b[n..]))
                }
                Self::LiteralSuffix {
                    left,
                    suffix,
                    suffix_case_sensitive,
                } => {
                    if *suffix_case_sensitive {
                        b.strip_suffix(suffix.as_bytes())
                            .is_some_and(|rest| left.matches_bytes(rest))
                    } else {
                        suffix_case_insensitive_match_len_bytes(b, suffix.as_bytes())
                            .is_some_and(|n| left.matches_bytes(&b[..b.len() - n]))
                    }
                }
                Self::Contains {
                    left,
                    substrings,
                    right,
                } => {
                    for sub in substrings {
                        let sb = sub.as_bytes();
                        match (left.as_deref(), right.as_deref()) {
                            (Some(l), Some(r)) => {
                                let mut start = 0usize;
                                while start + sb.len() <= b.len() {
                                    if let Some(rel) = find_subslice(&b[start..], sb) {
                                        let pos = start + rel;
                                        if l.matches_bytes(&b[..pos])
                                            && r.matches_bytes(&b[pos + sb.len()..])
                                        {
                                            return true;
                                        }
                                        start = pos + 1;
                                    } else {
                                        break;
                                    }
                                }
                            }
                            (Some(l), None) => {
                                if b.ends_with(sb) && l.matches_bytes(&b[..b.len() - sb.len()]) {
                                    return true;
                                }
                            }
                            (None, Some(r)) => {
                                if b.starts_with(sb) && r.matches_bytes(&b[sb.len()..]) {
                                    return true;
                                }
                            }
                            (None, None) => {}
                        }
                    }
                    false
                }
            }
        }

        pub fn add(&mut self, s: &str) {
            match self {
                Self::EqualMultiSlice {
                    values,
                    lengths_mask,
                    ..
                } => {
                    values.push(s.to_string());
                    *lengths_mask |= length_mask(s);
                }
                Self::EqualMultiMap {
                    values,
                    lengths_mask,
                    case_sensitive,
                    ..
                } => {
                    if !*case_sensitive {
                        values.insert(to_normalised_lower(s));
                    } else {
                        *lengths_mask |= length_mask(s);
                        values.insert(s.to_string());
                    }
                }
                _ => panic!("add called on non-multi StringMatcher"),
            }
        }

        pub fn add_prefix(
            &mut self,
            prefix: &str,
            prefix_case_sensitive: bool,
            matcher: StringMatcher,
        ) {
            match self {
                Self::EqualMultiSlice { .. } => panic!("not implemented"),
                Self::EqualMultiMap {
                    prefixes,
                    min_prefix_len,
                    case_sensitive,
                    ..
                } => {
                    assert!(*min_prefix_len != 0, "addPrefix called when no prefix length defined");
                    assert!(prefix.len() >= *min_prefix_len, "addPrefix called with a too short prefix");
                    assert!(
                        *case_sensitive == prefix_case_sensitive,
                        "addPrefix called with a prefix whose case sensitivity is different than the expected one"
                    );
                    let key = if !*case_sensitive {
                        prefix[..*min_prefix_len].to_ascii_lowercase()
                    } else {
                        prefix[..*min_prefix_len].to_string()
                    };
                    prefixes.entry(key).or_default().push(matcher);
                }
                _ => panic!("add_prefix called on non-multi StringMatcher"),
            }
        }

        pub fn set_matches(&self) -> Option<Vec<String>> {
            match self {
                Self::EqualMultiSlice { values, .. } => Some(values.clone()),
                Self::EqualMultiMap {
                    values, prefixes, ..
                } => {
                    if values.len() >= MAX_SET_MATCHES || !prefixes.is_empty() {
                        return None;
                    }
                    Some(values.iter().cloned().collect())
                }
                _ => None,
            }
        }
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() {
            return Some(0);
        }
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    #[inline]
    fn decode_go_rune(b: &[u8]) -> (char, usize) {
        if b.is_empty() {
            return ('\u{FFFD}', 0);
        }
        let b0 = b[0];
        if b0 < 0x80 {
            return (b0 as char, 1);
        }
        let max_len = b.len().min(4);
        for len in 2..=max_len {
            if let Ok(s) = std::str::from_utf8(&b[..len]) {
                if let Some(ch) = s.chars().next() {
                    return (ch, len);
                }
            }
        }
        ('\u{FFFD}', 1)
    }

    #[inline]
    fn decode_go_last_rune(b: &[u8]) -> (char, usize) {
        if b.is_empty() {
            return ('\u{FFFD}', 0);
        }
        let b_last = b[b.len() - 1];
        if b_last < 0x80 {
            return (b_last as char, 1);
        }
        let max_len = b.len().min(4);
        for len in 2..=max_len {
            if let Ok(s) = std::str::from_utf8(&b[b.len() - len..]) {
                if let Some(ch) = s.chars().next() {
                    return (ch, len);
                }
            }
        }
        ('\u{FFFD}', 1)
    }

    #[inline]
    pub fn rune_fold_equal(a: char, b: char) -> bool {
        if a == b {
            return true;
        }
        let mut cur = simple_fold_next(a);
        while cur != a {
            if cur == b {
                return true;
            }
            cur = simple_fold_next(cur);
        }
        false
    }

    fn equal_fold_bytes(a: &[u8], b: &[u8]) -> bool {
        let mut ia = 0usize;
        let mut ib = 0usize;
        while ia < a.len() && ib < b.len() {
            let (ca, la) = decode_go_rune(&a[ia..]);
            let (cb, lb) = decode_go_rune(&b[ib..]);
            if !rune_fold_equal(ca, cb) {
                return false;
            }
            ia += la;
            ib += lb;
        }
        ia == a.len() && ib == b.len()
    }

    #[inline]
    pub fn length_mask(s: &str) -> u64 {
        1u64 << s.len().min(63)
    }

    #[inline]
    pub fn length_mask_bytes(b: &[u8]) -> u64 {
        1u64 << b.len().min(63)
    }

    pub fn to_normalised_lower(s: &str) -> String {
        let bytes = s.as_bytes();
        for (i, &c) in bytes.iter().enumerate() {
            if c >= 0x80 {
                return to_normalised_lower_unicode(s);
            }
            if c.is_ascii_uppercase() {
                let mut out = bytes.to_vec();
                for byte in &mut out[i..] {
                    if *byte >= 0x80 {
                        return to_normalised_lower_unicode(s);
                    }
                    if byte.is_ascii_uppercase() {
                        *byte = *byte + b'a' - b'A';
                    }
                }
                return String::from_utf8(out).expect("ASCII");
            }
        }
        s.to_string()
    }

    fn to_normalised_lower_unicode(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for ch in s.chars() {
            match ch {
                '\u{017f}' => out.push('s'),
                '\u{212a}' => out.push('k'),
                c => {
                    for lc in c.to_lowercase() {
                        out.push(lc);
                    }
                }
            }
        }
        out
    }

    #[inline]
    pub fn has_prefix_case_insensitive(s: &str, prefix: &str) -> bool {
        prefix_case_insensitive_match_len(s, prefix).is_some()
    }

    #[inline]
    pub fn prefix_case_insensitive_match_len(s: &str, prefix: &str) -> Option<usize> {
        prefix_case_insensitive_match_len_bytes(s.as_bytes(), prefix.as_bytes())
    }

    pub fn prefix_case_insensitive_match_len_bytes(s: &[u8], prefix: &[u8]) -> Option<usize> {
        let mut i = 0usize;
        while i < prefix.len() && i < s.len() {
            let pc = prefix[i];
            let sc = s[i];
            if pc >= 0x80 || sc >= 0x80 {
                break;
            }
            if pc != sc && !pc.eq_ignore_ascii_case(&sc) {
                return None;
            }
            i += 1;
        }
        if i == prefix.len() {
            return Some(i);
        }
        let mut pi = i;
        let mut si = i;
        while pi < prefix.len() {
            if si >= s.len() {
                return None;
            }
            let (pr, plen) = decode_go_rune(&prefix[pi..]);
            let (sr, slen) = decode_go_rune(&s[si..]);
            if !rune_fold_equal(sr, pr) {
                return None;
            }
            pi += plen;
            si += slen;
        }
        Some(si)
    }

    #[inline]
    pub fn suffix_case_insensitive_match_len(s: &str, suffix: &str) -> Option<usize> {
        suffix_case_insensitive_match_len_bytes(s.as_bytes(), suffix.as_bytes())
    }

    pub fn suffix_case_insensitive_match_len_bytes(s: &[u8], suffix: &[u8]) -> Option<usize> {
        let mut i = suffix.len();
        let mut j = s.len();
        while i > 0 && j > 0 {
            let pc = suffix[i - 1];
            let sc = s[j - 1];
            if pc >= 0x80 || sc >= 0x80 {
                break;
            }
            if pc != sc && !pc.eq_ignore_ascii_case(&sc) {
                return None;
            }
            i -= 1;
            j -= 1;
        }
        if i == 0 {
            return Some(s.len() - j);
        }
        while i > 0 {
            if j == 0 {
                return None;
            }
            let (pr, plen) = decode_go_last_rune(&suffix[..i]);
            let (sr, slen) = decode_go_last_rune(&s[..j]);
            if !rune_fold_equal(sr, pr) {
                return None;
            }
            i -= plen;
            j -= slen;
        }
        Some(s.len() - j)
    }

    #[inline]
    pub fn contains_in_order(s: &str, contains: &[&str]) -> bool {
        if contains.len() == 1 {
            return s.contains(contains[0]);
        }
        let mut offset = 0usize;
        for substr in contains {
            match s[offset..].find(substr) {
                Some(at) => offset += at + substr.len(),
                None => return false,
            }
        }
        true
    }

    pub fn new_equal_multi_string_matcher(
        case_sensitive: bool,
        estimated_size: usize,
        estimated_prefixes: usize,
        min_prefix_len: usize,
    ) -> StringMatcher {
        if estimated_size < MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD && estimated_prefixes == 0 {
            StringMatcher::EqualMultiSlice {
                values: Vec::with_capacity(estimated_size),
                lengths_mask: 0,
                case_sensitive,
            }
        } else {
            StringMatcher::EqualMultiMap {
                values: HashSet::with_capacity(estimated_size),
                lengths_mask: 0,
                prefixes: BTreeMap::new(),
                min_prefix_len,
                case_sensitive,
            }
        }
    }

    fn is_go_literal(s: &str) -> bool {
        !s.bytes().any(|b| {
            matches!(
                b,
                b'\\' | b'+' | b'.' | b'*' | b'?' | b'(' | b')' | b'|' | b'[' | b']' | b'{' | b'}' | b'^' | b'$'
            )
        })
    }

    pub fn optimize_alternating_literals(s: &str) -> Option<(StringMatcher, Option<Vec<String>>)> {
        if s.is_empty() {
            return Some((StringMatcher::Empty, None));
        }
        let estimated = s.bytes().filter(|&b| b == b'|').count() + 1;
        if estimated == 1 {
            if is_go_literal(s) {
                return Some((
                    StringMatcher::Equal {
                        s: s.to_string(),
                        case_sensitive: true,
                    },
                    Some(vec![s.to_string()]),
                ));
            }
            return None;
        }
        let mut multi = new_equal_multi_string_matcher(true, estimated, 0, 0);
        for sub in s.split('|') {
            if !is_go_literal(sub) {
                return None;
            }
            multi.add(sub);
        }
        let set = multi.set_matches();
        Some((multi, set))
    }

    fn is_match_any(r: &SyntaxRegexp) -> bool {
        r.op == SyntaxOp::Star && r.sub.first().is_some_and(|s| s.op == SyntaxOp::AnyChar)
    }

    fn is_case_sensitive_literal(r: &SyntaxRegexp) -> bool {
        r.op == SyntaxOp::Literal && r.is_case_sensitive()
    }

    pub fn optimize_alternating_simple_contains(r: SyntaxRegexp) -> SyntaxRegexp {
        if r.op != SyntaxOp::Alternate {
            return r;
        }
        let mut literals = Vec::with_capacity(r.sub.len());
        for sub in &r.sub {
            if sub.op != SyntaxOp::Concat || sub.sub.len() != 3 {
                return r;
            }
            let cs = &sub.sub;
            if !is_case_sensitive_literal(&cs[1]) || !is_match_any(&cs[0]) || !is_match_any(&cs[2]) {
                return r;
            }
            literals.push(cs[1].clone());
        }
        if literals.len() > 1 {
            let mut prefix_any = SyntaxRegexp::new(SyntaxOp::Star, FLAG_DOT_NL);
            prefix_any.sub.push(SyntaxRegexp::new(SyntaxOp::AnyChar, FLAG_DOT_NL));
            let mut suffix_any = SyntaxRegexp::new(SyntaxOp::Star, FLAG_DOT_NL);
            suffix_any.sub.push(SyntaxRegexp::new(SyntaxOp::AnyChar, FLAG_DOT_NL));
            let mut alts = SyntaxRegexp::new(SyntaxOp::Alternate, FLAG_DOT_NL);
            alts.sub = literals;
            let mut concat = SyntaxRegexp::new(SyntaxOp::Concat, FLAG_DOT_NL);
            concat.sub = vec![prefix_any, alts, suffix_any];
            return concat;
        }
        r
    }

    pub fn clear_capture(re: &mut SyntaxRegexp) {
        while re.op == SyntaxOp::Capture {
            let inner = re.sub.remove(0);
            *re = inner;
        }
    }

    fn clear_begin_end_text(re: &mut SyntaxRegexp) {
        if re.op == SyntaxOp::Alternate || re.sub.is_empty() {
            return;
        }
        if re.sub.len() == 1
            && (re.sub[0].op == SyntaxOp::BeginText || re.sub[0].op == SyntaxOp::EndText)
        {
            re.op = SyntaxOp::EmptyMatch;
            re.sub.clear();
            return;
        }
        if re.sub.first().is_some_and(|s| s.op == SyntaxOp::BeginText) {
            re.sub.remove(0);
        }
        if re.sub.last().is_some_and(|s| s.op == SyntaxOp::EndText) {
            re.sub.pop();
        }
    }

    pub fn optimize_concat_regex(r: &SyntaxRegexp) -> (bool, String, String, Vec<String>) {
        if r.op != SyntaxOp::Concat {
            return (false, String::new(), String::new(), Vec::new());
        }
        let mut subs: Vec<SyntaxRegexp> = r.sub.clone();
        for s in &mut subs {
            clear_capture(s);
        }
        if subs.first().is_some_and(|s| s.op == SyntaxOp::BeginText) {
            subs.remove(0);
        }
        if subs.last().is_some_and(|s| s.op == SyntaxOp::EndText) {
            subs.pop();
        }
        if subs.is_empty() {
            return (false, String::new(), String::new(), Vec::new());
        }
        let mut prefix = String::new();
        let mut case_insensitive_prefix = false;
        let mut suffix = String::new();
        let mut contains = Vec::new();
        if subs[0].op == SyntaxOp::Literal {
            prefix = subs[0].runes.iter().collect();
            case_insensitive_prefix = subs[0].is_case_insensitive();
        }
        let last = subs.len() - 1;
        if subs[last].op == SyntaxOp::Literal && subs[last].is_case_sensitive() {
            suffix = subs[last].runes.iter().collect();
        }
        if subs.len() > 2 {
            for mid in &subs[1..last] {
                if mid.op == SyntaxOp::Literal && mid.is_case_sensitive() {
                    contains.push(mid.runes.iter().collect());
                }
            }
        }
        (case_insensitive_prefix, prefix, suffix, contains)
    }

    pub fn find_set_matches(re: &mut SyntaxRegexp) -> (Option<Vec<String>>, bool) {
        clear_begin_end_text(re);
        find_set_matches_internal(re, "")
    }

    fn find_set_matches_internal(re: &mut SyntaxRegexp, base: &str) -> (Option<Vec<String>>, bool) {
        match re.op {
            SyntaxOp::BeginText | SyntaxOp::EndText => (None, false),
            SyntaxOp::Literal => {
                let s: String = re.runes.iter().collect();
                (Some(vec![format!("{base}{s}")]), re.is_case_sensitive())
            }
            SyntaxOp::EmptyMatch => {
                if !base.is_empty() {
                    (Some(vec![base.to_string()]), re.is_case_sensitive())
                } else {
                    (None, false)
                }
            }
            SyntaxOp::Alternate => find_set_matches_from_alternate(re, base),
            SyntaxOp::Capture => {
                clear_capture(re);
                find_set_matches_internal(re, base)
            }
            SyntaxOp::Concat => find_set_matches_from_concat(re, base),
            SyntaxOp::CharClass => {
                if re.negated || !re.runes.len().is_multiple_of(2) {
                    return (None, false);
                }
                let mut total = 0usize;
                for pair in re.runes.as_chunks::<2>().0 {
                    if pair[1] < pair[0] {
                        return (None, false);
                    }
                    total = total.saturating_add((pair[1] as u32 - pair[0] as u32 + 1) as usize);
                }
                if total > MAX_SET_MATCHES {
                    return (None, false);
                }
                let mut matches = Vec::with_capacity(total);
                for pair in re.runes.as_chunks::<2>().0 {
                    for c in pair[0]..=pair[1] {
                        matches.push(format!("{base}{c}"));
                    }
                }
                (Some(matches), re.is_case_sensitive())
            }
            _ => (None, false),
        }
    }

    fn find_set_matches_from_concat(
        re: &mut SyntaxRegexp,
        base: &str,
    ) -> (Option<Vec<String>>, bool) {
        if re.sub.is_empty() {
            return (None, false);
        }
        for s in &mut re.sub {
            clear_capture(s);
        }
        let mut matches = vec![base.to_string()];
        let mut matches_case_sensitive = false;
        for i in 0..re.sub.len() {
            let mut new_matches: Vec<String> = Vec::new();
            for (j, b) in matches.iter().enumerate() {
                let (Some(m), cs) = find_set_matches_internal(&mut re.sub[i], b) else {
                    return (None, false);
                };
                if new_matches.len() + m.len() > MAX_SET_MATCHES {
                    return (None, false);
                }
                if i == 0 && j == 0 {
                    matches_case_sensitive = cs;
                }
                if matches_case_sensitive != cs {
                    return (None, false);
                }
                new_matches.extend(m);
            }
            matches = new_matches;
        }
        (Some(matches), matches_case_sensitive)
    }

    fn find_set_matches_from_alternate(
        re: &mut SyntaxRegexp,
        base: &str,
    ) -> (Option<Vec<String>>, bool) {
        let mut matches = Vec::new();
        let mut matches_case_sensitive = false;
        for (i, sub) in re.sub.iter_mut().enumerate() {
            let (Some(found), cs) = find_set_matches_internal(sub, base) else {
                return (None, false);
            };
            if matches.len() + found.len() > MAX_SET_MATCHES {
                return (None, false);
            }
            if i == 0 {
                matches_case_sensitive = cs;
            }
            if matches_case_sensitive != cs {
                return (None, false);
            }
            matches.extend(found);
        }
        (Some(matches), matches_case_sensitive)
    }

    pub fn is_simple_concatenation_pattern(re: &SyntaxRegexp) -> bool {
        if re.op != SyntaxOp::Concat || re.sub.len() < 3 || re.sub.len().is_multiple_of(2) {
            return false;
        }
        let mut num_literals = 0usize;
        for (i, sub) in re.sub.iter().enumerate() {
            if i % 2 == 0 {
                if !is_match_any(sub) {
                    return false;
                }
            } else {
                if !is_case_sensitive_literal(sub) {
                    return false;
                }
                num_literals += 1;
            }
        }
        num_literals > 1
    }

    pub fn string_matcher_from_regexp(re: &mut SyntaxRegexp) -> Option<StringMatcher> {
        clear_begin_end_text(re);
        let m = string_matcher_from_regexp_internal(re)?;
        Some(optimize_equal_or_prefix_string_matchers(
            m,
            MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD,
        ))
    }

    pub fn string_matcher_from_regexp_internal(re: &mut SyntaxRegexp) -> Option<StringMatcher> {
        clear_capture(re);
        match re.op {
            SyntaxOp::BeginText | SyntaxOp::EndText => None,
            SyntaxOp::Plus => {
                let s0 = re.sub.first()?;
                if s0.op != SyntaxOp::AnyChar && s0.op != SyntaxOp::AnyCharNotNL {
                    return None;
                }
                Some(StringMatcher::AnyNonEmpty {
                    match_nl: s0.op == SyntaxOp::AnyChar,
                })
            }
            SyntaxOp::Star => {
                let s0 = re.sub.first()?;
                if s0.op != SyntaxOp::AnyChar && s0.op != SyntaxOp::AnyCharNotNL {
                    return None;
                }
                if s0.op == SyntaxOp::AnyChar {
                    Some(StringMatcher::True)
                } else {
                    Some(StringMatcher::AnyWithoutNewline)
                }
            }
            SyntaxOp::Quest => {
                if re.sub.len() != 1
                    || (re.sub[0].op != SyntaxOp::AnyChar && re.sub[0].op != SyntaxOp::AnyCharNotNL)
                {
                    return None;
                }
                Some(StringMatcher::ZeroOrOneChar {
                    match_nl: re.sub[0].op == SyntaxOp::AnyChar,
                })
            }
            SyntaxOp::EmptyMatch => Some(StringMatcher::Empty),
            SyntaxOp::Literal => Some(StringMatcher::Equal {
                s: re.runes.iter().collect(),
                case_sensitive: re.is_case_sensitive(),
            }),
            SyntaxOp::Alternate => {
                let mut or = Vec::with_capacity(re.sub.len());
                for sub in &mut re.sub {
                    or.push(string_matcher_from_regexp_internal(sub)?);
                }
                Some(StringMatcher::Or(or))
            }
            SyntaxOp::Concat => {
                for s in &mut re.sub {
                    clear_capture(s);
                }
                re.sub.retain(|s| s.op != SyntaxOp::EmptyMatch);
                if re.sub.is_empty() {
                    return Some(StringMatcher::Empty);
                }
                if re.sub.len() == 1 {
                    return string_matcher_from_regexp_internal(&mut re.sub[0]);
                }
                let mut left: Option<StringMatcher> = None;
                let mut right: Option<StringMatcher> = None;
                if matches!(
                    re.sub[0].op,
                    SyntaxOp::Plus | SyntaxOp::Star | SyntaxOp::Quest
                ) {
                    left = Some(string_matcher_from_regexp_internal(&mut re.sub[0])?);
                    re.sub.remove(0);
                }
                if !re.sub.is_empty()
                    && matches!(
                        re.sub[re.sub.len() - 1].op,
                        SyntaxOp::Plus | SyntaxOp::Star | SyntaxOp::Quest
                    )
                {
                    let mut tail = re.sub.pop().unwrap();
                    right = Some(string_matcher_from_regexp_internal(&mut tail)?);
                }
                let (mut matches, mut matches_cs) = find_set_matches_internal(re, "");
                if matches.as_ref().is_none_or(|m| m.is_empty()) && re.sub.len() == 2 {
                    if right.is_none() && re.sub[0].op == SyntaxOp::Literal {
                        if let Some(rm) = string_matcher_from_regexp_internal(&mut re.sub[1]) {
                            right = Some(rm);
                            matches = Some(vec![re.sub[0].runes.iter().collect()]);
                            matches_cs = re.sub[0].is_case_sensitive();
                        }
                    } else if left.is_none() && re.sub[1].op == SyntaxOp::Literal {
                        if let Some(lm) = string_matcher_from_regexp_internal(&mut re.sub[0]) {
                            left = Some(lm);
                            matches = Some(vec![re.sub[1].runes.iter().collect()]);
                            matches_cs = re.sub[1].is_case_sensitive();
                        }
                    }
                }
                let matches = matches.filter(|m| !m.is_empty())?;
                match (left, right) {
                    (None, None) => {
                        let or = matches
                            .into_iter()
                            .map(|m| StringMatcher::Equal {
                                s: m,
                                case_sensitive: matches_cs,
                            })
                            .collect();
                        Some(StringMatcher::Or(or))
                    }
                    (None, Some(r)) if matches.len() == 1 => {
                        let p = matches.into_iter().next().unwrap();
                        if matches_cs {
                            Some(StringMatcher::LiteralPrefixSensitive {
                                prefix: p,
                                right: Box::new(r),
                            })
                        } else {
                            Some(StringMatcher::LiteralPrefixInsensitive {
                                prefix: p,
                                right: Box::new(r),
                            })
                        }
                    }
                    (Some(l), None) if matches.len() == 1 => {
                        let suf = matches.into_iter().next().unwrap();
                        Some(StringMatcher::LiteralSuffix {
                            left: Box::new(l),
                            suffix: suf,
                            suffix_case_sensitive: matches_cs,
                        })
                    }
                    (l, r) if matches_cs => Some(StringMatcher::Contains {
                        left: l.map(Box::new),
                        substrings: matches,
                        right: r.map(Box::new),
                    }),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub type PrefixMatcherCb<'a> = &'a mut dyn FnMut(&str, bool, &StringMatcher) -> bool;

    pub fn find_equal_or_prefix_string_matchers(
        input: &StringMatcher,
        equal_cb: &mut impl FnMut(&str, bool) -> bool,
        prefix_cb: Option<PrefixMatcherCb<'_>>,
    ) -> bool {
        let mut prefix_cb = prefix_cb;
        find_equal_or_prefix_inner(input, equal_cb, &mut prefix_cb)
    }

    fn find_equal_or_prefix_inner(
        input: &StringMatcher,
        equal_cb: &mut impl FnMut(&str, bool) -> bool,
        prefix_cb: &mut Option<PrefixMatcherCb<'_>>,
    ) -> bool {
        let StringMatcher::Or(or_input) = input else {
            return false;
        };
        for m in or_input {
            match m {
                StringMatcher::Or(_) => {
                    if !find_equal_or_prefix_inner(m, equal_cb, prefix_cb) {
                        return false;
                    }
                }
                StringMatcher::Equal { s, case_sensitive } => {
                    if !equal_cb(s, *case_sensitive) {
                        return false;
                    }
                }
                StringMatcher::LiteralPrefixSensitive { prefix, .. } => {
                    let Some(cb) = prefix_cb.as_deref_mut() else {
                        return false;
                    };
                    if !cb(prefix, true, m) {
                        return false;
                    }
                }
                StringMatcher::LiteralPrefixInsensitive { prefix, .. } => {
                    let Some(cb) = prefix_cb.as_deref_mut() else {
                        return false;
                    };
                    if !cb(prefix, false, m) {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }

    enum EqOrPrefixItem<'a> {
        Equal(&'a str, bool),
        Prefix(&'a str, bool, &'a StringMatcher),
    }

    fn collect_equal_or_prefix<'a>(
        input: &'a StringMatcher,
        out: &mut Vec<EqOrPrefixItem<'a>>,
    ) -> bool {
        let StringMatcher::Or(or_input) = input else {
            return false;
        };
        for m in or_input {
            match m {
                StringMatcher::Or(_) => {
                    if !collect_equal_or_prefix(m, out) {
                        return false;
                    }
                }
                StringMatcher::Equal { s, case_sensitive } => {
                    out.push(EqOrPrefixItem::Equal(s, *case_sensitive));
                }
                StringMatcher::LiteralPrefixSensitive { prefix, .. } => {
                    out.push(EqOrPrefixItem::Prefix(prefix, true, m));
                }
                StringMatcher::LiteralPrefixInsensitive { prefix, .. } => {
                    out.push(EqOrPrefixItem::Prefix(prefix, false, m));
                }
                _ => return false,
            }
        }
        true
    }

    pub fn optimize_equal_or_prefix_string_matchers(
        input: StringMatcher,
        threshold: usize,
    ) -> StringMatcher {
        let mut items = Vec::new();
        if !collect_equal_or_prefix(&input, &mut items) {
            return input;
        }

        let mut case_sensitive = false;
        let mut case_sensitive_set = false;
        let mut num_values = 0usize;
        let mut num_prefixes = 0usize;
        let mut min_prefix_len = 0usize;

        for item in &items {
            match *item {
                EqOrPrefixItem::Equal(_, cs) => {
                    if case_sensitive_set && case_sensitive != cs {
                        return input;
                    }
                    if !case_sensitive_set {
                        case_sensitive = cs;
                        case_sensitive_set = true;
                    }
                    num_values += 1;
                }
                EqOrPrefixItem::Prefix(prefix, pcs, _) => {
                    if case_sensitive_set && case_sensitive != pcs {
                        return input;
                    }
                    if !case_sensitive_set {
                        case_sensitive = pcs;
                        case_sensitive_set = true;
                    }
                    if num_prefixes == 0 || prefix.len() < min_prefix_len {
                        min_prefix_len = prefix.len();
                    }
                    num_prefixes += 1;
                }
            }
        }

        if (num_values + num_prefixes) < threshold {
            return input;
        }

        let mut multi = new_equal_multi_string_matcher(
            case_sensitive,
            num_values,
            num_prefixes,
            min_prefix_len,
        );
        for item in items {
            match item {
                EqOrPrefixItem::Equal(s, _) => multi.add(s),
                EqOrPrefixItem::Prefix(prefix, _, m) => {
                    multi.add_prefix(prefix, case_sensitive, m.clone());
                }
            }
        }
        multi
    }

    #[derive(Clone, Debug)]
    pub struct FastRegexMatcher {
        pub re_string: String,
        re: Option<Regex>,
        pub set_matches: Option<Vec<String>>,
        pub string_matcher: Option<StringMatcher>,
        pub prefix: String,
        pub suffix: String,
        pub contains: Vec<String>,
        pub case_insensitive_prefix: bool,
        pub parsed_re: Option<SyntaxRegexp>,
    }

    impl FastRegexMatcher {
        pub fn new(v: &str) -> Result<Self, MatcherError> {
            let cache = global_cache();
            if let Some(hit) = cache.lock().get(v) {
                return Ok(hit);
            }
            let matcher = Self::new_without_cache(v)?;
            let cost = matcher.estimated_size_bytes();
            cache.lock().set(v.to_string(), matcher.clone(), cost);
            Ok(matcher)
        }

        pub fn new_without_cache(v: &str) -> Result<Self, MatcherError> {
            if let Some((sm, set_matches)) = optimize_alternating_literals(v) {
                return Ok(Self {
                    re_string: v.to_string(),
                    re: None,
                    set_matches,
                    string_matcher: Some(sm),
                    prefix: String::new(),
                    suffix: String::new(),
                    contains: Vec::new(),
                    case_insensitive_prefix: false,
                    parsed_re: None,
                });
            }

            let mut parsed = parse_go_syntax(v)?;
            parsed = optimize_alternating_simple_contains(parsed);

            let translated = go_regexp_syntax::translate_to_rust(v)?;
            let anchored = format!(r"\A(?s:{})\z", translated.pattern);
            let re = RegexBuilder::new(&anchored)
                .nest_limit(1000)
                .size_limit(128 << 20)
                .build()
                .map_err(|e| MatcherError::InvalidRegex {
                    pattern: v.to_string(),
                    reason: e.to_string(),
                })?;

            clear_capture(&mut parsed);

            let (case_insensitive_prefix, prefix, suffix, contains) =
                if parsed.op == SyntaxOp::Concat {
                    optimize_concat_regex(&parsed)
                } else {
                    (false, String::new(), String::new(), Vec::new())
                };

            let mut set_matches = None;
            let mut string_matcher = None;
            let (found_set, cs) = find_set_matches(&mut parsed);
            if let Some(matches) = found_set {
                if !matches.is_empty() {
                    if cs {
                        set_matches = Some(matches.clone());
                    }
                    if matches.len() > 1 {
                        let mut emsm = new_equal_multi_string_matcher(cs, matches.len(), 0, 0);
                        for m in &matches {
                            emsm.add(m);
                        }
                        string_matcher = Some(emsm);
                    }
                }
            }

            if string_matcher.is_none() {
                if is_simple_concatenation_pattern(&parsed) {
                    string_matcher = Some(StringMatcher::True);
                } else {
                    string_matcher = string_matcher_from_regexp(&mut parsed);
                }
            }

            Ok(Self {
                re_string: v.to_string(),
                re: Some(re),
                set_matches,
                string_matcher,
                prefix,
                suffix,
                contains,
                case_insensitive_prefix,
                parsed_re: Some(parsed),
            })
        }

        pub fn is_match(&self, s: &str) -> bool {
            if let Some(ref set) = self.set_matches {
                if set.len() == 1 {
                    return s == set[0];
                }
            }
            if self.prefix.is_empty()
                && self.suffix.is_empty()
                && self.contains.is_empty()
            {
                if let Some(ref sm) = self.string_matcher {
                    return sm.matches(s);
                }
            }
            if self.case_insensitive_prefix && !self.prefix.is_empty() {
                if !has_prefix_case_insensitive(s, &self.prefix) {
                    return false;
                }
                return self.re.as_ref().is_some_and(|r| r.is_match(s));
            }
            if !self.prefix.is_empty() && !s.starts_with(&self.prefix) {
                return false;
            }
            if !self.suffix.is_empty() && !s.ends_with(&self.suffix) {
                return false;
            }
            if !self.contains.is_empty() {
                let refs: Vec<&str> = self.contains.iter().map(String::as_str).collect();
                if !contains_in_order(s, &refs) {
                    return false;
                }
            }
            if let Some(ref sm) = self.string_matcher {
                return sm.matches(s);
            }
            self.re.as_ref().is_some_and(|r| r.is_match(s))
        }

        #[inline]
        pub fn set_matches(&self) -> Option<Vec<String>> {
            self.set_matches.clone()
        }

        #[inline]
        pub fn get_regex_string(&self) -> &str {
            &self.re_string
        }

        #[inline]
        pub fn is_optimized(&self) -> bool {
            self.set_matches.as_ref().is_some_and(|s| !s.is_empty())
                || self.string_matcher.is_some()
                || !self.prefix.is_empty()
                || !self.suffix.is_empty()
                || !self.contains.is_empty()
        }

        pub fn estimated_size_bytes(&self) -> usize {
            let mut bytes = std::mem::size_of::<Self>()
                + self.re_string.capacity()
                + self.prefix.capacity()
                + self.suffix.capacity()
                + self.contains.iter().map(|s| s.capacity() + 24).sum::<usize>()
                + self
                    .set_matches
                    .as_ref()
                    .map_or(0, |v| v.iter().map(|s| s.capacity() + 24).sum::<usize>());
            if let Some(ref sm) = self.string_matcher {
                bytes += estimate_sm_size(sm);
            }
            if let Some(ref pr) = self.parsed_re {
                bytes += estimate_syntax_size(pr);
            }
            bytes
        }
    }

    fn estimate_sm_size(sm: &StringMatcher) -> usize {
        match sm {
            StringMatcher::Equal { s, .. } => 32 + s.capacity(),
            StringMatcher::Or(v) => 24 + v.iter().map(estimate_sm_size).sum::<usize>(),
            StringMatcher::EqualMultiSlice { values, .. } => {
                32 + values.iter().map(|s| s.capacity() + 24).sum::<usize>()
            }
            StringMatcher::EqualMultiMap {
                values, prefixes, ..
            } => {
                64 + values.iter().map(|s| s.capacity() + 32).sum::<usize>()
                    + prefixes
                        .values()
                        .map(|v| v.iter().map(estimate_sm_size).sum::<usize>())
                        .sum::<usize>()
            }
            StringMatcher::LiteralPrefixSensitive { prefix, right }
            | StringMatcher::LiteralPrefixInsensitive { prefix, right } => {
                32 + prefix.capacity() + estimate_sm_size(right)
            }
            StringMatcher::LiteralSuffix { left, suffix, .. } => {
                32 + suffix.capacity() + estimate_sm_size(left)
            }
            StringMatcher::Contains {
                left,
                substrings,
                right,
            } => {
                32 + substrings.iter().map(|s| s.capacity() + 24).sum::<usize>()
                    + left.as_deref().map_or(0, estimate_sm_size)
                    + right.as_deref().map_or(0, estimate_sm_size)
            }
            _ => 16,
        }
    }

    fn estimate_syntax_size(re: &SyntaxRegexp) -> usize {
        64 + re.runes.capacity() * 4 + re.sub.iter().map(estimate_syntax_size).sum::<usize>()
    }

    pub struct FastRegexMatcherCache {
        max_bytes: usize,
        cur_bytes: usize,
        entries: std::collections::HashMap<String, (FastRegexMatcher, usize)>,
        order: VecDeque<String>,
    }

    impl FastRegexMatcherCache {
        pub fn new(max_bytes: usize) -> Self {
            Self {
                max_bytes,
                cur_bytes: 0,
                entries: std::collections::HashMap::new(),
                order: VecDeque::new(),
            }
        }

        pub fn clear(&mut self) {
            self.cur_bytes = 0;
            self.entries.clear();
            self.order.clear();
        }

        pub fn get(&self, key: &str) -> Option<FastRegexMatcher> {
            self.entries.get(key).map(|(m, _)| m.clone())
        }

        pub fn contains(&self, key: &str) -> bool {
            self.entries.contains_key(key)
        }

        pub fn set(&mut self, key: String, val: FastRegexMatcher, cost: usize) {
            if cost > self.max_bytes {
                return;
            }
            if let Some((_, old_cost)) = self.entries.remove(&key) {
                self.cur_bytes = self.cur_bytes.saturating_sub(old_cost);
                self.order.retain(|k| k != &key);
            }
            while self.cur_bytes + cost > self.max_bytes {
                let Some(evict_key) = self.order.pop_front() else {
                    break;
                };
                if let Some((_, ec)) = self.entries.remove(&evict_key) {
                    self.cur_bytes = self.cur_bytes.saturating_sub(ec);
                }
            }
            self.cur_bytes += cost;
            self.order.push_back(key.clone());
            self.entries.insert(key, (val, cost));
        }

        pub fn set_max_bytes(&mut self, max_bytes: usize) {
            self.max_bytes = max_bytes;
        }
    }

    static FAST_REGEX_MATCHER_CACHE: OnceLock<Mutex<FastRegexMatcherCache>> = OnceLock::new();

    pub fn global_cache() -> &'static Mutex<FastRegexMatcherCache> {
        FAST_REGEX_MATCHER_CACHE
            .get_or_init(|| Mutex::new(FastRegexMatcherCache::new(FAST_REGEX_MATCHER_CACHE_MAX_SIZE_BYTES)))
    }
}

/// Ported from github.com/prometheus/prometheus/model/labels/cost.go
/// and github.com/prometheus/prometheus/model/labels/matcher.go.
pub mod cost {
    use std::fmt;

    use crate::labels::prom_regexp::{
        parse_go_syntax, FastRegexMatcher, StringMatcher, SyntaxOp, SyntaxRegexp,
    };
    use crate::labels::{write_go_quote, ValidationScheme};
    use crate::matcher::{MatchType, MatcherError};

    pub const ESTIMATED_STRING_EQUALITY_COST: f64 = 1.0;
    pub const ESTIMATED_STRING_HAS_PREFIX_COST: f64 = 1.0;
    pub const ESTIMATED_SLICE_CONTAINS_COST_PER_ELEMENT: f64 = 1.1;
    pub const ESTIMATED_MAP_CONTAINS_COST: f64 = 4.0;

    #[derive(Clone, Debug)]
    pub struct PromMatcher {
        pub match_type: MatchType,
        pub name: String,
        pub value: String,
        pub re: Option<FastRegexMatcher>,
    }

    impl PromMatcher {
        pub fn new(
            match_type: MatchType,
            name: impl Into<String>,
            value: impl Into<String>,
        ) -> Result<Self, MatcherError> {
            let name = name.into();
            let value = value.into();
            let re = match match_type {
                MatchType::RegexMatch | MatchType::RegexNotMatch => {
                    Some(FastRegexMatcher::new(&value)?)
                }
                MatchType::Equal | MatchType::NotEqual => None,
            };
            Ok(Self {
                match_type,
                name,
                value,
                re,
            })
        }

        pub fn must_new(
            match_type: MatchType,
            name: impl Into<String>,
            value: impl Into<String>,
        ) -> Self {
            Self::new(match_type, name, value).expect("invalid regex matcher")
        }

        pub fn matches(&self, s: &str) -> bool {
            match self.match_type {
                MatchType::Equal => s == self.value,
                MatchType::NotEqual => s != self.value,
                MatchType::RegexMatch => self.re.as_ref().is_some_and(|r| r.is_match(s)),
                MatchType::RegexNotMatch => !self.re.as_ref().is_some_and(|r| r.is_match(s)),
            }
        }

        pub fn inverse(&self) -> Result<Self, MatcherError> {
            let inv = match self.match_type {
                MatchType::Equal => MatchType::NotEqual,
                MatchType::NotEqual => MatchType::Equal,
                MatchType::RegexMatch => MatchType::RegexNotMatch,
                MatchType::RegexNotMatch => MatchType::RegexMatch,
            };
            Self::new(inv, self.name.clone(), self.value.clone())
        }

        pub fn get_regex_string(&self) -> &str {
            self.re.as_ref().map_or("", |r| r.get_regex_string())
        }

        pub fn set_matches(&self) -> Option<Vec<String>> {
            self.re.as_ref().and_then(|r| r.set_matches())
        }

        pub fn prefix(&self) -> &str {
            match self.re {
                Some(ref r) if !r.case_insensitive_prefix => &r.prefix,
                _ => "",
            }
        }

        pub fn has_case_insensitive_prefix(&self) -> bool {
            self.re.as_ref().is_some_and(|r| r.case_insensitive_prefix)
        }

        pub fn is_regex_optimized(&self) -> bool {
            self.re.as_ref().is_some_and(|r| r.is_optimized())
        }

        pub fn single_match_cost(&self) -> f64 {
            match self.match_type {
                MatchType::Equal | MatchType::NotEqual => ESTIMATED_STRING_EQUALITY_COST,
                MatchType::RegexMatch | MatchType::RegexNotMatch => {
                    let re = self.re.as_ref().expect("regex matcher");
                    if let Some(ref set) = re.set_matches {
                        if !set.is_empty() {
                            return ESTIMATED_SLICE_CONTAINS_COST_PER_ELEMENT * (set.len() as f64);
                        }
                    }
                    if matches!(re.string_matcher, Some(StringMatcher::EqualMultiMap { .. })) {
                        return ESTIMATED_MAP_CONTAINS_COST;
                    }
                    if matches!(re.string_matcher, Some(StringMatcher::True)) {
                        let chars: usize = re.contains.iter().map(String::len).sum();
                        return (chars as f64).max(1.0);
                    }
                    if !re.prefix.is_empty() {
                        return ESTIMATED_STRING_HAS_PREFIX_COST;
                    }
                    re.single_match_cost()
                }
            }
        }

        pub fn estimate_selectivity(&self, total_label_values: u64) -> f64 {
            if total_label_values == 0 {
                return 1.0;
            }
            let mut selectivity = match self.match_type {
                MatchType::Equal | MatchType::NotEqual => {
                    if self.value.is_empty() {
                        0.0
                    } else {
                        1.0 / (total_label_values as f64)
                    }
                }
                MatchType::RegexMatch | MatchType::RegexNotMatch => {
                    let re = self.re.as_ref().expect("regex matcher");
                    let set_size = re.set_matches.as_ref().map_or(0, Vec::len);
                    if set_size > 0 {
                        (set_size as f64) / (total_label_values as f64)
                    } else if self.value.is_empty() {
                        0.0
                    } else if !re.prefix.is_empty() {
                        0.1
                    } else if self.value == ".+" || self.value == ".*" {
                        1.0
                    } else {
                        0.1
                    }
                }
            };
            selectivity = selectivity.clamp(0.0, 1.0);
            if matches!(self.match_type, MatchType::NotEqual | MatchType::RegexNotMatch) {
                selectivity = 1.0 - selectivity;
            }
            selectivity
        }
    }

    impl fmt::Display for PromMatcher {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if !ValidationScheme::Legacy.is_valid_label_name(&self.name) {
                write_go_quote(f, &self.name)?;
            } else {
                f.write_str(&self.name)?;
            }
            write!(f, "{}", self.match_type)?;
            write_go_quote(f, &self.value)
        }
    }

    impl FastRegexMatcher {
        pub fn single_match_cost(&self) -> f64 {
            if let Some(ref parsed) = self.parsed_re {
                return ESTIMATED_STRING_EQUALITY_COST.max(cost_estimate(parsed));
            }
            match parse_go_syntax(&self.re_string) {
                Ok(parsed) => ESTIMATED_STRING_EQUALITY_COST.max(cost_estimate(&parsed)),
                Err(_) => ESTIMATED_STRING_EQUALITY_COST,
            }
        }
    }

    pub fn cost_estimate(re: &SyntaxRegexp) -> f64 {
        match re.op {
            SyntaxOp::Literal => re.runes.len() as f64,
            SyntaxOp::Star => 10.0,
            SyntaxOp::Alternate => 1.0 + re.sub.iter().map(cost_estimate).sum::<f64>(),
            SyntaxOp::Capture => re.sub.first().map_or(1.0, cost_estimate),
            SyntaxOp::Concat => re.sub.iter().map(cost_estimate).sum(),
            SyntaxOp::CharClass => re.runes.len() as f64,
            _ => 1.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_roundtrip_and_lookup() {
        let ls = Labels::from_pairs([("job", "api"), ("__name__", "up"), ("instance", "a:1")]);
        assert_eq!(ls.len(), 3);
        assert_eq!(ls.get("__name__"), "up");
        assert_eq!(ls.get("job"), "api");
        assert_eq!(ls.get("missing"), "");
        assert!(ls.has("job"));
        assert!(!ls.has("missing"));
        assert!(!ls.has(""));

        let pairs: Vec<_> = ls.iter().collect();
        assert_eq!(
            pairs,
            vec![("__name__", "up"), ("instance", "a:1"), ("job", "api")]
        );
    }

    #[test]
    fn symbol_labels_roundtrip_and_hash_parity() {
        let mut st = SymbolTable::new();
        let ls = Labels::from_pairs([("job", "api"), ("__name__", "http_requests_total")]);
        let sl = SymbolLabels::from_labels(&mut st, &ls);
        assert_eq!(sl.prom_hash(&st), ls.hash());
        assert_eq!(sl.to_labels(&st), ls);
        assert_eq!(sl.get(&st, "job"), "api");
        assert_eq!(sl.get(&st, "absent"), "");
    }

    #[test]
    fn long_label_size_encoding() {
        let long_val = "x".repeat(300);
        let ls = Labels::from_pairs([("big", long_val.as_str())]);
        assert_eq!(ls.get("big"), long_val);
        assert_eq!(ls.len(), 1);
    }

    #[test]
    fn labels_clone_and_iter_bytes_slices_zero_copy() {
        let ls = Labels::from_pairs([("__name__", "up"), ("job", "api"), ("instance", "a:1")]);
        let cloned = ls.clone();
        assert_eq!(ls.as_bytes().as_ptr(), cloned.as_bytes().as_ptr());

        let base_start = ls.as_bytes().as_ptr() as usize;
        let base_end = base_start + ls.as_bytes().len();
        let pairs: Vec<(Bytes, Bytes)> = ls.iter_bytes_slices().collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0.as_ref(), b"__name__");
        assert_eq!(pairs[0].1.as_ref(), b"up");
        assert_eq!(pairs[1].0.as_ref(), b"instance");
        assert_eq!(pairs[1].1.as_ref(), b"a:1");
        assert_eq!(pairs[2].0.as_ref(), b"job");
        assert_eq!(pairs[2].1.as_ref(), b"api");
        for (k, v) in &pairs {
            let kp = k.as_ptr() as usize;
            let vp = v.as_ptr() as usize;
            assert!(kp >= base_start && kp + k.len() <= base_end);
            assert!(vp >= base_start && vp + v.len() <= base_end);
        }
    }

    #[test]
    fn append_stringlabel_pair_and_iter_parity() {
        let long_val = "v".repeat(300);
        let pairs = [
            ("__name__", "http_requests_total"),
            ("instance", "10.0.0.1:9090"),
            ("job", "api"),
            ("long", long_val.as_str()),
        ];
        let mut buf = Vec::new();
        for &(k, v) in &pairs {
            append_stringlabel_pair(&mut buf, k.as_bytes(), v.as_bytes());
        }
        let expected = Labels::from_sorted_slice(&pairs);
        assert_eq!(buf.as_slice(), expected.as_bytes());
        assert_eq!(xxh64(&buf, 0), expected.hash());
        let decoded: Vec<(&str, &str)> = iter_stringlabels(&buf).collect();
        assert_eq!(decoded, pairs);
    }

    #[test]
    fn from_raw_stringlabels_bytes_validates_utf8_and_framing() {
        // Invalid UTF-8 inside value
        let bad_utf8 = Bytes::from_static(&[1, b'k', 2, 0xff, 0xfe]);
        assert!(Labels::try_from_raw_stringlabels_bytes(bad_utf8.clone()).is_err());
        assert!(std::panic::catch_unwind(|| Labels::from_raw_stringlabels_bytes(bad_utf8)).is_err());

        // Truncated length
        let trunc = Bytes::from_static(&[5, b'a', b'b']);
        assert!(Labels::try_from_raw_stringlabels_bytes(trunc).is_err());
    }

    #[test]
    fn iter_stringlabels_safe_on_malformed_and_non_utf8_bytes() {
        // Non-UTF-8 bytes in name/value must return None without UB or panic.
        assert_eq!(iter_stringlabels(&[1, 0xff, 1, 0xff]).next(), None);
        assert_eq!(iter_stringlabels(&[1, b'k', 1, 0xff]).next(), None);

        // Truncated 1-byte and 4-byte (255-prefix) lengths must return None without panic.
        assert_eq!(iter_stringlabels(&[5, b'a']).next(), None);
        assert_eq!(iter_stringlabels(&[1, b'k', 5, b'v']).next(), None);
        assert_eq!(iter_stringlabels(&[255, 1, 0]).next(), None);
        assert_eq!(iter_stringlabels(&[255, 0xff, 0xff, 0xff]).next(), None);
        assert_eq!(iter_stringlabels(&[1, b'k', 255, 1, 0]).next(), None);

        // Valid pair followed by malformed pair yields only the valid pair.
        let got: Vec<(&str, &str)> = iter_stringlabels(&[1, b'a', 1, b'b', 5, b'x']).collect();
        assert_eq!(got, vec![("a", "b")]);
    }
}
