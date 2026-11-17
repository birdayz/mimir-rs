//! Prometheus label sets, interned `SymbolLabels`, and bit-exact `stringlabels` hashing.
//!
//! Ported from github.com/prometheus/prometheus/model/labels/labels_stringlabels.go
//! and github.com/prometheus/prometheus/model/labels/labels_common.go.

use std::cmp::Ordering;
use std::fmt;

use bytes::Bytes;
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
/// - `255 <= len <= 2^24`: `[255, le0, le1, le2]`
#[inline]
pub fn encode_stringlabel_size(buf: &mut Vec<u8>, len: usize) {
    if len < 255 {
        buf.push(len as u8);
    } else {
        assert!(len <= (1 << 24), "string too long to encode as label");
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

/// Iterates over `(name, value)` pairs inside a raw Prometheus `stringlabels` byte slice.
///
/// Caller must ensure `data` was produced from valid UTF-8 `stringlabels` pairs.
#[inline]
pub fn iter_stringlabels(data: &[u8]) -> LabelsIter<'_> {
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

#[inline]
fn decode_stringlabel_size(data: &[u8], pos: usize) -> (usize, usize) {
    let b = data[pos];
    if b == 255 {
        let len = (data[pos + 1] as usize)
            | ((data[pos + 2] as usize) << 8)
            | ((data[pos + 3] as usize) << 16);
        (len, pos + 4)
    } else {
        (b as usize, pos + 1)
    }
}

#[inline]
fn decode_stringlabel_str(data: &[u8], pos: usize) -> (&str, usize) {
    let (len, start) = decode_stringlabel_size(data, pos);
    let end = start + len;
    // SAFETY: `Labels` only ever encodes valid UTF-8 strings.
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
                        let (val, _) = decode_stringlabel_str(&self.data, nstart + nlen);
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
        LabelsIter {
            data: &self.data,
            pos: 0,
        }
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

impl fmt::Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("{")?;
        for (i, (k, v)) in self.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{k}={v:?}")?;
        }
        f.write_str("}")
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

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }
        let (name, p1) = decode_stringlabel_str(self.data, self.pos);
        let (value, p2) = decode_stringlabel_str(self.data, p1);
        self.pos = p2;
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
}
