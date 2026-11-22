//! Token range ownership helpers.
//!
//! Ported from github.com/grafana/dskit/ring/token_range.go

/// Describes token ranges owned by an instance as inclusive `[start, end]` pairs.
/// For example `[5, 10, 20, 30]` covers tokens `5..=10` and `20..=30`.
pub type TokenRanges = Vec<u32>;

/// Returns `true` if `key` falls within any inclusive `[start, end]` pair in `ranges`.
pub fn includes_key(ranges: &[u32], key: u32) -> bool {
    if ranges.is_empty() || key < ranges[0] || key > ranges[ranges.len() - 1] {
        return false;
    }
    match ranges.binary_search(&key) {
        Ok(_) => true,
        Err(idx) => idx % 2 == 1,
    }
}

/// Returns `true` if `a` and `b` contain identical token range boundaries.
pub fn token_ranges_equal(a: &[u32], b: &[u32]) -> bool {
    a == b
}

pub trait TokenRangesExt {
    fn includes_key(&self, key: u32) -> bool;
    fn equal(&self, other: &[u32]) -> bool;
}

impl TokenRangesExt for [u32] {
    fn includes_key(&self, key: u32) -> bool {
        includes_key(self, key)
    }

    fn equal(&self, other: &[u32]) -> bool {
        token_ranges_equal(self, other)
    }
}

impl TokenRangesExt for Vec<u32> {
    fn includes_key(&self, key: u32) -> bool {
        includes_key(self, key)
    }

    fn equal(&self, other: &[u32]) -> bool {
        token_ranges_equal(self, other)
    }
}
