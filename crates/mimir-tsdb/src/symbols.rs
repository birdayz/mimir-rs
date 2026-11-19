//! Dense arena-backed string symbol interner.
//!
//! Ported from github.com/prometheus/prometheus/model/labels/labels_dedupelabels.go
//! and github.com/prometheus/prometheus/tsdb/addsymbol.go.

use hashbrown::HashTable;
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

/// Errors returned by [`SymbolTable::try_intern`] when capacity limits are exceeded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum SymbolTableError {
    #[error("symbol table byte arena exceeded 4 GiB")]
    ArenaFull,
    #[error("single symbol exceeded 4 GiB")]
    SymbolTooLarge,
    #[error("symbol table exceeded u32::MAX entries")]
    TooManySymbols,
}

/// Compact 32-bit identifier for an interned string in a [`SymbolTable`].
///
/// `SymbolId(0)` is reserved for the empty string `""`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SymbolId(pub u32);

impl SymbolId {
    /// The canonical ID for the empty string `""`.
    pub const EMPTY: Self = Self(0);

    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

#[derive(Clone, Copy, Debug)]
struct SymbolEntry {
    offset: u32,
    len: u32,
}

/// Dense arena-backed symbol table mapping UTF-8 strings to [`SymbolId`] (`u32`).
///
/// Uses a contiguous byte arena for UTF-8 data and a `hashbrown::HashTable` for
/// zero-clone lookups on hit.
#[derive(Debug)]
pub struct SymbolTable {
    data: Vec<u8>,
    entries: Vec<SymbolEntry>,
    table: HashTable<u32>,
    #[cfg(test)]
    fail_next_intern: bool,
}

impl Default for SymbolTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SymbolTable {
    /// Creates a new `SymbolTable` pre-seeded with `SymbolId::EMPTY` (`""`) at index 0.
    pub fn new() -> Self {
        Self::with_capacity(64, 1024)
    }

    /// Creates a new `SymbolTable` with pre-allocated symbol and byte capacities.
    pub fn with_capacity(symbol_cap: usize, bytes_cap: usize) -> Self {
        let mut entries = Vec::with_capacity(symbol_cap.max(1));
        entries.push(SymbolEntry { offset: 0, len: 0 });
        Self {
            data: Vec::with_capacity(bytes_cap),
            entries,
            table: HashTable::with_capacity(symbol_cap),
            #[cfg(test)]
            fail_next_intern: false,
        }
    }

    #[inline]
    fn slice_bytes<'a>(data: &'a [u8], entries: &[SymbolEntry], id: u32) -> &'a [u8] {
        let e = entries[id as usize];
        let start = e.offset as usize;
        &data[start..start + e.len as usize]
    }

    /// Interns `s`, returning its existing [`SymbolId`] or inserting it into the arena.
    ///
    /// Returns [`SymbolId::EMPTY`] when `s.is_empty()`. Returns an error if the
    /// 4 GiB arena or `u32::MAX` symbol limit would be exceeded.
    #[cfg(test)]
    pub(crate) fn fail_next_intern_for_test(&mut self) {
        self.fail_next_intern = true;
    }

    pub fn try_intern(&mut self, s: &str) -> Result<SymbolId, SymbolTableError> {
        if s.is_empty() {
            return Ok(SymbolId::EMPTY);
        }
        #[cfg(test)]
        if self.fail_next_intern {
            self.fail_next_intern = false;
            return Err(SymbolTableError::TooManySymbols);
        }
        let bytes = s.as_bytes();
        let hash = xxh3_64(bytes);

        let data = &self.data;
        let entries = &self.entries;
        match self.table.entry(
            hash,
            |&id| Self::slice_bytes(data, entries, id) == bytes,
            |&id| xxh3_64(Self::slice_bytes(data, entries, id)),
        ) {
            hashbrown::hash_table::Entry::Occupied(entry) => Ok(SymbolId(*entry.get())),
            hashbrown::hash_table::Entry::Vacant(entry) => {
                let offset = u32::try_from(self.data.len())
                    .map_err(|_| SymbolTableError::ArenaFull)?;
                let len = u32::try_from(bytes.len())
                    .map_err(|_| SymbolTableError::SymbolTooLarge)?;
                let id = u32::try_from(self.entries.len())
                    .map_err(|_| SymbolTableError::TooManySymbols)?;

                self.data.extend_from_slice(bytes);
                self.entries.push(SymbolEntry { offset, len });
                entry.insert(id);
                Ok(SymbolId(id))
            }
        }
    }

    /// Interns `s`, panicking if the 4 GiB arena or `u32::MAX` entry limit is exceeded.
    pub fn intern(&mut self, s: &str) -> SymbolId {
        self.try_intern(s).expect("symbol table limit exceeded")
    }

    /// Looks up `s` without inserting it.
    #[inline]
    pub fn get(&self, s: &str) -> Option<SymbolId> {
        if s.is_empty() {
            return Some(SymbolId::EMPTY);
        }
        let bytes = s.as_bytes();
        let hash = xxh3_64(bytes);
        self.table
            .find(hash, |&id| Self::slice_bytes(&self.data, &self.entries, id) == bytes)
            .map(|&id| SymbolId(id))
    }

    /// Resolves `id` to its string slice.
    ///
    /// Panics if `id` does not exist in this `SymbolTable`.
    #[inline]
    pub fn resolve(&self, id: SymbolId) -> &str {
        self.try_resolve(id).expect("invalid SymbolId")
    }

    /// Resolves `id` to its string slice, returning `None` if `id` is out of bounds.
    #[inline]
    pub fn try_resolve(&self, id: SymbolId) -> Option<&str> {
        let e = self.entries.get(id.0 as usize)?;
        let start = e.offset as usize;
        let bytes = &self.data[start..start + e.len as usize];
        // SAFETY: only valid &str slices are ever pushed into `self.data`.
        Some(unsafe { std::str::from_utf8_unchecked(bytes) })
    }

    /// Number of symbols in the table (including `""` at index 0).
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if no non-empty symbols have been interned yet.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.len() <= 1
    }

    /// Total UTF-8 bytes stored in the arena.
    #[inline]
    pub fn bytes_len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_symbol_is_zero() {
        let mut st = SymbolTable::new();
        assert_eq!(st.intern(""), SymbolId::EMPTY);
        assert_eq!(st.get(""), Some(SymbolId::EMPTY));
        assert_eq!(st.resolve(SymbolId::EMPTY), "");
        assert_eq!(st.len(), 1);
        assert!(st.is_empty());
    }

    #[test]
    fn intern_and_resolve_roundtrip() {
        let mut st = SymbolTable::new();
        let a = st.intern("__name__");
        let b = st.intern("http_requests_total");
        let c = st.intern("job");
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_eq!(st.intern("__name__"), a);
        assert_eq!(st.intern("http_requests_total"), b);
        assert_eq!(st.get("job"), Some(c));
        assert_eq!(st.get("missing"), None);
        assert_eq!(st.resolve(a), "__name__");
        assert_eq!(st.resolve(b), "http_requests_total");
        assert_eq!(st.resolve(c), "job");
        assert_eq!(st.len(), 4);
        assert!(!st.is_empty());
        assert_eq!(st.try_intern("job"), Ok(c));
    }
}
