//! In-memory inverted postings index and Prometheus matcher evaluation.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/index/postings.go
//! and github.com/prometheus/prometheus/tsdb/querier.go.

use std::cmp::Ordering;
use std::collections::HashSet;

use hashbrown::HashMap;
use parking_lot::RwLock;
use thiserror::Error;

use crate::labels::SymbolLabels;
use crate::matcher::{LabelMatcher, MatchType};
use crate::symbols::{SymbolId, SymbolTable};

/// Canonical key `(SymbolId::EMPTY, SymbolId::EMPTY)` storing all series refs in `MemPostings`.
pub const ALL_POSTINGS_KEY: (SymbolId, SymbolId) = (SymbolId::EMPTY, SymbolId::EMPTY);

#[derive(Debug, Error)]
pub enum PostingsError {
    #[error("unexpected all-postings matcher among other matchers")]
    UnexpectedAllPostings,
    #[error("matcher error: {0}")]
    Matcher(#[from] crate::matcher::MatcherError),
    #[error("index read error: {0}")]
    Index(String),
}

/// Trait implemented by both [`MemPostingsView`] (Head) and [`crate::index::IndexReader`] (Block)
/// so [`postings_for_matchers`] evaluates matchers identically across Head and Blocks.
pub trait PostingsSource {
    fn all_postings(&self) -> Result<Vec<u64>, PostingsError>;
    fn postings_for(&self, name: &str, values: &[&str]) -> Result<Vec<u64>, PostingsError>;
    fn postings_for_all_values(&self, name: &str) -> Result<Vec<u64>, PostingsError>;
    fn postings_for_label_matching(
        &self,
        name: &str,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Result<Vec<u64>, PostingsError>;
}

/// Inverted index mapping `(label_name_id, label_value_id)` -> sorted `Vec<u64>` series refs.
#[derive(Debug)]
pub struct MemPostings {
    inner: RwLock<MemPostingsInner>,
}

#[derive(Debug)]
struct MemPostingsInner {
    m: HashMap<SymbolId, HashMap<SymbolId, Vec<u64>>>,
    lvs: HashMap<SymbolId, Vec<SymbolId>>,
    ordered: bool,
}

impl Default for MemPostings {
    fn default() -> Self {
        Self::new()
    }
}

impl MemPostings {
    /// Creates an ordered `MemPostings` ready for concurrent reads and writes.
    pub fn new() -> Self {
        Self::with_ordered(true)
    }

    /// Creates an unordered `MemPostings` for fast WAL replay (call [`Self::ensure_order`] before reading).
    pub fn new_unordered() -> Self {
        Self::with_ordered(false)
    }

    fn with_ordered(ordered: bool) -> Self {
        Self {
            inner: RwLock::new(MemPostingsInner {
                m: HashMap::with_capacity(512),
                lvs: HashMap::with_capacity(512),
                ordered,
            }),
        }
    }

    /// Adds `series_ref` for all `(name_id, value_id)` pairs in `labels`, plus `ALL_POSTINGS_KEY`.
    pub fn add(&self, series_ref: u64, labels: &SymbolLabels) {
        let mut inner = self.inner.write();
        for &(name_id, value_id) in labels.as_slice() {
            inner.add_for(series_ref, name_id, value_id);
        }
        inner.add_for(series_ref, ALL_POSTINGS_KEY.0, ALL_POSTINGS_KEY.1);
    }

    /// Ensures all postings lists are sorted in ascending order (and deduplicated) and switches to ordered mode.
    pub fn ensure_order(&self) {
        let mut inner = self.inner.write();
        if inner.ordered {
            return;
        }
        for values_map in inner.m.values_mut() {
            for list in values_map.values_mut() {
                list.sort_unstable();
                list.dedup();
            }
        }
        inner.ordered = true;
    }

    /// Removes all `deleted` series refs from `affected` label pairs and `ALL_POSTINGS_KEY`.
    pub fn delete(&self, deleted: &HashSet<u64>, affected: &[(SymbolId, SymbolId)]) {
        if deleted.is_empty() {
            return;
        }
        let mut inner = self.inner.write();
        let mut affected_names: HashSet<SymbolId> = HashSet::new();

        for &(name_id, value_id) in affected {
            if inner.delete_pair(deleted, name_id, value_id) {
                affected_names.insert(name_id);
            }
        }
        inner.delete_pair(deleted, ALL_POSTINGS_KEY.0, ALL_POSTINGS_KEY.1);

        for name_id in affected_names {
            let empty_name = inner.m.get(&name_id).is_none_or(|vm| vm.is_empty());
            if empty_name {
                inner.m.remove(&name_id);
                inner.lvs.remove(&name_id);
            } else if let Some(vm) = inner.m.get(&name_id) {
                let lvs: Vec<SymbolId> = vm.keys().copied().collect();
                inner.lvs.insert(name_id, lvs);
            }
        }
    }

    /// Returns a sorted list of all series refs in the index.
    pub fn all_postings(&self) -> Vec<u64> {
        let inner = self.inner.read();
        inner
            .m
            .get(&ALL_POSTINGS_KEY.0)
            .and_then(|vm| vm.get(&ALL_POSTINGS_KEY.1))
            .cloned()
            .unwrap_or_default()
    }

    /// Returns sorted postings for `(name_id, value_id)`.
    pub fn postings_for_id(&self, name_id: SymbolId, value_id: SymbolId) -> Vec<u64> {
        let inner = self.inner.read();
        inner
            .m
            .get(&name_id)
            .and_then(|vm| vm.get(&value_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Returns sorted union of postings for `name_id` across `value_ids`.
    pub fn postings_for_ids(&self, name_id: SymbolId, value_ids: &[SymbolId]) -> Vec<u64> {
        if value_ids.is_empty() {
            return Vec::new();
        }
        let inner = self.inner.read();
        let Some(vm) = inner.m.get(&name_id) else {
            return Vec::new();
        };
        let lists: Vec<&[u64]> = value_ids
            .iter()
            .filter_map(|vid| vm.get(vid).map(|v| v.as_slice()))
            .filter(|s| !s.is_empty())
            .collect();
        merge_postings(&lists)
    }

    /// Returns sorted union of all postings for `name_id`.
    pub fn postings_for_all_values_id(&self, name_id: SymbolId) -> Vec<u64> {
        let inner = self.inner.read();
        let Some(vm) = inner.m.get(&name_id) else {
            return Vec::new();
        };
        let lists: Vec<&[u64]> = vm
            .values()
            .map(|v| v.as_slice())
            .filter(|s| !s.is_empty())
            .collect();
        merge_postings(&lists)
    }

    /// Returns sorted postings for `(name_id, value)` where `pred(value_str)` is true.
    pub fn postings_for_label_matching_id(
        &self,
        name_id: SymbolId,
        symbols: &SymbolTable,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Vec<u64> {
        // Snapshot the candidate value SymbolIds while holding the postings lock briefly,
        // then evaluate `pred` without holding the postings write lock.
        let value_ids: Vec<SymbolId> = {
            let inner = self.inner.read();
            match inner.lvs.get(&name_id) {
                Some(v) => v.clone(),
                None => return Vec::new(),
            }
        };

        let matching_ids: Vec<SymbolId> = value_ids
            .into_iter()
            .filter(|&vid| pred(symbols.resolve(vid)))
            .collect();

        self.postings_for_ids(name_id, &matching_ids)
    }

    /// Returns all unique label names (excluding the internal `ALL_POSTINGS_KEY` empty name), sorted.
    pub fn label_names(&self, symbols: &SymbolTable) -> Vec<String> {
        let ids: Vec<SymbolId> = {
            let inner = self.inner.read();
            inner
                .m
                .keys()
                .copied()
                .filter(|&k| k != ALL_POSTINGS_KEY.0)
                .collect()
        };
        let mut names: Vec<String> = ids
            .into_iter()
            .map(|id| symbols.resolve(id).to_string())
            .collect();
        names.sort_unstable();
        names
    }

    /// Returns all label values for `name` (unsorted, matching `MemPostings.LabelValues`).
    pub fn label_values(&self, name: &str, symbols: &SymbolTable) -> Vec<String> {
        let Some(name_id) = symbols.get(name) else {
            return Vec::new();
        };
        let ids: Vec<SymbolId> = {
            let inner = self.inner.read();
            match inner.lvs.get(&name_id) {
                Some(v) => v.clone(),
                None => return Vec::new(),
            }
        };
        ids.into_iter()
            .map(|id| symbols.resolve(id).to_string())
            .collect()
    }

    /// Returns all label values for `name` sorted lexicographically.
    pub fn sorted_label_values(&self, name: &str, symbols: &SymbolTable) -> Vec<String> {
        let mut vals = self.label_values(name, symbols);
        vals.sort_unstable();
        vals
    }

    /// Returns all `(name, value)` pairs present in postings sorted lexicographically.
    pub fn sorted_keys(&self, symbols: &SymbolTable) -> Vec<(String, String)> {
        let pairs: Vec<(SymbolId, SymbolId)> = {
            let inner = self.inner.read();
            let mut out = Vec::new();
            for (&nid, vm) in &inner.m {
                for &vid in vm.keys() {
                    out.push((nid, vid));
                }
            }
            out
        };
        let mut resolved: Vec<(String, String)> = pairs
            .into_iter()
            .map(|(nid, vid)| (symbols.resolve(nid).to_string(), symbols.resolve(vid).to_string()))
            .collect();
        resolved.sort_unstable();
        resolved
    }

    /// Returns a view implementing [`PostingsSource`] paired with `symbols`.
    pub fn as_source<'a>(&'a self, symbols: &'a SymbolTable) -> MemPostingsView<'a> {
        MemPostingsView {
            postings: self,
            symbols,
        }
    }
}

impl MemPostingsInner {
    fn add_for(&mut self, id: u64, name_id: SymbolId, value_id: SymbolId) {
        let vm = self.m.entry(name_id).or_default();
        let list = match vm.get_mut(&value_id) {
            Some(list) => list,
            None => {
                self.lvs.entry(name_id).or_default().push(value_id);
                vm.entry(value_id).or_default()
            }
        };
        list.push(id);
        if self.ordered && list.len() >= 2 {
            // Repair tail if out-of-order ID was inserted; usually O(1) break immediately.
            let mut i = list.len() - 1;
            while i >= 1 && list[i] < list[i - 1] {
                list.swap(i, i - 1);
                i -= 1;
            }
        }
    }

    /// Returns `true` if the value bucket became empty and was removed.
    fn delete_pair(&mut self, deleted: &HashSet<u64>, name_id: SymbolId, value_id: SymbolId) -> bool {
        let Some(vm) = self.m.get_mut(&name_id) else {
            return false;
        };
        let Some(list) = vm.get_mut(&value_id) else {
            return false;
        };
        list.retain(|id| !deleted.contains(id));
        if list.is_empty() {
            vm.remove(&value_id);
            return true;
        }
        false
    }
}

/// Borrowed view pairing [`MemPostings`] with a [`SymbolTable`] to implement [`PostingsSource`].
pub struct MemPostingsView<'a> {
    pub postings: &'a MemPostings,
    pub symbols: &'a SymbolTable,
}

impl PostingsSource for MemPostingsView<'_> {
    fn all_postings(&self) -> Result<Vec<u64>, PostingsError> {
        Ok(self.postings.all_postings())
    }

    fn postings_for(&self, name: &str, values: &[&str]) -> Result<Vec<u64>, PostingsError> {
        let Some(name_id) = self.symbols.get(name) else {
            return Ok(Vec::new());
        };
        let value_ids: Vec<SymbolId> = values
            .iter()
            .filter_map(|v| self.symbols.get(v))
            .collect();
        Ok(self.postings.postings_for_ids(name_id, &value_ids))
    }

    fn postings_for_all_values(&self, name: &str) -> Result<Vec<u64>, PostingsError> {
        let Some(name_id) = self.symbols.get(name) else {
            return Ok(Vec::new());
        };
        Ok(self.postings.postings_for_all_values_id(name_id))
    }

    fn postings_for_label_matching(
        &self,
        name: &str,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Result<Vec<u64>, PostingsError> {
        let Some(name_id) = self.symbols.get(name) else {
            return Ok(Vec::new());
        };
        Ok(self
            .postings
            .postings_for_label_matching_id(name_id, self.symbols, pred))
    }
}

/// Computes the sorted intersection of multiple sorted postings lists.
pub fn intersect_postings(mut lists: Vec<Vec<u64>>) -> Vec<u64> {
    if lists.is_empty() {
        return Vec::new();
    }
    if lists.len() == 1 {
        return lists.into_iter().next().unwrap();
    }
    if lists.iter().any(|l| l.is_empty()) {
        return Vec::new();
    }
    // Start with the smallest list to minimize comparisons and allocations.
    lists.sort_unstable_by_key(|l| l.len());
    let mut acc = lists.swap_remove(0);
    for other in &lists {
        if acc.is_empty() {
            break;
        }
        acc = intersect_two(&acc, other);
    }
    acc
}

#[inline]
fn intersect_two(a: &[u64], b: &[u64]) -> Vec<u64> {
    let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut out = Vec::with_capacity(small.len().min(64));

    // Use binary search / galloping when sizes are skewed, two-pointer otherwise.
    if large.len() > small.len().saturating_mul(8) {
        let mut lo = 0usize;
        for &x in small {
            match large[lo..].binary_search(&x) {
                Ok(idx) => {
                    out.push(x);
                    lo += idx + 1;
                }
                Err(idx) => {
                    lo += idx;
                }
            }
            if lo >= large.len() {
                break;
            }
        }
    } else {
        let (mut i, mut j) = (0usize, 0usize);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                Ordering::Less => i += 1,
                Ordering::Greater => j += 1,
                Ordering::Equal => {
                    out.push(a[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
    }
    out
}

/// Computes the sorted, deduplicated union of sorted postings slices.
pub fn merge_postings(lists: &[&[u64]]) -> Vec<u64> {
    let non_empty: Vec<&[u64]> = lists.iter().copied().filter(|s| !s.is_empty()).collect();
    if non_empty.is_empty() {
        return Vec::new();
    }
    if non_empty.len() == 1 {
        return non_empty[0].to_vec();
    }
    let total: usize = non_empty.iter().map(|s| s.len()).sum();
    let mut out = Vec::with_capacity(total);
    for s in non_empty {
        out.extend_from_slice(s);
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Computes `full \ drop` over sorted postings slices.
pub fn without_postings(full: &[u64], drop: &[u64]) -> Vec<u64> {
    if full.is_empty() {
        return Vec::new();
    }
    if drop.is_empty() {
        return full.to_vec();
    }
    let mut out = Vec::with_capacity(full.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < full.len() && j < drop.len() {
        match full[i].cmp(&drop[j]) {
            Ordering::Less => {
                out.push(full[i]);
                i += 1;
            }
            Ordering::Greater => {
                j += 1;
            }
            Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    if i < full.len() {
        out.extend_from_slice(&full[i..]);
    }
    out
}

/// Evaluates `matchers` against `source`, matching Prometheus `tsdb.PostingsForMatchers` semantics
/// including match-empty matchers (`foo=""`, `foo!="bar"`, `foo=~".*"`, `foo!~".+"`, etc.).
pub fn postings_for_matchers<S: PostingsSource + ?Sized>(
    source: &S,
    matchers: &[LabelMatcher],
) -> Result<Vec<u64>, PostingsError> {
    if matchers.is_empty() {
        return Ok(Vec::new());
    }
    if matchers.len() == 1 && matchers[0].name.is_empty() && matchers[0].value.is_empty() {
        return source.all_postings();
    }

    let mut label_must_be_set: HashSet<&str> = HashSet::with_capacity(matchers.len());
    for m in matchers {
        if !m.matches_empty() {
            label_must_be_set.insert(m.name.as_str());
        }
    }

    let is_subtracting_matcher = |m: &LabelMatcher| -> bool {
        if !label_must_be_set.contains(m.name.as_str()) {
            return true;
        }
        matches!(m.match_type, MatchType::NotEqual | MatchType::RegexNotMatch) && m.matches_empty()
    };

    let mut has_subtracting = false;
    let mut has_intersecting = false;
    for m in matchers {
        if is_subtracting_matcher(m) {
            has_subtracting = true;
        } else {
            has_intersecting = true;
        }
    }

    let mut its: Vec<Vec<u64>> = Vec::new();
    let mut not_its: Vec<Vec<u64>> = Vec::new();

    if has_subtracting && !has_intersecting {
        its.push(source.all_postings()?);
    }

    // Evaluate intersecting matchers before subtracting matchers, matching Prometheus order.
    let mut ordered: Vec<&LabelMatcher> = matchers.iter().collect();
    ordered.sort_by_key(|m| is_subtracting_matcher(m));

    for m in ordered {
        if m.name.is_empty() && m.value.is_empty() {
            return Err(PostingsError::UnexpectedAllPostings);
        }
        if m.match_type == MatchType::RegexMatch && m.value == ".*" {
            continue;
        }
        if m.match_type == MatchType::RegexNotMatch && m.value == ".*" {
            return Ok(Vec::new());
        }
        if m.match_type == MatchType::RegexMatch && m.value == ".+" {
            let p = source.postings_for_all_values(&m.name)?;
            if p.is_empty() {
                return Ok(Vec::new());
            }
            its.push(p);
            continue;
        }
        if m.match_type == MatchType::RegexNotMatch && m.value == ".+" {
            let p = source.postings_for_all_values(&m.name)?;
            if !p.is_empty() {
                not_its.push(p);
            }
            continue;
        }

        if label_must_be_set.contains(m.name.as_str()) {
            let matches_empty = m.matches_empty();
            let is_not = matches!(m.match_type, MatchType::NotEqual | MatchType::RegexNotMatch);
            if is_not && matches_empty {
                let inv = m.inverse()?;
                let p = postings_for_single_matcher(source, &inv)?;
                if !p.is_empty() {
                    not_its.push(p);
                }
            } else if is_not && !matches_empty {
                let inv = m.inverse()?;
                let p = inverse_postings_for_single_matcher(source, &inv)?;
                if p.is_empty() {
                    return Ok(Vec::new());
                }
                its.push(p);
            } else {
                let p = postings_for_single_matcher(source, m)?;
                if p.is_empty() {
                    return Ok(Vec::new());
                }
                its.push(p);
            }
        } else {
            let p = inverse_postings_for_single_matcher(source, m)?;
            if !p.is_empty() {
                not_its.push(p);
            }
        }
    }

    let mut result = intersect_postings(its);
    for drop in not_its {
        if result.is_empty() {
            break;
        }
        result = without_postings(&result, &drop);
    }
    Ok(result)
}

fn postings_for_single_matcher<S: PostingsSource + ?Sized>(
    source: &S,
    m: &LabelMatcher,
) -> Result<Vec<u64>, PostingsError> {
    if m.match_type == MatchType::Equal {
        return source.postings_for(&m.name, &[m.value.as_str()]);
    }
    if m.match_type == MatchType::RegexMatch {
        if let Some(set) = m.set_matches() {
            let refs: Vec<&str> = set.iter().map(String::as_str).collect();
            return source.postings_for(&m.name, &refs);
        }
    }
    source.postings_for_label_matching(&m.name, &mut |v| m.matches(v))
}

fn inverse_postings_for_single_matcher<S: PostingsSource + ?Sized>(
    source: &S,
    m: &LabelMatcher,
) -> Result<Vec<u64>, PostingsError> {
    if m.match_type == MatchType::RegexNotMatch {
        if let Some(set) = m.set_matches() {
            let refs: Vec<&str> = set.iter().map(String::as_str).collect();
            return source.postings_for(&m.name, &refs);
        }
    }
    if m.match_type == MatchType::NotEqual {
        return source.postings_for(&m.name, &[m.value.as_str()]);
    }
    if m.value.is_empty() && matches!(m.match_type, MatchType::RegexMatch | MatchType::Equal) {
        return source.postings_for_all_values(&m.name);
    }
    source.postings_for_label_matching(&m.name, &mut |v| !m.matches(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_postings_and_matchers_semantics() {
        let mut syms = SymbolTable::new();
        let p = MemPostings::new();

        let s1 = SymbolLabels::from_pairs(&mut syms, [("__name__", "up"), ("job", "api")]);
        let s2 = SymbolLabels::from_pairs(
            &mut syms,
            [("__name__", "up"), ("job", "db"), ("env", "prod")],
        );
        let s3 = SymbolLabels::from_pairs(
            &mut syms,
            [("__name__", "down"), ("job", "api"), ("env", "dev")],
        );

        p.add(1, &s1);
        p.add(2, &s2);
        p.add(3, &s3);

        let src = p.as_source(&syms);

        // Positive equal
        let m_up = LabelMatcher::new(MatchType::Equal, "__name__", "up").unwrap();
        assert_eq!(
            postings_for_matchers(&src, std::slice::from_ref(&m_up)).unwrap(),
            vec![1, 2]
        );

        // Match empty: env="" should match s1 (missing env)
        let m_empty_env = LabelMatcher::new(MatchType::Equal, "env", "").unwrap();
        assert_eq!(
            postings_for_matchers(&src, std::slice::from_ref(&m_empty_env)).unwrap(),
            vec![1]
        );

        // NotEqual on missing label: env!="prod" matches s1 and s3
        let m_not_prod = LabelMatcher::new(MatchType::NotEqual, "env", "prod").unwrap();
        assert_eq!(
            postings_for_matchers(&src, std::slice::from_ref(&m_not_prod)).unwrap(),
            vec![1, 3]
        );

        // Combined: __name__="up", env!="prod" -> s1
        assert_eq!(
            postings_for_matchers(&src, &[m_up, m_not_prod]).unwrap(),
            vec![1]
        );

        // Regex set optimization: job=~"api|db" -> all 3
        let m_set = LabelMatcher::new(MatchType::RegexMatch, "job", "api|db").unwrap();
        assert_eq!(postings_for_matchers(&src, &[m_set]).unwrap(), vec![1, 2, 3]);

        // Delete s2
        let mut del = HashSet::new();
        del.insert(2);
        p.delete(&del, s2.as_slice());
        assert_eq!(p.all_postings(), vec![1, 3]);
    }
}
