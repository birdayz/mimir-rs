//! Circular in-memory exemplar storage matching Prometheus `tsdb.CircularExemplarStorage`.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/exemplar.go
//! and github.com/prometheus/prometheus/model/exemplar/exemplar.go.

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use prometheus::{Counter, Gauge, Opts, Registry};
use thiserror::Error;

use crate::labels::Labels;
use crate::matcher::LabelMatcher;

/// OpenMetrics maximum combined rune length of an exemplar's label names and values.
pub const EXEMPLAR_MAX_LABEL_SET_LENGTH: usize = 128;

const NO_EXEMPLAR: u32 = u32::MAX;

#[inline]
fn next_exemplar_cap(cur_cap: usize, max_exemplars: usize) -> usize {
    let next = if cur_cap == 0 {
        64
    } else {
        cur_cap.saturating_add((cur_cap / 2).max(16))
    };
    next.min(max_exemplars)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExemplarError {
    #[error("exemplar storage is disabled")]
    Disabled,
    #[error("label name {0:?} is not unique: invalid exemplar")]
    DuplicateLabelName(String),
    #[error("label length for exemplar exceeds maximum of {EXEMPLAR_MAX_LABEL_SET_LENGTH} UTF-8 characters")]
    LabelLengthExceeded,
    #[error("duplicate exemplar")]
    DuplicateExemplar,
    #[error("out of order exemplar")]
    OutOfOrderExemplar,
}

/// A single Prometheus exemplar (`model/exemplar/exemplar.go`).
#[derive(Clone, Debug, PartialEq)]
pub struct Exemplar {
    pub labels: Labels,
    pub value: f64,
    pub timestamp_ms: i64,
    pub has_ts: bool,
}

impl Exemplar {
    #[inline]
    pub fn new(labels: Labels, value: f64, timestamp_ms: i64) -> Self {
        Self {
            labels,
            value,
            timestamp_ms,
            has_ts: true,
        }
    }

    /// Matches Prometheus `exemplar.Exemplar.Equals`.
    pub fn prom_equals(&self, other: &Self) -> bool {
        if self.labels != other.labels {
            return false;
        }
        if (self.has_ts || other.has_ts) && self.timestamp_ms != other.timestamp_ms {
            return false;
        }
        self.value == other.value
    }
}

/// Exemplars returned for a single series (`exemplar.QueryResult`).
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarQueryResult {
    pub series_labels: Labels,
    pub exemplars: Vec<Exemplar>,
}

#[derive(Clone, Debug)]
struct SeriesIndexEntry {
    oldest: u32,
    newest: u32,
}

#[derive(Clone, Debug)]
struct CircularBufferEntry {
    exemplar: Option<Exemplar>,
    series_labels: Option<Labels>,
    next: u32,
    prev: u32,
}

impl Default for CircularBufferEntry {
    fn default() -> Self {
        Self {
            exemplar: None,
            series_labels: None,
            next: NO_EXEMPLAR,
            prev: NO_EXEMPLAR,
        }
    }
}

#[derive(Clone, Debug)]
struct ExemplarMetrics {
    exemplars_appended: Counter,
    exemplars_in_storage: Gauge,
    series_with_exemplars_in_storage: Gauge,
    last_exemplars_ts: Gauge,
    out_of_order_exemplars: Counter,
    max_exemplars: Gauge,
}

impl ExemplarMetrics {
    fn new(reg: Option<&Registry>) -> Self {
        let exemplars_appended = Counter::with_opts(Opts::new(
            "prometheus_tsdb_exemplar_exemplars_appended_total",
            "Total number of appended exemplars.",
        ))
        .expect("valid counter");
        let exemplars_in_storage = Gauge::with_opts(Opts::new(
            "prometheus_tsdb_exemplar_exemplars_in_storage",
            "Number of exemplars currently in circular storage.",
        ))
        .expect("valid gauge");
        let series_with_exemplars_in_storage = Gauge::with_opts(Opts::new(
            "prometheus_tsdb_exemplar_series_with_exemplars_in_storage",
            "Number of series with exemplars currently in circular storage.",
        ))
        .expect("valid gauge");
        let last_exemplars_ts = Gauge::with_opts(Opts::new(
            "prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds",
            // Verbatim from Go (`exemplar.go`), string concatenation typos included.
            "The timestamp of the oldest exemplar stored in circular storage. Useful to check for what timerange the current exemplar buffer limit allows. This usually means the last timestampfor all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.",
        ))
        .expect("valid gauge");
        let out_of_order_exemplars = Counter::with_opts(Opts::new(
            "prometheus_tsdb_exemplar_out_of_order_exemplars_total",
            "Total number of out of order exemplar ingestion failed attempts.",
        ))
        .expect("valid counter");
        let max_exemplars = Gauge::with_opts(Opts::new(
            "prometheus_tsdb_exemplar_max_exemplars",
            "Total number of exemplars the exemplar storage can store, resizeable.",
        ))
        .expect("valid gauge");

        if let Some(r) = reg {
            let _ = r.register(Box::new(exemplars_appended.clone()));
            let _ = r.register(Box::new(exemplars_in_storage.clone()));
            let _ = r.register(Box::new(series_with_exemplars_in_storage.clone()));
            let _ = r.register(Box::new(last_exemplars_ts.clone()));
            let _ = r.register(Box::new(out_of_order_exemplars.clone()));
            let _ = r.register(Box::new(max_exemplars.clone()));
        }

        Self {
            exemplars_appended,
            exemplars_in_storage,
            series_with_exemplars_in_storage,
            last_exemplars_ts,
            out_of_order_exemplars,
            max_exemplars,
        }
    }
}

/// Thread-safe circular exemplar buffer matching `tsdb.CircularExemplarStorage`.
#[derive(Debug)]
pub struct CircularExemplarStorage {
    inner: RwLock<CircularExemplarInner>,
    metrics: ExemplarMetrics,
}

#[derive(Debug)]
struct CircularExemplarInner {
    exemplars: Vec<CircularBufferEntry>,
    max_exemplars: usize,
    next_index: usize,
    ooo_time_window_ms: i64,
    index: HashMap<Labels, SeriesIndexEntry>,
    out_of_order_count: u64,
    appended_count: u64,
}

impl CircularExemplarStorage {
    /// Creates a new circular exemplar storage with capacity `max_exemplars` and OOO window `ooo_time_window_ms`.
    pub fn new(max_exemplars: usize, ooo_time_window_ms: i64) -> Self {
        Self::new_with_registry(max_exemplars, ooo_time_window_ms, None)
    }

    pub fn new_with_registry(
        max_exemplars: usize,
        ooo_time_window_ms: i64,
        registry: Option<&Registry>,
    ) -> Self {
        let ooo = ooo_time_window_ms.max(0);
        let metrics = ExemplarMetrics::new(registry);
        metrics.max_exemplars.set(max_exemplars as f64);
        Self {
            inner: RwLock::new(CircularExemplarInner {
                exemplars: Vec::new(),
                max_exemplars,
                next_index: 0,
                ooo_time_window_ms: ooo,
                index: HashMap::new(),
                out_of_order_count: 0,
                appended_count: 0,
            }),
            metrics,
        }
    }

    fn compute_metrics(&self, inner: &CircularExemplarInner) {
        self.metrics
            .series_with_exemplars_in_storage
            .set(inner.index.len() as f64);
        if inner.max_exemplars == 0 || inner.exemplars.is_empty() {
            self.metrics.exemplars_in_storage.set(0.0);
            self.metrics.last_exemplars_ts.set(0.0);
            return;
        }
        if inner.exemplars.len() == inner.max_exemplars
            && inner.exemplars[inner.next_index].series_labels.is_some()
        {
            self.metrics
                .exemplars_in_storage
                .set(inner.exemplars.len() as f64);
            if let Some(ref ex) = inner.exemplars[inner.next_index].exemplar {
                self.metrics
                    .last_exemplars_ts
                    .set((ex.timestamp_ms as f64) / 1000.0);
            }
            return;
        }
        self.metrics
            .exemplars_in_storage
            .set(inner.exemplars.len() as f64);
        if let Some(ref ex) = inner.exemplars[0].exemplar {
            self.metrics
                .last_exemplars_ts
                .set((ex.timestamp_ms as f64) / 1000.0);
        }
    }

    pub fn set_ooo_time_window(&self, ms: i64) {
        self.inner.write().ooo_time_window_ms = ms.max(0);
    }

    pub fn capacity(&self) -> usize {
        self.inner.read().max_exemplars
    }

    pub fn out_of_order_count(&self) -> u64 {
        self.inner.read().out_of_order_count
    }

    pub fn appended_count(&self) -> u64 {
        self.inner.read().appended_count
    }

    /// Validates an exemplar without inserting it.
    pub fn validate_exemplar(&self, series_labels: &Labels, e: &Exemplar) -> Result<(), ExemplarError> {
        let inner = self.inner.read();
        let entry = inner.index.get(series_labels);
        inner.validate_exemplar(entry, e)
    }

    /// Validates and appends an exemplar for `series_labels`.
    pub fn add_exemplar(&self, series_labels: &Labels, e: Exemplar) -> Result<(), ExemplarError> {
        let mut inner = self.inner.write();
        if inner.max_exemplars == 0 {
            return Err(ExemplarError::Disabled);
        }

        let validation = {
            let entry = inner.index.get(series_labels);
            inner.validate_exemplar(entry, &e)
        };
        if let Err(err) = validation {
            if err == ExemplarError::DuplicateExemplar {
                // Duplicate exemplar, noop (Go `AddExemplar`).
                return Ok(());
            }
            if err == ExemplarError::OutOfOrderExemplar {
                inner.out_of_order_count += 1;
                self.metrics.out_of_order_exemplars.inc();
            }
            return Err(err);
        }

        // If we insert an out-of-order exemplar, preemptively find the insertion index to check
        // for duplicates *before* evicting anything from the buffer (Go `AddExemplar`).
        let (mut ooo, mut insertion) = match inner.index.get(series_labels) {
            Some(entry) => {
                let oldest_ts = inner.ts_at(entry.oldest);
                let newest_ts = inner.ts_at(entry.newest);
                let ooo = e.timestamp_ms >= oldest_ts && e.timestamp_ms < newest_ts;
                let mut insertion = NO_EXEMPLAR;
                if ooo {
                    insertion = inner.find_insertion_index(&e, entry);
                    if inner.ts_at(insertion) == e.timestamp_ms {
                        // Assume duplicate exemplar, noop (Go `AddExemplar`).
                        return Ok(());
                    }
                }
                (ooo, insertion)
            }
            None => (false, NO_EXEMPLAR),
        };

        let idx = if inner.exemplars.len() < inner.max_exemplars {
            let i = inner.exemplars.len();
            let len = inner.exemplars.len();
            if len == inner.exemplars.capacity() {
                let cap_limit = inner.max_exemplars.min((u32::MAX as usize) - 1);
                let target = next_exemplar_cap(inner.exemplars.capacity(), cap_limit);
                inner.exemplars.reserve_exact(target - len);
            }
            inner.exemplars.push(CircularBufferEntry::default());
            i as u32
        } else {
            let i = inner.next_index as u32;
            if let Some(prev_labels) = inner.exemplars[i as usize].series_labels.clone() {
                inner.remove_at(i);
                if &prev_labels == series_labels && ooo && insertion == i {
                    // The entry we were going to insert after was removed from the same series:
                    // recalculate the insertion point in the updated linked list.
                    match inner.index.get(series_labels) {
                        Some(entry) => insertion = inner.find_insertion_index(&e, entry),
                        None => ooo = false,
                    }
                }
            }
            i
        };

        let (oldest, newest) = match inner.index.get(series_labels) {
            Some(entry) => (entry.oldest, entry.newest),
            None => (NO_EXEMPLAR, NO_EXEMPLAR),
        };
        let (prev, next) = if newest == NO_EXEMPLAR {
            // First and only exemplar of the series.
            (NO_EXEMPLAR, NO_EXEMPLAR)
        } else if e.timestamp_ms >= inner.ts_at(newest) {
            // At the tip (after newest).
            inner.exemplars[newest as usize].next = idx;
            (newest, NO_EXEMPLAR)
        } else if e.timestamp_ms < inner.ts_at(oldest) || !ooo {
            // At the tail (before oldest).
            inner.exemplars[oldest as usize].prev = idx;
            (NO_EXEMPLAR, oldest)
        } else {
            // After the most recent in-order exemplar preceding it.
            let next = inner.exemplars[insertion as usize].next;
            inner.exemplars[insertion as usize].next = idx;
            if next != NO_EXEMPLAR {
                inner.exemplars[next as usize].prev = idx;
            }
            (insertion, next)
        };
        inner.exemplars[idx as usize] = CircularBufferEntry {
            exemplar: Some(e),
            series_labels: Some(series_labels.clone()),
            next,
            prev,
        };
        let entry = inner
            .index
            .entry(series_labels.clone())
            .or_insert_with(|| SeriesIndexEntry {
                oldest: idx,
                newest: idx,
            });
        if prev == NO_EXEMPLAR {
            entry.oldest = idx;
        }
        if next == NO_EXEMPLAR {
            entry.newest = idx;
        }

        inner.next_index = ((idx as usize) + 1) % inner.max_exemplars;
        inner.appended_count += 1;
        self.metrics.exemplars_appended.inc();
        self.compute_metrics(&inner);
        Ok(())
    }

    /// Selects exemplars in `[start, end]` matching any of the matcher sets (OR across outer slice, AND within each slice).
    pub fn select(
        &self,
        start: i64,
        end: i64,
        matcher_sets: &[&[LabelMatcher]],
    ) -> Vec<ExemplarQueryResult> {
        let inner = self.inner.read();
        if inner.exemplars.is_empty() {
            return Vec::new();
        }

        let mut ret = Vec::new();
        for (series_labels, entry) in &inner.index {
            let oldest_ex = match inner.exemplars[entry.oldest as usize].exemplar.as_ref() {
                Some(ex) => ex,
                None => continue,
            };
            let newest_ex = match inner.exemplars[entry.newest as usize].exemplar.as_ref() {
                Some(ex) => ex,
                None => continue,
            };
            if oldest_ex.timestamp_ms > end || newest_ex.timestamp_ms < start {
                continue;
            }
            if !matches_some_matcher_set(series_labels, matcher_sets) {
                continue;
            }

            let mut exemplars = Vec::new();
            let mut cur = entry.oldest;
            while cur != NO_EXEMPLAR {
                let Some(ex) = inner.exemplars[cur as usize].exemplar.as_ref() else {
                    break;
                };
                if ex.timestamp_ms > end {
                    break;
                }
                if ex.timestamp_ms >= start {
                    exemplars.push(ex.clone());
                }
                cur = inner.exemplars[cur as usize].next;
            }
            if !exemplars.is_empty() {
                ret.push(ExemplarQueryResult {
                    series_labels: series_labels.clone(),
                    exemplars,
                });
            }
        }

        ret.sort_by(|a, b| a.series_labels.cmp(&b.series_labels));
        ret
    }

    /// Iterates over all stored exemplars in global ring-buffer ingestion order (oldest to newest),
    /// matching Go `CircularExemplarStorage.IterateExemplars`.
    pub fn iterate_exemplars<F>(&self, mut f: F)
    where
        F: FnMut(&Labels, &Exemplar),
    {
        let inner = self.inner.read();
        let len = inner.exemplars.len();
        if len == 0 {
            return;
        }
        let start = if len == inner.max_exemplars {
            inner.next_index
        } else {
            0
        };
        for offset in 0..len {
            let idx = (start + offset) % len;
            let slot = &inner.exemplars[idx];
            if let (Some(ref ex), Some(ref lbls)) = (&slot.exemplar, &slot.series_labels) {
                f(lbls, ex);
            }
        }
    }

    /// Resizes the circular buffer while holding the write lock, preserving the newest entries
    /// and their per-series linked-list order without re-validating or touching append counters
    /// (matching Go `CircularExemplarStorage.Resize`).
    pub fn resize(&self, new_size: usize) -> usize {
        let mut inner = self.inner.write();
        let old_max = inner.max_exemplars;
        if new_size == old_max {
            return 0;
        }

        self.metrics.max_exemplars.set(new_size as f64);
        if new_size == 0 {
            inner.exemplars.clear();
            inner.exemplars.shrink_to_fit();
            inner.max_exemplars = 0;
            inner.next_index = 0;
            inner.index.clear();
            self.compute_metrics(&inner);
            return 0;
        }

        let cur_len = inner.exemplars.len();
        if cur_len == 0 {
            inner.max_exemplars = new_size;
            inner.next_index = 0;
            self.compute_metrics(&inner);
            return 0;
        }

        let start = if cur_len == old_max {
            inner.next_index
        } else {
            0
        };
        let mut active_positions = Vec::with_capacity(cur_len);
        for offset in 0..cur_len {
            let pos = (start + offset) % cur_len;
            if inner.exemplars[pos].series_labels.is_some() {
                active_positions.push(pos);
            }
        }

        let drop_count = active_positions.len().saturating_sub(new_size);
        for &pos in &active_positions[..drop_count] {
            inner.remove_at(pos as u32);
        }

        let kept = &active_positions[drop_count..];
        let migrated = kept.len();
        let mut slot_remap = vec![NO_EXEMPLAR; cur_len];
        for (new_idx, &old_pos) in kept.iter().enumerate() {
            slot_remap[old_pos] = new_idx as u32;
        }

        let mut new_exemplars = Vec::new();
        new_exemplars.reserve_exact(migrated);
        for &old_pos in kept {
            let mut entry = std::mem::take(&mut inner.exemplars[old_pos]);
            if entry.prev != NO_EXEMPLAR {
                entry.prev = slot_remap[entry.prev as usize];
            }
            if entry.next != NO_EXEMPLAR {
                entry.next = slot_remap[entry.next as usize];
            }
            new_exemplars.push(entry);
        }
        for idx_entry in inner.index.values_mut() {
            if idx_entry.oldest != NO_EXEMPLAR {
                idx_entry.oldest = slot_remap[idx_entry.oldest as usize];
            }
            if idx_entry.newest != NO_EXEMPLAR {
                idx_entry.newest = slot_remap[idx_entry.newest as usize];
            }
        }

        inner.exemplars = new_exemplars;
        inner.max_exemplars = new_size;
        inner.next_index = migrated % new_size;
        self.compute_metrics(&inner);
        migrated
    }
}

impl CircularExemplarInner {
    /// Timestamp of the exemplar stored at buffer slot `i` (`i64::MIN` for an empty slot).
    fn ts_at(&self, i: u32) -> i64 {
        if i == NO_EXEMPLAR {
            return i64::MIN;
        }
        self.exemplars
            .get(i as usize)
            .and_then(|c| c.exemplar.as_ref())
            .map_or(i64::MIN, |e| e.timestamp_ms)
    }

    /// Go `findInsertionIndex`: the most recent exemplar of the series not newer than `e`.
    fn find_insertion_index(&self, e: &Exemplar, entry: &SeriesIndexEntry) -> u32 {
        let mut i = entry.newest;
        while i != NO_EXEMPLAR {
            if self.ts_at(i) <= e.timestamp_ms {
                return i;
            }
            i = self.exemplars[i as usize].prev;
        }
        entry.oldest
    }

    fn validate_exemplar(
        &self,
        entry: Option<&SeriesIndexEntry>,
        e: &Exemplar,
    ) -> Result<(), ExemplarError> {
        if self.max_exemplars == 0 {
            return Err(ExemplarError::Disabled);
        }

        let mut seen = HashSet::with_capacity(e.labels.len());
        let mut rune_len = 0usize;
        for (name, value) in e.labels.iter() {
            if !seen.insert(name) {
                return Err(ExemplarError::DuplicateLabelName(name.to_string()));
            }
            rune_len += name.chars().count() + value.chars().count();
            if rune_len > EXEMPLAR_MAX_LABEL_SET_LENGTH {
                return Err(ExemplarError::LabelLengthExceeded);
            }
        }

        let Some(entry) = entry else {
            return Ok(());
        };

        let Some(newest) = self
            .exemplars
            .get(entry.newest as usize)
            .and_then(|c| c.exemplar.as_ref())
        else {
            return Ok(());
        };
        if newest.prom_equals(e) {
            return Err(ExemplarError::DuplicateExemplar);
        }

        if (e.timestamp_ms < newest.timestamp_ms
            && (e.timestamp_ms as i128)
                <= (newest.timestamp_ms as i128) - (self.ooo_time_window_ms as i128))
            || (e.timestamp_ms == newest.timestamp_ms && e.value < newest.value)
            || (e.timestamp_ms == newest.timestamp_ms
                && e.value == newest.value
                && e.labels.hash() < newest.labels.hash())
        {
            return Err(ExemplarError::OutOfOrderExemplar);
        }

        Ok(())
    }

    fn remove_at(&mut self, idx: u32) {
        let idx_u = idx as usize;
        let Some(lbls) = self.exemplars[idx_u].series_labels.take() else {
            return;
        };
        self.exemplars[idx_u].exemplar = None;
        let prev = self.exemplars[idx_u].prev;
        let next = self.exemplars[idx_u].next;
        self.exemplars[idx_u].prev = NO_EXEMPLAR;
        self.exemplars[idx_u].next = NO_EXEMPLAR;

        if prev != NO_EXEMPLAR {
            self.exemplars[prev as usize].next = next;
        }
        if next != NO_EXEMPLAR {
            self.exemplars[next as usize].prev = prev;
        }

        if prev == NO_EXEMPLAR && next == NO_EXEMPLAR {
            self.index.remove(&lbls);
        } else if let Some(ent) = self.index.get_mut(&lbls) {
            if ent.oldest == idx {
                ent.oldest = next;
            }
            if ent.newest == idx {
                ent.newest = prev;
            }
        }
    }
}

/// Prometheus `matchesSomeMatcherSet`: an empty outer slice matches nothing (only `[{}]`, a
/// single empty matcher set, matches every series).
fn matches_some_matcher_set(lbls: &Labels, matcher_sets: &[&[LabelMatcher]]) -> bool {
    matcher_sets.iter().any(|ms| lbls.matches(ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matcher::MatchType;

    #[test]
    fn circular_exemplar_storage_basic_and_eviction() {
        let store = CircularExemplarStorage::new(2, 0);
        let s1 = Labels::from_pairs([("__name__", "req"), ("job", "a")]);
        let s2 = Labels::from_pairs([("__name__", "req"), ("job", "b")]);
        let ex_lbls = Labels::from_pairs([("trace_id", "1")]);

        store
            .add_exemplar(&s1, Exemplar::new(ex_lbls.clone(), 1.0, 100))
            .unwrap();
        // Exact duplicate is a no-op (Ok(())) matching Go AddExemplar, while validate_exemplar still reports DuplicateExemplar
        assert_eq!(
            store.validate_exemplar(&s1, &Exemplar::new(ex_lbls.clone(), 1.0, 100)),
            Err(ExemplarError::DuplicateExemplar)
        );
        assert_eq!(
            store.add_exemplar(&s1, Exemplar::new(ex_lbls.clone(), 1.0, 100)),
            Ok(())
        );
        // Out of order when ooo window is 0
        assert_eq!(
            store.add_exemplar(&s1, Exemplar::new(ex_lbls.clone(), 2.0, 50)),
            Err(ExemplarError::OutOfOrderExemplar)
        );

        store
            .add_exemplar(&s2, Exemplar::new(ex_lbls.clone(), 2.0, 200))
            .unwrap();
        // Overwrites oldest (s1 at 100)
        store
            .add_exemplar(&s1, Exemplar::new(ex_lbls.clone(), 3.0, 300))
            .unwrap();

        let m = LabelMatcher::new(MatchType::Equal, "__name__", "req").unwrap();
        let res = store.select(0, 500, &[&[m]]);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].series_labels, s1);
        assert_eq!(res[0].exemplars.len(), 1);
        assert_eq!(res[0].exemplars[0].timestamp_ms, 300);
        assert_eq!(res[1].series_labels, s2);
    }

    #[test]
    fn select_with_empty_matcher_sets_matches_nothing() {
        // Prometheus matchesSomeMatcherSet: `[]` matches nothing, `[{}]` matches everything.
        let store = CircularExemplarStorage::new(4, 0);
        let s = Labels::from_pairs([("__name__", "m")]);
        store
            .add_exemplar(&s, Exemplar::new(Labels::from_pairs([("trace_id", "1")]), 1.0, 100))
            .unwrap();
        assert!(store.select(0, 1000, &[]).is_empty());
        let empty: &[LabelMatcher] = &[];
        assert_eq!(store.select(0, 1000, &[empty]).len(), 1);
    }

    #[test]
    fn duplicate_exemplar_label_name_error_text() {
        let store = CircularExemplarStorage::new(4, 0);
        let s = Labels::from_pairs([("__name__", "m")]);
        let dup = Labels::from_sorted_slice(&[("trace_id", "1"), ("trace_id", "2")]);
        let err = store.add_exemplar(&s, Exemplar::new(dup, 1.0, 100)).unwrap_err();
        assert_eq!(err.to_string(), "label name \"trace_id\" is not unique: invalid exemplar");
    }

    #[test]
    fn ooo_duplicate_on_full_buffer_does_not_evict() {
        // Go AddExemplar checks the OOO insertion point / duplicate timestamp before evicting.
        let store = CircularExemplarStorage::new(3, 1000);
        let s = Labels::from_pairs([("__name__", "m")]);
        let other = Labels::from_pairs([("__name__", "o")]);
        let l = |v: &str| Labels::from_pairs([("trace_id", v)]);
        store.add_exemplar(&other, Exemplar::new(l("o"), 1.0, 50)).unwrap();
        store.add_exemplar(&s, Exemplar::new(l("a"), 1.0, 100)).unwrap();
        store.add_exemplar(&s, Exemplar::new(l("b"), 2.0, 300)).unwrap();
        // Buffer full; OOO exemplar with the same timestamp as a stored one is a no-op.
        assert_eq!(
            store.add_exemplar(&s, Exemplar::new(l("c"), 3.0, 100)),
            Ok(())
        );
        let all: &[LabelMatcher] = &[];
        let res = store.select(0, 1000, &[all]);
        assert_eq!(res.len(), 2, "nothing evicted: {res:?}");
        assert_eq!(res[0].series_labels, s);
        assert_eq!(res[0].exemplars.len(), 2);
        assert_eq!(res[1].exemplars.len(), 1);

        // A genuine OOO exemplar evicts the oldest slot (series "o") and is linked in order.
        store.add_exemplar(&s, Exemplar::new(l("d"), 4.0, 200)).unwrap();
        let res = store.select(0, 1000, &[all]);
        assert_eq!(res.len(), 1);
        let ts: Vec<i64> = res[0].exemplars.iter().map(|e| e.timestamp_ms).collect();
        assert_eq!(ts, vec![100, 200, 300]);

        // OOO insertion whose insertion point is the slot being evicted (same series).
        store.add_exemplar(&s, Exemplar::new(l("e"), 5.0, 150)).unwrap();
        let res = store.select(0, 1000, &[all]);
        let ts: Vec<i64> = res[0].exemplars.iter().map(|e| e.timestamp_ms).collect();
        assert_eq!(ts, vec![150, 200, 300]);
    }

    #[test]
    fn circular_exemplar_label_length_limit() {
        let store = CircularExemplarStorage::new(10, 0);
        let s = Labels::from_pairs([("__name__", "m")]);
        let long = "a".repeat(120);
        let ex_lbls = Labels::from_pairs([("trace_id", long.as_str())]); // 8 + 120 = 128 OK
        store
            .add_exemplar(&s, Exemplar::new(ex_lbls, 1.0, 100))
            .unwrap();

        let too_long = "a".repeat(121);
        let ex_lbls2 = Labels::from_pairs([("trace_id", too_long.as_str())]); // 8 + 121 = 129 ERR
        assert_eq!(
            store.add_exemplar(&s, Exemplar::new(ex_lbls2, 1.0, 200)),
            Err(ExemplarError::LabelLengthExceeded)
        );
    }

    #[test]
    fn circular_exemplar_validate_near_i64_min_no_underflow() {
        let store = CircularExemplarStorage::new(10, 3_600_000);
        let s = Labels::from_pairs([("__name__", "m")]);
        let l = |v: &str| Labels::from_pairs([("trace_id", v)]);
        store
            .add_exemplar(&s, Exemplar::new(l("a"), 1.0, i64::MIN + 1_000))
            .unwrap();
        // Within OOO window (500ms earlier) must succeed without i64 underflow!
        store
            .add_exemplar(&s, Exemplar::new(l("b"), 2.0, i64::MIN + 500))
            .unwrap();
        store
            .add_exemplar(&s, Exemplar::new(l("c"), 3.0, i64::MIN))
            .unwrap();
    }

    #[test]
    fn circular_exemplar_storage_exact_capacity_and_compact_slot_size() {
        assert_eq!(std::mem::size_of::<CircularBufferEntry>(), 96);
        assert_eq!(std::mem::size_of::<SeriesIndexEntry>(), 8);

        let max = 1000;
        let store = CircularExemplarStorage::new(max, 0);
        let ex_lbls = Labels::from_pairs([("trace_id", "1")]);
        for i in 0..max {
            let s = Labels::from_pairs([("__name__", "m"), ("id", &i.to_string())]);
            store
                .add_exemplar(&s, Exemplar::new(ex_lbls.clone(), i as f64, i as i64))
                .unwrap();
        }
        let inner = store.inner.read();
        assert_eq!(inner.exemplars.len(), max);
        assert_eq!(inner.exemplars.capacity(), max);
    }
}
