//! Circular in-memory exemplar storage matching Prometheus `tsdb.CircularExemplarStorage`.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/exemplar.go
//! and github.com/prometheus/prometheus/model/exemplar/exemplar.go.

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use thiserror::Error;

use crate::labels::Labels;
use crate::matcher::LabelMatcher;

/// OpenMetrics maximum combined rune length of an exemplar's label names and values.
pub const EXEMPLAR_MAX_LABEL_SET_LENGTH: usize = 128;

const NO_EXEMPLAR: usize = usize::MAX;
const ESTIMATED_EXEMPLARS_PER_SERIES: usize = 16;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExemplarError {
    #[error("exemplar storage is disabled")]
    Disabled,
    #[error("duplicate label name {0:?} in exemplar")]
    DuplicateLabelName(String),
    #[error("exemplar label set length exceeds {EXEMPLAR_MAX_LABEL_SET_LENGTH} characters")]
    LabelLengthExceeded,
    #[error("duplicate exemplar")]
    DuplicateExemplar,
    #[error("out-of-order exemplar")]
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
    oldest: usize,
    newest: usize,
    series_labels: Labels,
}

#[derive(Clone, Debug)]
struct CircularBufferEntry {
    exemplar: Option<Exemplar>,
    next: usize,
    prev: usize,
    series_key: Option<Vec<u8>>,
}

impl Default for CircularBufferEntry {
    fn default() -> Self {
        Self {
            exemplar: None,
            next: NO_EXEMPLAR,
            prev: NO_EXEMPLAR,
            series_key: None,
        }
    }
}

/// Thread-safe circular exemplar buffer matching `tsdb.CircularExemplarStorage`.
#[derive(Debug)]
pub struct CircularExemplarStorage {
    inner: RwLock<CircularExemplarInner>,
}

#[derive(Debug)]
struct CircularExemplarInner {
    exemplars: Vec<CircularBufferEntry>,
    next_index: usize,
    ooo_time_window_ms: i64,
    index: HashMap<Vec<u8>, SeriesIndexEntry>,
    out_of_order_count: u64,
    appended_count: u64,
}

impl CircularExemplarStorage {
    /// Creates a new circular exemplar storage with capacity `max_exemplars` and OOO window `ooo_time_window_ms`.
    pub fn new(max_exemplars: usize, ooo_time_window_ms: i64) -> Self {
        let ooo = ooo_time_window_ms.max(0);
        let init_index_cap = max_exemplars / ESTIMATED_EXEMPLARS_PER_SERIES;
        Self {
            inner: RwLock::new(CircularExemplarInner {
                exemplars: vec![CircularBufferEntry::default(); max_exemplars],
                next_index: 0,
                ooo_time_window_ms: ooo,
                index: HashMap::with_capacity(init_index_cap),
                out_of_order_count: 0,
                appended_count: 0,
            }),
        }
    }

    pub fn set_ooo_time_window(&self, ms: i64) {
        self.inner.write().ooo_time_window_ms = ms.max(0);
    }

    pub fn capacity(&self) -> usize {
        self.inner.read().exemplars.len()
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
        let entry = inner.index.get(series_labels.as_bytes());
        inner.validate_exemplar(entry, e)
    }

    /// Validates and appends an exemplar for `series_labels`.
    pub fn add_exemplar(&self, series_labels: &Labels, e: Exemplar) -> Result<(), ExemplarError> {
        let mut inner = self.inner.write();
        if inner.exemplars.is_empty() {
            return Err(ExemplarError::Disabled);
        }

        let key = series_labels.as_bytes();
        let validation = {
            let entry = inner.index.get(key);
            inner.validate_exemplar(entry, &e)
        };
        if let Err(err) = validation {
            if err == ExemplarError::OutOfOrderExemplar {
                inner.out_of_order_count += 1;
            }
            return Err(err);
        }

        let idx = inner.next_index;
        // Evict existing occupant at `idx` if present.
        if inner.exemplars[idx].series_key.is_some() {
            inner.remove_at(idx);
        }

        let key_vec = key.to_vec();
        let entry = inner.index.entry(key_vec.clone()).or_insert_with(|| SeriesIndexEntry {
            oldest: idx,
            newest: NO_EXEMPLAR,
            series_labels: series_labels.clone(),
        });

        let prev_newest = entry.newest;
        // Insert into doubly-linked list maintaining timestamp-ascending order for this series,
        // matching CircularExemplarStorage.AddExemplar.
        if prev_newest == NO_EXEMPLAR {
            entry.oldest = idx;
            entry.newest = idx;
            inner.exemplars[idx] = CircularBufferEntry {
                exemplar: Some(e),
                next: NO_EXEMPLAR,
                prev: NO_EXEMPLAR,
                series_key: Some(key_vec),
            };
        } else {
            let newest_ts = inner.exemplars[prev_newest]
                .exemplar
                .as_ref()
                .unwrap()
                .timestamp_ms;
            if e.timestamp_ms >= newest_ts {
                inner.exemplars[prev_newest].next = idx;
                inner.exemplars[idx] = CircularBufferEntry {
                    exemplar: Some(e),
                    next: NO_EXEMPLAR,
                    prev: prev_newest,
                    series_key: Some(key_vec.clone()),
                };
                if let Some(ent) = inner.index.get_mut(&key_vec) {
                    ent.newest = idx;
                }
            } else {
                // OOO exemplar insertion: walk backwards from newest to find insertion point.
                let mut cur = prev_newest;
                while cur != NO_EXEMPLAR {
                    let cur_ex = inner.exemplars[cur].exemplar.as_ref().unwrap();
                    if cur_ex.timestamp_ms < e.timestamp_ms
                        || (cur_ex.timestamp_ms == e.timestamp_ms && cur_ex.value < e.value)
                        || (cur_ex.timestamp_ms == e.timestamp_ms
                            && cur_ex.value == e.value
                            && cur_ex.labels.hash() < e.labels.hash())
                    {
                        break;
                    }
                    if cur_ex.prom_equals(&e) {
                        return Err(ExemplarError::DuplicateExemplar);
                    }
                    cur = inner.exemplars[cur].prev;
                }

                if cur == NO_EXEMPLAR {
                    // Insert at oldest.
                    let old_oldest = inner.index.get(&key_vec).unwrap().oldest;
                    inner.exemplars[old_oldest].prev = idx;
                    inner.exemplars[idx] = CircularBufferEntry {
                        exemplar: Some(e),
                        next: old_oldest,
                        prev: NO_EXEMPLAR,
                        series_key: Some(key_vec.clone()),
                    };
                    inner.index.get_mut(&key_vec).unwrap().oldest = idx;
                } else {
                    let old_next = inner.exemplars[cur].next;
                    inner.exemplars[cur].next = idx;
                    if old_next != NO_EXEMPLAR {
                        inner.exemplars[old_next].prev = idx;
                    }
                    inner.exemplars[idx] = CircularBufferEntry {
                        exemplar: Some(e),
                        next: old_next,
                        prev: cur,
                        series_key: Some(key_vec.clone()),
                    };
                    if old_next == NO_EXEMPLAR {
                        inner.index.get_mut(&key_vec).unwrap().newest = idx;
                    }
                }
            }
        }

        inner.next_index = (idx + 1) % inner.exemplars.len();
        inner.appended_count += 1;
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
        for entry in inner.index.values() {
            let oldest_ex = match inner.exemplars[entry.oldest].exemplar.as_ref() {
                Some(ex) => ex,
                None => continue,
            };
            let newest_ex = match inner.exemplars[entry.newest].exemplar.as_ref() {
                Some(ex) => ex,
                None => continue,
            };
            if oldest_ex.timestamp_ms > end || newest_ex.timestamp_ms < start {
                continue;
            }
            if !matches_some_matcher_set(&entry.series_labels, matcher_sets) {
                continue;
            }

            let mut exemplars = Vec::new();
            let mut cur = entry.oldest;
            while cur != NO_EXEMPLAR {
                let Some(ex) = inner.exemplars[cur].exemplar.as_ref() else {
                    break;
                };
                if ex.timestamp_ms > end {
                    break;
                }
                if ex.timestamp_ms >= start {
                    exemplars.push(ex.clone());
                }
                cur = inner.exemplars[cur].next;
            }
            if !exemplars.is_empty() {
                ret.push(ExemplarQueryResult {
                    series_labels: entry.series_labels.clone(),
                    exemplars,
                });
            }
        }

        ret.sort_by(|a, b| a.series_labels.cmp(&b.series_labels));
        ret
    }

    /// Iterates over all stored exemplars in buffer order.
    pub fn iterate_exemplars<F>(&self, mut f: F)
    where
        F: FnMut(&Labels, &Exemplar),
    {
        let inner = self.inner.read();
        for entry in inner.index.values() {
            let mut cur = entry.oldest;
            while cur != NO_EXEMPLAR {
                let Some(ex) = inner.exemplars[cur].exemplar.as_ref() else {
                    break;
                };
                f(&entry.series_labels, ex);
                cur = inner.exemplars[cur].next;
            }
        }
    }

    /// Resizes the circular buffer, preserving the newest entries up to `new_size`.
    pub fn resize(&self, new_size: usize) -> usize {
        let mut inner = self.inner.write();
        let old_size = inner.exemplars.len();
        if new_size == old_size {
            return 0;
        }

        // Collect all active exemplars in ingestion order (oldest -> newest).
        let mut ordered: Vec<(Labels, Exemplar)> = Vec::new();
        if old_size > 0 {
            for offset in 0..old_size {
                let pos = (inner.next_index + offset) % old_size;
                if let (Some(ex), Some(key)) = (
                    inner.exemplars[pos].exemplar.take(),
                    inner.exemplars[pos].series_key.take(),
                ) {
                    if let Some(ent) = inner.index.get(&key) {
                        ordered.push((ent.series_labels.clone(), ex));
                    }
                }
            }
        }

        inner.exemplars = vec![CircularBufferEntry::default(); new_size];
        inner.next_index = 0;
        inner.index.clear();

        if new_size == 0 {
            return 0;
        }

        let skip = ordered.len().saturating_sub(new_size);
        let kept = ordered.into_iter().skip(skip).collect::<Vec<_>>();
        let migrated = kept.len();
        drop(inner);

        for (sl, ex) in kept {
            let _ = self.add_exemplar(&sl, ex);
        }
        migrated
    }
}

impl CircularExemplarInner {
    fn validate_exemplar(
        &self,
        entry: Option<&SeriesIndexEntry>,
        e: &Exemplar,
    ) -> Result<(), ExemplarError> {
        if self.exemplars.is_empty() {
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

        let newest = self.exemplars[entry.newest]
            .exemplar
            .as_ref()
            .expect("newest index valid");
        if newest.prom_equals(e) {
            return Err(ExemplarError::DuplicateExemplar);
        }

        if (e.timestamp_ms < newest.timestamp_ms
            && e.timestamp_ms <= newest.timestamp_ms - self.ooo_time_window_ms)
            || (e.timestamp_ms == newest.timestamp_ms && e.value < newest.value)
            || (e.timestamp_ms == newest.timestamp_ms
                && e.value == newest.value
                && e.labels.hash() < newest.labels.hash())
        {
            return Err(ExemplarError::OutOfOrderExemplar);
        }

        Ok(())
    }

    fn remove_at(&mut self, idx: usize) {
        let Some(key) = self.exemplars[idx].series_key.take() else {
            return;
        };
        self.exemplars[idx].exemplar = None;
        let prev = self.exemplars[idx].prev;
        let next = self.exemplars[idx].next;
        self.exemplars[idx].prev = NO_EXEMPLAR;
        self.exemplars[idx].next = NO_EXEMPLAR;

        if prev != NO_EXEMPLAR {
            self.exemplars[prev].next = next;
        }
        if next != NO_EXEMPLAR {
            self.exemplars[next].prev = prev;
        }

        if prev == NO_EXEMPLAR && next == NO_EXEMPLAR {
            self.index.remove(&key);
        } else if let Some(ent) = self.index.get_mut(&key) {
            if ent.oldest == idx {
                ent.oldest = next;
            }
            if ent.newest == idx {
                ent.newest = prev;
            }
        }
    }
}

fn matches_some_matcher_set(lbls: &Labels, matcher_sets: &[&[LabelMatcher]]) -> bool {
    if matcher_sets.is_empty() {
        return true;
    }
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
        // Exact duplicate returns DuplicateExemplar
        assert_eq!(
            store.add_exemplar(&s1, Exemplar::new(ex_lbls.clone(), 1.0, 100)),
            Err(ExemplarError::DuplicateExemplar)
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
}
