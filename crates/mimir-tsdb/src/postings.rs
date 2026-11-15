//! In-memory inverted postings index and Prometheus matcher evaluation.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/index/postings.go
//! and github.com/prometheus/prometheus/tsdb/querier.go.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

use hashbrown::HashMap;
use parking_lot::RwLock;
use smallvec::SmallVec;
use thiserror::Error;

use crate::labels::SymbolLabels;
use crate::matcher::{LabelMatcher, MatchType};
use crate::symbols::{SymbolId, SymbolTable};

/// Canonical key `(SymbolId::EMPTY, SymbolId::EMPTY)` storing all series refs in `MemPostings`.
pub const ALL_POSTINGS_KEY: (SymbolId, SymbolId) = (SymbolId::EMPTY, SymbolId::EMPTY);

/// Minimum label value byte length required before counting towards per-label-name byte accounting.
pub const LABEL_VALUE_BYTES_MIN_LENGTH: usize = 128;

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

const POSTINGS_STRIPES: usize = 64;

#[derive(Debug, Default)]
struct PostingsList {
    small: Vec<u32>,
    overflow: Vec<u64>,
}

impl PostingsList {
    #[inline]
    fn is_empty(&self) -> bool {
        self.small.is_empty() && self.overflow.is_empty()
    }

    #[inline]
    fn len(&self) -> usize {
        self.small.len() + self.overflow.len()
    }

    #[inline]
    fn push(&mut self, id: u64, ordered: bool) {
        if let Ok(id32) = u32::try_from(id) {
            if !ordered || self.small.last().copied() < Some(id32) {
                self.small.push(id32);
            } else if let Err(pos) = self.small.binary_search(&id32) {
                self.small.insert(pos, id32);
            }
        } else if !ordered || self.overflow.last().copied() < Some(id) {
            self.overflow.push(id);
        } else if let Err(pos) = self.overflow.binary_search(&id) {
            self.overflow.insert(pos, id);
        }
    }

    fn delete(&mut self, deleted: &HashSet<u64>) {
        self.small.retain(|&id| !deleted.contains(&(id as u64)));
        if !self.overflow.is_empty() {
            self.overflow.retain(|id| !deleted.contains(id));
        }
    }

    fn sort_and_dedup(&mut self) {
        if self.small.len() > 1 {
            self.small.sort_unstable();
            self.small.dedup();
        }
        if self.overflow.len() > 1 {
            self.overflow.sort_unstable();
            self.overflow.dedup();
        }
    }

    fn to_u64_vec(&self) -> Vec<u64> {
        if self.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.len());
        out.extend(self.small.iter().map(|&x| x as u64));
        out.extend_from_slice(&self.overflow);
        out
    }

    fn first(&self, ordered: bool) -> Option<u64> {
        if !self.small.is_empty() {
            return if ordered {
                self.small.first().map(|&x| x as u64)
            } else {
                self.small.iter().copied().min().map(|x| x as u64)
            };
        }
        if !self.overflow.is_empty() {
            return if ordered {
                self.overflow.first().copied()
            } else {
                self.overflow.iter().copied().min()
            };
        }
        None
    }
}

#[derive(Debug, Default)]
struct PostingsStripe {
    m: HashMap<SymbolId, HashMap<SymbolId, Vec<u32>>>,
    overflow: HashMap<(SymbolId, SymbolId), Vec<u64>>,
    lvs: HashMap<SymbolId, Vec<SymbolId>>,
}

/// Reusable scratch buffers for [`MemPostings::add_batch_unordered_with_scratch`].
pub struct PostingsBatchScratch {
    buckets: [Vec<(u64, SymbolId, SymbolId)>; POSTINGS_STRIPES],
}

impl Default for PostingsBatchScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl PostingsBatchScratch {
    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| Vec::new()),
        }
    }
}

/// 64-stripe sharded inverted index mapping `(label_name_id, label_value_id)` -> sorted
/// `Vec<u32>` series refs (with overflow `Vec<u64>` map only if any explicit ref exceeds `u32::MAX`),
/// plus a dedicated `all_postings` list for `ALL_POSTINGS_KEY`.
#[derive(Debug)]
pub struct MemPostings {
    stripes: [RwLock<PostingsStripe>; POSTINGS_STRIPES],
    all_postings: RwLock<PostingsList>,
    ordered: AtomicBool,
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
            stripes: std::array::from_fn(|_| RwLock::new(PostingsStripe::default())),
            all_postings: RwLock::new(PostingsList::default()),
            ordered: AtomicBool::new(ordered),
        }
    }

    #[inline]
    fn stripe_idx(name_id: SymbolId) -> usize {
        (name_id.0 as usize).wrapping_mul(0x9E37_79B9) & (POSTINGS_STRIPES - 1)
    }

    /// Adds `series_ref` for all `(name_id, value_id)` pairs in `labels`, plus `ALL_POSTINGS_KEY`.
    pub fn add(&self, series_ref: u64, labels: &SymbolLabels) {
        let ordered = self.ordered.load(AtomicOrdering::Acquire);
        {
            let mut all = self.all_postings.write();
            all.push(series_ref, ordered);
        }
        let pairs = labels.as_slice();
        if pairs.is_empty() {
            return;
        }
        if pairs.len() == 1 {
            let (nid, vid) = pairs[0];
            if (nid, vid) != ALL_POSTINGS_KEY {
                self.stripes[Self::stripe_idx(nid)]
                    .write()
                    .add_for(series_ref, nid, vid, ordered);
            }
            return;
        }
        let mut routed: SmallVec<[(usize, SymbolId, SymbolId); 24]> =
            SmallVec::with_capacity(pairs.len());
        for &(nid, vid) in pairs {
            if (nid, vid) != ALL_POSTINGS_KEY {
                routed.push((Self::stripe_idx(nid), nid, vid));
            }
        }
        routed.sort_unstable_by_key(|x| x.0);
        let mut i = 0;
        while i < routed.len() {
            let s_idx = routed[i].0;
            let mut stripe = self.stripes[s_idx].write();
            while i < routed.len() && routed[i].0 == s_idx {
                stripe.add_for(series_ref, routed[i].1, routed[i].2, ordered);
                i += 1;
            }
        }
    }

    /// Batch-inserts series postings in unordered mode, acquiring each stripe lock at most once per batch.
    pub fn add_batch_unordered(&self, batch: &[(u64, SymbolLabels)]) {
        let mut scratch = PostingsBatchScratch::new();
        self.add_batch_unordered_with_scratch(batch, &mut scratch);
    }

    /// Batch-inserts series postings in unordered mode using reusable `scratch` buffers.
    pub fn add_batch_unordered_with_scratch(
        &self,
        batch: &[(u64, SymbolLabels)],
        scratch: &mut PostingsBatchScratch,
    ) {
        if batch.is_empty() {
            return;
        }
        let ordered = self.ordered.load(AtomicOrdering::Acquire);
        {
            let mut all = self.all_postings.write();
            all.small.reserve(batch.len());
            for &(ref_id, _) in batch {
                all.push(ref_id, ordered);
            }
        }
        for b in &mut scratch.buckets {
            b.clear();
        }
        for (ref_id, labels) in batch {
            for &(nid, vid) in labels.as_slice() {
                if (nid, vid) != ALL_POSTINGS_KEY {
                    scratch.buckets[Self::stripe_idx(nid)].push((*ref_id, nid, vid));
                }
            }
        }
        for (s_idx, bucket) in scratch.buckets.iter_mut().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let mut stripe = self.stripes[s_idx].write();
            for &(ref_id, nid, vid) in bucket.iter() {
                stripe.add_for(ref_id, nid, vid, ordered);
            }
            bucket.clear();
        }
    }

    /// Ensures all postings lists are sorted in ascending order (and deduplicated) and switches to ordered mode.
    pub fn ensure_order(&self) {
        self.ensure_order_parallel();
    }

    /// Sorts and deduplicates all 64 stripes and `all_postings` concurrently across worker threads.
    pub fn ensure_order_parallel(&self) {
        if self.ordered.load(AtomicOrdering::Acquire) {
            return;
        }
        let next_task = AtomicUsize::new(0);
        let num_tasks = POSTINGS_STRIPES + 1;
        let workers = std::thread::available_parallelism()
            .map_or(4, |n| n.get())
            .clamp(2, 16)
            .min(num_tasks);

        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| loop {
                    let task = next_task.fetch_add(1, AtomicOrdering::Relaxed);
                    if task >= num_tasks {
                        break;
                    }
                    if task == POSTINGS_STRIPES {
                        self.all_postings.write().sort_and_dedup();
                    } else {
                        self.stripes[task].write().sort_and_dedup();
                    }
                });
            }
        });

        self.ordered.store(true, AtomicOrdering::Release);
    }

    /// Removes all `deleted` series refs from `affected` label pairs and `ALL_POSTINGS_KEY`.
    pub fn delete(&self, deleted: &HashSet<u64>, affected: &[(SymbolId, SymbolId)]) {
        if deleted.is_empty() {
            return;
        }
        {
            let mut all = self.all_postings.write();
            all.delete(deleted);
        }
        if affected.is_empty() {
            return;
        }
        let mut buckets: [Vec<(SymbolId, SymbolId)>; POSTINGS_STRIPES] =
            std::array::from_fn(|_| Vec::new());
        for &(nid, vid) in affected {
            if (nid, vid) != ALL_POSTINGS_KEY {
                buckets[Self::stripe_idx(nid)].push((nid, vid));
            }
        }
        for (s_idx, pairs) in buckets.into_iter().enumerate() {
            if pairs.is_empty() {
                continue;
            }
            let mut stripe = self.stripes[s_idx].write();
            let mut affected_names: HashSet<SymbolId> = HashSet::new();
            for (name_id, value_id) in pairs {
                if stripe.delete_pair(deleted, name_id, value_id) {
                    affected_names.insert(name_id);
                }
            }
            for name_id in affected_names {
                let empty_name = stripe.m.get(&name_id).is_none_or(|vm| vm.is_empty());
                if empty_name {
                    stripe.m.remove(&name_id);
                    stripe.lvs.remove(&name_id);
                } else if let Some(vm) = stripe.m.get(&name_id) {
                    let lvs: Vec<SymbolId> = vm.keys().copied().collect();
                    stripe.lvs.insert(name_id, lvs);
                }
            }
        }
    }

    /// Returns a sorted list of all series refs in the index.
    pub fn all_postings(&self) -> Vec<u64> {
        self.all_postings.read().to_u64_vec()
    }

    /// Returns sorted postings for `(name_id, value_id)`.
    pub fn postings_for_id(&self, name_id: SymbolId, value_id: SymbolId) -> Vec<u64> {
        if (name_id, value_id) == ALL_POSTINGS_KEY {
            return self.all_postings();
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        stripe.pair_to_u64(name_id, value_id)
    }

    /// Visits postings slices for `(name_id, value_id)` under the read lock with zero allocation.
    pub fn visit_postings_for_id<R>(
        &self,
        name_id: SymbolId,
        value_id: SymbolId,
        f: impl FnOnce(&[u32], &[u64]) -> R,
    ) -> R {
        if (name_id, value_id) == ALL_POSTINGS_KEY {
            let all = self.all_postings.read();
            return f(&all.small, &all.overflow);
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let small = stripe
            .m
            .get(&name_id)
            .and_then(|vm| vm.get(&value_id))
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let large = if stripe.overflow.is_empty() {
            &[]
        } else {
            stripe
                .overflow
                .get(&(name_id, value_id))
                .map(Vec::as_slice)
                .unwrap_or(&[])
        };
        f(small, large)
    }

    /// Returns the number of postings for `(name_id, value_id)`.
    pub fn postings_len(&self, name_id: SymbolId, value_id: SymbolId) -> usize {
        if (name_id, value_id) == ALL_POSTINGS_KEY {
            return self.all_postings.read().len();
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let u32_len = stripe
            .m
            .get(&name_id)
            .and_then(|vm| vm.get(&value_id))
            .map_or(0, Vec::len);
        let ov_len = if stripe.overflow.is_empty() {
            0
        } else {
            stripe
                .overflow
                .get(&(name_id, value_id))
                .map_or(0, Vec::len)
        };
        u32_len + ov_len
    }

    /// Returns the smallest/first posting ref for `(name_id, value_id)`, if any.
    pub fn first_posting(&self, name_id: SymbolId, value_id: SymbolId) -> Option<u64> {
        let ordered = self.ordered.load(AtomicOrdering::Acquire);
        if (name_id, value_id) == ALL_POSTINGS_KEY {
            return self.all_postings.read().first(ordered);
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let list = stripe.m.get(&name_id).and_then(|vm| vm.get(&value_id));
        if let Some(l) = list {
            if !l.is_empty() {
                return if ordered {
                    l.first().map(|&x| x as u64)
                } else {
                    l.iter().copied().min().map(|x| x as u64)
                };
            }
        }
        if !stripe.overflow.is_empty() {
            if let Some(ov) = stripe.overflow.get(&(name_id, value_id)) {
                return if ordered {
                    ov.first().copied()
                } else {
                    ov.iter().copied().min()
                };
            }
        }
        None
    }

    /// Appends up to `limit` sample postings refs for `(name_id, value_id)` into `out`.
    pub fn append_postings_sample(
        &self,
        name_id: SymbolId,
        value_id: SymbolId,
        limit: usize,
        out: &mut Vec<u64>,
    ) {
        if limit == 0 {
            return;
        }
        if (name_id, value_id) == ALL_POSTINGS_KEY {
            let all = self.all_postings.read();
            append_random_sample_from_slices(&all.small, &all.overflow, limit, out);
            return;
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let list = stripe
            .m
            .get(&name_id)
            .and_then(|vm| vm.get(&value_id))
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let ov = if stripe.overflow.is_empty() {
            &[]
        } else {
            stripe
                .overflow
                .get(&(name_id, value_id))
                .map(Vec::as_slice)
                .unwrap_or(&[])
        };
        append_random_sample_from_slices(list, ov, limit, out);
    }

    /// Returns total byte length of distinct label values (longer than `LABEL_VALUE_BYTES_MIN_LENGTH`) per label name.
    pub fn label_values_bytes(&self, symbols: &SymbolTable) -> std::collections::HashMap<String, u64> {
        let mut out = std::collections::HashMap::new();
        for s_rw in &self.stripes {
            let stripe = s_rw.read();
            for (&name_id, val_ids) in &stripe.lvs {
                if name_id == ALL_POSTINGS_KEY.0 {
                    continue;
                }
                let total: u64 = val_ids
                    .iter()
                    .map(|&vid| symbols.resolve(vid).len() as u64)
                    .filter(|&len| len > LABEL_VALUE_BYTES_MIN_LENGTH as u64)
                    .sum();
                if total > 0 {
                    out.insert(symbols.resolve(name_id).to_string(), total);
                }
            }
        }
        out
    }

    /// Returns sorted union of postings for `name_id` across `value_ids`.
    pub fn postings_for_ids(&self, name_id: SymbolId, value_ids: &[SymbolId]) -> Vec<u64> {
        if value_ids.is_empty() {
            return Vec::new();
        }
        if name_id == ALL_POSTINGS_KEY.0 && value_ids.contains(&ALL_POSTINGS_KEY.1) {
            return self.all_postings();
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let Some(vm) = stripe.m.get(&name_id) else {
            return Vec::new();
        };
        if stripe.overflow.is_empty() {
            let lists: Vec<&[u32]> = value_ids
                .iter()
                .filter_map(|vid| vm.get(vid).map(|v| v.as_slice()))
                .filter(|s| !s.is_empty())
                .collect();
            return merge_postings_u32_to_u64(&lists);
        }
        let mut total = 0usize;
        for vid in value_ids {
            if let Some(v) = vm.get(vid) {
                total += v.len();
            }
            if let Some(ov) = stripe.overflow.get(&(name_id, *vid)) {
                total += ov.len();
            }
        }
        if total == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(total);
        for vid in value_ids {
            if let Some(v) = vm.get(vid) {
                out.extend(v.iter().map(|&x| x as u64));
            }
            if let Some(ov) = stripe.overflow.get(&(name_id, *vid)) {
                out.extend_from_slice(ov);
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Returns sorted union of all postings for `name_id`.
    pub fn postings_for_all_values_id(&self, name_id: SymbolId) -> Vec<u64> {
        if name_id == ALL_POSTINGS_KEY.0 {
            return self.all_postings();
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let Some(vm) = stripe.m.get(&name_id) else {
            return Vec::new();
        };
        if stripe.overflow.is_empty() {
            let lists: Vec<&[u32]> = vm
                .values()
                .map(|v| v.as_slice())
                .filter(|s| !s.is_empty())
                .collect();
            return merge_postings_u32_to_u64(&lists);
        }
        let mut out = Vec::new();
        for (&vid, v) in vm {
            out.extend(v.iter().map(|&x| x as u64));
            if let Some(ov) = stripe.overflow.get(&(name_id, vid)) {
                out.extend_from_slice(ov);
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Returns sorted postings for `(name_id, value)` where `pred(value_str)` is true.
    pub fn postings_for_label_matching_id(
        &self,
        name_id: SymbolId,
        symbols: &SymbolTable,
        pred: &mut dyn FnMut(&str) -> bool,
    ) -> Vec<u64> {
        let value_ids: Vec<SymbolId> = {
            let stripe = self.stripes[Self::stripe_idx(name_id)].read();
            match stripe.lvs.get(&name_id) {
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
        let mut ids: Vec<SymbolId> = Vec::new();
        for s_rw in &self.stripes {
            let stripe = s_rw.read();
            for &k in stripe.m.keys() {
                if k != ALL_POSTINGS_KEY.0 {
                    ids.push(k);
                }
            }
        }
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
            let stripe = self.stripes[Self::stripe_idx(name_id)].read();
            match stripe.lvs.get(&name_id) {
                Some(v) => v.clone(),
                None => return Vec::new(),
            }
        };
        ids.into_iter()
            .map(|id| symbols.resolve(id).to_string())
            .collect()
    }

    /// Returns all label values for `name` sorted lexicographically, optionally truncated to `limit`
    /// before allocating `String`s.
    pub fn sorted_label_values_with_limit(
        &self,
        name: &str,
        symbols: &SymbolTable,
        limit: Option<usize>,
    ) -> Vec<String> {
        let Some(name_id) = symbols.get(name) else {
            return Vec::new();
        };
        let ids: Vec<SymbolId> = {
            let stripe = self.stripes[Self::stripe_idx(name_id)].read();
            match stripe.lvs.get(&name_id) {
                Some(v) => v.clone(),
                None => return Vec::new(),
            }
        };
        let mut strs: Vec<&str> = ids.into_iter().map(|id| symbols.resolve(id)).collect();
        if let Some(lim) = limit {
            if lim > 0 && strs.len() > lim {
                strs.select_nth_unstable(lim - 1);
                strs.truncate(lim);
            }
        }
        strs.sort_unstable();
        strs.into_iter().map(str::to_string).collect()
    }

    /// Returns all label values for `name` sorted lexicographically.
    pub fn sorted_label_values(&self, name: &str, symbols: &SymbolTable) -> Vec<String> {
        self.sorted_label_values_with_limit(name, symbols, None)
    }

    /// Returns all `(name, value)` pairs present in postings sorted lexicographically.
    pub fn sorted_keys(&self, symbols: &SymbolTable) -> Vec<(String, String)> {
        let mut pairs: Vec<(SymbolId, SymbolId)> = Vec::new();
        if !self.all_postings.read().is_empty() {
            pairs.push(ALL_POSTINGS_KEY);
        }
        for s_rw in &self.stripes {
            let stripe = s_rw.read();
            for (&nid, vm) in &stripe.m {
                for &vid in vm.keys() {
                    pairs.push((nid, vid));
                }
            }
        }
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

fn append_random_sample_from_slices(list: &[u32], ov: &[u64], limit: usize, out: &mut Vec<u64>) {
    let total_len = list.len() + ov.len();
    if total_len == 0 {
        return;
    }
    let first_val = if !list.is_empty() {
        list[0] as u64
    } else {
        ov[0]
    };
    let mut seed = (total_len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ first_val;
    for _ in 0..limit {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let idx = ((seed >> 33) as usize) % total_len;
        if idx < list.len() {
            out.push(list[idx] as u64);
        } else {
            out.push(ov[idx - list.len()]);
        }
    }
}

impl PostingsStripe {
    fn sort_and_dedup(&mut self) {
        for values_map in self.m.values_mut() {
            for list in values_map.values_mut() {
                if list.len() > 1 {
                    list.sort_unstable();
                    list.dedup();
                }
            }
        }
        for list in self.overflow.values_mut() {
            if list.len() > 1 {
                list.sort_unstable();
                list.dedup();
            }
        }
    }

    fn pair_to_u64(&self, name_id: SymbolId, value_id: SymbolId) -> Vec<u64> {
        let u32_list = self.m.get(&name_id).and_then(|vm| vm.get(&value_id));
        let ov_list = if self.overflow.is_empty() {
            None
        } else {
            self.overflow.get(&(name_id, value_id))
        };
        let total = u32_list.map_or(0, Vec::len) + ov_list.map_or(0, Vec::len);
        if total == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(total);
        if let Some(l) = u32_list {
            out.extend(l.iter().map(|&x| x as u64));
        }
        if let Some(ov) = ov_list {
            out.extend_from_slice(ov);
        }
        out
    }

    fn add_for(&mut self, id: u64, name_id: SymbolId, value_id: SymbolId, ordered: bool) {
        let vm = self.m.entry(name_id).or_default();
        let list = match vm.get_mut(&value_id) {
            Some(list) => list,
            None => {
                self.lvs.entry(name_id).or_default().push(value_id);
                vm.entry(value_id).or_default()
            }
        };
        if let Ok(id32) = u32::try_from(id) {
            if !ordered || list.last().copied() < Some(id32) {
                list.push(id32);
            } else if let Err(pos) = list.binary_search(&id32) {
                list.insert(pos, id32);
            }
        } else {
            let ov = self.overflow.entry((name_id, value_id)).or_default();
            if !ordered || ov.last().copied() < Some(id) {
                ov.push(id);
            } else if let Err(pos) = ov.binary_search(&id) {
                ov.insert(pos, id);
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
        list.retain(|&id| !deleted.contains(&(id as u64)));
        let mut ov_empty = true;
        if !self.overflow.is_empty() {
            if let Some(ov) = self.overflow.get_mut(&(name_id, value_id)) {
                ov.retain(|id| !deleted.contains(id));
                if ov.is_empty() {
                    self.overflow.remove(&(name_id, value_id));
                } else {
                    ov_empty = false;
                }
            }
        }
        if list.is_empty() && ov_empty {
            vm.remove(&value_id);
            return true;
        }
        false
    }
}

fn merge_postings_u32_to_u64(lists: &[&[u32]]) -> Vec<u64> {
    if lists.is_empty() {
        return Vec::new();
    }
    if lists.len() == 1 {
        return lists[0].iter().map(|&x| x as u64).collect();
    }
    let total: usize = lists.iter().map(|s| s.len()).sum();
    let mut merged = Vec::with_capacity(total);
    for s in lists {
        merged.extend_from_slice(s);
    }
    merged.sort_unstable();
    merged.dedup();
    merged.into_iter().map(|x| x as u64).collect()
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
        if values.len() == 1 {
            return Ok(match self.symbols.get(values[0]) {
                Some(vid) => self.postings.postings_for_id(name_id, vid),
                None => Vec::new(),
            });
        }
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

    #[test]
    fn mem_postings_u32_and_u64_overflow_refs() {
        let mut syms = SymbolTable::new();
        let p = MemPostings::new_unordered();
        let big1 = (u32::MAX as u64) + 100;
        let big2 = (u32::MAX as u64) + 5;

        let s1 = SymbolLabels::from_pairs(&mut syms, [("__name__", "up"), ("job", "a")]);
        let s2 = SymbolLabels::from_pairs(&mut syms, [("__name__", "up"), ("job", "b")]);
        let s3 = SymbolLabels::from_pairs(&mut syms, [("__name__", "up"), ("job", "a")]);

        p.add(big1, &s1);
        p.add(42, &s2);
        p.add(big2, &s3);
        p.ensure_order();

        assert_eq!(p.all_postings(), vec![42, big2, big1]);
        let nid = syms.get("__name__").unwrap();
        let vid = syms.get("up").unwrap();
        assert_eq!(p.postings_len(nid, vid), 3);
        assert_eq!(p.first_posting(nid, vid), Some(42));
        assert_eq!(p.postings_for_all_values_id(nid), vec![42, big2, big1]);

        let mut del = HashSet::new();
        del.insert(big2);
        p.delete(&del, s3.as_slice());
        assert_eq!(p.all_postings(), vec![42, big1]);
    }
}
