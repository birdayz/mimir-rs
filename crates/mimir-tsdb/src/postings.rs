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

    /// Optional fast-path evaluator for [`postings_for_matchers`] (used by [`MemPostingsView`]
    /// to intersect/subtract `&[u32]` posting slices directly without intermediate `Vec<u64>`
    /// allocations).
    #[doc(hidden)]
    fn try_eval_matchers_fast(
        &self,
        _matchers: &[LabelMatcher],
    ) -> Option<Result<Vec<u64>, PostingsError>> {
        None
    }
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
        if self.small.is_empty() {
            self.small.shrink_to_fit();
        } else if self.small.capacity() > (self.small.len() * 4).max(1024) {
            self.small.shrink_to(self.small.len() * 2);
        }
        if !self.overflow.is_empty() {
            self.overflow.retain(|id| !deleted.contains(id));
            if self.overflow.is_empty() {
                self.overflow.shrink_to_fit();
            } else if self.overflow.capacity() > (self.overflow.len() * 4).max(256) {
                self.overflow.shrink_to(self.overflow.len() * 2);
            }
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
    /// Shared read lock held across `add` so a reader can barrier on `write()`
    /// between materialising intersecting base postings and resolving subtracting matchers.
    add_gate: RwLock<()>,
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
            add_gate: RwLock::new(()),
            ordered: AtomicBool::new(ordered),
        }
    }

    #[inline]
    fn stripe_idx(name_id: SymbolId) -> usize {
        (name_id.0 as usize).wrapping_mul(0x9E37_79B9) & (POSTINGS_STRIPES - 1)
    }

    /// Adds `series_ref` for all `(name_id, value_id)` pairs in `labels`, plus `ALL_POSTINGS_KEY`.
    pub fn add(&self, series_ref: u64, labels: &SymbolLabels) {
        let _gate = self.add_gate.read();
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
        let _gate = self.add_gate.read();
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

    /// Batch-inserts multiple `(ref_id, SymbolLabels)` slices concurrently across `pool`.
    pub fn add_batches_unordered_parallel(
        &self,
        batches: &[Vec<(u64, SymbolLabels)>],
        pool: &mimir_common::thread_pool::ScopedWorkerPool,
    ) {
        let non_empty: usize = batches.iter().map(Vec::len).sum();
        if non_empty == 0 {
            return;
        }
        if batches.len() <= 1 || non_empty < 256 {
            // `add_batch_unordered_with_scratch` takes `add_gate` itself. Never hold it here:
            // a recursive `parking_lot` read deadlocks once a reader has queued its
            // `add_gate.write()` barrier between the two acquisitions.
            let mut scratch = PostingsBatchScratch::new();
            for b in batches {
                self.add_batch_unordered_with_scratch(b, &mut scratch);
            }
            return;
        }
        // Held by this thread only: the pool workers below never touch `add_gate`.
        let _gate = self.add_gate.read();

        let ordered = self.ordered.load(AtomicOrdering::Acquire);
        let mut scratches: Vec<PostingsBatchScratch> =
            (0..batches.len()).map(|_| PostingsBatchScratch::new()).collect();

        // Phase 1: bucket each batch's `(ref_id, nid, vid)` into 64 stripe buckets in parallel.
        struct MutScratchPtr(*mut PostingsBatchScratch);
        impl MutScratchPtr {
            #[inline]
            #[allow(clippy::mut_from_ref)]
            unsafe fn get(&self, idx: usize) -> &mut PostingsBatchScratch {
                unsafe { &mut *self.0.add(idx) }
            }
        }
        unsafe impl Send for MutScratchPtr {}
        unsafe impl Sync for MutScratchPtr {}
        let s_ptr = MutScratchPtr(scratches.as_mut_ptr());
        let num_workers = batches.len().clamp(2, 16);

        pool.scoped_execute(batches.len(), |b_idx| {
            // SAFETY: each `b_idx` is distinct across the `batches.len()` jobs.
            let scratch = unsafe { s_ptr.get(b_idx) };
            for (ref_id, labels) in &batches[b_idx] {
                for &(nid, vid) in labels.as_slice() {
                    if (nid, vid) != ALL_POSTINGS_KEY {
                        scratch.buckets[Self::stripe_idx(nid)].push((*ref_id, nid, vid));
                    }
                }
            }
        });

        // Phase 2: apply all_postings (task 64) + 64 postings stripes (tasks 0..64) in parallel.
        let next_task = AtomicUsize::new(0);
        let total_tasks = POSTINGS_STRIPES + 1;
        let scratches_ref: &[PostingsBatchScratch] = &scratches;
        pool.scoped_execute(num_workers, |_| loop {
            let task = next_task.fetch_add(1, AtomicOrdering::Relaxed);
            if task >= total_tasks {
                break;
            }
            if task == POSTINGS_STRIPES {
                let mut all = self.all_postings.write();
                all.small.reserve(non_empty);
                for b in batches {
                    for &(ref_id, _) in b {
                        all.push(ref_id, ordered);
                    }
                }
            } else {
                let mut has_any = false;
                for sc in scratches_ref {
                    if !sc.buckets[task].is_empty() {
                        has_any = true;
                        break;
                    }
                }
                if has_any {
                    let mut stripe = self.stripes[task].write();
                    for sc in scratches_ref {
                        for &(ref_id, nid, vid) in &sc.buckets[task] {
                            stripe.add_for(ref_id, nid, vid, ordered);
                        }
                    }
                }
            }
        });
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
                } else if let Some(vm) = stripe.m.get_mut(&name_id) {
                    let vm_len = vm.len();
                    if vm.capacity() > (vm_len * 4).max(64) {
                        vm.shrink_to(vm_len * 2);
                    }
                    let lvs: Vec<SymbolId> = vm.keys().copied().collect();
                    stripe.lvs.insert(name_id, lvs);
                }
            }
            if stripe.m.is_empty() {
                stripe.m.shrink_to_fit();
                stripe.lvs.shrink_to_fit();
                stripe.overflow.shrink_to_fit();
            } else {
                let m_len = stripe.m.len();
                if stripe.m.capacity() > (m_len * 4).max(64) {
                    stripe.m.shrink_to(m_len * 2);
                }
                let lvs_len = stripe.lvs.len();
                if stripe.lvs.capacity() > (lvs_len * 4).max(64) {
                    stripe.lvs.shrink_to(lvs_len * 2);
                }
            }
        }
    }

    /// Returns `true` when `all_postings` is empty (zero indexed series).
    pub fn is_all_empty(&self) -> bool {
        self.all_postings.read().is_empty()
    }

    /// Clears and shrinks all stripe maps when zero series remain.
    pub fn clear_and_shrink(&self) {
        {
            let mut all = self.all_postings.write();
            all.small = Vec::new();
            all.overflow = Vec::new();
        }
        for s in &self.stripes {
            let mut st = s.write();
            st.m = HashMap::new();
            st.overflow = HashMap::new();
            st.lvs = HashMap::new();
        }
    }

    /// Marks every `SymbolId` currently referenced by live postings keys in `used`.
    pub fn collect_live_symbols(&self, used: &mut [bool]) {
        for s in &self.stripes {
            let st = s.read();
            for (&nid, vm) in &st.m {
                if let Some(slot) = used.get_mut(nid.0 as usize) {
                    *slot = true;
                }
                for &vid in vm.keys() {
                    if let Some(slot) = used.get_mut(vid.0 as usize) {
                        *slot = true;
                    }
                }
            }
            for &(nid, vid) in st.overflow.keys() {
                if let Some(slot) = used.get_mut(nid.0 as usize) {
                    *slot = true;
                }
                if let Some(slot) = used.get_mut(vid.0 as usize) {
                    *slot = true;
                }
            }
        }
    }

    /// Remaps all `SymbolId` keys across all 64 stripes according to `remap`, re-bucketing
    /// entries to their new `stripe_idx(new_name_id)` and shrinking stripe tables to fit.
    pub fn remap_symbols(&self, remap: &[SymbolId]) {
        let mut guards: SmallVec<[_; POSTINGS_STRIPES]> =
            self.stripes.iter().map(|s| s.write()).collect();
        let mut new_stripes: [PostingsStripe; POSTINGS_STRIPES] =
            std::array::from_fn(|_| PostingsStripe::default());

        for g in guards.iter_mut() {
            for (old_nid, vm) in g.m.drain() {
                let new_nid = remap[old_nid.0 as usize];
                let dst = &mut new_stripes[Self::stripe_idx(new_nid)];
                let dst_vm = dst.m.entry(new_nid).or_default();
                let dst_lvs = dst.lvs.entry(new_nid).or_default();
                dst_vm.reserve(vm.len());
                dst_lvs.reserve(vm.len());
                for (old_vid, list) in vm {
                    let new_vid = remap[old_vid.0 as usize];
                    dst_vm.insert(new_vid, list);
                    dst_lvs.push(new_vid);
                }
            }
            for ((old_nid, old_vid), ov) in g.overflow.drain() {
                let new_nid = remap[old_nid.0 as usize];
                let new_vid = remap[old_vid.0 as usize];
                new_stripes[Self::stripe_idx(new_nid)]
                    .overflow
                    .insert((new_nid, new_vid), ov);
            }
        }

        for (g, ns) in guards.iter_mut().zip(new_stripes) {
            **g = ns;
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
        if name_id == ALL_POSTINGS_KEY.0 {
            return if pred("") {
                self.all_postings()
            } else {
                Vec::new()
            };
        }
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
        if name.is_empty() {
            return if self.is_all_empty() {
                Vec::new()
            } else {
                vec![String::new()]
            };
        }
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
        if name.is_empty() {
            return if self.is_all_empty() {
                Vec::new()
            } else {
                vec![String::new()]
            };
        }
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

    /// Visits `(value_str, small_u32_slice, large_u64_slice)` for every value of `name`
    /// under a single postings stripe read lock with zero postings cloning.
    pub fn visit_all_values_postings_for_name(
        &self,
        name: &str,
        symbols: &SymbolTable,
        mut f: impl FnMut(&str, &[u32], &[u64]),
    ) {
        if !self.ordered.load(AtomicOrdering::Acquire) {
            self.ensure_order();
        }
        let Some(name_id) = symbols.get(name) else {
            return;
        };
        if name_id == ALL_POSTINGS_KEY.0 {
            return;
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let Some(vm) = stripe.m.get(&name_id) else {
            return;
        };
        for (&val_id, small) in vm {
            let val_str = symbols.resolve(val_id);
            let large = if stripe.overflow.is_empty() {
                &[]
            } else {
                stripe
                    .overflow
                    .get(&(name_id, val_id))
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
            };
            f(val_str, small.as_slice(), large);
        }
        if !stripe.overflow.is_empty() {
            for (&(nid, vid), large) in &stripe.overflow {
                if nid == name_id && !vm.contains_key(&vid) {
                    f(symbols.resolve(vid), &[], large.as_slice());
                }
            }
        }
    }

    /// Evaluates `matchers` directly on `MemPostings` `&[u32]` posting slices (widening to
    /// `Vec<u64>` only once for the final result), avoiding intermediate `Vec<u64>` allocations.
    pub fn postings_for_matchers(
        &self,
        symbols: &SymbolTable,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<u64>, PostingsError> {
        if matchers.is_empty() {
            return Ok(Vec::new());
        }
        if matchers.len() == 1 && matchers[0].name.is_empty() && matchers[0].value.is_empty() {
            return Ok(self.all_postings());
        }
        if !self.ordered.load(AtomicOrdering::Acquire) {
            self.ensure_order();
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
            matches!(m.match_type, MatchType::NotEqual | MatchType::RegexNotMatch)
                && m.matches_empty()
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

        let mut its: SmallVec<[MemTerm; 8]> = SmallVec::new();
        let mut intersecting: SmallVec<[&LabelMatcher; 8]> = SmallVec::new();
        let mut subtracting: SmallVec<[&LabelMatcher; 4]> = SmallVec::new();
        for m in matchers {
            if is_subtracting_matcher(m) {
                subtracting.push(m);
            } else {
                intersecting.push(m);
            }
        }

        if has_subtracting && !has_intersecting {
            let len = self.all_postings.read().len();
            if len == 0 {
                for m in &subtracting {
                    if m.name.is_empty() && m.value.is_empty() {
                        return Err(PostingsError::UnexpectedAllPostings);
                    }
                }
                return Ok(Vec::new());
            }
            its.push(MemTerm::AllPostings { len_hint: len });
        }

        // Phase 1: resolve and materialise all intersecting terms (and all_postings base)
        // BEFORE resolving any subtracting matcher.
        for m in intersecting {
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
                match self.resolve_all_values_term(symbols, &m.name) {
                    Some(t) => its.push(t),
                    None => return Ok(Vec::new()),
                }
                continue;
            }
            let is_not = matches!(m.match_type, MatchType::NotEqual | MatchType::RegexNotMatch);
            if is_not {
                let inv = m.inverse()?;
                match self.resolve_inverse_single_matcher_term(symbols, &inv) {
                    Some(t) => its.push(t),
                    None => return Ok(Vec::new()),
                }
            } else {
                match self.resolve_single_matcher_term(symbols, m) {
                    Some(t) => its.push(t),
                    None => return Ok(Vec::new()),
                }
            }
        }

        if its.is_empty() {
            return Ok(Vec::new());
        }

        // Start from the smallest intersecting term so larger terms intersect in-place
        // directly against `&[u32]` under their stripe read lock with zero intermediate allocs.
        its.sort_unstable_by_key(MemTerm::len_hint);

        let mut acc = PostingsAcc::from_term(self, &its[0]);
        for term in &its[1..] {
            if acc.is_empty() {
                break;
            }
            acc.intersect_term(self, term);
        }

        if acc.is_empty() {
            for m in &subtracting {
                if m.name.is_empty() && m.value.is_empty() {
                    return Err(PostingsError::UnexpectedAllPostings);
                }
            }
            return Ok(Vec::new());
        }

        // Phase 2: wait for any concurrent `add()` that could have contributed a ref to `acc`
        // to finish all its stripe writes before resolving subtracting matchers.
        if !subtracting.is_empty() {
            drop(self.add_gate.write());

            let mut not_its: SmallVec<[MemTerm; 4]> = SmallVec::new();
            for m in subtracting {
                if m.name.is_empty() && m.value.is_empty() {
                    return Err(PostingsError::UnexpectedAllPostings);
                }
                if m.match_type == MatchType::RegexMatch && m.value == ".*" {
                    continue;
                }
                if m.match_type == MatchType::RegexNotMatch && m.value == ".*" {
                    return Ok(Vec::new());
                }
                if m.match_type == MatchType::RegexNotMatch && m.value == ".+" {
                    if let Some(t) = self.resolve_all_values_term(symbols, &m.name) {
                        not_its.push(t);
                    }
                    continue;
                }
                if label_must_be_set.contains(m.name.as_str()) {
                    let inv = m.inverse()?;
                    if let Some(t) = self.resolve_single_matcher_term(symbols, &inv) {
                        not_its.push(t);
                    }
                } else if let Some(t) = self.resolve_inverse_single_matcher_term(symbols, m) {
                    not_its.push(t);
                }
            }

            for term in &not_its {
                if acc.is_empty() {
                    return Ok(Vec::new());
                }
                acc.subtract_term(self, term);
            }
        }
        Ok(acc.into_u64_vec())
    }

    fn resolve_single_matcher_term(&self, symbols: &SymbolTable, m: &LabelMatcher) -> Option<MemTerm> {
        if m.match_type == MatchType::Equal {
            return self.resolve_single_value_term(symbols, &m.name, &m.value);
        }
        if m.match_type == MatchType::RegexMatch {
            if let Some(set) = m.set_matches() {
                return self.resolve_set_values_term(symbols, &m.name, set);
            }
        }
        self.resolve_matching_term(symbols, &m.name, |v| m.matches(v))
    }

    fn resolve_inverse_single_matcher_term(
        &self,
        symbols: &SymbolTable,
        m: &LabelMatcher,
    ) -> Option<MemTerm> {
        if m.match_type == MatchType::RegexNotMatch {
            if let Some(set) = m.set_matches() {
                return self.resolve_set_values_term(symbols, &m.name, set);
            }
        }
        if m.match_type == MatchType::NotEqual {
            return self.resolve_single_value_term(symbols, &m.name, &m.value);
        }
        if m.value.is_empty() && matches!(m.match_type, MatchType::RegexMatch | MatchType::Equal) {
            return self.resolve_all_values_term(symbols, &m.name);
        }
        self.resolve_matching_term(symbols, &m.name, |v| !m.matches(v))
    }

    fn resolve_single_value_term(
        &self,
        symbols: &SymbolTable,
        name: &str,
        value: &str,
    ) -> Option<MemTerm> {
        let name_id = symbols.get(name)?;
        let value_id = symbols.get(value)?;
        if (name_id, value_id) == ALL_POSTINGS_KEY {
            let len = self.all_postings.read().len();
            return (len > 0).then_some(MemTerm::AllPostings { len_hint: len });
        }
        if name_id == ALL_POSTINGS_KEY.0 {
            return None;
        }
        let len = self.postings_len(name_id, value_id);
        if len == 0 {
            None
        } else {
            Some(MemTerm::SinglePair {
                name_id,
                value_id,
                len_hint: len,
            })
        }
    }

    fn resolve_set_values_term(
        &self,
        symbols: &SymbolTable,
        name: &str,
        values: &[String],
    ) -> Option<MemTerm> {
        if values.is_empty() {
            return None;
        }
        if values.len() == 1 {
            return self.resolve_single_value_term(symbols, name, &values[0]);
        }
        let name_id = symbols.get(name)?;
        if name_id == ALL_POSTINGS_KEY.0 {
            let has_empty = values
                .iter()
                .any(|v| symbols.get(v) == Some(ALL_POSTINGS_KEY.1));
            if has_empty {
                let len = self.all_postings.read().len();
                return (len > 0).then_some(MemTerm::AllPostings { len_hint: len });
            }
            return None;
        }
        let mut value_ids: SmallVec<[SymbolId; 8]> = SmallVec::with_capacity(values.len());
        for v in values {
            if let Some(vid) = symbols.get(v) {
                if !value_ids.contains(&vid) {
                    value_ids.push(vid);
                }
            }
        }
        if value_ids.is_empty() {
            return None;
        }
        if value_ids.len() == 1 {
            let vid = value_ids[0];
            let len = self.postings_len(name_id, vid);
            return (len > 0).then_some(MemTerm::SinglePair {
                name_id,
                value_id: vid,
                len_hint: len,
            });
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let vm = stripe.m.get(&name_id)?;
        let mut non_empty_ids: SmallVec<[SymbolId; 8]> = SmallVec::with_capacity(value_ids.len());
        let mut len_hint = 0usize;
        for vid in value_ids {
            let s_len = vm.get(&vid).map_or(0, Vec::len);
            let o_len = if stripe.overflow.is_empty() {
                0
            } else {
                stripe.overflow.get(&(name_id, vid)).map_or(0, Vec::len)
            };
            let total = s_len + o_len;
            if total > 0 {
                len_hint += total;
                non_empty_ids.push(vid);
            }
        }
        if non_empty_ids.is_empty() {
            None
        } else if non_empty_ids.len() == 1 {
            Some(MemTerm::SinglePair {
                name_id,
                value_id: non_empty_ids[0],
                len_hint,
            })
        } else {
            Some(MemTerm::MultiValues {
                name_id,
                value_ids: non_empty_ids,
                len_hint,
            })
        }
    }

    fn resolve_all_values_term(&self, symbols: &SymbolTable, name: &str) -> Option<MemTerm> {
        let name_id = symbols.get(name)?;
        if name_id == ALL_POSTINGS_KEY.0 {
            let len = self.all_postings.read().len();
            return (len > 0).then_some(MemTerm::AllPostings { len_hint: len });
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let vm = stripe.m.get(&name_id)?;
        if vm.len() == 1 && stripe.overflow.is_empty() {
            if let Some((&vid, list)) = vm.iter().next() {
                return (!list.is_empty()).then_some(MemTerm::SinglePair {
                    name_id,
                    value_id: vid,
                    len_hint: list.len(),
                });
            }
        }
        let mut len_hint = 0usize;
        for list in vm.values() {
            len_hint += list.len();
        }
        if !stripe.overflow.is_empty() {
            for (&(nid, _), ov) in &stripe.overflow {
                if nid == name_id {
                    len_hint += ov.len();
                }
            }
        }
        (len_hint > 0).then_some(MemTerm::AllValuesForName { name_id, len_hint })
    }

    fn resolve_matching_term(
        &self,
        symbols: &SymbolTable,
        name: &str,
        mut pred: impl FnMut(&str) -> bool,
    ) -> Option<MemTerm> {
        let name_id = symbols.get(name)?;
        if name_id == ALL_POSTINGS_KEY.0 {
            if !pred("") {
                return None;
            }
            let len = self.all_postings.read().len();
            return (len > 0).then_some(MemTerm::AllPostings { len_hint: len });
        }
        let stripe = self.stripes[Self::stripe_idx(name_id)].read();
        let lvs = stripe.lvs.get(&name_id)?;
        let vm = stripe.m.get(&name_id)?;
        let mut value_ids: SmallVec<[SymbolId; 8]> = SmallVec::new();
        let mut len_hint = 0usize;
        for &vid in lvs {
            if pred(symbols.resolve(vid)) {
                let s_len = vm.get(&vid).map_or(0, Vec::len);
                let o_len = if stripe.overflow.is_empty() {
                    0
                } else {
                    stripe.overflow.get(&(name_id, vid)).map_or(0, Vec::len)
                };
                let total = s_len + o_len;
                if total > 0 {
                    len_hint += total;
                    value_ids.push(vid);
                }
            }
        }
        if value_ids.is_empty() {
            None
        } else if value_ids.len() == 1 {
            Some(MemTerm::SinglePair {
                name_id,
                value_id: value_ids[0],
                len_hint,
            })
        } else {
            Some(MemTerm::MultiValues {
                name_id,
                value_ids,
                len_hint,
            })
        }
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

fn merge_postings_u32(lists: &[&[u32]]) -> Vec<u32> {
    if lists.is_empty() {
        return Vec::new();
    }
    if lists.len() == 1 {
        return lists[0].to_vec();
    }
    let total: usize = lists.iter().map(|s| s.len()).sum();
    let mut merged = Vec::with_capacity(total);
    for s in lists {
        merged.extend_from_slice(s);
    }
    merged.sort_unstable();
    merged.dedup();
    merged
}

fn merge_postings_u32_to_u64(lists: &[&[u32]]) -> Vec<u64> {
    if lists.is_empty() {
        return Vec::new();
    }
    if lists.len() == 1 {
        return lists[0].iter().map(|&x| x as u64).collect();
    }
    merge_postings_u32(lists)
        .into_iter()
        .map(|x| x as u64)
        .collect()
}

enum MemTerm {
    AllPostings {
        len_hint: usize,
    },
    SinglePair {
        name_id: SymbolId,
        value_id: SymbolId,
        len_hint: usize,
    },
    MultiValues {
        name_id: SymbolId,
        value_ids: SmallVec<[SymbolId; 8]>,
        len_hint: usize,
    },
    AllValuesForName {
        name_id: SymbolId,
        len_hint: usize,
    },
}

impl MemTerm {
    #[inline]
    fn len_hint(&self) -> usize {
        match *self {
            Self::AllPostings { len_hint }
            | Self::SinglePair { len_hint, .. }
            | Self::MultiValues { len_hint, .. }
            | Self::AllValuesForName { len_hint, .. } => len_hint,
        }
    }
}

struct PostingsAcc {
    small: Vec<u32>,
    large: Vec<u64>,
}

impl PostingsAcc {
    #[inline]
    fn is_empty(&self) -> bool {
        self.small.is_empty() && self.large.is_empty()
    }

    fn into_u64_vec(self) -> Vec<u64> {
        let mut out = Vec::with_capacity(self.small.len() + self.large.len());
        out.extend(self.small.iter().map(|&x| x as u64));
        out.extend_from_slice(&self.large);
        out
    }

    fn from_term(postings: &MemPostings, term: &MemTerm) -> Self {
        match term {
            MemTerm::AllPostings { .. } => {
                let all = postings.all_postings.read();
                Self {
                    small: all.small.clone(),
                    large: all.overflow.clone(),
                }
            }
            MemTerm::SinglePair {
                name_id, value_id, ..
            } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                let small = stripe
                    .m
                    .get(name_id)
                    .and_then(|vm| vm.get(value_id))
                    .cloned()
                    .unwrap_or_default();
                let large = if stripe.overflow.is_empty() {
                    Vec::new()
                } else {
                    stripe
                        .overflow
                        .get(&(*name_id, *value_id))
                        .cloned()
                        .unwrap_or_default()
                };
                Self { small, large }
            }
            MemTerm::MultiValues {
                name_id, value_ids, ..
            } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                let small = match stripe.m.get(name_id) {
                    Some(vm) => {
                        let lists: SmallVec<[&[u32]; 16]> = value_ids
                            .iter()
                            .filter_map(|vid| vm.get(vid).map(Vec::as_slice))
                            .filter(|s| !s.is_empty())
                            .collect();
                        merge_postings_u32(&lists)
                    }
                    None => Vec::new(),
                };
                let large = if stripe.overflow.is_empty() {
                    Vec::new()
                } else {
                    let ov_lists: SmallVec<[&[u64]; 8]> = value_ids
                        .iter()
                        .filter_map(|vid| stripe.overflow.get(&(*name_id, *vid)).map(Vec::as_slice))
                        .filter(|s| !s.is_empty())
                        .collect();
                    merge_postings(&ov_lists)
                };
                Self { small, large }
            }
            MemTerm::AllValuesForName { name_id, .. } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                let small = match stripe.m.get(name_id) {
                    Some(vm) => {
                        let lists: Vec<&[u32]> = vm
                            .values()
                            .map(Vec::as_slice)
                            .filter(|s| !s.is_empty())
                            .collect();
                        merge_postings_u32(&lists)
                    }
                    None => Vec::new(),
                };
                let large = if stripe.overflow.is_empty() {
                    Vec::new()
                } else {
                    let ov_lists: Vec<&[u64]> = stripe
                        .overflow
                        .iter()
                        .filter(|(&(nid, _), ov)| nid == *name_id && !ov.is_empty())
                        .map(|(_, ov)| ov.as_slice())
                        .collect();
                    merge_postings(&ov_lists)
                };
                Self { small, large }
            }
        }
    }

    fn intersect_term(&mut self, postings: &MemPostings, term: &MemTerm) {
        match term {
            MemTerm::AllPostings { .. } => {
                let all = postings.all_postings.read();
                intersect_u32_in_place(&mut self.small, &all.small);
                if !self.large.is_empty() {
                    intersect_u64_in_place(&mut self.large, &all.overflow);
                }
            }
            MemTerm::SinglePair {
                name_id, value_id, ..
            } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                if !self.small.is_empty() {
                    let other_small = stripe
                        .m
                        .get(name_id)
                        .and_then(|vm| vm.get(value_id))
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    intersect_u32_in_place(&mut self.small, other_small);
                }
                if !self.large.is_empty() {
                    let other_large = if stripe.overflow.is_empty() {
                        &[]
                    } else {
                        stripe
                            .overflow
                            .get(&(*name_id, *value_id))
                            .map(Vec::as_slice)
                            .unwrap_or(&[])
                    };
                    intersect_u64_in_place(&mut self.large, other_large);
                }
            }
            MemTerm::MultiValues {
                name_id, value_ids, ..
            } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                if !self.small.is_empty() {
                    let lists: SmallVec<[&[u32]; 16]> = stripe
                        .m
                        .get(name_id)
                        .map(|vm| {
                            value_ids
                                .iter()
                                .filter_map(|vid| vm.get(vid).map(Vec::as_slice))
                                .filter(|s| !s.is_empty())
                                .collect()
                        })
                        .unwrap_or_default();
                    intersect_u32_union_in_place(&mut self.small, &lists);
                }
                if !self.large.is_empty() {
                    if stripe.overflow.is_empty() {
                        self.large.clear();
                    } else {
                        let ov_lists: SmallVec<[&[u64]; 8]> = value_ids
                            .iter()
                            .filter_map(|vid| {
                                stripe.overflow.get(&(*name_id, *vid)).map(Vec::as_slice)
                            })
                            .filter(|s| !s.is_empty())
                            .collect();
                        let merged = merge_postings(&ov_lists);
                        intersect_u64_in_place(&mut self.large, &merged);
                    }
                }
            }
            MemTerm::AllValuesForName { name_id, .. } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                if !self.small.is_empty() {
                    let lists: Vec<&[u32]> = stripe
                        .m
                        .get(name_id)
                        .map(|vm| {
                            vm.values()
                                .map(Vec::as_slice)
                                .filter(|s| !s.is_empty())
                                .collect()
                        })
                        .unwrap_or_default();
                    intersect_u32_union_in_place(&mut self.small, &lists);
                }
                if !self.large.is_empty() {
                    if stripe.overflow.is_empty() {
                        self.large.clear();
                    } else {
                        let ov_lists: Vec<&[u64]> = stripe
                            .overflow
                            .iter()
                            .filter(|(&(nid, _), ov)| nid == *name_id && !ov.is_empty())
                            .map(|(_, ov)| ov.as_slice())
                            .collect();
                        let merged = merge_postings(&ov_lists);
                        intersect_u64_in_place(&mut self.large, &merged);
                    }
                }
            }
        }
    }

    fn subtract_term(&mut self, postings: &MemPostings, term: &MemTerm) {
        match term {
            MemTerm::AllPostings { .. } => {
                let all = postings.all_postings.read();
                without_u32_in_place(&mut self.small, &all.small);
                if !self.large.is_empty() && !all.overflow.is_empty() {
                    without_u64_in_place(&mut self.large, &all.overflow);
                }
            }
            MemTerm::SinglePair {
                name_id, value_id, ..
            } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                if !self.small.is_empty() {
                    if let Some(other_small) =
                        stripe.m.get(name_id).and_then(|vm| vm.get(value_id))
                    {
                        without_u32_in_place(&mut self.small, other_small);
                    }
                }
                if !self.large.is_empty() && !stripe.overflow.is_empty() {
                    if let Some(other_large) = stripe.overflow.get(&(*name_id, *value_id)) {
                        without_u64_in_place(&mut self.large, other_large);
                    }
                }
            }
            MemTerm::MultiValues {
                name_id, value_ids, ..
            } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                if !self.small.is_empty() {
                    let lists: SmallVec<[&[u32]; 16]> = stripe
                        .m
                        .get(name_id)
                        .map(|vm| {
                            value_ids
                                .iter()
                                .filter_map(|vid| vm.get(vid).map(Vec::as_slice))
                                .filter(|s| !s.is_empty())
                                .collect()
                        })
                        .unwrap_or_default();
                    without_u32_union_in_place(&mut self.small, &lists);
                }
                if !self.large.is_empty() && !stripe.overflow.is_empty() {
                    let ov_lists: SmallVec<[&[u64]; 8]> = value_ids
                        .iter()
                        .filter_map(|vid| stripe.overflow.get(&(*name_id, *vid)).map(Vec::as_slice))
                        .filter(|s| !s.is_empty())
                        .collect();
                    if !ov_lists.is_empty() {
                        let merged = merge_postings(&ov_lists);
                        without_u64_in_place(&mut self.large, &merged);
                    }
                }
            }
            MemTerm::AllValuesForName { name_id, .. } => {
                let stripe = postings.stripes[MemPostings::stripe_idx(*name_id)].read();
                if !self.small.is_empty() {
                    let lists: Vec<&[u32]> = stripe
                        .m
                        .get(name_id)
                        .map(|vm| {
                            vm.values()
                                .map(Vec::as_slice)
                                .filter(|s| !s.is_empty())
                                .collect()
                        })
                        .unwrap_or_default();
                    without_u32_union_in_place(&mut self.small, &lists);
                }
                if !self.large.is_empty() && !stripe.overflow.is_empty() {
                    let ov_lists: Vec<&[u64]> = stripe
                        .overflow
                        .iter()
                        .filter(|(&(nid, _), ov)| nid == *name_id && !ov.is_empty())
                        .map(|(_, ov)| ov.as_slice())
                        .collect();
                    if !ov_lists.is_empty() {
                        let merged = merge_postings(&ov_lists);
                        without_u64_in_place(&mut self.large, &merged);
                    }
                }
            }
        }
    }
}

#[inline]
fn intersect_u32_union_in_place(acc: &mut Vec<u32>, lists: &[&[u32]]) {
    if acc.is_empty() || lists.is_empty() {
        acc.clear();
        return;
    }
    if lists.len() == 1 {
        intersect_u32_in_place(acc, lists[0]);
        return;
    }
    let total_len: usize = lists.iter().map(|s| s.len()).sum();
    if acc.len().saturating_mul(lists.len()).saturating_mul(8) < total_len {
        acc.retain(|x| lists.iter().any(|s| s.binary_search(x).is_ok()));
    } else {
        let merged = merge_postings_u32(lists);
        intersect_u32_in_place(acc, &merged);
    }
}

#[inline]
fn without_u32_union_in_place(acc: &mut Vec<u32>, lists: &[&[u32]]) {
    if acc.is_empty() || lists.is_empty() {
        return;
    }
    if lists.len() == 1 {
        without_u32_in_place(acc, lists[0]);
        return;
    }
    let total_len: usize = lists.iter().map(|s| s.len()).sum();
    if acc.len().saturating_mul(lists.len()).saturating_mul(8) < total_len {
        acc.retain(|x| !lists.iter().any(|s| s.binary_search(x).is_ok()));
    } else {
        let merged = merge_postings_u32(lists);
        without_u32_in_place(acc, &merged);
    }
}

#[inline]
fn intersect_u32_in_place(acc: &mut Vec<u32>, other: &[u32]) {
    if acc.is_empty() || other.is_empty() {
        acc.clear();
        return;
    }
    let min_a = acc[0];
    let max_a = acc[acc.len() - 1];
    let min_b = other[0];
    let max_b = other[other.len() - 1];
    if max_a < min_b || min_a > max_b {
        acc.clear();
        return;
    }
    if other.len() > acc.len().saturating_mul(8) {
        let mut write = 0usize;
        let mut lo = if min_a > min_b {
            other.partition_point(|&x| x < min_a)
        } else {
            0
        };
        for read in 0..acc.len() {
            let x = acc[read];
            if x > max_b {
                break;
            }
            match other[lo..].binary_search(&x) {
                Ok(idx) => {
                    acc[write] = x;
                    write += 1;
                    lo += idx + 1;
                }
                Err(idx) => {
                    lo += idx;
                }
            }
            if lo >= other.len() {
                break;
            }
        }
        acc.truncate(write);
    } else if acc.len() > other.len().saturating_mul(8) {
        let mut write = 0usize;
        let mut lo = if min_b > min_a {
            acc.partition_point(|&x| x < min_b)
        } else {
            0
        };
        for &y in other {
            if y > max_a {
                break;
            }
            match acc[lo..].binary_search(&y) {
                Ok(idx) => {
                    acc[write] = y;
                    write += 1;
                    lo += idx + 1;
                }
                Err(idx) => {
                    lo += idx;
                }
            }
            if lo >= acc.len() {
                break;
            }
        }
        acc.truncate(write);
    } else {
        let mut write = 0usize;
        let mut i = if min_a < min_b {
            acc.partition_point(|&x| x < min_b)
        } else {
            0
        };
        let mut j = if min_b < min_a {
            other.partition_point(|&x| x < min_a)
        } else {
            0
        };
        while i < acc.len() && j < other.len() {
            let a = acc[i];
            let b = other[j];
            match a.cmp(&b) {
                Ordering::Less => i += 1,
                Ordering::Greater => j += 1,
                Ordering::Equal => {
                    acc[write] = a;
                    write += 1;
                    i += 1;
                    j += 1;
                }
            }
        }
        acc.truncate(write);
    }
}

#[inline]
fn intersect_u64_in_place(acc: &mut Vec<u64>, other: &[u64]) {
    if acc.is_empty() || other.is_empty() {
        acc.clear();
        return;
    }
    let min_a = acc[0];
    let max_a = acc[acc.len() - 1];
    let min_b = other[0];
    let max_b = other[other.len() - 1];
    if max_a < min_b || min_a > max_b {
        acc.clear();
        return;
    }
    let mut write = 0usize;
    let mut i = if min_a < min_b {
        acc.partition_point(|&x| x < min_b)
    } else {
        0
    };
    let mut j = if min_b < min_a {
        other.partition_point(|&x| x < min_a)
    } else {
        0
    };
    while i < acc.len() && j < other.len() {
        match acc[i].cmp(&other[j]) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                acc[write] = acc[i];
                write += 1;
                i += 1;
                j += 1;
            }
        }
    }
    acc.truncate(write);
}

#[inline]
fn without_u32_in_place(acc: &mut Vec<u32>, drop: &[u32]) {
    if acc.is_empty() || drop.is_empty() {
        return;
    }
    let min_a = acc[0];
    let max_a = acc[acc.len() - 1];
    let min_d = drop[0];
    let max_d = drop[drop.len() - 1];
    if max_a < min_d || min_a > max_d {
        return;
    }
    if drop.len() > acc.len().saturating_mul(8) {
        let mut write = 0usize;
        let mut lo = if min_a > min_d {
            drop.partition_point(|&x| x < min_a)
        } else {
            0
        };
        for read in 0..acc.len() {
            let x = acc[read];
            if x > max_d {
                let rem = acc.len() - read;
                acc.copy_within(read.., write);
                write += rem;
                break;
            }
            match drop[lo..].binary_search(&x) {
                Ok(idx) => {
                    lo += idx + 1;
                }
                Err(idx) => {
                    acc[write] = x;
                    write += 1;
                    lo += idx;
                }
            }
            if lo >= drop.len() {
                let next_read = read + 1;
                if next_read < acc.len() {
                    let rem = acc.len() - next_read;
                    acc.copy_within(next_read.., write);
                    write += rem;
                }
                break;
            }
        }
        acc.truncate(write);
    } else {
        let mut i = if min_a < min_d {
            acc.partition_point(|&x| x < min_d)
        } else {
            0
        };
        if i >= acc.len() {
            return;
        }
        let mut write = i;
        let mut j = if min_d < acc[i] {
            drop.partition_point(|&x| x < acc[i])
        } else {
            0
        };
        while i < acc.len() && j < drop.len() {
            match acc[i].cmp(&drop[j]) {
                Ordering::Less => {
                    acc[write] = acc[i];
                    write += 1;
                    i += 1;
                }
                Ordering::Greater => j += 1,
                Ordering::Equal => {
                    i += 1;
                    j += 1;
                }
            }
        }
        if i < acc.len() {
            let rem = acc.len() - i;
            acc.copy_within(i.., write);
            write += rem;
        }
        acc.truncate(write);
    }
}

#[inline]
fn without_u64_in_place(acc: &mut Vec<u64>, drop: &[u64]) {
    if acc.is_empty() || drop.is_empty() {
        return;
    }
    let min_a = acc[0];
    let max_a = acc[acc.len() - 1];
    let min_d = drop[0];
    let max_d = drop[drop.len() - 1];
    if max_a < min_d || min_a > max_d {
        return;
    }
    let mut i = if min_a < min_d {
        acc.partition_point(|&x| x < min_d)
    } else {
        0
    };
    if i >= acc.len() {
        return;
    }
    let mut write = i;
    let mut j = if min_d < acc[i] {
        drop.partition_point(|&x| x < acc[i])
    } else {
        0
    };
    while i < acc.len() && j < drop.len() {
        match acc[i].cmp(&drop[j]) {
            Ordering::Less => {
                acc[write] = acc[i];
                write += 1;
                i += 1;
            }
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    if i < acc.len() {
        let rem = acc.len() - i;
        acc.copy_within(i.., write);
        write += rem;
    }
    acc.truncate(write);
}

/// Counts elements present in both sorted `small: &[u32]` and sorted `eligible: &[u64]`.
#[inline]
pub fn intersect_sorted_u32_u64_count(small: &[u32], eligible: &[u64]) -> u64 {
    if small.is_empty() || eligible.is_empty() {
        return 0;
    }
    let min_s = small[0] as u64;
    let max_s = small[small.len() - 1] as u64;
    let min_e = eligible[0];
    let max_e = eligible[eligible.len() - 1];
    if max_s < min_e || min_s > max_e {
        return 0;
    }
    let mut count = 0u64;
    if small.len().saturating_mul(16) < eligible.len() {
        let mut lo = if min_s > min_e {
            eligible.partition_point(|&x| x < min_s)
        } else {
            0
        };
        for &s in small {
            let v = s as u64;
            if v > max_e {
                break;
            }
            match eligible[lo..].binary_search(&v) {
                Ok(pos) => {
                    count += 1;
                    lo += pos + 1;
                }
                Err(pos) => {
                    lo += pos;
                }
            }
        }
        return count;
    }
    if eligible.len().saturating_mul(16) < small.len() {
        let mut lo = if min_e > min_s {
            small.partition_point(|&x| (x as u64) < min_e)
        } else {
            0
        };
        for &e in eligible {
            if e > max_s {
                break;
            }
            let Ok(e32) = u32::try_from(e) else {
                break;
            };
            match small[lo..].binary_search(&e32) {
                Ok(pos) => {
                    count += 1;
                    lo += pos + 1;
                }
                Err(pos) => {
                    lo += pos;
                }
            }
        }
        return count;
    }
    let mut i = if min_s < min_e {
        small.partition_point(|&x| (x as u64) < min_e)
    } else {
        0
    };
    let mut j = if min_e < min_s {
        eligible.partition_point(|&x| x < min_s)
    } else {
        0
    };
    while i < small.len() && j < eligible.len() {
        let a = small[i] as u64;
        let b = eligible[j];
        match a.cmp(&b) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                count += 1;
                i += 1;
                j += 1;
            }
        }
    }
    count
}

/// Counts elements present in both sorted `large: &[u64]` and sorted `eligible: &[u64]`.
#[inline]
pub fn intersect_sorted_u64_count(large: &[u64], eligible: &[u64]) -> u64 {
    if large.is_empty() || eligible.is_empty() {
        return 0;
    }
    let min_l = large[0];
    let max_l = large[large.len() - 1];
    let min_e = eligible[0];
    let max_e = eligible[eligible.len() - 1];
    if max_l < min_e || min_l > max_e {
        return 0;
    }
    let mut count = 0u64;
    if large.len().saturating_mul(16) < eligible.len() {
        let mut lo = if min_l > min_e {
            eligible.partition_point(|&x| x < min_l)
        } else {
            0
        };
        for &v in large {
            if v > max_e {
                break;
            }
            match eligible[lo..].binary_search(&v) {
                Ok(pos) => {
                    count += 1;
                    lo += pos + 1;
                }
                Err(pos) => {
                    lo += pos;
                }
            }
        }
        return count;
    }
    if eligible.len().saturating_mul(16) < large.len() {
        let mut lo = if min_e > min_l {
            large.partition_point(|&x| x < min_e)
        } else {
            0
        };
        for &e in eligible {
            if e > max_l {
                break;
            }
            match large[lo..].binary_search(&e) {
                Ok(pos) => {
                    count += 1;
                    lo += pos + 1;
                }
                Err(pos) => {
                    lo += pos;
                }
            }
        }
        return count;
    }
    let mut i = if min_l < min_e {
        large.partition_point(|&x| x < min_e)
    } else {
        0
    };
    let mut j = if min_e < min_l {
        eligible.partition_point(|&x| x < min_l)
    } else {
        0
    };
    while i < large.len() && j < eligible.len() {
        let a = large[i];
        let b = eligible[j];
        match a.cmp(&b) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                count += 1;
                i += 1;
                j += 1;
            }
        }
    }
    count
}

/// Counts total matching refs across disjoint `(small, large)` postings slices and `eligible`.
#[inline]
pub fn intersect_sorted_count(small: &[u32], large: &[u64], eligible: &[u64]) -> u64 {
    intersect_sorted_u32_u64_count(small, eligible) + intersect_sorted_u64_count(large, eligible)
}

/// Returns `true` if sorted `small: &[u32]` and sorted `eligible: &[u64]` share at least one ref.
#[inline]
pub fn intersect_sorted_u32_u64_any(small: &[u32], eligible: &[u64]) -> bool {
    if small.is_empty() || eligible.is_empty() {
        return false;
    }
    let min_s = small[0] as u64;
    let max_s = small[small.len() - 1] as u64;
    let min_e = eligible[0];
    let max_e = eligible[eligible.len() - 1];
    if max_s < min_e || min_s > max_e {
        return false;
    }
    if small.len().saturating_mul(16) < eligible.len() {
        let mut lo = if min_s > min_e {
            eligible.partition_point(|&x| x < min_s)
        } else {
            0
        };
        for &s in small {
            let v = s as u64;
            if v > max_e {
                break;
            }
            match eligible[lo..].binary_search(&v) {
                Ok(_) => return true,
                Err(pos) => lo += pos,
            }
        }
        return false;
    }
    if eligible.len().saturating_mul(16) < small.len() {
        let mut lo = if min_e > min_s {
            small.partition_point(|&x| (x as u64) < min_e)
        } else {
            0
        };
        for &e in eligible {
            if e > max_s {
                break;
            }
            let Ok(e32) = u32::try_from(e) else {
                break;
            };
            match small[lo..].binary_search(&e32) {
                Ok(_) => return true,
                Err(pos) => lo += pos,
            }
        }
        return false;
    }
    let mut i = if min_s < min_e {
        small.partition_point(|&x| (x as u64) < min_e)
    } else {
        0
    };
    let mut j = if min_e < min_s {
        eligible.partition_point(|&x| x < min_s)
    } else {
        0
    };
    while i < small.len() && j < eligible.len() {
        let a = small[i] as u64;
        let b = eligible[j];
        match a.cmp(&b) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => return true,
        }
    }
    false
}

/// Returns `true` if sorted `large: &[u64]` and sorted `eligible: &[u64]` share at least one ref.
#[inline]
pub fn intersect_sorted_u64_any(large: &[u64], eligible: &[u64]) -> bool {
    if large.is_empty() || eligible.is_empty() {
        return false;
    }
    let min_l = large[0];
    let max_l = large[large.len() - 1];
    let min_e = eligible[0];
    let max_e = eligible[eligible.len() - 1];
    if max_l < min_e || min_l > max_e {
        return false;
    }
    if large.len().saturating_mul(16) < eligible.len() {
        let mut lo = if min_l > min_e {
            eligible.partition_point(|&x| x < min_l)
        } else {
            0
        };
        for &v in large {
            if v > max_e {
                break;
            }
            match eligible[lo..].binary_search(&v) {
                Ok(_) => return true,
                Err(pos) => lo += pos,
            }
        }
        return false;
    }
    if eligible.len().saturating_mul(16) < large.len() {
        let mut lo = if min_e > min_l {
            large.partition_point(|&x| x < min_e)
        } else {
            0
        };
        for &e in eligible {
            if e > max_l {
                break;
            }
            match large[lo..].binary_search(&e) {
                Ok(_) => return true,
                Err(pos) => lo += pos,
            }
        }
        return false;
    }
    let mut i = if min_l < min_e {
        large.partition_point(|&x| x < min_e)
    } else {
        0
    };
    let mut j = if min_e < min_l {
        eligible.partition_point(|&x| x < min_l)
    } else {
        0
    };
    while i < large.len() && j < eligible.len() {
        let a = large[i];
        let b = eligible[j];
        match a.cmp(&b) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => return true,
        }
    }
    false
}

/// Returns `true` if `(small, large)` postings slices and `eligible` share at least one ref.
#[inline]
pub fn intersect_sorted_any(small: &[u32], large: &[u64], eligible: &[u64]) -> bool {
    intersect_sorted_u32_u64_any(small, eligible) || intersect_sorted_u64_any(large, eligible)
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

    fn try_eval_matchers_fast(
        &self,
        matchers: &[LabelMatcher],
    ) -> Option<Result<Vec<u64>, PostingsError>> {
        Some(self.postings.postings_for_matchers(self.symbols, matchers))
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
    if let Some(res) = source.try_eval_matchers_fast(matchers) {
        return res;
    }
    postings_for_matchers_generic(source, matchers)
}

fn postings_for_matchers_generic<S: PostingsSource + ?Sized>(
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

    #[test]
    fn visit_all_values_and_galloping_helpers() {
        let mut syms = SymbolTable::new();
        let p = MemPostings::new_unordered();
        let big = (u32::MAX as u64) + 77;
        let n = if cfg!(miri) { 40u64 } else { 200u64 };
        for i in 1..=n {
            let lbls = SymbolLabels::from_pairs(
                &mut syms,
                [
                    ("__name__", "req"),
                    ("env", if i % 4 == 0 { "prod" } else { "dev" }),
                    ("shard", &format!("s-{}", i % 10)),
                ],
            );
            p.add(i, &lbls);
        }
        let lbl_big = SymbolLabels::from_pairs(
            &mut syms,
            [("__name__", "req"), ("env", "staging"), ("shard", "s-0")],
        );
        p.add(big, &lbl_big);
        p.ensure_order();

        let mut visited: std::collections::BTreeMap<String, (Vec<u32>, Vec<u64>)> =
            std::collections::BTreeMap::new();
        p.visit_all_values_postings_for_name("env", &syms, |val, small, large| {
            visited.insert(val.to_string(), (small.to_vec(), large.to_vec()));
        });
        assert_eq!(visited.len(), 3);
        assert_eq!(visited["prod"].0.len(), (n / 4) as usize);
        assert_eq!(visited["dev"].0.len(), (n - n / 4) as usize);
        assert!(visited["staging"].0.is_empty());
        assert_eq!(visited["staging"].1, vec![big]);

        // Test galloping count & any across skewed, comparable, and overflow inputs
        let elig_small = [4u64, 8, 12, big];
        assert_eq!(
            intersect_sorted_count(&visited["prod"].0, &visited["prod"].1, &elig_small),
            3
        );
        assert!(intersect_sorted_any(
            &visited["prod"].0,
            &visited["prod"].1,
            &elig_small
        ));
        assert_eq!(
            intersect_sorted_count(&visited["staging"].0, &visited["staging"].1, &elig_small),
            1
        );
        assert!(!intersect_sorted_u32_u64_any(&[1, 2, 3], &[4, 5, 6]));
        assert_eq!(intersect_sorted_u32_u64_count(&[1, 5, 9], &[2, 5, 9, 10]), 2);

        // Large eligible vs tiny small (exercises small*16 < eligible branch)
        let huge_len = if cfg!(miri) { 400u64 } else { 5000u64 };
        let huge_elig: Vec<u64> = (1..=huge_len).collect();
        assert_eq!(
            intersect_sorted_u32_u64_count(&[10, 100, (huge_len - 1) as u32, (huge_len + 1000) as u32], &huge_elig),
            3
        );
        assert!(intersect_sorted_u32_u64_any(&[(huge_len - 1) as u32], &huge_elig));
        // Tiny eligible vs large small (exercises eligible*16 < small branch)
        let huge_small: Vec<u32> = (1..=huge_len as u32).collect();
        assert_eq!(intersect_sorted_u32_u64_count(&huge_small, &[42, huge_len - 1, huge_len + 4999]), 2);
        assert!(intersect_sorted_u32_u64_any(&huge_small, &[huge_len - 1, huge_len + 4999]));
        assert!(!intersect_sorted_u32_u64_any(&huge_small, &[huge_len + 1000, huge_len + 2000]));
    }

    #[test]
    fn direct_u32_matchers_parity_with_generic_and_overflow() {
        let mut syms = SymbolTable::new();
        let p = MemPostings::new_unordered();
        let big1 = (u32::MAX as u64) + 10;
        let big2 = (u32::MAX as u64) + 20;

        let n = if cfg!(miri) { 60u64 } else { 600u64 };
        for i in 1..=n {
            let mut pairs = vec![
                ("__name__", if i % 5 == 0 { "err_total" } else { "req_total" }),
                ("job", if i % 3 == 0 { "db" } else if i % 3 == 1 { "api" } else { "web" }),
            ];
            let shard_val = format!("s-{}", i % 25);
            pairs.push(("shard", &shard_val));
            let env_val = if i % 7 == 0 { "" } else if i % 2 == 0 { "prod" } else { "dev" };
            if !env_val.is_empty() {
                pairs.push(("env", env_val));
            }
            let lbls = SymbolLabels::from_pairs(&mut syms, pairs);
            p.add(i, &lbls);
        }
        // Add u64 overflow refs too
        let lbl_ov1 = SymbolLabels::from_pairs(
            &mut syms,
            [("__name__", "req_total"), ("job", "api"), ("shard", "s-0"), ("env", "prod")],
        );
        let lbl_ov2 = SymbolLabels::from_pairs(
            &mut syms,
            [("__name__", "err_total"), ("job", "web"), ("shard", "s-1")],
        );
        p.add(big1, &lbl_ov1);
        p.add(big2, &lbl_ov2);
        p.ensure_order();

        let src = p.as_source(&syms);
        let m = |mt, k, v| LabelMatcher::new(mt, k, v).unwrap();
        let suites: Vec<Vec<LabelMatcher>> = vec![
            vec![],
            vec![m(MatchType::Equal, "", "")],
            vec![m(MatchType::Equal, "__name__", "req_total")],
            vec![
                m(MatchType::Equal, "__name__", "req_total"),
                m(MatchType::Equal, "shard", "s-0"),
            ],
            vec![
                m(MatchType::Equal, "__name__", "req_total"),
                m(MatchType::RegexMatch, "job", "api|web"),
                m(MatchType::NotEqual, "env", "dev"),
            ],
            vec![
                m(MatchType::Equal, "env", ""),
                m(MatchType::RegexNotMatch, "job", "db"),
            ],
            vec![
                m(MatchType::NotEqual, "env", ""),
                m(MatchType::RegexMatch, "shard", "s-1|s-2|s-3"),
            ],
            vec![
                m(MatchType::RegexMatch, "__name__", ".*"),
                m(MatchType::RegexMatch, "env", ".+"),
                m(MatchType::RegexNotMatch, "shard", "s-0|s-1"),
            ],
            vec![
                m(MatchType::RegexNotMatch, "job", ".*"),
                m(MatchType::Equal, "__name__", "req_total"),
            ],
            vec![
                m(MatchType::RegexNotMatch, "env", ".+"),
            ],
            vec![
                m(MatchType::Equal, "__name__", "missing"),
                m(MatchType::Equal, "job", "api"),
            ],
            vec![
                m(MatchType::RegexMatch, "shard", "s-[12].*"),
                m(MatchType::RegexNotMatch, "env", "^$"),
            ],
        ];

        for ms in &suites {
            let fast = p.postings_for_matchers(&syms, ms).unwrap();
            let generic = postings_for_matchers_generic(&src, ms).unwrap();
            assert_eq!(fast, generic, "parity mismatch for matchers {ms:?}");
            let via_trait = postings_for_matchers(&src, ms).unwrap();
            assert_eq!(via_trait, generic, "trait fast-path mismatch for {ms:?}");
        }
    }

    #[test]
    fn add_batches_unordered_parallel_disjoint_scratch_miri() {
        let mut syms = SymbolTable::new();
        let p = MemPostings::new_unordered();
        let pool = mimir_common::thread_pool::ScopedWorkerPool::new("miri-postings", 2);

        // 4 batches * 80 series = 320 (or 4 * 64 = 256 under Miri) >= 256 to exercise the parallel MutScratchPtr path.
        let per_batch = if cfg!(miri) { 64usize } else { 80usize };
        let total = 4 * per_batch;
        let mut batches: Vec<Vec<(u64, SymbolLabels)>> = Vec::with_capacity(4);
        for b in 0..4usize {
            let mut batch = Vec::with_capacity(per_batch);
            for i in 0..per_batch {
                let ref_id = (b * per_batch + i + 1) as u64;
                let shard = format!("s-{}", i % 16);
                let lbls = SymbolLabels::from_pairs(
                    &mut syms,
                    [
                        ("__name__", "parallel_req_total"),
                        ("batch", &format!("b-{b}")),
                        ("shard", &shard),
                    ],
                );
                batch.push((ref_id, lbls));
            }
            batches.push(batch);
        }

        p.add_batches_unordered_parallel(&batches, &pool);
        p.ensure_order();

        assert_eq!(p.all_postings().len(), total);
        let nid = syms.get("__name__").unwrap();
        let vid = syms.get("parallel_req_total").unwrap();
        assert_eq!(p.postings_len(nid, vid), total);
    }

    /// The small-batch path of `add_batches_unordered_parallel` delegates to
    /// `add_batch_unordered_with_scratch`, which takes `add_gate` itself. Holding the gate
    /// across that call is a recursive `parking_lot` read: it deadlocks as soon as a
    /// `postings_for_matchers` subtracting-matcher barrier (`add_gate.write()`) queues in between.
    #[test]
    fn add_batches_unordered_parallel_small_path_does_not_reenter_add_gate() {
        use std::sync::atomic::AtomicBool;
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::time::Duration;

        let mut syms = SymbolTable::new();
        let lbls = SymbolLabels::from_pairs(&mut syms, [("__name__", "gate_reentry"), ("job", "a")]);
        let batches: Arc<Vec<Vec<(u64, SymbolLabels)>>> = Arc::new(vec![vec![(1, lbls)]]);
        let p = Arc::new(MemPostings::new_unordered());
        let iterations = if cfg!(miri) { 50usize } else { 20_000usize };

        let stop = Arc::new(AtomicBool::new(false));
        let barrier = {
            let (p, stop) = (Arc::clone(&p), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(AtomicOrdering::Acquire) {
                    drop(p.add_gate.write());
                    std::thread::yield_now();
                }
            })
        };
        let (done_tx, done_rx) = mpsc::channel();
        let adder = {
            let (p, batches) = (Arc::clone(&p), Arc::clone(&batches));
            std::thread::spawn(move || {
                let pool = mimir_common::thread_pool::ScopedWorkerPool::new("gate-reentry", 2);
                for _ in 0..iterations {
                    p.add_batches_unordered_parallel(&batches, &pool);
                }
                let _ = done_tx.send(());
            })
        };

        let finished = done_rx.recv_timeout(Duration::from_secs(120)).is_ok();
        stop.store(true, AtomicOrdering::Release);
        assert!(
            finished,
            "add_batches_unordered_parallel deadlocked against a queued add_gate.write() barrier"
        );
        adder.join().unwrap();
        barrier.join().unwrap();
        assert_eq!(p.all_postings.read().len(), iterations);
    }
}
