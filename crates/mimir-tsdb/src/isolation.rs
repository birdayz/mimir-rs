//! Append/read transaction isolation and OOO read tracking.
//!
//! Ported from github.com/prometheus/prometheus/tsdb/isolation.go
//! and github.com/prometheus/prometheus/tsdb/ooo_isolation.go.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::head_chunks::ChunkDiskMapperRef;

/// Snapshot of an open read isolation state.
#[derive(Clone, Debug)]
pub struct IsolationStateInfo {
    pub read_id: u64,
    pub max_append_id: u64,
    pub incomplete_appends: HashSet<u64>,
    pub low_watermark: u64,
    pub mint: i64,
    pub maxt: i64,
}

#[derive(Default)]
struct AppendState {
    last_append_id: u64,
    /// Open append IDs mapped to their `min_time`.
    appends_open: BTreeMap<u64, i64>,
}

#[derive(Default)]
struct ReadState {
    next_read_id: u64,
    /// Ordered newest-at-front, oldest-at-back, matching Go doubly-linked list.
    reads_open: VecDeque<IsolationStateInfo>,
}

struct IsolationInner {
    disabled: bool,
    append: Mutex<AppendState>,
    read: Mutex<ReadState>,
}

/// Global append/read isolation coordinator (`tsdb.isolation`).
#[derive(Clone)]
pub struct Isolation {
    inner: Arc<IsolationInner>,
}

impl Isolation {
    pub fn new(disabled: bool) -> Self {
        Self {
            inner: Arc::new(IsolationInner {
                disabled,
                append: Mutex::new(AppendState::default()),
                read: Mutex::new(ReadState::default()),
            }),
        }
    }

    pub fn is_disabled(&self) -> bool {
        self.inner.disabled
    }

    fn low_watermark_locked(disabled: bool, append: &AppendState, read: &ReadState) -> u64 {
        if disabled {
            return 0;
        }
        if let Some(oldest_read) = read.reads_open.back() {
            return oldest_read.low_watermark;
        }
        if let Some((&lowest_open, _)) = append.appends_open.first_key_value() {
            return lowest_open;
        }
        append.last_append_id
    }

    /// Returns the appendID below which per-append tracking is no longer needed.
    pub fn low_watermark(&self) -> u64 {
        if self.inner.disabled {
            return 0;
        }
        let append = self.inner.append.lock();
        let read = self.inner.read.lock();
        Self::low_watermark_locked(self.inner.disabled, &append, &read)
    }

    /// Returns the lowest `min_time` for any open appender, or `i64::MAX` if none.
    pub fn lowest_append_time(&self) -> i64 {
        let append = self.inner.append.lock();
        append
            .appends_open
            .values()
            .copied()
            .min()
            .unwrap_or(i64::MAX)
    }

    /// Opens an isolation read state for `[mint, maxt]`. Must be closed when complete.
    pub fn state(&self, mint: i64, maxt: i64) -> IsolationState {
        let append = self.inner.append.lock();
        let mut read = self.inner.read.lock();
        read.next_read_id += 1;
        let read_id = read.next_read_id;

        let low_watermark = append
            .appends_open
            .first_key_value()
            .map_or(append.last_append_id, |(&k, _)| k);
        let incomplete_appends: HashSet<u64> = append.appends_open.keys().copied().collect();
        let info = IsolationStateInfo {
            read_id,
            max_append_id: append.last_append_id,
            incomplete_appends,
            low_watermark,
            mint,
            maxt,
        };
        read.reads_open.push_front(info.clone());
        IsolationState {
            inner: Arc::clone(&self.inner),
            info,
            closed: Mutex::new(false),
        }
    }

    /// Iterates open reads, stopping early if `f` returns `false`.
    pub fn traverse_open_reads(&self, mut f: impl FnMut(&IsolationStateInfo) -> bool) {
        let read = self.inner.read.lock();
        for s in &read.reads_open {
            if !f(s) {
                break;
            }
        }
    }

    pub fn open_reads_count(&self) -> usize {
        self.inner.read.lock().reads_open.len()
    }

    /// Issues a new append ID and returns `(append_id, low_watermark)`.
    pub fn new_append_id(&self, min_time: i64) -> (u64, u64) {
        if self.inner.disabled {
            return (0, 0);
        }
        let mut append = self.inner.append.lock();
        append.last_append_id += 1;
        let id = append.last_append_id;
        append.appends_open.insert(id, min_time);
        let read = self.inner.read.lock();
        let lwm = Self::low_watermark_locked(self.inner.disabled, &append, &read);
        (id, lwm)
    }

    pub fn last_append_id(&self) -> u64 {
        if self.inner.disabled {
            return 0;
        }
        self.inner.append.lock().last_append_id
    }

    /// Returns the highest appendID guaranteed to be committed at the time of the call.
    pub fn committed_append_id(&self) -> u64 {
        if self.inner.disabled {
            return 0;
        }
        let append = self.inner.append.lock();
        match append.appends_open.first_key_value() {
            None => append.last_append_id,
            Some((&lowest_open, _)) => lowest_open - 1,
        }
    }

    pub fn close_append(&self, append_id: u64) {
        if self.inner.disabled {
            return;
        }
        self.inner.append.lock().appends_open.remove(&append_id);
    }
}

/// Active read handle returned by [`Isolation::state`].
pub struct IsolationState {
    inner: Arc<IsolationInner>,
    info: IsolationStateInfo,
    closed: Mutex<bool>,
}

impl IsolationState {
    pub fn info(&self) -> &IsolationStateInfo {
        &self.info
    }

    pub fn isolation_disabled(&self) -> bool {
        self.inner.disabled
    }

    pub fn close(&self) {
        let mut closed = self.closed.lock();
        if *closed {
            return;
        }
        *closed = true;
        let mut read = self.inner.read.lock();
        if let Some(pos) = read
            .reads_open
            .iter()
            .position(|r| r.read_id == self.info.read_id)
        {
            read.reads_open.remove(pos);
        }
    }
}

impl Drop for IsolationState {
    fn drop(&mut self) {
        self.close();
    }
}

/// Transaction ID ring buffer (`tsdb.txRing`).
#[derive(Clone, Debug, Default)]
pub struct TxRing {
    tx_ids: Vec<u64>,
    tx_id_first: usize,
    tx_id_count: usize,
}

impl TxRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            tx_ids: vec![0; capacity],
            tx_id_first: 0,
            tx_id_count: 0,
        }
    }

    pub fn add(&mut self, append_id: u64) {
        if self.tx_id_count == self.tx_ids.len() {
            let new_len = if self.tx_ids.is_empty() {
                4
            } else {
                self.tx_ids.len() * 2
            };
            let mut new_ring = vec![0u64; new_len];
            let first_part = self.tx_ids.len() - self.tx_id_first;
            if first_part > 0 {
                new_ring[..first_part].copy_from_slice(&self.tx_ids[self.tx_id_first..]);
            }
            if self.tx_id_first > 0 {
                new_ring[first_part..first_part + self.tx_id_first]
                    .copy_from_slice(&self.tx_ids[..self.tx_id_first]);
            }
            self.tx_ids = new_ring;
            self.tx_id_first = 0;
        }
        let idx = (self.tx_id_first + self.tx_id_count) % self.tx_ids.len();
        self.tx_ids[idx] = append_id;
        self.tx_id_count += 1;
    }

    pub fn cleanup_append_ids_below(&mut self, bound: u64) {
        if self.tx_ids.is_empty() {
            return;
        }
        let len = self.tx_ids.len();
        let mut pos = self.tx_id_first;
        while self.tx_id_count > 0 {
            if self.tx_ids[pos] >= bound {
                break;
            }
            self.tx_id_first += 1;
            self.tx_id_count -= 1;
            pos += 1;
            if pos == len {
                pos = 0;
            }
        }
        self.tx_id_first %= len;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.tx_id_count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tx_id_count == 0
    }

    pub fn last_append_id(&self) -> Option<u64> {
        if self.tx_id_count == 0 || self.tx_ids.is_empty() {
            return None;
        }
        let pos = (self.tx_id_first + self.tx_id_count - 1) % self.tx_ids.len();
        Some(self.tx_ids[pos])
    }

    pub fn iterator(&self) -> TxRingIterator<'_> {
        TxRingIterator {
            ids: &self.tx_ids,
            pos: self.tx_id_first,
        }
    }
}

pub struct TxRingIterator<'a> {
    ids: &'a [u64],
    pos: usize,
}

impl TxRingIterator<'_> {
    pub fn at(&self) -> u64 {
        self.ids[self.pos]
    }

    pub fn next_pos(&mut self) {
        self.pos += 1;
        if self.pos == self.ids.len() {
            self.pos = 0;
        }
    }
}

#[derive(Default)]
struct OooIsolationInner {
    next_id: u64,
    open_reads: Vec<(u64, ChunkDiskMapperRef)>,
}

/// Out-of-order read isolation tracker (`tsdb.oooIsolation`).
pub struct OooIsolation {
    inner: Arc<Mutex<OooIsolationInner>>,
}

impl Default for OooIsolation {
    fn default() -> Self {
        Self::new()
    }
}

impl OooIsolation {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OooIsolationInner::default())),
        }
    }

    /// Returns `true` if any open read uses chunks at or before `ref_id`
    /// (`ref_id > s.min_ref`, matching Go `ref.GreaterThan(s.minRef)`).
    pub fn has_open_reads_at_or_before(&self, ref_id: ChunkDiskMapperRef) -> bool {
        let inner = self.inner.lock();
        inner.open_reads.iter().any(|&(_, min_ref)| ref_id > min_ref)
    }

    /// Records a read that uses chunks with reference after `min_ref`.
    pub fn track_read_after(&self, min_ref: ChunkDiskMapperRef) -> OooIsolationState {
        let mut inner = self.inner.lock();
        inner.next_id += 1;
        let id = inner.next_id;
        inner.open_reads.push((id, min_ref));
        OooIsolationState {
            inner: Arc::clone(&self.inner),
            id,
        }
    }
}

/// Active OOO read handle; calling `close()` multiple times is safe and idempotent.
pub struct OooIsolationState {
    inner: Arc<Mutex<OooIsolationInner>>,
    id: u64,
}

impl OooIsolationState {
    pub fn close(&self) {
        let mut inner = self.inner.lock();
        if let Some(pos) = inner.open_reads.iter().position(|&(rid, _)| rid == self.id) {
            inner.open_reads.swap_remove(pos);
        }
    }
}

impl Drop for OooIsolationState {
    fn drop(&mut self) {
        self.close();
    }
}
