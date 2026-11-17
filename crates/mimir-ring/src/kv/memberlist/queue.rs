//! Transmit-limited broadcast queue.
//!
//! Ported from github.com/hashicorp/memberlist/queue.go (grafana fork) — `TransmitLimitedQueue`,
//! `Broadcast`, `NamedBroadcast` — and `retransmitLimit` from util.go.
//!
//! Ordering is identical to the Go btree: transmits ascending, then message length descending,
//! then id descending (newest first).

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;

/// Go `memberlist.Broadcast` (+ optional `NamedBroadcast`).
pub trait Broadcast: Send + Sync {
    /// Go `Invalidates`: whether this broadcast makes `other` obsolete. Only called for
    /// non-named broadcasts against non-named queued broadcasts (as Go's type switch does).
    fn invalidates(&self, other: &dyn Broadcast) -> bool;
    fn message(&self) -> &[u8];
    /// Called when the broadcast is dropped from the queue (sent enough times or invalidated).
    fn finished(&self);
    /// Go `NamedBroadcast.Name()`: `Some` for broadcasts that replace any queued broadcast with
    /// the same name.
    fn name(&self) -> Option<&str> {
        None
    }
    fn as_any(&self) -> &dyn std::any::Any;
}

type Key = (usize, Reverse<i64>, Reverse<i64>);

struct Entry {
    transmits: usize,
    msg_len: i64,
    id: i64,
    b: Arc<dyn Broadcast>,
}

impl Entry {
    fn key(&self) -> Key {
        (self.transmits, Reverse(self.msg_len), Reverse(self.id))
    }
}

#[derive(Default)]
struct Inner {
    tq: BTreeMap<Key, Entry>,
    tm: HashMap<String, Key>,
    id_gen: i64,
}

impl Inner {
    fn delete_item(&mut self, key: &Key) -> Option<Entry> {
        let e = self.tq.remove(key)?;
        if let Some(name) = e.b.name() {
            self.tm.remove(name);
        }
        Some(e)
    }

    fn add_item(&mut self, e: Entry) {
        let key = e.key();
        if let Some(name) = e.b.name() {
            self.tm.insert(name.to_string(), key);
        }
        self.tq.insert(key, e);
    }
}

/// Go `memberlist.TransmitLimitedQueue`.
pub struct TransmitLimitedQueue {
    num_nodes: Box<dyn Fn() -> usize + Send + Sync>,
    retransmit_mult: usize,
    inner: Mutex<Inner>,
}

/// Go `retransmitLimit`.
pub fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
    let node_scale = ((n + 1) as f64).log10().ceil() as usize;
    retransmit_mult * node_scale
}

impl TransmitLimitedQueue {
    pub fn new(retransmit_mult: usize, num_nodes: Box<dyn Fn() -> usize + Send + Sync>) -> Self {
        Self {
            num_nodes,
            retransmit_mult,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Go `QueueBroadcast`.
    pub fn queue_broadcast(&self, b: Arc<dyn Broadcast>) {
        let mut q = self.inner.lock();
        q.id_gen = if q.id_gen == i64::MAX { 1 } else { q.id_gen + 1 };
        let id = q.id_gen;
        let e = Entry {
            transmits: 0,
            msg_len: b.message().len() as i64,
            id,
            b: Arc::clone(&b),
        };
        if let Some(name) = b.name() {
            if let Some(old_key) = q.tm.get(name).copied() {
                if let Some(old) = q.delete_item(&old_key) {
                    old.b.finished();
                }
            }
        } else {
            let remove: Vec<Key> = q
                .tq
                .iter()
                .filter(|(_, cur)| cur.b.name().is_none() && b.invalidates(cur.b.as_ref()))
                .map(|(k, _)| *k)
                .collect();
            for k in remove {
                if let Some(cur) = q.delete_item(&k) {
                    cur.b.finished();
                }
            }
        }
        q.add_item(e);
    }

    /// Go `GetBroadcasts`.
    pub fn get_broadcasts(&self, overhead: usize, limit: usize) -> Vec<Vec<u8>> {
        let mut q = self.inner.lock();
        if q.tq.is_empty() {
            return Vec::new();
        }
        let transmit_limit = retransmit_limit(self.retransmit_mult, (self.num_nodes)());
        let mut bytes_used = 0i64;
        let mut to_send = Vec::new();
        let mut reinsert = Vec::new();

        let min_tr = q.tq.keys().next().map(|k| k.0).unwrap_or(0);
        let max_tr = q.tq.keys().next_back().map(|k| k.0).unwrap_or(0);
        let mut transmits = min_tr;
        while transmits <= max_tr {
            let free = limit as i64 - bytes_used - overhead as i64;
            if free <= 0 {
                break;
            }
            let start: Key = (transmits, Reverse(free), Reverse(i64::MAX));
            let end: Key = (transmits + 1, Reverse(i64::MAX), Reverse(i64::MAX));
            let keep = q
                .tq
                .range(start..end)
                .find(|(_, cur)| cur.b.message().len() as i64 <= free)
                .map(|(k, _)| *k);
            let Some(keep) = keep else {
                transmits += 1;
                continue;
            };
            let mut e = q.delete_item(&keep).expect("present");
            let msg = e.b.message().to_vec();
            bytes_used += (overhead + msg.len()) as i64;
            to_send.push(msg);
            if e.transmits + 1 >= transmit_limit {
                e.b.finished();
            } else {
                e.transmits += 1;
                reinsert.push(e);
            }
        }
        for e in reinsert {
            q.add_item(e);
        }
        to_send
    }

    /// Go `NumQueued`.
    pub fn num_queued(&self) -> usize {
        self.inner.lock().tq.len()
    }

    /// Go `Reset`.
    pub fn reset(&self) {
        let mut q = self.inner.lock();
        for e in q.tq.values() {
            e.b.finished();
        }
        *q = Inner::default();
    }

    /// Go `Prune`: retain the `max_retain` most valuable (least transmitted) broadcasts.
    pub fn prune(&self, max_retain: usize) {
        let mut q = self.inner.lock();
        while q.tq.len() > max_retain {
            let Some(k) = q.tq.keys().next_back().copied() else {
                break;
            };
            if let Some(e) = q.delete_item(&k) {
                e.b.finished();
            }
        }
    }

    /// Ordered snapshot of queued messages (test helper, Go `orderedView(false)`).
    pub fn ordered_messages(&self) -> Vec<(usize, Vec<u8>)> {
        self.inner
            .lock()
            .tq
            .values()
            .map(|e| (e.transmits, e.b.message().to_vec()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Named {
        name: String,
        msg: Vec<u8>,
        finished: Arc<AtomicUsize>,
    }
    impl Broadcast for Named {
        fn invalidates(&self, _other: &dyn Broadcast) -> bool {
            false
        }
        fn message(&self) -> &[u8] {
            &self.msg
        }
        fn finished(&self) {
            self.finished.fetch_add(1, Ordering::Relaxed);
        }
        fn name(&self) -> Option<&str> {
            Some(&self.name)
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn named(name: &str, msg: &str, f: &Arc<AtomicUsize>) -> Arc<dyn Broadcast> {
        Arc::new(Named {
            name: name.to_string(),
            msg: msg.as_bytes().to_vec(),
            finished: Arc::clone(f),
        })
    }

    // Upstream: github.com/hashicorp/memberlist/queue_test.go::TestTransmitLimited_Queue
    #[test]
    fn test_transmit_limited_queue() {
        let f = Arc::new(AtomicUsize::new(0));
        let q = TransmitLimitedQueue::new(1, Box::new(|| 1));
        q.queue_broadcast(named("test", "1", &f));
        q.queue_broadcast(named("foo", "2", &f));
        q.queue_broadcast(named("bar", "3", &f));
        assert_eq!(q.num_queued(), 3);
        let order: Vec<Vec<u8>> = q.ordered_messages().into_iter().map(|(_, m)| m).collect();
        assert_eq!(order, vec![b"3".to_vec(), b"2".to_vec(), b"1".to_vec()]);
        // Should invalidate previous message
        q.queue_broadcast(named("test", "4", &f));
        assert_eq!(q.num_queued(), 3);
        let order: Vec<Vec<u8>> = q.ordered_messages().into_iter().map(|(_, m)| m).collect();
        assert_eq!(order, vec![b"4".to_vec(), b"3".to_vec(), b"2".to_vec()]);
        assert_eq!(f.load(Ordering::Relaxed), 1);
    }

    // Upstream: github.com/hashicorp/memberlist/queue_test.go::TestTransmitLimited_GetBroadcasts_Limit
    #[test]
    fn test_transmit_limited_get_broadcasts_limit() {
        let f = Arc::new(AtomicUsize::new(0));
        let q = TransmitLimitedQueue::new(1, Box::new(|| 10));
        assert_eq!(q.get_broadcasts(3, 10).len(), 0);
        q.queue_broadcast(named("test", "1. this is a test.", &f));
        q.queue_broadcast(named("foo", "2. this is a test.", &f));
        q.queue_broadcast(named("bar", "3. this is a test.", &f));
        q.queue_broadcast(named("baz", "4. this is a test.", &f));
        assert_eq!(retransmit_limit(1, 10), 2, "sanity check transmit limits");
        // 3 byte overhead, should only get 3 messages back
        assert_eq!(q.get_broadcasts(3, 80).len(), 3);
        assert_eq!(q.get_broadcasts(3, 80).len(), 3);
        // Only two not expired
        assert_eq!(q.get_broadcasts(3, 80).len(), 2);
        // Should get nothing
        assert_eq!(q.get_broadcasts(3, 80).len(), 0);
    }

    // Upstream: github.com/hashicorp/memberlist/queue_test.go::TestTransmitLimited_Prune
    #[test]
    fn test_transmit_limited_prune() {
        let f = Arc::new(AtomicUsize::new(0));
        let q = TransmitLimitedQueue::new(1, Box::new(|| 10));
        q.queue_broadcast(named("test", "1. this is a test.", &f));
        q.queue_broadcast(named("foo", "2. this is a test.", &f));
        q.queue_broadcast(named("bar", "3. this is a test.", &f));
        q.queue_broadcast(named("baz", "4. this is a test.", &f));
        q.prune(2);
        assert_eq!(f.load(Ordering::Relaxed), 2);
        assert_eq!(q.num_queued(), 2);
        let order: Vec<Vec<u8>> = q.ordered_messages().into_iter().map(|(_, m)| m).collect();
        assert_eq!(
            order,
            vec![b"4. this is a test.".to_vec(), b"3. this is a test.".to_vec()]
        );
    }

    // Upstream: github.com/hashicorp/memberlist/queue_test.go::TestTransmitLimited_GetBroadcasts
    #[test]
    fn test_transmit_limited_get_broadcasts() {
        let f = Arc::new(AtomicUsize::new(0));
        let q = TransmitLimitedQueue::new(3, Box::new(|| 10));
        q.queue_broadcast(named("test", "1. this is a test.", &f));
        q.queue_broadcast(named("foo", "2. this is a test.", &f));
        q.queue_broadcast(named("bar", "3. this is a test.", &f));
        q.queue_broadcast(named("baz", "4. this is a test.", &f));
        // 2 byte overhead per message, should get all 4 messages
        let all = q.get_broadcasts(2, 80);
        assert_eq!(all.len(), 4);
        // 3 byte overhead, should only get 3 messages back
        let partial = q.get_broadcasts(3, 80);
        assert_eq!(partial.len(), 3);
    }
}
