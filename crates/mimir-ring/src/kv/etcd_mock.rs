//! In-memory mock implementation of an Etcd v3 client for unit tests.
//!
//! Ported from github.com/grafana/dskit/kv/etcd/mock.go

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mimir_common::services::ServiceContext;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};

const CHANNEL_BUFFER_SIZE: usize = 10;

/// Mirror of `mvccpb.KeyValue`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MockKeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MockEventType {
    Put,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockEvent {
    pub event_type: MockEventType,
    pub kv: MockKeyValue,
}

#[derive(Clone, Debug, Default)]
pub struct MockWatchResponse {
    pub events: Vec<MockEvent>,
    pub canceled: bool,
}

#[derive(Clone, Debug)]
pub struct MockGetResponse {
    pub kvs: Vec<MockKeyValue>,
    pub count: i64,
}

#[derive(Clone, Debug)]
pub struct MockDeleteResponse {
    pub deleted: i64,
}

#[derive(Clone, Debug)]
pub struct MockTxnResponse {
    pub succeeded: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Equal,
    Greater,
    Less,
    NotEqual,
}

#[derive(Clone, Debug)]
pub enum CmpTarget {
    Value(Vec<u8>),
    CreateRevision(i64),
    Lease(i64),
    Version(i64),
    ModRevision(i64),
}

#[derive(Clone, Debug)]
pub struct MockCmp {
    pub key: String,
    pub op: CmpOp,
    pub target: CmpTarget,
}

#[derive(Clone, Debug)]
pub enum MockOp {
    Get { key: String, prefix: bool },
    Put { key: String, value: Vec<u8> },
    Delete { key: String, prefix: bool },
}

/// Port of `dskit/kv/etcd.mockKV`.
pub struct MockEtcdKv {
    values: Mutex<HashMap<String, MockKeyValue>>,
    events: Mutex<Vec<mpsc::Sender<MockEvent>>>,
    closed: Arc<AtomicBool>,
    close_notify: Arc<Notify>,
}

impl Default for MockEtcdKv {
    fn default() -> Self {
        Self {
            values: Mutex::new(HashMap::new()),
            events: Mutex::new(Vec::new()),
            closed: Arc::new(AtomicBool::new(false)),
            close_notify: Arc::new(Notify::new()),
        }
    }
}

impl MockEtcdKv {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Inserts or replaces a raw entry directly (mirroring test setup `kv.values[k] = ...`).
    pub fn insert_raw(&self, key: &str, kv: MockKeyValue) {
        self.values.lock().insert(key.to_string(), kv);
    }

    pub fn get_raw(&self, key: &str) -> Option<MockKeyValue> {
        self.values.lock().get(key).cloned()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.values.lock().contains_key(key)
    }

    pub fn is_empty(&self) -> bool {
        self.values.lock().is_empty()
    }

    pub fn get(&self, key: &str, prefix: bool) -> MockGetResponse {
        let values = self.values.lock();
        let matching = matching_keys(key, prefix, &values);
        let mut kvs = Vec::with_capacity(matching.len());
        for k in matching {
            if let Some(kv) = values.get(&k) {
                kvs.push(kv.clone());
            }
        }
        let count = kvs.len() as i64;
        MockGetResponse { kvs, count }
    }

    pub fn put(&self, key: &str, val: Vec<u8>) {
        let mut values = self.values.lock();
        self.do_put(&mut values, key.as_bytes().to_vec(), val);
    }

    pub fn delete(&self, key: &str, prefix: bool) -> MockDeleteResponse {
        let mut values = self.values.lock();
        self.do_delete(&mut values, key, prefix)
    }

    pub fn eval_cmp(&self, cmp: &MockCmp) -> bool {
        let values = self.values.lock();
        eval_cmp_locked(&values, cmp)
    }

    pub fn txn(
        &self,
        cmps: &[MockCmp],
        then_ops: &[MockOp],
        else_ops: &[MockOp],
    ) -> MockTxnResponse {
        let mut values = self.values.lock();
        let succeeded = cmps.iter().all(|c| eval_cmp_locked(&values, c));
        let to_run = if succeeded { then_ops } else { else_ops };
        for op in to_run {
            match op {
                MockOp::Get { key, prefix } => {
                    let _ = matching_keys(key, *prefix, &values);
                }
                MockOp::Put { key, value } => {
                    self.do_put(&mut values, key.as_bytes().to_vec(), value.clone());
                }
                MockOp::Delete { key, prefix } => {
                    self.do_delete(&mut values, key, *prefix);
                }
            }
        }
        MockTxnResponse { succeeded }
    }

    /// Watches `key` (or keys matching prefix if `prefix == true`) until `cancel` or `close()`.
    pub fn watch(
        self: &Arc<Self>,
        cancel: ServiceContext,
        key: &str,
        prefix: bool,
    ) -> mpsc::Receiver<MockWatchResponse> {
        let (watcher_tx, watcher_rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        let (consumer_tx, mut consumer_rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        self.events.lock().push(consumer_tx);

        let this = Arc::clone(self);
        let key_owned = key.to_string();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.canceled() => break,
                    _ = this.close_notify.notified() => break,
                    maybe_ev = consumer_rx.recv() => {
                        let Some(ev) = maybe_ev else {
                            break;
                        };
                        if this.closed.load(Ordering::Acquire) {
                            break;
                        }
                        if key_matches(&key_owned, prefix, &ev.kv.key) {
                            let _ = watcher_tx.try_send(MockWatchResponse {
                                events: vec![ev],
                                canceled: false,
                            });
                        }
                    }
                }
            }

            // Drop dead senders on cleanup.
            this.events.lock().retain(|s| !s.is_closed());
            let _ = watcher_tx.try_send(MockWatchResponse {
                events: Vec::new(),
                canceled: true,
            });
        });

        watcher_rx
    }

    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.events.lock().clear();
            self.close_notify.notify_waiters();
        }
    }

    fn do_put(&self, values: &mut HashMap<String, MockKeyValue>, key_bytes: Vec<u8>, val: Vec<u8>) {
        let key = String::from_utf8_lossy(&key_bytes).into_owned();
        let new_val = if let Some(old) = values.get(&key) {
            MockKeyValue {
                key: old.key.clone(),
                value: val,
                create_revision: old.create_revision,
                mod_revision: old.mod_revision + 1,
                version: old.version + 1,
                lease: old.lease,
            }
        } else {
            MockKeyValue {
                key: key_bytes,
                value: val,
                create_revision: 1,
                mod_revision: 1,
                version: 1,
                lease: 0,
            }
        };
        values.insert(key, new_val.clone());
        self.send_event(MockEvent {
            event_type: MockEventType::Put,
            kv: new_val,
        });
    }

    fn do_delete(
        &self,
        values: &mut HashMap<String, MockKeyValue>,
        key: &str,
        prefix: bool,
    ) -> MockDeleteResponse {
        let matching = matching_keys(key, prefix, values);
        let deleted = matching.len() as i64;
        for k in matching {
            if let Some(mut kv) = values.remove(&k) {
                kv.mod_revision = kv.version;
                self.send_event(MockEvent {
                    event_type: MockEventType::Delete,
                    kv,
                });
            }
        }
        MockDeleteResponse { deleted }
    }

    fn send_event(&self, event: MockEvent) {
        let mut events = self.events.lock();
        events.retain(|ch| !ch.is_closed());
        for ch in events.iter() {
            let _ = ch.try_send(event.clone());
        }
    }
}

fn key_matches(filter: &str, prefix: bool, key: &[u8]) -> bool {
    if !prefix {
        key == filter.as_bytes()
    } else if filter.is_empty() {
        true
    } else {
        key.starts_with(filter.as_bytes())
    }
}

fn matching_keys(filter: &str, prefix: bool, kvps: &HashMap<String, MockKeyValue>) -> Vec<String> {
    if !prefix {
        if kvps.contains_key(filter) {
            vec![filter.to_string()]
        } else {
            Vec::new()
        }
    } else if filter.is_empty() {
        kvps.keys().cloned().collect()
    } else {
        kvps.keys()
            .filter(|k| k.as_bytes().starts_with(filter.as_bytes()))
            .cloned()
            .collect()
    }
}

fn eval_cmp_locked(values: &HashMap<String, MockKeyValue>, cmp: &MockCmp) -> bool {
    let default_kv = MockKeyValue::default();
    let entry = values.get(&cmp.key).unwrap_or(&default_kv);
    match &cmp.target {
        CmpTarget::Value(target) => eval_ord(entry.value.cmp(target), cmp.op),
        CmpTarget::CreateRevision(target) => eval_ord(entry.create_revision.cmp(target), cmp.op),
        CmpTarget::Lease(target) => eval_ord(entry.lease.cmp(target), cmp.op),
        CmpTarget::Version(target) => eval_ord(entry.version.cmp(target), cmp.op),
        CmpTarget::ModRevision(target) => eval_ord(entry.mod_revision.cmp(target), cmp.op),
    }
}

fn eval_ord(ord: std::cmp::Ordering, op: CmpOp) -> bool {
    match op {
        CmpOp::Equal => ord == std::cmp::Ordering::Equal,
        CmpOp::Greater => ord == std::cmp::Ordering::Greater,
        CmpOp::Less => ord == std::cmp::Ordering::Less,
        CmpOp::NotEqual => ord != std::cmp::Ordering::Equal,
    }
}
