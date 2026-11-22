//! In-memory KV store (port of Consul mock KV).
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/consul/mock.go
//! - github.com/grafana/dskit/kv/client.go

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use mimir_common::services::ServiceContext;
use parking_lot::Mutex;
use tokio::sync::watch;

use super::{BoxFuture, CasFn, CasOutcome, Codec, KvClient, KvError};

#[derive(Clone)]
struct KvEntry {
    value: Vec<u8>,
    modify_index: u64,
}

pub struct InMemoryStore {
    inner: Mutex<InMemoryState>,
    notify: watch::Sender<u64>,
}

struct InMemoryState {
    kvps: HashMap<String, KvEntry>,
    current: u64,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(InMemoryState {
                kvps: HashMap::new(),
                // Start from 1 to match Consul/dskit mock.go semantics.
                current: 1,
            }),
            notify: watch::channel(0u64).0,
        }
    }

    fn bump_notify(&self) {
        self.notify.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub fn put_raw(&self, key: &str, value: Vec<u8>) {
        {
            let mut st = self.inner.lock();
            st.current += 1;
            let idx = st.current;
            match st.kvps.get_mut(key) {
                Some(entry) => {
                    entry.value = value;
                    entry.modify_index = idx;
                }
                None => {
                    st.kvps.insert(
                        key.to_string(),
                        KvEntry {
                            value,
                            modify_index: idx,
                        },
                    );
                }
            }
        }
        self.bump_notify();
    }
}

fn shared_store() -> Arc<InMemoryStore> {
    static SHARED: OnceLock<Arc<InMemoryStore>> = OnceLock::new();
    SHARED.get_or_init(|| Arc::new(InMemoryStore::new())).clone()
}

/// In-memory [`KvClient`] implementation matching `dskit/kv/consul.NewInMemoryClient`.
pub struct InMemoryKv {
    store: Arc<InMemoryStore>,
    codec: Arc<dyn Codec>,
    max_cas_retries: usize,
}

impl InMemoryKv {
    pub fn new(codec: Arc<dyn Codec>) -> Self {
        Self {
            store: Arc::new(InMemoryStore::new()),
            codec,
            max_cas_retries: 10,
        }
    }

    pub fn with_store(store: Arc<InMemoryStore>, codec: Arc<dyn Codec>) -> Self {
        Self {
            store,
            codec,
            max_cas_retries: 10,
        }
    }

    pub fn shared_with_codec(codec: Arc<dyn Codec>) -> Self {
        Self {
            store: shared_store(),
            codec,
            max_cas_retries: 10,
        }
    }

    pub fn with_codec(&self, codec: Arc<dyn Codec>) -> Self {
        Self {
            store: self.store.clone(),
            codec,
            max_cas_retries: self.max_cas_retries,
        }
    }

    pub fn store(&self) -> &Arc<InMemoryStore> {
        &self.store
    }

    pub async fn put_any(&self, key: &str, val: &(dyn Any + Send + Sync)) -> Result<(), KvError> {
        let bytes = self.codec.encode(val)?;
        self.store.put_raw(key, bytes);
        Ok(())
    }
}

impl KvClient for InMemoryKv {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let st = self.store.inner.lock();
            let keys = st
                .kvps
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            Ok(keys)
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let bytes = {
                let st = self.store.inner.lock();
                st.kvps.get(key).map(|e| e.value.clone())
            };
            match bytes {
                None => Ok(None),
                Some(b) => Ok(Some(self.codec.decode(&b)?)),
            }
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let removed = {
                let mut st = self.store.inner.lock();
                let r = st.kvps.remove(key).is_some();
                if r {
                    st.current += 1;
                }
                r
            };
            if removed {
                self.store.bump_notify();
            }
            Ok(())
        })
    }

    fn cas<'a>(&'a self, key: &'a str, mut f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            for _ in 0..self.max_cas_retries {
                let (existing_bytes, expected_index) = {
                    let st = self.store.inner.lock();
                    match st.kvps.get(key) {
                        Some(entry) => (Some(entry.value.clone()), entry.modify_index),
                        None => (None, 0),
                    }
                };

                let decoded = match existing_bytes {
                    Some(b) => Some(self.codec.decode(&b)?),
                    None => None,
                };

                let outcome = match f(decoded.as_deref()) {
                    Ok(o) => o,
                    Err(cb_err) => {
                        if !cb_err.retry {
                            return Err(cb_err.err);
                        }
                        continue;
                    }
                };

                let new_val = match outcome {
                    CasOutcome::NoChange => return Ok(()),
                    CasOutcome::Update(v) | CasOutcome::UpdateOnce(v) => v,
                };

                let encoded = self.codec.encode(new_val.as_ref())?;
                let swapped = {
                    let mut st = self.store.inner.lock();
                    let cur_idx = st.kvps.get(key).map(|e| e.modify_index).unwrap_or(0);
                    if cur_idx != expected_index {
                        false
                    } else {
                        st.current += 1;
                        let new_idx = st.current;
                        match st.kvps.get_mut(key) {
                            Some(entry) => {
                                entry.value = encoded;
                                entry.modify_index = new_idx;
                            }
                            None => {
                                st.kvps.insert(
                                    key.to_string(),
                                    KvEntry {
                                        value: encoded,
                                        modify_index: new_idx,
                                    },
                                );
                            }
                        }
                        true
                    }
                };

                if swapped {
                    self.store.bump_notify();
                    return Ok(());
                }
            }
            Err(KvError::CasFailed {
                key: key.to_string(),
                reason: "max retries exceeded".to_string(),
            })
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut last_index = 0u64;
            let mut rx = self.store.notify.subscribe();
            loop {
                if cancel.is_canceled() {
                    return;
                }

                let _ = rx.borrow_and_update();
                let snapshot = {
                    let st = self.store.inner.lock();
                    st.kvps
                        .get(key)
                        .map(|e| (e.modify_index, e.value.clone()))
                };

                if let Some((idx, bytes)) = snapshot {
                    if idx != last_index {
                        last_index = idx;
                        if let Ok(decoded) = self.codec.decode(&bytes) {
                            if !f(decoded) {
                                return;
                            }
                        }
                        continue;
                    }
                }

                tokio::select! {
                    _ = cancel.canceled() => return,
                    res = rx.changed() => {
                        if res.is_err() {
                            return;
                        }
                    }
                }
            }
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut last_seen: HashMap<String, u64> = HashMap::new();
            let mut rx = self.store.notify.subscribe();
            loop {
                if cancel.is_canceled() {
                    return;
                }

                let _ = rx.borrow_and_update();
                let changed: Vec<(String, u64, Vec<u8>)> = {
                    let st = self.store.inner.lock();
                    let mut out = Vec::new();
                    for (k, entry) in &st.kvps {
                        if k.starts_with(prefix) {
                            let prev = last_seen.get(k).copied().unwrap_or(0);
                            if entry.modify_index > prev {
                                out.push((k.clone(), entry.modify_index, entry.value.clone()));
                            }
                        }
                    }
                    out
                };

                if !changed.is_empty() {
                    for (k, idx, bytes) in changed {
                        last_seen.insert(k.clone(), idx);
                        if let Ok(decoded) = self.codec.decode(&bytes) {
                            if !f(&k, decoded) {
                                return;
                            }
                        }
                    }
                    continue;
                }

                tokio::select! {
                    _ = cancel.canceled() => return,
                    res = rx.changed() => {
                        if res.is_err() {
                            return;
                        }
                    }
                }
            }
        })
    }
}
