//! Multi-backend KV client with runtime primary switchover and mirroring.
//!
//! Ported from github.com/grafana/dskit/kv/multi.go

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_common::services::ServiceContext;
use mimir_config::ingester::MultiKVConfig;
use parking_lot::Mutex;
use prometheus::{Counter, Gauge, GaugeVec, Opts, Registry};
use tracing::{info, warn};

use super::{BoxFuture, CasCallbackError, CasFn, CasOutcome, Codec, KvClient, KvError, Registerer};

/// Value written to the primary store, encoded with the mirror codec (or the
/// encoding error).
type EncodedMirrorValue = Result<Vec<u8>, String>;

#[derive(Clone)]
pub struct NamedKvClient {
    pub name: String,
    pub client: Arc<dyn KvClient>,
}

#[derive(Clone, Debug, Default)]
pub struct MultiRuntimeConfig {
    pub primary_store: String,
    pub mirroring: Option<bool>,
}

struct InProgressWatch {
    client_idx: usize,
    ctx: ServiceContext,
}

struct MultiMetrics {
    primary_store: GaugeVec,
    mirror_enabled: Gauge,
    mirror_writes: Counter,
    mirror_failures: Counter,
}

impl MultiMetrics {
    fn new(reg: Option<&Registerer>) -> Self {
        let primary_store = GaugeVec::new(
            Opts::new("multikv_primary_store", "Selected primary KV store"),
            &["store"],
        )
        .expect("valid metric");
        let mirror_enabled = Gauge::new(
            "multikv_mirror_enabled",
            "Is mirroring to secondary store enabled",
        )
        .expect("valid metric");
        let mirror_writes = Counter::new(
            "multikv_mirror_writes_total",
            "Number of mirror-writes to secondary store",
        )
        .expect("valid metric");
        let mirror_failures = Counter::new(
            "multikv_mirror_write_errors_total",
            "Number of failures to mirror-write to secondary store",
        )
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(primary_store.clone()));
            let _ = r.register(Box::new(mirror_enabled.clone()));
            let _ = r.register(Box::new(mirror_writes.clone()));
            let _ = r.register(Box::new(mirror_failures.clone()));
        }

        Self {
            primary_store,
            mirror_enabled,
            mirror_writes,
            mirror_failures,
        }
    }
}

pub struct MultiKv {
    clients: Vec<NamedKvClient>,
    mirror_timeout: Duration,
    mirroring_enabled: AtomicBool,
    primary_id: AtomicUsize,
    in_progress: Mutex<HashMap<usize, InProgressWatch>>,
    in_progress_cnt: AtomicUsize,
    metrics: MultiMetrics,
    /// Codec used to copy the value written to the primary store into the
    /// secondary stores when mirroring through the generic [`KvClient::cas`]
    /// (Go hands the same `interface{}` value to every store).
    mirror_codec: Option<Arc<dyn Codec>>,
}

impl MultiKv {
    pub fn new(
        cfg: MultiKVConfig,
        clients: Vec<NamedKvClient>,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        Self::with_registerer(cfg, clients, None, reg.map(Registerer::new).as_ref())
    }

    /// Go `NewMultiClient(cfg, clients, logger, reg)` with a (possibly
    /// wrapped) Go-style registerer. When `mirror_codec` is set, the generic
    /// [`KvClient::cas`] mirrors the written value to the secondary stores
    /// like Go `MultiClient.CAS`.
    pub fn with_registerer(
        cfg: MultiKVConfig,
        clients: Vec<NamedKvClient>,
        mirror_codec: Option<Arc<dyn Codec>>,
        reg: Option<&Registerer>,
    ) -> Arc<Self> {
        let m = Arc::new(Self {
            clients,
            mirror_timeout: cfg.mirror_timeout.as_std().unwrap_or(Duration::ZERO),
            mirroring_enabled: AtomicBool::new(cfg.mirror_enabled),
            primary_id: AtomicUsize::new(0),
            in_progress: Mutex::new(HashMap::new()),
            in_progress_cnt: AtomicUsize::new(0),
            metrics: MultiMetrics::new(reg),
            mirror_codec,
        });
        m.update_primary_store_gauge();
        m.update_mirror_enabled_gauge();
        m
    }

    pub fn apply_runtime_config(&self, cfg: MultiRuntimeConfig) -> Result<(), KvError> {
        if let Some(enabled) = cfg.mirroring {
            let old = self.mirroring_enabled.swap(enabled, Ordering::AcqRel);
            if old != enabled {
                info!(enabled, "toggled mirroring");
            }
            self.update_mirror_enabled_gauge();
        }
        if !cfg.primary_store.is_empty() {
            let switched = self.set_new_primary_client(&cfg.primary_store)?;
            if switched {
                info!(primary = %cfg.primary_store, "switched primary KV store");
            }
        }
        Ok(())
    }

    pub fn set_new_primary_client(&self, store: &str) -> Result<bool, KvError> {
        let new_ix = self
            .clients
            .iter()
            .position(|c| c.name == store)
            .ok_or_else(|| KvError::InvalidConfig(format!("KV store not found: {store}")))?;

        let prev = self.primary_id.swap(new_ix, Ordering::AcqRel);
        if prev == new_ix {
            return Ok(false);
        }

        self.update_primary_store_gauge();

        let to_cancel: Vec<ServiceContext> = {
            let guard = self.in_progress.lock();
            guard
                .values()
                .filter(|w| w.client_idx == prev)
                .map(|w| w.ctx.clone())
                .collect()
        };
        for ctx in to_cancel {
            ctx.cancel();
        }
        Ok(true)
    }

    fn get_primary_client(&self) -> (usize, &NamedKvClient) {
        let idx = self.primary_id.load(Ordering::Acquire);
        (idx, &self.clients[idx])
    }

    fn update_primary_store_gauge(&self) {
        let (pid, _) = self.get_primary_client();
        for (i, kv) in self.clients.iter().enumerate() {
            let val = if i == pid { 1.0 } else { 0.0 };
            self.metrics
                .primary_store
                .with_label_values(&[kv.name.as_str()])
                .set(val);
        }
    }

    fn update_mirror_enabled_gauge(&self) {
        let val = if self.mirroring_enabled.load(Ordering::Acquire) {
            1.0
        } else {
            0.0
        };
        self.metrics.mirror_enabled.set(val);
    }

    fn register_watch(&self, client_idx: usize, ctx: ServiceContext) -> usize {
        let id = self.in_progress_cnt.fetch_add(1, Ordering::Relaxed) + 1;
        self.in_progress
            .lock()
            .insert(id, InProgressWatch { client_idx, ctx });
        id
    }

    fn unregister_watch(&self, id: usize) {
        self.in_progress.lock().remove(&id);
    }
}

impl MultiKv {
    /// CAS helper that can mirror a known-clonable value to secondary stores when mirroring is enabled.
    pub async fn cas_with_mirror_clone<T, F>(
        &self,
        key: &str,
        mut f: F,
    ) -> Result<(), KvError>
    where
        T: Clone + Send + Sync + 'static,
        F: FnMut(Option<&T>) -> Result<Option<T>, CasCallbackError> + Send + 'static,
    {
        let (_, primary) = self.get_primary_client();
        let last_written: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let last_written_cb = last_written.clone();

        let cb: CasFn = Box::new(move |cur| {
            let typed = match cur {
                None => None,
                Some(a) => Some(
                    a.downcast_ref::<T>()
                        .ok_or_else(|| CasCallbackError::fatal(KvError::InvalidType("mismatch".into())))?,
                ),
            };
            match f(typed)? {
                None => {
                    *last_written_cb.lock() = None;
                    Ok(CasOutcome::NoChange)
                }
                Some(v) => {
                    *last_written_cb.lock() = Some(v.clone());
                    Ok(CasOutcome::Update(Box::new(v)))
                }
            }
        });

        let res = primary.client.cas(key, cb).await;
        if res.is_ok() && self.mirroring_enabled.load(Ordering::Acquire) {
            let maybe_val = last_written.lock().take();
            if let Some(val) = maybe_val {
                for kv in &self.clients {
                    if kv.name == primary.name {
                        continue;
                    }
                    self.metrics.mirror_writes.inc();
                    let v_clone = val.clone();
                    let sec_cb: CasFn = Box::new(move |_| Ok(CasOutcome::Update(Box::new(v_clone.clone()))));
                    let fut = kv.client.cas(key, sec_cb);
                    let r = if !self.mirror_timeout.is_zero() {
                        match tokio::time::timeout(self.mirror_timeout, fut).await {
                            Ok(r) => r,
                            Err(_) => Err(KvError::Other("mirror write timeout".to_string())),
                        }
                    } else {
                        fut.await
                    };
                    if let Err(err) = r {
                        self.metrics.mirror_failures.inc();
                        warn!(key, secondary = %kv.name, %err, "failed to mirror-write value to secondary store");
                    }
                }
            }
        }
        res
    }
}

impl KvClient for MultiKv {
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
        Box::pin(async move {
            let (_, primary) = self.get_primary_client();
            primary.client.list(prefix).await
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
        Box::pin(async move {
            let (_, primary) = self.get_primary_client();
            primary.client.get(key).await
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let (_, primary) = self.get_primary_client();
            primary.client.delete(key).await
        })
    }

    fn cas<'a>(&'a self, key: &'a str, mut f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
        Box::pin(async move {
            let (_, primary) = self.get_primary_client();
            let Some(codec) = self.mirror_codec.clone() else {
                return primary.client.cas(key, f).await;
            };
            if !self.mirroring_enabled.load(Ordering::Acquire) {
                return primary.client.cas(key, f).await;
            }

            // Capture the last value handed to the primary store (encoded, so
            // it can be re-created for each secondary store).
            let updated: Arc<Mutex<Option<EncodedMirrorValue>>> = Arc::new(Mutex::new(None));
            let updated_cb = updated.clone();
            let enc_codec = codec.clone();
            let cb: CasFn = Box::new(move |cur| {
                let out = f(cur)?;
                *updated_cb.lock() = match &out {
                    CasOutcome::NoChange => None,
                    CasOutcome::Update(v) => {
                        Some(enc_codec.encode(v.as_ref()).map_err(|e| e.to_string()))
                    }
                };
                Ok(out)
            });

            let res = primary.client.cas(key, cb).await;
            if res.is_ok() && self.mirroring_enabled.load(Ordering::Acquire) {
                let maybe = updated.lock().take();
                if let Some(encoded) = maybe {
                    for kv in &self.clients {
                        if kv.name == primary.name {
                            continue;
                        }
                        self.metrics.mirror_writes.inc();
                        let value = match &encoded {
                            Ok(bytes) => codec.decode(bytes),
                            Err(e) => Err(KvError::Codec(e.clone())),
                        };
                        let r = match value {
                            Err(e) => Err(e),
                            Ok(value) => {
                                let mut value = Some(value);
                                // try once
                                let sec_cb: CasFn = Box::new(move |_| {
                                    value.take().map(CasOutcome::Update).ok_or_else(|| {
                                        CasCallbackError::fatal(KvError::UnexpectedState(
                                            "mirror value already consumed".to_string(),
                                        ))
                                    })
                                });
                                let fut = kv.client.cas(key, sec_cb);
                                if !self.mirror_timeout.is_zero() {
                                    match tokio::time::timeout(self.mirror_timeout, fut).await {
                                        Ok(r) => r,
                                        Err(_) => Err(KvError::Other(
                                            "context deadline exceeded".to_string(),
                                        )),
                                    }
                                } else {
                                    fut.await
                                }
                            }
                        };
                        if let Err(err) = r {
                            self.metrics.mirror_failures.inc();
                            warn!(
                                key,
                                primary = %primary.name,
                                secondary = %kv.name,
                                %err,
                                "failed to update value in secondary store"
                            );
                        }
                    }
                }
            }
            res
        })
    }

    fn watch_key<'a>(
        &'a self,
        key: &'a str,
        cancel: ServiceContext,
        mut f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let shared_f = Arc::new(Mutex::new(move |val: Box<dyn Any + Send + Sync>| f(val)));
            loop {
                if cancel.is_canceled() {
                    return;
                }
                let (pid, primary) = self.get_primary_client();
                let child_ctx = cancel.child();
                let reg_id = self.register_watch(pid, child_ctx.clone());

                let stopped_by_cb = Arc::new(AtomicBool::new(false));
                let stopped_flag = stopped_by_cb.clone();
                let cb_ref = shared_f.clone();
                primary
                    .client
                    .watch_key(
                        key,
                        child_ctx.clone(),
                        Box::new(move |v| {
                            let cont = (cb_ref.lock())(v);
                            if !cont {
                                stopped_flag.store(true, Ordering::Release);
                            }
                            cont
                        }),
                    )
                    .await;
                self.unregister_watch(reg_id);

                if stopped_by_cb.load(Ordering::Acquire) || cancel.is_canceled() {
                    return;
                }
                // If child_ctx was canceled due to primary switchover, loop and re-watch on new primary.
                if !child_ctx.is_canceled() {
                    return;
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
            let shared_f = Arc::new(Mutex::new(
                move |k: &str, val: Box<dyn Any + Send + Sync>| f(k, val),
            ));
            loop {
                if cancel.is_canceled() {
                    return;
                }
                let (pid, primary) = self.get_primary_client();
                let child_ctx = cancel.child();
                let reg_id = self.register_watch(pid, child_ctx.clone());

                let stopped_by_cb = Arc::new(AtomicBool::new(false));
                let stopped_flag = stopped_by_cb.clone();
                let cb_ref = shared_f.clone();
                primary
                    .client
                    .watch_prefix(
                        prefix,
                        child_ctx.clone(),
                        Box::new(move |k, v| {
                            let cont = (cb_ref.lock())(k, v);
                            if !cont {
                                stopped_flag.store(true, Ordering::Release);
                            }
                            cont
                        }),
                    )
                    .await;
                self.unregister_watch(reg_id);

                if stopped_by_cb.load(Ordering::Acquire) || cancel.is_canceled() {
                    return;
                }
                if !child_ctx.is_canceled() {
                    return;
                }
            }
        })
    }
}
