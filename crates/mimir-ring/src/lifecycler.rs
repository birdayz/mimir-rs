//! Ring lifecycler and basic lifecycler state machines.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/basic_lifecycler.go
//! - github.com/grafana/dskit/ring/basic_lifecycler_delegates.go
//! - github.com/grafana/dskit/ring/basic_lifecycler_metrics.go
//! - github.com/grafana/dskit/ring/lifecycler.go
//! - github.com/grafana/dskit/ring/lifecycler_metrics.go
//! - github.com/grafana/dskit/ring/flush.go
//! - github.com/grafana/dskit/ring/util.go (`GetInstanceAddr`, via `crate::ring::util`)

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::services::{basic_service, ManagedBasicService, Service, ServiceContext, ServiceError, State};
use mimir_config::ingester::RingConfig;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use parking_lot::{Mutex, RwLock};
use prometheus::{Counter, Gauge, HistogramOpts, HistogramVec, Opts};
use rand::seq::SliceRandom;
use thiserror::Error;
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tracing::{debug, error, info, warn};

use crate::kv::memberlist::MemberlistKv;
use crate::kv::{
    build_kv_client, registerer_with_kv_name, AsRegisterer, BoxFuture, CasCallbackError,
    CasOutcome, KvClient, KvError, Registerer,
};
use crate::ring::model::{ring_desc_codec, system_time_to_unix_secs, DescExt, InstanceDescExt};
use crate::ring::replication_strategy::WRITE;
use crate::ring::util::{get_instance_addr, get_instance_port, join_host_port};
use crate::token_gen::{
    load_tokens_from_file, store_tokens_to_file, RandomTokenGenerator,
    SpreadMinimizingTokenGenerator, TokenGenerator,
};

#[derive(Debug, Error)]
pub enum LifecyclerError {
    #[error("transfers disabled")]
    TransferDisabled,
    #[error("lifecycler not running")]
    NotRunning,
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("KV error: {0}")]
    Kv(#[from] KvError),
    #[error("state transition disallowed: {from:?} -> {to:?}")]
    DisallowedTransition {
        from: InstanceState,
        to: InstanceState,
    },
    #[error("{0}")]
    Other(String),
}

pub trait FlushTransferer: Send + Sync {
    /// Flushes all in-memory data (`FlushTransferer.Flush()`); awaited by the lifecycler while
    /// it keeps heartbeating in LEAVING state.
    fn flush(&self) -> BoxFuture<'_, ()>;
    fn transfer_out(&self) -> BoxFuture<'_, Result<(), LifecyclerError>>;
}

#[derive(Default)]
pub struct NoopFlushTransferer;

impl FlushTransferer for NoopFlushTransferer {
    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn transfer_out(&self) -> BoxFuture<'_, Result<(), LifecyclerError>> {
        Box::pin(async { Ok(()) })
    }
}

// --- BasicLifecycler & Delegates ---

pub trait BasicLifecyclerDelegate: Send + Sync {
    fn on_ring_instance_register(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &Desc,
        instance_exists: bool,
        instance_id: &str,
        instance_desc: Option<&InstanceDesc>,
    ) -> (InstanceState, Vec<u32>);

    fn on_ring_instance_tokens(&self, lifecycler: &BasicLifecycler, tokens: &[u32]);

    fn on_ring_instance_stopping<'a>(
        &'a self,
        lifecycler: &'a BasicLifecycler,
    ) -> BoxFuture<'a, ()>;

    fn on_ring_instance_heartbeat(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &mut Desc,
        instance_desc: &mut InstanceDesc,
    );
}

#[derive(Clone)]
pub struct BasicLifecyclerConfig {
    pub id: String,
    pub addr: String,
    pub zone: String,
    pub heartbeat_period: Duration,
    pub heartbeat_timeout: Duration,
    pub tokens_observe_period: Duration,
    pub num_tokens: usize,
    pub keep_instance_in_the_ring_on_shutdown: bool,
    pub token_generator: Option<Arc<dyn TokenGenerator>>,
    pub versions: HashMap<u64, u64>,
}

impl BasicLifecyclerConfig {
    pub fn from_ring_config_tokenless(
        cfg: &RingConfig,
        listen_port: u16,
    ) -> Result<Self, LifecyclerError> {
        // pkg/ingester/ingester_ring.go ToTokenlessBasicLifecyclerConfig: the address is
        // auto-detected from the configured interfaces when not set; failing to find one is a
        // startup error (never fall back to loopback).
        let host = get_instance_addr(
            &cfg.instance_addr,
            &cfg.instance_interface_names,
            cfg.instance_enable_ipv6,
        )
        .map_err(|e| LifecyclerError::Other(e.to_string()))?;
        let port = get_instance_port(cfg.instance_port, i64::from(listen_port));
        let addr = join_host_port(&host, &port.to_string());

        Ok(Self {
            id: cfg.instance_id.clone(),
            addr,
            zone: cfg.instance_availability_zone.clone(),
            heartbeat_period: cfg.heartbeat_period.as_std().unwrap_or(Duration::ZERO),
            heartbeat_timeout: cfg.heartbeat_timeout.as_std().unwrap_or(Duration::ZERO),
            tokens_observe_period: Duration::ZERO,
            num_tokens: 0,
            keep_instance_in_the_ring_on_shutdown: !cfg.unregister_on_shutdown,
            token_generator: None,
            versions: HashMap::new(),
        })
    }
}

struct BasicLifecyclerMetrics {
    heartbeats: Counter,
    tokens_owned: Gauge,
    tokens_to_own: Gauge,
    read_only: Gauge,
}

impl BasicLifecyclerMetrics {
    fn new(ring_name: &str, reg: Option<&Registerer>) -> Self {
        let mut labels = HashMap::new();
        labels.insert("name".to_string(), ring_name.to_string());

        let heartbeats = Counter::with_opts(
            Opts::new("ring_member_heartbeats_total", "The total number of heartbeats sent.")
                .const_labels(labels.clone()),
        )
        .expect("valid metric");
        let tokens_owned = Gauge::with_opts(
            Opts::new("ring_member_tokens_owned", "The number of tokens owned in the ring.")
                .const_labels(labels.clone()),
        )
        .expect("valid metric");
        let tokens_to_own = Gauge::with_opts(
            Opts::new("ring_member_tokens_to_own", "The number of tokens to own in the ring.")
                .const_labels(labels.clone()),
        )
        .expect("valid metric");
        let read_only = Gauge::with_opts(
            Opts::new(
                "lifecycler_read_only",
                "Set to 1 if this lifecycler's instance entry is in read-only state.",
            )
            .const_labels(labels),
        )
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(heartbeats.clone()));
            let _ = r.register(Box::new(tokens_owned.clone()));
            let _ = r.register(Box::new(tokens_to_own.clone()));
            let _ = r.register(Box::new(read_only.clone()));
        }

        Self {
            heartbeats,
            tokens_owned,
            tokens_to_own,
            read_only,
        }
    }
}

pub struct BasicLifecycler {
    me: std::sync::Weak<BasicLifecycler>,
    cfg: BasicLifecyclerConfig,
    ring_name: String,
    ring_key: String,
    store: Arc<dyn KvClient>,
    delegate: Arc<dyn BasicLifecyclerDelegate>,
    metrics: BasicLifecyclerMetrics,
    curr_instance_desc: RwLock<Option<InstanceDesc>>,
    keep_on_shutdown: AtomicBool,
    token_generator: Arc<dyn TokenGenerator>,
    is_running: AtomicBool,
    op_lock: AsyncMutex<()>,
    service: RwLock<Option<std::sync::Weak<ManagedBasicService>>>,
}

impl BasicLifecycler {
    pub fn new(
        cfg: BasicLifecyclerConfig,
        ring_name: &str,
        ring_key: &str,
        store: Arc<dyn KvClient>,
        delegate: Arc<dyn BasicLifecyclerDelegate>,
        reg: Option<&dyn AsRegisterer>,
    ) -> Arc<Self> {
        let reg = reg.map(|r| r.as_registerer());
        let token_generator = cfg
            .token_generator
            .clone()
            .unwrap_or_else(|| Arc::new(RandomTokenGenerator::new()));
        let metrics = BasicLifecyclerMetrics::new(ring_name, reg.as_ref());
        metrics.tokens_to_own.set(cfg.num_tokens as f64);

        Arc::new_cyclic(|me| Self {
            me: me.clone(),
            keep_on_shutdown: AtomicBool::new(cfg.keep_instance_in_the_ring_on_shutdown),
            cfg,
            ring_name: ring_name.to_string(),
            ring_key: ring_key.to_string(),
            store,
            delegate,
            metrics,
            curr_instance_desc: RwLock::new(None),
            token_generator,
            is_running: AtomicBool::new(false),
            op_lock: AsyncMutex::new(()),
            service: RwLock::new(None),
        })
    }

    pub fn instance_id(&self) -> &str {
        &self.cfg.id
    }

    pub fn instance_addr(&self) -> &str {
        &self.cfg.addr
    }

    pub fn instance_zone(&self) -> &str {
        &self.cfg.zone
    }

    pub fn get_state(&self) -> InstanceState {
        self.curr_instance_desc
            .read()
            .as_ref()
            .map(|d| d.instance_state())
            .unwrap_or(InstanceState::Pending)
    }

    pub fn get_read_only_state(&self) -> (bool, Option<SystemTime>) {
        self.curr_instance_desc
            .read()
            .as_ref()
            .map(|d| d.get_read_only_state())
            .unwrap_or((false, None))
    }

    pub fn get_tokens(&self) -> Vec<u32> {
        self.curr_instance_desc
            .read()
            .as_ref()
            .map(|d| d.tokens.clone())
            .unwrap_or_default()
    }

    pub fn get_registered_at(&self) -> Option<SystemTime> {
        self.curr_instance_desc
            .read()
            .as_ref()
            .and_then(|d| d.get_registered_at())
    }

    pub fn is_registered(&self) -> bool {
        self.curr_instance_desc.read().is_some()
    }

    pub fn token_generator(&self) -> &Arc<dyn TokenGenerator> {
        &self.token_generator
    }

    pub fn service_state(&self) -> State {
        self.service
            .read()
            .as_ref()
            .and_then(|w| w.upgrade())
            .map(|s| s.state())
            .unwrap_or(State::New)
    }

    pub fn should_keep_instance_in_the_ring_on_shutdown(&self) -> bool {
        self.keep_on_shutdown.load(Ordering::Acquire)
    }

    pub fn set_keep_instance_in_the_ring_on_shutdown(&self, enabled: bool) {
        self.keep_on_shutdown.store(enabled, Ordering::Release);
    }

    pub async fn change_state(&self, state: InstanceState) -> Result<(), LifecyclerError> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        let op = self.op_lock.lock().await;
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        self.change_state_locked(&op, state).await
    }

    pub async fn change_state_inner(
        &self,
        state: InstanceState,
    ) -> Result<(), LifecyclerError> {
        let op = self.op_lock.lock().await;
        self.change_state_locked(&op, state).await
    }

    async fn change_state_locked(
        &self,
        op: &tokio::sync::MutexGuard<'_, ()>,
        state: InstanceState,
    ) -> Result<(), LifecyclerError> {
        self.update_instance_locked(op, move |_ring, inst| {
            if inst.instance_state() == state {
                return false;
            }
            inst.state = state as i32;
            true
        })
        .await
    }

    pub async fn change_read_only_state(&self, read_only: bool) -> Result<(), LifecyclerError> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        let op = self.op_lock.lock().await;
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        self.update_instance_locked(&op, move |_ring, inst| {
            if inst.read_only == read_only {
                return false;
            }
            inst.read_only = read_only;
            inst.read_only_updated_timestamp = system_time_to_unix_secs(SystemTime::now());
            true
        })
        .await?;
        self.metrics
            .read_only
            .set(if read_only { 1.0 } else { 0.0 });
        Ok(())
    }

    pub fn into_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        let l_start = self.clone();
        let l_run = self.clone();
        let l_stop = self.clone();

        let svc = basic_service(
            Some(Box::new(move |ctx| {
                let l = l_start.clone();
                Box::pin(async move {
                    tokio::select! {
                        biased;
                        _ = ctx.canceled() => return Err(ServiceError::Canceled),
                        res = l.register_instance() => {
                            res.map_err(|e| ServiceError::failure(format!("register instance in the ring: {e}")))?;
                        }
                    }

                    if !l.get_tokens().is_empty() && !l.cfg.tokens_observe_period.is_zero() {
                        l.wait_stable_tokens(ctx, l.cfg.tokens_observe_period)
                            .await
                            .map_err(|e| ServiceError::failure(format!("wait stable tokens: {e}")))?;
                    }

                    let toks = l.get_tokens();
                    if !toks.is_empty() {
                        l.metrics.tokens_owned.set(toks.len() as f64);
                        l.delegate.on_ring_instance_tokens(&l, &toks);
                    }
                    Ok(())
                })
            })),
            Some(Box::new(move |ctx: ServiceContext| {
                let l = l_run.clone();
                Box::pin(async move {
                    l.is_running.store(true, Ordering::Release);
                    let hb_period = l.cfg.heartbeat_period;

                    loop {
                        if hb_period.is_zero() {
                            ctx.canceled().await;
                            break;
                        } else {
                            tokio::select! {
                                biased;
                                _ = ctx.canceled() => break,
                                _ = tokio::time::sleep(hb_period) => {
                                    tokio::select! {
                                        biased;
                                        _ = ctx.canceled() => break,
                                        _ = l.heartbeat() => {}
                                    }
                                }
                            }
                        }
                    }
                    l.is_running.store(false, Ordering::Release);
                    Ok(())
                })
            })),
            Some(Box::new(move |running_err| {
                let l = l_stop.clone();
                Box::pin(async move {
                    if running_err.is_some() {
                        return Ok(());
                    }
                    // Run stopping delegate on an independent task (like Go's goroutine) so it
                    // is polled concurrently with heartbeats and cannot self-deadlock on op_lock.
                    let l_bg = l.clone();
                    let mut stop_task = tokio::spawn(async move {
                        l_bg.delegate.on_ring_instance_stopping(&l_bg).await;
                    });
                    let hb_period = l.cfg.heartbeat_period;

                    if hb_period.is_zero() {
                        if let Err(err) = stop_task.await {
                            error!(ring = %l.ring_name, %err, "on_ring_instance_stopping task panicked");
                        }
                    } else {
                        loop {
                            tokio::select! {
                                res = &mut stop_task => {
                                    if let Err(err) = res {
                                        error!(ring = %l.ring_name, %err, "on_ring_instance_stopping task panicked");
                                    }
                                    break;
                                }
                                _ = tokio::time::sleep(hb_period) => {
                                    l.heartbeat().await;
                                }
                            }
                        }
                    }

                    if !l.should_keep_instance_in_the_ring_on_shutdown() {
                        l.unregister_instance()
                            .await
                            .map_err(|e| ServiceError::failure(format!("unregister failed: {e}")))?;
                    }
                    Ok(())
                })
            })),
        )
        .with_name(format!("{} basic lifecycler", self.ring_name));
        *self.service.write() = Some(Arc::downgrade(&svc));
        svc
    }

    async fn register_instance(&self) -> Result<(), LifecyclerError> {
        self.register_instance_with_self().await
    }

    async fn register_instance_with_self(&self) -> Result<(), LifecyclerError> {
        let id = self.cfg.id.clone();
        let addr = self.cfg.addr.clone();
        let zone = self.cfg.zone.clone();
        let versions = self.cfg.versions.clone();
        let delegate = self.delegate.clone();
        let me = self.me.clone();
        let last_desc: Arc<Mutex<Option<InstanceDesc>>> = Arc::new(Mutex::new(None));
        let last_desc_cb = last_desc.clone();

        self.store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let Some(this) = me.upgrade() else {
                        return Err(CasCallbackError::fatal(KvError::Other(
                            "BasicLifecycler dropped".to_string(),
                        )));
                    };
                    let mut ring_desc = in_val
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();
                    let existing = ring_desc.ingesters.get(&id).cloned();
                    let exists = existing.is_some();

                    let (state, mut tokens) = delegate.on_ring_instance_register(
                        &this,
                        &ring_desc,
                        exists,
                        &id,
                        existing.as_ref(),
                    );
                    tokens.sort_unstable();
                    let registered_at = if let Some(ref ex) = existing {
                        ex.get_registered_at()
                    } else {
                        Some(SystemTime::now())
                    };
                    let (ro, ro_ts) = existing
                        .as_ref()
                        .map(|e| e.get_read_only_state())
                        .unwrap_or((false, None));
                    let inst_desc = ring_desc.add_ingester(
                        &id,
                        &addr,
                        &zone,
                        tokens,
                        state,
                        registered_at,
                        ro,
                        ro_ts,
                        versions.clone(),
                    );
                    *last_desc_cb.lock() = Some(inst_desc);
                    Ok(CasOutcome::Update(Box::new(ring_desc)))
                }),
            )
            .await?;

        let inst_desc = last_desc.lock().take();
        if let Some(ref d) = inst_desc {
            self.metrics
                .read_only
                .set(if d.read_only { 1.0 } else { 0.0 });
        }
        *self.curr_instance_desc.write() = inst_desc;
        Ok(())
    }

    async fn wait_stable_tokens(
        &self,
        ctx: ServiceContext,
        period: Duration,
    ) -> Result<(), LifecyclerError> {
        let mut observe_deadline = tokio::time::Instant::now() + period;
        let hb = self.cfg.heartbeat_period;
        let mut next_hb = tokio::time::Instant::now() + hb;
        loop {
            tokio::select! {
                biased;
                _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
                _ = tokio::time::sleep_until(observe_deadline) => {
                    let verified = tokio::select! {
                        biased;
                        _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
                        v = self.verify_tokens() => v,
                    };
                    if verified {
                        return Ok(());
                    }
                    observe_deadline = tokio::time::Instant::now() + period;
                }
                _ = tokio::time::sleep_until(next_hb), if !hb.is_zero() => {
                    tokio::select! {
                        biased;
                        _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
                        _ = self.heartbeat() => {}
                    }
                    next_hb = tokio::time::Instant::now() + hb;
                }
            }
        }
    }

    async fn verify_tokens(&self) -> bool {
        let verified = Arc::new(AtomicBool::new(false));
        let verified_cb = verified.clone();
        let id = self.cfg.id.clone();
        let num_tokens = self.cfg.num_tokens;
        let token_generator = self.token_generator.clone();
        let my_tokens = self.get_tokens();
        let res = self
            .update_instance(move |ring, inst| {
                // Reset on every CAS attempt so a stale match never leaks across retries.
                verified_cb.store(false, Ordering::Release);
                let (mut actual, taken) = ring.tokens_for(&id);
                let mut sorted_my = my_tokens.clone();
                sorted_my.sort_unstable();
                actual.sort_unstable();
                if actual == sorted_my {
                    verified_cb.store(true, Ordering::Release);
                    return false;
                }
                let need = num_tokens.saturating_sub(actual.len());
                let new_toks = token_generator.generate_tokens(need, &taken);
                actual.extend(new_toks);
                actual.sort_unstable();
                inst.tokens = actual;
                true
            })
            .await;
        if let Err(ref err) = res {
            error!(ring = %self.ring_name, %err, "failed to verify tokens");
        }
        res.is_ok() && verified.load(Ordering::Acquire)
    }

    async fn unregister_instance(&self) -> Result<(), LifecyclerError> {
        let _op = self.op_lock.lock().await;
        let id = self.cfg.id.clone();
        self.store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let Some(any) = in_val else {
                        return Err(CasCallbackError::fatal(KvError::Other(
                            "found empty ring when trying to unregister".to_string(),
                        )));
                    };
                    let mut desc = any
                        .downcast_ref::<Desc>()
                        .ok_or_else(|| {
                            CasCallbackError::fatal(KvError::InvalidType("expected Desc".into()))
                        })?
                        .clone();
                    desc.remove_ingester(&id);
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await?;
        *self.curr_instance_desc.write() = None;
        self.metrics.tokens_to_own.set(0.0);
        self.metrics.tokens_owned.set(0.0);
        self.metrics.read_only.set(0.0);
        Ok(())
    }

    pub async fn update_instance<F>(&self, update: F) -> Result<(), LifecyclerError>
    where
        F: FnMut(&mut Desc, &mut InstanceDesc) -> bool + Send + 'static,
    {
        let op = self.op_lock.lock().await;
        self.update_instance_locked(&op, update).await
    }

    async fn update_instance_locked<F>(
        &self,
        _op: &tokio::sync::MutexGuard<'_, ()>,
        update: F,
    ) -> Result<(), LifecyclerError>
    where
        F: FnMut(&mut Desc, &mut InstanceDesc) -> bool + Send + 'static,
    {
        let id = self.cfg.id.clone();
        let addr = self.cfg.addr.clone();
        let zone = self.cfg.zone.clone();
        let versions = self.cfg.versions.clone();
        let tokens_fallback = self.get_tokens();
        let state_fallback = self.get_state();
        let (ro, ro_ts) = self.get_read_only_state();
        let last_inst = Arc::new(Mutex::new(None));
        let last_inst_cb = last_inst.clone();
        let mut update_fn = update;

        self.store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    // Reset CAS output at the top of every retry attempt so a failed
                    // attempt never leaks its snapshot on retry.
                    *last_inst_cb.lock() = None;
                    let mut ring_desc = in_val
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();

                    let existed = ring_desc.ingesters.contains_key(&id);
                    let mut inst = if let Some(ex) = ring_desc.ingesters.get(&id).cloned() {
                        ex
                    } else {
                        ring_desc.add_ingester(
                            &id,
                            &addr,
                            &zone,
                            tokens_fallback.clone(),
                            state_fallback,
                            Some(SystemTime::now()),
                            ro,
                            ro_ts,
                            versions.clone(),
                        )
                    };

                    let prev_ts = inst.timestamp;
                    let changed = update_fn(&mut ring_desc, &mut inst);
                    if existed && !changed {
                        *last_inst_cb.lock() = Some(inst);
                        return Ok(CasOutcome::NoChange);
                    }
                    if inst.timestamp == prev_ts {
                        inst.timestamp = system_time_to_unix_secs(SystemTime::now());
                    }
                    ring_desc.ingesters.insert(id.clone(), inst.clone());
                    *last_inst_cb.lock() = Some(inst);
                    Ok(CasOutcome::Update(Box::new(ring_desc)))
                }),
            )
            .await?;

        if let Some(inst) = last_inst.lock().take() {
            *self.curr_instance_desc.write() = Some(inst);
        }
        Ok(())
    }

    async fn heartbeat(&self) {
        let delegate = self.delegate.clone();
        let me = self.me.clone();
        let res = self
            .update_instance(move |ring, inst| {
                if let Some(this) = me.upgrade() {
                    delegate.on_ring_instance_heartbeat(&this, ring, inst);
                }
                inst.timestamp = system_time_to_unix_secs(SystemTime::now());
                true
            })
            .await;
        if let Err(err) = res {
            warn!(ring = %self.ring_name, %err, "failed to heartbeat instance in the ring");
        } else {
            self.metrics.heartbeats.inc();
        }
    }
}

pub struct LeaveOnStoppingDelegate {
    next: Arc<dyn BasicLifecyclerDelegate>,
}

impl LeaveOnStoppingDelegate {
    pub fn new(next: Arc<dyn BasicLifecyclerDelegate>) -> Self {
        Self { next }
    }
}

impl BasicLifecyclerDelegate for LeaveOnStoppingDelegate {
    fn on_ring_instance_register(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &Desc,
        instance_exists: bool,
        instance_id: &str,
        instance_desc: Option<&InstanceDesc>,
    ) -> (InstanceState, Vec<u32>) {
        self.next.on_ring_instance_register(
            lifecycler,
            ring_desc,
            instance_exists,
            instance_id,
            instance_desc,
        )
    }

    fn on_ring_instance_tokens(&self, lifecycler: &BasicLifecycler, tokens: &[u32]) {
        self.next.on_ring_instance_tokens(lifecycler, tokens);
    }

    fn on_ring_instance_stopping<'a>(
        &'a self,
        lifecycler: &'a BasicLifecycler,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Err(err) = lifecycler.change_state_inner(InstanceState::Leaving).await {
                error!(%err, "failed to change instance state to LEAVING in the ring");
            }
            self.next.on_ring_instance_stopping(lifecycler).await;
        })
    }

    fn on_ring_instance_heartbeat(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &mut Desc,
        instance_desc: &mut InstanceDesc,
    ) {
        self.next
            .on_ring_instance_heartbeat(lifecycler, ring_desc, instance_desc);
    }
}

pub struct AutoForgetDelegate {
    next: Arc<dyn BasicLifecyclerDelegate>,
    forget_period: Duration,
}

impl AutoForgetDelegate {
    pub fn new(forget_period: Duration, next: Arc<dyn BasicLifecyclerDelegate>) -> Self {
        Self {
            next,
            forget_period,
        }
    }
}

impl BasicLifecyclerDelegate for AutoForgetDelegate {
    fn on_ring_instance_register(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &Desc,
        instance_exists: bool,
        instance_id: &str,
        instance_desc: Option<&InstanceDesc>,
    ) -> (InstanceState, Vec<u32>) {
        self.next.on_ring_instance_register(
            lifecycler,
            ring_desc,
            instance_exists,
            instance_id,
            instance_desc,
        )
    }

    fn on_ring_instance_tokens(&self, lifecycler: &BasicLifecycler, tokens: &[u32]) {
        self.next.on_ring_instance_tokens(lifecycler, tokens);
    }

    fn on_ring_instance_stopping<'a>(
        &'a self,
        lifecycler: &'a BasicLifecycler,
    ) -> BoxFuture<'a, ()> {
        self.next.on_ring_instance_stopping(lifecycler)
    }

    fn on_ring_instance_heartbeat(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &mut Desc,
        instance_desc: &mut InstanceDesc,
    ) {
        let now = SystemTime::now();
        let expired: Vec<String> = ring_desc
            .ingesters
            .iter()
            .filter_map(|(id, inst)| {
                let elapsed = if inst.timestamp >= 0 {
                    UNIX_EPOCH
                        .checked_add(Duration::from_secs(inst.timestamp as u64))
                        .and_then(|hb| now.duration_since(hb).ok())
                        .unwrap_or(Duration::ZERO)
                } else {
                    let before_epoch = Duration::from_secs(inst.timestamp.unsigned_abs());
                    now.duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .saturating_add(before_epoch)
                };
                if elapsed > self.forget_period {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        for id in expired {
            warn!(instance = %id, "auto-forgetting unhealthy instance from the ring");
            ring_desc.remove_ingester(&id);
        }
        self.next
            .on_ring_instance_heartbeat(lifecycler, ring_desc, instance_desc);
    }
}

pub struct InstanceRegisterDelegate {
    register_state: InstanceState,
    token_count: usize,
}

impl InstanceRegisterDelegate {
    pub fn new(register_state: InstanceState, token_count: usize) -> Self {
        Self {
            register_state,
            token_count,
        }
    }
}

impl BasicLifecyclerDelegate for InstanceRegisterDelegate {
    fn on_ring_instance_register(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &Desc,
        instance_exists: bool,
        _instance_id: &str,
        instance_desc: Option<&InstanceDesc>,
    ) -> (InstanceState, Vec<u32>) {
        let mut tokens = if instance_exists {
            instance_desc.map(|d| d.tokens.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };
        let taken = ring_desc.get_tokens();
        let need = self.token_count.saturating_sub(tokens.len());
        let new_tokens = lifecycler.token_generator().generate_tokens(need, &taken);
        tokens.extend(new_tokens);
        (self.register_state, tokens)
    }

    fn on_ring_instance_tokens(&self, _lifecycler: &BasicLifecycler, _tokens: &[u32]) {}

    fn on_ring_instance_stopping<'a>(
        &'a self,
        _lifecycler: &'a BasicLifecycler,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn on_ring_instance_heartbeat(
        &self,
        _lifecycler: &BasicLifecycler,
        _ring_desc: &mut Desc,
        _instance_desc: &mut InstanceDesc,
    ) {
    }
}

/// Persists generated ring tokens to disk and reloads them on restart if the instance is not yet
/// registered with tokens in the ring.
///
/// Ported from `github.com/grafana/dskit/ring/basic_lifecycler_delegates.go` (`TokensPersistencyDelegate`).
pub struct TokensPersistencyDelegate {
    next: Arc<dyn BasicLifecyclerDelegate>,
    tokens_path: PathBuf,
    load_state: InstanceState,
}

impl TokensPersistencyDelegate {
    pub fn new(
        tokens_path: impl Into<PathBuf>,
        load_state: InstanceState,
        next: Arc<dyn BasicLifecyclerDelegate>,
    ) -> Self {
        Self {
            next,
            tokens_path: tokens_path.into(),
            load_state,
        }
    }
}

impl BasicLifecyclerDelegate for TokensPersistencyDelegate {
    fn on_ring_instance_register(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &Desc,
        instance_exists: bool,
        instance_id: &str,
        instance_desc: Option<&InstanceDesc>,
    ) -> (InstanceState, Vec<u32>) {
        if self.tokens_path.as_os_str().is_empty() {
            info!("not loading tokens from file, tokens file path is empty");
            return self.next.on_ring_instance_register(
                lifecycler,
                ring_desc,
                instance_exists,
                instance_id,
                instance_desc,
            );
        }

        if instance_exists && instance_desc.is_some_and(|d| !d.tokens.is_empty()) {
            info!("not loading tokens from file, instance already in the ring");
            return self.next.on_ring_instance_register(
                lifecycler,
                ring_desc,
                instance_exists,
                instance_id,
                instance_desc,
            );
        }

        let tokens_from_file = match load_tokens_from_file(&self.tokens_path) {
            Ok(t) => t,
            Err(err) => {
                let is_not_found = matches!(
                    err,
                    crate::token_gen::TokenGenError::Io(ref ioe)
                        if ioe.kind() == std::io::ErrorKind::NotFound
                );
                if !is_not_found {
                    error!(%err, "error loading tokens from file");
                }
                return self.next.on_ring_instance_register(
                    lifecycler,
                    ring_desc,
                    instance_exists,
                    instance_id,
                    instance_desc,
                );
            }
        };

        let synthetic = InstanceDesc {
            id: lifecycler.instance_id().to_string(),
            addr: lifecycler.instance_addr().to_string(),
            timestamp: system_time_to_unix_secs(SystemTime::now()),
            registered_timestamp: lifecycler
                .get_registered_at()
                .map(system_time_to_unix_secs)
                .unwrap_or(0),
            state: self.load_state as i32,
            tokens: tokens_from_file,
            zone: lifecycler.instance_zone().to_string(),
            ..Default::default()
        };
        self.next.on_ring_instance_register(
            lifecycler,
            ring_desc,
            true,
            lifecycler.instance_id(),
            Some(&synthetic),
        )
    }

    fn on_ring_instance_tokens(&self, lifecycler: &BasicLifecycler, tokens: &[u32]) {
        if !self.tokens_path.as_os_str().is_empty() {
            if let Err(err) = store_tokens_to_file(tokens, &self.tokens_path) {
                error!(path = %self.tokens_path.display(), %err, "error storing tokens to disk");
            }
        }
        self.next.on_ring_instance_tokens(lifecycler, tokens);
    }

    fn on_ring_instance_stopping<'a>(
        &'a self,
        lifecycler: &'a BasicLifecycler,
    ) -> BoxFuture<'a, ()> {
        self.next.on_ring_instance_stopping(lifecycler)
    }

    fn on_ring_instance_heartbeat(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &mut Desc,
        instance_desc: &mut InstanceDesc,
    ) {
        self.next
            .on_ring_instance_heartbeat(lifecycler, ring_desc, instance_desc);
    }
}

// --- Classic Lifecycler ---

#[derive(Clone)]
pub struct LifecyclerConfig {
    pub heartbeat_period: Duration,
    pub heartbeat_timeout: Duration,
    pub observe_period: Duration,
    pub join_after: Duration,
    pub min_ready_duration: Duration,
    pub final_sleep: Duration,
    pub num_tokens: usize,
    pub tokens_file_path: String,
    pub zone: String,
    pub unregister_on_shutdown: bool,
    pub readiness_check_ring_health: bool,
    pub addr: String,
    pub port: u16,
    pub id: String,
    pub token_generator: Option<Arc<dyn TokenGenerator>>,
    pub spread_minimizing_token_generator: bool,
}

impl Default for LifecyclerConfig {
    fn default() -> Self {
        Self {
            heartbeat_period: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(60),
            observe_period: Duration::ZERO,
            join_after: Duration::ZERO,
            min_ready_duration: Duration::from_secs(15),
            final_sleep: Duration::ZERO,
            num_tokens: 128,
            tokens_file_path: String::new(),
            zone: String::new(),
            unregister_on_shutdown: true,
            readiness_check_ring_health: true,
            addr: String::new(),
            port: 0,
            id: String::new(),
            token_generator: None,
            spread_minimizing_token_generator: false,
        }
    }
}

impl LifecyclerConfig {
    pub fn from_ring_config(cfg: &RingConfig, listen_port: u16) -> Result<Self, LifecyclerError> {
        // dskit ring.NewLifecycler: GetInstanceAddr(cfg.Addr, cfg.InfNames, logger,
        // cfg.EnableInet6) (ToLifecyclerConfig sets InfNames = InstanceInterfaceNames).
        // Failing to find an address is a startup error (never fall back to loopback).
        let addr = get_instance_addr(
            &cfg.instance_addr,
            &cfg.instance_interface_names,
            cfg.instance_enable_ipv6,
        )
        .map_err(|e| LifecyclerError::Other(e.to_string()))?;
        let port = get_instance_port(cfg.instance_port, i64::from(listen_port)) as u16;

        let is_spread_minimizing = cfg.token_generation_strategy == "spread-minimizing";
        let token_generator: Arc<dyn TokenGenerator> = if is_spread_minimizing {
            let zones = cfg.spread_minimizing_zones.0.clone();
            Arc::new(
                SpreadMinimizingTokenGenerator::new(
                    &cfg.instance_id,
                    &cfg.instance_availability_zone,
                    &zones,
                    cfg.spread_minimizing_join_ring_in_order,
                )
                .map_err(|e| LifecyclerError::InvalidConfig(e.to_string()))?,
            )
        } else {
            Arc::new(RandomTokenGenerator::new())
        };

        Ok(Self {
            heartbeat_period: cfg.heartbeat_period.as_std().unwrap_or(Duration::ZERO),
            heartbeat_timeout: cfg.heartbeat_timeout.as_std().unwrap_or(Duration::ZERO),
            observe_period: cfg.observe_period.as_std().unwrap_or(Duration::ZERO),
            join_after: Duration::ZERO,
            min_ready_duration: cfg.min_ready_duration.as_std().unwrap_or(Duration::ZERO),
            final_sleep: cfg.final_sleep.as_std().unwrap_or(Duration::ZERO),
            num_tokens: cfg.num_tokens.max(0) as usize,
            tokens_file_path: cfg.tokens_file_path.clone(),
            zone: cfg.instance_availability_zone.clone(),
            unregister_on_shutdown: cfg.unregister_on_shutdown,
            readiness_check_ring_health: false,
            addr,
            port,
            id: cfg.instance_id.clone(),
            token_generator: Some(token_generator),
            spread_minimizing_token_generator: is_spread_minimizing,
        })
    }

    pub fn set_token_generator<T: TokenGenerator + 'static>(&mut self, generator: Arc<T>) {
        self.spread_minimizing_token_generator =
            std::any::TypeId::of::<T>() == std::any::TypeId::of::<SpreadMinimizingTokenGenerator>();
        self.token_generator = Some(generator);
    }

    /// Ported from `github.com/grafana/dskit/ring/lifecycler.go` (`LifecyclerConfig.Validate`).
    pub fn validate(&self) -> Result<(), LifecyclerError> {
        if self.spread_minimizing_token_generator && !self.tokens_file_path.is_empty() {
            return Err(LifecyclerError::InvalidConfig(
                "you can't configure the tokens file path when using the spread minimizing token strategy. Please set the tokens file path to an empty string".to_string(),
            ));
        }
        if self.heartbeat_period.is_zero() {
            return Err(LifecyclerError::InvalidConfig(
                "heartbeat period must be greater than 0".to_string(),
            ));
        }
        if self.heartbeat_timeout.is_zero() {
            return Err(LifecyclerError::InvalidConfig(
                "heartbeat timeout must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

struct LifecyclerMetrics {
    consul_heartbeats: Counter,
    shutdown_duration: HistogramVec,
    readonly: Gauge,
}

impl LifecyclerMetrics {
    fn new(ring_name: &str, reg: Option<&Registerer>) -> Self {
        let mut labels = HashMap::new();
        labels.insert("name".to_string(), ring_name.to_string());

        let consul_heartbeats = Counter::with_opts(
            Opts::new(
                "member_consul_heartbeats_total",
                "The total number of heartbeats sent to consul.",
            )
            .const_labels(labels.clone()),
        )
        .expect("valid metric");
        let shutdown_duration = HistogramVec::new(
            HistogramOpts::new(
                "shutdown_duration_seconds",
                "Duration (in seconds) of shutdown procedure (ie transfer or flush).",
            )
            .buckets(vec![10.0, 20.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0])
            .const_labels(labels.clone()),
            &["op", "status"],
        )
        .expect("valid metric");
        let readonly = Gauge::with_opts(
            Opts::new(
                "lifecycler_read_only",
                "Set to 1 if this lifecycler's instance entry is in read-only state.",
            )
            .const_labels(labels),
        )
        .expect("valid metric");

        if let Some(r) = reg {
            let _ = r.register(Box::new(consul_heartbeats.clone()));
            let _ = r.register(Box::new(shutdown_duration.clone()));
            let _ = r.register(Box::new(readonly.clone()));
        }

        Self {
            consul_heartbeats,
            shutdown_duration,
            readonly,
        }
    }
}

struct LifecyclerState {
    state: InstanceState,
    tokens: Vec<u32>,
    registered_at: Option<SystemTime>,
    read_only: bool,
    read_only_last_updated: Option<SystemTime>,
}

struct LifecyclerCounters {
    healthy_instances_count: usize,
    instances_count: usize,
    read_only_instances_count: usize,
    healthy_instances_in_zone_count: usize,
    instances_in_zone_count: usize,
    zones_count: usize,
    zones: Vec<String>,
}

struct ReadyState {
    ready: bool,
    ready_since: Option<Instant>,
}

pub struct Lifecycler {
    cfg: LifecyclerConfig,
    flush_transferer: RwLock<Arc<dyn FlushTransferer>>,
    kv_store: Arc<dyn KvClient>,
    pub id: String,
    pub addr: String,
    pub ring_name: String,
    pub ring_key: String,
    pub zone: String,

    flush_on_shutdown: AtomicBool,
    unregister_on_shutdown: AtomicBool,
    clear_tokens_on_shutdown: AtomicBool,

    state: RwLock<LifecyclerState>,
    ready: Mutex<ReadyState>,
    counters: RwLock<LifecyclerCounters>,
    token_generator: Arc<dyn TokenGenerator>,
    can_join_timeout: RwLock<Duration>,
    metrics: LifecyclerMetrics,
    is_running: AtomicBool,
    op_lock: AsyncMutex<()>,
}

impl Lifecycler {
    /// Go `ring.NewLifecycler`. `reg` is the Go registerer (Mimir's ingester
    /// passes `cortex_`-prefixed); the KV client registers its metrics with
    /// `kv_name="<ring_name>-lifecycler"` like dskit. The lifecycler's own
    /// metrics are registered through `reg` too (and thus carry its prefix).
    #[allow(clippy::too_many_arguments)]
    pub fn new<R: AsRegisterer + ?Sized>(
        ring_cfg: &RingConfig,
        listen_port: u16,
        flush_transferer: Option<Arc<dyn FlushTransferer>>,
        ring_name: &str,
        ring_key: &str,
        flush_on_shutdown: bool,
        memberlist_kv: Option<Arc<MemberlistKv>>,
        reg: Option<&R>,
    ) -> Result<Arc<Self>, LifecyclerError> {
        let registerer = reg.map(|r| r.as_registerer());
        let kv_reg =
            registerer_with_kv_name(registerer.as_ref(), &format!("{ring_name}-lifecycler"));
        let store = build_kv_client(
            &ring_cfg.kvstore,
            ring_desc_codec(),
            memberlist_kv,
            kv_reg.as_ref(),
        )?;
        let cfg = LifecyclerConfig::from_ring_config(ring_cfg, listen_port)?;
        cfg.validate()?;
        Ok(Self::with_store(
            cfg,
            store,
            flush_transferer,
            ring_name,
            ring_key,
            flush_on_shutdown,
            registerer.as_ref().map(|r| r as &dyn AsRegisterer),
        ))
    }

    pub fn with_store(
        cfg: LifecyclerConfig,
        kv_store: Arc<dyn KvClient>,
        flush_transferer: Option<Arc<dyn FlushTransferer>>,
        ring_name: &str,
        ring_key: &str,
        flush_on_shutdown: bool,
        reg: Option<&dyn AsRegisterer>,
    ) -> Arc<Self> {
        let reg = reg.map(|r| r.as_registerer());
        // net.JoinHostPort(addr, strconv.Itoa(port)).
        let full_addr = join_host_port(&cfg.addr, &cfg.port.to_string());
        let token_generator = cfg
            .token_generator
            .clone()
            .unwrap_or_else(|| Arc::new(RandomTokenGenerator::new()));

        Arc::new(Self {
            id: cfg.id.clone(),
            addr: full_addr,
            ring_name: ring_name.to_string(),
            ring_key: ring_key.to_string(),
            zone: cfg.zone.clone(),
            flush_on_shutdown: AtomicBool::new(flush_on_shutdown),
            unregister_on_shutdown: AtomicBool::new(cfg.unregister_on_shutdown),
            clear_tokens_on_shutdown: AtomicBool::new(false),
            state: RwLock::new(LifecyclerState {
                state: InstanceState::Pending,
                tokens: Vec::new(),
                registered_at: None,
                read_only: false,
                read_only_last_updated: None,
            }),
            ready: Mutex::new(ReadyState {
                ready: false,
                ready_since: None,
            }),
            counters: RwLock::new(LifecyclerCounters {
                healthy_instances_count: 0,
                instances_count: 0,
                read_only_instances_count: 0,
                healthy_instances_in_zone_count: 0,
                instances_in_zone_count: 0,
                zones_count: 0,
                zones: Vec::new(),
            }),
            token_generator,
            can_join_timeout: RwLock::new(Duration::from_secs(5 * 60)),
            metrics: LifecyclerMetrics::new(ring_name, reg.as_ref()),
            is_running: AtomicBool::new(false),
            op_lock: AsyncMutex::new(()),
            cfg,
            flush_transferer: RwLock::new(
                flush_transferer.unwrap_or_else(|| Arc::new(NoopFlushTransferer)),
            ),
            kv_store,
        })
    }

    pub fn get_state(&self) -> InstanceState {
        self.state.read().state
    }

    pub fn get_tokens(&self) -> Vec<u32> {
        self.state.read().tokens.clone()
    }

    pub fn get_registered_at(&self) -> Option<SystemTime> {
        self.state.read().registered_at
    }

    pub fn token_generator(&self) -> &Arc<dyn TokenGenerator> {
        &self.token_generator
    }

    pub fn set_can_join_timeout(&self, timeout: Duration) {
        *self.can_join_timeout.write() = timeout;
    }

    pub fn can_join_timeout(&self) -> Duration {
        *self.can_join_timeout.read()
    }

    pub fn get_read_only_state(&self) -> (bool, Option<SystemTime>) {
        let st = self.state.read();
        (st.read_only, st.read_only_last_updated)
    }

    pub fn healthy_instances_count(&self) -> usize {
        self.counters.read().healthy_instances_count
    }

    pub fn instances_count(&self) -> usize {
        self.counters.read().instances_count
    }

    pub fn read_only_instances_count(&self) -> usize {
        self.counters.read().read_only_instances_count
    }

    pub fn healthy_instances_in_zone_count(&self) -> usize {
        self.counters.read().healthy_instances_in_zone_count
    }

    pub fn instances_in_zone_count(&self) -> usize {
        self.counters.read().instances_in_zone_count
    }

    pub fn zones_count(&self) -> usize {
        self.counters.read().zones_count
    }

    pub fn zones(&self) -> Vec<String> {
        self.counters.read().zones.clone()
    }

    /// Replaces the [`FlushTransferer`] used on shutdown. Mimir passes the Ingester itself to
    /// `ring.NewLifecycler`; in Rust the Ingester is constructed after its lifecycler, so it
    /// registers itself here before the lifecycler is started.
    pub fn set_flush_transferer(&self, flush_transferer: Arc<dyn FlushTransferer>) {
        *self.flush_transferer.write() = flush_transferer;
    }

    pub fn set_flush_on_shutdown(&self, enabled: bool) {
        self.flush_on_shutdown.store(enabled, Ordering::Release);
    }

    pub fn flush_on_shutdown(&self) -> bool {
        self.flush_on_shutdown.load(Ordering::Acquire)
    }

    pub fn set_unregister_on_shutdown(&self, enabled: bool) {
        self.unregister_on_shutdown.store(enabled, Ordering::Release);
    }

    pub fn should_unregister_on_shutdown(&self) -> bool {
        self.unregister_on_shutdown.load(Ordering::Acquire)
    }

    pub fn set_clear_tokens_on_shutdown(&self, enabled: bool) {
        self.clear_tokens_on_shutdown
            .store(enabled, Ordering::Release);
    }

    pub fn clear_tokens_on_shutdown(&self) -> bool {
        self.clear_tokens_on_shutdown.load(Ordering::Acquire)
    }

    pub fn set_tokens(&self, tokens: Vec<u32>) {
        {
            let mut st = self.state.write();
            st.tokens = tokens.clone();
        }
        if !self.cfg.tokens_file_path.is_empty() {
            let path = PathBuf::from(&self.cfg.tokens_file_path);
            if let Err(err) = store_tokens_to_file(&tokens, &path) {
                error!(path = %self.cfg.tokens_file_path, %err, "error storing tokens to disk");
            }
        }
    }

    fn set_read_only_internal(&self, read_only: bool, updated: Option<SystemTime>) {
        {
            let mut st = self.state.write();
            st.read_only = read_only;
            st.read_only_last_updated = updated;
        }
        self.metrics.readonly.set(if read_only { 1.0 } else { 0.0 });
    }

    pub async fn check_ready(&self) -> Result<(), String> {
        {
            let ready = self.ready.lock();
            if ready.ready {
                return Ok(());
            }
        }

        if let Err(err) = self.check_ring_health_for_readiness().await {
            self.ready.lock().ready_since = None;
            return Err(err);
        }

        let mut ready = self.ready.lock();
        if ready.ready {
            return Ok(());
        }
        let since = *ready.ready_since.get_or_insert_with(Instant::now);
        if since.elapsed() < self.cfg.min_ready_duration {
            let dur_str = mimir_config::format_go_duration(
                self.cfg.min_ready_duration.as_nanos().min(i64::MAX as u128) as i64,
            );
            return Err(format!("waiting for {dur_str} after being ready"));
        }
        ready.ready = true;
        Ok(())
    }

    async fn check_ring_health_for_readiness(&self) -> Result<(), String> {
        if self.get_tokens().is_empty() {
            return Err("this instance owns no tokens".to_string());
        }
        let desc_any = self
            .kv_store
            .get(&self.ring_key)
            .await
            .map_err(|e| format!("error talking to the KV store: {e}"))?
            .ok_or_else(|| "no ring returned from the KV store".to_string())?;
        let desc = desc_any
            .downcast::<Desc>()
            .map_err(|_| "no ring returned from the KV store".to_string())?;

        let now = SystemTime::now();
        if self.cfg.readiness_check_ring_health {
            desc.is_ready(now, self.cfg.heartbeat_timeout)
        } else {
            let inst = desc
                .ingesters
                .get(&self.id)
                .ok_or_else(|| format!("instance {} not found in the ring", self.id))?;
            inst.is_ready(now, self.cfg.heartbeat_timeout)
        }
    }

    pub async fn change_state(&self, target: InstanceState) -> Result<(), LifecyclerError> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        let op = self.op_lock.lock().await;
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        self.change_state_inner_locked(&op, target).await
    }

    async fn change_state_inner(&self, target: InstanceState) -> Result<(), LifecyclerError> {
        let op = self.op_lock.lock().await;
        self.change_state_inner_locked(&op, target).await
    }

    async fn change_state_inner_locked(
        &self,
        _op: &tokio::sync::MutexGuard<'_, ()>,
        target: InstanceState,
    ) -> Result<(), LifecyclerError> {
        let curr = self.get_state();
        let allowed = matches!(
            (curr, target),
            (InstanceState::Pending, InstanceState::Joining)
                | (InstanceState::Joining, InstanceState::Pending)
                | (InstanceState::Joining, InstanceState::Active)
                | (InstanceState::Pending, InstanceState::Active)
                | (InstanceState::Active, InstanceState::Leaving)
        );
        if !allowed {
            return Err(LifecyclerError::DisallowedTransition {
                from: curr,
                to: target,
            });
        }
        info!(
            old_state = curr.as_str_name(),
            new_state = target.as_str_name(),
            ring = %self.ring_name,
            "changing instance state from"
        );
        self.state.write().state = target;
        self.update_consul_locked().await
    }

    pub async fn change_read_only_state(&self, read_only: bool) -> Result<(), LifecyclerError> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        let _op = self.op_lock.lock().await;
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        let (prev, _) = self.get_read_only_state();
        if prev == read_only {
            return Ok(());
        }
        info!(readOnly = read_only, ring = %self.ring_name, "changing read-only state of instance in the ring");
        self.set_read_only_internal(read_only, Some(SystemTime::now()));
        self.update_consul_locked().await
    }

    pub async fn claim_tokens_for(&self, source_ingester_id: &str) -> Result<(), LifecyclerError> {
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        let _op = self.op_lock.lock().await;
        if !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        let my_id = self.id.clone();
        let src_id = source_ingester_id.to_string();
        let claimed = Arc::new(Mutex::new(Vec::new()));
        let claimed_cb = claimed.clone();

        self.kv_store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let Some(any) = in_val else {
                        return Err(CasCallbackError::fatal(KvError::Other(
                            "cannot claim tokens in an empty ring".to_string(),
                        )));
                    };
                    let mut desc = any
                        .downcast_ref::<Desc>()
                        .ok_or_else(|| {
                            CasCallbackError::fatal(KvError::InvalidType("expected Desc".into()))
                        })?
                        .clone();
                    let mut toks = desc.claim_tokens(&src_id, &my_id);
                    toks.sort_unstable();
                    if let Some(ing) = desc.ingesters.get_mut(&my_id) {
                        ing.timestamp = system_time_to_unix_secs(SystemTime::now());
                        ing.tokens = toks.clone();
                    }
                    *claimed_cb.lock() = toks;
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await?;

        let toks = claimed.lock().clone();
        self.set_tokens(toks);
        Ok(())
    }

    pub async fn init_ring(&self) -> Result<(), LifecyclerError> {
        let tokens_from_file = if !self.cfg.tokens_file_path.is_empty() {
            let p = PathBuf::from(&self.cfg.tokens_file_path);
            match load_tokens_from_file(&p) {
                Ok(t) => t,
                Err(crate::token_gen::TokenGenError::Io(ref e))
                    if e.kind() == std::io::ErrorKind::NotFound =>
                {
                    Vec::new()
                }
                Err(e) => {
                    error!(err = %e, "error loading tokens from file");
                    Vec::new()
                }
            }
        } else {
            info!("not loading tokens from file, tokens file path is empty");
            Vec::new()
        };

        let ring_name = self.ring_name.clone();
        let id = self.id.clone();
        let addr = self.addr.clone();
        let zone = self.zone.clone();
        let num_tokens = self.cfg.num_tokens;
        let token_generator = self.token_generator.clone();
        let last_desc = Arc::new(Mutex::new(None));
        let last_desc_cb = last_desc.clone();
        let init_state = self.get_state();
        let init_reg = self.state.read().registered_at;
        let init_ro = self.get_read_only_state();
        let picked_state = Arc::new(Mutex::new(init_state));
        let picked_tokens: Arc<Mutex<Option<Vec<u32>>>> = Arc::new(Mutex::new(None));
        let picked_reg = Arc::new(Mutex::new(init_reg));
        let picked_ro = Arc::new(Mutex::new(init_ro));

        let ps_cb = picked_state.clone();
        let pt_cb = picked_tokens.clone();
        let pr_cb = picked_reg.clone();
        let pro_cb = picked_ro.clone();

        self.kv_store
            .cas(
                &self.ring_key,
                Box::new(move |cur| {
                    // Reset CAS outputs at the start of every retry attempt so a failed
                    // attempt's picked state/tokens never leak into the winning attempt.
                    *ps_cb.lock() = init_state;
                    *pt_cb.lock() = None;
                    *pr_cb.lock() = init_reg;
                    *pro_cb.lock() = init_ro;

                    let mut ring_desc = cur
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();

                    let mut needs_write = true;
                    if let Some(mut inst) = ring_desc.ingesters.get(&id).cloned() {
                        let reg_at = inst.get_registered_at();
                        let (ro, ro_ts) = inst.get_read_only_state();
                        *pr_cb.lock() = reg_at;
                        *pro_cb.lock() = (ro, ro_ts);

                        if inst.instance_state() == InstanceState::Joining {
                            warn!(ring = %ring_name, "instance found in ring as JOINING, setting to PENDING");
                            inst.id = id.clone();
                            inst.addr = addr.clone();
                            inst.zone = zone.clone();
                            inst.state = InstanceState::Pending as i32;
                            let now_s = system_time_to_unix_secs(SystemTime::now());
                            inst.timestamp = if inst.timestamp == now_s {
                                now_s.saturating_add(1)
                            } else {
                                now_s
                            };
                            *ps_cb.lock() = InstanceState::Pending;
                            ring_desc.ingesters.insert(id.clone(), inst);
                        } else {
                            let mut toks = inst.tokens.clone();
                            info!(
                                state = inst.instance_state().as_str_name(),
                                tokens = toks.len(),
                                ring = %ring_name,
                                readOnly = ro,
                                readOnlyStateUpdate = %ro_ts.map_or_else(
                                    || "0001-01-01T00:00:00Z".to_string(),
                                    mimir_common::logging::format_rfc3339_nano_utc,
                                ),
                                "existing instance found in ring"
                            );
                            if inst.instance_state() == InstanceState::Leaving {
                                if toks.len() < num_tokens {
                                    info!(
                                        current_tokens = toks.len(),
                                        desired_tokens = num_tokens,
                                        "existing instance has too few tokens, adding difference"
                                    );
                                    let new_toks = token_generator
                                        .generate_tokens(num_tokens - toks.len(), &ring_desc.get_tokens());
                                    toks.extend(new_toks);
                                    toks.sort_unstable();
                                } else if toks.len() > num_tokens {
                                    info!(
                                        current_tokens = toks.len(),
                                        desired_tokens = num_tokens,
                                        "existing instance has too many tokens, removing difference"
                                    );
                                    toks.shuffle(&mut rand::thread_rng());
                                    toks.truncate(num_tokens);
                                    toks.sort_unstable();
                                }
                                inst.state = InstanceState::Active as i32;
                                inst.tokens = toks.clone();
                            }

                            *ps_cb.lock() = inst.instance_state();
                            *pt_cb.lock() = Some(toks);
                            let orig = ring_desc.ingesters.get(&id).cloned();
                            inst.id = id.clone();
                            inst.addr = addr.clone();
                            inst.zone = zone.clone();

                            if orig.as_ref() == Some(&inst) {
                                needs_write = false;
                            } else {
                                inst.timestamp = system_time_to_unix_secs(SystemTime::now());
                                ring_desc.ingesters.insert(id.clone(), inst);
                            }
                        }
                    } else {
                        let now = SystemTime::now();
                        *pr_cb.lock() = Some(now);
                        *pro_cb.lock() = (false, None);

                        if !tokens_from_file.is_empty() {
                            info!(num_tokens = tokens_from_file.len(), "adding tokens from file");
                            let st = if tokens_from_file.len() >= num_tokens {
                                InstanceState::Active
                            } else {
                                *ps_cb.lock()
                            };
                            *ps_cb.lock() = st;
                            ring_desc.add_ingester(
                                &id,
                                &addr,
                                &zone,
                                tokens_from_file.clone(),
                                st,
                                Some(now),
                                false,
                                None,
                                HashMap::new(),
                            );
                            *pt_cb.lock() = Some(tokens_from_file.clone());
                        } else {
                            info!(ring = %ring_name, "instance not found in ring, adding with no tokens");
                            let st = *ps_cb.lock();
                            ring_desc.add_ingester(
                                &id,
                                &addr,
                                &zone,
                                Vec::new(),
                                st,
                                Some(now),
                                false,
                                None,
                                HashMap::new(),
                            );
                        }
                    }

                    *last_desc_cb.lock() = Some(ring_desc.clone());
                    if !needs_write {
                        Ok(CasOutcome::NoChange)
                    } else {
                        Ok(CasOutcome::Update(Box::new(ring_desc)))
                    }
                }),
            )
            .await?;

        self.state.write().state = *picked_state.lock();
        self.state.write().registered_at = *picked_reg.lock();
        let (ro, ro_ts) = *picked_ro.lock();
        self.set_read_only_internal(ro, ro_ts);
        if let Some(toks) = picked_tokens.lock().take() {
            self.set_tokens(toks);
        }
        if let Some(d) = last_desc.lock().take() {
            self.update_counters(&d);
        }
        Ok(())
    }

    pub async fn wait_before_joining(&self, ctx: &ServiceContext) -> Result<(), LifecyclerError> {
        if !self.token_generator.can_join_enabled() {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.can_join_timeout();
        loop {
            if ctx.is_canceled() {
                return Err(LifecyclerError::NotRunning);
            }
            let get_res = tokio::select! {
                biased;
                _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
                _ = tokio::time::sleep_until(deadline) => break,
                res = self.kv_store.get(&self.ring_key) => res,
            };
            if let Ok(Some(any)) = get_res {
                if let Ok(desc) = any.downcast::<Desc>() {
                    if self.token_generator.can_join(&desc.ingesters).is_ok() {
                        break;
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::select! {
                biased;
                _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
                _ = tokio::time::sleep_until(deadline) => break,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
        if ctx.is_canceled() {
            return Err(LifecyclerError::NotRunning);
        }
        Ok(())
    }

    async fn auto_join(
        &self,
        ctx: &ServiceContext,
        target_state: InstanceState,
    ) -> Result<(), LifecyclerError> {
        self.wait_before_joining(ctx).await?;

        let _op = tokio::select! {
            biased;
            _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
            g = self.op_lock.lock() => g,
        };
        if ctx.is_canceled() || !self.is_running.load(Ordering::Acquire) {
            return Err(LifecyclerError::NotRunning);
        }
        if self.get_state() != InstanceState::Pending {
            return Ok(());
        }
        let id = self.id.clone();
        let addr = self.addr.clone();
        let zone = self.zone.clone();
        let num_tokens = self.cfg.num_tokens;
        let token_generator = self.token_generator.clone();
        let reg_at = self.state.read().registered_at;
        let (ro, ro_ts) = self.get_read_only_state();
        let last_desc = Arc::new(Mutex::new(None));
        let last_desc_cb = last_desc.clone();
        let final_tokens = Arc::new(Mutex::new(Vec::new()));
        let final_tokens_cb = final_tokens.clone();

        tokio::select! {
            biased;
            _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
            res = self.kv_store.cas(
                &self.ring_key,
                Box::new(move |cur| {
                    let mut ring_desc = cur
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();

                    let (mut my_tokens, taken) = ring_desc.tokens_for(&id);
                    let need = num_tokens.saturating_sub(my_tokens.len());
                    let new_toks = token_generator.generate_tokens(need, &taken);
                    my_tokens.extend(new_toks);
                    my_tokens.sort_unstable();

                    ring_desc.add_ingester(
                        &id,
                        &addr,
                        &zone,
                        my_tokens.clone(),
                        target_state,
                        reg_at,
                        ro,
                        ro_ts,
                        HashMap::new(),
                    );
                    *final_tokens_cb.lock() = my_tokens;
                    *last_desc_cb.lock() = Some(ring_desc.clone());
                    Ok(CasOutcome::Update(Box::new(ring_desc)))
                }),
            ) => res?,
        }

        self.state.write().state = target_state;
        let toks = std::mem::take(&mut *final_tokens.lock());
        self.set_tokens(toks);
        if let Some(d) = last_desc.lock().take() {
            self.update_counters(&d);
        }
        Ok(())
    }

    async fn verify_tokens(&self) -> bool {
        let _op = self.op_lock.lock().await;
        let id = self.id.clone();
        let addr = self.addr.clone();
        let zone = self.zone.clone();
        let num_tokens = self.cfg.num_tokens;
        let token_generator = self.token_generator.clone();
        let st = self.get_state();
        let reg_at = self.state.read().registered_at;
        let (ro, ro_ts) = self.get_read_only_state();
        let mut my_tokens = self.get_tokens();
        my_tokens.sort_unstable();
        let verified = Arc::new(AtomicBool::new(false));
        let verified_cb = verified.clone();
        let new_tokens_out: Arc<Mutex<Option<Vec<u32>>>> = Arc::new(Mutex::new(None));
        let new_tokens_cb = new_tokens_out.clone();

        let res = self
            .kv_store
            .cas(
                &self.ring_key,
                Box::new(move |cur| {
                    // Reset CAS outputs at the top of every retry attempt so uncommitted
                    // conflict tokens from an earlier failed attempt never leak.
                    verified_cb.store(false, Ordering::Release);
                    *new_tokens_cb.lock() = None;
                    let mut desc = cur
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();
                    let (mut ring_tokens, taken) = desc.tokens_for(&id);
                    ring_tokens.sort_unstable();
                    if ring_tokens == my_tokens {
                        verified_cb.store(true, Ordering::Release);
                        return Ok(CasOutcome::NoChange);
                    }
                    let need = num_tokens.saturating_sub(ring_tokens.len());
                    let new_toks = token_generator.generate_tokens(need, &taken);
                    ring_tokens.extend(new_toks);
                    ring_tokens.sort_unstable();
                    *new_tokens_cb.lock() = Some(ring_tokens.clone());
                    desc.add_ingester(
                        &id,
                        &addr,
                        &zone,
                        ring_tokens,
                        st,
                        reg_at,
                        ro,
                        ro_ts,
                        HashMap::new(),
                    );
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await;

        match res {
            Ok(()) => {
                if let Some(toks) = new_tokens_out.lock().take() {
                    self.set_tokens(toks);
                }
                verified.load(Ordering::Acquire)
            }
            Err(err) => {
                error!(ring = %self.ring_name, %err, "failed to verify tokens");
                false
            }
        }
    }

    async fn update_consul(&self) -> Result<(), LifecyclerError> {
        let _op = self.op_lock.lock().await;
        self.update_consul_locked().await
    }

    async fn update_consul_locked(&self) -> Result<(), LifecyclerError> {
        let id = self.id.clone();
        let addr = self.addr.clone();
        let zone = self.zone.clone();
        let state = self.get_state();
        let tokens_fallback = self.get_tokens();
        let (ro, ro_ts) = self.get_read_only_state();
        let registered_at = self.state.read().registered_at;
        let picked_reg = Arc::new(Mutex::new(registered_at));
        let picked_reg_cb = picked_reg.clone();
        let updated_desc = Arc::new(Mutex::new(None));
        let updated_desc_cb = updated_desc.clone();

        self.kv_store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let mut desc = in_val
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();
                    let (toks, reg) = if let Some(ex) = desc.ingesters.get(&id) {
                        *picked_reg_cb.lock() = registered_at;
                        (ex.tokens.clone(), registered_at)
                    } else {
                        let now = Some(SystemTime::now());
                        *picked_reg_cb.lock() = now;
                        (tokens_fallback.clone(), now)
                    };
                    desc.add_ingester(
                        &id,
                        &addr,
                        &zone,
                        toks,
                        state,
                        reg,
                        ro,
                        ro_ts,
                        HashMap::new(),
                    );
                    *updated_desc_cb.lock() = Some(desc.clone());
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await?;

        self.state.write().registered_at = *picked_reg.lock();
        if let Some(d) = updated_desc.lock().take() {
            self.update_counters(&d);
        }
        Ok(())
    }

    async fn unregister(&self) -> Result<(), LifecyclerError> {
        let _op = self.op_lock.lock().await;
        let id = self.id.clone();
        self.kv_store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let Some(any) = in_val else {
                        return Err(CasCallbackError::fatal(KvError::Other(
                            "found empty ring when trying to unregister".to_string(),
                        )));
                    };
                    let mut desc = any
                        .downcast_ref::<Desc>()
                        .ok_or_else(|| {
                            CasCallbackError::fatal(KvError::InvalidType("expected Desc".into()))
                        })?
                        .clone();
                    desc.remove_ingester(&id);
                    Ok(CasOutcome::Update(Box::new(desc)))
                }),
            )
            .await?;
        Ok(())
    }

    fn update_counters(&self, ring_desc: &Desc) {
        let now = SystemTime::now();
        let mut healthy = 0usize;
        let mut read_only = 0usize;
        let mut zones: HashMap<String, usize> = HashMap::new();
        let mut healthy_in_zone: HashMap<String, usize> = HashMap::new();

        for ing in ring_desc.ingesters.values() {
            *zones.entry(ing.zone.clone()).or_insert(0) += 1;
            if ing.read_only {
                read_only += 1;
            }
            if ing.is_healthy(WRITE, self.cfg.heartbeat_timeout, now) {
                healthy += 1;
                *healthy_in_zone.entry(ing.zone.clone()).or_insert(0) += 1;
            }
        }
        let mut zone_names: Vec<String> = zones.keys().cloned().collect();
        zone_names.sort();

        let mut c = self.counters.write();
        c.healthy_instances_count = healthy;
        c.instances_count = ring_desc.ingesters.len();
        c.read_only_instances_count = read_only;
        c.healthy_instances_in_zone_count = healthy_in_zone.get(&self.zone).copied().unwrap_or(0);
        c.instances_in_zone_count = zones.get(&self.zone).copied().unwrap_or(0);
        c.zones_count = zone_names.len();
        c.zones = zone_names;
    }

    async fn process_shutdown(&self) {
        let mut flush_required = self.flush_on_shutdown();
        let t_start = Instant::now();
        let flush_transferer = self.flush_transferer.read().clone();
        match flush_transferer.transfer_out().await {
            Ok(()) => {
                flush_required = false;
                self.metrics
                    .shutdown_duration
                    .with_label_values(&["transfer", "success"])
                    .observe(t_start.elapsed().as_secs_f64());
            }
            Err(LifecyclerError::TransferDisabled) => {
                info!("transfers are disabled");
            }
            Err(err) => {
                error!(ring = %self.ring_name, %err, "failed to transfer chunks to another instance");
                self.metrics
                    .shutdown_duration
                    .with_label_values(&["transfer", "fail"])
                    .observe(t_start.elapsed().as_secs_f64());
            }
        }

        if flush_required {
            let f_start = Instant::now();
            flush_transferer.flush().await;
            self.metrics
                .shutdown_duration
                .with_label_values(&["flush", "success"])
                .observe(f_start.elapsed().as_secs_f64());
        }

        info!(
            final_sleep = %mimir_config::format_go_duration(
                self.cfg.final_sleep.as_nanos().min(i64::MAX as u128) as i64
            ),
            "lifecycler entering final sleep before shutdown"
        );
        if !self.cfg.final_sleep.is_zero() {
            tokio::time::sleep(self.cfg.final_sleep).await;
        }
    }

    /// Wraps this [`Lifecycler`] as a [`ManagedBasicService`].
    pub fn into_service(self: Arc<Self>) -> Arc<ManagedBasicService> {
        let l_run = self.clone();
        let l_stop = self.clone();

        basic_service(
            None,
            Some(Box::new(move |ctx: ServiceContext| {
                let l = l_run.clone();
                Box::pin(async move {
                    tokio::select! {
                        biased;
                        _ = ctx.canceled() => {
                            return Err(ServiceError::failure(format!(
                                "failed to join the ring {}: context canceled",
                                l.ring_name
                            )));
                        }
                        res = l.init_ring() => {
                            res.map_err(|e| ServiceError::failure(format!("failed to join the ring {}: {e}", l.ring_name)))?;
                        }
                    }
                    l.is_running.store(true, Ordering::Release);

                    let join_deadline = tokio::time::Instant::now() + l.cfg.join_after;
                    let mut join_fired = false;
                    let mut observe_deadline: Option<tokio::time::Instant> = None;
                    let hb_period = l.cfg.heartbeat_period;
                    let mut hb_interval = tokio::time::interval(if hb_period.is_zero() {
                        Duration::from_secs(3600)
                    } else {
                        hb_period
                    });
                    hb_interval.tick().await; // consume immediate first tick

                    loop {
                        tokio::select! {
                            biased;
                            _ = ctx.canceled() => {
                                info!(ring = %l.ring_name, "lifecycler loop() exited gracefully");
                                l.is_running.store(false, Ordering::Release);
                                break;
                            }
                            _ = tokio::time::sleep_until(join_deadline), if !join_fired => {
                                join_fired = true;
                                debug!(ring = %l.ring_name, "JoinAfter expired");
                                if l.get_state() == InstanceState::Pending {
                                    info!(ring = %l.ring_name, "auto-joining cluster after timeout");
                                    let target = if !l.cfg.observe_period.is_zero() {
                                        InstanceState::Joining
                                    } else {
                                        InstanceState::Active
                                    };
                                    match l.auto_join(&ctx, target).await {
                                        Ok(()) => {
                                            if target == InstanceState::Joining {
                                                info!(ring = %l.ring_name, "observing tokens before going ACTIVE");
                                                observe_deadline = Some(tokio::time::Instant::now() + l.cfg.observe_period);
                                            }
                                        }
                                        Err(LifecyclerError::NotRunning) if ctx.is_canceled() => {
                                            info!(ring = %l.ring_name, "lifecycler loop() exited gracefully");
                                            l.is_running.store(false, Ordering::Release);
                                            break;
                                        }
                                        Err(e) => {
                                            l.is_running.store(false, Ordering::Release);
                                            return Err(ServiceError::failure(format!("failed to pick tokens: {e}")));
                                        }
                                    }
                                }
                            }
                            _ = async {
                                if let Some(d) = observe_deadline {
                                    tokio::time::sleep_until(d).await;
                                } else {
                                    std::future::pending::<()>().await;
                                }
                            }, if observe_deadline.is_some() => {
                                observe_deadline = None;
                                let s = l.get_state();
                                if s != InstanceState::Joining {
                                    error!(state = s.as_str_name(), ring = %l.ring_name, "unexpected state while observing tokens");
                                }
                                let verified = tokio::select! {
                                    biased;
                                    _ = ctx.canceled() => {
                                        info!(ring = %l.ring_name, "lifecycler loop() exited gracefully");
                                        l.is_running.store(false, Ordering::Release);
                                        break;
                                    }
                                    v = l.verify_tokens() => v,
                                };
                                if verified {
                                    info!(ring = %l.ring_name, "token verification successful");
                                    let res = tokio::select! {
                                        biased;
                                        _ = ctx.canceled() => {
                                            info!(ring = %l.ring_name, "lifecycler loop() exited gracefully");
                                            l.is_running.store(false, Ordering::Release);
                                            break;
                                        }
                                        r = l.change_state_inner(InstanceState::Active) => r,
                                    };
                                    if let Err(err) = res {
                                        error!(ring = %l.ring_name, %err, "failed to set state to ACTIVE");
                                    }
                                } else {
                                    info!(ring = %l.ring_name, "token verification failed, observing");
                                    observe_deadline = Some(tokio::time::Instant::now() + l.cfg.observe_period);
                                }
                            }
                            _ = hb_interval.tick(), if !hb_period.is_zero() => {
                                l.metrics.consul_heartbeats.inc();
                                let res = tokio::select! {
                                    biased;
                                    _ = ctx.canceled() => {
                                        info!(ring = %l.ring_name, "lifecycler loop() exited gracefully");
                                        l.is_running.store(false, Ordering::Release);
                                        break;
                                    }
                                    r = l.update_consul() => r,
                                };
                                if let Err(err) = res {
                                    error!(ring = %l.ring_name, %err, "failed to write to the KV store, sleeping");
                                }
                            }
                        }
                    }
                    Ok(())
                })
            })),
            Some(Box::new(move |running_err| {
                let l = l_stop.clone();
                Box::pin(async move {
                    if let Some(err) = running_err {
                        return Err(err);
                    }
                    l.is_running.store(false, Ordering::Release);
                    if let Err(err) = l.change_state_inner(InstanceState::Leaving).await {
                        error!(ring = %l.ring_name, %err, "failed to set state to LEAVING");
                    }

                    // Run process_shutdown on an independent task (like Go's goroutine) so
                    // slow/stalled KV heartbeats never starve Flush() / block shipping.
                    let l_bg = l.clone();
                    let mut shutdown_task = tokio::spawn(async move {
                        l_bg.process_shutdown().await;
                    });
                    let hb_period = l.cfg.heartbeat_period;

                    if hb_period.is_zero() {
                        if let Err(err) = shutdown_task.await {
                            error!(ring = %l.ring_name, %err, "process_shutdown task panicked");
                        }
                    } else {
                        loop {
                            tokio::select! {
                                res = &mut shutdown_task => {
                                    if let Err(err) = res {
                                        error!(ring = %l.ring_name, %err, "process_shutdown task panicked");
                                    }
                                    break;
                                }
                                _ = tokio::time::sleep(hb_period) => {
                                    l.metrics.consul_heartbeats.inc();
                                    if let Err(err) = l.update_consul().await {
                                        error!(ring = %l.ring_name, %err, "failed to write to the KV store, sleeping");
                                    }
                                }
                            }
                        }
                    }

                    if l.should_unregister_on_shutdown() {
                        l.unregister()
                            .await
                            .map_err(|e| ServiceError::failure(format!("unregister failed: {e}")))?;
                        info!(ring = %l.ring_name, "instance removed from the KV store");
                    }

                    if !l.cfg.tokens_file_path.is_empty() && l.clear_tokens_on_shutdown() {
                        std::fs::remove_file(&l.cfg.tokens_file_path).map_err(|e| {
                            ServiceError::failure(format!(
                                "failed to delete tokens file {}: {e}",
                                l.cfg.tokens_file_path
                            ))
                        })?;
                        info!(path = %l.cfg.tokens_file_path, "removed tokens file from disk");
                    }
                    Ok(())
                })
            })),
        )
        .with_name(format!("{} ring lifecycler", self.ring_name))
    }
}

/// Unused helper placeholder kept private to satisfy channel signature if needed later.
#[allow(dead_code)]
async fn _unused_oneshot_helper(_tx: oneshot::Sender<()>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::inmemory::InMemoryKv;

    fn lifecycler_addr(cfg: &RingConfig, listen_port: u16) -> Result<String, LifecyclerError> {
        let lc_cfg = LifecyclerConfig::from_ring_config(cfg, listen_port)?;
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        Ok(Lifecycler::with_store(lc_cfg, kv, None, "ingester", "ring", false, None)
            .addr
            .clone())
    }

    /// dskit `NewLifecycler` / mimir `ToTokenlessBasicLifecyclerConfig` register
    /// `net.JoinHostPort(addr, port)`: IPv6 hosts are bracketed and always get the port.
    #[test]
    fn instance_addr_uses_join_host_port() {
        for (addr, port, listen, want) in [
            ("1.2.3.4", 0, 9095, "1.2.3.4:9095"),
            ("1.2.3.4", 1111, 9095, "1.2.3.4:1111"),
            ("fc99::9a", 0, 9095, "[fc99::9a]:9095"),
            ("2001:db8::1", 1111, 9095, "[2001:db8::1]:1111"),
            ("ingester-0.ingester.svc", 0, 9095, "ingester-0.ingester.svc:9095"),
        ] {
            let cfg = RingConfig {
                instance_addr: addr.to_string(),
                instance_port: port,
                ..Default::default()
            };
            assert_eq!(lifecycler_addr(&cfg, listen).unwrap(), want, "classic {addr}");
            let basic = BasicLifecyclerConfig::from_ring_config_tokenless(&cfg, listen).unwrap();
            assert_eq!(basic.addr, want, "tokenless {addr}");
        }
    }

    /// No instance_addr and no usable interface: Go fails `NewLifecycler` /
    /// `ToTokenlessBasicLifecyclerConfig` with the netutil error; never registers 127.0.0.1.
    #[test]
    fn missing_interface_is_a_startup_error() {
        let cfg = RingConfig {
            instance_addr: String::new(),
            instance_interface_names: vec!["mimir-no-such-if0".to_string()],
            ..Default::default()
        };
        let want = "no useable address found for interfaces [mimir-no-such-if0]";
        let err = LifecyclerConfig::from_ring_config(&cfg, 9095).err().expect("must fail");
        assert_eq!(err.to_string(), want);
        let err = BasicLifecyclerConfig::from_ring_config_tokenless(&cfg, 9095)
            .err()
            .expect("must fail");
        assert_eq!(err.to_string(), want);
    }

    /// Without instance_addr the address is auto-detected from the interfaces (never loopback).
    #[cfg(target_os = "linux")]
    #[test]
    fn auto_detected_addr_is_never_loopback() {
        let cfg = RingConfig {
            instance_addr: String::new(),
            instance_interface_names: vec!["lo".to_string()],
            ..Default::default()
        };
        // `lo` only has loopback addresses, which filterBestIP never selects.
        let err = LifecyclerConfig::from_ring_config(&cfg, 9095).err().expect("must fail");
        assert_eq!(err.to_string(), "no useable address found for interfaces [lo]");

        let cfg = RingConfig { instance_addr: String::new(), ..Default::default() };
        if let Ok(expected) = mimir_common::netutil::get_first_address_of(
            &cfg.instance_interface_names,
            false,
        ) {
            assert_eq!(lifecycler_addr(&cfg, 9095).unwrap(), format!("{expected}:9095"));
            assert!(!expected.starts_with("127."));
        }
    }

    #[test]
    fn lifecycler_config_validate_rejects_zero_or_negative_heartbeats() {
        let valid_ring = RingConfig {
            instance_addr: "127.0.0.1".to_string(),
            ..Default::default()
        };
        let valid = LifecyclerConfig::from_ring_config(&valid_ring, 9095).unwrap();
        assert!(valid.validate().is_ok());

        let zero_period = RingConfig {
            instance_addr: "127.0.0.1".to_string(),
            heartbeat_period: mimir_config::GoDuration::ZERO,
            ..Default::default()
        };
        let err = LifecyclerConfig::from_ring_config(&zero_period, 9095)
            .unwrap()
            .validate()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: heartbeat period must be greater than 0"
        );

        let neg_period = RingConfig {
            instance_addr: "127.0.0.1".to_string(),
            heartbeat_period: mimir_config::GoDuration(-1),
            ..Default::default()
        };
        assert!(LifecyclerConfig::from_ring_config(&neg_period, 9095)
            .unwrap()
            .validate()
            .is_err());

        let zero_timeout = RingConfig {
            instance_addr: "127.0.0.1".to_string(),
            heartbeat_timeout: mimir_config::GoDuration::ZERO,
            ..Default::default()
        };
        let err = LifecyclerConfig::from_ring_config(&zero_timeout, 9095)
            .unwrap()
            .validate()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: heartbeat timeout must be greater than 0"
        );

        // Lifecycler::new also rejects zero heartbeat_period / heartbeat_timeout.
        let mut zero_period_inmem = zero_period;
        zero_period_inmem.kvstore.store = "inmemory".to_string();
        let err = Lifecycler::new::<prometheus::Registry>(
            &zero_period_inmem,
            9095,
            None,
            "ingester",
            "ring",
            false,
            None,
            None,
        )
        .err()
        .expect("Lifecycler::new must reject heartbeat_period=0");
        assert!(
            err.to_string()
                .contains("heartbeat period must be greater than 0"),
            "unexpected err: {err}"
        );
    }

    #[tokio::test]
    async fn test_lifecycler_not_running_errors_and_joining_recovery_timestamp() {
        use crate::kv::memberlist::{MemberlistKv, MemberlistKvClient};
        use mimir_config::{GoDuration, MemberlistConfig};

        let cfg = RingConfig {
            instance_id: "rec-1".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            instance_port: 9095,
            num_tokens: 2,
            heartbeat_period: GoDuration::from_millis(50),
            ..Default::default()
        };

        let ml_cfg = MemberlistConfig {
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            node_name: "lc-rec-node".to_string(),
            ..Default::default()
        };
        let ml = MemberlistKv::start(ml_cfg, vec![ring_desc_codec()], "test", None)
            .await
            .unwrap();
        let kv: Arc<dyn KvClient> =
            Arc::new(MemberlistKvClient::new(Arc::clone(&ml), ring_desc_codec()).unwrap());

        let now_s = system_time_to_unix_secs(SystemTime::now());
        kv.cas(
            "ring",
            Box::new(move |_| {
                let mut d = Desc::default();
                d.ingesters.insert(
                    "rec-1".to_string(),
                    InstanceDesc {
                        id: "rec-1".to_string(),
                        addr: "127.0.0.1:9095".to_string(),
                        timestamp: now_s,
                        state: InstanceState::Joining as i32,
                        tokens: vec![10, 20],
                        ..Default::default()
                    },
                );
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();

        let lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        let lc = Lifecycler::with_store(
            lc_cfg,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            false,
            None,
        );

        assert!(matches!(
            lc.change_state(InstanceState::Active).await,
            Err(LifecyclerError::NotRunning)
        ));
        assert!(matches!(
            lc.change_read_only_state(true).await,
            Err(LifecyclerError::NotRunning)
        ));
        assert!(matches!(
            lc.claim_tokens_for("other").await,
            Err(LifecyclerError::NotRunning)
        ));

        // JOINING -> PENDING recovery must bump timestamp so MemberlistKv CAS succeeds immediately.
        lc.init_ring().await.unwrap();
        let got = kv
            .get("ring")
            .await
            .unwrap()
            .unwrap()
            .downcast::<Desc>()
            .unwrap();
        let inst = &got.ingesters["rec-1"];
        assert_eq!(inst.instance_state(), InstanceState::Pending);
        assert!(inst.timestamp > now_s || now_s != system_time_to_unix_secs(SystemTime::now()));

        ml.shutdown().await;
    }

    #[tokio::test]
    async fn test_clear_tokens_on_shutdown_propagates_remove_error() {
        use mimir_common::services::Service;
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("unremovable-dir");
        std::fs::create_dir_all(missing.join("sub")).unwrap();
        let ring_cfg = RingConfig {
            instance_id: "clr-1".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            tokens_file_path: missing.to_string_lossy().into_owned(),
            final_sleep: mimir_config::GoDuration::ZERO,
            heartbeat_period: mimir_config::GoDuration::from_millis(50),
            ..Default::default()
        };

        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
        let lc = Lifecycler::with_store(
            lc_cfg,
            kv,
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            false,
            None,
        );
        lc.set_clear_tokens_on_shutdown(true);
        let svc = lc.into_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();
        svc.stop_async();
        let err = svc.await_terminated().await.unwrap_err();
        assert!(
            err.to_string().contains("failed to delete tokens file"),
            "unexpected err: {err}"
        );
    }

    #[tokio::test]
    async fn test_basic_lifecycler_stopping_slow_cas_does_not_deadlock_and_post_unregister_change_state_rejected() {
        use mimir_common::services::Service;
        use std::sync::atomic::AtomicUsize;

        struct SlowCasKv {
            inner: InMemoryKv,
            cas_calls: AtomicUsize,
            cas_delay: Duration,
            cas_entered: Arc<tokio::sync::Notify>,
            cas_release: Arc<tokio::sync::Notify>,
        }

        impl KvClient for SlowCasKv {
            fn list<'a>(
                &'a self,
                prefix: &'a str,
            ) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                self.inner.list(prefix)
            }
            fn get<'a>(
                &'a self,
                key: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                self.inner.get(key)
            }
            fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                self.inner.delete(key)
            }
            fn cas<'a>(
                &'a self,
                key: &'a str,
                f: crate::kv::CasFn,
            ) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    let n = self.cas_calls.fetch_add(1, Ordering::SeqCst);
                    if n == 1 {
                        // First CAS after register (LeaveOnStoppingDelegate -> Leaving):
                        // hold op_lock longer than heartbeat_period until test releases it.
                        self.cas_entered.notify_waiters();
                        self.cas_release.notified().await;
                        tokio::time::sleep(self.cas_delay).await;
                    }
                    self.inner.cas(key, f).await
                })
            }
            fn watch_key<'a>(
                &'a self,
                key: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_key(key, ctx, f)
            }
            fn watch_prefix<'a>(
                &'a self,
                prefix: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_prefix(prefix, ctx, f)
            }
        }

        struct NoopBasicDelegate;
        impl BasicLifecyclerDelegate for NoopBasicDelegate {
            fn on_ring_instance_register(
                &self,
                _lc: &BasicLifecycler,
                _ring: &Desc,
                _exists: bool,
                _id: &str,
                _desc: Option<&InstanceDesc>,
            ) -> (InstanceState, Vec<u32>) {
                (InstanceState::Active, Vec::new())
            }
            fn on_ring_instance_tokens(&self, _lc: &BasicLifecycler, _tokens: &[u32]) {}
            fn on_ring_instance_stopping<'a>(
                &'a self,
                _lc: &'a BasicLifecycler,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }
            fn on_ring_instance_heartbeat(
                &self,
                _lc: &BasicLifecycler,
                _ring: &mut Desc,
                _inst: &mut InstanceDesc,
            ) {
            }
        }

        let cas_entered = Arc::new(tokio::sync::Notify::new());
        let cas_release = Arc::new(tokio::sync::Notify::new());
        let kv = Arc::new(SlowCasKv {
            inner: InMemoryKv::new(ring_desc_codec()),
            cas_calls: AtomicUsize::new(0),
            cas_delay: Duration::from_millis(5),
            cas_entered: cas_entered.clone(),
            cas_release: cas_release.clone(),
        });
        let cfg = BasicLifecyclerConfig {
            id: "basic-1".to_string(),
            addr: "127.0.0.1:9095".to_string(),
            zone: String::new(),
            heartbeat_period: Duration::from_millis(15),
            heartbeat_timeout: Duration::from_secs(5),
            tokens_observe_period: Duration::ZERO,
            num_tokens: 0,
            keep_instance_in_the_ring_on_shutdown: false,
            token_generator: None,
            versions: HashMap::new(),
        };
        let delegate: Arc<dyn BasicLifecyclerDelegate> =
            Arc::new(LeaveOnStoppingDelegate::new(Arc::new(NoopBasicDelegate)));
        let lc = BasicLifecycler::new(cfg, "ingester", "ring", kv.clone(), delegate, None);
        let svc = lc.clone().into_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();

        // Trigger stop_async; LeaveOnStoppingDelegate enters slow CAS and holds op_lock across >1 heartbeat tick.
        svc.stop_async();
        cas_entered.notified().await;
        // Let heartbeat_period elapse so heartbeat() blocks on op_lock in the stopping select! loop.
        tokio::time::sleep(Duration::from_millis(40)).await;
        // Concurrently queue change_state while op_lock is held during shutdown.
        let lc_race = lc.clone();
        let race_fut = tokio::spawn(async move { lc_race.change_state(InstanceState::Active).await });
        tokio::task::yield_now().await;
        cas_release.notify_waiters();

        tokio::time::timeout(Duration::from_secs(2), svc.await_terminated())
            .await
            .expect("stopping must not deadlock on op_lock")
            .unwrap();
        let race_res = race_fut.await.unwrap();
        assert!(matches!(race_res, Err(LifecyclerError::NotRunning)), "got {race_res:?}");

        // Ring must remain unregistered (no zombie re-insertion).
        let desc = kv
            .get("ring")
            .await
            .unwrap()
            .unwrap()
            .downcast::<Desc>()
            .unwrap();
        assert!(!desc.ingesters.contains_key("basic-1"));
    }

    #[tokio::test]
    async fn test_lifecycler_post_unregister_change_state_never_resurrects_zombie() {
        use mimir_common::services::Service;

        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let ring_cfg = RingConfig {
            instance_id: "zombie-1".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            final_sleep: mimir_config::GoDuration::ZERO,
            heartbeat_period: mimir_config::GoDuration::from_millis(50),
            unregister_on_shutdown: true,
            ..Default::default()
        };
        let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
        let lc = Lifecycler::with_store(
            lc_cfg,
            kv.clone(),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            false,
            None,
        );
        let svc = lc.clone().into_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();

        svc.stop_async();
        svc.await_terminated().await.unwrap();

        assert!(matches!(
            lc.change_state(InstanceState::Active).await,
            Err(LifecyclerError::NotRunning)
        ));
        assert!(matches!(
            lc.change_read_only_state(true).await,
            Err(LifecyclerError::NotRunning)
        ));
        let desc = kv
            .get("ring")
            .await
            .unwrap()
            .unwrap()
            .downcast::<Desc>()
            .unwrap();
        assert!(!desc.ingesters.contains_key("zombie-1"));
    }

    #[tokio::test]
    async fn test_basic_lifecycler_passes_real_self_to_delegate() {
        use mimir_common::services::Service;
        use std::sync::atomic::AtomicBool;

        struct CheckDelegate {
            reg_saw_keep: AtomicBool,
            hb_saw_keep: AtomicBool,
        }
        impl BasicLifecyclerDelegate for CheckDelegate {
            fn on_ring_instance_register(
                &self,
                lifecycler: &BasicLifecycler,
                _ring_desc: &Desc,
                _instance_exists: bool,
                _instance_id: &str,
                _instance_desc: Option<&InstanceDesc>,
            ) -> (InstanceState, Vec<u32>) {
                self.reg_saw_keep.store(
                    lifecycler.should_keep_instance_in_the_ring_on_shutdown(),
                    Ordering::Release,
                );
                (InstanceState::Active, vec![10, 20])
            }
            fn on_ring_instance_tokens(&self, _lifecycler: &BasicLifecycler, _tokens: &[u32]) {}
            fn on_ring_instance_stopping<'a>(
                &'a self,
                _lifecycler: &'a BasicLifecycler,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }
            fn on_ring_instance_heartbeat(
                &self,
                lifecycler: &BasicLifecycler,
                _ring_desc: &mut Desc,
                _instance_desc: &mut InstanceDesc,
            ) {
                self.hb_saw_keep.store(
                    lifecycler.should_keep_instance_in_the_ring_on_shutdown(),
                    Ordering::Release,
                );
            }
        }

        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let del = Arc::new(CheckDelegate {
            reg_saw_keep: AtomicBool::new(false),
            hb_saw_keep: AtomicBool::new(false),
        });
        let cfg = BasicLifecyclerConfig {
            id: "self-check".to_string(),
            addr: "127.0.0.1:9095".to_string(),
            zone: "zone-a".to_string(),
            heartbeat_period: Duration::from_millis(20),
            heartbeat_timeout: Duration::from_secs(1),
            tokens_observe_period: Duration::ZERO,
            num_tokens: 2,
            keep_instance_in_the_ring_on_shutdown: true,
            token_generator: None,
            versions: HashMap::new(),
        };
        let lc = BasicLifecycler::new(cfg, "ingester", "ring", kv, del.clone(), None);
        let svc = lc.into_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;
        svc.stop_async();
        svc.await_terminated().await.unwrap();

        assert!(del.reg_saw_keep.load(Ordering::Acquire));
        assert!(del.hb_saw_keep.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn w21_aps_07_empty_ring_unregister_and_claim_use_kv_other() {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let cfg = RingConfig {
            instance_id: "ing-empty".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            instance_port: 9095,
            instance_availability_zone: "zone-a".to_string(),
            num_tokens: 0,
            ..Default::default()
        };
        let lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        let lc = Lifecycler::with_store(
            lc_cfg,
            kv,
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            false,
            None,
        );

        let err = lc.unregister().await.unwrap_err().to_string();
        assert!(
            err.contains("found empty ring when trying to unregister") && !err.contains("key not found"),
            "unexpected err: {err}"
        );
    }

    #[tokio::test]
    async fn w23_csc_02_verify_tokens_does_not_persist_uncommitted_tokens_on_cas_failure_or_retry() {
        use std::sync::atomic::AtomicUsize;

        // (a) CAS failure after token conflict must leave local tokens & tokens_file_path untouched.
        struct FailAfterReadKv {
            inner: InMemoryKv,
            fail_cas: AtomicBool,
        }
        impl KvClient for FailAfterReadKv {
            fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                self.inner.list(prefix)
            }
            fn get<'a>(
                &'a self,
                key: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                self.inner.get(key)
            }
            fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                self.inner.delete(key)
            }
            fn cas<'a>(&'a self, key: &'a str, mut f: crate::kv::CasFn) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    if self.fail_cas.load(Ordering::Acquire) {
                        let cur = self.inner.get(key).await?;
                        let _ = f(cur.as_deref());
                        return Err(KvError::CasFailed { key: key.to_string(), reason: "injected".to_string() });
                    }
                    self.inner.cas(key, f).await
                })
            }
            fn watch_key<'a>(
                &'a self,
                key: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_key(key, ctx, f)
            }
            fn watch_prefix<'a>(
                &'a self,
                prefix: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_prefix(prefix, ctx, f)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let tokens_path = dir.path().join("tokens.json");
        let initial_tokens = vec![100, 200, 300];
        store_tokens_to_file(&initial_tokens, &tokens_path).unwrap();

        let kv = Arc::new(FailAfterReadKv {
            inner: InMemoryKv::new(ring_desc_codec()),
            fail_cas: AtomicBool::new(false),
        });
        kv.cas(
            "ring",
            Box::new(|_| {
                let mut desc = Desc::default();
                desc.add_ingester(
                    "ing-1",
                    "127.0.0.1:9095",
                    "zone-a",
                    vec![100],
                    InstanceState::Joining,
                    Some(SystemTime::now()),
                    false,
                    None,
                    HashMap::new(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

        let mut cfg = RingConfig {
            instance_id: "ing-1".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            instance_port: 9095,
            instance_availability_zone: "zone-a".to_string(),
            num_tokens: 3,
            tokens_file_path: tokens_path.to_string_lossy().into_owned(),
            ..Default::default()
        };
        cfg.kvstore.store = "inmemory".to_string();
        let lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        let lc = Lifecycler::with_store(
            lc_cfg,
            kv.clone(),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            false,
            None,
        );
        lc.set_tokens(initial_tokens.clone());
        kv.fail_cas.store(true, Ordering::Release);

        assert!(!lc.verify_tokens().await);
        assert_eq!(lc.get_tokens(), initial_tokens);
        assert_eq!(load_tokens_from_file(&tokens_path).unwrap(), initial_tokens);

        // (b) Attempt 1 sees conflict + CAS version mismatch, attempt 2 sees clean match ->
        // verify_tokens returns true and attempt 1 tokens are NOT written!
        struct RetryOnceConflictKv {
            inner: InMemoryKv,
            attempts: AtomicUsize,
            clean_tokens: Vec<u32>,
        }
        impl KvClient for RetryOnceConflictKv {
            fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                self.inner.list(prefix)
            }
            fn get<'a>(
                &'a self,
                key: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                self.inner.get(key)
            }
            fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                self.inner.delete(key)
            }
            fn cas<'a>(&'a self, key: &'a str, mut f: crate::kv::CasFn) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    let n = self.attempts.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        let mut stale = Desc::default();
                        stale.add_ingester(
                            "ing-1",
                            "127.0.0.1:9095",
                            "zone-a",
                            vec![100],
                            InstanceState::Joining,
                            Some(SystemTime::now()),
                            false,
                            None,
                            HashMap::new(),
                        );
                        let boxed: Box<dyn std::any::Any + Send + Sync> = Box::new(stale);
                        let _ = f(Some(boxed.as_ref())).map_err(|e| e.err)?;

                        let mut clean = Desc::default();
                        clean.add_ingester(
                            "ing-1",
                            "127.0.0.1:9095",
                            "zone-a",
                            self.clean_tokens.clone(),
                            InstanceState::Joining,
                            Some(SystemTime::now()),
                            false,
                            None,
                            HashMap::new(),
                        );
                        let boxed2: Box<dyn std::any::Any + Send + Sync> = Box::new(clean);
                        let _ = f(Some(boxed2.as_ref())).map_err(|e| e.err)?;
                        return Ok(());
                    }
                    self.inner.cas(key, f).await
                })
            }
            fn watch_key<'a>(
                &'a self,
                key: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_key(key, ctx, f)
            }
            fn watch_prefix<'a>(
                &'a self,
                prefix: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_prefix(prefix, ctx, f)
            }
        }

        let kv2 = Arc::new(RetryOnceConflictKv {
            inner: InMemoryKv::new(ring_desc_codec()),
            attempts: AtomicUsize::new(0),
            clean_tokens: initial_tokens.clone(),
        });
        let lc2_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        let lc2 = Lifecycler::with_store(
            lc2_cfg,
            kv2,
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            false,
            None,
        );
        lc2.set_tokens(initial_tokens.clone());

        assert!(lc2.verify_tokens().await);
        assert_eq!(lc2.get_tokens(), initial_tokens);
        assert_eq!(load_tokens_from_file(&tokens_path).unwrap(), initial_tokens);

        // (c) BasicLifecycler::verify_tokens + update_instance_locked: attempt 1 conflict + version mismatch,
        // attempt 2 clean match (CasOutcome::NoChange) -> returns true & updates curr_instance_desc to attempt 2 snapshot!
        struct BasicRetryOnceKv {
            inner: InMemoryKv,
            attempts: AtomicUsize,
            clean_tokens: Vec<u32>,
        }
        impl KvClient for BasicRetryOnceKv {
            fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                self.inner.list(prefix)
            }
            fn get<'a>(
                &'a self,
                key: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                self.inner.get(key)
            }
            fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                self.inner.delete(key)
            }
            fn cas<'a>(&'a self, key: &'a str, mut f: crate::kv::CasFn) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    let n = self.attempts.fetch_add(1, Ordering::SeqCst);
                    if n == 1 {
                        let mut stale = Desc::default();
                        stale.add_ingester(
                            "basic-retry",
                            "127.0.0.1:9095",
                            "zone-a",
                            vec![100],
                            InstanceState::Joining,
                            Some(SystemTime::now()),
                            false,
                            None,
                            HashMap::new(),
                        );
                        let b1: Box<dyn std::any::Any + Send + Sync> = Box::new(stale);
                        let _ = f(Some(b1.as_ref())).map_err(|e| e.err)?;

                        let mut clean = Desc::default();
                        clean.add_ingester(
                            "basic-retry",
                            "127.0.0.1:9095",
                            "zone-a",
                            self.clean_tokens.clone(),
                            InstanceState::Joining,
                            Some(SystemTime::now()),
                            false,
                            None,
                            HashMap::new(),
                        );
                        let b2: Box<dyn std::any::Any + Send + Sync> = Box::new(clean);
                        let _ = f(Some(b2.as_ref())).map_err(|e| e.err)?;
                        return Ok(());
                    }
                    self.inner.cas(key, f).await
                })
            }
            fn watch_key<'a>(
                &'a self,
                key: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_key(key, ctx, f)
            }
            fn watch_prefix<'a>(
                &'a self,
                prefix: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_prefix(prefix, ctx, f)
            }
        }
        struct StaticTokensDelegate(Vec<u32>);
        impl BasicLifecyclerDelegate for StaticTokensDelegate {
            fn on_ring_instance_register(
                &self,
                _lc: &BasicLifecycler,
                _ring: &Desc,
                _exists: bool,
                _id: &str,
                _desc: Option<&InstanceDesc>,
            ) -> (InstanceState, Vec<u32>) {
                (InstanceState::Joining, self.0.clone())
            }
            fn on_ring_instance_tokens(&self, _lc: &BasicLifecycler, _tokens: &[u32]) {}
            fn on_ring_instance_stopping<'a>(&'a self, _lc: &'a BasicLifecycler) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }
            fn on_ring_instance_heartbeat(
                &self,
                _lc: &BasicLifecycler,
                _ring: &mut Desc,
                _inst: &mut InstanceDesc,
            ) {
            }
        }
        let bkv = Arc::new(BasicRetryOnceKv {
            inner: InMemoryKv::new(ring_desc_codec()),
            attempts: AtomicUsize::new(0),
            clean_tokens: initial_tokens.clone(),
        });
        let bcfg = BasicLifecyclerConfig {
            id: "basic-retry".to_string(),
            addr: "127.0.0.1:9095".to_string(),
            zone: "zone-a".to_string(),
            heartbeat_period: Duration::from_secs(60),
            heartbeat_timeout: Duration::from_secs(120),
            tokens_observe_period: Duration::ZERO,
            num_tokens: 3,
            keep_instance_in_the_ring_on_shutdown: false,
            token_generator: None,
            versions: HashMap::new(),
        };
        let blc = BasicLifecycler::new(
            bcfg,
            "ingester",
            "ring",
            bkv.clone(),
            Arc::new(StaticTokensDelegate(initial_tokens.clone())),
            None,
        );
        blc.register_instance().await.unwrap();
        assert!(blc.verify_tokens().await);
        assert_eq!(blc.get_tokens(), initial_tokens);
    }

    #[tokio::test]
    async fn w23_csc_03_stopping_spawns_process_shutdown_and_auto_join_failure_resets_is_running() {
        use mimir_common::services::Service;
        use std::sync::atomic::AtomicUsize;

        // (a) Classic Lifecycler stopping must run Flush() concurrently even while KV CAS hangs.
        struct HangOnStoppingHeartbeatKv {
            inner: InMemoryKv,
            stopping_started: AtomicBool,
            leaving_committed: AtomicBool,
            hb_hung_once: AtomicBool,
            hb_entered: Arc<tokio::sync::Notify>,
            hb_release: Arc<tokio::sync::Notify>,
        }
        impl KvClient for HangOnStoppingHeartbeatKv {
            fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                self.inner.list(prefix)
            }
            fn get<'a>(
                &'a self,
                key: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                self.inner.get(key)
            }
            fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                self.inner.delete(key)
            }
            fn cas<'a>(&'a self, key: &'a str, f: crate::kv::CasFn) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    if self.stopping_started.load(Ordering::Acquire) {
                        if !self.leaving_committed.swap(true, Ordering::AcqRel) {
                            return self.inner.cas(key, f).await;
                        }
                        if !self.hb_hung_once.swap(true, Ordering::AcqRel) {
                            self.hb_entered.notify_waiters();
                            self.hb_release.notified().await;
                        }
                    }
                    self.inner.cas(key, f).await
                })
            }
            fn watch_key<'a>(
                &'a self,
                key: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_key(key, ctx, f)
            }
            fn watch_prefix<'a>(
                &'a self,
                prefix: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_prefix(prefix, ctx, f)
            }
        }

        struct SlowFlushTransferer {
            hb_entered: Arc<tokio::sync::Notify>,
            flush_done: Arc<tokio::sync::Notify>,
        }
        impl FlushTransferer for SlowFlushTransferer {
            fn flush(&self) -> BoxFuture<'_, ()> {
                Box::pin(async move {
                    self.hb_entered.notified().await;
                    self.flush_done.notify_waiters();
                })
            }
            fn transfer_out(&self) -> BoxFuture<'_, Result<(), LifecyclerError>> {
                Box::pin(async { Err(LifecyclerError::TransferDisabled) })
            }
        }

        let hb_entered = Arc::new(tokio::sync::Notify::new());
        let hb_release = Arc::new(tokio::sync::Notify::new());
        let flush_done = Arc::new(tokio::sync::Notify::new());
        let kv = Arc::new(HangOnStoppingHeartbeatKv {
            inner: InMemoryKv::new(ring_desc_codec()),
            stopping_started: AtomicBool::new(false),
            leaving_committed: AtomicBool::new(false),
            hb_hung_once: AtomicBool::new(false),
            hb_entered: hb_entered.clone(),
            hb_release: hb_release.clone(),
        });
        let cfg = RingConfig {
            instance_id: "ing-stop".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            instance_port: 9095,
            instance_availability_zone: "zone-a".to_string(),
            num_tokens: 2,
            heartbeat_period: mimir_config::GoDuration::from_millis(15),
            ..Default::default()
        };
        let mut lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        lc_cfg.join_after = Duration::ZERO;
        lc_cfg.observe_period = Duration::ZERO;
        lc_cfg.min_ready_duration = Duration::ZERO;
        lc_cfg.final_sleep = Duration::ZERO;
        let lc = Lifecycler::with_store(
            lc_cfg,
            kv.clone(),
            Some(Arc::new(SlowFlushTransferer {
                hb_entered: hb_entered.clone(),
                flush_done: flush_done.clone(),
            })),
            "ingester",
            "ring",
            true,
            None,
        );
        let svc = lc.into_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;

        kv.stopping_started.store(true, Ordering::Release);
        svc.stop_async();
        tokio::time::timeout(Duration::from_secs(2), flush_done.notified())
            .await
            .expect("process_shutdown / Flush() must not be starved by hanging KV CAS");

        hb_release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), svc.await_terminated())
            .await
            .expect("service should terminate after KV CAS unblocks")
            .unwrap();

        // (b) auto_join failure resets is_running to false.
        struct FailSecondCasKv {
            inner: InMemoryKv,
            calls: AtomicUsize,
        }
        impl KvClient for FailSecondCasKv {
            fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                self.inner.list(prefix)
            }
            fn get<'a>(
                &'a self,
                key: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                self.inner.get(key)
            }
            fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                self.inner.delete(key)
            }
            fn cas<'a>(&'a self, key: &'a str, f: crate::kv::CasFn) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    let n = self.calls.fetch_add(1, Ordering::SeqCst);
                    if n == 1 {
                        return Err(KvError::Other("auto_join boom".to_string()));
                    }
                    self.inner.cas(key, f).await
                })
            }
            fn watch_key<'a>(
                &'a self,
                key: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_key(key, ctx, f)
            }
            fn watch_prefix<'a>(
                &'a self,
                prefix: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_prefix(prefix, ctx, f)
            }
        }
        let kv_fail = Arc::new(FailSecondCasKv {
            inner: InMemoryKv::new(ring_desc_codec()),
            calls: AtomicUsize::new(0),
        });
        let mut lc2_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        lc2_cfg.join_after = Duration::ZERO;
        lc2_cfg.observe_period = Duration::ZERO;
        lc2_cfg.min_ready_duration = Duration::ZERO;
        let lc2 = Lifecycler::with_store(
            lc2_cfg,
            kv_fail,
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            false,
            None,
        );
        let svc2 = lc2.clone().into_service();
        svc2.start_async(ServiceContext::background()).await.unwrap();
        let term_res = tokio::time::timeout(Duration::from_secs(2), svc2.await_terminated())
            .await
            .expect("service should fail on auto_join error");
        assert!(term_res.is_err());
        assert!(!lc2.is_running.load(Ordering::Acquire));
        assert!(matches!(
            lc2.change_state(InstanceState::Active).await,
            Err(LifecyclerError::NotRunning)
        ));
    }

    #[tokio::test]
    async fn c01_lifecycler_and_basic_lifecycler_cancel_promptly_on_hanging_kv() {
        use mimir_common::services::Service;
        use std::sync::atomic::AtomicUsize;

        struct TestNoopBasicDelegate;
        impl BasicLifecyclerDelegate for TestNoopBasicDelegate {
            fn on_ring_instance_register(
                &self,
                _lifecycler: &BasicLifecycler,
                _ring_desc: &Desc,
                _instance_exists: bool,
                _instance_id: &str,
                _instance_desc: Option<&InstanceDesc>,
            ) -> (InstanceState, Vec<u32>) {
                (InstanceState::Active, Vec::new())
            }
            fn on_ring_instance_tokens(&self, _lifecycler: &BasicLifecycler, _tokens: &[u32]) {}
            fn on_ring_instance_stopping<'a>(
                &'a self,
                _lifecycler: &'a BasicLifecycler,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }
            fn on_ring_instance_heartbeat(
                &self,
                _lifecycler: &BasicLifecycler,
                _ring_desc: &mut Desc,
                _instance_desc: &mut InstanceDesc,
            ) {
            }
        }

        struct HangingCasKv {
            hang_after_calls: usize,
            cas_calls: AtomicUsize,
            get_calls: AtomicUsize,
            hang_get: bool,
        }

        impl KvClient for HangingCasKv {
            fn list<'a>(&'a self, _p: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                Box::pin(async { Ok(Vec::new()) })
            }
            fn get<'a>(
                &'a self,
                _k: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                Box::pin(async move {
                    self.get_calls.fetch_add(1, Ordering::SeqCst);
                    if self.hang_get {
                        std::future::pending::<()>().await;
                    }
                    Ok(None)
                })
            }
            fn delete<'a>(&'a self, _k: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async { Ok(()) })
            }
            fn cas<'a>(
                &'a self,
                _k: &'a str,
                mut f: crate::kv::CasFn,
            ) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    let n = self.cas_calls.fetch_add(1, Ordering::SeqCst);
                    if n >= self.hang_after_calls {
                        std::future::pending::<()>().await;
                    }
                    let _ = f(None);
                    Ok(())
                })
            }
            fn watch_key<'a>(
                &'a self,
                _k: &'a str,
                ctx: ServiceContext,
                _f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async move {
                    ctx.canceled().await;
                })
            }
            fn watch_prefix<'a>(
                &'a self,
                _p: &'a str,
                ctx: ServiceContext,
                _f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async move {
                    ctx.canceled().await;
                })
            }
        }

        // 1. Lifecycler blocked in init_ring CAS cancels promptly on stop_async.
        let cfg = RingConfig {
            instance_id: "c01-lc-init".into(),
            instance_addr: "127.0.0.1".into(),
            num_tokens: 2,
            ..Default::default()
        };
        let mut lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        lc_cfg.heartbeat_period = Duration::from_secs(60);
        let lc1 = Lifecycler::with_store(
            lc_cfg,
            Arc::new(HangingCasKv {
                hang_after_calls: 0,
                cas_calls: AtomicUsize::new(0),
                get_calls: AtomicUsize::new(0),
                hang_get: false,
            }),
            None,
            "ingester",
            "ring",
            false,
            None,
        );
        let svc1 = lc1.clone().into_service();
        svc1.start_async(ServiceContext::background()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        svc1.stop_async();
        let res1 = tokio::time::timeout(Duration::from_millis(500), svc1.await_terminated())
            .await
            .expect("Lifecycler blocked in init_ring must cancel promptly");
        assert!(res1.is_err());

        // 2. Lifecycler blocked in update_consul heartbeat cancels promptly and enters stopping.
        let mut lc2_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        lc2_cfg.join_after = Duration::from_secs(60);
        lc2_cfg.heartbeat_period = Duration::from_millis(15);
        let kv2 = Arc::new(HangingCasKv {
            hang_after_calls: 1, // init_ring (call 0) succeeds, first heartbeat (call 1) hangs
            cas_calls: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
            hang_get: false,
        });
        let lc2 = Lifecycler::with_store(
            lc2_cfg,
            kv2,
            None,
            "ingester",
            "ring",
            false,
            None,
        );
        let svc2 = lc2.clone().into_service();
        svc2.start_async(ServiceContext::background()).await.unwrap();
        svc2.await_running().await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        svc2.stop_async();
        for _ in 0..30 {
            if !lc2.is_running.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !lc2.is_running.load(Ordering::Acquire),
            "Lifecycler running loop must exit promptly even while update_consul hangs"
        );

        // 3. BasicLifecycler blocked in register_instance cancels promptly on stop_async.
        let bcfg = BasicLifecyclerConfig {
            id: "c01-blc".into(),
            addr: "127.0.0.1:9095".into(),
            zone: String::new(),
            heartbeat_period: Duration::from_millis(50),
            heartbeat_timeout: Duration::from_secs(5),
            num_tokens: 2,
            tokens_observe_period: Duration::ZERO,
            keep_instance_in_the_ring_on_shutdown: false,
            token_generator: None,
            versions: HashMap::new(),
        };
        let blc = BasicLifecycler::new(
            bcfg,
            "ingester",
            "ring",
            Arc::new(HangingCasKv {
                hang_after_calls: 0,
                cas_calls: AtomicUsize::new(0),
                get_calls: AtomicUsize::new(0),
                hang_get: false,
            }),
            Arc::new(TestNoopBasicDelegate),
            None,
        );
        let bsvc = blc.into_service();
        bsvc.start_async(ServiceContext::background()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        bsvc.stop_async();
        let bres = tokio::time::timeout(Duration::from_millis(500), bsvc.await_terminated())
            .await
            .expect("BasicLifecycler blocked in register_instance must cancel promptly");
        assert!(bres.is_err());

        // 4. auto_join respects can_join_timeout when kv_store.get hangs.
        struct CanJoinTokenGen;
        impl crate::token_gen::TokenGenerator for CanJoinTokenGen {
            fn generate_tokens(&self, count: usize, _taken: &[u32]) -> Vec<u32> {
                (0..count as u32).collect()
            }
            fn can_join_enabled(&self) -> bool {
                true
            }
            fn can_join(
                &self,
                _ingesters: &HashMap<String, InstanceDesc>,
            ) -> Result<(), crate::token_gen::TokenGenError> {
                Err(crate::token_gen::TokenGenError::SpreadMinimizing("wait".to_string()))
            }
        }
        let mut lc3_cfg = LifecyclerConfig::from_ring_config(&cfg, 9095).unwrap();
        lc3_cfg.heartbeat_period = Duration::from_secs(60);
        lc3_cfg.token_generator = Some(Arc::new(CanJoinTokenGen));
        let lc3 = Lifecycler::with_store(
            lc3_cfg,
            Arc::new(HangingCasKv {
                hang_after_calls: 99,
                cas_calls: AtomicUsize::new(0),
                get_calls: AtomicUsize::new(0),
                hang_get: true,
            }),
            None,
            "ingester",
            "ring",
            false,
            None,
        );
        lc3.set_can_join_timeout(Duration::from_millis(60));
        lc3.is_running.store(true, Ordering::Release);
        let ctx3 = ServiceContext::background();
        let aj_res = tokio::time::timeout(
            Duration::from_millis(500),
            lc3.auto_join(&ctx3, InstanceState::Active),
        )
        .await
        .expect("auto_join must respect can_join_timeout even if kv_store.get hangs");
        assert!(aj_res.is_ok());
    }

    #[tokio::test]
    async fn c05_wait_stable_tokens_heartbeats_while_observe_period_shorter_than_heartbeat() {
        struct StealingKv {
            inner: InMemoryKv,
            stealing: AtomicBool,
            hb: Arc<std::sync::atomic::AtomicUsize>,
            next_tok: std::sync::atomic::AtomicU32,
        }
        impl KvClient for StealingKv {
            fn list<'a>(&'a self, p: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
                self.inner.list(p)
            }
            fn get<'a>(
                &'a self,
                k: &'a str,
            ) -> BoxFuture<'a, Result<Option<Box<dyn std::any::Any + Send + Sync>>, KvError>> {
                self.inner.get(k)
            }
            fn delete<'a>(&'a self, k: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
                self.inner.delete(k)
            }
            fn cas<'a>(
                &'a self,
                k: &'a str,
                f: crate::kv::CasFn,
            ) -> BoxFuture<'a, Result<(), KvError>> {
                Box::pin(async move {
                    self.inner.cas(k, f).await?;
                    if self.stealing.load(Ordering::Acquire) && self.hb.load(Ordering::SeqCst) < 3 {
                        let tok = self.next_tok.fetch_add(1, Ordering::SeqCst);
                        let _ = self
                            .inner
                            .cas(
                                k,
                                Box::new(move |cur| {
                                    let mut d =
                                        cur.unwrap().downcast_ref::<Desc>().unwrap().clone();
                                    if let Some(inst) = d.ingesters.get_mut("c05-blc") {
                                        inst.tokens = vec![tok];
                                        return Ok(CasOutcome::Update(Box::new(d)));
                                    }
                                    Ok(CasOutcome::NoChange)
                                }),
                            )
                            .await;
                    }
                    Ok(())
                })
            }
            fn watch_key<'a>(
                &'a self,
                k: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_key(k, ctx, f)
            }
            fn watch_prefix<'a>(
                &'a self,
                p: &'a str,
                ctx: ServiceContext,
                f: Box<dyn FnMut(&str, Box<dyn std::any::Any + Send + Sync>) -> bool + Send>,
            ) -> BoxFuture<'a, ()> {
                self.inner.watch_prefix(p, ctx, f)
            }
        }
        let cfg = BasicLifecyclerConfig {
            id: "c05-blc".into(),
            addr: "127.0.0.1:9095".into(),
            zone: String::new(),
            heartbeat_period: Duration::from_millis(60),
            heartbeat_timeout: Duration::from_secs(5),
            num_tokens: 2,
            tokens_observe_period: Duration::from_millis(20),
            keep_instance_in_the_ring_on_shutdown: false,
            token_generator: None,
            versions: HashMap::new(),
        };

        let hb_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        struct CountingDelegate {
            hb: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl BasicLifecyclerDelegate for CountingDelegate {
            fn on_ring_instance_register(
                &self,
                _lifecycler: &BasicLifecycler,
                _ring_desc: &Desc,
                _instance_exists: bool,
                _instance_id: &str,
                _instance_desc: Option<&InstanceDesc>,
            ) -> (InstanceState, Vec<u32>) {
                (InstanceState::Active, vec![10, 20])
            }
            fn on_ring_instance_tokens(&self, _lifecycler: &BasicLifecycler, _tokens: &[u32]) {}
            fn on_ring_instance_stopping<'a>(
                &'a self,
                _lifecycler: &'a BasicLifecycler,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }
            fn on_ring_instance_heartbeat(
                &self,
                _lifecycler: &BasicLifecycler,
                _ring_desc: &mut Desc,
                _instance_desc: &mut InstanceDesc,
            ) {
                self.hb.fetch_add(1, Ordering::SeqCst);
            }
        }

        let kv = Arc::new(StealingKv {
            inner: InMemoryKv::new(ring_desc_codec()),
            stealing: AtomicBool::new(false),
            hb: hb_count.clone(),
            next_tok: std::sync::atomic::AtomicU32::new(100),
        });
        let blc = BasicLifecycler::new(
            cfg,
            "ingester",
            "ring",
            kv.clone(),
            Arc::new(CountingDelegate {
                hb: hb_count.clone(),
            }),
            None,
        );
        blc.register_instance().await.unwrap();
        kv.stealing.store(true, Ordering::Release);
        kv.cas(
            "ring",
            Box::new(|cur| {
                let mut d = cur.unwrap().downcast_ref::<Desc>().unwrap().clone();
                if let Some(inst) = d.ingesters.get_mut("c05-blc") {
                    inst.tokens = vec![99];
                }
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();

        let ctx = ServiceContext::background();
        let res = tokio::time::timeout(
            Duration::from_secs(3),
            blc.wait_stable_tokens(ctx, Duration::from_millis(20)),
        )
        .await;
        res.expect("wait_stable_tokens must complete once 3 heartbeats pass")
            .unwrap();
        assert!(
            hb_count.load(Ordering::SeqCst) >= 3,
            "expected >= 3 heartbeats during observe retries, got {}",
            hb_count.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn c09_auto_forget_delegate_removes_zero_and_negative_timestamps() {
        struct TestNoopBasicDelegate;
        impl BasicLifecyclerDelegate for TestNoopBasicDelegate {
            fn on_ring_instance_register(
                &self,
                _lifecycler: &BasicLifecycler,
                _ring_desc: &Desc,
                _instance_exists: bool,
                _instance_id: &str,
                _instance_desc: Option<&InstanceDesc>,
            ) -> (InstanceState, Vec<u32>) {
                (InstanceState::Active, Vec::new())
            }
            fn on_ring_instance_tokens(&self, _lifecycler: &BasicLifecycler, _tokens: &[u32]) {}
            fn on_ring_instance_stopping<'a>(
                &'a self,
                _lifecycler: &'a BasicLifecycler,
            ) -> BoxFuture<'a, ()> {
                Box::pin(async {})
            }
            fn on_ring_instance_heartbeat(
                &self,
                _lifecycler: &BasicLifecycler,
                _ring_desc: &mut Desc,
                _instance_desc: &mut InstanceDesc,
            ) {
            }
        }
        let cfg = BasicLifecyclerConfig {
            id: "self".into(),
            addr: "127.0.0.1:9095".into(),
            zone: String::new(),
            heartbeat_period: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(60),
            tokens_observe_period: Duration::ZERO,
            num_tokens: 0,
            keep_instance_in_the_ring_on_shutdown: false,
            token_generator: None,
            versions: HashMap::new(),
        };
        let kv = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let blc = BasicLifecycler::new(
            cfg,
            "ingester",
            "ring",
            kv,
            Arc::new(TestNoopBasicDelegate),
            None,
        );

        let delegate = AutoForgetDelegate::new(Duration::from_secs(60), Arc::new(TestNoopBasicDelegate));
        let mut ring_desc = Desc::default();
        let now_s = system_time_to_unix_secs(SystemTime::now());
        ring_desc.ingesters.insert(
            "zero-ts".into(),
            InstanceDesc {
                id: "zero-ts".into(),
                timestamp: 0,
                state: InstanceState::Active as i32,
                ..Default::default()
            },
        );
        ring_desc.ingesters.insert(
            "neg-ts".into(),
            InstanceDesc {
                id: "neg-ts".into(),
                timestamp: -10,
                state: InstanceState::Active as i32,
                ..Default::default()
            },
        );
        ring_desc.ingesters.insert(
            "fresh".into(),
            InstanceDesc {
                id: "fresh".into(),
                timestamp: now_s,
                state: InstanceState::Active as i32,
                ..Default::default()
            },
        );
        let mut self_inst = InstanceDesc {
            id: "self".into(),
            timestamp: now_s,
            ..Default::default()
        };
        delegate.on_ring_instance_heartbeat(&blc, &mut ring_desc, &mut self_inst);
        assert!(!ring_desc.ingesters.contains_key("zero-ts"));
        assert!(!ring_desc.ingesters.contains_key("neg-ts"));
        assert!(ring_desc.ingesters.contains_key("fresh"));
    }
}
