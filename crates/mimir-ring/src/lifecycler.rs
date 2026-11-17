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

use mimir_common::services::{basic_service, ManagedBasicService, ServiceContext, ServiceError};
use mimir_config::ingester::RingConfig;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use parking_lot::{Mutex, RwLock};
use prometheus::{Counter, Gauge, HistogramOpts, HistogramVec, Opts, Registry};
use rand::seq::SliceRandom;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::kv::memberlist::MemberlistKv;
use crate::kv::{
    build_kv_client, registerer_with_kv_name, AsRegisterer, BoxFuture, CasCallbackError,
    CasOutcome, KvClient, KvError,
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
    fn flush(&self);
    fn transfer_out(&self) -> BoxFuture<'_, Result<(), LifecyclerError>>;
}

#[derive(Default)]
pub struct NoopFlushTransferer;

impl FlushTransferer for NoopFlushTransferer {
    fn flush(&self) {}

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
    fn new(ring_name: &str, reg: Option<&Registry>) -> Self {
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

type ActorMsg = Box<dyn FnOnce() + Send>;

pub struct BasicLifecycler {
    cfg: BasicLifecyclerConfig,
    ring_name: String,
    ring_key: String,
    store: Arc<dyn KvClient>,
    delegate: Arc<dyn BasicLifecyclerDelegate>,
    metrics: BasicLifecyclerMetrics,
    curr_instance_desc: RwLock<Option<InstanceDesc>>,
    keep_on_shutdown: AtomicBool,
    token_generator: Arc<dyn TokenGenerator>,
    actor_tx: Mutex<Option<mpsc::Sender<ActorMsg>>>,
}

impl BasicLifecycler {
    pub fn new(
        cfg: BasicLifecyclerConfig,
        ring_name: &str,
        ring_key: &str,
        store: Arc<dyn KvClient>,
        delegate: Arc<dyn BasicLifecyclerDelegate>,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
        let token_generator = cfg
            .token_generator
            .clone()
            .unwrap_or_else(|| Arc::new(RandomTokenGenerator::new()));
        let metrics = BasicLifecyclerMetrics::new(ring_name, reg);
        metrics.tokens_to_own.set(cfg.num_tokens as f64);

        Arc::new(Self {
            keep_on_shutdown: AtomicBool::new(cfg.keep_instance_in_the_ring_on_shutdown),
            cfg,
            ring_name: ring_name.to_string(),
            ring_key: ring_key.to_string(),
            store,
            delegate,
            metrics,
            curr_instance_desc: RwLock::new(None),
            token_generator,
            actor_tx: Mutex::new(None),
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

    pub fn should_keep_instance_in_the_ring_on_shutdown(&self) -> bool {
        self.keep_on_shutdown.load(Ordering::Acquire)
    }

    pub fn set_keep_instance_in_the_ring_on_shutdown(&self, enabled: bool) {
        self.keep_on_shutdown.store(enabled, Ordering::Release);
    }

    pub async fn change_state(&self, state: InstanceState) -> Result<(), LifecyclerError> {
        self.update_instance(move |_ring, inst| {
            if inst.instance_state() == state {
                return false;
            }
            inst.state = state as i32;
            true
        })
        .await
    }

    pub async fn change_read_only_state(&self, read_only: bool) -> Result<(), LifecyclerError> {
        self.update_instance(move |_ring, inst| {
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

        basic_service(
            Some(Box::new(move |ctx| {
                let l = l_start.clone();
                Box::pin(async move {
                    l.register_instance()
                        .await
                        .map_err(|e| ServiceError::failure(format!("register instance in the ring: {e}")))?;

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
                    let (tx, mut rx) = mpsc::channel::<ActorMsg>(32);
                    *l.actor_tx.lock() = Some(tx);
                    let hb_period = l.cfg.heartbeat_period;

                    loop {
                        if hb_period.is_zero() {
                            tokio::select! {
                                _ = ctx.canceled() => break,
                                msg = rx.recv() => {
                                    if let Some(f) = msg { f(); } else { break; }
                                }
                            }
                        } else {
                            tokio::select! {
                                _ = ctx.canceled() => break,
                                msg = rx.recv() => {
                                    if let Some(f) = msg { f(); } else { break; }
                                }
                                _ = tokio::time::sleep(hb_period) => {
                                    l.heartbeat().await;
                                }
                            }
                        }
                    }
                    *l.actor_tx.lock() = None;
                    Ok(())
                })
            })),
            Some(Box::new(move |running_err| {
                let l = l_stop.clone();
                Box::pin(async move {
                    if running_err.is_some() {
                        return Ok(());
                    }
                    let stop_fut = l.delegate.on_ring_instance_stopping(&l);
                    let hb_period = l.cfg.heartbeat_period;
                    tokio::pin!(stop_fut);

                    loop {
                        if hb_period.is_zero() {
                            stop_fut.await;
                            break;
                        }
                        tokio::select! {
                            _ = &mut stop_fut => break,
                            _ = tokio::time::sleep(hb_period) => {
                                l.heartbeat().await;
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
        .with_name(format!("{} basic lifecycler", self.ring_name))
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
        let token_generator = self.token_generator.clone();
        let num_tokens = self.cfg.num_tokens;
        let last_desc: Arc<Mutex<Option<InstanceDesc>>> = Arc::new(Mutex::new(None));
        let last_desc_cb = last_desc.clone();

        let store = self.store.clone();
        self.store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let mut ring_desc = in_val
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();
                    let existing = ring_desc.ingesters.get(&id).cloned();
                    let exists = existing.is_some();

                    let tmp_lc = BasicLifecycler::new(
                        BasicLifecyclerConfig {
                            id: id.clone(),
                            addr: addr.clone(),
                            zone: zone.clone(),
                            heartbeat_period: Duration::ZERO,
                            heartbeat_timeout: Duration::ZERO,
                            tokens_observe_period: Duration::ZERO,
                            num_tokens,
                            keep_instance_in_the_ring_on_shutdown: false,
                            token_generator: Some(token_generator.clone()),
                            versions: versions.clone(),
                        },
                        "",
                        "",
                        store.clone(),
                        delegate.clone(),
                        None,
                    );
                    let (state, mut tokens) = delegate.on_ring_instance_register(
                        &tmp_lc,
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
        loop {
            let hb = self.cfg.heartbeat_period;
            tokio::select! {
                _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
                _ = tokio::time::sleep_until(observe_deadline) => {
                    if self.verify_tokens().await {
                        return Ok(());
                    }
                    observe_deadline = tokio::time::Instant::now() + period;
                }
                _ = tokio::time::sleep(hb), if !hb.is_zero() => {
                    self.heartbeat().await;
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
        res.is_ok() && verified.load(Ordering::Acquire)
    }

    async fn unregister_instance(&self) -> Result<(), LifecyclerError> {
        let id = self.cfg.id.clone();
        self.store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let Some(any) = in_val else {
                        return Err(CasCallbackError::fatal(KvError::NotFound(
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

    async fn update_instance<F>(&self, update: F) -> Result<(), LifecyclerError>
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
        let store = self.store.clone();
        let cfg = self.cfg.clone();
        let token_generator = self.token_generator.clone();
        let res = self
            .update_instance(move |ring, inst| {
                let tmp_lc = BasicLifecycler::new(
                    BasicLifecyclerConfig {
                        id: cfg.id.clone(),
                        addr: cfg.addr.clone(),
                        zone: cfg.zone.clone(),
                        heartbeat_period: cfg.heartbeat_period,
                        heartbeat_timeout: cfg.heartbeat_timeout,
                        tokens_observe_period: cfg.tokens_observe_period,
                        num_tokens: cfg.num_tokens,
                        keep_instance_in_the_ring_on_shutdown: cfg.keep_instance_in_the_ring_on_shutdown,
                        token_generator: Some(token_generator.clone()),
                        versions: cfg.versions.clone(),
                    },
                    "",
                    "",
                    store.clone(),
                    delegate.clone(),
                    None,
                );
                delegate.on_ring_instance_heartbeat(&tmp_lc, ring, inst);
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
            if let Err(err) = lifecycler.change_state(InstanceState::Leaving).await {
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
                if inst.timestamp <= 0 {
                    return None;
                }
                let hb = UNIX_EPOCH + Duration::from_secs(inst.timestamp as u64);
                let elapsed = now.duration_since(hb).unwrap_or(Duration::ZERO);
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

        let token_generator: Arc<dyn TokenGenerator> =
            if cfg.token_generation_strategy == "spread-minimizing" {
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
        })
    }
}

struct LifecyclerMetrics {
    consul_heartbeats: Counter,
    shutdown_duration: HistogramVec,
    readonly: Gauge,
}

impl LifecyclerMetrics {
    fn new(ring_name: &str, reg: Option<&Registry>) -> Self {
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
    flush_transferer: Arc<dyn FlushTransferer>,
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
    can_join_timeout: Duration,
    metrics: LifecyclerMetrics,
}

impl Lifecycler {
    /// Go `ring.NewLifecycler`. `reg` is the Go registerer (Mimir's ingester
    /// passes `cortex_`-prefixed); the KV client registers its metrics with
    /// `kv_name="<ring_name>-lifecycler"` like dskit. The lifecycler's own
    /// metrics are registered into the underlying registry.
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
        let reg: Option<&Registry> = registerer.as_ref().map(|r| r.registry());
        let cfg = LifecyclerConfig::from_ring_config(ring_cfg, listen_port)?;
        Ok(Self::with_store(
            cfg,
            store,
            flush_transferer,
            ring_name,
            ring_key,
            flush_on_shutdown,
            reg,
        ))
    }

    pub fn with_store(
        cfg: LifecyclerConfig,
        kv_store: Arc<dyn KvClient>,
        flush_transferer: Option<Arc<dyn FlushTransferer>>,
        ring_name: &str,
        ring_key: &str,
        flush_on_shutdown: bool,
        reg: Option<&Registry>,
    ) -> Arc<Self> {
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
            can_join_timeout: Duration::from_secs(5 * 60),
            metrics: LifecyclerMetrics::new(ring_name, reg),
            cfg,
            flush_transferer: flush_transferer.unwrap_or_else(|| Arc::new(NoopFlushTransferer)),
            kv_store,
        })
    }

    pub fn get_state(&self) -> InstanceState {
        self.state.read().state
    }

    pub fn get_tokens(&self) -> Vec<u32> {
        self.state.read().tokens.clone()
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

    fn set_tokens(&self, tokens: Vec<u32>) {
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
            return Err(format!(
                "waiting for {:?} after being ready",
                self.cfg.min_ready_duration
            ));
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
        self.update_consul().await
    }

    pub async fn change_read_only_state(&self, read_only: bool) -> Result<(), LifecyclerError> {
        let (prev, _) = self.get_read_only_state();
        if prev == read_only {
            return Ok(());
        }
        info!(readOnly = read_only, ring = %self.ring_name, "changing read-only state of instance in the ring");
        self.set_read_only_internal(read_only, Some(SystemTime::now()));
        self.update_consul().await
    }

    pub async fn claim_tokens_for(&self, source_ingester_id: &str) -> Result<(), LifecyclerError> {
        let my_id = self.id.clone();
        let src_id = source_ingester_id.to_string();
        let claimed = Arc::new(Mutex::new(Vec::new()));
        let claimed_cb = claimed.clone();

        self.kv_store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let Some(any) = in_val else {
                        return Err(CasCallbackError::fatal(KvError::NotFound(
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
        let picked_state = Arc::new(Mutex::new(self.get_state()));
        let picked_tokens: Arc<Mutex<Option<Vec<u32>>>> = Arc::new(Mutex::new(None));
        let picked_reg = Arc::new(Mutex::new(self.state.read().registered_at));
        let picked_ro = Arc::new(Mutex::new(self.get_read_only_state()));

        let ps_cb = picked_state.clone();
        let pt_cb = picked_tokens.clone();
        let pr_cb = picked_reg.clone();
        let pro_cb = picked_ro.clone();

        self.kv_store
            .cas(
                &self.ring_key,
                Box::new(move |cur| {
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
                            inst.state = InstanceState::Pending as i32;
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

    async fn auto_join(
        &self,
        ctx: &ServiceContext,
        target_state: InstanceState,
    ) -> Result<(), LifecyclerError> {
        if self.token_generator.can_join_enabled() {
            let deadline = tokio::time::Instant::now() + self.can_join_timeout;
            loop {
                if ctx.is_canceled() {
                    return Err(LifecyclerError::NotRunning);
                }
                if let Ok(Some(any)) = self.kv_store.get(&self.ring_key).await {
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
                    _ = ctx.canceled() => return Err(LifecyclerError::NotRunning),
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
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

        self.kv_store
            .cas(
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
            )
            .await?;

        self.state.write().state = target_state;
        let toks = std::mem::take(&mut *final_tokens.lock());
        self.set_tokens(toks);
        if let Some(d) = last_desc.lock().take() {
            self.update_counters(&d);
        }
        Ok(())
    }

    async fn verify_tokens(&self) -> bool {
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

        if let Some(toks) = new_tokens_out.lock().take() {
            self.set_tokens(toks);
        }
        res.is_ok() && verified.load(Ordering::Acquire)
    }

    async fn update_consul(&self) -> Result<(), LifecyclerError> {
        let id = self.id.clone();
        let addr = self.addr.clone();
        let zone = self.zone.clone();
        let state = self.get_state();
        let tokens_fallback = self.get_tokens();
        let (ro, ro_ts) = self.get_read_only_state();
        let registered_at = self.state.read().registered_at;
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
                        (ex.tokens.clone(), registered_at)
                    } else {
                        (tokens_fallback.clone(), Some(SystemTime::now()))
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

        if let Some(d) = updated_desc.lock().take() {
            self.update_counters(&d);
        }
        Ok(())
    }

    async fn unregister(&self) -> Result<(), LifecyclerError> {
        let id = self.id.clone();
        self.kv_store
            .cas(
                &self.ring_key,
                Box::new(move |in_val| {
                    let Some(any) = in_val else {
                        return Err(CasCallbackError::fatal(KvError::NotFound(
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
        match self.flush_transferer.transfer_out().await {
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
            self.flush_transferer.flush();
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
                    l.init_ring()
                        .await
                        .map_err(|e| ServiceError::failure(format!("failed to join the ring {}: {e}", l.ring_name)))?;

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
                            _ = ctx.canceled() => {
                                info!(ring = %l.ring_name, "lifecycler loop() exited gracefully");
                                break;
                            }
                            _ = tokio::time::sleep_until(join_deadline), if !join_fired => {
                                join_fired = true;
                                debug!(ring = %l.ring_name, "JoinAfter expired");
                                if l.get_state() == InstanceState::Pending {
                                    info!(ring = %l.ring_name, "auto-joining cluster after timeout");
                                    if !l.cfg.observe_period.is_zero() {
                                        l.auto_join(&ctx, InstanceState::Joining).await.map_err(|e| {
                                            ServiceError::failure(format!("failed to pick tokens: {e}"))
                                        })?;
                                        info!(ring = %l.ring_name, "observing tokens before going ACTIVE");
                                        observe_deadline = Some(tokio::time::Instant::now() + l.cfg.observe_period);
                                    } else {
                                        l.auto_join(&ctx, InstanceState::Active).await.map_err(|e| {
                                            ServiceError::failure(format!("failed to pick tokens: {e}"))
                                        })?;
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
                                if l.verify_tokens().await {
                                    info!(ring = %l.ring_name, "token verification successful");
                                    if let Err(err) = l.change_state(InstanceState::Active).await {
                                        error!(ring = %l.ring_name, %err, "failed to set state to ACTIVE");
                                    }
                                } else {
                                    info!(ring = %l.ring_name, "token verification failed, observing");
                                    observe_deadline = Some(tokio::time::Instant::now() + l.cfg.observe_period);
                                }
                            }
                            _ = hb_interval.tick(), if !hb_period.is_zero() => {
                                l.metrics.consul_heartbeats.inc();
                                if let Err(err) = l.update_consul().await {
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
                    if let Err(err) = l.change_state(InstanceState::Leaving).await {
                        error!(ring = %l.ring_name, %err, "failed to set state to LEAVING");
                    }

                    let shutdown_fut = l.process_shutdown();
                    tokio::pin!(shutdown_fut);
                    let hb_period = l.cfg.heartbeat_period;

                    loop {
                        if hb_period.is_zero() {
                            shutdown_fut.await;
                            break;
                        }
                        tokio::select! {
                            _ = &mut shutdown_fut => break,
                            _ = tokio::time::sleep(hb_period) => {
                                l.metrics.consul_heartbeats.inc();
                                let _ = l.update_consul().await;
                            }
                        }
                    }

                    if l.should_unregister_on_shutdown() {
                        l.unregister()
                            .await
                            .map_err(|e| ServiceError::failure(format!("unregister failed: {e}")))?;
                        info!(ring = %l.ring_name, "instance removed from the KV store");
                    }

                    if !l.cfg.tokens_file_path.is_empty()
                        && l.clear_tokens_on_shutdown()
                        && std::fs::remove_file(&l.cfg.tokens_file_path).is_ok()
                    {
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
}
