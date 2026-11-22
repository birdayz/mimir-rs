//! Replication set result, context, and inflight instance trackers.
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/replication_set_tracker.go
//! - github.com/grafana/dskit/cancellation (error wrapping for context cancellation causes)

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use mimir_proto::ringpb::InstanceDesc;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use tokio::sync::{watch, Notify};

use super::replication_strategy::ReplicationSet;

pub type BoxDynError = Arc<dyn std::error::Error + Send + Sync>;
type ReleaseChannel = (watch::Sender<Option<bool>>, watch::Receiver<Option<bool>>);
pub type ZoneSorter = Arc<dyn Fn(Vec<String>) -> Vec<String> + Send + Sync>;

/// Structured logger hook matching `go-kit/log` assertions in `replication_set_test.go` and
/// `replication_set_tracker_test.go`.
pub trait TrackerLogger: Send + Sync {
    fn log(&self, level: &str, msg: &str, fields: &[(&str, String)]);
}

#[derive(Default)]
pub struct NoopTrackerLogger;

impl TrackerLogger for NoopTrackerLogger {
    #[inline]
    fn log(&self, _level: &str, _msg: &str, _fields: &[(&str, String)]) {}
}

/// Standard context cancellation / deadline error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextError {
    Canceled,
    DeadlineExceeded,
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Canceled => f.write_str("context canceled"),
            Self::DeadlineExceeded => f.write_str("context deadline exceeded"),
        }
    }
}

impl std::error::Error for ContextError {}

/// Cancellation error matching `dskit/cancellation.NewErrorf`, whose `Display` formats as
/// `"context canceled: <cause>"` and `source()` exposes `ContextError::Canceled` (or wraps cause).
#[derive(Debug, Clone)]
pub struct CancellationError {
    pub msg: String,
}

impl CancellationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into() }
    }
}

impl fmt::Display for CancellationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "context canceled: {}", self.msg)
    }
}

impl std::error::Error for CancellationError {}

/// Error returned by `await_start` when an instance's request is no longer needed because quorum
/// has already been reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrResultNotNeeded;

impl fmt::Display for ErrResultNotNeeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("result from this instance is not needed")
    }
}

impl std::error::Error for ErrResultNotNeeded {}

// ---------------------------------------------------------------------------
// RingContext (Go context.Context with WithCancelCause / WithTimeoutCause / WithValue)
// ---------------------------------------------------------------------------

struct CancelState {
    canceled: AtomicBool,
    base_err: Mutex<Option<ContextError>>,
    cause: Mutex<Option<BoxDynError>>,
    deadline: Option<tokio::time::Instant>,
    deadline_cause: Option<BoxDynError>,
    notify: Notify,
    children: Mutex<Vec<Weak<CancelState>>>,
    parent: Option<Arc<CancelState>>,
    on_cancel: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl fmt::Debug for CancelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelState")
            .field("canceled", &self.canceled.load(Ordering::Relaxed))
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl CancelState {
    fn check_deadline_expired(&self) {
        if let Some(dl) = self.deadline {
            if tokio::time::Instant::now() >= dl && !self.canceled.swap(true, Ordering::AcqRel) {
                {
                    let mut be = self.base_err.lock();
                    if be.is_none() {
                        *be = Some(ContextError::DeadlineExceeded);
                    }
                }
                {
                    let mut c = self.cause.lock();
                    if c.is_none() {
                        *c = Some(
                            self.deadline_cause
                                .clone()
                                .unwrap_or_else(|| Arc::new(ContextError::DeadlineExceeded)),
                        );
                    }
                }
                self.notify.notify_waiters();
                self.propagate_cancel();
            }
        }
    }

    fn propagate_cancel(&self) {
        let children = {
            let mut g = self.children.lock();
            std::mem::take(&mut *g)
        };
        let parent_base = self.base_err.lock().clone();
        let parent_cause = self.cause.lock().clone();
        for weak in children {
            if let Some(child) = weak.upgrade() {
                child.cancel_with(parent_base.clone(), parent_cause.clone());
            }
        }
    }

    fn cancel_with(&self, base: Option<ContextError>, cause: Option<BoxDynError>) {
        if !self.canceled.swap(true, Ordering::AcqRel) {
            {
                let mut be = self.base_err.lock();
                if be.is_none() {
                    *be = Some(base.unwrap_or(ContextError::Canceled));
                }
            }
            {
                let mut c = self.cause.lock();
                if c.is_none() {
                    *c = Some(cause.unwrap_or_else(|| Arc::new(ContextError::Canceled)));
                }
            }
            if let Some(cb) = self.on_cancel.lock().take() {
                cb();
            }
            self.notify.notify_waiters();
            self.propagate_cancel();
        }
    }

    fn is_done(&self) -> bool {
        if let Some(p) = &self.parent {
            p.check_deadline_expired();
            if p.canceled.load(Ordering::Acquire) {
                self.cancel_with(p.base_err.lock().clone(), p.cause.lock().clone());
            }
        }
        self.check_deadline_expired();
        self.canceled.load(Ordering::Acquire)
    }
}

/// Hierarchical cancellation & value context matching Go `context.Context`.
#[derive(Clone, Debug)]
pub struct RingContext {
    state: Arc<CancelState>,
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

#[derive(Clone)]
pub struct CancelCauseFunc {
    state: Arc<CancelState>,
}

impl CancelCauseFunc {
    pub fn cancel(&self, cause: Option<BoxDynError>) {
        self.state.cancel_with(Some(ContextError::Canceled), cause);
    }
}

impl Default for RingContext {
    fn default() -> Self {
        Self::background()
    }
}

impl RingContext {
    pub fn background() -> Self {
        Self {
            state: Arc::new(CancelState {
                canceled: AtomicBool::new(false),
                base_err: Mutex::new(None),
                cause: Mutex::new(None),
                deadline: None,
                deadline_cause: None,
                notify: Notify::new(),
                children: Mutex::new(Vec::new()),
                parent: None,
                on_cancel: Mutex::new(None),
            }),
            values: HashMap::new(),
        }
    }

    pub fn with_cancel(&self) -> (Self, CancelCauseFunc) {
        self.with_cancel_cause()
    }

    pub fn with_cancel_cause(&self) -> (Self, CancelCauseFunc) {
        self.with_cancel_cause_inner(None)
    }

    pub fn with_cancel_cause_hook<F>(&self, on_cancel: F) -> (Self, CancelCauseFunc)
    where
        F: FnOnce() + Send + Sync + 'static,
    {
        self.with_cancel_cause_inner(Some(Box::new(on_cancel)))
    }

    fn with_cancel_cause_inner(
        &self,
        on_cancel: Option<Box<dyn FnOnce() + Send + Sync>>,
    ) -> (Self, CancelCauseFunc) {
        let child_state = Arc::new(CancelState {
            canceled: AtomicBool::new(false),
            base_err: Mutex::new(None),
            cause: Mutex::new(None),
            deadline: self.state.deadline,
            deadline_cause: self.state.deadline_cause.clone(),
            notify: Notify::new(),
            children: Mutex::new(Vec::new()),
            parent: Some(Arc::clone(&self.state)),
            on_cancel: Mutex::new(on_cancel),
        });
        {
            let mut children = self.state.children.lock();
            if self.state.is_done() {
                child_state.cancel_with(
                    self.state.base_err.lock().clone(),
                    self.state.cause.lock().clone(),
                );
            } else {
                children.retain(|w| w.strong_count() > 0);
                children.push(Arc::downgrade(&child_state));
            }
        }
        let cancel = CancelCauseFunc {
            state: Arc::clone(&child_state),
        };
        (
            Self {
                state: child_state,
                values: self.values.clone(),
            },
            cancel,
        )
    }

    pub fn with_timeout(&self, timeout: Duration) -> (Self, CancelCauseFunc) {
        self.with_timeout_inner(timeout, None)
    }

    pub fn with_timeout_cause(&self, timeout: Duration, cause: BoxDynError) -> (Self, CancelCauseFunc) {
        self.with_timeout_inner(timeout, Some(cause))
    }

    fn with_timeout_inner(
        &self,
        timeout: Duration,
        deadline_cause: Option<BoxDynError>,
    ) -> (Self, CancelCauseFunc) {
        let dl = tokio::time::Instant::now() + timeout;
        let (effective_dl, effective_cause) = match self.state.deadline {
            Some(pdl) if pdl < dl => (pdl, self.state.deadline_cause.clone()),
            _ => (dl, deadline_cause),
        };
        let child_state = Arc::new(CancelState {
            canceled: AtomicBool::new(false),
            base_err: Mutex::new(None),
            cause: Mutex::new(None),
            deadline: Some(effective_dl),
            deadline_cause: effective_cause,
            notify: Notify::new(),
            children: Mutex::new(Vec::new()),
            parent: Some(Arc::clone(&self.state)),
            on_cancel: Mutex::new(None),
        });
        {
            let mut children = self.state.children.lock();
            if self.state.is_done() {
                child_state.cancel_with(
                    self.state.base_err.lock().clone(),
                    self.state.cause.lock().clone(),
                );
            } else {
                children.retain(|w| w.strong_count() > 0);
                children.push(Arc::downgrade(&child_state));
            }
        }
        child_state.check_deadline_expired();
        let cancel = CancelCauseFunc {
            state: Arc::clone(&child_state),
        };
        (
            Self {
                state: child_state,
                values: self.values.clone(),
            },
            cancel,
        )
    }

    pub fn with_value<K: 'static, V: Send + Sync + 'static>(&self, val: V) -> Self {
        let mut next = self.clone();
        next.values.insert(TypeId::of::<K>(), Arc::new(val));
        next
    }

    pub fn value<K: 'static, V: Send + Sync + 'static>(&self) -> Option<Arc<V>> {
        self.values
            .get(&TypeId::of::<K>())
            .and_then(|v| Arc::clone(v).downcast::<V>().ok())
    }

    pub fn err(&self) -> Option<ContextError> {
        if self.state.is_done() {
            self.state.base_err.lock().clone().or(Some(ContextError::Canceled))
        } else {
            None
        }
    }

    pub fn cause(&self) -> Option<BoxDynError> {
        if self.state.is_done() {
            self.state
                .cause
                .lock()
                .clone()
                .or_else(|| Some(Arc::new(ContextError::Canceled)))
        } else {
            None
        }
    }

    pub fn is_canceled(&self) -> bool {
        self.state.is_done()
    }

    pub async fn done(&self) {
        loop {
            if self.state.is_done() {
                return;
            }
            let notified = self.state.notify.notified();
            if self.state.is_done() {
                return;
            }
            match self.state.deadline {
                Some(dl) => {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(dl) => {
                            self.state.check_deadline_expired();
                            return;
                        }
                    }
                }
                None => {
                    notified.await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tracker traits
// ---------------------------------------------------------------------------

pub trait ReplicationSetResultTracker: Send + Sync {
    fn done(&self, instance_idx: Option<usize>, err: Option<&dyn std::error::Error>);
    fn succeeded(&self) -> bool;
    fn failed(&self) -> bool;
    fn should_include_result_from(&self, instance_idx: usize) -> bool;
    fn start_minimum_requests(&self);
    fn start_additional_requests(&self);
    fn start_all_requests(&self);
    fn await_start(
        &self,
        ctx: &RingContext,
        instance_idx: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), BoxDynError>> + Send + '_>>;
}

pub trait ReplicationSetContextTracker: Send + Sync {
    fn context_for(&self, instance_idx: usize) -> (RingContext, CancelCauseFunc);
    fn cancel_context_for(&self, instance_idx: usize, cause: Option<BoxDynError>);
    fn cancel_all_contexts(&self, cause: Option<BoxDynError>);
}

// ---------------------------------------------------------------------------
// DefaultResultTracker
// ---------------------------------------------------------------------------

struct DefaultResultInner {
    min_succeeded: usize,
    num_succeeded: usize,
    num_errors: usize,
    max_errors: usize,
    pending_instances: Vec<usize>,
}

pub struct DefaultResultTracker {
    instances: Vec<InstanceDesc>,
    release_txs: Vec<ReleaseChannel>,
    inner: Mutex<DefaultResultInner>,
    logger: Arc<dyn TrackerLogger>,
}

impl DefaultResultTracker {
    pub fn new(
        instances: Vec<InstanceDesc>,
        max_errors: usize,
        logger: Option<Arc<dyn TrackerLogger>>,
    ) -> Self {
        let min_succeeded = instances.len().saturating_sub(max_errors);
        let release_txs = (0..instances.len())
            .map(|_| watch::channel(None))
            .collect();
        Self {
            instances,
            release_txs,
            inner: Mutex::new(DefaultResultInner {
                min_succeeded,
                num_succeeded: 0,
                num_errors: 0,
                max_errors,
                pending_instances: Vec::new(),
            }),
            logger: logger.unwrap_or_else(|| Arc::new(NoopTrackerLogger)),
        }
    }

    fn on_succeeded_locked(&self, inner: &mut DefaultResultInner) {
        for idx in inner.pending_instances.drain(..) {
            if let Some((tx, _)) = self.release_txs.get(idx) {
                let _ = tx.send_replace(Some(false));
            }
        }
    }

    fn start_additional_requests_due_to_locked(&self, inner: &mut DefaultResultInner, reason: &str) {
        if !inner.pending_instances.is_empty() {
            let idx = inner.pending_instances.remove(0);
            let inst = &self.instances[idx];
            self.logger.log(
                "debug",
                "starting request to instance",
                &[
                    ("reason", reason.to_string()),
                    ("instanceAddr", inst.addr.clone()),
                    ("instanceID", inst.id.clone()),
                ],
            );
            let _ = self.release_txs[idx].0.send_replace(Some(true));
        }
    }
}

impl ReplicationSetResultTracker for DefaultResultTracker {
    fn done(&self, instance_idx: Option<usize>, err: Option<&dyn std::error::Error>) {
        let mut inner = self.inner.lock();
        if let Some(e) = err {
            let (addr, id) = instance_idx
                .and_then(|idx| self.instances.get(idx))
                .map(|i| (i.addr.clone(), i.id.clone()))
                .unwrap_or_default();
            self.logger.log(
                "warn",
                "instance failed",
                &[
                    ("instanceAddr", addr),
                    ("instanceID", id),
                    ("err", e.to_string()),
                ],
            );
            inner.num_errors += 1;
            self.start_additional_requests_due_to_locked(&mut inner, "failure of other instance");
        } else {
            inner.num_succeeded += 1;
            if inner.num_succeeded >= inner.min_succeeded {
                self.on_succeeded_locked(&mut inner);
            }
        }
    }

    fn succeeded(&self) -> bool {
        let inner = self.inner.lock();
        inner.num_succeeded >= inner.min_succeeded
    }

    fn failed(&self) -> bool {
        let inner = self.inner.lock();
        inner.num_errors > inner.max_errors
    }

    fn should_include_result_from(&self, _instance_idx: usize) -> bool {
        true
    }

    fn start_minimum_requests(&self) {
        let mut inner = self.inner.lock();
        let mut order: Vec<usize> = (0..self.instances.len()).collect();
        order.shuffle(&mut rand::thread_rng());
        inner.pending_instances = Vec::with_capacity(inner.max_errors);

        for idx in order {
            if inner.pending_instances.len() < inner.max_errors {
                inner.pending_instances.push(idx);
            } else {
                let inst = &self.instances[idx];
                self.logger.log(
                    "debug",
                    "starting request to instance",
                    &[
                        ("reason", "initial requests".to_string()),
                        ("instanceAddr", inst.addr.clone()),
                        ("instanceID", inst.id.clone()),
                    ],
                );
                let _ = self.release_txs[idx].0.send_replace(Some(true));
            }
        }

        if inner.num_succeeded >= inner.min_succeeded {
            self.on_succeeded_locked(&mut inner);
        }
    }

    fn start_additional_requests(&self) {
        let mut inner = self.inner.lock();
        self.start_additional_requests_due_to_locked(&mut inner, "hedging");
    }

    fn start_all_requests(&self) {
        for (idx, inst) in self.instances.iter().enumerate() {
            self.logger.log(
                "debug",
                "starting request to instance",
                &[
                    ("reason", "initial requests".to_string()),
                    ("instanceAddr", inst.addr.clone()),
                    ("instanceID", inst.id.clone()),
                ],
            );
            let _ = self.release_txs[idx].0.send_replace(Some(true));
        }
    }

    fn await_start(
        &self,
        ctx: &RingContext,
        instance_idx: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), BoxDynError>> + Send + '_>> {
        let mut rx = self.release_txs[instance_idx].1.clone();
        let ctx = ctx.clone();
        Box::pin(async move {
            loop {
                if let Some(should_start) = *rx.borrow() {
                    return if should_start {
                        Ok(())
                    } else {
                        Err(Arc::new(ErrResultNotNeeded) as BoxDynError)
                    };
                }
                if ctx.is_canceled() {
                    return Err(ctx
                        .cause()
                        .unwrap_or_else(|| Arc::new(ContextError::Canceled)));
                }
                tokio::select! {
                    biased;
                    _ = ctx.done() => {
                        return Err(ctx.cause().unwrap_or_else(|| Arc::new(ContextError::Canceled)));
                    }
                    res = rx.changed() => {
                        if res.is_err() {
                            return Err(Arc::new(ErrResultNotNeeded) as BoxDynError);
                        }
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// DefaultContextTracker
// ---------------------------------------------------------------------------

pub struct DefaultContextTracker {
    parent_ctx: RingContext,
    cancel_funcs: Mutex<HashMap<usize, CancelCauseFunc>>,
}

impl DefaultContextTracker {
    pub fn new(ctx: RingContext, num_instances: usize) -> Self {
        Self {
            parent_ctx: ctx,
            cancel_funcs: Mutex::new(HashMap::with_capacity(num_instances)),
        }
    }
}

impl ReplicationSetContextTracker for DefaultContextTracker {
    fn context_for(&self, instance_idx: usize) -> (RingContext, CancelCauseFunc) {
        let (ctx, cancel) = self.parent_ctx.with_cancel_cause();
        self.cancel_funcs.lock().insert(instance_idx, cancel.clone());
        (ctx, cancel)
    }

    fn cancel_context_for(&self, instance_idx: usize, cause: Option<BoxDynError>) {
        if let Some(cancel) = self.cancel_funcs.lock().remove(&instance_idx) {
            cancel.cancel(cause);
        }
    }

    fn cancel_all_contexts(&self, cause: Option<BoxDynError>) {
        let funcs: Vec<CancelCauseFunc> = self
            .cancel_funcs
            .lock()
            .drain()
            .map(|(_, c)| c)
            .collect();
        for cancel in funcs {
            cancel.cancel(cause.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// ZoneAwareResultTracker
// ---------------------------------------------------------------------------

struct ZoneAwareResultInner {
    waiting_by_zone: HashMap<String, usize>,
    failures_by_zone: HashMap<String, usize>,
    min_successful_zones: usize,
    max_unavailable_zones: usize,
    pending_zones: Vec<String>,
}

pub struct ZoneAwareResultTracker {
    instances: Vec<InstanceDesc>,
    zone_release_txs: HashMap<String, ReleaseChannel>,
    inner: Mutex<ZoneAwareResultInner>,
    zone_sorter: Option<ZoneSorter>,
    logger: Arc<dyn TrackerLogger>,
}

impl ZoneAwareResultTracker {
    pub fn new(
        instances: Vec<InstanceDesc>,
        max_unavailable_zones: usize,
        zone_sorter: Option<ZoneSorter>,
        logger: Option<Arc<dyn TrackerLogger>>,
    ) -> Self {
        let mut waiting_by_zone: HashMap<String, usize> = HashMap::new();
        for inst in &instances {
            *waiting_by_zone.entry(inst.zone.clone()).or_insert(0) += 1;
        }
        let min_successful_zones = waiting_by_zone.len().saturating_sub(max_unavailable_zones);
        let zone_release_txs = waiting_by_zone
            .keys()
            .map(|z| (z.clone(), watch::channel(None)))
            .collect();

        Self {
            instances,
            zone_release_txs,
            inner: Mutex::new(ZoneAwareResultInner {
                waiting_by_zone,
                failures_by_zone: HashMap::new(),
                min_successful_zones,
                max_unavailable_zones,
                pending_zones: Vec::new(),
            }),
            zone_sorter,
            logger: logger.unwrap_or_else(|| Arc::new(NoopTrackerLogger)),
        }
    }

    fn succeeded_locked(inner: &ZoneAwareResultInner) -> bool {
        let mut successful_zones = 0usize;
        for (zone, &num_waiting) in &inner.waiting_by_zone {
            if num_waiting == 0 && inner.failures_by_zone.get(zone).copied().unwrap_or(0) == 0 {
                successful_zones += 1;
            }
        }
        successful_zones >= inner.min_successful_zones
    }

    fn release_zone(&self, zone: &str, should_start: bool) {
        if let Some((tx, _)) = self.zone_release_txs.get(zone) {
            let _ = tx.send_replace(Some(should_start));
        }
    }

    fn on_succeeded_locked(&self, inner: &mut ZoneAwareResultInner) {
        for zone in inner.pending_zones.drain(..) {
            self.release_zone(&zone, false);
        }
    }

    fn start_additional_requests_due_to_locked(
        &self,
        inner: &mut ZoneAwareResultInner,
        reason: &str,
    ) {
        if !inner.pending_zones.is_empty() {
            let zone = inner.pending_zones.remove(0);
            self.logger.log(
                "debug",
                "starting requests to zone",
                &[("reason", reason.to_string()), ("zone", zone.clone())],
            );
            self.release_zone(&zone, true);
        }
    }
}

impl ReplicationSetResultTracker for ZoneAwareResultTracker {
    fn done(&self, instance_idx: Option<usize>, err: Option<&dyn std::error::Error>) {
        let Some(inst) = instance_idx.and_then(|idx| self.instances.get(idx)) else {
            return;
        };
        let mut inner = self.inner.lock();
        if let Some(w) = inner.waiting_by_zone.get_mut(&inst.zone) {
            *w = w.saturating_sub(1);
        }
        if let Some(e) = err {
            let fails = inner.failures_by_zone.entry(inst.zone.clone()).or_insert(0);
            *fails += 1;
            if *fails == 1 {
                self.logger.log(
                    "warn",
                    "request to instance has failed, zone cannot contribute to quorum",
                    &[
                        ("zone", inst.zone.clone()),
                        ("failingInstanceAddr", inst.addr.clone()),
                        ("failingInstanceID", inst.id.clone()),
                        ("err", e.to_string()),
                    ],
                );
                self.start_additional_requests_due_to_locked(&mut inner, "failure of other zone");
            }
        } else if Self::succeeded_locked(&inner) {
            self.on_succeeded_locked(&mut inner);
        }
    }

    fn succeeded(&self) -> bool {
        let inner = self.inner.lock();
        Self::succeeded_locked(&inner)
    }

    fn failed(&self) -> bool {
        let inner = self.inner.lock();
        inner.failures_by_zone.len() > inner.max_unavailable_zones
    }

    fn should_include_result_from(&self, instance_idx: usize) -> bool {
        let Some(inst) = self.instances.get(instance_idx) else {
            return false;
        };
        let inner = self.inner.lock();
        inner.failures_by_zone.get(&inst.zone).copied().unwrap_or(0) == 0
            && inner.waiting_by_zone.get(&inst.zone).copied().unwrap_or(0) == 0
    }

    fn start_minimum_requests(&self) {
        let mut inner = self.inner.lock();
        let mut all_zones: Vec<String> = inner.waiting_by_zone.keys().cloned().collect();
        all_zones = match &self.zone_sorter {
            Some(sorter) => sorter(all_zones),
            None => {
                all_zones.shuffle(&mut rand::thread_rng());
                all_zones
            }
        };

        let min_zones = inner.min_successful_zones.min(all_zones.len());
        for zone in &all_zones[..min_zones] {
            self.logger.log(
                "debug",
                "starting requests to zone",
                &[
                    ("reason", "initial requests".to_string()),
                    ("zone", zone.clone()),
                ],
            );
            self.release_zone(zone, true);
        }
        inner.pending_zones = all_zones[min_zones..].to_vec();

        if Self::succeeded_locked(&inner) {
            self.on_succeeded_locked(&mut inner);
        }
    }

    fn start_additional_requests(&self) {
        let mut inner = self.inner.lock();
        self.start_additional_requests_due_to_locked(&mut inner, "hedging");
    }

    fn start_all_requests(&self) {
        let inner = self.inner.lock();
        let zones: Vec<String> = inner.waiting_by_zone.keys().cloned().collect();
        drop(inner);
        for zone in zones {
            self.logger.log(
                "debug",
                "starting requests to zone",
                &[
                    ("reason", "initial requests".to_string()),
                    ("zone", zone.clone()),
                ],
            );
            self.release_zone(&zone, true);
        }
    }

    fn await_start(
        &self,
        ctx: &RingContext,
        instance_idx: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), BoxDynError>> + Send + '_>> {
        let zone = self.instances[instance_idx].zone.clone();
        let mut rx = self.zone_release_txs[&zone].1.clone();
        let ctx = ctx.clone();
        Box::pin(async move {
            loop {
                if let Some(should_start) = *rx.borrow() {
                    return if should_start {
                        Ok(())
                    } else {
                        Err(Arc::new(ErrResultNotNeeded) as BoxDynError)
                    };
                }
                if ctx.is_canceled() {
                    return Err(ctx
                        .cause()
                        .unwrap_or_else(|| Arc::new(ContextError::Canceled)));
                }
                tokio::select! {
                    biased;
                    _ = ctx.done() => {
                        return Err(ctx.cause().unwrap_or_else(|| Arc::new(ContextError::Canceled)));
                    }
                    res = rx.changed() => {
                        if res.is_err() {
                            return Err(Arc::new(ErrResultNotNeeded) as BoxDynError);
                        }
                    }
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// ZoneAwareContextTracker
// ---------------------------------------------------------------------------

struct ZoneAwareContextInner {
    contexts: HashMap<usize, RingContext>,
    cancel_funcs: HashMap<usize, CancelCauseFunc>,
}

pub struct ZoneAwareContextTracker {
    instances: Vec<InstanceDesc>,
    inner: Mutex<ZoneAwareContextInner>,
}

impl ZoneAwareContextTracker {
    pub fn new(ctx: RingContext, instances: Vec<InstanceDesc>) -> Self {
        let mut contexts = HashMap::with_capacity(instances.len());
        let mut cancel_funcs = HashMap::with_capacity(instances.len());
        for i in 0..instances.len() {
            let (ictx, cancel) = ctx.with_cancel_cause();
            contexts.insert(i, ictx);
            cancel_funcs.insert(i, cancel);
        }
        Self {
            instances,
            inner: Mutex::new(ZoneAwareContextInner {
                contexts,
                cancel_funcs,
            }),
        }
    }
}

impl ReplicationSetContextTracker for ZoneAwareContextTracker {
    fn context_for(&self, instance_idx: usize) -> (RingContext, CancelCauseFunc) {
        let inner = self.inner.lock();
        (
            inner.contexts[&instance_idx].clone(),
            inner.cancel_funcs[&instance_idx].clone(),
        )
    }

    fn cancel_context_for(&self, instance_idx: usize, cause: Option<BoxDynError>) {
        let target_zone = &self.instances[instance_idx].zone;
        let mut inner = self.inner.lock();
        let matching_indices: Vec<usize> = inner
            .cancel_funcs
            .keys()
            .copied()
            .filter(|&i| self.instances[i].zone == *target_zone)
            .collect();
        for idx in matching_indices {
            if let Some(cancel) = inner.cancel_funcs.remove(&idx) {
                cancel.cancel(cause.clone());
            }
            inner.contexts.remove(&idx);
        }
    }

    fn cancel_all_contexts(&self, cause: Option<BoxDynError>) {
        let mut inner = self.inner.lock();
        let funcs: Vec<CancelCauseFunc> = inner.cancel_funcs.drain().map(|(_, c)| c).collect();
        inner.contexts.clear();
        for cancel in funcs {
            cancel.cancel(cause.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// InflightInstanceTracker
// ---------------------------------------------------------------------------

struct InflightInner {
    inflight: Vec<Vec<usize>>,
    expect_more_instances: bool,
}

pub struct InflightInstanceTracker {
    inner: Mutex<InflightInner>,
}

impl InflightInstanceTracker {
    pub fn new(sets: &[ReplicationSet]) -> Self {
        let inflight = sets
            .iter()
            .map(|s| Vec::with_capacity(s.instances.len()))
            .collect();
        Self {
            inner: Mutex::new(InflightInner {
                inflight,
                expect_more_instances: true,
            }),
        }
    }

    pub fn add_instance(&self, replication_set_idx: usize, instance_idx: usize) {
        let mut inner = self.inner.lock();
        let list = &mut inner.inflight[replication_set_idx];
        if !list.contains(&instance_idx) {
            list.push(instance_idx);
        }
    }

    pub fn remove_instance(&self, replication_set_idx: usize, instance_idx: usize) {
        let mut inner = self.inner.lock();
        let list = &mut inner.inflight[replication_set_idx];
        if let Some(pos) = list.iter().position(|&x| x == instance_idx) {
            list.remove(pos);
        }
    }

    pub fn all_instances_added(&self) {
        self.inner.lock().expect_more_instances = false;
    }

    pub fn all_instances_completed(&self) -> bool {
        let inner = self.inner.lock();
        if inner.expect_more_instances {
            return false;
        }
        inner.inflight.iter().all(Vec::is_empty)
    }
}

/// Helper used by tests to count unique zones in a list of instances.
pub fn unique_zone_count(instances: &[InstanceDesc]) -> usize {
    let zones: HashSet<&str> = instances.iter().map(|i| i.zone.as_str()).collect();
    zones.len()
}
