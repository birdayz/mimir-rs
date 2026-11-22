//! Service lifecycle state machine, ServiceManager, listeners, and FailureWatcher.
//!
//! Ported from:
//! - github.com/grafana/dskit/services/service.go
//! - github.com/grafana/dskit/services/services.go
//! - github.com/grafana/dskit/services/basic_service.go
//! - github.com/grafana/dskit/services/manager.go
//! - github.com/grafana/dskit/services/failure_watcher.go

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{mpsc, watch, Notify};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Service lifecycle state matching `dskit/services.State`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    New,
    Starting,
    Running,
    Stopping,
    Terminated,
    Failed,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            State::New => "New",
            State::Starting => "Starting",
            State::Running => "Running",
            State::Stopping => "Stopping",
            State::Terminated => "Terminated",
            State::Failed => "Failed",
        };
        f.write_str(s)
    }
}

/// Errors returned by [`Service`] and [`ServiceManager`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServiceError {
    #[error("invalid service state: {state}, expected: {expected}")]
    InvalidState { state: State, expected: State },

    #[error("invalid service state: {state}, expected: {expected}, failure: {failure}")]
    InvalidStateWithFailure {
        state: State,
        expected: State,
        failure: String,
    },

    #[error("no services")]
    NoServices,

    #[error("unexpected service state: {0}")]
    UnexpectedState(State),

    #[error("not healthy, {terminated} terminated, {failed} failed: [{}]", .reasons.join(", "))]
    NotHealthy {
        terminated: usize,
        failed: usize,
        reasons: Vec<String>,
    },

    #[error("context canceled")]
    Canceled,

    #[error("context deadline exceeded")]
    DeadlineExceeded,

    #[error("{0}")]
    Failure(String),
}

impl ServiceError {
    pub fn failure(msg: impl Into<String>) -> Self {
        Self::Failure(msg.into())
    }
}

/// Cancellable hierarchical context passed to services (`StartAsync` / `RunningFn`).
#[derive(Debug, Clone)]
pub struct ServiceContext {
    inner: Arc<CancelNode>,
}

#[derive(Debug)]
struct CancelNode {
    canceled: AtomicBool,
    deadline: Option<tokio::time::Instant>,
    notify: Notify,
    children: Mutex<Vec<Weak<CancelNode>>>,
}

impl ServiceContext {
    pub fn background() -> Self {
        Self {
            inner: Arc::new(CancelNode {
                canceled: AtomicBool::new(false),
                deadline: None,
                notify: Notify::new(),
                children: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Creates a child context that cancels when the parent cancels or after `timeout` elapses.
    pub fn with_timeout(&self, timeout: Duration) -> Self {
        let dl = tokio::time::Instant::now() + timeout;
        let effective_dl = match self.inner.deadline {
            Some(parent_dl) if parent_dl < dl => parent_dl,
            _ => dl,
        };
        let child = Self {
            inner: Arc::new(CancelNode {
                canceled: AtomicBool::new(false),
                deadline: Some(effective_dl),
                notify: Notify::new(),
                children: Mutex::new(Vec::new()),
            }),
        };
        let mut children = self.inner.children.lock();
        if self.inner.canceled.load(Ordering::Acquire) {
            child.cancel();
        } else {
            children.retain(|w| w.strong_count() > 0);
            children.push(Arc::downgrade(&child.inner));
        }
        child
    }

    /// Creates a child context that cancels automatically when this parent cancels.
    pub fn child(&self) -> Self {
        let child = Self {
            inner: Arc::new(CancelNode {
                canceled: AtomicBool::new(false),
                deadline: self.inner.deadline,
                notify: Notify::new(),
                children: Mutex::new(Vec::new()),
            }),
        };
        let mut children = self.inner.children.lock();
        if self.inner.canceled.load(Ordering::Acquire) {
            child.cancel();
        } else {
            children.retain(|w| w.strong_count() > 0);
            children.push(Arc::downgrade(&child.inner));
        }
        child
    }

    pub fn cancel(&self) {
        if !self.inner.canceled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
            let children = {
                let mut guard = self.inner.children.lock();
                std::mem::take(&mut *guard)
            };
            for weak in children {
                if let Some(c) = weak.upgrade() {
                    let ctx = ServiceContext { inner: c };
                    ctx.cancel();
                }
            }
        }
    }

    pub fn is_canceled(&self) -> bool {
        self.err().is_some()
    }

    pub fn err(&self) -> Option<ServiceError> {
        if self.inner.canceled.load(Ordering::Acquire) {
            return Some(ServiceError::Canceled);
        }
        if let Some(dl) = self.inner.deadline {
            if tokio::time::Instant::now() >= dl {
                return Some(ServiceError::DeadlineExceeded);
            }
        }
        None
    }

    pub async fn canceled(&self) {
        loop {
            if self.is_canceled() {
                return;
            }
            let notified = self.inner.notify.notified();
            if self.is_canceled() {
                return;
            }
            match self.inner.deadline {
                Some(dl) => {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(dl) => return,
                    }
                }
                None => {
                    notified.await;
                }
            }
        }
    }
}

impl Default for ServiceContext {
    fn default() -> Self {
        Self::background()
    }
}

/// Listener notified on [`Service`] state transitions.
pub trait Listener: Send + Sync {
    fn starting(&self) {}
    fn running(&self) {}
    fn stopping(&self, _from: State) {}
    fn terminated(&self, _from: State) {}
    fn failed(&self, _from: State, _failure: &ServiceError) {}
}

type ListenerEvent = Box<dyn FnOnce(&dyn Listener) + Send>;

/// Handle returned by `add_listener` to unregister the listener.
pub struct ListenerHandle {
    unregister: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl ListenerHandle {
    fn new<F: FnOnce() + Send + 'static>(f: F) -> Self {
        Self {
            unregister: Mutex::new(Some(Box::new(f))),
        }
    }

    pub fn noop() -> Self {
        Self {
            unregister: Mutex::new(None),
        }
    }

    pub fn stop(&self) {
        if let Some(f) = self.unregister.lock().take() {
            f();
        }
    }
}

pub type FailedCallback = Box<dyn Fn(State, &ServiceError) + Send + Sync>;

/// Closure-based listener builder matching `dskit/services.NewListener`.
pub struct FnListener {
    starting_fn: Option<Box<dyn Fn() + Send + Sync>>,
    running_fn: Option<Box<dyn Fn() + Send + Sync>>,
    stopping_fn: Option<Box<dyn Fn(State) + Send + Sync>>,
    terminated_fn: Option<Box<dyn Fn(State) + Send + Sync>>,
    failed_fn: Option<FailedCallback>,
}

impl FnListener {
    pub fn new(
        starting_fn: Option<Box<dyn Fn() + Send + Sync>>,
        running_fn: Option<Box<dyn Fn() + Send + Sync>>,
        stopping_fn: Option<Box<dyn Fn(State) + Send + Sync>>,
        terminated_fn: Option<Box<dyn Fn(State) + Send + Sync>>,
        failed_fn: Option<FailedCallback>,
    ) -> Arc<Self> {
        Arc::new(Self {
            starting_fn,
            running_fn,
            stopping_fn,
            terminated_fn,
            failed_fn,
        })
    }
}

impl Listener for FnListener {
    fn starting(&self) {
        if let Some(f) = &self.starting_fn {
            f();
        }
    }
    fn running(&self) {
        if let Some(f) = &self.running_fn {
            f();
        }
    }
    fn stopping(&self, from: State) {
        if let Some(f) = &self.stopping_fn {
            f(from);
        }
    }
    fn terminated(&self, from: State) {
        if let Some(f) = &self.terminated_fn {
            f(from);
        }
    }
    fn failed(&self, from: State, failure: &ServiceError) {
        if let Some(f) = &self.failed_fn {
            f(from, failure);
        }
    }
}

/// Object-safe async service trait matching `dskit/services.Service` + `NamedService`.
pub trait Service: Send + Sync {
    fn service_name(&self) -> String {
        String::new()
    }

    fn start_async(&self, parent_ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>>;

    fn await_running(&self) -> BoxFuture<'_, Result<(), ServiceError>>;

    fn await_running_ctx(&self, ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>>;

    fn stop_async(&self);

    fn await_terminated(&self) -> BoxFuture<'_, Result<(), ServiceError>>;

    fn await_terminated_ctx(&self, ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>>;

    fn failure_case(&self) -> Option<ServiceError>;

    fn state(&self) -> State;

    fn add_listener(&self, listener: Arc<dyn Listener>) -> ListenerHandle;
}

pub type StartingFn =
    Box<dyn Fn(ServiceContext) -> BoxFuture<'static, Result<(), ServiceError>> + Send + Sync>;
pub type RunningFn =
    Box<dyn Fn(ServiceContext) -> BoxFuture<'static, Result<(), ServiceError>> + Send + Sync>;
pub type StoppingFn =
    Box<dyn Fn(Option<ServiceError>) -> BoxFuture<'static, Result<(), ServiceError>> + Send + Sync>;

struct ListenerEntry {
    id: u64,
    tx: mpsc::UnboundedSender<ListenerEvent>,
    pending_rx: Option<mpsc::UnboundedReceiver<ListenerEvent>>,
    listener: Arc<dyn Listener>,
}

struct BasicServiceState {
    state: State,
    failure_case: Option<ServiceError>,
    service_name: String,
    service_ctx: Option<ServiceContext>,
    listeners: Vec<ListenerEntry>,
    next_listener_id: u64,
}

/// Standard [`Service`] implementation built from `StartingFn`, `RunningFn`, and `StoppingFn`.
pub struct BasicService {
    start_fn: Option<StartingFn>,
    running_fn: Option<RunningFn>,
    stopping_fn: Option<StoppingFn>,
    inner: Mutex<BasicServiceState>,
    state_tx: watch::Sender<State>,
    state_rx: watch::Receiver<State>,
}

impl BasicService {
    pub fn new(
        start_fn: Option<StartingFn>,
        running_fn: Option<RunningFn>,
        stopping_fn: Option<StoppingFn>,
    ) -> Arc<Self> {
        let (state_tx, state_rx) = watch::channel(State::New);
        Arc::new(Self {
            start_fn,
            running_fn,
            stopping_fn,
            inner: Mutex::new(BasicServiceState {
                state: State::New,
                failure_case: None,
                service_name: String::new(),
                service_ctx: None,
                listeners: Vec::new(),
                next_listener_id: 1,
            }),
            state_tx,
            state_rx,
        })
    }

    pub fn with_name(self: Arc<Self>, name: impl Into<String>) -> Arc<Self> {
        {
            let mut st = self.inner.lock();
            if st.state == State::New {
                st.service_name = name.into();
            }
        }
        self
    }

    pub fn service_context(&self) -> Option<ServiceContext> {
        let st = self.inner.lock();
        if st.state == State::New {
            None
        } else {
            st.service_ctx.clone()
        }
    }

    fn spawn_pending_listeners_in_runtime(st: &mut BasicServiceState) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            for entry in &mut st.listeners {
                if let Some(mut rx) = entry.pending_rx.take() {
                    let listener = entry.listener.clone();
                    handle.spawn(async move {
                        while let Some(ev) = rx.recv().await {
                            ev(listener.as_ref());
                        }
                    });
                }
            }
        }
    }

    fn notify_listeners(st: &mut BasicServiceState, close: bool, mk: impl Fn() -> ListenerEvent) {
        for entry in &st.listeners {
            let _ = entry.tx.send(mk());
        }
        if close {
            // Collect any unspawned synchronous receivers to drain outside the lock,
            // and drop senders so spawned tasks finish cleanly.
            st.listeners.clear();
        }
    }

    fn take_pending_unspawned(
        st: &mut BasicServiceState,
    ) -> Vec<(mpsc::UnboundedReceiver<ListenerEvent>, Arc<dyn Listener>)> {
        let mut out = Vec::new();
        for entry in &mut st.listeners {
            if let Some(rx) = entry.pending_rx.take() {
                out.push((rx, entry.listener.clone()));
            }
        }
        out
    }

    async fn await_state(
        &self,
        expected: State,
        ctx: Option<ServiceContext>,
    ) -> Result<(), ServiceError> {
        let mut rx = self.state_rx.clone();
        loop {
            let current = *rx.borrow_and_update();
            let done = match expected {
                State::Running => matches!(
                    current,
                    State::Running | State::Stopping | State::Terminated | State::Failed
                ),
                State::Terminated => matches!(current, State::Terminated | State::Failed),
                other => current == other,
            };
            if done {
                if current == expected {
                    return Ok(());
                }
                if let Some(failure) = self.failure_case() {
                    return Err(ServiceError::InvalidStateWithFailure {
                        state: current,
                        expected,
                        failure: failure.to_string(),
                    });
                }
                return Err(ServiceError::InvalidState {
                    state: current,
                    expected,
                });
            }

            match &ctx {
                Some(c) => {
                    tokio::select! {
                        _ = c.canceled() => return Err(c.err().unwrap_or(ServiceError::Canceled)),
                        res = rx.changed() => {
                            if res.is_err() {
                                return Err(ServiceError::Canceled);
                            }
                        }
                    }
                }
                None => {
                    if rx.changed().await.is_err() {
                        return Err(ServiceError::Canceled);
                    }
                }
            }
        }
    }
}

/// Convenience constructors for `BasicService` (mirrors `services.NewIdleService` & `services.NewTimerService`).
pub fn new_idle_service(
    up: Option<StartingFn>,
    down: Option<StoppingFn>,
) -> Arc<BasicService> {
    let run: RunningFn = Box::new(|ctx| {
        Box::pin(async move {
            ctx.canceled().await;
            Ok(())
        })
    });
    BasicService::new(up, Some(run), down)
}

pub fn new_timer_service<F>(
    interval: Duration,
    up: Option<StartingFn>,
    iter: F,
    down: Option<StoppingFn>,
) -> Arc<BasicService>
where
    F: Fn(ServiceContext) -> BoxFuture<'static, Result<(), ServiceError>> + Send + Sync + 'static,
{
    let iter = Arc::new(iter);
    let run: RunningFn = Box::new(move |ctx| {
        let iter = iter.clone();
        Box::pin(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip immediate first tick to match Go time.NewTicker
            loop {
                tokio::select! {
                    _ = ctx.canceled() => return Ok(()),
                    _ = ticker.tick() => {
                        iter(ctx.clone()).await?;
                    }
                }
            }
        })
    });
    BasicService::new(up, Some(run), down)
}

impl Service for BasicService {
    fn service_name(&self) -> String {
        self.inner.lock().service_name.clone()
    }

    fn start_async(&self, _parent_ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>> {
        // BasicService must be held via Arc; use start_async_arc or spawn via Weak?
        // Wait: to allow `&self` without requiring `Arc<Self>` in the trait signature,
        // wait - in Rust `BasicService::new` returns `Arc<BasicService>`, OR we can store a Weak<Self> or state!
        // Let's check: we can't store Weak before Arc::new unless BasicService::new uses Arc::new_cyclic!
        // Using `Arc::new_cyclic` inside `BasicService::new` makes `&self` work seamlessly through `dyn Service`!
        Box::pin(async move {
            Err(ServiceError::Failure(
                "BasicService must be constructed with BasicService::new".to_string(),
            ))
        })
    }

    fn await_running(&self) -> BoxFuture<'_, Result<(), ServiceError>> {
        Box::pin(self.await_state(State::Running, None))
    }

    fn await_running_ctx(&self, ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>> {
        Box::pin(self.await_state(State::Running, Some(ctx)))
    }

    fn stop_async(&self) {
        let mut drain_sync = Vec::new();
        {
            let mut st = self.inner.lock();
            if matches!(
                st.state,
                State::Stopping | State::Terminated | State::Failed
            ) {
                return;
            }
            if st.state == State::New {
                st.state = State::Terminated;
                Self::spawn_pending_listeners_in_runtime(&mut st);
                Self::notify_listeners(&mut st, false, || {
                    Box::new(|l| l.terminated(State::New))
                });
                drain_sync = Self::take_pending_unspawned(&mut st);
                st.listeners.clear();
                let _ = self.state_tx.send(State::Terminated);
            } else if let Some(ctx) = &st.service_ctx {
                ctx.cancel();
            }
        }
        for (mut rx, listener) in drain_sync {
            while let Ok(ev) = rx.try_recv() {
                ev(listener.as_ref());
            }
        }
    }

    fn await_terminated(&self) -> BoxFuture<'_, Result<(), ServiceError>> {
        Box::pin(self.await_state(State::Terminated, None))
    }

    fn await_terminated_ctx(&self, ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>> {
        Box::pin(self.await_state(State::Terminated, Some(ctx)))
    }

    fn failure_case(&self) -> Option<ServiceError> {
        self.inner.lock().failure_case.clone()
    }

    fn state(&self) -> State {
        *self.state_rx.borrow()
    }

    fn add_listener(&self, listener: Arc<dyn Listener>) -> ListenerHandle {
        let mut st = self.inner.lock();
        if matches!(st.state, State::Terminated | State::Failed) {
            return ListenerHandle::noop();
        }
        let id = st.next_listener_id;
        st.next_listener_id += 1;
        let (tx, mut rx) = mpsc::unbounded_channel::<ListenerEvent>();
        let pending_rx = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let listener_clone = listener.clone();
            handle.spawn(async move {
                while let Some(ev) = rx.recv().await {
                    ev(listener_clone.as_ref());
                }
            });
            None
        } else {
            Some(rx)
        };
        st.listeners.push(ListenerEntry {
            id,
            tx,
            pending_rx,
            listener,
        });
        // We can't capture `&self` in 'static handle unless using Weak, so we provide unregister via `Weak` in `ManagedBasicService` or simply store an `Arc<AtomicBool>` active flag?
        // Actually, if we wrap `BasicService` with `Weak` via `BasicService::new` using `Arc::new_cyclic`, we have `Weak<BasicService>`!
        ListenerHandle::noop()
    }
}

/// Full [`BasicService`] holding a self-`Weak` via `Arc::new_cyclic` so `Service::start_async(&self)`
/// and `ListenerHandle::stop()` work seamlessly on `Arc<dyn Service>`.
pub struct ManagedBasicService {
    weak_self: Weak<ManagedBasicService>,
    base: BasicService,
}

impl ManagedBasicService {
    pub fn new(
        start_fn: Option<StartingFn>,
        running_fn: Option<RunningFn>,
        stopping_fn: Option<StoppingFn>,
    ) -> Arc<Self> {
        let (state_tx, state_rx) = watch::channel(State::New);
        Arc::new_cyclic(|weak| Self {
            weak_self: weak.clone(),
            base: BasicService {
                start_fn,
                running_fn,
                stopping_fn,
                inner: Mutex::new(BasicServiceState {
                    state: State::New,
                    failure_case: None,
                    service_name: String::new(),
                    service_ctx: None,
                    listeners: Vec::new(),
                    next_listener_id: 1,
                }),
                state_tx,
                state_rx,
            },
        })
    }

    pub fn with_name(self: Arc<Self>, name: impl Into<String>) -> Arc<Self> {
        {
            let mut st = self.base.inner.lock();
            if st.state == State::New {
                st.service_name = name.into();
            }
        }
        self
    }

    pub fn service_context(&self) -> Option<ServiceContext> {
        self.base.service_context()
    }

    pub fn listener_count(&self) -> usize {
        self.base.inner.lock().listeners.len()
    }

    async fn run_main_managed(self: Arc<Self>, service_ctx: ServiceContext) {
        let mut err = if let Some(start_fn) = &self.base.start_fn {
            start_fn(service_ctx.clone()).await
        } else {
            Ok(())
        };

        if let Err(e) = err {
            {
                let mut st = self.base.inner.lock();
                assert_eq!(st.state, State::Starting);
                st.state = State::Failed;
                st.failure_case = Some(e.clone());
                BasicService::spawn_pending_listeners_in_runtime(&mut st);
                let e_clone = e.clone();
                BasicService::notify_listeners(&mut st, true, move || {
                    let e_inner = e_clone.clone();
                    Box::new(move |l| l.failed(State::Starting, &e_inner))
                });
                let _ = self.base.state_tx.send(State::Failed);
            }
            service_ctx.cancel();
            return;
        }

        let mut stopping_from = State::Starting;
        if service_ctx.is_canceled() {
            err = Ok(());
        } else {
            {
                let mut st = self.base.inner.lock();
                assert_eq!(st.state, State::Starting);
                st.state = State::Running;
                BasicService::spawn_pending_listeners_in_runtime(&mut st);
                BasicService::notify_listeners(&mut st, false, || Box::new(|l| l.running()));
                let _ = self.base.state_tx.send(State::Running);
            }
            stopping_from = State::Running;
            if let Some(running_fn) = &self.base.running_fn {
                err = running_fn(service_ctx.clone()).await;
            }
        }

        let mut failure = err.err();
        {
            let mut st = self.base.inner.lock();
            assert_eq!(st.state, stopping_from);
            st.state = State::Stopping;
            BasicService::spawn_pending_listeners_in_runtime(&mut st);
            BasicService::notify_listeners(&mut st, false, move || {
                Box::new(move |l| l.stopping(stopping_from))
            });
            let _ = self.base.state_tx.send(State::Stopping);
        }

        service_ctx.cancel();

        if let Some(stopping_fn) = &self.base.stopping_fn {
            let stop_res = stopping_fn(failure.clone()).await;
            if failure.is_none() {
                failure = stop_res.err();
            }
        }

        if let Some(fail_err) = failure {
            let mut st = self.base.inner.lock();
            assert_eq!(st.state, State::Stopping);
            st.state = State::Failed;
            st.failure_case = Some(fail_err.clone());
            BasicService::spawn_pending_listeners_in_runtime(&mut st);
            BasicService::notify_listeners(&mut st, true, move || {
                let e_inner = fail_err.clone();
                Box::new(move |l| l.failed(State::Stopping, &e_inner))
            });
            let _ = self.base.state_tx.send(State::Failed);
        } else {
            let mut st = self.base.inner.lock();
            assert_eq!(st.state, State::Stopping);
            st.state = State::Terminated;
            BasicService::spawn_pending_listeners_in_runtime(&mut st);
            BasicService::notify_listeners(&mut st, true, || {
                Box::new(|l| l.terminated(State::Stopping))
            });
            let _ = self.base.state_tx.send(State::Terminated);
        }
    }
}

impl Service for ManagedBasicService {
    fn service_name(&self) -> String {
        self.base.service_name()
    }

    fn start_async(&self, parent_ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>> {
        let res = {
            let mut st = self.base.inner.lock();
            if st.state != State::New {
                Err(ServiceError::InvalidState {
                    state: st.state,
                    expected: State::New,
                })
            } else {
                st.state = State::Starting;
                let service_ctx = parent_ctx.child();
                st.service_ctx = Some(service_ctx.clone());
                BasicService::spawn_pending_listeners_in_runtime(&mut st);
                BasicService::notify_listeners(&mut st, false, || Box::new(|l| l.starting()));
                let _ = self.base.state_tx.send(State::Starting);
                Ok(service_ctx)
            }
        };

        Box::pin(async move {
            let service_ctx = res?;
            if let Some(arc_self) = self.weak_self.upgrade() {
                tokio::spawn(arc_self.run_main_managed(service_ctx));
            }
            Ok(())
        })
    }

    fn await_running(&self) -> BoxFuture<'_, Result<(), ServiceError>> {
        self.base.await_running()
    }

    fn await_running_ctx(&self, ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>> {
        self.base.await_running_ctx(ctx)
    }

    fn stop_async(&self) {
        self.base.stop_async();
    }

    fn await_terminated(&self) -> BoxFuture<'_, Result<(), ServiceError>> {
        self.base.await_terminated()
    }

    fn await_terminated_ctx(&self, ctx: ServiceContext) -> BoxFuture<'_, Result<(), ServiceError>> {
        self.base.await_terminated_ctx(ctx)
    }

    fn failure_case(&self) -> Option<ServiceError> {
        self.base.failure_case()
    }

    fn state(&self) -> State {
        self.base.state()
    }

    fn add_listener(&self, listener: Arc<dyn Listener>) -> ListenerHandle {
        let mut st = self.base.inner.lock();
        if matches!(st.state, State::Terminated | State::Failed) {
            return ListenerHandle::noop();
        }
        let id = st.next_listener_id;
        st.next_listener_id += 1;
        let (tx, mut rx) = mpsc::unbounded_channel::<ListenerEvent>();
        let active = Arc::new(AtomicBool::new(true));
        let active_task = active.clone();
        let pending_rx = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let listener_clone = listener.clone();
            handle.spawn(async move {
                while let Some(ev) = rx.recv().await {
                    if !active_task.load(Ordering::Acquire) {
                        break;
                    }
                    ev(listener_clone.as_ref());
                }
            });
            None
        } else {
            Some(rx)
        };
        st.listeners.push(ListenerEntry {
            id,
            tx,
            pending_rx,
            listener,
        });
        let weak = self.weak_self.clone();
        ListenerHandle::new(move || {
            active.store(false, Ordering::Release);
            if let Some(arc) = weak.upgrade() {
                let mut st = arc.base.inner.lock();
                st.listeners.retain(|e| e.id != id);
            }
        })
    }
}

/// Convenience wrapper for constructing a [`ManagedBasicService`].
pub fn basic_service(
    start_fn: Option<StartingFn>,
    running_fn: Option<RunningFn>,
    stopping_fn: Option<StoppingFn>,
) -> Arc<ManagedBasicService> {
    ManagedBasicService::new(start_fn, running_fn, stopping_fn)
}

pub fn idle_service(up: Option<StartingFn>, down: Option<StoppingFn>) -> Arc<ManagedBasicService> {
    let run: RunningFn = Box::new(|ctx| {
        Box::pin(async move {
            ctx.canceled().await;
            Ok(())
        })
    });
    ManagedBasicService::new(up, Some(run), down)
}

pub fn timer_service<F>(
    interval: Duration,
    up: Option<StartingFn>,
    iter: F,
    down: Option<StoppingFn>,
) -> Arc<ManagedBasicService>
where
    F: Fn(ServiceContext) -> BoxFuture<'static, Result<(), ServiceError>> + Send + Sync + 'static,
{
    let iter = Arc::new(iter);
    let run: RunningFn = Box::new(move |ctx| {
        let iter = iter.clone();
        Box::pin(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ctx.canceled() => return Ok(()),
                    _ = ticker.tick() => {
                        iter(ctx.clone()).await?;
                    }
                }
            }
        })
    });
    ManagedBasicService::new(up, Some(run), down)
}

pub async fn start_and_await_running(
    ctx: ServiceContext,
    service: &dyn Service,
) -> Result<(), ServiceError> {
    service.start_async(ctx.clone()).await?;
    let res = service.await_running_ctx(ctx).await;
    if let Some(failure) = service.failure_case() {
        return Err(failure);
    }
    res
}

pub async fn stop_and_await_terminated(
    ctx: ServiceContext,
    service: &dyn Service,
) -> Result<(), ServiceError> {
    service.stop_async();
    let res = service.await_terminated_ctx(ctx).await;
    if res.is_ok() {
        return Ok(());
    }
    if let Some(failure) = service.failure_case() {
        return Err(failure);
    }
    res
}

pub fn describe_service(service: &dyn Service) -> String {
    let name = service.service_name();
    if name.is_empty() {
        "unnamed-service".to_string()
    } else {
        name
    }
}

// ---------------------------------------------------------------------------
// ServiceManager (dskit/services/manager.go)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagerState {
    Unknown,
    Healthy,
    Stopped,
}

/// Listener for [`ServiceManager`] lifecycle events.
pub trait ManagerListener: Send + Sync {
    fn healthy(&self) {}
    fn stopped(&self) {}
    fn failure(&self, _service: &Arc<dyn Service>) {}
}

type ManagerListenerEvent = Box<dyn FnOnce(&dyn ManagerListener) + Send>;

struct ManagerListenerEntry {
    id: u64,
    tx: mpsc::UnboundedSender<ManagerListenerEvent>,
    pending_rx: Option<mpsc::UnboundedReceiver<ManagerListenerEvent>>,
    listener: Arc<dyn ManagerListener>,
}

struct ServiceManagerInner {
    state: ManagerState,
    by_state: HashMap<State, Vec<usize>>,
    service_states: Vec<State>,
    healthy_closed: bool,
    listeners: Vec<ManagerListenerEntry>,
    next_listener_id: u64,
}

/// Manages a collection of services as a group (`dskit/services.Manager`).
pub struct ServiceManager {
    weak_self: Weak<ServiceManager>,
    services: Vec<Arc<dyn Service>>,
    inner: Mutex<ServiceManagerInner>,
    healthy_tx: watch::Sender<bool>,
    healthy_rx: watch::Receiver<bool>,
    stopped_tx: watch::Sender<bool>,
    stopped_rx: watch::Receiver<bool>,
    _service_listeners: Mutex<Vec<ListenerHandle>>,
}

impl ServiceManager {
    pub fn new(services: Vec<Arc<dyn Service>>) -> Result<Arc<Self>, ServiceError> {
        if services.is_empty() {
            return Err(ServiceError::NoServices);
        }

        let mut by_state: HashMap<State, Vec<usize>> = HashMap::new();
        let mut service_states = Vec::with_capacity(services.len());
        for (idx, s) in services.iter().enumerate() {
            let st = s.state();
            if st != State::New {
                return Err(ServiceError::UnexpectedState(st));
            }
            service_states.push(st);
            by_state.entry(st).or_default().push(idx);
        }

        let (healthy_tx, healthy_rx) = watch::channel(false);
        let (stopped_tx, stopped_rx) = watch::channel(false);

        let manager = Arc::new_cyclic(|weak| Self {
            weak_self: weak.clone(),
            services,
            inner: Mutex::new(ServiceManagerInner {
                state: ManagerState::Unknown,
                by_state,
                service_states,
                healthy_closed: false,
                listeners: Vec::new(),
                next_listener_id: 1,
            }),
            healthy_tx,
            healthy_rx,
            stopped_tx,
            stopped_rx,
            _service_listeners: Mutex::new(Vec::new()),
        });

        let mut handles = Vec::with_capacity(manager.services.len());
        for (idx, svc) in manager.services.iter().enumerate() {
            let l = Arc::new(ManagerServiceListener {
                manager: Arc::downgrade(&manager),
                idx,
            });
            handles.push(svc.add_listener(l));
        }
        *manager._service_listeners.lock() = handles;

        Ok(manager)
    }

    pub async fn start_async(&self, ctx: ServiceContext) -> Result<(), ServiceError> {
        {
            let mut inner = self.inner.lock();
            Self::spawn_pending_listeners_in_runtime(&mut inner);
        }
        for s in &self.services {
            s.start_async(ctx.clone()).await?;
        }
        Ok(())
    }

    pub fn stop_async(&self) {
        for s in &self.services {
            s.stop_async();
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.inner.lock().state == ManagerState::Healthy
    }

    pub async fn await_healthy(&self) -> Result<(), ServiceError> {
        self.await_healthy_internal(None).await
    }

    pub async fn await_healthy_ctx(&self, ctx: ServiceContext) -> Result<(), ServiceError> {
        self.await_healthy_internal(Some(ctx)).await
    }

    async fn await_healthy_internal(&self, ctx: Option<ServiceContext>) -> Result<(), ServiceError> {
        let mut rx = self.healthy_rx.clone();
        loop {
            if *rx.borrow_and_update() {
                break;
            }
            match &ctx {
                Some(c) => {
                    tokio::select! {
                        _ = c.canceled() => return Err(c.err().unwrap_or(ServiceError::Canceled)),
                        res = rx.changed() => {
                            if res.is_err() {
                                break;
                            }
                        }
                    }
                }
                None => {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            }
        }

        let inner = self.inner.lock();
        if inner.state != ManagerState::Healthy {
            let terminated = inner.by_state.get(&State::Terminated).map_or(0, Vec::len);
            let mut failed_reasons = Vec::new();
            if let Some(failed_indices) = inner.by_state.get(&State::Failed) {
                for &idx in failed_indices {
                    if let Some(err) = self.services[idx].failure_case() {
                        failed_reasons.push(err.to_string());
                    }
                }
            }
            return Err(ServiceError::NotHealthy {
                terminated,
                failed: failed_reasons.len(),
                reasons: failed_reasons,
            });
        }
        Ok(())
    }

    pub fn is_stopped(&self) -> bool {
        self.inner.lock().state == ManagerState::Stopped
    }

    pub async fn await_stopped(&self) -> Result<(), ServiceError> {
        self.await_stopped_internal(None).await
    }

    pub async fn await_stopped_ctx(&self, ctx: ServiceContext) -> Result<(), ServiceError> {
        self.await_stopped_internal(Some(ctx)).await
    }

    async fn await_stopped_internal(&self, ctx: Option<ServiceContext>) -> Result<(), ServiceError> {
        let mut rx = self.stopped_rx.clone();
        loop {
            if *rx.borrow_and_update() {
                return Ok(());
            }
            match &ctx {
                Some(c) => {
                    tokio::select! {
                        _ = c.canceled() => return Err(c.err().unwrap_or(ServiceError::Canceled)),
                        res = rx.changed() => {
                            if res.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
                None => {
                    if rx.changed().await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Snapshot of services grouped by state, matching `Manager.ServicesByState()` for `/services`.
    pub fn services_by_state(&self) -> HashMap<State, Vec<Arc<dyn Service>>> {
        let inner = self.inner.lock();
        let mut out = HashMap::new();
        for (&st, indices) in &inner.by_state {
            let list = indices.iter().map(|&i| self.services[i].clone()).collect();
            out.insert(st, list);
        }
        out
    }

    /// Returns `(service_name, state)` pairs for every managed service (helpful for `/services` endpoint).
    pub fn listener_count(&self) -> usize {
        self.inner.lock().listeners.len()
    }

    pub fn service_states(&self) -> Vec<(String, State)> {
        let inner = self.inner.lock();
        self.services
            .iter()
            .enumerate()
            .map(|(i, s)| (describe_service(s.as_ref()), inner.service_states[i]))
            .collect()
    }

    pub fn add_listener(&self, listener: Arc<dyn ManagerListener>) -> ListenerHandle {
        let mut inner = self.inner.lock();
        if inner.state == ManagerState::Stopped {
            return ListenerHandle::noop();
        }
        let id = inner.next_listener_id;
        inner.next_listener_id += 1;
        let (tx, mut rx) = mpsc::unbounded_channel::<ManagerListenerEvent>();
        let active = Arc::new(AtomicBool::new(true));
        let active_task = active.clone();
        let pending_rx = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let listener_clone = listener.clone();
            handle.spawn(async move {
                while let Some(ev) = rx.recv().await {
                    if !active_task.load(Ordering::Acquire) {
                        break;
                    }
                    ev(listener_clone.as_ref());
                }
            });
            None
        } else {
            Some(rx)
        };
        inner.listeners.push(ManagerListenerEntry {
            id,
            tx,
            pending_rx,
            listener,
        });
        let weak = self.weak_self.clone();
        ListenerHandle::new(move || {
            active.store(false, Ordering::Release);
            if let Some(arc) = weak.upgrade() {
                let mut inner = arc.inner.lock();
                inner.listeners.retain(|e| e.id != id);
            }
        })
    }

    fn spawn_pending_listeners_in_runtime(inner: &mut ServiceManagerInner) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            for entry in &mut inner.listeners {
                if let Some(mut rx) = entry.pending_rx.take() {
                    let listener = entry.listener.clone();
                    handle.spawn(async move {
                        while let Some(ev) = rx.recv().await {
                            ev(listener.as_ref());
                        }
                    });
                }
            }
        }
    }

    fn service_state_changed(&self, idx: usize, from: State, to: State) {
        let mut inner = self.inner.lock();
        Self::spawn_pending_listeners_in_runtime(&mut inner);

        if let Some(list) = inner.by_state.get_mut(&from) {
            list.retain(|&i| i != idx);
            if list.is_empty() {
                inner.by_state.remove(&from);
            }
        }
        inner.by_state.entry(to).or_default().push(idx);
        inner.service_states[idx] = to;

        if to == State::Failed {
            let svc = self.services[idx].clone();
            for entry in &inner.listeners {
                let svc_c = svc.clone();
                let _ = entry.tx.send(Box::new(move |l| l.failure(&svc_c)));
            }
        }

        let running = inner.by_state.get(&State::Running).map_or(0, Vec::len);
        let stopping = inner.by_state.get(&State::Stopping).map_or(0, Vec::len);
        let done = inner.by_state.get(&State::Terminated).map_or(0, Vec::len)
            + inner.by_state.get(&State::Failed).map_or(0, Vec::len);
        let all = self.services.len();

        if running == all {
            inner.state = ManagerState::Healthy;
            inner.healthy_closed = true;
            let _ = self.healthy_tx.send(true);
            for entry in &inner.listeners {
                let _ = entry.tx.send(Box::new(|l| l.healthy()));
            }
        } else if done == all {
            if !inner.healthy_closed {
                inner.healthy_closed = true;
                let _ = self.healthy_tx.send(true);
            }
            inner.state = ManagerState::Stopped;
            let _ = self.stopped_tx.send(true);
            for entry in &inner.listeners {
                let _ = entry.tx.send(Box::new(|l| l.stopped()));
            }
            inner.listeners.clear();
        } else {
            if !inner.healthy_closed && (done > 0 || stopping > 0) {
                inner.healthy_closed = true;
                let _ = self.healthy_tx.send(true);
            }
            inner.state = ManagerState::Unknown;
        }
    }
}

struct ManagerServiceListener {
    manager: Weak<ServiceManager>,
    idx: usize,
}

impl Listener for ManagerServiceListener {
    fn starting(&self) {
        if let Some(m) = self.manager.upgrade() {
            m.service_state_changed(self.idx, State::New, State::Starting);
        }
    }
    fn running(&self) {
        if let Some(m) = self.manager.upgrade() {
            m.service_state_changed(self.idx, State::Starting, State::Running);
        }
    }
    fn stopping(&self, from: State) {
        if let Some(m) = self.manager.upgrade() {
            m.service_state_changed(self.idx, from, State::Stopping);
        }
    }
    fn terminated(&self, from: State) {
        if let Some(m) = self.manager.upgrade() {
            m.service_state_changed(self.idx, from, State::Terminated);
        }
    }
    fn failed(&self, from: State, _failure: &ServiceError) {
        if let Some(m) = self.manager.upgrade() {
            m.service_state_changed(self.idx, from, State::Failed);
        }
    }
}

pub async fn start_manager_and_await_healthy(
    ctx: ServiceContext,
    manager: &ServiceManager,
) -> Result<(), ServiceError> {
    manager.start_async(ctx.clone()).await?;
    manager.await_healthy_ctx(ctx).await
}

pub async fn stop_manager_and_await_stopped(
    ctx: ServiceContext,
    manager: &ServiceManager,
) -> Result<(), ServiceError> {
    manager.stop_async();
    manager.await_stopped_ctx(ctx).await
}

/// Failure notification emitted by [`FailureWatcher`], preserving the underlying `cause`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("service {service_name} failed: {cause}")]
pub struct WatchedFailure {
    pub service_name: String,
    pub cause: ServiceError,
}

/// Watches services or managers for failures and sends them on a channel (`FailureWatcher`).
pub struct FailureWatcher {
    tx: Mutex<Option<mpsc::UnboundedSender<WatchedFailure>>>,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<WatchedFailure>>>,
    handles: Mutex<Vec<ListenerHandle>>,
}

impl FailureWatcher {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx: Mutex::new(Some(tx)),
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
            handles: Mutex::new(Vec::new()),
        }
    }

    pub fn watch_service(&self, service: &Arc<dyn Service>) {
        let tx = self
            .tx
            .lock()
            .clone()
            .expect("FailureWatcher has been closed");
        let name = describe_service(service.as_ref());
        let l = FnListener::new(
            None,
            None,
            None,
            None,
            Some(Box::new(move |_, err| {
                let _ = tx.send(WatchedFailure {
                    service_name: name.clone(),
                    cause: err.clone(),
                });
            })),
        );
        self.handles.lock().push(service.add_listener(l));
    }

    pub fn watch_manager(&self, manager: &ServiceManager) {
        let tx = self
            .tx
            .lock()
            .clone()
            .expect("FailureWatcher has been closed");
        struct MgrWatch(mpsc::UnboundedSender<WatchedFailure>);
        impl ManagerListener for MgrWatch {
            fn failure(&self, service: &Arc<dyn Service>) {
                let name = describe_service(service.as_ref());
                let cause = service
                    .failure_case()
                    .unwrap_or_else(|| ServiceError::Failure(String::new()));
                let _ = self.0.send(WatchedFailure {
                    service_name: name,
                    cause,
                });
            }
        }
        self.handles
            .lock()
            .push(manager.add_listener(Arc::new(MgrWatch(tx))));
    }

    pub fn try_recv(&self) -> Option<ServiceError> {
        self.rx
            .try_lock()
            .ok()
            .and_then(|mut rx| rx.try_recv().ok())
            .map(|wf| ServiceError::Failure(wf.to_string()))
    }

    pub async fn recv(&self) -> Option<WatchedFailure> {
        self.rx.lock().await.recv().await
    }

    pub fn close(&self) {
        let handles = std::mem::take(&mut *self.handles.lock());
        for h in handles {
            h.stop();
        }
        *self.tx.lock() = None;
    }
}

impl Default for FailureWatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_service_lifecycle() {
        let svc = idle_service(None, None).with_name("idle");
        assert_eq!(svc.state(), State::New);
        assert_eq!(svc.service_name(), "idle");

        let ctx = ServiceContext::background();
        start_and_await_running(ctx.clone(), svc.as_ref())
            .await
            .unwrap();
        assert_eq!(svc.state(), State::Running);

        stop_and_await_terminated(ctx, svc.as_ref())
            .await
            .unwrap();
        assert_eq!(svc.state(), State::Terminated);
    }

    #[tokio::test]
    async fn test_service_manager_healthy_and_failure() {
        let s1 = idle_service(None, None).with_name("s1");
        let s2 = basic_service(
            Some(Box::new(|_| {
                Box::pin(async { Err(ServiceError::failure("boom")) })
            })),
            None,
            None,
        )
        .with_name("s2");

        let mgr = ServiceManager::new(vec![s1.clone(), s2.clone()]).unwrap();
        let ctx = ServiceContext::background();
        let err = start_manager_and_await_healthy(ctx.clone(), &mgr)
            .await
            .unwrap_err();
        assert!(matches!(err, ServiceError::NotHealthy { failed: 1, .. }));

        stop_manager_and_await_stopped(ctx, &mgr).await.unwrap();
        assert!(mgr.is_stopped());
    }
}
