//! 1:1 upstream-ported tests for `dskit/services` (29) and `dskit/concurrency` (20).

use std::collections::{BTreeSet, HashMap};
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_common::concurrency::{
    create_jobs_from_strings, for_each, for_each_job, for_each_job_merge_results, for_each_user,
    LimitedConcurrencySingleFlight, ReusableGoroutinesPool,
};
use mimir_common::multierror::BoxError;
use mimir_common::services::{
    basic_service, describe_service, idle_service, start_and_await_running,
    start_manager_and_await_healthy, stop_and_await_terminated, stop_manager_and_await_stopped,
    timer_service, FailureWatcher, Listener, ManagedBasicService, ManagerListener, RunningFn,
    Service, ServiceContext, ServiceError, ServiceManager, StartingFn, State, StoppingFn,
};
use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};

// ============================================================================
// Helpers for dskit/services tests
// ============================================================================

#[derive(Clone, Default)]
struct ServConf {
    start_sleep: Duration,
    start_return_ctx_err: bool,
    start_ret_val: Option<ServiceError>,
    run_sleep: Duration,
    run_return_ctx_err: bool,
    run_ret_val: Option<ServiceError>,
    stop_ret_val: Option<ServiceError>,
}

fn new_serv(conf: ServConf) -> Arc<ManagedBasicService> {
    let c1 = conf.clone();
    let start_fn: StartingFn = Box::new(move |ctx| {
        let c = c1.clone();
        Box::pin(async move {
            tokio::select! {
                _ = tokio::time::sleep(c.start_sleep) => {}
                _ = ctx.canceled() => {
                    if c.start_return_ctx_err {
                        return Err(ctx.err().unwrap_or(ServiceError::Canceled));
                    }
                }
            }
            match c.start_ret_val {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    });

    let c2 = conf.clone();
    let run_fn: RunningFn = Box::new(move |ctx| {
        let c = c2.clone();
        Box::pin(async move {
            tokio::select! {
                _ = tokio::time::sleep(c.run_sleep) => {}
                _ = ctx.canceled() => {
                    if c.run_return_ctx_err {
                        return Err(ctx.err().unwrap_or(ServiceError::Canceled));
                    }
                }
            }
            match c.run_ret_val {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    });

    let c3 = conf;
    let stop_fn: StoppingFn = Box::new(move |_| {
        let c = c3.clone();
        Box::pin(async move {
            match c.stop_ret_val {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    });

    basic_service(Some(start_fn), Some(run_fn), Some(stop_fn))
}



struct ChanListener {
    tx: Mutex<Option<mpsc::UnboundedSender<String>>>,
}

impl ChanListener {
    fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                tx: Mutex::new(Some(tx)),
            }),
            rx,
        )
    }
}

impl Listener for ChanListener {
    fn starting(&self) {
        if let Some(tx) = &*self.tx.lock() {
            let _ = tx.send("starting".to_string());
        }
    }
    fn running(&self) {
        if let Some(tx) = &*self.tx.lock() {
            let _ = tx.send("running".to_string());
        }
    }
    fn stopping(&self, from: State) {
        if let Some(tx) = &*self.tx.lock() {
            let _ = tx.send(format!("stopping: {from}"));
        }
    }
    fn terminated(&self, from: State) {
        if let Some(tx) = self.tx.lock().take() {
            let _ = tx.send(format!("terminated: {from}"));
        }
    }
    fn failed(&self, from: State, failure: &ServiceError) {
        if let Some(tx) = self.tx.lock().take() {
            let _ = tx.send(format!("failed: {from}: {failure}"));
        }
    }
}

struct GatheringManagerListener {
    tx: Mutex<Option<mpsc::UnboundedSender<String>>>,
}

impl GatheringManagerListener {
    fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                tx: Mutex::new(Some(tx)),
            }),
            rx,
        )
    }
}

impl ManagerListener for GatheringManagerListener {
    fn healthy(&self) {
        if let Some(tx) = &*self.tx.lock() {
            let _ = tx.send("healthy".to_string());
        }
    }
    fn stopped(&self) {
        if let Some(tx) = self.tx.lock().take() {
            let _ = tx.send("stopped".to_string());
        }
    }
    fn failure(&self, _service: &Arc<dyn Service>) {
        if let Some(tx) = &*self.tx.lock() {
            let _ = tx.send("failed".to_string());
        }
    }
}

async fn drain_rx(mut rx: mpsc::UnboundedReceiver<String>) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(s) = rx.recv().await {
        out.push(s);
    }
    out
}

fn service_that_doesnt_do_anything() -> Arc<dyn Service> {
    idle_service(None, None)
}

fn service_that_fails_to_start() -> Arc<dyn Service> {
    basic_service(
        Some(Box::new(|_| {
            Box::pin(async { Err(ServiceError::failure("failed to start")) })
        })),
        None,
        None,
    )
}

fn service_that_stops_on_its_own_after_timeout(timeout: Duration) -> Arc<dyn Service> {
    basic_service(
        None,
        Some(Box::new(move |ctx| {
            Box::pin(async move {
                tokio::select! {
                    _ = ctx.canceled() => Ok(()),
                    _ = tokio::time::sleep(timeout) => Ok(()),
                }
            })
        })),
        None,
    )
}

fn merge_states(m: &HashMap<State, Vec<Arc<dyn Service>>>, states: &[State]) -> Vec<usize> {
    let mut ptrs = Vec::new();
    for st in states {
        if let Some(svcs) = m.get(st) {
            for s in svcs {
                ptrs.push(Arc::as_ptr(s) as *const () as usize);
            }
        }
    }
    ptrs.sort();
    ptrs
}

fn svc_ptrs(svcs: &[Arc<dyn Service>]) -> Vec<usize> {
    let mut ptrs: Vec<usize> = svcs
        .iter()
        .map(|s| Arc::as_ptr(s) as *const () as usize)
        .collect();
    ptrs.sort();
    ptrs
}

// ============================================================================
// github.com/grafana/dskit/services/basic_service_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/services/basic_service_test.go::TestStopInNew
#[tokio::test]
async fn test_stop_in_new() {
    let s = new_serv(ServConf::default());
    assert_eq!(s.state(), State::New);
    s.stop_async();
    assert!(s.await_running().await.is_err());
    assert!(s.await_terminated().await.is_ok());
    assert_eq!(s.state(), State::Terminated);
}

// Upstream: github.com/grafana/dskit/services/basic_service_test.go::TestAllFunctionality
#[tokio::test]
async fn test_all_functionality() {
    let err_start = ServiceError::failure("start failed");
    let err_run = ServiceError::failure("runFn failed");
    let err_stop = ServiceError::failure("stop failed");

    struct Case {
        name: &'static str,
        conf: ServConf,
        cancel_after_start: bool,
        stop_after_start: bool,
        cancel_after_running: bool,
        await_running_errs: Vec<ServiceError>,
        await_terminated_err: Option<ServiceError>,
        failure_case: Option<ServiceError>,
        listener_log: Vec<&'static str>,
    }

    let cases = vec![
        Case {
            name: "normal flow",
            conf: ServConf {
                start_sleep: Duration::from_millis(50),
                run_sleep: Duration::from_millis(50),
                ..Default::default()
            },
            cancel_after_start: false,
            stop_after_start: false,
            cancel_after_running: false,
            await_running_errs: vec![],
            await_terminated_err: None,
            failure_case: None,
            listener_log: vec!["starting", "running", "stopping: Running", "terminated: Stopping"],
        },
        Case {
            name: "start returns error",
            conf: ServConf {
                start_sleep: Duration::from_millis(50),
                start_ret_val: Some(err_start.clone()),
                run_sleep: Duration::from_millis(50),
                ..Default::default()
            },
            cancel_after_start: false,
            stop_after_start: false,
            cancel_after_running: false,
            await_running_errs: vec![ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Running,
                failure: "start failed".to_string(),
            }],
            await_terminated_err: Some(ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Terminated,
                failure: "start failed".to_string(),
            }),
            failure_case: Some(err_start),
            listener_log: vec!["starting", "failed: Starting: start failed"],
        },
        Case {
            name: "start is canceled via context and returns cancelation error",
            conf: ServConf {
                start_sleep: Duration::from_millis(200),
                start_return_ctx_err: true,
                run_sleep: Duration::from_millis(50),
                ..Default::default()
            },
            cancel_after_start: true,
            stop_after_start: false,
            cancel_after_running: false,
            await_running_errs: vec![ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Running,
                failure: "context canceled".to_string(),
            }],
            await_terminated_err: Some(ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Terminated,
                failure: "context canceled".to_string(),
            }),
            failure_case: Some(ServiceError::Canceled),
            listener_log: vec!["starting", "failed: Starting: context canceled"],
        },
        Case {
            name: "start is canceled via context, doesn't return error",
            conf: ServConf {
                start_sleep: Duration::from_millis(200),
                start_return_ctx_err: false,
                run_sleep: Duration::from_millis(50),
                ..Default::default()
            },
            cancel_after_start: true,
            stop_after_start: false,
            cancel_after_running: false,
            await_running_errs: vec![
                ServiceError::InvalidState {
                    state: State::Stopping,
                    expected: State::Running,
                },
                ServiceError::InvalidState {
                    state: State::Terminated,
                    expected: State::Running,
                },
            ],
            await_terminated_err: None,
            failure_case: None,
            listener_log: vec!["starting", "stopping: Starting", "terminated: Stopping"],
        },
        Case {
            name: "start is canceled via StopAsync, but start doesn't return error",
            conf: ServConf {
                start_sleep: Duration::from_millis(200),
                start_return_ctx_err: false,
                run_sleep: Duration::from_millis(50),
                ..Default::default()
            },
            cancel_after_start: false,
            stop_after_start: true,
            cancel_after_running: false,
            await_running_errs: vec![
                ServiceError::InvalidState {
                    state: State::Stopping,
                    expected: State::Running,
                },
                ServiceError::InvalidState {
                    state: State::Terminated,
                    expected: State::Running,
                },
            ],
            await_terminated_err: None,
            failure_case: None,
            listener_log: vec!["starting", "stopping: Starting", "terminated: Stopping"],
        },
        Case {
            name: "runFn returns error",
            conf: ServConf {
                start_sleep: Duration::from_millis(20),
                run_sleep: Duration::from_millis(20),
                run_ret_val: Some(err_run.clone()),
                ..Default::default()
            },
            cancel_after_start: false,
            stop_after_start: false,
            cancel_after_running: false,
            await_running_errs: vec![],
            await_terminated_err: Some(ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Terminated,
                failure: "runFn failed".to_string(),
            }),
            failure_case: Some(err_run.clone()),
            listener_log: vec![
                "starting",
                "running",
                "stopping: Running",
                "failed: Stopping: runFn failed",
            ],
        },
        Case {
            name: "runFn returns error from context cancelation",
            conf: ServConf {
                start_sleep: Duration::from_millis(20),
                run_sleep: Duration::from_millis(200),
                run_return_ctx_err: true,
                ..Default::default()
            },
            cancel_after_start: false,
            stop_after_start: false,
            cancel_after_running: true,
            await_running_errs: vec![],
            await_terminated_err: Some(ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Terminated,
                failure: "context canceled".to_string(),
            }),
            failure_case: Some(ServiceError::Canceled),
            listener_log: vec![
                "starting",
                "running",
                "stopping: Running",
                "failed: Stopping: context canceled",
            ],
        },
        Case {
            name: "runFn and stop both return error, only one is reported",
            conf: ServConf {
                start_sleep: Duration::from_millis(20),
                run_sleep: Duration::from_millis(20),
                run_ret_val: Some(err_run.clone()),
                stop_ret_val: Some(err_stop.clone()),
                ..Default::default()
            },
            cancel_after_start: false,
            stop_after_start: false,
            cancel_after_running: false,
            await_running_errs: vec![],
            await_terminated_err: Some(ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Terminated,
                failure: "runFn failed".to_string(),
            }),
            failure_case: Some(err_run),
            listener_log: vec![
                "starting",
                "running",
                "stopping: Running",
                "failed: Stopping: runFn failed",
            ],
        },
        Case {
            name: "stop returns error",
            conf: ServConf {
                start_sleep: Duration::from_millis(20),
                run_sleep: Duration::from_millis(20),
                stop_ret_val: Some(err_stop.clone()),
                ..Default::default()
            },
            cancel_after_start: false,
            stop_after_start: false,
            cancel_after_running: false,
            await_running_errs: vec![],
            await_terminated_err: Some(ServiceError::InvalidStateWithFailure {
                state: State::Failed,
                expected: State::Terminated,
                failure: "stop failed".to_string(),
            }),
            failure_case: Some(err_stop),
            listener_log: vec![
                "starting",
                "running",
                "stopping: Running",
                "failed: Stopping: stop failed",
            ],
        },
    ];

    for tc in cases {
        let s = new_serv(tc.conf);
        let (listener, rx) = ChanListener::new();
        s.add_listener(listener);
        assert_eq!(s.state(), State::New, "{}", tc.name);

        let ctx = ServiceContext::background();
        s.start_async(ctx.clone()).await.expect(tc.name);
        assert!(s.start_async(ctx.clone()).await.is_err(), "{}", tc.name);

        if tc.cancel_after_start {
            ctx.cancel();
        }
        if tc.stop_after_start {
            s.stop_async();
        }

        let await_ctx = ServiceContext::background().with_timeout(Duration::from_secs(5));
        let ar_res = s.await_running_ctx(await_ctx).await;
        if tc.await_running_errs.is_empty() {
            assert!(ar_res.is_ok(), "{}: {ar_res:?}", tc.name);
        } else {
            let err = ar_res.unwrap_err();
            assert!(
                tc.await_running_errs.contains(&err),
                "{}: got {err:?}",
                tc.name
            );
        }

        if tc.cancel_after_running {
            ctx.cancel();
        }

        let await_ctx = ServiceContext::background().with_timeout(Duration::from_secs(5));
        let at_res = s.await_terminated_ctx(await_ctx).await;
        match &tc.await_terminated_err {
            None => assert!(at_res.is_ok(), "{}: {at_res:?}", tc.name),
            Some(exp) => assert_eq!(&at_res.unwrap_err(), exp, "{}", tc.name),
        }
        assert_eq!(s.failure_case(), tc.failure_case, "{}", tc.name);

        let log = drain_rx(rx).await;
        let expected_log: Vec<String> = tc.listener_log.into_iter().map(str::to_string).collect();
        assert_eq!(log, expected_log, "{}", tc.name);
    }

}

// Upstream: github.com/grafana/dskit/services/basic_service_test.go::TestFailureCaseFromRunningIsPassedToStopping
#[tokio::test]
async fn test_failure_case_from_running_is_passed_to_stopping() {
    let err = ServiceError::failure("test error");
    let err_c = err.clone();
    let s = basic_service(
        None,
        Some(Box::new(move |_| {
            let e = err_c.clone();
            Box::pin(async move { Err(e) })
        })),
        Some(Box::new(|failure_case| {
            Box::pin(async move {
                assert_eq!(failure_case, Some(ServiceError::failure("test error")));
                Ok(())
            })
        })),
    );
    s.start_async(ServiceContext::background()).await.unwrap();
    assert!(s.await_terminated().await.is_err());
    assert_eq!(s.failure_case(), Some(err));
}

// Upstream: github.com/grafana/dskit/services/basic_service_test.go::TestServiceName
#[tokio::test]
async fn test_service_name() {
    let s = idle_service(None, None).with_name("test name");
    assert_eq!(describe_service(s.as_ref()), "test name");

    let ctx = ServiceContext::background();
    s.start_async(ctx).await.unwrap();

    let s = s.with_name("new");
    assert_eq!(describe_service(s.as_ref()), "test name");
    stop_and_await_terminated(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/services/basic_service_test.go::TestListenerCancellationUnstartedService
#[tokio::test]
async fn test_listener_cancellation_unstarted_service() {
    let s = idle_service(None, None);
    let (sl, _) = ChanListener::new();
    for _ in 0..10 {
        let stop = s.add_listener(sl.clone());
        stop.stop();
        stop.stop();
    }
    assert_eq!(s.listener_count(), 0);
}

// Upstream: github.com/grafana/dskit/services/basic_service_test.go::TestListenerCancellationTerminatedService
#[tokio::test]
async fn test_listener_cancellation_terminated_service() {
    let s = idle_service(None, None);
    stop_and_await_terminated(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
    let (sl, _) = ChanListener::new();
    for _ in 0..10 {
        let stop = s.add_listener(sl.clone());
        stop.stop();
        stop.stop();
    }
    assert_eq!(s.listener_count(), 0);
}

// Upstream: github.com/grafana/dskit/services/basic_service_test.go::TestListenerCancellationRunningService
#[tokio::test]
async fn test_listener_cancellation_running_service() {
    let s = idle_service(None, None);
    start_and_await_running(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
    let (sl, _) = ChanListener::new();
    let mut stop_fns = Vec::new();
    for _ in 0..10 {
        stop_fns.push(s.add_listener(sl.clone()));
    }
    assert_eq!(s.listener_count(), 10);
    for stop in stop_fns {
        stop.stop();
        stop.stop();
    }
    assert_eq!(s.listener_count(), 0);
    stop_and_await_terminated(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
}

// ============================================================================
// github.com/grafana/dskit/services/failure_watch_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/services/failure_watch_test.go::TestNilServiceFailureWatcher
#[tokio::test]
async fn test_nil_service_failure_watcher() {
    #[allow(clippy::unnecessary_literal_unwrap)]
    let panic_on_nil = || {
        let w: Option<&FailureWatcher> = None;
        assert!(w.is_none());

        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let svc: Arc<dyn Service> = idle_service(None, None);
            w.expect("nil FailureWatcher").watch_service(&svc);
        }));
        assert!(res.is_err());

        let m = ServiceManager::new(vec![idle_service(None, None)]).unwrap();
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            w.expect("nil FailureWatcher").watch_manager(&m);
        }));
        (m, res)
    };
    let (m, res) = panic_on_nil();
    assert!(res.is_err());
    stop_manager_and_await_stopped(ServiceContext::background(), &m)
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/services/failure_watch_test.go::TestServiceFailureWatcher
#[tokio::test]
async fn test_service_failure_watcher() {
    let w = FailureWatcher::new();
    let err = ServiceError::failure("this error doesn't end with dot");
    let err_c = err.clone();
    let failing: Arc<dyn Service> = basic_service(
        None,
        None,
        Some(Box::new(move |_| {
            let e = err_c.clone();
            Box::pin(async move { Err(e) })
        })),
    );
    w.watch_service(&failing);
    failing
        .start_async(ServiceContext::background())
        .await
        .unwrap();

    let e = w.recv().await.expect("watched failure");
    assert_eq!(e.cause, err);
}

// Upstream: github.com/grafana/dskit/services/failure_watch_test.go::TestServiceFailureWatcherClose
#[tokio::test]
async fn test_service_failure_watcher_close() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let m = ServiceManager::new(vec![s1, s2]).unwrap();

    let (tx3, rx3) = mpsc::unbounded_channel::<ServiceError>();
    let rx3 = Arc::new(tokio::sync::Mutex::new(rx3));
    let s3: Arc<dyn Service> = basic_service(
        None,
        Some(Box::new(move |ctx| {
            let rx_c = Arc::clone(&rx3);
            Box::pin(async move {
                let mut rx = rx_c.lock().await;
                tokio::select! {
                    _ = ctx.canceled() => Ok(()),
                    e = rx.recv() => match e {
                        Some(err) => Err(err),
                        None => Ok(()),
                    }
                }
            })
        })),
        None,
    );
    let (tx4, rx4) = mpsc::unbounded_channel::<ServiceError>();
    let rx4 = Arc::new(tokio::sync::Mutex::new(rx4));
    let s4: Arc<dyn Service> = basic_service(
        None,
        Some(Box::new(move |ctx| {
            let rx_c = Arc::clone(&rx4);
            Box::pin(async move {
                let mut rx = rx_c.lock().await;
                tokio::select! {
                    _ = ctx.canceled() => Ok(()),
                    e = rx.recv() => match e {
                        Some(err) => Err(err),
                        None => Ok(()),
                    }
                }
            })
        })),
        None,
    );

    start_manager_and_await_healthy(ServiceContext::background(), &m)
        .await
        .unwrap();
    start_and_await_running(ServiceContext::background(), s3.as_ref())
        .await
        .unwrap();
    start_and_await_running(ServiceContext::background(), s4.as_ref())
        .await
        .unwrap();

    let w = FailureWatcher::new();
    w.watch_manager(&m);
    w.watch_service(&s3);
    w.watch_service(&s4);

    let err3 = ServiceError::failure("service3 error");
    tx3.send(err3.clone()).unwrap();
    let got = w.recv().await.unwrap();
    assert_eq!(got.cause, err3);

    w.close();
    assert!(w.recv().await.is_none());

    let _ = tx4.send(ServiceError::failure("service4 error"));
    assert!(w.recv().await.is_none());

    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        w.watch_service(&s3);
    }))
    .is_err());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        w.watch_manager(&m);
    }))
    .is_err());

    w.close();
    stop_manager_and_await_stopped(ServiceContext::background(), &m)
        .await
        .unwrap();
    assert!(stop_and_await_terminated(ServiceContext::background(), s3.as_ref()).await.is_err());
    assert!(stop_and_await_terminated(ServiceContext::background(), s4.as_ref()).await.is_err());
}

// ============================================================================
// github.com/grafana/dskit/services/manager_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestNoServices
#[tokio::test]
async fn test_no_services() {
    assert!(ServiceManager::new(vec![]).is_err());
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestBasicManagerTransitions
#[tokio::test]
async fn test_basic_manager_transitions() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_doesnt_do_anything();
    let (gl, rx) = GatheringManagerListener::new();

    let m = ServiceManager::new(vec![s1, s2, s3]).unwrap();
    m.add_listener(gl);

    let states = m.services_by_state();
    assert_eq!(states.len(), 1);
    assert_eq!(states[&State::New].len(), 3);

    assert!(!m.is_healthy());
    assert!(!m.is_stopped());
    m.start_async(ServiceContext::background()).await.unwrap();
    m.await_healthy().await.unwrap();
    assert!(m.is_healthy());
    assert!(!m.is_stopped());

    let states = m.services_by_state();
    assert_eq!(states.len(), 1);
    assert_eq!(states[&State::Running].len(), 3);

    m.stop_async();
    m.await_stopped().await.unwrap();
    assert!(!m.is_healthy());
    assert!(m.is_stopped());

    let states = m.services_by_state();
    assert_eq!(states.len(), 1);
    assert_eq!(states[&State::Terminated].len(), 3);

    assert_eq!(drain_rx(rx).await, vec!["healthy", "stopped"]);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerRequiresServicesToBeInNewState
#[tokio::test]
async fn test_manager_requires_services_to_be_in_new_state() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_doesnt_do_anything();

    s1.start_async(ServiceContext::background()).await.unwrap();
    assert!(ServiceManager::new(vec![s1.clone(), s2, s3]).is_err());
    stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerReactsOnExternalStateChanges
#[tokio::test]
async fn test_manager_reacts_on_external_state_changes() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_doesnt_do_anything();
    let (gl, rx) = GatheringManagerListener::new();

    let m = ServiceManager::new(vec![s1.clone(), s2.clone(), s3.clone()]).unwrap();
    m.add_listener(gl);

    s1.start_async(ServiceContext::background()).await.unwrap();
    s2.start_async(ServiceContext::background()).await.unwrap();
    s3.stop_async();

    assert!(m.await_healthy().await.is_err());

    let states = m.services_by_state();
    assert_eq!(
        merge_states(&states, &[State::Stopping, State::Terminated]),
        svc_ptrs(std::slice::from_ref(&s3))
    );
    assert_eq!(
        merge_states(&states, &[State::New, State::Starting, State::Running]),
        svc_ptrs(&[s1.clone(), s2.clone()])
    );

    m.stop_async();
    m.await_stopped().await.unwrap();

    let states = m.services_by_state();
    assert_eq!(
        merge_states(&states, &[State::Terminated]),
        svc_ptrs(&[s1, s2, s3])
    );
    assert_eq!(drain_rx(rx).await, vec!["stopped"]);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerGoesToStoppedStateBeforeBeingHealthyOrEvenStarting
#[tokio::test]
async fn test_manager_goes_to_stopped_state_before_being_healthy_or_even_starting() {
    let s1 = service_that_doesnt_do_anything();
    let (gl, rx) = GatheringManagerListener::new();

    let m = ServiceManager::new(vec![s1.clone()]).unwrap();
    m.add_listener(gl);
    assert!(!m.is_stopped());

    s1.stop_async();
    assert!(m.await_healthy().await.is_err());
    m.await_stopped().await.unwrap();
    assert!(m.is_stopped());

    let states = m.services_by_state();
    assert_eq!(merge_states(&states, &[State::Terminated]), svc_ptrs(&[s1]));
    assert_eq!(drain_rx(rx).await, vec!["stopped"]);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerCannotStartIfAllServicesArentNew
#[tokio::test]
async fn test_manager_cannot_start_if_all_services_arent_new() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_doesnt_do_anything();
    let (gl, rx) = GatheringManagerListener::new();

    let m = ServiceManager::new(vec![s1.clone(), s2, s3]).unwrap();
    m.add_listener(gl);

    s1.start_async(ServiceContext::background()).await.unwrap();
    assert!(m.start_async(ServiceContext::background()).await.is_err());

    m.stop_async();
    m.await_stopped().await.unwrap();
    assert_eq!(drain_rx(rx).await, vec!["stopped"]);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerThatFailsToStart
#[tokio::test]
async fn test_manager_that_fails_to_start() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_fails_to_start();
    let (gl, rx) = GatheringManagerListener::new();

    let m = ServiceManager::new(vec![s1.clone(), s2.clone(), s3.clone()]).unwrap();
    m.add_listener(gl);

    let states = m.services_by_state();
    assert_eq!(
        merge_states(&states, &[State::New]),
        svc_ptrs(&[s1.clone(), s2.clone(), s3.clone()])
    );

    m.start_async(ServiceContext::background()).await.unwrap();
    let err = m.await_healthy().await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "not healthy, 0 terminated, 1 failed: [failed to start]"
    );

    let states = m.services_by_state();
    assert_eq!(merge_states(&states, &[State::Failed]), svc_ptrs(std::slice::from_ref(&s3)));
    assert_eq!(
        merge_states(&states, &[State::New, State::Starting, State::Running]),
        svc_ptrs(&[s1.clone(), s2.clone()])
    );

    m.stop_async();
    m.await_stopped().await.unwrap();

    let states = m.services_by_state();
    assert_eq!(merge_states(&states, &[State::Terminated]), svc_ptrs(&[s1, s2]));
    assert_eq!(merge_states(&states, &[State::Failed]), svc_ptrs(&[s3]));
    assert_eq!(drain_rx(rx).await, vec!["failed", "stopped"]);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerEntersStopStateEventually
#[tokio::test]
async fn test_manager_enters_stop_state_eventually() {
    let s1 = service_that_stops_on_its_own_after_timeout(Duration::from_millis(80));
    let s2 = service_that_stops_on_its_own_after_timeout(Duration::from_millis(120));
    let (gl, rx) = GatheringManagerListener::new();

    let m = ServiceManager::new(vec![s1, s2]).unwrap();
    m.add_listener(gl);

    m.start_async(ServiceContext::background()).await.unwrap();
    m.await_healthy().await.unwrap();
    m.await_stopped().await.unwrap();

    assert_eq!(drain_rx(rx).await, vec!["healthy", "stopped"]);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerAwaitWithContextCancellation
#[tokio::test]
async fn test_manager_await_with_context_cancellation() {
    let s1 = service_that_stops_on_its_own_after_timeout(Duration::from_millis(100));
    let s2 = service_that_stops_on_its_own_after_timeout(Duration::from_millis(150));
    let (gl, rx) = GatheringManagerListener::new();

    let m = ServiceManager::new(vec![s1, s2]).unwrap();
    m.add_listener(gl);

    let stopped_ctx = ServiceContext::background();
    stopped_ctx.cancel();
    assert_eq!(
        m.await_healthy_ctx(stopped_ctx).await.unwrap_err(),
        ServiceError::Canceled
    );

    m.start_async(ServiceContext::background()).await.unwrap();
    let short_ctx = ServiceContext::background().with_timeout(Duration::from_millis(10));
    assert_eq!(
        m.await_stopped_ctx(short_ctx).await.unwrap_err(),
        ServiceError::DeadlineExceeded
    );
    m.await_stopped().await.unwrap();

    assert_eq!(drain_rx(rx).await, vec!["healthy", "stopped"]);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerListenerCancellationUnstartedManager
#[tokio::test]
async fn test_manager_listener_cancellation_unstarted_manager() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_doesnt_do_anything();
    let m = ServiceManager::new(vec![s1, s2, s3]).unwrap();
    let (gl, _) = GatheringManagerListener::new();
    for _ in 0..10 {
        let stop = m.add_listener(gl.clone());
        stop.stop();
        stop.stop();
    }
    assert_eq!(m.listener_count(), 0);
    stop_manager_and_await_stopped(ServiceContext::background(), &m)
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerListenerCancellationStoppedManager
#[tokio::test]
async fn test_manager_listener_cancellation_stopped_manager() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_doesnt_do_anything();
    let m = ServiceManager::new(vec![s1, s2, s3]).unwrap();

    start_manager_and_await_healthy(ServiceContext::background(), &m)
        .await
        .unwrap();
    stop_manager_and_await_stopped(ServiceContext::background(), &m)
        .await
        .unwrap();

    let (gl, _) = GatheringManagerListener::new();
    for _ in 0..10 {
        let stop = m.add_listener(gl.clone());
        stop.stop();
        stop.stop();
    }
    assert_eq!(m.listener_count(), 0);
}

// Upstream: github.com/grafana/dskit/services/manager_test.go::TestManagerListenerCancellationHealthyManager
#[tokio::test]
async fn test_manager_listener_cancellation_healthy_manager() {
    let s1 = service_that_doesnt_do_anything();
    let s2 = service_that_doesnt_do_anything();
    let s3 = service_that_doesnt_do_anything();
    let m = ServiceManager::new(vec![s1, s2, s3]).unwrap();
    start_manager_and_await_healthy(ServiceContext::background(), &m)
        .await
        .unwrap();

    let (gl, _) = GatheringManagerListener::new();
    let mut stop_fns = Vec::new();
    for _ in 0..10 {
        stop_fns.push(m.add_listener(gl.clone()));
    }
    assert_eq!(m.listener_count(), 10);
    for stop in stop_fns {
        stop.stop();
        stop.stop();
    }
    assert_eq!(m.listener_count(), 0);

    stop_manager_and_await_stopped(ServiceContext::background(), &m)
        .await
        .unwrap();
}

// ============================================================================
// github.com/grafana/dskit/services/services_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/services/services_test.go::TestIdleService
#[tokio::test]
async fn test_idle_service() {
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let st_c = Arc::clone(&started);
    let sp_c = Arc::clone(&stopped);
    let s = idle_service(
        Some(Box::new(move |_| {
            let st = Arc::clone(&st_c);
            Box::pin(async move {
                st.store(true, Ordering::SeqCst);
                Ok(())
            })
        })),
        Some(Box::new(move |_| {
            let sp = Arc::clone(&sp_c);
            Box::pin(async move {
                sp.store(true, Ordering::SeqCst);
                Ok(())
            })
        })),
    );

    assert!(!started.load(Ordering::SeqCst));
    assert_eq!(s.state(), State::New);
    s.start_async(ServiceContext::background()).await.unwrap();
    assert!(s.start_async(ServiceContext::background()).await.is_err());
    s.await_running().await.unwrap();
    assert!(started.load(Ordering::SeqCst));
    assert!(!stopped.load(Ordering::SeqCst));
    assert_eq!(s.state(), State::Running);
    s.stop_async();
    s.await_terminated().await.unwrap();
    assert!(stopped.load(Ordering::SeqCst));
}

// Upstream: github.com/grafana/dskit/services/services_test.go::TestTimerService
#[tokio::test]
async fn test_timer_service() {
    let iterations = Arc::new(AtomicU64::new(0));
    let it_c = Arc::clone(&iterations);
    let s = timer_service(
        Duration::from_millis(25),
        None,
        move |_| {
            let it = Arc::clone(&it_c);
            Box::pin(async move {
                it.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        },
        None,
    );

    assert_eq!(s.state(), State::New);
    s.start_async(ServiceContext::background()).await.unwrap();
    assert!(s.start_async(ServiceContext::background()).await.is_err());
    s.await_running().await.unwrap();
    assert_eq!(s.state(), State::Running);

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(iterations.load(Ordering::SeqCst) > 0);

    s.stop_async();
    s.await_terminated().await.unwrap();
    assert_eq!(s.state(), State::Terminated);
}

// Upstream: github.com/grafana/dskit/services/services_test.go::TestHelperFunctionsNoError
#[tokio::test]
async fn test_helper_functions_no_error() {
    let s = idle_service(None, None);
    start_and_await_running(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/services/services_test.go::TestHelperFunctionsStartError
#[tokio::test]
async fn test_helper_functions_start_error() {
    let e = ServiceError::failure("some error");
    let e_c = e.clone();
    let s = idle_service(
        Some(Box::new(move |_| {
            let err = e_c.clone();
            Box::pin(async move { Err(err) })
        })),
        None,
    );
    assert_eq!(
        start_and_await_running(ServiceContext::background(), s.as_ref())
            .await
            .unwrap_err(),
        e
    );
    assert_eq!(
        stop_and_await_terminated(ServiceContext::background(), s.as_ref())
            .await
            .unwrap_err(),
        e
    );
}

// Upstream: github.com/grafana/dskit/services/services_test.go::TestHelperFunctionsStartTooSlow
#[tokio::test]
async fn test_helper_functions_start_too_slow() {
    let s = idle_service(
        Some(Box::new(|_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(250)).await;
                Ok(())
            })
        })),
        None,
    );

    let ctx = ServiceContext::background();
    let ctx_c = ctx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        ctx_c.cancel();
    });

    assert_eq!(
        start_and_await_running(ctx, s.as_ref()).await.unwrap_err(),
        ServiceError::Canceled
    );
    stop_and_await_terminated(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/services/services_test.go::TestHelperFunctionsStopError
#[tokio::test]
async fn test_helper_functions_stop_error() {
    let e = ServiceError::failure("some error");
    let e_c = e.clone();
    let s = idle_service(
        None,
        Some(Box::new(move |_| {
            let err = e_c.clone();
            Box::pin(async move { Err(err) })
        })),
    );
    start_and_await_running(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
    assert_eq!(
        stop_and_await_terminated(ServiceContext::background(), s.as_ref())
            .await
            .unwrap_err(),
        e
    );
}

// Upstream: github.com/grafana/dskit/services/services_test.go::TestHelperFunctionsStopTooSlow
#[tokio::test]
async fn test_helper_functions_stop_too_slow() {
    let s = idle_service(
        None,
        Some(Box::new(|_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(250)).await;
                Ok(())
            })
        })),
    );
    start_and_await_running(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();

    let ctx = ServiceContext::background().with_timeout(Duration::from_millis(30));
    assert_eq!(
        stop_and_await_terminated(ctx, s.as_ref()).await.unwrap_err(),
        ServiceError::DeadlineExceeded
    );
    stop_and_await_terminated(ServiceContext::background(), s.as_ref())
        .await
        .unwrap();
}

// ============================================================================
// github.com/grafana/dskit/concurrency/*_test.go
// ============================================================================

#[derive(Debug)]
struct StrError(&'static str);

impl fmt::Display for StrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl StdError for StrError {}

fn err_box(msg: &'static str) -> BoxError {
    Arc::new(StrError(msg))
}

// Upstream: github.com/grafana/dskit/concurrency/limited_concurrency_singleflight_test.go::TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ConcurrencyLimit
#[tokio::test]
async fn test_limited_concurrency_single_flight_for_each_not_in_flight_concurrency_limit() {
    let ctx = ServiceContext::background();
    let sf = Arc::new(LimitedConcurrencySingleFlight::new(10));
    let workers_wait = ServiceContext::background();
    let started = Arc::new(AtomicUsize::new(0));
    let all_started = Arc::new(Notify::new());

    let mut handles = Vec::new();
    for pair in [["0", "1"], ["2", "3"], ["4", "5"], ["6", "7"], ["8", "9"]] {
        let sf_c = Arc::clone(&sf);
        let ctx_c = ctx.clone();
        let ww = workers_wait.clone();
        let st = Arc::clone(&started);
        let notify = Arc::clone(&all_started);
        let tokens: Vec<String> = pair.iter().map(|s| (*s).to_string()).collect();
        handles.push(tokio::spawn(async move {
            sf_c.for_each_not_in_flight(&ctx_c, &tokens, move |_, _| {
                let ww_i = ww.clone();
                let st_i = Arc::clone(&st);
                let n_i = Arc::clone(&notify);
                async move {
                    if st_i.fetch_add(1, Ordering::AcqRel) + 1 == 10 {
                        n_i.notify_waiters();
                    }
                    ww_i.canceled().await;
                    Ok(())
                }
            })
            .await
        }));
    }

    while started.load(Ordering::Acquire) < 10 {
        all_started.notified().await;
    }

    let extra_invoked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ei_c = Arc::clone(&extra_invoked);
    let sf_c = Arc::clone(&sf);
    let ctx_c = ctx.clone();
    let extra_handle = tokio::spawn(async move {
        sf_c.for_each_not_in_flight(&ctx_c, &["10".to_string()], move |_, _| {
            let ei = Arc::clone(&ei_c);
            async move {
                ei.store(true, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !extra_invoked.load(Ordering::SeqCst),
        "extra worker should not have been invoked while concurrency limit maxed out"
    );

    workers_wait.cancel();
    for h in handles {
        h.await.unwrap().unwrap();
    }
    extra_handle.await.unwrap().unwrap();
    sf.wait().await;
}

// Upstream: github.com/grafana/dskit/concurrency/limited_concurrency_singleflight_test.go::TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ReturnsWhenAllTokensAreInflight
#[tokio::test]
async fn test_limited_concurrency_single_flight_for_each_not_in_flight_returns_when_all_tokens_are_inflight() {
    let ctx = ServiceContext::background();
    let sf = Arc::new(LimitedConcurrencySingleFlight::new(10));
    let workers_wait = ServiceContext::background();
    let started = Arc::new(Notify::new());
    let started_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let sf_c = Arc::clone(&sf);
    let ctx_c = ctx.clone();
    let ww = workers_wait.clone();
    let st_n = Arc::clone(&started);
    let st_f = Arc::clone(&started_flag);
    let h = tokio::spawn(async move {
        sf_c.for_each_not_in_flight(&ctx_c, &["t".to_string()], move |_, _| {
            let ww_i = ww.clone();
            let n = Arc::clone(&st_n);
            let f = Arc::clone(&st_f);
            async move {
                f.store(true, Ordering::SeqCst);
                n.notify_waiters();
                ww_i.canceled().await;
                Ok(())
            }
        })
        .await
    });

    loop {
        let notified = started.notified();
        if started_flag.load(Ordering::Acquire) {
            break;
        }
        notified.await;
    }

    let dup_invoked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let di_c = Arc::clone(&dup_invoked);
    sf.for_each_not_in_flight(&ctx, &["t".to_string()], move |_, _| {
        let di = Arc::clone(&di_c);
        async move {
            di.store(true, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();
    assert!(!dup_invoked.load(Ordering::SeqCst));

    workers_wait.cancel();
    h.await.unwrap().unwrap();
    sf.wait().await;
}

// Upstream: github.com/grafana/dskit/concurrency/limited_concurrency_singleflight_test.go::TestLimitedConcurrencySingleFlight_ForEachNotInFlight_CallsOnlyNotInFlightTokens
#[tokio::test]
async fn test_limited_concurrency_single_flight_for_each_not_in_flight_calls_only_not_in_flight_tokens() {
    let ctx = ServiceContext::background();
    let sf = Arc::new(LimitedConcurrencySingleFlight::new(10));
    let workers_wait = ServiceContext::background();
    let started = Arc::new(Notify::new());
    let started_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let sf_c = Arc::clone(&sf);
    let ctx_c = ctx.clone();
    let ww = workers_wait.clone();
    let st_n = Arc::clone(&started);
    let st_f = Arc::clone(&started_flag);
    let h = tokio::spawn(async move {
        sf_c.for_each_not_in_flight(&ctx_c, &["a".to_string()], move |_, _| {
            let ww_i = ww.clone();
            let n = Arc::clone(&st_n);
            let f = Arc::clone(&st_f);
            async move {
                f.store(true, Ordering::SeqCst);
                n.notify_waiters();
                ww_i.canceled().await;
                Ok(())
            }
        })
        .await
    });

    loop {
        let notified = started.notified();
        if started_flag.load(Ordering::Acquire) {
            break;
        }
        notified.await;
    }

    let invocations = Arc::new(AtomicUsize::new(0));
    let inv_c = Arc::clone(&invocations);
    sf.for_each_not_in_flight(&ctx, &["a".to_string(), "b".to_string()], move |_, s| {
        let inv = Arc::clone(&inv_c);
        async move {
            assert_eq!(s, "b");
            inv.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(invocations.load(Ordering::SeqCst), 1);

    workers_wait.cancel();
    h.await.unwrap().unwrap();
    sf.wait().await;
}

// Upstream: github.com/grafana/dskit/concurrency/limited_concurrency_singleflight_test.go::TestLimitedConcurrencySingleFlight_ForEachNotInFlight_ReturnsWhenTokensAreEmpty
#[tokio::test]
async fn test_limited_concurrency_single_flight_for_each_not_in_flight_returns_when_tokens_are_empty() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let inv_c = Arc::clone(&invocations);
    let sf = LimitedConcurrencySingleFlight::new(10);
    sf.for_each_not_in_flight(&ServiceContext::background(), &[], move |_, _| {
        let inv = Arc::clone(&inv_c);
        async move {
            inv.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
}

// Upstream: github.com/grafana/dskit/concurrency/limited_concurrency_singleflight_test.go::TestLimitedConcurrencySingleFlight_ForEachNotInFlight_CancelledContext
#[tokio::test]
async fn test_limited_concurrency_single_flight_for_each_not_in_flight_cancelled_context() {
    let tokens = vec!["t1".to_string(), "t2".to_string()];
    let ctx = ServiceContext::background();
    let sf = Arc::new(LimitedConcurrencySingleFlight::new(1));
    let workers_wait = ServiceContext::background();
    let started = Arc::new(Notify::new());
    let started_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let invocations = Arc::new(AtomicI32::new(0));

    let sf_c = Arc::clone(&sf);
    let ctx_c = ctx.clone();
    let tokens_c = tokens.clone();
    let ww = workers_wait.clone();
    let st_n = Arc::clone(&started);
    let st_f = Arc::clone(&started_flag);
    let inv_c = Arc::clone(&invocations);
    let h = tokio::spawn(async move {
        sf_c.for_each_not_in_flight(&ctx_c, &tokens_c, move |c, _| {
            let ww_i = ww.clone();
            let n = Arc::clone(&st_n);
            let f = Arc::clone(&st_f);
            let inv = Arc::clone(&inv_c);
            async move {
                assert!(!c.is_canceled(), "worker called with cancelled context");
                inv.fetch_add(1, Ordering::SeqCst);
                f.store(true, Ordering::SeqCst);
                n.notify_waiters();
                ww_i.canceled().await;
                Ok(())
            }
        })
        .await
    });

    loop {
        let notified = started.notified();
        if started_flag.load(Ordering::Acquire) {
            break;
        }
        notified.await;
    }

    ctx.cancel();
    workers_wait.cancel();
    h.await.unwrap().unwrap();
    sf.wait().await;
    assert_eq!(invocations.load(Ordering::SeqCst), 1);

    invocations.store(0, Ordering::SeqCst);
    let ww2 = ServiceContext::background();
    ww2.cancel();
    let inv_c2 = Arc::clone(&invocations);
    sf.for_each_not_in_flight(&ServiceContext::background(), &tokens, move |c, _| {
        let inv = Arc::clone(&inv_c2);
        let ww_i = ww2.clone();
        async move {
            assert!(!c.is_canceled());
            inv.fetch_add(1, Ordering::SeqCst);
            ww_i.canceled().await;
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachUser
#[tokio::test]
async fn test_for_each_user() {
    let processed = Arc::new(Mutex::new(BTreeSet::new()));
    let input: Vec<String> = ["a", "b", "c"].into_iter().map(str::to_string).collect();
    let proc_c = Arc::clone(&processed);
    for_each_user(&ServiceContext::background(), &input, 2, move |_, u| {
        let p = Arc::clone(&proc_c);
        async move {
            p.lock().insert(u);
            Ok(())
        }
    })
    .await
    .unwrap();
    let expected: BTreeSet<String> = input.into_iter().collect();
    assert_eq!(&*processed.lock(), &expected);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachUser_ShouldContinueOnErrorButReturnIt
#[tokio::test]
async fn test_for_each_user_should_continue_on_error_but_return_it() {
    let processed = Arc::new(AtomicI32::new(0));
    let input: Vec<String> = ["a", "b", "c"].into_iter().map(str::to_string).collect();
    let proc_c = Arc::clone(&processed);
    let err = for_each_user(&ServiceContext::background(), &input, 2, move |ctx, _| {
        let p = Arc::clone(&proc_c);
        async move {
            if p.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return Err(err_box("the first request is failing"));
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(40)) => {
                    p.fetch_add(1, Ordering::SeqCst);
                }
                _ = ctx.canceled() => {
                    return Err(Arc::new(ctx.err().unwrap_or(ServiceError::Canceled)) as BoxError);
                }
            }
            Ok(())
        }
    })
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "the first request is failing");
    assert_eq!(processed.load(Ordering::SeqCst), input.len() as i32);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachUser_ShouldReturnImmediatelyOnNoUsersProvided
#[tokio::test]
async fn test_for_each_user_should_return_immediately_on_no_users_provided() {
    for_each_user(&ServiceContext::background(), &[], 2, |_, _| async { Ok(()) })
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachJob
#[tokio::test]
async fn test_for_each_job() {
    let jobs = ["a", "b", "c"];
    let processed = Arc::new(Mutex::new(vec![String::new(); jobs.len()]));
    let proc_c = Arc::clone(&processed);
    for_each_job(&ServiceContext::background(), jobs.len(), 2, move |_, idx| {
        let p = Arc::clone(&proc_c);
        async move {
            p.lock()[idx] = ["a", "b", "c"][idx].to_string();
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(&*processed.lock(), &["a", "b", "c"]);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachJob_ShouldBreakOnFirstError_ContextCancellationHandled
#[tokio::test]
async fn test_for_each_job_should_break_on_first_error_context_cancellation_handled() {
    let processed = Arc::new(AtomicI32::new(0));
    let proc_c = Arc::clone(&processed);
    let err = for_each_job(&ServiceContext::background(), 3, 2, move |ctx, _| {
        let p = Arc::clone(&proc_c);
        async move {
            if p.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return Err(err_box("the first request is failing"));
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    p.fetch_add(1, Ordering::SeqCst);
                }
                _ = ctx.canceled() => {
                    return Err(Arc::new(ctx.err().unwrap_or(ServiceError::Canceled)) as BoxError);
                }
            }
            Ok(())
        }
    })
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "the first request is failing");
    assert_eq!(processed.load(Ordering::SeqCst), 1);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachJob_ShouldBreakOnFirstError_ContextCancellationUnhandled
#[tokio::test]
async fn test_for_each_job_should_break_on_first_error_context_cancellation_unhandled() {
    let processed = Arc::new(AtomicI32::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let two_started = Arc::new(Notify::new());
    let proc_c = Arc::clone(&processed);
    let st_c = Arc::clone(&started);
    let notify_c = Arc::clone(&two_started);
    let err = for_each_job(&ServiceContext::background(), 3, 2, move |ctx, _| {
        let p = Arc::clone(&proc_c);
        let st = Arc::clone(&st_c);
        let n = Arc::clone(&notify_c);
        async move {
            if st.fetch_add(1, Ordering::SeqCst) + 1 >= 2 {
                n.notify_waiters();
            }
            if p.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                while st.load(Ordering::SeqCst) < 2 {
                    n.notified().await;
                }
                return Err(err_box("the first request is failing"));
            }
            ctx.canceled().await;
            p.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "the first request is failing");
    assert_eq!(processed.load(Ordering::SeqCst), 2);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachJob_ShouldReturnImmediatelyOnNoJobsProvided
#[tokio::test]
async fn test_for_each_job_should_return_immediately_on_no_jobs_provided() {
    let processed = Arc::new(AtomicI32::new(0));
    let p_c = Arc::clone(&processed);
    for_each_job(&ServiceContext::background(), 0, 2, move |_, _| {
        let p = Arc::clone(&p_c);
        async move {
            p.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(processed.load(Ordering::SeqCst), 0);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachJob_ShouldCancelContextPassedToCallbackOnceDone
#[tokio::test]
async fn test_for_each_job_should_cancel_context_passed_to_callback_once_done() {
    for jobs in 1..=3 {
        for concurrency in 1..=jobs {
            let contexts = Arc::new(Mutex::new(Vec::new()));
            let ctxs_c = Arc::clone(&contexts);
            for_each_job(&ServiceContext::background(), jobs, concurrency, move |ctx, _| {
                let cs = Arc::clone(&ctxs_c);
                async move {
                    assert!(ctx.err().is_none());
                    cs.lock().push(ctx);
                    Ok(())
                }
            })
            .await
            .unwrap();
            let guard = contexts.lock();
            assert_eq!(guard.len(), jobs);
            for c in guard.iter() {
                assert_eq!(c.err(), Some(ServiceError::Canceled));
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEach
#[tokio::test]
async fn test_for_each() {
    let jobs = create_jobs_from_strings(&["a", "b", "c"]);
    let processed = Arc::new(Mutex::new(BTreeSet::new()));
    let proc_c = Arc::clone(&processed);
    for_each(&ServiceContext::background(), &jobs, 2, move |_, job| {
        let p = Arc::clone(&proc_c);
        async move {
            p.lock().insert(job);
            Ok(())
        }
    })
    .await
    .unwrap();
    let expected: BTreeSet<String> = jobs.into_iter().collect();
    assert_eq!(&*processed.lock(), &expected);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEach_ShouldBreakOnFirstError_ContextCancellationHandled
#[tokio::test]
async fn test_for_each_should_break_on_first_error_context_cancellation_handled() {
    let jobs = create_jobs_from_strings(&["a", "b", "c"]);
    let processed = Arc::new(AtomicI32::new(0));
    let proc_c = Arc::clone(&processed);
    let err = for_each(&ServiceContext::background(), &jobs, 2, move |ctx, _| {
        let p = Arc::clone(&proc_c);
        async move {
            if p.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return Err(err_box("the first request is failing"));
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(200)) => {
                    p.fetch_add(1, Ordering::SeqCst);
                }
                _ = ctx.canceled() => {
                    return Err(Arc::new(ctx.err().unwrap_or(ServiceError::Canceled)) as BoxError);
                }
            }
            Ok(())
        }
    })
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "the first request is failing");
    assert_eq!(processed.load(Ordering::SeqCst), 1);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEach_ShouldBreakOnFirstError_ContextCancellationUnhandled
#[tokio::test]
async fn test_for_each_should_break_on_first_error_context_cancellation_unhandled() {
    let jobs = create_jobs_from_strings(&["a", "b", "c"]);
    let processed = Arc::new(AtomicI32::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let two_started = Arc::new(Notify::new());
    let proc_c = Arc::clone(&processed);
    let st_c = Arc::clone(&started);
    let notify_c = Arc::clone(&two_started);
    let err = for_each(&ServiceContext::background(), &jobs, 2, move |ctx, _| {
        let p = Arc::clone(&proc_c);
        let st = Arc::clone(&st_c);
        let n = Arc::clone(&notify_c);
        async move {
            if st.fetch_add(1, Ordering::SeqCst) + 1 >= 2 {
                n.notify_waiters();
            }
            if p.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                while st.load(Ordering::SeqCst) < 2 {
                    n.notified().await;
                }
                return Err(err_box("the first request is failing"));
            }
            ctx.canceled().await;
            p.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    })
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "the first request is failing");
    assert_eq!(processed.load(Ordering::SeqCst), 2);
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEach_ShouldReturnImmediatelyOnNoJobsProvided
#[tokio::test]
async fn test_for_each_should_return_immediately_on_no_jobs_provided() {
    let jobs: Vec<String> = vec![];
    for_each(&ServiceContext::background(), &jobs, 2, |_, _| async { Ok(()) })
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/concurrency/runner_test.go::TestForEachJobMergeResults
#[tokio::test]
async fn test_for_each_job_merge_results() {
    // 1. no jobs
    let empty: Vec<Vec<String>> = vec![];
    let res: Vec<String> =
        for_each_job_merge_results(&ServiceContext::background(), &empty, 0, |_, j| async move {
            Ok(j)
        })
        .await
        .unwrap();
    assert!(res.is_empty());

    // 2. single job
    let jobs1 = vec![vec!["1".to_string(), "2".to_string()]];
    let mut res =
        for_each_job_merge_results(&ServiceContext::background(), &jobs1, 0, |_, j| async move {
            Ok(j)
        })
        .await
        .unwrap();
    res.sort();
    assert_eq!(res, vec!["1", "2"]);

    // 3. multiple jobs
    let jobs3 = vec![
        vec!["1".to_string(), "2".to_string()],
        vec!["3".to_string()],
        vec!["4".to_string(), "5".to_string()],
    ];
    let mut res =
        for_each_job_merge_results(&ServiceContext::background(), &jobs3, 0, |_, j| async move {
            Ok(j)
        })
        .await
        .unwrap();
    res.sort();
    assert_eq!(res, vec!["1", "2", "3", "4", "5"]);

    // 4. concurrent execution
    let jobs5: Vec<Vec<String>> = (1..=5).map(|i| vec![i.to_string()]).collect();
    let start = Instant::now();
    let mut res =
        for_each_job_merge_results(&ServiceContext::background(), &jobs5, 0, |_, j| async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            Ok(j)
        })
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(200));
    res.sort();
    assert_eq!(res, vec!["1", "2", "3", "4", "5"]);

    // 5. first error cancels context
    let jobs_err: Vec<Vec<String>> = (1..=3).map(|i| vec![i.to_string()]).collect();
    let start = Instant::now();
    let err = for_each_job_merge_results(
        &ServiceContext::background(),
        &jobs_err,
        0,
        |ctx, j| async move {
            if j.len() == 1 && j[0] == "3" {
                return Err(err_box("mocked error"));
            }
            tokio::select! {
                _ = ctx.canceled() => {}
                _ = tokio::time::sleep(Duration::from_secs(2)) => panic!("context not canceled"),
            }
            Ok(j)
        },
    )
    .await
    .unwrap_err();
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(err.to_string(), "mocked error");

    // 6. simultaneous errors
    let started = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let st_c = Arc::clone(&started);
    let n_c = Arc::clone(&notify);
    let r_c = Arc::clone(&release);
    let rf_c = Arc::clone(&released);
    tokio::spawn(async move {
        while st_c.load(Ordering::Acquire) < 3 {
            n_c.notified().await;
        }
        rf_c.store(true, Ordering::Release);
        r_c.notify_waiters();
    });

    let err = for_each_job_merge_results(
        &ServiceContext::background(),
        &jobs_err,
        0,
        move |_, _| {
            let st = Arc::clone(&started);
            let n = Arc::clone(&notify);
            let r = Arc::clone(&release);
            let rf = Arc::clone(&released);
            async move {
                if st.fetch_add(1, Ordering::AcqRel) + 1 == 3 {
                    n.notify_waiters();
                }
                while !rf.load(Ordering::Acquire) {
                    r.notified().await;
                }
                Err::<Vec<String>, BoxError>(err_box("mocked error"))
            }
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), "mocked error");
}

// Upstream: github.com/grafana/dskit/concurrency/worker_test.go::TestReusableGoroutinesPool
#[tokio::test]
async fn test_reusable_goroutines_pool() {
    use std::sync::Condvar;
    let w = ReusableGoroutinesPool::new(2);
    assert_eq!(w.active_worker_count(), 0);

    let gate = Arc::new((std::sync::Mutex::new(false), Condvar::new()));
    for expected in [1, 2, 3] {
        let g = Arc::clone(&gate);
        w.go(move || {
            let (lock, cv) = &*g;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cv.wait(open).unwrap();
            }
        });
        assert_eq!(w.active_worker_count(), expected);
    }

    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }

    for _ in 0..1000 {
        if w.active_worker_count() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(w.active_worker_count(), 2);

    w.close();
    for _ in 0..1000 {
        if w.active_worker_count() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!(
        "expected 0 active workers after close, got {}",
        w.active_worker_count()
    );
}

// Upstream: github.com/grafana/dskit/concurrency/worker_test.go::TestReusableGoroutinesPool_Race
#[tokio::test]
async fn test_reusable_goroutines_pool_race() {
    let w = Arc::new(ReusableGoroutinesPool::new(2));
    let run_count = Arc::new(AtomicI32::new(0));
    const MAX_MSG: i32 = 10;

    let w_c = Arc::clone(&w);
    let rc_c = Arc::clone(&run_count);
    let handle = tokio::spawn(async move {
        for _ in 0..MAX_MSG {
            let rc = Arc::clone(&rc_c);
            w_c.go(move || {
                rc.fetch_add(1, Ordering::SeqCst);
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    w.close();
    handle.await.unwrap();

    for _ in 0..500 {
        if run_count.load(Ordering::SeqCst) == MAX_MSG {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(run_count.load(Ordering::SeqCst), MAX_MSG);
}
