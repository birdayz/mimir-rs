//! Ported from github.com/grafana/dskit/{modules,limiter,log}/*_test.go (39 tests total).

use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mimir_cmd::dskit_log::{
    drop_unsafe_chars_wrapper, escape_unsafe_chars_wrapper, format_logfmt_sanitized,
    format_logfmt_sanitized_escaped, BufferedLogger, LazySprintf, UnsafeInput,
};
use mimir_cmd::modules::{
    new_module_service, user_invisible_module, user_invisible_targetable_module, ModuleInitFn,
    ModuleManager,
};
use mimir_common::limiter::{RateLimiter, RateLimiterStrategy, RATE_INF};
use mimir_common::logging::{
    encode_json_record, format_json_line, format_logfmt_line,
    LevelFilterLogger, LogLevel as CommonLogLevel, LogValue, Logger,
    TokenBucketRateLimitedLogger,
};
use mimir_common::services::{
    basic_service, idle_service, start_manager_and_await_healthy, stop_and_await_terminated,
    stop_manager_and_await_stopped, Service, ServiceContext, ServiceError, ServiceManager, State,
};
use mimir_config::yaml_v3_emit::marshal_struct;
use mimir_config::LogLevel as ConfigLogLevel;
use parking_lot::Mutex;
use prometheus::Registry;

// =============================================================================================
// Thread-local allocation counter for TestZeroAllocsOnCleanStringPath
// =============================================================================================

struct CountingAlloc;

thread_local! {
    static COUNT_ALLOCS: Cell<bool> = const { Cell::new(false) };
    static ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNT_ALLOCS.with(|c| {
            if c.get() {
                ALLOC_COUNT.with(|n| n.set(n.get() + 1));
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn count_allocs<R>(f: impl FnOnce() -> R) -> (R, usize) {
    ALLOC_COUNT.with(|n| n.set(0));
    COUNT_ALLOCS.with(|c| c.set(true));
    let res = f();
    COUNT_ALLOCS.with(|c| c.set(false));
    let count = ALLOC_COUNT.with(|n| n.get());
    (res, count)
}

// =============================================================================================
// Shared test helpers
// =============================================================================================

#[derive(Default)]
struct NopLogger;

impl Logger for NopLogger {
    fn log(&self, _level: CommonLogLevel, _kvs: &[(&str, &str)]) {}
}

struct CaptureBuf(Arc<Mutex<Vec<u8>>>);

impl Write for CaptureBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct CounterLogfmtLogger {
    buf: Arc<Mutex<Vec<u8>>>,
    count: AtomicUsize,
}

impl CounterLogfmtLogger {
    fn new(buf: Arc<Mutex<Vec<u8>>>) -> Arc<Self> {
        Arc::new(Self {
            buf,
            count: AtomicUsize::new(0),
        })
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn output(&self) -> String {
        String::from_utf8(self.buf.lock().clone()).unwrap()
    }
}

impl Logger for CounterLogfmtLogger {
    fn log(&self, level: CommonLogLevel, kvs: &[(&str, &str)]) {
        let mut pairs = Vec::with_capacity(kvs.len() + 1);
        pairs.push(("level", level.as_str()));
        pairs.extend_from_slice(kvs);
        let line = format_logfmt_line(&pairs);
        self.buf.lock().extend_from_slice(line.as_bytes());
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

struct WithFieldsLogger {
    inner: Arc<dyn Logger>,
    prefix_kvs: Vec<(String, String)>,
}

impl Logger for WithFieldsLogger {
    fn log(&self, level: CommonLogLevel, kvs: &[(&str, &str)]) {
        let mut merged: Vec<(&str, &str)> = self
            .prefix_kvs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        merged.extend_from_slice(kvs);
        self.inner.log(level, &merged);
    }

    fn probe_level(&self) -> Option<CommonLogLevel> {
        self.inner.probe_level()
    }
}

fn mock_init_ok() -> ModuleInitFn {
    Arc::new(|| Ok(Some(idle_service(None, None) as Arc<dyn Service>)))
}

fn mock_init_fail() -> ModuleInitFn {
    Arc::new(|| Err(anyhow::anyhow!("Error")))
}

// =============================================================================================
// dskit/modules (16 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestDependencies
#[test]
fn test_dependencies() {
    let mut mm = ModuleManager::new(Arc::new(NopLogger));
    mm.register_module("serviceA", Some(mock_init_ok()), &[]);
    mm.register_module("serviceB", Some(mock_init_ok()), &[]);
    mm.register_module("serviceC", Some(mock_init_ok()), &[]);
    mm.register_module("serviceD", Some(mock_init_fail()), &[]);

    mm.add_dependency("serviceB", &["serviceA"]).unwrap();
    mm.add_dependency("serviceC", &["serviceB"]).unwrap();
    assert_eq!(mm.get_module("serviceB").unwrap().deps, vec!["serviceA"]);

    let inv_deps = mm.inverse_dependencies_for_module("serviceA");
    assert_eq!(inv_deps, vec!["serviceB", "serviceC"]);

    // Unknown module
    let err = mm.init_module_services(&["service_unknown"]).err().unwrap();
    assert!(err.contains("unrecognised module name: service_unknown"), "{err}");

    // Init failure
    assert!(mm.init_module_services(&["serviceD"]).is_err());

    // Loading several modules
    let res = mm.init_module_services(&["serviceA", "serviceB"]).unwrap();
    assert_eq!(res.services.len(), 2);
    assert_eq!(res.stop_dependencies_for_module("serviceA"), vec!["serviceB"]);
    assert!(res.stop_dependencies_for_module("serviceB").is_empty());

    let res = mm.init_module_services(&["serviceC"]).unwrap();
    assert_eq!(res.services.len(), 3);
    assert_eq!(
        res.stop_dependencies_for_module("serviceA"),
        vec!["serviceB", "serviceC"]
    );
    assert_eq!(res.stop_dependencies_for_module("serviceB"), vec!["serviceC"]);
    assert!(res.stop_dependencies_for_module("serviceC").is_empty());

    // Second init creates distinct service instances
    let res2 = mm.init_module_services(&["serviceC"]).unwrap();
    assert_eq!(res2.services.len(), 3);
    assert!(!Arc::ptr_eq(&res.services["serviceA"], &res2.services["serviceA"]));
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestManaged_AddDependency_ShouldErrorOnCircularDependencies
#[test]
fn test_managed_add_dependency_should_error_on_circular_dependencies() {
    let mut mm = ModuleManager::new(Arc::new(NopLogger));
    mm.register_module("serviceA", Some(mock_init_ok()), &[]);
    mm.register_module("serviceB", Some(mock_init_ok()), &[]);
    mm.register_module("serviceC", Some(mock_init_ok()), &[]);
    mm.add_dependency("serviceA", &["serviceB"]).unwrap();
    mm.add_dependency("serviceB", &["serviceC"]).unwrap();

    let err = mm.add_dependency("serviceB", &["serviceA"]).unwrap_err();
    assert!(err.contains("circular dependency"), "{err}");

    let err = mm.add_dependency("serviceC", &["serviceA"]).unwrap_err();
    assert!(err.contains("circular dependency"), "{err}");
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestManaged_AddDependency_ShouldErrorIfModuleDoesNotExist
#[test]
fn test_managed_add_dependency_should_error_if_module_does_not_exist() {
    let mut mm = ModuleManager::new(Arc::new(NopLogger));
    mm.register_module("serviceA", Some(mock_init_ok()), &[]);
    mm.register_module("serviceB", Some(mock_init_ok()), &[]);
    mm.add_dependency("serviceA", &["serviceB"]).unwrap();

    assert_eq!(
        mm.add_dependency("serviceUnknown", &["serviceA"]).unwrap_err(),
        "no such module: serviceUnknown"
    );
    assert_eq!(
        mm.add_dependency("serviceA", &["serviceUnknown"]).unwrap_err(),
        "no such module: serviceUnknown"
    );
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestRegisterModuleDefaultsToUserVisible
#[test]
fn test_register_module_defaults_to_user_visible() {
    let mut sut = ModuleManager::new(Arc::new(NopLogger));
    sut.register_module("module1", Some(mock_init_ok()), &[]);
    let m = sut.get_module("module1").unwrap();
    assert!(m.init_fn.is_some());
    assert!(m.user_visible);
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestFunctionalOptAtTheEndWins
#[test]
fn test_functional_opt_at_the_end_wins() {
    fn user_visible_mod(m: &mut mimir_cmd::modules::ModuleDef) {
        m.user_visible = true;
    }
    let mut sut = ModuleManager::new(Arc::new(NopLogger));
    sut.register_module(
        "mod1",
        Some(mock_init_ok()),
        &[user_invisible_module, user_visible_mod, user_invisible_module],
    );
    let m = sut.get_module("mod1").unwrap();
    assert!(m.init_fn.is_some());
    assert!(!m.user_visible);
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestGetAllUserVisibleModulesNames
#[test]
fn test_get_all_user_visible_modules_names() {
    let mut sut = ModuleManager::new(Arc::new(NopLogger));
    sut.register_module("userVisible3", Some(mock_init_ok()), &[]);
    sut.register_module("userVisible2", Some(mock_init_ok()), &[]);
    sut.register_module("userVisible1", Some(mock_init_ok()), &[]);
    sut.register_module("internal1", Some(mock_init_ok()), &[user_invisible_module]);
    sut.register_module("internal2", Some(mock_init_ok()), &[user_invisible_module]);

    assert_eq!(
        sut.user_visible_module_names(),
        vec!["userVisible1", "userVisible2", "userVisible3"]
    );
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestGetAllUserVisibleModulesNamesHasNoDupWithDependency
#[test]
fn test_get_all_user_visible_modules_names_has_no_dup_with_dependency() {
    let mut sut = ModuleManager::new(Arc::new(NopLogger));
    sut.register_module("userVisible1", Some(mock_init_ok()), &[]);
    sut.register_module("userVisible2", Some(mock_init_ok()), &[]);
    sut.register_module("userVisible3", Some(mock_init_ok()), &[]);
    sut.add_dependency("userVisible1", &["userVisible2", "userVisible3"])
        .unwrap();

    assert_eq!(
        sut.user_visible_module_names(),
        vec!["userVisible1", "userVisible2", "userVisible3"]
    );
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestGetEmptyListWhenThereIsNoUserVisibleModule
#[test]
fn test_get_empty_list_when_there_is_no_user_visible_module() {
    let mut sut = ModuleManager::new(Arc::new(NopLogger));
    sut.register_module("internal1", Some(mock_init_ok()), &[user_invisible_module]);
    sut.register_module("internal2", Some(mock_init_ok()), &[user_invisible_module]);
    sut.register_module("internal3", Some(mock_init_ok()), &[user_invisible_module]);
    sut.register_module("internal4", Some(mock_init_ok()), &[user_invisible_module]);

    assert!(sut.user_visible_module_names().is_empty());
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestIsUserVisibleModule
#[test]
fn test_is_user_visible_module() {
    let mut sut = ModuleManager::new(Arc::new(NopLogger));
    sut.register_module("userVisible", Some(mock_init_ok()), &[]);
    sut.register_module("invisible", Some(mock_init_ok()), &[user_invisible_module]);

    assert!(sut.is_user_visible_module("userVisible"));
    assert!(sut.is_targetable_module("userVisible"));
    assert!(!sut.is_user_visible_module("invisible"));
    assert!(!sut.is_targetable_module("invisible"));
    assert!(!sut.is_user_visible_module("ghost"));
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestIsTargetableModule
#[test]
fn test_is_targetable_module() {
    let mut sut = ModuleManager::new(Arc::new(NopLogger));
    sut.register_module("userVisible", Some(mock_init_ok()), &[]);
    sut.register_module(
        "invisible",
        Some(mock_init_ok()),
        &[user_invisible_targetable_module],
    );

    assert!(sut.is_user_visible_module("userVisible"));
    assert!(sut.is_targetable_module("userVisible"));
    assert!(!sut.is_user_visible_module("invisible"));
    assert!(sut.is_targetable_module("invisible"));
    assert!(!sut.is_targetable_module("ghost"));
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestIsModuleRegistered
#[test]
fn test_is_module_registered() {
    let mut m = ModuleManager::new(Arc::new(NopLogger));
    m.register_module("successModule", Some(mock_init_ok()), &[]);
    assert!(m.is_module_registered("successModule"));
    assert!(!m.is_module_registered("failureModule"));
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestManager_DependenciesForModule
#[test]
fn test_manager_dependencies_for_module() {
    let mut m = ModuleManager::new(Arc::new(NopLogger));
    for name in ["test", "dep1", "dep2", "dep3"] {
        m.register_module(name, None, &[]);
    }
    m.add_dependency("test", &["dep2", "dep1"]).unwrap();
    m.add_dependency("dep1", &["dep2"]).unwrap();
    m.add_dependency("dep2", &["dep3"]).unwrap();

    assert_eq!(
        m.dependencies_for_module("test"),
        vec!["dep1", "dep2", "dep3"]
    );
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestManager_inverseDependenciesForModule
#[test]
fn test_manager_inverse_dependencies_for_module() {
    let mut m = ModuleManager::new(Arc::new(NopLogger));
    for name in ["test", "dep1", "dep2", "dep3"] {
        m.register_module(name, None, &[]);
    }
    m.add_dependency("test", &["dep2", "dep1"]).unwrap();
    m.add_dependency("dep1", &["dep2"]).unwrap();
    m.add_dependency("dep2", &["dep3"]).unwrap();

    assert!(m.inverse_dependencies_for_module("test").is_empty());
    assert_eq!(m.inverse_dependencies_for_module("dep1"), vec!["test"]);
    assert_eq!(
        m.inverse_dependencies_for_module("dep2"),
        vec!["dep1", "test"]
    );
    assert_eq!(
        m.inverse_dependencies_for_module("dep3"),
        vec!["dep1", "dep2", "test"]
    );
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestModuleWaitsForAllDependencies
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_module_waits_for_all_dependencies() {
    let service_a_holder: Arc<Mutex<Option<Arc<dyn Service>>>> = Arc::new(Mutex::new(None));
    let holder_a = Arc::clone(&service_a_holder);
    let init_a: ModuleInitFn = Arc::new(move || {
        let svc: Arc<dyn Service> = idle_service(
            Some(Box::new(|_| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(())
                })
            })),
            None,
        );
        *holder_a.lock() = Some(Arc::clone(&svc));
        Ok(Some(svc))
    });

    let holder_c = Arc::clone(&service_a_holder);
    let init_c: ModuleInitFn = Arc::new(move || {
        let holder = Arc::clone(&holder_c);
        let svc: Arc<dyn Service> = idle_service(
            Some(Box::new(move |_| {
                let holder = Arc::clone(&holder);
                Box::pin(async move {
                    let s = holder.lock().as_ref().unwrap().state();
                    if s != State::Running {
                        return Err(ServiceError::failure(format!(
                            "serviceA has invalid state: {s}"
                        )));
                    }
                    Ok(())
                })
            })),
            None,
        );
        Ok(Some(svc))
    });

    let mut m = ModuleManager::new(Arc::new(NopLogger));
    m.register_module("A", Some(init_a), &[]);
    m.register_module("B", None, &[]);
    m.register_module("C", Some(init_c), &[]);
    m.add_dependency("B", &["A"]).unwrap();
    m.add_dependency("C", &["B"]).unwrap();

    let res = m.init_module_services(&["C"]).unwrap();
    let servs: Vec<Arc<dyn Service>> = res.services.into_values().collect();
    let mgr = ServiceManager::new(servs).unwrap();
    start_manager_and_await_healthy(ServiceContext::background(), &mgr)
        .await
        .unwrap();
    stop_manager_and_await_stopped(ServiceContext::background(), &mgr)
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestModuleService_InterruptedFastStartup
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_module_service_interrupted_fast_startup() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let finish_rx = Arc::new(tokio::sync::Mutex::new(Some(finish_rx)));
    let subservice_stopped = Arc::new(AtomicBool::new(false));
    let ss_stop = Arc::clone(&subservice_stopped);

    let sub_service: Arc<dyn Service> = basic_service(
        Some(Box::new(move |_| {
            let started_tx = Arc::clone(&started_tx);
            let finish_rx = Arc::clone(&finish_rx);
            Box::pin(async move {
                if let Some(tx) = started_tx.lock().take() {
                    let _ = tx.send(());
                }
                let rx = finish_rx.lock().await.take();
                if let Some(rx) = rx {
                    let _ = rx.await;
                }
                Ok(())
            })
        })),
        Some(Box::new(|ctx| {
            Box::pin(async move {
                ctx.canceled().await;
                Ok(())
            })
        })),
        Some(Box::new(move |_| {
            let ss_stop = Arc::clone(&ss_stop);
            Box::pin(async move {
                ss_stop.store(true, Ordering::SeqCst);
                Ok(())
            })
        })),
    );

    let no_deps: mimir_cmd::modules::DepsMapFn = Arc::new(|_| HashMap::new());
    let module_svc = new_module_service(
        "A",
        Arc::new(NopLogger),
        Arc::clone(&sub_service),
        Arc::clone(&no_deps),
        no_deps,
    );

    let ctx = ServiceContext::background();
    let ctx_cancel = ctx.clone();
    tokio::spawn(async move {
        let _ = started_rx.await;
        ctx_cancel.cancel();
        let _ = finish_tx.send(());
    });

    module_svc.start_async(ctx).await.unwrap();
    let err = module_svc.await_running().await.unwrap_err().to_string();
    assert!(err.contains("context canceled"), "{err}");
    assert!(err.contains("starting module A"), "{err}");

    assert!(subservice_stopped.load(Ordering::SeqCst));
    assert_eq!(module_svc.state(), State::Failed);
    assert_eq!(sub_service.state(), State::Terminated);
}

// Upstream: github.com/grafana/dskit/modules/modules_test.go::TestModuleService_InterruptedSlowStartup
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_module_service_interrupted_slow_startup() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let subservice_stopped = Arc::new(AtomicBool::new(false));
    let ss_stop = Arc::clone(&subservice_stopped);

    let sub_service: Arc<dyn Service> = basic_service(
        Some(Box::new(move |ctx| {
            let started_tx = Arc::clone(&started_tx);
            Box::pin(async move {
                if let Some(tx) = started_tx.lock().take() {
                    let _ = tx.send(());
                }
                ctx.canceled().await;
                Ok(())
            })
        })),
        Some(Box::new(|_| {
            Box::pin(async {
                panic!("did not expect to enter running state");
            })
        })),
        Some(Box::new(move |_| {
            let ss_stop = Arc::clone(&ss_stop);
            Box::pin(async move {
                ss_stop.store(true, Ordering::SeqCst);
                Ok(())
            })
        })),
    );

    let no_deps: mimir_cmd::modules::DepsMapFn = Arc::new(|_| HashMap::new());
    let module_svc = new_module_service(
        "A",
        Arc::new(NopLogger),
        Arc::clone(&sub_service),
        Arc::clone(&no_deps),
        no_deps,
    );

    module_svc
        .start_async(ServiceContext::background())
        .await
        .unwrap();
    let _ = started_rx.await;

    let err = stop_and_await_terminated(ServiceContext::background(), module_svc.as_ref())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("context canceled"), "{err}");
    assert!(err.contains("starting module A"), "{err}");

    assert!(subservice_stopped.load(Ordering::SeqCst));
    assert_eq!(module_svc.state(), State::Failed);
    assert_eq!(sub_service.state(), State::Terminated);
}

// =============================================================================================
// dskit/limiter (8 tests)
// =============================================================================================

struct IncreasingLimitStrategy {
    limit: Mutex<f64>,
    burst: Mutex<usize>,
}

impl RateLimiterStrategy for IncreasingLimitStrategy {
    fn limit(&self, _tenant_id: &str) -> f64 {
        let mut l = self.limit.lock();
        *l += 1.0;
        *l
    }

    fn burst(&self, _tenant_id: &str) -> usize {
        let mut b = self.burst.lock();
        *b += 1;
        *b
    }
}

struct StaticLimitStrategy {
    tenants: HashMap<&'static str, (f64, usize)>,
}

impl RateLimiterStrategy for StaticLimitStrategy {
    fn limit(&self, tenant_id: &str) -> f64 {
        self.tenants.get(tenant_id).map_or(0.0, |v| v.0)
    }

    fn burst(&self, tenant_id: &str) -> usize {
        self.tenants.get(tenant_id).map_or(0, |v| v.1)
    }
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_RecheckPeriod
#[test]
fn test_rate_limiter_recheck_period() {
    let strategy = Arc::new(IncreasingLimitStrategy {
        limit: Mutex::new(0.0),
        burst: Mutex::new(0),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(10));
    let now = Instant::now();

    assert_eq!(limiter.limit(now, "test"), 1.0);
    assert_eq!(limiter.burst(now, "test"), 1);

    assert_eq!(limiter.limit(now + Duration::from_secs(9), "test"), 1.0);
    assert_eq!(limiter.burst(now + Duration::from_secs(9), "test"), 1);

    assert_eq!(limiter.limit(now + Duration::from_secs(10), "test"), 2.0);
    assert_eq!(limiter.burst(now + Duration::from_secs(10), "test"), 2);

    assert_eq!(limiter.limit(now + Duration::from_secs(19), "test"), 2.0);
    assert_eq!(limiter.burst(now + Duration::from_secs(19), "test"), 2);

    assert_eq!(limiter.limit(now + Duration::from_secs(20), "test"), 3.0);
    assert_eq!(limiter.burst(now + Duration::from_secs(20), "test"), 3);
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_AllowN
#[test]
fn test_rate_limiter_allow_n() {
    let strategy = Arc::new(StaticLimitStrategy {
        tenants: HashMap::from([("tenant-1", (10.0, 20)), ("tenant-2", (20.0, 40))]),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(10));
    let now = Instant::now();

    assert!(limiter.allow_n(now, "tenant-1", 8));
    assert!(limiter.allow_n(now, "tenant-1", 10));
    assert!(!limiter.allow_n(now, "tenant-1", 3));
    assert!(limiter.allow_n(now, "tenant-1", 2));

    let t1 = now + Duration::from_secs(1);
    assert!(limiter.allow_n(t1, "tenant-1", 8));
    assert!(!limiter.allow_n(t1, "tenant-1", 3));
    assert!(limiter.allow_n(t1, "tenant-1", 2));

    assert!(limiter.allow_n(now, "tenant-2", 18));
    assert!(limiter.allow_n(now, "tenant-2", 20));
    assert!(!limiter.allow_n(now, "tenant-2", 3));
    assert!(limiter.allow_n(now, "tenant-2", 2));

    assert!(limiter.allow_n(t1, "tenant-2", 18));
    assert!(!limiter.allow_n(t1, "tenant-2", 3));
    assert!(limiter.allow_n(t1, "tenant-2", 2));
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_TokensAt
#[test]
fn test_rate_limiter_tokens_at() {
    let strategy = Arc::new(StaticLimitStrategy {
        tenants: HashMap::from([("tenant-1", (10.0, 20)), ("unlimited", (RATE_INF, 0))]),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(10));
    let now = Instant::now();

    assert_eq!(limiter.tokens_at(now, "tenant-1"), 20.0);
    assert_eq!(limiter.tokens_at(now, "tenant-1"), 20.0);
    assert!(limiter.allow_n(now, "tenant-1", 20));
    assert_eq!(limiter.tokens_at(now, "tenant-1"), 0.0);
    assert!(!limiter.allow_n(now, "tenant-1", 1));
    assert_eq!(
        limiter.tokens_at(now + Duration::from_millis(500), "tenant-1"),
        5.0
    );

    let (_, r) = limiter.reserve_n(now, "tenant-1", 15);
    assert!(r.ok());
    assert_eq!(limiter.tokens_at(now, "tenant-1"), -15.0);

    assert!(limiter.allow_n(now, "unlimited", 1));
    assert!(limiter.tokens_at(now, "unlimited").is_infinite());
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_ReserveN
#[test]
fn test_rate_limiter_reserve_n() {
    let strategy = Arc::new(StaticLimitStrategy {
        tenants: HashMap::from([("tenant-1", (10.0, 20))]),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(10));
    let now = Instant::now();

    let (_, r) = limiter.reserve_n(now, "tenant-1", 5);
    assert!(r.ok());
    assert_eq!(r.delay_from(now), Duration::ZERO);

    let (_, r) = limiter.reserve_n(now, "tenant-1", 15);
    assert!(r.ok());
    assert_eq!(r.delay_from(now), Duration::ZERO);

    let (_, r) = limiter.reserve_n(now, "tenant-1", 5);
    assert!(r.ok());
    assert!(r.delay_from(now) > Duration::ZERO);
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_ReserveN_Cancel
#[test]
fn test_rate_limiter_reserve_n_cancel() {
    let strategy = Arc::new(StaticLimitStrategy {
        tenants: HashMap::from([("tenant-1", (10.0, 20))]),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(10));
    let now = Instant::now();

    let (tok, r) = limiter.reserve_n(now, "tenant-1", 20);
    assert!(r.ok());
    assert_eq!(r.delay_from(now), Duration::ZERO);
    assert!(!limiter.allow_n(now, "tenant-1", 20));

    tok.cancel_at(now, &r);
    assert!(limiter.allow_n(now, "tenant-1", 20));
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_ReserveN_ExceedsBurst
#[test]
fn test_rate_limiter_reserve_n_exceeds_burst() {
    let strategy = Arc::new(StaticLimitStrategy {
        tenants: HashMap::from([("tenant-1", (10.0, 20))]),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(10));
    let now = Instant::now();

    let (_, r) = limiter.reserve_n(now, "tenant-1", 21);
    assert!(!r.ok());
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_WaitN
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rate_limiter_wait_n() {
    let strategy = Arc::new(StaticLimitStrategy {
        tenants: HashMap::from([("tenant", (4.0, 5))]),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(1));

    let start = Instant::now();
    limiter.wait_n("tenant", 3).await.unwrap();
    limiter.wait_n("tenant", 2).await.unwrap();
    assert!(start.elapsed() <= Duration::from_millis(20));

    let start = Instant::now();
    limiter.wait_n("tenant", 1).await.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(200) && elapsed <= Duration::from_millis(350),
        "elapsed={elapsed:?}"
    );

    assert!(limiter.wait_n("tenant", 6).await.is_err());

    limiter.wait_n("tenant", 1).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), limiter.wait_n("tenant", 1))
            .await
            .is_err()
    );

    limiter.wait_n("tenant", 1).await.unwrap();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = cancel_tx.send(());
    });
    let canceled = tokio::select! {
        _ = cancel_rx => true,
        _ = limiter.wait_n("tenant", 1) => false,
    };
    assert!(canceled);
}

// Upstream: github.com/grafana/dskit/limiter/rate_limiter_test.go::TestRateLimiter_RemoveStaleEntries
#[test]
fn test_rate_limiter_remove_stale_entries() {
    let strategy = Arc::new(StaticLimitStrategy {
        tenants: HashMap::from([
            ("tenant-1", (10.0, 20)),
            ("tenant-2", (10.0, 20)),
            ("tenant-3", (10.0, 20)),
        ]),
    });
    let limiter = RateLimiter::new(strategy, Duration::from_secs(10));
    let now = Instant::now();

    assert_eq!(
        limiter.remove_stale_entries(now + Duration::from_secs(3600)),
        0
    );

    limiter.allow_n(now, "tenant-1", 1);
    limiter.allow_n(now + Duration::from_secs(5), "tenant-2", 1);
    limiter.allow_n(now + Duration::from_secs(9), "tenant-3", 1);

    assert_eq!(limiter.remove_stale_entries(now), 0);
    assert_eq!(
        limiter.remove_stale_entries(now + Duration::from_secs(12)),
        1
    );

    assert!(limiter.allow_n(now + Duration::from_secs(12), "tenant-2", 1));
    assert!(limiter.allow_n(now + Duration::from_secs(12), "tenant-3", 1));

    limiter.allow_n(now + Duration::from_secs(12), "tenant-1", 1);

    assert_eq!(
        limiter.remove_stale_entries(now + Duration::from_secs(20)),
        2
    );
    assert_eq!(
        limiter.remove_stale_entries(now + Duration::from_secs(3600)),
        1
    );
    assert_eq!(
        limiter.remove_stale_entries(now + Duration::from_secs(7200)),
        0
    );
}

// =============================================================================================
// dskit/log (15 tests)
// =============================================================================================

// Upstream: github.com/grafana/dskit/log/buffered_test.go::TestBufferedConcurrency
#[test]
fn test_buffered_concurrency() {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let buf_log = Arc::new(BufferedLogger::with_options(
        Box::new(CaptureBuf(sink)),
        256,
        Some(10_000_000),
        Some(Duration::from_millis(10)),
        None,
    ));

    let total = 10_000usize;
    let n = (total as f64).sqrt() as usize;
    let share = total / n;

    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let sw = buf_log.shared_writer();
        handles.push(std::thread::spawn(move || {
            let mut sw = sw;
            for i in 0..share {
                let line = format_logfmt_line(&[("key", &i.to_string())]);
                sw.write_all(line.as_bytes()).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    buf_log.flush().unwrap();
}

// Upstream: github.com/grafana/dskit/log/buffered_test.go::TestOnFlushCallback
#[test]
fn test_on_flush_callback() {
    let flush_count = Arc::new(AtomicUsize::new(0));
    let flushed_entries = Arc::new(AtomicUsize::new(0));
    let out = Arc::new(Mutex::new(Vec::new()));

    let fc = Arc::clone(&flush_count);
    let fe = Arc::clone(&flushed_entries);
    let buf_log = BufferedLogger::with_options(
        Box::new(CaptureBuf(Arc::clone(&out))),
        2,
        Some(1024),
        None,
        Some(Arc::new(move |entries| {
            fc.fetch_add(1, Ordering::SeqCst);
            fe.fetch_add(entries as usize, Ordering::SeqCst);
        })),
    );

    buf_log.write_entry(b"msg=line\n").unwrap();
    buf_log.write_entry(b"msg=line\n").unwrap();
    // Third write triggers flush of the first 2 entries
    buf_log.write_entry(b"msg=line\n").unwrap();

    assert_eq!(buf_log.size(), 1);
    buf_log.flush().unwrap();

    assert_eq!(flush_count.load(Ordering::SeqCst), 2);
    let text = String::from_utf8(out.lock().clone()).unwrap();
    let lines: Vec<&str> = text.split('\n').collect();
    assert_eq!(lines.len(), flushed_entries.load(Ordering::SeqCst) + 1);
}

// Upstream: github.com/grafana/dskit/log/gokit_test.go::TestLazySprintf
#[test]
fn test_lazy_sprintf() {
    for (id, lvl_str, should_log) in [(1, "debug", true), (2, "info", false)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let lazy = LazySprintf::new(move || {
            calls_inner.fetch_add(1, Ordering::SeqCst);
            format!("{lvl_str} {id} has been logged")
        });

        let buf = Arc::new(Mutex::new(Vec::new()));
        let inner = CounterLogfmtLogger::new(Arc::clone(&buf));
        let lvl = CommonLogLevel::parse(lvl_str).unwrap();
        let filtered = LevelFilterLogger::new(inner.clone(), lvl);

        if filtered.debug_enabled() {
            let rendered = lazy.to_string();
            filtered.log(CommonLogLevel::Debug, &[("msg", &rendered)]);
        }

        let output = inner.output();
        if should_log {
            assert!(output.contains(&format!("debug {id} has been logged")), "{output}");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        } else {
            assert!(!output.contains(&format!("info {id} has been logged")), "{output}");
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }
}

// Upstream: github.com/grafana/dskit/log/gokit_test.go::TestNewGoKitWithRateLimitedAndFields
#[test]
fn test_new_go_kit_with_rate_limited_and_fields() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let base = CounterLogfmtLogger::new(Arc::clone(&buf));
    let reg = Registry::new();
    let rl: Arc<dyn Logger> = Arc::new(TokenBucketRateLimitedLogger::new(
        base.clone(),
        1.0,
        2,
        Some(&reg),
    ));
    let with_fields: Arc<dyn Logger> = Arc::new(WithFieldsLogger {
        inner: rl,
        prefix_kvs: vec![
            ("ts".into(), "1970-01-01T00:00:00Z".into()),
            ("caller".into(), "gokit_test.go:88".into()),
        ],
    });
    let filtered = LevelFilterLogger::new(with_fields, CommonLogLevel::Info);

    let now = Instant::now();
    for i in 0..1000 {
        let msg = format!("info {i}");
        filtered.log(CommonLogLevel::Info, &[("msg", &msg)]);
        if filtered.debug_enabled() {
            let dmsg = format!("debug {i}");
            filtered.log(CommonLogLevel::Debug, &[("msg", &dmsg)]);
        }
    }
    let _ = now;

    let out = base.output();
    for i in 0..1000 {
        let needle = format!("msg=\"info {i}\"");
        if i < 2 {
            assert!(out.contains(&needle), "missing {needle} in {out}");
        } else {
            assert!(!out.contains(&needle), "unexpected {needle}");
        }
        assert!(!out.contains(&format!("msg=\"debug {i}\"")));
    }
}

// Upstream: github.com/grafana/dskit/log/level_test.go::TestMarshalYAML
#[test]
fn test_marshal_yaml() {
    let l = ConfigLogLevel::parse("debug").unwrap();
    let val = serde_yaml::to_value(l).unwrap();
    let y = marshal_struct(&val).unwrap();
    assert_eq!(y, "debug\n");
}

// Upstream: github.com/grafana/dskit/log/ratelimit_test.go::TestRateLimitedLoggerLogs
#[test]
fn test_rate_limited_logger_logs() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let c = CounterLogfmtLogger::new(buf);
    let reg = Registry::new();
    let r = TokenBucketRateLimitedLogger::new(c.clone(), 1.0, 1, Some(&reg));

    r.log(CommonLogLevel::Error, &[("msg", "error will be logged")]);
    assert_eq!(c.count(), 1);
    let out = c.output();
    assert!(out.contains("error"));
    assert!(out.contains("error will be logged"));

    let mf = reg
        .gather()
        .into_iter()
        .find(|f| f.get_name() == "logger_rate_limit_discarded_log_lines_total")
        .unwrap();
    for m in mf.get_metric() {
        assert_eq!(m.get_counter().get_value(), 0.0);
    }
}

// Upstream: github.com/grafana/dskit/log/ratelimit_test.go::TestRateLimitedLoggerLimits
#[test]
fn test_rate_limited_logger_limits() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let c = CounterLogfmtLogger::new(buf);
    let reg = Registry::new();
    let r = TokenBucketRateLimitedLogger::new(c.clone(), 2.0, 2, Some(&reg));

    let t0 = Instant::now();
    r.log_at(t0, CommonLogLevel::Error, &[("msg", "error 1 will be logged")]);
    assert_eq!(c.count(), 1);
    assert!(c.output().contains("error 1 will be logged"));

    r.log_at(t0, CommonLogLevel::Info, &[("msg", "info 1 will be logged")]);
    assert_eq!(c.count(), 2);
    assert!(c.output().contains("info 1 will be logged"));

    r.log_at(t0, CommonLogLevel::Debug, &[("msg", "debug 1 will be discarded")]);
    assert_eq!(c.count(), 2);
    assert!(!c.output().contains("debug 1 will be discarded"));

    r.log_at(t0, CommonLogLevel::Warn, &[("msg", "warning 1 will be discarded")]);
    assert_eq!(c.count(), 2);
    assert!(!c.output().contains("warning 1 will be discarded"));

    let discarded = |lvl: &str| -> f64 {
        reg.gather()
            .into_iter()
            .find(|f| f.get_name() == "logger_rate_limit_discarded_log_lines_total")
            .unwrap()
            .get_metric()
            .iter()
            .find(|m| m.get_label().iter().any(|l| l.get_name() == "level" && l.get_value() == lvl))
            .unwrap()
            .get_counter()
            .get_value()
    };
    assert_eq!(discarded("info"), 0.0);
    assert_eq!(discarded("debug"), 1.0);
    assert_eq!(discarded("warn"), 1.0);
    assert_eq!(discarded("error"), 0.0);

    let t1 = t0 + Duration::from_secs(1);
    r.log_at(t1, CommonLogLevel::Debug, &[("msg", "debug 2 will be logged")]);
    assert_eq!(c.count(), 3);
    assert!(c.output().contains("debug 2 will be logged"));

    r.log_at(t1, CommonLogLevel::Info, &[("msg", "info 2 will be logged")]);
    assert_eq!(c.count(), 4);
    assert!(c.output().contains("info 2 will be logged"));

    r.log_at(t1, CommonLogLevel::Error, &[("msg", "error 2 will be discarded")]);
    assert_eq!(c.count(), 4);
    assert!(!c.output().contains("error 2 will be discarded"));

    r.log_at(t1, CommonLogLevel::Warn, &[("msg", "warning 2 will be discarded")]);
    assert_eq!(c.count(), 4);
    assert!(!c.output().contains("warning 2 will be discarded"));

    assert_eq!(discarded("info"), 0.0);
    assert_eq!(discarded("debug"), 1.0);
    assert_eq!(discarded("error"), 1.0);
    assert_eq!(discarded("warn"), 2.0);
}

// Upstream: github.com/grafana/dskit/log/ratelimit_test.go::TestRateLimitedLoggerWithFields
#[test]
fn test_rate_limited_logger_with_fields() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let c = CounterLogfmtLogger::new(buf);
    let reg = Registry::new();
    let rl: Arc<dyn Logger> = Arc::new(TokenBucketRateLimitedLogger::new(
        c.clone(),
        1.0,
        1,
        Some(&reg),
    ));
    let with_fields = WithFieldsLogger {
        inner: Arc::clone(&rl),
        prefix_kvs: vec![("key".into(), "value".into())],
    };

    with_fields.log(CommonLogLevel::Error, &[("msg", "error will be logged")]);
    assert_eq!(c.count(), 1);
    let out = c.output();
    assert!(out.contains("key"));
    assert!(out.contains("value"));
    assert!(out.contains("error will be logged"));

    rl.log(CommonLogLevel::Info, &[("msg", "info will not be logged")]);
    assert!(!c.output().contains("info will not be logged"));

    with_fields.log(CommonLogLevel::Debug, &[("msg", "debug will not be logged")]);
    assert!(!c.output().contains("debug will not be logged"));

    with_fields.log(CommonLogLevel::Warn, &[("msg", "warning will not be logged")]);
    assert!(!c.output().contains("warning will not be logged"));
    assert_eq!(c.count(), 1);

    let discarded = |lvl: &str| -> f64 {
        reg.gather()
            .into_iter()
            .find(|f| f.get_name() == "logger_rate_limit_discarded_log_lines_total")
            .unwrap()
            .get_metric()
            .iter()
            .find(|m| m.get_label().iter().any(|l| l.get_name() == "level" && l.get_value() == lvl))
            .unwrap()
            .get_counter()
            .get_value()
    };
    assert_eq!(discarded("info"), 1.0);
    assert_eq!(discarded("debug"), 1.0);
    assert_eq!(discarded("warn"), 1.0);
    assert_eq!(discarded("error"), 0.0);
}

// Upstream: github.com/grafana/dskit/log/sanitize_test.go::TestDropVsEscapeUnsafeChars
#[test]
fn test_drop_vs_escape_unsafe_chars() {
    let cases: &[(&str, UnsafeInput<'_>, &str, &str)] = &[
        (
            "plain ascii",
            UnsafeInput::Str("hello world"),
            "hello world",
            "hello world",
        ),
        (
            "tab preserved",
            UnsafeInput::Str("col1\tcol2"),
            "col1\tcol2",
            "col1\tcol2",
        ),
        (
            "newline injection",
            UnsafeInput::Str("first line\nlevel=info msg=fake"),
            "first linelevel=info msg=fake",
            "first line\\x0alevel=info msg=fake",
        ),
        (
            "ANSI escape",
            UnsafeInput::Str("\x1b[31mFAKE ERROR\x1b[0m"),
            "[31mFAKE ERROR[0m",
            "\\x1b[31mFAKE ERROR\\x1b[0m",
        ),
        (
            "NUL byte",
            UnsafeInput::Str("abc\x00def"),
            "abcdef",
            "abc\\x00def",
        ),
        (
            "DEL 0x7F",
            UnsafeInput::Str("abc\x7fdef"),
            "abcdef",
            "abc\\x7fdef",
        ),
        (
            "C1 NEL U+0085",
            UnsafeInput::Str("abc\u{0085}def"),
            "abcdef",
            "abc\\x85def",
        ),
        (
            "lone 0x85 invalid UTF-8",
            UnsafeInput::Bytes(b"abc\x85def"),
            "abcdef",
            "abc\\x85def",
        ),
        (
            "lone 0x82 invalid UTF-8",
            UnsafeInput::Bytes(b"abc\x82def"),
            "abcdef",
            "abc\\x82def",
        ),
        (
            "truncated U+2028 prefix",
            UnsafeInput::Bytes(b"first\xe2\x80second"),
            "firstsecond",
            "first\\xe2\\x80second",
        ),
        (
            "Unicode U+2028",
            UnsafeInput::Str("first\u{2028}second"),
            "firstsecond",
            "first\\u2028second",
        ),
        (
            "bidi override",
            UnsafeInput::Str("if (admin)\u{202e} true \u{202d}{ deleteAll() }"),
            "if (admin) true { deleteAll() }",
            "if (admin)\\u202e true \\u202d{ deleteAll() }",
        ),
        (
            "non-control unicode",
            UnsafeInput::Str("Mozilla/5.0 (café)"),
            "Mozilla/5.0 (café)",
            "Mozilla/5.0 (café)",
        ),
        (
            "int 42",
            UnsafeInput::Owned(42.to_string()),
            "42",
            "42",
        ),
        ("null", UnsafeInput::Null, "null", "null"),
    ];

    for (name, input, want_drop, want_escape) in cases {
        let d = match input {
            UnsafeInput::Str(s) => drop_unsafe_chars_wrapper(UnsafeInput::Str(s)).to_string(),
            UnsafeInput::Bytes(b) => drop_unsafe_chars_wrapper(UnsafeInput::Bytes(b)).to_string(),
            UnsafeInput::Owned(s) => drop_unsafe_chars_wrapper(UnsafeInput::Str(s)).to_string(),
            UnsafeInput::Null => drop_unsafe_chars_wrapper(UnsafeInput::Null).to_string(),
        };
        let e = match input {
            UnsafeInput::Str(s) => escape_unsafe_chars_wrapper(UnsafeInput::Str(s)).to_string(),
            UnsafeInput::Bytes(b) => escape_unsafe_chars_wrapper(UnsafeInput::Bytes(b)).to_string(),
            UnsafeInput::Owned(s) => escape_unsafe_chars_wrapper(UnsafeInput::Str(s)).to_string(),
            UnsafeInput::Null => escape_unsafe_chars_wrapper(UnsafeInput::Null).to_string(),
        };
        assert_eq!(d, *want_drop, "{name} drop");
        assert_eq!(e, *want_escape, "{name} escape");
    }
}

// Upstream: github.com/grafana/dskit/log/sanitize_test.go::TestDroppedUnsafeChars_MarshalJSON
#[test]
fn test_dropped_unsafe_chars_marshal_json() {
    let w = drop_unsafe_chars_wrapper(UnsafeInput::Str("Mozilla\nlevel=info msg=fake\x1b[31m!"));
    let got = serde_json::to_string(&w).unwrap();
    assert_eq!(got, r#""Mozillalevel=info msg=fake[31m!""#);
}

// Upstream: github.com/grafana/dskit/log/sanitize_test.go::TestEscapedUnsafeChars_MarshalJSON
#[test]
fn test_escaped_unsafe_chars_marshal_json() {
    let w = escape_unsafe_chars_wrapper(UnsafeInput::Str("Mozilla\nlevel=info msg=fake\x1b[31m!"));
    let got = serde_json::to_string(&w).unwrap();
    assert_eq!(got, r#""Mozilla\\x0alevel=info msg=fake\\x1b[31m!""#);
}

// Upstream: github.com/grafana/dskit/log/sanitize_test.go::TestUnsafeChars_NullValue_MarshalJSON
#[test]
fn test_unsafe_chars_null_value_marshal_json() {
    let d = drop_unsafe_chars_wrapper(UnsafeInput::Null);
    let e = escape_unsafe_chars_wrapper(UnsafeInput::Null);
    assert_eq!(serde_json::to_string(&d).unwrap(), "null");
    assert_eq!(serde_json::to_string(&e).unwrap(), "null");
}

// Upstream: github.com/grafana/dskit/log/sanitize_test.go::TestZeroAllocsOnCleanStringPath
#[test]
fn test_zero_allocs_on_clean_string_path() {
    let clean = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36";
    let dropped = drop_unsafe_chars_wrapper(UnsafeInput::Str(clean));
    let escaped = escape_unsafe_chars_wrapper(UnsafeInput::Str(clean));

    let (d_cow, d_allocs) = count_allocs(|| dropped.as_cow_str());
    assert!(matches!(d_cow, Cow::Borrowed(_)));
    assert_eq!(d_allocs, 0);

    let (e_cow, e_allocs) = count_allocs(|| escaped.as_cow_str());
    assert!(matches!(e_cow, Cow::Borrowed(_)));
    assert_eq!(e_allocs, 0);
}

// Upstream: github.com/grafana/dskit/log/sanitize_test.go::TestEndToEnd_logfmt
#[test]
fn test_end_to_end_logfmt() {
    let cases: &[(&str, UnsafeInput<'_>, &str, &str)] = &[
        (
            "clean string",
            UnsafeInput::Str("Mozilla/5.0"),
            "user_agent=Mozilla/5.0\n",
            "user_agent=Mozilla/5.0\n",
        ),
        (
            "embedded newline",
            UnsafeInput::Str("Mozilla\nFAKE"),
            "user_agent=MozillaFAKE\n",
            "user_agent=Mozilla\\x0aFAKE\n",
        ),
        (
            "contains equals",
            UnsafeInput::Str("path=/foo\nFAKE"),
            "user_agent=\"path=/fooFAKE\"\n",
            "user_agent=\"path=/foo\\\\x0aFAKE\"\n",
        ),
        ("null", UnsafeInput::Null, "user_agent=null\n", "user_agent=null\n"),
    ];

    for (name, input, want_drop, want_escape) in cases {
        let (d_out, e_out) = match input {
            UnsafeInput::Str(s) => (
                format_logfmt_sanitized("user_agent", &drop_unsafe_chars_wrapper(UnsafeInput::Str(s))),
                format_logfmt_sanitized_escaped("user_agent", &escape_unsafe_chars_wrapper(UnsafeInput::Str(s))),
            ),
            UnsafeInput::Null => (
                format_logfmt_sanitized("user_agent", &drop_unsafe_chars_wrapper(UnsafeInput::Null)),
                format_logfmt_sanitized_escaped("user_agent", &escape_unsafe_chars_wrapper(UnsafeInput::Null)),
            ),
            _ => unreachable!(),
        };
        assert_eq!(&d_out, want_drop, "{name} drop");
        assert_eq!(&e_out, want_escape, "{name} escape");
    }
}

// Upstream: github.com/grafana/dskit/log/sanitize_test.go::TestEndToEnd_json
#[test]
fn test_end_to_end_json() {
    let cases: &[(&str, UnsafeInput<'_>, &str, &str)] = &[
        (
            "clean string",
            UnsafeInput::Str("Mozilla/5.0"),
            r#"{"user_agent":"Mozilla/5.0"}"#,
            r#"{"user_agent":"Mozilla/5.0"}"#,
        ),
        (
            "embedded newline",
            UnsafeInput::Str("Mozilla\nFAKE"),
            r#"{"user_agent":"MozillaFAKE"}"#,
            r#"{"user_agent":"Mozilla\\x0aFAKE"}"#,
        ),
        (
            "contains equals",
            UnsafeInput::Str("path=/foo\nFAKE"),
            r#"{"user_agent":"path=/fooFAKE"}"#,
            r#"{"user_agent":"path=/foo\\x0aFAKE"}"#,
        ),
        (
            "null",
            UnsafeInput::Null,
            r#"{"user_agent":null}"#,
            r#"{"user_agent":null}"#,
        ),
    ];

    for (name, input, want_drop, want_escape) in cases {
        match input {
            UnsafeInput::Str(s) => {
                let d_val = drop_unsafe_chars_wrapper(UnsafeInput::Str(s)).to_string();
                let e_val = escape_unsafe_chars_wrapper(UnsafeInput::Str(s)).to_string();
                let mut d_out = String::new();
                encode_json_record(&mut d_out, &[("user_agent", LogValue::Str(d_val.into()))]);
                assert_eq!(d_out.trim(), *want_drop, "{name} drop");
                let mut e_out = String::new();
                encode_json_record(&mut e_out, &[("user_agent", LogValue::Str(e_val.into()))]);
                assert_eq!(e_out.trim(), *want_escape, "{name} escape");
            }
            UnsafeInput::Null => {
                let d_out = format_json_line(&[]);
                let _ = d_out;
                let d = drop_unsafe_chars_wrapper(UnsafeInput::Null);
                let e = escape_unsafe_chars_wrapper(UnsafeInput::Null);
                assert_eq!(
                    format!("{{\"user_agent\":{}}}", serde_json::to_string(&d).unwrap()),
                    *want_drop
                );
                assert_eq!(
                    format!("{{\"user_agent\":{}}}", serde_json::to_string(&e).unwrap()),
                    *want_escape
                );
            }
            _ => unreachable!(),
        }
    }
}
