//! Ported from:
//! - github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go (all 7 tests)
//! - github.com/grafana/dskit/ring/basic_lifecycler_test.go (all 12 tests)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::{
    start_and_await_running, stop_and_await_terminated, Service, ServiceContext, State,
};
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{BoxFuture, CasOutcome, KvClient};
use mimir_ring::lifecycler::{
    AutoForgetDelegate, BasicLifecycler, BasicLifecyclerConfig, BasicLifecyclerDelegate,
    InstanceRegisterDelegate, LeaveOnStoppingDelegate, TokensPersistencyDelegate,
};
use mimir_ring::ring::model::{
    ring_desc_codec, system_time_to_unix_secs, DescExt, InstanceDescExt,
};
use mimir_ring::token_gen::{
    load_tokens_from_file, store_tokens_to_file, RandomTokenGenerator,
    SpreadMinimizingTokenGenerator, TokenGenerator,
};
use parking_lot::Mutex;
use prometheus::Registry;

const TEST_RING_KEY: &str = "test";
const TEST_RING_NAME: &str = "test";
const TEST_INSTANCE_ID: &str = "test-id-1";

fn zone(id: usize) -> String {
    format!("zone{id}")
}

fn prepare_basic_lifecycler_config() -> BasicLifecyclerConfig {
    BasicLifecyclerConfig {
        id: TEST_INSTANCE_ID.to_string(),
        addr: "127.0.0.1:12345".to_string(),
        zone: zone(1),
        heartbeat_period: Duration::from_secs(60),
        heartbeat_timeout: Duration::from_secs(60),
        tokens_observe_period: Duration::ZERO,
        num_tokens: 5,
        keep_instance_in_the_ring_on_shutdown: false,
        token_generator: None,
        versions: HashMap::new(),
    }
}

type OnRegisterFn = Arc<
    dyn Fn(&BasicLifecycler, &Desc, bool, &str, Option<&InstanceDesc>) -> (InstanceState, Vec<u32>)
        + Send
        + Sync,
>;
type OnTokensFn = Arc<dyn Fn(&BasicLifecycler, &[u32]) + Send + Sync>;
type OnStoppingFn = Arc<
    dyn for<'a> Fn(&'a BasicLifecycler) -> BoxFuture<'a, ()> + Send + Sync,
>;
type OnHeartbeatFn =
    Arc<dyn Fn(&BasicLifecycler, &mut Desc, &mut InstanceDesc) + Send + Sync>;

#[derive(Default)]
struct MockDelegate {
    on_register: Mutex<Option<OnRegisterFn>>,
    on_tokens_changed: Mutex<Option<OnTokensFn>>,
    on_stopping: Mutex<Option<OnStoppingFn>>,
    on_heartbeat: Mutex<Option<OnHeartbeatFn>>,
}

impl MockDelegate {
    fn set_on_register<F>(&self, f: F)
    where
        F: Fn(
                &BasicLifecycler,
                &Desc,
                bool,
                &str,
                Option<&InstanceDesc>,
            ) -> (InstanceState, Vec<u32>)
            + Send
            + Sync
            + 'static,
    {
        *self.on_register.lock() = Some(Arc::new(f));
    }

    fn set_on_stopping<F>(&self, f: F)
    where
        F: for<'a> Fn(&'a BasicLifecycler) -> BoxFuture<'a, ()> + Send + Sync + 'static,
    {
        *self.on_stopping.lock() = Some(Arc::new(f));
    }
}

impl BasicLifecyclerDelegate for MockDelegate {
    fn on_ring_instance_register(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &Desc,
        instance_exists: bool,
        instance_id: &str,
        instance_desc: Option<&InstanceDesc>,
    ) -> (InstanceState, Vec<u32>) {
        let cb = self.on_register.lock().clone();
        if let Some(f) = cb {
            f(lifecycler, ring_desc, instance_exists, instance_id, instance_desc)
        } else {
            (InstanceState::Pending, Vec::new())
        }
    }

    fn on_ring_instance_tokens(&self, lifecycler: &BasicLifecycler, tokens: &[u32]) {
        let cb = self.on_tokens_changed.lock().clone();
        if let Some(f) = cb {
            f(lifecycler, tokens);
        }
    }

    fn on_ring_instance_stopping<'a>(
        &'a self,
        lifecycler: &'a BasicLifecycler,
    ) -> BoxFuture<'a, ()> {
        let cb = self.on_stopping.lock().clone();
        Box::pin(async move {
            if let Some(f) = cb {
                f(lifecycler).await;
            }
        })
    }

    fn on_ring_instance_heartbeat(
        &self,
        lifecycler: &BasicLifecycler,
        ring_desc: &mut Desc,
        instance_desc: &mut InstanceDesc,
    ) {
        let cb = self.on_heartbeat.lock().clone();
        if let Some(f) = cb {
            f(lifecycler, ring_desc, instance_desc);
        }
    }
}

fn prepare_basic_lifecycler_with_delegate(
    cfg: BasicLifecyclerConfig,
    delegate: Arc<dyn BasicLifecyclerDelegate>,
    reg: Option<&Registry>,
) -> (Arc<BasicLifecycler>, Arc<dyn KvClient>) {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let lc = BasicLifecycler::new(
        cfg,
        TEST_RING_NAME,
        TEST_RING_KEY,
        Arc::clone(&store),
        delegate,
        reg.map(|r| r as &dyn mimir_ring::kv::AsRegisterer),
    );
    (lc, store)
}

fn prepare_basic_lifecycler(
    cfg: BasicLifecyclerConfig,
    reg: Option<&Registry>,
) -> (Arc<BasicLifecycler>, Arc<MockDelegate>, Arc<dyn KvClient>) {
    let delegate = Arc::new(MockDelegate::default());
    let (lc, store) = prepare_basic_lifecycler_with_delegate(cfg, delegate.clone(), reg);
    (lc, delegate, store)
}

async fn get_instance_from_store(store: &dyn KvClient, instance_id: &str) -> Option<InstanceDesc> {
    let out = store.get(TEST_RING_KEY).await.unwrap()?;
    let desc = out.downcast_ref::<Desc>()?;
    desc.ingesters.get(instance_id).cloned()
}

fn gather_metric(reg: &Registry, name: &str) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if m.has_gauge() {
                    return m.get_gauge().get_value();
                }
                if m.has_counter() {
                    return m.get_counter().get_value();
                }
            }
        }
    }
    0.0
}

async fn poll<F, Fut>(timeout: Duration, interval: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}

// ============================================================================
// basic_lifecycler_delegates_test.go (7 tests)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go::TestLeaveOnStoppingDelegate
#[tokio::test]
async fn test_leave_on_stopping_delegate() {
    let on_stopping_called = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&on_stopping_called);

    let cfg = prepare_basic_lifecycler_config();
    let test_delegate = Arc::new(MockDelegate::default());
    test_delegate.set_on_stopping(move |l| {
        let flag = Arc::clone(&flag);
        Box::pin(async move {
            assert_eq!(InstanceState::Leaving, l.get_state());
            flag.store(true, Ordering::SeqCst);
        })
    });

    let leave_delegate = Arc::new(LeaveOnStoppingDelegate::new(test_delegate));
    let (lifecycler, _store) = prepare_basic_lifecycler_with_delegate(cfg, leave_delegate, None);
    let svc = lifecycler.into_service();

    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    assert!(on_stopping_called.load(Ordering::SeqCst));
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go::TestTokensPersistencyDelegate_ShouldSkipTokensLoadingIfFileDoesNotExist
#[tokio::test]
async fn test_tokens_persistency_delegate_should_skip_tokens_loading_if_file_does_not_exist() {
    let dir = tempfile::tempdir().unwrap();
    let tokens_path = dir.path().join("tokens-missing");

    let test_delegate = Arc::new(MockDelegate::default());
    test_delegate.set_on_register(|_, _, instance_exists, _, _| {
        assert!(!instance_exists);
        (InstanceState::Joining, vec![1, 2, 3, 4, 5])
    });

    let persistency_delegate = Arc::new(TokensPersistencyDelegate::new(
        &tokens_path,
        InstanceState::Active,
        test_delegate,
    ));

    let cfg = prepare_basic_lifecycler_config();
    let (lifecycler, _store) =
        prepare_basic_lifecycler_with_delegate(cfg, persistency_delegate, None);
    let svc = lifecycler.clone().into_service();

    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert_eq!(InstanceState::Joining, lifecycler.get_state());
    assert_eq!(vec![1, 2, 3, 4, 5], lifecycler.get_tokens());
    assert!(lifecycler.is_registered());

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();

    let actual_tokens = load_tokens_from_file(&tokens_path).unwrap();
    assert_eq!(vec![1, 2, 3, 4, 5], actual_tokens);
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go::TestTokensPersistencyDelegate_ShouldLoadTokensFromFileIfFileExist
#[tokio::test]
async fn test_tokens_persistency_delegate_should_load_tokens_from_file_if_file_exist() {
    let dir = tempfile::tempdir().unwrap();
    let tokens_path = dir.path().join("tokens-existing");
    let stored_tokens = vec![6, 7, 8, 9, 10];
    store_tokens_to_file(&stored_tokens, &tokens_path).unwrap();

    let expected = stored_tokens.clone();
    let test_delegate = Arc::new(MockDelegate::default());
    test_delegate.set_on_register(move |_, _, instance_exists, _, instance_desc| {
        assert!(instance_exists);
        let desc = instance_desc.expect("synthetic instance_desc");
        assert_eq!(InstanceState::Active, desc.instance_state());
        assert_eq!(expected, desc.tokens);
        assert!(desc.get_registered_at().is_none());
        (desc.instance_state(), desc.tokens.clone())
    });

    let persistency_delegate = Arc::new(TokensPersistencyDelegate::new(
        &tokens_path,
        InstanceState::Active,
        test_delegate,
    ));

    let cfg = prepare_basic_lifecycler_config();
    let (lifecycler, _store) =
        prepare_basic_lifecycler_with_delegate(cfg, persistency_delegate, None);
    let svc = lifecycler.clone().into_service();

    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert_eq!(InstanceState::Active, lifecycler.get_state());
    assert_eq!(stored_tokens, lifecycler.get_tokens());
    assert!(lifecycler.is_registered());
    let now_secs = system_time_to_unix_secs(SystemTime::now());
    let reg_secs = system_time_to_unix_secs(lifecycler.get_registered_at().unwrap());
    assert!((now_secs - reg_secs).abs() <= 2);

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();

    let actual_tokens = load_tokens_from_file(&tokens_path).unwrap();
    assert_eq!(stored_tokens, actual_tokens);
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go::TestTokensPersistencyDelegate_ShouldHandleTheCaseTheInstanceIsAlreadyInTheRing
#[tokio::test]
async fn test_tokens_persistency_delegate_should_handle_the_case_the_instance_is_already_in_the_ring() {
    let stored_tokens = vec![6, 7, 8, 9, 10];
    let different_tokens = vec![1, 2, 3, 4, 5];

    struct Case {
        initial_state: InstanceState,
        initial_tokens: Vec<u32>,
        expected_state: InstanceState,
        expected_tokens: Vec<u32>,
    }

    let cases = [
        Case {
            initial_state: InstanceState::Pending,
            initial_tokens: Vec::new(),
            expected_state: InstanceState::Active,
            expected_tokens: stored_tokens.clone(),
        },
        Case {
            initial_state: InstanceState::Joining,
            initial_tokens: different_tokens.clone(),
            expected_state: InstanceState::Joining,
            expected_tokens: different_tokens.clone(),
        },
    ];

    for tc in cases {
        let dir = tempfile::tempdir().unwrap();
        let tokens_path = dir.path().join("tokens");
        store_tokens_to_file(&stored_tokens, &tokens_path).unwrap();

        let registered_at = SystemTime::now() - Duration::from_secs(3600);

        let test_delegate = Arc::new(MockDelegate::default());
        test_delegate.set_on_register(|_, _, _, _, instance_desc| {
            let d = instance_desc.expect("existing instance");
            (d.instance_state(), d.tokens.clone())
        });

        let persistency_delegate = Arc::new(TokensPersistencyDelegate::new(
            &tokens_path,
            InstanceState::Active,
            test_delegate,
        ));

        let cfg = prepare_basic_lifecycler_config();
        let cfg_clone = cfg.clone();
        let (lifecycler, store) =
            prepare_basic_lifecycler_with_delegate(cfg, persistency_delegate, None);

        let init_tokens = tc.initial_tokens.clone();
        let init_state = tc.initial_state;
        store
            .cas(
                TEST_RING_KEY,
                Box::new(move |_| {
                    let mut ring_desc = Desc::default();
                    ring_desc.add_ingester(
                        &cfg_clone.id,
                        &cfg_clone.addr,
                        &cfg_clone.zone,
                        init_tokens.clone(),
                        init_state,
                        Some(registered_at),
                        false,
                        None,
                        HashMap::new(),
                    );
                    Ok(CasOutcome::Update(Box::new(ring_desc)))
                }),
            )
            .await
            .unwrap();

        let svc = lifecycler.clone().into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();

        assert_eq!(tc.expected_state, lifecycler.get_state());
        assert_eq!(tc.expected_tokens, lifecycler.get_tokens());
        assert!(lifecycler.is_registered());
        assert_eq!(
            system_time_to_unix_secs(registered_at),
            system_time_to_unix_secs(lifecycler.get_registered_at().unwrap())
        );

        stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go::TestDelegatesChain
#[tokio::test]
async fn test_delegates_chain() {
    let on_stopping_called = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&on_stopping_called);

    let dir = tempfile::tempdir().unwrap();
    let tokens_path = dir.path().join("tokens-chain");

    let base = Arc::new(MockDelegate::default());
    base.set_on_register(|_, _, instance_exists, _, _| {
        assert!(!instance_exists);
        (InstanceState::Joining, vec![1, 2, 3, 4, 5])
    });
    base.set_on_stopping(move |l| {
        let flag = Arc::clone(&flag);
        Box::pin(async move {
            assert_eq!(InstanceState::Leaving, l.get_state());
            flag.store(true, Ordering::SeqCst);
        })
    });

    let d1: Arc<dyn BasicLifecyclerDelegate> = Arc::new(TokensPersistencyDelegate::new(
        &tokens_path,
        InstanceState::Active,
        base,
    ));
    let d2: Arc<dyn BasicLifecyclerDelegate> = Arc::new(LeaveOnStoppingDelegate::new(d1));
    let chain: Arc<dyn BasicLifecyclerDelegate> =
        Arc::new(AutoForgetDelegate::new(Duration::from_secs(60), d2));

    let cfg = prepare_basic_lifecycler_config();
    let (lifecycler, _store) = prepare_basic_lifecycler_with_delegate(cfg, chain, None);
    let svc = lifecycler.clone().into_service();

    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert_eq!(InstanceState::Joining, lifecycler.get_state());
    assert_eq!(vec![1, 2, 3, 4, 5], lifecycler.get_tokens());
    assert!(lifecycler.is_registered());

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    assert!(on_stopping_called.load(Ordering::SeqCst));

    let actual_tokens = load_tokens_from_file(&tokens_path).unwrap();
    assert_eq!(vec![1, 2, 3, 4, 5], actual_tokens);
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go::TestAutoForgetDelegate
#[tokio::test]
async fn test_auto_forget_delegate() {
    let forget_period = Duration::from_secs(60);
    let registered_at = SystemTime::now();

    struct Case {
        setup: Box<dyn Fn(&mut Desc) + Send + Sync>,
        expected_instances: Vec<&'static str>,
    }

    let cases = [
        Case {
            setup: Box::new(move |ring_desc| {
                ring_desc.add_ingester(
                    "instance-1",
                    "1.1.1.1",
                    "",
                    Vec::new(),
                    InstanceState::Active,
                    Some(registered_at),
                    false,
                    None,
                    HashMap::new(),
                );
            }),
            expected_instances: vec![TEST_INSTANCE_ID, "instance-1"],
        },
        Case {
            setup: Box::new(move |ring_desc| {
                let mut i = ring_desc.add_ingester(
                    "instance-1",
                    "1.1.1.1",
                    "",
                    Vec::new(),
                    InstanceState::Active,
                    Some(registered_at),
                    false,
                    None,
                    HashMap::new(),
                );
                i.timestamp = system_time_to_unix_secs(
                    SystemTime::now() - forget_period + Duration::from_secs(5),
                );
                ring_desc.ingesters.insert("instance-1".to_string(), i);
            }),
            expected_instances: vec![TEST_INSTANCE_ID, "instance-1"],
        },
        Case {
            setup: Box::new(move |ring_desc| {
                let mut i = ring_desc.add_ingester(
                    "instance-1",
                    "1.1.1.1",
                    "",
                    Vec::new(),
                    InstanceState::Active,
                    Some(registered_at),
                    false,
                    None,
                    HashMap::new(),
                );
                i.timestamp = system_time_to_unix_secs(
                    SystemTime::now() - forget_period - Duration::from_secs(5),
                );
                ring_desc.ingesters.insert("instance-1".to_string(), i);
            }),
            expected_instances: vec![TEST_INSTANCE_ID],
        },
    ];

    for tc in cases {
        let mut cfg = prepare_basic_lifecycler_config();
        cfg.heartbeat_period = Duration::from_millis(50);

        let reg = Registry::new();
        let test_delegate = Arc::new(MockDelegate::default());
        let auto_forget = Arc::new(AutoForgetDelegate::new(forget_period, test_delegate));
        let (lifecycler, store) =
            prepare_basic_lifecycler_with_delegate(cfg, auto_forget, Some(&reg));

        let setup = tc.setup;
        store
            .cas(
                TEST_RING_KEY,
                Box::new(move |_| {
                    let mut ring_desc = Desc::default();
                    setup(&mut ring_desc);
                    Ok(CasOutcome::Update(Box::new(ring_desc)))
                }),
            )
            .await
            .unwrap();

        let svc = lifecycler.clone().into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();

        let hb_ok = poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let r = reg.clone();
            async move { gather_metric(&r, "ring_member_heartbeats_total") > 0.0 }
        })
        .await;
        assert!(hb_ok);

        let v = store.get(TEST_RING_KEY).await.unwrap().unwrap();
        let desc = v.downcast_ref::<Desc>().unwrap();
        let mut actual: Vec<String> = desc.ingesters.keys().cloned().collect();
        actual.sort();
        let mut expected: Vec<String> = tc
            .expected_instances
            .iter()
            .map(|s| s.to_string())
            .collect();
        expected.sort();
        assert_eq!(expected, actual);

        stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_delegates_test.go::TestInstanceRegisterDelegate_OnRingInstanceRegister
#[test]
fn test_instance_register_delegate_on_ring_instance_register() {
    const TOKEN_COUNT: usize = 128;

    // no previous tokens, JOINING state
    {
        let delegate = Arc::new(InstanceRegisterDelegate::new(
            InstanceState::Joining,
            TOKEN_COUNT,
        ));
        let mut cfg = prepare_basic_lifecycler_config();
        cfg.heartbeat_period = Duration::from_millis(100);
        let (lifecycler, _store) =
            prepare_basic_lifecycler_with_delegate(cfg, delegate.clone(), None);

        let other_tokens = vec![100, 200, 300, 400, 500];
        let mut desc = Desc::default();
        desc.add_ingester(
            "other-instance",
            "addr",
            "zone",
            other_tokens.clone(),
            InstanceState::Active,
            Some(SystemTime::now()),
            false,
            None,
            HashMap::new(),
        );

        let (state, tokens) = delegate.on_ring_instance_register(
            &lifecycler,
            &desc,
            false,
            "test-instance",
            None,
        );
        assert_eq!(InstanceState::Joining, state);
        assert_eq!(TOKEN_COUNT, tokens.len());
        for tok in &other_tokens {
            assert!(!tokens.contains(tok));
        }
    }

    // with previous tokens, ACTIVE state
    {
        let delegate = Arc::new(InstanceRegisterDelegate::new(
            InstanceState::Active,
            TOKEN_COUNT,
        ));
        let mut cfg = prepare_basic_lifecycler_config();
        cfg.heartbeat_period = Duration::from_millis(100);
        let (lifecycler, _store) =
            prepare_basic_lifecycler_with_delegate(cfg, delegate.clone(), None);

        let other_tokens = vec![100, 200, 300, 400, 500];
        let mut desc = Desc::default();
        desc.add_ingester(
            "other-instance",
            "addr",
            "zone",
            other_tokens.clone(),
            InstanceState::Active,
            Some(SystemTime::now()),
            false,
            None,
            HashMap::new(),
        );

        let prev_tokens = vec![10, 20, 30];
        desc.add_ingester(
            "test-instance",
            "test-addr",
            "zone",
            prev_tokens.clone(),
            InstanceState::Joining,
            Some(SystemTime::now()),
            false,
            None,
            HashMap::new(),
        );

        let existing = desc.ingesters.get("test-instance").cloned();
        let (state, tokens) = delegate.on_ring_instance_register(
            &lifecycler,
            &desc,
            true,
            "test-instance",
            existing.as_ref(),
        );
        assert_eq!(InstanceState::Active, state);
        assert_eq!(TOKEN_COUNT, tokens.len());
        for tok in &prev_tokens {
            assert!(tokens.contains(tok));
        }
        for tok in &other_tokens {
            assert!(!tokens.contains(tok));
        }
    }
}

// ============================================================================
// basic_lifecycler_test.go (12 tests)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_GetTokenGenerator
#[test]
fn test_basic_lifecycler_get_token_generator() {
    let base_cfg = prepare_basic_lifecycler_config();
    let smt = Arc::new(
        SpreadMinimizingTokenGenerator::new(&base_cfg.id, &base_cfg.zone, &[zone(1), zone(2), zone(3)], true)
            .unwrap(),
    );

    let generators: Vec<(Option<Arc<dyn TokenGenerator>>, bool)> = vec![
        (None, false),
        (Some(Arc::new(RandomTokenGenerator::with_seed(42))), false),
        (Some(smt.clone()), true),
    ];

    for (gen_opt, expected_can_join) in generators {
        let mut cfg = base_cfg.clone();
        cfg.token_generator = gen_opt;
        let (lifecycler, _delegate, _store) = prepare_basic_lifecycler(cfg, None);
        assert_eq!(
            expected_can_join,
            lifecycler.token_generator().can_join_enabled()
        );
        if expected_can_join {
            assert_eq!(
                smt.generate_tokens(10, &[]),
                lifecycler.token_generator().generate_tokens(10, &[])
            );
        } else {
            assert_eq!(10, lifecycler.token_generator().generate_tokens(10, &[]).len());
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_RegisterOnStart
#[tokio::test]
async fn test_basic_lifecycler_register_on_start() {
    struct Case {
        initial_id: Option<&'static str>,
        initial_desc: Option<InstanceDesc>,
        register_state: InstanceState,
        register_tokens: Vec<u32>,
    }

    let now_minus_1h = system_time_to_unix_secs(SystemTime::now() - Duration::from_secs(3600));
    let now_secs = system_time_to_unix_secs(SystemTime::now());
    let mut versions_other = HashMap::new();
    versions_other.insert(1, 10);
    versions_other.insert(2, 40);

    let cases = [
        Case {
            initial_id: None,
            initial_desc: None,
            register_state: InstanceState::Active,
            register_tokens: vec![1, 2, 3, 4, 5],
        },
        Case {
            initial_id: Some("instance-1"),
            initial_desc: Some(InstanceDesc {
                addr: "1.1.1.1".to_string(),
                state: InstanceState::Active as i32,
                tokens: vec![6, 7, 8, 9, 10],
                registered_timestamp: now_minus_1h,
                ..Default::default()
            }),
            register_state: InstanceState::Active,
            register_tokens: vec![1, 2, 3, 4, 5],
        },
        Case {
            initial_id: Some(TEST_INSTANCE_ID),
            initial_desc: Some(InstanceDesc {
                addr: "1.1.1.1".to_string(),
                state: InstanceState::Active as i32,
                tokens: vec![6, 7, 8, 9, 10],
                registered_timestamp: now_minus_1h,
                versions: versions_other,
                ..Default::default()
            }),
            register_state: InstanceState::Joining,
            register_tokens: vec![1, 2, 3, 4, 5],
        },
        Case {
            initial_id: Some(TEST_INSTANCE_ID),
            initial_desc: Some(InstanceDesc {
                addr: "1.1.1.1".to_string(),
                state: InstanceState::Active as i32,
                tokens: vec![1, 2, 3, 4, 5],
                registered_timestamp: now_minus_1h,
                ..Default::default()
            }),
            register_state: InstanceState::Active,
            register_tokens: vec![1, 2, 3, 4, 5],
        },
        Case {
            initial_id: Some(TEST_INSTANCE_ID),
            initial_desc: Some(InstanceDesc {
                addr: "1.1.1.1".to_string(),
                state: InstanceState::Active as i32,
                tokens: vec![1, 2, 3, 4, 5],
                registered_timestamp: 0,
                ..Default::default()
            }),
            register_state: InstanceState::Active,
            register_tokens: vec![1, 2, 3, 4, 5],
        },
        Case {
            initial_id: Some(TEST_INSTANCE_ID),
            initial_desc: Some(InstanceDesc {
                addr: "1.1.1.1".to_string(),
                state: InstanceState::Active as i32,
                tokens: vec![1, 2, 3, 4, 5],
                registered_timestamp: now_minus_1h,
                read_only: true,
                read_only_updated_timestamp: now_secs,
                ..Default::default()
            }),
            register_state: InstanceState::Active,
            register_tokens: vec![1, 2, 3, 4, 5],
        },
    ];

    for tc in cases {
        let mut cfg = prepare_basic_lifecycler_config();
        cfg.versions.insert(1, 2);
        cfg.versions.insert(3, 4);

        let reg = Registry::new();
        let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg.clone(), Some(&reg));

        if let (Some(init_id), Some(desc)) = (tc.initial_id, tc.initial_desc.clone()) {
            let init_id = init_id.to_string();
            let d = desc.clone();
            store
                .cas(
                    TEST_RING_KEY,
                    Box::new(move |_| {
                        let mut ring_desc = Desc::default();
                        let ro_ts = if d.read_only_updated_timestamp > 0 {
                            Some(UNIX_EPOCH + Duration::from_secs(d.read_only_updated_timestamp as u64))
                        } else {
                            None
                        };
                        ring_desc.add_ingester(
                            &init_id,
                            &d.addr,
                            &d.zone,
                            d.tokens.clone(),
                            d.instance_state(),
                            d.get_registered_at(),
                            d.read_only,
                            ro_ts,
                            d.versions.clone(),
                        );
                        Ok(CasOutcome::Update(Box::new(ring_desc)))
                    }),
                )
                .await
                .unwrap();
        }

        let tc_init_id = tc.initial_id;
        let tc_init_desc = tc.initial_desc.clone();
        let reg_state = tc.register_state;
        let reg_tokens = tc.register_tokens.clone();
        delegate.set_on_register(move |lc, _ring_desc, exists, id, inst_desc| {
            assert_eq!(State::Starting, lc.service_state());
            assert!(!lc.is_registered());
            assert_eq!(TEST_INSTANCE_ID, id);
            if tc_init_id == Some(id) {
                assert!(exists);
                let d = inst_desc.expect("existing instance");
                let expected = tc_init_desc.as_ref().unwrap();
                assert_eq!(TEST_INSTANCE_ID, d.id);
                assert_eq!(expected.addr, d.addr);
                assert_eq!(expected.zone, d.zone);
                assert_eq!(expected.state, d.state);
                assert_eq!(expected.tokens, d.tokens);
                assert_eq!(expected.registered_timestamp, d.registered_timestamp);
                assert_eq!(expected.read_only, d.read_only);
                assert_eq!(
                    expected.read_only_updated_timestamp,
                    d.read_only_updated_timestamp
                );
                assert_eq!(expected.versions, d.versions);
            } else {
                assert!(!exists);
            }
            (reg_state, reg_tokens.clone())
        });

        let expected_ro = tc.initial_desc.as_ref().is_some_and(|d| d.read_only);
        let expected_ro_ts = tc
            .initial_desc
            .as_ref()
            .map(|d| d.read_only_updated_timestamp)
            .unwrap_or(0);

        assert_eq!(TEST_INSTANCE_ID, lifecycler.instance_id());
        assert_eq!(State::New, lifecycler.service_state());
        assert_eq!(InstanceState::Pending, lifecycler.get_state());
        assert!(lifecycler.get_tokens().is_empty());
        assert!(!lifecycler.is_registered());
        assert_eq!(0.0, gather_metric(&reg, "ring_member_tokens_owned"));
        assert_eq!(
            cfg.num_tokens as f64,
            gather_metric(&reg, "ring_member_tokens_to_own")
        );
        assert_eq!(0.0, gather_metric(&reg, "lifecycler_read_only"));
        assert!(lifecycler.get_registered_at().is_none());
        let (ro, ro_since) = lifecycler.get_read_only_state();
        assert!(!ro);
        assert!(ro_since.is_none());

        let svc = lifecycler.clone().into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();

        assert_eq!(State::Running, svc.state());
        assert_eq!(tc.register_state, lifecycler.get_state());
        assert_eq!(tc.register_tokens, lifecycler.get_tokens());
        assert!(lifecycler.is_registered());
        assert_eq!(
            cfg.num_tokens as f64,
            gather_metric(&reg, "ring_member_tokens_owned")
        );
        assert_eq!(
            cfg.num_tokens as f64,
            gather_metric(&reg, "ring_member_tokens_to_own")
        );
        let (ro, ro_since) = lifecycler.get_read_only_state();
        assert_eq!(expected_ro, ro);
        if expected_ro_ts > 0 {
            assert_eq!(expected_ro_ts, system_time_to_unix_secs(ro_since.unwrap()));
        } else {
            assert!(ro_since.is_none());
        }
        assert_eq!(
            if expected_ro { 1.0 } else { 0.0 },
            gather_metric(&reg, "lifecycler_read_only")
        );

        let inst_desc = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
            .await
            .expect("instance in ring");
        assert_eq!(TEST_INSTANCE_ID, inst_desc.id);
        assert_eq!(cfg.addr, inst_desc.addr);
        assert_eq!(tc.register_state as i32, inst_desc.state);
        assert_eq!(tc.register_tokens, inst_desc.tokens);
        assert_eq!(cfg.zone, inst_desc.zone);
        assert_eq!(expected_ro, inst_desc.read_only);
        assert_eq!(expected_ro_ts, inst_desc.read_only_updated_timestamp);
        assert_eq!(cfg.versions, inst_desc.versions);

        if tc.initial_id == Some(TEST_INSTANCE_ID) {
            assert_eq!(
                tc.initial_desc.as_ref().unwrap().registered_timestamp,
                inst_desc.registered_timestamp
            );
        } else {
            let now_s = system_time_to_unix_secs(SystemTime::now());
            assert!((now_s - inst_desc.registered_timestamp).abs() <= 2);
        }

        stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_UnregisterOnStop
#[tokio::test]
async fn test_basic_lifecycler_unregister_on_stop() {
    let cfg = prepare_basic_lifecycler_config();
    let reg = Registry::new();
    let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg.clone(), Some(&reg));

    delegate.set_on_register(|_, _, _, _, _| (InstanceState::Active, vec![1, 2, 3, 4, 5]));
    delegate.set_on_stopping(|lc| {
        Box::pin(async move {
            assert_eq!(State::Stopping, lc.service_state());
        })
    });

    let svc = lifecycler.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert_eq!(InstanceState::Active, lifecycler.get_state());
    assert_eq!(vec![1, 2, 3, 4, 5], lifecycler.get_tokens());
    assert!(lifecycler.is_registered());
    assert!(lifecycler.get_registered_at().is_some());
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_owned")
    );
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_to_own")
    );

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    assert_eq!(InstanceState::Pending, lifecycler.get_state());
    assert!(lifecycler.get_tokens().is_empty());
    assert!(!lifecycler.is_registered());
    assert!(lifecycler.get_registered_at().is_none());
    assert_eq!(0.0, gather_metric(&reg, "ring_member_tokens_owned"));
    assert_eq!(0.0, gather_metric(&reg, "ring_member_tokens_to_own"));

    assert!(get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
        .await
        .is_none());
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_KeepInTheRingOnStop
#[tokio::test]
async fn test_basic_lifecycler_keep_in_the_ring_on_stop() {
    let mut cfg = prepare_basic_lifecycler_config();
    cfg.keep_instance_in_the_ring_on_shutdown = true;

    let reg = Registry::new();
    let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg.clone(), Some(&reg));
    assert!(lifecycler.should_keep_instance_in_the_ring_on_shutdown());

    delegate.set_on_register(|_, _, _, _, _| (InstanceState::Active, vec![1, 2, 3, 4, 5]));
    delegate.set_on_stopping(|lc| {
        Box::pin(async move {
            lc.change_state_inner(InstanceState::Leaving).await.unwrap();
        })
    });

    let svc = lifecycler.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert_eq!(InstanceState::Active, lifecycler.get_state());
    assert_eq!(vec![1, 2, 3, 4, 5], lifecycler.get_tokens());
    assert!(lifecycler.is_registered());
    assert!(lifecycler.get_registered_at().is_some());
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_owned")
    );
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_to_own")
    );

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    assert_eq!(InstanceState::Leaving, lifecycler.get_state());
    assert_eq!(vec![1, 2, 3, 4, 5], lifecycler.get_tokens());
    assert!(lifecycler.is_registered());
    assert!(lifecycler.get_registered_at().is_some());
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_owned")
    );
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_to_own")
    );

    let inst = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
        .await
        .expect("instance kept in ring");
    assert_eq!(cfg.id, inst.id);
    assert_eq!(cfg.addr, inst.addr);
    assert_eq!(InstanceState::Leaving as i32, inst.state);
    assert_eq!(vec![1, 2, 3, 4, 5], inst.tokens);
    assert_eq!(cfg.zone, inst.zone);
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_UnregisterFromTheRingOnStop
#[tokio::test]
async fn test_basic_lifecycler_unregister_from_the_ring_on_stop() {
    let mut cfg = prepare_basic_lifecycler_config();
    cfg.keep_instance_in_the_ring_on_shutdown = true;

    let reg = Registry::new();
    let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg.clone(), Some(&reg));
    assert!(lifecycler.should_keep_instance_in_the_ring_on_shutdown());

    delegate.set_on_register(|_, _, _, _, _| (InstanceState::Active, vec![1, 2, 3, 4, 5]));
    delegate.set_on_stopping(|lc| {
        Box::pin(async move {
            lc.change_state_inner(InstanceState::Leaving).await.unwrap();
        })
    });

    let svc = lifecycler.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert_eq!(InstanceState::Active, lifecycler.get_state());
    assert_eq!(vec![1, 2, 3, 4, 5], lifecycler.get_tokens());
    assert!(lifecycler.is_registered());
    assert!(lifecycler.get_registered_at().is_some());
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_owned")
    );
    assert_eq!(
        cfg.num_tokens as f64,
        gather_metric(&reg, "ring_member_tokens_to_own")
    );

    lifecycler.set_keep_instance_in_the_ring_on_shutdown(false);

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    assert_ne!(InstanceState::Active, lifecycler.get_state());
    assert_ne!(InstanceState::Leaving, lifecycler.get_state());
    assert!(lifecycler.get_tokens().is_empty());
    assert!(!lifecycler.is_registered());
    assert!(lifecycler.get_registered_at().is_none());
    assert_eq!(0.0, gather_metric(&reg, "ring_member_tokens_owned"));
    assert_eq!(0.0, gather_metric(&reg, "ring_member_tokens_to_own"));

    assert!(get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
        .await
        .is_none());
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_HeartbeatWhileRunning
#[tokio::test]
async fn test_basic_lifecycler_heartbeat_while_running() {
    let mut cfg = prepare_basic_lifecycler_config();
    cfg.heartbeat_period = Duration::from_millis(10);

    let reg = Registry::new();
    let (lifecycler, _delegate, store) = prepare_basic_lifecycler(cfg, Some(&reg));
    let svc = lifecycler.into_service();

    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    let initial_ts = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
        .await
        .unwrap()
        .timestamp;

    let ok = poll(Duration::from_secs(2), Duration::from_millis(10), || {
        let store = Arc::clone(&store);
        async move {
            get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
                .await
                .is_some_and(|d| d.timestamp > initial_ts)
        }
    })
    .await;
    assert!(ok, "expected timestamp to be updated");
    assert!(gather_metric(&reg, "ring_member_heartbeats_total") > 0.0);

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_HeartbeatWhileStopping
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_basic_lifecycler_heartbeat_while_stopping() {
    let mut cfg = prepare_basic_lifecycler_config();
    cfg.heartbeat_period = Duration::from_millis(10);

    let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg, None);
    let svc = lifecycler.into_service();

    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    let on_stopping_called = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&on_stopping_called);
    let store_cb = Arc::clone(&store);
    delegate.set_on_stopping(move |_| {
        let flag = Arc::clone(&flag);
        let store_cb = Arc::clone(&store_cb);
        Box::pin(async move {
            store_cb
                .cas(
                    TEST_RING_KEY,
                    Box::new(|in_val| {
                        let mut ring_desc = in_val
                            .and_then(|a| a.downcast_ref::<Desc>())
                            .cloned()
                            .unwrap_or_default();
                        if let Some(inst) = ring_desc.ingesters.get_mut(TEST_INSTANCE_ID) {
                            inst.timestamp = 0;
                        }
                        Ok(CasOutcome::Update(Box::new(ring_desc)))
                    }),
                )
                .await
                .unwrap();

            let updated = poll(Duration::from_secs(1), Duration::from_millis(10), || {
                let s = Arc::clone(&store_cb);
                async move {
                    get_instance_from_store(s.as_ref(), TEST_INSTANCE_ID)
                        .await
                        .is_some_and(|d| d.timestamp != 0)
                }
            })
            .await;
            assert!(updated, "heartbeat must update timestamp while stopping");
            flag.store(true, Ordering::SeqCst);
        })
    });

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    assert!(on_stopping_called.load(Ordering::SeqCst));
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_HeartbeatAfterBackendReset
#[tokio::test]
async fn test_basic_lifecycler_heartbeat_after_backend_reset() {
    let mut cfg = prepare_basic_lifecycler_config();
    cfg.heartbeat_period = Duration::from_millis(10);

    let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg.clone(), None);
    let register_tokens = vec![1, 2, 3, 4, 5];
    let rt = register_tokens.clone();
    delegate.set_on_register(move |_, _, _, _, _| (InstanceState::Active, rt.clone()));

    let svc = lifecycler.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    let prev_registered_at = lifecycler.get_registered_at().unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    store
        .cas(
            TEST_RING_KEY,
            Box::new(|_| Ok(CasOutcome::Update(Box::new(Desc::default())))),
        )
        .await
        .unwrap();

    let restored = poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let s = Arc::clone(&store);
        let rt = register_tokens.clone();
        let cfg = cfg.clone();
        async move {
            get_instance_from_store(s.as_ref(), TEST_INSTANCE_ID)
                .await
                .is_some_and(|d| {
                    d.timestamp > 0
                        && d.instance_state() == InstanceState::Active
                        && d.tokens == rt
                        && d.addr == cfg.addr
                        && d.id == cfg.id
                })
        }
    })
    .await;
    assert!(restored);

    let desc = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
        .await
        .unwrap();
    assert!(desc.registered_timestamp > system_time_to_unix_secs(prev_registered_at));
    assert!(
        system_time_to_unix_secs(lifecycler.get_registered_at().unwrap())
            > system_time_to_unix_secs(prev_registered_at)
    );

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_ChangeState
#[tokio::test]
async fn test_basic_lifecycler_change_state() {
    let cfg = prepare_basic_lifecycler_config();
    let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg, None);
    delegate.set_on_register(|_, _, _, _, _| (InstanceState::Joining, vec![1, 2, 3, 4, 5]));

    let svc = lifecycler.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert_eq!(InstanceState::Joining, lifecycler.get_state());

    for state in [InstanceState::Active, InstanceState::Leaving] {
        lifecycler.change_state(state).await.unwrap();
        assert_eq!(state, lifecycler.get_state());
        let desc = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
            .await
            .expect("instance in ring");
        assert_eq!(state, desc.instance_state());
    }

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_ChangeReadOnlyState
#[tokio::test]
async fn test_basic_lifecycler_change_read_only_state() {
    let cfg = prepare_basic_lifecycler_config();
    let reg = Registry::new();
    let (lifecycler, _delegate, store) = prepare_basic_lifecycler(cfg, Some(&reg));

    let svc = lifecycler.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    // Default read-only state
    {
        let (ro, ro_since) = lifecycler.get_read_only_state();
        assert!(!ro);
        assert!(ro_since.is_none());
        assert_eq!(0.0, gather_metric(&reg, "lifecycler_read_only"));

        let desc = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
            .await
            .unwrap();
        let (ro, ro_since) = desc.get_read_only_state();
        assert!(!ro);
        assert!(ro_since.is_none());
    }

    // Change read-only to true
    {
        let ro_change = system_time_to_unix_secs(SystemTime::now());
        lifecycler.change_read_only_state(true).await.unwrap();
        assert_eq!(1.0, gather_metric(&reg, "lifecycler_read_only"));

        let desc = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
            .await
            .unwrap();
        let (ro, ro_since) = desc.get_read_only_state();
        assert!(ro);
        let since_s = system_time_to_unix_secs(ro_since.unwrap());
        assert!((since_s - ro_change).abs() <= 1);
    }

    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Change read-only to false
    {
        let (_, prev_ro_since) = lifecycler.get_read_only_state();
        let prev_ro_since = prev_ro_since.unwrap();

        lifecycler.change_read_only_state(false).await.unwrap();
        assert_eq!(0.0, gather_metric(&reg, "lifecycler_read_only"));

        let desc = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
            .await
            .unwrap();
        let (ro, ro_since) = desc.get_read_only_state();
        assert!(!ro);
        assert!(ro_since.unwrap() > prev_ro_since);
    }

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_TokensObservePeriod
#[tokio::test]
async fn test_basic_lifecycler_tokens_observe_period() {
    let mut cfg = prepare_basic_lifecycler_config();
    cfg.num_tokens = 5;
    cfg.tokens_observe_period = Duration::from_secs(1);

    let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg, None);
    delegate.set_on_register(|_, _, _, _, _| (InstanceState::Active, vec![1, 2, 3, 4, 5]));

    let svc = lifecycler.clone().into_service();
    svc.start_async(ServiceContext::background()).await.unwrap();

    // Wait until registered, then remove tokens 1, 2, 3 to simulate gossip conflict.
    let updated = poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let s = Arc::clone(&store);
        async move {
            let Some(desc) = get_instance_from_store(s.as_ref(), TEST_INSTANCE_ID).await else {
                return false;
            };
            s.cas(
                TEST_RING_KEY,
                Box::new(move |in_val| {
                    let mut ring_desc = in_val
                        .and_then(|a| a.downcast_ref::<Desc>())
                        .cloned()
                        .unwrap_or_default();
                    ring_desc.add_ingester(
                        TEST_INSTANCE_ID,
                        &desc.addr,
                        &desc.zone,
                        vec![4, 5],
                        desc.instance_state(),
                        Some(SystemTime::now()),
                        false,
                        None,
                        HashMap::new(),
                    );
                    Ok(CasOutcome::Update(Box::new(ring_desc)))
                }),
            )
            .await
            .is_ok()
        }
    })
    .await;
    assert!(updated);

    svc.await_running().await.unwrap();
    let toks = lifecycler.get_tokens();
    assert!(toks.contains(&4));
    assert!(toks.contains(&5));
    assert!(!toks.contains(&1));
    assert!(!toks.contains(&2));
    assert!(!toks.contains(&3));

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
}

// Upstream: github.com/grafana/dskit/ring/basic_lifecycler_test.go::TestBasicLifecycler_updateInstance_ShouldAddInstanceToTheRingIfDoesNotExistEvenIfNotChanged
#[tokio::test]
async fn test_basic_lifecycler_update_instance_should_add_instance_to_the_ring_if_does_not_exist_even_if_not_changed() {
    for read_only in [false, true] {
        let mut cfg = prepare_basic_lifecycler_config();
        cfg.heartbeat_period = Duration::from_secs(3600);
        cfg.versions.insert(1, 2);
        cfg.versions.insert(3, 5);

        let reg = Registry::new();
        let (lifecycler, delegate, store) = prepare_basic_lifecycler(cfg.clone(), Some(&reg));

        let register_tokens = vec![1, 2, 3, 4, 5];
        let rt = register_tokens.clone();
        delegate.set_on_register(move |_, _, _, _, _| (InstanceState::Active, rt.clone()));

        let svc = lifecycler.clone().into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();

        let expected_registered_at = lifecycler.get_registered_at().unwrap();
        if read_only {
            lifecycler.change_read_only_state(true).await.unwrap();
        }

        // Delete instance from ring to simulate backend reset.
        store
            .cas(
                TEST_RING_KEY,
                Box::new(|_| Ok(CasOutcome::Update(Box::new(Desc::default())))),
            )
            .await
            .unwrap();

        // No-op update_instance callback returning false; must re-add the instance anyway.
        lifecycler.update_instance(|_, _| false).await.unwrap();

        let desc = get_instance_from_store(store.as_ref(), TEST_INSTANCE_ID)
            .await
            .expect("instance re-added to ring");
        assert_eq!(cfg.id, desc.id);
        assert_eq!(InstanceState::Active, desc.instance_state());
        assert_eq!(register_tokens, desc.tokens);
        assert_eq!(cfg.addr, desc.addr);
        assert_eq!(
            system_time_to_unix_secs(expected_registered_at),
            desc.registered_timestamp
        );
        assert_eq!(
            system_time_to_unix_secs(expected_registered_at),
            system_time_to_unix_secs(desc.get_registered_at().unwrap())
        );
        let (ro_state, ro_since) = desc.get_read_only_state();
        assert_eq!(read_only, ro_state);
        assert_eq!(read_only, ro_since.is_some());
        assert_eq!(cfg.versions, desc.versions);
        assert_eq!(
            if read_only { 1.0 } else { 0.0 },
            gather_metric(&reg, "lifecycler_read_only")
        );

        stop_and_await_terminated(ServiceContext::background(), svc.as_ref()).await.unwrap();
    }
}
