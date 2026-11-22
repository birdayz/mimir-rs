//! Ported from github.com/grafana/dskit/ring/lifecycler_test.go (25 missing tests;
//! TestLifecycler_HeartbeatAfterBackendReset and TestLifecycler_ShouldHandleInstanceAbruptlyRestarted
//! are already in upstream_dskit_ring_tests.rs).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mimir_common::services::{
    start_and_await_running, stop_and_await_terminated, ServiceContext,
};
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{CasOutcome, KvClient};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig, NoopFlushTransferer};
use mimir_ring::ring::model::{ring_desc_codec, system_time_to_unix_secs, DescExt, InstanceDescExt};
use mimir_ring::token_gen::{
    RandomTokenGenerator, SpreadMinimizingTokenGenerator, TokenGenError, TokenGenerator,
    OPTIMAL_TOKENS_PER_INSTANCE,
};
use prometheus::Registry;

const RING_KEY: &str = "ring";

fn zone(id: usize) -> String {
    format!("zone{id}")
}

fn instance_name(instance_id: usize, zone_id: usize) -> String {
    format!("instance-{}-{instance_id}", zone(zone_id))
}

fn test_lifecycler_config(id: &str) -> LifecyclerConfig {
    LifecyclerConfig {
        addr: "0.0.0.0".to_string(),
        port: 1,
        num_tokens: 1,
        id: id.to_string(),
        zone: zone(1),
        final_sleep: Duration::ZERO,
        heartbeat_period: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_secs(60),
        observe_period: Duration::ZERO,
        join_after: Duration::ZERO,
        min_ready_duration: Duration::from_secs(15),
        unregister_on_shutdown: true,
        readiness_check_ring_health: true,
        ..Default::default()
    }
}

async fn get_instance_from_store(store: &dyn KvClient, key: &str, id: &str) -> Option<InstanceDesc> {
    let out = store.get(key).await.unwrap()?;
    out.downcast_ref::<Desc>()?.ingesters.get(id).cloned()
}

fn gather_metric_with_label(reg: &Registry, name: &str, label_name: &str, label_val: &str) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if m.get_label()
                    .iter()
                    .any(|lp| lp.get_name() == label_name && lp.get_value() == label_val)
                {
                    if m.has_gauge() {
                        return Some(m.get_gauge().get_value());
                    }
                    if m.has_counter() {
                        return Some(m.get_counter().get_value());
                    }
                }
            }
        }
    }
    None
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

async fn wait_ring_instance<F>(timeout: Duration, store: &dyn KvClient, key: &str, id: &str, mut check: F) -> bool
where
    F: FnMut(&InstanceDesc) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(inst) = get_instance_from_store(store, key, id).await {
            if check(&inst) {
                return true;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct SpreadMinimizingTokenGeneratorWithDelay {
    inner: SpreadMinimizingTokenGenerator,
    can_join_delay: Duration,
}

impl TokenGenerator for SpreadMinimizingTokenGeneratorWithDelay {
    fn generate_tokens(&self, requested_tokens_count: usize, all_taken_tokens: &[u32]) -> Vec<u32> {
        self.inner.generate_tokens(requested_tokens_count, all_taken_tokens)
    }

    fn can_join(&self, instances: &HashMap<String, InstanceDesc>) -> Result<(), TokenGenError> {
        std::thread::sleep(self.can_join_delay);
        self.inner.can_join(instances)
    }

    fn can_join_enabled(&self) -> bool {
        self.inner.can_join_enabled()
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecyclerConfig_Validate
#[test]
fn test_lifecycler_config_validate() {
    let mut cfg = test_lifecycler_config("instance-1");
    let path_to_tokens = "/path/to/tokens";
    cfg.tokens_file_path = path_to_tokens.to_string();

    cfg.validate().unwrap();
    assert_eq!(path_to_tokens, cfg.tokens_file_path);

    let smt = Arc::new(
        SpreadMinimizingTokenGenerator::new(&cfg.id, &cfg.zone, &[zone(1), zone(2), zone(3)], true)
            .unwrap(),
    );
    cfg.set_token_generator(smt);
    assert!(cfg.validate().is_err());
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_TokenGenerator
#[test]
fn test_lifecycler_token_generator() {
    let base_cfg = test_lifecycler_config("instance-1");
    let smt = Arc::new(
        SpreadMinimizingTokenGenerator::new(
            &base_cfg.id,
            &base_cfg.zone,
            &[zone(1), zone(2), zone(3)],
            true,
        )
        .unwrap(),
    );
    let rand_gen = Arc::new(RandomTokenGenerator::with_seed(12345));

    let tests: Vec<(Option<Arc<dyn TokenGenerator>>, bool)> = vec![
        (None, false),
        (Some(rand_gen), false),
        (Some(smt.clone()), true),
    ];

    for (tg, expected_can_join) in tests {
        let mut cfg = base_cfg.clone();
        cfg.token_generator = tg;
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let lc = Lifecycler::with_store(
            cfg,
            kv,
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            RING_KEY,
            true,
            None,
        );
        assert_eq!(expected_can_join, lc.token_generator().can_join_enabled());
        if expected_can_join {
            assert_eq!(
                smt.generate_tokens(10, &[]),
                lc.token_generator().generate_tokens(10, &[])
            );
        } else {
            assert_eq!(10, lc.token_generator().generate_tokens(10, &[]).len());
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_HealthyInstancesCount
#[tokio::test]
async fn test_lifecycler_healthy_instances_count() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let mut cfg1 = test_lifecycler_config("ing1");
    cfg1.heartbeat_period = Duration::from_millis(100);
    cfg1.join_after = Duration::from_millis(100);
    let lc1 = Lifecycler::with_store(
        cfg1,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    assert_eq!(0, lc1.healthy_instances_count());
    let svc1 = lc1.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc1.clone();
        async move { l.healthy_instances_count() == 1 }
    })
    .await);

    let mut cfg2 = test_lifecycler_config("ing2");
    cfg2.heartbeat_period = Duration::from_millis(100);
    cfg2.join_after = Duration::from_millis(100);
    let lc2 = Lifecycler::with_store(
        cfg2,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    assert_eq!(0, lc2.healthy_instances_count());
    let svc2 = lc2.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc2.clone();
        async move { l.healthy_instances_count() == 2 }
    })
    .await);
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc1.clone();
        async move { l.healthy_instances_count() == 2 }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_HealthyInstancesInZoneCount
#[tokio::test]
async fn test_lifecycler_healthy_instances_in_zone_count() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let make_lc = |id: &str, z: &str| {
        let mut cfg = test_lifecycler_config(id);
        cfg.heartbeat_period = Duration::from_millis(100);
        cfg.join_after = Duration::from_millis(100);
        cfg.zone = z.to_string();
        Lifecycler::with_store(
            cfg,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            RING_KEY,
            true,
            None,
        )
    };

    let lc1 = make_lc("ing1", "zone-a");
    assert_eq!(0, lc1.healthy_instances_in_zone_count());
    let svc1 = lc1.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc1.clone();
        async move { l.healthy_instances_in_zone_count() == 1 }
    })
    .await);

    let lc2 = make_lc("ing2", "zone-a");
    assert_eq!(0, lc2.healthy_instances_in_zone_count());
    let svc2 = lc2.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc2.clone();
        async move { l.healthy_instances_in_zone_count() == 2 }
    })
    .await);
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc1.clone();
        async move { l.healthy_instances_in_zone_count() == 2 }
    })
    .await);

    let lc3 = make_lc("ing3", "zone-b");
    assert_eq!(0, lc3.healthy_instances_in_zone_count());
    let svc3 = lc3.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc3.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc3.clone();
        async move { l.healthy_instances_in_zone_count() == 1 }
    })
    .await);
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc1.clone();
        async move { l.healthy_instances_in_zone_count() == 2 }
    })
    .await);
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc2.clone();
        async move { l.healthy_instances_in_zone_count() == 2 }
    })
    .await);

    for s in [svc3, svc2, svc1] {
        stop_and_await_terminated(ServiceContext::background(), s.as_ref())
            .await
            .unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_InstancesInZoneCount
#[tokio::test]
async fn test_lifecycler_instances_in_zone_count() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let cases = [
        ("zone-a", true, 1, 1, 1, 1, 1),
        ("zone-a", false, 2, 2, 1, 1, 1),
        ("zone-a", true, 3, 3, 2, 1, 2),
        ("zone-b", true, 1, 4, 3, 2, 1),
        ("zone-c", false, 1, 5, 3, 3, 0),
        ("zone-c", true, 2, 6, 4, 3, 1),
        ("zone-b", true, 2, 7, 5, 3, 2),
    ];

    let mut svcs = Vec::new();
    let mut expected_healthy_total = 0usize;
    for (idx, (z, healthy, exp_in_zone, exp_total, exp_healthy, exp_zones, exp_healthy_in_zone)) in
        cases.into_iter().enumerate()
    {
        let mut cfg = test_lifecycler_config(&format!("instance-{idx}"));
        cfg.heartbeat_period = Duration::from_millis(100);
        cfg.zone = z.to_string();
        if healthy {
            expected_healthy_total += 1;
            cfg.join_after = Duration::from_millis(100);
        } else {
            cfg.join_after = Duration::from_secs(60);
        }

        let lc = Lifecycler::with_store(
            cfg,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "instance",
            RING_KEY,
            true,
            None,
        );
        assert_eq!(0, lc.instances_in_zone_count());
        let svc = lc.clone().into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();
        svcs.push(svc);

        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc.clone();
            async move { l.healthy_instances_count() == expected_healthy_total }
        })
        .await);
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc.clone();
            async move { l.healthy_instances_in_zone_count() == exp_healthy_in_zone }
        })
        .await);

        assert_eq!(exp_in_zone, lc.instances_in_zone_count());
        assert_eq!(exp_total, lc.instances_count());
        assert_eq!(exp_healthy, lc.healthy_instances_count());
        assert_eq!(exp_zones, lc.zones_count());
        assert_eq!(exp_healthy_in_zone, lc.healthy_instances_in_zone_count());
    }

    for svc in svcs {
        stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_ZonesCount
#[tokio::test]
async fn test_lifecycler_zones_count() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let events = [("zone-a", 1), ("zone-b", 2), ("zone-a", 2), ("zone-c", 3)];
    let mut svcs = Vec::new();

    for (idx, (z, exp_zones)) in events.into_iter().enumerate() {
        let mut cfg = test_lifecycler_config(&format!("instance-{idx}"));
        cfg.heartbeat_period = Duration::from_millis(100);
        cfg.join_after = Duration::from_millis(100);
        cfg.zone = z.to_string();

        let lc = Lifecycler::with_store(
            cfg,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            RING_KEY,
            true,
            None,
        );
        assert_eq!(0, lc.zones_count());
        let svc = lc.clone().into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();
        svcs.push(svc);

        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc.clone();
            async move { l.healthy_instances_count() == idx + 1 }
        })
        .await);
        assert_eq!(exp_zones, lc.zones_count());
    }

    for svc in svcs {
        stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_Zones
#[tokio::test]
async fn test_lifecycler_zones() {
    // Subtest 1: instances joining different zones
    {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let events = [
            ("zone-a", vec!["zone-a"]),
            ("zone-b", vec!["zone-a", "zone-b"]),
            ("zone-a", vec!["zone-a", "zone-b"]),
            ("zone-c", vec!["zone-a", "zone-b", "zone-c"]),
        ];
        let mut svcs = Vec::new();
        for (idx, (z, exp)) in events.into_iter().enumerate() {
            let mut cfg = test_lifecycler_config(&format!("instance-{idx}"));
            cfg.heartbeat_period = Duration::from_millis(100);
            cfg.join_after = Duration::from_millis(100);
            cfg.zone = z.to_string();
            let lc = Lifecycler::with_store(
                cfg,
                Arc::clone(&kv),
                Some(Arc::new(NoopFlushTransferer)),
                "ingester",
                RING_KEY,
                true,
                None,
            );
            assert_eq!(0, lc.zones_count());
            let svc = lc.clone().into_service();
            start_and_await_running(ServiceContext::background(), svc.as_ref())
                .await
                .unwrap();
            svcs.push(svc);
            assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
                let l = lc.clone();
                async move { l.healthy_instances_count() == idx + 1 }
            })
            .await);
            assert_eq!(exp, lc.zones());
        }
        for s in svcs {
            stop_and_await_terminated(ServiceContext::background(), s.as_ref())
                .await
                .unwrap();
        }
    }

    // Subtest 2: instances leaving zones
    {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let mk = |id: &str, z: &str| {
            let mut cfg = test_lifecycler_config(id);
            cfg.heartbeat_period = Duration::from_millis(100);
            cfg.join_after = Duration::from_millis(100);
            cfg.zone = z.to_string();
            Lifecycler::with_store(
                cfg,
                Arc::clone(&kv),
                Some(Arc::new(NoopFlushTransferer)),
                "ingester",
                RING_KEY,
                true,
                None,
            )
        };
        let lc1 = mk("instance-1", "zone-a");
        let lc2 = mk("instance-2", "zone-b");
        let lc3 = mk("instance-3", "zone-c");
        let s1 = lc1.clone().into_service();
        let s2 = lc2.clone().into_service();
        let s3 = lc3.clone().into_service();
        for s in [&s1, &s2, &s3] {
            start_and_await_running(ServiceContext::background(), s.as_ref())
                .await
                .unwrap();
        }
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc1.clone();
            async move { l.healthy_instances_count() == 3 }
        })
        .await);
        assert_eq!(vec!["zone-a", "zone-b", "zone-c"], lc1.zones());
        assert_eq!(3, lc1.zones_count());

        stop_and_await_terminated(ServiceContext::background(), s2.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc1.clone();
            async move { l.zones() == vec!["zone-a", "zone-c"] }
        })
        .await);
        assert_eq!(2, lc1.zones_count());

        stop_and_await_terminated(ServiceContext::background(), s3.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc1.clone();
            async move { l.zones() == vec!["zone-a"] }
        })
        .await);
        assert_eq!(1, lc1.zones_count());

        stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
            .await
            .unwrap();
    }

    // Subtest 3: multiple instances per zone with some leaving
    {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let mk = |id: &str, z: &str| {
            let mut cfg = test_lifecycler_config(id);
            cfg.heartbeat_period = Duration::from_millis(100);
            cfg.join_after = Duration::from_millis(100);
            cfg.zone = z.to_string();
            Lifecycler::with_store(
                cfg,
                Arc::clone(&kv),
                Some(Arc::new(NoopFlushTransferer)),
                "ingester",
                RING_KEY,
                true,
                None,
            )
        };
        let lc1 = mk("instance-1", "zone-a");
        let lc2 = mk("instance-2", "zone-a");
        let lc3 = mk("instance-3", "zone-b");
        let s1 = lc1.clone().into_service();
        let s2 = lc2.clone().into_service();
        let s3 = lc3.clone().into_service();
        for s in [&s1, &s2, &s3] {
            start_and_await_running(ServiceContext::background(), s.as_ref())
                .await
                .unwrap();
        }
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc1.clone();
            async move { l.healthy_instances_count() == 3 }
        })
        .await);
        assert_eq!(vec!["zone-a", "zone-b"], lc1.zones());

        stop_and_await_terminated(ServiceContext::background(), s2.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc1.clone();
            async move { l.healthy_instances_count() == 2 }
        })
        .await);
        assert_eq!(vec!["zone-a", "zone-b"], lc1.zones());

        stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc3.clone();
            async move { l.zones() == vec!["zone-b"] }
        })
        .await);
        assert_eq!(1, lc3.zones_count());

        stop_and_await_terminated(ServiceContext::background(), s3.as_ref())
            .await
            .unwrap();
    }

    // Subtest 4: instances joining and leaving dynamically
    {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let mk = |id: &str, z: &str| {
            let mut cfg = test_lifecycler_config(id);
            cfg.heartbeat_period = Duration::from_millis(100);
            cfg.join_after = Duration::from_millis(100);
            cfg.zone = z.to_string();
            Lifecycler::with_store(
                cfg,
                Arc::clone(&kv),
                Some(Arc::new(NoopFlushTransferer)),
                "ingester",
                RING_KEY,
                true,
                None,
            )
        };
        let lc1 = mk("instance-1", "zone-a");
        let s1 = lc1.clone().into_service();
        start_and_await_running(ServiceContext::background(), s1.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc1.clone();
            async move { l.healthy_instances_count() == 1 }
        })
        .await);
        assert_eq!(vec!["zone-a"], lc1.zones());

        let lc2 = mk("instance-2", "zone-b");
        let s2 = lc2.clone().into_service();
        start_and_await_running(ServiceContext::background(), s2.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc1.clone();
            async move { l.healthy_instances_count() == 2 }
        })
        .await);
        assert_eq!(vec!["zone-a", "zone-b"], lc1.zones());

        stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc2.clone();
            async move { l.zones() == vec!["zone-b"] }
        })
        .await);
        assert_eq!(1, lc2.zones_count());

        let lc3 = mk("instance-3", "zone-a");
        let s3 = lc3.clone().into_service();
        start_and_await_running(ServiceContext::background(), s3.as_ref())
            .await
            .unwrap();
        assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
            let l = lc2.clone();
            async move { l.healthy_instances_count() == 2 }
        })
        .await);
        assert_eq!(vec!["zone-a", "zone-b"], lc2.zones());

        stop_and_await_terminated(ServiceContext::background(), s2.as_ref())
            .await
            .unwrap();
        stop_and_await_terminated(ServiceContext::background(), s3.as_ref())
            .await
            .unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_NilFlushTransferer
#[tokio::test]
async fn test_lifecycler_nil_flush_transferer() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let cfg = test_lifecycler_config("ing1");
    let lc = Lifecycler::with_store(cfg, Arc::clone(&kv), None, "ingester", RING_KEY, true, None);
    let svc = lc.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc.clone();
        async move { l.healthy_instances_count() == 1 }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert!(get_instance_from_store(kv.as_ref(), RING_KEY, "ing1")
        .await
        .is_none());
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_TwoRingsWithDifferentKeysOnTheSameKVStore
#[tokio::test]
async fn test_lifecycler_two_rings_with_different_keys_on_the_same_kv_store() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let lc1 = Lifecycler::with_store(
        test_lifecycler_config("instance-1"),
        Arc::clone(&kv),
        None,
        "service-1",
        "ring-1",
        true,
        None,
    );
    let svc1 = lc1.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc1.clone();
        async move { l.healthy_instances_count() == 1 }
    })
    .await);

    let lc2 = Lifecycler::with_store(
        test_lifecycler_config("instance-2"),
        Arc::clone(&kv),
        None,
        "service-2",
        "ring-2",
        true,
        None,
    );
    let svc2 = lc2.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc2.clone();
        async move { l.healthy_instances_count() == 1 }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_IncreasingTokensLeavingInstanceInTheRing
#[tokio::test]
async fn test_lifecycler_increasing_tokens_leaving_instance_in_the_ring() {
    const NUM_TOKENS: usize = 128;
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let mut cfg = test_lifecycler_config("ing1");
    cfg.num_tokens = NUM_TOKENS;

    let orig_tokens = RandomTokenGenerator::with_seed(99).generate_tokens(64, &[]);
    let orig_clone = orig_tokens.clone();
    let addr = format!("{}:{}", cfg.addr, cfg.port);
    let zone_str = cfg.zone.clone();
    kv.cas(
        RING_KEY,
        Box::new(move |_| {
            let mut ring_desc = Desc::default();
            ring_desc.add_ingester(
                "ing1",
                &addr,
                &zone_str,
                orig_clone.clone(),
                InstanceState::Leaving,
                Some(SystemTime::now()),
                false,
                None,
                HashMap::new(),
            );
            Ok(CasOutcome::Update(Box::new(ring_desc)))
        }),
    )
    .await
    .unwrap();

    let lc = Lifecycler::with_store(
        cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let svc = lc.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    let ok = poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        async move {
            let Some(d) = get_instance_from_store(kv.as_ref(), RING_KEY, "ing1").await else {
                return false;
            };
            d.instance_state() == InstanceState::Active && d.tokens.len() == NUM_TOKENS
        }
    })
    .await;
    assert!(ok);
    let ing_desc = get_instance_from_store(kv.as_ref(), RING_KEY, "ing1")
        .await
        .unwrap();

    let token_set: HashSet<u32> = ing_desc.tokens.iter().copied().collect();
    let orig_seen = orig_tokens.iter().filter(|t| token_set.contains(t)).count();
    assert_eq!(orig_tokens.len(), orig_seen);
    assert!(ing_desc.tokens.windows(2).all(|w| w[0] < w[1]));

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_ChangeReadOnlyState
#[tokio::test]
async fn test_lifecycler_change_read_only_state() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let reg = Registry::new();

    let make_lc = |id: &str, r: Option<&Registry>| {
        let mut cfg = test_lifecycler_config(id);
        cfg.heartbeat_period = Duration::from_millis(100);
        cfg.join_after = Duration::from_millis(100);
        Lifecycler::with_store(
            cfg,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            RING_KEY,
            true,
            r.map(|x| x as &dyn mimir_ring::kv::AsRegisterer),
        )
    };

    let lc1 = make_lc("ing1", Some(&reg));
    let svc1 = lc1.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();

    let lc2 = make_lc("ing2", None);
    let svc2 = lc2.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();

    assert_eq!(
        Some(0.0),
        gather_metric_with_label(&reg, "lifecycler_read_only", "name", "ingester")
    );

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l1 = lc1.clone();
        let l2 = lc2.clone();
        async move { l1.healthy_instances_count() == 2 && l2.healthy_instances_count() == 2 }
    })
    .await);

    let (ro, ts) = lc1.get_read_only_state();
    assert!(!ro);
    assert!(ts.is_none());

    lc1.change_read_only_state(true).await.unwrap();
    let (ro, ts) = lc1.get_read_only_state();
    assert!(ro);
    assert!(ts.is_some());

    assert_eq!(
        Some(1.0),
        gather_metric_with_label(&reg, "lifecycler_read_only", "name", "ingester")
    );

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l1 = lc1.clone();
        let l2 = lc2.clone();
        async move { l1.read_only_instances_count() == 1 && l2.read_only_instances_count() == 1 }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_StartingWithReadOnlyInstanceInRing
#[tokio::test]
async fn test_lifecycler_starting_with_read_only_instance_in_ring() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let make_lc = |id: &str| {
        let mut cfg = test_lifecycler_config(id);
        cfg.heartbeat_period = Duration::from_millis(100);
        cfg.join_after = Duration::from_millis(100);
        Lifecycler::with_store(
            cfg,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            RING_KEY,
            true,
            None,
        )
    };

    let lc1 = make_lc("ing1");
    let svc1 = lc1.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();

    lc1.change_read_only_state(true).await.unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l1 = lc1.clone();
        async move { l1.healthy_instances_count() == 1 && l1.read_only_instances_count() == 1 }
    })
    .await);

    let lc2 = make_lc("ing2");
    let svc2 = lc2.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l2 = lc2.clone();
        async move { l2.healthy_instances_count() == 2 && l2.read_only_instances_count() == 1 }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), svc2.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), svc1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_DecreasingTokensLeavingInstanceInTheRing
#[tokio::test]
async fn test_lifecycler_decreasing_tokens_leaving_instance_in_the_ring() {
    const NUM_TOKENS: usize = 64;
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let mut cfg = test_lifecycler_config("ing1");
    cfg.num_tokens = NUM_TOKENS;

    let orig_tokens = RandomTokenGenerator::with_seed(123).generate_tokens(128, &[]);
    let orig_clone = orig_tokens.clone();
    let addr = format!("{}:{}", cfg.addr, cfg.port);
    let zone_str = cfg.zone.clone();
    kv.cas(
        RING_KEY,
        Box::new(move |_| {
            let mut ring_desc = Desc::default();
            ring_desc.add_ingester(
                "ing1",
                &addr,
                &zone_str,
                orig_clone.clone(),
                InstanceState::Leaving,
                Some(SystemTime::now()),
                false,
                None,
                HashMap::new(),
            );
            Ok(CasOutcome::Update(Box::new(ring_desc)))
        }),
    )
    .await
    .unwrap();

    let lc = Lifecycler::with_store(
        cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let svc = lc.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        async move {
            let Some(d) = get_instance_from_store(kv.as_ref(), RING_KEY, "ing1").await else {
                return false;
            };
            d.id == "ing1" && d.instance_state() == InstanceState::Active && d.tokens.len() == NUM_TOKENS
        }
    })
    .await);

    let ing_desc = get_instance_from_store(kv.as_ref(), RING_KEY, "ing1")
        .await
        .unwrap();
    let orig_set: HashSet<u32> = orig_tokens.iter().copied().collect();
    let mut seen = HashSet::new();
    for tok in &ing_desc.tokens {
        assert!(seen.insert(*tok), "tokens are not unique");
        assert!(orig_set.contains(tok), "old tokens were not re-used");
    }
    assert!(ing_desc.tokens.windows(2).all(|w| w[0] < w[1]));

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestCheckReady_NoRingInKVStore
#[tokio::test]
async fn test_check_ready_no_ring_in_kv_store() {
    struct MockEmptyKv;
    impl KvClient for MockEmptyKv {
        fn list(&self, _prefix: &str) -> mimir_ring::kv::BoxFuture<'_, Result<Vec<String>, mimir_ring::kv::KvError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn get(&self, _key: &str) -> mimir_ring::kv::BoxFuture<'_, Result<Option<Box<dyn std::any::Any + Send + Sync>>, mimir_ring::kv::KvError>> {
            Box::pin(async { Ok(None) })
        }
        fn delete(&self, _key: &str) -> mimir_ring::kv::BoxFuture<'_, Result<(), mimir_ring::kv::KvError>> {
            Box::pin(async { Ok(()) })
        }
        fn cas(&self, _key: &str, _f: mimir_ring::kv::CasFn) -> mimir_ring::kv::BoxFuture<'_, Result<(), mimir_ring::kv::KvError>> {
            Box::pin(async { Ok(()) })
        }
        fn watch_key(&self, _key: &str, ctx: ServiceContext, _f: mimir_ring::kv::WatchKeyFn) -> mimir_ring::kv::BoxFuture<'_, ()> {
            Box::pin(async move { ctx.canceled().await })
        }
        fn watch_prefix(&self, _prefix: &str, ctx: ServiceContext, _f: mimir_ring::kv::WatchPrefixFn) -> mimir_ring::kv::BoxFuture<'_, ()> {
            Box::pin(async move { ctx.canceled().await })
        }
    }

    let kv: Arc<dyn KvClient> = Arc::new(MockEmptyKv);
    let mut cfg = test_lifecycler_config("ring1");
    cfg.min_ready_duration = Duration::from_nanos(1);

    let lc = Lifecycler::with_store(
        cfg,
        kv,
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let svc = lc.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    lc.set_tokens(vec![1]);

    let err = lc.check_ready().await.unwrap_err();
    assert!(err.contains("no ring returned from the KV store"), "got: {err}");

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestCheckReady_MinReadyDuration
#[tokio::test]
async fn test_check_ready_min_ready_duration() {
    for (min_ready_duration, expected_min_delay) in [
        (Duration::ZERO, Duration::ZERO),
        (Duration::from_secs(1), Duration::from_secs(1)),
    ] {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let mut cfg = test_lifecycler_config("instance-1");
        cfg.readiness_check_ring_health = false;
        cfg.min_ready_duration = min_ready_duration;

        let lc = Lifecycler::with_store(
            cfg.clone(),
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ring",
            RING_KEY,
            true,
            None,
        );
        let svc = lc.clone().into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();

        let start_time = Instant::now();
        assert!(wait_ring_instance(
            Duration::from_secs(3),
            kv.as_ref(),
            RING_KEY,
            "instance-1",
            |inst| inst.is_ready(SystemTime::now(), cfg.heartbeat_timeout).is_ok()
        )
        .await);

        if expected_min_delay.is_zero() {
            lc.check_ready().await.unwrap();
        } else {
            let ready = poll(Duration::from_secs(3), Duration::from_millis(20), || {
                let l = lc.clone();
                async move { l.check_ready().await.is_ok() }
            })
            .await;
            assert!(ready);
            assert!(start_time.elapsed() >= expected_min_delay);
        }

        stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestCheckReady_CheckRingHealth
#[tokio::test]
async fn test_check_ready_check_ring_health() {
    for (check_ring_health, first_join, second_join, min_ready, max_ready) in [
        (
            false,
            Duration::from_secs(1),
            Duration::from_secs(3),
            Duration::from_secs(1),
            Duration::from_secs(2),
        ),
        (
            true,
            Duration::from_secs(1),
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(4),
        ),
    ] {
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

        let mut cfg1 = test_lifecycler_config("instance-1");
        cfg1.readiness_check_ring_health = check_ring_health;
        cfg1.min_ready_duration = Duration::ZERO;
        cfg1.join_after = first_join;
        let l1 = Lifecycler::with_store(
            cfg1,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ring",
            RING_KEY,
            true,
            None,
        );
        let s1 = l1.clone().into_service();
        start_and_await_running(ServiceContext::background(), s1.as_ref())
            .await
            .unwrap();

        let mut cfg2 = test_lifecycler_config("instance-2");
        cfg2.readiness_check_ring_health = check_ring_health;
        cfg2.min_ready_duration = Duration::ZERO;
        cfg2.join_after = second_join;
        let l2 = Lifecycler::with_store(
            cfg2,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ring",
            RING_KEY,
            true,
            None,
        );
        let s2 = l2.clone().into_service();
        start_and_await_running(ServiceContext::background(), s2.as_ref())
            .await
            .unwrap();

        let start_time = Instant::now();
        assert!(wait_ring_instance(Duration::from_secs(3), kv.as_ref(), RING_KEY, "instance-1", |_| true).await);
        assert!(wait_ring_instance(Duration::from_secs(3), kv.as_ref(), RING_KEY, "instance-2", |_| true).await);

        let ready = poll(Duration::from_secs(5), Duration::from_millis(20), || {
            let l = l1.clone();
            async move { l.check_ready().await.is_ok() }
        })
        .await;
        assert!(ready);
        let elapsed = start_time.elapsed();
        assert!(elapsed >= min_ready, "elapsed {elapsed:?} < {min_ready:?}");
        assert!(elapsed <= max_ready, "elapsed {elapsed:?} > {max_ready:?}");

        stop_and_await_terminated(ServiceContext::background(), s2.as_ref())
            .await
            .unwrap();
        stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
            .await
            .unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestRestartIngester_NoUnregister_LongHeartbeat
#[tokio::test]
async fn test_restart_ingester_no_unregister_long_heartbeat() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let orig_tokens = RandomTokenGenerator::with_seed(77).generate_tokens(100, &[]);
    const ID: &str = "test";
    let registered_at = SystemTime::now() - Duration::from_secs(3600);

    let orig_clone = orig_tokens.clone();
    kv.cas(
        RING_KEY,
        Box::new(move |_| {
            let mut r = Desc::default();
            r.add_ingester(
                ID,
                "3.3.3.3:333",
                "old",
                orig_clone.clone(),
                InstanceState::Leaving,
                Some(registered_at),
                false,
                None,
                HashMap::new(),
            );
            Ok(CasOutcome::Update(Box::new(r)))
        }),
    )
    .await
    .unwrap();

    let cfg = LifecyclerConfig {
        addr: "1.1.1.1".to_string(),
        port: 111,
        zone: "new".to_string(),
        num_tokens: orig_tokens.len(),
        id: ID.to_string(),
        heartbeat_period: Duration::from_secs(300),
        min_ready_duration: Duration::ZERO,
        join_after: Duration::from_secs(60),
        unregister_on_shutdown: false,
        ..Default::default()
    };

    let lc = Lifecycler::with_store(
        cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "test",
        RING_KEY,
        false,
        None,
    );
    let svc = lc.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let l = lc.clone();
        async move { l.check_ready().await.is_ok() }
    })
    .await);

    assert_eq!(InstanceState::Active, lc.get_state());
    assert_eq!(orig_tokens, lc.get_tokens());
    assert_eq!(
        system_time_to_unix_secs(registered_at),
        system_time_to_unix_secs(lc.get_registered_at().unwrap())
    );

    let inst = get_instance_from_store(kv.as_ref(), RING_KEY, ID)
        .await
        .unwrap();
    assert_eq!(ID, inst.id);
    assert_eq!(InstanceState::Active, inst.instance_state());
    assert_eq!("1.1.1.1:111", inst.addr);
    assert_eq!("new", inst.zone);
    assert_eq!(system_time_to_unix_secs(registered_at), inst.registered_timestamp);

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestTokensOnDisk
#[tokio::test]
async fn test_tokens_on_disk() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let token_dir = tempfile::tempdir().unwrap();

    let mut cfg = test_lifecycler_config("ing1");
    cfg.num_tokens = 512;
    cfg.tokens_file_path = token_dir.path().join("tokens").to_string_lossy().to_string();

    let l1 = Lifecycler::with_store(
        cfg.clone(),
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let s1 = l1.clone().into_service();
    start_and_await_running(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        async move {
            let Some(v) = kv.get(RING_KEY).await.unwrap() else {
                return false;
            };
            let Some(desc) = v.downcast_ref::<Desc>() else {
                return false;
            };
            desc.ingesters.len() == 1
                && desc.ingesters.get("ing1").is_some_and(|i| {
                    i.instance_state() == InstanceState::Active && i.tokens.len() == 512
                })
        }
    })
    .await);
    let mut exp_tokens = get_instance_from_store(kv.as_ref(), RING_KEY, "ing1")
        .await
        .unwrap()
        .tokens;

    stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();

    cfg.id = "ing2".to_string();
    let l2 = Lifecycler::with_store(
        cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let s2 = l2.clone().into_service();
    start_and_await_running(ServiceContext::background(), s2.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        async move {
            let Some(v) = kv.get(RING_KEY).await.unwrap() else {
                return false;
            };
            let Some(desc) = v.downcast_ref::<Desc>() else {
                return false;
            };
            desc.ingesters.len() == 1
                && desc.ingesters.get("ing2").is_some_and(|i| {
                    i.instance_state() == InstanceState::Active && i.tokens.len() == 512
                })
        }
    })
    .await);
    let mut act_tokens = get_instance_from_store(kv.as_ref(), RING_KEY, "ing2")
        .await
        .unwrap()
        .tokens;

    exp_tokens.sort_unstable();
    act_tokens.sort_unstable();
    assert_eq!(exp_tokens, act_tokens);

    stop_and_await_terminated(ServiceContext::background(), s2.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestDeletePersistedTokensOnShutdown
#[tokio::test]
async fn test_delete_persisted_tokens_on_shutdown() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let token_dir = tempfile::tempdir().unwrap();

    let mut cfg = test_lifecycler_config("ing1");
    cfg.num_tokens = 512;
    cfg.tokens_file_path = token_dir.path().join("tokens").to_string_lossy().to_string();

    let l1 = Lifecycler::with_store(
        cfg.clone(),
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let s1 = l1.clone().into_service();
    start_and_await_running(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        async move {
            let Some(v) = kv.get(RING_KEY).await.unwrap() else {
                return false;
            };
            let Some(desc) = v.downcast_ref::<Desc>() else {
                return false;
            };
            desc.ingesters.len() == 1
                && desc.ingesters.get("ing1").is_some_and(|i| {
                    i.instance_state() == InstanceState::Active && i.tokens.len() == 512
                })
        }
    })
    .await);

    l1.set_clear_tokens_on_shutdown(true);
    stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();

    assert!(!std::path::Path::new(&cfg.tokens_file_path).exists());
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestJoinInLeavingState
#[tokio::test]
async fn test_join_in_leaving_state() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let mut cfg = test_lifecycler_config("ing1");
    cfg.num_tokens = 2;
    cfg.min_ready_duration = Duration::from_nanos(1);

    kv.cas(
        RING_KEY,
        Box::new(|_| {
            let mut r = Desc::default();
            r.ingesters.insert(
                "ing1".to_string(),
                InstanceDesc {
                    state: InstanceState::Leaving as i32,
                    tokens: vec![1, 4],
                    ..Default::default()
                },
            );
            r.ingesters.insert(
                "ing2".to_string(),
                InstanceDesc {
                    tokens: vec![2, 3],
                    ..Default::default()
                },
            );
            Ok(CasOutcome::Update(Box::new(r)))
        }),
    )
    .await
    .unwrap();

    let l1 = Lifecycler::with_store(
        cfg.clone(),
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let s1 = l1.into_service();
    start_and_await_running(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        async move {
            let Some(v) = kv.get(RING_KEY).await.unwrap() else {
                return false;
            };
            let Some(desc) = v.downcast_ref::<Desc>() else {
                return false;
            };
            desc.ingesters.len() == 2
                && desc.ingesters.get("ing1").is_some_and(|i| {
                    i.instance_state() == InstanceState::Active && i.tokens.len() == 2
                })
                && desc.ingesters.get("ing2").is_some_and(|i| i.tokens.len() == 2)
        }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestJoinInJoiningState
#[tokio::test]
async fn test_join_in_joining_state() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let mut cfg = test_lifecycler_config("ing1");
    cfg.num_tokens = 2;
    cfg.min_ready_duration = Duration::from_nanos(1);

    let ing1_reg = system_time_to_unix_secs(SystemTime::now() - Duration::from_secs(3600));
    let ing2_reg = system_time_to_unix_secs(SystemTime::now() - Duration::from_secs(7200));

    kv.cas(
        RING_KEY,
        Box::new(move |_| {
            let mut r = Desc::default();
            r.ingesters.insert(
                "ing1".to_string(),
                InstanceDesc {
                    state: InstanceState::Joining as i32,
                    tokens: vec![1, 4],
                    registered_timestamp: ing1_reg,
                    ..Default::default()
                },
            );
            r.ingesters.insert(
                "ing2".to_string(),
                InstanceDesc {
                    tokens: vec![2, 3],
                    registered_timestamp: ing2_reg,
                    ..Default::default()
                },
            );
            Ok(CasOutcome::Update(Box::new(r)))
        }),
    )
    .await
    .unwrap();

    let l1 = Lifecycler::with_store(
        cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let s1 = l1.into_service();
    start_and_await_running(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        async move {
            let Some(v) = kv.get(RING_KEY).await.unwrap() else {
                return false;
            };
            let Some(desc) = v.downcast_ref::<Desc>() else {
                return false;
            };
            desc.ingesters.len() == 2
                && desc.ingesters.get("ing1").is_some_and(|i| {
                    i.instance_state() == InstanceState::Active
                        && i.tokens.len() == 2
                        && i.registered_timestamp == ing1_reg
                })
                && desc.ingesters.get("ing2").is_some_and(|i| {
                    i.tokens.len() == 2 && i.registered_timestamp == ing2_reg
                })
        }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestWaitBeforeJoining
#[tokio::test]
async fn test_wait_before_joining() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    kv.cas(
        RING_KEY,
        Box::new(|_| {
            let mut r = Desc::default();
            r.ingesters.insert(
                instance_name(0, 1),
                InstanceDesc {
                    state: InstanceState::Active as i32,
                    tokens: vec![1, 2, 3],
                    ..Default::default()
                },
            );
            r.ingesters.insert(
                instance_name(1, 1),
                InstanceDesc {
                    state: InstanceState::Active as i32,
                    tokens: vec![4, 5, 6],
                    ..Default::default()
                },
            );
            Ok(CasOutcome::Update(Box::new(r)))
        }),
    )
    .await
    .unwrap();

    let target_zone = zone(1);
    let smt_zones = vec![zone(1), zone(2), zone(3)];
    let can_join_timeout = Duration::from_secs(5);
    let make_smt = |id: &str, can_join: bool| -> Arc<dyn TokenGenerator> {
        Arc::new(
            SpreadMinimizingTokenGenerator::new(id, &target_zone, &smt_zones, can_join).unwrap(),
        )
    };

    let cases: Vec<(String, Arc<dyn TokenGenerator>, bool, bool)> = vec![
        (
            instance_name(3, 1),
            Arc::new(RandomTokenGenerator::with_seed(1)),
            false,
            false,
        ),
        (
            instance_name(3, 1),
            make_smt(&instance_name(2, 1), false),
            false,
            false,
        ),
        (
            instance_name(0, 1),
            make_smt(&instance_name(0, 1), true),
            false,
            false,
        ),
        (
            instance_name(3, 1),
            make_smt(&instance_name(3, 1), true),
            true,
            false,
        ),
        (
            instance_name(2, 1),
            make_smt(&instance_name(2, 1), true),
            false,
            false,
        ),
        (
            instance_name(3, 1),
            make_smt(&instance_name(3, 1), true),
            false,
            true,
        ),
    ];

    for (target_id, tg, timeout_required, error_required) in cases {
        let ctx = ServiceContext::background();
        let mut cfg = test_lifecycler_config(&target_id);
        cfg.num_tokens = OPTIMAL_TOKENS_PER_INSTANCE;
        cfg.token_generator = Some(tg);

        let lc = Lifecycler::with_store(
            cfg,
            Arc::clone(&kv),
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            RING_KEY,
            true,
            None,
        );
        lc.set_can_join_timeout(can_join_timeout);
        lc.init_ring().await.unwrap();

        let start = Instant::now();
        if error_required {
            ctx.cancel();
        }
        let res = lc.wait_before_joining(&ctx).await;

        if error_required {
            assert!(res.is_err());
        } else {
            res.unwrap();
        }

        if timeout_required {
            assert!(start.elapsed() >= can_join_timeout);
        } else {
            assert!(start.elapsed() < can_join_timeout);
        }
        ctx.cancel();
    }
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestAutoJoinWithSpreadMinimizingTokenGenerator
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_auto_join_with_spread_minimizing_token_generator() {
    let can_join_delay = Duration::from_secs(1);
    let can_join_timeout = Duration::from_secs(5);
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));

    let first_id = instance_name(0, 1);
    let second_id = instance_name(1, 1);

    let first_reg_ts = Arc::new(parking_lot::Mutex::new(None::<Instant>));
    let second_reg_ts = Arc::new(parking_lot::Mutex::new(None::<Instant>));

    let kv_poll = Arc::clone(&kv);
    let f_id = first_id.clone();
    let s_id = second_id.clone();
    let f_ts = Arc::clone(&first_reg_ts);
    let s_ts = Arc::clone(&second_reg_ts);
    let watcher_handle = tokio::spawn(async move {
        let first_done = AtomicBool::new(false);
        let second_done = AtomicBool::new(false);
        let ok = poll(Duration::from_secs(20), Duration::from_millis(20), || {
            let kv = Arc::clone(&kv_poll);
            let f_id = f_id.clone();
            let s_id = s_id.clone();
            let f_ts = Arc::clone(&f_ts);
            let s_ts = Arc::clone(&s_ts);
            let f_done = &first_done;
            let s_done = &second_done;
            async move {
                let Some(v) = kv.get(RING_KEY).await.unwrap() else {
                    return false;
                };
                let Some(desc) = v.downcast_ref::<Desc>() else {
                    return false;
                };
                if let Some(inst) = desc.ingesters.get(&f_id) {
                    if inst.instance_state() == InstanceState::Active
                        && !inst.tokens.is_empty()
                        && f_done
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        *f_ts.lock() = Some(Instant::now());
                    }
                }
                if let Some(inst) = desc.ingesters.get(&s_id) {
                    if inst.instance_state() == InstanceState::Active
                        && !inst.tokens.is_empty()
                        && s_done
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        *s_ts.lock() = Some(Instant::now());
                    }
                }
                f_done.load(Ordering::SeqCst) && s_done.load(Ordering::SeqCst)
            }
        })
        .await;
        assert!(ok, "both instances must become active with tokens");
    });

    let zone_id = zone(1);
    let smt_zones = vec![zone(1), zone(2), zone(3)];

    let tg_delay = Arc::new(SpreadMinimizingTokenGeneratorWithDelay {
        inner: SpreadMinimizingTokenGenerator::new(&first_id, &zone_id, &smt_zones, true).unwrap(),
        can_join_delay,
    });
    let mut first_cfg = test_lifecycler_config(&first_id);
    first_cfg.num_tokens = OPTIMAL_TOKENS_PER_INSTANCE;
    first_cfg.token_generator = Some(tg_delay);
    let first_lc = Lifecycler::with_store(
        first_cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let first_svc = first_lc.into_service();
    start_and_await_running(ServiceContext::background(), first_svc.as_ref())
        .await
        .unwrap();

    let tg_second = Arc::new(
        SpreadMinimizingTokenGenerator::new(&second_id, &zone_id, &smt_zones, true).unwrap(),
    );
    let mut second_cfg = test_lifecycler_config(&second_id);
    second_cfg.num_tokens = OPTIMAL_TOKENS_PER_INSTANCE;
    second_cfg.token_generator = Some(tg_second);
    let second_lc = Lifecycler::with_store(
        second_cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    second_lc.set_can_join_timeout(can_join_timeout);
    let second_svc = second_lc.into_service();
    start_and_await_running(ServiceContext::background(), second_svc.as_ref())
        .await
        .unwrap();

    watcher_handle.await.unwrap();

    let t1 = first_reg_ts.lock().unwrap();
    let t2 = second_reg_ts.lock().unwrap();
    assert!(t1 < t2, "first instance must register before second");

    stop_and_await_terminated(ServiceContext::background(), second_svc.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), first_svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestRestoreOfZoneWhenOverwritten
#[tokio::test]
async fn test_restore_of_zone_when_overwritten() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let cfg = test_lifecycler_config("ing1");

    kv.cas(
        RING_KEY,
        Box::new(|_| {
            let mut r = Desc::default();
            r.ingesters.insert(
                "ing1".to_string(),
                InstanceDesc {
                    state: InstanceState::Active as i32,
                    addr: "0.0.0.0".to_string(),
                    tokens: vec![1, 4],
                    ..Default::default()
                },
            );
            r.ingesters.insert(
                "ing2".to_string(),
                InstanceDesc {
                    tokens: vec![2, 3],
                    ..Default::default()
                },
            );
            Ok(CasOutcome::Update(Box::new(r)))
        }),
    )
    .await
    .unwrap();

    let l1 = Lifecycler::with_store(
        cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let expected_zone = l1.zone.clone();
    let s1 = l1.into_service();
    start_and_await_running(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(20), || {
        let kv = Arc::clone(&kv);
        let z = expected_zone.clone();
        async move {
            let Some(v) = kv.get(RING_KEY).await.unwrap() else {
                return false;
            };
            let Some(desc) = v.downcast_ref::<Desc>() else {
                return false;
            };
            desc.ingesters.len() == 2
                && desc.ingesters.get("ing1").is_some_and(|i| i.zone == z)
                && desc.ingesters.get("ing2").is_some_and(|i| i.zone.is_empty())
        }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), s1.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestDefaultFinalSleepValue
#[test]
fn test_default_final_sleep_value() {
    // default value is 0
    let cfg = LifecyclerConfig::default();
    assert_eq!(Duration::ZERO, cfg.final_sleep);

    // default value is overridable
    let cfg_override = LifecyclerConfig {
        final_sleep: Duration::from_secs(60),
        ..Default::default()
    };
    assert_eq!(Duration::from_secs(60), cfg_override.final_sleep);
}
