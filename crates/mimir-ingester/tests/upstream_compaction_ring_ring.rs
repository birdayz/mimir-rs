//! Ported from:
//! - github.com/grafana/mimir/pkg/ingester/ingester_partition_ring_test.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_ring_test.go
//! - github.com/grafana/mimir/pkg/ingester/ingester_ring_lifecycler_test.go

mod compaction_ring_helpers;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mimir_common::services::{Service, ServiceContext};
use mimir_config::{GoDuration, PartitionRingConfig, RingConfig, StringSliceCSV};
use mimir_ingester::ingester_ring_lifecycler::{FlushFn, IngesterLifecycler, TokenlessLifecycler};
use mimir_proto::ringpb::{InstanceDesc, InstanceState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{BoxFuture, KvClient};
use mimir_ring::lifecycler::{
    BasicLifecyclerConfig, FlushTransferer, Lifecycler, LifecyclerConfig, LifecyclerError,
    NoopFlushTransferer,
};
use mimir_ring::partition_ring::PartitionInstanceLifecyclerConfig;
use mimir_ring::ring::model::ring_desc_codec;
use mimir_ring::ring::{to_status_page_config, Ring};
use mimir_ring::token_gen::SpreadMinimizingTokenGenerator;
use parking_lot::Mutex;
use prometheus::Registry;

use compaction_ring_helpers::{
    create_test_ring, get_instance_state_in_ring, get_instance_token_count, poll_until,
    INGEST_RING_KEY, INGEST_RING_NAME,
};

const INSTANCE_ID: &str = "instance-10";
const INSTANCE_ZONE: &str = "zone-a";
const WRONG_ZONE: &str = "zone-d";

// Upstream: pkg/ingester/ingester_partition_ring_test.go::TestPartitionRingConfig_ToLifecyclerConfig
#[test]
fn test_partition_ring_config_to_lifecycler_config() {
    let cfg = PartitionRingConfig {
        min_partition_owners_count: 2,
        min_partition_owners_duration: GoDuration::from_mins(1),
        delete_inactive_partition_after: GoDuration::from_hours(1),
        create_partitions_with_derived_tokens: true,
        max_derived_token_partitions: 128,
        ..PartitionRingConfig::default()
    };

    let expected = PartitionInstanceLifecyclerConfig {
        partition_id: 3,
        instance_id: "ingester-zone-a-3".to_string(),
        multi_partition_ownership: false,
        wait_owners_count_on_pending: 2,
        wait_owners_duration_on_pending: Duration::from_secs(60),
        delete_inactive_partition_after: Duration::from_secs(3600),
        polling_interval: Duration::from_secs(1),
        create_partitions_with_derived_tokens: true,
        max_derived_token_partitions: 128,
    };

    let actual = PartitionInstanceLifecyclerConfig::from_partition_ring_config(
        &cfg,
        3,
        "ingester-zone-a-3",
        Duration::from_secs(1),
    );
    assert_eq!(expected, actual);
}

// Upstream: pkg/ingester/ingester_ring_test.go::TestRingConfig_DefaultConfig
#[test]
fn test_ring_config_default_config() {
    let cfg = RingConfig::default();

    // 1. ToLifecyclerConfig()
    let lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 0).expect("default lc config");
    assert_eq!(lc_cfg.num_tokens, cfg.num_tokens as usize);
    assert_eq!(lc_cfg.min_ready_duration, Duration::from_secs(15));
    assert_eq!(lc_cfg.final_sleep, Duration::ZERO);
    assert!(!lc_cfg.readiness_check_ring_health);
    assert_eq!(lc_cfg.heartbeat_period, Duration::from_secs(15));
    assert_eq!(lc_cfg.heartbeat_timeout, Duration::from_secs(60));
    assert!(lc_cfg.unregister_on_shutdown);
    assert!(!lc_cfg.token_generator.as_ref().unwrap().can_join_enabled());
    assert_eq!(cfg.kvstore.store, "memberlist");
    assert_eq!(cfg.replication_factor, 3);

    // 2. ToTokenlessBasicLifecyclerConfig() should match ToLifecyclerConfig()
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let expected_lc = Lifecycler::with_store(
        lc_cfg.clone(),
        kv,
        None,
        INGEST_RING_NAME,
        INGEST_RING_KEY,
        false,
        None,
    );

    let basic_cfg =
        BasicLifecyclerConfig::from_ring_config_tokenless(&cfg, 0).expect("tokenless basic cfg");
    assert_eq!(expected_lc.id, basic_cfg.id);
    assert_eq!(expected_lc.addr, basic_cfg.addr);
    assert_eq!(expected_lc.zone, basic_cfg.zone);
    assert_eq!(lc_cfg.heartbeat_period, basic_cfg.heartbeat_period);
    assert_eq!(lc_cfg.heartbeat_timeout, basic_cfg.heartbeat_timeout);
    assert_eq!(
        !lc_cfg.unregister_on_shutdown,
        basic_cfg.keep_instance_in_the_ring_on_shutdown
    );

    // 3. ToTokenlessBasicLifecyclerConfig()
    assert_eq!(basic_cfg.num_tokens, 0);
    assert_eq!(basic_cfg.tokens_observe_period, Duration::ZERO);
    assert_eq!(
        !cfg.unregister_on_shutdown,
        basic_cfg.keep_instance_in_the_ring_on_shutdown
    );
}

// Upstream: pkg/ingester/ingester_ring_test.go::TestRingConfig_CustomConfig
#[test]
fn test_ring_config_custom_config() {
    let cfg = RingConfig {
        heartbeat_period: GoDuration::from_secs(1),
        heartbeat_timeout: GoDuration::from_secs(10),
        replication_factor: 10,
        zone_awareness_enabled: true,
        excluded_zones: StringSliceCSV(vec!["zone-a".into(), "zone-b".into()]),
        num_tokens: 1234,
        instance_id: "instance-10".to_string(),
        instance_interface_names: vec!["abc".to_string()],
        instance_port: 1111,
        instance_addr: "1.2.3.4".to_string(),
        instance_availability_zone: "zone-c".to_string(),
        unregister_on_shutdown: true,
        observe_period: GoDuration::from_mins(10),
        min_ready_duration: GoDuration::from_mins(3),
        final_sleep: GoDuration::from_mins(2),
        token_generation_strategy: "spread-minimizing".to_string(),
        spread_minimizing_zones: StringSliceCSV(vec![
            "zone-a".into(),
            "zone-b".into(),
            "zone-c".into(),
        ]),
        spread_minimizing_join_ring_in_order: true,
        ..Default::default()
    };
    cfg.validate().expect("custom config should validate");

    // 1. ToLifecyclerConfig()
    let result = LifecyclerConfig::from_ring_config(&cfg, 10).expect("custom lc config");
    assert_eq!(result.num_tokens, 1234);
    assert_eq!(result.heartbeat_period, Duration::from_secs(1));
    assert_eq!(result.heartbeat_timeout, Duration::from_secs(10));
    assert_eq!(result.observe_period, Duration::from_secs(600));
    assert_eq!(result.join_after, Duration::ZERO);
    assert_eq!(result.min_ready_duration, Duration::from_secs(180));
    assert_eq!(result.final_sleep, Duration::from_secs(120));
    assert_eq!(result.zone, "zone-c");
    assert!(result.unregister_on_shutdown);
    assert!(!result.readiness_check_ring_health);
    assert_eq!(result.addr, "1.2.3.4");
    assert_eq!(result.port, 1111);
    assert_eq!(result.id, "instance-10");
    assert!(result.token_generator.as_ref().unwrap().can_join_enabled());

    // 2. ToTokenlessBasicLifecyclerConfig() should match ToLifecyclerConfig()
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let expected_lc = Lifecycler::with_store(
        result.clone(),
        kv,
        None,
        INGEST_RING_NAME,
        INGEST_RING_KEY,
        false,
        None,
    );

    let basic_cfg =
        BasicLifecyclerConfig::from_ring_config_tokenless(&cfg, 10).expect("tokenless basic cfg");
    assert_eq!(expected_lc.id, basic_cfg.id);
    assert_eq!(expected_lc.addr, basic_cfg.addr);
    assert_eq!(expected_lc.zone, basic_cfg.zone);
    assert_eq!(result.heartbeat_period, basic_cfg.heartbeat_period);
    assert_eq!(result.heartbeat_timeout, basic_cfg.heartbeat_timeout);
    assert_eq!(
        !result.unregister_on_shutdown,
        basic_cfg.keep_instance_in_the_ring_on_shutdown
    );

    // 3. ToTokenlessBasicLifecyclerConfig()
    assert_eq!(basic_cfg.num_tokens, 0);
    assert_eq!(basic_cfg.tokens_observe_period, Duration::ZERO);
    assert_eq!(
        !cfg.unregister_on_shutdown,
        basic_cfg.keep_instance_in_the_ring_on_shutdown
    );
}

// Upstream: pkg/ingester/ingester_ring_test.go::TestRingConfig_StatusPageIngesterLink
#[test]
fn test_ring_config_status_page_ingester_link() {
    let default_cfg = RingConfig::default();
    let default_template = default_cfg.status_page_ingester_link_template.clone();

    let cases = [
        (
            "default template",
            default_template.as_str(),
            "10.0.0.1:9095",
            "http://10.0.0.1:8080/ingester/tenants",
        ),
        (
            "default template with IPv6 address",
            default_template.as_str(),
            "[2001:db8::1]:9095",
            "http://[2001:db8::1]:8080/ingester/tenants",
        ),
        (
            "default template with address without port",
            default_template.as_str(),
            "ingester-zone-a-1.ingester",
            "http://ingester-zone-a-1.ingester:8080/ingester/tenants",
        ),
        (
            "ingester ID template",
            "../{ingester}/ingester/tenants",
            "10.0.0.1:9095",
            "../ingester-zone-a-1/ingester/tenants",
        ),
        ("empty template", "", "10.0.0.1:9095", ""),
    ];

    for (name, template, addr, expected_link) in cases {
        let cfg = RingConfig {
            status_page_ingester_link_template: template.to_string(),
            ..Default::default()
        };
        let sp = to_status_page_config(&cfg);
        let inst = InstanceDesc {
            addr: addr.to_string(),
            ..Default::default()
        };
        let actual = (sp.instance_link.as_ref().unwrap())("ingester-zone-a-1", &inst);
        assert_eq!(actual, expected_link, "case {name}");
    }
}

// Upstream: pkg/ingester/ingester_ring_test.go::TestRingConfig_Validate
#[test]
fn test_ring_config_validate() {
    let spread_zones = vec!["zone-a".to_string(), "zone-b".to_string(), "zone-c".to_string()];

    struct Case<'a> {
        name: &'a str,
        num_tokens: i64,
        instance_id: &'a str,
        zone: &'a str,
        strategy: &'a str,
        join_in_order: bool,
        zones: Vec<String>,
        tokens_file_path: &'a str,
        expected_err: Option<&'a str>,
    }

    let cases = [
        Case {
            name: "spread-minimizing and correct zones pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "spread-minimizing",
            join_in_order: false,
            zones: spread_zones.clone(),
            tokens_file_path: "",
            expected_err: None,
        },
        Case {
            name: "spread-minimizing and a wrong instance-id don't pass validation",
            num_tokens: 128,
            instance_id: "broken-instance-48pgb",
            zone: INSTANCE_ZONE,
            strategy: "spread-minimizing",
            join_in_order: false,
            zones: spread_zones.clone(),
            tokens_file_path: "",
            expected_err: Some("\"spread-minimizing\" token generation strategy is misconfigured: unable to extract instance id"),
        },
        Case {
            name: "spread-minimizing and no spread-minimizing-zones zone don't pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "spread-minimizing",
            join_in_order: false,
            zones: vec![],
            tokens_file_path: "",
            expected_err: Some("number of zones 0 is not correct: it must be greater than 0 and less or equal than 8"),
        },
        Case {
            name: "spread-minimizing and a wrong zone don't pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: WRONG_ZONE,
            strategy: "spread-minimizing",
            join_in_order: false,
            zones: spread_zones.clone(),
            tokens_file_path: "",
            expected_err: Some("zone zone-d is not valid"),
        },
        Case {
            name: "spread-minimizing and tokens-file-path set don't pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "spread-minimizing",
            join_in_order: false,
            zones: spread_zones.clone(),
            tokens_file_path: "/path/tokens",
            expected_err: Some("strategy requires \"tokens-file-path\" to be empty"),
        },
        Case {
            name: "spread-minimizing and spread-minimizing-join-ring-in-order pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "spread-minimizing",
            join_in_order: true,
            zones: spread_zones.clone(),
            tokens_file_path: "",
            expected_err: None,
        },
        Case {
            name: "random passes validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "random",
            join_in_order: false,
            zones: vec![],
            tokens_file_path: "",
            expected_err: None,
        },
        Case {
            name: "random and tokens-file-path set pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "random",
            join_in_order: false,
            zones: vec![],
            tokens_file_path: "/path/tokens",
            expected_err: None,
        },
        Case {
            name: "random and spread-minimizing-join-ring-in-order don't pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "random",
            join_in_order: true,
            zones: vec![],
            tokens_file_path: "",
            expected_err: Some("\"spread-minimizing-join-ring-in-order\" must be false when using \"random\" token generation strategy"),
        },
        Case {
            name: "unknown token generation doesn't pass validation",
            num_tokens: 128,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "bla-bla-tokens",
            join_in_order: false,
            zones: vec![],
            tokens_file_path: "",
            expected_err: Some("unsupported token generation strategy (\"bla-bla-tokens\") has been chosen for token-generation-strategy"),
        },
        Case {
            name: "tokenless mode passes validation",
            num_tokens: 0,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "random",
            join_in_order: false,
            zones: vec![],
            tokens_file_path: "",
            expected_err: None,
        },
        Case {
            name: "tokenless mode with tokens-file-path set doesn't pass validation",
            num_tokens: 0,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "random",
            join_in_order: false,
            zones: vec![],
            tokens_file_path: "/path/tokens",
            expected_err: Some("tokens file path must be empty when ring tokens are disabled"),
        },
        Case {
            name: "tokenless mode with spread-minimizing strategy doesn't pass validation",
            num_tokens: 0,
            instance_id: INSTANCE_ID,
            zone: INSTANCE_ZONE,
            strategy: "spread-minimizing",
            join_in_order: false,
            zones: spread_zones,
            tokens_file_path: "",
            expected_err: Some("spread minimizing token generation strategy is not supported when ring tokens are disabled"),
        },
    ];

    for tc in cases {
        let cfg = RingConfig {
            num_tokens: tc.num_tokens,
            instance_id: tc.instance_id.to_string(),
            instance_availability_zone: tc.zone.to_string(),
            token_generation_strategy: tc.strategy.to_string(),
            spread_minimizing_join_ring_in_order: tc.join_in_order,
            spread_minimizing_zones: StringSliceCSV(tc.zones),
            tokens_file_path: tc.tokens_file_path.to_string(),
            ..Default::default()
        };

        let res = cfg.validate();
        match tc.expected_err {
            None => assert!(res.is_ok(), "case '{}' unexpected error: {:?}", tc.name, res),
            Some(substr) => {
                let err = res.expect_err(&format!("case '{}' expected error", tc.name));
                assert!(
                    err.to_string().contains(substr),
                    "case '{}': error {:?} does not contain {:?}",
                    tc.name,
                    err.to_string(),
                    substr
                );
            }
        }
    }
}

// Upstream: pkg/ingester/ingester_ring_test.go::TestRingConfig_CustomTokenGenerator
#[test]
fn test_ring_config_custom_token_generator() {
    let spread_zones = vec!["zone-a".to_string(), "zone-b".to_string(), "zone-c".to_string()];

    // Case 1: spread-minimizing
    {
        let cfg = RingConfig {
            instance_id: INSTANCE_ID.to_string(),
            instance_availability_zone: INSTANCE_ZONE.to_string(),
            token_generation_strategy: "spread-minimizing".to_string(),
            spread_minimizing_zones: StringSliceCSV(spread_zones.clone()),
            ..Default::default()
        };
        let lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 0).unwrap();
        let gen = lc_cfg.token_generator.unwrap();
        let expected_sm = SpreadMinimizingTokenGenerator::new(
            INSTANCE_ID,
            INSTANCE_ZONE,
            &spread_zones,
            false,
        )
        .unwrap();
        // Verify generated tokens match SpreadMinimizingTokenGenerator deterministically.
        assert_eq!(
            gen.generate_tokens(512, &[]),
            mimir_ring::token_gen::TokenGenerator::generate_tokens(&expected_sm, 512, &[])
        );
    }

    // Case 2: random
    {
        let cfg = RingConfig {
            instance_id: INSTANCE_ID.to_string(),
            instance_availability_zone: INSTANCE_ZONE.to_string(),
            token_generation_strategy: "random".to_string(),
            ..Default::default()
        };
        let lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 0).unwrap();
        let gen = lc_cfg.token_generator.unwrap();
        assert_eq!(gen.generate_tokens(128, &[]).len(), 128);
        assert!(!gen.can_join_enabled());
    }
}

// Upstream: pkg/ingester/ingester_ring_test.go::TestRingConfig_SpreadMinimizingJoinRingInOrder
#[test]
fn test_ring_config_spread_minimizing_join_ring_in_order() {
    let spread_zones = vec!["zone-a".to_string(), "zone-b".to_string(), "zone-c".to_string()];

    for (strategy, join_in_order, expected) in [
        ("spread-minimizing", true, true),
        ("spread-minimizing", false, false),
        ("random", false, false),
    ] {
        let cfg = RingConfig {
            instance_id: INSTANCE_ID.to_string(),
            instance_availability_zone: INSTANCE_ZONE.to_string(),
            token_generation_strategy: strategy.to_string(),
            spread_minimizing_join_ring_in_order: join_in_order,
            spread_minimizing_zones: StringSliceCSV(spread_zones.clone()),
            ..Default::default()
        };
        let lc_cfg = LifecyclerConfig::from_ring_config(&cfg, 0).unwrap();
        let gen = lc_cfg.token_generator.unwrap();
        assert_eq!(gen.can_join_enabled(), expected);
    }
}

struct TestFlushTransferer {
    flush_called: AtomicBool,
    state_on_flush: Mutex<Option<InstanceState>>,
    lifecycler: Mutex<Option<Arc<dyn IngesterLifecycler>>>,
}

impl TestFlushTransferer {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            flush_called: AtomicBool::new(false),
            state_on_flush: Mutex::new(None),
            lifecycler: Mutex::new(None),
        })
    }
}

impl TestFlushTransferer {
    fn record_flush(&self) {
        self.flush_called.store(true, Ordering::Release);
        if let Some(ref lc) = *self.lifecycler.lock() {
            *self.state_on_flush.lock() = Some(lc.get_state());
        }
    }
}

impl FlushTransferer for TestFlushTransferer {
    fn flush(&self) -> BoxFuture<'_, ()> {
        self.record_flush();
        Box::pin(async {})
    }

    fn transfer_out(&self) -> BoxFuture<'_, Result<(), LifecyclerError>> {
        Box::pin(async { Err(LifecyclerError::TransferDisabled) })
    }
}

async fn wait_instance_state(ring: &Ring, instance_id: &str, expected: InstanceState) {
    let ok = poll_until(Duration::from_secs(5), || async {
        ring.get_instance_state(instance_id).ok() == Some(expected)
    })
    .await;
    assert!(ok, "timed out waiting for {instance_id} to reach {expected:?}");
}

// Upstream: pkg/ingester/ingester_ring_lifecycler_test.go::TestLifecycler_FlushOnShutdown
#[tokio::test]
async fn test_lifecycler_flush_on_shutdown() {
    let cases = [
        ("not initialized with flush", false, false, false),
        ("initialized with flush", true, false, true),
        ("SetFlushOnShutdown(true) called", false, true, true),
    ];

    for (case_name, initial_flush, call_set_flush, expect_flush) in cases {
        for impl_name in ["classic Lifecycler", "tokenlessLifecycler"] {
            let mut ring_cfg = RingConfig::default();
            ring_cfg.kvstore.store = "inmemory".to_string();
            ring_cfg.instance_id = "test-instance-1".to_string();
            ring_cfg.instance_addr = "127.0.0.1".to_string();
            ring_cfg.num_tokens = if impl_name == "classic Lifecycler" { 16 } else { 0 };
            ring_cfg.heartbeat_period = GoDuration::from_millis(50);
            ring_cfg.min_ready_duration = GoDuration::ZERO;

            let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
            let (ring, ring_svc) = create_test_ring(&ring_cfg, Arc::clone(&kv), None).await;
            let flusher = TestFlushTransferer::new();

            let lc: Arc<dyn IngesterLifecycler> = if impl_name == "classic Lifecycler" {
                let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
                Lifecycler::with_store(
                    lc_cfg,
                    Arc::clone(&kv),
                    Some(Arc::clone(&flusher) as Arc<dyn FlushTransferer>),
                    INGEST_RING_NAME,
                    INGEST_RING_KEY,
                    initial_flush,
                    None,
                )
            } else {
                let basic_cfg = BasicLifecyclerConfig::from_ring_config_tokenless(&ring_cfg, 9095)
                    .unwrap();
                let f = Arc::clone(&flusher);
                let flush_cb: FlushFn = Arc::new(move || {
                    f.record_flush();
                    Box::pin(async {})
                });
                TokenlessLifecycler::new(
                    basic_cfg,
                    INGEST_RING_NAME,
                    INGEST_RING_KEY,
                    Arc::clone(&kv),
                    Duration::ZERO,
                    Duration::ZERO,
                    initial_flush,
                    Some(flush_cb),
                    None,
                )
            };
            *flusher.lifecycler.lock() = Some(Arc::clone(&lc));

            let svc = Arc::clone(&lc).into_managed_service();
            svc.start_async(ServiceContext::background()).await.unwrap();
            svc.await_running().await.unwrap();
            wait_instance_state(&ring, &ring_cfg.instance_id, InstanceState::Active).await;

            assert_eq!(lc.get_state(), InstanceState::Active);
            let tok_cnt = get_instance_token_count(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id)
                .await
                .unwrap();
            assert_eq!(tok_cnt, ring_cfg.num_tokens as usize);

            if call_set_flush {
                lc.set_flush_on_shutdown(true);
            }

            svc.stop_async();
            svc.await_terminated().await.unwrap();

            assert_eq!(
                flusher.flush_called.load(Ordering::Acquire),
                expect_flush,
                "{case_name} / {impl_name}"
            );
            if expect_flush {
                assert_eq!(
                    *flusher.state_on_flush.lock(),
                    Some(InstanceState::Leaving),
                    "{case_name} / {impl_name}"
                );
            }

            ring_svc.stop_async();
            let _ = ring_svc.await_terminated().await;
        }
    }
}

// Upstream: pkg/ingester/ingester_ring_lifecycler_test.go::TestLifecycler_ReadOnlyState
#[tokio::test]
async fn test_lifecycler_read_only_state() {
    for (set_ro, expect_ro, expected_metric) in [(false, false, 0.0), (true, true, 1.0)] {
        for impl_name in ["classic Lifecycler", "tokenlessLifecycler"] {
            let mut ring_cfg = RingConfig::default();
            ring_cfg.kvstore.store = "inmemory".to_string();
            ring_cfg.instance_id = "test-instance-1".to_string();
            ring_cfg.instance_addr = "127.0.0.1".to_string();
            ring_cfg.num_tokens = if impl_name == "classic Lifecycler" { 16 } else { 0 };
            ring_cfg.heartbeat_period = GoDuration::from_millis(50);

            let reg = Registry::new();
            let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
            let (ring, ring_svc) = create_test_ring(&ring_cfg, Arc::clone(&kv), None).await;

            let lc: Arc<dyn IngesterLifecycler> = if impl_name == "classic Lifecycler" {
                let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
                Lifecycler::with_store(
                    lc_cfg,
                    Arc::clone(&kv),
                    Some(Arc::new(NoopFlushTransferer)),
                    INGEST_RING_NAME,
                    INGEST_RING_KEY,
                    false,
                    Some(&reg),
                )
            } else {
                let basic_cfg = BasicLifecyclerConfig::from_ring_config_tokenless(&ring_cfg, 9095)
                    .unwrap();
                TokenlessLifecycler::new(
                    basic_cfg,
                    INGEST_RING_NAME,
                    INGEST_RING_KEY,
                    Arc::clone(&kv),
                    Duration::ZERO,
                    Duration::ZERO,
                    false,
                    None,
                    Some(&reg),
                )
            };

            let svc = Arc::clone(&lc).into_managed_service();
            svc.start_async(ServiceContext::background()).await.unwrap();
            svc.await_running().await.unwrap();
            wait_instance_state(&ring, &ring_cfg.instance_id, InstanceState::Active).await;

            let tok_cnt = get_instance_token_count(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id)
                .await
                .unwrap();
            assert_eq!(tok_cnt, ring_cfg.num_tokens as usize);

            let (ro, _) = lc.get_read_only_state();
            assert!(!ro);

            if set_ro {
                lc.change_read_only_state(true).await.unwrap();
            }

            let (ro, ts) = lc.get_read_only_state();
            assert_eq!(ro, expect_ro);
            if expect_ro {
                assert!(ts.is_some());
            }

            let mfs = reg.gather();
            let ro_metric = mfs
                .iter()
                .find(|mf| mf.get_name() == "lifecycler_read_only")
                .expect("lifecycler_read_only registered");
            let val = ro_metric.get_metric()[0].get_gauge().get_value();
            assert_eq!(val, expected_metric, "{impl_name}");

            svc.stop_async();
            svc.await_terminated().await.unwrap();
            ring_svc.stop_async();
            let _ = ring_svc.await_terminated().await;
        }
    }
}

// Upstream: pkg/ingester/ingester_ring_lifecycler_test.go::TestLifecycler_UnregisterOnShutdown
#[tokio::test]
async fn test_lifecycler_unregister_on_shutdown() {
    for (call_set, expect_unregister) in [
        (None, true),
        (Some(true), true),
        (Some(false), false),
    ] {
        for impl_name in ["classic Lifecycler", "tokenlessLifecycler"] {
            let mut ring_cfg = RingConfig::default();
            ring_cfg.kvstore.store = "inmemory".to_string();
            ring_cfg.instance_id = "test-instance-1".to_string();
            ring_cfg.instance_addr = "127.0.0.1".to_string();
            ring_cfg.num_tokens = if impl_name == "classic Lifecycler" { 16 } else { 0 };
            ring_cfg.heartbeat_period = GoDuration::from_millis(50);

            let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
            let (ring, ring_svc) = create_test_ring(&ring_cfg, Arc::clone(&kv), None).await;

            let lc: Arc<dyn IngesterLifecycler> = if impl_name == "classic Lifecycler" {
                let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
                Lifecycler::with_store(
                    lc_cfg,
                    Arc::clone(&kv),
                    Some(Arc::new(NoopFlushTransferer)),
                    INGEST_RING_NAME,
                    INGEST_RING_KEY,
                    false,
                    None,
                )
            } else {
                let basic_cfg = BasicLifecyclerConfig::from_ring_config_tokenless(&ring_cfg, 9095)
                    .unwrap();
                TokenlessLifecycler::new(
                    basic_cfg,
                    INGEST_RING_NAME,
                    INGEST_RING_KEY,
                    Arc::clone(&kv),
                    Duration::ZERO,
                    Duration::ZERO,
                    false,
                    None,
                    None,
                )
            };

            let svc = Arc::clone(&lc).into_managed_service();
            svc.start_async(ServiceContext::background()).await.unwrap();
            svc.await_running().await.unwrap();
            wait_instance_state(&ring, &ring_cfg.instance_id, InstanceState::Active).await;

            assert_eq!(lc.get_state(), InstanceState::Active);
            let tok_cnt = get_instance_token_count(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id)
                .await
                .unwrap();
            assert_eq!(tok_cnt, ring_cfg.num_tokens as usize);

            if let Some(v) = call_set {
                lc.set_unregister_on_shutdown(v);
            }
            assert_eq!(lc.should_unregister_on_shutdown(), expect_unregister);

            svc.stop_async();
            svc.await_terminated().await.unwrap();

            let state_opt =
                get_instance_state_in_ring(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id).await;
            if expect_unregister {
                assert!(state_opt.is_none(), "instance should be removed from ring");
            } else {
                assert_eq!(state_opt, Some(InstanceState::Leaving));
            }

            ring_svc.stop_async();
            let _ = ring_svc.await_terminated().await;
        }
    }
}

// Upstream: pkg/ingester/ingester_ring_lifecycler_test.go::TestLifecycler_CheckReady
#[tokio::test]
async fn test_lifecycler_check_ready() {
    let min_ready_duration = Duration::from_millis(200);

    for impl_name in ["classic Lifecycler", "tokenlessLifecycler"] {
        let mut ring_cfg = RingConfig::default();
        ring_cfg.kvstore.store = "inmemory".to_string();
        ring_cfg.instance_id = "test-instance-1".to_string();
        ring_cfg.instance_addr = "127.0.0.1".to_string();
        ring_cfg.min_ready_duration = GoDuration::from_millis(200);
        ring_cfg.num_tokens = if impl_name == "classic Lifecycler" { 16 } else { 0 };
        ring_cfg.heartbeat_period = GoDuration::from_millis(50);

        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let (ring, ring_svc) = create_test_ring(&ring_cfg, Arc::clone(&kv), None).await;

        let lc: Arc<dyn IngesterLifecycler> = if impl_name == "classic Lifecycler" {
            let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
            Lifecycler::with_store(
                lc_cfg,
                Arc::clone(&kv),
                Some(Arc::new(NoopFlushTransferer)),
                INGEST_RING_NAME,
                INGEST_RING_KEY,
                false,
                None,
            )
        } else {
            let basic_cfg =
                BasicLifecyclerConfig::from_ring_config_tokenless(&ring_cfg, 9095).unwrap();
            TokenlessLifecycler::new(
                basic_cfg,
                INGEST_RING_NAME,
                INGEST_RING_KEY,
                Arc::clone(&kv),
                min_ready_duration,
                Duration::ZERO,
                false,
                None,
                None,
            )
        };

        // Before starting: CheckReady returns an error.
        assert!(lc.check_ready().await.is_err());

        let svc = Arc::clone(&lc).into_managed_service();
        svc.start_async(ServiceContext::background()).await.unwrap();
        svc.await_running().await.unwrap();
        wait_instance_state(&ring, &ring_cfg.instance_id, InstanceState::Active).await;

        let tok_cnt = get_instance_token_count(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id)
            .await
            .unwrap();
        assert_eq!(tok_cnt, ring_cfg.num_tokens as usize);

        // Immediately after ACTIVE: CheckReady returns error waiting for MinReadyDuration.
        let err = lc.check_ready().await.unwrap_err();
        assert!(
            err.contains("waiting for"),
            "expected 'waiting for' in error, got: {err}"
        );

        // Wait for MinReadyDuration to elapse.
        tokio::time::sleep(min_ready_duration + Duration::from_millis(30)).await;
        lc.check_ready().await.expect("ready after min_ready_duration");

        // Change instance to read-only; ready state is latched.
        lc.change_read_only_state(true).await.unwrap();
        lc.check_ready()
            .await
            .expect("ready state should remain latched in read-only");

        svc.stop_async();
        svc.await_terminated().await.unwrap();
        ring_svc.stop_async();
        let _ = ring_svc.await_terminated().await;
    }
}

// Upstream: pkg/ingester/ingester_ring_lifecycler_test.go::TestTokenlessLifecycler_RemovesPreExistingTokens
#[tokio::test]
async fn test_tokenless_lifecycler_removes_pre_existing_tokens() {
    let mut ring_cfg = RingConfig::default();
    ring_cfg.kvstore.store = "inmemory".to_string();
    ring_cfg.instance_id = "test-instance-1".to_string();
    ring_cfg.instance_addr = "127.0.0.1".to_string();
    ring_cfg.num_tokens = 16;
    ring_cfg.heartbeat_period = GoDuration::from_millis(50);

    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let (ring, ring_svc) = create_test_ring(&ring_cfg, Arc::clone(&kv), None).await;

    // Step 1: Pre-register the instance with tokens using classic lifecycler.
    let classic_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 9095).unwrap();
    let classic = Lifecycler::with_store(
        classic_cfg,
        Arc::clone(&kv),
        Some(Arc::new(NoopFlushTransferer)),
        INGEST_RING_NAME,
        INGEST_RING_KEY,
        false,
        None,
    );
    let classic_svc = Arc::clone(&classic).into_service();
    classic_svc.start_async(ServiceContext::background()).await.unwrap();
    classic_svc.await_running().await.unwrap();
    wait_instance_state(&ring, &ring_cfg.instance_id, InstanceState::Active).await;

    assert_eq!(
        get_instance_token_count(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id).await,
        Some(16)
    );

    classic.set_unregister_on_shutdown(false);
    classic_svc.stop_async();
    classic_svc.await_terminated().await.unwrap();

    // Still has tokens after classic lifecycler stopped.
    assert_eq!(
        get_instance_token_count(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id).await,
        Some(16)
    );

    // Step 2: Start tokenless lifecycler for the same instance.
    ring_cfg.num_tokens = 0;
    let basic_cfg = BasicLifecyclerConfig::from_ring_config_tokenless(&ring_cfg, 9095).unwrap();
    let tokenless = TokenlessLifecycler::new(
        basic_cfg,
        INGEST_RING_NAME,
        INGEST_RING_KEY,
        Arc::clone(&kv),
        Duration::ZERO,
        Duration::ZERO,
        false,
        None,
        None,
    );
    let tokenless_svc = Arc::clone(&tokenless).into_managed_service();
    tokenless_svc.start_async(ServiceContext::background()).await.unwrap();
    tokenless_svc.await_running().await.unwrap();
    wait_instance_state(&ring, &ring_cfg.instance_id, InstanceState::Active).await;

    // Step 3: Verify tokens have been removed.
    assert_eq!(
        get_instance_token_count(kv.as_ref(), INGEST_RING_KEY, &ring_cfg.instance_id).await,
        Some(0)
    );

    tokenless_svc.stop_async();
    tokenless_svc.await_terminated().await.unwrap();
    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}

/// Returns the sample count of `shutdown_duration_seconds{op, status}`.
fn shutdown_duration_count(reg: &Registry, op: &str, status: &str) -> u64 {
    reg.gather()
        .iter()
        .filter(|mf| mf.get_name() == "shutdown_duration_seconds")
        .flat_map(|mf| mf.get_metric().iter())
        .filter(|m| {
            let l = m.get_label();
            l.iter().any(|p| p.get_name() == "op" && p.get_value() == op)
                && l.iter().any(|p| p.get_name() == "status" && p.get_value() == status)
        })
        .map(|m| m.get_histogram().get_sample_count())
        .sum()
}

// The Ingester is the lifecycler's FlushTransferer (pkg/ingester/ingester.go `ring.NewLifecycler(
// ..., i, ...)`): on shutdown the lifecycler switches to LEAVING, then calls Ingester.Flush()
// (TransferOut is disabled), which compacts and ships the head, then unregisters.
#[tokio::test]
async fn test_ingester_flushes_through_lifecycler_on_shutdown() {
    use compaction_ring_helpers::{
        default_ingester_test_config, prepare_ingester_with_blocks_storage,
        push_series_to_ingester, start_and_wait_healthy,
    };

    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = default_ingester_test_config(tmp.path());
    cfg.blocks.tsdb.flush_blocks_on_shutdown = true;
    cfg.blocks.tsdb.ship_interval = GoDuration::from_secs(3600);
    let bucket_dir = tmp.path().join("bucket");
    let ring_kv = Arc::clone(&cfg.ring_kv);
    let reg = Registry::new();
    let (ing, ring, ring_svc) = prepare_ingester_with_blocks_storage(cfg, None, Some(&reg)).await;
    start_and_wait_healthy(&ing, &ring).await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    push_series_to_ingester(&ing, "user-1", &[(&[("__name__", "test_metric")], &[(now, 1.0)])])
        .await
        .unwrap();

    ing.stop().await.unwrap();

    assert_eq!(shutdown_duration_count(&reg, "flush", "success"), 1);
    assert_eq!(shutdown_duration_count(&reg, "transfer", "success"), 0);
    assert_eq!(shutdown_duration_count(&reg, "transfer", "fail"), 0);

    // The head was compacted and shipped to the bucket by the lifecycler-driven flush.
    let user_bucket = bucket_dir.join("user-1");
    let shipped: Vec<_> = std::fs::read_dir(&user_bucket)
        .map(|rd| rd.filter_map(Result::ok).collect())
        .unwrap_or_default();
    assert!(!shipped.is_empty(), "no block shipped to {}", user_bucket.display());

    // unregister_on_shutdown defaults to true: the instance left the ring after the flush.
    assert!(get_instance_state_in_ring(ring_kv.as_ref(), INGEST_RING_KEY, "localhost")
        .await
        .is_none());

    ring_svc.stop_async();
    let _ = ring_svc.await_terminated().await;
}
