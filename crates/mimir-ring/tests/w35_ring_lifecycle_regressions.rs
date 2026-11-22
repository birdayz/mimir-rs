use std::any::Any;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::{Service, ServiceContext, State};
use mimir_config::server::MemberlistConfig;
use mimir_config::{GoDuration, RingConfig, StringSliceCSVMulti};
use mimir_proto::ringpb::{Desc, InstanceState, PartitionRingDesc};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::memberlist::client::{MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::memberlist::kv_init_service::KvInitService;
use mimir_ring::kv::memberlist::Mergeable;
use mimir_ring::kv::{BoxFuture, CasFn, KvClient, KvError};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig, NoopFlushTransferer};
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, PartitionInstanceLifecycler, PartitionInstanceLifecyclerConfig,
};
use mimir_ring::ring::model::{ring_desc_codec, DescExt, InstanceDescExt};

fn free_loopback_port() -> u16 {
    let ln = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    ln.local_addr().expect("local_addr").port()
}

fn closed_loopback_addr() -> String {
    let port = free_loopback_port();
    format!("127.0.0.1:{port}")
}

#[tokio::test]
async fn bug_r1_fast_join_failure_and_bad_advertise_release_bound_tcp_port() {
    // 1. Fast-join failure with abort_if_cluster_fast_join_fails=true must release the bound TCP port.
    let port = free_loopback_port();
    let unreachable = closed_loopback_addr();
    let cfg = MemberlistConfig {
        node_name: "bug-r1-fast-join".to_string(),
        randomize_node_name: false,
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: i64::from(port),
        join_members: StringSliceCSVMulti(vec![unreachable]),
        min_join_backoff: GoDuration::from_millis(1),
        max_join_backoff: GoDuration::from_millis(5),
        max_join_retries: 1,
        stream_timeout: GoDuration::from_millis(100),
        abort_if_cluster_fast_join_fails: true,
        abort_if_cluster_fast_join_fails_min_nodes: 1,
        ..Default::default()
    };

    let err = MemberlistKv::start(cfg, vec![ring_desc_codec()], "test", None)
        .await
        .err()
        .expect("fast-join must fail");
    assert!(
        err.to_string().contains("fast-join"),
        "unexpected error: {err}"
    );

    // Yield briefly so any spawned listener tasks finish dropping their TcpListener handles.
    tokio::task::yield_now().await;
    let rebind = TcpListener::bind(("127.0.0.1", port));
    assert!(
        rebind.is_ok(),
        "bound port {port} must be released after fast-join abort: {:?}",
        rebind.err()
    );
    drop(rebind);

    // 2. Invalid advertise_addr after TcpTransport::new must also release the bound TCP port.
    let port2 = free_loopback_port();
    let cfg2 = MemberlistConfig {
        node_name: "bug-r1-bad-adv".to_string(),
        randomize_node_name: false,
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: i64::from(port2),
        advertise_addr: "not-a-valid-ip".to_string(),
        ..Default::default()
    };
    let err2 = MemberlistKv::start(cfg2, vec![ring_desc_codec()], "test", None)
        .await
        .err()
        .expect("bad advertise_addr must fail");
    assert!(
        err2.to_string().contains("final advertise address"),
        "unexpected error: {err2}"
    );
    let rebind2 = TcpListener::bind(("127.0.0.1", port2));
    assert!(
        rebind2.is_ok(),
        "bound port {port2} must be released after advertise_addr error: {:?}",
        rebind2.err()
    );
}

#[tokio::test]
async fn bug_r2_watch_key_and_watch_prefix_abort_promptly_when_canceled_while_kv_starting() {
    // Silent listener accepts TCP connect into backlog without replying, holding fast-join in Starting.
    let silent_ln = TcpListener::bind("127.0.0.1:0").expect("bind silent listener");
    let silent_addr = silent_ln.local_addr().expect("local_addr").to_string();
    let cfg = MemberlistConfig {
        node_name: "bug-r2-node".to_string(),
        randomize_node_name: false,
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        join_members: StringSliceCSVMulti(vec![silent_addr]),
        min_join_backoff: GoDuration::from_millis(1),
        max_join_backoff: GoDuration::from_millis(5),
        max_join_retries: 1,
        stream_timeout: GoDuration::from_millis(400),
        abort_if_cluster_fast_join_fails: false,
        ..Default::default()
    };

    let codec = ring_desc_codec();
    let kv = MemberlistKv::new(cfg, vec![Arc::clone(&codec)], "test", None, Default::default())
        .expect("MemberlistKv::new");
    kv.service()
        .start_async(ServiceContext::background())
        .await
        .expect("start_async");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while kv.state() == State::New {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for State::Starting"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(kv.state(), State::Starting);

    let client = Arc::new(MemberlistKvClient::new(Arc::clone(&kv), codec).expect("new client"));
    let watch_key_ctx = ServiceContext::background();
    let watch_prefix_ctx = ServiceContext::background();

    let c1 = Arc::clone(&client);
    let ctx1 = watch_key_ctx.clone();
    let h_key = tokio::spawn(async move {
        c1.watch_key("ring", ctx1, Box::new(|_| true)).await;
    });

    let c2 = Arc::clone(&client);
    let ctx2 = watch_prefix_ctx.clone();
    let h_prefix = tokio::spawn(async move {
        c2.watch_prefix("tenants/", ctx2, Box::new(|_, _| true))
            .await;
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    watch_key_ctx.cancel();
    watch_prefix_ctx.cancel();

    tokio::time::timeout(Duration::from_millis(500), h_key)
        .await
        .expect("watch_key must abort promptly when canceled during Starting")
        .expect("watch_key task");
    tokio::time::timeout(Duration::from_millis(500), h_prefix)
        .await
        .expect("watch_prefix must abort promptly when canceled during Starting")
        .expect("watch_prefix task");

    kv.shutdown().await;
    drop(silent_ln);
}

#[tokio::test]
async fn bug_r3_kv_init_service_propagates_failure_and_shuts_down_inflight_kv() {
    // Part A: inner MemberlistKv runtime failure propagates to KvInitService.
    {
        let unreachable = closed_loopback_addr();
        let cfg = MemberlistConfig {
            node_name: "bug-r3-fail".to_string(),
            randomize_node_name: false,
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            join_members: StringSliceCSVMulti(vec![unreachable]),
            min_join_backoff: GoDuration::from_millis(1),
            max_join_backoff: GoDuration::from_millis(5),
            max_join_retries: 1,
            stream_timeout: GoDuration::from_millis(50),
            abort_if_cluster_fast_join_fails: true,
            abort_if_cluster_fast_join_fails_min_nodes: 1,
            ..Default::default()
        };
        let init = KvInitService::new(cfg, vec![ring_desc_codec()], None, None);
        let svc = init.service();
        svc.start_async(ServiceContext::background())
            .await
            .expect("start_async");
        svc.await_running().await.expect("await_running");

        let _kv = init
            .get_memberlist_kv()
            .await
            .expect("get_memberlist_kv starts kv asynchronously and returns Ok");

        let term = tokio::time::timeout(Duration::from_secs(3), svc.await_terminated())
            .await
            .expect("KvInitService must terminate when inner MemberlistKv fails");
        assert!(term.is_err(), "expected failure propagation, got {term:?}");
        assert_eq!(svc.state(), State::Failed);
        let failure = svc
            .failure_case()
            .expect("failure_case should be populated");
        assert!(
            failure.to_string().contains("fast-join"),
            "unexpected failure_case: {failure}"
        );
    }

    // Part B: stopping KvInitService mid-startup shuts down an in-flight MemberlistKv cleanly.
    {
        let port = free_loopback_port();
        let silent_ln = TcpListener::bind("127.0.0.1:0").expect("bind silent listener");
        let silent_addr = silent_ln.local_addr().expect("local_addr").to_string();
        let cfg = MemberlistConfig {
            node_name: "bug-r3-inflight-stop".to_string(),
            randomize_node_name: false,
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: i64::from(port),
            join_members: StringSliceCSVMulti(vec![silent_addr]),
            min_join_backoff: GoDuration::from_millis(1),
            max_join_backoff: GoDuration::from_millis(5),
            max_join_retries: 1,
            stream_timeout: GoDuration::from_millis(400),
            abort_if_cluster_fast_join_fails: false,
            ..Default::default()
        };
        let init = KvInitService::new(cfg, vec![ring_desc_codec()], None, None);
        let svc = init.service();
        svc.start_async(ServiceContext::background())
            .await
            .expect("start_async");
        svc.await_running().await.expect("await_running");

        let kv = init
            .get_memberlist_kv()
            .await
            .expect("get_memberlist_kv");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while kv.listening_port() != port {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for transport bind"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(kv.state(), State::Starting);

        svc.stop_async();
        tokio::time::timeout(Duration::from_secs(3), svc.await_terminated())
            .await
            .expect("KvInitService must stop cleanly")
            .expect("clean termination");
        assert_eq!(kv.state(), State::Terminated);

        drop(silent_ln);
        let rebind = TcpListener::bind(("127.0.0.1", port));
        assert!(
            rebind.is_ok(),
            "port {port} must be released after mid-startup KvInitService stop: {:?}",
            rebind.err()
        );
    }
}

#[tokio::test]
async fn bug_r4_partition_lifecycler_removes_owner_when_canceled_after_registering() {
    struct StopAfterFirstCasKv {
        inner: InMemoryKv,
        cas_count: AtomicUsize,
        svc_slot: OnceLock<Arc<mimir_common::services::ManagedBasicService>>,
    }

    impl KvClient for StopAfterFirstCasKv {
        fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>, KvError>> {
            self.inner.list(prefix)
        }
        fn get<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxFuture<'a, Result<Option<Box<dyn Any + Send + Sync>>, KvError>> {
            self.inner.get(key)
        }
        fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), KvError>> {
            self.inner.delete(key)
        }
        fn cas<'a>(&'a self, key: &'a str, f: CasFn) -> BoxFuture<'a, Result<(), KvError>> {
            Box::pin(async move {
                let res = self.inner.cas(key, f).await;
                if self.cas_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    if let Some(svc) = self.svc_slot.get() {
                        // Cancel right after create_partition_and_register_owner CAS succeeds,
                        // before starting() returns to ManagedBasicService.
                        svc.stop_async();
                    }
                }
                res
            })
        }
        fn watch_key<'a>(
            &'a self,
            key: &'a str,
            cancel: ServiceContext,
            f: Box<dyn FnMut(Box<dyn Any + Send + Sync>) -> bool + Send>,
        ) -> BoxFuture<'a, ()> {
            self.inner.watch_key(key, cancel, f)
        }
        fn watch_prefix<'a>(
            &'a self,
            prefix: &'a str,
            cancel: ServiceContext,
            f: Box<dyn FnMut(&str, Box<dyn Any + Send + Sync>) -> bool + Send>,
        ) -> BoxFuture<'a, ()> {
            self.inner.watch_prefix(prefix, cancel, f)
        }
    }

    let kv = Arc::new(StopAfterFirstCasKv {
        inner: InMemoryKv::new(partition_ring_desc_codec()),
        cas_count: AtomicUsize::new(0),
        svc_slot: OnceLock::new(),
    });
    let cfg = PartitionInstanceLifecyclerConfig {
        partition_id: 0,
        instance_id: "ing-r4".to_string(),
        polling_interval: Duration::from_secs(60),
        ..Default::default()
    };
    let plc = PartitionInstanceLifecycler::new(
        cfg,
        "ingester-partitions",
        "ingester-partitions",
        Arc::clone(&kv) as Arc<dyn KvClient>,
        None,
    )
    .expect("PartitionInstanceLifecycler::new");
    plc.set_create_partition_on_startup(true);
    plc.set_remove_owner_on_shutdown(true);

    let svc = plc.into_service();
    let _ = kv.svc_slot.set(Arc::clone(&svc));

    svc.start_async(ServiceContext::background())
        .await
        .expect("start_async");
    tokio::time::timeout(Duration::from_secs(2), svc.await_terminated())
        .await
        .expect("service must terminate")
        .expect("service must transition Starting -> Stopping -> Terminated, not Failed");

    let raw = kv
        .inner
        .get("ingester-partitions")
        .await
        .expect("get")
        .expect("ring desc exists");
    let desc = raw
        .downcast_ref::<PartitionRingDesc>()
        .expect("PartitionRingDesc");
    assert!(
        desc.owners.is_empty(),
        "owner must be removed during stopping() when canceled right after registering: {:?}",
        desc.owners
    );
}

#[tokio::test]
async fn bug_r5_init_ring_bumps_timestamp_on_same_second_leaving_to_active_recovery() {
    // Retry once if the wall clock ticks to the next second between seeding and init_ring.
    for attempt in 0..2 {
        let kv = Arc::new(InMemoryKv::new(ring_desc_codec()));
        let now_s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_secs() as i64;

        let mut old_desc = Desc::default();
        old_desc.add_ingester(
            "ing-r5",
            "127.0.0.1:9095",
            "zone-a",
            vec![100, 200],
            InstanceState::Leaving,
            Some(SystemTime::now()),
            false,
            None,
            Default::default(),
        );
        if let Some(inst) = old_desc.ingesters.get_mut("ing-r5") {
            inst.timestamp = now_s;
        }
        let seed_desc = old_desc.clone();
        kv.cas(
            "ring",
            Box::new(move |_| {
                Ok(mimir_ring::kv::CasOutcome::Update(Box::new(
                    seed_desc.clone(),
                )))
            }),
        )
        .await
        .expect("seed ring");

        let ring_cfg = RingConfig {
            instance_id: "ing-r5".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            instance_port: 9095,
            num_tokens: 2,
            unregister_on_shutdown: false,
            ..Default::default()
        };
        let mut lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 0).expect("LifecyclerConfig");
        lc_cfg.zone = "zone-a".to_string();
        let lc = Lifecycler::with_store(
            lc_cfg,
            Arc::clone(&kv) as Arc<dyn KvClient>,
            Some(Arc::new(NoopFlushTransferer)),
            "ingester",
            "ring",
            true,
            None,
        );
        lc.init_ring().await.expect("init_ring");

        let after_raw = kv.get("ring").await.expect("get").expect("desc");
        let mut new_desc = after_raw.downcast_ref::<Desc>().expect("Desc").clone();
        let after_inst = new_desc.ingesters.get("ing-r5").expect("ingester present");
        assert_eq!(after_inst.instance_state(), InstanceState::Active);

        let after_wall_s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_secs() as i64;
        if after_wall_s != now_s && attempt == 0 {
            continue;
        }
        assert!(
            after_inst.timestamp > now_s,
            "expected bumped timestamp > {now_s}, got {}",
            after_inst.timestamp
        );

        let mut peer_view = old_desc.clone();
        let merged = peer_view.merge(&mut new_desc, false).expect("merge ok");
        assert!(
            merged.is_some(),
            "memberlist Merge must accept same-second LEAVING -> ACTIVE recovery"
        );
        let merged_inst = peer_view.ingesters.get("ing-r5").expect("merged inst");
        assert_eq!(merged_inst.instance_state(), InstanceState::Active);
        return;
    }
}
