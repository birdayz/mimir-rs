//! Ported from github.com/grafana/dskit/ring/lifecycler_test.go (subset) and
//! github.com/grafana/dskit/ring/util.go `GetInstanceAddr` (instance address auto-detection).

use std::sync::Arc;
use std::time::Duration;

use mimir_common::services::{Service, ServiceContext};
use mimir_config::{GoDuration, RingConfig};
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{CasOutcome, KvClient};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig, NoopFlushTransferer};
use mimir_ring::ring::model::ring_desc_codec;

const RING_KEY: &str = "ring";
const INSTANCE_ID: &str = "ing1";

// dskit lifecycler_test.go testLifecyclerConfig: Addr 0.0.0.0, Port 1, NumTokens 1, Zone "zone-1",
// FinalSleep 0, HeartbeatPeriod 100ms.
fn test_lifecycler_config(id: &str) -> LifecyclerConfig {
    let ring_cfg = RingConfig {
        instance_id: id.to_string(),
        instance_addr: "0.0.0.0".to_string(),
        instance_port: 1,
        num_tokens: 1,
        instance_availability_zone: "zone-1".to_string(),
        final_sleep: GoDuration::ZERO,
        heartbeat_period: GoDuration::from_millis(100),
        observe_period: GoDuration::ZERO,
        min_ready_duration: GoDuration::ZERO,
        ..Default::default()
    };
    LifecyclerConfig::from_ring_config(&ring_cfg, 0).unwrap()
}

async fn get_instance(kv: &dyn KvClient, id: &str) -> Option<InstanceDesc> {
    let v = kv.get(RING_KEY).await.unwrap()?;
    v.downcast_ref::<Desc>()?.ingesters.get(id).cloned()
}

async fn poll<F, Fut>(timeout: Duration, mut f: F) -> bool
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
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn check_normalised(kv: &Arc<dyn KvClient>, id: &str) -> bool {
    let Some(v) = kv.get(RING_KEY).await.unwrap() else {
        return false;
    };
    let Some(desc) = v.downcast_ref::<Desc>() else {
        return false;
    };
    desc.ingesters.len() == 1
        && desc.ingesters.get(id).is_some_and(|i| {
            i.state == InstanceState::Active as i32 && i.tokens.len() == 1
        })
}

async fn start_lifecycler(cfg: LifecyclerConfig, kv: &Arc<dyn KvClient>) -> Arc<dyn Service> {
    let lc = Lifecycler::with_store(
        cfg,
        Arc::clone(kv),
        Some(Arc::new(NoopFlushTransferer)),
        "ingester",
        RING_KEY,
        true,
        None,
    );
    let svc = lc.into_service();
    svc.start_async(ServiceContext::background()).await.unwrap();
    svc.await_running().await.unwrap();
    svc
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_HeartbeatAfterBackendReset
#[tokio::test]
async fn test_lifecycler_heartbeat_after_backend_reset() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let cfg = test_lifecycler_config(INSTANCE_ID);
    let svc = start_lifecycler(cfg, &kv).await;

    assert!(poll(Duration::from_secs(1), || check_normalised(&kv, INSTANCE_ID)).await);
    let prev = get_instance(kv.as_ref(), INSTANCE_ID).await.unwrap();

    // Registration timestamp has second precision.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Simulate a ring storage reset; the next heartbeat must restore the instance.
    kv.cas(
        RING_KEY,
        Box::new(|_| Ok(CasOutcome::Update(Box::new(Desc::default())))),
    )
    .await
    .unwrap();

    let restored = poll(Duration::from_secs(1), || async {
        get_instance(kv.as_ref(), INSTANCE_ID).await.is_some()
    })
    .await;
    assert!(restored, "next heartbeat must re-add the instance after a backend reset");

    let desc = get_instance(kv.as_ref(), INSTANCE_ID).await.unwrap();
    assert!(desc.registered_timestamp > prev.registered_timestamp);
    assert!(desc.timestamp > 0);
    assert_eq!(desc.id, INSTANCE_ID);
    assert_eq!(desc.state, InstanceState::Active as i32);
    assert_eq!(desc.addr, "0.0.0.0:1");
    assert_eq!(desc.zone, "zone-1");
    assert_eq!(desc.tokens, prev.tokens);

    svc.stop_async();
    svc.await_terminated().await.unwrap();
}

// Upstream: github.com/grafana/dskit/ring/lifecycler_test.go::TestLifecycler_ShouldHandleInstanceAbruptlyRestarted
#[tokio::test]
async fn test_lifecycler_should_handle_instance_abruptly_restarted() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(ring_desc_codec()));
    let l1 = start_lifecycler(test_lifecycler_config(INSTANCE_ID), &kv).await;
    assert!(poll(Duration::from_secs(1), || check_normalised(&kv, INSTANCE_ID)).await);
    let first = get_instance(kv.as_ref(), INSTANCE_ID).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Second lifecycler with the same settings: it must think it restarted.
    let l2 = start_lifecycler(test_lifecycler_config(INSTANCE_ID), &kv).await;
    let same = poll(Duration::from_secs(1), || async {
        if !check_normalised(&kv, INSTANCE_ID).await {
            return false;
        }
        let d = get_instance(kv.as_ref(), INSTANCE_ID).await.unwrap();
        d.tokens == first.tokens && d.registered_timestamp == first.registered_timestamp
    })
    .await;
    assert!(same, "restarted instance must keep tokens and registered timestamp");
    l2.stop_async();
    let _ = l2.await_terminated().await;
    l1.stop_async();
    let _ = l1.await_terminated().await;
}

// Derived from github.com/grafana/dskit/ring/util.go::GetInstanceAddr +
// github.com/grafana/dskit/netutil/netutil.go::GetFirstAddressOf (TestGetFirstAddressOf
// "no address" case): with an empty instance_addr, the address is taken from
// instance_interface_names (default [eth0, en0]); if none has an address the lifecycler fails to
// start. It must never silently register 127.0.0.1.
#[test]
fn test_instance_addr_auto_detected_from_interface_names() {
    let ring_cfg = RingConfig {
        instance_id: INSTANCE_ID.to_string(),
        instance_addr: String::new(),
        instance_interface_names: vec!["mimir-rs-no-such-if0".to_string()],
        ..Default::default()
    };
    match LifecyclerConfig::from_ring_config(&ring_cfg, 9095) {
        Err(_) => {}
        Ok(cfg) => panic!(
            "dskit fails with 'no address found for [mimir-rs-no-such-if0]'; mimir-rs registered addr={:?}",
            cfg.addr
        ),
    }
}
