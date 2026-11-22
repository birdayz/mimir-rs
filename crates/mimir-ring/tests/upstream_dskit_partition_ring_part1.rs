//! Ported from github.com/grafana/dskit/ring:
//! - multi_partition_instance_ring_test.go (1)
//! - partition_instance_lifecycler_test.go (5)
//! - partition_instance_ring_test.go (2)
//! - partition_instance_rings_test.go (2)
//! - partition_ring_cache_test.go (1)
//! - partition_ring_clone_test.go (1)
//! - partition_ring_editor_test.go (3)
//! - partition_ring_http_test.go (3)
//! - partition_ring_model_test.go (15)

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::{
    start_and_await_running, stop_and_await_terminated, Service, ServiceContext, State,
};
use mimir_proto::ringpb::{
    InstanceDesc, InstanceState, OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc,
    PartitionState, PartitionTokenScheme,
};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::memberlist::Mergeable;
use mimir_ring::kv::{CasOutcome, KvClient};
use mimir_ring::partition_ring::http::partition_ring_router_at;
use mimir_ring::partition_ring::model::{
    partition_ring_desc_codec, PartitionDescExt, PartitionRingDescExt, PartitionRingError,
};
use mimir_ring::partition_ring::{
    multi_partition_owner_instance_id, InstanceRingReader, MultiPartitionInstanceRing,
    PartitionInstanceLifecycler, PartitionInstanceLifecyclerConfig, PartitionInstanceRing,
    PartitionInstanceRings, PartitionRing, PartitionRingEditor, PartitionRingOptions,
    PartitionRingWatcher, PartitionRingWatchers, StaticPartitionRingReader,
};
use mimir_ring::ring::model::{system_time_to_unix_secs, MAX_FUTURE_TIMESTAMP_SKEW};
use mimir_ring::ring::replication_strategy::Operation;
use mimir_ring::ring::RingError;
use mimir_ring::token_gen::{
    PartitionTokenGenerator, TokenGenError, OPTIMAL_TOKENS_PER_INSTANCE,
};
use regex::Regex;
use tokio::net::TcpListener;

const RING_KEY: &str = "ring";
const TEST_RING_NAME: &str = "test";

struct StaticInstanceRing {
    instances: HashMap<String, InstanceDesc>,
}

impl InstanceRingReader for StaticInstanceRing {
    fn get_instance(&self, id: &str) -> Result<InstanceDesc, RingError> {
        self.instances
            .get(id)
            .cloned()
            .ok_or(RingError::InstanceNotFound)
    }

    fn instances_count(&self) -> usize {
        self.instances.len()
    }
}

async fn get_partition_ring_from_store(store: &dyn KvClient, key: &str) -> PartitionRingDesc {
    let v = store.get(key).await.unwrap();
    v.and_then(|a| a.downcast::<PartitionRingDesc>().ok().map(|b| *b))
        .unwrap_or_default()
}

async fn get_partition_state_from_store(
    store: &dyn KvClient,
    key: &str,
    partition_id: i32,
) -> PartitionState {
    let desc = get_partition_ring_from_store(store, key).await;
    desc.partitions
        .get(&partition_id)
        .map(|p| p.partition_state())
        .unwrap_or(PartitionState::PartitionUnknown)
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

fn assert_map_elements_match(
    expected: BTreeMap<i32, Vec<String>>,
    actual: HashMap<i32, Vec<String>>,
) {
    let mut act_sorted: BTreeMap<i32, Vec<String>> = actual
        .into_iter()
        .map(|(k, mut v)| {
            v.sort();
            (k, v)
        })
        .collect();
    let exp_sorted: BTreeMap<i32, Vec<String>> = expected
        .into_iter()
        .map(|(k, mut v)| {
            v.sort();
            (k, v)
        })
        .collect();
    assert_eq!(exp_sorted, act_sorted);
    let _ = &mut act_sorted;
}

fn create_test_pil_config(
    partition_id: i32,
    instance_id: &str,
) -> PartitionInstanceLifecyclerConfig {
    PartitionInstanceLifecyclerConfig {
        partition_id,
        instance_id: instance_id.to_string(),
        wait_owners_count_on_pending: 0,
        wait_owners_duration_on_pending: Duration::ZERO,
        delete_inactive_partition_after: Duration::ZERO,
        polling_interval: Duration::from_millis(10),
        ..Default::default()
    }
}

fn create_test_multi_pil_config(
    partition_id: i32,
    instance_id: &str,
) -> PartitionInstanceLifecyclerConfig {
    PartitionInstanceLifecyclerConfig {
        partition_id,
        instance_id: instance_id.to_string(),
        multi_partition_ownership: true,
        wait_owners_count_on_pending: 0,
        wait_owners_duration_on_pending: Duration::ZERO,
        delete_inactive_partition_after: Duration::ZERO,
        polling_interval: Duration::from_millis(10),
        ..Default::default()
    }
}

fn derived_tokens_lifecycler_config(
    partition_id: i32,
    derived: bool,
) -> PartitionInstanceLifecyclerConfig {
    let mut cfg = create_test_pil_config(partition_id, "instance-1");
    cfg.create_partitions_with_derived_tokens = derived;
    cfg.max_derived_token_partitions = 8;
    cfg
}

struct CachedFnTokenGen {
    tokens: HashMap<i32, Vec<u32>>,
}

impl PartitionTokenGenerator for CachedFnTokenGen {
    fn tokens_for(&self, id: i32) -> Result<&[u32], TokenGenError> {
        self.tokens
            .get(&id)
            .map(Vec::as_slice)
            .ok_or_else(|| TokenGenError::SpreadMinimizing(format!("must not resolve dangling {id}")))
    }
}

// ============================================================================
// multi_partition_instance_ring_test.go (1)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/multi_partition_instance_ring_test.go::TestMultiPartitionInstanceRing_GetReplicationSetForPartitionAndOperation_HighestPreferrablyNonReadOnlyFromEachZone
#[test]
fn test_multi_partition_instance_ring_get_replication_set_for_partition_and_operation_highest_preferrably_non_read_only_from_each_zone() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let op = Operation::new(&[InstanceState::Active], &[]);
    let heartbeat_timeout = Duration::from_secs(60);
    const TEST_PARTITION_ID: i32 = 1;

    #[derive(Debug, PartialEq, Eq)]
    struct ComparableSet {
        instances: Vec<String>,
        max_unavailable_zones: usize,
        partition_id: i32,
    }

    enum Expected {
        EmptyRing,
        TooManyUnhealthy,
        Set(ComparableSet),
    }

    let mk_inst = |id: &str, zone: &str, ts: i64, ro: bool| InstanceDesc {
        id: id.to_string(),
        state: InstanceState::Active as i32,
        zone: zone.to_string(),
        timestamp: ts,
        read_only: ro,
        ..Default::default()
    };

    let cases: Vec<(PartitionRingDesc, HashMap<String, InstanceDesc>, Expected)> = vec![
        // empty partitions ring
        (
            PartitionRingDesc::default(),
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", now_s, false)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "a", now_s, false)),
            ]),
            Expected::EmptyRing,
        ),
        // empty instances ring
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    (multi_partition_owner_instance_id("instance-zone-a-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-a-1", 2), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::new(),
            Expected::TooManyUnhealthy,
        ),
        // at least 1 healthy instance
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    (multi_partition_owner_instance_id("instance-zone-a-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-a-1", 2), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", now_s, false)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-b-1", "b", now_s, false)),
            ]),
            Expected::Set(ComparableSet {
                instances: vec!["instance-zone-a-1".into(), "instance-zone-b-1".into()],
                max_unavailable_zones: 1,
                partition_id: TEST_PARTITION_ID,
            }),
        ),
        // no healthy instances
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    (multi_partition_owner_instance_id("instance-zone-a-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-a-1", 2), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", now_s - 120, false)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-b-1", "b", now_s - 180, false)),
            ]),
            Expected::TooManyUnhealthy,
        ),
        // excluding unhealthy instances
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    (multi_partition_owner_instance_id("instance-zone-a-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-a-1", 2), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", now_s - 120, false)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-b-1", "b", now_s, false)),
            ]),
            Expected::Set(ComparableSet {
                instances: vec!["instance-zone-b-1".into()],
                max_unavailable_zones: 0,
                partition_id: TEST_PARTITION_ID,
            }),
        ),
        // highest owner from each zone
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    (multi_partition_owner_instance_id("instance-zone-a-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-a-2", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-2", 2), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", now_s, false)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "a", now_s, false)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-b-1", "b", now_s, false)),
                ("instance-zone-b-2".into(), mk_inst("instance-zone-b-2", "b", now_s, false)),
            ]),
            Expected::Set(ComparableSet {
                instances: vec!["instance-zone-a-2".into(), "instance-zone-b-1".into()],
                max_unavailable_zones: 1,
                partition_id: TEST_PARTITION_ID,
            }),
        ),
        // highest non read-only owner from each zone
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    (multi_partition_owner_instance_id("instance-zone-a-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-a-2", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-2", 2), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", now_s, false)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "a", now_s, true)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-b-1", "b", now_s, false)),
                ("instance-zone-b-2".into(), mk_inst("instance-zone-b-2", "b", now_s, false)),
            ]),
            Expected::Set(ComparableSet {
                instances: vec!["instance-zone-a-1".into(), "instance-zone-b-1".into()],
                max_unavailable_zones: 1,
                partition_id: TEST_PARTITION_ID,
            }),
        ),
        // read-only owner if it's the only one
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    (multi_partition_owner_instance_id("instance-zone-a-2", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-1", 1), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    (multi_partition_owner_instance_id("instance-zone-b-2", 2), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", now_s, false)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "a", now_s, true)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-b-1", "b", now_s, false)),
                ("instance-zone-b-2".into(), mk_inst("instance-zone-b-2", "b", now_s, false)),
            ]),
            Expected::Set(ComparableSet {
                instances: vec!["instance-zone-a-2".into(), "instance-zone-b-1".into()],
                max_unavailable_zones: 1,
                partition_id: TEST_PARTITION_ID,
            }),
        ),
    ];

    for (p_desc, instances, expected) in cases {
        let p_ring = PartitionRing::new(p_desc).unwrap();
        let inst_ring: Arc<dyn InstanceRingReader> = Arc::new(StaticInstanceRing { instances });
        let r = MultiPartitionInstanceRing::new(
            Arc::new(StaticPartitionRingReader::new(p_ring)),
            inst_ring,
            heartbeat_timeout,
        );
        let res = r.get_replication_set_for_partition_and_operation_at(TEST_PARTITION_ID, op, now);
        match expected {
            Expected::EmptyRing => {
                assert!(matches!(res, Err(PartitionRingError::EmptyRing)));
            }
            Expected::TooManyUnhealthy => {
                assert!(matches!(
                    res,
                    Err(PartitionRingError::PartitionTooManyUnhealthyInstances(_))
                ));
            }
            Expected::Set(exp) => {
                let set = res.unwrap();
                let mut ids: Vec<String> = set.instances.into_iter().map(|i| i.id).collect();
                ids.sort();
                let actual = ComparableSet {
                    instances: ids,
                    max_unavailable_zones: set.max_unavailable_zones,
                    partition_id: set.partition_id,
                };
                assert_eq!(exp, actual);
            }
        }
    }
}

// ============================================================================
// partition_instance_lifecycler_test.go (5)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/partition_instance_lifecycler_test.go::TestPartitionInstanceLifecycler
#[tokio::test]
async fn test_partition_instance_lifecycler() {
    let eventually_tick = Duration::from_millis(10);

    // Subtest 1: wait for minimum number of owners
    {
        let mut cfg1a = create_test_pil_config(1, "instance-zone-a-1");
        let mut cfg1b = create_test_pil_config(1, "instance-zone-b-1");
        cfg1a.wait_owners_count_on_pending = 2;
        cfg1a.wait_owners_duration_on_pending = Duration::ZERO;
        cfg1b.wait_owners_count_on_pending = 2;
        cfg1b.wait_owners_duration_on_pending = Duration::ZERO;

        let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let l1a = PartitionInstanceLifecycler::new(
            cfg1a.clone(),
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        let s1a = l1a.into_service();
        start_and_await_running(ServiceContext::background(), s1a.as_ref())
            .await
            .unwrap();

        tokio::time::sleep(2 * cfg1a.polling_interval).await;
        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert_eq!(1, actual.partitions.len());
        assert!(actual.has_partition(1));
        assert_eq!(
            PartitionState::PartitionPending as i32,
            actual.partitions[&1].state
        );
        assert_eq!(
            vec!["instance-zone-a-1".to_string()],
            actual.owners_by_partition()[&1]
        );

        let l1b = PartitionInstanceLifecycler::new(
            cfg1b,
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        let s1b = l1b.into_service();
        start_and_await_running(ServiceContext::background(), s1b.as_ref())
            .await
            .unwrap();

        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert_eq!(
            vec!["instance-zone-a-1".to_string(), "instance-zone-b-1".to_string()],
            actual.owners_by_partition()[&1]
        );

        let ok = poll(Duration::from_secs(3), eventually_tick, || {
            let store = Arc::clone(&store);
            async move {
                get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
                    == PartitionState::PartitionActive
            }
        })
        .await;
        assert!(ok);

        stop_and_await_terminated(ServiceContext::background(), s1b.as_ref())
            .await
            .unwrap();
        stop_and_await_terminated(ServiceContext::background(), s1a.as_ref())
            .await
            .unwrap();
    }

    // Subtest 2: wait for minimum waiting time
    {
        let mut cfg = create_test_pil_config(1, "instance-1");
        cfg.wait_owners_count_on_pending = 1;
        cfg.wait_owners_duration_on_pending = Duration::from_secs(2);

        let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let start_time = std::time::Instant::now();
        let lc = PartitionInstanceLifecycler::new(
            cfg.clone(),
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        let svc = lc.into_service();
        start_and_await_running(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();

        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert_eq!(1, actual.partitions.len());
        assert_eq!(
            PartitionState::PartitionPending as i32,
            actual.partitions[&1].state
        );
        assert_eq!(
            vec!["instance-1".to_string()],
            actual.owners_by_partition()[&1]
        );

        let ok = poll(2 * cfg.wait_owners_duration_on_pending, eventually_tick, || {
            let store = Arc::clone(&store);
            async move {
                get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
                    == PartitionState::PartitionActive
            }
        })
        .await;
        assert!(ok);
        assert!(start_time.elapsed() >= cfg.wait_owners_duration_on_pending);

        stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();
    }

    // Subtest 3: inactive partitions removed only if waiting period passed and no owners
    {
        let mut cfg1a = create_test_pil_config(1, "instance-zone-a-1");
        let mut cfg1b = create_test_pil_config(1, "instance-zone-b-1");
        let mut cfg2a = create_test_pil_config(2, "instance-zone-a-2");
        let del_dur = Duration::from_millis(100);
        cfg1a.delete_inactive_partition_after = del_dur;
        cfg1b.delete_inactive_partition_after = del_dur;
        cfg2a.delete_inactive_partition_after = del_dur;

        let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let l1a =
            PartitionInstanceLifecycler::new(cfg1a, "test", RING_KEY, Arc::clone(&store), None)
                .unwrap();
        let l1b =
            PartitionInstanceLifecycler::new(cfg1b, "test", RING_KEY, Arc::clone(&store), None)
                .unwrap();
        let l2a =
            PartitionInstanceLifecycler::new(cfg2a, "test", RING_KEY, Arc::clone(&store), None)
                .unwrap();
        let s1a = l1a.clone().into_service();
        let s1b = l1b.clone().into_service();
        let s2a = l2a.clone().into_service();
        for s in [&s1a, &s1b, &s2a] {
            start_and_await_running(ServiceContext::background(), s.as_ref())
                .await
                .unwrap();
        }

        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert_eq!(2, actual.partitions.len());
        assert_map_elements_match(
            BTreeMap::from([
                (1, vec!["instance-zone-a-1".into(), "instance-zone-b-1".into()]),
                (2, vec!["instance-zone-a-2".into()]),
            ]),
            actual.owners_by_partition(),
        );

        l1a.change_partition_state(PartitionState::PartitionInactive)
            .await
            .unwrap();
        tokio::time::sleep(2 * del_dur).await;
        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert_eq!(2, actual.partitions.len());

        l1a.set_remove_owner_on_shutdown(true);
        stop_and_await_terminated(ServiceContext::background(), s1a.as_ref())
            .await
            .unwrap();
        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert_map_elements_match(
            BTreeMap::from([
                (1, vec!["instance-zone-b-1".into()]),
                (2, vec!["instance-zone-a-2".into()]),
            ]),
            actual.owners_by_partition(),
        );

        l1b.set_remove_owner_on_shutdown(true);
        stop_and_await_terminated(ServiceContext::background(), s1b.as_ref())
            .await
            .unwrap();
        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert_map_elements_match(
            BTreeMap::from([(2, vec!["instance-zone-a-2".into()])]),
            actual.owners_by_partition(),
        );

        let removed = poll(Duration::from_secs(1), eventually_tick, || {
            let store = Arc::clone(&store);
            async move {
                !get_partition_ring_from_store(store.as_ref(), RING_KEY)
                    .await
                    .has_partition(1)
            }
        })
        .await;
        assert!(removed);
        assert!(
            get_partition_ring_from_store(store.as_ref(), RING_KEY)
                .await
                .has_partition(2)
        );

        stop_and_await_terminated(ServiceContext::background(), s2a.as_ref())
            .await
            .unwrap();
    }

    // Subtest 4: wait until partition exists when create_partition_on_startup=false
    {
        let cfg = create_test_pil_config(1, "instance-1");
        let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let lc = PartitionInstanceLifecycler::new(
            cfg.clone(),
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        lc.set_create_partition_on_startup(false);
        let svc = lc.into_service();

        let svc_bg = Arc::clone(&svc);
        let h = tokio::spawn(async move {
            start_and_await_running(ServiceContext::background(), svc_bg.as_ref())
                .await
                .unwrap();
        });

        tokio::time::sleep(10 * cfg.polling_interval).await;
        assert_eq!(State::Starting, svc.state());
        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert!(!actual.has_partition(1));
        assert!(actual.owners_by_partition().is_empty());

        store
            .cas(
                RING_KEY,
                Box::new(|in_val| {
                    let mut ring = in_val
                        .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                        .cloned()
                        .unwrap_or_default();
                    ring.add_partition(1, PartitionState::PartitionPending, SystemTime::now());
                    Ok(CasOutcome::Update(Box::new(ring)))
                }),
            )
            .await
            .unwrap();

        h.await.unwrap();
        assert_eq!(State::Running, svc.state());
        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert!(actual.has_partition(1));
        assert_map_elements_match(
            BTreeMap::from([(1, vec!["instance-1".into()])]),
            actual.owners_by_partition(),
        );

        stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
            .await
            .unwrap();
    }

    // Subtest 5: stop waiting for partition creation if context gets canceled
    {
        let cfg = create_test_pil_config(1, "instance-1");
        let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let lc = PartitionInstanceLifecycler::new(
            cfg.clone(),
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        lc.set_create_partition_on_startup(false);
        let svc = lc.into_service();

        let start_ctx = ServiceContext::background();
        let start_ctx_bg = start_ctx.clone();
        let svc_bg = Arc::clone(&svc);
        let h = tokio::spawn(async move {
            let err = start_and_await_running(start_ctx_bg, svc_bg.as_ref())
                .await
                .unwrap_err();
            err
        });

        tokio::time::sleep(10 * cfg.polling_interval).await;
        assert_eq!(State::Starting, svc.state());
        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert!(!actual.has_partition(1));

        start_ctx.cancel();
        let err = h.await.unwrap();
        assert!(err.to_string().contains("canceled") || matches!(err, mimir_common::services::ServiceError::Canceled));

        let actual = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        assert!(!actual.has_partition(1));
        assert!(poll(Duration::from_secs(1), eventually_tick, || {
            let svc = Arc::clone(&svc);
            async move { svc.state() == State::Failed }
        })
        .await);
    }

    // Subtest 6: register and unregister with multi-owner ID
    {
        const ID1: &str = "instance-zone-a-1";
        const ID2: &str = "instance-zone-b-1";
        let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));

        let l1p1 = PartitionInstanceLifecycler::new(
            create_test_multi_pil_config(1, ID1),
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        l1p1.set_remove_owner_on_shutdown(true);
        let s1p1 = l1p1.into_service();
        start_and_await_running(ServiceContext::background(), s1p1.as_ref())
            .await
            .unwrap();
        assert_map_elements_match(
            BTreeMap::from([(1, vec![multi_partition_owner_instance_id(ID1, 1)])]),
            get_partition_ring_from_store(store.as_ref(), RING_KEY)
                .await
                .owners_by_partition(),
        );

        let l1p2 = PartitionInstanceLifecycler::new(
            create_test_multi_pil_config(2, ID1),
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        l1p2.set_remove_owner_on_shutdown(false);
        let s1p2 = l1p2.into_service();
        start_and_await_running(ServiceContext::background(), s1p2.as_ref())
            .await
            .unwrap();
        assert_map_elements_match(
            BTreeMap::from([
                (1, vec![multi_partition_owner_instance_id(ID1, 1)]),
                (2, vec![multi_partition_owner_instance_id(ID1, 2)]),
            ]),
            get_partition_ring_from_store(store.as_ref(), RING_KEY)
                .await
                .owners_by_partition(),
        );

        let l2p1 = PartitionInstanceLifecycler::new(
            create_test_multi_pil_config(1, ID2),
            "test",
            RING_KEY,
            Arc::clone(&store),
            None,
        )
        .unwrap();
        l2p1.set_remove_owner_on_shutdown(true);
        let s2p1 = l2p1.into_service();
        start_and_await_running(ServiceContext::background(), s2p1.as_ref())
            .await
            .unwrap();
        assert_map_elements_match(
            BTreeMap::from([
                (
                    1,
                    vec![
                        multi_partition_owner_instance_id(ID1, 1),
                        multi_partition_owner_instance_id(ID2, 1),
                    ],
                ),
                (2, vec![multi_partition_owner_instance_id(ID1, 2)]),
            ]),
            get_partition_ring_from_store(store.as_ref(), RING_KEY)
                .await
                .owners_by_partition(),
        );

        stop_and_await_terminated(ServiceContext::background(), s1p1.as_ref())
            .await
            .unwrap();
        assert_map_elements_match(
            BTreeMap::from([
                (1, vec![multi_partition_owner_instance_id(ID2, 1)]),
                (2, vec![multi_partition_owner_instance_id(ID1, 2)]),
            ]),
            get_partition_ring_from_store(store.as_ref(), RING_KEY)
                .await
                .owners_by_partition(),
        );

        stop_and_await_terminated(ServiceContext::background(), s1p2.as_ref())
            .await
            .unwrap();
        assert_map_elements_match(
            BTreeMap::from([
                (1, vec![multi_partition_owner_instance_id(ID2, 1)]),
                (2, vec![multi_partition_owner_instance_id(ID1, 2)]),
            ]),
            get_partition_ring_from_store(store.as_ref(), RING_KEY)
                .await
                .owners_by_partition(),
        );

        stop_and_await_terminated(ServiceContext::background(), s2p1.as_ref())
            .await
            .unwrap();
        assert_map_elements_match(
            BTreeMap::from([(2, vec![multi_partition_owner_instance_id(ID1, 2)])]),
            get_partition_ring_from_store(store.as_ref(), RING_KEY)
                .await
                .owners_by_partition(),
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_instance_lifecycler_test.go::TestPartitionInstanceLifecycler_GetAndChangePartitionState
#[tokio::test]
async fn test_partition_instance_lifecycler_get_and_change_partition_state() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let cfg = create_test_pil_config(1, "instance-1");
    let lc = PartitionInstanceLifecycler::new(cfg, "test", RING_KEY, Arc::clone(&store), None)
        .unwrap();
    let svc = lc.clone().into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let s = Arc::clone(&store);
        async move {
            get_partition_state_from_store(s.as_ref(), RING_KEY, 1).await
                == PartitionState::PartitionActive
        }
    })
    .await);

    let (actual_state, _) = lc.get_partition_state().await.unwrap();
    assert_eq!(PartitionState::PartitionActive, actual_state);

    let err = lc
        .change_partition_state(PartitionState::PartitionPending)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        PartitionRingError::PartitionStateChangeNotAllowed { .. }
    ));
    assert_eq!(
        PartitionState::PartitionActive,
        lc.get_partition_state().await.unwrap().0
    );

    lc.change_partition_state(PartitionState::PartitionInactive)
        .await
        .unwrap();
    assert_eq!(
        PartitionState::PartitionInactive,
        lc.get_partition_state().await.unwrap().0
    );
    assert_eq!(
        PartitionState::PartitionInactive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );

    // Same state is a no-op
    lc.change_partition_state(PartitionState::PartitionInactive)
        .await
        .unwrap();
    assert_eq!(
        PartitionState::PartitionInactive,
        lc.get_partition_state().await.unwrap().0
    );

    // Inactive -> Pending is disallowed
    let err = lc
        .change_partition_state(PartitionState::PartitionPending)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        PartitionRingError::PartitionStateChangeNotAllowed { .. }
    ));
    assert_eq!(
        PartitionState::PartitionInactive,
        lc.get_partition_state().await.unwrap().0
    );

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_instance_lifecycler_test.go::TestNewPartitionInstanceLifecycler_ValidatesPartitionID
#[test]
fn test_new_partition_instance_lifecycler_validates_partition_id() {
    let cases = [
        (0, 8, true, true),
        (7, 8, true, true),
        (8, 8, true, false),
        (0, 0, true, false),
        (0, -1, true, false),
        (8, 8, false, true),
        (0, 0, false, true),
        (-1, 8, true, false),
        (-1, 8, false, false),
    ];

    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    for (pid, max_p, derived, valid) in cases {
        let mut cfg = derived_tokens_lifecycler_config(pid, derived);
        cfg.max_derived_token_partitions = max_p;
        let res = PartitionInstanceLifecycler::new(cfg, "test", RING_KEY, Arc::clone(&store), None);
        assert_eq!(valid, res.is_ok(), "pid={pid} max={max_p} derived={derived}");
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_instance_lifecycler_test.go::TestPartitionInstanceLifecycler_CreatePartition
#[tokio::test]
async fn test_partition_instance_lifecycler_create_partition() {
    for (derived, expected_scheme, expected_tokens) in [
        (false, PartitionTokenScheme::PartitionTokensStored, OPTIMAL_TOKENS_PER_INSTANCE),
        (true, PartitionTokenScheme::PartitionTokensSmt512, 0),
    ] {
        let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
        let cfg = derived_tokens_lifecycler_config(7, derived);
        let lc = PartitionInstanceLifecycler::new(cfg, "test", RING_KEY, Arc::clone(&store), None)
            .unwrap();
        lc.create_partition_and_register_owner().await.unwrap();
        let desc = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
        let part = &desc.partitions[&7];
        assert_eq!(PartitionState::PartitionPending, part.partition_state());
        assert_eq!(expected_scheme, part.token_scheme());
        assert_eq!(expected_tokens, part.tokens.len());
        assert_eq!(7, desc.owners["instance-1"].owned_partition);
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_instance_lifecycler_test.go::TestPartitionInstanceLifecycler_KeepExistingPartition
#[tokio::test]
async fn test_partition_instance_lifecycler_keep_existing_partition() {
    let existing_cases = [
        PartitionDesc {
            id: 1,
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 1,
            tokens: vec![1, 2],
            ..Default::default()
        },
        PartitionDesc {
            id: 1,
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 1,
            token_scheme: PartitionTokenScheme::PartitionTokensSmt512 as i32,
            ..Default::default()
        },
    ];

    for existing in existing_cases {
        for derived in [false, true] {
            let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
            let ex_clone = existing.clone();
            store
                .cas(
                    RING_KEY,
                    Box::new(move |_| {
                        let mut desc = PartitionRingDesc::default();
                        desc.partitions.insert(ex_clone.id, ex_clone.clone());
                        Ok(CasOutcome::Update(Box::new(desc)))
                    }),
                )
                .await
                .unwrap();

            let cfg = derived_tokens_lifecycler_config(1, derived);
            let lc =
                PartitionInstanceLifecycler::new(cfg, "test", RING_KEY, Arc::clone(&store), None)
                    .unwrap();
            lc.create_partition_and_register_owner().await.unwrap();

            let desc = get_partition_ring_from_store(store.as_ref(), RING_KEY).await;
            assert_eq!(existing, desc.partitions[&1]);
            assert_eq!(1, desc.owners["instance-1"].owned_partition);
        }
    }
}

// ============================================================================
// partition_instance_ring_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/partition_instance_ring_test.go::TestPartitionInstanceRing_GetReplicationSetsForOperation
#[test]
fn test_partition_instance_ring_get_replication_sets_for_operation() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let op = Operation::new(&[InstanceState::Active], &[]);
    let heartbeat_timeout = Duration::from_secs(60);

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct ComparableSet {
        partition_id: i32,
        instances: Vec<String>,
        max_unavailable_zones: usize,
    }

    enum Expected {
        EmptyRing,
        TooManyUnhealthy,
        Sets(Vec<ComparableSet>),
    }

    let mk_inst = |id: &str, zone: &str, state: InstanceState, ts: i64| InstanceDesc {
        id: id.to_string(),
        zone: zone.to_string(),
        state: state as i32,
        timestamp: ts,
        ..Default::default()
    };

    let cases: Vec<(PartitionRingDesc, HashMap<String, InstanceDesc>, Expected)> = vec![
        (
            PartitionRingDesc::default(),
            HashMap::from([
                ("instance-1".into(), mk_inst("instance-1", "", InstanceState::Active, now_s)),
                ("instance-2".into(), mk_inst("instance-2", "", InstanceState::Active, now_s)),
            ]),
            Expected::EmptyRing,
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    ("instance-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::new(),
            Expected::TooManyUnhealthy,
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    ("instance-zone-a-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-a-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    ("instance-zone-b-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", InstanceState::Active, now_s)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "a", InstanceState::Active, now_s)),
                ("instance-zone-b-2".into(), mk_inst("instance-zone-b-2", "b", InstanceState::Active, now_s)),
            ]),
            Expected::Sets(vec![
                ComparableSet { partition_id: 1, instances: vec!["instance-zone-a-1".into()], max_unavailable_zones: 0 },
                ComparableSet { partition_id: 2, instances: vec!["instance-zone-a-2".into(), "instance-zone-b-2".into()], max_unavailable_zones: 1 },
            ]),
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    ("instance-zone-a-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-a-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    ("instance-zone-b-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", InstanceState::Active, now_s)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "a", InstanceState::Active, now_s - 120)),
            ]),
            Expected::TooManyUnhealthy,
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    ("instance-zone-a-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-b-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-a-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    ("instance-zone-b-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", InstanceState::Active, now_s)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-a-1", "a", InstanceState::Leaving, now_s)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "a", InstanceState::Active, now_s)),
                ("instance-zone-b-2".into(), mk_inst("instance-zone-b-2", "b", InstanceState::Active, now_s - 120)),
            ]),
            Expected::Sets(vec![
                ComparableSet { partition_id: 1, instances: vec!["instance-zone-a-1".into()], max_unavailable_zones: 0 },
                ComparableSet { partition_id: 2, instances: vec!["instance-zone-a-2".into()], max_unavailable_zones: 0 },
            ]),
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    ("instance-zone-a-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-b-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-a-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    ("instance-zone-b-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "a", InstanceState::Active, now_s)),
                ("instance-zone-b-2".into(), mk_inst("instance-zone-b-2", "b", InstanceState::Active, now_s)),
            ]),
            Expected::Sets(vec![
                ComparableSet { partition_id: 1, instances: vec!["instance-zone-a-1".into()], max_unavailable_zones: 0 },
                ComparableSet { partition_id: 2, instances: vec!["instance-zone-b-2".into()], max_unavailable_zones: 0 },
            ]),
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, PartitionDesc { state: PartitionState::PartitionActive as i32, ..Default::default() }),
                    (2, PartitionDesc { state: PartitionState::PartitionInactive as i32, ..Default::default() }),
                ]),
                owners: HashMap::from([
                    ("instance-zone-a-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-b-1".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
                    ("instance-zone-a-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                    ("instance-zone-b-2".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
                ]),
            },
            HashMap::from([
                ("instance-zone-a-1".into(), mk_inst("instance-zone-a-1", "fixed", InstanceState::Active, now_s)),
                ("instance-zone-b-1".into(), mk_inst("instance-zone-b-1", "fixed", InstanceState::Active, now_s)),
                ("instance-zone-a-2".into(), mk_inst("instance-zone-a-2", "fixed", InstanceState::Active, now_s)),
                ("instance-zone-b-2".into(), mk_inst("instance-zone-b-2", "fixed", InstanceState::Active, now_s)),
            ]),
            Expected::Sets(vec![
                ComparableSet { partition_id: 1, instances: vec!["instance-zone-a-1".into(), "instance-zone-b-1".into()], max_unavailable_zones: 0 },
                ComparableSet { partition_id: 2, instances: vec!["instance-zone-a-2".into(), "instance-zone-b-2".into()], max_unavailable_zones: 0 },
            ]),
        ),
    ];

    for (p_desc, instances, expected) in cases {
        let p_ring = PartitionRing::new(p_desc).unwrap();
        let r = PartitionInstanceRing::new(
            Arc::new(StaticPartitionRingReader::new(p_ring)),
            Arc::new(StaticInstanceRing { instances }),
            heartbeat_timeout,
        );
        let res = r.get_replication_sets_for_operation_at(op, now);
        match expected {
            Expected::EmptyRing => assert!(matches!(res, Err(PartitionRingError::EmptyRing))),
            Expected::TooManyUnhealthy => assert!(matches!(
                res,
                Err(PartitionRingError::PartitionTooManyUnhealthyInstances(_))
            )),
            Expected::Sets(mut exp) => {
                let sets = res.unwrap();
                let mut actual: Vec<ComparableSet> = sets
                    .into_iter()
                    .map(|s| {
                        let mut ids: Vec<String> = s.instances.into_iter().map(|i| i.id).collect();
                        ids.sort();
                        ComparableSet {
                            partition_id: s.partition_id,
                            instances: ids,
                            max_unavailable_zones: s.max_unavailable_zones,
                        }
                    })
                    .collect();
                actual.sort();
                exp.sort();
                assert_eq!(exp, actual);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_instance_ring_test.go::TestPartitionInstanceRing_ShuffleShard
#[test]
fn test_partition_instance_ring_shuffle_shard() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);

    let mut desc = PartitionRingDesc::default();
    desc.add_partition(1, PartitionState::PartitionActive, now - Duration::from_secs(7200));
    desc.add_partition(2, PartitionState::PartitionActive, now - Duration::from_secs(1800));
    desc.add_partition(3, PartitionState::PartitionActive, now - Duration::from_secs(1800));
    desc.add_or_update_owner("instance-1", OwnerState::OwnerActive, 1, now - Duration::from_secs(1800));
    desc.add_or_update_owner("instance-2", OwnerState::OwnerActive, 2, now - Duration::from_secs(1800));
    desc.add_or_update_owner("instance-3", OwnerState::OwnerActive, 3, now - Duration::from_secs(1800));

    let instances = HashMap::from([
        ("instance-1".into(), InstanceDesc { id: "instance-1".into(), state: InstanceState::Active as i32, timestamp: now_s, ..Default::default() }),
        ("instance-2".into(), InstanceDesc { id: "instance-2".into(), state: InstanceState::Active as i32, timestamp: now_s, ..Default::default() }),
        ("instance-3".into(), InstanceDesc { id: "instance-3".into(), state: InstanceState::Active as i32, timestamp: now_s, ..Default::default() }),
    ]);

    let p_ring = PartitionRing::new(desc).unwrap();
    let r = PartitionInstanceRing::new(
        Arc::new(StaticPartitionRingReader::new(p_ring)),
        Arc::new(StaticInstanceRing { instances }),
        Duration::ZERO,
    );

    let actual = r.shuffle_shard("test", 2).unwrap();
    assert_eq!(2, actual.partition_ring().partitions_count());
    assert_eq!(3, actual.instance_ring().instances_count());

    let actual_lb = r
        .shuffle_shard_with_lookback("test", 2, Duration::from_secs(3600), now)
        .unwrap();
    assert_eq!(3, actual_lb.partition_ring().partitions_count());
    assert_eq!(3, actual_lb.instance_ring().instances_count());
}

// ============================================================================
// partition_instance_rings_test.go (2)
// ============================================================================

async fn write_partition_and_owner(
    kv_client: &dyn KvClient,
    key: &str,
    partition_id: i32,
    owner_id: &str,
) {
    let owner_id = owner_id.to_string();
    kv_client
        .cas(
            key,
            Box::new(move |_| {
                let mut desc = PartitionRingDesc::default();
                desc.add_partition(partition_id, PartitionState::PartitionActive, SystemTime::now());
                desc.add_or_update_owner(&owner_id, OwnerState::OwnerActive, partition_id, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_instance_rings_test.go::TestPartitionInstanceRings
#[tokio::test]
async fn test_partition_instance_rings() {
    let kv_client: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    write_partition_and_owner(kv_client.as_ref(), "ring-0", 0, "instance-0").await;
    write_partition_and_owner(kv_client.as_ref(), "ring-1", 0, "instance-1").await;

    let watchers = PartitionRingWatchers::new(vec![
        PartitionRingWatcher::new(
            "ring-0",
            "ring-0",
            Arc::clone(&kv_client),
            PartitionRingOptions::default(),
            None,
        ),
        PartitionRingWatcher::new(
            "ring-1",
            "ring-1",
            Arc::clone(&kv_client),
            PartitionRingOptions::default(),
            None,
        ),
    ])
    .unwrap();
    let svc = Arc::clone(&watchers).into_service().unwrap();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&watchers);
        async move { w.partition_ring(0).is_some_and(|r| r.partitions_count() > 0) }
    })
    .await);

    let now_s = system_time_to_unix_secs(SystemTime::now());
    let instance_ring: Arc<dyn InstanceRingReader> = Arc::new(StaticInstanceRing {
        instances: HashMap::from([
            (
                "instance-0".into(),
                InstanceDesc {
                    addr: "addr-0".into(),
                    state: InstanceState::Active as i32,
                    timestamp: now_s,
                    zone: "zone-a".into(),
                    ..Default::default()
                },
            ),
            (
                "instance-1".into(),
                InstanceDesc {
                    addr: "addr-1".into(),
                    state: InstanceState::Active as i32,
                    timestamp: now_s,
                    zone: "zone-a".into(),
                    ..Default::default()
                },
            ),
        ]),
    });

    let rings = PartitionInstanceRings::new(&watchers, instance_ring, Duration::from_secs(60));
    assert_eq!(2, rings.count());
    assert_eq!(1, rings.get(0).partition_ring().partitions_count());
    assert_eq!(1, rings.get(1).partition_ring().partitions_count());

    let read_op = Operation::new(&[InstanceState::Active, InstanceState::Pending], &[]);
    let sets = rings
        .get(0)
        .get_replication_sets_for_operation(read_op)
        .unwrap();
    assert_eq!(1, sets.len());
    assert_eq!(1, sets[0].instances.len());
    assert_eq!("addr-0", sets[0].instances[0].addr);

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_instance_rings_test.go::TestPartitionInstanceRings_SingleRing
#[tokio::test]
async fn test_partition_instance_rings_single_ring() {
    let kv_client: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    write_partition_and_owner(kv_client.as_ref(), "ring", 0, "instance-0").await;

    let watchers = PartitionRingWatchers::new(vec![PartitionRingWatcher::new(
        "ring",
        "ring",
        Arc::clone(&kv_client),
        PartitionRingOptions::default(),
        None,
    )])
    .unwrap();
    let svc = Arc::clone(&watchers).into_service().unwrap();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&watchers);
        async move { w.partition_ring(0).is_some_and(|r| r.partitions_count() > 0) }
    })
    .await);

    let rings = PartitionInstanceRings::new(
        &watchers,
        Arc::new(StaticInstanceRing {
            instances: HashMap::new(),
        }),
        Duration::from_secs(60),
    );
    assert_eq!(1, rings.count());
    assert!(Arc::ptr_eq(
        &watchers.watcher(0).unwrap().partition_ring(),
        &rings.get(0).partition_ring()
    ));

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// ============================================================================
// partition_ring_cache_test.go (1)
// ============================================================================

fn cache_test_now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_767_268_800) // 2026-01-01 12:00:00 UTC
}

fn new_partition_ring_for_cache_test(
    cache_size: usize,
    states: &[PartitionState],
) -> Arc<PartitionRing> {
    let now = cache_test_now();
    let mut desc = PartitionRingDesc::default();
    for (i, &state) in states.iter().enumerate() {
        let changed_at = if state == PartitionState::PartitionInactive {
            now - Duration::from_secs(1800)
        } else {
            now - Duration::from_secs(7200)
        };
        desc.add_partition((i + 1) as i32, state, changed_at);
    }
    PartitionRing::with_options(
        desc,
        PartitionRingOptions {
            token_generator: None,
            shuffle_shard_cache_size: cache_size,
        },
    )
    .unwrap()
}

fn shuffle_shard_for_cache_test(
    r: &PartitionRing,
    tenant: &str,
    size: usize,
    lookback: bool,
    now: SystemTime,
) -> Arc<PartitionRing> {
    if lookback {
        r.shuffle_shard_with_lookback(tenant, size, Duration::from_secs(3600), now)
            .unwrap()
    } else {
        r.shuffle_shard(tenant, size).unwrap()
    }
}

fn partition_ring_cache_entries(r: &PartitionRing, lookback: bool) -> usize {
    if lookback {
        r.cache_with_lookback_len()
    } else {
        r.cache_without_lookback_len()
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_cache_test.go::TestPartitionRing_ShuffleShardCache
#[test]
fn test_partition_ring_shuffle_shard_cache() {
    let now = cache_test_now();
    for cache_size in [0usize, 64] {
        for lookback in [false, true] {
            // normalized sizes
            {
                let cases = [
                    (
                        "active",
                        vec![
                            PartitionState::PartitionActive,
                            PartitionState::PartitionActive,
                            PartitionState::PartitionActive,
                        ],
                        vec![1, 2, 3],
                    ),
                    (
                        "mixed",
                        vec![
                            PartitionState::PartitionActive,
                            PartitionState::PartitionActive,
                            PartitionState::PartitionInactive,
                            PartitionState::PartitionPending,
                        ],
                        vec![1, 2],
                    ),
                    ("empty", vec![], vec![]),
                ];
                for (name, states, mut expected) in cases {
                    let r = new_partition_ring_for_cache_test(cache_size, &states);
                    if lookback && name == "mixed" {
                        expected = vec![1, 2, 3];
                    }
                    let first = shuffle_shard_for_cache_test(
                        &r,
                        "tenant",
                        r.partitions_count(),
                        lookback,
                        now,
                    );
                    assert_eq!(expected, first.partition_ids());

                    let non_pending = states
                        .iter()
                        .filter(|&&s| s != PartitionState::PartitionPending)
                        .count();
                    let mut sizes = vec![0usize, non_pending, r.partitions_count(), r.partitions_count() + 1, usize::MAX];
                    for sz in (r.partitions_count() + 2)..(r.partitions_count() + 34) {
                        sizes.push(sz);
                    }
                    for sz in sizes {
                        let actual = shuffle_shard_for_cache_test(&r, "tenant", sz, lookback, now);
                        assert_eq!(expected, actual.partition_ids());
                        assert_eq!(1, partition_ring_cache_entries(&r, lookback));
                        assert!(Arc::ptr_eq(&first, &actual));
                    }
                }
            }

            // distinct keys
            {
                let r = new_partition_ring_for_cache_test(
                    cache_size,
                    &[
                        PartitionState::PartitionActive,
                        PartitionState::PartitionActive,
                        PartitionState::PartitionActive,
                        PartitionState::PartitionActive,
                    ],
                );
                let full = shuffle_shard_for_cache_test(&r, "tenant", usize::MAX, lookback, now);
                assert_eq!(vec![1, 2, 3, 4], full.partition_ids());
                let small = shuffle_shard_for_cache_test(&r, "tenant", 2, lookback, now);
                assert_eq!(2, small.partition_ids().len());
                let larger = shuffle_shard_for_cache_test(&r, "tenant", 3, lookback, now);
                assert_eq!(3, larger.partition_ids().len());
                let other = shuffle_shard_for_cache_test(&r, "other", 2, lookback, now);
                assert_eq!(2, other.partition_ids().len());
                assert!(!Arc::ptr_eq(&small, &other));
                assert_eq!(4, partition_ring_cache_entries(&r, lookback));
                let again = shuffle_shard_for_cache_test(&r, "tenant", 2, lookback, now);
                assert!(Arc::ptr_eq(&small, &again));
            }

            // lookback validity
            if lookback {
                let r = new_partition_ring_for_cache_test(
                    cache_size,
                    &[
                        PartitionState::PartitionActive,
                        PartitionState::PartitionActive,
                        PartitionState::PartitionInactive,
                        PartitionState::PartitionPending,
                    ],
                );
                let first = r
                    .shuffle_shard_with_lookback(
                        "tenant",
                        r.partitions_count() + 1,
                        Duration::from_secs(3600),
                        now,
                    )
                    .unwrap();
                assert_eq!(vec![1, 2, 3], first.partition_ids());

                let boundary = r
                    .shuffle_shard_with_lookback(
                        "tenant",
                        0,
                        Duration::from_secs(3600),
                        now + Duration::from_secs(1800),
                    )
                    .unwrap();
                assert_eq!(vec![1, 2, 3], boundary.partition_ids());
                assert!(Arc::ptr_eq(&first, &boundary));

                let late_time = now + Duration::from_secs(31 * 60);
                let late = r
                    .shuffle_shard_with_lookback(
                        "tenant",
                        usize::MAX,
                        Duration::from_secs(3600),
                        late_time,
                    )
                    .unwrap();
                assert_eq!(vec![1, 2], late.partition_ids());
                assert!(!Arc::ptr_eq(&first, &late));

                let early_again = r
                    .shuffle_shard_with_lookback("tenant", 0, Duration::from_secs(3600), now)
                    .unwrap();
                assert_eq!(vec![1, 2, 3], early_again.partition_ids());
                let late_again = r
                    .shuffle_shard_with_lookback(
                        "tenant",
                        r.partitions_count(),
                        Duration::from_secs(3600),
                        late_time,
                    )
                    .unwrap();
                assert!(Arc::ptr_eq(&late, &late_again));
                assert_eq!(1, r.cache_with_lookback_len());

                let longer = r
                    .shuffle_shard_with_lookback(
                        "tenant",
                        0,
                        Duration::from_secs(7200),
                        late_time,
                    )
                    .unwrap();
                assert_eq!(vec![1, 2, 3], longer.partition_ids());
                assert!(!Arc::ptr_eq(&late, &longer));
                assert_eq!(2, r.cache_with_lookback_len());
            }

            // replacement snapshot
            {
                let old = new_partition_ring_for_cache_test(
                    cache_size,
                    &[PartitionState::PartitionActive, PartitionState::PartitionActive],
                );
                let requested_size = 3;
                let before = shuffle_shard_for_cache_test(&old, "tenant", requested_size, lookback, now);
                assert_eq!(vec![1, 2], before.partition_ids());

                let mut desc = old.desc().clone();
                desc.add_partition(3, PartitionState::PartitionActive, now - Duration::from_secs(7200));
                desc.add_partition(4, PartitionState::PartitionActive, now - Duration::from_secs(7200));
                let replacement =
                    PartitionRing::with_options(desc, old.options().clone()).unwrap();

                let after = shuffle_shard_for_cache_test(&replacement, "tenant", requested_size, lookback, now);
                assert_eq!(requested_size, after.partition_ids().len());
                assert!(!Arc::ptr_eq(&before, &after));

                let full = shuffle_shard_for_cache_test(&replacement, "tenant", 0, lookback, now);
                assert_eq!(vec![1, 2, 3, 4], full.partition_ids());
                assert_eq!(2, partition_ring_cache_entries(&replacement, lookback));

                let old_again = shuffle_shard_for_cache_test(&old, "tenant", requested_size, lookback, now);
                assert!(Arc::ptr_eq(&before, &old_again));
                assert_eq!(vec![1, 2], old_again.partition_ids());
                assert_eq!(1, partition_ring_cache_entries(&old, lookback));
            }

            // concurrent normalized sizes
            {
                let r = new_partition_ring_for_cache_test(
                    cache_size,
                    &[PartitionState::PartitionActive, PartitionState::PartitionActive],
                );
                const WORKERS: usize = 16;
                let barrier = Arc::new(std::sync::Barrier::new(WORKERS));
                let mut handles = Vec::with_capacity(WORKERS);
                for i in 0..WORKERS {
                    let r = Arc::clone(&r);
                    let b = Arc::clone(&barrier);
                    handles.push(std::thread::spawn(move || {
                        b.wait();
                        shuffle_shard_for_cache_test(
                            &r,
                            "tenant",
                            r.partitions_count() + i + 1,
                            lookback,
                            now,
                        )
                    }));
                }
                for h in handles {
                    let sub = h.join().unwrap();
                    assert_eq!(vec![1, 2], sub.partition_ids());
                }
                assert_eq!(1, partition_ring_cache_entries(&r, lookback));
            }
        }
    }
}

// ============================================================================
// partition_ring_clone_test.go (1)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/partition_ring_clone_test.go::TestPartitionRingDesc_Clone
#[test]
fn test_partition_ring_desc_clone() {
    let populated = PartitionRingDesc {
        partitions: HashMap::from([
            (
                1,
                PartitionDesc {
                    id: 1,
                    tokens: vec![1, 2],
                    state: PartitionState::PartitionActive as i32,
                    state_timestamp: 10,
                    state_change_locked: true,
                    state_change_locked_timestamp: 20,
                    ..Default::default()
                },
            ),
            (
                2,
                PartitionDesc {
                    id: 2,
                    state: PartitionState::PartitionDeleted as i32,
                    state_timestamp: 30,
                    ..Default::default()
                },
            ),
            (
                3,
                PartitionDesc {
                    id: 3,
                    tokens: Vec::new(),
                    state: PartitionState::PartitionPending as i32,
                    ..Default::default()
                },
            ),
        ]),
        owners: HashMap::from([
            (
                "active".into(),
                OwnerDesc {
                    owned_partition: 1,
                    state: OwnerState::OwnerActive as i32,
                    updated_timestamp: 10,
                },
            ),
            (
                "deleted".into(),
                OwnerDesc {
                    owned_partition: 2,
                    state: OwnerState::OwnerDeleted as i32,
                    updated_timestamp: 30,
                },
            ),
        ]),
    };

    for desc in [PartitionRingDesc::default(), populated] {
        let mut cloned_box = desc.clone_mergeable();
        let cloned = cloned_box
            .as_any_mut()
            .downcast_mut::<PartitionRingDesc>()
            .unwrap();
        assert_eq!(&desc, cloned);
        let snapshot = desc.clone();
        cloned.partitions.remove(&1);
        cloned.partitions.insert(
            99,
            PartitionDesc {
                id: 99,
                ..Default::default()
            },
        );
        cloned.owners.remove("active");
        cloned.owners.insert(
            "new".into(),
            OwnerDesc {
                owned_partition: 99,
                ..Default::default()
            },
        );
        assert_eq!(snapshot, desc);
    }
}

// ============================================================================
// partition_ring_editor_test.go (3)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/partition_ring_editor_test.go::TestPartitionRingEditor_ChangePartitionState
#[tokio::test]
async fn test_partition_ring_editor_change_partition_state() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    store
        .cas(
            RING_KEY,
            Box::new(|_| {
                let mut desc = PartitionRingDesc::default();
                desc.add_partition(1, PartitionState::PartitionActive, SystemTime::now());
                desc.add_partition(2, PartitionState::PartitionActive, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    let editor = PartitionRingEditor::new(RING_KEY, Arc::clone(&store));
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 2).await
    );

    let err = editor
        .change_partition_state(1, PartitionState::PartitionPending)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        PartitionRingError::PartitionStateChangeNotAllowed { .. }
    ));
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );

    editor
        .change_partition_state(1, PartitionState::PartitionInactive)
        .await
        .unwrap();
    assert_eq!(
        PartitionState::PartitionInactive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 2).await
    );

    // Same state is a no-op
    editor
        .change_partition_state(1, PartitionState::PartitionInactive)
        .await
        .unwrap();
    assert_eq!(
        PartitionState::PartitionInactive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_editor_test.go::TestPartitionRingEditor_RemoveMultiPartitionOwner
#[tokio::test]
async fn test_partition_ring_editor_remove_multi_partition_owner() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    store
        .cas(
            RING_KEY,
            Box::new(|_| {
                let mut desc = PartitionRingDesc::default();
                let now = SystemTime::now();
                desc.add_partition(1, PartitionState::PartitionActive, now);
                desc.add_or_update_owner(&multi_partition_owner_instance_id("instance-1", 1), OwnerState::OwnerActive, 1, now);
                desc.add_or_update_owner(&multi_partition_owner_instance_id("instance-1", 2), OwnerState::OwnerActive, 2, now);
                desc.add_or_update_owner(&multi_partition_owner_instance_id("instance-2", 1), OwnerState::OwnerActive, 1, now);
                desc.add_or_update_owner(&multi_partition_owner_instance_id("instance-2", 2), OwnerState::OwnerActive, 2, now);
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    assert_map_elements_match(
        BTreeMap::from([
            (
                1,
                vec![
                    multi_partition_owner_instance_id("instance-1", 1),
                    multi_partition_owner_instance_id("instance-2", 1),
                ],
            ),
            (
                2,
                vec![
                    multi_partition_owner_instance_id("instance-1", 2),
                    multi_partition_owner_instance_id("instance-2", 2),
                ],
            ),
        ]),
        get_partition_ring_from_store(store.as_ref(), RING_KEY)
            .await
            .owners_by_partition(),
    );

    let editor = PartitionRingEditor::new(RING_KEY, Arc::clone(&store));
    editor
        .remove_multi_partition_owner("instance-1", 1)
        .await
        .unwrap();

    assert_map_elements_match(
        BTreeMap::from([
            (1, vec![multi_partition_owner_instance_id("instance-2", 1)]),
            (
                2,
                vec![
                    multi_partition_owner_instance_id("instance-1", 2),
                    multi_partition_owner_instance_id("instance-2", 2),
                ],
            ),
        ]),
        get_partition_ring_from_store(store.as_ref(), RING_KEY)
            .await
            .owners_by_partition(),
    );
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_editor_test.go::TestPartitionRingEditor_SetPartitionStateChangeLock
#[tokio::test]
async fn test_partition_ring_editor_set_partition_state_change_lock() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    store
        .cas(
            RING_KEY,
            Box::new(|_| {
                let mut desc = PartitionRingDesc::default();
                desc.add_partition(1, PartitionState::PartitionActive, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    let editor = PartitionRingEditor::new(RING_KEY, Arc::clone(&store));
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );

    editor.set_partition_state_change_lock(1, true).await.unwrap();
    let err = editor
        .change_partition_state(1, PartitionState::PartitionInactive)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        PartitionRingError::PartitionStateChangeLocked
    ));
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );

    editor.set_partition_state_change_lock(1, false).await.unwrap();
    editor
        .change_partition_state(1, PartitionState::PartitionInactive)
        .await
        .unwrap();
    assert_eq!(
        PartitionState::PartitionInactive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );
}

// ============================================================================
// partition_ring_http_test.go (3)
// ============================================================================

fn partition_ring_page_test_desc() -> PartitionRingDesc {
    let now_s = system_time_to_unix_secs(SystemTime::now());
    PartitionRingDesc {
        partitions: HashMap::from([
            (
                1,
                PartitionDesc {
                    state: PartitionState::PartitionActive as i32,
                    state_timestamp: now_s,
                    tokens: vec![1_000_000, 3_000_000, 6_000_000],
                    ..Default::default()
                },
            ),
            (
                2,
                PartitionDesc {
                    state: PartitionState::PartitionInactive as i32,
                    state_timestamp: now_s,
                    tokens: vec![2_000_000, 4_000_000, 5_000_000, 7_000_000],
                    ..Default::default()
                },
            ),
        ]),
        owners: HashMap::from([
            ("ingester-zone-a-0".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
            ("ingester-zone-a-1".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
            ("ingester-zone-b-0".into(), OwnerDesc { owned_partition: 1, ..Default::default() }),
            ("ingester-zone-b-1".into(), OwnerDesc { owned_partition: 2, ..Default::default() }),
            ("ingester-zone-b-2".into(), OwnerDesc { owned_partition: 3, ..Default::default() }),
        ]),
    }
}

async fn test_partition_ring_page_view(part_ring: Arc<PartitionRing>, expected_scheme: &str) {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let app = partition_ring_router_at(
        "/partition-ring",
        Arc::new(StaticPartitionRingReader::new(part_ring)),
        Arc::new(PartitionRingEditor::new(RING_KEY, store)),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/partition-ring"))
        .send()
        .await
        .unwrap();
    assert_eq!(200, resp.status().as_u16());
    assert_eq!(
        "text/html",
        resp.headers().get("content-type").unwrap().to_str().unwrap()
    );
    let body = resp.text().await.unwrap();

    let row1 = Regex::new(&format!(
        r"(?m)<td>\s*1\s*</td>\s*<td>\s*Active\s*</td>\s*<td>\s*[^<]+\s*</td>\s*<td>\s*ingester-zone-a-0\s*<br />\s*ingester-zone-b-0\s*<br />\s*</td>\s*<td>\s*{expected_scheme}\s*</td>\s*<td>\s*3\s*</td>\s*<td>\s*99\.9%\s*</td>"
    ))
    .unwrap();
    assert!(row1.is_match(&body), "row1 mismatch in:\n{body}");

    let row2 = Regex::new(&format!(
        r"(?m)<td>\s*2\s*</td>\s*<td>\s*Inactive\s*</td>\s*<td>\s*[^<]+\s*</td>\s*<td>\s*ingester-zone-a-1\s*<br />\s*ingester-zone-b-1\s*<br />\s*</td>\s*<td>\s*{expected_scheme}\s*</td>\s*<td>\s*4\s*</td>\s*<td>\s*0\.0931%\s*</td>"
    ))
    .unwrap();
    assert!(row2.is_match(&body), "row2 mismatch in:\n{body}");

    let row3 = Regex::new(
        r"(?m)<td>\s*3\s*</td>\s*<td>\s*Corrupt\s*</td>\s*<td>\s*N/A\s*</td>\s*<td>\s*ingester-zone-b-2\s*<br />\s*</td>\s*<td>\s*</td>\s*<td>\s*0\s*</td>\s*<td>\s*0%\s*</td>",
    )
    .unwrap();
    assert!(row3.is_match(&body), "row3 mismatch in:\n{body}");

    let show_btn = Regex::new(
        r#"(?m)<input type="button" value="Show Tokens" onclick="window\.location\.href = '\?tokens=true'"/>"#,
    )
    .unwrap();
    assert!(show_btn.is_match(&body));

    // With ?tokens=true
    let resp_toks = client
        .get(format!("http://{addr}/partition-ring?tokens=true"))
        .send()
        .await
        .unwrap();
    assert_eq!(200, resp_toks.status().as_u16());
    let body_toks = resp_toks.text().await.unwrap();
    let hide_btn = Regex::new(
        r#"(?m)<input type="button" value="Hide Tokens" onclick="window\.location\.href = '\?tokens=false'"/>"#,
    )
    .unwrap();
    assert!(hide_btn.is_match(&body_toks));
    let p1_toks = Regex::new(
        r"(?m)<h2>\s*Partition: 1\s*</h2>\s*<p>\s*Tokens:<br/>\s*1000000\s*3000000\s*6000000\s*</p>",
    )
    .unwrap();
    assert!(p1_toks.is_match(&body_toks));
    let p2_toks = Regex::new(
        r"(?m)<h2>\s*Partition: 2\s*</h2>\s*<p>\s*Tokens:<br/>\s*2000000\s*4000000\s*5000000\s*7000000\s*</p>",
    )
    .unwrap();
    assert!(p2_toks.is_match(&body_toks));
    let p3_toks = Regex::new(r"(?m)<h2>\s*Partition: 3\s*</h2>\s*<p>\s*Tokens:<br/>\s*</p>").unwrap();
    assert!(p3_toks.is_match(&body_toks));

    srv.abort();
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_http_test.go::TestPartitionRingPageHandler_ViewPage
#[tokio::test]
async fn test_partition_ring_page_handler_view_page() {
    let part_ring = PartitionRing::new(partition_ring_page_test_desc()).unwrap();
    test_partition_ring_page_view(part_ring, "Stored").await;
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_http_test.go::TestPartitionRingPageHandler_ViewPageWithDerivedTokens
#[tokio::test]
async fn test_partition_ring_page_handler_view_page_with_derived_tokens() {
    let mut desc = partition_ring_page_test_desc();
    let mut tokens_by_partition = HashMap::new();
    for (&id, part) in &mut desc.partitions {
        tokens_by_partition.insert(id, std::mem::take(&mut part.tokens));
        part.token_scheme = PartitionTokenScheme::PartitionTokensSmt512 as i32;
    }
    let opts = PartitionRingOptions {
        token_generator: Some(Arc::new(CachedFnTokenGen {
            tokens: tokens_by_partition,
        })),
        ..Default::default()
    };
    let part_ring = PartitionRing::with_options(desc, opts).unwrap();
    test_partition_ring_page_view(part_ring, "Smt512").await;
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_http_test.go::TestPartitionRingPageHandler_ChangePartitionState
#[tokio::test]
async fn test_partition_ring_page_handler_change_partition_state() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    store
        .cas(
            RING_KEY,
            Box::new(|_| {
                let mut desc = PartitionRingDesc::default();
                desc.add_partition(1, PartitionState::PartitionActive, SystemTime::now());
                desc.add_partition(2, PartitionState::PartitionActive, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    let watcher = PartitionRingWatcher::new(
        TEST_RING_NAME,
        RING_KEY,
        Arc::clone(&store),
        PartitionRingOptions::default(),
        None,
    );
    let w_svc = Arc::clone(&watcher).into_service();
    start_and_await_running(ServiceContext::background(), w_svc.as_ref())
        .await
        .unwrap();

    let editor = Arc::new(PartitionRingEditor::new(RING_KEY, Arc::clone(&store)));
    let app = partition_ring_router_at(
        "/partition-ring",
        Arc::clone(&watcher) as Arc<dyn mimir_ring::partition_ring::PartitionRingReader>,
        editor,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let url = format!("http://{addr}/partition-ring");

    // 1. invalid partition ID
    let r = client
        .post(&url)
        .form(&[
            ("action", "change_state"),
            ("partition_id", "xxx"),
            ("partition_state", PartitionState::PartitionActive.as_str_name()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(400, r.status().as_u16());
    assert!(r.text().await.unwrap().contains("invalid partition ID"));

    // 2. partition does not exist
    let r = client
        .post(&url)
        .form(&[
            ("action", "change_state"),
            ("partition_id", "100"),
            ("partition_state", PartitionState::PartitionActive.as_str_name()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(400, r.status().as_u16());
    assert!(r
        .text()
        .await
        .unwrap()
        .contains(&PartitionRingError::PartitionDoesNotExist.to_string()));

    // 3. invalid state
    let r = client
        .post(&url)
        .form(&[
            ("action", "change_state"),
            ("partition_id", "1"),
            ("partition_state", "xxx"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(400, r.status().as_u16());
    assert!(r.text().await.unwrap().contains("invalid partition state"));

    // 4. state change not allowed
    let r = client
        .post(&url)
        .form(&[
            ("action", "change_state"),
            ("partition_id", "1"),
            ("partition_state", PartitionState::PartitionPending.as_str_name()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(400, r.status().as_u16());
    assert!(r
        .text()
        .await
        .unwrap()
        .contains("partition state change not allowed"));

    // 5. succeed if allowed
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );
    let r = client
        .post(&url)
        .form(&[
            ("action", "change_state"),
            ("partition_id", "1"),
            ("partition_state", PartitionState::PartitionInactive.as_str_name()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(302, r.status().as_u16());
    assert_eq!(
        PartitionState::PartitionInactive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );
    assert_eq!(
        PartitionState::PartitionActive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 2).await
    );

    // 6. state_change_lock locks and unlocks
    let r = client
        .post(&url)
        .form(&[
            ("action", "state_change_lock"),
            ("partition_id", "1"),
            ("locked", "true"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(302, r.status().as_u16());
    assert!(
        get_partition_ring_from_store(store.as_ref(), RING_KEY)
            .await
            .partitions[&1]
            .state_change_locked
    );

    let r = client
        .post(&url)
        .form(&[
            ("action", "state_change_lock"),
            ("partition_id", "1"),
            ("locked", "false"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(302, r.status().as_u16());
    assert!(
        !get_partition_ring_from_store(store.as_ref(), RING_KEY)
            .await
            .partitions[&1]
            .state_change_locked
    );

    // 7. change_state_and_lock also locks
    store
        .cas(
            RING_KEY,
            Box::new(|in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                let tokens = desc.partitions.get(&1).map(|p| p.tokens.clone()).unwrap_or_default();
                desc.partitions.insert(
                    1,
                    PartitionDesc {
                        id: 1,
                        state: PartitionState::PartitionActive as i32,
                        state_timestamp: system_time_to_unix_secs(SystemTime::now()),
                        tokens,
                        ..Default::default()
                    },
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    let r = client
        .post(&url)
        .form(&[
            ("action", "change_state_and_lock"),
            ("partition_id", "1"),
            ("partition_state", PartitionState::PartitionInactive.as_str_name()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(302, r.status().as_u16());
    assert_eq!(
        PartitionState::PartitionInactive,
        get_partition_state_from_store(store.as_ref(), RING_KEY, 1).await
    );
    assert!(
        get_partition_ring_from_store(store.as_ref(), RING_KEY)
            .await
            .partitions[&1]
            .state_change_locked
    );

    srv.abort();
    stop_and_await_terminated(ServiceContext::background(), w_svc.as_ref())
        .await
        .unwrap();
}

// ============================================================================
// partition_ring_model_test.go (15)
// ============================================================================

fn p_desc(id: i32, tokens: Vec<u32>, state: PartitionState, ts: i64) -> PartitionDesc {
    PartitionDesc {
        id,
        tokens,
        state: state as i32,
        state_timestamp: ts,
        ..Default::default()
    }
}

fn o_desc(part: i32, state: OwnerState, ts: i64) -> OwnerDesc {
    OwnerDesc {
        owned_partition: part,
        state: state as i32,
        updated_timestamp: ts,
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_countPartitionsByState
#[test]
fn test_partition_ring_desc_count_partitions_by_state() {
    let empty = PartitionRingDesc::default();
    assert_eq!(
        HashMap::from([
            (PartitionState::PartitionPending, 0),
            (PartitionState::PartitionActive, 0),
            (PartitionState::PartitionInactive, 0),
        ]),
        empty.count_partitions_by_state()
    );

    let active_only = PartitionRingDesc {
        partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
        owners: HashMap::new(),
    };
    assert_eq!(
        HashMap::from([
            (PartitionState::PartitionPending, 0),
            (PartitionState::PartitionActive, 1),
            (PartitionState::PartitionInactive, 0),
        ]),
        active_only.count_partitions_by_state()
    );

    let mixed = PartitionRingDesc {
        partitions: HashMap::from([
            (1, p_desc(1, vec![1], PartitionState::PartitionActive, 10)),
            (2, p_desc(2, vec![2], PartitionState::PartitionActive, 20)),
            (3, p_desc(3, vec![3], PartitionState::PartitionActive, 30)),
            (4, p_desc(4, vec![4], PartitionState::PartitionInactive, 40)),
            (5, p_desc(5, vec![5], PartitionState::PartitionInactive, 50)),
            (6, p_desc(6, vec![6], PartitionState::PartitionPending, 60)),
        ]),
        owners: HashMap::from([
            ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
            ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
        ]),
    };
    assert_eq!(
        HashMap::from([
            (PartitionState::PartitionPending, 1),
            (PartitionState::PartitionActive, 3),
            (PartitionState::PartitionInactive, 2),
        ]),
        mixed.count_partitions_by_state()
    );
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_AddOrUpdateOwner
#[test]
fn test_partition_ring_desc_add_or_update_owner() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);

    let mut desc = PartitionRingDesc::default();
    assert!(desc.add_or_update_owner("instance-1", OwnerState::OwnerActive, 1, now));
    assert_eq!(
        PartitionRingDesc {
            partitions: HashMap::new(),
            owners: HashMap::from([("instance-1".into(), o_desc(1, OwnerState::OwnerActive, now_s))]),
        },
        desc
    );

    assert!(desc.add_or_update_owner(
        "instance-1",
        OwnerState::OwnerActive,
        2,
        now + Duration::from_secs(1)
    ));
    assert_eq!(
        PartitionRingDesc {
            partitions: HashMap::new(),
            owners: HashMap::from([(
                "instance-1".into(),
                o_desc(2, OwnerState::OwnerActive, now_s + 1)
            )]),
        },
        desc
    );

    // No-op if owner already exists with same state & partition
    assert!(!desc.add_or_update_owner(
        "instance-1",
        OwnerState::OwnerActive,
        2,
        now + Duration::from_secs(5)
    ));
    assert_eq!(now_s + 1, desc.owners["instance-1"].updated_timestamp);
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_AddPartition
#[test]
fn test_partition_ring_desc_merge_add_partition() {
    let cases = [
        (
            PartitionRingDesc::default(),
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::new(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::new(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::new(),
            },
        ),
        (
            PartitionRingDesc::default(),
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                ]),
            },
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20))]),
                owners: HashMap::new(),
            },
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                    ("ingester-zone-b-1".into(), o_desc(2, OwnerState::OwnerActive, 25)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
                    ("ingester-zone-b-1".into(), o_desc(2, OwnerState::OwnerActive, 25)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20))]),
                owners: HashMap::from([
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                    ("ingester-zone-b-1".into(), o_desc(2, OwnerState::OwnerActive, 25)),
                ]),
            },
        ),
    ];

    for (local, incoming, exp_local, exp_change) in cases {
        for local_cas in [true, false] {
            let mut l = local.clone();
            let mut r = incoming.clone();
            let change = l.merge(&mut r, local_cas).unwrap();
            assert_eq!(exp_local, l);
            let ch = change
                .unwrap()
                .as_any()
                .downcast_ref::<PartitionRingDesc>()
                .unwrap()
                .clone();
            assert_eq!(exp_change, ch);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_UpdatePartition
#[test]
fn test_partition_ring_desc_merge_update_partition() {
    let base_owners = HashMap::from([
        ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
        ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
        ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 15)),
        ("ingester-zone-b-1".into(), o_desc(2, OwnerState::OwnerActive, 25)),
    ]);

    let cases: Vec<(PartitionRingDesc, PartitionRingDesc, PartitionRingDesc, Option<PartitionRingDesc>)> = vec![
        // newer timestamp
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: base_owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionInactive, 30)),
                ]),
                owners: base_owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionInactive, 30)),
                ]),
                owners: base_owners.clone(),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::from([(2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionInactive, 30))]),
                owners: HashMap::new(),
            }),
        ),
        // older timestamp
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: base_owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionInactive, 10)),
                ]),
                owners: base_owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: base_owners.clone(),
            },
            None,
        ),
        // same state, newer timestamp
        (
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: base_owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 30)),
                ]),
                owners: base_owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 30)),
                ]),
                owners: base_owners,
            },
            Some(PartitionRingDesc {
                partitions: HashMap::from([(2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 30))]),
                owners: HashMap::new(),
            }),
        ),
        // token scheme and tokens are not merged
        (
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::new(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(
                    1,
                    PartitionDesc {
                        id: 1,
                        token_scheme: PartitionTokenScheme::PartitionTokensSmt512 as i32,
                        state: PartitionState::PartitionInactive as i32,
                        state_timestamp: 20,
                        ..Default::default()
                    },
                )]),
                owners: HashMap::new(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionInactive, 20))]),
                owners: HashMap::new(),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionInactive, 20))]),
                owners: HashMap::new(),
            }),
        ),
    ];

    for (local, incoming, exp_local, exp_change) in cases {
        for local_cas in [true, false] {
            let mut l = local.clone();
            let mut r = incoming.clone();
            let change = l.merge(&mut r, local_cas).unwrap();
            assert_eq!(exp_local, l);
            let ch = change.map(|b| {
                b.as_any()
                    .downcast_ref::<PartitionRingDesc>()
                    .unwrap()
                    .clone()
            });
            assert_eq!(exp_change, ch);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_StateChangeLocked
#[test]
fn test_partition_ring_desc_merge_state_change_locked() {
    let now = SystemTime::now();
    let mut r1 = PartitionRingDesc::default();
    r1.add_partition(1, PartitionState::PartitionActive, now);
    r1.update_partition_state_change_lock(1, false, now);

    let mut r2 = PartitionRingDesc::default();
    r2.add_partition(1, PartitionState::PartitionActive, now);
    r2.update_partition_state_change_lock(1, true, now + Duration::from_secs(1));

    let merged = r1.merge(&mut r2, false).unwrap().unwrap();
    assert!(r1.partitions[&1].state_change_locked);
    assert_eq!(
        system_time_to_unix_secs(now + Duration::from_secs(1)),
        r1.partitions[&1].state_change_locked_timestamp
    );
    let ch = merged.as_any().downcast_ref::<PartitionRingDesc>().unwrap();
    assert!(ch.partitions[&1].state_change_locked);
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_RemovePartition
#[test]
fn test_partition_ring_desc_merge_remove_partition() {
    let now = UNIX_EPOCH + Duration::from_secs(10_000);
    let now_s = 10_000i64;
    let owners = HashMap::from([
        ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
        ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
    ]);

    let cases: Vec<(bool, PartitionRingDesc, PartitionRingDesc, PartitionRingDesc, Option<PartitionRingDesc>)> = vec![
        // localCAS removes partition -> PartitionDeleted at now
        (
            true,
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, now_s)),
                ]),
                owners: owners.clone(),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::from([(2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, now_s))]),
                owners: HashMap::new(),
            }),
        ),
        // localCAS when already deleted -> no change
        (
            true,
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 20)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 20)),
                ]),
                owners: owners.clone(),
            },
            None,
        ),
        // incoming deleted newer timestamp
        (
            false,
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 30)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 30)),
                ]),
                owners: owners.clone(),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::from([(2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 30))]),
                owners: HashMap::new(),
            }),
        ),
        // incoming deleted equal timestamp -> deletion wins
        (
            false,
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 20)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 20)),
                ]),
                owners: owners.clone(),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::from([(2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 20))]),
                owners: HashMap::new(),
            }),
        ),
        // incoming deleted older timestamp -> ignored
        (
            false,
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionDeleted, 10)),
                ]),
                owners: owners.clone(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([
                    (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
                    (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
                ]),
                owners,
            },
            None,
        ),
    ];

    for (local_cas, mut local, incoming, exp_local, exp_change) in cases {
        let change = local.merge_with_time(&incoming, local_cas, now);
        assert_eq!(exp_local, local);
        assert_eq!(exp_change, change);
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_AddOwner
#[test]
fn test_partition_ring_desc_merge_add_owner() {
    let cases = [
        (
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::new(),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10))]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10))]),
            },
            PartitionRingDesc {
                partitions: HashMap::new(),
                owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10))]),
            },
        ),
        (
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10))]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10))]),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: HashMap::new(),
                owners: HashMap::from([("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, 20))]),
            },
        ),
    ];

    for (local, incoming, exp_local, exp_change) in cases {
        for local_cas in [true, false] {
            let mut l = local.clone();
            let mut r = incoming.clone();
            let change = l.merge(&mut r, local_cas).unwrap().unwrap();
            assert_eq!(exp_local, l);
            let ch = change.as_any().downcast_ref::<PartitionRingDesc>().unwrap();
            assert_eq!(&exp_change, ch);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_RemoveOwner
#[test]
fn test_partition_ring_desc_merge_remove_owner() {
    let now = UNIX_EPOCH + Duration::from_secs(10_000);
    let now_s = 10_000i64;
    let parts = HashMap::from([
        (1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, 10)),
        (2, p_desc(2, vec![4, 5, 6], PartitionState::PartitionActive, 20)),
    ]);

    let cases: Vec<(bool, PartitionRingDesc, PartitionRingDesc, PartitionRingDesc, Option<PartitionRingDesc>)> = vec![
        (
            true,
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10))]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, now_s)),
                ]),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::new(),
                owners: HashMap::from([("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, now_s))]),
            }),
        ),
        (
            true,
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10))]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 20)),
                ]),
            },
            None,
        ),
        (
            false,
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 30)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 30)),
                ]),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::new(),
                owners: HashMap::from([("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 30))]),
            }),
        ),
        (
            false,
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 20)),
                ]),
            },
            Some(PartitionRingDesc {
                partitions: HashMap::new(),
                owners: HashMap::from([("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 20))]),
            }),
        ),
        (
            false,
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts.clone(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerDeleted, 10)),
                ]),
            },
            PartitionRingDesc {
                partitions: parts,
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, 10)),
                    ("ingester-zone-a-1".into(), o_desc(2, OwnerState::OwnerActive, 20)),
                ]),
            },
            None,
        ),
    ];

    for (local_cas, mut local, incoming, exp_local, exp_change) in cases {
        let change = local.merge_with_time(&incoming, local_cas, now);
        assert_eq!(exp_local, local);
        assert_eq!(exp_change, change);
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_EdgeCases
#[test]
fn test_partition_ring_desc_merge_edge_cases() {
    let mut desc = PartitionRingDesc::default();
    let mut empty = PartitionRingDesc::default();
    assert!(desc.merge(&mut empty, false).unwrap().is_none());

    let remote = PartitionRingDesc::default();
    let codec = partition_ring_desc_codec();
    let encoded = codec.encode(&remote).unwrap();
    let decoded = codec.decode(&encoded).unwrap();
    let mut local = decoded.downcast_ref::<PartitionRingDesc>().unwrap().clone();
    assert!(local.partitions.is_empty());
    assert!(local.owners.is_empty());

    let mut incoming = PartitionRingDesc::default();
    let ts = UNIX_EPOCH + Duration::from_secs(10);
    incoming.add_partition(1, PartitionState::PartitionActive, ts);
    incoming.add_or_update_owner("instance-1", OwnerState::OwnerActive, 1, ts);

    let change = local.merge(&mut incoming.clone(), false).unwrap().unwrap();
    assert_eq!(incoming, local);
    assert_eq!(
        &incoming,
        change.as_any().downcast_ref::<PartitionRingDesc>().unwrap()
    );
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_RemoveTombstones
#[test]
fn test_partition_ring_desc_remove_tombstones() {
    let now = SystemTime::now();
    let mk = || {
        let mut d = PartitionRingDesc::default();
        d.add_partition(1, PartitionState::PartitionActive, now + Duration::from_secs(1));
        d.add_partition(2, PartitionState::PartitionInactive, now + Duration::from_secs(2));
        d.add_partition(3, PartitionState::PartitionDeleted, now + Duration::from_secs(3));
        d.add_or_update_owner("owner-1", OwnerState::OwnerActive, 1, now + Duration::from_secs(4));
        d.add_or_update_owner("owner-2", OwnerState::OwnerActive, 2, now + Duration::from_secs(4));
        d.add_or_update_owner("owner-3", OwnerState::OwnerDeleted, 3, now + Duration::from_secs(4));
        d
    };

    let mut d = mk();
    let (total, removed) = d.remove_tombstones(None);
    assert_eq!((0, 2), (total, removed));
    assert!(!d.has_partition(3));
    assert!(!d.has_owner("owner-3"));

    let mut d = mk();
    let (total, removed) = d.remove_tombstones(Some(now));
    assert_eq!((2, 0), (total, removed));
    assert!(d.has_partition(3));
    assert!(d.has_owner("owner-3"));

    let (total, removed) = d.remove_tombstones(Some(now + Duration::from_secs(3)));
    assert_eq!((1, 1), (total, removed));
    assert!(!d.has_partition(3));
    assert!(d.has_owner("owner-3"));

    let (total, removed) = d.remove_tombstones(Some(now + Duration::from_secs(4)));
    assert_eq!((0, 1), (total, removed));
    assert!(!d.has_partition(3));
    assert!(!d.has_owner("owner-3"));
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_UpdatePartitionState
#[test]
fn test_partition_ring_desc_update_partition_state() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);

    let mut desc = PartitionRingDesc::default();
    desc.add_partition(1, PartitionState::PartitionActive, now);
    desc.update_partition_state_change_lock(1, true, now);
    let err = desc
        .update_partition_state(1, PartitionState::PartitionInactive, now + Duration::from_secs(1))
        .unwrap_err();
    assert!(matches!(err, PartitionRingError::PartitionStateChangeLocked));
    assert_eq!(PartitionState::PartitionActive as i32, desc.partitions[&1].state);
    assert_eq!(now_s, desc.partitions[&1].state_timestamp);

    desc.update_partition_state_change_lock(1, false, now + Duration::from_secs(2));
    let changed = desc
        .update_partition_state(1, PartitionState::PartitionInactive, now + Duration::from_secs(3))
        .unwrap();
    assert!(changed);
    assert_eq!(PartitionState::PartitionInactive as i32, desc.partitions[&1].state);
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_PartitionOwnersCountUpdatedBefore
#[test]
fn test_partition_ring_desc_partition_owners_count_updated_before() {
    let now = SystemTime::now();
    let mut desc = PartitionRingDesc::default();
    desc.add_partition(1, PartitionState::PartitionActive, now);
    desc.add_partition(2, PartitionState::PartitionActive, now);
    desc.add_or_update_owner("owner-1-a", OwnerState::OwnerActive, 1, now);
    desc.add_or_update_owner("owner-1-b", OwnerState::OwnerActive, 1, now - Duration::from_secs(1));
    desc.add_or_update_owner("owner-1-c", OwnerState::OwnerActive, 1, now - Duration::from_secs(2));
    desc.add_or_update_owner("owner-2-a", OwnerState::OwnerActive, 2, now - Duration::from_secs(3));

    assert_eq!(2, desc.partition_owners_count_updated_before(1, now));
    assert_eq!(1, desc.partition_owners_count_updated_before(1, now - Duration::from_secs(1)));
    assert_eq!(0, desc.partition_owners_count_updated_before(1, now - Duration::from_secs(2)));
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_Merge_FutureTimestamps
#[test]
fn test_partition_ring_desc_merge_future_timestamps() {
    let now = UNIX_EPOCH + Duration::from_secs(10_000);
    let valid = 10_000i64;
    let within_skew = system_time_to_unix_secs(now + MAX_FUTURE_TIMESTAMP_SKEW);
    let skewed = system_time_to_unix_secs(now + Duration::from_secs(90 * 24 * 3600));

    let p_locked = |id: i32, st: PartitionState, ts: i64, locked: bool, lts: i64| PartitionDesc {
        id,
        tokens: vec![1, 2, 3],
        state: st as i32,
        state_timestamp: ts,
        state_change_locked: locked,
        state_change_locked_timestamp: lts,
        ..Default::default()
    };

    let cases: Vec<(bool, PartitionRingDesc, PartitionRingDesc, PartitionRingDesc, Option<PartitionRingDesc>)> = vec![
        // incoming partition state too far in the future is ignored
        (
            false,
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, valid))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionInactive, skewed))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, valid))]), owners: HashMap::new() },
            None,
        ),
        // incoming within skew is accepted
        (
            false,
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, valid))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionInactive, within_skew))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionInactive, within_skew))]), owners: HashMap::new() },
            Some(PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionInactive, within_skew))]), owners: HashMap::new() }),
        ),
        // new partition with state too far in future is not added
        (
            false,
            PartitionRingDesc::default(),
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, skewed))]), owners: HashMap::new() },
            PartitionRingDesc::default(),
            None,
        ),
        // new partition with only lock too far in future is added
        (
            false,
            PartitionRingDesc::default(),
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid, true, skewed))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid, true, skewed))]), owners: HashMap::new() },
            Some(PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid, true, skewed))]), owners: HashMap::new() }),
        ),
        // local lock too far in future replaced by never locked
        (
            false,
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid, true, skewed))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, valid))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, valid))]), owners: HashMap::new() },
            Some(PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, valid))]), owners: HashMap::new() }),
        ),
        // incoming lock too far in future is ignored
        (
            false,
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid - 10, false, valid - 10))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionInactive, valid, true, skewed))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionInactive, valid, false, valid - 10))]), owners: HashMap::new() },
            Some(PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionInactive, valid, false, valid - 10))]), owners: HashMap::new() }),
        ),
        // local timestamps too far in future replaced by older incoming
        (
            false,
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionInactive, skewed, true, skewed))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid, false, valid))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid, false, valid))]), owners: HashMap::new() },
            Some(PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionActive, valid, false, valid))]), owners: HashMap::new() }),
        ),
        // local too far in future not replaced by itself
        (
            false,
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionInactive, skewed, true, skewed))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionInactive, skewed, true, skewed))]), owners: HashMap::new() },
            PartitionRingDesc { partitions: HashMap::from([(1, p_locked(1, PartitionState::PartitionInactive, skewed, true, skewed))]), owners: HashMap::new() },
            None,
        ),
        // local CAS removing partition too far in future marks deleted now
        (
            true,
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionActive, skewed))]), owners: HashMap::new() },
            PartitionRingDesc::default(),
            PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionDeleted, valid))]), owners: HashMap::new() },
            Some(PartitionRingDesc { partitions: HashMap::from([(1, p_desc(1, vec![1, 2, 3], PartitionState::PartitionDeleted, valid))]), owners: HashMap::new() }),
        ),
        // incoming owner too far in future is ignored
        (
            false,
            PartitionRingDesc { partitions: HashMap::new(), owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, valid))]) },
            PartitionRingDesc {
                partitions: HashMap::new(),
                owners: HashMap::from([
                    ("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerDeleted, skewed)),
                    ("ingester-zone-b-0".into(), o_desc(1, OwnerState::OwnerActive, skewed)),
                ]),
            },
            PartitionRingDesc { partitions: HashMap::new(), owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, valid))]) },
            None,
        ),
        // local owner too far in future replaced by older incoming
        (
            false,
            PartitionRingDesc { partitions: HashMap::new(), owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerActive, skewed))]) },
            PartitionRingDesc { partitions: HashMap::new(), owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerDeleted, valid))]) },
            PartitionRingDesc { partitions: HashMap::new(), owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerDeleted, valid))]) },
            Some(PartitionRingDesc { partitions: HashMap::new(), owners: HashMap::from([("ingester-zone-a-0".into(), o_desc(1, OwnerState::OwnerDeleted, valid))]) }),
        ),
    ];

    for (local_cas, mut local, incoming, exp_local, exp_change) in cases {
        let change = local.merge_with_time(&incoming, local_cas, now);
        assert_eq!(exp_local, local);
        assert_eq!(exp_change, change);
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_MergeContent
#[test]
fn test_partition_ring_desc_merge_content() {
    assert!(PartitionRingDesc::default().merge_content().is_empty());

    let mut desc = PartitionRingDesc::default();
    let now = SystemTime::now();
    desc.add_partition(1, PartitionState::PartitionActive, now);
    desc.add_partition(2, PartitionState::PartitionActive, now);
    desc.add_or_update_owner("ingester-zone-a-0", OwnerState::OwnerActive, 1, now);
    let mut content = desc.merge_content();
    content.sort();
    assert_eq!(vec!["1", "2", "ingester-zone-a-0"], content);
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_model_test.go::TestPartitionRingDesc_TokenSchemeSurvivesEncodeAndClone
#[test]
fn test_partition_ring_desc_token_scheme_survives_encode_and_clone() {
    for scheme in [
        PartitionTokenScheme::PartitionTokensStored as i32,
        PartitionTokenScheme::PartitionTokensSmt512 as i32,
        99,
    ] {
        let mut desc = PartitionRingDesc::default();
        desc.partitions.insert(
            1,
            PartitionDesc {
                id: 1,
                tokens: vec![10],
                token_scheme: scheme,
                state: PartitionState::PartitionActive as i32,
                state_timestamp: 1,
                ..Default::default()
            },
        );
        let codec = partition_ring_desc_codec();
        let encoded = codec.encode(&desc).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        let decoded_desc = decoded.downcast_ref::<PartitionRingDesc>().unwrap();
        assert_eq!(&desc, decoded_desc);
        let cloned = desc.clone_mergeable();
        assert_eq!(&desc, cloned.as_any().downcast_ref::<PartitionRingDesc>().unwrap());
    }
}


