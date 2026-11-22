//! Ported from github.com/grafana/dskit/ring:
//! - partition_ring_shuffle_shard_test.go (2)
//! - partition_ring_test.go (25)
//! - partition_ring_watcher_test.go (4)
//! - partition_ring_watchers_test.go (4)
//! - partition_tokens_test.go (5)
//! - partitions_ring_shuffle_shard_cache_test.go (1)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mimir_common::services::{
    start_and_await_running, stop_and_await_terminated, Service, ServiceContext, State,
};
use mimir_proto::ringpb::{
    OwnerDesc, OwnerState, PartitionDesc, PartitionRingDesc, PartitionState, PartitionTokenScheme,
};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::{AsRegisterer, CasOutcome, KvClient};
use mimir_ring::partition_ring::model::{
    partition_ring_desc_codec, PartitionDescExt, PartitionRingDescExt, PartitionRingError,
};
use mimir_ring::partition_ring::{
    same_except_pending, ActivePartitionBatchRing, CacheKey, CacheStorage, PartitionRing,
    PartitionRingOptions, PartitionRingWatcher, PartitionRingWatcherDelegate,
    PartitionRingWatchers,
};
use mimir_ring::prng::{shuffle_shard_seed, GoRand};
use mimir_ring::ring::model::system_time_to_unix_secs;
use mimir_ring::token_gen::{
    search_token, PartitionTokenGenerator, PartitionTokenTable, SpreadMinimizingTokenGenerator,
    TokenGenError, OPTIMAL_TOKENS_PER_INSTANCE,
};
use parking_lot::Mutex;
use prometheus::Registry;

const RING_KEY: &str = "ring";

fn gather_metric(reg: &Registry, name: &str, label_match: Option<(&str, &str)>) -> Option<f64> {
    for mf in reg.gather() {
        if mf.get_name() == name {
            for m in mf.get_metric() {
                if let Some((ln, lv)) = label_match {
                    if !m
                        .get_label()
                        .iter()
                        .any(|lp| lp.get_name() == ln && lp.get_value() == lv)
                    {
                        continue;
                    }
                }
                if m.has_gauge() {
                    return Some(m.get_gauge().get_value());
                }
                if m.has_counter() {
                    return Some(m.get_counter().get_value());
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

fn generate_partition_tokens(max_partition_id: i32) -> Result<Vec<Vec<u32>>, TokenGenError> {
    if max_partition_id < 0 {
        return Err(TokenGenError::NegativePartitionId(max_partition_id));
    }
    let gen = SpreadMinimizingTokenGenerator::for_instance_and_zone_id(
        "",
        max_partition_id as usize,
        0,
        false,
    );
    let mut tokens = gen.generate_tokens_by_instance_id()?;
    for t in &mut tokens {
        t.sort_unstable();
    }
    Ok(tokens)
}

struct MapPartitionTokenGen {
    tokens: HashMap<i32, Vec<u32>>,
    calls: Mutex<Vec<i32>>,
    fail: Option<String>,
}

impl MapPartitionTokenGen {
    fn from_map(tokens: HashMap<i32, Vec<u32>>) -> Self {
        Self {
            tokens,
            calls: Mutex::new(Vec::new()),
            fail: None,
        }
    }

    fn failing(msg: &str) -> Self {
        Self {
            tokens: HashMap::new(),
            calls: Mutex::new(Vec::new()),
            fail: Some(msg.to_string()),
        }
    }
}

impl PartitionTokenGenerator for MapPartitionTokenGen {
    fn tokens_for(&self, id: i32) -> Result<&[u32], TokenGenError> {
        self.calls.lock().push(id);
        if let Some(ref msg) = self.fail {
            return Err(TokenGenError::SpreadMinimizing(msg.clone()));
        }
        self.tokens
            .get(&id)
            .map(Vec::as_slice)
            .ok_or_else(|| TokenGenError::SpreadMinimizing(format!("missing partition {id}")))
    }
}

struct TableTokenGen {
    tokens: Vec<Vec<u32>>,
}

impl PartitionTokenGenerator for TableTokenGen {
    fn tokens_for(&self, id: i32) -> Result<&[u32], TokenGenError> {
        if id < 0 || (id as usize) >= self.tokens.len() {
            return Err(TokenGenError::PartitionIdOutOfRange {
                id,
                len: self.tokens.len(),
            });
        }
        Ok(&self.tokens[id as usize])
    }
}

// ============================================================================
// partition_ring_shuffle_shard_test.go (2)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/partition_ring_shuffle_shard_test.go::TestPartitionRing_ShuffleShard_SubringIndexes
#[test]
fn test_partition_ring_shuffle_shard_subring_indexes() {
    let now = UNIX_EPOCH + Duration::from_secs(1_792_597_591);
    for cache_size in [0usize, 2] {
        for derived in [false, true] {
            for scenario in ["active", "mixed", "tokenless", "no active", "empty"] {
                let mut desc = PartitionRingDesc::default();
                let mut table_tokens = vec![Vec::new(); 12];
                let mut active = HashSet::new();

                if scenario != "empty" {
                    for id in 0..12i32 {
                        let state = match scenario {
                            "mixed" => [
                                PartitionState::PartitionActive,
                                PartitionState::PartitionInactive,
                                PartitionState::PartitionPending,
                            ][(id as usize) % 3],
                            "no active" => PartitionState::PartitionInactive,
                            _ => PartitionState::PartitionActive,
                        };
                        let mut tokens = vec![(id as u32) << 28, ((id as u32) << 28) + 1];
                        if scenario == "tokenless" && id % 4 == 0 {
                            tokens.clear();
                        }
                        table_tokens[id as usize] = tokens.clone();
                        let ts = if state == PartitionState::PartitionInactive {
                            now - Duration::from_secs(1800)
                        } else {
                            now - Duration::from_secs(7200)
                        };
                        let mut partition = PartitionDesc {
                            id,
                            state: state as i32,
                            state_timestamp: system_time_to_unix_secs(ts),
                            tokens: tokens.clone(),
                            ..Default::default()
                        };
                        if derived {
                            partition.token_scheme =
                                PartitionTokenScheme::PartitionTokensSmt512 as i32;
                            partition.tokens.clear();
                        }
                        desc.partitions.insert(id, partition);
                        desc.owners.insert(
                            format!("owner-{id}"),
                            OwnerDesc {
                                owned_partition: id,
                                ..Default::default()
                            },
                        );
                        if state == PartitionState::PartitionActive && !tokens.is_empty() {
                            active.insert(id);
                        }
                    }
                }
                desc.owners.insert(
                    "orphan".into(),
                    OwnerDesc {
                        owned_partition: 99,
                        ..Default::default()
                    },
                );
                let before = desc.clone();
                let mut opts = PartitionRingOptions {
                    token_generator: None,
                    shuffle_shard_cache_size: cache_size,
                };
                if derived {
                    opts.token_generator = Some(Arc::new(TableTokenGen {
                        tokens: table_tokens,
                    }));
                }
                let parent = PartitionRing::with_options(desc.clone(), opts.clone()).unwrap();

                for size in [1usize, 4, 12, 0] {
                    for lookback in [Duration::ZERO, Duration::from_secs(3600)] {
                        let shard = parent
                            .compute_shuffle_shard("tenant", size, lookback, now)
                            .unwrap();
                        if lookback.is_zero() && (size == 0 || size >= active.len()) {
                            let ids: HashSet<i32> = shard.partition_ids().into_iter().collect();
                            assert_eq!(active, ids);
                        }
                        let expected =
                            PartitionRing::with_options(shard.desc().clone(), opts.clone()).unwrap();
                        assert!(expected.subring_indexes_eq(&shard));
                        assert!(!parent.cache_ptr_eq(&shard));
                        assert!(!shard.desc().owners.contains_key("orphan"));
                        assert_eq!(
                            opts.shuffle_shard_cache_size,
                            shard.options().shuffle_shard_cache_size
                        );

                        let want_nested = expected.shuffle_shard("nested", 2).unwrap();
                        let got_nested = shard.shuffle_shard("nested", 2).unwrap();
                        assert_eq!(want_nested.desc(), got_nested.desc());
                        assert!(want_nested.subring_indexes_eq(&got_nested));
                    }
                }
                assert_eq!(before, desc);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_shuffle_shard_test.go::TestPartitionRing_ShuffleShard_SharesAllActiveSubring
#[test]
fn test_partition_ring_shuffle_shard_shares_all_active_subring() {
    let tokens = generate_partition_tokens(15).unwrap();
    let mut desc = PartitionRingDesc::default();
    let mut active = Vec::new();
    for id in 0..16i32 {
        let state = if id >= 14 {
            PartitionState::PartitionInactive
        } else if id >= 12 {
            PartitionState::PartitionPending
        } else {
            active.push(id);
            PartitionState::PartitionActive
        };
        desc.partitions.insert(
            id,
            PartitionDesc {
                id,
                state: state as i32,
                tokens: tokens[id as usize].clone(),
                ..Default::default()
            },
        );
        desc.owners.insert(
            format!("owner-{id}"),
            OwnerDesc {
                owned_partition: id,
                ..Default::default()
            },
        );
    }
    let r = PartitionRing::new(desc).unwrap();

    let full = r.shuffle_shard("tenant-a", 0).unwrap();
    assert_eq!(active, full.partition_ids());
    for (tenant, size) in [
        ("tenant-b", 0usize),
        ("tenant-c", active.len()),
        ("tenant-d", 100),
    ] {
        let shard = r.shuffle_shard(tenant, size).unwrap();
        assert!(Arc::ptr_eq(&full, &shard));
    }

    let sub = r.shuffle_shard("tenant-e", 6).unwrap();
    assert_eq!(6, sub.partition_ids().len());
    let nested = sub.shuffle_shard("nested", 0).unwrap();
    assert_eq!(sub.partition_ids(), nested.partition_ids());
}

// ============================================================================
// partition_ring_watcher_test.go (4)
// ============================================================================

struct RingWatcherDelegateStub {
    new_ring: Mutex<PartitionRingDesc>,
}

impl RingWatcherDelegateStub {
    fn new() -> Self {
        Self {
            new_ring: Mutex::new(PartitionRingDesc::default()),
        }
    }

    fn partition_state(&self, partition: i32) -> PartitionState {
        self.new_ring
            .lock()
            .partitions
            .get(&partition)
            .map(|p| p.partition_state())
            .unwrap_or(PartitionState::PartitionUnknown)
    }
}

impl PartitionRingWatcherDelegate for RingWatcherDelegateStub {
    fn on_partition_ring_changed(
        &self,
        _old_ring: &PartitionRingDesc,
        new_ring: &PartitionRingDesc,
    ) {
        *self.new_ring.lock() = new_ring.clone();
    }
}

fn assert_partition_ring_watcher_metrics(
    reg: &Registry,
    pending: f64,
    active: f64,
    inactive: f64,
    max_id: f64,
    max_derived_id: f64,
) {
    assert_eq!(
        Some(pending),
        gather_metric(reg, "partition_ring_partitions", Some(("state", "Pending")))
    );
    assert_eq!(
        Some(active),
        gather_metric(reg, "partition_ring_partitions", Some(("state", "Active")))
    );
    assert_eq!(
        Some(inactive),
        gather_metric(reg, "partition_ring_partitions", Some(("state", "Inactive")))
    );
    assert_eq!(
        Some(max_id),
        gather_metric(reg, "partition_ring_max_partition_id", None)
    );
    assert_eq!(
        Some(max_derived_id),
        gather_metric(reg, "partition_ring_max_derived_partition_id", None)
    );
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watcher_test.go::TestPartitionRingWatcher_ShouldWatchUpdates
#[tokio::test]
async fn test_partition_ring_watcher_should_watch_updates() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let reg = Registry::new();
    let delegate = Arc::new(RingWatcherDelegateStub::new());
    let tokens = Arc::new(PartitionTokenTable::new(4, None).unwrap());
    let opts = PartitionRingOptions {
        token_generator: Some(tokens),
        shuffle_shard_cache_size: 1,
    };
    let watcher = PartitionRingWatcher::new(
        "test",
        RING_KEY,
        Arc::clone(&store),
        opts.clone(),
        Some(&reg as &dyn AsRegisterer),
    );
    watcher.with_delegate(delegate.clone());

    assert_eq!(0, watcher.partition_ring().partitions_count());
    let svc = Arc::clone(&watcher).into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    assert_eq!(0, watcher.partition_ring().partitions_count());
    assert_partition_ring_watcher_metrics(&reg, 0.0, 0.0, 0.0, -1.0, -1.0);

    // Add ACTIVE partition 1
    store
        .cas(
            RING_KEY,
            Box::new(|in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_partition(1, PartitionState::PartitionActive, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&watcher);
        let d = Arc::clone(&delegate);
        async move {
            w.partition_ring().partitions_count() == 1
                && d.partition_state(1) == PartitionState::PartitionActive
        }
    })
    .await);
    assert_eq!(1, watcher.partition_ring().options().shuffle_shard_cache_size);
    assert_partition_ring_watcher_metrics(&reg, 0.0, 1.0, 0.0, 1.0, -1.0);

    // Add INACTIVE partition 2 with derived tokens
    store
        .cas(
            RING_KEY,
            Box::new(|in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_partition_with_derived_tokens(
                    2,
                    PartitionState::PartitionInactive,
                    SystemTime::now(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&watcher);
        let d = Arc::clone(&delegate);
        async move {
            w.partition_ring().partitions_count() == 2
                && d.partition_state(2) == PartitionState::PartitionInactive
        }
    })
    .await);
    assert_partition_ring_watcher_metrics(&reg, 0.0, 1.0, 1.0, 2.0, 2.0);
    assert!(
        watcher.partition_ring().desc().partitions[&2]
            .tokens
            .is_empty()
    );
    assert_eq!(
        OPTIMAL_TOKENS_PER_INSTANCE,
        watcher.partition_ring().partition_tokens(2).unwrap().len()
    );

    // Add PENDING partition 3
    store
        .cas(
            RING_KEY,
            Box::new(|in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                desc.add_partition(3, PartitionState::PartitionPending, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&watcher);
        let d = Arc::clone(&delegate);
        async move {
            w.partition_ring().partitions_count() == 3
                && d.partition_state(3) == PartitionState::PartitionPending
        }
    })
    .await);
    assert_partition_ring_watcher_metrics(&reg, 1.0, 1.0, 1.0, 3.0, 2.0);

    // Switch partition 1 to Inactive
    store
        .cas(
            RING_KEY,
            Box::new(|in_val| {
                let mut desc = in_val
                    .and_then(|a| a.downcast_ref::<PartitionRingDesc>())
                    .cloned()
                    .unwrap_or_default();
                let _ = desc.update_partition_state(
                    1,
                    PartitionState::PartitionInactive,
                    SystemTime::now(),
                );
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();

    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&watcher);
        let d = Arc::clone(&delegate);
        async move {
            w.partition_ring().desc().partitions[&1].partition_state()
                == PartitionState::PartitionInactive
                && d.partition_state(1) == PartitionState::PartitionInactive
        }
    })
    .await);

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watcher_test.go::TestPartitionRingWatcher_FailsToStartWhenTokensCannotBeResolved
#[tokio::test]
async fn test_partition_ring_watcher_fails_to_start_when_tokens_cannot_be_resolved() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    store
        .cas(
            RING_KEY,
            Box::new(|_| {
                let mut d = PartitionRingDesc::default();
                d.add_partition_with_derived_tokens(
                    42,
                    PartitionState::PartitionActive,
                    SystemTime::now(),
                );
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();

    let reg = Registry::new();
    let watcher = PartitionRingWatcher::new(
        "test",
        RING_KEY,
        store,
        PartitionRingOptions::default(),
        Some(&reg as &dyn AsRegisterer),
    );
    let svc = Arc::clone(&watcher).into_service();
    let err = start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
    assert_eq!(State::Failed, svc.state());
    assert!(watcher.partition_ring().partition_ids().is_empty());
    assert_eq!(
        Some(-1.0),
        gather_metric(&reg, "partition_ring_max_partition_id", None)
    );
    assert_eq!(
        Some(-1.0),
        gather_metric(&reg, "partition_ring_max_derived_partition_id", None)
    );
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watcher_test.go::TestPartitionRingWatcher_StopsWhenUpdatedTokensCannotBeResolved
#[tokio::test]
async fn test_partition_ring_watcher_stops_when_updated_tokens_cannot_be_resolved() {
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    store
        .cas(
            RING_KEY,
            Box::new(|_| {
                let mut d = PartitionRingDesc::default();
                d.add_partition(3, PartitionState::PartitionActive, SystemTime::now());
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();

    let reg = Registry::new();
    let watcher = PartitionRingWatcher::new(
        "test",
        RING_KEY,
        Arc::clone(&store),
        PartitionRingOptions::default(),
        Some(&reg as &dyn AsRegisterer),
    );
    let svc = Arc::clone(&watcher).into_service();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();

    store
        .cas(
            RING_KEY,
            Box::new(|_| {
                let mut d = PartitionRingDesc::default();
                d.add_partition_with_derived_tokens(
                    42,
                    PartitionState::PartitionActive,
                    SystemTime::now(),
                );
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();

    let err = svc.await_terminated().await.unwrap_err();
    assert!(!err.to_string().is_empty());
    assert_eq!(State::Failed, svc.state());
    assert_eq!(vec![3], watcher.partition_ring().partition_ids());
    assert_eq!(
        Some(3.0),
        gather_metric(&reg, "partition_ring_max_partition_id", None)
    );
    assert_eq!(
        Some(-1.0),
        gather_metric(&reg, "partition_ring_max_derived_partition_id", None)
    );
}

fn partition_creation_descs(active: usize, k: usize) -> Vec<PartitionRingDesc> {
    let tokens = generate_partition_tokens((active + k - 1) as i32).unwrap();
    let mut start = PartitionRingDesc::default();
    for id in 0..active as i32 {
        start.partitions.insert(
            id,
            PartitionDesc {
                id,
                state: PartitionState::PartitionActive as i32,
                state_timestamp: 10,
                tokens: tokens[id as usize].clone(),
                ..Default::default()
            },
        );
        for z in ["a", "b"] {
            start.owners.insert(
                format!("ingester-zone-{z}-{id}"),
                OwnerDesc {
                    owned_partition: id,
                    state: OwnerState::OwnerActive as i32,
                    ..Default::default()
                },
            );
        }
    }
    let mut descs = vec![start];
    let created: Vec<i32> = (0..k).map(|j| (active + j) as i32).collect();
    for &id in &created {
        let mut next = descs.last().unwrap().clone();
        next.partitions.insert(
            id,
            PartitionDesc {
                id,
                state: PartitionState::PartitionPending as i32,
                state_timestamp: 20,
                tokens: tokens[id as usize].clone(),
                ..Default::default()
            },
        );
        descs.push(next);
    }
    for z in ["a", "b"] {
        for &id in &created {
            let mut next = descs.last().unwrap().clone();
            next.owners.insert(
                format!("ingester-zone-{z}-{id}"),
                OwnerDesc {
                    owned_partition: id,
                    state: OwnerState::OwnerActive as i32,
                    ..Default::default()
                },
            );
            descs.push(next);
        }
    }
    for &id in &created {
        let mut next = descs.last().unwrap().clone();
        if let Some(p) = next.partitions.get_mut(&id) {
            p.state = PartitionState::PartitionActive as i32;
            p.state_timestamp = 30;
        }
        descs.push(next);
    }
    descs
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watcher_test.go::TestPartitionRingWatcher_ShardReuseDuringPartitionCreation
#[test]
fn test_partition_ring_watcher_shard_reuse_during_partition_creation() {
    let sizes = [0usize, 1, 5, 12];
    let descs = partition_creation_descs(40, 4);
    let store: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let watcher = PartitionRingWatcher::new(
        "test",
        "test",
        store,
        PartitionRingOptions::default(),
        None,
    );
    let lookback = Duration::from_secs(3600);
    let now = UNIX_EPOCH + Duration::from_secs(3625);

    let mut prev: Vec<Arc<PartitionRing>> = Vec::new();
    let mut prev_lookback: Vec<Arc<PartitionRing>> = Vec::new();

    for (i, desc) in descs.iter().enumerate() {
        watcher.update_partition_ring(desc.clone()).unwrap();
        let fresh = PartitionRing::new(desc.clone()).unwrap();
        let pending_only = i > 0 && same_except_pending(&descs[i - 1], desc);

        let mut shards = Vec::with_capacity(sizes.len());
        let mut lookback_shards = Vec::with_capacity(sizes.len());

        for (j, &size) in sizes.iter().enumerate() {
            let tenant = format!("tenant-{j}");
            let got = watcher.partition_ring().shuffle_shard(&tenant, size).unwrap();
            let want = fresh.shuffle_shard(&tenant, size).unwrap();
            assert_eq!(want.partition_ids(), got.partition_ids());
            if pending_only {
                assert!(Arc::ptr_eq(&prev[j], &got));
            }
            shards.push(got);

            let got_lb = watcher
                .partition_ring()
                .shuffle_shard_with_lookback(&tenant, size, lookback, now)
                .unwrap();
            let want_lb = fresh
                .shuffle_shard_with_lookback(&tenant, size, lookback, now)
                .unwrap();
            assert_eq!(want_lb.partition_ids(), got_lb.partition_ids());
            if pending_only {
                if size > 0 {
                    assert_eq!(size, got_lb.partition_ids().len());
                }
                assert!(Arc::ptr_eq(&prev_lookback[j], &got_lb));
            }
            lookback_shards.push(got_lb);
        }
        assert_eq!(
            sizes.len(),
            watcher.partition_ring().cache_without_lookback_len()
        );
        assert_eq!(
            sizes.len(),
            watcher.partition_ring().cache_with_lookback_len()
        );
        prev = shards;
        prev_lookback = lookback_shards;
    }
}

// ============================================================================
// partition_ring_watchers_test.go (4)
// ============================================================================

async fn write_partitions(kv_client: &dyn KvClient, key: &str, partition_ids: &[i32]) {
    let ids = partition_ids.to_vec();
    kv_client
        .cas(
            key,
            Box::new(move |_| {
                let mut desc = PartitionRingDesc::default();
                for &id in &ids {
                    desc.add_partition(id, PartitionState::PartitionActive, SystemTime::now());
                }
                Ok(CasOutcome::Update(Box::new(desc)))
            }),
        )
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watchers_test.go::TestPartitionRingWatchers_SingleWatcher
#[tokio::test]
async fn test_partition_ring_watchers_single_watcher() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    write_partitions(kv.as_ref(), "ring", &[0, 1, 2]).await;

    let w = PartitionRingWatchers::new(vec![PartitionRingWatcher::new(
        "ring",
        "ring",
        kv,
        PartitionRingOptions::default(),
        None,
    )])
    .unwrap();
    let svc = Arc::clone(&w).into_service().unwrap();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&w);
        async move { w.partition_ring(0).is_some_and(|r| r.partitions_count() > 0) }
    })
    .await);

    assert_eq!(1, w.count());
    assert_eq!(3, w.partition_ring(0).unwrap().partitions_count());
    assert!(Arc::ptr_eq(w.watcher(0).unwrap(), &w.all()[0]));
    assert!(Arc::ptr_eq(
        &w.watcher(0).unwrap().partition_ring(),
        &w.partition_ring(0).unwrap()
    ));

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watchers_test.go::TestPartitionRingWatchers_MultipleWatchers
#[tokio::test]
async fn test_partition_ring_watchers_multiple_watchers() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    write_partitions(kv.as_ref(), "ring-0", &[0]).await;
    write_partitions(kv.as_ref(), "ring-1", &[0, 1]).await;
    write_partitions(kv.as_ref(), "ring-2", &[0, 1, 2]).await;

    let w = PartitionRingWatchers::new(vec![
        PartitionRingWatcher::new("ring-0", "ring-0", Arc::clone(&kv), PartitionRingOptions::default(), None),
        PartitionRingWatcher::new("ring-1", "ring-1", Arc::clone(&kv), PartitionRingOptions::default(), None),
        PartitionRingWatcher::new("ring-2", "ring-2", Arc::clone(&kv), PartitionRingOptions::default(), None),
    ])
    .unwrap();
    let svc = Arc::clone(&w).into_service().unwrap();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    assert!(poll(Duration::from_secs(1), Duration::from_millis(10), || {
        let w = Arc::clone(&w);
        async move { w.partition_ring(0).is_some_and(|r| r.partitions_count() > 0) }
    })
    .await);

    assert_eq!(3, w.count());
    assert_eq!(3, w.all().len());
    assert_eq!(1, w.partition_ring(0).unwrap().partitions_count());
    assert_eq!(2, w.partition_ring(1).unwrap().partitions_count());
    assert_eq!(3, w.partition_ring(2).unwrap().partitions_count());
    assert!(Arc::ptr_eq(
        &w.watcher(1).unwrap().partition_ring(),
        &w.partition_ring(1).unwrap()
    ));

    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watchers_test.go::TestPartitionRingWatchers_DoesNotPanicWithRealRegistererAndDistinctlyNamedWatchers
#[tokio::test]
async fn test_partition_ring_watchers_does_not_panic_with_real_registerer_and_distinctly_named_watchers() {
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(partition_ring_desc_codec()));
    let reg = Registry::new();
    let r: &dyn AsRegisterer = &reg;
    let w = PartitionRingWatchers::new(vec![
        PartitionRingWatcher::new("ring-0", "ring-0", Arc::clone(&kv), PartitionRingOptions::default(), Some(r)),
        PartitionRingWatcher::new("ring-1", "ring-1", Arc::clone(&kv), PartitionRingOptions::default(), Some(r)),
        PartitionRingWatcher::new("ring-2", "ring-2", Arc::clone(&kv), PartitionRingOptions::default(), Some(r)),
        PartitionRingWatcher::new("ring-3", "ring-3", Arc::clone(&kv), PartitionRingOptions::default(), Some(r)),
    ])
    .unwrap();
    assert_eq!(4, w.count());
    let svc = w.into_service().unwrap();
    start_and_await_running(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
    stop_and_await_terminated(ServiceContext::background(), svc.as_ref())
        .await
        .unwrap();
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_watchers_test.go::TestPartitionRingWatchers_ReturnsErrorWhenNoWatchersProvided
#[test]
fn test_partition_ring_watchers_returns_error_when_no_watchers_provided() {
    assert!(PartitionRingWatchers::new(Vec::new()).is_err());
}

// ============================================================================
// partition_tokens_test.go (5)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/partition_tokens_test.go::TestResolveRingTokens
#[test]
fn test_resolve_ring_tokens() {
    let desc = PartitionRingDesc {
        partitions: HashMap::from([
            (1, PartitionDesc { tokens: vec![1, 5, 8], ..Default::default() }),
            (2, PartitionDesc { tokens: vec![3, 4, 9], ..Default::default() }),
        ]),
        owners: HashMap::new(),
    };
    let ring = PartitionRing::new(desc).unwrap();
    assert_eq!(&[1, 3, 4, 5, 8, 9], ring.ring_tokens());
    assert_eq!(
        &HashMap::from([(1, 1), (5, 1), (8, 1), (3, 2), (4, 2), (9, 2)]),
        ring.partition_by_token()
    );
}

// Upstream: github.com/grafana/dskit/ring/partition_tokens_test.go::TestResolvePartitionTokens_PartitionDoesNotExist
#[test]
fn test_resolve_partition_tokens_partition_does_not_exist() {
    let desc = PartitionRingDesc {
        partitions: HashMap::from([(1, PartitionDesc { tokens: vec![1], ..Default::default() })]),
        owners: HashMap::new(),
    };
    let err = mimir_ring::partition_ring::resolve_partition_tokens(
        &desc,
        2,
        &PartitionRingOptions::default(),
    )
    .unwrap_err();
    assert!(matches!(err, PartitionRingError::PartitionDoesNotExist));
}

// Upstream: github.com/grafana/dskit/ring/partition_tokens_test.go::TestPartitionTokenTable_GeneratesConfiguredIDsOnce
#[test]
fn test_partition_token_table_generates_configured_ids_once() {
    // negative
    {
        let reg = Registry::new();
        assert!(PartitionTokenTable::new(-1, Some(&reg as &dyn AsRegisterer)).is_err());
        assert_eq!(
            None,
            gather_metric(&reg, "partition_ring_max_derived_token_partitions", None)
        );
    }
    // zero
    {
        let reg = Registry::new();
        let table = PartitionTokenTable::new(0, Some(&reg as &dyn AsRegisterer)).unwrap();
        assert!(table.tokens_for(0).is_err());
        assert_eq!(
            Some(0.0),
            gather_metric(&reg, "partition_ring_max_derived_token_partitions", None)
        );
    }
    // positive
    {
        let reg = Registry::new();
        let table = PartitionTokenTable::new(8, Some(&reg as &dyn AsRegisterer)).unwrap();
        assert!(table.tokens_for(-1).is_err());
        assert!(table.tokens_for(8).is_err());
        let toks = table.tokens_for(7).unwrap();
        assert_eq!(OPTIMAL_TOKENS_PER_INSTANCE, toks.len());
        assert_eq!(
            Some(8.0),
            gather_metric(&reg, "partition_ring_max_derived_token_partitions", None)
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_tokens_test.go::TestPartitionTokenTable_FailsWhenGenerationFails
#[test]
fn test_partition_token_table_fails_when_generation_fails() {
    let gen = Arc::new(MapPartitionTokenGen::failing("generation failed"));
    let calls = AtomicUsize::new(0);
    let err = gen.tokens_for(0).unwrap_err();
    calls.fetch_add(1, Ordering::SeqCst);
    assert!(err.to_string().contains("generation failed"));
    assert_eq!(1, calls.load(Ordering::SeqCst));
    assert!(PartitionTokenTable::new(-5, None).is_err());
}

// Upstream: github.com/grafana/dskit/ring/partition_tokens_test.go::TestGeneratePartitionTokens_MatchesAddPartition
#[test]
fn test_generate_partition_tokens_matches_add_partition() {
    let table = PartitionTokenTable::new(33, None).unwrap();
    for max_id in [0i32, 1, 15, 32] {
        let tokens = generate_partition_tokens(max_id).unwrap();
        assert_eq!((max_id + 1) as usize, tokens.len());
        let mut desc = PartitionRingDesc::default();
        for id in 0..=max_id {
            desc.add_partition(id, PartitionState::PartitionActive, UNIX_EPOCH + Duration::from_secs(1));
            assert_eq!(tokens[id as usize], desc.partitions[&id].tokens);
            let from_table = table.tokens_for(id).unwrap();
            assert_eq!(tokens[id as usize].as_slice(), from_table);
        }
    }
    assert!(generate_partition_tokens(-1).is_err());
}

// ============================================================================
// partitions_ring_shuffle_shard_cache_test.go (1)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/partitions_ring_shuffle_shard_cache_test.go::TestShuffleShardCacheStorage
#[test]
fn test_shuffle_shard_cache_storage() {
    let storages = [
        CacheStorage::<String>::new_map(),
        CacheStorage::<String>::new_lru(10),
    ];

    for mut storage in storages {
        let key = CacheKey {
            identifier: "test".to_string(),
            shard_size: 3,
            lookback_period: Duration::from_secs(3600),
        };

        assert_eq!(0, storage.len());
        assert!(storage.get(&key).is_none());

        storage.insert(key.clone(), "value1".to_string());
        assert_eq!(1, storage.len());
        assert_eq!(Some("value1".to_string()), storage.get(&key));

        storage.insert(key.clone(), "value2".to_string());
        assert_eq!(1, storage.len());
        assert_eq!(Some("value2".to_string()), storage.get(&key));
    }
}

// ============================================================================
// partition_ring_test.go (25)
// ============================================================================

fn user_token(user: &str, zone: &str, skip: usize) -> u32 {
    let mut r = GoRand::new(shuffle_shard_seed(user, zone));
    for _ in 0..skip {
        let _ = r.uint32();
    }
    r.uint32()
}

fn create_partition_ring_with_partitions(
    opts: PartitionRingOptions,
    num_active: usize,
    num_inactive: usize,
    num_pending: usize,
) -> Arc<PartitionRing> {
    let mut desc = PartitionRingDesc::default();
    let now = SystemTime::now();
    for i in 0..num_active {
        desc.add_partition(i as i32, PartitionState::PartitionActive, now);
    }
    for i in num_active..(num_active + num_inactive) {
        desc.add_partition(i as i32, PartitionState::PartitionInactive, now);
    }
    for i in (num_active + num_inactive)..(num_active + num_inactive + num_pending) {
        desc.add_partition(i as i32, PartitionState::PartitionPending, now);
    }
    PartitionRing::with_options(desc, opts).unwrap()
}

fn compare_partition_ids(first: &[i32], second: &[i32]) -> (Vec<i32>, Vec<i32>) {
    let removed: Vec<i32> = first
        .iter()
        .copied()
        .filter(|id| !second.contains(id))
        .collect();
    let added: Vec<i32> = second
        .iter()
        .copied()
        .filter(|id| !first.contains(id))
        .collect();
    (added, removed)
}

fn random_string(rnd: &mut GoRand, len: usize) -> String {
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        let ch = b'0' + (rnd.uint32() % ((b'Z' - b'0') as u32)) as u8;
        out.push(ch as char);
    }
    out
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ActivePartitionForKey
#[test]
fn test_partition_ring_active_partition_for_key() {
    let tokens_for_partition = |pid: i32| -> Vec<u32> {
        let tg = SpreadMinimizingTokenGenerator::for_instance_and_zone_id(
            "",
            pid as usize,
            0,
            false,
        );
        mimir_ring::token_gen::TokenGenerator::generate_tokens(&tg, OPTIMAL_TOKENS_PER_INSTANCE, &[])
    };

    let cases: Vec<(Vec<PartitionState>, u32, Result<i32, PartitionRingError>)> = vec![
        (vec![], 1, Err(PartitionRingError::NoActivePartitionFound)),
        (vec![PartitionState::PartitionActive], 1, Ok(0)),
        (vec![PartitionState::PartitionInactive], 1, Err(PartitionRingError::NoActivePartitionFound)),
        (vec![PartitionState::PartitionDeleted], 1, Err(PartitionRingError::NoActivePartitionFound)),
        (
            vec![PartitionState::PartitionActive, PartitionState::PartitionActive, PartitionState::PartitionActive],
            tokens_for_partition(2)[123] - 10,
            Ok(2),
        ),
        (
            vec![PartitionState::PartitionActive, PartitionState::PartitionActive, PartitionState::PartitionInactive],
            tokens_for_partition(2)[123] - 10,
            Ok(1),
        ),
        (
            vec![PartitionState::PartitionActive, PartitionState::PartitionInactive, PartitionState::PartitionInactive],
            tokens_for_partition(2)[123] - 10,
            Ok(0),
        ),
        (
            vec![PartitionState::PartitionActive, PartitionState::PartitionActive, PartitionState::PartitionPending],
            tokens_for_partition(2)[123] - 10,
            Ok(1),
        ),
        (
            vec![PartitionState::PartitionActive, PartitionState::PartitionPending, PartitionState::PartitionPending],
            tokens_for_partition(2)[123] - 10,
            Ok(0),
        ),
        (
            vec![PartitionState::PartitionActive, PartitionState::PartitionActive, PartitionState::PartitionActive],
            tokens_for_partition(1)[123],
            Ok(0),
        ),
    ];

    for (partitions, key, expected) in cases {
        let mut desc = PartitionRingDesc::default();
        for (p_idx, state) in partitions.into_iter().enumerate() {
            desc.add_partition(p_idx as i32, state, SystemTime::now());
        }
        let pr = PartitionRing::new(desc).unwrap();
        let actual = pr.active_partition_for_key(key);
        match expected {
            Ok(exp_id) => assert_eq!(exp_id, actual.unwrap()),
            Err(_) => assert!(matches!(actual, Err(PartitionRingError::NoActivePartitionFound))),
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ActivePartitionForKey_NoMemoryAllocations
#[test]
fn test_partition_ring_active_partition_for_key_no_memory_allocations() {
    let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 100, 10, 0);
    let mut r = GoRand::new(12345);
    for _ in 0..1000 {
        let pid = ring.active_partition_for_key(r.uint32()).unwrap();
        assert!((0..100).contains(&pid));
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShard
#[test]
fn test_partition_ring_shuffle_shard() {
    // honor shard size
    {
        const NUM_ACTIVE: usize = 5;
        let opts = PartitionRingOptions {
            token_generator: None,
            shuffle_shard_cache_size: 1,
        };
        let ring = create_partition_ring_with_partitions(opts.clone(), NUM_ACTIVE, 0, 0);
        for shard_size in 1..=NUM_ACTIVE {
            let subring = ring.shuffle_shard("tenant-id", shard_size).unwrap();
            assert_eq!(shard_size, subring.partitions_count());
            assert_eq!(subring.partitions_count(), ring.shuffle_shard_size(shard_size));
        }
        let subring = ring.shuffle_shard("tenant-id", NUM_ACTIVE + 1).unwrap();
        assert_eq!(NUM_ACTIVE, subring.partitions_count());
        assert_eq!(subring.partitions_count(), ring.shuffle_shard_size(NUM_ACTIVE + 1));
        assert_eq!(
            opts.shuffle_shard_cache_size,
            subring.options().shuffle_shard_cache_size
        );
    }

    // never return INACTIVE or PENDING partitions
    {
        let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 5, 5, 5);
        for shard_size in 0..=(ring.partitions_count() + 5) {
            let subring = ring.shuffle_shard("tenant-id", shard_size).unwrap();
            for p in subring.partitions() {
                assert_eq!(PartitionState::PartitionActive, p.partition_state());
            }
            assert_eq!(subring.partitions_count(), ring.shuffle_shard_size(shard_size));
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShard_Stability
#[test]
fn test_partition_ring_shuffle_shard_stability() {
    let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 50, 10, 5);
    for i in 1..=100 {
        let tenant_id = i.to_string();
        for size in [3usize, 6, 9, 12, 15] {
            let expected = ring.shuffle_shard(&tenant_id, size).unwrap();
            assert_eq!(expected.partitions_count(), ring.shuffle_shard_size(size));
            for _ in 0..10 {
                let actual = ring.shuffle_shard(&tenant_id, size).unwrap();
                assert_eq!(expected.desc(), actual.desc());
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShard_Shuffling
#[test]
fn test_partition_ring_shuffle_shard_shuffling() {
    const NUM_TENANTS: usize = 1000;
    const NUM_ACTIVE: usize = 90;
    const SHARD_SIZE: usize = 3;
    let theoretical = [
        (0usize, 90.2239_f64),
        (1, 9.55312),
        (2, 0.22217),
        (3, 0.00085),
    ];

    let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), NUM_ACTIVE, 0, 0);
    let mut partitions_by_tenant: Vec<Vec<i32>> = Vec::with_capacity(NUM_TENANTS);
    for i in 1..=NUM_TENANTS {
        let sub = ring.shuffle_shard(&i.to_string(), SHARD_SIZE).unwrap();
        partitions_by_tenant.push(sub.partition_ids());
    }

    let mut distribution = [0usize; 4];
    for i in 0..NUM_TENANTS {
        for j in 0..NUM_TENANTS {
            if i == j {
                continue;
            }
            let matching = partitions_by_tenant[i]
                .iter()
                .filter(|p| partitions_by_tenant[j].contains(p))
                .count();
            distribution[matching] += 1;
        }
    }

    let max_combinations = (NUM_TENANTS * NUM_TENANTS - NUM_TENANTS) as f64;
    for (num_matching, prob) in theoretical {
        let max_dev = 1.0_f64.min((0.2_f64).max(prob * 0.1));
        let actual = (distribution[num_matching] as f64 / max_combinations) * 100.0;
        assert!(
            (actual - prob).abs() <= max_dev,
            "num_matching={num_matching}: actual={actual}, prob={prob}, max_dev={max_dev}"
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_Sorting
#[test]
fn test_partition_ring_sorting() {
    let mut desc = PartitionRingDesc::default();
    let states = [
        PartitionState::PartitionPending,
        PartitionState::PartitionActive,
        PartitionState::PartitionInactive,
    ];
    for i in (0..10).rev() {
        desc.add_partition(i as i32, states[i % 3], SystemTime::now());
    }
    let pr = PartitionRing::new(desc).unwrap();
    assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], pr.partition_ids());
    assert_eq!(vec![0, 3, 6, 9], pr.pending_partition_ids());
    assert_eq!(vec![1, 4, 7], pr.active_partition_ids());
    assert_eq!(vec![2, 5, 8], pr.inactive_partition_ids());
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShard_ConsistencyOnPartitionsTopologyChange
#[test]
fn test_partition_ring_shuffle_shard_consistency_on_partitions_topology_change() {
    #[derive(Clone, Copy)]
    enum Change {
        AddActive,
        SwitchPendingToActive,
        SwitchActiveToInactive,
        RemoveActive,
        RemoveInactive,
    }

    let precomputed = generate_partition_tokens(65).unwrap();
    let now = SystemTime::now();
    const NUM_TENANTS: usize = 100;
    for num_partitions in [20usize, 30, 40, 50] {
        let mut base_desc = PartitionRingDesc::default();
        for (i, toks) in precomputed.iter().enumerate().take(num_partitions + 10) {
            let state = if i < num_partitions {
                PartitionState::PartitionActive
            } else if i < num_partitions + 5 {
                PartitionState::PartitionInactive
            } else {
                PartitionState::PartitionPending
            };
            base_desc.partitions.insert(
                i as i32,
                PartitionDesc {
                    id: i as i32,
                    state: state as i32,
                    state_timestamp: system_time_to_unix_secs(now),
                    tokens: toks.clone(),
                    ..Default::default()
                },
            );
        }
        let base_ring = PartitionRing::new(base_desc).unwrap();

        for shard_size in [3usize, 6, 9, 12, 15] {
            let mut initial = Vec::with_capacity(NUM_TENANTS);
            for id in 0..NUM_TENANTS {
                initial.push(
                    base_ring
                        .shuffle_shard(&id.to_string(), shard_size)
                        .unwrap()
                        .partition_ids(),
                );
            }
            for c in [
                Change::AddActive,
                Change::SwitchPendingToActive,
                Change::SwitchActiveToInactive,
                Change::RemoveActive,
                Change::RemoveInactive,
            ] {
                let mut ring = Arc::clone(&base_ring);

                let mut desc = ring.desc().clone();
                match c {
                    Change::AddActive => {
                        let new_id = (num_partitions + 10) as i32;
                        desc.partitions.insert(
                            new_id,
                            PartitionDesc {
                                id: new_id,
                                state: PartitionState::PartitionActive as i32,
                                state_timestamp: system_time_to_unix_secs(now),
                                tokens: precomputed[new_id as usize].clone(),
                                ..Default::default()
                            },
                        );
                    }
                    Change::SwitchPendingToActive => {
                        let pid = ring.pending_partition_ids()[0];
                        let _ = desc.update_partition_state(
                            pid,
                            PartitionState::PartitionActive,
                            SystemTime::now(),
                        );
                    }
                    Change::SwitchActiveToInactive => {
                        let pid = ring.active_partition_ids()[0];
                        let _ = desc.update_partition_state(
                            pid,
                            PartitionState::PartitionInactive,
                            SystemTime::now(),
                        );
                    }
                    Change::RemoveActive => {
                        let pid = ring.active_partition_ids()[0];
                        desc.remove_partition(pid);
                    }
                    Change::RemoveInactive => {
                        let pid = ring.inactive_partition_ids()[0];
                        desc.remove_partition(pid);
                    }
                }
                ring = PartitionRing::new(desc).unwrap();

                for (id, init_ids) in initial.iter().enumerate() {
                    let updated = ring
                        .shuffle_shard(&id.to_string(), shard_size)
                        .unwrap()
                        .partition_ids();
                    let (added, removed) = compare_partition_ids(init_ids, &updated);
                    assert!(added.len() <= 1);
                    assert!(removed.len() <= 1);
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShard_ConsistencyOnShardSizeChanged
#[test]
fn test_partition_ring_shuffle_shard_consistency_on_shard_size_changed() {
    let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 30, 0, 0);
    let first = ring.shuffle_shard("tenant-id", 3).unwrap().partition_ids();
    assert_eq!(3, first.len());
    let second = ring.shuffle_shard("tenant-id", 6).unwrap().partition_ids();
    assert_eq!(6, second.len());
    for id in &first {
        assert!(second.contains(id));
    }
    let third = ring.shuffle_shard("tenant-id", 9).unwrap().partition_ids();
    assert_eq!(9, third.len());
    for id in &second {
        assert!(third.contains(id));
    }
    let fourth = ring.shuffle_shard("tenant-id", 6).unwrap().partition_ids();
    assert_eq!(second, fourth);
    let fifth = ring.shuffle_shard("tenant-id", 3).unwrap().partition_ids();
    assert_eq!(first, fifth);
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShardWithLookback
#[test]
fn test_partition_ring_shuffle_shard_with_lookback() {
    #[derive(Clone, Copy)]
    enum EvKind {
        Add,
        Update,
        Remove,
        Test,
    }
    struct Ev {
        kind: EvKind,
        pid: i32,
        state: PartitionState,
        ts: SystemTime,
        shard_size: usize,
        expected: Vec<i32>,
    }

    let lookback = Duration::from_secs(3600);
    let now = SystemTime::now();
    let outside = now - 2 * lookback;
    let within = now - lookback / 2;
    let user_id = "user-1";

    let add = |pid: i32, state: PartitionState, ts: SystemTime| Ev {
        kind: EvKind::Add,
        pid,
        state,
        ts,
        shard_size: 0,
        expected: vec![],
    };
    let upd = |pid: i32, state: PartitionState, ts: SystemTime| Ev {
        kind: EvKind::Update,
        pid,
        state,
        ts,
        shard_size: 0,
        expected: vec![],
    };
    let rem = |pid: i32| Ev {
        kind: EvKind::Remove,
        pid,
        state: PartitionState::PartitionUnknown,
        ts: now,
        shard_size: 0,
        expected: vec![],
    };
    let tst = |shard_size: usize, mut expected: Vec<i32>| {
        expected.sort_unstable();
        Ev {
            kind: EvKind::Test,
            pid: 0,
            state: PartitionState::PartitionUnknown,
            ts: now,
            shard_size,
            expected,
        }
    };

    let cases = [
        (
            HashMap::from([
                (0, vec![user_token(user_id, "", 0) + 1]),
                (1, vec![user_token(user_id, "", 1) + 1]),
                (2, vec![user_token(user_id, "", 2) + 1]),
            ]),
            vec![
                add(0, PartitionState::PartitionActive, outside),
                add(1, PartitionState::PartitionActive, outside),
                add(2, PartitionState::PartitionActive, outside),
                tst(4, vec![0, 1, 2]),
            ],
        ),
        (
            HashMap::from([
                (0, vec![user_token(user_id, "", 0) + 1]),
                (1, vec![user_token(user_id, "", 1) + 1]),
                (2, vec![user_token(user_id, "", 2) + 1]),
            ]),
            vec![
                add(0, PartitionState::PartitionActive, outside),
                add(1, PartitionState::PartitionActive, outside),
                add(2, PartitionState::PartitionActive, outside),
                tst(1, vec![0]),
            ],
        ),
        (
            HashMap::from([
                (0, vec![user_token(user_id, "", 0) + 1]),
                (1, vec![user_token(user_id, "", 2) + 1]),
                (2, vec![user_token(user_id, "", 1) + 1]),
            ]),
            vec![
                add(0, PartitionState::PartitionActive, outside),
                add(1, PartitionState::PartitionActive, outside),
                tst(2, vec![0, 1]),
                add(2, PartitionState::PartitionActive, within),
                tst(2, vec![0, 1, 2]),
                upd(2, PartitionState::PartitionActive, outside),
                tst(2, vec![0, 2]),
            ],
        ),
        (
            HashMap::from([
                (0, vec![user_token(user_id, "", 1) + 1]),
                (1, vec![user_token(user_id, "", 2) + 1]),
                (2, vec![user_token(user_id, "", 0) + 1]),
            ]),
            vec![
                add(0, PartitionState::PartitionActive, outside),
                add(1, PartitionState::PartitionActive, outside),
                add(2, PartitionState::PartitionActive, outside),
                tst(2, vec![2, 0]),
                upd(2, PartitionState::PartitionInactive, within),
                tst(2, vec![2, 1, 0]),
                upd(2, PartitionState::PartitionInactive, outside),
                tst(2, vec![1, 0]),
                upd(2, PartitionState::PartitionActive, within),
                tst(2, vec![2, 1, 0]),
                upd(2, PartitionState::PartitionActive, outside),
                tst(2, vec![2, 0]),
            ],
        ),
        (
            HashMap::from([
                (0, vec![user_token(user_id, "", 1) + 1]),
                (1, vec![user_token(user_id, "", 2) + 1]),
                (2, vec![user_token(user_id, "", 0) + 1]),
            ]),
            vec![
                add(0, PartitionState::PartitionActive, outside),
                add(1, PartitionState::PartitionActive, outside),
                add(2, PartitionState::PartitionActive, outside),
                tst(2, vec![2, 0]),
                upd(2, PartitionState::PartitionInactive, within),
                tst(2, vec![2, 1, 0]),
                upd(2, PartitionState::PartitionInactive, outside),
                tst(2, vec![1, 0]),
                rem(2),
                tst(2, vec![1, 0]),
                upd(2, PartitionState::PartitionActive, within),
                tst(2, vec![2, 1, 0]),
                upd(2, PartitionState::PartitionActive, outside),
                tst(2, vec![2, 0]),
            ],
        ),
        (
            HashMap::from([
                (0, vec![user_token(user_id, "", 0) + 1]),
                (1, vec![user_token(user_id, "", 1) + 1]),
                (2, vec![user_token(user_id, "", 2) + 1]),
            ]),
            vec![
                add(0, PartitionState::PartitionActive, outside),
                add(1, PartitionState::PartitionActive, outside),
                add(2, PartitionState::PartitionActive, outside),
                tst(1, vec![0]),
                tst(2, vec![0, 1]),
                tst(3, vec![0, 1, 2]),
                tst(4, vec![0, 1, 2]),
            ],
        ),
    ];

    for (toks, timeline) in cases {
        let mut ring_desc = PartitionRingDesc::default();
        for ev in timeline {
            match ev.kind {
                EvKind::Add | EvKind::Update => {
                    ring_desc.partitions.insert(
                        ev.pid,
                        PartitionDesc {
                            state: ev.state as i32,
                            state_timestamp: system_time_to_unix_secs(ev.ts),
                            tokens: toks[&ev.pid].clone(),
                            ..Default::default()
                        },
                    );
                }
                EvKind::Remove => {
                    ring_desc.partitions.remove(&ev.pid);
                }
                EvKind::Test => {
                    let ring = PartitionRing::new(ring_desc.clone()).unwrap();
                    let sub = ring
                        .shuffle_shard_with_lookback(user_id, ev.shard_size, lookback, now)
                        .unwrap();
                    assert_eq!(ev.expected, sub.partition_ids());
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShardWithLookback_CorrectnessWithFuzzy
#[test]
fn test_partition_ring_shuffle_shard_with_lookback_correctness_with_fuzzy() {
    let lookback_period = Duration::from_secs(3600);
    let delay_between_events = Duration::from_secs(300);

    let precomputed = generate_partition_tokens(200).unwrap();
    for num_partitions in [9usize, 30, 60, 90] {
        let mut rnd = GoRand::new(1000 + num_partitions as i64);
        let mut init_desc = PartitionRingDesc::default();
        let init_now = SystemTime::now();
        for (i, toks) in precomputed.iter().enumerate().take(num_partitions) {
            init_desc.partitions.insert(
                i as i32,
                PartitionDesc {
                    id: i as i32,
                    state: PartitionState::PartitionActive as i32,
                    state_timestamp: system_time_to_unix_secs(init_now),
                    tokens: toks.clone(),
                    ..Default::default()
                },
            );
        }
        let mut ring = PartitionRing::new(init_desc).unwrap();
        let mut shard_size = 1usize;
        let user_id = random_string(&mut rnd, 6);

        let mut curr_time = SystemTime::now();
        let sub = ring
            .compute_shuffle_shard(&user_id, shard_size, Duration::ZERO, curr_time)
            .unwrap();
        let mut history: Vec<(SystemTime, Vec<i32>)> = vec![(curr_time, sub.partition_ids())];

        curr_time += lookback_period + Duration::from_secs(60);
        let mut next_pid = ring.partitions_count() as i32;

        for _ in 1..=100 {
            curr_time += delay_between_events;
            let r = rnd.uint32() % 100;
            let mut desc = ring.desc().clone();
            if r < 35 {
                desc.partitions.insert(
                    next_pid,
                    PartitionDesc {
                        id: next_pid,
                        state: PartitionState::PartitionPending as i32,
                        state_timestamp: system_time_to_unix_secs(curr_time),
                        tokens: precomputed[next_pid as usize].clone(),
                        ..Default::default()
                    },
                );
                ring = PartitionRing::new(desc).unwrap();
                next_pid += 1;
            } else if r < 70 {
                let pending = ring.pending_partition_ids();
                if !pending.is_empty() {
                    let pid = pending[(rnd.uint32() as usize) % pending.len()];
                    let _ = desc.update_partition_state(pid, PartitionState::PartitionActive, curr_time);
                    ring = PartitionRing::new(desc).unwrap();
                }
            } else if r < 80 {
                let active = ring.active_partition_ids();
                if !active.is_empty() {
                    let pid = active[(rnd.uint32() as usize) % active.len()];
                    let _ = desc.update_partition_state(pid, PartitionState::PartitionInactive, curr_time);
                    ring = PartitionRing::new(desc).unwrap();
                }
            } else if r < 90 {
                let inactive = ring.inactive_partition_ids();
                if !inactive.is_empty() {
                    let pid = inactive[(rnd.uint32() as usize) % inactive.len()];
                    desc.remove_partition(pid);
                    ring = PartitionRing::new(desc).unwrap();
                    for (_, pids) in &mut history {
                        pids.retain(|&p| p != pid);
                    }
                }
            } else {
                shard_size += 1;
            }

            let sub = ring
                .compute_shuffle_shard(&user_id, shard_size, Duration::ZERO, curr_time)
                .unwrap();
            history.push((curr_time, sub.partition_ids()));

            let sub_lb = ring
                .shuffle_shard_with_lookback(&user_id, shard_size, lookback_period, curr_time)
                .unwrap();
            let lb_ids = sub_lb.partition_ids();

            let cutoff = curr_time - lookback_period;
            history.retain(|(t, _)| *t >= cutoff);
            for (_, pids) in &history {
                for exp_pid in pids {
                    assert!(lb_ids.contains(exp_pid));
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShardWithLookback_Caching
#[test]
fn test_partition_ring_shuffle_shard_with_lookback_caching() {
    let tenant_id = "user-1";
    let other_tenant_id = "user-2";
    let subring_size = 3usize;
    let now = SystemTime::now();

    let mut desc = PartitionRingDesc::default();
    desc.add_partition(1, PartitionState::PartitionActive, now - Duration::from_secs(3600));
    desc.add_partition(2, PartitionState::PartitionActive, now - Duration::from_secs(5400));
    desc.add_partition(3, PartitionState::PartitionActive, now - Duration::from_secs(6 * 3600));
    desc.add_partition(4, PartitionState::PartitionActive, now - Duration::from_secs(6 * 3600));
    desc.add_partition(5, PartitionState::PartitionActive, now - Duration::from_secs(6 * 3600));
    desc.add_partition(6, PartitionState::PartitionActive, now - Duration::from_secs(6 * 3600));
    let ring = PartitionRing::new(desc).unwrap();

    // identical requests
    {
        let first = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size, Duration::from_secs(60), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size, Duration::from_secs(60), now)
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(vec![2, 3, 5], first.partition_ids());
    }

    // different shard size
    {
        let first = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size, Duration::from_secs(60), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size + 1, Duration::from_secs(60), now)
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(vec![2, 3, 5], first.partition_ids());
        assert_eq!(vec![1, 2, 3, 5], second.partition_ids());
    }

    // different identifiers
    {
        let first = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size, Duration::from_secs(60), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(other_tenant_id, subring_size, Duration::from_secs(60), now)
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(vec![2, 3, 5], first.partition_ids());
        assert_eq!(vec![1, 4, 6], second.partition_ids());
    }

    // different lookback windows
    {
        let first = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size, Duration::from_secs(30 * 60), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size, Duration::from_secs(80 * 60), now)
            .unwrap();
        let third = ring
            .shuffle_shard_with_lookback(tenant_id, subring_size, Duration::from_secs(100 * 60), now)
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &third));
        assert_eq!(vec![2, 3, 5], first.partition_ids());
        assert_eq!(vec![2, 3, 5], second.partition_ids());
        assert_eq!(vec![2, 3, 5, 6], third.partition_ids());
    }

    // same lookback window touching same partition state change
    {
        let first = ring
            .shuffle_shard_with_lookback(other_tenant_id, subring_size, Duration::from_secs(80 * 60), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(
                other_tenant_id,
                subring_size,
                Duration::from_secs(80 * 60),
                now + Duration::from_secs(300),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(vec![1, 2, 4, 6], first.partition_ids());
    }

    // crossing most recent partition state change
    {
        let first = ring
            .shuffle_shard_with_lookback(other_tenant_id, subring_size, Duration::from_secs(80 * 60), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(
                other_tenant_id,
                subring_size,
                Duration::from_secs(80 * 60),
                now + Duration::from_secs(1800),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(vec![1, 2, 4, 6], first.partition_ids());
        assert_eq!(vec![1, 4, 6], second.partition_ids());
    }

    // crossing 2 most recent partition state changes
    {
        let first = ring
            .shuffle_shard_with_lookback(other_tenant_id, subring_size, Duration::from_secs(100 * 60), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(
                other_tenant_id,
                subring_size,
                Duration::from_secs(100 * 60),
                now + Duration::from_secs(3600),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(vec![1, 2, 4, 5, 6], first.partition_ids());
        assert_eq!(vec![1, 4, 6], second.partition_ids());
    }

    // alternate times crossing most recent partition state change
    {
        let first_early = ring
            .shuffle_shard_with_lookback(other_tenant_id, subring_size, Duration::from_secs(62 * 60), now)
            .unwrap();
        let first_late = ring
            .shuffle_shard_with_lookback(
                other_tenant_id,
                subring_size,
                Duration::from_secs(62 * 60),
                now + Duration::from_secs(300),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&first_early, &first_late));
        assert_eq!(vec![1, 2, 4, 6], first_early.partition_ids());
        assert_eq!(vec![1, 4, 6], first_late.partition_ids());

        let second_early = ring
            .shuffle_shard_with_lookback(other_tenant_id, subring_size, Duration::from_secs(62 * 60), now)
            .unwrap();
        let second_late = ring
            .shuffle_shard_with_lookback(
                other_tenant_id,
                subring_size,
                Duration::from_secs(62 * 60),
                now + Duration::from_secs(300),
            )
            .unwrap();
        assert!(!Arc::ptr_eq(&second_early, &second_late));
        assert_eq!(first_early.partition_ids(), second_early.partition_ids());
        assert_eq!(first_late.partition_ids(), second_late.partition_ids());
        assert!(!Arc::ptr_eq(&first_early, &second_early));
        assert!(Arc::ptr_eq(&first_late, &second_late));
    }

    // lookback window including all partitions
    {
        let first = ring
            .shuffle_shard_with_lookback(other_tenant_id, subring_size, Duration::from_secs(12 * 3600), now)
            .unwrap();
        let second = ring
            .shuffle_shard_with_lookback(
                other_tenant_id,
                subring_size,
                Duration::from_secs(12 * 3600),
                now + Duration::from_secs(1800),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(vec![1, 2, 3, 4, 5, 6], first.partition_ids());
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_ShuffleShardWithLookback_CachingConcurrency
#[test]
fn test_partition_ring_shuffle_shard_with_lookback_caching_concurrency() {
    for cache_size in [0usize, 128] {
        let ring = create_partition_ring_with_partitions(
            PartitionRingOptions {
                token_generator: None,
                shuffle_shard_cache_size: cache_size,
            },
            100,
            10,
            10,
        );
        let now = SystemTime::now() + Duration::from_secs(7200);
        let lookback = Duration::from_secs(3600);
        let mut handles = Vec::new();
        for w in 0..10 {
            let r = Arc::clone(&ring);
            handles.push(std::thread::spawn(move || {
                let uid = format!("user-{w}");
                let expected = r.shuffle_shard_with_lookback(&uid, 3, lookback, now).unwrap();
                for req in 0..1000 {
                    let actual = r.shuffle_shard_with_lookback(&uid, 3, lookback, now).unwrap();
                    assert_eq!(expected.desc(), actual.desc());
                    let _ = r
                        .shuffle_shard_with_lookback(&format!("stress-{req}"), 3, lookback, now)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(ring.cache_with_lookback_len() > 0);
        assert_eq!(0, ring.cache_without_lookback_len());
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRingGetTokenRangesForPartition
#[test]
fn test_partition_ring_get_token_ranges_for_partition() {
    let gen = mimir_ring::token_gen::RandomTokenGenerator::with_seed(42);
    use mimir_ring::token_gen::TokenGenerator;

    type TokenRangeCase = (HashMap<i32, Vec<u32>>, HashMap<i32, Vec<u32>>);
    let cases: Vec<TokenRangeCase> = vec![
        (
            HashMap::from([(0, gen.generate_tokens(512, &[]))]),
            HashMap::from([(0, vec![0, u32::MAX])]),
        ),
        (
            HashMap::from([(0, vec![25, 75]), (1, vec![10, 50, 100])]),
            HashMap::from([
                (0, vec![10, 24, 50, 74]),
                (1, vec![0, 9, 25, 49, 75, u32::MAX]),
            ]),
        ),
        (
            HashMap::from([
                (0, vec![10, 20, 30, 40, 50]),
                (1, vec![1000, 2000, 3000, 4000]),
            ]),
            HashMap::from([(0, vec![0, 49, 4000, u32::MAX]), (1, vec![50, 3999])]),
        ),
        (
            HashMap::from([(0, vec![99]), (1, vec![100])]),
            HashMap::from([(0, vec![0, 98, 100, u32::MAX]), (1, vec![99, 99])]),
        ),
        (
            HashMap::from([(0, vec![0]), (1, vec![u32::MAX])]),
            HashMap::from([
                (0, vec![u32::MAX, u32::MAX]),
                (1, vec![0, u32::MAX - 1]),
            ]),
        ),
    ];

    for (part_tokens, expected) in cases {
        let mut ring_desc = PartitionRingDesc::default();
        for (p, toks) in part_tokens {
            ring_desc.partitions.insert(
                p,
                PartitionDesc {
                    tokens: toks,
                    ..Default::default()
                },
            );
        }
        let r = PartitionRing::new(ring_desc).unwrap();
        for (id, exp) in expected {
            let ranges = r.get_token_ranges_for_partition(id).unwrap();
            assert_eq!(exp, ranges);
            for token in ranges {
                let i = search_token(r.ring_tokens(), token);
                let tok = r.ring_tokens()[i];
                assert_eq!(Some(&id), r.partition_by_token().get(&tok));
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRingGetTokenRangesForPartitionExhaustive
#[test]
fn test_partition_ring_get_token_ranges_for_partition_exhaustive() {
    for partitions in [1i32, 3, 9, 27, 81] {
        let mut prd = PartitionRingDesc::default();
        for pid in 0..partitions {
            prd.add_partition(pid, PartitionState::PartitionActive, UNIX_EPOCH);
        }
        let pr = PartitionRing::new(prd).unwrap();
        for partition in 0..partitions {
            let ranges = pr.get_token_ranges_for_partition(partition).unwrap();
            for pair in ranges.as_chunks::<2>().0 {
                let (start, end) = (pair[0], pair[1]);
                assert_eq!(partition, pr.active_partition_for_key(start).unwrap());
                assert_eq!(partition, pr.active_partition_for_key(end).unwrap());
                let mid = ((start as u64 + end as u64) / 2) as u32;
                assert_eq!(partition, pr.active_partition_for_key(mid).unwrap());
                if start > 0 {
                    assert_ne!(partition, pr.active_partition_for_key(start - 1).unwrap());
                }
                if end < u32::MAX {
                    assert_ne!(partition, pr.active_partition_for_key(end + 1).unwrap());
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestActivePartitionBatchRing
#[test]
fn test_active_partition_batch_ring() {
    let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 2, 0, 0);
    let batch = ActivePartitionBatchRing::new(ring);
    assert_eq!(1, batch.replication_factor());
    assert_eq!(2, batch.instances_count());
    let set = batch.get(12345).unwrap();
    assert_eq!(1, set.instances.len());
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestActivePartitionBatchRing_InstancesCount
#[test]
fn test_active_partition_batch_ring_instances_count() {
    let active = ActivePartitionBatchRing::new(create_partition_ring_with_partitions(
        PartitionRingOptions::default(),
        10,
        3,
        2,
    ));
    assert_eq!(10, active.instances_count());
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestActivePartitionBatchRing_Get
#[test]
fn test_active_partition_batch_ring_get() {
    let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 10, 5, 5);
    let active_ring = ActivePartitionBatchRing::new(Arc::clone(&ring));
    let mut rnd = GoRand::new(9876);
    for _ in 0..1000 {
        let key = rnd.uint32() % (i32::MAX as u32);
        let expected = ring.active_partition_for_key(key).unwrap();
        let actual = active_ring.get(key).unwrap();
        assert_eq!(1, actual.instances.len());
        assert_eq!(expected.to_string(), actual.instances[0].id);
        assert_eq!(expected.to_string(), actual.instances[0].addr);
        assert_eq!(0, actual.max_errors);
        assert_eq!(0, actual.max_unavailable_zones);
        assert!(!actual.zone_awareness_enabled);
        assert_eq!(expected, actual.partition_id);
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_MultiPartitionOwnerIDs
#[test]
fn test_partition_ring_multi_partition_owner_ids() {
    let instance1 = "instance/0";
    let instance2 = "instance-1";
    let now = UNIX_EPOCH + Duration::from_secs(1_696_118_400);

    let mut desc = PartitionRingDesc::default();
    desc.add_partition(1, PartitionState::PartitionActive, now - Duration::from_secs(3600));
    desc.add_partition(2, PartitionState::PartitionActive, now - Duration::from_secs(5400));
    desc.add_or_update_owner(
        &mimir_ring::partition_ring::multi_partition_owner_instance_id(instance1, 0),
        OwnerState::OwnerActive,
        0,
        now,
    );
    desc.add_or_update_owner(
        &mimir_ring::partition_ring::multi_partition_owner_instance_id(instance1, 1),
        OwnerState::OwnerActive,
        1,
        now,
    );
    desc.add_or_update_owner(
        &mimir_ring::partition_ring::multi_partition_owner_instance_id(instance2, 1),
        OwnerState::OwnerActive,
        1,
        now,
    );
    desc.add_or_update_owner(
        &mimir_ring::partition_ring::multi_partition_owner_instance_id(instance2, 2),
        OwnerState::OwnerActive,
        2,
        now,
    );
    let ring = PartitionRing::new(desc).unwrap();

    assert_eq!(vec![instance1.to_string()], ring.multi_partition_owner_ids(0));
    let mut p1 = ring.multi_partition_owner_ids(1);
    p1.sort();
    let mut exp_p1 = vec![instance1.to_string(), instance2.to_string()];
    exp_p1.sort();
    assert_eq!(exp_p1, p1);
    assert_eq!(vec![instance2.to_string()], ring.multi_partition_owner_ids(2));
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestActivePartitionBatchRing_GetKeysByPartition
#[test]
fn test_active_partition_batch_ring_get_keys_by_partition() {
    // no active partitions returns error
    {
        let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 0, 3, 0);
        let r = ActivePartitionBatchRing::new(ring);
        assert!(r.get_keys_by_partition(None, &[1, 2, 3]).is_err());
    }

    // empty keys returns empty
    {
        let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 10, 0, 0);
        let r = ActivePartitionBatchRing::new(ring);
        assert!(r.get_keys_by_partition(None, &[]).unwrap().is_empty());
    }

    // context cancelled returns error
    {
        let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 10, 0, 0);
        let r = ActivePartitionBatchRing::new(ring);
        let ctx = ServiceContext::background();
        ctx.cancel();
        assert!(r.get_keys_by_partition(Some(&ctx), &[1, 2, 3]).is_err());
    }

    // gaps in partition IDs does not panic
    {
        let mut desc = PartitionRingDesc::default();
        let now = SystemTime::now();
        desc.add_partition(0, PartitionState::PartitionActive, now);
        desc.add_partition(1, PartitionState::PartitionInactive, now);
        desc.add_partition(2, PartitionState::PartitionInactive, now);
        desc.add_partition(5, PartitionState::PartitionActive, now);
        desc.add_partition(10, PartitionState::PartitionActive, now);
        let ring = PartitionRing::new(desc).unwrap();
        let r = ActivePartitionBatchRing::new(Arc::clone(&ring));

        let mut rnd = GoRand::new(0);
        let keys: Vec<u32> = (0..100).map(|_| rnd.uint32()).collect();
        let res = r.get_keys_by_partition(None, &keys).unwrap();
        assert!(!res.is_empty());

        let mut key_to_part = HashMap::new();
        for pk in &res {
            assert!([0, 5, 10].contains(&pk.partition_id));
            assert!(!pk.indexes.is_empty());
            for &idx in &pk.indexes {
                key_to_part.insert(idx, pk.partition_id);
            }
        }
        assert_eq!(keys.len(), key_to_part.len());
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(ring.active_partition_for_key(k).unwrap(), key_to_part[&i]);
        }
    }

    // returned partitions match ActivePartitionForKey
    {
        let ring = create_partition_ring_with_partitions(PartitionRingOptions::default(), 10, 5, 0);
        let r = ActivePartitionBatchRing::new(Arc::clone(&ring));
        let mut rnd = GoRand::new(0);
        let keys: Vec<u32> = (0..1000).map(|_| rnd.uint32()).collect();
        let res = r.get_keys_by_partition(None, &keys).unwrap();
        let mut key_to_part = HashMap::new();
        for pk in &res {
            for &idx in &pk.indexes {
                key_to_part.insert(idx, pk.partition_id);
            }
        }
        assert_eq!(keys.len(), key_to_part.len());
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(ring.active_partition_for_key(k).unwrap(), key_to_part[&i]);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestActivePartitionBatchRing_GetKeysByPartition_MatchesDoBatchWithOptions
#[test]
fn test_active_partition_batch_ring_get_keys_by_partition_matches_do_batch_with_options() {
    for (num_active, num_inactive, num_keys) in [(10, 0, 100), (10, 5, 100), (30, 0, 1000)] {
        let ring = create_partition_ring_with_partitions(
            PartitionRingOptions::default(),
            num_active,
            num_inactive,
            0,
        );
        let r = ActivePartitionBatchRing::new(ring);
        let mut rnd = GoRand::new(0);
        let keys: Vec<u32> = (0..num_keys).map(|_| rnd.uint32()).collect();

        let split_res = r.get_keys_by_partition(None, &keys).unwrap();
        let mut do_batch_res: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, &k) in keys.iter().enumerate() {
            let rs = r.get(k).unwrap();
            for inst in rs.instances {
                do_batch_res.entry(inst.addr).or_default().push(i);
            }
        }

        let mut split_map: HashMap<String, Vec<usize>> = HashMap::with_capacity(split_res.len());
        for pk in split_res {
            split_map.insert(pk.partition_id.to_string(), pk.indexes);
        }
        assert_eq!(do_batch_res.len(), split_map.len());
        for (addr, mut expected_idxs) in do_batch_res {
            let mut actual_idxs = split_map.remove(&addr).unwrap();
            expected_idxs.sort_unstable();
            actual_idxs.sort_unstable();
            assert_eq!(expected_idxs, actual_idxs);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_countTokens
#[test]
fn test_partition_ring_count_tokens() {
    let empty = PartitionRing::new(PartitionRingDesc::default()).unwrap();
    assert!(empty.count_tokens().is_empty());

    let desc = PartitionRingDesc {
        partitions: HashMap::from([
            (1, PartitionDesc { tokens: vec![1_000_000, 3_000_000, 6_000_000], ..Default::default() }),
            (2, PartitionDesc { tokens: vec![2_000_000, 4_000_000, 8_000_000], ..Default::default() }),
            (3, PartitionDesc { tokens: vec![5_000_000, 9_000_000], ..Default::default() }),
        ]),
        owners: HashMap::new(),
    };
    let ring = PartitionRing::new(desc).unwrap();
    assert_eq!(
        HashMap::from([
            (1, 3_000_000 + (u32::MAX as i64 + 1 - 9_000_000)),
            (2, 4_000_000),
            (3, 2_000_000),
        ]),
        ring.count_tokens()
    );

    let desc_empty_toks = PartitionRingDesc {
        partitions: HashMap::from([
            (1, PartitionDesc { tokens: vec![1_000_000, 3_000_000, 6_000_000], ..Default::default() }),
            (2, PartitionDesc { tokens: vec![2_000_000, 4_000_000, 8_000_000], ..Default::default() }),
            (3, PartitionDesc { tokens: vec![5_000_000, 9_000_000], ..Default::default() }),
            (4, PartitionDesc { tokens: vec![], ..Default::default() }),
        ]),
        owners: HashMap::new(),
    };
    let ring2 = PartitionRing::new(desc_empty_toks).unwrap();
    let counts = ring2.count_tokens();
    assert_eq!(Some(&0), counts.get(&4));
}

fn assert_equivalent_partition_routing(want: &PartitionRing, got: &PartitionRing) {
    assert_eq!(want.partition_ids(), got.partition_ids());
    assert_eq!(want.count_tokens(), got.count_tokens());
    let mut keys = vec![0u32, 1, u32::MAX];
    for &tok in want.ring_tokens() {
        keys.push(tok.wrapping_sub(1));
        keys.push(tok);
        keys.push(tok.wrapping_add(1));
    }
    for key in keys {
        let w = want.active_partition_for_key(key);
        let g = got.active_partition_for_key(key);
        assert_eq!(w.is_ok(), g.is_ok());
        if let (Ok(wid), Ok(gid)) = (w, g) {
            assert_eq!(wid, gid);
        }
    }
    for id in want.partition_ids() {
        assert_eq!(
            want.get_token_ranges_for_partition(id).unwrap(),
            got.get_token_ranges_for_partition(id).unwrap()
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_DerivedTokensRouteLikeStoredTokens
#[test]
fn test_partition_ring_derived_tokens_route_like_stored_tokens() {
    let mut stored = PartitionRingDesc::default();
    for id in 0..6i32 {
        stored.partitions.insert(
            id,
            PartitionDesc {
                id,
                state: PartitionState::PartitionActive as i32,
                state_timestamp: 1,
                tokens: vec![(id as u32) << 28, ((id + 8) as u32) << 28],
                ..Default::default()
            },
        );
    }
    stored.partitions.insert(
        7,
        PartitionDesc {
            id: 7,
            state: PartitionState::PartitionActive as i32,
            tokens: vec![3, 4],
            ..Default::default()
        },
    );
    stored.partitions.insert(
        6,
        PartitionDesc {
            id: 6,
            state: PartitionState::PartitionInactive as i32,
            tokens: vec![1, 2],
            ..Default::default()
        },
    );
    let want = PartitionRing::new(stored.clone()).unwrap();

    for mode in ["all derived", "mixed", "all stored"] {
        let mut desc = stored.clone();
        let mut token_map = HashMap::new();
        let mut expected_derived = Vec::new();
        for (&id, part) in &mut desc.partitions {
            token_map.insert(id, stored.partitions[&id].tokens.clone());
            if mode == "all derived" || (mode == "mixed" && id % 2 == 0) {
                part.tokens = vec![id as u32];
                part.token_scheme = PartitionTokenScheme::PartitionTokensSmt512 as i32;
                expected_derived.push(id);
            }
        }
        let before = desc.clone();
        let gen = Arc::new(MapPartitionTokenGen::from_map(token_map));
        let opts = PartitionRingOptions {
            token_generator: Some(gen.clone()),
            ..Default::default()
        };
        let got = PartitionRing::with_options(desc.clone(), opts).unwrap();

        let mut calls = gen.calls.lock().clone();
        calls.sort_unstable();
        expected_derived.sort_unstable();
        assert_eq!(expected_derived, calls);
        assert_eq!(before, desc);

        assert_equivalent_partition_routing(&want, &got);

        for tenant in ["tenant-a", "tenant-b", "tenant-c"] {
            let want_shard = want.shuffle_shard(tenant, 3).unwrap();
            let got_shard = got.shuffle_shard(tenant, 3).unwrap();
            assert_equivalent_partition_routing(&want_shard, &got_shard);
            assert!(got_shard.options().token_generator.is_some());
            for id in got_shard.partition_ids() {
                assert_eq!(desc.partitions[&id], got_shard.desc().partitions[&id]);
            }

            let want_nested = want_shard.shuffle_shard(tenant, 1).unwrap();
            let got_nested = got_shard.shuffle_shard(tenant, 1).unwrap();
            assert_equivalent_partition_routing(&want_nested, &got_nested);

            let want_lb = want
                .shuffle_shard_with_lookback(
                    tenant,
                    3,
                    Duration::from_secs(3600),
                    UNIX_EPOCH + Duration::from_secs(3600),
                )
                .unwrap();
            let got_lb = got
                .shuffle_shard_with_lookback(
                    tenant,
                    3,
                    Duration::from_secs(3600),
                    UNIX_EPOCH + Duration::from_secs(3600),
                )
                .unwrap();
            assert_equivalent_partition_routing(&want_lb, &got_lb);
        }
        assert_eq!(before, desc);
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_WithoutDerivedPartitionsSkipsGenerator
#[test]
fn test_partition_ring_without_derived_partitions_skips_generator() {
    let cases = [
        (PartitionRingDesc::default(), HashMap::new()),
        (
            PartitionRingDesc {
                partitions: HashMap::from([(
                    42,
                    PartitionDesc {
                        state: PartitionState::PartitionActive as i32,
                        ..Default::default()
                    },
                )]),
                owners: HashMap::new(),
            },
            HashMap::from([(42, 0i64)]),
        ),
    ];

    for (desc, owned) in cases {
        for with_gen in [false, true] {
            let gen = Arc::new(MapPartitionTokenGen::from_map(HashMap::new()));
            let mut opts = PartitionRingOptions::default();
            if with_gen {
                opts.token_generator = Some(gen.clone());
            }
            let r = PartitionRing::with_options(desc.clone(), opts).unwrap();
            assert!(gen.calls.lock().is_empty());
            assert_eq!(owned, r.count_tokens());
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRing_FailsWhenTokensCannotBeResolved
#[test]
fn test_partition_ring_fails_when_tokens_cannot_be_resolved() {
    let cases: Vec<(i32, Option<Arc<dyn PartitionTokenGenerator>>)> = vec![
        (PartitionTokenScheme::PartitionTokensSmt512 as i32, None),
        (
            PartitionTokenScheme::PartitionTokensSmt512 as i32,
            Some(Arc::new(MapPartitionTokenGen::failing("generator unavailable"))),
        ),
        (
            99,
            Some(Arc::new(MapPartitionTokenGen::from_map(HashMap::from([
                (42, vec![42]),
            ])))),
        ),
    ];

    for (scheme, generator) in cases {
        let desc = PartitionRingDesc {
            partitions: HashMap::from([(
                42,
                PartitionDesc {
                    state: PartitionState::PartitionActive as i32,
                    tokens: vec![42],
                    token_scheme: scheme,
                    ..Default::default()
                },
            )]),
            owners: HashMap::new(),
        };
        let opts = PartitionRingOptions {
            token_generator: generator,
            ..Default::default()
        };
        assert!(PartitionRing::with_options(desc, opts).is_err());
    }
}

// Upstream: github.com/grafana/dskit/ring/partition_ring_test.go::TestPartitionRingDesc_SameExceptPending
#[test]
fn test_partition_ring_desc_same_except_pending() {
    let base = || PartitionRingDesc {
        partitions: HashMap::from([
            (1, PartitionDesc { id: 1, state: PartitionState::PartitionActive as i32, state_timestamp: 10, tokens: vec![100, 200], ..Default::default() }),
            (2, PartitionDesc { id: 2, state: PartitionState::PartitionInactive as i32, state_timestamp: 10, tokens: vec![300], ..Default::default() }),
            (3, PartitionDesc { id: 3, state: PartitionState::PartitionPending as i32, state_timestamp: 10, tokens: vec![400], ..Default::default() }),
        ]),
        owners: HashMap::from([
            ("owner-1".into(), OwnerDesc { owned_partition: 1, state: OwnerState::OwnerActive as i32, ..Default::default() }),
            ("owner-2".into(), OwnerDesc { owned_partition: 2, state: OwnerState::OwnerActive as i32, ..Default::default() }),
            ("owner-3".into(), OwnerDesc { owned_partition: 3, state: OwnerState::OwnerActive as i32, ..Default::default() }),
        ]),
    };

    type MutatorCase = (bool, Box<dyn Fn(&mut PartitionRingDesc)>);
    let cases: Vec<MutatorCase> = vec![
        (
            true,
            Box::new(|d| {
                d.partitions.insert(
                    4,
                    PartitionDesc {
                        id: 4,
                        state: PartitionState::PartitionPending as i32,
                        tokens: vec![500],
                        ..Default::default()
                    },
                );
            }),
        ),
        (
            true,
            Box::new(|d| {
                d.owners.insert(
                    "owner-3b".into(),
                    OwnerDesc {
                        owned_partition: 3,
                        state: OwnerState::OwnerActive as i32,
                        ..Default::default()
                    },
                );
            }),
        ),
        (
            true,
            Box::new(|d| {
                d.partitions.remove(&3);
                d.owners.remove("owner-3");
            }),
        ),
        (
            false,
            Box::new(|d| {
                d.partitions.get_mut(&3).unwrap().state = PartitionState::PartitionActive as i32;
            }),
        ),
        (
            false,
            Box::new(|d| {
                d.partitions.get_mut(&1).unwrap().tokens = vec![100, 250];
            }),
        ),
        (
            false,
            Box::new(|d| {
                d.partitions.get_mut(&2).unwrap().state_timestamp = 20;
            }),
        ),
        (
            false,
            Box::new(|d| {
                d.owners.insert(
                    "owner-1b".into(),
                    OwnerDesc {
                        owned_partition: 1,
                        state: OwnerState::OwnerActive as i32,
                        ..Default::default()
                    },
                );
            }),
        ),
        (
            false,
            Box::new(|d| {
                d.owners.insert(
                    "owner-3".into(),
                    OwnerDesc {
                        owned_partition: 1,
                        state: OwnerState::OwnerActive as i32,
                        ..Default::default()
                    },
                );
            }),
        ),
    ];

    for (same, change) in cases {
        let a = base();
        let mut b = base();
        change(&mut b);
        assert_eq!(same, same_except_pending(&a, &b));
        assert_eq!(same, same_except_pending(&b, &a));
    }
}

