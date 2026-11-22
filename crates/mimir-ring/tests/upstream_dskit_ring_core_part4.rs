//! Ported upstream dskit/ring core tests (part 4: ring_test.go — 36 tests).
//!
//! Ported from github.com/grafana/dskit/ring/ring_test.go

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mimir_common::services::{start_and_await_running, stop_and_await_terminated, ManagedBasicService, ServiceContext};
use mimir_proto::grpcutilpb::status_with_http_response;
use mimir_proto::httpgrpcpb::HttpResponse;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::kv::inmemory::{InMemoryKv, InMemoryStore};
use mimir_ring::kv::KvClient;
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig, NoopFlushTransferer};
use mimir_ring::prng::{shuffle_shard_seed, GoRand};
use mimir_ring::ring::batch::{do_batch, do_batch_with_options, is_http_status_4xx, DoBatchOptions};
use mimir_ring::ring::model::{ring_desc_codec, system_time_to_unix_secs, DescExt};
use mimir_ring::ring::replication_set::SimpleError;
use mimir_ring::ring::replication_set_tracker::{BoxDynError, RingContext};
use mimir_ring::ring::replication_strategy::{
    DefaultReplicationStrategy, IgnoreUnhealthyInstancesReplicationStrategy, ReplicationSet,
    ReplicationStrategy, READ, REPORTING, WRITE,
};
use mimir_ring::ring::{
    merge_token_groups, GetOptions, Ring, RingError, RingSettings,
};
use mimir_ring::token_gen::{RandomTokenGenerator, TokenGenerator};
use parking_lot::Mutex;
use prometheus::Registry;
use tokio::sync::Notify;

const NUM_TOKENS: usize = 512;

fn make_settings(hb: Duration, rf: usize, za: bool) -> RingSettings {
    RingSettings {
        heartbeat_timeout: hb,
        replication_factor: rf,
        zone_awareness_enabled: za,
        excluded_zones: Vec::new(),
        subring_cache_disabled: false,
    }
}

fn generate_keys(rng: &mut GoRand, count: usize) -> Vec<u32> {
    (0..count).map(|_| rng.uint32()).collect()
}

fn gen_ring_instance_with_info(
    id: &str,
    zone: &str,
    tokens: Vec<u32>,
    reg_at: Option<SystemTime>,
) -> InstanceDesc {
    InstanceDesc {
        id: id.to_string(),
        addr: id.to_string(),
        timestamp: system_time_to_unix_secs(SystemTime::now()),
        registered_timestamp: reg_at.map(system_time_to_unix_secs).unwrap_or(0),
        state: InstanceState::Active as i32,
        tokens,
        zone: zone.to_string(),
        ..Default::default()
    }
}

fn gen_ring_instance(
    gen: &dyn TokenGenerator,
    id: usize,
    zone: usize,
    num_tokens: usize,
    used: &[u32],
) -> (String, InstanceDesc, Vec<u32>) {
    let instance_id = format!("instance-{id}");
    let zone_id = format!("zone-{zone}");
    let toks = gen.generate_tokens(num_tokens, used);
    let desc = gen_ring_instance_with_info(&instance_id, &zone_id, toks.clone(), Some(SystemTime::now()));
    (instance_id, desc, toks)
}

fn gen_ring_instances(
    gen: &dyn TokenGenerator,
    num_instances: usize,
    num_zones: usize,
    num_tokens: usize,
) -> HashMap<String, InstanceDesc> {
    let mut instances = HashMap::with_capacity(num_instances);
    let mut all_tokens = Vec::new();
    for i in 1..=num_instances {
        let (id, desc, toks) = gen_ring_instance(gen, i, i % num_zones, num_tokens, &all_tokens);
        instances.insert(id, desc);
        all_tokens.extend_from_slice(&toks);
    }
    instances
}

fn user_token(user: &str, zone: &str, skip: usize) -> u32 {
    let mut r = GoRand::new(shuffle_shard_seed(user, zone));
    for _ in 0..skip {
        let _ = r.uint32();
    }
    r.uint32()
}

fn make_read_only(mut desc: InstanceDesc, ts: SystemTime) -> InstanceDesc {
    desc.read_only = true;
    desc.read_only_updated_timestamp = system_time_to_unix_secs(ts);
    desc
}

fn compare_replication_sets(first: &ReplicationSet, second: &ReplicationSet) -> (Vec<String>, Vec<String>) {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    for inst in &first.instances {
        if !second.includes(&inst.addr) {
            added.push(inst.addr.clone());
        }
    }
    for inst in &second.instances {
        if !first.includes(&inst.addr) {
            removed.push(inst.addr.clone());
        }
    }
    (added, removed)
}

fn get_random_instance_id(instances: &HashMap<String, InstanceDesc>, rng: &mut GoRand) -> String {
    let mut ids: Vec<String> = instances.keys().cloned().collect();
    ids.sort();
    let idx = rng.intn(ids.len());
    ids[idx].clone()
}

fn generate_tokens_linear(instance_id: usize, num_instances: usize, num_tokens: usize) -> Vec<u32> {
    let mut tokens = Vec::with_capacity(num_tokens);
    let step = i64::from(u32::MAX) / (num_tokens as i64);
    let offset = (step / (num_instances as i64)) * (instance_id as i64);
    let mut t = offset;
    while t <= i64::from(u32::MAX) {
        tokens.push(t as u32);
        t += step;
    }
    tokens
}

// ============================================================================
// Batch tests (3)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestDoBatchZeroInstances
#[tokio::test]
async fn test_do_batch_zero_instances() {
    let mut rng = GoRand::new(1);
    let keys = generate_keys(&mut rng, 10);
    let r = Ring::new_for_testing(make_settings(Duration::ZERO, 0, false), Arc::new(DefaultReplicationStrategy));
    r.set_ring_state_from_desc(Desc::default(), false, false, false);
    let res = do_batch(
        &RingContext::background(),
        WRITE,
        r.as_ref(),
        &keys,
        |_, _| async { Ok(()) },
        Some(Arc::new(|| {})),
    )
    .await;
    assert!(res.is_err());
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestDoBatchWithOptionsContextCancellation
#[tokio::test]
async fn test_do_batch_with_options_context_cancellation() {
    const NUM_KEYS: usize = 200_000;
    const NUM_INSTANCES: usize = 100;
    const NUM_ZONES: usize = 3;

    let keys = generate_keys(&mut GoRand::new(0), NUM_KEYS);
    let gen = RandomTokenGenerator::with_seed(0);
    let desc = Desc {
        ingesters: gen_ring_instances(&gen, NUM_INSTANCES, NUM_ZONES, NUM_TOKENS),
    };
    let mut settings = make_settings(Duration::from_secs(3600), NUM_ZONES, true);
    settings.subring_cache_disabled = true;
    let r = Ring::new_for_testing(settings, Arc::new(DefaultReplicationStrategy));
    r.set_ring_state_from_desc(desc, false, false, false);

    let t0 = Instant::now();
    do_batch_with_options(
        &RingContext::background(),
        WRITE,
        r.as_ref(),
        &keys,
        |_, _| async { Ok(()) },
        DoBatchOptions::default(),
    )
    .await
    .unwrap();
    let baseline = t0.elapsed();

    let run_case = |ctx: RingContext, expected_msg: &'static str, hang_callback: bool| {
        let r_c = Arc::clone(&r);
        let keys_c = keys.clone();
        async move {
            let cleaned = Arc::new(AtomicBool::new(false));
            let clean_notify = Arc::new(Notify::new());
            let cl_c = Arc::clone(&cleaned);
            let cn_c = Arc::clone(&clean_notify);
            let ctx_cb = ctx.clone();

            let err = do_batch_with_options(
                &ctx,
                WRITE,
                r_c.as_ref(),
                &keys_c,
                move |_, _| {
                    let c = ctx_cb.clone();
                    async move {
                        if hang_callback {
                            c.done().await;
                            Ok(())
                        } else {
                            panic!("should not be called");
                        }
                    }
                },
                DoBatchOptions {
                    cleanup: Some(Arc::new(move || {
                        cl_c.store(true, Ordering::Release);
                        cn_c.notify_waiters();
                    })),
                    is_client_error: None,
                },
            )
            .await
            .unwrap_err();
            assert_eq!(err.to_string(), expected_msg);
            while !cleaned.load(Ordering::Acquire) {
                clean_notify.notified().await;
            }
        }
    };

    // 1. context deadline exceeded (using 1ns timeout or tiny fraction of baseline)
    {
        let timeout = (baseline / 100).max(Duration::from_nanos(1));
        let (ctx, _cancel) = RingContext::background().with_timeout(timeout);
        if timeout == Duration::from_nanos(1) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        run_case(ctx, "context deadline exceeded", false).await;
    }
    // 2. context deadline exceeded with cause
    {
        let timeout = (baseline / 100).max(Duration::from_nanos(1));
        let (ctx, _cancel) = RingContext::background()
            .with_timeout_cause(timeout, Arc::new(SimpleError("cancel cause".into())));
        if timeout == Duration::from_nanos(1) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        run_case(ctx, "cancel cause", false).await;
    }
    // 3. context initially cancelled without cause
    {
        let (ctx, cancel) = RingContext::background().with_cancel();
        cancel.cancel(None);
        run_case(ctx, "context canceled", false).await;
    }
    // 4. context initially cancelled with cause
    {
        let (ctx, cancel) = RingContext::background().with_cancel_cause();
        cancel.cancel(Some(Arc::new(SimpleError("cancel cause".into()))));
        run_case(ctx, "cancel cause", false).await;
    }
    // 5. context cancelled during batch processing
    {
        let (ctx, cancel) = RingContext::background().with_cancel_cause();
        let started = Arc::new(AtomicUsize::new(0));
        let started_notify = Arc::new(Notify::new());
        let st_c = Arc::clone(&started);
        let sn_c = Arc::clone(&started_notify);
        let cancel_c = cancel.clone();
        tokio::spawn(async move {
            while st_c.load(Ordering::Acquire) < NUM_INSTANCES {
                sn_c.notified().await;
            }
            cancel_c.cancel(Some(Arc::new(SimpleError("cancel cause".into()))));
        });

        let cleaned = Arc::new(AtomicBool::new(false));
        let clean_notify = Arc::new(Notify::new());
        let cl_c = Arc::clone(&cleaned);
        let cn_c = Arc::clone(&clean_notify);
        let ctx_cb = ctx.clone();

        let err = do_batch_with_options(
            &ctx,
            WRITE,
            r.as_ref(),
            &keys,
            move |_, _| {
                let c = ctx_cb.clone();
                let st = Arc::clone(&started);
                let sn = Arc::clone(&started_notify);
                async move {
                    if st.fetch_add(1, Ordering::AcqRel) + 1 >= NUM_INSTANCES {
                        sn.notify_waiters();
                    }
                    c.done().await;
                    Ok(())
                }
            },
            DoBatchOptions {
                cleanup: Some(Arc::new(move || {
                    cl_c.store(true, Ordering::Release);
                    cn_c.notify_waiters();
                })),
                is_client_error: None,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "cancel cause");
        while !cleaned.load(Ordering::Acquire) {
            clean_notify.notified().await;
        }
    }
}

#[derive(Debug)]
struct MockError {
    is_client_err: bool,
    message: &'static str,
}

impl std::fmt::Display for MockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}

impl std::error::Error for MockError {}

fn httpgrpc_err(code: i32, msg: &str) -> BoxDynError {
    let resp = HttpResponse {
        code,
        headers: Vec::new(),
        body: msg.as_bytes().to_vec().into(),
    };
    Arc::new(status_with_http_response(&resp, msg))
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestDoBatch_QuorumError
#[tokio::test]
async fn test_do_batch_quorum_error() {
    const NUM_OPS: usize = 200;
    const RF: usize = 3;

    let gen = RandomTokenGenerator::with_seed(99);
    let mut desc = Desc::default();
    for addr in 0..RF {
        let toks = gen.generate_tokens(128, &[]);
        let id = addr.to_string();
        desc.add_ingester(&id, &id, "", toks, InstanceState::Active, Some(SystemTime::now()), false, None, HashMap::new());
    }
    let ring = Ring::with_optional_store_and_strategy(
        make_settings(Duration::from_secs(3600), RF, false),
        "ingester",
        "ring",
        None,
        Arc::new(DefaultReplicationStrategy),
        None,
    )
    .unwrap();
    ring.update_ring_state(desc);
    let keys = [1u32, 10, 100];

    let is_mock_client = Arc::new(|e: &(dyn std::error::Error + 'static)| -> bool {
        e.downcast_ref::<MockError>().is_some_and(|m| m.is_client_err)
    });
    let is_http_4xx = Arc::new(is_http_status_4xx);

    let http_429 = httpgrpc_err(429, "Throttling");
    let http_500 = httpgrpc_err(500, "InternalServerError");
    let mock_client: BoxDynError = Arc::new(MockError { is_client_err: true, message: "mock-client" });
    let mock_server: BoxDynError = Arc::new(MockError { is_client_err: false, message: "mock-server" });

    type QuorumCase<'a> = (&'a str, [Option<BoxDynError>; 3], Vec<&'a str>, Arc<dyn Fn(&(dyn std::error::Error + 'static)) -> bool + Send + Sync>, Vec<String>);
    let cases: Vec<QuorumCase<'_>> = vec![
        ("no error", [None, None, None], vec![], is_http_4xx.clone(), vec![]),
        ("1 http500 -> ok", [Some(http_500.clone()), None, None], vec![], is_http_4xx.clone(), vec![]),
        ("1 mock client -> ok", [Some(mock_client.clone()), None, None], vec![], is_http_4xx.clone(), vec![]),
        ("2 4xx + 1 5xx -> 4xx", [Some(http_429.clone()), Some(http_500.clone()), Some(http_429.clone())], vec![], is_http_4xx.clone(), vec![http_429.to_string()]),
        ("2 5xx + 1 4xx -> 5xx", [Some(http_500.clone()), Some(http_429.clone()), Some(http_500.clone())], vec![], is_http_4xx.clone(), vec![http_500.to_string()]),
        ("1 4xx + 1 5xx + 1 ok", [Some(http_429.clone()), Some(http_500.clone()), None], vec![], is_http_4xx.clone(), vec![http_429.to_string(), http_500.to_string()]),
        ("1 500 + 1 unhealthy", [Some(http_500.clone()), None, None], vec!["2"], is_http_4xx, vec![http_500.to_string()]),
        ("2 mock client + 1 server -> client", [Some(mock_client.clone()), Some(mock_client.clone()), Some(mock_server.clone())], vec![], is_mock_client.clone(), vec![mock_client.to_string()]),
        ("2 mock server + 1 client -> server", [Some(mock_server.clone()), Some(mock_client.clone()), Some(mock_server.clone())], vec![], is_mock_client.clone(), vec![mock_server.to_string()]),
        ("1 client + 1 unhealthy", [Some(mock_client.clone()), None, None], vec!["2"], is_mock_client.clone(), vec![mock_client.to_string()]),
        ("1 server + 1 unhealthy", [Some(mock_server.clone()), None, None], vec!["2"], is_mock_client.clone(), vec![mock_server.to_string()]),
        ("2 http429 + 1 mock client w/ isMockClient", [Some(http_429.clone()), Some(http_429.clone()), Some(mock_client)], vec![], is_mock_client, vec![http_429.to_string()]),
    ];

    for (name, errs, unavail, ice, acceptable) in cases {
        let mut d = ring.desc_snapshot();
        for id in &unavail {
            let inst = d.ingesters.get_mut(*id).unwrap();
            inst.state = InstanceState::Left as i32;
            inst.timestamp = system_time_to_unix_secs(SystemTime::now());
        }
        ring.update_ring_state(d);

        for _ in 0..NUM_OPS {
            let errs_c = errs.clone();
            let res = do_batch_with_options(
                &RingContext::background(),
                WRITE,
                ring.as_ref(),
                &keys,
                move |i, _| {
                    let e = errs_c[i.addr.parse::<usize>().unwrap()].clone();
                    async move {
                        match e {
                            Some(err) => Err(err),
                            None => Ok(()),
                        }
                    }
                },
                DoBatchOptions {
                    cleanup: None,
                    is_client_error: Some(ice.clone()),
                },
            )
            .await;

            if acceptable.is_empty() {
                assert!(res.is_ok(), "{name}: expected Ok, got {res:?}");
            } else {
                let msg = res.unwrap_err().to_string();
                assert!(acceptable.contains(&msg), "{name}: got {msg}");
            }
        }

        let mut d = ring.desc_snapshot();
        for id in &unavail {
            let inst = d.ingesters.get_mut(*id).unwrap();
            inst.state = InstanceState::Active as i32;
            inst.timestamp = system_time_to_unix_secs(SystemTime::now());
        }
        ring.update_ring_state(d);
    }
}

// ============================================================================
// Ring model & core lookup tests (11)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestAddIngester
#[test]
fn test_add_ingester() {
    let mut r = Desc::default();
    let now = SystemTime::now();
    let toks = RandomTokenGenerator::with_seed(1).generate_tokens(128, &[]);
    let versions = HashMap::from([(1u64, 2u64), (3, 5)]);
    r.add_ingester("ing1", "addr", "1", toks.clone(), InstanceState::Active, Some(now), false, None, versions.clone());

    let ing = &r.ingesters["ing1"];
    assert_eq!(ing.addr, "addr");
    assert_eq!(ing.tokens, toks);
    assert!((ing.timestamp - system_time_to_unix_secs(SystemTime::now())).abs() <= 2);
    assert_eq!(ing.registered_timestamp, system_time_to_unix_secs(now));
    assert!(!ing.read_only);
    assert_eq!(ing.read_only_updated_timestamp, 0);
    assert_eq!(ing.versions, versions);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestAddIngesterReplacesExistingTokens
#[test]
fn test_add_ingester_replaces_existing_tokens() {
    let mut r = Desc::default();
    r.ingesters.insert(
        "ing1".into(),
        InstanceDesc {
            tokens: vec![11111, 22222, 33333],
            ..Default::default()
        },
    );
    let new_tokens = RandomTokenGenerator::with_seed(2).generate_tokens(128, &[]);
    r.add_ingester("ing1", "addr", "1", new_tokens.clone(), InstanceState::Active, Some(SystemTime::now()), false, None, HashMap::new());
    assert_eq!(r.ingesters["ing1"].tokens, new_tokens);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_Get_ZoneAwarenessWithIngesterLeaving
#[test]
fn test_ring_get_zone_awareness_with_ingester_leaving() {
    const TEST_COUNT: usize = 10_000;
    for (rf, exp_inst, exp_zones) in [(3usize, 3usize, 3usize), (2, 2, 2)] {
        let gen = RandomTokenGenerator::with_seed(10);
        let mut r = Desc::default();
        let defs = [
            ("instance-1", "127.0.0.1", "zone-a", InstanceState::Active),
            ("instance-2", "127.0.0.2", "zone-a", InstanceState::Active),
            ("instance-3", "127.0.0.3", "zone-b", InstanceState::Active),
            ("instance-4", "127.0.0.4", "zone-b", InstanceState::Active),
            ("instance-5", "127.0.0.5", "zone-c", InstanceState::Leaving),
            ("instance-6", "127.0.0.6", "zone-c", InstanceState::Active),
        ];
        let mut prev = Vec::new();
        for (id, addr, zone, st) in defs {
            let toks = gen.generate_tokens(128, &prev);
            prev.extend_from_slice(&toks);
            r.add_ingester(id, addr, zone, toks, st, Some(SystemTime::now()), false, None, HashMap::new());
        }
        let ring = Ring::new_for_testing(
            make_settings(Duration::from_secs(3600), rf, true),
            Arc::new(DefaultReplicationStrategy),
        );
        ring.set_ring_state_from_desc(r, false, false, false);

        for val in gen.generate_tokens(TEST_COUNT, &[]) {
            let set = ring.get(val, WRITE).unwrap();
            let distinct_zones: HashSet<&str> = set.instances.iter().map(|i| i.zone.as_str()).collect();
            assert_eq!(set.instances.len(), exp_inst);
            assert_eq!(distinct_zones.len(), exp_zones);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_Get_ZoneAwareness
#[test]
fn test_ring_get_zone_awareness() {
    const TEST_COUNT: usize = 10_000;
    let cases = [
        (16usize, 3usize, 3usize, true, None, 3usize),
        (16, 1, 3, true, Some("at least 2 live replicas required across different availability zones, could only find 1"), 0),
        (16, 2, 3, true, None, 2),
        (16, 1, 3, false, None, 3),
    ];

    for (num_inst, num_zones, rf, za, exp_err, exp_inst) in cases {
        let gen = RandomTokenGenerator::with_seed(11);
        let mut r = Desc::default();
        let mut prev = Vec::new();
        for i in 0..num_inst {
            let toks = gen.generate_tokens(128, &prev);
            prev.extend_from_slice(&toks);
            r.add_ingester(
                &format!("ing{i}"),
                &format!("127.0.0.{i}"),
                &format!("zone-{}", i % num_zones),
                toks,
                InstanceState::Active,
                Some(SystemTime::now()),
                false,
                None,
                HashMap::new(),
            );
        }
        let ring = Ring::new_for_testing(
            make_settings(Duration::from_secs(3600), rf, za),
            Arc::new(DefaultReplicationStrategy),
        );
        ring.set_ring_state_from_desc(r, false, false, false);

        for val in gen.generate_tokens(TEST_COUNT, &[]) {
            let res = ring.get(val, WRITE);
            if let Some(msg) = exp_err {
                assert_eq!(res.unwrap_err().to_string(), msg);
            } else {
                let set = res.unwrap();
                assert_eq!(set.instances.len(), exp_inst);
                if za {
                    let zones: HashSet<&str> = set.instances.iter().map(|i| i.zone.as_str()).collect();
                    assert_eq!(zones.len(), set.instances.len());
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_GetWithOptions
#[test]
fn test_ring_get_with_options() {
    const TEST_COUNT: usize = 10_000;
    let now = SystemTime::now();
    let healthy_ts = system_time_to_unix_secs(now);
    let unhealthy_ts = healthy_ts - 120;

    let make_6 = |unhealthy_cnt: usize| -> HashMap<String, InstanceDesc> {
        let zones = ["zone-a", "zone-a", "zone-b", "zone-b", "zone-c", "zone-c"];
        (0..6)
            .map(|i| {
                (
                    format!("instance-{}", i + 1),
                    InstanceDesc {
                        addr: format!("127.0.0.{}", i + 1),
                        zone: zones[i].into(),
                        state: InstanceState::Active as i32,
                        timestamp: if i < unhealthy_cnt { unhealthy_ts } else { healthy_ts },
                        ..Default::default()
                    },
                )
            })
            .collect()
    };
    let make_9 = || -> HashMap<String, InstanceDesc> {
        let zones = ["zone-a", "zone-a", "zone-a", "zone-b", "zone-b", "zone-b", "zone-c", "zone-c", "zone-c"];
        (0..9)
            .map(|i| {
                (
                    format!("instance-{}", i + 1),
                    InstanceDesc {
                        addr: format!("127.0.0.{}", i + 1),
                        zone: zones[i].into(),
                        state: InstanceState::Active as i32,
                        timestamp: healthy_ts,
                        ..Default::default()
                    },
                )
            })
            .collect()
    };

    let def = || Arc::new(DefaultReplicationStrategy) as Arc<dyn ReplicationStrategy>;
    let ign = || Arc::new(IgnoreUnhealthyInstancesReplicationStrategy) as Arc<dyn ReplicationStrategy>;

    type GetOptCase<'a> = (&'a str, HashMap<String, InstanceDesc>, Arc<dyn ReplicationStrategy>, usize, usize, bool);
    let real_cases: Vec<GetOptCase<'_>> = vec![
        ("no options default", make_6(0), def(), 0, 3, false),
        ("lower rf default", make_6(0), def(), 1, 3, false),
        ("higher rf default", make_6(0), def(), 6, 0, true),
        ("higher rf default some unhealthy", make_6(2), def(), 6, 0, true),
        ("higher rf default most unhealthy", make_6(4), def(), 6, 0, true),
        ("slightly higher rf ignore unhealthy", make_9(), ign(), 6, 6, false),
        ("higher rf ignore unhealthy all healthy", make_6(0), ign(), 6, 6, false),
        ("higher rf ignore unhealthy some unhealthy", make_6(2), ign(), 6, 4, false),
        ("higher rf ignore unhealthy most unhealthy", make_6(4), ign(), 6, 2, false),
        ("higher rf ignore unhealthy single healthy", make_6(5), ign(), 6, 1, false),
        ("higher rf ignore unhealthy all unhealthy", make_6(6), ign(), 6, 0, true),
    ];

    for (name, ing_map, strategy, rf_opt, exp_size, exp_err) in real_cases {
        for za in [false, true] {
            let gen = RandomTokenGenerator::with_seed(12);
            let mut prev = Vec::new();
            let mut r = Desc::default();
            let mut ids: Vec<String> = ing_map.keys().cloned().collect();
            ids.sort();
            for id in ids {
                let mut desc = ing_map[&id].clone();
                desc.tokens = gen.generate_tokens(128, &prev);
                prev.extend_from_slice(&desc.tokens);
                r.ingesters.insert(id, desc);
            }
            let ring = Ring::new_for_testing(
                make_settings(Duration::from_secs(60), 3, za),
                Arc::clone(&strategy),
            );
            ring.set_ring_state_from_desc(r, false, false, false);

            for val in gen.generate_tokens(TEST_COUNT, &[]) {
                let res = ring.get_with_options(val, WRITE, GetOptions { replication_factor: rf_opt });
                if exp_err {
                    assert!(res.is_err(), "{name} za={za}");
                } else {
                    assert_eq!(res.unwrap().instances.len(), exp_size, "{name} za={za}");
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_GetAllHealthy
#[test]
fn test_ring_get_all_healthy() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let ring_empty = Ring::new_for_testing(make_settings(Duration::from_secs(60), 0, false), Arc::new(DefaultReplicationStrategy));
    ring_empty.set_ring_state_from_desc(Desc::default(), false, false, false);
    assert!(matches!(ring_empty.get_all_healthy_at(READ, now), Err(RingError::EmptyRing)));
    assert!(matches!(ring_empty.get_all_healthy_at(WRITE, now), Err(RingError::EmptyRing)));
    assert!(matches!(ring_empty.get_all_healthy_at(REPORTING, now), Err(RingError::EmptyRing)));

    let desc = Desc {
        ingesters: HashMap::from([
            ("instance-1".into(), InstanceDesc { addr: "127.0.0.1".into(), state: InstanceState::Active as i32, timestamp: now_s, ..Default::default() }),
            ("instance-2".into(), InstanceDesc { addr: "127.0.0.2".into(), state: InstanceState::Pending as i32, timestamp: now_s - 10, ..Default::default() }),
            ("instance-3".into(), InstanceDesc { addr: "127.0.0.3".into(), state: InstanceState::Joining as i32, timestamp: now_s - 20, ..Default::default() }),
            ("instance-4".into(), InstanceDesc { addr: "127.0.0.4".into(), state: InstanceState::Leaving as i32, timestamp: now_s - 30, ..Default::default() }),
            ("instance-5".into(), InstanceDesc { addr: "127.0.0.5".into(), state: InstanceState::Active as i32, timestamp: now_s - 120, ..Default::default() }),
        ]),
    };
    let ring = Ring::new_for_testing(make_settings(Duration::from_secs(60), 0, false), Arc::new(DefaultReplicationStrategy));
    ring.set_ring_state_from_desc(desc, false, false, false);

    let mut r_addrs = ring.get_all_healthy_at(READ, now).unwrap().get_addresses();
    r_addrs.sort();
    assert_eq!(r_addrs, vec!["127.0.0.1", "127.0.0.2", "127.0.0.4"]);

    assert_eq!(ring.get_all_healthy_at(WRITE, now).unwrap().get_addresses(), vec!["127.0.0.1"]);

    let mut rep_addrs = ring.get_all_healthy_at(REPORTING, now).unwrap().get_addresses();
    rep_addrs.sort();
    assert_eq!(rep_addrs, vec!["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"]);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_GetReplicationSetForOperation
#[test]
fn test_ring_get_replication_set_for_operation() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let gen = RandomTokenGenerator::with_seed(13);

    // empty ring
    let empty_ring = Ring::new_for_testing(make_settings(Duration::from_secs(60), 1, false), Arc::new(DefaultReplicationStrategy));
    empty_ring.set_ring_state_from_desc(Desc::default(), false, false, false);
    assert!(matches!(empty_ring.get_replication_set_for_operation_at(READ, now), Err(RingError::EmptyRing)));
    assert!(matches!(empty_ring.get_replication_set_for_operation_at(WRITE, now), Err(RingError::EmptyRing)));
    assert!(matches!(empty_ring.get_replication_set_for_operation_at(REPORTING, now), Err(RingError::EmptyRing)));

    let make_5 = |offsets: [i64; 5]| -> Desc {
        Desc {
            ingesters: (0..5)
                .map(|i| {
                    (
                        format!("instance-{}", i + 1),
                        InstanceDesc {
                            addr: format!("127.0.0.{}", i + 1),
                            state: InstanceState::Active as i32,
                            timestamp: now_s - offsets[i],
                            tokens: gen.generate_tokens(128, &[]),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
        }
    };

    // all healthy RF=1
    let r1 = Ring::new_for_testing(make_settings(Duration::from_secs(60), 1, false), Arc::new(DefaultReplicationStrategy));
    r1.set_ring_state_from_desc(make_5([0, 10, 20, 30, 40]), false, false, false);
    for op in [READ, WRITE, REPORTING] {
        let rs = r1.get_replication_set_for_operation_at(op, now).unwrap();
        assert_eq!(rs.partition_id, -1);
        assert_eq!(rs.instances.len(), 5);
    }

    // old timestamps, 1h timeout
    let r2 = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, false), Arc::new(DefaultReplicationStrategy));
    r2.set_ring_state_from_desc(make_5([120, 120, 120, 120, 120]), false, false, false);
    for op in [READ, WRITE, REPORTING] {
        assert_eq!(r2.get_replication_set_for_operation_at(op, now).unwrap().instances.len(), 5);
    }

    // 1 unhealthy RF=1 -> fail
    let r3 = Ring::new_for_testing(make_settings(Duration::from_secs(60), 1, false), Arc::new(DefaultReplicationStrategy));
    r3.set_ring_state_from_desc(make_5([0, 10, 20, 30, 120]), false, false, false);
    for op in [READ, WRITE, REPORTING] {
        assert!(matches!(r3.get_replication_set_for_operation_at(op, now), Err(RingError::TooManyUnhealthyInstances)));
    }

    // 1 unhealthy RF=3 -> succeed
    let r4 = Ring::new_for_testing(make_settings(Duration::from_secs(60), 3, false), Arc::new(DefaultReplicationStrategy));
    r4.set_ring_state_from_desc(make_5([0, 10, 20, 30, 120]), false, false, false);
    for op in [READ, WRITE, REPORTING] {
        let rs = r4.get_replication_set_for_operation_at(op, now).unwrap();
        assert_eq!(rs.instances.len(), 4);
    }

    // 2 unhealthy RF=3 -> fail
    let r5 = Ring::new_for_testing(make_settings(Duration::from_secs(60), 3, false), Arc::new(DefaultReplicationStrategy));
    r5.set_ring_state_from_desc(make_5([0, 10, 20, 120, 120]), false, false, false);
    for op in [READ, WRITE, REPORTING] {
        assert!(matches!(r5.get_replication_set_for_operation_at(op, now), Err(RingError::TooManyUnhealthyInstances)));
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_GetReplicationSetForOperation_WithZoneAwarenessEnabled
#[test]
fn test_ring_get_replication_set_for_operation_with_zone_awareness_enabled() {
    let now = SystemTime::now();
    let now_s = system_time_to_unix_secs(now);
    let gen = RandomTokenGenerator::with_seed(14);

    let insts = |zones: &[&str]| -> HashMap<String, InstanceDesc> {
        zones
            .iter()
            .enumerate()
            .map(|(i, z)| {
                (
                    format!("instance-{}", i + 1),
                    InstanceDesc {
                        addr: format!("127.0.0.{}", i + 1),
                        zone: (*z).to_string(),
                        tokens: gen.generate_tokens(128, &[]),
                        state: InstanceState::Active as i32,
                        timestamp: now_s,
                        ..Default::default()
                    },
                )
            })
            .collect()
    };

    type ZoneOpCase<'a> = (&'a str, Vec<&'a str>, Vec<&'a str>, usize, Option<&'a [&'a str]>, usize, bool);
    let cases: Vec<ZoneOpCase<'_>> = vec![
        ("empty ring", vec![], vec![], 1, None, 0, true),
        ("RF=1 1 zone", vec!["zone-a", "zone-a"], vec![], 1, Some(&["127.0.0.1", "127.0.0.2"]), 0, false),
        ("RF=1 1 zone 1 unavail", vec!["zone-a", "zone-a", "zone-a"], vec!["instance-2"], 1, None, 0, true),
        ("RF=1 3 zones 1 unavail", vec!["zone-a", "zone-b", "zone-c"], vec!["instance-3"], 1, None, 0, true),
        ("RF=2 2 zones", vec!["zone-a", "zone-b"], vec![], 2, Some(&["127.0.0.1", "127.0.0.2"]), 1, false),
        ("RF=2 2 zones 1 unavail", vec!["zone-a", "zone-b"], vec!["instance-2"], 2, Some(&["127.0.0.1"]), 0, false),
        ("RF=3 3 zones", vec!["zone-a", "zone-b", "zone-c"], vec![], 3, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.3"]), 1, false),
        ("RF=3 3 zones 1 unavail", vec!["zone-a", "zone-b", "zone-c"], vec!["instance-1"], 3, Some(&["127.0.0.2", "127.0.0.3"]), 0, false),
        ("RF=3 3 zones 2 unavail", vec!["zone-a", "zone-b", "zone-c"], vec!["instance-1", "instance-2"], 3, None, 0, true),
        ("RF=3 3 zones all unavail", vec!["zone-a", "zone-b", "zone-c"], vec!["instance-1", "instance-2", "instance-3"], 3, None, 0, true),
        ("RF=3 3 zones 2/zone", vec!["zone-a", "zone-a", "zone-b", "zone-b", "zone-c", "zone-c"], vec![], 3, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6"]), 1, false),
        ("RF=3 3 zones 2/zone 2 unavail same zone", vec!["zone-a", "zone-a", "zone-b", "zone-b", "zone-c", "zone-c"], vec!["instance-3", "instance-4"], 3, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.5", "127.0.0.6"]), 0, false),
        ("RF=3 3 zones 3/zone 2 unavail same zone", vec!["zone-a", "zone-a", "zone-a", "zone-b", "zone-b", "zone-b", "zone-c", "zone-c", "zone-c"], vec!["instance-4", "instance-6"], 3, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.7", "127.0.0.8", "127.0.0.9"]), 0, false),
        ("RF=3 2 zones", vec!["zone-a", "zone-a", "zone-b", "zone-b"], vec![], 3, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"]), 1, false),
        ("RF=3 2 zones 1 unavail", vec!["zone-a", "zone-a", "zone-b", "zone-b"], vec!["instance-4"], 3, Some(&["127.0.0.1", "127.0.0.2"]), 0, false),
        ("RF=3 1 zone", vec!["zone-a", "zone-a"], vec![], 3, Some(&["127.0.0.1", "127.0.0.2"]), 0, false),
        ("RF=3 1 zone 1 unavail", vec!["zone-a", "zone-a"], vec!["instance-2"], 3, None, 0, true),
        ("RF=5 5 zones", vec!["zone-a", "zone-a", "zone-b", "zone-b", "zone-c", "zone-c", "zone-d", "zone-d", "zone-e", "zone-e", "zone-e"], vec![], 5, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.11"]), 2, false),
        ("RF=5 5 zones 2 unavail same zone", vec!["zone-a", "zone-a", "zone-b", "zone-b", "zone-c", "zone-c", "zone-d", "zone-d", "zone-e", "zone-e", "zone-e"], vec!["instance-3", "instance-4"], 5, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.11"]), 1, false),
        ("RF=5 5 zones 2 unavail diff zones", vec!["zone-a", "zone-a", "zone-b", "zone-b", "zone-c", "zone-c", "zone-d", "zone-d", "zone-e", "zone-e", "zone-e"], vec!["instance-3", "instance-5"], 5, Some(&["127.0.0.1", "127.0.0.2", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.11"]), 0, false),
        ("RF=5 5 zones 3 unavail", vec!["zone-a", "zone-b", "zone-c", "zone-d", "zone-e"], vec!["instance-2", "instance-4", "instance-5"], 5, None, 0, true),
    ];

    for (name, zones, unavail, rf, exp_addrs, exp_unavail_zones, is_err) in cases {
        let mut map = insts(&zones);
        for id in unavail {
            map.get_mut(id).unwrap().timestamp = now_s - 3600;
        }
        let ring = Ring::new_for_testing(
            make_settings(Duration::from_secs(60), rf, true),
            Arc::new(DefaultReplicationStrategy),
        );
        ring.set_ring_state_from_desc(Desc { ingesters: map }, false, false, false);
        let res = ring.get_replication_set_for_operation_at(READ, now);
        if is_err {
            assert!(res.is_err(), "{name}");
        } else {
            let rs = res.unwrap();
            assert_eq!(rs.max_errors, 0, "{name}");
            assert_eq!(rs.max_unavailable_zones, exp_unavail_zones, "{name}");
            assert!(rs.zone_awareness_enabled, "{name}");
            assert_eq!(rs.partition_id, -1, "{name}");
            let mut got = rs.get_addresses();
            got.sort();
            let mut exp: Vec<String> = exp_addrs.unwrap().iter().map(|s| (*s).to_string()).collect();
            exp.sort();
            assert_eq!(got, exp, "{name}");
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_GetSubringForOperationStates
#[test]
fn test_ring_get_subring_for_operation_states() {
    let cases = [
        (
            vec![("i1", InstanceState::Active), ("i2", InstanceState::Active), ("i3", InstanceState::Active)],
            WRITE,
            vec!["i1", "i2", "i3"],
        ),
        (
            vec![("i1", InstanceState::Active), ("i2", InstanceState::Leaving), ("i3", InstanceState::Pending)],
            READ,
            vec!["i1", "i2", "i3"],
        ),
        (
            vec![("i1", InstanceState::Active), ("i2", InstanceState::Left), ("i3", InstanceState::Pending)],
            WRITE,
            vec!["i1"],
        ),
        (
            vec![("i1", InstanceState::Left), ("i3", InstanceState::Left)],
            WRITE,
            vec![],
        ),
        (vec![], WRITE, vec![]),
    ];

    for (input, op, exp_ids) in cases {
        let desc = Desc {
            ingesters: input
                .into_iter()
                .map(|(id, st)| (id.to_string(), InstanceDesc { state: st as i32, ..Default::default() }))
                .collect(),
        };
        let ring = Ring::new_for_testing(make_settings(Duration::from_secs(60), 1, false), Arc::new(DefaultReplicationStrategy));
        ring.set_ring_state_from_desc(desc, false, false, false);
        let sub = ring.get_subring_for_operation_states(op);
        let mut got: Vec<String> = sub.desc_snapshot().ingesters.keys().cloned().collect();
        got.sort();
        let mut exp: Vec<String> = exp_ids.into_iter().map(String::from).collect();
        exp.sort();
        assert_eq!(got, exp);
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_GetInstancesWithTokensCounts
#[test]
fn test_ring_get_instances_with_tokens_counts() {
    let gen = RandomTokenGenerator::with_seed(15);
    let now_s = system_time_to_unix_secs(SystemTime::now());
    let mk = |zone: &str, st: InstanceState, has_toks: bool| InstanceDesc {
        addr: "127.0.0.1".into(),
        zone: zone.into(),
        state: st as i32,
        timestamp: now_s,
        tokens: if has_toks { gen.generate_tokens(128, &[]) } else { vec![] },
        ..Default::default()
    };

    let cases = [
        (vec![], 0usize, vec![]),
        (
            vec![
                ("i1", mk("zone-a", InstanceState::Active, false)),
                ("i2", mk("zone-a", InstanceState::Leaving, false)),
                ("i3", mk("zone-a", InstanceState::Pending, false)),
                ("i4", mk("zone-a", InstanceState::Joining, false)),
            ],
            0,
            vec![("zone-a", 0)],
        ),
        (
            vec![
                ("i1", mk("zone-a", InstanceState::Active, true)),
                ("i2", mk("zone-a", InstanceState::Active, false)),
                ("i3", mk("zone-a", InstanceState::Leaving, true)),
                ("i4", mk("zone-a", InstanceState::Leaving, false)),
                ("i5", mk("zone-a", InstanceState::Pending, true)),
                ("i6", mk("zone-a", InstanceState::Pending, false)),
                ("i7", mk("zone-a", InstanceState::Joining, true)),
                ("i8", mk("zone-a", InstanceState::Joining, false)),
            ],
            4,
            vec![("zone-a", 4)],
        ),
        (
            vec![
                ("i1", mk("zone-a", InstanceState::Active, true)),
                ("i2", mk("zone-a", InstanceState::Active, false)),
                ("i3", mk("zone-b", InstanceState::Leaving, true)),
                ("i4", mk("zone-b", InstanceState::Leaving, false)),
                ("i5", mk("zone-c", InstanceState::Pending, true)),
                ("i6", mk("zone-d", InstanceState::Pending, false)),
                ("i7", mk("zone-c", InstanceState::Joining, true)),
                ("i8", mk("zone-d", InstanceState::Joining, false)),
            ],
            4,
            vec![("zone-a", 1), ("zone-b", 1), ("zone-c", 2), ("zone-d", 0)],
        ),
    ];

    for (items, exp_total, exp_zones) in cases {
        let desc = Desc {
            ingesters: items.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        };
        let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
        ring.set_ring_state_from_desc(desc, false, false, false);
        assert_eq!(ring.instances_with_tokens_count(), exp_total);
        for (z, cnt) in exp_zones {
            assert_eq!(ring.instances_with_tokens_in_zone_count(z), cnt);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_GetWritableInstancesWithTokensCounts
#[test]
fn test_ring_get_writable_instances_with_tokens_counts() {
    let gen = RandomTokenGenerator::with_seed(16);
    let now_s = system_time_to_unix_secs(SystemTime::now());
    let mk = |zone: &str, st: InstanceState, has_toks: bool, ro: bool| InstanceDesc {
        addr: "127.0.0.1".into(),
        zone: zone.into(),
        state: st as i32,
        timestamp: now_s,
        tokens: if has_toks { gen.generate_tokens(128, &[]) } else { vec![] },
        read_only: ro,
        ..Default::default()
    };

    let cases = [
        (vec![], 0usize, vec![]),
        (
            vec![
                ("i1", mk("zone-a", InstanceState::Active, false, false)),
                ("i2", mk("zone-a", InstanceState::Leaving, false, true)),
                ("i3", mk("zone-a", InstanceState::Pending, false, false)),
                ("i4", mk("zone-a", InstanceState::Joining, false, false)),
            ],
            0,
            vec![("zone-a", 0)],
        ),
        (
            vec![
                ("i1", mk("zone-a", InstanceState::Active, true, false)),
                ("i2", mk("zone-a", InstanceState::Active, false, false)),
                ("i3", mk("zone-a", InstanceState::Leaving, true, false)),
                ("i4", mk("zone-a", InstanceState::Leaving, false, false)),
                ("i5", mk("zone-a", InstanceState::Pending, true, false)),
                ("i6", mk("zone-a", InstanceState::Pending, false, true)),
                ("i7", mk("zone-a", InstanceState::Joining, true, true)),
                ("i8", mk("zone-a", InstanceState::Joining, false, true)),
            ],
            3,
            vec![("zone-a", 3)],
        ),
        (
            vec![
                ("i1", mk("zone-a", InstanceState::Active, true, false)),
                ("i2", mk("zone-a", InstanceState::Active, false, true)),
                ("i3", mk("zone-b", InstanceState::Leaving, true, true)),
                ("i4", mk("zone-b", InstanceState::Leaving, false, false)),
                ("i5", mk("zone-c", InstanceState::Pending, true, false)),
                ("i6", mk("zone-d", InstanceState::Pending, false, false)),
                ("i7", mk("zone-c", InstanceState::Joining, true, false)),
                ("i8", mk("zone-d", InstanceState::Joining, false, true)),
            ],
            3,
            vec![("zone-a", 1), ("zone-b", 0), ("zone-c", 2), ("zone-d", 0)],
        ),
    ];

    for (items, exp_total, exp_zones) in cases {
        let desc = Desc {
            ingesters: items.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        };
        let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
        ring.set_ring_state_from_desc(desc, false, false, false);
        assert_eq!(ring.writable_instances_with_tokens_count(), exp_total);
        for (z, cnt) in exp_zones {
            assert_eq!(ring.writable_instances_with_tokens_in_zone_count(z), cnt);
        }
    }
}

// ============================================================================
// Shuffle shard tests (13)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShard
#[test]
fn test_ring_shuffle_shard() {
    let gen = RandomTokenGenerator::with_seed(17);
    let now_s = system_time_to_unix_secs(SystemTime::now());
    let mk = |id: usize, zone: &str, ro: bool| -> (String, InstanceDesc) {
        (
            format!("instance-{id}"),
            InstanceDesc {
                addr: format!("127.0.0.{id}"),
                zone: zone.into(),
                state: InstanceState::Active as i32,
                timestamp: now_s,
                read_only: ro,
                tokens: gen.generate_tokens(128, &[]),
                ..Default::default()
            },
        )
    };

    type ShuffleCase<'a> = (&'a str, Vec<(String, InstanceDesc)>, usize, bool, usize, Vec<usize>, usize, Vec<(&'a str, usize)>);
    let cases: Vec<ShuffleCase<'_>> = vec![
        ("empty ring", vec![], 2, true, 0, vec![], 0, vec![]),
        ("empty ring shardSize=0", vec![], 0, true, 0, vec![], 0, vec![]),
        ("single zone shard > inst", vec![mk(1, "zone-a", false), mk(2, "zone-a", false)], 3, true, 2, vec![2], 1, vec![("zone-a", 2)]),
        ("single zone shard < inst", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-a", false)], 2, true, 2, vec![2], 1, vec![("zone-a", 2)]),
        ("single zone read only shard=3", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-a", true)], 3, true, 2, vec![2], 1, vec![("zone-a", 2)]),
        ("single zone read only shard=0", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-a", true)], 0, true, 2, vec![2], 1, vec![("zone-a", 2)]),
        ("multi zone shard < zones", vec![mk(1, "zone-a", false), mk(2, "zone-b", false), mk(3, "zone-c", false)], 2, true, 3, vec![1, 1, 1], 3, vec![("zone-a", 1), ("zone-b", 1), ("zone-c", 1)]),
        ("multi zone shard divisible", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-b", false), mk(4, "zone-b", false), mk(5, "zone-c", false), mk(6, "zone-c", false)], 3, true, 3, vec![1, 1, 1], 3, vec![("zone-a", 1), ("zone-b", 1), ("zone-c", 1)]),
        ("multi zone shard not divisible", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-b", false), mk(4, "zone-b", false), mk(5, "zone-c", false), mk(6, "zone-c", false)], 4, true, 6, vec![2, 2, 2], 3, vec![("zone-a", 2), ("zone-b", 2), ("zone-c", 2)]),
        ("multi zone shard not divisible no za", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-b", false), mk(4, "zone-b", false), mk(5, "zone-c", false), mk(6, "zone-c", false)], 4, false, 4, vec![], 0, vec![]),
        ("multi zone ro shard=3", vec![mk(1, "zone-a", false), mk(2, "zone-b", false), mk(3, "zone-c", true)], 3, true, 2, vec![1, 1], 2, vec![("zone-a", 1), ("zone-b", 1), ("zone-c", 0)]),
        ("multi zone ro shard=0", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-b", true), mk(4, "zone-b", false), mk(5, "zone-c", false), mk(6, "zone-c", true)], 0, true, 4, vec![2, 1, 1], 3, vec![("zone-a", 2), ("zone-b", 1), ("zone-c", 1)]),
        ("multi zone unbalanced shard=6", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-a", false), mk(4, "zone-b", false), mk(5, "zone-b", false), mk(6, "zone-c", false)], 6, true, 5, vec![2, 2, 1], 3, vec![("zone-a", 2), ("zone-b", 2), ("zone-c", 1)]),
        ("multi zone unbalanced shard=9", vec![mk(1, "zone-a", false), mk(2, "zone-a", false), mk(3, "zone-a", false), mk(4, "zone-b", false), mk(5, "zone-b", false), mk(6, "zone-c", false)], 9, true, 6, vec![3, 2, 1], 3, vec![("zone-a", 3), ("zone-b", 2), ("zone-c", 1)]),
    ];

    for (name, items, shard_size, za, exp_size, exp_dist, exp_z_cnt, exp_per_z) in cases {
        for update_ro in [false, true] {
            let desc = Desc {
                ingesters: items.iter().cloned().collect(),
            };
            let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, za), Arc::new(DefaultReplicationStrategy));
            ring.set_ring_state_from_desc(desc, false, false, update_ro);

            let shard_ring = ring.shuffle_shard("tenant-id", shard_size);
            assert_eq!(shard_ring.instances_count(), exp_size, "{name}");

            if za {
                assert_eq!(shard_ring.zones_count(), exp_z_cnt, "{name}");
                for &(z, cnt) in &exp_per_z {
                    assert_eq!(shard_ring.instances_in_zone_count(z), cnt, "{name} {z}");
                }
                let mut actual_dist = Vec::new();
                if shard_ring.instances_count() > 0 {
                    let all = shard_ring.get_all_healthy(READ).unwrap();
                    let mut by_z: HashMap<String, usize> = HashMap::new();
                    for inst in all.instances {
                        *by_z.entry(inst.zone).or_insert(0) += 1;
                    }
                    actual_dist = by_z.into_values().collect();
                }
                actual_dist.sort();
                let mut exp_d = exp_dist.clone();
                exp_d.sort();
                assert_eq!(actual_dist, exp_d, "{name}");
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShard_Stability
#[test]
fn test_ring_shuffle_shard_stability() {
    let gen = RandomTokenGenerator::with_seed(18);
    let desc = Desc {
        ingesters: gen_ring_instances(&gen, 50, 3, 128),
    };
    let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
    ring.set_ring_state_from_desc(desc, false, false, false);

    for i in 1..=100 {
        let tenant = i.to_string();
        for size in [3, 6, 9, 12, 15] {
            let r = ring.shuffle_shard(&tenant, size);
            let mut expected = r.get_all_healthy(READ).unwrap().get_addresses();
            expected.sort();
            for _ in 0..10 {
                let r2 = ring.shuffle_shard(&tenant, size);
                let mut actual = r2.get_all_healthy(READ).unwrap().get_addresses();
                actual.sort();
                assert_eq!(actual, expected);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShard_Shuffling
#[test]
fn test_ring_shuffle_shard_shuffling() {
    const NUM_TENANTS: usize = 1000;
    const NUM_INSTANCES: usize = 90;
    const NUM_ZONES: usize = 3;
    const SHARD_SIZE: usize = 3;
    let theoretical = [(0usize, 90.2239f64), (1, 9.55312), (2, 0.22217), (3, 0.00085)];

    let now_s = system_time_to_unix_secs(SystemTime::now());
    let mut instances = HashMap::with_capacity(NUM_INSTANCES);
    for i in 0..NUM_INSTANCES {
        let id = format!("instance-{i}");
        instances.insert(
            id.clone(),
            InstanceDesc {
                id,
                addr: format!("127.0.0.{i}"),
                timestamp: now_s,
                registered_timestamp: now_s,
                state: InstanceState::Active as i32,
                tokens: generate_tokens_linear(i, NUM_INSTANCES, 128),
                zone: format!("zone-{}", i % NUM_ZONES),
                ..Default::default()
            },
        );
    }

    let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
    ring.set_ring_state_from_desc(Desc { ingesters: instances }, false, false, false);

    let mut shards: Vec<Vec<String>> = Vec::with_capacity(NUM_TENANTS);
    for i in 1..=NUM_TENANTS {
        let r = ring.shuffle_shard(&i.to_string(), SHARD_SIZE);
        let set = r.get_all_healthy(READ).unwrap();
        shards.push(set.get_addresses());
    }

    let mut dist = [0usize; 4];
    for (ci, curr) in shards.iter().enumerate() {
        for (oi, other) in shards.iter().enumerate() {
            if ci == oi {
                continue;
            }
            let m = curr.iter().filter(|c| other.contains(c)).count();
            dist[m] += 1;
        }
    }

    let max_comb = (NUM_TENANTS * NUM_TENANTS - NUM_TENANTS) as f64;
    for (num_match, prob) in theoretical {
        let max_dev = 1.0f64.min((prob * 0.1).max(0.2));
        let actual = (dist[num_match] as f64 / max_comb) * 100.0;
        assert!(
            (actual - prob).abs() <= max_dev,
            "num_match={num_match}: actual={actual}, expected={prob} +- {max_dev}"
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShard_Consistency
#[test]
fn test_ring_shuffle_shard_consistency() {
    #[derive(Clone, Copy)]
    enum Change {
        Add,
        Remove,
        EnableRo,
        DisableRo,
    }
    const NUM_TENANTS: usize = 50;
    let mut rnd = GoRand::new(19);
    let gen = RandomTokenGenerator::with_seed(19);

    for num_instances in [20usize, 30, 50] {
        for shard_size in [3usize, 6, 9, 12] {
            for change in [Change::Add, Change::Remove, Change::EnableRo, Change::DisableRo] {
                let mut desc = Desc {
                    ingesters: gen_ring_instances(&gen, num_instances, 3, 128),
                };
                for _ in (0..desc.ingesters.len()).step_by(8) {
                    let id = get_random_instance_id(&desc.ingesters, &mut rnd);
                    let inst = desc.ingesters.get_mut(&id).unwrap();
                    inst.read_only = true;
                    inst.read_only_updated_timestamp = system_time_to_unix_secs(SystemTime::now());
                }

                let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
                ring.set_ring_state_from_desc(desc.clone(), false, false, false);

                let mut initial = Vec::with_capacity(NUM_TENANTS);
                for id in 0..NUM_TENANTS {
                    initial.push(ring.shuffle_shard(&id.to_string(), shard_size).get_all_healthy(READ).unwrap());
                }

                match change {
                    Change::Add => {
                        let (new_id, new_desc, _) = gen_ring_instance(&gen, num_instances + 1, 0, 128, &[]);
                        desc.ingesters.insert(new_id, new_desc);
                    }
                    Change::Remove => {
                        if let Some(first_id) = desc.ingesters.keys().min().cloned() {
                            desc.ingesters.remove(&first_id);
                        }
                    }
                    Change::EnableRo => {
                        let mut keys: Vec<String> = desc.ingesters.keys().cloned().collect();
                        keys.sort();
                        if let Some(k) = keys.first() {
                            let inst = desc.ingesters.get_mut(k).unwrap();
                            inst.read_only = true;
                            inst.read_only_updated_timestamp = system_time_to_unix_secs(SystemTime::now());
                        }
                    }
                    Change::DisableRo => {
                        let mut keys: Vec<String> = desc.ingesters.keys().cloned().collect();
                        keys.sort();
                        for k in keys {
                            let inst = desc.ingesters.get_mut(&k).unwrap();
                            if inst.read_only {
                                inst.read_only = false;
                                inst.read_only_updated_timestamp = system_time_to_unix_secs(SystemTime::now());
                                break;
                            }
                        }
                    }
                }

                ring.set_ring_state_from_desc(desc, false, false, false);
                for (id, init_set) in initial.iter().enumerate() {
                    let updated = ring.shuffle_shard(&id.to_string(), shard_size).get_all_healthy(READ).unwrap();
                    let (added, removed) = compare_replication_sets(init_set, &updated);
                    assert!(added.len() <= 1, "added: {added:?}");
                    assert!(removed.len() <= 1, "removed: {removed:?}");
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShard_ConsistencyOnShardSizeChanged
#[test]
fn test_ring_shuffle_shard_consistency_on_shard_size_changed() {
    let gen = RandomTokenGenerator::with_seed(20);
    let mut ring_instances = HashMap::new();
    for i in 0..30 {
        let (name, desc, _) = gen_ring_instance(&gen, i, i % 3, 128, &[]);
        ring_instances.insert(name, desc);
    }
    let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
    ring.set_ring_state_from_desc(Desc { ingesters: ring_instances }, false, false, false);

    let s1 = ring.shuffle_shard("tenant-id", 3);
    assert_eq!(s1.instances_count(), 3);
    let set1 = s1.get_all_healthy(READ).unwrap();

    let s2 = ring.shuffle_shard("tenant-id", 6);
    assert_eq!(s2.instances_count(), 6);
    let set2 = s2.get_all_healthy(READ).unwrap();
    for inst in &set1.instances {
        assert!(set2.includes(&inst.addr));
    }

    let s3 = ring.shuffle_shard("tenant-id", 9);
    assert_eq!(s3.instances_count(), 9);
    let set3 = s3.get_all_healthy(READ).unwrap();
    for inst in &set2.instances {
        assert!(set3.includes(&inst.addr));
    }

    let s4 = ring.shuffle_shard("tenant-id", 6);
    assert_eq!(s4.instances_count(), 6);
    let set4 = s4.get_all_healthy(READ).unwrap();
    for inst in &set2.instances {
        assert!(set4.includes(&inst.addr));
    }

    let s5 = ring.shuffle_shard("tenant-id", 3);
    assert_eq!(s5.instances_count(), 3);
    let set5 = s5.get_all_healthy(READ).unwrap();
    for inst in &set1.instances {
        assert!(set5.includes(&inst.addr));
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShard_ConsistencyOnZonesChanged
#[test]
fn test_ring_shuffle_shard_consistency_on_zones_changed() {
    let gen = RandomTokenGenerator::with_seed(21);
    let mut ring_instances = HashMap::new();
    for i in 0..20 {
        let (name, desc, _) = gen_ring_instance(&gen, i, i % 2, 128, &[]);
        ring_instances.insert(name, desc);
    }
    let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
    ring.set_ring_state_from_desc(Desc { ingesters: ring_instances.clone() }, false, false, false);

    let s1 = ring.shuffle_shard("tenant-id", 2);
    assert_eq!(s1.instances_count(), 2);
    let set1 = s1.get_all_healthy(READ).unwrap();

    let s2 = ring.shuffle_shard("tenant-id", 4);
    assert_eq!(s2.instances_count(), 4);
    let set2 = s2.get_all_healthy(READ).unwrap();
    for inst in &set1.instances {
        assert!(set2.includes(&inst.addr));
    }

    for i in 20..30 {
        let (name, desc, _) = gen_ring_instance(&gen, i, 2, 128, &[]);
        ring_instances.insert(name, desc);
    }
    ring.set_ring_state_from_desc(Desc { ingesters: ring_instances }, false, false, false);

    let s3 = ring.shuffle_shard("tenant-id", 6);
    assert_eq!(s3.instances_count(), 6);
    let set3 = s3.get_all_healthy(READ).unwrap();
    for inst in &set2.instances {
        assert!(set3.includes(&inst.addr));
    }

    let s4 = ring.shuffle_shard("tenant-id", 9);
    assert_eq!(s4.instances_count(), 9);
    let set4 = s4.get_all_healthy(READ).unwrap();
    for inst in &set3.instances {
        assert!(set4.includes(&inst.addr));
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShardWithLookback
#[test]
fn test_ring_shuffle_shard_with_lookback() {
    #[derive(Clone)]
    enum Ev {
        Add {
            id: &'static str,
            zone: &'static str,
            tok: u32,
            reg: Option<SystemTime>,
            ro: bool,
            ro_time: Option<SystemTime>,
        },
        Remove(&'static str),
        Test {
            shard_size: usize,
            expected: Vec<&'static str>,
        },
    }

    let lookback = Duration::from_secs(3600);
    let user = "user-1";
    let now_s = system_time_to_unix_secs(SystemTime::now());
    let now = UNIX_EPOCH + Duration::from_secs(now_s as u64);
    let old = now - 2 * lookback;
    let min_ago = now - Duration::from_secs(60);

    let timelines: Vec<(&str, Vec<Ev>)> = vec![
        (
            "single zone recently bootstrapped",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-a", tok: user_token(user, "zone-a", 1) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Add { id: "instance-3", zone: "zone-a", tok: user_token(user, "zone-a", 2) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Test { shard_size: 1, expected: vec!["instance-1", "instance-2", "instance-3"] },
            ],
        ),
        (
            "single zone shard=0 recently bootstrapped",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-a", tok: user_token(user, "zone-a", 1) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Add { id: "instance-3", zone: "zone-a", tok: user_token(user, "zone-a", 2) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Test { shard_size: 0, expected: vec!["instance-1", "instance-2", "instance-3"] },
            ],
        ),
        (
            "single zone scale up",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 3, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-a", tok: user_token(user, "zone-a", 1) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-3", zone: "zone-a", tok: user_token(user, "zone-a", 2) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Test { shard_size: 1, expected: vec!["instance-1"] },
                Ev::Add { id: "instance-4", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 2, reg: Some(now - Duration::from_secs(600)), ro: false, ro_time: None },
                Ev::Test { shard_size: 1, expected: vec!["instance-4", "instance-1"] },
                Ev::Add { id: "instance-5", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(now - Duration::from_secs(300)), ro: false, ro_time: None },
                Ev::Test { shard_size: 1, expected: vec!["instance-5", "instance-4", "instance-1"] },
            ],
        ),
        (
            "single zone scale down",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-a", tok: user_token(user, "zone-a", 1) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-3", zone: "zone-a", tok: user_token(user, "zone-a", 2) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Test { shard_size: 1, expected: vec!["instance-1"] },
                Ev::Remove("instance-3"),
                Ev::Test { shard_size: 1, expected: vec!["instance-1"] },
                Ev::Remove("instance-1"),
                Ev::Test { shard_size: 1, expected: vec!["instance-2"] },
            ],
        ),
        (
            "single zone ro within lookback shard=2",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-a", tok: user_token(user, "zone-a", 1) + 1, reg: Some(old), ro: true, ro_time: Some(now + lookback / 2) },
                Ev::Add { id: "instance-3", zone: "zone-a", tok: user_token(user, "zone-a", 2) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Test { shard_size: 2, expected: vec!["instance-1", "instance-2", "instance-3"] },
            ],
        ),
        (
            "single zone ro outside lookback shard=2",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-a", tok: user_token(user, "zone-a", 1) + 1, reg: Some(old), ro: true, ro_time: Some(old) },
                Ev::Add { id: "instance-3", zone: "zone-a", tok: user_token(user, "zone-a", 2) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Test { shard_size: 2, expected: vec!["instance-1", "instance-3"] },
            ],
        ),
        (
            "multi zone without registered_timestamp",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: None, ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-b", tok: user_token(user, "zone-b", 0) + 1, reg: None, ro: false, ro_time: None },
                Ev::Add { id: "instance-3", zone: "zone-c", tok: user_token(user, "zone-c", 0) + 1, reg: None, ro: false, ro_time: None },
                Ev::Add { id: "instance-4", zone: "zone-a", tok: user_token(user, "zone-a", 1) + 1, reg: None, ro: false, ro_time: None },
                Ev::Add { id: "instance-5", zone: "zone-b", tok: user_token(user, "zone-b", 1) + 1, reg: None, ro: false, ro_time: None },
                Ev::Add { id: "instance-6", zone: "zone-c", tok: user_token(user, "zone-c", 1) + 1, reg: None, ro: false, ro_time: None },
                Ev::Test { shard_size: 3, expected: vec!["instance-1", "instance-2", "instance-3"] },
                Ev::Remove("instance-1"),
                Ev::Remove("instance-2"),
                Ev::Remove("instance-3"),
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-b", tok: user_token(user, "zone-b", 0) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Add { id: "instance-3", zone: "zone-c", tok: user_token(user, "zone-c", 0) + 1, reg: Some(min_ago), ro: false, ro_time: None },
                Ev::Test { shard_size: 3, expected: vec!["instance-1", "instance-2", "instance-3", "instance-4", "instance-5", "instance-6"] },
            ],
        ),
        (
            "multi zone with ro within lookback",
            vec![
                Ev::Add { id: "instance-1", zone: "zone-a", tok: user_token(user, "zone-a", 0) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-2", zone: "zone-b", tok: user_token(user, "zone-b", 1) + 1, reg: Some(old), ro: true, ro_time: Some(now + lookback / 2) },
                Ev::Add { id: "instance-3", zone: "zone-b", tok: user_token(user, "zone-b", 2) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-4", zone: "zone-c", tok: user_token(user, "zone-c", 3) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-5", zone: "zone-c", tok: user_token(user, "zone-c", 4) + 1, reg: Some(old), ro: false, ro_time: None },
                Ev::Add { id: "instance-6", zone: "zone-c", tok: user_token(user, "zone-c", 5) + 1, reg: Some(old), ro: true, ro_time: Some(now + lookback / 2) },
                Ev::Test { shard_size: 3, expected: vec!["instance-1", "instance-2", "instance-3", "instance-4", "instance-6"] },
            ],
        ),
    ];

    for (name, events) in timelines {
        for upd_reg in [false, true] {
            for upd_ro in [false, true] {
                let mut desc = Desc::default();
                let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
                ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);

                for (step, ev) in events.iter().enumerate() {
                    match ev {
                        Ev::Add { id, zone, tok, reg, ro, ro_time } => {
                            let mut inst = gen_ring_instance_with_info(id, zone, vec![*tok], *reg);
                            inst.read_only = *ro;
                            inst.read_only_updated_timestamp = ro_time.map(system_time_to_unix_secs).unwrap_or(0);
                            desc.ingesters.insert((*id).to_string(), inst);
                            ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);
                        }
                        Ev::Remove(id) => {
                            desc.ingesters.remove(*id);
                            ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);
                        }
                        Ev::Test { shard_size, expected } => {
                            let rs = ring.shuffle_shard_with_lookback(user, *shard_size, lookback, now).get_all_healthy(READ).unwrap();
                            let mut got = rs.get_addresses();
                            got.sort();
                            let mut exp: Vec<String> = expected.iter().map(|s| (*s).to_string()).collect();
                            exp.sort();
                            assert_eq!(got, exp, "{name} step={step}");
                        }
                    }
                }
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShardWithLookback_CorrectnessWithFuzzy
#[test]
fn test_ring_shuffle_shard_with_lookback_correctness_with_fuzzy() {
    let lookback = Duration::from_secs(3600);
    let delay = Duration::from_secs(300);
    let user = "user-1";

    for upd_reg in [false, true] {
        for upd_ro in [false, true] {
            for num_inst in [9usize, 30] {
                for num_zones in [1usize, 3] {
                    let seed = 1_700_000_000i64 + (num_inst as i64) + (num_zones as i64);
                    let mut rnd = GoRand::new(seed);
                    let gen = RandomTokenGenerator::with_seed(seed);
                    let mut desc = Desc {
                        ingesters: gen_ring_instances(&gen, num_inst, num_zones, 128),
                    };
                    let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 3, true), Arc::new(DefaultReplicationStrategy));
                    ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);

                    let mut shard_size = num_zones;
                    let mut curr_time = SystemTime::now() + lookback + Duration::from_secs(60);

                    let rs0 = ring.shuffle_shard(user, shard_size).get_replication_set_for_operation(READ).unwrap();
                    let mut history: Vec<(ReplicationSet, SystemTime)> = vec![(rs0, curr_time)];
                    let mut ro_map: HashMap<String, InstanceDesc> = HashMap::new();
                    let mut next_id = desc.ingesters.len() + 1;

                    for _event in 1..=60 {
                        curr_time += delay;
                        let r = rnd.int31n(100);
                        if r < 60 {
                            let id = format!("instance-{next_id}");
                            let z = format!("zone-{}", next_id % num_zones);
                            next_id += 1;
                            let toks = gen.generate_tokens(128, &desc.get_tokens());
                            desc.ingesters.insert(id.clone(), gen_ring_instance_with_info(&id, &z, toks, Some(curr_time)));
                            ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);
                        } else if r < 70 {
                            let id_rm = get_random_instance_id(&desc.ingesters, &mut rnd);
                            let z = desc.ingesters[&id_rm].zone.clone();
                            if ring.instances_in_zone_count(&z) > 1 {
                                desc.ingesters.remove(&id_rm);
                                ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);
                                for (hist_rs, _) in &mut history {
                                    hist_rs.instances.retain(|i| i.addr != id_rm);
                                }
                                ro_map.remove(&id_rm);
                            }
                        } else if r < 80 {
                            let id = get_random_instance_id(&desc.ingesters, &mut rnd);
                            let inst = desc.ingesters.get_mut(&id).unwrap();
                            if !inst.read_only {
                                inst.read_only = true;
                                inst.read_only_updated_timestamp = system_time_to_unix_secs(curr_time);
                                ro_map.insert(id, inst.clone());
                                ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);
                            }
                        } else if r < 90 {
                            if !ro_map.is_empty() {
                                let id = get_random_instance_id(&ro_map, &mut rnd);
                                let inst = desc.ingesters.get_mut(&id).unwrap();
                                inst.read_only = false;
                                inst.read_only_updated_timestamp = system_time_to_unix_secs(curr_time);
                                ro_map.remove(&id);
                                ring.set_ring_state_from_desc(desc.clone(), false, upd_reg, upd_ro);
                            }
                        } else {
                            shard_size += num_zones;
                        }

                        let rs = ring.shuffle_shard(user, shard_size).get_replication_set_for_operation(READ).unwrap();
                        history.push((rs, curr_time));
                        let rs_lb = ring.shuffle_shard_with_lookback(user, shard_size, lookback, curr_time).get_replication_set_for_operation(READ).unwrap();
                        let cutoff = curr_time - lookback;
                        history.retain(|(_, ts)| *ts >= cutoff);
                        for (hist_rs, _) in &history {
                            for inst in &hist_rs.instances {
                                if !inst.read_only {
                                    assert!(rs_lb.includes(&inst.addr), "missing {}", inst.addr);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn lookback_caching_instances(user_id: &str, reg: SystemTime) -> HashMap<String, InstanceDesc> {
    HashMap::from([
        ("instance-1".into(), gen_ring_instance_with_info("instance-1", "zone-a", vec![user_token(user_id, "zone-a", 0) + 1], Some(reg))),
        ("instance-2".into(), gen_ring_instance_with_info("instance-2", "zone-a", vec![user_token(user_id, "zone-a", 1) + 1], Some(reg))),
        ("instance-3".into(), gen_ring_instance_with_info("instance-3", "zone-b", vec![user_token(user_id, "zone-b", 0) + 1], Some(reg))),
        ("instance-4".into(), gen_ring_instance_with_info("instance-4", "zone-b", vec![user_token(user_id, "zone-b", 1) + 1], Some(reg))),
        ("instance-5".into(), gen_ring_instance_with_info("instance-5", "zone-c", vec![user_token(user_id, "zone-c", 0) + 1], Some(reg))),
        ("instance-6".into(), gen_ring_instance_with_info("instance-6", "zone-c", vec![user_token(user_id, "zone-c", 1) + 1], Some(reg))),
    ])
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShardWithLookback_Caching
#[test]
fn test_ring_shuffle_shard_with_lookback_caching() {
    let user_id = "user-1";
    let other_user_id = "user-2";
    let subring_size = 3;
    let now = UNIX_EPOCH + Duration::from_secs(system_time_to_unix_secs(SystemTime::now()) as u64);
    let old = now - Duration::from_secs(7200);
    let hour = Duration::from_secs(3600);

    let new_ring = |ingesters: HashMap<String, InstanceDesc>| {
        let r = Ring::new_for_testing(make_settings(Duration::from_secs(3 * 3600), 1, true), Arc::new(DefaultReplicationStrategy));
        r.update_ring_state(Desc { ingesters });
        r
    };

    // identical request
    {
        let ring = new_ring(lookback_caching_instances(user_id, old));
        let first = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        let second = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.zones_count(), 3);
    }
    // after cleanup
    {
        let ring = new_ring(lookback_caching_instances(user_id, old));
        let first = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        ring.cleanup_shuffle_shard_cache(user_id);
        let second = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        assert!(!Arc::ptr_eq(&first, &second));
    }
    // different sizes
    {
        let ring = new_ring(lookback_caching_instances(user_id, old));
        let first = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        let second = ring.shuffle_shard_with_lookback(user_id, subring_size + 1, hour, now);
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(first.instances_in_zone_count("zone-a"), 1);
        assert_eq!(second.instances_in_zone_count("zone-a"), 2);
    }
    // different identifiers
    {
        let ring = new_ring(lookback_caching_instances(user_id, old));
        let first = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        let second = ring.shuffle_shard_with_lookback(other_user_id, subring_size, hour, now);
        assert!(!Arc::ptr_eq(&first, &second));
    }
    // out-of-order lookback window eviction
    {
        let mut insts = lookback_caching_instances(user_id, old);
        insts.insert(
            "instance-1".into(),
            gen_ring_instance_with_info("instance-1", "zone-a", vec![user_token(user_id, "zone-a", 0) + 1], Some(now - Duration::from_secs(62 * 60))),
        );
        let ring = new_ring(insts);
        let first_early = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now - Duration::from_secs(180));
        let first_later = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        assert!(!Arc::ptr_eq(&first_early, &first_later));
        assert_eq!(first_early.instances_in_zone_count("zone-a"), 2);
        assert_eq!(first_later.instances_in_zone_count("zone-a"), 1);

        let second_early = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now - Duration::from_secs(180));
        assert!(!Arc::ptr_eq(&first_early, &second_early));
        let second_later = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        assert!(Arc::ptr_eq(&first_later, &second_later));
    }
    // different lookback durations
    {
        let mut insts = lookback_caching_instances(user_id, old);
        insts.insert(
            "instance-1".into(),
            gen_ring_instance_with_info("instance-1", "zone-a", vec![user_token(user_id, "zone-a", 0) + 1], Some(now - hour)),
        );
        let ring = new_ring(insts);
        let h1 = ring.shuffle_shard_with_lookback(user_id, subring_size, hour, now);
        let h30 = ring.shuffle_shard_with_lookback(user_id, subring_size, Duration::from_secs(1800), now);
        assert!(!Arc::ptr_eq(&h1, &h30));
        assert_eq!(h1.instances_in_zone_count("zone-a"), 2);
        assert_eq!(h30.instances_in_zone_count("zone-a"), 1);
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShardWithLookback_CachingAfterTopologyChange
#[test]
fn test_ring_shuffle_shard_with_lookback_caching_after_topology_change() {
    let reg = Registry::new();
    let ring = Ring::with_optional_store_and_strategy(
        make_settings(Duration::from_secs(3 * 3600), 1, true),
        "test",
        "test",
        None,
        Arc::new(DefaultReplicationStrategy),
        Some(&reg),
    )
    .unwrap();
    let user_id = "user-1";
    let now = SystemTime::now();
    let mut insts = lookback_caching_instances(user_id, now - Duration::from_secs(7200));
    ring.update_ring_state(Desc { ingesters: insts.clone() });

    let first = ring.shuffle_shard_with_lookback(user_id, 3, Duration::from_secs(3600), now);
    let mut addrs1 = first.get_all_healthy(READ).unwrap().get_addresses();
    addrs1.sort();
    assert_eq!(addrs1, vec!["instance-1", "instance-3", "instance-5"]);

    insts.remove("instance-1");
    ring.update_ring_state(Desc { ingesters: insts });

    let second = ring.shuffle_shard_with_lookback(user_id, 3, Duration::from_secs(3600), now);
    assert!(!Arc::ptr_eq(&first, &second));
    let mut addrs2 = second.get_all_healthy(READ).unwrap().get_addresses();
    addrs2.sort();
    assert_eq!(addrs2, vec!["instance-2", "instance-3", "instance-5"]);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShardWithLookback_CachingAfterReadOnlyChange
#[test]
fn test_ring_shuffle_shard_with_lookback_caching_after_read_only_change() {
    let reg = Registry::new();
    let ring = Ring::with_optional_store_and_strategy(
        make_settings(Duration::from_secs(3 * 3600), 1, true),
        "test",
        "test",
        None,
        Arc::new(DefaultReplicationStrategy),
        Some(&reg),
    )
    .unwrap();
    let user_id = "user-1";
    let now = SystemTime::now();
    let mut insts = lookback_caching_instances(user_id, now - Duration::from_secs(7200));
    ring.update_ring_state(Desc { ingesters: insts.clone() });

    let first = ring.shuffle_shard_with_lookback(user_id, 3, Duration::from_secs(3600), now);
    assert_eq!(first.instances_count(), 3);

    insts.insert("instance-1".into(), make_read_only(insts["instance-1"].clone(), now));
    insts.insert("instance-5".into(), make_read_only(insts["instance-5"].clone(), now));
    ring.update_ring_state(Desc { ingesters: insts });

    let second = ring.shuffle_shard_with_lookback(user_id, 3, Duration::from_secs(3600), now);
    assert!(!Arc::ptr_eq(&first, &second));
    let mut addrs2 = second.get_all_healthy(READ).unwrap().get_addresses();
    addrs2.sort();
    assert_eq!(addrs2, vec!["instance-1", "instance-2", "instance-3", "instance-5", "instance-6"]);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShardWithLookback_CachingAfterHeartbeatOrStateChange
#[test]
fn test_ring_shuffle_shard_with_lookback_caching_after_heartbeat_or_state_change() {
    let reg = Registry::new();
    let ring = Ring::with_optional_store_and_strategy(
        make_settings(Duration::from_secs(3 * 3600), 1, true),
        "test",
        "test",
        None,
        Arc::new(DefaultReplicationStrategy),
        Some(&reg),
    )
    .unwrap();
    let user_id = "user-1";
    let now = SystemTime::now();
    let mut insts = lookback_caching_instances(user_id, now - Duration::from_secs(7200));
    ring.update_ring_state(Desc { ingesters: insts.clone() });

    let first = ring.shuffle_shard_with_lookback(user_id, 3, Duration::from_secs(3600), now);

    let new_ts = system_time_to_unix_secs(now + Duration::from_secs(32 * 60));
    insts.get_mut("instance-1").unwrap().timestamp = new_ts;
    insts.get_mut("instance-3").unwrap().state = InstanceState::Leaving as i32;
    ring.update_ring_state(Desc { ingesters: insts });

    let second = ring.shuffle_shard_with_lookback(user_id, 3, Duration::from_secs(3600), now);
    assert!(Arc::ptr_eq(&first, &second));

    let rs = first.get_all_healthy(READ).unwrap();
    for inst in rs.instances {
        if inst.addr == "instance-1" {
            assert_eq!(inst.timestamp, new_ts);
        } else if inst.addr == "instance-3" {
            assert_eq!(inst.state, InstanceState::Leaving as i32);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShardWithLookback_CachingConcurrency
#[tokio::test]
async fn test_ring_shuffle_shard_with_lookback_caching_concurrency() {
    let now = SystemTime::now();
    let ring = Ring::with_optional_store_and_strategy(
        make_settings(Duration::from_secs(3600), 1, true),
        "test",
        "test",
        None,
        Arc::new(DefaultReplicationStrategy),
        Some(&Registry::new()),
    )
    .unwrap();
    let gen = RandomTokenGenerator::with_seed(22);
    let old = now - Duration::from_secs(7200);
    let desc = Desc {
        ingesters: HashMap::from([
            ("instance-1".into(), gen_ring_instance_with_info("instance-1", "zone-a", gen.generate_tokens(128, &[]), Some(old))),
            ("instance-2".into(), make_read_only(gen_ring_instance_with_info("instance-2", "zone-a", gen.generate_tokens(128, &[]), Some(old)), now)),
            ("instance-3".into(), gen_ring_instance_with_info("instance-3", "zone-b", gen.generate_tokens(128, &[]), Some(old))),
            ("instance-4".into(), gen_ring_instance_with_info("instance-4", "zone-b", gen.generate_tokens(128, &[]), Some(old))),
            ("instance-5".into(), gen_ring_instance_with_info("instance-5", "zone-c", gen.generate_tokens(128, &[]), Some(old))),
            ("instance-6".into(), make_read_only(gen_ring_instance_with_info("instance-6", "zone-c", gen.generate_tokens(128, &[]), Some(old)), old)),
        ]),
    };
    ring.update_ring_state(desc);

    for shard_size in [3usize, 0] {
        let mut handles = Vec::new();
        for w in 0..10 {
            let r_c = Arc::clone(&ring);
            handles.push(tokio::spawn(async move {
                let user_id = format!("user-{w}");
                let expected = r_c.shuffle_shard_with_lookback(&user_id, shard_size, Duration::from_secs(3600), now);
                for req in 0..200 {
                    let actual = r_c.shuffle_shard_with_lookback(&user_id, shard_size, Duration::from_secs(3600), now);
                    assert_eq!(actual.desc_snapshot(), expected.desc_snapshot());
                    let _ = r_c.shuffle_shard_with_lookback(&format!("stress-{req}"), shard_size, Duration::from_secs(3600), now);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_Get_NoMemoryAllocations
#[test]
fn test_ring_get_no_memory_allocations() {
    let gen = RandomTokenGenerator::with_seed(23);
    let desc = Desc {
        ingesters: gen_ring_instances(&gen, 3, 3, 128),
    };
    let mut settings = make_settings(Duration::from_secs(3600), 3, true);
    settings.subring_cache_disabled = true;
    let ring = Ring::new_for_testing(settings, Arc::new(DefaultReplicationStrategy));
    ring.set_ring_state_from_desc(desc, false, false, false);

    let mut rng = GoRand::new(23);
    for _ in 0..10 {
        let set = ring.get(rng.uint32(), WRITE).unwrap();
        assert_eq!(set.instances.len(), 3);
    }
}

async fn start_test_lifecycler(
    kv: Arc<dyn KvClient>,
    hb: Duration,
    id_num: usize,
    zones: usize,
) -> (Arc<Lifecycler>, Arc<ManagedBasicService>) {
    let cfg = LifecyclerConfig {
        heartbeat_period: hb,
        heartbeat_timeout: Duration::from_secs(60),
        observe_period: Duration::ZERO,
        join_after: Duration::ZERO,
        min_ready_duration: Duration::ZERO,
        final_sleep: Duration::ZERO,
        num_tokens: 16,
        tokens_file_path: String::new(),
        zone: format!("zone-{}", id_num % zones),
        unregister_on_shutdown: true,
        readiness_check_ring_health: false,
        addr: format!("addr-{id_num}"),
        port: 1,
        id: format!("instance-{id_num}"),
        token_generator: None,
        spread_minimizing_token_generator: false,
    };
    let lc = Lifecycler::with_store(
        cfg,
        kv,
        Some(Arc::new(NoopFlushTransferer)),
        "test",
        "test",
        false,
        None,
    );
    let svc = lc.clone().into_service();
    start_and_await_running(ServiceContext::background(), &*svc)
        .await
        .unwrap();
    (lc, svc)
}

async fn poll_eq<T: PartialEq + std::fmt::Debug>(
    timeout: Duration,
    expected: T,
    mut f: impl FnMut() -> T,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let got = f();
        if got == expected {
            return;
        }
        assert!(Instant::now() < deadline, "poll timeout: expected {expected:?}, got {got:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRingUpdates
#[tokio::test]
async fn test_ring_updates() {
    let cases = [
        (vec![], 3usize, 3usize, vec![("zone-0", 1usize), ("zone-1", 1), ("zone-2", 1)]),
        (vec!["zone-0".to_string()], 2, 2, vec![("zone-0", 0), ("zone-1", 1), ("zone-2", 1)]),
    ];

    for (excluded, exp_inst, exp_zones, exp_per_zone) in cases {
        let store = Arc::new(InMemoryStore::new());
        let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::with_store(store, ring_desc_codec()));
        let mut settings = make_settings(Duration::from_secs(60), 3, false);
        settings.excluded_zones = excluded.clone();

        let ring = Ring::with_store_and_strategy(
            settings,
            "test",
            "test",
            Arc::clone(&kv),
            Arc::new(DefaultReplicationStrategy),
            None,
        )
        .unwrap();
        let ring_svc = ring.clone().into_service();
        start_and_await_running(ServiceContext::background(), &*ring_svc)
            .await
            .unwrap();
        assert_eq!(ring.instances_count(), 0);

        let mut lcs = Vec::new();
        for i in 1..=3 {
            lcs.push(start_test_lifecycler(Arc::clone(&kv), Duration::from_millis(100), i, 3).await);
        }

        poll_eq(Duration::from_secs(2), exp_inst, || ring.instances_count()).await;
        for (z, cnt) in exp_per_zone {
            assert_eq!(ring.instances_in_zone_count(z), cnt);
        }
        assert_eq!(ring.zones_count(), exp_zones);

        tokio::time::sleep(Duration::from_secs(2)).await;
        let rs = ring.get_all_healthy(READ).unwrap();
        let now_s = system_time_to_unix_secs(SystemTime::now());
        for ing in rs.instances {
            assert!((now_s - ing.timestamp).abs() <= 2);
            assert!(!excluded.contains(&ing.zone));
        }

        for (_lc, svc) in lcs {
            stop_and_await_terminated(ServiceContext::background(), &*svc)
                .await
                .unwrap();
        }
        poll_eq(Duration::from_secs(2), 0, || ring.instances_count()).await;
        stop_and_await_terminated(ServiceContext::background(), &*ring_svc)
            .await
            .unwrap();
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_ShuffleShard_Caching
#[tokio::test]
async fn test_ring_shuffle_shard_caching() {
    let store = Arc::new(InMemoryStore::new());
    let kv: Arc<dyn KvClient> = Arc::new(InMemoryKv::with_store(store, ring_desc_codec()));
    let ring = Ring::with_store_and_strategy(
        make_settings(Duration::from_secs(60), 3, true),
        "test",
        "test",
        Arc::clone(&kv),
        Arc::new(DefaultReplicationStrategy),
        None,
    )
    .unwrap();
    let ring_svc = ring.clone().into_service();
    start_and_await_running(ServiceContext::background(), &*ring_svc)
        .await
        .unwrap();

    const ZONES: usize = 3;
    const NUM_LCS: usize = ZONES * 3;
    let mut lcs = Vec::with_capacity(NUM_LCS);
    for i in 0..NUM_LCS {
        lcs.push(start_test_lifecycler(Arc::clone(&kv), Duration::from_millis(500), i, ZONES).await);
    }

    poll_eq(Duration::from_secs(5), NUM_LCS, || {
        ring.get_replication_set_for_operation(READ)
            .map(|rs| rs.instances.iter().filter(|i| i.state == InstanceState::Active as i32).count())
            .unwrap_or(0)
    })
    .await;

    let subring = ring.shuffle_shard("user", ZONES);
    for _ in 0..60 {
        let s2 = ring.shuffle_shard("user", ZONES);
        assert!(Arc::ptr_eq(&subring, &s2));
        assert_eq!(subring.instances_count(), ZONES);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let rs = subring.get_replication_set_for_operation(READ).unwrap();
    let now_s = system_time_to_unix_secs(SystemTime::now());
    for ing in rs.instances {
        assert!((now_s - ing.timestamp).abs() <= 2);
    }

    lcs[0].0.change_read_only_state(true).await.unwrap();
    poll_eq(Duration::from_secs(5), 1, || ring.read_only_instance_count()).await;

    let new_sub = ring.shuffle_shard("user", ZONES);
    assert!(!Arc::ptr_eq(&subring, &new_sub));

    let lb1 = ring.shuffle_shard_with_lookback("user", ZONES, Duration::from_secs(600), SystemTime::now());
    let lb2 = ring.shuffle_shard_with_lookback("user", ZONES, Duration::from_secs(600), SystemTime::now());
    assert!(Arc::ptr_eq(&lb1, &lb2));
    let lb3 = ring.shuffle_shard_with_lookback("user", ZONES, Duration::from_nanos(1), SystemTime::now());
    assert!(!Arc::ptr_eq(&lb2, &lb3));

    for (_lc, svc) in lcs.drain(..ZONES) {
        stop_and_await_terminated(ServiceContext::background(), &*svc)
            .await
            .unwrap();
    }
    poll_eq(Duration::from_secs(5), NUM_LCS - ZONES, || ring.instances_count()).await;

    let sub_after = ring.shuffle_shard("user", ZONES);
    assert!(!Arc::ptr_eq(&subring, &sub_after));

    let sub_1 = ring.shuffle_shard("user", 1);
    assert!(!Arc::ptr_eq(&sub_after, &sub_1));
    assert_eq!(sub_1.instances_count(), ZONES);

    let sub_1_cached = ring.shuffle_shard("user", 1);
    assert!(Arc::ptr_eq(&sub_1, &sub_1_cached));

    ring.cleanup_shuffle_shard_cache("user");
    let sub_1_fresh = ring.shuffle_shard("user", 1);
    assert!(!Arc::ptr_eq(&sub_1, &sub_1_fresh));

    let full_sub = ring.shuffle_shard("user", NUM_LCS);
    assert_eq!(full_sub.desc_snapshot().ingesters.len(), ring.desc_snapshot().ingesters.len());

    for (_lc, svc) in lcs {
        stop_and_await_terminated(ServiceContext::background(), &*svc)
            .await
            .unwrap();
    }
    stop_and_await_terminated(ServiceContext::background(), &*ring_svc)
        .await
        .unwrap();
}

fn find_metric(reg: &Registry, metric: &str, labels: &[(&str, &str)]) -> f64 {
    for mf in reg.gather() {
        if mf.get_name() != metric {
            continue;
        }
        for m in mf.get_metric() {
            let matched = labels.iter().all(|(k, v)| {
                m.get_label()
                    .iter()
                    .any(|lp| lp.get_name() == *k && lp.get_value() == *v)
            });
            if matched {
                return m.get_gauge().get_value();
            }
        }
    }
    panic!("metric {metric} with labels {labels:?} not found");
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestUpdateMetrics
#[test]
fn test_update_metrics() {
    let reg = Registry::new();
    let ring = Ring::with_optional_store_and_strategy(
        make_settings(Duration::from_secs(3600), 3, true),
        "test",
        "test",
        None,
        Arc::new(DefaultReplicationStrategy),
        Some(&reg),
    )
    .unwrap();
    let now = system_time_to_unix_secs(SystemTime::now());
    let desc = Desc {
        ingesters: HashMap::from([
            ("A".into(), InstanceDesc { addr: "127.0.0.1".into(), zone: "a".into(), timestamp: now - 11, tokens: vec![u32::MAX / 4, (u32::MAX / 4) * 3], ..Default::default() }),
            ("B".into(), InstanceDesc { addr: "127.0.0.2".into(), zone: "b".into(), timestamp: now - 22, tokens: vec![(u32::MAX / 4) * 2, u32::MAX], ..Default::default() }),
        ]),
    };
    ring.update_ring_state(desc);

    assert_eq!(find_metric(&reg, "ring_members", &[("name", "test"), ("state", "ACTIVE")]), 2.0);
    assert_eq!(find_metric(&reg, "ring_members", &[("name", "test"), ("state", "JOINING")]), 0.0);
    assert_eq!(find_metric(&reg, "ring_oldest_member_timestamp", &[("name", "test"), ("state", "ACTIVE")]), (now - 22) as f64);
    assert_eq!(find_metric(&reg, "ring_tokens_total", &[("name", "test")]), 4.0);
    assert_eq!(find_metric(&reg, "ring_zone_members", &[("name", "test"), ("zone", "a"), ("state", "ACTIVE")]), 1.0);
    assert_eq!(find_metric(&reg, "ring_zone_members", &[("name", "test"), ("zone", "b"), ("state", "ACTIVE")]), 1.0);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestUpdateMetricsWithRemoval
#[test]
fn test_update_metrics_with_removal() {
    let reg = Registry::new();
    let ring = Ring::with_optional_store_and_strategy(
        make_settings(Duration::from_secs(3600), 3, true),
        "test",
        "test",
        None,
        Arc::new(DefaultReplicationStrategy),
        Some(&reg),
    )
    .unwrap();
    let now = system_time_to_unix_secs(SystemTime::now());
    ring.update_ring_state(Desc {
        ingesters: HashMap::from([
            ("A".into(), InstanceDesc { addr: "127.0.0.1".into(), zone: "a".into(), timestamp: now - 11, tokens: vec![u32::MAX / 4, (u32::MAX / 4) * 3], ..Default::default() }),
            ("B".into(), InstanceDesc { addr: "127.0.0.2".into(), zone: "b".into(), timestamp: now - 22, tokens: vec![(u32::MAX / 4) * 2, u32::MAX], ..Default::default() }),
        ]),
    });
    assert_eq!(find_metric(&reg, "ring_members", &[("name", "test"), ("state", "ACTIVE")]), 2.0);
    assert_eq!(find_metric(&reg, "ring_tokens_total", &[("name", "test")]), 4.0);

    ring.update_ring_state(Desc {
        ingesters: HashMap::from([(
            "A".into(),
            InstanceDesc { addr: "127.0.0.1".into(), zone: "a".into(), timestamp: now - 11, tokens: vec![u32::MAX / 4, (u32::MAX / 4) * 3], ..Default::default() },
        )]),
    });
    assert_eq!(find_metric(&reg, "ring_members", &[("name", "test"), ("state", "ACTIVE")]), 1.0);
    assert_eq!(find_metric(&reg, "ring_oldest_member_timestamp", &[("name", "test"), ("state", "ACTIVE")]), (now - 11) as f64);
    assert_eq!(find_metric(&reg, "ring_tokens_total", &[("name", "test")]), 2.0);
    assert_eq!(find_metric(&reg, "ring_zone_members", &[("name", "test"), ("zone", "a"), ("state", "ACTIVE")]), 1.0);
    assert_eq!(find_metric(&reg, "ring_zone_members", &[("name", "test"), ("zone", "b"), ("state", "ACTIVE")]), 0.0);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestMergeTokenGroups
#[test]
fn test_merge_token_groups() {
    let cases = [
        (vec![], vec![]),
        (vec![("group-1", vec![])], vec![]),
        (vec![("group-1", vec![1])], vec![1]),
        (vec![("group-1", vec![1, 2, 3])], vec![1, 2, 3]),
        (vec![("group-1", vec![1, 2, 2])], vec![1, 2, 2]),
        (vec![("group-1", vec![1]), ("group-2", vec![2])], vec![1, 2]),
        (vec![("group-1", vec![1, 3, 4, 7]), ("group-2", vec![2, 5, 6])], vec![1, 2, 3, 4, 5, 6, 7]),
        (vec![("group-1", vec![1, 4, 6]), ("group-2", vec![2, 4, 5])], vec![1, 2, 4, 4, 5, 6]),
        (vec![("group-1", vec![1, 4, 7]), ("group-2", vec![2, 5, 8]), ("group-3", vec![3, 6, 9])], vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
    ];
    for (groups, expected) in cases {
        let map: HashMap<String, Vec<u32>> = groups.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        assert_eq!(merge_token_groups(&map), expected);
    }
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestCountTokensSingleZone
#[test]
fn test_count_tokens_single_zone() {
    assert!(Desc::default().count_tokens().is_empty());

    let d1 = Desc {
        ingesters: HashMap::from([(
            "ingester-1".into(),
            InstanceDesc { tokens: vec![1_000_000], ..Default::default() },
        )]),
    };
    assert_eq!(d1.count_tokens()["ingester-1"], i64::from(u32::MAX) + 1);

    let d2 = Desc {
        ingesters: HashMap::from([(
            "ingester-1".into(),
            InstanceDesc { tokens: vec![1_000_000, 2_000_000, 3_000_000], ..Default::default() },
        )]),
    };
    assert_eq!(d2.count_tokens()["ingester-1"], i64::from(u32::MAX) + 1);

    let d3 = Desc {
        ingesters: HashMap::from([
            ("ingester-1".into(), InstanceDesc { tokens: vec![1_000_000, 3_000_000, 6_000_000], ..Default::default() }),
            ("ingester-2".into(), InstanceDesc { tokens: vec![2_000_000, 4_000_000, 8_000_000], ..Default::default() }),
            ("ingester-3".into(), InstanceDesc { tokens: vec![5_000_000, 9_000_000], ..Default::default() }),
        ]),
    };
    let c3 = d3.count_tokens();
    assert_eq!(c3["ingester-1"], 3_000_000 + (i64::from(u32::MAX) + 1 - 9_000_000));
    assert_eq!(c3["ingester-2"], 4_000_000);
    assert_eq!(c3["ingester-3"], 2_000_000);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestCountTokensMultiZones
#[test]
fn test_count_tokens_multi_zones() {
    assert!(Desc::default().count_tokens().is_empty());

    let d1 = Desc {
        ingesters: HashMap::from([
            ("ingester-zone-a-1".into(), InstanceDesc { zone: "zone-a".into(), tokens: vec![1_000_000], ..Default::default() }),
            ("ingester-zone-b-1".into(), InstanceDesc { zone: "zone-b".into(), tokens: vec![1_000_001], ..Default::default() }),
            ("ingester-zone-c-1".into(), InstanceDesc { zone: "zone-c".into(), tokens: vec![1_000_002], ..Default::default() }),
        ]),
    };
    let c1 = d1.count_tokens();
    assert_eq!(c1["ingester-zone-a-1"], i64::from(u32::MAX) + 1);
    assert_eq!(c1["ingester-zone-b-1"], i64::from(u32::MAX) + 1);
    assert_eq!(c1["ingester-zone-c-1"], i64::from(u32::MAX) + 1);

    let d2 = Desc {
        ingesters: HashMap::from([
            ("ingester-zone-a-1".into(), InstanceDesc { zone: "zone-a".into(), tokens: vec![1_000_000, 3_000_000, 6_000_000], ..Default::default() }),
            ("ingester-zone-a-2".into(), InstanceDesc { zone: "zone-a".into(), tokens: vec![2_000_000, 4_000_000, 8_000_000], ..Default::default() }),
            ("ingester-zone-a-3".into(), InstanceDesc { zone: "zone-a".into(), tokens: vec![5_000_000, 9_000_000], ..Default::default() }),
            ("ingester-zone-b-1".into(), InstanceDesc { zone: "zone-b".into(), tokens: vec![1_000_001, 3_000_001, 6_000_001], ..Default::default() }),
            ("ingester-zone-b-2".into(), InstanceDesc { zone: "zone-b".into(), tokens: vec![2_000_001, 4_000_001, 8_000_001], ..Default::default() }),
            ("ingester-zone-b-3".into(), InstanceDesc { zone: "zone-b".into(), tokens: vec![5_000_001, 9_000_001], ..Default::default() }),
        ]),
    };
    let c2 = d2.count_tokens();
    let expected_1 = 3_000_000 + (i64::from(u32::MAX) + 1 - 9_000_000);
    assert_eq!(c2["ingester-zone-a-1"], expected_1);
    assert_eq!(c2["ingester-zone-a-2"], 4_000_000);
    assert_eq!(c2["ingester-zone-a-3"], 2_000_000);
    assert_eq!(c2["ingester-zone-b-1"], expected_1);
    assert_eq!(c2["ingester-zone-b-2"], 4_000_000);
    assert_eq!(c2["ingester-zone-b-3"], 2_000_000);
}

// Upstream: github.com/grafana/dskit/ring/ring_test.go::TestRing_Zones
#[test]
fn test_ring_zones() {
    let now_s = system_time_to_unix_secs(SystemTime::now());
    let cases = [
        (vec![], vec![]),
        (
            vec![
                ("i1", "zone-a", InstanceState::Active),
                ("i2", "zone-a", InstanceState::Leaving),
            ],
            vec!["zone-a"],
        ),
        (
            vec![
                ("i1", "zone-c", InstanceState::Active),
                ("i2", "zone-a", InstanceState::Active),
                ("i3", "zone-b", InstanceState::Active),
            ],
            vec!["zone-a", "zone-b", "zone-c"],
        ),
        (
            vec![
                ("i1", "zone-b", InstanceState::Active),
                ("i2", "zone-a", InstanceState::Leaving),
                ("i3", "zone-c", InstanceState::Pending),
                ("i4", "zone-d", InstanceState::Joining),
            ],
            vec!["zone-a", "zone-b", "zone-c", "zone-d"],
        ),
    ];

    for (items, expected) in cases {
        let desc = Desc {
            ingesters: items
                .into_iter()
                .map(|(id, z, st)| {
                    (
                        id.to_string(),
                        InstanceDesc {
                            addr: "127.0.0.1".into(),
                            zone: z.into(),
                            state: st as i32,
                            timestamp: now_s,
                            ..Default::default()
                        },
                    )
                })
                .collect(),
        };
        let ring = Ring::new_for_testing(make_settings(Duration::from_secs(3600), 1, true), Arc::new(DefaultReplicationStrategy));
        ring.set_ring_state_from_desc(desc, false, false, false);
        assert_eq!(ring.zones(), expected);
        assert_eq!(ring.zones_count(), expected.len());
    }
    let _ = Mutex::new(());
}
