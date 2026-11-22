//! Ported upstream dskit/ring core tests (part 2: spread_minimizing_token_generator, token_range, util).
//!
//! Ported from:
//! - github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go
//! - github.com/grafana/dskit/ring/token_range_test.go
//! - github.com/grafana/dskit/ring/util_test.go

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use mimir_common::backoff::BackoffError;
use mimir_common::services::ServiceError;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use mimir_ring::prng::GoRand;
use mimir_ring::ring::model::{system_time_to_unix_secs, DescExt};
use mimir_ring::ring::replication_set_tracker::RingContext;
use mimir_ring::ring::replication_strategy::{
    DefaultReplicationStrategy, Operation, ReplicationSet, ReplicationStrategy, REPORTING,
    WRITE_NO_EXTEND,
};
use mimir_ring::ring::token_range::{includes_key, token_ranges_equal, TokenRangesExt};
use mimir_ring::ring::util::{
    wait_instance_state, wait_ring_stability, wait_ring_tokens_stability, StringSet,
};
use mimir_ring::ring::{GetOptions, ReadRing, Ring, RingError, RingSettings};
use mimir_ring::token_gen::{
    find_zone_id, parse_instance_id, search_token, token_distance, tokens_equals,
    RandomTokenGenerator, RingToken, SpreadMinimizingTokenGenerator, TokenGenerator,
    OPTIMAL_TOKENS_PER_INSTANCE,
};
use parking_lot::Mutex;
use rand::Rng;

const TEST_INSTANCE: &str = "instance-zone-a-1000";
const TEST_ZONE: &str = "zone-a";

fn test_zones() -> Vec<String> {
    vec![
        "zone-a".to_string(),
        "zone-b".to_string(),
        "zone-c".to_string(),
    ]
}

fn create_smt_gen(instance: &str, zone: &str, zones: &[String]) -> SpreadMinimizingTokenGenerator {
    SpreadMinimizingTokenGenerator::new(instance, zone, zones, true).unwrap()
}

fn create_tokens_for_all_instances_and_zones(
    max_instance_id: usize,
    _tokens_per_instance: usize,
) -> (HashMap<u32, String>, HashMap<String, Vec<u32>>) {
    let zones = test_zones();
    let mut instance_by_token: HashMap<u32, String> = HashMap::new();
    let mut tokens_by_zone: HashMap<String, Vec<u32>> = HashMap::with_capacity(zones.len());

    for zone in &zones {
        let instance = format!("instance-{zone}-{max_instance_id}");
        let tg = create_smt_gen(&instance, zone, &zones);
        let by_id = tg.generate_tokens_by_instance_id().unwrap();
        let mut zone_tokens = Vec::new();
        for (id, mut tokens) in by_id.into_iter().enumerate() {
            tokens.sort_unstable();
            let inst_id = format!("instance-{zone}-{id}");
            for &t in &tokens {
                if let Some(prev) = instance_by_token.insert(t, inst_id.clone()) {
                    panic!("token {t} was already assigned to instance {prev}");
                }
            }
            zone_tokens.extend(tokens);
        }
        zone_tokens.sort_unstable();
        tokens_by_zone.insert(zone.clone(), zone_tokens);
    }

    (instance_by_token, tokens_by_zone)
}

// ============================================================================
// spread_minimizing_token_generator_test.go (15)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_ParseInstanceID
#[test]
fn test_spread_minimizing_token_generator_parse_instance_id() {
    let cases = [
        ("instance-zone-a-10", Ok(("instance-zone-a-", 10usize))),
        ("instance-zone-b-0", Ok(("instance-zone-b-", 0))),
        ("store-gateway-zone-c-7", Ok(("store-gateway-zone-c-", 7))),
        ("instance-zone-c", Err("unable to extract instance id from \"instance-zone-c\"")),
        ("", Err("unable to extract instance id from \"\"")),
    ];

    for (input, expected) in cases {
        let res = parse_instance_id(input);
        match expected {
            Ok((exp_prefix, exp_id)) => {
                let (prefix, id) = res.unwrap();
                assert_eq!(prefix, exp_prefix);
                assert_eq!(id, exp_id);
            }
            Err(exp_err) => {
                assert_eq!(res.unwrap_err().to_string(), exp_err);
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_FindZoneID
#[test]
fn test_spread_minimizing_token_generator_find_zone_id() {
    let zones = test_zones();
    assert_eq!(find_zone_id("zone-a", &zones).unwrap(), 0);
    assert_eq!(find_zone_id("zone-c", &zones).unwrap(), 2);
    assert_eq!(
        find_zone_id("zone-d", &zones).unwrap_err().to_string(),
        "zone zone-d is not valid"
    );
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_NewSpreadMinimizingTokenGenerator
#[test]
fn test_spread_minimizing_token_generator_new() {
    let z = |id: usize| format!("zone{id}");
    let cases = [
        (z(1), vec![], Some("number of zones 0 is not correct: it must be greater than 0 and less or equal than 8")),
        (
            z(1),
            (1..=9).map(z).collect::<Vec<_>>(),
            Some("number of zones 9 is not correct: it must be greater than 0 and less or equal than 8"),
        ),
        (z(1), vec![z(1), z(2)], None),
        (z(3), vec![z(1), z(2)], Some("zone zone3 is not valid")),
    ];

    for (zone, sm_zones, exp_err) in cases {
        let instance = format!("instance-{zone}-1");
        let res = SpreadMinimizingTokenGenerator::new(&instance, &zone, &sm_zones, true);
        match exp_err {
            None => assert!(res.is_ok()),
            Some(msg) => assert_eq!(res.unwrap_err().to_string(), msg),
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_GenerateFirstInstanceTokens
#[test]
fn test_spread_minimizing_token_generator_generate_first_instance_tokens() {
    let zones = test_zones();
    for (z, zone) in zones.iter().enumerate() {
        let instance = format!("instance-{zone}-10");
        let tg = create_smt_gen(&instance, zone, &zones);
        let tokens = tg.generate_first_instance_tokens();
        for (i, &token) in tokens.iter().enumerate() {
            assert_eq!(token, ((1u32 << 23) * (i as u32)) + (z as u32));
            assert_eq!(token % 8, z as u32);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_GenerateFirstInstanceTokensIdempotent
#[test]
fn test_spread_minimizing_token_generator_generate_first_instance_tokens_idempotent() {
    let zones = test_zones();
    for zone in &zones {
        let instance = format!("instance-{zone}-10");
        let tg = create_smt_gen(&instance, zone, &zones);
        let t1 = tg.generate_first_instance_tokens();
        assert_eq!(t1.len(), OPTIMAL_TOKENS_PER_INSTANCE);
        let t2 = tg.generate_first_instance_tokens();
        assert!(tokens_equals(&t1, &t2));
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_OptimalTokenOwnership
#[test]
fn test_spread_minimizing_token_generator_optimal_token_ownership() {
    let tg = create_smt_gen(TEST_INSTANCE, TEST_ZONE, &test_zones());
    assert_eq!(tg.optimal_token_ownership(1000.0, 900.0, 4), 24);
    assert_eq!(tg.optimal_token_ownership(1000.0, 990.0, 4), 0);
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_CalculateNewToken
#[test]
fn test_spread_minimizing_token_generator_calculate_new_token() {
    let tg = create_smt_gen(TEST_INSTANCE, TEST_ZONE, &test_zones());
    let cases = [
        (RingToken { token: 800, prev_token: 40 }, 80, Ok(120u32)),
        (RingToken { token: 801, prev_token: 41 }, 80, Ok(121)),
        (RingToken { token: 802, prev_token: 42 }, 80, Ok(122)),
        (RingToken { token: 416, prev_token: 4294967136 }, 240, Ok(88)),
        (RingToken { token: 417, prev_token: 4294967137 }, 240, Ok(89)),
        (RingToken { token: 418, prev_token: 4294967138 }, 240, Ok(90)),
        (RingToken { token: 416, prev_token: 4294967136 }, 120, Ok(4294967256)),
        (RingToken { token: 417, prev_token: 4294967137 }, 120, Ok(4294967257)),
        (RingToken { token: 418, prev_token: 4294967138 }, 120, Ok(4294967258)),
        (
            RingToken { token: 90, prev_token: 31 },
            80,
            Err("spread minimizing token calculation failed: calculation of a new token between 31 and 90 with optimal token ownership 80 was impossible: lower and upper bounds must be congruent modulo maximal allowed number of zones 8"),
        ),
        (
            RingToken { token: 240, prev_token: 80 },
            2,
            Err("spread minimizing token calculation failed: calculation of a new token between 80 and 240 with optimal token ownership 2 was impossible: optimal token ownership must be a positive multiple of maximal allowed number of zones 8"),
        ),
        (
            RingToken { token: 240, prev_token: 80 },
            42,
            Err("spread minimizing token calculation failed: calculation of a new token between 80 and 240 with optimal token ownership 42 was impossible: optimal token ownership must be a positive multiple of maximal allowed number of zones 8"),
        ),
        (
            RingToken { token: 240, prev_token: 80 },
            400,
            Err("spread minimizing token calculation failed: calculation of a new token between 80 and 240 with optimal token ownership 400 was impossible: distance between lower and upper bound 160 is not big enough"),
        ),
    ];

    for (rt, opt, expected) in cases {
        let res = tg.calculate_new_token(rt, opt);
        match expected {
            Ok(tok) => assert_eq!(res.unwrap(), tok),
            Err(msg) => assert_eq!(res.unwrap_err().to_string(), msg),
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_GenerateAllTokensIdempotent
#[test]
fn test_spread_minimizing_token_generator_generate_all_tokens_idempotent() {
    let zones = test_zones();
    for instance_id in 0..128 {
        for zone in &zones {
            let instance = format!("instance-{zone}-{instance_id}");
            let tg = create_smt_gen(&instance, zone, &zones);
            let t1 = tg.generate_all_tokens().unwrap();
            assert_eq!(t1.len(), OPTIMAL_TOKENS_PER_INSTANCE);
            let t2 = tg.generate_all_tokens().unwrap();
            assert!(tokens_equals(&t1, &t2));
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_VerifyTokensByZone
#[test]
fn test_spread_minimizing_token_generator_verify_tokens_by_zone() {
    let zones = test_zones();
    let instances_per_zone = 128;
    let (_, tokens_by_zone) =
        create_tokens_for_all_instances_and_zones(instances_per_zone, OPTIMAL_TOKENS_PER_INSTANCE);
    for z in 1..zones.len() {
        for (prev, curr) in tokens_by_zone[&zones[z - 1]]
            .iter()
            .zip(tokens_by_zone[&zones[z]].iter())
            .take(instances_per_zone * OPTIMAL_TOKENS_PER_INSTANCE)
        {
            assert_eq!(prev + 1, *curr);
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_VerifyInstanceOwnershipSpreadByZone
#[test]
fn test_spread_minimizing_token_generator_verify_instance_ownership_spread_by_zone() {
    let instances_per_zone = 1000;
    let (instance_by_token, tokens_by_zone) =
        create_tokens_for_all_instances_and_zones(instances_per_zone, OPTIMAL_TOKENS_PER_INSTANCE);

    for (_zone, tokens) in tokens_by_zone {
        if tokens.is_empty() {
            continue;
        }
        let mut ownership_by_instance: HashMap<&str, f64> =
            HashMap::with_capacity(instances_per_zone);
        let mut prev = tokens.len() - 1;
        for (tk, &token) in tokens.iter().enumerate() {
            let ownership = token_distance(tokens[prev], token) as f64;
            let inst_id = instance_by_token[&token].as_str();
            *ownership_by_instance.entry(inst_id).or_insert(0.0) += ownership;
            prev = tk;
        }
        let mut min_own = f64::MAX;
        let mut max_own = 0.0_f64;
        for &own in ownership_by_instance.values() {
            min_own = min_own.min(own);
            max_own = max_own.max(own);
        }
        let spread = 100.0 * (1.0 - min_own / max_own);
        assert!(spread < 0.2, "spread was {spread}");
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_CheckTokenUniqueness
#[test]
fn test_spread_minimizing_token_generator_check_token_uniqueness() {
    let zones = test_zones();
    let instance_id = 10_000;
    let mut all_tokens: HashSet<u32> =
        HashSet::with_capacity(OPTIMAL_TOKENS_PER_INSTANCE * (instance_id + 1) * zones.len());

    for zone in &zones {
        let instance = format!("instance-{zone}-{instance_id}");
        let tg = create_smt_gen(&instance, zone, &zones);
        let tokens = tg.generate_tokens_by_instance_id().unwrap();
        for tks in tokens.into_iter().take(instance_id + 1) {
            for token in tks {
                assert!(all_tokens.insert(token), "token {token} found more than once");
            }
        }
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_GenerateAtMost512Tokens
#[test]
fn test_spread_minimizing_token_generator_generate_at_most_512_tokens() {
    let tg = create_smt_gen(TEST_INSTANCE, TEST_ZONE, &test_zones());
    let tokens = tg.generate_tokens(2 * OPTIMAL_TOKENS_PER_INSTANCE, &[]);
    assert_eq!(tokens.len(), OPTIMAL_TOKENS_PER_INSTANCE);
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_GenerateTokens
#[test]
fn test_spread_minimizing_token_generator_generate_tokens() {
    let zones = test_zones();
    let zone = &zones[0];
    let instance = format!("instance-{zone}-1000");
    let tg = create_smt_gen(&instance, zone, &zones);
    let all_tokens = tg.generate_all_tokens().unwrap();
    assert_eq!(all_tokens.len(), OPTIMAL_TOKENS_PER_INSTANCE);

    let tokens_count = 300;
    let tokens = tg.generate_tokens(tokens_count, &[]);
    assert_eq!(tokens.len(), tokens_count);
    assert_eq!(tokens, all_tokens[..tokens_count]);

    let mut taken_tokens = tokens;
    let remaining_count = OPTIMAL_TOKENS_PER_INSTANCE - tokens_count;
    let remaining_tokens = tg.generate_tokens(remaining_count, &taken_tokens);
    assert_eq!(remaining_tokens.len(), remaining_count);
    assert_eq!(remaining_tokens, all_tokens[tokens_count..]);

    taken_tokens.extend(remaining_tokens);
    let no_tokens = tg.generate_tokens(1, &taken_tokens);
    assert!(no_tokens.is_empty());
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_GetMissingTokens
#[test]
fn test_spread_minimizing_token_generator_get_missing_tokens() {
    let tg = create_smt_gen(TEST_INSTANCE, TEST_ZONE, &test_zones());
    let mut rng = rand::thread_rng();
    let mut missing_indexes = Vec::with_capacity(3);
    for _ in 0..3 {
        let idx = rng.gen_range(0..OPTIMAL_TOKENS_PER_INSTANCE - 1);
        if !missing_indexes.contains(&idx) {
            missing_indexes.push(idx);
        }
    }
    missing_indexes.sort_unstable();

    let all_tokens = tg.generate_tokens(OPTIMAL_TOKENS_PER_INSTANCE, &[]);
    let taken_tokens: Vec<u32> = all_tokens
        .iter()
        .enumerate()
        .filter(|(i, _)| !missing_indexes.contains(i))
        .map(|(_, &t)| t)
        .collect();

    let tokens = tg.generate_tokens(missing_indexes.len(), &taken_tokens);
    assert_eq!(tokens.len(), missing_indexes.len());
    for (i, &missing_idx) in missing_indexes.iter().enumerate() {
        assert_eq!(tokens[i], all_tokens[missing_idx]);
    }
}

// Upstream: github.com/grafana/dskit/ring/spread_minimizing_token_generator_test.go::TestSpreadMinimizingTokenGenerator_CanJoin
#[test]
fn test_spread_minimizing_token_generator_can_join() {
    let zones = test_zones();
    let zone = &zones[0];

    let first_instance = format!("instance-{zone}-0");
    let mut tg = create_smt_gen(&first_instance, zone, &zones);
    tg.can_join_enabled = true;
    assert!(tg.can_join(&HashMap::new()).is_ok());

    let instance_id = 128usize;
    let target_instance = format!("instance-{zone}-{instance_id}");
    let mut tg = create_smt_gen(&target_instance, zone, &zones);
    let all_tokens = tg.generate_tokens_by_instance_id().unwrap();
    assert_eq!(all_tokens.len(), instance_id + 1);

    let mut ring_desc = Desc::default();
    for (i, toks) in all_tokens.iter().take(instance_id).enumerate() {
        let inst = format!("instance-{zone}-{i}");
        let (state, tokens) = if i <= instance_id - 2 {
            (InstanceState::Active, toks.clone())
        } else {
            (InstanceState::Pending, Vec::new())
        };
        ring_desc.add_ingester(
            &inst,
            &inst,
            zone,
            tokens,
            state,
            Some(SystemTime::now()),
            false,
            None,
            HashMap::new(),
        );
    }

    let pending_id = instance_id - 1;
    let pending_instance = format!("instance-{zone}-{pending_id}");
    assert!(ring_desc.ingesters[&pending_instance].tokens.is_empty());

    tg.can_join_enabled = false;
    assert!(tg.can_join(&ring_desc.ingesters).is_ok());

    tg.can_join_enabled = true;
    let err = tg.can_join(&ring_desc.ingesters).unwrap_err();
    assert_eq!(
        err.to_string(),
        format!("the instance {pending_instance:?} has not been registered to the ring or has no tokens yet")
    );

    let p = ring_desc.ingesters.get_mut(&pending_instance).unwrap();
    p.state = InstanceState::Active as i32;
    p.tokens = all_tokens[pending_id].clone();
    assert!(tg.can_join(&ring_desc.ingesters).is_ok());
}

// ============================================================================
// token_range_test.go (4)
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/token_range_test.go::TestKeyInTokenRanges
#[test]
fn test_key_in_token_ranges() {
    let ranges = vec![4u32, 8, 12, 16];
    assert!(!ranges.includes_key(0));
    assert!(ranges.includes_key(4));
    assert!(ranges.includes_key(6));
    assert!(ranges.includes_key(8));
    assert!(!ranges.includes_key(10));
    assert!(!ranges.includes_key(20));
    assert!(!includes_key(&[], 5));
}

// Upstream: github.com/grafana/dskit/ring/token_range_test.go::TestTokenRangesEqual
#[test]
fn test_token_ranges_equal() {
    let ranges = vec![4u32, 8, 12, 16];
    assert!(ranges.equal(&[4, 8, 12, 16]));
    assert!(!ranges.equal(&[4, 8]));
    assert!(!ranges.equal(&[4, 8, 12, 17]));
    assert!(!ranges.equal(&[4, 8, 12, 17, 20, 25]));
    assert!(!token_ranges_equal(&ranges, &[]));
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

fn gen_ring_instances(
    gen: &dyn TokenGenerator,
    num_instances: usize,
    num_zones: usize,
    num_tokens: usize,
) -> HashMap<String, InstanceDesc> {
    let mut instances = HashMap::with_capacity(num_instances);
    let mut all_tokens = Vec::new();
    for i in 1..=num_instances {
        let id = format!("instance-{i}");
        let zone = format!("zone-{}", i % num_zones);
        let new_tokens = gen.generate_tokens(num_tokens, &all_tokens);
        all_tokens.extend_from_slice(&new_tokens);
        instances.insert(
            id.clone(),
            gen_ring_instance_with_info(&id, &zone, new_tokens, Some(SystemTime::now())),
        );
    }
    instances
}

// Upstream: github.com/grafana/dskit/ring/token_range_test.go::TestGetTokenRangesForInstance
#[test]
fn test_get_token_ranges_for_instance() {
    let num_zones = 3;
    let gen = RandomTokenGenerator::with_seed(42);

    type RangeCase<'a> = (&'a str, Vec<(&'a str, Vec<u32>)>, Vec<(&'a str, Vec<u32>)>);
    let cases: Vec<RangeCase<'_>> = vec![
        (
            "single instance in zone",
            vec![("instance-0-0", gen.generate_tokens(512, &[]))],
            vec![("instance-0-0", vec![0, u32::MAX])],
        ),
        (
            "simple ranges",
            vec![
                ("instance-0-0", vec![25, 75]),
                ("instance-0-1", vec![10, 50, 100]),
            ],
            vec![
                ("instance-0-0", vec![10, 24, 50, 74]),
                ("instance-0-1", vec![0, 9, 25, 49, 75, u32::MAX]),
            ],
        ),
        (
            "grouped tokens",
            vec![
                ("instance-0-0", vec![10, 20, 30, 40, 50]),
                ("instance-0-1", vec![1000, 2000, 3000, 4000]),
            ],
            vec![
                ("instance-0-0", vec![0, 49, 4000, u32::MAX]),
                ("instance-0-1", vec![50, 3999]),
            ],
        ),
        (
            "consecutive tokens",
            vec![("instance-0-0", vec![99]), ("instance-0-1", vec![100])],
            vec![
                ("instance-0-0", vec![0, 98, 100, u32::MAX]),
                ("instance-0-1", vec![99, 99]),
            ],
        ),
        (
            "extremes",
            vec![
                ("instance-0-0", vec![0]),
                ("instance-0-1", vec![u32::MAX]),
            ],
            vec![
                ("instance-0-0", vec![u32::MAX, u32::MAX]),
                ("instance-0-1", vec![0, u32::MAX - 1]),
            ],
        ),
    ];

    for (name, zone_tokens, expected) in cases {
        let mut instances = HashMap::new();
        let mut all_tokens = Vec::new();
        for (id, tokens) in &zone_tokens {
            instances.insert(
                (*id).to_string(),
                gen_ring_instance_with_info(id, "zone-0", tokens.clone(), Some(SystemTime::now())),
            );
            all_tokens.extend_from_slice(tokens);
        }
        for z in 1..num_zones {
            for i in 0..zone_tokens.len() {
                let id = format!("instance-{z}-{i}");
                let tokens = gen.generate_tokens(512, &all_tokens);
                instances.insert(
                    id.clone(),
                    gen_ring_instance_with_info(
                        &id,
                        &format!("zone-{z}"),
                        tokens.clone(),
                        Some(SystemTime::now()),
                    ),
                );
                all_tokens.extend_from_slice(&tokens);
            }
        }

        let desc = Desc {
            ingesters: instances,
        };
        let ring = Ring::from_desc(
            RingSettings {
                heartbeat_timeout: Duration::from_secs(3600),
                replication_factor: num_zones,
                zone_awareness_enabled: true,
                excluded_zones: Vec::new(),
                subring_cache_disabled: true,
            },
            desc.clone(),
            Arc::new(DefaultReplicationStrategy),
        )
        .unwrap();

        let z0_tokens = desc.get_tokens_by_zone().remove("zone-0").unwrap();
        let mut tok_owner = HashMap::new();
        for (id, inst) in &desc.ingesters {
            for &t in &inst.tokens {
                tok_owner.insert(t, id.clone());
            }
        }

        for (id, exp) in expected {
            let ranges = ring.get_token_ranges_for_instance(id).unwrap();
            assert_eq!(ranges, exp, "{name}: {id}");
            for &t in &ranges {
                let idx = search_token(&z0_tokens, t);
                assert_eq!(tok_owner[&z0_tokens[idx]], id);
            }
        }
    }
}

struct NopReplicationStrategy;

impl ReplicationStrategy for NopReplicationStrategy {
    fn filter(
        &self,
        instances: Vec<InstanceDesc>,
        _op: Operation,
        _replication_factor: usize,
        _heartbeat_timeout: Duration,
        _zone_awareness_enabled: bool,
        _now: SystemTime,
    ) -> Result<(Vec<InstanceDesc>, usize), RingError> {
        Ok((instances, 0))
    }

    fn supports_expanded_replication(&self) -> bool {
        false
    }
}

// Upstream: github.com/grafana/dskit/ring/token_range_test.go::TestCheckingOfKeyOwnership
#[test]
fn test_checking_of_key_ownership() {
    for randomize in [false, true] {
        const INSTANCES_PER_ZONE: usize = 20;
        const NUM_ZONES: usize = 3;
        const NUM_TOKENS: usize = 512;
        const RF: usize = NUM_ZONES;

        let seed = 1_700_000_000i64 + if randomize { 1 } else { 0 };
        let gen = RandomTokenGenerator::with_seed(seed);
        let mut state_rng = GoRand::new(seed);

        let mut user_tokens = HashMap::new();
        let mut shard_sizes = HashMap::new();
        for cnt in [1000usize, 5000, 10_000, 25_000, 50_000, 100_000] {
            let uid = format!("{}k", cnt / 1000);
            user_tokens.insert(uid.clone(), gen.generate_tokens(cnt, &[]));
            let mut shard_size = ((cnt / 7500) / NUM_ZONES) * NUM_ZONES;
            if shard_size < NUM_ZONES {
                shard_size = NUM_ZONES;
            }
            shard_sizes.insert(uid, shard_size);
        }

        let mut desc = Desc {
            ingesters: gen_ring_instances(&gen, INSTANCES_PER_ZONE * NUM_ZONES, NUM_ZONES, NUM_TOKENS),
        };
        if randomize {
            for ing in desc.ingesters.values_mut() {
                ing.state = state_rng.int31n(InstanceState::Left as i32);
            }
        }

        let ring = Ring::from_desc(
            RingSettings {
                heartbeat_timeout: Duration::from_secs(3600),
                replication_factor: RF,
                zone_awareness_enabled: true,
                excluded_zones: Vec::new(),
                subring_cache_disabled: false,
            },
            desc,
            Arc::new(NopReplicationStrategy),
        )
        .unwrap();

        for (uid, tokens) in &user_tokens {
            let shard_size = shard_sizes[uid];
            let sub_ring = ring.shuffle_shard(uid, shard_size);
            let sub_desc = sub_ring.desc_snapshot();
            for instance_id in sub_desc.ingesters.keys() {
                let ranges = sub_ring.get_token_ranges_for_instance(instance_id).unwrap();
                let cnt_via_tokens = tokens.iter().filter(|&&t| ranges.includes_key(t)).count();

                let mut cnt_via_get = 0usize;
                if sub_ring.has_instance(instance_id) {
                    for &tok in tokens {
                        let s = sub_ring.get(tok, WRITE_NO_EXTEND).unwrap();
                        if s.instances.iter().any(|i| i.id == *instance_id) {
                            cnt_via_get += 1;
                        }
                    }
                }
                assert_eq!(
                    cnt_via_tokens, cnt_via_get,
                    "randomize={randomize} user={uid} instance={instance_id}"
                );
            }
        }
    }
}

// ============================================================================
// util_test.go (14)
// ============================================================================

struct RingMock {
    state_result: Mutex<Result<InstanceState, RingError>>,
    calls: AtomicUsize,
    last_instance_id: Mutex<Option<String>>,
}

impl RingMock {
    fn new(res: Result<InstanceState, RingError>) -> Self {
        Self {
            state_result: Mutex::new(res),
            calls: AtomicUsize::new(0),
            last_instance_id: Mutex::new(None),
        }
    }
}

impl ReadRing for RingMock {
    fn get(&self, _key: u32, _op: Operation) -> Result<ReplicationSet, RingError> {
        Ok(ReplicationSet::default())
    }
    fn get_with_options(
        &self,
        _key: u32,
        _op: Operation,
        _opts: GetOptions,
    ) -> Result<ReplicationSet, RingError> {
        Ok(ReplicationSet::default())
    }
    fn get_all_healthy(&self, _op: Operation) -> Result<ReplicationSet, RingError> {
        Ok(ReplicationSet::default())
    }
    fn get_replication_set_for_operation(&self, _op: Operation) -> Result<ReplicationSet, RingError> {
        Ok(ReplicationSet::default())
    }
    fn get_subring_for_operation_states(&self, _op: Operation) -> Arc<Ring> {
        Ring::new_for_testing(
            RingSettings {
                heartbeat_timeout: Duration::from_secs(60),
                replication_factor: 1,
                zone_awareness_enabled: false,
                excluded_zones: Vec::new(),
                subring_cache_disabled: false,
            },
            Arc::new(DefaultReplicationStrategy),
        )
    }
    fn replication_factor(&self) -> usize {
        0
    }
    fn instances_count(&self) -> usize {
        0
    }
    fn instances_with_tokens_count(&self) -> usize {
        0
    }
    fn get_instance_state(&self, instance_id: &str) -> Result<InstanceState, RingError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        *self.last_instance_id.lock() = Some(instance_id.to_string());
        match &*self.state_result.lock() {
            Ok(s) => Ok(*s),
            Err(_) => Err(RingError::InstanceNotFound),
        }
    }
    fn has_instance(&self, _instance_id: &str) -> bool {
        true
    }
    fn cleanup_shuffle_shard_cache(&self, _identifier: &str) {}
    fn get_token_ranges_for_instance(&self, _instance_id: &str) -> Result<Vec<u32>, RingError> {
        Ok(vec![0, u32::MAX])
    }
    fn instances_in_zone_count(&self, _zone: &str) -> usize {
        0
    }
    fn instances_with_tokens_in_zone_count(&self, _zone: &str) -> usize {
        0
    }
    fn writable_instances_with_tokens_count(&self) -> usize {
        0
    }
    fn writable_instances_with_tokens_in_zone_count(&self, _zone: &str) -> usize {
        0
    }
    fn zones_count(&self) -> usize {
        0
    }
    fn zones(&self) -> Vec<String> {
        Vec::new()
    }
}

fn create_starting_ring() -> Arc<Ring> {
    let now_s = system_time_to_unix_secs(SystemTime::now());
    let desc = Desc {
        ingesters: HashMap::from([
            ("instance-1".into(), InstanceDesc { id: "instance-1".into(), addr: "127.0.0.1".into(), state: InstanceState::Active as i32, timestamp: now_s, ..Default::default() }),
            ("instance-2".into(), InstanceDesc { id: "instance-2".into(), addr: "127.0.0.2".into(), state: InstanceState::Pending as i32, timestamp: now_s, ..Default::default() }),
            ("instance-3".into(), InstanceDesc { id: "instance-3".into(), addr: "127.0.0.3".into(), state: InstanceState::Joining as i32, timestamp: now_s, ..Default::default() }),
            ("instance-4".into(), InstanceDesc { id: "instance-4".into(), addr: "127.0.0.4".into(), state: InstanceState::Leaving as i32, timestamp: now_s, ..Default::default() }),
            ("instance-5".into(), InstanceDesc { id: "instance-5".into(), addr: "127.0.0.5".into(), state: InstanceState::Active as i32, timestamp: now_s, ..Default::default() }),
        ]),
    };
    let ring = Ring::new_for_testing(
        RingSettings {
            heartbeat_timeout: Duration::from_secs(60),
            replication_factor: 3,
            zone_awareness_enabled: false,
            excluded_zones: Vec::new(),
            subring_cache_disabled: false,
        },
        Arc::new(DefaultReplicationStrategy),
    );
    ring.set_ring_state_from_desc(desc, false, false, false);
    ring
}

fn spawn_add_instance_after(ring: Arc<Ring>, delay: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let mut desc = ring.desc_snapshot();
        let instance_id = format!("127.0.0.{}", desc.ingesters.len() + 1);
        desc.ingesters.insert(
            instance_id.clone(),
            InstanceDesc {
                addr: instance_id,
                state: InstanceState::Active as i32,
                timestamp: system_time_to_unix_secs(SystemTime::now()),
                ..Default::default()
            },
        );
        ring.set_ring_state_from_desc(desc, false, false, false);
    });
}

fn spawn_add_instances_periodically(ring: Arc<Ring>) -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    tokio::spawn(async move {
        while !stop_c.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(850)).await;
            if stop_c.load(Ordering::Acquire) {
                break;
            }
            let mut desc = ring.desc_snapshot();
            let instance_id = format!("127.0.0.{}", desc.ingesters.len() + 1);
            desc.ingesters.insert(
                instance_id.clone(),
                InstanceDesc {
                    id: instance_id.clone(),
                    addr: instance_id,
                    state: InstanceState::Active as i32,
                    timestamp: system_time_to_unix_secs(SystemTime::now()),
                    ..Default::default()
                },
            );
            ring.set_ring_state_from_desc(desc, false, false, false);
        }
    });
    stop
}

fn spawn_change_state_periodically(ring: Arc<Ring>) -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    tokio::spawn(async move {
        let states = [
            InstanceState::Pending,
            InstanceState::Joining,
            InstanceState::Active,
            InstanceState::Leaving,
        ];
        let mut idx = 0usize;
        {
            let desc = ring.desc_snapshot();
            let cur = desc.ingesters["instance-1"].state;
            while states[idx % states.len()] as i32 != cur {
                idx += 1;
            }
        }
        while !stop_c.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(850)).await;
            if stop_c.load(Ordering::Acquire) {
                break;
            }
            idx += 1;
            let mut desc = ring.desc_snapshot();
            if let Some(inst) = desc.ingesters.get_mut("instance-1") {
                inst.state = states[idx % states.len()] as i32;
                inst.timestamp = system_time_to_unix_secs(SystemTime::now());
            }
            ring.set_ring_state_from_desc(desc, false, false, false);
        }
    });
    stop
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingStability_ShouldReturnAsSoonAsMinStabilityIsReachedOnNoChanges
#[tokio::test]
async fn test_wait_ring_stability_should_return_as_soon_as_min_stability_is_reached_on_no_changes() {
    let min_stability = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(10);
    let ring = create_starting_ring();
    let start = Instant::now();
    wait_ring_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= min_stability, "elapsed {elapsed:?}");
    assert!(elapsed < 2 * min_stability, "elapsed {elapsed:?}");
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingTokensStability_ShouldReturnAsSoonAsMinStabilityIsReachedOnNoChanges
#[tokio::test]
async fn test_wait_ring_tokens_stability_should_return_as_soon_as_min_stability_is_reached_on_no_changes() {
    let min_stability = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(10);
    let ring = create_starting_ring();
    let start = Instant::now();
    wait_ring_tokens_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= min_stability, "elapsed {elapsed:?}");
    assert!(elapsed < 2 * min_stability, "elapsed {elapsed:?}");
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReached
#[tokio::test]
async fn test_wait_ring_stability_should_return_once_min_stability_of_instances_has_been_reached() {
    let min_stability = Duration::from_secs(3);
    let add_after = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(15);
    let ring = create_starting_ring();
    spawn_add_instance_after(Arc::clone(&ring), add_after);
    let start = Instant::now();
    wait_ring_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= min_stability + add_after, "elapsed {elapsed:?}");
    assert!(elapsed <= min_stability + add_after + Duration::from_secs(3), "elapsed {elapsed:?}");
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingTokensStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReached
#[tokio::test]
async fn test_wait_ring_tokens_stability_should_return_once_min_stability_of_instances_has_been_reached() {
    let min_stability = Duration::from_secs(3);
    let add_after = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(15);
    let ring = create_starting_ring();
    spawn_add_instance_after(Arc::clone(&ring), add_after);
    let start = Instant::now();
    wait_ring_tokens_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= min_stability + add_after, "elapsed {elapsed:?}");
    assert!(elapsed <= min_stability + add_after + Duration::from_secs(3), "elapsed {elapsed:?}");
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingStability_ShouldReturnErrorIfInstancesAddedAndMaxWaitingIsReached
#[tokio::test]
async fn test_wait_ring_stability_should_return_error_if_instances_added_and_max_waiting_is_reached() {
    let min_stability = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(7);
    let ring = create_starting_ring();
    let stop = spawn_add_instances_periodically(Arc::clone(&ring));
    let start = Instant::now();
    let err = wait_ring_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap_err();
    stop.store(true, Ordering::Release);
    assert_eq!(err, ServiceError::DeadlineExceeded);
    assert!(start.elapsed() >= max_waiting);
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingTokensStability_ShouldReturnErrorIfInstancesAddedAndMaxWaitingIsReached
#[tokio::test]
async fn test_wait_ring_tokens_stability_should_return_error_if_instances_added_and_max_waiting_is_reached() {
    let min_stability = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(7);
    let ring = create_starting_ring();
    let stop = spawn_add_instances_periodically(Arc::clone(&ring));
    let start = Instant::now();
    let err = wait_ring_tokens_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap_err();
    stop.store(true, Ordering::Release);
    assert_eq!(err, ServiceError::DeadlineExceeded);
    assert!(start.elapsed() >= max_waiting);
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingStability_ShouldReturnErrorIfInstanceStateIsChangingAndMaxWaitingIsReached
#[tokio::test]
async fn test_wait_ring_stability_should_return_error_if_instance_state_is_changing_and_max_waiting_is_reached() {
    let min_stability = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(7);
    let ring = create_starting_ring();
    let stop = spawn_change_state_periodically(Arc::clone(&ring));
    let start = Instant::now();
    let err = wait_ring_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap_err();
    stop.store(true, Ordering::Release);
    assert_eq!(err, ServiceError::DeadlineExceeded);
    assert!(start.elapsed() >= max_waiting);
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitRingTokensStability_ShouldReturnOnceMinStabilityOfInstancesHasBeenReachedWhileStateCanChange
#[tokio::test]
async fn test_wait_ring_tokens_stability_should_return_once_min_stability_of_instances_has_been_reached_while_state_can_change() {
    let min_stability = Duration::from_secs(2);
    let max_waiting = Duration::from_secs(7);
    let ring = create_starting_ring();
    let stop = spawn_change_state_periodically(Arc::clone(&ring));
    let start = Instant::now();
    wait_ring_tokens_stability(&RingContext::background(), ring.as_ref(), REPORTING, min_stability, max_waiting)
        .await
        .unwrap();
    stop.store(true, Ordering::Release);
    let elapsed = start.elapsed();
    assert!(elapsed >= min_stability, "elapsed {elapsed:?}");
    assert!(elapsed < 2 * min_stability, "elapsed {elapsed:?}");
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitInstanceState_Timeout
#[tokio::test]
async fn test_wait_instance_state_timeout() {
    let (ctx, _cancel) = RingContext::background().with_timeout(Duration::from_secs(1));
    let ring = RingMock::new(Ok(InstanceState::Active));
    let err = wait_instance_state(&ctx, &ring, "test", InstanceState::Pending)
        .await
        .unwrap_err();
    assert_eq!(err, BackoffError::DeadlineExceeded);
    assert_eq!(ring.last_instance_id.lock().as_deref(), Some("test"));
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitInstanceState_TimeoutOnError
#[tokio::test]
async fn test_wait_instance_state_timeout_on_error() {
    let (ctx, _cancel) = RingContext::background().with_timeout(Duration::from_secs(1));
    let ring = RingMock::new(Err(RingError::InstanceNotFound));
    let err = wait_instance_state(&ctx, &ring, "test", InstanceState::Active)
        .await
        .unwrap_err();
    assert_eq!(err, BackoffError::DeadlineExceeded);
    assert_eq!(ring.last_instance_id.lock().as_deref(), Some("test"));
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestWaitInstanceState_ExitsAfterActualStateEqualsState
#[tokio::test]
async fn test_wait_instance_state_exits_after_actual_state_equals_state() {
    let (ctx, _cancel) = RingContext::background().with_timeout(Duration::from_secs(1));
    let ring = RingMock::new(Ok(InstanceState::Active));
    wait_instance_state(&ctx, &ring, "test", InstanceState::Active)
        .await
        .unwrap();
    assert_eq!(ring.calls.load(Ordering::Acquire), 1);
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestGetTokenDistance
#[test]
fn test_get_token_distance() {
    let cases = [
        (10u32, 10u32, i64::from(u32::MAX) + 1),
        (0, 10, 10),
        (u32::MAX - 10, u32::MAX, 10),
        (u32::MAX, 0, 1),
        (u32::MAX - 10, 10, 21),
    ];
    for (from, to, expected) in cases {
        assert_eq!(token_distance(from, to), expected);
    }
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestSearchToken
#[test]
fn test_search_token() {
    let tokens = [3u32, 5];
    assert_eq!(search_token(&tokens, 0), 0);
    assert_eq!(search_token(&tokens, 3), 1);
    assert_eq!(search_token(&tokens, 4), 1);
    assert_eq!(search_token(&tokens, 5), 0);
    assert_eq!(search_token(&tokens, 7), 0);
}

// Upstream: github.com/grafana/dskit/ring/util_test.go::TestStringSet
#[test]
fn test_string_set() {
    {
        let s = StringSet::with_capacity(5);
        assert_eq!(s.len(), 0);
        assert!(!s.contains("anything"));
    }
    {
        let s = StringSet::with_capacity(0);
        assert_eq!(s.len(), 0);
        assert!(!s.contains("anything"));
    }
    {
        let mut s = StringSet::with_capacity(5);
        assert!(!s.is_map_mode());
        for elem in ["one", "two", "three"] {
            assert!(!s.contains(elem));
            s.add(elem.to_string());
            assert!(s.contains(elem));
        }
        assert_eq!(s.len(), 3);
        assert!(!s.contains("four"));
        assert!(!s.is_map_mode());
    }
    {
        let mut s = StringSet::with_capacity(3);
        s.add("one".into());
        s.add("two".into());
        s.add("three".into());
        assert!(!s.is_map_mode());
        s.add("four".into());
        assert!(s.is_map_mode());
        assert_eq!(s.len(), 4);
        for elem in ["one", "two", "three", "four"] {
            assert!(s.contains(elem));
        }
    }
    {
        let mut s = StringSet::with_capacity(2);
        s.add("one".into());
        s.add("two".into());
        s.add("three".into());
        assert!(s.is_map_mode());
        s.add("four".into());
        s.add("five".into());
        assert_eq!(s.len(), 5);
        for elem in ["one", "two", "three", "four", "five"] {
            assert!(s.contains(elem));
        }
        assert!(!s.contains("six"));
    }
    {
        let mut s = StringSet::with_capacity(0);
        assert!(!s.is_map_mode());
        s.add("one".into());
        assert!(s.is_map_mode());
        assert_eq!(s.len(), 1);
        assert!(s.contains("one"));
    }
    {
        let mut s = StringSet::with_capacity(1);
        s.add("one".into());
        assert!(!s.is_map_mode());
        s.add("two".into());
        assert!(s.is_map_mode());
        assert_eq!(s.len(), 2);
        assert!(s.contains("one"));
        assert!(s.contains("two"));
    }
    {
        let mut s = StringSet::with_capacity(10);
        for i in 0..1000 {
            let elem = format!("element-{i}");
            if !s.contains(&elem) {
                s.add(elem);
            }
        }
        assert_eq!(s.len(), 1000);
        assert!(s.is_map_mode());
        for i in 0..1000 {
            assert!(s.contains(&format!("element-{i}")));
        }
        assert!(!s.contains("nonexistent"));
    }
}
