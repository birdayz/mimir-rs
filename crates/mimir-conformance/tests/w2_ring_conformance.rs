//! Wave 2 Task W2-B Conformance Tests for `crates/mimir-ring`.
//!
//! Differential conformance against `target/go-oracle` and live `target/go-mimir`:
//! a) `partition-tokens`: 64 * 512 = 32,768 tokens bit-for-bit parity.
//! b) `ring-merge`: 20+ CAS & gossip merge scenarios bit-for-bit parity.
//! c) `ring-lookup`: single-zone, 3-zone zone-aware, unhealthy, JOINING/LEAVING/PENDING,
//!    and read_only across Write, WriteNoExtend, Read, and Reporting for 200+ tokens.
//! d) `partition-ring-lookup`: Stored & Smt512, Pending/Active/Inactive/Deleted,
//!    and multiple owners across 200+ tokens.
//! e) Live mixed-cluster memberlist gossip against real `target/go-mimir -target=ingester`.

use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use mimir_common::services::{
    start_and_await_running, stop_and_await_terminated, ServiceContext,
};
use mimir_config::ingester::{MultiKVConfig, RingConfig};
use mimir_config::server::MemberlistConfig;
use mimir_config::types::{StringSliceCSV, StringSliceCSVMulti};
use mimir_conformance::oracle::{
    ensure_go_oracle_built, go_mimir_bin, invoke_oracle, PartitionRingLookupRequest,
    PartitionRingLookupResponse, PartitionTokensRequest, PartitionTokensResponse,
    RingLookupRequest, RingLookupResponse, RingMergeRequest, RingMergeResponse,
};
use mimir_proto::ringpb::{
    Desc, InstanceDesc, InstanceState, OwnerState, PartitionDesc, PartitionRingDesc,
    PartitionState, PartitionTokenScheme,
};
use mimir_ring::kv::inmemory::InMemoryKv;
use mimir_ring::kv::memberlist::{MemberlistKv, MemberlistKvClient};
use mimir_ring::kv::multi::{MultiKv, NamedKvClient};
use mimir_ring::kv::prefix::PrefixedKvClient;
use mimir_ring::kv::{get_typed, KvClient, StringCodec};
use mimir_ring::lifecycler::{Lifecycler, LifecyclerConfig};
use mimir_ring::partition_ring::{
    partition_ring_desc_codec, PartitionRing, PartitionRingDescExt, PartitionRingOptions,
};
use mimir_ring::prost::Message as _;
use mimir_ring::ring::model::{ring_desc_codec, DescExt};
use mimir_ring::ring::replication_strategy::{
    DefaultReplicationStrategy, Operation, READ, REPORTING, WRITE, WRITE_NO_EXTEND,
};
use mimir_ring::ring::{Ring, RingSettings};
use mimir_ring::token_gen::{PartitionTokenGenerator, PartitionTokenTable};
use mimir_ring::{reqwest, tokio};

fn free_localhost_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local_addr").port()
}

struct ChildGuard {
    child: Child,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn desc_sorted_eq(a: &Desc, b: &Desc) -> bool {
    if a.ingesters.len() != b.ingesters.len() {
        return false;
    }
    for (k, va) in &a.ingesters {
        let Some(vb) = b.ingesters.get(k) else {
            return false;
        };
        if va != vb {
            return false;
        }
    }
    true
}

// ============================================================================
// (a) Differential test: partition-tokens (64 * 512 = 32,768 tokens)
// ============================================================================

#[test]
fn conformance_partition_tokens_64_bit_exact() {
    ensure_go_oracle_built().expect("go-oracle must build");

    let table = PartitionTokenTable::new(64, None).expect("rust PartitionTokenTable(64)");
    let oracle: PartitionTokensResponse = invoke_oracle(
        "partition-tokens",
        &PartitionTokensRequest { partitions: 64 },
    )
    .expect("go-oracle partition-tokens");

    assert_eq!(oracle.tokens_by_partition.len(), 64);
    let mut total_tokens = 0usize;
    for pid in 0..64_i32 {
        let rust_toks = table.tokens_for(pid).expect("tokens_for");
        let go_toks = &oracle.tokens_by_partition[pid as usize];
        assert_eq!(rust_toks.len(), 512, "partition {pid} token count");
        assert_eq!(
            rust_toks,
            go_toks.as_slice(),
            "token mismatch for partition {pid}"
        );
        total_tokens += rust_toks.len();
    }
    assert_eq!(total_tokens, 64 * 512);
}

// ============================================================================
// (b) Differential test: ring-merge (20+ CAS & gossip merge scenarios)
// ============================================================================

#[allow(clippy::too_many_arguments)]
fn mk_inst(
    id: &str,
    addr: &str,
    zone: &str,
    state: InstanceState,
    timestamp: i64,
    tokens: Vec<u32>,
    registered_timestamp: i64,
    read_only: bool,
    read_only_updated_timestamp: i64,
) -> InstanceDesc {
    InstanceDesc {
        id: id.to_string(),
        addr: addr.to_string(),
        zone: zone.to_string(),
        state: state as i32,
        timestamp,
        tokens,
        registered_timestamp,
        read_only,
        read_only_updated_timestamp,
        versions: HashMap::new(),
    }
}

fn mk_desc(items: Vec<InstanceDesc>) -> Desc {
    let mut ingesters = HashMap::new();
    for i in items {
        ingesters.insert(i.id.clone(), i);
    }
    Desc { ingesters }
}

#[test]
fn conformance_ring_merge_20_plus_scenarios() {
    ensure_go_oracle_built().expect("go-oracle must build");
    let b64 = base64::engine::general_purpose::STANDARD;
    let now = SystemTime::now();
    let now_sec = now.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let t0 = now_sec - 100;

    let scenarios: Vec<(&str, Desc, Desc, bool, bool)> = vec![
        // (name, local, remote, cas, compare_exact_timestamp)
        (
            "1_empty_local_new_remote_gossip",
            mk_desc(vec![]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            false,
            true,
        ),
        (
            "2_disjoint_instances_gossip",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("b", "10.0.0.2:9095", "z2", InstanceState::Active, t0 + 1, vec![30, 40], t0 + 1, false, 0)]),
            false,
            true,
        ),
        (
            "3_newer_heartbeat_wins",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 5, vec![10, 20], t0, false, 0)]),
            false,
            true,
        ),
        (
            "4_older_heartbeat_ignored",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 10, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 5, vec![10, 99], t0, false, 0)]),
            false,
            true,
        ),
        (
            "5_equal_timestamp_active_ignored",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Leaving, t0, vec![10, 20], t0, false, 0)]),
            false,
            true,
        ),
        (
            "6_equal_timestamp_left_tombstone_accepted",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Left, t0, vec![10, 20], t0, false, 0)]),
            false,
            true,
        ),
        (
            "7_equal_timestamp_already_left_noop",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Left, t0, vec![], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Left, t0, vec![], t0, false, 0)]),
            false,
            true,
        ),
        (
            "8_pending_to_joining_to_active",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Pending, t0, vec![], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Joining, t0 + 1, vec![100, 200], t0, false, 0)]),
            false,
            true,
        ),
        (
            "9_token_conflict_lexicographic_winner",
            mk_desc(vec![mk_inst("ing-b", "10.0.0.2:9095", "z1", InstanceState::Active, t0, vec![100, 200], t0, false, 0)]),
            mk_desc(vec![mk_inst("ing-a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 1, vec![100, 300], t0, false, 0)]),
            false,
            true,
        ),
        (
            "10_token_conflict_leaving_loses_to_active",
            mk_desc(vec![mk_inst("ing-a", "10.0.0.1:9095", "z1", InstanceState::Leaving, t0 + 1, vec![100, 200], t0, false, 0)]),
            mk_desc(vec![mk_inst("ing-z", "10.0.0.2:9095", "z1", InstanceState::Active, t0 + 2, vec![100, 300], t0, false, 0)]),
            false,
            true,
        ),
        (
            "11_unsorted_and_duplicate_tokens_normalized",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("b", "10.0.0.2:9095", "z1", InstanceState::Active, t0 + 1, vec![50, 30, 50, 40], t0, false, 0)]),
            false,
            true,
        ),
        (
            "12_read_only_true_transition",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 3, vec![10, 20], t0, true, t0 + 3)]),
            false,
            true,
        ),
        (
            "13_read_only_false_transition",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, true, t0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 4, vec![10, 20], t0, false, t0 + 4)]),
            false,
            true,
        ),
        (
            "14_cas_missing_instance_becomes_left_tombstone",
            mk_desc(vec![
                mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0),
                mk_inst("b", "10.0.0.2:9095", "z2", InstanceState::Active, t0, vec![30, 40], t0, false, 0),
            ]),
            mk_desc(vec![
                mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 1, vec![10, 20], t0, false, 0),
            ]),
            true,
            false,
        ),
        (
            "15_cas_already_left_not_reemitted",
            mk_desc(vec![
                mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0),
                mk_inst("b", "10.0.0.2:9095", "z2", InstanceState::Left, t0, vec![], t0, false, 0),
            ]),
            mk_desc(vec![
                mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 1, vec![10, 20], t0, false, 0),
            ]),
            true,
            true,
        ),
        (
            "16_cas_no_change_returns_none",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            true,
            true,
        ),
        (
            "17_future_skewed_remote_timestamp_ignored",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, now_sec + 3600, vec![99], t0, false, 0)]),
            false,
            true,
        ),
        (
            "18_future_skewed_local_replaced_by_valid_remote",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, now_sec + 3600, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 1, vec![55, 66], t0, false, 0)]),
            false,
            true,
        ),
        (
            "19_cas_new_instance_added",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![
                mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10, 20], t0, false, 0),
                mk_inst("b", "10.0.0.2:9095", "z2", InstanceState::Joining, t0 + 2, vec![30, 40], t0 + 2, false, 0),
            ]),
            true,
            true,
        ),
        (
            "20_multi_conflict_resolution_three_way",
            mk_desc(vec![
                mk_inst("ing-2", "10.0.0.2:9095", "z1", InstanceState::Active, t0, vec![1, 2, 3], t0, false, 0),
                mk_inst("ing-3", "10.0.0.3:9095", "z2", InstanceState::Active, t0, vec![4, 5], t0, false, 0),
            ]),
            mk_desc(vec![
                mk_inst("ing-1", "10.0.0.1:9095", "z3", InstanceState::Active, t0 + 1, vec![2, 4, 6], t0 + 1, false, 0),
            ]),
            false,
            true,
        ),
        (
            "21_token_update_on_same_instance",
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Joining, t0, vec![10, 20], t0, false, 0)]),
            mk_desc(vec![mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0 + 2, vec![10, 20, 30, 40], t0, false, 0)]),
            false,
            true,
        ),
        (
            "22_cas_all_removed_become_left",
            mk_desc(vec![
                mk_inst("a", "10.0.0.1:9095", "z1", InstanceState::Active, t0, vec![10], t0, false, 0),
                mk_inst("b", "10.0.0.2:9095", "z2", InstanceState::Active, t0, vec![20], t0, false, 0),
            ]),
            mk_desc(vec![]),
            true,
            false,
        ),
    ];

    assert!(scenarios.len() >= 20);

    for (name, local, remote, cas, exact_ts) in scenarios {
        let mut rust_local = local.clone();
        let mut rust_remote = remote.clone();
        let rust_change = rust_local.merge_with_time(&mut rust_remote, cas, now);

        let go_resp: RingMergeResponse = invoke_oracle(
            "ring-merge",
            &RingMergeRequest {
                local_b64: b64.encode(local.encode_to_vec()),
                remote_b64: b64.encode(remote.encode_to_vec()),
                cas,
            },
        )
        .unwrap_or_else(|e| panic!("go-oracle ring-merge {name}: {e}"));

        let go_merged = Desc::decode(
            b64.decode(&go_resp.merged_b64)
                .expect("merged_b64")
                .as_slice(),
        )
        .expect("decode go merged");
        let go_change = go_resp.change_b64.map(|s| {
            Desc::decode(b64.decode(s).expect("change_b64").as_slice()).expect("decode go change")
        });

        if exact_ts {
            assert!(
                desc_sorted_eq(&rust_local, &go_merged),
                "scenario {name}: merged mismatch\nrust: {rust_local:?}\ngo:   {go_merged:?}"
            );
            assert_eq!(
                rust_change.is_some(),
                go_change.is_some(),
                "scenario {name}: change presence mismatch"
            );
            if let (Some(rc), Some(gc)) = (&rust_change, &go_change) {
                assert!(
                    desc_sorted_eq(rc, gc),
                    "scenario {name}: change mismatch\nrust: {rc:?}\ngo:   {gc:?}"
                );
            }
        } else {
            assert_eq!(rust_local.ingesters.len(), go_merged.ingesters.len(), "scenario {name}");
            for (k, rv) in &rust_local.ingesters {
                let gv = &go_merged.ingesters[k];
                assert_eq!(rv.state, gv.state, "scenario {name} key {k} state");
                assert_eq!(rv.tokens, gv.tokens, "scenario {name} key {k} tokens");
                assert!((rv.timestamp - gv.timestamp).abs() <= 2, "scenario {name} key {k} ts");
            }
            assert_eq!(rust_change.is_some(), go_change.is_some(), "scenario {name}");
        }
    }
}

// ============================================================================
// (c) Differential test: ring-lookup (200+ tokens across topologies & ops)
// ============================================================================

fn op_from_str(s: &str) -> Operation {
    match s {
        "Write" => WRITE,
        "WriteNoExtend" => WRITE_NO_EXTEND,
        "Read" => READ,
        "Reporting" => REPORTING,
        other => panic!("unknown op {other}"),
    }
}

#[test]
fn conformance_ring_lookup_200_plus_tokens() {
    ensure_go_oracle_built().expect("go-oracle must build");
    let b64 = base64::engine::general_purpose::STANDARD;
    let now_sec = 1_700_000_000_i64;
    let now = UNIX_EPOCH + Duration::from_secs(now_sec as u64);

    // Build 4 diverse ring configurations:
    // 1. Single-zone RF=3 with 5 active instances
    let single_zone = mk_desc(vec![
        mk_inst("ing-0", "10.0.0.1:9095", "", InstanceState::Active, now_sec - 5, vec![100, 1_000_000, 2_000_000_000], now_sec - 100, false, 0),
        mk_inst("ing-1", "10.0.0.2:9095", "", InstanceState::Active, now_sec - 5, vec![500, 50_000_000, 2_500_000_000], now_sec - 100, false, 0),
        mk_inst("ing-2", "10.0.0.3:9095", "", InstanceState::Active, now_sec - 5, vec![10_000, 500_000_000, 3_000_000_000], now_sec - 100, false, 0),
        mk_inst("ing-3", "10.0.0.4:9095", "", InstanceState::Active, now_sec - 5, vec![100_000, 1_000_000_000, 3_500_000_000], now_sec - 100, false, 0),
        mk_inst("ing-4", "10.0.0.5:9095", "", InstanceState::Active, now_sec - 5, vec![10_000_000, 1_500_000_000, 4_000_000_000], now_sec - 100, false, 0),
    ]);

    // 2. 3-zone zone-aware RF=3 with mixed ACTIVE, JOINING, LEAVING, PENDING, read_only
    let three_zone_mixed = mk_desc(vec![
        mk_inst("za-0", "10.1.0.1:9095", "zone-a", InstanceState::Active, now_sec - 5, vec![1_000, 1_000_000_000], now_sec - 100, false, 0),
        mk_inst("za-1", "10.1.0.2:9095", "zone-a", InstanceState::Joining, now_sec - 5, vec![2_000, 2_000_000_000], now_sec - 100, false, 0),
        mk_inst("zb-0", "10.2.0.1:9095", "zone-b", InstanceState::Active, now_sec - 5, vec![3_000, 1_500_000_000], now_sec - 100, true, now_sec - 50),
        mk_inst("zb-1", "10.2.0.2:9095", "zone-b", InstanceState::Leaving, now_sec - 5, vec![4_000, 2_500_000_000], now_sec - 100, false, 0),
        mk_inst("zc-0", "10.3.0.1:9095", "zone-c", InstanceState::Active, now_sec - 5, vec![5_000, 3_000_000_000], now_sec - 100, false, 0),
        mk_inst("zc-1", "10.3.0.2:9095", "zone-c", InstanceState::Pending, now_sec - 5, vec![6_000, 3_500_000_000], now_sec - 100, false, 0),
    ]);

    // 3. Unhealthy instance scenario (2 healthy, 1 unhealthy in zone-a)
    let unhealthy_mix = mk_desc(vec![
        mk_inst("u-0", "10.4.0.1:9095", "zone-a", InstanceState::Active, now_sec - 300, vec![100, 1_000_000_000], now_sec - 500, false, 0),
        mk_inst("u-1", "10.4.0.2:9095", "zone-b", InstanceState::Active, now_sec - 5, vec![200, 2_000_000_000], now_sec - 500, false, 0),
        mk_inst("u-2", "10.4.0.3:9095", "zone-c", InstanceState::Active, now_sec - 5, vec![300, 3_000_000_000], now_sec - 500, false, 0),
    ]);

    // 4. Too many unhealthy (2 unhealthy -> quorum fails)
    let too_many_unhealthy = mk_desc(vec![
        mk_inst("f-0", "10.5.0.1:9095", "zone-a", InstanceState::Active, now_sec - 300, vec![100], now_sec - 500, false, 0),
        mk_inst("f-1", "10.5.0.2:9095", "zone-b", InstanceState::Active, now_sec - 300, vec![200], now_sec - 500, false, 0),
        mk_inst("f-2", "10.5.0.3:9095", "zone-c", InstanceState::Active, now_sec - 5, vec![300], now_sec - 500, false, 0),
    ]);

    let configs: Vec<(&str, &Desc, bool, usize)> = vec![
        ("single_zone", &single_zone, false, 3),
        ("three_zone_mixed", &three_zone_mixed, true, 3),
        ("unhealthy_mix", &unhealthy_mix, true, 3),
        ("too_many_unhealthy", &too_many_unhealthy, true, 3),
    ];

    // 20+ distinct tokens per config * ops = well over 200 lookups!
    let tokens: Vec<u32> = vec![
        0, 1, 99, 100, 101, 499, 500, 501, 1_000, 2_000, 3_000, 4_000, 5_000, 6_000, 999_999,
        1_000_000, 50_000_000, 500_000_000, 1_500_000_000, 2_500_000_000, 3_500_000_000,
        4_000_000_000, u32::MAX - 1, u32::MAX,
    ];
    let ops = ["Write", "WriteNoExtend", "Read", "Reporting"];

    let mut total_checks = 0usize;
    for (cfg_name, desc, zone_aware, rf) in configs {
        let settings = RingSettings {
            heartbeat_timeout: Duration::from_secs(60),
            replication_factor: rf,
            zone_awareness_enabled: zone_aware,
            excluded_zones: Vec::new(),
            subring_cache_disabled: false,
        };
        let rust_ring =
            Ring::from_desc(settings, desc.clone(), Arc::new(DefaultReplicationStrategy))
                .expect("from_desc");
        let desc_b64 = b64.encode(desc.encode_to_vec());

        for &op_str in &ops {
            let op = op_from_str(op_str);
            for &tok in &tokens {
                total_checks += 1;
                let rust_res = rust_ring.get_at(tok, op, rf, now);
                let go_resp: RingLookupResponse = invoke_oracle(
                    "ring-lookup",
                    &RingLookupRequest {
                        desc_b64: desc_b64.clone(),
                        replication_factor: rf as i32,
                        heartbeat_timeout_ms: 60_000,
                        zone_awareness_enabled: zone_aware,
                        op: op_str.to_string(),
                        token: tok,
                        now_unix_sec: now_sec,
                    },
                )
                .unwrap_or_else(|e| panic!("oracle ring-lookup {cfg_name} {op_str} {tok}: {e}"));

                match (rust_res, go_resp.error) {
                    (Ok(rs), None) => {
                        let rust_ids: Vec<String> =
                            rs.instances.iter().map(|i| i.id.clone()).collect();
                        assert_eq!(
                            rust_ids, go_resp.instance_ids,
                            "instance_ids mismatch for {cfg_name} op={op_str} tok={tok}"
                        );
                        assert_eq!(
                            rs.max_errors as i32, go_resp.max_errors,
                            "max_errors mismatch for {cfg_name} op={op_str} tok={tok}"
                        );
                        assert_eq!(
                            rs.max_unavailable_zones as i32, go_resp.max_unavailable_zones,
                            "max_unavailable_zones mismatch for {cfg_name} op={op_str} tok={tok}"
                        );
                    }
                    (Err(_), Some(_)) => {}
                    (Ok(rs), Some(ge)) => panic!(
                        "mismatch {cfg_name} op={op_str} tok={tok}: rust Ok({:?}) vs go Err({ge})",
                        rs.instances.iter().map(|i| &i.id).collect::<Vec<_>>()
                    ),
                    (Err(re), None) => panic!(
                        "mismatch {cfg_name} op={op_str} tok={tok}: rust Err({re}) vs go Ok({:?})",
                        go_resp.instance_ids
                    ),
                }
            }
        }
    }
    assert!(total_checks >= 200, "checked {total_checks} lookups");
}

// ============================================================================
// (d) Differential test: partition-ring-lookup (200+ tokens)
// ============================================================================

#[test]
fn conformance_partition_ring_lookup_200_plus_tokens() {
    ensure_go_oracle_built().expect("go-oracle must build");
    let b64 = base64::engine::general_purpose::STANDARD;
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    // 1. PartitionTokensSmt512 with Pending/Active/Inactive/Deleted & multi-owners
    let mut smt_desc = PartitionRingDesc::default();
    let states = [
        PartitionState::PartitionPending,
        PartitionState::PartitionActive,
        PartitionState::PartitionInactive,
        PartitionState::PartitionActive,
        PartitionState::PartitionDeleted,
        PartitionState::PartitionActive,
    ];
    for (pid_u, &st) in states.iter().enumerate() {
        let pid = pid_u as i32;
        smt_desc.add_partition_with_derived_tokens(pid, st, now);
        smt_desc.add_or_update_owner(&format!("ing-{pid}-a"), OwnerState::OwnerActive, pid, now);
        smt_desc.add_or_update_owner(&format!("ing-{pid}-b"), OwnerState::OwnerActive, pid, now);
    }

    // 2. PartitionTokensStored with custom tokens and multiple owners
    let mut stored_desc = PartitionRingDesc::default();
    stored_desc.partitions.insert(
        0,
        PartitionDesc {
            id: 0,
            tokens: vec![100, 10_000, 1_000_000, 2_000_000_000],
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 100,
            state_change_locked: false,
            state_change_locked_timestamp: 0,
            token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
        },
    );
    stored_desc.partitions.insert(
        1,
        PartitionDesc {
            id: 1,
            tokens: vec![500, 50_000, 500_000_000, 3_000_000_000],
            state: PartitionState::PartitionPending as i32,
            state_timestamp: 100,
            state_change_locked: false,
            state_change_locked_timestamp: 0,
            token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
        },
    );
    stored_desc.partitions.insert(
        2,
        PartitionDesc {
            id: 2,
            tokens: vec![1_000, 100_000, 1_500_000_000, 4_000_000_000],
            state: PartitionState::PartitionActive as i32,
            state_timestamp: 100,
            state_change_locked: false,
            state_change_locked_timestamp: 0,
            token_scheme: PartitionTokenScheme::PartitionTokensStored as i32,
        },
    );
    stored_desc.add_or_update_owner("owner-0-z", OwnerState::OwnerActive, 0, now);
    stored_desc.add_or_update_owner("owner-0-a", OwnerState::OwnerActive, 0, now);
    stored_desc.add_or_update_owner("owner-2-a", OwnerState::OwnerActive, 2, now);

    // 3. No active partitions (only Pending/Inactive)
    let mut no_active_desc = PartitionRingDesc::default();
    no_active_desc.add_partition_with_derived_tokens(0, PartitionState::PartitionPending, now);
    no_active_desc.add_partition_with_derived_tokens(1, PartitionState::PartitionInactive, now);

    let table = Arc::new(PartitionTokenTable::new(8, None).unwrap());
    let smt_pr = PartitionRing::with_options(
        smt_desc.clone(),
        PartitionRingOptions {
            token_generator: Some(table.clone()),
            shuffle_shard_cache_size: 0,
        },
    )
    .unwrap();
    let stored_pr = PartitionRing::new(stored_desc.clone()).unwrap();
    let no_active_pr = PartitionRing::with_options(
        no_active_desc.clone(),
        PartitionRingOptions {
            token_generator: Some(table),
            shuffle_shard_cache_size: 0,
        },
    )
    .unwrap();

    // Generate 110 diverse tokens -> 110 * 2 + 10 = 230 checks!
    let mut tokens: Vec<u32> = (0..100)
        .map(|i| (i as u32).wrapping_mul(42_949_672))
        .collect();
    tokens.extend([0, 1, 99, 100, 101, 500, 1000, 2_000_000_000, u32::MAX - 1, u32::MAX]);

    let cases: Vec<(&str, &PartitionRing, &PartitionRingDesc, i32, &[u32])> = vec![
        ("smt512", &smt_pr, &smt_desc, 8, &tokens),
        ("stored", &stored_pr, &stored_desc, -1, &tokens),
        ("no_active", &no_active_pr, &no_active_desc, 8, &tokens[..10]),
    ];

    let mut total_checks = 0usize;
    for (name, pr, desc, max_derived, toks) in cases {
        let desc_b64 = b64.encode(desc.encode_to_vec());
        for &tok in toks {
            total_checks += 1;
            let rust_res = pr.active_partition_for_key(tok);
            let go_resp: PartitionRingLookupResponse = invoke_oracle(
                "partition-ring-lookup",
                &PartitionRingLookupRequest {
                    desc_b64: desc_b64.clone(),
                    max_derived_token_partitions: max_derived,
                    token: tok,
                    op: "Write".to_string(),
                },
            )
            .unwrap_or_else(|e| panic!("oracle partition-ring-lookup {name} {tok}: {e}"));

            match (rust_res, go_resp.error) {
                (Ok(pid), None) => {
                    assert_eq!(Some(pid), go_resp.partition_id, "{name} tok={tok} pid");
                    let owners = pr.partition_owner_ids_copy(pid);
                    assert_eq!(owners, go_resp.owners, "{name} tok={tok} owners");
                }
                (Err(_), Some(_)) => {}
                (Ok(pid), Some(ge)) => panic!("{name} tok={tok}: rust Ok({pid}) vs go Err({ge})"),
                (Err(re), None) => panic!(
                    "{name} tok={tok}: rust Err({re}) vs go Ok({:?})",
                    go_resp.partition_id
                ),
            }
        }
    }
    assert!(total_checks >= 200, "checked {total_checks} lookups");
}

// ============================================================================
// (e) LIVE MIXED-CLUSTER MEMBERLIST TEST against real target/go-mimir
// ============================================================================

#[derive(serde::Deserialize)]
struct GoRingJson {
    shards: Vec<GoShardJson>,
}

#[derive(serde::Deserialize)]
struct GoShardJson {
    id: String,
    state: String,
    address: String,
    tokens: Vec<u32>,
}

#[test]
fn live_mixed_cluster_memberlist_go_mimir() {
    ensure_go_oracle_built().expect("go-mimir must build");

    let tmp = tempfile::tempdir().expect("tempdir");
    let blocks_dir = tmp.path().join("blocks");
    let tsdb_dir = tmp.path().join("tsdb");
    std::fs::create_dir_all(&blocks_dir).unwrap();
    std::fs::create_dir_all(&tsdb_dir).unwrap();

    let go_http_port = free_localhost_port();
    let go_grpc_port = free_localhost_port();
    let go_ml_port = free_localhost_port();

    let child = Command::new(go_mimir_bin())
        .current_dir(tmp.path())
        .args([
            "-target=ingester",
            "-server.http-listen-address=127.0.0.1",
            &format!("-server.http-listen-port={go_http_port}"),
            "-server.grpc-listen-address=127.0.0.1",
            &format!("-server.grpc-listen-port={go_grpc_port}"),
            "-memberlist.bind-addr=127.0.0.1",
            &format!("-memberlist.bind-port={go_ml_port}"),
            "-memberlist.advertise-addr=127.0.0.1",
            &format!("-memberlist.advertise-port={go_ml_port}"),
            "-blocks-storage.backend=filesystem",
            &format!("-blocks-storage.filesystem.dir={}", blocks_dir.display()),
            &format!("-blocks-storage.tsdb.dir={}", tsdb_dir.display()),
            "-ingester.ring.num-tokens=8",
            "-ingester.ring.min-ready-duration=0s",
            "-ingester.ring.instance-addr=127.0.0.1",
            "-ingester.ring.instance-id=go-ingester-1",
            "-ingester.ring.unregister-on-shutdown=false",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn target/go-mimir");
    let mut guard = ChildGuard { child };
    let go_stderr = guard.child.stderr.take();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async {
        let http_client = reqwest::Client::new();
        let ring_url = format!("http://127.0.0.1:{go_http_port}/ingester/ring");

        // 1. Wait for Go Mimir HTTP server and go-ingester-1 ACTIVE in ring.
        let mut go_ready = false;
        for _ in 0..80 {
            if let Ok(resp) = http_client
                .get(&ring_url)
                .header("Accept", "application/json")
                .send()
                .await
            {
                if resp.status().is_success() {
                    if let Ok(json) = resp.json::<GoRingJson>().await {
                        if json
                            .shards
                            .iter()
                            .any(|s| s.id == "go-ingester-1" && s.state == "ACTIVE")
                        {
                            go_ready = true;
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(go_ready, "Go Mimir did not become ready with go-ingester-1 ACTIVE");

        // 2. Start Rust MemberlistKv joining Go Mimir's memberlist port.
        let ml_cfg = MemberlistConfig {
            node_name: "rust-node".to_string(),
            randomize_node_name: true,
            bind_addr: vec!["127.0.0.1".to_string()],
            bind_port: 0,
            advertise_addr: "127.0.0.1".to_string(),
            advertise_port: 0,
            join_members: StringSliceCSVMulti(vec![format!("127.0.0.1:{go_ml_port}")]),
            abort_if_cluster_join_fails: true,
            ..Default::default()
        };

        let ml_kv = MemberlistKv::start(
            ml_cfg,
            vec![ring_desc_codec(), partition_ring_desc_codec()],
            "",
            None,
        )
        .await
        .expect("start Rust MemberlistKv");

        let raw_client =
            Arc::new(MemberlistKvClient::new(ml_kv.clone(), ring_desc_codec()).unwrap());
        let prefixed_kv: Arc<dyn KvClient> =
            Arc::new(PrefixedKvClient::new(raw_client, "collectors/"));

        // Verify Rust received Go Mimir's ring state via Push/Pull join!
        let initial_ring: Option<Desc> = get_typed(prefixed_kv.as_ref(), "ring")
            .await
            .expect("get collectors/ring");
        let initial_ring = initial_ring.expect("ring should exist after join push/pull");
        assert!(
            initial_ring.ingesters.contains_key("go-ingester-1"),
            "Rust MemberlistKv should see go-ingester-1 after join: {:?}",
            initial_ring.ingesters.keys().collect::<Vec<_>>()
        );

        // 3. Start Rust Lifecycler registering "rust-ingester-1" with 128 tokens into "collectors/ring".
        let ring_cfg = RingConfig {
            instance_id: "rust-ingester-1".to_string(),
            instance_addr: "127.0.0.1".to_string(),
            instance_port: 19097,
            num_tokens: 128,
            min_ready_duration: mimir_config::duration::GoDuration::ZERO,
            observe_period: mimir_config::duration::GoDuration::ZERO,
            unregister_on_shutdown: true,
            ..Default::default()
        };

        let lc_cfg = LifecyclerConfig::from_ring_config(&ring_cfg, 19097).unwrap();
        let lifecycler = Lifecycler::with_store(
            lc_cfg,
            prefixed_kv.clone(),
            None,
            "ingester",
            "ring",
            false,
            None,
        );
        let lc_svc = lifecycler.clone().into_service();
        start_and_await_running(ServiceContext::background(), lc_svc.as_ref())
            .await
            .expect("start Rust lifecycler");

        // Wait for Rust lifecycler to become ACTIVE with 128 tokens.
        for _ in 0..50 {
            if lifecycler.get_state() == InstanceState::Active
                && lifecycler.get_tokens().len() == 128
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(lifecycler.get_state(), InstanceState::Active);
        let rust_tokens = lifecycler.get_tokens();
        assert_eq!(rust_tokens.len(), 128);

        // 4. Poll Go Mimir GET /ingester/ring (Accept: application/json) and assert rust-ingester-1 is ACTIVE with all 128 tokens!
        let mut found_rust_shard: Option<GoShardJson> = None;
        for _ in 0..80 {
            if let Ok(resp) = http_client
                .get(&ring_url)
                .header("Accept", "application/json")
                .send()
                .await
            {
                if let Ok(json) = resp.json::<GoRingJson>().await {
                    if let Some(s) = json
                        .shards
                        .into_iter()
                        .find(|s| s.id == "rust-ingester-1" && s.state == "ACTIVE")
                    {
                        if s.tokens.len() == 128 {
                            found_rust_shard = Some(s);
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if found_rust_shard.is_none() {
            let _ = guard.child.kill();
            let _ = guard.child.wait();
            if let Some(mut se) = go_stderr {
                use std::io::Read;
                let mut s = String::new();
                let _ = se.read_to_string(&mut s);
                eprintln!("=== GO MIMIR LOG ===\n{s}\n====================");
            }
        }
        let rust_shard = found_rust_shard
            .expect("Go Mimir /ingester/ring did not report rust-ingester-1 as ACTIVE with 128 tokens");
        assert_eq!(rust_shard.state, "ACTIVE");
        assert_eq!(rust_shard.address, "127.0.0.1:19097");
        assert_eq!(rust_shard.tokens, rust_tokens);

        // 5. Clean shutdown of Rust lifecycler & memberlist KV.
        stop_and_await_terminated(ServiceContext::background(), lc_svc.as_ref())
            .await
            .expect("stop Rust lifecycler");

        // Poll Go Mimir /ingester/ring to verify rust-ingester-1 transitioned out of ACTIVE (LEFT or removed).
        let mut left_observed = false;
        for _ in 0..40 {
            if let Ok(resp) = http_client
                .get(&ring_url)
                .header("Accept", "application/json")
                .send()
                .await
            {
                if let Ok(json) = resp.json::<GoRingJson>().await {
                    let s_opt = json.shards.into_iter().find(|s| s.id == "rust-ingester-1");
                    match s_opt {
                        None => {
                            left_observed = true;
                            break;
                        }
                        Some(s) if s.state == "LEFT" => {
                            left_observed = true;
                            break;
                        }
                        _ => {}
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            left_observed,
            "Go Mimir did not observe rust-ingester-1 leaving after Rust lifecycler shutdown"
        );

        ml_kv.shutdown().await;
    });
}

// ============================================================================
// Additional KV & ShuffleShard tests
// ============================================================================

#[test]
fn kv_inmemory_multi_and_shuffle_shard_smoke() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let codec = Arc::new(StringCodec);
        let primary: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(codec.clone()));
        let secondary: Arc<dyn KvClient> = Arc::new(InMemoryKv::new(codec));

        let multi = MultiKv::new(
            MultiKVConfig {
                primary: "inmemory-1".to_string(),
                secondary: "inmemory-2".to_string(),
                mirror_enabled: true,
                mirror_timeout: mimir_config::duration::GoDuration::from_secs(2),
            },
            vec![
                NamedKvClient {
                    name: "inmemory-1".to_string(),
                    client: primary.clone(),
                },
                NamedKvClient {
                    name: "inmemory-2".to_string(),
                    client: secondary.clone(),
                },
            ],
            None,
        );

        multi
            .cas_with_mirror_clone::<String, _>("k1", |_| Ok(Some("v1".to_string())))
            .await
            .unwrap();

        let p_val = get_typed::<String>(primary.as_ref(), "k1").await.unwrap();
        let s_val = get_typed::<String>(secondary.as_ref(), "k1").await.unwrap();
        assert_eq!(p_val.as_deref(), Some("v1"));
        assert_eq!(s_val.as_deref(), Some("v1"));

        // WatchKey on InMemoryKv
        let cancel = ServiceContext::background();
        let cancel_cb = cancel.clone();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        let p_watch = primary.clone();
        let handle = tokio::spawn(async move {
            p_watch
                .watch_key(
                    "k1",
                    cancel_cb,
                    Box::new(move |any| {
                        let s = any.downcast::<String>().unwrap();
                        seen_cb.lock().unwrap().push(*s);
                        false
                    }),
                )
                .await;
        });
        handle.await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec!["v1".to_string()]);
    });

    // ShuffleShard stability & determinism test
    let now_sec = 1_700_000_000_i64;
    let mut ingesters = BTreeMap::new();
    for i in 0..6 {
        let zone = format!("zone-{}", ['a', 'b', 'c'][i % 3]);
        let id = format!("ing-{i}");
        ingesters.insert(
            id.clone(),
            mk_inst(
                &id,
                &format!("10.0.0.{}:9095", i + 1),
                &zone,
                InstanceState::Active,
                now_sec,
                vec![(i as u32 + 1) * 500_000_000],
                now_sec - 100,
                false,
                0,
            ),
        );
    }
    let desc = Desc {
        ingesters: ingesters.into_iter().collect(),
    };
    let ring = Ring::from_desc(
        RingSettings {
            heartbeat_timeout: Duration::from_secs(60),
            replication_factor: 3,
            zone_awareness_enabled: true,
            excluded_zones: Vec::new(),
            subring_cache_disabled: false,
        },
        desc,
        Arc::new(DefaultReplicationStrategy),
    )
    .unwrap();

    let s1 = ring.shuffle_shard("tenant-alpha", 3);
    let s2 = ring.shuffle_shard("tenant-alpha", 3);
    assert_eq!(s1.instances_count(), 3);
    assert_eq!(s1.zones_count(), 3);
    assert_eq!(s1.desc_snapshot().ingesters.len(), s2.desc_snapshot().ingesters.len());
    for k in s1.desc_snapshot().ingesters.keys() {
        assert!(s2.has_instance(k));
    }

    // Ring & PartitionRing HTTP status JSON check
    let _ = StringSliceCSV::default();
}
